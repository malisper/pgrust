use core::cell::Cell;
use std::rc::Rc;

use mcx::PgVec;
use types_core::{
    InvalidOid, InvalidRelFileNumber, InvalidSubTransactionId, Oid, ProcNumber,
    INVALID_PROC_NUMBER, PG_CATALOG_NAMESPACE, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{
    FormData_pg_class, RelationData, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_RELATION,
    REPLICA_IDENTITY_NOTHING,
};
use types_tuple::{NameData, TupleConstr};

use crate::schemapg::BootstrapCatalog;
use crate::{cache_mcx, store, with_state, InProgressEnt};

pub const GLOBALTABLESPACE_OID: Oid = 1664;
pub const HEAP_TABLE_AM_OID: Oid = 2;

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_relpersistence(c: u8) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid relpersistence: {}", c as char))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// RelationInitTableAccessMethod (relcache.c), closed-AM form: C resolves
// pg_am.amhandler into rd_tableam per entry; here heap-handler AMs are
// recorded in tableam_vocab's registry so TableAm::of maps non-builtin
// relam values. Handler proc oid 3 = heap_tableam_handler (pg_proc.dat).
pub(crate) fn RelationInitTableAccessMethod(relkind: u8, relam: Oid) -> PgResult<()> {
    const F_HEAP_TABLEAM_HANDLER: Oid = 3;
    if !types_rel::RELKIND_HAS_TABLE_AM(relkind)
        || relam == InvalidOid
        || relam == HEAP_TABLE_AM_OID
    {
        return Ok(());
    }
    // pgrcolumnar is identified by pg_am.amname, not amhandler: the closed-AM
    // engine never invokes handlers (docs/design/pgrcolumnar-impl.md §7.1).
    if syscache_seams::pg_am_amname::call(relam)?.as_deref() == Some("cbstore") {
        tableam_vocab::register_pgrcolumnar_table_am(relam);
        return Ok(());
    }
    match syscache_seams::pg_am_amhandler::call(relam)? {
        Some(F_HEAP_TABLEAM_HANDLER) => {
            tableam_vocab::register_heap_table_am(relam);
            Ok(())
        }
        Some(other) => panic!(
            "unported: table AM handler function {other} (relam {relam}); \
             only heap_tableam_handler is carried"
        ),
        None => Err(Box::new(PgError::error(format!(
            "cache lookup failed for access method {relam}"
        )))),
    }
}

// Steady-state arms only: the historic-snapshot (logical decoding) refresh is
// unported. The parallel-worker rd_firstRelfilelocatorSubid restore is ported
// below (W2a parallel writes; C c6b92041 parity).
pub(crate) fn RelationInitPhysicalAddr(data: &RelationData<'_>) -> PgResult<()> {
    if !types_rel::RELKIND_HAS_STORAGE(data.rd_rel.relkind) {
        return Ok(());
    }
    let spc = if data.rd_rel.reltablespace != InvalidOid {
        data.rd_rel.reltablespace
    } else {
        init_small::globals::MyDatabaseTableSpace()
    };
    let db = if spc == GLOBALTABLESPACE_OID {
        InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    };
    let rel_number = if data.rd_rel.relfilenode != InvalidRelFileNumber {
        data.rd_rel.relfilenode
    } else {
        let n = relmapper_seams::relation_map_oid_to_filenumber::call(
            data.rd_id,
            data.rd_rel.relisshared,
        );
        if n == InvalidRelFileNumber {
            return Err(Box::new(PgError::error(format!(
                "could not find relation mapping for relation \"{}\", OID {}",
                String::from_utf8_lossy(data.rd_rel.relname.name_str()),
                data.rd_id
            ))));
        }
        n
    };
    let locator = types_storage::RelFileLocator::new(spc, db, rel_number);
    data.rd_locator.set(locator);

    // C relcache.c (c6b92041 wal_level=minimal WAL-skip): "For
    // RelationNeedsWAL() to answer correctly on parallel workers, restore
    // rd_firstRelfilelocatorSubid. No subtransactions start or end while in
    // parallel mode, so the specific SubTransactionId does not matter."
    // A worker's fresh entry for a leader-created-this-xact relation would
    // otherwise read as WAL-logged while the leader's reads WAL-skipped —
    // splitting one relfilenode between logged and unlogged writes (the W2a
    // parallel-write engage seam asserts the two agree and fails closed).
    // pendingSyncs are restored into workers by the binder (RestorePendingSyncs),
    // so the probe answers leader-identically. DIVERGENCE from C: also
    // register for AtEOXact cleanup — C's worker process dies at xact end,
    // but a retained worker thread must not park with the field set (the
    // next task's blanket invalidation would trip RelationClearRelation's
    // no-in-transaction-subids invariant).
    if parallel_seams::is_parallel_worker::is_installed()
        && parallel_seams::is_parallel_worker::call()
        && catalog_storage_seams::rel_file_locator_skipping_wal::is_installed()
        && catalog_storage_seams::rel_file_locator_skipping_wal::call(locator)
    {
        data.rd_firstRelfilelocatorSubid
            .set(types_core::xact::TopSubTransactionId);
        crate::store::eoxact_list_add(data.rd_id);
    }
    Ok(())
}

fn resolve_backend(form: &FormData_pg_class) -> PgResult<(ProcNumber, bool)> {
    match form.relpersistence {
        RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => Ok((INVALID_PROC_NUMBER, false)),
        RELPERSISTENCE_TEMP => {
            if namespace_seams::is_temp_or_temp_toast_namespace::call(form.relnamespace) {
                Ok((init_small::globals::ProcNumberForTempRelations(), true))
            } else {
                let backend =
                    namespace_seams::get_temp_namespace_proc_number::call(form.relnamespace)?;
                debug_assert!(backend != INVALID_PROC_NUMBER);
                Ok((backend, false))
            }
        }
        other => Err(invalid_relpersistence(other)),
    }
}

// RelationBuildDesc minus insert/validity: the catalog half runs behind the
// relcache_build_seams surfaces. Returns None when no pg_class row exists.
// On error the in_progress frame stays, exactly like C: AtEOXact_RelationCache
// clears it during abort.
pub(crate) fn build_desc_data(target_rel_id: Oid) -> PgResult<Option<RelationData<'static>>> {
    let offset = with_state(|st| {
        st.in_progress.push(InProgressEnt { reloid: target_rel_id, invalidated: false });
        st.in_progress.len() - 1
    });

    let built = loop {
        with_state(|st| st.in_progress[offset].invalidated = false);

        let index_ok = with_state(|st| st.critical_relcaches_built);
        let Some(scanned) =
            relcache_build_seams::scan_pg_relation::call(target_rel_id, index_ok, false)?
        else {
            break None;
        };

        let mcx = cache_mcx();
        let (rd_backend, rd_islocaltemp) = resolve_backend(&scanned.form)?;
        RelationInitTableAccessMethod(scanned.form.relkind, scanned.form.relam)?;
        let rd_att =
            relcache_build_seams::relation_build_tuple_desc::call(
                mcx,
                target_rel_id,
                &scanned.form,
                scanned.relchecks,
            )?;

        let (rd_index, opcintype, opfamily, indoption, indcollation, supportinfo, support) =
            if matches!(
                scanned.form.relkind,
                RELKIND_INDEX | RELKIND_PARTITIONED_INDEX
            ) {
                let ii = relcache_build_seams::relation_init_index_access_info::call(
                    mcx,
                    target_rel_id,
                    &scanned.form,
                )?;
                (
                    Some(ii.index),
                    ii.opcintype,
                    ii.opfamily,
                    ii.indoption,
                    ii.indcollation,
                    ii.supportinfo,
                    ii.support,
                )
            } else {
                (
                    None,
                    PgVec::new_in(mcx),
                    PgVec::new_in(mcx),
                    PgVec::new_in(mcx),
                    PgVec::new_in(mcx),
                    Vec::new(),
                    PgVec::new_in(mcx),
                )
            };

        // rules/triggers/RLS live with the nodexform unit (no rd_rules/
        // trigdesc/rd_rsdesc fields in the trimmed entry); rd_locator/rd_smgr
        // wait on the storage unit (RelationInitPhysicalAddr absent).
        let rd_lockInfo = lmgr::RelationInitLockInfo(target_rel_id, scanned.form.relisshared);

        let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
            rd_id: target_rel_id,
            rd_backend,
            rd_islocaltemp,
            rd_isvalid: Cell::new(false),
            rd_createSubid: Cell::new(InvalidSubTransactionId),
            rd_newRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
            rd_firstRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
            rd_droppedSubid: Cell::new(InvalidSubTransactionId),
            rd_lockInfo,
            rd_rel: scanned.form,
            rd_att,
            rd_index,
            rd_opcintype: opcintype,
            rd_opfamily: opfamily,
            rd_indoption: indoption,
            rd_indcollation: indcollation,
            rd_options: scanned.options,
            pgstat_enabled: Cell::new(false),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: support,
            rd_supportinfo: core::cell::RefCell::new(supportinfo),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: scanned.relhastriggers, rd_hasrules: scanned.relhasrules,
        };
        RelationInitPhysicalAddr(&data)?;

        if with_state(|st| st.in_progress[offset].invalidated) {
            continue;
        }
        break Some(data);
    };

    with_state(|st| {
        debug_assert_eq!(offset + 1, st.in_progress.len());
        st.in_progress.pop();
    });
    Ok(built)
}

pub fn RelationBuildDesc(
    targetRelId: Oid,
    insertIt: bool,
) -> PgResult<Option<Rc<RelationData<'static>>>> {
    let Some(data) = build_desc_data(targetRelId)? else {
        return Ok(None);
    };
    let rel = Rc::new(data);
    if insertIt {
        store::insert(Rc::clone(&rel), false, true)?;
    }
    rel.rd_isvalid.set(true);
    Ok(Some(rel))
}

// formrdesc: nailed-catalog bootstrap from the hardcoded genbki descriptors.
pub fn formrdesc(cat: &BootstrapCatalog) -> PgResult<()> {
    let mcx = cache_mcx();
    let relid = cat.relid;
    debug_assert_eq!(relid, cat.attrs[0].attrelid);

    let mut td = tupdesc::CreateTupleDesc(mcx, cat.attrs)?;
    td.tdtypeid = cat.rowtype_id;
    td.tdtypmod = -1;
    td.tdrefcount = 1;
    td.compact_attrs[0].attcacheoff.set(0);
    if cat.attrs.iter().any(|a| a.attnotnull) {
        td.constr = Some(mcx::box_new_in(
            mcx,
            TupleConstr {
                defval: PgVec::new_in(mcx),
                check: PgVec::new_in(mcx),
                missing: PgVec::new_in(mcx),
                num_defval: 0,
                num_check: 0,
                has_not_null: true,
                has_generated_stored: false,
                has_generated_virtual: false,
            },
        ));
    }

    let is_bootstrap = miscinit_seams::is_bootstrap_processing_mode::call();
    let mut relname = NameData::default();
    relname.namestrcpy(cat.name);

    // relowner stays InvalidOid: RelationCacheInitializePhase3's cue that the
    // real pg_class row hasn't replaced this stub yet.
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: PG_CATALOG_NAMESPACE,
        reltype: cat.rowtype_id,
        relowner: InvalidOid,
        relam: HEAP_TABLE_AM_OID,
        relfilenode: InvalidRelFileNumber,
        reltablespace: if cat.shared { GLOBALTABLESPACE_OID } else { InvalidOid },
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: InvalidOid,
        relhasindex: !is_bootstrap,
        relisshared: cat.shared,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: REPLICA_IDENTITY_NOTHING,
        relispartition: false,
        relfrozenxid: 0,
        relminmxid: 0,
    };

    if is_bootstrap {
        relmapper_seams::relation_map_update_map::call(relid, relid, cat.shared, true)?;
    }

    let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: relid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(InvalidSubTransactionId),
        rd_newRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_firstRelfilelocatorSubid: Cell::new(InvalidSubTransactionId),
        rd_droppedSubid: Cell::new(InvalidSubTransactionId),
        rd_lockInfo: lmgr::RelationInitLockInfo(relid, cat.shared),
        rd_rel,
        rd_att: Rc::new(td),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    RelationInitPhysicalAddr(&data)?;

    store::insert(Rc::new(data), true, false)
}
