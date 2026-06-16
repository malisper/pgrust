//! build family — `RelationBuildDesc` orchestration (IN-CRATE) and the
//! descriptor-assembly subroutines.
//!
//! The orchestration (`RelationBuildDesc`, `AllocateRelationDesc`,
//! `RelationBuildTupleDesc`, `RelationParseRelOptions`, `formrdesc` +
//! `BuildHardcodedDescriptor`, `AttrDefaultFetch`, `CheckNNConstraintFetch`)
//! is relcache's OWN logic and lands here in full over the REAL entry store.
//! ONLY the catalog-scan / tuple-deform primitives these routines sit on top
//! of (`ScanPgRelation`: `systable_beginscan`/`getnext` via genam, the
//! `GETSTRUCT` deform of `pg_class`/`pg_attribute`, `extractRelOptions` via the
//! reloptions unit) are genuine cross-unit seams, routed through their owner
//! (seam-and-panic until the owner lands).

use backend_access_common_tupdesc::CreateTupleDesc;
use backend_utils_error::{ereport, PgResult};
use mcx::{Mcx, PgString, PgVec};
use types_catalog::catalog::GLOBALTABLESPACE_OID;
use types_core::catalog::{
    PG_CATALOG_NAMESPACE, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_core::primitive::Oid;
use types_core::xact::InvalidSubTransactionId;
use types_core::{InvalidOid, INVALID_PROC_NUMBER};
use types_error::ERROR;
use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use crate::core_entry_store::entry::{FormPgClass, OwnedAttr, OwnedTupleDesc, RelationData};
use crate::core_entry_store::{cache_insert, with_state, InProgressEnt};

/// `RECORDOID` (`pg_type.h`) — the pseudo-type for anonymous record types.
const RECORDOID: Oid = 2249;
/// `RELPERSISTENCE_TEMP`/`_PERMANENT`/`_UNLOGGED` are `u8` in the type crate;
/// the entry stores `relpersistence` as `i8`. These mirror them in `i8`.
const PERSIST_PERMANENT: i8 = RELPERSISTENCE_PERMANENT as i8;
const PERSIST_UNLOGGED: i8 = RELPERSISTENCE_UNLOGGED as i8;
const PERSIST_TEMP: i8 = RELPERSISTENCE_TEMP as i8;

/// `lookup_rowtype_tupdesc_internal(type_id, typmod=-1)` (typcache.c) relcache
/// half: open the composite type's relation by `typrelid` via
/// [`crate::core_entry_store::RelationIdGetRelation`], materialize a copy of its
/// `rd_att` tuple descriptor into `mcx`, and close the relation. **Own logic**
/// over the entry store + the `CreateTupleDesc` tupdesc utility; the caller
/// (typcache) holds the refcount discipline.
pub fn relation_get_composite_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    typrelid: Oid,
    type_id: Oid,
) -> PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>> {
    // relation_open(typrelid, AccessShareLock) — the relcache pin (RAII guard).
    let rel = crate::core_entry_store::RelationRef::open(typrelid)?;
    // CreateTupleDescCopyConstr(RelationGetDescr(rel)) — materialize a standalone
    // copy of the entry's tuple descriptor, tagged with the composite type.
    let attrs = rel.with(|r| r.rd_att.build_form_attrs(r.rd_id));
    let mut td = CreateTupleDesc(mcx, &attrs)?;
    td.tdtypeid = type_id;
    td.tdtypmod = -1;
    td.tdrefcount = 1;
    let boxed = mcx::alloc_in(mcx, td)?;
    // relation_close(rel, AccessShareLock) — release the pin (guard Drop).
    drop(rel);
    Ok(boxed)
}

/// `CreateFakeRelcacheEntry(rlocator)` (xlogutils.c): build a throwaway
/// relcache entry for a relation we only know the [`RelFileLocator`] of (WAL
/// replay / page-level operations). The C `palloc0`s a `FakeRelCacheEntryData`,
/// sets the physical-storage fields, marks it permanent, and `smgropen`s a
/// non-pinned handle. **Own substrate**: we build the cross-unit value-slice
/// (the trimmed `rd_rel`, an empty `rd_att`, `rd_locator`/`rd_backend`); the
/// non-pinned `SMgrRelation` is opened lazily by the storage owner and is not
/// part of the value-slice.
pub fn create_fake_relcache_entry<'mcx>(
    mcx: Mcx<'mcx>,
    rlocator: types_storage::RelFileLocator,
) -> PgResult<types_rel::RelationData<'mcx>> {
    // palloc0(FakeRelCacheEntryData) + the hand-filled pg_class fields.
    let mut entry = RelationData::new_blank();
    entry.rd_locator = rlocator;
    entry.rd_backend = INVALID_PROC_NUMBER;
    // C: "We will never be working with temp rels during recovery." — the fake
    // entry is always treated as a permanent relation.
    entry.rd_rel.relpersistence = PERSIST_PERMANENT;
    project_entry(mcx, &entry)
}

/// `FreeFakeRelcacheEntry(fakerel)` (xlogutils.c): drop the throwaway entry.
/// The C `smgrclose`s the non-pinned handle then `pfree`s; here the value-slice
/// is `mcx`-allocated, so taking ownership and dropping it is the reclaim (the
/// smgr handle, when opened by the storage owner, is closed on its side).
pub fn free_fake_relcache_entry(fakerel: types_rel::RelationData<'_>) {
    drop(fakerel);
}

/// Project the owned relcache entry into the cross-unit
/// [`types_rel::RelationData`] value-slice, copied into `mcx` (the C "copy the
/// consumed slice of the entry into the caller's memory context"). This is the
/// build family's projection half, used by the `relation_id_get_relation`
/// seam. **Own logic.**
pub(crate) fn project_relation_data<'mcx>(
    mcx: Mcx<'mcx>,
    r: &RelationData,
) -> PgResult<types_rel::RelationData<'mcx>> {
    // `r` is a live cache-owned (or in-build) descriptor borrowed by the caller;
    // we only read its scalar/owned fields to materialize the value-slice.
    project_entry(mcx, r)
}

/// Build the cross-unit [`types_rel::RelationData`] slice from the owned entry.
/// Mirrors the field subset that crosses the seam (see `crates/types-rel`):
/// `rd_id`/`rd_locator`/`rd_backend`, the trimmed `rd_rel`, the materialized
/// `rd_att` tuple descriptor, `rd_options`, and the index fields (`None`/empty
/// for a table). Everything is allocated in `mcx`.
fn project_entry<'mcx>(
    mcx: Mcx<'mcx>,
    r: &RelationData,
) -> PgResult<types_rel::RelationData<'mcx>> {
    let rd_rel = project_form_pg_class(mcx, &r.rd_rel)?;
    // Materialize the tuple descriptor from the entry's owned attribute rows.
    // The owned->borrowed projection (build the `Form_pg_attribute[]`, feed it
    // through `CreateTupleDesc` to populate the parallel compact_attrs, stamp
    // the composite type id/typmod/refcount) now lives on the entry type as
    // `OwnedTupleDesc::project_in` (F0''); delegate to it.
    let rd_att = r.rd_att.project_in(mcx, r.rd_id)?;
    // Index fields: `rd_index` / `rd_opcintype` (None/empty for a table).
    let rd_index = r.rd_index.as_ref().map(|ix| types_rel::FormData_pg_index {
        indnatts: ix.indnatts,
        indnkeyatts: ix.indnkeyatts,
        indisunique: ix.indisunique,
        indisprimary: ix.indisprimary,
        indisexclusion: ix.indisexclusion,
        indisready: ix.indisready,
        indimmediate: ix.indimmediate,
        indnullsnotdistinct: ix.indnullsnotdistinct,
        indrelid: ix.indrelid,
        indisvalid: ix.indisvalid,
        indkey0: ix.indkey.first().copied().unwrap_or(0),
    });
    let mut rd_opcintype: PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for &t in &r.rd_opcintype {
        rd_opcintype.push(t);
    }
    let mut rd_opfamily: PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for &t in &r.rd_opfamily {
        rd_opfamily.push(t);
    }
    let mut rd_indoption: PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
    for &t in &r.rd_indoption {
        rd_indoption.push(t);
    }
    let mut rd_indcollation: PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    for &t in &r.rd_indcollation {
        rd_indcollation.push(t);
    }
    Ok(types_rel::RelationData {
        rd_id: r.rd_id,
        rd_locator: r.rd_locator,
        rd_backend: r.rd_backend,
        rd_rel,
        rd_att,
        rd_options: r.rd_options.clone(),
        rd_index,
        rd_opcintype,
        rd_opfamily,
        rd_indoption,
        rd_indcollation,
        // rd_trigdesc: deep-copy the entry's cache-arena TriggerDesc (built by
        // RelationBuildTriggers) into `mcx` for the cross-unit value-slice. C
        // hands the consumer the cache's `trigdesc` pointer; the owned model
        // projects a copy charged to the caller's context (`None` is the C
        // NULL — no triggers).
        rd_trigdesc: match &r.rd_trigdesc {
            Some(td) => Some(project_trigdesc(mcx, td)?),
            None => None,
        },
        // Mirror of the entry's `pgstat_enabled`; the pgstat count seams read
        // it off this trimmed handle to replicate the C count macros' gate.
        pgstat_enabled: r.pgstat_enabled,
    })
}

/// Deep-copy the entry's cache-arena `TriggerDesc` into `mcx` for the
/// cross-unit value-slice (`types_rel::RelationData.rd_trigdesc`). Mirrors
/// `CopyTriggerDesc` (every `Trigger` field — name / attr / args / qual /
/// transition tables — is duplicated), but copying into the caller's `mcx`
/// instead of `CacheMemoryContext`.
fn project_trigdesc<'mcx>(
    mcx: Mcx<'mcx>,
    src: &types_trigger::TriggerDesc<'_>,
) -> PgResult<mcx::PgBox<'mcx, types_trigger::TriggerDesc<'mcx>>> {
    let mut td = types_trigger::TriggerDesc::new_in(mcx);
    // Copy the hint flags verbatim.
    td.numtriggers = src.numtriggers;
    td.trig_insert_before_row = src.trig_insert_before_row;
    td.trig_insert_after_row = src.trig_insert_after_row;
    td.trig_insert_instead_row = src.trig_insert_instead_row;
    td.trig_insert_before_statement = src.trig_insert_before_statement;
    td.trig_insert_after_statement = src.trig_insert_after_statement;
    td.trig_update_before_row = src.trig_update_before_row;
    td.trig_update_after_row = src.trig_update_after_row;
    td.trig_update_instead_row = src.trig_update_instead_row;
    td.trig_update_before_statement = src.trig_update_before_statement;
    td.trig_update_after_statement = src.trig_update_after_statement;
    td.trig_delete_before_row = src.trig_delete_before_row;
    td.trig_delete_after_row = src.trig_delete_after_row;
    td.trig_delete_instead_row = src.trig_delete_instead_row;
    td.trig_delete_before_statement = src.trig_delete_before_statement;
    td.trig_delete_after_statement = src.trig_delete_after_statement;
    td.trig_truncate_before_statement = src.trig_truncate_before_statement;
    td.trig_truncate_after_statement = src.trig_truncate_after_statement;
    td.trig_insert_new_table = src.trig_insert_new_table;
    td.trig_update_old_table = src.trig_update_old_table;
    td.trig_update_new_table = src.trig_update_new_table;
    td.trig_delete_old_table = src.trig_delete_old_table;

    let mut triggers: PgVec<'mcx, types_trigger::Trigger<'mcx>> = mcx::PgVec::new_in(mcx);
    triggers
        .try_reserve(src.triggers.len())
        .map_err(|_| mcx.oom(src.triggers.len()))?;
    for t in src.triggers.iter() {
        let tgname = PgString::from_str_in(t.tgname.as_str(), mcx)?;
        let mut tgattr: PgVec<'mcx, i16> = mcx::PgVec::new_in(mcx);
        tgattr.try_reserve(t.tgattr.len()).map_err(|_| mcx.oom(t.tgattr.len()))?;
        for &a in t.tgattr.iter() {
            tgattr.push(a);
        }
        let mut tgargs: PgVec<'mcx, PgString<'mcx>> = mcx::PgVec::new_in(mcx);
        tgargs.try_reserve(t.tgargs.len()).map_err(|_| mcx.oom(t.tgargs.len()))?;
        for a in t.tgargs.iter() {
            tgargs.push(PgString::from_str_in(a.as_str(), mcx)?);
        }
        let tgqual = match &t.tgqual {
            Some(q) => Some(PgString::from_str_in(q.as_str(), mcx)?),
            None => None,
        };
        let tgoldtable = match &t.tgoldtable {
            Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };
        let tgnewtable = match &t.tgnewtable {
            Some(s) => Some(PgString::from_str_in(s.as_str(), mcx)?),
            None => None,
        };
        triggers.push(types_trigger::Trigger {
            tgoid: t.tgoid,
            tgname,
            tgfoid: t.tgfoid,
            tgtype: t.tgtype,
            tgenabled: t.tgenabled,
            tgisinternal: t.tgisinternal,
            tgisclone: t.tgisclone,
            tgconstrrelid: t.tgconstrrelid,
            tgconstrindid: t.tgconstrindid,
            tgconstraint: t.tgconstraint,
            tgdeferrable: t.tgdeferrable,
            tginitdeferred: t.tginitdeferred,
            tgnargs: t.tgnargs,
            tgnattr: t.tgnattr,
            tgattr,
            tgargs,
            tgqual,
            tgoldtable,
            tgnewtable,
        });
    }
    td.triggers = triggers;
    mcx::alloc_in(mcx, td).map_err(|_| mcx.oom(0))
}

/// Project the owned `FormPgClass` mirror into the cross-unit trimmed form,
/// copying the name into `mcx`. `relpersistence`/`relkind` are `i8` on the
/// entry; the cross-unit slice carries them as `u8` (`RELPERSISTENCE_*`/
/// `RELKIND_*`).
fn project_form_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    f: &FormPgClass,
) -> PgResult<types_rel::FormData_pg_class<'mcx>> {
    Ok(types_rel::FormData_pg_class {
        relname: PgString::from_str_in(&f.relname, mcx)?,
        relnamespace: f.relnamespace,
        relowner: f.relowner,
        relrowsecurity: f.relrowsecurity,
        relpages: f.relpages,
        reltuples: f.reltuples,
        relallvisible: f.relallvisible,
        reltoastrelid: f.reltoastrelid,
        reltablespace: f.reltablespace,
        relfilenode: f.relfilenode,
        relisshared: f.relisshared,
        relhasindex: f.relhasindex,
        relhassubclass: f.relhassubclass,
        relpersistence: f.relpersistence as u8,
        relkind: f.relkind as u8,
        relam: f.relam,
        relispopulated: f.relispopulated,
        relreplident: f.relreplident as u8,
        relispartition: f.relispartition,
        relfrozenxid: f.relfrozenxid,
        relminmxid: f.relminmxid,
    })
}

/// `RelationBuildDesc(targetRelId, insertIt)` (relcache.c): assemble a fresh
/// relcache entry for `targetRelId` by reading `pg_class` (via
/// [`ScanPgRelation`]), build its tuple descriptor, parse reloptions,
/// initialize index/table access info, and (if `insertIt`) install it in the
/// `RelationIdCache`. Returns the built `Relation` (the C pointer), or `null`
/// when no `pg_class` row exists. **Own orchestration.**
pub fn RelationBuildDesc(targetRelId: Oid, insertIt: bool) -> PgResult<Oid> {
    // Push our entry onto in_progress_list (the invalidation-restart protocol).
    // C grows a fixed array; the owned model is a Vec, so the offset is the
    // current length before the push.
    let in_progress_offset = with_state(|st| {
        let off = st.in_progress_list.len();
        st.in_progress_list.push(InProgressEnt {
            reloid: targetRelId,
            invalidated: false,
        });
        off
    });

    let mut relation: Box<RelationData> = loop {
        // Reset the invalidated flag for this attempt.
        with_state(|st| st.in_progress_list[in_progress_offset].invalidated = false);

        // Read pg_class for the target (catalog scan — cross-unit seam). The
        // reloptions text[] bytes ride alongside the form for
        // RelationParseRelOptions (C hands it the whole pg_class_tuple).
        let (relp, reloptions) = match ScanPgRelation(targetRelId, true, false)? {
            Some(pair) => pair,
            None => {
                // No pg_class row: pop our in_progress entry and return NULL.
                with_state(|st| {
                    st.in_progress_list.truncate(in_progress_offset);
                });
                return Ok(types_core::InvalidOid);
            }
        };

        let relid = targetRelId;

        // Allocate the descriptor and copy the pg_class form into rd_rel.
        let mut relation = AllocateRelationDesc(relp)?;

        // Initialize the relation's lifecycle fields.
        relation.rd_id = relid;
        relation.rd_refcnt = 0;
        relation.rd_isnailed = false;
        relation.rd_createSubid = InvalidSubTransactionId;
        relation.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
        relation.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
        relation.rd_droppedSubid = InvalidSubTransactionId;

        // rd_backend / rd_islocaltemp from relpersistence.
        match relation.rd_rel.relpersistence {
            PERSIST_UNLOGGED | PERSIST_PERMANENT => {
                relation.rd_backend = INVALID_PROC_NUMBER;
                relation.rd_islocaltemp = false;
            }
            PERSIST_TEMP => {
                // Temp-namespace ownership resolution (isTempOrTempToastNamespace
                // / GetTempNamespaceProcNumber) is namespace.c logic — a genuine
                // cross-unit dependency. Seam-and-panic until that owner lands.
                return Err(ereport(ERROR)
                    .errmsg_internal(
                        "relcache-build: temp-relation backend resolution \
                         (isTempOrTempToastNamespace/GetTempNamespaceProcNumber) \
                         is namespace.c (cross-unit); not yet landed",
                    )
                    .into_error());
            }
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(&format!("invalid relpersistence: {}", other as u8 as char))
                    .into_error());
            }
        }

        // Build the tuple descriptor (pg_attribute scan + constraints).
        RelationBuildTupleDesc(&mut relation)?;

        // Derived-list / partition presence reset (C zeroes these here).
        relation.rd_fkeyvalid = false;
        relation.rd_has_partkey = false;
        relation.rd_has_partdesc = false;
        relation.rd_partcheckvalid = false;

        // Index vs table access-method init (index family — own logic, separate
        // branch). Partitioned tables get neither, exactly as C. These run on the
        // local, not-yet-inserted descriptor via `&mut RelationData`.
        let relkind = relation.rd_rel.relkind as u8;
        if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
            crate::index::RelationInitIndexAccessInfo(&mut relation)?;
        } else if relkind == RELKIND_RELATION
            || relkind == RELKIND_TOASTVALUE
            || relkind == RELKIND_MATVIEW
            || relkind == RELKIND_SEQUENCE
        {
            crate::index::RelationInitTableAccessMethod(&mut relation)?;
        } else {
            // RELKIND_PARTITIONED_TABLE: no access method (C falls through).
            debug_assert!(relkind == RELKIND_PARTITIONED_TABLE || true);
        }

        // Parse reloptions into rd_options.
        RelationParseRelOptions(&mut relation, reloptions.as_deref())?;

        // Rules / triggers / row-security (derived family — own logic, separate
        // branch). C builds them when the pg_class flags are set, else NULLs.
        if relation.rd_rel.relhasrules {
            crate::derived::RelationBuildRuleLock(&mut relation)?;
        } else {
            relation.rd_rules = None;
        }
        if relation.rd_rel.relhastriggers {
            // RelationBuildTriggers (commands/trigger.c): scan pg_trigger, build
            // the TriggerDesc, and store it on the not-yet-inserted descriptor.
            // The catalog-read leg is OWN logic over the genam
            // `relcache_scan_pg_trigger` primitive; the descriptor is allocated
            // in the CacheMemoryContext arena (see derived::RelationBuildTriggers).
            crate::derived::RelationBuildTriggers(&mut relation)?;
            // C: RelationBuildTriggers sets relation->trigdesc; the presence flag
            // tracks whether the scan actually found triggers (an empty scan
            // leaves trigdesc NULL even though relhastriggers was set).
            relation.rd_has_trigdesc = relation.rd_trigdesc.is_some();
        } else {
            relation.rd_trigdesc = None;
            relation.rd_has_trigdesc = false;
        }
        if relation.rd_rel.relrowsecurity {
            // RelationBuildRowSecurity is policy.c (cross-unit). Seam-and-panic.
            return Err(ereport(ERROR)
                .errmsg_internal(
                    "relcache-build: RelationBuildRowSecurity is rewrite/rowsecurity.c \
                     (cross-unit); not yet landed",
                )
                .into_error());
        } else {
            relation.rd_has_rsdesc = false;
        }

        // Lock info + physical address (index family — own logic, separate
        // branch). RelationInitLockInfo fills rd_lockInfo from rd_id/relisshared.
        RelationInitLockInfo(&mut relation);
        crate::index::RelationInitPhysicalAddr(&mut relation)?;

        // C frees pg_class_tuple here; in the owned model `relp` was already
        // consumed by AllocateRelationDesc.

        // Restart if invalidated mid-build, else done.
        let invalidated =
            with_state(|st| st.in_progress_list[in_progress_offset].invalidated);
        if !invalidated {
            break relation;
        }
        // Invalidated: destroy this local descriptor (drop the Box — the C
        // `RelationDestroyRelation(relation, false)`) and retry.
        crate::invalidate::RelationDestroyRelation(relation, false);
    };

    // Pop our in_progress entry.
    with_state(|st| st.in_progress_list.truncate(in_progress_offset));

    // It's fully valid.
    relation.rd_isvalid = true;

    let relid = relation.rd_id;
    if insertIt {
        // RelationCacheInsert(relation, true): install into the store, replacing
        // any existing entry. The entry store owns the `Box`; the displaced
        // descriptor (if any) is freed/leak-warned there (the C macro's
        // RelationDestroyRelation / still-referenced WARNING).
        cache_insert(relation, true)?;
        Ok(relid)
    } else {
        // C keeps `newrel` OUT of the hash for RelationRebuildRelation's
        // in-place swap; park it in the scratch slot and return its OID.
        finish_uninserted(relation)
    }
}

thread_local! {
    /// Holds the single not-yet-inserted descriptor produced by
    /// `RelationBuildDesc(.., insertIt=false)` for `RelationRebuildRelation`'s
    /// temporary `newrel`. C keeps `newrel` as a local pointer outside the hash;
    /// the owned model parks it here between build and swap. At most one is live.
    static SCRATCH: std::cell::RefCell<Option<Box<RelationData>>> =
        const { std::cell::RefCell::new(None) };
}

/// Park a not-yet-inserted descriptor and return the OID handle naming it. The
/// rebuild path retrieves it with [`take_scratch`].
fn finish_uninserted(relation: Box<RelationData>) -> PgResult<Oid> {
    let id = relation.rd_id;
    SCRATCH.with(|s| *s.borrow_mut() = Some(relation));
    Ok(id)
}

/// Take the parked scratch descriptor (the `newrel` of a rebuild).
pub(crate) fn take_scratch() -> Option<Box<RelationData>> {
    SCRATCH.with(|s| s.borrow_mut().take())
}

/// `ScanPgRelation(targetRelId, indexOK, force_non_historic)` (relcache.c):
/// fetch the `pg_class` heap tuple for `targetRelId`. The scan itself
/// (`table_open` + `systable_beginscan`/`systable_getnext` and the `GETSTRUCT`
/// deform into `Form_pg_class`) is the genuine cross-unit seam (genam owner +
/// the `pg_class` deform primitive); this routine's caller orchestration is own
/// logic. Returns the owned `pg_class` form for the found row plus the verbatim
/// `reloptions` `text[]` varlena bytes (`None` for the C `isnull`; consumed
/// separately by `RelationParseRelOptions`, exactly as C passes the whole
/// `pg_class_tuple` to it), or `None` for the C NULL (no row). Seam-and-panic
/// until the catalog-read owner lands.
pub fn ScanPgRelation(
    targetRelId: Oid,
    indexOK: bool,
    force_non_historic: bool,
) -> PgResult<Option<(FormPgClass, Option<Vec<u8>>)>> {
    // C: must have selected a database before reading pg_class. The owned model
    // surfaces the same guard once the database-id state lands; the catalog read
    // below is the cross-unit primitive that gates this.
    //
    // The `force_non_historic` snapshot toggle (RegisterSnapshot(
    // GetNonHistoricCatalogSnapshot)) is internal to the genam scan the seam
    // performs; the relcache only needs it on the historic-decoding relfilenode
    // re-read path (RelationInitPhysicalAddr). The genam seam's signature does
    // not surface the snapshot toggle yet, so the non-historic case is not
    // distinguished here — it falls back to the catalog-snapshot scan the seam
    // runs. Surface it precisely when that read is genuinely exercised.
    if force_non_historic {
        return Err(ereport(ERROR)
            .errmsg_internal(
                "relcache-build: ScanPgRelation force_non_historic (non-historic \
                 catalog-snapshot pg_class re-read for logical decoding) is not yet \
                 surfaced by the genam scan_pg_class seam",
            )
            .into_error());
    }

    // table_open(RelationRelationId) + systable_beginscan(ClassOidIndexId,
    // oid = targetRelId) + a single systable_getnext + GETSTRUCT(Form_pg_class)
    // deform, copied out into palloc'd storage. The genam owner performs the
    // whole scan-and-decode (forcing a heap scan when the critical relcaches
    // are not yet built or indexOK is false) and returns the decoded row, or
    // None for the C NULL (no matching pg_class tuple).
    let scanned =
        match backend_access_index_genam_seams::scan_pg_class::call(targetRelId, indexOK)? {
            Some(s) => s,
            None => return Ok(None),
        };

    // Marshal the owner-vocabulary ScannedPgClass into the owned FormPgClass
    // mirror (the C "memcpy of CLASS_TUPLE_SIZE into rd_rel"). The variable-
    // length tail columns (relacl/reloptions/relpartbound) are not cached in
    // rd_rel; reloptions is consumed separately by RelationParseRelOptions, so
    // it is returned alongside the form (the C `pg_class_tuple` the relcache
    // hands to RelationParseRelOptions).
    let reloptions = scanned.reloptions.clone();
    Ok(Some((FormPgClass {
        relname: scanned.relname,
        relnamespace: scanned.relnamespace,
        reltype: scanned.reltype,
        reloftype: scanned.reloftype,
        relowner: scanned.relowner,
        relam: scanned.relam,
        relfilenode: scanned.relfilenode,
        reltablespace: scanned.reltablespace,
        relpages: scanned.relpages,
        reltuples: scanned.reltuples,
        relallvisible: scanned.relallvisible,
        reltoastrelid: scanned.reltoastrelid,
        relhasindex: scanned.relhasindex,
        relisshared: scanned.relisshared,
        relpersistence: scanned.relpersistence,
        relkind: scanned.relkind,
        relnatts: scanned.relnatts,
        relchecks: scanned.relchecks,
        relhasrules: scanned.relhasrules,
        relhastriggers: scanned.relhastriggers,
        relhassubclass: scanned.relhassubclass,
        relrowsecurity: scanned.relrowsecurity,
        relforcerowsecurity: scanned.relforcerowsecurity,
        relispopulated: scanned.relispopulated,
        relreplident: scanned.relreplident,
        relispartition: scanned.relispartition,
        relrewrite: scanned.relrewrite,
        relfrozenxid: scanned.relfrozenxid,
        relminmxid: scanned.relminmxid,
    }, reloptions)))
}

/// `AllocateRelationDesc(relp)` (relcache.c): `palloc0` a fresh descriptor and
/// copy the `pg_class` form into `rd_rel`, allocating the template tuple
/// descriptor sized `relnatts`. **Own logic.**
pub fn AllocateRelationDesc(relp: FormPgClass) -> PgResult<Box<RelationData>> {
    // palloc0 the descriptor (every field zero/empty/None, sentinel OIDs).
    let mut relation = RelationData::new_blank();
    // CreateTemplateTupleDesc(relnatts): a blank descriptor with `relnatts`
    // attribute slots. The owned model carries `natts` and an empty attrs Vec
    // that RelationBuildTupleDesc fills.
    let natts = relp.relnatts as i32;
    relation.rd_att = OwnedTupleDesc {
        natts,
        tdtypeid: InvalidOid,
        tdtypmod: -1,
        attrs: Vec::new(),
        constr: None,
    };
    // Copy the pg_class form into rd_rel (C memcpy of CLASS_TUPLE_SIZE).
    relation.rd_rel = relp;
    Ok(relation)
}

/// `RelationBuildTupleDesc(relation)` (relcache.c): build `rd_att` from
/// `pg_attribute` (+ attrdef/notnull constraint fetches). **Own logic**; the
/// `pg_attribute` scan + `GETSTRUCT` deform is the seamed catalog primitive.
pub fn RelationBuildTupleDesc(relation: &mut RelationData) -> PgResult<()> {
    use types_tuple::access::{ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL};
    use types_tuple::heaptuple::{
        ATTNULLABLE_INVALID, ATTNULLABLE_UNKNOWN, ATTNULLABLE_UNRESTRICTED, ATTNULLABLE_VALID,
    };

    // C sets the descriptor's composite type id/typmod first (own logic).
    relation.rd_att.tdtypeid = if relation.rd_rel.reltype != InvalidOid {
        relation.rd_rel.reltype
    } else {
        RECORDOID
    };
    relation.rd_att.tdtypmod = -1;

    // Fresh TupleConstr accounting (C MemoryContextAllocZero(sizeof(TupleConstr))).
    let mut constr = types_relcache_entry::OwnedTupleConstr::default();
    let mut ndef: i32 = 0;

    let relid = relation.rd_id;
    let relname = relation.rd_rel.relname.clone();
    let natts = relation.rd_rel.relnatts as i32;
    let is_catalog =
        backend_catalog_catalog_seams::is_catalog_relation_oid::call(relid);

    // table_open(AttributeRelationId) + systable_beginscan(
    // AttributeRelidNumIndexId, attrelid = relid, attnum > 0) +
    // systable_getnext loop + GETSTRUCT(Form_pg_attribute) deform. The genam
    // owner performs the whole scan-and-decode, returning the user-column rows
    // (attnum > 0); the per-attribute assembly / accounting below is own logic.
    let rows =
        backend_access_index_genam_seams::scan_pg_attribute::call(relid, natts as i16)?;

    // Size the descriptor's attribute array (CreateTemplateTupleDesc(relnatts)
    // gave a blank, zero-length-attrs OwnedTupleDesc in AllocateRelationDesc;
    // fill exactly `natts` slots, then copy each scanned row into its attnum-1
    // slot, exactly as the C memcpy into TupleDescAttr(rd_att, attnum-1)).
    let mut attrs: Vec<OwnedAttr> = vec![OwnedAttr::default(); natts as usize];
    let mut filled = vec![false; natts as usize];
    let mut need = natts;
    // attrmiss = NULL; lazily allocated relnatts-long when the first column with
    // a missing value is seen (C: MemoryContextAllocZero(relnatts *
    // sizeof(AttrMissing))). `None` until then is the C NULL `attrmiss`.
    let mut attrmiss: Option<Vec<types_relcache_entry::OwnedAttrMissing>> = None;

    for attp in &rows {
        let attnum = attp.attnum;
        if attnum <= 0 || attnum as i32 > natts {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "invalid attribute number {} for relation \"{}\"",
                    attp.attnum, relname
                ))
                .into_error());
        }

        // If the column has a "missing" value, put it in the attrmiss array.
        // The genam scan already performed heap_getattr(attmissingval) +
        // array_get_element (extracting element 1 of the single-element array);
        // a present value crosses as a lifetime-free MissingValueImage, `None`
        // for the C `missingNull` (no missing value for this column).
        if attp.atthasmissing {
            if let Some(image) = &attp.attmissingval {
                let arr = attrmiss
                    .get_or_insert_with(|| {
                        vec![
                            types_relcache_entry::OwnedAttrMissing::default();
                            natts as usize
                        ]
                    });
                arr[(attnum - 1) as usize] = types_relcache_entry::OwnedAttrMissing {
                    am_present: true,
                    am_value: Some(image.clone()),
                };
            }
        }

        let idx = (attnum - 1) as usize;
        // Initial attnullability, exactly as populate_compact_attribute derives
        // it: UNRESTRICTED when no not-null; VALID for a catalog (known valid);
        // UNKNOWN otherwise (validity decided later by CheckNNConstraintFetch).
        let attnullability = if !attp.attnotnull {
            ATTNULLABLE_UNRESTRICTED
        } else if is_catalog {
            ATTNULLABLE_VALID
        } else {
            ATTNULLABLE_UNKNOWN
        };
        attrs[idx] = OwnedAttr {
            attname: attp.attname.clone(),
            atttypid: attp.atttypid,
            attlen: attp.attlen,
            attnum,
            atttypmod: attp.atttypmod,
            attbyval: attp.attbyval,
            attalign: attp.attalign,
            attnotnull: attp.attnotnull,
            attidentity: attp.attidentity,
            attgenerated: attp.attgenerated,
            attisdropped: attp.attisdropped,
            attcollation: attp.attcollation,
            attnullability,
        };
        filled[idx] = true;

        // Update constraint/default info (C: GETSTRUCT flags).
        if attp.attnotnull {
            constr.has_not_null = true;
        }
        if attp.attgenerated == ATTRIBUTE_GENERATED_STORED {
            constr.has_generated_stored = true;
        }
        if attp.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            constr.has_generated_virtual = true;
        }
        if attp.atthasdef {
            ndef += 1;
        }

        need -= 1;
        if need == 0 {
            break;
        }
    }

    if need != 0 {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "pg_attribute catalog is missing {} attribute(s) for relation OID {}",
                need, relid
            ))
            .into_error());
    }
    debug_assert!(filled.iter().all(|&f| f));

    relation.rd_att.attrs = attrs;
    // attcacheoff of the first attribute is necessarily zero; the owned model
    // derives attcacheoff at projection time (CreateTupleDesc), so there is no
    // separate field to stamp here (C: TupleDescCompactAttr(rd_att,0)->attcacheoff
    // = 0 is reproduced by the projection's compact-attr derivation).

    // Set up constraint/default info.
    if constr.has_not_null
        || constr.has_generated_stored
        || constr.has_generated_virtual
        || ndef > 0
        || attrmiss.is_some()
        || relation.rd_rel.relchecks > 0
    {
        // Install the constr now so AttrDefaultFetch/CheckNNConstraintFetch
        // (which get_or_insert into rd_att.constr) accumulate into this one.
        relation.rd_att.constr = Some(constr);

        if ndef > 0 {
            AttrDefaultFetch(relation, ndef)?;
        }
        // (C: else constr->num_defval = 0 — the empty Vec already encodes that.)

        // constr->missing = attrmiss; (the empty Vec encodes the C NULL when no
        // column had a missing value, but we only reach here with attrmiss set
        // if attrmiss.is_some()).
        if let Some(arr) = attrmiss {
            if let Some(c) = relation.rd_att.constr.as_mut() {
                c.missing = arr;
            }
        }

        // CHECK and NOT NULLs.
        if relation.rd_rel.relchecks > 0
            || (!is_catalog
                && relation
                    .rd_att
                    .constr
                    .as_ref()
                    .is_some_and(|c| c.has_not_null))
        {
            CheckNNConstraintFetch(relation)?;
        }

        // Any not-null constraint not marked invalid by CheckNNConstraintFetch
        // is necessarily valid; make it so. (C does this on the CompactAttribute
        // array; the owned model carries attnullability per OwnedAttr row.)
        if !is_catalog {
            for a in relation.rd_att.attrs.iter_mut() {
                if a.attnullability == ATTNULLABLE_UNKNOWN {
                    a.attnullability = ATTNULLABLE_VALID;
                } else {
                    debug_assert!(
                        a.attnullability == ATTNULLABLE_INVALID
                            || a.attnullability == ATTNULLABLE_UNRESTRICTED
                    );
                }
            }
        }
        // (C: if relchecks == 0, constr->num_check = 0 — the empty Vec encodes that.)
    } else {
        // No constraints/defaults: rd_att->constr = NULL.
        relation.rd_att.constr = None;
    }

    Ok(())
}

/// `RelationParseRelOptions(relation, tuple)` (relcache.c): parse
/// `pg_class.reloptions` into `rd_options`. **Own logic** is the relkind
/// dispatch + storing the parsed result; the parse itself (`extractRelOptions`,
/// access/common/reloptions.c, deforming the reloptions column and invoking the
/// AM `amoptions`) is the cross-unit primitive.
pub fn RelationParseRelOptions(
    relation: &mut RelationData,
    reloptions: Option<&[u8]>,
) -> PgResult<()> {
    // C resets rd_options to NULL, then dispatches on relkind: tables/views/
    // matviews/toast/partitioned-tables use the generic (NULL amoptions) path;
    // indexes use rd_indam->amoptions; everything else returns with no options.
    relation.rd_options = None;
    let relkind = relation.rd_rel.relkind as u8;
    // amoptsfn: NULL for the table-shaped relkinds; rd_indam->amoptions for the
    // index relkinds; for everything else the C returns with no options.
    let amoptions: Option<Oid> = match relkind {
        RELKIND_RELATION
        | RELKIND_TOASTVALUE
        | RELKIND_VIEW
        | RELKIND_MATVIEW
        | RELKIND_PARTITIONED_TABLE => None,
        // amoptsfn = rd_indam->amoptions — modeled as the index AM's handler OID
        // the am_reloptions seam dispatches on (rd_amhandler).
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => Some(relation.rd_amhandler),
        // Everything else: no options, return.
        _ => return Ok(()),
    };

    // extractRelOptions(tuple, GetPgClassDescriptor(), amoptsfn): parse the
    // pg_class.reloptions text[] (carried alongside the form by ScanPgRelation,
    // the C `pg_class_tuple` argument) into the parsed-options struct. The
    // relkind dispatch + AM amoptions invocation live in the reloptions owner,
    // reached through the extract_rel_options seam. C copies the result into
    // CacheMemoryContext; the owned value is moved into rd_options directly.
    //
    // For the table-shaped relkinds (the user-table path) the owner parses with
    // the in-crate heap/view/partitioned-table parsers (no further seam). For
    // an index whose reloptions are non-NULL the owner drives the AM
    // am_reloptions callback, which is itself genuinely unported (uninstalled
    // amapi seam) — so an index with reloptions set still bottoms out there,
    // precisely.
    relation.rd_options =
        backend_access_common_reloptions_seams::extract_rel_options::call(
            relkind, reloptions, amoptions,
        )?;
    Ok(())
}

/// `formrdesc(relationName, relationReltype, isshared, natts, attrs)`
/// (relcache.c): build a hardcoded bootstrap relcache entry for a nailed
/// system catalog without catalog access, and install it in `RelationIdCache`.
/// **Own logic**; the hardcoded `FormData_pg_attribute` rows are the genbki
/// `Schema_pg_*` arrays carried in `schema` (catalog-header data, plus the
/// catalog relation OID for `rd_id` — the C `attrs[0]->attrelid`).
pub fn formrdesc(
    relationName: &str,
    relationReltype: Oid,
    isshared: bool,
    natts: i32,
    schema: &types_relcache_entry::BootstrapCatalogSchema,
) -> PgResult<Oid> {
    let attrs: &[OwnedAttr] = &schema.attrs;
    // palloc0 the descriptor; nailed, pinned, valid bootstrap entry.
    let mut relation = RelationData::new_blank();
    relation.rd_refcnt = 1;
    relation.rd_isnailed = true;
    relation.rd_createSubid = InvalidSubTransactionId;
    relation.rd_newRelfilelocatorSubid = InvalidSubTransactionId;
    relation.rd_firstRelfilelocatorSubid = InvalidSubTransactionId;
    relation.rd_droppedSubid = InvalidSubTransactionId;
    relation.rd_backend = INVALID_PROC_NUMBER;
    relation.rd_islocaltemp = false;

    // Hardcoded rd_rel (the bootstrap pg_class row).
    relation.rd_rel.relname = relationName.to_string();
    relation.rd_rel.relnamespace = PG_CATALOG_NAMESPACE;
    relation.rd_rel.reltype = relationReltype;
    relation.rd_rel.relisshared = isshared;
    if isshared {
        relation.rd_rel.reltablespace = GLOBALTABLESPACE_OID;
    }
    relation.rd_rel.relpersistence = PERSIST_PERMANENT;
    relation.rd_rel.relispopulated = true;
    relation.rd_rel.relreplident = crate::REPLICA_IDENTITY_NOTHING;
    relation.rd_rel.relpages = 0;
    relation.rd_rel.reltuples = -1.0;
    relation.rd_rel.relallvisible = 0;
    relation.rd_rel.relkind = RELKIND_RELATION as i8;
    relation.rd_rel.relnatts = natts as i16;

    // Build rd_att from the hardcoded attribute rows.
    debug_assert_eq!(attrs.len(), natts as usize);
    let mut has_not_null = false;
    let mut owned_attrs = Vec::with_capacity(natts as usize);
    for a in attrs.iter() {
        has_not_null |= a.attnotnull;
        owned_attrs.push(a.clone());
    }
    relation.rd_att = OwnedTupleDesc {
        natts,
        tdtypeid: relationReltype,
        tdtypmod: -1,
        attrs: owned_attrs,
        constr: None,
    };
    // C sets a TupleConstr{has_not_null} when any column is NOT NULL; the owned
    // entry tracks NOT NULL on each attr row, so the per-row attnotnull above is
    // authoritative (has_not_null retained for faithful structure / asserts).
    let _ = has_not_null;

    // rd_id is the attrelid of the first hardcoded attribute (every Schema_pg_*
    // row carries the catalog's OID in attrelid). The OwnedAttr mirror drops
    // attrelid (it is identical for every row), so the genbki bootstrap-data
    // owner carries it on `BootstrapCatalogSchema.relid`; this is the C
    // `relation->rd_id = attrs[0]->attrelid`.
    relation.rd_id = schema.relid;

    // All relations made with formrdesc are mapped (there is no other way to
    // know their current filenumber). In bootstrap mode, add them to the initial
    // relation mapper data, with the initial filenumber == the OID.
    relation.rd_rel.relfilenode = InvalidOid;
    let is_bootstrap = backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call();
    if is_bootstrap {
        backend_utils_cache_relmapper_seams::relation_map_update_map::call(
            relation.rd_id,
            relation.rd_id,
            isshared,
            true,
        )?;
    }

    // Initialize the relation lock manager information (lmgr.c).
    RelationInitLockInfo(&mut relation);

    // Initialize physical addressing information for the relation.
    crate::index::RelationInitPhysicalAddr(&mut relation)?;

    // Initialize the table AM handler. C sets relam = HEAP_TABLE_AM_OID and
    // rd_tableam = GetHeapamTableAmRoutine() directly. RelationInitTableAccessMethod
    // takes the catalog-relation branch for every formrdesc relation (they are all
    // nailed catalogs), which sets rd_amhandler = F_HEAP_TABLEAM_HANDLER and resolves
    // the heap table-AM vtable — the same const heapam_methods GetHeapamTableAmRoutine
    // returns, without any syscache lookup. Set relam first so that branch's
    // invariant (relam == HEAP_TABLE_AM_OID) holds, mirroring C.
    relation.rd_rel.relam = HEAP_TABLE_AM_OID;
    crate::index::RelationInitTableAccessMethod(&mut relation)?;

    // Initialize the rel-has-index flag, using hardwired knowledge: bootstrap
    // mode has no indexes; otherwise all the rels formrdesc is used for have them.
    relation.rd_rel.relhasindex = !is_bootstrap;

    // It's fully valid. (C sets rd_isvalid = true after RelationCacheInsert on
    // the same pointer; the owned model sets it before the move-into-store, which
    // is equivalent since it is the same descriptor.)
    relation.rd_isvalid = true;

    // Add new reldesc to relcache (RelationCacheInsert(relation, false)).
    let relid = relation.rd_id;
    cache_insert(relation, false)?;
    Ok(relid)
}

/// `HEAP_TABLE_AM_OID` (`pg_am.h`) — the built-in heap table access method,
/// hardcoded for every `formrdesc` nailed catalog (C: `relam = HEAP_TABLE_AM_OID`
/// before `GetHeapamTableAmRoutine()`).
const HEAP_TABLE_AM_OID: Oid = 2;

/// One deformed `pg_attrdef` row for [`AttrDefaultFetch`]: the `adnum` plus the
/// `adbin` default-expression node-tree text (`None` is the C `isnull`). The
/// `TextDatumGetCString` detoast of `adbin` happens behind the scan seam (it is
/// a cross-unit deform); the per-attribute accounting is own logic.
pub(crate) struct ScannedAttrDefault {
    pub adnum: types_core::primitive::AttrNumber,
    pub adbin: Option<String>,
}

/// `AttrDefaultFetch(relation, ndef)` (relcache.c): load column default
/// expressions from `pg_attrdef`. **Own logic** is the accounting/sort/install;
/// the `pg_attrdef` systable scan + `TextDatumGetCString` of the `adbin` node
/// tree is the seamed catalog primitive (`scan_pg_attrdef_seam`).
pub fn AttrDefaultFetch(relation: &mut RelationData, ndef: i32) -> PgResult<()> {
    use crate::core_entry_store::entry::OwnedAttrDefault;

    // Allocate array with room for as many entries as expected (the C
    // MemoryContextAllocZero of `ndef` slots; here a Vec we fill up to `found`).
    let mut attrdef: Vec<OwnedAttrDefault> = Vec::with_capacity(ndef.max(0) as usize);

    let relname = relation.rd_rel.relname.clone();
    // Search pg_attrdef for relevant entries (adrelid = RelationGetRelid). The
    // scan + GETSTRUCT deform + adbin detoast is the cross-unit catalog
    // primitive; it yields the deformed rows. The accounting below is own logic.
    let rows = scan_pg_attrdef_seam(relation.rd_id)?;

    let mut found: i32 = 0;
    for row in &rows {
        // protect limited size of array
        if found >= ndef {
            crate::elog_warning(format!(
                "unexpected pg_attrdef record found for attribute {} of relation \"{}\"",
                row.adnum, relname
            ))?;
            break;
        }

        match &row.adbin {
            None => {
                crate::elog_warning(format!(
                    "null adbin for attribute {} of relation \"{}\"",
                    row.adnum, relname
                ))?;
            }
            Some(s) => {
                attrdef.push(OwnedAttrDefault {
                    adnum: row.adnum,
                    adbin: s.clone(),
                });
                found += 1;
            }
        }
    }

    if found != ndef {
        crate::elog_warning(format!(
            "{} pg_attrdef record(s) missing for relation \"{}\"",
            ndef - found,
            relname
        ))?;
    }

    // Sort the AttrDefault entries by adnum (for equalTupleDescs convenience).
    if found > 1 {
        attrdef.sort_by(|a, b| a.adnum.cmp(&b.adnum));
    }

    // Install array only after it's fully valid: rd_att->constr->defval/num_defval.
    let constr = relation
        .rd_att
        .constr
        .get_or_insert_with(Default::default);
    constr.defval = attrdef;
    // num_defval is the Vec length (`found`); the owned mirror tracks it via len.
    Ok(())
}

/// One deformed `pg_constraint` row for [`CheckNNConstraintFetch`]. Carries the
/// `contype` plus the per-kind fields the routine consumes: for a NOT NULL
/// constraint, `convalidated` + the `extractNotNullColumn` attnum; for a CHECK
/// constraint, the enforced/valid/noinherit flags, the name, and the `conbin`
/// node-tree text (`None` is the C `isnull`). The GETSTRUCT deform,
/// `extractNotNullColumn`, and `TextDatumGetCString` of `conbin` happen behind
/// the scan seam (cross-unit); the accounting is own logic.
pub(crate) struct ScannedConstraint {
    /// `conform->contype` (`CONSTRAINT_NOTNULL`/`CONSTRAINT_CHECK`/other).
    pub contype: i8,
    /// NOT NULL only: `!conform->convalidated`.
    pub notnull_invalid: bool,
    /// NOT NULL only: `extractNotNullColumn(htup)`.
    pub notnull_attnum: types_core::primitive::AttrNumber,
    /// CHECK only: `conform->conenforced`.
    pub ccenforced: bool,
    /// CHECK only: `conform->convalidated`.
    pub ccvalid: bool,
    /// CHECK only: `conform->connoinherit`.
    pub ccnoinherit: bool,
    /// CHECK only: `NameStr(conform->conname)`.
    pub ccname: String,
    /// CHECK only: `conbin` cstring, or `None` for the C `isnull`.
    pub ccbin: Option<String>,
}

/// `CONSTRAINT_CHECK` (`catalog/pg_constraint.h`).
const CONSTRAINT_CHECK: i8 = b'c' as i8;
/// `CONSTRAINT_NOTNULL` (`catalog/pg_constraint.h`).
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;

/// `CheckNNConstraintFetch(relation)` (relcache.c): load check constraints and
/// update not-null validity of invalid constraints, from `pg_constraint`.
/// **Own logic** is the accounting/sort/install + the not-null attnullability
/// fixup; the `pg_constraint` systable scan + `extractNotNullColumn` +
/// `TextDatumGetCString` of `conbin` is the seamed catalog primitive
/// (`scan_pg_constraint_nncheck_seam`).
pub fn CheckNNConstraintFetch(relation: &mut RelationData) -> PgResult<()> {
    use crate::core_entry_store::entry::OwnedConstrCheck;
    use types_tuple::heaptuple::ATTNULLABLE_UNKNOWN;

    let ncheck = relation.rd_rel.relchecks as i32;
    let relname = relation.rd_rel.relname.clone();

    // Allocate array with room for as many entries as expected, if needed.
    let mut check: Vec<OwnedConstrCheck> = Vec::with_capacity(ncheck.max(0) as usize);

    // Search pg_constraint for relevant entries (conrelid = RelationGetRelid).
    // The scan + GETSTRUCT deform + extractNotNullColumn + conbin detoast is the
    // cross-unit catalog primitive; accounting below is own logic.
    let rows = scan_pg_constraint_nncheck_seam(relation.rd_id)?;

    let mut found: i32 = 0;
    for row in &rows {
        // If this is a not-null constraint, only look at it if it's invalid,
        // and if so mark the TupleDesc entry as known invalid. Otherwise move
        // on. Remaining UNKNOWN columns are marked known-valid later.
        if row.contype == CONSTRAINT_NOTNULL {
            if row.notnull_invalid {
                let attnum = row.notnull_attnum;
                let idx = (attnum - 1) as usize;
                debug_assert!(
                    relation.rd_att.attrs[idx].attnullability == ATTNULLABLE_UNKNOWN
                );
                relation.rd_att.attrs[idx].attnullability =
                    types_tuple::heaptuple::ATTNULLABLE_INVALID;
            }
            continue;
        }

        // For what follows, consider check constraints only.
        if row.contype != CONSTRAINT_CHECK {
            continue;
        }

        // protect limited size of array
        if found >= ncheck {
            crate::elog_warning(format!(
                "unexpected pg_constraint record found for relation \"{relname}\""
            ))?;
            break;
        }

        // Grab and test conbin is actually set.
        match &row.ccbin {
            None => {
                crate::elog_warning(format!("null conbin for relation \"{relname}\""))?;
            }
            Some(s) => {
                check.push(OwnedConstrCheck {
                    ccname: row.ccname.clone(),
                    ccbin: s.clone(),
                    ccenforced: row.ccenforced,
                    ccvalid: row.ccvalid,
                    ccnoinherit: row.ccnoinherit,
                });
                found += 1;
            }
        }
    }

    if found != ncheck {
        crate::elog_warning(format!(
            "{} pg_constraint record(s) missing for relation \"{relname}\"",
            ncheck - found
        ))?;
    }

    // Sort the records by name (deterministic CHECK order + faster
    // equalTupleDescs). C uses strcmp; Rust String Ord is the same byte order.
    if found > 1 {
        check.sort_by(|a, b| a.ccname.cmp(&b.ccname));
    }

    // Install array only after it's fully valid: rd_att->constr->check/num_check.
    let constr = relation
        .rd_att
        .constr
        .get_or_insert_with(Default::default);
    constr.check = check;
    Ok(())
}

/// `systable_beginscan(pg_attrdef, adrelid = relid)` + per-row GETSTRUCT deform
/// of `Form_pg_attrdef` and `TextDatumGetCString(adbin)`. The `table_open` of
/// pg_attrdef, the genam scan, and the `adbin` text detoast are genuine
/// cross-unit primitives (genam owner + heap deform). Seam-and-panic until the
/// owner lands; the [`AttrDefaultFetch`] accounting around it is own logic.
fn scan_pg_attrdef_seam(relid: Oid) -> PgResult<Vec<ScannedAttrDefault>> {
    use backend_access_index_genam_seams as genam_seam;

    // The `table_open(AttrDefaultRelationId)`, the
    // `systable_beginscan(adrelid = relid)`, the per-row
    // `GETSTRUCT(Form_pg_attrdef)` deform, and the `adbin`
    // `TextDatumGetCString` detoast are the genam owner's primitive; it returns
    // the deformed rows. Marshal each into the crate-local accounting carrier.
    let rows = genam_seam::scan_pg_attrdef::call(relid)?;
    Ok(rows
        .into_iter()
        .map(|r| ScannedAttrDefault {
            adnum: r.adnum,
            adbin: r.adbin,
        })
        .collect())
}

/// `systable_beginscan(pg_constraint, conrelid = relid)` + per-row GETSTRUCT
/// deform of `Form_pg_constraint`, `extractNotNullColumn(htup)` for NOT NULL
/// rows, and `TextDatumGetCString(conbin)` for CHECK rows. The `table_open` of
/// pg_constraint, the genam scan, the not-null-column extraction, and the
/// `conbin` text detoast are genuine cross-unit primitives. Seam-and-panic
/// until the owner lands; the [`CheckNNConstraintFetch`] accounting around it
/// is own logic.
fn scan_pg_constraint_nncheck_seam(relid: Oid) -> PgResult<Vec<ScannedConstraint>> {
    use backend_access_index_genam_seams as genam_seam;

    // The `table_open(ConstraintRelationId)`, the
    // `systable_beginscan(conrelid = relid)`, the per-row
    // `GETSTRUCT(Form_pg_constraint)` deform, `extractNotNullColumn(htup)` for
    // NOT NULL rows, and the `conbin` `TextDatumGetCString` detoast for CHECK
    // rows are the genam owner's primitive; it returns the deformed rows.
    // Marshal each into the crate-local accounting carrier.
    let rows = genam_seam::scan_pg_constraint_nncheck::call(relid)?;
    Ok(rows
        .into_iter()
        .map(|r| ScannedConstraint {
            contype: r.contype,
            notnull_invalid: r.notnull_invalid,
            notnull_attnum: r.notnull_attnum,
            ccenforced: r.ccenforced,
            ccvalid: r.ccvalid,
            ccnoinherit: r.ccnoinherit,
            ccname: r.ccname,
            ccbin: r.ccbin,
        })
        .collect())
}

/// `RelationInitLockInfo(relation)` (relcache.c): fill `rd_lockInfo.lockRelId`
/// from the relation's OID and database (`InvalidOid` for shared relations).
/// **Own logic.**
fn RelationInitLockInfo(relation: &mut RelationData) {
    relation.rd_lockInfo.lockRelId.relId = relation.rd_id;
    // C: lockRelId.dbId = relisshared ? InvalidOid : MyDatabaseId. The
    // MyDatabaseId backend-state read lands with the init/postinit owner; for a
    // shared relation it is unconditionally InvalidOid.
    relation.rd_lockInfo.lockRelId.dbId = if relation.rd_rel.relisshared {
        InvalidOid
    } else {
        // MyDatabaseId: filled by the owner; InvalidOid until then (a non-shared
        // relation's dbId is a backend-state read, not catalog data).
        InvalidOid
    };
}
