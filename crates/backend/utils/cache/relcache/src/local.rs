// RelationBuildLocalRelation (relcache.c): relcache entry for a relation
// whose pg_class row does not exist yet.
use core::cell::Cell;
use std::rc::Rc;

use mcx::PgVec;
use types_core::{
    InvalidOid, Oid, RelFileNumber, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_error::PgResult;
use types_rel::{FormData_pg_class, RelationData, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT, REPLICA_IDENTITY_NOTHING};
use types_tuple::{NameData, TupleConstr, TupleDescData};

use crate::build::{RelationInitPhysicalAddr, RelationInitTableAccessMethod};
use crate::{cache_mcx, store};

// reltype is a build-time parameter (C's AddNewRelationTuple pokes it into
// rd_rel/rd_att after the fact; rd_rel is not interior-mutable here).
pub fn RelationBuildLocalRelation(
    relname: &str,
    relnamespace: Oid,
    tupDesc: &TupleDescData<'_>,
    relid: Oid,
    reltype: Oid,
    accessmtd: Oid,
    relfilenumber: RelFileNumber,
    reltablespace: Oid,
    shared_relation: bool,
    mapped_relation: bool,
    relpersistence: u8,
    relkind: u8,
) -> PgResult<Rc<RelationData<'static>>> {
    if shared_relation {
        panic!(
            "RelationBuildLocalRelation (relcache.c): shared relation \
             creation unported (relid {relid})"
        );
    }
    let mcx = cache_mcx();
    let mut rd_att = tupdesc::CreateTupleDescCopy(mcx, tupDesc)?;
    rd_att.tdrefcount = 1;
    rd_att.tdtypeid = if reltype != InvalidOid { reltype } else { types_core::RECORDOID };
    rd_att.tdtypmod = -1;
    let mut has_not_null = false;
    for i in 0..rd_att.natts as usize {
        let src = &tupDesc.attrs[i];
        let (notnull, identity, generated) = (src.attnotnull, src.attidentity, src.attgenerated);
        let dst = rd_att.attr_mut(i);
        dst.attnotnull = notnull;
        dst.attidentity = identity;
        dst.attgenerated = generated;
        has_not_null |= notnull;
        tupdesc::populate_compact_attribute(&mut rd_att, i);
    }
    if has_not_null {
        rd_att.constr = Some(mcx::box_new_in(
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

    let (rd_backend, rd_islocaltemp) = match relpersistence {
        RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => (INVALID_PROC_NUMBER, false),
        RELPERSISTENCE_TEMP => {
            debug_assert!(namespace_seams::is_temp_or_temp_toast_namespace::call(relnamespace));
            (init_small::globals::ProcNumberForTempRelations(), true)
        }
        _ => panic!(
            "RelationBuildLocalRelation (relcache.c): relpersistence {:?} invalid",
            relpersistence as char
        ),
    };

    let mut name = NameData::default();
    name.namestrcpy(relname);
    let rd_rel = FormData_pg_class {
        relname: name,
        relnamespace,
        reltype,
        relowner: InvalidOid,
        relam: accessmtd,
        // Mapped relations keep relfilenode 0; RelationInitPhysicalAddr
        // consults the map (relcache.c RelationBuildLocalRelation).
        relfilenode: if mapped_relation { types_core::InvalidRelFileNumber } else { relfilenumber },
        reltablespace,
        // C's palloc0 leaves relpages/reltuples/relallvisible zero; the pokes
        // in AddNewRelationTuple (tables: reltuples -1) happen at insert time.
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        reltoastrelid: InvalidOid,
        relhasindex: false,
        relisshared: false,
        relpersistence,
        relkind,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: relkind != RELKIND_MATVIEW,
        // IsCatalogNamespace || IsToastNamespace (catalog.c): PG_CATALOG(11)/pg_toast(99).
        relreplident: if !(relnamespace == 11 || relnamespace == 99)
            && matches!(
                relkind,
                RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_PARTITIONED_TABLE
            ) {
            REPLICA_IDENTITY_DEFAULT
        } else {
            REPLICA_IDENTITY_NOTHING
        },
        relispartition: false,
        relfrozenxid: 0,
        relminmxid: 0,
    };

    let subid = xact_seams::get_current_sub_transaction_id::call();
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: relid,
        rd_backend,
        rd_islocaltemp,
        rd_isvalid: Cell::new(false),
        rd_createSubid: Cell::new(subid),
        rd_newRelfilelocatorSubid: Cell::new(subid),
        rd_firstRelfilelocatorSubid: Cell::new(subid),
        rd_droppedSubid: Cell::new(types_core::InvalidSubTransactionId),
        rd_lockInfo: lmgr::RelationInitLockInfo(relid, false),
        rd_rel,
        rd_att: Rc::new(rd_att),
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
        rd_supportinfo: core::cell::RefCell::new(Vec::new()),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    if mapped_relation {
        relmapper_seams::relation_map_update_map::call(
            relid,
            relfilenumber,
            shared_relation,
            true,
        )?;
    }
    RelationInitPhysicalAddr(&data)?;
    RelationInitTableAccessMethod(relkind, accessmtd)?;

    let rel = Rc::new(data);
    store::insert(Rc::clone(&rel), false, false)?;
    store::eoxact_list_add(relid);
    rel.rd_isvalid.set(true);
    Ok(rel)
}
