#![allow(non_upper_case_globals)]
// ExecuteTruncate/ExecuteTruncateGuts, single-lane: permanent plain tables,
// RESTRICT, no RESTART IDENTITY. RelationSetNewRelfilenumber (relcache.c) is
// hosted here: relcache cannot dep catalog_storage/tableam/catalog_indexing
// without cycling, and this is its only caller.
use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidBlockNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::parsenodes::{DropBehavior, ObjectType, TruncateStmt};
use types_rel::{
    AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::unported;

const Natts_pg_class: usize = 34;
const Anum_pg_class_relfilenode: usize = 8;
const Anum_pg_class_relpages: usize = 10;
const Anum_pg_class_reltuples: usize = 11;
const Anum_pg_class_relallvisible: usize = 12;
const Anum_pg_class_relallfrozen: usize = 13;
const Anum_pg_class_relpersistence: usize = 17;
const Anum_pg_class_relfrozenxid: usize = 30;
const Anum_pg_class_relminmxid: usize = 31;

fn oid_key(attno: AttrNumber, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

pub fn ExecuteTruncate<'mcx>(mcx: Mcx<'mcx>, stmt: &TruncateStmt<'mcx>) -> PgResult<()> {
    if stmt.restart_seqs {
        unported("ExecuteTruncate: RESTART IDENTITY");
    }
    if stmt.behavior == DropBehavior::DROP_CASCADE {
        unported("ExecuteTruncate: CASCADE");
    }

    let mut rels: Vec<Relation<'mcx>> = Vec::new();
    let mut relids: Vec<Oid> = Vec::new();

    for cell in stmt.relations.iter() {
        let rv = cell.as_range_var().expect("TRUNCATE target is a RangeVar");
        let rv = rel_vocab::RangeVar {
            catalogname: rv.catalogname,
            schemaname: rv.schemaname,
            relname: rv.relname.expect("relation_expr always carries relname"),
            inh: rv.inh,
            relpersistence: rv.relpersistence,
            location: rv.location,
        };

        let mut callback = |_rv: &rel_vocab::RangeVar<'_>, relOid: Oid, _old: Oid| {
            RangeVarCallbackForTruncate(mcx, relOid)
        };
        let myrelid = catalog_namespace::RangeVarGetRelidExtended(
            &rv,
            AccessExclusiveLock,
            0,
            Some(&mut callback),
        )?;

        if relids.contains(&myrelid) {
            continue;
        }
        let rel = table::table_open(mcx, myrelid, NoLock)?;
        truncate_check_activity(&rel)?;

        if rv.inh && rel.rd_rel.relhassubclass {
            unported("ExecuteTruncate: find_all_inheritors (inheritance/partition lane)");
        }
        if !rv.inh && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            return Err(Box::new(
                PgError::new(ERROR, "cannot truncate only a partitioned table".to_string())
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        rels.push(rel);
        relids.push(myrelid);
    }

    ExecuteTruncateGuts(mcx, &rels)?;

    for rel in rels {
        rel.close(NoLock)?;
    }
    Ok(())
}

fn ExecuteTruncateGuts<'mcx>(mcx: Mcx<'mcx>, rels: &[Relation<'mcx>]) -> PgResult<()> {
    let my_subid = xact::GetCurrentSubTransactionId();
    for rel in rels {
        if rel.rd_createSubid.get() == my_subid
            || rel.rd_newRelfilelocatorSubid.get() == my_subid
        {
            tableam::table_relation_nontransactional_truncate(rel)?;
        } else {
            predicate_seams::check_for_serializable_conflict_in::call(
                rel,
                None,
                InvalidBlockNumber,
            )?;
            RelationSetNewRelfilenumber(mcx, rel, rel.rd_rel.relpersistence)?;

            let toast_relid = rel.rd_rel.reltoastrelid;
            if toast_relid != InvalidOid {
                let toastrel = table::table_open(mcx, toast_relid, AccessExclusiveLock)?;
                RelationSetNewRelfilenumber(mcx, &toastrel, toastrel.rd_rel.relpersistence)?;
                toastrel.close(NoLock)?;
            }

            reindex_relation_guard(mcx, rel.rd_id, toast_relid)?;
        }
        pgstat::relation::pgstat_count_truncate(rel.rd_id, rel.rd_rel.relisshared);
    }
    // XLOG_HEAP_TRUNCATE rides wal_level=logical, const-false here as in the
    // visibilitymap catalog-rel gate.
    Ok(())
}

// reindex_relation (index.c) is the catalog-index lane; until it lands any
// index on the truncated rel (incl. the toast index) must be loud, never a
// silently stale index.
fn reindex_relation_guard<'mcx>(mcx: Mcx<'mcx>, relid: Oid, toast_relid: Oid) -> PgResult<()> {
    if !relcache_seams::relation_get_index_list::call(mcx, relid)?.is_empty() {
        unported("ExecuteTruncateGuts: reindex_relation (catalog-index lane)");
    }
    if toast_relid != InvalidOid
        && !relcache_seams::relation_get_index_list::call(mcx, toast_relid)?.is_empty()
    {
        unported("ExecuteTruncateGuts: reindex_relation over the toast index (catalog-index lane)");
    }
    Ok(())
}

fn RangeVarCallbackForTruncate<'mcx>(mcx: Mcx<'mcx>, relOid: Oid) -> PgResult<()> {
    if relOid == InvalidOid {
        return Ok(());
    }
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = [oid_key(1, relOid)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relOid}"));
    let desc = pg_class.descr();
    let get = |attnum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_class columns under pg_class's descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };
    let relname: String = {
        // SAFETY: relname is a NameData at attnum 2 in every pg_class row.
        let name = unsafe { &*(get(2).as_usize() as *const types_tuple::NameData) };
        String::from_utf8_lossy(name.name_str()).into_owned()
    };
    let relnamespace = get(3).as_oid();
    let relkind = get(18).as_i8() as u8;
    let relhastriggers = get(22).as_bool();
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;

    // heap_truncate_check_FKs + TRUNCATE triggers (C checks these in the
    // guts): FK constraints and triggers both flip relhastriggers, the only
    // way either can exist today.
    if relhastriggers {
        unported("ExecuteTruncateGuts: FK checks / TRUNCATE triggers (trigger lane)");
    }

    truncate_check_rel(relOid, relkind, relnamespace, &relname)?;
    truncate_check_perms(relOid, relkind, &relname)
}

fn truncate_check_rel(relid: Oid, relkind: u8, relnamespace: Oid, relname: &str) -> PgResult<()> {
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(Box::new(
            PgError::new(ERROR, format!("\"{relname}\" is not a table"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    let is_system =
        catalog::IsCatalogRelationOid(relid) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied: \"{relname}\" is a system catalog"),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn truncate_check_perms(relid: Oid, relkind: u8, relname: &str) -> PgResult<()> {
    let aclresult =
        aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), adt_acl::ACL_TRUNCATE)?;
    if aclresult != aclchk::ACLCHECK_OK {
        let _ = relkind; // get_relkind_objtype: both reachable relkinds map to OBJECT_TABLE
        aclchk_seams::aclcheck_error::call(aclresult, ObjectType::OBJECT_TABLE as i32, relname)?;
    }
    Ok(())
}

fn truncate_check_activity(rel: &Relation<'_>) -> PgResult<()> {
    if rel.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP {
        unported("truncate_check_activity: temp tables");
    }
    catalog_heap::CheckTableNotInUse(rel, "TRUNCATE")
}

// RelationSetNewRelfilenumber (relcache.c). The catalog write is the
// unlocked-tuple shape every catalog updater here uses (no
// InplaceUpdateTupleLock; that divergence rides repo-wide). The subid Cells
// are set before CommandCounterIncrement so the inval rebuild's
// copy_preserved carries them onto the rebuilt entry.
pub(crate) fn RelationSetNewRelfilenumber<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    persistence: u8,
) -> PgResult<()> {
    if rel.is_mapped() {
        unported("RelationSetNewRelfilenumber: mapped relations");
    }
    let newrelfilenumber =
        catalog::GetNewRelFileNumber(mcx, rel.rd_rel.reltablespace, None, persistence)?;

    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = [oid_key(1, rel.rd_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &key)?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("could not find tuple for relation {}", rel.rd_id));

    catalog_storage::RelationDropStorage(rel)?;

    let mut newrlocator = rel.rd_locator.get();
    newrlocator.relNumber = newrelfilenumber;
    let (freeze_xid, minmulti) =
        tableam::table_relation_set_new_filelocator(rel, &newrlocator, persistence as i8)?;

    let mut values = [Datum::null(); Natts_pg_class];
    let isnull = [false; Natts_pg_class];
    let mut replace = [false; Natts_pg_class];
    let mut set = |anum: usize, d: Datum| {
        values[anum - 1] = d;
        replace[anum - 1] = true;
    };
    set(Anum_pg_class_relfilenode, Datum::from_oid(newrelfilenumber));
    set(Anum_pg_class_relpages, Datum::from_i32(0));
    set(Anum_pg_class_reltuples, Datum::from_f32(-1.0));
    set(Anum_pg_class_relallvisible, Datum::from_i32(0));
    set(Anum_pg_class_relallfrozen, Datum::from_i32(0));
    set(Anum_pg_class_relfrozenxid, Datum::from_transaction_id(freeze_xid));
    set(Anum_pg_class_relminmxid, Datum::from_transaction_id(minmulti));
    set(Anum_pg_class_relpersistence, Datum::from_char(persistence as i8));
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, reltup, pg_class.descr(), &values, &isnull, &replace)?;
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_class, &otid, &mut newtup)?;
    pg_class.close(RowExclusiveLock)?;

    // RelationAssumeNewRelfilelocator + the physical-addr refresh the C
    // in-place rebuild would perform on this same entry.
    rel.rd_locator.set(newrlocator);
    let subid = xact::GetCurrentSubTransactionId();
    rel.rd_newRelfilelocatorSubid.set(subid);
    if rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId {
        rel.rd_firstRelfilelocatorSubid.set(subid);
    }

    xact::CommandCounterIncrement()
}
