// cluster.c ALTER TABLE rewrite slice: make_new_heap / swap_relation_files /
// finish_heap_swap for plain unindexed heap tables (toast rides the link
// swap). LOUD: CLUSTER/VACUUM FULL entry points, mapped relations, user
// index rebuilds (reindex_relation), swap-by-content, reloptions.
#![allow(non_snake_case, non_upper_case_globals)]

mod command;
mod copy;
pub use command::{
    check_index_is_clusterable, cluster, cluster_rel, init_seams, mark_index_clustered,
    ClusterParams, CLUOPT_RECHECK, CLUOPT_RECHECK_ISCLUSTERED, CLUOPT_VERBOSE,
};

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::PgResult;
use types_rel::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const Anum_pg_class_relam: usize = 7;
const Anum_pg_class_relfilenode: usize = 8;
const Anum_pg_class_reltablespace: usize = 9;
const Anum_pg_class_relpages: usize = 10;
const Anum_pg_class_reltuples: usize = 11;
const Anum_pg_class_relallvisible: usize = 12;
const Anum_pg_class_relallfrozen: usize = 13;
const Anum_pg_class_reltoastrelid: usize = 14;
const Anum_pg_class_relpersistence: usize = 17;
const Anum_pg_class_relrewrite: usize = 29;
const Anum_pg_class_relfrozenxid: usize = 30;
const Anum_pg_class_relminmxid: usize = 31;

fn oid_key(attno: usize, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: cluster {what}")
}

pub fn make_new_heap<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap_oid: Oid,
    new_tablespace: Oid,
    persistence: u8,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let old_heap = table::table_open(mcx, old_heap_oid, lockmode)?;
    if persistence == types_core::RELPERSISTENCE_TEMP {
        unported("make_new_heap temp-namespace lookup");
    }
    // rd_options reloptions are not copied by our heap_create_with_catalog.
    if old_heap.rd_options.is_some() {
        unported("make_new_heap: reloptions copy");
    }
    let namespaceid = old_heap.rd_rel.relnamespace;
    let new_heap_name = format!("pg_temp_{old_heap_oid}");

    let oid_new_heap = catalog_heap::heap_create_with_catalog(
        mcx,
        &catalog_heap::HeapCreateParams {
            relname: &new_heap_name,
            relnamespace: namespaceid,
            reltablespace: new_tablespace,
            ownerid: old_heap.rd_rel.relowner,
            accessmtd: old_heap.rd_rel.relam,
            relkind: types_rel::RELKIND_RELATION,
            relpersistence: persistence,
            allow_system_table_mods: true,
            reloptions: None,
        },
        &old_heap.rd_att,
    )?;

    xact::CommandCounterIncrement()?;
    // C threads relrewrite through heap_create_with_catalog; setting it on the
    // now-visible row is the same catalog end-state.
    set_relrewrite(mcx, oid_new_heap, old_heap_oid)?;
    xact::CommandCounterIncrement()?;

    if old_heap.rd_rel.reltoastrelid != InvalidOid {
        // C creates the new toast with the old toast's reloptions and
        // relrewrite = old toast oid; relrewrite is reset to 0 at swap end
        // either way (single-backend: mid-xact catalog state only).
        // C reads toast reloptions via syscache, lock-free; this open needs a
        // lock only for the strict-open assert (parent is exclusively locked).
        let old_toast = table::table_open(mcx, old_heap.rd_rel.reltoastrelid, AccessShareLock)?;
        let has_opts = old_toast.rd_options.is_some();
        old_toast.close(AccessShareLock)?;
        if has_opts {
            unported("make_new_heap: toast reloptions copy");
        }
        catalog_toasting::NewRelationCreateToastTable(mcx, oid_new_heap, None)?;
    }
    old_heap.close(NoLock)?;
    Ok(oid_new_heap)
}

// finish_heap_swap. ALTER TABLE rewrites pass frozen_xid = RecentXmin and
// cutoff_multi = ReadNextMultiXactId (ATRewriteTables' choice); CLUSTER
// passes copy_table_data's cutoffs.
#[allow(clippy::too_many_arguments)]
pub fn finish_heap_swap<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap_oid: Oid,
    new_heap_oid: Oid,
    is_system_catalog: bool,
    swap_toast_by_content: bool,
    check_constraints: bool,
    _is_internal: bool,
    frozen_xid: types_core::primitive::TransactionId,
    cutoff_multi: types_core::primitive::MultiXactId,
    newrelpersistence: u8,
) -> PgResult<()> {
    if swap_toast_by_content {
        unported("finish_heap_swap: swap_toast_by_content (system-catalog rewrite lane)");
    }
    let (toast1, toast2) =
        swap_relation_files(mcx, old_heap_oid, new_heap_oid, frozen_xid, cutoff_multi)?;

    if is_system_catalog {
        inval::invalidate::CacheInvalidateCatalog(old_heap_oid)?;
    }

    {
        let mut reindex_flags = catalog_index::REINDEX_REL_SUPPRESS_INDEX_USE;
        if check_constraints {
            reindex_flags |= catalog_index::REINDEX_REL_CHECK_CONSTRAINTS;
        }
        if newrelpersistence == types_core::RELPERSISTENCE_UNLOGGED {
            reindex_flags |= catalog_index::REINDEX_REL_FORCE_INDEXES_UNLOGGED;
        } else if newrelpersistence == types_core::catalog::RELPERSISTENCE_PERMANENT {
            reindex_flags |= catalog_index::REINDEX_REL_FORCE_INDEXES_PERMANENT;
        }
        let rebuilt = catalog_index::reindex_relation(
            mcx,
            old_heap_oid,
            reindex_flags,
            &catalog_index::ReindexParams::default(),
        )?;
        if !rebuilt {
            // reindex_relation's trailing CCI (it ran none without indexes):
            // the swap's pg_class/pg_depend writes must be visible to the
            // deletion traversal below.
            xact::CommandCounterIncrement()?;
        }
    }

    debug_assert!(old_heap_oid != RELATION_RELATION_ID);

    let object = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, new_heap_oid);
    catalog_dependency::performDeletion(
        mcx,
        &object,
        types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
        catalog_dependency::PERFORM_DELETION_INTERNAL,
    )?;

    // Toast-by-links rename: the surviving toast (swapped onto the old heap)
    // carries the transient name; rename it and its index, reset relrewrite.
    let _ = toast2;
    if toast1 != InvalidOid || toast2 != InvalidOid {
        let newrel = table::table_open(mcx, old_heap_oid, NoLock)?;
        let cur_toast = newrel.rd_rel.reltoastrelid;
        newrel.close(NoLock)?;
        if cur_toast != InvalidOid {
            let toastidx = {
                let toastrel = table::table_open(mcx, cur_toast, AccessShareLock)?;
                let idxs = relcache::RelationGetIndexList(mcx, cur_toast)?;
                toastrel.close(AccessShareLock)?;
                assert!(idxs.len() == 1, "toast table with {} indexes", idxs.len());
                idxs[0]
            };
            tablecmds_rename_seam(mcx, cur_toast, &format!("pg_toast_{old_heap_oid}"), false)?;
            tablecmds_rename_seam(
                mcx,
                toastidx,
                &format!("pg_toast_{old_heap_oid}_index"),
                true,
            )?;
            xact::CommandCounterIncrement()?;
            set_relrewrite(mcx, cur_toast, InvalidOid)?;
        }
    }

    if !is_system_catalog {
        catalog_heap::RelationClearMissing(mcx, old_heap_oid)?;
    }
    Ok(())
}

// RenameRelationInternal lives in tablecmds, which depends on this crate; the
// call is marshalled through cluster_seams to break the cycle.
fn tablecmds_rename_seam<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    newname: &str,
    is_index: bool,
) -> PgResult<()> {
    tablecmds_seams::rename_relation_internal::call(mcx, relid, newname, is_index)
}

// swap_relation_files, non-mapped arm; returns both reltoastrelid values as
// seen before the swap.
fn swap_relation_files<'mcx>(
    mcx: Mcx<'mcx>,
    r1: Oid,
    r2: Oid,
    frozen_xid: types_core::primitive::TransactionId,
    cutoff_multi: types_core::primitive::MultiXactId,
) -> PgResult<(Oid, Oid)> {
    let rel_relation = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let desc = rel_relation.descr();
    let natts = desc.natts as usize;

    struct Row<'mcx> {
        tid: types_tuple::ItemPointerData,
        vals: PgVec<'mcx, (usize, Datum)>,
        relfilenode: Oid,
        reltablespace: Oid,
        relam: Oid,
        relpersistence: i8,
        reltoastrelid: Oid,
        relpages: Datum,
        reltuples: Datum,
        relallvisible: Datum,
        relallfrozen: Datum,
    }

    let read_row = |relid: Oid| -> PgResult<Row<'mcx>> {
        let key = oid_key(1, relid);
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel_relation,
            catalog::ClassOidIndexId,
            true,
            None,
            &[key],
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
        let get = |anum: usize| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_class columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, anum as i32, desc, &mut isnull) }
        };
        let row = Row {
            tid: tup.t_self,
            vals: PgVec::new_in(mcx),
            relfilenode: get(Anum_pg_class_relfilenode).as_oid(),
            reltablespace: get(Anum_pg_class_reltablespace).as_oid(),
            relam: get(Anum_pg_class_relam).as_oid(),
            relpersistence: get(Anum_pg_class_relpersistence).as_i8(),
            reltoastrelid: get(Anum_pg_class_reltoastrelid).as_oid(),
            relpages: get(Anum_pg_class_relpages),
            reltuples: get(Anum_pg_class_reltuples),
            relallvisible: get(Anum_pg_class_relallvisible),
            relallfrozen: get(Anum_pg_class_relallfrozen),
        };
        genam::systable_endscan(mcx, scan)?;
        Ok(row)
    };

    let mut row1 = read_row(r1)?;
    let mut row2 = read_row(r2)?;
    assert!(
        row1.relfilenode != InvalidOid && row2.relfilenode != InvalidOid,
        "unported: swap_relation_files mapped relations"
    );
    assert!(row1.relam == row2.relam);

    // C's rd_createSubid/rd_*RelfilelocatorSubid transfer +
    // RelationAssumeNewRelfilelocator(rel1) (cluster.c:1188-1205) is not
    // ported: heapam never WAL-skips permanent rels and bulkwrite/nbtree
    // smgrimmedsync eagerly, so no deferred pendingSyncs read those fields.
    // Load-bearing the day a WAL-skip (wal_level=minimal) lane lands.
    for pair in [
        (Anum_pg_class_relfilenode, Datum::from_oid(row2.relfilenode)),
        (Anum_pg_class_reltablespace, Datum::from_oid(row2.reltablespace)),
        (Anum_pg_class_relpersistence, Datum::from_i8(row2.relpersistence)),
        (Anum_pg_class_reltoastrelid, Datum::from_oid(row2.reltoastrelid)),
        (Anum_pg_class_relfrozenxid, Datum::from_transaction_id(frozen_xid)),
        (Anum_pg_class_relminmxid, Datum::from_u32(cutoff_multi)),
        (Anum_pg_class_relpages, row2.relpages),
        (Anum_pg_class_reltuples, row2.reltuples),
        (Anum_pg_class_relallvisible, row2.relallvisible),
        (Anum_pg_class_relallfrozen, row2.relallfrozen),
    ] {
        row1.vals.push(pair);
    }
    for pair in [
        (Anum_pg_class_relfilenode, Datum::from_oid(row1.relfilenode)),
        (Anum_pg_class_reltablespace, Datum::from_oid(row1.reltablespace)),
        (Anum_pg_class_relpersistence, Datum::from_i8(row1.relpersistence)),
        (Anum_pg_class_reltoastrelid, Datum::from_oid(row1.reltoastrelid)),
        (Anum_pg_class_relpages, row1.relpages),
        (Anum_pg_class_reltuples, row1.reltuples),
        (Anum_pg_class_relallvisible, row1.relallvisible),
        (Anum_pg_class_relallfrozen, row1.relallfrozen),
    ] {
        row2.vals.push(pair);
    }

    for (relid, row) in [(r1, &row1), (r2, &row2)] {
        let key = oid_key(1, relid);
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel_relation,
            catalog::ClassOidIndexId,
            true,
            None,
            &[key],
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        for &(anum, v) in &row.vals {
            repl_values[anum - 1] = v;
            repl[anum - 1] = true;
        }
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
        let otid = row.tid;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel_relation, &otid, &mut newtup)?;
    }
    rel_relation.close(RowExclusiveLock)?;

    // Toast link swap: rewire the INTERNAL toast->owner dependencies.
    if row1.reltoastrelid != InvalidOid || row2.reltoastrelid != InvalidOid {
        if row1.reltoastrelid != InvalidOid {
            delete_toast_dependency(mcx, row1.reltoastrelid)?;
        }
        if row2.reltoastrelid != InvalidOid {
            delete_toast_dependency(mcx, row2.reltoastrelid)?;
        }
        // After the swap r1 owns row2's toast and vice versa.
        if row2.reltoastrelid != InvalidOid {
            let toastobject =
                pg_depend::ObjectAddress::set(RELATION_RELATION_ID, row2.reltoastrelid);
            let baseobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, r1);
            pg_depend::recordDependencyOn(
                mcx,
                &toastobject,
                &baseobject,
                pg_depend::DependencyType::Internal,
            )?;
        }
        if row1.reltoastrelid != InvalidOid {
            let toastobject =
                pg_depend::ObjectAddress::set(RELATION_RELATION_ID, row1.reltoastrelid);
            let baseobject = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, r2);
            pg_depend::recordDependencyOn(
                mcx,
                &toastobject,
                &baseobject,
                pg_depend::DependencyType::Internal,
            )?;
        }
    }
    Ok((row1.reltoastrelid, row2.reltoastrelid))
}

// deleteDependencyRecordsFor(RelationRelationId, toastrelid) — a toast
// table's only dependency is the INTERNAL one on its owner.
fn delete_toast_dependency<'mcx>(mcx: Mcx<'mcx>, toastrelid: Oid) -> PgResult<()> {
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, RowExclusiveLock)?;
    let keys = [oid_key(1, RELATION_RELATION_ID), oid_key(2, toastrelid)];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    let mut count = 0;
    let mut tids: PgVec<'mcx, types_tuple::ItemPointerData> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        tids.push(tup.t_self);
        count += 1;
    }
    genam::systable_endscan(mcx, scan)?;
    assert!(count == 1, "expected one dependency record for TOAST table, found {count}");
    for tid in tids.iter() {
        catalog_indexing::CatalogTupleDelete(&dep_rel, tid)?;
    }
    dep_rel.close(RowExclusiveLock)
}

fn set_relrewrite<'mcx>(mcx: Mcx<'mcx>, relid: Oid, relrewrite: Oid) -> PgResult<()> {
    let rel_relation = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let desc = rel_relation.descr();
    let natts = desc.natts as usize;
    let key = oid_key(1, relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel_relation,
        catalog::ClassOidIndexId,
        true,
        None,
        &[key],
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    repl_values[Anum_pg_class_relrewrite - 1] = Datum::from_oid(relrewrite);
    repl[Anum_pg_class_relrewrite - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel_relation, &otid, &mut newtup)?;
    rel_relation.close(RowExclusiveLock)
}
