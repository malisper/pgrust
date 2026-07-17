// cluster.c ALTER TABLE + CLUSTER/VACUUM FULL rewrite slice: make_new_heap /
// swap_relation_files / finish_heap_swap. Toast swaps by content (CLUSTER) or
// by links (ALTER TABLE rewrites). Mapped relations (VACUUM FULL
// pg_class/pg_database) swap via the relation mapper.
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
use types_rel::{AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE, RELKIND_INDEX, RELKIND_TOASTVALUE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const Anum_pg_class_relnamespace: usize = 3;
const Anum_pg_class_relam: usize = 7;
const Anum_pg_class_relfilenode: usize = 8;
const Anum_pg_class_reltablespace: usize = 9;
const Anum_pg_class_relpages: usize = 10;
const Anum_pg_class_reltuples: usize = 11;
const Anum_pg_class_relallvisible: usize = 12;
const Anum_pg_class_relallfrozen: usize = 13;
const Anum_pg_class_reltoastrelid: usize = 14;
const Anum_pg_class_relisshared: usize = 16;
const Anum_pg_class_relpersistence: usize = 17;
const Anum_pg_class_relkind: usize = 18;
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

pub fn make_new_heap<'mcx>(
    mcx: Mcx<'mcx>,
    old_heap_oid: Oid,
    new_tablespace: Oid,
    new_access_method: Oid,
    persistence: u8,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let old_heap = table::table_open(mcx, old_heap_oid, lockmode)?;
    let reloptions = pg_class_reloptions_image(mcx, old_heap_oid)?;
    // C threads the tablespace in from the caller (matview CONCURRENTLY:
    // GetDefaultTablespace(TEMP); ALTER TABLE rewrite: new or old tablespace);
    // only the namespace switches on persistence.
    let namespaceid = if persistence == types_core::RELPERSISTENCE_TEMP {
        catalog_namespace::GetTempTableNamespace(mcx)?
    } else {
        old_heap.rd_rel.relnamespace
    };
    let new_heap_name = format!("pg_temp_{old_heap_oid}");

    let oid_new_heap = catalog_heap::heap_create_with_catalog(
        mcx,
        &catalog_heap::HeapCreateParams {
            relname: &new_heap_name,
            relnamespace: namespaceid,
            reltablespace: new_tablespace,
            ownerid: old_heap.rd_rel.relowner,
            accessmtd: new_access_method,
            relkind: types_rel::RELKIND_RELATION,
            relpersistence: persistence,
            reloftype: types_core::InvalidOid,
            // "the new heap is not a shared relation, even if we are
            // rebuilding a shared rel. However, we do make the new heap
            // mapped if the source is mapped" (cluster.c:751-756).
            mapped: old_heap.is_mapped(),
            allow_system_table_mods: true,
            reloptions: reloptions.as_deref(),
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
        let toast_options = pg_class_reloptions_image(mcx, old_heap.rd_rel.reltoastrelid)?;
        catalog_toasting::NewRelationCreateToastTable(mcx, oid_new_heap, toast_options.as_deref())?;
    }
    old_heap.close(NoLock)?;
    Ok(oid_new_heap)
}

// pg_class.reloptions image for a relation (make_new_heap reads it via
// syscache RELOID); None when NULL.
fn pg_class_reloptions_image<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    const Anum_pg_class_reloptions: i32 = 33;
    let rel_relation = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let desc = rel_relation.descr();
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
    let mut isnull = false;
    // SAFETY: reloptions is a pg_class column under its descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, Anum_pg_class_reloptions, desc, &mut isnull) };
    let image = if isnull { None } else { Some(reloptions::text_array_image(mcx, d)?) };
    genam::systable_endscan(mcx, scan)?;
    rel_relation.close(AccessShareLock)?;
    Ok(image)
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
    let mut mapped_tables: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let (toast1, toast2) = swap_relation_files(
        mcx,
        old_heap_oid,
        new_heap_oid,
        old_heap_oid == RELATION_RELATION_ID,
        swap_toast_by_content,
        frozen_xid,
        cutoff_multi,
        &mut mapped_tables,
    )?;

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
            &mut |_index_id| {},
        )?;
        if !rebuilt {
            // reindex_relation's trailing CCI (it ran none without indexes):
            // the swap's pg_class/pg_depend writes must be visible to the
            // deletion traversal below.
            xact::CommandCounterIncrement()?;
        }
    }

    // Rebuilding pg_class: swap_relation_files couldn't touch pg_class's own
    // row, so relfrozenxid wasn't updated — do it now that the new relation
    // is reachable through its rebuilt indexes (cluster.c:1522-1556).
    if old_heap_oid == RELATION_RELATION_ID {
        let rel_relation = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
        let desc = rel_relation.descr();
        let natts = desc.natts as usize;
        let key = oid_key(1, old_heap_oid);
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel_relation,
            catalog::ClassOidIndexId,
            true,
            None,
            &[key],
        )?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {old_heap_oid}"));
        let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        repl_values.resize(natts, Datum::null());
        repl_isnull.resize(natts, false);
        repl.resize(natts, false);
        repl_values[Anum_pg_class_relfrozenxid - 1] = Datum::from_transaction_id(frozen_xid);
        repl[Anum_pg_class_relfrozenxid - 1] = true;
        repl_values[Anum_pg_class_relminmxid - 1] = Datum::from_u32(cutoff_multi);
        repl[Anum_pg_class_relminmxid - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tup, desc, &repl_values, &repl_isnull, &repl)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel_relation, &otid, &mut newtup)?;
        rel_relation.close(RowExclusiveLock)?;
    }

    let object = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, new_heap_oid);
    catalog_dependency::performDeletion(
        mcx,
        &object,
        types_nodes::parsenodes::DropBehavior::DROP_RESTRICT,
        catalog_dependency::PERFORM_DELETION_INTERNAL,
    )?;

    // Drop the transient tables' relation-map entries before commit; the
    // relmapper rejects new permanent map entries post-bootstrap
    // (cluster.c:1568-1575).
    for &oid in mapped_tables.iter() {
        relmapper::RelationMapRemoveMapping(oid)?;
    }

    // Toast-by-links rename: the surviving toast (swapped onto the old heap)
    // carries the transient name; rename it and its index, reset relrewrite.
    // By-content swaps keep the old toast row (name already right).
    if !swap_toast_by_content && (toast1 != InvalidOid || toast2 != InvalidOid) {
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

// swap_relation_files; returns both reltoastrelid values as seen before the
// swap. By-content recursion (CLUSTER/VACUUM FULL) swaps the toast tables and
// their valid indexes in place of the link swap. Mapped relations swap their
// relation-map entries instead of the pg_class physical columns, and each
// mapped r2 (the transient side) is appended to mapped_tables for
// finish_heap_swap's RelationMapRemoveMapping pass (cluster.c:1056-1186).
fn swap_relation_files<'mcx>(
    mcx: Mcx<'mcx>,
    r1: Oid,
    r2: Oid,
    target_is_pg_class: bool,
    swap_toast_by_content: bool,
    frozen_xid: types_core::primitive::TransactionId,
    cutoff_multi: types_core::primitive::MultiXactId,
    mapped_tables: &mut PgVec<'mcx, Oid>,
) -> PgResult<(Oid, Oid)> {
    let rel_relation = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let desc = rel_relation.descr();
    let natts = desc.natts as usize;

    struct Row<'mcx> {
        tid: types_tuple::ItemPointerData,
        vals: PgVec<'mcx, (usize, Datum)>,
        relnamespace: Oid,
        relfilenode: Oid,
        reltablespace: Oid,
        relam: Oid,
        relpersistence: i8,
        relkind: i8,
        relisshared: bool,
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
            relnamespace: get(Anum_pg_class_relnamespace).as_oid(),
            relfilenode: get(Anum_pg_class_relfilenode).as_oid(),
            reltablespace: get(Anum_pg_class_reltablespace).as_oid(),
            relam: get(Anum_pg_class_relam).as_oid(),
            relpersistence: get(Anum_pg_class_relpersistence).as_i8(),
            relkind: get(Anum_pg_class_relkind).as_i8(),
            relisshared: get(Anum_pg_class_relisshared).as_bool(),
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

    if row1.relfilenode != InvalidOid && row2.relfilenode != InvalidOid {
        // Normal non-mapped relations: swap relfilenumbers, reltablespaces,
        // relam, relpersistence (cluster.c:1101-1134).
        debug_assert!(!target_is_pg_class);
        row1.vals.push((Anum_pg_class_relfilenode, Datum::from_oid(row2.relfilenode)));
        row1.vals.push((Anum_pg_class_reltablespace, Datum::from_oid(row2.reltablespace)));
        row1.vals.push((Anum_pg_class_relam, Datum::from_oid(row2.relam)));
        row1.vals.push((Anum_pg_class_relpersistence, Datum::from_i8(row2.relpersistence)));
        row2.vals.push((Anum_pg_class_relfilenode, Datum::from_oid(row1.relfilenode)));
        row2.vals.push((Anum_pg_class_reltablespace, Datum::from_oid(row1.reltablespace)));
        row2.vals.push((Anum_pg_class_relam, Datum::from_oid(row1.relam)));
        row2.vals.push((Anum_pg_class_relpersistence, Datum::from_i8(row1.relpersistence)));
        if !swap_toast_by_content {
            row1.vals.push((Anum_pg_class_reltoastrelid, Datum::from_oid(row2.reltoastrelid)));
            row2.vals.push((Anum_pg_class_reltoastrelid, Datum::from_oid(row1.reltoastrelid)));
        }
    } else {
        // Mapped-relation case (cluster.c:1135-1186): swap the relation-map
        // entries instead of the pg_class physical columns. Both must be
        // mapped; the equality checks are C's non-user-facing backstops.
        assert!(
            row1.relfilenode == InvalidOid && row2.relfilenode == InvalidOid,
            "cannot swap mapped relation {r1} with non-mapped relation"
        );
        assert!(
            row1.reltablespace == row2.reltablespace,
            "cannot change tablespace of mapped relation {r1}"
        );
        assert!(
            row1.relpersistence == row2.relpersistence,
            "cannot change persistence of mapped relation {r1}"
        );
        assert!(
            row1.relam == row2.relam,
            "cannot change access method of mapped relation {r1}"
        );
        assert!(
            swap_toast_by_content
                || (row1.reltoastrelid == InvalidOid && row2.reltoastrelid == InvalidOid),
            "cannot swap toast by links for mapped relation {r1}"
        );
        let n1 = relmapper::RelationMapOidToFilenumber(r1, row1.relisshared);
        assert!(
            n1 != types_core::InvalidRelFileNumber,
            "could not find relation mapping for relation {r1}"
        );
        let n2 = relmapper::RelationMapOidToFilenumber(r2, row2.relisshared);
        assert!(
            n2 != types_core::InvalidRelFileNumber,
            "could not find relation mapping for relation {r2}"
        );
        // Replacement mappings take effect at CommandCounterIncrement.
        relmapper::RelationMapUpdateMap(r1, n2, row1.relisshared, false)?;
        relmapper::RelationMapUpdateMap(r2, n1, row2.relisshared, false)?;
        mapped_tables.push(r2);
    }

    // C's rd_createSubid/rd_*RelfilelocatorSubid transfer +
    // RelationAssumeNewRelfilelocator(rel1) (cluster.c:1188-1205) is not
    // ported: heapam never WAL-skips permanent rels and bulkwrite/nbtree
    // smgrimmedsync eagerly, so no deferred pendingSyncs read those fields.
    // Load-bearing the day a WAL-skip (wal_level=minimal) lane lands.
    if row1.relkind != RELKIND_INDEX as i8 {
        row1.vals.push((Anum_pg_class_relfrozenxid, Datum::from_transaction_id(frozen_xid)));
        row1.vals.push((Anum_pg_class_relminmxid, Datum::from_u32(cutoff_multi)));
    }
    // Swap size statistics too, since new rel has freshly-updated stats.
    for (anum, a, b) in [
        (Anum_pg_class_relpages, row2.relpages, row1.relpages),
        (Anum_pg_class_reltuples, row2.reltuples, row1.reltuples),
        (Anum_pg_class_relallvisible, row2.relallvisible, row1.relallvisible),
        (Anum_pg_class_relallfrozen, row2.relallfrozen, row1.relallfrozen),
    ] {
        row1.vals.push((anum, a));
        row2.vals.push((anum, b));
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
        if !target_is_pg_class {
            catalog_indexing::CatalogTupleUpdate(mcx, &rel_relation, &otid, &mut newtup)?;
        } else {
            // Updating pg_class's own rows would scribble on the old data
            // we're about to throw away; the map change is the real work.
            // Relcache inval is still required (cluster.c:1248-1273).
            inval::invalidate::CacheInvalidateRelcacheByTuple(newtup.as_tuple())?;
        }
    }
    rel_relation.close(RowExclusiveLock)?;

    // Repoint the relations' pg_am dependencies at their post-swap AMs
    // (cluster.c:1275-1297).
    if row1.relam != row2.relam {
        for (relid, old_am, new_am) in
            [(r1, row1.relam, row2.relam), (r2, row2.relam, row1.relam)]
        {
            if pg_depend::changeDependencyFor(
                mcx,
                RELATION_RELATION_ID,
                relid,
                catalog::AccessMethodRelationId,
                old_am,
                new_am,
            )? != 1
            {
                panic!("could not change access method dependency for relation {relid}");
            }
        }
    }

    if row1.reltoastrelid != InvalidOid || row2.reltoastrelid != InvalidOid {
        if swap_toast_by_content {
            // Recursively swap the toast tables' contents; their pg_class
            // links stayed put, so the old toast OID now owns the new data.
            assert!(
                row1.reltoastrelid != InvalidOid && row2.reltoastrelid != InvalidOid,
                "cannot swap toast files by content when there's only one"
            );
            swap_relation_files(
                mcx,
                row1.reltoastrelid,
                row2.reltoastrelid,
                target_is_pg_class,
                true,
                frozen_xid,
                cutoff_multi,
                mapped_tables,
            )?;
        } else {
            // Link swap: rewire the INTERNAL toast->owner dependencies.
            // Disallowed for system catalogs (the catalog being rebuilt could
            // be one the dependency changes touch).
            assert!(
                !(catalog::IsCatalogRelationOid(r1) || catalog::IsToastNamespace(row1.relnamespace)),
                "cannot swap toast files by links for system catalogs"
            );
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
    }

    // By-content toast swaps carry their valid indexes with them.
    if swap_toast_by_content
        && row1.relkind == RELKIND_TOASTVALUE as i8
        && row2.relkind == RELKIND_TOASTVALUE as i8
    {
        let toast_index1 = heaptoast::toast_get_valid_index(mcx, r1, AccessExclusiveLock)?;
        let toast_index2 = heaptoast::toast_get_valid_index(mcx, r2, AccessExclusiveLock)?;
        swap_relation_files(
            mcx,
            toast_index1,
            toast_index2,
            target_is_pg_class,
            true,
            0,
            0,
            mapped_tables,
        )?;
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
