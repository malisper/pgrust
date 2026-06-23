//! `heap_drop_with_catalog` (catalog/heap.c) — removes a relation from the
//! catalogs. Reached through dependency.c's `doDeletion` for an `OCLASS_CLASS`
//! relation object (after the dependency tracer has already dropped indexes,
//! constraints, etc.). Wired as the `heap_drop_with_catalog` inward seam.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::mcx::Mcx;
use ::types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use ::types_core::fmgr::F_OIDEQ;
use types_error::{PgResult, ERROR};
use ::scankey::ScanKeyInit;
use ::utils_error::elog;
use ::types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use ::types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};
use ::types_tuple::access::{RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE};
use types_tuple::heaptuple::Datum;

use crate::delete::{DeleteAttributeTuples, DeleteRelationTuple, RelationRemoveInheritance};
use crate::partition::RemovePartitionKeyByRelId;
use crate::statistics::RemoveStatistics;
use crate::RELKIND_HAS_STORAGE;

/* genbki catalog + index OIDs (catalog/pg_foreign_table.h, indexing.h). */
const ForeignTableRelationId: Oid = 3118;
const ForeignTableRelidIndexId: Oid = 3119;
const Anum_pg_foreign_table_ftrelid: AttrNumber = 1;

/*
 * heap_drop_with_catalog	- removes specified relation from catalogs
 *
 * Note that this routine is not responsible for dropping objects that are
 * linked to the pg_class entry via dependencies (for example, indexes and
 * constraints).  Those are deleted by the dependency-tracing logic in
 * dependency.c before control gets here.  In general, therefore, this routine
 * should never be called directly; go through performDeletion() instead.
 */
pub fn heap_drop_with_catalog<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let mut parentOid: Oid = InvalidOid;
    let mut defaultPartOid: Oid = InvalidOid;

    /*
     * To drop a partition safely, we must grab exclusive lock on its parent.
     * Read the relispartition flag through the pg_class syscache (C:
     * `SearchSysCache1(RELOID)` + `GETSTRUCT(...)->relispartition`, with the
     * `!HeapTupleIsValid` `elog(ERROR)` reproduced on a cache miss).
     */
    let Some(relispartition) =
        syscache_seams::rel_relispartition::call(relid)?
    else {
        return elog(ERROR, &format!("cache lookup failed for relation {relid}"));
    };
    if relispartition {
        /*
         * We have to lock the parent if the partition is being detached,
         * because it's possible that some query still has a partition
         * descriptor that includes this partition.
         */
        parentOid = catalog_partition::get_partition_parent(relid, true)?;
        lmgr::LockRelationOid(parentOid, AccessExclusiveLock)?;

        /*
         * If this is not the default partition, dropping it will change the
         * default partition's partition constraint, so we must lock it.
         */
        defaultPartOid = catalog_partition::get_default_partition_oid(parentOid)?;
        if OidIsValid(defaultPartOid) && relid != defaultPartOid {
            lmgr::LockRelationOid(defaultPartOid, AccessExclusiveLock)?;
        }
    }

    /*
     * Open and lock the relation.
     */
    let rel = common_relation::relation_open(mcx, relid, AccessExclusiveLock)?;

    /*
     * There can no longer be anyone *else* touching the relation, but we
     * might still have open queries or cursors, or pending trigger events, in
     * our own session.
     */
    tablecmds_seams::check_table_not_in_use::call(&rel, "DROP TABLE")?;

    /*
     * This effectively deletes all rows in the table, and may be done in a
     * serializable transaction.  In that case we must record a rw-conflict in
     * to this transaction from each transaction holding a predicate lock on
     * the table.
     */
    predicate_seams::check_table_for_serializable_conflict_in::call(&rel)?;

    /*
     * Delete pg_foreign_table tuple first.
     */
    if rel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        let ftrel = table::table_open(
            mcx,
            ForeignTableRelationId,
            RowExclusiveLock,
        )?;

        let mut key = [ScanKeyData::empty()];
        ScanKeyInit(
            &mut key[0],
            Anum_pg_foreign_table_ftrelid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            Datum::from_oid(relid),
        )?;

        let mut scan = genam_seams::systable_beginscan::call(
            &ftrel,
            ForeignTableRelidIndexId,
            true,
            None,
            &key,
        )?;

        let fttuple =
            genam_seams::systable_getnext::call(mcx, scan.desc_mut())?;
        let Some(fttuple) = fttuple else {
            scan.end()?;
            ftrel.close(RowExclusiveLock)?;
            rel.close(NoLock)?;
            return elog(ERROR, &format!("cache lookup failed for foreign table {relid}"));
        };

        indexing_seams::catalog_tuple_delete::call(&ftrel, fttuple.tuple.t_self)?;

        scan.end()?;
        ftrel.close(RowExclusiveLock)?;
    }

    /*
     * If a partitioned table, delete the pg_partitioned_table tuple.
     */
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        RemovePartitionKeyByRelId(mcx, relid)?;
    }

    /*
     * If the relation being dropped is the default partition itself,
     * invalidate its entry in pg_partitioned_table.
     */
    if relid == defaultPartOid {
        catalog_partition::update_default_partition_oid(parentOid, InvalidOid)?;
    }

    /*
     * Schedule unlinking of the relation's physical files at commit.
     */
    if RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        catalog_storage_seams::relation_drop_storage::call(rel.rd_locator, rel.rd_backend)?;
    }

    /* ensure that stats are dropped if transaction commits */
    pgstat_seams::pgstat_drop_relation::call(relid, rel.rd_rel.relisshared)?;

    /*
     * Close relcache entry, but *keep* AccessExclusiveLock on the relation
     * until transaction commit.
     */
    rel.close(NoLock)?;

    /*
     * Remove any associated relation synchronization states.
     */
    pg_subscription_seams::remove_subscription_rel::call(InvalidOid, relid)?;

    /*
     * Forget any ON COMMIT action for the rel
     */
    tablecmds_seams::remove_on_commit_action::call(relid)?;

    /*
     * Flush the relation from the relcache.
     */
    relcache::invalidate::RelationForgetRelation(relid)?;

    /*
     * remove inheritance information
     */
    RelationRemoveInheritance(mcx, relid)?;

    /*
     * delete statistics
     */
    RemoveStatistics(mcx, relid, 0)?;

    /*
     * delete attribute tuples
     */
    DeleteAttributeTuples(mcx, relid)?;

    /*
     * delete relation tuple
     */
    DeleteRelationTuple(mcx, relid)?;

    if OidIsValid(parentOid) {
        /*
         * If this is not the default partition, the partition constraint of
         * the default partition has changed to include the portion of the key
         * space previously covered by the dropped partition.
         */
        if OidIsValid(defaultPartOid) && relid != defaultPartOid {
            inval::cache_invalidate::CacheInvalidateRelcacheByRelid(defaultPartOid)?;
        }

        /*
         * Invalidate the parent's relcache so that the partition is no longer
         * included in its partition descriptor.
         */
        inval::cache_invalidate::CacheInvalidateRelcacheByRelid(parentOid)?;
        /* keep the lock */
    }

    Ok(())
}
