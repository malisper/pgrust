//! Outward seam declarations consumed by `backend-commands-cluster` for
//! owners that do not yet have a per-owner `-seams` crate (see this crate's
//! Cargo.toml note and `audits/backend-commands-cluster.md` — this is grouped
//! design debt to be split per owner when those units land). Until an owner is
//! ported, every call panics loudly with the seam path.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, PgVec};
use types_cluster::{
    CatalogIndexStateToken, CopyForClusterResult, ParseState, PgClassForm, PgIndexForm,
    PgRusageToken, RelOptionsToken, ReindexParams, VacuumCutoffs,
};
use types_core::{MultiXactId, Oid, RelFileNumber, TransactionId};
use types_error::{ErrorLevel, PgResult};
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::access::RangeVar;
use types_tuple::heaptuple::ItemPointerData;

/* ---- parser/parse_node.c ------------------------------------------------ */

seam_core::seam!(
    /// `parser_errposition(pstate, location)` (parse_node.c): cursor position
    /// (1-based char index) for the error from a token location, or 0.
    pub fn parser_errposition(pstate: &ParseState, location: i32) -> PgResult<i32>
);

/* ---- catalog/namespace.c ------------------------------------------------ */

seam_core::seam!(
    /// `RangeVarGetRelidExtended(relation, AccessExclusiveLock, 0,
    /// RangeVarCallbackMaintainsTable, NULL)` (namespace.c): resolve+lock the
    /// CLUSTER target, running the maintains-table permission callback.
    pub fn range_var_get_relid_maintains_table<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupCreationNamespace(nspname)` (namespace.c): OID of the namespace
    /// to create in (`pg_temp` for temp); `Err` on ACL/lookup failure.
    pub fn lookup_creation_namespace(nspname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RestrictSearchPath()` (namespace.c): set search_path to a safe value
    /// for a security-restricted operation.
    pub fn restrict_search_path() -> PgResult<()>
);

/* ---- access/table/table.c, access/index/indexam.c ----------------------- */

seam_core::seam!(
    /// `table_open(relationId, lockmode)` (table.c): open+lock a table by OID.
    pub fn table_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    /// `index_open(relationId, lockmode)` (indexam.c): open+lock an index by OID.
    pub fn index_open<'mcx>(
        mcx: Mcx<'mcx>,
        relation_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<Relation<'mcx>>
);

seam_core::seam!(
    /// `relation_close(relid, lockmode)` (relation.c) for the cases where the
    /// owned-handle is no longer in scope (the C `goto out` early-exits close
    /// the same OID the caller still references). Refcount + optional lock.
    pub fn relation_close(relid: Oid, lockmode: LOCKMODE) -> PgResult<()>
);

/* ---- utils/cache/relcache.c (rd_rel / rd_index / rd_indam reads) --------- */

seam_core::seam!(
    /// `rel->rd_rel->relam`.
    pub fn rd_rel_relam(rel: &Relation<'_>) -> PgResult<Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->reltablespace`.
    pub fn rd_rel_reltablespace(rel: &Relation<'_>) -> PgResult<Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->relowner`.
    pub fn rd_rel_relowner(rel: &Relation<'_>) -> PgResult<Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->relisshared`.
    pub fn rd_rel_relisshared(rel: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `RelationGetNamespace(rel)` = `rel->rd_rel->relnamespace`.
    pub fn rd_rel_relnamespace(rel: &Relation<'_>) -> PgResult<Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->relfrozenxid`.
    pub fn rd_rel_relfrozenxid(rel: &Relation<'_>) -> PgResult<TransactionId>
);
seam_core::seam!(
    /// `rel->rd_rel->relminmxid`.
    pub fn rd_rel_relminmxid(rel: &Relation<'_>) -> PgResult<MultiXactId>
);
seam_core::seam!(
    /// `rel->rd_islocaltemp` — this backend's own temp relation.
    pub fn rd_islocaltemp(rel: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_index->indrelid` — `None` if `rd_index == NULL` (not an index).
    pub fn rd_index_indrelid(index: &Relation<'_>) -> PgResult<Option<Oid>>
);
seam_core::seam!(
    /// `index->rd_index->indisvalid`.
    pub fn rd_index_indisvalid(index: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `!heap_attisnull(index->rd_indextuple, Anum_pg_index_indpred, NULL)` —
    /// the index has a partial-index predicate.
    pub fn rd_index_has_indpred(index: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_indam->amclusterable`.
    pub fn rd_indam_amclusterable(index: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `RelationIsMapped(rel)` — the relation uses the relation map.
    pub fn relation_is_mapped(rel: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `RelationGetIndexList(rel)` — OIDs of the relation's indexes.
    pub fn relation_get_index_list<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'_>,
    ) -> PgResult<PgVec<'mcx, Oid>>
);
seam_core::seam!(
    /// `RelationGetNumberOfBlocks(rel)` (bufmgr.h) — current block count.
    pub fn relation_get_number_of_blocks(rel: &Relation<'_>) -> PgResult<u32>
);
seam_core::seam!(
    /// Set `NewHeap->rd_toastoid = value` (relcache, transient setting honored
    /// while NewHeap stays open during the cluster copy).
    pub fn set_rd_toastoid(new_heap: &Relation<'_>, value: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `RelationAssumeNewRelfilelocator(rel1)` plus the rel2 subid copy that
    /// `swap_relation_files` performs in its `relation_open` block.
    pub fn swap_relfilelocator_subids(r1: Oid, r2: Oid) -> PgResult<()>
);

/* ---- utils/cache/lsyscache.c (the OID-keyed reads not in lsyscache-seams) */

seam_core::seam!(
    /// `get_index_isclustered(indexOid)` (lsyscache.c).
    pub fn get_index_isclustered(index_oid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `get_rel_namespace(relid)` (lsyscache.c).
    pub fn get_rel_namespace(relid: Oid) -> PgResult<Oid>
);

/* ---- utils/cache/syscache.c (pg_class / pg_index writable copies) -------- */

seam_core::seam!(
    /// `SearchSysCacheExists1(RELOID, indexOid)` (syscache.c).
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `SearchSysCacheCopy1(RELOID, relid)` + `GETSTRUCT` (syscache.c): the
    /// writable pg_class row and its `t_self`; `None` on a cache miss.
    pub fn search_syscache_copy_pg_class<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<(ItemPointerData, PgClassForm)>>
);
seam_core::seam!(
    /// `SearchSysCacheCopy1(INDEXRELID, indexOid)` + `GETSTRUCT` (syscache.c):
    /// the writable pg_index row and its `t_self`; `None` on a cache miss.
    pub fn search_syscache_copy_pg_index<'mcx>(
        mcx: Mcx<'mcx>,
        index_oid: Oid,
    ) -> PgResult<Option<(ItemPointerData, PgIndexForm)>>
);
seam_core::seam!(
    /// `SearchSysCache1 + SysCacheGetAttr(Anum_pg_class_reloptions) +
    /// ReleaseSysCache` (the make_new_heap reloptions fetch): the pg_class
    /// reloptions token (NULL when unset). `Err` "cache lookup failed for
    /// relation %u" when the tuple is missing.
    pub fn fetch_class_reloptions<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<RelOptionsToken>
);

/* ---- access/common/indexing (CatalogTupleUpdate family) ----------------- */

seam_core::seam!(
    /// `CatalogTupleUpdate(pg_class_rel, &tup->t_self, tup)` after reforming
    /// the mutated `PgClassForm` (indexing.c).
    pub fn catalog_tuple_update_pg_class<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'_>,
        tid: ItemPointerData,
        form: &PgClassForm,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogTupleUpdate(pg_index_rel, &tup->t_self, tup)` after reforming
    /// the mutated `PgIndexForm` (indexing.c).
    pub fn catalog_tuple_update_pg_index<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'_>,
        tid: ItemPointerData,
        form: &PgIndexForm,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogOpenIndexes(rel)` (indexing.c).
    pub fn catalog_open_indexes<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'_>,
    ) -> PgResult<CatalogIndexStateToken>
);
seam_core::seam!(
    /// `CatalogTupleUpdateWithInfo(rel, &tup->t_self, tup, indstate)`.
    pub fn catalog_tuple_update_with_info_pg_class<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'_>,
        tid: ItemPointerData,
        form: &PgClassForm,
        indstate: &CatalogIndexStateToken,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `CatalogCloseIndexes(indstate)` (indexing.c).
    pub fn catalog_close_indexes(indstate: CatalogIndexStateToken) -> PgResult<()>
);

/* ---- utils/cache/inval.c ------------------------------------------------ */

seam_core::seam!(
    /// `CacheInvalidateCatalog(catalogId)` (inval.c).
    pub fn cache_invalidate_catalog(catalog_id: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `CacheInvalidateRelcacheByTuple(classTuple)` (inval.c): invalidate the
    /// relcache entry described by the (reformed) pg_class row.
    pub fn cache_invalidate_relcache_by_pg_class(
        tid: ItemPointerData,
        form: &PgClassForm,
    ) -> PgResult<()>
);

/* ---- catalog/catalog.c -------------------------------------------------- */

seam_core::seam!(
    /// `IsSystemRelation(rel)` (catalog.c).
    pub fn is_system_relation(rel: &Relation<'_>) -> PgResult<bool>
);
seam_core::seam!(
    /// `IsSystemClass(relid, reltuple)` (catalog.c).
    pub fn is_system_class(relid: Oid, form: &PgClassForm) -> PgResult<bool>
);

/* ---- catalog/heap.c ----------------------------------------------------- */

seam_core::seam!(
    /// `heap_create_with_catalog(...)` (heap.c) as specialized for the cluster
    /// transient heap: the NewHeap clones OldHeap's tuple descriptor, owner,
    /// AM, persistence, mapped-ness and reloptions, with `relid = OIDOldHeap`
    /// passed for the relrewrite/identity bookkeeping. Returns the new OID.
    pub fn heap_create_with_catalog_transient<'mcx>(
        mcx: Mcx<'mcx>,
        new_heap_name: &str,
        namespaceid: Oid,
        new_tablespace: Oid,
        owner: Oid,
        new_access_method: Oid,
        old_heap: &Relation<'_>,
        relpersistence: u8,
        mapped: bool,
        reloptions: RelOptionsToken,
        old_heap_oid: Oid,
    ) -> PgResult<Oid>
);
seam_core::seam!(
    /// `RelationClearMissing(rel)` (heap.c).
    pub fn relation_clear_missing(rel: &Relation<'_>) -> PgResult<()>
);

/* ---- catalog/toasting.c, access/common/toast_internals.c ---------------- */

seam_core::seam!(
    /// `NewHeapCreateToastTable(relOid, reloptions, lockmode, toastOid)`
    /// (toasting.c) — ends with CommandCounterIncrement.
    pub fn new_heap_create_toast_table<'mcx>(
        mcx: Mcx<'mcx>,
        rel_oid: Oid,
        reloptions: RelOptionsToken,
        lockmode: LOCKMODE,
        toast_oid: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `toast_get_valid_index(toastoid, lockmode)` (toast_internals.c).
    pub fn toast_get_valid_index(toastoid: Oid, lockmode: LOCKMODE) -> PgResult<Oid>
);

/* ---- catalog/index.c, catalog/pg_inherits.c ----------------------------- */

seam_core::seam!(
    /// `reindex_relation(NULL, relid, flags, &params)` (index.c) — rebuilds
    /// every index on the heap; ends with CommandCounterIncrement.
    pub fn reindex_relation<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        flags: i32,
        params: ReindexParams,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `IndexGetRelation(indexId, missing_ok)` (index.c).
    pub fn index_get_relation(index_id: Oid, missing_ok: bool) -> PgResult<Oid>
);
seam_core::seam!(
    /// `find_all_inheritors(parentrelId, lockmode, NULL)` (pg_inherits.c) —
    /// all inheritor OIDs (CLUSTER passes `NoLock`).
    pub fn find_all_inheritors<'mcx>(
        mcx: Mcx<'mcx>,
        parent_rel_id: Oid,
        lockmode: LOCKMODE,
    ) -> PgResult<PgVec<'mcx, Oid>>
);

/* ---- commands/tablecmds.c ----------------------------------------------- */

seam_core::seam!(
    /// `CheckTableNotInUse(rel, stmt)` (tablecmds.c).
    pub fn check_table_not_in_use(rel: &Relation<'_>, stmt: &str) -> PgResult<()>
);
seam_core::seam!(
    /// `RenameRelationInternal(myrelid, newrelname, is_internal, is_index)`
    /// (tablecmds.c).
    pub fn rename_relation_internal<'mcx>(
        mcx: Mcx<'mcx>,
        myrelid: Oid,
        newrelname: &str,
        is_internal: bool,
        is_index: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `ResetRelRewrite(myrelid)` (tablecmds.c).
    pub fn reset_rel_rewrite(myrelid: Oid) -> PgResult<()>
);

/* ---- commands/vacuum.c -------------------------------------------------- */

seam_core::seam!(
    /// `memset(&params, 0, sizeof(VacuumParams)); vacuum_get_cutoffs(OldHeap,
    /// &params, &cutoffs)` (vacuum.c): freeze/cutoff computation.
    pub fn vacuum_get_cutoffs(old_heap: &Relation<'_>) -> PgResult<VacuumCutoffs>
);

/* ---- optimizer/plan/planner.c (plancat) --------------------------------- */

seam_core::seam!(
    /// `plan_cluster_use_sort(tableOid, indexOid)` (planner.c): whether a
    /// seqscan+sort beats an indexscan for the cluster copy.
    pub fn plan_cluster_use_sort(table_oid: Oid, index_oid: Oid) -> PgResult<bool>
);

/* ---- storage/lmgr/predicate.c, storage/lmgr/lmgr.c ---------------------- */

seam_core::seam!(
    /// `TransferPredicateLocksToHeapRelation(relation)` (predicate.c): promote
    /// tuple/page predicate locks to a relation lock before the rewrite.
    pub fn transfer_predicate_locks_to_heap_relation(relid: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `CheckRelationOidLockedByMe(relid, lockmode, orstronger)` (lmgr.c).
    pub fn check_relation_oid_locked_by_me(
        relid: Oid,
        lockmode: LOCKMODE,
        orstronger: bool,
    ) -> bool
);

/* ---- access/heap/heapam.c (the indisclustered catalog scan) ------------- */

seam_core::seam!(
    /// `table_open(IndexRelationId, AccessShareLock)` +
    /// `ScanKeyInit(indisclustered = true)` + `table_beginscan_catalog` +
    /// `heap_getnext(ForwardScanDirection)` loop + `table_endscan` +
    /// `relation_close`, batched (the genam `systable_scan` precedent): the
    /// `(indrelid, indexrelid)` of every pg_index row with `indisclustered`.
    pub fn scan_indisclustered<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, (Oid, Oid)>>
);

/* ---- access/table/tableam.c --------------------------------------------- */

seam_core::seam!(
    /// `table_relation_copy_for_cluster(OldHeap, NewHeap, OldIndex, use_sort,
    /// OldestXmin, &FreezeXid, &MultiXactCutoff, &num_tuples, &tups_vacuumed,
    /// &tups_recently_dead)` (tableam.h): AM-specific heap rewrite.
    pub fn table_relation_copy_for_cluster(
        old_heap: &Relation<'_>,
        new_heap: &Relation<'_>,
        old_index: Option<&Relation<'_>>,
        use_sort: bool,
        oldest_xmin: TransactionId,
        freeze_xid: TransactionId,
        multixact_cutoff: MultiXactId,
    ) -> PgResult<CopyForClusterResult>
);

/* ---- utils/cache/relmapper.c -------------------------------------------- */

seam_core::seam!(
    /// `RelationMapOidToFilenumber(relationId, shared)` (relmapper.c).
    pub fn relation_map_oid_to_filenumber(relation_id: Oid, shared: bool) -> PgResult<RelFileNumber>
);
seam_core::seam!(
    /// `RelationMapUpdateMap(relationId, filenumber, shared, immediate)`.
    pub fn relation_map_update_map(
        relation_id: Oid,
        filenumber: RelFileNumber,
        shared: bool,
        immediate: bool,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `RelationMapRemoveMapping(relationId)` (relmapper.c).
    pub fn relation_map_remove_mapping(relation_id: Oid) -> PgResult<()>
);

/* ---- catalog/objectaccess.h (InvokeObjectPostAlterHookArg) -------------- */

seam_core::seam!(
    /// `InvokeObjectPostAlterHookArg(classId, objectId, subId, auxObjId,
    /// is_internal)` (objectaccess.h): fire the post-alter object-access hook.
    pub fn invoke_object_post_alter_hook_arg(
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        aux_obj_id: Oid,
        is_internal: bool,
    ) -> PgResult<()>
);

/* ---- utils/acl.c -------------------------------------------------------- */

seam_core::seam!(
    /// `pg_class_aclcheck(relid, userid, ACL_MAINTAIN) == ACLCHECK_OK` (acl.c).
    pub fn pg_class_aclcheck_maintain_ok(relid: Oid, userid: Oid) -> PgResult<bool>
);

/* ---- utils/activity/backend_progress.c (pgstat progress) ---------------- */

seam_core::seam!(
    /// `pgstat_progress_start_command(cmdtype, relid)`.
    pub fn pgstat_progress_start_command(cmdtype: i32, relid: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_update_param(index, val)`.
    pub fn pgstat_progress_update_param(index: i32, val: i64) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_progress_end_command()`.
    pub fn pgstat_progress_end_command() -> PgResult<()>
);

/* ---- utils/time/snapmgr.c (active snapshot stack) ----------------------- */

seam_core::seam!(
    /// `PopActiveSnapshot()` (snapmgr.c).
    pub fn pop_active_snapshot() -> PgResult<()>
);
seam_core::seam!(
    /// `PushActiveSnapshot(GetTransactionSnapshot())` (snapmgr.c): take and
    /// push the transaction snapshot.
    pub fn push_active_snapshot_transaction() -> PgResult<()>
);

/* ---- access/transam/xact.c (transaction-block guard) -------------------- */

seam_core::seam!(
    /// `PreventInTransactionBlock(isTopLevel, stmtType)` (xact.c).
    pub fn prevent_in_transaction_block(is_top_level: bool, stmt_type: &str) -> PgResult<()>
);

/* ---- utils/misc/pg_rusage.c --------------------------------------------- */

seam_core::seam!(
    /// `pg_rusage_init(&ru0)` (pg_rusage.c): start the timer snapshot.
    pub fn pg_rusage_init() -> PgRusageToken
);
seam_core::seam!(
    /// `pg_rusage_show(&ru0)` (pg_rusage.c): render the elapsed CPU/wall text.
    pub fn pg_rusage_show<'mcx>(mcx: Mcx<'mcx>, ru0: PgRusageToken) -> PgResult<mcx::PgString<'mcx>>
);

/* ---- utils/error/elog.c (the elevel ereport(msg[, detail])) ------------- */

seam_core::seam!(
    /// `ereport(elevel, (errmsg("..."), [errdetail("...")]))` for the
    /// INFO/DEBUG2 progress logging in `copy_table_data`. Crosses as a
    /// pre-rendered message + optional detail; the owner emits at `elevel`.
    pub fn ereport_msg(elevel: ErrorLevel, msg: String, detail: Option<String>) -> PgResult<()>
);
