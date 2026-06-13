//! Seam declarations for the `backend-utils-cache-relcache` unit
//! (`utils/cache/relcache.c`), which owns relcache entries.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. An open relation crosses seams as the
//! trimmed [`types_rel::RelationData`] copy (see `crates/types-rel`), so
//! plain field reads need no seam; only `rd_tableam` — whose vtable type
//! lives above `types-rel` — is resolved through the owner.

seam_core::seam!(
    /// `relation->rd_tableam` — the relation's table-access-method vtable
    /// (`None` for relations without one: views, foreign tables,
    /// partitioned tables/indexes). The owner resolves the vtable from its
    /// cached entry for `rel.rd_id`. Pure lookup; cannot `ereport`.
    pub fn relation_rd_tableam(
        rel: &types_rel::RelationData<'_>,
    ) -> Option<types_tableam::TableAmRoutine>
);

seam_core::seam!(
    /// `RelationNeedsWAL(relation)` (utils/rel.h): true if the relation needs
    /// WAL — permanent and not skipping WAL for a new relfilenode this
    /// transaction. Reads `rd_createSubid`/`rd_firstRelfilelocatorSubid` (not
    /// in the trimmed `RelationData`) and the `wal_level` GUC, so the owner
    /// evaluates the whole macro. Pure read.
    pub fn relation_needs_wal(rel: &types_rel::RelationData<'_>) -> bool
);

seam_core::seam!(
    /// `RELATION_IS_LOCAL(relation)` (utils/rel.h): true if the relation is
    /// temp or newly created this transaction (accessible only to this
    /// backend). Reads `rd_islocaltemp`/`rd_createSubid` (not in the trimmed
    /// `RelationData`), so the owner evaluates the macro. Pure read.
    pub fn relation_is_local(rel: &types_rel::RelationData<'_>) -> bool
);

seam_core::seam!(
    /// `AtEOXact_RelationCache(isCommit)` — relcache cleanup at top-level
    /// transaction end.
    pub fn at_eoxact_relation_cache(is_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_RelationCache(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_relation_cache(
        is_commit: bool,
        my_subid: types_core::SubTransactionId,
        parent_subid: types_core::SubTransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationGetIdentityKeyBitmap(relation)` (relcache.c): the bitmap of
    /// replica-identity-index key columns, offset by
    /// `FirstLowInvalidHeapAttributeNumber`, or `None` when the relation has
    /// no replica identity index (the C NULL). The set is allocated in `mcx`
    /// (C: built under a short-lived context and `bms_copy`d into the
    /// caller's). Opens the identity index, so it can `ereport(ERROR)`,
    /// carried on `Err` (which also includes OOM from the copy).
    pub fn relation_get_identity_key_bitmap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::RelationData<'_>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);

seam_core::seam!(
    /// Read the relation's cached partition key (`relation->rd_partkey`),
    /// returning a copy in `mcx`, or `Ok(None)` when it has not been built
    /// yet (the C NULL). `partcache.c`'s `RelationGetPartitionKey` builds the
    /// key lazily and the relcache caches it on the entry, preserved across
    /// relcache rebuilds; this is the relcache-owned read half (partcache
    /// owns the build). Pure cache read; cannot `ereport` (OOM from the copy
    /// is carried on `Err`).
    pub fn relation_get_partkey<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: types_core::Oid,
    ) -> types_error::PgResult<Option<types_partition::PartitionKeyData<'mcx>>>
);

seam_core::seam!(
    /// Store the freshly built partition key on the relation's relcache entry
    /// (`relation->rd_partkey = key`, in the entry's own `rd_partkeycxt`
    /// child of `CacheMemoryContext`). The relcache owner copies `key` into
    /// that long-lived context; `partcache.c`'s `RelationBuildPartitionKey`
    /// is the builder. `Err` carries OOM from the copy into cache memory.
    pub fn relation_set_partkey<'mcx>(
        relid: types_core::Oid,
        key: types_partition::PartitionKeyData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Read the relation's cached partition CHECK qual list
    /// (`relation->rd_partcheck`), returning a copy in `mcx`, plus the
    /// `relation->rd_partcheckvalid` flag. When the flag is false the cache is
    /// stale and the caller rebuilds; partcache owns the build/recursion.
    /// OOM from the copy is carried on `Err`.
    pub fn relation_get_partcheck<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: types_core::Oid,
    ) -> types_error::PgResult<(bool, mcx::PgVec<'mcx, types_nodes::nodes::Node<'mcx>>)>
);

seam_core::seam!(
    /// Store the freshly built partition CHECK qual list on the relation's
    /// relcache entry (`relation->rd_partcheck = copyObject(result)` in
    /// `rd_partcheckcxt`, then `rd_partcheckvalid = true`). An empty list is
    /// the C NIL (no context made). The relcache owner copies into cache
    /// memory; `Err` carries OOM.
    pub fn relation_set_partcheck<'mcx>(
        relid: types_core::Oid,
        partcheck: mcx::PgVec<'mcx, types_nodes::nodes::Node<'mcx>>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `load_typcache_tupdesc`'s relcache access: open the composite type's
    /// relation under `AccessShareLock`, assert `rd_rel->reltype == type_id`,
    /// copy the relation's `TupleDesc` (`RelationGetDescr`) into `mcx`, and
    /// close the relation. The C shares the relcache's reference-counted
    /// descriptor and bumps `tdrefcount`; the safe port returns an owned copy
    /// the cache keeps. `Err` carries the open `ereport(ERROR)` and OOM.
    pub fn relation_get_composite_tupdesc<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        typrelid: types_core::Oid,
        type_id: types_core::Oid,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>
);

/* ---- CLUSTER rd_rel / rd_index / rd_indam field reads and transient sets --
 * Used by `backend-commands-cluster`; the relcache owner installs these from
 * its `init_seams()` when it lands. */

seam_core::seam!(
    /// `rel->rd_rel->relam`.
    pub fn rd_rel_relam(rel: &types_rel::Relation<'_>) -> types_error::PgResult<types_core::Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->reltablespace`.
    pub fn rd_rel_reltablespace(rel: &types_rel::Relation<'_>) -> types_error::PgResult<types_core::Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->relowner`.
    pub fn rd_rel_relowner(rel: &types_rel::Relation<'_>) -> types_error::PgResult<types_core::Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->relisshared`.
    pub fn rd_rel_relisshared(rel: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `RelationGetNamespace(rel)` = `rel->rd_rel->relnamespace`.
    pub fn rd_rel_relnamespace(rel: &types_rel::Relation<'_>) -> types_error::PgResult<types_core::Oid>
);
seam_core::seam!(
    /// `rel->rd_rel->relfrozenxid`.
    pub fn rd_rel_relfrozenxid(rel: &types_rel::Relation<'_>) -> types_error::PgResult<types_core::TransactionId>
);
seam_core::seam!(
    /// `rel->rd_rel->relminmxid`.
    pub fn rd_rel_relminmxid(rel: &types_rel::Relation<'_>) -> types_error::PgResult<types_core::MultiXactId>
);
seam_core::seam!(
    /// `rel->rd_islocaltemp` — this backend's own temp relation.
    pub fn rd_islocaltemp(rel: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_index->indrelid` — `None` if `rd_index == NULL` (not an index).
    pub fn rd_index_indrelid(index: &types_rel::Relation<'_>) -> types_error::PgResult<Option<types_core::Oid>>
);
seam_core::seam!(
    /// `index->rd_index->indisvalid`.
    pub fn rd_index_indisvalid(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `!heap_attisnull(index->rd_indextuple, Anum_pg_index_indpred, NULL)` —
    /// the index has a partial-index predicate.
    pub fn rd_index_has_indpred(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_indam->amclusterable`.
    pub fn rd_indam_amclusterable(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `RelationIsMapped(rel)` — the relation uses the relation map.
    pub fn relation_is_mapped(rel: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `RelationGetIndexList(rel)` — OIDs of the relation's indexes.
    pub fn relation_get_index_list<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'_>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_core::Oid>>
);
seam_core::seam!(
    /// `RelationGetNumberOfBlocks(rel)` (bufmgr.h) — current block count.
    pub fn relation_get_number_of_blocks(rel: &types_rel::Relation<'_>) -> types_error::PgResult<u32>
);
seam_core::seam!(
    /// Set `NewHeap->rd_toastoid = value` (relcache, transient setting honored
    /// while NewHeap stays open during the cluster copy).
    pub fn set_rd_toastoid(new_heap: &types_rel::Relation<'_>, value: types_core::Oid) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `RelationAssumeNewRelfilelocator(rel1)` plus the rel2 subid copy that
    /// `swap_relation_files` performs in its `relation_open` block.
    pub fn swap_relfilelocator_subids(r1: types_core::Oid, r2: types_core::Oid) -> types_error::PgResult<()>
);
