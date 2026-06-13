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
