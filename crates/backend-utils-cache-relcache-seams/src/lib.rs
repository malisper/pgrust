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
    /// `RelationCacheInvalidate(debug_discard)` (relcache.c): blow away the
    /// whole relcache (the `SHAREDINVALRELCACHE_ID`-with-`InvalidOid` and
    /// `InvalidateSystemCaches` paths). Also flushes smgr and the relation
    /// map. Can `ereport(ERROR)` while rebuilding nailed entries, carried on
    /// `Err`.
    pub fn relation_cache_invalidate(debug_discard: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationCacheInvalidateEntry(relationId)` (relcache.c): mark one
    /// relcache entry invalid (the per-relation `SHAREDINVALRELCACHE_ID` arm).
    /// Can `ereport(ERROR)`, carried on `Err`.
    pub fn relation_cache_invalidate_entry(
        relation_id: types_core::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationIdIsInInitFile(relationId)` (relcache.c): is the relation one
    /// whose relcache entry is cached in the relcache init file (so a change
    /// must zap that file at commit)? Pure lookup; infallible.
    pub fn relation_id_is_in_init_file(relation_id: types_core::Oid) -> bool
);

seam_core::seam!(
    /// `RelationCacheInitFilePreInvalidate()` (relcache.c): take
    /// `RelCacheInitLock` and unlink the init file ahead of sending
    /// invalidations. Can `ereport(ERROR)`, carried on `Err`.
    pub fn relation_cache_init_file_pre_invalidate() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationCacheInitFilePostInvalidate()` (relcache.c): release
    /// `RelCacheInitLock` after invalidations are sent.
    pub fn relation_cache_init_file_post_invalidate() -> types_error::PgResult<()>
);
