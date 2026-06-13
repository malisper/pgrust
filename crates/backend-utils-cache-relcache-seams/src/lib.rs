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
    /// `RelationNeedsWAL(relation)` (utils/rel.h) — whether modifications to
    /// the relation must be WAL-logged: `RelationIsPermanent(relation) &&
    /// (XLogIsNeeded() || (rd_createSubid == InvalidSubTransactionId &&
    /// rd_firstRelfilelocatorSubid == InvalidSubTransactionId))`. The
    /// `rd_createSubid` / `rd_firstRelfilelocatorSubid` fields and the
    /// `wal_level` GUC behind `XLogIsNeeded()` live with the relcache owner,
    /// so this resolves against its cached entry for `rel.rd_id`. Pure
    /// predicate; cannot `ereport`.
    pub fn relation_needs_wal(rel: &types_rel::RelationData<'_>) -> bool
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
