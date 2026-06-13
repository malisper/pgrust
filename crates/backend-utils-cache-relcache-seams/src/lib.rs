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
