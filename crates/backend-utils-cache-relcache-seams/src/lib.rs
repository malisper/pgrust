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
    /// `relation->rd_indam` — an index relation's index-access-method vtable
    /// (`access/amapi.h` `IndexAmRoutine`), resolved by OID from the relcache
    /// entry. `None` for relations without one; the indexam dispatch layer
    /// treats a missing vtable as the C NULL-pointer crash. Pure lookup;
    /// cannot `ereport`.
    pub fn relation_rd_indam(
        index_oid: types_core::primitive::Oid,
    ) -> Option<types_tableam::amapi::IndexAmRoutine>
);

seam_core::seam!(
    /// `RelationIncrementReferenceCount(rel)` (relcache.c): bump the relcache
    /// entry's refcount so it stays pinned for the scan's lifetime. Pure
    /// bookkeeping on the entry for `index_oid`; cannot `ereport`, but
    /// fallible only in that the entry must exist — modeled infallible (the C
    /// asserts the entry).
    pub fn relation_increment_reference_count(
        index_oid: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationDecrementReferenceCount(rel)` (relcache.c): drop the refcount
    /// taken by [`relation_increment_reference_count`].
    pub fn relation_decrement_reference_count(
        index_oid: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `irel->rd_support[procindex]` (relcache.c): the support-procedure OID
    /// at `procindex` in the index's relcache-cached `rd_support` array.
    /// `Assert(loc != NULL)` is a debug-only relcache invariant. Pure read;
    /// cannot `ereport`.
    pub fn rd_support_at(
        index_oid: types_core::primitive::Oid,
        procindex: i32,
    ) -> types_error::PgResult<types_core::primitive::RegProcedure>
);

seam_core::seam!(
    /// `index_getprocinfo(irel, attnum, procnum)` lazy-init half: return the
    /// `rd_supportinfo[procindex]` `FmgrInfo`, lazily initialized on first use
    /// (`fmgr_info_cxt(procId, locinfo, irel->rd_indexcxt)`, plus
    /// `set_fn_opclass_options(locinfo, attoptions[attnum-1])` when `procnum
    /// != optsproc`). The cache + its `rd_indexcxt` memory context are
    /// relcache-owned. `Err` carries the C `elog(ERROR, "missing support
    /// function %d for attribute %d of index \"%s\"")` and the
    /// `RelationGetIndexAttOptions` fetch errors.
    pub fn index_getprocinfo(
        index_oid: types_core::primitive::Oid,
        attnum: types_core::primitive::AttrNumber,
        procnum: u16,
        optsproc: u16,
        procindex: i32,
    ) -> types_error::PgResult<types_core::fmgr::FmgrInfo>
);

seam_core::seam!(
    /// The `index_opclass_options` no-procedure error path: build the C
    /// `ereport(ERROR, ERRCODE_INVALID_PARAMETER_VALUE, "operator class %s
    /// has no options")` whose `%s` is `generate_opclass_name(opclass)` for
    /// `opclass = indclass->values[attnum-1]` read off `indrel->rd_indextuple`
    /// (`SysCacheGetAttrNotNull(INDEXRELID, ..., Anum_pg_index_indclass)`).
    /// The syscache fetch + ruleutils naming + the resulting `PgError` are all
    /// the relcache/syscache owner's; the seam returns the constructed error.
    pub fn index_opclass_missing_options_error(
        index_oid: types_core::primitive::Oid,
        attnum: types_core::primitive::AttrNumber,
    ) -> types_error::PgResult<types_error::PgError>
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

seam_core::seam!(
    /// `CreateFakeRelcacheEntry(rlocator)` (xlogutils.c, but allocating a
    /// relcache `RelationData` + non-pinned `SMgrRelation`, which is relcache
    /// substrate). The C `palloc0`s a `FakeRelCacheEntryData`, fills the
    /// physical-storage fields, and `smgropen`s a non-pinned handle. Returns
    /// the owned fake entry in `mcx`. `Err` carries OOM.
    pub fn create_fake_relcache_entry<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rlocator: types_storage::RelFileLocator,
    ) -> types_error::PgResult<types_rel::RelationData<'mcx>>
);

seam_core::seam!(
    /// `FreeFakeRelcacheEntry(fakerel)` (xlogutils.c) — `pfree` the fake entry.
    /// Takes ownership; the owner drops the allocation and its `SMgrRelation`.
    pub fn free_fake_relcache_entry(fakerel: types_rel::RelationData<'_>)
);
