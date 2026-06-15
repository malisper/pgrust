//! Seam declarations for the `backend-utils-cache-relcache` unit
//! (`utils/cache/relcache.c`), which owns relcache entries.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. An open relation crosses seams as the
//! trimmed [`types_rel::RelationData`] copy (see `crates/types-rel`), so
//! plain field reads need no seam; only `rd_tableam` — whose vtable type
//! lives above `types-rel` — is resolved through the owner.

/// The owned relcache entry-store type family, relocated to the standalone
/// `types-relcache-entry` crate in F0'. Re-exported here so this seam crate can
/// name `RelationData` (+ companions) for the forthcoming cross-crate
/// shared-`Rc<RefCell<RelationData>>` seam (`relation_id_get_relation_shared`,
/// promoted in a later wave). No seam consumes it yet — this is the naming
/// enabler only.
pub use types_relcache_entry::{
    FormPgClass, FormPgIndex, LockInfoData, OwnedAttr, OwnedAttrDefault, OwnedConstrCheck,
    OwnedTupleConstr, OwnedTupleDesc, RelationData,
};

/// The dual-carry shared relcache cell type: `Rc<RefCell<RelationData>>` — a
/// CLONE of C's live `RelationData *` into the cache. `types_rel::Relation`
/// carries this (type-erased to `Rc<dyn Any>` to dodge the crate cycle); these
/// monomorphized wrappers recover the concrete cell for consumers that cannot
/// (or would rather not) spell the downcast.
pub type RelcacheEntryCell = std::rc::Rc<std::cell::RefCell<RelationData>>;

/// The concrete shared relcache cell a [`types_rel::Relation`] carries, if it
/// was opened from the cache (the dual-carry migration target). `None` for a
/// cache-less handle (transient/bootstrap/test rels). Monomorphizes
/// [`types_rel::Relation::entry_as`] over the relcache owner's `RelationData`.
pub fn relation_entry_cell(rel: &types_rel::Relation<'_>) -> Option<RelcacheEntryCell> {
    rel.entry_as::<RelationData>()
}

/// Borrow the shared relcache entry a [`types_rel::Relation`] carries and run
/// `f` against it (the live entry — sees in-place rebuilds). `None` for a
/// cache-less handle. Monomorphizes [`types_rel::Relation::with_entry`] over
/// the relcache owner's `RelationData`; the off-`Deref` migration helper.
pub fn relation_with_entry<R>(
    rel: &types_rel::Relation<'_>,
    f: impl FnOnce(&RelationData) -> R,
) -> Option<R> {
    rel.with_entry::<RelationData, R>(f)
}

seam_core::seam!(
    /// `RelationIdGetRelation(relationId)` (relcache.c): load (or build) the
    /// relcache entry for `relationId`, taking the `rd_refcnt += 1` pin, and
    /// hand back the consumed slice of the entry copied into `mcx`. `Ok(None)`
    /// is the C NULL (no `pg_class` row); the owner releases its pin on the
    /// not-found path. Can `ereport(ERROR)` (catalog read failure, OOM),
    /// carried on `Err`.
    pub fn relation_id_get_relation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<types_rel::RelationData<'mcx>>>
);

seam_core::seam!(
    /// `RelationBuildTriggers(relation)` (commands/trigger.c): scan `pg_trigger`
    /// for the relation, build the `TriggerDesc`, and store it on the relcache
    /// entry (`relation->trigdesc`). Called from the relcache build path when
    /// `relhastriggers` is set.
    ///
    /// The relation crosses by `Oid` (the bare-Oid relation convention used for
    /// cross-unit relcache-build helpers, cf. hio-seams), since the trigger owner
    /// re-opens the entry it mutates; relcache's build family applies the result
    /// (it sets `rd_has_trigdesc`).
    ///
    /// This is an OUTWARD seam from relcache's perspective: relcache (the build
    /// path) CALLS it, but the real owner is `commands/trigger.c` — NEEDS_DECOMP
    /// (trigger F1, blocked on this F0 keystone). It is declared in this
    /// relcache-seams crate (rather than a trigger-seams crate) because a
    /// `commands/trigger` → relcache direct dependency would otherwise cycle; F1
    /// installs it from the trigger owner's `init_seams()`. Until then it is
    /// uninstalled and panics loudly on call (mirror-pg-and-panic) — no
    /// allowlist entry is needed because the `seams-init` guard already exempts a
    /// seam the dir-owner itself calls (outward dependency).
    pub fn relation_build_triggers(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationIdGetRelation(relationId)` + hand back C's live shared pointer
    /// (relcache.c): the ADDITIVE shared-ref entry point. Same lookup/build/pin
    /// logic as [`relation_id_get_relation`], but instead of projecting a *copy*
    /// of the entry it returns a CLONE of the cache's
    /// `Rc<RefCell<RelationData>>` (C's `RelationData *` into the cache). A
    /// holder of this clone sees the in-place `*cell.borrow_mut() = rebuilt`
    /// rebuild (true C semantics) and makes `Rc::strong_count > 1` (the safe
    /// analog of `rd_refcnt > 0` pinning the allocation). The pin is tracked on
    /// `rd_refcnt`; the holder must `relation_close`/drop a paired pin to
    /// release it. `Ok(None)` is the C NULL (no `pg_class` row).
    ///
    /// This is the cross-crate promotion of the relcache owner's crate-local
    /// `relation_id_get_relation_shared`, declared here so the later Deref-flip
    /// wave can re-key `types_rel::Relation` onto the shared entry cell across
    /// crates. It coexists with the copy-projecting [`relation_id_get_relation`]
    /// (kept alive for the consumers that have not migrated yet); both
    /// representations are produced from the same cell.
    pub fn relation_id_get_relation_shared(
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<
        Option<std::rc::Rc<std::cell::RefCell<types_relcache_entry::RelationData>>>,
    >
);

seam_core::seam!(
    /// `RelationClose(relation)` (relcache.c): drop the relcache reference
    /// (`rd_refcnt -= 1`) for the entry identified by `relation_id`. C can
    /// `elog(WARNING)` on a refcount inconsistency, carried on `Err`.
    pub fn relation_close(relation_id: types_core::primitive::Oid) -> types_error::PgResult<()>
);

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
    /// `RelationGetRelid(relation)`'s `rd_tableam` — the table-access-method
    /// vtable for the cached relation identified by `relid` (the by-OID form of
    /// [`relation_rd_tableam`], for dispatch wrappers that are keyed by OID
    /// because the `rd_tableam` vtable cannot cross their seam boundary). `None`
    /// for relations without an AM, or no cached entry. Pure lookup; cannot
    /// `ereport`.
    pub fn relation_rd_tableam_by_oid(
        relid: types_core::primitive::Oid,
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

/// `IndexAttrBitmapKind` (relcache.h) — which attribute bitmap
/// `RelationGetIndexAttrBitmap` should return. Mirrors the owner's enum (kept
/// here so cross-crate callers can name the kind without depending on the
/// relcache crate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexAttrBitmapKind {
    Keys,
    PrimaryKey,
    Identity,
    HotBlocking,
    Summarized,
}

seam_core::seam!(
    /// `RelationGetIndexAttrBitmap(relation, attrKind)` (relcache.c): the set of
    /// table column numbers (offset by `FirstLowInvalidHeapAttributeNumber`)
    /// indexed under the requested `attrKind`, or `None` when the relation has
    /// no indexes contributing to that bitmap (the C NULL). Built once and
    /// cached on the entry; the returned set is `bms_copy`d into `mcx`. Opens
    /// the relation's indexes, so it can `ereport(ERROR)`, carried on `Err`.
    pub fn relation_get_index_attr_bitmap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::RelationData<'_>,
        attr_kind: IndexAttrBitmapKind,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
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

// --- backend-utils-init-postinit consumers (relcache.c) ---

seam_core::seam!(
    /// `RelationCacheInitialize()` (relcache.c): set up the relcache hashtable
    /// (no catalog access). `Err` carries its OOM surface.
    pub fn relation_cache_initialize() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationCacheInitializePhase2()` (relcache.c): load relcache entries
    /// for the shared system catalogs. `Err` carries its `ereport` surface.
    pub fn relation_cache_initialize_phase2() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationCacheInitializePhase3()` (relcache.c): load the nailed-in
    /// system-catalog relcache entries (real catalog access). `Err` carries its
    /// `ereport` surface.
    pub fn relation_cache_initialize_phase3() -> types_error::PgResult<()>
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

/* ---- index relcache field reads used by sortsupport.c
 * (`PrepareSortSupportFrom{Index,GistIndex}Rel`). The opclass arrays
 * (`rd_opfamily`/`rd_opcintype`) and the index AM vtable's `amcanorder` flag
 * are relcache-owned per-index state; the relcache owner installs these from
 * its `init_seams()` when it lands. */

seam_core::seam!(
    /// `indexRel->rd_opfamily[attno - 1]` — the operator-family OID of the
    /// index column `attno` (1-based, as in C). `Err` only on a relcache miss.
    pub fn rd_opfamily(
        index: &types_rel::Relation<'_>,
        attno: types_core::primitive::AttrNumber,
    ) -> types_error::PgResult<types_core::Oid>
);
seam_core::seam!(
    /// `indexRel->rd_opcintype[attno - 1]` — the opclass input-type OID of the
    /// index column `attno` (1-based, as in C). `Err` only on a relcache miss.
    pub fn rd_opcintype(
        index: &types_rel::Relation<'_>,
        attno: types_core::primitive::AttrNumber,
    ) -> types_error::PgResult<types_core::Oid>
);
seam_core::seam!(
    /// `indexRel->rd_indam->amcanorder` — whether the index AM produces ordered
    /// output (i.e. supports btree-style ordering). `Err` only on a relcache
    /// miss (the C dereferences `rd_indam` unconditionally).
    pub fn rd_indam_amcanorder(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `indexRel->rd_indam->amsearcharray` — whether the index AM expands
    /// `ScalarArrayOpExpr` quals itself (the `SK_SEARCHARRAY` build path in
    /// `ExecIndexBuildScanKeys`). `Err` only on a relcache miss.
    pub fn rd_indam_amsearcharray(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `criticalRelcachesBuilt` (relcache.c): true once relcache
    /// initialization has built the critical relcache entries (the catcaches
    /// may then use indexscans; cf. catcache.c `IndexScanOK`).
    pub fn critical_relcaches_built() -> bool
);

seam_core::seam!(
    /// `criticalSharedRelcachesBuilt` (relcache.c): true once the critical
    /// *shared* relcache entries (the shared catalogs' indexes) have been
    /// built; gates indexscans on the authentication syscaches in
    /// catcache.c `IndexScanOK`.
    pub fn critical_shared_relcaches_built() -> bool
);

seam_core::seam!(
    /// `AssertCouldGetRelation()` (relcache.c): the assertion-build-only check
    /// that the current process is in a state where it could open a relation
    /// (a live transaction / parallel worker context). A no-op in
    /// non-assert builds; routed here because the state it inspects is owned
    /// by the relcache/xact layer.
    pub fn assert_could_get_relation()
);

seam_core::seam!(
    /// `relation->rd_fdwroutine` (relcache.c): the cached FDW callback-presence
    /// table for the relcache entry `relation_id`, or `None` (the C `NULL`)
    /// before `GetFdwRoutineForRelation` has populated it. The `rd_fdwroutine`
    /// slot lives on the relcache entry the owner keeps, so the read is seamed.
    pub fn relation_fdwroutine(
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<types_nodes::FdwRoutine>>
);

seam_core::seam!(
    /// `cfdwroutine = MemoryContextAlloc(CacheMemoryContext, sizeof(FdwRoutine));
    /// memcpy(...); relation->rd_fdwroutine = cfdwroutine`
    /// (`GetFdwRoutineForRelation`, foreign.c): cache the resolved FDW
    /// callback-presence table on the relcache entry `relation_id` for reuse.
    pub fn set_relation_fdwroutine(
        relation_id: types_core::primitive::Oid,
        fdwroutine: types_nodes::FdwRoutine,
    ) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// Hash access-method consumers (hashutil.c / hashpage.c / hashsearch.c).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `RelationIsAccessibleInLogicalDecoding(rel)` (rel.h): `XLogLogicalInfoActive()
    /// && RelationNeedsWAL(rel) && (IsCatalogRelation(rel) ||
    /// RelationIsUsedAsCatalogTable(rel))`. Determines the `isCatalogRel` flag a
    /// WAL deletion record carries for standby logical-decoding conflict
    /// resolution. The wal-level GUC + relcache predicates are the relcache
    /// owner's. `Err` only on a relcache miss.
    pub fn relation_is_accessible_in_logical_decoding(
        rel: &types_rel::Relation<'_>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `rel->rd_indcollation[attno - 1]` — the collation OID of the index
    /// column `attno` (1-based, as in C). The hash AM reads `rd_indcollation[0]`
    /// for its single-column hash. `Err` only on a relcache miss.
    pub fn rd_indcollation(
        index: &types_rel::Relation<'_>,
        attno: types_core::primitive::AttrNumber,
    ) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `index_getprocid(irel, attnum, procnum)` (indexam.c) — the OID of the
    /// support procedure `procnum` for index column `attnum` (1-based). Read off
    /// `irel->rd_support`. Used by `_hash_init` for `HASHSTANDARD_PROC`.
    pub fn index_getprocid(
        index: &types_rel::Relation<'_>,
        attnum: types_core::primitive::AttrNumber,
        procnum: u16,
    ) -> types_error::PgResult<types_core::primitive::RegProcedure>
);

seam_core::seam!(
    /// `(HashMetaPage) rel->rd_amcache` (hashpage.c `_hash_getcachedmetap`) —
    /// fetch the cached `HashMetaPageData` for this index, or `None` (the C
    /// `rd_amcache == NULL`). The opaque `rd_amcache` slot lives on the relcache
    /// entry, so the cache read/write is seamed onto the relcache owner.
    pub fn rd_amcache_hashmeta(
        index_oid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<types_hash::hashpage::HashMetaPageData>>
);

seam_core::seam!(
    /// `rel->rd_amcache = MemoryContextAlloc(rel->rd_indexcxt,
    /// sizeof(HashMetaPageData)); memcpy(rel->rd_amcache, ...)` (hashpage.c
    /// `_hash_getcachedmetap`) — install/refresh the cached `HashMetaPageData`
    /// on the relcache entry's `rd_amcache` slot.
    pub fn set_rd_amcache_hashmeta(
        index_oid: types_core::primitive::Oid,
        metap: types_hash::hashpage::HashMetaPageData,
    ) -> types_error::PgResult<()>
);
