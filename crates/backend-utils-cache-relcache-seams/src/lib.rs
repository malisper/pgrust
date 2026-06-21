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

/// `RewriteRule` (`rewrite/prs2lock.h`), re-projected into a per-query `'mcx`
/// arena — the mcx-bound mirror of the relcache entry's `'static`
/// [`types_relcache_entry::RewriteRule`].
///
/// The cached entry holds its rule trees in the process-lifetime
/// CacheMemoryContext (`Query<'static>`/`Node<'static>`), reachable only
/// in-crate to the relcache owner. `rewriteHandler.c` runs on a per-query
/// `'mcx` arena and may NOT borrow `'static` cache memory across a seam (the
/// cache entry can be invalidated/rebuilt mid-query). So the
/// [`relation_rules`] reader deep-copies (`Query::clone_in`/`Node::clone_in`,
/// the C `copyObject`) each rule into the caller's `mcx`, exactly as the C
/// rewriter copies the rule action list before mutating it. This is the
/// faithful per-query rendering of the cached `RewriteRule`, not an invented
/// handle — field-for-field with the carrier, only the lifetime differs.
pub struct RewriteRuleImage<'mcx> {
    /// `Oid ruleId` — the `pg_rewrite` OID.
    pub ruleId: types_core::primitive::Oid,
    /// `CmdType event` — the command the rule fires on.
    pub event: types_nodes::nodes::CmdType,
    /// `char ev_enabled` — `'O'`/`'D'`/`'R'`/`'A'` (the raw stored char).
    pub enabled: u8,
    /// `bool isInstead` — is this an INSTEAD rule?
    pub isInstead: bool,
    /// `Node *qual` — the rule qualification, re-homed into `mcx`, or `None`
    /// for an unconditional rule.
    pub qual: Option<mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>>,
    /// `List *actions` — the rule's action `Query` trees, re-homed into `mcx`.
    pub actions: mcx::PgVec<'mcx, types_nodes::copy_query::Query<'mcx>>,
}

/// `RuleLock` (`rewrite/prs2lock.h`), re-projected into a per-query `'mcx`
/// arena — the mcx-bound mirror of the relcache entry's `'static`
/// [`types_relcache_entry::RuleLock`], returned by [`relation_rules`]. As with
/// the carrier, `numLocks` is implicit in `rules.len()`.
pub struct RuleLockImage<'mcx> {
    /// `RewriteRule **rules` — the rules in `RelationBuildRuleLock` read order.
    pub rules: mcx::PgVec<'mcx, RewriteRuleImage<'mcx>>,
}

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
    /// `RelationForgetRelation(rid)` (relcache.c): drop (or mark drop-pending)
    /// the relcache entry for a relation the caller has deleted. `index_drop`
    /// (catalog/index.c) calls it after closing the index relation so the
    /// relcache won't rebuild the entry while its catalog rows are removed.
    /// `Err` carries its `ereport(ERROR)`s.
    pub fn relation_forget_relation(rid: types_core::primitive::Oid) -> types_error::PgResult<()>
);

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
    /// Project the consumed slice of an ALREADY-PINNED entry into `mcx`
    /// WITHOUT taking another `rd_refcnt` pin. The prebuilt-entry companion to
    /// [`relation_id_get_relation`], used by `heap_create`: the entry was just
    /// built by `RelationBuildLocalRelation`, which already took the single
    /// `RelationIncrementReferenceCount` pin C's `heap_create_with_catalog`
    /// later releases with `table_close(new_rel_desc, NoLock)`. Opening the
    /// just-built entry via the normal (incrementing) path would over-pin it,
    /// leaving a stuck reference that makes a later `CheckTableNotInUse`
    /// (DROP/TRUNCATE) report the relation as still in use. `Ok(None)` if the
    /// entry is absent (cannot happen for a freshly built local relation).
    pub fn relation_project_existing<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<types_rel::RelationData<'mcx>>>
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
    /// Clone the cache's shared cell for an ALREADY-PINNED entry, WITHOUT
    /// taking a second `rd_refcnt` pin. This is the dual-carry companion to
    /// [`relation_id_get_relation`]: `relation_open` pins the entry once via
    /// the copy path, then fetches the same cell here to ride alongside the
    /// trimmed copy. Taking another `rd_refcnt` here would double-count the
    /// single open (the handle's closer only decrements once), so this is a
    /// pin-free clone — the `Rc::strong_count` it adds is the cell-allocation
    /// pin, not the `rd_refcnt` bookkeeping. `Ok(None)` if the entry is absent.
    pub fn relation_id_get_relation_cell(
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
/// `RelationGetIndexAttrBitmap` should return. Canonical definition lives in
/// `types-relcache-entry` (the relcache entry-store vocabulary crate); both the
/// owner and cross-crate callers name it from there.
pub use types_relcache_entry::IndexAttrBitmapKind;

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
    /// `RelationGetForm(rel)->relnatts` — number of (live + dropped) columns in
    /// the relation, read by `catalog/index.c` `ConstructTupleDescriptor` as a
    /// bounds check (`atnum > natts` is the C "invalid column number" error).
    pub fn rd_rel_relnatts(rel: &types_rel::Relation<'_>) -> types_error::PgResult<i16>
);
seam_core::seam!(
    /// `rel->rd_rel->relispartition` — read by `catalog/index.c`
    /// `index_check_primary_key` (a CREATE TABLE .. PARTITION OF check).
    pub fn rd_rel_relispartition(rel: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `RelationGetDescr(rel)` (`access/htup_details.h`): the relation's tuple
    /// descriptor. The C shares the relcache's reference-counted descriptor; the
    /// safe port returns an owned `mcx`-backed copy. Read by `catalog/index.c`
    /// `ConstructTupleDescriptor` (the heap relation's per-column
    /// `FormData_pg_attribute` fields the index columns copy from). `Err`
    /// carries OOM.
    pub fn relation_get_descr<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>
);
seam_core::seam!(
    /// `RelationGetDummyIndexExpressions(relation)` (relcache.c): like
    /// `RelationGetIndexExpressions`, but returns null `Const`s of the right
    /// types/typmods/collations in place of the real index expressions (used by
    /// `catalog/index.c` `BuildDummyIndexInfo` to avoid running user code). The
    /// fresh expression list is allocated in `mcx`; `None` for a non-expression
    /// index. `Err` carries the `pg_node_tree` decode `ereport(ERROR)`.
    pub fn relation_get_dummy_index_expressions<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, types_nodes::primnodes::Expr<'static>>>>
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
    /// `index->rd_index->indkey.values[0..IndexRelationGetNumberOfAttributes]`
    /// — the table column number of each index column (the full `int2vector`
    /// indkey), as genam's `systable_beginscan` attribute-number→index-column
    /// conversion loop reads it. The trimmed in-cache `FormData_pg_index`
    /// carrier (`types_rel`) keeps only `indkey0`; the full vector lives in the
    /// relcache entry, so the conversion reads it through this accessor.
    /// `None` if `rd_index == NULL` (not an index). `IndexRelationGetNumber-
    /// OfAttributes(irel)` is the returned vector's length (`indnatts`).
    pub fn rd_index_indkey(index: &types_rel::Relation<'_>) -> types_error::PgResult<Option<std::vec::Vec<types_core::primitive::AttrNumber>>>
);
seam_core::seam!(
    /// `index->rd_index->indnatts` — total number of columns in the index
    /// (`IndexRelationGetNumberOfAttributes`). Read by `BuildIndexInfo`
    /// (`catalog/index.c`) to size `ii_IndexAttrNumbers` and copy
    /// `indkey.values[0..indnatts]`. `None` if `rd_index == NULL` (not an
    /// index). Pure read.
    pub fn rd_index_indnatts(index: &types_rel::Relation<'_>) -> types_error::PgResult<Option<i16>>
);
seam_core::seam!(
    /// `index->rd_index->indnkeyatts` — number of key columns in the index
    /// (`IndexRelationGetNumberOfKeyAttributes`). Read by `BuildIndexInfo`
    /// for `makeIndexInfo`. `None` if `rd_index == NULL` (not an index). Pure
    /// read.
    pub fn rd_index_indnkeyatts(index: &types_rel::Relation<'_>) -> types_error::PgResult<Option<i16>>
);
seam_core::seam!(
    /// `index->rd_index->indisunique` — is this a unique index? Read by
    /// `BuildIndexInfo` for `makeIndexInfo`. `false` if `rd_index == NULL`
    /// (not an index). Pure read.
    pub fn rd_index_indisunique(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_index->indisprimary` — is this index for a primary key?
    /// `false` if `rd_index == NULL` (not an index). Pure read.
    pub fn rd_index_indisprimary(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_index->indisexclusion` — is this index for an exclusion
    /// constraint? `BuildIndexInfo` uses `indisexclusion && indisunique` and
    /// gates `RelationGetExclusionInfo`. `false` if `rd_index == NULL` (not an
    /// index). Pure read.
    pub fn rd_index_indisexclusion(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_index->indisready` — is this index ready for inserts? Read
    /// by `BuildIndexInfo` (`indexStruct->indisready`) for `makeIndexInfo`.
    /// `false` if `rd_index == NULL` (not an index). Pure read.
    pub fn rd_index_indisready(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `index->rd_index->indnullsnotdistinct` — does this unique index treat
    /// NULLs as not-distinct? Read by `BuildIndexInfo`
    /// (`indexStruct->indnullsnotdistinct`) for `makeIndexInfo`. `false` if
    /// `rd_index == NULL` (not an index). Pure read.
    pub fn rd_index_indnullsnotdistinct(index: &types_rel::Relation<'_>) -> types_error::PgResult<bool>
);
seam_core::seam!(
    /// `rel->rd_rel->relpersistence` — the relation's persistence
    /// (`RELPERSISTENCE_PERMANENT`/`UNLOGGED`/`TEMP`). Read by `index_build`
    /// (the unlogged init-fork check) and `index_update_stats`. Pure read.
    pub fn rd_rel_relpersistence(rel: &types_rel::Relation<'_>) -> types_error::PgResult<i8>
);
seam_core::seam!(
    /// `rel->rd_rel->relkind` — the relation's kind (`RELKIND_RELATION`/
    /// `INDEX`/`TOASTVALUE`/`MATVIEW`/…). Read by `index_update_stats` to gate
    /// the autovacuum / visibility-map handling. Pure read.
    pub fn rd_rel_relkind(rel: &types_rel::Relation<'_>) -> types_error::PgResult<i8>
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
    /// `RelationAssumeNewRelfilelocator(relation)` (relcache.c): record that the
    /// relation took a new relfilenumber this (sub)transaction and flag it for
    /// end-of-xact cleanup. The Relation-keyed standalone form (as called by
    /// `ATExecSetTableSpace`, tablecmds.c). The relcache owns the entry, so only
    /// the relation OID crosses.
    pub fn relation_assume_new_relfilelocator(relid: types_core::Oid) -> types_error::PgResult<()>
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
    /// `IsToastRelation(relation)` (catalog.c): true if the relation lives in a
    /// `pg_toast` namespace (`IsToastNamespace(RelationGetNamespace(relation))`).
    /// `RelationGetNamespace` reads `rd_rel->relnamespace`, and the
    /// `pg_toast`-namespace test belongs to the catalog owner; both live behind
    /// the relcache owner here. Keyed by OID (the open relation the logical-
    /// decoding apply path holds is identified by its relid). `Err` only on a
    /// relcache miss.
    pub fn is_toast_relation(
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `RelationIsLogicallyLogged(relation)` (rel.h): `XLogLogicalInfoActive()
    /// && RelationNeedsWAL(relation) && relkind != RELKIND_FOREIGN_TABLE &&
    /// !IsCatalogRelation(relation)`. The `wal_level` GUC + `RelationNeedsWAL`
    /// (`rd_createSubid`/`rd_firstRelfilelocatorSubid`) + the catalog-relation
    /// test are the relcache owner's. Keyed by OID. `Err` only on a relcache
    /// miss.
    pub fn relation_is_logically_logged(
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `relation->rd_rel->relrewrite` (pg_class): the OID of the table this is a
    /// transient rewrite copy of during a heap rewrite (e.g. ALTER TABLE), or
    /// `InvalidOid`. Read by the logical-decoding apply path
    /// (`ReorderBufferProcessTXN`) to skip transient DDL heaps unless the plugin
    /// asked for them. Keyed by OID. `Err` only on a relcache miss.
    pub fn rd_rel_relrewrite(
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `relation->rd_rel->relkind` (pg_class), keyed by OID — the relation's
    /// `RELKIND_*`. The by-`&Relation` [`rd_rel_relkind`] needs a live handle;
    /// this OID-keyed variant serves the logical-decoding apply path, which
    /// holds the relation only by the relid it resolved. `Err` only on a miss.
    pub fn rd_rel_relkind_by_oid(
        relation_id: types_core::primitive::Oid,
    ) -> types_error::PgResult<i8>
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

seam_core::seam!(
    /// `(SpGistCache *) index->rd_amcache` (spgutils.c `spgGetCache`) — fetch
    /// the cached `SpGistCache` for this index, or `None` (the C
    /// `rd_amcache == NULL`, in which case `spgGetCache` rebuilds and installs
    /// one via [`set_rd_amcache_spgist`]). The opaque `rd_amcache` slot lives on
    /// the relcache entry, so the cache read/write is seamed onto the relcache
    /// owner.
    pub fn rd_amcache_spgist(
        index_oid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<types_spgist::SpGistCache>>
);

seam_core::seam!(
    /// `index->rd_amcache = MemoryContextAlloc(index->rd_indexcxt,
    /// sizeof(SpGistCache)); memcpy(index->rd_amcache, &cache, ...)` (spgutils.c
    /// `spgGetCache`) — install/refresh the cached `SpGistCache` on the relcache
    /// entry's `rd_amcache` slot.
    pub fn set_rd_amcache_spgist(
        index_oid: types_core::primitive::Oid,
        cache: types_spgist::SpGistCache,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationSetNewRelfilenumber(relation, persistence)` (relcache.c):
    /// assign a new relfilenumber to a relation, creating new physical
    /// storage (the rewriting forms of ALTER TABLE / TRUNCATE / sequence
    /// rewrite). Keyed by the relation OID; `persistence` is the target
    /// `relpersistence`. `Err` carries the smgr/catalog `ereport(ERROR)`s.
    pub fn relation_set_new_relfilenumber(
        relation: types_core::primitive::Oid,
        persistence: i8,
    ) -> types_error::PgResult<()>
);

/* ======================================================================== *
 * BuildIndexInfo's relcache reads (relcache.c). `BuildIndexInfo`
 * (catalog/index.c) fetches the index's expression / predicate node trees and,
 * for an exclusion index, the exclusion operator/proc/strategy arrays. Each
 * decodes the raw `rd_indextuple` (`indexprs`/`indpred` pg_node_tree text) or
 * walks `pg_constraint`, which is the un-decoded relcache node path — keyed by
 * the index relation. The owner installs from `init_seams()` once that decode
 * path lands; until then the call panics (`mirror-pg-and-panic`).
 * ======================================================================== */

seam_core::seam!(
    /// `RelationGetIndexExpressions(relation)` (relcache.c): `stringToNode` of
    /// the raw `pg_index.indexprs`, `eval_const_expressions`, `fix_opfuncids`,
    /// and `copyObject` of the cached `rd_indexprs` list into the caller's
    /// context. Returns the expression-column trees (`List *`, the C `NIL`
    /// becomes `None`), as `BuildIndexInfo` feeds them straight into
    /// `makeIndexInfo`. `rel` is the open index relation. Can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn relation_get_index_expressions<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, types_nodes::Expr<'static>>>>
);

seam_core::seam!(
    /// `RelationGetIndexPredicate(relation)` (relcache.c): `stringToNode` of the
    /// raw `pg_index.indpred`, `eval_const_expressions`, `canonicalize_qual`,
    /// `make_ands_implicit`, `fix_opfuncids`, and `copyObject` of the cached
    /// `rd_indpred` list into the caller's context. Returns the partial-index
    /// predicate (implicit-AND `List *`, the C `NIL` becomes `None`), which
    /// `BuildIndexInfo` feeds into `makeIndexInfo`. `rel` is the open index
    /// relation. Can `ereport(ERROR)`, carried on `Err`.
    pub fn relation_get_index_predicate<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, types_nodes::Expr<'static>>>>
);

seam_core::seam!(
    /// `RelationGetExclusionInfo(indexRelation, &operators, &procs, &strategies)`
    /// (relcache.c): for an exclusion-constraint index, find the owning
    /// `pg_constraint` row (`get_index_constraint` + `SearchSysCache1(CONSTROID)`
    /// + `DatumGetArrayTypeP(conexclop)`), then per key column resolve the
    /// operator OID, its underlying function (`get_opcode`), and the opclass
    /// strategy number (`get_op_opfamily_strategy` over the index's opfamily).
    /// Returns the three parallel per-key arrays (length `indnkeyatts`) that
    /// `BuildIndexInfo` stores into `ii_ExclusionOps`/`ii_ExclusionProcs`/
    /// `ii_ExclusionStrats`. `rel` is the open index relation. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn relation_get_exclusion_info<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<(
        mcx::PgVec<'mcx, types_core::primitive::Oid>,
        mcx::PgVec<'mcx, types_core::primitive::Oid>,
        mcx::PgVec<'mcx, u16>,
    )>
);

seam_core::seam!(
    /// `RelationBuildLocalRelation(relname, relnamespace, tupDesc, relid,
    /// accessmtd, relfilenumber, reltablespace, shared_relation,
    /// mapped_relation, relpersistence, relkind)` (relcache.c): build a local
    /// (uncataloged) relcache entry for a freshly-created relation and add it to
    /// the relcache. Called by `heap_create` (catalog/heap.c). In C the return
    /// is the new `Relation`; the owned model returns the new relcache entry's
    /// OID (the entry lives in the relcache store, addressed by OID). Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn relation_build_local_relation<'mcx>(
        relname: &str,
        relnamespace: types_core::primitive::Oid,
        tup_desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        relid: types_core::primitive::Oid,
        accessmtd: types_core::primitive::Oid,
        reltablespace: types_core::primitive::Oid,
        shared_relation: bool,
        mapped_relation: bool,
        relpersistence: i8,
        relkind: i8,
        relfilenumber: types_core::primitive::RelFileNumber,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

/// `RowSecurityPolicy` (`rewrite/rowsecurity.h`), re-projected into a per-query
/// `'mcx` arena — the mcx-bound mirror of the relcache entry's `'static`
/// [`types_relcache_entry::RowSecurityPolicy`].
///
/// The cached entry holds its policy quals in the process-lifetime
/// CacheMemoryContext (`Node<'static>`), reachable only in-crate to the relcache
/// owner. `rowsecurity.c` runs on a per-query `'mcx` arena and copies each
/// policy qual (`copyObject(policy->qual)`) before re-pointing its Vars; the
/// reader hands it the already-`mcx`-homed copy. `roles`/`polcmd`/`permissive`/
/// `hassublinks` are the scalar fields `get_policies_for_relation` reads.
pub struct RowSecurityPolicyImage<'mcx> {
    /// `char *policy_name`.
    pub policy_name: mcx::PgString<'mcx>,
    /// `char polcmd` — `'r'`/`'a'`/`'w'`/`'d'`/`'*'`.
    pub polcmd: i8,
    /// `ArrayType *roles`, decoded to element `Oid[]`.
    pub roles: mcx::PgVec<'mcx, types_core::primitive::Oid>,
    /// `bool permissive`.
    pub permissive: bool,
    /// `Expr *qual` — `copyObject(policy->qual)`, re-homed into `mcx`.
    pub qual: Option<mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>>,
    /// `Expr *with_check_qual` — `copyObject(policy->with_check_qual)`.
    pub with_check_qual: Option<mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>>,
    /// `bool hassublinks`.
    pub hassublinks: bool,
}

seam_core::seam!(
    /// The per-query row-security policy reader for `rowsecurity.c`. C reads the
    /// policy list directly off the open relation
    /// (`relation->rd_rsdesc->policies`), but the policy quals live in the
    /// relcache entry's process-lifetime CacheMemoryContext (`Node<'static>`),
    /// reachable only in-crate to the relcache owner — the trimmed cross-unit
    /// `types_rel::Relation<'mcx>` handle carries no `rd_rsdesc`. This seam
    /// fetches the relcache entry by `reloid`, and if `rd_rsdesc` is set
    /// re-projects every policy into the caller's `mcx` arena (`Node::clone_in`,
    /// the C `copyObject` the rewriter performs before mutating a qual). The
    /// returned vector preserves `rd_rsdesc->policies` order. `Ok(None)` is the C
    /// `rd_rsdesc == NULL` (RLS disabled / no policies). Installed from
    /// relcache's `init_seams()`; can `ereport(ERROR)` (relation not open, OOM
    /// during the deep copy), carried on `Err`.
    pub fn relation_row_security<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        reloid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, RowSecurityPolicyImage<'mcx>>>>
);

seam_core::seam!(
    /// The per-query rewrite-rule reader for `rewriteHandler.c`. C reads the
    /// rule list directly off the open relation (`relation->rd_rules->rules[i]`),
    /// but the rule trees live in the relcache entry's process-lifetime
    /// CacheMemoryContext (`Query<'static>`), reachable only in-crate to the
    /// relcache owner — the trimmed cross-unit `types_rel::Relation<'mcx>` handle
    /// the rewriter holds is node-vocabulary-free and carries no `rd_rules`. This
    /// seam fetches the relcache entry by `reloid` (the C
    /// `RelationIdGetRelation`/`with_relation` target), and if `rd_rules` is set
    /// re-projects the whole `RuleLock` into the caller's `mcx` arena via
    /// `Query::clone_in`/`Node::clone_in` (the C `copyObject` the rewriter
    /// performs before mutating a rule's action list). `Ok(None)` is the C
    /// `rd_rules == NULL` (the relation has no rules). The owner is the relcache
    /// crate (it owns the entry store and already deps `types-nodes`), so this is
    /// installed from relcache's `init_seams()`. Can `ereport(ERROR)`
    /// (relation not open, OOM during the deep copy), carried on `Err`.
    pub fn relation_rules<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        reloid: types_core::primitive::Oid,
    ) -> types_error::PgResult<Option<RuleLockImage<'mcx>>>
);

seam_core::seam!(
    /// `RelationInitIndexAccessInfo(relation)` (relcache.c): set up the
    /// in-memory index access information (`rd_indexcxt`, `rd_indam`,
    /// `rd_index`/`rd_indextuple`, opclass/opfamily/support arrays) for an open
    /// index relcache entry. `index_create` (catalog/index.c) calls this only in
    /// bootstrap mode (otherwise the entry is rebuilt by the `sinval` flush at
    /// `CommandCounterIncrement`). The relcache entry is registry-owned, so the
    /// seam addresses it by OID. `Err` carries the catalog-lookup
    /// `ereport(ERROR)`s.
    pub fn relation_init_index_access_info(index_id: types_core::primitive::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `formrdesc`'s hardcoded `Schema_pg_*[]` `FormData_pg_attribute` rows for a
    /// nailed bootstrap catalog, keyed by the catalog's row-type OID
    /// (`*Relation_Rowtype_Id`, the value relcache's Phase2/3 passes).
    ///
    /// This is genbki-generated catalog-header bootstrap data
    /// (`catalog/schemapg.h`), owned by the `backend-bootstrap-catalog-data`
    /// crate; it crosses into relcache as a pure value carrier
    /// ([`BootstrapCatalogSchema`] = the row vector plus the catalog relation OID
    /// `formrdesc` reads for `rd_id`, which the `OwnedAttr` rows cannot carry).
    ///
    /// OUTWARD seam from relcache's perspective: relcache CALLS it (from
    /// `formrdesc`'s Phase2/3 callers), the bootstrap-catalog-data owner INSTALLS
    /// it from its `init_seams()`. Declared here (not in a separate seams crate)
    /// to avoid a relcache→catalog-data dependency cycle. Infallible in C (a pure
    /// static-data lookup); panics on an unknown `reltype` (a bootstrap bug).
    pub fn catalog_schema_attrs(
        reltype: types_core::primitive::Oid,
    ) -> types_relcache_entry::BootstrapCatalogSchema
);

seam_core::seam!(
    /// `RelationCacheInitFileRemove()` (relcache.c) — unlink the relcache init
    /// files (`global/` and each tablespace's `pg_internal.init`) at startup so
    /// a stale cache from a previous lifecycle is not reused. Called once from
    /// `StartupXLOG` (xlog.c:5657). The unlink path can `ereport`, carried on
    /// `Err`.
    pub fn relation_cache_init_file_remove() -> types_error::PgResult<()>
);

/// Projection of `rel->rd_options` viewed as `StdRdOptions` (`access/reloptions.h`)
/// for the VACUUM driver (`commands/vacuum.c` `vacuum_rel`). `has_options` is
/// false when `rd_options == NULL` (no reloptions set), exactly as the C code
/// branches on `(StdRdOptions *) onerel->rd_options`. The carried fields are the
/// ones `vacuum_rel` reads: `vacuum_index_cleanup` (the raw `StdRdOptIndexCleanup`
/// enum value), `max_eager_freeze_failure_rate`, and the
/// `(vacuum_truncate_set, vacuum_truncate)` pair carried in `vacuum_truncate`
/// (`Some` mirrors a present `rd_options`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct StdRdOptionsView {
    /// `onerel->rd_options != NULL`.
    pub has_options: bool,
    /// `((StdRdOptions *) rd_options)->vacuum_index_cleanup` (raw enum value).
    pub vacuum_index_cleanup: u8,
    /// `((StdRdOptions *) rd_options)->vacuum_max_eager_freeze_failure_rate`.
    pub max_eager_freeze_failure_rate: f64,
    /// `(vacuum_truncate_set, vacuum_truncate)` of the reloptions; `None` when
    /// `rd_options == NULL`.
    pub vacuum_truncate: Option<(bool, bool)>,
}

seam_core::seam!(
    /// `onerel->rd_rel->relfrozenxid` + `onerel->rd_rel->relminmxid` — the
    /// relation's frozen-xid / min-multixact horizons, read together by
    /// `vacuum_get_cutoffs` (`commands/vacuum.c`) to seed `cutoffs->relfrozenxid`
    /// / `cutoffs->relminmxid`. By-OID read off the live relcache entry.
    pub fn rel_frozenxid_minmxid(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<(types_core::TransactionId, types_core::MultiXactId)>
);
seam_core::seam!(
    /// `onerel->rd_rel->relpages` + `onerel->rd_rel->reltuples` — the relation's
    /// stored page count (`BlockNumber`) and live-tuple estimate (`float4`),
    /// read together by `vac_estimate_reltuples` (`commands/vacuum.c`).
    /// `reltuples` is widened from the stored `float4` to `f64` for the caller.
    pub fn rel_pages_tuples(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<(types_core::BlockNumber, f64)>
);
seam_core::seam!(
    /// `onerel->rd_rel->relowner` — the relation's owner role OID, read by
    /// `vacuum_rel` (`commands/vacuum.c`) to switch userid before vacuuming.
    pub fn rel_relowner(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);
seam_core::seam!(
    /// `onerel->rd_rel->reltoastrelid` — the relation's TOAST relation OID
    /// (`InvalidOid` if none), read by `vacuum_rel` (`commands/vacuum.c`) to
    /// remember the TOAST table for the later recursive vacuum.
    pub fn rel_reltoastrelid(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);
seam_core::seam!(
    /// `relation->rd_toastoid` — the CLUSTER/rewrite transient toast-OID hack
    /// (`InvalidOid` when off), read by `toast_save_datum`
    /// (`access/common/toast_internals.c`) to decide whether to substitute the
    /// permanent toast table's OID into the result pointer and to preserve the
    /// old toast value OID during a table rewrite.
    pub fn rel_rd_toastoid(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);
seam_core::seam!(
    /// `(StdRdOptions *) onerel->rd_options` viewed for `vacuum_rel`
    /// (`commands/vacuum.c`): the `index_cleanup` / `max_eager_freeze_failure_rate`
    /// / `truncate` reloptions it consults when the corresponding GUC-derived
    /// option is unspecified. See [`StdRdOptionsView`].
    pub fn rel_std_rd_options(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<StdRdOptionsView>
);
seam_core::seam!(
    /// `onerel->rd_lockInfo.lockRelId` — the `(relId, dbId)` pair `vacuum_rel`
    /// (`commands/vacuum.c`) hands to `LockRelationIdForSession` /
    /// `UnlockRelationIdForSession` for the session-level lock.
    pub fn rel_lock_relid(
        rel: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_storage::lock::LockRelId>
);

seam_core::seam!(
    /// `TupleDescCompactAttr(RelationGetDescr(rel), attnum - 1)->attnullability
    /// = attnullability` (set_attnotnull, commands/tablecmds.c): poke the live
    /// relcache entry's compact-attribute nullability state for one column. The
    /// in-place mutation of the cached `RelationData` C performs through the
    /// relation pointer; expressed here as a by-OID mutator over the relcache
    /// owner's `with_relation_mut`. `Err` if the entry is absent.
    pub fn set_relcache_attnullability(
        relid: types_core::primitive::Oid,
        attnum: types_core::primitive::AttrNumber,
        attnullability: i8,
    ) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// Relation-ref resource-owner bookkeeping (relcache.c
// `RelationIncrementReferenceCount` / `RelationDecrementReferenceCount`, which
// call `ResourceOwnerEnlarge` / `ResourceOwnerRememberRelationRef` /
// `ResourceOwnerForgetRelationRef`). The relation-ref `ResourceOwnerDesc`
// (`relref_resowner_desc`) is defined in relcache.c; in this port the resowner
// owner crate holds the descriptor and installs the three remember/forget/
// enlarge seams below (mirroring the buffer-pin/buffer-IO arrangement), so the
// relcache owner can wire its refcount lifecycle to the current resource owner.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ResourceOwnerEnlarge(CurrentResourceOwner)` (relcache.c, before
    /// `ResourceOwnerRememberRelationRef`) — ensure the current resource owner
    /// has room to remember one more relation ref so the remember below cannot
    /// fail. `Err` carries the `ereport(ERROR)` on memory exhaustion. Installed
    /// by the resowner owner crate.
    pub fn resource_owner_enlarge_relation() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerRememberRelationRef(CurrentResourceOwner, rel)`
    /// (relcache.c) — record one relcache pin on the current resource owner so a
    /// transaction/portal abort can release the leaked pin. The relation is
    /// identified by its `Oid` handle. Installed by the resowner owner crate.
    pub fn resource_owner_remember_relation(relid: types_core::primitive::Oid)
);

seam_core::seam!(
    /// `ResourceOwnerForgetRelationRef(CurrentResourceOwner, rel)` (relcache.c)
    /// — drop the record of one relcache pin from the current resource owner.
    /// Installed by the resowner owner crate.
    pub fn resource_owner_forget_relation(relid: types_core::primitive::Oid)
);

seam_core::seam!(
    /// `ResOwnerReleaseRelation(Datum res)` (relcache.c) — the `ReleaseResource`
    /// callback of `relref_resowner_desc`: release a leaked relcache pin found
    /// during resource-owner release. Decrements the entry's refcount WITHOUT
    /// re-forgetting it from the (already-being-released) owner, then runs the
    /// `RelationCloseCleanup` drop-of-invalidated path. Installed by the
    /// relcache owner crate; called by the resowner desc. `Err` carries any
    /// `ereport`.
    pub fn release_relation_ref(relid: types_core::primitive::Oid) -> types_error::PgResult<()>
);

/// One index key column's identity for the `ATExecReplicaIdentity` nullability
/// check: the table column number `indkey.values[key]` and, when it is a real
/// (non-system, non-expression) column, that column's `attname`. System columns
/// (`attno <= 0`, including the `0` expression-column marker) are surfaced via
/// `attno` alone; the caller raises the system-column error before touching the
/// table descriptor.
#[derive(Clone, Debug)]
pub struct ReplidentKeyColumn {
    /// `indexRel->rd_index->indkey.values[key]` — the table column number.
    pub attno: i16,
}

/// Everything `ATExecReplicaIdentity` (tablecmds.c) reads off the opened index
/// relation (`index_open(indexOid, ShareLock)`): `rd_index` flags, the AM's
/// `amcanunique`, the expression/predicate presence, and the key-column list.
/// The `index_open`/`index_close(.., NoLock)` pin lifecycle is owned by the
/// relcache installer; the `ShareLock` is taken by the caller before this seam.
#[derive(Clone, Debug)]
pub struct ReplidentIndexInfo {
    /// `indexRel->rd_index != NULL` — the relation is in fact an index.
    pub is_index: bool,
    /// `indexRel->rd_index->indrelid` — the table the index is for.
    pub indrelid: types_core::primitive::Oid,
    /// `indexRel->rd_indam->amcanunique` — the AM supports uniqueness.
    pub amcanunique: bool,
    /// `indexRel->rd_index->indisunique`.
    pub indisunique: bool,
    /// `indexRel->rd_index->indisexclusion`.
    pub indisexclusion: bool,
    /// `indexRel->rd_index->indimmediate`.
    pub indimmediate: bool,
    /// `RelationGetIndexExpressions(indexRel) != NIL` — an expression index.
    pub has_expressions: bool,
    /// `RelationGetIndexPredicate(indexRel) != NIL` — a partial index.
    pub has_predicate: bool,
    /// `indexRel->rd_index->indkey.values[0..indnkeyatts]` — the key columns.
    pub key_columns: Vec<ReplidentKeyColumn>,
}

seam_core::seam!(
    /// `index_open(indexOid, NoLock)` + the `rd_index`/`rd_indam`/expression/
    /// predicate reads `ATExecReplicaIdentity` (tablecmds.c:18490) performs on the
    /// opened index, projected into a [`ReplidentIndexInfo`] (the index is left
    /// closed at return, mirroring `index_close(indexRel, NoLock)`). The caller
    /// holds the `ShareLock` already; this only pins/reads/unpins the relcache
    /// entry. `Err` carries a `could not open index` failure.
    pub fn get_replident_index_info(
        index_oid: types_core::primitive::Oid,
    ) -> types_error::PgResult<ReplidentIndexInfo>
);
