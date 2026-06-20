//! Seam declarations for the `backend-catalog-index` unit
//! (`catalog/index.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

/// Arguments to [`index_create`], mirroring the C `index_create(...)`
/// parameter list (catalog/index.c) trimmed to the fields the current callers
/// supply. The C `IndexInfo *indexInfo` crosses by value; the index column
/// names cross as an owned `Vec<String>` (the C `const List *indexColNames`);
/// the C `const Oid *collationIds` / `const Oid *opclassIds` /
/// `const int16 *coloptions` arrays cross as owned `Vec`s. The C
/// `const Datum *opclassOptions` (per-column attoptions, one `Datum` per index
/// attribute) crosses as [`IndexCreateArgs::opclass_options`]: `None` is the C
/// NULL `opclassOptions` (no per-column opclass options — the common case);
/// `Some(vec)` carries one canonical `Datum` per index attribute (`Datum::null()`
/// for a column with no options). `stattargets` remains NULL/ignored at the
/// current call sites and is not carried.
#[derive(Debug)]
pub struct IndexCreateArgs<'mcx> {
    /// `const char *indexRelationName`.
    pub index_relation_name: std::string::String,
    /// `Oid indexRelationId`.
    pub index_relation_id: types_core::primitive::Oid,
    /// `Oid parentIndexRelid`.
    pub parent_index_relid: types_core::primitive::Oid,
    /// `Oid parentConstraintId`.
    pub parent_constraint_id: types_core::primitive::Oid,
    /// `RelFileNumber relFileNumber`.
    pub rel_file_number: types_core::primitive::Oid,
    /// `IndexInfo *indexInfo`.
    pub index_info: types_nodes::execnodes::IndexInfo<'mcx>,
    /// `const List *indexColNames`.
    pub index_col_names: std::vec::Vec<std::string::String>,
    /// `Oid accessMethodId`.
    pub access_method_id: types_core::primitive::Oid,
    /// `Oid tableSpaceId`.
    pub table_space_id: types_core::primitive::Oid,
    /// `const Oid *collationIds`.
    pub collation_ids: std::vec::Vec<types_core::primitive::Oid>,
    /// `const Oid *opclassIds`.
    pub opclass_ids: std::vec::Vec<types_core::primitive::Oid>,
    /// `const int16 *coloptions`.
    pub coloptions: std::vec::Vec<i16>,
    /// `Datum reloptions`.
    pub reloptions: types_tuple::Datum<'mcx>,
    /// `const Datum *opclassOptions` — per-column attoptions (one canonical
    /// `Datum` per index attribute; `Datum::null()` for a column with no
    /// options). `None` ⇒ the C NULL `opclassOptions` (no per-column opclass
    /// options at all).
    pub opclass_options: std::option::Option<std::vec::Vec<types_tuple::Datum<'mcx>>>,
    /// `bits16 flags`.
    pub flags: u16,
    /// `bits16 constr_flags`.
    pub constr_flags: u16,
    /// `bool allow_system_table_mods`.
    pub allow_system_table_mods: bool,
    /// `bool is_internal`.
    pub is_internal: bool,
}

seam_core::seam!(
    /// `index_create(heapRelation, ...)` (catalog/index.c): create the
    /// catalog entries for a new index and build it. Returns
    /// `(indexRelationId, createdConstraintId)` — the new index relation's OID
    /// and the OID of the constraint created for it (the C `Oid *constraintId`
    /// out-parameter; `InvalidOid` when no constraint was created, mirroring the
    /// C contract where `*constraintId` is only written on the
    /// `INDEX_CREATE_ADD_CONSTRAINT` path). `Err` carries the catalog-mutation /
    /// validation `ereport(ERROR)`s and OOM. The open `heapRelation` crosses by
    /// reference; the caller retains ownership and closes it afterward.
    pub fn index_create<'mcx>(
        heap_relation: &types_rel::Relation<'mcx>,
        args: IndexCreateArgs<'mcx>,
    ) -> types_error::PgResult<(types_core::primitive::Oid, types_core::primitive::Oid)>
);

/// `IndexStateFlagsAction` (`catalog/index.h`) — the state transition
/// `index_set_state_flags` applies to a `pg_index` row. Owned by this unit
/// (catalog/index.c is its sole producer/consumer); defined here so the seam
/// can name it without a dependency on the parsenodes crate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub enum IndexStateFlagsAction {
    /// `INDEX_CREATE_SET_READY` — set `indisready` during CREATE INDEX CONCURRENTLY.
    SetReady = 0,
    /// `INDEX_CREATE_SET_VALID` — set `indisvalid` during CREATE INDEX CONCURRENTLY.
    SetValid,
    /// `INDEX_DROP_CLEAR_VALID` — clear `indisvalid` (+ `indisclustered`/`indisreplident`).
    DropClearValid,
    /// `INDEX_DROP_SET_DEAD` — clear `indisready`/`indislive` during DROP INDEX CONCURRENTLY.
    DropSetDead,
}

seam_core::seam!(
    /// `index_constraint_create(heapRelation, indexRelationId, parentConstraintId,
    /// indexInfo, constraintName, constraintType, constr_flags,
    /// allow_system_table_mods, is_internal)` (catalog/index.c): register a
    /// constraint (PRIMARY KEY / UNIQUE / EXCLUDE) for an existing index — build
    /// the `pg_constraint` entry, its dependencies, the deferred-uniqueness
    /// trigger (if deferrable), and optionally mark the index primary/deferred.
    /// Returns the constraint's `ObjectAddress`. `Err` carries the
    /// catalog-mutation `ereport(ERROR)`s.
    pub fn index_constraint_create<'mcx>(
        heap_relation: &types_rel::Relation<'_>,
        index_relation_id: types_core::primitive::Oid,
        parent_constraint_id: types_core::primitive::Oid,
        index_info: &types_nodes::execnodes::IndexInfo<'mcx>,
        constraint_name: &str,
        constraint_type: i8,
        constr_flags: u16,
        allow_system_table_mods: bool,
        is_internal: bool,
    ) -> types_error::PgResult<types_catalog::catalog_dependency::ObjectAddress>
);

seam_core::seam!(
    /// `index_set_state_flags(indexId, action)` (catalog/index.c): perform a
    /// non-transactional `pg_index` flag transition (CREATE/DROP INDEX
    /// CONCURRENTLY). The C runs in `CurrentMemoryContext`; the owned model
    /// threads the caller's `mcx` for the syscache copy + catalog update.
    /// `Err` carries the catalog `ereport(ERROR)` surface.
    pub fn index_set_state_flags<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index_id: types_core::primitive::Oid,
        action: IndexStateFlagsAction,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `DefineIndex`'s partitioned-recursion `invalidate_parent` update
    /// (indexcmds.c:1573): transactionally clear `pg_index.indisvalid` for the
    /// parent partitioned index when an attached child index is itself invalid.
    /// Unlike `index_set_state_flags` (a non-transactional CONCURRENTLY
    /// transition), this is an ordinary `CatalogTupleUpdate`. `Err` carries the
    /// catalog `ereport(ERROR)` surface (incl. the `cache lookup failed`).
    pub fn index_mark_invalid(index_relation_id: types_core::primitive::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `validatePartitionedIndex`'s pg_index update (tablecmds.c:21877):
    /// transactionally set `pg_index.indisvalid = true` for a partitioned index
    /// once all of its leaf partitions have a matching valid index. Like
    /// [`index_mark_invalid`] this is an ordinary `CatalogTupleUpdate` (not the
    /// non-transactional CONCURRENTLY `index_set_state_flags` path). `Err`
    /// carries the catalog `ereport(ERROR)` surface (incl. `cache lookup
    /// failed for index %u`).
    pub fn index_mark_valid(index_relation_id: types_core::primitive::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ResetReindexState(nestLevel)` — forget any active REINDEX at abort.
    pub fn reset_reindex_state(nest_level: i32)
);

seam_core::seam!(
    /// `ReindexIsProcessingIndex(indexOid)` (catalog/index.c): is the given
    /// index OID the one currently being reindexed, or pending reindex? Reads
    /// index.c's backend-local reindex state. Pure lookup; cannot `ereport`.
    pub fn reindex_is_processing_index(index_oid: types_core::primitive::Oid) -> bool
);

seam_core::seam!(
    /// `ReindexIsCurrentlyProcessingIndex(indexOid)` (catalog/index.c,
    /// file-static): is the given index OID the one *currently* being reindexed
    /// (the `currentlyReindexedIndex` global), ignoring the pending list? Reads
    /// index.c's backend-local reindex state. Pure lookup; cannot `ereport`.
    /// Used by `IndexCheckExclusion` (executor layer) to decide whether the
    /// target exclusion index must be un-suppressed before probing it.
    pub fn reindex_is_currently_processing_index(index_oid: types_core::primitive::Oid) -> bool
);

seam_core::seam!(
    /// `ResetReindexProcessing()` (catalog/index.c, file-static): clear the
    /// `currentlyReindexedHeap`/`currentlyReindexedIndex` globals, re-allowing
    /// use of the target index for index probes. (The reindexing nest level
    /// stays set until end of (sub)transaction.) Called by `IndexCheckExclusion`
    /// once the freshly-built exclusion index is fully valid. Cannot `ereport`.
    pub fn reset_reindex_processing()
);

seam_core::seam!(
    /// `BuildIndexInfo(index)` (catalog/index.c): construct an `IndexInfo`
    /// describing the open index relation, fetching any expression / predicate
    /// / exclusion info. The expression / predicate / exclusion legs allocate
    /// `PgVec<'mcx, …>` in the caller's `mcx`. Cache lookups and the
    /// `pg_node_tree` decode can `ereport(ERROR)`, carried on `Err`.
    pub fn build_index_info<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<types_nodes::execnodes::IndexInfo<'mcx>>
);

seam_core::seam!(
    /// `index_check_primary_key(heapRel, indexInfo, is_alter_table, stmt)`
    /// (catalog/index.c): apply the special checks before promoting an index to
    /// a PRIMARY KEY — no pre-existing primary key (ALTER TABLE / partition-of),
    /// no NULLS NOT DISTINCT index, and every key column marked NOT NULL (and not
    /// an expression). Reached from `ATExecAddIndexConstraint` (ADD CONSTRAINT
    /// ... PRIMARY KEY USING INDEX). The unused C `const IndexStmt *stmt` is
    /// omitted (its body never reads it). `Err` carries the `ereport(ERROR)`s.
    pub fn index_check_primary_key<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_rel: &types_rel::Relation<'mcx>,
        index_info: &types_nodes::execnodes::IndexInfo<'mcx>,
        is_alter_table: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FormIndexDatum(indexInfo, slot, estate, values, isnull)`
    /// (catalog/index.c): compute the index tuple's column values from the
    /// slot's row, evaluating index expressions in the estate's per-tuple
    /// context. The C fills caller-provided `Datum values[INDEX_MAX_KEYS]` /
    /// `bool isnull[INDEX_MAX_KEYS]` buffers; they return by value here.
    /// Expression evaluation can `ereport(ERROR)`, carried on `Err`.
    ///
    /// The result array carries the canonical per-attribute
    /// [`types_tuple::backend_access_common_heaptuple::Datum`] so a
    /// by-reference index key (text/varchar/name/numeric/uuid/macaddr/…) crosses
    /// as its `ByRef` byte image rather than collapsing to a bare machine word
    /// (which would panic the scalar accessor on a by-ref value). The downstream
    /// consumers — `index_insert`, the unique/exclusion `ScanKey`, and
    /// `backend-access-index-genam-seams::build_index_value_description` — all
    /// take this canonical `Datum`.
    pub fn form_index_datum<'mcx>(
        index_info: &types_nodes::execnodes::IndexInfo<'_>,
        slot: types_nodes::execnodes::SlotId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(
        [types_tuple::backend_access_common_heaptuple::Datum<'mcx>;
            types_core::fmgr::INDEX_MAX_KEYS as usize],
        [bool; types_core::fmgr::INDEX_MAX_KEYS as usize],
    )>
);

seam_core::seam!(
    /// `index_build(heapRelation, indexRelation, indexInfo, isreindex=false,
    /// parallel=false)` (index.c) as called from bootstrap's `build_indices`:
    /// scan the heap and fill the index. `Err` carries the build error surface.
    /// `indexInfo` crosses by `&mut` because the AM build edge needs a live
    /// `&mut IndexInfo<'mcx>` to construct the `IndexInfoCarrier` (#342), and
    /// the C `index_build` itself mutates `indexInfo->ii_ParallelWorkers`.
    pub fn index_build<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap: &types_rel::Relation<'mcx>,
        index: &types_rel::Relation<'mcx>,
        index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationTruncateIndexes(heapRelation)` (heap.c): truncate every index of
    /// the relation to zero tuples and rebuild it from the empty heap (using a
    /// dummy IndexInfo so no user code runs). Reached from `heap_truncate_one_rel`
    /// (catalog/heap.c, the ON COMMIT / in-place TRUNCATE path). Owned by
    /// `catalog/index.c` (where index_open / BuildDummyIndexInfo / index_build
    /// live). `Err` carries the rebuild `ereport(ERROR)`s.
    pub fn relation_truncate_indexes<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `plan_create_index_workers(tableOid, indexOid)` (planner.c) — decide how
    /// many parallel workers a CREATE INDEX should request. Reached from
    /// `index_build` only in normal processing mode for a parallel-capable AM.
    /// The planner is above this layer; the planner unit installs this from its
    /// `init_seams()`. Until then a call panics loudly. `Err` carries the
    /// planning `ereport(ERROR)` surface.
    pub fn plan_create_index_workers(table_oid: types_core::Oid, index_oid: types_core::Oid) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `index_build`'s unlogged-index init-fork emit leg (index.c): when the
    /// index is `RELPERSISTENCE_UNLOGGED` and no INIT fork yet exists,
    /// `smgrcreate(RelationGetSmgr(index), INIT_FORKNUM, false)` +
    /// `log_smgrcreate(&rd_locator, INIT_FORKNUM)` + `rd_indam->ambuildempty(index)`.
    /// Needs the smgr-create + WAL + AM-empty-build substrate (catalog/storage
    /// layer), which owns it and installs this from `init_seams()`. Until then a
    /// call panics loudly. `Err` carries the storage `ereport(ERROR)` surface.
    pub fn build_index_init_fork_if_needed<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_build`'s broken-HOT-chain leg (index.c): mark the just-built
    /// index `indcheckxmin = true` via a transactional `CatalogTupleUpdate` of
    /// its `INDEXRELID` pg_index tuple. Reached only on a non-concurrent,
    /// non-reindex build that found broken HOT chains. Needs the
    /// `SearchSysCacheCopy1(INDEXRELID)` + GETSTRUCT-field-write +
    /// `CatalogTupleUpdate` path (catalog-indexing layer). Until installed a
    /// call panics loudly. `Err` carries the catalog `ereport(ERROR)` surface.
    pub fn set_index_indcheckxmin(index_id: types_core::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `IndexCheckExclusion(heapRelation, indexRelation, indexInfo)` (index.c):
    /// after building an exclusion-constraint index, scan the heap a second
    /// time to verify the constraint holds. Needs the full executor table-scan
    /// + `check_exclusion_constraint` substrate (executor layer), which owns it
    /// and installs this from `init_seams()`. Until then a call panics loudly.
    /// `Err` carries the constraint-violation `ereport(ERROR)` surface.
    pub fn index_check_exclusion<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap: &types_rel::Relation<'mcx>,
        index: &types_rel::Relation<'mcx>,
        index_info: &types_nodes::execnodes::IndexInfo<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `index_update_stats(rel, hasindex, reltuples)` (catalog/index.c, a
    /// file-static helper of `index_build`/`index_create`): non-transactionally
    /// (`systable_inplace_update`) update the relation's `pg_class` row —
    /// `relhasindex`, and (when `reltuples >= 0 && !IsBinaryUpgrade`, gated by
    /// the autovacuum/relkind rules) `relpages`/`reltuples`/`relallvisible`/
    /// `relallfrozen` — then `CacheInvalidateRelcacheByTuple` if dirty.
    ///
    /// This is the pg_class field-level in-place mutation leg: it needs the
    /// `table_open(RelationRelationId)` + GETSTRUCT-field-write +
    /// `systable_inplace_update_finish` path over the live pg_class tuple, plus
    /// `RelationGetNumberOfBlocks` / `visibilitymap_count` /
    /// `AutoVacuumingActive` / `IsBinaryUpgrade`. That typed pg_class-row
    /// mutator lives in the catalog-indexing (pg_class write) layer, which owns
    /// `pg_class` writes and installs this from its `init_seams()`. Until then a
    /// call panics loudly (`mirror-pg-and-panic`). `Err` carries the catalog /
    /// buffer-lock `ereport(ERROR)` surface.
    pub fn index_update_stats(
        rel: &types_rel::Relation<'_>,
        hasindex: bool,
        reltuples: f64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `reindex_relation(NULL, relid, flags, &params)` (index.c) — rebuilds
    /// every index on the heap; ends with CommandCounterIncrement.
    pub fn reindex_relation<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relid: types_core::Oid,
        flags: i32,
        params: types_cluster::ReindexParams,
    ) -> types_error::PgResult<()>
);
seam_core::seam!(
    /// `reindex_index(stmt, indexId, skip_constraint_checks, persistence,
    /// params)` (catalog/index.c, file-static): rebuild one existing index in
    /// place — re-acquire locks, reset its relfilenumber, re-run `index_build`,
    /// reset `pg_index` validity, and fire the post-reindex hook. Reached from
    /// the non-concurrent `ReindexIndex` command driver (indexcmds.c).
    ///
    /// Owned by this unit (catalog/index.c) and installed from its
    /// `init_seams()`. `Err` carries the build/catalog `ereport(ERROR)` surface.
    pub fn reindex_index<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        stmt: &types_nodes::ddlnodes::ReindexStmt<'mcx>,
        index_id: types_core::Oid,
        skip_constraint_checks: bool,
        persistence: i8,
        params: types_cluster::ReindexParams,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `IndexGetRelation(indexId, missing_ok)` (index.c).
    pub fn index_get_relation(index_id: types_core::Oid, missing_ok: bool) -> types_error::PgResult<types_core::Oid>
);

seam_core::seam!(
    /// `index_drop(indexId, concurrent, concurrent_lock_mode)` (catalog/index.c):
    /// the per-class index drop handler dependency.c's `doDeletion` invokes for
    /// `RelationRelationId` objects that are indexes. Removes the index relation
    /// and its catalog rows. Can `ereport(ERROR)`, carried on `Err`.
    pub fn index_drop(
        index_id: types_core::Oid,
        concurrent: bool,
        concurrent_lock_mode: bool,
    ) -> types_error::PgResult<()>
);

/* ---------------------------------------------------------------------------
 * The REINDEX-with-SET-TABLESPACE leg of `reindex_index`.
 *
 * These three producers (`CheckRelationTableSpaceMove` / `SetRelationTableSpace`
 * from tablecmds.c, `RelationAssumeNewRelfilelocator` from relcache.c) are
 * reached by `reindex_index` only when `params.tablespaceOid` is valid — a path
 * no current caller exercises (`cluster` and `tablecmds`-TRUNCATE both pass a
 * default `ReindexParams` with `tablespace_oid == InvalidOid`). Their owning
 * units (commands/tablecmds.c, the relcache relfilelocator-mutation helper) are
 * not yet ported, so these inward seams stay UNINSTALLED and a call panics
 * loudly (mirror-PG-and-panic) until that lands. Declared here so `reindex_index`
 * can express the leg faithfully rather than dropping it.
 * ------------------------------------------------------------------------- */

seam_core::seam!(
    /// `CheckRelationTableSpaceMove(rel, newTableSpaceId)` (tablecmds.c): is a
    /// move of `rel` to `newTableSpaceId` actually needed (and permitted)?
    /// Returns `false` (no move needed) when the relation already lives in the
    /// target tablespace or has no storage; raises on a disallowed move. Owner =
    /// commands/tablecmds.c (unported). `Err` carries the `ereport(ERROR)`s.
    pub fn check_relation_table_space_move(
        rel: &types_rel::Relation<'_>,
        new_tablespace_id: types_core::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `SetRelationTableSpace(rel, newTableSpaceId, newRelFileNumber)`
    /// (tablecmds.c): update `rel`'s `pg_class` row to the new tablespace (and,
    /// if `newRelFileNumber` is valid, the new relfilenumber). Owner =
    /// commands/tablecmds.c (unported). `Err` carries the catalog
    /// `ereport(ERROR)`s.
    pub fn set_relation_table_space(
        rel: &types_rel::Relation<'_>,
        new_tablespace_id: types_core::Oid,
        new_rel_file_number: types_core::Oid,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationDropStorage(rel)` + `RelationAssumeNewRelfilelocator(rel)`
    /// (catalog/storage.c + relcache.c): schedule unlinking of `rel`'s current
    /// physical storage at commit and mark the relcache entry as assuming a new
    /// relfilelocator. Bundled because `reindex_index`'s SET TABLESPACE leg
    /// always performs them as a pair on the same open index relation. Owner =
    /// catalog/storage.c + relcache.c (the Relation-keyed form is unported).
    /// `Err` carries any `ereport(ERROR)`.
    pub fn drop_storage_assume_new_relfilelocator(
        rel: &types_rel::Relation<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BuildSpeculativeIndexInfo(index, ii)` (catalog/index.c): add to a
    /// unique-index `IndexInfo` the extra information speculative insertion
    /// (INSERT ... ON CONFLICT) needs — the per-key equality operators and
    /// their support procs/strategies (`ii_UniqueOps` / `ii_UniqueProcs` /
    /// `ii_UniqueStrats`), looked up from the index opclasses. Mutates
    /// `index_info` in place. Reached from `ExecOpenIndices(..., speculative)`.
    /// `Err` carries the opclass-lookup `ereport(ERROR)` surface.
    pub fn build_speculative_index_info<'mcx>(
        index: &types_rel::Relation<'mcx>,
        index_info: &mut types_nodes::execnodes::IndexInfo<'mcx>,
    ) -> types_error::PgResult<()>
);
