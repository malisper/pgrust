//! Seam declarations for the `backend-catalog-index` unit
//! (`catalog/index.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

/// Arguments to [`index_create`], mirroring the C `index_create(...)`
/// parameter list (catalog/index.c) trimmed to the fields the current callers
/// supply. The C `IndexInfo *indexInfo` crosses by value; the index column
/// names cross as an owned `Vec<String>` (the C `const List *indexColNames`);
/// the C `const Oid *collationIds` / `const Oid *opclassIds` /
/// `const int16 *coloptions` arrays cross as owned `Vec`s. `opclassOptions`,
/// `stattargets`, and the `Oid *constraintId` out-parameter are NULL/ignored
/// at the current call sites and are not carried.
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
    /// catalog entries for a new index and build it. Returns the new index
    /// relation's OID. `Err` carries the catalog-mutation / validation
    /// `ereport(ERROR)`s and OOM. The open `heapRelation` crosses by
    /// reference; the caller retains ownership and closes it afterward.
    pub fn index_create<'mcx>(
        heap_relation: &types_rel::Relation<'_>,
        args: IndexCreateArgs<'mcx>,
    ) -> types_error::PgResult<types_core::primitive::Oid>
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
    /// `FormIndexDatum(indexInfo, slot, estate, values, isnull)`
    /// (catalog/index.c): compute the index tuple's column values from the
    /// slot's row, evaluating index expressions in the estate's per-tuple
    /// context. The C fills caller-provided `Datum values[INDEX_MAX_KEYS]` /
    /// `bool isnull[INDEX_MAX_KEYS]` buffers; they return by value here.
    /// Expression evaluation can `ereport(ERROR)`, carried on `Err`.
    ///
    /// The result array stays on the word-model `types_datum::Datum` (rather
    /// than the canonical `Datum`): the sole consumer feeds it straight
    /// into `backend-access-index-genam-seams::build_index_value_description`,
    /// whose `values: &[types_datum::Datum]` contract is owned outside this
    /// batch. Migrating the element type here would diverge from that landed
    /// contract; it follows when genam migrates.
    pub fn form_index_datum<'mcx>(
        index_info: &types_nodes::execnodes::IndexInfo<'_>,
        slot: types_nodes::execnodes::SlotId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(
        [types_datum::Datum; types_core::fmgr::INDEX_MAX_KEYS as usize],
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
