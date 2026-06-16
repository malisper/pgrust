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
    /// describing the open index relation. The owned `IndexInfo` is trimmed
    /// to the fields consumers read, so no allocation crosses the seam yet;
    /// cache lookups can `elog(ERROR)`, carried on `Err`.
    pub fn build_index_info<'mcx>(
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
    pub fn index_build(
        heap: &types_rel::Relation<'_>,
        index: &types_rel::Relation<'_>,
        index_info: &types_nodes::execnodes::IndexInfo<'_>,
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
