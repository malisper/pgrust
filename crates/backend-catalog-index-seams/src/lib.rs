//! Seam declarations for the `backend-catalog-index` unit
//! (`catalog/index.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

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
    pub fn build_index_info(
        index: &types_rel::Relation<'_>,
    ) -> types_error::PgResult<types_nodes::execnodes::IndexInfo>
);

seam_core::seam!(
    /// `FormIndexDatum(indexInfo, slot, estate, values, isnull)`
    /// (catalog/index.c): compute the index tuple's column values from the
    /// slot's row, evaluating index expressions in the estate's per-tuple
    /// context. The C fills caller-provided `Datum values[INDEX_MAX_KEYS]` /
    /// `bool isnull[INDEX_MAX_KEYS]` buffers; they return by value here.
    /// Expression evaluation can `ereport(ERROR)`, carried on `Err`.
    pub fn form_index_datum<'mcx>(
        index_info: &types_nodes::execnodes::IndexInfo,
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
        index_info: &types_nodes::execnodes::IndexInfo,
    ) -> types_error::PgResult<()>
);
