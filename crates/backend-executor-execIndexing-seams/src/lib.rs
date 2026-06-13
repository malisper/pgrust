//! Seam declarations for the `backend-executor-execIndexing` unit
//! (`executor/execIndexing.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecOpenIndices(resultRelInfo, speculative)` (execIndexing.c): open all
    /// indexes of the result relation and fill `ri_IndexRelationDescs` /
    /// `ri_IndexRelationInfo` / `ri_NumIndices` on the pooled `ResultRelInfo`.
    /// Allocates and reads the relcache, so fallible.
    pub fn exec_open_indices<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        speculative: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecInsertIndexTuples(resultRelInfo, slot, estate, update, noDupErr,
    /// specConflict, arbiterIndexes, onlySummarizing)` (execIndexing.c): insert
    /// index entries for the tuple in `slot`, returning the list of index OIDs
    /// whose predicates must be rechecked (the C returned `List *`). The
    /// allocated result lives in `mcx`. Index AM work can `ereport(ERROR)`.
    pub fn exec_insert_index_tuples<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        result_rel_info: types_nodes::RriId,
        slot: types_nodes::SlotId,
        update: bool,
        no_dup_err: bool,
        spec_conflict: Option<&mut bool>,
        arbiter_indexes: &[types_core::Oid],
        only_summarizing: bool,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_core::Oid>>
);
