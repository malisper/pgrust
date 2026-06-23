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
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        speculative: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecCloseIndices(resultRelInfo)` (execIndexing.c): close the result
    /// relation's opened index descriptors (running each index AM's
    /// `index_insert_cleanup`, dropping the lock taken by `ExecOpenIndices`).
    /// A no-op when the relation has no opened indexes (`ri_NumIndices == 0`).
    /// Consumed by `ExecCloseResultRelations` (execUtils). Index AM cleanup can
    /// `ereport(ERROR)`.
    pub fn exec_close_indices<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
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
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        slot: nodes::SlotId,
        update: bool,
        no_dup_err: bool,
        spec_conflict: Option<&mut bool>,
        arbiter_indexes: &[types_core::Oid],
        only_summarizing: bool,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_core::Oid>>
);

seam_core::seam!(
    /// `ExecCheckIndexConstraints(resultRelInfo, slot, estate, conflictTid,
    /// tupleid, arbiterIndexes)` (execIndexing.c): non-conclusively check for a
    /// conflict in the arbiter indexes for ON CONFLICT. Returns `true` when no
    /// conflict was found; on `false` it sets `*conflict_tid` to the TID of the
    /// (possibly) conflicting tuple. Index AM work can `ereport(ERROR)`.
    pub fn exec_check_index_constraints<'mcx>(
        estate: &mut nodes::EStateData<'mcx>,
        result_rel_info: nodes::RriId,
        slot: nodes::SlotId,
        conflict_tid: &mut types_tuple::heaptuple::ItemPointerData,
        tupleid: &types_tuple::heaptuple::ItemPointerData,
        arbiter_indexes: &[types_core::Oid],
    ) -> types_error::PgResult<bool>
);
