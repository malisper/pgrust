seam_core::seam!(
    // ResetReindexState(nestLevel) (catalog/index.c).
    pub fn reset_reindex_state(nest_level: i32)
);

seam_core::seam!(
    // BuildDummyIndexInfo + index_build (catalog/index.c pair, the
    // RelationTruncateIndexes callee shape); seam because heap.c sits below
    // index.c in the crate graph, and IndexInfo's crate would cycle through
    // xact if it appeared in this signature.
    pub fn index_build_dummy<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_relation: &types_rel::Relation<'mcx>,
        index_relation: &types_rel::Relation<'mcx>,
        isreindex: bool,
    ) -> types_error::PgResult<()>
);
