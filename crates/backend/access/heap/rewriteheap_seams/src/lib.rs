seam_core::seam!(
    // CheckPointLogicalRewriteHeap() (rewriteheap.c:1155): remove logical
    // rewrite mapping files no decoding slot can still need, fsync the rest.
    // Called from CheckPointGuts (checkpoints and restartpoints).
    pub fn check_point_logical_rewrite_heap() -> types_error::PgResult<()>
);
