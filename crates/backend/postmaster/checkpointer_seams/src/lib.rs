seam_core::seam!(
    // RequestCheckpoint(flags) (checkpointer.c).
    pub fn request_checkpoint(flags: i32) -> types_error::PgResult<()>
);
