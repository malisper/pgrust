seam_core::seam!(
    // StartupReorderBuffer (reorderbuffer.c): remove serialized spill files.
    pub fn startup_reorder_buffer() -> types_error::PgResult<()>
);
