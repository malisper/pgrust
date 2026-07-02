seam_core::seam!(
    // LogMemoryContextPending — per-backend volatile flag owned by mcxt.c.
    pub fn log_memory_context_pending() -> bool
);

seam_core::seam!(
    // ProcessLogMemoryContextInterrupt() — ereport paths exist in C.
    pub fn process_log_memory_context_interrupt() -> types_error::PgResult<()>
);
