seam_core::seam!(
    // LogMemoryContextPending — per-backend volatile flag owned by mcxt.c.
    pub fn log_memory_context_pending() -> bool
);

seam_core::seam!(
    // ProcessLogMemoryContextInterrupt() — ereport paths exist in C.
    pub fn process_log_memory_context_interrupt() -> types_error::PgResult<()>
);

seam_core::seam!(
    // HandleLogMemoryContextInterrupt() (mcxt.c); signal-handler-reachable,
    // so the implementation must be allocation-free.
    pub fn handle_log_memory_context_interrupt()
);
