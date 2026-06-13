//! Seam declarations for the interrupt-driven memory-context-logging surface
//! of `utils/mmgr/mcxt.c`.
//!
//! The core allocator lives in `crates/mcx`, which deliberately excludes this
//! surface: `ProcessLogMemoryContextInterrupt` needs procsignal state and
//! elog LOG emission (see `docs/mctx-design.md`). Whichever crate ports that
//! remainder installs these; until then a call panics loudly.

seam_core::seam!(
    /// Read `LogMemoryContextPending` (mcxt.c), the per-backend
    /// `volatile sig_atomic_t` set by `HandleLogMemoryContextInterrupt()`
    /// when another backend requests a memory-context dump.
    pub fn log_memory_context_pending() -> bool
);

seam_core::seam!(
    /// `ProcessLogMemoryContextInterrupt()` (mcxt.c): clear the pending flag
    /// and emit this process's memory-context stats to the server log. Runs
    /// under PG_TRY/PG_FINALLY, so an `ereport(ERROR)` propagates.
    pub fn process_log_memory_context_interrupt() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `HandleLogMemoryContextInterrupt()` (mcxt.c) — the
    /// PROCSIG_LOG_MEMORY_CONTEXT arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_log_memory_context_interrupt()
);

seam_core::seam!(
    /// `MemoryContextSwitchTo(TopMemoryContext)` (`mcxt.c` /
    /// `utils/palloc.h`): make `TopMemoryContext` the current allocation
    /// context.
    pub fn switch_to_top_memory_context()
);
