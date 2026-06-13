//! Seam declarations for the interrupt-driven memory-context-logging surface
//! of `utils/mmgr/mcxt.c`.
//!
//! The core allocator lives in `crates/mcx`, which deliberately excludes this
//! surface: `ProcessLogMemoryContextInterrupt` needs procsignal state and
//! elog LOG emission (see `docs/mctx-design.md`). Whichever crate ports that
//! remainder installs these; until then a call panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

seam_core::seam!(
    /// `MemoryContextStrdup(context, string)` (mcxt.c): duplicate `s` into the
    /// given (foreign-owned) memory context. The PREPARE/EXECUTE driver copies
    /// the plan's query string into the portal's `portalContext` (owned by the
    /// portalmem unit, hence the opaque handle); the canonical copy lives in
    /// that context, and the returned owned `String` is the value the driver
    /// hands to `PortalDefineQuery`. Allocates / can `ereport(ERROR)`.
    pub fn memory_context_strdup(
        context: types_nodes::parsestmt::MemoryContextHandle,
        s: &str,
    ) -> types_error::PgResult<alloc::string::String>
);

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

// ---------------------------------------------------------------------------
// Named-child-context lifecycle handles consumed by logical decoding.
//
// DESIGN DEBT (DESIGN_DEBT.md): logical.c's decoding context is an
// `AllocSetContextCreate(CurrentMemoryContext, "Logical decoding context")`
// whose handle is threaded through `MemoryContextSwitchTo`/`Delete` and stored
// in `ctx->context`, plus the `makeStringInfo()` for `ctx->out`. The owner
// (mcxt/aset) is not ported, so these are opaque handles the owner resolves;
// once it lands as a real `mcx::Mcx`-creating API the context becomes an owned
// `Mcx<'mcx>` value and these handle seams retire.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `AllocSetContextCreate(CurrentMemoryContext, "Logical decoding context",
    /// ALLOCSET_DEFAULT_SIZES)`.
    pub fn create_logical_decoding_context_memcxt() -> types_logical::MemoryContextHandle
);
seam_core::seam!(
    /// `MemoryContextSwitchTo(context)` — returns the previous context.
    pub fn MemoryContextSwitchTo(context: types_logical::MemoryContextHandle) -> types_logical::MemoryContextHandle
);
seam_core::seam!(
    /// `MemoryContextDelete(context)`.
    pub fn MemoryContextDelete(context: types_logical::MemoryContextHandle)
);
seam_core::seam!(
    /// `makeStringInfo()` — allocate an empty output buffer (`ctx->out`).
    pub fn makeStringInfo() -> types_logical::StringInfoHandle
);
