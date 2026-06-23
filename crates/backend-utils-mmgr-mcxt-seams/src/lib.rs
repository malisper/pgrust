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
    /// `MemoryContextInit()` (mcxt.c): create the `TopMemoryContext` and
    /// `ErrorContext` that bootstrap the whole allocator. Called exactly once,
    /// very early in `main()`, before any other context exists.
    /// `elog(FATAL)` on allocation failure.
    pub fn memory_context_init() -> types_error::PgResult<()>
);

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

seam_core::seam!(
    /// `TopMemoryContext` (`mcxt.c`) — the long-lived backend context, as an
    /// `Mcx<'static>` handle. Callers that must allocate something outliving any
    /// short-lived caller context (e.g. a DSM descriptor / `on_dsm_detach`
    /// callback record) source the context here. Infallible.
    pub fn top_memory_context() -> mcx::Mcx<'static>
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

// ---------------------------------------------------------------------------
// Archiver's private memory context (pgarch.c).
//
// `archive_context = AllocSetContextCreate(TopMemoryContext, "archiver",
// ALLOCSET_DEFAULT_SIZES)`. The handle is created once in PgArchiverMain and
// thereafter switched into / reset around each archive_file_cb call. Same
// DESIGN_DEBT shape as the logical-decoding context above: the mmgr owner
// resolves these opaque handles; the calling sequence (when to switch and when
// to reset) is archiver-private logic that lives in backend-postmaster-pgarch.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `AllocSetContextCreate(TopMemoryContext, "archiver",
    /// ALLOCSET_DEFAULT_SIZES)` (pgarch.c PgArchiverMain).
    pub fn create_archiver_memcxt() -> types_logical::MemoryContextHandle
);
seam_core::seam!(
    /// `MemoryContextReset(context)` — release the context's child contexts
    /// and free all but the context's standard allocation, keeping the
    /// context itself reusable.
    pub fn MemoryContextReset(context: types_logical::MemoryContextHandle)
);

// ---------------------------------------------------------------------------
// Logical-decoding `ctx->out` StringInfo + memory-context backing store.
//
// The `makeStringInfo`/`create_logical_decoding_context_memcxt`/
// `MemoryContextSwitchTo`/`MemoryContextDelete` seams above are opaque-handle
// shaped because the mcx world has no ambient `CurrentMemoryContext` and the
// `StringInfoHandle`/`MemoryContextHandle` are bare `usize` newtypes. The
// logical-decoding output path (logicalfuncs / walsender / the builtin output
// plugin) needs a real backing store: the output plugin writes its textual
// decode into `ctx->out` (a `StringInfo`) and `LogicalOutputWrite` reads the
// finished bytes back out. This crate owns a shared, per-backend
// (thread-local) store that backs every such handle. It is small and
// process-local, exactly like C's per-backend StringInfo/MemoryContext.
//
// The `MemoryContextHandle` store is intentionally minimal: the logical
// decoding context and the plugin's private "text conversion context" are
// identity tokens that the decode path switches into / resets / deletes. The
// mcx world allocates everything in owned arenas, so there is no per-context
// allocation list to walk — a context handle is just a live/dead marker plus a
// name, and reset/delete are no-ops on the allocation side (the owned arenas
// die with their Rust owners). The StringInfo store IS a real buffer, since
// `ctx->out` content genuinely crosses from the plugin to the writer.
// ---------------------------------------------------------------------------

use core::cell::RefCell;
use types_logical::{MemoryContextHandle, StringInfoHandle};

struct LogicalMcxtStore {
    /// Live StringInfo buffers, keyed by handle id (handle.0). Index 0 is
    /// reserved as the C `NULL` StringInfo.
    strings: alloc::vec::Vec<Option<alloc::vec::Vec<u8>>>,
    /// Live memory contexts, keyed by handle id. Stores the name only (identity
    /// + liveness marker). Index 0 is the C `NULL` MemoryContext.
    contexts: alloc::vec::Vec<Option<alloc::string::String>>,
    /// The current context handle (`CurrentMemoryContext`), as last set by
    /// `MemoryContextSwitchTo`. `MemoryContextHandle(0)` means "no logical
    /// decoding context is current" (the default backend context).
    current: MemoryContextHandle,
}

impl LogicalMcxtStore {
    const fn new() -> Self {
        LogicalMcxtStore {
            strings: alloc::vec::Vec::new(),
            contexts: alloc::vec::Vec::new(),
            current: MemoryContextHandle(0),
        }
    }
}

thread_local! {
    static LOGICAL_MCXT_STORE: RefCell<LogicalMcxtStore> =
        const { RefCell::new(LogicalMcxtStore::new()) };
}

/// `makeStringInfo()` — allocate an empty `ctx->out` buffer and return its
/// handle. Backs the installed body of [`makeStringInfo`].
pub fn store_make_string_info() -> StringInfoHandle {
    LOGICAL_MCXT_STORE.with(|s| {
        let mut s = s.borrow_mut();
        // Reserve slot 0 as the C NULL StringInfo on first use.
        if s.strings.is_empty() {
            s.strings.push(None);
        }
        s.strings.push(Some(alloc::vec::Vec::new()));
        StringInfoHandle(s.strings.len() - 1)
    })
}

/// `resetStringInfo(s)` — clear the buffer's contents, keeping the allocation.
pub fn store_reset_string_info(handle: StringInfoHandle) {
    LOGICAL_MCXT_STORE.with(|s| {
        let mut s = s.borrow_mut();
        if let Some(Some(buf)) = s.strings.get_mut(handle.0) {
            buf.clear();
        }
    });
}

/// `appendBinaryStringInfo(s, data, datalen)` — append raw bytes to the buffer.
pub fn store_append_string_info(handle: StringInfoHandle, data: &[u8]) {
    LOGICAL_MCXT_STORE.with(|s| {
        let mut s = s.borrow_mut();
        if let Some(Some(buf)) = s.strings.get_mut(handle.0) {
            buf.extend_from_slice(data);
        }
    });
}

/// `s->data` / `s->len` — read the finished buffer contents back out (the bytes
/// `LogicalOutputWrite` turns into the `data text` result column).
pub fn store_read_string_info(handle: StringInfoHandle) -> alloc::vec::Vec<u8> {
    LOGICAL_MCXT_STORE.with(|s| {
        let s = s.borrow();
        match s.strings.get(handle.0) {
            Some(Some(buf)) => buf.clone(),
            _ => alloc::vec::Vec::new(),
        }
    })
}

/// `AllocSetContextCreate(...)` — create a logical-decoding memory context and
/// return its handle. Backs [`create_logical_decoding_context_memcxt`] and the
/// plugin-private "text conversion context".
pub fn store_create_context(name: &str) -> MemoryContextHandle {
    LOGICAL_MCXT_STORE.with(|s| {
        let mut s = s.borrow_mut();
        if s.contexts.is_empty() {
            s.contexts.push(None);
        }
        s.contexts.push(Some(alloc::string::String::from(name)));
        MemoryContextHandle(s.contexts.len() - 1)
    })
}

/// `MemoryContextSwitchTo(context)` — set the current context, returning the
/// previous one.
pub fn store_switch_to(context: MemoryContextHandle) -> MemoryContextHandle {
    LOGICAL_MCXT_STORE.with(|s| {
        let mut s = s.borrow_mut();
        let old = s.current;
        s.current = context;
        old
    })
}

/// `MemoryContextReset(context)` — no-op on the allocation side (owned arenas
/// die with their Rust owners); kept for faithful call-shape.
pub fn store_reset_context(_context: MemoryContextHandle) {
    // Nothing to free: the mcx world holds allocations in owned arenas.
}

/// `MemoryContextDelete(context)` — drop the context's liveness marker. The
/// decode path drops `ctx->out` implicitly when the whole context dies; the
/// buffers are tiny and the store is per-backend, so we only mark the context
/// slot dead.
pub fn store_delete_context(context: MemoryContextHandle) {
    LOGICAL_MCXT_STORE.with(|s| {
        let mut s = s.borrow_mut();
        if let Some(slot) = s.contexts.get_mut(context.0) {
            *slot = None;
        }
        if s.current == context {
            s.current = MemoryContextHandle(0);
        }
    });
}
