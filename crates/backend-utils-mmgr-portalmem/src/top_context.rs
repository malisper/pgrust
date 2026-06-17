//! The process-global `TopMemoryContext` substrate for forked children
//! (`utils/mmgr/mcxt.c` `MemoryContextInit` / `palloc.h`
//! `MemoryContextSwitchTo(TopMemoryContext)`).
//!
//! # Why this lives here, and why it is a per-process (thread-local) leak
//!
//! In stock PG, `MemoryContextInit()` runs very early in `main()` and creates
//! the process-global `TopMemoryContext`; `MemoryContextSwitchTo` then flips a
//! process-global `CurrentMemoryContext`. A backend forked by the postmaster
//! inherits `TopMemoryContext` through `fork()` and, at the top of its `*Main`
//! routine, does `MemoryContextSwitchTo(TopMemoryContext)` to make it current
//! (`postmaster_child_launch`, the C aux-process / `BackendInitialize` path).
//!
//! This tree's `mcx` model deliberately has **no ambient current context**
//! (docs/mctx-design.md): every allocation threads an owned `Mcx<'mcx>`
//! explicitly, so there is nothing for a "switch" to flip. For the single-user
//! / standalone backend the binary shell (`bin/postgres.rs`) leaks one
//! `TopMemoryContext` and hands its `Mcx<'static>` to `pg_main`, which threads
//! it down explicitly — no global is needed.
//!
//! A postmaster-forked child, however, enters through
//! `postmaster_child_launch` (backend-postmaster-launch-backend), which has no
//! `Mcx` parameter to thread and instead calls the `switch_to_top_memory_context`
//! seam — the equivalent of the C child's `MemoryContextSwitchTo(TopMemoryContext)`
//! at `*Main` entry. To serve it (and the `top_memory_context()` seam that
//! consumers such as dsm `on_dsm_detach` records source) without threading a
//! handle, the child establishes its own root `TopMemoryContext` here, exactly
//! as C's child gets `TopMemoryContext` established at process start.
//!
//! It is a **per-process** root, modeled as a `thread_local!` holding a
//! `Box::leak`'d `&'static MemoryContext`, created lazily on first access. This
//! mirrors C (one `TopMemoryContext` per process, lives for the whole process)
//! and is the same proven idiom as the dsm test bring-up's `TopMemoryContext`
//! stand-in (`backend-storage-ipc-dsm-core`'s `TOP_MCX`). A `thread_local!`
//! (not a `static`) is required because `mcx::MemoryContext` is `!Sync` by
//! construction (interior `Cell`/`Rc`: a context belongs to one process/thread,
//! as in PG) and so cannot live in a `Sync` `static`.

use mcx::{Mcx, MemoryContext};

thread_local! {
    /// This process's `TopMemoryContext` (`mcxt.c`). Leaked so its handle is
    /// `'static` for the life of the process, matching C where
    /// `TopMemoryContext` is never freed. Created lazily on first access by a
    /// forked child's `switch_to_top_memory_context` / `top_memory_context`
    /// seam call — the equivalent of `MemoryContextInit` establishing it.
    static TOP_MEMORY_CONTEXT: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("TopMemoryContext")));
}

/// `TopMemoryContext` (`mcxt.c`) as an `Mcx<'static>` handle. Establishes the
/// per-process root context on first call (the `MemoryContextInit` analog) and
/// returns it. Infallible.
pub fn top_memory_context() -> Mcx<'static> {
    TOP_MEMORY_CONTEXT.with(|ctx| ctx.mcx())
}

/// `MemoryContextSwitchTo(TopMemoryContext)` (`palloc.h`), as called by a
/// forked child at `*Main` entry. In this mcx model there is no process-global
/// `CurrentMemoryContext` to flip — downstream code threads `Mcx` explicitly —
/// so the only observable effect of the C switch we must reproduce is that
/// `TopMemoryContext` exists and is reachable afterwards. Touching the
/// thread-local forces its lazy creation (the child's `MemoryContextInit`
/// equivalent); there is nothing further to flip. Infallible.
pub fn switch_to_top_memory_context() {
    TOP_MEMORY_CONTEXT.with(|_ctx| {});
}
