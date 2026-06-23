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
//! (not a `static`) is required because `::mcx::MemoryContext` is `!Sync` by
//! construction (interior `Cell`/`Rc`: a context belongs to one process/thread,
//! as in PG) and so cannot live in a `Sync` `static`.

use core::cell::Cell;
use core::cell::RefCell;
use mcx::{Mcx, MemoryContext, TreeStats};

thread_local! {
    /// This process's `TopMemoryContext` (`mcxt.c`). Leaked so its handle is
    /// `'static` for the life of the process, matching C where
    /// `TopMemoryContext` is never freed. Created lazily on first access by a
    /// forked child's `switch_to_top_memory_context` / `top_memory_context`
    /// seam call — the equivalent of `MemoryContextInit` establishing it.
    static TOP_MEMORY_CONTEXT: &'static MemoryContext =
        Box::leak(Box::new(MemoryContext::new("TopMemoryContext")));

    /// This process's `PostmasterContext` (`postmaster.c`): the postmaster's
    /// working memory context, a child of `TopMemoryContext`
    /// (`PostmasterContext = AllocSetContextCreate(TopMemoryContext,
    /// "Postmaster", ...)`). Unlike `TopMemoryContext` it is **deletable**: a
    /// forked child calls `delete_postmaster_context`
    /// (`MemoryContextDelete(PostmasterContext); PostmasterContext = NULL`)
    /// once it has copied the startup data it needs out of it. So it is held
    /// as an owned `Option<MemoryContext>` (`NULL` once deleted), not a leaked
    /// `&'static`. It is `None` until the postmaster creates it in
    /// `PostmasterMain` (via [`create_postmaster_context`]); the forked child
    /// inherits the populated slot across `fork()` and then takes+drops it.
    static POSTMASTER_CONTEXT: RefCell<Option<MemoryContext>> =
        const { RefCell::new(None) };

    /// `LogMemoryContextPending` (`mcxt.c`): a per-backend
    /// `volatile sig_atomic_t`, set by `HandleLogMemoryContextInterrupt()` when
    /// another backend sends `PROCSIG_LOG_MEMORY_CONTEXT`, read every
    /// `CHECK_FOR_INTERRUPTS()`, and cleared by
    /// `ProcessLogMemoryContextInterrupt()`. Default `false` — the happy path
    /// never sets it. Modeled as a `thread_local!` `Cell<bool>` (one per
    /// process, as in C where it is a process global).
    static LOG_MEMORY_CONTEXT_PENDING: Cell<bool> = const { Cell::new(false) };

    /// `LogMemoryContextInProgress` (`mcxt.c`): re-entrancy guard set while
    /// `ProcessLogMemoryContextInterrupt()` is dumping, so a second pending
    /// request that arrives mid-dump does not recurse. Per-backend, default
    /// `false`.
    static LOG_MEMORY_CONTEXT_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
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

/// `PostmasterContext = AllocSetContextCreate(TopMemoryContext, "Postmaster",
/// ALLOCSET_DEFAULT_SIZES); MemoryContextSwitchTo(PostmasterContext)`
/// (`postmaster.c`, early in `PostmasterMain`). Establishes the postmaster's
/// working memory context as a child of this process's `TopMemoryContext`. The
/// `MemoryContextSwitchTo` has no effect in this mcx model (no ambient current
/// context — downstream threads `Mcx` explicitly), so the observable effect we
/// reproduce is that `PostmasterContext` exists and is reachable, and is freed
/// by the forked child via [`delete_postmaster_context`]. Idempotent: if the
/// slot is already populated (re-entry), the existing context is kept. The
/// child inherits this populated slot across `fork()`. Infallible.
pub fn create_postmaster_context() {
    POSTMASTER_CONTEXT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            let child = TOP_MEMORY_CONTEXT.with(|top| top.new_child("Postmaster"));
            *slot = Some(child);
        }
    });
}

/// `MemoryContextDelete(PostmasterContext); PostmasterContext = NULL`
/// (`auxprocess.c` / `bgworker.c` / etc., right after the child switches to
/// `TopMemoryContext`): a freshly-forked child releases the postmaster's
/// working context after copying its startup data out. Guarded exactly like C's
/// `if (PostmasterContext) { ... }` — a no-op if the slot is already `NULL`
/// (e.g. the postmaster never created it). Dropping the owned `MemoryContext`
/// is the faithful `MemoryContextDelete` (fires reset callbacks, frees the
/// arena); clearing the slot is `PostmasterContext = NULL`. Infallible.
pub fn delete_postmaster_context() {
    POSTMASTER_CONTEXT.with(|slot| {
        // Take the value out (clearing the slot to `None` = `PostmasterContext
        // = NULL`) and let it drop = `MemoryContextDelete`. Done as a separate
        // step so the `RefCell` borrow is released before the (re-entrant-safe)
        // drop / reset-callbacks run.
        let ctx = slot.borrow_mut().take();
        drop(ctx);
    });
}

/// Read `LogMemoryContextPending` (`mcxt.c`): the per-backend
/// `volatile sig_atomic_t` checked in every `CHECK_FOR_INTERRUPTS()` /
/// `HandleMainLoopInterrupts()` to decide whether to call
/// `ProcessLogMemoryContextInterrupt()`. `false` on the happy path (it is only
/// set when another backend sends `PROCSIG_LOG_MEMORY_CONTEXT`), so this read
/// unblocks every aux-process `*Main` loop and `CHECK_FOR_INTERRUPTS()`.
pub fn log_memory_context_pending() -> bool {
    LOG_MEMORY_CONTEXT_PENDING.with(Cell::get)
}

/// `HandleLogMemoryContextInterrupt()` (`mcxt.c`), the
/// `PROCSIG_LOG_MEMORY_CONTEXT` arm of `procsignal_sigusr1_handler`:
///
/// ```c
/// void HandleLogMemoryContextInterrupt(void) {
///     InterruptPending = true;
///     LogMemoryContextPending = true;
///     /* latch will be set by procsignal_sigusr1_handler */
/// }
/// ```
///
/// Signal-handler-safe flag flipping: set this backend's pending flag and raise
/// the global `InterruptPending` so the next `CHECK_FOR_INTERRUPTS()` notices.
/// Infallible. (The latch set is done by `procsignal_sigusr1_handler`, not
/// here, exactly as in C.)
pub fn handle_log_memory_context_interrupt() {
    init_small_seams::set_interrupt_pending::call(true);
    LOG_MEMORY_CONTEXT_PENDING.with(|p| p.set(true));
}

/// `ProcessLogMemoryContextInterrupt()` (`mcxt.c`): clear the pending flag and
/// dump this backend's memory-context statistics to the server log.
///
/// Faithful to C:
/// - `LogMemoryContextPending = false;` first.
/// - re-entrancy guard `LogMemoryContextInProgress`: if already dumping, return
///   immediately (prevents recursion under rapid repeated requests).
/// - emit `LOG_SERVER_ONLY` "logging memory contexts of PID <MyProcPid>" with
///   `errhidestmt(true)/errhidecontext(true)` (kept off the client connection).
/// - `MemoryContextStatsDetail(TopMemoryContext, 100, 100, false)` — in the
///   non-`print_to_stderr` path C emits **one `ereport(LOG_SERVER_ONLY)` per
///   context** (`MemoryContextStatsPrint`: `"level: %d; %s: %s%s"`) plus a final
///   "Grand total" message. We reproduce that per-context-per-message shape over
///   this allocator's own context tree (`::mcx::MemoryContext::stats_tree()`, the
///   model's `MemoryContextStatsDetail` analog), with depth/child caps of 100.
/// - the in-progress flag is cleared in all paths (the `PG_FINALLY` analog) — a
///   propagating `ereport(ERROR)` here is returned as `Err`, and the guard is
///   reset before returning either way.
///
/// `MemoryContextStatsDetail` reports the stats this allocator genuinely tracks
/// (per-context `used` / `peak`, subtree totals). This model is a value
/// allocator without the block/freelist accounting of C's aset.c, so the
/// per-context line carries the real numbers it has rather than fabricated
/// block/free counts.
pub fn process_log_memory_context_interrupt() -> ::types_error::PgResult<()> {
    LOG_MEMORY_CONTEXT_PENDING.with(|p| p.set(false));

    // Exit immediately if a dump is already in progress (recursion guard).
    if LOG_MEMORY_CONTEXT_IN_PROGRESS.with(Cell::get) {
        return Ok(());
    }
    LOG_MEMORY_CONTEXT_IN_PROGRESS.with(|p| p.set(true));

    // PG_TRY/PG_FINALLY analog: run the dump, then always clear the in-progress
    // flag (even on a propagating ereport(ERROR), surfaced here as Err).
    let result = dump_memory_contexts();
    LOG_MEMORY_CONTEXT_IN_PROGRESS.with(|p| p.set(false));
    result
}

/// The body of [`process_log_memory_context_interrupt`]'s `PG_TRY` block:
/// `ereport`s the header line then walks the `TopMemoryContext` stats tree
/// (`MemoryContextStatsDetail(TopMemoryContext, 100, 100, false)`).
fn dump_memory_contexts() -> ::types_error::PgResult<()> {
    use ::utils_error::ereport;
    use ::types_error::LOG_SERVER_ONLY;

    let pid = init_small_seams::my_proc_pid::call();

    // ereport(LOG_SERVER_ONLY, (errhidestmt(true), errhidecontext(true),
    //          errmsg("logging memory contexts of PID %d", MyProcPid)));
    ereport(LOG_SERVER_ONLY)
        .errhidestmt(true)
        .errhidecontext(true)
        .errmsg(format!("logging memory contexts of PID {pid}"))
        .finish(loc(1320, "ProcessLogMemoryContextInterrupt"))?;

    // MemoryContextStatsDetail(TopMemoryContext, 100, 100, false).
    let tree = TOP_MEMORY_CONTEXT.with(|top| top.stats_tree());
    memory_context_stats_detail(&tree, 100, 100)
}

/// `MemoryContextStatsDetail(context, max_level, max_children, print_to_stderr
/// = false)` (`mcxt.c`) over this allocator's `TreeStats`. Emits one
/// `LOG_SERVER_ONLY` message per context (`MemoryContextStatsPrint`'s
/// `"level: %d; %s: %s%s"` shape, `level` starting at 1) plus the trailing
/// "Grand total" message, honoring the depth (`max_level`) and per-parent child
/// (`max_children`) caps by summarizing the remainder, as C does.
fn memory_context_stats_detail(
    root: &TreeStats,
    max_level: usize,
    max_children: usize,
) -> ::types_error::PgResult<()> {
    let mut grand_total_used: usize = 0;
    stats_internal(root, 1, max_level, max_children, &mut grand_total_used)?;

    // Grand total (errmsg_internal in C; LOG_SERVER_ONLY, hidden stmt/context).
    ::utils_error::ereport(::types_error::LOG_SERVER_ONLY)
        .errhidestmt(true)
        .errhidecontext(true)
        .errmsg(format!("Grand total: {grand_total_used} bytes used"))
        .finish(loc(866, "MemoryContextStatsDetail"))
}

/// One recursion level of `MemoryContextStatsInternal` (`mcxt.c`): emit this
/// context's stats line, accumulate into the grand total, then recurse into up
/// to `max_children` children while `level <= max_level`, summarizing the rest.
fn stats_internal(
    node: &TreeStats,
    level: usize,
    max_level: usize,
    max_children: usize,
    grand_total_used: &mut usize,
) -> ::types_error::PgResult<()> {
    use core::fmt::Write;

    *grand_total_used = grand_total_used.saturating_add(node.used);

    // MemoryContextStatsPrint: "level: %d; %s: %s%s" — name, the model's stats
    // string, then the ": <ident>" suffix when an ident is present (truncated
    // to 100 bytes / control chars spaced, as C does).
    let stats_string = format!(
        "{} used ({} in subtree); peak {} ({} subtree peak)",
        node.used, node.subtree_used, node.peak, node.subtree_peak
    );
    let mut line = format!("level: {level}; {}: {stats_string}", node.name);
    if let Some(ident) = &node.ident {
        line.push_str(": ");
        let mut idlen = 0usize;
        for c in ident.chars() {
            if idlen >= 100 {
                line.push_str("...");
                break;
            }
            // Replace ASCII control characters (e.g. newlines) with spaces.
            let _ = write!(line, "{}", if (c as u32) < 0x20 { ' ' } else { c });
            idlen += 1;
        }
    }
    ::utils_error::ereport(::types_error::LOG_SERVER_ONLY)
        .errhidestmt(true)
        .errhidecontext(true)
        .errmsg(line)
        .finish(loc(976, "MemoryContextStatsPrint"))?;

    // Examine children: explicitly recurse into the first max_children while
    // within the depth limit; summarize the remainder without recursing.
    if level <= max_level {
        for child in node.children.iter().take(max_children) {
            stats_internal(child, level + 1, max_level, max_children, grand_total_used)?;
        }
        let remaining = node.children.len().saturating_sub(max_children);
        if remaining != 0 {
            let mut summarized_used = 0usize;
            for child in node.children.iter().skip(max_children) {
                summarized_used = summarized_used.saturating_add(child.subtree_used);
            }
            *grand_total_used = grand_total_used.saturating_add(summarized_used);
            ::utils_error::ereport(::types_error::LOG_SERVER_ONLY)
                .errhidestmt(true)
                .errhidecontext(true)
                .errmsg(format!(
                    "level: {}; {} more child contexts containing {summarized_used} bytes used",
                    level + 1,
                    remaining
                ))
                .finish(loc(927, "MemoryContextStatsInternal"))?;
        }
    } else if !node.children.is_empty() {
        // Past the recursion depth limit: summarize all children.
        let mut summarized_used = 0usize;
        for child in &node.children {
            summarized_used = summarized_used.saturating_add(child.subtree_used);
        }
        *grand_total_used = grand_total_used.saturating_add(summarized_used);
        ::utils_error::ereport(::types_error::LOG_SERVER_ONLY)
            .errhidestmt(true)
            .errhidecontext(true)
            .errmsg(format!(
                "level: {}; {} child contexts containing {summarized_used} bytes used",
                level + 1,
                node.children.len()
            ))
            .finish(loc(927, "MemoryContextStatsInternal"))?;
    }
    Ok(())
}

/// `ErrorLocation` for the `mcxt.c` interrupt/stats functions.
fn loc(lineno: i32, func: &str) -> ::types_error::ErrorLocation {
    ::types_error::ErrorLocation::new("mcxt.c", lineno, func)
}
