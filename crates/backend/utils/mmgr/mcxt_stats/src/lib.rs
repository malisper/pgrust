//! mcxt.c stats surface: per-backend root-context registry (thread-native
//! stand-in for TopMemoryContext linkage) + the log-memory-context trio.

use std::cell::RefCell;

use ::mcx::{RootWeak, TreeStats};
use ::types_error::{ErrorLocation, PgResult, LOG_SERVER_ONLY};
use elog::ereport;

thread_local! {
    static ROOTS: RefCell<Vec<RootWeak>> = const { RefCell::new(Vec::new()) };
}

fn observe_root(w: RootWeak) {
    ROOTS.with(|r| {
        let mut v = r.borrow_mut();
        if v.len() == v.capacity() {
            v.retain(RootWeak::is_live);
        }
        v.push(w);
    });
}

/// Live root context trees created on this thread, oldest first.
pub fn backend_context_forest() -> Vec<TreeStats> {
    ROOTS.with(|r| {
        let mut v = r.borrow_mut();
        v.retain(RootWeak::is_live);
        v.iter().filter_map(RootWeak::tree_stats).collect()
    })
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn handle_log_memory_context_interrupt() {
    init_small::globals::SetLogMemoryContextPending(true);
    init_small::globals::SetInterruptPending(true);
}

fn log_memory_context_pending() -> bool {
    init_small::globals::LogMemoryContextPending()
}

// MemoryContextStatsDetail(_, 100, 100, false) shape; C divergence:
// allocator-native free accounting (footprint - used, no chunk counts).
fn process_log_memory_context_interrupt() -> PgResult<()> {
    init_small::globals::SetLogMemoryContextPending(false);

    ereport(LOG_SERVER_ONLY)
        .errmsg(format!(
            "logging memory contexts of PID {}",
            init_small::globals::MyProcPid()
        ))
        .finish(loc("ProcessLogMemoryContextInterrupt"))?;

    const MAX_CHILDREN_PER_LEVEL: usize = 100;
    let mut grand_total = 0usize;
    let mut grand_used = 0usize;
    for root in backend_context_forest() {
        log_tree(&root, 1, MAX_CHILDREN_PER_LEVEL, &mut grand_total, &mut grand_used)?;
    }
    ereport(LOG_SERVER_ONLY)
        .errmsg(format!(
            "Grand total: {grand_total} bytes; {grand_used} used"
        ))
        .finish(loc("ProcessLogMemoryContextInterrupt"))?;
    Ok(())
}

fn log_tree(
    t: &TreeStats,
    level: usize,
    max_children: usize,
    grand_total: &mut usize,
    grand_used: &mut usize,
) -> PgResult<()> {
    // Aset/Malloc backends do not publish block bytes into Acct (exact
    // per-chunk charge instead); floor total at used so an aset context
    // never dumps as "0 total" with megabytes used (mcxtfuncs convention).
    let total = t.arena_footprint.max(t.used);
    let used = t.used;
    let free = total.saturating_sub(used);
    *grand_total += total;
    *grand_used += used;
    let ident = match &t.ident {
        Some(id) => format!(": {id}"),
        None => String::new(),
    };
    ereport(LOG_SERVER_ONLY)
        .errmsg(format!(
            "level: {level}; {}{ident}: {total} total in {} blocks; {free} free; {used} used [{}]",
            t.name,
            t.nblocks.max(1),
            t.kind
        ))
        .finish(loc("MemoryContextStatsInternal"))?;
    for child in t.children.iter().take(max_children) {
        log_tree(child, level + 1, max_children, grand_total, grand_used)?;
    }
    if t.children.len() > max_children {
        ereport(LOG_SERVER_ONLY)
            .errmsg(format!(
                "level: {}; {} more child contexts not shown",
                level + 1,
                t.children.len() - max_children
            ))
            .finish(loc("MemoryContextStatsInternal"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Session-memory teardown (FPBUDGET-1): the thread-local phased LIFO behind
// mcx::register_session_cleanup / mcx::session_root. The backend runner
// (launch_backend) drains it once at clean task end — C's
// process-exit-frees-TopMemoryContext, made explicit for the thread model.
//
// v2 (train-29 bounce fix): three phases drained in order — Portals, State,
// Roots — porting C's exit order (portal cleanup inside the exit-callback
// ceremony; memory dies last, see the phase doc in mcx). Every cleanup runs
// under catch_unwind: cleanup paths must be panic-free by construction
// (tolerate absent state), and if one still panics we degrade to a stderr
// WARNING and keep draining rather than letting the panic cross Drop glue
// and abort the whole threaded server (the ipc::run_callback_guarded
// discipline). The guard is defense in depth, not the fix: the phase order
// plus the launch_backend crash-exit gate are what remove the t29 abort.
// ---------------------------------------------------------------------------

use ::mcx::SessionCleanupPhase;

thread_local! {
    static SESSION_CLEANUPS: [RefCell<Vec<Box<dyn FnOnce()>>>; 3] =
        const { [RefCell::new(Vec::new()), RefCell::new(Vec::new()), RefCell::new(Vec::new())] };
}

fn phase_index(phase: SessionCleanupPhase) -> usize {
    match phase {
        SessionCleanupPhase::Portals => 0,
        SessionCleanupPhase::State => 1,
        SessionCleanupPhase::Roots => 2,
    }
}

fn session_cleanup_push(phase: SessionCleanupPhase, f: Box<dyn FnOnce()>) {
    SESSION_CLEANUPS.with(|c| c[phase_index(phase)].borrow_mut().push(f));
}

/// Drain this thread's session cleanups: Portals, then State, then Roots;
/// newest first within each phase (C's callback LIFO discipline). Idempotent;
/// a cleanup registering further cleanups extends the drain — including into
/// an earlier phase, which the outer loop re-visits before finishing.
pub fn run_session_teardown() {
    loop {
        // Re-derive the first non-empty phase after EVERY cleanup: a
        // mid-drain registration into an earlier phase runs before any
        // later-phase work, so no Roots free can ever precede an owed
        // Portals/State cleanup.
        let next = SESSION_CLEANUPS.with(|c| {
            for (i, list) in c.iter().enumerate() {
                if let Some(f) = list.borrow_mut().pop() {
                    return Some((i, f));
                }
            }
            None
        });
        let Some((i, f)) = next else { return };
        // Absent-state tolerance is each cleanup's contract; the guard
        // keeps one bad cleanup from aborting the server.
        if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            let msg = e
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| e.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            eprintln!("WARNING: session-teardown cleanup panicked (phase {i}): {msg}");
        }
    }
}

/// Registered-cleanup count across all phases (leak-guard probes).
pub fn session_cleanup_count() -> usize {
    SESSION_CLEANUPS.with(|c| c.iter().map(|v| v.borrow().len()).sum())
}

// ---------------------------------------------------------------------------
// GL-MEMWATCH-1: C parity for aset.c's MemoryContextStats(TopMemoryContext)
// on allocation failure — dump the FAILING thread's context forest before the
// "out of memory" error propagates. Raw stderr (C's fprintf choice: the
// ereport path may itself allocate mid-OOM); the log collector captures it.
// Reentry-guarded: the dump's own formatting failing must not recurse.
// ---------------------------------------------------------------------------

fn fmt_tree(out: &mut String, t: &TreeStats, level: usize, max_children: usize, gt: &mut usize, gu: &mut usize) {
    use std::fmt::Write as _;
    let total = t.arena_footprint.max(t.used);
    let used = t.used;
    let free = total - used;
    *gt += total;
    *gu += used;
    let ident = match &t.ident {
        Some(id) => format!(": {id}"),
        None => String::new(),
    };
    let _ = writeln!(
        out,
        "level: {level}; {}{ident}: {total} total in {} blocks; {free} free; {used} used [{}]",
        t.name,
        t.nblocks.max(1),
        t.kind
    );
    for child in t.children.iter().take(max_children) {
        fmt_tree(out, child, level + 1, max_children, gt, gu);
    }
    if t.children.len() > max_children {
        let _ = writeln!(
            out,
            "level: {}; {} more child contexts not shown",
            level + 1,
            t.children.len() - max_children
        );
    }
}

fn oom_observer(context_name: &str, request: usize) {
    thread_local! {
        static IN_OOM_DUMP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if IN_OOM_DUMP.with(|c| c.replace(true)) {
        return;
    }
    let mut out = format!(
        "LOG:  memory context dump on allocation failure (request of size {request} in context \"{context_name}\", pid {})\n",
        init_small::globals::MyProcPid()
    );
    let mut grand_total = 0usize;
    let mut grand_used = 0usize;
    for root in backend_context_forest() {
        fmt_tree(&mut out, &root, 1, 100, &mut grand_total, &mut grand_used);
    }
    use std::fmt::Write as _;
    let _ = writeln!(
        out,
        "Grand total: {grand_total} bytes; {grand_used} used; process-wide context blocks: {} bytes",
        mcx::global_footprint::bytes()
    );
    elog::write_stderr(&out);
    IN_OOM_DUMP.with(|c| c.set(false));
}

pub fn init_seams() {
    mcx::set_root_observer(observe_root);
    mcx::set_session_cleanup_sink(session_cleanup_push);
    mcx::set_oom_observer(oom_observer);
    mcxt_seams::handle_log_memory_context_interrupt::set(handle_log_memory_context_interrupt);
    mcxt_seams::log_memory_context_pending::set(log_memory_context_pending);
    mcxt_seams::process_log_memory_context_interrupt::set(process_log_memory_context_interrupt);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forest_tracks_roots_and_prunes() {
        mcx::set_root_observer(observe_root);
        let a = mcx::MemoryContext::new("root-a");
        let _kid = a.new_child("kid");
        {
            let _b = mcx::MemoryContext::new_bump("root-b");
            let names: Vec<_> =
                backend_context_forest().iter().map(|t| t.name).collect();
            assert!(names.contains(&"root-a") && names.contains(&"root-b"));
        }
        let forest = backend_context_forest();
        let a_tree = forest.iter().find(|t| t.name == "root-a").unwrap();
        assert_eq!(a_tree.children.len(), 1);
        assert_eq!(a_tree.kind, "AllocSet");
        assert!(!forest.iter().any(|t| t.name == "root-b"));
    }
}
