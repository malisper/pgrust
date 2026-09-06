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

// memgrowth-discriminator (suspect-A census): raw-Rust slot-registry census
// hook. The slot registries (execmain querydesc ENTRIES, pquery stmt_list,
// queryenvironment/tuplestore holds, the resowner arena) are thread-local
// Vec slabs on the global allocator — bytes neither the context ledger nor
// accounted_contexts can see. This crate sits below those crates, so the
// formatter is installed from seams_init (which depends on all of them);
// the census runs on WHICHEVER thread calls it, so the log-memory-contexts
// interrupt path reports the TARGET backend's registries.
static SLOT_CENSUS: std::sync::OnceLock<fn() -> String> = std::sync::OnceLock::new();

pub fn set_slot_census(f: fn() -> String) {
    let _ = SLOT_CENSUS.set(f);
}

/// This thread's slot-registry census line (None until seams_init installs).
pub fn slot_census_line() -> Option<String> {
    SLOT_CENSUS.get().map(|f| f())
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

// ProcessLogMemoryContextInterrupt (mcxt.c:1298): MemoryContextStatsDetail(
// TopMemoryContext, 100, 100, false) over this backend's context forest.
// C's re-entrancy guard (LogMemoryContextInProgress) has no counterpart: it
// fences errfinish's trailing CHECK_FOR_INTERRUPTS, which the Rust ereport
// path does not perform (elog stack.rs: "the interrupt machinery owns that"),
// so nothing below can re-enter here.
fn process_log_memory_context_interrupt() -> PgResult<()> {
    init_small::globals::SetLogMemoryContextPending(false);

    ereport(LOG_SERVER_ONLY)
        .errhidestmt(true)
        .errhidecontext(true)
        .errmsg(format!(
            "logging memory contexts of PID {}",
            init_small::globals::MyProcPid()
        ))
        .finish(loc("ProcessLogMemoryContextInterrupt"))?;

    // mcxt.c:1320: depth and children per parent both capped at 100. The
    // forest stands in for TopMemoryContext's subtree — every root is walked
    // at level 1 (C divergence: no TopMemoryContext parentage in the
    // ownership model, so no single level-1 line).
    let mut grand_totals = Counters::default();
    let mut emit = |line: String| {
        ereport(LOG_SERVER_ONLY)
            .errhidestmt(true)
            .errhidecontext(true)
            .errmsg_internal(line)
            .finish(loc("MemoryContextStatsInternal"))
    };
    for root in backend_context_forest() {
        stats_internal(&root, 1, 100, 100, &mut grand_totals, false, &mut emit)?;
    }
    // mcxt.c:864: one message per context, then the grand total — and
    // nothing after it.
    ereport(LOG_SERVER_ONLY)
        .errhidestmt(true)
        .errhidecontext(true)
        .errmsg_internal(grand_total_line(&grand_totals))
        .finish(loc("MemoryContextStatsDetail"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// MemoryContextStatsInternal / MemoryContextStatsPrint (mcxt.c:883-1048) over
// a TreeStats snapshot. One walker serves both print_to_stderr arms: the
// LOG_SERVER_ONLY dump ("level: N; ..." lines) and the stderr dump on
// allocation failure (two-space indentation per level, C's fprintf shape).
// ---------------------------------------------------------------------------

/// MemoryContextCounters (memnodes.h): what `methods->stats` accumulates.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
struct Counters {
    nblocks: usize,
    freechunks: usize,
    totalspace: usize,
    freespace: usize,
}

impl Counters {
    fn add(&mut self, o: &Counters) {
        self.nblocks += o.nblocks;
        self.freechunks += o.freechunks;
        self.totalspace += o.totalspace;
        self.freespace += o.freespace;
    }

    /// C prints `totalspace - freespace` as "used" everywhere.
    fn used(&self) -> usize {
        self.totalspace - self.freespace
    }
}

/// The context's own counters (C's per-allocator `stats` method). C
/// divergence: allocator-native accounting, the same convention as
/// mcxtfuncs put_context_row so the view and the dump agree — block
/// footprint where tracked, charged bytes otherwise (floored at used, so an
/// aset context never dumps as "0 total" with megabytes used); bump free
/// space is the block-transition window-tail snapshot; free-chunk counts are
/// not tracked (0).
fn counters_of(t: &TreeStats) -> Counters {
    let total = t.arena_footprint.max(t.used);
    let free = if t.is_bump { t.free_tail.min(total) } else { total - t.used };
    Counters { nblocks: t.nblocks.max(1), freechunks: 0, totalspace: total, freespace: free }
}

/// Each allocator's stats string (aset.c:1596, generation.c:1076, slab.c:978,
/// bump.c:713). Generation's allocated-chunk count and Slab's empty-block
/// count are not tracked either (0).
fn stats_string(kind: &str, c: &Counters) -> String {
    let (total, nblocks, free, chunks, used) =
        (c.totalspace, c.nblocks, c.freespace, c.freechunks, c.used());
    match kind {
        "Generation" => format!(
            "{total} total in {nblocks} blocks ({chunks} chunks); {free} free ({chunks} chunks); {used} used"
        ),
        "Slab" => format!(
            "{total} total in {nblocks} blocks; 0 empty blocks; {free} free ({chunks} chunks); {used} used"
        ),
        "Bump" => format!("{total} total in {nblocks} blocks; {free} free; {used} used"),
        _ => format!("{total} total in {nblocks} blocks; {free} free ({chunks} chunks); {used} used"),
    }
}

/// mcxt.c:1002 MemoryContextStatsPrint's label: a dynahash context is
/// labelled by its table name alone (mcxt.c:1018); the identifier, printed
/// AFTER the stats as ": <ident>", is clipped at 100 bytes on a character
/// boundary (pg_mbcliplen) with "..." appended, and ASCII control characters
/// (the newlines of a multi-line query) become spaces (mcxt.c:1027-1048).
fn print_label(t: &TreeStats) -> (&str, String) {
    let mut name: &str = t.name;
    let mut ident = t.ident.as_deref();
    if let Some(id) = ident {
        if name == "dynahash" {
            name = id;
            ident = None;
        }
    }
    let mut truncated_ident = String::new();
    if let Some(id) = ident {
        truncated_ident.push_str(": ");
        let mut idlen = id.len();
        let truncated = idlen > 100;
        if truncated {
            idlen = 100;
            while !id.is_char_boundary(idlen) {
                idlen -= 1;
            }
        }
        truncated_ident.extend(id[..idlen].chars().map(|c| if c < ' ' { ' ' } else { c }));
        if truncated {
            truncated_ident.push_str("...");
        }
    }
    (name, truncated_ident)
}

/// mcxt.c:883 MemoryContextStatsInternal: one recursion level. `emit` takes
/// each finished line (an ereport, or a push into the stderr buffer).
fn stats_internal(
    t: &TreeStats,
    level: usize,
    max_level: usize,
    max_children: usize,
    totals: &mut Counters,
    print_to_stderr: bool,
    emit: &mut dyn FnMut(String) -> PgResult<()>,
) -> PgResult<()> {
    let own = counters_of(t);
    let (name, truncated_ident) = print_label(t);
    let stats = stats_string(t.kind, &own);
    emit(if print_to_stderr {
        format!("{}{name}: {stats}{truncated_ident}", "  ".repeat(level - 1))
    } else {
        format!("level: {level}; {name}: {stats}{truncated_ident}")
    })?;
    totals.add(&own);

    // mcxt.c:905: past the depth limit or running low on stack, the children
    // are not printed; beyond max_children the rest are not printed either.
    // Both tails are summarized instead (never an error).
    let mut shown = 0;
    if level <= max_level && !stack_depth_core::stack_is_too_deep() {
        for child in t.children.iter().take(max_children) {
            stats_internal(child, level + 1, max_level, max_children, totals, print_to_stderr, emit)?;
            shown += 1;
        }
    }
    let rest = &t.children[shown..];
    if !rest.is_empty() {
        // mcxt.c:939: tally the unshown children AND their descendants
        // (MemoryContextTraverseNext), at the parent's own level.
        let mut local_totals = Counters::default();
        let mut ichild = 0usize;
        for child in rest {
            tally_subtree(child, &mut local_totals, &mut ichild);
        }
        let summary = format!(
            "{ichild} more child contexts containing {} total in {} blocks; {} free ({} chunks); {} used",
            local_totals.totalspace,
            local_totals.nblocks,
            local_totals.freespace,
            local_totals.freechunks,
            local_totals.used()
        );
        emit(if print_to_stderr {
            format!("{}{summary}", "  ".repeat(level))
        } else {
            format!("level: {level}; {summary}")
        })?;
        totals.add(&local_totals);
    }
    Ok(())
}

fn tally_subtree(t: &TreeStats, local_totals: &mut Counters, ichild: &mut usize) {
    local_totals.add(&counters_of(t));
    *ichild += 1;
    for child in &t.children {
        tally_subtree(child, local_totals, ichild);
    }
}

/// mcxt.c:864 MemoryContextStatsDetail's closing line.
fn grand_total_line(g: &Counters) -> String {
    format!(
        "Grand total: {} bytes in {} blocks; {} free ({} chunks); {} used",
        g.totalspace,
        g.nblocks,
        g.freespace,
        g.freechunks,
        g.used()
    )
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
        // unwind-ok: log-then-die
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
    // MemoryContextStats(TopMemoryContext) (aset.c): MemoryContextStatsDetail
    // (_, 100, 100, true) — the stderr shape.
    let mut grand_totals = Counters::default();
    {
        let mut emit = |line: String| {
            out.push_str(&line);
            out.push('\n');
            Ok(())
        };
        for root in backend_context_forest() {
            let _ = stats_internal(&root, 1, 100, 100, &mut grand_totals, true, &mut emit);
        }
    }
    use std::fmt::Write as _;
    let _ = writeln!(
        out,
        "{}; process-wide context blocks: {} bytes",
        grand_total_line(&grand_totals),
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

    fn node(name: &'static str, ident: Option<&str>, used: usize, children: Vec<TreeStats>) -> TreeStats {
        TreeStats {
            name,
            ident: ident.map(String::from),
            kind: "AllocSet",
            used,
            peak: used,
            subtree_used: used,
            subtree_peak: used,
            limit: 0,
            is_bump: false,
            arena_footprint: used + 8,
            nblocks: 1,
            free_tail: 0,
            children,
        }
    }

    fn dump(root: &TreeStats, max_level: usize, max_children: usize, to_stderr: bool) -> (Vec<String>, Counters) {
        let mut lines = Vec::new();
        let mut totals = Counters::default();
        let mut emit = |l: String| {
            lines.push(l);
            Ok(())
        };
        stats_internal(root, 1, max_level, max_children, &mut totals, to_stderr, &mut emit).unwrap();
        (lines, totals)
    }

    // mcxt.c:1002 MemoryContextStatsPrint + aset.c:1596: "level: N; name:
    // <stats>: <ident>" — ident after the stats, "(N chunks)" present, no
    // allocator-kind suffix.
    #[test]
    fn print_line_is_c_shaped() {
        let t = node("PortalContext", Some("<unnamed>"), 392, vec![]);
        let (lines, totals) = dump(&t, 100, 100, false);
        assert_eq!(lines, ["level: 1; PortalContext: 400 total in 1 blocks; 8 free (0 chunks); 392 used: <unnamed>"]);
        assert_eq!(grand_total_line(&totals), "Grand total: 400 bytes in 1 blocks; 8 free (0 chunks); 392 used");
        let (lines, _) = dump(&node("TopMemoryContext", None, 10, vec![]), 100, 100, false);
        assert_eq!(lines, ["level: 1; TopMemoryContext: 18 total in 1 blocks; 8 free (0 chunks); 10 used"]);
    }

    // mcxt.c:1018: a dynahash context is labelled by its table name alone.
    #[test]
    fn dynahash_context_is_relabelled_by_its_ident() {
        let t = node("dynahash", Some("Prepared Queries"), 100, vec![]);
        let (lines, _) = dump(&t, 100, 100, false);
        assert_eq!(lines, ["level: 1; Prepared Queries: 108 total in 1 blocks; 8 free (0 chunks); 100 used"]);
    }

    // mcxt.c:1027-1048: clip at 100 bytes on a character boundary + "...",
    // control characters replaced by spaces.
    #[test]
    fn ident_is_clipped_and_scrubbed() {
        let long = format!("PREPARE p AS SELECT '{}'", "a".repeat(120));
        let (lines, _) = dump(&node("CachedPlanSource", Some(&long), 1, vec![]), 100, 100, false);
        let expect = format!(": {}...", &long[..100]);
        assert!(lines[0].ends_with(&expect), "{}", lines[0]);
        assert_eq!(lines[0].len(), "level: 1; CachedPlanSource: 9 total in 1 blocks; 8 free (0 chunks); 1 used".len() + 2 + 100 + 3);

        // byte 100 falls inside a 3-byte character: clip before it (pg_mbcliplen)
        let mb = format!("{}€€", "x".repeat(99));
        let (lines, _) = dump(&node("CachedPlanSource", Some(&mb), 1, vec![]), 100, 100, false);
        assert!(lines[0].ends_with(&format!(": {}...", "x".repeat(99))), "{}", lines[0]);

        let multi = "PREPARE q AS SELECT 1,\n2;\ttab\r";
        let (lines, _) = dump(&node("CachedPlanSource", Some(multi), 1, vec![]), 100, 100, false);
        assert!(lines[0].ends_with(": PREPARE q AS SELECT 1, 2; tab "), "{}", lines[0]);

        // exactly 100 bytes is not truncated
        let exact = "y".repeat(100);
        let (lines, _) = dump(&node("CachedPlanSource", Some(&exact), 1, vec![]), 100, 100, false);
        assert!(lines[0].ends_with(&format!(": {exact}")) && !lines[0].ends_with("..."));
    }

    // mcxt.c:939-967: children beyond max_children are summarized at the
    // PARENT's level, with their whole subtrees tallied (MemoryContextTraverseNext).
    #[test]
    fn extra_children_are_summarized_with_totals() {
        let kids: Vec<TreeStats> = (0..5)
            .map(|i| node("CachedPlanSource", None, 10 * (i + 1), vec![node("CachedPlanQuery", None, 1, vec![])]))
            .collect();
        let t = node("Prepared Queries", None, 100, kids);
        let (lines, totals) = dump(&t, 100, 2, false);
        assert_eq!(lines.len(), 1 + 2 * 2 + 1);
        assert_eq!(lines[1], "level: 2; CachedPlanSource: 18 total in 1 blocks; 8 free (0 chunks); 10 used");
        assert_eq!(lines[2], "level: 3; CachedPlanQuery: 9 total in 1 blocks; 8 free (0 chunks); 1 used");
        // 3 unshown children + 3 grandchildren = 6 contexts;
        // totals 38+48+58 + 3*9 = 171, 6 blocks, 48 free
        assert_eq!(lines[5], "level: 1; 6 more child contexts containing 171 total in 6 blocks; 48 free (0 chunks); 123 used");
        assert_eq!(totals, Counters { nblocks: 1 + 4 + 6, freechunks: 0, totalspace: 108 + 18 + 9 + 28 + 9 + 171, freespace: 8 * 11 });
    }

    // mcxt.c:905: `level <= max_level` gates the recursion; deeper levels
    // are summarized, never printed.
    #[test]
    fn depth_beyond_max_level_is_summarized() {
        let chain = node("l1", None, 1, vec![node("l2", None, 2, vec![node("l3", None, 3, vec![node("l4", None, 4, vec![node("l5", None, 5, vec![])])])])]);
        let (lines, _) = dump(&chain, 2, 100, false);
        assert_eq!(
            lines,
            [
                "level: 1; l1: 9 total in 1 blocks; 8 free (0 chunks); 1 used",
                "level: 2; l2: 10 total in 1 blocks; 8 free (0 chunks); 2 used",
                "level: 3; l3: 11 total in 1 blocks; 8 free (0 chunks); 3 used",
                "level: 3; 2 more child contexts containing 25 total in 2 blocks; 16 free (0 chunks); 9 used",
            ]
        );
        // the production caps: a 101-deep chain stops printing at level 101
        let mut deep = node("leaf", None, 1, vec![]);
        for _ in 0..104 {
            deep = node("n", None, 1, vec![deep]);
        }
        let (lines, _) = dump(&deep, 100, 100, false);
        assert_eq!(lines.len(), 102);
        assert_eq!(lines[100], "level: 101; n: 9 total in 1 blocks; 8 free (0 chunks); 1 used");
        assert_eq!(lines[101], "level: 101; 4 more child contexts containing 36 total in 4 blocks; 32 free (0 chunks); 4 used");
    }

    // print_to_stderr = true (MemoryContextStats on allocation failure):
    // two-space indentation per level instead of "level: N; ".
    #[test]
    fn stderr_shape_indents_by_level() {
        let t = node("TopMemoryContext", None, 1, vec![node("dynahash", Some("Prepared Queries"), 2, vec![node("a", None, 3, vec![]), node("b", None, 3, vec![])])]);
        let (lines, _) = dump(&t, 100, 1, true);
        assert_eq!(
            lines,
            [
                "TopMemoryContext: 9 total in 1 blocks; 8 free (0 chunks); 1 used",
                "  Prepared Queries: 10 total in 1 blocks; 8 free (0 chunks); 2 used",
                "    a: 11 total in 1 blocks; 8 free (0 chunks); 3 used",
                "    1 more child contexts containing 11 total in 1 blocks; 8 free (0 chunks); 3 used",
            ]
        );
    }

    #[test]
    fn stats_string_follows_each_allocator() {
        let c = Counters { nblocks: 2, freechunks: 0, totalspace: 100, freespace: 40 };
        assert_eq!(stats_string("AllocSet", &c), "100 total in 2 blocks; 40 free (0 chunks); 60 used");
        assert_eq!(stats_string("Generation", &c), "100 total in 2 blocks (0 chunks); 40 free (0 chunks); 60 used");
        assert_eq!(stats_string("Slab", &c), "100 total in 2 blocks; 0 empty blocks; 40 free (0 chunks); 60 used");
        assert_eq!(stats_string("Bump", &c), "100 total in 2 blocks; 40 free; 60 used");
    }
}
