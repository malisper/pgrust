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

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/utils/mmgr/mcxt.c", 0, funcname)
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
    let total = t.arena_footprint;
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
            t.name, t.nblocks, t.kind
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

pub fn init_seams() {
    mcx::set_root_observer(observe_root);
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
