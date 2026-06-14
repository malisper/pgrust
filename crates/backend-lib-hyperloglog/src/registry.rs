//! Handle registry backing the `backend-lib-hyperloglog-seams` seams.
//!
//! `nodeAgg`'s hash-aggregation spill path holds each per-partition
//! `hyperLogLogState *` only as an opaque `usize` handle word
//! (`HashAggSpill.hll_card`'s entries) and drives it through four seams
//! (`init`/`add`/`estimate`/`free`). On the C side those words *are* the
//! pointer; here the owner cannot hand a real `hyperLogLogState` reference
//! across the seam (the seam types are `usize`), so it mints a stable handle
//! word and keeps the real owned [`HyperLogLog`] in this process-wide table,
//! resolving the word back to the struct on each call. This is the
//! opacity-inherited pattern: the opaque pointer the consumer already used
//! becomes a real owned struct on this side of the seam.
//!
//! The seam-contract crate documents "These functions never ereport"; the
//! consumer always passes a valid `bwidth` (`HASHAGG_HLL_BIT_WIDTH` == 5), so
//! the only `elog(ERROR)` in `initHyperLogLog` (bwidth out of 4..=16) cannot
//! fire on the seam path. Were it ever to, the seam has no error channel, so
//! it surfaces as a loud panic rather than a fabricated handle.

use std::cell::RefCell;
use std::collections::HashMap;

use backend_lib_hyperloglog_seams as seams;
use mcx::MemoryContext;

use crate::{hyperLogLogState, HyperLogLog};

// The handle table is per-thread, not a process-wide static: a
// `MemoryContext` is single-threaded (it holds `Rc`/`Cell` accounting), so a
// `HyperLogLog` is neither `Send` nor `Sync`. This matches PostgreSQL's
// process-per-backend model, where each backend drives its own spill counters;
// the C `hyperLogLogState *` never crosses threads either.
thread_local! {
    /// The per-thread handle table. Each live counter is keyed by its handle
    /// word; `init` inserts, `free` removes (and drops, returning the register
    /// array's charge to its context).
    static REGISTRY: RefCell<Registry> = const { RefCell::new(Registry::new()) };
}

struct Registry {
    next: usize,
    counters: Option<HashMap<usize, HyperLogLog>>,
}

impl Registry {
    const fn new() -> Self {
        Registry { next: 1, counters: None }
    }

    fn map(&mut self) -> &mut HashMap<usize, HyperLogLog> {
        self.counters.get_or_insert_with(HashMap::new)
    }
}

/// Seam impl for `initHyperLogLog`: build a counter with `bwidth` register-index
/// bits in a fresh context and return its opaque handle word.
fn init_hyper_log_log(bwidth: u8) -> usize {
    // The C call site allocates `hashesArr` in the current memory context; the
    // seam carries no context, so the owner gives each counter its own.
    let context = MemoryContext::new("hyperloglog (spill cardinality)");
    let counter = crate::initHyperLogLog(context, bwidth)
        .expect("initHyperLogLog: bit width must be between 4 and 16 inclusive");

    REGISTRY.with(|reg| {
        let mut reg = reg.borrow_mut();
        let handle = reg.next;
        reg.next += 1;
        reg.map().insert(handle, counter);
        handle
    })
}

/// Seam impl for `addHyperLogLog`.
fn add_hyper_log_log(handle: usize, hash: u32) {
    REGISTRY.with(|reg| {
        let mut reg = reg.borrow_mut();
        let counter = reg
            .map()
            .get_mut(&handle)
            .expect("add_hyper_log_log: unknown HyperLogLog handle");
        counter.with_mut(|s: &mut hyperLogLogState<'_>| s.addHyperLogLog(hash));
    });
}

/// Seam impl for `estimateHyperLogLog`.
fn estimate_hyper_log_log(handle: usize) -> f64 {
    REGISTRY.with(|reg| {
        let reg = reg.borrow();
        let counter = reg
            .counters
            .as_ref()
            .and_then(|m| m.get(&handle))
            .expect("estimate_hyper_log_log: unknown HyperLogLog handle");
        counter.with(|s: &hyperLogLogState<'_>| s.estimateHyperLogLog())
    })
}

/// Seam impl for `freeHyperLogLog`: drop the counter, releasing its register
/// array's charge.
fn free_hyper_log_log(handle: usize) {
    let counter = REGISTRY.with(|reg| {
        reg.borrow_mut()
            .map()
            .remove(&handle)
            .expect("free_hyper_log_log: unknown HyperLogLog handle")
    });
    crate::freeHyperLogLog(counter);
}

/// Install all four seams. Called once from [`crate::init_seams`].
pub fn init_seams() {
    seams::init_hyper_log_log::set(init_hyper_log_log);
    seams::add_hyper_log_log::set(add_hyper_log_log);
    seams::estimate_hyper_log_log::set(estimate_hyper_log_log);
    seams::free_hyper_log_log::set(free_hyper_log_log);
}
