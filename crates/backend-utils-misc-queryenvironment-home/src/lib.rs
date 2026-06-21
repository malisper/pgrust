//! The per-backend **query-environment home**: a thread-local stack of
//! [`QueryEnvironment`]s that the AFTER-trigger firing code owns and the SPI
//! execution leg borrow-reads.
//!
//! # Why a home (the reclaim model)
//!
//! In C, an AFTER trigger's transition tables (`OLD TABLE`/`NEW TABLE`) live in
//! tuplestores owned by the after-trigger `query_stack[qd].tables[i]`, freed at
//! `AfterTriggerEndQuery` — *not* at `SPI_finish`. The trigger function's
//! language handler (plpgsql) registers them as Ephemeral Named Relations in a
//! `QueryEnvironment` (`SPI_register_trigger_data`), and the executor reads them
//! back (`nodeNamedtuplestorescan`). `enr->reldata` is a *borrowed* pointer; the
//! store is never freed by SPI.
//!
//! The owned-Rust model cannot have the executor alias a store owned elsewhere
//! across the SPI boundary. The faithful reclaim: the **firing code owns** the
//! `QueryEnvironment` here (it built it from the query-stack tuplestores), pushes
//! it onto this thread-local stack for the duration of the trigger call, and the
//! SPI execution leg *borrows* the top entry (never freeing the stores). When
//! the trigger call returns, firing pops the env and moves the tuplestores back
//! into the query stack. Nothing transfers ownership across the SPI boundary —
//! mirroring C's lifetime exactly.
//!
//! A **stack** (not a single slot) is required so a nested trigger that fires
//! within an outer trigger's SPI query gets its own env without clobbering the
//! outer one (the SPI reader always sees the innermost/topmost env, which is the
//! one for the currently-executing trigger function body).
//!
//! This is the same `es_*_shared` side-table pattern that resolved the owned
//! `PlanState` keystones, applied to the query environment instead of an EState
//! index.

#![allow(non_snake_case)]

use std::cell::RefCell;
use types_nodes::queryenvironment::QueryEnvironment;

thread_local! {
    /// The stack of live query environments. The top entry is the env for the
    /// currently-executing trigger function body; SPI reads it, the firing code
    /// owns and pops it.
    static ENV_HOME: RefCell<Vec<QueryEnvironment<'static>>> = const { RefCell::new(Vec::new()) };
}

/// Push an environment onto the home, returning its depth index (the position
/// it occupies). The firing code holds this index to pop the exact entry back.
pub fn push_query_env(env: QueryEnvironment<'static>) -> usize {
    ENV_HOME.with(|h| {
        let mut h = h.borrow_mut();
        h.push(env);
        h.len() - 1
    })
}

/// Pop the environment at `depth` off the home and return it (so the firing code
/// can move its tuplestores back into the query stack). `depth` must be the
/// current top of the stack — the trigger call is strictly nested, so its env is
/// always the innermost one when it returns.
pub fn pop_query_env(depth: usize) -> Option<QueryEnvironment<'static>> {
    ENV_HOME.with(|h| {
        let mut h = h.borrow_mut();
        debug_assert_eq!(
            h.len(),
            depth + 1,
            "pop_query_env: env home stack not strictly nested"
        );
        if h.len() == depth + 1 {
            h.pop()
        } else {
            // Defensive: only remove if it is the top, never an inner entry.
            None
        }
    })
}

/// Borrow the top (innermost) query environment mutably for the duration of `f`.
/// Returns `None` when the home is empty (no trigger transition-table env is
/// live — the common case for a plain SPI query). The executor / parser read the
/// ENR list and the executor takes the `reldata` tuplestore alias inside `f`.
pub fn with_top_query_env<R>(f: impl FnOnce(Option<&mut QueryEnvironment<'static>>) -> R) -> R {
    ENV_HOME.with(|h| {
        let mut h = h.borrow_mut();
        f(h.last_mut())
    })
}

/// True iff there is a live query environment on the home (a trigger reading a
/// transition table is currently on the SPI call stack).
pub fn has_query_env() -> bool {
    ENV_HOME.with(|h| !h.borrow().is_empty())
}
