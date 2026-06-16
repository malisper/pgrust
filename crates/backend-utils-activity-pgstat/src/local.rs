//! The backend-local process-global state that `pgstat.c` / `pgstat_shmem.c`
//! keep as C file-statics: `pgStatLocal` (the [`PgStat_LocalState`] control
//! block) and the pending-flush bookkeeping (`pgStatPending` /
//! `pgStatEntryRefHash`, grouped in [`PgStat_PendingState`]).
//!
//! In C these are plain process globals, mutated in-place by the report/flush
//! paths. The Rust port keeps them as thread-locals (each backend is a thread
//! in this model), wrapped so the core fns can borrow them mutably. They are
//! never shared between backends — the shared state lives in the DSA segment
//! these handles point into — so a `RefCell` is the faithful single-owner
//! model (matching C's "only the owning backend touches its `pgStatLocal`").

use core::cell::RefCell;

use crate::entry_ref::{PgStat_LocalState, PgStat_PendingState};

thread_local! {
    /// `PgStat_LocalState pgStatLocal` (`pgstat.c`) — this backend's cumulative
    /// statistics control block (shared-control pointer, DSA area, shared hash,
    /// snapshot).
    static PG_STAT_LOCAL: RefCell<PgStat_LocalState> = RefCell::new(PgStat_LocalState::new());

    /// `dlist_head pgStatPending` + `pgstat_entry_ref_hash_hash *pgStatEntryRefHash`
    /// (`pgstat_shmem.c`) — this backend's pending-flush list and entry-ref
    /// lookup hash.
    static PG_STAT_PENDING: RefCell<PgStat_PendingState> =
        RefCell::new(PgStat_PendingState::new());

    /// `bool pgstat_is_initialized` (`pgstat.c`) — set once
    /// `pgstat_initialize()` has run for this backend.
    static PG_STAT_IS_INITIALIZED: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };

    /// `bool pgstat_is_shutdown` (`pgstat.c`) — set once
    /// `pgstat_shutdown_hook()` has run, to catch post-shutdown reporting.
    static PG_STAT_IS_SHUTDOWN: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// Borrow `pgStatLocal` mutably for the duration of `f`.
pub(crate) fn with_local<R>(f: impl FnOnce(&mut PgStat_LocalState) -> R) -> R {
    PG_STAT_LOCAL.with(|l| f(&mut l.borrow_mut()))
}

/// Borrow the pending-flush bookkeeping mutably for the duration of `f`.
pub(crate) fn with_pending<R>(f: impl FnOnce(&mut PgStat_PendingState) -> R) -> R {
    PG_STAT_PENDING.with(|p| f(&mut p.borrow_mut()))
}

/// `pgstat_is_initialized`.
pub(crate) fn is_initialized() -> bool {
    PG_STAT_IS_INITIALIZED.with(|c| c.get())
}

/// Set `pgstat_is_initialized`.
pub(crate) fn set_initialized(v: bool) {
    PG_STAT_IS_INITIALIZED.with(|c| c.set(v));
}

/// `pgstat_is_shutdown`.
pub(crate) fn is_shutdown() -> bool {
    PG_STAT_IS_SHUTDOWN.with(|c| c.get())
}

/// Set `pgstat_is_shutdown`.
pub(crate) fn set_shutdown(v: bool) {
    PG_STAT_IS_SHUTDOWN.with(|c| c.set(v));
}
