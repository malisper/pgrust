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
use std::collections::HashMap;

use types_pgstat::pgstat_internal::PgStat_HashKey;

use crate::entry_ref::{PgStat_LocalState, PgStat_PendingState};

/// The variable-numbered half of `PgStat_Snapshot` that `types-pgstat`
/// deliberately omits as owner-internal: C's `pgstat_snapshot_hash *stats`
/// (the simplehash of per-entry snapshot copies) and the `MemoryContext
/// context` they are bulk-freed from.
///
/// In C `stats` is a simplehash keyed by [`PgStat_HashKey`] whose values carry a
/// `void *data` (the copied stats body), and `context` is the arena all those
/// copies are allocated in and reset together. The idiomatic model collapses
/// both into one owning `HashMap`: the value is the `Option<Box<[u8]>>` copy of
/// the entry's `shared_data_len` stats bytes (the box *is* the arena allocation;
/// the map *is* the context, reset by clearing it). `None` is C's
/// `entry->data == NULL` negative-cache marker that
/// `PGSTAT_FETCH_CONSISTENCY_CACHE` inserts for a missing/dropped entry.
///
/// `prepared` mirrors `pgStatLocal.snapshot.stats != NULL`: the hash exists once
/// `pgstat_prep_snapshot` has run for the current snapshot lifetime, and is
/// torn down by `pgstat_clear_snapshot`.
pub struct PgStat_SnapshotStats {
    /// `pgStatLocal.snapshot.stats != NULL` — whether the snapshot hash is live.
    pub prepared: bool,
    /// C's `pgstat_snapshot_hash *stats` + the `context` arena: each value is the
    /// snapshot copy of one variable-numbered entry's stats bytes (`None` ==
    /// C's `entry->data == NULL` negative-cache marker).
    pub stats: HashMap<PgStat_HashKey, Option<Box<[u8]>>>,
}

impl PgStat_SnapshotStats {
    pub fn new() -> Self {
        PgStat_SnapshotStats {
            prepared: false,
            stats: HashMap::new(),
        }
    }
}

impl Default for PgStat_SnapshotStats {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    /// `PgStat_LocalState pgStatLocal` (`pgstat.c`) — this backend's cumulative
    /// statistics control block (shared-control pointer, DSA area, shared hash,
    /// snapshot).
    static PG_STAT_LOCAL: RefCell<PgStat_LocalState> = RefCell::new(PgStat_LocalState::new());

    /// The owner-internal variable-numbered snapshot hash + its arena (the half
    /// of `PgStat_Snapshot` that `types-pgstat` omits). See
    /// [`PgStat_SnapshotStats`].
    static PG_STAT_SNAPSHOT_STATS: RefCell<PgStat_SnapshotStats> =
        RefCell::new(PgStat_SnapshotStats::new());

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
///
/// Public so per-kind owner crates (e.g. `pgstat_io` / `pgstat_wal`) can reach
/// their fixed-kind shared region (`pgStatLocal.shmem->io` / `->wal`) and the
/// cached snapshot from their `flush_static_cb` / fetch paths, which the core
/// dispatches without passing the control block.
pub fn with_local<R>(f: impl FnOnce(&mut PgStat_LocalState) -> R) -> R {
    PG_STAT_LOCAL.with(|l| f(&mut l.borrow_mut()))
}

/// Borrow the pending-flush bookkeeping mutably for the duration of `f`.
pub(crate) fn with_pending<R>(f: impl FnOnce(&mut PgStat_PendingState) -> R) -> R {
    PG_STAT_PENDING.with(|p| f(&mut p.borrow_mut()))
}

/// Borrow the variable-numbered snapshot hash + arena mutably for the duration
/// of `f` (the owner-internal half of `pgStatLocal.snapshot`).
pub(crate) fn with_snapshot_stats<R>(f: impl FnOnce(&mut PgStat_SnapshotStats) -> R) -> R {
    PG_STAT_SNAPSHOT_STATS.with(|s| f(&mut s.borrow_mut()))
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
