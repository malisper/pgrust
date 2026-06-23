//! Parallel-vacuum value types (`commands/vacuumparallel.c`, `access/genam.h`,
//! `commands/vacuum.h`), trimmed to what the lazy-vacuum driver consumes across
//! its seams.

use core::sync::atomic::{AtomicU32, Ordering};

use alloc::sync::Arc;

use types_core::{Oid, Size};

/// The genuinely-shared vacuum cost-balance / active-worker counters that, in C,
/// live in the parallel-vacuum DSM segment (`PVShared.cost_balance` /
/// `PVShared.active_nworkers`, both `pg_atomic_uint32`). The leader's
/// `ParallelVacuumState` allocates them in the DSM segment; the worker attaches
/// to the same segment. Both then point the process globals
/// `VacuumSharedCostBalance` / `VacuumActiveNWorkers` at these atomics and
/// atomic-add/sub the shared balance as cost-based delay accumulates.
///
/// Modeled here as a single shared cell holding both `AtomicU32`s, shared by
/// reference-counted handle (`Arc`) so that the leader codepath and the worker
/// codepath operate on the *same* atomics — the faithful single-process image of
/// "both processes mapped the same DSM page". This is never a process-local
/// stand-in: every holder of an `Arc<VacuumSharedCostState>` sees the same
/// underlying memory, exactly as `VacuumSharedCostBalance` aliases
/// `&shared->cost_balance` in C.
#[derive(Debug)]
pub struct VacuumSharedCostState {
    /// `pg_atomic_uint32 cost_balance` — accumulated balance of each worker.
    pub cost_balance: AtomicU32,
    /// `pg_atomic_uint32 active_nworkers` — number of active parallel workers.
    pub active_nworkers: AtomicU32,
}

impl VacuumSharedCostState {
    /// Allocate the shared atomics, initialized as in `pg_atomic_init_u32`
    /// during `parallel_vacuum_init` (both start at the supplied seeds).
    pub fn new(cost_balance: u32, active_nworkers: u32) -> Arc<Self> {
        Arc::new(VacuumSharedCostState {
            cost_balance: AtomicU32::new(cost_balance),
            active_nworkers: AtomicU32::new(active_nworkers),
        })
    }

    /// `pg_atomic_add_fetch_u32(&cost_balance, v)`.
    pub fn cost_balance_add_fetch(&self, v: u32) -> u32 {
        self.cost_balance.fetch_add(v, Ordering::SeqCst).wrapping_add(v)
    }

    /// `pg_atomic_sub_fetch_u32(&cost_balance, v)`.
    pub fn cost_balance_sub_fetch(&self, v: u32) -> u32 {
        self.cost_balance.fetch_sub(v, Ordering::SeqCst).wrapping_sub(v)
    }

    /// `pg_atomic_read_u32(&cost_balance)`.
    pub fn cost_balance_read(&self) -> u32 {
        self.cost_balance.load(Ordering::SeqCst)
    }

    /// `pg_atomic_add_fetch_u32(&active_nworkers, v)`.
    pub fn active_nworkers_add(&self, v: u32) -> u32 {
        self.active_nworkers
            .fetch_add(v, Ordering::SeqCst)
            .wrapping_add(v)
    }

    /// `pg_atomic_sub_fetch_u32(&active_nworkers, v)`.
    pub fn active_nworkers_sub(&self, v: u32) -> u32 {
        self.active_nworkers
            .fetch_sub(v, Ordering::SeqCst)
            .wrapping_sub(v)
    }

    /// `pg_atomic_read_u32(&active_nworkers)`.
    pub fn active_nworkers_read(&self) -> u32 {
        self.active_nworkers.load(Ordering::SeqCst)
    }
}

/// `VacDeadItemsInfo` (`commands/vacuum.h`) — TID-store sizing/accounting.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VacDeadItemsInfo {
    /// `size_t max_bytes` — the maximum bytes the TidStore can use.
    pub max_bytes: Size,
    /// `int64 num_items` — current number of entries.
    pub num_items: i64,
}

/// `IndexBulkDeleteResult` (`access/genam.h`) — per-index vacuum statistics.
/// Canonically defined in `types_tableam::genam` (the `access/genam.h` home);
/// re-exported here so existing
/// `types_vacuum::vacuumparallel::IndexBulkDeleteResult` paths keep working.
pub use types_tableam::genam::IndexBulkDeleteResult;

/// `IndexVacuumInfo` (`access/genam.h`) — the per-call info struct handed to
/// `ambulkdelete` / `amvacuumcleanup`.
#[derive(Clone, Debug)]
pub struct IndexVacuumInfo {
    /// `Relation index` — the index being vacuumed (its `Oid`).
    pub index: Oid,
    /// `Relation heaprel` — the heap relation the index belongs to (its `Oid`).
    pub heaprel: Oid,
    /// `bool analyze_only` — ANALYZE (without any actual vacuum).
    pub analyze_only: bool,
    /// `bool report_progress` — emit progress.h status reports.
    pub report_progress: bool,
    /// `bool estimated_count` — `num_heap_tuples` is an estimate.
    pub estimated_count: bool,
    /// `int message_level` — ereport level for progress messages.
    pub message_level: i32,
    /// `double num_heap_tuples` — tuples remaining in heap.
    pub num_heap_tuples: f64,
    /// `BufferAccessStrategy strategy` — access strategy for reads (`None` for
    /// the C `NULL` strategy).
    pub strategy: types_storage::buf::BufferAccessStrategy,
}
