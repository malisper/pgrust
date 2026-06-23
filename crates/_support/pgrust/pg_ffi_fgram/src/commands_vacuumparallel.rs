//! ABI vocabulary for `backend/commands/vacuumparallel.c`.
//!
//! These are the shared-memory-resident structs that cross the leader/worker
//! process boundary inside the parallel-vacuum DSM segment (`PVShared`,
//! `PVIndStats`, the `ParallelVacuumState` per-backend state), plus the
//! `genam.h`/`vacuum.h` value structs the file passes by reference
//! (`VacDeadItemsInfo`, `IndexBulkDeleteResult`, `IndexVacuumInfo`) and the
//! `VACUUM_OPTION_*` / progress / DSA-handle vocabulary.
//!
//! Every struct is `#[repr(C)]` and laid out to match PostgreSQL 18.3 exactly,
//! so that `PVShared` / `PVIndStats` are genuine DSM-resident state (faithful
//! shmem) rather than backend-local copies.

use core::ffi::{c_char, c_int};

use crate::instrument::{BufferUsage, WalUsage};
use crate::storage::pg_atomic_uint32;
use crate::types::{int64, uint8, BlockNumber, Oid, Size};
use crate::BufferAccessStrategy;

/// `Relation` — the relcache entry pointer (carried opaque, as elsewhere).
pub type Relation = *mut core::ffi::c_void;
/// `TidStore *` — the dead-items TID store (carried opaque).
pub type TidStore = core::ffi::c_void;
/// `ParallelContext *` — DSM/parallel-query coordination context (carried opaque).
pub type ParallelContext = core::ffi::c_void;

/// `dsa_handle` (utils/dsa.h) — a handle to a `dsa_area` usable across backends.
pub type dsa_handle = u32;
/// `dsa_pointer` (utils/dsa.h) — an offset into a `dsa_area`.
pub type dsa_pointer = u64;

// VACUUM_OPTION_* bit flags advertised by `amparallelvacuumoptions`
// (src/include/commands/vacuum.h).
/// `VACUUM_OPTION_NO_PARALLEL` — index AM has no parallel-vacuum support.
pub const VACUUM_OPTION_NO_PARALLEL: uint8 = 0;
/// `VACUUM_OPTION_PARALLEL_BULKDEL` — supports parallel bulk-deletion.
pub const VACUUM_OPTION_PARALLEL_BULKDEL: uint8 = 1 << 0;
/// `VACUUM_OPTION_PARALLEL_COND_CLEANUP` — supports parallel cleanup conditionally.
pub const VACUUM_OPTION_PARALLEL_COND_CLEANUP: uint8 = 1 << 1;
/// `VACUUM_OPTION_PARALLEL_CLEANUP` — supports parallel cleanup unconditionally.
pub const VACUUM_OPTION_PARALLEL_CLEANUP: uint8 = 1 << 2;
/// `VACUUM_OPTION_MAX_VALID_VALUE` — the largest valid combination of the above.
pub const VACUUM_OPTION_MAX_VALID_VALUE: uint8 = (1 << 3) - 1;

// Progress parameters used by parallel vacuum (src/include/commands/progress.h).
/// `PROGRESS_VACUUM_INDEXES_PROCESSED`.
pub const PROGRESS_VACUUM_INDEXES_PROCESSED: c_int = 9;
/// `PROGRESS_VACUUM_DELAY_TIME`.
pub const PROGRESS_VACUUM_DELAY_TIME: c_int = 10;

/// `VacDeadItemsInfo` (commands/vacuum.h) — TID-store sizing/accounting that the
/// leader keeps in the DSM-resident `PVShared`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VacDeadItemsInfo {
    /// `size_t max_bytes` — the maximum bytes the TidStore can use.
    pub max_bytes: Size,
    /// `int64 num_items` — current number of entries.
    pub num_items: int64,
}

/// `IndexBulkDeleteResult` (access/genam.h) — per-index vacuum statistics.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IndexBulkDeleteResult {
    /// `BlockNumber num_pages` — pages remaining in index.
    pub num_pages: BlockNumber,
    /// `bool estimated_count` — `num_index_tuples` is an estimate.
    pub estimated_count: bool,
    /// `double num_index_tuples` — tuples remaining.
    pub num_index_tuples: f64,
    /// `double tuples_removed` — number removed during vacuum operation.
    pub tuples_removed: f64,
    /// `BlockNumber pages_newly_deleted` — pages marked deleted by us.
    pub pages_newly_deleted: BlockNumber,
    /// `BlockNumber pages_deleted` — pages marked deleted (could be by us).
    pub pages_deleted: BlockNumber,
    /// `BlockNumber pages_free` — pages available for reuse.
    pub pages_free: BlockNumber,
}

/// `IndexVacuumInfo` (access/genam.h) — the per-call info struct handed to
/// `ambulkdelete` / `amvacuumcleanup` (built per-index by the leader/worker).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IndexVacuumInfo {
    /// `Relation index` — the index being vacuumed.
    pub index: Relation,
    /// `Relation heaprel` — the heap relation the index belongs to.
    pub heaprel: Relation,
    /// `bool analyze_only` — ANALYZE (without any actual vacuum).
    pub analyze_only: bool,
    /// `bool report_progress` — emit progress.h status reports.
    pub report_progress: bool,
    /// `bool estimated_count` — `num_heap_tuples` is an estimate.
    pub estimated_count: bool,
    /// `int message_level` — ereport level for progress messages.
    pub message_level: c_int,
    /// `double num_heap_tuples` — tuples remaining in heap.
    pub num_heap_tuples: f64,
    /// `BufferAccessStrategy strategy` — access strategy for reads.
    pub strategy: BufferAccessStrategy,
}

/// `PVIndVacStatus` (vacuumparallel.c) — status of an index during a parallel
/// index vacuum or cleanup pass.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PVIndVacStatus {
    /// `PARALLEL_INDVAC_STATUS_INITIAL`.
    Initial = 0,
    /// `PARALLEL_INDVAC_STATUS_NEED_BULKDELETE`.
    NeedBulkdelete,
    /// `PARALLEL_INDVAC_STATUS_NEED_CLEANUP`.
    NeedCleanup,
    /// `PARALLEL_INDVAC_STATUS_COMPLETED`.
    Completed,
}

/// `PVShared` (vacuumparallel.c) — shared information among parallel workers,
/// allocated in the DSM segment (faithful shmem).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PVShared {
    /// `Oid relid` — target table relid.
    pub relid: Oid,
    /// `int elevel` — log level for VACUUM VERBOSE worker-launch messages.
    pub elevel: c_int,
    /// `int64 queryid`.
    pub queryid: int64,

    /// `double reltuples` — total number of input heap tuples.
    pub reltuples: f64,
    /// `bool estimated_count` — `reltuples` is an estimated value.
    pub estimated_count: bool,

    /// `int maintenance_work_mem_worker` — per-worker maintenance_work_mem.
    pub maintenance_work_mem_worker: c_int,

    /// `int ring_nbuffers` — buffers in each worker's Buffer Access Strategy ring.
    pub ring_nbuffers: c_int,

    /// `pg_atomic_uint32 cost_balance` — shared vacuum cost balance.
    pub cost_balance: pg_atomic_uint32,

    /// `pg_atomic_uint32 active_nworkers` — number of active parallel workers.
    pub active_nworkers: pg_atomic_uint32,

    /// `pg_atomic_uint32 idx` — counter for vacuuming and cleanup.
    pub idx: pg_atomic_uint32,

    /// `dsa_handle dead_items_dsa_handle` — DSA handle where the TidStore lives.
    pub dead_items_dsa_handle: dsa_handle,

    /// `dsa_pointer dead_items_handle` — DSA pointer to the shared TidStore.
    pub dead_items_handle: dsa_pointer,

    /// `VacDeadItemsInfo dead_items_info` — statistics of shared dead items.
    pub dead_items_info: VacDeadItemsInfo,
}

/// `PVIndStats` (vacuumparallel.c) — per-index vacuum status + statistics, an
/// array of which lives in the DSM segment (faithful shmem).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PVIndStats {
    /// `PVIndVacStatus status`.
    pub status: PVIndVacStatus,
    /// `bool parallel_workers_can_process` — leader+worker (true) vs leader-only.
    pub parallel_workers_can_process: bool,
    /// `bool istat_updated` — are the stats updated?
    pub istat_updated: bool,
    /// `IndexBulkDeleteResult istat` — the index vacuum/cleanup result.
    pub istat: IndexBulkDeleteResult,
}

/// `struct ParallelVacuumState` (typedef in vacuum.h) — the per-backend state
/// for a parallel vacuum.  Carried by pointer between this file's functions;
/// the `shared`/`indstats` members point into the DSM segment.
#[repr(C)]
pub struct ParallelVacuumState {
    /// `ParallelContext *pcxt` — NULL for worker processes.
    pub pcxt: *mut ParallelContext,
    /// `Relation heaprel` — parent heap relation.
    pub heaprel: Relation,
    /// `Relation *indrels` — target indexes.
    pub indrels: *mut Relation,
    /// `int nindexes`.
    pub nindexes: c_int,
    /// `PVShared *shared` — shared information among workers (DSM-resident).
    pub shared: *mut PVShared,
    /// `PVIndStats *indstats` — shared index statistics (DSM-resident).
    pub indstats: *mut PVIndStats,
    /// `TidStore *dead_items` — shared dead-items space.
    pub dead_items: *mut TidStore,
    /// `BufferUsage *buffer_usage` — points to buffer usage area in DSM.
    pub buffer_usage: *mut BufferUsage,
    /// `WalUsage *wal_usage` — points to WAL usage area in DSM.
    pub wal_usage: *mut WalUsage,
    /// `bool *will_parallel_vacuum` — per-index parallel-suitability flags.
    pub will_parallel_vacuum: *mut bool,
    /// `int nindexes_parallel_bulkdel`.
    pub nindexes_parallel_bulkdel: c_int,
    /// `int nindexes_parallel_cleanup`.
    pub nindexes_parallel_cleanup: c_int,
    /// `int nindexes_parallel_condcleanup`.
    pub nindexes_parallel_condcleanup: c_int,
    /// `BufferAccessStrategy bstrategy` — leader's buffer access strategy.
    pub bstrategy: BufferAccessStrategy,
    /// `char *relnamespace` — error-reporting state.
    pub relnamespace: *mut c_char,
    /// `char *relname`.
    pub relname: *mut c_char,
    /// `char *indname`.
    pub indname: *mut c_char,
    /// `PVIndVacStatus status`.
    pub status: PVIndVacStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn vac_dead_items_info_layout() {
        assert_eq!(size_of::<VacDeadItemsInfo>(), 16);
        assert_eq!(align_of::<VacDeadItemsInfo>(), 8);
        assert_eq!(offset_of!(VacDeadItemsInfo, max_bytes), 0);
        assert_eq!(offset_of!(VacDeadItemsInfo, num_items), 8);
    }

    #[test]
    fn index_bulk_delete_result_layout() {
        // num_pages(4) + estimated_count(1) + pad(3) + num_index_tuples(8) +
        // tuples_removed(8) + 3*BlockNumber(12) = 4 -> with 8-byte align => 40.
        assert_eq!(align_of::<IndexBulkDeleteResult>(), 8);
        assert_eq!(size_of::<IndexBulkDeleteResult>(), 40);
        assert_eq!(offset_of!(IndexBulkDeleteResult, num_pages), 0);
        assert_eq!(offset_of!(IndexBulkDeleteResult, estimated_count), 4);
        assert_eq!(offset_of!(IndexBulkDeleteResult, num_index_tuples), 8);
        assert_eq!(offset_of!(IndexBulkDeleteResult, tuples_removed), 16);
        assert_eq!(offset_of!(IndexBulkDeleteResult, pages_newly_deleted), 24);
        assert_eq!(offset_of!(IndexBulkDeleteResult, pages_deleted), 28);
        assert_eq!(offset_of!(IndexBulkDeleteResult, pages_free), 32);
    }

    #[test]
    fn index_vacuum_info_layout() {
        assert_eq!(align_of::<IndexVacuumInfo>(), 8);
        assert_eq!(offset_of!(IndexVacuumInfo, index), 0);
        assert_eq!(offset_of!(IndexVacuumInfo, heaprel), 8);
        assert_eq!(offset_of!(IndexVacuumInfo, analyze_only), 16);
        assert_eq!(offset_of!(IndexVacuumInfo, report_progress), 17);
        assert_eq!(offset_of!(IndexVacuumInfo, estimated_count), 18);
        assert_eq!(offset_of!(IndexVacuumInfo, message_level), 20);
        assert_eq!(offset_of!(IndexVacuumInfo, num_heap_tuples), 24);
        assert_eq!(offset_of!(IndexVacuumInfo, strategy), 32);
    }

    #[test]
    fn pvshared_layout() {
        assert_eq!(align_of::<PVShared>(), 8);
        assert_eq!(offset_of!(PVShared, relid), 0);
        assert_eq!(offset_of!(PVShared, elevel), 4);
        assert_eq!(offset_of!(PVShared, queryid), 8);
        assert_eq!(offset_of!(PVShared, reltuples), 16);
        assert_eq!(offset_of!(PVShared, estimated_count), 24);
        assert_eq!(offset_of!(PVShared, maintenance_work_mem_worker), 28);
        assert_eq!(offset_of!(PVShared, ring_nbuffers), 32);
        assert_eq!(offset_of!(PVShared, cost_balance), 36);
        assert_eq!(offset_of!(PVShared, active_nworkers), 40);
        assert_eq!(offset_of!(PVShared, idx), 44);
        assert_eq!(offset_of!(PVShared, dead_items_dsa_handle), 48);
        assert_eq!(offset_of!(PVShared, dead_items_handle), 56);
        assert_eq!(offset_of!(PVShared, dead_items_info), 64);
    }

    #[test]
    fn pvindstats_layout() {
        assert_eq!(align_of::<PVIndStats>(), 8);
        assert_eq!(offset_of!(PVIndStats, status), 0);
        assert_eq!(offset_of!(PVIndStats, parallel_workers_can_process), 4);
        assert_eq!(offset_of!(PVIndStats, istat_updated), 5);
        assert_eq!(offset_of!(PVIndStats, istat), 8);
    }
}
