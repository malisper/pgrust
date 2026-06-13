//! Parallel-vacuum value types (`commands/vacuumparallel.c`, `access/genam.h`,
//! `commands/vacuum.h`), trimmed to what the lazy-vacuum driver consumes across
//! its seams.

use types_core::{Oid, Size};

/// `BufferAccessStrategy` — a buffer-replacement ring, owned by `freelist.c`;
/// named here by an opaque handle (`0` is the C `NULL` strategy).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct BufferAccessStrategyHandle(pub u64);

/// `VacDeadItemsInfo` (`commands/vacuum.h`) — TID-store sizing/accounting.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VacDeadItemsInfo {
    /// `size_t max_bytes` — the maximum bytes the TidStore can use.
    pub max_bytes: Size,
    /// `int64 num_items` — current number of entries.
    pub num_items: i64,
}

/// `IndexBulkDeleteResult` (`access/genam.h`) — per-index vacuum statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IndexBulkDeleteResult {
    /// `BlockNumber num_pages` — pages remaining in index.
    pub num_pages: u32,
    /// `bool estimated_count` — `num_index_tuples` is an estimate.
    pub estimated_count: bool,
    /// `double num_index_tuples` — tuples remaining.
    pub num_index_tuples: f64,
    /// `double tuples_removed` — number removed during vacuum operation.
    pub tuples_removed: f64,
    /// `BlockNumber pages_newly_deleted` — pages marked deleted by us.
    pub pages_newly_deleted: u32,
    /// `BlockNumber pages_deleted` — pages marked deleted (could be by us).
    pub pages_deleted: u32,
    /// `BlockNumber pages_free` — pages available for reuse.
    pub pages_free: u32,
}

/// `IndexVacuumInfo` (`access/genam.h`) — the per-call info struct handed to
/// `ambulkdelete` / `amvacuumcleanup`.
#[derive(Clone, Copy, Debug, PartialEq)]
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
    /// `BufferAccessStrategy strategy` — access strategy for reads.
    pub strategy: BufferAccessStrategyHandle,
}
