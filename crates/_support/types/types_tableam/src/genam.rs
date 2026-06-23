//! `access/genam.h` — generalized index-access vocabulary consumed by the
//! index-AM dispatch layer (`access/index/indexam.c`): the vacuum-info input,
//! the bulk-delete result, the scan instrumentation counters, and the distance
//! struct.

use std::boxed::Box;

use types_core::primitive::BlockNumber;
use rel::Relation;

/// `BufferAccessStrategy` (`storage/bufmgr.h`) — opaque ring-buffer access
/// strategy; indexam.c only passes it through inside [`IndexVacuumInfo`].
pub struct BufferAccessStrategyData {
    pub payload: Option<Box<dyn core::any::Any>>,
}

/// `IndexVacuumInfo` (`access/genam.h`) — input arguments passed to
/// `ambulkdelete` and `amvacuumcleanup`.
pub struct IndexVacuumInfo<'mcx> {
    /// `Relation index` — the index being vacuumed.
    pub index: Relation<'mcx>,
    /// `Relation heaprel` — the heap relation the index belongs to.
    pub heaprel: Relation<'mcx>,
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
    /// `BufferAccessStrategy strategy` — access strategy for reads
    /// (`None` for the C `NULL`).
    pub strategy: Option<Box<BufferAccessStrategyData>>,
}

/// `IndexBulkDeleteResult` (`access/genam.h`) — statistics returned by
/// `ambulkdelete` / `amvacuumcleanup`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IndexBulkDeleteResult {
    /// `BlockNumber num_pages` — pages remaining in index.
    pub num_pages: BlockNumber,
    /// `bool estimated_count` — `num_index_tuples` is an estimate.
    pub estimated_count: bool,
    /// `double num_index_tuples` — tuples remaining.
    pub num_index_tuples: f64,
    /// `double tuples_removed` — # removed during vacuum operation.
    pub tuples_removed: f64,
    /// `BlockNumber pages_newly_deleted` — # pages marked deleted by us.
    pub pages_newly_deleted: BlockNumber,
    /// `BlockNumber pages_deleted` — # pages marked deleted (could be by us).
    pub pages_deleted: BlockNumber,
    /// `BlockNumber pages_free` — # pages available for reuse.
    pub pages_free: BlockNumber,
}

/// `IndexScanInstrumentation` (`access/genam.h`) — statistics maintained by
/// `amgettuple`/`amgetbitmap`. Contains no pointers (it is copied into a
/// `SharedIndexScanInstrumentation` during parallel scans).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexScanInstrumentation {
    /// `uint64 nsearches` — index search count.
    pub nsearches: u64,
}

/// `SharedIndexScanInstrumentation` (`access/genam.h`) — every worker's
/// [`IndexScanInstrumentation`], stored in shared memory. The
/// `FLEXIBLE_ARRAY_MEMBER winstrument[]` becomes a length-`num_workers`
/// vector.
#[derive(Clone, Debug, Default)]
pub struct SharedIndexScanInstrumentation {
    /// `int num_workers`.
    pub num_workers: i32,
    /// `IndexScanInstrumentation winstrument[FLEXIBLE_ARRAY_MEMBER]`.
    pub winstrument: std::vec::Vec<IndexScanInstrumentation>,
}

/// `IndexOrderByDistance` (`access/genam.h`) — a nullable "ORDER BY col op
/// const" distance from the AM distance function.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct IndexOrderByDistance {
    /// `double value` — the distance, valid only when `!isnull`.
    pub value: f64,
    /// `bool isnull`.
    pub isnull: bool,
}
