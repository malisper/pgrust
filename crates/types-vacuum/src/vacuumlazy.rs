//! Identity handles + seam DTOs for the lazy (concurrent) heap VACUUM driver
//! (`src/backend/access/heap/vacuumlazy.c`).
//!
//! The substrate-owned objects (buffer-access strategy ring, dead-TID store,
//! parallel-vacuum state, visibility test, read stream) cross the driver's
//! seams as small `Copy` id handles (`id == 0` is the C `NULL`). The arg/result
//! DTOs mirror the C functions' by-reference out-params field-for-field.

use alloc::vec::Vec;

use types_core::{BlockNumber, Buffer, MultiXactId, OffsetNumber, Oid, TransactionId, XLogRecPtr};

use crate::vacuum::VacuumCutoffs;
use crate::vacuum::PruneFreezeResult;
use crate::vacuumparallel::VacDeadItemsInfo;

// ---- Identity handles for substrate-owned state ----------------------

/// A `BufferAccessStrategy` ring. `id == 0` is the C `NULL` strategy (use all
/// of shared buffers); the runtime maps a non-zero id to the ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct StrategyHandle {
    pub id: u64,
}
impl StrategyHandle {
    pub const fn new(id: u64) -> Self {
        Self { id }
    }
    /// The C `NULL` strategy.
    pub const fn none() -> Self {
        Self { id: 0 }
    }
    pub const fn is_none(self) -> bool {
        self.id == 0
    }
}

/// A `TidStore` (the radix-tree dead-TID store). `id == 0` is the C `NULL`
/// store (not yet allocated).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct TidStore {
    pub id: u64,
}
impl TidStore {
    pub const fn new(id: u64) -> Self {
        Self { id }
    }
    pub const fn none() -> Self {
        Self { id: 0 }
    }
    pub const fn is_none(self) -> bool {
        self.id == 0
    }
}

/// A `TidStoreIter` (the radix iterator's runtime identity). `id == 0` is the
/// C `NULL`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct TidStoreIterHandle {
    pub id: u64,
}
impl TidStoreIterHandle {
    pub const fn new(id: u64) -> Self {
        Self { id }
    }
}

/// A `ParallelVacuumState`. `id == 0` is the C `NULL` (serial vacuum).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct ParallelVacuumStateHandle {
    pub id: u64,
}
impl ParallelVacuumStateHandle {
    pub const fn new(id: u64) -> Self {
        Self { id }
    }
    pub const fn none() -> Self {
        Self { id: 0 }
    }
    pub const fn is_none(self) -> bool {
        self.id == 0
    }
}

/// A `GlobalVisState` (snapshot-visibility test). `id == 0` is `NULL`.
///
/// Defined in `types-core` to avoid a `types-snapshot -> types-vacuum`
/// dependency cycle (`SnapshotData.vistest` needs it); re-exported here so
/// existing `types_vacuum::vacuumlazy::GlobalVisStateHandle` consumers keep
/// working unchanged.
pub use types_core::GlobalVisStateHandle;

/// A `ReadStream` (sequential read-ahead over a relation fork).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct ReadStreamHandle {
    pub id: u64,
}
impl ReadStreamHandle {
    pub const fn new(id: u64) -> Self {
        Self { id }
    }
}

/// Which read-stream callback the second-pass / scan stream should drive. The C
/// passes a function pointer; the owned model passes this tag and the runtime
/// selects the in-engine callback to invoke between buffers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ScanCallback {
    /// `heap_vac_scan_next_block` — phase-I block selection state machine.
    ScanNextBlock,
    /// `vacuum_reap_lp_read_stream_next` — phase-III TID-store iteration.
    ReapNextBlock,
}

/// One block's reaped-offset payload the phase-III read stream yields
/// (`TidStoreIterResult`): the block and its dead offsets.
#[derive(Clone, Debug, Default)]
pub struct ReapBlockInfo {
    pub blkno: BlockNumber,
    /// The dead offsets recorded for `blkno` (`TidStoreGetBlockOffsets`).
    pub offsets: Vec<OffsetNumber>,
}

// ---- commands/vacuum.h — cutoff / relstat / per-index command layer ----

/// `vac_update_relstats(...)` inputs (the C out-params come back in the result).
#[derive(Clone, Copy, Debug)]
pub struct UpdateRelStatsArgs {
    pub relation: Oid,
    pub num_pages: BlockNumber,
    pub num_tuples: f64,
    pub num_all_visible_pages: BlockNumber,
    pub num_all_frozen_pages: BlockNumber,
    pub hasindex: bool,
    pub frozenxid: TransactionId,
    pub minmulti: MultiXactId,
    pub in_outer_xact: bool,
}

// ---- access/heapam.h — heap-page prune/freeze + visibility predicates ----

/// `heap_page_prune_and_freeze(...)` out-params the driver threads back into the
/// `LVRelState`: the per-page result plus the running new-relfrozenxid /
/// new-relminmxid the C updates by reference, plus the `off_loc` offset.
#[derive(Clone, Copy, Debug)]
pub struct PruneAndFreezeOut {
    pub presult: PruneFreezeResult,
    pub new_relfrozen_xid: TransactionId,
    pub new_relmin_mxid: MultiXactId,
    pub off_loc: OffsetNumber,
}

/// `heap_page_prune_and_freeze(...)` inputs.
#[derive(Clone, Copy, Debug)]
pub struct PruneAndFreezeArgs {
    pub relation: Oid,
    pub buffer: Buffer,
    pub vistest: GlobalVisStateHandle,
    pub options: i32,
    pub cutoffs: VacuumCutoffs,
    pub reason: i32,
    /// `*new_relfrozen_xid` in (C threads this by ref).
    pub new_relfrozen_xid_in: TransactionId,
    /// `*new_relmin_mxid` in.
    pub new_relmin_mxid_in: MultiXactId,
    /// `*off_loc` in.
    pub off_loc_in: OffsetNumber,
}

// ---- access/visibilitymap.h — visibilitymap_set inputs ----

/// `visibilitymap_set(...)` inputs.
#[derive(Clone, Copy, Debug)]
pub struct VmSetArgs {
    pub rel: Oid,
    pub heap_blk: BlockNumber,
    pub heap_buf: Buffer,
    pub rec_ptr: XLogRecPtr,
    pub vm_buf: Buffer,
    pub cutoff_xid: TransactionId,
    pub flags: u8,
}

// ---- storage/bufpage.h — line-pointer state for truncation / second pass ----

/// State of one line pointer the truncation back-scan / second pass reads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LinePointerState {
    pub is_used: bool,
    pub is_redirected: bool,
    pub is_dead: bool,
    pub is_normal: bool,
    pub has_storage: bool,
}

// ---- vacuumparallel.c — parallel index vacuuming ----

/// Result of `parallel_vacuum_init`: the new pvs plus the dead-items store and
/// info it allocated in DSM.
#[derive(Clone, Copy, Debug)]
pub struct ParallelVacuumInit {
    pub pvs: ParallelVacuumStateHandle,
    pub dead_items: TidStore,
    pub dead_items_info: VacDeadItemsInfo,
}

/// `parallel_vacuum_init(...)` inputs.
#[derive(Clone, Debug)]
pub struct ParallelVacuumInitArgs {
    pub rel: Oid,
    pub indrels: Vec<Oid>,
    pub nindexes: i32,
    pub nrequested: i32,
    pub vac_work_mem: i32,
    pub elevel: i32,
    pub bstrategy: StrategyHandle,
}
