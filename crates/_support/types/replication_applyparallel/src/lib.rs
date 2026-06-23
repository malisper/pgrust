//! Parallel-apply worker vocabulary (`replication/worker_internal.h`), trimmed
//! to what the launcher's `logicalrep_pa_worker_stop` consumes.
//!
//! The launcher receives a `ParallelApplyWorkerInfo` from
//! applyparallelworker.c and forwards it to applyparallelworker-owned seams
//! that read its `shared->{generation, slot_no}` (under the shared spinlock)
//! and detach its error message queue. The launcher itself never inspects the
//! fields, so the `shm_mq_handle`/`dsm_segment`/`FileSet` substrate
//! applyparallelworker owns is not modeled here.

use ::types_core::primitive::{TransactionId, XLogRecPtr};

/// `ParallelTransState` (worker_internal.h) — commit-ordering state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum ParallelTransState {
    /// `PARALLEL_TRANS_UNKNOWN`
    Unknown = 0,
    /// `PARALLEL_TRANS_STARTED`
    Started = 1,
    /// `PARALLEL_TRANS_FINISHED`
    Finished = 2,
}

/// `ParallelApplyWorkerShared` (worker_internal.h), trimmed to the slot
/// identity the launcher reads under the shared spinlock.
#[derive(Clone, Copy, Debug)]
pub struct ParallelApplyWorkerShared {
    /// `xid`.
    pub xid: TransactionId,
    /// `xact_state`.
    pub xact_state: ParallelTransState,
    /// `logicalrep_worker_generation` — the generation of the corresponding
    /// `LogicalRepWorker` slot.
    pub logicalrep_worker_generation: u16,
    /// `logicalrep_worker_slot_no` — index into the launcher's worker array.
    pub logicalrep_worker_slot_no: i32,
    /// `last_commit_end`.
    pub last_commit_end: XLogRecPtr,
}

/// `ParallelApplyWorkerInfo` (worker_internal.h), trimmed to the fields the
/// launcher's stop path touches. `error_mq_handle` is whether the leader still
/// holds the parallel worker's error queue (the real `shm_mq_handle *` is
/// applyparallelworker substrate; the launcher only asks "is it set?" and
/// "detach it").
#[derive(Clone, Copy, Debug)]
pub struct ParallelApplyWorkerInfo {
    /// `error_mq_handle != NULL`.
    pub has_error_mq_handle: bool,
    /// `in_use`.
    pub in_use: bool,
    /// `shared` — the shared-memory control block.
    pub shared: ParallelApplyWorkerShared,
}
