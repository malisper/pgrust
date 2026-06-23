//! Node-state and plan-node vocabulary consumed by `nodeBitmapHeapscan.c`
//! (execnodes.h / plannodes.h).
//!
//! The node-state / plan-node / shared-DSM structs relocated DOWN into
//! `types-nodes` (module [`::nodes::nodebitmapheapscan`]) so the central
//! `PlanStateNode` dispatch enum can name `BitmapHeapScanState` as a variant.
//! That relocation became acyclic with the slot-vocab F0 keystone: "Edge A"
//! added the `types-nodes -> types-tableam` edge (for `TableScanDesc`) and
//! "Edge B" relocated the typed shared-DSM-object primitive (`SharedRef` /
//! `SharedSlice` / `SharedDsmObject`) DOWN into `types-parallel`. All of those
//! crates sit below `types-nodes`, so the structs name them without a cycle.
//!
//! This module re-exports the relocated vocabulary at its historical
//! `crate::nodes::…` path so the executor logic in `lib.rs` (and the in-crate
//! tests) compile unchanged. The only item that STAYS here is [`SpinLockGuard`],
//! the `s_lock`-based RAII helper — runtime executor machinery, not node
//! vocabulary, and the one item that would otherwise pull
//! `backend-storage-lmgr-s-lock` into `types-nodes`.

use types_storage::Spinlock;

pub use ::nodes::nodebitmapheapscan::{
    BitmapHeapScan, BitmapHeapScanInstrumentation, BitmapHeapScanState, NodeSinstrument,
    ParallelBitmapHeapState, Plan, Scan, SharedBitmapHeapInstrumentation,
    SharedBitmapHeapScanInstr, SharedBitmapState, BM_FINISHED, BM_INITIAL, BM_INPROGRESS,
};

/// RAII spinlock guard: `SpinLockAcquire` on construction, `SpinLockRelease`
/// on `Drop`. Acquire is the uncontended TAS fast path, falling back to the
/// `s_lock` backoff loop on contention (storage/lmgr/s_lock.c).
pub struct SpinLockGuard<'a> {
    lock: &'a Spinlock,
}

impl<'a> SpinLockGuard<'a> {
    /// `SpinLockAcquire(lock)`.
    pub fn acquire(lock: &'a Spinlock) -> Self {
        // SpinLockAcquire: TAS_SPIN; on failure, s_lock() the backoff loop.
        if lock.tas_spin() != 0 {
            s_lock::s_lock(lock, Some(file!()), line!() as i32, None);
        }
        SpinLockGuard { lock }
    }
}

impl Drop for SpinLockGuard<'_> {
    /// `SpinLockRelease(lock)`.
    fn drop(&mut self) {
        self.lock.unlock();
    }
}
