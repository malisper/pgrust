#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::Cell;

use ::mcx::{Mcx, PgVec};
use ::types_core::{CommandId, GlobalVisStateHandle, TransactionId};

// HTSV_Result (access/heapam.h); i32 codes match the C enum order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum HTSV_Result {
    HEAPTUPLE_DEAD = 0,
    HEAPTUPLE_LIVE,
    HEAPTUPLE_RECENTLY_DEAD,
    HEAPTUPLE_INSERT_IN_PROGRESS,
    HEAPTUPLE_DELETE_IN_PROGRESS,
}

pub use HTSV_Result::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum SnapshotType {
    SNAPSHOT_MVCC = 0,
    SNAPSHOT_SELF,
    SNAPSHOT_ANY,
    SNAPSHOT_TOAST,
    SNAPSHOT_DIRTY,
    SNAPSHOT_HISTORIC_MVCC,
    SNAPSHOT_NON_VACUUMABLE,
}

pub use SnapshotType::*;

// SnapshotData (utils/snapshot.h). xip/subxip capacity outlives xcnt/subxcnt
// (C reuses the GetSnapshotData arrays), so the counts stay explicit; the
// refcounts are Cell (snapmgr writes them through the shared pointer); C's
// intrusive ph_node is replaced by shared-handle pointer identity.
#[derive(Debug)]
pub struct SnapshotData<'mcx> {
    pub snapshot_type: SnapshotType,
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub xip: PgVec<'mcx, TransactionId>,
    pub xcnt: u32,
    pub subxip: PgVec<'mcx, TransactionId>,
    pub subxcnt: i32,
    pub suboverflowed: bool,
    pub takenDuringRecovery: bool,
    pub copied: bool,
    // Cell: CommandCounterIncrement propagates curcid through shared handles
    // (SnapshotSetCommandId / UpdateActiveSnapshotCommandId).
    pub curcid: Cell<CommandId>,
    pub speculativeToken: u32,
    // SnapshotDirty out-params for probes behind the shared &Snapshot seam
    // (index fetch): the marshal copies the per-tuple write-back here, since
    // the C &mut xmin/xmax channel isn't reachable through that seam.
    pub dirty_xmin: Cell<TransactionId>,
    pub dirty_xmax: Cell<TransactionId>,
    pub dirty_speculative_token: Cell<u32>,
    pub vistest: GlobalVisStateHandle,
    pub active_count: Cell<u32>,
    pub regd_count: Cell<u32>,
    pub snapXactCompletionCount: u64,
}

impl<'mcx> SnapshotData<'mcx> {
    // C's static-snapshot form `{SNAPSHOT_ANY}`: type-only, rest zeroed.
    pub fn sentinel(mcx: Mcx<'mcx>, snapshot_type: SnapshotType) -> Self {
        SnapshotData {
            snapshot_type,
            xmin: 0,
            xmax: 0,
            xip: PgVec::new_in(mcx),
            xcnt: 0,
            subxip: PgVec::new_in(mcx),
            subxcnt: 0,
            suboverflowed: false,
            takenDuringRecovery: false,
            copied: false,
            curcid: Cell::new(0),
            speculativeToken: 0,
            dirty_xmin: Cell::new(0),
            dirty_xmax: Cell::new(0),
            dirty_speculative_token: Cell::new(0),
            vistest: GlobalVisStateHandle::new(0),
            active_count: Cell::new(0),
            regd_count: Cell::new(0),
            snapXactCompletionCount: 0,
        }
    }
}

pub const XVM_SNAP_VALID: u8 = 1;
pub const XVM_IN_SNAPSHOT: u8 = 2;
pub const XVM_COMMIT_VALID: u8 = 4;
pub const XVM_COMMITTED: u8 = 8;
pub const XVM_HINT_OK: u8 = 16;

// Per-page xid-status memo for batch MVCC visibility (one page walk under one
// buffer lock = one consistent resolution window). Hint denials are never
// cached: the page LSN can advance mid-walk and re-allow them.
pub struct XidVisMemo {
    xids: [TransactionId; XidVisMemo::N],
    flags: [u8; XidVisMemo::N],
    next: u8,
}

impl Default for XidVisMemo {
    fn default() -> Self {
        Self::new()
    }
}

impl XidVisMemo {
    const N: usize = 4;

    #[inline]
    pub fn new() -> Self {
        XidVisMemo { xids: [0; Self::N], flags: [0; Self::N], next: 0 }
    }

    #[inline]
    pub fn get(&self, xid: TransactionId) -> u8 {
        for i in 0..Self::N {
            if self.xids[i] == xid {
                return self.flags[i];
            }
        }
        0
    }

    #[inline]
    pub fn merge(&mut self, xid: TransactionId, flags: u8) {
        // Keys must be nonzero: xids[i] == 0 marks an empty slot. Resolvers
        // hoist non-normal xids before the memo (a raw xmin of 0 is a real
        // on-disk state — heap_abort_speculative leaves xmin invalid without
        // the HEAP_XMIN_INVALID hint; see PageMemoResolve::did_commit).
        debug_assert!(xid != 0);
        for i in 0..Self::N {
            if self.xids[i] == xid {
                self.flags[i] |= flags;
                return;
            }
        }
        let slot = self.next as usize;
        self.xids[slot] = xid;
        self.flags[slot] = flags;
        self.next = ((slot + 1) % Self::N) as u8;
    }
}

#[inline]
pub fn IsMVCCSnapshot(snapshot: &SnapshotData<'_>) -> bool {
    snapshot.snapshot_type == SNAPSHOT_MVCC
        || snapshot.snapshot_type == SNAPSHOT_HISTORIC_MVCC
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    #[test]
    fn snapshot_type_codes_match_snapshot_h() {
        assert_eq!(SNAPSHOT_MVCC as i32, 0);
        assert_eq!(SNAPSHOT_SELF as i32, 1);
        assert_eq!(SNAPSHOT_ANY as i32, 2);
        assert_eq!(SNAPSHOT_TOAST as i32, 3);
        assert_eq!(SNAPSHOT_DIRTY as i32, 4);
        assert_eq!(SNAPSHOT_HISTORIC_MVCC as i32, 5);
        assert_eq!(SNAPSHOT_NON_VACUUMABLE as i32, 6);
    }

    #[test]
    fn htsv_codes_match_heapam_h() {
        assert_eq!(HEAPTUPLE_DEAD as i32, 0);
        assert_eq!(HEAPTUPLE_LIVE as i32, 1);
        assert_eq!(HEAPTUPLE_RECENTLY_DEAD as i32, 2);
        assert_eq!(HEAPTUPLE_INSERT_IN_PROGRESS as i32, 3);
        assert_eq!(HEAPTUPLE_DELETE_IN_PROGRESS as i32, 4);
    }

    #[test]
    fn sentinel_is_type_only_zero_form() {
        let cx = MemoryContext::new("test");
        let snap = SnapshotData::sentinel(cx.mcx(), SNAPSHOT_ANY);
        assert_eq!(snap.snapshot_type, SNAPSHOT_ANY);
        assert_eq!(snap.xmin, 0);
        assert_eq!(snap.xmax, 0);
        assert!(snap.xip.is_empty());
        assert_eq!(snap.xcnt, 0);
        assert_eq!(snap.subxcnt, 0);
        assert!(!snap.copied);
        assert_eq!(snap.active_count.get(), 0);
        assert_eq!(snap.regd_count.get(), 0);
        assert!(snap.vistest.is_none());
    }

    #[test]
    fn mvcc_predicate_matches_snapmgr_h() {
        let cx = MemoryContext::new("test");
        for (ty, expect) in [
            (SNAPSHOT_MVCC, true),
            (SNAPSHOT_HISTORIC_MVCC, true),
            (SNAPSHOT_SELF, false),
            (SNAPSHOT_ANY, false),
            (SNAPSHOT_TOAST, false),
            (SNAPSHOT_DIRTY, false),
            (SNAPSHOT_NON_VACUUMABLE, false),
        ] {
            assert_eq!(IsMVCCSnapshot(&SnapshotData::sentinel(cx.mcx(), ty)), expect);
        }
    }
}
