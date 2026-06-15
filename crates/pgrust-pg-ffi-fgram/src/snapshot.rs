//! `utils/snapshot.h` — the in-memory `SnapshotData` struct and `SnapshotType`
//! enum, plus the opaque `GlobalVisState`/`pairingheap_node` helpers they embed.
//!
//! `SnapshotData` is an in-memory (not on-disk) struct, but it crosses the C ABI
//! boundary between the snapshot manager (`snapmgr.c`), the visibility routines
//! (`heapam_visibility.c`) and every table AM, so it is laid out `#[repr(C)]`
//! with exact-layout assertions.  The xid arrays (`xip`/`subxip`) and the
//! `vistest` pointer are owned/allocated elsewhere; this crate only describes
//! the shape so callers can read the fields.

use core::ffi::c_void;

use crate::algorithms::pairingheap_node;
use crate::{int32, uint32, uint64, CommandId, TransactionId};

/// `typedef struct SnapshotData *Snapshot` (snapshot.h).  The opaque
/// `access::Snapshot` (`*mut c_void`) is the canonical alias used across the
/// crate; this typed pointer is the same address, narrowed for field access.
pub type SnapshotPtr = *mut SnapshotData;

/// `struct GlobalVisState` (snapmgr internal); opaque to everyone else.
pub type GlobalVisStatePtr = *mut c_void;

/// `enum SnapshotType` (snapshot.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum SnapshotType {
    /// A tuple is visible iff valid for the given MVCC snapshot.
    SNAPSHOT_MVCC = 0,
    /// A tuple is visible iff valid "for itself".
    SNAPSHOT_SELF,
    /// Any tuple is visible.
    SNAPSHOT_ANY,
    /// A tuple is visible iff valid as a TOAST row.
    SNAPSHOT_TOAST,
    /// A tuple is visible including effects of open transactions.
    SNAPSHOT_DIRTY,
    /// MVCC rules, but usable in timetravel context (logical decoding).
    SNAPSHOT_HISTORIC_MVCC,
    /// A tuple is visible iff it might be visible to some transaction.
    SNAPSHOT_NON_VACUUMABLE,
}
pub use SnapshotType::*;

/// `struct SnapshotData` (snapshot.h).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SnapshotData {
    /// type of snapshot
    pub snapshot_type: SnapshotType,
    /// all XID < xmin are visible to me
    pub xmin: TransactionId,
    /// all XID >= xmax are invisible to me
    pub xmax: TransactionId,
    /// in-progress (or, for historic, committed) xact IDs in [xmin, xmax)
    pub xip: *mut TransactionId,
    /// # of xact ids in xip[]
    pub xcnt: uint32,
    /// in-progress subxact IDs (or, for historic, all replayed xids)
    pub subxip: *mut TransactionId,
    /// # of xact ids in subxip[]
    pub subxcnt: int32,
    /// has the subxip array overflowed?
    pub suboverflowed: bool,
    /// recovery-shaped snapshot?
    pub takenDuringRecovery: bool,
    /// false if it's a static snapshot
    pub copied: bool,
    /// in my xact, CID < curcid are visible
    pub curcid: CommandId,
    /// extra return value for HeapTupleSatisfiesDirty
    pub speculativeToken: uint32,
    /// used to determine whether a row could be vacuumed (NON_VACUUMABLE)
    pub vistest: GlobalVisStatePtr,
    /// refcount on ActiveSnapshot stack
    pub active_count: uint32,
    /// refcount on RegisteredSnapshots
    pub regd_count: uint32,
    /// link in the RegisteredSnapshots heap
    pub ph_node: pairingheap_node,
    /// txn completion count at GetSnapshotData() time
    pub snapXactCompletionCount: uint64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn snapshotdata_layout_matches_pg_abi() {
        assert_eq!(size_of::<pairingheap_node>(), 24);
        assert_eq!(size_of::<SnapshotData>(), 104);
        assert_eq!(align_of::<SnapshotData>(), 8);
        assert_eq!(offset_of!(SnapshotData, snapshot_type), 0);
        assert_eq!(offset_of!(SnapshotData, xmin), 4);
        assert_eq!(offset_of!(SnapshotData, xmax), 8);
        assert_eq!(offset_of!(SnapshotData, xip), 16);
        assert_eq!(offset_of!(SnapshotData, xcnt), 24);
        assert_eq!(offset_of!(SnapshotData, subxip), 32);
        assert_eq!(offset_of!(SnapshotData, subxcnt), 40);
        assert_eq!(offset_of!(SnapshotData, suboverflowed), 44);
        assert_eq!(offset_of!(SnapshotData, takenDuringRecovery), 45);
        assert_eq!(offset_of!(SnapshotData, copied), 46);
        assert_eq!(offset_of!(SnapshotData, curcid), 48);
        assert_eq!(offset_of!(SnapshotData, speculativeToken), 52);
        assert_eq!(offset_of!(SnapshotData, vistest), 56);
        assert_eq!(offset_of!(SnapshotData, active_count), 64);
        assert_eq!(offset_of!(SnapshotData, regd_count), 68);
        assert_eq!(offset_of!(SnapshotData, ph_node), 72);
        assert_eq!(offset_of!(SnapshotData, snapXactCompletionCount), 96);
    }
}
