//! `utils/snapshot.h` — the snapshot type tag and `SnapshotData`.

use alloc::vec::Vec;

use types_core::{CommandId, TransactionId};

/// `SnapshotType` (`utils/snapshot.h`) — the different snapshot semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotType {
    /// `SNAPSHOT_MVCC` — accordant with the xmin/xmax/xip MVCC rules.
    SNAPSHOT_MVCC = 0,
    /// `SNAPSHOT_SELF` — effects of the current command are visible.
    SNAPSHOT_SELF,
    /// `SNAPSHOT_ANY` — any tuple is visible.
    SNAPSHOT_ANY,
    /// `SNAPSHOT_TOAST` — visibility rules for TOAST table access.
    SNAPSHOT_TOAST,
    /// `SNAPSHOT_DIRTY` — in-progress changes are visible.
    SNAPSHOT_DIRTY,
    /// `SNAPSHOT_HISTORIC_MVCC` — MVCC over a historic catalog state
    /// (logical decoding).
    SNAPSHOT_HISTORIC_MVCC,
    /// `SNAPSHOT_NON_VACUUMABLE` — everything `HeapTupleSatisfiesVacuum`
    /// would not call dead.
    SNAPSHOT_NON_VACUUMABLE,
}

/// `SnapshotData` (`utils/snapshot.h`).
///
/// The MVCC payload (xmin/xmax/xip/subxip and the snapshot-manager
/// bookkeeping counts) lands with the `utils/time/snapmgr.c` owner. The C
/// `vistest` (a `GlobalVisState *` owned by procarray) and `speculativeToken`
/// (a `HeapTupleSatisfiesDirty` return slot) are not consumed by any ported
/// unit and are omitted until a consumer needs them; the intrusive
/// `pairingheap_node ph_node` is replaced by snapmgr's `Vec`-scanned
/// registered set, so it is not carried here either.
///
/// `xip`/`subxip` are owned `Vec`s rather than raw arrays; `xcnt`/`subxcnt`
/// remain explicit (their lengths) to mirror the C field-by-field semantics
/// the manager relies on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotData {
    /// `snapshot_type` — what these values mean.
    pub snapshot_type: SnapshotType,

    /// `xmin` — all XID < xmin are visible to me.
    pub xmin: TransactionId,
    /// `xmax` — all XID >= xmax are invisible to me.
    pub xmax: TransactionId,

    /// `xip` — in-progress xact IDs (committed ones for historic snapshots).
    pub xip: Vec<TransactionId>,
    /// `xcnt` — number of xact ids in `xip`.
    pub xcnt: u32,

    /// `subxip` — in-progress subxact IDs (all replayed xids for historic).
    pub subxip: Vec<TransactionId>,
    /// `subxcnt` — number of xact ids in `subxip`.
    pub subxcnt: i32,
    /// `suboverflowed` — has the subxip array overflowed?
    pub suboverflowed: bool,

    /// `takenDuringRecovery` — recovery-shaped snapshot?
    pub takenDuringRecovery: bool,
    /// `copied` — false if it's a static snapshot.
    pub copied: bool,

    /// `curcid` — in my xact, CID < curcid are visible.
    pub curcid: CommandId,

    /// `active_count` — refcount on the ActiveSnapshot stack.
    pub active_count: u32,
    /// `regd_count` — refcount on RegisteredSnapshots.
    pub regd_count: u32,

    /// `snapXactCompletionCount` — the transaction completion count at the time
    /// `GetSnapshotData()` built this snapshot.
    pub snapXactCompletionCount: u64,
}

/// `IsMVCCSnapshot(snapshot)` (`utils/snapmgr.h`).
#[inline]
pub fn IsMVCCSnapshot(snapshot: &SnapshotData) -> bool {
    snapshot.snapshot_type == SnapshotType::SNAPSHOT_MVCC
        || snapshot.snapshot_type == SnapshotType::SNAPSHOT_HISTORIC_MVCC
}
