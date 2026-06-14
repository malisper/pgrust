//! `utils/snapshot.h` — the snapshot type tag and `SnapshotData`.

use alloc::vec::Vec;

use types_core::{CommandId, TransactionId};

pub use types_core::GlobalVisStateHandle;

/// `HTSV_Result` (`access/heapam.h`) — the status of a tuple as judged by
/// `HeapTupleSatisfiesVacuum`. Discriminants mirror the C enum order so the
/// integer codes match (and so `as i32` agrees with the `i32`-coded vacuum
/// seam).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum HTSV_Result {
    /// `HEAPTUPLE_DEAD` — tuple is dead and deletable.
    HEAPTUPLE_DEAD = 0,
    /// `HEAPTUPLE_LIVE` — tuple is live (committed, not deleted).
    HEAPTUPLE_LIVE,
    /// `HEAPTUPLE_RECENTLY_DEAD` — tuple is dead, but not deletable yet.
    HEAPTUPLE_RECENTLY_DEAD,
    /// `HEAPTUPLE_INSERT_IN_PROGRESS` — inserting transaction is still active.
    HEAPTUPLE_INSERT_IN_PROGRESS,
    /// `HEAPTUPLE_DELETE_IN_PROGRESS` — deleting transaction is still active.
    HEAPTUPLE_DELETE_IN_PROGRESS,
}

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

/// Result of `ResolveCminCmaxDuringDecoding` (reorderbuffer.c) — the
/// combo-CID-resolved cmin/cmax for a tuple seen during logical decoding.
/// `resolved` mirrors the C `bool` return; `cmin`/`cmax` are the out-parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolveCminCmaxResult {
    /// C `bool` return: whether the combo CID was decoded and resolved.
    pub resolved: bool,
    /// `*cmin` out-parameter.
    pub cmin: CommandId,
    /// `*cmax` out-parameter.
    pub cmax: CommandId,
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

    /// `vistest` — for `SNAPSHOT_NON_VACUUMABLE`, the `GlobalVisState *` that
    /// decides whether a deleting xid is removable. `id == 0` is the C `NULL`.
    pub vistest: GlobalVisStateHandle,

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

    /// `speculativeToken` — `HeapTupleSatisfiesDirty` output: the speculative
    /// insertion token of an in-progress speculative insertion (0 if none).
    pub speculativeToken: u32,

    /// `active_count` — refcount on the ActiveSnapshot stack.
    pub active_count: u32,
    /// `regd_count` — refcount on RegisteredSnapshots.
    pub regd_count: u32,

    /// `snapXactCompletionCount` — the transaction completion count at the time
    /// `GetSnapshotData()` built this snapshot.
    pub snapXactCompletionCount: u64,
}

impl SnapshotData {
    /// A static, type-only sentinel snapshot — the C
    /// `SnapshotData SnapshotAnyData = {SNAPSHOT_ANY}` / `SnapshotSelfData` /
    /// `SnapshotDirtyData` form, where only `snapshot_type` is meaningful and
    /// every other field is C zero-initialized. Used where the executor passes
    /// one of the static snapshot identities (e.g. `SnapshotAny`).
    pub const fn sentinel(snapshot_type: SnapshotType) -> Self {
        SnapshotData {
            snapshot_type,
            vistest: GlobalVisStateHandle::new(0),
            xmin: 0,
            xmax: 0,
            xip: Vec::new(),
            xcnt: 0,
            subxip: Vec::new(),
            subxcnt: 0,
            suboverflowed: false,
            takenDuringRecovery: false,
            copied: false,
            curcid: 0,
            speculativeToken: 0,
            active_count: 0,
            regd_count: 0,
            snapXactCompletionCount: 0,
        }
    }
}

/// `IsMVCCSnapshot(snapshot)` (`utils/snapmgr.h`).
#[inline]
pub fn IsMVCCSnapshot(snapshot: &SnapshotData) -> bool {
    snapshot.snapshot_type == SnapshotType::SNAPSHOT_MVCC
        || snapshot.snapshot_type == SnapshotType::SNAPSHOT_HISTORIC_MVCC
}
