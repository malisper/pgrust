//! `utils/snapshot.h` — the snapshot type tag and a trimmed `SnapshotData`.

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

/// `SnapshotData` (`utils/snapshot.h`), trimmed to the type tag — the only
/// field tableam-level ports consume. The xmin/xmax/xip payload lands with
/// the snapmgr owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotData {
    /// `snapshot_type` — what these values mean.
    pub snapshot_type: SnapshotType,
}

/// `IsMVCCSnapshot(snapshot)` (`utils/snapmgr.h`).
#[inline]
pub fn IsMVCCSnapshot(snapshot: &SnapshotData) -> bool {
    snapshot.snapshot_type == SnapshotType::SNAPSHOT_MVCC
        || snapshot.snapshot_type == SnapshotType::SNAPSHOT_HISTORIC_MVCC
}
