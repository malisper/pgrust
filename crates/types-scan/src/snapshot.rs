//! Handle vocabulary for snapshots (`utils/snapshot.h`).

/// Opaque token standing in for C's `Snapshot` pointer when a snapshot
/// crosses a seam: the snapshot-manager runtime owns the live `SnapshotData`
/// and hands the consumer this ticket (C callers likewise never own the
/// pointed-to snapshot).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotHandle(pub u64);
