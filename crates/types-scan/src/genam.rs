//! System-table scan vocabulary (`access/genam.h`).

use types_snapshot::SnapshotData;

/// `SysScanDescData` (`access/genam.h`), trimmed.
///
/// C spells the struct out: `heap_rel`, `irel`, the live `TableScanDescData`
/// / `IndexScanDescData` pointers, `snapshot`, and the result `slot`. The
/// live scan-state pointers belong to the unported genam owner
/// (`access/index/genam.c`), which extends this struct with the fields its
/// implementation needs when it lands; today the struct carries the one
/// field representable at this layer. Consumers never construct one — they
/// receive it from `systable_beginscan*` (wrapped in the seam crate's scan
/// guard) and hand it back to `systable_getnext*` / `systable_endscan*`.
#[derive(Debug)]
pub struct SysScanDescData {
    /// `snapshot` — the snapshot to unregister at end of scan, or `None`
    /// (C's NULL: the caller's snapshot, nothing to unregister).
    pub snapshot: Option<SnapshotData>,
}
