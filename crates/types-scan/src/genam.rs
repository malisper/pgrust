//! Handle vocabulary for `access/index/genam.c` scans.

/// Opaque token standing in for C's `SysScanDesc` pointer when a systable
/// scan crosses a seam: the genam runtime owns the live scan state and hands
/// the consumer this ticket. Valid from `systable_beginscan*` until the
/// matching `systable_endscan*`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SysScanHandle(pub u64);
