//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// One created/dropped pgstat item carried on commit/abort/prepare WAL
/// records, matching C's `xl_xact_stats_item` (`access/xact.h`:
/// `{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`). The split
/// `objid_lo`/`objid_hi` words (alignment-friendly WAL layout) are carried as
/// the single `u64` they encode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XlXactStatsItem {
    pub kind: i32,
    pub dboid: crate::primitive::Oid,
    pub objid: u64,
}
