//! Transaction-system scalar vocabulary (`c.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `InvalidTransactionId` (`access/transam.h`).
pub const InvalidTransactionId: crate::primitive::TransactionId = 0;

/// `TransactionIdIsValid(xid)` (`access/transam.h`).
#[inline]
pub const fn TransactionIdIsValid(xid: crate::primitive::TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// One created/dropped pgstat item carried on commit/abort/prepare WAL
/// records, matching C's `xl_xact_stats_item` (`access/xact.h`:
/// `{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`). The split
/// `objid_lo`/`objid_hi` words (alignment-friendly WAL layout) are carried as
/// the single `u64` they encode.
///
/// **Not the WAL wire layout.** C deliberately keeps `objid` as two 4-byte
/// words so `xl_xact_stats_item` stays 4-byte-aligned in WAL records; this
/// struct's size/alignment differ from the on-disk record member. WAL
/// (de)serialization must re-apply the lo/hi split
/// (`objid_lo = objid as u32`, `objid_hi = (objid >> 32) as u32`;
/// recombine with `((objid_hi as u64) << 32) | objid_lo as u64`) — never
/// treat this struct's bytes as the record image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XlXactStatsItem {
    pub kind: i32,
    pub dboid: crate::primitive::Oid,
    pub objid: u64,
}
