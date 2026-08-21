//! The tombstone write-path codec over the FROZEN spec §15 vocabulary
//! (`pgrc2_format::dml`): a tombstone is a heap row in the table's
//! tombstone relation whose single payload column
//! (`TOMBSTONE_NATTS` = 1) is the deleted **sealed** RowId as int8.
//!
//! This module is deliberately tiny — the format already froze the shape;
//! what machinery adds is the LAW enforcement: a tombstone may only name a
//! sealed rowid (delta rows die by heap delete), checked typed at encode
//! AND at decode-for-bitmap (defense in depth; the bitmap build re-checks
//! because tombstone bytes at product grain arrive from a heap scan this
//! crate does not control).
//!
//! Datum currency: the payload is the packed rowid's raw 64 bits carried
//! in an int8 attribute — encode is a bit-preserving u64 → i64 cast
//! (int8 is the format's chosen carrier, spec §15; ordering of tombstone
//! PAYLOADS is meaningless and never consulted).

use crate::rowid::is_delta_rowid;
use crate::{DeltaError, DeltaResult};

/// Number of attributes in a tombstone relation's schema — re-pinned from
/// the frozen vocabulary so a drift in either place fails the golden suite.
pub const TOMBSTONE_NATTS: usize = pgrc2_format::dml::TOMBSTONE_NATTS;

/// Encode a sealed rowid as the tombstone payload datum (int8, bit-
/// preserving). Typed refusal for delta-tagged rowids (crate doc law).
#[inline]
pub fn tombstone_payload(sealed_rowid: u64) -> DeltaResult<i64> {
    if is_delta_rowid(sealed_rowid) {
        return Err(DeltaError::DeltaTaggedTombstone { rowid: sealed_rowid });
    }
    Ok(sealed_rowid as i64)
}

/// Decode a tombstone payload datum back to the sealed rowid it names.
/// Typed refusal for delta-tagged payloads (a tombstone row carrying one
/// is corrupt or was written past the encode guard — never silently
/// indexed).
#[inline]
pub fn rowid_of_tombstone(payload: i64) -> DeltaResult<u64> {
    let rowid = payload as u64;
    if is_delta_rowid(rowid) {
        return Err(DeltaError::DeltaTaggedTombstone { rowid });
    }
    Ok(rowid)
}
