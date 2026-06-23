//! Native-endian field reads for record-body decoding, panicking where the C
//! cast would read past the record.

use ::types_core::FullTransactionId;
use ::types_storage::RelFileLocator;
use ::types_tuple::{BlockIdData, ItemPointerData};

pub(crate) const SHORT_RECORD: &str = "WAL record data shorter than the C struct it must hold";

pub(crate) fn u8_at(d: &[u8], off: usize) -> u8 {
    *d.get(off).expect(SHORT_RECORD)
}

pub(crate) fn i8_at(d: &[u8], off: usize) -> i8 {
    u8_at(d, off) as i8
}

pub(crate) fn bool_at(d: &[u8], off: usize) -> bool {
    u8_at(d, off) != 0
}

pub(crate) fn u16_at(d: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(d[off..off + 2].try_into().expect(SHORT_RECORD))
}

pub(crate) fn u32_at(d: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(d[off..off + 4].try_into().expect(SHORT_RECORD))
}

pub(crate) fn i32_at(d: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(d[off..off + 4].try_into().expect(SHORT_RECORD))
}

pub(crate) fn u64_at(d: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(d[off..off + 8].try_into().expect(SHORT_RECORD))
}

pub(crate) fn f64_at(d: &[u8], off: usize) -> f64 {
    f64::from_ne_bytes(d[off..off + 8].try_into().expect(SHORT_RECORD))
}

pub(crate) fn full_xid_at(d: &[u8], off: usize) -> FullTransactionId {
    FullTransactionId::from_u64(u64_at(d, off))
}

/// A `RelFileLocator` at `off`: `{spcOid, dbOid, relNumber}`, three `Oid`s.
pub(crate) fn locator_at(d: &[u8], off: usize) -> RelFileLocator {
    RelFileLocator {
        spcOid: u32_at(d, off),
        dbOid: u32_at(d, off + 4),
        relNumber: u32_at(d, off + 8),
    }
}

/// A `BlockIdData {bi_hi, bi_lo}` at `off`.
pub(crate) fn block_id_at(d: &[u8], off: usize) -> BlockIdData {
    BlockIdData {
        bi_hi: u16_at(d, off),
        bi_lo: u16_at(d, off + 2),
    }
}

/// An `ItemPointerData {ip_blkid, ip_posid}` at `off` (6 bytes, 2-aligned).
pub(crate) fn item_pointer_at(d: &[u8], off: usize) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: block_id_at(d, off),
        ip_posid: u16_at(d, off + 4),
    }
}
