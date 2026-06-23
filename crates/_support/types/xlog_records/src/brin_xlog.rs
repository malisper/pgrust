//! BRIN rmgr WAL record bodies (`access/brin_xlog.h`).

use crate::bytes::{u16_at, u32_at};
use ::types_core::{BlockNumber, OffsetNumber};

/// `xl_brin_createidx`: `{BlockNumber pagesPerRange; uint16 version;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_brin_createidx {
    pub pagesPerRange: BlockNumber,
    pub version: u16,
}

impl xl_brin_createidx {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            pagesPerRange: u32_at(rec, 0),
            version: u16_at(rec, 4),
        }
    }
}

/// `xl_brin_insert`: `{BlockNumber heapBlk; BlockNumber pagesPerRange;
/// OffsetNumber offnum;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_brin_insert {
    pub heapBlk: BlockNumber,
    pub pagesPerRange: BlockNumber,
    pub offnum: OffsetNumber,
}

impl xl_brin_insert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            heapBlk: u32_at(rec, 0),
            pagesPerRange: u32_at(rec, 4),
            offnum: u16_at(rec, 8),
        }
    }
}

/// `xl_brin_update`: `{OffsetNumber oldOffnum; xl_brin_insert insert;}` —
/// the embedded insert is 4-aligned at 4.
#[derive(Clone, Copy, Debug)]
pub struct xl_brin_update {
    pub oldOffnum: OffsetNumber,
    pub insert: xl_brin_insert,
}

impl xl_brin_update {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            oldOffnum: u16_at(rec, 0),
            insert: xl_brin_insert::from_bytes(&rec[4..]),
        }
    }
}

/// `xl_brin_samepage_update`: `{OffsetNumber offnum;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_brin_samepage_update {
    pub offnum: OffsetNumber,
}

impl xl_brin_samepage_update {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { offnum: u16_at(rec, 0) }
    }
}

/// `xl_brin_revmap_extend`: `{BlockNumber targetBlk;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_brin_revmap_extend {
    pub targetBlk: BlockNumber,
}

impl xl_brin_revmap_extend {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { targetBlk: u32_at(rec, 0) }
    }
}

/// `xl_brin_desummarize`: `{BlockNumber pagesPerRange; BlockNumber heapBlk;
/// OffsetNumber regOffset;}`.
#[derive(Clone, Copy, Debug)]
pub struct xl_brin_desummarize {
    pub pagesPerRange: BlockNumber,
    pub heapBlk: BlockNumber,
    pub regOffset: OffsetNumber,
}

impl xl_brin_desummarize {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            pagesPerRange: u32_at(rec, 0),
            heapBlk: u32_at(rec, 4),
            regOffset: u16_at(rec, 8),
        }
    }
}
