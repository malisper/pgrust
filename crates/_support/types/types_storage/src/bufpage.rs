//! Page / line-pointer vocabulary (`storage/bufpage.h`, `storage/off.h`,
//! `storage/itemid.h`, `access/htup_details.h`): the on-disk line pointer
//! (`ItemIdData`), the page LSN word (`PageXLogRecPtr`), the page sizing
//! constants, and the page-flag / verify / add-item bit constants that
//! `backend-storage-page` (`bufpage.c`) consumes.

use alloc::vec::Vec;

use types_core::{uint16, uint32, uint8, BlockNumber, InvalidBlockNumber, OffsetNumber, Size, XLogRecPtr, BLCKSZ};
use types_error::PgError;

/// `ItemOffset` (`storage/itemid.h`): a line pointer's `lp_off` field.
pub type ItemOffset = uint16;
/// `ItemLength` (`storage/itemid.h`): a line pointer's `lp_len` field.
pub type ItemLength = uint16;

/// `LP_UNUSED` (`storage/itemid.h`): unused (should always have lp_len == 0).
pub const LP_UNUSED: u32 = 0;
/// `LP_NORMAL` (`storage/itemid.h`): used (should always have lp_len > 0).
pub const LP_NORMAL: u32 = 1;
/// `LP_REDIRECT` (`storage/itemid.h`): HOT redirect (should have lp_len == 0).
pub const LP_REDIRECT: u32 = 2;
/// `LP_DEAD` (`storage/itemid.h`): dead, may or may not have storage.
pub const LP_DEAD: u32 = 3;

/// `ItemIdData` (`storage/itemid.h`) — a line pointer: a 4-byte packed
/// `(lp_off:15, lp_flags:2, lp_len:15)` word.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ItemIdData {
    raw: uint32,
}

impl ItemIdData {
    pub const fn new(lp_off: ItemOffset, lp_flags: u32, lp_len: ItemLength) -> Self {
        Self {
            raw: (lp_off as uint32 & 0x7fff)
                | ((lp_flags & 0x0003) << 15)
                | ((lp_len as uint32 & 0x7fff) << 17),
        }
    }

    pub const fn lp_off(&self) -> ItemOffset {
        (self.raw & 0x7fff) as ItemOffset
    }

    pub const fn lp_flags(&self) -> u32 {
        (self.raw >> 15) & 0x0003
    }

    pub const fn lp_len(&self) -> ItemLength {
        ((self.raw >> 17) & 0x7fff) as ItemLength
    }

    /// `ItemIdSetUnused` — set to unused, no storage.
    pub fn set_unused(&mut self) {
        *self = Self::new(0, LP_UNUSED, 0);
    }

    /// `ItemIdSetNormal` — set to normal, with the given storage.
    pub fn set_normal(&mut self, off: ItemOffset, len: ItemLength) {
        *self = Self::new(off, LP_NORMAL, len);
    }

    /// Update the item's offset and length without changing its lp_flags field.
    ///
    /// Mirrors bufpage.c's `PageIndexTupleOverwrite`, which writes `tupid->lp_off`
    /// and `tupid->lp_len` directly while preserving the existing lp_flags
    /// (e.g. `LP_DEAD`).
    pub fn set_storage(&mut self, off: ItemOffset, len: ItemLength) {
        *self = Self::new(off, self.lp_flags(), len);
    }

    /// `ItemIdSetRedirect` — set to redirect, with the given link.
    pub fn set_redirect(&mut self, link: OffsetNumber) {
        *self = Self::new(link, LP_REDIRECT, 0);
    }

    /// `ItemIdSetDead` — set to dead, no storage.
    pub fn set_dead(&mut self) {
        *self = Self::new(0, LP_DEAD, 0);
    }

    /// `ItemIdMarkDead` — set to dead, keeping the existing storage.
    pub fn mark_dead(&mut self) {
        *self = Self::new(self.lp_off(), LP_DEAD, self.lp_len());
    }

    pub const fn is_used(&self) -> bool {
        self.lp_flags() != LP_UNUSED
    }

    pub const fn is_normal(&self) -> bool {
        self.lp_flags() == LP_NORMAL
    }

    pub const fn is_redirected(&self) -> bool {
        self.lp_flags() == LP_REDIRECT
    }

    pub const fn is_dead(&self) -> bool {
        self.lp_flags() == LP_DEAD
    }

    pub const fn has_storage(&self) -> bool {
        self.lp_len() != 0
    }
}

/// `PageXLogRecPtr` (`storage/bufpage.h`): the page LSN, stored as two
/// `uint32`s to avoid alignment assumptions in the on-disk header.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PageXLogRecPtr {
    pub xlogid: uint32,
    pub xrecoff: uint32,
}

impl PageXLogRecPtr {
    pub const fn from_lsn(lsn: XLogRecPtr) -> Self {
        Self {
            xlogid: (lsn >> 32) as uint32,
            xrecoff: lsn as uint32,
        }
    }

    pub const fn lsn(&self) -> XLogRecPtr {
        ((self.xlogid as XLogRecPtr) << 32) | self.xrecoff as XLogRecPtr
    }
}

/// `MaxOffsetNumber` (`storage/off.h`) — `(OffsetNumber)(BLCKSZ /
/// sizeof(ItemIdData))`.
pub const MaxOffsetNumber: OffsetNumber = (BLCKSZ / core::mem::size_of::<ItemIdData>()) as u16;

/// `MovedPartitionsOffsetNumber` (`storage/itemptr.h`): a special t_ctid that
/// marks a tuple moved to another partition by UPDATE.
pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
/// `MovedPartitionsBlockNumber` (`storage/itemptr.h`).
pub const MovedPartitionsBlockNumber: BlockNumber = InvalidBlockNumber;

/// `PD_HAS_FREE_LINES` (`storage/bufpage.h`): are there any unused line pointers?
pub const PD_HAS_FREE_LINES: uint16 = 0x0001;
/// `PD_PAGE_FULL` (`storage/bufpage.h`): not enough free space for new tuple?
pub const PD_PAGE_FULL: uint16 = 0x0002;
/// `PD_ALL_VISIBLE` (`storage/bufpage.h`): all tuples on page are visible to all.
pub const PD_ALL_VISIBLE: uint16 = 0x0004;
/// `PD_VALID_FLAG_BITS` (`storage/bufpage.h`): OR of all valid pd_flags bits.
pub const PD_VALID_FLAG_BITS: uint16 = 0x0007;

/// `PG_PAGE_LAYOUT_VERSION` (`storage/bufpage.h`).
pub const PG_PAGE_LAYOUT_VERSION: uint8 = 4;

/// `SizeOfPageHeaderData` (`storage/bufpage.h`) —
/// `offsetof(PageHeaderData, pd_linp)` == 24 bytes on the supported build.
pub const SizeOfPageHeaderData: Size = 24;

/// `PAI_OVERWRITE` (`storage/bufpage.h`): overwrite an existing item.
pub const PAI_OVERWRITE: i32 = 1 << 0;
/// `PAI_IS_HEAP` (`storage/bufpage.h`): page is a heap (vs. index) page.
pub const PAI_IS_HEAP: i32 = 1 << 1;

/// `PIV_LOG_WARNING` (`storage/bufpage.h`): log a WARNING on checksum failure.
pub const PIV_LOG_WARNING: i32 = 1 << 0;
/// `PIV_LOG_LOG` (`storage/bufpage.h`): log a LOG on checksum failure.
pub const PIV_LOG_LOG: i32 = 1 << 1;
/// `PIV_IGNORE_CHECKSUM_FAILURE` (`storage/bufpage.h`): proceed despite a
/// checksum failure (used by `zero_damaged_pages`).
pub const PIV_IGNORE_CHECKSUM_FAILURE: i32 = 1 << 2;

/// `SizeofHeapTupleHeader` (`access/htup_details.h`) —
/// `offsetof(HeapTupleHeaderData, t_bits)`.
pub const SizeofHeapTupleHeader: usize = 23;

/// `MaxHeapTuplesPerPage` (`access/htup_details.h`):
/// `(BLCKSZ - SizeOfPageHeaderData) / (MAXALIGN(SizeofHeapTupleHeader) +
/// sizeof(ItemIdData))`. `MAXALIGN(23) == 24` on the 8-byte-aligned build.
pub const MaxHeapTuplesPerPage: usize =
    (BLCKSZ - SizeOfPageHeaderData) / (24 + core::mem::size_of::<ItemIdData>());

/// `MaxHeapTupleSize` (`access/htup_details.h`):
/// `(BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData)))`.
/// `MAXALIGN(24 + 4) == 32` on the 8-byte-aligned build.
pub const MaxHeapTupleSize: Size = BLCKSZ - {
    let raw = SizeOfPageHeaderData + core::mem::size_of::<ItemIdData>();
    (raw + 7) & !7
};

/// `PG_IO_ALIGN_SIZE` (`c.h`): the alignment, in bytes, that PostgreSQL likes
/// for page-sized I/O buffers.
pub const PG_IO_ALIGN_SIZE: usize = 4096;

/// An owned, page-sized scratch buffer in local (non-shared) memory.
///
/// The idiomatic owned-tree counterpart of the `Page` (a `char *`) returned by
/// `PageGetTempPage` / `PageGetTempPageCopy` / `PageGetTempPageCopySpecial` in
/// C. In PostgreSQL those allocate via `palloc(pageSize)`; here the scratch
/// page owns its bytes directly, so `PageRestoreTempPage` (`memcpy` back +
/// `pfree`) becomes a copy plus drop with no separate free seam.
///
/// The buffer is always exactly `BLCKSZ` bytes (PostgreSQL only ever formats
/// `BLCKSZ` pages); the active page may use a smaller `pd_pagesize`, which the
/// page accessors honor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageTemp {
    bytes: Vec<u8>,
}

impl PageTemp {
    /// Allocate a zeroed scratch page of `page_size` bytes (rounded up to a full
    /// `BLCKSZ` buffer), the idiomatic equivalent of `palloc(pageSize)`.
    ///
    /// `page_size` is data-derived (it comes from the source page's
    /// `pd_pagesize_version`), so the allocation is bounded by `BLCKSZ` and the
    /// growth uses `try_reserve` per the workspace allocation-safety rule.
    pub fn new(page_size: Size) -> Result<Self, PgError> {
        if page_size == 0 || page_size > BLCKSZ {
            return Err(PgError::error(
                "PageTemp page size is out of range (must be 1..=BLCKSZ)",
            ));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(BLCKSZ)
            .map_err(|_| PgError::error("PageTemp allocation failed"))?;
        bytes.resize(BLCKSZ, 0);
        Ok(Self { bytes })
    }

    /// Read-only view of the scratch page's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Mutable view of the scratch page's bytes.
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    /// Consume the scratch page, returning its owned byte buffer.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}
