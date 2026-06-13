//! WAL record types and resource-manager constants.

use mcx::PgVec;
use types_core::{pg_crc32c, uint16, uint32, uint8, RmgrId, TransactionId, XLogRecPtr};

/// `MAX_XLINFO_TYPES` (access/xlogstats.h) — sixteen per-record buckets per
/// rmgr (the four xl_info bits in the rmgr's domain).
pub const MAX_XLINFO_TYPES: usize = 16;

/// `RM_MAX_ID` (access/rmgr.h) == `UINT8_MAX` (255), NOT `RM_MAX_BUILTIN_ID`.
/// Sizes `XLogStats.rmgr_stats`/`record_stats` to 256 rows (xlogstats.h)
/// so custom rmgr ids 128..=255 (`RM_MIN/MAX_CUSTOM_ID`) index in bounds.
pub const RM_MAX_ID: usize = u8::MAX as usize;

/// `RM_XACT_ID` — the Transaction resource manager (rmgrlist.h entry 1).
pub const RM_XACT_ID: RmgrId = 1;

/// The fixed-size WAL record header (`XLogRecord`, access/xlogrecord.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XLogRecord {
    xl_tot_len: uint32,
    xl_xid: TransactionId,
    xl_prev: XLogRecPtr,
    xl_info: uint8,
    xl_rmid: RmgrId,
    xl_crc: pg_crc32c,
}

impl XLogRecord {
    pub const fn new(
        xl_tot_len: uint32,
        xl_xid: TransactionId,
        xl_prev: XLogRecPtr,
        xl_info: uint8,
        xl_rmid: RmgrId,
        xl_crc: pg_crc32c,
    ) -> Self {
        Self {
            xl_tot_len,
            xl_xid,
            xl_prev,
            xl_info,
            xl_rmid,
            xl_crc,
        }
    }

    /// `XLogRecGetTotalLen` — `xl_tot_len`.
    pub const fn total_len(&self) -> uint32 {
        self.xl_tot_len
    }

    /// `XLogRecGetInfo` — `xl_info`.
    pub const fn info(&self) -> uint8 {
        self.xl_info
    }

    /// `XLogRecGetRmid` — `xl_rmid`.
    pub const fn rmid(&self) -> RmgrId {
        self.xl_rmid
    }
}

/// One decoded block reference of a WAL record (`DecodedBkpBlock`,
/// access/xlogreader.h). Trimmed to the fields current ports read; the
/// xlogreader port owns the full shape and will widen it. The block-data
/// borrow points into the reader's decode buffer (C `char *data`).
#[derive(Clone, Copy, Debug, Default)]
pub struct DecodedBkpBlock<'a> {
    in_use: bool,
    has_image: bool,
    apply_image: bool,
    bimg_len: uint16,
    /// `Some` iff the block carries block data (`has_data`); the slice is the
    /// `XLogRecGetBlockData` payload.
    data: Option<&'a [u8]>,
}

impl<'a> DecodedBkpBlock<'a> {
    pub const fn new(
        in_use: bool,
        has_image: bool,
        apply_image: bool,
        bimg_len: uint16,
        data: Option<&'a [u8]>,
    ) -> Self {
        Self {
            in_use,
            has_image,
            apply_image,
            bimg_len,
            data,
        }
    }

    /// The FPI bytes this block contributes: `bimg_len` when the block is in
    /// use and carries an image (`XLogRecHasBlockRef && XLogRecHasBlockImage`),
    /// else 0.
    pub const fn fpi_len(&self) -> uint32 {
        if self.in_use && self.has_image {
            self.bimg_len as uint32
        } else {
            0
        }
    }
}

/// A decoded WAL record (`DecodedXLogRecord`, access/xlogreader.h). Trimmed to
/// the header, the main data (`XLogRecGetData`), and the block references
/// `0..=max_block_id`. The block array is context-allocated (C pallocs the
/// decode buffer in the reader's context), so the record carries its
/// allocator lifetime; `main_data` and the per-block data borrow the decode
/// buffer with the same lifetime.
#[derive(Debug)]
pub struct DecodedXLogRecord<'mcx> {
    header: XLogRecord,
    /// `XLogRecGetData` — the record's main data.
    main_data: &'mcx [u8],
    blocks: PgVec<'mcx, DecodedBkpBlock<'mcx>>,
}

impl<'mcx> DecodedXLogRecord<'mcx> {
    /// `blocks` must hold the block references `0..=max_block_id` (in-use or
    /// not), mirroring the C array indexed by block id.
    pub const fn new(
        header: XLogRecord,
        main_data: &'mcx [u8],
        blocks: PgVec<'mcx, DecodedBkpBlock<'mcx>>,
    ) -> Self {
        Self {
            header,
            main_data,
            blocks,
        }
    }

    pub const fn header(&self) -> &XLogRecord {
        &self.header
    }

    pub fn blocks(&self) -> &[DecodedBkpBlock<'mcx>] {
        &self.blocks
    }

    /// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
    pub const fn info(&self) -> uint8 {
        self.header.info()
    }

    /// `XLogRecGetData(record)`.
    pub const fn data(&self) -> &'mcx [u8] {
        self.main_data
    }

    fn block(&self, block_id: usize) -> Option<&DecodedBkpBlock<'mcx>> {
        self.blocks.get(block_id).filter(|b| b.in_use)
    }

    /// `XLogRecHasBlockData(record, block_id)`.
    pub fn has_block_data(&self, block_id: usize) -> bool {
        self.block(block_id).is_some_and(|b| b.data.is_some())
    }

    /// `XLogRecGetBlockData(record, block_id, NULL)` — `None` where C returns
    /// a NULL pointer (block not in use, or no block data).
    pub fn block_data(&self, block_id: usize) -> Option<&'mcx [u8]> {
        self.block(block_id).and_then(|b| b.data)
    }

    /// `XLogRecHasBlockImage(record, block_id)`.
    pub fn has_block_image(&self, block_id: usize) -> bool {
        self.block(block_id).is_some_and(|b| b.has_image)
    }

    /// `XLogRecBlockImageApply(record, block_id)`.
    pub fn block_image_apply(&self, block_id: usize) -> bool {
        self.block(block_id).is_some_and(|b| b.apply_image)
    }
}

/// `XLR_INFO_MASK` (access/xlogrecord.h) — the bits of `xl_info` reserved for
/// the WAL machinery itself; the rmgr's record type lives in the high bits.
pub const XLR_INFO_MASK: uint8 = 0x0F;
