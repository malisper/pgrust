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
/// xlogreader port owns the full shape and will widen it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DecodedBkpBlock {
    in_use: bool,
    has_image: bool,
    bimg_len: uint16,
}

impl DecodedBkpBlock {
    pub const fn new(in_use: bool, has_image: bool, bimg_len: uint16) -> Self {
        Self {
            in_use,
            has_image,
            bimg_len,
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
/// the header plus the block references `0..=max_block_id`. The block array
/// is context-allocated (C pallocs the decode buffer in the reader's
/// context), so the record carries its allocator lifetime.
#[derive(Debug)]
pub struct DecodedXLogRecord<'mcx> {
    header: XLogRecord,
    blocks: PgVec<'mcx, DecodedBkpBlock>,
}

impl<'mcx> DecodedXLogRecord<'mcx> {
    /// `blocks` must hold the block references `0..=max_block_id` (in-use or
    /// not), mirroring the C array indexed by block id.
    pub const fn new(header: XLogRecord, blocks: PgVec<'mcx, DecodedBkpBlock>) -> Self {
        Self { header, blocks }
    }

    pub const fn header(&self) -> &XLogRecord {
        &self.header
    }

    pub fn blocks(&self) -> &[DecodedBkpBlock] {
        &self.blocks
    }
}

/// `XLR_INFO_MASK` (access/xlogrecord.h) — the bits of `xl_info` reserved for
/// the WAL machinery itself; the rmgr's record type lives in the high bits.
pub const XLR_INFO_MASK: uint8 = 0x0F;

/// One block reference of a record as the rm_desc routines see it: the
/// `XLogRecHasBlockImage` / `XLogRecBlockImageApply` / `XLogRecHasBlockData` /
/// `XLogRecGetBlockData` facet of `XLogReaderState`'s per-block decode state.
#[derive(Clone, Copy, Debug, Default)]
pub struct XLogRecordBlockView<'a> {
    in_use: bool,
    has_image: bool,
    apply_image: bool,
    /// `Some` iff the block carries block data (`has_data`); the slice is the
    /// `XLogRecGetBlockData` payload.
    data: Option<&'a [u8]>,
}

impl<'a> XLogRecordBlockView<'a> {
    pub const fn new(
        in_use: bool,
        has_image: bool,
        apply_image: bool,
        data: Option<&'a [u8]>,
    ) -> Self {
        Self { in_use, has_image, apply_image, data }
    }
}

/// Borrowed view of a decoded WAL record, trimmed to the accessors the
/// rm_desc/rm_identify routines consume: `XLogRecGetInfo`, `XLogRecGetData`,
/// and the per-block-reference queries. The owning reader holds the decoded
/// bytes; this view only borrows them.
#[derive(Clone, Copy, Debug)]
pub struct XLogRecordView<'a> {
    info: uint8,
    /// `XLogRecGetData` — the record's main data.
    main_data: &'a [u8],
    /// Block references indexed by block id (`0..=max_block_id`).
    blocks: &'a [XLogRecordBlockView<'a>],
}

impl<'a> XLogRecordView<'a> {
    pub const fn new(
        info: uint8,
        main_data: &'a [u8],
        blocks: &'a [XLogRecordBlockView<'a>],
    ) -> Self {
        Self { info, main_data, blocks }
    }

    /// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
    pub const fn info(&self) -> uint8 {
        self.info
    }

    /// `XLogRecGetData(record)`.
    pub const fn data(&self) -> &'a [u8] {
        self.main_data
    }

    fn block(&self, block_id: usize) -> Option<&XLogRecordBlockView<'a>> {
        self.blocks.get(block_id).filter(|b| b.in_use)
    }

    /// `XLogRecHasBlockData(record, block_id)`.
    pub fn has_block_data(&self, block_id: usize) -> bool {
        self.block(block_id).is_some_and(|b| b.data.is_some())
    }

    /// `XLogRecGetBlockData(record, block_id, NULL)` — `None` where C returns
    /// a NULL pointer (block not in use, or no block data).
    pub fn block_data(&self, block_id: usize) -> Option<&'a [u8]> {
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
