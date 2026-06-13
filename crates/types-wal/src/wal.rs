//! WAL record types and resource-manager constants.

use mcx::PgVec;
use types_core::{
    pg_crc32c, uint16, uint32, uint8, Oid, RelFileNumber, RmgrId, TransactionId, XLogRecPtr,
};

/// `WalLevel` enum (`access/xlog.h`): the level of information written to WAL.
/// Modeled as the C `int` GUC value; comparisons are `>=`.
pub const WAL_LEVEL_MINIMAL: i32 = 0;
pub const WAL_LEVEL_REPLICA: i32 = 1;
pub const WAL_LEVEL_LOGICAL: i32 = 2;

/// `XLR_INFO_MASK` (access/xlogrecord.h) — the low nibble of `xl_info` is
/// reserved for xlog-insertion flags; the rmgr's record opcode lives in the
/// high nibble (`info & ~XLR_INFO_MASK`).
pub const XLR_INFO_MASK: uint8 = 0x0F;

/// `MAX_XLINFO_TYPES` (access/xlogstats.h) — sixteen per-record buckets per
/// rmgr (the four xl_info bits in the rmgr's domain).
pub const MAX_XLINFO_TYPES: usize = 16;

/// `RM_MAX_ID` (access/rmgr.h) == `UINT8_MAX` (255), NOT `RM_MAX_BUILTIN_ID`.
/// Sizes `XLogStats.rmgr_stats`/`record_stats` to 256 rows (xlogstats.h)
/// so custom rmgr ids 128..=255 (`RM_MIN/MAX_CUSTOM_ID`) index in bounds.
pub const RM_MAX_ID: usize = u8::MAX as usize;

/// `RM_XACT_ID` — the Transaction resource manager (rmgrlist.h entry 1).
pub const RM_XACT_ID: RmgrId = 1;

/// `RelFileLocator` (storage/relfilelocator.h) — the physical identity of a
/// relation: tablespace, database, relfilenumber.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelFileLocator {
    spcOid: Oid,
    dbOid: Oid,
    relNumber: RelFileNumber,
}

impl RelFileLocator {
    pub const fn new(spcOid: Oid, dbOid: Oid, relNumber: RelFileNumber) -> Self {
        Self {
            spcOid,
            dbOid,
            relNumber,
        }
    }

    /// Bounds-checked read at the C `#[repr(C)]` offsets (three `Oid`s, no
    /// padding — the header requires that for hashtable keys): spcOid@0,
    /// dbOid@4, relNumber@8. `None` when fewer than 12 bytes are present.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        let word = |off: usize| -> Option<uint32> {
            Some(uint32::from_ne_bytes(
                data.get(off..off + 4)?.try_into().ok()?,
            ))
        };
        Some(Self {
            spcOid: word(0)?,
            dbOid: word(4)?,
            relNumber: word(8)?,
        })
    }

    pub const fn spc_oid(&self) -> Oid {
        self.spcOid
    }

    pub const fn db_oid(&self) -> Oid {
        self.dbOid
    }

    pub const fn rel_number(&self) -> RelFileNumber {
        self.relNumber
    }
}

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

    /// `XLogRecGetData` (with `XLogRecGetDataLen` == `.len()`) — alias of
    /// [`Self::data`] kept for the rmgrdesc-small ports.
    pub const fn main_data(&self) -> &'mcx [u8] {
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

