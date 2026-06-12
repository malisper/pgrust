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

/// The trimmed `XLogReaderState` view handed to an rmgr `rm_redo` entry
/// point: `XLogRecGetInfo(record)`, `XLogRecGetData(record)` (with
/// `XLogRecGetDataLen` folded into the slice), and
/// `XLogRecHasAnyBlockRefs(record)`. All rmgr redo seams share this shape;
/// the dispatcher marshals it from the decoded record.
#[derive(Clone, Copy, Debug)]
pub struct RedoRecord<'a> {
    /// The raw `xl_info` byte (rmgr bits plus `XLR_INFO_MASK` bits).
    pub info: uint8,
    /// The record's main data.
    pub data: &'a [u8],
    /// Whether any block references are present.
    pub has_any_block_refs: bool,
}

/// `RM_STANDBY_ID` — the Standby resource manager (rmgrlist.h entry 8).
pub const RM_STANDBY_ID: RmgrId = 8;

/// `XLR_INFO_MASK` (access/xlogrecord.h) — the low info bits reserved for
/// xlog-internal use; rmgr-specific info lives above it.
pub const XLR_INFO_MASK: uint8 = 0x0F;

/// `XLOG_MARK_UNIMPORTANT` (access/xlog.h) — record flag: not important for
/// durability decisions (checkpoint / archive-timeout triggering).
pub const XLOG_MARK_UNIMPORTANT: uint8 = 0x02;

/// `WalLevel` (access/xlog.h) — the `wal_level` GUC.
pub type WalLevel = i32;
pub const WAL_LEVEL_MINIMAL: WalLevel = 0;
pub const WAL_LEVEL_REPLICA: WalLevel = 1;
pub const WAL_LEVEL_LOGICAL: WalLevel = 2;

/// `HotStandbyState` (access/xlogutils.h) — the hot-standby state machine
/// driven by the startup process (`standbyState` global, owned by the
/// xlogutils unit).
pub type HotStandbyState = u32;
pub const STANDBY_DISABLED: HotStandbyState = 0;
pub const STANDBY_INITIALIZED: HotStandbyState = 1;
pub const STANDBY_SNAPSHOT_PENDING: HotStandbyState = 2;
pub const STANDBY_SNAPSHOT_READY: HotStandbyState = 3;

/// `ReplicationSlotInvalidationCause` (replication/slot.h) — bitmask of
/// invalidation causes.
pub type ReplicationSlotInvalidationCause = u32;
pub const RS_INVAL_NONE: ReplicationSlotInvalidationCause = 0;
pub const RS_INVAL_WAL_REMOVED: ReplicationSlotInvalidationCause = 1 << 0;
pub const RS_INVAL_HORIZON: ReplicationSlotInvalidationCause = 1 << 1;
pub const RS_INVAL_WAL_LEVEL: ReplicationSlotInvalidationCause = 1 << 2;
pub const RS_INVAL_IDLE_TIMEOUT: ReplicationSlotInvalidationCause = 1 << 3;
