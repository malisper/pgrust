//! WAL record types and resource-manager constants.

use mcx::PgVec;
use types_core::{
    pg_crc32c, pg_time_t, uint16, uint32, uint8, BlockNumber, Buffer, ForkNumber, Oid,
    RelFileNumber, RepOriginId, RmgrId, TimeLineID, TransactionId, XLogRecPtr, MAXPGPATH,
};

// `WAL_LEVEL_MINIMAL`/`WAL_LEVEL_REPLICA`/`WAL_LEVEL_LOGICAL` are the canonical
// `WalLevel` enum aliases in `xlog_consts` (main's authoritative `wal_level`
// vocabulary, re-exported at the crate root). The earlier `int`-typed copies
// here were the slotsync branch's divergence and collided with them on merge.

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

/// `RM_XLOG_ID` — the XLOG resource manager (rmgrlist.h entry 0).
pub const RM_XLOG_ID: RmgrId = 0;

/// `RM_XACT_ID` — the Transaction resource manager (rmgrlist.h entry 1).
pub const RM_XACT_ID: RmgrId = 1;

/// `RM_SMGR_ID` — the Storage resource manager (rmgrlist.h entry 2).
pub const RM_SMGR_ID: RmgrId = 2;

/// `RM_CLOG_ID` — the CLOG resource manager (rmgrlist.h entry 3).
pub const RM_CLOG_ID: RmgrId = 3;

/// `CLOG_ZEROPAGE` (access/clog.h) — clog rmgr opcode for "a new page was
/// zeroed" (`info & ~XLR_INFO_MASK`).
pub const CLOG_ZEROPAGE: u8 = 0x00;

/// `CLOG_TRUNCATE` (access/clog.h) — clog rmgr opcode for "segments truncated".
pub const CLOG_TRUNCATE: u8 = 0x10;

/// `RM_DBASE_ID` — the Database resource manager (rmgrlist.h entry 4).
pub const RM_DBASE_ID: RmgrId = 4;

/// `RM_RELMAP_ID` — the RelMap resource manager (rmgrlist.h entry 7).
pub const RM_RELMAP_ID: RmgrId = 7;

/// `BKPBLOCK_WILL_INIT` (access/xlogrecord.h) — redo will re-init the page.
pub const BKPBLOCK_WILL_INIT: uint8 = 0x40;

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

    /// `XLogRecGetXid` — `xl_xid`.
    pub const fn xid(&self) -> TransactionId {
        self.xl_xid
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
    /// Identity of the block: `RelFileLocator rlocator; ForkNumber forknum;
    /// BlockNumber blkno` (the `XLogRecGetBlockTagExtended` triple).
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    /// `Buffer prefetch_buffer` — buffer the prefetcher found the block in
    /// (`InvalidBuffer` when none); read by `XLogReadBufferForRedoExtended`.
    prefetch_buffer: Buffer,
    /// `uint8 flags` — the `BKPBLOCK_*` header flag bits.
    flags: uint8,
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
            rlocator: RelFileLocator::new(0, 0, 0),
            forknum: ForkNumber::MAIN_FORKNUM,
            blkno: 0,
            prefetch_buffer: 0,
            flags: 0,
            has_image,
            apply_image,
            bimg_len,
            data,
        }
    }

    /// Set the block-reference identity fields (`rlocator`/`forknum`/`blkno`/
    /// `flags`), builder-style.
    pub const fn with_block_ref(
        mut self,
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blkno: BlockNumber,
        flags: uint8,
    ) -> Self {
        self.rlocator = rlocator;
        self.forknum = forknum;
        self.blkno = blkno;
        self.flags = flags;
        self
    }

    /// `block->in_use`.
    pub const fn in_use(&self) -> bool {
        self.in_use
    }

    /// `block->rlocator`.
    pub const fn rlocator(&self) -> RelFileLocator {
        self.rlocator
    }

    /// `block->forknum`.
    pub const fn forknum(&self) -> ForkNumber {
        self.forknum
    }

    /// `block->blkno`.
    pub const fn blkno(&self) -> BlockNumber {
        self.blkno
    }

    /// `block->flags` — the `BKPBLOCK_*` bits.
    pub const fn flags(&self) -> uint8 {
        self.flags
    }

    /// `block->has_image`.
    pub const fn has_image(&self) -> bool {
        self.has_image
    }

    /// `block->prefetch_buffer`.
    pub const fn prefetch_buffer(&self) -> Buffer {
        self.prefetch_buffer
    }

    /// `block->prefetch_buffer = buffer` — the prefetcher's write-back.
    pub fn set_prefetch_buffer(&mut self, buffer: Buffer) {
        self.prefetch_buffer = buffer;
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
    /// `DecodedXLogRecord.record_origin` — the replication origin decoded from
    /// the record (`XLogRecGetOrigin`); `InvalidRepOriginId` when none.
    record_origin: RepOriginId,
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
            record_origin: types_core::InvalidRepOriginId,
        }
    }

    /// Set `record_origin` (`XLogRecGetOrigin`), builder-style; the decoder
    /// fills it from the record's origin block-data when present.
    pub const fn with_origin(mut self, origin: RepOriginId) -> Self {
        self.record_origin = origin;
        self
    }

    pub const fn header(&self) -> &XLogRecord {
        &self.header
    }

    /// `XLogRecGetXid(record)` — `record->header.xl_xid`.
    pub const fn xid(&self) -> TransactionId {
        self.header.xid()
    }

    /// `XLogRecGetOrigin(record)` — `record->record_origin`.
    pub const fn record_origin(&self) -> RepOriginId {
        self.record_origin
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

    /// `XLogRecMaxBlockId(record)` — the highest block id in the record
    /// (`record->max_block_id`); `-1` when no blocks are registered. The block
    /// array is sized `0..=max_block_id`, so this is `blocks.len() - 1`.
    pub fn max_block_id(&self) -> i32 {
        self.blocks.len() as i32 - 1
    }

    /// `XLogRecHasBlockRef(record, block_id)` — whether the block id is in
    /// range and the entry is in use.
    pub fn has_block_ref(&self, block_id: usize) -> bool {
        self.block(block_id).is_some()
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

/// `RM_GENERIC_ID` — the Generic-WAL resource manager (rmgrlist.h entry 20).
pub const RM_GENERIC_ID: RmgrId = 20;

/// `XLOG_MARK_UNIMPORTANT` (access/xlog.h) — record flag: not important for
/// durability decisions (checkpoint / archive-timeout triggering).
pub const XLOG_MARK_UNIMPORTANT: uint8 = 0x02;

// `WalLevel` and `ArchiveMode` are the canonical enums in `xlog_consts` (main's
// single source, re-exported at the crate root); the launcher/walreceiver ports
// use those. No duplicate definition here.


/// `ReplicationSlotInvalidationCause` (replication/slot.h) — bitmask of
/// invalidation causes.
pub type ReplicationSlotInvalidationCause = u32;
pub const RS_INVAL_NONE: ReplicationSlotInvalidationCause = 0;
pub const RS_INVAL_WAL_REMOVED: ReplicationSlotInvalidationCause = 1 << 0;
pub const RS_INVAL_HORIZON: ReplicationSlotInvalidationCause = 1 << 1;
pub const RS_INVAL_WAL_LEVEL: ReplicationSlotInvalidationCause = 1 << 2;
pub const RS_INVAL_IDLE_TIMEOUT: ReplicationSlotInvalidationCause = 1 << 3;

/// One entry of a parsed timeline-history file (`TimeLineHistoryEntry`,
/// `access/timeline.h`): the LSN range over which `tli` was the current
/// timeline (`begin <= lsn < end`; `end == InvalidXLogRecPtr` for the latest).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    pub begin: XLogRecPtr,
    pub end: XLogRecPtr,
}

impl TimeLineHistoryEntry {
    pub const fn new(tli: TimeLineID, begin: XLogRecPtr, end: XLogRecPtr) -> Self {
        Self { tli, begin, end }
    }
}

/// `XLR_SPECIAL_REL_UPDATE` (`access/xlogrecord.h`) — flag bit in `xl_info`:
/// the record modifies relation files outside the buffer manager's view.
pub const XLR_SPECIAL_REL_UPDATE: uint8 = 0x01;

/// `XLOG_INCLUDE_ORIGIN` (`access/xloginsert.h`) — record flag: include the
/// replication origin in the record.
pub const XLOG_INCLUDE_ORIGIN: uint8 = 0x01;

/// The header facts of the record most recently decoded by `XLogReadAhead`
/// (access/xlogreader.c), copied out for the WAL prefetcher: in C the
/// prefetcher holds the `DecodedXLogRecord *` itself; the record lives in the
/// reader's decode queue, so the cross-cycle seam hands back these `Copy`
/// projections of it (`lsn`, `header.xl_rmid`, `header.xl_info`,
/// `max_block_id`) and the block references are re-read through the reader.
#[derive(Clone, Copy, Debug)]
pub struct ReadAheadRecordInfo {
    /// `record->lsn` — the record's start LSN.
    pub lsn: XLogRecPtr,
    /// `record->header.xl_rmid`.
    pub xl_rmid: RmgrId,
    /// `record->header.xl_info`.
    pub xl_info: uint8,
    /// `record->max_block_id` — highest block_id in use (-1 if none).
    pub max_block_id: i32,
}

/// The outcome of `XLogNextRecord(reader, &errmsg)` (access/xlogreader.c):
/// the C function returns the next `DecodedXLogRecord *` off the decode queue
/// (becoming `reader->record`, readable there), or NULL with `*errmsg`
/// pointing into the reader's `errormsg_buf`.
#[derive(Debug)]
pub enum XLogNextRecordResult<'a> {
    /// A record was returned; `lsn` is `record->lsn` (the record itself is
    /// the reader's current record).
    Record {
        /// `record->lsn`.
        lsn: XLogRecPtr,
    },
    /// NULL — no record ready; `errmsg` is the deferred error text, if any.
    NoRecord {
        /// `*errmsg` (borrowed from the reader's `errormsg_buf`).
        errmsg: Option<&'a str>,
    },
}

/// `BackupState` (access/xlogbackup.h:20-38) — the snapshot of base-backup
/// metadata recorded at backup start and end, used to render the
/// `backup_label` / backup history file contents.
///
/// `name` holds the backup label as server-encoding bytes (the C
/// `char[MAXPGPATH + 1]`), NUL-padded and never UTF-8-validated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackupState {
    /* Fields saved at backup start */
    name: [u8; MAXPGPATH + 1],
    startpoint: XLogRecPtr,
    starttli: TimeLineID,
    checkpointloc: XLogRecPtr,
    starttime: pg_time_t,
    started_in_recovery: bool,
    istartpoint: XLogRecPtr,
    istarttli: TimeLineID,
    /* Fields saved at the end of backup */
    stoppoint: XLogRecPtr,
    stoptli: TimeLineID,
    stoptime: pg_time_t,
}

impl BackupState {
    /// Construct a [`BackupState`] from its fields, mirroring the C struct
    /// layout (access/xlogbackup.h:21-37).
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        name: [u8; MAXPGPATH + 1],
        startpoint: XLogRecPtr,
        starttli: TimeLineID,
        checkpointloc: XLogRecPtr,
        starttime: pg_time_t,
        started_in_recovery: bool,
        istartpoint: XLogRecPtr,
        istarttli: TimeLineID,
        stoppoint: XLogRecPtr,
        stoptli: TimeLineID,
        stoptime: pg_time_t,
    ) -> Self {
        Self {
            name,
            startpoint,
            starttli,
            checkpointloc,
            starttime,
            started_in_recovery,
            istartpoint,
            istarttli,
            stoppoint,
            stoptli,
            stoptime,
        }
    }

    /// `state->name` — the backup label bytes (server encoding, NUL-padded).
    pub const fn name(&self) -> &[u8; MAXPGPATH + 1] {
        &self.name
    }

    /// `state->startpoint` — backup start WAL location.
    pub const fn startpoint(&self) -> XLogRecPtr {
        self.startpoint
    }

    /// `state->starttli` — backup start TLI.
    pub const fn starttli(&self) -> TimeLineID {
        self.starttli
    }

    /// `state->checkpointloc` — last checkpoint location.
    pub const fn checkpointloc(&self) -> XLogRecPtr {
        self.checkpointloc
    }

    /// `state->starttime` — backup start time.
    pub const fn starttime(&self) -> pg_time_t {
        self.starttime
    }

    /// `state->started_in_recovery` — whether the backup started in recovery.
    pub const fn started_in_recovery(&self) -> bool {
        self.started_in_recovery
    }

    /// `state->istartpoint` — incremental-based-on backup LSN.
    pub const fn istartpoint(&self) -> XLogRecPtr {
        self.istartpoint
    }

    /// `state->istarttli` — incremental-based-on backup TLI.
    pub const fn istarttli(&self) -> TimeLineID {
        self.istarttli
    }

    /// Set `state->istartpoint` (written by `PrepareForIncrementalBackup`).
    pub fn set_istartpoint(&mut self, istartpoint: XLogRecPtr) {
        self.istartpoint = istartpoint;
    }

    /// Set `state->istarttli` (written by `PrepareForIncrementalBackup`).
    pub fn set_istarttli(&mut self, istarttli: TimeLineID) {
        self.istarttli = istarttli;
    }

    /// `state->stoppoint` — backup stop WAL location.
    pub const fn stoppoint(&self) -> XLogRecPtr {
        self.stoppoint
    }

    /// `state->stoptli` — backup stop TLI.
    pub const fn stoptli(&self) -> TimeLineID {
        self.stoptli
    }

    /// `state->stoptime` — backup stop time.
    pub const fn stoptime(&self) -> pg_time_t {
        self.stoptime
    }
}
