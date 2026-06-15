use core::ffi::{c_char, c_int, c_void};
use core::ptr::NonNull;

use crate::types::{
    pg_crc32c, pg_time_t, uint16, uint32, uint64, uint8, BlockNumber, Buffer, ForkNumber, Oid,
    RelFileNumber, RepOriginId, RmgrId, Size, TimeLineID, TimestampTz, TransactionId, XLogRecPtr,
    XLogSegNo, MAXPGPATH,
};

pub const MAX_XLINFO_TYPES: usize = 16;
/// `RM_MAX_ID` (access/rmgr.h:33) == `UINT8_MAX` (255), NOT `RM_MAX_BUILTIN_ID`.
/// Sizes `XLogStats.rmgr_stats`/`record_stats` to 256 rows (xlogstats.h:35-36)
/// so custom rmgr ids 128..=255 (`RM_MIN/MAX_CUSTOM_ID`) index in bounds.
pub const RM_MAX_ID: usize = u8::MAX as usize;

// --- Resource manager IDs (access/rmgrlist.h) ---
// `RM_HEAP_ID`/`RM_HEAP2_ID` live in `heap.rs`; the others used by the WAL
// crates are defined here. Values match the `PG_RMGR` entry order 0..21.
/// `RM_XLOG_ID` -- the XLOG resource manager (rmgrlist.h entry 0).
pub const RM_XLOG_ID: RmgrId = 0;
pub const RM_XACT_ID: RmgrId = 1;
/// `RM_TBLSPC_ID` -- the Tablespace resource manager (rmgrlist.h entry 5).
pub const RM_TBLSPC_ID: RmgrId = 5;
/// `RM_GENERIC_ID` -- the Generic WAL resource manager (rmgrlist.h entry 20).
pub const RM_GENERIC_ID: RmgrId = 20;
/// `RM_LOGICALMSG_ID` -- the LogicalMessage resource manager (entry 21).
pub const RM_LOGICALMSG_ID: RmgrId = 21;

// --- Resource manager ID bounds + validity (access/rmgr.h) ---
/// `RM_NEXT_ID` -- one past the last builtin rmgr id (22 builtins: 0..=21).
pub const RM_NEXT_ID: RmgrId = 22;
/// `RM_MAX_BUILTIN_ID` == `RM_NEXT_ID - 1`.
pub const RM_MAX_BUILTIN_ID: RmgrId = RM_NEXT_ID - 1;
/// `RM_MIN_CUSTOM_ID` -- first custom (extension) rmgr id.
pub const RM_MIN_CUSTOM_ID: RmgrId = 128;
/// `RM_MAX_CUSTOM_ID` == `UINT8_MAX`.
pub const RM_MAX_CUSTOM_ID: RmgrId = u8::MAX;
/// `RM_EXPERIMENTAL_ID` -- custom rmgr id for in-development extensions.
pub const RM_EXPERIMENTAL_ID: RmgrId = 128;

/// `RmgrIdIsBuiltin` (access/rmgr.h).
#[inline]
pub const fn RmgrIdIsBuiltin(rmid: c_int) -> bool {
    rmid <= RM_MAX_BUILTIN_ID as c_int
}

/// `RmgrIdIsCustom` (access/rmgr.h).
#[inline]
pub const fn RmgrIdIsCustom(rmid: c_int) -> bool {
    rmid >= RM_MIN_CUSTOM_ID as c_int && rmid <= RM_MAX_CUSTOM_ID as c_int
}

/// `RmgrIdIsValid` (access/rmgr.h).
#[inline]
pub const fn RmgrIdIsValid(rmid: c_int) -> bool {
    RmgrIdIsBuiltin(rmid) || RmgrIdIsCustom(rmid)
}

// --- XLOG (RM_XLOG_ID) record opcodes (catalog/pg_control.h) ---
/// `XLOG_SWITCH` -- the XLOG SWITCH record's info byte.
pub const XLOG_SWITCH: u8 = 0x40;
/// `XLOG_FPI_FOR_HINT` -- full-page image written for a hint-bit update.
pub const XLOG_FPI_FOR_HINT: u8 = 0xA0;
/// `XLOG_FPI` -- a standalone full-page image record.
pub const XLOG_FPI: u8 = 0xB0;
pub const DEFAULT_XLOG_SEG_SIZE: c_int = 16 * 1024 * 1024;
pub const XLOG_BLCKSZ: usize = 8192;
pub const XLOG_PAGE_MAGIC: uint16 = 0xD118;
pub const XLP_FIRST_IS_CONTRECORD: uint16 = 0x0001;
pub const XLP_LONG_HEADER: uint16 = 0x0002;
pub const XLP_BKP_REMOVABLE: uint16 = 0x0004;
pub const XLP_FIRST_IS_OVERWRITE_CONTRECORD: uint16 = 0x0008;
pub const XLP_ALL_FLAGS: uint16 = 0x000F;
pub const WAL_SEG_MIN_SIZE: c_int = 1024 * 1024;
pub const WAL_SEG_MAX_SIZE: c_int = 1024 * 1024 * 1024;
pub const DEFAULT_MIN_WAL_SEGS: c_int = 5;
pub const DEFAULT_MAX_WAL_SEGS: c_int = 64;
pub const XLOG_FNAME_LEN: usize = 24;
pub const MAXFNAMELEN: usize = 64;
pub const XLOGDIR: &str = "pg_wal";
pub const XLOG_CONTROL_FILE: &str = "global/pg_control";
pub const SIZE_OF_XLOG_SHORT_PHD: usize = core::mem::size_of::<XLogPageHeaderData>();
pub const SIZE_OF_XLOG_LONG_PHD: usize = core::mem::size_of::<XLogLongPageHeaderData>();

pub type WalSyncMethod = c_int;
pub const WAL_SYNC_METHOD_FSYNC: WalSyncMethod = 0;
pub const WAL_SYNC_METHOD_FDATASYNC: WalSyncMethod = 1;
pub const WAL_SYNC_METHOD_OPEN: WalSyncMethod = 2;
pub const WAL_SYNC_METHOD_FSYNC_WRITETHROUGH: WalSyncMethod = 3;
pub const WAL_SYNC_METHOD_OPEN_DSYNC: WalSyncMethod = 4;

pub type ArchiveMode = c_int;
pub const ARCHIVE_MODE_OFF: ArchiveMode = 0;
pub const ARCHIVE_MODE_ON: ArchiveMode = 1;
pub const ARCHIVE_MODE_ALWAYS: ArchiveMode = 2;

pub type WalLevel = c_int;
pub const WAL_LEVEL_MINIMAL: WalLevel = 0;
pub const WAL_LEVEL_REPLICA: WalLevel = 1;
pub const WAL_LEVEL_LOGICAL: WalLevel = 2;

pub type WalCompression = c_int;
pub const WAL_COMPRESSION_NONE: WalCompression = 0;
pub const WAL_COMPRESSION_PGLZ: WalCompression = 1;
pub const WAL_COMPRESSION_LZ4: WalCompression = 2;
pub const WAL_COMPRESSION_ZSTD: WalCompression = 3;

pub type RecoveryState = c_int;
pub const RECOVERY_STATE_CRASH: RecoveryState = 0;
pub const RECOVERY_STATE_ARCHIVE: RecoveryState = 1;
pub const RECOVERY_STATE_DONE: RecoveryState = 2;

pub type WALAvailability = c_int;
pub const WALAVAIL_INVALID_LSN: WALAvailability = 0;
pub const WALAVAIL_RESERVED: WALAvailability = 1;
pub const WALAVAIL_EXTENDED: WALAvailability = 2;
pub const WALAVAIL_UNRESERVED: WALAvailability = 3;
pub const WALAVAIL_REMOVED: WALAvailability = 4;

pub const CHECKPOINT_IS_SHUTDOWN: c_int = 0x0001;
pub const CHECKPOINT_END_OF_RECOVERY: c_int = 0x0002;
pub const CHECKPOINT_IMMEDIATE: c_int = 0x0004;
pub const CHECKPOINT_FORCE: c_int = 0x0008;
pub const CHECKPOINT_FLUSH_ALL: c_int = 0x0010;
pub const CHECKPOINT_WAIT: c_int = 0x0020;
pub const CHECKPOINT_REQUESTED: c_int = 0x0040;
pub const CHECKPOINT_CAUSE_XLOG: c_int = 0x0080;
pub const CHECKPOINT_CAUSE_TIME: c_int = 0x0100;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalSummaryIO {
    file: crate::File,
    filepos: i64,
}

impl WalSummaryIO {
    pub const fn new(file: crate::File, filepos: i64) -> Self {
        Self { file, filepos }
    }

    pub const fn file(&self) -> crate::File {
        self.file
    }

    pub const fn filepos(&self) -> i64 {
        self.filepos
    }

    pub fn advance(&mut self, nbytes: usize) {
        self.filepos = self.filepos.saturating_add(nbytes as i64);
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalSummaryFile {
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    tli: TimeLineID,
}

impl WalSummaryFile {
    pub const fn new(tli: TimeLineID, start_lsn: XLogRecPtr, end_lsn: XLogRecPtr) -> Self {
        Self {
            start_lsn,
            end_lsn,
            tli,
        }
    }

    pub const fn start_lsn(&self) -> XLogRecPtr {
        self.start_lsn
    }

    pub const fn end_lsn(&self) -> XLogRecPtr {
        self.end_lsn
    }

    pub const fn tli(&self) -> TimeLineID {
        self.tli
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackupState {
    name: [c_char; MAXPGPATH + 1],
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
}

impl BackupState {
    pub const fn new(
        name: [c_char; MAXPGPATH + 1],
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

    pub const fn name(&self) -> &[c_char; MAXPGPATH + 1] {
        &self.name
    }

    pub const fn startpoint(&self) -> XLogRecPtr {
        self.startpoint
    }

    pub const fn starttli(&self) -> TimeLineID {
        self.starttli
    }

    pub const fn checkpointloc(&self) -> XLogRecPtr {
        self.checkpointloc
    }

    pub const fn starttime(&self) -> pg_time_t {
        self.starttime
    }

    pub const fn started_in_recovery(&self) -> bool {
        self.started_in_recovery
    }

    pub const fn istartpoint(&self) -> XLogRecPtr {
        self.istartpoint
    }

    pub const fn istarttli(&self) -> TimeLineID {
        self.istarttli
    }

    /// Set `backup_state->istartpoint` (written by `PrepareForIncrementalBackup`).
    pub fn set_istartpoint(&mut self, istartpoint: XLogRecPtr) {
        self.istartpoint = istartpoint;
    }

    /// Set `backup_state->istarttli` (written by `PrepareForIncrementalBackup`).
    pub fn set_istarttli(&mut self, istarttli: TimeLineID) {
        self.istarttli = istarttli;
    }

    pub const fn stoppoint(&self) -> XLogRecPtr {
        self.stoppoint
    }

    pub const fn stoptli(&self) -> TimeLineID {
        self.stoptli
    }

    pub const fn stoptime(&self) -> pg_time_t {
        self.stoptime
    }
}

pub const XLOG_INCLUDE_ORIGIN: uint8 = 0x01;
pub const XLOG_MARK_UNIMPORTANT: uint8 = 0x02;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogPageHeaderData {
    xlp_magic: uint16,
    xlp_info: uint16,
    xlp_tli: TimeLineID,
    xlp_pageaddr: XLogRecPtr,
    xlp_rem_len: uint32,
}

impl XLogPageHeaderData {
    pub const fn new(
        xlp_magic: uint16,
        xlp_info: uint16,
        xlp_tli: TimeLineID,
        xlp_pageaddr: XLogRecPtr,
        xlp_rem_len: uint32,
    ) -> Self {
        Self {
            xlp_magic,
            xlp_info,
            xlp_tli,
            xlp_pageaddr,
            xlp_rem_len,
        }
    }

    pub const fn magic(&self) -> uint16 {
        self.xlp_magic
    }

    pub const fn info(&self) -> uint16 {
        self.xlp_info
    }

    pub const fn timeline_id(&self) -> TimeLineID {
        self.xlp_tli
    }

    pub const fn pageaddr(&self) -> XLogRecPtr {
        self.xlp_pageaddr
    }

    pub const fn rem_len(&self) -> uint32 {
        self.xlp_rem_len
    }

    pub const fn has_long_header(&self) -> bool {
        self.xlp_info & XLP_LONG_HEADER != 0
    }

    pub const fn page_header_size(&self) -> usize {
        if self.has_long_header() {
            SIZE_OF_XLOG_LONG_PHD
        } else {
            SIZE_OF_XLOG_SHORT_PHD
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogLongPageHeaderData {
    std: XLogPageHeaderData,
    xlp_sysid: uint64,
    xlp_seg_size: uint32,
    xlp_xlog_blcksz: uint32,
}

impl XLogLongPageHeaderData {
    pub const fn new(
        std: XLogPageHeaderData,
        xlp_sysid: uint64,
        xlp_seg_size: uint32,
        xlp_xlog_blcksz: uint32,
    ) -> Self {
        Self {
            std,
            xlp_sysid,
            xlp_seg_size,
            xlp_xlog_blcksz,
        }
    }

    pub const fn standard_header(&self) -> &XLogPageHeaderData {
        &self.std
    }

    pub const fn system_identifier(&self) -> uint64 {
        self.xlp_sysid
    }

    pub const fn segment_size(&self) -> uint32 {
        self.xlp_seg_size
    }

    pub const fn xlog_block_size(&self) -> uint32 {
        self.xlp_xlog_blcksz
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_parameter_change {
    MaxConnections: c_int,
    max_worker_processes: c_int,
    max_wal_senders: c_int,
    max_prepared_xacts: c_int,
    max_locks_per_xact: c_int,
    wal_level: c_int,
    wal_log_hints: bool,
    track_commit_timestamp: bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_restore_point {
    rp_time: TimestampTz,
    rp_name: [c_char; MAXFNAMELEN],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_overwrite_contrecord {
    overwritten_lsn: XLogRecPtr,
    overwrite_time: TimestampTz,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_end_of_recovery {
    end_time: TimestampTz,
    ThisTimeLineID: TimeLineID,
    PrevTimeLineID: TimeLineID,
    wal_level: c_int,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XLogRecData {
    next: *mut XLogRecData,
    data: *const c_void,
    len: uint32,
}

impl XLogRecData {
    pub const fn new(next: *mut XLogRecData, data: *const c_void, len: uint32) -> Self {
        Self { next, data, len }
    }

    /// `rdata->next` -- the next link in the intrusive `XLogRecData` chain.
    pub const fn next(&self) -> *mut XLogRecData {
        self.next
    }

    /// `rdata->data` -- the payload pointer.
    pub const fn data(&self) -> *const c_void {
        self.data
    }

    /// `rdata->len` -- the payload length.
    pub const fn len(&self) -> uint32 {
        self.len
    }

    /// Whether the payload length is zero (mirrors `rdata->len == 0`).
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Set `rdata->next` when building/walking the chain in `XLogRecordAssemble`.
    pub fn set_next(&mut self, next: *mut XLogRecData) {
        self.next = next;
    }

    /// Set `rdata->data`.
    pub fn set_data(&mut self, data: *const c_void) {
        self.data = data;
    }

    /// Set `rdata->len`.
    pub fn set_len(&mut self, len: uint32) {
        self.len = len;
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CheckpointStatsData {
    ckpt_start_t: TimestampTz,
    ckpt_write_t: TimestampTz,
    ckpt_sync_t: TimestampTz,
    ckpt_sync_end_t: TimestampTz,
    ckpt_end_t: TimestampTz,
    ckpt_bufs_written: c_int,
    ckpt_slru_written: c_int,
    ckpt_segs_added: c_int,
    ckpt_segs_removed: c_int,
    ckpt_segs_recycled: c_int,
    ckpt_sync_rels: c_int,
    ckpt_longest_sync: uint64,
    ckpt_agg_sync_time: uint64,
}

#[repr(C)]
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

#[repr(C)]
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

    pub const fn total_len(&self) -> uint32 {
        self.xl_tot_len
    }

    pub const fn info(&self) -> uint8 {
        self.xl_info
    }

    pub const fn rmid(&self) -> RmgrId {
        self.xl_rmid
    }
}

/// `SizeOfXLogRecord` -- size of the fixed-size record header (through xl_crc).
pub const SIZE_OF_XLOG_RECORD: usize = core::mem::size_of::<XLogRecord>();

/*
 * The high 4 bits in xl_info may be used freely by rmgr. The
 * XLR_SPECIAL_REL_UPDATE and XLR_CHECK_CONSISTENCY bits can be passed by
 * XLogInsert caller. The rest are set internally by XLogInsert.
 *
 * `XLR_INFO_MASK` is re-exported from `rmgrdesc` (same value 0x0F).
 */
pub const XLR_RMGR_INFO_MASK: uint8 = 0xF0;

/// Maximum allowed length of a single WAL record (see xlogrecord.h).
pub const XLOG_RECORD_MAX_SIZE: usize = 1020 * 1024 * 1024;

pub const XLR_SPECIAL_REL_UPDATE: uint8 = 0x01;
pub const XLR_CHECK_CONSISTENCY: uint8 = 0x02;

/// Header info for block data appended to an XLOG record.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogRecordBlockHeader {
    pub id: uint8,
    pub fork_flags: uint8,
    pub data_length: uint16,
}

impl XLogRecordBlockHeader {
    pub const fn new(id: uint8, fork_flags: uint8, data_length: uint16) -> Self {
        Self {
            id,
            fork_flags,
            data_length,
        }
    }
}

/// `SizeOfXLogRecordBlockHeader` (offsetof(.., data_length) + sizeof(uint16)).
pub const SIZE_OF_XLOG_RECORD_BLOCK_HEADER: usize = core::mem::size_of::<XLogRecordBlockHeader>();

/// Additional header information when a full-page image is included.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogRecordBlockImageHeader {
    pub length: uint16,
    pub hole_offset: uint16,
    pub bimg_info: uint8,
}

impl XLogRecordBlockImageHeader {
    pub const fn new(length: uint16, hole_offset: uint16, bimg_info: uint8) -> Self {
        Self {
            length,
            hole_offset,
            bimg_info,
        }
    }
}

/// `SizeOfXLogRecordBlockImageHeader` (offsetof(.., bimg_info) + sizeof(uint8)).
pub const SIZE_OF_XLOG_RECORD_BLOCK_IMAGE_HEADER: usize =
    core::mem::offset_of!(XLogRecordBlockImageHeader, bimg_info) + core::mem::size_of::<uint8>();

/* Information stored in bimg_info */
pub const BKPIMAGE_HAS_HOLE: uint8 = 0x01;
pub const BKPIMAGE_APPLY: uint8 = 0x02;
pub const BKPIMAGE_COMPRESS_PGLZ: uint8 = 0x04;
pub const BKPIMAGE_COMPRESS_LZ4: uint8 = 0x08;
pub const BKPIMAGE_COMPRESS_ZSTD: uint8 = 0x10;

/// `BKPIMAGE_COMPRESSED(info)` macro: any of the compression bits set.
pub const fn bkpimage_compressed(info: uint8) -> bool {
    (info & (BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_COMPRESS_LZ4 | BKPIMAGE_COMPRESS_ZSTD)) != 0
}

/// Extra header information used when page image has "hole" and is compressed.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogRecordBlockCompressHeader {
    pub hole_length: uint16,
}

impl XLogRecordBlockCompressHeader {
    pub const fn new(hole_length: uint16) -> Self {
        Self { hole_length }
    }
}

/// `SizeOfXLogRecordBlockCompressHeader`.
pub const SIZE_OF_XLOG_RECORD_BLOCK_COMPRESS_HEADER: usize =
    core::mem::size_of::<XLogRecordBlockCompressHeader>();

/// `MaxSizeOfXLogRecordBlockHeader` -- temporary-buffer size for constructing a
/// block reference header.
pub const MAX_SIZE_OF_XLOG_RECORD_BLOCK_HEADER: usize = SIZE_OF_XLOG_RECORD_BLOCK_HEADER
    + SIZE_OF_XLOG_RECORD_BLOCK_IMAGE_HEADER
    + SIZE_OF_XLOG_RECORD_BLOCK_COMPRESS_HEADER
    + core::mem::size_of::<RelFileLocator>()
    + core::mem::size_of::<BlockNumber>();

/* The fork number fits in the lower 4 bits of fork_flags; upper bits are flags. */
pub const BKPBLOCK_FORK_MASK: uint8 = 0x0F;
pub const BKPBLOCK_FLAG_MASK: uint8 = 0xF0;
pub const BKPBLOCK_HAS_IMAGE: uint8 = 0x10;
pub const BKPBLOCK_HAS_DATA: uint8 = 0x20;
pub const BKPBLOCK_WILL_INIT: uint8 = 0x40;
pub const BKPBLOCK_SAME_REL: uint8 = 0x80;

/// Main-data short header (`XLogRecordDataHeaderShort`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogRecordDataHeaderShort {
    pub id: uint8,
    pub data_length: uint8,
}

/// `SizeOfXLogRecordDataHeaderShort`.
pub const SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT: usize = core::mem::size_of::<uint8>() * 2;

/// Main-data long header (`XLogRecordDataHeaderLong`); followed by an unaligned
/// uint32 data_length.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XLogRecordDataHeaderLong {
    pub id: uint8,
}

/// `SizeOfXLogRecordDataHeaderLong` (uint8 id + unaligned uint32 length).
pub const SIZE_OF_XLOG_RECORD_DATA_HEADER_LONG: usize =
    core::mem::size_of::<uint8>() + core::mem::size_of::<uint32>();

/* Block IDs used to distinguish different kinds of record fragments. */
pub const XLR_MAX_BLOCK_ID: c_int = 32;

pub const XLR_BLOCK_ID_DATA_SHORT: uint8 = 255;
pub const XLR_BLOCK_ID_DATA_LONG: uint8 = 254;
pub const XLR_BLOCK_ID_ORIGIN: uint8 = 253;
pub const XLR_BLOCK_ID_TOPLEVEL_XID: uint8 = 252;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WALOpenSegment {
    ws_file: c_int,
    ws_segno: XLogSegNo,
    ws_tli: TimeLineID,
}

impl WALOpenSegment {
    pub const fn new(ws_file: c_int, ws_segno: XLogSegNo, ws_tli: TimeLineID) -> Self {
        Self {
            ws_file,
            ws_segno,
            ws_tli,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WALSegmentContext {
    ws_dir: [c_char; 1024],
    ws_segsize: c_int,
}

impl Default for WALSegmentContext {
    fn default() -> Self {
        Self {
            ws_dir: [0; 1024],
            ws_segsize: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct XLogReaderRoutine {
    page_read: XLogPageReadCB,
    segment_open: WALSegmentOpenCB,
    segment_close: WALSegmentCloseCB,
}

impl XLogReaderRoutine {
    pub const fn empty() -> Self {
        Self {
            page_read: None,
            segment_open: None,
            segment_close: None,
        }
    }
}

pub type WALSegmentCloseCB = Option<unsafe extern "C" fn(*mut XLogReaderState)>;
pub type WALSegmentOpenCB =
    Option<unsafe extern "C" fn(*mut XLogReaderState, XLogSegNo, *mut TimeLineID)>;
pub type XLogPageReadCB = Option<
    unsafe extern "C" fn(*mut XLogReaderState, XLogRecPtr, c_int, XLogRecPtr, *mut c_char) -> c_int,
>;

#[repr(C)]
pub struct XLogReaderState {
    routine: XLogReaderRoutine,
    system_identifier: uint64,
    private_data: *mut c_void,
    ReadRecPtr: XLogRecPtr,
    EndRecPtr: XLogRecPtr,
    abortedRecPtr: XLogRecPtr,
    missingContrecPtr: XLogRecPtr,
    overwrittenRecPtr: XLogRecPtr,
    DecodeRecPtr: XLogRecPtr,
    NextRecPtr: XLogRecPtr,
    PrevRecPtr: XLogRecPtr,
    record: *mut DecodedXLogRecord,
    decode_buffer: *mut c_char,
    decode_buffer_size: Size,
    free_decode_buffer: bool,
    decode_buffer_head: *mut c_char,
    decode_buffer_tail: *mut c_char,
    decode_queue_head: *mut DecodedXLogRecord,
    decode_queue_tail: *mut DecodedXLogRecord,
    readBuf: *mut c_char,
    readLen: uint32,
    segcxt: WALSegmentContext,
    seg: WALOpenSegment,
    segoff: uint32,
    latestPagePtr: XLogRecPtr,
    latestPageTLI: TimeLineID,
    currRecPtr: XLogRecPtr,
    currTLI: TimeLineID,
    currTLIValidUntil: XLogRecPtr,
    nextTLI: TimeLineID,
    readRecordBuf: *mut c_char,
    readRecordBufSize: uint32,
    errormsg_buf: *mut c_char,
    errormsg_deferred: bool,
    nonblocking: bool,
}

impl XLogReaderState {
    pub fn with_decoded_record(record: &mut DecodedXLogRecord) -> Self {
        Self {
            routine: XLogReaderRoutine::empty(),
            system_identifier: 0,
            private_data: core::ptr::null_mut(),
            ReadRecPtr: 0,
            EndRecPtr: 0,
            abortedRecPtr: 0,
            missingContrecPtr: 0,
            overwrittenRecPtr: 0,
            DecodeRecPtr: 0,
            NextRecPtr: 0,
            PrevRecPtr: 0,
            record,
            decode_buffer: core::ptr::null_mut(),
            decode_buffer_size: 0,
            free_decode_buffer: false,
            decode_buffer_head: core::ptr::null_mut(),
            decode_buffer_tail: core::ptr::null_mut(),
            decode_queue_head: core::ptr::null_mut(),
            decode_queue_tail: core::ptr::null_mut(),
            readBuf: core::ptr::null_mut(),
            readLen: 0,
            segcxt: WALSegmentContext::default(),
            seg: WALOpenSegment::new(0, 0, 0),
            segoff: 0,
            latestPagePtr: 0,
            latestPageTLI: 0,
            currRecPtr: 0,
            currTLI: 0,
            currTLIValidUntil: 0,
            nextTLI: 0,
            readRecordBuf: core::ptr::null_mut(),
            readRecordBufSize: 0,
            errormsg_buf: core::ptr::null_mut(),
            errormsg_deferred: false,
            nonblocking: false,
        }
    }

    pub fn decoded_record(&self) -> Option<&DecodedXLogRecord> {
        NonNull::new(self.record).map(|record| unsafe { record.as_ref() })
    }

    // -- Field accessors used by the WAL-recovery layer (xlogrecovery.c). --

    /// `xlogreader->private_data` (the `XLogPageReadPrivate *` passed down to the
    /// page_read callback).
    pub fn private_data(&self) -> *mut c_void {
        self.private_data
    }

    /// `xlogreader->ReadRecPtr` -- start of last record read.
    pub fn read_rec_ptr(&self) -> XLogRecPtr {
        self.ReadRecPtr
    }

    /// `xlogreader->EndRecPtr` -- end+1 of last record read.
    pub fn end_rec_ptr(&self) -> XLogRecPtr {
        self.EndRecPtr
    }

    /// `xlogreader->abortedRecPtr` -- start of a broken record at end of WAL.
    pub fn aborted_rec_ptr(&self) -> XLogRecPtr {
        self.abortedRecPtr
    }

    /// `xlogreader->missingContrecPtr` -- first missing contrecord location.
    pub fn missing_contrec_ptr(&self) -> XLogRecPtr {
        self.missingContrecPtr
    }

    /// `xlogreader->latestPagePtr` -- LSN of the most recently read page.
    pub fn latest_page_ptr(&self) -> XLogRecPtr {
        self.latestPagePtr
    }

    /// `xlogreader->latestPageTLI` -- TLI of the most recently read page.
    pub fn latest_page_tli(&self) -> TimeLineID {
        self.latestPageTLI
    }

    /// `xlogreader->seg.ws_tli` -- TLI of the currently open segment.
    pub fn seg_ws_tli(&self) -> TimeLineID {
        self.seg.ws_tli
    }

    /// Set `xlogreader->seg.ws_tli`.
    pub fn set_seg_ws_tli(&mut self, tli: TimeLineID) {
        self.seg.ws_tli = tli;
    }

    /// `xlogreader->errormsg_buf` -- the error message scratch buffer.
    pub fn errormsg_buf(&self) -> *mut c_char {
        self.errormsg_buf
    }

    /// `xlogreader->nonblocking` -- set while prefetching ahead of replay.
    pub fn nonblocking(&self) -> bool {
        self.nonblocking
    }
}

#[repr(C)]
pub struct DecodedXLogRecord {
    size: Size,
    oversized: bool,
    next: *mut DecodedXLogRecord,
    lsn: XLogRecPtr,
    next_lsn: XLogRecPtr,
    header: XLogRecord,
    record_origin: RepOriginId,
    toplevel_xid: TransactionId,
    main_data: *mut c_char,
    main_data_len: uint32,
    max_block_id: c_int,
    blocks: [DecodedBkpBlock; 0],
}

impl DecodedXLogRecord {
    pub fn new(header: XLogRecord, block_count: usize) -> Self {
        let max_block_id = block_count.checked_sub(1).map_or(-1, |id| id as c_int);
        Self {
            size: core::mem::size_of::<Self>()
                + block_count * core::mem::size_of::<DecodedBkpBlock>(),
            oversized: false,
            next: core::ptr::null_mut(),
            lsn: 0,
            next_lsn: 0,
            header,
            record_origin: 0,
            toplevel_xid: 0,
            main_data: core::ptr::null_mut(),
            main_data_len: 0,
            max_block_id,
            blocks: [],
        }
    }

    pub const fn header(&self) -> &XLogRecord {
        &self.header
    }

    pub fn main_data(&self) -> &[u8] {
        if self.main_data.is_null() || self.main_data_len == 0 {
            return &[];
        }
        unsafe {
            core::slice::from_raw_parts(self.main_data.cast::<u8>(), self.main_data_len as usize)
        }
    }

    /// Set the decoded record's main-data pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure `data` is valid for `len` bytes for as long as
    /// this decoded record can be read.
    pub unsafe fn set_main_data(&mut self, data: *mut c_char, len: uint32) {
        self.main_data = data;
        self.main_data_len = len;
    }

    pub fn blocks(&self) -> &[DecodedBkpBlock] {
        if self.max_block_id < 0 {
            return &[];
        }
        let len = self.max_block_id as usize + 1;
        unsafe { core::slice::from_raw_parts(self.blocks.as_ptr(), len) }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodedBkpBlock {
    in_use: bool,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    prefetch_buffer: Buffer,
    flags: uint8,
    has_image: bool,
    apply_image: bool,
    bkp_image: *mut c_char,
    hole_offset: uint16,
    hole_length: uint16,
    bimg_len: uint16,
    bimg_info: uint8,
    has_data: bool,
    data: *mut c_char,
    data_len: uint16,
    data_bufsz: uint16,
}

impl DecodedBkpBlock {
    pub const fn new(in_use: bool, has_image: bool, bimg_len: uint16) -> Self {
        Self {
            in_use,
            rlocator: RelFileLocator::new(0, 0, 0),
            forknum: 0,
            blkno: 0,
            prefetch_buffer: 0,
            flags: 0,
            has_image,
            apply_image: false,
            bkp_image: core::ptr::null_mut(),
            hole_offset: 0,
            hole_length: 0,
            bimg_len,
            bimg_info: 0,
            has_data: false,
            data: core::ptr::null_mut(),
            data_len: 0,
            data_bufsz: 0,
        }
    }

    pub const fn fpi_len(&self) -> uint32 {
        if self.in_use && self.has_image {
            self.bimg_len as uint32
        } else {
            0
        }
    }
}

/* xloginsert.h working-area sizes and XLogRegisterBuffer flags. */
pub const XLR_NORMAL_MAX_BLOCK_ID: c_int = 4;
pub const XLR_NORMAL_RDATAS: c_int = 20;

pub const REGBUF_FORCE_IMAGE: uint8 = 0x01;
pub const REGBUF_NO_IMAGE: uint8 = 0x02;
pub const REGBUF_WILL_INIT: uint8 = 0x04 | 0x02;
pub const REGBUF_STANDARD: uint8 = 0x08;
pub const REGBUF_KEEP_DATA: uint8 = 0x10;
pub const REGBUF_NO_CHANGE: uint8 = 0x20;

/// Buffer size required to store a compressed version of a backup block image.
/// `COMPRESS_BUFSIZE = Max(PGLZ_MAX_BLCKSZ, LZ4_MAX_BLCKSZ, ZSTD_MAX_BLCKSZ)`.
/// `PGLZ_MAX_BLCKSZ = PGLZ_MAX_OUTPUT(BLCKSZ) = BLCKSZ + 4`, which dominates the
/// LZ4/ZSTD compress-bounds for BLCKSZ=8192.
pub const COMPRESS_BUFSIZE: usize = crate::types::BLCKSZ + 4;

/// `registered_buffer` (file-static in xloginsert.c): the working slot filled in
/// for each block reference registered with `XLogRegisterBuffer`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RegisteredBuffer {
    pub in_use: bool,
    pub flags: uint8,
    pub rlocator: RelFileLocator,
    pub forkno: ForkNumber,
    pub block: BlockNumber,
    pub page: *const c_char,
    pub rdata_len: uint32,
    pub rdata_head: *mut XLogRecData,
    pub rdata_tail: *mut XLogRecData,
    pub bkp_rdatas: [XLogRecData; 2],
    pub compressed_page: [c_char; COMPRESS_BUFSIZE],
}

impl Default for RegisteredBuffer {
    fn default() -> Self {
        Self {
            in_use: false,
            flags: 0,
            rlocator: RelFileLocator::new(0, 0, 0),
            forkno: 0,
            block: 0,
            page: core::ptr::null(),
            rdata_len: 0,
            rdata_head: core::ptr::null_mut(),
            rdata_tail: core::ptr::null_mut(),
            bkp_rdatas: [XLogRecData::new(core::ptr::null_mut(), core::ptr::null(), 0); 2],
            compressed_page: [0; COMPRESS_BUFSIZE],
        }
    }
}

/// Convenience alias matching the C `registered_buffer` type name.
pub type registered_buffer = RegisteredBuffer;

/* generic_xlog.c delta sizing. */
pub const FRAGMENT_HEADER_SIZE: usize = 2 * core::mem::size_of::<crate::types::OffsetNumber>();
pub const MATCH_THRESHOLD: usize = FRAGMENT_HEADER_SIZE;
pub const MAX_DELTA_SIZE: usize = crate::types::BLCKSZ + 2 * FRAGMENT_HEADER_SIZE;
pub const MAX_GENERIC_XLOG_PAGES: usize = XLR_NORMAL_MAX_BLOCK_ID as usize;
pub const GENERIC_XLOG_FULL_IMAGE: c_int = 0x0001;

/// `PGIOAlignedBlock` -- a BLCKSZ-sized buffer aligned to PG_IO_ALIGN_SIZE
/// (4096). Used for the page images held by `GenericXLogState`.
#[repr(C, align(4096))]
#[derive(Clone, Copy)]
pub struct PGIOAlignedBlock {
    pub data: [c_char; crate::types::BLCKSZ],
}

impl Default for PGIOAlignedBlock {
    fn default() -> Self {
        Self {
            data: [0; crate::types::BLCKSZ],
        }
    }
}

/// `GenericXLogPageData` -- per-page state inside `GenericXLogState`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GenericXLogPageData {
    pub buffer: Buffer,
    pub flags: c_int,
    pub delta_len: c_int,
    pub image: *mut c_char,
    pub delta: [c_char; MAX_DELTA_SIZE],
}

impl Default for GenericXLogPageData {
    fn default() -> Self {
        Self {
            buffer: 0,
            flags: 0,
            delta_len: 0,
            image: core::ptr::null_mut(),
            delta: [0; MAX_DELTA_SIZE],
        }
    }
}

/// `GenericXLogState` -- state of generic xlog record construction. Must be
/// allocated at an I/O-aligned address; `images` is first and aligned.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GenericXLogState {
    pub images: [PGIOAlignedBlock; MAX_GENERIC_XLOG_PAGES],
    pub pages: [GenericXLogPageData; MAX_GENERIC_XLOG_PAGES],
    pub is_logged: bool,
}

impl Default for GenericXLogState {
    fn default() -> Self {
        Self {
            images: [PGIOAlignedBlock::default(); MAX_GENERIC_XLOG_PAGES],
            pages: [GenericXLogPageData::default(); MAX_GENERIC_XLOG_PAGES],
            is_logged: false,
        }
    }
}

/// `TimeLineHistoryEntry` -- one piece of WAL belonging to the timeline history.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn xlog_page_header_layout_matches_postgres() {
        assert_eq!(size_of::<XLogPageHeaderData>(), 24);
        assert_eq!(align_of::<XLogPageHeaderData>(), 8);
        assert_eq!(offset_of!(XLogPageHeaderData, xlp_magic), 0);
        assert_eq!(offset_of!(XLogPageHeaderData, xlp_info), 2);
        assert_eq!(offset_of!(XLogPageHeaderData, xlp_tli), 4);
        assert_eq!(offset_of!(XLogPageHeaderData, xlp_pageaddr), 8);
        assert_eq!(offset_of!(XLogPageHeaderData, xlp_rem_len), 16);

        assert_eq!(size_of::<XLogLongPageHeaderData>(), 40);
        assert_eq!(align_of::<XLogLongPageHeaderData>(), 8);
        assert_eq!(offset_of!(XLogLongPageHeaderData, std), 0);
        assert_eq!(offset_of!(XLogLongPageHeaderData, xlp_sysid), 24);
        assert_eq!(offset_of!(XLogLongPageHeaderData, xlp_seg_size), 32);
        assert_eq!(offset_of!(XLogLongPageHeaderData, xlp_xlog_blcksz), 36);
    }

    #[test]
    fn xlog_record_block_headers_match_postgres() {
        // SizeOfXLogRecord = offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c)
        assert_eq!(SIZE_OF_XLOG_RECORD, 24);

        // SizeOfXLogRecordBlockHeader = offsetof(.., data_length) + sizeof(uint16) = 4
        assert_eq!(SIZE_OF_XLOG_RECORD_BLOCK_HEADER, 4);
        assert_eq!(offset_of!(XLogRecordBlockHeader, id), 0);
        assert_eq!(offset_of!(XLogRecordBlockHeader, fork_flags), 1);
        assert_eq!(offset_of!(XLogRecordBlockHeader, data_length), 2);

        // SizeOfXLogRecordBlockImageHeader = offsetof(.., bimg_info) + sizeof(uint8) = 5
        assert_eq!(SIZE_OF_XLOG_RECORD_BLOCK_IMAGE_HEADER, 5);
        assert_eq!(offset_of!(XLogRecordBlockImageHeader, length), 0);
        assert_eq!(offset_of!(XLogRecordBlockImageHeader, hole_offset), 2);
        assert_eq!(offset_of!(XLogRecordBlockImageHeader, bimg_info), 4);

        assert_eq!(SIZE_OF_XLOG_RECORD_BLOCK_COMPRESS_HEADER, 2);
        assert_eq!(SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT, 2);
        assert_eq!(SIZE_OF_XLOG_RECORD_DATA_HEADER_LONG, 5);
    }

    #[test]
    fn timeline_history_entry_layout_matches_postgres() {
        assert_eq!(size_of::<TimeLineHistoryEntry>(), 24);
        assert_eq!(offset_of!(TimeLineHistoryEntry, tli), 0);
        assert_eq!(offset_of!(TimeLineHistoryEntry, begin), 8);
        assert_eq!(offset_of!(TimeLineHistoryEntry, end), 16);
    }
}
