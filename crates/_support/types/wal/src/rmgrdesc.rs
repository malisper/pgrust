//! Typed WAL record payloads read by the rmgr descriptor routines, one struct
//! per C `xl_*` record (access/clog.h, access/commit_ts.h,
//! commands/dbcommands_xlog.h, replication/message.h, utils/relmapper.h,
//! commands/sequence.h, commands/tablespace.h). Where C casts the raw record
//! data to a struct pointer, these parse with a bounds-checked `from_bytes`
//! (native-endian, the C `#[repr(C)]` offsets) so the layout knowledge lives
//! here once. `from_bytes` returns `None` when the payload is too short —
//! impossible for well-formed WAL (the C would read garbage); callers raise
//! their data-corruption error.

use crate::wal::RelFileLocator;
use types_core::{
    int64, MultiXactId, MultiXactOffset, Oid, RepOriginId, TimeLineID, TimestampTz, TransactionId,
    XLogRecPtr,
};

fn read_u16(data: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_ne_bytes(
        data.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_ne_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_u64(data: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_ne_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

/// Read a C `bool` (one byte; nonzero == true), the layout `xl_*` records use.
fn read_bool(data: &[u8], offset: usize) -> Option<bool> {
    Some(*data.get(offset)? != 0)
}

fn read_i32(data: &[u8], offset: usize) -> Option<i32> {
    Some(i32::from_ne_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn read_i64(data: &[u8], offset: usize) -> Option<i64> {
    Some(i64::from_ne_bytes(
        data.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

/// Read a C `Size` (native `size_t`).
fn read_size(data: &[u8], offset: usize) -> Option<usize> {
    const N: usize = core::mem::size_of::<usize>();
    Some(usize::from_ne_bytes(
        data.get(offset..offset + N)?.try_into().ok()?,
    ))
}

/// `xl_clog_truncate` (access/clog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_clog_truncate {
    pageno: int64,
    oldestXact: TransactionId,
    oldestXactDb: Oid,
}

impl xl_clog_truncate {
    /// The C `memcpy(&xlrec, rec, sizeof(xl_clog_truncate))`: pageno@0,
    /// oldestXact@8, oldestXactDb@12.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            pageno: read_i64(data, 0)?,
            oldestXact: read_u32(data, 8)?,
            oldestXactDb: read_u32(data, 12)?,
        })
    }

    pub const fn pageno(&self) -> int64 {
        self.pageno
    }

    pub const fn oldest_xact(&self) -> TransactionId {
        self.oldestXact
    }

    pub const fn oldest_xact_db(&self) -> Oid {
        self.oldestXactDb
    }
}

/// `xl_commit_ts_truncate` (access/commit_ts.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_commit_ts_truncate {
    pageno: int64,
    oldestXid: TransactionId,
}

impl xl_commit_ts_truncate {
    /// pageno@0, oldestXid@8; the record is `SizeOfCommitTsTruncate` (12)
    /// bytes — the struct's trailing padding is not written to WAL.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            pageno: read_i64(data, 0)?,
            oldestXid: read_u32(data, 8)?,
        })
    }

    pub const fn pageno(&self) -> int64 {
        self.pageno
    }

    pub const fn oldest_xid(&self) -> TransactionId {
        self.oldestXid
    }
}

/// `xl_smgr_create` (catalog/storage_xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_smgr_create {
    rlocator: RelFileLocator,
    forkNum: ::types_core::ForkNumber,
}

impl xl_smgr_create {
    /// rlocator@0 (three `Oid`s), forkNum@12 (a C `int`).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            rlocator: RelFileLocator::new(
                read_u32(data, 0)?,
                read_u32(data, 4)?,
                read_u32(data, 8)?,
            ),
            forkNum: ::types_core::ForkNumber::from_i32(read_i32(data, 12)?)?,
        })
    }

    pub const fn rlocator(&self) -> RelFileLocator {
        self.rlocator
    }

    pub const fn fork_num(&self) -> ::types_core::ForkNumber {
        self.forkNum
    }
}

/// `xl_smgr_truncate` (catalog/storage_xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_smgr_truncate {
    blkno: ::types_core::BlockNumber,
    rlocator: RelFileLocator,
    flags: i32,
}

impl xl_smgr_truncate {
    /// blkno@0, rlocator@4 (three `Oid`s), flags@16.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            blkno: read_u32(data, 0)?,
            rlocator: RelFileLocator::new(
                read_u32(data, 4)?,
                read_u32(data, 8)?,
                read_u32(data, 12)?,
            ),
            flags: read_i32(data, 16)?,
        })
    }

    pub const fn blkno(&self) -> ::types_core::BlockNumber {
        self.blkno
    }

    pub const fn rlocator(&self) -> RelFileLocator {
        self.rlocator
    }

    /// The `SMGR_TRUNCATE_*` flag bits.
    pub const fn flags(&self) -> i32 {
        self.flags
    }
}

/// `xl_dbase_create_file_copy_rec` (commands/dbcommands_xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_dbase_create_file_copy_rec {
    db_id: Oid,
    tablespace_id: Oid,
    src_db_id: Oid,
    src_tablespace_id: Oid,
}

impl xl_dbase_create_file_copy_rec {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            db_id: read_u32(data, 0)?,
            tablespace_id: read_u32(data, 4)?,
            src_db_id: read_u32(data, 8)?,
            src_tablespace_id: read_u32(data, 12)?,
        })
    }

    pub const fn db_id(&self) -> Oid {
        self.db_id
    }

    pub const fn tablespace_id(&self) -> Oid {
        self.tablespace_id
    }

    pub const fn src_db_id(&self) -> Oid {
        self.src_db_id
    }

    pub const fn src_tablespace_id(&self) -> Oid {
        self.src_tablespace_id
    }
}

/// `xl_dbase_create_wal_log_rec` (commands/dbcommands_xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_dbase_create_wal_log_rec {
    db_id: Oid,
    tablespace_id: Oid,
}

impl xl_dbase_create_wal_log_rec {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            db_id: read_u32(data, 0)?,
            tablespace_id: read_u32(data, 4)?,
        })
    }

    pub const fn db_id(&self) -> Oid {
        self.db_id
    }

    pub const fn tablespace_id(&self) -> Oid {
        self.tablespace_id
    }
}

/// `xl_dbase_drop_rec` (commands/dbcommands_xlog.h) — fixed header plus the
/// flexible `Oid tablespace_ids[ntablespaces]` array, kept as a borrow of the
/// record payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_dbase_drop_rec<'a> {
    db_id: Oid,
    ntablespaces: i32,
    tablespace_ids: &'a [u8],
}

impl<'a> xl_dbase_drop_rec<'a> {
    /// db_id@0, ntablespaces@4 (`MinSizeOfDbaseDropRec` == 8),
    /// tablespace_ids@8. Requires all `ntablespaces` ids to be present.
    pub fn from_bytes(data: &'a [u8]) -> Option<Self> {
        let db_id = read_u32(data, 0)?;
        let ntablespaces = read_i32(data, 4)?;
        // C's `for (i = 0; i < ntablespaces; i++)` reads nothing when the
        // count is negative.
        let count = usize::try_from(ntablespaces).unwrap_or(0);
        let tablespace_ids = data.get(8..8usize.checked_add(count.checked_mul(4)?)?)?;
        Some(Self {
            db_id,
            ntablespaces,
            tablespace_ids,
        })
    }

    pub const fn db_id(&self) -> Oid {
        self.db_id
    }

    pub const fn ntablespaces(&self) -> i32 {
        self.ntablespaces
    }

    /// The `tablespace_ids[0..ntablespaces]` array.
    pub fn tablespace_ids(&self) -> impl Iterator<Item = Oid> + '_ {
        self.tablespace_ids
            .chunks_exact(4)
            .map(|b| Oid::from_ne_bytes(b.try_into().expect("chunks_exact yields 4 bytes")))
    }
}

/// `xl_logical_message` (replication/message.h) — fixed header plus the
/// flexible `message` array (NUL-terminated prefix of `prefix_size` bytes,
/// then `message_size` payload bytes), kept as a borrow of the record payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_logical_message<'a> {
    dbId: Oid,
    transactional: bool,
    prefix_size: usize,
    message_size: usize,
    message: &'a [u8],
}

impl<'a> xl_logical_message<'a> {
    /// `#[repr(C)]` offsets: dbId@0, transactional@4, then padding to `Size`
    /// alignment: prefix_size@8, message_size@8+sizeof(Size),
    /// message@`SizeOfLogicalMessage` (8+2*sizeof(Size)). Requires the
    /// prefix and payload bytes to be present.
    pub fn from_bytes(data: &'a [u8]) -> Option<Self> {
        const SIZE: usize = core::mem::size_of::<usize>();
        let prefix_size = read_size(data, 8)?;
        let message_size = read_size(data, 8 + SIZE)?;
        let message_off = 8 + 2 * SIZE;
        let total = prefix_size.checked_add(message_size)?;
        let message = data.get(message_off..message_off.checked_add(total)?)?;
        Some(Self {
            dbId: read_u32(data, 0)?,
            transactional: *data.get(4)? != 0,
            prefix_size,
            message_size,
            message,
        })
    }

    pub const fn db_id(&self) -> Oid {
        self.dbId
    }

    pub const fn transactional(&self) -> bool {
        self.transactional
    }

    pub const fn prefix_size(&self) -> usize {
        self.prefix_size
    }

    pub const fn message_size(&self) -> usize {
        self.message_size
    }

    /// `xlrec->message`: the NUL-terminated prefix (`prefix_size` bytes
    /// including the NUL).
    pub fn prefix(&self) -> &'a [u8] {
        &self.message[..self.prefix_size]
    }

    /// `xlrec->message + xlrec->prefix_size`: the `message_size` payload
    /// bytes.
    pub fn payload(&self) -> &'a [u8] {
        &self.message[self.prefix_size..]
    }
}

/// `xl_relmap_update` (utils/relmapper.h), trimmed of the flexible map-image
/// `data` array no port reads yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_relmap_update {
    dbid: Oid,
    tsid: Oid,
    nbytes: i32,
}

impl xl_relmap_update {
    /// dbid@0, tsid@4, nbytes@8 (`MinSizeOfRelmapUpdate` == 12).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            dbid: read_u32(data, 0)?,
            tsid: read_u32(data, 4)?,
            nbytes: read_i32(data, 8)?,
        })
    }

    pub const fn dbid(&self) -> Oid {
        self.dbid
    }

    pub const fn tsid(&self) -> Oid {
        self.tsid
    }

    pub const fn nbytes(&self) -> i32 {
        self.nbytes
    }
}

/// `xl_seq_rec` (commands/sequence.h); the sequence tuple data that follows
/// the locator is not read by any port yet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_seq_rec {
    locator: RelFileLocator,
}

impl xl_seq_rec {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            locator: RelFileLocator::from_bytes(data)?,
        })
    }

    pub const fn locator(&self) -> &RelFileLocator {
        &self.locator
    }
}

/// `xl_tblspc_create_rec` (commands/tablespace.h) — `ts_id` plus the flexible
/// NUL-terminated `ts_path`, kept as a borrow of the record payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_tblspc_create_rec<'a> {
    ts_id: Oid,
    ts_path: &'a [u8],
}

impl<'a> xl_tblspc_create_rec<'a> {
    /// ts_id@0, ts_path@4. Requires the NUL terminator the C's `%s` relies
    /// on; `None` when it is missing rather than a wild read.
    pub fn from_bytes(data: &'a [u8]) -> Option<Self> {
        let ts_id = read_u32(data, 0)?;
        let path_bytes = data.get(4..)?;
        let nul = path_bytes.iter().position(|&b| b == 0)?;
        Some(Self {
            ts_id,
            ts_path: &path_bytes[..nul],
        })
    }

    pub const fn ts_id(&self) -> Oid {
        self.ts_id
    }

    /// `ts_path` up to (excluding) its NUL terminator.
    pub const fn ts_path(&self) -> &'a [u8] {
        self.ts_path
    }
}

/// `xl_tblspc_drop_rec` (commands/tablespace.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_tblspc_drop_rec {
    ts_id: Oid,
}

impl xl_tblspc_drop_rec {
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            ts_id: read_u32(data, 0)?,
        })
    }

    pub const fn ts_id(&self) -> Oid {
        self.ts_id
    }
}

/// `CheckPoint` (catalog/pg_control.h) — the checkpoint record body shared by
/// `XLOG_CHECKPOINT_SHUTDOWN` / `XLOG_CHECKPOINT_ONLINE`. `sizeof == 88`; the
/// `time` field (a `pg_time_t`) is not rendered by `xlog_desc`, so it is not
/// exposed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckPoint {
    redo: XLogRecPtr,
    ThisTimeLineID: TimeLineID,
    PrevTimeLineID: TimeLineID,
    fullPageWrites: bool,
    wal_level: i32,
    /// `FullTransactionId` (a `uint64` value).
    nextXid: u64,
    nextOid: Oid,
    nextMulti: MultiXactId,
    nextMultiOffset: MultiXactOffset,
    oldestXid: TransactionId,
    oldestXidDB: Oid,
    oldestMulti: MultiXactId,
    oldestMultiDB: Oid,
    oldestCommitTsXid: TransactionId,
    newestCommitTsXid: TransactionId,
    oldestActiveXid: TransactionId,
}

impl CheckPoint {
    /// redo@0, ThisTimeLineID@8, PrevTimeLineID@12, fullPageWrites@16,
    /// wal_level@20, nextXid@24 (u64), nextOid@32, nextMulti@36,
    /// nextMultiOffset@40, oldestXid@44, oldestXidDB@48, oldestMulti@52,
    /// oldestMultiDB@56, time@64 (skipped), oldestCommitTsXid@72,
    /// newestCommitTsXid@76, oldestActiveXid@80.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            redo: read_u64(data, 0)?,
            ThisTimeLineID: read_u32(data, 8)?,
            PrevTimeLineID: read_u32(data, 12)?,
            fullPageWrites: read_bool(data, 16)?,
            wal_level: read_i32(data, 20)?,
            nextXid: read_u64(data, 24)?,
            nextOid: read_u32(data, 32)?,
            nextMulti: read_u32(data, 36)?,
            nextMultiOffset: read_u32(data, 40)?,
            oldestXid: read_u32(data, 44)?,
            oldestXidDB: read_u32(data, 48)?,
            oldestMulti: read_u32(data, 52)?,
            oldestMultiDB: read_u32(data, 56)?,
            oldestCommitTsXid: read_u32(data, 72)?,
            newestCommitTsXid: read_u32(data, 76)?,
            oldestActiveXid: read_u32(data, 80)?,
        })
    }

    pub const fn redo(&self) -> XLogRecPtr {
        self.redo
    }
    pub const fn this_timeline_id(&self) -> TimeLineID {
        self.ThisTimeLineID
    }
    pub const fn prev_timeline_id(&self) -> TimeLineID {
        self.PrevTimeLineID
    }
    pub const fn full_page_writes(&self) -> bool {
        self.fullPageWrites
    }
    pub const fn wal_level(&self) -> i32 {
        self.wal_level
    }
    /// The `FullTransactionId` value (`.value`).
    pub const fn next_xid(&self) -> u64 {
        self.nextXid
    }
    pub const fn next_oid(&self) -> Oid {
        self.nextOid
    }
    pub const fn next_multi(&self) -> MultiXactId {
        self.nextMulti
    }
    pub const fn next_multi_offset(&self) -> MultiXactOffset {
        self.nextMultiOffset
    }
    pub const fn oldest_xid(&self) -> TransactionId {
        self.oldestXid
    }
    pub const fn oldest_xid_db(&self) -> Oid {
        self.oldestXidDB
    }
    pub const fn oldest_multi(&self) -> MultiXactId {
        self.oldestMulti
    }
    pub const fn oldest_multi_db(&self) -> Oid {
        self.oldestMultiDB
    }
    pub const fn oldest_commit_ts_xid(&self) -> TransactionId {
        self.oldestCommitTsXid
    }
    pub const fn newest_commit_ts_xid(&self) -> TransactionId {
        self.newestCommitTsXid
    }
    pub const fn oldest_active_xid(&self) -> TransactionId {
        self.oldestActiveXid
    }
}

/// `xl_restore_point` (access/xlog_internal.h) — `rp_time` plus the fixed
/// `char rp_name[MAXFNAMELEN]`. `rp_name` starts at offset 8 (after the
/// `TimestampTz`); `xlog_desc` only renders `rp_name`, kept as a borrow of the
/// NUL-terminated bytes (the C `%s` relies on the terminator).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_restore_point<'a> {
    rp_time: TimestampTz,
    rp_name: &'a [u8],
}

impl<'a> xl_restore_point<'a> {
    /// rp_time@0, rp_name@8 (`char[64]`, NUL-terminated).
    pub fn from_bytes(data: &'a [u8]) -> Option<Self> {
        let rp_time = read_i64(data, 0)?;
        let name_bytes = data.get(8..)?;
        let nul = name_bytes.iter().position(|&b| b == 0)?;
        Some(Self {
            rp_time,
            rp_name: &name_bytes[..nul],
        })
    }

    pub const fn rp_time(&self) -> TimestampTz {
        self.rp_time
    }

    /// `rp_name` up to (excluding) its NUL terminator.
    pub const fn rp_name(&self) -> &'a [u8] {
        self.rp_name
    }
}

/// `xl_parameter_change` (access/xlog_internal.h). `sizeof == 28`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_parameter_change {
    MaxConnections: i32,
    max_worker_processes: i32,
    max_wal_senders: i32,
    max_prepared_xacts: i32,
    max_locks_per_xact: i32,
    wal_level: i32,
    wal_log_hints: bool,
    track_commit_timestamp: bool,
}

impl xl_parameter_change {
    /// MaxConnections@0, max_worker_processes@4, max_wal_senders@8,
    /// max_prepared_xacts@12, max_locks_per_xact@16, wal_level@20,
    /// wal_log_hints@24, track_commit_timestamp@25.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            MaxConnections: read_i32(data, 0)?,
            max_worker_processes: read_i32(data, 4)?,
            max_wal_senders: read_i32(data, 8)?,
            max_prepared_xacts: read_i32(data, 12)?,
            max_locks_per_xact: read_i32(data, 16)?,
            wal_level: read_i32(data, 20)?,
            wal_log_hints: read_bool(data, 24)?,
            track_commit_timestamp: read_bool(data, 25)?,
        })
    }

    pub const fn max_connections(&self) -> i32 {
        self.MaxConnections
    }
    pub const fn max_worker_processes(&self) -> i32 {
        self.max_worker_processes
    }
    pub const fn max_wal_senders(&self) -> i32 {
        self.max_wal_senders
    }
    pub const fn max_prepared_xacts(&self) -> i32 {
        self.max_prepared_xacts
    }
    pub const fn max_locks_per_xact(&self) -> i32 {
        self.max_locks_per_xact
    }
    pub const fn wal_level(&self) -> i32 {
        self.wal_level
    }
    pub const fn wal_log_hints(&self) -> bool {
        self.wal_log_hints
    }
    pub const fn track_commit_timestamp(&self) -> bool {
        self.track_commit_timestamp
    }
}

/// `xl_end_of_recovery` (access/xlog_internal.h). `sizeof == 24`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_end_of_recovery {
    end_time: TimestampTz,
    ThisTimeLineID: TimeLineID,
    PrevTimeLineID: TimeLineID,
    wal_level: i32,
}

impl xl_end_of_recovery {
    /// end_time@0, ThisTimeLineID@8, PrevTimeLineID@12, wal_level@16.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            end_time: read_i64(data, 0)?,
            ThisTimeLineID: read_u32(data, 8)?,
            PrevTimeLineID: read_u32(data, 12)?,
            wal_level: read_i32(data, 16)?,
        })
    }

    pub const fn end_time(&self) -> TimestampTz {
        self.end_time
    }
    pub const fn this_timeline_id(&self) -> TimeLineID {
        self.ThisTimeLineID
    }
    pub const fn prev_timeline_id(&self) -> TimeLineID {
        self.PrevTimeLineID
    }
    pub const fn wal_level(&self) -> i32 {
        self.wal_level
    }
}

/// `xl_overwrite_contrecord` (access/xlog_internal.h). `sizeof == 16`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_overwrite_contrecord {
    overwritten_lsn: XLogRecPtr,
    overwrite_time: TimestampTz,
}

impl xl_overwrite_contrecord {
    /// overwritten_lsn@0, overwrite_time@8.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            overwritten_lsn: read_u64(data, 0)?,
            overwrite_time: read_i64(data, 8)?,
        })
    }

    pub const fn overwritten_lsn(&self) -> XLogRecPtr {
        self.overwritten_lsn
    }
    pub const fn overwrite_time(&self) -> TimestampTz {
        self.overwrite_time
    }
}

/// `xl_replorigin_set` (replication/origin.h). `sizeof == 16`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_replorigin_set {
    remote_lsn: XLogRecPtr,
    node_id: RepOriginId,
    force: bool,
}

impl xl_replorigin_set {
    /// remote_lsn@0, node_id@8 (`RepOriginId` == `uint16`), force@10.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            remote_lsn: read_u64(data, 0)?,
            node_id: read_u16(data, 8)?,
            force: read_bool(data, 10)?,
        })
    }

    pub const fn remote_lsn(&self) -> XLogRecPtr {
        self.remote_lsn
    }
    pub const fn node_id(&self) -> RepOriginId {
        self.node_id
    }
    pub const fn force(&self) -> bool {
        self.force
    }
}

/// `xl_replorigin_drop` (replication/origin.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_replorigin_drop {
    node_id: RepOriginId,
}

impl xl_replorigin_drop {
    /// node_id@0 (`RepOriginId` == `uint16`).
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        Some(Self {
            node_id: read_u16(data, 0)?,
        })
    }

    pub const fn node_id(&self) -> RepOriginId {
        self.node_id
    }
}
