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
use types_core::{int64, Oid, TransactionId};

fn read_u32(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_ne_bytes(
        data.get(offset..offset + 4)?.try_into().ok()?,
    ))
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
    forkNum: types_core::ForkNumber,
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
            forkNum: types_core::ForkNumber::from_i32(read_i32(data, 12)?)?,
        })
    }

    pub const fn rlocator(&self) -> RelFileLocator {
        self.rlocator
    }

    pub const fn fork_num(&self) -> types_core::ForkNumber {
        self.forkNum
    }
}

/// `xl_smgr_truncate` (catalog/storage_xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct xl_smgr_truncate {
    blkno: types_core::BlockNumber,
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

    pub const fn blkno(&self) -> types_core::BlockNumber {
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
