use crate::types::{uint32, Oid, Size, TimestampTz, TransactionId};
use crate::wal::RelFileLocator;

pub const XLR_INFO_MASK: u8 = 0x0f;

pub const CLOG_ZEROPAGE: u8 = 0x00;
pub const CLOG_TRUNCATE: u8 = 0x10;

pub const COMMIT_TS_ZEROPAGE: u8 = 0x00;
pub const COMMIT_TS_TRUNCATE: u8 = 0x10;

pub const XLOG_DBASE_CREATE_FILE_COPY: u8 = 0x00;
pub const XLOG_DBASE_CREATE_WAL_LOG: u8 = 0x10;
pub const XLOG_DBASE_DROP: u8 = 0x20;

pub const XLOG_GENERIC: u8 = 0x00;
pub const XLOG_LOGICAL_MESSAGE: u8 = 0x00;
pub const XLOG_RELMAP_UPDATE: u8 = 0x00;
pub const XLOG_SEQ_LOG: u8 = 0x00;
pub const XLOG_TBLSPC_CREATE: u8 = 0x00;
pub const XLOG_TBLSPC_DROP: u8 = 0x10;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_clog_truncate {
    pageno: i64,
    oldestXact: TransactionId,
    oldestXactDb: Oid,
}

impl xl_clog_truncate {
    pub const fn new(pageno: i64, oldestXact: TransactionId, oldestXactDb: Oid) -> Self {
        Self {
            pageno,
            oldestXact,
            oldestXactDb,
        }
    }

    pub const fn pageno(&self) -> i64 {
        self.pageno
    }

    pub const fn oldest_xact(&self) -> TransactionId {
        self.oldestXact
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_commit_ts_truncate {
    pageno: i64,
    oldestXid: TransactionId,
}

impl xl_commit_ts_truncate {
    pub const fn new(pageno: i64, oldestXid: TransactionId) -> Self {
        Self { pageno, oldestXid }
    }

    pub const fn pageno(&self) -> i64 {
        self.pageno
    }

    pub const fn oldest_xid(&self) -> TransactionId {
        self.oldestXid
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_commit_ts_set {
    timestamp: TimestampTz,
    nodeid: u16,
    mainxid: TransactionId,
}

impl xl_commit_ts_set {
    pub const fn timestamp(&self) -> TimestampTz {
        self.timestamp
    }

    pub const fn nodeid(&self) -> u16 {
        self.nodeid
    }

    pub const fn mainxid(&self) -> TransactionId {
        self.mainxid
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_dbase_create_file_copy_rec {
    db_id: Oid,
    tablespace_id: Oid,
    src_db_id: Oid,
    src_tablespace_id: Oid,
}

impl xl_dbase_create_file_copy_rec {
    pub const fn new(
        db_id: Oid,
        tablespace_id: Oid,
        src_db_id: Oid,
        src_tablespace_id: Oid,
    ) -> Self {
        Self {
            db_id,
            tablespace_id,
            src_db_id,
            src_tablespace_id,
        }
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

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_dbase_create_wal_log_rec {
    db_id: Oid,
    tablespace_id: Oid,
}

impl xl_dbase_create_wal_log_rec {
    pub const fn new(db_id: Oid, tablespace_id: Oid) -> Self {
        Self {
            db_id,
            tablespace_id,
        }
    }

    pub const fn db_id(&self) -> Oid {
        self.db_id
    }

    pub const fn tablespace_id(&self) -> Oid {
        self.tablespace_id
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_dbase_drop_rec {
    db_id: Oid,
    ntablespaces: i32,
}

impl xl_dbase_drop_rec {
    pub const fn new(db_id: Oid, ntablespaces: i32) -> Self {
        Self {
            db_id,
            ntablespaces,
        }
    }

    pub const fn db_id(&self) -> Oid {
        self.db_id
    }

    pub const fn ntablespaces(&self) -> i32 {
        self.ntablespaces
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_logical_message {
    dbId: Oid,
    transactional: bool,
    prefix_size: Size,
    message_size: Size,
}

impl xl_logical_message {
    pub const fn new(
        dbId: Oid,
        transactional: bool,
        prefix_size: Size,
        message_size: Size,
    ) -> Self {
        Self {
            dbId,
            transactional,
            prefix_size,
            message_size,
        }
    }

    pub const fn db_id(&self) -> Oid {
        self.dbId
    }

    pub const fn transactional(&self) -> bool {
        self.transactional
    }

    pub const fn prefix_size(&self) -> Size {
        self.prefix_size
    }

    pub const fn message_size(&self) -> Size {
        self.message_size
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_relmap_update {
    dbid: Oid,
    tsid: Oid,
    nbytes: i32,
}

impl xl_relmap_update {
    pub const fn new(dbid: Oid, tsid: Oid, nbytes: i32) -> Self {
        Self { dbid, tsid, nbytes }
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

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_seq_rec {
    locator: RelFileLocator,
}

impl xl_seq_rec {
    pub const fn new(locator: RelFileLocator) -> Self {
        Self { locator }
    }

    pub const fn locator(&self) -> &RelFileLocator {
        &self.locator
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_tblspc_create_rec {
    ts_id: Oid,
}

impl xl_tblspc_create_rec {
    pub const fn new(ts_id: Oid) -> Self {
        Self { ts_id }
    }

    pub const fn ts_id(&self) -> Oid {
        self.ts_id
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct xl_tblspc_drop_rec {
    ts_id: Oid,
}

impl xl_tblspc_drop_rec {
    pub const fn new(ts_id: Oid) -> Self {
        Self { ts_id }
    }

    pub const fn ts_id(&self) -> Oid {
        self.ts_id
    }
}

pub type GenericXLogOffset = uint32;
