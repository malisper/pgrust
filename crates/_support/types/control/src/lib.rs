//! Control-file and checkpoint vocabulary (`catalog/pg_control.h`,
//! `access/transam.h`): `CheckPoint`, `ControlFileData`, `DBState`, and the
//! associated constants. Trimmed to what the xlog units consume; field order
//! and types mirror the C structs (the on-disk codec lives in the xlog crate).

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::types_core::{
    pg_crc32c, pg_time_t, uint32, uint64, MultiXactId, MultiXactOffset, Oid, TimeLineID,
    TransactionId, XLogRecPtr,
};

pub use ::types_core::FullTransactionId;

// ===========================================================================
// Constants (catalog/pg_control.h, access/xlogdefs.h).
// ===========================================================================

/// `PG_CONTROL_VERSION` (`catalog/pg_control.h`).
pub const PG_CONTROL_VERSION: uint32 = 1800;
/// `PG_CONTROL_FILE_SIZE` (`catalog/pg_control.h`) — on-disk padded file size.
pub const PG_CONTROL_FILE_SIZE: usize = 8192;
/// `MOCK_AUTH_NONCE_LEN` (`catalog/pg_control.h`).
pub const MOCK_AUTH_NONCE_LEN: usize = 32;
/// `FLOATFORMAT_VALUE` (`catalog/pg_control.h`) — float-format canary.
pub const FLOATFORMAT_VALUE: f64 = 1234567.0;

/// `FirstNormalUnloggedLSN` (`access/xlogdefs.h`).
pub const FirstNormalUnloggedLSN: XLogRecPtr = 1000;

// ===========================================================================
// DBState (catalog/pg_control.h).
// ===========================================================================

/// `DBState` (`catalog/pg_control.h`) — `ControlFileData.state`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum DBState {
    Startup = 0,
    Shutdowned = 1,
    ShutdownedInRecovery = 2,
    Shutdowning = 3,
    InCrashRecovery = 4,
    InArchiveRecovery = 5,
    InProduction = 6,
}

impl Default for DBState {
    fn default() -> Self {
        DBState::Startup
    }
}

// ===========================================================================
// CheckPoint (access/transam.h).
// ===========================================================================

/// `CheckPoint` (`access/transam.h`) — the contents of a checkpoint WAL record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckPoint {
    pub redo: XLogRecPtr,
    pub ThisTimeLineID: TimeLineID,
    pub PrevTimeLineID: TimeLineID,
    pub fullPageWrites: bool,
    pub wal_level: i32,
    pub nextXid: FullTransactionId,
    pub nextOid: Oid,
    pub nextMulti: MultiXactId,
    pub nextMultiOffset: MultiXactOffset,
    pub oldestXid: TransactionId,
    pub oldestXidDB: Oid,
    pub oldestMulti: MultiXactId,
    pub oldestMultiDB: Oid,
    pub time: pg_time_t,
    pub oldestCommitTsXid: TransactionId,
    pub newestCommitTsXid: TransactionId,
    pub oldestActiveXid: TransactionId,
}

impl CheckPoint {
    /// Deserialize a `CheckPoint` from the main-data area of a checkpoint WAL
    /// record (`memcpy(&checkPoint, XLogRecGetData(record), sizeof(CheckPoint))`).
    ///
    /// Field offsets (LP64, `sizeof(CheckPoint) == 88`): redo@0,
    /// ThisTimeLineID@8, PrevTimeLineID@12, fullPageWrites@16, wal_level@20,
    /// nextXid@24 (FullTransactionId, u64), nextOid@32, nextMulti@36,
    /// nextMultiOffset@40, oldestXid@44, oldestXidDB@48, oldestMulti@52,
    /// oldestMultiDB@56, time@64 (pg_time_t, i64), oldestCommitTsXid@72,
    /// newestCommitTsXid@76, oldestActiveXid@80. Returns `None` if `data` is too
    /// short.
    pub fn from_record_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 88 {
            return None;
        }
        let u32_at = |off: usize| -> uint32 {
            uint32::from_ne_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
        };
        let u64_at = |off: usize| -> uint64 {
            uint64::from_ne_bytes([
                data[off],
                data[off + 1],
                data[off + 2],
                data[off + 3],
                data[off + 4],
                data[off + 5],
                data[off + 6],
                data[off + 7],
            ])
        };
        Some(Self {
            redo: u64_at(0),
            ThisTimeLineID: u32_at(8),
            PrevTimeLineID: u32_at(12),
            fullPageWrites: data[16] != 0,
            wal_level: u32_at(20) as i32,
            nextXid: FullTransactionId { value: u64_at(24) },
            nextOid: u32_at(32),
            nextMulti: u32_at(36),
            nextMultiOffset: u32_at(40),
            oldestXid: u32_at(44),
            oldestXidDB: u32_at(48),
            oldestMulti: u32_at(52),
            oldestMultiDB: u32_at(56),
            time: u64_at(64) as pg_time_t,
            oldestCommitTsXid: u32_at(72),
            newestCommitTsXid: u32_at(76),
            oldestActiveXid: u32_at(80),
        })
    }
}

// ===========================================================================
// ControlFileData (catalog/pg_control.h).
// ===========================================================================

/// `ControlFileData` (`catalog/pg_control.h`) — the `global/pg_control` image.
#[derive(Clone, Copy, Debug)]
pub struct ControlFileData {
    pub system_identifier: uint64,
    pub pg_control_version: uint32,
    pub catalog_version_no: uint32,
    pub state: DBState,
    pub time: pg_time_t,
    pub checkPoint: XLogRecPtr,
    pub checkPointCopy: CheckPoint,
    pub unloggedLSN: XLogRecPtr,
    pub minRecoveryPoint: XLogRecPtr,
    pub minRecoveryPointTLI: TimeLineID,
    pub backupStartPoint: XLogRecPtr,
    pub backupEndPoint: XLogRecPtr,
    pub backupEndRequired: bool,
    pub wal_level: i32,
    pub wal_log_hints: bool,
    pub MaxConnections: i32,
    pub max_worker_processes: i32,
    pub max_wal_senders: i32,
    pub max_prepared_xacts: i32,
    pub max_locks_per_xact: i32,
    pub track_commit_timestamp: bool,
    pub maxAlign: uint32,
    pub floatFormat: f64,
    pub blcksz: uint32,
    pub relseg_size: uint32,
    pub xlog_blcksz: uint32,
    pub xlog_seg_size: uint32,
    pub nameDataLen: uint32,
    pub indexMaxKeys: uint32,
    pub toast_max_chunk_size: uint32,
    pub loblksize: uint32,
    pub float8ByVal: bool,
    pub data_checksum_version: uint32,
    pub default_char_signedness: bool,
    pub mock_authentication_nonce: [u8; MOCK_AUTH_NONCE_LEN],
    pub crc: pg_crc32c,
}

impl Default for ControlFileData {
    fn default() -> Self {
        ControlFileData {
            system_identifier: 0,
            pg_control_version: 0,
            catalog_version_no: 0,
            state: DBState::default(),
            time: 0,
            checkPoint: 0,
            checkPointCopy: CheckPoint::default(),
            unloggedLSN: 0,
            minRecoveryPoint: 0,
            minRecoveryPointTLI: 0,
            backupStartPoint: 0,
            backupEndPoint: 0,
            backupEndRequired: false,
            wal_level: 0,
            wal_log_hints: false,
            MaxConnections: 0,
            max_worker_processes: 0,
            max_wal_senders: 0,
            max_prepared_xacts: 0,
            max_locks_per_xact: 0,
            track_commit_timestamp: false,
            maxAlign: 0,
            floatFormat: 0.0,
            blcksz: 0,
            relseg_size: 0,
            xlog_blcksz: 0,
            xlog_seg_size: 0,
            nameDataLen: 0,
            indexMaxKeys: 0,
            toast_max_chunk_size: 0,
            loblksize: 0,
            float8ByVal: false,
            data_checksum_version: 0,
            default_char_signedness: false,
            mock_authentication_nonce: [0u8; MOCK_AUTH_NONCE_LEN],
            crc: 0,
        }
    }
}
