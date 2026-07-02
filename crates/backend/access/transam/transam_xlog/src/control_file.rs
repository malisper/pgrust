use std::cell::UnsafeCell;
use std::mem::{offset_of, size_of};

use crc32c::{fin_crc32c, pg_comp_crc32c, CRC32C_INIT};
use elog::ereport;
use types_core::{
    pg_time_t, FullTransactionId, MultiXactId, MultiXactOffset, Oid, TimeLineID, TransactionId,
    XLogRecPtr,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL, PANIC,
};

use crate::{DBState, IsValidWalSegSize, XLogMBVarToSegs, WAL_LEVEL_REPLICA};

pub const PG_CONTROL_VERSION: u32 = 1800;
pub const CATALOG_VERSION_NO: u32 = 202506291;
pub const PG_CONTROL_FILE_SIZE: usize = 8192;
pub const PG_CONTROL_MAX_SAFE_SIZE: usize = 512;
pub const MOCK_AUTH_NONCE_LEN: usize = 32;
pub const XLOG_CONTROL_FILE: &str = "global/pg_control";
pub const FLOATFORMAT_VALUE: f64 = 1234567.0;
pub const FirstNormalUnloggedLSN: XLogRecPtr = 1000;

// pg_control.h layout, byte-exact (CRC + on-disk image depend on it; layout
// asserts in tests.rs mirror a C compile of the header).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
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
    pub const ZEROED: CheckPoint = CheckPoint {
        redo: 0,
        ThisTimeLineID: 0,
        PrevTimeLineID: 0,
        fullPageWrites: false,
        wal_level: 0,
        nextXid: FullTransactionId { value: 0 },
        nextOid: 0,
        nextMulti: 0,
        nextMultiOffset: 0,
        oldestXid: 0,
        oldestXidDB: 0,
        oldestMulti: 0,
        oldestMultiDB: 0,
        time: 0,
        oldestCommitTsXid: 0,
        newestCommitTsXid: 0,
        oldestActiveXid: 0,
    };

    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: repr(C) POD view; padding bytes read as whatever the zeroed
        // initialization left (all images originate from ZEROED copies).
        unsafe {
            std::slice::from_raw_parts(self as *const CheckPoint as *const u8, size_of::<CheckPoint>())
        }
    }

    pub fn from_bytes(data: &[u8]) -> CheckPoint {
        assert!(data.len() >= size_of::<CheckPoint>());
        let mut ckpt = CheckPoint::ZEROED;
        // SAFETY: repr(C) POD, any bit pattern valid (bool fields come from
        // images this struct wrote).
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                &mut ckpt as *mut CheckPoint as *mut u8,
                size_of::<CheckPoint>(),
            );
        }
        ckpt
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ControlFileData {
    pub system_identifier: u64,
    pub pg_control_version: u32,
    pub catalog_version_no: u32,
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
    pub maxAlign: u32,
    pub floatFormat: f64,
    pub blcksz: u32,
    pub relseg_size: u32,
    pub xlog_blcksz: u32,
    pub xlog_seg_size: u32,
    pub nameDataLen: u32,
    pub indexMaxKeys: u32,
    pub toast_max_chunk_size: u32,
    pub loblksize: u32,
    pub float8ByVal: bool,
    pub data_checksum_version: u32,
    pub default_char_signedness: bool,
    pub mock_authentication_nonce: [u8; MOCK_AUTH_NONCE_LEN],
    pub crc: u32,
}

const _: () = assert!(size_of::<ControlFileData>() <= PG_CONTROL_MAX_SAFE_SIZE);

impl ControlFileData {
    pub const ZEROED: ControlFileData = ControlFileData {
        system_identifier: 0,
        pg_control_version: 0,
        catalog_version_no: 0,
        state: 0,
        time: 0,
        checkPoint: 0,
        checkPointCopy: CheckPoint::ZEROED,
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
        mock_authentication_nonce: [0; MOCK_AUTH_NONCE_LEN],
        crc: 0,
    };
}

struct ControlFileCell(UnsafeCell<ControlFileData>);
// SAFETY: mutations happen in the startup/checkpoint paths under
// ControlFileLock (C's protocol); concurrent readers copy scalar fields that
// are quiescent outside those windows. Same discipline as C's shmem image.
unsafe impl Sync for ControlFileCell {}

static CONTROL_FILE: ControlFileCell = ControlFileCell(UnsafeCell::new(ControlFileData::ZEROED));
static CONTROL_FILE_READ: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn control_file() -> &'static ControlFileData {
    debug_assert!(CONTROL_FILE_READ.load(std::sync::atomic::Ordering::Relaxed));
    // SAFETY: see ControlFileCell.
    unsafe { &*CONTROL_FILE.0.get() }
}

// Caller must hold ControlFileLock (or be the pre-shmem startup singleton).
pub fn control_file_update(f: impl FnOnce(&mut ControlFileData)) {
    // SAFETY: see ControlFileCell; exclusivity per the caller contract.
    unsafe { f(&mut *CONTROL_FILE.0.get()) }
}

fn loc(func: &'static str) -> ErrorLocation {
    ErrorLocation::new("xlog.c", 0, func)
}

fn control_file_crc(cf: &ControlFileData) -> u32 {
    // SAFETY: repr(C) POD byte view over the CRC-covered prefix.
    let bytes = unsafe {
        std::slice::from_raw_parts(
            cf as *const ControlFileData as *const u8,
            offset_of!(ControlFileData, crc),
        )
    };
    fin_crc32c(pg_comp_crc32c(CRC32C_INIT, bytes))
}

fn incompatible(detail: String, hint: &str) -> PgResult<()> {
    ereport(FATAL)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg("database files are incompatible with server")
        .errdetail(detail)
        .errhint(hint.to_string())
        .finish(loc("ReadControlFile"))
}

const INITDB_HINT: &str = "It looks like you need to initdb.";
const RECOMPILE_HINT: &str = "It looks like you need to recompile or initdb.";

pub fn ReadControlFile() -> PgResult<()> {
    let dir = init_small::globals::DataDir().unwrap_or(".");
    let path = format!("{dir}/{XLOG_CONTROL_FILE}");
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) => {
            return ereport(PANIC)
                .errmsg(format!("could not open file \"{XLOG_CONTROL_FILE}\": {e}"))
                .finish(loc("ReadControlFile"));
        }
    };
    if data.len() < size_of::<ControlFileData>() {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read file \"{XLOG_CONTROL_FILE}\": read {} of {}",
                data.len(),
                size_of::<ControlFileData>()
            ))
            .finish(loc("ReadControlFile"));
    }

    let mut cf = ControlFileData::ZEROED;
    // SAFETY: length checked; repr(C) POD copy of the on-disk image.
    unsafe {
        std::ptr::copy_nonoverlapping(
            data.as_ptr(),
            &mut cf as *mut ControlFileData as *mut u8,
            size_of::<ControlFileData>(),
        );
    }

    if cf.pg_control_version != PG_CONTROL_VERSION
        && cf.pg_control_version % 65536 == 0
        && cf.pg_control_version / 65536 != 0
    {
        incompatible(
            format!(
                "The database cluster was initialized with PG_CONTROL_VERSION {} (0x{:08x}), but the server was compiled with PG_CONTROL_VERSION {} (0x{:08x}).",
                cf.pg_control_version, cf.pg_control_version, PG_CONTROL_VERSION, PG_CONTROL_VERSION
            ),
            "This could be a problem of mismatched byte ordering.  It looks like you need to initdb.",
        )?;
    }
    if cf.pg_control_version != PG_CONTROL_VERSION {
        incompatible(
            format!(
                "The database cluster was initialized with PG_CONTROL_VERSION {}, but the server was compiled with PG_CONTROL_VERSION {}.",
                cf.pg_control_version, PG_CONTROL_VERSION
            ),
            INITDB_HINT,
        )?;
    }

    if control_file_crc(&cf) != cf.crc {
        return ereport(FATAL)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("incorrect checksum in control file")
            .finish(loc("ReadControlFile"));
    }

    let mism = |name: &str, theirs: i64, ours: i64, hint: &str| -> PgResult<()> {
        incompatible(
            format!(
                "The database cluster was initialized with {name} {theirs}, but the server was compiled with {name} {ours}."
            ),
            hint,
        )
    };

    if cf.catalog_version_no != CATALOG_VERSION_NO {
        mism("CATALOG_VERSION_NO", cf.catalog_version_no as i64, CATALOG_VERSION_NO as i64, INITDB_HINT)?;
    }
    if cf.maxAlign != 8 {
        mism("MAXALIGN", cf.maxAlign as i64, 8, INITDB_HINT)?;
    }
    if cf.floatFormat != FLOATFORMAT_VALUE {
        incompatible(
            "The database cluster appears to use a different floating-point number format than the server executable.".to_string(),
            INITDB_HINT,
        )?;
    }
    if cf.blcksz != types_core::BLCKSZ as u32 {
        mism("BLCKSZ", cf.blcksz as i64, types_core::BLCKSZ as i64, RECOMPILE_HINT)?;
    }
    if cf.relseg_size != types_storage::smgr::RELSEG_SIZE {
        mism("RELSEG_SIZE", cf.relseg_size as i64, types_storage::smgr::RELSEG_SIZE as i64, RECOMPILE_HINT)?;
    }
    if cf.xlog_blcksz != crate::XLOG_BLCKSZ as u32 {
        mism("XLOG_BLCKSZ", cf.xlog_blcksz as i64, crate::XLOG_BLCKSZ as i64, RECOMPILE_HINT)?;
    }
    if cf.nameDataLen != types_core::NAMEDATALEN as u32 {
        mism("NAMEDATALEN", cf.nameDataLen as i64, types_core::NAMEDATALEN as i64, RECOMPILE_HINT)?;
    }
    if cf.indexMaxKeys != types_core::INDEX_MAX_KEYS as u32 {
        mism("INDEX_MAX_KEYS", cf.indexMaxKeys as i64, types_core::INDEX_MAX_KEYS as i64, RECOMPILE_HINT)?;
    }
    if cf.toast_max_chunk_size != TOAST_MAX_CHUNK_SIZE {
        mism("TOAST_MAX_CHUNK_SIZE", cf.toast_max_chunk_size as i64, TOAST_MAX_CHUNK_SIZE as i64, RECOMPILE_HINT)?;
    }
    if cf.loblksize != types_storage::large_object::LOBLKSIZE as u32 {
        mism("LOBLKSIZE", cf.loblksize as i64, types_storage::large_object::LOBLKSIZE as i64, RECOMPILE_HINT)?;
    }
    if !cf.float8ByVal {
        incompatible(
            "The database cluster was initialized without USE_FLOAT8_BYVAL but the server was compiled with USE_FLOAT8_BYVAL.".to_string(),
            RECOMPILE_HINT,
        )?;
    }

    let wal_segment_size = cf.xlog_seg_size as i32;
    if !IsValidWalSegSize(wal_segment_size) {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid WAL segment size in control file ({wal_segment_size} bytes)"))
            .errdetail("The WAL segment size must be a power of two between 1 MB and 1 GB.".to_string())
            .finish(loc("ReadControlFile"));
    }
    guc_tables::vars::wal_segment_size.write(wal_segment_size);
    crate::set_wal_segment_size(wal_segment_size);

    if XLogMBVarToSegs(guc_tables::vars::min_wal_size_mb.read(), wal_segment_size) < 2 {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("\"min_wal_size\" must be at least twice \"wal_segment_size\"")
            .finish(loc("ReadControlFile"));
    }
    if XLogMBVarToSegs(guc_tables::vars::max_wal_size_mb.read(), wal_segment_size) < 2 {
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("\"max_wal_size\" must be at least twice \"wal_segment_size\"")
            .finish(loc("ReadControlFile"));
    }

    crate::CalculateCheckpointSegments();

    control_file_update(|dst| *dst = cf);
    CONTROL_FILE_READ.store(true, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

// TOAST_MAX_CHUNK_SIZE (heaptoast.h) for BLCKSZ 8192.
pub const TOAST_MAX_CHUNK_SIZE: u32 = 1996;

pub fn LocalProcessControlFile(_reset: bool) -> PgResult<()> {
    ReadControlFile()
}

pub fn control_file_loaded() -> bool {
    CONTROL_FILE_READ.load(std::sync::atomic::Ordering::Relaxed)
}

// update_controlfile(DataDir, ControlFile, do_sync=true)
// (common/controldata_utils.c): recompute CRC, single atomic pwrite of
// sizeof(ControlFileData) bytes at offset 0, fsync.
pub fn UpdateControlFile() -> PgResult<()> {
    let mut image = *control_file();
    image.crc = control_file_crc(&image);
    control_file_update(|dst| dst.crc = image.crc);

    let dir = init_small::globals::DataDir().unwrap_or(".");
    let path = format!("{dir}/{XLOG_CONTROL_FILE}");

    use std::io::{Seek, Write};
    let res = (|| -> std::io::Result<()> {
        let mut f = std::fs::OpenOptions::new().write(true).open(&path)?;
        // SAFETY: repr(C) POD byte view of the full struct.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                &image as *const ControlFileData as *const u8,
                size_of::<ControlFileData>(),
            )
        };
        f.seek(std::io::SeekFrom::Start(0))?;
        f.write_all(bytes)?;
        if init_small::globals::enableFsync() {
            f.sync_all()?;
        }
        Ok(())
    })();
    if let Err(e) = res {
        return ereport(PANIC)
            .errmsg(format!("could not write to file \"{XLOG_CONTROL_FILE}\": {e}"))
            .finish(loc("UpdateControlFile"));
    }
    Ok(())
}

pub fn GetSystemIdentifier() -> u64 {
    control_file().system_identifier
}

pub fn GetMockAuthenticationNonce() -> [u8; MOCK_AUTH_NONCE_LEN] {
    control_file().mock_authentication_nonce
}

pub fn DataChecksumsEnabled() -> bool {
    control_file().data_checksum_version > 0
}

pub fn GetDefaultCharSignedness() -> bool {
    control_file().default_char_signedness
}

pub fn GetActiveWalLevelOnStandby() -> i32 {
    control_file().wal_level
}

// CheckRequiredParameterValues (xlog.c): replay/hot-standby GUC floor checks.
pub fn CheckRequiredParameterValues() -> PgResult<()> {
    use init_small::globals;
    let cf = control_file();
    let archive = xlogrecovery_seams::archive_recovery_requested::is_installed()
        && xlogrecovery_seams::archive_recovery_requested::call();
    if archive && cf.wal_level == crate::WAL_LEVEL_MINIMAL {
        return ereport(FATAL)
            .errmsg("WAL was generated with \"wal_level=minimal\", cannot continue recovering")
            .errdetail("This happens if you temporarily set \"wal_level=minimal\" on the server.".to_string())
            .errhint("Use a backup taken after setting \"wal_level\" to higher than \"minimal\".".to_string())
            .finish(loc("CheckRequiredParameterValues"));
    }
    if archive && guc_tables::vars::EnableHotStandby.read() {
        if cf.wal_level < WAL_LEVEL_REPLICA {
            return ereport(ERROR)
                .errmsg("hot standby is not possible because \"wal_level\" was not set to \"replica\" or higher on the primary server")
                .finish(loc("CheckRequiredParameterValues"));
        }
        let checks: [(&str, i32, i32); 5] = [
            ("max_connections", cf.MaxConnections, globals::MaxConnections()),
            ("max_worker_processes", cf.max_worker_processes, globals::max_worker_processes()),
            ("max_wal_senders", cf.max_wal_senders, guc_tables::vars::max_wal_senders.read()),
            ("max_prepared_transactions", cf.max_prepared_xacts, guc_tables::vars::max_prepared_xacts.read()),
            ("max_locks_per_transaction", cf.max_locks_per_xact, guc_tables::vars::max_locks_per_xact.read()),
        ];
        for (name, primary, ours) in checks {
            if ours < primary {
                return ereport(ERROR)
                    .errmsg(format!(
                        "insufficient parameter settings detected: \"{name}\" is {ours}, needs to be at least {primary} (the value on the primary server)"
                    ))
                    .finish(loc("CheckRequiredParameterValues"));
            }
        }
    }
    Ok(())
}
