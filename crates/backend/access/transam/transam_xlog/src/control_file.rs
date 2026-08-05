use std::cell::UnsafeCell;

use elog::ereport;
use types_core::XLogRecPtr;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, FATAL,
};

use crate::{IsValidWalSegSize, XLogMBVarToSegs, WAL_LEVEL_REPLICA};

pub use controldata_utils::{
    CATALOG_VERSION_NO, MOCK_AUTH_NONCE_LEN, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    XLOG_CONTROL_FILE,
};
pub const FLOATFORMAT_VALUE: f64 = 1234567.0;
pub const FirstNormalUnloggedLSN: XLogRecPtr = 1000;

pub use controldata_utils::{CheckPoint, ControlFileData};

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

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

// Test-only: marks the zeroed in-process image as read so control-file
// consumers (e.g. scram_mock_salt) run without a datadir.
#[doc(hidden)]
pub fn control_file_mark_read_for_tests() {
    CONTROL_FILE_READ.store(true, std::sync::atomic::Ordering::Relaxed);
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
    let (cf, crc_ok) = controldata_utils::get_controlfile(dir)?;

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

    if !crc_ok {
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
    // C: SetConfigOption("wal_segment_size", ..., PGC_INTERNAL,
    // PGC_S_DYNAMIC_DEFAULT). Recording the source in the registry is load-
    // bearing under the thread model: each backend thread's GUC re-init
    // republishes boot values into the PROCESS-SHARED backing var, so a bare
    // write here gets clobbered back to 16MB by the next thread bring-up
    // (visible only on --wal-segsize != 16MB clusters).
    if guc_seams::set_config_option_internal_dynamic_default::is_installed() {
        guc_seams::set_config_option_internal_dynamic_default::call(
            "wal_segment_size",
            &wal_segment_size.to_string(),
        )?;
    } else {
        guc_tables::vars::wal_segment_size.write(wal_segment_size);
    }
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

pub fn UpdateControlFile() -> PgResult<()> {
    let mut image = *control_file();
    let dir = init_small::globals::DataDir().unwrap_or(".");
    controldata_utils::update_controlfile(dir, &mut image, init_small::globals::enableFsync())?;
    control_file_update(|dst| {
        dst.time = image.time;
        dst.crc = image.crc;
    });
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
