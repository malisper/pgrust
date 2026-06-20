//! Port of `src/common/controldata_utils.c`.
//!
//! `get_controlfile` reads `<DataDir>/global/pg_control`, validates the stored
//! CRC and the `pg_control_version`, and returns the parsed
//! [`ControlFileData`]. The on-disk byte image is a fixed-layout struct (field
//! order, types and alignment padding mirror the C struct so the CRC is
//! computed over the identical byte sequence the backend produces). The CRC
//! check does NOT raise on mismatch — instead the match result is returned to
//! the caller as `crc_ok` (C's `*crc_ok_p`), which the `pg_control_*` SQL
//! functions turn into their own "calculated CRC checksum does not match"
//! error.

use backend_storage_file_fd_seams as fd;
use backend_utils_error::ereport;
use types_control::{
    CheckPoint, ControlFileData, DBState, MOCK_AUTH_NONCE_LEN, PG_CONTROL_VERSION,
};
use types_core::{pg_crc32c, FullTransactionId};
use types_error::{PgResult, ERROR};

/// `sizeof(ControlFileData)` on LP64 (`catalog/pg_control.h`), up to and
/// including the trailing `crc` field.
const SIZE_OF_CONTROL_FILE_DATA: usize = 296;

/// `offsetof(ControlFileData, crc)`.
const OFFSET_OF_CRC: usize = 292;

// ---------------------------------------------------------------------------
// CRC32C helpers (`c.h` / `port/pg_crc32c.h`).
// ---------------------------------------------------------------------------

fn INIT_CRC32C() -> u32 {
    0xFFFF_FFFF
}
fn COMP_CRC32C(crc: u32, data: &[u8]) -> u32 {
    port_crc32c::pg_comp_crc32c_sb8(crc, data)
}
fn FIN_CRC32C(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}
fn EQ_CRC32C(c1: u32, c2: u32) -> bool {
    c1 == c2
}

// ---------------------------------------------------------------------------
// On-disk byte-image decoders.
//
// Field offsets mirror the C struct on LP64. These match the encoder in the
// xlog crate (`control_file_image_no_crc`) byte for byte so the CRC computed
// here over `&bytes[..OFFSET_OF_CRC]` reproduces the one the backend wrote.
// ---------------------------------------------------------------------------

fn get_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}
fn get_i32(b: &[u8], off: usize) -> i32 {
    i32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}
fn get_u64(b: &[u8], off: usize) -> u64 {
    u64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}
fn get_i64(b: &[u8], off: usize) -> i64 {
    i64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}
fn get_f64(b: &[u8], off: usize) -> f64 {
    f64::from_ne_bytes(b[off..off + 8].try_into().unwrap())
}
fn get_bool(b: &[u8], off: usize) -> bool {
    b[off] != 0
}

fn db_state_from_u32(v: u32) -> DBState {
    match v {
        0 => DBState::Startup,
        1 => DBState::Shutdowned,
        2 => DBState::ShutdownedInRecovery,
        3 => DBState::Shutdowning,
        4 => DBState::InCrashRecovery,
        5 => DBState::InArchiveRecovery,
        6 => DBState::InProduction,
        _ => DBState::Startup,
    }
}

fn checkpoint_from_bytes(b: &[u8], base: usize) -> CheckPoint {
    CheckPoint {
        redo: get_u64(b, base),
        ThisTimeLineID: get_u32(b, base + 8),
        PrevTimeLineID: get_u32(b, base + 12),
        fullPageWrites: get_bool(b, base + 16),
        wal_level: get_i32(b, base + 20),
        nextXid: FullTransactionId {
            value: get_u64(b, base + 24),
        },
        nextOid: get_u32(b, base + 32),
        nextMulti: get_u32(b, base + 36),
        nextMultiOffset: get_u32(b, base + 40),
        oldestXid: get_u32(b, base + 44),
        oldestXidDB: get_u32(b, base + 48),
        oldestMulti: get_u32(b, base + 52),
        oldestMultiDB: get_u32(b, base + 56),
        time: get_i64(b, base + 64),
        oldestCommitTsXid: get_u32(b, base + 72),
        newestCommitTsXid: get_u32(b, base + 76),
        oldestActiveXid: get_u32(b, base + 80),
    }
}

/// Deserialize a [`ControlFileData`] from its on-disk byte image.
fn control_file_from_bytes(b: &[u8]) -> ControlFileData {
    let mut nonce = [0u8; MOCK_AUTH_NONCE_LEN];
    nonce.copy_from_slice(&b[257..257 + MOCK_AUTH_NONCE_LEN]);
    ControlFileData {
        system_identifier: get_u64(b, 0),
        pg_control_version: get_u32(b, 8),
        catalog_version_no: get_u32(b, 12),
        state: db_state_from_u32(get_u32(b, 16)),
        time: get_i64(b, 24),
        checkPoint: get_u64(b, 32),
        checkPointCopy: checkpoint_from_bytes(b, 40),
        unloggedLSN: get_u64(b, 128),
        minRecoveryPoint: get_u64(b, 136),
        minRecoveryPointTLI: get_u32(b, 144),
        backupStartPoint: get_u64(b, 152),
        backupEndPoint: get_u64(b, 160),
        backupEndRequired: get_bool(b, 168),
        wal_level: get_i32(b, 172),
        wal_log_hints: get_bool(b, 176),
        MaxConnections: get_i32(b, 180),
        max_worker_processes: get_i32(b, 184),
        max_wal_senders: get_i32(b, 188),
        max_prepared_xacts: get_i32(b, 192),
        max_locks_per_xact: get_i32(b, 196),
        track_commit_timestamp: get_bool(b, 200),
        maxAlign: get_u32(b, 204),
        floatFormat: get_f64(b, 208),
        blcksz: get_u32(b, 216),
        relseg_size: get_u32(b, 220),
        xlog_blcksz: get_u32(b, 224),
        xlog_seg_size: get_u32(b, 228),
        nameDataLen: get_u32(b, 232),
        indexMaxKeys: get_u32(b, 236),
        toast_max_chunk_size: get_u32(b, 240),
        loblksize: get_u32(b, 244),
        float8ByVal: get_bool(b, 248),
        data_checksum_version: get_u32(b, 252),
        default_char_signedness: get_bool(b, 256),
        mock_authentication_nonce: nonce,
        crc: get_u32(b, OFFSET_OF_CRC),
    }
}

// ---------------------------------------------------------------------------
// get_controlfile (controldata_utils.c:42).
// ---------------------------------------------------------------------------

/// `get_controlfile(DataDir, &crc_ok)` — read and parse
/// `<DataDir>/global/pg_control`, returning the parsed [`ControlFileData`] and
/// whether the stored CRC matched.
///
/// Faithful to C's `get_controlfile_internal`: open the control file, read
/// `sizeof(ControlFileData)` bytes (a short read is a hard error), recompute
/// the CRC over `offsetof(ControlFileData, crc)` and compare to the stored
/// `crc` (returned as `crc_ok`, NOT raised), then validate
/// `pg_control_version` (raising on a byte-ordering mismatch or an incorrect
/// version).
pub fn get_controlfile(datadir: &str) -> PgResult<(ControlFileData, bool)> {
    let control_file_path = format!("{datadir}/global/pg_control");

    // OpenTransientFile + read(sizeof(ControlFileData)) + CloseTransientFile,
    // via the fd.c whole-file reader. `Ok(None)` is C's `open()` failing.
    let bytes = match fd::allocate_file_read::call(&control_file_path)? {
        Some(b) => b,
        None => {
            return Err(ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{control_file_path}\" for reading"))
                .into_error());
        }
    };

    // C's `read(fd, ControlFile, sizeof(ControlFileData))` reads exactly
    // `sizeof(ControlFileData)` bytes off the front of the file; the on-disk
    // `global/pg_control` is zero-padded out to `PG_CONTROL_FILE_SIZE` (8192),
    // so a faithful read takes the leading `SIZE_OF_CONTROL_FILE_DATA` bytes
    // and ignores the padding. `if (r != sizeof(ControlFileData))` — a short
    // file (fewer than `sizeof(ControlFileData)` bytes available) is fatal.
    if bytes.len() < SIZE_OF_CONTROL_FILE_DATA {
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not read file \"{}\": read {} of {}",
                control_file_path,
                bytes.len(),
                SIZE_OF_CONTROL_FILE_DATA
            ))
            .into_error());
    }
    let bytes = &bytes[..SIZE_OF_CONTROL_FILE_DATA];

    let control_file = control_file_from_bytes(bytes);

    // Check the CRC. `*crc_ok_p` is returned, not raised.
    let mut crc = INIT_CRC32C();
    crc = COMP_CRC32C(crc, &bytes[..OFFSET_OF_CRC]);
    crc = FIN_CRC32C(crc);
    let crc_ok = EQ_CRC32C(crc, control_file.crc as pg_crc32c);

    // Make sure the control file is valid byte order.
    if control_file.pg_control_version % 65536 == 0 && control_file.pg_control_version / 65536 != 0
    {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "byte ordering mismatch in control file \"{control_file_path}\""
            ))
            .errdetail(
                "Possibly byte ordering doesn't match the database server that wrote this control file.",
            )
            .into_error());
    } else if control_file.pg_control_version != PG_CONTROL_VERSION {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "the database cluster was initialized with PG_CONTROL_VERSION {} (0x{:08x}), but the server was compiled with PG_CONTROL_VERSION {} (0x{:08x})",
                control_file.pg_control_version,
                control_file.pg_control_version,
                PG_CONTROL_VERSION,
                PG_CONTROL_VERSION
            ))
            .errhint("This could be a problem of mismatched byte ordering.  It looks like you need to initdb.")
            .into_error());
    }

    Ok((control_file, crc_ok))
}

/// Install the `controldata_utils.c` seams.
pub fn init_seams() {
    common_controldata_utils_seams::get_controlfile::set(get_controlfile);
}
