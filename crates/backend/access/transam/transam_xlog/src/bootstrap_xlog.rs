//! [`BootStrapXLOG`] (xlog.c:5095) and [`InitControlFile`] (xlog.c:4224): the
//! one-time WAL + control-file install performed by `postgres --boot` (and
//! `initdb`).
//!
//! `BootStrapXLOG` picks a system identifier, writes the first WAL segment
//! (`0/1`) holding a single `XLOG_CHECKPOINT_SHUTDOWN` record built from a fresh
//! bootstrap `CheckPoint`, seeds the in-memory cluster bounds
//! (`TransamVariables` / multixact / clog / commit-ts limits), writes
//! `global/pg_control`, bootstraps the four SLRUs, and re-reads the control file.
//!
//! Control flow is 1:1 with C `BootStrapXLOG`. The genuine differences from C:
//!   * the `CheckPoint` is serialized into the record body with the byte-exact
//!     [`crate::checkpoint::checkpoint_to_bytes`] image (the Rust `CheckPoint`
//!     is not `repr(C)`, so a raw memcpy would not match the on-disk format);
//!   * the segment page bytes are written through `libc::write` / `libc::close`
//!     and fsynced with [`crate::write::issue_xlog_fsync`] (which, like C's
//!     `pg_fsync`, no-ops when `enableFsync` is off);
//!   * the cross-subsystem in-memory limit setters and the four `BootStrap*`
//!     SLRU calls go through their owner seams (the WAL crate must not depend on
//!     clog/multixact/subtrans/commit_ts directly).

extern crate alloc;

use alloc::vec;

use ::control::{FirstNormalUnloggedLSN, MOCK_AUTH_NONCE_LEN};
use ::types_core::xact::{FullTransactionId, InvalidTransactionId};
use ::types_core::{Oid, TransactionId};
use ::types_core::catalog::FirstGenbkiObjectId;
use ::types_error::{ErrorLocation, PgResult, PANIC};
use ::utils_error::ereport;
use ::wal::wal::{RM_XLOG_ID, SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT};
use ::wal::xlog_consts::{BOOTSTRAP_TIME_LINE_ID, SIZE_OF_XLOG_LONG_PHD};

use crate::checkpoint::{checkpoint_to_bytes, SIZE_OF_CHECK_POINT};
use crate::shmem::{self, control_file_mut, wal_segment_size};

/// `XLOG_BLCKSZ` (pg_config.h) — WAL block size in bytes.
const XLOG_BLCKSZ: usize = 8192;

/// `XLOG_PAGE_MAGIC` (access/xlog_internal.h).
const XLOG_PAGE_MAGIC: u16 = 0xD118;
/// `XLP_LONG_HEADER` (access/xlog_internal.h).
const XLP_LONG_HEADER: u16 = 0x0002;

/// `SizeOfXLogRecord` (access/xlogrecord.h) — `offsetof(XLogRecord, xl_crc) +
/// sizeof(pg_crc32c)` = 24 on LP64.
const SIZE_OF_XLOG_RECORD: usize = 24;

/// `XLR_BLOCK_ID_DATA_SHORT` (access/xlogrecord.h).
const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;

/// `XLOG_CHECKPOINT_SHUTDOWN` (catalog/pg_control.h) — the rmgr info byte for a
/// shutdown checkpoint.
const XLOG_CHECKPOINT_SHUTDOWN: u8 = 0x00;

/// `Template1DbOid` (catalog/pg_database_d.h) — the OID of the `template1`
/// database, used as the `oldestXidDB` / `oldestMultiDB` at bootstrap.
const TEMPLATE1_DB_OID: Oid = 1;

/// `FirstMultiXactId` (access/multixact.h).
const FIRST_MULTI_XACT_ID: u32 = 1;

/// `FirstNormalTransactionId` (access/transam.h).
const FIRST_NORMAL_TRANSACTION_ID: TransactionId = 3;

/// `InitControlFile(sysidentifier, data_checksum_version)` (xlog.c:4224) — zero
/// the control-file image and seed the status + replay-parameter fields. The
/// version/compat fields are filled later by `WriteControlFile`.
pub fn InitControlFile(sysidentifier: u64, data_checksum_version: u32) -> PgResult<()> {
    use ::control::DBState;

    // Generate a random nonce for the mock authentication challenge.
    let mut mock_auth_nonce = [0u8; MOCK_AUTH_NONCE_LEN];
    if !pg_strong_random::pg_strong_random(&mut mock_auth_nonce) {
        return ereport(PANIC)
            .errmsg("could not generate secret authorization token")
            .finish(ErrorLocation::new("xlog.c", 4233, "InitControlFile"))
            .map(|_| ());
    }

    // memset(ControlFile, 0, sizeof(ControlFileData)); then set the fields C sets.
    let cf = control_file_mut();
    *cf = ::control::ControlFileData::default();

    cf.system_identifier = sysidentifier;
    cf.mock_authentication_nonce = mock_auth_nonce;
    cf.state = DBState::Shutdowned;
    cf.unloggedLSN = FirstNormalUnloggedLSN;

    // Important parameter values for use when replaying WAL. These come from the
    // GUCs as they stand at bootstrap.
    cf.MaxConnections = guc_tables::vars::MaxConnections.read();
    cf.max_worker_processes = guc_tables::vars::max_worker_processes.read();
    cf.max_wal_senders = guc_tables::vars::max_wal_senders.read();
    cf.max_prepared_xacts = guc_tables::vars::max_prepared_xacts.read();
    cf.max_locks_per_xact = guc_tables::vars::max_locks_per_xact.read();
    cf.wal_level = guc_tables::vars::wal_level.read();
    cf.wal_log_hints = guc_tables::vars::wal_log_hints.read();
    cf.track_commit_timestamp = guc_tables::vars::track_commit_timestamp.read();
    cf.data_checksum_version = data_checksum_version;

    Ok(())
}

/// `BootStrapXLOG(data_checksum_version)` (xlog.c:5095) — create `pg_control`
/// and the initial XLOG segment. Must be called ONCE on system install.
pub fn BootStrapXLOG(data_checksum_version: u32) -> PgResult<()> {
    // Allow ordinary WAL segment creation, like StartupXLOG() would.
    crate::write::SetInstallXLogFileSegmentActive()?;

    // Select a hopefully-unique system identifier: tv_sec<<32 | tv_usec<<12 |
    // (pid & 0xFFF). (xlog.c:5121-5124)
    let (tv_sec, tv_usec) = gettimeofday();
    let sysidentifier: u64 =
        ((tv_sec as u64) << 32) | ((tv_usec as u64) << 12) | ((getpid() as u64) & 0xFFF);

    let wal_segsz = wal_segment_size() as u64;

    // Set up information for the initial checkpoint record. The initial
    // checkpoint record is written to the beginning of the WAL segment with
    // logid=0 logseg=1; segment 0/0 is reserved as "before any valid WAL".
    // (xlog.c:5137-5165)
    let check_point = ::control::CheckPoint {
        redo: wal_segsz + SIZE_OF_XLOG_LONG_PHD as u64,
        ThisTimeLineID: BOOTSTRAP_TIME_LINE_ID,
        PrevTimeLineID: BOOTSTRAP_TIME_LINE_ID,
        fullPageWrites: guc_tables::vars::fullPageWrites.read(),
        wal_level: guc_tables::vars::wal_level.read(),
        nextXid: FullTransactionId::from_epoch_and_xid(0, FIRST_NORMAL_TRANSACTION_ID),
        nextOid: FirstGenbkiObjectId,
        nextMulti: FIRST_MULTI_XACT_ID,
        nextMultiOffset: 0,
        oldestXid: FIRST_NORMAL_TRANSACTION_ID,
        oldestXidDB: TEMPLATE1_DB_OID,
        oldestMulti: FIRST_MULTI_XACT_ID,
        oldestMultiDB: TEMPLATE1_DB_OID,
        oldestCommitTsXid: InvalidTransactionId,
        newestCommitTsXid: InvalidTransactionId,
        time: pg_time_now(),
        oldestActiveXid: InvalidTransactionId,
    };

    // Seed the in-memory cluster bounds from the checkpoint record.
    // (xlog.c:5158-5165) — through the owner seams (the WAL crate cannot depend
    // on varsup/multixact/clog/commit-ts directly).
    varsup_seams::set_transam_variables_at_startup::call(check_point.nextXid, check_point.nextOid);
    multixact_seams::multi_xact_set_next_m_xact::call(
        check_point.nextMulti,
        check_point.nextMultiOffset,
    )?;
    varsup_seams::advance_oldest_clog_xid::call(check_point.oldestXid)?;
    vacuum_seams::set_transaction_id_limit::call(check_point.oldestXid, check_point.oldestXidDB)?;
    multixact_seams::set_multi_xact_id_limit::call(
        check_point.oldestMulti,
        check_point.oldestMultiDB,
        true,
    )?;
    commit_ts_seams::set_commit_ts_limit::call(InvalidTransactionId, InvalidTransactionId)?;

    // Build the first XLOG page in a heap buffer (C uses a palloc'd O_DIRECT-
    // aligned buffer; the bytes written are identical). (xlog.c:5167-5210)
    let mut page = vec![0u8; XLOG_BLCKSZ];

    // XLogLongPageHeaderData over the std header. Layout (LP64): xlp_magic@0(u16),
    // xlp_info@2(u16), xlp_tli@4(u32), xlp_pageaddr@8(u64), xlp_rem_len@16(u32),
    // pad@20..24, xlp_sysid@24(u64), xlp_seg_size@32(u32), xlp_xlog_blcksz@36(u32).
    put_u16(&mut page, 0, XLOG_PAGE_MAGIC);
    put_u16(&mut page, 2, XLP_LONG_HEADER);
    put_u32(&mut page, 4, BOOTSTRAP_TIME_LINE_ID);
    put_u64(&mut page, 8, wal_segsz); // xlp_pageaddr = wal_segment_size
    put_u64(&mut page, 24, sysidentifier);
    put_u32(&mut page, 32, wal_segsz as u32);
    put_u32(&mut page, 36, XLOG_BLCKSZ as u32);

    // Insert the initial checkpoint record at SizeOfXLogLongPHD. (xlog.c:5191)
    let rec_off = SIZE_OF_XLOG_LONG_PHD;
    let cp_bytes = checkpoint_to_bytes(&check_point);
    debug_assert_eq!(cp_bytes.len(), SIZE_OF_CHECK_POINT);

    let xl_tot_len =
        (SIZE_OF_XLOG_RECORD + SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT + SIZE_OF_CHECK_POINT) as u32;

    // XLogRecord header (LP64): xl_tot_len@0(u32) xl_xid@4(u32) xl_prev@8(u64)
    // xl_info@16(u8) xl_rmid@17(u8) pad@18..20 xl_crc@20(u32).
    put_u32(&mut page, rec_off, xl_tot_len);
    put_u32(&mut page, rec_off + 4, InvalidTransactionId); // xl_xid
    put_u64(&mut page, rec_off + 8, 0); // xl_prev
    page[rec_off + 16] = XLOG_CHECKPOINT_SHUTDOWN; // xl_info
    page[rec_off + 17] = RM_XLOG_ID; // xl_rmid
    // xl_crc filled below.

    // XLogRecordDataHeaderShort: id_data_short, then 1-byte length.
    let data_hdr_off = rec_off + SIZE_OF_XLOG_RECORD;
    page[data_hdr_off] = XLR_BLOCK_ID_DATA_SHORT;
    page[data_hdr_off + 1] = SIZE_OF_CHECK_POINT as u8;

    // memcpy(recptr, &checkPoint, sizeof(checkPoint)).
    let cp_off = data_hdr_off + SIZE_OF_XLOG_RECORD_DATA_HEADER_SHORT;
    page[cp_off..cp_off + SIZE_OF_CHECK_POINT].copy_from_slice(&cp_bytes);
    debug_assert_eq!(cp_off + SIZE_OF_CHECK_POINT - rec_off, xl_tot_len as usize);

    // CRC: COMP over the record body (everything after SizeOfXLogRecord), then
    // over the header up to offsetof(XLogRecord, xl_crc) = 20. (xlog.c:5202-5206)
    let body_start = rec_off + SIZE_OF_XLOG_RECORD;
    let body_end = rec_off + xl_tot_len as usize;
    let mut crc = crc32c::pg_comp_crc32c_sb8(0xFFFF_FFFF, &page[body_start..body_end]);
    crc = crc32c::pg_comp_crc32c_sb8(crc, &page[rec_off..rec_off + 20]);
    crc ^= 0xFFFF_FFFF;
    put_u32(&mut page, rec_off + 20, crc); // xl_crc

    // Create the first XLOG segment file (logsegno=1, BootstrapTimeLineID). The
    // global openLogTLI/openLogFile bookkeeping C does is internal to the WAL
    // engine; here XLogFileInit returns the fd directly. (xlog.c:5209-5210)
    let fd = crate::write::XLogFileInit(1, BOOTSTRAP_TIME_LINE_ID)?;

    // Write the first page with the initial record. (xlog.c:5217-5230)
    let written = unsafe {
        libc::write(
            fd,
            page.as_ptr() as *const libc::c_void,
            XLOG_BLCKSZ as libc::size_t,
        )
    };
    if written != XLOG_BLCKSZ as isize {
        unsafe { libc::close(fd) };
        return ereport(PANIC)
            .errmsg("could not write bootstrap write-ahead log file")
            .finish(ErrorLocation::new("xlog.c", 5226, "BootStrapXLOG"))
            .map(|_| ());
    }

    // fsync (no-op when enableFsync is off, like C's pg_fsync). (xlog.c:5233)
    crate::write::issue_xlog_fsync(fd, 1, BOOTSTRAP_TIME_LINE_ID)?;

    if unsafe { libc::close(fd) } != 0 {
        return ereport(PANIC)
            .errmsg("could not close bootstrap write-ahead log file")
            .finish(ErrorLocation::new("xlog.c", 5243, "BootStrapXLOG"))
            .map(|_| ());
    }

    // Now create pg_control. (xlog.c:5249-5256)
    InitControlFile(sysidentifier, data_checksum_version)?;
    {
        let cf = control_file_mut();
        cf.time = check_point.time;
        cf.checkPoint = check_point.redo;
        cf.checkPointCopy = check_point;
    }

    // Some additional ControlFile fields are set in WriteControlFile().
    shmem::WriteControlFile()?;

    // Bootstrap the commit log, too. (xlog.c:5259-5262)
    transam_xlog_seams::boot_strap_clog::call()?;
    transam_xlog_seams::boot_strap_commit_ts::call()?;
    transam_xlog_seams::boot_strap_sub_trans::call()?;
    transam_xlog_seams::boot_strap_multi_xact::call()?;

    // Force control file to be read — runs the checks + GUC-related inits.
    // (xlog.c:5271)
    shmem::ReadControlFile()?;

    Ok(())
}

// ===========================================================================
// Little-endian-of-native byte-poke helpers over the in-memory WAL page bytes.
// The WAL on-disk image is native-endian.
// ===========================================================================

#[inline]
fn put_u16(b: &mut [u8], off: usize, v: u16) {
    b[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

#[inline]
fn put_u32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}

#[inline]
fn put_u64(b: &mut [u8], off: usize, v: u64) {
    b[off..off + 8].copy_from_slice(&v.to_ne_bytes());
}

/// `gettimeofday(&tv, NULL)` → `(tv_sec, tv_usec)`.
fn gettimeofday() -> (i64, i64) {
    let mut tv = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    unsafe {
        libc::gettimeofday(&mut tv, core::ptr::null_mut());
    }
    (tv.tv_sec as i64, tv.tv_usec as i64)
}

/// `getpid()`.
fn getpid() -> i32 {
    unsafe { libc::getpid() }
}

/// `(pg_time_t) time(NULL)` — current Unix time in seconds.
fn pg_time_now() -> i64 {
    let mut t: libc::time_t = 0;
    unsafe {
        libc::time(&mut t);
    }
    t as i64
}
