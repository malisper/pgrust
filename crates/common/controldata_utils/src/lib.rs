//! src/common/controldata_utils.c — get_controlfile / update_controlfile over
//! the pg_control.h on-disk image (byte-exact; every offset asserted vs C).
//! Struct declarations mirror transam_xlog::control_file field-for-field so
//! the two converge when the xlog lane adopts this unit.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use core::mem::{offset_of, size_of, size_of_val};

use crc32c::{fin_crc32c, pg_comp_crc32c, CRC32C_INIT};
use elog::ereport;
use types_core::{
    pg_time_t, FullTransactionId, MultiXactId, MultiXactOffset, Oid, TimeLineID, TransactionId,
    XLogRecPtr,
};
use types_error::{ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, PANIC};

pub const PG_CONTROL_VERSION: u32 = 1800;
pub const CATALOG_VERSION_NO: u32 = 202506291;
pub const PG_CONTROL_FILE_SIZE: usize = 8192;
pub const PG_CONTROL_MAX_SAFE_SIZE: usize = 512;
pub const MOCK_AUTH_NONCE_LEN: usize = 32;
pub const XLOG_CONTROL_FILE: &str = "global/pg_control";

pub type DBState = i32;
pub const DB_STARTUP: DBState = 0;
pub const DB_SHUTDOWNED: DBState = 1;
pub const DB_SHUTDOWNED_IN_RECOVERY: DBState = 2;
pub const DB_SHUTDOWNING: DBState = 3;
pub const DB_IN_CRASH_RECOVERY: DBState = 4;
pub const DB_IN_ARCHIVE_RECOVERY: DBState = 5;
pub const DB_IN_PRODUCTION: DBState = 6;

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

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
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

pub const SIZEOF_CONTROL_FILE_DATA: usize = 296;
pub const OFFSETOF_CRC: usize = 292;
pub const SIZEOF_CHECKPOINT: usize = 88;

// Offsets from a clang offsetof dump of catalog/pg_control.h (REL_18_3, LP64);
// the CRC and the on-disk image depend on every one of them.
const _: () = {
    assert!(size_of::<ControlFileData>() == SIZEOF_CONTROL_FILE_DATA);
    assert!(size_of::<CheckPoint>() == SIZEOF_CHECKPOINT);
    assert!(size_of::<ControlFileData>() <= PG_CONTROL_MAX_SAFE_SIZE);
    assert!(offset_of!(ControlFileData, system_identifier) == 0);
    assert!(offset_of!(ControlFileData, pg_control_version) == 8);
    assert!(offset_of!(ControlFileData, catalog_version_no) == 12);
    assert!(offset_of!(ControlFileData, state) == 16);
    assert!(offset_of!(ControlFileData, time) == 24);
    assert!(offset_of!(ControlFileData, checkPoint) == 32);
    assert!(offset_of!(ControlFileData, checkPointCopy) == 40);
    assert!(offset_of!(ControlFileData, unloggedLSN) == 128);
    assert!(offset_of!(ControlFileData, minRecoveryPoint) == 136);
    assert!(offset_of!(ControlFileData, minRecoveryPointTLI) == 144);
    assert!(offset_of!(ControlFileData, backupStartPoint) == 152);
    assert!(offset_of!(ControlFileData, backupEndPoint) == 160);
    assert!(offset_of!(ControlFileData, backupEndRequired) == 168);
    assert!(offset_of!(ControlFileData, wal_level) == 172);
    assert!(offset_of!(ControlFileData, wal_log_hints) == 176);
    assert!(offset_of!(ControlFileData, MaxConnections) == 180);
    assert!(offset_of!(ControlFileData, max_worker_processes) == 184);
    assert!(offset_of!(ControlFileData, max_wal_senders) == 188);
    assert!(offset_of!(ControlFileData, max_prepared_xacts) == 192);
    assert!(offset_of!(ControlFileData, max_locks_per_xact) == 196);
    assert!(offset_of!(ControlFileData, track_commit_timestamp) == 200);
    assert!(offset_of!(ControlFileData, maxAlign) == 204);
    assert!(offset_of!(ControlFileData, floatFormat) == 208);
    assert!(offset_of!(ControlFileData, blcksz) == 216);
    assert!(offset_of!(ControlFileData, relseg_size) == 220);
    assert!(offset_of!(ControlFileData, xlog_blcksz) == 224);
    assert!(offset_of!(ControlFileData, xlog_seg_size) == 228);
    assert!(offset_of!(ControlFileData, nameDataLen) == 232);
    assert!(offset_of!(ControlFileData, indexMaxKeys) == 236);
    assert!(offset_of!(ControlFileData, toast_max_chunk_size) == 240);
    assert!(offset_of!(ControlFileData, loblksize) == 244);
    assert!(offset_of!(ControlFileData, float8ByVal) == 248);
    assert!(offset_of!(ControlFileData, data_checksum_version) == 252);
    assert!(offset_of!(ControlFileData, default_char_signedness) == 256);
    assert!(offset_of!(ControlFileData, mock_authentication_nonce) == 257);
    assert!(offset_of!(ControlFileData, crc) == 292);
    assert!(offset_of!(CheckPoint, redo) == 0);
    assert!(offset_of!(CheckPoint, ThisTimeLineID) == 8);
    assert!(offset_of!(CheckPoint, PrevTimeLineID) == 12);
    assert!(offset_of!(CheckPoint, fullPageWrites) == 16);
    assert!(offset_of!(CheckPoint, wal_level) == 20);
    assert!(offset_of!(CheckPoint, nextXid) == 24);
    assert!(offset_of!(CheckPoint, nextOid) == 32);
    assert!(offset_of!(CheckPoint, nextMulti) == 36);
    assert!(offset_of!(CheckPoint, nextMultiOffset) == 40);
    assert!(offset_of!(CheckPoint, oldestXid) == 44);
    assert!(offset_of!(CheckPoint, oldestXidDB) == 48);
    assert!(offset_of!(CheckPoint, oldestMulti) == 52);
    assert!(offset_of!(CheckPoint, oldestMultiDB) == 56);
    assert!(offset_of!(CheckPoint, time) == 64);
    assert!(offset_of!(CheckPoint, oldestCommitTsXid) == 72);
    assert!(offset_of!(CheckPoint, newestCommitTsXid) == 76);
    assert!(offset_of!(CheckPoint, oldestActiveXid) == 80);
};

macro_rules! put {
    ($out:expr, $off:expr, $v:expr) => {{
        let v = $v;
        $out[$off..$off + size_of_val(&v)].copy_from_slice(&v.to_ne_bytes())
    }};
}

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

    pub fn to_bytes(&self) -> [u8; SIZEOF_CHECKPOINT] {
        let mut b = [0u8; SIZEOF_CHECKPOINT];
        self.write_bytes(&mut b);
        b
    }

    pub fn from_bytes(b: &[u8]) -> CheckPoint {
        macro_rules! o {
            ($f:ident) => {
                offset_of!(CheckPoint, $f)
            };
        }
        CheckPoint {
            redo: get_u64(b, o!(redo)),
            ThisTimeLineID: get_u32(b, o!(ThisTimeLineID)),
            PrevTimeLineID: get_u32(b, o!(PrevTimeLineID)),
            fullPageWrites: get_bool(b, o!(fullPageWrites)),
            wal_level: get_i32(b, o!(wal_level)),
            nextXid: FullTransactionId {
                value: get_u64(b, o!(nextXid)),
            },
            nextOid: get_u32(b, o!(nextOid)),
            nextMulti: get_u32(b, o!(nextMulti)),
            nextMultiOffset: get_u32(b, o!(nextMultiOffset)),
            oldestXid: get_u32(b, o!(oldestXid)),
            oldestXidDB: get_u32(b, o!(oldestXidDB)),
            oldestMulti: get_u32(b, o!(oldestMulti)),
            oldestMultiDB: get_u32(b, o!(oldestMultiDB)),
            time: get_i64(b, o!(time)),
            oldestCommitTsXid: get_u32(b, o!(oldestCommitTsXid)),
            newestCommitTsXid: get_u32(b, o!(newestCommitTsXid)),
            oldestActiveXid: get_u32(b, o!(oldestActiveXid)),
        }
    }

    pub fn write_bytes(&self, out: &mut [u8]) {
        macro_rules! o {
            ($f:ident) => {
                offset_of!(CheckPoint, $f)
            };
        }
        put!(out, o!(redo), self.redo);
        put!(out, o!(ThisTimeLineID), self.ThisTimeLineID);
        put!(out, o!(PrevTimeLineID), self.PrevTimeLineID);
        out[o!(fullPageWrites)] = self.fullPageWrites as u8;
        put!(out, o!(wal_level), self.wal_level);
        put!(out, o!(nextXid), self.nextXid.value);
        put!(out, o!(nextOid), self.nextOid);
        put!(out, o!(nextMulti), self.nextMulti);
        put!(out, o!(nextMultiOffset), self.nextMultiOffset);
        put!(out, o!(oldestXid), self.oldestXid);
        put!(out, o!(oldestXidDB), self.oldestXidDB);
        put!(out, o!(oldestMulti), self.oldestMulti);
        put!(out, o!(oldestMultiDB), self.oldestMultiDB);
        put!(out, o!(time), self.time);
        put!(out, o!(oldestCommitTsXid), self.oldestCommitTsXid);
        put!(out, o!(newestCommitTsXid), self.newestCommitTsXid);
        put!(out, o!(oldestActiveXid), self.oldestActiveXid);
    }
}

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

    pub fn from_disk_bytes(b: &[u8]) -> ControlFileData {
        assert!(b.len() >= SIZEOF_CONTROL_FILE_DATA);
        macro_rules! o {
            ($f:ident) => {
                offset_of!(ControlFileData, $f)
            };
        }
        let mut nonce = [0u8; MOCK_AUTH_NONCE_LEN];
        nonce.copy_from_slice(
            &b[o!(mock_authentication_nonce)..o!(mock_authentication_nonce) + MOCK_AUTH_NONCE_LEN],
        );
        ControlFileData {
            system_identifier: get_u64(b, o!(system_identifier)),
            pg_control_version: get_u32(b, o!(pg_control_version)),
            catalog_version_no: get_u32(b, o!(catalog_version_no)),
            state: get_i32(b, o!(state)),
            time: get_i64(b, o!(time)),
            checkPoint: get_u64(b, o!(checkPoint)),
            checkPointCopy: CheckPoint::from_bytes(&b[o!(checkPointCopy)..]),
            unloggedLSN: get_u64(b, o!(unloggedLSN)),
            minRecoveryPoint: get_u64(b, o!(minRecoveryPoint)),
            minRecoveryPointTLI: get_u32(b, o!(minRecoveryPointTLI)),
            backupStartPoint: get_u64(b, o!(backupStartPoint)),
            backupEndPoint: get_u64(b, o!(backupEndPoint)),
            backupEndRequired: get_bool(b, o!(backupEndRequired)),
            wal_level: get_i32(b, o!(wal_level)),
            wal_log_hints: get_bool(b, o!(wal_log_hints)),
            MaxConnections: get_i32(b, o!(MaxConnections)),
            max_worker_processes: get_i32(b, o!(max_worker_processes)),
            max_wal_senders: get_i32(b, o!(max_wal_senders)),
            max_prepared_xacts: get_i32(b, o!(max_prepared_xacts)),
            max_locks_per_xact: get_i32(b, o!(max_locks_per_xact)),
            track_commit_timestamp: get_bool(b, o!(track_commit_timestamp)),
            maxAlign: get_u32(b, o!(maxAlign)),
            floatFormat: get_f64(b, o!(floatFormat)),
            blcksz: get_u32(b, o!(blcksz)),
            relseg_size: get_u32(b, o!(relseg_size)),
            xlog_blcksz: get_u32(b, o!(xlog_blcksz)),
            xlog_seg_size: get_u32(b, o!(xlog_seg_size)),
            nameDataLen: get_u32(b, o!(nameDataLen)),
            indexMaxKeys: get_u32(b, o!(indexMaxKeys)),
            toast_max_chunk_size: get_u32(b, o!(toast_max_chunk_size)),
            loblksize: get_u32(b, o!(loblksize)),
            float8ByVal: get_bool(b, o!(float8ByVal)),
            data_checksum_version: get_u32(b, o!(data_checksum_version)),
            default_char_signedness: get_bool(b, o!(default_char_signedness)),
            mock_authentication_nonce: nonce,
            crc: get_u32(b, o!(crc)),
        }
    }

    /// The exact byte image C writes (padding holes zeroed, matching the C
    /// side's zeroed shmem/buffer images).
    pub fn to_disk_bytes(&self) -> [u8; SIZEOF_CONTROL_FILE_DATA] {
        macro_rules! o {
            ($f:ident) => {
                offset_of!(ControlFileData, $f)
            };
        }
        let mut b = [0u8; SIZEOF_CONTROL_FILE_DATA];
        put!(b, o!(system_identifier), self.system_identifier);
        put!(b, o!(pg_control_version), self.pg_control_version);
        put!(b, o!(catalog_version_no), self.catalog_version_no);
        put!(b, o!(state), self.state);
        put!(b, o!(time), self.time);
        put!(b, o!(checkPoint), self.checkPoint);
        self.checkPointCopy
            .write_bytes(&mut b[o!(checkPointCopy)..o!(checkPointCopy) + SIZEOF_CHECKPOINT]);
        put!(b, o!(unloggedLSN), self.unloggedLSN);
        put!(b, o!(minRecoveryPoint), self.minRecoveryPoint);
        put!(b, o!(minRecoveryPointTLI), self.minRecoveryPointTLI);
        put!(b, o!(backupStartPoint), self.backupStartPoint);
        put!(b, o!(backupEndPoint), self.backupEndPoint);
        b[o!(backupEndRequired)] = self.backupEndRequired as u8;
        put!(b, o!(wal_level), self.wal_level);
        b[o!(wal_log_hints)] = self.wal_log_hints as u8;
        put!(b, o!(MaxConnections), self.MaxConnections);
        put!(b, o!(max_worker_processes), self.max_worker_processes);
        put!(b, o!(max_wal_senders), self.max_wal_senders);
        put!(b, o!(max_prepared_xacts), self.max_prepared_xacts);
        put!(b, o!(max_locks_per_xact), self.max_locks_per_xact);
        b[o!(track_commit_timestamp)] = self.track_commit_timestamp as u8;
        put!(b, o!(maxAlign), self.maxAlign);
        put!(b, o!(floatFormat), self.floatFormat);
        put!(b, o!(blcksz), self.blcksz);
        put!(b, o!(relseg_size), self.relseg_size);
        put!(b, o!(xlog_blcksz), self.xlog_blcksz);
        put!(b, o!(xlog_seg_size), self.xlog_seg_size);
        put!(b, o!(nameDataLen), self.nameDataLen);
        put!(b, o!(indexMaxKeys), self.indexMaxKeys);
        put!(b, o!(toast_max_chunk_size), self.toast_max_chunk_size);
        put!(b, o!(loblksize), self.loblksize);
        b[o!(float8ByVal)] = self.float8ByVal as u8;
        put!(b, o!(data_checksum_version), self.data_checksum_version);
        b[o!(default_char_signedness)] = self.default_char_signedness as u8;
        b[o!(mock_authentication_nonce)..o!(mock_authentication_nonce) + MOCK_AUTH_NONCE_LEN]
            .copy_from_slice(&self.mock_authentication_nonce);
        put!(b, o!(crc), self.crc);
        b
    }
}

pub fn crc_of_image(image: &[u8]) -> u32 {
    fin_crc32c(pg_comp_crc32c(CRC32C_INIT, &image[..OFFSETOF_CRC]))
}

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn c_path(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path.as_bytes()).unwrap_or_else(|_| std::ffi::CString::new("").unwrap())
}

/// get_controlfile(DataDir, &crc_ok): the CRC verdict is returned, not raised.
pub fn get_controlfile(datadir: &str) -> PgResult<(ControlFileData, bool)> {
    get_controlfile_by_exact_path(&format!("{datadir}/{XLOG_CONTROL_FILE}"))
}

pub fn get_controlfile_by_exact_path(path: &str) -> PgResult<(ControlFileData, bool)> {
    const F: &str = "get_controlfile_by_exact_path";

    let cpath = c_path(path);
    let fd = vfs::open(&cpath, libc::O_RDONLY, 0);
    if fd < 0 {
        let e = std::io::Error::from_raw_os_error(vfs::get_errno());
        ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\" for reading: {e}"))
            .finish(loc(F))?;
        unreachable!()
    }

    let mut image = [0u8; SIZEOF_CONTROL_FILE_DATA];
    let mut r = 0usize;
    while r < SIZEOF_CONTROL_FILE_DATA {
        let n = vfs::pread(fd, &mut image[r..], r as libc::off_t);
        if n == 0 {
            break;
        }
        if n < 0 {
            let en = vfs::get_errno();
            if en == libc::EINTR {
                continue;
            }
            let e = std::io::Error::from_raw_os_error(en);
            vfs::close(fd);
            ereport(ERROR)
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": {e}"))
                .finish(loc(F))?;
            unreachable!()
        }
        r += n as usize;
    }
    vfs::close(fd);
    if r != SIZEOF_CONTROL_FILE_DATA {
        ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read file \"{path}\": read {r} of {SIZEOF_CONTROL_FILE_DATA}"
            ))
            .finish(loc(F))?;
        unreachable!()
    }

    let control_file = ControlFileData::from_disk_bytes(&image);
    let crc_ok = crc_of_image(&image) == control_file.crc;

    if control_file.pg_control_version % 65536 == 0 && control_file.pg_control_version / 65536 != 0
    {
        ereport(ERROR).errmsg("byte ordering mismatch").finish(loc(F))?;
        unreachable!()
    }

    Ok((control_file, crc_ok))
}

/// update_controlfile(DataDir, ControlFile, do_sync). Backend flavor: all I/O
/// failures are PANIC; the caller holds ControlFileLock.
pub fn update_controlfile(
    datadir: &str,
    control_file: &mut ControlFileData,
    do_sync: bool,
) -> PgResult<()> {
    const F: &str = "update_controlfile";

    // DST P2 (contract §1.2): control-file stamp on pg_clock::wall_secs().
    control_file.time = pg_clock::wall_secs() as pg_time_t;

    let mut image = control_file.to_disk_bytes();
    let crc = crc_of_image(&image);
    control_file.crc = crc;
    put!(image, OFFSETOF_CRC, crc);

    // Write PG_CONTROL_FILE_SIZE bytes, zero-padding the excess, to avoid
    // premature-EOF errors on read.
    let mut buffer = [0u8; PG_CONTROL_FILE_SIZE];
    buffer[..SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&image);

    let path = format!("{datadir}/{XLOG_CONTROL_FILE}");
    let cpath = c_path(&path);
    let fd = vfs::open(&cpath, libc::O_RDWR, 0);
    if fd < 0 {
        let e = std::io::Error::from_raw_os_error(vfs::get_errno());
        return ereport(PANIC)
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": {e}"))
            .finish(loc(F));
    }

    // Single pwrite of the whole PG_CONTROL_FILE_SIZE image at offset 0 —
    // the 512 B sector-atomicity floor (PG_CONTROL_MAX_SAFE_SIZE) rides one
    // syscall, as before.
    let mut written = 0usize;
    while written < buffer.len() {
        let w = vfs::pwrite(fd, &buffer[written..], written as libc::off_t);
        if w < 0 {
            let en = vfs::get_errno();
            if en == libc::EINTR {
                continue;
            }
            let e = std::io::Error::from_raw_os_error(en);
            vfs::close(fd);
            return ereport(PANIC)
                .errcode_for_file_access()
                .errmsg(format!("could not write file \"{path}\": {e}"))
                .finish(loc(F));
        }
        written += w as usize;
    }

    if do_sync && vfs::fsync(fd) != 0 {
        let e = std::io::Error::from_raw_os_error(vfs::get_errno());
        vfs::close(fd);
        return ereport(PANIC)
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{path}\": {e}"))
            .finish(loc(F));
    }
    vfs::close(fd);

    Ok(())
}
