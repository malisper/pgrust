use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::{
    convert_and_check_filename, io_error, log_directory, time_t_to_timestamptz, tmpdir_path,
    PG_LOGICAL_MAPPINGS_DIR, PG_LOGICAL_SNAPSHOTS_DIR, XLOGDIR,
};

fn arg_filename(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    // SAFETY: these builtins are strict; arg i is a non-null text datum.
    let raw = unsafe { fcinfo.arg_varlena_packed(i) }?;
    Ok(String::from_utf8(raw.data().to_vec()).expect("non-UTF-8 filename"))
}

fn bytes_result(fcinfo: &Fcinfo, bytes: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(fcinfo.result_mcx(), bytes)?))
}

fn read_file(
    fcinfo: &mut Fcinfo,
    seek_offset: i64,
    bytes_to_read: i64,
    read_to_eof: bool,
    missing_ok: bool,
) -> PgResult<Datum> {
    let filename = arg_filename(fcinfo, 0)?;
    match crate::pg_read_file_common(&filename, seek_offset, bytes_to_read, read_to_eof, missing_ok)?
    {
        Some(buf) => bytes_result(fcinfo, &buf),
        None => Ok(fcinfo.return_null()),
    }
}

fn read_binary_file(
    fcinfo: &mut Fcinfo,
    seek_offset: i64,
    bytes_to_read: i64,
    read_to_eof: bool,
    missing_ok: bool,
) -> PgResult<Datum> {
    let filename = arg_filename(fcinfo, 0)?;
    match crate::pg_read_binary_file_common(
        &filename,
        seek_offset,
        bytes_to_read,
        read_to_eof,
        missing_ok,
    )? {
        Some(buf) => bytes_result(fcinfo, &buf),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_read_file_off_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (off, len) = (fcinfo.arg_i64(1), fcinfo.arg_i64(2));
    read_file(fcinfo, off, len, false, false)
}

pub fn fc_pg_read_file_off_len_missing(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (off, len, missing_ok) = (fcinfo.arg_i64(1), fcinfo.arg_i64(2), fcinfo.arg_bool(3));
    read_file(fcinfo, off, len, false, missing_ok)
}

pub fn fc_pg_read_file_all(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    read_file(fcinfo, 0, -1, true, false)
}

pub fn fc_pg_read_file_all_missing(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let missing_ok = fcinfo.arg_bool(1);
    read_file(fcinfo, 0, -1, true, missing_ok)
}

pub fn fc_pg_read_binary_file_off_len(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (off, len) = (fcinfo.arg_i64(1), fcinfo.arg_i64(2));
    read_binary_file(fcinfo, off, len, false, false)
}

pub fn fc_pg_read_binary_file_off_len_missing(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (off, len, missing_ok) = (fcinfo.arg_i64(1), fcinfo.arg_i64(2), fcinfo.arg_bool(3));
    read_binary_file(fcinfo, off, len, false, missing_ok)
}

pub fn fc_pg_read_binary_file_all(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    read_binary_file(fcinfo, 0, -1, true, false)
}

pub fn fc_pg_read_binary_file_all_missing(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let missing_ok = fcinfo.arg_bool(1);
    read_binary_file(fcinfo, 0, -1, true, missing_ok)
}


// (size, atime, mtime, ctime) in time_t seconds, C's pg_stat_file words.
#[cfg(not(target_family = "wasm"))]
fn stat_words(md: &std::fs::Metadata, _path: &str) -> (i64, i64, i64, i64) {
    use std::os::unix::fs::MetadataExt;
    (md.size() as i64, md.atime(), md.mtime(), md.ctime())
}

// wasm32: std's wasi MetadataExt exposes only dev/ino/nlink; the timestamp
// words come from wasi-libc's stat (st_atim/st_mtim/st_ctim carry the
// filestat_t fields std does not surface). Failure leaves epoch zeros —
// the file was stat-able a moment ago, so this arm is effectively dead.
#[cfg(target_family = "wasm")]
fn stat_words(md: &std::fs::Metadata, path: &str) -> (i64, i64, i64, i64) {
    let size = md.len() as i64;
    let Ok(c) = std::ffi::CString::new(path) else { return (size, 0, 0, 0) };
    // SAFETY: stat fills the zeroed out-param only on rc==0, which gates reads.
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        if libc::stat(c.as_ptr(), &mut st) == 0 {
            (size, st.st_atim.tv_sec as i64, st.st_mtim.tv_sec as i64, st.st_ctim.tv_sec as i64)
        } else {
            (size, 0, 0, 0)
        }
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_row_type() -> Box<PgError> {
    Box::new(PgError::error("return type must be a row type"))
}

fn stat_file(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    missing_ok: bool,
) -> PgResult<Datum> {
    let filename = convert_and_check_filename(&arg_filename(fcinfo, 0)?)?;

    let md = match std::fs::metadata(&filename) {
        Ok(md) => md,
        Err(e) => {
            if missing_ok && e.kind() == std::io::ErrorKind::NotFound {
                return Ok(fcinfo.return_null());
            }
            return Err(io_error(&e, format!("could not stat file \"{filename}\": %m")));
        }
    };

    let flinfo = flinfo.expect("pg_stat_file: resolved FmgrInfo required");
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(not_row_type());
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");

    let (f_size, f_atime, f_mtime, f_ctime) = stat_words(&md, &filename);
    let values = [
        Datum::from_i64(f_size),
        Datum::from_i64(time_t_to_timestamptz(f_atime)),
        Datum::from_i64(time_t_to_timestamptz(f_mtime)),
        Datum::from_i64(time_t_to_timestamptz(f_ctime)),
        Datum::null(),
        Datum::from_bool(md.is_dir()),
    ];
    // Unix: "change" is st_ctime, "creation" is NULL (C WIN32 branch inverted).
    let isnull = [false, false, false, false, true, false];

    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup);
    Ok(d)
}

pub fn fc_pg_stat_file(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let missing_ok = fcinfo.arg_bool(1);
    stat_file(flinfo, fcinfo, missing_ok)
}

pub fn fc_pg_stat_file_1arg(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    stat_file(flinfo, fcinfo, false)
}

fn open_dir_error(e: &std::io::Error, dir: &str) -> Box<PgError> {
    io_error(e, format!("could not open directory \"{dir}\": %m"))
}

fn read_dir_error(e: &std::io::Error, dir: &str) -> Box<PgError> {
    io_error(e, format!("could not read directory \"{dir}\": %m"))
}

fn entry_name(entry: std::fs::DirEntry) -> String {
    entry.file_name().into_string().expect("non-UTF-8 directory entry name")
}

fn ls_dir(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    three_args: bool,
) -> PgResult<Datum> {
    let location = convert_and_check_filename(&arg_filename(fcinfo, 0)?)?;

    let mut missing_ok = false;
    let mut include_dot_dirs = false;
    if three_args {
        if !fcinfo.argisnull(1) {
            missing_ok = fcinfo.arg_bool(1);
        }
        if !fcinfo.argisnull(2) {
            include_dot_dirs = fcinfo.arg_bool(2);
        }
    }

    let flinfo = flinfo.expect("pg_ls_dir: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf =
        funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, funcapi::MAT_SRF_USE_EXPECTED_DESC)?;

    let dir = match std::fs::read_dir(&location) {
        Ok(dir) => dir,
        Err(e) => {
            if missing_ok && e.kind() == std::io::ErrorKind::NotFound {
                return Ok(srf.finish(fcinfo));
            }
            return Err(open_dir_error(&e, &location));
        }
    };

    // C readdir yields "." and ".."; std::fs::read_dir omits them, so emit
    // them here (readdir order is unspecified anyway).
    if include_dot_dirs {
        for name in [".", ".."] {
            srf.putvalues(&[bytes_result(fcinfo, name.as_bytes())?], &[false])?;
        }
    }

    for entry in dir {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => return Err(read_dir_error(&e, &location)),
        };
        let name = entry_name(entry);
        srf.putvalues(&[bytes_result(fcinfo, name.as_bytes())?], &[false])?;
    }

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_ls_dir(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ls_dir(flinfo, fcinfo, true)
}

pub fn fc_pg_ls_dir_1arg(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ls_dir(flinfo, fcinfo, false)
}

fn ls_dir_files(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    dir: &str,
    missing_ok: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_ls_dir_files: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            if missing_ok && e.kind() == std::io::ErrorKind::NotFound {
                return Ok(srf.finish(fcinfo));
            }
            return Err(open_dir_error(&e, dir));
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => return Err(read_dir_error(&e, dir)),
        };
        let name = entry_name(entry);
        if name.starts_with('.') {
            continue;
        }

        let path = format!("{dir}/{name}");
        let md = match std::fs::metadata(&path) {
            Ok(md) => md,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    continue;
                }
                return Err(io_error(&e, format!("could not stat file \"{path}\": %m")));
            }
        };

        if !md.file_type().is_file() {
            continue;
        }

        let (e_size, _, e_mtime, _) = stat_words(&md, &path);
        let values = [
            bytes_result(fcinfo, name.as_bytes())?,
            Datum::from_i64(e_size),
            Datum::from_i64(time_t_to_timestamptz(e_mtime)),
        ];
        srf.putvalues(&values, &[false, false, false])?;
    }

    Ok(srf.finish(fcinfo))
}

pub fn fc_pg_ls_logdir(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let dir = log_directory();
    ls_dir_files(flinfo, fcinfo, &dir, false)
}

pub fn fc_pg_ls_waldir(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    ls_dir_files(flinfo, fcinfo, XLOGDIR, false)
}

pub fn fc_pg_ls_archive_statusdir(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ls_dir_files(flinfo, fcinfo, "pg_wal/archive_status", true)
}

pub fn fc_pg_ls_summariesdir(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ls_dir_files(flinfo, fcinfo, "pg_wal/summaries", true)
}

pub fn fc_pg_ls_tmpdir_noargs(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let dir = tmpdir_path(fd::temp::DEFAULTTABLESPACE_OID)?;
    ls_dir_files(flinfo, fcinfo, &dir, true)
}

pub fn fc_pg_ls_tmpdir_1arg(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let tblspc = fcinfo.arg(0).as_oid();
    let dir = tmpdir_path(tblspc)?;
    ls_dir_files(flinfo, fcinfo, &dir, true)
}

pub fn fc_pg_ls_logicalsnapdir(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ls_dir_files(flinfo, fcinfo, PG_LOGICAL_SNAPSHOTS_DIR, false)
}

pub fn fc_pg_ls_logicalmapdir(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    ls_dir_files(flinfo, fcinfo, PG_LOGICAL_MAPPINGS_DIR, false)
}

pub fn fc_pg_ls_replslotdir(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let slotname = arg_filename(fcinfo, 0)?;
    if slot::SearchNamedReplicationSlot(&slotname, true)?.is_none() {
        return Err(Box::new(
            elog::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("replication slot \"{slotname}\" does not exist"))
                .into_error(),
        ));
    }
    let dir = format!("{}/{slotname}", slot::PG_REPLSLOT_DIR);
    ls_dir_files(flinfo, fcinfo, &dir, false)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

// pg_proc.dat rows (all proisstrict), OID-ascending.
pub const GENFILE_BUILTINS: &[FmgrBuiltin] = &[
    b(2623, "pg_stat_file_1arg", 1, fc_pg_stat_file_1arg),
    b(2624, "pg_read_file_off_len", 3, fc_pg_read_file_off_len),
    srf(2625, "pg_ls_dir_1arg", 1, fc_pg_ls_dir_1arg),
    b(3293, "pg_read_file_off_len_missing", 4, fc_pg_read_file_off_len_missing),
    b(3295, "pg_read_binary_file_off_len_missing", 4, fc_pg_read_binary_file_off_len_missing),
    srf(3297, "pg_ls_dir", 3, fc_pg_ls_dir),
    b(3307, "pg_stat_file", 2, fc_pg_stat_file),
    srf(3353, "pg_ls_logdir", 0, fc_pg_ls_logdir),
    srf(3354, "pg_ls_waldir", 0, fc_pg_ls_waldir),
    b(3826, "pg_read_file_all", 1, fc_pg_read_file_all),
    b(3827, "pg_read_binary_file_off_len", 3, fc_pg_read_binary_file_off_len),
    b(3828, "pg_read_binary_file_all", 1, fc_pg_read_binary_file_all),
    srf(5029, "pg_ls_tmpdir_noargs", 0, fc_pg_ls_tmpdir_noargs),
    srf(5030, "pg_ls_tmpdir_1arg", 1, fc_pg_ls_tmpdir_1arg),
    srf(5031, "pg_ls_archive_statusdir", 0, fc_pg_ls_archive_statusdir),
    b(6208, "pg_read_file_all_missing", 2, fc_pg_read_file_all_missing),
    b(6209, "pg_read_binary_file_all_missing", 2, fc_pg_read_binary_file_all_missing),
    srf(6270, "pg_ls_logicalsnapdir", 0, fc_pg_ls_logicalsnapdir),
    srf(6271, "pg_ls_logicalmapdir", 0, fc_pg_ls_logicalmapdir),
    srf(6272, "pg_ls_replslotdir", 1, fc_pg_ls_replslotdir),
    srf(6400, "pg_ls_summariesdir", 0, fc_pg_ls_summariesdir),
];
