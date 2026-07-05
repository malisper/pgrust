mod builtins;
pub use builtins::GENFILE_BUILTINS;

use std::io::{Read, Seek, SeekFrom};

use adt_datetime::{POSTGRES_EPOCH_JDATE, SECS_PER_DAY, UNIX_EPOCH_JDATE, USECS_PER_SEC};
use datum::Datum;
use elog::ereport;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_OBJECT, ERROR,
};

const ROLE_PG_READ_SERVER_FILES: Oid = 4569;
// MaxAllocSize (memutils.h): 1 GB - 1.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;
const VARHDRSZ: usize = 4;
pub(crate) const XLOGDIR: &str = "pg_wal";
pub(crate) const PG_LOGICAL_SNAPSHOTS_DIR: &str = "pg_logical/snapshots";
pub(crate) const PG_LOGICAL_MAPPINGS_DIR: &str = "pg_logical/mappings";

pub(crate) fn io_error(e: &std::io::Error, message: String) -> Box<PgError> {
    let mut builder = ereport(ERROR);
    if let Some(errno) = e.raw_os_error() {
        builder = builder.with_saved_errno(errno);
    }
    Box::new(builder.errcode_for_file_access().errmsg(message).into_error())
}

fn path_is_prefix_of_path(path1: &str, path2: &str) -> bool {
    match path2.strip_prefix(path1) {
        Some(rest) => rest.is_empty() || rest.starts_with('/'),
        None => false,
    }
}

// path must be canonicalized already (path.c path_contains_parent_reference).
fn path_contains_parent_reference(path: &str) -> bool {
    path == ".." || path.starts_with("../") || path.contains("/../") || path.ends_with("/..")
}

fn path_is_relative_and_below_cwd(path: &str) -> bool {
    !pg_path::is_absolute_path(path) && !path_contains_parent_reference(path)
}

// C reads the Log_directory global directly; the slot's owner (syslogger) may
// not be linked, in which case the GUC boot value "log" applies.
pub(crate) fn log_directory() -> String {
    if guc_tables::vars::Log_directory.installed() {
        if let Some(dir) = guc_tables::vars::Log_directory.read() {
            return dir;
        }
    }
    "log".to_string()
}

pub(crate) fn convert_and_check_filename(arg: &str) -> PgResult<String> {
    let filename = pg_path::canonicalize_path(arg);

    if acl_seams::has_privs_of_role::call(miscinit::GetUserId(), ROLE_PG_READ_SERVER_FILES)? {
        return Ok(filename);
    }

    if pg_path::is_absolute_path(&filename) {
        let data_dir =
            init_small::globals::DataDir().expect("DataDir must be set (C Assert in path checks)");
        let log_dir = log_directory();
        if !path_is_prefix_of_path(data_dir, &filename)
            && (!pg_path::is_absolute_path(&log_dir)
                || !path_is_prefix_of_path(&log_dir, &filename))
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("absolute path not allowed")
                .into_error()
                .into());
        }
    } else if !path_is_relative_and_below_cwd(&filename) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("path must be in or below the data directory")
            .into_error()
            .into());
    }

    Ok(filename)
}

fn read_binary_file(
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    missing_ok: bool,
) -> PgResult<Option<Vec<u8>>> {
    if bytes_to_read > (MAX_ALLOC_SIZE - VARHDRSZ) as i64 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("requested length too large")
            .into_error()
            .into());
    }

    let mut file = match std::fs::File::open(filename) {
        Ok(f) => f,
        Err(e) => {
            if missing_ok && e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(io_error(
                &e,
                format!("could not open file \"{filename}\" for reading: %m"),
            ));
        }
    };

    let whence = if seek_offset >= 0 {
        SeekFrom::Start(seek_offset as u64)
    } else {
        SeekFrom::End(seek_offset)
    };
    if let Err(e) = file.seek(whence) {
        return Err(io_error(&e, format!("could not seek in file \"{filename}\": %m")));
    }

    let buf = if bytes_to_read >= 0 {
        let mut buf = vec![0u8; bytes_to_read as usize];
        let mut nbytes = 0;
        while nbytes < buf.len() {
            match file.read(&mut buf[nbytes..]) {
                Ok(0) => break,
                Ok(n) => nbytes += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(io_error(&e, format!("could not read file \"{filename}\": %m")))
                }
            }
        }
        buf.truncate(nbytes);
        buf
    } else {
        // C caps the whole-file StringInfo at MaxAllocSize - 1 bytes including
        // the reserved varlena header.
        let limit = MAX_ALLOC_SIZE - 1 - VARHDRSZ;
        let mut buf = Vec::new();
        if let Err(e) = file.by_ref().take(limit as u64 + 1).read_to_end(&mut buf) {
            return Err(io_error(&e, format!("could not read file \"{filename}\": %m")));
        }
        if buf.len() > limit {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg("file length too large")
                .into_error()
                .into());
        }
        buf
    };

    Ok(Some(buf))
}

fn read_text_file(
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    missing_ok: bool,
) -> PgResult<Option<Vec<u8>>> {
    let buf = read_binary_file(filename, seek_offset, bytes_to_read, missing_ok)?;
    if let Some(buf) = &buf {
        mbutils::pg_verifymbstr(buf, false)?;
    }
    Ok(buf)
}

fn negative_length() -> Box<PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("requested length cannot be negative")
            .into_error(),
    )
}

pub(crate) fn pg_read_file_common(
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    read_to_eof: bool,
    missing_ok: bool,
) -> PgResult<Option<Vec<u8>>> {
    if read_to_eof {
        debug_assert_eq!(bytes_to_read, -1);
    } else if bytes_to_read < 0 {
        return Err(negative_length());
    }
    read_text_file(&convert_and_check_filename(filename)?, seek_offset, bytes_to_read, missing_ok)
}

pub(crate) fn pg_read_binary_file_common(
    filename: &str,
    seek_offset: i64,
    bytes_to_read: i64,
    read_to_eof: bool,
    missing_ok: bool,
) -> PgResult<Option<Vec<u8>>> {
    if read_to_eof {
        debug_assert_eq!(bytes_to_read, -1);
    } else if bytes_to_read < 0 {
        return Err(negative_length());
    }
    read_binary_file(&convert_and_check_filename(filename)?, seek_offset, bytes_to_read, missing_ok)
}

// pg_ls_tmpdir (genfile.c:649): TABLESPACEOID existence check + TempTablespacePath.
pub(crate) fn tmpdir_path(tblspc: Oid) -> PgResult<String> {
    use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheKey, TABLESPACEOID};
    match SearchSysCache1(TABLESPACEOID, SysCacheKey::Value(Datum::from_oid(tblspc)))? {
        Some(ht) => ReleaseSysCache(ht),
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("tablespace with OID {tblspc} does not exist"))
                .into_error()
                .into())
        }
    }
    Ok(fd::TempTablespacePath(tblspc))
}

pub(crate) fn time_t_to_timestamptz(tm: i64) -> i64 {
    (tm - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) as i64 * SECS_PER_DAY as i64) * USECS_PER_SEC
}
