//! Port of `src/backend/libpq/be-secure-common.c`.
//!
//! Common, implementation-independent SSL support code shared by the
//! library-specific implementations (e.g. `be-secure-openssl.c`):
//! `run_ssl_passphrase_command` (run the `ssl_passphrase_command` GUC and read
//! the passphrase from its pipe) and `check_ssl_key_file_permissions` (enforce
//! the SSL private key file's ownership/mode rules).
//!
//! The pipe machinery (`OpenPipeStream`/`fgets`/`ClosePipeStream`) belongs to
//! `fd.c` and is reached through its seam crate. `stat`/`geteuid` are plain
//! POSIX syscalls (no PG translation unit owns them) and are issued directly,
//! matching the sibling ports (xlogarchive.c, miscinit.c). `wait_result_to_str`
//! is reached through its (unported) owner's seam.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use be_secure_common_seams as my_seams;
use fd_seams as fd_seams;
use ::fd_seams::PipeReadLine;
use ::utils_error::{ereport, ErrorLevel};
use ::guc_tables::{vars, GucVarAccessors};
use ::percentrepl::replace_percent_placeholders;
use ::string::pg_strip_crlf;
use ::mcx::{Mcx, PgVec};
use ::types_error::{ErrorLocation, PgResult, ERRCODE_CONFIG_FILE_ERROR, ERROR, FATAL, LOG};

const FILENAME: &str = "../src/backend/libpq/be-secure-common.c";

/// `geteuid()` — the calling process's effective user id (a plain POSIX
/// syscall; no PG translation unit owns it).
fn geteuid() -> u32 {
    // SAFETY: geteuid is always-successful and takes no arguments.
    unsafe { libc::geteuid() }
}

/// `stat(path, &buf)` — returns the file's `st_mode`/`st_uid` on success, or
/// `Err(errno)` on failure. A plain POSIX syscall (cf. xlogarchive.c).
fn raw_stat(path: &str) -> Result<(u32, u32), i32> {
    let cpath = match std::ffi::CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => return Err(libc::ENAMETOOLONG),
    };
    let mut sb: libc::stat = unsafe { core::mem::zeroed() };
    let rc = unsafe { libc::stat(cpath.as_ptr(), &mut sb) };
    if rc == 0 {
        Ok((sb.st_mode as u32, sb.st_uid as u32))
    } else {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(0))
    }
}

// POSIX `<sys/stat.h>` file-mode constants.
const S_IFMT: u32 = 0o170000;
const S_IFREG: u32 = 0o100000;
const S_IRWXG: u32 = 0o000070;
const S_IRWXO: u32 = 0o000007;
const S_IWGRP: u32 = 0o000020;
const S_IXGRP: u32 = 0o000010;

#[inline]
fn s_isreg(m: u32) -> bool {
    (m & S_IFMT) == S_IFREG
}

/// Run `ssl_passphrase_command`.
///
/// `prompt` is substituted for `%p`; `is_server_start` selects the loglevel of
/// error messages (`ERROR` vs `LOG`). Returns the passphrase bytes (already
/// stripped of trailing CR/LF, capped at `size - 1` bytes); the length is the C
/// return value. A `LOG`-level failure returns an empty buffer (C's
/// fall-through with `len == 0`); an `ERROR`-level failure propagates as `Err`.
pub fn run_ssl_passphrase_command<'mcx>(
    mcx: Mcx<'mcx>,
    prompt: &str,
    is_server_start: bool,
    size: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let loglevel: ErrorLevel = if is_server_start { ERROR } else { LOG };

    // Assert(prompt); Assert(size > 0);
    debug_assert!(size > 0);

    // command = replace_percent_placeholders(ssl_passphrase_command,
    //                                         "ssl_passphrase_command", "p", prompt);
    // C reads the GUC string; an empty/NULL GUC yields an empty template, which
    // replace_percent_placeholders handles. A bad template ereport(ERROR)s
    // (carried on Err unconditionally, matching C).
    let ssl_passphrase_command = vars::ssl_passphrase_command.read().unwrap_or_default();
    let command = replace_percent_placeholders(
        mcx,
        &ssl_passphrase_command,
        "ssl_passphrase_command",
        &[('p', Some(prompt))],
    )?;
    let command: &str = command.as_str();

    // buf[0] = '\0'; — the result accumulator starts empty.
    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);

    // fh = OpenPipeStream(command, "r");
    let fh = match fd_seams::open_pipe_stream_read::call(command)? {
        Some(fh) => fh,
        None => {
            // ereport(loglevel, errcode_for_file_access(),
            //         "could not execute command \"%s\": %m");
            let errno = fd_seams::last_errno::call();
            report(loglevel, "run_ssl_passphrase_command", |b| {
                b.with_saved_errno(errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not execute command \"{command}\": %m"))
            })?;
            return Ok(buf); // goto error;
        }
    };

    // if (!fgets(buf, size, fh)) { if (ferror(fh)) { ... goto error; } }
    match fd_seams::pipe_read_line::call(fh, size)? {
        PipeReadLine::Line(bytes) => {
            // fgets copied data into buf (up to size-1 bytes).
            let n = bytes.len().min((size - 1).max(0) as usize);
            buf = ::mcx::slice_in(mcx, &bytes[..n])?;
        }
        PipeReadLine::Eof => {
            // fgets returned NULL but !ferror: nothing read, fall through.
        }
        PipeReadLine::Error(errno) => {
            // explicit_bzero(buf, size);
            explicit_bzero(&mut buf);
            report(loglevel, "run_ssl_passphrase_command", |b| {
                b.with_saved_errno(errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not read from command \"{command}\": %m"))
            })?;
            return Ok(buf); // goto error;
        }
    }

    // pclose_rc = ClosePipeStream(fh);
    let pclose_rc = fd_seams::close_pipe_stream::call(fh)?;
    if pclose_rc == -1 {
        // explicit_bzero(buf, size);
        explicit_bzero(&mut buf);
        let errno = fd_seams::last_errno::call();
        report(loglevel, "run_ssl_passphrase_command", |b| {
            b.with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg("could not close pipe to external command: %m")
        })?;
        return Ok(buf); // goto error;
    } else if pclose_rc != 0 {
        // explicit_bzero(buf, size);
        explicit_bzero(&mut buf);
        // reason = wait_result_to_str(pclose_rc);
        let reason = wait_error_seams::wait_result_to_str::call(pclose_rc);
        report(loglevel, "run_ssl_passphrase_command", |b| {
            b.errcode_for_file_access()
                .errmsg(format!("command \"{command}\" failed"))
                .errdetail_internal(reason)
        })?;
        return Ok(buf); // goto error;
    }

    // strip trailing newline and carriage return: len = pg_strip_crlf(buf);
    strip_crlf_in_place(&mut buf);

    // error: pfree(command); return len;  (command is mcx-owned, freed with the
    // context.)
    Ok(buf)
}

/// Check permissions for SSL key files.
///
/// `stat` the file and enforce: regular file; owned by us or root; if owned by
/// us, mode `0600` or less; if owned by root, `0640` or less. `is_server_start`
/// selects the loglevel (`FATAL` vs `LOG`). `Ok(true)` if acceptable;
/// `Ok(false)` after a `LOG`-level report; `Err` for a `FATAL` report.
pub fn check_ssl_key_file_permissions(
    ssl_key_file: &str,
    is_server_start: bool,
) -> PgResult<bool> {
    let loglevel: ErrorLevel = if is_server_start { FATAL } else { LOG };

    // if (stat(ssl_key_file, &buf) != 0) { ereport(loglevel, ...); return false; }
    let (st_mode, st_uid) = match raw_stat(ssl_key_file) {
        Ok(info) => info,
        Err(errno) => {
            report(loglevel, "check_ssl_key_file_permissions", |b| {
                b.with_saved_errno(errno)
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not access private key file \"{ssl_key_file}\": %m"
                    ))
            })?;
            return Ok(false);
        }
    };

    // Key file must be a regular file
    if !s_isreg(st_mode) {
        report(loglevel, "check_ssl_key_file_permissions", |b| {
            b.errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                "private key file \"{ssl_key_file}\" is not a regular file"
            ))
        })?;
        return Ok(false);
    }

    // #if !defined(WIN32) && !defined(__CYGWIN__)
    let euid = geteuid();

    if st_uid != euid && st_uid != 0 {
        report(loglevel, "check_ssl_key_file_permissions", |b| {
            b.errcode(ERRCODE_CONFIG_FILE_ERROR).errmsg(format!(
                "private key file \"{ssl_key_file}\" must be owned by the database user or root"
            ))
        })?;
        return Ok(false);
    }

    if (st_uid == euid && (st_mode & (S_IRWXG | S_IRWXO)) != 0)
        || (st_uid == 0 && (st_mode & (S_IWGRP | S_IXGRP | S_IRWXO)) != 0)
    {
        report(loglevel, "check_ssl_key_file_permissions", |b| {
            b.errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "private key file \"{ssl_key_file}\" has group or world access"
                ))
                .errdetail(
                    "File must have permissions u=rw (0600) or less if owned by the database user, or permissions u=rw,g=r (0640) or less if owned by root.",
                )
        })?;
        return Ok(false);
    }
    // #endif

    Ok(true)
}

/// Mirror `ereport(level, ...)`: build the report and run it. Sub-ERROR levels
/// emit and return `Ok(())`; `ERROR`/`FATAL`/`PANIC` propagate as `Err`.
fn report<F>(level: ErrorLevel, funcname: &'static str, build: F) -> PgResult<()>
where
    F: FnOnce(::utils_error::ErrorBuilder) -> ::utils_error::ErrorBuilder,
{
    build(ereport(level)).finish(ErrorLocation::new(FILENAME, 0, funcname))
}

/// `explicit_bzero(buf, size)` — wipe the accumulated passphrase bytes. The
/// owned buffer holds only what was read (<= size), so clearing its contents is
/// the faithful effect.
fn explicit_bzero(buf: &mut PgVec<'_, u8>) {
    for b in buf.iter_mut() {
        *b = 0;
    }
    buf.clear();
}

/// `pg_strip_crlf(buf)` against the accumulated bytes: strip trailing CR/LF and
/// truncate to the new length (the C macro zero-terminates in place and returns
/// the new length).
fn strip_crlf_in_place(buf: &mut PgVec<'_, u8>) {
    let s = String::from_utf8_lossy(buf).into_owned();
    let stripped = pg_strip_crlf(&s);
    let new_len = stripped.len();
    buf.truncate(new_len);
}

// Runtime storage for the `ssl_key_file` GUC (`char *ssl_key_file;` in
// be-secure.c — the SSL-transport translation unit this crate represents).
// It is an ordinary `PGC_SIGHUP` string GUC read from the GUC slot (boot value
// "server.key"); C leaves the pointer NULL at startup and the GUC machinery
// assigns it. We mirror that `char **variable` here, where `None` is C's NULL.
thread_local! {
    static SSL_KEY_FILE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Read `*conf->variable` for `ssl_key_file` (`char *ssl_key_file`).
fn ssl_key_file() -> Option<String> {
    SSL_KEY_FILE.with(|v| v.borrow().clone())
}

/// Write `*conf->variable` for `ssl_key_file`.
fn set_ssl_key_file(value: Option<String>) {
    SSL_KEY_FILE.with(|v| *v.borrow_mut() = value);
}

/// Install this unit's inward seams (consumed by `be-secure-openssl.c`).
pub fn init_seams() {
    my_seams::run_ssl_passphrase_command::set(run_ssl_passphrase_command);
    my_seams::check_ssl_key_file_permissions::set(check_ssl_key_file_permissions);

    // Install the `ssl_key_file` GUC variable accessor over this unit's backing
    // store (C's `char *ssl_key_file` in be-secure.c).
    vars::ssl_key_file.install(GucVarAccessors {
        get: ssl_key_file,
        set: set_ssl_key_file,
    });
}
