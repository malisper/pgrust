//! Interlock lock-file machinery (`miscinit.c`): `UnlinkLockFiles`,
//! `CreateLockFile`, `CreateDataDirLockFile`, `CreateSocketLockFile`,
//! `TouchSocketLockFiles`, `AddToDataDirLockFile`, `RecheckDataDirLockFile`,
//! ported over `std::fs`.
//!
//! The `lock_files` list is the single postmaster/standalone backend's
//! backend-private state, so it is a `thread_local!`. Genuine OS / cross-
//! subsystem externals (parent/grandparent PID, `kill(pid,0)` liveness,
//! `PostPortNumber`, `utime`, `PGSharedMemoryIsInUse`, `on_proc_exit`) cross
//! their owners' seams; everything else is plain `std::fs` here.

use std::cell::{Cell, RefCell};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;

use types_error::{PgError, PgResult};

use crate::MISCINIT_C;

/// `DIRECTORY_LOCK_FILE` (`miscinit.c:60`).
pub(crate) const DIRECTORY_LOCK_FILE: &str = "postmaster.pid";

/// `pg_file_create_mode` default (`file_perm.c`): `0600`. The C comment in
/// `CreateLockFile` insists this never be made weaker than 0600/0640.
const PG_FILE_CREATE_MODE: u32 = 0o600;

/// `LOCK_FILE_LINE_SHMEM_KEY` (`pidfile.h`): the lock-file line carrying the
/// `id1 id2` shmem-key pair the stale-lock recheck scans for.
const LOCK_FILE_LINE_SHMEM_KEY: i32 = 7;

/// `BLCKSZ` (`pg_config.h`) — the working-buffer size for the data-dir lock
/// file rewrite/recheck.
const BLCKSZ: usize = 8192;

thread_local! {
    /// `static List *lock_files = NIL;` — files to unlink at proc exit, in
    /// reverse creation order (C `lcons` prepends).
    static LOCK_FILES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    /// Whether `on_proc_exit(UnlinkLockFiles, 0)` has been registered yet
    /// (the C `if (lock_files == NIL)` check).
    static UNLINK_HOOK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

fn errno_of(e: &std::io::Error) -> i32 {
    e.raw_os_error().unwrap_or(0)
}

/// `errcode_for_file_access() + errmsg("...: %m")` analog: render an
/// `io::Error` into a `PgError`, keeping the OS errno text in the `%m` tail.
fn file_err(context: impl Into<String>, e: &std::io::Error) -> PgError {
    PgError::error(format!("{}: {e}", context.into()))
}

/// `DataDir` (globals.c).
fn data_dir() -> String {
    backend_utils_init_small::globals::DataDir().unwrap_or_default()
}

/// `MyStartTime` (globals.c, `pg_time_t` seconds), written to line 3.
fn my_start_time() -> i64 {
    backend_utils_init_small::globals::MyStartTime()
}

// EEXIST / EACCES / ENOENT — the POSIX errno values the C open/read branch
// discrimination tests against (kept local; no libc dep needed here).
const EEXIST: i32 = 17;
const EACCES: i32 = 13;
const ENOENT: i32 = 2;

// ===========================================================================
// UnlinkLockFiles (miscinit.c:1175) — the on_proc_exit callback body.
// ===========================================================================

/// `on_proc_exit` callback shape (`fn(int status, Datum arg)`) for
/// `UnlinkLockFiles`. Delegates to [`unlink_lock_files`]; cannot fail.
fn unlink_lock_files_hook(_status: i32, _arg: types_datum::Datum) -> PgResult<()> {
    unlink_lock_files();
    Ok(())
}

/// `UnlinkLockFiles(status, arg)` (`miscinit.c:1175`): unlink every recorded
/// lock file, then clear the list and log shutdown completion.
pub fn unlink_lock_files() {
    LOCK_FILES.with(|files| {
        let mut files = files.borrow_mut();
        for curfile in files.iter() {
            // unlink(curfile); /* Should we complain if the unlink fails? */
            let _ = std::fs::remove_file(curfile);
        }
        // lock_files = NIL;
        files.clear();
    });

    // Lock file removal is the last externally visible action of a postmaster
    // or standalone backend; log completion (LOG under postmaster, else NOTICE).
    let elevel = if backend_utils_init_small::globals::IsPostmasterEnvironment() {
        types_error::LOG
    } else {
        types_error::NOTICE
    };
    let _ = backend_utils_error::ereport(elevel)
        .errmsg("database system is shut down")
        .finish(types_error::ErrorLocation::new(MISCINIT_C, 1197, "UnlinkLockFiles"));
}

/// `on_proc_exit(UnlinkLockFiles, 0)` once, then `lcons` the file (prepend, so
/// the unlink at exit happens in reverse creation order — C marks this critical).
fn register_lock_file(filename: &str) -> PgResult<()> {
    UNLINK_HOOK_REGISTERED.with(|reg| -> PgResult<()> {
        if !reg.get() {
            backend_storage_ipc_seams::on_proc_exit::call(
                unlink_lock_files_hook,
                types_datum::Datum::null(),
            )?;
            reg.set(true);
        }
        Ok(())
    })?;
    LOCK_FILES.with(|files| files.borrow_mut().insert(0, filename.to_string()));
    Ok(())
}

// ===========================================================================
// CreateLockFile (miscinit.c:1209), ported over std::fs.
// ===========================================================================

/// `CreateLockFile(filename, amPostmaster, socketDir, isDDLock, refName)`
/// (`miscinit.c:1209`).
pub fn create_lock_file(
    filename: &str,
    am_postmaster: bool,
    socket_dir: &str,
    is_dd_lock: bool,
    ref_name: &str,
) -> PgResult<()> {
    // my_pid = getpid(); my_p_pid = getppid(); my_gp_pid = atoi(getenv(...)).
    let my_pid: i32 = std::process::id() as i32;
    let my_p_pid: i32 = backend_port_path_seams::getppid::call();
    let my_gp_pid: i32 = match std::env::var("PG_GRANDPARENT_PID") {
        Ok(v) => parse_leading_i32(&v),
        Err(_) => 0,
    };

    let created;

    // The race-resolution retry loop, capped at 100 tries.
    let mut ntries = 0;
    loop {
        // Try to create the lock file --- O_EXCL makes this atomic.
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(PG_FILE_CREATE_MODE)
            .open(filename)
        {
            Ok(f) => {
                created = Some(f);
                break; // Success; exit the retry loop
            }
            Err(e) => {
                // Couldn't create the pid file. Probably it already exists.
                let errno = errno_of(&e);
                if (errno != EEXIST && errno != EACCES) || ntries > 100 {
                    return Err(file_err(
                        format!("could not create lock file \"{filename}\""),
                        &e,
                    ));
                }
            }
        }

        // Read the file to get the old owner's PID. Race: the file might have
        // been deleted since we tried to create it.
        let mut existing = match OpenOptions::new().read(true).open(filename) {
            Ok(f) => f,
            Err(e) => {
                if errno_of(&e) == ENOENT {
                    ntries += 1;
                    continue; // race condition; try again
                }
                return Err(file_err(
                    format!("could not open lock file \"{filename}\""),
                    &e,
                ));
            }
        };

        let mut buffer = String::new();
        if let Err(e) = existing.read_to_string(&mut buffer) {
            return Err(file_err(
                format!("could not read lock file \"{filename}\""),
                &e,
            ));
        }
        drop(existing);

        if buffer.is_empty() {
            return Err(PgError::error(format!(
                "lock file \"{filename}\" is empty. Either another server is starting, \
                 or the lock file is the remnant of a previous server startup crash."
            ))
            .with_sqlstate(types_error::ERRCODE_LOCK_FILE_EXISTS));
        }

        // encoded_pid = atoi(buffer); if pid < 0, it's a postgres, not postmaster.
        let encoded_pid: i32 = parse_leading_i32(&buffer);
        let other_pid: i32 = encoded_pid.unsigned_abs() as i32;

        if other_pid <= 0 {
            return Err(PgError::error(format!(
                "bogus data in lock file \"{filename}\": \"{}\"",
                buffer.trim_end_matches('\0')
            )));
        }

        // Check to see if the other process still exists. my_pid/my_p_pid/
        // my_gp_pid can be ignored as false matches.
        if other_pid != my_pid && other_pid != my_p_pid && other_pid != my_gp_pid
            && backend_port_path_seams::pid_appears_live::call(other_pid)
        {
            // lockfile belongs to a live process
            let what = if is_dd_lock {
                if encoded_pid < 0 {
                    format!(
                        "Is another postgres (PID {other_pid}) running in data directory \"{ref_name}\"?"
                    )
                } else {
                    format!(
                        "Is another postmaster (PID {other_pid}) running in data directory \"{ref_name}\"?"
                    )
                }
            } else if encoded_pid < 0 {
                format!("Is another postgres (PID {other_pid}) using socket file \"{ref_name}\"?")
            } else {
                format!("Is another postmaster (PID {other_pid}) using socket file \"{ref_name}\"?")
            };
            return Err(PgError::error(format!(
                "lock file \"{filename}\" already exists. {what}"
            ))
            .with_sqlstate(types_error::ERRCODE_LOCK_FILE_EXISTS));
        }

        // No live creator. Check for an orphaned shmem segment. Because
        // postmaster.pid is written in steps, the shmem ID line may be absent —
        // not an error.
        if is_dd_lock {
            if let Some((id1, id2)) = scan_shmem_key_line(&buffer) {
                if backend_port_sysv_shmem_seams::pg_shared_memory_is_in_use::call(id1, id2)? {
                    return Err(PgError::error(format!(
                        "pre-existing shared memory block (key {id1}, ID {id2}) is still in use. \
                         Terminate any old server processes associated with data directory \"{ref_name}\"."
                    ))
                    .with_sqlstate(types_error::ERRCODE_LOCK_FILE_EXISTS));
                }
            }
        }

        // Looks like nobody's home. Unlink the file and try again to create it.
        if let Err(e) = std::fs::remove_file(filename) {
            return Err(file_err(
                format!(
                    "could not remove old lock file \"{filename}\". The file seems \
                     accidentally left over, but it could not be removed. Please remove \
                     the file by hand and try again."
                ),
                &e,
            ));
        }
        ntries += 1;
    }

    let mut file = created.expect("lock file fd set on the success break");

    // Successfully created the file, now fill it. The first five lines (PID,
    // DataDir, MyStartTime, PostPortNumber, socketDir) match the C snprintf.
    let pid_field = if am_postmaster { my_pid } else { -my_pid };
    let mut contents = format!(
        "{pid_field}\n{}\n{}\n{}\n{}\n",
        data_dir(),
        my_start_time(),
        backend_port_path_seams::post_port_number::call(),
        socket_dir,
    );
    // In a standalone backend, the LISTEN_ADDR line never receives data, so
    // fill it in as empty now.
    if is_dd_lock && !am_postmaster {
        contents.push('\n');
    }

    if let Err(e) = file.write_all(contents.as_bytes()) {
        let _ = std::fs::remove_file(filename);
        return Err(file_err(
            format!("could not write lock file \"{filename}\""),
            &e,
        ));
    }

    // pg_fsync(fd)
    if let Err(e) = file.sync_all() {
        let _ = std::fs::remove_file(filename);
        return Err(file_err(
            format!("could not write lock file \"{filename}\""),
            &e,
        ));
    }
    drop(file); // close(fd)

    // Arrange to unlink at proc_exit (reverse creation order via prepend).
    register_lock_file(filename)
}

/// `atoi(s)` for the leading integer (skip leading whitespace, optional sign,
/// stop at first non-digit).
fn parse_leading_i32(s: &str) -> i32 {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }
    let mut sign = 1i64;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let mut val: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    (sign * val).clamp(i32::MIN as i64, i32::MAX as i64) as i32
}

/// `atol(s)` for a leading long (the `RecheckDataDirLockFile` PID parse).
fn parse_leading_i64(s: &str) -> i64 {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i] as char).is_whitespace() {
        i += 1;
    }
    let mut sign = 1i64;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        if bytes[i] == b'-' {
            sign = -1;
        }
        i += 1;
    }
    let mut val: i64 = 0;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        val = val.saturating_mul(10).saturating_add((bytes[i] - b'0') as i64);
        i += 1;
    }
    sign.saturating_mul(val)
}

/// Advance to `LOCK_FILE_LINE_SHMEM_KEY` and `sscanf("%lu %lu")` the shmem-key
/// ids (the C pointer-walk in `CreateLockFile`). `None` when the file has not
/// been written that far (not an error).
fn scan_shmem_key_line(buffer: &str) -> Option<(u64, u64)> {
    let mut lines = buffer.split('\n');
    for _ in 1..LOCK_FILE_LINE_SHMEM_KEY {
        lines.next()?;
    }
    let line = lines.next()?;
    let mut it = line.split_whitespace();
    let id1 = it.next()?.parse::<u64>().ok()?;
    let id2 = it.next()?.parse::<u64>().ok()?;
    Some((id1, id2))
}

// ===========================================================================
// CreateDataDirLockFile / CreateSocketLockFile / TouchSocketLockFiles.
// ===========================================================================

/// `CreateDataDirLockFile(amPostmaster)` (`miscinit.c:1514`).
pub fn create_data_dir_lock_file(am_postmaster: bool) -> PgResult<()> {
    create_lock_file(DIRECTORY_LOCK_FILE, am_postmaster, "", true, &data_dir())
}

/// `CreateSocketLockFile(socketfile, amPostmaster, socketDir)` (`miscinit.c:1523`).
pub fn create_socket_lock_file(
    socketfile: &str,
    am_postmaster: bool,
    socket_dir: &str,
) -> PgResult<()> {
    let lockfile = format!("{socketfile}.lock");
    create_lock_file(&lockfile, am_postmaster, socket_dir, false, socketfile)
}

/// `TouchSocketLockFiles()` (`miscinit.c:1541`): `utime(socketLockFile, NULL)`
/// each recorded socket lock file (skipping the data-dir lock file). Errors
/// ignored, matching the C `(void) utime(...)`.
pub fn touch_socket_lock_files() {
    LOCK_FILES.with(|files| {
        for f in files.borrow().iter() {
            // No need to touch the data directory lock file, we trust.
            if f == DIRECTORY_LOCK_FILE {
                continue;
            }
            // we just ignore any error here
            backend_port_path_seams::touch_file_times::call(f);
        }
    });
}

// ===========================================================================
// AddToDataDirLockFile (miscinit.c:1570).
// ===========================================================================

/// `AddToDataDirLockFile(target_line, str)` (`miscinit.c:1570`): add or replace
/// a line in the data directory lock file. The given string should not include
/// a trailing newline. Any I/O failure is `ereport(LOG)` (non-fatal).
pub fn AddToDataDirLockFile(target_line: i32, line: &str) -> PgResult<()> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(DIRECTORY_LOCK_FILE)
    {
        Ok(f) => f,
        Err(e) => {
            return log_file_access(
                format!("could not open file \"{DIRECTORY_LOCK_FILE}\""),
                &e,
                "AddToDataDirLockFile",
                1586,
            );
        }
    };

    let mut srcbuffer = String::new();
    if let Err(e) = file.read_to_string(&mut srcbuffer) {
        return log_file_access(
            format!("could not read from file \"{DIRECTORY_LOCK_FILE}\""),
            &e,
            "AddToDataDirLockFile",
            1597,
        );
    }

    // Advance over lines we are not supposed to rewrite, copying them; then fill
    // in any missing lines before the target, then write/rewrite the target
    // line, then append any trailing lines from the old file.
    let src = srcbuffer.as_str();
    let mut dest = String::with_capacity(BLCKSZ);

    // srcptr walks the source; find the byte offset after (target_line-1) '\n's.
    let mut srcptr = 0usize;
    let mut lineno = 1;
    while lineno < target_line {
        match src[srcptr..].find('\n') {
            Some(rel) => srcptr += rel + 1,
            None => break, // not enough lines in file yet
        }
        lineno += 1;
    }
    // memcpy(destbuffer, srcbuffer, srcptr - srcbuffer)
    dest.push_str(&src[..srcptr]);

    // Fill in any missing lines before the target line.
    while lineno < target_line {
        dest.push('\n');
        lineno += 1;
    }

    // Write or rewrite the target line.
    dest.push_str(line);
    dest.push('\n');

    // If there are more lines in the old file, append them.
    if let Some(rel) = src[srcptr..].find('\n') {
        let after = srcptr + rel + 1;
        dest.push_str(&src[after..]);
    }

    // Rewrite the data in a single pwrite at offset 0 (atomic to onlookers).
    use std::os::unix::fs::FileExt;
    match file.write_all_at(dest.as_bytes(), 0) {
        Ok(()) => {}
        Err(e) => {
            return log_file_access(
                format!("could not write to file \"{DIRECTORY_LOCK_FILE}\""),
                &e,
                "AddToDataDirLockFile",
                1661,
            );
        }
    }
    if let Err(e) = file.sync_all() {
        return log_file_access(
            format!("could not write to file \"{DIRECTORY_LOCK_FILE}\""),
            &e,
            "AddToDataDirLockFile",
            1672,
        );
    }
    Ok(())
}

/// `ereport(LOG, (errcode_for_file_access(), errmsg(... %m)))` then `return`.
fn log_file_access(
    msg: String,
    e: &std::io::Error,
    funcname: &'static str,
    lineno: i32,
) -> PgResult<()> {
    backend_utils_error::ereport(types_error::LOG)
        .errcode_for_file_access()
        .errmsg(format!("{msg}: {e}"))
        .finish(types_error::ErrorLocation::new(MISCINIT_C, lineno, funcname))
}

// ===========================================================================
// RecheckDataDirLockFile (miscinit.c:1697).
// ===========================================================================

/// `RecheckDataDirLockFile()` (`miscinit.c:1697`): true if the data-dir lock
/// file still looks OK (return true on any doubt to avoid an unnecessary panic
/// shutdown).
pub fn RecheckDataDirLockFile() -> PgResult<bool> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(DIRECTORY_LOCK_FILE)
    {
        Ok(f) => f,
        Err(e) => {
            // Fail only on enumerated clearly-something-is-wrong conditions.
            let errno = errno_of(&e);
            const ENOTDIR: i32 = 20;
            if errno == ENOENT || errno == ENOTDIR {
                // disaster
                backend_utils_error::ereport(types_error::LOG)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{DIRECTORY_LOCK_FILE}\": {e}"))
                    .finish(types_error::ErrorLocation::new(
                        MISCINIT_C, 1720, "RecheckDataDirLockFile",
                    ))?;
                return Ok(false);
            }
            // non-fatal, at least for now
            backend_utils_error::ereport(types_error::LOG)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not open file \"{DIRECTORY_LOCK_FILE}\": {e}; continuing anyway"
                ))
                .finish(types_error::ErrorLocation::new(
                    MISCINIT_C, 1727, "RecheckDataDirLockFile",
                ))?;
            return Ok(true);
        }
    };

    let mut buffer = String::new();
    if let Err(e) = file.read_to_string(&mut buffer) {
        backend_utils_error::ereport(types_error::LOG)
            .errcode_for_file_access()
            .errmsg(format!("could not read from file \"{DIRECTORY_LOCK_FILE}\": {e}"))
            .finish(types_error::ErrorLocation::new(
                MISCINIT_C, 1739, "RecheckDataDirLockFile",
            ))?;
        return Ok(true); // treat read failure as nonfatal
    }
    drop(file);

    let file_pid = parse_leading_i64(&buffer);
    if file_pid == std::process::id() as i64 {
        return Ok(true); // all is well
    }

    // Trouble: someone's overwritten the lock file.
    backend_utils_error::ereport(types_error::LOG)
        .errmsg(format!(
            "lock file \"{DIRECTORY_LOCK_FILE}\" contains wrong PID: {file_pid} instead of {}",
            std::process::id()
        ))
        .finish(types_error::ErrorLocation::new(
            MISCINIT_C, 1751, "RecheckDataDirLockFile",
        ))?;
    Ok(false)
}
