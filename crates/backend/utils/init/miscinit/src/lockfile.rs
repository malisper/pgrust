//! Interlock lock files: postmaster.pid and $SOCKFILE.lock. `lock_files` is
//! cold process-lifetime state: plain heap Vec<String> (C: List in
//! TopMemoryContext). WAIT_EVENT_LOCK_FILE_* lands with the wait-event unit.

use std::cell::{Cell, RefCell};
use std::fs::OpenOptions;
use std::io::{Read, Write};
#[cfg(not(target_family = "wasm"))]
use std::os::unix::fs::{FileExt, OpenOptionsExt};
// wasm32: std::os::wasi::fs::FileExt is unstable (wasi_ext) and .mode() has
// no WASI counterpart (no file mode bits); the wasm write arm below seeks
// then writes — C's own shape (miscinit.c uses lseek + write here).
#[cfg(target_family = "wasm")]
use std::io::{Seek, SeekFrom};

use elog::ereport;
use init_small::globals as g;
use types_error::{PgResult, ERRCODE_LOCK_FILE_EXISTS, FATAL, LOG, NOTICE};

use crate::process::{leading_i64, loc};

pub(crate) const DIRECTORY_LOCK_FILE: &str = "postmaster.pid";
// pg_file_create_mode default; the 0640 group variant lands with file_perm.c.
// wasm32: WASI files carry no unix mode bits — the constant has no consumer.
#[cfg(not(target_family = "wasm"))]
const PG_FILE_CREATE_MODE: u32 = 0o600;
const LOCK_FILE_LINE_SHMEM_KEY: usize = 7;

thread_local! {
    static LOCK_FILES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static UNLINK_HOOK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

fn my_pid() -> i32 {
    init_small::globals::process_id() as i32
}

pub fn UnlinkLockFiles(_status: i32, _arg: usize) {
    LOCK_FILES.with_borrow_mut(|files| {
        for curfile in files.iter() {
            let _ = std::fs::remove_file(curfile);
        }
        files.clear();
    });

    let elevel = if g::IsPostmasterEnvironment() {
        LOG
    } else {
        NOTICE
    };
    let _ = ereport(elevel)
        .errmsg("database system is shut down")
        .finish(loc(1197, "UnlinkLockFiles"));
}

fn register_lock_file(filename: &str) {
    if !UNLINK_HOOK_REGISTERED.get() {
        ipc_seams::on_proc_exit::call(UnlinkLockFiles, 0);
        UNLINK_HOOK_REGISTERED.set(true);
    }
    // C lcons: unlink order is reverse creation order (critical!).
    LOCK_FILES.with_borrow_mut(|files| files.insert(0, filename.to_string()));
}

// EPERM implies a different userid, so not a competing postmaster.
#[cfg(not(target_family = "wasm"))]
fn pid_appears_live(pid: i32) -> bool {
    // SAFETY: kill with signal 0 only probes for existence.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno != libc::ESRCH && errno != libc::EPERM
}

// wasm32: WASI p1 has no processes and no kill(2); the instance is the only
// process that can exist, so a lock-file PID is always a stale remnant of a
// (native) crash — never a live competitor.
#[cfg(target_family = "wasm")]
fn pid_appears_live(_pid: i32) -> bool {
    false
}

fn scan_shmem_key_line(buffer: &str) -> Option<(u64, u64)> {
    let mut lines = buffer.split('\n');
    for _ in 1..LOCK_FILE_LINE_SHMEM_KEY {
        lines.next()?;
    }
    let mut it = lines.next()?.split_whitespace();
    let id1 = it.next()?.parse::<u64>().ok()?;
    let id2 = it.next()?.parse::<u64>().ok()?;
    Some((id1, id2))
}

// pub(crate) so the datadir-first-contact tests can drive the data-directory
// arm (is_dd_lock = true) at an absolute path, without the process-global cwd
// change CreateDataDirLockFile's relative path would force on them.
pub(crate) fn CreateLockFile(
    filename: &str,
    am_postmaster: bool,
    socket_dir: &str,
    is_dd_lock: bool,
    ref_name: &str,
) -> PgResult<()> {
    let my_pid = my_pid();
    // SAFETY: getppid has no failure modes.
    #[cfg(not(target_family = "wasm"))]
    let my_p_pid = unsafe { libc::getppid() } as i32;
    // wasm32: no parent process exists on WASI; 0 is C's "no such pid"
    // sentinel (same word my_gp_pid uses when PG_GRANDPARENT_PID is unset).
    #[cfg(target_family = "wasm")]
    let my_p_pid = 0i32;
    let my_gp_pid = match std::env::var("PG_GRANDPARENT_PID") {
        Ok(v) => leading_i64(&v) as i32,
        Err(_) => 0,
    };

    let mut file;
    let mut ntries = 0;
    loop {
        let mut open_opts = OpenOptions::new();
        open_opts.read(true).write(true).create_new(true);
        // wasm32: WASI has no file modes; create_new keeps the interlock.
        #[cfg(not(target_family = "wasm"))]
        open_opts.mode(PG_FILE_CREATE_MODE);
        match open_opts.open(filename) {
            Ok(f) => {
                file = f;
                break;
            }
            Err(e) => {
                let errno = e.raw_os_error().unwrap_or(0);
                if (errno != libc::EEXIST && errno != libc::EACCES) || ntries > 100 {
                    ereport(FATAL)
                        .with_saved_errno(errno)
                        .errcode_for_file_access()
                        .errmsg(format!("could not create lock file \"{filename}\": %m"))
                        .finish(loc(1285, "CreateLockFile"))?;
                }
            }
        }

        // Raw bytes, like C's read(): a torn write leaving non-UTF8 garbage
        // past the leading PID digits must not abort the whole parse.
        let mut raw = Vec::new();
        match OpenOptions::new().read(true).open(filename) {
            Ok(mut f) => {
                if let Err(e) = f.read_to_end(&mut raw) {
                    ereport(FATAL)
                        .with_saved_errno(e.raw_os_error().unwrap_or(0))
                        .errcode_for_file_access()
                        .errmsg(format!("could not read lock file \"{filename}\": %m"))
                        .finish(loc(1306, "CreateLockFile"))?;
                }
            }
            Err(e) => {
                if e.raw_os_error() == Some(libc::ENOENT) {
                    ntries += 1;
                    continue;
                }
                ereport(FATAL)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not open lock file \"{filename}\": %m"))
                    .finish(loc(1299, "CreateLockFile"))?;
            }
        }

        if raw.is_empty() {
            ereport(FATAL)
                .errcode(ERRCODE_LOCK_FILE_EXISTS)
                .errmsg(format!("lock file \"{filename}\" is empty"))
                .errhint(
                    "Either another server is starting, or the lock file is the remnant \
                     of a previous server startup crash.",
                )
                .finish(loc(1315, "CreateLockFile"))?;
        }
        let buffer = String::from_utf8_lossy(&raw).into_owned();

        // encoded_pid < 0 marks a standalone postgres, not a postmaster.
        let encoded_pid = leading_i64(&buffer) as i32;
        let other_pid = encoded_pid.unsigned_abs() as i32;
        if other_pid <= 0 {
            // C's elog(FATAL, ..., buffer) prints the whole raw buffer, not
            // just its first line.
            ereport(FATAL)
                .errmsg_internal(format!(
                    "bogus data in lock file \"{filename}\": \"{buffer}\""
                ))
                .finish(loc(1326, "CreateLockFile"))?;
        }

        // my/parent/grandparent pid are false matches (reboot PID reuse).
        if other_pid != my_pid
            && other_pid != my_p_pid
            && other_pid != my_gp_pid
            && pid_appears_live(other_pid)
        {
            let hint = match (is_dd_lock, encoded_pid < 0) {
                (true, true) => format!(
                    "Is another postgres (PID {other_pid}) running in data directory \"{ref_name}\"?"
                ),
                (true, false) => format!(
                    "Is another postmaster (PID {other_pid}) running in data directory \"{ref_name}\"?"
                ),
                (false, true) => format!(
                    "Is another postgres (PID {other_pid}) using socket file \"{ref_name}\"?"
                ),
                (false, false) => format!(
                    "Is another postmaster (PID {other_pid}) using socket file \"{ref_name}\"?"
                ),
            };
            ereport(FATAL)
                .errcode(ERRCODE_LOCK_FILE_EXISTS)
                .errmsg(format!("lock file \"{filename}\" already exists"))
                .errhint(hint)
                .finish(loc(1358, "CreateLockFile"))?;
        }

        // Dead creator: check for orphan backends holding the shmem segment.
        if is_dd_lock {
            if let Some((id1, id2)) = scan_shmem_key_line(&buffer) {
                if shmem_seams::pg_shared_memory_is_in_use::call(id1, id2)? {
                    ereport(FATAL)
                        .errcode(ERRCODE_LOCK_FILE_EXISTS)
                        .errmsg(format!(
                            "pre-existing shared memory block (key {id1}, ID {id2}) is still in use"
                        ))
                        .errhint(format!(
                            "Terminate any old server processes associated with data directory \"{ref_name}\"."
                        ))
                        .finish(loc(1405, "CreateLockFile"))?;
                }
            }
        }

        if let Err(e) = std::fs::remove_file(filename) {
            ereport(FATAL)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not remove old lock file \"{filename}\": %m"))
                .errhint(
                    "The file seems accidentally left over, but it could not be removed. \
                     Please remove the file by hand and try again.",
                )
                .finish(loc(1420, "CreateLockFile"))?;
        }
        ntries += 1;
    }

    // pidfile.h lines 1-5: PID, DataDir, MyStartTime, PostPortNumber, socketDir.
    let pid_field = if am_postmaster { my_pid } else { -my_pid };
    let mut contents = format!(
        "{pid_field}\n{}\n{}\n{}\n{socket_dir}\n",
        g::DataDir().unwrap_or_default(),
        g::MyStartTime(),
        (guc_tables::vars::PostPortNumber.get().get)(),
    );
    if is_dd_lock && !am_postmaster {
        contents.push('\n');
    }

    let write_result = file.write_all(contents.as_bytes()).and_then(|()| file.sync_all());
    // C's third distinct check: close(fd) itself can fail (e.g. NFS EIO);
    // write, fsync, and close failures all share this exact message.
    // std::os::fd is the portable spelling (present on wasi too; unix::io
    // merely re-exports it) — same trait, identical native codegen.
    use std::os::fd::IntoRawFd;
    let raw_fd = file.into_raw_fd();
    // SAFETY: raw_fd was just taken from `file`, which owned it exclusively;
    // closing it here replaces the implicit close the Drop would have done.
    let close_errno = if unsafe { libc::close(raw_fd) } != 0 {
        std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
    } else {
        0
    };
    if let Err(e) = write_result {
        let errno = e.raw_os_error().unwrap_or(libc::ENOSPC);
        let _ = std::fs::remove_file(filename);
        ereport(FATAL)
            .with_saved_errno(errno)
            .errcode_for_file_access()
            .errmsg(format!("could not write lock file \"{filename}\": %m"))
            .finish(loc(1461, "CreateLockFile"))?;
    }
    if close_errno != 0 {
        let _ = std::fs::remove_file(filename);
        ereport(FATAL)
            .with_saved_errno(close_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not write lock file \"{filename}\": %m"))
            .finish(loc(1486, "CreateLockFile"))?;
    }

    register_lock_file(filename);
    Ok(())
}

// cwd is already DataDir: the relative path locks the right directory.
pub fn CreateDataDirLockFile(am_postmaster: bool) -> PgResult<()> {
    CreateLockFile(
        DIRECTORY_LOCK_FILE,
        am_postmaster,
        "",
        true,
        g::DataDir().unwrap_or_default(),
    )
}

pub fn CreateSocketLockFile(
    socketfile: &str,
    am_postmaster: bool,
    socket_dir: &str,
) -> PgResult<()> {
    let lockfile = format!("{socketfile}.lock");
    CreateLockFile(&lockfile, am_postmaster, socket_dir, false, socketfile)
}

// Keep dates recent so /tmp cleaners spare the files; errors ignored.
#[cfg(not(target_family = "wasm"))]
pub fn TouchSocketLockFiles() {
    LOCK_FILES.with_borrow(|files| {
        for f in files.iter() {
            if f == DIRECTORY_LOCK_FILE {
                continue;
            }
            let path = std::ffi::CString::new(f.as_str()).expect("lock file path has no NUL");
            // SAFETY: NUL-terminated path; NULL utimbuf sets times to now.
            unsafe { libc::utime(path.as_ptr(), std::ptr::null()) };
        }
    });
}

// wasm32: sessions are socketless on WASI (no AF_UNIX), so socket lock files
// are never created and there is nothing to touch; the wasi libc crate also
// exposes no utime. No-op keeps the caller shape.
#[cfg(target_family = "wasm")]
pub fn TouchSocketLockFiles() {}

// Add or replace one line (no trailing newline in `line`). The file is never
// truncated, so lines must never shrink; every failure is ereport(LOG).
pub fn AddToDataDirLockFile(target_line: i32, line: &str) -> PgResult<()> {
    let mut file = match OpenOptions::new().read(true).write(true).open(DIRECTORY_LOCK_FILE) {
        Ok(f) => f,
        Err(e) => {
            return ereport(LOG)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{DIRECTORY_LOCK_FILE}\": %m"))
                .finish(loc(1586, "AddToDataDirLockFile"));
        }
    };
    let mut src = String::new();
    if let Err(e) = file.read_to_string(&mut src) {
        return ereport(LOG)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not read from file \"{DIRECTORY_LOCK_FILE}\": %m"))
            .finish(loc(1597, "AddToDataDirLockFile"));
    }

    let mut srcptr = 0usize;
    let mut lineno = 1;
    while lineno < target_line {
        match src[srcptr..].find('\n') {
            Some(rel) => srcptr += rel + 1,
            None => break,
        }
        lineno += 1;
    }
    let mut dest = String::with_capacity(src.len() + line.len() + 8);
    dest.push_str(&src[..srcptr]);
    for _ in lineno..target_line {
        dest.push('\n');
    }

    dest.push_str(line);
    dest.push('\n');
    if let Some(rel) = src[srcptr..].find('\n') {
        dest.push_str(&src[srcptr + rel + 1..]);
    }

    #[cfg(not(target_family = "wasm"))]
    let result = file
        .write_all_at(dest.as_bytes(), 0)
        .and_then(|()| file.sync_all());
    // wasm32: positioned-write ext trait is unstable; seek+write is C's shape.
    #[cfg(target_family = "wasm")]
    let result = file
        .seek(SeekFrom::Start(0))
        .and_then(|_| file.write_all(dest.as_bytes()))
        .and_then(|()| file.sync_all());
    if let Err(e) = result {
        return ereport(LOG)
            .with_saved_errno(e.raw_os_error().unwrap_or(libc::ENOSPC))
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{DIRECTORY_LOCK_FILE}\": %m"))
            .finish(loc(1661, "AddToDataDirLockFile"));
    }
    Ok(())
}

// Postmaster's periodic check that the lock file still carries our PID.
// Return true on any doubt — false triggers a panic shutdown.
pub fn RecheckDataDirLockFile() -> PgResult<bool> {
    let mut file = match OpenOptions::new().read(true).write(true).open(DIRECTORY_LOCK_FILE) {
        Ok(f) => f,
        Err(e) => {
            // Fail only on enumerated clearly-something-is-wrong conditions.
            let errno = e.raw_os_error().unwrap_or(0);
            if errno == libc::ENOENT || errno == libc::ENOTDIR {
                ereport(LOG)
                    .with_saved_errno(errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{DIRECTORY_LOCK_FILE}\": %m"))
                    .finish(loc(1720, "RecheckDataDirLockFile"))?;
                return Ok(false);
            }
            ereport(LOG)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not open file \"{DIRECTORY_LOCK_FILE}\": %m; continuing anyway"
                ))
                .finish(loc(1727, "RecheckDataDirLockFile"))?;
            return Ok(true);
        }
    };

    let mut buffer = String::new();
    if let Err(e) = file.read_to_string(&mut buffer) {
        ereport(LOG)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not read from file \"{DIRECTORY_LOCK_FILE}\": %m"))
            .finish(loc(1739, "RecheckDataDirLockFile"))?;
        return Ok(true);
    }
    drop(file);

    let file_pid = leading_i64(&buffer);
    if file_pid == my_pid() as i64 {
        return Ok(true);
    }

    ereport(LOG)
        .errmsg(format!(
            "lock file \"{DIRECTORY_LOCK_FILE}\" contains wrong PID: {file_pid} instead of {}",
            my_pid()
        ))
        .finish(loc(1751, "RecheckDataDirLockFile"))?;
    Ok(false)
}
