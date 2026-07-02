//! Interlock lock files: postmaster.pid and $SOCKFILE.lock. `lock_files` is
//! cold process-lifetime state: plain heap Vec<String> (C: List in
//! TopMemoryContext). WAIT_EVENT_LOCK_FILE_* lands with the wait-event unit.

use std::cell::{Cell, RefCell};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};

use elog::ereport;
use init_small::globals as g;
use types_error::{PgResult, ERRCODE_LOCK_FILE_EXISTS, FATAL, LOG, NOTICE};

use crate::process::{leading_i64, loc};

pub(crate) const DIRECTORY_LOCK_FILE: &str = "postmaster.pid";
// pg_file_create_mode default; the 0640 group variant lands with file_perm.c.
const PG_FILE_CREATE_MODE: u32 = 0o600;
const LOCK_FILE_LINE_SHMEM_KEY: usize = 7;

thread_local! {
    static LOCK_FILES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static UNLINK_HOOK_REGISTERED: Cell<bool> = const { Cell::new(false) };
}

fn my_pid() -> i32 {
    std::process::id() as i32
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
fn pid_appears_live(pid: i32) -> bool {
    // SAFETY: kill with signal 0 only probes for existence.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    errno != libc::ESRCH && errno != libc::EPERM
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

fn CreateLockFile(
    filename: &str,
    am_postmaster: bool,
    socket_dir: &str,
    is_dd_lock: bool,
    ref_name: &str,
) -> PgResult<()> {
    let my_pid = my_pid();
    // SAFETY: getppid has no failure modes.
    let my_p_pid = unsafe { libc::getppid() } as i32;
    let my_gp_pid = match std::env::var("PG_GRANDPARENT_PID") {
        Ok(v) => leading_i64(&v) as i32,
        Err(_) => 0,
    };

    let mut file;
    let mut ntries = 0;
    loop {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(PG_FILE_CREATE_MODE)
            .open(filename)
        {
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

        let mut buffer = String::new();
        match OpenOptions::new().read(true).open(filename) {
            Ok(mut f) => {
                if let Err(e) = f.read_to_string(&mut buffer) {
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

        if buffer.is_empty() {
            ereport(FATAL)
                .errcode(ERRCODE_LOCK_FILE_EXISTS)
                .errmsg(format!("lock file \"{filename}\" is empty"))
                .errhint(
                    "Either another server is starting, or the lock file is the remnant \
                     of a previous server startup crash.",
                )
                .finish(loc(1315, "CreateLockFile"))?;
        }

        // encoded_pid < 0 marks a standalone postgres, not a postmaster.
        let encoded_pid = leading_i64(&buffer) as i32;
        let other_pid = encoded_pid.unsigned_abs() as i32;
        if other_pid <= 0 {
            ereport(FATAL)
                .errmsg_internal(format!(
                    "bogus data in lock file \"{filename}\": \"{}\"",
                    buffer.lines().next().unwrap_or("")
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
    if let Err(e) = write_result {
        let errno = e.raw_os_error().unwrap_or(libc::ENOSPC);
        drop(file);
        let _ = std::fs::remove_file(filename);
        ereport(FATAL)
            .with_saved_errno(errno)
            .errcode_for_file_access()
            .errmsg(format!("could not write lock file \"{filename}\": %m"))
            .finish(loc(1461, "CreateLockFile"))?;
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

    let result = file
        .write_all_at(dest.as_bytes(), 0)
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
