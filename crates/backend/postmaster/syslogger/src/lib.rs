//! syslogger.c: the logging collector as a postmaster child thread. The pipe
//! chunk protocol, logfile rotation, and current_logfiles metafile are
//! C-shaped; the single fd table changes the pipe choreography (see the
//! divergence comments at each site): the postmaster's post-fork fclose of
//! the log files and the collector's close of the pipe write end are fork
//! artifacts and are skipped, the restart-case stderr->DEVNULL redirect would
//! sever every thread's log path and is skipped, and pipe EOF (C: last
//! writer process exits) is manufactured at postmaster exit by repointing
//! fds 1/2 at /dev/null, with a bounded wait for the collector's final flush.
//! Std Vec/String throughout: cold daemon thread, no mcx to charge
//! (elog-report precedent).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicPtr, Ordering::Relaxed};
use std::sync::Mutex;

use elog::ereport;
use init_small::globals as g;
use types_core::{pg_time_t, BackendType, MAXPGPATH};
use types_error::{
    ErrorLocation, PgError, PgResult, DEBUG1, FATAL, LOG, LOG_DESTINATION_CSVLOG,
    LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR,
};
use types_startup::StartupData;
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WaitEvent, WL_LATCH_SET, WL_SOCKET_READABLE};

// wasm32: no SIG* names in the wasi libc crate; Linux-numbered
// thread-signal emulation space (procsignal wasm arm).
#[cfg(not(target_family = "wasm"))]
use libc::{SIGHUP, SIGUSR1};
#[cfg(target_family = "wasm")]
const SIGHUP: i32 = 1;
#[cfg(target_family = "wasm")]
const SIGUSR1: i32 = 10;

#[cfg(test)]
mod tests;

// wasm32: no pipes on WASI; POSIX floor (512), elog's wasm arm convention.
#[cfg(not(target_family = "wasm"))]
pub const PIPE_CHUNK_SIZE: usize = if libc::PIPE_BUF > 65536 { 65536 } else { libc::PIPE_BUF };
#[cfg(target_family = "wasm")]
pub const PIPE_CHUNK_SIZE: usize = 512;
pub const PIPE_HEADER_SIZE: usize = 9;
pub const PIPE_MAX_PAYLOAD: usize = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE;
const READ_BUF_SIZE: usize = 2 * PIPE_CHUNK_SIZE;

const PIPE_PROTO_IS_LAST: u8 = 0x01;
const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;

const LOGROTATE_SIGNAL_FILE: &str = "logrotate";
pub const LOG_METAINFO_DATAFILE: &str = "current_logfiles";
const LOG_METAINFO_DATAFILE_TMP: &str = "current_logfiles.tmp";

const NBUFFER_LISTS: usize = 256;
const SECS_PER_MINUTE: i64 = 60;

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_SYSLOGGER_MAIN: u32 = PG_WAIT_ACTIVITY + 13;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

static LOGGING_COLLECTOR: AtomicBool = AtomicBool::new(false);
static LOG_ROTATION_AGE: AtomicI32 = AtomicI32::new(24 * 60);
static LOG_ROTATION_SIZE: AtomicI32 = AtomicI32::new(10 * 1024);
static LOG_TRUNCATE_ON_ROTATION: AtomicBool = AtomicBool::new(false);
static LOG_FILE_MODE: AtomicI32 = AtomicI32::new(0o600);
static LOG_DIRECTORY: Mutex<Option<String>> = Mutex::new(None);
static LOG_FILENAME: Mutex<Option<String>> = Mutex::new(None);

// C's syslogPipe[2] / FILE* statics cross the postmaster/collector thread
// boundary (fork used to copy them); process statics with the postmaster
// writing only before launch / in the redirect step.
static SYSLOG_PIPE_R: AtomicI32 = AtomicI32::new(-1);
static SYSLOG_PIPE_W: AtomicI32 = AtomicI32::new(-1);
static SYSLOG_FILE: AtomicPtr<libc::FILE> = AtomicPtr::new(std::ptr::null_mut());
static CSVLOG_FILE: AtomicPtr<libc::FILE> = AtomicPtr::new(std::ptr::null_mut());
static JSONLOG_FILE: AtomicPtr<libc::FILE> = AtomicPtr::new(std::ptr::null_mut());
static FIRST_SYSLOGGER_FILE_TIME: AtomicI64 = AtomicI64::new(0);

// The unattached logger holds no ProcSignal slot, so SendThreadSignal can't
// reach it; the postmaster's kill(syslogger, SIGHUP/SIGUSR1) is a direct
// poke: pend the flag, wake the parked latch.
static ROTATION_REQUESTED: AtomicBool = AtomicBool::new(false);
static CONFIG_RELOAD_PENDING: AtomicBool = AtomicBool::new(false);
static SYSLOGGER_LATCH: Mutex<Option<LatchHandle>> = Mutex::new(None);
static SYSLOGGER_EXITED: AtomicBool = AtomicBool::new(false);

pub fn collector_kill(signo: i32) {
    match signo {
        SIGUSR1 => ROTATION_REQUESTED.store(true, Relaxed),
        SIGHUP => CONFIG_RELOAD_PENDING.store(true, Relaxed),
        _ => return,
    }
    if let Some(l) = *SYSLOGGER_LATCH.lock().unwrap() {
        latch::SetLatch(l);
    }
}

pub fn Logging_collector() -> bool {
    LOGGING_COLLECTOR.load(Relaxed)
}

fn Log_RotationAge() -> i32 {
    LOG_ROTATION_AGE.load(Relaxed)
}

fn Log_RotationSize() -> i32 {
    LOG_ROTATION_SIZE.load(Relaxed)
}

fn Log_directory() -> String {
    LOG_DIRECTORY.lock().unwrap().clone().unwrap_or_else(|| "log".to_string())
}

fn Log_filename() -> String {
    LOG_FILENAME
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_else(|| "postgresql-%Y-%m-%d_%H%M%S.log".to_string())
}

struct SaveBuffer {
    pid: i32,
    data: Vec<u8>,
}

struct SysLoggerState {
    next_rotation_time: pg_time_t,
    pipe_eof_seen: bool,
    rotation_disabled: bool,
    last_sys_file_name: Option<String>,
    last_csv_file_name: Option<String>,
    last_json_file_name: Option<String>,
    buffer_lists: Vec<Vec<SaveBuffer>>,
}

impl SysLoggerState {
    fn new() -> Self {
        let mut buffer_lists = Vec::new();
        buffer_lists.resize_with(NBUFFER_LISTS, Vec::new);
        SysLoggerState {
            next_rotation_time: 0,
            pipe_eof_seen: false,
            rotation_disabled: false,
            last_sys_file_name: None,
            last_csv_file_name: None,
            last_json_file_name: None,
            buffer_lists,
        }
    }
}

fn time_now() -> pg_time_t {
    unsafe { libc::time(std::ptr::null_mut()) as pg_time_t }
}

fn last_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

// logfile_open's C contract: errno still valid for the caller's ENFILE/EMFILE
// classification after the LOG report.
fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    // SAFETY: writing the calling thread's errno slot.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    // SAFETY: writing the calling thread's errno slot.
    unsafe {
        *libc::__errno_location() = value;
    }
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn SysLoggerMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    let mut logbuffer = vec![0u8; READ_BUF_SIZE];
    let mut bytes_in_logbuffer: usize = 0;

    let mut now: pg_time_t = g::MyStartTime();

    miscinit::SetMyBackendType(BackendType::Logger);
    elog::config::set_am_syslogger(true);
    ps_status::init_ps_display(None);

    // C restart case: point the child's stderr at /dev/null so its inherited
    // pipe-write fd closes. One fd table: fd 2 is every thread's pipe write
    // end and the collector's own elog already routes via am_syslogger() to
    // write_syslogger_file — skipped.
    // C also closes its fork-copy of syslogPipe[1] here; the postmaster's
    // close after dup2 is the only one — skipped.

    // C's pqsignal block (ignore all termination signals, SIGHUP reload,
    // SIGUSR1 rotate): no ProcSignal slot without shmem attach — the
    // postmaster pokes collector_kill instead, and termination signals can't
    // reach this thread at all.

    let my_latch = g::MyLatch().expect("SysLoggerMain: MyLatch not initialized");
    *SYSLOGGER_LATCH.lock().unwrap() = Some(my_latch);
    SYSLOGGER_EXITED.store(false, Relaxed);

    let mut st = SysLoggerState::new();

    let first_time = FIRST_SYSLOGGER_FILE_TIME.load(Relaxed);
    st.last_sys_file_name = Some(logfile_getname(first_time, None));
    if !CSVLOG_FILE.load(Relaxed).is_null() {
        st.last_csv_file_name = Some(logfile_getname(first_time, Some(".csv")));
    }
    if !JSONLOG_FILE.load(Relaxed).is_null() {
        st.last_json_file_name = Some(logfile_getname(first_time, Some(".json")));
    }

    let mut current_log_dir = Log_directory();
    let mut current_log_filename = Log_filename();
    let mut current_log_rotation_age = Log_RotationAge();
    set_next_rotation_time(&mut st);
    if let Err(e) = update_metainfo_datafile(&st) {
        fatal_exit(&e);
    }

    elog::config::set_where_to_send_output(types_dest::CommandDest::None);

    let mut run = || -> PgResult<()> {
        let wes = waiteventset::CreateWaitEventSet(2)?;
        waiteventset::AddWaitEventToSet(
            wes,
            WL_LATCH_SET,
            types_core::PGINVALID_SOCKET,
            Some(my_latch),
            None,
        )?;
        waiteventset::AddWaitEventToSet(
            wes,
            WL_SOCKET_READABLE,
            SYSLOG_PIPE_R.load(Relaxed),
            None,
            None,
        )?;

        loop {
            let mut time_based_rotation = false;
            let mut size_rotation_for: i32 = 0;

            latch::ResetLatch(my_latch);

            if CONFIG_RELOAD_PENDING.swap(false, Relaxed) {
                guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;

                if Log_directory() != current_log_dir {
                    current_log_dir = Log_directory();
                    ROTATION_REQUESTED.store(true, Relaxed);
                    let _ = fd::MakePGDirectory(&current_log_dir);
                }
                if Log_filename() != current_log_filename {
                    current_log_filename = Log_filename();
                    ROTATION_REQUESTED.store(true, Relaxed);
                }

                if ((elog::config::log_destination() & LOG_DESTINATION_CSVLOG) != 0)
                    != !CSVLOG_FILE.load(Relaxed).is_null()
                {
                    ROTATION_REQUESTED.store(true, Relaxed);
                }
                if ((elog::config::log_destination() & LOG_DESTINATION_JSONLOG) != 0)
                    != !JSONLOG_FILE.load(Relaxed).is_null()
                {
                    ROTATION_REQUESTED.store(true, Relaxed);
                }

                if current_log_rotation_age != Log_RotationAge() {
                    current_log_rotation_age = Log_RotationAge();
                    set_next_rotation_time(&mut st);
                }

                if st.rotation_disabled {
                    st.rotation_disabled = false;
                    ROTATION_REQUESTED.store(true, Relaxed);
                }

                update_metainfo_datafile(&st)?;
            }

            if Log_RotationAge() > 0 && !st.rotation_disabled {
                now = time_now();
                if now >= st.next_rotation_time {
                    ROTATION_REQUESTED.store(true, Relaxed);
                    time_based_rotation = true;
                }
            }

            if !ROTATION_REQUESTED.load(Relaxed) && Log_RotationSize() > 0 && !st.rotation_disabled
            {
                let limit = Log_RotationSize() as i64 * 1024;
                if unsafe { libc::ftello(SYSLOG_FILE.load(Relaxed)) } >= limit {
                    ROTATION_REQUESTED.store(true, Relaxed);
                    size_rotation_for |= LOG_DESTINATION_STDERR;
                }
                let csv = CSVLOG_FILE.load(Relaxed);
                if !csv.is_null() && unsafe { libc::ftello(csv) } >= limit {
                    ROTATION_REQUESTED.store(true, Relaxed);
                    size_rotation_for |= LOG_DESTINATION_CSVLOG;
                }
                let json = JSONLOG_FILE.load(Relaxed);
                if !json.is_null() && unsafe { libc::ftello(json) } >= limit {
                    ROTATION_REQUESTED.store(true, Relaxed);
                    size_rotation_for |= LOG_DESTINATION_JSONLOG;
                }
            }

            if ROTATION_REQUESTED.load(Relaxed) {
                if !time_based_rotation && size_rotation_for == 0 {
                    size_rotation_for = LOG_DESTINATION_STDERR
                        | LOG_DESTINATION_CSVLOG
                        | LOG_DESTINATION_JSONLOG;
                }
                logfile_rotate(time_based_rotation, size_rotation_for, &mut st)?;
            }

            let cur_timeout: i64 = if Log_RotationAge() > 0 && !st.rotation_disabled {
                let mut delay = st.next_rotation_time - now;
                if delay > 0 {
                    if delay > (i32::MAX / 1000) as i64 {
                        delay = (i32::MAX / 1000) as i64;
                    }
                    delay * 1000
                } else {
                    0
                }
            } else {
                -1
            };

            let mut occurred = [WaitEvent::default(); 1];
            let rc =
                waiteventset::WaitEventSetWait(wes, cur_timeout, &mut occurred, WAIT_EVENT_SYSLOGGER_MAIN)?;

            if rc == 1 && occurred[0].events == WL_SOCKET_READABLE {
                let bytes_read = unsafe {
                    libc::read(
                        SYSLOG_PIPE_R.load(Relaxed),
                        logbuffer.as_mut_ptr().add(bytes_in_logbuffer).cast(),
                        READ_BUF_SIZE - bytes_in_logbuffer,
                    )
                };
                if bytes_read < 0 {
                    let e = last_errno();
                    if e != libc::EINTR {
                        ereport(LOG)
                            .with_saved_errno(e)
                            .errcode_for_file_access()
                            .errmsg("could not read from logger pipe: %m")
                            .finish(loc("SysLoggerMain"))?;
                    }
                } else if bytes_read > 0 {
                    bytes_in_logbuffer += bytes_read as usize;
                    process_pipe_input(&mut st, &mut logbuffer, &mut bytes_in_logbuffer);
                    continue;
                } else {
                    st.pipe_eof_seen = true;
                    flush_pipe_input(&mut st, &logbuffer, &mut bytes_in_logbuffer);
                }
            }

            if st.pipe_eof_seen {
                ereport(DEBUG1)
                    .errmsg_internal("logger shutting down")
                    .finish(loc("SysLoggerMain"))?;
                return Ok(());
            }
        }
    };

    match run() {
        Ok(()) => {
            SYSLOGGER_EXITED.store(true, Relaxed);
            ipc::proc_exit(0, g::MyProcPid())
        }
        Err(e) => {
            SYSLOGGER_EXITED.store(true, Relaxed);
            fatal_exit(&e)
        }
    }
}

/// One fd table: the C child's pipe write ends live on as this process's fds
/// 1/2 forever, so C's EOF-when-every-writer-died never happens. At
/// postmaster exit (all other children already gone) the write ends are
/// repointed at /dev/null, which is the last-writer close; then bound-wait
/// for the collector's final flush.
fn syslogger_drain_at_exit(_code: i32, _arg: usize) {
    // wasm32: no /dev/null and no dup2 on WASI; the collector never runs
    // (logging_collector requires pipes), so only the bound-wait remains.
    #[cfg(not(target_family = "wasm"))]
    {
        let devnull = std::ffi::CString::new("/dev/null").unwrap();
        unsafe {
            let fd = libc::open(devnull.as_ptr(), libc::O_WRONLY, 0);
            if fd != -1 {
                libc::dup2(fd, libc::STDOUT_FILENO);
                libc::dup2(fd, libc::STDERR_FILENO);
                libc::close(fd);
            }
        }
    }
    for _ in 0..10_000 {
        if SYSLOGGER_EXITED.load(Relaxed) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

pub fn SysLogger_Start(child_slot: i32) -> PgResult<i32> {
    debug_assert!(Logging_collector());

    if SYSLOG_PIPE_R.load(Relaxed) < 0 {
        let mut fds = [0i32; 2];
        // wasm32: no pipe(2) on WASI — the logging collector cannot exist;
        // the FATAL below is the honest outcome if it is ever requested.
        #[cfg(not(target_family = "wasm"))]
        let pipe_rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        #[cfg(target_family = "wasm")]
        let pipe_rc = -1;
        if pipe_rc < 0 {
            ereport(FATAL)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg("could not create pipe for syslog: %m")
                .finish(loc("SysLogger_Start"))?;
        }
        SYSLOG_PIPE_R.store(fds[0], Relaxed);
        SYSLOG_PIPE_W.store(fds[1], Relaxed);
    }

    let _ = fd::MakePGDirectory(&Log_directory());

    let first_time = time_now();
    FIRST_SYSLOGGER_FILE_TIME.store(first_time, Relaxed);

    // Restart: the dead collector thread's FILE handles were never closed
    // (C's exit did); close them before reopening.
    for slot in [&SYSLOG_FILE, &CSVLOG_FILE, &JSONLOG_FILE] {
        let old = slot.swap(std::ptr::null_mut(), Relaxed);
        if !old.is_null() {
            unsafe { libc::fclose(old) };
        }
    }

    let filename = logfile_getname(first_time, None);
    SYSLOG_FILE.store(logfile_open(&filename, "a", false)?, Relaxed);

    if elog::config::log_destination() & LOG_DESTINATION_CSVLOG != 0 {
        let filename = logfile_getname(first_time, Some(".csv"));
        CSVLOG_FILE.store(logfile_open(&filename, "a", false)?, Relaxed);
    }
    if elog::config::log_destination() & LOG_DESTINATION_JSONLOG != 0 {
        let filename = logfile_getname(first_time, Some(".json"));
        JSONLOG_FILE.store(logfile_open(&filename, "a", false)?, Relaxed);
    }

    let syslogger_pid =
        launch_backend::postmaster_child_launch(BackendType::Logger, child_slot, StartupData::None, None);

    if syslogger_pid == -1 {
        ereport(LOG)
            .errmsg("could not fork system logger: %m")
            .finish(loc("SysLogger_Start"))?;
        return Ok(0);
    }

    if !elog::config::redirection_done() {
        ereport(LOG)
            .errmsg("redirecting log output to logging collector process")
            .errhint(format!(
                "Future log output will appear in directory \"{}\".",
                Log_directory()
            ))
            .finish(loc("SysLogger_Start"))?;

        let pipe_write = SYSLOG_PIPE_W.load(Relaxed);
        // wasm32: unreachable (pipe creation FATALed above); no dup2 on WASI.
        #[cfg(not(target_family = "wasm"))]
        unsafe {
            use std::io::Write as _;
            let _ = std::io::stdout().flush();
            if libc::dup2(pipe_write, libc::STDOUT_FILENO) < 0 {
                ereport(FATAL)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg("could not redirect stdout: %m")
                    .finish(loc("SysLogger_Start"))?;
            }
            let _ = std::io::stderr().flush();
            if libc::dup2(pipe_write, libc::STDERR_FILENO) < 0 {
                ereport(FATAL)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg("could not redirect stderr: %m")
                    .finish(loc("SysLogger_Start"))?;
            }
            libc::close(pipe_write);
        }
        #[cfg(target_family = "wasm")]
        let _ = pipe_write;
        SYSLOG_PIPE_W.store(-1, Relaxed);
        elog::config::set_redirection_done(true);
        ipc::on_proc_exit(syslogger_drain_at_exit, 0);
    }

    // C's postmaster closes its fork-copies of the log FILEs here; these ARE
    // the collector's handles — kept open.

    Ok(syslogger_pid)
}

fn process_pipe_input(st: &mut SysLoggerState, logbuffer: &mut [u8], bytes_in_logbuffer: &mut usize) {
    let mut cursor: usize = 0;
    let mut count: usize = *bytes_in_logbuffer;
    let mut dest: i32 = LOG_DESTINATION_STDERR;

    while count >= PIPE_HEADER_SIZE + 1 {
        let buf = &logbuffer[cursor..];
        let len = u16::from_ne_bytes([buf[2], buf[3]]) as usize;
        let pid = i32::from_ne_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let flags = buf[8];
        let dest_flags =
            flags & (PIPE_PROTO_DEST_STDERR | PIPE_PROTO_DEST_CSVLOG | PIPE_PROTO_DEST_JSONLOG);

        if buf[0] == 0
            && buf[1] == 0
            && len > 0
            && len <= PIPE_MAX_PAYLOAD
            && pid != 0
            && dest_flags.count_ones() == 1
        {
            let chunklen = PIPE_HEADER_SIZE + len;
            if count < chunklen {
                break;
            }

            if flags & PIPE_PROTO_DEST_STDERR != 0 {
                dest = LOG_DESTINATION_STDERR;
            } else if flags & PIPE_PROTO_DEST_CSVLOG != 0 {
                dest = LOG_DESTINATION_CSVLOG;
            } else if flags & PIPE_PROTO_DEST_JSONLOG != 0 {
                dest = LOG_DESTINATION_JSONLOG;
            }

            let payload = &logbuffer[cursor + PIPE_HEADER_SIZE..cursor + chunklen];
            let list = &mut st.buffer_lists[(pid as u32 as usize) % NBUFFER_LISTS];

            if flags & PIPE_PROTO_IS_LAST == 0 {
                if let Some(existing) = list.iter_mut().find(|b| b.pid == pid) {
                    existing.data.extend_from_slice(payload);
                } else if let Some(free) = list.iter_mut().find(|b| b.pid == 0) {
                    free.pid = pid;
                    free.data.extend_from_slice(payload);
                } else {
                    list.push(SaveBuffer { pid, data: payload.to_vec() });
                }
            } else if let Some(existing) = list.iter_mut().find(|b| b.pid == pid) {
                existing.data.extend_from_slice(payload);
                write_syslogger_file(&existing.data, dest);
                existing.pid = 0;
                existing.data.clear();
            } else {
                write_syslogger_file(payload, dest);
            }

            cursor += chunklen;
            count -= chunklen;
        } else {
            let mut chunklen: usize = 1;
            while chunklen < count {
                if logbuffer[cursor + chunklen] == 0 {
                    break;
                }
                chunklen += 1;
            }
            {
                let (head, _) = logbuffer.split_at(cursor + chunklen);
                write_syslogger_file(&head[cursor..], LOG_DESTINATION_STDERR);
            }
            cursor += chunklen;
            count -= chunklen;
        }
    }

    if count > 0 && cursor != 0 {
        logbuffer.copy_within(cursor..cursor + count, 0);
    }
    *bytes_in_logbuffer = count;
}

fn flush_pipe_input(st: &mut SysLoggerState, logbuffer: &[u8], bytes_in_logbuffer: &mut usize) {
    for list in &mut st.buffer_lists {
        for buf in list.iter_mut() {
            if buf.pid != 0 {
                write_syslogger_file(&buf.data, LOG_DESTINATION_STDERR);
                buf.pid = 0;
                buf.data.clear();
            }
        }
    }
    if *bytes_in_logbuffer > 0 {
        write_syslogger_file(&logbuffer[..*bytes_in_logbuffer], LOG_DESTINATION_STDERR);
    }
    *bytes_in_logbuffer = 0;
}

pub fn write_syslogger_file(buffer: &[u8], destination: i32) {
    let csv = CSVLOG_FILE.load(Relaxed);
    let json = JSONLOG_FILE.load(Relaxed);
    let logfile = if destination & LOG_DESTINATION_CSVLOG != 0 && !csv.is_null() {
        csv
    } else if destination & LOG_DESTINATION_JSONLOG != 0 && !json.is_null() {
        json
    } else {
        SYSLOG_FILE.load(Relaxed)
    };

    let rc = if logfile.is_null() {
        0
    } else {
        unsafe { libc::fwrite(buffer.as_ptr().cast(), 1, buffer.len(), logfile) }
    };
    if rc != buffer.len() {
        elog::write_stderr(&elog::errno::replace_percent_m(
            "could not write to log file: %m\n",
            last_errno(),
        ));
    }
}

/// C sets a temporary umask so fopen creates the file as
/// Log_file_mode|S_IWUSR; umask is process-wide here, so create narrow and
/// fchmod to the exact C mode instead.
fn logfile_open(filename: &str, mode: &str, allow_errors: bool) -> PgResult<*mut libc::FILE> {
    let file_mode = ((LOG_FILE_MODE.load(Relaxed) as u32 | 0o200) & 0o777) as libc::mode_t;
    let oflags = libc::O_WRONLY
        | libc::O_CREAT
        | if mode == "w" { libc::O_TRUNC } else { libc::O_APPEND };
    let c_filename = std::ffi::CString::new(filename).expect("log path contains NUL");

    let fh = unsafe {
        let fd = libc::open(c_filename.as_ptr(), oflags, 0o600 as libc::c_uint);
        if fd < 0 {
            std::ptr::null_mut()
        } else {
            libc::fchmod(fd, file_mode);
            let c_mode =
                std::ffi::CString::new(if mode == "w" { "w" } else { "a" }).unwrap();
            let fh = libc::fdopen(fd, c_mode.as_ptr());
            if fh.is_null() {
                libc::close(fd);
            }
            fh
        }
    };

    if !fh.is_null() {
        unsafe { libc::setvbuf(fh, std::ptr::null_mut(), libc::_IOLBF, 0) };
    } else {
        let save_errno = last_errno();
        ereport(if allow_errors { LOG } else { FATAL })
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not open log file \"{filename}\": %m"))
            .finish(loc("logfile_open"))?;
        set_errno(save_errno);
    }

    Ok(fh)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Slot {
    Stderr,
    Csvlog,
    Jsonlog,
}

fn logfile_rotate_dest(
    st: &mut SysLoggerState,
    time_based_rotation: bool,
    size_rotation_for: i32,
    fntime: pg_time_t,
    target_dest: i32,
    which: Slot,
) -> PgResult<bool> {
    let file_slot = match which {
        Slot::Stderr => &SYSLOG_FILE,
        Slot::Csvlog => &CSVLOG_FILE,
        Slot::Jsonlog => &JSONLOG_FILE,
    };
    macro_rules! last_name {
        () => {
            match which {
                Slot::Stderr => &mut st.last_sys_file_name,
                Slot::Csvlog => &mut st.last_csv_file_name,
                Slot::Jsonlog => &mut st.last_json_file_name,
            }
        };
    }

    if (elog::config::log_destination() & target_dest) == 0 && target_dest != LOG_DESTINATION_STDERR
    {
        let old = file_slot.swap(std::ptr::null_mut(), Relaxed);
        if !old.is_null() {
            unsafe { libc::fclose(old) };
        }
        *last_name!() = None;
        return Ok(true);
    }

    if !time_based_rotation && (size_rotation_for & target_dest) == 0 {
        return Ok(true);
    }

    let log_file_ext = match which {
        Slot::Stderr => None,
        Slot::Csvlog => Some(".csv"),
        Slot::Jsonlog => Some(".json"),
    };

    let filename = logfile_getname(fntime, log_file_ext);

    let fh = if LOG_TRUNCATE_ON_ROTATION.load(Relaxed)
        && time_based_rotation
        && last_name!().is_some()
        && last_name!().as_deref() != Some(filename.as_str())
    {
        logfile_open(&filename, "w", true)?
    } else {
        logfile_open(&filename, "a", true)?
    };

    if fh.is_null() {
        let e = last_errno();
        if e != libc::ENFILE && e != libc::EMFILE {
            ereport(LOG)
                .errmsg("disabling automatic rotation (use SIGHUP to re-enable)")
                .finish(loc("logfile_rotate_dest"))?;
            st.rotation_disabled = true;
        }
        return Ok(false);
    }

    let old = file_slot.swap(fh, Relaxed);
    if !old.is_null() {
        unsafe { libc::fclose(old) };
    }
    *last_name!() = Some(filename);

    Ok(true)
}

fn logfile_rotate(time_based_rotation: bool, size_rotation_for: i32, st: &mut SysLoggerState) -> PgResult<()> {
    ROTATION_REQUESTED.store(false, Relaxed);

    let fntime = if time_based_rotation { st.next_rotation_time } else { time_now() };

    if !logfile_rotate_dest(st, time_based_rotation, size_rotation_for, fntime, LOG_DESTINATION_STDERR, Slot::Stderr)? {
        return Ok(());
    }
    if !logfile_rotate_dest(st, time_based_rotation, size_rotation_for, fntime, LOG_DESTINATION_CSVLOG, Slot::Csvlog)? {
        return Ok(());
    }
    if !logfile_rotate_dest(st, time_based_rotation, size_rotation_for, fntime, LOG_DESTINATION_JSONLOG, Slot::Jsonlog)? {
        return Ok(());
    }

    update_metainfo_datafile(st)?;
    set_next_rotation_time(st);
    Ok(())
}

fn logfile_getname(timestamp: pg_time_t, suffix: Option<&str>) -> String {
    let mut filename = format!("{}/", Log_directory());
    if filename.len() >= MAXPGPATH {
        filename.truncate(MAXPGPATH - 1);
    }

    let len = filename.len();
    let tz = pgtz::log_timezone().expect("log_timezone not initialized");
    if let Some(tm) = localtime::pg_localtime(timestamp, tz) {
        let mut buf = [0u8; MAXPGPATH];
        let cap = (MAXPGPATH - len).saturating_sub(1);
        if let Some(n) = strftime::pg_strftime(&mut buf[..cap], Log_filename().as_bytes(), &tm) {
            filename.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }

    if let Some(suffix) = suffix {
        let mut len = filename.len();
        if len > 4 && filename.as_bytes()[len - 4..] == *b".log" {
            len -= 4;
            filename.truncate(len);
        }
        let keep = (MAXPGPATH - len).saturating_sub(1).min(suffix.len());
        filename.push_str(&suffix[..keep]);
    }

    filename
}

fn set_next_rotation_time(st: &mut SysLoggerState) {
    if Log_RotationAge() <= 0 {
        return;
    }

    let rotinterval = Log_RotationAge() as i64 * SECS_PER_MINUTE;
    let mut now = time_now();
    let tz = pgtz::log_timezone().expect("log_timezone not initialized");
    let gmtoff = localtime::pg_localtime(now, tz).map_or(0, |tm| tm.tm_gmtoff);
    now += gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= gmtoff;
    st.next_rotation_time = now;
}

/// pg_mode_mask (common/file_perm.c) recomputed from the inherited dir
/// create mode: 0700 -> 0077, 0750 -> 0027.
fn pg_mode_mask() -> u32 {
    0o777 & !(g::data_directory_mode() as u32)
}

fn update_metainfo_datafile(st: &SysLoggerState) -> PgResult<()> {
    let dest = elog::config::log_destination();

    if dest & (LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG | LOG_DESTINATION_JSONLOG) == 0 {
        match std::fs::remove_file(LOG_METAINFO_DATAFILE) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                ereport(LOG)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{LOG_METAINFO_DATAFILE}\": %m"))
                    .finish(loc("update_metainfo_datafile"))?;
            }
        }
        return Ok(());
    }

    // C fopen("w") under umask(pg_mode_mask); exact mode via fchmod (shared
    // process umask, see logfile_open).
    let c_tmp = std::ffi::CString::new(LOG_METAINFO_DATAFILE_TMP).unwrap();
    let fh = unsafe {
        let fd = libc::open(
            c_tmp.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o600 as libc::c_uint,
        );
        if fd < 0 {
            std::ptr::null_mut()
        } else {
            libc::fchmod(fd, (0o666 & !pg_mode_mask()) as libc::mode_t);
            let c_mode = std::ffi::CString::new("w").unwrap();
            let fh = libc::fdopen(fd, c_mode.as_ptr());
            if fh.is_null() {
                libc::close(fd);
            }
            fh
        }
    };
    if fh.is_null() {
        ereport(LOG)
            .with_saved_errno(last_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{LOG_METAINFO_DATAFILE_TMP}\": %m"))
            .finish(loc("update_metainfo_datafile"))?;
        return Ok(());
    }
    unsafe { libc::setvbuf(fh, std::ptr::null_mut(), libc::_IOLBF, 0) };

    let entries: [(i32, &str, &Option<String>); 3] = [
        (LOG_DESTINATION_STDERR, "stderr", &st.last_sys_file_name),
        (LOG_DESTINATION_CSVLOG, "csvlog", &st.last_csv_file_name),
        (LOG_DESTINATION_JSONLOG, "jsonlog", &st.last_json_file_name),
    ];
    for (bit, label, name) in entries {
        let Some(name) = name else { continue };
        if dest & bit == 0 {
            continue;
        }
        let line = format!("{label} {name}\n");
        let written = unsafe { libc::fwrite(line.as_ptr().cast(), 1, line.len(), fh) };
        if written != line.len() {
            let e = last_errno();
            unsafe { libc::fclose(fh) };
            ereport(LOG)
                .with_saved_errno(e)
                .errcode_for_file_access()
                .errmsg(format!("could not write file \"{LOG_METAINFO_DATAFILE_TMP}\": %m"))
                .finish(loc("update_metainfo_datafile"))?;
            return Ok(());
        }
    }
    unsafe { libc::fclose(fh) };

    if std::fs::rename(LOG_METAINFO_DATAFILE_TMP, LOG_METAINFO_DATAFILE).is_err() {
        ereport(LOG)
            .with_saved_errno(last_errno())
            .errcode_for_file_access()
            .errmsg(format!(
                "could not rename file \"{LOG_METAINFO_DATAFILE_TMP}\" to \"{LOG_METAINFO_DATAFILE}\": %m"
            ))
            .finish(loc("update_metainfo_datafile"))?;
    }

    Ok(())
}

pub fn CheckLogrotateSignal() -> bool {
    std::fs::metadata(LOGROTATE_SIGNAL_FILE).is_ok()
}

pub fn RemoveLogrotateSignalFiles() {
    let _ = std::fs::remove_file(LOGROTATE_SIGNAL_FILE);
}

pub fn init_seams() {
    use guc_tables::{vars, GucVarAccessors};

    vars::Logging_collector.install(GucVarAccessors {
        get: Logging_collector,
        set: |v| LOGGING_COLLECTOR.store(v, Relaxed),
    });
    vars::Log_RotationAge.install(GucVarAccessors {
        get: || LOG_ROTATION_AGE.load(Relaxed),
        set: |v| LOG_ROTATION_AGE.store(v, Relaxed),
    });
    vars::Log_RotationSize.install(GucVarAccessors {
        get: || LOG_ROTATION_SIZE.load(Relaxed),
        set: |v| LOG_ROTATION_SIZE.store(v, Relaxed),
    });
    vars::Log_truncate_on_rotation.install(GucVarAccessors {
        get: || LOG_TRUNCATE_ON_ROTATION.load(Relaxed),
        set: |v| LOG_TRUNCATE_ON_ROTATION.store(v, Relaxed),
    });
    vars::Log_file_mode.install(GucVarAccessors {
        get: || LOG_FILE_MODE.load(Relaxed),
        set: |v| LOG_FILE_MODE.store(v, Relaxed),
    });
    vars::Log_directory.install(GucVarAccessors {
        get: || Some(Log_directory()),
        set: |v| *LOG_DIRECTORY.lock().unwrap() = v,
    });
    vars::Log_filename.install(GucVarAccessors {
        get: || Some(Log_filename()),
        set: |v| *LOG_FILENAME.lock().unwrap() = v,
    });
    syslogger_seams::check_logrotate_signal::set(CheckLogrotateSignal);
    syslogger_seams::remove_logrotate_signal_files::set(RemoveLogrotateSignalFiles);
    syslogger_seams::write_syslogger_file::set(write_syslogger_file);
    syslogger_seams::sys_logger_main::set(SysLoggerMain);
}
