//! Port of `src/backend/postmaster/syslogger.c` (PostgreSQL 18.3) — the
//! system logger (logging collector). It catches all stderr output from the
//! postmaster, backends, and other subprocesses by redirecting to a pipe, and
//! writes it to a set of logfiles, rotating them on a size
//! (`Log_RotationSize`) or age (`Log_RotationAge`) limit, with optional
//! CSV/JSON destination fan-out.
//!
//! # Design notes / sanctioned divergences (audit against these)
//!
//! - **Per-process statics are `thread_local!`** (AGENTS.md "Backend-global
//!   state"): the C file-scope statics (`syslogFile`, `next_rotation_time`,
//!   `buffer_lists`, ...) and the GUC globals ([`config`], owned here with
//!   setters for the guc/postmaster units, following
//!   `backend-utils-error::config`).
//! - **OS edges are direct `libc` calls** (fopen/fwrite/ftello/fclose with
//!   `setvbuf` line buffering, pipe/read/dup2/close, umask, stat/unlink/
//!   rename, sigprocmask), as in the elog port. `FILE *` is carried as
//!   `*mut libc::FILE` (null == C NULL).
//! - **PgResult instead of longjmp**: functions whose C bodies can
//!   `ereport(ERROR)`-or-higher (here only via callees: ProcessConfigFile,
//!   WaitEventSet calls, plus FATAL paths) return
//!   `types_error::PgResult`; FATAL (like every severity >= ERROR)
//!   propagates as `Err(PgError)` if `errfinish` returns it, per the settled
//!   AGENTS.md convention.
//! - **Cross-unit calls**: direct deps where acyclic (elog, interrupt,
//!   pqsignal masks); not-yet-ported owners via their seam crates
//!   (waiteventset, latch, guc, fd.c's `MakePGDirectory`, `pg_localtime`,
//!   `pg_strftime`, `postmaster_child_launch`, `init_ps_display`,
//!   `pqsignal`, `proc_exit`, `MyBackendType`) — those panic until the
//!   owners land. Foreign per-backend global *values* (`MyStartTime`,
//!   `pg_mode_mask`) are explicit [`SysLoggerMain`] parameters; the
//!   `log_timezone` GUC mirror lives on [`config`] (set by the pgtz/guc
//!   owner when it lands).
//! - **Sanctioned divergence — infallible std allocation**: the C file's
//!   pallocs (`logfile_getname`'s MAXPGPATH buffer, the per-pid save
//!   buffers' appendBinaryStringInfo growth) can `ereport(ERROR,
//!   OUT_OF_MEMORY)`; this daemon's allocations are small, process-lifetime
//!   std `String`/`Vec` ones with no memory context to charge, so they stay
//!   infallible (abort-on-OOM) instead of threading `Mcx` through the
//!   logging path.
//! - **`MemoryContextDelete(PostmasterContext)` is not reproduced**: under
//!   the mcx RAII model (docs/mctx-design.md) there is no ambient
//!   `PostmasterContext` global; releasing the postmaster's working memory in
//!   the child belongs to the child-launch machinery that owns that context
//!   value.
//! - `MyBackendType = B_LOGGER` ports as the globals.c owner seam **plus**
//!   `backend_utils_error::config::set_am_syslogger(true)` (elog's
//!   per-backend mirror of `MyBackendType == B_LOGGER`).
//! - The `WIN32` paths (`pipeThread`, `_setmode`, CRLF) and the
//!   `EXEC_BACKEND` `syslogger_fdget`/`syslogger_fdopen` re-open-via-fd path
//!   are not ported — this targets the `!WIN32`, non-`EXEC_BACKEND` build,
//!   like the rest of the repo.
//! - `pg_number_of_ones[dest_flags] == 1` is `u8::count_ones() == 1`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::io::Write as _;
use std::ptr;

use backend_postmaster_interrupt::{
    ConfigReloadPending, SetConfigReloadPending, SignalHandlerForConfigReload,
};
use backend_utils_error::config as elog_config;
use backend_utils_error::{ereport, errno};
use backend_storage_ipc_waiteventset_seams::WaitEventSet;
use types_core::init::BackendType;
use types_core::{pg_time_t, MAXPGPATH, PGINVALID_SOCKET};
use types_error::{
    PgResult, DEBUG1, FATAL, LOG, LOG_DESTINATION_CSVLOG, LOG_DESTINATION_JSONLOG,
    LOG_DESTINATION_STDERR,
};
use types_pgstat::wait_event::WAIT_EVENT_SYSLOGGER_MAIN;
use types_signal::SigHandler;
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WL_LATCH_SET, WL_SOCKET_READABLE};

pub mod config;

/// `ereport` location capture (`__FILE__` / `__LINE__`; `__func__` is not
/// available in Rust and is left unset, as `elog()` in the elog crate does).
macro_rules! here {
    () => {
        types_error::ErrorLocation {
            filename: Some(file!().to_string()),
            lineno: line!() as i32,
            funcname: None,
        }
    };
}

// ---------------------------------------------------------------------------
// pipe protocol layout (postmaster/syslogger.h)
// ---------------------------------------------------------------------------

/// `PIPE_CHUNK_SIZE` (`syslogger.h`): the OS `PIPE_BUF` clamped to 64K, so
/// both sides share the OS constant. Must match elog's `write_pipe_chunks`.
pub const PIPE_CHUNK_SIZE: usize = if libc::PIPE_BUF > 65536 {
    65536
} else {
    libc::PIPE_BUF
};

/// `PIPE_HEADER_SIZE = offsetof(PipeProtoHeader, data)`:
/// `nuls[2]` (2) + `uint16 len` (2) + `int32 pid` (4) + `bits8 flags` (1) —
/// `data` is `char[]`, so no tail padding before it.
pub const PIPE_HEADER_SIZE: usize = 9;

/// `PIPE_MAX_PAYLOAD`.
pub const PIPE_MAX_PAYLOAD: usize = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE;

/// `READ_BUF_SIZE` — temp read buffer twice as big as a chunk, so a leftover
/// fragment can be moved down and a full chunk still fits.
const READ_BUF_SIZE: usize = 2 * PIPE_CHUNK_SIZE;

/* flag bits for PipeProtoHeader->flags */
const PIPE_PROTO_IS_LAST: u8 = 0x01;
const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;

/// Log rotation signal file path, relative to $PGDATA.
const LOGROTATE_SIGNAL_FILE: &str = "logrotate";

/// Name of files saving meta-data about the log files currently in use.
pub const LOG_METAINFO_DATAFILE: &str = "current_logfiles";
pub const LOG_METAINFO_DATAFILE_TMP: &str = "current_logfiles.tmp";

/// `NBUFFER_LISTS` — number of per-pid save-buffer lists.
const NBUFFER_LISTS: usize = 256;

// From datetime.h, compile-time constants in the C.
const HOURS_PER_DAY: i32 = 24;
const MINS_PER_HOUR: i32 = 60;
const SECS_PER_MINUTE: i32 = 60;

/// The C initializer of `Log_RotationAge` (minutes).
pub const DEFAULT_LOG_ROTATION_AGE: i32 = HOURS_PER_DAY * MINS_PER_HOUR;
/// The C initializer of `Log_RotationSize` (kilobytes).
pub const DEFAULT_LOG_ROTATION_SIZE: i32 = 10 * 1024;

// ---------------------------------------------------------------------------
// Private state (file-scope statics in the C source)
// ---------------------------------------------------------------------------

/// `save_buffer`: per-source-pid accumulation of partial messages. An
/// inactive (reusable) slot has `pid == 0`; its `data` is empty rather than
/// C's "undefined contents".
struct SaveBuffer {
    pid: i32,
    data: Vec<u8>,
}

thread_local! {
    /// `static pg_time_t next_rotation_time`.
    static NEXT_ROTATION_TIME: Cell<pg_time_t> = const { Cell::new(0) };
    /// `static bool pipe_eof_seen`.
    static PIPE_EOF_SEEN: Cell<bool> = const { Cell::new(false) };
    /// `static bool rotation_disabled`.
    static ROTATION_DISABLED: Cell<bool> = const { Cell::new(false) };
    /// `static FILE *syslogFile`.
    static SYSLOG_FILE: Cell<*mut libc::FILE> = const { Cell::new(ptr::null_mut()) };
    /// `static FILE *csvlogFile`.
    static CSVLOG_FILE: Cell<*mut libc::FILE> = const { Cell::new(ptr::null_mut()) };
    /// `static FILE *jsonlogFile`.
    static JSONLOG_FILE: Cell<*mut libc::FILE> = const { Cell::new(ptr::null_mut()) };
    /// `NON_EXEC_STATIC pg_time_t first_syslogger_file_time`.
    static FIRST_SYSLOGGER_FILE_TIME: Cell<pg_time_t> = const { Cell::new(0) };
    /// `static char *last_sys_file_name`.
    static LAST_SYS_FILE_NAME: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `static char *last_csv_file_name`.
    static LAST_CSV_FILE_NAME: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `static char *last_json_file_name`.
    static LAST_JSON_FILE_NAME: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `static List *buffer_lists[NBUFFER_LISTS]` (sized lazily on first use).
    static BUFFER_LISTS: RefCell<Vec<Vec<SaveBuffer>>> = const { RefCell::new(Vec::new()) };
    /// `static volatile sig_atomic_t rotation_requested`.
    static ROTATION_REQUESTED: Cell<bool> = const { Cell::new(false) };
}

fn with_buffer_lists<R>(f: impl FnOnce(&mut Vec<Vec<SaveBuffer>>) -> R) -> R {
    BUFFER_LISTS.with(|bl| {
        let mut bl = bl.borrow_mut();
        if bl.is_empty() {
            bl.resize_with(NBUFFER_LISTS, Vec::new);
        }
        f(&mut bl)
    })
}

/// Which `(FILE *, last_file_name)` destination slot a rotation step operates
/// on — the owned stand-in for `logfile_rotate_dest`'s `char
/// **last_file_name, FILE **logFile` out-parameters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Slot {
    Stderr,
    Csvlog,
    Jsonlog,
}

fn slot_file(which: Slot) -> *mut libc::FILE {
    match which {
        Slot::Stderr => SYSLOG_FILE.get(),
        Slot::Csvlog => CSVLOG_FILE.get(),
        Slot::Jsonlog => JSONLOG_FILE.get(),
    }
}

fn set_slot_file(which: Slot, file: *mut libc::FILE) {
    match which {
        Slot::Stderr => SYSLOG_FILE.set(file),
        Slot::Csvlog => CSVLOG_FILE.set(file),
        Slot::Jsonlog => JSONLOG_FILE.set(file),
    }
}

fn slot_last_name(which: Slot) -> Option<String> {
    match which {
        Slot::Stderr => LAST_SYS_FILE_NAME.with(|c| c.borrow().clone()),
        Slot::Csvlog => LAST_CSV_FILE_NAME.with(|c| c.borrow().clone()),
        Slot::Jsonlog => LAST_JSON_FILE_NAME.with(|c| c.borrow().clone()),
    }
}

fn set_slot_last_name(which: Slot, name: Option<String>) {
    match which {
        Slot::Stderr => LAST_SYS_FILE_NAME.with(|c| *c.borrow_mut() = name),
        Slot::Csvlog => LAST_CSV_FILE_NAME.with(|c| *c.borrow_mut() = name),
        Slot::Jsonlog => LAST_JSON_FILE_NAME.with(|c| *c.borrow_mut() = name),
    }
}

/// Restore C `errno` (logfile_open's contract leaves errno valid for the
/// caller's ENFILE/EMFILE classification).
fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    unsafe {
        *libc::__errno_location() = value;
    }
}

fn cstring(s: &str) -> CString {
    CString::new(s).expect("path contains interior NUL byte")
}

// ===========================================================================
// SysLoggerMain — main entry point for syslogger process
// ===========================================================================

/// `void SysLoggerMain(const void *startup_data, size_t startup_data_len)`.
///
/// `pg_noreturn` in C: normal exit is `proc_exit(0)` on pipe EOF (through the
/// `backend-storage-ipc-seams` seam, which never returns); an
/// `ereport(ERROR)` from a callee — impossible to catch here in C without a
/// handler, hence promoted — surfaces as the `Err` return.
///
/// `start_time` is the C `MyStartTime`, `mode_mask` the C `pg_mode_mask`
/// (`common/file_perm.c`), and `my_latch` C's `MyLatch` (globals.c),
/// registered in the wait set and reset each loop iteration — foreign
/// per-backend/process globals taken as explicit parameters per AGENTS.md;
/// the child-launch machinery that sets them passes them in. (The SIGUSR1
/// handler still sets the latch through the `set_latch_my_latch` seam, the
/// signal-safe shape.)
pub fn SysLoggerMain(
    startup_data: &[u8],
    start_time: pg_time_t,
    mode_mask: u32,
    my_latch: LatchHandle,
) -> PgResult<()> {
    let mut logbuffer = [0u8; READ_BUF_SIZE];
    let mut bytes_in_logbuffer: usize = 0;

    // Assert(startup_data_len == 0);  (non-EXEC_BACKEND)
    debug_assert!(startup_data.is_empty());

    // C releases PostmasterContext here; under the mcx RAII model that
    // context is an owned value of the child-launch machinery (see crate
    // docs), so there is nothing to delete in this crate.

    // now = MyStartTime (passed in by the child-launch machinery).
    let mut now: pg_time_t = start_time;

    // MyBackendType = B_LOGGER (plus elog's per-backend mirror of it).
    backend_utils_init_small_seams::set_my_backend_type::call(BackendType::Logger);
    elog_config::set_am_syslogger(true);
    backend_utils_misc_more_seams::init_ps_display::call(None);

    // If we restarted, our stderr is already redirected into our own input
    // pipe. Point stderr to /dev/null: all interesting messages generated in
    // the syslogger come through elog.c / write_syslogger_file.
    if elog_config::redirection_done() {
        let devnull = cstring("/dev/null");
        let fd = unsafe { libc::open(devnull.as_ptr(), libc::O_WRONLY, 0) };

        // The closes might look redundant, but they are not: we want to be
        // darn sure the pipe gets closed even if the open failed. We can
        // survive running with stderr pointing nowhere, but we can't afford
        // extra pipe input descriptors hanging around. No point checking for
        // failure of close/dup2 here either.
        unsafe {
            libc::close(libc::STDOUT_FILENO);
            libc::close(libc::STDERR_FILENO);
            if fd != -1 {
                libc::dup2(fd, libc::STDOUT_FILENO);
                libc::dup2(fd, libc::STDERR_FILENO);
                libc::close(fd);
            }
        }
    }

    // Also close our copy of the write end of the pipe, to ensure we can
    // detect pipe EOF correctly. (In the restart case the postmaster already
    // did this.)
    let mut pipe = config::syslog_pipe();
    if pipe[1] >= 0 {
        unsafe {
            libc::close(pipe[1]);
        }
    }
    pipe[1] = -1;
    config::set_syslog_pipe(pipe);

    // Properly accept or ignore signals the postmaster might send us.
    //
    // Note: we ignore all termination signals, and instead exit only when all
    // upstream processes are gone, to ensure we don't miss any dying gasps of
    // broken backends...
    let pqsignal = port_pqsignal_seams::pqsignal::call;
    pqsignal(libc::SIGHUP, SigHandler::Handler(sighup_handler));
    pqsignal(libc::SIGINT, SigHandler::Ignore);
    pqsignal(libc::SIGTERM, SigHandler::Ignore);
    pqsignal(libc::SIGQUIT, SigHandler::Ignore);
    pqsignal(libc::SIGALRM, SigHandler::Ignore);
    pqsignal(libc::SIGPIPE, SigHandler::Ignore);
    pqsignal(libc::SIGUSR1, SigHandler::Handler(sigUsr1Handler)); /* request log rotation */
    pqsignal(libc::SIGUSR2, SigHandler::Ignore);

    // Reset some signals that are accepted by postmaster but not here.
    pqsignal(libc::SIGCHLD, SigHandler::Default);

    let masks = backend_libpq_pqsignal::signal_masks();
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), ptr::null_mut());
    }

    // Remember active logfiles' name(s), recomputed from the reference time.
    let first_time = FIRST_SYSLOGGER_FILE_TIME.get();
    LAST_SYS_FILE_NAME.with(|c| *c.borrow_mut() = Some(logfile_getname(first_time, None)));
    if !CSVLOG_FILE.get().is_null() {
        LAST_CSV_FILE_NAME.with(|c| *c.borrow_mut() = Some(logfile_getname(first_time, Some(".csv"))));
    }
    if !JSONLOG_FILE.get().is_null() {
        LAST_JSON_FILE_NAME
            .with(|c| *c.borrow_mut() = Some(logfile_getname(first_time, Some(".json"))));
    }

    // remember active logfile parameters
    let mut current_log_dir = config::log_directory();
    let mut current_log_filename = config::log_filename();
    let mut current_log_rotation_age = config::log_rotation_age();
    // set next planned rotation time
    set_next_rotation_time();
    update_metainfo_datafile(mode_mask)?;

    // Reset whereToSendOutput, as the postmaster will do (but hasn't yet, at
    // the point where we forked), to prevent duplicate output of messages
    // from syslogger itself.
    elog_config::set_where_to_send_output(types_dest::CommandDest::None);

    // Set up a reusable WaitEventSet for our latch and the pipe's read end.
    //
    // Unlike all other postmaster child processes, we ignore postmaster death
    // because we want to collect final log output from all backends and exit
    // last — we run until we see EOF on the syslog pipe.
    let wes = WaitEventSet::create(2)?;
    wes.add_event(WL_LATCH_SET, PGINVALID_SOCKET, Some(my_latch))?;
    wes.add_event(WL_SOCKET_READABLE, config::syslog_pipe()[0], None)?;

    // main worker loop
    loop {
        let mut time_based_rotation = false;
        let mut size_rotation_for: i32 = 0;
        let cur_timeout: i64;

        // Clear any already-pending wakeups.
        backend_storage_ipc_latch_seams::reset_latch::call(my_latch);

        // Process any requests or signals received recently.
        if ConfigReloadPending() {
            SetConfigReloadPending(false);
            backend_utils_misc_guc_file_seams::process_config_file::call(types_guc::PGC_SIGHUP)?;

            // Check if the log directory or filename pattern changed in
            // postgresql.conf. If so, force rotation to make sure we're
            // writing the logfiles in the right place.
            if config::log_directory() != current_log_dir {
                current_log_dir = config::log_directory();
                ROTATION_REQUESTED.set(true);

                // Also, create new directory if not present; ignore errors.
                backend_storage_file_fd_seams::make_pg_directory::call(&current_log_dir);
            }
            if config::log_filename() != current_log_filename {
                current_log_filename = config::log_filename();
                ROTATION_REQUESTED.set(true);
            }

            // Force a rotation if CSVLOG output was just turned on or off and
            // we need to open or close csvlogFile accordingly.
            if ((elog_config::log_destination() & LOG_DESTINATION_CSVLOG) != 0)
                != !CSVLOG_FILE.get().is_null()
            {
                ROTATION_REQUESTED.set(true);
            }

            // Force a rotation if JSONLOG output was just turned on or off
            // and we need to open or close jsonlogFile accordingly.
            if ((elog_config::log_destination() & LOG_DESTINATION_JSONLOG) != 0)
                != !JSONLOG_FILE.get().is_null()
            {
                ROTATION_REQUESTED.set(true);
            }

            // If rotation time parameter changed, reset next rotation time,
            // but don't immediately force a rotation.
            if current_log_rotation_age != config::log_rotation_age() {
                current_log_rotation_age = config::log_rotation_age();
                set_next_rotation_time();
            }

            // If we had a rotation-disabling failure, re-enable rotation
            // attempts after SIGHUP, and force one immediately.
            if ROTATION_DISABLED.get() {
                ROTATION_DISABLED.set(false);
                ROTATION_REQUESTED.set(true);
            }

            // Force rewriting last log filename when reloading configuration.
            // Even if rotation_requested is false, log_destination may have
            // been changed and we don't want to wait the next file rotation.
            update_metainfo_datafile(mode_mask)?;
        }

        if config::log_rotation_age() > 0 && !ROTATION_DISABLED.get() {
            // Do a logfile rotation if it's time.
            now = unsafe { libc::time(ptr::null_mut()) };
            if now >= NEXT_ROTATION_TIME.get() {
                ROTATION_REQUESTED.set(true);
                time_based_rotation = true;
            }
        }

        if !ROTATION_REQUESTED.get() && config::log_rotation_size() > 0 && !ROTATION_DISABLED.get()
        {
            // Do a rotation if file is too big. (syslogFile is always open in
            // the running collector; C calls ftello on it unconditionally.)
            let limit = config::log_rotation_size() as i64 * 1024;
            if unsafe { libc::ftello(SYSLOG_FILE.get()) } >= limit {
                ROTATION_REQUESTED.set(true);
                size_rotation_for |= LOG_DESTINATION_STDERR;
            }
            let csv = CSVLOG_FILE.get();
            if !csv.is_null() && unsafe { libc::ftello(csv) } >= limit {
                ROTATION_REQUESTED.set(true);
                size_rotation_for |= LOG_DESTINATION_CSVLOG;
            }
            let json = JSONLOG_FILE.get();
            if !json.is_null() && unsafe { libc::ftello(json) } >= limit {
                ROTATION_REQUESTED.set(true);
                size_rotation_for |= LOG_DESTINATION_JSONLOG;
            }
        }

        if ROTATION_REQUESTED.get() {
            // Force rotation when both values are zero. It means the request
            // was sent by pg_rotate_logfile() or "pg_ctl logrotate".
            if !time_based_rotation && size_rotation_for == 0 {
                size_rotation_for =
                    LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG | LOG_DESTINATION_JSONLOG;
            }
            logfile_rotate(time_based_rotation, size_rotation_for, mode_mask)?;
        }

        // Calculate time till next time-based rotation, so that we don't
        // sleep longer than that. Beware of overflow: with large
        // Log_RotationAge, next_rotation_time could be more than INT_MAX msec
        // in the future; wait no more than INT_MAX msec and try again.
        if config::log_rotation_age() > 0 && !ROTATION_DISABLED.get() {
            let mut delay: pg_time_t = NEXT_ROTATION_TIME.get() - now;
            if delay > 0 {
                if delay > (i32::MAX / 1000) as pg_time_t {
                    delay = (i32::MAX / 1000) as pg_time_t;
                }
                cur_timeout = delay * 1000; /* msec */
            } else {
                cur_timeout = 0;
            }
        } else {
            cur_timeout = -1;
        }

        // Sleep until there's something to do.
        let mut occurred = [types_storage::waiteventset::WaitEvent::default(); 1];
        let noccurred = wes.wait(cur_timeout, &mut occurred, WAIT_EVENT_SYSLOGGER_MAIN)?;

        if noccurred == 1 && occurred[0].events == WL_SOCKET_READABLE {
            let bytes_read = unsafe {
                libc::read(
                    config::syslog_pipe()[0],
                    logbuffer.as_mut_ptr().add(bytes_in_logbuffer) as *mut libc::c_void,
                    READ_BUF_SIZE - bytes_in_logbuffer,
                )
            };
            if bytes_read < 0 {
                let e = errno::current_errno();
                if e != libc::EINTR {
                    ereport(LOG)
                        .with_saved_errno(e)
                        .errcode_for_socket_access()
                        .errmsg("could not read from logger pipe: %m")
                        .finish(here!())?;
                }
            } else if bytes_read > 0 {
                bytes_in_logbuffer += bytes_read as usize;
                process_pipe_input(&mut logbuffer, &mut bytes_in_logbuffer);
                continue;
            } else {
                // Zero bytes read when select() is saying read-ready means
                // EOF on the pipe: there are no longer any processes with the
                // pipe write end open. Therefore, the postmaster and all
                // backends are shut down, and we are done.
                PIPE_EOF_SEEN.set(true);

                // if there's any data left then force it out now
                flush_pipe_input(&mut logbuffer, &mut bytes_in_logbuffer);
            }
        }

        if PIPE_EOF_SEEN.get() {
            // Seeing this message on the real stderr is annoying — so we make
            // it DEBUG1 to suppress in normal use.
            ereport(DEBUG1)
                .errmsg_internal("logger shutting down")
                .finish(here!())?;

            // Normal exit from the syslogger is here. Note that we
            // deliberately do not close syslogFile before exiting; this is to
            // allow for the possibility of elog messages being generated
            // inside proc_exit. Regular exit() will take care of flushing and
            // closing stdio channels.
            backend_storage_ipc_seams::proc_exit::call(
                0,
                backend_utils_init_small_seams::my_proc_pid::call(),
            );
        }
    }
}

/// SIGHUP: `SignalHandlerForConfigReload` with the C `SIGNAL_ARGS` shape.
fn sighup_handler(_postgres_signal_arg: i32) {
    SignalHandlerForConfigReload();
}

// ===========================================================================
// SysLogger_Start — postmaster subroutine to start a syslogger subprocess
// ===========================================================================

/// `int SysLogger_Start(int child_slot)`.
///
/// Runs in the postmaster: (re)creates the stderr pipe, the initial
/// logfile(s), forks the collector, redirects stderr into the pipe (first
/// time only), and closes the postmaster's copies of the file handles.
/// Returns the collector pid, or `0` on fork failure.
pub fn SysLogger_Start(child_slot: i32) -> PgResult<i32> {
    debug_assert!(config::logging_collector());

    // If first time through, create the pipe which will receive stderr
    // output.
    //
    // If the syslogger crashes and needs to be restarted, we continue to use
    // the same pipe (indeed must do so, since extant backends will be writing
    // into that pipe). This means the postmaster must continue to hold the
    // read end of the pipe open, so we can pass it down to the reincarnated
    // syslogger.
    //
    // Note we don't bother counting the pipe FDs via
    // Reserve/ReleaseExternalFD; there's no real need to account for them in
    // the postmaster or syslogger process.
    if config::syslog_pipe()[0] < 0 {
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            ereport(FATAL)
                .with_saved_errno(errno::current_errno())
                .errcode_for_socket_access()
                .errmsg("could not create pipe for syslog: %m")
                .finish(here!())?;
        } else {
            config::set_syslog_pipe(fds);
        }
    }

    // Create log directory if not present; ignore errors.
    backend_storage_file_fd_seams::make_pg_directory::call(&config::log_directory());

    // The initial logfile is created right in the postmaster, to verify that
    // the Log_directory is writable. We save the reference time so that the
    // syslogger child process can recompute this file name. Note we always
    // append here, we won't overwrite any existing file (by definition this
    // is not a time-based rotation).
    let first_time = unsafe { libc::time(ptr::null_mut()) };
    FIRST_SYSLOGGER_FILE_TIME.set(first_time);

    let filename = logfile_getname(first_time, None);
    SYSLOG_FILE.set(logfile_open(&filename, "a", false)?);

    // Likewise for the initial CSV log file, if that's enabled. (Note that we
    // open syslogFile even when only CSV output is nominally enabled, since
    // some code paths will write to syslogFile anyway.)
    if elog_config::log_destination() & LOG_DESTINATION_CSVLOG != 0 {
        let filename = logfile_getname(first_time, Some(".csv"));
        CSVLOG_FILE.set(logfile_open(&filename, "a", false)?);
    }

    // Likewise for the initial JSON log file, if that's enabled.
    if elog_config::log_destination() & LOG_DESTINATION_JSONLOG != 0 {
        let filename = logfile_getname(first_time, Some(".json"));
        JSONLOG_FILE.set(logfile_open(&filename, "a", false)?);
    }

    let syslogger_pid = backend_postmaster_launch_backend_seams::postmaster_child_launch::call(
        BackendType::Logger,
        child_slot,
        &[],
    );

    if syslogger_pid == -1 {
        ereport(LOG)
            .with_saved_errno(errno::current_errno())
            .errmsg("could not fork system logger: %m")
            .finish(here!())?;
        return Ok(0);
    }

    // success, in postmaster

    // now we redirect stderr, if not done already
    if !elog_config::redirection_done() {
        // Leave a breadcrumb trail when redirecting, in case the user forgets
        // that redirection is active and looks only at the original stderr
        // target file.
        ereport(LOG)
            .errmsg("redirecting log output to logging collector process")
            .errhint(format!(
                "Future log output will appear in directory \"{}\".",
                config::log_directory()
            ))
            .finish(here!())?;

        let pipe_write = config::syslog_pipe()[1];

        // fflush(stdout) — flush Rust-side buffered stdout before dup2.
        let _ = std::io::stdout().flush();
        if unsafe { libc::dup2(pipe_write, libc::STDOUT_FILENO) } < 0 {
            ereport(FATAL)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg("could not redirect stdout: %m")
                .finish(here!())?;
        }
        // fflush(stderr)
        let _ = std::io::stderr().flush();
        if unsafe { libc::dup2(pipe_write, libc::STDERR_FILENO) } < 0 {
            ereport(FATAL)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg("could not redirect stderr: %m")
                .finish(here!())?;
        }
        // Now we are done with the write end of the pipe.
        unsafe {
            libc::close(pipe_write);
        }
        let mut pipe = config::syslog_pipe();
        pipe[1] = -1;
        config::set_syslog_pipe(pipe);

        elog_config::set_redirection_done(true);
    }

    // postmaster will never write the file(s); close 'em
    let f = SYSLOG_FILE.get();
    unsafe {
        libc::fclose(f);
    }
    SYSLOG_FILE.set(ptr::null_mut());
    let f = CSVLOG_FILE.get();
    if !f.is_null() {
        unsafe {
            libc::fclose(f);
        }
        CSVLOG_FILE.set(ptr::null_mut());
    }
    let f = JSONLOG_FILE.get();
    if !f.is_null() {
        unsafe {
            libc::fclose(f);
        }
        JSONLOG_FILE.set(ptr::null_mut());
    }

    Ok(syslogger_pid)
}

// ===========================================================================
// pipe protocol handling
// ===========================================================================

/// `static void process_pipe_input(char *logbuffer, int *bytes_in_logbuffer)`.
///
/// Interprets the log pipe protocol: chunks framed with two NUL bytes, a
/// 16-bit length, the source pid, and an is-last flag are detected and
/// reassembled in per-pid buffers; non-protocol data is written out directly
/// (stderr destination). On exit any not-yet-eaten data is left-justified in
/// `logbuffer` and `*bytes_in_logbuffer` updated.
fn process_pipe_input(logbuffer: &mut [u8], bytes_in_logbuffer: &mut usize) {
    let mut cursor: usize = 0;
    let mut count: usize = *bytes_in_logbuffer;
    let mut dest: i32 = LOG_DESTINATION_STDERR;

    // While we have enough for a header, process data...
    while count >= PIPE_HEADER_SIZE + 1 {
        // Do we have a valid header?  (memcpy of the leading fixed fields;
        // native byte order, as for a struct written by a peer on this host.)
        let buf = &logbuffer[cursor..];
        let nul0 = buf[0];
        let nul1 = buf[1];
        let len = u16::from_ne_bytes([buf[2], buf[3]]) as usize;
        let pid = i32::from_ne_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let flags = buf[8];
        let dest_flags =
            flags & (PIPE_PROTO_DEST_STDERR | PIPE_PROTO_DEST_CSVLOG | PIPE_PROTO_DEST_JSONLOG);

        if nul0 == 0
            && nul1 == 0
            && len > 0
            && len <= PIPE_MAX_PAYLOAD
            && pid != 0
            && dest_flags.count_ones() == 1
        {
            let chunklen = PIPE_HEADER_SIZE + len;

            // Fall out of loop if we don't have the whole chunk yet.
            if count < chunklen {
                break;
            }

            if flags & PIPE_PROTO_DEST_STDERR != 0 {
                dest = LOG_DESTINATION_STDERR;
            } else if flags & PIPE_PROTO_DEST_CSVLOG != 0 {
                dest = LOG_DESTINATION_CSVLOG;
            } else if flags & PIPE_PROTO_DEST_JSONLOG != 0 {
                dest = LOG_DESTINATION_JSONLOG;
            } else {
                // this should never happen as of the header validation
                debug_assert!(false);
            }

            let payload_start = cursor + PIPE_HEADER_SIZE;
            let payload_end = cursor + chunklen;
            // C indexes buffer_lists[pid % NBUFFER_LISTS]; a negative pid
            // would be out-of-bounds UB there and an index panic here.
            let list_idx = (pid % NBUFFER_LISTS as i32) as usize;

            if flags & PIPE_PROTO_IS_LAST == 0 {
                // Save a complete non-final chunk in a per-pid buffer.
                let payload = &logbuffer[payload_start..payload_end];
                with_buffer_lists(|bl| {
                    let list = &mut bl[list_idx];
                    if let Some(existing) = list.iter_mut().find(|b| b.pid == pid) {
                        // Add chunk to data from preceding chunks.
                        existing.data.extend_from_slice(payload);
                    } else if let Some(free) = list.iter_mut().find(|b| b.pid == 0) {
                        // First chunk of message, save in a free buffer.
                        free.pid = pid;
                        free.data = payload.to_vec();
                    } else {
                        // Need a free slot, but there isn't one in the list,
                        // so create a new one and extend the list with it.
                        list.push(SaveBuffer {
                            pid,
                            data: payload.to_vec(),
                        });
                    }
                });
            } else {
                // Final chunk --- add it to anything saved for that pid, and
                // either way write the whole thing out.
                let payload = &logbuffer[payload_start..payload_end];
                let saved: Option<Vec<u8>> = with_buffer_lists(|bl| {
                    let list = &mut bl[list_idx];
                    list.iter_mut().find(|b| b.pid == pid).map(|existing| {
                        let mut data = std::mem::take(&mut existing.data);
                        data.extend_from_slice(payload);
                        // Mark the buffer unused, and reclaim string storage.
                        existing.pid = 0;
                        data
                    })
                });
                match saved {
                    Some(data) => write_syslogger_file(&data, dest),
                    // The whole message was one chunk, evidently.
                    None => write_syslogger_file(payload, dest),
                }
            }

            // Finished processing this chunk.
            cursor += chunklen;
            count -= chunklen;
        } else {
            // Process non-protocol data.
            //
            // Look for the start of a protocol header. If found, dump data up
            // to there and repeat the loop. Otherwise, dump it all and fall
            // out of the loop. (We want to dump it all if at all possible, so
            // as to avoid dividing non-protocol messages across logfiles; in
            // many scenarios a non-protocol message arrives in one read().)
            let mut chunklen: usize = 1;
            while chunklen < count {
                if logbuffer[cursor + chunklen] == 0 {
                    break;
                }
                chunklen += 1;
            }
            // fall back on the stderr log as the destination
            let slice = logbuffer[cursor..cursor + chunklen].to_vec();
            write_syslogger_file(&slice, LOG_DESTINATION_STDERR);
            cursor += chunklen;
            count -= chunklen;
        }
    }

    // We don't have a full chunk, so left-align what remains in the buffer.
    if count > 0 && cursor != 0 {
        logbuffer.copy_within(cursor..cursor + count, 0);
    }
    *bytes_in_logbuffer = count;
}

/// `static void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer)`.
///
/// Force out any buffered data. Currently used only at syslogger shutdown,
/// but careful to leave things in a clean state.
fn flush_pipe_input(logbuffer: &mut [u8], bytes_in_logbuffer: &mut usize) {
    // Dump any incomplete protocol messages.
    let pending: Vec<Vec<u8>> = with_buffer_lists(|bl| {
        let mut out = Vec::new();
        for list in bl.iter_mut() {
            for buf in list.iter_mut() {
                if buf.pid != 0 {
                    out.push(std::mem::take(&mut buf.data));
                    // Mark the buffer unused, and reclaim string storage.
                    buf.pid = 0;
                }
            }
        }
        out
    });
    for data in pending {
        write_syslogger_file(&data, LOG_DESTINATION_STDERR);
    }

    // Force out any remaining pipe data as-is; we don't bother trying to
    // remove any protocol headers that may exist in it.
    if *bytes_in_logbuffer > 0 {
        let slice = logbuffer[..*bytes_in_logbuffer].to_vec();
        write_syslogger_file(&slice, LOG_DESTINATION_STDERR);
    }
    *bytes_in_logbuffer = 0;
}

// ===========================================================================
// logfile routines
// ===========================================================================

/// `void write_syslogger_file(const char *buffer, int count, int destination)`
/// — write text to the currently open logfile (the C `buffer`/`count` pair is
/// the slice).
///
/// Exported (and installed into `backend-postmaster-syslogger-seams`) so that
/// elog.c can call it when `MyBackendType == B_LOGGER`: the syslogger process
/// records elog messages of its own even though its stderr does not point at
/// the syslog pipe.
pub fn write_syslogger_file(buffer: &[u8], destination: i32) {
    // If we're told to write to a structured log file, but it's not open,
    // dump the data to syslogFile (which is always open) instead. This can
    // happen if structured output is enabled after postmaster start and we've
    // been unable to open logFile, and during parameter-change races. Think
    // not to improve this by trying to open logFile on-the-fly: any failure
    // in that would lead to recursion.
    let logfile: *mut libc::FILE =
        if destination & LOG_DESTINATION_CSVLOG != 0 && !CSVLOG_FILE.get().is_null() {
            CSVLOG_FILE.get()
        } else if destination & LOG_DESTINATION_JSONLOG != 0 && !JSONLOG_FILE.get().is_null() {
            JSONLOG_FILE.get()
        } else {
            SYSLOG_FILE.get()
        };

    // C fwrites through syslogFile unconditionally (always open in the
    // running collector — a NULL would segfault). A null here surfaces as the
    // failed-write report below instead.
    let rc = if logfile.is_null() {
        0
    } else {
        unsafe { libc::fwrite(buffer.as_ptr() as *const libc::c_void, 1, buffer.len(), logfile) }
    };

    // Try to report any failure. We mustn't use ereport because it would just
    // recurse right back here, but write_stderr is OK: it writes either to
    // the postmaster's original stderr, or to /dev/null, but never to our
    // input pipe. (%m expanded here; the C write_stderr's printf does it.)
    if rc != buffer.len() {
        backend_utils_error::write_stderr(&errno::replace_percent_m(
            "could not write to log file: %m\n",
            errno::current_errno(),
        ));
    }
}

/// `static FILE *logfile_open(const char *filename, const char *mode,
/// bool allow_errors)` — open a new logfile with proper permissions and
/// buffering options.
///
/// If `allow_errors` is true, we just log any open failure and return NULL
/// (with errno still correct for the fopen failure). Otherwise, errors are
/// FATAL.
fn logfile_open(filename: &str, mode: &str, allow_errors: bool) -> PgResult<*mut libc::FILE> {
    // Note we do not let Log_file_mode disable IWUSR, since we certainly want
    // to be able to write the files ourselves.
    let oumask = unsafe {
        libc::umask(
            !(config::log_file_mode() as libc::mode_t | libc::S_IWUSR)
                & (libc::S_IRWXU | libc::S_IRWXG | libc::S_IRWXO),
        )
    };
    let c_filename = cstring(filename);
    let c_mode = cstring(mode);
    let fh = unsafe { libc::fopen(c_filename.as_ptr(), c_mode.as_ptr()) };
    unsafe {
        libc::umask(oumask);
    }

    if !fh.is_null() {
        // setvbuf(fh, NULL, PG_IOLBF, 0): line buffered
        unsafe {
            libc::setvbuf(fh, ptr::null_mut(), libc::_IOLBF, 0);
        }
    } else {
        let save_errno = errno::current_errno();
        ereport(if allow_errors { LOG } else { FATAL })
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not open log file \"{}\": %m", filename))
            .finish(here!())?;
        set_errno(save_errno);
    }

    Ok(fh)
}

/// `static bool logfile_rotate_dest(...)` — do logfile rotation for a single
/// destination. The destination's `last_file_name`/`logFile` slot is updated
/// on a successful rotation. Returns `false` if the rotation has been
/// stopped, or `true` to move on to the processing of other formats.
fn logfile_rotate_dest(
    time_based_rotation: bool,
    size_rotation_for: i32,
    fntime: pg_time_t,
    target_dest: i32,
    which: Slot,
) -> PgResult<bool> {
    // If the target destination was just turned off, close the previous file
    // and unregister its data. This cannot happen for stderr as syslogFile is
    // assumed to be always opened even if stderr is disabled in
    // log_destination.
    if (elog_config::log_destination() & target_dest) == 0
        && target_dest != LOG_DESTINATION_STDERR
    {
        let file = slot_file(which);
        if !file.is_null() {
            unsafe {
                libc::fclose(file);
            }
        }
        set_slot_file(which, ptr::null_mut());
        set_slot_last_name(which, None);
        return Ok(true);
    }

    // Leave if it is not time for a rotation or if the target destination has
    // no need to do a rotation based on the size of its file.
    if !time_based_rotation && (size_rotation_for & target_dest) == 0 {
        return Ok(true);
    }

    // file extension depends on the destination type
    let log_file_ext: Option<&str> = if target_dest == LOG_DESTINATION_STDERR {
        None
    } else if target_dest == LOG_DESTINATION_CSVLOG {
        Some(".csv")
    } else if target_dest == LOG_DESTINATION_JSONLOG {
        Some(".json")
    } else {
        // cannot happen
        debug_assert!(false);
        None
    };

    // build the new file name
    let filename = logfile_getname(fntime, log_file_ext);

    // Decide whether to overwrite or append. We can overwrite if (a)
    // Log_truncate_on_rotation is set, (b) the rotation was triggered by
    // elapsed time and not something else, and (c) the computed file name is
    // different from what we were previously logging into.
    let last_file_name = slot_last_name(which);
    let fh = if config::log_truncate_on_rotation()
        && time_based_rotation
        && last_file_name.is_some()
        && last_file_name.as_deref() != Some(filename.as_str())
    {
        logfile_open(&filename, "w", true)?
    } else {
        logfile_open(&filename, "a", true)?
    };

    if fh.is_null() {
        // ENFILE/EMFILE are not too surprising on a busy system; just keep
        // using the old file till we manage to get a new one. Otherwise,
        // assume something's wrong with Log_directory and stop trying to
        // create files.
        let e = errno::current_errno();
        if e != libc::ENFILE && e != libc::EMFILE {
            ereport(LOG)
                .errmsg("disabling automatic rotation (use SIGHUP to re-enable)")
                .finish(here!())?;
            ROTATION_DISABLED.set(true);
        }
        return Ok(false);
    }

    // fill in the new information
    let old = slot_file(which);
    if !old.is_null() {
        unsafe {
            libc::fclose(old);
        }
    }
    set_slot_file(which, fh);

    // instead of pfree'ing filename, remember it for next time
    set_slot_last_name(which, Some(filename));

    Ok(true)
}

/// `static void logfile_rotate(bool time_based_rotation, int
/// size_rotation_for)` — perform logfile rotation.
fn logfile_rotate(
    time_based_rotation: bool,
    size_rotation_for: i32,
    mode_mask: u32,
) -> PgResult<()> {
    ROTATION_REQUESTED.set(false);

    // When doing a time-based rotation, invent the new logfile name based on
    // the planned rotation time, not current time, to avoid "slippage" in the
    // file name when we don't do the rotation immediately.
    let fntime: pg_time_t = if time_based_rotation {
        NEXT_ROTATION_TIME.get()
    } else {
        unsafe { libc::time(ptr::null_mut()) }
    };

    // file rotation for stderr
    if !logfile_rotate_dest(
        time_based_rotation,
        size_rotation_for,
        fntime,
        LOG_DESTINATION_STDERR,
        Slot::Stderr,
    )? {
        return Ok(());
    }

    // file rotation for csvlog
    if !logfile_rotate_dest(
        time_based_rotation,
        size_rotation_for,
        fntime,
        LOG_DESTINATION_CSVLOG,
        Slot::Csvlog,
    )? {
        return Ok(());
    }

    // file rotation for jsonlog
    if !logfile_rotate_dest(
        time_based_rotation,
        size_rotation_for,
        fntime,
        LOG_DESTINATION_JSONLOG,
        Slot::Jsonlog,
    )? {
        return Ok(());
    }

    update_metainfo_datafile(mode_mask)?;

    set_next_rotation_time();

    Ok(())
}

/// `static char *logfile_getname(pg_time_t timestamp, const char *suffix)` —
/// construct logfile name using timestamp information.
///
/// If `suffix` isn't None, append it to the name, replacing any ".log" that
/// may be in the pattern. The C `palloc(MAXPGPATH)` buffer's
/// snprintf/strlcpy truncation is mirrored byte-for-byte (reserving the NUL).
fn logfile_getname(timestamp: pg_time_t, suffix: Option<&str>) -> String {
    // snprintf(filename, MAXPGPATH, "%s/", Log_directory);
    let mut filename = String::new();
    filename.push_str(&config::log_directory());
    filename.push('/');
    truncate_to_cstr_capacity(&mut filename, MAXPGPATH);

    // treat Log_filename as a strftime pattern, formatted in log_timezone
    // (this crate's config mirror of the pgtz-owned GUC)
    let tz = config::log_timezone();
    let tm = backend_timezone_pgtz_seams::pg_localtime::call(timestamp, &tz)
        .expect("pg_localtime returned NULL for a valid wall-clock time");
    // pg_strftime(filename + len, MAXPGPATH - len, Log_filename, tm): the
    // seam buffer's length plays the C `maxsize - 1` role (no stored NUL).
    let len = filename.len();
    let mut buf = [0u8; MAXPGPATH];
    let cap = (MAXPGPATH - len).saturating_sub(1);
    let n = backend_timezone_strftime_seams::pg_strftime::call(
        &mut buf[..cap],
        &config::log_filename(),
        &tm,
    );
    filename.push_str(&String::from_utf8_lossy(&buf[..n]));

    if let Some(suffix) = suffix {
        let mut len = filename.len();
        // Byte comparison, like C's strcmp: a String slice (`&filename[len -
        // 4..]`) would panic when `len - 4` splits a multibyte character;
        // since ".log" is ASCII, a byte match guarantees `len - 4` is a char
        // boundary, making the truncate safe.
        if len > 4 && &filename.as_bytes()[len - 4..] == b".log" {
            len -= 4;
            filename.truncate(len);
        }
        // strlcpy(filename + len, suffix, MAXPGPATH - len)
        let mut suffix_owned = suffix.to_string();
        truncate_to_cstr_capacity(&mut suffix_owned, MAXPGPATH - len);
        filename.push_str(&suffix_owned);
    }

    filename
}

/// `static void set_next_rotation_time(void)` — determine the next planned
/// rotation time, and store in `next_rotation_time`.
fn set_next_rotation_time() {
    // nothing to do if time-based rotation is disabled
    if config::log_rotation_age() <= 0 {
        return;
    }

    // The requirements here are to choose the next time > now that is a
    // "multiple" of the log rotation interval. "Multiple" can be interpreted
    // fairly loosely. In this version we align to log_timezone rather than
    // GMT.
    let rotinterval = (config::log_rotation_age() * SECS_PER_MINUTE) as pg_time_t;
    let mut now: pg_time_t = unsafe { libc::time(ptr::null_mut()) };
    let tz = config::log_timezone();
    let tm = backend_timezone_pgtz_seams::pg_localtime::call(now, &tz)
        .expect("pg_localtime returned NULL for a valid wall-clock time");
    now += tm.tm_gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= tm.tm_gmtoff;
    NEXT_ROTATION_TIME.set(now);
}

/// Owned `FILE *`: `fclose` on drop, so an `Err`-propagating `?` can never
/// leak the stream (AGENTS.md "Locks and held resources"). `close()` is the
/// explicit straight-line fclose.
struct FileGuard(*mut libc::FILE);

impl FileGuard {
    /// `fclose` now (the C straight-line `fclose(fh)`).
    fn close(self) {
        drop(self);
    }
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                libc::fclose(self.0);
            }
        }
    }
}

/// `static void update_metainfo_datafile(void)` — store the name of the
/// file(s) the log collector writes to in `current_logfiles`. Filenames are
/// written to a temporary file renamed into the final destination for
/// atomicity; the file gets the data-directory permissions (`mode_mask` =
/// the C `pg_mode_mask` global, an explicit parameter here) and line
/// buffering.
fn update_metainfo_datafile(mode_mask: u32) -> PgResult<()> {
    let dest = elog_config::log_destination();

    if dest & LOG_DESTINATION_STDERR == 0
        && dest & LOG_DESTINATION_CSVLOG == 0
        && dest & LOG_DESTINATION_JSONLOG == 0
    {
        let c_path = cstring(LOG_METAINFO_DATAFILE);
        if unsafe { libc::unlink(c_path.as_ptr()) } < 0 && errno::current_errno() != libc::ENOENT
        {
            ereport(LOG)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not remove file \"{}\": %m",
                    LOG_METAINFO_DATAFILE
                ))
                .finish(here!())?;
        }
        return Ok(());
    }

    // use the same permissions as the data directory for the new file
    let oumask = unsafe { libc::umask(mode_mask as libc::mode_t) };
    let c_tmp = cstring(LOG_METAINFO_DATAFILE_TMP);
    let c_mode = cstring("w");
    let fh = unsafe { libc::fopen(c_tmp.as_ptr(), c_mode.as_ptr()) };
    unsafe {
        libc::umask(oumask);
    }

    if !fh.is_null() {
        unsafe {
            libc::setvbuf(fh, ptr::null_mut(), libc::_IOLBF, 0);
        }
    } else {
        ereport(LOG)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!(
                "could not open file \"{}\": %m",
                LOG_METAINFO_DATAFILE_TMP
            ))
            .finish(here!())?;
        return Ok(());
    }
    let fh = FileGuard(fh);

    // fprintf(fh, "<label> <last_file_name>\n") per enabled destination, in
    // stderr/csvlog/jsonlog order; a short write aborts the rewrite.
    let entries: [(i32, &str, Option<String>); 3] = [
        (
            LOG_DESTINATION_STDERR,
            "stderr",
            LAST_SYS_FILE_NAME.with(|c| c.borrow().clone()),
        ),
        (
            LOG_DESTINATION_CSVLOG,
            "csvlog",
            LAST_CSV_FILE_NAME.with(|c| c.borrow().clone()),
        ),
        (
            LOG_DESTINATION_JSONLOG,
            "jsonlog",
            LAST_JSON_FILE_NAME.with(|c| c.borrow().clone()),
        ),
    ];
    for (bit, label, name) in entries {
        let Some(name) = name else { continue };
        if dest & bit == 0 {
            continue;
        }
        let line = format!("{} {}\n", label, name);
        let written = unsafe {
            libc::fwrite(line.as_ptr() as *const libc::c_void, 1, line.len(), fh.0)
        };
        if written != line.len() {
            // fh closes via the guard on both the Err propagation and the
            // early Ok return.
            ereport(LOG)
                .with_saved_errno(errno::current_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not write file \"{}\": %m",
                    LOG_METAINFO_DATAFILE_TMP
                ))
                .finish(here!())?;
            return Ok(());
        }
    }
    fh.close();

    let c_final = cstring(LOG_METAINFO_DATAFILE);
    if unsafe { libc::rename(c_tmp.as_ptr(), c_final.as_ptr()) } != 0 {
        ereport(LOG)
            .with_saved_errno(errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!(
                "could not rename file \"{}\" to \"{}\": %m",
                LOG_METAINFO_DATAFILE_TMP, LOG_METAINFO_DATAFILE
            ))
            .finish(here!())?;
    }

    Ok(())
}

// ===========================================================================
// signal handler routines
// ===========================================================================

/// `bool CheckLogrotateSignal(void)` — check whether a log rotation request
/// has arrived (the `logrotate` signal file exists). Called by the postmaster
/// after receiving SIGUSR1.
pub fn CheckLogrotateSignal() -> bool {
    let c_path = cstring(LOGROTATE_SIGNAL_FILE);
    let mut stat_buf = std::mem::MaybeUninit::<libc::stat>::uninit();
    unsafe { libc::stat(c_path.as_ptr(), stat_buf.as_mut_ptr()) == 0 }
}

/// `void RemoveLogrotateSignalFiles(void)` — remove the file signaling a log
/// rotation request.
pub fn RemoveLogrotateSignalFiles() {
    let c_path = cstring(LOGROTATE_SIGNAL_FILE);
    unsafe {
        libc::unlink(c_path.as_ptr());
    }
}

/// `static void sigUsr1Handler(SIGNAL_ARGS)` — SIGUSR1: set flag to rotate
/// logfile and wake the main loop.
fn sigUsr1Handler(_postgres_signal_arg: i32) {
    ROTATION_REQUESTED.set(true);
    backend_storage_ipc_latch_seams::set_latch_my_latch::call();
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Truncate `s` so its UTF-8 byte length fits in `cap - 1` bytes (room for
/// the C NUL terminator that `snprintf`/`strlcpy` reserve in a `cap`-byte
/// buffer), never splitting a UTF-8 code point. `cap == 0` empties the
/// string. Mirrors the truncation `snprintf(buf, MAXPGPATH, ...)` /
/// `strlcpy(..., MAXPGPATH - len)` perform when the rendered name would
/// overflow the `MAXPGPATH` buffer.
fn truncate_to_cstr_capacity(s: &mut String, cap: usize) {
    if cap == 0 {
        s.clear();
        return;
    }
    let max_bytes = cap - 1; // reserve one byte for the NUL terminator
    if s.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
}

/// `sys_logger_main` inward-seam adapter (`-> !`): the child-launch machinery
/// (`postmaster_child_launch`) invokes the seam with only `&StartupData`, but
/// the C `SysLoggerMain` also reads the per-process globals `MyStartTime`,
/// `pg_mode_mask`, and `MyLatch`. Those are sourced here at the seam boundary —
/// `MyStartTime`/`data_directory_mode` (the C `pg_mode_mask`) from
/// `backend_utils_init_small::globals`, and `MyLatch` from the latch unit's
/// `my_latch()` (the C `MyLatch` global, set by `InitProcessLocalLatch`; NULL
/// is a C NULL-deref) — and passed into the real body. `SysLoggerMain` is
/// `pg_noreturn` in C and
/// only leaves via `proc_exit` on pipe EOF, so a returned `Ok` is unreachable;
/// a top-level `Err` is an unhandled ERROR promoted to FATAL, re-thrown to the
/// process exit exactly as in C.
fn sys_logger_main_entry(startup_data: &types_startup::StartupData) -> ! {
    // Assert(startup_data_len == 0); — non-EXEC_BACKEND syslogger gets NULL.
    debug_assert!(matches!(startup_data, types_startup::StartupData::None));
    let startup_slice: &[u8] = &[];

    let start_time = backend_utils_init_small::globals::MyStartTime();
    let mode_mask = backend_utils_init_small::globals::data_directory_mode() as u32;
    let my_latch = backend_storage_ipc_latch::my_latch()
        .expect("SysLoggerMain: MyLatch is NULL (InitProcessLocalLatch not run)");

    match SysLoggerMain(startup_slice, start_time, mode_mask, my_latch) {
        Ok(()) => unreachable!("SysLoggerMain returned Ok; it only exits via proc_exit"),
        Err(err) => {
            backend_utils_error::emit_error_report_for(&err);
            backend_storage_ipc_seams::proc_exit::call(
                1,
                backend_utils_init_small_seams::my_proc_pid::call(),
            )
        }
    }
}

/// Install this crate's implementations into its seam crate, plus its GUC
/// storage variables into the GUC tables' slots.
pub fn init_seams() {
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    backend_postmaster_syslogger_seams::write_syslogger_file::set(crate::write_syslogger_file);
    backend_postmaster_syslogger_seams::sys_logger_main::set(sys_logger_main_entry);

    vars::Logging_collector.install(GucVarAccessors {
        get: config::logging_collector,
        set: config::set_logging_collector,
    });
    vars::Log_RotationAge.install(GucVarAccessors {
        get: config::log_rotation_age,
        set: config::set_log_rotation_age,
    });
    vars::Log_RotationSize.install(GucVarAccessors {
        get: config::log_rotation_size,
        set: config::set_log_rotation_size,
    });
    // log_directory / log_filename boot to non-NULL values, and GUC string
    // storage can never go back to NULL afterwards (guc_tables.h).
    vars::Log_directory.install(GucVarAccessors {
        get: || Some(config::log_directory()),
        set: |v| config::set_log_directory(v.unwrap_or_default()),
    });
    vars::Log_filename.install(GucVarAccessors {
        get: || Some(config::log_filename()),
        set: |v| config::set_log_filename(v.unwrap_or_default()),
    });
    vars::Log_truncate_on_rotation.install(GucVarAccessors {
        get: config::log_truncate_on_rotation,
        set: config::set_log_truncate_on_rotation,
    });
    vars::Log_file_mode.install(GucVarAccessors {
        get: config::log_file_mode,
        set: config::set_log_file_mode,
    });
}

#[cfg(test)]
mod tests;
