#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! PostgreSQL WAL archiver (`postmaster/pgarch.c`).
//!
//! The postmaster forks the archiver, which loads an archive module (the
//! built-in shell module or a loadable library), then loops scanning
//! `pg_wal/archive_status` for `.ready` files and copying the highest-priority
//! WAL segments via the module's `archive_file_cb`, renaming each to `.done`
//! when it succeeds.
//!
//! Owned in-crate: the directory-scan priority tracking (a fixed-capacity
//! max-heap, [`ArchFilesState`]), the loaded-module callback table, and every
//! function the archiver runs — `pgarch_MainLoop`, `pgarch_ArchiverCopyLoop`,
//! `pgarch_readyXlog`, `pgarch_archiveXlog`, `pgarch_archiveDone`, the orphan
//! cleanup, the retry counting, `PgArchForceDirScan`, `PgArchWakeup`,
//! `pgarch_die`, `ProcessPgArchInterrupts`, and `LoadArchiveLibrary`.
//!
//! The shared-memory control block ([`types_pgarch::PgArchData`]) holds
//! `pgprocno`/`force_dir_scan` as interior atomics (real shmem-shared state);
//! `PgArchShmemInit` registers it. The `stat`/`unlink`/`rename` status-file
//! syscalls are plain libc, done in-crate via `std::fs`. Every PG-subsystem
//! boundary the state machine reaches (aux-process setup, signal install,
//! latch ops, exit hooks, `proc_exit`, `PostmasterIsAlive`, the interrupt
//! flags + handlers, `set_ps_display`, `pgstat_report_archiver`, `pg_usleep`,
//! the GUC strings, the `archive_status` directory walk, the archive-module
//! loader + callback dispatch, and the error-recovery cleanup suite) goes
//! through that owner's seam crate or a direct dependency.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;
use core::cell::{Cell, RefCell};
use core::sync::atomic::Ordering;
use std::sync::OnceLock;

use backend_utils_error::{ereport, PgResult};
use types_error::{ErrorLocation, ERROR, LOG, WARNING};
use types_core::{init::BackendType, INVALID_PROC_NUMBER};
use types_error::ERRCODE_INVALID_PARAMETER_VALUE;
use types_guc::PGC_SIGHUP;
use types_pgarch::{ArchiveModuleCallbacks, ArchiveModuleState, PgArchData};
use types_pgstat::wait_event::WAIT_EVENT_ARCHIVER_MAIN;
use types_signal::SigHandler;
use types_storage::waiteventset::{WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT};
use types_wal::{MAXFNAMELEN, XLOGDIR};

#[cfg(test)]
mod tests;

// ----------
// Timer definitions.
// ----------

/// How often to force a poll of the archive status directory; in seconds.
const PGARCH_AUTOWAKE_INTERVAL: i64 = 60;
/// How often to attempt to restart a failed archiver; in seconds.
const PGARCH_RESTART_INTERVAL: u32 = 10;

/// Maximum number of retries allowed when attempting to archive a WAL file.
const NUM_ARCHIVE_RETRIES: i32 = 3;

/// Maximum number of retries allowed when attempting to remove an orphan
/// archive status file.
const NUM_ORPHAN_CLEANUP_RETRIES: i32 = 3;

/// Maximum number of .ready files to gather per directory scan.
const NUM_FILES_PER_DIRECTORY_SCAN: usize = 64;

/// `MAXPGPATH` (`pg_config_manual.h`).
const MAXPGPATH: usize = types_core::MAXPGPATH;

// pgarch.h archiver control info.
const MIN_XFN_CHARS: usize = 16;
const MAX_XFN_CHARS: usize = 40;
const VALID_XFN_CHARS: &[u8] = b"0123456789ABCDEF.history.backup.partial";

// ---------------------------------------------------------------------------
// Shared-memory control block (PgArchData). Real shmem-shared, atomic interior.
// ---------------------------------------------------------------------------

/// `static PgArchData *PgArch = NULL;` — the archiver's shared-memory control
/// block, created by `PgArchShmemInit`. Process-global (shared across every
/// backend); the field atomics make a shared `&` sound.
static PG_ARCH: OnceLock<PgArchData> = OnceLock::new();

/// The `PgArch->...` dereference; C would crash on use before
/// `PgArchShmemInit`, here a loud panic.
fn pg_arch() -> &'static PgArchData {
    PG_ARCH
        .get()
        .expect("PgArch shared memory not initialized (PgArchShmemInit not called)")
}

// ---------------------------------------------------------------------------
// Local data (file-scope statics, per-backend — modeled as thread-locals; the
// archiver is a single thread/process).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static time_t last_sigterm_time = 0;`
    static last_sigterm_time: Cell<i64> = const { Cell::new(0) };

    /// `static volatile sig_atomic_t ready_to_stop = false;`
    static ready_to_stop: Cell<bool> = const { Cell::new(false) };

    /// `static struct arch_files_state *arch_files = NULL;` — the per-scan
    /// priority-tracking workspace, allocated in `PgArchiverMain`.
    static arch_files: RefCell<Option<ArchFilesState>> = const { RefCell::new(None) };

    /// `static const ArchiveModuleCallbacks *ArchiveCallbacks;` and
    /// `static ArchiveModuleState *archive_module_state;` — the loaded archive
    /// module's callback table (`'static`, owned by the module) plus the
    /// archiver-allocated per-module state, set up by `LoadArchiveLibrary`.
    static archive_callbacks: Cell<Option<&'static ArchiveModuleCallbacks>> =
        const { Cell::new(None) };
    static archive_module_state: RefCell<Option<ArchiveModuleState>> =
        const { RefCell::new(None) };

    /// `static MemoryContext archive_context = NULL;` — the archiver-private
    /// `AllocSetContextCreate(TopMemoryContext, "archiver", ...)`. Allocated in
    /// `PgArchiverMain`; switched into and reset around each `archive_file_cb`.
    static archive_context: Cell<Option<types_logical::MemoryContextHandle>> =
        const { Cell::new(None) };

    /// `char *arch_module_check_errdetail_string;` (pgarch.c) — the global an
    /// archive module's `check_configured_cb` may set via the
    /// `arch_module_check_errdetail()` macro (archive_module.h). Reset before
    /// each check; consumed when emitting the "not configured" WARNING.
    static arch_module_check_errdetail_string: RefCell<Option<alloc::string::String>> =
        const { RefCell::new(None) };
}

/// Set `arch_module_check_errdetail_string` (the `arch_module_check_errdetail()`
/// macro in `archive/archive_module.h`). Archive modules call this from their
/// `check_configured_cb` to attach an errdetail to the "not configured" WARNING.
pub fn set_arch_module_check_errdetail(detail: alloc::string::String) {
    arch_module_check_errdetail_string.with(|c| *c.borrow_mut() = Some(detail));
}

// ---------------------------------------------------------------------------
// arch_files_state: the per-scan priority tracking workspace.
// ---------------------------------------------------------------------------

/// `struct arch_files_state`.
///
/// `arch_heap` is a max-heap used during the directory scan to track the
/// highest-priority files to archive. After the scan, the file names are stored
/// in ascending order of priority in `arch_files`; `pgarch_readyXlog()` returns
/// files from `arch_files` until it is empty, at which point another scan runs.
///
/// C uses a `binaryheap` of `char *` datums pointing into a fixed
/// `arch_filenames[NUM_FILES_PER_DIRECTORY_SCAN][MAX_XFN_CHARS+1]` buffer plus a
/// `char *arch_files[NUM_FILES_PER_DIRECTORY_SCAN]` array; idiomatically both
/// the heap and the ascending array hold owned `String`s, so the slot buffer is
/// unnecessary.
struct ArchFilesState {
    arch_heap: ArchHeap,
    /// number of live entries in `arch_files`.
    arch_files_size: usize,
    /// files to archive, ascending order of priority (highest at the end,
    /// popped first).
    arch_files: Vec<String>,
}

// ---------------------------------------------------------------------------
// ArchHeap: the bounded max-heap (lib/binaryheap.c, specialized for pgarch).
// ---------------------------------------------------------------------------

/// Fixed-capacity binary max-heap of file names, ordered by
/// [`ready_file_comparator`] (the C `binaryheap` instantiated for the archiver).
///
/// "max-heap": `binaryheap_first` returns the element with the *lowest*
/// archival priority among the stored set, so `pgarch_readyXlog` can evict it
/// when a higher-priority file arrives. The comparator returns negative when
/// `a` has higher priority than `b`; the root is the comparator-maximum (lowest
/// priority).
struct ArchHeap {
    nodes: Vec<String>,
    capacity: usize,
}

impl ArchHeap {
    /// `binaryheap_allocate(capacity, ready_file_comparator, NULL)`.
    fn allocate(capacity: usize) -> Self {
        ArchHeap {
            nodes: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// `bh_size`.
    fn len(&self) -> usize {
        self.nodes.len()
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// `binaryheap_reset(heap)`.
    fn reset(&mut self) {
        self.nodes.clear();
    }

    /// > 0 when `a` should sink below `b` (i.e. `a` has lower archival priority
    /// than `b`).
    #[inline]
    fn compare(a: &str, b: &str) -> i32 {
        ready_file_comparator(a, b)
    }

    /// `binaryheap_add_unordered(heap, d)`.
    fn add_unordered(&mut self, value: String) {
        debug_assert!(self.nodes.len() < self.capacity);
        self.nodes.push(value);
    }

    /// `binaryheap_build(heap)`.
    fn build(&mut self) {
        if self.nodes.len() <= 1 {
            return;
        }
        let mut i = (self.nodes.len() / 2) as isize - 1;
        while i >= 0 {
            self.sift_down(i as usize);
            i -= 1;
        }
    }

    /// `binaryheap_add(heap, d)`.
    fn add(&mut self, value: String) {
        debug_assert!(self.nodes.len() < self.capacity);
        self.nodes.push(value);
        self.sift_up(self.nodes.len() - 1);
    }

    /// `binaryheap_first(heap)`.
    fn first(&self) -> Option<&str> {
        self.nodes.first().map(|s| s.as_str())
    }

    /// `binaryheap_remove_first(heap)`.
    fn remove_first(&mut self) -> Option<String> {
        let n = self.nodes.len();
        if n == 0 {
            return None;
        }
        if n == 1 {
            return self.nodes.pop();
        }
        let last = self.nodes.swap_remove(0);
        self.sift_down(0);
        Some(last)
    }

    fn sift_up(&mut self, mut node_off: usize) {
        while node_off > 0 {
            let parent_off = (node_off - 1) / 2;
            if Self::compare(&self.nodes[parent_off], &self.nodes[node_off]) >= 0 {
                break;
            }
            self.nodes.swap(node_off, parent_off);
            node_off = parent_off;
        }
    }

    fn sift_down(&mut self, mut node_off: usize) {
        let n = self.nodes.len();
        loop {
            let left = 2 * node_off + 1;
            let right = 2 * node_off + 2;
            let mut largest = node_off;
            if left < n && Self::compare(&self.nodes[largest], &self.nodes[left]) < 0 {
                largest = left;
            }
            if right < n && Self::compare(&self.nodes[largest], &self.nodes[right]) < 0 {
                largest = right;
            }
            if largest == node_off {
                break;
            }
            self.nodes.swap(node_off, largest);
            node_off = largest;
        }
    }
}

// ---------------------------------------------------------------------------
// Small predicates / helpers (inlined from xlog_internal.h / string.h).
// ---------------------------------------------------------------------------

/// `IsTLHistoryFileName(fname)` (access/xlog_internal.h): an 8-hex-digit
/// (upper-case) timeline id followed by `.history`.
fn is_tl_history_file_name(fname: &[u8]) -> bool {
    const SUFFIX: &[u8] = b".history";
    fname.len() == 8 + SUFFIX.len()
        && fname[..8]
            .iter()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase())
        && &fname[8..] == SUFFIX
}

/// `strspn(s, accept)`.
fn c_strspn(s: &[u8], accept: &[u8]) -> usize {
    let mut i = 0;
    while i < s.len() && accept.contains(&s[i]) {
        i += 1;
    }
    i
}

/// `strcmp(a, b)` (byte-lexicographic, like C).
fn c_strcmp(a: &[u8], b: &[u8]) -> i32 {
    let n = a.len().min(b.len());
    for i in 0..n {
        if a[i] != b[i] {
            return a[i] as i32 - b[i] as i32;
        }
    }
    a.len() as i32 - b.len() as i32
}

/// `StatusFilePath(path, xlog, suffix)` (access/xlog_internal.h):
/// `snprintf(path, MAXPGPATH, XLOGDIR "/archive_status/%s%s", xlog, suffix)`.
fn status_file_path(xlog: &str, suffix: &str) -> String {
    truncate(
        alloc::format!("{XLOGDIR}/archive_status/{xlog}{suffix}"),
        MAXPGPATH,
    )
}

/// `set_ps_display(activitymsg)` where `activitymsg` is a `char[MAXFNAMELEN + 16]`
/// buffer — truncate accordingly.
fn truncate_activity(s: String) -> String {
    truncate(s, MAXFNAMELEN + 16)
}

/// Emulate `snprintf` truncation into a fixed `n`-byte buffer (n includes the
/// NUL, so at most n-1 content bytes survive).
fn truncate(mut s: String, n: usize) -> String {
    let max = n.saturating_sub(1);
    if s.len() > max {
        while !s.is_char_boundary(max) {
            s.truncate(s.len() - 1);
        }
        s.truncate(max);
    }
    s
}

/// `ErrorLocation` for `.finish()` — the archiver reports without
/// macro-granularity `__FILE__`/`__LINE__`, matching the sibling postmaster
/// crates' non-macro reporting.
fn loc() -> ErrorLocation {
    ErrorLocation {
        filename: None,
        lineno: 0,
        funcname: None,
    }
}

fn with_arch_files<R>(f: impl FnOnce(&mut ArchFilesState) -> R) -> R {
    arch_files.with(|c| {
        let mut borrow = c.borrow_mut();
        let state = borrow.as_mut().expect("arch_files must be allocated");
        f(state)
    })
}

// ===========================================================================
// Public C entry points.
// ===========================================================================

/// `PgArchShmemSize(void)` — report shared memory space needed by
/// `PgArchShmemInit`. `size = add_size(0, sizeof(PgArchData))`.
pub fn PgArchShmemSize() -> usize {
    core::mem::size_of::<PgArchData>()
}

/// `PgArchShmemInit(void)` — allocate and initialize archiver-related shared
/// memory. First caller creates and initializes the block (the C `!found`
/// branch: `MemSet` to zero, `pgprocno = INVALID_PROC_NUMBER`,
/// `force_dir_scan = 0`); later callers just attach.
pub fn PgArchShmemInit() {
    PG_ARCH.get_or_init(|| {
        let data = PgArchData::new();
        // !found branch: pgprocno = INVALID_PROC_NUMBER; force_dir_scan = 0.
        data.pgprocno.store(INVALID_PROC_NUMBER, Ordering::Relaxed);
        data.force_dir_scan.value.store(0, Ordering::Relaxed);
        data
    });
}

/// `PgArchCanRestart(void)`.
///
/// Return true (archiver allowed to restart) if enough time has passed since it
/// was launched last to reach `PGARCH_RESTART_INTERVAL`. Otherwise false.
///
/// Safety valve against continuous respawn if the archiver dies immediately at
/// launch; the postmaster main loop retries, so we get another chance later.
pub fn PgArchCanRestart() -> bool {
    // static time_t last_pgarch_start_time = 0;
    thread_local! {
        static last_pgarch_start_time: Cell<i64> = const { Cell::new(0) };
    }

    let curtime = now_seconds();

    if (curtime.wrapping_sub(last_pgarch_start_time.with(|c| c.get())) as u32)
        < PGARCH_RESTART_INTERVAL
    {
        return false;
    }

    last_pgarch_start_time.with(|c| c.set(curtime));
    true
}

/// `PgArchiverMain(startup_data, startup_data_len)` — main entry point for the
/// archiver process. `pg_noreturn` in C (ends with `proc_exit(0)`); the inward
/// `pg_archiver_main` seam returns `!`.
pub fn PgArchiverMain(startup_data: &types_startup::StartupData) -> ! {
    match pg_archiver_main_inner(startup_data) {
        Ok(()) => unreachable!("PgArchiverMain falls off the end only via proc_exit(0)"),
        // An ERROR escaping the archiver's main with no handler is promoted to
        // FATAL, exactly as in C; proc_exit carries it out of the process.
        Err(err) => {
            backend_utils_error::emit_error_report_for(&err);
            backend_storage_ipc_seams::proc_exit::call(
                1,
                backend_utils_init_small_seams::my_proc_pid::call(),
            )
        }
    }
}

fn pg_archiver_main_inner(startup_data: &types_startup::StartupData) -> PgResult<()> {
    // Assert(startup_data_len == 0);
    debug_assert!(matches!(startup_data, types_startup::StartupData::None));

    // MyBackendType = B_ARCHIVER;
    backend_utils_init_small_seams::set_my_backend_type::call(BackendType::Archiver);
    // AuxiliaryProcessMainCommon();
    backend_postmaster_auxprocess_seams::auxiliary_process_main_common::call()?;

    // Ignore all signals usually bound to some action in the postmaster, except
    // for SIGHUP, SIGTERM, SIGUSR1, SIGUSR2, and SIGQUIT.
    port_pqsignal_seams::pqsignal::call(
        libc::SIGHUP,
        SigHandler::Handler(signal_handler_for_config_reload),
    );
    port_pqsignal_seams::pqsignal::call(libc::SIGINT, SigHandler::Ignore);
    port_pqsignal_seams::pqsignal::call(
        libc::SIGTERM,
        SigHandler::Handler(signal_handler_for_shutdown_request),
    );
    // SIGQUIT handler was already set up by InitPostmasterChild.
    port_pqsignal_seams::pqsignal::call(libc::SIGALRM, SigHandler::Ignore);
    port_pqsignal_seams::pqsignal::call(libc::SIGPIPE, SigHandler::Ignore);
    port_pqsignal_seams::pqsignal::call(
        libc::SIGUSR1,
        SigHandler::Handler(backend_storage_ipc_procsignal_seams::procsignal_sigusr1_handler::call),
    );
    port_pqsignal_seams::pqsignal::call(libc::SIGUSR2, SigHandler::Handler(pgarch_waken_stop));

    // Reset some signals that are accepted by postmaster but not here.
    port_pqsignal_seams::pqsignal::call(libc::SIGCHLD, SigHandler::Default);

    // Unblock signals (they were blocked when the postmaster forked us).
    // C: sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);
    let masks = backend_libpq_pqsignal::signal_masks();
    // SAFETY: setting this thread's signal mask from an initialized sigset_t.
    unsafe {
        libc::sigprocmask(libc::SIG_SETMASK, masks.unblock_sig(), std::ptr::null_mut());
    }

    // We shouldn't be launched unnecessarily.
    // Assert(XLogArchivingActive()); — debug-only GUC assertion owned by xlog.c.

    // Arrange to clean up at archiver exit: on_shmem_exit(pgarch_die, 0).
    backend_storage_ipc_seams::on_shmem_exit::call(pgarch_die, types_datum::Datum::from_i32(0))?;

    // Advertise our proc number so backends can use our latch to wake us:
    // PgArch->pgprocno = MyProcNumber;
    pg_arch().pgprocno.store(
        backend_utils_init_small_seams::my_proc_number::call(),
        Ordering::Relaxed,
    );

    // Create workspace for pgarch_readyXlog(); initialize the max-heap.
    let state = ArchFilesState {
        arch_heap: ArchHeap::allocate(NUM_FILES_PER_DIRECTORY_SCAN),
        arch_files_size: 0,
        arch_files: Vec::with_capacity(NUM_FILES_PER_DIRECTORY_SCAN),
    };
    arch_files.with(|c| *c.borrow_mut() = Some(state));

    // Create a memory context to use for the WAL archiver:
    // archive_context = AllocSetContextCreate(TopMemoryContext, "archiver",
    //                                         ALLOCSET_DEFAULT_SIZES);
    let ctx = backend_utils_mmgr_mcxt_seams::create_archiver_memcxt::call();
    archive_context.with(|c| c.set(Some(ctx)));

    // Load the archive_library.
    LoadArchiveLibrary()?;

    pgarch_MainLoop()?;

    backend_storage_ipc_seams::proc_exit::call(0, backend_utils_init_small_seams::my_proc_pid::call())
}

/// `PgArchWakeup(void)` — wake up the archiver.
pub fn PgArchWakeup() {
    let arch_pgprocno = pg_arch().pgprocno.load(Ordering::Relaxed);

    // We don't acquire ProcArrayLock here. It's fine because procLatch isn't
    // ever freed, so at worst we set the wrong (or no) process' latch; the
    // archiver will be relaunched shortly and start archiving.
    if arch_pgprocno != INVALID_PROC_NUMBER {
        backend_storage_lmgr_proc_seams::set_proc_latch::call(arch_pgprocno);
    }
}

/// `pgarch_waken_stop(SIGNAL_ARGS)` — SIGUSR2 signal handler.
fn pgarch_waken_stop(_postgres_signal_arg: i32) {
    // set flag to do a final cycle and shut down afterwards
    ready_to_stop.with(|c| c.set(true));
    backend_storage_ipc_latch_seams::set_latch_my_latch::call();
}

/// SIGHUP handler adapter: `SignalHandlerForConfigReload(SIGNAL_ARGS)`.
fn signal_handler_for_config_reload(_postgres_signal_arg: i32) {
    backend_postmaster_interrupt::SignalHandlerForConfigReload();
}

/// SIGTERM handler adapter: `SignalHandlerForShutdownRequest(SIGNAL_ARGS)`.
fn signal_handler_for_shutdown_request(_postgres_signal_arg: i32) {
    backend_postmaster_interrupt::SignalHandlerForShutdownRequest();
}

/// `pgarch_MainLoop(void)` — main loop for archiver.
fn pgarch_MainLoop() -> PgResult<()> {
    let mut time_to_stop: bool;

    // There shouldn't be anything for the archiver to do except wait for a
    // signal ... however, the archiver exists to protect our data, so it wakes
    // up occasionally to be proactive.
    loop {
        backend_storage_ipc_latch_seams::reset_latch_my_latch::call();

        // When we get SIGUSR2, do one more archive cycle, then exit.
        time_to_stop = ready_to_stop.with(|c| c.get());

        // Check for barrier events and config update.
        ProcessPgArchInterrupts()?;

        // If we've gotten SIGTERM, normally sit and do nothing until SIGUSR2
        // arrives. But a random SIGTERM would disable archiving indefinitely,
        // so if more than 60 seconds pass since SIGTERM, exit anyway so the
        // postmaster can start a new archiver if needed.
        if backend_postmaster_interrupt::ShutdownRequestPending() {
            let curtime = now_seconds();

            if last_sigterm_time.with(|c| c.get()) == 0 {
                last_sigterm_time.with(|c| c.set(curtime));
            } else if (curtime.wrapping_sub(last_sigterm_time.with(|c| c.get())) as u32) >= 60u32 {
                break;
            }
        }

        // Do what we're here for.
        pgarch_ArchiverCopyLoop()?;

        // Sleep until a signal is received, or a poll is forced by
        // PGARCH_AUTOWAKE_INTERVAL, or the postmaster dies.
        if !time_to_stop {
            // Don't wait during last iteration.
            let rc = backend_storage_ipc_latch_seams::wait_latch_my_latch::call(
                WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                PGARCH_AUTOWAKE_INTERVAL * 1000,
                WAIT_EVENT_ARCHIVER_MAIN,
            )?;
            if rc & WL_POSTMASTER_DEATH != 0 {
                time_to_stop = true;
            }
        }

        // The archiver quits when the postmaster dies (not expected) or after
        // completing one more cycle following SIGUSR2.
        if time_to_stop {
            break;
        }
    }
    Ok(())
}

/// `pgarch_ArchiverCopyLoop(void)` — archives all outstanding xlogs then returns.
fn pgarch_ArchiverCopyLoop() -> PgResult<()> {
    // force directory scan in the first call to pgarch_readyXlog()
    with_arch_files(|af| af.arch_files_size = 0);

    // loop through all xlogs with archive_status of .ready and archive them...
    // mostly a single file, though a backend may add files while we copy
    // earlier archives.
    while let Some(xlog) = pgarch_readyXlog()? {
        let mut failures: i32 = 0;
        let mut failures_orphan: i32 = 0;

        loop {
            // Do not initiate any more archive commands after SIGTERM, nor after
            // the postmaster has died unexpectedly. First condition keeps init
            // from SIGKILLing the command; second avoids conflicts with another
            // archiver spawned by a newer postmaster.
            if backend_postmaster_interrupt::ShutdownRequestPending()
                || !backend_storage_ipc_pmsignal_seams::postmaster_is_alive::call()
            {
                return Ok(());
            }

            // Check for barrier events and config update so we adopt a new
            // archive_command as soon as possible even with a backlog.
            ProcessPgArchInterrupts()?;

            // Reset variables that might be set by the callback.
            // arch_module_check_errdetail_string = NULL;
            arch_module_check_errdetail_string.with(|c| *c.borrow_mut() = None);

            // can't do anything if not configured ...
            // if (ArchiveCallbacks->check_configured_cb != NULL &&
            //     !ArchiveCallbacks->check_configured_cb(archive_module_state))
            let check_cb = archive_callbacks
                .with(|c| c.get())
                .expect("archive module not loaded")
                .check_configured_cb;
            if let Some(check_configured_cb) = check_cb {
                if !with_module_state(check_configured_cb) {
                    // ereport(WARNING,
                    //   (errmsg("\"archive_mode\" enabled, yet archiving is not configured"),
                    //    arch_module_check_errdetail_string ?
                    //    errdetail_internal("%s", arch_module_check_errdetail_string) : 0));
                    let mut report = ereport(WARNING).errmsg(
                        "\"archive_mode\" enabled, yet archiving is not configured",
                    );
                    if let Some(detail) =
                        arch_module_check_errdetail_string.with(|c| c.borrow().clone())
                    {
                        report = report.errdetail_internal(detail);
                    }
                    report.finish(loc())?;

                    return Ok(());
                }
            }

            // snprintf(pathname, MAXPGPATH, XLOGDIR "/%s", xlog);
            let pathname = truncate(alloc::format!("{XLOGDIR}/{xlog}"), MAXPGPATH);

            // Since archive status files are not removed durably, a crash could
            // leave behind .ready files for already-recycled WAL segments. In
            // that case, remove the orphan status file and move on. unlink() is
            // used as even on subsequent crashes the same orphan files would get
            // removed, so durability isn't a concern.
            // stat(pathname, &stat_buf) != 0 && errno == ENOENT
            if !path_exists(&pathname)? {
                // StatusFilePath(xlogready, xlog, ".ready");
                let xlogready = status_file_path(&xlog, ".ready");
                if unlink_file(&xlogready) {
                    ereport(WARNING)
                        .errmsg(alloc::format!(
                            "removed orphan archive status file \"{xlogready}\""
                        ))
                        .finish(loc())?;

                    // leave loop and move to the next status file
                    break;
                }

                failures_orphan += 1;
                if failures_orphan >= NUM_ORPHAN_CLEANUP_RETRIES {
                    ereport(WARNING)
                        .errmsg(alloc::format!(
                            "removal of orphan archive status file \"{xlogready}\" failed too many times, will try again later"
                        ))
                        .finish(loc())?;

                    // give up cleanup of orphan status files
                    return Ok(());
                }

                // wait a bit before retrying
                port_pgsleep_seams::pg_usleep::call(1_000_000);
                continue;
            }

            if pgarch_archiveXlog(&xlog)? {
                // successful
                pgarch_archiveDone(&xlog)?;

                // Tell the cumulative stats system about the successfully
                // archived WAL file.
                report_archiver(&xlog, false);

                break; // out of inner retry loop
            } else {
                // Tell the cumulative stats system about the failed WAL file.
                report_archiver(&xlog, true);

                failures += 1;
                if failures >= NUM_ARCHIVE_RETRIES {
                    ereport(WARNING)
                        .errmsg(alloc::format!(
                            "archiving write-ahead log file \"{xlog}\" failed too many times, will try again later"
                        ))
                        .finish(loc())?;
                    return Ok(()); // give up archiving for now
                }
                port_pgsleep_seams::pg_usleep::call(1_000_000); // wait a bit before retrying
            }
        }
    }
    Ok(())
}

/// `pgstat_report_archiver(xlog, failed)` — copies exactly `WAL_NAME_LEN` bytes
/// from the (NUL-terminated) WAL name; build that fixed buffer here.
fn report_archiver(xlog: &str, failed: bool) {
    let mut buf = [0u8; types_pgstat::activity_pgstat::WAL_NAME_LEN];
    let bytes = xlog.as_bytes();
    let n = bytes.len().min(buf.len());
    buf[..n].copy_from_slice(&bytes[..n]);
    backend_utils_activity_small::pgstat_archiver::pgstat_report_archiver(&buf, failed);
}

/// `pgarch_archiveXlog(xlog)` — invoke `archive_file_cb` to copy one archive
/// file to its destination. Returns true if successful.
fn pgarch_archiveXlog(xlog: &str) -> PgResult<bool> {
    // snprintf(pathname, MAXPGPATH, XLOGDIR "/%s", xlog);
    let pathname = truncate(alloc::format!("{XLOGDIR}/{xlog}"), MAXPGPATH);

    // Report archive activity in PS display.
    let activitymsg = truncate_activity(alloc::format!("archiving {xlog}"));
    backend_utils_misc_ps_status_seams::set_ps_display::call(&activitymsg);

    // oldcontext = MemoryContextSwitchTo(archive_context);
    let ctx = archive_context
        .with(|c| c.get())
        .expect("archive_context created in PgArchiverMain");
    let oldcontext = backend_utils_mmgr_mcxt_seams::MemoryContextSwitchTo::call(ctx);

    // The archiver operates at the bottom of the exception stack, so an ERROR
    // would normally turn into FATAL and restart the process. To avoid that,
    // pgarch.c installs its own exception handler catching ERRORs and returning
    // false. Idiomatically the archive_file_cb returns Result; an `Err` IS that
    // caught ERROR. On Err we perform the same cleanup the sigsetjmp block runs,
    // then return false.
    let cb = archive_callbacks
        .with(|c| c.get())
        .expect("archive module not loaded")
        .archive_file_cb
        .expect("archive_file_cb is required (checked in LoadArchiveLibrary)");

    let result = with_module_state(|state| cb(state, xlog, &pathname));

    let ret = match result {
        Ok(r) => {
            // Remove our exception handler / reset our memory context and switch
            // back to the original one:
            // MemoryContextSwitchTo(oldcontext);
            // MemoryContextReset(archive_context);
            backend_utils_mmgr_mcxt_seams::MemoryContextSwitchTo::call(oldcontext);
            backend_utils_mmgr_mcxt_seams::MemoryContextReset::call(ctx);
            r
        }
        Err(_err) => {
            archive_error_cleanup(oldcontext, ctx);
            // Report failure so that the archiver retries this file.
            false
        }
    };

    let activitymsg = if ret {
        truncate_activity(alloc::format!("last was {xlog}"))
    } else {
        truncate_activity(alloc::format!("failed on {xlog}"))
    };
    backend_utils_misc_ps_status_seams::set_ps_display::call(&activitymsg);

    Ok(ret)
}

/// The `pgarch_archiveXlog` sigsetjmp error-recovery block: reset the error
/// stack, hold interrupts, emit the error, run the module-leftover cleanup
/// suite, flush the error state, reset the archive context, resume interrupts.
fn archive_error_cleanup(
    oldcontext: types_logical::MemoryContextHandle,
    archive_ctx: types_logical::MemoryContextHandle,
) {
    // error_context_stack = NULL; / HOLD_INTERRUPTS().
    backend_utils_init_small_seams::hold_interrupts::call();

    // Report the error to the server log. (The error value is the `Err` from the
    // callback; the elog stack may have no frame, so emit it directly.)
    let _ = backend_utils_error::EmitErrorReport();

    // Try to clean up anything the archive module left behind.
    let _ = backend_utils_misc_timeout_seams::disable_all_timeouts::call(false);
    backend_storage_lmgr_lwlock_seams::lwlock_release_all::call();
    backend_storage_lmgr_condition_variable_seams::condition_variable_cancel_sleep::call();
    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();
    backend_storage_aio_core_seams::pgaio_error_cleanup::call();
    backend_utils_resowner_all_seams::release_aux_process_resources::call(false);
    backend_storage_file_fd_seams::at_eoxact_files::call(false);
    backend_utils_hash_dynahash_seams::at_eoxact_hash_tables::call(false);

    // Return to the original memory context and clear ErrorContext for next
    // time: MemoryContextSwitchTo(oldcontext); FlushErrorState();
    backend_utils_mmgr_mcxt_seams::MemoryContextSwitchTo::call(oldcontext);
    backend_utils_error::FlushErrorState();

    // Flush any leaked data: MemoryContextReset(archive_context).
    backend_utils_mmgr_mcxt_seams::MemoryContextReset::call(archive_ctx);

    // PG_exception_stack = NULL; / RESUME_INTERRUPTS().
    backend_utils_init_small_seams::resume_interrupts::call();
}

/// `pgarch_readyXlog(xlog)` — return the name of the oldest xlog not yet
/// archived. `Some(name)` when found, `None` otherwise.
///
/// Returning the oldest archives xlogs in write order: (1) to maintain the
/// sequential chain required for recovery, (2) because the oldest become
/// recycling candidates first at checkpoint. `.history` files rank older than
/// any non-history file; smaller-ID timelines rank older than larger-ID ones.
fn pgarch_readyXlog() -> PgResult<Option<String>> {
    // If a directory scan was requested, clear stored file names and proceed:
    // pg_atomic_exchange_u32(&PgArch->force_dir_scan, 0) == 1
    let forced = pg_arch().force_dir_scan.value.swap(0, Ordering::SeqCst);
    if forced == 1 {
        with_arch_files(|af| {
            af.arch_files_size = 0;
            af.arch_files.clear();
        });
    }

    // If we still have stored file names from the previous scan, try to return
    // one. Make sure the status file is still present, as a previous file's
    // archive_command may have already marked it done.
    loop {
        let arch_file = with_arch_files(|af| {
            if af.arch_files_size == 0 {
                None
            } else {
                af.arch_files_size -= 1;
                Some(af.arch_files[af.arch_files_size].clone())
            }
        });
        let Some(arch_file) = arch_file else { break };

        // StatusFilePath(status_file, arch_file, ".ready");
        let status_file = status_file_path(&arch_file, ".ready");

        // stat(status_file, &st) == 0 -> present; ENOENT -> skip; other -> ERROR.
        match stat_status_file(&status_file) {
            Ok(true) => return Ok(Some(arch_file)),
            Ok(false) => {
                // errno == ENOENT: already marked done; continue scanning.
            }
            Err(message) => {
                // errno != ENOENT: real stat failure.
                return Err(ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg_internal(message)
                    .into_error());
            }
        }
    }

    // arch_heap is probably empty, but let's make sure.
    with_arch_files(|af| af.arch_heap.reset());

    // Open the archive status directory and read through .ready files, looking
    // for the earliest ones.
    // snprintf(XLogArchiveStatusDir, MAXPGPATH, XLOGDIR "/archive_status");
    let xlog_archive_status_dir = truncate(alloc::format!("{XLOGDIR}/archive_status"), MAXPGPATH);

    // AllocateDir / ReadDir / FreeDir as one owned walk; the heap is built
    // inside the per-entry callback, exactly as the C while-loop body.
    backend_storage_file_seams::with_allocated_dir::call(&xlog_archive_status_dir, &mut |d_name| {
        let name = d_name.as_bytes();
        // int basenamelen = (int) strlen(rlde->d_name) - 6;
        let basenamelen_i = name.len() as isize - 6;

        // Ignore entries with unexpected number of characters.
        if basenamelen_i < MIN_XFN_CHARS as isize || basenamelen_i > MAX_XFN_CHARS as isize {
            return Ok(());
        }
        let basenamelen = basenamelen_i as usize;

        // Ignore entries with unexpected characters.
        if c_strspn(name, VALID_XFN_CHARS) < basenamelen {
            return Ok(());
        }

        // Ignore anything not suffixed with .ready.
        if &name[basenamelen..] != b".ready" {
            return Ok(());
        }

        // Truncate off the .ready: basename = d_name[..basenamelen].
        let basename = &d_name[..basenamelen];

        // Store the file in our max-heap if it has a high enough priority.
        with_arch_files(|af| {
            let bh_size = af.arch_heap.len();
            if bh_size < NUM_FILES_PER_DIRECTORY_SCAN {
                // Heap not full: quickly add it.
                af.arch_heap.add_unordered(String::from(basename));

                // If we just filled the heap, make it valid.
                if af.arch_heap.len() == NUM_FILES_PER_DIRECTORY_SCAN {
                    af.arch_heap.build();
                }
            } else if af
                .arch_heap
                .first()
                .map(|first| ArchHeap::compare(first, basename) > 0)
                .unwrap_or(false)
            {
                // ready_file_comparator(binaryheap_first(heap), basename) > 0:
                // remove the lowest-priority file and add the current one.
                af.arch_heap.remove_first();
                af.arch_heap.add(String::from(basename));
            }
        });
        Ok(())
    })?;

    // If no files were found, simply return.
    if with_arch_files(|af| af.arch_heap.is_empty()) {
        return Ok(None);
    }

    // If we didn't fill the heap, we didn't make it valid. Do that now.
    if with_arch_files(|af| af.arch_heap.len() < NUM_FILES_PER_DIRECTORY_SCAN) {
        with_arch_files(|af| af.arch_heap.build());
    }

    // Fill arch_files in ascending order of priority.
    with_arch_files(|af| {
        af.arch_files_size = af.arch_heap.len();
        af.arch_files.clear();
        for _ in 0..af.arch_files_size {
            let f = af.arch_heap.remove_first().expect("non-empty heap");
            af.arch_files.push(f);
        }
    });

    // Return the highest priority file.
    let xlog = with_arch_files(|af| {
        af.arch_files_size -= 1;
        af.arch_files[af.arch_files_size].clone()
    });

    Ok(Some(xlog))
}

/// `ready_file_comparator(a, b, arg)`.
///
/// Compares archival priority. If "a" has higher priority than "b", returns
/// negative; if "b" is higher, positive; equivalent values return 0.
fn ready_file_comparator(a: &str, b: &str) -> i32 {
    let a_str = a.as_bytes();
    let b_str = b.as_bytes();
    let a_history = is_tl_history_file_name(a_str);
    let b_history = is_tl_history_file_name(b_str);

    // Timeline history files always have the highest priority.
    if a_history != b_history {
        return if a_history { -1 } else { 1 };
    }

    // Priority is given to older files: strcmp(a_str, b_str).
    c_strcmp(a_str, b_str)
}

/// `PgArchForceDirScan(void)` — make the next `pgarch_readyXlog()` perform a
/// directory scan (so important files such as timeline history files are
/// archived ASAP).
pub fn PgArchForceDirScan() {
    // pg_atomic_write_membarrier_u32(&PgArch->force_dir_scan, 1);
    pg_arch().force_dir_scan.value.store(1, Ordering::SeqCst);
}

/// `pgarch_archiveDone(xlog)` — emit notification that an xlog has been
/// successfully archived, by renaming the status file from NNN.ready to
/// NNN.done. Eventually a checkpoint deletes both the NNN.done file and the
/// xlog itself.
fn pgarch_archiveDone(xlog: &str) -> PgResult<()> {
    let rlogready = status_file_path(xlog, ".ready");
    let rlogdone = status_file_path(xlog, ".done");

    // To avoid extra overhead, we don't durably rename .ready to .done. Archive
    // commands/libraries must gracefully handle re-archiving (e.g. if the server
    // crashes just before this), so a reappearing .ready after a crash is okay.
    if let Err(message) = rename_file(&rlogready, &rlogdone) {
        ereport(WARNING)
            .errcode_for_file_access()
            .errmsg_internal(alloc::format!(
                "could not rename file \"{rlogready}\" to \"{rlogdone}\": {message}"
            ))
            .finish(loc())?;
    }
    Ok(())
}

/// `pgarch_die(code, arg)` — exit-time cleanup handler.
fn pgarch_die(_code: i32, _arg: types_datum::Datum) -> PgResult<()> {
    pg_arch().pgprocno.store(INVALID_PROC_NUMBER, Ordering::Relaxed);
    Ok(())
}

/// `ProcessPgArchInterrupts(void)` — interrupt handler for the WAL archiver,
/// called in both `pgarch_MainLoop` and `pgarch_ArchiverCopyLoop`. Checks for
/// barrier events, config update, and memory-context logging — but not shutdown
/// request (handled differently between those loops).
fn ProcessPgArchInterrupts() -> PgResult<()> {
    if backend_storage_ipc_procsignal_seams::proc_signal_barrier_pending::call() {
        backend_storage_ipc_procsignal_seams::process_proc_signal_barrier::call()?;
    }

    // Perform logging of memory contexts of this process.
    if backend_utils_mmgr_mcxt_seams::log_memory_context_pending::call() {
        backend_utils_mmgr_mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    if backend_postmaster_interrupt::ConfigReloadPending() {
        // char *archiveLib = pstrdup(XLogArchiveLibrary);
        let archive_lib = backend_access_transam_xlog_seams::xlog_archive_library::call();

        backend_postmaster_interrupt::SetConfigReloadPending(false);
        backend_utils_misc_guc_file_seams::process_config_file::call(PGC_SIGHUP)?;

        if !backend_access_transam_xlog_seams::xlog_archive_library::call().is_empty()
            && !backend_access_transam_xlog_seams::xlog_archive_command::call().is_empty()
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("both \"archive_command\" and \"archive_library\" set")
                .errdetail("Only one of \"archive_command\", \"archive_library\" may be set.")
                .into_error());
        }

        let archive_lib_changed =
            backend_access_transam_xlog_seams::xlog_archive_library::call() != archive_lib;
        // pfree(archiveLib) — `archive_lib` dropped at scope end.

        if archive_lib_changed {
            // Ideally we'd unload the previous archive module and load the new
            // one, but there's no mechanism to unload a library. So we restart
            // the archiver; the new module loads at the new archiver's startup.
            // This triggers the module's shutdown callback, if defined.
            ereport(LOG)
                .errmsg(
                    "restarting archiver process because value of \"archive_library\" was changed",
                )
                .finish(loc())?;

            backend_storage_ipc_seams::proc_exit::call(
                0,
                backend_utils_init_small_seams::my_proc_pid::call(),
            );
        }
    }
    Ok(())
}

/// `LoadArchiveLibrary(void)` — load the archiving callbacks into our local
/// `ArchiveCallbacks`.
fn LoadArchiveLibrary() -> PgResult<()> {
    if !backend_access_transam_xlog_seams::xlog_archive_library::call().is_empty()
        && !backend_access_transam_xlog_seams::xlog_archive_command::call().is_empty()
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("both \"archive_command\" and \"archive_library\" set")
            .errdetail("Only one of \"archive_command\", \"archive_library\" may be set.")
            .into_error());
    }

    // If shell archiving is enabled, use shell_archive_init(); otherwise load
    // the library and call its _PG_archive_module_init().
    let archive_lib = backend_access_transam_xlog_seams::xlog_archive_library::call();
    let archive_init: types_pgarch::ArchiveModuleInit = if archive_lib.is_empty() {
        backend_archive_shell_archive_seams::shell_archive_init::call
    } else {
        match backend_utils_fmgr_dfmgr_seams::load_archive_module_init::call(&archive_lib)? {
            Some(init) => init,
            // archive_init == NULL
            None => {
                return Err(ereport(ERROR)
                    .errmsg(alloc::format!(
                        "archive modules have to define the symbol {}",
                        "_PG_archive_module_init"
                    ))
                    .into_error());
            }
        }
    };

    // ArchiveCallbacks = (*archive_init)();
    let callbacks: &'static ArchiveModuleCallbacks = archive_init();
    archive_callbacks.with(|c| c.set(Some(callbacks)));

    if callbacks.archive_file_cb.is_none() {
        return Err(ereport(ERROR)
            .errmsg("archive modules must register an archive callback")
            .into_error());
    }

    // archive_module_state = palloc0(sizeof(ArchiveModuleState));
    archive_module_state.with(|c| *c.borrow_mut() = Some(ArchiveModuleState::new()));

    // if (startup_cb != NULL) startup_cb(archive_module_state);
    if let Some(startup_cb) = callbacks.startup_cb {
        with_module_state(|state| startup_cb(state));
    }

    // before_shmem_exit(pgarch_call_module_shutdown_cb, 0).
    backend_storage_ipc_seams::before_shmem_exit::call(
        pgarch_call_module_shutdown_cb,
        types_datum::Datum::from_i32(0),
    )?;
    Ok(())
}

/// `pgarch_call_module_shutdown_cb(code, arg)` — call the shutdown callback of
/// the loaded archive module, if defined. Registered with `before_shmem_exit`.
fn pgarch_call_module_shutdown_cb(_code: i32, _arg: types_datum::Datum) -> PgResult<()> {
    if let Some(callbacks) = archive_callbacks.with(|c| c.get()) {
        if let Some(shutdown_cb) = callbacks.shutdown_cb {
            with_module_state(|state| shutdown_cb(state));
        }
    }
    Ok(())
}

/// Run `f` with mutable access to the archiver's `ArchiveModuleState`.
fn with_module_state<R>(f: impl FnOnce(&mut ArchiveModuleState) -> R) -> R {
    archive_module_state.with(|c| {
        let mut borrow = c.borrow_mut();
        let state = borrow.as_mut().expect("archive_module_state allocated");
        f(state)
    })
}

// ---------------------------------------------------------------------------
// In-crate filesystem helpers (direct libc syscalls in C: stat / unlink /
// rename). These are not PG-subsystem boundaries, so the archiver owns them.
// ---------------------------------------------------------------------------

/// `stat(pathname, &stat_buf) != 0 && errno == ENOENT` inverted: returns
/// `Ok(true)` if the file exists, `Ok(false)` on ENOENT, `Err` on any other
/// stat failure. Used at `pgarch_ArchiverCopyLoop`'s orphan check (C only
/// distinguishes ENOENT vs. exists; other errno values fall through to the
/// archive attempt — see below).
fn path_exists(path: &str) -> PgResult<bool> {
    // C: `if (stat(pathname, &stat_buf) != 0 && errno == ENOENT)` treats the
    // file as missing only on ENOENT; any other stat outcome (success, or a
    // different errno) takes the "exists / try to archive" branch. Mirror that:
    // missing iff ENOENT.
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        // Any other errno: C does NOT take the orphan branch, so report "exists"
        // and let the archive attempt proceed (which surfaces the real error).
        Err(_) => Ok(true),
    }
}

/// `stat(status_file, &st)` in `pgarch_readyXlog`: `Ok(true)` present,
/// `Ok(false)` on ENOENT, `Err(message)` (errno != ENOENT) → the C
/// `ereport(ERROR, errcode_for_file_access(), "could not stat file ...")`.
fn stat_status_file(path: &str) -> Result<bool, String> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(alloc::format!("could not stat file \"{path}\": {e}")),
    }
}

/// `unlink(path) == 0` — returns true on success.
fn unlink_file(path: &str) -> bool {
    std::fs::remove_file(path).is_ok()
}

/// `rename(from, to) < 0` — `Err(message)` on failure.
fn rename_file(from: &str, to: &str) -> Result<(), String> {
    std::fs::rename(from, to).map_err(|e| alloc::format!("{e}"))
}

// ---------------------------------------------------------------------------
// time(NULL) — wall-clock seconds, for the SIGTERM/restart back-off timers.
// ---------------------------------------------------------------------------

/// `time(NULL)` in seconds since the epoch. Used only for the coarse
/// SIGTERM-grace (60s) and restart-interval (10s) timers; the OS-clock read is
/// not a subsystem boundary worth seaming.
#[cfg(not(test))]
fn now_seconds() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// In tests, the clock is driven from a thread-local for deterministic timer
/// exercises.
#[cfg(test)]
fn now_seconds() -> i64 {
    tests::test_clock()
}

/// Install this crate's implementations into its seam crate.
pub fn init_seams() {
    backend_postmaster_pgarch_seams::pg_archiver_main::set(PgArchiverMain);
}
