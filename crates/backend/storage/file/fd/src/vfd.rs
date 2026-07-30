use std::cell::{Cell, RefCell};
use std::ffi::CString;

use ::vfs::VfsFd;

use ::elog::ereport;
use ::types_core::Oid;
use ::types_error::{
    ErrorLevel, ErrorLocation, PgResult, DEBUG2, ERRCODE_INSUFFICIENT_RESOURCES, FATAL, LOG,
    PANIC, WARNING,
};
use ::types_resowner::ResourceOwner;
use ::types_storage::{FD_MINFREE, NUM_RESERVED_FDS};

use crate::desc::AllocateDesc;

pub(crate) const FD_DELETE_AT_CLOSE: u16 = 1 << 0;
pub(crate) const FD_CLOSE_AT_EOXACT: u16 = 1 << 1;
pub(crate) const FD_TEMP_FILE_LIMIT: u16 = 1 << 2;

// The synthetic direct-IO bit lives in vfs (its F_NOCACHE mapping is
// PosixVfs's platform split); re-exported here for the existing callers.
pub use ::vfs::PG_O_DIRECT;

pub(crate) struct Vfd {
    // `fd` -- None is VFD_CLOSED; the VfsFd is the RAII close guard (finding
    // F1b: vfs-minted, so an unwind/thread-exit drop must release through the
    // vfs that minted it, never posix-side).
    pub fd: Option<VfsFd>,
    // Companion O_DIRECT descriptor, used ONLY by uring pool-read SQEs; every
    // other path (sync reads, writes, WAL, recovery) stays on the buffered fd
    // so kernel readahead/caching there is untouched.
    pub fd_dio: Option<VfsFd>,
    pub dio_failed: bool,
    pub fdstate: u16,
    pub resowner: ResourceOwner,
    pub next_free: i32,
    pub lru_more_recently: i32,
    pub lru_less_recently: i32,
    pub file_size: i64,
    pub file_name: Option<String>,
    pub file_flags: i32,
    pub file_mode: u32,
}

impl Vfd {
    pub(crate) const fn zeroed() -> Self {
        Vfd {
            fd: None,
            fd_dio: None,
            dio_failed: false,
            fdstate: 0,
            resowner: ResourceOwner::NULL,
            next_free: 0,
            lru_more_recently: 0,
            lru_less_recently: 0,
            file_size: 0,
            file_name: None,
            file_flags: 0,
            file_mode: 0,
        }
    }
}

pub(crate) struct FdState {
    pub vfd_cache: Vec<Vfd>,
    pub nfile: i32,
    pub have_xact_temporary_files: bool,
    pub temporary_files_size: u64,
    pub temporary_files_allowed: bool,
    /// Stable-index slots: C's FreeFile searches by FILE*, so its
    /// compaction is invisible; index handles require tombstones instead.
    pub allocated_descs: Vec<Option<AllocateDesc>>,
    pub max_allocated_descs: i32,
    pub temp_file_counter: i64,
    // None mirrors C's `numTempTableSpaces == -1`.
    pub temp_table_spaces: Option<Vec<Oid>>,
    pub next_temp_table_space: i32,
}

impl FdState {
    const fn new() -> Self {
        FdState {
            vfd_cache: Vec::new(),
            nfile: 0,
            have_xact_temporary_files: false,
            temporary_files_size: 0,
            temporary_files_allowed: false,
            allocated_descs: Vec::new(),
            max_allocated_descs: 0,
            temp_file_counter: 0,
            temp_table_spaces: None,
            next_temp_table_space: 0,
        }
    }

    pub(crate) fn size_vfd_cache(&self) -> usize {
        self.vfd_cache.len()
    }
}

thread_local! {
    static FD: RefCell<FdState> = const { RefCell::new(FdState::new()) };
}

pub(crate) fn with_fd<R>(f: impl FnOnce(&mut FdState) -> R) -> R {
    FD.with(|cell| f(&mut cell.borrow_mut()))
}

macro_rules! scalar_global {
    ($($cell:ident, $get:ident, $set:ident, $ty:ty, $init:expr;)+) => {
        $(
            thread_local! {
                static $cell: Cell<$ty> = const {
                    assert!(!core::mem::needs_drop::<$ty>());
                    Cell::new($init)
                };
            }

            pub fn $get() -> $ty {
                $cell.get()
            }

            pub fn $set(value: $ty) {
                $cell.set(value);
            }
        )+
    };
}

scalar_global! {
    MAX_FILES_PER_PROCESS, max_files_per_process, set_max_files_per_process, i32, 1000;
    MAX_SAFE_FDS, max_safe_fds, set_max_safe_fds_value, i32, FD_MINFREE;
    DATA_SYNC_RETRY, data_sync_retry, set_data_sync_retry, bool, false;
    RECOVERY_INIT_SYNC_METHOD, recovery_init_sync_method, set_recovery_init_sync_method,
        i32, ::types_storage::DATA_DIR_SYNC_METHOD_FSYNC;
    FILE_EXTEND_METHOD, file_extend_method, set_file_extend_method,
        i32, ::types_storage::DEFAULT_FILE_EXTEND_METHOD;
    // copydir.c global (FILE_COPY_METHOD_COPY boot default).
    FILE_COPY_METHOD, file_copy_method, set_file_copy_method, i32, 0;
    IO_DIRECT_FLAGS, io_direct_flags, set_io_direct_flags, i32, 0;
    NUM_EXTERNAL_FDS, num_external_fds, set_num_external_fds, i32, 0;
}

// file_perm.c globals (unported common unit); fd.c is their only backend
// reader, so the storage lives here until that unit lands.
//
// PROCESS-GLOBAL, not thread-local: C sets these once in checkDataDir()
// (postmaster startup, before any children exist) and every child inherits
// them by fork. Here children are threads — a thread-local would leave every
// backend at the 0600/0700 defaults after the postmaster enabled group access
// (thread-model hazard class 1; caught by pg_basebackup/010 group-permission
// leg: runtime-created tablespace/WAL files came out 0600 on a 0750 cluster).
static PG_FILE_CREATE_MODE: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0o600);
static PG_DIR_CREATE_MODE: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0o700);

pub fn pg_file_create_mode() -> u32 {
    PG_FILE_CREATE_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_pg_file_create_mode(mode: u32) {
    PG_FILE_CREATE_MODE.store(mode, std::sync::atomic::Ordering::Relaxed);
}

pub fn pg_dir_create_mode() -> u32 {
    PG_DIR_CREATE_MODE.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_pg_dir_create_mode(mode: u32) {
    PG_DIR_CREATE_MODE.store(mode, std::sync::atomic::Ordering::Relaxed);
}

// temp_tablespaces backing store; C home is tablespace.c (see lib.rs install).
thread_local! {
    static TEMP_TABLESPACES_GUC: core::cell::RefCell<Option<String>> =
        const { core::cell::RefCell::new(None) };
}

pub fn temp_tablespaces_guc() -> Option<String> {
    TEMP_TABLESPACES_GUC.with(|c| c.borrow().clone())
}

pub fn set_temp_tablespaces_guc(value: Option<String>) {
    TEMP_TABLESPACES_GUC.with(|c| *c.borrow_mut() = value);
}

// The shared errno TLS cell lives in vfs (DST P1 contract §1.1: SimVfs sets
// errno through the same cell fd reads).
pub(crate) use ::vfs::{get_errno, set_errno};

pub(crate) fn cpath(path: &str) -> CString {
    CString::new(path.as_bytes()).unwrap_or_else(|_| CString::new("").unwrap())
}

#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub(crate) fn Delete(fd: &mut FdState, file: i32) {
    debug_assert!(file != 0);
    let less = fd.vfd_cache[file as usize].lru_less_recently;
    let more = fd.vfd_cache[file as usize].lru_more_recently;
    fd.vfd_cache[less as usize].lru_more_recently = more;
    fd.vfd_cache[more as usize].lru_less_recently = less;
}

pub(crate) fn CloseDioFd(fd: &mut FdState, file: i32) {
    if let Some(dio) = fd.vfd_cache[file as usize].fd_dio.take() {
        crate::pgaio_closing_fd_if_engine_present(dio.as_raw());
        set_num_external_fds(num_external_fds() - 1);
        // Deliberate close path, made explicit (this used to be an implicit
        // posix-side OwnedFd drop — an F1b instance on a NON-unwind path):
        // disarm the guard and close through the vfs that minted the fd.
        // Result ignored, exactly as the old drop ignored it.
        let raw = dio.into_raw();
        let _ = vfs::close(raw);
    }
}

pub(crate) fn LruDelete(fd: &mut FdState, file: i32) -> PgResult<()> {
    debug_assert!(file != 0);

    CloseDioFd(fd, file);
    let handle = fd.vfd_cache[file as usize].fd.take().expect("LruDelete on closed VFD");
    crate::pgaio_closing_fd_if_engine_present(handle.as_raw());

    // Live descriptor just released from the guard (disarmed); closed
    // exactly once here.
    let raw = handle.into_raw();
    let close_failed = vfs::close(raw) != 0;
    fd.nfile -= 1;
    Delete(fd, file);

    if close_failed {
        let en = get_errno();
        let elevel = if fd.vfd_cache[file as usize].fdstate & FD_TEMP_FILE_LIMIT != 0 {
            LOG
        } else {
            data_sync_elevel(LOG)
        };
        let name = fd.vfd_cache[file as usize].file_name.clone().unwrap_or_default();
        ereport(elevel)
            .with_saved_errno(en)
            .errmsg_internal(format!("could not close file \"{name}\": %m"))
            .finish(loc("LruDelete"))?;
    }
    Ok(())
}

pub(crate) fn Insert(fd: &mut FdState, file: i32) {
    debug_assert!(file != 0);
    fd.vfd_cache[file as usize].lru_more_recently = 0;
    let prev_head = fd.vfd_cache[0].lru_less_recently;
    fd.vfd_cache[file as usize].lru_less_recently = prev_head;
    fd.vfd_cache[0].lru_less_recently = file;
    fd.vfd_cache[prev_head as usize].lru_more_recently = file;
}

pub(crate) fn LruInsert(fd: &mut FdState, file: i32) -> PgResult<i32> {
    debug_assert!(file != 0);

    if FileIsNotOpen(fd, file) {
        ReleaseLruFiles(fd)?;

        let name = fd.vfd_cache[file as usize].file_name.clone().unwrap_or_default();
        let flags = fd.vfd_cache[file as usize].file_flags;
        let mode = fd.vfd_cache[file as usize].file_mode;
        let raw = BasicOpenFilePermInternal(fd, &name, flags, mode)?;
        if raw < 0 {
            return Ok(-1);
        }
        // SAFETY: `raw` is a freshly vfs-opened descriptor now owned by the VFD.
        fd.vfd_cache[file as usize].fd = Some(unsafe { VfsFd::from_raw(raw) });
        fd.nfile += 1;
    }

    Insert(fd, file);
    Ok(0)
}

pub(crate) fn ReleaseLruFile(fd: &mut FdState) -> PgResult<bool> {
    if fd.nfile > 0 {
        debug_assert!(fd.vfd_cache[0].lru_more_recently != 0);
        let victim = fd.vfd_cache[0].lru_more_recently;
        LruDelete(fd, victim)?;
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn ReleaseLruFiles(fd: &mut FdState) -> PgResult<()> {
    while fd.nfile + occupied_descs(fd) + num_external_fds() >= max_safe_fds() {
        if !ReleaseLruFile(fd)? {
            break;
        }
    }
    Ok(())
}

pub(crate) fn AllocateVfd(fd: &mut FdState) -> i32 {
    debug_assert!(fd.size_vfd_cache() > 0, "InitFileAccess not called?");

    if fd.vfd_cache[0].next_free == 0 {
        let old_size = fd.size_vfd_cache();
        let new_size = (old_size * 2).max(32);

        // C reallocs and ereports ERROR on OOM; Vec growth aborts instead.
        fd.vfd_cache.reserve(new_size - old_size);
        for i in old_size..new_size {
            let mut v = Vfd::zeroed();
            v.next_free = (i + 1) as i32;
            fd.vfd_cache.push(v);
        }
        fd.vfd_cache[new_size - 1].next_free = 0;
        fd.vfd_cache[0].next_free = old_size as i32;
    }

    let file = fd.vfd_cache[0].next_free;
    fd.vfd_cache[0].next_free = fd.vfd_cache[file as usize].next_free;
    file
}

pub(crate) fn FreeVfd(fd: &mut FdState, file: i32) {
    CloseDioFd(fd, file);
    let head = fd.vfd_cache[0].next_free;
    let vfd_p = &mut fd.vfd_cache[file as usize];
    vfd_p.dio_failed = false;
    vfd_p.file_name = None;
    vfd_p.fdstate = 0x0;
    vfd_p.next_free = head;
    fd.vfd_cache[0].next_free = file;
}

pub(crate) fn FileAccess(fd: &mut FdState, file: i32) -> PgResult<i32> {
    if FileIsNotOpen(fd, file) {
        let rc = LruInsert(fd, file)?;
        if rc != 0 {
            return Ok(rc);
        }
    } else if fd.vfd_cache[0].lru_less_recently != file {
        Delete(fd, file);
        Insert(fd, file);
    }
    Ok(0)
}

// Resolves the companion O_DIRECT descriptor for uring pool-read SQEs (opened
// lazily, cached on the VFD, closed wherever the primary fd closes). -1 means
// the filesystem/platform refused O_DIRECT -- caller falls back to advisory
// prefetch, never to buffered uring (refuted, docs/optimizations/
// uring-buffer-reads.md).
pub(crate) fn FileAccessDio(fd: &mut FdState, file: i32) -> PgResult<i32> {
    if let Some(d) = &fd.vfd_cache[file as usize].fd_dio {
        return Ok(d.as_raw());
    }
    if fd.vfd_cache[file as usize].dio_failed || PG_O_DIRECT == 0 {
        return Ok(-1);
    }
    ReleaseLruFiles(fd)?;
    let name = fd.vfd_cache[file as usize].file_name.clone().unwrap_or_default();
    let flags = (fd.vfd_cache[file as usize].file_flags | PG_O_DIRECT)
        & !(libc::O_CREAT | libc::O_TRUNC | libc::O_EXCL);
    let mode = fd.vfd_cache[file as usize].file_mode;
    let raw = BasicOpenFilePermInternal(fd, &name, flags, mode)?;
    if raw < 0 {
        let en = get_errno();
        fd.vfd_cache[file as usize].dio_failed = true;
        use std::sync::atomic::{AtomicBool, Ordering};
        static LOGGED: AtomicBool = AtomicBool::new(false);
        if !LOGGED.swap(true, Ordering::Relaxed) {
            let _ = ereport(LOG)
                .errmsg_internal(format!(
                    "O_DIRECT unavailable for \"{name}\" (errno {en}); io_uring prefetch falls back to posix_fadvise"
                ))
                .finish(loc("FileAccessDio"));
        }
        return Ok(-1);
    }
    // SAFETY: freshly vfs-opened descriptor, owned by the VFD's companion slot.
    fd.vfd_cache[file as usize].fd_dio = Some(unsafe { VfsFd::from_raw(raw) });
    set_num_external_fds(num_external_fds() + 1);
    Ok(raw)
}

pub(crate) fn FileIsNotOpen(fd: &FdState, file: i32) -> bool {
    fd.vfd_cache[file as usize].fd.is_none()
}

pub(crate) fn FileIsValid(fd: &FdState, file: i32) -> bool {
    file > 0
        && (file as usize) < fd.size_vfd_cache()
        && fd.vfd_cache[file as usize].file_name.is_some()
}

pub(crate) trait RawOf {
    fn as_raw(&self) -> i32;
}
impl RawOf for VfsFd {
    fn as_raw(&self) -> i32 {
        use std::os::fd::AsRawFd;
        self.as_raw_fd()
    }
}

pub fn InitFileAccess() {
    with_fd(|fd| {
        debug_assert_eq!(fd.size_vfd_cache(), 0, "call me only once");
        fd.vfd_cache.push(Vfd::zeroed());
    });
}

pub fn InitTemporaryFileAccess() -> PgResult<()> {
    debug_assert!(with_fd(|fd| fd.size_vfd_cache() != 0));
    debug_assert!(!with_fd(|fd| fd.temporary_files_allowed), "call me only once");

    ipc_seams::before_shmem_exit::call(before_shmem_exit_files_cb, datum::Datum::from_i32(0))?;

    with_fd(|fd| fd.temporary_files_allowed = true);
    Ok(())
}

/// Retention claim (wretain): the VFD cache survived the park; the park's
/// exit callback closed temp files and cleared temporary_files_allowed, so
/// re-arm the callback and re-allow temp files (InitTemporaryFileAccess
/// minus its call-me-once assert).
pub fn ReattachRetainedFileAccess() -> PgResult<()> {
    debug_assert!(with_fd(|fd| fd.size_vfd_cache() != 0));
    debug_assert!(!with_fd(|fd| fd.temporary_files_allowed));
    ipc_seams::before_shmem_exit::call(before_shmem_exit_files_cb, datum::Datum::from_i32(0))?;
    with_fd(|fd| fd.temporary_files_allowed = true);
    Ok(())
}

fn before_shmem_exit_files_cb(code: i32, arg: datum::Datum) -> PgResult<()> {
    let _ = (code, arg);
    crate::sync::BeforeShmemExit_Files();
    Ok(())
}

// wasm32: WASI p1 has neither getrlimit nor dup(2)-probing; report the whole
// request usable with just stdio open (fd table is embedder-sized). The
// FD_MINFREE fatal arm in set_max_safe_fds still guards absurd
// max_files_per_process settings.
#[cfg(target_family = "wasm")]
pub(crate) fn count_usable_fds(max_to_probe: i32) -> PgResult<(i32, i32)> {
    Ok((max_to_probe, 3))
}

#[cfg(not(target_family = "wasm"))]
pub(crate) fn count_usable_fds(max_to_probe: i32) -> PgResult<(i32, i32)> {
    // The getrlimit + dup(2) probe runs inside the VFS (SimVfs: fixed pinned
    // budget, no real fds touched); this function keeps the C-shaped
    // diagnostics. The two WARNINGs now surface after the probe loop rather
    // than mid-loop — same lines, same errnos, diagnostics-only paths.
    let probe = vfs::fd_budget_probe_report(max_to_probe.max(0) as usize);

    if probe.getrlimit_failed {
        ereport(WARNING)
            .with_saved_errno(probe.getrlimit_errno)
            .errmsg("getrlimit failed: %m")
            .finish(loc("count_usable_fds"))?;
    }

    if probe.stop_errno != 0
        && probe.stop_errno != libc::EMFILE
        && probe.stop_errno != libc::ENFILE
    {
        let used = probe.used;
        ereport(WARNING)
            .with_saved_errno(probe.stop_errno)
            .errmsg_internal(format!(
                "duplicating stderr file descriptor failed after {used} successes: %m"
            ))
            .finish(loc("count_usable_fds"))?;
    }

    Ok((probe.used, probe.highest_fd + 1 - probe.used))
}

/// Descriptors one session thread holds outside the file-descriptor
/// cache, counted from the code that creates them:
///   1  client socket           (postmaster `AcceptConnection`, closed at
///                               child-thread exit)
///   2  waiter wake pipe        (`waiter::ensure_wake_pipe`, one pipe(2))
///   1  latch wait set          (`latch::InitializeLatchWaitSet`, epoll/kqueue)
///   1  FeBeWaitSet             (`pqcomm::pq_init`, epoll/kqueue)
/// The last three are also counted per thread by `num_external_fds`, so this
/// constant is the floor the budget hands out before any file is opened.
pub const PER_SESSION_SETUP_FDS: i32 = 5;

/// Descriptors the supervisor half keeps open for the life of the server:
/// listen sockets (MAXLISTEN = 64), the lock files, the control file, the
/// log-collector pipe. A flat allowance — it is paid once, not per session.
pub const POSTMASTER_RESERVED_FDS: i32 = 96;

/// The one-process fd budget (see `set_max_safe_fds`), factored out so the
/// arithmetic is testable without touching the real limits.
///
/// `process_ceiling` = RLIMIT_NOFILE soft limit in force (None = unknown,
/// which drops the sharing arm and reproduces C's per-process formula).
/// Returns the max_safe_fds to install.
pub fn compute_max_safe_fds(
    usable_fds: i32,
    max_files_per_process: i32,
    process_ceiling: Option<i32>,
    max_child_threads: i32,
    max_session_threads: i32,
) -> i32 {
    // C: Min(usable_fds, max_files_per_process) - NUM_RESERVED_FDS.
    let c_shaped = usable_fds.min(max_files_per_process);
    let Some(ceiling) = process_ceiling else {
        return c_shaped - NUM_RESERVED_FDS;
    };
    // One process, one fd table: every live child is a thread inside this
    // ceiling, so the file-descriptor cache each session may fill is the
    // ceiling MINUS every session's setup descriptors, divided by the
    // sessions that can hold a cache at once.
    let sessions = max_session_threads.max(1);
    let setup = PER_SESSION_SETUP_FDS.saturating_mul(max_child_threads.max(1));
    let for_caches = ceiling
        .saturating_sub(setup)
        .saturating_sub(POSTMASTER_RESERVED_FDS);
    let share = for_caches / sessions;
    // The share only ever LOWERS the answer: where the limit is generous
    // (containers, `nofile` raised to the hard limit) max_files_per_process
    // still binds and the result is byte-identical to C's.
    c_shaped.min(share) - NUM_RESERVED_FDS
}

/// set_max_safe_fds (fd.c), adapted to the one-process topology.
///
/// `max_child_threads` = every live postmaster child (each is a thread in
/// this fd table); `max_session_threads` = the children that hold a
/// file-descriptor cache. Both are 1 for single-user/wire boots.
pub fn set_max_safe_fds(max_child_threads: i32, max_session_threads: i32) -> PgResult<()> {
    // Raise the soft limit to the hard limit BEFORE probing: in C the soft
    // limit is a per-backend budget, here it is the whole server's.
    let limits = vfs::raise_fd_soft_limit_to_hard();
    // A failed getrlimit is not reported here (the ceiling is simply unknown
    // and the sharing arm drops out below): count_usable_fds carries C's
    // "getrlimit failed" WARNING, and platforms with no rlimits at all — the
    // simulated and wasm filesystems — must boot silently.
    if !limits.getrlimit_failed {
        if limits.soft_after > limits.soft_before {
            ::elog::elog(
                LOG,
                format!(
                    "raised open file limit from {} to {} (hard limit {})",
                    limits.soft_before,
                    limits.soft_after,
                    hard_limit_text(limits.hard)
                ),
            )?;
        } else if limits.setrlimit_errno != 0 {
            ereport(WARNING)
                .with_saved_errno(limits.setrlimit_errno)
                .errmsg("setrlimit(RLIMIT_NOFILE) failed: %m")
                .finish(loc("set_max_safe_fds"))?;
        }
    }

    let mfp = max_files_per_process();
    let (usable_fds, already_open) = count_usable_fds(mfp)?;

    let ceiling: Option<i32> = if limits.getrlimit_failed {
        None
    } else {
        Some(limits.soft_after.min(i32::MAX as u64) as i32)
    };

    let new_max =
        compute_max_safe_fds(usable_fds, mfp, ceiling, max_child_threads, max_session_threads);
    set_max_safe_fds_value(new_max);

    if new_max < FD_MINFREE {
        // Never boot a server whose sessions would die at connection setup:
        // C refuses the same way when one backend cannot get enough
        // descriptors, and here the shortage is the whole server's.
        let needed_per_session = FD_MINFREE + NUM_RESERVED_FDS;
        let needed_total = (needed_per_session as i64)
            * (max_session_threads.max(1) as i64)
            + (PER_SESSION_SETUP_FDS as i64) * (max_child_threads.max(1) as i64)
            + POSTMASTER_RESERVED_FDS as i64;
        return ereport(FATAL)
            .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
            .errmsg("insufficient file descriptors available to start server process")
            .errdetail(format!(
                "The whole server runs in one process: {} concurrent sessions \
                 (max_connections and friends) need at least {} open file \
                 descriptors, but the limit in force allows {} ({} already open, \
                 hard limit {}). Each session needs {} descriptors for its file \
                 cache plus {} for its socket, wake pipe and wait sets.",
                max_session_threads.max(1),
                needed_total,
                ceiling.map(|c| c.to_string()).unwrap_or_else(|| usable_fds.to_string()),
                already_open,
                hard_limit_text(limits.hard),
                needed_per_session,
                PER_SESSION_SETUP_FDS,
            ))
            .errhint(
                "Raise the open file limit (ulimit -n / LimitNOFILE), or lower \
                 max_connections."
                    .to_string(),
            )
            .finish(loc("set_max_safe_fds"));
    }

    ::elog::elog(
        DEBUG2,
        format!(
            "max_safe_fds = {new_max}, usable_fds = {usable_fds}, already_open = {already_open}, \
             sessions = {max_session_threads}, child threads = {max_child_threads}"
        ),
    )
}

fn hard_limit_text(hard: u64) -> String {
    if hard == u64::MAX {
        "unlimited".to_string()
    } else {
        hard.to_string()
    }
}

pub fn BasicOpenFile(file_name: &str, file_flags: i32) -> PgResult<i32> {
    BasicOpenFilePerm(file_name, file_flags, pg_file_create_mode())
}

// C contract: the raw kernel fd, or -1 with errno set.
pub fn BasicOpenFilePerm(file_name: &str, file_flags: i32, file_mode: u32) -> PgResult<i32> {
    with_fd(|fd| BasicOpenFilePermInternal(fd, file_name, file_flags, file_mode))
}

pub(crate) fn BasicOpenFilePermInternal(
    fd: &mut FdState,
    file_name: &str,
    file_flags: i32,
    file_mode: u32,
) -> PgResult<i32> {
    let path = cpath(file_name);

    loop {
        // The open(2) chokepoint. PG_O_DIRECT handling (macOS: mask +
        // F_NOCACHE) is PosixVfs's platform split; the EMFILE-retry LRU dance
        // below stays here.
        let raw = vfs::open(&path, file_flags, file_mode as libc::mode_t);
        if raw >= 0 {
            return Ok(raw);
        }

        if get_errno() == libc::EMFILE || get_errno() == libc::ENFILE {
            let save_errno = get_errno();
            ereport(LOG)
                .with_saved_errno(save_errno)
                .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
                .errmsg("out of file descriptors: %m; release and retry")
                .finish(loc("BasicOpenFilePerm"))?;
            set_errno(0);
            if ReleaseLruFile(fd)? {
                continue;
            }
            set_errno(save_errno);
        }

        return Ok(-1);
    }
}

pub fn AcquireExternalFD() -> PgResult<bool> {
    if num_external_fds() < max_safe_fds() / 3 {
        ReserveExternalFD()?;
        Ok(true)
    } else {
        set_errno(libc::EMFILE);
        Ok(false)
    }
}

pub fn ReserveExternalFD() -> PgResult<()> {
    with_fd(ReleaseLruFiles)?;
    set_num_external_fds(num_external_fds() + 1);
    Ok(())
}

pub fn ReleaseExternalFD() {
    debug_assert!(num_external_fds() > 0);
    set_num_external_fds(num_external_fds() - 1);
}

pub fn MakePGDirectory(directory_name: &str) -> i32 {
    // mkdir(2) with the configured directory mode (composite policy stays
    // here; the syscall is the VFS's).
    let path = cpath(directory_name);
    vfs::mkdir(&path, pg_dir_create_mode() as libc::mode_t)
}

pub fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    if data_sync_retry() {
        elevel
    } else {
        PANIC
    }
}

// check_debug_io_direct (fd.c:4007). PG_O_DIRECT != 0 on supported platforms
// and BLCKSZ/XLOG_BLCKSZ >= PG_IO_ALIGN_SIZE in the default config, so those
// compile-time reject branches are absent from this build.
pub fn check_debug_io_direct(newval: &str) -> PgResult<i32> {
    use ::types_error::PgError;
    use ::types_storage::{IO_DIRECT_DATA, IO_DIRECT_WAL, IO_DIRECT_WAL_INIT};

    let mut flags = 0;
    for item in newval.split(',') {
        // SplitGUCList over these unquoted identifiers is comma-split + trim.
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if item.eq_ignore_ascii_case("data") {
            flags |= IO_DIRECT_DATA;
        } else if item.eq_ignore_ascii_case("wal") {
            flags |= IO_DIRECT_WAL;
        } else if item.eq_ignore_ascii_case("wal_init") {
            flags |= IO_DIRECT_WAL_INIT;
        } else {
            return Err(PgError::error(format!("Invalid option \"{item}\".")).into());
        }
    }
    Ok(flags)
}

pub fn assign_debug_io_direct(flags: i32) {
    set_io_direct_flags(flags);
}

pub mod resowner {
    use ::datum::Datum;
    use ::mcx::{Mcx, PgString};
    use ::types_error::PgResult;
    use ::types_resowner::{
        ResourceOwner, ResourceOwnerDesc, RELEASE_PRIO_FILES, RESOURCE_RELEASE_AFTER_LOCKS,
    };
    use ::types_storage::File;

    pub static FILE_RESOWNER_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
        name: "File",
        release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
        release_priority: RELEASE_PRIO_FILES,
        ReleaseResource: ResOwnerReleaseFile,
        DebugPrint: Some(ResOwnerPrintFile),
    };

    pub fn ResOwnerReleaseFile(res: Datum) {
        let file = File(res.as_i32());
        super::with_fd(|fd| {
            debug_assert!(super::FileIsValid(fd, file.0));
            fd.vfd_cache[file.0 as usize].resowner = ResourceOwner::NULL;
        });
        let _ = crate::io::FileClose(file);
    }

    fn ResOwnerPrintFile<'a>(mcx: Mcx<'a>, res: Datum) -> PgResult<PgString<'a>> {
        PgString::from_str_in(&format!("File {}", res.as_i32()), mcx)
    }

    pub(crate) fn current_resource_owner() -> ResourceOwner {
        resowner_seams::current_resource_owner::call()
    }

    pub(crate) fn resource_owner_enlarge(owner: ResourceOwner) {
        resowner_seams::resource_owner_enlarge::call(owner).expect("ResourceOwnerEnlarge");
    }

    pub(crate) fn resource_owner_remember_file(owner: ResourceOwner, file: File) {
        resowner_seams::resource_owner_remember::call(
            owner,
            Datum::from_i32(file.0),
            &FILE_RESOWNER_DESC,
        )
        .expect("ResourceOwnerRememberFile");
    }

    pub(crate) fn resource_owner_forget_file(owner: ResourceOwner, file: File) {
        resowner_seams::resource_owner_forget::call(
            owner,
            Datum::from_i32(file.0),
            &FILE_RESOWNER_DESC,
        )
        .expect("ResourceOwnerForgetFile");
    }
}

pub(crate) fn occupied_descs(fd: &FdState) -> i32 {
    fd.allocated_descs.iter().flatten().count() as i32
}

/// `SHOW pgrust.resource_counters` (the simharness F8 resource-baseline hook
/// channel). Reports the ABOVE-VFD-CACHE counters — the class that must
/// return to baseline between statements (spec §2.1 "vfd-aware by
/// definition": LRU-cached vfds legitimately stay open, so they are exactly
/// what this string must NOT count):
///   allocated  = live AllocateFile/AllocateDir/OpenTransientFile descs
///                (0 between statements; leaks/holds move it)
///   maxdescs   = the allocated-desc cap (max_safe_fds/3 once scaled; a
///                backend thread stranded at the FD_MINFREE boot default
///                freezes it at 16 — the max_safe_fds-inheritance bug class)
///   safe       = this thread's max_safe_fds
///   maxfiles   = max_files_per_process (context for the numbers above)
/// Per-thread state only: single-session campaigns read it deterministically.
pub fn show_resource_counters() -> String {
    with_fd(|fd| {
        format!(
            "allocated={} maxdescs={} safe={} maxfiles={}",
            occupied_descs(fd),
            fd.max_allocated_descs,
            max_safe_fds(),
            max_files_per_process(),
        )
    })
}
