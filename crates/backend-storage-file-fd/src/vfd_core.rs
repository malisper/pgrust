//! `fd-vfd-core` — the VFD cache and its supporting machinery.
//!
//! The [`Vfd`] record, the per-backend [`FdState`] (VFD cache + LRU ring +
//! free list + allocated-descriptor table + temp-file/tablespace state), the
//! GUC/global mirrors, the private LRU primitives (`Delete`/`LruDelete`/
//! `Insert`/`LruInsert`/`ReleaseLruFile(s)`), `AllocateVfd`/`FreeVfd`,
//! `FileAccess`/`FileInvalidate`, `BasicOpenFile[Perm]`, the external-FD
//! reservation family, `count_usable_fds`/`set_max_safe_fds`, and
//! `InitFileAccess`.

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::cell::RefCell;
use std::fs::File as StdFile;
#[cfg(not(target_family = "wasm"))]
use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
#[cfg(target_family = "wasm")]
use wasm_libc_shim::osfd::{AsRawFd, FromRawFd, IntoRawFd};
use std::path::Path;

use backend_storage_aio_seams as aio_seams;
use backend_storage_ipc_dsm_core_seams as ipc_seams;
use backend_utils_error::{elog, ereport};
use types_core::{Oid, SubTransactionId};
use types_tuple::Datum;
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, DEBUG2, ERRCODE_INSUFFICIENT_RESOURCES, FATAL, LOG, PANIC,
    WARNING,
};
use types_storage::{File, FD_MINFREE};

// ---------------------------------------------------------------------------
// fdstate bitflags (fd.c:195-197).
// ---------------------------------------------------------------------------

/// `FD_DELETE_AT_CLOSE` (fd.c:195) — delete the file when the VFD closes.
pub(crate) const FD_DELETE_AT_CLOSE: u16 = 1 << 0;
/// `FD_CLOSE_AT_EOXACT` (fd.c:196) — close at end of transaction.
pub(crate) const FD_CLOSE_AT_EOXACT: u16 = 1 << 1;
/// `FD_TEMP_FILE_LIMIT` (fd.c:197) — respect `temp_file_limit`.
pub(crate) const FD_TEMP_FILE_LIMIT: u16 = 1 << 2;

/// `NUM_RESERVED_FDS` (fd.c:129).
pub(crate) const NUM_RESERVED_FDS: i32 = 10;

// ---------------------------------------------------------------------------
// Vfd (fd.c:199-212).
// ---------------------------------------------------------------------------

/// `Vfd` (fd.c) — one virtual file descriptor record. The kernel handle is an
/// owned [`StdFile`] (`None` when the slot is physically closed, the
/// `VFD_CLOSED` state).
pub(crate) struct Vfd {
    /// open kernel handle, or `None` when physically closed (`fd == VFD_CLOSED`).
    pub handle: Option<StdFile>,
    /// `true` while a kernel handle is held.
    pub is_open: bool,
    /// `fdstate` bitflags (`FD_*`).
    pub fdstate: u16,
    /// whether this VFD is registered with a `ResourceOwner`.
    pub has_resowner: bool,
    /// `nextFree` — link to the next free VFD on the free list.
    pub next_free: i32,
    /// `lruMoreRecently` — doubly linked recency-of-use list.
    pub lru_more_recently: i32,
    /// `lruLessRecently`.
    pub lru_less_recently: i32,
    /// `fileSize` — current size of file (0 if not temporary).
    pub file_size: i64,
    /// `fileName` — name of file, or `None` for an unused VFD.
    pub file_name: Option<String>,
    /// `fileFlags` — open(2) flags for (re)opening the file.
    pub file_flags: i32,
    /// `fileMode` — mode to pass to open(2).
    pub file_mode: u32,
}

impl Vfd {
    pub(crate) const fn zeroed() -> Self {
        Self {
            handle: None,
            is_open: false,
            fdstate: 0,
            has_resowner: false,
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

// ---------------------------------------------------------------------------
// AllocateDesc table (fd.c:250-272) — owned by fd-allocated-desc, but its
// records live in the shared per-backend FdState here so all families see one
// cache.
// ---------------------------------------------------------------------------

/// `AllocateDescKind` (fd.c:250-256) discriminant carrying the owned handle.
pub(crate) enum AllocatedHandle {
    /// `AllocateDescFile` — a buffered stdio stream (`FILE *`).
    File(StdFile),
    /// `AllocateDescDir` — an `opendir` iterator.
    Dir(DirHandle),
    /// `AllocateDescRawFD` — an `OpenTransientFile` handle.
    RawFd(StdFile),
    /// `AllocateDescPipe` — a `popen`'d child process.
    Pipe(PipeHandle),
}

/// A `popen`'d pipe stream: the owned child plus the kept pipe end.
pub(crate) struct PipeHandle {
    pub child: std::process::Child,
    pub stdout: Option<std::process::ChildStdout>,
    pub stdin: Option<std::process::ChildStdin>,
}

/// A live directory iterator opened with `AllocateDir`.
pub(crate) struct DirHandle {
    pub iter: Option<std::fs::ReadDir>,
}

/// `AllocateDesc` (fd.c:258-268).
pub(crate) struct AllocateDesc {
    pub create_subid: SubTransactionId,
    pub desc: AllocatedHandle,
}

// ---------------------------------------------------------------------------
// Per-backend state (fd.c statics). thread_local!, never shared statics: this
// is per-backend private memory (AGENTS.md "Backend-global state").
// ---------------------------------------------------------------------------

/// All of fd.c's per-backend file-management state.
pub(crate) struct FdState {
    /// `Vfd *VfdCache`; entry 0 is the LRU ring / free-list header (not a
    /// usable VFD). `vfd_cache.len()` is `SizeVfdCache`.
    pub vfd_cache: Vec<Vfd>,
    /// `int nfile` — kernel handles currently held by VFD entries.
    pub nfile: i32,
    /// `have_xact_temporary_files`.
    pub have_xact_temporary_files: bool,
    /// `temporary_files_size`.
    pub temporary_files_size: u64,
    /// `temporary_files_allowed`.
    pub temporary_files_allowed: bool,
    /// `allocatedDescs` (`numAllocatedDescs`/`maxAllocatedDescs` == vec len/cap).
    pub allocated_descs: Vec<AllocateDesc>,
    /// `tempFileCounter`.
    pub temp_file_counter: i64,
    /// `tempTableSpaces`; `None` ≡ `numTempTableSpaces == -1`.
    pub temp_table_spaces: Option<Vec<Oid>>,
    /// `nextTempTableSpace`.
    pub next_temp_table_space: i32,
}

impl FdState {
    const fn new() -> Self {
        Self {
            vfd_cache: Vec::new(),
            nfile: 0,
            have_xact_temporary_files: false,
            temporary_files_size: 0,
            temporary_files_allowed: false,
            allocated_descs: Vec::new(),
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
    pub(crate) static FD: RefCell<FdState> = const { RefCell::new(FdState::new()) };
}

pub(crate) fn with_fd<R>(f: impl FnOnce(&mut FdState) -> R) -> R {
    FD.with(|cell| f(&mut cell.borrow_mut()))
}

// ---------------------------------------------------------------------------
// GUC / global mirrors. Per-backend (GUC-assigned or backend-private), so
// thread_local!, never shared statics.
// ---------------------------------------------------------------------------

/// `pg_dir_create_mode` (file_perm.c) default value: 0700.
const DEFAULT_PG_DIR_MODE: u32 = 0o700;
/// `PG_FILE_MODE_OWNER` (file_perm.h): 0600.
const PG_FILE_MODE_OWNER: u32 = 0o600;

struct Globals {
    max_files_per_process: i32,
    max_safe_fds: i32,
    data_sync_retry: bool,
    recovery_init_sync_method: i32,
    file_extend_method: i32,
    io_direct_flags: i32,
    num_external_fds: i32,
    pg_file_create_mode: u32,
    pg_dir_create_mode: u32,
    temp_file_limit: i32,
    log_temp_files: i32,
}

impl Globals {
    const fn new() -> Self {
        Self {
            max_files_per_process: 1000,
            max_safe_fds: FD_MINFREE,
            data_sync_retry: false,
            recovery_init_sync_method: 0, // DATA_DIR_SYNC_METHOD_FSYNC
            file_extend_method: 0,        // DEFAULT_FILE_EXTEND_METHOD
            io_direct_flags: 0,
            num_external_fds: 0,
            pg_file_create_mode: PG_FILE_MODE_OWNER,
            pg_dir_create_mode: DEFAULT_PG_DIR_MODE,
            temp_file_limit: -1,
            log_temp_files: -1,
        }
    }
}

thread_local! {
    static G: RefCell<Globals> = const { RefCell::new(Globals::new()) };
}

fn with_g<R>(f: impl FnOnce(&mut Globals) -> R) -> R {
    G.with(|cell| f(&mut cell.borrow_mut()))
}

pub fn max_files_per_process() -> i32 {
    with_g(|g| g.max_files_per_process)
}
pub fn set_max_files_per_process(value: i32) {
    with_g(|g| g.max_files_per_process = value);
}
pub fn max_safe_fds() -> i32 {
    with_g(|g| g.max_safe_fds)
}
pub fn data_sync_retry() -> bool {
    with_g(|g| g.data_sync_retry)
}
pub fn set_data_sync_retry(value: bool) {
    with_g(|g| g.data_sync_retry = value);
}
pub fn recovery_init_sync_method() -> i32 {
    with_g(|g| g.recovery_init_sync_method)
}
pub fn set_recovery_init_sync_method(value: i32) {
    with_g(|g| g.recovery_init_sync_method = value);
}
pub fn file_extend_method() -> i32 {
    with_g(|g| g.file_extend_method)
}
pub fn set_file_extend_method(value: i32) {
    with_g(|g| g.file_extend_method = value);
}
pub fn io_direct_flags() -> i32 {
    with_g(|g| g.io_direct_flags)
}
pub fn set_io_direct_flags(value: i32) {
    with_g(|g| g.io_direct_flags = value);
}

/// `check_debug_io_direct(char **newval, void **extra, GucSource source)`
/// (fd.c:4007) — GUC check-hook for `debug_io_direct`. Parses the comma list
/// into the `IO_DIRECT_*` flag set; `Ok(flags)` becomes the `*extra` value the
/// assign-hook stores, `Err(detail)` is the `GUC_check_errdetail` text.
///
/// `PG_O_DIRECT != 0` on every supported platform here, so the
/// `#if PG_O_DIRECT == 0` reject-non-empty branch is not in the build config.
/// The two `#if XLOG_BLCKSZ < PG_IO_ALIGN_SIZE` / `#if BLCKSZ < PG_IO_ALIGN_SIZE`
/// guards are compile-time false in the default block-size config (both 8192 >=
/// 4096), so they are likewise absent from the build and omitted here.
pub fn check_debug_io_direct(newval: &str, _source: types_guc::GucSource) -> Result<i32, String> {
    use types_storage::{IO_DIRECT_DATA, IO_DIRECT_WAL, IO_DIRECT_WAL_INIT};

    let mut flags = 0;
    // SplitGUCList on ',' for these simple unquoted identifiers is a
    // comma-split with surrounding-whitespace trimming.
    for item in newval.split(',') {
        let item = item.trim();
        // C's SplitGUCList produces no element for an all-empty input.
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
            return Err(format!("Invalid option \"{item}\"."));
        }
    }

    Ok(flags)
}

/// `assign_debug_io_direct(const char *newval, void *extra)` (fd.c:4094) —
/// GUC assign-hook: store the flag set computed by the check-hook.
pub fn assign_debug_io_direct(flags: i32) {
    set_io_direct_flags(flags);
}
pub fn num_external_fds() -> i32 {
    with_g(|g| g.num_external_fds)
}
pub fn pg_file_create_mode() -> u32 {
    with_g(|g| g.pg_file_create_mode)
}
pub fn set_pg_file_create_mode(value: u32) {
    with_g(|g| g.pg_file_create_mode = value);
}
pub fn pg_dir_create_mode() -> u32 {
    with_g(|g| g.pg_dir_create_mode)
}
pub fn set_pg_dir_create_mode(value: u32) {
    with_g(|g| g.pg_dir_create_mode = value);
}
/// `SetDataDirectoryCreatePerm(dataDirMode)` (`common/file_perm.c:33-50`).
///
/// If the data directory mode has group access, relax the create modes/mask to
/// allow group read/execute; else use owner-only defaults. Sets the
/// `pg_dir_create_mode` / `pg_file_create_mode` globals and returns
/// `(pg_mode_mask, pg_dir_create_mode)` so the caller (miscinit's
/// `checkDataDir`) can run its own `umask(pg_mode_mask)` and assign the
/// `data_directory_mode` GUC.
pub fn set_data_directory_create_perm(data_dir_mode: u32) -> (u32, u32) {
    use libc::{S_IRGRP, S_IRUSR, S_IRWXG, S_IRWXO, S_IRWXU, S_IWGRP, S_IWUSR, S_IXGRP};
    // file_perm.h constants.
    let pg_dir_mode_owner = S_IRWXU as u32;
    let pg_dir_mode_group = (S_IRWXU | S_IRGRP | S_IXGRP) as u32;
    let pg_file_mode_owner = (S_IRUSR | S_IWUSR) as u32;
    let pg_file_mode_group = (S_IRUSR | S_IWUSR | S_IRGRP) as u32;
    let pg_mode_mask_owner = (S_IRWXG | S_IRWXO) as u32;
    let pg_mode_mask_group = (S_IWGRP | S_IRWXO) as u32;

    let (dir_mode, file_mode, mode_mask) =
        if (pg_dir_mode_group & data_dir_mode) == pg_dir_mode_group {
            (pg_dir_mode_group, pg_file_mode_group, pg_mode_mask_group)
        } else {
            (pg_dir_mode_owner, pg_file_mode_owner, pg_mode_mask_owner)
        };
    with_g(|g| {
        g.pg_dir_create_mode = dir_mode;
        g.pg_file_create_mode = file_mode;
    });
    (mode_mask, dir_mode)
}

pub fn temp_file_limit() -> i32 {
    with_g(|g| g.temp_file_limit)
}
pub fn set_temp_file_limit(value: i32) {
    with_g(|g| g.temp_file_limit = value);
}
pub fn log_temp_files() -> i32 {
    with_g(|g| g.log_temp_files)
}
pub fn set_log_temp_files(value: i32) {
    with_g(|g| g.log_temp_files = value);
}
pub fn set_max_safe_fds_value(value: i32) {
    with_g(|g| g.max_safe_fds = value);
}

/// Test-only: whether this thread's VFD cache has been initialized.
#[doc(hidden)]
pub fn vfd_cache_is_initialized() -> bool {
    with_fd(|fd| fd.size_vfd_cache() != 0)
}

// ---------------------------------------------------------------------------
// LRU ring + free list (fd.c:1250-1550). Private routines.
// ---------------------------------------------------------------------------

/// `Delete(File file)` (fd.c) — unlink a VFD from the LRU ring.
pub(crate) fn Delete(fd: &mut FdState, file: i32) {
    debug_assert!(file != 0);

    let cache = &mut fd.vfd_cache;
    let less = cache[file as usize].lru_less_recently;
    let more = cache[file as usize].lru_more_recently;

    cache[less as usize].lru_more_recently = more;
    cache[more as usize].lru_less_recently = less;
}

/// `LruDelete(File file)` (fd.c) — close the kernel handle and remove from ring.
pub(crate) fn LruDelete(fd: &mut FdState, file: i32) {
    debug_assert!(file != 0);

    // pgaio_closing_fd(vfdP->fd): let the AIO subsystem drain in-flight IO that
    // still references this kernel fd before we close it.
    let raw = fd.vfd_cache[file as usize]
        .handle
        .as_ref()
        .map(AsRawFd::as_raw_fd)
        .unwrap_or(-1);
    aio_seams::pgaio_closing_fd::call(raw);

    // Close the file.  We aren't expecting this to fail; if it does, better to
    // leak the FD than to mess up our internal state.  Dropping the StdFile
    // performs the close(2); to mirror the C diagnostic on failure we close the
    // raw descriptor explicitly and inspect the result.
    let temp_limit = fd.vfd_cache[file as usize].fdstate & FD_TEMP_FILE_LIMIT != 0;
    if let Some(handle) = fd.vfd_cache[file as usize].handle.take() {
        let raw = handle.into_raw_fd();
        // SAFETY: `raw` is a live owned descriptor that we just took ownership
        // of; we close it exactly once here.
        if unsafe { libc::close(raw) } != 0 {
            let elevel = if temp_limit {
                LOG
            } else {
                data_sync_elevel(LOG)
            };
            let name = fd.vfd_cache[file as usize]
                .file_name
                .clone()
                .unwrap_or_default();
            let _ = elog(elevel, format!("could not close file \"{name}\": %m"));
        }
    }
    fd.vfd_cache[file as usize].is_open = false;
    fd.nfile -= 1;

    // delete the vfd record from the LRU ring
    Delete(fd, file);
}

/// `Insert(File file)` (fd.c) — insert a VFD at the head of the LRU ring.
pub(crate) fn Insert(fd: &mut FdState, file: i32) {
    debug_assert!(file != 0);

    let cache = &mut fd.vfd_cache;
    cache[file as usize].lru_more_recently = 0;
    let prev_head = cache[0].lru_less_recently;
    cache[file as usize].lru_less_recently = prev_head;
    cache[0].lru_less_recently = file;
    cache[prev_head as usize].lru_more_recently = file;
}

/// `LruInsert(File file)` (fd.c) — (re)open the kernel handle and insert.
///
/// Returns 0 on success, -1 on re-open failure (with errno set).
pub(crate) fn LruInsert(fd: &mut FdState, file: i32) -> PgResult<i32> {
    debug_assert!(file != 0);

    if FileIsNotOpen(fd, file) {
        // Close excess kernel FDs.
        ReleaseLruFiles(fd)?;

        // The open could still fail for lack of file descriptors, eg due to
        // overall system file table being full.  So, be prepared to release
        // another FD if necessary...
        let name = fd.vfd_cache[file as usize]
            .file_name
            .clone()
            .unwrap_or_default();
        let file_flags = fd.vfd_cache[file as usize].file_flags;
        let file_mode = fd.vfd_cache[file as usize].file_mode;
        match BasicOpenFilePermFd(&name, file_flags, file_mode)? {
            -1 => {
                return Ok(-1);
            }
            raw => {
                // SAFETY: `raw` is a freshly opened owned descriptor.
                fd.vfd_cache[file as usize].handle =
                    Some(unsafe { StdFile::from_raw_fd(raw) });
                fd.vfd_cache[file as usize].is_open = true;
                fd.nfile += 1;
            }
        }
    }

    // put it at the head of the Lru ring
    Insert(fd, file);

    Ok(0)
}

/// `ReleaseLruFile(void)` (fd.c) — close one LRU file; returns whether one was
/// available to close.
pub(crate) fn ReleaseLruFile(fd: &mut FdState) -> bool {
    if fd.nfile > 0 {
        // There are opened files and so there should be at least one used vfd
        // in the ring.
        debug_assert!(fd.vfd_cache[0].lru_more_recently != 0);
        let victim = fd.vfd_cache[0].lru_more_recently;
        LruDelete(fd, victim);
        true // freed a file
    } else {
        false // no files available to free
    }
}

/// `ReleaseLruFiles(void)` (fd.c) — release LRU files until under the limit.
pub(crate) fn ReleaseLruFiles(fd: &mut FdState) -> PgResult<()> {
    let max = max_safe_fds();
    while fd.nfile + (fd.allocated_descs.len() as i32) + num_external_fds() >= max {
        if !ReleaseLruFile(fd) {
            break;
        }
    }
    Ok(())
}

/// `AllocateVfd(void)` (fd.c) — grab a free VFD slot, growing the cache as needed.
pub(crate) fn AllocateVfd(fd: &mut FdState) -> PgResult<i32> {
    debug_assert!(fd.size_vfd_cache() > 0); // InitFileAccess not called?

    if fd.vfd_cache[0].next_free == 0 {
        // The free list is empty so it is time to increase the size of the
        // array.  We choose to double it each time this happens. However,
        // there's not much point in starting *real* small.
        let old_size = fd.size_vfd_cache();
        let mut new_cache_size = old_size * 2;
        if new_cache_size < 32 {
            new_cache_size = 32;
        }

        // Initialize the new entries and link them into the free list.
        fd.vfd_cache.reserve(new_cache_size - old_size);
        for i in old_size..new_cache_size {
            let mut v = Vfd::zeroed();
            v.next_free = (i + 1) as i32;
            // VfdCache[i].fd = VFD_CLOSED  ==>  is_open = false / handle = None
            fd.vfd_cache.push(v);
        }
        fd.vfd_cache[new_cache_size - 1].next_free = 0;
        fd.vfd_cache[0].next_free = old_size as i32;
    }

    let file = fd.vfd_cache[0].next_free;
    fd.vfd_cache[0].next_free = fd.vfd_cache[file as usize].next_free;

    Ok(file)
}

/// `FreeVfd(File file)` (fd.c) — return a VFD slot to the free list.
pub(crate) fn FreeVfd(fd: &mut FdState, file: i32) {
    fd.vfd_cache[file as usize].file_name = None;
    fd.vfd_cache[file as usize].fdstate = 0x0;

    fd.vfd_cache[file as usize].next_free = fd.vfd_cache[0].next_free;
    fd.vfd_cache[0].next_free = file;
}

/// `FileAccess(File file)` (fd.c) — ensure a VFD's kernel handle is open and
/// mark it most-recently-used.
///
/// Returns 0 on success, -1 on re-open failure (with errno set).
pub(crate) fn FileAccess(fd: &mut FdState, file: i32) -> PgResult<i32> {
    // Is the file open?  If not, open it and put it at the head of the LRU ring
    // (possibly closing the least recently used file to get an FD).
    if FileIsNotOpen(fd, file) {
        let return_value = LruInsert(fd, file)?;
        if return_value != 0 {
            return Ok(return_value);
        }
    } else if fd.vfd_cache[0].lru_less_recently != file {
        // We now know that the file is open and that it is not the last one
        // accessed, so we need to move it to the head of the Lru ring.
        Delete(fd, file);
        Insert(fd, file);
    }

    Ok(0)
}

/// `FileInvalidate(File file)` (fd.c, `#ifdef NOT_USED`) — force a VFD's kernel
/// handle closed.
pub fn FileInvalidate(file: File) -> PgResult<()> {
    let file = file.0;
    with_fd(|fd| {
        debug_assert!(FileIsValid(fd, file));
        if !FileIsNotOpen(fd, file) {
            LruDelete(fd, file);
        }
    });
    Ok(())
}

/// `FileIsNotOpen(file)` (fd.c:192) — `VfdCache[file].fd == VFD_CLOSED`.
pub(crate) fn FileIsNotOpen(fd: &FdState, file: i32) -> bool {
    !fd.vfd_cache[file as usize].is_open
}

/// `FileIsValid(file)` (fd.c:189).
pub(crate) fn FileIsValid(fd: &FdState, file: i32) -> bool {
    file > 0
        && (file as usize) < fd.size_vfd_cache()
        && fd.vfd_cache[file as usize].file_name.is_some()
}

/// `ResourceOwnerForgetFile(ResourceOwner owner, File file)` (fd.c) — drop the
/// VFD's registration with its `ResourceOwner` (the resowner-> RAII `File`
/// ownership glue this family owns). Called by `FileClose` when
/// `Vfd::has_resowner`.
///
/// fd.c models a registered file as `vfdP->resowner != NULL`, calls
/// `ResourceOwnerForget(owner, FileGetDatum(file), &file_resowner_desc)` to
/// unhook the File from the owner's tracked-array, then clears the back-link
/// (`vfdP->resowner = NULL`). This crate represents the registration as the
/// `Vfd::has_resowner` flag (see `RegisterTemporaryFile`); the resowner side
/// of the bookkeeping is the owner's responsibility, so forgetting the file is
/// simply clearing that flag.
pub(crate) fn ResourceOwnerForgetFile(file: i32) {
    with_fd(|fd| {
        fd.vfd_cache[file as usize].has_resowner = false;
    });
}

// ---------------------------------------------------------------------------
// FD limit probing + initialization (fd.c:937-1100, 2802-2906 region).
// ---------------------------------------------------------------------------

/// `count_usable_fds(int max_to_probe, int *usable_fds, int *already_open)`
/// (fd.c) — probe how many fds we can actually open. Returns
/// `(usable_fds, already_open)`.
pub(crate) fn count_usable_fds(max_to_probe: i32) -> (i32, i32) {
    let mut fd: Vec<i32> = Vec::with_capacity(1024);
    let mut used: i32 = 0;
    let mut highestfd: i32 = 0;

    // getrlimit(RLIMIT_NOFILE, &rlim)
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes into the provided rlimit struct.
    let getrlimit_status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if getrlimit_status != 0 {
        let _ = ereport(WARNING)
            .errcode_for_file_access()
            .errmsg("getrlimit failed: %m")
            .finish(here("count_usable_fds"));
    }

    // dup until failure or probe limit reached
    loop {
        // don't go beyond RLIMIT_NOFILE; causes irritating kernel logs on some
        // platforms
        if getrlimit_status == 0 && highestfd as u64 >= (rlim.rlim_cur as u64).wrapping_sub(1) {
            break;
        }

        // SAFETY: dup(2) of stderr (fd 2); returns a new fd or -1.
        let thisfd = unsafe { libc::dup(2) };
        if thisfd < 0 {
            // Expect EMFILE or ENFILE, else it's fishy
            let e = errno();
            if e != libc::EMFILE && e != libc::ENFILE {
                let _ = elog(
                    WARNING,
                    format!("duplicating stderr file descriptor failed after {used} successes: %m"),
                );
            }
            break;
        }

        fd.push(thisfd);
        used += 1;

        if highestfd < thisfd {
            highestfd = thisfd;
        }

        if used >= max_to_probe {
            break;
        }
    }

    // release the files we opened
    for &thisfd in &fd {
        // SAFETY: each entry is a live fd we dup'd above.
        unsafe { libc::close(thisfd) };
    }

    // Return results.  usable_fds is just the number of successful dups. We
    // assume that the system limit is highestfd+1 (remember 0 is a legal FD
    // number) and so already_open is highestfd+1 - usable_fds.
    let usable_fds = used;
    let already_open = highestfd + 1 - used;
    (usable_fds, already_open)
}

/// `set_max_safe_fds(void)` (fd.c) — compute `max_safe_fds` from the probe and
/// `max_files_per_process`.
pub fn set_max_safe_fds() -> PgResult<()> {
    // We want to set max_safe_fds to MIN(usable_fds, max_files_per_process)
    // less the slop factor for files that are opened without consulting fd.c.
    let mfp = max_files_per_process();
    let (usable_fds, already_open) = count_usable_fds(mfp);

    let mut new_max = usable_fds.min(mfp);

    // Take off the FDs reserved for system() etc.
    new_max -= NUM_RESERVED_FDS;

    set_max_safe_fds_value(new_max);

    // Make sure we still have enough to get by.
    if new_max < FD_MINFREE {
        return ereport(FATAL)
            .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
            .errmsg("insufficient file descriptors available to start server process")
            .errdetail(format!(
                "System allows {}, server needs at least {}, {} files are already open.",
                new_max + NUM_RESERVED_FDS,
                FD_MINFREE + NUM_RESERVED_FDS,
                already_open
            ))
            .finish(here("set_max_safe_fds"));
    }

    elog(
        DEBUG2,
        format!("max_safe_fds = {new_max}, usable_fds = {usable_fds}, already_open = {already_open}"),
    )
}

/// `InitFileAccess(void)` (fd.c) — initialize the VFD cache (entry 0 header).
pub fn InitFileAccess() {
    with_fd(|fd| {
        debug_assert_eq!(fd.size_vfd_cache(), 0); // call me only once

        // initialize cache header entry: VfdCache[0] zeroed, fd = VFD_CLOSED.
        let mut header = Vfd::zeroed();
        header.is_open = false; // VFD_CLOSED
        fd.vfd_cache.push(header);
        // SizeVfdCache = 1  ==>  vfd_cache.len() == 1
    });
}

/// `InitTemporaryFileAccess(void)` (fd.c) — enable temp-file accounting and
/// register the before-shmem-exit temp-file cleanup.
pub fn InitTemporaryFileAccess() -> PgResult<()> {
    debug_assert!(vfd_cache_is_initialized()); // InitFileAccess() needs to have run
    debug_assert!(!with_fd(|fd| fd.temporary_files_allowed)); // call me only once

    // Register before-shmem-exit hook to ensure temp files are dropped while we
    // can still report stats.
    ipc_seams::before_shmem_exit::call(before_shmem_exit_files_cb, Datum::from_i32(0))?;

    // USE_ASSERT_CHECKING: temporary_files_allowed = true
    with_fd(|fd| fd.temporary_files_allowed = true);
    Ok(())
}

/// Adapter matching the `before_shmem_exit` seam callback signature, dispatching
/// to `BeforeShmemExit_Files` (owned by the `sync_cleanup` sibling family).
fn before_shmem_exit_files_cb(_code: i32, _arg: Datum<'static>) -> PgResult<()> {
    crate::sync_cleanup::BeforeShmemExit_Files();
    Ok(())
}

// ---------------------------------------------------------------------------
// BasicOpenFile (fd.c:1095-1170) — open a kernel fd directly (no VFD).
// ---------------------------------------------------------------------------

/// `BasicOpenFile(const char *fileName, int fileFlags)` (fd.c).
pub fn BasicOpenFile(file_name: impl AsRef<Path>, file_flags: i32) -> PgResult<StdFile> {
    BasicOpenFilePerm(file_name, file_flags, pg_file_create_mode())
}

/// `BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)`
/// (fd.c) — open a kernel fd, retrying after `ReleaseLruFile` on EMFILE/ENFILE.
///
/// The C function returns an `int` fd or -1 (errno set); here a successful
/// open yields an owned [`StdFile`] and an open failure surfaces as `Err` with
/// the OS errno preserved (the kernel `open` already set it). The inner
/// [`BasicOpenFilePermFd`] keeps the raw `-1` semantics for VFD callers.
pub fn BasicOpenFilePerm(
    file_name: impl AsRef<Path>,
    file_flags: i32,
    file_mode: u32,
) -> PgResult<StdFile> {
    match BasicOpenFilePermFd(file_name, file_flags, file_mode)? {
        -1 => ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg("could not open file: %m")
            .finish(here("BasicOpenFilePerm"))
            .map(|()| unreachable!()),
        // SAFETY: `raw` is a freshly opened owned descriptor.
        raw => Ok(unsafe { StdFile::from_raw_fd(raw) }),
    }
}

/// Like [`BasicOpenFilePerm`] but, on open failure, returns the saved OS
/// `errno` instead of ereporting — mirroring C's `BasicOpenFilePerm`, which
/// returns `-1` with `errno` set so callers (e.g. `OpenTransientFilePerm`,
/// `durable_rename`) can inspect and tolerate specific errnos such as `ENOENT`.
pub fn BasicOpenFilePermOrErrno(
    file_name: impl AsRef<Path>,
    file_flags: i32,
    file_mode: u32,
) -> PgResult<Result<StdFile, i32>> {
    match BasicOpenFilePermFd(file_name, file_flags, file_mode)? {
        -1 => Ok(Err(errno())),
        // SAFETY: `raw` is a freshly opened owned descriptor.
        raw => Ok(Ok(unsafe { StdFile::from_raw_fd(raw) })),
    }
}

/// Inner of `BasicOpenFilePerm` returning the raw kernel fd (or -1, errno set),
/// matching the exact C control flow including the EMFILE/ENFILE retry loop.
pub(crate) fn BasicOpenFilePermFd(
    file_name: impl AsRef<Path>,
    file_flags: i32,
    file_mode: u32,
) -> PgResult<i32> {
    let cpath = path_to_cstring(file_name.as_ref());

    loop {
        // SAFETY: cpath is NUL-terminated; open(2) with flags+mode.
        let fd = unsafe { libc::open(cpath.as_ptr(), file_flags, file_mode as libc::c_uint) };

        if fd >= 0 {
            return Ok(fd); // success!
        }

        if errno() == libc::EMFILE || errno() == libc::ENFILE {
            let save_errno = errno();

            ereport(LOG)
                .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
                .errmsg("out of file descriptors: %m; release and retry")
                .finish(here("BasicOpenFilePerm"))?;
            set_errno(0);
            if with_fd(ReleaseLruFile) {
                continue; // goto tryAgain
            }
            set_errno(save_errno);
        }

        return Ok(-1); // failure
    }
}

// ---------------------------------------------------------------------------
// External-FD reservation family (fd.c:1180-1248).
// ---------------------------------------------------------------------------

/// `AcquireExternalFD(void)` (fd.c) — try to reserve one externally-consumed
/// fd against `max_safe_fds`; returns whether the reservation succeeded.
pub fn AcquireExternalFD() -> bool {
    // We don't want more than max_safe_fds / 3 FDs to be consumed for
    // "external" FDs.
    if num_external_fds() < max_safe_fds() / 3 {
        ReserveExternalFD();
        true
    } else {
        set_errno(libc::EMFILE);
        false
    }
}

/// `ReserveExternalFD(void)` (fd.c) — reserve one externally-consumed fd,
/// freeing LRU virtual fds if necessary (cannot fail).
pub fn ReserveExternalFD() {
    // Release VFDs if needed to stay safe.  Because we do this before
    // incrementing numExternalFDs, the final state will be as desired, i.e.,
    // nfile + numAllocatedDescs + numExternalFDs <= max_safe_fds.
    let _ = with_fd(ReleaseLruFiles);

    with_g(|g| g.num_external_fds += 1);
}

/// `ReleaseExternalFD(void)` (fd.c) — release a reservation.
///
/// This is guaranteed not to change errno, so it can be used in failure paths.
pub fn ReleaseExternalFD() {
    with_g(|g| {
        debug_assert!(g.num_external_fds > 0);
        g.num_external_fds -= 1;
    });
}

/// RAII wrapper around [`AcquireExternalFD`] / [`ReleaseExternalFD`].
pub struct ExternalFdReservation;

impl ExternalFdReservation {
    pub fn acquire() -> Option<Self> {
        AcquireExternalFD().then_some(Self)
    }
}

impl Drop for ExternalFdReservation {
    fn drop(&mut self) {
        ReleaseExternalFD();
    }
}

// ---------------------------------------------------------------------------
// Seam adapters installed by `init_seams`.
// ---------------------------------------------------------------------------

/// `data_sync_elevel(int elevel)` (fd.c) — bump fsync-failure elevel to PANIC
/// unless `data_sync_retry` is set.
pub fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    if data_sync_retry() {
        elevel
    } else {
        PANIC
    }
}

/// `MakePGDirectory(const char *directoryName)` (fd.c) — `mkdir` with
/// `pg_dir_create_mode`. Seam adapter for `make_pg_directory`. Returns the
/// `mkdir(2)` result (0 on success, -1 with errno set on failure).
pub(crate) fn seam_make_pg_directory(directory_name: &str) -> i32 {
    let cpath = path_to_cstring(Path::new(directory_name));
    // SAFETY: cpath is NUL-terminated; mkdir(2) with the configured dir mode.
    unsafe { libc::mkdir(cpath.as_ptr(), pg_dir_create_mode() as libc::mode_t) }
}

/// Inward-seam adapter for `init_file_access` — `InitFileAccess()` is
/// infallible; the seam carries the fallible `Init*` shape uniformly.
pub fn seam_init_file_access() -> PgResult<()> {
    InitFileAccess();
    Ok(())
}

/// Inward-seam adapter for `last_errno` — read the current OS `errno`.
pub fn seam_last_errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// Inward-seam adapter for `access_f_ok` — `access(path, F_OK)` (InitPostgres).
pub fn seam_access_f_ok(
    path: &str,
) -> PgResult<backend_storage_file_fd_seams::AccessResult> {
    use backend_storage_file_fd_seams::AccessResult;
    let cpath = path_to_cstring(Path::new(path));
    // SAFETY: cpath is NUL-terminated; access(2) probe for existence.
    let rc = unsafe { libc::access(cpath.as_ptr(), libc::F_OK) };
    if rc == 0 {
        return Ok(AccessResult::Ok);
    }
    let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    if e == libc::ENOENT {
        Ok(AccessResult::NoEnt)
    } else {
        Ok(AccessResult::Other(e))
    }
}

/// Inward-seam adapter for `unlink_file` — `unlink(2)`; `0` / `-errno`.
pub fn seam_unlink_file(path: &str) -> i32 {
    let cpath = path_to_cstring(Path::new(path));
    // SAFETY: cpath is NUL-terminated.
    if unsafe { libc::unlink(cpath.as_ptr()) } == 0 {
        0
    } else {
        -std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
    }
}

/// Inward-seam adapter for `rename_file` — `rename(2)`; `0` / `-errno`.
pub fn seam_rename_file(from: &str, to: &str) -> i32 {
    let cfrom = path_to_cstring(Path::new(from));
    let cto = path_to_cstring(Path::new(to));
    // SAFETY: both paths are NUL-terminated.
    if unsafe { libc::rename(cfrom.as_ptr(), cto.as_ptr()) } == 0 {
        0
    } else {
        -std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
    }
}

/// `pg_mkdir_p(path, pg_dir_create_mode)` (src/port/pgmkdirp.c) — create every
/// missing parent directory in `path`, POSIX `mkdir -p` semantics (changing the
/// umask to `~(S_IWUSR|S_IXUSR)` for the parents, restoring it for the final
/// component and on exit). Returns `Ok(())` on success, `Err(errno)` on `< 0`.
/// Seam adapter for `pg_mkdir_p`.
pub(crate) fn seam_pg_mkdir_p(path: &str) -> Result<(), i32> {
    let omode = pg_dir_create_mode() as libc::mode_t;
    // bytes copy we can mutate in place (the C truncates at each '/').
    let mut bytes: Vec<u8> = path.as_bytes().to_vec();
    bytes.push(0); // NUL terminator; we index up to the '\0'.

    let mut retval: Result<(), i32> = Ok(());

    // oumask = umask(0); numask = oumask & ~(S_IWUSR|S_IXUSR); umask(numask);
    // SAFETY: umask(2) only swaps the process file-creation mask.
    let oumask = unsafe { libc::umask(0) };
    let numask = oumask & !((libc::S_IWUSR | libc::S_IXUSR) as libc::mode_t);
    unsafe {
        libc::umask(numask);
    }

    let mut i = 0usize;
    if bytes[0] == b'/' {
        i += 1; // Skip leading '/'.
    }
    let mut last = false;
    while !last {
        if bytes[i] == 0 {
            last = true;
        } else if bytes[i] != b'/' {
            i += 1;
            continue;
        }
        // Save and NUL out the separator so `path[..i]` names this prefix.
        let saved = bytes[i];
        bytes[i] = 0;
        if !last && bytes[i + 1] == 0 {
            last = true;
        }
        if last {
            // SAFETY: restore umask for the final component.
            unsafe {
                libc::umask(oumask);
            }
        }

        let cpath = std::ffi::CString::new(&bytes[..i]).unwrap_or_else(|_| {
            std::ffi::CString::new("").unwrap()
        });

        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        // check for pre-existing directory
        // SAFETY: cpath is NUL-terminated; st is a valid out-param.
        if unsafe { libc::stat(cpath.as_ptr(), &mut st) } == 0 {
            if (st.st_mode & libc::S_IFMT) != libc::S_IFDIR {
                set_errno(if last { libc::EEXIST } else { libc::ENOTDIR });
                retval = Err(errno());
                break;
            }
        } else {
            let mode = if last {
                omode
            } else {
                (libc::S_IRWXU | libc::S_IRWXG | libc::S_IRWXO) as libc::mode_t
            };
            // SAFETY: cpath is NUL-terminated.
            if unsafe { libc::mkdir(cpath.as_ptr(), mode) } < 0 {
                retval = Err(errno());
                break;
            }
        }
        if !last {
            bytes[i] = saved; // restore '/'
        }
        i += 1;
    }

    // ensure we restored umask
    // SAFETY: umask(2).
    unsafe {
        libc::umask(oumask);
    }
    retval
}

/// `rmdir(path)` (libc) — remove the empty directory `path`. `Ok(())` on
/// success, `Err(errno)` on `< 0`. Seam adapter for the fd-seams `rmdir`.
pub(crate) fn seam_rmdir(path: &str) -> Result<(), i32> {
    let cpath = path_to_cstring(Path::new(path));
    // SAFETY: cpath is NUL-terminated.
    if unsafe { libc::rmdir(cpath.as_ptr()) } == 0 {
        Ok(())
    } else {
        Err(errno())
    }
}

// ---------------------------------------------------------------------------
// Small helpers.
// ---------------------------------------------------------------------------

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("fd.c", 0, funcname)
}

/// The calling thread's current `errno`.
fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// `pub(crate)` wrapper over [`set_errno`] for other modules (e.g. the
/// NULL-returning `OpenPipeStreamOrNull` leaving `errno` set).
pub(crate) fn set_errno_pub(value: i32) {
    set_errno(value)
}

/// Set the calling thread's `errno` (mirrors C `errno = x`).
fn set_errno(value: i32) {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
    // SAFETY: errno is a thread-local lvalue.
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
    // SAFETY: errno is a thread-local lvalue.
    unsafe {
        *libc::__errno_location() = value;
    }
}

/// Render a filesystem path as a NUL-terminated C string for the libc calls
/// that mirror fd.c's direct `open`/`mkdir`.
fn path_to_cstring(path: &Path) -> std::ffi::CString {
    #[cfg(not(target_family = "wasm"))]
    use std::os::unix::ffi::OsStrExt;
    #[cfg(target_family = "wasm")]
    use wasm_libc_shim::osfd::OsStrExt;
    std::ffi::CString::new(path.as_os_str().as_bytes())
        .unwrap_or_else(|_| std::ffi::CString::new("").unwrap())
}
