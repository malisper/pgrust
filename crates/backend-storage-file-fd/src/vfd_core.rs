//! `fd-vfd-core` — the VFD cache and its supporting machinery.
//!
//! The [`Vfd`] record, the per-backend [`FdState`] (VFD cache + LRU ring +
//! free list + allocated-descriptor table + temp-file/tablespace state), the
//! GUC/global mirrors, the private LRU primitives (`Delete`/`LruDelete`/
//! `Insert`/`LruInsert`/`ReleaseLruFile(s)`), `AllocateVfd`/`FreeVfd`,
//! `FileAccess`/`FileInvalidate`, `BasicOpenFile[Perm]`, the external-FD
//! reservation family, `count_usable_fds`/`set_max_safe_fds`, and
//! `InitFileAccess`.

use std::cell::RefCell;
use std::fs::File as StdFile;
use std::path::Path;

use types_core::{Oid, SubTransactionId};
use types_error::{ErrorLevel, PgResult};
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
    pub next_free: File,
    /// `lruMoreRecently` — doubly linked recency-of-use list.
    pub lru_more_recently: File,
    /// `lruLessRecently`.
    pub lru_less_recently: File,
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
pub(crate) fn Delete(_file: File) {
    todo!("fd.c Delete: unlink from LRU ring")
}

/// `LruDelete(File file)` (fd.c) — close the kernel handle and remove from ring.
pub(crate) fn LruDelete(_file: File) -> PgResult<()> {
    todo!("fd.c LruDelete: close kernel fd, remove from LRU ring")
}

/// `Insert(File file)` (fd.c) — insert a VFD at the head of the LRU ring.
pub(crate) fn Insert(_file: File) {
    todo!("fd.c Insert: link at MRU end of LRU ring")
}

/// `LruInsert(File file)` (fd.c) — (re)open the kernel handle and insert.
pub(crate) fn LruInsert(_file: File) -> PgResult<i32> {
    todo!("fd.c LruInsert: reopen kernel fd, insert into LRU ring")
}

/// `ReleaseLruFile(void)` (fd.c) — close one LRU file; returns whether one was
/// available to close.
pub(crate) fn ReleaseLruFile() -> bool {
    todo!("fd.c ReleaseLruFile: close least-recently-used open kernel fd")
}

/// `ReleaseLruFiles(void)` (fd.c) — release LRU files until under the limit.
pub(crate) fn ReleaseLruFiles() -> PgResult<()> {
    todo!("fd.c ReleaseLruFiles: free fds until nfile + numAllocatedDescs < max_safe_fds")
}

/// `AllocateVfd(void)` (fd.c) — grab a free VFD slot, growing the cache as needed.
pub(crate) fn AllocateVfd() -> PgResult<File> {
    todo!("fd.c AllocateVfd: pull from free list or grow VfdCache")
}

/// `FreeVfd(File file)` (fd.c) — return a VFD slot to the free list.
pub(crate) fn FreeVfd(_file: File) {
    todo!("fd.c FreeVfd: free fileName, push onto free list")
}

/// `FileAccess(File file)` (fd.c) — ensure a VFD's kernel handle is open and
/// mark it most-recently-used. Returns 0 on success or sets errno on reopen
/// failure.
pub(crate) fn FileAccess(_file: File) -> PgResult<i32> {
    todo!("fd.c FileAccess: reopen if closed, move to MRU")
}

/// `FileInvalidate(File file)` (fd.c) — force a VFD's kernel handle closed
/// (used after a fork in EXEC_BACKEND-style reinit).
pub fn FileInvalidate(_file: File) -> PgResult<()> {
    todo!("fd.c FileInvalidate: LruDelete if open")
}

/// `ResourceOwnerForgetFile(ResourceOwner owner, File file)` (fd.c) — drop the
/// VFD's registration with its `ResourceOwner` (the resowner-> RAII `File`
/// ownership glue this family owns). Called by `FileClose` when
/// `Vfd::has_resowner`.
pub(crate) fn ResourceOwnerForgetFile(_file: File) {
    todo!("fd.c ResourceOwnerForgetFile: unregister File from its ResourceOwner")
}

// ---------------------------------------------------------------------------
// FD limit probing + initialization (fd.c:937-1100, 2802-2906 region).
// ---------------------------------------------------------------------------

/// `count_usable_fds(int max_to_probe, int *usable_fds, int *already_open)`
/// (fd.c) — probe how many fds we can actually open.
pub(crate) fn count_usable_fds(_max_to_probe: i32) -> (i32, i32) {
    todo!("fd.c count_usable_fds: probe by opening dup'd fds up to max_to_probe")
}

/// `set_max_safe_fds(void)` (fd.c) — compute `max_safe_fds` from the probe and
/// `max_files_per_process`.
pub fn set_max_safe_fds() -> PgResult<()> {
    todo!("fd.c set_max_safe_fds: max_safe_fds = Min(usable_fds, max_files_per_process) - NUM_RESERVED_FDS")
}

/// `InitFileAccess(void)` (fd.c) — initialize the VFD cache (entry 0 header)
/// and register the before-shmem-exit cleanup.
pub fn InitFileAccess() {
    todo!("fd.c InitFileAccess: alloc VfdCache[0] header, register BeforeShmemExit_Files")
}

/// `InitTemporaryFileAccess(void)` (fd.c) — enable temp-file accounting and
/// register the on-proc-exit temp-file cleanup.
pub fn InitTemporaryFileAccess() {
    todo!("fd.c InitTemporaryFileAccess: set temporary_files_allowed, register cleanup")
}

// ---------------------------------------------------------------------------
// BasicOpenFile (fd.c:1095-1170) — open a kernel fd directly (no VFD).
// ---------------------------------------------------------------------------

/// `BasicOpenFile(const char *fileName, int fileFlags)` (fd.c).
pub fn BasicOpenFile(file_name: impl AsRef<Path>, file_flags: i32) -> PgResult<StdFile> {
    BasicOpenFilePerm(file_name, file_flags, pg_file_create_mode())
}

/// `BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)`
/// (fd.c) — open a kernel fd, retrying after `ReleaseLruFiles` on EMFILE/ENFILE.
pub fn BasicOpenFilePerm(
    _file_name: impl AsRef<Path>,
    _file_flags: i32,
    _file_mode: u32,
) -> PgResult<StdFile> {
    todo!("fd.c BasicOpenFilePerm: open(2) with retry on out-of-fd")
}

// ---------------------------------------------------------------------------
// External-FD reservation family (fd.c:1180-1248).
// ---------------------------------------------------------------------------

/// `AcquireExternalFD(void)` (fd.c) — try to reserve one externally-consumed
/// fd against `max_safe_fds`; returns whether the reservation succeeded.
pub fn AcquireExternalFD() -> bool {
    todo!("fd.c AcquireExternalFD: bump numExternalFDs if room, else false")
}

/// `ReserveExternalFD(void)` (fd.c) — reserve one externally-consumed fd,
/// freeing LRU virtual fds if necessary (cannot fail).
pub fn ReserveExternalFD() {
    todo!("fd.c ReserveExternalFD: ReleaseLruFiles as needed, bump numExternalFDs")
}

/// `ReleaseExternalFD(void)` (fd.c) — release a reservation.
pub fn ReleaseExternalFD() {
    todo!("fd.c ReleaseExternalFD: decrement numExternalFDs")
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
pub fn data_sync_elevel(_elevel: ErrorLevel) -> ErrorLevel {
    todo!("fd.c data_sync_elevel: PANIC unless data_sync_retry")
}

/// `MakePGDirectory(const char *directoryName)` (fd.c) — `mkdir` with
/// `pg_dir_create_mode`. Seam adapter for `make_pg_directory`.
pub(crate) fn seam_make_pg_directory(_directory_name: &str) -> i32 {
    todo!("fd.c MakePGDirectory: mkdir(directoryName, pg_dir_create_mode)")
}
