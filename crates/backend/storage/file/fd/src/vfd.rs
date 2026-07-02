use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};

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

#[cfg(target_os = "linux")]
pub const PG_O_DIRECT: i32 = libc::O_DIRECT;
// storage/fd.h: the F_NOCACHE stand-in bit; asserted disjoint from open(2) flags.
#[cfg(target_os = "macos")]
pub const PG_O_DIRECT: i32 = 0x8000_0000u32 as i32;
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub const PG_O_DIRECT: i32 = 0;

#[cfg(target_os = "macos")]
const _: () = assert!(
    PG_O_DIRECT
        & (libc::O_APPEND
            | libc::O_CLOEXEC
            | libc::O_CREAT
            | libc::O_DSYNC
            | libc::O_EXCL
            | libc::O_RDWR
            | libc::O_RDONLY
            | libc::O_SYNC
            | libc::O_TRUNC
            | libc::O_WRONLY)
        == 0
);

pub(crate) struct Vfd {
    // `fd` -- None is VFD_CLOSED; the OwnedFd is the RAII close guard.
    pub fd: Option<OwnedFd>,
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
    pub allocated_descs: Vec<AllocateDesc>,
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
    IO_DIRECT_FLAGS, io_direct_flags, set_io_direct_flags, i32, 0;
    NUM_EXTERNAL_FDS, num_external_fds, set_num_external_fds, i32, 0;
    // file_perm.c globals (unported common unit); fd.c is their only backend
    // reader, so the storage lives here until that unit lands.
    PG_FILE_CREATE_MODE, pg_file_create_mode, set_pg_file_create_mode, u32, 0o600;
    PG_DIR_CREATE_MODE, pg_dir_create_mode, set_pg_dir_create_mode, u32, 0o700;
}

#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: returns the thread-local errno lvalue.
    unsafe { libc::__error() }
}
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd")))]
fn errno_location() -> *mut libc::c_int {
    // SAFETY: returns the thread-local errno lvalue.
    unsafe { libc::__errno_location() }
}

pub(crate) fn get_errno() -> i32 {
    // SAFETY: reading the thread-local errno.
    unsafe { *errno_location() }
}

pub(crate) fn set_errno(value: i32) {
    // SAFETY: writing the thread-local errno.
    unsafe {
        *errno_location() = value;
    }
}

pub(crate) fn cpath(path: &str) -> CString {
    CString::new(path.as_bytes()).unwrap_or_else(|_| CString::new("").unwrap())
}

pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("fd.c", 0, funcname)
}

pub(crate) fn Delete(fd: &mut FdState, file: i32) {
    debug_assert!(file != 0);
    let less = fd.vfd_cache[file as usize].lru_less_recently;
    let more = fd.vfd_cache[file as usize].lru_more_recently;
    fd.vfd_cache[less as usize].lru_more_recently = more;
    fd.vfd_cache[more as usize].lru_less_recently = less;
}

pub(crate) fn LruDelete(fd: &mut FdState, file: i32) -> PgResult<()> {
    debug_assert!(file != 0);

    let handle = fd.vfd_cache[file as usize].fd.take().expect("LruDelete on closed VFD");
    aio_seams::pgaio_closing_fd::call(handle.as_raw());

    let raw = handle.into_raw_fd();
    // SAFETY: `raw` is the live descriptor just released from the guard;
    // closed exactly once here.
    let close_failed = unsafe { libc::close(raw) } != 0;
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
        // SAFETY: `raw` is a freshly opened descriptor now owned by the VFD.
        fd.vfd_cache[file as usize].fd = Some(unsafe { OwnedFd::from_raw_fd(raw) });
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
    while fd.nfile + fd.allocated_descs.len() as i32 + num_external_fds() >= max_safe_fds() {
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
    let head = fd.vfd_cache[0].next_free;
    let vfd_p = &mut fd.vfd_cache[file as usize];
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
impl RawOf for OwnedFd {
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

fn before_shmem_exit_files_cb(code: i32, arg: datum::Datum) -> PgResult<()> {
    let _ = (code, arg);
    crate::sync::BeforeShmemExit_Files();
    Ok(())
}

pub(crate) fn count_usable_fds(max_to_probe: i32) -> PgResult<(i32, i32)> {
    let mut opened: Vec<i32> = Vec::with_capacity(1024);
    let mut used: i32 = 0;
    let mut highestfd: i32 = 0;

    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit writes the out-param struct.
    let getrlimit_status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
    if getrlimit_status != 0 {
        ereport(WARNING)
            .with_saved_errno(get_errno())
            .errmsg("getrlimit failed: %m")
            .finish(loc("count_usable_fds"))?;
    }

    loop {
        if getrlimit_status == 0 && highestfd as u64 >= (rlim.rlim_cur as u64).wrapping_sub(1) {
            break;
        }

        // SAFETY: dup(2) of stderr; yields a fresh fd or -1.
        let thisfd = unsafe { libc::dup(2) };
        if thisfd < 0 {
            let en = get_errno();
            if en != libc::EMFILE && en != libc::ENFILE {
                ereport(WARNING)
                    .with_saved_errno(en)
                    .errmsg_internal(format!(
                        "duplicating stderr file descriptor failed after {used} successes: %m"
                    ))
                    .finish(loc("count_usable_fds"))?;
            }
            break;
        }

        opened.push(thisfd);
        used += 1;
        if highestfd < thisfd {
            highestfd = thisfd;
        }
        if used >= max_to_probe {
            break;
        }
    }

    for &thisfd in &opened {
        // SAFETY: each entry is a live fd dup'd above.
        unsafe { libc::close(thisfd) };
    }

    Ok((used, highestfd + 1 - used))
}

pub fn set_max_safe_fds() -> PgResult<()> {
    let mfp = max_files_per_process();
    let (usable_fds, already_open) = count_usable_fds(mfp)?;

    let new_max = usable_fds.min(mfp) - NUM_RESERVED_FDS;
    set_max_safe_fds_value(new_max);

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
            .finish(loc("set_max_safe_fds"));
    }

    ::elog::elog(
        DEBUG2,
        format!("max_safe_fds = {new_max}, usable_fds = {usable_fds}, already_open = {already_open}"),
    )
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
        #[cfg(target_os = "macos")]
        // SAFETY: NUL-terminated path; PG_O_DIRECT is a synthetic bit masked off.
        let raw = unsafe {
            libc::open(path.as_ptr(), file_flags & !PG_O_DIRECT, file_mode as libc::c_uint)
        };
        #[cfg(not(target_os = "macos"))]
        // SAFETY: NUL-terminated path.
        let raw = unsafe { libc::open(path.as_ptr(), file_flags, file_mode as libc::c_uint) };

        if raw >= 0 {
            #[cfg(target_os = "macos")]
            if file_flags & PG_O_DIRECT != 0 {
                // SAFETY: `raw` is live; F_NOCACHE is macOS's O_DIRECT analogue.
                if unsafe { libc::fcntl(raw, libc::F_NOCACHE, 1) } < 0 {
                    let save_errno = get_errno();
                    // SAFETY: closing the fd we just opened.
                    unsafe { libc::close(raw) };
                    set_errno(save_errno);
                    return Ok(-1);
                }
            }
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
    let path = cpath(directory_name);
    // SAFETY: NUL-terminated path; mkdir(2) with the configured directory mode.
    unsafe { libc::mkdir(path.as_ptr(), pg_dir_create_mode() as libc::mode_t) }
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

    #[cold]
    #[inline(never)]
    fn unported(what: &str) -> ! {
        panic!("unported callee reached from fd.c: {what} (utils/resowner/resowner.c)")
    }

    pub(crate) fn current_resource_owner() -> ResourceOwner {
        unported("CurrentResourceOwner")
    }

    pub(crate) fn resource_owner_enlarge(_owner: ResourceOwner) {
        unported("ResourceOwnerEnlarge")
    }

    pub(crate) fn resource_owner_remember_file(_owner: ResourceOwner, _file: File) {
        unported("ResourceOwnerRemember")
    }

    pub(crate) fn resource_owner_forget_file(_owner: ResourceOwner, _file: File) {
        unported("ResourceOwnerForget")
    }
}
