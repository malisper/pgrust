//! `storage/ipc/dsm_impl.c` — low-level dynamic shared memory primitives.
//!
//! Three of the C file's four implementations are ported: POSIX shm
//! (`shm_open`), System V shm (`shmget`), and mmap'd files under
//! `pg_dynshmem`. The Windows implementation (`dsm_impl_windows`, plus the
//! Windows bodies of `dsm_impl_pin_segment`/`dsm_impl_unpin_segment`) is not,
//! matching the repo's platform scope.
//!
//! The C out-parameters (`void **impl_private`, `void **mapped_address`,
//! `Size *mapped_size`) stay out-parameters (`&mut`); `impl_private` is the
//! [`DsmImplPrivate`] enum instead of a `void *` (System V caches its shm
//! ident there; C heap-allocates an `int` in `TopMemoryContext`, which an
//! inline enum variant replaces — dropping that allocation's OOM path).

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::ffi::CString;

use backend_utils_error::config;
use backend_utils_error::errno::current_errno;
use backend_utils_error::{elog, ereport};
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, SqlState, DEBUG4, ERRCODE_OUT_OF_MEMORY, ERROR,
};
use types_guc::config_enum_entry;
use types_storage::dsm_handle;

/// `dsm_op` (`storage/dsm_impl.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DsmOp {
    /// `DSM_OP_CREATE`.
    Create,
    /// `DSM_OP_ATTACH`.
    Attach,
    /// `DSM_OP_DETACH`.
    Detach,
    /// `DSM_OP_DESTROY`.
    Destroy,
}

// `dsm_impl.h` implementation selectors.
pub const DSM_IMPL_POSIX: i32 = 1;
pub const DSM_IMPL_SYSV: i32 = 2;
pub const DSM_IMPL_WINDOWS: i32 = 3;
pub const DSM_IMPL_MMAP: i32 = 4;

/// `DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE` — `HAVE_SHM_OPEN` holds on the
/// supported (non-Windows) platforms.
pub const DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE: i32 = DSM_IMPL_POSIX;

/// `PG_DYNSHMEM_DIR` — directory for on-disk state.
pub const PG_DYNSHMEM_DIR: &str = "pg_dynshmem";
/// `PG_DYNSHMEM_MMAP_FILE_PREFIX`.
pub const PG_DYNSHMEM_MMAP_FILE_PREFIX: &str = "mmap.";

/// `ZBUFFER_SIZE` — size of buffer used for zero-filling.
const ZBUFFER_SIZE: usize = 8192;

/// `dynamic_shared_memory_options` (the GUC enum table); the Windows entry is
/// absent because that implementation is not ported. C's NULL terminator is
/// dropped (slice).
pub static DYNAMIC_SHARED_MEMORY_OPTIONS: &[config_enum_entry] = &[
    config_enum_entry {
        name: "posix",
        val: DSM_IMPL_POSIX,
        hidden: false,
    },
    config_enum_entry {
        name: "sysv",
        val: DSM_IMPL_SYSV,
        hidden: false,
    },
    config_enum_entry {
        name: "mmap",
        val: DSM_IMPL_MMAP,
        hidden: false,
    },
];

// The two GUC-assigned globals of dsm_impl.c. Backend-private state, so
// thread_local (AGENTS.md "Backend-global state").
thread_local! {
    /// `dynamic_shared_memory_type` — implementation selector.
    static DYNAMIC_SHARED_MEMORY_TYPE: std::cell::Cell<i32> =
        const { std::cell::Cell::new(DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE) };
    /// `min_dynamic_shared_memory` — MB of main-region space reserved for DSM.
    static MIN_DYNAMIC_SHARED_MEMORY: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
}

pub fn dynamic_shared_memory_type() -> i32 {
    DYNAMIC_SHARED_MEMORY_TYPE.with(|c| c.get())
}

/// GUC assign hook target for `dynamic_shared_memory_type`.
pub fn set_dynamic_shared_memory_type(value: i32) {
    DYNAMIC_SHARED_MEMORY_TYPE.with(|c| c.set(value));
}

pub fn min_dynamic_shared_memory() -> i32 {
    MIN_DYNAMIC_SHARED_MEMORY.with(|c| c.get())
}

/// GUC assign hook target for `min_dynamic_shared_memory`.
pub fn set_min_dynamic_shared_memory(value: i32) {
    MIN_DYNAMIC_SHARED_MEMORY.with(|c| c.set(value));
}

/// The `void *impl_private` slot: per-segment implementation-private data.
/// Only the System V implementation uses it on the ported platforms (its
/// cached shm ident).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DsmImplPrivate {
    #[default]
    None,
    SysvIdent(libc::c_int),
}

// Wait events (generated `wait_event_types.h`; values cross-checked against
// the c2rust rendering — PG_WAIT_IO class).
pub const WAIT_EVENT_DSM_ALLOCATE: u32 = 0x0A00_0019;
pub const WAIT_EVENT_DSM_FILL_ZERO_WRITE: u32 = 0x0A00_001A;

/// `PG_FILE_MODE_OWNER` (`common/file_perm.h`): `S_IRUSR | S_IWUSR`.
const PG_FILE_MODE_OWNER: libc::mode_t = (libc::S_IRUSR | libc::S_IWUSR) as libc::mode_t;

/// `IPCProtection` (`portability/mem.h`): access/modify by user only.
const IPC_PROTECTION: libc::c_int = 0o600;

/// `PG_SHMAT_FLAGS` (`portability/mem.h`): 0 except on Solaris.
const PG_SHMAT_FLAGS: libc::c_int = 0;

/// `MAP_HASSEMAPHORE` is BSD-derived; absent (and unneeded) on Linux.
#[cfg(target_os = "linux")]
const MAP_HASSEMAPHORE: libc::c_int = 0;
#[cfg(not(target_os = "linux"))]
const MAP_HASSEMAPHORE: libc::c_int = libc::MAP_HASSEMAPHORE;

/// `MAP_NOSYNC` exists only on FreeBSD-style systems (`portability/mem.h`
/// defines it to 0 elsewhere).
const MAP_NOSYNC: libc::c_int = 0;

#[cfg(target_os = "macos")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}
#[cfg(target_os = "linux")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}
#[cfg(target_family = "wasm")]
fn errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

fn set_errno(value: i32) {
    unsafe {
        *errno_location() = value;
    }
}

fn loc(funcname: &str) -> ErrorLocation {
    ErrorLocation::new("dsm_impl.c", 0, funcname)
}

/// `errcode_for_dynamic_shared_memory()` — but taking the saved errno instead
/// of reading the global (the callers all report with a saved errno).
fn sqlstate_for_dynamic_shared_memory(saved_errno: i32) -> Option<SqlState> {
    if saved_errno == libc::EFBIG || saved_errno == libc::ENOMEM {
        Some(ERRCODE_OUT_OF_MEMORY)
    } else {
        None // use errcode_for_file_access on the builder
    }
}

/// `ereport(elevel, (errcode_for_dynamic_shared_memory(), errmsg(msg: %m)))`.
/// Returns `Err` exactly when `elevel >= ERROR`.
fn report_errno(elevel: ErrorLevel, saved_errno: i32, msg: String, funcname: &str) -> PgResult<()> {
    let mut builder = ereport(elevel).with_saved_errno(saved_errno);
    builder = match sqlstate_for_dynamic_shared_memory(saved_errno) {
        Some(code) => builder.errcode(code),
        None => builder.errcode_for_file_access(),
    };
    builder.errmsg(msg).finish(loc(funcname))
}

/// `dsm_impl_op` — perform a low-level shared memory operation as dictated by
/// `dynamic_shared_memory_type`.
///
/// Returns `Ok(true)` on success and `Ok(false)` on failure (after logging at
/// `elevel`, except for the silent `DSM_OP_CREATE` name-collision case);
/// `Err` is the `elevel >= ERROR` ereport.
pub fn dsm_impl_op(
    op: DsmOp,
    handle: dsm_handle,
    request_size: usize,
    impl_private: &mut DsmImplPrivate,
    mapped_address: &mut *mut u8,
    mapped_size: &mut usize,
    elevel: ErrorLevel,
) -> PgResult<bool> {
    debug_assert!(op == DsmOp::Create || request_size == 0);
    debug_assert!(
        (op != DsmOp::Create && op != DsmOp::Attach)
            || (mapped_address.is_null() && *mapped_size == 0)
    );

    // wasm64 single-user is a single address space with no shm_open/mmap/shmget,
    // so DSM "segments" are plain heap allocations tracked by handle. Route every
    // op to the in-process backend regardless of the GUC's value.
    #[cfg(target_family = "wasm")]
    {
        return dsm_impl_wasm(op, handle, request_size, mapped_address, mapped_size);
    }
    #[cfg(not(target_family = "wasm"))]
    match dynamic_shared_memory_type() {
        DSM_IMPL_POSIX => dsm_impl_posix(
            op,
            handle,
            request_size,
            impl_private,
            mapped_address,
            mapped_size,
            elevel,
        ),
        DSM_IMPL_SYSV => dsm_impl_sysv(
            op,
            handle,
            request_size,
            impl_private,
            mapped_address,
            mapped_size,
            elevel,
        ),
        DSM_IMPL_MMAP => dsm_impl_mmap(
            op,
            handle,
            request_size,
            impl_private,
            mapped_address,
            mapped_size,
            elevel,
        ),
        ty => {
            elog(ERROR, format!("unexpected dynamic shared memory type: {ty}"))?;
            Ok(false)
        }
    }
}

fn cstring(s: &str) -> CString {
    CString::new(s).expect("interior NUL in dsm segment name")
}

/// `dsm_impl_posix` — POSIX shared memory (`shm_open`/`shm_unlink`; sizing
/// and mapping as if the segments were files).
/// wasm64 in-process DSM backend. Single-user wasm has one address space and no
/// `shm_open`/`mmap`/`shmget`, so a DSM "segment" is just a leaked heap region
/// tracked by its `dsm_handle`; "attach" returns the same pointer (the lone
/// process created it), "detach" is a no-op (the region stays mapped for the
/// process), and "destroy" frees it. Mirrors the SysV-shmem heap-arena stub.
#[cfg(target_family = "wasm")]
fn dsm_impl_wasm(
    op: DsmOp,
    handle: dsm_handle,
    request_size: usize,
    mapped_address: &mut *mut u8,
    mapped_size: &mut usize,
) -> PgResult<bool> {
    use std::cell::RefCell;
    use std::collections::HashMap;
    thread_local! {
        // handle -> (boxed region ptr, byte length).
        static SEGMENTS: RefCell<HashMap<dsm_handle, (*mut u8, usize)>> =
            RefCell::new(HashMap::new());
    }

    match op {
        DsmOp::Create => {
            // A 8-byte-aligned zeroed region; leak it (lives for the process).
            let words = request_size.div_ceil(8).max(1);
            let mut backing: Vec<u64> = std::vec![0u64; words];
            let ptr = backing.as_mut_ptr() as *mut u8;
            std::mem::forget(backing);
            SEGMENTS.with(|s| s.borrow_mut().insert(handle, (ptr, request_size)));
            *mapped_address = ptr;
            *mapped_size = request_size;
            Ok(true)
        }
        DsmOp::Attach => {
            // The creating (lone) process already holds the region.
            match SEGMENTS.with(|s| s.borrow().get(&handle).copied()) {
                Some((ptr, size)) => {
                    *mapped_address = ptr;
                    *mapped_size = size;
                    Ok(true)
                }
                None => Ok(false), // no such segment
            }
        }
        DsmOp::Detach => {
            // Single address space: keep the region mapped, just clear the view.
            *mapped_address = std::ptr::null_mut();
            *mapped_size = 0;
            Ok(true)
        }
        DsmOp::Destroy => {
            if let Some((ptr, size)) = SEGMENTS.with(|s| s.borrow_mut().remove(&handle)) {
                let words = size.div_ceil(8).max(1);
                // SAFETY: reconstruct the leaked Vec<u64> to free it.
                unsafe {
                    drop(std::vec::Vec::from_raw_parts(ptr as *mut u64, words, words));
                }
            }
            *mapped_address = std::ptr::null_mut();
            *mapped_size = 0;
            Ok(true)
        }
    }
}

#[cfg(not(target_family = "wasm"))]
fn dsm_impl_posix(
    op: DsmOp,
    handle: dsm_handle,
    mut request_size: usize,
    _impl_private: &mut DsmImplPrivate,
    mapped_address: &mut *mut u8,
    mapped_size: &mut usize,
    elevel: ErrorLevel,
) -> PgResult<bool> {
    let name = format!("/PostgreSQL.{handle}");
    let cname = cstring(&name);

    // Handle teardown cases.
    if op == DsmOp::Detach || op == DsmOp::Destroy {
        if !mapped_address.is_null()
            && unsafe { libc::munmap((*mapped_address).cast(), *mapped_size) } != 0
        {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not unmap shared memory segment \"{name}\": %m"),
                "dsm_impl_posix",
            )?;
            return Ok(false);
        }
        *mapped_address = std::ptr::null_mut();
        *mapped_size = 0;
        if op == DsmOp::Destroy && unsafe { libc::shm_unlink(cname.as_ptr()) } != 0 {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not remove shared memory segment \"{name}\": %m"),
                "dsm_impl_posix",
            )?;
            return Ok(false);
        }
        return Ok(true);
    }

    // Create new segment or open an existing one for attach. Reserve/Release
    // ExternalFD reduce the probability of EMFILE failure even though we
    // close the FD before returning.
    backend_storage_file_seams::reserve_external_fd::call();

    let flags = libc::O_RDWR
        | if op == DsmOp::Create {
            libc::O_CREAT | libc::O_EXCL
        } else {
            0
        };
    let fd = unsafe { libc::shm_open(cname.as_ptr(), flags, PG_FILE_MODE_OWNER as libc::c_uint) };
    if fd == -1 {
        let en = current_errno();
        backend_storage_file_seams::release_external_fd::call();
        if op == DsmOp::Attach || en != libc::EEXIST {
            report_errno(
                elevel,
                en,
                format!("could not open shared memory segment \"{name}\": %m"),
                "dsm_impl_posix",
            )?;
        }
        return Ok(false);
    }

    // If attaching, determine the current size; if creating, set the size to
    // the requested value.
    if op == DsmOp::Attach {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } != 0 {
            // Back out what's already been done.
            let save_errno = current_errno();
            unsafe { libc::close(fd) };
            backend_storage_file_seams::release_external_fd::call();
            report_errno(
                elevel,
                save_errno,
                format!("could not stat shared memory segment \"{name}\": %m"),
                "dsm_impl_posix",
            )?;
            return Ok(false);
        }
        request_size = st.st_size as usize;
    } else if dsm_impl_posix_resize(fd, request_size as libc::off_t) != 0 {
        // Back out what's already been done.
        let save_errno = current_errno();
        unsafe { libc::close(fd) };
        backend_storage_file_seams::release_external_fd::call();
        unsafe { libc::shm_unlink(cname.as_ptr()) };
        report_errno(
            elevel,
            save_errno,
            format!("could not resize shared memory segment \"{name}\" to {request_size} bytes: %m"),
            "dsm_impl_posix",
        )?;
        return Ok(false);
    }

    // Map it.
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            request_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC,
            fd,
            0,
        )
    };
    if address == libc::MAP_FAILED {
        // Back out what's already been done.
        let save_errno = current_errno();
        unsafe { libc::close(fd) };
        backend_storage_file_seams::release_external_fd::call();
        if op == DsmOp::Create {
            unsafe { libc::shm_unlink(cname.as_ptr()) };
        }
        report_errno(
            elevel,
            save_errno,
            format!("could not map shared memory segment \"{name}\": %m"),
            "dsm_impl_posix",
        )?;
        return Ok(false);
    }
    *mapped_address = address.cast();
    *mapped_size = request_size;
    unsafe { libc::close(fd) };
    backend_storage_file_seams::release_external_fd::call();

    Ok(true)
}

/// `dsm_impl_posix_resize` — set the size of the region behind `fd`,
/// ensuring (on Linux) that the virtual memory is actually allocated so a
/// later access cannot SIGBUS. Returns non-zero on failure with `errno` set.
fn dsm_impl_posix_resize(fd: libc::c_int, size: libc::off_t) -> libc::c_int {
    let mut save_sigmask: libc::sigset_t = unsafe { std::mem::zeroed() };
    let under_postmaster = config::is_under_postmaster();

    // Block all blockable signals except SIGQUIT: posix_fallocate() can run
    // for a long time and is all-or-nothing, so repeated SIGUSR1 interruption
    // (e.g. recovery conflicts) must not starve the retry loop.
    if under_postmaster {
        let masks = backend_libpq_pqsignal::signal_masks();
        unsafe {
            libc::sigprocmask(libc::SIG_SETMASK, masks.block_sig(), &mut save_sigmask);
        }
    }

    backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(
        WAIT_EVENT_DSM_ALLOCATE,
    );

    #[cfg(target_os = "linux")]
    let rc = {
        // On Linux the shm fd is tmpfs-backed; ftruncate would leave a hole
        // that can SIGBUS later, so ask tmpfs to allocate pages now and fail
        // gracefully with ENOSPC. EINTR retry handles SIGCONT —
        // posix_fallocate doesn't restart automatically.
        let mut rc;
        loop {
            rc = unsafe { libc::posix_fallocate(fd, 0, size) };
            if rc != libc::EINTR {
                break;
            }
        }
        // posix_fallocate returns error numbers directly without setting
        // errno; the caller expects errno to be set.
        set_errno(rc);
        rc
    };
    #[cfg(not(target_os = "linux"))]
    let rc = {
        // Extend the file to the requested size.
        let mut rc;
        loop {
            rc = unsafe { libc::ftruncate(fd, size) };
            if !(rc < 0 && current_errno() == libc::EINTR) {
                break;
            }
        }
        rc
    };

    backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();

    if under_postmaster {
        let save_errno = current_errno();
        unsafe {
            libc::sigprocmask(libc::SIG_SETMASK, &save_sigmask, std::ptr::null_mut());
        }
        set_errno(save_errno);
    }

    rc
}

/// `dsm_impl_sysv` — System V shared memory (`shmget`/`shmat`/`shmdt`/
/// `shmctl`).
fn dsm_impl_sysv(
    op: DsmOp,
    handle: dsm_handle,
    mut request_size: usize,
    impl_private: &mut DsmImplPrivate,
    mapped_address: &mut *mut u8,
    mapped_size: &mut usize,
    elevel: ErrorLevel,
) -> PgResult<bool> {
    // POSIX and mmap identify segments with names; use the handle as the name
    // to avoid needless error message variation.
    let name = format!("{handle}");

    // The System V namespace is a key_t. If dsm_handle is bigger the cast
    // truncates the same bits the same way every time, which is all that
    // matters; we only make sure the key isn't negative.
    let mut key = handle as libc::key_t;
    if key < 1 {
        key = -key;
    }

    // IPC_PRIVATE can't be used: if we land on it during create, pretend the
    // segment already exists so the caller retries.
    if key == libc::IPC_PRIVATE {
        if op != DsmOp::Create {
            elog(DEBUG4, "System V shared memory key may not be IPC_PRIVATE")?;
        }
        set_errno(libc::EEXIST);
        return Ok(false);
    }

    // Map the key to a shared memory identifier with shmget(); cache the
    // ident in impl_private to avoid repeated lookups. (C heap-allocates an
    // int for the cache; the enum variant stores it inline.)
    let ident = match *impl_private {
        DsmImplPrivate::SysvIdent(ident) => ident,
        DsmImplPrivate::None => {
            let mut flags = IPC_PROTECTION;
            // When using shmget to find an existing segment, the size must be
            // passed as 0 (a non-zero size greater than the actual size gives
            // EINVAL).
            let mut segsize: libc::size_t = 0;

            if op == DsmOp::Create {
                flags |= libc::IPC_CREAT | libc::IPC_EXCL;
                segsize = request_size;
            }

            let ident = unsafe { libc::shmget(key, segsize, flags) };
            if ident == -1 {
                let en = current_errno();
                if op == DsmOp::Attach || en != libc::EEXIST {
                    report_errno(
                        elevel,
                        en,
                        "could not get shared memory segment: %m".to_string(),
                        "dsm_impl_sysv",
                    )?;
                }
                return Ok(false);
            }

            *impl_private = DsmImplPrivate::SysvIdent(ident);
            ident
        }
    };

    // Handle teardown cases.
    if op == DsmOp::Detach || op == DsmOp::Destroy {
        *impl_private = DsmImplPrivate::None;
        if !mapped_address.is_null() && unsafe { libc::shmdt((*mapped_address).cast()) } != 0 {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not unmap shared memory segment \"{name}\": %m"),
                "dsm_impl_sysv",
            )?;
            return Ok(false);
        }
        *mapped_address = std::ptr::null_mut();
        *mapped_size = 0;
        if op == DsmOp::Destroy
            && unsafe { libc::shmctl(ident, libc::IPC_RMID, std::ptr::null_mut()) } < 0
        {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not remove shared memory segment \"{name}\": %m"),
                "dsm_impl_sysv",
            )?;
            return Ok(false);
        }
        return Ok(true);
    }

    // If attaching, use IPC_STAT to determine the size.
    if op == DsmOp::Attach {
        let mut shm: libc::shmid_ds = unsafe { std::mem::zeroed() };
        if unsafe { libc::shmctl(ident, libc::IPC_STAT, &mut shm) } != 0 {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not stat shared memory segment \"{name}\": %m"),
                "dsm_impl_sysv",
            )?;
            return Ok(false);
        }
        request_size = shm.shm_segsz as usize;
    }

    // Map it.
    let address = unsafe { libc::shmat(ident, std::ptr::null(), PG_SHMAT_FLAGS) };
    if address == (-1isize) as *mut libc::c_void {
        // Back out what's already been done.
        let save_errno = current_errno();
        if op == DsmOp::Create {
            unsafe { libc::shmctl(ident, libc::IPC_RMID, std::ptr::null_mut()) };
        }
        report_errno(
            elevel,
            save_errno,
            format!("could not map shared memory segment \"{name}\": %m"),
            "dsm_impl_sysv",
        )?;
        return Ok(false);
    }
    *mapped_address = address.cast();
    *mapped_size = request_size;

    Ok(true)
}

/// `dsm_impl_mmap` — "shared memory" as files in `pg_dynshmem`, mapped into
/// the address space.
fn dsm_impl_mmap(
    op: DsmOp,
    handle: dsm_handle,
    mut request_size: usize,
    _impl_private: &mut DsmImplPrivate,
    mapped_address: &mut *mut u8,
    mapped_size: &mut usize,
    elevel: ErrorLevel,
) -> PgResult<bool> {
    let name = format!("{PG_DYNSHMEM_DIR}/{PG_DYNSHMEM_MMAP_FILE_PREFIX}{handle}");
    let cname = cstring(&name);

    // Handle teardown cases.
    if op == DsmOp::Detach || op == DsmOp::Destroy {
        if !mapped_address.is_null()
            && unsafe { libc::munmap((*mapped_address).cast(), *mapped_size) } != 0
        {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not unmap shared memory segment \"{name}\": %m"),
                "dsm_impl_mmap",
            )?;
            return Ok(false);
        }
        *mapped_address = std::ptr::null_mut();
        *mapped_size = 0;
        if op == DsmOp::Destroy && unsafe { libc::unlink(cname.as_ptr()) } != 0 {
            let en = current_errno();
            report_errno(
                elevel,
                en,
                format!("could not remove shared memory segment \"{name}\": %m"),
                "dsm_impl_mmap",
            )?;
            return Ok(false);
        }
        return Ok(true);
    }

    // Create new segment or open an existing one for attach.
    let flags = libc::O_RDWR
        | if op == DsmOp::Create {
            libc::O_CREAT | libc::O_EXCL
        } else {
            0
        };
    let fd = backend_storage_file_seams::open_transient_file::call(&name, flags)?;
    if fd == -1 {
        let en = current_errno();
        if op == DsmOp::Attach || en != libc::EEXIST {
            report_errno(
                elevel,
                en,
                format!("could not open shared memory segment \"{name}\": %m"),
                "dsm_impl_mmap",
            )?;
        }
        return Ok(false);
    }

    if op == DsmOp::Attach {
        // Determine the current size.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } != 0 {
            // Back out what's already been done.
            let save_errno = current_errno();
            backend_storage_file_seams::close_transient_file::call(fd);
            report_errno(
                elevel,
                save_errno,
                format!("could not stat shared memory segment \"{name}\": %m"),
                "dsm_impl_mmap",
            )?;
            return Ok(false);
        }
        request_size = st.st_size as usize;
    } else {
        // Zero-fill the file the hard way to ensure all the file space is
        // really allocated, so we don't later seg fault accessing the
        // mapping. (C pallocs the zero buffer for alignment; a stack buffer
        // serves the same purpose here.)
        let zbuffer = [0u8; ZBUFFER_SIZE];
        let mut remaining = request_size;
        let mut success = true;

        while success && remaining > 0 {
            let goal = remaining.min(ZBUFFER_SIZE);
            backend_utils_activity_waitevent_seams::pgstat_report_wait_start::call(
                WAIT_EVENT_DSM_FILL_ZERO_WRITE,
            );
            if unsafe { libc::write(fd, zbuffer.as_ptr().cast(), goal) } == goal as libc::ssize_t {
                remaining -= goal;
            } else {
                success = false;
            }
            backend_utils_activity_waitevent_seams::pgstat_report_wait_end::call();
        }

        if !success {
            // Back out what's already been done.
            let mut save_errno = current_errno();
            backend_storage_file_seams::close_transient_file::call(fd);
            unsafe { libc::unlink(cname.as_ptr()) };
            if save_errno == 0 {
                save_errno = libc::ENOSPC;
            }
            report_errno(
                elevel,
                save_errno,
                format!(
                    "could not resize shared memory segment \"{name}\" to {request_size} bytes: %m"
                ),
                "dsm_impl_mmap",
            )?;
            return Ok(false);
        }
    }

    // Map it.
    let address = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            request_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC,
            fd,
            0,
        )
    };
    if address == libc::MAP_FAILED {
        // Back out what's already been done.
        let save_errno = current_errno();
        backend_storage_file_seams::close_transient_file::call(fd);
        if op == DsmOp::Create {
            unsafe { libc::unlink(cname.as_ptr()) };
        }
        report_errno(
            elevel,
            save_errno,
            format!("could not map shared memory segment \"{name}\": %m"),
            "dsm_impl_mmap",
        )?;
        return Ok(false);
    }
    *mapped_address = address.cast();
    *mapped_size = request_size;

    if backend_storage_file_seams::close_transient_file::call(fd) != 0 {
        let en = current_errno();
        ereport(elevel)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!(
                "could not close shared memory segment \"{name}\": %m"
            ))
            .finish(loc("dsm_impl_mmap"))?;
        return Ok(false);
    }

    Ok(true)
}

/// `dsm_impl_pin_segment` — implementation-specific work to preserve a
/// segment with no attached backends. Only Windows needs to do anything (it
/// duplicates the handle into the postmaster); on the ported platforms this
/// is a no-op returning the NULL `impl_private_pm_handle`.
pub fn dsm_impl_pin_segment(_handle: dsm_handle, _impl_private: &DsmImplPrivate) -> usize {
    0
}

/// `dsm_impl_unpin_segment` — reverse of [`dsm_impl_pin_segment`]; a no-op
/// except on (unported) Windows, where it closes the postmaster-side handle.
pub fn dsm_impl_unpin_segment(_handle: dsm_handle, impl_private_pm_handle: &mut usize) {
    let _ = impl_private_pm_handle;
}
