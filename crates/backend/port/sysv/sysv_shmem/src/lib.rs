//! Port of `src/backend/port/sysv_shmem.c` (PostgreSQL 18.3): shared memory
//! implemented using SysV facilities plus anonymous `mmap()`.
//!
//! As of PostgreSQL 9.3, we normally allocate only a very small amount of
//! System V shared memory (the interlock shim that protects the data
//! directory), and put the real shared-memory block in an anonymous `mmap()`
//! segment. We still require a SysV shmem block to exist, because mmap'd shmem
//! provides no way to find out how many processes are attached, which we need
//! for interlocking.
//!
//! # Model notes (audit against these)
//!
//! - `UsedShmemSegID`/`UsedShmemSegAddr` (C globals) and the file-local
//!   `AnonymousShmem`/`AnonymousShmemSize` statics are backend-private
//!   process-globals here (a `Mutex`-guarded [`ShmemState`]). The pointers are
//!   genuine shared-memory addresses (raw pointers, opacity inherited).
//! - The OS primitives (`shmget`/`shmat`/`shmdt`/`shmctl`/`mmap`/`munmap`/
//!   `stat`/`getpid`) are direct `libc` calls — the genuine OS boundary, not a
//!   Rust dependency, so per AGENTS.md no seam is introduced for them.
//! - `PG_SHMAT_FLAGS` is `0` on all supported platforms (the old
//!   `SHM_SHARE_MMU` is SunOS-only). `PG_MMAP_FLAGS` is
//!   `MAP_SHARED | MAP_ANONYMOUS | MAP_HASSEMAPHORE`.
//! - Huge-page logic (`MAP_HUGETLB`, `/proc/meminfo`, `MAP_HUGE_*`) is
//!   Linux-only and lives behind `cfg(target_os = "linux")`, faithfully; on
//!   other platforms `MAP_HUGETLB` is undefined, so `GetHugePageSize` returns
//!   `(0, 0)` and `huge_pages = on` is rejected, exactly as the C `#if
//!   defined(MAP_HUGETLB)` arms dictate.
//! - GUC reads (`huge_pages`, `shared_memory_type`, `huge_page_size`) go
//!   through `backend-utils-misc-guc-tables` var accessors; `SetConfigOption`
//!   for `huge_pages_status` goes through the guc seam.
//! - `AddToDataDirLockFile(LOCK_FILE_LINE_SHMEM_KEY, line)` records the shmem
//!   key/ID; `dsm_cleanup_using_control_segment` reclaims orphaned DSM; both
//!   via seams. `on_shmem_exit` registers the detach/delete/unmap callbacks.
//! - EXEC_BACKEND-only `PGSharedMemoryReAttach`/`NoReAttach` are not part of
//!   this (non-EXEC_BACKEND) build, matching the other ports; they have no seam
//!   declared. The `PG_SHMEM_ADDR` `requestedAddress` klugy workaround is
//!   EXEC_BACKEND-only, so `requestedAddress` is always NULL here.
//! - `ereport(FATAL)`/`ERROR` become `PgError`; `elog(LOG)` best-effort
//!   failures are swallowed as in C.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

// Imports shared by both the native and the wasm-stub arms. The native arm
// (`mod native`) pulls these in via `use super::*`; the wasm stub references
// `SHMEM_TYPE_MMAP`, `Size`, `PgResult`, `PGShmemHeader`, `PGShmemMagic`.
use ::guc_tables::consts::SHMEM_TYPE_MMAP;
use ::types_core::Size;
use ::types_error::PgResult;
use ::types_storage::storage::PGShmemHeader;

// ---------------------------------------------------------------------------
// wasm (single-process) stub.
//
// wasm has no SysV shared memory or `mmap` (`shmget`/`shmat`/`mmap`/`stat`/
// `getpid` and the `IPC_*`/`MAP_*`/errno families are absent from `libc`).
// Single-process wasm has ONE address space, so "shared memory" is simply a
// heap allocation that every (notional) backend in the same process sees. The
// stub allocates a leaked, zeroed, MAXALIGNed region of the requested size,
// writes the standard `PGShmemHeader` at its base, and returns the pointer.
// Detach is a no-op (the region lives for the process lifetime, like C's
// postmaster master mapping); the OS reclaims it at exit.
// ---------------------------------------------------------------------------
#[cfg(target_family = "wasm")]
mod wasm_stub {
    use super::*;
    use ::types_storage::storage::PGShmemMagic;

    /// `MAXALIGN(len)`.
    fn maxalign(len: usize) -> usize {
        const MAXIMUM_ALIGNOF: usize = 8;
        (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
    }

    /// `PGSharedMemoryCreate` — heap-backed single-process "shared" segment.
    pub fn PGSharedMemoryCreate(
        size: Size,
    ) -> PgResult<(*mut PGShmemHeader, *mut PGShmemHeader)> {
        debug_assert!(size > maxalign(core::mem::size_of::<PGShmemHeader>()));

        // Leak a zeroed, 8-byte-aligned region of `size` bytes. `Vec<u64>` gives
        // 8-byte alignment (MAXIMUM_ALIGNOF); leak it so the region is valid for
        // the process lifetime (the OS reclaims it at exit, matching C).
        let words = size.div_ceil(core::mem::size_of::<u64>());
        let mut backing: Vec<u64> = vec![0u64; words];
        let base = backing.as_mut_ptr() as *mut u8;
        core::mem::forget(backing);

        let hdr = base as *mut PGShmemHeader;
        // SAFETY: `hdr` points at a zeroed region sized for at least a header.
        unsafe {
            (*hdr).creatorPID = 1; // no getpid(); single process is PID 1.
            (*hdr).magic = PGShmemMagic;
            (*hdr).dsm_control = 0;
            (*hdr).device = 0;
            (*hdr).inode = 0;
            (*hdr).totalsize = size;
            (*hdr).freeoffset = maxalign(core::mem::size_of::<PGShmemHeader>());
        }
        Ok((hdr, hdr))
    }

    /// `PGSharedMemoryDetach` — no-op: the region persists for the process.
    pub fn PGSharedMemoryDetach() {}

    /// `PGSharedMemoryIsInUse` — single process, no foreign segments ever exist.
    pub fn PGSharedMemoryIsInUse(_id1: u64, _id2: u64) -> PgResult<bool> {
        Ok(false)
    }

    /// `GetHugePageSize` — huge pages unsupported on wasm.
    pub fn GetHugePageSize() -> (Size, i32) {
        (0, 0)
    }
}

#[cfg(target_family = "wasm")]
pub use wasm_stub::{
    GetHugePageSize, PGSharedMemoryCreate, PGSharedMemoryDetach, PGSharedMemoryIsInUse,
};

#[cfg(not(target_family = "wasm"))]
pub use native::{
    GetHugePageSize, PGSharedMemoryCreate, PGSharedMemoryDetach, PGSharedMemoryIsInUse,
};

#[cfg(not(target_family = "wasm"))]
mod native {
use super::*;
use std::sync::Mutex;
use ::guc_tables::consts::{HUGE_PAGES_ON, HUGE_PAGES_TRY};
use ::types_error::{PgError, ERROR, FATAL};
use ::types_storage::storage::{dsm_handle, PGShmemMagic};

/// `IPCProtection` (`port/sysv_shmem.c`) — access/modify by user only.
const IPC_PROTECTION: libc::c_int = 0o600;

/// `PG_SHMAT_FLAGS` (`portability/mem.h`) — `0` on all supported platforms.
const PG_SHMAT_FLAGS: libc::c_int = 0;

/// `LOCK_FILE_LINE_SHMEM_KEY` (`pidfile.h`) — the postmaster.pid line carrying
/// the shmem key/ID.
const LOCK_FILE_LINE_SHMEM_KEY: i32 = 7;

type IpcMemoryKey = libc::key_t;
type IpcMemoryId = libc::c_int;

/// `enum IpcMemoryState` — how a given `IpcMemoryId` relates to this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IpcMemoryState {
    /// unexpected failure to analyze the ID
    AnalysisFailure,
    /// pertinent to DataDir, has attached PIDs
    Attached,
    /// no segment of that ID
    Enoent,
    /// exists, but not pertinent to DataDir
    Foreign,
    /// pertinent to DataDir, no attached PIDs
    Unattached,
}

/// Backend-private process-global state: the C `UsedShmemSegID`/
/// `UsedShmemSegAddr` globals plus the `AnonymousShmem`/`AnonymousShmemSize`
/// file-local statics.
struct ShmemState {
    /// `unsigned long UsedShmemSegID`.
    used_shmem_seg_id: u64,
    /// `void *UsedShmemSegAddr` (the attached SysV shim address).
    used_shmem_seg_addr: *mut libc::c_void,
    /// `static void *AnonymousShmem`.
    anonymous_shmem: *mut libc::c_void,
    /// `static Size AnonymousShmemSize`.
    anonymous_shmem_size: Size,
}

// SAFETY: the pointers are shared-memory addresses; access is serialized by
// the Mutex, and the addresses are stable for the process lifetime (mirroring
// the C globals, which are likewise only touched in single-threaded startup /
// exit paths).
unsafe impl Send for ShmemState {}

static SHMEM_STATE: Mutex<ShmemState> = Mutex::new(ShmemState {
    used_shmem_seg_id: 0,
    used_shmem_seg_addr: std::ptr::null_mut(),
    anonymous_shmem: std::ptr::null_mut(),
    anonymous_shmem_size: 0,
});

/// `InternalIpcMemoryCreate(memKey, size)`.
///
/// Attempt to create a new shared memory segment with the specified key. Will
/// fail (return `Ok(None)`) if such a segment already exists. If successful,
/// attach the segment to the current process and return its attached address.
/// On success, callbacks are registered with `on_shmem_exit` to detach and
/// delete the segment.
fn internal_ipc_memory_create(
    mem_key: IpcMemoryKey,
    size: Size,
) -> PgResult<Option<*mut libc::c_void>> {
    // requestedAddress is EXEC_BACKEND-only (PG_SHMEM_ADDR); always NULL here.
    let requested_address: *mut libc::c_void = std::ptr::null_mut();

    // SAFETY: shmget syscall with plain integer args.
    let mut shmid = unsafe {
        libc::shmget(
            mem_key,
            size,
            libc::IPC_CREAT | libc::IPC_EXCL | IPC_PROTECTION,
        )
    };

    if shmid < 0 {
        let shmget_errno = errno();

        // Fail quietly if error indicates a collision with existing segment.
        if shmget_errno == libc::EEXIST
            || shmget_errno == libc::EACCES
            || shmget_errno == libc::EIDRM
        {
            return Ok(None);
        }

        // Some BSD-derived kernels return EINVAL, not EEXIST, if there is an
        // existing segment but it's smaller than "size". Distinguish via a
        // second try with size = 0.
        if shmget_errno == libc::EINVAL {
            // SAFETY: shmget probe with size 0.
            shmid = unsafe {
                libc::shmget(mem_key, 0, libc::IPC_CREAT | libc::IPC_EXCL | IPC_PROTECTION)
            };
            if shmid < 0 {
                // As above, fail quietly if we verify a collision.
                let e = errno();
                if e == libc::EEXIST || e == libc::EACCES || e == libc::EIDRM {
                    return Ok(None);
                }
                // Otherwise, fall through to report the original error.
            } else {
                // We succeeded in creating a zero-size segment; free it and
                // then fall through to report the original error.
                // SAFETY: shmctl IPC_RMID on the just-created segment.
                if unsafe { libc::shmctl(shmid, libc::IPC_RMID, std::ptr::null_mut()) } < 0 {
                    // elog(LOG) — best effort, ignore.
                }
            }
        }

        // Else complain and abort.
        let hint = if shmget_errno == libc::EINVAL {
            "\nThis error usually means that PostgreSQL's request for a shared memory \
             segment exceeded your kernel's SHMMAX parameter, or possibly that it is \
             less than your kernel's SHMMIN parameter.\n\
             The PostgreSQL documentation contains more information about shared \
             memory configuration."
        } else if shmget_errno == libc::ENOMEM {
            "\nThis error usually means that PostgreSQL's request for a shared memory \
             segment exceeded your kernel's SHMALL parameter.  You might need to \
             reconfigure the kernel with larger SHMALL.\n\
             The PostgreSQL documentation contains more information about shared \
             memory configuration."
        } else if shmget_errno == libc::ENOSPC {
            "\nThis error does *not* mean that you have run out of disk space.  It \
             occurs either if all available shared memory IDs have been taken, in \
             which case you need to raise the SHMMNI parameter in your kernel, or \
             because the system's overall limit for shared memory has been reached.\n\
             The PostgreSQL documentation contains more information about shared \
             memory configuration."
        } else {
            ""
        };
        return Err(PgError::new(
            FATAL,
            format!(
                "could not create shared memory segment: {}\n\
                 Failed system call was shmget(key={}, size={}, 0{:o}).{}",
                os_error_string(shmget_errno),
                mem_key as u64,
                size,
                libc::IPC_CREAT | libc::IPC_EXCL | IPC_PROTECTION,
                hint,
            ),
        ));
    }

    // Register on-exit routine to delete the new segment.
    dsm_core_seams::on_shmem_exit::call(
        ipc_memory_delete,
        types_tuple::Datum::from_i32(shmid),
    )?;

    // OK, should be able to attach to the segment.
    // SAFETY: shmat with the freshly-created shmid.
    let mem_address = unsafe { libc::shmat(shmid, requested_address, PG_SHMAT_FLAGS) };

    if mem_address == (-1isize) as *mut libc::c_void {
        return Err(PgError::new(
            FATAL,
            format!(
                "shmat(id={shmid}, addr={requested_address:p}, flags=0x{PG_SHMAT_FLAGS:x}) failed: {}",
                os_error_string(errno()),
            ),
        ));
    }

    // Register on-exit routine to detach new segment before deleting.
    dsm_core_seams::on_shmem_exit::call(
        ipc_memory_detach,
        types_tuple::Datum::from_usize(mem_address as usize),
    )?;

    // Store shmem key and ID in data directory lockfile.
    let line = format!("{:9} {:9}", mem_key as u64, shmid as u64);
    miscinit_seams::add_to_data_dir_lock_file::call(
        LOCK_FILE_LINE_SHMEM_KEY,
        &line,
    )?;

    Ok(Some(mem_address))
}

/// `IpcMemoryDetach(status, shmaddr)` — removes a shared memory segment from
/// the process' address space (an `on_shmem_exit` callback).
fn ipc_memory_detach(_status: i32, shmaddr: types_tuple::Datum<'static>) -> PgResult<()> {
    let addr = shmaddr.as_usize() as *mut libc::c_void;
    // SAFETY: shmdt on a previously-attached address.
    if unsafe { libc::shmdt(addr) } < 0 {
        // elog(LOG) — best effort, ignore.
    }
    Ok(())
}

/// `IpcMemoryDelete(status, shmId)` — deletes a shared memory segment (an
/// `on_shmem_exit` callback).
fn ipc_memory_delete(_status: i32, shm_id: types_tuple::Datum<'static>) -> PgResult<()> {
    // SAFETY: shmctl IPC_RMID on a known shmid.
    if unsafe { libc::shmctl(shm_id.as_i32(), libc::IPC_RMID, std::ptr::null_mut()) } < 0 {
        // elog(LOG) — best effort, ignore.
    }
    Ok(())
}

/// `PGSharedMemoryIsInUse(id1, id2)` — is a previously-existing shmem segment
/// still existing and in use?
pub fn PGSharedMemoryIsInUse(_id1: u64, id2: u64) -> PgResult<bool> {
    let (state, mem_address) = pg_shared_memory_attach(id2 as IpcMemoryId, std::ptr::null_mut())?;
    if !mem_address.is_null() {
        // SAFETY: shmdt on the address we just attached.
        if unsafe { libc::shmdt(mem_address as *const libc::c_void) } < 0 {
            // elog(LOG) — best effort, ignore.
        }
    }
    Ok(match state {
        IpcMemoryState::Enoent | IpcMemoryState::Foreign | IpcMemoryState::Unattached => false,
        IpcMemoryState::AnalysisFailure | IpcMemoryState::Attached => true,
    })
}

/// `PGSharedMemoryAttach(shmId, attachAt, &addr)` — test for a segment with id
/// `shm_id`. Returns the analyzed state and the attached address (`null` if not
/// attached).
fn pg_shared_memory_attach(
    shm_id: IpcMemoryId,
    attach_at: *mut libc::c_void,
) -> PgResult<(IpcMemoryState, *mut PGShmemHeader)> {
    // First, try to stat the shm segment ID, to see if it exists at all.
    // SAFETY: shmctl IPC_STAT into a zeroed shmid_ds.
    let mut shm_stat: libc::shmid_ds = unsafe { core::mem::zeroed() };
    if unsafe { libc::shmctl(shm_id, libc::IPC_STAT, &mut shm_stat as *mut libc::shmid_ds) } < 0 {
        let e = errno();
        // EINVAL: assume the segment no longer exists.
        if e == libc::EINVAL {
            return Ok((IpcMemoryState::Enoent, std::ptr::null_mut()));
        }
        // EACCES: no read permission => not a relevant Postgres segment.
        if e == libc::EACCES {
            return Ok((IpcMemoryState::Foreign, std::ptr::null_mut()));
        }
        // Otherwise assume the segment is in use (likely spec-compliant EIDRM).
        return Ok((IpcMemoryState::AnalysisFailure, std::ptr::null_mut()));
    }

    // Try to attach to the segment and see if it matches our data directory.
    let data_dir = init_small_seams::data_dir::call().unwrap_or_default();
    let statbuf = match stat(&data_dir) {
        Ok(s) => s,
        // can't stat; be conservative
        Err(_) => return Ok((IpcMemoryState::AnalysisFailure, std::ptr::null_mut())),
    };

    // SAFETY: shmat with the analyzed shmid.
    let hdr = unsafe { libc::shmat(shm_id, attach_at, PG_SHMAT_FLAGS) } as *mut PGShmemHeader;
    if hdr == (-1isize) as *mut PGShmemHeader {
        // Attachment failed.
        let e = errno();
        if e == libc::EINVAL {
            return Ok((IpcMemoryState::Enoent, std::ptr::null_mut())); // segment disappeared
        }
        if e == libc::EACCES {
            return Ok((IpcMemoryState::Foreign, std::ptr::null_mut())); // non-Postgres
        }
        return Ok((IpcMemoryState::AnalysisFailure, std::ptr::null_mut()));
    }

    // SAFETY: hdr is a freshly-attached PGShmemHeader.
    let hdr_ref = unsafe { &*hdr };
    if hdr_ref.magic != PGShmemMagic
        || hdr_ref.device != statbuf.st_dev
        || hdr_ref.inode != statbuf.st_ino
    {
        // It's either not a Postgres segment, or not one for my data directory.
        return Ok((IpcMemoryState::Foreign, hdr));
    }

    // It matches; test whether any processes are still attached. (We are, now,
    // but shm_nattch is from before we attached.)
    let state = if shm_stat.shm_nattch == 0 {
        IpcMemoryState::Unattached
    } else {
        IpcMemoryState::Attached
    };
    Ok((state, hdr))
}

/// `GetHugePageSize(Size *hugepagesize, int *mmap_flags)` — identify the huge
/// page size to use and compute the related mmap flags. Returns
/// `(hugepagesize, mmap_flags)`; `(0, 0)` if huge pages are not supported
/// (non-Linux: `MAP_HUGETLB` undefined).
pub fn GetHugePageSize() -> (Size, i32) {
    #[cfg(target_os = "linux")]
    {
        let mut default_hugepagesize: Size = 0;

        // On Linux, read /proc/meminfo looking for "Hugepagesize: nnnn kB".
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for buf in contents.lines() {
                // sscanf(buf, "Hugepagesize: %u %c", &sz, &ch) == 2
                if let Some(rest) = buf.strip_prefix("Hugepagesize:") {
                    let mut it = rest.split_whitespace();
                    if let (Some(sz_s), Some(ch_s)) = (it.next(), it.next()) {
                        if let Ok(sz) = sz_s.parse::<u64>() {
                            if ch_s.starts_with('k') {
                                default_hugepagesize = (sz as Size) * 1024;
                                break;
                            }
                            // We could accept other units besides kB, if needed.
                        }
                    }
                }
            }
        }

        let huge_page_size = ::guc_tables::vars::huge_page_size.read();
        let hugepagesize_local: Size = if huge_page_size != 0 {
            // If huge page size is requested explicitly, use that.
            (huge_page_size as Size) * 1024
        } else if default_hugepagesize != 0 {
            // Otherwise use the system default, if we have it.
            default_hugepagesize
        } else {
            // Assume 2MB if we can't find out.
            2 * 1024 * 1024
        };

        let mut mmap_flags_local: i32 = libc::MAP_HUGETLB;

        // On recent enough Linux, also include the explicit page size.
        // MAP_HUGE_MASK / MAP_HUGE_SHIFT.
        if hugepagesize_local != default_hugepagesize {
            let shift = pg_ceil_log2_64(hugepagesize_local as i64);
            mmap_flags_local |= (shift & libc::MAP_HUGE_MASK) << libc::MAP_HUGE_SHIFT;
        }

        (hugepagesize_local, mmap_flags_local)
    }
    #[cfg(not(target_os = "linux"))]
    {
        (0, 0)
    }
}

/// `pg_ceil_log2_64(num)` (`port/pg_bitutils.h`).
#[cfg(target_os = "linux")]
fn pg_ceil_log2_64(num: i64) -> i32 {
    if num <= 1 {
        return 0;
    }
    let v = (num - 1) as u64;
    (64 - v.leading_zeros()) as i32
}

/// `CreateAnonymousSegment(Size *size)` — create an anonymous `mmap()`ed shared
/// memory segment. Returns the mapped address and the (possibly rounded-up)
/// actual size.
fn create_anonymous_segment(size: Size) -> PgResult<(*mut libc::c_void, Size)> {
    let huge_pages = ::guc_tables::vars::huge_pages.read();

    let mut allocsize = size;
    let mut ptr: *mut libc::c_void = MAP_FAILED();
    let mut mmap_errno: libc::c_int = 0;

    #[cfg(not(target_os = "linux"))]
    {
        // PGSharedMemoryCreate should have dealt with this case.
        debug_assert!(huge_pages != HUGE_PAGES_ON);
    }

    #[cfg(target_os = "linux")]
    {
        if huge_pages == HUGE_PAGES_ON || huge_pages == HUGE_PAGES_TRY {
            // Round up the request size to a suitable large value.
            let (hugepagesize, mmap_flags) = GetHugePageSize();
            if allocsize % hugepagesize != 0 {
                allocsize += hugepagesize - (allocsize % hugepagesize);
            }
            // SAFETY: mmap of anonymous huge pages.
            ptr = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    allocsize,
                    libc::PROT_READ | libc::PROT_WRITE,
                    PG_MMAP_FLAGS | mmap_flags,
                    -1,
                    0,
                )
            };
            mmap_errno = errno();
            if huge_pages == HUGE_PAGES_TRY && ptr == MAP_FAILED() {
                // elog(DEBUG1, "mmap with MAP_HUGETLB failed ...") — best effort.
            }
        }
    }
    // Silence unused warnings on non-linux where the TRY/ON arms compile out.
    let _ = HUGE_PAGES_TRY;

    // Report whether huge pages are in use.
    guc_seams::set_config_option_internal_dynamic_default::call(
        "huge_pages_status",
        if ptr == MAP_FAILED() { "off" } else { "on" },
    )?;

    if ptr == MAP_FAILED() && huge_pages != HUGE_PAGES_ON {
        // Use the original size, not the rounded-up value, when falling back to
        // non-huge pages.
        allocsize = size;
        // SAFETY: mmap of anonymous shared memory.
        ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                allocsize,
                libc::PROT_READ | libc::PROT_WRITE,
                PG_MMAP_FLAGS,
                -1,
                0,
            )
        };
        mmap_errno = errno();
    }

    if ptr == MAP_FAILED() {
        let hint = if mmap_errno == libc::ENOMEM {
            format!(
                "\nThis error usually means that PostgreSQL's request for a shared \
                 memory segment exceeded available memory, swap space, or huge pages. \
                 To reduce the request size (currently {allocsize} bytes), reduce \
                 PostgreSQL's shared memory usage, perhaps by reducing \
                 \"shared_buffers\" or \"max_connections\"."
            )
        } else {
            String::new()
        };
        return Err(PgError::new(
            FATAL,
            format!(
                "could not map anonymous shared memory: {}{hint}",
                os_error_string(mmap_errno),
            ),
        ));
    }

    Ok((ptr, allocsize))
}

/// `PG_MMAP_FLAGS` (`portability/mem.h`) — `MAP_SHARED | MAP_ANONYMOUS`, plus
/// `MAP_HASSEMAPHORE` only `#ifdef MAP_HASSEMAPHORE` (a BSD/macOS flag that is
/// not defined on Linux/glibc), exactly as PostgreSQL's mem.h gates it.
#[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd",
          target_os = "netbsd", target_os = "openbsd", target_os = "dragonfly"))]
const PG_MMAP_FLAGS: libc::c_int = libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_HASSEMAPHORE;
#[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "freebsd",
              target_os = "netbsd", target_os = "openbsd", target_os = "dragonfly")))]
const PG_MMAP_FLAGS: libc::c_int = libc::MAP_SHARED | libc::MAP_ANONYMOUS;

/// `MAP_FAILED` as a `*mut c_void` (libc exposes it as a value via the macro).
#[inline]
fn MAP_FAILED() -> *mut libc::c_void {
    (-1isize) as *mut libc::c_void
}

/// `AnonymousShmemDetach(status, arg)` — detach from an anonymous mmap'd block
/// (an `on_shmem_exit` callback).
///
/// DIVERGENCE FROM C, tied to this tree's shared-memory model: in C the
/// `MAP_SHARED|MAP_ANONYMOUS` block is a kernel object that every backend
/// `fork()`s a view onto, and crash reinit (`shmem_exit(1)` →
/// `CreateSharedMemoryAndSemaphores()` in postmaster.c) detaches the old block
/// and `mmap()`s a *fresh* one. This tree's postmaster instead retains a single
/// segment for the cluster's lifetime: the per-subsystem `*ShmemInit` functions
/// publish `&'static` views of it into write-once cells, so it can never be
/// re-created, and crash reinit (statemachine.rs) deliberately skips
/// `CreateSharedMemoryAndSemaphores()` and keeps the postmaster's mapping as the
/// master copy every re-forked child inherits.
///
/// But crash reinit still runs `shmem_exit(1)` in the postmaster (faithful to
/// C, to drop this process' LWLocks / callbacks), and that fires THIS callback.
/// If we `munmap()`ed here, the postmaster would unmap its master segment with
/// nothing to re-map it — the very next re-forked startup child would inherit an
/// address space where the shared `ProcStructLock` / PGPROC array / latches all
/// point into an unmapped hole and SIGSEGV on first touch (in
/// `InitAuxiliaryProcess`). So in the postmaster process we keep the mapping:
/// the segment must outlive every reinit, and at genuine postmaster exit the OS
/// reclaims it anyway. A real child detaching its own private fork view on its
/// own exit is unaffected (separate page tables; the shared object persists).
fn anonymous_shmem_detach(_status: i32, _arg: types_tuple::Datum<'static>) -> PgResult<()> {
    // The postmaster owns the persistent master mapping that every re-forked
    // child inherits across crash reinit; it must never be unmapped while the
    // postmaster lives (see the divergence note above).
    if init_small_seams::is_postmaster_environment::call()
        && !init_small_seams::is_under_postmaster::call()
    {
        return Ok(());
    }

    let mut state = SHMEM_STATE.lock().unwrap();
    if !state.anonymous_shmem.is_null() {
        // SAFETY: munmap of the anonymous block we mapped.
        if unsafe { libc::munmap(state.anonymous_shmem, state.anonymous_shmem_size) } < 0 {
            // elog(LOG) — best effort, ignore.
        }
        state.anonymous_shmem = std::ptr::null_mut();
    }
    Ok(())
}

/// `PGSharedMemoryCreate(Size size, PGShmemHeader **shim)` — create a shared
/// memory segment of the given size and initialize its standard header. Returns
/// the real block pointer and sets `*shim`; the tuple is `(real_block, shim)`.
pub fn PGSharedMemoryCreate(
    mut size: Size,
) -> PgResult<(*mut PGShmemHeader, *mut PGShmemHeader)> {
    let huge_pages = ::guc_tables::vars::huge_pages.read();
    let shared_memory_type = ::guc_tables::vars::shared_memory_type.read();

    // We use the data directory's ID info to positively identify shmem segments
    // associated with this data dir, and as seeds for searching for a free key.
    let data_dir = init_small_seams::data_dir::call().unwrap_or_default();
    let statbuf = match stat(&data_dir) {
        Ok(s) => s,
        Err(e) => {
            return Err(PgError::new(
                FATAL,
                format!("could not stat data directory \"{data_dir}\": {}", os_error_string(e)),
            ))
        }
    };

    // Complain if hugepages demanded but we can't possibly support them.
    #[cfg(not(target_os = "linux"))]
    {
        if huge_pages == HUGE_PAGES_ON {
            return Err(PgError::new(
                ERROR,
                "huge pages not supported on this platform".to_string(),
            ));
        }
    }

    // For now, we don't support huge pages in SysV memory.
    if huge_pages == HUGE_PAGES_ON && shared_memory_type != SHMEM_TYPE_MMAP {
        return Err(PgError::new(
            ERROR,
            "huge pages not supported with the current \"shared_memory_type\" setting".to_string(),
        ));
    }

    // Room for a header?
    debug_assert!(size > maxalign(core::mem::size_of::<PGShmemHeader>()));

    let sysvsize: Size;
    if shared_memory_type == SHMEM_TYPE_MMAP {
        let (anon, actual) = create_anonymous_segment(size)?;
        size = actual;
        {
            let mut state = SHMEM_STATE.lock().unwrap();
            state.anonymous_shmem = anon;
            state.anonymous_shmem_size = size;
        }
        // Register on-exit routine to unmap the anonymous segment.
        dsm_core_seams::on_shmem_exit::call(
            anonymous_shmem_detach,
            types_tuple::Datum::from_usize(0),
        )?;
        // Now we need only allocate a minimal-sized SysV shmem block.
        sysvsize = core::mem::size_of::<PGShmemHeader>();
    } else {
        sysvsize = size;
        // huge pages are only available with mmap.
        guc_seams::set_config_option_internal_dynamic_default::call(
            "huge_pages_status",
            "off",
        )?;
    }

    // Loop till we find a free IPC key.
    let mut next_shmem_seg_id: IpcMemoryKey = statbuf.st_ino as IpcMemoryKey;

    let mem_address: *mut libc::c_void = loop {
        // Try to create new segment.
        if let Some(addr) = internal_ipc_memory_create(next_shmem_seg_id, sysvsize)? {
            break addr; // successful create and attach
        }

        // Check shared memory and possibly remove and recreate.
        // SAFETY: shmget probe without creation flags.
        let shmid = unsafe {
            libc::shmget(next_shmem_seg_id, core::mem::size_of::<PGShmemHeader>(), 0)
        };
        let (state, oldhdr) = if shmid < 0 {
            (IpcMemoryState::Foreign, std::ptr::null_mut())
        } else {
            pg_shared_memory_attach(shmid, std::ptr::null_mut())?
        };

        match state {
            IpcMemoryState::AnalysisFailure | IpcMemoryState::Attached => {
                if !oldhdr.is_null() {
                    // SAFETY: shmdt the segment we attached during analysis.
                    let _ = unsafe { libc::shmdt(oldhdr as *const libc::c_void) };
                }
                return Err(PgError::new(
                    FATAL,
                    format!(
                        "pre-existing shared memory block (key {}, ID {}) is still in use\n\
                         Terminate any old server processes associated with data directory \"{data_dir}\".",
                        next_shmem_seg_id as u64, shmid as u64,
                    ),
                ));
            }
            IpcMemoryState::Enoent => {
                // Some other process deleted since our last create. Try the
                // same ID again. (elog LOG omitted.)
            }
            IpcMemoryState::Foreign => {
                next_shmem_seg_id += 1;
            }
            IpcMemoryState::Unattached => {
                // The segment pertains to DataDir and every process that used
                // it has died/detached. Zap it (and any associated DSM), then
                // try again to create.
                // SAFETY: oldhdr is the attached PGShmemHeader (non-null for
                // the Unattached state).
                let dsm_control = unsafe { (*oldhdr).dsm_control };
                if dsm_control != 0 {
                    dsm_core_seams::dsm_cleanup_using_control_segment::call(
                        dsm_control as dsm_handle,
                    )?;
                }
                // SAFETY: shmctl IPC_RMID on the orphaned segment.
                if unsafe { libc::shmctl(shmid, libc::IPC_RMID, std::ptr::null_mut()) } < 0 {
                    next_shmem_seg_id += 1;
                }
            }
        }

        if !oldhdr.is_null() {
            // SAFETY: shmdt the segment we attached during analysis.
            if unsafe { libc::shmdt(oldhdr as *const libc::c_void) } < 0 {
                // elog(LOG) — best effort, ignore.
            }
        }
    };

    // Initialize new segment.
    let hdr = mem_address as *mut PGShmemHeader;
    // SAFETY: hdr points at the freshly-attached SysV segment, sized for at
    // least a PGShmemHeader.
    unsafe {
        (*hdr).creatorPID = getpid();
        (*hdr).magic = PGShmemMagic;
        (*hdr).dsm_control = 0;
        // Fill in the data directory ID info, too.
        (*hdr).device = statbuf.st_dev;
        (*hdr).inode = statbuf.st_ino;
        // Initialize space allocation status for segment.
        (*hdr).totalsize = size;
        (*hdr).freeoffset = maxalign(core::mem::size_of::<PGShmemHeader>());
    }
    let shim = hdr;

    // Save info for possible future use.
    let anonymous_shmem;
    {
        let mut state = SHMEM_STATE.lock().unwrap();
        state.used_shmem_seg_addr = mem_address;
        state.used_shmem_seg_id = next_shmem_seg_id as u64;
        anonymous_shmem = state.anonymous_shmem;
    }

    // If AnonymousShmem is NULL, return the SysV block; otherwise copy the
    // header into the anonymous block and return that.
    if anonymous_shmem.is_null() {
        return Ok((hdr, shim));
    }
    // SAFETY: copy the PGShmemHeader into the start of the anonymous block.
    unsafe {
        std::ptr::copy_nonoverlapping(
            hdr as *const u8,
            anonymous_shmem as *mut u8,
            core::mem::size_of::<PGShmemHeader>(),
        );
    }
    Ok((anonymous_shmem as *mut PGShmemHeader, shim))
}

/// `PGSharedMemoryDetach(void)` — detach from the shared memory segment, if
/// still attached. For subprocesses that inherited an attachment.
pub fn PGSharedMemoryDetach() {
    let mut state = SHMEM_STATE.lock().unwrap();
    if !state.used_shmem_seg_addr.is_null() {
        // SAFETY: shmdt the inherited attachment.
        if unsafe { libc::shmdt(state.used_shmem_seg_addr as *const libc::c_void) } < 0 {
            // elog(LOG) — best effort, ignore.
        }
        state.used_shmem_seg_addr = std::ptr::null_mut();
    }

    if !state.anonymous_shmem.is_null() {
        // SAFETY: munmap the inherited anonymous block.
        if unsafe { libc::munmap(state.anonymous_shmem, state.anonymous_shmem_size) } < 0 {
            // elog(LOG) — best effort, ignore.
        }
        state.anonymous_shmem = std::ptr::null_mut();
    }
}

// ---- helpers ----

/// `MAXALIGN(len)`.
fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

fn errno() -> libc::c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

fn os_error_string(e: libc::c_int) -> String {
    std::io::Error::from_raw_os_error(e).to_string()
}

fn getpid() -> libc::pid_t {
    // SAFETY: getpid is always safe.
    unsafe { libc::getpid() }
}

/// `stat(path, &statbuf)` returning the `stat` struct or the errno.
fn stat(path: &str) -> Result<libc::stat, libc::c_int> {
    let c_path = match std::ffi::CString::new(path) {
        Ok(c) => c,
        Err(_) => return Err(libc::ENOENT),
    };
    // SAFETY: stat into a zeroed struct.
    let mut statbuf: libc::stat = unsafe { core::mem::zeroed() };
    let rc = unsafe { libc::stat(c_path.as_ptr(), &mut statbuf as *mut libc::stat) };
    if rc < 0 {
        Err(errno())
    } else {
        Ok(statbuf)
    }
}

} // mod native

// ===========================================================================
// sysv_shmem.c-owned GUC variable storage (the `config_*` entries in
// guc_tables.c point their `&variable` at these sysv_shmem.c symbols). Each is
// read by C straight from its GUC slot (`*conf->variable`). Boot defaults
// mirror the `boot_val` column of each guc_tables.c entry.
// ===========================================================================
std::thread_local! {
    /// `int shared_memory_type = DEFAULT_SHARED_MEMORY_TYPE` (sysv_shmem.c).
    /// `shared_memory_type` enum GUC; boot_val SHMEM_TYPE_MMAP.
    static SHARED_MEMORY_TYPE: std::cell::Cell<i32> =
        const { std::cell::Cell::new(SHMEM_TYPE_MMAP) };
}

/// `check_huge_page_size(newval, extra, source)` (sysv_shmem.c:578) — GUC
/// check_hook for `huge_page_size`. On platforms without `MAP_HUGE_*`
/// (everything but recent Linux, incl. this darwin build) a non-zero value is
/// rejected.
fn check_huge_page_size(
    newval: &mut i32,
    _extra: &mut Option<::guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    // !(MAP_HUGE_MASK && MAP_HUGE_SHIFT): not Linux, so reject non-zero.
    if *newval != 0 {
        guc_seams::guc_check_errdetail::call(
            "\"huge_page_size\" must be 0 on this platform.".to_string(),
        );
        return Ok(false);
    }
    Ok(true)
}

/// Install the inward `PGSharedMemory*` / `GetHugePageSize` seams consumed by
/// ipci/postmaster/miscinit/shmem.
pub fn init_seams() {
    use ::guc_tables::{hooks, vars, GucVarAccessors};

    sysv_shmem_seams::pg_shared_memory_detach::set(PGSharedMemoryDetach);
    sysv_shmem_seams::pg_shared_memory_is_in_use::set(PGSharedMemoryIsInUse);
    sysv_shmem_seams::get_huge_page_size::set(GetHugePageSize);
    sysv_shmem_seams::pg_shared_memory_create::set(PGSharedMemoryCreate);

    // `shared_memory_type` is the one sysv_shmem.c-owned GUC variable whose
    // backing store is not provided by guc-tables itself (the preset/computed
    // `huge_pages` / `huge_pages_status` / `huge_page_size` are installed there).
    vars::shared_memory_type.install(GucVarAccessors {
        get: || SHARED_MEMORY_TYPE.with(std::cell::Cell::get),
        set: |v| SHARED_MEMORY_TYPE.with(|c| c.set(v)),
    });
    // `check_huge_page_size` (sysv_shmem.c:578) is the C check_hook for the
    // `huge_page_size` GUC.
    hooks::check_huge_page_size.install(check_huge_page_size);
}
