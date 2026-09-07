//! The *probe* half of C `src/backend/port/sysv_shmem.c`.
//!
//! pgrust's "shared memory" is process memory shared between backend threads
//! (crates/backend/storage/ipc/shmem), so it never creates a System V segment
//! and never writes line 7 of `postmaster.pid`. It must still be able to READ
//! a foreign one. A data directory last held by C PostgreSQL leaves behind a
//! `postmaster.pid` whose line 7 carries "<key> <id>", and CreateLockFile's
//! orphaned-segment interlock (miscinit.c) probes that id to tell a merely
//! stale pid file from one whose postmaster died leaving live backends still
//! attached to the segment. That is the migrate-from-C first-contact path,
//! and it is the only reason this file exists here.
//!
//! `PGSharedMemoryCreate` exists here as its two startup interlocks only
//! (audit rows a186-candidate-fp-port-sysv_shmem-{79ac1c53,53018cea,78cb3328}):
//! the huge_pages/shared_memory_type refusals (sysv_shmem.c:723-733) and the
//! key-space walk from DataDir's inode (sysv_shmem.c:764-855) that refuses to
//! start while a segment of this data directory still has processes attached
//! and recycles one nobody is attached to. What it does NOT do, because the
//! thread model has no counterpart: mint a segment of its own
//! (`InternalIpcMemoryCreate`), map the anonymous main region, or write line 7
//! of `postmaster.pid`; `PGSharedMemoryReAttach`/`Detach` stay unported. Of
//! `PGSharedMemoryAttach` only the probe shape exists here — attachAt stays
//! NULL, exactly as `PGSharedMemoryIsInUse` calls it in C.

#![allow(non_snake_case)]

use types_error::PgResult;

/// C `IpcMemoryState` (sysv_shmem.c): how a given segment id relates to this
/// process. Variant order and meaning are C's.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IpcMemoryState {
    /// Unexpected failure to analyze the id.
    AnalysisFailure,
    /// Pertinent to DataDir, has attached PIDs.
    Attached,
    /// No segment of that id.
    Enoent,
    /// Exists, but not pertinent to DataDir.
    Foreign,
    /// Pertinent to DataDir, no attached PIDs.
    Unattached,
}

/// C `PGSharedMemoryIsInUse(id1, id2)`.
///
/// `id1` (the SysV key) is unread, as in C: only `id2`, the segment id, is
/// probed. Returns true when the segment belongs to our data directory and
/// still has processes attached, or when the analysis itself failed — C is
/// deliberately conservative there, because refusing to start is recoverable
/// and corrupting a live cluster's data directory is not.
pub fn PGSharedMemoryIsInUse(_id1: u64, id2: u64) -> PgResult<bool> {
    let (state, addr) = PGSharedMemoryAttach(id2 as libc::c_int);
    if !addr.is_null() {
        detach(addr)?;
    }
    Ok(match state {
        IpcMemoryState::Enoent | IpcMemoryState::Foreign | IpcMemoryState::Unattached => false,
        IpcMemoryState::AnalysisFailure | IpcMemoryState::Attached => true,
    })
}

#[cfg(not(target_family = "wasm"))]
fn detach(addr: *mut libc::c_void) -> PgResult<()> {
    detach_at(addr, 324, "PGSharedMemoryIsInUse")
}

/// C's `if (shmdt(addr) < 0) elog(LOG, "shmdt(%p) failed: %m", addr)`, at the
/// caller's own line (PGSharedMemoryIsInUse:324, PGSharedMemoryCreate:846).
#[cfg(not(target_family = "wasm"))]
fn detach_at(addr: *mut libc::c_void, line: i32, func: &'static str) -> PgResult<()> {
    // SAFETY: `addr` is the mapping PGSharedMemoryAttach just returned from
    // shmat, and no reference into it outlives this call.
    if unsafe { libc::shmdt(addr) } < 0 {
        let errnum = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        elog::ereport(types_error::LOG)
            .with_saved_errno(errnum)
            .errmsg_internal(format!("shmdt({addr:p}) failed: %m"))
            .finish(types_error::ErrorLocation::new("sysv_shmem.c", line, func))?;
    }
    Ok(())
}

/// C `PGSharedMemoryAttach(shmId, NULL, &memAddress)`: the errno decision tree
/// over shmctl(IPC_STAT) + shmat, then the DataDir device/inode identity check.
/// Returns the state and, when we attached, the mapping the caller must detach
/// (C's `*addr` out-parameter).
#[cfg(not(target_family = "wasm"))]
pub fn PGSharedMemoryAttach(shmId: libc::c_int) -> (IpcMemoryState, *mut libc::c_void) {
    use types_storage::{PGShmemHeader, PGShmemMagic};

    let null = std::ptr::null_mut();

    // First, try to stat the shm segment ID, to see if it exists at all.
    let mut shm_stat: libc::shmid_ds = unsafe { std::mem::zeroed() };
    // SAFETY: IPC_STAT only writes the caller-owned shmid_ds.
    if unsafe { libc::shmctl(shmId, libc::IPC_STAT, &mut shm_stat) } < 0 {
        return (errno_state(), null);
    }

    // Try to attach and see if it matches our data directory. This avoids any
    // risk of duplicate-shmem-key conflicts on machines running several
    // postmasters under the same userid.
    //
    // C stats the global DataDir; a data directory we cannot stat is C's
    // "can't stat; be conservative" arm. DataDir is always set by the time
    // CreateDataDirLockFile runs, so None lands in the same conservative arm.
    let Some(datadir) = init_small::globals::DataDir() else {
        return (IpcMemoryState::AnalysisFailure, null);
    };
    let mut datadir_info = vfs::FileInfo::zeroed();
    if fd::sync::pg_stat(datadir, &mut datadir_info) != 0 {
        return (IpcMemoryState::AnalysisFailure, null);
    }

    // Reading a segment created by another program means reading memory whose
    // size we do not control. C reads the header unconditionally and would
    // fault on a segment shorter than one; a too-small segment cannot be a
    // Postgres segment (C always creates at least sizeof(PGShmemHeader)), so
    // the answer C's magic/device/inode test would give for it is FOREIGN.
    if (shm_stat.shm_segsz as u64) < std::mem::size_of::<PGShmemHeader>() as u64 {
        return (IpcMemoryState::Foreign, null);
    }

    // PG_SHMAT_FLAGS is 0 on every platform pgrust builds for (portability/mem.h
    // only sets SHM_SHARE_MMU on Solaris).
    // SAFETY: attaching read/write at a kernel-chosen address; the mapping is
    // only read through, below, and is detached by the caller.
    let addr = unsafe { libc::shmat(shmId, std::ptr::null(), 0) };
    if addr as isize == -1 {
        // Attachment failed. Same cases as the shmctl above; in particular the
        // owning postmaster could have terminated and removed the segment
        // between the shmctl and the shmat.
        return (errno_state(), null);
    }

    // SAFETY: `addr` is a live mapping of at least size_of::<PGShmemHeader>()
    // bytes (checked against shm_segsz above), shmat returns page-aligned
    // addresses, and PGShmemHeader is repr(C) and plain-old-data. The contents
    // are untrusted, which is exactly what the identity test below is for.
    let hdr = unsafe { std::ptr::read(addr as *const PGShmemHeader) };

    if hdr.magic != PGShmemMagic
        || hdr.device != datadir_info.dev as libc::dev_t
        || hdr.inode != datadir_info.ino as libc::ino_t
    {
        // Either not a Postgres segment, or not one for my data directory.
        return (IpcMemoryState::Foreign, addr);
    }

    // It does match our data directory, so now test whether any processes are
    // still attached to it. (We are, now, but the shm_nattch result is from
    // before we attached to it.)
    let state = if shm_stat.shm_nattch == 0 {
        IpcMemoryState::Unattached
    } else {
        IpcMemoryState::Attached
    };
    (state, addr)
}

/// The errno arms shared by the shmctl and shmat failures in C's
/// PGSharedMemoryAttach.
#[cfg(not(target_family = "wasm"))]
fn errno_state() -> IpcMemoryState {
    let errnum = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);

    // EINVAL actually has multiple possible causes documented in the shmctl
    // man page, but we assume it must mean the segment no longer exists.
    if errnum == libc::EINVAL {
        return IpcMemoryState::Enoent;
    }
    // EACCES implies we have no read permission, which means it is not a
    // Postgres segment (or at least, not one relevant to our data directory).
    if errnum == libc::EACCES {
        return IpcMemoryState::Foreign;
    }
    // HAVE_LINUX_EIDRM_BUG (port/linux.h): all known Linux kernels sometimes
    // return EIDRM where EINVAL is correct, and Linux has no state that would
    // justify EIDRM, so treat it as EINVAL there and only there.
    #[cfg(target_os = "linux")]
    if errnum == libc::EIDRM {
        return IpcMemoryState::Enoent;
    }
    // Otherwise assume the segment is in use. The only likely case is
    // (non-Linux, spec-compliant) EIDRM, which implies the segment has been
    // IPC_RMID'd but processes are still attached to it.
    IpcMemoryState::AnalysisFailure
}

// wasm32: WASI p1 has no System V IPC (no shmctl/shmat) and no processes other
// than this instance, so no foreign segment can exist and none can be attached
// — C's SHMSTATE_ENOENT arm, reached without a syscall.
#[cfg(target_family = "wasm")]
pub fn PGSharedMemoryAttach(_shmId: libc::c_int) -> (IpcMemoryState, *mut libc::c_void) {
    (IpcMemoryState::Enoent, std::ptr::null_mut())
}

#[cfg(target_family = "wasm")]
fn detach(_addr: *mut libc::c_void) -> PgResult<()> {
    Ok(())
}

/// C `PGSharedMemoryCreate`'s `#if !defined(MAP_HUGETLB)` (sysv_shmem.c:723):
/// the flag exists on Linux (and Android's libc) only.
pub const MAP_HUGETLB_AVAILABLE: bool = cfg!(any(target_os = "linux", target_os = "android"));

/// The huge_pages refusals of C `PGSharedMemoryCreate` (sysv_shmem.c:722-733),
/// in C's order: without MAP_HUGETLB, `huge_pages = on` is "not supported on
/// this platform"; with it, `on` under a non-mmap `shared_memory_type` is "not
/// supported with the current "shared_memory_type" setting". Where C would
/// then go on to mmap the main region with MAP_HUGETLB (Linux, mmap), pgrust
/// has no region to map: thread-shared state lives on the process heap, so
/// `on` cannot be honoured and is refused with the platform message — the
/// typed refusal of an unported feature, never the silent `huge_pages_status
/// = off` boot the audit found. `try` and `off` pass everywhere, as in C.
pub fn huge_pages_startup_gate(
    huge_pages: i32,
    shared_memory_type: i32,
    map_hugetlb: bool,
) -> Result<(), &'static str> {
    use guc_tables::consts::{HUGE_PAGES_ON, SHMEM_TYPE_MMAP};

    if huge_pages != HUGE_PAGES_ON {
        return Ok(());
    }
    if !map_hugetlb {
        return Err("huge pages not supported on this platform");
    }
    if shared_memory_type != SHMEM_TYPE_MMAP {
        return Err("huge pages not supported with the current \"shared_memory_type\" setting");
    }
    Err("huge pages not supported on this platform")
}

/// C `PGSharedMemoryCreate` (sysv_shmem.c:702-870) as far as the thread model
/// reaches: stat DataDir, the huge_pages refusals, then the key-space walk.
/// `dsm_cleanup` is `dsm_cleanup_using_control_segment` (dsm.c), run on the
/// control handle of a recycled segment exactly where C runs it; ipci passes
/// the real one, tests pass a recorder.
///
/// The walk is C's loop minus the segment C ends it by creating: C tries
/// `InternalIpcMemoryCreate(key)` first and only probes a key it could not
/// create at, so the key it stops at is the first one with no segment behind
/// it. Here a key with no segment (shmget → ENOENT) ends the walk the same
/// way; every other key is probed with the same `PGSharedMemoryAttach` and
/// acted on with C's arms (ATTACHED/ANALYSIS_FAILURE → FATAL, ENOENT → retry,
/// FOREIGN → next key, UNATTACHED → dsm cleanup + IPC_RMID).
pub fn PGSharedMemoryCreate(dsm_cleanup: fn(u32) -> PgResult<()>) -> PgResult<()> {
    let datadir = init_small::globals::DataDir().unwrap_or("");
    let mut statbuf = vfs::FileInfo::zeroed();
    if fd::sync::pg_stat(datadir, &mut statbuf) != 0 {
        let errnum = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        return elog::ereport(types_error::FATAL)
            .with_saved_errno(errnum)
            .errcode_for_file_access()
            .errmsg(format!("could not stat data directory \"{datadir}\": %m"))
            .finish(types_error::ErrorLocation::new("sysv_shmem.c", 716, "PGSharedMemoryCreate"));
    }

    let huge_pages = guc_tables::vars::huge_pages.read();
    let shared_memory_type = guc_tables::vars::shared_memory_type.read();
    if let Err(msg) = huge_pages_startup_gate(huge_pages, shared_memory_type, MAP_HUGETLB_AVAILABLE) {
        return elog::ereport(types_error::ERROR)
            .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(msg)
            .finish(types_error::ErrorLocation::new("sysv_shmem.c", 723, "PGSharedMemoryCreate"));
    }

    probe_key_space(datadir, statbuf.ino, dsm_cleanup)
}

/// The key-space walk of C `PGSharedMemoryCreate` (sysv_shmem.c:764-855),
/// seeded with DataDir's inode. See [`PGSharedMemoryCreate`].
#[cfg(not(target_family = "wasm"))]
pub fn probe_key_space(datadir: &str, ino: u64, dsm_cleanup: fn(u32) -> PgResult<()>) -> PgResult<()> {
    use types_storage::PGShmemHeader;

    // C: `IpcMemoryKey NextShmemSegID = statbuf.st_ino` — key_t is int, the
    // inode is truncated to it exactly as C's assignment does.
    let mut next_key = ino as libc::key_t;
    loop {
        // SAFETY: shmget with no IPC_CREAT only looks a key up.
        let shmid = unsafe { libc::shmget(next_key, std::mem::size_of::<PGShmemHeader>(), 0) };
        let (state, oldaddr) = if shmid < 0 {
            let errnum = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            match errnum {
                // No segment behind this key: C's InternalIpcMemoryCreate
                // succeeds here and its loop ends.
                libc::ENOENT => break,
                // C: "shmget() failure is typically EACCES, hence SHMSTATE_FOREIGN".
                libc::EACCES => (IpcMemoryState::Foreign, std::ptr::null_mut()),
                // Any other failure (ENOSYS: no System V IPC at all) means no
                // Postgres segment can exist for this key space; C would fail
                // creating its own, pgrust needs none.
                _ => break,
            }
        } else {
            PGSharedMemoryAttach(shmid)
        };

        let key = next_key as i64 as u64;
        match state {
            IpcMemoryState::AnalysisFailure | IpcMemoryState::Attached => {
                if !oldaddr.is_null() {
                    detach_at(oldaddr, 846, "PGSharedMemoryCreate")?;
                }
                return elog::ereport(types_error::FATAL)
                    .errcode(types_error::ERRCODE_LOCK_FILE_EXISTS)
                    .errmsg(format!(
                        "pre-existing shared memory block (key {key}, ID {}) is still in use",
                        shmid as i64 as u64
                    ))
                    .errhint(format!(
                        "Terminate any old server processes associated with data directory \"{datadir}\"."
                    ))
                    .finish(types_error::ErrorLocation::new("sysv_shmem.c", 798, "PGSharedMemoryCreate"));
            }
            IpcMemoryState::Enoent => {
                // To our surprise, some other process deleted it since our
                // shmget. Try that same key again.
                elog::elog(
                    types_error::LOG,
                    format!(
                        "shared memory block (key {key}, ID {}) deleted during startup",
                        shmid as i64 as u64
                    ),
                )?;
            }
            IpcMemoryState::Foreign => next_key += 1,
            IpcMemoryState::Unattached => {
                // The segment pertains to DataDir, and every process that had
                // used it has died or detached. Zap it, if possible, and any
                // associated dynamic shared memory segments, as well. If that
                // fails, assume the segment belongs to someone else after all,
                // and try the next candidate.
                // SAFETY: UNATTACHED is only returned with the mapping still
                // attached and its header already validated by the probe.
                let dsm_control = unsafe { (*(oldaddr as *const PGShmemHeader)).dsm_control };
                if dsm_control != 0 {
                    dsm_cleanup(dsm_control)?;
                }
                // SAFETY: our own data directory's abandoned segment.
                let removed = unsafe { libc::shmctl(shmid, libc::IPC_RMID, std::ptr::null_mut()) } == 0;
                detach_at(oldaddr, 846, "PGSharedMemoryCreate")?;
                if !removed {
                    next_key += 1;
                    continue;
                }
                // The key is free now: C's next InternalIpcMemoryCreate at it
                // succeeds and ends the loop.
                break;
            }
        }
        if !oldaddr.is_null() && state != IpcMemoryState::Unattached {
            detach_at(oldaddr, 846, "PGSharedMemoryCreate")?;
        }
    }
    Ok(())
}

// wasm32: no System V IPC and no other process, so the key space is empty —
// the walk ends at its first key without a syscall.
#[cfg(target_family = "wasm")]
pub fn probe_key_space(_datadir: &str, _ino: u64, _dsm_cleanup: fn(u32) -> PgResult<()>) -> PgResult<()> {
    Ok(())
}

/// What C `GetHugePageSize` (sysv_shmem.c:479-571) hands back through its two
/// out-parameters: the huge page size in bytes (0 = huge pages unsupported)
/// and the mmap flags that request a page of that size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HugePageSize {
    pub hugepagesize: usize,
    pub mmap_flags: i32,
}

/// Linux `MAP_HUGETLB`; 0 where the flag does not exist (C never reaches the
/// mmap_flags assignment there: the whole body is `#ifdef MAP_HUGETLB`).
#[cfg(any(target_os = "linux", target_os = "android"))]
const MAP_HUGETLB: i32 = libc::MAP_HUGETLB;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const MAP_HUGETLB: i32 = 0;
/// Linux uapi `MAP_HUGE_SHIFT`/`MAP_HUGE_MASK` (asm-generic/hugetlb_encode.h;
/// the same on every architecture); only consulted where
/// [`HUGE_PAGE_SIZE_SELECTABLE`] says they exist.
const MAP_HUGE_SHIFT: i32 = 26;
const MAP_HUGE_MASK: i32 = 0x3f;

/// The `/proc/meminfo` scan of C `GetHugePageSize` (sysv_shmem.c:495-517):
/// the first line `sscanf(buf, "Hugepagesize: %u %c", &sz, &ch)` converts
/// both fields of, with `ch == 'k'`, gives `sz * 1024`; a line with another
/// unit is skipped ("we could accept other units besides kB, if needed") and
/// a file with no such line gives 0, the "unknown" the fallback arm reads.
pub fn meminfo_default_hugepagesize(meminfo: &str) -> usize {
    for line in meminfo.lines() {
        let Some(rest) = line.strip_prefix("Hugepagesize:") else { continue };
        // %u: skip whitespace, an optional sign, then a decimal run.
        let rest = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        let (neg, rest) = match rest.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, rest.strip_prefix('+').unwrap_or(rest)),
        };
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if digits == 0 {
            continue;
        }
        let Ok(sz) = rest[..digits].parse::<u32>() else { continue };
        // " %c": skip whitespace, then any one character (none = only one
        // conversion, the line does not match).
        let rest = rest[digits..].trim_start_matches(|c: char| c.is_ascii_whitespace());
        let Some(ch) = rest.chars().next() else { continue };
        if ch == 'k' {
            // C's `sz * (Size) 1024` on the unsigned conversion: a negative
            // "-N" is %u's two's-complement wrap.
            let sz = if neg { sz.wrapping_neg() } else { sz };
            return sz as usize * 1024;
        }
    }
    0
}

/// The platform-parameterised body of C `GetHugePageSize`
/// (sysv_shmem.c:520-562): `huge_page_size_kb` is the GUC (kB),
/// `default_hugepagesize` what `/proc/meminfo` reported (0 = unknown);
/// `map_hugetlb` is whether `MAP_HUGETLB` exists at all and `huge_shift`
/// whether `MAP_HUGE_MASK`/`MAP_HUGE_SHIFT` do.
pub fn resolve_huge_page_size(
    huge_page_size_kb: i32,
    default_hugepagesize: usize,
    map_hugetlb: bool,
    huge_shift: bool,
) -> HugePageSize {
    if !map_hugetlb {
        return HugePageSize { hugepagesize: 0, mmap_flags: 0 };
    }
    let hugepagesize_local = if huge_page_size_kb != 0 {
        // If huge page size is requested explicitly, use that.
        huge_page_size_kb as usize * 1024
    } else if default_hugepagesize != 0 {
        // Otherwise use the system default, if we have it.
        default_hugepagesize
    } else {
        // Neither known: assume 2MB (works when the real size is smaller).
        2 * 1024 * 1024
    };
    let mut mmap_flags_local = MAP_HUGETLB;
    // On recent enough Linux, also include the explicit page size, if
    // necessary.
    if huge_shift && hugepagesize_local != default_hugepagesize {
        let shift = pg_bitutils::pg_ceil_log2_64(hugepagesize_local as u64) as i32;
        mmap_flags_local |= (shift & MAP_HUGE_MASK) << MAP_HUGE_SHIFT;
    }
    HugePageSize { hugepagesize: hugepagesize_local, mmap_flags: mmap_flags_local }
}

/// C `GetHugePageSize(&hugepagesize, &mmap_flags)` (sysv_shmem.c:479-571).
/// On Linux the system default comes from `/proc/meminfo` through
/// `AllocateFile`, read failures ignored as in C; the `huge_page_size` GUC
/// overrides it; 2MB is the fallback. Without `MAP_HUGETLB` both results
/// are 0. `InitializeShmemGUCs` (ipci.c:377) sizes
/// shared_memory_size_in_huge_pages from the result.
pub fn GetHugePageSize() -> PgResult<HugePageSize> {
    let mut default_hugepagesize = 0;
    if MAP_HUGETLB_AVAILABLE && cfg!(target_os = "linux") {
        let fp = fd::AllocateFile("/proc/meminfo", "r")?;
        if fp >= 0 {
            let contents = fd::with_allocated_stdio(fp, |f| {
                use std::io::Read;
                let mut buf = Vec::new();
                f.read_to_end(&mut buf).map(|_| buf)
            });
            if let Some(Ok(contents)) = contents {
                default_hugepagesize =
                    meminfo_default_hugepagesize(&String::from_utf8_lossy(&contents));
            }
            fd::FreeFile(fp)?;
        }
    }
    let huge_page_size = guc_tables::vars::huge_page_size.read();
    Ok(resolve_huge_page_size(
        huge_page_size,
        default_hugepagesize,
        MAP_HUGETLB_AVAILABLE,
        HUGE_PAGE_SIZE_SELECTABLE,
    ))
}

/// C `check_huge_page_size`'s platform gate (sysv_shmem.c:580): the size is
/// honoured only where `MAP_HUGE_MASK` and `MAP_HUGE_SHIFT` exist (Linux);
/// elsewhere any non-zero value is rejected.
pub const HUGE_PAGE_SIZE_SELECTABLE: bool = cfg!(any(target_os = "linux", target_os = "android"));

/// The platform-parameterised body of `check_huge_page_size`
/// (sysv_shmem.c:578-591): returns C's verdict and, when rejecting, the
/// GUC_check_errdetail text.
pub fn check_huge_page_size_value(newval: i32, selectable: bool) -> Result<(), &'static str> {
    if !selectable && newval != 0 {
        return Err("\"huge_page_size\" must be 0 on this platform.");
    }
    Ok(())
}

// GUC check_hook for huge_page_size (sysv_shmem.c:578).
fn check_huge_page_size(
    newval: &mut i32,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: types_guc::GucSource,
) -> PgResult<bool> {
    match check_huge_page_size_value(*newval, HUGE_PAGE_SIZE_SELECTABLE) {
        Ok(()) => Ok(true),
        Err(detail) => {
            if guc_seams::guc_check_errdetail::is_installed() {
                guc_seams::guc_check_errdetail::call(detail.to_string());
            }
            Ok(false)
        }
    }
}

pub fn init_seams() {
    shmem_seams::pg_shared_memory_is_in_use::set(PGSharedMemoryIsInUse);
    guc_tables::hooks::check_huge_page_size.install(check_huge_page_size);
}

#[cfg(test)]
mod tests;
