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
//! What is NOT ported, because it has no thread-model counterpart: segment
//! creation (`InternalIpcMemoryCreate`, `PGSharedMemoryCreate`), the
//! anonymous-mmap main region, `PGSharedMemoryReAttach`/`Detach`, huge-page
//! plumbing (that lives in bufmgr::hugepages), and the key-space recycling
//! walk. Of `PGSharedMemoryAttach` only the probe shape exists here — attachAt
//! stays NULL, exactly as `PGSharedMemoryIsInUse` calls it in C.

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
    // SAFETY: `addr` is the mapping PGSharedMemoryAttach just returned from
    // shmat, and no reference into it outlives this call.
    if unsafe { libc::shmdt(addr) } < 0 {
        let errnum = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        elog::ereport(types_error::LOG)
            .with_saved_errno(errnum)
            .errmsg_internal(format!("shmdt({addr:p}) failed: %m"))
            .finish(types_error::ErrorLocation::new(
                "sysv_shmem.c",
                324,
                "PGSharedMemoryIsInUse",
            ))?;
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

pub fn init_seams() {
    shmem_seams::pg_shared_memory_is_in_use::set(PGSharedMemoryIsInUse);
}

#[cfg(test)]
mod tests;
