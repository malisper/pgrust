#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! Magnetic-disk storage manager — an idiomatic, safe-Rust port of
//! `src/backend/storage/smgr/md.c`.
//!
//! md.c is the one concrete storage manager behind smgr.c's `f_smgr` dispatch
//! (`smgr_which == SMGR_MD`). It breaks each relation fork into `RELSEG_SIZE`
//! (1 GB) segment files, keeps a per-fork array of open segment VFDs
//! ([`MdRelnState`]'s `md_seg_fds` / `md_num_open_segs`), and performs the
//! actual block I/O through the fd.c Virtual File Descriptor pool.
//!
//! Every md.c function is implemented here 1:1 with the C logic (branches,
//! order, error SQLSTATE+messages, segment math, the `EXTENSION_*` behavior
//! flags). The VFD layer (`backend-storage-file-fd`) and `common/relpath.c` are
//! DIRECT deps (no cycle); sync.c's request queue, tablespace.c, the recovery /
//! GUC globals, and the AIO engine cross seams (sync would cycle; the rest are
//! peers/unported).
//!
//! ## Per-backend state
//!
//! C keeps the per-fork open-segment fd arrays (`md_seg_fds` /
//! `md_num_open_segs`) inside `SMgrRelationData`. The boundary
//! [`SMgrRelationData`] in `types-storage` omits them; this crate carries them
//! in the process-local [`MdRelnState`], threaded alongside, in a
//! `thread_local!` backend cache (md's `SMgrRelationHash` is backend-local in C
//! too — these are kernel fds held by one backend, never shared memory).

#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::cell::RefCell;
use std::collections::HashMap;

use backend_common_relpath::relpathbackend as relpath_backend_fn;
use backend_storage_file_fd as fd;
use backend_storage_file_fd::vfd_core;
use backend_utils_error::{ereport, PgError, PgResult};
use types_error::{DEBUG1, ERROR, FATAL, WARNING};
use types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, MaxBlockNumber, ProcNumber, INVALID_PROC_NUMBER,
    MAX_FORKNUM,
};
use types_core::{Oid, BLCKSZ};
use types_error::ErrorLevel;
use types_error::{
    ERRCODE_DATA_CORRUPTED, ERRCODE_DISK_FULL, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_pgstat::wait_event::{
    WAIT_EVENT_DATA_FILE_EXTEND, WAIT_EVENT_DATA_FILE_FLUSH, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC,
    WAIT_EVENT_DATA_FILE_PREFETCH, WAIT_EVENT_DATA_FILE_READ, WAIT_EVENT_DATA_FILE_SYNC,
    WAIT_EVENT_DATA_FILE_TRUNCATE, WAIT_EVENT_DATA_FILE_WRITE,
};
use types_storage::file::{File, FILE_EXTEND_METHOD_WRITE_ZEROS, IO_DIRECT_DATA};
use types_storage::smgr::{
    MdRelnState, MdfdVec, SMgrRelationData, EXTENSION_CREATE, EXTENSION_CREATE_RECOVERY,
    EXTENSION_DONT_OPEN, EXTENSION_FAIL, EXTENSION_RETURN_NULL, RELSEG_SIZE,
};
use types_storage::sync::{FileTag, FileTagOpResult, SyncRequestHandler, SyncRequestType};
use types_storage::{RelFileLocator, RelFileLocatorBackend};

use backend_access_transam_xlogrecovery_seams::in_recovery as in_recovery_seam;
use backend_catalog_binary_upgrade_seams::is_binary_upgrade as is_binary_upgrade_seam;
use backend_commands_tablespace_seams::tablespace_create_dbspace as tablespace_create_dbspace_seam;
use backend_storage_smgr_md_seams as md_seam;
use backend_storage_sync_seams::register_sync_request as register_sync_request_seam;

// ===========================================================================
// md.c constants
// ===========================================================================

/// `BLCKSZ` as an `i64` for the `(off_t) BLCKSZ * ...` seek arithmetic.
const BLCKSZ_I64: i64 = BLCKSZ as i64;

/// `PG_IOV_MAX` — md clamps each segment's block run to `lengthof(iov)`.
const PG_IOV_MAX: usize = types_storage::smgr::PG_IOV_MAX;

// `errno` constants md.c branches on (`storage/fd.h` FILE_POSSIBLY_DELETED).
const ENOENT: i32 = libc::ENOENT;
const ENOSPC: i32 = libc::ENOSPC;

// ===========================================================================
// errno access (the fd layer sets the real libc errno; md.c reads it after
// each fallible syscall via the `ret < 0` + `errno` idiom).
// ===========================================================================

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

/// The saved kernel `errno` from the last fd-layer syscall.
fn last_errno() -> i32 {
    unsafe { *errno_location() }
}

/// Restore `errno` (md.c's `errno = save_errno;` after intervening work).
fn set_errno(value: i32) {
    unsafe {
        *errno_location() = value;
    }
}

// ===========================================================================
// helpers: FILE_POSSIBLY_DELETED, data_sync_elevel, fork indexing, GUCs
// ===========================================================================

/// `FILE_POSSIBLY_DELETED(err)` (storage/fd.h): true when an open failure errno
/// means the file might have been deleted out from under us (`ENOENT`).
#[inline]
fn file_possibly_deleted(err: i32) -> bool {
    err == ENOENT
}

/// `data_sync_elevel(elevel)` (fd.c): PostgreSQL escalates data fsync failures
/// to PANIC when `data_sync_retry` is off (the default); the VFD/runtime layer
/// owns that GUC, so md.c's literal `ERROR` argument is kept here — what the
/// in-crate control flow needs is that the call returns `Err` at >= ERROR.
#[inline]
fn data_sync_elevel(elevel: ErrorLevel) -> ErrorLevel {
    elevel
}

/// Index a per-fork array by `ForkNumber`.
#[inline]
fn fork_idx(forknum: ForkNumber) -> usize {
    forknum as usize
}

/// `SmgrIsTemp(reln)` (smgr.h): true for a backend-local temporary relation.
#[inline]
fn smgr_is_temp(reln: &SMgrRelationData) -> bool {
    is_temp(reln.smgr_rlocator)
}

/// `RelFileLocatorBackendIsTemp(rlocator)` (relfilelocator.h).
#[inline]
fn is_temp(rlocator: RelFileLocatorBackend) -> bool {
    rlocator.backend != INVALID_PROC_NUMBER
}

/// `InRecovery` — true while this process is replaying WAL.
#[inline]
fn in_recovery() -> bool {
    in_recovery_seam::call()
}

/// `io_direct_flags & IO_DIRECT_DATA` (fd.c GUC; fd is a direct dep).
#[inline]
fn io_direct_data() -> bool {
    (vfd_core::io_direct_flags() & IO_DIRECT_DATA) != 0
}

/// `zero_damaged_pages` GUC (defined in guc_tables; a direct dep).
#[inline]
fn zero_damaged_pages() -> bool {
    backend_utils_misc_guc_tables::vars::zero_damaged_pages.read()
}

// ===========================================================================
// relpath helpers (common/relpath.c macros).
// ===========================================================================

/// `relpath(rlocator, forknum)` ==
/// `relpathbackend((rlocator).locator, (rlocator).backend, forknum)`.
#[inline]
fn relpath(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> String {
    relpath_backend_fn(rlocator.locator, rlocator.backend, forknum)
}

/// `relpathperm(rlocator, forknum)` ==
/// `relpathbackend(rlocator, INVALID_PROC_NUMBER, forknum)`.
#[inline]
fn relpathperm(rlocator: RelFileLocator, forknum: ForkNumber) -> String {
    relpath_backend_fn(rlocator, INVALID_PROC_NUMBER, forknum)
}

// ===========================================================================
// error helpers
// ===========================================================================

fn oom_error(what: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg_internal(format!("out of memory allocating {what}"))
        .into_error()
}

// ===========================================================================
// Backend-local SMgrRelation cache (C's `SMgrRelationHash` dynahash + the
// per-reln md segment-fd state, both backend-private).
// ===========================================================================

/// One cached relation: the boundary smgr view plus md's private open-segment
/// fd state, plus the pin count (C's `unpinned_relns` membership when 0).
pub struct MdCacheEntry {
    pub data: SMgrRelationData,
    pub md: MdRelnState,
    /// `pincount` — when 0 the reln is eligible for `smgrdestroyall`.
    pub pincount: i32,
}

thread_local! {
    /// C's `SMgrRelationHash` (backend-local dynahash). `InRecovery` is a
    /// separate global consulted via the xlogrecovery seam.
    static RELNS: RefCell<HashMap<RelFileLocatorBackend, MdCacheEntry>> =
        RefCell::new(HashMap::new());
}

/// `smgropen(rlocator, backend)` — look up or create the cache entry, running
/// `mdopen` on a fresh one. Returns the boundary `SMgrRelationData` snapshot.
/// (smgr.c owns the public `smgropen`; this is the md-side cache primitive the
/// smgr crate and md's own sync callback share.)
pub fn cache_open(rlocator: RelFileLocator, backend: ProcNumber) -> PgResult<SMgrRelationData> {
    let key = RelFileLocatorBackend {
        locator: rlocator,
        backend,
    };
    let existing = RELNS.with(|r| r.borrow().get(&key).map(|e| e.data));
    if let Some(data) = existing {
        return Ok(data);
    }
    let mut entry = MdCacheEntry {
        data: SMgrRelationData::new(key),
        md: MdRelnState::default(),
        pincount: 0,
    };
    // smgrsw[reln->smgr_which].smgr_open(reln) == mdopen.
    mdopen(&mut entry.md)?;
    let data = entry.data;
    RELNS.with(|r| {
        let mut map = r.borrow_mut();
        if map.try_reserve(1).is_err() {
            return Err(oom_error("SMgrRelation hashtable"));
        }
        map.insert(key, entry);
        Ok(())
    })?;
    Ok(data)
}

/// True if a cache entry exists for `key` (smgr's `smgrreleaserellocator`
/// avoids materializing a new entry).
pub fn cache_contains(key: RelFileLocatorBackend) -> bool {
    RELNS.with(|r| r.borrow().contains_key(&key))
}

/// Snapshot the boundary `SMgrRelationData` for an open reln, or `None`.
pub fn cache_get(key: RelFileLocatorBackend) -> Option<SMgrRelationData> {
    RELNS.with(|r| r.borrow().get(&key).map(|e| e.data))
}

/// Run `f` over an existing entry's boundary data (e.g. smgr's cache updates).
pub fn with_data_mut<R>(key: RelFileLocatorBackend, f: impl FnOnce(&mut SMgrRelationData) -> R) -> Option<R> {
    RELNS.with(|r| r.borrow_mut().get_mut(&key).map(|e| f(&mut e.data)))
}

/// Run `f` over the (boundary data, md state) of an existing entry.
fn with_entry_mut<R>(
    key: RelFileLocatorBackend,
    f: impl FnOnce(&SMgrRelationData, &mut MdRelnState) -> R,
) -> Option<R> {
    RELNS.with(|r| {
        r.borrow_mut().get_mut(&key).map(|e| {
            let data = e.data;
            f(&data, &mut e.md)
        })
    })
}

/// Adjust the pin count of an entry (smgrpin/smgrunpin).
pub fn cache_adjust_pincount(key: RelFileLocatorBackend, delta: i32) {
    RELNS.with(|r| {
        if let Some(e) = r.borrow_mut().get_mut(&key) {
            e.pincount += delta;
        }
    });
}

/// The pin count of an entry (0 if absent).
pub fn cache_pincount(key: RelFileLocatorBackend) -> i32 {
    RELNS.with(|r| r.borrow().get(&key).map(|e| e.pincount).unwrap_or(0))
}

/// Remove an entry from the cache (`smgrdestroy`'s `HASH_REMOVE`). Returns
/// whether it was present.
pub fn cache_remove(key: RelFileLocatorBackend) -> bool {
    RELNS.with(|r| r.borrow_mut().remove(&key).is_some())
}

/// All cached keys (for the bulk walk-and-modify operations).
pub fn cache_keys() -> Vec<RelFileLocatorBackend> {
    RELNS.with(|r| r.borrow().keys().copied().collect())
}

// ===========================================================================
// md storage-manager API (md.h: the f_smgr dispatch targets). Each takes the
// boundary `SMgrRelationData` + a `&mut MdRelnState`; smgr drives them through
// the cache via `cache_*` / `md_with_*` wrappers below.
// ===========================================================================

/// `_mdfd_open_flags()` (md.c:166-174) — base `O_RDWR | PG_BINARY`, plus
/// `PG_O_DIRECT` when `io_direct_flags & IO_DIRECT_DATA`. `static inline`.
#[inline]
fn _mdfd_open_flags() -> i32 {
    // PG_BINARY is 0 on non-Windows.
    let mut flags = libc::O_RDWR;
    if io_direct_data() {
        flags |= pg_o_direct();
    }
    flags
}

/// `PG_O_DIRECT` (`storage/fd.h`): `O_DIRECT` on platforms that have it, else 0.
#[inline]
fn pg_o_direct() -> i32 {
    #[cfg(target_os = "linux")]
    {
        libc::O_DIRECT
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

/// `mdinit()` (md.c:179-185) — initialize md's private memory context for the
/// `MdfdVec` objects. Here the per-fork `MdfdVec` arrays live in process-local
/// `Vec`s, so the context creation is a no-op.
pub fn mdinit() -> PgResult<()> {
    Ok(())
}

/// `mdexists()` (md.c:192-204) — does the physical file for a fork exist?
pub fn mdexists(reln: &SMgrRelationData, st: &mut MdRelnState, forknum: ForkNumber) -> PgResult<bool> {
    // Close it first, to ensure that we notice if the fork has been unlinked
    // since we opened it.  As an optimization, skip that in recovery.
    if !in_recovery() {
        mdclose(st, forknum)?;
    }

    Ok(mdopenfork(reln, st, forknum, EXTENSION_RETURN_NULL)?.is_some())
}

/// `mdcreate()` (md.c:211-263) — create a new relation on disk
/// (`O_CREAT | O_EXCL`), register the dirty segment if not temp.
pub fn mdcreate(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    is_redo: bool,
) -> PgResult<()> {
    let fk = fork_idx(forknum);

    if is_redo && st.md_num_open_segs[fk] > 0 {
        return Ok(()); // created and opened already...
    }

    debug_assert!(st.md_num_open_segs[fk] == 0);

    // We may be using the target tablespace for the first time in this
    // database, so create a per-database subdirectory if needed.
    tablespace_create_dbspace_seam::call(
        reln.smgr_rlocator.locator.spcOid,
        reln.smgr_rlocator.locator.dbOid,
        is_redo,
    )?;

    let path = relpath(reln.smgr_rlocator, forknum);

    let mut fd = fd::vfd_io::PathNameOpenFile(&path, _mdfd_open_flags() | libc::O_CREAT | libc::O_EXCL)?;

    if fd.0 < 0 {
        let save_errno = last_errno();

        if is_redo {
            fd = fd::vfd_io::PathNameOpenFile(&path, _mdfd_open_flags())?;
        }
        if fd.0 < 0 {
            // be sure to report the error reported by create, not open
            return Err(ereport(ERROR)
                .with_saved_errno(save_errno)
                .errcode_for_file_access()
                .errmsg(format!("could not create file \"{path}\": %m"))
                .into_error());
        }
    }

    _fdvec_resize(st, forknum, 1)?;
    {
        let mdfd = &mut st.md_seg_fds[fk][0];
        mdfd.mdfd_vfd = fd;
        mdfd.mdfd_segno = 0;
    }

    if !smgr_is_temp(reln) {
        let seg = st.md_seg_fds[fk][0];
        register_dirty_segment(reln, forknum, &seg)?;
    }

    Ok(())
}

/// `mdunlink()` (md.c:326-337) — unlink a relation's fork(s)
/// (`InvalidForkNumber` => all forks).
pub fn mdunlink(rlocator: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()> {
    if forknum == ForkNumber::InvalidForkNumber {
        for fork in fork_iter() {
            mdunlinkfork(rlocator, fork, is_redo)?;
        }
    } else {
        mdunlinkfork(rlocator, forknum, is_redo)?;
    }

    Ok(())
}

/// `do_truncate()` (md.c:342-361) — truncate a path to 0, logging a WARNING on
/// non-ENOENT error. Returns the `pg_truncate` result (`0`/`-1`). `static`.
fn do_truncate(path: &str) -> PgResult<i32> {
    let ret = pg_truncate_raw(path, 0);

    if ret < 0 && last_errno() != ENOENT {
        let save_errno = last_errno();
        ereport(WARNING)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not truncate file \"{path}\": %m"))
            .finish(md_location("do_truncate"))?;
        set_errno(save_errno);
    }

    Ok(ret)
}

/// `mdunlinkfork()` (md.c:363-465) — the per-fork body of `mdunlink`. `static`.
fn mdunlinkfork(rlocator: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()> {
    let path = relpath(rlocator, forknum);
    let mut ret: i32;

    if is_redo
        || is_binary_upgrade_seam::call()
        || forknum != ForkNumber::MAIN_FORKNUM
        || is_temp(rlocator)
    {
        if !is_temp(rlocator) {
            // Prevent other backends' fds from holding on to the disk space.
            ret = do_truncate(&path)?;

            // Forget any pending sync requests for the first segment.
            let save_errno = last_errno();
            register_forget_request(rlocator, forknum, 0 /* first seg */)?;
            set_errno(save_errno);
        } else {
            ret = 0;
        }

        // Next unlink the file, unless it was already found to be missing.
        if ret >= 0 || last_errno() != ENOENT {
            ret = unlink_raw(&path);
            if ret < 0 && last_errno() != ENOENT {
                let save_errno = last_errno();
                ereport(WARNING)
                    .with_saved_errno(save_errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{path}\": %m"))
                    .finish(md_location("mdunlinkfork"))?;
                set_errno(save_errno);
            }
        }
    } else {
        // Prevent other backends' fds from holding on to the disk space.
        ret = do_truncate(&path)?;

        // Register request to unlink first segment later.
        let save_errno = last_errno();
        register_unlink_segment(rlocator, forknum, 0 /* first seg */)?;
        set_errno(save_errno);
    }

    // Delete any additional segments. Loop until ENOENT to remove inactive
    // segments too.
    if ret >= 0 || last_errno() != ENOENT {
        let mut segno: BlockNumber = 1;
        loop {
            let segpath = format!("{path}.{segno}");

            if !is_temp(rlocator) {
                // Prevent other backends' fds from holding the disk space.
                // We're done if we see ENOENT.
                if do_truncate(&segpath)? < 0 && last_errno() == ENOENT {
                    break;
                }

                // Forget pending sync requests for this segment before unlink.
                register_forget_request(rlocator, forknum, segno)?;
            }

            if unlink_raw(&segpath) < 0 {
                // ENOENT is expected after the last segment...
                if last_errno() != ENOENT {
                    ereport(WARNING)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!("could not remove file \"{segpath}\": %m"))
                        .finish(md_location("mdunlinkfork"))?;
                }
                break;
            }

            segno += 1;
        }
    }

    Ok(())
}

/// `mdextend()` (md.c:476-533) — add a block to a fork at `blocknum`.
pub fn mdextend(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    // Refuse to create block InvalidBlockNumber.
    if blocknum == InvalidBlockNumber {
        let path = relpath(reln.smgr_rlocator, forknum);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot extend file \"{path}\" beyond {InvalidBlockNumber} blocks"
            ))
            .into_error());
    }

    let v = _mdfd_getseg(reln, st, forknum, blocknum, skip_fsync, EXTENSION_CREATE)?
        .expect("EXTENSION_CREATE never returns None");

    let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;

    debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

    let nbytes = file_write_block(v.mdfd_vfd, buffer, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)?;
    if nbytes != BLCKSZ as isize {
        if nbytes < 0 {
            return Err(ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not extend file \"{}\": %m",
                    fd::vfd_io::FilePathName(v.mdfd_vfd)
                ))
                .errhint("Check free disk space.")
                .into_error());
        }
        // short write: complain appropriately
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DISK_FULL)
            .errmsg(format!(
                "could not extend file \"{}\": wrote only {} of {} bytes at block {}",
                fd::vfd_io::FilePathName(v.mdfd_vfd),
                nbytes,
                BLCKSZ,
                blocknum
            ))
            .errhint("Check free disk space.")
            .into_error());
    }

    if !skip_fsync && !smgr_is_temp(reln) {
        register_dirty_segment(reln, forknum, &v)?;
    }

    debug_assert!(_mdnblocks(&v)? <= RELSEG_SIZE);

    Ok(())
}

/// `mdzeroextend()` (md.c:541-652) — extend a fork by `nblocks` zeroed blocks.
pub fn mdzeroextend(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) -> PgResult<()> {
    let mut curblocknum = blocknum;
    let mut remblocks = nblocks;

    debug_assert!(nblocks > 0);

    // Refuse to create block InvalidBlockNumber or larger.
    if blocknum as u64 + nblocks as u64 >= InvalidBlockNumber as u64 {
        let path = relpath(reln.smgr_rlocator, forknum);
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot extend file \"{path}\" beyond {InvalidBlockNumber} blocks"
            ))
            .into_error());
    }

    while remblocks > 0 {
        let segstartblock = curblocknum % RELSEG_SIZE;
        let seekpos = BLCKSZ_I64 * segstartblock as i64;
        let numblocks: i32 = if (segstartblock as i64 + remblocks as i64) > RELSEG_SIZE as i64 {
            (RELSEG_SIZE - segstartblock) as i32
        } else {
            remblocks
        };

        let v = _mdfd_getseg(reln, st, forknum, curblocknum, skip_fsync, EXTENSION_CREATE)?
            .expect("EXTENSION_CREATE never returns None");

        debug_assert!(segstartblock < RELSEG_SIZE);
        debug_assert!(segstartblock + numblocks as BlockNumber <= RELSEG_SIZE);

        // Use FileFallocate for large extensions, FileZero otherwise; cutoff 8.
        // The build only reaches here with FILE_EXTEND_METHOD_POSIX_FALLOCATE
        // (the other non-WRITE_ZEROS value); md.c elog(ERROR)s any other.
        if numblocks > 8 && vfd_core::file_extend_method() != FILE_EXTEND_METHOD_WRITE_ZEROS {
            let ret = fd::vfd_io::FileFallocate(
                v.mdfd_vfd,
                seekpos,
                BLCKSZ_I64 * numblocks as i64,
                WAIT_EVENT_DATA_FILE_EXTEND,
            )?;
            if ret != 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not extend file \"{}\" with FileFallocate(): %m",
                        fd::vfd_io::FilePathName(v.mdfd_vfd)
                    ))
                    .errhint("Check free disk space.")
                    .into_error());
            }
        } else {
            let ret = fd::vfd_io::FileZero(
                v.mdfd_vfd,
                seekpos,
                BLCKSZ_I64 * numblocks as i64,
                WAIT_EVENT_DATA_FILE_EXTEND,
            )?;
            if ret < 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not extend file \"{}\": %m",
                        fd::vfd_io::FilePathName(v.mdfd_vfd)
                    ))
                    .errhint("Check free disk space.")
                    .into_error());
            }
        }

        if !skip_fsync && !smgr_is_temp(reln) {
            register_dirty_segment(reln, forknum, &v)?;
        }

        debug_assert!(_mdnblocks(&v)? <= RELSEG_SIZE);

        remblocks -= numblocks;
        curblocknum += numblocks as BlockNumber;
    }

    Ok(())
}

/// `mdopenfork()` (md.c:664-697) — open the first segment of a fork, or
/// ereport/return NULL per `behavior`. `static`.
fn mdopenfork(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    behavior: i32,
) -> PgResult<Option<MdfdVec>> {
    let fk = fork_idx(forknum);

    // No work if already open.
    if st.md_num_open_segs[fk] > 0 {
        return Ok(Some(st.md_seg_fds[fk][0]));
    }

    let path = relpath(reln.smgr_rlocator, forknum);

    let file = fd::vfd_io::PathNameOpenFile(&path, _mdfd_open_flags())?;

    if file.0 < 0 {
        if (behavior & EXTENSION_RETURN_NULL) != 0 && file_possibly_deleted(last_errno()) {
            return Ok(None);
        }
        return Err(ereport(ERROR)
            .with_saved_errno(last_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{path}\": %m"))
            .into_error());
    }

    _fdvec_resize(st, forknum, 1)?;
    {
        let mdfd = &mut st.md_seg_fds[fk][0];
        mdfd.mdfd_vfd = file;
        mdfd.mdfd_segno = 0;
    }

    let mdfd = st.md_seg_fds[fk][0];
    debug_assert!(_mdnblocks(&mdfd)? <= RELSEG_SIZE);

    Ok(Some(mdfd))
}

/// `mdopen()` (md.c:702-708) — mark every fork as having zero open segments.
pub fn mdopen(st: &mut MdRelnState) -> PgResult<()> {
    for forknum in 0..=fork_idx(MAX_FORKNUM) {
        st.md_num_open_segs[forknum] = 0;
    }
    Ok(())
}

/// `mdclose()` (md.c:713-731) — close the fork's open segments (from the end).
pub fn mdclose(st: &mut MdRelnState, forknum: ForkNumber) -> PgResult<()> {
    let fk = fork_idx(forknum);
    let mut nopensegs = st.md_num_open_segs[fk];

    if nopensegs == 0 {
        return Ok(());
    }

    while nopensegs > 0 {
        let v = st.md_seg_fds[fk][(nopensegs - 1) as usize];
        fd::vfd_io::FileClose(v.mdfd_vfd)?;
        _fdvec_resize(st, forknum, nopensegs - 1)?;
        nopensegs -= 1;
    }

    Ok(())
}

/// `mdprefetch()` (md.c:736-775) — initiate asynchronous read of blocks.
pub fn mdprefetch(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
) -> PgResult<bool> {
    debug_assert!(!io_direct_data());

    if blocknum as u64 + nblocks as u64 > MaxBlockNumber as u64 + 1 {
        return Ok(false);
    }

    let mut blocknum = blocknum;
    let mut nblocks = nblocks;
    while nblocks > 0 {
        let behavior = if in_recovery() {
            EXTENSION_RETURN_NULL
        } else {
            EXTENSION_FAIL
        };
        let v = _mdfd_getseg(reln, st, forknum, blocknum, false, behavior)?;
        let v = match v {
            Some(v) => v,
            None => return Ok(false),
        };

        let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;

        debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

        let nblocks_this_segment =
            core::cmp::min(nblocks as i64, (RELSEG_SIZE - (blocknum % RELSEG_SIZE)) as i64) as i32;

        let _ = fd::vfd_io::FilePrefetch(
            v.mdfd_vfd,
            seekpos,
            BLCKSZ as i64 * nblocks_this_segment as i64,
            WAIT_EVENT_DATA_FILE_PREFETCH,
        )?;

        blocknum += nblocks_this_segment as BlockNumber;
        nblocks -= nblocks_this_segment;
    }

    Ok(true)
}

/// `buffers_to_iovec()` (md.c:784-827) — coalesce block buffers into iovecs.
/// In this safe port each block is its own iovec (Rust slices are distinct
/// allocations); the byte movement is identical. Returns the iovec count.
fn buffers_to_iovec(nblocks: i32) -> i32 {
    debug_assert!(nblocks >= 1);
    nblocks
}

/// `mdmaxcombine()` (md.c:833-842) — max blocks combinable into one IO without
/// crossing a segment boundary: `RELSEG_SIZE - (blocknum % RELSEG_SIZE)`. PURE.
pub fn mdmaxcombine(_reln: &SMgrRelationData, _forknum: ForkNumber, blocknum: BlockNumber) -> u32 {
    let segoff = blocknum % RELSEG_SIZE;
    RELSEG_SIZE - segoff
}

/// `mdreadv()` (md.c:847-980) — read a block range into the supplied buffers.
pub fn mdreadv(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut [u8]],
    nblocks: BlockNumber,
) -> PgResult<()> {
    let mut blocknum = blocknum;
    let mut nblocks = nblocks;
    let mut buf_off: usize = 0;

    while nblocks > 0 {
        let v = _mdfd_getseg(
            reln,
            st,
            forknum,
            blocknum,
            false,
            EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
        )?
        .expect("EXTENSION_FAIL ereports rather than returning None");

        let mut seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;

        debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

        let mut nblocks_this_segment =
            core::cmp::min(nblocks as i64, (RELSEG_SIZE - (blocknum % RELSEG_SIZE)) as i64)
                as BlockNumber;
        nblocks_this_segment = core::cmp::min(nblocks_this_segment, PG_IOV_MAX as BlockNumber);

        if nblocks_this_segment != nblocks {
            return elog_error("read crosses segment boundary", "mdreadv");
        }

        let _iovcnt = buffers_to_iovec(nblocks_this_segment as i32);
        let size_this_segment = nblocks_this_segment as usize * BLCKSZ;
        let mut transferred_this_segment: usize = 0;

        loop {
            let nbytes = {
                let seg_bufs = &mut buffers[buf_off..buf_off + nblocks_this_segment as usize];
                read_segment(v.mdfd_vfd, seg_bufs, seekpos, transferred_this_segment)?
            };

            if nbytes < 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not read blocks {}..{} in file \"{}\": %m",
                        blocknum,
                        blocknum + nblocks_this_segment - 1,
                        fd::vfd_io::FilePathName(v.mdfd_vfd)
                    ))
                    .into_error());
            }

            if nbytes == 0 {
                // At or past EOF. Normally an error; if zero_damaged_pages or
                // InRecovery, return zeroes.
                if zero_damaged_pages() || in_recovery() {
                    let start = transferred_this_segment / BLCKSZ;
                    for i in start..nblocks_this_segment as usize {
                        for b in buffers[buf_off + i].iter_mut() {
                            *b = 0;
                        }
                    }
                    break;
                } else {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATA_CORRUPTED)
                        .errmsg(format!(
                            "could not read blocks {}..{} in file \"{}\": read only {} of {} bytes",
                            blocknum,
                            blocknum + nblocks_this_segment - 1,
                            fd::vfd_io::FilePathName(v.mdfd_vfd),
                            transferred_this_segment,
                            size_this_segment
                        ))
                        .into_error());
                }
            }

            transferred_this_segment += nbytes as usize;
            debug_assert!(transferred_this_segment <= size_this_segment);
            if transferred_this_segment == size_this_segment {
                break;
            }

            seekpos += nbytes as i64;
        }

        nblocks -= nblocks_this_segment;
        buf_off += nblocks_this_segment as usize;
        blocknum += nblocks_this_segment;
    }

    Ok(())
}

/// Helper for `mdreadv`'s short-read loop: one `FileReadV` over the segment's
/// remaining buffers, skipping `already` bytes (mirrors C's
/// `compute_remaining_iovec` partial-iovec fixup).
fn read_segment(
    file: File,
    seg_bufs: &mut [&mut [u8]],
    seekpos: i64,
    already: usize,
) -> PgResult<isize> {
    let mut skip = already;
    let mut start_block = 0usize;
    while start_block < seg_bufs.len() && skip >= seg_bufs[start_block].len() {
        skip -= seg_bufs[start_block].len();
        start_block += 1;
    }
    if start_block >= seg_bufs.len() {
        return Ok(0);
    }
    let (head, tail) = seg_bufs[start_block..].split_at_mut(1);
    let mut iov: Vec<std::io::IoSliceMut<'_>> = Vec::new();
    iov.try_reserve(1 + tail.len())
        .map_err(|_| oom_error("read_segment iovec"))?;
    iov.push(std::io::IoSliceMut::new(&mut head[0][skip..]));
    for b in tail.iter_mut() {
        iov.push(std::io::IoSliceMut::new(b));
    }
    fd::vfd_io::FileReadV(file, &mut iov, seekpos, WAIT_EVENT_DATA_FILE_READ)
}

/// `mdwritev()` (md.c:1059-1155) — write the supplied blocks.
pub fn mdwritev(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[&[u8]],
    nblocks: BlockNumber,
    skip_fsync: bool,
) -> PgResult<()> {
    let mut blocknum = blocknum;
    let mut nblocks = nblocks;
    let mut buf_off: usize = 0;

    while nblocks > 0 {
        let v = _mdfd_getseg(
            reln,
            st,
            forknum,
            blocknum,
            skip_fsync,
            EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
        )?
        .expect("EXTENSION_FAIL ereports rather than returning None");

        let mut seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;

        debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

        let mut nblocks_this_segment =
            core::cmp::min(nblocks as i64, (RELSEG_SIZE - (blocknum % RELSEG_SIZE)) as i64)
                as BlockNumber;
        nblocks_this_segment = core::cmp::min(nblocks_this_segment, PG_IOV_MAX as BlockNumber);

        if nblocks_this_segment != nblocks {
            return elog_error("write crosses segment boundary", "mdwritev");
        }

        let _iovcnt = buffers_to_iovec(nblocks_this_segment as i32);
        let size_this_segment = nblocks_this_segment as usize * BLCKSZ;
        let mut transferred_this_segment: usize = 0;

        loop {
            let nbytes = {
                let seg_bufs = &buffers[buf_off..buf_off + nblocks_this_segment as usize];
                write_segment(v.mdfd_vfd, seg_bufs, seekpos, transferred_this_segment)?
            };

            if nbytes < 0 {
                let enospc = last_errno() == ENOSPC;
                let mut b = ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not write blocks {}..{} in file \"{}\": %m",
                        blocknum,
                        blocknum + nblocks_this_segment - 1,
                        fd::vfd_io::FilePathName(v.mdfd_vfd)
                    ));
                if enospc {
                    b = b.errhint("Check free disk space.");
                }
                return Err(b.into_error());
            }

            transferred_this_segment += nbytes as usize;
            debug_assert!(transferred_this_segment <= size_this_segment);
            if transferred_this_segment == size_this_segment {
                break;
            }

            seekpos += nbytes as i64;
        }

        if !skip_fsync && !smgr_is_temp(reln) {
            register_dirty_segment(reln, forknum, &v)?;
        }

        nblocks -= nblocks_this_segment;
        buf_off += nblocks_this_segment as usize;
        blocknum += nblocks_this_segment;
    }

    Ok(())
}

/// Helper for `mdwritev`'s short-write loop (C's `compute_remaining_iovec`).
fn write_segment(file: File, seg_bufs: &[&[u8]], seekpos: i64, already: usize) -> PgResult<isize> {
    let mut skip = already;
    let mut start_block = 0usize;
    while start_block < seg_bufs.len() && skip >= seg_bufs[start_block].len() {
        skip -= seg_bufs[start_block].len();
        start_block += 1;
    }
    if start_block >= seg_bufs.len() {
        return Ok(0);
    }
    let mut iov: Vec<std::io::IoSlice<'_>> = Vec::new();
    iov.try_reserve(seg_bufs.len() - start_block)
        .map_err(|_| oom_error("write_segment iovec"))?;
    iov.push(std::io::IoSlice::new(&seg_bufs[start_block][skip..]));
    for b in &seg_bufs[start_block + 1..] {
        iov.push(std::io::IoSlice::new(b));
    }
    fd::vfd_io::FileWriteV(file, &iov, seekpos, WAIT_EVENT_DATA_FILE_WRITE)
}

/// `mdwriteback()` (md.c:1164-1213) — kernel writeback hints for a block range.
pub fn mdwriteback(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    debug_assert!(!io_direct_data());

    let mut blocknum = blocknum;
    let mut nblocks = nblocks;

    while nblocks > 0 {
        let mut nflush = nblocks;

        let v = _mdfd_getseg(reln, st, forknum, blocknum, true /* not used */, EXTENSION_DONT_OPEN)?;

        // We might be flushing buffers of already-removed relations; ignore.
        // If the segment wasn't open, don't re-open it.
        let v = match v {
            Some(v) => v,
            None => return Ok(()),
        };

        let segnum_start = blocknum / RELSEG_SIZE;
        let segnum_end = (blocknum + nblocks - 1) / RELSEG_SIZE;
        if segnum_start != segnum_end {
            nflush = RELSEG_SIZE - (blocknum % RELSEG_SIZE);
        }

        debug_assert!(nflush >= 1);
        debug_assert!(nflush <= nblocks);

        let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;

        fd::vfd_io::FileWriteback(
            v.mdfd_vfd,
            seekpos,
            BLCKSZ_I64 * nflush as i64,
            WAIT_EVENT_DATA_FILE_FLUSH,
        )?;

        nblocks -= nflush;
        blocknum += nflush;
    }

    Ok(())
}

/// `mdnblocks()` (md.c:1223-1275) — number of blocks in a fork; opens all
/// active segments.
pub fn mdnblocks(reln: &SMgrRelationData, st: &mut MdRelnState, forknum: ForkNumber) -> PgResult<BlockNumber> {
    let fk = fork_idx(forknum);

    mdopenfork(reln, st, forknum, EXTENSION_FAIL)?;

    debug_assert!(st.md_num_open_segs[fk] > 0);

    let mut segno = (st.md_num_open_segs[fk] - 1) as BlockNumber;
    let mut v = st.md_seg_fds[fk][segno as usize];

    loop {
        let nblocks = _mdnblocks(&v)?;
        if nblocks > RELSEG_SIZE {
            return elog_fatal("segment too big", "mdnblocks");
        }
        if nblocks < RELSEG_SIZE {
            return Ok(segno * RELSEG_SIZE + nblocks);
        }

        segno += 1;

        match _mdfd_openseg(reln, st, forknum, segno, 0)? {
            Some(seg) => v = seg,
            None => return Ok(segno * RELSEG_SIZE),
        }
    }
}

/// `mdtruncate()` (md.c:1290-1374) — truncate a fork to `nblocks` from `curnblk`.
pub fn mdtruncate(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    curnblk: BlockNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    let fk = fork_idx(forknum);

    if nblocks > curnblk {
        // Bogus request ... but no complaint if InRecovery.
        if in_recovery() {
            return Ok(());
        }
        let path = relpath(reln.smgr_rlocator, forknum);
        return Err(ereport(ERROR)
            .errmsg(format!(
                "could not truncate file \"{path}\" to {nblocks} blocks: it's only {curnblk} blocks now"
            ))
            .into_error());
    }
    if nblocks == curnblk {
        return Ok(()); // no work
    }

    let mut curopensegs = st.md_num_open_segs[fk];
    while curopensegs > 0 {
        let priorblocks = (curopensegs - 1) as BlockNumber * RELSEG_SIZE;

        let v = st.md_seg_fds[fk][(curopensegs - 1) as usize];

        if priorblocks > nblocks {
            // This segment is no longer active. Truncate but do not delete it.
            if fd::vfd_io::FileTruncate(v.mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE)? < 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not truncate file \"{}\": %m",
                        fd::vfd_io::FilePathName(v.mdfd_vfd)
                    ))
                    .into_error());
            }

            if !smgr_is_temp(reln) {
                register_dirty_segment(reln, forknum, &v)?;
            }

            // we never drop the 1st segment
            debug_assert!(curopensegs - 1 != 0);

            fd::vfd_io::FileClose(v.mdfd_vfd)?;
            _fdvec_resize(st, forknum, curopensegs - 1)?;
        } else if priorblocks + RELSEG_SIZE > nblocks {
            // The last segment we want to keep. Truncate to the right length.
            let lastsegblocks = nblocks - priorblocks;

            if fd::vfd_io::FileTruncate(
                v.mdfd_vfd,
                lastsegblocks as i64 * BLCKSZ_I64,
                WAIT_EVENT_DATA_FILE_TRUNCATE,
            )? < 0
            {
                return Err(ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not truncate file \"{}\" to {} blocks: %m",
                        fd::vfd_io::FilePathName(v.mdfd_vfd),
                        nblocks
                    ))
                    .into_error());
            }
            if !smgr_is_temp(reln) {
                register_dirty_segment(reln, forknum, &v)?;
            }
        } else {
            // Still need this and all earlier segments.
            break;
        }
        curopensegs -= 1;
    }

    Ok(())
}

/// `mdregistersync()` (md.c:1380-1417) — mark a whole fork as needing fsync.
pub fn mdregistersync(reln: &SMgrRelationData, st: &mut MdRelnState, forknum: ForkNumber) -> PgResult<()> {
    let fk = fork_idx(forknum);

    // mdnblocks opens all active segments so the loop below gets them all.
    mdnblocks(reln, st, forknum)?;

    let min_inactive_seg = st.md_num_open_segs[fk];
    let mut segno = min_inactive_seg;

    while _mdfd_openseg(reln, st, forknum, segno as BlockNumber, 0)?.is_some() {
        segno += 1;
    }

    while segno > 0 {
        let v = st.md_seg_fds[fk][(segno - 1) as usize];

        register_dirty_segment(reln, forknum, &v)?;

        if segno > min_inactive_seg {
            fd::vfd_io::FileClose(v.mdfd_vfd)?;
            _fdvec_resize(st, forknum, segno - 1)?;
        }

        segno -= 1;
    }

    Ok(())
}

/// `mdimmedsync()` (md.c:1431-1481) — immediately fsync a fork (all segments).
pub fn mdimmedsync(reln: &SMgrRelationData, st: &mut MdRelnState, forknum: ForkNumber) -> PgResult<()> {
    let fk = fork_idx(forknum);

    mdnblocks(reln, st, forknum)?;

    let min_inactive_seg = st.md_num_open_segs[fk];
    let mut segno = min_inactive_seg;

    while _mdfd_openseg(reln, st, forknum, segno as BlockNumber, 0)?.is_some() {
        segno += 1;
    }

    while segno > 0 {
        let v = st.md_seg_fds[fk][(segno - 1) as usize];

        if file_sync_failed(v.mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC)? {
            return Err(ereport(data_sync_elevel(ERROR))
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not fsync file \"{}\": %m",
                    fd::vfd_io::FilePathName(v.mdfd_vfd)
                ))
                .into_error());
        }

        if segno > min_inactive_seg {
            fd::vfd_io::FileClose(v.mdfd_vfd)?;
            _fdvec_resize(st, forknum, segno - 1)?;
        }

        segno -= 1;
    }

    Ok(())
}

/// `mdfd()` (md.c:1483-1496) — the raw kernel fd + in-segment offset for a
/// block (AIO worker hand-off). Returns `(fd, off)`.
pub fn mdfd(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
) -> PgResult<(i32, u32)> {
    let _ = mdopenfork(reln, st, forknum, EXTENSION_FAIL)?;

    let v = _mdfd_getseg(reln, st, forknum, blocknum, false, EXTENSION_FAIL)?
        .expect("mdfd: EXTENSION_FAIL ereports rather than returning None");

    let off = (BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64) as u32;

    debug_assert!((off as i64) < BLCKSZ_I64 * RELSEG_SIZE as i64);

    let raw = fd::vfd_io::FileGetRawDesc(v.mdfd_vfd)?;
    Ok((raw, off))
}

// ===========================================================================
// md sync callbacks + DB-wide helpers (md.h)
// ===========================================================================

/// `register_dirty_segment()` (md.c:1507-1546) — mark a segment as needing
/// fsync: `RegisterSyncRequest(SYNC_REQUEST)`, falling back to a synchronous
/// `FileSync` when the checkpointer queue is full. `static`.
fn register_dirty_segment(reln: &SMgrRelationData, forknum: ForkNumber, seg: &MdfdVec) -> PgResult<()> {
    // Temp relations should never be fsync'd.
    debug_assert!(!smgr_is_temp(reln));

    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        forknum,
        reln.smgr_rlocator.locator,
        seg.mdfd_segno as u64,
    );

    if !register_sync_request_seam::call(tag, SyncRequestType::SYNC_REQUEST, false /* retryOnError */)? {
        ereport(DEBUG1)
            .errmsg_internal("could not forward fsync request because request queue is full")
            .finish(md_location("register_dirty_segment"))?;

        if file_sync_failed(seg.mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC)? {
            return Err(ereport(data_sync_elevel(ERROR))
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not fsync file \"{}\": %m",
                    fd::vfd_io::FilePathName(seg.mdfd_vfd)
                ))
                .into_error());
        }
    }

    Ok(())
}

/// `register_unlink_segment()` (md.c:1551-1563) — schedule a segment for unlink
/// after the next checkpoint (`SYNC_UNLINK_REQUEST`). `static`.
fn register_unlink_segment(rlocator: RelFileLocatorBackend, forknum: ForkNumber, segno: BlockNumber) -> PgResult<()> {
    // Should never be used with temp relations.
    debug_assert!(!is_temp(rlocator));

    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        forknum,
        rlocator.locator,
        segno as u64,
    );
    register_sync_request_seam::call(tag, SyncRequestType::SYNC_UNLINK_REQUEST, true /* retryOnError */)?;
    Ok(())
}

/// `register_forget_request()` (md.c:1568-1577) — forget pending fsyncs for a
/// segment (`SYNC_FORGET_REQUEST`). `static`.
fn register_forget_request(rlocator: RelFileLocatorBackend, forknum: ForkNumber, segno: BlockNumber) -> PgResult<()> {
    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        forknum,
        rlocator.locator,
        segno as u64,
    );
    register_sync_request_seam::call(tag, SyncRequestType::SYNC_FORGET_REQUEST, true /* retryOnError */)?;
    Ok(())
}

/// `ForgetDatabaseSyncRequests()` (md.c:1583-1595) — forget all fsyncs/unlinks
/// for a database (a `SYNC_FILTER_REQUEST`).
pub fn ForgetDatabaseSyncRequests(dbid: Oid) -> PgResult<()> {
    let rlocator = RelFileLocator {
        spcOid: 0,
        dbOid: dbid,
        relNumber: 0,
    };

    // INIT_MD_FILETAG(tag, rlocator, InvalidForkNumber, InvalidBlockNumber)
    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        ForkNumber::InvalidForkNumber,
        rlocator,
        InvalidBlockNumber as u64,
    );
    register_sync_request_seam::call(tag, SyncRequestType::SYNC_FILTER_REQUEST, true /* retryOnError */)?;
    Ok(())
}

// ===========================================================================
// md static (file-local) helpers
// ===========================================================================

/// `_fdvec_resize()` (md.c:1632-1674) — grow/shrink the fork's `md_seg_fds`
/// open-segment array to `nseg` entries (never shrinks the allocation below the
/// high-water mark, so `mdtruncate` allocates no memory). `static`.
fn _fdvec_resize(reln: &mut MdRelnState, forknum: ForkNumber, nseg: i32) -> PgResult<()> {
    let fk = fork_idx(forknum);

    if nseg == 0 {
        if reln.md_num_open_segs[fk] > 0 {
            reln.md_seg_fds[fk] = Vec::new();
        }
    } else if reln.md_num_open_segs[fk] == 0 {
        let mut v: Vec<MdfdVec> = Vec::new();
        v.try_reserve(nseg as usize).map_err(|_| oom_error("_fdvec_resize"))?;
        v.resize(nseg as usize, MdfdVec::default());
        reln.md_seg_fds[fk] = v;
    } else if nseg > reln.md_num_open_segs[fk] {
        if reln.md_seg_fds[fk].len() < nseg as usize {
            let additional = nseg as usize - reln.md_seg_fds[fk].len();
            reln.md_seg_fds[fk]
                .try_reserve(additional)
                .map_err(|_| oom_error("_fdvec_resize grow"))?;
            reln.md_seg_fds[fk].resize(nseg as usize, MdfdVec::default());
        }
    } else {
        // Do not reallocate smaller so mdtruncate can promise no allocation;
        // keep the high-water-mark capacity/len, only md_num_open_segs shrinks.
    }

    reln.md_num_open_segs[fk] = nseg;
    Ok(())
}

/// `_mdfd_segpath()` (md.c:1680-1694) — the path for a given segment of a fork
/// (`"<relpath>"` for segno 0, else `"<relpath>.<segno>"`). `static`.
fn _mdfd_segpath(reln: &SMgrRelationData, forknum: ForkNumber, segno: BlockNumber) -> String {
    let path = relpath(reln.smgr_rlocator, forknum);

    if segno > 0 {
        format!("{path}.{segno}")
    } else {
        path
    }
}

/// `_mdfd_openseg()` (md.c:1700-1733) — open the specified segment and append an
/// `MdfdVec` for it to `md_seg_fds`; returns `None` on open failure. `static`.
fn _mdfd_openseg(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    segno: BlockNumber,
    oflags: i32,
) -> PgResult<Option<MdfdVec>> {
    let fk = fork_idx(forknum);

    let fullpath = _mdfd_segpath(reln, forknum, segno);

    let file = fd::vfd_io::PathNameOpenFile(&fullpath, _mdfd_open_flags() | oflags)?;

    if file.0 < 0 {
        return Ok(None);
    }

    // Segments are always opened in order; we add a new one at the end.
    debug_assert!(segno == st.md_num_open_segs[fk] as BlockNumber);

    _fdvec_resize(st, forknum, segno as i32 + 1)?;

    {
        let v = &mut st.md_seg_fds[fk][segno as usize];
        v.mdfd_vfd = file;
        v.mdfd_segno = segno;
    }

    let v = st.md_seg_fds[fk][segno as usize];
    debug_assert!(_mdnblocks(&v)? <= RELSEG_SIZE);

    Ok(Some(v))
}

/// `_mdfd_getseg()` (md.c:1743-1867) — find (and per `behavior` open/create) the
/// segment holding `blkno`. `static`.
fn _mdfd_getseg(
    reln: &SMgrRelationData,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blkno: BlockNumber,
    skip_fsync: bool,
    behavior: i32,
) -> PgResult<Option<MdfdVec>> {
    let fk = fork_idx(forknum);

    debug_assert!(
        behavior & (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL | EXTENSION_DONT_OPEN)
            != 0
    );

    let targetseg = blkno / RELSEG_SIZE;

    // If an existing and opened segment, we're done.
    if targetseg < st.md_num_open_segs[fk] as BlockNumber {
        let v = st.md_seg_fds[fk][targetseg as usize];
        return Ok(Some(v));
    }

    // The caller only wants the segment if we already had it open.
    if behavior & EXTENSION_DONT_OPEN != 0 {
        return Ok(None);
    }

    // The target segment is not yet open. Iterate from the last opened (or the
    // first segment if none was opened before) up to the target.
    let mut v: MdfdVec;
    if st.md_num_open_segs[fk] > 0 {
        v = st.md_seg_fds[fk][(st.md_num_open_segs[fk] - 1) as usize];
    } else {
        match mdopenfork(reln, st, forknum, behavior)? {
            Some(seg) => v = seg,
            None => return Ok(None), // if behavior & EXTENSION_RETURN_NULL
        }
    }

    let mut nextsegno = st.md_num_open_segs[fk] as BlockNumber;
    while nextsegno <= targetseg {
        let nblocks = _mdnblocks(&v)?;
        let mut flags = 0;

        debug_assert!(nextsegno == v.mdfd_segno + 1);

        if nblocks > RELSEG_SIZE {
            return elog_fatal("segment too big", "_mdfd_getseg");
        }

        if (behavior & EXTENSION_CREATE != 0)
            || (in_recovery() && (behavior & EXTENSION_CREATE_RECOVERY != 0))
        {
            // Create new segments during recovery; pad short prior segments out
            // to RELSEG_SIZE with zeroes to maintain the size invariant.
            if nblocks < RELSEG_SIZE {
                let zerobuf = vec![0u8; BLCKSZ];
                mdextend(
                    reln,
                    st,
                    forknum,
                    nextsegno * RELSEG_SIZE - 1,
                    &zerobuf,
                    skip_fsync,
                )?;
            }
            flags = libc::O_CREAT;
        } else if nblocks < RELSEG_SIZE {
            // When not extending, only open the next segment if the current one
            // is exactly RELSEG_SIZE. If not, return NULL or fail.
            if behavior & EXTENSION_RETURN_NULL != 0 {
                // No failing syscall is involved; set errno to ENOENT so
                // callers that discern reasons see the deleted-file case.
                set_errno(ENOENT);
                return Ok(None);
            }

            return Err(ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not open file \"{}\" (target block {}): previous segment is only {} blocks",
                    _mdfd_segpath(reln, forknum, nextsegno),
                    blkno,
                    nblocks
                ))
                .into_error());
        }

        match _mdfd_openseg(reln, st, forknum, nextsegno, flags)? {
            Some(seg) => v = seg,
            None => {
                if (behavior & EXTENSION_RETURN_NULL != 0) && file_possibly_deleted(last_errno()) {
                    return Ok(None);
                }
                return Err(ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not open file \"{}\" (target block {}): %m",
                        _mdfd_segpath(reln, forknum, nextsegno),
                        blkno
                    ))
                    .into_error());
            }
        }

        nextsegno += 1;
    }

    Ok(Some(v))
}

/// `_mdnblocks()` (md.c:1872-1885) — blocks present in a single segment file
/// (`FileSize(seg) / BLCKSZ`, ignoring a partial trailing block). `static`.
fn _mdnblocks(seg: &MdfdVec) -> PgResult<BlockNumber> {
    let len = fd::vfd_io::FileSize(seg.mdfd_vfd)?;
    if len < 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(last_errno())
            .errcode_for_file_access()
            .errmsg(format!(
                "could not seek to end of file \"{}\": %m",
                fd::vfd_io::FilePathName(seg.mdfd_vfd)
            ))
            .into_error());
    }
    // ignore any partial block at EOF
    Ok((len / BLCKSZ_I64) as BlockNumber)
}

// ===========================================================================
// md sync-handler callbacks (syncsw[SYNC_HANDLER_MD] — consumed by sync.c)
// ===========================================================================

/// `mdsyncfiletag()` (md.c:1893-1937) — sync a file given its `FileTag`. Sync
/// callback. Returns `(result, path, errno)` as a [`FileTagOpResult`].
pub fn mdsyncfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    // reln = smgropen(ftag->rlocator, INVALID_PROC_NUMBER); we need its md state.
    let key = RelFileLocatorBackend {
        locator: ftag.rlocator,
        backend: INVALID_PROC_NUMBER,
    };
    cache_open(ftag.rlocator, INVALID_PROC_NUMBER)?;
    let forknum = ForkNumber::from_i32(ftag.forknum as i32).expect("FileTag.forknum is a ForkNumber");
    let fk = fork_idx(forknum);

    // See if we already have the file open, or need to open it.
    let (file, need_to_close, path): (File, bool, String) = with_entry_mut(key, |reln, st| {
        if (ftag.segno as i32) < st.md_num_open_segs[fk] {
            let file = st.md_seg_fds[fk][ftag.segno as usize].mdfd_vfd;
            let path = fd::vfd_io::FilePathName(file);
            Ok((file, false, path))
        } else {
            let p = _mdfd_segpath(reln, forknum, ftag.segno as BlockNumber);
            let file = fd::vfd_io::PathNameOpenFile(&p, _mdfd_open_flags())?;
            Ok((file, true, p))
        }
    })
    .expect("mdsyncfiletag: smgropen created the cache entry")?;

    if file.0 < 0 {
        return Ok(FileTagOpResult {
            result: -1,
            path,
            errno: last_errno(),
        });
    }

    // Sync the file.
    let result = if file_sync_failed(file, WAIT_EVENT_DATA_FILE_SYNC)? {
        -1
    } else {
        0
    };
    let save_errno = last_errno();

    if need_to_close {
        fd::vfd_io::FileClose(file)?;
    }

    set_errno(save_errno);
    Ok(FileTagOpResult {
        result,
        path,
        errno: save_errno,
    })
}

/// `mdunlinkfiletag()` (md.c:1945-1956) — unlink a file given its `FileTag`.
/// Unlink callback. Returns `(result, path, errno)`.
pub fn mdunlinkfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    // p = relpathperm(ftag->rlocator, MAIN_FORKNUM);
    let path = relpathperm(ftag.rlocator, ForkNumber::MAIN_FORKNUM);

    // Try to unlink the file.
    let ret = unlink_raw(&path);
    Ok(FileTagOpResult {
        result: ret,
        errno: last_errno(),
        path,
    })
}

/// `mdfiletagmatches()` (md.c:1963-1973) — does a candidate `FileTag` match the
/// filter `FileTag` (same dbOid)? Used for `SYNC_FILTER_REQUEST`. PURE; the
/// seam's `PgResult` failure surface is always `Ok` here (md.c never errors).
pub fn mdfiletagmatches(ftag: FileTag, candidate: FileTag) -> PgResult<bool> {
    Ok(ftag.rlocator.dbOid == candidate.rlocator.dbOid)
}

// ===========================================================================
// raw filesystem ops (md.c calls libc unlink()/truncate() directly on its
// non-VFD-managed segment paths; the VFD pool is only for OPEN segments).
// ===========================================================================

/// `unlink(path)` (libc) — returns 0 / -1 with errno set, exactly as md.c uses.
fn unlink_raw(path: &str) -> i32 {
    let c = match std::ffi::CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            set_errno(libc::EINVAL);
            return -1;
        }
    };
    unsafe { libc::unlink(c.as_ptr()) }
}

/// `pg_truncate(path, len)` (file_utils) over the raw path — `truncate(2)` on
/// non-Windows; returns 0 / -1 with errno set, as md.c's `do_truncate` expects.
fn pg_truncate_raw(path: &str, length: i64) -> i32 {
    let c = match std::ffi::CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            set_errno(libc::EINVAL);
            return -1;
        }
    };
    unsafe { libc::truncate(c.as_ptr(), length as libc::off_t) }
}

/// `FileSync(file)` returning whether it FAILED (`< 0`), mapping the fd layer's
/// `PgResult<()>` (Ok on success or skipped-because-not-open) to md.c's
/// `FileSync(...) < 0` test while preserving errno on failure.
fn file_sync_failed(file: File, wait_event: u32) -> PgResult<bool> {
    match fd::vfd_io::FileSync(file, wait_event) {
        Ok(()) => Ok(false),
        Err(e) => {
            // Surface the saved errno for the caller's `%m` message.
            if let Some(en) = e.saved_errno() {
                set_errno(en);
            }
            Ok(true)
        }
    }
}

/// `FileWrite` of one `BLCKSZ` block at `seekpos` returning the byte count
/// (md.c's `FileWrite(...) != BLCKSZ` extend path). Wraps `FileWriteV` with a
/// single-iovec window.
fn file_write_block(file: File, buffer: &[u8], seekpos: i64, wait_event: u32) -> PgResult<isize> {
    let iov = [std::io::IoSlice::new(buffer)];
    fd::vfd_io::FileWriteV(file, &iov, seekpos, wait_event)
}

// ===========================================================================
// helpers: elog, fork iteration, location
// ===========================================================================

/// The four-fork iterator (`for (forknum = 0; forknum <= MAX_FORKNUM; ...)`),
/// since `ForkNumber` is a non-stepping enum.
fn fork_iter() -> [ForkNumber; types_storage::smgr::SMGR_NFORKS] {
    [
        ForkNumber::MAIN_FORKNUM,
        ForkNumber::FSM_FORKNUM,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        ForkNumber::INIT_FORKNUM,
    ]
}

/// `elog(ERROR, msg)` — an internal-error abort (always `Err`).
fn elog_error<T>(msg: &str, func: &'static str) -> PgResult<T> {
    Err(ereport(ERROR)
        .errmsg_internal(msg.to_string())
        .into_error()
        .with_error_location(md_location(func)))
}

/// `elog(FATAL, msg)` — a fatal internal-error abort (always `Err`).
fn elog_fatal<T>(msg: &str, func: &'static str) -> PgResult<T> {
    Err(ereport(FATAL)
        .errmsg_internal(msg.to_string())
        .into_error()
        .with_error_location(md_location(func)))
}

/// Error location for md.c diagnostics.
fn md_location(funcname: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new("md.c", 0, funcname)
}

// ===========================================================================
// Cache-driving wrappers — the entry points smgr.c (and md's own callbacks)
// call to run an md operation against a cached relation's md state.
// ===========================================================================

/// Run `mdopenfork`/`mdexists`-style operations needing `&mut MdRelnState`
/// against the cached entry for `key`, panicking if it is absent (smgr always
/// `smgropen`s first). Generic over the md function.
fn md_run<R>(key: RelFileLocatorBackend, f: impl FnOnce(&SMgrRelationData, &mut MdRelnState) -> PgResult<R>) -> PgResult<R> {
    with_entry_mut(key, f).expect("md operation on an unopened SMgrRelation")
}

/// `smgr_exists` -> `mdexists`.
pub fn md_exists(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<bool> {
    md_run(key, |reln, st| mdexists(reln, st, forknum))
}

/// `smgr_create` -> `mdcreate`.
pub fn md_create(key: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()> {
    md_run(key, |reln, st| mdcreate(reln, st, forknum, is_redo))
}

/// `smgr_unlink` -> `mdunlink` (no cache entry required).
pub fn md_unlink(rlocator: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()> {
    mdunlink(rlocator, forknum, is_redo)
}

/// `smgr_extend` -> `mdextend`.
pub fn md_extend(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    md_run(key, |reln, st| mdextend(reln, st, forknum, blocknum, buffer, skip_fsync))
}

/// `smgr_zeroextend` -> `mdzeroextend`.
pub fn md_zeroextend(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) -> PgResult<()> {
    md_run(key, |reln, st| mdzeroextend(reln, st, forknum, blocknum, nblocks, skip_fsync))
}

/// `smgr_close` -> `mdclose`.
pub fn md_close(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    md_run(key, |_reln, st| mdclose(st, forknum))
}

/// `smgr_prefetch` -> `mdprefetch`.
pub fn md_prefetch(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber, nblocks: i32) -> PgResult<bool> {
    md_run(key, |reln, st| mdprefetch(reln, st, forknum, blocknum, nblocks))
}

/// `smgr_readv` -> `mdreadv`.
pub fn md_readv(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut [u8]],
    nblocks: BlockNumber,
) -> PgResult<()> {
    md_run(key, |reln, st| mdreadv(reln, st, forknum, blocknum, buffers, nblocks))
}

/// `smgr_writev` -> `mdwritev`.
pub fn md_writev(
    key: RelFileLocatorBackend,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[&[u8]],
    nblocks: BlockNumber,
    skip_fsync: bool,
) -> PgResult<()> {
    md_run(key, |reln, st| mdwritev(reln, st, forknum, blocknum, buffers, nblocks, skip_fsync))
}

/// `smgr_writeback` -> `mdwriteback`.
pub fn md_writeback(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber, nblocks: BlockNumber) -> PgResult<()> {
    md_run(key, |reln, st| mdwriteback(reln, st, forknum, blocknum, nblocks))
}

/// `smgr_nblocks` -> `mdnblocks`.
pub fn md_nblocks(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<BlockNumber> {
    md_run(key, |reln, st| mdnblocks(reln, st, forknum))
}

/// `smgr_truncate` -> `mdtruncate`.
pub fn md_truncate(key: RelFileLocatorBackend, forknum: ForkNumber, curnblk: BlockNumber, nblocks: BlockNumber) -> PgResult<()> {
    md_run(key, |reln, st| mdtruncate(reln, st, forknum, curnblk, nblocks))
}

/// `smgr_registersync` -> `mdregistersync`.
pub fn md_registersync(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    md_run(key, |reln, st| mdregistersync(reln, st, forknum))
}

/// `smgr_immedsync` -> `mdimmedsync`.
pub fn md_immedsync(key: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<()> {
    md_run(key, |reln, st| mdimmedsync(reln, st, forknum))
}

/// `smgr_fd` -> `mdfd`.
pub fn md_fd(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber) -> PgResult<(i32, u32)> {
    md_run(key, |reln, st| mdfd(reln, st, forknum, blocknum))
}

/// `smgr_maxcombine` -> `mdmaxcombine` (pure; no cache state needed, but the
/// reln is required by the signature — use a default boundary view).
pub fn md_maxcombine(key: RelFileLocatorBackend, forknum: ForkNumber, blocknum: BlockNumber) -> u32 {
    let reln = SMgrRelationData::new(key);
    mdmaxcombine(&reln, forknum, blocknum)
}

// ===========================================================================
// init_seams() — install the md sync-handler callbacks (syncsw[SYNC_HANDLER_MD])
// consumed by sync.c.
// ===========================================================================

/// Install every seam this unit OWNS (`backend-storage-smgr-md-seams`).
pub fn init_seams() {
    md_seam::mdsyncfiletag::set(mdsyncfiletag);
    md_seam::mdunlinkfiletag::set(mdunlinkfiletag);
    md_seam::mdfiletagmatches::set(mdfiletagmatches);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rl(db: Oid) -> RelFileLocator {
        RelFileLocator { spcOid: 1, dbOid: db, relNumber: 16384 }
    }
    fn rlb(db: Oid, backend: ProcNumber) -> RelFileLocatorBackend {
        RelFileLocatorBackend { locator: rl(db), backend }
    }

    #[test]
    fn relseg_size_is_one_gb_of_blocks() {
        // 1 GiB / 8 KiB = 131072 blocks per segment.
        assert_eq!(RELSEG_SIZE, 131072);
    }

    #[test]
    fn extension_flag_values_match_c() {
        assert_eq!(EXTENSION_FAIL, 1);
        assert_eq!(EXTENSION_RETURN_NULL, 2);
        assert_eq!(EXTENSION_CREATE, 4);
        assert_eq!(EXTENSION_CREATE_RECOVERY, 8);
        assert_eq!(EXTENSION_DONT_OPEN, 32);
    }

    #[test]
    fn mdmaxcombine_is_blocks_to_end_of_segment() {
        let reln = SMgrRelationData::new(rlb(5, INVALID_PROC_NUMBER));
        // At a segment boundary: a full segment can be combined.
        assert_eq!(mdmaxcombine(&reln, ForkNumber::MAIN_FORKNUM, 0), RELSEG_SIZE);
        // One block in: one fewer.
        assert_eq!(mdmaxcombine(&reln, ForkNumber::MAIN_FORKNUM, 1), RELSEG_SIZE - 1);
        // Last block of a segment: exactly one.
        assert_eq!(mdmaxcombine(&reln, ForkNumber::MAIN_FORKNUM, RELSEG_SIZE - 1), 1);
        // First block of the second segment: a full segment again.
        assert_eq!(mdmaxcombine(&reln, ForkNumber::MAIN_FORKNUM, RELSEG_SIZE), RELSEG_SIZE);
    }

    #[test]
    fn mdfiletagmatches_compares_dboid() {
        let a = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, ForkNumber::MAIN_FORKNUM, rl(10), 0);
        let same_db = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, ForkNumber::FSM_FORKNUM, rl(10), 7);
        let diff_db = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, ForkNumber::MAIN_FORKNUM, rl(11), 0);
        assert_eq!(mdfiletagmatches(a, same_db).unwrap(), true);
        assert_eq!(mdfiletagmatches(a, diff_db).unwrap(), false);
    }

    #[test]
    fn is_temp_follows_backend() {
        assert!(!is_temp(rlb(1, INVALID_PROC_NUMBER)));
        assert!(is_temp(rlb(1, 3)));
    }
}
