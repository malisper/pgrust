#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! src/backend/storage/smgr/md.c — magnetic-disk storage manager: 1 GB
//! (`RELSEG_SIZE`) segment fan-out per fork over fd.c VFDs. State (`MdRelnState`)
//! lives in smgr's handle cache and is threaded in by the dispatch layer.

use std::io::IoSliceMut;
use std::mem::MaybeUninit;

use ::elog::{ereport, ErrorBuilder};
use ::types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, MaxBlockNumber, INVALID_PROC_NUMBER, MAX_FORKNUM,
};
use ::types_core::{Oid, BLCKSZ};
use ::types_error::{
    ErrorLocation, PgResult, DEBUG1, ERROR, ERRCODE_DATA_CORRUPTED, ERRCODE_DISK_FULL,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, FATAL, WARNING,
};
use ::types_storage::file::{File, FILE_EXTEND_METHOD_WRITE_ZEROS, IO_DIRECT_DATA};
use ::types_storage::smgr::{
    MdRelnState, MdfdVec, EXTENSION_CREATE, EXTENSION_CREATE_RECOVERY, EXTENSION_DONT_OPEN,
    EXTENSION_FAIL, EXTENSION_RETURN_NULL, PG_IOV_MAX, RELSEG_SIZE, SMGR_NFORKS,
};
use ::types_storage::sync::{FileTag, FileTagOpResult, SyncRequestHandler, SyncRequestType};
use ::types_storage::{RelFileLocator, RelFileLocatorBackend, WriteChunk};

pub mod nblocks_cache;

const PG_WAIT_IO: u32 = 0x0A00_0000;
// WaitEventIO ordinals (wait_event_names.txt, alphabetical within section).
const WAIT_EVENT_DATA_FILE_EXTEND: u32 = PG_WAIT_IO + 17;
const WAIT_EVENT_DATA_FILE_FLUSH: u32 = PG_WAIT_IO + 18;
const WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC: u32 = PG_WAIT_IO + 19;
const WAIT_EVENT_DATA_FILE_PREFETCH: u32 = PG_WAIT_IO + 20;
const WAIT_EVENT_DATA_FILE_READ: u32 = PG_WAIT_IO + 21;
pub const WAIT_EVENT_DATA_FILE_SYNC: u32 = PG_WAIT_IO + 22;
const WAIT_EVENT_DATA_FILE_TRUNCATE: u32 = PG_WAIT_IO + 23;
const WAIT_EVENT_DATA_FILE_WRITE: u32 = PG_WAIT_IO + 24;

const BLCKSZ_I64: i64 = BLCKSZ as i64;
const ENOENT: i32 = libc::ENOENT;
const ENOSPC: i32 = libc::ENOSPC;

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

pub fn last_errno() -> i32 {
    // SAFETY: reading the thread-local errno.
    unsafe { *errno_location() }
}

pub fn set_errno(value: i32) {
    // SAFETY: writing the thread-local errno.
    unsafe {
        *errno_location() = value;
    }
}

// bufmgr owns track_io_timing's backing; unread until its install runs.
pub fn track_io_timing() -> bool {
    guc_tables::vars::track_io_timing.installed() && guc_tables::vars::track_io_timing.read()
}

#[inline]
fn file_possibly_deleted(err: i32) -> bool {
    err == ENOENT
}

#[inline]
fn fork_idx(forknum: ForkNumber) -> usize {
    forknum as usize
}

#[inline]
fn is_temp(rlocator: RelFileLocatorBackend) -> bool {
    rlocator.backend != INVALID_PROC_NUMBER
}

#[inline]
fn in_recovery() -> bool {
    xlogutils::in_recovery()
}

#[inline]
fn io_direct_data() -> bool {
    (fd::io_direct_flags() & IO_DIRECT_DATA) != 0
}

#[inline]
fn relpath(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> String {
    relpath_seams::relpathbackend::call(rlocator.locator, rlocator.backend, forknum)
}

pub fn fork_iter() -> [ForkNumber; SMGR_NFORKS] {
    [
        ForkNumber::MAIN_FORKNUM,
        ForkNumber::FSM_FORKNUM,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        ForkNumber::INIT_FORKNUM,
    ]
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

#[cold]
#[inline(never)]
fn throw<T>(b: ErrorBuilder, funcname: &'static str) -> PgResult<T> {
    b.finish(loc(funcname))?;
    unreachable!("finish at >= ERROR always returns Err")
}

#[inline]
pub fn mdopenflags() -> i32 {
    let mut flags = libc::O_RDWR;
    if io_direct_data() {
        flags |= fd::vfd::PG_O_DIRECT;
    }
    flags
}

pub fn mdinit() -> PgResult<()> {
    // MdCxt is only for MdfdVec arrays; those live in the smgr cache entry here.
    Ok(())
}

pub fn mdopen(st: &mut MdRelnState) {
    for forknum in 0..=fork_idx(MAX_FORKNUM) {
        st.md_num_open_segs[forknum] = 0;
    }
}

pub fn mdexists(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
) -> PgResult<bool> {
    // Close first so a concurrent unlink is noticed; skipped in recovery.
    if !in_recovery() {
        mdclose(st, forknum)?;
    }
    Ok(mdopenfork(rlocator, st, forknum, EXTENSION_RETURN_NULL)?.is_some())
}

pub fn mdcreate(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    is_redo: bool,
) -> PgResult<()> {
    let fk = fork_idx(forknum);

    if is_redo && st.md_num_open_segs[fk] > 0 {
        return Ok(());
    }
    debug_assert!(st.md_num_open_segs[fk] == 0);

    tablespace_seams::tablespace_create_dbspace::call(
        rlocator.locator.spcOid,
        rlocator.locator.dbOid,
        is_redo,
    )?;

    let path = relpath(rlocator, forknum);
    let mut file = fd::PathNameOpenFile(&path, mdopenflags() | libc::O_CREAT | libc::O_EXCL)?;
    if file.0 < 0 {
        let save_errno = last_errno();
        if is_redo {
            file = fd::PathNameOpenFile(&path, mdopenflags())?;
        }
        if file.0 < 0 {
            // report the error reported by create, not open
            return throw(
                ereport(ERROR)
                    .with_saved_errno(save_errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not create file \"{path}\": %m")),
                "mdcreate",
            );
        }
    }

    _fdvec_resize(st, forknum, 1);
    st.md_seg_fds[fk][0] = MdfdVec { mdfd_vfd: file, mdfd_segno: 0 };

    if !is_temp(rlocator) {
        // Fresh file identity: drop any stale size-cache entry (including a
        // columnar poison — a reused relfilenumber starts clean; the next
        // read walks, the next columnar writer open re-poisons).
        nblocks_cache::remove(rlocator.locator, forknum);
        let seg = st.md_seg_fds[fk][0];
        register_dirty_segment(rlocator, forknum, seg)?;
    }
    Ok(())
}

pub fn mdunlink(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    is_redo: bool,
) -> PgResult<()> {
    if forknum == ForkNumber::InvalidForkNumber {
        for fork in fork_iter() {
            mdunlinkfork(rlocator, fork, is_redo)?;
        }
    } else {
        mdunlinkfork(rlocator, forknum, is_redo)?;
    }
    Ok(())
}

fn do_truncate(path: &str) -> PgResult<i32> {
    let ret = pg_truncate_raw(path, 0);
    if ret < 0 && last_errno() != ENOENT {
        let save_errno = last_errno();
        ereport(WARNING)
            .with_saved_errno(save_errno)
            .errcode_for_file_access()
            .errmsg(format!("could not truncate file \"{path}\": %m"))
            .finish(loc("do_truncate"))?;
        set_errno(save_errno);
    }
    Ok(ret)
}

fn mdunlinkfork(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    is_redo: bool,
) -> PgResult<()> {
    // Drop the size-cache entry (poison included) BEFORE touching the file:
    // any racing reader then walks the real file and sees exactly what lseek
    // sees at its instant of the race — the same nondeterminism C has.
    if !is_temp(rlocator) {
        nblocks_cache::remove(rlocator.locator, forknum);
    }
    let path = relpath(rlocator, forknum);
    let mut ret: i32;

    if is_redo
        || init_small::globals::IsBinaryUpgrade()
        || forknum != ForkNumber::MAIN_FORKNUM
        || is_temp(rlocator)
    {
        if !is_temp(rlocator) {
            ret = do_truncate(&path)?;

            let save_errno = last_errno();
            register_forget_request(rlocator, forknum, 0)?;
            set_errno(save_errno);
        } else {
            ret = 0;
        }

        if ret >= 0 || last_errno() != ENOENT {
            ret = unlink_raw(&path);
            if ret < 0 && last_errno() != ENOENT {
                let save_errno = last_errno();
                ereport(WARNING)
                    .with_saved_errno(save_errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not remove file \"{path}\": %m"))
                    .finish(loc("mdunlinkfork"))?;
                set_errno(save_errno);
            }
        }
    } else {
        // Main fork at commit: truncate now, unlink after the next checkpoint.
        ret = do_truncate(&path)?;

        let save_errno = last_errno();
        register_unlink_segment(rlocator, forknum, 0)?;
        set_errno(save_errno);
    }

    if ret >= 0 || last_errno() != ENOENT {
        // Delete additional segments until ENOENT (inactive segments included).
        let mut segno: BlockNumber = 1;
        loop {
            let segpath = format!("{path}.{segno}");

            if !is_temp(rlocator) {
                if do_truncate(&segpath)? < 0 && last_errno() == ENOENT {
                    break;
                }
                register_forget_request(rlocator, forknum, segno)?;
            }

            if unlink_raw(&segpath) < 0 {
                if last_errno() != ENOENT {
                    ereport(WARNING)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!("could not remove file \"{segpath}\": %m"))
                        .finish(loc("mdunlinkfork"))?;
                }
                break;
            }
            segno += 1;
        }
    }
    Ok(())
}

pub fn mdextend(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    let r = mdextend_inner(rlocator, st, forknum, blocknum, buffer, skip_fsync);
    if !is_temp(rlocator) {
        match &r {
            // The file now reaches at least blocknum+1 (intermediate segments
            // were zero-filled by _mdfd_getseg under the same call).
            Ok(()) => nblocks_cache::extend_to(rlocator.locator, forknum, blocknum + 1),
            Err(_) => nblocks_cache::invalidate(rlocator.locator, forknum),
        }
    }
    r
}

fn mdextend_inner(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &[u8],
    skip_fsync: bool,
) -> PgResult<()> {
    if blocknum == InvalidBlockNumber {
        let path = relpath(rlocator, forknum);
        return throw(
            ereport(ERROR).errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED).errmsg(format!(
                "cannot extend file \"{path}\" beyond {InvalidBlockNumber} blocks"
            )),
            "mdextend",
        );
    }

    let v = _mdfd_getseg(rlocator, st, forknum, blocknum, skip_fsync, EXTENSION_CREATE)?
        .expect("EXTENSION_CREATE never returns None");

    let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;
    debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

    let iov = [WriteChunk::from_slice(buffer)];
    let nbytes = fd::FileWriteV(v.mdfd_vfd, &iov, seekpos, WAIT_EVENT_DATA_FILE_EXTEND)?;
    if nbytes != BLCKSZ as isize {
        if nbytes < 0 {
            return throw(
                ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not extend file \"{}\": %m",
                        fd::FilePathName(v.mdfd_vfd)
                    ))
                    .errhint("Check free disk space."),
                "mdextend",
            );
        }
        return throw(
            ereport(ERROR)
                .errcode(ERRCODE_DISK_FULL)
                .errmsg(format!(
                    "could not extend file \"{}\": wrote only {} of {} bytes at block {}",
                    fd::FilePathName(v.mdfd_vfd),
                    nbytes,
                    BLCKSZ,
                    blocknum
                ))
                .errhint("Check free disk space."),
            "mdextend",
        );
    }

    if !skip_fsync && !is_temp(rlocator) {
        register_dirty_segment(rlocator, forknum, v)?;
    }
    debug_assert!(_mdnblocks(&v)? <= RELSEG_SIZE);
    Ok(())
}

pub fn mdzeroextend(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) -> PgResult<()> {
    let r = mdzeroextend_inner(rlocator, st, forknum, blocknum, nblocks, skip_fsync);
    if !is_temp(rlocator) {
        match &r {
            Ok(()) => match cache_extent_after(blocknum, nblocks) {
                Some(end) => nblocks_cache::extend_to(rlocator.locator, forknum, end),
                // Not representable as a block count: do exactly what the
                // error arm does and forget the size, so the next read
                // measures the real file. Never publish a guessed value here —
                // extend_to() only ever RAISES, so a too-high entry is
                // permanent until something invalidates it.
                None => nblocks_cache::invalidate(rlocator.locator, forknum),
            },
            Err(_) => nblocks_cache::invalidate(rlocator.locator, forknum),
        }
    }
    r
}

/// The relation's block count after a successful `mdzeroextend` of `nblocks`
/// blocks starting at `blocknum`, or `None` when that is not a representable
/// block count — in which case no cached size may be published.
///
/// Checked rather than wrapping, and the reason is specific. `nblocks` is C's
/// `int` (`md.c` `mdzeroextend`), so the obvious `blocknum + nblocks as
/// BlockNumber` wraps whenever `nblocks` is negative. The dangerous direction
/// is `blocknum + nblocks < 0`, where it wraps **upward**: `(0, -1)` lands
/// exactly on `InvalidBlockNumber`, and `(5, -10)` on 4294967291. Because
/// `nblocks_cache::extend_to` only ever raises the cached size, publishing one
/// of those means every later size read reports a relation *longer than its
/// file*, i.e. past-EOF blocks served as valid. C cannot express this defect:
/// it keeps no cross-backend size cache, so out-of-contract `nblocks` there is
/// simply a loop that does not run.
///
/// Two further notes worth keeping next to the arithmetic:
///
/// * `overflow-checks` would **not** have caught the dangerous cases. They stay
///   inside `u32` (`0 + 0xFFFFFFFF == 0xFFFFFFFF`), so a trapping build traps
///   only the *harmless* downward cases such as `(1000, -1) -> 999`. Checked
///   arithmetic on a wider signed type is the only thing that sees them.
/// * `nblocks <= 0` is out of contract (`mdzeroextend_inner` asserts
///   `nblocks > 0`), and this returns `None` for it rather than the technically
///   truthful `blocknum`, so the contract stays "Some only for a well-formed
///   extension" and the caller's response to anything else is uniform.
fn cache_extent_after(blocknum: BlockNumber, nblocks: i32) -> Option<BlockNumber> {
    if nblocks <= 0 {
        return None;
    }
    let end = i64::from(blocknum) + i64::from(nblocks);
    // mdzeroextend_inner rejects this range itself; keep the helper total so it
    // is correct read on its own.
    if end >= i64::from(InvalidBlockNumber) {
        return None;
    }
    Some(end as BlockNumber)
}

fn mdzeroextend_inner(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) -> PgResult<()> {
    let mut curblocknum = blocknum;
    let mut remblocks = nblocks;

    debug_assert!(nblocks > 0);

    if blocknum as u64 + nblocks as u64 >= InvalidBlockNumber as u64 {
        let path = relpath(rlocator, forknum);
        return throw(
            ereport(ERROR).errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED).errmsg(format!(
                "cannot extend file \"{path}\" beyond {InvalidBlockNumber} blocks"
            )),
            "mdzeroextend",
        );
    }

    while remblocks > 0 {
        let segstartblock = curblocknum % RELSEG_SIZE;
        let seekpos = BLCKSZ_I64 * segstartblock as i64;
        let numblocks: i32 = if (segstartblock as i64 + remblocks as i64) > RELSEG_SIZE as i64 {
            (RELSEG_SIZE - segstartblock) as i32
        } else {
            remblocks
        };

        let v = _mdfd_getseg(rlocator, st, forknum, curblocknum, skip_fsync, EXTENSION_CREATE)?
            .expect("EXTENSION_CREATE never returns None");

        debug_assert!(segstartblock < RELSEG_SIZE);
        debug_assert!(segstartblock + numblocks as BlockNumber <= RELSEG_SIZE);

        // Fallocate for larger extensions (cutoff 8 blocks per md.c), FileZero
        // otherwise or when file_extend_method says write zeroes.
        if numblocks > 8 && fd::vfd::file_extend_method() != FILE_EXTEND_METHOD_WRITE_ZEROS {
            let ret = fd::FileFallocate(
                v.mdfd_vfd,
                seekpos,
                BLCKSZ_I64 * numblocks as i64,
                WAIT_EVENT_DATA_FILE_EXTEND,
            )?;
            if ret != 0 {
                return throw(
                    ereport(ERROR)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not extend file \"{}\" with FileFallocate(): %m",
                            fd::FilePathName(v.mdfd_vfd)
                        ))
                        .errhint("Check free disk space."),
                    "mdzeroextend",
                );
            }
        } else {
            let ret = fd::FileZero(
                v.mdfd_vfd,
                seekpos,
                BLCKSZ_I64 * numblocks as i64,
                WAIT_EVENT_DATA_FILE_EXTEND,
            )?;
            if ret < 0 {
                return throw(
                    ereport(ERROR)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not extend file \"{}\": %m",
                            fd::FilePathName(v.mdfd_vfd)
                        ))
                        .errhint("Check free disk space."),
                    "mdzeroextend",
                );
            }
        }

        if !skip_fsync && !is_temp(rlocator) {
            register_dirty_segment(rlocator, forknum, v)?;
        }
        debug_assert!(_mdnblocks(&v)? <= RELSEG_SIZE);

        remblocks -= numblocks;
        curblocknum += numblocks as BlockNumber;
    }
    Ok(())
}

fn mdopenfork(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    behavior: i32,
) -> PgResult<Option<MdfdVec>> {
    let fk = fork_idx(forknum);

    if st.md_num_open_segs[fk] > 0 {
        return Ok(Some(st.md_seg_fds[fk][0]));
    }

    let path = relpath(rlocator, forknum);
    let file = fd::PathNameOpenFile(&path, mdopenflags())?;
    if file.0 < 0 {
        if (behavior & EXTENSION_RETURN_NULL) != 0 && file_possibly_deleted(last_errno()) {
            return Ok(None);
        }
        return throw(
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m")),
            "mdopenfork",
        );
    }

    _fdvec_resize(st, forknum, 1);
    st.md_seg_fds[fk][0] = MdfdVec { mdfd_vfd: file, mdfd_segno: 0 };

    let mdfd = st.md_seg_fds[fk][0];
    debug_assert!(_mdnblocks(&mdfd)? <= RELSEG_SIZE);
    Ok(Some(mdfd))
}

pub fn mdclose(st: &mut MdRelnState, forknum: ForkNumber) -> PgResult<()> {
    let fk = fork_idx(forknum);
    let mut nopensegs = st.md_num_open_segs[fk];

    // Close segments from the end (mirrors md.c; failures surface per close).
    while nopensegs > 0 {
        let v = st.md_seg_fds[fk][(nopensegs - 1) as usize];
        fd::FileClose(v.mdfd_vfd)?;
        _fdvec_resize(st, forknum, nopensegs - 1);
        nopensegs -= 1;
    }
    Ok(())
}

pub fn mdprefetch(
    rlocator: RelFileLocatorBackend,
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
        let behavior = if in_recovery() { EXTENSION_RETURN_NULL } else { EXTENSION_FAIL };
        let v = match _mdfd_getseg(rlocator, st, forknum, blocknum, false, behavior)? {
            Some(v) => v,
            None => return Ok(false),
        };

        let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;
        debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

        let nblocks_this_segment =
            core::cmp::min(nblocks as i64, (RELSEG_SIZE - (blocknum % RELSEG_SIZE)) as i64) as i32;

        let _ = fd::FilePrefetch(
            v.mdfd_vfd,
            seekpos,
            BLCKSZ_I64 * nblocks_this_segment as i64,
            WAIT_EVENT_DATA_FILE_PREFETCH,
        )?;

        blocknum += nblocks_this_segment as BlockNumber;
        nblocks -= nblocks_this_segment;
    }
    Ok(true)
}

pub fn mdstartbufread(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: i32,
) -> PgResult<bool> {
    let behavior = if in_recovery() { EXTENSION_RETURN_NULL } else { EXTENSION_FAIL };
    let v = match _mdfd_getseg(rlocator, st, forknum, blocknum, false, behavior)? {
        Some(v) => v,
        None => return Ok(false),
    };
    let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;
    debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);
    fd::FileStartBufferRead(v.mdfd_vfd, seekpos, buffer)
}

pub fn mdmaxcombine(blocknum: BlockNumber) -> u32 {
    RELSEG_SIZE - (blocknum % RELSEG_SIZE)
}

// C's iovec array is stack-local (md.c buffers_to_iovec); same here — a
// MaybeUninit prefix avoids both heap traffic and 2 KB zero-init per I/O.
fn with_iov_mut<R>(
    seg_bufs: &mut [&mut [u8]],
    skip: usize,
    f: impl FnOnce(&mut [IoSliceMut<'_>]) -> R,
) -> Option<R> {
    let mut skip = skip;
    let mut start = 0usize;
    while start < seg_bufs.len() && skip >= seg_bufs[start].len() {
        skip -= seg_bufs[start].len();
        start += 1;
    }
    if start >= seg_bufs.len() {
        return None;
    }
    debug_assert!(seg_bufs.len() - start <= PG_IOV_MAX);
    let mut iov: [MaybeUninit<IoSliceMut<'_>>; PG_IOV_MAX] =
        [const { MaybeUninit::uninit() }; PG_IOV_MAX];
    let mut n = 0usize;
    for (i, b) in seg_bufs[start..].iter_mut().enumerate() {
        let s: &mut [u8] = if i == 0 { &mut b[skip..] } else { &mut b[..] };
        iov[n] = MaybeUninit::new(IoSliceMut::new(s));
        n += 1;
    }
    // SAFETY: the first n entries were just initialized; IoSliceMut has no Drop.
    let iov_init =
        unsafe { core::slice::from_raw_parts_mut(iov.as_mut_ptr().cast::<IoSliceMut<'_>>(), n) };
    Some(f(iov_init))
}

fn with_iov<'a, R>(
    seg_bufs: &[WriteChunk<'a>],
    skip: usize,
    f: impl FnOnce(&[WriteChunk<'a>]) -> R,
) -> Option<R> {
    let mut skip = skip;
    let mut start = 0usize;
    while start < seg_bufs.len() && skip >= seg_bufs[start].len() {
        skip -= seg_bufs[start].len();
        start += 1;
    }
    if start >= seg_bufs.len() {
        return None;
    }
    debug_assert!(seg_bufs.len() - start <= PG_IOV_MAX);
    let mut iov: [MaybeUninit<WriteChunk<'a>>; PG_IOV_MAX] =
        [const { MaybeUninit::uninit() }; PG_IOV_MAX];
    let mut n = 0usize;
    for (i, b) in seg_bufs[start..].iter().enumerate() {
        iov[n] = MaybeUninit::new(if i == 0 { b.advance(skip) } else { *b });
        n += 1;
    }
    // SAFETY: the first n entries were just initialized; WriteChunk has no Drop.
    let iov_init =
        unsafe { core::slice::from_raw_parts(iov.as_ptr().cast::<WriteChunk<'a>>(), n) };
    Some(f(iov_init))
}

pub fn mdreadv(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut [u8]],
) -> PgResult<()> {
    let mut blocknum = blocknum;
    let mut nblocks = buffers.len() as BlockNumber;
    let mut buf_off: usize = 0;

    while nblocks > 0 {
        let v = _mdfd_getseg(
            rlocator,
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
            return throw(
                ereport(ERROR).errmsg_internal("read crosses segment boundary"),
                "mdreadv",
            );
        }

        let size_this_segment = nblocks_this_segment as usize * BLCKSZ;
        let mut transferred_this_segment: usize = 0;

        loop {
            let seg_bufs = &mut buffers[buf_off..buf_off + nblocks_this_segment as usize];
            let nbytes = with_iov_mut(seg_bufs, transferred_this_segment, |iov| {
                fd::FileReadV(v.mdfd_vfd, iov, seekpos, WAIT_EVENT_DATA_FILE_READ)
            })
            .unwrap_or(Ok(0))?;

            if nbytes < 0 {
                return throw(
                    ereport(ERROR)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not read blocks {}..{} in file \"{}\": %m",
                            blocknum,
                            blocknum + nblocks_this_segment - 1,
                            fd::FilePathName(v.mdfd_vfd)
                        )),
                    "mdreadv",
                );
            }

            if nbytes == 0 {
                // Past EOF: error unless zero_damaged_pages or InRecovery,
                // where the missing tail reads back as zeroes.
                if guc_tables::vars::zero_damaged_pages.read() || in_recovery() {
                    let start = transferred_this_segment / BLCKSZ;
                    for i in start..nblocks_this_segment as usize {
                        buffers[buf_off + i].fill(0);
                    }
                    break;
                }
                return throw(
                    ereport(ERROR).errcode(ERRCODE_DATA_CORRUPTED).errmsg(format!(
                        "could not read blocks {}..{} in file \"{}\": read only {} of {} bytes",
                        blocknum,
                        blocknum + nblocks_this_segment - 1,
                        fd::FilePathName(v.mdfd_vfd),
                        transferred_this_segment,
                        size_this_segment
                    )),
                    "mdreadv",
                );
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

pub fn mdwritev(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[WriteChunk<'_>],
    skip_fsync: bool,
) -> PgResult<()> {
    let mut blocknum = blocknum;
    let mut nblocks = buffers.len() as BlockNumber;
    let mut buf_off: usize = 0;

    while nblocks > 0 {
        let v = _mdfd_getseg(
            rlocator,
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
            return throw(
                ereport(ERROR).errmsg_internal("write crosses segment boundary"),
                "mdwritev",
            );
        }

        let size_this_segment = nblocks_this_segment as usize * BLCKSZ;
        let mut transferred_this_segment: usize = 0;

        loop {
            let seg_bufs = &buffers[buf_off..buf_off + nblocks_this_segment as usize];
            let nbytes = with_iov(seg_bufs, transferred_this_segment, |iov| {
                fd::FileWriteV(v.mdfd_vfd, iov, seekpos, WAIT_EVENT_DATA_FILE_WRITE)
            })
            .unwrap_or(Ok(0))?;

            if nbytes < 0 {
                let enospc = last_errno() == ENOSPC;
                let mut b = ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not write blocks {}..{} in file \"{}\": %m",
                        blocknum,
                        blocknum + nblocks_this_segment - 1,
                        fd::FilePathName(v.mdfd_vfd)
                    ));
                if enospc {
                    b = b.errhint("Check free disk space.");
                }
                return throw(b, "mdwritev");
            }

            transferred_this_segment += nbytes as usize;
            debug_assert!(transferred_this_segment <= size_this_segment);
            if transferred_this_segment == size_this_segment {
                break;
            }
            seekpos += nbytes as i64;
        }

        if !skip_fsync && !is_temp(rlocator) {
            register_dirty_segment(rlocator, forknum, v)?;
        }

        // In recovery the CREATE_RECOVERY arm can have re-created a dropped
        // segment and this write then grows it (replay of a rel truncated
        // later in the WAL). The size cache must see that growth; max()
        // semantics make this a no-op for ordinary in-bounds writes.
        if in_recovery() && !is_temp(rlocator) {
            nblocks_cache::extend_to(rlocator.locator, forknum, blocknum + nblocks_this_segment);
        }

        nblocks -= nblocks_this_segment;
        buf_off += nblocks_this_segment as usize;
        blocknum += nblocks_this_segment;
    }
    Ok(())
}

pub fn mdwriteback(
    rlocator: RelFileLocatorBackend,
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

        // Never re-open a closed segment: flushed buffers may belong to
        // already-removed relations.
        let v = match _mdfd_getseg(rlocator, st, forknum, blocknum, true, EXTENSION_DONT_OPEN)? {
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
        fd::FileWriteback(v.mdfd_vfd, seekpos, BLCKSZ_I64 * nflush as i64, WAIT_EVENT_DATA_FILE_FLUSH)?;

        nblocks -= nflush;
        blocknum += nflush;
    }
    Ok(())
}

pub fn mdnblocks(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
) -> PgResult<BlockNumber> {
    if !is_temp(rlocator) {
        if let Some(cached) = nblocks_cache::lookup(rlocator.locator, forknum) {
            if nblocks_validate() {
                validate_cached(rlocator, st, forknum, cached)?;
            }
            return Ok(cached);
        }
    }
    mdnblocks_walk(rlocator, st, forknum, true)
}

/// Cross-check of the size cache against the real lseek walk
/// (`PGRUST_NBLOCKS_VALIDATE=1`): a debugging tool, default off.
fn nblocks_validate() -> bool {
    static ON: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    *ON.get_or_init(|| matches!(std::env::var("PGRUST_NBLOCKS_VALIDATE").as_deref(), Ok("1")))
}

/// Validation-mode arm: a concurrent extension landing between the cache
/// read and the walk makes transient inequality EXPECTED (the same
/// nondeterminism two racing lseeks have); the bug signature is a FROZEN
/// cache value diverging from the real walk. Bounded spin (no sleeps), each
/// probe pair non-publishing so a real staleness cannot self-heal out of
/// sight.
#[cold]
#[inline(never)]
fn validate_cached(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    cached: BlockNumber,
) -> PgResult<()> {
    let mut last = cached;
    let mut walked = 0;
    for _ in 0..2000 {
        walked = mdnblocks_walk(rlocator, st, forknum, false)?;
        match nblocks_cache::lookup(rlocator.locator, forknum) {
            // Entry removed under us (drop/rewrite churn): nothing to check.
            None => return Ok(()),
            Some(c) if c == walked => return Ok(()),
            Some(c) => last = c,
        }
        std::hint::spin_loop();
    }
    panic!(
        "nblocks cache incoherent: cached {last} vs walked {walked} for rel {} fork {}",
        rlocator.locator.relNumber, forknum as u32
    );
}

/// The real probe: lseek(SEEK_END) per segment from the last open one. Side
/// effect relied on by mdregistersync/mdimmedsync/mdtruncate: opens every
/// active segment. With `publish`, the result repopulates the process-global
/// size cache (non-temp relations); the validation arm passes false so a
/// stale entry cannot self-heal before it is caught.
fn mdnblocks_walk(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    publish: bool,
) -> PgResult<BlockNumber> {
    let fk = fork_idx(forknum);

    mdopenfork(rlocator, st, forknum, EXTENSION_FAIL)?;
    debug_assert!(st.md_num_open_segs[fk] > 0);

    let mut segno = (st.md_num_open_segs[fk] - 1) as BlockNumber;
    let mut v = st.md_seg_fds[fk][segno as usize];

    let total = loop {
        let nblocks = _mdnblocks(&v)?;
        if nblocks > RELSEG_SIZE {
            return throw(ereport(FATAL).errmsg_internal("segment too big"), "mdnblocks");
        }
        if nblocks < RELSEG_SIZE {
            break segno * RELSEG_SIZE + nblocks;
        }

        segno += 1;
        match _mdfd_openseg(rlocator, st, forknum, segno, 0)? {
            Some(seg) => v = seg,
            None => break segno * RELSEG_SIZE,
        }
    };
    if publish && !is_temp(rlocator) {
        nblocks_cache::note_walked(rlocator.locator, forknum, total);
    }
    Ok(total)
}

pub fn mdtruncate(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    curnblk: BlockNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    let r = mdtruncate_inner(rlocator, st, forknum, curnblk, nblocks);
    if !is_temp(rlocator) {
        match &r {
            // The recovery replay no-op arm (nblocks > curnblk) changed
            // nothing, so any cached value is still exact — leave it.
            Ok(()) if nblocks <= curnblk => {
                nblocks_cache::set_exact(rlocator.locator, forknum, nblocks)
            }
            Ok(()) => {}
            Err(_) => nblocks_cache::invalidate(rlocator.locator, forknum),
        }
    }
    r
}

fn mdtruncate_inner(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    curnblk: BlockNumber,
    nblocks: BlockNumber,
) -> PgResult<()> {
    let fk = fork_idx(forknum);

    if nblocks > curnblk {
        // Bogus request, unless replaying WAL on a standby that already truncated.
        if in_recovery() {
            return Ok(());
        }
        let path = relpath(rlocator, forknum);
        return throw(
            ereport(ERROR).errmsg(format!(
                "could not truncate file \"{path}\" to {nblocks} blocks: it's only {curnblk} blocks now"
            )),
            "mdtruncate",
        );
    }
    if nblocks == curnblk {
        return Ok(());
    }

    // C's contract makes the CALLER's smgrnblocks open every active segment
    // so the loop below sees them all; with the size cache that call may not
    // walk, so reinstate the walk here (same lseek pattern, rare path).
    mdnblocks_walk(rlocator, st, forknum, true)?;

    let mut curopensegs = st.md_num_open_segs[fk];
    while curopensegs > 0 {
        let priorblocks = (curopensegs - 1) as BlockNumber * RELSEG_SIZE;
        let v = st.md_seg_fds[fk][(curopensegs - 1) as usize];

        if priorblocks > nblocks {
            // Fully inactive segment: truncate to zero, keep the file.
            if fd::FileTruncate(v.mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE)? < 0 {
                return throw(
                    ereport(ERROR)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not truncate file \"{}\": %m",
                            fd::FilePathName(v.mdfd_vfd)
                        )),
                    "mdtruncate",
                );
            }
            if !is_temp(rlocator) {
                register_dirty_segment(rlocator, forknum, v)?;
            }
            debug_assert!(curopensegs - 1 != 0);
            fd::FileClose(v.mdfd_vfd)?;
            _fdvec_resize(st, forknum, curopensegs - 1);
        } else if priorblocks + RELSEG_SIZE > nblocks {
            // Last segment to keep: truncate to the remainder.
            let lastsegblocks = nblocks - priorblocks;
            if fd::FileTruncate(
                v.mdfd_vfd,
                lastsegblocks as i64 * BLCKSZ_I64,
                WAIT_EVENT_DATA_FILE_TRUNCATE,
            )? < 0
            {
                return throw(
                    ereport(ERROR)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not truncate file \"{}\" to {} blocks: %m",
                            fd::FilePathName(v.mdfd_vfd),
                            nblocks
                        )),
                    "mdtruncate",
                );
            }
            if !is_temp(rlocator) {
                register_dirty_segment(rlocator, forknum, v)?;
            }
        } else {
            // Earlier segments are all retained as-is.
            break;
        }
        curopensegs -= 1;
    }
    Ok(())
}

pub fn mdregistersync(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
) -> PgResult<()> {
    let fk = fork_idx(forknum);

    // The walk (not the cached read: the SIDE EFFECT is the point) opens all
    // active segments; probe further for inactive ones.
    mdnblocks_walk(rlocator, st, forknum, true)?;

    let min_inactive_seg = st.md_num_open_segs[fk];
    let mut segno = min_inactive_seg;

    while _mdfd_openseg(rlocator, st, forknum, segno as BlockNumber, 0)?.is_some() {
        segno += 1;
    }

    while segno > 0 {
        let v = st.md_seg_fds[fk][(segno - 1) as usize];
        register_dirty_segment(rlocator, forknum, v)?;
        if segno > min_inactive_seg {
            fd::FileClose(v.mdfd_vfd)?;
            _fdvec_resize(st, forknum, segno - 1);
        }
        segno -= 1;
    }
    Ok(())
}

pub fn mdimmedsync(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
) -> PgResult<()> {
    let fk = fork_idx(forknum);

    // Walk, not the cached read: the open-all-active-segments side effect
    // is required so the fsync loop below reaches every segment.
    mdnblocks_walk(rlocator, st, forknum, true)?;

    let min_inactive_seg = st.md_num_open_segs[fk];
    let mut segno = min_inactive_seg;

    while _mdfd_openseg(rlocator, st, forknum, segno as BlockNumber, 0)?.is_some() {
        segno += 1;
    }

    while segno > 0 {
        let v = st.md_seg_fds[fk][(segno - 1) as usize];

        if file_sync_failed(v.mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC)? {
            return throw(
                ereport(fd::data_sync_elevel(ERROR))
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not fsync file \"{}\": %m",
                        fd::FilePathName(v.mdfd_vfd)
                    )),
                "mdimmedsync",
            );
        }

        if segno > min_inactive_seg {
            fd::FileClose(v.mdfd_vfd)?;
            _fdvec_resize(st, forknum, segno - 1);
        }
        segno -= 1;
    }
    Ok(())
}

pub fn mdfd(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
) -> PgResult<(i32, u32)> {
    let _ = mdopenfork(rlocator, st, forknum, EXTENSION_FAIL)?;

    let v = _mdfd_getseg(rlocator, st, forknum, blocknum, false, EXTENSION_FAIL)?
        .expect("EXTENSION_FAIL ereports rather than returning None");

    let off = (BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64) as u32;
    debug_assert!((off as i64) < BLCKSZ_I64 * RELSEG_SIZE as i64);

    let raw = fd::FileGetRawDesc(v.mdfd_vfd)?;
    Ok((raw, off))
}

fn register_dirty_segment(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    seg: MdfdVec,
) -> PgResult<()> {
    debug_assert!(!is_temp(rlocator));

    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        forknum,
        rlocator.locator,
        seg.mdfd_segno as u64,
    );

    if !sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_REQUEST, false)? {
        // Checkpointer queue full: fsync it ourselves, as md.c does.
        ereport(DEBUG1)
            .errmsg_internal("could not forward fsync request because request queue is full")
            .finish(loc("register_dirty_segment"))?;

        let io_start = pgstat::io::pgstat_prepare_io_time(track_io_timing());

        if file_sync_failed(seg.mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC)? {
            return throw(
                ereport(fd::data_sync_elevel(ERROR))
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not fsync file \"{}\": %m",
                        fd::FilePathName(seg.mdfd_vfd)
                    )),
                "register_dirty_segment",
            );
        }

        pgstat::io::pgstat_count_io_op_time(
            pgstat::io::IOObject::Relation,
            pgstat::io::IOContext::IOCONTEXT_NORMAL,
            pgstat::io::IOOp::Fsync,
            io_start,
            1,
            0,
        );
    }
    Ok(())
}

fn register_unlink_segment(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    segno: BlockNumber,
) -> PgResult<()> {
    debug_assert!(!is_temp(rlocator));

    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        forknum,
        rlocator.locator,
        segno as u64,
    );
    sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_UNLINK_REQUEST, true)?;
    Ok(())
}

fn register_forget_request(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    segno: BlockNumber,
) -> PgResult<()> {
    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        forknum,
        rlocator.locator,
        segno as u64,
    );
    sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_FORGET_REQUEST, true)?;
    Ok(())
}

pub fn ForgetDatabaseSyncRequests(dbid: Oid) -> PgResult<()> {
    let rlocator = RelFileLocator { spcOid: 0, dbOid: dbid, relNumber: 0 };
    let tag = FileTag::new(
        SyncRequestHandler::SYNC_HANDLER_MD,
        ForkNumber::InvalidForkNumber,
        rlocator,
        InvalidBlockNumber as u64,
    );
    sync_seams::register_sync_request::call(tag, SyncRequestType::SYNC_FILTER_REQUEST, true)?;
    Ok(())
}

pub fn mdunlinkfiletag(ftag: FileTag) -> PgResult<FileTagOpResult> {
    // The checkpointer's deferred unlink of a dropped rel's tombstone file.
    // A size probe between the drop and this point legitimately re-cached
    // the 0-block tombstone (C would have lseek'd it too); once the file is
    // gone the entry must go with it or a later probe would get 0 where C
    // gets ENOENT.
    nblocks_cache::remove(ftag.rlocator, ForkNumber::MAIN_FORKNUM);
    let path = relpath_seams::relpathperm::call(ftag.rlocator, ForkNumber::MAIN_FORKNUM);
    let ret = unlink_raw(&path);
    Ok(FileTagOpResult { result: ret, errno: last_errno(), path })
}

pub fn mdfiletagmatches(ftag: FileTag, candidate: FileTag) -> bool {
    // SYNC_FILTER_REQUEST from ForgetDatabaseSyncRequests: match on dbOid.
    ftag.rlocator.dbOid == candidate.rlocator.dbOid
}

fn _fdvec_resize(st: &mut MdRelnState, forknum: ForkNumber, nseg: i32) {
    let fk = fork_idx(forknum);
    let v = &mut st.md_seg_fds[fk];
    if nseg as usize > v.len() {
        // Vec keeps high-water capacity on truncate, so mdtruncate never allocates.
        v.resize(nseg as usize, MdfdVec::default());
    } else {
        v.truncate(nseg as usize);
    }
    st.md_num_open_segs[fk] = nseg;
}

pub fn mdsegpath(rlocator: RelFileLocatorBackend, forknum: ForkNumber, segno: BlockNumber) -> String {
    let path = relpath(rlocator, forknum);
    if segno > 0 {
        format!("{path}.{segno}")
    } else {
        path
    }
}

fn _mdfd_openseg(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    segno: BlockNumber,
    oflags: i32,
) -> PgResult<Option<MdfdVec>> {
    let fk = fork_idx(forknum);
    let fullpath = mdsegpath(rlocator, forknum, segno);

    let file = fd::PathNameOpenFile(&fullpath, mdopenflags() | oflags)?;
    if file.0 < 0 {
        return Ok(None);
    }

    // Segments open strictly in order; the new one lands at the end.
    debug_assert!(segno == st.md_num_open_segs[fk] as BlockNumber);
    _fdvec_resize(st, forknum, segno as i32 + 1);
    st.md_seg_fds[fk][segno as usize] = MdfdVec { mdfd_vfd: file, mdfd_segno: segno };

    let v = st.md_seg_fds[fk][segno as usize];
    debug_assert!(_mdnblocks(&v)? <= RELSEG_SIZE);
    Ok(Some(v))
}

fn _mdfd_getseg(
    rlocator: RelFileLocatorBackend,
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

    if targetseg < st.md_num_open_segs[fk] as BlockNumber {
        return Ok(Some(st.md_seg_fds[fk][targetseg as usize]));
    }

    if behavior & EXTENSION_DONT_OPEN != 0 {
        return Ok(None);
    }

    let mut v: MdfdVec;
    if st.md_num_open_segs[fk] > 0 {
        v = st.md_seg_fds[fk][(st.md_num_open_segs[fk] - 1) as usize];
    } else {
        match mdopenfork(rlocator, st, forknum, behavior)? {
            Some(seg) => v = seg,
            None => return Ok(None),
        }
    }

    let mut nextsegno = st.md_num_open_segs[fk] as BlockNumber;
    while nextsegno <= targetseg {
        let nblocks = _mdnblocks(&v)?;
        let mut flags = 0;

        debug_assert!(nextsegno == v.mdfd_segno + 1);

        if nblocks > RELSEG_SIZE {
            return throw(ereport(FATAL).errmsg_internal("segment too big"), "_mdfd_getseg");
        }

        if (behavior & EXTENSION_CREATE != 0)
            || (in_recovery() && (behavior & EXTENSION_CREATE_RECOVERY != 0))
        {
            if nblocks < RELSEG_SIZE {
                // Pad the short prior segment to RELSEG_SIZE with a zero block
                // so segment-boundary math holds when creating the next one.
                let zerobuf = [0u8; BLCKSZ];
                mdextend(
                    rlocator,
                    st,
                    forknum,
                    nextsegno * RELSEG_SIZE - 1,
                    &zerobuf,
                    skip_fsync,
                )?;
            }
            flags = libc::O_CREAT;
        } else if nblocks < RELSEG_SIZE {
            // Only chain into the next segment past an exactly-full one.
            if behavior & EXTENSION_RETURN_NULL != 0 {
                // No syscall failed; fake ENOENT so callers see the
                // deleted-file case.
                set_errno(ENOENT);
                return Ok(None);
            }
            return throw(
                ereport(ERROR)
                    .with_saved_errno(last_errno())
                    .errcode_for_file_access()
                    .errmsg(format!(
                        "could not open file \"{}\" (target block {}): previous segment is only {} blocks",
                        mdsegpath(rlocator, forknum, nextsegno),
                        blkno,
                        nblocks
                    )),
                "_mdfd_getseg",
            );
        }

        match _mdfd_openseg(rlocator, st, forknum, nextsegno, flags)? {
            Some(seg) => v = seg,
            None => {
                if (behavior & EXTENSION_RETURN_NULL != 0) && file_possibly_deleted(last_errno()) {
                    return Ok(None);
                }
                return throw(
                    ereport(ERROR)
                        .with_saved_errno(last_errno())
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not open file \"{}\" (target block {}): %m",
                            mdsegpath(rlocator, forknum, nextsegno),
                            blkno
                        )),
                    "_mdfd_getseg",
                );
            }
        }
        nextsegno += 1;
    }

    Ok(Some(v))
}

fn _mdnblocks(seg: &MdfdVec) -> PgResult<BlockNumber> {
    let len = fd::FileSize(seg.mdfd_vfd)?;
    if len < 0 {
        return throw(
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not seek to end of file \"{}\": %m",
                    fd::FilePathName(seg.mdfd_vfd)
                )),
            "_mdnblocks",
        );
    }
    // partial block at EOF is ignored
    Ok((len / BLCKSZ_I64) as BlockNumber)
}

fn unlink_raw(path: &str) -> i32 {
    let c = match std::ffi::CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            set_errno(libc::EINVAL);
            return -1;
        }
    };
    // SAFETY: NUL-terminated path.
    unsafe { libc::unlink(c.as_ptr()) }
}

fn pg_truncate_raw(path: &str, length: i64) -> i32 {
    let c = match std::ffi::CString::new(path.as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            set_errno(libc::EINVAL);
            return -1;
        }
    };
    // SAFETY: NUL-terminated path.
    unsafe { libc::truncate(c.as_ptr(), length as libc::off_t) }
}

// Maps fd's PgResult FileSync onto md.c's `FileSync(...) < 0` test, keeping
// errno for the caller's %m.
pub fn file_sync_failed(file: File, wait_event: u32) -> PgResult<bool> {
    Ok(fd::FileSync(file, wait_event)? < 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rl(db: Oid) -> RelFileLocator {
        RelFileLocator { spcOid: 1, dbOid: db, relNumber: 16384 }
    }

    #[test]
    fn md_relpath_forwards_locator_backend_and_fork_to_relpathbackend() {
        // Regression pin (GL-TESTFIX-1 F-R2-2 adjudication): the old
        // hand-rolled mint dropped spcOid (shared catalogs printed as
        // "base/0/NNNN") and stamped numeric fork suffixes. C parity =
        // relpathbackend(rlocator, is_temp ? MyProcNumber : INVALID, fork);
        // the stub renders all three inputs so forwarding is fully pinned.
        // Seams are install-once per process: this is the ONLY md unit test
        // that installs relpathbackend, so both arms are covered here.
        relpath_seams::relpathbackend::set(|l, b, f| {
            format!("{}|{}|{}|{}|{}", l.spcOid, l.dbOid, l.relNumber, b, f as i32)
        });
        let shared = types_storage::aio::PgAioTargetData {
            smgr: types_storage::aio::PgAioTargetSmgr {
                rlocator: RelFileLocator { spcOid: 1664, dbOid: 0, relNumber: 1260 },
                blockNum: 0,
                nblocks: 1,
                forkNum: ForkNumber::MAIN_FORKNUM,
                is_temp: false,
                ..Default::default()
            },
        };
        assert_eq!(md_relpath(&shared), format!("1664|0|1260|{INVALID_PROC_NUMBER}|0"));

        let temp = types_storage::aio::PgAioTargetData {
            smgr: types_storage::aio::PgAioTargetSmgr {
                rlocator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16384 },
                blockNum: 0,
                nblocks: 1,
                forkNum: ForkNumber::FSM_FORKNUM,
                is_temp: true,
                ..Default::default()
            },
        };
        let me = init_small::globals::MyProcNumber();
        assert_eq!(
            md_relpath(&temp),
            format!("1663|5|16384|{me}|{}", ForkNumber::FSM_FORKNUM as i32)
        );
    }

    // GL-MDWRAP-1. The bar sits on `cache_extent_after` and on `wrapping_add`,
    // NOT on a call into mdzeroextend, and that placement is deliberate:
    // mdzeroextend_inner asserts `nblocks > 0`, so any fixture that drove the
    // out-of-contract input through it would be correct in the shipped profiles
    // and RED at the test profile (which inherits dev, where assertions live).
    // Everything asserted here means the same thing under `cargo test` and
    // `cargo test --release`, so the bar can neither break the gate at one tier
    // nor go inert at the other.
    #[test]
    fn zeroextend_cache_extent_is_checked_not_wrapping() {
        // Well-formed extensions still publish the new block count.
        assert_eq!(cache_extent_after(0, 1), Some(1));
        assert_eq!(cache_extent_after(100, 64), Some(164));
        assert_eq!(cache_extent_after(RELSEG_SIZE, 1), Some(RELSEG_SIZE + 1));

        // Out of contract => no cached size is published at all, so the caller
        // invalidates and the next read measures the real file.
        for (b, n) in [(0u32, -1i32), (0, -64), (5, -10), (1000, -1), (0, 0), (7, 0)] {
            assert_eq!(cache_extent_after(b, n), None, "blocknum={b} nblocks={n}");
        }

        // Representability ceiling (mdzeroextend_inner errors in this range, so
        // the cache must not be handed a value from either arm).
        assert_eq!(cache_extent_after(InvalidBlockNumber - 1, 1), None);
        assert_eq!(cache_extent_after(InvalidBlockNumber - 2, 1), Some(InvalidBlockNumber - 1));

        // What the shipped profiles computed BEFORE the fix, written with
        // wrapping_add so it denotes the same value at every profile. Two of
        // these exceed `blocknum` -- a relation size ABOVE the real file length,
        // i.e. past-EOF blocks served as valid -- and the first is exactly the
        // InvalidBlockNumber sentinel.
        assert_eq!(0u32.wrapping_add(-1i32 as u32), InvalidBlockNumber);
        assert!(0u32.wrapping_add(-64i32 as u32) > 0);
        assert!(5u32.wrapping_add(-10i32 as u32) > 5);
        // And the reason `overflow-checks` was never going to catch this: the
        // dangerous cases stay inside u32, so a trapping build traps only the
        // HARMLESS downward one.
        assert_eq!(1000u32.wrapping_add(-1i32 as u32), 999);
    }

    #[test]
    fn extension_flags_and_geometry_match_c() {
        assert_eq!(EXTENSION_FAIL, 1);
        assert_eq!(EXTENSION_RETURN_NULL, 2);
        assert_eq!(EXTENSION_CREATE, 4);
        assert_eq!(EXTENSION_CREATE_RECOVERY, 8);
        assert_eq!(EXTENSION_DONT_OPEN, 32);
        assert_eq!(RELSEG_SIZE, 131072);
        assert_eq!(SMGR_NFORKS, 4);
    }

    #[test]
    fn data_file_wait_events_match_wait_event_names() {
        assert_eq!(WAIT_EVENT_DATA_FILE_EXTEND, 0x0A00_0011);
        assert_eq!(WAIT_EVENT_DATA_FILE_WRITE, 0x0A00_0018);
    }

    #[test]
    fn mdmaxcombine_is_blocks_to_segment_end() {
        assert_eq!(mdmaxcombine(0), RELSEG_SIZE);
        assert_eq!(mdmaxcombine(1), RELSEG_SIZE - 1);
        assert_eq!(mdmaxcombine(RELSEG_SIZE - 1), 1);
        assert_eq!(mdmaxcombine(RELSEG_SIZE), RELSEG_SIZE);
    }

    #[test]
    fn fdvec_resize_keeps_highwater_capacity() {
        let mut st = MdRelnState::default();
        _fdvec_resize(&mut st, ForkNumber::MAIN_FORKNUM, 3);
        assert_eq!(st.md_num_open_segs[0], 3);
        let cap = st.md_seg_fds[0].capacity();
        _fdvec_resize(&mut st, ForkNumber::MAIN_FORKNUM, 1);
        assert_eq!(st.md_num_open_segs[0], 1);
        assert_eq!(st.md_seg_fds[0].capacity(), cap);
        _fdvec_resize(&mut st, ForkNumber::MAIN_FORKNUM, 0);
        assert_eq!(st.md_num_open_segs[0], 0);
    }

    #[test]
    fn mdfiletagmatches_compares_dboid() {
        let a = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, ForkNumber::MAIN_FORKNUM, rl(10), 0);
        let same = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, ForkNumber::FSM_FORKNUM, rl(10), 7);
        let diff = FileTag::new(SyncRequestHandler::SYNC_HANDLER_MD, ForkNumber::MAIN_FORKNUM, rl(11), 0);
        assert!(mdfiletagmatches(a, same));
        assert!(!mdfiletagmatches(a, diff));
    }

    #[test]
    fn is_temp_follows_backend_field() {
        assert!(!is_temp(RelFileLocatorBackend { locator: rl(1), backend: INVALID_PROC_NUMBER }));
        assert!(is_temp(RelFileLocatorBackend { locator: rl(1), backend: 3 }));
    }

    #[test]
    fn iov_builders_skip_partial_transfers() {
        let mut a = [1u8; 4];
        let mut b = [2u8; 4];
        let mut bufs: [&mut [u8]; 2] = [&mut a, &mut b];
        let n = with_iov_mut(&mut bufs, 6, |iov| {
            assert_eq!(iov.len(), 1);
            assert_eq!(iov[0].len(), 2);
            iov.len()
        });
        assert_eq!(n, Some(1));
        assert!(with_iov_mut(&mut bufs, 8, |_| ()).is_none());

        let ra = [1u8; 4];
        let rb = [2u8; 4];
        let rbufs: [WriteChunk<'_>; 2] =
            [WriteChunk::from_slice(&ra), WriteChunk::from_slice(&rb)];
        let n = with_iov(&rbufs, 3, |iov| {
            assert_eq!(iov.len(), 2);
            assert_eq!(iov[0].len(), 1);
            assert_eq!(iov[1].len(), 4);
        });
        assert!(n.is_some());
    }
}

// ---------------------------------------------------------------------------
// AIO arms (md.c): mdstartreadv + the PGAIO_HCB_MD_READV callbacks.
// ---------------------------------------------------------------------------

/// mdstartreadv: asynchronous mdreadv on the current handed-out AIO handle.
/// `pages` are pinned pool pages (BLCKSZ each), consecutive on disk.
pub fn mdstartreadv(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    pages: &[*mut u8],
) -> PgResult<()> {
    let nblocks = pages.len() as BlockNumber;
    let v = _mdfd_getseg(
        rlocator,
        st,
        forknum,
        blocknum,
        false,
        EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
    )?
    .expect("EXTENSION_FAIL ereports rather than returning None");

    let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;
    debug_assert!(seekpos < BLCKSZ_I64 * RELSEG_SIZE as i64);

    let nblocks_this_segment =
        core::cmp::min(nblocks as i64, (RELSEG_SIZE - (blocknum % RELSEG_SIZE)) as i64)
            as BlockNumber;
    if nblocks_this_segment != nblocks {
        return throw(
            ereport(ERROR).errmsg_internal("read crossing segment boundary"),
            "mdstartreadv",
        );
    }

    let iovcnt = aio_core::pgaio_io_set_iovec_pages(pages, BLCKSZ);
    debug_assert!(iovcnt <= nblocks_this_segment as i32);

    if fd::io_direct_flags() & IO_DIRECT_DATA == 0 {
        aio_core::pgaio_io_set_flag(aio_core::pgaio_io_current(), types_storage::aio::PGAIO_HF_BUFFERED);
    }

    let ioh = aio_core::pgaio_io_current();
    aio_core::pgaio_io_set_target_smgr(
        ioh,
        rlocator.locator,
        forknum,
        blocknum,
        nblocks,
        rlocator.backend != INVALID_PROC_NUMBER,
        false,
    );
    aio_core::pgaio_io_register_callbacks(ioh, types_storage::aio::PGAIO_HCB_MD_READV, 0);

    let ret = fd::FileStartReadV(v.mdfd_vfd, iovcnt, seekpos, WAIT_EVENT_DATA_FILE_READ)?;
    if ret != 0 {
        return throw(
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not start reading blocks {}..{} in file \"{}\": %m",
                    blocknum,
                    blocknum + nblocks_this_segment - 1,
                    fd::FilePathName(v.mdfd_vfd)
                )),
            "mdstartreadv",
        );
    }
    // Post-read checks live in md_readv_complete; zero_damaged_pages'
    // past-EOF arm is intentionally NOT implemented on the AIO path (C 18
    // dropped it there too).
    Ok(())
}

/// smgr_aio_reopen's md half: resolve the segment through THIS thread's vfd
/// cache and return the raw fd (workers re-resolve, never reuse the issuer's
/// fd — the C cross-process reopen contract, thread-rendered).
pub fn md_aio_reopen_fd(
    rlocator: RelFileLocatorBackend,
    st: &mut MdRelnState,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    expected_offset: u64,
) -> PgResult<i32> {
    let v = _mdfd_getseg(
        rlocator,
        st,
        forknum,
        blocknum,
        false,
        EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
    )?
    .expect("EXTENSION_FAIL ereports rather than returning None");
    let seekpos = BLCKSZ_I64 * (blocknum % RELSEG_SIZE) as i64;
    debug_assert_eq!(seekpos as u64, expected_offset);
    let raw = fd::FileRawDescForAio(v.mdfd_vfd)?;
    if raw < 0 {
        return throw(
            ereport(ERROR)
                .with_saved_errno(last_errno())
                .errcode_for_file_access()
                .errmsg(format!(
                    "could not reopen file \"{}\" for IO: %m",
                    fd::FilePathName(v.mdfd_vfd)
                )),
            "md_aio_reopen_fd",
        );
    }
    Ok(raw)
}

// C md_readv_report's path mint: relpathbackend with MyProcNumber only for
// temp relations. The previous hand-rolled "base/{dbOid}/..." spelling
// ignored the tablespace (shared catalogs printed as "base/0/NNNN" instead
// of "global/NNNN", non-default tablespaces as base/), and stamped numeric
// fork suffixes ("_1") where C prints fork names ("_fsm") — wrong paths in
// every AIO read-error report (GL-TESTFIX-1 F-R2-2 adjudication).
fn md_relpath(td: &types_storage::aio::PgAioTargetData) -> String {
    let backend = if td.smgr.is_temp {
        init_small::globals::MyProcNumber()
    } else {
        INVALID_PROC_NUMBER
    };
    relpath_seams::relpathbackend::call(td.smgr.rlocator, backend, td.smgr.forkNum)
}

/// md_readv_complete: distill the raw byte result into blocks; encode hard
/// errors / short reads for md_readv_report.
pub fn md_readv_complete(
    ioh: u32,
    prior_result: types_storage::aio::PgAioResult,
    _cb_data: u8,
) -> types_storage::aio::PgAioResult {
    use types_storage::aio::PgAioResultStatus as Rs;
    let td = aio_core::pgaio_io_get_target_data(ioh);
    let mut result = prior_result;

    if prior_result.result < 0 {
        result.status = Rs::Error;
        result.id = types_storage::aio::PGAIO_HCB_MD_READV;
        // Hard errors carry the errno in error_data.
        result.error_data = (-prior_result.result) as u32;
        result.result = 0;
        // Log immediately, server-only: the definer may never process the
        // result (see bufmgr's completion rationale).
        let _ = aio_core::pgaio_result_report(result, &td, types_error::LOG_SERVER_ONLY);
        return result;
    }

    // The smgr API is block-, not byte-grained.
    result.result /= BLCKSZ as i32;
    debug_assert!(result.result <= td.smgr.nblocks as i32);

    if result.result == 0 {
        // Zero blocks read is a failure (unexpected EOF).
        result.status = Rs::Error;
        result.id = types_storage::aio::PGAIO_HCB_MD_READV;
        result.error_data = 0;
        let _ = aio_core::pgaio_result_report(result, &td, types_error::LOG_SERVER_ONLY);
        return result;
    }

    if result.status != Rs::Error && result.result < td.smgr.nblocks as i32 {
        // Partial reads are retried at the bufmgr level.
        result.status = Rs::Partial;
        result.id = types_storage::aio::PGAIO_HCB_MD_READV;
    }

    result
}

/// md_readv_report: error_data != 0 is a hard errno; == 0 is a short read.
pub fn md_readv_report(
    result: types_storage::aio::PgAioResult,
    td: types_storage::aio::PgAioTargetData,
    elevel: types_error::ErrorLevel,
) -> PgResult<()> {
    let path = md_relpath(&td);
    let first = td.smgr.blockNum;
    let last = first + td.smgr.nblocks - 1;
    if result.error_data != 0 {
        ereport(elevel)
            .with_saved_errno(result.error_data as i32)
            .errcode_for_file_access()
            .errmsg(format!("could not read blocks {first}..{last} in file \"{path}\": %m"))
            .finish(loc("md_readv_report"))
    } else {
        ereport(elevel)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(format!(
                "could not read blocks {first}..{last} in file \"{path}\": read only {} of {} bytes",
                result.result as i64 * BLCKSZ_I64,
                td.smgr.nblocks as i64 * BLCKSZ_I64
            ))
            .finish(loc("md_readv_report"))
    }
}
