//! `backend-storage-file-vfs` — the DST P1 VFS contract surface (contract v1,
//! frozen at tag `vfs-trait-v1`).
//!
//! The [`Vfs`] trait exists for **shape enforcement and SimVfs conformance**,
//! not dynamic dispatch. Product builds monomorphize [`ActiveVfs`] =
//! [`posix::PosixVfs`]; the sim harness selects `SimVfs` with the non-default
//! `--cfg pgrust_sim` (set exclusively by the sim harness RUSTFLAGS — never in
//! `.cargo/config`, product profiles, or fleet submit envs). Product codegen
//! is byte-identical to raw libc calls by construction: every op is an
//! `#[inline]` single-syscall wrapper.
//!
//! Binding rules (contract §1.1):
//! - **Errno contract (C-shaped):** every op returns `-1`/negative on failure
//!   and sets errno through the shared TLS cell ([`set_errno`]) that `fd` uses.
//!   No `io::Error` wrapping at this layer; the raw errno stays visible above.
//! - **Ops are single-shot.** All retry policy (EINTR loops, fd.c's
//!   EMFILE-retry `LruOpen` → `ReleaseLruFiles` dance) lives ABOVE the trait,
//!   in `fd`, exactly where it lives today. SimVfs never emits EINTR.
//! - **PG error-code mapping stays ABOVE the trait** (`errcode_for_file_access`
//!   in fd). SimVfs fault injection (P4) may only emit errnos from that
//!   vocabulary.
//! - **Composites stay above:** `durable_rename`, `durable_unlink`,
//!   `MakePGDirectory`, the VFD table/LRU/`max_safe_fds` machinery, temp-file
//!   accounting, resowner hooks, and waitevent brackets are fd's, untouched.
//!   The trait never owns, recomputes, or intercepts the fd budget.
//! - **Platform splits live INSIDE PosixVfs:** macOS F_FULLFSYNC, the
//!   synthetic `PG_O_DIRECT` bit → `fcntl(F_NOCACHE)` mapping, F_RDADVISE vs
//!   posix_fadvise.
//! - **Out of Vfs scope, permanently:** io_uring (stays behind `aio_seams`),
//!   anonymous mmap, stdio streams/pipes (`AllocateFile` fopen /
//!   `OpenPipeStream` popen), wait-event instrumentation.

#![allow(clippy::missing_safety_doc)]

use core::ffi::CStr;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};

pub use libc::{c_int, mode_t, off_t};

pub mod posix;

// ---------------------------------------------------------------------------
// Errno plumbing — the shared TLS cell (the C `errno` itself). `fd` re-exports
// these; PosixVfs sets errno implicitly via the kernel, SimVfs sets it here.
// ---------------------------------------------------------------------------

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

#[inline]
pub fn get_errno() -> i32 {
    // SAFETY: reading the thread-local errno.
    unsafe { *errno_location() }
}

#[inline]
pub fn set_errno(value: i32) {
    // SAFETY: writing the thread-local errno.
    unsafe {
        *errno_location() = value;
    }
}

// ---------------------------------------------------------------------------
// PG_O_DIRECT — the synthetic direct-IO open flag (storage/fd.h). Lives here
// because the PG_O_DIRECT → F_NOCACHE mapping is PosixVfs's platform split;
// `fd` re-exports it.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Plain types
// ---------------------------------------------------------------------------

/// Stat result, plain-typed. `mode` is `st_mode` widened to u32 (mode_t is u16
/// on macOS).
///
/// Contract v1.1 (additive revision, WS-B request, tag `vfs-trait-v1.1`):
/// `dev`/`ino` carry the (st_dev, st_ino) identity vocabulary the part-cache
/// staleness rule and the SegMap::open_shared registry key speak.
///
/// Contract v1.2 (additive revision, WS-A inc-3, tag `vfs-trait-v1.2`):
/// `uid`/`gid` carry the ownership words basebackup's tar headers stamp.
///
/// Construct via [`FileInfo::zeroed`] (or struct-update from it) so additive
/// revisions stay non-breaking. SimVfs fills synthetic stable values.
#[derive(Clone, Copy, Debug, Default)]
pub struct FileInfo {
    pub size: i64,
    pub mode: u32,
    pub nlink: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub dev: u64,
    pub ino: u64,
    pub uid: u32,
    pub gid: u32,
}

impl FileInfo {
    pub const fn zeroed() -> Self {
        FileInfo {
            size: 0,
            mode: 0,
            nlink: 0,
            mtime_sec: 0,
            mtime_nsec: 0,
            dev: 0,
            ino: 0,
            uid: 0,
            gid: 0,
        }
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        self.mode & (libc::S_IFMT as u32) == libc::S_IFDIR as u32
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        self.mode & (libc::S_IFMT as u32) == libc::S_IFREG as u32
    }

    #[inline]
    pub fn is_symlink(&self) -> bool {
        self.mode & (libc::S_IFMT as u32) == libc::S_IFLNK as u32
    }
}

/// Errno-carrying result for the ops that can't express failure in-band.
pub type VfsResult<T> = Result<T, c_int>;

enum DirIterInner {
    /// Kernel readdir order, "." and ".." elided (std::fs::read_dir semantics —
    /// identical to what fd's AllocateDir has always exposed to its callers).
    Posix(std::fs::ReadDir),
    /// SimVfs constructor: a pre-collected name list (BTree order — the
    /// deterministic-readdir rule).
    Named(std::vec::IntoIter<String>),
}

/// Directory iterator. Yields entry names (lossy UTF-8, matching fd's
/// `ReadDirExtended`), never "." or "..". `Err(errno)` mirrors a readdir
/// failure mid-stream.
pub struct VfsDirIter(DirIterInner);

impl VfsDirIter {
    /// SimVfs constructor: names must already be in deterministic (BTree)
    /// order and exclude "." / "..".
    pub fn from_names(names: Vec<String>) -> Self {
        VfsDirIter(DirIterInner::Named(names.into_iter()))
    }

    pub(crate) fn from_read_dir(rd: std::fs::ReadDir) -> Self {
        VfsDirIter(DirIterInner::Posix(rd))
    }
}

impl Iterator for VfsDirIter {
    type Item = Result<String, c_int>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            DirIterInner::Posix(rd) => match rd.next() {
                Some(Ok(entry)) => {
                    Some(Ok(entry.file_name().to_string_lossy().into_owned()))
                }
                Some(Err(e)) => {
                    let en = e.raw_os_error().unwrap_or(libc::EIO);
                    set_errno(en);
                    Some(Err(en))
                }
                None => None,
            },
            DirIterInner::Named(names) => names.next().map(Ok),
        }
    }
}

/// The full posix fd-budget probe outcome (`count_usable_fds` shape). The
/// trait op [`Vfs::fd_budget_probe`] returns only the budget; fd's diagnostics
/// (`already_open`, getrlimit/dup warnings) consume this report through
/// [`fd_budget_probe_report`].
#[derive(Clone, Copy, Debug)]
pub struct FdBudgetProbe {
    /// Successfully probed descriptors (the usable budget).
    pub used: i32,
    /// Highest fd number observed during the probe.
    pub highest_fd: i32,
    /// getrlimit(RLIMIT_NOFILE) failed; `getrlimit_errno` holds why.
    pub getrlimit_failed: bool,
    pub getrlimit_errno: i32,
    /// Errno that stopped the dup loop (0 = stopped by limit/max_to_probe;
    /// EMFILE/ENFILE are the expected stops and not warned about).
    pub stop_errno: i32,
}

/// Outcome of the boot-time RLIMIT_NOFILE raise ([`raise_fd_soft_limit_to_hard`]).
/// `soft_after` is the descriptor ceiling the whole server process has to
/// live inside — in the one-process/thread-per-session model that is the
/// budget every session shares, which is why fd's budget arithmetic needs it.
#[derive(Clone, Copy, Debug)]
pub struct FdSoftLimitRaise {
    pub soft_before: u64,
    /// Soft limit in force after the raise (== `soft_before` when no raise
    /// was needed or none was accepted).
    pub soft_after: u64,
    /// Hard limit; `u64::MAX` when the platform reports RLIM_INFINITY.
    pub hard: u64,
    /// getrlimit(RLIMIT_NOFILE) failed — the process ceiling is unknown and
    /// callers must fall back to the probed budget.
    pub getrlimit_failed: bool,
    pub getrlimit_errno: i32,
    /// Errno of the last rejected setrlimit (0 = no raise attempted, or the
    /// raise succeeded).
    pub setrlimit_errno: i32,
}

// ---------------------------------------------------------------------------
// The Vfs trait — the contract surface (signatures binding, names normative).
// FROZEN at tag vfs-trait-v1; any op-set or signature change after that tag is
// a contract revision, not a lane decision.
//
// v1 op-set note (folded in at freeze time): `rmdir` was added to the contract
// draft's list — fd's fenced clusters remove directories (RemovePgTempFiles,
// rmtree, unlink_if_exists_fname) and the namespace plane must be closed under
// the ops fd actually performs.
// ---------------------------------------------------------------------------

pub trait Vfs {
    // -- open/close (vfd.rs chokepoint) --
    /// PG_O_DIRECT accepted; -1/errno. Single-shot: the EMFILE-retry loop
    /// stays in fd's LruOpen path.
    fn open(&self, path: &CStr, flags: c_int, mode: mode_t) -> c_int;
    fn close(&self, fd: c_int) -> c_int;

    // -- data plane (io.rs; plus the SLRU transient-fd plane) --
    /// Caller contract: `iov` base pointers are valid for writes of their
    /// lengths (IoSliceMut-shaped).
    fn preadv(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize;
    fn pwritev(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize;
    fn pread(&self, fd: c_int, buf: &mut [u8], off: off_t) -> isize; // slru/xlogrecovery/basebackup
    fn pwrite(&self, fd: c_int, buf: &[u8], off: off_t) -> isize; // slru + transam_xlog write.rs

    // -- durability --
    /// Plain fsync(2), single-shot. The pg_fsync POLICY (enableFsync gate,
    /// wal_sync_method writethrough dispatch, debug asserts) stays in fd; the
    /// macOS F_FULLFSYNC arm is PosixVfs's [`posix::PosixVfs::fsync_writethrough`]
    /// (free-fn shim: [`fsync_writethrough`]).
    fn fsync(&self, fd: c_int) -> c_int;
    fn fdatasync(&self, fd: c_int) -> c_int;
    /// pg_flush_data hint; MAY no-op. Posix: sync_file_range on Linux, no-op
    /// elsewhere (the non-Linux mmap/msync arm stays cfg'd in fd — carve-out).
    fn flush_range(&self, fd: c_int, off: off_t, len: off_t) -> c_int;

    // -- size / space --
    fn ftruncate(&self, fd: c_int, len: off_t) -> c_int;
    fn truncate_path(&self, path: &CStr, len: off_t) -> c_int;
    /// posix_fallocate convention: 0 on success, positive errno on failure
    /// (EINTR possible; caller retries). Non-Linux returns EOPNOTSUPP and the
    /// caller zero-extends (fd's FileZero).
    fn fallocate(&self, fd: c_int, off: off_t, len: off_t) -> c_int;
    /// lseek(SEEK_END); -1/errno on failure.
    fn file_size(&self, fd: c_int) -> off_t;
    /// Advisory readahead hint; MAY no-op. Posix: posix_fadvise(WILLNEED) on
    /// Linux (returns positive errno on failure, EINTR possible), F_RDADVISE
    /// on macOS (-1/errno on failure).
    fn fadvise_willneed(&self, fd: c_int, off: off_t, len: off_t) -> c_int;

    // -- namespace --
    fn stat(&self, path: &CStr, out: &mut FileInfo) -> c_int;
    fn fstat(&self, fd: c_int, out: &mut FileInfo) -> c_int;
    fn lstat(&self, path: &CStr, out: &mut FileInfo) -> c_int; // copydir symlink_metadata
    /// readlink(2) shape: bytes written (no NUL), or -1/errno.
    fn read_link(&self, path: &CStr, buf: &mut [u8]) -> isize;
    fn unlink(&self, path: &CStr) -> c_int;
    /// Atomic; durable_rename composes ABOVE in fd.
    fn rename(&self, from: &CStr, to: &CStr) -> c_int;
    fn mkdir(&self, path: &CStr, mode: mode_t) -> c_int;
    fn rmdir(&self, path: &CStr) -> c_int;
    /// SimVfs: deterministic (BTree) order. Never yields "." or "..".
    fn read_dir(&self, path: &CStr) -> VfsResult<VfsDirIter>;

    // -- process/budget probe (vfd.rs dup(2) + getrlimit) --
    /// SimVfs: fixed pinned budget, no real fds touched.
    fn fd_budget_probe(&self, max_to_probe: usize) -> usize;
}

// ---------------------------------------------------------------------------
// Dispatch — cfg-selected monomorphization (contract §1.2). Zero-cost by
// construction: ActiveVfs is a ZST and every shim is #[inline].
// ---------------------------------------------------------------------------

#[cfg(not(pgrust_sim))]
pub type ActiveVfs = posix::PosixVfs;

// --- append region (WS-C): sim module + cfg hook ---
#[cfg(pgrust_sim)]
pub mod sim;
#[cfg(pgrust_sim)]
pub mod sim_boot;
#[cfg(pgrust_sim)]
pub type ActiveVfs = sim::SimVfs;
// --- end append region (WS-C) ---

/// The one active instance. Both PosixVfs and SimVfs are ZSTs with
/// `pub const fn new()`; sim state lives in thread-locals inside SimVfs.
pub const ACTIVE: ActiveVfs = ActiveVfs::new();

// Conformance assert: both impls must satisfy the trait even though dispatch
// is static (contract §1.2).
const _: () = {
    fn assert_conforms<T: Vfs>() {}
    #[allow(dead_code)]
    fn _both() {
        assert_conforms::<posix::PosixVfs>();
        #[cfg(pgrust_sim)]
        assert_conforms::<sim::SimVfs>();
    }
};

// ---------------------------------------------------------------------------
// Free-function shims — what fd (and controldata_utils) actually call.
// ---------------------------------------------------------------------------

macro_rules! shims {
    ($($(#[$doc:meta])* fn $name:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)+) => {
        $(
            $(#[$doc])*
            #[inline]
            pub fn $name($($arg: $ty),*) -> $ret {
                ACTIVE.$name($($arg),*)
            }
        )+
    };
}

shims! {
    fn open(path: &CStr, flags: c_int, mode: mode_t) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn preadv(fd: c_int, iov: &[libc::iovec], off: off_t) -> isize;
    fn pwritev(fd: c_int, iov: &[libc::iovec], off: off_t) -> isize;
    fn pread(fd: c_int, buf: &mut [u8], off: off_t) -> isize;
    fn pwrite(fd: c_int, buf: &[u8], off: off_t) -> isize;
    fn fsync(fd: c_int) -> c_int;
    fn fdatasync(fd: c_int) -> c_int;
    fn flush_range(fd: c_int, off: off_t, len: off_t) -> c_int;
    fn ftruncate(fd: c_int, len: off_t) -> c_int;
    fn truncate_path(path: &CStr, len: off_t) -> c_int;
    fn fallocate(fd: c_int, off: off_t, len: off_t) -> c_int;
    fn file_size(fd: c_int) -> off_t;
    fn fadvise_willneed(fd: c_int, off: off_t, len: off_t) -> c_int;
    fn stat(path: &CStr, out: &mut FileInfo) -> c_int;
    fn fstat(fd: c_int, out: &mut FileInfo) -> c_int;
    fn lstat(path: &CStr, out: &mut FileInfo) -> c_int;
    fn read_link(path: &CStr, buf: &mut [u8]) -> isize;
    fn unlink(path: &CStr) -> c_int;
    fn rename(from: &CStr, to: &CStr) -> c_int;
    fn mkdir(path: &CStr, mode: mode_t) -> c_int;
    fn rmdir(path: &CStr) -> c_int;
    fn read_dir(path: &CStr) -> VfsResult<VfsDirIter>;
    fn fd_budget_probe(max_to_probe: usize) -> usize;
}

// ---------------------------------------------------------------------------
// VfsFd — the owned handle for vfs-minted descriptors (DST P4 finding F1b).
// Additive helper NEXT TO the contract, not a trait revision: the Vfs op-set
// and signatures are untouched.
// ---------------------------------------------------------------------------

/// Owned handle to a **vfs-minted** file descriptor.
///
/// Deliberate close paths call [`VfsFd::into_raw`] (which disarms the guard)
/// and then [`close`], exactly as they did with `OwnedFd::into_raw_fd`. The
/// Drop arm exists for the paths that never reach a deliberate close —
/// unwinds and thread-exit TLS teardown — and routes the release through the
/// SAME provider that minted the fd ([`close_on_drop`]). With plain `OwnedFd`
/// those paths closed posix-side: under `--cfg pgrust_sim` the fd is foreign
/// to the kernel, so the close EBADF'd, the sim fd leaked in the sim table,
/// and std's debug IO-safety check aborted the whole process (P4 fault
/// injection unwinds through exactly these states).
///
/// Zero-cost off-sim by construction: `repr(transparent)` over
/// `ManuallyDrop<OwnedFd>` keeps the size AND the niche (`Option<VfsFd>` is
/// fd-sized, like `Option<OwnedFd>` — const-asserted below); `as_raw_fd` /
/// `into_raw` are field reads; and the Drop arm monomorphizes to the same
/// single `close(2)` call `OwnedFd`'s own Drop makes (minus std's debug-only
/// already-closed probe).
#[repr(transparent)]
pub struct VfsFd(core::mem::ManuallyDrop<std::os::fd::OwnedFd>);

// The zero-cost law, machine-checked: same size and same niche as the
// OwnedFd this type replaces inside Option<>-shaped holders.
const _: () = {
    assert!(
        core::mem::size_of::<Option<VfsFd>>()
            == core::mem::size_of::<Option<std::os::fd::OwnedFd>>()
    );
    assert!(core::mem::size_of::<Option<VfsFd>>() == core::mem::size_of::<c_int>());
};

impl VfsFd {
    /// Take ownership of `raw`.
    ///
    /// Thread affinity: drop (or deliberately close) the guard on the thread
    /// that minted `raw`. Under `--cfg pgrust_sim` each thread is its own vfs
    /// universe (thread-local fd table), so a guard moved cross-thread would
    /// release into the DESTINATION thread's sim table — EBADF there, silent
    /// leak in the minting universe. All current holders are FdState-TLS-bound
    /// so this cannot happen today; keep it that way.
    ///
    /// # Safety
    /// `raw` must be a live descriptor minted by the ACTIVE vfs ([`open`]),
    /// exclusively owned by the returned guard (`OwnedFd::from_raw_fd` rules,
    /// with "open file description" read against the active vfs domain).
    #[inline]
    pub unsafe fn from_raw(raw: RawFd) -> VfsFd {
        // SAFETY: caller contract above.
        VfsFd(core::mem::ManuallyDrop::new(unsafe {
            std::os::fd::OwnedFd::from_raw_fd(raw)
        }))
    }

    /// Disarm the guard and hand the descriptor back to the caller, who must
    /// [`close`] it — the deliberate-close path. No close happens here.
    #[inline]
    pub fn into_raw(self) -> RawFd {
        let raw = self.0.as_raw_fd();
        core::mem::forget(self);
        raw
    }
}

impl std::os::fd::AsRawFd for VfsFd {
    #[inline]
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl Drop for VfsFd {
    #[inline]
    fn drop(&mut self) {
        // Leak/unwind path only: deliberate closes disarm via into_raw().
        // The result is deliberately ignored — Drop has nobody to report to,
        // exactly like OwnedFd's own Drop.
        let _ = close_on_drop(self.0.as_raw_fd());
    }
}

/// The [`VfsFd`] drop arm. Posix: exactly `close(2)` — the identical single
/// call `OwnedFd`'s Drop makes. Sim: [`sim::SimVfs::close_on_drop`] — the
/// same close as [`close`], but tolerant of the sim thread-local already
/// being destroyed (guard drops legally run inside thread-exit TLS
/// destructors, and TLS destructor order is unspecified).
#[cfg(not(pgrust_sim))]
#[inline]
fn close_on_drop(fd: c_int) -> c_int {
    ACTIVE.close(fd)
}
#[cfg(pgrust_sim)]
#[inline]
fn close_on_drop(fd: c_int) -> c_int {
    sim::SimVfs::close_on_drop(fd)
}

/// The macOS F_FULLFSYNC arm of pg_fsync (wal_sync_method=fsync_writethrough).
/// Platform split lives inside PosixVfs; under sim a writethrough is simply a
/// (strictly weaker than requested, strictly correct) sim fsync.
#[cfg(not(pgrust_sim))]
#[inline]
pub fn fsync_writethrough(fd: c_int) -> c_int {
    ACTIVE.fsync_writethrough(fd)
}
#[cfg(pgrust_sim)]
#[inline]
pub fn fsync_writethrough(fd: c_int) -> c_int {
    ACTIVE.fsync(fd)
}

/// Instrumented fd-budget probe: same loop as [`Vfs::fd_budget_probe`], with
/// the diagnostics fd's `count_usable_fds` reports (already_open, getrlimit
/// and dup failure warnings). Sim arm synthesizes a clean report from the
/// pinned budget (already_open = 3: stdin/stdout/stderr).
#[cfg(not(pgrust_sim))]
#[inline]
pub fn fd_budget_probe_report(max_to_probe: usize) -> FdBudgetProbe {
    ACTIVE.fd_budget_probe_report(max_to_probe)
}
#[cfg(pgrust_sim)]
pub fn fd_budget_probe_report(max_to_probe: usize) -> FdBudgetProbe {
    let used = ACTIVE.fd_budget_probe(max_to_probe) as i32;
    FdBudgetProbe {
        used,
        highest_fd: used + 2,
        getrlimit_failed: false,
        getrlimit_errno: 0,
        stop_errno: 0,
    }
}

/// Boot-time RLIMIT_NOFILE raise (soft → hard). Sim arm: the simulated
/// process has a pinned budget and no rlimits, reported as "no getrlimit" so
/// fd keeps its probe-only arithmetic (deterministic across replays).
#[cfg(not(pgrust_sim))]
#[inline]
pub fn raise_fd_soft_limit_to_hard() -> FdSoftLimitRaise {
    ACTIVE.raise_fd_soft_limit_to_hard()
}
#[cfg(pgrust_sim)]
pub fn raise_fd_soft_limit_to_hard() -> FdSoftLimitRaise {
    FdSoftLimitRaise {
        soft_before: 0,
        soft_after: 0,
        hard: 0,
        getrlimit_failed: true,
        getrlimit_errno: libc::ENOSYS,
        setrlimit_errno: 0,
    }
}

#[cfg(test)]
mod tests;
