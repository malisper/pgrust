//! PosixVfs — the product-path Vfs: thin, `#[inline]`, single-syscall
//! wrappers. Codegen through the free-fn shims is byte-identical to the raw
//! libc calls fd made before P1. Platform splits (contract §1.1) live here:
//! macOS F_FULLFSYNC, PG_O_DIRECT → F_NOCACHE, F_RDADVISE vs posix_fadvise.

use core::ffi::CStr;

use crate::{
    c_int, mode_t, off_t, set_errno, FdBudgetProbe, FdSoftLimitRaise, FileInfo, Vfs, VfsDirIter,
    VfsResult, PG_O_DIRECT,
};
// wasm32: only the (not-wasm) probe + macOS open path read errno directly.
#[cfg(not(target_family = "wasm"))]
use crate::get_errno;

pub struct PosixVfs;

impl PosixVfs {
    pub const fn new() -> Self {
        PosixVfs
    }

    /// pg_fsync_writethrough's platform arm: F_FULLFSYNC where it exists,
    /// ENOSYS elsewhere (fd keeps the enableFsync gate above).
    #[inline]
    pub fn fsync_writethrough(&self, fd: c_int) -> c_int {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        {
            // SAFETY: F_FULLFSYNC on a caller-owned descriptor.
            if unsafe { libc::fcntl(fd, libc::F_FULLFSYNC, 0) } == -1 {
                -1
            } else {
                0
            }
        }
        #[cfg(not(any(target_os = "macos", target_os = "ios")))]
        {
            let _ = fd;
            set_errno(libc::ENOSYS);
            -1
        }
    }

    /// count_usable_fds' probe core: getrlimit-bounded dup(2) loop, all probe
    /// fds closed before returning. Diagnostics captured for fd to report.
    ///
    /// wasm32: WASI p1 has neither getrlimit nor dup(2); fd's own
    /// count_usable_fds wasm arm short-circuits before reaching the Vfs, so
    /// this probe is launch-unreachable there — the arm reports an honest
    /// zero-probe (ENOSYS) and exists only so the hub compiles.
    #[cfg(target_family = "wasm")]
    pub fn fd_budget_probe_report(&self, _max_to_probe: usize) -> FdBudgetProbe {
        FdBudgetProbe {
            used: 0,
            highest_fd: 0,
            getrlimit_failed: true,
            getrlimit_errno: libc::ENOSYS,
            stop_errno: libc::ENOSYS,
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn fd_budget_probe_report(&self, max_to_probe: usize) -> FdBudgetProbe {
        let mut opened: Vec<i32> = Vec::with_capacity(1024);
        let mut used: i32 = 0;
        let mut highestfd: i32 = 0;
        let mut stop_errno: i32 = 0;

        let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: getrlimit writes the out-param struct.
        let getrlimit_status = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) };
        let getrlimit_errno = if getrlimit_status != 0 { get_errno() } else { 0 };

        loop {
            if getrlimit_status == 0 && highestfd as u64 >= (rlim.rlim_cur as u64).wrapping_sub(1)
            {
                break;
            }

            // SAFETY: dup(2) of stderr; yields a fresh fd or -1.
            let thisfd = unsafe { libc::dup(2) };
            if thisfd < 0 {
                stop_errno = get_errno();
                break;
            }

            opened.push(thisfd);
            used += 1;
            if highestfd < thisfd {
                highestfd = thisfd;
            }
            if used as usize >= max_to_probe {
                break;
            }
        }

        for &thisfd in &opened {
            // SAFETY: each entry is a live fd dup'd above.
            unsafe { libc::close(thisfd) };
        }

        FdBudgetProbe {
            used,
            highest_fd: highestfd,
            getrlimit_failed: getrlimit_status != 0,
            getrlimit_errno,
            stop_errno,
        }
    }

    /// Raise this process's RLIMIT_NOFILE soft limit to the hard limit.
    ///
    /// C never needs this: one backend = one process = one fd table, so the
    /// stock soft limit is a per-backend budget. pgrust is ONE process with a
    /// thread per session, so the soft limit is the budget for the WHOLE
    /// server and the stock value (8192 on AL2023) runs out mid-workload.
    /// Raising soft to hard is what every server that keeps many descriptors
    /// open does, and it needs no privilege.
    ///
    /// An unlimited or unreachable hard limit is walked down: some kernels
    /// (macOS: `kern.maxfilesperproc`) refuse a soft limit the process can
    /// never actually use, so each rejected target is halved until one is
    /// accepted or we are back at the starting soft limit.
    ///
    /// wasm32: WASI p1 has no rlimits — reported as "no getrlimit".
    #[cfg(target_family = "wasm")]
    pub fn raise_fd_soft_limit_to_hard(&self) -> FdSoftLimitRaise {
        FdSoftLimitRaise {
            soft_before: 0,
            soft_after: 0,
            hard: 0,
            getrlimit_failed: true,
            getrlimit_errno: libc::ENOSYS,
            setrlimit_errno: 0,
        }
    }

    #[cfg(not(target_family = "wasm"))]
    pub fn raise_fd_soft_limit_to_hard(&self) -> FdSoftLimitRaise {
        let mut rlim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: getrlimit writes the out-param struct.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) } != 0 {
            return FdSoftLimitRaise {
                soft_before: 0,
                soft_after: 0,
                hard: 0,
                getrlimit_failed: true,
                getrlimit_errno: get_errno(),
                setrlimit_errno: 0,
            };
        }
        // rlim_t is u64 on every platform this builds for (Linux, macOS), so
        // the report's u64 fields take these directly.
        let soft_before = rlim.rlim_cur;
        let hard = rlim.rlim_max;
        let mut report = FdSoftLimitRaise {
            soft_before,
            soft_after: soft_before,
            hard: if hard == libc::RLIM_INFINITY { u64::MAX } else { hard },
            getrlimit_failed: false,
            getrlimit_errno: 0,
            setrlimit_errno: 0,
        };
        if hard != libc::RLIM_INFINITY && soft_before >= hard {
            return report;
        }

        // An infinite hard limit is not a number the kernel will accept as a
        // soft limit everywhere; start from a ceiling no realistic server
        // exceeds and walk down.
        const UNLIMITED_TARGET: u64 = 1 << 20;
        let mut target: u64 = if hard == libc::RLIM_INFINITY {
            UNLIMITED_TARGET.max(soft_before)
        } else {
            hard
        };

        let mut last_errno = 0;
        while target > soft_before {
            let want = libc::rlimit { rlim_cur: target, rlim_max: hard };
            // SAFETY: setrlimit reads the struct; failure leaves limits as-is.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &want) } == 0 {
                report.soft_after = target;
                report.setrlimit_errno = 0;
                return report;
            }
            last_errno = get_errno();
            target /= 2;
        }
        report.setrlimit_errno = last_errno;
        report
    }
}

impl Default for PosixVfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Vfs for PosixVfs {
    #[inline]
    fn open(&self, path: &CStr, flags: c_int, mode: mode_t) -> c_int {
        #[cfg(target_os = "macos")]
        {
            // SAFETY: NUL-terminated path; PG_O_DIRECT is a synthetic bit
            // masked off before reaching the kernel.
            let raw = unsafe {
                libc::open(path.as_ptr(), flags & !PG_O_DIRECT, mode as libc::c_uint)
            };
            if raw >= 0 && flags & PG_O_DIRECT != 0 {
                // SAFETY: `raw` is live; F_NOCACHE is macOS's O_DIRECT analogue.
                if unsafe { libc::fcntl(raw, libc::F_NOCACHE, 1) } < 0 {
                    let save_errno = get_errno();
                    // SAFETY: closing the fd we just opened.
                    unsafe { libc::close(raw) };
                    set_errno(save_errno);
                    return -1;
                }
            }
            raw
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = PG_O_DIRECT; // real O_DIRECT (or 0): passes straight through
            // SAFETY: NUL-terminated path.
            unsafe { libc::open(path.as_ptr(), flags, mode as libc::c_uint) }
        }
    }

    #[inline]
    fn close(&self, fd: c_int) -> c_int {
        // SAFETY: caller owns the descriptor; closed exactly once by contract.
        unsafe { libc::close(fd) }
    }

    #[inline]
    fn preadv(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize {
        // SAFETY: caller contract — iov bases valid for writes of their lens.
        unsafe { libc::preadv(fd, iov.as_ptr(), iov.len() as c_int, off) as isize }
    }

    #[inline]
    fn pwritev(&self, fd: c_int, iov: &[libc::iovec], off: off_t) -> isize {
        // SAFETY: caller contract — iov bases valid for reads of their lens.
        unsafe { libc::pwritev(fd, iov.as_ptr(), iov.len() as c_int, off) as isize }
    }

    #[inline]
    fn pread(&self, fd: c_int, buf: &mut [u8], off: off_t) -> isize {
        // SAFETY: buf is a live writable slice.
        unsafe { libc::pread(fd, buf.as_mut_ptr().cast(), buf.len(), off) as isize }
    }

    #[inline]
    fn pwrite(&self, fd: c_int, buf: &[u8], off: off_t) -> isize {
        // SAFETY: buf is a live readable slice.
        unsafe { libc::pwrite(fd, buf.as_ptr().cast(), buf.len(), off) as isize }
    }

    #[inline]
    fn fsync(&self, fd: c_int) -> c_int {
        // SAFETY: fsync(2) on a caller-owned descriptor.
        unsafe { libc::fsync(fd) }
    }

    #[inline]
    fn fdatasync(&self, fd: c_int) -> c_int {
        // The libc crate doesn't declare fdatasync for apple targets; macOS
        // has it.
        #[cfg(target_os = "macos")]
        extern "C" {
            fn fdatasync(fd: libc::c_int) -> libc::c_int;
        }
        #[cfg(not(target_os = "macos"))]
        use libc::fdatasync;

        // SAFETY: fdatasync(2) on a caller-owned descriptor.
        unsafe { fdatasync(fd) }
    }

    #[inline]
    fn flush_range(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: sync_file_range(2) on a caller-owned descriptor.
            unsafe { libc::sync_file_range(fd, off, len, libc::SYNC_FILE_RANGE_WRITE) }
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Hint op, MAY no-op: the non-Linux mmap/msync writeback arm stays
            // cfg'd in fd::sync::pg_flush_data (documented carve-out).
            let _ = (fd, off, len);
            0
        }
    }

    #[inline]
    fn ftruncate(&self, fd: c_int, len: off_t) -> c_int {
        // SAFETY: ftruncate(2) on a caller-owned descriptor.
        unsafe { libc::ftruncate(fd, len) }
    }

    #[inline]
    fn truncate_path(&self, path: &CStr, len: off_t) -> c_int {
        // SAFETY: truncate(2) on a NUL-terminated path.
        unsafe { libc::truncate(path.as_ptr(), len) }
    }

    #[inline]
    fn fallocate(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: posix_fallocate on a caller-owned descriptor.
            unsafe { libc::posix_fallocate(fd, off, len) }
        }
        #[cfg(not(target_os = "linux"))]
        {
            // posix_fallocate convention: positive errno. Caller zero-extends.
            let _ = (fd, off, len);
            libc::EOPNOTSUPP
        }
    }

    #[inline]
    fn file_size(&self, fd: c_int) -> off_t {
        // SAFETY: lseek(2) on a caller-owned descriptor.
        unsafe { libc::lseek(fd, 0, libc::SEEK_END) }
    }

    #[inline]
    fn fadvise_willneed(&self, fd: c_int, off: off_t, len: off_t) -> c_int {
        #[cfg(target_os = "linux")]
        {
            // SAFETY: advisory hint on a caller-owned descriptor.
            unsafe { libc::posix_fadvise(fd, off, len, libc::POSIX_FADV_WILLNEED) }
        }
        #[cfg(target_os = "macos")]
        {
            #[repr(C)]
            struct Radvisory {
                ra_offset: libc::off_t,
                ra_count: libc::c_int,
            }
            let ra = Radvisory { ra_offset: off, ra_count: len as libc::c_int };
            // SAFETY: F_RDADVISE reads `ra`; fd is caller-owned.
            unsafe { libc::fcntl(fd, libc::F_RDADVISE, &ra) }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (fd, off, len);
            0
        }
    }

    #[inline]
    fn stat(&self, path: &CStr, out: &mut FileInfo) -> c_int {
        // SAFETY: st is a valid out-param; path is NUL-terminated.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::stat(path.as_ptr(), &mut st) };
        if rc == 0 {
            *out = file_info_from_stat(&st);
        }
        rc
    }

    #[inline]
    fn fstat(&self, fd: c_int, out: &mut FileInfo) -> c_int {
        // SAFETY: st is a valid out-param; fd may be any descriptor.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::fstat(fd, &mut st) };
        if rc == 0 {
            *out = file_info_from_stat(&st);
        }
        rc
    }

    #[inline]
    fn lstat(&self, path: &CStr, out: &mut FileInfo) -> c_int {
        // SAFETY: st is a valid out-param; path is NUL-terminated.
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        let rc = unsafe { libc::lstat(path.as_ptr(), &mut st) };
        if rc == 0 {
            *out = file_info_from_stat(&st);
        }
        rc
    }

    #[inline]
    fn read_link(&self, path: &CStr, buf: &mut [u8]) -> isize {
        // SAFETY: readlink(2) into a live writable slice.
        unsafe { libc::readlink(path.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) as isize }
    }

    #[inline]
    fn unlink(&self, path: &CStr) -> c_int {
        // SAFETY: NUL-terminated path.
        unsafe { libc::unlink(path.as_ptr()) }
    }

    #[inline]
    fn rename(&self, from: &CStr, to: &CStr) -> c_int {
        // SAFETY: both paths NUL-terminated; rename(2) is atomic.
        unsafe { libc::rename(from.as_ptr(), to.as_ptr()) }
    }

    #[inline]
    fn mkdir(&self, path: &CStr, mode: mode_t) -> c_int {
        // SAFETY: NUL-terminated path.
        unsafe { libc::mkdir(path.as_ptr(), mode) }
    }

    #[inline]
    fn rmdir(&self, path: &CStr) -> c_int {
        // SAFETY: NUL-terminated path.
        unsafe { libc::rmdir(path.as_ptr()) }
    }

    fn read_dir(&self, path: &CStr) -> VfsResult<VfsDirIter> {
        #[cfg(not(target_family = "wasm"))]
        use std::os::unix::ffi::OsStrExt;
        // wasm32: same from_bytes surface, wasi's spelling of the trait home.
        #[cfg(target_family = "wasm")]
        use std::os::wasi::ffi::OsStrExt;
        let os = std::ffi::OsStr::from_bytes(path.to_bytes());
        match std::fs::read_dir(os) {
            Ok(rd) => Ok(VfsDirIter::from_read_dir(rd)),
            Err(e) => {
                let en = e.raw_os_error().unwrap_or(libc::EIO);
                set_errno(en);
                Err(en)
            }
        }
    }

    fn fd_budget_probe(&self, max_to_probe: usize) -> usize {
        self.fd_budget_probe_report(max_to_probe).used as usize
    }
}

#[inline]
fn file_info_from_stat(st: &libc::stat) -> FileInfo {
    #[cfg(not(target_family = "wasm"))]
    let (mtime_sec, mtime_nsec) = (st.st_mtime as i64, st.st_mtime_nsec as i64);
    // wasm32: wasi-libc's stat spells it st_mtim (timespec); the libc crate
    // exposes no st_mtime/st_mtime_nsec alias fields (basebackup precedent).
    #[cfg(target_family = "wasm")]
    let (mtime_sec, mtime_nsec) = (st.st_mtim.tv_sec as i64, st.st_mtim.tv_nsec as i64);
    FileInfo {
        size: st.st_size as i64,
        mode: st.st_mode as u32,
        nlink: st.st_nlink as u64,
        mtime_sec,
        mtime_nsec,
        dev: st.st_dev as u64,
        ino: st.st_ino as u64,
        uid: st.st_uid as u32,
        gid: st.st_gid as u32,
    }
}
