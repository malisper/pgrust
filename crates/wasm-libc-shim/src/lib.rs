//! `wasm-libc-shim` — a single shared `libc` stand-in for the wasm64 single-user
//! `postgres` build.
//!
//! On `wasm64-unknown-unknown` the `libc` crate *links* but exposes essentially
//! none of the POSIX constants, types, or functions that the backend's
//! OS-coupling crates (`fd.c`, `pqcomm.c`, `pg_locale.c`, the signal/IPC seams,
//! …) reference. Rather than copy a `libc_wasm` module into each of ~30 crates,
//! this crate provides one authoritative set of stand-ins, and each consumer
//! brings it in with, per module:
//!
//! ```ignore
//! #[cfg(target_family = "wasm")]
//! use wasm_libc_shim as libc;
//! ```
//!
//! which shadows the extern-prelude `libc` for that module only.
//!
//! ## Design
//! - **Constants** use the standard Linux/glibc numeric values — they are the
//!   ground truth this port mirrors, and they appear in `match` patterns
//!   (errno classification, `S_IFMT` masking, signal dispatch), so they must be
//!   real `const`s.
//! - **Types** (`stat`, `iovec`, `sockaddr*`, `rlimit`, `tm`, `FILE`, …) are
//!   `repr(C)` layouts matching Linux widths, sufficient for the call sites.
//! - **Functions** fall into two classes:
//!   - *real* where a faithful in-address-space implementation exists
//!     (`malloc`/`free`/`realloc`/`calloc` over Rust's global allocator;
//!     errno via a thread-local; the ctype/case helpers);
//!   - *single-user no-ops or ENOSYS-style errors* for genuinely-unsupportable
//!     ops (`fork`, SysV shm, sockets, signals, raw file syscalls) — single-user
//!     `postgres --single` never reaches the spawn/listen/signal paths, and the
//!     file path is meant to be routed through a host VFS behind the file seams.
//!
//! Everything here is `#![cfg(target_family = "wasm")]`-gated at the crate root,
//! so on native targets this crate is empty and inert.

#![cfg_attr(target_family = "wasm", allow(dead_code, non_camel_case_types))]
#![cfg_attr(not(target_family = "wasm"), allow(unused))]

#[cfg(target_family = "wasm")]
mod imp {
    pub use core::ffi::c_void;

    // ====================================================================
    // Primitive C type aliases (Linux widths on a 64-bit/LP64-like target).
    // wasm64 has 64-bit pointers/usize, so these match the native port.
    // ====================================================================
    pub type c_char = i8;
    pub type c_schar = i8;
    pub type c_uchar = u8;
    pub type c_short = i16;
    pub type c_ushort = u16;
    pub type c_int = i32;
    pub type c_uint = u32;
    pub type c_long = i64;
    pub type c_ulong = u64;
    pub type c_longlong = i64;
    pub type c_ulonglong = u64;
    pub type c_float = f32;
    pub type c_double = f64;

    pub type size_t = usize;
    pub type ssize_t = isize;
    pub type off_t = i64;
    pub type mode_t = u32;
    pub type pid_t = i32;
    pub type uid_t = u32;
    pub type gid_t = u32;
    pub type dev_t = u64;
    pub type ino_t = u64;
    pub type nlink_t = u64;
    pub type blksize_t = i64;
    pub type blkcnt_t = i64;
    pub type time_t = i64;
    pub type suseconds_t = i64;
    pub type clockid_t = i32;
    pub type wchar_t = i32;
    pub type key_t = i32;
    pub type socklen_t = u32;
    pub type sa_family_t = u16;
    pub type in_port_t = u16;
    pub type in_addr_t = u32;
    pub type nl_item = i32;
    pub type rlim_t = u64;
    pub type sigset_t = u64;
    pub type sighandler_t = usize;


    // ====================================================================
    // errno
    // ====================================================================
    pub const EPERM: c_int = 1;
    pub const ENOENT: c_int = 2;
    pub const ESRCH: c_int = 3;
    pub const EINTR: c_int = 4;
    pub const EIO: c_int = 5;
    pub const ENXIO: c_int = 6;
    pub const EBADF: c_int = 9;
    pub const EAGAIN: c_int = 11;
    pub const EWOULDBLOCK: c_int = 11;
    pub const ENOMEM: c_int = 12;
    pub const EACCES: c_int = 13;
    pub const EFAULT: c_int = 14;
    pub const EBUSY: c_int = 16;
    pub const EEXIST: c_int = 17;
    pub const ENODEV: c_int = 19;
    pub const ENOTDIR: c_int = 20;
    pub const EISDIR: c_int = 21;
    pub const EINVAL: c_int = 22;
    pub const ENFILE: c_int = 23;
    pub const EMFILE: c_int = 24;
    pub const EFBIG: c_int = 27;
    pub const ENOSPC: c_int = 28;
    pub const EROFS: c_int = 30;
    pub const EPIPE: c_int = 32;
    pub const ERANGE: c_int = 34;
    pub const ENAMETOOLONG: c_int = 36;
    pub const ENOSYS: c_int = 38;
    pub const ENOTEMPTY: c_int = 39;
    pub const ELOOP: c_int = 40;
    pub const EOPNOTSUPP: c_int = 95;
    pub const EADDRINUSE: c_int = 98;
    pub const ECONNABORTED: c_int = 103;
    pub const ECONNRESET: c_int = 104;
    pub const ETIMEDOUT: c_int = 110;
    pub const ECONNREFUSED: c_int = 111;

    // ====================================================================
    // open(2) flags / access / seek
    // ====================================================================
    pub const O_RDONLY: c_int = 0;
    pub const O_WRONLY: c_int = 1;
    pub const O_RDWR: c_int = 2;
    pub const O_CREAT: c_int = 0o100;
    pub const O_EXCL: c_int = 0o200;
    pub const O_TRUNC: c_int = 0o1000;
    pub const O_APPEND: c_int = 0o2000;
    pub const O_NONBLOCK: c_int = 0o4000;
    pub const O_DSYNC: c_int = 0o10000;
    pub const O_DIRECT: c_int = 0o40000;
    pub const O_CLOEXEC: c_int = 0o2000000;
    pub const O_SYNC: c_int = 0o4010000;

    pub const F_OK: c_int = 0;
    pub const X_OK: c_int = 1;
    pub const W_OK: c_int = 2;
    pub const R_OK: c_int = 4;

    pub const SEEK_SET: c_int = 0;
    pub const SEEK_CUR: c_int = 1;
    pub const SEEK_END: c_int = 2;

    // fcntl
    pub const F_GETFL: c_int = 3;
    pub const F_SETFL: c_int = 4;
    pub const F_GETFD: c_int = 1;
    pub const F_SETFD: c_int = 2;
    pub const FD_CLOEXEC: c_int = 1;
    // macOS-only fadvise analog referenced by fd.c; harmless const here.
    pub const F_RDADVISE: c_int = 44;

    // posix_fadvise
    pub const POSIX_FADV_NORMAL: c_int = 0;
    pub const POSIX_FADV_WILLNEED: c_int = 3;
    pub const POSIX_FADV_DONTNEED: c_int = 4;

    // ====================================================================
    // stat / mode bits
    // ====================================================================
    pub const S_IFMT: mode_t = 0o170000;
    pub const S_IFIFO: mode_t = 0o010000;
    pub const S_IFCHR: mode_t = 0o020000;
    pub const S_IFDIR: mode_t = 0o040000;
    pub const S_IFBLK: mode_t = 0o060000;
    pub const S_IFREG: mode_t = 0o100000;
    pub const S_IFLNK: mode_t = 0o120000;
    pub const S_IFSOCK: mode_t = 0o140000;

    pub const S_IRWXU: mode_t = 0o700;
    pub const S_IRUSR: mode_t = 0o400;
    pub const S_IWUSR: mode_t = 0o200;
    pub const S_IXUSR: mode_t = 0o100;
    pub const S_IRWXG: mode_t = 0o070;
    pub const S_IRGRP: mode_t = 0o040;
    pub const S_IWGRP: mode_t = 0o020;
    pub const S_IXGRP: mode_t = 0o010;
    pub const S_IRWXO: mode_t = 0o007;
    pub const S_IROTH: mode_t = 0o004;
    pub const S_IWOTH: mode_t = 0o002;
    pub const S_IXOTH: mode_t = 0o001;

    // ====================================================================
    // std fd numbers
    // ====================================================================
    pub const STDIN_FILENO: c_int = 0;
    pub const STDOUT_FILENO: c_int = 1;
    pub const STDERR_FILENO: c_int = 2;

    // ====================================================================
    // signals (Linux numbers). Single-user installs/masks are no-ops.
    // ====================================================================
    pub const SIGHUP: c_int = 1;
    pub const SIGINT: c_int = 2;
    pub const SIGQUIT: c_int = 3;
    pub const SIGILL: c_int = 4;
    pub const SIGABRT: c_int = 6;
    pub const SIGFPE: c_int = 8;
    pub const SIGKILL: c_int = 9;
    pub const SIGUSR1: c_int = 10;
    pub const SIGSEGV: c_int = 11;
    pub const SIGUSR2: c_int = 12;
    pub const SIGPIPE: c_int = 13;
    pub const SIGALRM: c_int = 14;
    pub const SIGTERM: c_int = 15;
    pub const SIGCHLD: c_int = 17;
    pub const SIGCONT: c_int = 18;
    pub const SIGURG: c_int = 23;
    pub const SIGWINCH: c_int = 28;
    pub const SIGPWR: c_int = 30;
    pub const SIGSYS: c_int = 31;
    pub const SIGSTOP: c_int = 19;
    pub const SIGTSTP: c_int = 20;
    pub const SIGTTIN: c_int = 21;
    pub const SIGTTOU: c_int = 22;
    pub const SIGXCPU: c_int = 24;
    pub const SIGXFSZ: c_int = 25;
    pub const SIGVTALRM: c_int = 26;
    pub const SIGPROF: c_int = 27;

    pub const WNOHANG: c_int = 1;
    pub const WUNTRACED: c_int = 2;

    pub const EAI_NONAME: c_int = -2;
    pub const EAI_FAIL: c_int = -4;
    pub const EAI_AGAIN: c_int = -3;
    pub const EAI_MEMORY: c_int = -10;
    // BSD-only signals referenced by some diagnostics; benign constants.
    pub const SIGINFO: c_int = 29;

    pub const SIG_BLOCK: c_int = 0;
    pub const SIG_UNBLOCK: c_int = 1;
    pub const SIG_SETMASK: c_int = 2;

    pub const SIG_DFL: sighandler_t = 0;
    pub const SIG_IGN: sighandler_t = 1;
    pub const SIG_ERR: sighandler_t = usize::MAX;

    pub const SA_RESTART: c_int = 0x1000_0000;
    pub const SA_NOCLDSTOP: c_int = 0x0000_0001;

    // wait status macros
    pub fn WIFEXITED(status: c_int) -> bool {
        (status & 0x7f) == 0
    }
    pub fn WEXITSTATUS(status: c_int) -> c_int {
        (status >> 8) & 0xff
    }
    pub fn WIFSIGNALED(status: c_int) -> bool {
        ((status & 0x7f) + 1) >> 1 > 0
    }
    pub fn WTERMSIG(status: c_int) -> c_int {
        status & 0x7f
    }

    // ====================================================================
    // sockets / netinet (single-user: listener cfg'd out; consts only)
    // ====================================================================
    pub const AF_UNSPEC: c_int = 0;
    pub const AF_UNIX: c_int = 1;
    pub const AF_INET: c_int = 2;
    pub const AF_INET6: c_int = 10;

    pub const SOCK_STREAM: c_int = 1;
    pub const SOCK_DGRAM: c_int = 2;

    pub const SOL_SOCKET: c_int = 1;
    pub const SO_REUSEADDR: c_int = 2;
    pub const SO_KEEPALIVE: c_int = 9;

    pub const IPPROTO_TCP: c_int = 6;
    pub const IPPROTO_IPV6: c_int = 41;
    pub const IPV6_V6ONLY: c_int = 26;

    pub const TCP_NODELAY: c_int = 1;
    pub const TCP_KEEPIDLE: c_int = 4;
    pub const TCP_KEEPINTVL: c_int = 5;
    pub const TCP_KEEPCNT: c_int = 6;
    pub const TCP_USER_TIMEOUT: c_int = 18;
    // macOS spellings referenced in keepalive code paths.
    pub const TCP_KEEPALIVE: c_int = 0x10;

    pub const AI_PASSIVE: c_int = 0x0001;
    pub const NI_NUMERICHOST: c_int = 1;

    // ====================================================================
    // mmap (single address space)
    // ====================================================================
    pub const PROT_READ: c_int = 1;
    pub const PROT_WRITE: c_int = 2;
    pub const MAP_SHARED: c_int = 0x01;
    pub const MAP_PRIVATE: c_int = 0x02;
    pub const MAP_ANONYMOUS: c_int = 0x20;
    pub const MAP_HASSEMAPHORE: c_int = 0x0200;
    pub const MAP_FAILED: *mut c_void = usize::MAX as *mut c_void;

    // ====================================================================
    // SysV IPC (single-user: arena/no-op semantics)
    // ====================================================================
    pub const IPC_PRIVATE: key_t = 0;
    pub const IPC_CREAT: c_int = 0o1000;
    pub const IPC_EXCL: c_int = 0o2000;
    pub const IPC_RMID: c_int = 0;
    pub const IPC_STAT: c_int = 2;

    // ====================================================================
    // rlimit
    // ====================================================================
    pub const RLIMIT_NOFILE: c_int = 7;
    pub const RLIMIT_STACK: c_int = 3;
    pub const RLIM_INFINITY: rlim_t = u64::MAX;

    // sysconf
    pub const _SC_PAGESIZE: c_int = 30;

    // stdio buffering
    pub const _IOFBF: c_int = 0;
    pub const _IOLBF: c_int = 1;
    pub const _IONBF: c_int = 2;

    // PIPE_BUF
    pub const PIPE_BUF: usize = 4096;

    // locale categories / masks (pg_locale single C-locale)
    pub const LC_CTYPE: c_int = 0;
    pub const LC_NUMERIC: c_int = 1;
    pub const LC_TIME: c_int = 2;
    pub const LC_COLLATE: c_int = 3;
    pub const LC_MONETARY: c_int = 4;
    pub const LC_MESSAGES: c_int = 5;
    pub const LC_ALL: c_int = 6;

    pub const LC_CTYPE_MASK: c_int = 1 << LC_CTYPE;
    pub const LC_NUMERIC_MASK: c_int = 1 << LC_NUMERIC;
    pub const LC_TIME_MASK: c_int = 1 << LC_TIME;
    pub const LC_COLLATE_MASK: c_int = 1 << LC_COLLATE;
    pub const LC_MONETARY_MASK: c_int = 1 << LC_MONETARY;
    pub const LC_MESSAGES_MASK: c_int = 1 << LC_MESSAGES;
    pub const LC_ALL_MASK: c_int = 0x1fff;

    // nl_langinfo item
    pub const CODESET: nl_item = 14;

    // ====================================================================
    // C structs (repr(C), Linux-ish layouts; sufficient for the call sites)
    // ====================================================================
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct iovec {
        pub iov_base: *mut c_void,
        pub iov_len: size_t,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct stat {
        pub st_dev: dev_t,
        pub st_ino: ino_t,
        pub st_nlink: nlink_t,
        pub st_mode: mode_t,
        pub st_uid: uid_t,
        pub st_gid: gid_t,
        pub __pad0: c_int,
        pub st_rdev: dev_t,
        pub st_size: off_t,
        pub st_blksize: blksize_t,
        pub st_blocks: blkcnt_t,
        pub st_atime: time_t,
        pub st_atime_nsec: i64,
        pub st_mtime: time_t,
        pub st_mtime_nsec: i64,
        pub st_ctime: time_t,
        pub st_ctime_nsec: i64,
        pub __unused: [i64; 3],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct rlimit {
        pub rlim_cur: rlim_t,
        pub rlim_max: rlim_t,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct tm {
        pub tm_sec: c_int,
        pub tm_min: c_int,
        pub tm_hour: c_int,
        pub tm_mday: c_int,
        pub tm_mon: c_int,
        pub tm_year: c_int,
        pub tm_wday: c_int,
        pub tm_yday: c_int,
        pub tm_isdst: c_int,
        pub tm_gmtoff: c_long,
        pub tm_zone: *const c_char,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct timespec {
        pub tv_sec: time_t,
        pub tv_nsec: c_long,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct timeval {
        pub tv_sec: time_t,
        pub tv_usec: suseconds_t,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct itimerval {
        pub it_interval: timeval,
        pub it_value: timeval,
    }

    pub const ITIMER_REAL: c_int = 0;

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct in_addr {
        pub s_addr: in_addr_t,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct in6_addr {
        pub s6_addr: [u8; 16],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct sockaddr {
        pub sa_family: sa_family_t,
        pub sa_data: [c_char; 14],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct sockaddr_in {
        pub sin_family: sa_family_t,
        pub sin_port: in_port_t,
        pub sin_addr: in_addr,
        pub sin_zero: [u8; 8],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct sockaddr_in6 {
        pub sin6_family: sa_family_t,
        pub sin6_port: in_port_t,
        pub sin6_flowinfo: u32,
        pub sin6_addr: in6_addr,
        pub sin6_scope_id: u32,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct sockaddr_un {
        pub sun_family: sa_family_t,
        pub sun_path: [c_char; 108],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct sockaddr_storage {
        pub ss_family: sa_family_t,
        __ss_pad1: [u8; 6],
        __ss_align: u64,
        __ss_pad2: [u8; 112],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct addrinfo {
        pub ai_flags: c_int,
        pub ai_family: c_int,
        pub ai_socktype: c_int,
        pub ai_protocol: c_int,
        pub ai_addrlen: socklen_t,
        pub ai_addr: *mut sockaddr,
        pub ai_canonname: *mut c_char,
        pub ai_next: *mut addrinfo,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct shmid_ds {
        pub shm_perm: ipc_perm,
        pub shm_segsz: size_t,
        pub shm_atime: time_t,
        pub shm_dtime: time_t,
        pub shm_ctime: time_t,
        pub shm_cpid: pid_t,
        pub shm_lpid: pid_t,
        pub shm_nattch: c_ulong,
        __unused: [c_ulong; 2],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct ipc_perm {
        pub __key: key_t,
        pub uid: uid_t,
        pub gid: gid_t,
        pub cuid: uid_t,
        pub cgid: gid_t,
        pub mode: c_ushort,
        __pad1: c_ushort,
        __seq: c_ushort,
        __pad2: c_ushort,
        __unused: [c_ulong; 2],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct passwd {
        pub pw_name: *mut c_char,
        pub pw_passwd: *mut c_char,
        pub pw_uid: uid_t,
        pub pw_gid: gid_t,
        pub pw_gecos: *mut c_char,
        pub pw_dir: *mut c_char,
        pub pw_shell: *mut c_char,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct group {
        pub gr_name: *mut c_char,
        pub gr_passwd: *mut c_char,
        pub gr_gid: gid_t,
        pub gr_mem: *mut *mut c_char,
    }

    /// Opaque C `FILE` — only ever used behind pointers.
    #[repr(C)]
    pub struct FILE {
        _opaque: [u8; 0],
    }

    /// Opaque locale handle — single C/POSIX locale, never dereferenced.
    pub type locale_t = *mut c_void;

    // ====================================================================
    // errno: thread-local mirror + __errno_location / __error
    // ====================================================================
    use core::cell::Cell;
    std::thread_local! {
        static ERRNO: Cell<c_int> = const { Cell::new(0) };
        static ERRNO_SLOT: Cell<c_int> = const { Cell::new(0) };
    }

    /// glibc spelling: returns a pointer to the per-thread errno.
    ///
    /// # Safety
    /// The returned pointer is valid for the lifetime of the current thread.
    pub unsafe fn __errno_location() -> *mut c_int {
        ERRNO_SLOT.with(|c| c.as_ptr())
    }

    /// macOS spelling of [`__errno_location`].
    ///
    /// # Safety
    /// See [`__errno_location`].
    pub unsafe fn __error() -> *mut c_int {
        unsafe { __errno_location() }
    }

    fn set_errno(e: c_int) {
        ERRNO_SLOT.with(|c| c.set(e));
    }

    // ====================================================================
    // malloc family — real, over Rust's global allocator. A size header word
    // precedes each block so `free`/`realloc` (which take no size in C) can
    // recover the layout. Self-consistent: blocks never cross to a system free.
    // ====================================================================
    const MHDR: usize = core::mem::size_of::<usize>();

    /// `void *malloc(size_t)`.
    /// # Safety
    /// Standard C `malloc` contract; release with [`free`].
    pub unsafe fn malloc(size: size_t) -> *mut c_void {
        if size == 0 {
            return alloc_block(1);
        }
        alloc_block(size)
    }

    /// `void *calloc(size_t nmemb, size_t size)` — zero-initialized.
    /// # Safety
    /// Standard C `calloc` contract.
    pub unsafe fn calloc(nmemb: size_t, size: size_t) -> *mut c_void {
        let total = match nmemb.checked_mul(size) {
            Some(0) => 1,
            Some(t) => t,
            None => return core::ptr::null_mut(),
        };
        let p = alloc_block(total);
        if !p.is_null() {
            // SAFETY: just allocated `total` writable bytes.
            unsafe { core::ptr::write_bytes(p as *mut u8, 0, total) };
        }
        p
    }

    /// `void *realloc(void *ptr, size_t size)`.
    /// # Safety
    /// `ptr` is null or from this module's allocator; standard C contract.
    pub unsafe fn realloc(ptr: *mut c_void, size: size_t) -> *mut c_void {
        if ptr.is_null() {
            return unsafe { malloc(size) };
        }
        if size == 0 {
            unsafe { free(ptr) };
            return core::ptr::null_mut();
        }
        let base = unsafe { (ptr as *mut u8).sub(MHDR) };
        let old_total = unsafe { (base as *mut usize).read() };
        let old_size = old_total - MHDR;
        let new = alloc_block(size);
        if new.is_null() {
            return core::ptr::null_mut();
        }
        let n = core::cmp::min(old_size, size);
        // SAFETY: both regions are valid for `n` bytes.
        unsafe { core::ptr::copy_nonoverlapping(ptr as *const u8, new as *mut u8, n) };
        unsafe { free(ptr) };
        new
    }

    /// `void free(void *)`. Null is a no-op.
    /// # Safety
    /// `ptr` is null or from this module's allocator.
    pub unsafe fn free(ptr: *mut c_void) {
        if ptr.is_null() {
            return;
        }
        let base = unsafe { (ptr as *mut u8).sub(MHDR) };
        let total = unsafe { (base as *mut usize).read() };
        let layout = core::alloc::Layout::from_size_align(total, MHDR)
            .expect("layout valid at malloc time");
        // SAFETY: base/layout match the original allocation.
        unsafe { std::alloc::dealloc(base, layout) };
    }

    fn alloc_block(size: usize) -> *mut c_void {
        let total = size + MHDR;
        let layout = match core::alloc::Layout::from_size_align(total, MHDR) {
            Ok(l) => l,
            Err(_) => return core::ptr::null_mut(),
        };
        // SAFETY: non-zero layout.
        let base = unsafe { std::alloc::alloc(layout) };
        if base.is_null() {
            return core::ptr::null_mut();
        }
        unsafe { (base as *mut usize).write(total) };
        unsafe { base.add(MHDR) as *mut c_void }
    }

    // ====================================================================
    // time
    // ====================================================================
    /// `time_t time(time_t *tloc)` — seconds since the epoch via `SystemTime`.
    /// # Safety
    /// `tloc` is null or a writable `*mut time_t`.
    pub unsafe fn time(tloc: *mut time_t) -> time_t {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as time_t)
            .unwrap_or(0);
        if !tloc.is_null() {
            unsafe { *tloc = secs };
        }
        secs
    }

    pub unsafe fn getpid() -> pid_t {
        // Single process; a stable nonzero pid is all callers need.
        1
    }
    pub unsafe fn geteuid() -> uid_t {
        0
    }
    pub unsafe fn getuid() -> uid_t {
        0
    }
    pub unsafe fn setenv(_n: *const c_char, _v: *const c_char, _o: c_int) -> c_int {
        0
    }
    pub unsafe fn unsetenv(_n: *const c_char) -> c_int {
        0
    }

    pub unsafe fn sysconf(name: c_int) -> c_long {
        match name {
            _SC_PAGESIZE => 65536,
            _ => -1,
        }
    }

    // ====================================================================
    // ctype helpers (C/POSIX locale: ASCII only, high bytes never fold)
    // ====================================================================
    pub unsafe fn isupper(c: c_int) -> c_int {
        ((c as u8).is_ascii_uppercase() && c < 0x80) as c_int
    }
    pub unsafe fn islower(c: c_int) -> c_int {
        ((c as u8).is_ascii_lowercase() && c < 0x80) as c_int
    }
    pub unsafe fn toupper(c: c_int) -> c_int {
        if (0x61..=0x7a).contains(&c) {
            c - 0x20
        } else {
            c
        }
    }
    pub unsafe fn tolower(c: c_int) -> c_int {
        if (0x41..=0x5a).contains(&c) {
            c + 0x20
        } else {
            c
        }
    }
    pub unsafe fn isalpha(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_alphabetic()) as c_int
    }
    pub unsafe fn isalnum(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_alphanumeric()) as c_int
    }
    pub unsafe fn isdigit(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_digit()) as c_int
    }
    pub unsafe fn isxdigit(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_hexdigit()) as c_int
    }
    pub unsafe fn isspace(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_whitespace()) as c_int
    }
    pub unsafe fn ispunct(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_punctuation()) as c_int
    }
    pub unsafe fn iscntrl(c: c_int) -> c_int {
        ((0..0x80).contains(&c) && (c as u8).is_ascii_control()) as c_int
    }
    pub unsafe fn isprint(c: c_int) -> c_int {
        ((0x20..0x7f).contains(&c)) as c_int
    }

    // ====================================================================
    // C string / mem builtins (used by c2rust-translated code, e.g. gram).
    // These mirror the libc memory/string primitives.
    // ====================================================================
    pub type useconds_t = u32;
    pub unsafe fn usleep(_usec: useconds_t) -> c_int {
        0
    }
    pub unsafe fn getppid() -> pid_t {
        1
    }
    pub unsafe fn realpath(_path: *const c_char, resolved: *mut c_char) -> *mut c_char {
        // No real FS resolution single-user wasm: report failure.
        set_errno(ENOSYS);
        let _ = resolved;
        core::ptr::null_mut()
    }
    pub unsafe fn getpwuid(_uid: uid_t) -> *mut passwd {
        // No passwd database single-user wasm; mirror "no entry" (errno unset).
        set_errno(0);
        core::ptr::null_mut()
    }

    /// `void *memcpy(void *dst, const void *src, size_t n)`.
    /// # Safety
    /// Standard C `memcpy` contract (non-overlapping regions of `n` bytes).
    pub unsafe fn memcpy(dst: *mut c_void, src: *const c_void, n: size_t) -> *mut c_void {
        unsafe { core::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, n) };
        dst
    }
    /// `void *memmove(void *dst, const void *src, size_t n)`.
    /// # Safety
    /// Standard C `memmove` contract.
    pub unsafe fn memmove(dst: *mut c_void, src: *const c_void, n: size_t) -> *mut c_void {
        unsafe { core::ptr::copy(src as *const u8, dst as *mut u8, n) };
        dst
    }
    /// `void *memset(void *s, int c, size_t n)`.
    /// # Safety
    /// Standard C `memset` contract.
    pub unsafe fn memset(s: *mut c_void, c: c_int, n: size_t) -> *mut c_void {
        unsafe { core::ptr::write_bytes(s as *mut u8, c as u8, n) };
        s
    }
    /// `int memcmp(const void *a, const void *b, size_t n)`.
    /// # Safety
    /// Both pointers are valid for `n` bytes.
    pub unsafe fn memcmp(a: *const c_void, b: *const c_void, n: size_t) -> c_int {
        let sa = unsafe { core::slice::from_raw_parts(a as *const u8, n) };
        let sb = unsafe { core::slice::from_raw_parts(b as *const u8, n) };
        match sa.cmp(sb) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        }
    }
    /// `void *memchr(const void *s, int c, size_t n)`.
    /// # Safety
    /// `s` is valid for `n` bytes.
    pub unsafe fn memchr(s: *const c_void, c: c_int, n: size_t) -> *mut c_void {
        let bytes = unsafe { core::slice::from_raw_parts(s as *const u8, n) };
        match bytes.iter().position(|&b| b == c as u8) {
            Some(i) => unsafe { (s as *mut u8).add(i) as *mut c_void },
            None => core::ptr::null_mut(),
        }
    }
    /// `size_t strlen(const char *s)`.
    /// # Safety
    /// `s` is a valid NUL-terminated C string.
    pub unsafe fn strlen(s: *const c_char) -> size_t {
        let mut n = 0usize;
        while unsafe { *s.add(n) } != 0 {
            n += 1;
        }
        n
    }
    /// `size_t strnlen(const char *s, size_t maxlen)`.
    /// # Safety
    /// `s` is valid for up to `maxlen` bytes.
    pub unsafe fn strnlen(s: *const c_char, maxlen: size_t) -> size_t {
        let mut n = 0usize;
        while n < maxlen && unsafe { *s.add(n) } != 0 {
            n += 1;
        }
        n
    }
    /// `int strcmp(const char *a, const char *b)`.
    /// # Safety
    /// Both are valid NUL-terminated C strings.
    pub unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
        let mut i = 0usize;
        loop {
            let ca = unsafe { *a.add(i) };
            let cb = unsafe { *b.add(i) };
            if ca != cb {
                return (ca as u8 as c_int) - (cb as u8 as c_int);
            }
            if ca == 0 {
                return 0;
            }
            i += 1;
        }
    }
    /// `int strncmp(const char *a, const char *b, size_t n)`.
    /// # Safety
    /// Both are valid for up to `n` bytes.
    pub unsafe fn strncmp(a: *const c_char, b: *const c_char, n: size_t) -> c_int {
        let mut i = 0usize;
        while i < n {
            let ca = unsafe { *a.add(i) };
            let cb = unsafe { *b.add(i) };
            if ca != cb {
                return (ca as u8 as c_int) - (cb as u8 as c_int);
            }
            if ca == 0 {
                return 0;
            }
            i += 1;
        }
        0
    }
    /// `char *strchr(const char *s, int c)`.
    /// # Safety
    /// `s` is a valid NUL-terminated C string.
    pub unsafe fn strchr(s: *const c_char, c: c_int) -> *mut c_char {
        let mut i = 0usize;
        loop {
            let cc = unsafe { *s.add(i) };
            if cc as u8 as c_int == (c & 0xff) {
                return unsafe { s.add(i) as *mut c_char };
            }
            if cc == 0 {
                return core::ptr::null_mut();
            }
            i += 1;
        }
    }
    /// `char *strcpy(char *dst, const char *src)`.
    /// # Safety
    /// `dst` has room for `src` (incl. NUL); standard C contract.
    pub unsafe fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char {
        let n = unsafe { strlen(src) } + 1;
        unsafe { core::ptr::copy_nonoverlapping(src, dst, n) };
        dst
    }

    // ====================================================================
    // strerror / strsignal — static stand-in strings.
    // ====================================================================
    static UNKNOWN_ERR: &[u8] = b"wasm: errno (no strerror)\0";
    static UNKNOWN_SIG: &[u8] = b"wasm: signal (no strsignal)\0";
    static EMPTY: &[u8] = b"\0";

    pub unsafe fn strerror(_errnum: c_int) -> *mut c_char {
        UNKNOWN_ERR.as_ptr() as *mut c_char
    }
    pub unsafe fn strsignal(_sig: c_int) -> *mut c_char {
        UNKNOWN_SIG.as_ptr() as *mut c_char
    }
    pub unsafe fn gai_strerror(_e: c_int) -> *const c_char {
        UNKNOWN_ERR.as_ptr() as *const c_char
    }

    // ====================================================================
    // File / VFS syscalls.
    //
    // wasm64-unknown-unknown provides no `std::fs` and no wasi imports, so
    // these raw POSIX-shaped entry points return an ENOSYS-style failure
    // (errno set, -1/null result) rather than performing I/O. The real datadir
    // I/O for single-user `postgres --single` is intended to be routed through
    // a host-import VFS behind the file *seams* (smgr/fd seams), not these raw
    // libc entry points; keeping them as failing-but-linkable stubs lets the
    // binary LINK while the VFS is wired separately. The common open/read/
    // write/stat/close path can be made real by routing these to host imports.
    // ====================================================================
    macro_rules! enosys_i32 {
        ($($name:ident ( $($a:ident : $t:ty),* $(,)? ) );* $(;)?) => {$(
            #[allow(unused_variables)]
            pub unsafe fn $name($($a : $t),*) -> c_int { set_errno(ENOSYS); -1 }
        )*};
    }

    enosys_i32! {
        close(fd: c_int);
        open(path: *const c_char, flags: c_int, mode: mode_t);
        unlink(path: *const c_char);
        rename(from: *const c_char, to: *const c_char);
        mkdir(path: *const c_char, mode: mode_t);
        rmdir(path: *const c_char);
        stat(path: *const c_char, buf: *mut stat);
        lstat(path: *const c_char, buf: *mut stat);
        fstat(fd: c_int, buf: *mut stat);
        ftruncate(fd: c_int, len: off_t);
        truncate(path: *const c_char, len: off_t);
        access(path: *const c_char, mode: c_int);
        chmod(path: *const c_char, mode: mode_t);
        chown(path: *const c_char, owner: uid_t, group: gid_t);
        symlink(target: *const c_char, link: *const c_char);
        fsync(fd: c_int);
        syncfs(fd: c_int);
        dup(fd: c_int);
        dup2(oldfd: c_int, newfd: c_int);
        fcntl(fd: c_int, cmd: c_int, arg: c_int);
        posix_fadvise(fd: c_int, off: off_t, len: off_t, advice: c_int);
        posix_fallocate(fd: c_int, off: off_t, len: off_t);
        getrlimit(res: c_int, lim: *mut rlimit);
        utime(path: *const c_char, times: *const c_void);
        munmap(addr: *mut c_void, len: size_t);
        pipe(fds: *mut c_int);
    }

    /// `int open(const char*, int, ...)` variadic-mode shim used by some sites.
    /// # Safety
    /// `path` is a valid NUL-terminated C string.
    pub unsafe fn open3(path: *const c_char, flags: c_int, mode: mode_t) -> c_int {
        unsafe { open(path, flags, mode) }
    }

    pub unsafe fn read(_fd: c_int, _buf: *mut c_void, _n: size_t) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn write(fd: c_int, buf: *const c_void, n: size_t) -> ssize_t {
        // Route std streams to the host so log/error output is visible; other
        // fds are ENOSYS until the VFS is wired.
        if fd == STDOUT_FILENO || fd == STDERR_FILENO {
            use std::io::Write as _;
            // SAFETY: caller guarantees `buf` points to `n` readable bytes.
            let slice = unsafe { core::slice::from_raw_parts(buf as *const u8, n) };
            let res = if fd == STDOUT_FILENO {
                std::io::stdout().write_all(slice)
            } else {
                std::io::stderr().write_all(slice)
            };
            return match res {
                Ok(()) => n as ssize_t,
                Err(_) => {
                    set_errno(EIO);
                    -1
                }
            };
        }
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn pread(_fd: c_int, _buf: *mut c_void, _n: size_t, _off: off_t) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn pwrite(_fd: c_int, _buf: *const c_void, _n: size_t, _off: off_t) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn preadv(_fd: c_int, _iov: *const iovec, _cnt: c_int, _off: off_t) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn pwritev(_fd: c_int, _iov: *const iovec, _cnt: c_int, _off: off_t) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn lseek(_fd: c_int, _off: off_t, _whence: c_int) -> off_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn readlink(_p: *const c_char, _b: *mut c_char, _s: size_t) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn umask(_mask: mode_t) -> mode_t {
        0o022
    }
    pub unsafe fn mmap(
        _addr: *mut c_void,
        _len: size_t,
        _prot: c_int,
        _flags: c_int,
        _fd: c_int,
        _off: off_t,
    ) -> *mut c_void {
        MAP_FAILED
    }

    // ====================================================================
    // stdio (FILE*) — single-user logging routes through Rust I/O; these are
    // linkable stubs (the syslogger/CSV-log file paths are cfg'd out anyway).
    // ====================================================================
    pub unsafe fn fopen(_path: *const c_char, _mode: *const c_char) -> *mut FILE {
        core::ptr::null_mut()
    }
    pub unsafe fn fclose(_f: *mut FILE) -> c_int {
        0
    }
    pub unsafe fn fflush(_f: *mut FILE) -> c_int {
        0
    }
    pub unsafe fn fwrite(_p: *const c_void, _sz: size_t, n: size_t, _f: *mut FILE) -> size_t {
        n
    }
    pub unsafe fn setvbuf(_f: *mut FILE, _b: *mut c_char, _m: c_int, _s: size_t) -> c_int {
        0
    }
    pub unsafe fn ftello(_f: *mut FILE) -> off_t {
        0
    }

    // ====================================================================
    // process control
    // ====================================================================
    pub unsafe fn atexit(_cb: extern "C" fn()) -> c_int {
        0
    }
    pub unsafe fn exit(code: c_int) -> ! {
        std::process::exit(code)
    }
    pub unsafe fn _exit(code: c_int) -> ! {
        std::process::exit(code)
    }
    pub unsafe fn setsid() -> pid_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn system(_cmd: *const c_char) -> c_int {
        // No shell single-user; report "command not executable".
        set_errno(ENOSYS);
        -1
    }

    /// `pid_t fork(void)` — genuinely unsupportable single-user wasm.
    /// The single-user path must never reach a spawn; reaching here is a bug.
    pub unsafe fn fork() -> pid_t {
        unimplemented!("fork() is not available on wasm single-user postgres")
    }

    // ====================================================================
    // signals (single-user no-ops)
    // ====================================================================
    pub unsafe fn kill(_pid: pid_t, _sig: c_int) -> c_int {
        0
    }
    pub unsafe fn raise(_sig: c_int) -> c_int {
        0
    }
    pub unsafe fn setitimer(
        _which: c_int,
        _new: *const itimerval,
        _old: *mut itimerval,
    ) -> c_int {
        0
    }
    pub unsafe fn sigprocmask(_how: c_int, _set: *const sigset_t, _old: *mut sigset_t) -> c_int {
        0
    }
    pub unsafe fn sigemptyset(set: *mut sigset_t) -> c_int {
        if !set.is_null() {
            unsafe { *set = 0 };
        }
        0
    }
    pub unsafe fn sigfillset(set: *mut sigset_t) -> c_int {
        if !set.is_null() {
            unsafe { *set = u64::MAX };
        }
        0
    }
    pub unsafe fn sigaddset(set: *mut sigset_t, sig: c_int) -> c_int {
        if !set.is_null() && sig > 0 && sig < 64 {
            unsafe { *set |= 1u64 << (sig - 1) };
        }
        0
    }
    pub unsafe fn sigdelset(set: *mut sigset_t, sig: c_int) -> c_int {
        if !set.is_null() && sig > 0 && sig < 64 {
            unsafe { *set &= !(1u64 << (sig - 1)) };
        }
        0
    }

    // ====================================================================
    // SysV shm/sem residue (single-user arena/no-op)
    // ====================================================================
    pub unsafe fn shmctl(_id: c_int, _cmd: c_int, _buf: *mut shmid_ds) -> c_int {
        0
    }
    pub unsafe fn shmget(_key: key_t, _size: size_t, _flag: c_int) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn shmat(_id: c_int, _addr: *const c_void, _flag: c_int) -> *mut c_void {
        usize::MAX as *mut c_void
    }
    pub unsafe fn shmdt(_addr: *const c_void) -> c_int {
        0
    }
    pub unsafe fn shm_open(_name: *const c_char, _flag: c_int, _mode: mode_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn shm_unlink(_name: *const c_char) -> c_int {
        0
    }

    // ====================================================================
    // sockets (single-user: listener cfg'd out — these are linkable stubs)
    // ====================================================================
    pub unsafe fn socket(_d: c_int, _t: c_int, _p: c_int) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn bind(_s: c_int, _a: *const sockaddr, _l: socklen_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn listen(_s: c_int, _b: c_int) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn accept(_s: c_int, _a: *mut sockaddr, _l: *mut socklen_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn connect(_s: c_int, _a: *const sockaddr, _l: socklen_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn send(_s: c_int, _b: *const c_void, _n: size_t, _f: c_int) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn recv(_s: c_int, _b: *mut c_void, _n: size_t, _f: c_int) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn setsockopt(
        _s: c_int,
        _l: c_int,
        _n: c_int,
        _v: *const c_void,
        _len: socklen_t,
    ) -> c_int {
        0
    }
    pub unsafe fn getsockopt(
        _s: c_int,
        _l: c_int,
        _n: c_int,
        _v: *mut c_void,
        _len: *mut socklen_t,
    ) -> c_int {
        0
    }
    pub unsafe fn getsockname(_s: c_int, _a: *mut sockaddr, _l: *mut socklen_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }

    // select(2): single-user has no listener; an empty/no-op fd_set + a
    // "nothing ready" return is sufficient for the linkable stub.
    pub const FD_SETSIZE: usize = 1024;
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct fd_set {
        fds_bits: [u64; FD_SETSIZE / 64],
    }
    #[allow(non_snake_case)]
    pub unsafe fn FD_ZERO(set: *mut fd_set) {
        if !set.is_null() {
            unsafe { (*set).fds_bits = [0u64; FD_SETSIZE / 64] };
        }
    }
    #[allow(non_snake_case)]
    pub unsafe fn FD_SET(fd: c_int, set: *mut fd_set) {
        if !set.is_null() && fd >= 0 && (fd as usize) < FD_SETSIZE {
            let i = fd as usize / 64;
            let b = fd as usize % 64;
            unsafe { (*set).fds_bits[i] |= 1u64 << b };
        }
    }
    #[allow(non_snake_case)]
    pub unsafe fn FD_ISSET(fd: c_int, set: *const fd_set) -> bool {
        if !set.is_null() && fd >= 0 && (fd as usize) < FD_SETSIZE {
            let i = fd as usize / 64;
            let b = fd as usize % 64;
            unsafe { ((*set).fds_bits[i] >> b) & 1 != 0 }
        } else {
            false
        }
    }
    pub unsafe fn select(
        _nfds: c_int,
        _readfds: *mut fd_set,
        _writefds: *mut fd_set,
        _exceptfds: *mut fd_set,
        _timeout: *mut timeval,
    ) -> c_int {
        // Nothing to wait on single-user; report "0 ready" (timeout).
        0
    }

    pub unsafe fn getpeername(_s: c_int, _a: *mut sockaddr, _l: *mut socklen_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn getpeereid(_s: c_int, _uid: *mut uid_t, _gid: *mut gid_t) -> c_int {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn recvfrom(
        _s: c_int,
        _b: *mut c_void,
        _n: size_t,
        _f: c_int,
        _a: *mut sockaddr,
        _l: *mut socklen_t,
    ) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn sendto(
        _s: c_int,
        _b: *const c_void,
        _n: size_t,
        _f: c_int,
        _a: *const sockaddr,
        _l: socklen_t,
    ) -> ssize_t {
        set_errno(ENOSYS);
        -1
    }
    pub unsafe fn getaddrinfo(
        _node: *const c_char,
        _service: *const c_char,
        _hints: *const addrinfo,
        res: *mut *mut addrinfo,
    ) -> c_int {
        if !res.is_null() {
            unsafe { *res = core::ptr::null_mut() };
        }
        // EAI_FAIL — single-user has no name resolution.
        -4
    }
    pub unsafe fn freeaddrinfo(_res: *mut addrinfo) {}
    pub unsafe fn getnameinfo(
        _a: *const sockaddr,
        _alen: socklen_t,
        _host: *mut c_char,
        _hostlen: socklen_t,
        _serv: *mut c_char,
        _servlen: socklen_t,
        _flags: c_int,
    ) -> c_int {
        -4
    }

    pub const AI_NUMERICHOST: c_int = 0x0004;
    pub const AI_NUMERICSERV: c_int = 0x0400;
    pub const NI_MAXHOST: usize = 1025;
    pub const NI_MAXSERV: usize = 32;
    pub const NI_NUMERICSERV: c_int = 2;
    pub const NI_NAMEREQD: c_int = 8;

    /// `int gettimeofday(struct timeval *tv, void *tz)`.
    /// # Safety
    /// `tv` is null or a writable `*mut timeval`.
    pub unsafe fn gettimeofday(tv: *mut timeval, _tz: *mut c_void) -> c_int {
        if !tv.is_null() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            unsafe {
                (*tv).tv_sec = now.as_secs() as time_t;
                (*tv).tv_usec = now.subsec_micros() as suseconds_t;
            }
        }
        0
    }

    /// `pid_t waitpid(pid_t, int *status, int options)` — single-user has no
    /// children; report "no child" (ECHILD).
    /// # Safety
    /// `status` is null or writable.
    pub unsafe fn waitpid(_pid: pid_t, _status: *mut c_int, _options: c_int) -> pid_t {
        set_errno(10 /* ECHILD */);
        -1
    }

    /// `int getpwuid_r(...)` — no passwd db; report "no entry".
    /// # Safety
    /// Standard C `getpwuid_r` contract.
    pub unsafe fn getpwuid_r(
        _uid: uid_t,
        _pwd: *mut passwd,
        _buf: *mut c_char,
        _buflen: size_t,
        result: *mut *mut passwd,
    ) -> c_int {
        if !result.is_null() {
            unsafe { *result = core::ptr::null_mut() };
        }
        0
    }

    // ====================================================================
    // misc
    // ====================================================================
    pub unsafe fn getgrnam(_name: *const c_char) -> *mut group {
        core::ptr::null_mut()
    }

    // ====================================================================
    // locale (single C/POSIX locale — no newlocale machinery)
    // ====================================================================
    pub unsafe fn newlocale(_mask: c_int, _name: *const c_char, _base: locale_t) -> locale_t {
        // Nonzero opaque handle so callers treat it as "valid C locale".
        1usize as locale_t
    }
    pub unsafe fn freelocale(_loc: locale_t) {}
    pub unsafe fn uselocale(_loc: locale_t) -> locale_t {
        1usize as locale_t
    }
    pub unsafe fn setlocale(_cat: c_int, _name: *const c_char) -> *mut c_char {
        static C_LOCALE: &[u8] = b"C\0";
        C_LOCALE.as_ptr() as *mut c_char
    }
    pub unsafe fn strftime_l(
        _s: *mut c_char,
        _max: size_t,
        _fmt: *const c_char,
        _tm: *const tm,
        _loc: locale_t,
    ) -> size_t {
        0
    }
    pub unsafe fn wctype(_name: *const c_char) -> c_ulong {
        0
    }
    pub unsafe fn nl_langinfo(_item: nl_item) -> *mut c_char {
        EMPTY.as_ptr() as *mut c_char
    }
}

#[cfg(target_family = "wasm")]
pub use imp::*;

/// wasm64 stand-ins for `std::os::fd` / `std::os::unix::io` / `std::os::unix::ffi`.
///
/// `wasm64-unknown-unknown` exposes none of `std::os::unix`/`std::os::fd`. The
/// `fd.c` port models open files as `std::fs::File` and converts to/from a raw
/// integer fd via `AsRawFd`/`FromRawFd`/`IntoRawFd`. On bare wasm64 there is no
/// real fd backing a `File`, so the *raw-fd conversion* entry points are
/// genuinely unsupportable here — single-user file I/O is meant to be routed
/// through a host VFS behind the smgr/fd *seams*, not through these conversions.
/// These traits exist so the crate LINKS; calling a raw-fd conversion panics
/// (`unimplemented!`) the same way `fork()` does, which the single-user path
/// must never reach. `OsStrExt::as_bytes` is the one genuinely-portable member
/// and is implemented for real via `OsStr::as_encoded_bytes`.
#[cfg(target_family = "wasm")]
pub mod osfd {
    use std::fs::File;

    /// `std::os::fd::RawFd`.
    pub type RawFd = i32;

    /// `std::os::fd::AsRawFd` / `std::os::unix::io::AsRawFd`.
    pub trait AsRawFd {
        fn as_raw_fd(&self) -> RawFd;
    }
    /// `std::os::fd::FromRawFd` / `std::os::unix::io::FromRawFd`.
    pub trait FromRawFd {
        /// # Safety
        /// Mirrors the std contract; unsupported on wasm64 (panics).
        unsafe fn from_raw_fd(fd: RawFd) -> Self;
    }
    /// `std::os::fd::IntoRawFd` / `std::os::unix::io::IntoRawFd`.
    pub trait IntoRawFd {
        fn into_raw_fd(self) -> RawFd;
    }

    impl AsRawFd for File {
        fn as_raw_fd(&self) -> RawFd {
            unimplemented!("File::as_raw_fd unavailable on wasm64 single-user; route file I/O through the VFS seams")
        }
    }
    impl FromRawFd for File {
        unsafe fn from_raw_fd(_fd: RawFd) -> Self {
            unimplemented!("File::from_raw_fd unavailable on wasm64 single-user; route file I/O through the VFS seams")
        }
    }
    impl IntoRawFd for File {
        fn into_raw_fd(self) -> RawFd {
            unimplemented!("File::into_raw_fd unavailable on wasm64 single-user; route file I/O through the VFS seams")
        }
    }

    /// `std::os::unix::ffi::OsStrExt` (the portable member — real impl).
    pub trait OsStrExt {
        fn as_bytes(&self) -> &[u8];
    }
    impl OsStrExt for std::ffi::OsStr {
        fn as_bytes(&self) -> &[u8] {
            self.as_encoded_bytes()
        }
    }

    /// `std::os::unix::process::ExitStatusExt` (subset used by fd.c).
    pub trait ExitStatusExt {
        fn signal(&self) -> Option<i32>;
    }
    impl ExitStatusExt for std::process::ExitStatus {
        fn signal(&self) -> Option<i32> {
            // wasm has no Unix signal exit; report "not signalled".
            None
        }
    }
}

/// wasm64 stand-ins for `std::os::unix::fs` extension traits.
///
/// `OpenOptionsExt::mode`, `FileExt::{read_at,write_at,write_all_at}`,
/// `MetadataExt::{uid,gid,mode,…}`, `PermissionsExt::{mode,set_mode}` —
/// unavailable on `wasm64-unknown-unknown`. The Unix permission bits are
/// meaningless single-user wasm, so `mode`/`uid`/`gid` report neutral values
/// and the positional file I/O is unsupported (the real datadir I/O is routed
/// through the VFS seams).
#[cfg(target_family = "wasm")]
pub mod osfs {
    use std::fs::{File, Metadata, OpenOptions, Permissions};

    /// `std::os::unix::fs::OpenOptionsExt`.
    pub trait OpenOptionsExt {
        fn mode(&mut self, mode: u32) -> &mut Self;
        fn custom_flags(&mut self, flags: i32) -> &mut Self;
    }
    impl OpenOptionsExt for OpenOptions {
        fn mode(&mut self, _mode: u32) -> &mut Self {
            // No Unix mode on wasm; ignore (single-user owns its datadir).
            self
        }
        fn custom_flags(&mut self, _flags: i32) -> &mut Self {
            self
        }
    }

    /// `std::os::unix::fs::FileExt` (positional I/O subset).
    pub trait FileExt {
        fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;
        fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize>;
        fn write_all_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()>;
    }
    impl FileExt for File {
        fn read_at(&self, _buf: &mut [u8], _offset: u64) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
        }
        fn write_at(&self, _buf: &[u8], _offset: u64) -> std::io::Result<usize> {
            Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
        }
        fn write_all_at(&self, _buf: &[u8], _offset: u64) -> std::io::Result<()> {
            Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
        }
    }

    /// `std::os::unix::fs::MetadataExt` (subset).
    pub trait MetadataExt {
        fn uid(&self) -> u32;
        fn gid(&self) -> u32;
        fn mode(&self) -> u32;
        fn ino(&self) -> u64;
        fn dev(&self) -> u64;
        fn size(&self) -> u64;
    }
    impl MetadataExt for Metadata {
        fn uid(&self) -> u32 {
            0
        }
        fn gid(&self) -> u32 {
            0
        }
        fn mode(&self) -> u32 {
            0o600
        }
        fn ino(&self) -> u64 {
            0
        }
        fn dev(&self) -> u64 {
            0
        }
        fn size(&self) -> u64 {
            self.len()
        }
    }

    /// `std::os::unix::fs::PermissionsExt` (subset).
    pub trait PermissionsExt {
        fn mode(&self) -> u32;
        fn set_mode(&mut self, mode: u32);
        fn from_mode(mode: u32) -> Self;
    }
    impl PermissionsExt for Permissions {
        fn mode(&self) -> u32 {
            // Report owner rw; single-user has no real Unix perms.
            0o600
        }
        fn set_mode(&mut self, _mode: u32) {}
        fn from_mode(_mode: u32) -> Self {
            // No public constructor for Permissions; this path is inert on
            // wasm (callers only set_permissions on real files, which the VFS
            // ignores). A temp file's permissions stand in.
            unimplemented!("Permissions::from_mode unavailable on wasm64 single-user")
        }
    }

    /// `std::os::unix::ffi::OsStrExt` / `OsStringExt` `as_bytes` (both types).
    pub trait OsStrBytesExt {
        fn as_bytes(&self) -> &[u8];
        /// `OsStr::from_bytes` — wasm has no Unix encoding, so we round-trip
        /// through `OsStr::from_encoded_bytes_unchecked`. Single-user paths are
        /// produced by the same wasm side, so the bytes are valid OS-encoded.
        ///
        /// Implemented meaningfully only for `OsStr` (the constructed type);
        /// the `OsString` impl never calls it.
        fn from_bytes(bytes: &[u8]) -> &std::ffi::OsStr {
            // SAFETY: on wasm the OS encoding is the self-consistent encoded
            // form; bytes come from this side's `as_encoded_bytes`/UTF-8.
            unsafe { std::ffi::OsStr::from_encoded_bytes_unchecked(bytes) }
        }
    }
    impl OsStrBytesExt for std::ffi::OsStr {
        fn as_bytes(&self) -> &[u8] {
            self.as_encoded_bytes()
        }
    }
    impl OsStrBytesExt for std::ffi::OsString {
        fn as_bytes(&self) -> &[u8] {
            self.as_os_str().as_encoded_bytes()
        }
    }
}
