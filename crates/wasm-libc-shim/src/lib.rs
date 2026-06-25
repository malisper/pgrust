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

    /// Read the calling thread's wasm `errno`.
    ///
    /// `std::io::Error::last_os_error()` reads std's own (always-0) errno on
    /// `wasm64-unknown-unknown`, so ported code that needs the *real* errno set
    /// by these shims (EMFILE retry loops, ENOENT tolerance, …) must call this.
    pub fn errno() -> c_int {
        ERRNO_SLOT.with(|c| c.get())
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
        // std's SystemTime panics on wasm64-unknown-unknown; read the host clock.
        let secs = (now_unix_nanos() / 1_000_000_000) as time_t;
        if !tloc.is_null() {
            unsafe { *tloc = secs };
        }
        secs
    }

    pub unsafe fn getpid() -> pid_t {
        // Single process; a stable nonzero pid is all callers need.
        1
    }
    /// The single wasm "user" id. Deliberately NON-root (postgres refuses to
    /// run as root in `check_root`, and a wasm sandbox has no privilege model).
    /// `WasmMetadata::uid()` reports the same value so the `checkDataDir`
    /// datadir-ownership interlock (`st_uid == geteuid()`) passes for the lone
    /// user.
    pub const WASM_UID: uid_t = 1000;
    pub unsafe fn geteuid() -> uid_t {
        WASM_UID
    }
    pub unsafe fn getuid() -> uid_t {
        WASM_UID
    }
    /// Fixed effective group id for the single-user wasm engine (mirrors
    /// `WASM_UID`).
    pub const WASM_GID: gid_t = 1000;
    pub unsafe fn getegid() -> gid_t {
        WASM_GID
    }
    pub unsafe fn getgid() -> gid_t {
        WASM_GID
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
    pub unsafe fn getpwuid(uid: uid_t) -> *mut passwd {
        // There is no passwd database on wasm, but the single-user startup path
        // (`get_user_name_or_exit` → `getpwuid(geteuid()).pw_name`) needs a
        // bootstrap user name to proceed. Synthesize a fixed "postgres" entry
        // for the lone wasm user (WASM_UID); any other uid reports "no entry".
        if uid != WASM_UID {
            set_errno(0);
            return core::ptr::null_mut();
        }
        // Static NUL-terminated fields + a static `passwd` pointing at them. The
        // wasm backend is single-threaded at startup, so a shared static is safe
        // and matches the C contract (getpwuid returns a pointer into a static
        // libc buffer the caller copies out immediately).
        static PW_NAME: &[u8] = b"postgres\0";
        static EMPTY: &[u8] = b"\0";
        static SLASH: &[u8] = b"/\0";
        static SHELL: &[u8] = b"/bin/sh\0";
        static mut PW: passwd = passwd {
            pw_name: core::ptr::null_mut(),
            pw_passwd: core::ptr::null_mut(),
            pw_uid: WASM_UID,
            pw_gid: WASM_UID,
            pw_gecos: core::ptr::null_mut(),
            pw_dir: core::ptr::null_mut(),
            pw_shell: core::ptr::null_mut(),
        };
        set_errno(0);
        // SAFETY: single-threaded startup; the statics outlive the returned
        // pointer (they are 'static), and the caller copies pw_name out at once.
        unsafe {
            let p = core::ptr::addr_of_mut!(PW);
            (*p).pw_name = PW_NAME.as_ptr() as *mut c_char;
            (*p).pw_passwd = EMPTY.as_ptr() as *mut c_char;
            (*p).pw_gecos = EMPTY.as_ptr() as *mut c_char;
            (*p).pw_dir = SLASH.as_ptr() as *mut c_char;
            (*p).pw_shell = SHELL.as_ptr() as *mut c_char;
            p
        }
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
    // ====================================================================
    // Host VFS imports.
    //
    // On `wasm64-unknown-unknown` there is no wasi and no `std::fs`, so the raw
    // POSIX file syscalls are routed to *host imports* the wasm runtime (the
    // wasmtime harness in `tools/wasm-harness`) provides. The host operates on
    // real files under a preopened datadir. Every import returns an i64: a
    // non-negative result on success (fd / byte count / 0), or `-errno` on
    // failure (negated Linux errno). The shim translates `-errno` back into the
    // POSIX `-1` + thread-local errno convention the fd.c port expects.
    //
    // All pointers are wasm linear-memory offsets (the host reads/writes guest
    // memory directly). `path`/`buf` are byte pointers with explicit lengths so
    // the host never has to scan for a NUL across the membrane.
    // ====================================================================
    #[link(wasm_import_module = "pgvfs")]
    extern "C" {
        fn host_open(path: *const u8, path_len: usize, flags: c_int, mode: mode_t) -> i64;
        fn host_close(fd: c_int) -> i64;
        fn host_read(fd: c_int, buf: *mut u8, n: usize) -> i64;
        fn host_write(fd: c_int, buf: *const u8, n: usize) -> i64;
        fn host_pread(fd: c_int, buf: *mut u8, n: usize, off: i64) -> i64;
        fn host_pwrite(fd: c_int, buf: *const u8, n: usize, off: i64) -> i64;
        fn host_lseek(fd: c_int, off: i64, whence: c_int) -> i64;
        fn host_fsync(fd: c_int) -> i64;
        fn host_ftruncate(fd: c_int, len: i64) -> i64;
        /// Fills a fixed-layout 64-byte record:
        /// [0]=st_mode(u32) [1]=pad [2]=st_size(i64) [3]=st_mtime(i64)
        /// [4]=st_ino(u64) [5]=st_dev(u64) [6]=st_nlink(u64) [7]=st_blocks(i64).
        fn host_stat(path: *const u8, path_len: usize, follow: c_int, out: *mut i64) -> i64;
        fn host_fstat(fd: c_int, out: *mut i64) -> i64;
        fn host_unlink(path: *const u8, path_len: usize) -> i64;
        fn host_mkdir(path: *const u8, path_len: usize, mode: mode_t) -> i64;
        fn host_rmdir(path: *const u8, path_len: usize) -> i64;
        fn host_rename(from: *const u8, from_len: usize, to: *const u8, to_len: usize) -> i64;
        fn host_access(path: *const u8, path_len: usize, mode: c_int) -> i64;
        fn host_readlink(path: *const u8, path_len: usize, buf: *mut u8, n: usize) -> i64;
        /// Write `n` bytes to the host stdout (fd 1) / stderr (fd 2). std's
        /// stdout/stderr are no-ops on `wasm64-unknown-unknown`, so the single-
        /// user query results + LOG lines route here instead.
        fn host_stdout(buf: *const u8, n: usize) -> i64;
        fn host_stderr(buf: *const u8, n: usize) -> i64;
        /// Read up to `n` bytes of the SQL input stream (host stdin) into `buf`;
        /// returns the byte count (0 = EOF) or -errno. std's stdin is a no-op on
        /// `wasm64-unknown-unknown`.
        fn host_stdin(buf: *mut u8, n: usize) -> i64;
        /// Number of process arguments the host wants to pass as the guest's
        /// `argv` (there is no WASI argv on `wasm64-unknown-unknown`).
        fn host_argc() -> i64;
        /// Copy argument `idx` into `buf` (max `n` bytes); returns its byte
        /// length (which may exceed `n`, signalling truncation) or -errno.
        fn host_argv(idx: i32, buf: *mut u8, n: usize) -> i64;
        /// Opens a directory stream; returns a non-negative dir handle or -errno.
        fn host_opendir(path: *const u8, path_len: usize) -> i64;
        /// Reads the next entry name into `buf` (no NUL); returns the byte
        /// length (0 = end of stream) or -errno.
        fn host_readdir(handle: c_int, buf: *mut u8, n: usize) -> i64;
        fn host_closedir(handle: c_int) -> i64;
        /// Terminate the guest with `code`. `std::process::exit`/`abort` trap
        /// (`unreachable`) on `wasm64-unknown-unknown` since there is no exit
        /// syscall, so the backend's process-exit paths route here; the harness
        /// turns it into a clean store shutdown. Never returns.
        fn host_proc_exit(code: i32) -> !;
        /// Wall-clock nanoseconds since the Unix epoch. `std::time::SystemTime`
        /// is unsupported on `wasm64-unknown-unknown` (its `now()` panics), so
        /// all clock reads route to the host.
        fn host_now_ns() -> i64;
    }

    /// Exit the process via the host (`std::process::exit` traps on
    /// `wasm64-unknown-unknown`). Never returns.
    pub fn proc_exit(code: i32) -> ! {
        // SAFETY: host import; diverges (the host stops the guest).
        unsafe { host_proc_exit(code) }
    }

    /// Wall-clock nanoseconds since the Unix epoch, from the host (std's
    /// `SystemTime::now()` panics on `wasm64-unknown-unknown`).
    pub fn now_unix_nanos() -> i64 {
        // SAFETY: host import.
        unsafe { host_now_ns() }
    }

    // Public thin wrappers over the raw host imports, used by the `osfile`
    // carrier (`WasmFile`). These return the raw host i64 (non-negative = ok,
    // negative = `-errno`); the caller maps to `io::Error`.
    /// # Safety
    /// `buf` points to `n` writable bytes.
    pub unsafe fn host_read_pub(fd: c_int, buf: *mut u8, n: usize) -> i64 {
        unsafe { host_read(fd, buf, n) }
    }
    /// # Safety
    /// `buf` points to `n` readable bytes.
    pub unsafe fn host_write_pub(fd: c_int, buf: *const u8, n: usize) -> i64 {
        unsafe { host_write(fd, buf, n) }
    }
    /// # Safety
    /// `fd` is a live host fd.
    pub unsafe fn host_lseek_pub(fd: c_int, off: i64, whence: c_int) -> i64 {
        unsafe { host_lseek(fd, off, whence) }
    }
    /// # Safety
    /// `fd` is a live host fd.
    pub unsafe fn host_fsync_pub(fd: c_int) -> i64 {
        unsafe { host_fsync(fd) }
    }
    /// # Safety
    /// `fd` is a live host fd.
    pub unsafe fn host_ftruncate_pub(fd: c_int, len: i64) -> i64 {
        unsafe { host_ftruncate(fd, len) }
    }
    /// # Safety
    /// `fd` is a live host fd.
    pub unsafe fn host_close_pub(fd: c_int) -> i64 {
        unsafe { host_close(fd) }
    }
    /// # Safety
    /// `out` points to 8 writable i64 words.
    pub unsafe fn host_fstat_pub(fd: c_int, out: *mut i64) -> i64 {
        unsafe { host_fstat(fd, out) }
    }

    /// Length of a NUL-terminated C string (the host imports take explicit
    /// lengths, so the membrane never scans guest memory).
    unsafe fn cstr_len(p: *const c_char) -> usize {
        if p.is_null() {
            return 0;
        }
        let mut n = 0usize;
        // SAFETY: caller guarantees `p` is NUL-terminated.
        while unsafe { *p.add(n) } != 0 {
            n += 1;
        }
        n
    }

    /// Map a host `i64` result (non-negative = ok, negative = `-errno`) onto the
    /// POSIX `-1` + errno convention, returning the success value as `i64`.
    fn host_ret(r: i64) -> i64 {
        if r < 0 {
            set_errno((-r) as c_int);
            -1
        } else {
            r
        }
    }

    pub unsafe fn open(path: *const c_char, flags: c_int, mode: mode_t) -> c_int {
        let len = unsafe { cstr_len(path) };
        host_ret(unsafe { host_open(path as *const u8, len, flags, mode) }) as c_int
    }

    /// Process arguments from the host (the wasm `bin`'s `argv`). On
    /// `wasm64-unknown-unknown` `std::env::args()` is empty, so the `postgres`
    /// entry shell calls this under `cfg(wasm)` to receive `["postgres",
    /// "--single", "-D", …]`.
    pub fn host_args() -> Vec<String> {
        // SAFETY: host imports; argc is small, each arg fits in the buffer.
        let argc = unsafe { host_argc() };
        if argc <= 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(argc as usize);
        for i in 0..argc as i32 {
            let mut buf = vec![0u8; 4096];
            let n = unsafe { host_argv(i, buf.as_mut_ptr(), buf.len()) };
            if n < 0 {
                break;
            }
            let n = (n as usize).min(buf.len());
            out.push(String::from_utf8_lossy(&buf[..n]).into_owned());
        }
        out
    }

    /// `int open(const char*, int, ...)` variadic-mode shim used by some sites.
    /// # Safety
    /// `path` is a valid NUL-terminated C string.
    pub unsafe fn open3(path: *const c_char, flags: c_int, mode: mode_t) -> c_int {
        unsafe { open(path, flags, mode) }
    }

    pub unsafe fn close(fd: c_int) -> c_int {
        host_ret(unsafe { host_close(fd) }) as c_int
    }

    pub unsafe fn read(fd: c_int, buf: *mut c_void, n: size_t) -> ssize_t {
        host_ret(unsafe { host_read(fd, buf as *mut u8, n) }) as ssize_t
    }
    pub unsafe fn write(fd: c_int, buf: *const c_void, n: size_t) -> ssize_t {
        // Route std streams to the host stdout/stderr writers (std's stdout/
        // stderr are no-ops on wasm64-unknown-unknown, so query results + LOG
        // lines must cross to the host).
        if fd == STDOUT_FILENO {
            return host_ret(unsafe { host_stdout(buf as *const u8, n) }) as ssize_t;
        }
        if fd == STDERR_FILENO {
            return host_ret(unsafe { host_stderr(buf as *const u8, n) }) as ssize_t;
        }
        host_ret(unsafe { host_write(fd, buf as *const u8, n) }) as ssize_t
    }

    /// Public helper: write `bytes` to host stdout (the single-user `print!`
    /// path routes here under cfg(wasm)).
    pub fn stdout_write(bytes: &[u8]) {
        // SAFETY: bytes is a valid readable slice.
        unsafe {
            host_stdout(bytes.as_ptr(), bytes.len());
        }
    }
    /// Public helper: write `bytes` to host stderr.
    pub fn stderr_write(bytes: &[u8]) {
        // SAFETY: bytes is a valid readable slice.
        unsafe {
            host_stderr(bytes.as_ptr(), bytes.len());
        }
    }
    /// Public helper: read up to `buf.len()` bytes from host stdin; returns the
    /// number read (0 = EOF).
    pub fn stdin_read(buf: &mut [u8]) -> usize {
        // SAFETY: buf is a valid writable slice.
        let r = unsafe { host_stdin(buf.as_mut_ptr(), buf.len()) };
        if r <= 0 {
            0
        } else {
            (r as usize).min(buf.len())
        }
    }
    pub unsafe fn pread(fd: c_int, buf: *mut c_void, n: size_t, off: off_t) -> ssize_t {
        host_ret(unsafe { host_pread(fd, buf as *mut u8, n, off) }) as ssize_t
    }
    pub unsafe fn pwrite(fd: c_int, buf: *const c_void, n: size_t, off: off_t) -> ssize_t {
        host_ret(unsafe { host_pwrite(fd, buf as *const u8, n, off) }) as ssize_t
    }
    pub unsafe fn preadv(fd: c_int, iov: *const iovec, cnt: c_int, mut off: off_t) -> ssize_t {
        // Decompose into per-segment pread (the host VFS speaks pread).
        let mut total: ssize_t = 0;
        for i in 0..cnt as isize {
            // SAFETY: caller guarantees `iov[0..cnt]` are valid.
            let v = unsafe { &*iov.offset(i) };
            if v.iov_len == 0 {
                continue;
            }
            let got = unsafe { pread(fd, v.iov_base, v.iov_len, off) };
            if got < 0 {
                return if total > 0 { total } else { got };
            }
            total += got;
            off += got as off_t;
            if (got as size_t) < v.iov_len {
                break; // short read
            }
        }
        total
    }
    pub unsafe fn pwritev(fd: c_int, iov: *const iovec, cnt: c_int, mut off: off_t) -> ssize_t {
        let mut total: ssize_t = 0;
        for i in 0..cnt as isize {
            // SAFETY: caller guarantees `iov[0..cnt]` are valid.
            let v = unsafe { &*iov.offset(i) };
            if v.iov_len == 0 {
                continue;
            }
            let put = unsafe { pwrite(fd, v.iov_base, v.iov_len, off) };
            if put < 0 {
                return if total > 0 { total } else { put };
            }
            total += put;
            off += put as off_t;
            if (put as size_t) < v.iov_len {
                break; // short write
            }
        }
        total
    }
    pub unsafe fn lseek(fd: c_int, off: off_t, whence: c_int) -> off_t {
        host_ret(unsafe { host_lseek(fd, off, whence) })
    }
    pub unsafe fn readlink(p: *const c_char, b: *mut c_char, s: size_t) -> ssize_t {
        let len = unsafe { cstr_len(p) };
        host_ret(unsafe { host_readlink(p as *const u8, len, b as *mut u8, s) }) as ssize_t
    }

    /// Decode the fixed 8-word `host_stat`/`host_fstat` record into a `stat`.
    fn fill_stat(words: &[i64; 8], buf: *mut stat) {
        // SAFETY: caller passes a valid, writable `stat` pointer.
        unsafe {
            let s = &mut *buf;
            *s = core::mem::zeroed();
            s.st_mode = words[0] as u32;
            s.st_size = words[2];
            s.st_mtime = words[3];
            s.st_mtime_nsec = 0;
            s.st_ino = words[4] as u64;
            s.st_dev = words[5] as u64;
            s.st_nlink = words[6] as u64;
            s.st_blocks = words[7];
            s.st_blksize = 8192;
        }
    }

    pub unsafe fn stat(path: *const c_char, buf: *mut stat) -> c_int {
        let len = unsafe { cstr_len(path) };
        let mut words = [0i64; 8];
        let r = host_ret(unsafe { host_stat(path as *const u8, len, 1, words.as_mut_ptr()) });
        if r < 0 {
            return -1;
        }
        fill_stat(&words, buf);
        0
    }
    pub unsafe fn lstat(path: *const c_char, buf: *mut stat) -> c_int {
        let len = unsafe { cstr_len(path) };
        let mut words = [0i64; 8];
        let r = host_ret(unsafe { host_stat(path as *const u8, len, 0, words.as_mut_ptr()) });
        if r < 0 {
            return -1;
        }
        fill_stat(&words, buf);
        0
    }
    pub unsafe fn fstat(fd: c_int, buf: *mut stat) -> c_int {
        let mut words = [0i64; 8];
        let r = host_ret(unsafe { host_fstat(fd, words.as_mut_ptr()) });
        if r < 0 {
            return -1;
        }
        fill_stat(&words, buf);
        0
    }
    pub unsafe fn unlink(path: *const c_char) -> c_int {
        let len = unsafe { cstr_len(path) };
        host_ret(unsafe { host_unlink(path as *const u8, len) }) as c_int
    }
    pub unsafe fn mkdir(path: *const c_char, mode: mode_t) -> c_int {
        let len = unsafe { cstr_len(path) };
        host_ret(unsafe { host_mkdir(path as *const u8, len, mode) }) as c_int
    }
    pub unsafe fn rmdir(path: *const c_char) -> c_int {
        let len = unsafe { cstr_len(path) };
        host_ret(unsafe { host_rmdir(path as *const u8, len) }) as c_int
    }
    pub unsafe fn rename(from: *const c_char, to: *const c_char) -> c_int {
        let fl = unsafe { cstr_len(from) };
        let tl = unsafe { cstr_len(to) };
        host_ret(unsafe { host_rename(from as *const u8, fl, to as *const u8, tl) }) as c_int
    }
    pub unsafe fn access(path: *const c_char, mode: c_int) -> c_int {
        let len = unsafe { cstr_len(path) };
        host_ret(unsafe { host_access(path as *const u8, len, mode) }) as c_int
    }
    pub unsafe fn ftruncate(fd: c_int, len: off_t) -> c_int {
        host_ret(unsafe { host_ftruncate(fd, len) }) as c_int
    }
    /// `truncate(path, len)` — the path-based POSIX form. The host VFS exposes
    /// only fd-based `host_ftruncate`, so implement the path form as
    /// `open(O_WRONLY) + ftruncate + close`. `mdunlink`/`mdtruncate` call the
    /// raw `truncate(2)` on a relation's path to release space before unlink;
    /// without this it returned ENOSYS, so DROP/TRUNCATE logged a spurious
    /// `could not truncate file "..." (os error 38)` WARNING (the operation
    /// still succeeded because the file is unlinked anyway, but the noise is
    /// alarming). Mirrors `open()`/`ftruncate()`/`close()` above.
    pub unsafe fn truncate(path: *const c_char, len: off_t) -> c_int {
        let plen = unsafe { cstr_len(path) };
        let fd = host_ret(unsafe { host_open(path as *const u8, plen, O_WRONLY, 0) }) as c_int;
        if fd < 0 {
            return -1;
        }
        let r = host_ret(unsafe { host_ftruncate(fd, len) }) as c_int;
        let _ = unsafe { host_close(fd) };
        r
    }
    pub unsafe fn fsync(fd: c_int) -> c_int {
        host_ret(unsafe { host_fsync(fd) }) as c_int
    }
    pub unsafe fn fdatasync(fd: c_int) -> c_int {
        host_ret(unsafe { host_fsync(fd) }) as c_int
    }

    /// Host directory-stream handle exposed to the `osfs`/`read_dir` shims.
    pub unsafe fn opendir_host(path: *const u8, len: usize) -> i64 {
        host_ret(unsafe { host_opendir(path, len) })
    }
    pub unsafe fn readdir_host(handle: c_int, buf: *mut u8, n: usize) -> i64 {
        host_ret(unsafe { host_readdir(handle, buf, n) })
    }
    pub unsafe fn closedir_host(handle: c_int) -> i64 {
        host_ret(unsafe { host_closedir(handle) })
    }

    // The remaining file-ish syscalls have no single-user effect or are
    // genuinely unsupported; keep them as inert/ENOSYS stubs.
    macro_rules! enosys_i32 {
        ($($name:ident ( $($a:ident : $t:ty),* $(,)? ) );* $(;)?) => {$(
            #[allow(unused_variables)]
            pub unsafe fn $name($($a : $t),*) -> c_int { set_errno(ENOSYS); -1 }
        )*};
    }

    enosys_i32! {
        chmod(path: *const c_char, mode: mode_t);
        chown(path: *const c_char, owner: uid_t, group: gid_t);
        symlink(target: *const c_char, link: *const c_char);
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
            // std's SystemTime panics on wasm64-unknown-unknown; use the host.
            let ns = now_unix_nanos();
            unsafe {
                (*tv).tv_sec = (ns / 1_000_000_000) as time_t;
                (*tv).tv_usec = ((ns % 1_000_000_000) / 1_000) as suseconds_t;
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

    /// `int getpwuid_r(...)` — synthesize the fixed "postgres" entry for the lone
    /// wasm user (mirrors `getpwuid`); any other uid reports "no entry".
    /// # Safety
    /// Standard C `getpwuid_r` contract.
    pub unsafe fn getpwuid_r(
        uid: uid_t,
        pwd: *mut passwd,
        buf: *mut c_char,
        buflen: size_t,
        result: *mut *mut passwd,
    ) -> c_int {
        if uid != WASM_UID || pwd.is_null() || buf.is_null() {
            if !result.is_null() {
                unsafe { *result = core::ptr::null_mut() };
            }
            return 0;
        }
        // Lay out "postgres\0" + a few empties in the caller's buffer.
        const NAME: &[u8] = b"postgres\0";
        const SLASH: &[u8] = b"/\0";
        const SHELL: &[u8] = b"/bin/sh\0";
        let need = NAME.len() + 1 /*passwd empty*/ + 1 /*gecos empty*/ + SLASH.len() + SHELL.len();
        if (buflen as usize) < need {
            return 34; // ERANGE
        }
        // SAFETY: buf has >= `need` bytes (checked); pwd is a valid passwd.
        unsafe {
            let mut p = buf as *mut u8;
            let name_ptr = p;
            core::ptr::copy_nonoverlapping(NAME.as_ptr(), p, NAME.len());
            p = p.add(NAME.len());
            let empty_ptr = p;
            *p = 0;
            p = p.add(1);
            let gecos_ptr = p;
            *p = 0;
            p = p.add(1);
            let dir_ptr = p;
            core::ptr::copy_nonoverlapping(SLASH.as_ptr(), p, SLASH.len());
            p = p.add(SLASH.len());
            let shell_ptr = p;
            core::ptr::copy_nonoverlapping(SHELL.as_ptr(), p, SHELL.len());

            (*pwd).pw_name = name_ptr as *mut c_char;
            (*pwd).pw_passwd = empty_ptr as *mut c_char;
            (*pwd).pw_uid = WASM_UID;
            (*pwd).pw_gid = WASM_UID;
            (*pwd).pw_gecos = gecos_ptr as *mut c_char;
            (*pwd).pw_dir = dir_ptr as *mut c_char;
            (*pwd).pw_shell = shell_ptr as *mut c_char;
            if !result.is_null() {
                *result = pwd;
            }
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

/// wasm64 stand-ins for the free `std::fs::{metadata,remove_file,rename,
/// remove_dir,create_dir_all}` functions, which perform no I/O on
/// `wasm64-unknown-unknown`. These route to the host-VFS libc shims. The
/// `backend-storage-file-fd` crate calls `fscompat::*` (aliased to plain
/// `std::fs::*` natively) at the scattered direct-`std::fs` sites.
#[cfg(target_family = "wasm")]
pub mod fscompat {
    use super::imp as libc;
    use super::osfile::WasmMetadata;
    use std::io;
    use std::path::Path;

    fn cpath(p: &Path) -> Vec<u8> {
        use super::osfd::OsStrExt as _;
        let mut v = p.as_os_str().as_bytes().to_vec();
        v.push(0);
        v
    }

    /// `std::fs::read` — open O_RDONLY, read to EOF via the host VFS.
    pub fn read(p: impl AsRef<Path>) -> io::Result<Vec<u8>> {
        use super::osfd::FromRawFd as _;
        use std::io::Read as _;
        let c = cpath(p.as_ref());
        // SAFETY: c is NUL-terminated.
        let fd = unsafe { libc::open(c.as_ptr() as *const i8, libc::O_RDONLY, 0) };
        if fd < 0 {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        // SAFETY: freshly opened owned fd → WasmFile (closes on drop).
        let mut f = unsafe { super::osfile::WasmFile::from_raw_fd(fd) };
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(buf)
    }
    /// `std::fs::read_to_string` — `read` + UTF-8 validation.
    pub fn read_to_string(p: impl AsRef<Path>) -> io::Result<String> {
        let bytes = read(p)?;
        String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "stream did not contain valid UTF-8"))
    }

    pub fn metadata(p: impl AsRef<Path>) -> io::Result<WasmMetadata> {
        let c = cpath(p.as_ref());
        let mut st: libc::stat = unsafe { core::mem::zeroed() };
        // SAFETY: c is NUL-terminated; st is a valid stat.
        if unsafe { libc::stat(c.as_ptr() as *const i8, &mut st) } < 0 {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        Ok(WasmMetadata::from_stat(st.st_mode, st.st_size as u64))
    }
    pub fn symlink_metadata(p: impl AsRef<Path>) -> io::Result<WasmMetadata> {
        let c = cpath(p.as_ref());
        let mut st: libc::stat = unsafe { core::mem::zeroed() };
        // SAFETY: c is NUL-terminated; st is a valid stat.
        if unsafe { libc::lstat(c.as_ptr() as *const i8, &mut st) } < 0 {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        Ok(WasmMetadata::from_stat(st.st_mode, st.st_size as u64))
    }
    pub fn remove_file(p: impl AsRef<Path>) -> io::Result<()> {
        let c = cpath(p.as_ref());
        // SAFETY: c is NUL-terminated.
        if unsafe { libc::unlink(c.as_ptr() as *const i8) } < 0 {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        Ok(())
    }
    pub fn remove_dir(p: impl AsRef<Path>) -> io::Result<()> {
        let c = cpath(p.as_ref());
        // SAFETY: c is NUL-terminated.
        if unsafe { libc::rmdir(c.as_ptr() as *const i8) } < 0 {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        Ok(())
    }
    pub fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
        let f = cpath(from.as_ref());
        let t = cpath(to.as_ref());
        // SAFETY: both are NUL-terminated.
        if unsafe { libc::rename(f.as_ptr() as *const i8, t.as_ptr() as *const i8) } < 0 {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        Ok(())
    }
    pub fn create_dir_all(p: impl AsRef<Path>) -> io::Result<()> {
        // Single-level mkdir is enough for the single-user paths that reach
        // here; create parents best-effort.
        let path = p.as_ref();
        let c = cpath(path);
        // SAFETY: c is NUL-terminated.
        let r = unsafe { libc::mkdir(c.as_ptr() as *const i8, 0o700) };
        if r < 0 && libc::errno() != libc::EEXIST {
            return Err(io::Error::from_raw_os_error(libc::errno()));
        }
        Ok(())
    }
}

/// wasm64 owned-file carrier standing in for `std::fs::File`.
///
/// On `wasm64-unknown-unknown` `std::fs::File` is the uninhabited never type
/// (`File(!)`) — it cannot be constructed and `std::fs` performs no I/O. The
/// `fd.c` port stores the kernel handle as a `std::fs::File` and bridges to the
/// raw integer fd via `AsRawFd`/`FromRawFd`/`IntoRawFd` before doing the actual
/// `pread`/`pwrite`/`close`. `WasmFile` is a real, constructible carrier with
/// the same surface: it owns an integer fd from the host VFS (closing it on
/// drop) and routes the `Read`/`Write`/`Seek`/metadata methods to the host
/// imports. The `backend-storage-file-fd` crate aliases its `OsFile` to this on
/// wasm and to `std::fs::File` natively.
#[cfg(target_family = "wasm")]
pub mod osfile {
    use super::imp as libc;
    use std::io::{self, Read, Seek, SeekFrom, Write};

    /// Owned host file descriptor; closes on drop (unless `into_raw_fd`'d out).
    #[derive(Debug)]
    pub struct WasmFile {
        fd: i32,
    }

    /// wasm64 `std::fs::OpenOptions` stand-in producing a host-VFS `WasmFile`.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct WasmOpenOptions {
        read: bool,
        write: bool,
        append: bool,
        truncate: bool,
        create: bool,
        create_new: bool,
        mode: u32,
        custom_flags: i32,
    }
    impl WasmOpenOptions {
        pub fn new() -> WasmOpenOptions {
            WasmOpenOptions { mode: 0o666, ..Default::default() }
        }
        pub fn read(&mut self, v: bool) -> &mut Self {
            self.read = v;
            self
        }
        pub fn write(&mut self, v: bool) -> &mut Self {
            self.write = v;
            self
        }
        pub fn append(&mut self, v: bool) -> &mut Self {
            self.append = v;
            self
        }
        pub fn truncate(&mut self, v: bool) -> &mut Self {
            self.truncate = v;
            self
        }
        pub fn create(&mut self, v: bool) -> &mut Self {
            self.create = v;
            self
        }
        pub fn create_new(&mut self, v: bool) -> &mut Self {
            self.create_new = v;
            self
        }
        pub fn mode(&mut self, m: u32) -> &mut Self {
            self.mode = m;
            self
        }
        pub fn custom_flags(&mut self, f: i32) -> &mut Self {
            self.custom_flags = f;
            self
        }
        pub fn open(&self, path: impl AsRef<std::path::Path>) -> io::Result<WasmFile> {
            use super::osfd::{FromRawFd as _, OsStrExt as _};
            let mut flags = if self.read && self.write {
                libc::O_RDWR
            } else if self.write || self.append {
                libc::O_WRONLY
            } else {
                libc::O_RDONLY
            };
            if self.append {
                flags |= libc::O_APPEND;
            }
            if self.truncate {
                flags |= libc::O_TRUNC;
            }
            if self.create_new {
                flags |= libc::O_CREAT | libc::O_EXCL;
            } else if self.create {
                flags |= libc::O_CREAT;
            }
            flags |= self.custom_flags;
            let mut cpath = path.as_ref().as_os_str().as_bytes().to_vec();
            cpath.push(0);
            // SAFETY: cpath is NUL-terminated.
            let fd = unsafe { libc::open(cpath.as_ptr() as *const i8, flags, self.mode) };
            if fd < 0 {
                return Err(io::Error::from_raw_os_error(libc::errno()));
            }
            // SAFETY: freshly opened owned host fd.
            Ok(unsafe { WasmFile::from_raw_fd(fd) })
        }
    }

    impl WasmFile {
        /// `std::fs::File::open` — open read-only via the host VFS.
        pub fn open(path: impl AsRef<std::path::Path>) -> io::Result<WasmFile> {
            WasmOpenOptions::new().read(true).open(path)
        }
        /// `std::fs::File::create` — open write/create/truncate via the host VFS.
        pub fn create(path: impl AsRef<std::path::Path>) -> io::Result<WasmFile> {
            WasmOpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
        }
        /// `std::fs::File::options` — a fresh `WasmOpenOptions` builder.
        pub fn options() -> WasmOpenOptions {
            WasmOpenOptions::new()
        }
    }

    impl WasmFile {
        /// Read all remaining bytes (current offset → EOF) onto `buf`.
        pub fn metadata(&self) -> io::Result<WasmMetadata> {
            let mut words = [0i64; 8];
            // SAFETY: words is a valid 8-word buffer.
            let r = unsafe { libc::host_fstat_pub(self.fd, words.as_mut_ptr()) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(WasmMetadata { size: words[2] as u64, mode: words[0] as u32 })
        }
        /// `File::sync_all` → host `fsync`.
        pub fn sync_all(&self) -> io::Result<()> {
            // SAFETY: fd is owned and valid.
            let r = unsafe { libc::host_fsync_pub(self.fd) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(())
        }
        /// `File::sync_data` → host `fsync` (no fdatasync distinction here).
        pub fn sync_data(&self) -> io::Result<()> {
            self.sync_all()
        }
        /// `File::set_len` → host `ftruncate`.
        pub fn set_len(&self, len: u64) -> io::Result<()> {
            // SAFETY: fd is owned and valid.
            let r = unsafe { libc::host_ftruncate_pub(self.fd, len as i64) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(())
        }
        /// `File::try_clone` — duplicate by re-wrapping the SAME fd is unsafe
        /// (double close); the host VFS has no dup, so this is unsupported.
        pub fn try_clone(&self) -> io::Result<WasmFile> {
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
        /// `FileExt::read_at` → host `pread`.
        pub fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
            // SAFETY: buf is a valid writable slice.
            let r = unsafe { libc::pread(self.fd, buf.as_mut_ptr() as *mut _, buf.len(), offset as i64) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error(libc::errno()));
            }
            Ok(r as usize)
        }
        /// `FileExt::write_at` → host `pwrite`.
        pub fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
            // SAFETY: buf is a valid readable slice.
            let r = unsafe { libc::pwrite(self.fd, buf.as_ptr() as *const _, buf.len(), offset as i64) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error(libc::errno()));
            }
            Ok(r as usize)
        }
        /// `FileExt::write_all_at` → looped `write_at`.
        pub fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
            while !buf.is_empty() {
                match self.write_at(buf, offset) {
                    Ok(0) => {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "failed to write whole buffer",
                        ))
                    }
                    Ok(n) => {
                        buf = &buf[n..];
                        offset += n as u64;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        }
        /// `File::set_permissions` — single-user wasm has no Unix perms; no-op.
        pub fn set_permissions(&self, _perm: WasmPermissions) -> io::Result<()> {
            Ok(())
        }
    }

    impl Read for WasmFile {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            // SAFETY: buf is a valid writable slice.
            let r = unsafe { libc::host_read_pub(self.fd, buf.as_mut_ptr(), buf.len()) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(r as usize)
        }
    }
    impl Write for WasmFile {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            // SAFETY: buf is a valid readable slice.
            let r = unsafe { libc::host_write_pub(self.fd, buf.as_ptr(), buf.len()) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(r as usize)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Seek for WasmFile {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            let (off, whence) = match pos {
                SeekFrom::Start(n) => (n as i64, 0),     // SEEK_SET
                SeekFrom::End(n) => (n, 2),              // SEEK_END
                SeekFrom::Current(n) => (n, 1),         // SEEK_CUR
            };
            // SAFETY: fd is owned and valid.
            let r = unsafe { libc::host_lseek_pub(self.fd, off, whence) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(r as u64)
        }
    }

    impl Drop for WasmFile {
        fn drop(&mut self) {
            if self.fd >= 0 {
                // SAFETY: fd is owned; closing once on drop.
                unsafe {
                    libc::host_close_pub(self.fd);
                }
            }
        }
    }

    impl super::osfd::AsRawFd for WasmFile {
        fn as_raw_fd(&self) -> super::osfd::RawFd {
            self.fd
        }
    }
    impl super::osfd::FromRawFd for WasmFile {
        unsafe fn from_raw_fd(fd: super::osfd::RawFd) -> Self {
            WasmFile { fd }
        }
    }
    impl super::osfd::IntoRawFd for WasmFile {
        fn into_raw_fd(self) -> super::osfd::RawFd {
            let fd = self.fd;
            core::mem::forget(self); // ownership transferred to caller
            fd
        }
    }

    /// wasm64 `std::fs::ReadDir` stand-in over the host `opendir`/`readdir`
    /// imports. Yields `io::Result<WasmDirEntry>`; closes the host stream on drop.
    #[derive(Debug)]
    pub struct WasmReadDir {
        handle: i32,
    }

    /// wasm64 `std::fs::DirEntry` stand-in — carries the entry name only (all
    /// `ReadDirExtended` reads is `file_name()`).
    #[derive(Debug)]
    pub struct WasmDirEntry {
        name: std::ffi::OsString,
    }
    impl WasmDirEntry {
        pub fn file_name(&self) -> std::ffi::OsString {
            self.name.clone()
        }
    }

    impl WasmReadDir {
        /// `opendir(path)` — open a host directory stream (or `io::Error`).
        pub fn open(path: &[u8]) -> io::Result<WasmReadDir> {
            // SAFETY: path slice is valid for its length.
            let r = unsafe { libc::opendir_host(path.as_ptr(), path.len()) };
            if r < 0 {
                return Err(io::Error::from_raw_os_error((-r) as i32));
            }
            Ok(WasmReadDir { handle: r as i32 })
        }
    }

    impl Iterator for WasmReadDir {
        type Item = io::Result<WasmDirEntry>;
        fn next(&mut self) -> Option<Self::Item> {
            let mut buf = [0u8; 512];
            // SAFETY: buf is a valid writable 512-byte buffer.
            let r = unsafe { libc::readdir_host(self.handle, buf.as_mut_ptr(), buf.len()) };
            if r < 0 {
                return Some(Err(io::Error::from_raw_os_error((-r) as i32)));
            }
            if r == 0 {
                return None; // end of stream
            }
            let name = std::ffi::OsString::from(
                String::from_utf8_lossy(&buf[..r as usize]).into_owned(),
            );
            Some(Ok(WasmDirEntry { name }))
        }
    }

    impl Drop for WasmReadDir {
        fn drop(&mut self) {
            // SAFETY: handle is an owned host dir stream.
            unsafe {
                libc::closedir_host(self.handle);
            }
        }
    }

    /// Minimal `std::fs::Metadata` stand-in (size + mode are all fd.c reads).
    #[derive(Clone, Copy, Debug)]
    pub struct WasmMetadata {
        size: u64,
        mode: u32,
    }
    impl WasmMetadata {
        /// Build from a `stat`'s `st_mode`/`st_size` (used by `fscompat`).
        pub fn from_stat(mode: u32, size: u64) -> WasmMetadata {
            WasmMetadata { size, mode }
        }
        pub fn len(&self) -> u64 {
            self.size
        }
        pub fn is_empty(&self) -> bool {
            self.size == 0
        }
        pub fn is_dir(&self) -> bool {
            (self.mode & libc::S_IFMT) == libc::S_IFDIR
        }
        pub fn is_file(&self) -> bool {
            (self.mode & libc::S_IFMT) == libc::S_IFREG
        }
        pub fn file_type(&self) -> WasmFileType {
            WasmFileType { mode: self.mode }
        }
        // `std::os::unix::fs::MetadataExt`-shaped inherent accessors (single-user
        // wasm has no real Unix owner/perm metadata; report neutral values so the
        // datadir-ownership interlock in `checkDataDir` passes for the lone user).
        pub fn uid(&self) -> u32 {
            // Match the shim's `geteuid()` (WASM_UID) so the datadir-ownership
            // interlock in `checkDataDir` passes for the lone wasm user.
            super::imp::WASM_UID as u32
        }
        pub fn gid(&self) -> u32 {
            super::imp::WASM_UID as u32
        }
        pub fn mode(&self) -> u32 {
            self.mode
        }
        pub fn ino(&self) -> u64 {
            0
        }
        pub fn dev(&self) -> u64 {
            0
        }
        pub fn size(&self) -> u64 {
            self.size
        }
        /// `std::fs::Metadata::permissions()` stand-in.
        pub fn permissions(&self) -> WasmPermissions {
            WasmPermissions { mode: self.mode & 0o7777 }
        }
        pub fn modified(&self) -> std::io::Result<std::time::SystemTime> {
            Ok(std::time::SystemTime::UNIX_EPOCH)
        }
    }

    /// wasm64 `std::fs::Permissions` stand-in (only `mode()` is read here).
    #[derive(Clone, Copy, Debug)]
    pub struct WasmPermissions {
        mode: u32,
    }
    impl WasmPermissions {
        pub fn mode(&self) -> u32 {
            self.mode
        }
        pub fn set_mode(&mut self, mode: u32) {
            self.mode = mode;
        }
        pub fn readonly(&self) -> bool {
            self.mode & 0o222 == 0
        }
    }

    /// wasm64 `std::fs::FileType` stand-in.
    #[derive(Clone, Copy, Debug)]
    pub struct WasmFileType {
        mode: u32,
    }
    impl WasmFileType {
        pub fn is_symlink(&self) -> bool {
            (self.mode & libc::S_IFMT) == libc::S_IFLNK
        }
        pub fn is_dir(&self) -> bool {
            (self.mode & libc::S_IFMT) == libc::S_IFDIR
        }
        pub fn is_file(&self) -> bool {
            (self.mode & libc::S_IFMT) == libc::S_IFREG
        }
    }
}
