//! `src/port/pg_strong_random.c` — the cryptographically-strong random-number
//! source, plus the real-time clock read UUID v7 generation uses.
//!
//! This build is configured without OpenSSL (matching the c2rust ground-truth
//! build), and not Win32, so the active variant is the third `#else` branch:
//! read `/dev/urandom` directly. `pg_strong_random_init()` is a no-op for every
//! supported source (the comment in the C file: "Initialization is a no-op for
//! all of them").
//!
//! The generator must be initialized with `pg_strong_random_init()` once per
//! process before any `pg_strong_random()` (notably on the child side of
//! `fork_process`).

/// `void pg_strong_random_init(void)` — per-process initialization. A no-op for
/// the `/dev/urandom` source (as for OpenSSL and Win32).
pub fn pg_strong_random_init() {
    /* No initialization needed */
}

/// `bool pg_strong_random(void *buf, size_t len)` — fill `buf` with `buf.len()`
/// cryptographically strong random bytes, by reading `/dev/urandom`. Returns
/// `false` if the device could not be opened or fully read.
///
/// Faithful to the non-OpenSSL/non-Win32 branch: `open("/dev/urandom",
/// O_RDONLY)`, then `read()` in a loop until `len` bytes are read, retrying on
/// `EINTR`, and `close()` at the end.
pub fn pg_strong_random(buf: &mut [u8]) -> bool {
    // f = open("/dev/urandom", O_RDONLY, 0);
    let path = c"/dev/urandom";
    let f = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY, 0) };
    if f == -1 {
        return false;
    }

    let mut off: usize = 0;
    let total = buf.len();
    let ok = loop {
        if off >= total {
            break true;
        }
        let res = unsafe {
            libc::read(
                f,
                buf[off..].as_mut_ptr() as *mut libc::c_void,
                total - off,
            )
        };
        if res <= 0 {
            // errno == EINTR: interrupted by signal, just retry.
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if res < 0 && errno == libc::EINTR {
                continue;
            }
            break false;
        }
        off += res as usize;
    };

    unsafe {
        libc::close(f);
    }
    ok
}

// ---------------------------------------------------------------------------
// clock_realtime_ns — the real-time clock read uuid.c's UUID v7 path uses.
//
// C (`uuid.c` `get_real_time_ns_ascending`): `clock_gettime(CLOCK_REALTIME, &tp)`
// combined as `tp.tv_sec * 1e9 + tp.tv_nsec` (or `gettimeofday` as
// `tv_sec * 1e9 + tv_usec * 1e3` on platforms without nanosecond precision).
// ---------------------------------------------------------------------------

/// The current real timestamp in nanoseconds since the UNIX epoch.
pub fn clock_realtime_ns() -> i64 {
    let mut tp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut tp) };
    if rc != 0 {
        // Fall back to gettimeofday on the (unexpected) clock_gettime failure.
        let mut tv = libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        };
        unsafe {
            libc::gettimeofday(&mut tv, core::ptr::null_mut());
        }
        return (tv.tv_sec as i64) * 1_000_000_000 + (tv.tv_usec as i64) * 1_000;
    }
    (tp.tv_sec as i64) * 1_000_000_000 + (tp.tv_nsec as i64)
}

/// Install this crate's seam implementations.
pub fn init_seams() {
    pg_strong_random_seams::pg_strong_random::set(pg_strong_random);
    pg_strong_random_seams::pg_strong_random_init::set(pg_strong_random_init);
    pg_strong_random_seams::clock_realtime_ns::set(clock_realtime_ns);
}
