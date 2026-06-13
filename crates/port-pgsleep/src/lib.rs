//! `src/port/pgsleep.c` — portable delay handling.
//!
//! This is the non-Windows / frontend implementation; a Windows backend would
//! instead use the signal-aware version in `src/backend/port/win32/signal.c`.

unsafe extern "C" {
    fn nanosleep(rqtp: *const Timespec, rmtp: *mut Timespec) -> core::ffi::c_int;
}

#[repr(C)]
struct Timespec {
    tv_sec: core::ffi::c_long,
    tv_nsec: core::ffi::c_long,
}

/// `pg_usleep` --- delay the specified number of microseconds.
///
/// NOTE: Although the delay is specified in microseconds, older Unixen and
/// Windows use periodic kernel ticks to wake up, which might increase the delay
/// time significantly. We've observed delay increases as large as 20
/// milliseconds on supported platforms.
///
/// On machines where "long" is 32 bits, the maximum delay is ~2000 seconds.
///
/// CAUTION: It's not a good idea to use long sleeps in the backend. They will
/// silently return early if a signal is caught, but that doesn't include
/// latches being set on most OSes, and even signal handlers that set MyLatch
/// might happen to run before the sleep begins, allowing the full delay.
/// Better practice is to use `WaitLatch()` with a timeout, so that backends
/// respond to latches and signals promptly.
pub fn pg_usleep(microsec: i64) {
    if microsec > 0 {
        let delay = Timespec {
            tv_sec: (microsec / 1_000_000) as core::ffi::c_long,
            tv_nsec: ((microsec % 1_000_000) * 1000) as core::ffi::c_long,
        };
        // nanosleep can return early on signal; the return value is intentionally
        // ignored, matching the C `(void) nanosleep(&delay, NULL);`.
        unsafe {
            nanosleep(&delay, core::ptr::null_mut());
        }
    }
}

/// Install this crate's seams.
pub fn init_seams() {
    port_pgsleep_seams::pg_usleep::set(pg_usleep);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn nonpositive_returns_immediately() {
        let start = Instant::now();
        pg_usleep(0);
        pg_usleep(-1000);
        assert!(start.elapsed().as_millis() < 50);
    }

    #[test]
    fn positive_delays_at_least_requested() {
        let start = Instant::now();
        pg_usleep(20_000); // 20 ms
        // Allow early return on signal but in a normal test run it sleeps.
        assert!(start.elapsed().as_micros() >= 10_000);
    }
}
