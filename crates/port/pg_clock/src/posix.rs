//! PosixClock — the product clock: `#[inline]` single-syscall wrappers over
//! `clock_gettime`. This module is the ONLY sanctioned home of a raw
//! monotonic/wall syscall outside the P0 ledger's INFRA/BOUNDARY residue
//! (contract §0.2: no second `clock_gettime` hub survives outside this file).
//!
//! Errno-analog rule: never panics. A failed `clock_gettime` leaves the
//! zeroed timespec and the read reports 0 — exactly the retired waiter
//! `RealClock` behavior.

use crate::ClockSource;

/// Zero-sized product clock. `ActiveClock` in every non-`pgrust_sim` build.
#[derive(Clone, Copy, Debug, Default)]
pub struct PosixClock;

impl PosixClock {
    #[inline]
    pub const fn new() -> Self {
        PosixClock
    }
}

#[inline]
fn read_clock_ns(clock: libc::clockid_t) -> i64 {
    // SAFETY: clock_gettime into a zeroed timespec; on failure the zeroed
    // timespec reads as 0 (never-panic rule).
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: valid pointer to ts.
    unsafe { libc::clock_gettime(clock, &mut ts) };
    ts.tv_sec as i64 * 1_000_000_000 + ts.tv_nsec as i64
}

impl ClockSource for PosixClock {
    #[inline]
    fn mono_ns(&self) -> u64 {
        read_clock_ns(libc::CLOCK_MONOTONIC).max(0) as u64
    }

    #[inline]
    fn wall_ns(&self) -> i64 {
        read_clock_ns(libc::CLOCK_REALTIME)
    }
}
