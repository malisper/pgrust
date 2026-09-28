//! PosixClock — the product clock: `#[inline]` single-syscall wrappers over
//! `clock_gettime`. This module is the ONLY sanctioned home of a raw
//! monotonic/wall syscall outside the P0 ledger's INFRA/BOUNDARY residue
//! (contract §0.2: no second `clock_gettime` hub survives outside this file).
//!
//! Errno-analog rule: never panics. A failed `clock_gettime` leaves the
//! zeroed timespec and the read reports 0 — exactly the retired waiter
//! `RealClock` behavior. That masking is why the WASIX clockid ABI mismatch
//! below read as "the clock is stuck at the epoch" instead of an error.

use crate::ClockSource;

/// The `clockid_t` constants + the `clock_gettime` import, declared against the
/// libc this target actually links.
mod abi {
    #[cfg(not(target_vendor = "wasmer"))]
    pub type ClockId = libc::clockid_t;
    #[cfg(not(target_vendor = "wasmer"))]
    pub const MONOTONIC: ClockId = libc::CLOCK_MONOTONIC;
    #[cfg(not(target_vendor = "wasmer"))]
    pub const REALTIME: ClockId = libc::CLOCK_REALTIME;

    /// wasix-libc is a musl fork: its `clockid_t` is a plain int holding the raw
    /// WASI clock id (`__header_time.h` defines `CLOCK_MONOTONIC` as
    /// `__WASI_CLOCKID_MONOTONIC`), not wasi-libc's pointer to an opaque
    /// `__clockid`. The `libc` crate applies the wasi-libc shape to every
    /// `target_os = "wasi"` build — `clockid_t(*const u8)` with
    /// `CLOCK_MONOTONIC = addr_of!(_CLOCK_MONOTONIC)` — and `wasm32-wasmer-wasi`
    /// reports that `target_os`, so through `libc` a data address reaches a host
    /// that wants 0 or 1. It answers EINVAL and, since the error path never
    /// writes the caller's timespec, both clocks then read 0.
    #[cfg(target_vendor = "wasmer")]
    pub type ClockId = libc::c_int;
    #[cfg(target_vendor = "wasmer")]
    pub const MONOTONIC: ClockId = 1; // __WASI_CLOCKID_MONOTONIC
    #[cfg(target_vendor = "wasmer")]
    pub const REALTIME: ClockId = 0; // __WASI_CLOCKID_REALTIME

    #[cfg(target_vendor = "wasmer")]
    extern "C" {
        fn clock_gettime(clock_id: ClockId, tp: *mut libc::timespec) -> libc::c_int;
    }

    /// # Safety
    /// `ts` must point at a live `timespec` for the duration of the call.
    #[inline]
    pub unsafe fn get(clock: ClockId, ts: *mut libc::timespec) -> libc::c_int {
        #[cfg(not(target_vendor = "wasmer"))]
        let rc = unsafe { libc::clock_gettime(clock, ts) };
        #[cfg(target_vendor = "wasmer")]
        let rc = unsafe { clock_gettime(clock, ts) };
        rc
    }
}

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
fn read_clock_ns(clock: abi::ClockId) -> i64 {
    // SAFETY: clock_gettime into a zeroed timespec; on failure the zeroed
    // timespec reads as 0 (never-panic rule).
    let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
    // SAFETY: valid pointer to ts.
    unsafe { abi::get(clock, &mut ts) };
    ts.tv_sec as i64 * 1_000_000_000 + ts.tv_nsec as i64
}

impl ClockSource for PosixClock {
    #[inline]
    fn mono_ns(&self) -> u64 {
        read_clock_ns(abi::MONOTONIC).max(0) as u64
    }

    #[inline]
    fn wall_ns(&self) -> i64 {
        read_clock_ns(abi::REALTIME)
    }
}
