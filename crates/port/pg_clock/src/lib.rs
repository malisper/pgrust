//! `pg_clock` — the DST P2 clock authority (contract v1, frozen at tag
//! `pg-clock-trait-v1`).
//!
//! Every monotonic read in the process resolves to [`mono_ns`] (contract law
//! §0.2: one monotonic authority) and every SEMANTIC wall read resolves to
//! the `wall_*` family. The [`ClockSource`] trait exists for **shape
//! enforcement and SimClock conformance**, not dynamic dispatch: product
//! builds monomorphize [`ActiveClock`] = [`posix::PosixClock`]; the sim
//! harness selects `SimClock` with the non-default `--cfg pgrust_sim` (set
//! exclusively by the sim-harness RUSTFLAGS — never in `.cargo/config`,
//! product profiles, or fleet submit envs). Product codegen is byte-identical
//! to the raw `clock_gettime` call by construction: every read is an
//! `#[inline]` single-syscall wrapper. No `OnceLock<&'static dyn …>` on any
//! time-read path in product cfg (contract law §0.1, the VFS dispatch
//! mechanism verbatim).
//!
//! Wall = base + mono coupling law (§0.3): any Sim provider derives wall time
//! as `wall_base + mono` from the single mono source, so wall ordering can
//! never disagree with mono ordering (uuidv7 ascending guard, checkpoint
//! pacing, timeout fin_time-vs-timer-deadline coherence). PosixClock reads
//! the two OS clocks independently, as today.
//!
//! Errno-analog rule (§1.1, from VFS §1.1 spirit): pg_clock never panics on
//! clock failure; PosixClock keeps the old waiter RealClock behavior (a
//! failed `clock_gettime` leaves the zeroed timespec, reading as 0).
//!
//! Telemetry stays raw (§0.4): INFRA-classified `Instant` spans are NOT
//! routed here; the reroute is confined to sites that are already syscalls.

pub mod posix;
#[cfg(pgrust_sim)]
pub mod sim;

// Pure parse fns for the sim knobs live outside the cfg so their unit corpus
// runs in every test build (contract §2.3: parse-fn unit-corpus rule).
#[doc(hidden)]
pub mod knob_parse;

use std::time::Duration;

/// Shape-enforcement + Sim-conformance trait. NOT dyn-dispatched in product.
pub trait ClockSource {
    /// CLOCK_MONOTONIC nanoseconds; never regresses.
    fn mono_ns(&self) -> u64;
    /// CLOCK_REALTIME nanoseconds since the Unix epoch (uuidv7 needs ns).
    fn wall_ns(&self) -> i64;
}

#[cfg(not(pgrust_sim))]
pub type ActiveClock = posix::PosixClock;
#[cfg(pgrust_sim)]
pub type ActiveClock = sim::SimClock;

const ACTIVE: ActiveClock = ActiveClock::new();

// ---------------------------------------------------------------------------
// Leaf API — the ONLY surface call sites use. All #[inline], monomorphized.
// ---------------------------------------------------------------------------

/// Monotonic nanoseconds (the one monotonic authority, law §0.2).
#[inline]
pub fn mono_ns() -> u64 {
    ACTIVE.mono_ns()
}

/// Monotonic milliseconds (`WaiterClock::now_ms` compatibility domain:
/// every non-loom WaiterClock provider's `now_ms` delegates here).
#[inline]
pub fn mono_ms() -> i64 {
    (mono_ns() / 1_000_000) as i64
}

/// Wall-clock nanoseconds since the Unix epoch.
#[inline]
pub fn wall_ns() -> i64 {
    ACTIVE.wall_ns()
}

/// Wall-clock microseconds since the Unix epoch (`SystemTime`/`UNIX_EPOCH`
/// replacement for GetCurrentTimestamp-shaped sites).
#[inline]
pub fn wall_us() -> i64 {
    wall_ns().div_euclid(1_000)
}

/// Wall-clock seconds since the Unix epoch (`libc::time` replacement;
/// the `pg_time_t` domain).
#[inline]
pub fn wall_secs() -> i64 {
    wall_ns().div_euclid(1_000_000_000)
}

/// Wall clock as `(seconds, microseconds)` — the `gettimeofday` replacement
/// (elog, adt_timestamp). One underlying clock read; the split carries
/// correctly for pre-epoch instants (`0 <= usec < 1_000_000` always).
#[inline]
pub fn wall_timeval() -> (i64, u32) {
    split_wall_ns(wall_ns())
}

/// Pure carry-split of wall ns into `(secs, usec)`; usec is always in
/// `0..1_000_000` (Euclidean split, so pre-1970 values carry correctly).
#[inline]
pub fn split_wall_ns(ns: i64) -> (i64, u32) {
    let secs = ns.div_euclid(1_000_000_000);
    let usec = (ns.rem_euclid(1_000_000_000) / 1_000) as u32;
    (secs, usec)
}

// ---------------------------------------------------------------------------
// MonoStamp — the Instant::now replacement for SEMANTIC elapsed measurement.
// ---------------------------------------------------------------------------

/// A monotonic origin stamp (`Instant::now` replacement). Reroute call sites
/// should prefer [`MonoStamp::elapsed_ns`]/[`MonoStamp::elapsed_ms`] over
/// [`MonoStamp::elapsed`]: the determinism lint's `.elapsed(` pattern is
/// name-based and would re-flag the rerouted line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MonoStamp(u64);

impl MonoStamp {
    #[inline]
    pub fn now() -> Self {
        MonoStamp(mono_ns())
    }

    /// The raw mono-ns reading this stamp was taken at.
    #[inline]
    pub fn as_ns(&self) -> u64 {
        self.0
    }

    /// Nanoseconds since this stamp (saturating; never negative).
    #[inline]
    pub fn elapsed_ns(&self) -> u64 {
        mono_ns().saturating_sub(self.0)
    }

    /// Milliseconds since this stamp.
    #[inline]
    pub fn elapsed_ms(&self) -> i64 {
        (self.elapsed_ns() / 1_000_000) as i64
    }

    #[inline]
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.elapsed_ns())
    }

    /// Saturating difference against an earlier stamp (0 if `earlier` is
    /// actually later — mirrors `Instant::saturating_duration_since`).
    #[inline]
    pub fn since_ns(&self, earlier: MonoStamp) -> u64 {
        self.0.saturating_sub(earlier.0)
    }

    /// Absolute deadline `d` after THIS stamp (`stamp + d` in the old
    /// Instant arithmetic; saturating).
    #[inline]
    pub fn deadline_after(&self, d: Duration) -> Deadline {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        Deadline(self.0.saturating_add(ns))
    }
}

// ---------------------------------------------------------------------------
// Deadline — absolute mono-domain deadline arithmetic (timeout/bgjobs/CV).
// ---------------------------------------------------------------------------

/// An absolute deadline in the monotonic domain. Saturating construction:
/// `Deadline::after(Duration::MAX)` is a far-future deadline, not a wrap.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Deadline(u64);

impl Deadline {
    /// Deadline `d` from now.
    #[inline]
    pub fn after(d: Duration) -> Self {
        let ns = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
        Deadline(mono_ns().saturating_add(ns))
    }

    /// Deadline at an absolute monotonic millisecond reading (the
    /// `WaiterClock::now_ms` domain). Negative clamps to 0 (already expired).
    #[inline]
    pub fn at_ms(ms: i64) -> Self {
        Deadline((ms.max(0) as u64).saturating_mul(1_000_000))
    }

    /// Deadline at an absolute monotonic nanosecond reading.
    #[inline]
    pub fn at_ns(ns: u64) -> Self {
        Deadline(ns)
    }

    /// The raw absolute mono-ns deadline.
    #[inline]
    pub fn as_ns(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn expired(&self) -> bool {
        mono_ns() >= self.0
    }

    /// Milliseconds until the deadline, rounded UP (a not-yet-expired
    /// deadline never reports 0 — callers sleep at least 1ms instead of
    /// spinning on a sub-ms remainder); 0 iff expired.
    #[inline]
    pub fn remaining_ms(&self) -> i64 {
        let rem = self.0.saturating_sub(mono_ns());
        (rem.div_ceil(1_000_000)) as i64
    }

    /// Time until the deadline (zero if expired).
    #[inline]
    pub fn remaining(&self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(mono_ns()))
    }
}

#[cfg(test)]
mod tests;
