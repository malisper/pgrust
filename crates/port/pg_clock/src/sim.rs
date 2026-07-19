//! SimClock — the `--cfg pgrust_sim` clock (P2 skeleton, contract §1.1).
//!
//! One `AtomicU64` mono counter + a wall base; `wall_ns = wall_base + mono`
//! (coupling law §0.3), so wall ordering can never disagree with mono
//! ordering. Three modes (`PGRUST_SIM_CLOCK_MODE`):
//!
//! * `frozen` (default) — mono never advances; the P2 determinism smoke.
//! * `tick:<ns>`        — mono += quantum per mono read; the fallback lever
//!                        for corpus items that busy-check elapsed time.
//! * `driven`           — [`advance_ns`] only; the P3 scheduler mode.
//!
//! `PGRUST_SIM_WALL_BASE` sets the wall base (unix-epoch ns; default
//! 2026-01-01T00:00:00Z). Both knobs are read ONLY in this module, which
//! exists only under `pgrust_sim` — no product reader exists (§2.3).
//!
//! P3 note: `advance_ns` is the driven-mode hook the scheduler will own;
//! waiter's `virtual_time::VirtualClock` advance couples its sleeper
//! re-notify to it (one timeline, §1.1).

use crate::knob_parse::{
    parse_clock_mode, parse_wall_base, SimClockMode, DEFAULT_WALL_BASE_NS,
};
use crate::ClockSource;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

static MONO_NS: AtomicU64 = AtomicU64::new(0);

struct SimConfig {
    mode: SimClockMode,
    wall_base_ns: i64,
}

fn config() -> &'static SimConfig {
    static CONFIG: OnceLock<SimConfig> = OnceLock::new();
    CONFIG.get_or_init(|| SimConfig {
        mode: std::env::var("PGRUST_SIM_CLOCK_MODE")
            .map(|v| parse_clock_mode(&v))
            .unwrap_or(SimClockMode::Frozen),
        wall_base_ns: std::env::var("PGRUST_SIM_WALL_BASE")
            .ok()
            .and_then(|v| parse_wall_base(&v))
            .unwrap_or(DEFAULT_WALL_BASE_NS),
    })
}

/// Advance the sim timeline (driven mode's only lever; legal in any mode —
/// a frozen corpus simply never calls it). The single mono authority moves;
/// wall moves with it by the coupling law.
pub fn advance_ns(ns: u64) {
    MONO_NS.fetch_add(ns, Ordering::SeqCst);
}

pub fn advance_ms(ms: u64) {
    advance_ns(ms.saturating_mul(1_000_000));
}

/// The configured wall base (exposed for conformance tests).
pub fn wall_base_ns() -> i64 {
    config().wall_base_ns
}

/// Zero-sized sim clock over the module-global timeline. `ActiveClock`
/// under `pgrust_sim`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SimClock;

impl SimClock {
    #[inline]
    pub const fn new() -> Self {
        SimClock
    }
}

impl ClockSource for SimClock {
    fn mono_ns(&self) -> u64 {
        match config().mode {
            SimClockMode::Tick(q) => MONO_NS.fetch_add(q, Ordering::SeqCst) + q,
            SimClockMode::Frozen | SimClockMode::Driven => MONO_NS.load(Ordering::SeqCst),
        }
    }

    fn wall_ns(&self) -> i64 {
        // Coupling law §0.3: wall = base + mono, from the single mono source.
        // (In tick mode the embedded mono read ticks the quantum — a wall
        // read IS a mono read under the coupling law.)
        config()
            .wall_base_ns
            .saturating_add_unsigned(self.mono_ns())
    }
}
