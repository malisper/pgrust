//! Monotonic time source behind a trait so the sizing state machine is
//! deterministically testable (virtual-time unit tests are an M0 gate) and so
//! a future deterministic mode (redesign §2.6) has its hook from day one.

use crate::sync::atomic::{AtomicU64, Ordering};

pub trait Clock: Send + Sync {
    /// Monotonic nanoseconds. Only differences are meaningful.
    fn now_ns(&self) -> u64;
}

/// Production clock. DST P2 (contract §1.3): origin/elapsed delegate to
/// `pg_clock` — the runtime's second clock_gettime hub is retired; only
/// differences are meaningful, exactly as before.
pub struct MonotonicClock {
    origin_ns: u64,
}

impl MonotonicClock {
    pub fn new() -> Self {
        MonotonicClock { origin_ns: pg_clock::mono_ns() }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now_ns(&self) -> u64 {
        pg_clock::mono_ns().saturating_sub(self.origin_ns)
    }
}

/// Virtual clock for deterministic tests: time moves only when advanced
/// (synthetic morsel work advances it by its configured cost).
pub struct VirtualClock {
    ns: AtomicU64,
}

impl VirtualClock {
    pub fn new() -> Self {
        VirtualClock { ns: AtomicU64::new(0) }
    }

    pub fn advance(&self, ns: u64) {
        self.ns.fetch_add(ns, Ordering::SeqCst);
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for VirtualClock {
    fn now_ns(&self) -> u64 {
        self.ns.load(Ordering::SeqCst)
    }
}
