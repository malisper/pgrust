//! Time + park provider (the DST hook). Production uses the monotonic OS
//! clock and real condvar timeouts; deterministic tests install
//! [`virtual_time::VirtualClock`] and drive time explicitly — parks then
//! block until either an unpark or an `advance()` past their deadline, with
//! no real time involved.

use crate::sync::MutexGuard;
use crate::{Slot, SlotInner};

/// Pluggable time + park backend. `wait` blocks on the slot's condvar until
/// notified or `timeout_ms` (per THIS provider's clock) elapses; it returns
/// the reacquired guard and whether the provider considers the wait timed
/// out. Spurious returns are fine — `Slot::park_core` re-evaluates deadlines
/// with `now_ms`.
pub trait WaiterClock: Sync + Send {
    fn now_ms(&self) -> i64;
    fn wait<'a>(
        &self,
        slot: &'a Slot,
        guard: MutexGuard<'a, SlotInner>,
        timeout_ms: Option<i64>,
    ) -> (MutexGuard<'a, SlotInner>, bool);
}

#[cfg(not(loom))]
mod real {
    use super::*;
    use std::sync::OnceLock;

    pub(crate) struct RealClock;

    impl WaiterClock for RealClock {
        fn now_ms(&self) -> i64 {
            // SAFETY: clock_gettime(CLOCK_MONOTONIC) into a zeroed timespec.
            let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
            // SAFETY: valid pointer to ts.
            unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
            ts.tv_sec as i64 * 1000 + ts.tv_nsec as i64 / 1_000_000
        }

        fn wait<'a>(
            &self,
            slot: &'a Slot,
            guard: MutexGuard<'a, SlotInner>,
            timeout_ms: Option<i64>,
        ) -> (MutexGuard<'a, SlotInner>, bool) {
            match timeout_ms {
                Some(t) => {
                    let (g, res) = slot
                        .cv()
                        .wait_timeout(guard, std::time::Duration::from_millis(t.max(0) as u64))
                        .unwrap_or_else(|e| e.into_inner());
                    (g, res.timed_out())
                }
                None => (
                    slot.cv().wait(guard).unwrap_or_else(|e| e.into_inner()),
                    false,
                ),
            }
        }
    }

    static REAL: RealClock = RealClock;
    static PROVIDER: OnceLock<&'static dyn WaiterClock> = OnceLock::new();

    pub(crate) fn provider() -> &'static dyn WaiterClock {
        *PROVIDER.get_or_init(|| &REAL)
    }

    /// Install a provider (tests; once per process, before any park).
    pub fn install(p: &'static dyn WaiterClock) {
        PROVIDER
            .set(p)
            .unwrap_or_else(|_| panic!("waiter clock provider already installed"));
    }
}

#[cfg(not(loom))]
pub(crate) use real::provider;
#[cfg(not(loom))]
pub use real::install;

/// Deterministic virtual-time provider: `now_ms` is a counter advanced only
/// by [`virtual_time::VirtualClock::advance`]; timed waits block on the real
/// condvar (so unparks still deliver) but their deadlines are judged in
/// virtual time, and `advance` re-notifies sleepers so expired deadlines are
/// observed immediately. No test sleeps on the wall clock.
#[cfg(not(loom))]
pub mod virtual_time {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Mutex;

    pub struct VirtualClock {
        now: AtomicI64,
        /// Addresses of slots currently blocked in a timed virtual wait.
        /// SAFETY invariant: VirtualClock is only ever installed for parks
        /// on the process-global waiter slab, whose slots are 'static; an
        /// address is registered strictly for the duration of one wait.
        sleepers: Mutex<Vec<usize>>,
    }

    impl VirtualClock {
        pub const fn new() -> Self {
            VirtualClock {
                now: AtomicI64::new(0),
                sleepers: Mutex::new(Vec::new()),
            }
        }

        pub fn now(&self) -> i64 {
            self.now.load(Ordering::SeqCst)
        }

        /// Advance virtual time and wake every timed sleeper so it re-judges
        /// its deadline against the new now.
        pub fn advance(&self, ms: i64) {
            self.now.fetch_add(ms, Ordering::SeqCst);
            // Snapshot then RELEASE the sleepers lock before touching slot
            // mutexes: a waking sleeper holds its slot mutex while removing
            // itself from sleepers — holding both here is a lock-order
            // inversion (deadlock, found by the virtual-time unit test).
            let addrs: Vec<usize> = self
                .sleepers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            for addr in addrs {
                // SAFETY: see the sleepers field invariant — the address
                // names a live 'static slot registered by a blocked waiter.
                let s = unsafe { &*(addr as *const Slot) };
                // Touch the mutex so the wake cannot race ahead of a sleeper
                // that has decided to wait but not yet blocked.
                drop(s.lock_inner());
                s.cv().notify_all();
            }
        }
    }

    impl Default for VirtualClock {
        fn default() -> Self {
            Self::new()
        }
    }

    impl WaiterClock for VirtualClock {
        fn now_ms(&self) -> i64 {
            self.now()
        }

        fn wait<'a>(
            &self,
            slot: &'a Slot,
            guard: MutexGuard<'a, SlotInner>,
            timeout_ms: Option<i64>,
        ) -> (MutexGuard<'a, SlotInner>, bool) {
            if timeout_ms.is_none() {
                return (
                    slot.cv().wait(guard).unwrap_or_else(|e| e.into_inner()),
                    false,
                );
            }
            self.sleepers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(slot as *const Slot as usize);
            // Block until an unpark or an advance() notification; park_core
            // re-evaluates the (virtual) deadline. The real-time timeout here
            // is only a liveness guard against a test forgetting to advance.
            let (g, _) = slot
                .cv()
                .wait_timeout(guard, std::time::Duration::from_secs(60))
                .unwrap_or_else(|e| e.into_inner());
            let mut sleepers = self.sleepers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(pos) = sleepers
                .iter()
                .position(|a| *a == slot as *const Slot as usize)
            {
                sleepers.swap_remove(pos);
            }
            (g, false)
        }
    }
}
