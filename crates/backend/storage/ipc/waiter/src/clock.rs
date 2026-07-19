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
///
/// DST P2 INVARIANT (contract §1.1): the park hook stays HERE — the
/// Slot/SlotInner types live in this crate and the dependency must not
/// invert — but every non-loom `WaiterClock` provider's `now_ms` MUST
/// delegate to `pg_clock::mono_ms()` (the one monotonic authority, law
/// §0.2). Under `--cfg pgrust_sim` the harness-installed provider reads
/// SimClock through the same `pg_clock` API, so waiter deadline math and
/// `pg_clock` reads can never disagree. Parking is pre-block (dispatch cost
/// irrelevant), so the `OnceLock<&'static dyn WaiterClock>` install hook
/// survives for `wait` ONLY. Loom keeps its own `LoomClock` (`--cfg loom`
/// compiles the slot core only; untouched). `VirtualClock` in plain test
/// builds is the deliberate exception: it IS the deterministic test rig and
/// drives its own counter; under `pgrust_sim` it re-homes onto SimClock.
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
    use pgsync::OnceLock;

    pub(crate) struct RealClock;

    impl WaiterClock for RealClock {
        fn now_ms(&self) -> i64 {
            // DST P2: the raw clock_gettime retired into pg_clock/src/posix.rs
            // (one monotonic authority; provider invariant above).
            pg_clock::mono_ms()
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
    #[cfg(not(pgrust_sim))]
    use std::sync::atomic::{AtomicI64, Ordering};
    use pgsync::Mutex;

    pub struct VirtualClock {
        /// Plain test builds drive their own counter (the deterministic test
        /// rig, contract §3.4: waiter suite green unmodified). Under
        /// `--cfg pgrust_sim` this counter RETIRES onto `pg_clock`'s
        /// SimClock — time reads come from `pg_clock::mono_ms()` and
        /// `advance` moves the SimClock timeline, so there is exactly one
        /// timeline (contract §1.1); only the sleeper registry + re-notify
        /// machinery below survives.
        #[cfg(not(pgrust_sim))]
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
                #[cfg(not(pgrust_sim))]
                now: AtomicI64::new(0),
                sleepers: Mutex::new(Vec::new()),
            }
        }

        pub fn now(&self) -> i64 {
            #[cfg(not(pgrust_sim))]
            {
                self.now.load(Ordering::SeqCst)
            }
            #[cfg(pgrust_sim)]
            {
                pg_clock::mono_ms()
            }
        }

        /// Advance virtual time and wake every timed sleeper so it re-judges
        /// its deadline against the new now.
        pub fn advance(&self, ms: i64) {
            #[cfg(not(pgrust_sim))]
            self.now.fetch_add(ms, Ordering::SeqCst);
            #[cfg(pgrust_sim)]
            pg_clock::sim::advance_ms(ms.max(0) as u64);
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
