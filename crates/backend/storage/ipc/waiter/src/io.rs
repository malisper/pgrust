//! IoToken registration surface (parallelism-redesign §2.9, reserved at M0).
//!
//! An in-flight IO is (owning ring id, cqe id, list of Waiter handles):
//! tasks park on the token via their Waiter handle, and ANY completing
//! thread unparks every registered handle — the generalization of bufmgr's
//! io_wref/WaitIO any-thread-completes discipline onto the Waiter. M0 ships
//! the API + unit/loom coverage only; the uring wiring lands in M1.
//!
//! Contract:
//!   * `register(handle)` under the token mutex: returns `AlreadyCompleted`
//!     if the IO finished first (the completed-fast-path — a late registrant
//!     must NOT park; it returns immediately).
//!   * `complete()` is idempotent; the first completer drains and unparks
//!     the whole registrant list. Register/complete race atomically under
//!     the mutex: a registrant is either drained (gets the unpark) or
//!     observes `AlreadyCompleted` — never neither.
//!   * Parkers loop on `is_completed()` around `waiter::park()` (standard
//!     predicate loop): an unpark that lands before the park is latched
//!     (wake-before-park), spurious wakes re-test the predicate.
//!   * A registrant that abandons the wait (error/cancel) may leave its
//!     handle registered: the eventual complete() then delivers at worst a
//!     spurious `Notified` to that thread's next park (or a `Stale` no-op if
//!     the incarnation retired) — consumed by predicate loops by design.
//!     A deregistration API is deliberately omitted from the M0 surface.

use crate::sync::Mutex;

/// Result of registering on an IoToken.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum IoRegister {
    /// Handle recorded; the completer will unpark it.
    Registered,
    /// The IO already completed: do not park, proceed immediately.
    AlreadyCompleted,
}

struct IoState<H> {
    completed: bool,
    registrants: Vec<H>,
}

/// Core token, generic over the handle representation so the loom models
/// can drive it against model slots. Production code uses [`IoToken`].
pub struct IoTokenCore<H> {
    ring_id: u32,
    cqe_id: u64,
    state: Mutex<IoState<H>>,
}

impl<H> IoTokenCore<H> {
    pub fn new(ring_id: u32, cqe_id: u64) -> Self {
        IoTokenCore {
            ring_id,
            cqe_id,
            state: Mutex::new(IoState {
                completed: false,
                registrants: Vec::new(),
            }),
        }
    }

    /// The owning ring (M1: which uring's cq the completion arrives on).
    pub fn ring_id(&self) -> u32 {
        self.ring_id
    }

    /// The completion id within the owning ring.
    pub fn cqe_id(&self) -> u64 {
        self.cqe_id
    }

    fn lock(&self) -> crate::sync::MutexGuard<'_, IoState<H>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Register a waiter handle for the completion wake. Completed-fast-path:
    /// a token that already completed refuses the registration and the
    /// caller returns immediately instead of parking.
    pub fn register(&self, handle: H) -> IoRegister {
        let mut g = self.lock();
        if g.completed {
            return IoRegister::AlreadyCompleted;
        }
        g.registrants.push(handle);
        IoRegister::Registered
    }

    /// True once any thread completed the token.
    pub fn is_completed(&self) -> bool {
        self.lock().completed
    }

    /// Mark complete and hand every registrant to `unpark` (ANY thread may
    /// complete, including one that is itself registered). Idempotent: only
    /// the first completer drains; returns how many handles it delivered.
    pub fn complete_with(&self, mut unpark: impl FnMut(H)) -> usize {
        let drained = {
            let mut g = self.lock();
            if g.completed {
                return 0;
            }
            g.completed = true;
            std::mem::take(&mut g.registrants)
        };
        let n = drained.len();
        for h in drained {
            unpark(h);
        }
        n
    }
}

/// Production token: registrants are [`crate::WakerHandle`]s, completion
/// unparks through the global waiter table.
#[cfg(not(loom))]
pub type IoToken = IoTokenCore<crate::WakerHandle>;

#[cfg(not(loom))]
impl IoToken {
    /// Unpark all registered waiters (any completing thread).
    pub fn complete(&self) -> usize {
        self.complete_with(|h| {
            crate::unpark(h);
        })
    }
}
