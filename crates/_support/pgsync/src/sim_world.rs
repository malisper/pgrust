//! Sim world (`--cfg pgrust_sim`): std-backed permit-hooked wrappers.
//!
//! Shape (contract §2.1/§2.2): every wrapper is a std primitive PLUS permit
//! bookkeeping. The wait/wake lists live in the WRAPPER, so every wakee /
//! next-holder / winning-sender choice is a seeded pick
//! ([`crate::sim::hooks::SchedulerHooks::pick_waiter`]). With no scheduler
//! installed ([`hooks::installed`] = None) every op degrades to the plain
//! std call — today's sim binaries are byte-for-byte unaffected.
//!
//! Soundness of the wait protocol (CHECK-BEFORE-PARK): a `wake` targeting a
//! thread that is not parked (e.g. one preempted at a seeded `touch`) may be
//! DROPPED by the scheduler, so a wrapper must never park unless its wait
//! predicate still holds. Every blocking op is a predicate loop that checks
//! its wait condition IMMEDIATELY BEFORE `block_on`/`timed_park`, with no
//! hook call between the check and the park (the caller holds the permit
//! across that gap, so no waker can run in it), and every waker falsifies
//! the predicate (removes the wakee from the wait list) BEFORE its `wake`
//! fires. This matters concretely for Condvar waits: the guard drop between
//! enqueue and park embeds unlock hooks (pick/wake/touch — a preemption
//! point), so a notify can consume the enqueued waiter before it ever
//! parks; check-before-park turns that into an immediate return instead of
//! a lost wake (adversarial-review finding BLOCKING-1, 2026-07-18). A
//! spurious permit grant re-checks and re-parks.
//!
//! Composites (Barrier / Once / OnceLock / mpsc — and lib.rs's Semaphore /
//! ParkLot) are built OVER the wrapped Mutex/Condvar, so their blocking is
//! hook-routed automatically; only Mutex/Condvar/RwLock and `thread::sleep`
//! talk to the hooks directly.
//!
//! Control plane exception: the wrappers' own wait lists use RAW
//! `std::sync::Mutex` (never contended across a park — strictly interior,
//! acquired and released within one quantum) so the bookkeeping cannot
//! recurse into the scheduler. Ledgered under the PERMIT-S1-SYNC band.

use core::cell::UnsafeCell;
use core::fmt;
use core::mem::ManuallyDrop;
use core::panic::Location;
use core::time::Duration;

use crate::sim::hooks::{self, OpClass, SchedulerHooks, Vpid};

pub use std::sync::atomic; // NOT intercepted (design §1: one runnable thread ⇒ total order)
pub use std::sync::{LockResult, PoisonError, TryLockError, TryLockResult};

// ---------------------------------------------------------------------------
// wait-list plumbing
// ---------------------------------------------------------------------------

/// Wrapper-owned wait list. Raw std mutex by design (control plane; see
/// module doc). Vpid order is arrival order; picks index into it.
struct WaitList {
    q: std::sync::Mutex<Vec<Vpid>>,
}

impl WaitList {
    const fn new() -> Self {
        WaitList { q: std::sync::Mutex::new(Vec::new()) }
    }

    fn enqueue(&self, v: Vpid) {
        self.q.lock().unwrap_or_else(|e| e.into_inner()).push(v);
    }

    /// Remove `v` if still queued; true if it was (i.e. NOT consumed by a
    /// notify pick — the timeout/no-longer-waiting arm).
    fn cancel(&self, v: Vpid) -> bool {
        let mut g = self.q.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = g.iter().position(|x| *x == v) {
            g.remove(i);
            true
        } else {
            false
        }
    }

    /// Seeded pick-and-remove of one waiter.
    fn pick(&self, h: &dyn SchedulerHooks, site: &'static Location<'static>) -> Option<Vpid> {
        let mut g = self.q.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_empty() {
            None
        } else {
            let i = h.pick_waiter(site, g.len());
            Some(g.remove(i))
        }
    }

    /// Drain every waiter (notify_all / disconnects), arrival order.
    fn drain(&self) -> Vec<Vpid> {
        core::mem::take(&mut *self.q.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

// ---------------------------------------------------------------------------
// Mutex
// ---------------------------------------------------------------------------

/// Permit-hooked mutex: contended acquire ⇒ scheduler park; the next holder
/// after an unlock is a seeded pick (barging by the running thread remains
/// possible, exactly as with std/OS mutexes — the schedule decides).
pub struct Mutex<T: ?Sized> {
    waiters: WaitList,
    inner: std::sync::Mutex<T>,
}

impl<T> Mutex<T> {
    pub const fn new(t: T) -> Mutex<T> {
        Mutex { waiters: WaitList::new(), inner: std::sync::Mutex::new(t) }
    }

    pub fn into_inner(self) -> LockResult<T> {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> Mutex<T> {
    #[track_caller]
    pub fn lock(&self) -> LockResult<MutexGuard<'_, T>> {
        self.lock_at(Location::caller())
    }

    /// Internal: lock attributed to an explicit site (Condvar reacquire path
    /// keeps the caller's wait site).
    fn lock_at(&self, site: &'static Location<'static>) -> LockResult<MutexGuard<'_, T>> {
        match hooks::installed() {
            None => wrap_lock(self, self.inner.lock(), site),
            Some(h) => loop {
                match self.inner.try_lock() {
                    Ok(g) => return Ok(MutexGuard::new(self, g, site)),
                    Err(TryLockError::Poisoned(p)) => {
                        return Err(PoisonError::new(MutexGuard::new(self, p.into_inner(), site)))
                    }
                    Err(TryLockError::WouldBlock) => {
                        let v = h.current_vpid();
                        self.waiters.enqueue(v);
                        h.block_on(site, OpClass::MutexLock);
                        // Whether woken by the holder's pick or spuriously
                        // granted: leave the queue and retry.
                        self.waiters.cancel(v);
                    }
                }
            },
        }
    }

    #[track_caller]
    pub fn try_lock(&self) -> TryLockResult<MutexGuard<'_, T>> {
        let site = Location::caller();
        match self.inner.try_lock() {
            Ok(g) => Ok(MutexGuard::new(self, g, site)),
            Err(TryLockError::Poisoned(p)) => Err(TryLockError::Poisoned(PoisonError::new(
                MutexGuard::new(self, p.into_inner(), site),
            ))),
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }

    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        self.inner.get_mut()
    }
}

fn wrap_lock<'a, T: ?Sized>(
    lock: &'a Mutex<T>,
    r: LockResult<std::sync::MutexGuard<'a, T>>,
    site: &'static Location<'static>,
) -> LockResult<MutexGuard<'a, T>> {
    match r {
        Ok(g) => Ok(MutexGuard::new(lock, g, site)),
        Err(p) => Err(PoisonError::new(MutexGuard::new(lock, p.into_inner(), site))),
    }
}

impl<T: Default> Default for Mutex<T> {
    fn default() -> Self {
        Mutex::new(T::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Mutex<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pub struct MutexGuard<'a, T: ?Sized> {
    lock: &'a Mutex<T>,
    site: &'static Location<'static>,
    inner: ManuallyDrop<std::sync::MutexGuard<'a, T>>,
}

impl<'a, T: ?Sized> MutexGuard<'a, T> {
    fn new(
        lock: &'a Mutex<T>,
        inner: std::sync::MutexGuard<'a, T>,
        site: &'static Location<'static>,
    ) -> Self {
        MutexGuard { lock, site, inner: ManuallyDrop::new(inner) }
    }
}

impl<T: ?Sized> core::ops::Deref for MutexGuard<'_, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: ?Sized> core::ops::DerefMut for MutexGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: ?Sized> Drop for MutexGuard<'_, T> {
    fn drop(&mut self) {
        // Release the OS lock FIRST, then hand the seeded pick its wake.
        unsafe { ManuallyDrop::drop(&mut self.inner) };
        if let Some(h) = hooks::installed() {
            if let Some(v) = self.lock.waiters.pick(h, self.site) {
                h.wake(v, self.site);
            }
            h.touch(self.site, OpClass::MutexUnlock);
        }
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

/// std parity (S1 papering, permit-s3): `std::sync::MutexGuard` forwards
/// `Display`; call sites (e.g. xlogrecovery targets.rs restore-point
/// formatting) rely on it.
impl<T: ?Sized + fmt::Display> fmt::Display for MutexGuard<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        (**self).fmt(f)
    }
}

// ---------------------------------------------------------------------------
// Condvar
// ---------------------------------------------------------------------------

/// `WaitTimeoutResult` mirror (std's has no public constructor).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct WaitTimeoutResult(bool);

impl WaitTimeoutResult {
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

/// Permit-hooked condvar: wait ⇒ park; the `notify_one` wakee is a seeded
/// pick; `notify_all` makes every waiter runnable. Spurious wakeups are
/// permitted exactly as by std (callers loop on their predicate).
pub struct Condvar {
    waiters: WaitList,
    inner: std::sync::Condvar,
}

impl Condvar {
    pub const fn new() -> Condvar {
        Condvar { waiters: WaitList::new(), inner: std::sync::Condvar::new() }
    }

    #[track_caller]
    pub fn wait<'a, T>(&self, guard: MutexGuard<'a, T>) -> LockResult<MutexGuard<'a, T>> {
        let site = Location::caller();
        match hooks::installed() {
            None => {
                // std path: destructure without running unlock bookkeeping
                // (the std condvar owns the atomic unlock-and-wait).
                let (lock, inner) = into_parts(guard);
                match self.inner.wait(inner) {
                    Ok(g) => Ok(MutexGuard::new(lock, g, site)),
                    Err(p) => Err(PoisonError::new(MutexGuard::new(lock, p.into_inner(), site))),
                }
            }
            Some(h) => {
                let v = h.current_vpid();
                // Enqueue BEFORE releasing the mutex: still holding the
                // permit, so no notifier can run in the gap.
                self.waiters.enqueue(v);
                let lock = guard.lock;
                // The guard drop embeds unlock hooks (pick/wake/touch — a
                // preemption point): a notifier may run, and consume this
                // waiter, BEFORE the first park; its wake would then target
                // a runnable thread and be dropped. Check-before-park.
                drop(guard); // sim unlock: seeded next-holder wake + touch
                loop {
                    // Consumed by a notify pick (no longer queued) => woken,
                    // possibly before we ever parked. Still queued =>
                    // (re-)park. No hook sits between this check and
                    // `block_on`, and the permit is held across the gap, so
                    // no notify can land in it.
                    if !still_queued(&self.waiters, v) {
                        break;
                    }
                    h.block_on(site, OpClass::CondWait);
                }
                lock.lock_at(site)
            }
        }
    }

    #[track_caller]
    pub fn wait_timeout<'a, T>(
        &self,
        guard: MutexGuard<'a, T>,
        dur: Duration,
    ) -> LockResult<(MutexGuard<'a, T>, WaitTimeoutResult)> {
        let site = Location::caller();
        match hooks::installed() {
            None => {
                let (lock, inner) = into_parts(guard);
                match self.inner.wait_timeout(inner, dur) {
                    Ok((g, r)) => {
                        Ok((MutexGuard::new(lock, g, site), WaitTimeoutResult(r.timed_out())))
                    }
                    Err(p) => {
                        let (g, r) = p.into_inner();
                        Err(PoisonError::new((
                            MutexGuard::new(lock, g, site),
                            WaitTimeoutResult(r.timed_out()),
                        )))
                    }
                }
            }
            Some(h) => {
                let v = h.current_vpid();
                self.waiters.enqueue(v);
                let lock = guard.lock;
                // Same check-before-park protocol as `wait` (the guard drop
                // is a preemption point; see there).
                drop(guard);
                let deadline = h.now_ns().saturating_add(dur.as_nanos() as u64);
                let timed_out = loop {
                    let now = h.now_ns();
                    if now >= deadline {
                        // Deadline reached before (re-)parking: leave the
                        // queue. cancel = false means a notify already
                        // consumed us — the queue is the truth, not the
                        // clock: report notified.
                        break self.waiters.cancel(v);
                    }
                    if !still_queued(&self.waiters, v) {
                        break false; // notify pick consumed us (maybe pre-park)
                    }
                    // No hook sits between the check above and this park
                    // (permit held across the gap): no notify can land in it.
                    let expired = h.timed_park(site, Duration::from_nanos(deadline - now));
                    if !still_queued(&self.waiters, v) {
                        break false; // notified (even if the clock also expired)
                    }
                    if expired {
                        self.waiters.cancel(v);
                        break true;
                    }
                    // Spurious grant before the deadline: re-check, re-park.
                };
                match lock.lock_at(site) {
                    Ok(g) => Ok((g, WaitTimeoutResult(timed_out))),
                    Err(p) => {
                        Err(PoisonError::new((p.into_inner(), WaitTimeoutResult(timed_out))))
                    }
                }
            }
        }
    }

    #[track_caller]
    pub fn notify_one(&self) {
        match hooks::installed() {
            None => self.inner.notify_one(),
            Some(h) => {
                let site = Location::caller();
                if let Some(v) = self.waiters.pick(h, site) {
                    h.wake(v, site);
                }
                h.touch(site, OpClass::CondNotify);
            }
        }
    }

    #[track_caller]
    pub fn notify_all(&self) {
        match hooks::installed() {
            None => self.inner.notify_all(),
            Some(h) => {
                let site = Location::caller();
                for v in self.waiters.drain() {
                    h.wake(v, site);
                }
                h.touch(site, OpClass::CondNotify);
            }
        }
    }
}

impl Default for Condvar {
    fn default() -> Self {
        Condvar::new()
    }
}

impl fmt::Debug for Condvar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Condvar { .. }")
    }
}

fn still_queued(w: &WaitList, v: Vpid) -> bool {
    w.q.lock().unwrap_or_else(|e| e.into_inner()).contains(&v)
}

/// Take the raw std guard out of a sim guard WITHOUT running the sim unlock
/// bookkeeping (std-fallback condvar path only).
fn into_parts<'a, T: ?Sized>(g: MutexGuard<'a, T>) -> (&'a Mutex<T>, std::sync::MutexGuard<'a, T>) {
    let mut g = ManuallyDrop::new(g);
    let lock = g.lock;
    // SAFETY: g is ManuallyDrop — the sim Drop impl will not run, and inner
    // is never touched again through g.
    let inner = unsafe { ManuallyDrop::take(&mut g.inner) };
    (lock, inner)
}

// ---------------------------------------------------------------------------
// RwLock
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, PartialEq, Eq)]
enum RwKind {
    Read,
    Write,
}

/// Permit-hooked rwlock. Wait list carries reader/writer kind; a release
/// picks ONE waiter (seeded) — a picked reader also releases every other
/// queued reader (std would admit them concurrently), a picked writer wakes
/// alone. Fairness is whatever the seeded schedule makes of it, as with the
/// OS lock.
pub struct RwLock<T: ?Sized> {
    waiters: std::sync::Mutex<Vec<(Vpid, RwKind)>>,
    inner: std::sync::RwLock<T>,
}

impl<T> RwLock<T> {
    pub const fn new(t: T) -> RwLock<T> {
        RwLock { waiters: std::sync::Mutex::new(Vec::new()), inner: std::sync::RwLock::new(t) }
    }

    pub fn into_inner(self) -> LockResult<T> {
        self.inner.into_inner()
    }
}

impl<T: ?Sized> RwLock<T> {
    #[track_caller]
    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        let site = Location::caller();
        match hooks::installed() {
            None => match self.inner.read() {
                Ok(g) => Ok(RwLockReadGuard::new(self, g, site)),
                Err(p) => {
                    Err(PoisonError::new(RwLockReadGuard::new(self, p.into_inner(), site)))
                }
            },
            Some(h) => loop {
                match self.inner.try_read() {
                    Ok(g) => return Ok(RwLockReadGuard::new(self, g, site)),
                    Err(TryLockError::Poisoned(p)) => {
                        return Err(PoisonError::new(RwLockReadGuard::new(
                            self,
                            p.into_inner(),
                            site,
                        )))
                    }
                    Err(TryLockError::WouldBlock) => {
                        let v = h.current_vpid();
                        self.enqueue(v, RwKind::Read);
                        h.block_on(site, OpClass::RwRead);
                        self.cancel(v);
                    }
                }
            },
        }
    }

    #[track_caller]
    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        let site = Location::caller();
        match hooks::installed() {
            None => match self.inner.write() {
                Ok(g) => Ok(RwLockWriteGuard::new(self, g, site)),
                Err(p) => {
                    Err(PoisonError::new(RwLockWriteGuard::new(self, p.into_inner(), site)))
                }
            },
            Some(h) => loop {
                match self.inner.try_write() {
                    Ok(g) => return Ok(RwLockWriteGuard::new(self, g, site)),
                    Err(TryLockError::Poisoned(p)) => {
                        return Err(PoisonError::new(RwLockWriteGuard::new(
                            self,
                            p.into_inner(),
                            site,
                        )))
                    }
                    Err(TryLockError::WouldBlock) => {
                        let v = h.current_vpid();
                        self.enqueue(v, RwKind::Write);
                        h.block_on(site, OpClass::RwWrite);
                        self.cancel(v);
                    }
                }
            },
        }
    }

    #[track_caller]
    pub fn try_read(&self) -> TryLockResult<RwLockReadGuard<'_, T>> {
        let site = Location::caller();
        match self.inner.try_read() {
            Ok(g) => Ok(RwLockReadGuard::new(self, g, site)),
            Err(TryLockError::Poisoned(p)) => Err(TryLockError::Poisoned(PoisonError::new(
                RwLockReadGuard::new(self, p.into_inner(), site),
            ))),
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }

    #[track_caller]
    pub fn try_write(&self) -> TryLockResult<RwLockWriteGuard<'_, T>> {
        let site = Location::caller();
        match self.inner.try_write() {
            Ok(g) => Ok(RwLockWriteGuard::new(self, g, site)),
            Err(TryLockError::Poisoned(p)) => Err(TryLockError::Poisoned(PoisonError::new(
                RwLockWriteGuard::new(self, p.into_inner(), site),
            ))),
            Err(TryLockError::WouldBlock) => Err(TryLockError::WouldBlock),
        }
    }

    pub fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned()
    }

    pub fn get_mut(&mut self) -> LockResult<&mut T> {
        self.inner.get_mut()
    }

    fn enqueue(&self, v: Vpid, k: RwKind) {
        self.waiters.lock().unwrap_or_else(|e| e.into_inner()).push((v, k));
    }

    fn cancel(&self, v: Vpid) {
        let mut g = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = g.iter().position(|(x, _)| *x == v) {
            g.remove(i);
        }
    }

    /// Release-side wake: seeded pick of one waiter; a picked reader brings
    /// every other queued reader with it.
    fn wake_on_release(&self, h: &dyn SchedulerHooks, site: &'static Location<'static>) {
        let mut g = self.waiters.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_empty() {
            drop(g);
        } else {
            let i = h.pick_waiter(site, g.len());
            let (v, kind) = g.remove(i);
            let mut wake = vec![v];
            if kind == RwKind::Read {
                let mut j = 0;
                while j < g.len() {
                    if g[j].1 == RwKind::Read {
                        wake.push(g.remove(j).0);
                    } else {
                        j += 1;
                    }
                }
            }
            drop(g);
            for v in wake {
                h.wake(v, site);
            }
        }
        h.touch(site, OpClass::RwUnlock);
    }
}

impl<T: Default> Default for RwLock<T> {
    fn default() -> Self {
        RwLock::new(T::default())
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for RwLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

macro_rules! rw_guard {
    ($name:ident, $std:ident, deref_mut: $($mutable:tt)?) => {
        pub struct $name<'a, T: ?Sized> {
            lock: &'a RwLock<T>,
            site: &'static Location<'static>,
            inner: ManuallyDrop<std::sync::$std<'a, T>>,
        }

        impl<'a, T: ?Sized> $name<'a, T> {
            fn new(
                lock: &'a RwLock<T>,
                inner: std::sync::$std<'a, T>,
                site: &'static Location<'static>,
            ) -> Self {
                $name { lock, site, inner: ManuallyDrop::new(inner) }
            }
        }

        impl<T: ?Sized> core::ops::Deref for $name<'_, T> {
            type Target = T;
            #[inline]
            fn deref(&self) -> &T {
                &self.inner
            }
        }

        $(
            impl<T: ?Sized> core::ops::DerefMut for $name<'_, T> {
                #[inline]
                fn deref_mut(&mut self) -> &mut T {
                    &$mutable self.inner
                }
            }
        )?

        impl<T: ?Sized> Drop for $name<'_, T> {
            fn drop(&mut self) {
                unsafe { ManuallyDrop::drop(&mut self.inner) };
                if let Some(h) = hooks::installed() {
                    self.lock.wake_on_release(h, self.site);
                }
            }
        }

        impl<T: ?Sized + fmt::Debug> fmt::Debug for $name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                (**self).fmt(f)
            }
        }

        /// std parity (S1 papering, permit-s3): std's rwlock guards forward
        /// `Display`.
        impl<T: ?Sized + fmt::Display> fmt::Display for $name<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                (**self).fmt(f)
            }
        }
    };
}

rw_guard!(RwLockReadGuard, RwLockReadGuard, deref_mut:);
rw_guard!(RwLockWriteGuard, RwLockWriteGuard, deref_mut: mut);

// ---------------------------------------------------------------------------
// Barrier (composite over the wrapped Mutex/Condvar — hook-routed for free)
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug)]
pub struct BarrierWaitResult(bool);

impl BarrierWaitResult {
    pub fn is_leader(&self) -> bool {
        self.0
    }
}

/// Mutex+Condvar barrier (design §3 row 18: one composite serves all
/// worlds' semantics; under sim its waits are seeded parks).
pub struct Barrier {
    state: Mutex<(usize, u64)>, // (arrived, generation)
    cv: Condvar,
    n: usize,
}

impl Barrier {
    pub const fn new(n: usize) -> Barrier {
        Barrier { state: Mutex::new((0, 0)), cv: Condvar::new(), n }
    }

    #[track_caller]
    pub fn wait(&self) -> BarrierWaitResult {
        if self.n <= 1 {
            return BarrierWaitResult(true);
        }
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        g.0 += 1;
        if g.0 == self.n {
            g.0 = 0;
            g.1 = g.1.wrapping_add(1);
            drop(g);
            self.cv.notify_all();
            BarrierWaitResult(true)
        } else {
            let gen = g.1;
            while g.1 == gen {
                g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
            }
            BarrierWaitResult(false)
        }
    }
}

impl fmt::Debug for Barrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Barrier { .. }")
    }
}

// ---------------------------------------------------------------------------
// OnceLock / Once (composite: the loser parks for a choke-crossing winner —
// design §3 row 19)
// ---------------------------------------------------------------------------

const ONCE_EMPTY: u8 = 0;
const ONCE_RUNNING: u8 = 1;
const ONCE_DONE: u8 = 2;

/// OnceLock composite: state machine over the wrapped Mutex/Condvar, so an
/// init loser is a hooked, seeded park instead of an invisible futex wait.
/// std parity: a panicking init closure resets the state (another caller may
/// retry); `set` during a competing init blocks until it resolves.
pub struct OnceLock<T> {
    state: Mutex<u8>,
    cv: Condvar,
    value: UnsafeCell<Option<T>>,
}

// SAFETY: value is written exactly once, before state becomes DONE, under
// the state mutex; readers only take shared refs after observing DONE.
unsafe impl<T: Send + Sync> Sync for OnceLock<T> {}
unsafe impl<T: Send> Send for OnceLock<T> {}

impl<T> OnceLock<T> {
    pub const fn new() -> OnceLock<T> {
        OnceLock { state: Mutex::new(ONCE_EMPTY), cv: Condvar::new(), value: UnsafeCell::new(None) }
    }

    pub fn get(&self) -> Option<&T> {
        let g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if *g == ONCE_DONE {
            // SAFETY: DONE ⇒ initialized and never written again.
            Some(unsafe { (*self.value.get()).as_ref().unwrap_unchecked() })
        } else {
            None
        }
    }

    #[track_caller]
    pub fn set(&self, value: T) -> Result<(), T> {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match *g {
                ONCE_DONE => return Err(value),
                ONCE_RUNNING => g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner()),
                _ => {
                    unsafe { *self.value.get() = Some(value) };
                    *g = ONCE_DONE;
                    drop(g);
                    self.cv.notify_all();
                    return Ok(());
                }
            }
        }
    }

    #[track_caller]
    pub fn get_or_init<F: FnOnce() -> T>(&self, f: F) -> &T {
        let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            match *g {
                ONCE_DONE => {
                    // SAFETY: as in get().
                    return unsafe { (*self.value.get()).as_ref().unwrap_unchecked() };
                }
                ONCE_RUNNING => {
                    // The loser parks (hooked via the wrapped Condvar).
                    g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
                }
                _ => {
                    *g = ONCE_RUNNING;
                    drop(g);
                    // std parity: reset on unwind so another caller may retry.
                    let reset = ResetOnPanic(self);
                    let v = f();
                    core::mem::forget(reset);
                    let mut g = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    unsafe { *self.value.get() = Some(v) };
                    *g = ONCE_DONE;
                    drop(g);
                    self.cv.notify_all();
                    return unsafe { (*self.value.get()).as_ref().unwrap_unchecked() };
                }
            }
        }
    }
}

impl<T> Default for OnceLock<T> {
    fn default() -> Self {
        OnceLock::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for OnceLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OnceLock").field(&self.get()).finish()
    }
}

struct ResetOnPanic<'a, T>(&'a OnceLock<T>);

impl<T> Drop for ResetOnPanic<'_, T> {
    fn drop(&mut self) {
        let mut g = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        *g = ONCE_EMPTY;
        drop(g);
        self.0.cv.notify_all();
    }
}

/// Once over the OnceLock composite (same loser-park semantics).
pub struct Once {
    l: OnceLock<()>,
}

impl Once {
    pub const fn new() -> Once {
        Once { l: OnceLock::new() }
    }

    #[track_caller]
    pub fn call_once<F: FnOnce()>(&self, f: F) {
        self.l.get_or_init(|| f());
    }

    pub fn is_completed(&self) -> bool {
        self.l.get().is_some()
    }
}

impl Default for Once {
    fn default() -> Self {
        Once::new()
    }
}

impl fmt::Debug for Once {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Once { .. }")
    }
}

// ---------------------------------------------------------------------------
// mpsc (composite over the wrapped Mutex/Condvar — hook-routed for free)
// ---------------------------------------------------------------------------

/// std-shaped mpsc over the wrapped primitives: empty-recv / full-send park
/// through the hooks; the winning sender on a freed bounded slot is a seeded
/// pick (via the Condvar wakee pick). Known step-1 fidelity gaps (worklog
/// §deviations): `sync_channel(0)` behaves as capacity 1 (no rendezvous
/// handshake yet); `recv_timeout`'s clock restarts on spurious grants.
pub mod mpsc {
    use super::{Condvar, Mutex};
    use std::collections::VecDeque;
    use std::sync::Arc;

    pub use std::sync::mpsc::{
        RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError,
    };

    struct State<T> {
        buf: VecDeque<T>,
        cap: Option<usize>,
        senders: usize,
        rx_alive: bool,
    }

    struct Shared<T> {
        st: Mutex<State<T>>,
        not_empty: Condvar,
        not_full: Condvar,
    }

    pub struct Sender<T>(Arc<Shared<T>>);
    pub struct SyncSender<T>(Arc<Shared<T>>);
    pub struct Receiver<T>(Arc<Shared<T>>);

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let sh = Arc::new(Shared {
            st: Mutex::new(State { buf: VecDeque::new(), cap: None, senders: 1, rx_alive: true }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        });
        (Sender(sh.clone()), Receiver(sh))
    }

    pub fn sync_channel<T>(bound: usize) -> (SyncSender<T>, Receiver<T>) {
        let sh = Arc::new(Shared {
            st: Mutex::new(State {
                buf: VecDeque::new(),
                cap: Some(bound.max(1)), // step-1: rendezvous(0) ≈ capacity 1
                senders: 1,
                rx_alive: true,
            }),
            not_empty: Condvar::new(),
            not_full: Condvar::new(),
        });
        (SyncSender(sh.clone()), Receiver(sh))
    }

    impl<T> Sender<T> {
        #[track_caller]
        pub fn send(&self, t: T) -> Result<(), SendError<T>> {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            if !g.rx_alive {
                return Err(SendError(t));
            }
            g.buf.push_back(t);
            drop(g);
            self.0.not_empty.notify_one();
            Ok(())
        }
    }

    impl<T> SyncSender<T> {
        #[track_caller]
        pub fn send(&self, t: T) -> Result<(), SendError<T>> {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if !g.rx_alive {
                    return Err(SendError(t));
                }
                let cap = g.cap.expect("sync_channel state");
                if g.buf.len() < cap {
                    g.buf.push_back(t);
                    drop(g);
                    self.0.not_empty.notify_one();
                    return Ok(());
                }
                g = self.0.not_full.wait(g).unwrap_or_else(|e| e.into_inner());
            }
        }

        #[track_caller]
        pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            if !g.rx_alive {
                return Err(TrySendError::Disconnected(t));
            }
            let cap = g.cap.expect("sync_channel state");
            if g.buf.len() < cap {
                g.buf.push_back(t);
                drop(g);
                self.0.not_empty.notify_one();
                Ok(())
            } else {
                Err(TrySendError::Full(t))
            }
        }
    }

    impl<T> Receiver<T> {
        #[track_caller]
        pub fn recv(&self) -> Result<T, RecvError> {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(v) = g.buf.pop_front() {
                    drop(g);
                    self.0.not_full.notify_one();
                    return Ok(v);
                }
                if g.senders == 0 {
                    return Err(RecvError);
                }
                g = self.0.not_empty.wait(g).unwrap_or_else(|e| e.into_inner());
            }
        }

        pub fn try_recv(&self) -> Result<T, TryRecvError> {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(v) = g.buf.pop_front() {
                drop(g);
                self.0.not_full.notify_one();
                Ok(v)
            } else if g.senders == 0 {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        }

        #[track_caller]
        pub fn recv_timeout(&self, dur: core::time::Duration) -> Result<T, RecvTimeoutError> {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(v) = g.buf.pop_front() {
                    drop(g);
                    self.0.not_full.notify_one();
                    return Ok(v);
                }
                if g.senders == 0 {
                    return Err(RecvTimeoutError::Disconnected);
                }
                let (g2, r) =
                    self.0.not_empty.wait_timeout(g, dur).unwrap_or_else(|e| e.into_inner());
                g = g2;
                if r.timed_out() {
                    return match g.buf.pop_front() {
                        Some(v) => {
                            drop(g);
                            self.0.not_full.notify_one();
                            Ok(v)
                        }
                        None if g.senders == 0 => Err(RecvTimeoutError::Disconnected),
                        None => Err(RecvTimeoutError::Timeout),
                    };
                }
            }
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.0.st.lock().unwrap_or_else(|e| e.into_inner()).senders += 1;
            Sender(self.0.clone())
        }
    }

    impl<T> Clone for SyncSender<T> {
        fn clone(&self) -> Self {
            self.0.st.lock().unwrap_or_else(|e| e.into_inner()).senders += 1;
            SyncSender(self.0.clone())
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            sender_gone(&self.0);
        }
    }

    impl<T> Drop for SyncSender<T> {
        fn drop(&mut self) {
            sender_gone(&self.0);
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            let mut g = self.0.st.lock().unwrap_or_else(|e| e.into_inner());
            g.rx_alive = false;
            drop(g);
            // Every blocked bounded sender must observe the disconnect.
            self.0.not_full.notify_all();
        }
    }

    fn sender_gone<T>(sh: &Shared<T>) {
        let mut g = sh.st.lock().unwrap_or_else(|e| e.into_inner());
        g.senders -= 1;
        let last = g.senders == 0;
        drop(g);
        if last {
            sh.not_empty.notify_all(); // blocked receiver observes disconnect
        }
    }
}

// ---------------------------------------------------------------------------
// thread
// ---------------------------------------------------------------------------

/// Sim thread surface. `sleep` ⇒ SimClock TimedPark; `spawn`/`Builder::spawn`
/// register the child through the hooks (synthetic vpid range for spawns
/// outside the launch_backend door — the door passes real vpids by calling
/// hooks itself, WS-CORE); `join` parks until the wakee deregisters (the
/// scheduler wakes joiners on `exit`).
///
/// Step-1 passthroughs (documented gaps, the watchdog names them if a corpus
/// reaches one): `park`/`park_timeout` (unpark-token protocol is a step-2
/// shim), `scope` (raw scoped gangs stay lint-fenced by the `scope` arm).
pub mod thread {
    use super::hooks::{self, OpClass, Vpid};
    use core::panic::Location;
    use core::time::Duration;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    pub use std::thread::{
        available_parallelism, current, panicking, park, park_timeout, scope, yield_now,
        AccessError, Result, Scope, ScopedJoinHandle, Thread, ThreadId,
    };

    /// Synthetic vpid allocator for spawns outside the spawn door
    /// (`0x8000_0000..`; the reserve_child_pid range never reaches this).
    fn next_synthetic_vpid() -> Vpid {
        use std::sync::atomic::AtomicU32;
        static NEXT: AtomicU32 = AtomicU32::new(0x8000_0000);
        NEXT.fetch_add(1, Ordering::Relaxed)
    }

    #[track_caller]
    pub fn sleep(dur: Duration) {
        match hooks::installed() {
            None => std::thread::sleep(dur),
            Some(h) => {
                let _ = h.timed_park(Location::caller(), dur);
            }
        }
    }

    pub struct JoinHandle<T> {
        inner: std::thread::JoinHandle<T>,
        done: Arc<AtomicBool>,
    }

    impl<T> JoinHandle<T> {
        #[track_caller]
        pub fn join(self) -> Result<T> {
            if let Some(h) = hooks::installed() {
                let site = Location::caller();
                while !self.done.load(Ordering::SeqCst) {
                    h.block_on(site, OpClass::Join);
                }
            }
            // Finished (exit hook already ran): the residual OS join is
            // bounded teardown, never a schedule-relevant wait.
            self.inner.join()
        }

        pub fn is_finished(&self) -> bool {
            self.inner.is_finished()
        }

        pub fn thread(&self) -> &Thread {
            self.inner.thread()
        }
    }

    pub struct Builder {
        inner: std::thread::Builder,
    }

    impl Builder {
        pub fn new() -> Builder {
            Builder { inner: std::thread::Builder::new() }
        }

        pub fn name(self, name: String) -> Builder {
            Builder { inner: self.inner.name(name) }
        }

        pub fn stack_size(self, size: usize) -> Builder {
            Builder { inner: self.inner.stack_size(size) }
        }

        #[track_caller]
        pub fn spawn<F, T>(self, f: F) -> std::io::Result<JoinHandle<T>>
        where
            F: FnOnce() -> T + Send + 'static,
            T: Send + 'static,
        {
            let site = Location::caller();
            let done = Arc::new(AtomicBool::new(false));
            let done2 = Arc::clone(&done);
            let inner = self.inner.spawn(move || {
                let vpid = match hooks::installed() {
                    Some(h) => {
                        let v = next_synthetic_vpid();
                        h.spawn(v, site); // child registers (and is gated) first
                        Some(v)
                    }
                    None => None,
                };
                let r = f();
                done2.store(true, Ordering::SeqCst);
                if let (Some(h), Some(v)) = (hooks::installed(), vpid) {
                    // Deregister LAST (P3 §1.6 rule 3: joiners wake on this,
                    // post-dating shared teardown run by the door epilogue).
                    h.exit(v);
                }
                r
            })?;
            if let Some(h) = hooks::installed() {
                h.touch(site, OpClass::Spawn);
            }
            Ok(JoinHandle { inner, done })
        }
    }

    impl Default for Builder {
        fn default() -> Self {
            Builder::new()
        }
    }

    #[track_caller]
    pub fn spawn<F, T>(f: F) -> JoinHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        Builder::new().spawn(f).expect("failed to spawn thread")
    }
}
