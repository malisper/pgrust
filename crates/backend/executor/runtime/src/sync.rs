//! Loom/std synchronization shim — the repo's first loom pattern.
//!
//! Rules of the pattern (set here deliberately; copy for future loom-modeled
//! crates):
//! - Atomics, `Mutex`, `Condvar`, and `thread` come from this module. Under
//!   `--cfg loom` they are loom's checked types; otherwise std's.
//! - `Arc` stays `std::sync::Arc` everywhere: loom's `Arc` does not coerce to
//!   `Arc<dyn Trait>` and we only use `Arc` for ownership of immutable or
//!   internally-loom-synchronized data, so loom loses nothing it needs.
//! - No statics hold loom types (loom atomics are not `const`-constructible);
//!   all scheduler state is instance-owned, which is what we want anyway.

#[cfg(loom)]
pub(crate) use loom::sync::atomic;
#[cfg(loom)]
pub(crate) use loom::sync::{Condvar, Mutex, MutexGuard};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic;
#[cfg(not(loom))]
pub(crate) use std::sync::{Condvar, Mutex, MutexGuard};

/// Poison-tolerant lock (matches the repo's `unwrap_or_else(e.into_inner)`
/// discipline; loom's Mutex never poisons in models but keeps the API).
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Counting semaphore (Mutex+Condvar; std has no stable Semaphore).
///
/// M0 use: the execution-permit semaphore — exactly `cores` permits; any
/// task-executing thread holds one. The pool runs `cores + K` threads
/// (K standbys, redesign §2.8): permits cap RUNNING tasks, standbys exist to
/// absorb permit releases from declared blocking sections (see [`IoGuard`]).
/// Leaders never execute (§2.5 decided design).
pub struct Semaphore {
    permits: Mutex<usize>,
    cv: Condvar,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        Semaphore { permits: Mutex::new(permits), cv: Condvar::new() }
    }

    pub fn acquire(&self) {
        let mut g = lock(&self.permits);
        while *g == 0 {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
        *g -= 1;
    }

    pub fn try_acquire(&self) -> bool {
        let mut g = lock(&self.permits);
        if *g == 0 {
            return false;
        }
        *g -= 1;
        true
    }

    pub fn release(&self) {
        let mut g = lock(&self.permits);
        *g += 1;
        drop(g);
        self.cv.notify_one();
    }

    pub fn available(&self) -> usize {
        *lock(&self.permits)
    }

    /// Enter a DECLARED BLOCKING SECTION (redesign §2.8: uring buffered-read
    /// waits, spill tape I/O, lock/Waiter waits — M1+ callers; reserved and
    /// loom-modeled in M0): the holder's permit is released on entry so a
    /// standby thread can absorb the freed core, and reacquired on guard
    /// drop. No priority on reacquisition — the releasing worker rejoins as
    /// an ordinary permit contender (condvar wake order; no inversion by
    /// construction). Capacity moves; the task does NOT — it stays on its
    /// thread with its pin-board entry and finalization-marker obligations
    /// intact, which is exactly what the standby-absorption loom
    /// interleavings verify against the last-worker-out counter.
    ///
    /// Caller contract: must actually hold a permit.
    pub fn io_section(&self) -> IoGuard<'_> {
        self.release();
        IoGuard { sem: self }
    }
}

/// RAII permit release for a declared blocking section. See
/// [`Semaphore::io_section`].
pub struct IoGuard<'a> {
    sem: &'a Semaphore,
}

impl Drop for IoGuard<'_> {
    fn drop(&mut self) {
        self.sem.acquire();
    }
}

/// Eventcount-style park/wake for idle workers (DuckDB parks workers on a
/// semaphore-style wait; ours is an epoch eventcount so a wake between the
/// failed pick and the park can never be lost).
///
/// Protocol: capture `epoch()` BEFORE looking for work; if no work is found,
/// `park(seen)` blocks only while the epoch is unchanged. Every publish of
/// new work bumps the epoch and notifies.
pub struct ParkLot {
    epoch: Mutex<u64>,
    cv: Condvar,
}

impl ParkLot {
    pub fn new() -> Self {
        ParkLot { epoch: Mutex::new(0), cv: Condvar::new() }
    }

    pub fn epoch(&self) -> u64 {
        *lock(&self.epoch)
    }

    pub fn park(&self, seen: u64) {
        let mut g = lock(&self.epoch);
        while *g == seen {
            g = self.cv.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    pub fn wake_all(&self) {
        let mut g = lock(&self.epoch);
        *g = g.wrapping_add(1);
        drop(g);
        self.cv.notify_all();
    }
}

impl Default for ParkLot {
    fn default() -> Self {
        Self::new()
    }
}
