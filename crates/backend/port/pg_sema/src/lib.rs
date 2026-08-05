//! One backend = one thread, so C's cross-process PGSemaphores reduce to
//! in-process counters keyed by ProcNumber, blocking on the owner's Waiter
//! (M0 lane C): PGSemaphoreLock is only ever called by the semaphore's own
//! proc (the LWLock block point parks the waiter itself; unlockers are
//! other threads posting the count and unparking the published handle).
//!
//! Dekker discipline mirroring the latch: the locker publishes its waker
//! handle, then re-checks the count (SeqCst edges both sides), so an unlock
//! either sees the handle or the locker sees the posted count — a lost wake
//! is impossible; the waiter recheck cadence stands behind it as the debug
//! backstop, as everywhere.
//!
//! Handle lifetime = one blocked lock (C parity: a returned sem_wait leaves
//! no registered waiter). The published handle is RETIRED (waiter word -> 0)
//! when the lock consumes its count and returns: waiter handles are
//! per-incarnation (wretain warm claims reissue the token; proc slots and
//! pool threads recycle), so a handle left published across an ownership
//! epoch is stale residue — the next owner's debug_assert reads it as a
//! phantom "concurrent waiter" (the 2026-07 dev-profile wedge-legs panic),
//! and unlocks route wakes to a structurally-dead handle. PGSemaphoreReset
//! (proc acquisition, C InitProcess parity) also clears the route, covering
//! abnormal exits. Loom models: waiter/tests/loom.rs `pg_sema_*` (protocol
//! mirror over the slot core, including the epoch-reuse shape).

#![allow(non_snake_case)]

use std::sync::atomic::{
    fence, AtomicI32, AtomicU64,
    Ordering::{Acquire, SeqCst},
};
use std::sync::RwLock;

use types_core::ProcNumber;

struct PgSemaphore {
    count: AtomicI32,
    /// Packed waiter handle of the blocked owner (0 = nobody parked).
    waiter: AtomicU64,
}

static SEMAS: RwLock<Vec<&'static PgSemaphore>> = RwLock::new(Vec::new());

fn sema(procno: ProcNumber) -> &'static PgSemaphore {
    SEMAS.read().unwrap().get(procno as usize).copied().unwrap_or_else(|| {
        panic!("pg_sema: semaphore for proc {procno} was never created")
    })
}

pub fn PGSemaphoreCreate(procno: ProcNumber) {
    let mut semas = SEMAS.write().unwrap();
    assert_eq!(
        semas.len(),
        procno as usize,
        "pg_sema: creates must arrive in ProcNumber order"
    );
    semas.push(Box::leak(Box::new(PgSemaphore {
        // sem_init(sem, 1, 1): initial value 1.
        count: AtomicI32::new(1),
        waiter: AtomicU64::new(0),
    })));
}

pub fn PGSemaphoreReset(procno: ProcNumber) {
    let s = sema(procno);
    s.count.store(0, SeqCst);
    // Fresh proc lifetime (InitProcess resets its PGPROC's semaphore): clear
    // the published wake route too. PGSemaphoreLock retires its handle on
    // return, so this is the backstop for ABNORMAL previous-owner exits
    // (anything that left the lock without returning) — without it, PGPROC
    // slot REUSE by a new worker thread would find the previous owner's
    // stale handle and trip the single-waiter invariant, observed live as
    // `pg_sema: concurrent waiters on one PGPROC semaphore` on back-to-back
    // parallel engagements (m2-agg-sink e2e + wedge legs 1/2/3/5, dev
    // asserts). The caller owns the proc slot exclusively here, so nobody
    // can be parked on it.
    s.waiter.store(0, SeqCst);
}

pub fn PGSemaphoreLock(procno: ProcNumber) {
    let s = sema(procno);
    let mut published = false;
    loop {
        // Fast path: consume a posted count.
        let mut c = s.count.load(Acquire);
        while c > 0 {
            match s.count.compare_exchange_weak(c, c - 1, SeqCst, Acquire) {
                Ok(_) => {
                    if published {
                        // Retire the wake route: handles are per-incarnation
                        // and the wait is over — left in place it leaks
                        // across proc-slot/thread reuse as stale residue the
                        // next owner's debug_assert reads as a phantom
                        // concurrent waiter. A racing unlock that already
                        // loaded the handle at worst latches a spurious
                        // notify our next park consumes (every park site is
                        // a predicate loop).
                        s.waiter.store(0, SeqCst);
                    }
                    return;
                }
                Err(actual) => c = actual,
            }
        }
        // Slow path: publish the wake route, re-check, park. Only the
        // owning proc ever locks its own semaphore (C parity: every
        // PGSemaphoreLock site passes MyProc's sema), so the single waiter
        // slot suffices; the debug_assert guards the invariant. With
        // retire-on-return above, `prev` can only be 0 or this call's own
        // handle (from a previous loop iteration) — anything else IS a
        // concurrent waiter.
        let cur = waiter::current_handle().as_u64();
        let prev = s.waiter.swap(cur, SeqCst);
        debug_assert!(
            prev == 0 || prev == cur,
            "pg_sema: concurrent waiters on one PGPROC semaphore \
             (procno={procno} prev=[{}] cur=[{}])",
            waiter::describe_word(prev),
            waiter::describe_word(cur),
        );
        published = true;
        fence(SeqCst);
        if s.count.load(SeqCst) > 0 {
            // Posted between the fast path and the publication: retry the
            // consume. The published handle stays; a racing unlock's unpark
            // at worst latches a notify our next park consumes.
            continue;
        }
        let _ = waiter::park();
        // Notified, or cadence recheck: loop re-tests the count.
    }
}

pub fn PGSemaphoreUnlock(procno: ProcNumber) {
    let s = sema(procno);
    s.count.fetch_add(1, SeqCst);
    fence(SeqCst);
    // Either the locker saw our count post, or its handle publication
    // preceded our fence and this unpark delivers.
    waiter::unpark_word(s.waiter.load(Acquire));
}

pub fn init_seams() {
    pg_sema_seams::pg_semaphore_create::set(PGSemaphoreCreate);
    pg_sema_seams::pg_semaphore_reset::set(PGSemaphoreReset);
    pg_sema_seams::pg_semaphore_lock::set(PGSemaphoreLock);
    pg_sema_seams::pg_semaphore_unlock::set(PGSemaphoreUnlock);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_reset_lock_unlock() {
        PGSemaphoreCreate(0);
        PGSemaphoreCreate(1);
        PGSemaphoreLock(0);
        PGSemaphoreUnlock(0);
        PGSemaphoreReset(1);
        PGSemaphoreUnlock(1);
        PGSemaphoreLock(1);
        let waiter = std::thread::spawn(|| PGSemaphoreLock(1));
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(!waiter.is_finished());
        PGSemaphoreUnlock(1);
        waiter.join().unwrap();

        // Posts before the lock are consumed without parking.
        PGSemaphoreCreate(2);
        PGSemaphoreReset(2);
        PGSemaphoreUnlock(2);
        PGSemaphoreUnlock(2);
        PGSemaphoreLock(2);
        PGSemaphoreLock(2);

        // Cross-epoch reuse (the 2026-07 dev wedge panic): a blocked owner
        // whose thread then DIES must leave no wake-route residue — the
        // next owner of the recycled proc slot is a different thread with a
        // different handle, and pre-fix its slow path read the stale handle
        // as a phantom "concurrent waiter" (debug_assert panic). No
        // PGSemaphoreReset between the epochs: this exercises the lock's
        // own retire-on-return, not the InitProcess backstop.
        PGSemaphoreCreate(3);
        PGSemaphoreReset(3);
        for _ in 0..2 {
            let waiter = std::thread::spawn(|| PGSemaphoreLock(3));
            std::thread::sleep(std::time::Duration::from_millis(20));
            PGSemaphoreUnlock(3);
            waiter.join().unwrap();
        }
    }
}
