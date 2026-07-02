//! PGSemaphore (port/posix_sema.c unnamed-sema arm): one backend = one
//! thread, so C's cross-process semaphores reduce to in-process
//! Mutex+Condvar counters keyed by ProcNumber.

#![allow(non_snake_case)]

use std::sync::{Condvar, Mutex, RwLock};

use types_core::ProcNumber;

struct PgSemaphore {
    count: Mutex<i32>,
    cv: Condvar,
}

// Created sequentially by InitProcGlobal at boot; lock/unlock only ever see
// an existing entry, so the read lock is uncontended after boot.
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
    // sem_init(sem, 1, 1): initial value 1.
    semas.push(Box::leak(Box::new(PgSemaphore {
        count: Mutex::new(1),
        cv: Condvar::new(),
    })));
}

pub fn PGSemaphoreReset(procno: ProcNumber) {
    let s = sema(procno);
    *s.count.lock().unwrap() = 0;
}

pub fn PGSemaphoreLock(procno: ProcNumber) {
    let s = sema(procno);
    let mut count = s.count.lock().unwrap();
    while *count <= 0 {
        count = s.cv.wait(count).unwrap();
    }
    *count -= 1;
}

pub fn PGSemaphoreUnlock(procno: ProcNumber) {
    let s = sema(procno);
    *s.count.lock().unwrap() += 1;
    s.cv.notify_one();
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
    }
}
