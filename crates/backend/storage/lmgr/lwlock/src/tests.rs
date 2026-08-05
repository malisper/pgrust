use super::*;
use core::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Once;

fn install_shmem_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        shmem::shmem_alloc::set(|size| {
            let layout = std::alloc::Layout::from_size_align(size, 128).unwrap();
            // Cluster-lifetime allocation, deliberately leaked (C: shmem segment).
            let p = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!p.is_null());
            Ok(p)
        });
        shmem::add_size::set(|a, b| {
            a.checked_add(b)
                .ok_or_else(|| Box::new(PgError::error("requested shared memory size overflows")))
        });
        shmem::mul_size::set(|a, b| {
            a.checked_mul(b)
                .ok_or_else(|| Box::new(PgError::error("requested shared memory size overflows")))
        });
        static SHMEM_LOCK: AtomicBool = AtomicBool::new(false);
        shmem::shmem_lock_acquire::set(|| {
            while SHMEM_LOCK.swap(true, Ordering::Acquire) {
                std::hint::spin_loop();
            }
        });
        shmem::shmem_lock_release::set(|| SHMEM_LOCK.store(false, Ordering::Release));
    });
}

fn new_lock(tranche: i32) -> LWLockPadded {
    LWLockPadded::new_unlocked(tranche)
}

fn state(lock: &LWLock) -> u32 {
    lock.state.load(Ordering::Relaxed)
}

#[test]
fn shared_acquire_release() {
    let slot = new_lock(0);
    let lock = &slot.lock;
    assert_eq!(state(lock), LW_FLAG_RELEASE_OK);

    assert!(LWLockAcquire(lock, LW_SHARED, 0).unwrap());
    assert_eq!(state(lock) & LW_LOCK_MASK, LW_VAL_SHARED);
    assert!(LWLockHeldByMe(lock));
    assert!(LWLockHeldByMeInMode(lock, LW_SHARED));
    assert!(!LWLockHeldByMeInMode(lock, LW_EXCLUSIVE));

    assert!(LWLockAcquire(lock, LW_SHARED, 0).unwrap());
    assert_eq!(state(lock) & LW_LOCK_MASK, 2 * LW_VAL_SHARED);

    LWLockRelease(lock).unwrap();
    LWLockRelease(lock).unwrap();
    assert_eq!(state(lock), LW_FLAG_RELEASE_OK);
    assert!(!LWLockHeldByMe(lock));
}

#[test]
fn exclusive_acquire_release() {
    let slot = new_lock(0);
    let lock = &slot.lock;

    assert!(LWLockAcquire(lock, LW_EXCLUSIVE, 0).unwrap());
    assert_eq!(state(lock) & LW_LOCK_MASK, LW_VAL_EXCLUSIVE);
    assert!(LWLockHeldByMeInMode(lock, LW_EXCLUSIVE));

    // Conflicting attempts fail without disturbing the state.
    assert!(!LWLockConditionalAcquire(lock, LW_SHARED).unwrap());
    assert!(!LWLockConditionalAcquire(lock, LW_EXCLUSIVE).unwrap());
    assert_eq!(state(lock) & LW_LOCK_MASK, LW_VAL_EXCLUSIVE);

    LWLockRelease(lock).unwrap();
    assert_eq!(state(lock), LW_FLAG_RELEASE_OK);
}

#[test]
fn shared_blocks_exclusive_conditional() {
    let slot = new_lock(0);
    let lock = &slot.lock;
    assert!(LWLockConditionalAcquire(lock, LW_SHARED).unwrap());
    assert!(!LWLockConditionalAcquire(lock, LW_EXCLUSIVE).unwrap());
    assert!(LWLockConditionalAcquire(lock, LW_SHARED).unwrap());
    LWLockRelease(lock).unwrap();
    LWLockRelease(lock).unwrap();
}

#[test]
fn release_not_held_errors() {
    let slot = new_lock(0);
    let err = LWLockRelease(&slot.lock).unwrap_err();
    assert_eq!(err.level, ERROR);
    assert!(err.message.contains("is not held"));
}

#[test]
fn too_many_lwlocks() {
    let slots: Vec<LWLockPadded> = (0..MAX_SIMUL_LWLOCKS + 1).map(|_| new_lock(0)).collect();
    for slot in slots.iter().take(MAX_SIMUL_LWLOCKS) {
        assert!(LWLockAcquire(&slot.lock, LW_SHARED, 0).unwrap());
    }
    let err = LWLockAcquire(&slots[MAX_SIMUL_LWLOCKS].lock, LW_SHARED, 0).unwrap_err();
    assert_eq!(err.message, "too many LWLocks taken");
    let err = LWLockConditionalAcquire(&slots[MAX_SIMUL_LWLOCKS].lock, LW_SHARED).unwrap_err();
    assert_eq!(err.message, "too many LWLocks taken");
    LWLockReleaseAll().unwrap();
    assert!(!LWLockHeldByMe(&slots[0].lock));
}

#[test]
fn release_all_and_foreach() {
    let slots = [new_lock(0), new_lock(0)];
    LWLockAcquire(&slots[0].lock, LW_SHARED, 0).unwrap();
    LWLockAcquire(&slots[1].lock, LW_EXCLUSIVE, 0).unwrap();

    let mut seen = Vec::new();
    ForEachLWLockHeldByMe(|lock, mode| seen.push((lock as *const LWLock, mode)));
    assert_eq!(
        seen,
        vec![
            (&slots[0].lock as *const LWLock, LW_SHARED),
            (&slots[1].lock as *const LWLock, LW_EXCLUSIVE)
        ]
    );
    assert!(LWLockAnyHeldByMe(&slots));

    globals::HoldInterrupts(); // error recovery owns the holdoff balance
    globals::HoldInterrupts();
    LWLockReleaseAll().unwrap();
    assert!(!LWLockAnyHeldByMe(&slots));
    assert_eq!(state(&slots[0].lock), LW_FLAG_RELEASE_OK);
    assert_eq!(state(&slots[1].lock), LW_FLAG_RELEASE_OK);
}

#[test]
fn disown_and_release_disowned() {
    let slot = new_lock(0);
    let lock = &slot.lock;
    LWLockAcquire(lock, LW_EXCLUSIVE, 0).unwrap();
    LWLockDisown(lock).unwrap();
    assert!(!LWLockHeldByMe(lock));
    assert_eq!(state(lock) & LW_LOCK_MASK, LW_VAL_EXCLUSIVE);
    LWLockReleaseDisowned(lock, LW_EXCLUSIVE);
    assert_eq!(state(lock), LW_FLAG_RELEASE_OK);
}

#[test]
fn var_wait_and_update() {
    let slot = new_lock(0);
    let lock = &slot.lock;
    let var = AtomicU64::new(7);
    let mut newval = 0;

    // Lock free: returns true immediately.
    assert!(LWLockWaitForVar(lock, &var, 7, &mut newval, 0).unwrap());

    LWLockAcquire(lock, LW_EXCLUSIVE, 0).unwrap();
    // Lock held but value moved on: returns false with the current value.
    assert!(!LWLockWaitForVar(lock, &var, 3, &mut newval, 0).unwrap());
    assert_eq!(newval, 7);

    LWLockUpdateVar(lock, &var, 42);
    assert_eq!(var.load(Ordering::Relaxed), 42);

    LWLockReleaseClearVar(lock, &var, 0).unwrap();
    assert_eq!(var.load(Ordering::Relaxed), 0);
    assert_eq!(state(lock), LW_FLAG_RELEASE_OK);
}

#[test]
fn tranche_names() {
    assert_eq!(GetLWTrancheName(1), "ShmemIndex");
    assert_eq!(GetLWTrancheName(4), "ProcArray");
    assert_eq!(GetLWTrancheName(LWTRANCHE_BUFFER_MAPPING as u16), "BufferMapping");
    assert_eq!(
        GetLWTrancheName((LWTRANCHE_FIRST_USER_DEFINED - 1) as u16),
        "AioUringCompletion"
    );
    assert_eq!(
        GetLWTrancheName(LWTRANCHE_FIRST_USER_DEFINED as u16),
        "extension"
    );
    LWLockRegisterTranche(LWTRANCHE_FIRST_USER_DEFINED + 3, "my_extension");
    assert_eq!(
        GetLWTrancheName((LWTRANCHE_FIRST_USER_DEFINED + 3) as u16),
        "my_extension"
    );
    assert_eq!(
        GetLWLockIdentifier(waitevent::PG_WAIT_LWLOCK, 4),
        "ProcArray"
    );
}

#[test]
fn request_named_tranche_outside_hook_is_fatal() {
    let err = RequestNamedLWLockTranche("nope", 1, false).unwrap_err();
    assert_eq!(err.level, FATAL);
}

#[test]
fn create_lwlocks_and_named_tranche() {
    install_shmem_seams();
    RequestNamedLWLockTranche("test_tranche", 2, true).unwrap();

    let size = LWLockShmemSize().unwrap();
    assert!(size >= ((NUM_FIXED_LWLOCKS + 2) as usize) * LWLOCK_PADDED_SIZE);

    let table = CreateLWLocks(false).unwrap();
    assert_eq!(
        table.locks().len(),
        (NUM_FIXED_LWLOCKS + 2) as usize
    );
    assert_eq!(main_lock(4).tranche, 4);
    assert_eq!(
        table.lock(BUFFER_MAPPING_LWLOCK_OFFSET as usize).unwrap().tranche,
        LWTRANCHE_BUFFER_MAPPING as u16
    );

    let named = GetNamedLWLockTranche(table, "test_tranche").unwrap();
    assert_eq!(named.len(), 2);
    assert_eq!(named[0].lock.tranche as i32, table.named_tranches()[0].tranche_id);
    assert!(GetNamedLWLockTranche(table, "missing").is_err());

    // Attach leg sees the same table.
    let attached = CreateLWLocks(true).unwrap();
    assert!(core::ptr::eq(table, attached));

    // The named-tranche locks work like any other lock.
    let lock = &named[0].lock;
    assert!(LWLockAcquire(lock, LW_EXCLUSIVE, 0).unwrap());
    LWLockRelease(lock).unwrap();
}

#[test]
fn new_tranche_ids_are_unique() {
    install_shmem_seams();
    let a = LWLockNewTrancheId();
    let b = LWLockNewTrancheId();
    assert!(a >= LWTRANCHE_FIRST_USER_DEFINED);
    assert!(b > a);
}

#[test]
fn conditional_acquire_race_is_mutually_exclusive() {
    static SLOT: LWLockPadded = LWLockPadded::new_unlocked(0);
    static IN_CRIT: AtomicU32 = AtomicU32::new(0);

    let threads: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                let mut acquired = 0;
                let mut spins = 0u32;
                while acquired < 200 && spins < 2_000_000 {
                    spins += 1;
                    if LWLockConditionalAcquire(&SLOT.lock, LW_EXCLUSIVE).unwrap() {
                        assert_eq!(IN_CRIT.fetch_add(1, Ordering::AcqRel), 0);
                        IN_CRIT.fetch_sub(1, Ordering::AcqRel);
                        LWLockRelease(&SLOT.lock).unwrap();
                        acquired += 1;
                    }
                }
                acquired
            })
        })
        .collect();
    let total: u32 = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert!(total > 0);
    assert_eq!(state(&SLOT.lock) & LW_LOCK_MASK, 0);
}

#[test]
fn shared_xadd_race_keeps_count_consistent() {
    static SLOT: LWLockPadded = LWLockPadded::new_unlocked(0);

    let threads: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..500 {
                    if LWLockConditionalAcquire(&SLOT.lock, LW_SHARED).unwrap() {
                        LWLockRelease(&SLOT.lock).unwrap();
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(state(&SLOT.lock) & LW_LOCK_MASK, 0);
}

#[test]
fn contended_wait_panics_loudly_without_proc_seams() {
    static SLOT: LWLockPadded = LWLockPadded::new_unlocked(0);
    LWLockAcquire(&SLOT.lock, LW_EXCLUSIVE, 0).unwrap();
    let waiter = std::thread::spawn(|| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = LWLockAcquire(&SLOT.lock, LW_EXCLUSIVE, 1);
        }));
        let err = result.unwrap_err();
        let msg = err
            .downcast_ref::<&str>()
            .map(|m| m.to_string())
            .or_else(|| err.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        assert!(msg.contains("seam not installed"), "panic was: {msg}");
    });
    waiter.join().unwrap();
    LWLockRelease(&SLOT.lock).unwrap();
}
