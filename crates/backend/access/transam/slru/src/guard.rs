use core::ptr::NonNull;
use init_small::globals;
use lwlock::{LWLock, LWLockAcquire, LWLockConditionalAcquire, LWLockMode, LWLockRelease};
use types_error::PgResult;

// Resource-guard module (docs/no-drop.md): `Drop` here is the abort path,
// mirroring C's transaction-abort `LWLockReleaseAll` for an ereport unwind
// with SLRU locks held. Explicit `unlock`/`release` is the ordered path.
pub struct LwGuard {
    lock: NonNull<LWLock>,
    mode: LWLockMode,
    held: bool,
}

impl LwGuard {
    pub fn acquire(lock: &LWLock, mode: LWLockMode) -> PgResult<Self> {
        LWLockAcquire(lock, mode, globals::MyProcNumber())?;
        Ok(Self {
            lock: NonNull::from(lock),
            mode,
            held: true,
        })
    }

    pub fn conditional_acquire(lock: &LWLock, mode: LWLockMode) -> PgResult<Option<Self>> {
        if LWLockConditionalAcquire(lock, mode)? {
            Ok(Some(Self {
                lock: NonNull::from(lock),
                mode,
                held: true,
            }))
        } else {
            Ok(None)
        }
    }

    fn lock_ref(&self) -> &LWLock {
        // SAFETY: the pointer was created from a live `&LWLock` into the
        // cluster-lifetime shared segment (SLRU locks are never freed).
        unsafe { self.lock.as_ref() }
    }

    pub fn unlock(&mut self) -> PgResult<()> {
        debug_assert!(self.held);
        self.held = false;
        LWLockRelease(self.lock_ref())
    }

    pub fn relock(&mut self, mode: LWLockMode) -> PgResult<()> {
        debug_assert!(!self.held);
        LWLockAcquire(self.lock_ref(), mode, globals::MyProcNumber())?;
        self.mode = mode;
        self.held = true;
        Ok(())
    }

    pub fn release(mut self) -> PgResult<()> {
        self.unlock()
    }

    pub fn covers(&self, lock: &LWLock) -> bool {
        core::ptr::eq(self.lock.as_ptr(), lock)
    }

    pub fn is_held(&self) -> bool {
        self.held
    }

    pub fn held_in_mode(&self, lock: &LWLock, mode: LWLockMode) -> bool {
        self.held && self.mode == mode && self.covers(lock)
    }
}

impl Drop for LwGuard {
    fn drop(&mut self) {
        if self.held {
            let _ = LWLockRelease(self.lock_ref());
        }
    }
}
