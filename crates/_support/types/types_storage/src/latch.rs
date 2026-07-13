use core::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use ::types_core::{sig_atomic_t, ProcNumber};

// Tagged handle naming a C `Latch *`: the latch unit's own registry (Local),
// a PGPROC's embedded procLatch (Proc, PROC_TAG bit set), or the single
// recovery-wakeup latch (reserved id usize::MAX).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LatchHandle(usize);

pub const PROC_TAG: usize = 1usize << (usize::BITS - 1);

pub const RECOVERY_WAKEUP_ID: usize = usize::MAX;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum LatchKind {
    Local(usize),
    Proc(ProcNumber),
    RecoveryWakeup,
}

impl LatchHandle {
    pub fn new(id: usize) -> Self {
        debug_assert!(id & PROC_TAG == 0, "local LatchHandle id overflows PROC_TAG");
        LatchHandle(id)
    }

    pub fn proc(procno: ProcNumber) -> Self {
        debug_assert!(procno >= 0);
        LatchHandle(PROC_TAG | (procno as usize))
    }

    pub fn recovery_wakeup() -> Self {
        LatchHandle(RECOVERY_WAKEUP_ID)
    }

    pub fn as_usize(self) -> usize {
        self.0
    }

    /// Rebuild a handle from `as_usize`'s value (stored-in-atomic round trip).
    pub fn from_raw(raw: usize) -> Self {
        LatchHandle(raw)
    }

    pub fn kind(self) -> LatchKind {
        if self.0 == RECOVERY_WAKEUP_ID {
            LatchKind::RecoveryWakeup
        } else if self.0 & PROC_TAG != 0 {
            LatchKind::Proc((self.0 & !PROC_TAG) as ProcNumber)
        } else {
            LatchKind::Local(self.0)
        }
    }
}

// Every field atomic: SetLatch runs from signal handlers and, for shared
// latches in PGPROC shmem, from other backends — all through `&Latch`.
#[derive(Debug)]
pub struct Latch {
    pub is_set: AtomicI32,
    pub maybe_sleeping: AtomicI32,
    pub is_shared: AtomicBool,
    pub owner_pid: AtomicI32,
}

impl Latch {
    pub const fn new(is_shared: bool, owner_pid: i32) -> Latch {
        Latch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            is_shared: AtomicBool::new(is_shared),
            owner_pid: AtomicI32::new(owner_pid),
        }
    }

    pub fn is_set(&self) -> bool {
        self.is_set.load(Ordering::SeqCst) != 0
    }

    pub fn set_maybe_sleeping(&self, value: bool) {
        self.maybe_sleeping.store(value as i32, Ordering::SeqCst);
    }

    pub fn owner_pid(&self) -> i32 {
        self.owner_pid.load(Ordering::SeqCst)
    }
}

const _: () = assert!(core::mem::size_of::<AtomicI32>() == core::mem::size_of::<sig_atomic_t>());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latch_handle_kind_round_trip() {
        assert_eq!(LatchHandle::new(3).kind(), LatchKind::Local(3));
        assert_eq!(LatchHandle::proc(17).kind(), LatchKind::Proc(17));
        assert_eq!(
            LatchHandle::recovery_wakeup().kind(),
            LatchKind::RecoveryWakeup
        );
    }
}
