//! Latch identity (`storage/latch.h`), trimmed to what consumers of the
//! unported latch unit need: a way to *name* a particular `Latch` (C passes
//! `Latch *`) when calling `SetLatch`/`ResetLatch`/`AddWaitEventToSet`.
//!
//! The C `struct Latch` itself (`is_set`, `maybe_sleeping`, `is_shared`,
//! `owner_pid`) lives in backend-private or shared memory owned by the latch
//! unit; no current consumer reads its fields, so only the identity token is
//! modeled. C call sites that read `MyLatch` (globals.c) translate to an
//! explicit `LatchHandle` parameter — the caller obtains it from its own
//! state when globals.c lands (AGENTS.md "no ambient-global seams").

/// Identity of a `Latch *` owned by the latch unit. `0` is never a valid
/// handle.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct LatchHandle(usize);

impl LatchHandle {
    /// Mint a handle. Only the latch owner (and test fakes) creates these.
    pub fn new(id: usize) -> Self {
        LatchHandle(id)
    }

    /// The owner-side id this handle names.
    pub fn as_usize(self) -> usize {
        self.0
    }
}

use core::sync::atomic::AtomicI32;

use types_core::sig_atomic_t;

/// `struct Latch` (`storage/latch.h`).
///
/// In C a latch is always reached through a pointer (`MyLatch`,
/// `&proc->procLatch`) and is mutated concurrently: `SetLatch` runs from
/// signal handlers and, for shared latches living in PGPROC shared memory,
/// from other backends. The `volatile sig_atomic_t` wait/set fields are
/// therefore atomics here, and a latch is shared by handle
/// (e.g. `Arc<Latch>`), never copied by value. `is_shared` / `owner_pid` are
/// written only by `InitLatch`/`InitSharedLatch`/`OwnLatch` before the latch
/// is visible to other parties.
#[derive(Debug)]
pub struct Latch {
    /// `sig_atomic_t is_set;`
    pub is_set: AtomicI32,
    /// `sig_atomic_t maybe_sleeping;`
    pub maybe_sleeping: AtomicI32,
    /// `bool is_shared;`
    pub is_shared: bool,
    /// `int owner_pid;`
    pub owner_pid: i32,
}

impl Latch {
    /// A cleared latch (`is_set`/`maybe_sleeping` zero), as `InitLatch`
    /// leaves the flag fields.
    pub fn new(is_shared: bool, owner_pid: i32) -> Latch {
        Latch {
            is_set: AtomicI32::new(0),
            maybe_sleeping: AtomicI32::new(0),
            is_shared,
            owner_pid,
        }
    }
}

/// Assert the C field widths: `sig_atomic_t` is `int` on every supported
/// target and `AtomicI32` has the same in-memory representation.
const _: () = assert!(core::mem::size_of::<AtomicI32>() == core::mem::size_of::<sig_atomic_t>());
