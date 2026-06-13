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
