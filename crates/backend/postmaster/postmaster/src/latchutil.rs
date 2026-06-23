//! Postmaster-latch helpers — `SetLatch(MyLatch)` / `ResetLatch(MyLatch)`.
//!
//! The postmaster's process-local latch (`MyLatch`) is established by
//! `InitProcessLocalLatch()` during `InitProcessGlobals`. The signal handlers
//! wake it; the main loop resets it after a wakeup. Both go through the real
//! latch unit.

#![allow(dead_code)]

use ::latch::{my_latch, ResetLatch, SetLatch};

/// `SetLatch(MyLatch)` — wake the postmaster's main loop. A `None` `MyLatch`
/// (the latch not yet initialized) is a no-op, matching the C handler running
/// before `MyLatch` is set (it checks `MyLatch != NULL` indirectly via the
/// async-safe `SetLatch`).
#[inline]
pub fn set_latch() {
    if let Some(l) = my_latch() {
        SetLatch(l);
    }
}

/// `ResetLatch(MyLatch)`.
#[inline]
pub fn reset_latch() {
    if let Some(l) = my_latch() {
        ResetLatch(l);
    }
}
