//! Seam declarations for the `backend-storage-ipc-waiteventset` unit
//! (`storage/ipc/waiteventset.c`), plus the owned [`WaitEventSet`] guard
//! consumers hold: C's `WaitEventSet *` is carried as an owning value whose
//! `Drop` is `FreeWaitEventSet`, never as a bare id a consumer could leak or
//! double-release (AGENTS.md "Locks and held resources").
//!
//! The owning unit installs the seams from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WaitEvent, WaitEventSetHandle};

seam_core::seam!(
    /// `CreateWaitEventSet(NULL /* resowner */, nevents)` — allocate a wait
    /// event set sized for `nevents` events, not tracked by any resource
    /// owner (the only shape current consumers need; the owner marshals the
    /// missing resowner as none). Can `ereport(ERROR)` (kernel event-queue
    /// creation failure, OOM). Consumers call [`WaitEventSet::create`]
    /// rather than this raw seam.
    pub fn create_wait_event_set(nevents: i32) -> types_error::PgResult<WaitEventSetHandle>
);

seam_core::seam!(
    /// `AddWaitEventToSet(set, events, fd, latch, NULL /* user_data */)` —
    /// register an event; returns its position. `latch` mirrors the C
    /// `Latch *` argument (`None` = `NULL`); `WL_LATCH_SET` callers pass the
    /// latch they hold (C's `MyLatch` reads become explicit parameters at
    /// the call sites). Can `elog(ERROR)` (too many events, bad flags,
    /// kernel registration failure).
    pub fn add_wait_event_to_set(
        set: WaitEventSetHandle,
        events: u32,
        fd: types_core::pgsocket,
        latch: Option<LatchHandle>,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `ModifyWaitEvent(set, pos, events, NULL /* latch */)` — change the
    /// event mask of position `pos` (latchless shape; the only one current
    /// consumers need). Can `elog(ERROR)`.
    pub fn modify_wait_event(
        set: WaitEventSetHandle,
        pos: i32,
        events: u32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `WaitEventSetWait(set, timeout, occurred_events, nevents,
    /// wait_event_info)` — wait for events; fills `occurred_events` (up to
    /// its length) and returns the count, `0` on timeout. Can
    /// `ereport(ERROR)` and runs `CHECK_FOR_INTERRUPTS`-adjacent processing.
    pub fn wait_event_set_wait(
        set: WaitEventSetHandle,
        timeout: i64,
        occurred_events: &mut [WaitEvent],
        wait_event_info: u32,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `FreeWaitEventSet(set)` — release the set's kernel object and memory.
    /// Infallible in C. Called from [`WaitEventSet`]'s `Drop`, never
    /// directly by consumers.
    pub fn free_wait_event_set(set: WaitEventSetHandle)
);

/// Owned `WaitEventSet *` (`storage/waiteventset.h`). Dropping the value is
/// `FreeWaitEventSet`; the methods marshal the raw owner-side handle into
/// the seams above so consumers never hold a bare id.
#[derive(Debug)]
pub struct WaitEventSet(WaitEventSetHandle);

impl WaitEventSet {
    /// `CreateWaitEventSet(NULL, nevents)`.
    pub fn create(nevents: i32) -> types_error::PgResult<WaitEventSet> {
        Ok(WaitEventSet(create_wait_event_set::call(nevents)?))
    }

    /// `AddWaitEventToSet(set, events, fd, latch, NULL)`; see
    /// [`add_wait_event_to_set`] for the `latch` marshaling (`None` =
    /// `NULL`; `WL_LATCH_SET` callers pass the latch they hold).
    pub fn add_event(
        &self,
        events: u32,
        fd: types_core::pgsocket,
        latch: Option<LatchHandle>,
    ) -> types_error::PgResult<i32> {
        add_wait_event_to_set::call(self.0, events, fd, latch)
    }

    /// `ModifyWaitEvent(set, pos, events, NULL)`.
    pub fn modify_event(&self, pos: i32, events: u32) -> types_error::PgResult<()> {
        modify_wait_event::call(self.0, pos, events)
    }

    /// `WaitEventSetWait(set, timeout, occurred_events, nevents,
    /// wait_event_info)`.
    pub fn wait(
        &self,
        timeout: i64,
        occurred_events: &mut [WaitEvent],
        wait_event_info: u32,
    ) -> types_error::PgResult<i32> {
        wait_event_set_wait::call(self.0, timeout, occurred_events, wait_event_info)
    }
}

impl Drop for WaitEventSet {
    /// `FreeWaitEventSet(set)`.
    fn drop(&mut self) {
        free_wait_event_set::call(self.0);
    }
}
