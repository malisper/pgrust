//! Seam declarations for the `backend-storage-ipc-waiteventset` unit
//! (`storage/ipc/waiteventset.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_storage::waiteventset::{WaitEvent, WaitEventSetHandle};

seam_core::seam!(
    /// `CreateWaitEventSet(NULL /* resowner */, nevents)` — allocate a wait
    /// event set sized for `nevents` events, not tracked by any resource
    /// owner (the only shape current consumers need; the owner marshals the
    /// missing resowner as none). Can `ereport(ERROR)` (kernel event-queue
    /// creation failure, OOM).
    pub fn create_wait_event_set(nevents: i32) -> types_error::PgResult<WaitEventSetHandle>
);

seam_core::seam!(
    /// `AddWaitEventToSet(set, events, fd, latch, NULL /* user_data */)` —
    /// register an event; returns its position. `attach_my_latch` marshals
    /// the C `latch` argument: `true` = `MyLatch` (the owner resolves the
    /// per-backend latch), `false` = `NULL`. Can `elog(ERROR)` (too many
    /// events, bad flags, kernel registration failure).
    pub fn add_wait_event_to_set(
        set: WaitEventSetHandle,
        events: u32,
        fd: types_core::pgsocket,
        attach_my_latch: bool,
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
