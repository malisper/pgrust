//! Seam declarations for the `backend-storage-ipc-waiteventset` unit
//! (`storage/ipc/waiteventset.c`), plus the owned [`WaitEventSet`] guard
//! consumers hold: C's `WaitEventSet *` is carried as an owning value whose
//! `Drop` is `FreeWaitEventSet`, never as a bare id a consumer could leak or
//! double-release (AGENTS.md "Locks and held resources").
//!
//! The owning unit installs the seams from its `init_seams()` when it lands;
//! until then a call panics loudly.

use ::types_storage::latch::LatchHandle;
use ::types_storage::waiteventset::{WaitEvent, WaitEventSetHandle};

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
    /// `AddWaitEventToSet(set, events, fd, latch, user_data)` — register an
    /// event; returns its position. `latch` mirrors the C `Latch *` argument
    /// (`None` = `NULL`); `WL_LATCH_SET` callers pass the latch they hold (C's
    /// `MyLatch` reads become explicit parameters at the call sites).
    /// `user_data` mirrors the C `void *user_data` payload stored on the event
    /// and handed back by `WaitEventSetWait` (`None` = `NULL`); the owned model
    /// carries a non-aliasing key (see [`WaitEvent::user_data`]). Can
    /// `elog(ERROR)` (too many events, bad flags, kernel registration failure).
    pub fn add_wait_event_to_set(
        set: WaitEventSetHandle,
        events: u32,
        fd: types_core::pgsocket,
        latch: Option<LatchHandle>,
        user_data: Option<i32>,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `ModifyWaitEvent(set, pos, events, latch)` — change the event mask
    /// (and, for a `WL_LATCH_SET` position, the latch) of position `pos`.
    /// `latch` mirrors the C `Latch *` argument (`None` = `NULL`). Can
    /// `elog(ERROR)`.
    pub fn modify_wait_event(
        set: WaitEventSetHandle,
        pos: i32,
        events: u32,
        latch: Option<LatchHandle>,
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
    /// `GetNumRegisteredWaitEvents(set)` — the number of events currently
    /// registered in the set (`set->nevents`). Infallible. Consumers call
    /// [`WaitEventSet::num_registered_events`].
    pub fn get_num_registered_wait_events(set: WaitEventSetHandle) -> i32
);

seam_core::seam!(
    /// `FreeWaitEventSet(set)` — release the set's kernel object and memory.
    /// Infallible in C. Called from [`WaitEventSet`]'s `Drop`, never
    /// directly by consumers.
    pub fn free_wait_event_set(set: WaitEventSetHandle)
);

seam_core::seam!(
    /// `WakeupMyProc(void)` — wake this process's own blocked
    /// `WaitEventSetWaitBlock()` (self-pipe byte or `kill(MyProcPid,
    /// SIGURG)`, only when `waiting`). Async-signal-safe and infallible in
    /// C; `SetLatch` uses it when the latch owner is the current process.
    pub fn wakeup_my_proc()
);

seam_core::seam!(
    /// `WakeupOtherProc(pid)` — `kill(pid, SIGURG)` to wake another
    /// process's blocked wait; errors are ignored in C. Infallible.
    pub fn wakeup_other_proc(pid: i32)
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

    /// `AddWaitEventToSet(set, events, fd, latch, user_data)`; see
    /// [`add_wait_event_to_set`] for the `latch`/`user_data` marshaling
    /// (`None` = `NULL`; `WL_LATCH_SET` callers pass the latch they hold).
    pub fn add_event(
        &self,
        events: u32,
        fd: types_core::pgsocket,
        latch: Option<LatchHandle>,
        user_data: Option<i32>,
    ) -> types_error::PgResult<i32> {
        add_wait_event_to_set::call(self.0, events, fd, latch, user_data)
    }

    /// `ModifyWaitEvent(set, pos, events, latch)`; `latch` as in
    /// [`add_wait_event_to_set`] (`None` = `NULL`).
    pub fn modify_event(
        &self,
        pos: i32,
        events: u32,
        latch: Option<LatchHandle>,
    ) -> types_error::PgResult<()> {
        modify_wait_event::call(self.0, pos, events, latch)
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

    /// `GetNumRegisteredWaitEvents(set)`.
    pub fn num_registered_events(&self) -> i32 {
        get_num_registered_wait_events::call(self.0)
    }
}

impl Drop for WaitEventSet {
    /// `FreeWaitEventSet(set)`.
    fn drop(&mut self) {
        free_wait_event_set::call(self.0);
    }
}

seam_core::seam!(
    /// `InitializeWaitEventSupport()` (`storage/ipc/waiteventset.c`) — set up
    /// the process-local wait-event support (epoll/kqueue fd, self-pipe). Called
    /// from `InitPostmasterChild`/`InitStandaloneProcess`. `Err` on the
    /// epoll_create/pipe failure path (`elog(ERROR)`).
    pub fn initialize_wait_event_support() -> types_error::PgResult<()>
);
