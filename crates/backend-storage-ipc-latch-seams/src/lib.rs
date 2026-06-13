//! Seam declarations for the `backend-storage-ipc-latch` unit
//! (`storage/ipc/latch.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `SetLatch(MyLatch)`: set this backend's own process latch. `MyLatch`
    /// is the per-backend latch pointer (globals.c); the latch crate resolves
    /// it when installing, so signal-handler callers (which cannot carry a
    /// `LatchHandle`) need no parameter. Async-signal-safe and infallible in
    /// C.
    pub fn set_latch_my_latch()
);

seam_core::seam!(
    /// `ResetLatch(latch)`: clear the given latch. C call sites that pass
    /// `MyLatch` translate to an explicit handle the caller holds.
    /// Infallible in C.
    pub fn reset_latch(latch: types_storage::latch::LatchHandle)
);

seam_core::seam!(
    /// `SetLatch(latch)`: set the given latch (possibly another backend's —
    /// e.g. `SetLatch(&proc->procLatch)`), waking any wait on it. Infallible
    /// in C.
    pub fn set_latch(latch: types_storage::latch::LatchHandle)
);

seam_core::seam!(
    /// `WaitLatch(latch, wakeEvents, timeout, wait_event_info)`
    /// (`storage/ipc/latch.c`): wait for the latch to be set or for one of
    /// the other requested events; returns the bitmask of events that
    /// occurred. C call sites that pass `MyLatch` translate to an explicit
    /// handle the caller holds. Can `elog/ereport(ERROR)` (bad flags, kernel
    /// event-queue failure in the underlying WaitEventSet machinery).
    pub fn wait_latch(
        latch: types_storage::latch::LatchHandle,
        wake_events: u32,
        timeout: i64,
        wait_event_info: u32,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `ResetLatch(MyLatch)`: clear this backend's own process latch; the
    /// latch crate resolves `MyLatch` (globals.c) when installing, like
    /// [`set_latch_my_latch`]. Infallible in C.
    pub fn reset_latch_my_latch()
);

seam_core::seam!(
    /// `WaitLatch(MyLatch, wake_events, timeout, wait_event_info)`: sleep
    /// until this backend's latch is set, the timeout (ms; -1 = none)
    /// elapses, or another requested `WL_*` event occurs; returns the
    /// bitmask of occurred events. The underlying `WaitEventSetWait` can
    /// `elog(ERROR)` (kernel event-queue failure), hence the `PgResult`.
    pub fn wait_latch_my_latch(
        wake_events: u32,
        timeout: i64,
        wait_event_info: u32,
    ) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// `WaitLatch(NULL, wake_events, timeout, wait_event_info)`: the
    /// no-latch wait the summarizer uses for its post-error back-off (it
    /// waits only on the timeout and `WL_EXIT_ON_PM_DEATH`, never on a
    /// latch). Returns the bitmask of occurred events; the underlying
    /// `WaitEventSetWait` can `elog(ERROR)`, hence `PgResult`.
    pub fn wait_latch_no_latch(
        wake_events: u32,
        timeout: i64,
        wait_event_info: u32,
    ) -> types_error::PgResult<u32>
);

seam_core::seam!(
    /// `SetLatch(&ProcGlobal->allProcs[pgprocno].procLatch)`: set another
    /// backend's process latch, named by its proc number (the PGPROC array
    /// is shared memory; latch.c sets the embedded latch). The
    /// no-ambient-global rule forbids a getter for the foreign latch, so the
    /// procno is passed explicitly. Infallible in C.
    pub fn set_latch_by_proc_number(pgprocno: types_core::ProcNumber)
);
