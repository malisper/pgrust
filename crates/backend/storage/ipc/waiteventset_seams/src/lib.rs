use types_core::pgsocket;
use types_error::PgResult;
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::{WaitEvent, WaitEventSetHandle};

seam_core::seam!(
    pub fn create_wait_event_set(nevents: i32) -> PgResult<WaitEventSetHandle>
);

seam_core::seam!(
    pub fn create_wait_event_set_current_owner(nevents: i32) -> PgResult<WaitEventSetHandle>
);

seam_core::seam!(
    pub fn add_wait_event_to_set(
        set: WaitEventSetHandle,
        events: u32,
        fd: pgsocket,
        latch: Option<LatchHandle>,
        user_data: Option<i32>,
    ) -> PgResult<i32>
);

seam_core::seam!(
    pub fn modify_wait_event(
        set: WaitEventSetHandle,
        pos: i32,
        events: u32,
        latch: Option<LatchHandle>,
    ) -> PgResult<()>
);

seam_core::seam!(
    // WaitEventSetWait with nevents=1 (latch.c's only shape); None = timeout.
    pub fn wait_event_set_wait_one(
        set: WaitEventSetHandle,
        timeout: i64,
        wait_event_info: u32,
    ) -> PgResult<Option<WaitEvent>>
);

seam_core::seam!(
    pub fn free_wait_event_set(set: WaitEventSetHandle)
);

seam_core::seam!(
    // Reachable from SetLatch in signal handlers: impls must be allocation-free.
    pub fn wakeup_my_proc()
);

seam_core::seam!(
    pub fn wakeup_other_proc(pid: i32)
);

seam_core::seam!(
    // Retained-thread pid refresh (wretain): re-key this thread's wakeup-pipe
    // registry entry to the current MyProcPid.
    pub fn rekey_wakeup_registry()
);
