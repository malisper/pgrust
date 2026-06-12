//! Seam declarations for the `backend-storage-lmgr-proc` unit
//! (`storage/lmgr/proc.c`): waits, the `DeadlockTimeout` GUC, and thin
//! accessors for `MyProc` fields the standby unit touches. The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

use types_core::TimestampTz;
use types_error::PgResult;

seam_core::seam!(
    /// `ProcWaitForSignal(wait_event_info)` — wait on the process latch until
    /// signalled. Can `ereport(ERROR)` via `CHECK_FOR_INTERRUPTS`.
    pub fn proc_wait_for_signal(wait_event_info: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `DeadlockTimeout` (proc.c GUC, in milliseconds).
    pub fn deadlock_timeout() -> i32
);

seam_core::seam!(
    /// `pg_atomic_read_u64(&MyProc->waitStart)`.
    pub fn my_proc_wait_start() -> TimestampTz
);

seam_core::seam!(
    /// `pg_atomic_write_u64(&MyProc->waitStart, value)`.
    pub fn set_my_proc_wait_start(value: TimestampTz)
);

seam_core::seam!(
    /// `MyProc->vxid.procNumber = value` — stamp the proc's vxid proc number
    /// (standby.c does this for the Startup process before
    /// `VirtualXactLockTableInsert`).
    pub fn set_my_proc_vxid_proc_number(value: types_core::ProcNumber)
);
