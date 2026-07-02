use types_error::PgResult;

seam_core::seam!(
    pub fn init_dead_lock_checking() -> PgResult<()>
);

use types_core::ProcNumber;
use types_storage::lock::{DeadLockState, LOCKMODE, LOCKTAG};

seam_core::seam!(
    // DeadLockCheck(MyProc); caller holds all lock partition LWLocks.
    pub fn dead_lock_check(procno: ProcNumber) -> DeadLockState
);

seam_core::seam!(
    // DeadLockReport(): always ereports ERRCODE_T_R_DEADLOCK_DETECTED.
    pub fn dead_lock_report() -> PgResult<()>
);

seam_core::seam!(
    // RememberSimpleDeadLock(MyProc, lockmode, lock, blocker); LOCK* is
    // marshaled to its tag.
    pub fn remember_simple_deadlock(
        checker: ProcNumber,
        lockmode: LOCKMODE,
        locktag: LOCKTAG,
        blocker: ProcNumber
    )
);

seam_core::seam!(
    // GetBlockingAutoVacuumPgproc().
    pub fn get_blocking_autovacuum_procno() -> Option<ProcNumber>
);
