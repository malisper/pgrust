//! Seam declarations for the `backend-storage-lmgr-proc` unit
//! (`storage/lmgr/proc.c`, incl. its per-backend `MyProc` fields and the
//! `TransactionTimeout` GUC it owns). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::LocalTransactionId;

seam_core::seam!(
    /// Read `MyProc->vxid.lxid`.
    pub fn my_proc_lxid() -> LocalTransactionId
);

seam_core::seam!(
    /// Write `MyProc->vxid.lxid` (StartTransaction advertises the new local
    /// xid in the proc array).
    pub fn set_my_proc_lxid(lxid: LocalTransactionId)
);

seam_core::seam!(
    /// Read the `transaction_timeout` GUC (`int TransactionTimeout`, proc.c).
    pub fn transaction_timeout() -> i32
);

seam_core::seam!(
    /// `LockErrorCleanup()` — clean up any open wait-for-lock state.
    pub fn lock_error_cleanup()
);

seam_core::seam!(
    /// Set/clear the `DELAY_CHKPT_START` bit in `MyProc->delayChkptFlags`
    /// (the commit critical section's checkpoint interlock).
    pub fn my_proc_set_delay_chkpt_start(on: bool)
);
