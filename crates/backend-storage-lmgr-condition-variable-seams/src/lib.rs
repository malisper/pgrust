//! Seam declarations for the `backend-storage-lmgr-condition-variable` unit
//! (`storage/lmgr/condition_variable.c`). The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `ConditionVariableCancelSleep()` — true if we were signaled.
    pub fn condition_variable_cancel_sleep() -> bool
);
