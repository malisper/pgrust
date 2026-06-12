//! Seam declarations for the `backend-access-transam-xlogutils` unit
//! (`access/transam/xlogutils.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// Read `standbyState` (`access/xlogutils.h` `HotStandbyState`, a redo-side
    /// global): 0 = STANDBY_DISABLED, 1 = INITIALIZED, 2 = SNAPSHOT_PENDING,
    /// 3 = SNAPSHOT_READY.
    pub fn standby_state() -> i32
);
