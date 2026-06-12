//! Seam declarations for the `backend-access-transam-xlogutils` unit
//! (`access/transam/xlogutils.c`): accessors for the `standbyState` global it
//! owns. The owning unit installs these from its `init_seams()` when it
//! lands; until then a call panics loudly.

use types_wal::HotStandbyState;

seam_core::seam!(
    /// Read `standbyState` (xlogutils.c global).
    pub fn standby_state() -> HotStandbyState
);

seam_core::seam!(
    /// Write `standbyState` (standby.c sets `STANDBY_INITIALIZED`;
    /// xlogrecovery.c drives the rest of the machine).
    pub fn set_standby_state(state: HotStandbyState)
);
