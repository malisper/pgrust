//! Seam declarations for the `backend-access-transam-xlogutils` unit
//! (`access/transam/xlogutils.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_wal::HotStandbyState;

seam_core::seam!(
    /// Read `standbyState` (`access/xlogutils.h`, a redo-side global).
    pub fn standby_state() -> HotStandbyState
);
