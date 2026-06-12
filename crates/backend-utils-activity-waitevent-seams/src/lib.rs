//! Seam declarations for the `backend-utils-activity-waitevent` unit
//! (`utils/activity/wait_event.c`, `wait_event_funcs.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::uint32;

seam_core::seam!(
    /// `pgstat_report_wait_start(wait_event_info)` (`utils/wait_event.h`) —
    /// record that the backend is now blocked on this wait event (for LWLocks:
    /// the tranche id OR'd with `PG_WAIT_LWLOCK`).
    pub fn pgstat_report_wait_start(wait_event_info: uint32)
);

seam_core::seam!(
    /// `pgstat_report_wait_end()` — record that the backend is no longer
    /// waiting.
    pub fn pgstat_report_wait_end()
);
