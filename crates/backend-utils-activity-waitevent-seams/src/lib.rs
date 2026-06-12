//! Seam declarations for the `backend-utils-activity-waitevent` unit
//! (`utils/activity/wait_event.c`, `utils/wait_event.h`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgstat_report_wait_start(uint32 wait_event_info)` — advertise in
    /// `MyProc->wait_event_info` that this backend is waiting on the given
    /// classified wait event.
    pub fn pgstat_report_wait_start(wait_event_info: u32)
);

seam_core::seam!(
    /// `pgstat_report_wait_end()` — clear the advertised wait event.
    pub fn pgstat_report_wait_end()
);
