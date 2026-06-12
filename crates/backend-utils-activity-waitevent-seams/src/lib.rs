//! Seam declarations for the `backend-utils-activity-waitevent` unit
//! (`utils/activity/wait_event.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `pgstat_report_wait_start(wait_event_info)`.
    pub fn pgstat_report_wait_start(wait_event_info: u32)
);

seam_core::seam!(
    /// `pgstat_report_wait_end()`.
    pub fn pgstat_report_wait_end()
);
