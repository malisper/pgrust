//! Seam declarations for the `backend-utils-activity-waitevent` unit
//! (`utils/activity/wait_event.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `pgstat_report_wait_end()` — clear this backend's advertised wait
    /// event.
    pub fn pgstat_report_wait_end()
);
