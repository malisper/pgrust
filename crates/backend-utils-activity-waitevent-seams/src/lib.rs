//! Seam declarations for the `backend-utils-activity-waitevent` unit
//! (`utils/activity/wait_event.c` and the `wait_event.h` inlines): the
//! per-backend wait-event slot writes. The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `pgstat_report_wait_start(wait_event_info)` (`utils/wait_event.h`) —
    /// `*(volatile uint32 *) my_wait_event_info = wait_event_info`.
    pub fn pgstat_report_wait_start(wait_event_info: u32)
);

seam_core::seam!(
    /// `pgstat_report_wait_end()` (`utils/wait_event.h`) — clear the
    /// backend's wait-event slot.
    pub fn pgstat_report_wait_end()
);
