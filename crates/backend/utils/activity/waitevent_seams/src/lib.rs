pub const PG_WAIT_LWLOCK: u32 = 0x0100_0000;

seam_core::seam!(
    pub fn pgstat_report_wait_start(wait_event_info: u32)
);

seam_core::seam!(
    pub fn pgstat_report_wait_end()
);

seam_core::seam!(
    pub fn pgstat_set_wait_event_storage(slot: &'static core::sync::atomic::AtomicU32)
);

seam_core::seam!(
    pub fn pgstat_reset_wait_event_storage()
);
