seam_core::seam!(
    // SetLatch(MyLatch) — async-signal-safe in C; the installed impl is
    // handler-reachable and must stay allocation-free.
    pub fn set_latch_my_latch()
);

use types_storage::latch::Latch;

seam_core::seam!(
    pub fn own_latch<'a>(latch: &'a Latch)
);

seam_core::seam!(
    pub fn disown_latch<'a>(latch: &'a Latch)
);

seam_core::seam!(
    pub fn set_latch<'a>(latch: &'a Latch)
);

seam_core::seam!(
    pub fn wait_latch_my_latch(wake_events: u32, timeout: i64, wait_event_info: u32) -> u32
);

seam_core::seam!(
    pub fn reset_latch_my_latch()
);
