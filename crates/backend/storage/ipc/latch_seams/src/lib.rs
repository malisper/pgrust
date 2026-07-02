seam_core::seam!(
    // SetLatch(MyLatch) — async-signal-safe in C; the installed impl is
    // handler-reachable and must stay allocation-free.
    pub fn set_latch_my_latch()
);
