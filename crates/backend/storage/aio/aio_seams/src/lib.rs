seam_core::seam!(
    // `pgaio_closing_fd(fd)` (storage/aio/aio.c) — drain in-flight AIO that
    // references this kernel fd before it is closed.
    pub fn pgaio_closing_fd(fd: i32)
);

seam_core::seam!(
    // `pgaio_io_start_readv(ioh, fd, iovcnt, offset)` (storage/aio/aio_io.c).
    // The PgAioHandle and its iovec live on the AIO side of the seam; only the
    // fd/iovcnt/offset triple crosses (fd.c:2241).
    pub fn pgaio_io_start_readv(fd: i32, iovcnt: i32, offset: i64)
);

seam_core::seam!(
    pub fn at_eoxact_aio(is_commit: bool)
);

seam_core::seam!(
    // method_io_uring.c read subset: submit one read SQE for (fd, offset)
    // landing directly in shared buffer `buffer`'s pool page. Caller has the
    // buffer pinned with BM_IO_IN_PROGRESS; the submit arms desc.io_wref
    // before the SQE is visible. false = ring unavailable/full — caller backs
    // the IO out and falls back to advisory prefetch.
    pub fn uring_buf_read(fd: i32, offset: i64, buffer: i32) -> bool
);

seam_core::seam!(
    // pgaio_wref_wait shape for uring buffer reads: any thread drains the
    // owning ring until (aio_index, generation) completes or is stale.
    pub fn uring_buf_read_wait(aio_index: u32, generation: u64)
);

seam_core::seam!(
    // Nonblocking: fills `out` with buffer ids of this thread's completed
    // reads (slots freed); caller drops the issuer pins.
    pub fn uring_collect_done(out: &mut [i32]) -> usize
);

seam_core::seam!(
    // Blocking form: waits out every in-flight read on this thread's ring,
    // then collects as uring_collect_done.
    pub fn uring_drain_own(out: &mut [i32]) -> usize
);

seam_core::seam!(
    pub fn uring_available() -> bool
);

seam_core::seam!(
    // Crash-cycle reset (postmaster, all children dead): wait out every ring's
    // in-flight DMA into the pool WITHOUT running completions.
    pub fn uring_drain_all_raw()
);

seam_core::seam!(
    pub fn pgaio_error_cleanup()
);

seam_core::seam!(
    // pgaio_init_backend (storage/aio/aio_init.c).
    pub fn pgaio_init_backend()
);
