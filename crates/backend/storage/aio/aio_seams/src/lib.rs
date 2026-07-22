seam_core::seam!(
    // `pgaio_closing_fd(fd)` (storage/aio/aio.c) — drain in-flight AIO that
    // references this kernel fd before it is closed.
    pub fn pgaio_closing_fd(fd: i32)
);

seam_core::seam!(
    // `pgaio_io_start_readv(ioh, fd, iovcnt, offset)` (storage/aio/aio_io.c).
    // The PgAioHandle and its iovec live on the AIO side of the seam; only the
    // fd/iovcnt/offset triple crosses (fd.c:2241). Staging can submit, and
    // submission can ereport, hence PgResult.
    pub fn pgaio_io_start_readv(fd: i32, iovcnt: i32, offset: i64) -> types_error::PgResult<()>
);

seam_core::seam!(
    // `pgaio_io_release_resowner(ioh_node, on_error)` (storage/aio/aio.c) —
    // resowner cleanup of a remembered AIO handle (`ioh_node` is the handle
    // index resowner stored).
    pub fn pgaio_io_release_resowner(ioh_node: usize, on_error: bool)
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
    // M1 §2.9 ring topology: eagerly create THIS thread's ring at runtime-
    // pool worker start and mark it boundary-reaped (its owner drains CQEs
    // at every task boundary, so WaitIO waiters may park on the IoToken
    // instead of blocking-reaping). Returns the ring id, or -1 when uring is
    // unavailable. Installed by aio_uring; called only by the runtime pool.
    pub fn uring_worker_ring_init() -> i32
);

seam_core::seam!(
    // M1 §2.9: tear down THIS thread's ring at pool-worker exit — waits out
    // in-flight DMA (completions run, IoTokens complete) then unmaps/closes.
    pub fn uring_worker_ring_teardown()
);

seam_core::seam!(
    // M1 §2.9 boundary duty: non-blocking drain of THIS thread's CQEs —
    // completions run (io_wref clear + TerminateBufferIO + IoToken
    // complete/unpark-all) and collected issuer pins drop. Called by the
    // runtime worker loop at every task boundary; rides the existing
    // ~1-2ms task cadence. Installed by aio_uring.
    pub fn uring_boundary_reap()
);

seam_core::seam!(
    // §2.8 declared-blocking-section entry, installed by the runtime
    // (launch_backend rtpool workers): if the calling thread is a pool
    // worker holding an execution permit, release it (a standby absorbs the
    // core) and return true — the caller MUST then call io_permit_reacquire
    // when the blocking wait ends. false = not a permit holder (plain
    // backend, standby, or already inside a blocking section): no-op, do
    // not call reacquire. First callers: aio_uring's genuinely-pending
    // uring_buf_read_wait paths (peek-complete elision happens before this).
    pub fn io_permit_release() -> bool
);

seam_core::seam!(
    // §2.8 declared-blocking-section exit: reacquire the execution permit
    // released by a true-returning io_permit_release (ordinary contender,
    // no priority). Pairs 1:1 with true returns.
    pub fn io_permit_reacquire()
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
