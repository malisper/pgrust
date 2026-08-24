use super::*;

#[test]
fn check_io_method_accepts_sync_and_worker_refuses_others() {
    let mut extra = None;
    for v in [guc_tables::consts::IOMETHOD_SYNC, guc_tables::consts::IOMETHOD_WORKER] {
        let mut v = v;
        assert!(check_io_method(&mut v, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap());
    }
    let mut v = 2; // io_uring's C value: unported until inc-2
    assert!(!check_io_method(&mut v, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap());
}

#[test]
fn check_io_max_concurrency_bounds() {
    let mut extra = None;
    let mut v = -1;
    assert!(check_io_max_concurrency(&mut v, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap());
    let mut v = 0;
    assert!(!check_io_max_concurrency(&mut v, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap());
    let mut v = 7;
    assert!(check_io_max_concurrency(&mut v, &mut extra, types_guc::GucSource::PGC_S_TEST).unwrap());
}

#[test]
fn worker_submission_queue_lock_offset_is_pinned() {
    assert_eq!(
        lwlock::GetLWTrancheName(method_worker::AIO_WORKER_SUBMISSION_QUEUE_LOCK as u16),
        "AioWorkerSubmissionQueue"
    );
}

#[test]
fn wref_roundtrip() {
    let w = types_storage::buf::PgAioWaitRef {
        aio_index: 7,
        generation_upper: 1,
        generation_lower: 0xffff_fffe,
    };
    assert!(pgaio_wref_valid(&w));
    let mut w2 = w;
    pgaio_wref_clear(&mut w2);
    assert!(!pgaio_wref_valid(&w2));
}

#[test]
fn io_handles_per_backend_ignores_clobbered_guc() {
    // Boot-time geometry: 3 backends, 8 handles each = 24 handles total.
    BACKEND_COUNT.store(3, Ordering::Relaxed);
    HANDLE_COUNT.store(24, Ordering::Relaxed);

    // Simulate child-launch GUC republication clobbering io_max_concurrency
    // back to its -1 sentinel (which becomes u32::MAX when cast).
    IO_MAX_CONCURRENCY.store(-1, Ordering::Relaxed);

    // The scan bound used by pgaio_io_wait_for_free must come from the
    // immutable table geometry, NOT the live GUC.
    assert_eq!(io_handles_per_backend(), 8);
    assert_ne!(io_handles_per_backend() as i64, io_max_concurrency() as i64);
}
