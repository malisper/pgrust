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
