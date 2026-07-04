use super::*;

#[test]
fn init_struct_create_then_attach() {
    let (p1, found1) = ShmemInitStruct("test_create_attach", 256).unwrap();
    assert!(!found1);
    assert_eq!(p1 as usize % PG_CACHE_LINE_SIZE, 0);
    // SAFETY: fresh 256-byte allocation owned by the registry.
    unsafe {
        assert!(std::slice::from_raw_parts(p1, 256).iter().all(|&b| b == 0));
        p1.write(0xAB);
    }

    let (p2, found2) = ShmemInitStruct("test_create_attach", 256).unwrap();
    assert!(found2);
    assert_eq!(p1, p2);
    // SAFETY: same allocation as above.
    unsafe { assert_eq!(p2.read(), 0xAB) };
}

#[test]
fn init_struct_size_mismatch_errors() {
    ShmemInitStruct("test_size_mismatch", 128).unwrap();
    let err = ShmemInitStruct("test_size_mismatch", 192).unwrap_err();
    assert!(err.message.contains("ShmemIndex entry size is wrong"));
    assert!(err.message.contains("expected 192, actual 128"));
}

#[test]
fn alloc_is_aligned_and_zeroed() {
    let p = ShmemAlloc(1).unwrap();
    assert_eq!(p as usize % PG_CACHE_LINE_SIZE, 0);
    let q = ShmemAllocNoError(4096);
    assert!(!q.is_null());
    // SAFETY: 4096 bytes freshly allocated above.
    unsafe { assert!(std::slice::from_raw_parts(q, 4096).iter().all(|&b| b == 0)) };
}

#[test]
fn alloc_overflow_is_oom() {
    let err = ShmemAlloc(usize::MAX - 7).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_OUT_OF_MEMORY);
    assert!(err.message.contains("out of shared memory"));
    assert!(ShmemAllocNoError(usize::MAX - 7).is_null());
}

#[test]
fn size_arithmetic_checks_overflow() {
    assert_eq!(add_size(3, 4).unwrap(), 7);
    assert_eq!(mul_size(3, 4).unwrap(), 12);
    assert_eq!(mul_size(0, usize::MAX).unwrap(), 0);
    assert_eq!(mul_size(usize::MAX, 0).unwrap(), 0);

    let err = add_size(usize::MAX, 1).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(err.message, "requested shared memory size overflows size_t");
    let err = mul_size(usize::MAX, 2).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
}

#[test]
fn shmem_lock_excludes() {
    init_seams();
    if !s_lock_seams::perform_spin_delay::is_installed() {
        s_lock::init_seams();
    }
    if !waitevent_seams::pgstat_report_wait_start::is_installed() {
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
    }
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let threads: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..1000 {
                    shmem_seams::shmem_lock_acquire::call();
                    let v = COUNTER.load(Ordering::Relaxed);
                    COUNTER.store(v + 1, Ordering::Relaxed);
                    shmem_seams::shmem_lock_release::call();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(COUNTER.load(Ordering::Relaxed), 4000);

    assert_eq!(shmem_seams::add_size::call(1, 2).unwrap(), 3);
    assert_eq!(shmem_seams::mul_size::call(2, 3).unwrap(), 6);
    let (p, found) = shmem_seams::shmem_init_struct::call("test_via_seam", 64).unwrap();
    assert!(!p.is_null() && !found);
    assert!(!shmem_seams::shmem_alloc::call(64).unwrap().is_null());
}

#[test]
fn concurrent_init_struct_single_creation() {
    let results: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                let (p, found) = ShmemInitStruct("test_concurrent", 512).unwrap();
                (p.expose_provenance(), found)
            })
        })
        .collect();
    let results: Vec<_> = results.into_iter().map(|t| t.join().unwrap()).collect();
    let addr = results[0].0;
    assert!(results.iter().all(|&(p, _)| p == addr));
    assert_eq!(results.iter().filter(|&&(_, found)| !found).count(), 1);
}
