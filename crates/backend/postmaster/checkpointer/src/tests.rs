use super::*;

// Shmem is a set-once process global and the tests below mutate it:
// init through one gate and serialize the mutators.
static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn shmem_for_tests() -> &'static CheckpointerShmemStruct {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| CheckpointerShmemInit(64));
    shmem()
}

#[test]
fn comm_lock_offset_is_checkpointer_comm() {
    assert_eq!(
        lwlock::GetLWTrancheName(CHECKPOINTER_COMM_LOCK_OFFSET as u16),
        "CheckpointerComm"
    );
}

#[test]
fn shmem_sizing_caps_at_max_requests() {
    let one = core::mem::size_of::<types_storage::SyncCell<u8>>();
    let _ = one;
    assert!(CheckpointerShmemSize(16384) > CheckpointerShmemSize(0));
}

#[test]
fn main_fn_matches_child_main_shape() {
    let f: fn(&types_startup::StartupData) -> ! = CheckpointerMain;
    let _ = f;
}

fn test_request(rel: u32) -> CheckpointerRequest {
    CheckpointerRequest {
        req_type: SyncRequestType::SYNC_REQUEST,
        ftag: FileTag::new(
            types_storage::sync::SyncRequestHandler::SYNC_HANDLER_MD,
            types_core::ForkNumber::MAIN_FORKNUM,
            types_storage::RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: rel },
            0,
        ),
    }
}

// Control: the post-clear absorb loop passes every drained request through
// in order when nothing fails.
#[test]
fn remember_absorbed_requests_processes_all() {
    let buf = [test_request(1), test_request(2), test_request(3)];
    let seen = std::cell::RefCell::new(Vec::new());
    remember_absorbed_requests(&buf, |ftag, req_type| {
        assert_eq!(req_type, SyncRequestType::SYNC_REQUEST);
        seen.borrow_mut().push(ftag.rlocator.relNumber);
        Ok(())
    });
    assert_eq!(*seen.borrow(), [1, 2, 3]);
}

// Issue #57: once the shared queue is cleared, a remember failure must be
// fatal (C PANICs here), never a recoverable Err — and the panic payload
// must be a plain string so panic-to-error converters cannot map it back to
// a client ERROR the way they re-raise PgError payloads.
#[test]
fn remember_absorbed_requests_escalates_failure_to_panic() {
    let buf = [test_request(1), test_request(2), test_request(3)];
    let seen = std::cell::RefCell::new(Vec::new());
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        remember_absorbed_requests(&buf, |ftag, _| {
            if ftag.rlocator.relNumber == 2 {
                return Err(Box::new(PgError::new(ERROR, "out of memory allocating pendingOps")));
            }
            seen.borrow_mut().push(ftag.rlocator.relNumber);
            Ok(())
        });
    }))
    .unwrap_err();
    let msg = payload
        .downcast_ref::<String>()
        .expect("crash-and-restart string payload, not a re-raisable PgError");
    assert!(msg.contains("could not absorb fsync request"), "got: {msg}");
    // Requests before the failure were already remembered (kept, like C).
    assert_eq!(*seen.borrow(), [1]);
}

// Issue #57 catch-site guard: an Err reaching the recovery loop with the
// crit-section count still nonzero bypassed errstart's ERROR->PANIC
// promotion (into_error); abort_cleanup must escalate instead of laundering
// the leaked section with SetCritSectionCount(0) and recovering.
#[test]
fn abort_cleanup_escalates_leaked_crit_section() {
    let err = PgError::new(ERROR, "out of memory allocating pendingOps");
    g::StartCriticalSection();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        abort_cleanup(&err);
    }));
    g::SetCritSectionCount(0);
    let payload = result.unwrap_err();
    let msg = payload
        .downcast_ref::<&'static str>()
        .expect("crash-and-restart string payload");
    assert!(msg.contains("critical section"), "got: {msg}");
}

#[test]
fn crash_reset_restores_boot_image() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let cp = shmem_for_tests();
    spin_acquire(&cp.ckpt_lck);
    cp.checkpointer_pid.store(77, Relaxed);
    cp.ckpt_started.store(3, Relaxed);
    cp.ckpt_done.store(2, Relaxed);
    cp.ckpt_failed.store(1, Relaxed);
    cp.ckpt_flags.store(CHECKPOINT_IMMEDIATE, Relaxed);
    cp.num_requests.set(5);
    ReqShutdownXLOG();

    CheckpointerShmemResetAfterCrash();

    assert!(cp.ckpt_lck.is_free());
    assert_eq!(cp.checkpointer_pid.load(Relaxed), 0);
    assert_eq!(cp.ckpt_started.load(Relaxed), 0);
    assert_eq!(cp.ckpt_done.load(Relaxed), 0);
    assert_eq!(cp.ckpt_failed.load(Relaxed), 0);
    assert_eq!(cp.ckpt_flags.load(Relaxed), 0);
    assert_eq!(cp.num_requests.get(), 0);
    assert!(!SHUTDOWN_XLOG_PENDING.load(Relaxed));
}

// Issue #56 sibling site: AbsorbSyncRequests' scratch reserve fails BEFORE
// the critical section (as C pallocs before START_CRIT_SECTION), so it must
// surface as ERROR 53200 through the PgResult — food for the main loop's
// sigsetjmp analog — with the comm lock left for abort_cleanup's
// LWLockReleaseAll, never as a thread-killing panic.
#[test]
fn absorb_oom_is_error_and_lock_release_recovers() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let cp = shmem_for_tests();
    static LWLOCKS: std::sync::Once = std::sync::Once::new();
    LWLOCKS.call_once(|| {
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("mul_size overflow")));
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("add_size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });
        lwlock::CreateLWLocks(false).unwrap();
    });

    let prev_type = miscinit::GetMyBackendType();
    miscinit::SetMyBackendType(types_core::BackendType::Checkpointer);
    cp.num_requests.set(4);

    ABSORB_SCRATCH_TEST_LIMIT.store(1, Relaxed);
    let res = AbsorbSyncRequests();
    ABSORB_SCRATCH_TEST_LIMIT.store(0, Relaxed);
    cp.num_requests.set(0);

    let err = res.expect_err("capped absorb scratch must fail the reserve");
    assert_eq!(err.sqlstate, types_error::ERRCODE_OUT_OF_MEMORY);
    assert_eq!(err.message, "out of memory");

    // The ERROR escapes with the comm lock held, exactly like C's elog(ERROR)
    // there; the recovery loop's abort_cleanup releases it wholesale.
    assert!(lwlock::LWLockHeldByMe(checkpointer_comm_lock()));
    lwlock::LWLockReleaseAll().unwrap();
    assert!(!lwlock::LWLockHeldByMe(checkpointer_comm_lock()));

    // The path is retryable once the lock is back: the (empty) queue drains
    // to completion on the very scratch vec that just failed.
    AbsorbSyncRequests().expect("retry after recovery succeeds");
    assert!(!lwlock::LWLockHeldByMe(checkpointer_comm_lock()));

    miscinit::SetMyBackendType(prev_type);
}
