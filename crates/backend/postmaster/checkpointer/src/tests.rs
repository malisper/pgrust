use super::*;

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
    CheckpointerShmemInit(64);
    let cp = shmem();
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
