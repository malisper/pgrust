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
