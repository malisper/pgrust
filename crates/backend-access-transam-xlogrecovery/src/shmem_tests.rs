//! Tests for the `XLogRecoveryCtl` shmem keystone: allocate-or-attach the
//! region through a fake `ShmemInitStruct`, then exercise the
//! spinlock-protected state accessors.
//!
//! The seam slots are process-global (`OnceLock`, "installed twice" panics)
//! while the `XLogRecoveryCtl` pointer is thread-local, so the full lifecycle
//! (install → init → accessors) runs inside a single test on one thread.

extern crate std;

use super::shmem::*;
use core::mem::{align_of, size_of};

use types_wal::wal::RecoveryPauseState;

use backend_storage_ipc_shmem_seams as shmem_seam;
use backend_storage_lmgr_condition_variable_seams as condvar_seam;

#[test]
fn shmem_size_is_one_ctl_struct() {
    assert_eq!(
        XLogRecoveryShmemSize().unwrap(),
        size_of::<XLogRecoveryState>()
    );
}

#[test]
fn shmem_init_then_accessors_roundtrip() {
    // Install fakes for the two cross-crate seams the keystone calls during
    // `XLogRecoveryShmemInit`: a leaking `ShmemInitStruct` backing the region
    // out of the heap (a shmem region outlives the process), and a no-op
    // `ConditionVariableInit` (the CV is already zeroed by the keystone's
    // memset).
    shmem_seam::shmem_init_struct::set(|_name, size| {
        let layout =
            std::alloc::Layout::from_size_align(size, align_of::<XLogRecoveryState>()).unwrap();
        // SAFETY: non-zero size; alignment is a valid power of two.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Ok((ptr, false))
    });
    condvar_seam::condition_variable_init::set(|_cv| {});

    XLogRecoveryShmemInit().expect("XLogRecoveryShmemInit");

    // Fresh region: replay positions zero, pause state NotPaused.
    assert_eq!(get_xlog_replay_rec_ptr(), (0, 0));
    assert_eq!(get_latest_xtime(), 0);
    assert_eq!(get_current_chunk_replay_start_time(), 0);
    assert_eq!(get_recovery_pause_state(), RecoveryPauseState::NotPaused);

    // SetRecoveryPause(true): NotPaused -> PauseRequested.
    set_recovery_pause(true);
    assert_eq!(get_recovery_pause_state(), RecoveryPauseState::PauseRequested);

    // SetRecoveryPause(true) again is idempotent (only NotPaused transitions).
    set_recovery_pause(true);
    assert_eq!(get_recovery_pause_state(), RecoveryPauseState::PauseRequested);

    // SetRecoveryPause(false): -> NotPaused.
    set_recovery_pause(false);
    assert_eq!(get_recovery_pause_state(), RecoveryPauseState::NotPaused);
}
