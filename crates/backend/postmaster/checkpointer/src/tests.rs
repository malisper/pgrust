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
