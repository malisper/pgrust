//! Tests for the checkpointer port.
//!
//! These exercise the IN-CRATE logic that does NOT require shared memory:
//!   * the shmem struct's `#[repr(C)]` layout (`offset_of`) vs. the C struct;
//!   * `requests_offset` (the flexible-array sizing);
//!   * the process-local GUC accessors;
//!   * the `SyncRequestType` raw round-trip.

use super::*;
use core::mem::{align_of, offset_of, size_of};

#[test]
fn checkpointer_shmem_struct_field_order() {
    // pid_t is the first field at offset 0.
    assert_eq!(offset_of!(CheckpointerShmemStruct, checkpointer_pid), 0);

    // The ckpt_* counters follow the spinlock, in declaration order.
    assert!(
        offset_of!(CheckpointerShmemStruct, ckpt_lck)
            < offset_of!(CheckpointerShmemStruct, ckpt_started)
    );
    assert!(
        offset_of!(CheckpointerShmemStruct, ckpt_started)
            < offset_of!(CheckpointerShmemStruct, ckpt_done)
    );
    assert!(
        offset_of!(CheckpointerShmemStruct, ckpt_done)
            < offset_of!(CheckpointerShmemStruct, ckpt_failed)
    );
    assert!(
        offset_of!(CheckpointerShmemStruct, ckpt_failed)
            < offset_of!(CheckpointerShmemStruct, ckpt_flags)
    );

    // The condition variables come after the counters and before the request
    // bookkeeping.
    assert!(
        offset_of!(CheckpointerShmemStruct, ckpt_flags)
            < offset_of!(CheckpointerShmemStruct, start_cv)
    );
    assert!(
        offset_of!(CheckpointerShmemStruct, start_cv)
            < offset_of!(CheckpointerShmemStruct, done_cv)
    );
    assert!(
        offset_of!(CheckpointerShmemStruct, done_cv)
            < offset_of!(CheckpointerShmemStruct, num_requests)
    );
    assert!(
        offset_of!(CheckpointerShmemStruct, num_requests)
            < offset_of!(CheckpointerShmemStruct, max_requests)
    );
}

#[test]
fn requests_offset_is_aligned_past_header() {
    let off = requests_offset();
    // The flexible array starts at or after the end of the bookkeeping field.
    assert!(off >= offset_of!(CheckpointerShmemStruct, max_requests) + size_of::<i32>());
    // ... and is aligned for CheckpointerRequest.
    assert_eq!(off % align_of::<CheckpointerRequest>(), 0);
}

#[test]
fn checkpointer_request_repr() {
    // The dedup keys on (type_, ftag); both fields are present and Copy.
    let r = CheckpointerRequest {
        type_: SyncRequestType::SYNC_REQUEST as i32,
        ftag: FileTag::default(),
    };
    let r2 = r;
    assert_eq!(r2.type_, 0);
}

#[test]
fn sync_request_type_round_trip() {
    for t in [
        SyncRequestType::SYNC_REQUEST,
        SyncRequestType::SYNC_UNLINK_REQUEST,
        SyncRequestType::SYNC_FORGET_REQUEST,
        SyncRequestType::SYNC_FILTER_REQUEST,
    ] {
        assert_eq!(sync_request_type_from_raw(t as i32), t);
    }
}

#[test]
fn guc_accessors_default_and_set() {
    assert_eq!(CheckPointTimeout(), 300);
    assert_eq!(CheckPointWarning(), 30);
    assert_eq!(CheckPointCompletionTarget(), 0.9);

    set_CheckPointTimeout(60);
    set_CheckPointWarning(5);
    set_CheckPointCompletionTarget(0.5);
    assert_eq!(CheckPointTimeout(), 60);
    assert_eq!(CheckPointWarning(), 5);
    assert_eq!(CheckPointCompletionTarget(), 0.5);
}
