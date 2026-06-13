//! Unit tests for the `applyparallelworker.c` port.
//!
//! These exercise the file's *own logic* that needs no installed seam: the
//! file-private constants, the savepoint-name formatting/truncation, the
//! message-pending / my-parallel-shared flag globals, and the trivial inline
//! predicates. The seam-routed state machine (DSM/shm_mq/lock sequencing) panics
//! loudly until its providers land, so those paths are intentionally not driven.

use super::*;

#[test]
fn constants_match_c() {
    assert_eq!(PG_LOGICAL_APPLY_SHM_MAGIC, 0x787c_a067);
    assert_eq!(PARALLEL_APPLY_KEY_SHARED, 1);
    assert_eq!(PARALLEL_APPLY_KEY_MQ, 2);
    assert_eq!(PARALLEL_APPLY_KEY_ERROR_QUEUE, 3);
    assert_eq!(DSM_QUEUE_SIZE, 16 * 1024 * 1024);
    assert_eq!(DSM_ERROR_QUEUE_SIZE, 16 * 1024);
    assert_eq!(PARALLEL_APPLY_LOCK_STREAM, 0);
    assert_eq!(PARALLEL_APPLY_LOCK_XACT, 1);
    assert_eq!(SHM_SEND_RETRY_INTERVAL_MS, 1000);
    assert_eq!(SHM_SEND_TIMEOUT_MS, 9000);
}

#[test]
fn size_stats_message_is_two_lsn_plus_timestamp() {
    assert_eq!(
        SIZE_STATS_MESSAGE,
        2 * core::mem::size_of::<XLogRecPtr>() + core::mem::size_of::<TimestampTz>()
    );
    assert_eq!(SIZE_STATS_MESSAGE, 24);
}

#[test]
fn savepoint_name_format() {
    let name = pa_savepoint_name(16384, 723, NAMEDATALEN);
    assert_eq!(name, "pg_sp_16384_723");
}

#[test]
fn savepoint_name_truncates_like_snprintf() {
    let full = pa_savepoint_name(4294967295, 4294967295, NAMEDATALEN);
    assert_eq!(full, "pg_sp_4294967295_4294967295");

    let trunc = pa_savepoint_name(16384, 723, 8);
    assert_eq!(trunc.len(), 7);
    assert_eq!(trunc, "pg_sp_1");

    assert_eq!(pa_savepoint_name(1, 1, 0), "");
}

#[test]
fn transaction_id_is_valid_predicate() {
    assert!(!TransactionIdIsValid(InvalidTransactionId));
    assert!(TransactionIdIsValid(1));
    assert!(TransactionIdIsValid(42));
}

#[test]
fn xlog_rec_ptr_is_invalid_predicate() {
    assert!(XLogRecPtrIsInvalid(InvalidXLogRecPtr));
    assert!(!XLogRecPtrIsInvalid(1));
    assert!(!XLogRecPtrIsInvalid(0xDEAD_BEEF));
}

#[test]
fn message_pending_flag_roundtrips() {
    set_parallel_apply_message_pending(true);
    assert!(parallel_apply_message_pending());
    set_parallel_apply_message_pending(false);
    assert!(!parallel_apply_message_pending());
}

#[test]
fn pa_find_worker_returns_none_when_xid_invalid() {
    assert_eq!(pa_find_worker(InvalidTransactionId), None);
}

#[test]
fn pa_set_stream_apply_worker_caches_index() {
    pa_set_stream_apply_worker(Some(3));
    assert!(super::with_globals(|g| g.stream_apply_worker == Some(3)));
    pa_set_stream_apply_worker(None);
    assert!(super::with_globals(|g| g.stream_apply_worker.is_none()));
}

#[test]
fn parallel_trans_state_ordering() {
    // pa_wait_for_xact_state compares with `>=`, so the discriminant order
    // (UNKNOWN < STARTED < FINISHED) is load-bearing.
    assert!(ParallelTransState::PARALLEL_TRANS_UNKNOWN < ParallelTransState::PARALLEL_TRANS_STARTED);
    assert!(
        ParallelTransState::PARALLEL_TRANS_STARTED < ParallelTransState::PARALLEL_TRANS_FINISHED
    );
    assert!(
        ParallelTransState::PARALLEL_TRANS_FINISHED >= ParallelTransState::PARALLEL_TRANS_STARTED
    );
}

#[test]
fn shared_header_xact_state_roundtrips_under_mutex() {
    // The spinlock-protected xact_state accessors are this file's own logic
    // (pa_set_xact_state / pa_get_xact_state, C 1313-1335); drive them directly
    // on an in-crate header without any installed seam.
    let shared = std::sync::Arc::new(ParallelApplyWorkerShared::new());
    assert_eq!(
        pa_get_xact_state(&shared),
        ParallelTransState::PARALLEL_TRANS_UNKNOWN
    );
    pa_set_xact_state_handle(&shared, ParallelTransState::PARALLEL_TRANS_STARTED);
    assert_eq!(
        pa_get_xact_state(&shared),
        ParallelTransState::PARALLEL_TRANS_STARTED
    );
    pa_set_xact_state_handle(&shared, ParallelTransState::PARALLEL_TRANS_FINISHED);
    assert_eq!(
        pa_get_xact_state(&shared),
        ParallelTransState::PARALLEL_TRANS_FINISHED
    );
}

#[test]
fn shared_header_fileset_state_serialize_done_needs_no_stream_fileset_for_other_states() {
    // pa_set_fileset_state's non-DONE arm only writes the field (no
    // stream_fileset read), so it works without the worker seam (C 1504-1519).
    let shared = std::sync::Arc::new(ParallelApplyWorkerShared::new());
    pa_set_fileset_state_handle(&shared, PartialFileSetState::FS_SERIALIZE_IN_PROGRESS).unwrap();
    assert_eq!(
        shared.locked.lock().unwrap().fileset_state,
        PartialFileSetState::FS_SERIALIZE_IN_PROGRESS
    );
    pa_set_fileset_state_handle(&shared, PartialFileSetState::FS_READY).unwrap();
    assert_eq!(
        shared.locked.lock().unwrap().fileset_state,
        PartialFileSetState::FS_READY
    );
}

#[test]
fn pending_stream_count_sub_fetch_semantics() {
    // pa_decr_and_wait_stream_block uses pg_atomic_sub_fetch_u32 == 0 (C 1614):
    // fetch_sub returns the old value, so old-1 is the new value.
    let shared = ParallelApplyWorkerShared::new();
    shared.pending_stream_count.store(2, SeqCst);
    assert_eq!(shared.pending_stream_count.fetch_sub(1, SeqCst) - 1, 1);
    assert_eq!(shared.pending_stream_count.fetch_sub(1, SeqCst) - 1, 0);
    assert_eq!(shared.pending_stream_count.load(SeqCst), 0);
}

#[test]
fn partial_fileset_state_distinct() {
    use PartialFileSetState::*;
    assert_ne!(FS_EMPTY, FS_READY);
    assert_ne!(FS_SERIALIZE_IN_PROGRESS, FS_SERIALIZE_DONE);
    assert_eq!(FS_EMPTY as i32, 0);
    assert_eq!(FS_SERIALIZE_IN_PROGRESS as i32, 1);
    assert_eq!(FS_SERIALIZE_DONE as i32, 2);
    assert_eq!(FS_READY as i32, 3);
}
