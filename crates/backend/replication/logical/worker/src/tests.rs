use types_core::{InvalidRepOriginId, InvalidXLogRecPtr};

// replorigin_reset (worker.c) as a before_shmem_exit hook: the exit drain of
// a dying worker must clear the session-origin advance state BEFORE
// ShutdownPostgres aborts the pending remote transaction, or the abort
// record advances the origin past an incomplete transaction (transaction
// loss — the publisher never resends it). This registers the hook exactly as
// apply_worker_body does and proves the drain resets all three variables.
#[test]
fn replorigin_reset_exit_hook_clears_session_origin_state() {
    // Mid-apply posture: origin id + lsn + timestamp all set (the state
    // apply.rs stamps before committing a remote transaction).
    origin::set_replorigin_session_origin(42);
    origin::set_replorigin_session_origin_lsn(0x1234_5678);
    origin::set_replorigin_session_origin_timestamp(777);

    // The production registration (apply_worker_body).
    ipc::before_shmem_exit(super::replorigin_reset, datum::Datum::null()).unwrap();

    // Drain the shmem-exit stacks as proc_exit would.
    ipc::shmem_exit(1).unwrap();

    assert_eq!(origin::replorigin_session_origin(), InvalidRepOriginId);
    assert_eq!(origin::replorigin_session_origin_lsn(), InvalidXLogRecPtr);
    assert_eq!(origin::replorigin_session_origin_timestamp(), 0);
}
