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

// ---- ported-site tests ------------------------------------------------------

fn test_sub(skiplsn: u64, runasowner: bool) -> super::MySub {
    super::MySub {
        oid: 16384,
        name: "sub".to_string(),
        conninfo: String::new(),
        slotname: Some("sub".to_string()),
        publications: Vec::new(),
        binary: false,
        stream: pg_subscription::LOGICALREP_STREAM_OFF,
        twophasestate: pg_subscription::LOGICALREP_TWOPHASE_STATE_DISABLED,
        enabled: true,
        origin: "any".to_string(),
        synccommit: "off".to_string(),
        skiplsn,
        owner: 10,
        ownersuperuser: true,
        passwordrequired: false,
        runasowner,
        failover: false,
        disableonerr: false,
        dbid: 5,
    }
}

// ALTER SUBSCRIPTION ... SKIP: maybe_start_skipping_changes engages only when
// the transaction's finish LSN equals the subscription's skiplsn
// (worker.c:4904); stop_skipping_changes resets (worker.c:4931).
#[test]
fn skip_transaction_start_stop() {
    super::MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(test_sub(0x10, true)));

    // Non-matching finish LSN: quick return, not skipping.
    super::maybe_start_skipping_changes(0x20);
    assert!(!super::is_skipping_changes());

    // Matching finish LSN: skipping engages; stop resets.
    super::maybe_start_skipping_changes(0x10);
    assert!(super::is_skipping_changes());
    super::stop_skipping_changes();
    assert!(!super::is_skipping_changes());

    // stop when not skipping is a no-op.
    super::stop_skipping_changes();
    assert!(!super::is_skipping_changes());

    // skiplsn unset: never engages; clear_subscription_skip_lsn is the
    // C likely() quick return (no transaction or catalog access).
    super::MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(test_sub(0, true)));
    super::maybe_start_skipping_changes(0x20);
    assert!(!super::is_skipping_changes());
    let cx = mcx::MemoryContext::new("t");
    super::clear_subscription_skip_lsn(cx.mcx(), 0x20).unwrap();
}

// run_as_owner (worker.c:2427 / tablesync.c:1515): runasowner=true opts out
// of the SwitchToUntrustedUser dance entirely.
#[test]
fn run_as_owner_opt_out_skips_user_switch() {
    super::MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(test_sub(0, true)));
    let cx = mcx::MemoryContext::new("t");
    let ucxt = super::apply::maybe_switch_to_table_owner(cx.mcx(), 10).unwrap();
    assert!(ucxt.is_none());
    super::apply::restore_user_context(&ucxt).unwrap();
}

// LOGICALREP_COLUMN_BINARY decode (worker.c:826): the type receive function
// consumes the column bytes; a partial consume is C's 22P03 with the 1-based
// remote column number.
#[test]
fn binary_column_receive() {
    let cx = mcx::MemoryContext::new("t");
    let mcx = cx.mcx();

    // int4recv (oid 2406) over a network-order int4.
    let mut flinfo = fmgr::FmgrInfo::new(adt_int::builtins::fc_int4recv, 2406, 1, true, false);
    let d =
        super::apply::receive_binary_column(mcx, &mut flinfo, 0, -1, &0x01020304i32.to_be_bytes(), 0)
            .unwrap();
    assert_eq!(d.as_i32(), 0x01020304);

    // Trailing bytes the receive function didn't eat.
    let mut flinfo = fmgr::FmgrInfo::new(adt_int::builtins::fc_int4recv, 2406, 1, true, false);
    let err = super::apply::receive_binary_column(mcx, &mut flinfo, 0, -1, &[0, 0, 0, 1, 9], 2)
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_BINARY_REPRESENTATION);
    assert_eq!(
        err.message(),
        "incorrect binary data format in logical replication column 3"
    );
}

// slot_fill_defaults' column filter (worker.c:757-766).
#[test]
fn default_fill_column_filter() {
    use super::apply::needs_default_fill;
    // Subscriber-only plain column: fill.
    assert!(needs_default_fill(false, 0, -1));
    // Replicated column keeps the received value.
    assert!(!needs_default_fill(false, 0, 0));
    assert!(!needs_default_fill(false, 0, 3));
    // Dropped and generated columns never get defaults.
    assert!(!needs_default_fill(true, 0, -1));
    assert!(!needs_default_fill(false, b's' as i8, -1));
}

// errdetail_apply_conflict's origin-differs sentences (conflict.c), in the
// port's lowercased single-line rendering.
#[test]
fn origin_differs_conflict_details() {
    use super::apply::origin_differs_detail;
    assert_eq!(
        origin_differs_detail("updating", false, None, 731, "ts0"),
        "updating the row that was modified locally in transaction 731 at ts0"
    );
    assert_eq!(
        origin_differs_detail("updating", true, Some("o1"), 731, "ts0"),
        "updating the row that was modified by a different origin \"o1\" in transaction 731 at ts0"
    );
    assert_eq!(
        origin_differs_detail("deleting", true, None, 731, "ts0"),
        "deleting the row that was modified by a non-existent origin in transaction 731 at ts0"
    );
}

// ---- audit-remediation b060 witnesses ---------------------------------------

// worker.c:811 / :928 (slot_store_data / slot_modify_data): a remote column
// index beyond the received tuple's ncols is ERRCODE_PROTOCOL_VIOLATION with
// C's message (rows a186-candidate-fp-logical-worker-p1-
// b5b0dbbbdcb1e0b76a84-1 and -fd1f387229ff79cdf6bb-1).
#[test]
fn tuple_column_beyond_received_ncols_is_c_protocol_violation() {
    assert!(super::apply::tuple_column_check(0, 1).is_ok());
    assert!(super::apply::tuple_column_check(1, 2).is_ok());
    let err = super::apply::tuple_column_check(2, 2).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(
        err.message(),
        "logical replication column 3 not found in tuple: only 2 column(s) received"
    );
    let err = super::apply::tuple_column_check(0, 0).unwrap_err();
    assert_eq!(
        err.message(),
        "logical replication column 1 not found in tuple: only 0 column(s) received"
    );
}

// apply_handle_origin (worker.c:1441): ORIGIN may only arrive inside a
// streamed transaction, or inside a remote transaction before any write;
// anywhere else it is ERRCODE_PROTOCOL_VIOLATION "ORIGIN message sent out of
// order" (row a186-candidate-fp-logical-worker-p1-2f93a77d6a87e4c5c17e-1).
#[test]
fn origin_message_outside_remote_transaction_is_out_of_order() {
    let cx = mcx::MemoryContext::new("t");
    // SAFETY: `cx` outlives every use within this test.
    let mcx: mcx::Mcx<'static> = unsafe { std::mem::transmute(cx.mcx()) };
    let mut buf = vec![b'O'];
    buf.extend_from_slice(&0x10u64.to_be_bytes());
    buf.extend_from_slice(b"pg_16390\0");

    super::IN_REMOTE_TRANSACTION.set(false);
    let err = super::apply::apply_dispatch(mcx, None, &buf).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(err.message(), "ORIGIN message sent out of order");

    // Inside a remote transaction with no local transaction started yet
    // (no writes so far): accepted.
    super::IN_REMOTE_TRANSACTION.set(true);
    assert!(!xact::IsTransactionState());
    super::apply::apply_dispatch(mcx, None, &buf).unwrap();
    super::IN_REMOTE_TRANSACTION.set(false);
}

// should_refetch_tuple (execReplication.c:135): TM_Invisible is
// elog(ERROR, "attempted to lock invisible tuple") and any unexpected status
// is "unexpected table_tuple_lock status: %u" (with the colon); concurrent
// update/delete retry at LOG (rows
// a186-candidate-fp-executor-execReplication-330cd77daffe6513e1c7-1 and
// a186-candidate-fp-executor-execReplication-c4eea761e16efc36738d-1).
#[test]
fn should_refetch_tuple_maps_lock_results_c_exactly() {
    use tableam_real::TM_Result;
    assert!(matches!(
        super::apply::should_refetch_tuple(TM_Result::TM_Ok, false),
        Ok(super::apply::LockOutcome::Ok)
    ));
    assert!(matches!(
        super::apply::should_refetch_tuple(TM_Result::TM_Updated, false),
        Ok(super::apply::LockOutcome::Retry)
    ));
    assert!(matches!(
        super::apply::should_refetch_tuple(TM_Result::TM_Deleted, false),
        Ok(super::apply::LockOutcome::Retry)
    ));

    let inv = super::apply::should_refetch_tuple(TM_Result::TM_Invisible, false).unwrap_err();
    assert_eq!(inv.message(), "attempted to lock invisible tuple");

    let unexpected =
        super::apply::should_refetch_tuple(TM_Result::TM_BeingModified, false).unwrap_err();
    assert_eq!(
        unexpected.message(),
        "unexpected table_tuple_lock status: 5"
    );
}

// set_stream_options (worker.c:4463/4476): proto_version and streaming mode are
// negotiated against the publisher's server version, not hardcoded to 4 /
// "parallel" (row a186-candidate-fp-logical-worker-p2-f9a3c01dea37ab25410f-1).
#[test]
fn stream_options_negotiate_by_server_version() {
    use pg_subscription::{LOGICALREP_STREAM_OFF, LOGICALREP_STREAM_ON, LOGICALREP_STREAM_PARALLEL};

    assert_eq!(super::logicalrep_proto_version(180006), 4);
    assert_eq!(super::logicalrep_proto_version(160000), 4);
    assert_eq!(super::logicalrep_proto_version(150005), 3);
    assert_eq!(super::logicalrep_proto_version(140010), 2);
    assert_eq!(super::logicalrep_proto_version(130000), 1);

    // >= 16 + parallel mode -> "parallel"; older publishers degrade.
    assert_eq!(
        super::logicalrep_streaming_str(180006, LOGICALREP_STREAM_PARALLEL),
        Some("parallel")
    );
    assert_eq!(
        super::logicalrep_streaming_str(150000, LOGICALREP_STREAM_PARALLEL),
        Some("on")
    );
    assert_eq!(
        super::logicalrep_streaming_str(140000, LOGICALREP_STREAM_ON),
        Some("on")
    );
    assert_eq!(
        super::logicalrep_streaming_str(130000, LOGICALREP_STREAM_PARALLEL),
        None
    );
    assert_eq!(
        super::logicalrep_streaming_str(180006, LOGICALREP_STREAM_OFF),
        None
    );
}

// maybe_reread_subscription (worker.c:4064): a superuser-owned subscription
// whose owner loses superuser must restart (row
// a186-candidate-fp-logical-worker-p2-ff9cfa15c8364174721a-1).
#[test]
fn owner_superuser_revoked_predicate() {
    assert!(super::owner_superuser_revoked(true, false));
    assert!(!super::owner_superuser_revoked(true, true));
    assert!(!super::owner_superuser_revoked(false, false));
    // A non-superuser owner gaining superuser is not a revoke.
    assert!(!super::owner_superuser_revoked(false, true));
}

// libpqwalreceiver.c:1051: the CREATE_REPLICATION_SLOT consistent point goes
// through pg_lsn_in — malformed text is 22P02 with pg_lsn_in's message, and
// a valid LSN parses exactly (row
// a186-candidate-fp-libpqwalreceiver-libpqwalreceiver-50d642674ff510053dc6-1).
#[test]
fn create_slot_consistent_point_uses_pg_lsn_in() {
    assert_eq!(super::tablesync::parse_consistent_point("0/16B3748").unwrap(), 0x16B3748);
    assert_eq!(
        super::tablesync::parse_consistent_point("A/FFFFFFFF").unwrap(),
        (0xA_u64 << 32) | 0xFFFF_FFFF
    );
    let err = super::tablesync::parse_consistent_point("garbage").unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type pg_lsn: \"garbage\"");
    let err = super::tablesync::parse_consistent_point("").unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
}

// applyparallelworker.c:1045: the leader's rethrow carries the worker error's
// CONTEXT plus the parallel-apply-worker line — never the primary message
// (row a186-candidate-fp-logical-applyparallelworker-bb0db1927baf6fe07776-1).
#[test]
fn parallel_apply_worker_error_context_is_context_not_message() {
    let with_ctx = types_error::PgError::error("duplicate key value violates unique constraint \"t_pkey\"")
        .with_context("processing remote data for replication origin \"pg_16390\" during message type \"INSERT\"");
    assert_eq!(
        super::parallel::parallel_apply_worker_context(&with_ctx),
        "processing remote data for replication origin \"pg_16390\" during message type \"INSERT\"\nlogical replication parallel apply worker"
    );
    let no_ctx = types_error::PgError::error("duplicate key value violates unique constraint \"t_pkey\"");
    assert_eq!(
        super::parallel::parallel_apply_worker_context(&no_ctx),
        "logical replication parallel apply worker"
    );
}
