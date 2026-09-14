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

// errdetail_apply_conflict's conflict-type sentences (conflict.c:200-275),
// C's text byte for byte: origin forms for the origin-differs types, and the
// with/without commit-timestamp forms for the unique-index types.
#[test]
fn conflict_type_detail_sentences() {
    use super::conflict::{conflict_type_detail, ConflictType};
    assert_eq!(
        conflict_type_detail(ConflictType::UpdateOriginDiffers, "", 731, Some("ts0"), None),
        "Updating the row that was modified locally in transaction 731 at ts0."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::UpdateOriginDiffers, "", 731, Some("ts0"), Some(Some("o1"))),
        "Updating the row that was modified by a different origin \"o1\" in transaction 731 at ts0."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::DeleteOriginDiffers, "", 731, Some("ts0"), Some(None)),
        "Deleting the row that was modified by a non-existent origin in transaction 731 at ts0."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::InsertExists, "t_pkey", 731, None, None),
        "Key already exists in unique index \"t_pkey\", modified in transaction 731."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::UpdateExists, "t_pkey", 731, Some("ts0"), None),
        "Key already exists in unique index \"t_pkey\", modified locally in transaction 731 at ts0."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::MultipleUniqueConflicts, "t_pkey", 731, Some("ts0"), Some(Some("o1"))),
        "Key already exists in unique index \"t_pkey\", modified by origin \"o1\" in transaction 731 at ts0."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::InsertExists, "t_pkey", 731, Some("ts0"), Some(None)),
        "Key already exists in unique index \"t_pkey\", modified by a non-existent origin in transaction 731 at ts0."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::UpdateMissing, "", 0, None, None),
        "Could not find the row to be updated."
    );
    assert_eq!(
        conflict_type_detail(ConflictType::DeleteMissing, "", 0, None, None),
        "Could not find the row to be deleted."
    );
    assert_eq!(ConflictType::MultipleUniqueConflicts as usize, 6);
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

// InitializeLogRepWorker (worker.c:4751): the tablesync start line carries
// the table NAME (get_rel_name), not the relation OID (row
// a186-candidate-fp-logical-worker-p2-eb4eccd992480bf6704a-1).
#[test]
fn tablesync_worker_started_line_names_the_table() {
    assert_eq!(
        super::tablesync_worker_started_message("s", Some("my_table")),
        "logical replication table synchronization worker for subscription \"s\", table \"my_table\" has started"
    );
    // C's snprintf renders a NULL get_rel_name as "(null)".
    assert_eq!(
        super::tablesync_worker_started_message("s", None),
        "logical replication table synchronization worker for subscription \"s\", table \"(null)\" has started"
    );
    assert_eq!(
        super::apply_worker_started_message("s"),
        "logical replication apply worker for subscription \"s\" has started"
    );
}

// ---- audit-remediation w2-021 witnesses ------------------------------------

fn errarg_rel() -> logicalproto::LogicalRepRelation {
    logicalproto::LogicalRepRelation {
        remoteid: 42,
        nspname: "public".into(),
        relname: "ts_t".into(),
        natts: 2,
        attnames: vec!["id".into(), "v".into()],
        atttyps: vec![23, 25],
        replident: b'd',
        relkind: b'r',
        attkeys: vec![true, false],
    }
}

// apply_error_callback (worker.c:5053-5121): the six errcontext forms, chosen
// by rel / remote_attnum / remote_xid / finish_lsn, and the command == 0
// early return (rows a186-candidate-fp-logical-applyparallelworker-
// 01931f170b8e9e5e4bf8-1 and -worker-p1-ab5cef64f7346570236f-1).
#[test]
fn apply_error_callback_six_errcontext_forms_match_c() {
    use super::{apply_error_context_line, ApplyErrorCallbackArg};
    let mut a = ApplyErrorCallbackArg::INITIAL;
    a.origin_name = Some("pg_16390".to_string());
    assert_eq!(apply_error_context_line(&a), None);

    a.command = logicalproto::LOGICAL_REP_MSG_BEGIN;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"BEGIN\""
    );
    a.remote_xid = 731;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"BEGIN\" in transaction 731"
    );
    a.finish_lsn = 0x1_6B37_48;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"BEGIN\" in transaction 731, finished at 0/16B3748"
    );

    a.command = logicalproto::LOGICAL_REP_MSG_INSERT;
    a.rel = Some(errarg_rel());
    a.finish_lsn = 0;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"INSERT\" for replication target relation \"public.ts_t\" in transaction 731"
    );
    a.finish_lsn = (0xA_u64 << 32) | 0xFFFF_FFFF;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"INSERT\" for replication target relation \"public.ts_t\" in transaction 731, finished at A/FFFFFFFF"
    );
    a.remote_attnum = 1;
    a.finish_lsn = 0;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"INSERT\" for replication target relation \"public.ts_t\" column \"v\" in transaction 731"
    );
    a.finish_lsn = 0x16B3748;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"INSERT\" for replication target relation \"public.ts_t\" column \"v\" in transaction 731, finished at 0/16B3748"
    );
    // An unknown command prints C's "??? (%d)".
    a.command = 0x7f;
    a.rel = None;
    a.remote_xid = 0;
    assert_eq!(
        apply_error_context_line(&a).unwrap(),
        "processing remote data for replication origin \"pg_16390\" during message type \"??? (127)\""
    );
}

// apply_dispatch (worker.c:3393/3486) saves the command on entry and restores
// it only on the normal exit; an error leaves the innermost command for the
// callback, and the apply loop's frame attaches its line to the propagating
// error (worker.c:3616-3622) — the ORIGIN out-of-order error carries C's
// CONTEXT line.
#[test]
fn apply_error_context_attaches_to_a_propagating_apply_error() {
    super::MY_SUBSCRIPTION.with(|s| *s.borrow_mut() = Some(test_sub(0, true)));
    super::reset_apply_error_context_info();
    super::set_apply_error_context_origin("pg_16384");
    let cx = mcx::MemoryContext::new("t");
    // SAFETY: `cx` outlives every use within this test.
    let mcx: mcx::Mcx<'static> = unsafe { std::mem::transmute(cx.mcx()) };
    let mut buf = vec![b'O'];
    buf.extend_from_slice(&0x10u64.to_be_bytes());
    buf.extend_from_slice(b"pg_16390\0");

    let frame = super::ApplyErrorContextFrame::push();

    // Normal exit: the saved command (0) is restored.
    super::IN_REMOTE_TRANSACTION.set(true);
    frame.attach(super::apply::apply_dispatch(mcx, None, &buf)).unwrap();
    assert_eq!(super::set_apply_error_context_command(0), 0);

    // Error exit: the command stays 'O' and the frame's attach adds the line.
    super::IN_REMOTE_TRANSACTION.set(false);
    let err = frame.attach(super::apply::apply_dispatch(mcx, None, &buf)).unwrap_err();
    assert_eq!(err.message(), "ORIGIN message sent out of order");
    assert_eq!(
        err.context(),
        Some("processing remote data for replication origin \"pg_16384\" during message type \"ORIGIN\"")
    );
    drop(frame);
    super::reset_apply_error_context_info();
}

// quote_literal_cstr (quote.c:103): a backslash forces the E'' form so a
// publisher with standard_conforming_strings = off reads the same bytes.
#[test]
fn tablesync_quote_literal_cstr_matches_c() {
    assert_eq!(super::tablesync::quote_literal_cstr("pub"), "'pub'");
    assert_eq!(super::tablesync::quote_literal_cstr("it's"), "'it''s'");
    assert_eq!(super::tablesync::quote_literal_cstr("p\\n0"), "E'p\\\\n0'");
}

// libpqrcv_startstreaming (libpqwalreceiver.c:630): binary needs a >= 14
// publisher, two_phase >= 15, origin >= 16; older publishers never see them.
#[test]
fn start_replication_gates_options_on_publisher_version() {
    let pubs = vec!["p".to_string(), "q\"r".to_string()];
    let cmd = |v: i32| {
        super::start_replication_command(v, "s", 0x1_0000_0010, &pubs, true, "none", None, true)
    };
    assert_eq!(
        cmd(180006),
        "START_REPLICATION SLOT \"s\" LOGICAL 1/10 (proto_version '4', two_phase 'on', \
         origin 'none', publication_names '\"p\",\"q\"\"r\"', binary 'true')"
    );
    let c15 = cmd(150005);
    assert!(c15.contains(", two_phase 'on'") && c15.contains(", binary 'true'"));
    assert!(!c15.contains("origin"));
    let c14 = cmd(140010);
    assert!(c14.contains(", binary 'true'"));
    assert!(!c14.contains("two_phase") && !c14.contains("origin"));
    let c13 = cmd(130000);
    assert!(!c13.contains("binary") && !c13.contains("two_phase") && !c13.contains("origin"));
    assert!(c13.ends_with(", publication_names '\"p\",\"q\"\"r\"')"));
    let streamed = super::start_replication_command(
        160000, "s", 0, &pubs, false, "any", Some("parallel"), false,
    );
    assert!(streamed.contains("(proto_version '4', streaming 'parallel', publication_names"));
    assert!(!streamed.contains("origin") && !streamed.contains("two_phase"));
}

// ProcessParallelApplyMessages (applyparallelworker.c:1141): a pool worker
// that exited without parking an error (SIGUSR2, proc_exit) detached its
// error queue, which the leader reports as 55000 "lost connection ...";
// a worker the leader itself is stopping (mailbox detached) is skipped.
#[test]
fn leader_reports_lost_connection_for_a_silently_exited_pa_worker() {
    if init_small::globals::MyProcNumber() == types_core::INVALID_PROC_NUMBER {
        init_small::globals::SetMyProcNumber(7);
    }
    let w = super::parallel::test_pool_worker();
    super::parallel::ProcessParallelApplyMessages().unwrap();
    super::parallel::test_worker_exited(&w);
    let err = super::parallel::ProcessParallelApplyMessages().unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(
        err.message(),
        "lost connection to the logical replication parallel apply worker"
    );
    super::parallel::pa_detach_all_error_mq();
    super::parallel::ProcessParallelApplyMessages().unwrap();
    // The leader's exit releases every pooled worker's segment
    // (dsm_backend_shutdown): the registry no longer holds its queue.
    let before = super::parallel::test_registry_len();
    super::parallel::pa_release_pool_dsm();
    assert_eq!(super::parallel::test_registry_len(), before - 1);
    super::parallel::ProcessParallelApplyMessages().unwrap();
}

// get_flush_position (worker.c:3505): the write (apply) position is the
// latest locally committed remote end — the tail entry once an unflushed
// commit is hit — while flush stops at the last flushed entry.
#[test]
fn flush_position_reports_tail_remote_end_as_write_while_commits_await_flush() {
    super::LSN_MAPPING.with(|m| m.borrow_mut().clear());
    super::store_flush_position(0x100, 0x10);
    super::store_flush_position(0x200, 0x20);
    super::store_flush_position(0x300, 0x30);

    // Local flush at 0x20: two entries drained, write = tail's remote end.
    assert_eq!(super::get_flush_position_at(0x20), (0x300, 0x200, true));
    assert_eq!(super::LSN_MAPPING.with(|m| m.borrow().len()), 1);
    // Nothing flushed yet: write still the tail, flush invalid.
    super::store_flush_position(0x400, 0x40);
    assert_eq!(super::get_flush_position_at(0x25), (0x400, InvalidXLogRecPtr, true));
    // Everything flushed: both positions at the last entry, list empty.
    assert_eq!(super::get_flush_position_at(0x40), (0x400, 0x400, false));
    assert_eq!(super::LSN_MAPPING.with(|m| m.borrow().len()), 0);
}

// store_flush_position (worker.c:3548): parallel apply workers keep no
// lsn_mapping (the leader maintains it), so nothing accumulates per commit.
#[test]
fn parallel_apply_worker_does_not_accumulate_lsn_mappings() {
    if init_small::globals::MyProcNumber() == types_core::INVALID_PROC_NUMBER {
        init_small::globals::SetMyProcNumber(7);
    }
    super::LSN_MAPPING.with(|m| m.borrow_mut().clear());
    let w = super::parallel::test_pool_worker();
    super::parallel::test_become_pa_worker(&w);
    super::store_flush_position(0x100, 0x10);
    super::store_flush_position(0x200, 0x20);
    assert_eq!(super::LSN_MAPPING.with(|m| m.borrow().len()), 0);
    super::parallel::test_leave_pa_worker();
    super::store_flush_position(0x300, 0x30);
    assert_eq!(super::LSN_MAPPING.with(|m| m.borrow().len()), 1);
    super::LSN_MAPPING.with(|m| m.borrow_mut().clear());
}

// LogicalRepApplyLoop reads the 'w'/'k' headers with pq_getmsgint64 /
// pq_getmsgbyte (worker.c:3672-3708) and apply_dispatch reads the action with
// pq_getmsgbyte (worker.c:3385): short or empty data is 08P01, never a silent
// skip.
#[test]
fn short_stream_headers_and_empty_payloads_are_protocol_violations() {
    let mut w = Vec::new();
    w.extend_from_slice(&0x10u64.to_be_bytes());
    w.extend_from_slice(&0x20u64.to_be_bytes());
    w.extend_from_slice(&7i64.to_be_bytes());
    w.push(b'X');
    let (s, e, t, payload) = super::parse_wal_data_message(&w).unwrap();
    assert_eq!((s, e, t, payload), (0x10, 0x20, 7, &b"X"[..]));
    let err = super::parse_wal_data_message(&w[..23]).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(err.message(), "insufficient data left in message");

    let mut k = Vec::new();
    k.extend_from_slice(&0x30u64.to_be_bytes());
    k.extend_from_slice(&9i64.to_be_bytes());
    k.push(1);
    assert_eq!(super::parse_keepalive_message(&k).unwrap(), (0x30, 9, true));
    let err = super::parse_keepalive_message(&k[..16]).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(err.message(), "no data left in message");

    let cx = mcx::MemoryContext::new("t");
    // SAFETY: `cx` outlives every use within this test.
    let mcx: mcx::Mcx<'static> = unsafe { std::mem::transmute(cx.mcx()) };
    let err = super::apply::apply_dispatch(mcx, None, &[]).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(err.message(), "no data left in message");
}

// A parallel apply worker leaving through proc_exit (SIGTERM's FATAL) still
// parks its report in the error queue and pokes the leader from the
// before_shmem_exit stage (applyparallelworker.c:918/937), so the leader
// reports "exited due to error" with the worker's context rather than a
// generic queue error.
#[test]
fn pa_worker_fatal_exit_parks_the_report_for_the_leader() {
    if init_small::globals::MyProcNumber() == types_core::INVALID_PROC_NUMBER {
        init_small::globals::SetMyProcNumber(7);
    }
    let w = super::parallel::test_pool_worker();
    super::parallel::test_become_pa_worker(&w);
    ipc::before_shmem_exit(super::parallel::pa_shutdown_on_exit, datum::Datum::from_i64(0))
        .unwrap();

    // A WARNING is not queued; the FATAL emitted before proc_exit is.
    let mut warn = types_error::PgError::new(types_error::WARNING, "noise");
    super::parallel::pa_park_error_report(&mut warn);
    let mut fatal = types_error::PgError::new(
        types_error::FATAL,
        "terminating logical replication worker due to administrator command",
    );
    fatal.add_context_line("processing remote data for replication origin \"pg_16384\"");
    super::parallel::pa_park_error_report(&mut fatal);
    ipc::shmem_exit(1).unwrap();
    super::parallel::test_leave_pa_worker();

    let err = super::parallel::ProcessParallelApplyMessages().unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    assert_eq!(err.message(), "logical replication parallel apply worker exited due to error");
    assert_eq!(
        err.context(),
        Some("processing remote data for replication origin \"pg_16384\"\nlogical replication parallel apply worker")
    );
    super::parallel::pa_detach_all_error_mq();
}

// fetch_remote_table_info's column list (tablesync.c:1023): int2vector text
// from the publisher's pg_get_publication_tables(...).attrs.
#[test]
fn publication_column_list_parses_as_int2vector() {
    assert_eq!(super::tablesync::parse_int2vector("1 3 4"), vec![1, 3, 4]);
    assert_eq!(super::tablesync::parse_int2vector(""), Vec::<i16>::new());
}
