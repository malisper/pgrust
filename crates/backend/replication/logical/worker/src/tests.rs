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
        skiplsn,
        owner: 10,
        ownersuperuser: true,
        passwordrequired: false,
        runasowner,
        failover: false,
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
