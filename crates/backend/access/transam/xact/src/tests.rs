use crate::*;
use std::cell::RefCell;
use std::sync::Once;

thread_local! {
    static LOG: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

fn log_push(v: usize) {
    LOG.with(|l| l.borrow_mut().push(v));
}

fn log_take() -> Vec<usize> {
    LOG.with(|l| std::mem::take(&mut *l.borrow_mut()))
}

fn install_test_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        parallel_seams::is_parallel_worker::set(|| false);
        xlog_seams::xlog_logical_info_active::set(|| false);
        origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
        origin_seams::replorigin_session_origin_lsn::set(|| 0);
        origin_seams::replorigin_session_origin_timestamp::set(|| 0);
        xloginsert_seams::xlog_insert_with_flags::set(|rmid, info, flags, fragments| {
            assert_eq!(rmid, RM_XACT_ID);
            assert_eq!(flags, XLOG_INCLUDE_ORIGIN);
            CAPTURED.with(|c| {
                let mut body = Vec::new();
                for f in fragments {
                    body.extend_from_slice(f);
                }
                *c.borrow_mut() = Some((info, body));
            });
            Ok(1234)
        });
    });
}

thread_local! {
    static CAPTURED: RefCell<Option<(u8, Vec<u8>)>> = const { RefCell::new(None) };
}

fn cb1(_e: XactEvent, arg: Datum) -> PgResult<()> {
    log_push(arg.as_usize());
    Ok(())
}

fn cb_self_unregister(e: XactEvent, arg: Datum) -> PgResult<()> {
    let _ = e;
    log_push(arg.as_usize());
    UnregisterXactCallback(cb_self_unregister, arg);
    Ok(())
}

fn cb_nested(_e: XactEvent, _arg: Datum) -> PgResult<()> {
    log_push(99);
    Ok(())
}

fn cb_registers_nested(_e: XactEvent, arg: Datum) -> PgResult<()> {
    log_push(arg.as_usize());
    RegisterXactCallback(cb_nested, Datum::from_usize(0));
    UnregisterXactCallback(cb_nested, Datum::from_usize(0));
    Ok(())
}

#[test]
fn callbacks_run_newest_first() {
    reset_xact_state_for_tests();
    for tag in [1usize, 2, 3] {
        RegisterXactCallback(cb1, Datum::from_usize(tag));
    }
    CallXactCallbacks(xs_ptr(), XACT_EVENT_COMMIT).unwrap();
    assert_eq!(log_take(), vec![3, 2, 1]);
    reset_xact_state_for_tests();
}

// Miri: fn-pointer identity is not stable under Miri (casts may mint distinct
// addresses), so UnregisterXactCallback's pointer-equality match can miss —
// same class as resowner's fn-pointer-identity test (no UB; pre-existing,
// verified against a tree with this batch's edits stashed).
#[cfg_attr(miri, ignore)]
#[test]
fn self_unregistration_is_safe() {
    reset_xact_state_for_tests();
    RegisterXactCallback(cb1, Datum::from_usize(1));
    RegisterXactCallback(cb_self_unregister, Datum::from_usize(2));
    CallXactCallbacks(xs_ptr(), XACT_EVENT_COMMIT).unwrap();
    CallXactCallbacks(xs_ptr(), XACT_EVENT_COMMIT).unwrap();
    assert_eq!(log_take(), vec![2, 1, 1]);
    reset_xact_state_for_tests();
}

#[test]
fn mid_iteration_registration_not_invoked_this_round() {
    reset_xact_state_for_tests();
    RegisterXactCallback(cb_registers_nested, Datum::from_usize(1));
    CallXactCallbacks(xs_ptr(), XACT_EVENT_COMMIT).unwrap();
    assert_eq!(log_take(), vec![1]);
    reset_xact_state_for_tests();
}

#[test]
fn block_status_code_idle_by_default() {
    reset_xact_state_for_tests();
    assert_eq!(TransactionBlockStatusCode(), b'I');
    assert!(!IsTransactionBlock());
    assert!(!IsTransactionOrTransactionBlock());
    assert!(!IsAbortedTransactionBlockState());
    assert!(!IsTransactionState());
    reset_xact_state_for_tests();
}

#[test]
fn command_counter_increment_noop_when_unused() {
    reset_xact_state_for_tests();
    CommandCounterIncrement().unwrap();
    reset_xact_state_for_tests();
}

#[test]
fn get_current_command_id_marks_used() {
    install_test_seams();
    reset_xact_state_for_tests();
    assert_eq!(GetCurrentCommandId(false).unwrap(), FirstCommandId);
    assert_eq!(GetCurrentCommandId(true).unwrap(), FirstCommandId);
    assert!(xs(|s| s.command_id_used()));
    reset_xact_state_for_tests();
}

#[test]
fn isolation_predicates_track_level() {
    reset_xact_state_for_tests();
    SetXactIsoLevel(XACT_READ_COMMITTED);
    assert!(!IsolationUsesXactSnapshot());
    assert!(!IsolationIsSerializable());
    SetXactIsoLevel(XACT_REPEATABLE_READ);
    assert!(IsolationUsesXactSnapshot());
    SetXactIsoLevel(XACT_SERIALIZABLE);
    assert!(IsolationIsSerializable());
    reset_xact_state_for_tests();
}

#[test]
fn parallel_mode_nesting() {
    reset_xact_state_for_tests();
    assert!(!IsInParallelMode());
    EnterParallelMode();
    assert!(IsInParallelMode());
    EnterParallelMode();
    ExitParallelMode();
    assert!(IsInParallelMode());
    ExitParallelMode();
    assert!(!IsInParallelMode());
    reset_xact_state_for_tests();
}

#[test]
fn my_xact_flags_or_path() {
    reset_xact_state_for_tests();
    assert_eq!(MyXactFlags(), 0);
    OrMyXactFlags(XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
    OrMyXactFlags(XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK);
    assert_eq!(
        MyXactFlags(),
        XACT_FLAGS_ACCESSEDTEMPNAMESPACE | XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK
    );
    reset_xact_state_for_tests();
}

#[test]
fn commit_record_round_trips_through_parser() {
    install_test_seams();
    reset_xact_state_for_tests();

    XactLogCommitRecord(777, &[], &[], &[], &[], false, 0, InvalidTransactionId, None).unwrap();
    let (info, body) = CAPTURED.with(|c| c.borrow_mut().take()).unwrap();
    assert_eq!(info, XLOG_XACT_COMMIT);
    assert_eq!(body.len(), 8);
    let parsed = parse_commit_record(info, &body).unwrap();
    assert_eq!(parsed.xact_time, 777);
    assert_eq!(parsed.xinfo, 0);
    assert!(parsed.subxacts.is_empty());

    let subs = [10u32, 11, 12];
    XactLogCommitRecord(
        888,
        &subs,
        &[],
        &[],
        &[],
        false,
        XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK,
        InvalidTransactionId,
        None,
    )
    .unwrap();
    let (info, body) = CAPTURED.with(|c| c.borrow_mut().take()).unwrap();
    assert_eq!(info, XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO);
    let parsed = parse_commit_record(info, &body).unwrap();
    assert_eq!(parsed.xact_time, 888);
    assert_eq!(parsed.subxacts, vec![10, 11, 12]);
    assert_ne!(parsed.xinfo & XACT_XINFO_HAS_AE_LOCKS, 0);
    assert_ne!(parsed.xinfo & XACT_XINFO_HAS_SUBXACTS, 0);
    reset_xact_state_for_tests();
}

#[test]
#[ignore = "child of commit_record_works_without_origin_seam"]
fn commit_record_no_origin_seam_child() {
    // Fresh process: origin seams deliberately NOT installed; the C default
    // (InvalidRepOriginId) must apply and the record must carry no origin.
    assert!(!origin_seams::replorigin_session_origin::is_installed());
    parallel_seams::is_parallel_worker::set(|| false);
    xlog_seams::xlog_logical_info_active::set(|| false);
    xloginsert_seams::xlog_insert_with_flags::set(|_, info, flags, fragments| {
        assert_eq!(flags, XLOG_INCLUDE_ORIGIN);
        CAPTURED.with(|c| {
            let mut body = Vec::new();
            for f in fragments {
                body.extend_from_slice(f);
            }
            *c.borrow_mut() = Some((info, body));
        });
        Ok(1234)
    });
    reset_xact_state_for_tests();

    XactLogCommitRecord(777, &[], &[], &[], &[], false, 0, InvalidTransactionId, None).unwrap();
    let (info, body) = CAPTURED.with(|c| c.borrow_mut().take()).unwrap();
    assert_eq!(info, XLOG_XACT_COMMIT);
    let parsed = parse_commit_record(info, &body).unwrap();
    assert_eq!(parsed.xinfo & XACT_XINFO_HAS_ORIGIN, 0);
}

#[test]
#[cfg_attr(miri, ignore)] // spawns a child process, unsupported under Miri
fn commit_record_works_without_origin_seam() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::commit_record_no_origin_seam_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "child failed: {out:?}");
}

#[test]
fn abort_record_round_trips_through_parser() {
    install_test_seams();
    reset_xact_state_for_tests();
    XactLogAbortRecord(555, &[7], &[], &[], 0, InvalidTransactionId, None).unwrap();
    let (info, body) = CAPTURED.with(|c| c.borrow_mut().take()).unwrap();
    assert_eq!(info, XLOG_XACT_ABORT | XLOG_XACT_HAS_INFO);
    let parsed = parse_abort_record(info, &body).unwrap();
    assert_eq!(parsed.xact_time, 555);
    assert_eq!(parsed.subxacts, vec![7]);
    reset_xact_state_for_tests();
}

#[test]
fn parser_rejects_truncated_records() {
    assert!(parse_commit_record(XLOG_XACT_COMMIT, &[0u8; 4]).is_err());
    let mut body = Vec::new();
    body.extend_from_slice(&1i64.to_ne_bytes());
    body.extend_from_slice(&XACT_XINFO_HAS_SUBXACTS.to_ne_bytes());
    body.extend_from_slice(&100i32.to_ne_bytes()); // claims 100 subxacts, none present
    assert!(parse_commit_record(XLOG_XACT_COMMIT | XLOG_XACT_HAS_INFO, &body).is_err());
}

#[test]
fn savepoint_ops_rejected_outside_blocks() {
    install_test_seams();
    reset_xact_state_for_tests();
    assert!(DefineSavepoint(Some("sp")).is_err());
    assert!(ReleaseSavepoint("sp").is_err());
    assert!(RollbackToSavepoint("sp").is_err());
    reset_xact_state_for_tests();
}

#[test]
fn sub_transaction_id_accessors() {
    reset_xact_state_for_tests();
    assert_eq!(GetCurrentSubTransactionId(), InvalidSubTransactionId);
    assert!(!SubTransactionIsActive(5));
    assert_eq!(GetCurrentTransactionNestLevel(), 0);
    assert!(!IsSubTransaction());
    reset_xact_state_for_tests();
}

#[test]
fn transaction_id_is_current_rejects_special_xids() {
    reset_xact_state_for_tests();
    assert!(!TransactionIdIsCurrentTransactionId(InvalidTransactionId));
    assert!(!TransactionIdIsCurrentTransactionId(BootstrapTransactionId));
    assert!(!TransactionIdIsCurrentTransactionId(FrozenTransactionId));
    assert!(!TransactionIdIsCurrentTransactionId(12345));
    reset_xact_state_for_tests();
}

#[test]
fn save_restore_transaction_characteristics() {
    reset_xact_state_for_tests();
    SetXactIsoLevel(XACT_SERIALIZABLE);
    SetXactReadOnly(true);
    SetXactDeferrable(true);
    let saved = SaveTransactionCharacteristics();
    SetXactIsoLevel(XACT_READ_COMMITTED);
    SetXactReadOnly(false);
    SetXactDeferrable(false);
    RestoreTransactionCharacteristics(saved);
    assert_eq!(XactIsoLevel(), XACT_SERIALIZABLE);
    assert!(XactReadOnly());
    assert!(XactDeferrable());
    reset_xact_state_for_tests();
}
