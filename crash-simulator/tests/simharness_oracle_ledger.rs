//! G-O1: ledger op-log/savepoint replay + both-direction red tests +
//! SQLSTATE-expectation tests (contract §3.4).

use simharness::oracle::check::{Row, Value};
use simharness::oracle::ledger::*;
use simharness::oracle::pstep::IsoLevel;

fn kv_cols() -> Vec<ColumnDef> {
    vec![
        ColumnDef { name: "k".into(), not_null: true },
        ColumnDef { name: "v".into(), not_null: false },
    ]
}

fn row(k: i64, v: Option<i64>) -> Row {
    Row(vec![Value::Int(k), v.map(Value::Int).unwrap_or(Value::Null)])
}

fn fresh_t1() -> Ledger {
    let mut l = Ledger::new();
    let out = l
        .apply(&LedgerOp::CreateTable { table: "t1".into(), cols: kv_cols(), key: Some(0) })
        .unwrap();
    assert!(matches!(out, ApplyOutcome::Ok(_)));
    l
}

#[test]
fn insert_update_delete_truncate() {
    let mut l = fresh_t1();
    let out = l
        .apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, Some(10)), row(2, None)] })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Ok(LedgerEffect { affected: 2 }));
    assert_eq!(l.table_cardinality("t1"), Some(2));

    let out = l
        .apply(&LedgerOp::UpdateByKey {
            table: "t1".into(),
            key: Value::Int(1),
            sets: vec![(1, Value::Int(99))],
        })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Ok(LedgerEffect { affected: 1 }));
    assert_eq!(l.table_rows("t1").unwrap(), vec![row(1, Some(99)), row(2, None)]);

    // Update of a missing key affects 0 rows (not an error).
    let out = l
        .apply(&LedgerOp::UpdateByKey {
            table: "t1".into(),
            key: Value::Int(42),
            sets: vec![(1, Value::Int(0))],
        })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Ok(LedgerEffect { affected: 0 }));

    let out = l
        .apply(&LedgerOp::DeleteByKey { table: "t1".into(), key: Value::Int(2) })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Ok(LedgerEffect { affected: 1 }));
    assert_eq!(l.table_cardinality("t1"), Some(1));

    let out = l.apply(&LedgerOp::Truncate { table: "t1".into() }).unwrap();
    assert!(matches!(out, ApplyOutcome::Ok(_)));
    assert_eq!(l.table_cardinality("t1"), Some(0));
}

#[test]
fn sqlstate_expectations() {
    let mut l = fresh_t1();
    // dup CREATE => 42P07
    let out = l
        .apply(&LedgerOp::CreateTable { table: "t1".into(), cols: kv_cols(), key: Some(0) })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Err(ExpectedError { sqlstate: "42P07".into() }));
    // post-DROP => 42P01
    l.apply(&LedgerOp::DropTable { table: "t1".into() }).unwrap();
    let out = l.apply(&LedgerOp::Truncate { table: "t1".into() }).unwrap();
    assert_eq!(out, ApplyOutcome::Err(ExpectedError { sqlstate: "42P01".into() }));
    // dup key => 23505; NOT NULL => 23502
    let mut l = fresh_t1();
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, None)] })
        .unwrap();
    let out = l
        .apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, Some(5))] })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Err(ExpectedError { sqlstate: "23505".into() }));
    let out = l
        .apply(&LedgerOp::InsertValues {
            table: "t1".into(),
            rows: vec![Row(vec![Value::Null, Value::Int(1)])],
        })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Err(ExpectedError { sqlstate: "23502".into() }));
    // an expected-error statement must not have changed state
    assert_eq!(l.table_cardinality("t1"), Some(1));
    // dup key WITHIN one statement's batch
    let out = l
        .apply(&LedgerOp::InsertValues {
            table: "t1".into(),
            rows: vec![row(7, None), row(7, None)],
        })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Err(ExpectedError { sqlstate: "23505".into() }));
    // update key onto an existing other key => 23505
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(2, None)] })
        .unwrap();
    let out = l
        .apply(&LedgerOp::UpdateByKey {
            table: "t1".into(),
            key: Value::Int(2),
            sets: vec![(0, Value::Int(1))],
        })
        .unwrap();
    assert_eq!(out, ApplyOutcome::Err(ExpectedError { sqlstate: "23505".into() }));
}

#[test]
fn tx_oplog_and_savepoint_replay() {
    let mut l = fresh_t1();
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, Some(1))] })
        .unwrap();

    l.begin(IsoLevel::ReadCommitted);
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(2, Some(2))] })
        .unwrap();
    l.savepoint("s1");
    l.apply(&LedgerOp::DeleteByKey { table: "t1".into(), key: Value::Int(1) })
        .unwrap();
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(3, Some(3))] })
        .unwrap();
    assert_eq!(l.table_cardinality("t1"), Some(2)); // {2,3}

    // Roll back to s1: delete + insert(3) un-done, insert(2) kept.
    l.rollback_to("s1").unwrap();
    assert_eq!(l.table_rows("t1").unwrap(), vec![row(1, Some(1)), row(2, Some(2))]);

    // The named savepoint survives and is reusable.
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(4, Some(4))] })
        .unwrap();
    l.rollback_to("s1").unwrap();
    assert_eq!(l.table_rows("t1").unwrap(), vec![row(1, Some(1)), row(2, Some(2))]);

    l.commit();
    assert_eq!(l.table_rows("t1").unwrap(), vec![row(1, Some(1)), row(2, Some(2))]);
}

#[test]
fn tx_full_rollback_and_transactional_ddl() {
    let mut l = fresh_t1();
    l.begin(IsoLevel::Serializable);
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, None)] })
        .unwrap();
    l.apply(&LedgerOp::CreateTable { table: "t2".into(), cols: kv_cols(), key: Some(0) })
        .unwrap();
    assert!(l.table_rows("t2").is_some());
    l.rollback();
    assert_eq!(l.table_cardinality("t1"), Some(0));
    assert!(l.table_rows("t2").is_none(), "transactional DDL rolls back");
}

#[test]
fn nested_savepoints_pop_later_ones() {
    let mut l = fresh_t1();
    l.begin(IsoLevel::RepeatableRead);
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, None)] })
        .unwrap();
    l.savepoint("a");
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(2, None)] })
        .unwrap();
    l.savepoint("b");
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(3, None)] })
        .unwrap();
    l.rollback_to("a").unwrap();
    assert_eq!(l.table_cardinality("t1"), Some(1));
    // "b" was popped by rolling back to "a".
    assert!(l.rollback_to("b").is_err());
}

// ---- both-direction planted-red tests (G-O1: these verdicts MUST be
// violations — a harness that misses either direction is broken) ----

#[test]
fn red_engine_ok_where_ledger_errors() {
    let mut l = fresh_t1();
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, None)] })
        .unwrap();
    // Ledger predicts 23505 for a dup-key insert...
    let expected = l
        .apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, Some(9))] })
        .unwrap();
    assert!(matches!(expected, ApplyOutcome::Err(_)));
    // ...but the (planted-buggy) engine accepted it.
    let verdict = reconcile(&expected, &EngineDmlResult::Ok { affected: 1 });
    assert!(verdict.is_violation(), "engine-ok/ledger-errors MUST flag: {verdict:?}");
    assert_eq!(
        verdict,
        ReconcileVerdict::EngineOkLedgerErr { expected: "23505".into() }
    );
}

#[test]
fn red_engine_errors_where_ledger_accepts() {
    let mut l = fresh_t1();
    let expected = l
        .apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, None)] })
        .unwrap();
    assert!(matches!(expected, ApplyOutcome::Ok(_)));
    // Planted-buggy engine rejects a legal insert.
    let verdict = reconcile(
        &expected,
        &EngineDmlResult::Err { sqlstate: "23505".into() },
    );
    assert!(verdict.is_violation(), "engine-rejects/ledger-accepts MUST flag");
    assert_eq!(verdict, ReconcileVerdict::EngineErrLedgerOk { got: "23505".into() });
}

#[test]
fn reconcile_class_and_effect() {
    // Same class, different detail code => Match (class-granular compare).
    let exp = ApplyOutcome::Err(ExpectedError { sqlstate: "23505".into() });
    assert_eq!(
        reconcile(&exp, &EngineDmlResult::Err { sqlstate: "23502".into() }),
        ReconcileVerdict::Match
    );
    // Different class => mismatch.
    assert!(reconcile(&exp, &EngineDmlResult::Err { sqlstate: "42P01".into() })
        .is_violation());
    // Effect mismatch both ok.
    let exp = ApplyOutcome::Ok(LedgerEffect { affected: 1 });
    assert_eq!(
        reconcile(&exp, &EngineDmlResult::Ok { affected: 2 }),
        ReconcileVerdict::EffectMismatch { expected: 1, got: 2 }
    );
}

#[test]
fn check_table_multiset_both_ways() {
    let mut l = fresh_t1();
    l.apply(&LedgerOp::InsertValues { table: "t1".into(), rows: vec![row(1, None), row(2, None)] })
        .unwrap();
    let expected = l.table_rows("t1").unwrap();
    // green: order-insensitive
    assert!(check_table_multiset(&expected, &[row(2, None), row(1, None)]).is_ok());
    // red: extra / missing rows
    assert!(check_table_multiset(&expected, &[row(1, None)]).is_err());
    assert!(check_table_multiset(&expected, &[row(1, None), row(2, None), row(3, None)]).is_err());
}
