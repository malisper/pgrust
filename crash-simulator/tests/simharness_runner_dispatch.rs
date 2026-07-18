//! G-R1: mark-honoring dispatch — READ compared, MUTATION exactly once per
//! engine (double execution refused), PASSTHROUGH uncompared; reserved fault
//! tags refuse at execute; mutation-outcome divergence HALTs.

#[path = "testutil/mod.rs"]
mod testutil;
use testutil::*;

use simharness::runner::driver::*;
use simharness::runner::planface::*;

fn sql(text: &str, mark: Mark) -> Sql {
    Sql { text: text.to_string(), mark, meta: SqlMeta::default() }
}

#[test]
fn mutation_double_execution_refused() {
    let mut dut = MockSession::ok("dut");
    let mut disp = Dispatcher::new(&mut dut, None);
    let s = sql("INSERT INTO t VALUES (1)", Mark::Mutation);
    assert!(disp.dispatch(7, &s).is_ok());
    let err = disp.dispatch(7, &s).unwrap_err();
    assert!(err.contains("refusal"), "expected refusal, got: {}", err);
    // A READ at the same index is not refused (only MUTATIONs are guarded).
    let r = sql("SELECT 1", Mark::Read);
    assert!(disp.dispatch(8, &r).is_ok());
    assert!(disp.dispatch(8, &r).is_ok());
}

#[test]
fn read_compared_mutation_once_passthrough_uncompared() {
    // Full plan through execute_plan with a diff leg; the mock records calls.
    let plan = Plan {
        header: header(11),
        steps: vec![
            Step::Ddl(sql("CREATE TABLE t (k int PRIMARY KEY)", Mark::Mutation)),
            Step::Query(sql("SELECT k FROM t ORDER BY k LIMIT 3", Mark::Read)),
            Step::Query(sql("SHOW server_version", Mark::Passthrough)),
        ],
    };
    let mut dut = MockSession::ok("dut");
    let mut cpg = MockSession::ok("cpg");
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert!(report.failure.is_none(), "{:?}", report.failure);
    // Both legs saw all three statements exactly once (state lockstep)...
    assert_eq!(dut.calls.len(), 3);
    assert_eq!(cpg.calls.len(), 3);
    // ...and each engine executed the MUTATION exactly once.
    let muts = |c: &Vec<String>| c.iter().filter(|s| s.starts_with("CREATE TABLE")).count();
    assert_eq!(muts(&dut.calls), 1);
    assert_eq!(muts(&cpg.calls), 1);
}

#[test]
fn passthrough_divergence_not_compared() {
    // PASSTHROUGH rows differ across engines: must NOT produce a finding.
    let plan = Plan {
        header: header(12),
        steps: vec![Step::Query(sql("SHOW server_version", Mark::Passthrough))],
    };
    let mut dut = MockSession::with_rows("dut", vec![vec![Some("pgrust".into())]]);
    let mut cpg = MockSession::with_rows("cpg", vec![vec![Some("18.3".into())]]);
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert!(report.failure.is_none());
    assert_eq!(report.class_counts.get("wrong-results"), None);
}

#[test]
fn read_divergence_is_wrong_results_p1() {
    let plan = Plan {
        header: header(13),
        steps: vec![Step::Query(sql("SELECT k FROM t ORDER BY k", Mark::Read))],
    };
    let mut dut = MockSession::with_rows("dut", vec![vec![Some("1".into())]]);
    let mut cpg = MockSession::with_rows("cpg", vec![vec![Some("2".into())]]);
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("expected wrong-results failure");
    assert_eq!(f.class, "wrong-results");
    assert_eq!(f.sev, "P1");
}

#[test]
fn order_underdetermined_read_sort_normalizes() {
    let s = Sql {
        text: "SELECT k FROM t".into(),
        mark: Mark::Read,
        meta: SqlMeta { order_underdetermined: true, float_lenient: false },
    };
    let plan = Plan { header: header(14), steps: vec![Step::Query(s)] };
    let mut dut = MockSession::with_rows(
        "dut",
        vec![vec![Some("1".into())], vec![Some("2".into())]],
    );
    let mut cpg = MockSession::with_rows(
        "cpg",
        vec![vec![Some("2".into())], vec![Some("1".into())]],
    );
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert!(report.failure.is_none(), "sort-normalized multiset must match");
}

#[test]
fn mutation_outcome_divergence_halts() {
    let plan = Plan {
        header: header(15),
        steps: vec![
            Step::Dml(sql("INSERT INTO t VALUES (1)", Mark::Mutation)),
            Step::Query(sql("SELECT 1", Mark::Read)), // must NOT run after HALT
        ],
    };
    let mut dut = MockSession::ok("dut");
    let mut cpg = MockSession::erroring("cpg", "23505", "duplicate key");
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert_eq!(report.halted_at, Some(0), "mutation divergence must HALT at the step");
    assert_eq!(dut.calls.len(), 1, "no statement may run after HALT");
    let f = report.failure.expect("failure recorded");
    assert_eq!(f.class, "c-err-rust-ok");
}

#[test]
fn reserved_fault_tags_refuse_at_execute() {
    for fault in [
        FaultPoint::Crash("pre-commit".into()),
        FaultPoint::TornWrite,
        FaultPoint::Env("enospc".into()),
    ] {
        let plan = Plan { header: header(16), steps: vec![Step::Fault(fault)] };
        let mut dut = MockSession::ok("dut");
        let report = execute_plan(
            &plan,
            &mut dut,
            None,
            &BasicCheckEval,
            &BasicDiffClassifier,
            &ExecOptions::default(),
        );
        let f = report.failure.expect("reserved fault must refuse");
        assert_eq!(f.class, "fault-reserved-refused");
        assert_eq!(dut.calls.len(), 0, "no SQL may run for a reserved fault");
    }
}

#[test]
fn disconnect_fault_reconnects_both_legs() {
    let plan = Plan {
        header: header(17),
        steps: vec![
            Step::Fault(FaultPoint::Disconnect),
            Step::Query(sql("SELECT 1", Mark::Read)),
        ],
    };
    let mut dut = MockSession::ok("dut");
    let mut cpg = MockSession::ok("cpg");
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert!(report.failure.is_none());
    assert_eq!(dut.reconnects, 1);
    assert_eq!(cpg.reconnects, 1);
}

#[test]
fn reconnect_server_without_restart_cmd_is_counted_skip() {
    let plan = Plan { header: header(18), steps: vec![Step::Fault(FaultPoint::ReconnectServer)] };
    let mut dut = MockSession::ok("dut");
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert!(report.failure.is_none());
    assert_eq!(report.class_counts.get("fault-skipped-no-restart-cmd"), Some(&1));
}

#[test]
fn assertion_failure_is_property_violation_p1() {
    let plan = Plan {
        header: header(19),
        steps: vec![
            Step::Query(sql("SELECT k FROM t WHERE k = 5", Mark::Read)),
            Step::Assertion("{\"op\":\"rowcount-eq\",\"value\":3}".into()),
        ],
    };
    let mut dut = MockSession::with_rows("dut", vec![vec![Some("5".into())]]);
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("assert must fail");
    assert_eq!(f.class, "property-violation");
    assert_eq!(f.sev, "P1");
}

#[test]
fn dut_connection_lost_is_rust_crash_p1() {
    let plan = Plan {
        header: header(20),
        steps: vec![Step::Query(sql("SELECT 1", Mark::Read))],
    };
    let mut dut = MockSession::crashing("dut");
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("crash failure");
    assert_eq!(f.class, "rust-crash");
    assert_eq!(f.sev, "P1");
}
