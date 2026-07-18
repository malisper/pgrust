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
    // The one-leg error triggers the symmetric recovery ROLLBACK (resync
    // law) on BOTH legs; no PLAN statement may run after HALT.
    assert_eq!(dut.calls, vec!["INSERT INTO t VALUES (1)".to_string(), "ROLLBACK".to_string()]);
    assert_eq!(cpg.calls, vec!["INSERT INTO t VALUES (1)".to_string(), "ROLLBACK".to_string()]);
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
fn one_leg_read_error_rolls_back_both_legs() {
    // Review finding: recovery ROLLBACK on only the erroring leg forks
    // diff-c state (the ok leg's open tx keeps running). Both legs must be
    // resynchronized after a one-leg statement error.
    let plan = Plan {
        header: header(30),
        steps: vec![Step::Query(sql("SELECT k FROM t", Mark::Read))],
    };
    let mut dut = MockSession::erroring("dut", "42P01", "relation does not exist");
    let mut cpg = MockSession::with_rows("cpg", vec![vec![Some("1".into())]]);
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("one-leg read error is a P2 finding");
    assert_eq!(f.class, "rust-err-c-ok");
    assert_eq!(dut.calls, vec!["SELECT k FROM t".to_string(), "ROLLBACK".to_string()]);
    assert_eq!(
        cpg.calls,
        vec!["SELECT k FROM t".to_string(), "ROLLBACK".to_string()],
        "the OK leg must be rolled back too (resync law)"
    );
}

#[test]
fn tx_step_connection_lost_is_rust_crash_p1() {
    // Review finding: a DUT that dies on COMMIT (the classic engine-bug
    // site) must be rust-crash P1, never a silent "ok" PASS.
    let plan = Plan { header: header(31), steps: vec![Step::Tx(TxCtl::Commit)] };
    let mut dut = MockSession::crashing("dut");
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("dead DUT on COMMIT must fail the plan");
    assert_eq!(f.class, "rust-crash");
    assert_eq!(f.sev, "P1");
    assert_eq!(report.class_counts.get("ok"), None, "no ok may be counted for a dead COMMIT");
}

#[test]
fn arm_step_connection_lost_is_rust_crash_p1() {
    let plan = Plan { header: header(32), steps: vec![Step::Arm(ArmCtl::ResetAll)] };
    let mut dut = MockSession::crashing("dut");
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("dead DUT on ARM must fail the plan");
    assert_eq!(f.class, "rust-crash");
    assert_eq!(f.sev, "P1");
}

#[test]
fn cleg_connection_lost_on_mutation_is_c_crash_p1() {
    // Review finding: a dead C server was classified through the
    // is_error()/SQLSTATE ladder as a P2 — pinned vocabulary says dead
    // server = c-crash P1, on every mark.
    let plan = Plan {
        header: header(33),
        steps: vec![
            Step::Dml(sql("INSERT INTO t VALUES (1)", Mark::Mutation)),
            Step::Query(sql("SELECT 1", Mark::Read)), // must NOT run after HALT
        ],
    };
    let mut dut = MockSession::ok("dut");
    let mut cpg = MockSession::crashing("cpg");
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("dead C leg must fail the plan");
    assert_eq!(f.class, "c-crash");
    assert_eq!(f.sev, "P1");
    assert_eq!(report.halted_at, Some(0));
    assert!(!dut.calls.iter().any(|c| c.starts_with("SELECT")), "no statement after HALT");
}

#[test]
fn cleg_connection_lost_on_tx_is_c_crash_p1() {
    let plan = Plan { header: header(34), steps: vec![Step::Tx(TxCtl::Commit)] };
    let mut dut = MockSession::ok("dut");
    let mut cpg = MockSession::crashing("cpg");
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    let f = report.failure.expect("dead C leg on COMMIT must fail the plan");
    assert_eq!(f.class, "c-crash");
    assert_eq!(f.sev, "P1");
    assert_eq!(report.halted_at, Some(0));
}

#[test]
fn reconnect_server_restarts_and_resyncs_both_legs() {
    // Review finding: ReconnectServer reconnected ONLY the DUT, leaving the
    // C leg's tx/GUC session state behind — a state fork. After a real
    // restart both legs must land in identical fresh-session state.
    let plan = Plan {
        header: header(35),
        steps: vec![
            Step::Fault(FaultPoint::ReconnectServer),
            Step::Query(sql("SELECT 1", Mark::Read)),
        ],
    };
    let mut dut = MockSession::dead_until_reconnect("dut"); // old conn dead = real restart
    let mut cpg = MockSession::ok("cpg");
    let opts = ExecOptions { restart_cmd: Some("true".into()), stop_on_failure: true, post_reset_sql: vec![], ..ExecOptions::default() };
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &opts,
    );
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert_eq!(dut.reconnects, 1);
    assert_eq!(cpg.reconnects, 1, "the C leg must be resynchronized (reconnected) too");
}

#[test]
fn reconnect_server_noop_restart_is_refused_p1() {
    // Review finding: the fleet job shipped --restart-cmd "true" — a no-op
    // that silently reset the DUT session mid-plan. The runner now probes
    // the old connection and refuses restarts that did not happen.
    let plan = Plan {
        header: header(36),
        steps: vec![
            Step::Fault(FaultPoint::ReconnectServer),
            Step::Query(sql("SELECT 1", Mark::Read)), // must NOT run
        ],
    };
    let mut dut = MockSession::ok("dut"); // old conn still answers = no-op restart
    let mut cpg = MockSession::ok("cpg");
    let opts = ExecOptions { restart_cmd: Some("true".into()), stop_on_failure: true, post_reset_sql: vec![], ..ExecOptions::default() };
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &opts,
    );
    let f = report.failure.expect("no-op restart must be refused");
    assert_eq!(f.class, "fault-restart-noop");
    assert_eq!(f.sev, "P1");
    assert_eq!(dut.reconnects, 0, "must not reset the DUT session on a fake restart");
    assert_eq!(cpg.reconnects, 0);
    assert!(
        !cpg.calls.iter().any(|c| c.starts_with("SELECT 1")),
        "no plan statement may run after the refusal"
    );
}

#[test]
fn normalize_site_truncates_on_char_boundary() {
    // 80-byte cap must not panic when the boundary falls inside a multibyte
    // char (real WS-GEN output may contain multibyte identifiers/literals).
    let stmt = format!("SELECT {}", "ü".repeat(60)); // "SELECT " = 7 bytes, boundary at 80 splits a 'ü'
    let site = normalize_site(&stmt);
    assert!(site.len() <= 80);
    assert!(site.starts_with("SELECT ü"));
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

// ---------------------------------------------------------------- H2 seam

/// H2 FaultDriver seam: with a driver that ARMS (mock installer), a reserved
/// fault tag no longer refuses — the run continues, the census records
/// `fault-armed`, and the plan's later steps execute.
#[test]
fn reserved_fault_tag_arms_via_fault_driver_and_run_continues() {
    use simharness::runner::faultdriver::{
        map_fault_step, FaultDriver, FaultDriverError, FaultPlanSpec,
    };
    use std::cell::RefCell;
    use std::rc::Rc;

    struct ArmingDriver {
        installed: Rc<RefCell<Vec<FaultPlanSpec>>>,
    }
    impl FaultDriver for ArmingDriver {
        fn name(&self) -> &'static str {
            "mock-installer"
        }
        fn map(
            &self,
            fault: &FaultPoint,
            plan_seed: u64,
            step_idx: usize,
        ) -> Result<FaultPlanSpec, FaultDriverError> {
            map_fault_step(fault, plan_seed, step_idx)
        }
        fn arm(&self, spec: &FaultPlanSpec) -> Result<(), FaultDriverError> {
            self.installed.borrow_mut().push(spec.clone());
            Ok(())
        }
    }

    let installed = Rc::new(RefCell::new(Vec::new()));
    let plan = Plan {
        header: header(37),
        steps: vec![
            Step::Fault(FaultPoint::TornWrite),
            Step::Query(sql("SELECT 1", Mark::Read)), // MUST still run
        ],
    };
    let mut dut = MockSession::ok("dut");
    let opts = ExecOptions {
        fault_driver: Box::new(ArmingDriver { installed: Rc::clone(&installed) }),
        ..ExecOptions::default()
    };
    let report =
        execute_plan(&plan, &mut dut, None, &BasicCheckEval, &BasicDiffClassifier, &opts);
    assert!(report.failure.is_none(), "{:?}", report.failure);
    assert_eq!(report.class_counts.get("fault-armed"), Some(&1));
    assert_eq!(dut.calls, vec!["SELECT 1".to_string()], "later steps run after arming");
    let specs = installed.borrow();
    assert_eq!(specs.len(), 1, "exactly one fault plan installed");
    // The installed spec is the deterministic mapping for (plan seed, step 0).
    assert_eq!(
        specs[0],
        map_fault_step(&FaultPoint::TornWrite, plan.header.seed, 0).unwrap()
    );
}
