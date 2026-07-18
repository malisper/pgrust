//! H4 panic escalation gate: a panicking-but-contained backend (XX000 +
//! panic-payload message, connection survives) must FAIL the campaign
//! verdict as `panic-signature` P1 — never PASS as rust-err-c-ok P2
//! (H3 finding 1: detection-in-census != detection-in-verdict).

#[path = "testutil/mod.rs"]
mod testutil;
use testutil::*;

use simharness::runner::driver::*;
use simharness::runner::planface::*;
use simharness::runner::verdict::Census;

fn sql(text: &str, mark: Mark) -> Sql {
    Sql { text: text.to_string(), mark, meta: SqlMeta::default() }
}

fn one_query_plan(text: &str) -> Plan {
    Plan { header: header(42), steps: vec![Step::Query(sql(text, Mark::Read))] }
}

fn verdict_line(report: &RunReport) -> String {
    let mut census = Census::default();
    census.merge(&report.class_counts);
    let mut out = Vec::new();
    census.emit(&mut out).unwrap();
    String::from_utf8(out).unwrap().lines().last().unwrap().to_string()
}

#[test]
fn synthetic_panic_flips_verdict_to_fail() {
    // The p6 witness shape: staged-parity assert contained by the backend,
    // surfaced to the client as SQLSTATE XX000 "assertion failed: ...".
    let plan = one_query_plan("SELECT c1, count(*) FROM t1 GROUP BY c1;");
    let mut dut = MockSession::erroring("dut", "XX000", "assertion failed: staged kernel parity");
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert_eq!(report.class_counts.get("panic-signature"), Some(&1));
    let f = report.failure.as_ref().expect("panic escalation must set the failure");
    assert_eq!(f.class, "panic-signature");
    assert_eq!(f.sev, "P1");
    assert_eq!(f.signature.sqlstate, "XX000");
    let v = verdict_line(&report);
    assert!(
        v.starts_with("SIMHARNESS-VERDICT|FAIL:") && v.contains("panic-signature"),
        "expected FAIL:panic-signature verdict, got {v}"
    );
}

#[test]
fn panic_escalation_rides_alongside_diffc_p2_class() {
    // With a healthy C leg the pinned ladder still classifies rust-err-c-ok
    // (P2, parity gate untouched); the escalation ADDS panic-signature P1.
    let plan = one_query_plan("SELECT c1, count(*) FROM t1 GROUP BY c1;");
    let mut dut = MockSession::erroring("dut", "XX000", "assertion failed: staged kernel parity");
    let mut cpg = MockSession::with_rows("cpg", vec![vec![Some("1".into()), Some("2".into())]]);
    let report = execute_plan(
        &plan,
        &mut dut,
        Some(&mut cpg),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert_eq!(report.class_counts.get("panic-signature"), Some(&1));
    let v = verdict_line(&report);
    assert!(v.contains("panic-signature"), "P1 escalation missing from verdict: {v}");
}

#[test]
fn ordinary_errors_do_not_escalate() {
    for (state, msg) in [
        ("XX000", "not yet ported: brin"),   // coverage stays coverage
        ("XX000", "cache lookup failed for type 42"), // legit internal elog
        ("23505", "duplicate key value violates unique constraint"),
        ("22012", "division by zero"),
    ] {
        let plan = one_query_plan("SELECT count(*) FROM t1;");
        let mut dut = MockSession::erroring("dut", state, msg);
        let report = execute_plan(
            &plan,
            &mut dut,
            None,
            &BasicCheckEval,
            &BasicDiffClassifier,
            &ExecOptions::default(),
        );
        assert_eq!(
            report.class_counts.get("panic-signature"),
            None,
            "{state} '{msg}' must not escalate"
        );
        let v = verdict_line(&report);
        assert_eq!(v, "SIMHARNESS-VERDICT|PASS", "{state} '{msg}': {v}");
    }
}

#[test]
fn ctl_step_panic_escalates() {
    // COMMIT is the classic engine-bug site; TX steps escalate too.
    let plan = Plan {
        header: header(7),
        steps: vec![Step::Tx(TxCtl::Commit)],
    };
    let mut dut = MockSession::erroring("dut", "XX000", "panicked at 'wal flush'");
    let report = execute_plan(
        &plan,
        &mut dut,
        None,
        &BasicCheckEval,
        &BasicDiffClassifier,
        &ExecOptions::default(),
    );
    assert_eq!(report.class_counts.get("panic-signature"), Some(&1));
    assert_eq!(report.failure.as_ref().map(|f| f.class.as_str()), Some("panic-signature"));
}
