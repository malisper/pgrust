//! G-R1: shrinker v1 — truncate-after-failure, non-touching property drop,
//! faults/tx/arm kept unconditionally, same-signature keep discipline.

use simharness::runner::driver::Signature;
use simharness::runner::planface::*;
use simharness::runner::shrink::{shrink, shrink_candidate};

fn header() -> PlanHeader {
    PlanHeader {
        seed: 1,
        profile: "test".into(),
        profile_sha256: "0".repeat(64),
        generator: "test".into(),
    }
}

fn dml(text: &str) -> Step {
    Step::Dml(Sql { text: text.into(), mark: Mark::Mutation, meta: SqlMeta::default() })
}

fn query(text: &str) -> Step {
    Step::Query(Sql { text: text.into(), mark: Mark::Read, meta: SqlMeta::default() })
}

/// Plan: property A on table t1 (irrelevant), fault inside A, property B on
/// table t2 (the failing one), trailing property C (after failure).
fn plan() -> Plan {
    Plan {
        header: header(),
        steps: vec![
            Step::BeginProperty { name: "A".into(), seq: 1, tables: vec!["t1".into()] },
            dml("INSERT INTO t1 VALUES (1)"),
            Step::Fault(FaultPoint::Disconnect),
            Step::Tx(TxCtl::Begin(IsoLevel::ReadCommitted)),
            Step::Arm(ArmCtl::SetGuc("work_mem".into(), "64kB".into())),
            Step::Tx(TxCtl::Commit),
            Step::EndProperty { seq: 1 },
            Step::BeginProperty { name: "B".into(), seq: 2, tables: vec!["t2".into()] },
            dml("INSERT INTO t2 VALUES (2)"),
            query("SELECT v FROM t2 WHERE k = 2"),
            Step::Assertion("{\"op\":\"rowcount-eq\",\"value\":1}".into()), // idx 10 = failure
            Step::EndProperty { seq: 2 },
            Step::BeginProperty { name: "C".into(), seq: 3, tables: vec!["t2".into()] },
            dml("INSERT INTO t2 VALUES (3)"),
            Step::EndProperty { seq: 3 },
        ],
    }
}

#[test]
fn candidate_truncates_and_drops_non_touching() {
    let p = plan();
    let cand = shrink_candidate(&p, 10).expect("candidate");
    // Truncated: nothing after idx 10 survives (C is gone entirely).
    assert!(!cand
        .steps
        .iter()
        .any(|s| matches!(s, Step::BeginProperty { name, .. } if name == "C")));
    assert!(!cand.steps.iter().any(
        |s| matches!(s, Step::Dml(sql) if sql.text.contains("VALUES (3)"))
    ));
    // Property A touches only t1 (disjoint from failing t2): its SQL dropped...
    assert!(!cand.steps.iter().any(
        |s| matches!(s, Step::Dml(sql) if sql.text.contains("t1"))
    ));
    // ...but faults/tx/arm are kept unconditionally.
    assert!(cand.steps.iter().any(|s| matches!(s, Step::Fault(FaultPoint::Disconnect))));
    assert!(cand.steps.iter().any(|s| matches!(s, Step::Tx(TxCtl::Begin(_)))));
    assert!(cand.steps.iter().any(|s| matches!(s, Step::Arm(ArmCtl::SetGuc(_, _)))));
    // The failing span survives whole.
    assert!(cand.steps.iter().any(
        |s| matches!(s, Step::Dml(sql) if sql.text.contains("t2 VALUES (2)"))
    ));
    assert!(cand.steps.iter().any(|s| matches!(s, Step::Assertion(_))));
    assert!(cand.steps.len() < p.steps.len());
}

#[test]
fn shrink_keeps_only_same_signature() {
    let p = plan();
    let sig = Signature {
        class: "property-violation".into(),
        sqlstate: "".into(),
        site: "assert".into(),
    };

    // Re-run reproduces the same signature: candidate kept.
    let mut same = |_: &Plan| Some(sig.clone());
    let kept = shrink(&p, 10, &sig, &mut same);
    assert!(kept.is_some(), "same-signature rerun must keep the shrunk plan");

    // Re-run yields a DIFFERENT signature: candidate discarded.
    let other = Signature { class: "rust-crash".into(), sqlstate: "".into(), site: "x".into() };
    let mut diff = |_: &Plan| Some(other.clone());
    assert!(shrink(&p, 10, &sig, &mut diff).is_none());

    // Re-run yields no failure at all: candidate discarded.
    let mut none = |_: &Plan| None;
    assert!(shrink(&p, 10, &sig, &mut none).is_none());
}

#[test]
fn unknown_dependencies_keep_everything_before_failure() {
    // Failing step outside any property span: dependency set unknown =>
    // fail-safe direction, only truncation happens.
    let p = Plan {
        header: header(),
        steps: vec![
            Step::BeginProperty { name: "A".into(), seq: 1, tables: vec!["t1".into()] },
            dml("INSERT INTO t1 VALUES (1)"),
            Step::EndProperty { seq: 1 },
            query("SELECT 1"), // idx 3 = failing, no span
            query("SELECT 2"),
        ],
    };
    let cand = shrink_candidate(&p, 3).unwrap();
    assert_eq!(cand.steps.len(), 4, "truncate only; nothing dropped when deps unknown");
}
