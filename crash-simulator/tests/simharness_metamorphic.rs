//! Metamorphic-oracle validation (TLP + NoREC) — the honesty bar:
//!
//! 1. Hand-built partition-reassembly cases: a whole/parts stack with known
//!    NULLs where the naive two-arm (p / NOT p) reassembly differs from the
//!    whole PROVES the `(p) IS NULL` arm is load-bearing, not decoration.
//! 2. Kleene 3VL pins for the predicate evaluator (the oracle side of every
//!    partition probe).
//! 3. A planted wrong-DUT: an executor that evaluates WHERE clauses under
//!    two-valued logic (UNKNOWN leaks through as TRUE, and `(p) IS NULL`
//!    never matches) — the classic engine NULL-semantics bug family. TLP and
//!    NoREC MUST fire on it across seeds, and MUST stay silent on the
//!    correct perfect-engine double over the same seeds (zero-FP control).
//! 4. The `--test-null-bug` DUT shim's rewrite discipline (SELECT-only).

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use simharness::oracle::check::{
    eval_check, Check, CheckOutcome, NoHooks, ResultStack, Row, StmtResult, Value,
};
use simharness::oracle::drive::{
    answer_probe, evaluate_instance, LedgerSimExecutor, PropertyOutcome, StepExecutor,
};
use simharness::oracle::ledger::{ApplyOutcome, Ledger};
use simharness::oracle::props::{self, ProfileView, PropertyId, SchemaView};
use simharness::oracle::pstep::{
    ArmCtl, CmpOp, PredSpec, SqlStep, TriSel, TxCtl,
};

fn ledger() -> Ledger {
    Ledger::new()
}

fn int_row(vals: &[Option<i64>]) -> Row {
    Row(vals.iter().map(|v| v.map(Value::Int).unwrap_or(Value::Null)).collect())
}

fn rows_slot(stack: &mut ResultStack, slot: u32, rows: Vec<Row>) {
    stack.put(slot, StmtResult::Rows { rows });
}

// ---------------------------------------------------------------------------
// 1. Hand-built reassembly: the IS NULL arm is load-bearing
// ---------------------------------------------------------------------------

/// Table: (k, v) = (1, 10), (2, 25), (3, NULL). Predicate p := v < 20.
/// 3VL partitions: T = {(1,10)}, F = {(2,25)}, N = {(3,NULL)}.
fn null_case_stack() -> ResultStack {
    let mut stack = ResultStack::new();
    let whole = vec![
        int_row(&[Some(1), Some(10)]),
        int_row(&[Some(2), Some(25)]),
        int_row(&[Some(3), None]),
    ];
    rows_slot(&mut stack, 0, whole);
    rows_slot(&mut stack, 1, vec![int_row(&[Some(1), Some(10)])]); // p
    rows_slot(&mut stack, 2, vec![int_row(&[Some(2), Some(25)])]); // NOT p
    rows_slot(&mut stack, 3, vec![int_row(&[Some(3), None])]); // p IS NULL
    stack
}

#[test]
fn where_tlp_three_arm_reassembly_passes() {
    let stack = null_case_stack();
    let check = Check::PartitionUnionEq { parts: vec![1, 2, 3], whole: 0 };
    assert_eq!(eval_check(&check, &stack, &ledger(), &NoHooks), CheckOutcome::Pass);
}

#[test]
fn where_tlp_naive_two_arm_reassembly_fails() {
    // The two-valued-logic partition (p / NOT p only) misses the NULL row —
    // whole != naive parts. This is the case that proves the IS NULL arm
    // matters; an oracle without it would silently under-partition.
    let stack = null_case_stack();
    let check = Check::PartitionUnionEq { parts: vec![1, 2], whole: 0 };
    match eval_check(&check, &stack, &ledger(), &NoHooks) {
        CheckOutcome::Fail(why) => {
            assert!(why.contains("reassemble"), "diagnostic names the law: {why}");
            assert!(why.contains("first diff row"), "diagnostic carries the row: {why}");
        }
        other => panic!("naive 2-arm reassembly must FAIL, got {other:?}"),
    }
}

#[test]
fn where_tlp_duplicated_null_row_fails() {
    // A wrong engine that returns the NULL row in BOTH the NOT-p and IS NULL
    // arms (2VL NOT): the reassembly has it twice, the whole once.
    let mut stack = null_case_stack();
    rows_slot(
        &mut stack,
        2,
        vec![int_row(&[Some(2), Some(25)]), int_row(&[Some(3), None])],
    );
    let check = Check::PartitionUnionEq { parts: vec![1, 2, 3], whole: 0 };
    assert!(matches!(
        eval_check(&check, &stack, &ledger(), &NoHooks),
        CheckOutcome::Fail(_)
    ));
}

#[test]
fn distinct_tlp_set_union_tolerates_overlap_but_not_loss() {
    let mut stack = ResultStack::new();
    // DISTINCT v over: whole {10, 25, NULL}; the value 10 appears in BOTH the
    // T and F partitions (different rows project the same value) — set union
    // must tolerate that.
    rows_slot(
        &mut stack,
        0,
        vec![int_row(&[Some(10)]), int_row(&[Some(25)]), Row(vec![Value::Null])],
    );
    rows_slot(&mut stack, 1, vec![int_row(&[Some(10)])]);
    rows_slot(&mut stack, 2, vec![int_row(&[Some(10)]), int_row(&[Some(25)])]);
    rows_slot(&mut stack, 3, vec![Row(vec![Value::Null])]);
    let check = Check::DistinctUnionEq { parts: vec![1, 2, 3], whole: 0 };
    assert_eq!(eval_check(&check, &stack, &ledger(), &NoHooks), CheckOutcome::Pass);

    // Losing the NULL-partition value breaks the set law.
    rows_slot(&mut stack, 3, vec![]);
    assert!(matches!(
        eval_check(&check, &stack, &ledger(), &NoHooks),
        CheckOutcome::Fail(_)
    ));
}

#[test]
fn minmax_tlp_extreme_and_all_null_cases() {
    let mut stack = ResultStack::new();
    rows_slot(&mut stack, 0, vec![int_row(&[Some(42)])]); // whole max
    rows_slot(&mut stack, 1, vec![int_row(&[Some(42)])]);
    rows_slot(&mut stack, 2, vec![int_row(&[Some(7)])]);
    rows_slot(&mut stack, 3, vec![Row(vec![Value::Null])]); // empty partition => NULL
    let max = Check::ScalarExtremeEq { parts: vec![1, 2, 3], whole: 0, max: true };
    assert_eq!(eval_check(&max, &stack, &ledger(), &NoHooks), CheckOutcome::Pass);
    // As MIN the same stack must fail (min is 7, whole says 42).
    let min = Check::ScalarExtremeEq { parts: vec![1, 2, 3], whole: 0, max: false };
    assert!(matches!(eval_check(&min, &stack, &ledger(), &NoHooks), CheckOutcome::Fail(_)));

    // All-NULL parts require a NULL whole.
    let mut stack = ResultStack::new();
    rows_slot(&mut stack, 0, vec![Row(vec![Value::Null])]);
    for s in 1..=3 {
        rows_slot(&mut stack, s, vec![Row(vec![Value::Null])]);
    }
    assert_eq!(eval_check(&max, &stack, &ledger(), &NoHooks), CheckOutcome::Pass);
    rows_slot(&mut stack, 0, vec![int_row(&[Some(1)])]);
    assert!(matches!(eval_check(&max, &stack, &ledger(), &NoHooks), CheckOutcome::Fail(_)));
}

#[test]
fn norec_true_count_law() {
    let mut stack = ResultStack::new();
    // Unoptimized projection: t, f, NULL, t => 2 trues ('t' text is the wire
    // form; Bool(true) accepted for typed sources).
    rows_slot(
        &mut stack,
        1,
        vec![
            Row(vec![Value::Text("t".into())]),
            Row(vec![Value::Text("f".into())]),
            Row(vec![Value::Null]),
            Row(vec![Value::Bool(true)]),
        ],
    );
    rows_slot(&mut stack, 0, vec![int_row(&[Some(1)]), int_row(&[Some(2)])]);
    let check = Check::NorecRowCountEq { optimized: 0, unoptimized: 1 };
    assert_eq!(eval_check(&check, &stack, &ledger(), &NoHooks), CheckOutcome::Pass);

    // Optimizer returning an extra row (the NoREC bug shape) must fail.
    rows_slot(
        &mut stack,
        0,
        vec![int_row(&[Some(1)]), int_row(&[Some(2)]), int_row(&[Some(3)])],
    );
    match eval_check(&check, &stack, &ledger(), &NoHooks) {
        CheckOutcome::Fail(why) => assert!(why.contains("NoREC"), "{why}"),
        other => panic!("NoREC mismatch must FAIL, got {other:?}"),
    }
}

#[test]
fn metamorphic_check_over_errored_slot_skips_never_fails() {
    // One TLP arm hit an error (e.g. statement_timeout): the law is not
    // evaluable — counted skip, never a minted violation (SQLancer's
    // ignore-erroring-queries rule).
    let mut stack = null_case_stack();
    stack.put(2, StmtResult::Error { sqlstate: "57014".into() });
    let check = Check::PartitionUnionEq { parts: vec![1, 2, 3], whole: 0 };
    assert!(matches!(
        eval_check(&check, &stack, &ledger(), &NoHooks),
        CheckOutcome::SkipInapplicable(_)
    ));
}

// ---------------------------------------------------------------------------
// 2. Kleene 3VL pins for the predicate evaluator
// ---------------------------------------------------------------------------

#[test]
fn kleene_three_valued_logic_pins() {
    let v_lt_20 = PredSpec::ColCmp { col: 0, op: CmpOp::Lt, lit: Value::Int(20) };
    let null_row = Row(vec![Value::Null]);
    let ten = Row(vec![Value::Int(10)]);

    // Comparison over NULL is UNKNOWN; NOT UNKNOWN is UNKNOWN.
    assert_eq!(v_lt_20.eval3(&null_row), None);
    assert_eq!(PredSpec::Not(Box::new(v_lt_20.clone())).eval3(&null_row), None);

    // UNKNOWN AND FALSE = FALSE; UNKNOWN OR TRUE = TRUE (Kleene absorption).
    let false_p = PredSpec::ColCmp { col: 0, op: CmpOp::Eq, lit: Value::Int(-1) };
    let true_p = PredSpec::ColCmp { col: 0, op: CmpOp::Eq, lit: Value::Int(10) };
    let unknown_p = PredSpec::ColCmp { col: 1, op: CmpOp::Lt, lit: Value::Int(5) };
    let row = Row(vec![Value::Int(10), Value::Null]);
    assert_eq!(
        PredSpec::And(Box::new(unknown_p.clone()), Box::new(false_p)).eval3(&row),
        Some(false)
    );
    assert_eq!(
        PredSpec::Or(Box::new(unknown_p.clone()), Box::new(true_p)).eval3(&row),
        Some(true)
    );
    // UNKNOWN AND TRUE = UNKNOWN.
    let true_p2 = PredSpec::ColCmp { col: 0, op: CmpOp::Eq, lit: Value::Int(10) };
    assert_eq!(PredSpec::And(Box::new(unknown_p), Box::new(true_p2)).eval3(&row), None);

    // IS NULL is two-valued — never UNKNOWN.
    assert_eq!(PredSpec::ColIsNull { col: 0 }.eval3(&null_row), Some(true));
    assert_eq!(PredSpec::ColIsNull { col: 0 }.eval3(&ten), Some(false));

    // Text compare, C-collation byte order.
    let s_lt = PredSpec::ColCmp { col: 0, op: CmpOp::Lt, lit: Value::Text("s20".into()) };
    assert_eq!(s_lt.eval3(&Row(vec![Value::Text("s05".into())])), Some(true));
    assert_eq!(s_lt.eval3(&Row(vec![Value::Text("s30".into())])), Some(false));
    assert_eq!(s_lt.eval3(&Row(vec![Value::Null])), None);

    // Every row falls in exactly one partition (the TLP invariant itself).
    for row in [&null_row, &ten] {
        let hits = [TriSel::True, TriSel::False, TriSel::Null]
            .iter()
            .filter(|s| v_lt_20.in_partition(row, **s))
            .count();
        assert_eq!(hits, 1, "row {row:?} must fall in exactly one partition");
    }
}

// ---------------------------------------------------------------------------
// 3. Planted wrong-DUT: 2VL WHERE evaluation (UNKNOWN leaks as TRUE,
//    predicate-level IS NULL never matches)
// ---------------------------------------------------------------------------

struct NullBug2vlExecutor {
    ledger: Ledger,
}

impl NullBug2vlExecutor {
    fn new() -> Self {
        NullBug2vlExecutor { ledger: Ledger::new() }
    }
}

impl StepExecutor for NullBug2vlExecutor {
    fn exec_sql(&mut self, step: &SqlStep) -> StmtResult {
        if let Some(op) = &step.ledger_op {
            return match self.ledger.apply(op) {
                Ok(ApplyOutcome::Ok(eff)) => StmtResult::Command { affected: eff.affected },
                Ok(ApplyOutcome::Err(e)) => StmtResult::Error { sqlstate: e.sqlstate },
                Err(m) => panic!("wrong-DUT misuse: {}", m.0),
            };
        }
        if let Some(spec) = &step.probe {
            // The planted bug lives in WHERE-clause evaluation ONLY; the
            // projection side (PredProjection, NoRecSum) stays correct —
            // exactly the optimized/unoptimized asymmetry NoREC targets.
            return answer_probe(&self.ledger, spec, &|pred, row, sel| {
                let p = pred.eval3(row);
                match sel {
                    TriSel::True => p != Some(false), // UNKNOWN => row kept (bug)
                    TriSel::False => p != Some(true), // NOT p under 2VL (bug)
                    TriSel::Null => false,            // "(p) IS NULL" never matches (bug)
                }
            });
        }
        StmtResult::Command { affected: 0 }
    }

    fn exec_tx(&mut self, ctl: &TxCtl) {
        match ctl {
            TxCtl::Begin(iso) => self.ledger.begin(*iso),
            TxCtl::Commit => self.ledger.commit(),
            TxCtl::Rollback => self.ledger.rollback(),
            TxCtl::Savepoint(n) => self.ledger.savepoint(n),
            TxCtl::RollbackTo(n) => {
                self.ledger.rollback_to(n).expect("wrong-DUT savepoint");
            }
        }
    }

    fn exec_arm(&mut self, _ctl: &ArmCtl) {}
}

/// Run `id` over many seeds against both the wrong-DUT and the correct
/// double; return (violations on wrong-DUT, violations on correct).
fn teeth_run(id: PropertyId, seeds: std::ops::Range<u64>) -> (u32, u32) {
    let (mut bug_fires, mut clean_fires) = (0u32, 0u32);
    for seed in seeds {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let inst = props::generate(id, &mut rng, &SchemaView::default(), &ProfileView::default());

        let mut exec = NullBug2vlExecutor::new();
        let mut oracle_ledger = Ledger::new();
        let report = evaluate_instance(&inst, &mut exec, &mut oracle_ledger, &NoHooks);
        if report.outcome == PropertyOutcome::Violation {
            bug_fires += 1;
        }

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let inst = props::generate(id, &mut rng, &SchemaView::default(), &ProfileView::default());
        let mut exec = LedgerSimExecutor::new();
        let mut oracle_ledger = Ledger::new();
        let report = evaluate_instance(&inst, &mut exec, &mut oracle_ledger, &NoHooks);
        if report.outcome == PropertyOutcome::Violation {
            clean_fires += 1;
        }
    }
    (bug_fires, clean_fires)
}

#[test]
fn tlp_fires_on_null_bug_dut_and_not_on_correct_dut() {
    let (bug, clean) = teeth_run(PropertyId::L1Tlp, 0..300);
    assert_eq!(clean, 0, "TLP must be silent on a correct engine (zero-FP control)");
    // Not every seed has NULL-predicate mass (and MIN/MAX-TLP is structurally
    // blind to this particular bug — the extreme survives partition leaks);
    // across 300 seeds a large fraction must fire.
    assert!(bug >= 60, "TLP teeth: expected >= 60 violations across 300 seeds, got {bug}");
}

#[test]
fn norec_fires_on_null_bug_dut_and_not_on_correct_dut() {
    let (bug, clean) = teeth_run(PropertyId::L2NoRec, 0..300);
    assert_eq!(clean, 0, "NoREC must be silent on a correct engine (zero-FP control)");
    assert!(bug >= 60, "NoREC teeth: expected >= 60 violations across 300 seeds, got {bug}");
}

// ---------------------------------------------------------------------------
// 4. The --test-null-bug DUT shim rewrite discipline
// ---------------------------------------------------------------------------

struct RecordingSession {
    calls: Vec<String>,
}

impl simharness::runner::driver::Session for RecordingSession {
    fn engine(&self) -> &str {
        "mock"
    }
    fn execute(&mut self, sql: &str) -> simharness::runner::driver::ExecOutcome {
        self.calls.push(sql.to_string());
        simharness::runner::driver::ExecOutcome::Command { tag: "OK".into() }
    }
    fn reconnect(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn null_bug_shim_rewrites_where_clauses_of_selects_only() {
    use simharness::runner::driver::{NullBugShim, Session};
    let mut shim = NullBugShim { inner: RecordingSession { calls: vec![] } };
    shim.execute("SELECT k FROM t WHERE ((v % 3) = 1) IS NULL ORDER BY k;");
    shim.execute("SELECT k FROM t WHERE v IS NOT NULL;");
    shim.execute("SELECT (s IS NULL) FROM t;");
    shim.execute("INSERT INTO t (k, v) VALUES (1, NULL);");
    let calls = &shim.inner.calls;
    assert_eq!(
        calls[0],
        "SELECT k FROM t WHERE ((v % 3) = 1) IS NULL AND false ORDER BY k;",
        "IS NULL inside a WHERE is doctored"
    );
    assert_eq!(calls[1], "SELECT k FROM t WHERE v IS NOT NULL;", "IS NOT NULL untouched");
    assert_eq!(
        calls[2],
        "SELECT (s IS NULL) FROM t;",
        "projection-side IS NULL untouched (the NoREC filter/projection asymmetry)"
    );
    assert_eq!(calls[3], "INSERT INTO t (k, v) VALUES (1, NULL);", "DML untouched");
}
