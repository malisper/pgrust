//! Reduced G-O3/G-O4 (standalone, no server): 1000 seeded plans against the
//! ledger-backed perfect-engine double.
//!
//! - G-O3 form: zero violations (perfect engine => any violation is an
//!   oracle bug = the FP-budget posture at the oracle level), every
//!   unconditional property fires >= 1 time (anti-vacuity floor), F7/F8
//!   skip-with-count under NoHooks.
//! - G-O4 form: the whole batch run twice => byte-identical verdict stream.
//!
//! The real-engine forms of G-O3/G-O4 need WS-GEN's generator + WS-RUNNER's
//! session driver and run on harness/h1-integration (worklog hand-back
//! ledger tracks them as open).

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use simharness::oracle::check::NoHooks;
use simharness::oracle::drive::{
    evaluate_instance, report_line, LedgerSimExecutor, OutcomeCounts, PropertyOutcome,
};
use simharness::oracle::ledger::Ledger;
use simharness::oracle::props::{self, ProfileView, SchemaView};

fn run_batch(n_seeds: u64) -> (String, OutcomeCounts) {
    let schema = SchemaView::default();
    let profile = ProfileView::default();
    let all = props::v1_set();
    let mut stream = String::new();
    let mut counts = OutcomeCounts::default();

    for seed in 0..n_seeds {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        // 3-8 property instances per plan, weighted-uniform over the v1 set.
        let n_props = rng.gen_range(3..=8);
        let mut exec = LedgerSimExecutor::new();
        let mut ledger = Ledger::new();
        for _ in 0..n_props {
            let id = all[rng.gen_range(0..all.len())];
            let inst = props::generate(id, &mut rng, &schema, &profile);
            let report = evaluate_instance(&inst, &mut exec, &mut ledger, &NoHooks);
            counts.record(&report);
            stream.push_str(&report_line(seed, &report));
            stream.push('\n');
        }
    }
    (stream, counts)
}

#[test]
fn smoke_1k_standalone_and_determinism_x2() {
    let (stream1, counts1) = run_batch(1000);
    let (stream2, counts2) = run_batch(1000);

    // G-O4 (reduced): byte-identical verdict streams across two runs.
    assert_eq!(stream1, stream2, "verdict stream must be byte-identical x2");
    assert_eq!(counts1, counts2);

    // G-O3 (reduced): zero violations against a correct engine.
    assert_eq!(
        counts1.by_class.get("property-violation"),
        None,
        "violations against the perfect-engine double are oracle bugs: {:?}",
        counts1.by_class
    );

    // Anti-vacuity floor: every unconditional property fired >= 1 time.
    for id in props::unconditional_v1() {
        let fired = counts1.by_property.get(id.as_str()).copied().unwrap_or(0);
        assert!(fired >= 1, "{} never fired across 1k plans", id.as_str());
    }

    // A5: hook-gated properties fired AND skipped-with-count under NoHooks.
    let f7 = counts1.by_property.get("F7-MemoryBaseline").copied().unwrap_or(0);
    let f8 = counts1.by_property.get("F8-ResourceBaseline").copied().unwrap_or(0);
    assert!(f7 >= 1 && f8 >= 1, "hook-gated properties must still be offered");
    assert_eq!(
        counts1.skipped_no_hook,
        f7 + f8,
        "every hook-gated instantiation must be a counted skip under NoHooks"
    );

    // Census sanity: pass + skip == total.
    let pass = counts1.by_class.get("ok").copied().unwrap_or(0);
    let skip = counts1.by_class.get("property-skipped").copied().unwrap_or(0);
    let total: u64 = counts1.by_property.values().sum();
    assert_eq!(pass + skip, total);
}

#[test]
fn planted_bug_is_found_by_seed_sweep() {
    // Mini planted-bug leg (G-R4's shape at oracle scale): an engine whose
    // DELETE silently deletes nothing must be caught by some seed within a
    // small budget.
    use simharness::oracle::check::StmtResult;
    use simharness::oracle::drive::StepExecutor;
    use simharness::oracle::ledger::LedgerOp;
    use simharness::oracle::pstep::{ArmCtl, SqlStep, TxCtl};

    struct NoDeleteEngine(LedgerSimExecutor);
    impl StepExecutor for NoDeleteEngine {
        fn exec_sql(&mut self, step: &SqlStep) -> StmtResult {
            if matches!(step.ledger_op, Some(LedgerOp::DeleteByKey { .. })) {
                // Planted bug: pretends to delete one row but doesn't.
                return StmtResult::Command { affected: 1 };
            }
            self.0.exec_sql(step)
        }
        fn exec_tx(&mut self, ctl: &TxCtl) {
            self.0.exec_tx(ctl)
        }
        fn exec_arm(&mut self, ctl: &ArmCtl) {
            self.0.exec_arm(ctl)
        }
    }

    let schema = SchemaView::default();
    let profile = ProfileView::default();
    let all = props::v1_set();
    let mut found = None;
    'seeds: for seed in 0..200u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let n_props = rng.gen_range(3..=8);
        let mut exec = NoDeleteEngine(LedgerSimExecutor::new());
        let mut ledger = Ledger::new();
        for _ in 0..n_props {
            let id = all[rng.gen_range(0..all.len())];
            let inst = props::generate(id, &mut rng, &schema, &profile);
            let report = evaluate_instance(&inst, &mut exec, &mut ledger, &NoHooks);
            if report.outcome == PropertyOutcome::Violation {
                found = Some((seed, report.property));
                break 'seeds;
            }
        }
    }
    let (seed, prop) = found.expect("planted no-delete bug must be found within 200 seeds");
    // The catching property must be one that exercises DELETE.
    assert!(
        matches!(
            prop,
            props::PropertyId::F3DeleteAbsence | props::PropertyId::M1ReadYourWrites
        ),
        "caught by {} at seed {seed}",
        prop.as_str()
    );
}
