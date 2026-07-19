//! G-O1: each property checker with one green and one planted-red case.
//! Green: evaluate against the ledger-backed perfect-engine double.
//! Red: same, but a PerturbExecutor corrupts one asserted slot — the
//! property MUST report a violation.

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use simharness::oracle::check::{AllHooks, NoHooks};
use simharness::oracle::drive::{
    evaluate_instance, LedgerSimExecutor, PerturbExecutor, PropertyOutcome,
};
use simharness::oracle::ledger::Ledger;
use simharness::oracle::props::{self, PropertyId, ProfileView, SchemaView};
use simharness::oracle::pstep::{PStep, PropertyInstance};

fn gen_instance(id: PropertyId, seed: u64) -> PropertyInstance {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    props::generate(id, &mut rng, &SchemaView::default(), &ProfileView::default())
}

/// First slot referenced by an Assert (perturb target).
fn first_asserted_slot(inst: &PropertyInstance) -> Option<u32> {
    use simharness::oracle::check::Check;
    for step in &inst.steps {
        if let PStep::Assert(c) = step {
            let slot = match c {
                Check::ScalarEq { slot, .. }
                | Check::RowCountEq { slot, .. }
                | Check::CardLe { slot, .. }
                | Check::SqlStateClass { slot, .. }
                | Check::StmtOk { slot }
                | Check::LedgerTable { slot, .. }
                | Check::CountMatchesLedger { slot, .. } => *slot,
                Check::ScalarPairEq { a, .. } | Check::MultisetEq { a, .. } => *a,
                Check::ScalarSumEq { whole, .. } => *whole,
                // Metamorphic checks: perturb the WHOLE (or the optimized
                // NoREC leg) — the reassembled parts must then disagree.
                Check::PartitionUnionEq { whole, .. }
                | Check::DistinctUnionEq { whole, .. }
                | Check::ScalarExtremeEq { whole, .. } => *whole,
                Check::NorecRowCountEq { optimized, .. } => *optimized,
                Check::UnionDoubling { single, .. } => *single,
                Check::HookBaseline { before, .. } => *before,
                Check::HookPresent { .. } => continue,
                // H8 cursor checks.
                Check::RowsEq { slot, .. } | Check::CmdCountEq { slot, .. } => *slot,
            };
            return Some(slot);
        }
    }
    None
}

fn green_red(id: PropertyId) {
    for seed in [1u64, 7, 42] {
        // green
        let inst = gen_instance(id, seed);
        let mut exec = LedgerSimExecutor::new();
        let mut ledger = Ledger::new();
        let report = evaluate_instance(&inst, &mut exec, &mut ledger, &AllHooks);
        assert_eq!(
            report.outcome,
            PropertyOutcome::Pass,
            "{} seed {seed} green leg: {:?}",
            id.as_str(),
            report.detail
        );

        // red: corrupt the first asserted slot
        let inst = gen_instance(id, seed);
        let slot = first_asserted_slot(&inst)
            .unwrap_or_else(|| panic!("{} has no asserted slot", id.as_str()));
        let mut exec = PerturbExecutor { inner: LedgerSimExecutor::new(), target_slot: slot };
        let mut ledger = Ledger::new();
        let report = evaluate_instance(&inst, &mut exec, &mut ledger, &AllHooks);
        assert_eq!(
            report.outcome,
            PropertyOutcome::Violation,
            "{} seed {seed} planted-red leg must FAIL (slot {slot})",
            id.as_str()
        );
    }
}

#[test]
fn f1_insert_select() {
    green_red(PropertyId::F1InsertSelect);
}
#[test]
fn f2_update_visibility() {
    green_red(PropertyId::F2UpdateVisibility);
}
#[test]
fn f3_delete_absence() {
    green_red(PropertyId::F3DeleteAbsence);
}
#[test]
fn f4_constraint_error() {
    green_red(PropertyId::F4ConstraintError);
}
#[test]
fn f5_ddl_effects() {
    green_red(PropertyId::F5DdlEffects);
}
#[test]
fn f6_agg_consistency() {
    green_red(PropertyId::F6AggConsistency);
}
#[test]
fn f7_memory_baseline() {
    green_red(PropertyId::F7MemoryBaseline);
}
#[test]
fn f8_resource_baseline() {
    green_red(PropertyId::F8ResourceBaseline);
}
#[test]
fn l1_tlp() {
    green_red(PropertyId::L1Tlp);
}
#[test]
fn l2_norec() {
    green_red(PropertyId::L2NoRec);
}
#[test]
fn x1_arm_equivalence() {
    green_red(PropertyId::X1ArmEquivalence);
}
#[test]
fn x2_index_invariance() {
    green_red(PropertyId::X2IndexInvariance);
}
#[test]
fn x4_statement_form() {
    green_red(PropertyId::X4StatementForm);
}
#[test]
fn m1_read_your_writes() {
    green_red(PropertyId::M1ReadYourWrites);
}

#[test]
fn c1_cursor_walk() {
    green_red(PropertyId::C1CursorWalk);
}
#[test]
fn c2_hold_cursor() {
    green_red(PropertyId::C2HoldCursor);
}

#[test]
fn multi_session_props_are_sim_inapplicable_but_wellformed() {
    // M2/S1 emit H8 session-family steps the single-session sim executor
    // cannot drive: it reports a counted skip (AssumptionFailed), never a
    // false pass or violation. They must still be well-formed instances
    // that return to session 0 by their last step (the estate invariant).
    use simharness::oracle::pstep::PStep;
    for id in [PropertyId::M2CrossSession, PropertyId::S1SpecConflict] {
        for seed in [1u64, 7, 42] {
            let inst = gen_instance(id, seed);
            // Session balance: the last Session step must return to 0.
            let mut last_session = 0u32;
            let mut saw_session = false;
            for s in &inst.steps {
                if let PStep::Session(k) = s {
                    last_session = *k;
                    saw_session = true;
                }
            }
            assert!(saw_session, "{} emits no Session step", id.as_str());
            assert_eq!(
                last_session,
                0,
                "{} seed {seed} does not return to session 0",
                id.as_str()
            );
            // Sim executor: counted skip, not a verdict.
            let mut exec = LedgerSimExecutor::new();
            let mut ledger = Ledger::new();
            let report = evaluate_instance(&inst, &mut exec, &mut ledger, &AllHooks);
            assert_eq!(
                report.outcome,
                PropertyOutcome::AssumptionFailed,
                "{} must be sim-inapplicable (counted skip)",
                id.as_str()
            );
        }
    }
}

#[test]
fn hook_gated_props_skip_counted_without_hooks() {
    // Contract §0 A5: hook absent => auto-skip with a counted outcome,
    // never a silent pass.
    for id in [PropertyId::F7MemoryBaseline, PropertyId::F8ResourceBaseline] {
        let inst = gen_instance(id, 5);
        let mut exec = LedgerSimExecutor::new();
        let mut ledger = Ledger::new();
        let report = evaluate_instance(&inst, &mut exec, &mut ledger, &NoHooks);
        assert_eq!(report.outcome, PropertyOutcome::SkippedNoHook, "{}", id.as_str());
    }
    // Unconditional properties never hook-skip.
    let inst = gen_instance(PropertyId::L1Tlp, 5);
    let mut exec = LedgerSimExecutor::new();
    let mut ledger = Ledger::new();
    let report = evaluate_instance(&inst, &mut exec, &mut ledger, &NoHooks);
    assert_eq!(report.outcome, PropertyOutcome::Pass);
}

#[test]
fn generation_is_deterministic_per_seed() {
    // Plan-determinism law (§0 A3) at the property level: same seed => same
    // instance, different seed => (generally) different instance.
    for id in props::v1_set() {
        let a = gen_instance(id, 1234);
        let b = gen_instance(id, 1234);
        assert_eq!(a, b, "{} same-seed instances differ", id.as_str());
    }
}

#[test]
fn table_dependency_sets_are_declared() {
    for id in props::v1_set() {
        let inst = gen_instance(id, 99);
        assert!(
            !inst.tables.is_empty(),
            "{} must declare its touched-table set (shrinker API)",
            id.as_str()
        );
    }
}
