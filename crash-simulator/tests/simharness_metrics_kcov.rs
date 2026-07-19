//! H5 rung A tests: production registry lock, k=1..3 coverage math, THE
//! REACH GATE (teeth + revert-check), and trace determinism/validity.

use std::collections::BTreeMap;

use simharness::bridge;
use simharness::gen::prodreg::{self, evaluate, registry, GenTraces, KpathAccum};
use simharness::gen::profile::{
    ColTypeWeights, GenProfile, IsoMix, PlanLen, StatementWeights, TableShape,
};
use simharness::oracle::props;

fn prop_names() -> Vec<String> {
    props::v1_set().iter().map(|id| id.as_str().to_string()).collect()
}

fn full_registry() -> Vec<prodreg::ProdDef> {
    let names = prop_names();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    registry(&refs)
}

fn battery_gen_profile(name: &str) -> GenProfile {
    // Mirror of the battery profile shape (default.json) at the gen tier.
    GenProfile {
        name: name.into(),
        plan_len: PlanLen { min: 24, max: 64 },
        statement_weights: StatementWeights {
            ddl: 10,
            dml: 24,
            query: 30,
            tx: 10,
            arm: 4,
            fault: 2,
            property: 20,
        },
        table_shape: TableShape {
            min_cols: 3,
            max_cols: 3,
            col_types: ColTypeWeights::default(),
            rows_max: 200,
        },
        iso_mix: IsoMix { rc: 60, rr: 30, ser: 10 },
        arm_sets: vec![vec![], vec![("work_mem".into(), "4MB".into())]],
        property_weights: BTreeMap::new(),
        float_lenient: false,
        test_disable_productions: Vec::new(),
        planner_knobs: None,
        multi_session: false,
    }
}

fn gen_corpus(profile: &GenProfile, seeds: std::ops::Range<u64>) -> KpathAccum {
    let mut acc = KpathAccum::default();
    for seed in seeds {
        let (_plan, _ctx, traces) =
            bridge::generate_plan_with_ctx_traced(seed, profile, "00", "test");
        acc.add(&traces);
    }
    acc
}

// ---------------------------------------------------------------- registry

#[test]
fn registry_lock_every_traced_name_is_registered() {
    let reg = full_registry();
    let names: std::collections::BTreeSet<&str> =
        reg.iter().map(|d| d.name.as_str()).collect();
    let profile = battery_gen_profile("reglock");
    for seed in 1000..1100 {
        let (_p, _c, traces) =
            bridge::generate_plan_with_ctx_traced(seed, &profile, "00", "test");
        for path in &traces.paths {
            assert!(!path.is_empty(), "empty trace path");
            for node in path {
                assert!(
                    names.contains(node.as_str()),
                    "traced production '{node}' is not in the registry — register it in \
                     gen::prodreg (anti-staleness law)"
                );
            }
        }
    }
}

#[test]
fn registry_lock_paths_follow_parent_edges() {
    let reg = full_registry();
    let by_name: BTreeMap<&str, &prodreg::ProdDef> =
        reg.iter().map(|d| (d.name.as_str(), d)).collect();
    let profile = battery_gen_profile("edges");
    for seed in 1000..1060 {
        let (_p, _c, traces) =
            bridge::generate_plan_with_ctx_traced(seed, &profile, "00", "test");
        for path in &traces.paths {
            // Head is a statement-kind node (parent = root).
            assert!(
                by_name[path[0].as_str()].parent.is_none(),
                "path head '{}' is not a statement-kind node",
                path[0]
            );
            for w in path.windows(2) {
                let child = by_name[w[1].as_str()];
                assert_eq!(
                    child.parent.as_deref(),
                    Some(w[0].as_str()),
                    "trace edge {} > {} does not match the registry parent",
                    w[0],
                    w[1]
                );
            }
        }
    }
}

#[test]
fn epsilon_productions_never_traced() {
    let profile = battery_gen_profile("eps");
    for seed in 1000..1200 {
        let (_p, _c, traces) =
            bridge::generate_plan_with_ctx_traced(seed, &profile, "00", "test");
        for path in &traces.paths {
            assert!(
                !path.iter().any(|n| n == prodreg::LJC_NO_QUAL),
                "epsilon-class production appeared in a trace"
            );
        }
    }
}

// ------------------------------------------------------------ trace basics

#[test]
fn traces_deterministic_and_plan_bytes_unchanged() {
    let profile = battery_gen_profile("det");
    for seed in [1, 7, 999, 424242] {
        let (p1, _c1, t1) = bridge::generate_plan_with_ctx_traced(seed, &profile, "00", "t");
        let (p2, _c2, t2) = bridge::generate_plan_with_ctx_traced(seed, &profile, "00", "t");
        assert_eq!(p1, p2, "plan determinism broke at seed {seed}");
        assert_eq!(t1.paths, t2.paths, "trace determinism broke at seed {seed}");
        assert_eq!(t1.arm_set_hits, t2.arm_set_hits);
        // The non-traced entry point yields byte-identical plans.
        let (p3, _c3) = bridge::generate_plan_with_ctx(seed, &profile, "00", "t");
        assert_eq!(simharness::plan::render(&p1), simharness::plan::render(&p3));
    }
}

#[test]
fn traces_cover_every_top_level_sql_statement() {
    use simharness::plan::{PlanItem, Step};
    let profile = battery_gen_profile("align");
    for seed in 1000..1050 {
        let (plan, _c, traces) =
            bridge::generate_plan_with_ctx_traced(seed, &profile, "00", "t");
        let top_sql = plan
            .items
            .iter()
            .filter(|i| {
                matches!(
                    i,
                    PlanItem::Step(Step::Ddl(_))
                        | PlanItem::Step(Step::Dml(_))
                        | PlanItem::Step(Step::Query(_))
                )
            })
            .count();
        // Every top-level SQL statement has a trace path; TX/ARM/FAULT and
        // property blocks add more (>=).
        assert!(
            traces.paths.len() >= top_sql,
            "seed {seed}: {} paths < {} top-level SQL statements",
            traces.paths.len(),
            top_sql
        );
    }
}

// ---------------------------------------------------------- coverage math

#[test]
fn kcov_math_on_synthetic_accum() {
    let reg = full_registry();
    let profile = battery_gen_profile("math");
    // Empty accumulator: nothing covered; every reachable node uncovered.
    let empty = KpathAccum::default();
    let r = evaluate(&empty, &reg, &profile);
    assert_eq!(r.k1.covered, 0);
    assert!(r.k1.total > 0);
    assert_eq!(r.k1.uncovered.len(), r.k1.total);
    assert_eq!(r.reach_gap.len(), r.k1.total);
    assert_eq!(r.k2.covered, 0);
    assert_eq!(r.k3.covered, 0);
    // One full query path: 3 nodes, 2 edges, 1 triple.
    let mut acc = KpathAccum::default();
    let mut t = GenTraces::default();
    t.paths.push(vec![
        prodreg::STMT_QUERY.into(),
        prodreg::Q_SCALAR_CALL.into(),
        prodreg::SC_INT_ABS.into(),
    ]);
    acc.add(&t);
    let r = evaluate(&acc, &reg, &profile);
    assert_eq!(r.k1.covered, 3);
    assert_eq!(r.k2.covered, 2);
    assert_eq!(r.k3.covered, 1);
    assert_eq!(r.statements, 1);
    // No subsumption trap: k1..k3 are reported jointly with independent
    // denominators.
    assert!(r.k1.total != r.k2.total || r.k2.total != r.k3.total);
    // float-agg is gated-unreachable under the battery profile (float8 col
    // weight 0) and thus OUT of the k1 denominator and reach gate.
    assert!(r.gated_unreachable.iter().any(|(n, _)| n == prodreg::Q_FLOAT_AGG));
    assert!(!r.k1.uncovered.contains(&prodreg::Q_FLOAT_AGG.to_string()));
    // epsilon exclusion is explicit.
    assert!(r.epsilon_excluded.contains(&prodreg::LJC_NO_QUAL.to_string()));
    // H6 fix of H5 find 1: the disconnect PAIR floor in Budgets::allocate
    // guarantees any profile with fault weight > 0 a fault budget of >= 2,
    // so stmt:fault is now reachable-by-default under battery weights — IN
    // the k=1 denominator (and in the reach gap here, since the accumulator
    // never saw it).
    assert!(
        !r.gated_unreachable.iter().any(|(n, _)| n == prodreg::STMT_FAULT),
        "stmt:fault must be reachable under the pair floor"
    );
    assert!(r.k1.uncovered.contains(&prodreg::STMT_FAULT.to_string()));
}

// ------------------------------------------------------------- reach gate

/// Validation (1): reach-gate teeth. Disabling the join productions behind
/// the test knob must go RED naming exactly the joins; reverting the knob
/// (same seeds) must go green. This is the automated H3-0/9 detector.
#[test]
fn reach_gate_teeth_disabled_joins_go_red_and_revert_green() {
    let reg = full_registry();
    let joins = [
        prodreg::Q_INNER_JOIN,
        prodreg::Q_LEFT_JOIN_COALESCE,
        prodreg::Q_OJ_NEST_COALESCE,
    ];

    // Teeth: joins disabled at emission, gate still expects them.
    let mut doctored = battery_gen_profile("teeth");
    doctored.test_disable_productions = joins.iter().map(|s| s.to_string()).collect();
    let acc = gen_corpus(&doctored, 1000..1300);
    let r = evaluate(&acc, &reg, &doctored);
    for j in &joins {
        assert!(
            r.reach_gap.contains(&j.to_string()),
            "reach gate failed to name disabled production {j}"
        );
    }
    // The gate is RED for the joins AND their descendants only — no
    // unrelated production may ride along on a 300-seed corpus.
    for gap in &r.reach_gap {
        assert!(
            joins.iter().any(|j| gap == j)
                || gap == prodreg::LJC_QUAL_COALESCE, // child of a disabled join
            "unexpected reach-gap entry '{gap}' (flaky corpus or gate bug)"
        );
    }

    // Revert-check: same seeds, knob off => joins covered, gate green.
    let clean = battery_gen_profile("revert");
    let acc = gen_corpus(&clean, 1000..1300);
    let r = evaluate(&acc, &reg, &clean);
    assert!(
        r.reach_gap.is_empty(),
        "clean battery-shaped profile shows reach gaps: {:?}",
        r.reach_gap
    );
    for j in &joins {
        assert!(!r.k1.uncovered.contains(&j.to_string()));
    }
}

/// The reach-gap census class is P1 (verdict RED), and distinct from the
/// pinned triage vocabulary.
#[test]
fn reach_gap_class_is_p1_and_not_a_triage_class() {
    assert_eq!(simharness::vocab::severity_of("reach-gap").as_str(), "P1");
    assert!(simharness::vocab::Class::from_str("reach-gap").is_none());
}

#[test]
fn profile_validator_rejects_unknown_disable_names() {
    let mut p = serde_json::from_str::<simharness::runner::profile::Profile>(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json"),
        )
        .unwrap(),
    )
    .unwrap();
    p.test_disable_productions = vec!["q:no-such-production".into()];
    let err = simharness::runner::profile::validate(&p).unwrap_err();
    assert!(err.contains("q:no-such-production"), "got: {err}");
}

/// H5 review F1 pin: a REGISTERED production whose emission site does not
/// consult the teeth knob must be rejected at validation — otherwise the
/// knob silently disables nothing while a teeth test believes otherwise
/// (live repro: dml:update validated fine, kept emitting, no reach-gap).
/// Honored names (gen_query variants) still validate.
#[test]
fn profile_validator_rejects_unhonored_disable_names() {
    let mut p = serde_json::from_str::<simharness::runner::profile::Profile>(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json"),
        )
        .unwrap(),
    )
    .unwrap();
    // Registered (DML_UPDATE is in the registry) but not honored at any
    // emission site — must fail with the honored-set message.
    p.test_disable_productions = vec![prodreg::DML_UPDATE.into()];
    let err = simharness::runner::profile::validate(&p).unwrap_err();
    assert!(
        err.contains(prodreg::DML_UPDATE) && err.contains("not honored"),
        "got: {err}"
    );
    // Honored entries (query variants) still pass validation.
    p.test_disable_productions = vec![prodreg::Q_SRF_UNNEST.into(), prodreg::Q_INNER_JOIN.into()];
    simharness::runner::profile::validate(&p).expect("honored names must validate");
    // The honored-set predicate itself: exactly the gen_query variants.
    assert!(simharness::gen::noise::teeth_knob_honored(prodreg::Q_OJ_NEST_COALESCE));
    assert!(!simharness::gen::noise::teeth_knob_honored(prodreg::STMT_FAULT));
}

/// Battery-profile reach pin: every checked-in profile, at the smoke run-tier
/// seed budget (500 seeds, FIXED base 1000 — deterministic, no sampling
/// flake), is gate-green. Catches weight changes that silently starve a
/// production (the fault-pair find) before the smoke tier goes RED.
#[test]
fn battery_profiles_gate_green_at_smoke_budget() {
    let reg = full_registry();
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles");
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let lp = simharness::runner::profile::load_profile(path.to_str().unwrap()).unwrap();
        let gp = bridge::runner_profile_to_gen(&lp.profile);
        let mut acc = KpathAccum::default();
        for seed in 1000..1500u64 {
            let (_p, _c, t) = bridge::generate_plan_with_ctx_traced(seed, &gp, "00", "t");
            acc.add(&t);
        }
        let r = evaluate(&acc, &reg, &gp);
        assert!(
            r.reach_gap.is_empty(),
            "profile {} has reach gaps at the 500-seed smoke budget: {:?}",
            lp.profile.name,
            r.reach_gap
        );
    }
}
