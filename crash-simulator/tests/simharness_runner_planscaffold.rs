//! planface tests (post-integration): the runner's flat execution view over
//! WS-GEN's frozen plan-format v1. Round-trip law + reserved-tag policy +
//! flat/nested conversion (the authoritative format round-trip property test
//! is WS-GEN's G-G1 on src/plan.rs; this pins the flat view's fidelity).

use simharness::runner::planface::*;
use simharness::runner::profile::load_profile;
use simharness::runner::runloop::gen_plan;

#[test]
fn round_trip_generated_plans_bit_exact() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles");
    for prof in ["default", "write-heavy", "spill-stress", "savepoint-stress", "float-lenient"] {
        let lp = load_profile(dir.join(format!("{}.json", prof)).to_str().unwrap()).unwrap();
        for seed in [1u64, 2, 3, 42, 999, 123456789, u64::MAX - 1] {
            let plan = gen_plan(seed, &lp, "testgen");
            let text = plan.render();
            let re = Plan::parse(&text).unwrap_or_else(|e| panic!("{} seed {}: {}", prof, seed, e));
            assert_eq!(re, plan, "{} seed {}", prof, seed);
            // Bit-exact: render(parse(render(p))) == render(p).
            assert_eq!(re.render(), text, "{} seed {}", prof, seed);
        }
    }
}

#[test]
fn session_switch_is_hard_parse_error() {
    let text = format!(
        "{}\n-- seed: 1 profile: t profile-sha256: {} generator: g\n-- SESSION 2\n",
        PLAN_HEADER_LINE,
        "0".repeat(64)
    );
    let err = Plan::parse(&text).unwrap_err();
    assert!(
        err.contains("reserved: multi-session"),
        "§0 A1: reserved tag must hard-error, got: {err}"
    );
}

#[test]
fn reserved_fault_tags_render_and_parse() {
    let plan = Plan {
        header: PlanHeader {
            seed: 5,
            profile: "t".into(),
            profile_sha256: "0".repeat(64),
            generator: "g".into(),
        },
        steps: vec![
            Step::Fault(FaultPoint::Crash("pre-commit".into())),
            Step::Fault(FaultPoint::TornWrite),
            Step::Fault(FaultPoint::Env("enospc".into())),
            Step::Fault(FaultPoint::Disconnect),
            Step::Fault(FaultPoint::ReconnectServer),
        ],
    };
    let re = Plan::parse(&plan.render()).unwrap();
    assert_eq!(re, plan);
    assert!(re.steps[0..3].iter().all(|s| matches!(s, Step::Fault(f) if f.reserved())));
    assert!(re.steps[3..].iter().all(|s| matches!(s, Step::Fault(f) if !f.reserved())));
}

#[test]
fn bare_sql_rejected() {
    let text = format!(
        "{}\n-- seed: 1 profile: t profile-sha256: {} generator: g\nSELECT 1;\n",
        PLAN_HEADER_LINE,
        "0".repeat(64)
    );
    let err = Plan::parse(&text).unwrap_err();
    assert!(err.contains("bare SQL"), "bare statements need STEP/MARK annotations: {err}");
}

#[test]
fn header_seed_survives() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json");
    let lp = load_profile(dir.to_str().unwrap()).unwrap();
    let plan = gen_plan(8873421, &lp, "gsha");
    let re = Plan::parse(&plan.render()).unwrap();
    assert_eq!(re.header.seed, 8873421);
    assert_eq!(re.header.profile, "default");
    assert_eq!(re.header.profile_sha256, lp.sha256);
    assert_eq!(re.header.generator, "gsha");
}

#[test]
fn truncated_property_block_renders_closed() {
    // Shrinker phase-1 truncates mid-property; the artifact must still be a
    // valid plan-format v1 file (synthesized end marker).
    let plan = Plan {
        header: PlanHeader {
            seed: 7,
            profile: "t".into(),
            profile_sha256: "0".repeat(64),
            generator: "g".into(),
        },
        steps: vec![
            Step::BeginProperty { name: "F1-InsertSelect".into(), seq: 1, tables: vec!["a1".into()] },
            Step::Dml(Sql {
                text: "INSERT INTO a1 (k, v) VALUES (1, 2)".into(),
                mark: Mark::Mutation,
                meta: SqlMeta::default(),
            }),
            // truncated: no EndProperty
        ],
    };
    let text = plan.render();
    let re = Plan::parse(&text).unwrap();
    assert_eq!(re.steps.len(), 3, "end marker synthesized on render");
    assert!(matches!(re.steps[2], Step::EndProperty { seq: 1 }));
}

#[test]
fn oracle_property_plans_flatten_and_nest() {
    // A generated plan with property blocks converts loss-free between the
    // nested core form and the flat execution view.
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles/default.json");
    let lp = load_profile(dir.to_str().unwrap()).unwrap();
    let mut saw_property = false;
    for seed in 1u64..40 {
        let plan = gen_plan(seed, &lp, "testgen");
        if plan.steps.iter().any(|s| matches!(s, Step::BeginProperty { .. })) {
            saw_property = true;
            let core = plan.to_core().unwrap();
            assert_eq!(Plan::from_core(&core), plan, "seed {seed}");
        }
    }
    assert!(saw_property, "anti-vacuity: at least one property block in 40 seeds");
}
