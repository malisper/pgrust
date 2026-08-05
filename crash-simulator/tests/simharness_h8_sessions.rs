//! H8 estate integration: the cursor pair (C1/C2) and the multi-session
//! pair (M2/S1) generate under the shipped profiles, render to the correct
//! plan-format version, and round-trip bit-exact (the frozen-format law
//! extended to v2). Also the reach/emission guarantees the charter's gate
//! requires: the new productions actually emit.

use std::collections::BTreeSet;

use simharness::bridge::generate_plan_with_ctx;
use simharness::plan::{self, Plan, PlanItem, Step};
use simharness::runner::profile::load_profile;

fn gen(profile_file: &str, seed: u64) -> Plan {
    let lp = load_profile(profile_file).expect("load profile");
    let gp = simharness::bridge::runner_profile_to_gen(&lp.profile);
    let (plan, _ctx) = generate_plan_with_ctx(seed, &gp, &lp.sha256, "h8test");
    plan
}

fn property_names(plan: &Plan) -> BTreeSet<String> {
    plan.items
        .iter()
        .filter_map(|it| match it {
            PlanItem::Property { name, .. } => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn roundtrips(plan: &Plan) {
    let text = plan::render(plan);
    let reparsed = plan::parse(&text).expect("v2 plan must parse");
    assert_eq!(plan, &reparsed, "render∘parse must be identity");
    // Re-render is byte-stable.
    assert_eq!(text, plan::render(&reparsed), "re-render must be byte-stable");
}

#[test]
fn cursor_props_emit_on_default_and_roundtrip() {
    // C1/C2 are single-session: they generate on the DEFAULT profile and a
    // plan containing only them stays plan-format v1 (byte-identical shape
    // to pre-H8 for every non-session step).
    let mut seen = BTreeSet::new();
    for seed in 0..400u64 {
        let plan = gen("profiles/default.json", seed);
        seen.extend(property_names(&plan));
        roundtrips(&plan);
        // A default-profile plan uses no session steps => v1 header.
        assert!(!plan::plan_is_v2(&plan), "default profile emitted a session step");
        assert!(
            plan::render(&plan).lines().next().unwrap().contains("v1 (serial single-session)"),
            "serial plan must carry the v1 header"
        );
    }
    assert!(seen.contains("C1-CursorWalk"), "C1 never emitted in 400 default seeds");
    assert!(seen.contains("C2-HoldCursor"), "C2 never emitted in 400 default seeds");
    // Session-gated properties must NOT appear on a non-multi_session profile.
    assert!(!seen.contains("M2-CrossSession"), "M2 leaked onto default profile");
    assert!(!seen.contains("S1-SpecConflict"), "S1 leaked onto default profile");
}

#[test]
fn multi_session_props_emit_and_render_v2() {
    let mut seen = BTreeSet::new();
    let mut saw_v2 = false;
    for seed in 0..400u64 {
        let plan = gen("profiles/multi-session.json", seed);
        seen.extend(property_names(&plan));
        roundtrips(&plan);
        // Any plan carrying a session-family step must render under the v2
        // header and re-parse as the same plan (already checked in
        // roundtrips); track that at least one v2 plan appears.
        if plan::plan_is_v2(&plan) {
            saw_v2 = true;
            let text = plan::render(&plan);
            assert!(
                text.lines().next().unwrap().contains("v2 (multi-session)"),
                "v2 plan must carry the v2 header"
            );
        }
    }
    assert!(saw_v2, "no multi-session plan rendered v2 in 400 seeds");
    assert!(seen.contains("M2-CrossSession"), "M2 never emitted");
    assert!(seen.contains("S1-SpecConflict"), "S1 never emitted");
    assert!(seen.contains("C1-CursorWalk"), "C1 never emitted on multi-session profile");
}

#[test]
fn session_steps_balance_to_zero() {
    // Every emitted multi-session property returns the active session to 0
    // by its EndProperty (the estate invariant the runner also enforces).
    for seed in 0..200u64 {
        let plan = gen("profiles/multi-session.json", seed);
        let mut active = 0u32;
        for it in &plan.items {
            if let PlanItem::Property { name, steps, .. } = it {
                for s in steps {
                    if let Step::Session(k) = s {
                        active = *k;
                    }
                }
                assert_eq!(
                    active, 0,
                    "property {name} (seed {seed}) left active session {active} != 0"
                );
            }
        }
    }
}

#[test]
fn v1_header_still_refuses_session_steps() {
    // A hand-written v1 file with a session step stays a hard parse error
    // (the §0 A1 guarantee for v1 files is preserved verbatim).
    let v1_with_session = "-- simharness plan v1 (serial single-session)\n\
        -- seed: 1 profile: p profile-sha256: ab generator: g\n\n\
        -- SESSION switch 1\n";
    assert!(plan::parse(v1_with_session).is_err(), "SESSION under v1 header must error");
}
