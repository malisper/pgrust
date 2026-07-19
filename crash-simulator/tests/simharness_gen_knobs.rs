//! H6-GUC: planner-knob swarm sampler tests — seed determinism, degeneracy
//! guards (never a whole strategy family off, never an empty sampled set),
//! validator rejections, and end-to-end engagement (sampled knobs actually
//! appear as SET steps in generated plans).

use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use simharness::bridge;
use simharness::plan::render;
use simharness::gen::knobs::{
    sample_knob_sets, validate as validate_knobs, PlannerKnobs, JOIN_GROUP, PARALLEL_FORCE_SET,
    SCAN_GROUP,
};
use simharness::runner::profile::{load_profile, validate, Profile};

fn profiles_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles")
}

fn swarm_profile() -> Profile {
    let p = profiles_dir().join("planner-swarm.json");
    load_profile(p.to_str().unwrap()).unwrap().profile
}

fn full_cfg(off_percent: u32, parallel_percent: u32) -> PlannerKnobs {
    PlannerKnobs {
        sets_per_seed: 4,
        off_percent,
        knobs: SCAN_GROUP.iter().chain(JOIN_GROUP.iter()).map(|s| s.to_string()).collect(),
        parallel_percent,
    }
}

// ---------------------------------------------------------------------------
// Sampler determinism
// ---------------------------------------------------------------------------

/// Same RNG seed + same config => identical sampled sets (the reproducibility
/// contract campaigns rely on).
#[test]
fn sampler_is_seed_deterministic() {
    let cfg = full_cfg(45, 35);
    for seed in [0u64, 1, 7, 1234, u64::MAX] {
        let a = sample_knob_sets(&mut ChaCha8Rng::seed_from_u64(seed), &cfg);
        let b = sample_knob_sets(&mut ChaCha8Rng::seed_from_u64(seed), &cfg);
        assert_eq!(a, b, "seed {seed}: sampled sets must be identical");
    }
}

/// Different seeds explore different configurations (fixed seeds, so this is
/// deterministic, not a flake): across 32 seeds we must see more than one
/// distinct first set.
#[test]
fn sampler_varies_across_seeds() {
    let cfg = full_cfg(45, 35);
    let mut distinct = std::collections::BTreeSet::new();
    for seed in 0..32u64 {
        let sets = sample_knob_sets(&mut ChaCha8Rng::seed_from_u64(seed), &cfg);
        distinct.insert(format!("{:?}", sets[0]));
    }
    assert!(distinct.len() > 8, "expected variety across seeds, got {}", distinct.len());
}

// ---------------------------------------------------------------------------
// Degeneracy guards
// ---------------------------------------------------------------------------

/// Even at off_percent=100 (every knob wants to be off), no sampled set may
/// turn off a WHOLE strategy family (all scans, or all joins).
#[test]
fn guard_never_disables_all_scans_or_all_joins() {
    let cfg = full_cfg(100, 0);
    for seed in 0..200u64 {
        for set in sample_knob_sets(&mut ChaCha8Rng::seed_from_u64(seed), &cfg) {
            let offs: Vec<&str> = set
                .iter()
                .filter(|(_, v)| v == "off")
                .map(|(k, _)| k.as_str())
                .collect();
            for group in [&SCAN_GROUP, &JOIN_GROUP] {
                assert!(
                    !group.iter().all(|g| offs.contains(g)),
                    "seed {seed}: whole family {group:?} off in {set:?}"
                );
            }
        }
    }
}

/// At off_percent=0 nothing would be sampled off — the empty-set guard must
/// force exactly one knob off so every sampled set is a real planner point.
#[test]
fn guard_never_produces_an_empty_set() {
    let cfg = full_cfg(0, 0);
    for seed in 0..200u64 {
        for set in sample_knob_sets(&mut ChaCha8Rng::seed_from_u64(seed), &cfg) {
            assert_eq!(set.len(), 1, "seed {seed}: expected exactly one forced-off knob");
            assert_eq!(set[0].1, "off");
        }
    }
}

/// parallel_percent=100 appends the full parallel-forcing block to every set.
#[test]
fn parallel_block_appended_when_drawn() {
    let cfg = full_cfg(0, 100);
    for set in sample_knob_sets(&mut ChaCha8Rng::seed_from_u64(9), &cfg) {
        for (k, v) in PARALLEL_FORCE_SET {
            assert!(
                set.iter().any(|(sk, sv)| sk == k && sv == v),
                "missing parallel pair {k}={v} in {set:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Validator
// ---------------------------------------------------------------------------

#[test]
fn validator_accepts_checked_in_swarm_profile() {
    let p = swarm_profile();
    validate(&p).unwrap();
}

#[test]
fn validator_rejects_bad_knob_blocks() {
    let name = "t";
    let good = full_cfg(45, 35);
    // unknown knob name
    let mut k = good.clone();
    k.knobs.push("enable_no_such_thing".into());
    assert!(validate_knobs(&k, name).unwrap_err().contains("enable_no_such_thing"));
    // duplicate knob
    let mut k = good.clone();
    k.knobs.push("enable_seqscan".into());
    assert!(validate_knobs(&k, name).unwrap_err().contains("twice"));
    // empty knob list
    let mut k = good.clone();
    k.knobs.clear();
    assert!(validate_knobs(&k, name).is_err());
    // sets_per_seed out of range
    let mut k = good.clone();
    k.sets_per_seed = 0;
    assert!(validate_knobs(&k, name).is_err());
    let mut k = good.clone();
    k.sets_per_seed = 9;
    assert!(validate_knobs(&k, name).is_err());
    // percents out of range
    let mut k = good.clone();
    k.off_percent = 101;
    assert!(validate_knobs(&k, name).is_err());
    let mut k = good;
    k.parallel_percent = 101;
    assert!(validate_knobs(&k, name).is_err());
}

/// A weighted 'arm' kind with empty arm_sets is still rejected WITHOUT a
/// planner_knobs block (pre-H6 behavior preserved) and accepted WITH one.
#[test]
fn planner_knobs_satisfies_the_arm_requirement() {
    let mut p = swarm_profile();
    p.arm_sets.clear();
    validate(&p).unwrap();
    p.planner_knobs = None;
    assert!(validate(&p).unwrap_err().contains("arm_sets is empty"));
}

// ---------------------------------------------------------------------------
// End-to-end: plans actually carry the sampled knobs
// ---------------------------------------------------------------------------

/// Whole-plan determinism through the real profile: same seed twice =>
/// byte-identical rendered plan (the sampler rides the one seeded stream).
#[test]
fn swarm_plans_are_byte_deterministic() {
    let gp = bridge::runner_profile_to_gen(&swarm_profile());
    for seed in [1u64, 42, 4711] {
        let (p1, _c1) = bridge::generate_plan_with_ctx(seed, &gp, "00", "t");
        let (p2, _c2) = bridge::generate_plan_with_ctx(seed, &gp, "00", "t");
        assert_eq!(render(&p1), render(&p2), "seed {seed}");
    }
}

/// Engagement: across a small fixed seed range, sampled planner knobs appear
/// as SET steps in generated plans (the swarm dimension is live, not inert).
#[test]
fn swarm_plans_contain_enable_knob_sets() {
    let gp = bridge::runner_profile_to_gen(&swarm_profile());
    let mut plans_with_knobs = 0u32;
    for seed in 1..=50u64 {
        let (p, _c) = bridge::generate_plan_with_ctx(seed, &gp, "00", "t");
        if render(&p).contains("set enable_") {
            plans_with_knobs += 1;
        }
    }
    assert!(
        plans_with_knobs >= 10,
        "expected >=10/50 plans to carry sampled enable_* sets, got {plans_with_knobs}"
    );
}

/// A knob-less profile must take ZERO extra RNG draws: adding the H6 field
/// as None changes nothing (guards against accidental draw-order skew for
/// every pre-H6 profile). The default profile's OWN arm sets legitimately
/// carry enable_hashagg/enable_seqscan (an arm draw can emit them at any
/// seed — the original single-seed "no `set enable_`" assertion was seed
/// luck, broken by the H7 grammar's draw-order shift), so the pin is on the
/// SAMPLER-ONLY knobs: no swarm-family knob outside the profile's arm sets
/// may ever appear without a planner_knobs block.
#[test]
fn knobless_profiles_unaffected() {
    let p = profiles_dir().join("default.json");
    let lp = load_profile(p.to_str().unwrap()).unwrap();
    assert!(lp.profile.planner_knobs.is_none());
    let profile_arm_gucs: std::collections::BTreeSet<String> = lp
        .profile
        .arm_sets
        .iter()
        .flatten()
        .filter_map(|arm| arm.split_once('=').map(|(k, _)| k.to_string()))
        .collect();
    let gp = bridge::runner_profile_to_gen(&lp.profile);
    for seed in 1..=20u64 {
        let (p1, _) = bridge::generate_plan_with_ctx(seed, &gp, "00", "t");
        let rendered = render(&p1);
        for knob in SCAN_GROUP.iter().chain(JOIN_GROUP.iter()) {
            if profile_arm_gucs.contains(*knob) {
                continue;
            }
            assert!(
                !rendered.contains(&format!("SET {knob}")),
                "seed {seed}: sampler-only knob {knob} appeared without a planner_knobs block"
            );
        }
    }
}
