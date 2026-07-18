//! H6 scheduler + metrics-output tests (integration tier; the scheduling
//! policy itself is unit-tested in src/runner/schedule.rs).
//!
//! What is pinned here:
//!   - the FSE'21 blind-arm law at the ARTIFACT level: a guided arm's
//!     metrics.json must suppress Good-Turing U and say why, a blind arm's
//!     must keep it;
//!   - the over-time views (S(n) curve was H5; U(n) = f1/n per checkpoint is
//!     H6) and the schedule section (productive-seed fraction);
//!   - blind-arm purity one more time through the public scheduler API.

use simharness::gen::prodreg::{registry, KpathAccum};
use simharness::gen::profile::GenProfile;
use simharness::metrics::{MetricsArtifact, SpeciesCensus};
use simharness::runner::schedule::{ScheduleConfig, ScheduleStats, SpeciesScheduler};

fn gen_profile() -> GenProfile {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/gen_profiles/default.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn small_census() -> SpeciesCensus {
    let mut c = SpeciesCensus::default();
    // Seed 1: two sightings of A -> S=1, f1=0.
    c.add_sighting("Seq Scan");
    c.add_sighting("Seq Scan");
    c.checkpoint();
    // Seed 2: B once -> S=2, f1=1, n=3.
    c.add_sighting("Sort(Seq Scan)");
    c.checkpoint();
    c
}

fn artifact_json(stats: &ScheduleStats) -> serde_json::Value {
    let names: Vec<String> = simharness::oracle::props::v1_set()
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    let reg = registry(&refs);
    let gp = gen_profile();
    let kreport = simharness::gen::prodreg::evaluate(&KpathAccum::default(), &reg, &gp);
    let census = small_census();
    MetricsArtifact {
        kpath: &kreport,
        species: &census,
        explain_sample_every: 1,
        profile_name: "test",
        seed_base: 1,
        seed_count: 2,
        schedule: Some(stats),
    }
    .to_json()
}

#[test]
fn guided_arm_suppresses_good_turing_u_in_metrics_json() {
    let stats = ScheduleStats { enabled: true, ..ScheduleStats::default() };
    let j = artifact_json(&stats);
    assert_eq!(j["blind_arm"], serde_json::json!(false));
    assert!(j["species"]["good_turing_u"].is_null(), "guided U must be suppressed");
    let note = j["species"]["good_turing_u_note"].as_str().expect("note present");
    assert!(note.contains("BLIND"), "note must direct readers to blind arms: {note}");
    assert_eq!(j["schedule"]["enabled"], serde_json::json!(true));
}

#[test]
fn blind_arm_keeps_good_turing_u_and_curves() {
    let stats = ScheduleStats { enabled: false, ..ScheduleStats::default() };
    let j = artifact_json(&stats);
    assert_eq!(j["blind_arm"], serde_json::json!(true));
    // n=3, f1=1 -> U = 1/3.
    let u = j["species"]["good_turing_u"].as_f64().expect("blind U present");
    assert!((u - 1.0 / 3.0).abs() < 1e-12);
    assert!(j["species"]["good_turing_u_note"].is_null());
    // Over-time views: S(n) checkpoints (2,1) then (3,2); U(n) 0 then 1/3.
    assert_eq!(j["species"]["curve"], serde_json::json!([[2, 1], [3, 2]]));
    let uc = j["species"]["u_curve"].as_array().unwrap();
    assert_eq!(uc.len(), 2);
    assert_eq!(uc[0][0], serde_json::json!(2));
    assert_eq!(uc[0][1].as_f64().unwrap(), 0.0);
    assert_eq!(uc[1][0], serde_json::json!(3));
    assert!((uc[1][1].as_f64().unwrap() - 1.0 / 3.0).abs() < 1e-12);
}

#[test]
fn schedule_section_reports_productive_fraction() {
    let stats = ScheduleStats {
        enabled: true,
        neighbors: 4,
        decay: 8,
        seeds_total: 10,
        fresh_seeds: 6,
        guided_seeds: 4,
        productive_seeds: 3,
        productive_guided: 1,
        decays: 1,
    };
    let j = artifact_json(&stats);
    let s = &j["schedule"];
    assert_eq!(s["seeds_total"], serde_json::json!(10));
    assert_eq!(s["fresh_seeds"], serde_json::json!(6));
    assert_eq!(s["guided_seeds"], serde_json::json!(4));
    assert_eq!(s["productive_seeds"], serde_json::json!(3));
    assert_eq!(s["productive_guided"], serde_json::json!(1));
    assert!((s["productive_fraction"].as_f64().unwrap() - 0.3).abs() < 1e-12);
    assert_eq!(s["decays"], serde_json::json!(1));
}

/// Blind purity through the public API: whatever novelty the campaign
/// observes, flag-off scheduling is exactly the H5 sequential loop.
#[test]
fn blind_flag_off_is_h5_identical_regardless_of_novelty() {
    for pattern in [0u64, 1, 2] {
        let mut s = SpeciesScheduler::new(ScheduleConfig::default(), 1000, 40);
        let mut got = Vec::new();
        while let Some(seed) = s.next_seed() {
            got.push(seed);
            // Adversarial novelty patterns: all, none, alternating.
            s.report(match pattern {
                0 => true,
                1 => false,
                _ => seed % 2 == 0,
            });
        }
        let want: Vec<u64> = (1000..1040).collect();
        assert_eq!(got, want, "novelty pattern {pattern} perturbed a blind arm");
        assert_eq!(s.stats.guided_seeds, 0);
        assert_eq!(s.stats.fresh_seeds, 40);
    }
}

/// Determinism through the public API: two schedulers fed the same novelty
/// function yield identical sequences (the guided schedule is a pure function
/// of campaign seed + observations).
#[test]
fn guided_schedule_is_deterministic_given_campaign_seed() {
    let run = || {
        let cfg = ScheduleConfig { enabled: true, neighbors: 4, decay: 8 };
        let mut s = SpeciesScheduler::new(cfg, 500, 120);
        let mut got = Vec::new();
        while let Some(seed) = s.next_seed() {
            got.push(seed);
            s.report(seed % 11 == 0);
        }
        (got, s.stats.clone())
    };
    let (a, sa) = run();
    let (b, sb) = run();
    assert_eq!(a, b);
    assert_eq!(sa.guided_seeds, sb.guided_seeds);
    assert_eq!(sa.productive_seeds, sb.productive_seeds);
    assert!(sa.guided_seeds > 0, "guided arm should actually schedule follow-ons");
}
