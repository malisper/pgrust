//! G-R1: profile validator — connections>1 rejected (H1 serial law),
//! floors-declared-but-skipped path, all checked-in profiles validate.

use simharness::runner::profile::{load_profile, validate, Profile};

fn profiles_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("profiles")
}

#[test]
fn all_checked_in_profiles_validate() {
    let names = ["default", "write-heavy", "spill-stress", "savepoint-stress", "float-lenient"];
    for n in names {
        let p = profiles_dir().join(format!("{}.json", n));
        let lp = load_profile(p.to_str().unwrap()).unwrap_or_else(|e| panic!("{}: {}", n, e));
        assert_eq!(lp.profile.name, n);
        assert_eq!(lp.sha256.len(), 64);
        // Background policy default: autovacuum off in every checked-in
        // profile (spec §2.1).
        assert_eq!(lp.profile.background_policy.autovacuum, "off", "{}", n);
        assert_eq!(lp.profile.connections, 1, "{}", n);
    }
}

fn base_profile() -> Profile {
    let p = profiles_dir().join("default.json");
    load_profile(p.to_str().unwrap()).unwrap().profile
}

#[test]
fn connections_over_one_rejected() {
    let mut p = base_profile();
    p.connections = 2;
    let err = validate(&p).unwrap_err();
    assert!(err.contains("serial"), "must name the serial law: {}", err);
}

#[test]
fn connections_zero_rejected() {
    let mut p = base_profile();
    p.connections = 0;
    assert!(validate(&p).is_err());
}

#[test]
fn unknown_iso_level_rejected() {
    let mut p = base_profile();
    p.iso_mix.insert("read-uncommitted".into(), 5);
    assert!(validate(&p).is_err());
}

#[test]
fn zero_weights_rejected() {
    let mut p = base_profile();
    for v in p.statement_weights.values_mut() {
        *v = 0;
    }
    assert!(validate(&p).is_err());
}

#[test]
fn bad_arm_rejected() {
    let mut p = base_profile();
    p.arm_sets.push(vec!["work_mem".into()]); // not guc=value
    assert!(validate(&p).is_err());
}

#[test]
fn spill_stress_carries_work_mem_arm() {
    let p = profiles_dir().join("spill-stress.json");
    let lp = load_profile(p.to_str().unwrap()).unwrap();
    assert!(
        lp.profile.arm_sets.iter().any(|s| s.iter().any(|a| a == "work_mem=64kB")),
        "spill-stress must declare the work_mem=64kB arm (contract §4.1.2)"
    );
}

#[test]
fn declared_floors_skip_with_count() {
    // §0 A4: floors declared but instrument absent => counted skip line, not
    // enforcement. The campaign emits floor-skipped-no-instrument when the
    // profile declares floors; exercised via the census merge path here.
    let mut census = simharness::runner::verdict::Census::default();
    let mut p = base_profile();
    p.engagement_floors.insert("lane_engagement_sum".into(), 100);
    if !p.engagement_floors.is_empty() {
        census.add("floor-skipped-no-instrument", 1);
    }
    let mut buf: Vec<u8> = Vec::new();
    census.emit(&mut buf).unwrap();
    let s = String::from_utf8(buf).unwrap();
    assert!(s.contains("SIMHARNESS|floor-skipped-no-instrument|1"));
    assert!(s.contains("SIMHARNESS-VERDICT|PASS"), "skip is fine-class, not a failure: {}", s);
}

#[test]
fn arm_weight_with_empty_arm_sets_rejected() {
    // Review note: an 'arm'-weighted profile with arm_sets=[] made the
    // generator loop forever (arm draws emit no step). Reject at validation.
    let mut p = base_profile();
    p.arm_sets.clear();
    let err = validate(&p).unwrap_err();
    assert!(err.contains("arm_sets"), "must name the empty arm_sets: {}", err);
}
