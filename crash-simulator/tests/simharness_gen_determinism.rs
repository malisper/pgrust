//! G-G3: plan determinism x2 — same seed+profile generated twice in SEPARATE
//! process invocations => byte-identical .plan, 100 seeds x all profiles.
//! Environment-unconditional (plan tier of the A3 determinism law).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn profile_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/gen_profiles")
}

fn run_batch(profile: &Path, out_dir: &Path) {
    let out = Command::new(env!("CARGO_BIN_EXE_simharness-gen"))
        .args([
            "gen-batch",
            "--seed-base",
            "7000",
            "--count",
            "100",
            "--profile",
            profile.to_str().unwrap(),
            "--out-dir",
            out_dir.to_str().unwrap(),
        ])
        // Pin the generator version so both invocations agree even if the
        // git worktree moves between them.
        .env("SIMHARNESS_GENERATOR_SHA", "determinism01")
        .output()
        .expect("spawn simharness-gen");
    assert!(
        out.status.success(),
        "gen-batch failed for {}:\n{}",
        profile.display(),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn dir_bytes(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for e in fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        out.insert(p.file_name().unwrap().to_string_lossy().into_owned(), fs::read(&p).unwrap());
    }
    out
}

#[test]
fn plan_determinism_x2_100_seeds_all_profiles() {
    let mut profiles: Vec<PathBuf> = fs::read_dir(profile_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    profiles.sort();
    assert_eq!(profiles.len(), 7, "expected 7 gen profiles");
    let base = std::env::temp_dir().join(format!("simharness-det-{}", std::process::id()));
    for profile in &profiles {
        let stem = profile.file_stem().unwrap().to_string_lossy();
        let d1 = base.join(format!("{stem}-run1"));
        let d2 = base.join(format!("{stem}-run2"));
        run_batch(profile, &d1);
        run_batch(profile, &d2);
        let b1 = dir_bytes(&d1);
        let b2 = dir_bytes(&d2);
        assert_eq!(b1.len(), 100, "{stem}: expected 100 plans");
        assert_eq!(
            b1.keys().collect::<Vec<_>>(),
            b2.keys().collect::<Vec<_>>(),
            "{stem}: file sets differ"
        );
        for (name, bytes1) in &b1 {
            assert_eq!(
                bytes1, &b2[name],
                "{stem}/{name}: plan bytes differ between process invocations"
            );
        }
    }
    let _ = fs::remove_dir_all(&base);
}

#[test]
fn header_pins_seed_profile_sha_and_generator() {
    let base = std::env::temp_dir().join(format!("simharness-hdr-{}", std::process::id()));
    fs::create_dir_all(&base).unwrap();
    let profile = profile_dir().join("default.json");
    let out = Command::new(env!("CARGO_BIN_EXE_simharness-gen"))
        .args(["gen", "--seed", "8873421", "--profile", profile.to_str().unwrap()])
        .env("SIMHARNESS_GENERATOR_SHA", "abc123def456")
        .output()
        .expect("spawn");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    let mut lines = text.lines();
    assert_eq!(lines.next().unwrap(), "-- simharness plan v1 (serial single-session)");
    let hdr = lines.next().unwrap();
    assert!(hdr.starts_with("-- seed: 8873421 profile: default profile-sha256: "));
    assert!(hdr.ends_with(" generator: abc123def456"));
    let _ = fs::remove_dir_all(&base);
}
