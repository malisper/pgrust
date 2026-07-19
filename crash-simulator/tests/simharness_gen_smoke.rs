//! G-G2: 1k-plan gen-only smoke — 1000 plans across all checked-in gen
//! profiles: render, re-parse, screen-lint, per-plan step-kind census.
//!
//! Runs the real binary (separate process) so this is exactly what
//! scripts/simharness/smoke-1k.sh --gen-only (WS-RUNNER) will wrap.

use std::path::PathBuf;
use std::process::Command;

fn profile_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/gen_profiles")
}

#[test]
fn gen_smoke_1000_plans_all_profiles() {
    let census = std::env::temp_dir().join(format!("simharness-gen-census-{}.tsv", std::process::id()));
    let out = Command::new(env!("CARGO_BIN_EXE_simharness-gen"))
        .args([
            "smoke",
            "--profile-dir",
            profile_dir().to_str().unwrap(),
            "--count",
            "1000",
            "--seed-base",
            "42000",
            "--census",
            census.to_str().unwrap(),
        ])
        .env("SIMHARNESS_GENERATOR_SHA", "smoketest0001")
        .output()
        .expect("spawn simharness-gen");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "smoke failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("SIMHARNESS-VERDICT|PASS"), "no PASS verdict:\n{stdout}");
    assert!(stdout.contains("SIMHARNESS|gen-smoke-plans|1000"), "wrong plan count:\n{stdout}");
    assert!(stdout.contains("SIMHARNESS|screen-violations|0"), "screen violations:\n{stdout}\n{stderr}");
    assert!(stdout.contains("SIMHARNESS|roundtrip-failures|0"), "roundtrip failures:\n{stdout}");
    // Census: one aggregate line per profile...
    for p in ["default", "write-heavy", "spill-stress", "savepoint-stress", "float-lenient"] {
        assert!(
            stdout.contains(&format!("SIMHARNESS|gen-census|{p}|")),
            "missing census line for {p}:\n{stdout}"
        );
    }
    // ...and one TSV row per plan (+ header).
    let tsv = std::fs::read_to_string(&census).expect("census tsv written");
    assert_eq!(tsv.lines().count(), 1001, "census tsv must have 1000 rows + header");
    let _ = std::fs::remove_file(&census);
    // The float-lenient profile must actually exercise the R7-tagged path
    // (anti-vacuity at the smoke level).
    let fl_line = stdout
        .lines()
        .find(|l| l.starts_with("SIMHARNESS|gen-census|float-lenient|"))
        .expect("float-lenient census line");
    let floatlen: u64 = fl_line
        .split("floatlen=")
        .nth(1)
        .and_then(|s| s.split_whitespace().next())
        .and_then(|s| s.parse().ok())
        .expect("floatlen field");
    assert!(floatlen > 0, "float-lenient profile generated no tagged float aggregates: {fl_line}");
}
