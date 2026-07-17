//! Unit-shaped wiring for crates/_support/seams_init/tests/lint-determinism.sh — the DST P0
//! determinism-fencing ratchet (docs/design/dst-and-wasm.md, P0 phasing row
//! + §3.3 blocking census). Six categories of raw nondeterminism (fs, time,
//! rand, spawn, env, blocking) are grepped over production code and diffed
//! against the budgeted ledger in crates/_support/seams_init/tests/lint-determinism.allow; budgets
//! may only shrink, rows may only die, and deliberate exceptions must carry
//! a "DST-REVIEW(<who>): <why>" marker the lint surfaces as a NOTE.
//!
//! Lives in seams_init beside the lint-seam-installs wrapper (the lint-family
//! precedent): `cargo test -p seams_init --test lint_determinism`.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repo root")
}

fn run_lint(tree: Option<&std::path::Path>, allowlist: Option<&std::path::Path>) -> (bool, String) {
    let repo = repo_root();
    let script = repo.join("crates/_support/seams_init/tests/lint-determinism.sh");
    assert!(script.is_file(), "missing {}", script.display());
    let mut cmd = Command::new("bash");
    cmd.arg(&script).current_dir(&repo);
    if let Some(t) = tree {
        cmd.env("LINT_DETERMINISM_TREE", t);
    }
    if let Some(a) = allowlist {
        cmd.env("LINT_DETERMINISM_ALLOWLIST", a);
    }
    let out = cmd.output().expect("run lint-determinism.sh");
    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), report)
}

/// Scratch dir unique per test (tests run concurrently in one process).
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("lint-determinism-{}-{}", std::process::id(), name));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// The lint is green on the real tree with the seeded ledger: no raw
/// fs/time/rand/spawn/env/blocking site exists outside the allowlist, and
/// no budget has been exceeded. A failure here means a new raw
/// nondeterminism primitive was introduced — route it through the
/// sanctioned surface named in the violation line, or (deliberately,
/// reviewed) add a DST-REVIEW row.
#[test]
fn determinism_lint_passes() {
    let (ok, report) = run_lint(None, None);
    assert!(
        ok,
        "lint-determinism.sh failed — a raw nondeterminism site was added \
         outside the ratchet ledger (or a budget grew):\n{report}"
    );
    assert!(
        report.contains("lint-determinism PASS"),
        "expected PASS marker in report:\n{report}"
    );
}

/// NEGATIVE: a synthetic new raw-fs site with no allowlist row must fail,
/// naming the file and the category.
#[test]
fn new_raw_fs_site_fails_named() {
    let dir = scratch("newsite");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "pub fn leak() -> String {\n    std::fs::read_to_string(\"/etc/hostname\").unwrap_or_default()\n}\n",
    )
    .unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "lint must fail on an unallowlisted raw fs site:\n{report}");
    assert!(
        report.contains("VIOLATION(new-site): [fs] crates/backend/synthetic/src/lib.rs"),
        "violation must name category and file:\n{report}"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// NEGATIVE: growth past a row's budget (the ratchet) must fail even though
/// the file has a row.
#[test]
fn budget_growth_fails_ratchet() {
    let dir = scratch("ratchet");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "pub fn leak() -> String {\n    let _ = std::fs::metadata(\"/tmp\");\n    std::fs::read_to_string(\"/etc/hostname\").unwrap_or_default()\n}\n",
    )
    .unwrap();
    let allow = dir.join("one.allow");
    fs::write(&allow, "fs\tcrates/backend/synthetic/src/lib.rs\t1\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "lint must fail when sites exceed the budget:\n{report}");
    assert!(
        report.contains("VIOLATION(ratchet): [fs] crates/backend/synthetic/src/lib.rs"),
        "ratchet violation must name category and file:\n{report}"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// NEGATIVE-turned-warning: a row whose sites vanished must WARN stale but
/// still pass (delete-the-row hygiene, not a gate).
#[test]
fn removed_site_warns_stale() {
    let dir = scratch("stale");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(src_dir.join("lib.rs"), "pub fn clean() -> u32 { 42 }\n").unwrap();
    let allow = dir.join("stale.allow");
    fs::write(&allow, "fs\tcrates/backend/synthetic/src/lib.rs\t2\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(ok, "stale rows warn, they do not fail:\n{report}");
    assert!(
        report.contains("WARN(stale-allowlist): [fs] crates/backend/synthetic/src/lib.rs"),
        "stale row must be warned about:\n{report}"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A DST-REVIEW-marked row is accepted and surfaced as a NOTE (this is how
/// deliberate exceptions stay visible in every train diff).
#[test]
fn review_marker_row_accepted_and_noted() {
    let dir = scratch("review");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "pub fn leak() -> String {\n    std::fs::read_to_string(\"/etc/hostname\").unwrap_or_default()\n}\n",
    )
    .unwrap();
    let allow = dir.join("review.allow");
    fs::write(
        &allow,
        "fs\tcrates/backend/synthetic/src/lib.rs\t1\tDST-REVIEW(test): deliberate exception\n",
    )
    .unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(ok, "a marked row within budget passes:\n{report}");
    assert!(
        report.contains("NOTE(review-marker): [fs] crates/backend/synthetic/src/lib.rs"),
        "marker rows must surface as NOTEs:\n{report}"
    );
    let _ = fs::remove_dir_all(&dir);
}
