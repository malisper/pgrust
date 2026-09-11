//! Unit-shaped wiring for crates/_support/seams_init/tests/lint-walker-arms.sh —
//! the rail for the "missing case arm" bug class (PR #2090's sublevels-up
//! walker without Aggref/GroupingFunc arms, PR1604-1's expr_collation without
//! T_NextValueExpr, walsender's dropped command guard): a C switch over a
//! NodeTag-like enum (or an IsA chain) has an arm the Rust `match` lacks, and
//! `_ =>` hides it. tools/walker-arms/walker_arms.py pairs every C dispatch
//! function in the pinned reference tree with its Rust port and diffs the
//! handled labels; every gap must be adjudicated in lint-walker-arms.allow.
//!
//! Lives in seams_init beside the other lint-family wrappers:
//! `cargo test -p seams_init --test lint_walker_arms`.

use std::path::Path;
use std::process::Command;

#[test]
fn walker_arms_lint_passes() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repo root");
    let script = repo.join("crates/_support/seams_init/tests/lint-walker-arms.sh");
    assert!(script.is_file(), "missing {}", script.display());

    let out = Command::new("bash")
        .arg(&script)
        .current_dir(&repo)
        .output()
        .expect("run lint-walker-arms.sh");

    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "lint-walker-arms.sh failed — a ported dispatch function lacks an arm \
         its C counterpart has (port the arm C-exactly, or adjudicate the row \
         in crates/_support/seams_init/tests/lint-walker-arms.allow):\n{report}"
    );
}
