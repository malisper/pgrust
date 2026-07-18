//! Unit-shaped wiring for crates/_support/seams_init/tests/lint-determinism.sh — the DST P0
//! determinism-fencing ratchet (docs/design/dst-and-wasm.md, P0 phasing row
//! + §3.3 blocking census). Ten categories of raw nondeterminism (fs, time,
//! rand, spawn, env, blocking, plus the P3 §1.3 layer-2 widened arms join,
//! barrier, once, scope — dst-p3-scheduler.md §3 rows 14/15/18/19, seeded
//! by the P3-CENSUS Wave-1 entry gate) are grepped over production code and
//! diffed against the budgeted ledger in crates/_support/seams_init/tests/lint-determinism.allow; budgets
//! may only shrink and rows may only die. Deliberate exceptions carry a
//! "DST-REVIEW(<who>): <why>" marker — a review convention enforced by
//! humans diffing the allowlist (the lint cannot tell a new row from a
//! seeded one); the lint's part is CONFIG-FAILing malformed markers and
//! surfacing marker rows as NOTEs on every run.
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

/// NEGATIVE: libc syscall-surface sites (the review's biggest evasion class)
/// are censused — fs, time, and blocking each catch their libc primitives.
#[test]
fn libc_surface_fails_per_category() {
    let dir = scratch("libc");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "pub fn shapes() {\n\
         \x20   unsafe { libc::open(core::ptr::null(), 0) };\n\
         \x20   unsafe { libc::pread(0, core::ptr::null_mut(), 0, 0) };\n\
         \x20   unsafe { libc::clock_gettime(0, core::ptr::null_mut()) };\n\
         \x20   unsafe { libc::nanosleep(core::ptr::null(), core::ptr::null_mut()) };\n\
         }\n",
    )
    .unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "libc fs/time/blocking sites must fail:\n{report}");
    for want in [
        "VIOLATION(new-site): [fs] crates/backend/synthetic/src/lib.rs carries 2",
        "VIOLATION(new-site): [time] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [blocking] crates/backend/synthetic/src/lib.rs",
    ] {
        assert!(report.contains(want), "missing {want:?} in:\n{report}");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// NEGATIVE: import-line evasions — `use std::fs::{...}` braced groups and
/// `use std::env::var;`-style fn imports — are counted at the use line.
/// thread::sleep is a blocking site.
#[test]
fn import_line_and_sleep_evasions_fail() {
    let dir = scratch("imports");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "use std::fs::{rename, copy};\n\
         use std::env::var as getenv;\n\
         pub fn nap() { std::thread::sleep(std::time::Duration::from_millis(1)); }\n",
    )
    .unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "import-line/sleep evasions must fail:\n{report}");
    for want in [
        "VIOLATION(new-site): [fs] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [env] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [blocking] crates/backend/synthetic/src/lib.rs",
    ] {
        assert!(report.contains(want), "missing {want:?} in:\n{report}");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// NEGATIVE: the P3 §1.3 layer-2 widened arms — a raw std::sync::Barrier,
/// a zero-arg thread join, a racing get_or_init, and a thread::scope gang
/// each fail under their own category (the census-blind classes the
/// adversarial review proved live; dst-p3-scheduler.md §3 rows 14/15/18/19).
#[test]
fn widened_arms_barrier_join_once_scope_fail() {
    let dir = scratch("widened");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "use std::sync::{Arc, Barrier, OnceLock};\n\
         static KNOB: OnceLock<bool> = OnceLock::new();\n\
         pub fn gang(b: &Barrier) {\n\
         \x20   let _ = KNOB.get_or_init(|| true);\n\
         \x20   let h = std::thread::spawn(|| ());\n\
         \x20   let _ = h.join();\n\
         \x20   std::thread::scope(|s| { s.spawn(|| { b.wait(); }); });\n\
         }\n",
    )
    .unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "widened-arm sites must fail:\n{report}");
    for want in [
        "VIOLATION(new-site): [barrier] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [join] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [once] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [scope] crates/backend/synthetic/src/lib.rs",
        "VIOLATION(new-site): [spawn] crates/backend/synthetic/src/lib.rs",
    ] {
        assert!(report.contains(want), "missing {want:?} in:\n{report}");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// NEGATIVE: a "//" inside a string literal is not a comment — the fs call
/// after it on the same line is still censused (string-aware scanner).
#[test]
fn comment_tokens_inside_strings_do_not_hide_sites() {
    let dir = scratch("strlit");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "pub fn leak() -> (&'static str, String) {\n\
         \x20   (\"http://x\", std::fs::read_to_string(\"/y\").unwrap_or_default())\n\
         }\n",
    )
    .unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "fs site after a string-literal // must still be seen:\n{report}");
    assert!(
        report.contains("VIOLATION(new-site): [fs] crates/backend/synthetic/src/lib.rs"),
        "string-aware scan must catch the site:\n{report}"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// cfg-attr stripping edges: `#[cfg(any(feature = "x", test))]` (test not
/// first) IS stripped; `#[cfg(not(test))]` is NEVER stripped (it is
/// production code); a char-literal '{' or b"/*" inside a cfg(test) mod
/// must not poison the scan of production code after the mod.
#[test]
fn cfg_edges_and_literal_poison_handled() {
    let dir = scratch("cfgedges");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(
        src_dir.join("any_feature.rs"),
        "#[cfg(any(feature = \"x\", test))]\n\
         mod helpers { pub fn t() { let _ = std::fs::read_to_string(\"/a\"); } }\n",
    )
    .unwrap();
    fs::write(
        src_dir.join("lib.rs"),
        "mod any_feature;\nmod nottest;\nmod charlit;\n",
    )
    .unwrap();
    fs::write(
        src_dir.join("nottest.rs"),
        "#[cfg(not(test))]\npub fn prod() { let _ = std::fs::read_to_string(\"/b\"); }\n",
    )
    .unwrap();
    fs::write(
        src_dir.join("charlit.rs"),
        "#[cfg(test)]\nmod tests {\n    fn f(c: char) -> bool { c == '{' }\n    const B: &[u8] = b\"/*\";\n}\n\
         pub fn after() { let _ = std::fs::read_to_string(\"/c\"); }\n",
    )
    .unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(!ok, "nottest.rs and charlit.rs must violate:\n{report}");
    assert!(
        !report.contains("any_feature.rs"),
        "cfg(any(feature, test)) code must be stripped:\n{report}"
    );
    assert!(
        report.contains("VIOLATION(new-site): [fs] crates/backend/synthetic/src/nottest.rs"),
        "cfg(not(test)) is production code:\n{report}"
    );
    assert!(
        report.contains("VIOLATION(new-site): [fs] crates/backend/synthetic/src/charlit.rs"),
        "literals inside the test mod must not poison the prod tail:\n{report}"
    );
    assert!(!report.contains("WARN(scan-poison)"), "no poison expected:\n{report}");
    let _ = fs::remove_dir_all(&dir);
}

/// An unclosed block comment at EOF leaves the scanner state open — that is
/// surfaced as WARN(scan-poison) (diagnosed, not silently swallowed) and
/// does not gate.
#[test]
fn eof_poison_warns_but_passes() {
    let dir = scratch("poison");
    let src_dir = dir.join("tree/crates/backend/synthetic/src");
    fs::create_dir_all(&src_dir).unwrap();
    fs::write(src_dir.join("lib.rs"), "pub fn fine() {}\n/* unclosed at EOF\n").unwrap();
    let allow = dir.join("empty.allow");
    fs::write(&allow, "# empty\n").unwrap();

    let (ok, report) = run_lint(Some(&dir.join("tree")), Some(&allow));
    assert!(ok, "poison is a WARN, not a gate:\n{report}");
    assert!(
        report.contains("WARN(scan-poison): crates/backend/synthetic/src/lib.rs"),
        "EOF poison must be diagnosed:\n{report}"
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
