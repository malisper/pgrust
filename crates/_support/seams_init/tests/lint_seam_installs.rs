//! Unit-shaped wiring for crates/_support/seams_init/tests/lint-seam-installs.sh — the CI differ for
//! the seam-install bug class (a ported+exported seam implementation that
//! init_all()'s closure never ::set()s, so is_installed()-guarded callers
//! silently skip forever). It would have caught the subtrans boot bug
//! (StartupSUBTRANS skipped on every boot) and the count_user_backends
//! panic-on-login; see the script header and notes/seam-audit.md.
//!
//! Lives in seams_init because init_all() IS the install closure the lint
//! checks against: `cargo test -p seams_init --test lint_seam_installs`.

use std::path::Path;
use std::process::Command;

#[test]
fn seam_installs_lint_passes() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repo root");
    let script = repo.join("crates/_support/seams_init/tests/lint-seam-installs.sh");
    assert!(script.is_file(), "missing {}", script.display());

    let out = Command::new("bash")
        .arg(&script)
        .current_dir(&repo)
        .output()
        .expect("run lint-seam-installs.sh");

    let report = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "lint-seam-installs.sh failed — a seam slot with a ported \
         implementation is not installed by seams_init::init_all() (or an \
         unported seam gained a caller without an allowlist classification):\n\
         {report}"
    );
}
