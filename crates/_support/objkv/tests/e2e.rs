//! The end-to-end shell scripts, from cargo:
//!
//!     cargo test -p objkv -- --ignored e2e
//!
//! Ignored by default: they need a server built with `--features objkv-s3`,
//! C PostgreSQL's initdb and psql, and an S3-compatible store (see
//! tests/server.sh for the environment).

#[test]
#[ignore = "needs a built server binary and an S3 store; see tests/server.sh"]
fn e2e_scripts_pass() {
    let runner = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("run_all.sh");
    let status = std::process::Command::new(&runner)
        .status()
        .unwrap_or_else(|e| panic!("cannot run {}: {e}", runner.display()));
    assert!(status.success(), "run_all.sh reported failures; see its output");
}
