//! RUNTIME seam-miss smoke gate (Hardening B).
//!
//! The static install-completeness guard
//! (`recurrence_guard::every_declared_seam_is_installed_by_its_owner`) can only
//! reason about seams that are STATICALLY `::call`ed in source. It is provably
//! blind to two runtime-reachability classes:
//!
//!   1. A seam not yet statically `::call`ed (dismissed as an unused lint) that
//!      becomes reachable the instant a consumer is implemented.
//!   2. A seam dispatched through a fn-ptr / vtable table (table-AM, index-AM,
//!      FDW, DestReceiver) — grep sees it as "uncalled".
//!
//! Both panic at runtime with `seam not installed: <path>` (and the
//! `pgrust-trace` `[seam] MISS <path>` hook fires first). This test EXERCISES
//! the landed single-user smoke query paths against the real `postgres
//! --single` binary and asserts (a) the expected output appears and (b) NO
//! seam-miss signal shows up on stderr/stdout — a deterministic detector for
//! unwired seams on the actually-executed code paths.
//!
//! ## Why `#[ignore]` (opt-in)
//!
//! It needs the BUILT `./target/debug/postgres` binary AND an initdb'd data
//! fixture, neither of which a plain `cargo test` produces. CI (or a developer)
//! opts in with the env + invocation below. Marking it `#[ignore]` keeps the
//! default `cargo test -p seams-init` fast and hermetic while leaving this gate
//! runnable on demand.
//!
//! ## How to run
//!
//! ```text
//! # 1. Build the binary with the share dir baked in (PGRUST_PGSHAREDIR is a
//! #    COMPILE-TIME option_env!, so it must be set when the binary is built):
//! PGRUST_PGSHAREDIR=/tmp/pgrust_share cargo build -p seams-init --bin postgres
//!
//! # 2. Run the gate (it copies the initdb'd fixture per-query and drives the
//! #    three documented smoke commands):
//! PGRUST_SMOKE=1 \
//!   PGRUST_PGSHAREDIR=/tmp/pgrust_share \
//!   PGRUST_INITFILLED=/tmp/pgrust_initfilledd \
//!   cargo test -p seams-init --test runtime_seam_miss_smoke -- --ignored --nocapture
//! ```
//!
//! Overridable env (all have defaults matching the documented milestone setup):
//!   * `PGRUST_SMOKE`        — must be set (any value) to actually run; else the
//!                             test no-ops with a skip note (so a bare
//!                             `--ignored` run on a machine without the fixture
//!                             does not spuriously fail).
//!   * `PGRUST_POSTGRES_BIN` — path to the built binary
//!                             (default `target/debug/postgres` relative to the
//!                             workspace root inferred from `CARGO_MANIFEST_DIR`).
//!   * `PGRUST_PGSHAREDIR`   — runtime share dir (default `/tmp/pgrust_share`).
//!   * `PGRUST_INITFILLED`   — the initdb'd template data dir, copied per query
//!                             (default `/tmp/pgrust_initfilledd`).

use std::path::{Path, PathBuf};
use std::process::Command;

/// A single-user smoke query and the substring its successful output must
/// contain (the printtup row text), plus the exact number of printed data rows
/// expected (`= "` appears once per printed column value).
struct Smoke {
    sql: &'static str,
    /// A substring that MUST appear in stdout on success.
    expect_contains: &'static str,
    /// Exact count of printed row-value lines (`= "` occurrences) expected.
    expect_value_lines: usize,
}

const SMOKES: &[Smoke] = &[
    // MILESTONE 1: SELECT 1 -> single row `?column? = "1"`.
    Smoke {
        sql: "SELECT 1;",
        expect_contains: "?column? = \"1\"",
        expect_value_lines: 1,
    },
    // MILESTONE 2: full pg_class scan -> 415 catalog rows.
    Smoke {
        sql: "SELECT relname FROM pg_class;",
        expect_contains: "relname = \"pg_class\"",
        expect_value_lines: 415,
    },
    // MILESTONE 3: WHERE relkind='r' -> 68 base tables (strict subset).
    Smoke {
        sql: "SELECT relname FROM pg_class WHERE relkind = 'r';",
        expect_contains: "relname = \"pg_class\"",
        expect_value_lines: 68,
    },
];

fn workspace_root() -> PathBuf {
    // crates/seams-init -> crates -> <root>
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

fn postgres_bin() -> PathBuf {
    if let Ok(p) = std::env::var("PGRUST_POSTGRES_BIN") {
        return PathBuf::from(p);
    }
    workspace_root().join("target/debug/postgres")
}

fn pgshare_dir() -> String {
    std::env::var("PGRUST_PGSHAREDIR").unwrap_or_else(|_| "/tmp/pgrust_share".to_string())
}

fn initfilled_dir() -> String {
    std::env::var("PGRUST_INITFILLED").unwrap_or_else(|_| "/tmp/pgrust_initfilledd".to_string())
}

/// Copy the initdb'd template fixture to a throwaway data dir for one query
/// (the single-user backend mutates/locks the dir, so each run gets a fresh
/// copy — mirroring the documented `rm -rf /tmp/pgrust_q; cp -R ...` recipe).
fn fresh_datadir(tag: usize) -> PathBuf {
    let dst = std::env::temp_dir().join(format!("pgrust_smoke_q{tag}"));
    let _ = std::fs::remove_dir_all(&dst);
    let status = Command::new("cp")
        .arg("-R")
        .arg(initfilled_dir())
        .arg(&dst)
        .status()
        .expect("spawn cp for fixture copy");
    assert!(
        status.success(),
        "failed to copy initdb fixture {} -> {} (is PGRUST_INITFILLED correct?)",
        initfilled_dir(),
        dst.display()
    );
    dst
}

/// The two seam-miss signals the runtime emits: the loud panic message and the
/// `pgrust-trace` MISS hook (which fires BEFORE the panic, so even a swallowed
/// panic on a vtable path leaves this breadcrumb).
fn seam_miss_lines(output: &str) -> Vec<String> {
    output
        .lines()
        .filter(|l| l.contains("seam not installed:") || l.contains("[seam] MISS"))
        .map(|l| l.trim().to_string())
        .collect()
}

#[test]
#[ignore = "needs the built `postgres` binary + initdb fixture; set PGRUST_SMOKE=1 to run (see file header)"]
fn no_seam_miss_on_single_user_smoke_paths() {
    if std::env::var("PGRUST_SMOKE").is_err() {
        eprintln!(
            "SKIP: PGRUST_SMOKE not set. This runtime seam-miss gate needs the built \
             binary + initdb fixture. See the file header for the run recipe."
        );
        return;
    }

    let bin = postgres_bin();
    assert!(
        bin.exists(),
        "postgres binary not found at {} — build it first with \
         `PGRUST_PGSHAREDIR={} cargo build -p seams-init --bin postgres` \
         (or set PGRUST_POSTGRES_BIN)",
        bin.display(),
        pgshare_dir(),
    );

    let mut failures: Vec<String> = Vec::new();

    for (i, smoke) in SMOKES.iter().enumerate() {
        let datadir = fresh_datadir(i);

        // Mirror the documented smoke invocation exactly: single-user mode,
        // sync IO, the elevated max_stack_depth the Rust seam frames need, and
        // the SQL piped on stdin.
        let mut child = Command::new(&bin)
            .arg("--single")
            .arg("-c")
            .arg("io_method=sync")
            .arg("-c")
            .arg("max_stack_depth=7000")
            .arg("-D")
            .arg(&datadir)
            .arg("postgres")
            .env("PGRUST_PGSHAREDIR", pgshare_dir())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn postgres --single");

        use std::io::Write;
        child
            .stdin
            .take()
            .expect("child stdin")
            .write_all(format!("{}\n", smoke.sql).as_bytes())
            .expect("write SQL to child stdin");

        let out = child.wait_with_output().expect("wait for child");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        let combined = format!("{stdout}\n{stderr}");

        // (a) ZERO seam-miss signals on the exercised path. This is the core
        //     assertion: a reachable-but-unwired seam (incl. vtable-dispatched)
        //     leaves either the panic line or the trace MISS breadcrumb.
        let misses = seam_miss_lines(&combined);
        if !misses.is_empty() {
            failures.push(format!(
                "query {:?}: {} seam-miss signal(s) on the executed path:\n    {}",
                smoke.sql,
                misses.len(),
                misses.join("\n    ")
            ));
            continue;
        }

        // (b) The expected output actually appeared (proves the path RAN to
        //     completion — a guard that passes only because the query never
        //     reached the interesting code would be worthless).
        if !combined.contains(smoke.expect_contains) {
            failures.push(format!(
                "query {:?}: expected output substring {:?} not found (path did not \
                 complete?). stdout tail:\n{}",
                smoke.sql,
                smoke.expect_contains,
                stdout.lines().rev().take(20).collect::<Vec<_>>().join("\n")
            ));
            continue;
        }

        // (c) Exact printed-row count — catches a silently-truncated scan.
        let value_lines = combined.matches("= \"").count();
        if value_lines != smoke.expect_value_lines {
            failures.push(format!(
                "query {:?}: expected {} printed row-value line(s), saw {}",
                smoke.sql, smoke.expect_value_lines, value_lines
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "runtime seam-miss smoke gate FAILED on {} smoke path(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
