//! The real-process kill -9 ladder (§5 M3-K: "kill -9 ladder at EVERY
//! publish step ⇒ recovery to a consistent manifest, acked data intact
//! (#253 law)"): spawn the `qa_crash_harness` binary against a REAL
//! directory, SIGKILL it at a randomized moment, adjudicate the directory
//! with the full checker (old-or-new vs the clog, both manifest walks
//! agree, decode == oracle, residue reclaimed), respawn — the respawn
//! itself recovers, so every cycle exercises crash recovery twice. The
//! fine-grain per-op boundary coverage is the SimVfs sweep's; this leg
//! proves the REAL filesystem + real process death path end to end.
//!
//! The #254 leg runs the harness in `--no-commit-last` mode: a fully
//! durable publish with no commit record must stay invisible and its
//! residue reclaimed.
//!
//! Born-RED: after the ladder is green, one flipped byte in a live part
//! file MUST fail the checker (the detector detects); iteration witnesses
//! prove the chartered kill count ran and that kills landed mid-run.

mod common;

use pgrc2_qa::harness::{check_dir, read_clog, round_fxid};
use pgrc2_write::wvfs::RealVfs;
use std::process::{Child, Command, Stdio};

const ROWS_PER_ROUND: u64 = 4_000;
const TARGET_ROUNDS: u64 = 40;

fn harness_bin() -> &'static str {
    env!("CARGO_BIN_EXE_qa_crash_harness")
}

fn spawn_harness(base: &str, rounds: u64, extra: &[&str]) -> Child {
    Command::new(harness_bin())
        .arg(base)
        .arg(rounds.to_string())
        .arg(ROWS_PER_ROUND.to_string())
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn qa_crash_harness")
}

fn fresh_base(tag: &str) -> String {
    let base = format!("{}/pgrc2-qa-ladder-{tag}", env!("CARGO_TARGET_TMPDIR"));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("mkdir base");
    base
}

/// One kill cycle: spawn, kill after `delay_ms`, wait, adjudicate.
/// Returns (committed_rounds, killed_before_done).
fn kill_cycle(base: &str, delay_ms: u64) -> (u64, bool) {
    let mut child = spawn_harness(base, TARGET_ROUNDS, &[]);
    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    // SIGKILL (std's kill on unix), then reap.
    let killed = match child.try_wait().expect("try_wait") {
        Some(_status) => false, // finished before the kill landed
        None => {
            child.kill().expect("SIGKILL");
            true
        }
    };
    let _ = child.wait();
    let mut vfs = RealVfs;
    let report = check_dir(&mut vfs, base, ROWS_PER_ROUND)
        .unwrap_or_else(|e| panic!("ladder checker failed after kill: {e}"));
    (report.committed_rounds, killed)
}

#[test]
fn kill9_ladder() {
    let s = common::scale();
    let mut base = fresh_base("main-0");
    let mut cycle_dir = 0u64;
    let mut kills_done = 0u64;
    let mut mid_run_kills = 0u64;
    let mut rng: u64 = 0x517;
    let mut last_committed = 0u64;
    while kills_done < s.ladder_kills {
        // Deterministic-ish pseudo-random delay; the harness's own speed is
        // the entropy that moves the kill point across publish steps.
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let delay = 2 + (rng >> 33) % 120;
        let (committed, killed) = kill_cycle(&base, delay);
        assert!(
            committed >= last_committed,
            "committed rounds went backwards: {last_committed} -> {committed}"
        );
        last_committed = committed;
        kills_done += 1;
        if killed {
            mid_run_kills += 1;
        }
        if committed >= TARGET_ROUNDS {
            // This directory is done — cycle a fresh one to keep killing.
            cycle_dir += 1;
            base = fresh_base(&format!("main-{cycle_dir}"));
            last_committed = 0;
        }
    }
    assert_eq!(kills_done, s.ladder_kills, "kill count witness");
    assert!(
        mid_run_kills > 0,
        "no kill ever landed mid-run — the ladder exercised nothing"
    );

    // Run the current directory to completion and verify the final state.
    let mut child = spawn_harness(&base, TARGET_ROUNDS, &[]);
    let status = child.wait().expect("wait");
    assert!(status.success(), "clean finishing run failed");
    let mut vfs = RealVfs;
    let report = check_dir(&mut vfs, &base, ROWS_PER_ROUND).expect("final check");
    assert_eq!(report.committed_rounds, TARGET_ROUNDS);
    assert_eq!(report.total_rows, TARGET_ROUNDS * ROWS_PER_ROUND);
    println!(
        "kill9 ladder: {kills_done} kills ({mid_run_kills} mid-run) across {} dirs, final {} rows",
        cycle_dir + 1,
        report.total_rows
    );

    // Born-RED tooth: corrupt one byte of a live part — the checker MUST
    // fail. (tests/ are outside the determinism-lint production cone.)
    let table = format!("{base}/table");
    let part_name = std::fs::read_dir(&table)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .find(|n| n.starts_with("part-"))
        .expect("a live part exists");
    let path = format!("{table}/{part_name}");
    let mut bytes = std::fs::read(&path).expect("read part");
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x20;
    std::fs::write(&path, &bytes).expect("write corrupted part");
    let verdict = check_dir(&mut vfs, &base, ROWS_PER_ROUND);
    assert!(
        verdict.is_err(),
        "born-RED failure: a corrupted live part passed the ladder checker"
    );
}

#[test]
fn no_commit_publish_stays_invisible_254() {
    let base = fresh_base("nc254");
    // Two committed rounds, then a third whose publish is durable but whose
    // commit record is never written.
    let mut child = spawn_harness(&base, 3, &["1", "--no-commit-last"]);
    let status = child.wait().expect("wait");
    assert!(status.success(), "no-commit harness errored");

    // Premise witnesses: 2 commit records; the dead generation's manifest
    // file IS on disk.
    let mut vfs = RealVfs;
    let clog = read_clog(&mut vfs, &base).expect("clog");
    assert_eq!(clog.len(), 2, "expected exactly 2 committed rounds");
    assert!(
        clog.contains(&round_fxid(0, 1)) && clog.contains(&round_fxid(1, 1)),
        "clog contents unexpected: {clog:?}"
    );
    let manifests: Vec<String> = std::fs::read_dir(format!("{base}/table"))
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("manifest-"))
        .collect();
    assert!(
        manifests.len() >= 3,
        "premise broken: dead gen-3 manifest not durable ({manifests:?})"
    );

    // The checker: effective = exactly 2 rounds (the durable-but-uncommitted
    // publish is invisible), residue reclaimed by check_dir's cleanup pass.
    let report = check_dir(&mut vfs, &base, ROWS_PER_ROUND).expect("check");
    assert_eq!(report.committed_rounds, 2);
    assert_eq!(report.total_rows, 2 * ROWS_PER_ROUND);
    assert!(
        report.removed_residue > 0,
        "the dead publish left no residue to reclaim — premise broken"
    );

    // Recovery-then-resume: a normal harness (different salt ⇒ different
    // epoch-qualified fxid) republishes round 3 and commits.
    let mut child = spawn_harness(&base, 3, &["0"]);
    let status = child.wait().expect("wait");
    assert!(status.success(), "resume harness errored");
    let report = check_dir(&mut vfs, &base, ROWS_PER_ROUND).expect("check after resume");
    assert_eq!(report.committed_rounds, 3);
    assert_eq!(report.total_rows, 3 * ROWS_PER_ROUND);
}
