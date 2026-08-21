//! qa_crash_harness — the kill -9 ladder's victim process.
//!
//! Runs publish/commit rounds against a REAL directory through `RealVfs`
//! (the sanctioned vfs:: shims), self-recovering on start; the ladder
//! driver (tests/kill9_ladder.rs) spawns it and SIGKILLs it at randomized
//! points, then adjudicates the directory with `harness::check_dir`.
//!
//! Usage: qa_crash_harness <base_dir> <target_rounds> <rows_per_round>
//!        [salt] [--no-commit-last]
//!
//! Deliberately free of clocks, sleeps, and OS entropy: the process runs
//! rounds flat out; WHERE the kill lands is the driver's randomness.

use pgrc2_qa::harness::{ensure_dirs, run_rounds};
use pgrc2_write::wvfs::RealVfs;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: qa_crash_harness <base> <rounds> <rows_per_round> [salt] [--no-commit-last]");
        std::process::exit(2);
    }
    let base = &args[1];
    let rounds: u64 = args[2].parse().expect("rounds");
    let rows: u64 = args[3].parse().expect("rows_per_round");
    let salt: u64 = args
        .get(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let commit_last = !args.iter().any(|a| a == "--no-commit-last");

    let mut vfs = RealVfs;
    ensure_dirs(&mut vfs, base).expect("ensure dirs");
    match run_rounds(&mut vfs, base, rounds, rows, salt, commit_last) {
        Ok(s) => {
            println!(
                "DONE resumed_at={} completed={} gen={}",
                s.resumed_at_round, s.completed_rounds, s.effective_gen
            );
        }
        Err(e) => {
            eprintln!("HARNESS ERROR: {e}");
            std::process::exit(1);
        }
    }
}
