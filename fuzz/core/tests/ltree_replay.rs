//! CI regression rail for ltree_diff (lane p1-ltree-t74): every committed
//! corpus input, plus any triage artifact, replays through the driver on
//! `cargo test` — stable rail, no nightly needed. This is also the rail the
//! injection sweep plants against, so its coverage IS the sweep's power.
use std::path::Path;

fn replay_dir(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else { return 0 };
    let mut n = 0;
    for e in rd {
        let p = e.unwrap().path();
        if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
            if std::env::var_os("LTREE_REPLAY_TRACE").is_some() {
                eprintln!("REPLAY {}", p.display());
            }
            decoder_fuzz::ltree_diff(&std::fs::read(&p).unwrap());
            n += 1;
        }
    }
    n
}

/// PRODUCTION STACK PAIRING (the lane's durable lesson, cost a fleet red):
/// the recursive parsers/matchers are guarded at max_stack_depth = 2048 kB,
/// and PG pairs that with an 8 MiB RLIMIT_STACK — 4x headroom. A worker with
/// a ~2 MiB stack AND a 2048 kB limit has none, so the guard can never fire
/// before the stack is exhausted and the process aborts. Any ltree fuzz
/// worker needs the same pairing.
const WORKER_STACK: usize = 8 << 20;

#[test]
fn replay_committed_corpus() {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(|| {
            let corpus = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/ltree_diff"));
            let n = replay_dir(corpus);
            assert!(n > 500, "ltree_diff corpus rail replayed only {n} inputs — bank missing?");
            eprintln!("ltree_diff: replayed {n} committed corpus inputs, 0 divergences");
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn replay_triage_artifacts() {
    let dir = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../artifacts-triage/ltree"));
    let n = replay_dir(dir);
    if n > 0 {
        eprintln!("ltree_diff: replayed {n} triage artifacts");
    }
}
