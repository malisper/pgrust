use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use pgrust::Database;
use pgrust::StatementResult;

const QUERIES: [&str; 4] = [
    "select ('{{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2]",
    "select ('{{3,4},{4,5}}'::int[])[1:1][1:2][1:2]",
    "select ('{{3,4},{4,5}}'::int[])[1:1][2][2]",
    "select ('[0:2][0:2]={{1,2,3},{4,5,6},{7,8,9}}'::int[])[1:2][2]",
];

struct Args {
    base_dir: PathBuf,
    iterations: usize,
    pool_size: usize,
    pause_before_run_secs: u64,
    preserve_existing: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin array_subscript_bench -- [options]

Options:
  --dir DIR                  Data dir (default: /tmp/pgrust_array_subscript_bench)
  --iterations N             Number of benchmark iterations (default: 20)
  --pool-size N              Buffer pool size (default: 16384)
  --pause-before-run-secs N  Sleep before timed execution so a profiler can attach
  --preserve-existing        Keep existing data dir contents"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_array_subscript_bench"),
        iterations: 20,
        pool_size: 16_384,
        pause_before_run_secs: 0,
        preserve_existing: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--dir" => args.base_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?),
            "--iterations" => {
                args.iterations = take_value(&raw, &mut i, "--iterations")?
                    .parse()
                    .map_err(|_| "invalid --iterations value".to_string())?;
            }
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--pause-before-run-secs" => {
                args.pause_before_run_secs = take_value(&raw, &mut i, "--pause-before-run-secs")?
                    .parse()
                    .map_err(|_| "invalid --pause-before-run-secs value".to_string())?;
            }
            "--preserve-existing" => {
                args.preserve_existing = true;
                i += 1;
            }
            "-h" | "--help" => usage(),
            other if other.starts_with("--") => return Err(format!("unknown flag: {other}")),
            _ => usage(),
        }
    }

    Ok(args)
}

fn take_value(args: &[String], i: &mut usize, name: &str) -> Result<String, String> {
    *i += 1;
    if *i >= args.len() {
        return Err(format!("{name} requires a value"));
    }
    let value = args[*i].clone();
    *i += 1;
    Ok(value)
}

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;

    if args.pause_before_run_secs > 0 {
        println!(
            "ready_for_profile: pid={} pause_secs={}",
            std::process::id(),
            args.pause_before_run_secs
        );
        std::thread::sleep(Duration::from_secs(args.pause_before_run_secs));
    }

    let started = Instant::now();
    let mut rows_seen = 0usize;
    for _ in 0..args.iterations {
        for query in QUERIES {
            let result = db.execute(1, query).map_err(|e| format!("{e:?}"))?;
            let StatementResult::Query { rows, .. } = result else {
                return Err("expected query result".into());
            };
            rows_seen += rows.len();
        }
    }
    let elapsed = started.elapsed();

    println!("engine: pgrust-direct");
    println!("base_dir: {}", args.base_dir.display());
    println!("iterations: {}", args.iterations);
    println!("queries_per_iteration: {}", QUERIES.len());
    println!("rows_seen: {rows_seen}");
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "avg_ms_per_iteration: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / args.iterations as f64
    );
    println!(
        "avg_ms_per_query: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / (args.iterations * QUERIES.len()) as f64
    );

    Ok(())
}
