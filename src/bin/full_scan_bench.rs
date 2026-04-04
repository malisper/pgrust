use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::Instant;

use pgrust::database::Database;
use pgrust::executor::{StatementResult, Value};

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    if !args.skip_load {
        db.execute(
            1,
            "create table scanbench (id int not null, payload text not null)",
        )
        .map_err(|e| format!("{e:?}"))?;

        for i in 0..args.row_count {
            db.execute(
                1,
                &format!("insert into scanbench (id, payload) values ({i}, 'row-{i}')"),
            )
            .map_err(|e| format!("{e:?}"))?;
        }
    }

    if args.pause_before_scan_secs > 0 {
        println!("setup_complete: pausing {}s before timed scans", args.pause_before_scan_secs);
        std::thread::sleep(std::time::Duration::from_secs(args.pause_before_scan_secs));
    }

    let mut total_rows = 0usize;
    let mut checksum = 0i64;
    db.pool.reset_usage_stats();
    let started = Instant::now();
    for _ in 0..args.iterations {
        let result = db
            .execute(2, "select * from scanbench")
            .map_err(|e| format!("{e:?}"))?;
        let StatementResult::Query { rows, .. } = result else {
            return Err("expected query result".into());
        };
        total_rows += rows.len();
        checksum += rows.iter().map(|row| row_checksum(row)).sum::<i64>();
    }
    let elapsed = started.elapsed();
    let stats = db.pool.usage_stats();

    println!("engine: pgrust-direct");
    println!("base_dir: {}", args.base_dir.display());
    println!("rows: {}", args.row_count);
    println!("iterations: {}", args.iterations);
    println!("total_rows_seen: {total_rows}");
    println!("checksum: {checksum}");
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "avg_ms_per_scan: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / args.iterations as f64
    );
    println!(
        "rows_per_sec: {:.3}",
        total_rows as f64 / elapsed.as_secs_f64()
    );
    println!("buffer_hits: {}", stats.shared_hit);
    println!("buffer_reads: {}", stats.shared_read);
    println!("buffer_written: {}", stats.shared_written);

    Ok(())
}

struct Args {
    base_dir: PathBuf,
    row_count: usize,
    iterations: usize,
    pool_size: usize,
    pause_before_scan_secs: u64,
    preserve_existing: bool,
    skip_load: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_full_scan_bench"),
        row_count: 10_000,
        iterations: 100,
        pool_size: 16384, // 128MB at 8KB per page
        pause_before_scan_secs: 0,
        preserve_existing: false,
        skip_load: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--preserve-existing" => {
                args.preserve_existing = true;
                i += 1;
            }
            "--skip-load" => {
                args.skip_load = true;
                i += 1;
            }
            "--dir" => {
                args.base_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?);
            }
            "--rows" => {
                args.row_count = take_value(&raw, &mut i, "--rows")?
                    .parse()
                    .map_err(|_| "invalid --rows value".to_string())?;
            }
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
            "--pause" => {
                args.pause_before_scan_secs = take_value(&raw, &mut i, "--pause")?
                    .parse()
                    .map_err(|_| "invalid --pause value".to_string())?;
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown flag: {other}"));
            }
            _ => {
                // Legacy positional args: dir [rows [iterations [pool_size [pause]]]]
                args.base_dir = PathBuf::from(&raw[i]);
                if i + 1 < raw.len() {
                    args.row_count = raw[i + 1].parse().map_err(|_| "invalid rows")?;
                }
                if i + 2 < raw.len() {
                    args.iterations = raw[i + 2].parse().map_err(|_| "invalid iterations")?;
                }
                if i + 3 < raw.len() {
                    args.pool_size = raw[i + 3].parse().map_err(|_| "invalid pool_size")?;
                }
                if i + 4 < raw.len() {
                    args.pause_before_scan_secs = raw[i + 4].parse().map_err(|_| "invalid pause")?;
                }
                break;
            }
        }
    }

    Ok(args)
}

fn take_value(args: &[String], i: &mut usize, name: &str) -> Result<String, String> {
    *i += 1;
    if *i >= args.len() {
        return Err(format!("{name} requires a value"));
    }
    let val = args[*i].clone();
    *i += 1;
    Ok(val)
}

fn row_checksum(row: &[Value]) -> i64 {
    row.iter().map(value_checksum).sum()
}

fn value_checksum(value: &Value) -> i64 {
    match value {
        Value::Int32(v) => *v as i64,
        Value::Float64(v) => *v as i64,
        Value::Text(v) => v.bytes().map(i64::from).sum(),
        Value::Bool(v) => i64::from(*v),
        Value::Null => 0,
    }
}
