use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Instant;

use pgrust::executor::{StatementResult, Value};
use pgrust::pgrust::database::{Database, Session};

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    if !args.skip_load {
        let mut session = Session::new(1);
        session
            .execute(
                &db,
                "create table scanbench (id int not null, payload text not null)",
            )
            .map_err(|e| format!("{e:?}"))?;

        session
            .execute(&db, "begin")
            .map_err(|e| format!("{e:?}"))?;
        for i in 0..args.row_count {
            session
                .execute(
                    &db,
                    &format!("insert into scanbench (id, payload) values ({i}, 'row-{i}')"),
                )
                .map_err(|e| format!("{e:?}"))?;
        }
        session
            .execute(&db, "commit")
            .map_err(|e| format!("{e:?}"))?;
    }

    if args.pause_before_scan_secs > 0 {
        println!(
            "setup_complete: pausing {}s before timed scans",
            args.pause_before_scan_secs
        );
        std::thread::sleep(std::time::Duration::from_secs(args.pause_before_scan_secs));
    }

    let total_rows = Arc::new(AtomicUsize::new(0));
    let checksum = Arc::new(AtomicI64::new(0));
    db.pool.reset_usage_stats();
    let db = Arc::new(db);
    let started = Instant::now();

    if args.clients <= 1 {
        // Single-threaded path.
        for _ in 0..args.iterations {
            let result = db.execute(2, &args.query).map_err(|e| format!("{e:?}"))?;
            let StatementResult::Query { rows, .. } = result else {
                return Err("expected query result".into());
            };
            total_rows.fetch_add(rows.len(), Ordering::Relaxed);
            checksum.fetch_add(
                rows.iter().map(|row| row_checksum(row)).sum::<i64>(),
                Ordering::Relaxed,
            );
        }
    } else {
        // Multi-threaded: divide iterations across clients.
        let iters_per_client = args.iterations / args.clients;
        let remainder = args.iterations % args.clients;
        let mut handles = Vec::new();

        for client_idx in 0..args.clients {
            let db = Arc::clone(&db);
            let total_rows = Arc::clone(&total_rows);
            let checksum = Arc::clone(&checksum);
            let iters = iters_per_client + if client_idx < remainder { 1 } else { 0 };
            let client_id = client_idx as u32 + 10; // offset to avoid collision with setup
            let query = args.query.clone();

            handles.push(std::thread::spawn(move || -> Result<(), String> {
                for _ in 0..iters {
                    let result = db
                        .execute(client_id, &query)
                        .map_err(|e| format!("{e:?}"))?;
                    let StatementResult::Query { rows, .. } = result else {
                        return Err("expected query result".into());
                    };
                    total_rows.fetch_add(rows.len(), Ordering::Relaxed);
                    checksum.fetch_add(
                        rows.iter().map(|row| row_checksum(row)).sum::<i64>(),
                        Ordering::Relaxed,
                    );
                }
                Ok(())
            }));
        }

        for h in handles {
            h.join().map_err(|_| "thread panicked")??;
        }
    }

    let elapsed = started.elapsed();
    let stats = db.pool.usage_stats();
    let total_rows = total_rows.load(Ordering::Relaxed);
    let checksum = checksum.load(Ordering::Relaxed);

    println!("engine: pgrust-direct");
    println!("base_dir: {}", args.base_dir.display());
    println!("rows: {}", args.row_count);
    println!("iterations: {}", args.iterations);
    println!("clients: {}", args.clients);
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
    clients: usize,
    pause_before_scan_secs: u64,
    preserve_existing: bool,
    skip_load: bool,
    wal_replay: bool,
    query: String,
    count: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_full_scan_bench"),
        row_count: 10_000,
        iterations: 100,
        pool_size: 16384, // 128MB at 8KB per page
        clients: 1,
        pause_before_scan_secs: 0,
        preserve_existing: false,
        skip_load: false,
        wal_replay: false,
        query: "select * from scanbench".to_string(),
        count: false,
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
            "--wal-replay" => {
                args.wal_replay = true;
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
            "--clients" => {
                args.clients = take_value(&raw, &mut i, "--clients")?
                    .parse()
                    .map_err(|_| "invalid --clients value".to_string())?;
            }
            "--pause" => {
                args.pause_before_scan_secs = take_value(&raw, &mut i, "--pause")?
                    .parse()
                    .map_err(|_| "invalid --pause value".to_string())?;
            }
            "--query" => {
                args.query = take_value(&raw, &mut i, "--query")?;
            }
            "--count" => {
                args.count = true;
                args.query = "select count(*) from scanbench".to_string();
                i += 1;
            }
            "--count-where" => {
                args.count = true;
                args.query = "select count(*) from scanbench where id > 0".to_string();
                i += 1;
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
                    args.pause_before_scan_secs =
                        raw[i + 4].parse().map_err(|_| "invalid pause")?;
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
        Value::Int16(v) => *v as i64,
        Value::Int32(v) => *v as i64,
        Value::Int64(v) => *v,
        Value::Float64(v) => *v as i64,
        Value::Numeric(v) => v.render().bytes().map(i64::from).sum(),
        Value::Json(v) => v.bytes().map(i64::from).sum(),
        Value::Jsonb(v) => v.iter().copied().map(i64::from).sum(),
        Value::JsonPath(v) => v.bytes().map(i64::from).sum(),
        Value::Bit(v) => v.render().bytes().map(i64::from).sum(),
        Value::Bytea(v) => v.iter().copied().map(i64::from).sum(),
        Value::Text(v) => v.bytes().map(i64::from).sum(),
        Value::TextRef(_, _) => value.as_text().unwrap().bytes().map(i64::from).sum(),
        Value::InternalChar(v) => i64::from(*v),
        Value::Bool(v) => i64::from(*v),
        Value::Array(items) => items.iter().map(value_checksum).sum(),
        Value::Null => 0,
    }
}
