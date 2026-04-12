use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::Instant;

use pgrust::executor::StatementResult;
use pgrust::pgrust::database::Database;

struct Args {
    base_dir: PathBuf,
    row_count: usize,
    iterations: usize,
    pool_size: usize,
    table_name: String,
    query: String,
    preserve_existing: bool,
    skip_load: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin inproc_query_bench -- [options]

Options:
  --dir DIR               Data dir (default: /tmp/pgrust_inproc_query_bench)
  --rows N                Number of rows to seed (default: 10000)
  --iterations N          Number of query executions (default: 100)
  --pool-size N           Buffer pool size (default: 16384)
  --table NAME            Seed table name (default: bench_select)
  --query SQL             Query to execute (default: select * from TABLE)
  --preserve-existing     Keep existing data dir contents
  --skip-load             Skip DROP/CREATE/INSERT setup

Examples:
  cargo run --release --bin inproc_query_bench -- --dir /tmp/pgrust_count_profile2 --rows 1000000 --iterations 200
  cargo run --release --bin inproc_query_bench -- --dir /tmp/pgrust_count_profile2 --rows 1000000 --iterations 200 --query 'select count(*) from bench_select;'"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_inproc_query_bench"),
        row_count: 10_000,
        iterations: 100,
        pool_size: 16_384,
        table_name: "bench_select".to_string(),
        query: String::new(),
        preserve_existing: false,
        skip_load: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--dir" => args.base_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?),
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
            "--table" => args.table_name = take_value(&raw, &mut i, "--table")?,
            "--query" => args.query = take_value(&raw, &mut i, "--query")?,
            "--preserve-existing" => {
                args.preserve_existing = true;
                i += 1;
            }
            "--skip-load" => {
                args.skip_load = true;
                i += 1;
            }
            "-h" | "--help" => usage(),
            other if other.starts_with("--") => return Err(format!("unknown flag: {other}")),
            _ => usage(),
        }
    }

    if args.query.is_empty() {
        args.query = format!("select * from {}", args.table_name);
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

fn setup_table(db: &Database, table_name: &str, row_count: usize) -> Result<(), String> {
    db.execute(1, &format!("drop table if exists {table_name}"))
        .map_err(|e| format!("{e:?}"))?;
    db.execute(
        1,
        &format!("create table {table_name} (id int4 not null, payload text not null)"),
    )
    .map_err(|e| format!("{e:?}"))?;

    db.execute(1, "begin").map_err(|e| format!("{e:?}"))?;
    for i in 1..=row_count {
        db.execute(
            1,
            &format!("insert into {table_name} (id, payload) values ({i}, 'row-{i}')"),
        )
        .map_err(|e| format!("{e:?}"))?;
    }
    db.execute(1, "commit").map_err(|e| format!("{e:?}"))?;
    Ok(())
}

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;

    if !args.skip_load {
        setup_table(&db, &args.table_name, args.row_count)?;
    }

    db.pool.reset_usage_stats();
    let started = Instant::now();
    let mut total_rows = 0usize;

    for _ in 0..args.iterations {
        let result = db.execute(2, &args.query).map_err(|e| format!("{e:?}"))?;
        let StatementResult::Query { rows, .. } = result else {
            return Err("expected query result".into());
        };
        total_rows += rows.len();
    }

    let elapsed = started.elapsed();
    let stats = db.pool.usage_stats();

    println!("engine: pgrust-inproc");
    println!("base_dir: {}", args.base_dir.display());
    println!("table: {}", args.table_name);
    println!("query: {}", args.query);
    println!("rows_seeded: {}", args.row_count);
    println!("iterations: {}", args.iterations);
    println!("total_rows_seen: {}", total_rows);
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "avg_ms_per_query: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / args.iterations as f64
    );
    println!("buffer_hits: {}", stats.shared_hit);
    println!("buffer_reads: {}", stats.shared_read);
    println!("buffer_written: {}", stats.shared_written);

    Ok(())
}
