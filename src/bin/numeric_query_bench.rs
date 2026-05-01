use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use pgrust::Database;
use pgrust::backend::executor::StatementResult;

struct Args {
    base_dir: PathBuf,
    rows: usize,
    iterations: usize,
    variant: String,
    preserve_existing: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: numeric_query_bench [options]

Options:
  --dir DIR               Data dir (default: /tmp/pgrust_numeric_query_bench)
  --rows N                generate_series upper bound (default: 100000)
  --iterations N          Number of measured query executions (default: 20)
  --variant NAME          target, count, sum-int, sum-series-numeric, series-rows, variance-huge, or all (default: all)
  --preserve-existing     Keep existing data dir contents
"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_numeric_query_bench"),
        rows: 100_000,
        iterations: 20,
        variant: "all".into(),
        preserve_existing: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--dir" => args.base_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?),
            "--rows" => {
                args.rows = take_value(&raw, &mut i, "--rows")?
                    .parse()
                    .map_err(|_| "invalid --rows value".to_string())?;
            }
            "--iterations" => {
                args.iterations = take_value(&raw, &mut i, "--iterations")?
                    .parse()
                    .map_err(|_| "invalid --iterations value".to_string())?;
            }
            "--variant" => args.variant = take_value(&raw, &mut i, "--variant")?,
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

fn query_for_variant(variant: &str, rows: usize) -> Result<String, String> {
    Ok(match variant {
        "target" => format!("select sum(9999::numeric) from generate_series(1, {rows})"),
        "count" => format!("select count(*) from generate_series(1, {rows})"),
        "sum-int" => format!("select sum(9999::int4) from generate_series(1, {rows})"),
        "sum-series-numeric" => {
            format!("select sum(i::numeric) from generate_series(1, {rows}) as g(i)")
        }
        "series-rows" => format!("select * from generate_series(1, {rows})"),
        "variance-huge" => "select variance(a) from num_variance".to_string(),
        other => return Err(format!("unknown variant: {other}")),
    })
}

fn prepare_variant(db: &Database, variant: &str) -> Result<(), String> {
    if variant != "variance-huge" {
        return Ok(());
    }

    for query in [
        "drop table if exists num_variance",
        "create table num_variance (a numeric)",
        "insert into num_variance select 9e131071 + x from generate_series(1, 5) x",
    ] {
        run_query(db, query)?;
    }
    Ok(())
}

fn run_query(db: &Database, query: &str) -> Result<usize, String> {
    match db.execute(1, query).map_err(|err| format!("{err:?}"))? {
        StatementResult::Query { rows, .. } => Ok(rows.len()),
        StatementResult::AffectedRows(rows) => Ok(rows),
    }
}

fn timed_run(db: &Database, query: &str, iterations: usize) -> Result<(Duration, usize), String> {
    let mut result_rows = 0;
    let started = Instant::now();
    for _ in 0..iterations {
        result_rows = run_query(db, query)?;
    }
    Ok((started.elapsed(), result_rows))
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, 16_384).map_err(|err| format!("{err:?}"))?;
    let variants: Vec<&str> = if args.variant == "all" {
        vec![
            "count",
            "sum-int",
            "target",
            "sum-series-numeric",
            "series-rows",
        ]
    } else {
        vec![args.variant.as_str()]
    };

    println!("engine: pgrust-direct");
    println!("base_dir: {}", args.base_dir.display());
    println!("rows: {}", args.rows);
    println!("iterations: {}", args.iterations);

    for variant in variants {
        prepare_variant(&db, variant)?;
        let query = query_for_variant(variant, args.rows)?;
        run_query(&db, &query)?;
        let (elapsed, result_rows) = timed_run(&db, &query, args.iterations)?;
        let total_ms = elapsed.as_secs_f64() * 1000.0;
        println!(
            "variant={variant} result_rows={result_rows} total_ms={total_ms:.3} avg_ms={:.3} query={query}",
            total_ms / args.iterations as f64
        );
    }

    Ok(())
}
