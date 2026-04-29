use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use pgrust::pgrust::database::Database;

struct Args {
    base_dir: PathBuf,
    rows: usize,
    iterations: usize,
    pool_size: usize,
    pause_before_alter_secs: u64,
    preserve_existing: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin btree_pk_build_profile -- [options]

Options:
  --dir DIR                  Data dir (default: /tmp/pgrust_btree_pk_build_profile)
  --rows N                   Rows inserted before ADD PRIMARY KEY (default: 80000)
  --iterations N             Number of fresh table/index builds (default: 1)
  --pool-size N              Buffer pool size (default: 16384)
  --pause-before-alter-secs N Sleep before ADD PRIMARY KEY so a profiler can attach
  --preserve-existing        Keep existing data dir contents"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_btree_pk_build_profile"),
        rows: 80_000,
        iterations: 1,
        pool_size: 16_384,
        pause_before_alter_secs: 0,
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
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--pause-before-alter-secs" => {
                args.pause_before_alter_secs =
                    take_value(&raw, &mut i, "--pause-before-alter-secs")?
                        .parse()
                        .map_err(|_| "invalid --pause-before-alter-secs value".to_string())?;
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

fn timed_execute(
    db: &Database,
    client_id: u32,
    label: &str,
    sql: &str,
) -> Result<Duration, String> {
    let started = Instant::now();
    db.execute(client_id, sql)
        .map_err(|err| format!("{label} failed: {err:?}"))?;
    let elapsed = started.elapsed();
    println!("{label}_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    Ok(elapsed)
}

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    println!("engine: pgrust-direct");
    println!("pid: {}", std::process::id());
    println!("base_dir: {}", args.base_dir.display());
    println!("rows: {}", args.rows);
    println!("iterations: {}", args.iterations);

    let total_started = Instant::now();
    let mut total_create = Duration::ZERO;
    let mut total_insert = Duration::ZERO;
    let mut total_alter = Duration::ZERO;

    for iteration in 0..args.iterations {
        let table = format!("delete_test_table_{iteration}");
        let client_id = iteration as u32 + 1;
        println!("iteration: {}", iteration + 1);

        total_create += timed_execute(
            &db,
            client_id,
            "create_table",
            &format!("CREATE TABLE {table} (a bigint, b bigint, c bigint, d bigint)"),
        )?;
        total_insert += timed_execute(
            &db,
            client_id,
            "insert_rows",
            &format!(
                "INSERT INTO {table} SELECT i, 1, 2, 3 FROM generate_series(1,{}) i",
                args.rows
            ),
        )?;

        if args.pause_before_alter_secs > 0 {
            println!(
                "ready_for_profile: pid={} pause_secs={}",
                std::process::id(),
                args.pause_before_alter_secs
            );
            std::thread::sleep(Duration::from_secs(args.pause_before_alter_secs));
        }

        db.pool.reset_usage_stats();
        total_alter += timed_execute(
            &db,
            client_id,
            "alter_add_primary_key",
            &format!("ALTER TABLE {table} ADD PRIMARY KEY (a,b,c,d)"),
        )?;
        let stats = db.pool.usage_stats();
        println!("alter_buffer_hits: {}", stats.shared_hit);
        println!("alter_buffer_reads: {}", stats.shared_read);
        println!("alter_buffer_written: {}", stats.shared_written);
    }

    let elapsed = total_started.elapsed();
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "total_create_table_ms: {:.3}",
        total_create.as_secs_f64() * 1000.0
    );
    println!(
        "total_insert_rows_ms: {:.3}",
        total_insert.as_secs_f64() * 1000.0
    );
    println!(
        "total_alter_add_primary_key_ms: {:.3}",
        total_alter.as_secs_f64() * 1000.0
    );

    Ok(())
}
