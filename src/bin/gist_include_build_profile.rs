use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use pgrust::Database;

struct Args {
    base_dir: PathBuf,
    rows: usize,
    pool_size: usize,
    sleep_before_index_ms: u64,
    include: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin gist_include_build_profile -- [options]

Options:
  --dir DIR                  Data dir (default: /tmp/pgrust_gist_include_build_profile)
  --rows N                   Rows to insert before CREATE INDEX (default: 8000)
  --pool-size N              Buffer pool size (default: 16384)
  --sleep-before-index-ms N  Sleep before CREATE INDEX for profiler attach (default: 0)
  --no-include               Build gist(c4) instead of gist(c4) INCLUDE (c1,c2,c3)"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_gist_include_build_profile"),
        rows: 8000,
        pool_size: 16_384,
        sleep_before_index_ms: 0,
        include: true,
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
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--sleep-before-index-ms" => {
                args.sleep_before_index_ms = take_value(&raw, &mut i, "--sleep-before-index-ms")?
                    .parse()
                    .map_err(|_| "invalid --sleep-before-index-ms value".to_string())?;
            }
            "--no-include" => {
                args.include = false;
                i += 1;
            }
            "-h" | "--help" => usage(),
            other if other.starts_with("--") => return Err(format!("unknown flag: {other}")),
            other => return Err(format!("unexpected argument: {other}")),
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

fn exec(db: &Database, sql: &str) -> Result<(), String> {
    db.execute(1, sql)
        .map(|_| ())
        .map_err(|err| format!("{err:?}"))
}

fn main() -> Result<(), String> {
    let args = parse_args()?;

    let _ = std::fs::remove_dir_all(&args.base_dir);
    std::fs::create_dir_all(&args.base_dir).map_err(|err| err.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|err| format!("{err:?}"))?;

    let setup_start = Instant::now();
    exec(
        &db,
        "create table tbl_gist (c1 int, c2 int, c3 int, c4 box)",
    )?;
    exec(
        &db,
        &format!(
            "insert into tbl_gist \
             select x, 2*x, 3*x, box(point(x,x+1),point(2*x,2*x+1)) \
             from generate_series(1,{}) as x",
            args.rows
        ),
    )?;
    let setup_elapsed = setup_start.elapsed();

    let index_sql = if args.include {
        "create index tbl_gist_idx on tbl_gist using gist (c4) include (c1,c2,c3)"
    } else {
        "create index tbl_gist_idx on tbl_gist using gist (c4)"
    };

    println!("pid: {}", std::process::id());
    println!("rows: {}", args.rows);
    println!("include: {}", args.include);
    println!("data_dir: {}", args.base_dir.display());
    println!("setup_ms: {:.3}", setup_elapsed.as_secs_f64() * 1000.0);
    println!("index_sql: {index_sql}");

    if args.sleep_before_index_ms > 0 {
        println!("sleeping_before_index_ms: {}", args.sleep_before_index_ms);
        std::thread::sleep(Duration::from_millis(args.sleep_before_index_ms));
    }

    let index_start = Instant::now();
    exec(&db, index_sql)?;
    let index_elapsed = index_start.elapsed();
    println!("index_ms: {:.3}", index_elapsed.as_secs_f64() * 1000.0);
    Ok(())
}
