use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::Instant;

use pgrust::DatabaseOpenOptions;
use pgrust::executor::{StatementResult, Value};
use pgrust::pgrust::database::{Database, Session};

struct Args {
    base_dir: PathBuf,
    iterations: usize,
    pool_size: usize,
    preserve_existing: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin comment_on_table_bench -- [options]

Options:
  --dir DIR               Data dir (default: /tmp/pgrust_comment_on_table_bench)
  --iterations N          Number of workload iterations (default: 200)
  --pool-size N           Buffer pool size (default: 16)
  --preserve-existing     Keep existing data dir contents
"
    );
    std::process::exit(2);
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

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_comment_on_table_bench"),
        iterations: 200,
        pool_size: 16,
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

fn query_rows(db: &Database, client_id: u32, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(client_id, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {other:?}"),
    }
}

fn run_iteration(base_dir: &PathBuf, pool_size: usize, iteration: usize) -> Result<(), String> {
    let iter_dir = base_dir.join(format!("iter_{iteration}"));
    let _ = std::fs::remove_dir_all(&iter_dir);
    std::fs::create_dir_all(&iter_dir).map_err(|e| e.to_string())?;

    let db = Database::open_with_options(&iter_dir, DatabaseOpenOptions::new(pool_size))
        .map_err(|e| format!("{e:?}"))?;
    let mut session = Session::new(1);
    let table_name = "items";
    let rolled_back = "rolled back";
    let committed = "committed";

    session
        .execute(
            &db,
            &format!("create table {table_name} (id int4 not null)"),
        )
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(&db, "begin")
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(
            &db,
            &format!("comment on table {table_name} is '{rolled_back}'"),
        )
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(&db, "rollback")
        .map_err(|e| format!("{e:?}"))?;

    let rollback_check = format!(
        "select count(*) \
         from pg_description d \
         join pg_class c on c.oid = d.objoid \
         where c.relname = '{table_name}' and d.classoid = 1259 and d.objsubid = 0"
    );
    if query_rows(&db, 1, &rollback_check) != vec![vec![Value::Int64(0)]] {
        return Err(format!("rollback check failed for {table_name}"));
    }

    session
        .execute(&db, "begin")
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(
            &db,
            &format!("comment on table {table_name} is '{committed}'"),
        )
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(&db, "commit")
        .map_err(|e| format!("{e:?}"))?;

    drop(session);
    drop(db);

    let reopened = Database::open(&iter_dir, pool_size).map_err(|e| format!("{e:?}"))?;
    let reopen_check = format!(
        "select d.description \
         from pg_description d \
         join pg_class c on c.oid = d.objoid \
         where c.relname = '{table_name}' and d.classoid = 1259 and d.objsubid = 0"
    );
    if query_rows(&reopened, 1, &reopen_check) != vec![vec![Value::Text(committed.into())]] {
        return Err(format!("reopen check failed for {table_name}"));
    }

    Ok(())
}

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let started = Instant::now();
    for iteration in 0..args.iterations {
        run_iteration(&args.base_dir, args.pool_size, iteration)?;
    }
    let elapsed = started.elapsed();

    println!("engine: pgrust-direct");
    println!("base_dir: {}", args.base_dir.display());
    println!("iterations: {}", args.iterations);
    println!("pool_size: {}", args.pool_size);
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "avg_ms_per_iteration: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / args.iterations as f64
    );

    Ok(())
}
