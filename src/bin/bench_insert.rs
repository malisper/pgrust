use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::Instant;

use pgrust::compact_string::CompactString;
use pgrust::database::{Database, Session};
use pgrust::executor::Value;

fn main() -> Result<(), String> {
    let args = parse_args()?;

    let _ = std::fs::remove_dir_all(&args.base_dir);
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    let mut session = Session::new(1);

    session.execute(&db, "create table insertbench (id int not null, payload text not null)")
        .map_err(|e| format!("{e:?}"))?;

    if args.wait {
        eprintln!("READY pid={}", std::process::id());
        unsafe { libc::raise(libc::SIGSTOP); }
    }

    let columns = vec!["id".to_string(), "payload".to_string()];
    let prepared = session
        .prepare_insert(&db, "insertbench", Some(&columns), 2)
        .map_err(|e| format!("{e:?}"))?;

    let started = Instant::now();
    if args.autocommit {
        for i in 0..args.row_count {
            db.execute(
                1,
                &format!("insert into insertbench (id, payload) values ({i}, 'row-{i}')"),
            )
            .map_err(|e| format!("{e:?}"))?;
        }
    } else {
        session.execute(&db, "begin").map_err(|e| format!("{e:?}"))?;
        for i in 0..args.row_count {
            let params = [
                Value::Int32(i as i32),
                Value::Text(CompactString::from_owned(format!("row-{i}"))),
            ];
            session
                .execute_prepared_insert(&db, &prepared, &params)
                .map_err(|e| format!("{e:?}"))?;
        }
        session.execute(&db, "commit").map_err(|e| format!("{e:?}"))?;
    }
    let elapsed = started.elapsed();

    println!("engine: pgrust-direct");
    println!("base_dir: {}", args.base_dir.display());
    println!("rows: {}", args.row_count);
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "avg_ms_per_insert: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / args.row_count as f64
    );
    println!(
        "inserts_per_sec: {:.0}",
        args.row_count as f64 / elapsed.as_secs_f64()
    );
    Ok(())
}

struct Args {
    base_dir: PathBuf,
    row_count: usize,
    pool_size: usize,
    autocommit: bool,
    wait: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_insert_bench"),
        row_count: 12_000_000,
        pool_size: 16384,
        autocommit: false,
        wait: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--autocommit" => {
                args.autocommit = true;
                i += 1;
            }
            "--wait" => {
                args.wait = true;
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
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            other if other.starts_with("--") => {
                return Err(format!("unknown flag: {other}"));
            }
            _ => {
                return Err(format!("unexpected argument: {}", raw[i]));
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
