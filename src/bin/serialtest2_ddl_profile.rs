use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use pgrust::{Database, Session};
use std::path::PathBuf;
use std::time::Instant;

struct Args {
    base_dir: PathBuf,
    iterations: usize,
    pool_size: usize,
    preserve_existing: bool,
    regression_prefix: bool,
    table_name: String,
    wait: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: serialtest2_ddl_profile [options]

Options:
  --dir DIR             Data dir (default: /tmp/pgrust_serialtest2_ddl_profile)
  --iterations N        Number of CREATE TABLE statements (default: 1)
  --pool-size N         Buffer pool size (default: 16384)
  --preserve-existing   Keep existing data dir contents
  --regression-prefix   Run sequence.sql statements before serialTest2
  --table-name NAME     Table name for first iteration (default: serialtest2_profile)
  --wait                Print PID and SIGSTOP before workload"
    );
    std::process::exit(2);
}

fn take_value(args: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    if *index >= args.len() {
        return Err(format!("{flag} requires a value"));
    }
    let value = args[*index].clone();
    *index += 1;
    Ok(value)
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_serialtest2_ddl_profile"),
        iterations: 1,
        pool_size: 16_384,
        preserve_existing: false,
        regression_prefix: false,
        table_name: "serialtest2_profile".into(),
        wait: false,
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
            "--regression-prefix" => {
                args.regression_prefix = true;
                i += 1;
            }
            "--table-name" => args.table_name = take_value(&raw, &mut i, "--table-name")?,
            "--wait" => {
                args.wait = true;
                i += 1;
            }
            "-h" | "--help" => usage(),
            other if other.starts_with("--") => return Err(format!("unknown flag: {other}")),
            _ => usage(),
        }
    }

    Ok(args)
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|error| error.to_string())?;

    let db =
        Database::open(&args.base_dir, args.pool_size).map_err(|error| format!("{error:?}"))?;
    let mut session = Session::new(1);

    println!("pid: {}", std::process::id());
    println!("base_dir: {}", args.base_dir.display());
    println!("iterations: {}", args.iterations);
    println!("regression_prefix: {}", args.regression_prefix);
    println!("table_name: {}", args.table_name);
    if args.wait {
        eprintln!("READY pid={}", std::process::id());
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
    }

    if args.regression_prefix {
        run_regression_prefix(&db, &mut session);
    }

    let total_start = Instant::now();
    for iteration in 0..args.iterations {
        let table = if iteration == 0 {
            args.table_name.clone()
        } else {
            format!("{}_{}", args.table_name, iteration)
        };
        let sql = format!(
            "CREATE TABLE {table} (f1 text, f2 serial, f3 smallserial, f4 serial2, f5 bigserial, f6 serial8)"
        );
        let start = Instant::now();
        session
            .execute(&db, &sql)
            .map_err(|error| format!("{error:?}"))?;
        println!(
            "iteration={} create_ms={:.3}",
            iteration,
            start.elapsed().as_secs_f64() * 1000.0
        );
    }

    println!(
        "total_ms={:.3}",
        total_start.elapsed().as_secs_f64() * 1000.0
    );
    Ok(())
}

fn run_regression_prefix(db: &Database, session: &mut Session) {
    let statements = [
        "CREATE SEQUENCE sequence_testx INCREMENT BY 0",
        "CREATE SEQUENCE sequence_testx INCREMENT BY -1 MINVALUE 20",
        "CREATE SEQUENCE sequence_testx INCREMENT BY 1 MAXVALUE -20",
        "CREATE SEQUENCE sequence_testx INCREMENT BY -1 START 10",
        "CREATE SEQUENCE sequence_testx INCREMENT BY 1 START -10",
        "CREATE SEQUENCE sequence_testx CACHE 0",
        "CREATE SEQUENCE sequence_testx OWNED BY nobody",
        "CREATE SEQUENCE sequence_testx OWNED BY pg_class_oid_index.oid",
        "CREATE SEQUENCE sequence_testx OWNED BY pg_class.relname",
        "CREATE TABLE sequence_test_table (a int)",
        "CREATE SEQUENCE sequence_testx OWNED BY sequence_test_table.b",
        "DROP TABLE sequence_test_table",
        "CREATE SEQUENCE sequence_test5 AS integer",
        "CREATE SEQUENCE sequence_test6 AS smallint",
        "CREATE SEQUENCE sequence_test7 AS bigint",
        "CREATE SEQUENCE sequence_test8 AS integer MAXVALUE 100000",
        "CREATE SEQUENCE sequence_test9 AS integer INCREMENT BY -1",
        "CREATE SEQUENCE sequence_test10 AS integer MINVALUE -100000 START 1",
        "CREATE SEQUENCE sequence_test11 AS smallint",
        "CREATE SEQUENCE sequence_test12 AS smallint INCREMENT -1",
        "CREATE SEQUENCE sequence_test13 AS smallint MINVALUE -32768",
        "CREATE SEQUENCE sequence_test14 AS smallint MAXVALUE 32767 INCREMENT -1",
        "CREATE SEQUENCE sequence_testx AS text",
        "CREATE SEQUENCE sequence_testx AS nosuchtype",
        "CREATE SEQUENCE sequence_testx AS smallint MAXVALUE 100000",
        "CREATE SEQUENCE sequence_testx AS smallint MINVALUE -100000",
        "ALTER SEQUENCE sequence_test5 AS smallint",
        "ALTER SEQUENCE sequence_test8 AS smallint",
        "ALTER SEQUENCE sequence_test8 AS smallint MAXVALUE 20000",
        "ALTER SEQUENCE sequence_test9 AS smallint",
        "ALTER SEQUENCE sequence_test10 AS smallint",
        "ALTER SEQUENCE sequence_test10 AS smallint MINVALUE -20000",
        "ALTER SEQUENCE sequence_test11 AS int",
        "ALTER SEQUENCE sequence_test12 AS int",
        "ALTER SEQUENCE sequence_test13 AS int",
        "ALTER SEQUENCE sequence_test14 AS int",
        "CREATE TABLE serialTest1 (f1 text, f2 serial)",
        "INSERT INTO serialTest1 VALUES ('foo')",
        "INSERT INTO serialTest1 VALUES ('bar')",
        "INSERT INTO serialTest1 VALUES ('force', 100)",
        "INSERT INTO serialTest1 VALUES ('wrong', NULL)",
        "SELECT * FROM serialTest1",
        "SELECT pg_get_serial_sequence('serialTest1', 'f2')",
    ];

    let start = Instant::now();
    let mut ok = 0usize;
    let mut err = 0usize;
    for statement in statements {
        match session.execute(db, statement) {
            Ok(_) => ok += 1,
            Err(_) => err += 1,
        }
    }
    println!(
        "prefix ok={} err={} ms={:.3}",
        ok,
        err,
        start.elapsed().as_secs_f64() * 1000.0
    );
}
