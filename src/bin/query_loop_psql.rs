use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

struct Config {
    host: String,
    port: u16,
    user: String,
    dbname: String,
    table: String,
    count: usize,
    query: Option<String>,
    quiet: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --bin query_loop_psql -- --table TABLE --count N [options]

Options:
  --host HOST         PostgreSQL host (default: 127.0.0.1)
  --port PORT         PostgreSQL port (default: 5432)
  --user USER         PostgreSQL user (default: postgres)
  --db DBNAME         Database name (default: postgres)
  --table TABLE       Table name used by the default query
  --count N           Number of times to run the query
  --query SQL         SQL to run instead of 'select * from TABLE;'
  --quiet             Suppress psql output

Examples:
  cargo run --bin query_loop_psql -- --port 5545 --table bench_select --count 500
  cargo run --bin query_loop_psql -- --port 5545 --count 500 --query 'select count(*) from bench_select;'"
    );
    std::process::exit(2);
}

fn parse_args() -> Config {
    let mut host = "127.0.0.1".to_string();
    let mut port = 5432u16;
    let mut user = "postgres".to_string();
    let mut dbname = "postgres".to_string();
    let mut table = None;
    let mut count = None;
    let mut query = None;
    let mut quiet = false;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--host" => host = args.next().unwrap_or_else(|| usage()),
            "--port" => {
                let value = args.next().unwrap_or_else(|| usage());
                port = value.parse().unwrap_or_else(|_| usage());
            }
            "--user" => user = args.next().unwrap_or_else(|| usage()),
            "--db" | "--dbname" => dbname = args.next().unwrap_or_else(|| usage()),
            "--table" => table = Some(args.next().unwrap_or_else(|| usage())),
            "--count" => {
                let value = args.next().unwrap_or_else(|| usage());
                count = Some(value.parse().unwrap_or_else(|_| usage()));
            }
            "--query" => query = Some(args.next().unwrap_or_else(|| usage())),
            "--quiet" => quiet = true,
            "-h" | "--help" => usage(),
            _ => {
                eprintln!("Unknown argument: {arg}");
                usage();
            }
        }
    }

    let table = table.unwrap_or_else(|| {
        if query.is_none() {
            eprintln!("--table is required unless --query is provided");
            usage();
        }
        String::new()
    });
    let count = count.unwrap_or_else(|| {
        eprintln!("--count is required");
        usage();
    });

    Config {
        host,
        port,
        user,
        dbname,
        table,
        count,
        query,
        quiet,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse_args();
    let query = cfg
        .query
        .clone()
        .unwrap_or_else(|| format!("select * from {};", cfg.table));
    let query = if query.trim_end().ends_with(';') {
        query
    } else {
        format!("{query};")
    };

    let mut workload = String::with_capacity(query.len() * cfg.count.saturating_add(1));
    for _ in 0..cfg.count {
        workload.push_str(&query);
        workload.push('\n');
    }

    let workload_path: PathBuf = env::temp_dir().join(format!(
        "pgrust-query-loop-{}-{}.sql",
        std::process::id(),
        cfg.port
    ));
    fs::write(&workload_path, workload)?;

    let mut cmd = Command::new("psql");
    cmd.arg("-X")
        .arg("-h")
        .arg(&cfg.host)
        .arg("-p")
        .arg(cfg.port.to_string())
        .arg("-U")
        .arg(&cfg.user)
        .arg("-d")
        .arg(&cfg.dbname)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-f")
        .arg(&workload_path);

    if cfg.quiet {
        cmd.arg("-q").stdout(Stdio::null()).stderr(Stdio::inherit());
    }

    let status = cmd.status()?;
    let _ = fs::remove_file(&workload_path);

    if !status.success() {
        return Err(format!("psql exited with status {status}").into());
    }

    println!(
        "executed {} queries using {}",
        cfg.count,
        workload_path.display()
    );
    Ok(())
}
