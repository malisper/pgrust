use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use pgrust::pgrust::database::Database;
use pgrust::pgrust::server::serve;

struct Args {
    base_dir: PathBuf,
    iterations: usize,
    pool_size: usize,
    port: u16,
    query: String,
    preserve_existing: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin inproc_query_bench -- [options]

Options:
  --dir DIR               Data dir (default: /tmp/pgrust_inproc_query_bench)
  --iterations N          Number of query executions (default: 100)
  --pool-size N           Buffer pool size (default: 16384)
  --port PORT             Server port (default: 5549)
  --query SQL             Query to execute (default: select 1)
  --preserve-existing     Keep existing data dir contents

Behavior:
  Starts pgrust in-process on a background thread, then runs the query
  through psql. Each iteration is a separate psql invocation.

Examples:
  cargo run --release --bin inproc_query_bench --
  cargo run --release --bin inproc_query_bench -- --iterations 500
  cargo run --release --bin inproc_query_bench -- --query 'select 1'"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_inproc_query_bench"),
        iterations: 100,
        pool_size: 16_384,
        port: 5549,
        query: "select 1".to_string(),
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
            "--port" => {
                args.port = take_value(&raw, &mut i, "--port")?
                    .parse()
                    .map_err(|_| "invalid --port value".to_string())?;
            }
            "--query" => args.query = take_value(&raw, &mut i, "--query")?,
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

fn raise_fd_limit() {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let target = 10240u64.min(rlim.rlim_max);
            if rlim.rlim_cur < target {
                rlim.rlim_cur = target;
                libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
            }
        }
    }
}

fn psql_command(port: u16, query: &str) -> Command {
    let mut cmd = Command::new("psql");
    cmd.env("PGPASSWORD", "x")
        .arg("-X")
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .arg("-U")
        .arg("postgres")
        .arg("-t")
        .arg("-A")
        .arg("-c")
        .arg(query);
    cmd
}

fn wait_for_server(port: u16, startup_rx: &mpsc::Receiver<String>) -> Result<(), String> {
    let start = Instant::now();
    let ready_deadline = Duration::from_secs(15);
    while start.elapsed() < ready_deadline {
        if psql_command(port, "select 1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok(());
        }

        if let Ok(err) = startup_rx.try_recv() {
            return Err(format!("server exited during startup: {err}"));
        }

        thread::sleep(Duration::from_millis(100));
    }

    if let Ok(err) = startup_rx.try_recv() {
        Err(format!("server failed to become ready: {err}"))
    } else {
        Err(format!(
            "server did not become ready on port {port} within {}s",
            ready_deadline.as_secs()
        ))
    }
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    raise_fd_limit();

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    let addr = format!("127.0.0.1:{}", args.port);
    let (startup_tx, startup_rx) = mpsc::channel::<String>();
    thread::spawn(move || {
        if let Err(err) = serve(&addr, db) {
            let _ = startup_tx.send(err.to_string());
        }
    });

    wait_for_server(args.port, &startup_rx)?;

    let started = Instant::now();
    for _ in 0..args.iterations {
        let status = psql_command(args.port, &args.query)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| format!("run psql: {e}"))?;
        if !status.success() {
            return Err(format!("psql exited with status {status}"));
        }
    }

    let elapsed = started.elapsed();
    println!("engine: pgrust-psql-loop");
    println!("base_dir: {}", args.base_dir.display());
    println!("query: {}", args.query);
    println!("iterations: {}", args.iterations);
    println!("port: {}", args.port);
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "avg_ms_per_query: {:.3}",
        elapsed.as_secs_f64() * 1000.0 / args.iterations as f64
    );

    Ok(())
}
