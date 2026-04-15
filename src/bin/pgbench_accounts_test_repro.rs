use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use pgrust::ClientId;
use pgrust::executor::StatementResult;
use pgrust::pgrust::database::Database;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

struct Args {
    base_dir: PathBuf,
    pool_size: usize,
    accounts: usize,
    clients: usize,
    ops_per_client: usize,
    load_only: bool,
    preserve_existing: bool,
    skip_load: bool,
    pause_before_workload_secs: u64,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin pgbench_accounts_test_repro -- [options]

Reproduces the workload shape from
`pgrust::database::tests::pgbench_style_accounts_workload_completes`.

Options:
  --dir DIR               Data dir (default: /tmp/pgrust_pgbench_accounts_repro)
  --pool-size N           Buffer pool size (default: 128)
  --accounts N            Number of pgbench_accounts rows (default: 5000)
  --clients N             Number of worker threads (default: 10)
  --ops N                 Operations per client (default: 10)
  --load-only             Load data and exit before running the workload
  --pause N               Sleep N seconds before the timed workload
  --preserve-existing     Keep existing data dir contents
  --skip-load             Reuse existing pgbench_accounts table contents

Examples:
  cargo run --release --bin pgbench_accounts_test_repro --
  cargo run --release --bin pgbench_accounts_test_repro -- --clients 20 --ops 50
  cargo run --release --bin pgbench_accounts_test_repro -- --pause 5"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_pgbench_accounts_repro"),
        pool_size: 128,
        accounts: 5_000,
        clients: 10,
        ops_per_client: 10,
        load_only: false,
        preserve_existing: false,
        skip_load: false,
        pause_before_workload_secs: 0,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--dir" => args.base_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?),
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--accounts" => {
                args.accounts = take_value(&raw, &mut i, "--accounts")?
                    .parse()
                    .map_err(|_| "invalid --accounts value".to_string())?;
            }
            "--clients" => {
                args.clients = take_value(&raw, &mut i, "--clients")?
                    .parse()
                    .map_err(|_| "invalid --clients value".to_string())?;
            }
            "--ops" => {
                args.ops_per_client = take_value(&raw, &mut i, "--ops")?
                    .parse()
                    .map_err(|_| "invalid --ops value".to_string())?;
            }
            "--load-only" => {
                args.load_only = true;
                i += 1;
            }
            "--pause" => {
                args.pause_before_workload_secs = take_value(&raw, &mut i, "--pause")?
                    .parse()
                    .map_err(|_| "invalid --pause value".to_string())?;
            }
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

    if args.clients == 0 {
        return Err("--clients must be greater than 0".to_string());
    }
    if args.accounts == 0 {
        return Err("--accounts must be greater than 0".to_string());
    }
    if args.pool_size == 0 {
        return Err("--pool-size must be greater than 0".to_string());
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

fn main() -> Result<(), String> {
    let args = parse_args()?;

    if !args.preserve_existing {
        let _ = std::fs::remove_dir_all(&args.base_dir);
    }
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open_with_options(&args.base_dir, args.pool_size, true)
        .map_err(|e| format!("{e:?}"))?;

    if !args.skip_load {
        load_accounts(&db, &args)?;
    }

    if args.load_only {
        println!("engine: pgrust-pgbench-accounts-repro");
        println!("base_dir: {}", args.base_dir.display());
        println!("load_only: true");
        return Ok(());
    }

    if args.pause_before_workload_secs > 0 {
        println!(
            "setup_complete: pausing {}s before workload",
            args.pause_before_workload_secs
        );
        thread::sleep(Duration::from_secs(args.pause_before_workload_secs));
    }

    let db = Arc::new(db);
    let started = Instant::now();
    let mut handles = Vec::with_capacity(args.clients);

    for t in 0..args.clients {
        let db = Arc::clone(&db);
        let accounts = args.accounts;
        let ops_per_client = args.ops_per_client;
        handles.push(thread::spawn(move || -> Result<(), String> {
            for i in 0..ops_per_client {
                let aid = ((t * 997 + i * 389) % accounts) + 1;
                db.execute(
                    (t + 2100) as ClientId,
                    &format!(
                        "update pgbench_accounts set abalance = abalance + -1 where aid = {aid}"
                    ),
                )
                .map_err(|e| format!("update failed for aid {aid}: {e:?}"))?;
                match db
                    .execute(
                        (t + 2200) as ClientId,
                        &format!("select abalance from pgbench_accounts where aid = {aid}"),
                    )
                    .map_err(|e| format!("select failed for aid {aid}: {e:?}"))?
                {
                    StatementResult::Query { rows, .. } if rows.len() == 1 => {}
                    StatementResult::Query { rows, .. } => {
                        return Err(format!(
                            "expected exactly 1 row for aid {aid}, got {}",
                            rows.len()
                        ));
                    }
                    other => return Err(format!("expected query result, got {other:?}")),
                }
            }
            Ok(())
        }));
    }

    for handle in handles {
        handle
            .join()
            .map_err(|e| format!("worker panicked: {e:?}"))??;
    }

    let elapsed = started.elapsed();
    let total_ops = args.clients * args.ops_per_client * 2;
    println!("engine: pgrust-pgbench-accounts-repro");
    println!("base_dir: {}", args.base_dir.display());
    println!("pool_size: {}", args.pool_size);
    println!("accounts: {}", args.accounts);
    println!("clients: {}", args.clients);
    println!("ops_per_client: {}", args.ops_per_client);
    println!("total_statements: {}", total_ops);
    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    println!(
        "statements_per_sec: {:.3}",
        total_ops as f64 / elapsed.as_secs_f64()
    );
    let stats = db.pool.usage_stats();
    println!("buffer_hits: {}", stats.shared_hit);
    println!("buffer_reads: {}", stats.shared_read);
    println!("buffer_written: {}", stats.shared_written);

    Ok(())
}

fn load_accounts(db: &Database, args: &Args) -> Result<(), String> {
    db.execute(1, "drop table if exists pgbench_accounts")
        .map_err(|e| format!("drop table failed: {e:?}"))?;
    db.execute(
        1,
        "create table pgbench_accounts (aid int4 not null, bid int4 not null, abalance int4 not null, filler text)",
    )
    .map_err(|e| format!("create table failed: {e:?}"))?;

    db.execute(1, "begin")
        .map_err(|e| format!("begin failed: {e:?}"))?;
    for aid in 1..=args.accounts {
        db.execute(
            1,
            &format!(
                "insert into pgbench_accounts (aid, bid, abalance, filler) values ({aid}, 1, 0, 'x')"
            ),
        )
        .map_err(|e| format!("insert failed for aid {aid}: {e:?}"))?;
    }
    db.execute(1, "commit")
        .map_err(|e| format!("commit failed: {e:?}"))?;
    Ok(())
}
