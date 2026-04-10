use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use pgrust::ClientId;
use pgrust::executor::{StatementResult, Value};
use pgrust::pgrust::database::{Database, Session};
use rand::Rng;
use std::env;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const NBRANCHES: i64 = 1;
const NTELLERS: i64 = 10;
const NACCOUNTS: i64 = 100_000;
const COPY_CHUNK_ROWS: usize = 5_000;

#[derive(Clone, Debug)]
struct Config {
    base_dir: PathBuf,
    pool_size: usize,
    clients: usize,
    duration: Option<Duration>,
    transactions_per_client: Option<usize>,
    scale: i64,
    init: bool,
    fillfactor: i32,
}

#[derive(Default)]
struct Totals {
    transactions: AtomicU64,
    failures: AtomicU64,
    latency_nanos: AtomicU64,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = Config::from_args(env::args().skip(1).collect())?;
    let db = Database::open(&config.base_dir, config.pool_size).map_err(|e| {
        format!(
            "failed to open database at {}: {e:?}",
            config.base_dir.display()
        )
    })?;

    if config.init {
        println!(
            "initializing pgbench-like schema in {} (scale={}, pool={})",
            config.base_dir.display(),
            config.scale,
            config.pool_size
        );
        initialize_schema(&db, &config)?;
    }

    if config.duration.is_none() && config.transactions_per_client.is_none() {
        return Ok(());
    }

    println!(
        "running workload (clients={}, scale={}, pool={})",
        config.clients, config.scale, config.pool_size
    );

    let totals = Arc::new(Totals::default());
    let barrier = Arc::new(Barrier::new(config.clients + 1));
    let deadline = config.duration.map(|d| Instant::now() + d);
    let wall_start = Instant::now();
    let mut handles = Vec::with_capacity(config.clients);

    for i in 0..config.clients {
        let db = db.clone();
        let cfg = config.clone();
        let totals = totals.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            worker(i as ClientId + 1, db, cfg, deadline, totals, barrier)
        }));
    }

    barrier.wait();

    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(format!("worker thread panicked: {e:?}")),
        }
    }

    let wall = wall_start.elapsed();
    let transactions = totals.transactions.load(Ordering::Relaxed);
    let failures = totals.failures.load(Ordering::Relaxed);
    let total_latency = Duration::from_nanos(totals.latency_nanos.load(Ordering::Relaxed));
    let avg_latency_ms = if transactions > 0 {
        total_latency.as_secs_f64() * 1000.0 / transactions as f64
    } else {
        0.0
    };
    let tps = if wall.as_secs_f64() > 0.0 {
        transactions as f64 / wall.as_secs_f64()
    } else {
        0.0
    };

    println!("transactions: {transactions}");
    println!("failures: {failures}");
    println!("wall time: {:.3} s", wall.as_secs_f64());
    println!("avg latency: {:.3} ms", avg_latency_ms);
    println!("tps: {:.3}", tps);

    let stats = db.pool.usage_stats();
    println!("buffer hits: {}", stats.shared_hit);
    println!("buffer reads: {}", stats.shared_read);
    println!("buffer written: {}", stats.shared_written);

    Ok(())
}

fn worker(
    client_id: ClientId,
    db: Database,
    config: Config,
    deadline: Option<Instant>,
    totals: Arc<Totals>,
    barrier: Arc<Barrier>,
) -> Result<(), String> {
    let mut session = Session::new(client_id);
    let mut rng = rand::thread_rng();
    let mut completed = 0usize;

    barrier.wait();

    loop {
        if let Some(max_txns) = config.transactions_per_client {
            if completed >= max_txns {
                break;
            }
        }
        if let Some(deadline) = deadline {
            if Instant::now() >= deadline {
                break;
            }
        }

        let started = Instant::now();
        match run_tpcb_like_transaction(&db, &mut session, &config, &mut rng) {
            Ok(()) => {
                completed += 1;
                totals.transactions.fetch_add(1, Ordering::Relaxed);
                totals.latency_nanos.fetch_add(
                    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                    Ordering::Relaxed,
                );
            }
            Err(e) => {
                totals.failures.fetch_add(1, Ordering::Relaxed);
                let _ = session.execute(&db, "rollback");
                if totals.failures.load(Ordering::Relaxed) <= 3 {
                    eprintln!("client {client_id} transaction failed: {e}");
                }
            }
        }
    }

    Ok(())
}

fn run_tpcb_like_transaction<R: Rng>(
    db: &Database,
    session: &mut Session,
    config: &Config,
    rng: &mut R,
) -> Result<(), String> {
    let aid = rng.gen_range(1..=NACCOUNTS * config.scale);
    let bid = rng.gen_range(1..=NBRANCHES * config.scale);
    let tid = rng.gen_range(1..=NTELLERS * config.scale);
    let delta = rng.gen_range(-5000..=5000);

    execute_expect_affected(session, db, "begin")?;
    execute_expect_affected(
        session,
        db,
        &format!("update pgbench_accounts set abalance = abalance + {delta} where aid = {aid}"),
    )?;
    execute_expect_select(
        session,
        db,
        &format!("select abalance from pgbench_accounts where aid = {aid}"),
    )?;
    execute_expect_affected(
        session,
        db,
        &format!("update pgbench_tellers set tbalance = tbalance + {delta} where tid = {tid}"),
    )?;
    execute_expect_affected(
        session,
        db,
        &format!("update pgbench_branches set bbalance = bbalance + {delta} where bid = {bid}"),
    )?;
    execute_expect_affected(
        session,
        db,
        &format!(
            "insert into pgbench_history (tid, bid, aid, delta, mtime) values ({tid}, {bid}, {aid}, {delta}, current_timestamp)"
        ),
    )?;
    execute_expect_affected(session, db, "commit")?;
    Ok(())
}

fn execute_expect_affected(session: &mut Session, db: &Database, sql: &str) -> Result<(), String> {
    match session.execute(db, sql) {
        Ok(StatementResult::AffectedRows(_)) => Ok(()),
        Ok(StatementResult::Query { .. }) => {
            Err(format!("expected affected-rows result for: {sql}"))
        }
        Err(e) => Err(format!("{sql}: {e:?}")),
    }
}

fn execute_expect_select(session: &mut Session, db: &Database, sql: &str) -> Result<i32, String> {
    match session.execute(db, sql) {
        Ok(StatementResult::Query { rows, .. }) => {
            let row = rows
                .first()
                .ok_or_else(|| format!("no rows returned for: {sql}"))?;
            let value = row
                .first()
                .ok_or_else(|| format!("empty row returned for: {sql}"))?;
            match value {
                Value::Int32(v) => Ok(*v),
                other => Err(format!("unexpected select result {other:?} for: {sql}")),
            }
        }
        Ok(StatementResult::AffectedRows(_)) => Err(format!("expected query result for: {sql}")),
        Err(e) => Err(format!("{sql}: {e:?}")),
    }
}

fn initialize_schema(db: &Database, config: &Config) -> Result<(), String> {
    db.execute(
        0,
        "drop table if exists pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers",
    )
    .map_err(|e| format!("drop tables failed: {e:?}"))?;
    db.execute(
        0,
        "create table pgbench_history(tid int,bid int,aid int,delta int,mtime timestamp,filler char(22))",
    )
    .map_err(|e| format!("create pgbench_history failed: {e:?}"))?;
    db.execute(
        0,
        &format!(
            "create table pgbench_tellers(tid int not null,bid int,tbalance int,filler char(84)) with (fillfactor={})",
            config.fillfactor
        ),
    )
    .map_err(|e| format!("create pgbench_tellers failed: {e:?}"))?;
    db.execute(
        0,
        &format!(
            "create table pgbench_accounts(aid int not null,bid int,abalance int,filler char(84)) with (fillfactor={})",
            config.fillfactor
        ),
    )
    .map_err(|e| format!("create pgbench_accounts failed: {e:?}"))?;
    db.execute(
        0,
        &format!(
            "create table pgbench_branches(bid int not null,bbalance int,filler char(88)) with (fillfactor={})",
            config.fillfactor
        ),
    )
    .map_err(|e| format!("create pgbench_branches failed: {e:?}"))?;

    let mut session = Session::new(0);
    execute_expect_affected(&mut session, db, "begin")?;
    execute_expect_affected(
        &mut session,
        db,
        "truncate table pgbench_accounts, pgbench_branches, pgbench_history, pgbench_tellers",
    )?;

    println!("copying pgbench_branches...");
    copy_rows_chunked(
        &mut session,
        db,
        "pgbench_branches",
        NBRANCHES * config.scale,
        |row| vec![(row + 1).to_string(), "0".to_string(), "\\N".to_string()],
    )?;
    println!("copying pgbench_tellers...");
    copy_rows_chunked(
        &mut session,
        db,
        "pgbench_tellers",
        NTELLERS * config.scale,
        |row| {
            vec![
                (row + 1).to_string(),
                (row / NTELLERS + 1).to_string(),
                "0".to_string(),
                "\\N".to_string(),
            ]
        },
    )?;
    println!("copying pgbench_accounts...");
    copy_rows_chunked(
        &mut session,
        db,
        "pgbench_accounts",
        NACCOUNTS * config.scale,
        |row| {
            vec![
                (row + 1).to_string(),
                (row / NACCOUNTS + 1).to_string(),
                "0".to_string(),
                String::new(),
            ]
        },
    )?;

    execute_expect_affected(&mut session, db, "commit")?;

    println!("initialized:");
    println!("  pgbench_branches: {}", NBRANCHES * config.scale);
    println!("  pgbench_tellers: {}", NTELLERS * config.scale);
    println!("  pgbench_accounts: {}", NACCOUNTS * config.scale);
    Ok(())
}

fn copy_rows_chunked<F>(
    session: &mut Session,
    db: &Database,
    table: &str,
    total_rows: i64,
    mut row_fn: F,
) -> Result<(), String>
where
    F: FnMut(i64) -> Vec<String>,
{
    let mut chunk = Vec::with_capacity(COPY_CHUNK_ROWS);
    for row in 0..total_rows {
        chunk.push(row_fn(row));
        if chunk.len() == COPY_CHUNK_ROWS {
            session
                .copy_from_rows(db, table, &chunk)
                .map_err(|e| format!("copy into {table} failed: {e:?}"))?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        session
            .copy_from_rows(db, table, &chunk)
            .map_err(|e| format!("copy into {table} failed: {e:?}"))?;
    }
    Ok(())
}

impl Config {
    fn from_args(args: Vec<String>) -> Result<Self, String> {
        let mut config = Self {
            base_dir: env::temp_dir().join("pgrust-pgbench-like"),
            pool_size: 128,
            clients: 10,
            duration: Some(Duration::from_secs(30)),
            transactions_per_client: None,
            scale: 1,
            init: false,
            fillfactor: 100,
        };

        let mut i = 0;
        while i < args.len() {
            let flag = &args[i];
            let next = |i: &mut usize| -> Result<String, String> {
                *i += 1;
                args.get(*i)
                    .cloned()
                    .ok_or_else(|| format!("missing value for {flag}"))
            };
            match flag.as_str() {
                "--base-dir" => config.base_dir = PathBuf::from(next(&mut i)?),
                "--pool-size" => {
                    config.pool_size = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid value for --pool-size".to_string())?
                }
                "--clients" => {
                    config.clients = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid value for --clients".to_string())?
                }
                "--time" => {
                    let seconds: u64 = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid value for --time".to_string())?;
                    config.duration = Some(Duration::from_secs(seconds));
                    config.transactions_per_client = None;
                }
                "--transactions" => {
                    config.transactions_per_client = Some(
                        next(&mut i)?
                            .parse()
                            .map_err(|_| "invalid value for --transactions".to_string())?,
                    );
                    config.duration = None;
                }
                "--scale" => {
                    config.scale = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid value for --scale".to_string())?
                }
                "--fillfactor" => {
                    config.fillfactor = next(&mut i)?
                        .parse()
                        .map_err(|_| "invalid value for --fillfactor".to_string())?
                }
                "--init" => config.init = true,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
            i += 1;
        }

        if config.clients == 0 {
            return Err("--clients must be greater than 0".to_string());
        }
        if config.pool_size == 0 {
            return Err("--pool-size must be greater than 0".to_string());
        }
        if config.scale <= 0 {
            return Err("--scale must be greater than 0".to_string());
        }
        if !(10..=100).contains(&config.fillfactor) {
            return Err("--fillfactor must be between 10 and 100".to_string());
        }

        Ok(config)
    }
}

fn print_usage() {
    println!("Usage: cargo run --release --bin pgbench_like -- [options]");
    println!();
    println!("Options:");
    println!("  --base-dir PATH         data directory (default /tmp/pgrust-pgbench-like)");
    println!("  --pool-size N           buffer pool size (default 128)");
    println!("  --init                  create and load the pgbench-like schema");
    println!("  --scale N               scale factor for init/workload (default 1)");
    println!("  --fillfactor N          fillfactor on created tables (default 100)");
    println!("  --clients N             number of concurrent client threads (default 10)");
    println!("  --time SECONDS          run for this duration (default 30)");
    println!("  --transactions N        run this many transactions per client");
    println!("  --help                  show this help");
}
