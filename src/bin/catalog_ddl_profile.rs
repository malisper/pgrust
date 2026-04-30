use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::Instant;

use pgrust::pgrust::database::{Database, Session};

struct Args {
    base_dir: PathBuf,
    iterations: usize,
    filler_tables: usize,
    children: usize,
    pool_size: usize,
    workload: Workload,
    triggers: bool,
    wait: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    PartitionedPkTree,
}

struct StatementTiming {
    label: String,
    ms: f64,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --features tools --bin catalog_ddl_profile -- [options]

Options:
  --dir DIR             Data dir (default: /tmp/pgrust_catalog_ddl_profile)
  --iterations N        Workload repetitions (default: 1)
  --filler-tables N     Extra catalog relations before timed workload (default: 0)
  --children N          Leaf partitions under the nested partition (default: 4)
  --pool-size N         Buffer pool size (default: 16384)
  --workload NAME       partitioned-pk-tree
  --triggers            Add partitioned row trigger cloning to the workload
  --wait                Print PID and SIGSTOP before timed workload"
    );
    std::process::exit(2);
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let _ = std::fs::remove_dir_all(&args.base_dir);
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    let mut session = Session::new(1);
    seed_filler_tables(&db, &mut session, args.filler_tables)?;

    println!("pid: {}", std::process::id());
    println!("workload: {:?}", args.workload);
    println!("base_dir: {}", args.base_dir.display());
    println!("iterations: {}", args.iterations);
    println!("filler_tables: {}", args.filler_tables);
    println!("children: {}", args.children);
    println!("triggers: {}", args.triggers);

    if args.wait {
        eprintln!("READY pid={}", std::process::id());
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
    }

    let started = Instant::now();
    let mut timings = Vec::new();
    for iteration in 0..args.iterations {
        match args.workload {
            Workload::PartitionedPkTree => run_partitioned_pk_tree(
                &db,
                &mut session,
                iteration,
                args.children,
                args.triggers,
                &mut timings,
            )?,
        }
    }

    let total_ms = started.elapsed().as_secs_f64() * 1000.0;
    for timing in &timings {
        println!("statement label={} ms={:.3}", timing.label, timing.ms);
    }
    println!("statements: {}", timings.len());
    println!("total_ms: {:.3}", total_ms);
    if args.iterations > 0 {
        println!(
            "avg_ms_per_iteration: {:.3}",
            total_ms / args.iterations as f64
        );
    }

    Ok(())
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join("pgrust_catalog_ddl_profile"),
        iterations: 1,
        filler_tables: 0,
        children: 4,
        pool_size: 16_384,
        workload: Workload::PartitionedPkTree,
        triggers: false,
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
            "--filler-tables" => {
                args.filler_tables = take_value(&raw, &mut i, "--filler-tables")?
                    .parse()
                    .map_err(|_| "invalid --filler-tables value".to_string())?;
            }
            "--children" => {
                args.children = take_value(&raw, &mut i, "--children")?
                    .parse()
                    .map_err(|_| "invalid --children value".to_string())?;
            }
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--workload" => {
                args.workload = match take_value(&raw, &mut i, "--workload")?.as_str() {
                    "partitioned-pk-tree" => Workload::PartitionedPkTree,
                    other => return Err(format!("unknown workload: {other}")),
                };
            }
            "--triggers" => {
                args.triggers = true;
                i += 1;
            }
            "--wait" => {
                args.wait = true;
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

fn seed_filler_tables(
    db: &Database,
    session: &mut Session,
    filler_tables: usize,
) -> Result<(), String> {
    for i in 0..filler_tables {
        exec(
            session,
            db,
            &format!("create table catddl_filler_{i} (id int4)"),
        )?;
    }
    Ok(())
}

fn run_partitioned_pk_tree(
    db: &Database,
    session: &mut Session,
    iteration: usize,
    children: usize,
    triggers: bool,
    timings: &mut Vec<StatementTiming>,
) -> Result<(), String> {
    let prefix = format!("catddl_{iteration}");
    exec_timed(
        session,
        db,
        timings,
        format!("{prefix}.create_parent"),
        &format!(
            "create table {prefix}_parent (
                id int4 not null,
                bucket int4 not null,
                payload text,
                primary key (id, bucket)
             ) partition by range (bucket)"
        ),
    )?;
    exec_timed(
        session,
        db,
        timings,
        format!("{prefix}.create_nested"),
        &format!(
            "create table {prefix}_nested partition of {prefix}_parent
             for values from (0) to (1000) partition by range (bucket)"
        ),
    )?;

    if triggers {
        exec_timed(
            session,
            db,
            timings,
            format!("{prefix}.create_trigger_func"),
            &format!(
                "create function {prefix}_trig_fn() returns trigger language plpgsql as $$
                 begin
                   return new;
                 end;
                 $$"
            ),
        )?;
        exec_timed(
            session,
            db,
            timings,
            format!("{prefix}.create_trigger"),
            &format!(
                "create trigger {prefix}_trig after insert on {prefix}_parent
                 for each row execute function {prefix}_trig_fn()"
            ),
        )?;
    }

    for child in 0..children {
        let low = child * 100;
        let high = low + 100;
        exec_timed(
            session,
            db,
            timings,
            format!("{prefix}.create_leaf_{child}"),
            &format!(
                "create table {prefix}_leaf_{child} partition of {prefix}_nested
                 for values from ({low}) to ({high})"
            ),
        )?;
    }

    exec_timed(
        session,
        db,
        timings,
        format!("{prefix}.create_attach_table"),
        &format!(
            "create table {prefix}_attach (
                id int4 not null,
                bucket int4 not null,
                payload text
             )"
        ),
    )?;
    exec_timed(
        session,
        db,
        timings,
        format!("{prefix}.attach_first"),
        &format!(
            "alter table {prefix}_parent attach partition {prefix}_attach
             for values from (1000) to (2000)"
        ),
    )?;
    exec_timed(
        session,
        db,
        timings,
        format!("{prefix}.detach"),
        &format!("alter table {prefix}_parent detach partition {prefix}_attach"),
    )?;
    exec_timed(
        session,
        db,
        timings,
        format!("{prefix}.attach_second"),
        &format!(
            "alter table {prefix}_parent attach partition {prefix}_attach
             for values from (1000) to (2000)"
        ),
    )?;

    Ok(())
}

fn exec_timed(
    session: &mut Session,
    db: &Database,
    timings: &mut Vec<StatementTiming>,
    label: String,
    sql: &str,
) -> Result<(), String> {
    let started = Instant::now();
    exec(session, db, sql)?;
    timings.push(StatementTiming {
        label,
        ms: started.elapsed().as_secs_f64() * 1000.0,
    });
    Ok(())
}

fn exec(session: &mut Session, db: &Database, sql: &str) -> Result<(), String> {
    session.execute(db, sql).map(|_| ()).map_err(|e| {
        let compact = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        format!("SQL failed: {compact}: {e:?}")
    })
}
