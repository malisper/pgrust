use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::path::PathBuf;
use std::time::Instant;

use pgrust::plpgsql::take_notices;
use pgrust::{Database, Session};

struct Args {
    base_dir: PathBuf,
    iterations: usize,
    filler_tables: usize,
    pool_size: usize,
    workload: Workload,
    wait: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    ReplicaAlwaysInsert,
    ForeignKeyChildInsert,
    ForeignKeyParentDelete,
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let _ = std::fs::remove_dir_all(&args.base_dir);
    std::fs::create_dir_all(&args.base_dir).map_err(|e| e.to_string())?;

    let db = Database::open(&args.base_dir, args.pool_size).map_err(|e| format!("{e:?}"))?;
    let mut session = Session::new(1);
    setup(&db, &mut session, args.filler_tables)?;

    let sql = match args.workload {
        Workload::ReplicaAlwaysInsert => {
            session
                .execute(&db, "set session_replication_role = replica")
                .map_err(|e| format!("{e:?}"))?;
            session
                .execute(
                    &db,
                    "alter table trigtest enable always trigger trigtest_a_stmt_tg",
                )
                .map_err(|e| format!("{e:?}"))?;
            "insert into trigtest default values"
        }
        Workload::ForeignKeyChildInsert => "insert into trigtest2 values (1)",
        Workload::ForeignKeyParentDelete => "delete from trigtest where i = 1",
    };

    println!("pid: {}", std::process::id());
    println!("workload: {:?}", args.workload);
    println!("base_dir: {}", args.base_dir.display());
    println!("iterations: {}", args.iterations);
    println!("filler_tables: {}", args.filler_tables);
    println!("sql: {sql}");

    if args.wait {
        eprintln!("READY pid={}", std::process::id());
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
    }

    let _ = take_notices();
    let started = Instant::now();
    for _ in 0..args.iterations {
        session.execute(&db, sql).map_err(|e| format!("{e:?}"))?;
        let _ = take_notices();
    }
    let elapsed = started.elapsed();

    println!("total_ms: {:.3}", elapsed.as_secs_f64() * 1000.0);
    if args.iterations > 0 {
        println!(
            "avg_ms_per_iteration: {:.3}",
            elapsed.as_secs_f64() * 1000.0 / args.iterations as f64
        );
    }

    Ok(())
}

fn setup(db: &Database, session: &mut Session, filler_tables: usize) -> Result<(), String> {
    for i in 0..filler_tables {
        session
            .execute(db, &format!("create table filler_{i} (id int4)"))
            .map_err(|e| format!("{e:?}"))?;
    }

    for sql in [
        "create table trigtest (i serial primary key)",
        "create table trigtest2 (i int references trigtest(i) on delete cascade)",
        "create function trigtest() returns trigger as $$ \
         begin \
             raise notice '% % % %', TG_TABLE_NAME, TG_OP, TG_WHEN, TG_LEVEL; \
             return new; \
         end;$$ language plpgsql",
        "create trigger trigtest_b_row_tg before insert or update or delete on trigtest \
         for each row execute procedure trigtest()",
        "create trigger trigtest_a_row_tg after insert or update or delete on trigtest \
         for each row execute procedure trigtest()",
        "create trigger trigtest_b_stmt_tg before insert or update or delete on trigtest \
         for each statement execute procedure trigtest()",
        "create trigger trigtest_a_stmt_tg after insert or update or delete on trigtest \
         for each statement execute procedure trigtest()",
        "insert into trigtest default values",
        "alter table trigtest disable trigger trigtest_b_row_tg",
        "insert into trigtest default values",
        "alter table trigtest disable trigger user",
        "insert into trigtest default values",
        "alter table trigtest enable trigger trigtest_a_stmt_tg",
        "insert into trigtest default values",
        "set session_replication_role = replica",
        "insert into trigtest default values",
        "reset session_replication_role",
    ] {
        session.execute(db, sql).map_err(|e| format!("{e:?}"))?;
    }

    Ok(())
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        base_dir: std::env::temp_dir().join(format!(
            "pgrust_triggers_regression_profile_{}",
            std::process::id()
        )),
        iterations: 1,
        filler_tables: 0,
        pool_size: 16_384,
        workload: Workload::ReplicaAlwaysInsert,
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
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--workload" => {
                args.workload = match take_value(&raw, &mut i, "--workload")?.as_str() {
                    "replica-always-insert" => Workload::ReplicaAlwaysInsert,
                    "fk-child-insert" => Workload::ForeignKeyChildInsert,
                    "fk-parent-delete" => Workload::ForeignKeyParentDelete,
                    other => return Err(format!("unknown workload: {other}")),
                };
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

fn usage() -> ! {
    eprintln!(
        "Usage: triggers_regression_profile [options]

Options:
  --dir DIR                 Data dir (default: /tmp/pgrust_triggers_regression_profile)
  --iterations N            Workload executions (default: 1)
  --filler-tables N         Extra catalog relations before setup (default: 0)
  --pool-size N             Buffer pool size (default: 16384)
  --workload NAME           replica-always-insert | fk-child-insert | fk-parent-delete
  --wait                    Print PID and SIGSTOP before timed workload"
    );
    std::process::exit(2);
}
