use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::ffi::CString;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pgrust::Cluster;
use pgrust::Database;
use pgrust::Session;
use pgrust::{ExecError, StatementResult, Value};

struct Args {
    seed_dir: PathBuf,
    work_root: PathBuf,
    iterations: usize,
    pool_size: usize,
    pause_after_seed_secs: u64,
    preserve_seed: bool,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin deferred_parent_delete_bench -- [options]

Options:
  --seed-dir DIR             Golden seed dir (default: /tmp/pgrust_deferred_parent_delete_seed)
  --work-root DIR            Per-iteration clone root (default: /tmp/pgrust_deferred_parent_delete_work)
  --iterations N             Workload iterations (default: 4)
  --pool-size N              Buffer pool size (default: 16)
  --pause-after-seed-secs N  Sleep after seed creation so a profiler can attach
  --preserve-seed            Reuse existing seed dir if present"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        seed_dir: std::env::temp_dir().join("pgrust_deferred_parent_delete_seed"),
        work_root: std::env::temp_dir().join("pgrust_deferred_parent_delete_work"),
        iterations: 4,
        pool_size: 16,
        pause_after_seed_secs: 0,
        preserve_seed: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--seed-dir" => args.seed_dir = PathBuf::from(take_value(&raw, &mut i, "--seed-dir")?),
            "--work-root" => {
                args.work_root = PathBuf::from(take_value(&raw, &mut i, "--work-root")?)
            }
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
            "--pause-after-seed-secs" => {
                args.pause_after_seed_secs = take_value(&raw, &mut i, "--pause-after-seed-secs")?
                    .parse()
                    .map_err(|_| "invalid --pause-after-seed-secs value".to_string())?;
            }
            "--preserve-seed" => {
                args.preserve_seed = true;
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

fn main() -> Result<(), String> {
    let args = parse_args()?;

    let seed_started = Instant::now();
    if !args.preserve_seed || !args.seed_dir.exists() {
        let _ = fs::remove_dir_all(&args.seed_dir);
        fs::create_dir_all(&args.seed_dir).map_err(|e| e.to_string())?;
        drop(Cluster::open(&args.seed_dir, args.pool_size).map_err(|e| format!("{e:?}"))?);
    }
    let seed_elapsed = seed_started.elapsed();

    let _ = fs::remove_dir_all(&args.work_root);
    fs::create_dir_all(&args.work_root).map_err(|e| e.to_string())?;

    println!("pid: {}", std::process::id());
    println!("seed_dir: {}", args.seed_dir.display());
    println!("work_root: {}", args.work_root.display());
    println!("seed_ms: {:.3}", seed_elapsed.as_secs_f64() * 1000.0);
    if args.pause_after_seed_secs > 0 {
        println!("ready_after_seed: sleeping {}s", args.pause_after_seed_secs);
        std::thread::sleep(Duration::from_secs(args.pause_after_seed_secs));
    }

    let total_started = Instant::now();
    let mut iteration_ms = Vec::with_capacity(args.iterations);
    for i in 0..args.iterations {
        let base = args.work_root.join(format!("run_{i}"));
        let _ = fs::remove_dir_all(&base);

        let clone_started = Instant::now();
        clone_tree(&args.seed_dir, &base)?;
        let clone_ms = clone_started.elapsed().as_secs_f64() * 1000.0;

        let workload_started = Instant::now();
        run_workload(&base, args.pool_size)?;
        let workload_ms = workload_started.elapsed().as_secs_f64() * 1000.0;
        let total_ms = clone_started.elapsed().as_secs_f64() * 1000.0;
        iteration_ms.push(total_ms);

        println!(
            "iteration={} clone_ms={:.3} workload_ms={:.3} total_ms={:.3}",
            i + 1,
            clone_ms,
            workload_ms,
            total_ms
        );
    }

    let total_ms = total_started.elapsed().as_secs_f64() * 1000.0;
    let avg_ms = iteration_ms.iter().sum::<f64>() / iteration_ms.len().max(1) as f64;
    println!("iterations: {}", args.iterations);
    println!("total_test_ms: {:.3}", total_ms);
    println!("avg_iteration_ms: {:.3}", avg_ms);
    Ok(())
}

fn run_workload(base: &Path, pool_size: usize) -> Result<(), String> {
    let db = Database::open(base, pool_size).map_err(|e| format!("{e:?}"))?;
    let mut session = Session::new(1);

    session
        .execute(&db, "create table parents (id int4 primary key)")
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(
            &db,
            "create table children (id int4 primary key, parent_id int4 references parents)",
        )
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(&db, "insert into parents values (1)")
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(&db, "insert into children values (1, 1)")
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(
            &db,
            "alter table children alter constraint children_parent_id_fkey deferrable initially deferred",
        )
        .map_err(|e| format!("{e:?}"))?;

    session
        .execute(&db, "begin")
        .map_err(|e| format!("{e:?}"))?;
    session
        .execute(&db, "delete from parents where id = 1")
        .map_err(|e| format!("{e:?}"))?;
    match session.execute(&db, "commit") {
        Err(ExecError::ForeignKeyViolation { constraint, .. }) => {
            if constraint != "children_parent_id_fkey" {
                return Err(format!("unexpected constraint: {constraint}"));
            }
        }
        other => {
            return Err(format!(
                "expected deferred parent-delete violation at commit, got {other:?}"
            ));
        }
    }

    if query_rows(&db, "select id from parents order by id") != vec![vec![Value::Int32(1)]] {
        return Err("parent row was not restored".into());
    }
    if query_rows(&db, "select id, parent_id from children order by id")
        != vec![vec![Value::Int32(1), Value::Int32(1)]]
    {
        return Err("child row changed unexpectedly".into());
    }

    Ok(())
}

fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(1, sql).unwrap() {
        StatementResult::Query { rows, .. } => rows,
        other => panic!("expected query result, got {other:?}"),
    }
}

fn clone_tree(src: &Path, dst: &Path) -> Result<(), String> {
    let metadata = fs::metadata(src).map_err(io_error)?;
    if metadata.is_dir() {
        fs::create_dir_all(dst).map_err(io_error)?;
        fs::set_permissions(dst, metadata.permissions()).map_err(io_error)?;
        for entry in fs::read_dir(src).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            clone_tree(&entry.path(), &dst.join(entry.file_name()))?;
        }
        return Ok(());
    }

    clone_file(src, dst)?;
    fs::set_permissions(dst, metadata.permissions()).map_err(io_error)?;
    Ok(())
}

fn clone_file(src: &Path, dst: &Path) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        if try_clonefile(src, dst).is_ok() {
            return Ok(());
        }
    }

    fs::copy(src, dst).map_err(io_error)?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn try_clonefile(src: &Path, dst: &Path) -> Result<(), String> {
    use std::os::unix::ffi::OsStrExt;

    let src = CString::new(src.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    let dst = CString::new(dst.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
    let rc = unsafe { libc::clonefile(src.as_ptr(), dst.as_ptr(), 0) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().to_string())
    }
}

fn io_error(err: std::io::Error) -> String {
    err.to_string()
}
