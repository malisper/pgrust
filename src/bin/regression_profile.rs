use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use pgrust::pgrust::database::Database;
use pgrust::pgrust::server::serve;

struct Args {
    data_dir: PathBuf,
    results_dir: PathBuf,
    port: u16,
    pool_size: usize,
    timeout_secs: u64,
    test_name: Option<String>,
    use_pgrust_setup: bool,
    keep_data: bool,
    keep_results: bool,
}

struct Paths {
    pg_regress_dir: PathBuf,
    sql_dir: PathBuf,
    expected_dir: PathBuf,
    pgrust_setup_sql: PathBuf,
}

struct Summary {
    total: usize,
    passed: usize,
    failed: usize,
    errored: usize,
    skipped: usize,
}

enum TestOutcome {
    Pass,
    Fail { diff_lines: usize },
    Error,
    Skip,
}

fn usage() -> ! {
    eprintln!(
        "Usage: cargo run --release --bin regression_profile -- [options]

Options:
  --dir DIR               Data dir (default: /tmp/pgrust_regression_profile_data)
  --results-dir DIR       Results dir (default: /tmp/pgrust_regression_profile)
  --port PORT             Server port (default: 5548)
  --pool-size N           Buffer pool size (default: 16384)
  --timeout SECS          Per-test timeout (default: 30)
  --test NAME             Run only one regression test
  --pgrust-setup          Use scripts/test_setup_pgrust.sql and skip upstream test_setup.sql
  --keep-data             Preserve the data dir contents
  --keep-results          Preserve existing results dir contents

Examples:
  cargo run --release --bin regression_profile --
  cargo run --release --bin regression_profile -- --test select --pgrust-setup
  cargo run --release --bin regression_profile -- --dir /tmp/pgrust_regprof --results-dir /tmp/pgrust_regprof_out"
    );
    std::process::exit(2);
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        data_dir: std::env::temp_dir().join("pgrust_regression_profile_data"),
        results_dir: std::env::temp_dir().join("pgrust_regression_profile"),
        port: 5548,
        pool_size: 16_384,
        timeout_secs: 30,
        test_name: None,
        use_pgrust_setup: false,
        keep_data: false,
        keep_results: false,
    };

    let raw = std::env::args().skip(1).collect::<Vec<_>>();
    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--dir" => args.data_dir = PathBuf::from(take_value(&raw, &mut i, "--dir")?),
            "--results-dir" => {
                args.results_dir = PathBuf::from(take_value(&raw, &mut i, "--results-dir")?)
            }
            "--port" => {
                args.port = take_value(&raw, &mut i, "--port")?
                    .parse()
                    .map_err(|_| "invalid --port value".to_string())?;
            }
            "--pool-size" => {
                args.pool_size = take_value(&raw, &mut i, "--pool-size")?
                    .parse()
                    .map_err(|_| "invalid --pool-size value".to_string())?;
            }
            "--timeout" => {
                args.timeout_secs = take_value(&raw, &mut i, "--timeout")?
                    .parse()
                    .map_err(|_| "invalid --timeout value".to_string())?;
            }
            "--test" => args.test_name = Some(take_value(&raw, &mut i, "--test")?),
            "--pgrust-setup" => {
                args.use_pgrust_setup = true;
                i += 1;
            }
            "--keep-data" => {
                args.keep_data = true;
                i += 1;
            }
            "--keep-results" => {
                args.keep_results = true;
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

fn repo_paths() -> Result<Paths, String> {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = repo_root
        .parent()
        .ok_or_else(|| "failed to locate workspace root".to_string())?;
    let pg_regress = workspace_root.join("postgres/src/test/regress");
    let sql_dir = pg_regress.join("sql");
    let expected_dir = pg_regress.join("expected");
    let pgrust_setup_sql = repo_root.join("scripts/test_setup_pgrust.sql");

    if !sql_dir.is_dir() {
        return Err(format!(
            "regression SQL dir not found: {}",
            sql_dir.display()
        ));
    }
    if !expected_dir.is_dir() {
        return Err(format!(
            "regression expected dir not found: {}",
            expected_dir.display()
        ));
    }

    Ok(Paths {
        pg_regress_dir: pg_regress,
        sql_dir,
        expected_dir,
        pgrust_setup_sql,
    })
}

fn prepare_dir(path: &Path, preserve: bool) -> Result<(), String> {
    if !preserve {
        let _ = fs::remove_dir_all(path);
    }
    fs::create_dir_all(path).map_err(|e| format!("create {}: {e}", path.display()))
}

fn start_server(data_dir: &Path, port: u16, pool_size: usize) -> Result<(), String> {
    let db = Database::open(data_dir, pool_size).map_err(|e| format!("{e:?}"))?;
    let addr = format!("127.0.0.1:{port}");
    let (tx, rx) = mpsc::channel::<String>();

    thread::spawn(move || {
        if let Err(err) = serve(&addr, db) {
            let _ = tx.send(err.to_string());
        }
    });

    let start = Instant::now();
    let ready_deadline = Duration::from_secs(15);
    while start.elapsed() < ready_deadline {
        if psql_command(port, None)
            .arg("-c")
            .arg("SELECT 1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
        {
            return Ok(());
        }

        if let Ok(err) = rx.try_recv() {
            return Err(format!("server exited during startup: {err}"));
        }

        thread::sleep(Duration::from_millis(100));
    }

    if let Ok(err) = rx.try_recv() {
        Err(format!("server failed to become ready: {err}"))
    } else {
        Err(format!(
            "server did not become ready on port {port} within {}s",
            ready_deadline.as_secs()
        ))
    }
}

fn psql_command(port: u16, pg_regress_dir: Option<&Path>) -> Command {
    let mut cmd = Command::new("psql");
    cmd.env("PGPASSWORD", "x")
        .arg("-X")
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .arg("-U")
        .arg("postgres");
    configure_pg_regress_env(&mut cmd, pg_regress_dir);
    cmd
}

fn configure_pg_regress_env(cmd: &mut Command, pg_regress_dir: Option<&Path>) {
    if let Some(pg_regress_dir) = pg_regress_dir {
        cmd.env("PG_ABS_SRCDIR", pg_regress_dir);
    }
    if let Ok(output) = Command::new("pg_config").arg("--pkglibdir").output() {
        if output.status.success() {
            let pkglibdir = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !pkglibdir.is_empty() {
                cmd.env("PG_LIBDIR", pkglibdir);
            }
        }
    }
    cmd.env("PG_DLSUFFIX", env::consts::DLL_SUFFIX);
}

fn run_sql_file(
    port: u16,
    pg_regress_dir: &Path,
    sql_path: &Path,
    output_path: &Path,
    timeout_secs: u64,
    on_error_stop: bool,
) -> Result<Option<ExitStatus>, String> {
    let input = File::open(sql_path).map_err(|e| format!("open {}: {e}", sql_path.display()))?;
    let output =
        File::create(output_path).map_err(|e| format!("create {}: {e}", output_path.display()))?;
    let err_output = output
        .try_clone()
        .map_err(|e| format!("clone {}: {e}", output_path.display()))?;

    let mut cmd = psql_command(port, Some(pg_regress_dir));
    if on_error_stop {
        cmd.arg("-v").arg("ON_ERROR_STOP=1");
    }
    cmd.arg("-a")
        .arg("-q")
        .stdin(Stdio::from(input))
        .stdout(Stdio::from(output))
        .stderr(Stdio::from(err_output));

    let mut child = cmd.spawn().map_err(|e| format!("spawn psql: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    loop {
        if let Some(status) = child.try_wait().map_err(|e| format!("wait on psql: {e}"))? {
            return Ok(Some(status));
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .map_err(|e| format!("kill timed out psql: {e}"))?;
            let _ = child.wait();
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(output_path)
                .map_err(|e| format!("append {}: {e}", output_path.display()))?;
            writeln!(file, "TIMEOUT").map_err(|e| format!("write timeout marker: {e}"))?;
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn collect_tests(args: &Args, paths: &Paths) -> Result<Vec<PathBuf>, String> {
    if let Some(test_name) = &args.test_name {
        let test_path = paths.sql_dir.join(format!("{test_name}.sql"));
        if !test_path.is_file() {
            return Err(format!("test file not found: {}", test_path.display()));
        }
        return Ok(vec![test_path]);
    }

    let mut tests = fs::read_dir(&paths.sql_dir)
        .map_err(|e| format!("read {}: {e}", paths.sql_dir.display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension() == Some(OsStr::new("sql")))
        .collect::<Vec<_>>();
    tests.sort();

    if args.use_pgrust_setup {
        tests.retain(|path| path.file_name() != Some(OsStr::new("test_setup.sql")));
    } else if let Some(pos) = tests
        .iter()
        .position(|path| path.file_name() == Some(OsStr::new("test_setup.sql")))
    {
        let test_setup = tests.remove(pos);
        tests.insert(0, test_setup);
    }

    Ok(tests)
}

fn expected_candidates(expected_dir: &Path, test_name: &str) -> Result<Vec<PathBuf>, String> {
    let mut candidates = Vec::new();
    let primary = expected_dir.join(format!("{test_name}.out"));
    if primary.is_file() {
        candidates.push(primary);
    }

    let prefix = format!("{test_name}_");
    for entry in
        fs::read_dir(expected_dir).map_err(|e| format!("read {}: {e}", expected_dir.display()))?
    {
        let path = entry
            .map_err(|e| format!("read dir entry in {}: {e}", expected_dir.display()))?
            .path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with(&prefix) && name.ends_with(".out") {
            candidates.push(path);
        }
    }

    candidates.sort();
    Ok(candidates)
}

fn diff_outputs(expected: &Path, actual: &Path) -> Result<Result<(), Vec<u8>>, String> {
    let output = Command::new("diff")
        .arg("-u")
        .arg("-b")
        .arg(expected)
        .arg(actual)
        .output()
        .map_err(|e| format!("run diff: {e}"))?;

    if output.status.success() {
        Ok(Ok(()))
    } else {
        let mut diff = output.stdout;
        diff.extend_from_slice(&output.stderr);
        Ok(Err(diff))
    }
}

fn classify_test(
    output_path: &Path,
    diff_path: &Path,
    expected_dir: &Path,
    test_name: &str,
) -> Result<TestOutcome, String> {
    let candidates = expected_candidates(expected_dir, test_name)?;
    if candidates.is_empty() {
        return Ok(TestOutcome::Skip);
    }

    let mut best_diff: Option<Vec<u8>> = None;
    let mut best_diff_lines = usize::MAX;
    for candidate in candidates {
        match diff_outputs(&candidate, output_path)? {
            Ok(()) => {
                let _ = fs::remove_file(diff_path);
                return Ok(TestOutcome::Pass);
            }
            Err(diff) => {
                let diff_lines = byte_line_count(&diff);
                if diff_lines < best_diff_lines {
                    best_diff_lines = diff_lines;
                    best_diff = Some(diff);
                }
            }
        }
    }

    if let Some(diff) = best_diff {
        fs::write(diff_path, diff).map_err(|e| format!("write {}: {e}", diff_path.display()))?;
    }

    let output_text = fs::read_to_string(output_path)
        .unwrap_or_default()
        .to_lowercase();
    if output_text.contains("connection refused")
        || output_text.contains("could not connect")
        || output_text.contains("server closed the connection unexpectedly")
        || output_text.contains("timeout")
    {
        Ok(TestOutcome::Error)
    } else {
        Ok(TestOutcome::Fail {
            diff_lines: best_diff_lines,
        })
    }
}

fn byte_line_count(bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        0
    } else {
        bytes.split(|&b| b == b'\n').count()
    }
}

fn write_summary(results_dir: &Path, summary: &Summary) -> Result<(), String> {
    let json = format!(
        concat!(
            "{{\n",
            "  \"tests\": {{\n",
            "    \"total\": {},\n",
            "    \"passed\": {},\n",
            "    \"failed\": {},\n",
            "    \"errored\": {},\n",
            "    \"skipped\": {}\n",
            "  }}\n",
            "}}\n"
        ),
        summary.total, summary.passed, summary.failed, summary.errored, summary.skipped
    );
    fs::write(results_dir.join("summary.json"), json)
        .map_err(|e| format!("write summary.json: {e}"))
}

fn main() -> Result<(), String> {
    let args = parse_args()?;
    let paths = repo_paths()?;

    raise_fd_limit();
    prepare_dir(&args.data_dir, args.keep_data)?;
    prepare_dir(&args.results_dir, args.keep_results)?;
    prepare_dir(&args.results_dir.join("output"), true)?;
    prepare_dir(&args.results_dir.join("diff"), true)?;

    start_server(&args.data_dir, args.port, args.pool_size)?;

    if args.use_pgrust_setup {
        if !paths.pgrust_setup_sql.is_file() {
            return Err(format!(
                "pgrust setup file not found: {}",
                paths.pgrust_setup_sql.display()
            ));
        }

        let setup_output = args.results_dir.join("output/test_setup_pgrust.out");
        let setup_status = run_sql_file(
            args.port,
            &paths.pg_regress_dir,
            &paths.pgrust_setup_sql,
            &setup_output,
            args.timeout_secs,
            true,
        )?;
        if !setup_status.is_some_and(|status| status.success()) {
            return Err(format!(
                "pgrust setup bootstrap failed; see {}",
                setup_output.display()
            ));
        }
    }

    let tests = collect_tests(&args, &paths)?;
    println!("engine: pgrust-regression-profile");
    println!("data_dir: {}", args.data_dir.display());
    println!("results_dir: {}", args.results_dir.display());
    println!("port: {}", args.port);
    println!("tests_selected: {}", tests.len());
    println!();

    let mut summary = Summary {
        total: 0,
        passed: 0,
        failed: 0,
        errored: 0,
        skipped: 0,
    };

    for sql_file in tests {
        let test_name = sql_file
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("invalid test file name: {}", sql_file.display()))?;
        let output_path = args
            .results_dir
            .join("output")
            .join(format!("{test_name}.out"));
        let diff_path = args
            .results_dir
            .join("diff")
            .join(format!("{test_name}.diff"));

        summary.total += 1;
        let _ = run_sql_file(
            args.port,
            &paths.pg_regress_dir,
            &sql_file,
            &output_path,
            args.timeout_secs,
            false,
        )?;
        match classify_test(&output_path, &diff_path, &paths.expected_dir, test_name)? {
            TestOutcome::Pass => {
                summary.passed += 1;
                println!("{test_name:<40} PASS");
            }
            TestOutcome::Fail { diff_lines } => {
                summary.failed += 1;
                println!("{test_name:<40} FAIL  ({diff_lines} diff lines)");
            }
            TestOutcome::Error => {
                summary.errored += 1;
                println!("{test_name:<40} ERROR");
            }
            TestOutcome::Skip => {
                summary.skipped += 1;
                println!("{test_name:<40} SKIP");
            }
        }
    }

    write_summary(&args.results_dir, &summary)?;

    println!();
    println!("summary.total:   {}", summary.total);
    println!("summary.passed:  {}", summary.passed);
    println!("summary.failed:  {}", summary.failed);
    println!("summary.errored: {}", summary.errored);
    println!("summary.skipped: {}", summary.skipped);
    println!(
        "summary.json:    {}",
        args.results_dir.join("summary.json").display()
    );

    if summary.failed > 0 || summary.errored > 0 {
        std::process::exit(1);
    }
    Ok(())
}
