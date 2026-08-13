//! Single-engine statement applier for the local coverage loop
//! (scripts/covloop.sh): read SQL statements from stdin (one per line,
//! `--`-comment lines skipped — fuzzgen's `--format sql` shape), apply
//! them over one wire-client session, and report counts. Differential
//! comparison is deliberately NOT done here — coverage is the signal the
//! covloop cares about; the diffrunner owns divergence hunting.

use std::io::BufRead;
use std::process::ExitCode;

use fuzzgen::catalog::{fixture_ddl, fixture_seed_sql, CatalogSource, FixtureCatalog};
use fuzzgen::diff::StmtOutcome;
use fuzzgen::runner::{ClientExecutor, Executor};

const USAGE: &str = "\
usage: covapply --port <n> [options] < statements.sql
  --host <host>   server host (default 127.0.0.1)
  --port <n>      server port (required)
  --db <name>     database (default: fuzz)
  --user <name>   role (default: $USER)
  --setup         create the fixture schema + seed rows first
  --xproto <seed> per-statement seeded protocol-mode mix (simple vs
                  extended Parse/Bind/Execute with text/binary parameters,
                  row limits, and binary result format); same planner as
                  diffrunner --xproto
  --copybin       after the stdin stream (or alone with an empty stdin),
                  run the single-engine COPY BINARY deck: every suite
                  table COPY'd TO STDOUT (FORMAT binary) and fed back
                  through COPY FROM STDIN (FORMAT binary) — the coverage
                  driver for copyto.c/copyfromparse.c binary arms and the
                  *_send/*_recv families
  --copyopts      run the single-engine COPY options-matrix deck (lane
                  COPYOPTS): the option-error/validation arms plus
                  control-char round-trip identity — the coverage driver
                  for copy.c ProcessCopyOptions / defGetCopy* and the
                  copyto.c text/csv out-function escape ladder. Sized by
                  --copyopts-seed / --copyopts-count
";

struct Args {
    host: String,
    port: Option<u16>,
    db: String,
    user: String,
    setup: bool,
    xproto: Option<u64>,
    copybin: bool,
    copytext: bool,
    copytext_seed: u64,
    copytext_count: u32,
    copyopts: bool,
    copyopts_seed: u64,
    copyopts_count: u32,
    dbddl: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        host: "127.0.0.1".to_string(),
        port: None,
        db: "fuzz".to_string(),
        user: std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()),
        setup: false,
        xproto: None,
        copybin: false,
        copytext: false,
        copytext_seed: 0,
        copytext_count: 200,
        copyopts: false,
        copyopts_seed: 0,
        copyopts_count: 200,
        dbddl: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = |name: &str| {
            it.next().ok_or_else(|| format!("{name} requires a value"))
        };
        match arg.as_str() {
            "--host" => args.host = value("--host")?,
            "--port" => {
                args.port =
                    Some(value("--port")?.parse().map_err(|e| format!("bad --port: {e}"))?)
            }
            "--db" => args.db = value("--db")?,
            "--user" => args.user = value("--user")?,
            "--setup" => args.setup = true,
            "--xproto" => {
                args.xproto = Some(
                    value("--xproto")?.parse().map_err(|e| format!("bad --xproto: {e}"))?,
                )
            }
            "--copybin" => args.copybin = true,
            "--copytext" => args.copytext = true,
            "--copytext-seed" => {
                args.copytext_seed = value("--copytext-seed")?
                    .parse()
                    .map_err(|e| format!("bad --copytext-seed: {e}"))?
            }
            "--copytext-count" => {
                args.copytext_count = value("--copytext-count")?
                    .parse()
                    .map_err(|e| format!("bad --copytext-count: {e}"))?
            }
            "--copyopts" => args.copyopts = true,
            "--copyopts-seed" => {
                args.copyopts_seed = value("--copyopts-seed")?
                    .parse()
                    .map_err(|e| format!("bad --copyopts-seed: {e}"))?
            }
            "--copyopts-count" => {
                args.copyopts_count = value("--copyopts-count")?
                    .parse()
                    .map_err(|e| format!("bad --copyopts-count: {e}"))?
            }
            "--dbddl" => args.dbddl = true,
            "--help" | "-h" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    Ok(args)
}

fn run() -> Result<ExitCode, String> {
    let args = parse_args()?;
    let port = args.port.ok_or("--port is required")?;
    let mut ex =
        ClientExecutor::connect_opts(&args.host, port, &args.db, &args.user, args.xproto)?;

    if args.setup {
        let catalog = FixtureCatalog.load_catalog()?;
        let setup: Vec<String> = fixture_ddl(&catalog)
            .lines()
            .map(|l| l.to_string())
            .chain(fixture_seed_sql())
            .collect();
        for sql in &setup {
            match ex.apply(sql) {
                StmtOutcome::Error { sqlstate, message } => {
                    return Err(format!("setup failed ({sqlstate} {message}): {sql}"));
                }
                StmtOutcome::ConnLost { detail } => {
                    return Err(format!("setup lost the connection ({detail}): {sql}"));
                }
                _ => {}
            }
        }
    }

    let mut applied = 0u64;
    let mut ok = 0u64;
    let mut errors = 0u64;
    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line.map_err(|e| format!("stdin: {e}"))?;
        let sql = line.trim();
        if sql.is_empty() || sql.starts_with("--") {
            continue;
        }
        applied += 1;
        match ex.apply(sql) {
            // Server-side errors are expected traffic (division by zero,
            // overflow, ...): they exercise error paths, which is coverage.
            StmtOutcome::Error { .. } => errors += 1,
            StmtOutcome::ConnLost { detail } => {
                eprintln!(
                    "covapply: connection lost after {applied} statements: {detail}"
                );
                return Ok(ExitCode::from(2));
            }
            _ => ok += 1,
        }
    }
    eprintln!("covapply: applied={applied} ok={ok} errors={errors}");
    if args.copybin {
        // Single-engine COPY BINARY deck: rides the simple protocol via a
        // dedicated connection so the xproto mode planner never routes a
        // COPY FROM feed onto the extended path.
        let mut cex = ClientExecutor::connect_opts(&args.host, port, &args.db, &args.user, None)?;
        let (capplied, cerrors) = fuzzgen::copybin::run_single(&mut cex);
        eprintln!("covapply: copybin applied={capplied} errors={cerrors}");
    }
    if args.copytext {
        // Simple-protocol connection for the same reason as --copybin.
        let mut cex = ClientExecutor::connect_opts(&args.host, port, &args.db, &args.user, None)?;
        let (capplied, cerrors) = fuzzgen::copytext::run_single(
            &mut cex,
            args.copytext_seed,
            args.copytext_count,
        );
        eprintln!("covapply: copytext applied={capplied} errors={cerrors}");
    }
    if args.copyopts {
        // Simple-protocol connection for the same reason as --copytext.
        let mut cex = ClientExecutor::connect_opts(&args.host, port, &args.db, &args.user, None)?;
        let (capplied, cerrors) = fuzzgen::copyopts::run_single(
            &mut cex,
            args.copyopts_seed,
            args.copyopts_count,
        );
        eprintln!("covapply: copyopts applied={capplied} errors={cerrors}");
    }
    if args.dbddl {
        let spec = fuzzgen::dbddl::ConnSpec {
            host: args.host.clone(),
            port,
            db: args.db.clone(),
            user: args.user.clone(),
            tsdir: std::env::temp_dir()
                .join(format!("fz_q8ts_cov_{port}"))
                .to_string_lossy()
                .into_owned(),
        };
        let (capplied, cerrors) = fuzzgen::dbddl::run_single(&spec, port as u32)?;
        eprintln!("covapply: dbddl applied={capplied} errors={cerrors}");
    }
    Ok(ExitCode::SUCCESS)
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("covapply: {e}");
            eprintln!("{USAGE}");
            ExitCode::FAILURE
        }
    }
}
