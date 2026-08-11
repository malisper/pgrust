//! Thin CLI over the fuzzgen library.

use std::io::Write;
use std::process::ExitCode;

use fuzzgen::catalog::{fixture_ddl, CatalogSource, FixtureCatalog};
use fuzzgen::rng::Rng;
use fuzzgen::session::{jsonl_record, run_session, SessionConfig};
use fuzzgen::toggles::ToggleVector;

const USAGE: &str = "\
usage: fuzzgen [options]
  --seed <u64>       session seed (default 0); the reproducibility witness
  --count <n>        statement budget (default 100)
  --modules <spec>   pin the toggle vector, e.g. expr=on or expr=on:2.5
  --swarm            swarm-random toggle vector sampled from the seed
                     (default: all modules on at default weights)
  --max-depth <n>    expression nesting bound (default 4)
  --format sql|jsonl output mode (default sql)
  --print-schema     emit fixture-schema DDL and exit
";

struct Args {
    seed: u64,
    count: u32,
    modules: Option<String>,
    swarm: bool,
    max_depth: u32,
    format: String,
    print_schema: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        seed: 0,
        count: 100,
        modules: None,
        swarm: false,
        max_depth: 4,
        format: "sql".to_string(),
        print_schema: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        let mut value = |name: &str| {
            it.next().ok_or_else(|| format!("{} requires a value", name))
        };
        match a.as_str() {
            "--seed" => args.seed = value("--seed")?.parse().map_err(|e| format!("bad --seed: {}", e))?,
            "--count" => args.count = value("--count")?.parse().map_err(|e| format!("bad --count: {}", e))?,
            "--modules" => args.modules = Some(value("--modules")?),
            "--swarm" => args.swarm = true,
            "--max-depth" => {
                args.max_depth = value("--max-depth")?.parse().map_err(|e| format!("bad --max-depth: {}", e))?
            }
            "--format" => {
                args.format = value("--format")?;
                if args.format != "sql" && args.format != "jsonl" {
                    return Err("--format must be sql or jsonl".to_string());
                }
            }
            "--print-schema" => args.print_schema = true,
            "--help" | "-h" => {
                print!("{}", USAGE);
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {:?}", other)),
        }
    }
    if args.modules.is_some() && args.swarm {
        return Err("--modules and --swarm are mutually exclusive".to_string());
    }
    Ok(args)
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let catalog = FixtureCatalog.load_catalog()?;

    if args.print_schema {
        print!("{}", fixture_ddl(&catalog));
        return Ok(());
    }

    // Toggle sampling draws from a PRNG stream derived only from the seed,
    // separate from the statement stream so a pinned vector and a
    // swarm-sampled vector that happen to match produce identical SQL.
    let toggles = match &args.modules {
        Some(spec) => ToggleVector::parse(spec)?,
        None if args.swarm => {
            // Domain-separate the toggle draw from the statement stream.
            ToggleVector::swarm(&mut Rng::new(args.seed ^ 0x7377_6172_6d5f_7476))
        }
        None => ToggleVector::all_on(),
    };

    let cfg = SessionConfig {
        seed: args.seed,
        toggles,
        budget: args.count,
        max_depth: args.max_depth,
    };
    let stmts = run_session(&cfg, &catalog);

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let spec = cfg.toggles.spec_string();
    if args.format == "sql" {
        // The seed is the witness — it leads the stream.
        writeln!(out, "-- fuzzgen seed={} modules={} count={}", cfg.seed, spec, cfg.budget)
            .map_err(|e| e.to_string())?;
        for s in &stmts {
            writeln!(out, "{}", s.sql).map_err(|e| e.to_string())?;
        }
    } else {
        for s in &stmts {
            writeln!(out, "{}", jsonl_record(cfg.seed, s)).map_err(|e| e.to_string())?;
        }
    }
    eprintln!("fuzzgen: seed={} modules={} statements={}", cfg.seed, spec, stmts.len());
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("fuzzgen: {}", e);
            eprintln!("{}", USAGE);
            ExitCode::FAILURE
        }
    }
}
