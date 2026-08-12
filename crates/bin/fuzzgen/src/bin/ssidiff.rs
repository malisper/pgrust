//! ssidiff — multi-session concurrency differential CLI (Q4 lane,
//! serializable-ssi chunk). Runs the hand-authored SSI/lock scenario deck
//! (and optionally seeded random interleavings) against two engines and
//! reports divergences. See fuzzgen::ssi for the engine + compare model.
//!
//! Exit codes: 0 = clean, 2 = divergences found, 1 = rig/usage error.

use std::io::Write;
use std::process::ExitCode;

use fuzzgen::ssi::{
    deck, random_scenario, run_differential, scenario_by_name, Endpoint, Scenario, Tunables,
};

const USAGE: &str = "\
usage: ssidiff --a <host:port> --b <host:port> [options]
  --a <host:port>       reference server (A: C Postgres)
  --b <host:port>       candidate server (B: pgrust; may equal --a's engine
                        on a second port for a self-test leg)
  --db <name>           database on both sides (default: fuzz)
  --user <name>         role on both sides (default: postgres)
  --scenario <name>     run one deck scenario (repeatable; default: all)
  --list                list deck scenarios and exit
  --rand-seeds <a..b>   ALSO run seeded random interleavings for the
                        inclusive seed range, e.g. 1..25
  --rand-steps <n>      statements per session in random mode (default 6)
  --findings <path>     findings JSONL (default: stdout)
  --transcripts <dir>   write per-scenario A/B transcripts on divergence
  --block-window-ms <n> wall-clock blocked-classification fallback window
                        (default 2500)
  --step-deadline-ms <n> hard per-step rig deadline (default 25000)
";

struct Args {
    a: Endpoint,
    b: Endpoint,
    scenarios: Vec<String>,
    list: bool,
    rand_seeds: Option<(u64, u64)>,
    rand_steps: usize,
    findings: Option<String>,
    transcripts: Option<String>,
    tun: Tunables,
}

fn parse_hostport(s: &str, db: &str, user: &str) -> Result<Endpoint, String> {
    let (host, port) = s
        .rsplit_once(':')
        .ok_or_else(|| format!("endpoint must be host:port, got {s:?}"))?;
    Ok(Endpoint {
        host: host.to_string(),
        port: port.parse().map_err(|e| format!("bad port {port:?}: {e}"))?,
        db: db.to_string(),
        user: user.to_string(),
    })
}

fn parse_args() -> Result<Args, String> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut a = None;
    let mut b = None;
    let mut db = "fuzz".to_string();
    let mut user = "postgres".to_string();
    let mut scenarios = Vec::new();
    let mut list = false;
    let mut rand_seeds = None;
    let mut rand_steps = 6usize;
    let mut findings = None;
    let mut transcripts = None;
    let mut tun = Tunables::default();
    let mut i = 0;
    let need = |i: &mut usize, argv: &[String], flag: &str| -> Result<String, String> {
        *i += 1;
        argv.get(*i).cloned().ok_or_else(|| format!("{flag} needs a value"))
    };
    while i < argv.len() {
        match argv[i].as_str() {
            "--a" => a = Some(need(&mut i, &argv, "--a")?),
            "--b" => b = Some(need(&mut i, &argv, "--b")?),
            "--db" => db = need(&mut i, &argv, "--db")?,
            "--user" => user = need(&mut i, &argv, "--user")?,
            "--scenario" => scenarios.push(need(&mut i, &argv, "--scenario")?),
            "--list" => list = true,
            "--rand-seeds" => {
                let v = need(&mut i, &argv, "--rand-seeds")?;
                let (lo, hi) = v
                    .split_once("..")
                    .ok_or_else(|| format!("--rand-seeds wants a..b, got {v:?}"))?;
                rand_seeds = Some((
                    lo.parse().map_err(|e| format!("bad seed {lo:?}: {e}"))?,
                    hi.parse().map_err(|e| format!("bad seed {hi:?}: {e}"))?,
                ));
            }
            "--rand-steps" => {
                rand_steps = need(&mut i, &argv, "--rand-steps")?
                    .parse()
                    .map_err(|e| format!("bad --rand-steps: {e}"))?
            }
            "--findings" => findings = Some(need(&mut i, &argv, "--findings")?),
            "--transcripts" => transcripts = Some(need(&mut i, &argv, "--transcripts")?),
            "--block-window-ms" => {
                tun.block_window_ms = need(&mut i, &argv, "--block-window-ms")?
                    .parse()
                    .map_err(|e| format!("bad --block-window-ms: {e}"))?
            }
            "--step-deadline-ms" => {
                tun.step_deadline_ms = need(&mut i, &argv, "--step-deadline-ms")?
                    .parse()
                    .map_err(|e| format!("bad --step-deadline-ms: {e}"))?
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
        i += 1;
    }
    if list {
        // endpoints not needed for --list
        let dummy = Endpoint {
            host: String::new(),
            port: 0,
            db: db.clone(),
            user: user.clone(),
        };
        return Ok(Args {
            a: dummy.clone(),
            b: dummy,
            scenarios,
            list,
            rand_seeds,
            rand_steps,
            findings,
            transcripts,
            tun,
        });
    }
    let a = parse_hostport(&a.ok_or("--a required")?, &db, &user)?;
    let b = parse_hostport(&b.ok_or("--b required")?, &db, &user)?;
    Ok(Args { a, b, scenarios, list, rand_seeds, rand_steps, findings, transcripts, tun })
}

fn json_escape(s: &str) -> String {
    fuzzgen::session::json_escape(s)
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("ssidiff: {e}\n{USAGE}");
            return ExitCode::from(1);
        }
    };
    if args.list {
        for sc in deck() {
            println!("{:24} {}", sc.name, sc.description);
        }
        return ExitCode::SUCCESS;
    }

    let mut out: Box<dyn Write> = match &args.findings {
        Some(p) => match std::fs::File::create(p) {
            Ok(f) => Box::new(f),
            Err(e) => {
                eprintln!("ssidiff: cannot create findings file {p}: {e}");
                return ExitCode::from(1);
            }
        },
        None => Box::new(std::io::stdout()),
    };

    // Assemble the run list.
    let mut list: Vec<Scenario> = Vec::new();
    if args.scenarios.is_empty() {
        list.extend(deck());
    } else {
        for name in &args.scenarios {
            match scenario_by_name(name) {
                Some(sc) => list.push(sc),
                None => {
                    eprintln!("ssidiff: unknown scenario {name:?} (--list to enumerate)");
                    return ExitCode::from(1);
                }
            }
        }
    }
    if let Some((lo, hi)) = args.rand_seeds {
        for seed in lo..=hi {
            list.push(random_scenario(seed, args.rand_steps));
        }
    }

    let _ = writeln!(
        out,
        "{{\"meta\":\"ssidiff\",\"a\":\"{}:{}\",\"b\":\"{}:{}\",\"scenarios\":{}}}",
        args.a.host, args.a.port, args.b.host, args.b.port, list.len()
    );

    let mut n_div = 0usize;
    let mut n_rig = 0usize;
    let mut monitor_gap_reported = false;
    for sc in &list {
        eprint!("ssidiff: {:24} ", sc.name);
        match run_differential(&args.a, &args.b, sc, &args.tun) {
            Ok(v) => {
                if (v.monitor_gap_a || v.monitor_gap_b) && !monitor_gap_reported {
                    monitor_gap_reported = true;
                    let side = if v.monitor_gap_a { "A" } else { "B" };
                    eprintln!(
                        "\nssidiff: NOTE: wait-state introspection unavailable on {side} \
                         (pg_stat_activity wait_event probe failed); blocked detection \
                         fell back to the {}ms window (inventory note, not a finding)",
                        args.tun.block_window_ms
                    );
                    eprint!("ssidiff: {:24} ", sc.name);
                }
                if v.divergences.is_empty() {
                    eprintln!("ok");
                } else {
                    n_div += v.divergences.len();
                    eprintln!("DIVERGED ({})", v.divergences.len());
                    for d in &v.divergences {
                        let _ = writeln!(
                            out,
                            "{{\"scenario\":\"{}\",\"class\":\"{}\",\"detail\":\"{}\"}}",
                            json_escape(v.scenario),
                            d.class,
                            json_escape(&d.detail)
                        );
                    }
                }
                // Transcripts always written when a dir is given (the
                // hand-verification evidence), divergence or not.
                if let Some(dir) = &args.transcripts {
                    let _ = std::fs::create_dir_all(dir);
                    let _ = std::fs::write(
                        format!("{dir}/{}-A.txt", v.scenario),
                        &v.transcript_a,
                    );
                    let _ = std::fs::write(
                        format!("{dir}/{}-B.txt", v.scenario),
                        &v.transcript_b,
                    );
                }
            }
            Err(e) => {
                n_rig += 1;
                eprintln!("RIG-ERROR: {e}");
                let _ = writeln!(
                    out,
                    "{{\"scenario\":\"{}\",\"class\":\"RIG\",\"detail\":\"{}\"}}",
                    json_escape(sc.name),
                    json_escape(&e)
                );
            }
        }
    }
    eprintln!(
        "ssidiff: done — {} scenario(s), {} divergence(s), {} rig error(s)",
        list.len(),
        n_div,
        n_rig
    );
    if n_rig > 0 {
        ExitCode::from(1)
    } else if n_div > 0 {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    }
}
