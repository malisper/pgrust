//! simharness — H1 sim-harness CLI (WS-RUNNER owns this file; contract §1.1).
//!
//! Verdict grammar (exact, contract §4.1.7):
//!   SIMHARNESS|<class>|<n>        one line per observed class
//!   SIMHARNESS-VERDICT|PASS       or  SIMHARNESS-VERDICT|FAIL:<p1>,<p2>
//!
//! Exit codes: 0 = PASS, 1 = FAIL (P1 present), 2 = harness error.

use clap::{Parser, Subcommand};
use simharness::runner;
use runner::bugbase::BugBase;
use runner::planface::Plan;
use runner::profile::load_profile;
use runner::runloop::{
    self, emit_verdict, gen_plan, gen_plan_ctx, generator_version, run_plan, EngineConfig,
};
use runner::verdict::Census;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "simharness", version, about = "pgrust H1 simulation harness (serial)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args, Clone)]
struct EngineArgs {
    /// DUT conninfo (pgrust; any PG-wire server for harness self-tests).
    #[arg(long, default_value = "host=/tmp port=5432 user=postgres dbname=postgres")]
    dut: String,
    /// C-PG conninfo: presence enables --diff-c semantics.
    #[arg(long)]
    diff_c: Option<String>,
    /// Shell command that restarts the DUT (Fault reconnect-server); absent
    /// => counted skip.
    #[arg(long)]
    restart_cmd: Option<String>,
    /// statement_timeout applied per session.
    #[arg(long, default_value = "5s")]
    statement_timeout: String,
}

impl EngineArgs {
    fn config(&self) -> EngineConfig {
        EngineConfig {
            dut_conninfo: self.dut.clone(),
            cpg_conninfo: self.diff_c.clone(),
            restart_cmd: self.restart_cmd.clone(),
            session_setup: vec![
                format!("SET statement_timeout = '{}'", self.statement_timeout),
                "SET search_path = simharness".to_string(),
            ],
            per_seed_reset: EngineConfig::default_reset(),
        }
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a plan (no execution). Plan determinism gate entry.
    Gen {
        #[arg(long)]
        seed: u64,
        #[arg(long)]
        profile: String,
        /// Write to file (default: stdout).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Validate a profile JSON.
    ValidateProfile { profile: String },
    /// Seed-per-iteration campaign; failures bank into the bugbase.
    Run {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value_t = 1)]
        seed_base: u64,
        #[arg(long, default_value_t = 100)]
        seeds: u64,
        #[arg(long, default_value = "bugbase")]
        bugbase: PathBuf,
        #[arg(long, default_value = "simharness-out")]
        out: PathBuf,
        /// replay-N flake policy: re-runs per banked failure.
        #[arg(long, default_value_t = 3)]
        replay_times: u32,
        /// Distillation target (band 95001+). Off unless given.
        #[arg(long)]
        repros: Option<PathBuf>,
        #[command(flatten)]
        engine: EngineArgs,
    },
    /// Replay from seed (regenerates the plan; byte-matches --plan if given).
    Replay {
        #[arg(long)]
        seed: u64,
        #[arg(long)]
        profile: String,
        /// Shipped .plan to byte-verify against the regenerated plan.
        #[arg(long)]
        plan: Option<PathBuf>,
        #[arg(long, default_value_t = 1)]
        times: u32,
        /// Generate + verify only; no server needed.
        #[arg(long, default_value_t = false)]
        no_exec: bool,
        #[command(flatten)]
        engine: EngineArgs,
    },
    /// List bugbase entries.
    List {
        #[arg(long, default_value = "bugbase")]
        bugbase: PathBuf,
    },
    /// Re-run banked bugs matching a signature filter; red = still failing.
    Test {
        #[arg(short = 'b', long, default_value = "")]
        filter: String,
        #[arg(long, default_value = "bugbase")]
        bugbase: PathBuf,
        /// Use the shrunk plan when present.
        #[arg(long, default_value_t = true)]
        shrunk: bool,
        #[command(flatten)]
        engine: EngineArgs,
    },
    /// Re-run every banked bug N times; report per-bug refail rates.
    Loop {
        #[arg(short = 'n', long, default_value_t = 5)]
        n: u32,
        #[arg(long, default_value = "bugbase")]
        bugbase: PathBuf,
        #[command(flatten)]
        engine: EngineArgs,
    },
}

fn main() {
    std::process::exit(match real_main() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("simharness: error: {}", e);
            2
        }
    });
}

fn real_main() -> Result<i32, String> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Gen { seed, profile, out } => {
            let lp = load_profile(&profile)?;
            let plan = gen_plan(seed, &lp, &generator_version());
            let text = plan.render();
            // Round-trip self-check on every gen (cheap, catches format skew).
            let re = Plan::parse(&text)?;
            if re != plan {
                return Err("gen: render/parse round-trip mismatch".into());
            }
            match out {
                Some(p) => std::fs::write(&p, &text).map_err(|e| e.to_string())?,
                None => print!("{}", text),
            }
            Ok(0)
        }
        Cmd::ValidateProfile { profile } => {
            let lp = load_profile(&profile)?;
            println!("profile '{}' OK sha256={}", lp.profile.name, lp.sha256);
            Ok(0)
        }
        Cmd::Run { profile, seed_base, seeds, bugbase, out, replay_times, repros, engine } => {
            let lp = load_profile(&profile)?;
            let cfg = engine.config();
            let cli_words: Vec<String> = std::env::args().collect();
            let outcome = runloop::run_campaign(
                &lp,
                &cfg,
                seed_base,
                seeds,
                &bugbase,
                &out,
                &cli_words,
                replay_times,
                repros.as_deref(),
            )?;
            emit_verdict(&outcome.census);
            Ok(if outcome.census.p1_classes().is_empty() { 0 } else { 1 })
        }
        Cmd::Replay { seed, profile, plan, times, no_exec, engine } => {
            let lp = load_profile(&profile)?;
            let (regen, ctx) = gen_plan_ctx(seed, &lp, &generator_version());
            let regen_text = regen.render();
            if let Some(p) = &plan {
                let shipped = std::fs::read_to_string(p).map_err(|e| e.to_string())?;
                if shipped != regen_text {
                    let shipped_gen = Plan::parse(&shipped)
                        .map(|pl| pl.header.generator)
                        .unwrap_or_else(|_| "<unparseable>".into());
                    return Err(format!(
                        "replay: regenerated plan does not byte-match {} — generator version delta (shipped generator: {}, this binary: {})",
                        p.display(),
                        shipped_gen,
                        generator_version()
                    ));
                }
                println!("replay: plan byte-match OK ({} bytes)", regen_text.len());
            }
            if no_exec {
                print!("{}", regen_text);
                return Ok(0);
            }
            let cfg = engine.config();
            let (attempts, refails, sig) = runloop::replay_n(&regen, &cfg, times, Some(&ctx))?;
            println!(
                "replay: seed={} attempts={} refails={} rate={:.2} sig={}",
                seed,
                attempts,
                refails,
                refails as f64 / attempts.max(1) as f64,
                sig.map(|s| s.to_key()).unwrap_or_else(|| "-".into())
            );
            // Replay-N flake policy: probabilistic failures are findings,
            // never gate-blockers; exit reflects reproduction only.
            Ok(if refails > 0 { 1 } else { 0 })
        }
        Cmd::List { bugbase } => {
            let bb = BugBase::new(&bugbase);
            let entries = bb.entries()?;
            println!("seed\tclass\tsev\tsignature\treplay(refails/attempts)\tprofile");
            for (seed, r) in &entries {
                println!(
                    "{}\t{}\t{}\t{}\t{}/{}\t{}",
                    seed,
                    r.class,
                    r.sev,
                    r.signature.to_key(),
                    r.replay_refails,
                    r.replay_attempts,
                    r.profile_name
                );
            }
            println!("total: {}", entries.len());
            Ok(0)
        }
        Cmd::Test { filter, bugbase, shrunk, engine } => {
            let bb = BugBase::new(&bugbase);
            let matches = bb.matching(&filter)?;
            if matches.is_empty() {
                println!("test: no banked bugs match '{}'", filter);
                return Ok(0);
            }
            let cfg = engine.config();
            let mut census = Census::default();
            let mut still_red = 0;
            for (seed, runs) in &matches {
                let text = bb
                    .load_plan(*seed, shrunk)
                    .or_else(|_| bb.load_plan(*seed, false))?;
                let plan = Plan::parse(&text)?;
                let report = run_plan(&plan, &cfg)?;
                census.merge(&report.class_counts);
                let red = report
                    .failure
                    .as_ref()
                    .map(|f| f.signature == runs.signature)
                    .unwrap_or(false);
                if red {
                    still_red += 1;
                }
                println!(
                    "test: seed={} banked={} rerun={}",
                    seed,
                    runs.signature.to_key(),
                    report
                        .failure
                        .as_ref()
                        .map(|f| f.signature.to_key())
                        .unwrap_or_else(|| "no-failure".into())
                );
            }
            emit_verdict(&census);
            println!("test: {}/{} still red", still_red, matches.len());
            // Exit must cohere with the verdict line: red if the banked
            // signature reproduced OR the reruns surfaced any P1 (a rerun
            // that fails with a DIFFERENT signature is still a failure —
            // grep-based gates key on SIMHARNESS-VERDICT|FAIL).
            Ok(if still_red > 0 || !census.p1_classes().is_empty() { 1 } else { 0 })
        }
        Cmd::Loop { n, bugbase, engine } => {
            let bb = BugBase::new(&bugbase);
            let entries = bb.entries()?;
            let cfg = engine.config();
            for (seed, runs) in &entries {
                let text = bb.load_plan(*seed, false)?;
                let plan = Plan::parse(&text)?;
                let (attempts, refails, _) = runloop::replay_n(&plan, &cfg, n, None)?;
                println!(
                    "loop: seed={} sig={} refails={}/{}",
                    seed,
                    runs.signature.to_key(),
                    refails,
                    attempts
                );
            }
            Ok(0)
        }
    }
}
