//! simharness — H1 sim-harness CLI (WS-RUNNER owns this file; contract §1.1).
//!
//! Verdict grammar (exact, contract §4.1.7):
//!   SIMHARNESS|<class>|<n>        one line per observed class
//!   SIMHARNESS-VERDICT|PASS       or  SIMHARNESS-VERDICT|FAIL:<p1>,<p2>
//!
//! Exit codes: 0 = PASS, 1 = FAIL (P1 present), 2 = harness error.

use clap::{Parser, Subcommand};
use simharness::runner;
use runner::simbridge;
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
    /// H5 rung B: fingerprint every Nth executed query via EXPLAIN (COSTS
    /// OFF) on the DUT (plan-species census; 0 = off). Default 1 = every
    /// query (measured overhead < 10 percent — see notes/h5-metrics.md).
    #[arg(long, default_value_t = 1)]
    explain_sample: u32,
    /// TEETH INSTRUMENT (metamorphic-oracle validation): wrap the DUT in a
    /// planted wrong-DUT that mis-evaluates NULL predicates (`IS NULL` =>
    /// `IS NULL AND false` on SELECTs). TLP/NoREC must fire; never use in
    /// battery configs.
    #[arg(long, default_value_t = false)]
    test_null_bug: bool,
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
            explain_every: self.explain_sample,
            test_null_bug: self.test_null_bug,
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
        /// H6 QPG-lite species-guided scheduling. OFF by default and off for
        /// every estimation arm: guided campaigns bias Good-Turing U (FSE'21
        /// adaptive-bias result), so U is suppressed whenever this is on.
        /// Flag off = seed sequence byte-identical to the H5 loop.
        #[arg(long, default_value_t = false)]
        species_sched: bool,
        /// Guided mode: neighbor seeds a productive seed earns.
        #[arg(long, default_value_t = 4)]
        sched_neighbors: u32,
        /// Guided mode: consecutive unproductive seeds before the neighbor
        /// queue decays back to pure sequential scheduling.
        #[arg(long, default_value_t = 8)]
        sched_decay: u32,
        /// H7: DUT server-log path to scrape for `panicked at` lines after
        /// every seed. Each hit mints a `panic-signature` P1 (verdict FAIL)
        /// — the only witness for contained panics whose payload matches
        /// C's error text byte-for-byte (the p9 interval-typmod class).
        #[arg(long)]
        dut_log: Option<PathBuf>,
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
    /// SIM-HARNESS-CONVERGE: drive v1 plans INSIDE the deterministic
    /// simulator (P13 registered-backend corpus). Model-oracle + property
    /// checks; diff-c is N/A inside the sim (disclosed in the output).
    SimRun {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value_t = 1)]
        seed_base: u64,
        #[arg(long, default_value_t = 100)]
        seeds: u64,
        /// The scheduler seed (PGRUST_SIM_SEED) — the second axis.
        #[arg(long, default_value_t = 7)]
        sched_seed: u64,
        /// Sim-built postgres binary (RUSTFLAGS=--cfg pgrust_sim).
        #[arg(long)]
        sim_bin: PathBuf,
        /// C-initdb datadir image the sim universe seeds from.
        #[arg(long)]
        datadir: PathBuf,
        /// Dir containing timezone/ and timezonesets/ (link-resolved).
        #[arg(long)]
        share_dir: PathBuf,
        #[arg(long, default_value = "simbridge-out")]
        out: PathBuf,
        /// First N seeds also get the x3 byte-identity proof.
        #[arg(long, default_value_t = 2)]
        x3: u64,
        /// First N seeds also get the serial-semantics proof (a second
        /// schedule seed must yield the identical parsed outcome stream).
        #[arg(long, default_value_t = 4)]
        serialsem: u64,
        #[arg(long, default_value_t = 180)]
        timeout_s: u64,
        /// TEETH INSTRUMENT: doctor the script with the NullBug rewrite —
        /// the model oracle MUST fire (planted-red validation of the whole
        /// bridge stack). Never in battery configs.
        #[arg(long, default_value_t = false)]
        test_null_bug: bool,
    },
    /// SIM-HARNESS-CONVERGE fault composition: crash-cut mid-plan (whole-
    /// node kill via the FaultDriver spec channel), pack the at-cut image,
    /// reboot through PRODUCT crash recovery, re-verify the model's
    /// crash-committed tables. --red weakens writer durability (fsync=off)
    /// and must be CAUGHT.
    SimFault {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value_t = 1)]
        seed_base: u64,
        #[arg(long, default_value_t = 100)]
        seeds: u64,
        #[arg(long, default_value_t = 7)]
        sched_seed: u64,
        #[arg(long)]
        sim_bin: PathBuf,
        #[arg(long)]
        datadir: PathBuf,
        #[arg(long)]
        share_dir: PathBuf,
        #[arg(long, default_value = "simfault-out")]
        out: PathBuf,
        #[arg(long, default_value_t = 180)]
        timeout_s: u64,
        #[arg(long, default_value_t = false)]
        red: bool,
    },
    /// SIM-CONVERGE inc-2: drive ONE H8 v2 TWO-session plan inside the sim —
    /// the plan's serialized interleaving maps onto the P13 corpus's two
    /// registered backends via the cross-session turn gate
    /// (PGRUST_SIMNET_TURNS, completion-ordered). Arms: green (+x3 identity
    /// + alt-sched observation), --red-order (gate OFF: the pre-lane race —
    /// the serialized-order model walk must catch it), --red-wedge (a wedged
    /// schedule must die as the named SCHEDCEILING verdict, never a panic),
    /// --test-null-bug (the TEETH instrument on the cross-session read).
    SimTwo {
        /// v2 plan file; omitted = the built-in cross-session fixture.
        #[arg(long)]
        plan: Option<PathBuf>,
        #[arg(long, default_value_t = 7)]
        sched_seed: u64,
        #[arg(long)]
        sim_bin: PathBuf,
        #[arg(long)]
        datadir: PathBuf,
        #[arg(long)]
        share_dir: PathBuf,
        #[arg(long, default_value = "simtwo-out")]
        out: PathBuf,
        /// Extra identical repetitions (2 = the x3 law).
        #[arg(long, default_value_t = 2)]
        x3: u64,
        /// Alternate schedule seeds to run as OBSERVATION legs (reported,
        /// not asserted — the determinism law binds bytes to (plan, seed)).
        #[arg(long, default_value_t = 2)]
        alt_scheds: u64,
        #[arg(long, default_value_t = 180)]
        timeout_s: u64,
        #[arg(long, default_value_t = false)]
        red_order: bool,
        #[arg(long, default_value_t = false)]
        red_wedge: bool,
        #[arg(long, default_value_t = false)]
        test_null_bug: bool,
        /// SIM-CONVERGE inc-3 planted red: perturb the NATIVE replay pool's
        /// session-B stream — the native-vs-re-zip agreement MUST catch it.
        #[arg(long, default_value_t = false)]
        red_pool: bool,
    },
    /// SIM-CONVERGE inc-3: drive v2 multi-session plans NATIVELY inside the
    /// sim — per-session scripts + TYPED turns (dispatch/join split for
    /// async, poll turns for WaitUntil, up to 4 sessions = sim s2..s5), then
    /// the model-oracle walk through the session-aware replay pool (the real
    /// execute_plan over the ORIGINAL v2 plan). Sources: --plan file,
    /// --profile (generated campaign, one plan per seed), or the built-in
    /// --fixture. Reds: --red-asyncturn (completion-ordered async turn =
    /// schedule deadlock -> named SCHEDCEILING), --red-detector (doctored S1
    /// detector read -> RowsEq property-violation), --test-null-bug.
    SimMulti {
        /// v2 plan file; see also --profile and --fixture.
        #[arg(long)]
        plan: Option<PathBuf>,
        /// Built-in fixture when neither --plan nor --profile: "async".
        #[arg(long, default_value = "async")]
        fixture: String,
        /// Generated mode: profile path (one v2 plan per workload seed).
        #[arg(long)]
        profile: Option<String>,
        #[arg(long, default_value_t = 1)]
        seed_base: u64,
        #[arg(long, default_value_t = 1)]
        seeds: u64,
        #[arg(long, default_value_t = 7)]
        sched_seed: u64,
        #[arg(long)]
        sim_bin: PathBuf,
        #[arg(long)]
        datadir: PathBuf,
        #[arg(long)]
        share_dir: PathBuf,
        #[arg(long, default_value = "simmulti-out")]
        out: PathBuf,
        /// First N plans also get the x3 byte-identity proof.
        #[arg(long, default_value_t = 2)]
        x3: u64,
        #[arg(long, default_value_t = 180)]
        timeout_s: u64,
        #[arg(long, default_value_t = false)]
        red_asyncturn: bool,
        #[arg(long, default_value_t = false)]
        red_detector: bool,
        #[arg(long, default_value_t = false)]
        test_null_bug: bool,
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
        Cmd::Run {
            profile,
            seed_base,
            seeds,
            bugbase,
            out,
            replay_times,
            repros,
            species_sched,
            sched_neighbors,
            sched_decay,
            dut_log,
            engine,
        } => {
            let lp = load_profile(&profile)?;
            let cfg = engine.config();
            let cli_words: Vec<String> = std::env::args().collect();
            let sched_cfg = runner::schedule::ScheduleConfig {
                enabled: species_sched,
                neighbors: sched_neighbors,
                decay: sched_decay,
            };
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
                &sched_cfg,
                dut_log.as_deref(),
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
        Cmd::SimRun {
            profile,
            seed_base,
            seeds,
            sched_seed,
            sim_bin,
            datadir,
            share_dir,
            out,
            x3,
            serialsem,
            timeout_s,
            test_null_bug,
        } => {
            let lp = load_profile(&profile)?;
            let args = simbridge::BridgeArgs {
                lp,
                seed_base,
                seeds,
                sched_seed,
                world: simbridge::SimWorld { sim_bin, datadir, share_dir, timeout_s },
                out,
                x3,
                serialsem,
                test_null_bug,
            };
            Ok(simbridge::run_bridge_campaign(&args))
        }
        Cmd::SimFault {
            profile,
            seed_base,
            seeds,
            sched_seed,
            sim_bin,
            datadir,
            share_dir,
            out,
            timeout_s,
            red,
        } => {
            let lp = load_profile(&profile)?;
            let args = simbridge::FaultArgs {
                lp,
                seed_base,
                seeds,
                sched_seed,
                world: simbridge::SimWorld { sim_bin, datadir, share_dir, timeout_s },
                out,
                red,
            };
            Ok(simbridge::run_fault_campaign(&args))
        }
        Cmd::SimTwo {
            plan,
            sched_seed,
            sim_bin,
            datadir,
            share_dir,
            out,
            x3,
            alt_scheds,
            timeout_s,
            red_order,
            red_wedge,
            test_null_bug,
            red_pool,
        } => {
            let args = simbridge::TwoArgs {
                plan_path: plan,
                sched_seed,
                world: simbridge::SimWorld { sim_bin, datadir, share_dir, timeout_s },
                out,
                x3,
                alt_scheds,
                red_order,
                red_wedge,
                test_null_bug,
                red_pool,
            };
            Ok(simbridge::run_two_session_campaign(&args))
        }
        Cmd::SimMulti {
            plan,
            fixture,
            profile,
            seed_base,
            seeds,
            sched_seed,
            sim_bin,
            datadir,
            share_dir,
            out,
            x3,
            timeout_s,
            red_asyncturn,
            red_detector,
            test_null_bug,
        } => {
            let lp = match &profile {
                Some(p) => Some(load_profile(p)?),
                None => None,
            };
            let args = simbridge::MultiArgs {
                plan_path: plan,
                fixture,
                lp,
                seed_base,
                seeds,
                sched_seed,
                world: simbridge::SimWorld { sim_bin, datadir, share_dir, timeout_s },
                out,
                x3,
                red_asyncturn,
                red_detector,
                test_null_bug,
            };
            Ok(simbridge::run_multi_campaign(&args))
        }
    }
}
