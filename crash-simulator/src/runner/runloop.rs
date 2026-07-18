//! Run loop, replay-from-seed, and distillation (contract §4.1.3/4/6).

use super::bugbase::{now_utc_string, BugBase, RunsJson};
use super::driver::{
    execute_plan, BasicCheckEval, BasicDiffClassifier, ExecOptions, PgSession, RunReport, Session,
    Signature,
};
use super::planface::Plan;
use super::profile::LoadedProfile;
use super::shrink;
use super::stubgen;
use super::verdict::{severity, ArtifactWriter, Census};
use std::path::Path;

pub struct EngineConfig {
    pub dut_conninfo: String,
    pub cpg_conninfo: Option<String>,
    pub restart_cmd: Option<String>,
    /// SQL run at (re)connect on every leg: statement_timeout etc.
    pub session_setup: Vec<String>,
    /// Fresh-schema SQL run before each seed (property-local isolation).
    pub per_seed_reset: Vec<String>,
}

impl EngineConfig {
    pub fn default_setup() -> Vec<String> {
        vec!["SET statement_timeout = '5s'".to_string()]
    }
    pub fn default_reset() -> Vec<String> {
        vec![
            "DROP SCHEMA IF EXISTS simharness CASCADE".to_string(),
            "CREATE SCHEMA simharness".to_string(),
            "SET search_path = simharness".to_string(),
        ]
    }
}

/// Generate the plan for a seed. THE integration point with WS-GEN: today
/// this calls the scaffold generator; post-integration it calls
/// `crate::gen::generate` behind the same signature.
pub fn gen_plan(seed: u64, lp: &LoadedProfile, generator_version: &str) -> Plan {
    stubgen::generate(seed, &lp.profile, &lp.sha256, generator_version)
}

/// Generator version string recorded in plan headers: the harness build's
/// git sha (compile-time env, falls back to "dev").
pub fn generator_version() -> String {
    option_env!("SIMHARNESS_GIT_SHA").unwrap_or("dev").to_string()
}

pub struct SeedRun {
    pub seed: u64,
    pub report: RunReport,
    pub plan_text: String,
}

fn connect_legs(
    cfg: &EngineConfig,
) -> Result<(PgSession, Option<PgSession>), String> {
    let dut = PgSession::connect("pgrust", &cfg.dut_conninfo, &cfg.session_setup)?;
    let cpg = match &cfg.cpg_conninfo {
        Some(ci) => Some(PgSession::connect("cpg", ci, &cfg.session_setup)?),
        None => None,
    };
    Ok((dut, cpg))
}

fn reset_leg(s: &mut dyn Session, resets: &[String]) -> Result<(), String> {
    for sql in resets {
        if let super::driver::ExecOutcome::SqlError { sqlstate, message } = s.execute(sql) {
            return Err(format!("{}: per-seed reset '{}': {} {}", s.engine(), sql, sqlstate, message));
        }
    }
    Ok(())
}

pub fn run_one_seed(seed: u64, lp: &LoadedProfile, cfg: &EngineConfig) -> Result<SeedRun, String> {
    let plan = gen_plan(seed, lp, &generator_version());
    let plan_text = plan.render();
    let report = run_plan(&plan, cfg)?;
    Ok(SeedRun { seed, report, plan_text })
}

pub fn run_plan(plan: &Plan, cfg: &EngineConfig) -> Result<RunReport, String> {
    let (mut dut, mut cpg) = connect_legs(cfg)?;
    reset_leg(&mut dut, &cfg.per_seed_reset)?;
    if let Some(c) = cpg.as_mut() {
        reset_leg(c, &cfg.per_seed_reset)?;
    }
    // NOTE: reconnect() must restore search_path; extend session_setup so
    // Fault(Disconnect) legs land back in the harness schema.
    let opts = ExecOptions { restart_cmd: cfg.restart_cmd.clone(), stop_on_failure: true };
    Ok(execute_plan(
        plan,
        &mut dut,
        cpg.as_mut().map(|c| c as &mut dyn Session),
        &BasicCheckEval,
        &BasicDiffClassifier,
        &opts,
    ))
}

/// Replay-N flake policy (contract §4.1.4): re-run K times, report re-fail
/// rate. Probabilistic failures are findings-with-a-plan, never
/// gate-blockers (spec HR3).
pub fn replay_n(plan: &Plan, cfg: &EngineConfig, times: u32) -> Result<(u32, u32, Option<Signature>), String> {
    let mut refails = 0;
    let mut last_sig = None;
    for _ in 0..times {
        let report = run_plan(plan, cfg)?;
        if let Some(f) = report.failure {
            refails += 1;
            last_sig = Some(f.signature);
        }
    }
    Ok((times, refails, last_sig))
}

pub struct CampaignOutcome {
    pub census: Census,
    pub seeds_run: u64,
    pub failures_banked: u64,
}

#[allow(clippy::too_many_arguments)]
pub fn run_campaign(
    lp: &LoadedProfile,
    cfg: &EngineConfig,
    seed_base: u64,
    seed_count: u64,
    bugbase_dir: &Path,
    out_dir: &Path,
    cli: &[String],
    replay_times: u32,
    repros_path: Option<&Path>,
) -> Result<CampaignOutcome, String> {
    let bb = BugBase::new(bugbase_dir);
    let mut census = Census::default();
    let mut artifacts = ArtifactWriter::new(out_dir)?;
    let mut failures_banked = 0u64;

    // Declared engagement floors: instrument absent at H1 (§0 A4) — counted
    // skip, never silent.
    if !lp.profile.engagement_floors.is_empty() {
        census.add("floor-skipped-no-instrument", 1);
    }

    for i in 0..seed_count {
        let seed = seed_base + i;
        let run = run_one_seed(seed, lp, cfg)?;
        census.merge(&run.report.class_counts);
        for r in &run.report.records {
            artifacts.record(seed, &r.class, &r.sev, &r.detail, &r.stmt_head);
        }
        if let Some(failure) = &run.report.failure {
            // Bank: replay-N first (flake evidence), then shrink with
            // same-signature keep, then distill.
            let plan = Plan::parse(&run.plan_text).map_err(|e| format!("re-parse banked plan: {}", e))?;
            let (attempts, refails, _) = replay_n(&plan, cfg, replay_times)?;
            let mut rerun = |cand: &Plan| -> Option<Signature> {
                run_plan(cand, cfg).ok().and_then(|r| r.failure.map(|f| f.signature))
            };
            let shrunk =
                shrink::shrink(&plan, failure.step_idx, &failure.signature, &mut rerun);
            let shrunk_text = shrunk.as_ref().map(|p| p.render());
            let runs = RunsJson {
                seed,
                commit_sha: generator_version(),
                timestamp_utc: now_utc_string(),
                signature: failure.signature.clone(),
                class: failure.class.clone(),
                sev: failure.sev.clone(),
                detail: failure.detail.clone(),
                step_idx: failure.step_idx,
                cli: cli.to_vec(),
                profile_name: lp.profile.name.clone(),
                profile_sha256: lp.sha256.clone(),
                replay_attempts: attempts,
                replay_refails: refails,
            };
            bb.bank(seed, &run.plan_text, shrunk_text.as_deref(), &runs)?;
            failures_banked += 1;
            if let Some(rp) = repros_path {
                distill(rp, seed, &lp.profile.name, failure, shrunk.as_ref().unwrap_or(&plan))?;
            }
        }
    }

    artifacts.finish(
        &census,
        serde_json::json!({
            "profile": lp.profile.name,
            "profile_sha256": lp.sha256,
            "seed_base": seed_base,
            "seed_count": seed_count,
            "failures_banked": failures_banked,
            "generator": generator_version(),
        }),
    )?;
    Ok(CampaignOutcome { census, seeds_run: seed_count, failures_banked })
}

/// Distillation (contract §4.1.6): banked failure -> statement-corpus entry
/// under scripts/sqlsmith/repros-simharness.sql conventions, band 95001+.
/// The band cursor lives in the file itself (max existing `-- test NNNNN`).
pub fn distill(
    repros_path: &Path,
    seed: u64,
    profile: &str,
    failure: &super::driver::Failure,
    plan: &Plan,
) -> Result<(), String> {
    const BAND_BASE: u64 = 95001;
    let existing = std::fs::read_to_string(repros_path).unwrap_or_default();
    let mut next = BAND_BASE;
    for line in existing.lines() {
        if let Some(r) = line.strip_prefix("-- test ") {
            if let Ok(n) = r.split_whitespace().next().unwrap_or("").parse::<u64>() {
                if n >= next {
                    next = n + 1;
                }
            }
        }
    }
    let mut entry = String::new();
    entry.push_str(&format!(
        "\n-- test {} class={} seed={} profile={} site={}\n",
        next, failure.class, seed, profile,
        failure.signature.site.replace('\n', " ")
    ));
    // Plain SQL: the executable steps of the (shrunk) plan.
    for step in &plan.steps {
        use super::planface::Step;
        match step {
            Step::Ddl(s) | Step::Dml(s) | Step::Query(s) => {
                entry.push_str(&s.text);
                entry.push_str(";\n");
            }
            Step::Tx(tx) => {
                entry.push_str(&super::driver::tx_sql(tx));
                entry.push_str(";\n");
            }
            Step::Arm(arm) => {
                entry.push_str(&super::driver::arm_sql(arm));
                entry.push_str(";\n");
            }
            _ => {}
        }
    }
    let mut content = existing;
    if content.is_empty() {
        content.push_str(REPROS_HEADER);
    }
    content.push_str(&entry);
    std::fs::write(repros_path, content).map_err(|e| e.to_string())
}

pub const REPROS_HEADER: &str = "\
-- repros-simharness.sql — statements distilled from simharness banked
-- failures (contract §4.1.6). Band: 95001+ (dualexec precedent: cursors
-- 92001+, spi 93001+; band claim recorded in notes/h1-ws-runner.md).
-- Each entry: `-- test NNNNN class=<class> seed=<seed> profile=<name>`
-- followed by plain SQL. Feeds the existing replay e2e gates.
";

/// Emit census + verdict in the house grammar (contract §4.1.7).
pub fn emit_verdict(census: &Census) {
    let mut out = std::io::stdout().lock();
    let _ = census.emit(&mut out);
}

pub fn class_is_p1(class: &str) -> bool {
    severity(class) == "P1"
}
