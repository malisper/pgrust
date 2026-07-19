//! Run loop, replay-from-seed, and distillation (contract §4.1.3/4/6).

use crate::bridge::{self, OracleCheckEval, OracleCtx, OracleDiffClassifier};
use super::bugbase::{now_utc_string, BugBase, RunsJson};
use super::driver::{
    execute_plan, CheckEval, ExecOptions, PgSession, RunReport, Session, Signature,
    SkipAllCheckEval,
};
use super::planface::Plan;
use super::profile::LoadedProfile;
use super::shrink;
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
    /// H5 rung B: fingerprint every Nth executed query via EXPLAIN (0 = off).
    pub explain_every: u32,
    /// TEETH INSTRUMENT (`--test-null-bug`): wrap the DUT session in the
    /// NULL-predicate-bug shim (planted wrong-DUT for metamorphic-oracle
    /// validation). Never set in battery configs.
    pub test_null_bug: bool,
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

/// Generate the plan for a seed — THE integration point with WS-GEN: drives
/// `crate::gen::generate_plan` with the oracle property registry and returns
/// the flat execution view.
pub fn gen_plan(seed: u64, lp: &LoadedProfile, generator_version: &str) -> Plan {
    gen_plan_ctx(seed, lp, generator_version).0
}

/// Generate the plan AND its oracle run context (property instances with
/// ledger ops + probe slots keyed by property seq). The context is derived
/// deterministically from the seed and never serialized (§0 A3).
pub fn gen_plan_ctx(seed: u64, lp: &LoadedProfile, generator_version: &str) -> (Plan, OracleCtx) {
    let (plan, ctx, _traces) = gen_plan_ctx_traced(seed, lp, generator_version);
    (plan, ctx)
}

/// `gen_plan_ctx` + the H5 production traces (plan bytes identical — trace
/// collection consumes no RNG draws).
pub fn gen_plan_ctx_traced(
    seed: u64,
    lp: &LoadedProfile,
    generator_version: &str,
) -> (Plan, OracleCtx, crate::gen::prodreg::GenTraces) {
    let gp = bridge::runner_profile_to_gen(&lp.profile);
    let (core, ctx, traces) =
        bridge::generate_plan_with_ctx_traced(seed, &gp, &lp.sha256, generator_version);
    (Plan::from_core(&core), ctx, traces)
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
) -> Result<(Box<dyn Session>, Option<PgSession>), String> {
    let dut = PgSession::connect("pgrust", &cfg.dut_conninfo, &cfg.session_setup)?;
    let dut: Box<dyn Session> = if cfg.test_null_bug {
        // Planted wrong-DUT (teeth): mis-evaluates NULL predicates.
        Box::new(super::driver::NullBugShim { inner: dut })
    } else {
        Box::new(dut)
    };
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
    let (plan, ctx) = gen_plan_ctx(seed, lp, &generator_version());
    let plan_text = plan.render();
    let report = run_plan_ctx(&plan, cfg, Some(&ctx))?;
    Ok(SeedRun { seed, report, plan_text })
}

pub fn run_plan(plan: &Plan, cfg: &EngineConfig) -> Result<RunReport, String> {
    run_plan_ctx(plan, cfg, None)
}

/// Run a plan. With an oracle context the model oracle is ON (ledger
/// reconcile + slot-addressed checks); without one, checks evaluate to a
/// counted skip (`property-skipped`) — never a false verdict (plans loaded
/// from disk without their seed's regenerated context, e.g. `test -b`).
pub fn run_plan_ctx(
    plan: &Plan,
    cfg: &EngineConfig,
    ctx: Option<&OracleCtx>,
) -> Result<RunReport, String> {
    let (mut dut, mut cpg) = connect_legs(cfg)?;
    reset_leg(dut.as_mut(), &cfg.per_seed_reset)?;
    if let Some(c) = cpg.as_mut() {
        reset_leg(c, &cfg.per_seed_reset)?;
    }
    // reconnect() restores session_setup (incl. search_path); ARM reset-all
    // replays post_reset_sql so generated unqualified names keep resolving.
    let session_sql: Vec<String> = cfg
        .session_setup
        .iter()
        .chain(cfg.per_seed_reset.iter())
        .filter(|s| s.to_ascii_uppercase().trim_start().starts_with("SET "))
        .cloned()
        .collect();
    let opts = ExecOptions {
        restart_cmd: cfg.restart_cmd.clone(),
        stop_on_failure: true,
        post_reset_sql: session_sql.clone(),
        explain_every: cfg.explain_every,
        // H8: worker sessions mirror the primary pair's connect discipline
        // (same conninfo, session_setup, and the per-seed SET replay so
        // unqualified names resolve in the simharness schema).
        session_pool: Some(crate::runner::sessions::SessionPoolConfig {
            dut_conninfo: cfg.dut_conninfo.clone(),
            cpg_conninfo: cfg.cpg_conninfo.clone(),
            session_sql: cfg
                .session_setup
                .iter()
                .cloned()
                .chain(session_sql.iter().cloned())
                .collect(),
        }),
        ..ExecOptions::default()
    };
    let classifier = OracleDiffClassifier::new(bridge::load_warts());
    let oracle_checks;
    let checks: &dyn CheckEval = match ctx {
        Some(c) => {
            // Engine-hook capability probe against the live DUT session
            // (contract §0 A5): one uncompared read per channel. Presence
            // un-gates F7 (Memory); absence keeps the counted skip.
            let hooks = bridge::probe_hooks(dut.as_mut());
            oracle_checks = OracleCheckEval::with_hooks(c, hooks);
            &oracle_checks
        }
        None => &SkipAllCheckEval,
    };
    Ok(execute_plan(
        plan,
        dut.as_mut(),
        cpg.as_mut().map(|c| c as &mut dyn Session),
        checks,
        &classifier,
        &opts,
    ))
}

/// Replay-N flake policy (contract §4.1.4): re-run K times, report re-fail
/// rate. Probabilistic failures are findings-with-a-plan, never
/// gate-blockers (spec HR3).
pub fn replay_n(
    plan: &Plan,
    cfg: &EngineConfig,
    times: u32,
    ctx: Option<&OracleCtx>,
) -> Result<(u32, u32, Option<Signature>), String> {
    let mut refails = 0;
    let mut last_sig = None;
    for _ in 0..times {
        let report = run_plan_ctx(plan, cfg, ctx)?;
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

/// H7: DUT server-log scrape for `panicked at` lines — the other half of H3
/// finding 1. A CONTAINED backend panic reaches the client as XX000 with the
/// PANIC PAYLOAD as the message; when the payload is a custom
/// `panic!("...")` string that is byte-identical to C's elog(ERROR) text on
/// the same path (the p9 interval-typmod class), the diff ladder sees
/// err-match and the client-side panic-signature grammar cannot match — the
/// server log's `panicked at` line is the ONLY witness. Each scraped hit
/// mints the existing `panic-signature` P1 census class, so the campaign
/// VERDICT fails, not just the census (the zero-panic ratchet's grammar).
/// Scanning starts at the log's size at campaign start, so earlier legs
/// sharing the same server log are never re-counted. Single-campaign-per-
/// server discipline: concurrent campaigns against one DUT would
/// cross-attribute lines (the validate rig runs legs sequentially).
struct DutLogScraper {
    path: std::path::PathBuf,
    offset: u64,
}

impl DutLogScraper {
    fn new(path: &Path) -> Self {
        let offset = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        DutLogScraper { path: path.to_path_buf(), offset }
    }

    /// `panicked at` lines appended since the last scan.
    fn scan(&mut self) -> Vec<String> {
        use std::io::{Read as _, Seek as _};
        let Ok(mut f) = std::fs::File::open(&self.path) else {
            return Vec::new();
        };
        if f.seek(std::io::SeekFrom::Start(self.offset)).is_err() {
            return Vec::new();
        }
        let mut buf = String::new();
        if f.read_to_string(&mut buf).is_err() {
            // Non-UTF8 window: skip past it rather than wedging the scraper.
            self.offset =
                std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(self.offset);
            return Vec::new();
        }
        self.offset += buf.len() as u64;
        buf.lines()
            .filter(|l| l.contains("panicked at"))
            .map(|l| l.to_string())
            .collect()
    }
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
    sched_cfg: &crate::runner::schedule::ScheduleConfig,
    dut_log: Option<&Path>,
) -> Result<CampaignOutcome, String> {
    let bb = BugBase::new(bugbase_dir);
    let mut census = Census::default();
    let mut artifacts = ArtifactWriter::new(out_dir)?;
    let mut failures_banked = 0u64;
    let mut log_scraper = dut_log.map(DutLogScraper::new);

    // H5 metrics accumulators (rung A: k-path over production traces; rung
    // B: plan-species census — blind arms only, see src/metrics.rs).
    let mut kacc = crate::gen::prodreg::KpathAccum::default();
    let mut species = crate::metrics::SpeciesCensus::default();

    // Declared engagement floors: instrument absent at H1 (§0 A4) — counted
    // skip, never silent.
    if !lp.profile.engagement_floors.is_empty() {
        census.add("floor-skipped-no-instrument", 1);
    }

    // H6 QPG-lite scheduling: OFF by default (blind arm — the seed sequence
    // is then exactly seed_base..seed_base+seed_count, byte-identical to the
    // H5 loop). When enabled, seeds that mint a NEW plan species earn
    // follow-on neighbor seeds; see runner/schedule.rs for the policy and the
    // FSE'21 blind-arm law (U suppressed on guided arms).
    let mut sched =
        crate::runner::schedule::SpeciesScheduler::new(sched_cfg.clone(), seed_base, seed_count);

    while let Some(seed) = sched.next_seed() {
        let (gen_plan, ctx, traces) = gen_plan_ctx_traced(seed, lp, &generator_version());
        kacc.add(&traces);
        let plan_text = gen_plan.render();
        let report = run_plan_ctx(&gen_plan, cfg, Some(&ctx))?;
        let species_before = species.distinct();
        for fp in &report.plan_fingerprints {
            species.add_sighting(fp);
        }
        species.explain_errors += report.explain_errors;
        for s in &report.explain_error_samples {
            if species.explain_error_samples.len() < 10 {
                species.explain_error_samples.push(s.clone());
            }
        }
        species.checkpoint();
        // Productive = this seed's statements minted at least one fingerprint
        // the campaign had never seen (novelty signal, keyed on the exact
        // canonical species string).
        sched.report(species.distinct() > species_before);
        // H7 dut-log scrape: attribute any fresh `panicked at` lines to this
        // seed (P1 — verdict-failing, see DutLogScraper).
        if let Some(s) = log_scraper.as_mut() {
            let hits = s.scan();
            if !hits.is_empty() {
                census.add(crate::vocab::PANIC_SIGNATURE, hits.len() as u64);
                for l in hits.iter().take(3) {
                    artifacts.record(
                        seed,
                        crate::vocab::PANIC_SIGNATURE,
                        "P1",
                        &format!("dut-log: {}", l.chars().take(200).collect::<String>()),
                        "dut-log-scrape",
                    );
                }
            }
        }
        let run = SeedRun { seed, report, plan_text };
        census.merge(&run.report.class_counts);
        for r in &run.report.records {
            artifacts.record(seed, &r.class, &r.sev, &r.detail, &r.stmt_head);
        }
        if let Some(failure) = &run.report.failure {
            // Bank: replay-N first (flake evidence), then shrink with
            // same-signature keep, then distill. The oracle context is the
            // seed's own (shrunk candidates keep property seqs, so instance
            // lookup by seq stays valid on subsets).
            let plan = Plan::parse(&run.plan_text).map_err(|e| format!("re-parse banked plan: {}", e))?;
            let (attempts, refails, _) = replay_n(&plan, cfg, replay_times, Some(&ctx))?;
            let mut rerun = |cand: &Plan| -> Option<Signature> {
                run_plan_ctx(cand, cfg, Some(&ctx)).ok().and_then(|r| r.failure.map(|f| f.signature))
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

    // Final scrape: a panic line whose write straddled the last per-seed
    // scan window is still counted (census-only; no seed to attribute).
    if let Some(s) = log_scraper.as_mut() {
        let hits = s.scan();
        if !hits.is_empty() {
            census.add(crate::vocab::PANIC_SIGNATURE, hits.len() as u64);
        }
    }

    // H5: evaluate k-path coverage + THE REACH GATE against the registry.
    // A production the profile marks reachable-by-default that never appeared
    // at k=1 mints `reach-gap` (P1) — campaign verdict RED, distinct from bug
    // findings. This is the check that would have caught H3's 0/9 pre-run.
    let prop_names: Vec<String> = crate::oracle::props::v1_set()
        .iter()
        .map(|id| id.as_str().to_string())
        .collect();
    let prop_refs: Vec<&str> = prop_names.iter().map(|s| s.as_str()).collect();
    let registry = crate::gen::prodreg::registry(&prop_refs);
    let gp = bridge::runner_profile_to_gen(&lp.profile);
    let kreport = crate::gen::prodreg::evaluate(&kacc, &registry, &gp);
    if !kreport.reach_gap.is_empty() {
        census.add(crate::metrics::REACH_GAP, kreport.reach_gap.len() as u64);
    }
    let metrics = crate::metrics::MetricsArtifact {
        kpath: &kreport,
        species: &species,
        explain_sample_every: cfg.explain_every,
        profile_name: &lp.profile.name,
        seed_base,
        seed_count,
        schedule: Some(&sched.stats),
    };
    metrics.write(out_dir)?;
    metrics.emit_stdout();

    artifacts.finish(
        &census,
        serde_json::json!({
            "profile": lp.profile.name,
            "profile_sha256": lp.sha256,
            "seed_base": seed_base,
            "seed_count": seed_count,
            "failures_banked": failures_banked,
            "generator": generator_version(),
            "metrics": {
                "k1": format!("{}/{}", kreport.k1.covered, kreport.k1.total),
                "k2": format!("{}/{}", kreport.k2.covered, kreport.k2.total),
                "k3": format!("{}/{}", kreport.k3.covered, kreport.k3.total),
                "reach_gap": kreport.reach_gap,
                "species_distinct": species.distinct(),
                "species_n": species.n,
                "good_turing_u": species.good_turing_u(),
            },
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
