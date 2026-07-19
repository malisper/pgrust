//! simbridge — SIM-HARNESS-CONVERGE increment 1: drive simharness-generated
//! v1 (single-session) plans INSIDE the deterministic simulator.
//!
//! ## Freshman's map of what happens per seed
//!
//! 1. The generator builds the plan for the workload seed exactly as a live
//!    campaign would (`gen_plan_ctx` — same bytes, same oracle context).
//! 2. The plan's statement stream is rendered to the sim corpus's script
//!    format (one statement per line) and the sim-built `postgres --sim-net`
//!    binary executes it as the P13 N-session corpus: the plan rides
//!    session 2 (a REGISTERED pmchild Backend over the sim_net transport
//!    under the seeded permit scheduler); session 3 is a fixed one-statement
//!    noise session, so the run is genuinely concurrent.
//! 3. The corpus mirrors the driver's symmetric error-recovery law
//!    (PGRUST_SIMNET_RECOVER=1: an errored statement is followed by an
//!    injected ROLLBACK — Dispatcher::dispatch / run_ctl_step, verbatim) and
//!    dumps three artifacts: the wire transcript (server bytes), the NETOP
//!    op log, and the SENT-LOG (what was actually sent, injections
//!    included).
//! 4. The harness re-walks the plan through the REAL `execute_plan` with a
//!    `ReplaySession` that serves each statement's outcome from the parsed
//!    transcript, verifying statement-for-statement alignment against the
//!    sent-log. The model oracle (ledger reconcile + slot checks) runs
//!    unchanged. The differential-vs-C oracle is N/A inside the sim (C
//!    cannot run under our scheduler/VFS) — model-oracle + property checks
//!    only, disclosed in the verdict header.
//!
//! ## The two lane assertions
//!
//! - DETERMINISM: same (workload seed, schedule seed) ⇒ byte-identical
//!   transcript + op log + sent-log + SCHEDOP stream, x3.
//! - SERIAL SEMANTICS: different schedule seeds with the same workload ⇒
//!   identical parsed outcome streams (the interleaving of an unrelated
//!   session must not change single-session answers). A divergence here is
//!   itself a bug find.
//!
//! ## The fault composition (sim-fault)
//!
//! Probe run (op census) → seed-drawn crash-cut op in the workload window →
//! writer run with the FaultDriver-spec delivery (PGRUST_SIM_FAULT_PLAN,
//! whole-node kill armed) → at-cut durable image packed to the host →
//! reboot run (single-session corpus over the pack; StartupXLOG crash
//! recovery is the PRODUCT's) → re-verify the ledger's crash-committed
//! table multisets through fresh SELECTs. The red arm (`--red`) weakens the
//! writer's durability (fsync=off — the faults battery's product-shaped red
//! arm) and MUST be caught by the same re-verify.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::bridge::{self, OracleCheckEval, OracleDiffClassifier};
use crate::oracle::check::StmtResult;
use crate::oracle::ledger::check_table_multiset;
use crate::runner::driver::{
    arm_sql, execute_plan, tx_sql, ExecOptions, ExecOutcome, RunReport, Session,
};
use crate::runner::planface::{ArmCtl, Plan, Step};
use crate::runner::profile::LoadedProfile;
use crate::runner::runloop::{class_is_p1, gen_plan_ctx, generator_version};

// ---------------------------------------------------------------------------
// Script synthesis
// ---------------------------------------------------------------------------

/// The per-seed session prologue (the live runner's `per_seed_reset` /
/// `default_reset`, verbatim). `SET statement_timeout` is deliberately NOT
/// carried into the sim (virtual-time semantics differ; wall timeouts are
/// the sim watchdog's job) — disclosed.
pub const RESET_STMTS: [&str; 3] = [
    "DROP SCHEMA IF EXISTS simharness CASCADE",
    "CREATE SCHEMA simharness",
    "SET search_path = simharness",
];

/// Replayed after `ARM reset-all` (RESET ALL nukes search_path — the live
/// runner's find-1 law, mirrored).
pub const POST_RESET_STMTS: [&str; 1] = ["SET search_path = simharness"];

/// Mirror of `NullBugShim::execute`'s rewrite (the TEETH instrument): the
/// planted wrong-DUT for oracle validation. Inside the sim the shim cannot
/// wrap a live session, so the rewrite is applied at script-synthesis time
/// (the sim executes the doctored SQL) and at replay-alignment time (the
/// driver's original statement maps onto the doctored sent-log entry).
pub fn null_bug_rewrite(sql: &str) -> String {
    let head = sql.trim_start();
    let is_select = head.get(..7).is_some_and(|h| h.eq_ignore_ascii_case("SELECT "));
    if is_select {
        if let Some(pos) = sql.find(" WHERE ") {
            let (pre, tail) = sql.split_at(pos);
            if tail.contains(" IS NULL") {
                return format!("{pre}{}", tail.replace(" IS NULL", " IS NULL AND false"));
            }
        }
    }
    sql.to_string()
}

/// Render the exact statement stream the live driver would issue for this
/// plan (minus the outcome-conditional recovery ROLLBACKs, which the sim
/// wire client injects itself under PGRUST_SIMNET_RECOVER=1).
///
/// Refusals (increment-1 scope, counted by the campaign, never silent):
/// - plans containing fault steps (client-side disconnect semantics have no
///   sim equivalent yet),
/// - v2 multi-session plans (increment 2).
pub fn synthesize_script(plan: &Plan, null_bug: bool) -> Result<Vec<String>, String> {
    let mut out: Vec<String> = RESET_STMTS.iter().map(|s| s.to_string()).collect();
    for step in &plan.steps {
        match step {
            Step::Ddl(sql) | Step::Dml(sql) | Step::Query(sql) => {
                let text =
                    if null_bug { null_bug_rewrite(&sql.text) } else { sql.text.clone() };
                out.push(text);
            }
            Step::Tx(t) => out.push(tx_sql(t)),
            Step::Arm(a) => {
                out.push(arm_sql(a));
                if matches!(a, ArmCtl::ResetAll) {
                    out.extend(POST_RESET_STMTS.iter().map(|s| s.to_string()));
                }
            }
            Step::BeginProperty { .. }
            | Step::EndProperty { .. }
            | Step::Assumption(_)
            | Step::Assertion(_) => {}
            Step::Fault(_) => return Err("bridge-refused-fault".into()),
            Step::Session(_) | Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_) => {
                return Err("bridge-refused-v2".into())
            }
        }
    }
    for s in &out {
        if s.contains('\n') || s.trim().is_empty() || s.trim_start().starts_with("--") {
            return Err("bridge-refused-unscriptable".into());
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Wire-transcript parsing (server->client bytes -> per-statement outcomes)
// ---------------------------------------------------------------------------

fn cstr(body: &[u8]) -> String {
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    String::from_utf8_lossy(&body[..end]).into_owned()
}

/// Mirror of the postgres crate's CommandComplete: the affected-row count is
/// the tag's last token when numeric, else 0; the driver stores
/// `n.to_string()` as the outcome tag.
fn tag_to_outcome_tag(tag: &str) -> String {
    tag.split_whitespace()
        .last()
        .and_then(|t| t.parse::<u64>().ok())
        .unwrap_or(0)
        .to_string()
}

/// Parsed transcript: one `ExecOutcome` per COMPLETED query cycle (cycle 0,
/// the startup exchange, is consumed and dropped), plus the trailing
/// un-Z-terminated error if the connection died mid-cycle (FATAL/kill).
pub struct ParsedTranscript {
    pub outcomes: Vec<ExecOutcome>,
    pub trailing_error: Option<ExecOutcome>,
}

pub fn parse_transcript(raw: &[u8]) -> Result<ParsedTranscript, String> {
    let mut outcomes = Vec::new();
    let mut cycle = 0usize; // completed Z frames seen
    let mut saw_rows = false;
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut tag: Option<String> = None;
    let mut err: Option<(String, String)> = None;
    let mut i = 0usize;
    while i + 5 <= raw.len() {
        let ty = raw[i];
        let len = i32::from_be_bytes(raw[i + 1..i + 5].try_into().unwrap()) as usize;
        if len < 4 || i + 1 + len > raw.len() {
            break; // truncated frame (cut mid-flush)
        }
        let body = &raw[i + 5..i + 1 + len];
        match ty {
            b'T' => saw_rows = true,
            b'D' => {
                if body.len() >= 2 {
                    let n = u16::from_be_bytes(body[..2].try_into().unwrap()) as usize;
                    let mut row = Vec::with_capacity(n);
                    let mut j = 2usize;
                    for _ in 0..n {
                        if j + 4 > body.len() {
                            return Err("DataRow truncated".into());
                        }
                        let l = i32::from_be_bytes(body[j..j + 4].try_into().unwrap());
                        j += 4;
                        if l < 0 {
                            row.push(None);
                        } else {
                            let l = l as usize;
                            if j + l > body.len() {
                                return Err("DataRow column truncated".into());
                            }
                            row.push(Some(
                                String::from_utf8_lossy(&body[j..j + l]).into_owned(),
                            ));
                            j += l;
                        }
                    }
                    rows.push(row);
                    saw_rows = true;
                }
            }
            b'C' => tag = Some(cstr(body)),
            b'E' => {
                let mut code = String::new();
                let mut msg = String::new();
                let mut j = 0usize;
                while j < body.len() && body[j] != 0 {
                    let f = body[j];
                    let s = cstr(&body[j + 1..]);
                    let adv = 1 + s.len() + 1;
                    match f {
                        b'C' => code = s,
                        b'M' => msg = s,
                        _ => {}
                    }
                    j += adv;
                }
                if err.is_none() {
                    err = Some((code, msg));
                }
            }
            b'Z' => {
                cycle += 1;
                if cycle > 1 {
                    // cycle 1..: one completed statement
                    outcomes.push(finish_cycle(
                        saw_rows,
                        std::mem::take(&mut rows),
                        tag.take(),
                        err.take(),
                    ));
                } else {
                    // startup cycle: an error here is a connect failure
                    if let Some((code, msg)) = err.take() {
                        return Err(format!("startup failed: {code} {msg}"));
                    }
                    rows.clear();
                    tag = None;
                }
                saw_rows = false;
            }
            _ => {} // R/S/K/N/A/I/... — session plumbing, notices
        }
        i += 1 + len;
    }
    let trailing_error =
        err.map(|(code, msg)| ExecOutcome::SqlError { sqlstate: code, message: msg });
    Ok(ParsedTranscript { outcomes, trailing_error })
}

fn finish_cycle(
    saw_rows: bool,
    rows: Vec<Vec<Option<String>>>,
    tag: Option<String>,
    err: Option<(String, String)>,
) -> ExecOutcome {
    if let Some((code, msg)) = err {
        return ExecOutcome::SqlError { sqlstate: code, message: msg };
    }
    if saw_rows {
        return ExecOutcome::Rows { rows };
    }
    ExecOutcome::Command { tag: tag.as_deref().map(tag_to_outcome_tag).unwrap_or_default() }
}

// ---------------------------------------------------------------------------
// The replay session (the bridge's Session impl)
// ---------------------------------------------------------------------------

/// Serves `execute()` from the recorded (sent-sql, outcome) stream, verifying
/// statement-for-statement alignment. Any mismatch is a DESYNC — loud,
/// counted, never silently realigned (increment-1 posture).
pub struct ReplaySession {
    entries: Vec<(String, ExecOutcome)>,
    cursor: usize,
    /// Applied to the driver's statement before comparing (the null-bug
    /// TEETH instrument doctors the script; alignment must see the same
    /// bytes the wire saw).
    rewrite: Option<fn(&str) -> String>,
    /// Fault-leg mode: an IO-dead / crash-class SQLSTATE (58*, XX*) marks
    /// the cut boundary — consumption stops there with a `client:
    /// simbridge-cut` fetch outcome instead of feeding post-kill refusals
    /// to the model.
    pub stop_at_io_error: bool,
    pub desync: Option<String>,
    pub cut_hit: bool,
}

impl ReplaySession {
    pub fn new(entries: Vec<(String, ExecOutcome)>) -> Self {
        ReplaySession {
            entries,
            cursor: 0,
            rewrite: None,
            stop_at_io_error: false,
            desync: None,
            cut_hit: false,
        }
    }

    pub fn with_rewrite(mut self, f: fn(&str) -> String) -> Self {
        self.rewrite = Some(f);
        self
    }

    pub fn consumed(&self) -> usize {
        self.cursor
    }
}

fn io_dead_class(sqlstate: &str) -> bool {
    sqlstate.starts_with("58") || sqlstate.starts_with("XX")
}

impl Session for ReplaySession {
    fn engine(&self) -> &str {
        "pgrust-sim"
    }

    fn execute(&mut self, sql: &str) -> ExecOutcome {
        if self.desync.is_some() || self.cut_hit {
            return ExecOutcome::ConnectionLost {
                message: "client: simbridge-halted".into(),
            };
        }
        let want = match self.rewrite {
            Some(f) => f(sql),
            None => sql.to_string(),
        };
        let Some((sent, outcome)) = self.entries.get(self.cursor) else {
            // Transcript exhausted: the cut boundary on the fault leg, a
            // desync anywhere else (the sim ran fewer statements than the
            // driver wants — impossible for a green single-session plan).
            if self.stop_at_io_error {
                self.cut_hit = true;
                return ExecOutcome::ConnectionLost {
                    message: "client: simbridge-cut (transcript exhausted)".into(),
                };
            }
            self.desync = Some(format!(
                "transcript exhausted at statement {} (driver wants: {})",
                self.cursor,
                &want[..want.len().min(80)]
            ));
            return ExecOutcome::ConnectionLost {
                message: "client: simbridge-desync (exhausted)".into(),
            };
        };
        if sent.trim() != want.trim() {
            self.desync = Some(format!(
                "statement {}: driver '{}' != sent '{}'",
                self.cursor,
                &want[..want.len().min(80)],
                &sent[..sent.len().min(80)]
            ));
            return ExecOutcome::ConnectionLost { message: "client: simbridge-desync".into() };
        }
        if self.stop_at_io_error {
            if let ExecOutcome::SqlError { sqlstate, .. } = outcome {
                if io_dead_class(sqlstate) {
                    self.cut_hit = true;
                    return ExecOutcome::ConnectionLost {
                        message: "client: simbridge-cut (io-dead statement)".into(),
                    };
                }
            }
        }
        let out = outcome.clone();
        self.cursor += 1;
        out
    }

    fn reconnect(&mut self) -> Result<(), String> {
        Err("simbridge: no client reconnect inside the sim world".into())
    }
}

// ---------------------------------------------------------------------------
// Corpus invocation
// ---------------------------------------------------------------------------

pub struct SimWorld {
    pub sim_bin: PathBuf,
    pub datadir: PathBuf,
    pub share_dir: PathBuf,
    pub timeout_s: u64,
}

pub struct CorpusRun {
    pub dir: PathBuf,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub stderr: String,
    pub transcript: Vec<u8>,
    pub sentlog: Vec<String>,
    pub oplog: Vec<u8>,
    pub schedlog: String,
}

pub struct CorpusSpec<'a> {
    /// Session-2 statements (the plan session) — or session-1 in
    /// single-session mode (the reboot leg).
    pub script: &'a [String],
    pub sched_seed: u64,
    /// N-session (P13 pattern) vs single-session (reboot leg).
    pub nsession: bool,
    /// FaultPlanSpec JSON for PGRUST_SIM_FAULT_PLAN (writer leg).
    pub fault_plan_json: Option<String>,
    /// PGRUST_SIMVFS_PACK target (writer leg).
    pub pack_dir: Option<PathBuf>,
    /// Weakened-durability red arm: adds -c fsync=off.
    pub fsync_off: bool,
    /// Emit SIMVFS-OPS lines (probe leg).
    pub ops_report: bool,
    /// Fsync the seeded image (fault legs: probe AND writer, so their op
    /// streams stay aligned for the cut-point rebasing).
    pub seed_durable: bool,
}

/// Recursive copy (skipping the boot-owned raw-plane lockfiles). The sim
/// mutates its HOST datadir through the raw plane (postmaster.pid,
/// pgrust_internal.init), so every corpus run gets a hermetic copy — two
/// concurrent runs on one datadir collide on the lockfile, and a shared
/// datadir drifts across runs (the first boot writes the relcache init
/// file the next seed then mirrors).
fn copy_tree(src: &Path, dst: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dst).map_err(|e| format!("mkdir {}: {e}", dst.display()))?;
    let mut entries: Vec<_> = std::fs::read_dir(src)
        .map_err(|e| format!("read_dir {}: {e}", src.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e.file_name();
        if name == "postmaster.pid" || name == "postmaster.opts" {
            continue;
        }
        let ft = e.file_type().map_err(|e| e.to_string())?;
        let to = dst.join(&name);
        if ft.is_dir() {
            copy_tree(&e.path(), &to)?;
        } else {
            std::fs::copy(e.path(), &to).map_err(|er| format!("copy {:?}: {er}", e.path()))?;
        }
    }
    Ok(())
}

pub fn run_corpus(world: &SimWorld, dir: &Path, spec: &CorpusSpec) -> Result<CorpusRun, String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
    // Hermetic per-run datadir (see copy_tree).
    let run_dd = dir.join("dd");
    copy_tree(&world.datadir, &run_dd)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&run_dd, std::fs::Permissions::from_mode(0o700));
    }
    let wf = |name: &str, content: &str| -> Result<PathBuf, String> {
        let p = dir.join(name);
        std::fs::write(&p, content).map_err(|e| format!("write {}: {e}", p.display()))?;
        Ok(p)
    };
    let mut script_text = spec.script.join("\n");
    script_text.push('\n');
    let tz = world.share_dir.join("timezone");
    let tzsets = world.share_dir.join("timezonesets");
    let mut cmd = std::process::Command::new(&world.sim_bin);
    cmd.arg("--sim-net");
    for guc in [
        "io_method=sync",
        "autovacuum=off",
        "wal_sync_method=fdatasync",
        "timezone=UTC",
        "log_timezone=UTC",
    ] {
        cmd.arg("-c").arg(guc);
    }
    if spec.fsync_off {
        cmd.arg("-c").arg("fsync=off");
    }
    cmd.arg("-D").arg(&run_dd);
    cmd.env("USER", "postgres")
        .env("PGRUST_RUNTIME", "0")
        .env("RUST_MIN_STACK", "67108864")
        .env("PGRUST_SIM_SCHED", "1")
        .env("PGRUST_SIM_SEED", spec.sched_seed.to_string())
        .env("PGRUST_SIM_SCHEDLOG", "stream")
        .env(
            "PGRUST_SIMNET_SEED_DIRS",
            format!("{}:{}", tz.display(), tzsets.display()),
        )
        .env("PGRUST_TZDIR", &tz)
        .env("PGRUST_PGSHAREDIR", &world.share_dir);
    let (transcript_p, sentlog_p, oplog_p);
    if spec.nsession {
        let s1 = wf("s1.sql", "SELECT 1\n")?;
        let s2 = wf("s2.sql", &script_text)?;
        let s3 = wf("s3.sql", "SELECT 'sim-noise'\n")?;
        transcript_p = dir.join("s2.transcript");
        sentlog_p = dir.join("s2.sentlog");
        oplog_p = dir.join("s2.oplog");
        cmd.env("PGRUST_SIMVFS_SHARED", "1")
            .env("PGRUST_SIMNET_NSESSION", "1")
            .env("PGRUST_NO_WORKER_POOL", "1")
            .env("PGRUST_SIMNET_RECOVER", "1")
            // No checkpointer runs in the corpus: sessions keep standalone-
            // topology local sync tables (the DROP-wedge fix; sim_net.rs).
            .env("PGRUST_SIMNET_LOCALSYNC", "1")
            .env("PGRUST_SIMNET_SQL", &s1)
            .env("PGRUST_SIMNET_SQL2", &s2)
            .env("PGRUST_SIMNET_SQL3", &s3)
            .env("PGRUST_SIMNET_TRANSCRIPT", dir.join("s1.transcript"))
            .env("PGRUST_SIMNET_TRANSCRIPT2", &transcript_p)
            .env("PGRUST_SIMNET_TRANSCRIPT3", dir.join("s3.transcript"))
            .env("PGRUST_SIMNET_OPLOG", dir.join("s1.oplog"))
            .env("PGRUST_SIMNET_OPLOG2", &oplog_p)
            .env("PGRUST_SIMNET_OPLOG3", dir.join("s3.oplog"))
            .env("PGRUST_SIMNET_SENTLOG2", &sentlog_p);
    } else {
        let s1 = wf("s1.sql", &script_text)?;
        transcript_p = dir.join("s1.transcript");
        sentlog_p = dir.join("s1.sentlog");
        oplog_p = dir.join("s1.oplog");
        cmd.env("PGRUST_SIMNET_SQL", &s1)
            .env("PGRUST_SIMNET_TRANSCRIPT", &transcript_p)
            .env("PGRUST_SIMNET_OPLOG", &oplog_p)
            .env("PGRUST_SIMNET_SENTLOG", &sentlog_p);
    }
    if let Some(json) = &spec.fault_plan_json {
        cmd.env("PGRUST_SIM_FAULT_PLAN", json);
    }
    if let Some(pack) = &spec.pack_dir {
        std::fs::create_dir_all(pack).map_err(|e| format!("mkdir pack: {e}"))?;
        cmd.env("PGRUST_SIMVFS_PACK", pack);
    }
    if spec.ops_report {
        cmd.env("PGRUST_SIMVFS_OPS_REPORT", "1");
    }
    if spec.seed_durable {
        // The fault-leg topology envelope (probe AND writer, identically —
        // op-stream alignment): durable seed + the DB_IN_PRODUCTION
        // re-flip after session 1's standalone-arm shutdown checkpoint
        // (without it the at-cut image reads as cleanly shut down and the
        // reboot skips crash recovery).
        cmd.env("PGRUST_SIMVFS_SEED_DURABLE", "1")
            .env("PGRUST_SIMNET_KEEP_INPRODUCTION", "1");
    }
    let out_f = std::fs::File::create(dir.join("stdout")).map_err(|e| e.to_string())?;
    let err_f = std::fs::File::create(dir.join("stderr")).map_err(|e| e.to_string())?;
    cmd.stdout(out_f).stderr(err_f).stdin(std::process::Stdio::null());
    let mut child = cmd.spawn().map_err(|e| format!("spawn sim: {e}"))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(world.timeout_s);
    let mut timed_out = false;
    let exit_code = loop {
        match child.try_wait().map_err(|e| e.to_string())? {
            Some(st) => break st.code(),
            None => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        }
    };
    // Reclaim the hermetic datadir copy (disk-reclaim law) — the run's
    // evidence lives in the artifact files, never in the copy.
    let _ = std::fs::remove_dir_all(&run_dd);
    let stderr = std::fs::read_to_string(dir.join("stderr")).unwrap_or_default();
    let schedlog: String = stderr
        .lines()
        .filter(|l| l.starts_with("SCHEDOP "))
        .map(|l| format!("{l}\n"))
        .collect();
    Ok(CorpusRun {
        dir: dir.to_path_buf(),
        exit_code,
        timed_out,
        stderr,
        transcript: std::fs::read(&transcript_p).unwrap_or_default(),
        sentlog: std::fs::read_to_string(&sentlog_p)
            .unwrap_or_default()
            .lines()
            .map(String::from)
            .collect(),
        oplog: std::fs::read(&oplog_p).unwrap_or_default(),
        schedlog,
    })
}

/// Zip the sent-log with the parsed transcript into the replay stream. The
/// trailing (un-acked) error frame, when present, becomes a final synthetic
/// entry — exactly what a live driver would have observed before the
/// connection died.
pub fn entries_from_run(run: &CorpusRun) -> Result<Vec<(String, ExecOutcome)>, String> {
    let parsed = parse_transcript(&run.transcript)?;
    let acked = parsed.outcomes.len();
    if acked > run.sentlog.len() {
        return Err(format!(
            "transcript has {acked} completed cycles but only {} statements were sent",
            run.sentlog.len()
        ));
    }
    let mut entries: Vec<(String, ExecOutcome)> = run
        .sentlog
        .iter()
        .take(acked)
        .cloned()
        .zip(parsed.outcomes)
        .map(|(s, o)| (s, o))
        .collect();
    if let Some(err) = parsed.trailing_error {
        if let Some(sql) = run.sentlog.get(acked) {
            entries.push((sql.clone(), err));
        }
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// The model-oracle replay (the live run_plan_ctx, sim-shaped)
// ---------------------------------------------------------------------------

pub struct CheckedRun {
    pub report: RunReport,
    pub desync: Option<String>,
    pub cut_hit: bool,
    pub consumed: usize,
    /// The model's crash-committed tables at the stopping point (fault leg).
    pub committed: Vec<(String, Vec<String>, Vec<crate::oracle::check::Row>)>,
    /// Ledger tx open at the stopping point (indeterminacy input).
    pub model_in_tx: bool,
}

pub fn check_entries(
    plan: &Plan,
    ctx: &bridge::OracleCtx,
    entries: Vec<(String, ExecOutcome)>,
    null_bug: bool,
    stop_at_io_error: bool,
) -> Result<CheckedRun, String> {
    let mut replay = ReplaySession::new(entries);
    if null_bug {
        replay = replay.with_rewrite(null_bug_rewrite);
    }
    replay.stop_at_io_error = stop_at_io_error;
    // The reset prologue (reset_leg's shape): consumed OUTSIDE the plan walk.
    for s in RESET_STMTS {
        match replay.execute(s) {
            ExecOutcome::SqlError { sqlstate, message } => {
                return Err(format!("reset '{s}' failed: {sqlstate} {message}"))
            }
            ExecOutcome::ConnectionLost { message } => {
                if replay.cut_hit {
                    // Fault leg: the cut landed before the prologue even
                    // acked — a legitimate (empty-model) cut point.
                    return Ok(CheckedRun {
                        report: RunReport::default(),
                        desync: None,
                        cut_hit: true,
                        consumed: replay.consumed(),
                        committed: Vec::new(),
                        model_in_tx: false,
                    });
                }
                return Err(format!("reset '{s}': {message}"))
            }
            _ => {}
        }
    }
    let checks = OracleCheckEval::new(ctx);
    let classifier = OracleDiffClassifier::new(bridge::load_warts());
    let opts = ExecOptions {
        stop_on_failure: true,
        post_reset_sql: POST_RESET_STMTS.iter().map(|s| s.to_string()).collect(),
        explain_every: 0,
        session_pool: None,
        ..Default::default()
    };
    let report = execute_plan(plan, &mut replay, None, &checks, &classifier, &opts);
    let committed = checks.crash_committed_tables();
    let model_in_tx = checks.model_in_open_tx();
    Ok(CheckedRun {
        report,
        desync: replay.desync.clone(),
        cut_hit: replay.cut_hit,
        consumed: replay.consumed(),
        committed,
        model_in_tx,
    })
}

// ---------------------------------------------------------------------------
// Campaign: the bridge (green + determinism x3 + serial semantics)
// ---------------------------------------------------------------------------

pub struct BridgeArgs {
    pub lp: LoadedProfile,
    pub seed_base: u64,
    pub seeds: u64,
    pub sched_seed: u64,
    pub world: SimWorld,
    pub out: PathBuf,
    pub x3: u64,
    pub serialsem: u64,
    pub test_null_bug: bool,
}

fn is_expected_fetch(report: &RunReport) -> bool {
    report
        .failure
        .as_ref()
        .is_some_and(|f| f.class == "harness-fetch" && f.detail.contains("simbridge"))
}

fn scrape_panics(stderr: &str) -> u64 {
    stderr.lines().filter(|l| l.contains("panicked at")).count() as u64
}

fn bump(census: &mut BTreeMap<String, u64>, k: &str, n: u64) {
    *census.entry(k.to_string()).or_insert(0) += n;
}

pub fn run_bridge_campaign(a: &BridgeArgs) -> i32 {
    let gv = generator_version();
    let mut census: BTreeMap<String, u64> = BTreeMap::new();
    println!(
        "SIMBRIDGE|mode|in-sim model-oracle + property checks (diff-c N/A inside the sim)"
    );
    let mut hard_fail = false;
    for i in 0..a.seeds {
        let wseed = a.seed_base + i;
        let (plan, ctx) = gen_plan_ctx(wseed, &a.lp, &gv);
        let script = match synthesize_script(&plan, a.test_null_bug) {
            Ok(s) => s,
            Err(class) => {
                bump(&mut census, &class, 1);
                continue;
            }
        };
        let seed_dir = a.out.join(format!("w{wseed}-s{}", a.sched_seed));
        let spec = CorpusSpec {
            script: &script,
            sched_seed: a.sched_seed,
            nsession: true,
            fault_plan_json: None,
            pack_dir: None,
            fsync_off: false,
            ops_report: false,
            seed_durable: false,
        };
        let run = match run_corpus(&a.world, &seed_dir.join("r1"), &spec) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("simbridge: seed {wseed}: {e}");
                bump(&mut census, "sim-run-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        if run.timed_out || run.exit_code != Some(0) {
            eprintln!(
                "simbridge: seed {wseed}: corpus exit={:?} timed_out={} (see {})",
                run.exit_code,
                run.timed_out,
                run.dir.display()
            );
            bump(&mut census, "sim-run-failed", 1);
            hard_fail = true;
            continue;
        }
        let panics = scrape_panics(&run.stderr);
        if panics > 0 {
            bump(&mut census, "panic-signature", panics);
            hard_fail = true;
            eprintln!(
                "simbridge: seed {wseed}: {panics} panic line(s) in sim stderr ({})",
                run.dir.display()
            );
        }
        // Determinism x3 (the first `x3` seeds): byte-identical artifacts.
        if i < a.x3 {
            let mut identical = true;
            for rep in ["r2", "r3"] {
                match run_corpus(&a.world, &seed_dir.join(rep), &spec) {
                    Ok(r) => {
                        if r.transcript != run.transcript
                            || r.oplog != run.oplog
                            || r.sentlog != run.sentlog
                            || r.schedlog != run.schedlog
                        {
                            identical = false;
                        }
                    }
                    Err(_) => identical = false,
                }
            }
            if identical {
                bump(&mut census, "x3-identical", 1);
            } else {
                bump(&mut census, "x3-DIVERGED", 1);
                hard_fail = true;
                eprintln!("simbridge: seed {wseed}: x3 artifacts DIVERGED ({})", seed_dir.display());
            }
        }
        // Serial semantics (the first `serialsem` seeds): a different
        // schedule seed must produce the identical parsed outcome stream.
        if i < a.serialsem {
            let alt = CorpusSpec { sched_seed: a.sched_seed + 1, ..spec_clone(&spec, &script) };
            match run_corpus(&a.world, &seed_dir.join("alt-sched"), &alt) {
                Ok(r2) if !r2.timed_out && r2.exit_code == Some(0) => {
                    let e1 = entries_from_run(&run);
                    let e2 = entries_from_run(&r2);
                    match (e1, e2) {
                        (Ok(x), Ok(y)) if x == y => bump(&mut census, "serialsem-identical", 1),
                        (Ok(x), Ok(y)) => {
                            bump(&mut census, "serialsem-DIVERGED", 1);
                            hard_fail = true;
                            let first = x
                                .iter()
                                .zip(y.iter())
                                .position(|(p, q)| p != q)
                                .map(|p| p.to_string())
                                .unwrap_or_else(|| format!("len {} vs {}", x.len(), y.len()));
                            eprintln!(
                                "simbridge: seed {wseed}: SERIAL-SEMANTICS DIVERGENCE at entry {first} ({})",
                                seed_dir.display()
                            );
                        }
                        _ => {
                            bump(&mut census, "serialsem-unparseable", 1);
                            hard_fail = true;
                        }
                    }
                }
                _ => {
                    bump(&mut census, "serialsem-run-failed", 1);
                    hard_fail = true;
                }
            }
        }
        // The model-oracle replay.
        let entries = match entries_from_run(&run) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("simbridge: seed {wseed}: transcript parse: {e}");
                bump(&mut census, "bridge-parse-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        match check_entries(&plan, &ctx, entries, a.test_null_bug, false) {
            Ok(checked) => {
                if let Some(d) = &checked.desync {
                    bump(&mut census, "bridge-desync", 1);
                    eprintln!("simbridge: seed {wseed}: DESYNC {d}");
                    // The desync-induced fetch record is bridge mechanics,
                    // not a DUT finding — replace the class.
                    let mut counts = checked.report.class_counts.clone();
                    if is_expected_fetch(&checked.report) {
                        if let Some(n) = counts.get_mut("harness-fetch") {
                            *n = n.saturating_sub(1);
                        }
                    }
                    for (k, v) in counts {
                        bump(&mut census, &k, v);
                    }
                    continue;
                }
                for (k, v) in &checked.report.class_counts {
                    bump(&mut census, k, *v);
                }
                if let Some(f) = &checked.report.failure {
                    let _ = std::fs::create_dir_all(&seed_dir);
                    let _ = std::fs::write(
                        seed_dir.join("failure.txt"),
                        format!(
                            "seed {wseed} class {} sev {} step {} site {}\n{}\n",
                            f.class, f.sev, f.step_idx, f.signature.site, f.detail
                        ),
                    );
                    let _ = std::fs::write(seed_dir.join("plan.plan"), plan_text(&plan));
                }
            }
            Err(e) => {
                eprintln!("simbridge: seed {wseed}: check failed: {e}");
                bump(&mut census, "bridge-check-failed", 1);
                hard_fail = true;
            }
        }
    }
    for (k, v) in &census {
        println!("SIMHARNESS|{k}|{v}");
    }
    let p1: Vec<String> = census
        .iter()
        .filter(|(k, v)| **v > 0 && (class_is_p1(k) || k.as_str() == "panic-signature"))
        .map(|(k, _)| k.clone())
        .collect();
    let fail = hard_fail || !p1.is_empty();
    if fail {
        let mut why = p1;
        for k in ["sim-run-failed", "x3-DIVERGED", "serialsem-DIVERGED", "bridge-parse-failed", "bridge-check-failed", "serialsem-run-failed", "serialsem-unparseable"] {
            if census.get(k).copied().unwrap_or(0) > 0 && !why.iter().any(|w| w == k) {
                why.push(k.to_string());
            }
        }
        println!("SIMBRIDGE-VERDICT|FAIL:{}", why.join(","));
        1
    } else {
        println!("SIMBRIDGE-VERDICT|PASS");
        0
    }
}

fn spec_clone<'a>(spec: &CorpusSpec<'a>, script: &'a [String]) -> CorpusSpec<'a> {
    CorpusSpec {
        script,
        sched_seed: spec.sched_seed,
        nsession: spec.nsession,
        fault_plan_json: spec.fault_plan_json.clone(),
        pack_dir: spec.pack_dir.clone(),
        fsync_off: spec.fsync_off,
        ops_report: spec.ops_report,
        seed_durable: spec.seed_durable,
    }
}

fn plan_text(plan: &Plan) -> String {
    plan.render()
}

// ---------------------------------------------------------------------------
// Campaign: the fault composition (crash-cut + recovery + re-verify)
// ---------------------------------------------------------------------------

pub struct FaultArgs {
    pub lp: LoadedProfile,
    pub seed_base: u64,
    pub seeds: u64,
    pub sched_seed: u64,
    pub world: SimWorld,
    pub out: PathBuf,
    /// Weakened-durability red arm (fsync=off writer) — the composition's
    /// planted red; the re-verify MUST catch it on some seeds.
    pub red: bool,
}

fn splitmix64(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn ops_from_stderr(stderr: &str, tag: &str) -> Option<u64> {
    let needle = format!("SIMVFS-OPS {tag}=");
    stderr
        .lines()
        .rev()
        .find_map(|l| l.strip_prefix(&needle).and_then(|v| v.parse().ok()))
}

/// In-flight indeterminacy: the statement at the cut boundary whose effects
/// recovery may legitimately keep or lose. Autocommit mutations and COMMIT
/// are indeterminate; anything inside a still-open tx (rolled back by
/// recovery either way) and any read is determinate.
fn step_kind_name(s: &Step) -> &'static str {
    match s {
        Step::BeginProperty { .. } => "BeginProperty",
        Step::EndProperty { .. } => "EndProperty",
        Step::Ddl(_) => "Ddl",
        Step::Dml(_) => "Dml",
        Step::Query(_) => "Query",
        Step::Tx(_) => "Tx",
        Step::Arm(_) => "Arm",
        Step::Assumption(_) => "Assumption",
        Step::Assertion(_) => "Assertion",
        Step::Fault(_) => "Fault",
        Step::Session(_) => "Session",
        Step::AsyncDml(_) => "AsyncDml",
        Step::Join(_) => "Join",
        Step::WaitUntil(_) => "WaitUntil",
    }
}

fn cut_indeterminate(plan: &Plan, checked: &CheckedRun) -> bool {
    let Some(f) = &checked.report.failure else { return false };
    let Some(step) = plan.steps.get(f.step_idx) else { return false };
    match step {
        Step::Tx(crate::runner::planface::TxCtl::Commit) => true,
        Step::Ddl(_) | Step::Dml(_) => !checked.model_in_tx,
        _ => false,
    }
}

pub fn run_fault_campaign(a: &FaultArgs) -> i32 {
    let gv = generator_version();
    let mut census: BTreeMap<String, u64> = BTreeMap::new();
    let arm = if a.red { "RED(fsync=off writer)" } else { "green" };
    println!("SIMBRIDGE|fault-arm|{arm}");
    let mut caught = 0u64;
    let mut hard_fail = false;
    for i in 0..a.seeds {
        let wseed = a.seed_base + i;
        let (plan, ctx) = gen_plan_ctx(wseed, &a.lp, &gv);
        let script = match synthesize_script(&plan, false) {
            Ok(s) => s,
            Err(class) => {
                bump(&mut census, &class, 1);
                continue;
            }
        };
        let seed_dir = a.out.join(format!("f{wseed}-s{}", a.sched_seed));
        // 1. probe: op census (also proves the workload is green pre-fault).
        let probe_spec = CorpusSpec {
            script: &script,
            sched_seed: a.sched_seed,
            nsession: true,
            fault_plan_json: None,
            pack_dir: None,
            fsync_off: false,
            ops_report: true,
            seed_durable: true,
        };
        let probe = match run_corpus(&a.world, &seed_dir.join("probe"), &probe_spec) {
            Ok(r) if !r.timed_out && r.exit_code == Some(0) => r,
            _ => {
                bump(&mut census, "fault-probe-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        let (Some(armed), Some(promote), Some(final_ops)) = (
            ops_from_stderr(&probe.stderr, "armed"),
            ops_from_stderr(&probe.stderr, "promote"),
            ops_from_stderr(&probe.stderr, "final"),
        ) else {
            bump(&mut census, "fault-probe-no-ops", 1);
            hard_fail = true;
            continue;
        };
        if final_ops <= promote + 4 || promote < armed {
            bump(&mut census, "fault-window-too-small", 1);
            continue;
        }
        // Green pre-check: a workload that fails its own model without any
        // fault belongs to the main campaign, not the fault attribution.
        match entries_from_run(&probe).and_then(|e| check_entries(&plan, &ctx, e, false, false)) {
            Ok(c) if c.desync.is_none() && c.report.failure.is_none() => {}
            _ => {
                bump(&mut census, "fault-skip-nongreen-workload", 1);
                continue;
            }
        }
        // 2. the seed-drawn cut op, mid-workload — rebased onto the rule
        // counter's frame (the plan counts matches from the arming point,
        // after the universe-seeding writes op_seq also counted).
        let mut s = wseed ^ (a.sched_seed.rotate_left(17)) ^ 0x51_C0_4E_5A_11;
        let cut_abs = promote + 2 + splitmix64(&mut s) % (final_ops - promote - 2);
        let cut = cut_abs - armed;
        if cut < 1 {
            bump(&mut census, "fault-window-too-small", 1);
            continue;
        }
        let fault_json = format!(
            "{{\"seed\":{},\"rules\":[{{\"matcher\":{{\"kinds\":null,\"class\":null,\"path_contains\":null}},\"nth\":{cut},\"action\":\"Crash\",\"sticky\":false}}]}}",
            splitmix64(&mut s)
        );
        let pack = seed_dir.join("pack");
        let writer_spec = CorpusSpec {
            script: &script,
            sched_seed: a.sched_seed,
            nsession: true,
            fault_plan_json: Some(fault_json),
            pack_dir: Some(pack.clone()),
            fsync_off: a.red,
            ops_report: false,
            seed_durable: true,
        };
        let writer = match run_corpus(&a.world, &seed_dir.join("writer"), &writer_spec) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("simbridge-fault: seed {wseed}: writer: {e}");
                bump(&mut census, "fault-writer-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        if !writer.stderr.contains("SIMCUT ") {
            bump(&mut census, "fault-no-cut", 1);
            eprintln!(
                "simbridge-fault: seed {wseed}: no SIMCUT (cut op {cut}, exit {:?}) — see {}",
                writer.exit_code,
                writer.dir.display()
            );
            hard_fail = true;
            continue;
        }
        // 3. the model state at the cut (acked prefix through the oracle).
        let checked = match entries_from_run(&writer)
            .and_then(|e| check_entries(&plan, &ctx, e, false, true))
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("simbridge-fault: seed {wseed}: cut-replay: {e}");
                bump(&mut census, "fault-cut-replay-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        if let Some(d) = &checked.desync {
            eprintln!("simbridge-fault: seed {wseed}: DESYNC before cut: {d}");
            bump(&mut census, "bridge-desync", 1);
            continue;
        }
        // A model-oracle P1 BEFORE the cut boundary is a real finding, not
        // fault noise (the pre-check was green; only the cut prefix differs
        // — same schedule, same statements — so this should be impossible).
        if let Some(f) = &checked.report.failure {
            if !is_expected_fetch(&checked.report) {
                eprintln!(
                    "simbridge-fault: seed {wseed}: pre-cut failure {}: {}",
                    f.class, f.detail
                );
                bump(&mut census, &format!("fault-precut-{}", f.class), 1);
                hard_fail = true;
                continue;
            }
        }
        // 4. reboot over the pack: product crash recovery + fresh SELECTs.
        let mut verify: Vec<String> = Vec::new();
        for (t, cols, _) in &checked.committed {
            verify.push(format!("SELECT {} FROM simharness.{t}", cols.join(", ")));
        }
        if verify.is_empty() {
            verify.push("SELECT 1".to_string());
        }
        // The boot ladder enforces datadir permissions (u=rwx).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&pack, std::fs::Permissions::from_mode(0o700));
        }
        let reboot_world = SimWorld {
            sim_bin: a.world.sim_bin.clone(),
            datadir: pack.clone(),
            share_dir: a.world.share_dir.clone(),
            timeout_s: a.world.timeout_s,
        };
        let reboot_spec = CorpusSpec {
            script: &verify,
            sched_seed: a.sched_seed,
            nsession: false,
            fault_plan_json: None,
            pack_dir: None,
            fsync_off: false,
            ops_report: false,
            seed_durable: false,
        };
        let reboot = match run_corpus(&reboot_world, &seed_dir.join("reboot"), &reboot_spec) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("simbridge-fault: seed {wseed}: reboot spawn: {e}");
                bump(&mut census, "fault-reboot-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        if reboot.timed_out || reboot.exit_code != Some(0) {
            // Crash recovery over a legal at-cut image failed — ALWAYS a
            // finding (green arm) / the red being caught (red arm).
            if a.red {
                caught += 1;
                bump(&mut census, "red-caught-reboot-crash", 1);
            } else {
                eprintln!(
                    "simbridge-fault: seed {wseed}: REBOOT FAILED exit={:?} timed_out={} ({})",
                    reboot.exit_code,
                    reboot.timed_out,
                    reboot.dir.display()
                );
                bump(&mut census, "fault-reboot-crash", 1);
                hard_fail = true;
            }
            continue;
        }
        let outcomes = match parse_transcript(&reboot.transcript) {
            Ok(p) => p.outcomes,
            Err(e) => {
                eprintln!("simbridge-fault: seed {wseed}: reboot transcript: {e}");
                bump(&mut census, "fault-reboot-parse-failed", 1);
                hard_fail = true;
                continue;
            }
        };
        let mut mismatch: Option<String> = None;
        for (idx, (t, _cols, expected)) in checked.committed.iter().enumerate() {
            match outcomes.get(idx) {
                Some(ExecOutcome::Rows { rows }) => {
                    let got = match bridge::to_stmt_result(&ExecOutcome::Rows {
                        rows: rows.clone(),
                    }) {
                        StmtResult::Rows { rows } => rows,
                        _ => Vec::new(),
                    };
                    if let Err(why) = check_table_multiset(expected, &got) {
                        mismatch = Some(format!("table {t}: {why}"));
                        break;
                    }
                }
                Some(ExecOutcome::SqlError { sqlstate, message }) => {
                    mismatch = Some(format!("table {t}: {sqlstate} {message}"));
                    break;
                }
                other => {
                    mismatch = Some(format!("table {t}: unexpected outcome {other:?}"));
                    break;
                }
            }
        }
        match mismatch {
            None => bump(&mut census, "fault-verified", 1),
            Some(why) => {
                // Adjudication evidence: what was in flight at the cut.
                if let Some(f) = &checked.report.failure {
                    eprintln!(
                        "simbridge-fault: seed {wseed}: cut boundary at plan step {} kind {:?} class {} detail '{}' model_in_tx={} consumed={}",
                        f.step_idx,
                        plan.steps.get(f.step_idx).map(step_kind_name),
                        f.class,
                        f.detail,
                        checked.model_in_tx,
                        checked.consumed,
                    );
                } else {
                    eprintln!(
                        "simbridge-fault: seed {wseed}: verify mismatch with NO failure record (walk completed?) consumed={}",
                        checked.consumed
                    );
                }
                if cut_indeterminate(&plan, &checked) {
                    // The in-flight statement's effects are legitimately
                    // either-way; increment-1 verifies the determinate cuts
                    // only (disclosed).
                    bump(&mut census, "fault-indeterminate", 1);
                } else if a.red {
                    caught += 1;
                    bump(&mut census, "red-caught-verify", 1);
                    println!("SIMBRIDGE|red-catch|seed {wseed}: {why}");
                } else {
                    eprintln!(
                        "simbridge-fault: seed {wseed}: VERIFY FAILED: {why} ({})",
                        seed_dir.display()
                    );
                    bump(&mut census, "fault-verify-FAILED", 1);
                    let _ = std::fs::write(seed_dir.join("verify-failure.txt"), why);
                    hard_fail = true;
                }
            }
        }
        // Reclaim the pack (disk-reclaim law): verified packs are bulky.
        if census.get("fault-verify-FAILED").copied().unwrap_or(0) == 0 {
            let _ = std::fs::remove_dir_all(&pack);
        }
    }
    for (k, v) in &census {
        println!("SIMHARNESS|{k}|{v}");
    }
    if a.red {
        let ok = caught > 0;
        println!(
            "SIMBRIDGE-FAULT-VERDICT|{}",
            if ok { format!("RED-CAUGHT|{caught}") } else { "RED-MISSED".to_string() }
        );
        if ok {
            0
        } else {
            1
        }
    } else {
        let fail = hard_fail;
        println!(
            "SIMBRIDGE-FAULT-VERDICT|{}",
            if fail { "FAIL" } else { "PASS" }
        );
        if fail {
            1
        } else {
            0
        }
    }
}
