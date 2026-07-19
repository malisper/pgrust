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
// SIM-CONVERGE inc-2: two-session plan split (the cross-session interleaving)
// ---------------------------------------------------------------------------

/// The per-session scripts + the global turn order a v2 TWO-session plan maps
/// onto the P13 N-session corpus (boot + two registered backends). The v2
/// format's serialized interleaving is a cross-session statement-ORDER
/// contract; the sim corpus drives each session's script independently, so the
/// order is carried out-of-band as [`turns`] (a session turn-id per global
/// statement) and enforced by the corpus's turn gate (PGRUST_SIMNET_TURNS,
/// sim_net.rs) — a client sends its next statement only on its turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwoSessionScripts {
    /// s1 (boot, turn-free): the shared setup — DROP/CREATE SCHEMA + all table
    /// DDL — run to completion before the worker sessions spawn, so both
    /// workers see the schema through the shared universe.
    pub setup: Vec<String>,
    /// s2 (turn-id 2): the plan's session 0 (primary) statement stream, with a
    /// per-connection `SET search_path` prologue.
    pub session_a: Vec<String>,
    /// s3 (turn-id 3): the plan's session 1 (worker) statement stream, with a
    /// per-connection `SET search_path` prologue.
    pub session_b: Vec<String>,
    /// The global turn order: the session turn-id (2 or 3) that owns each
    /// global statement, in plan order — including the two `SET search_path`
    /// prologue statements as the first two turns. Its length equals
    /// `(session_a.len() + session_b.len())`.
    pub turns: Vec<u32>,
}

/// Turn-id for the plan's primary session (0) and its single worker (1).
const TURN_A: u32 = 2; // sim s2
const TURN_B: u32 = 3; // sim s3

/// Split a v2 TWO-session plan into per-session scripts + the global turn
/// order (see [`TwoSessionScripts`]). This is the inc-2 milestone shape:
/// SESSION switches + synchronous statements over exactly TWO sessions (0 and
/// 1). Counted refusals (never silent — the campaign census carries them),
/// scoped honestly to inc-3:
/// - `bridge-refused-v2-async`   AsyncDml/Join/WaitUntil (the blocking-worker
///   choreography — its wire mapping + the model-oracle re-walk over a
///   ReplaySession pool is inc-3);
/// - `bridge-refused-v2-fanout`  a session id > 1 (the S1 specconflict shape
///   needs 4 sessions — beyond P13's two registered backends; inc-3);
/// - `bridge-refused-v2-lateddl` a DDL after the interleaving has begun (the
///   setup-reorder would change semantics; the milestone shape keeps all DDL
///   in the s1 setup prefix);
/// - `bridge-refused-v2-tx`      Tx steps (BEGIN/COMMIT/ROLLBACK). The model
///   walk replays the MERGED stream through the single-session driver walk;
///   a tx open on one connection is not a tx on the other, so session-scoped
///   tx modeling needs the session-aware replay pool — inc-3. Autocommit
///   statements are order-equivalent under the completion-ordered turn gate
///   (statement k fully completes before k+1 is sent), so the merged walk is
///   exact for the tx-free shape;
/// - `bridge-refused-fault` / `bridge-refused-unscriptable` as for v1.
pub fn synthesize_two_session(plan: &Plan, null_bug: bool) -> Result<TwoSessionScripts, String> {
    let mut setup: Vec<String> = RESET_STMTS.iter().map(|s| s.to_string()).collect();
    // Each worker is its OWN connection: it must set its own search_path (the
    // schema persists in the shared universe, but search_path is per-session).
    // These prologues are the first two global turns.
    let mut session_a: Vec<String> = vec![POST_RESET_STMTS[0].to_string()];
    let mut session_b: Vec<String> = vec![POST_RESET_STMTS[0].to_string()];
    let mut turns: Vec<u32> = vec![TURN_A, TURN_B];

    let mut active: u32 = 0;
    let mut interleaving_began = false;
    for step in &plan.steps {
        // Route a synchronous statement to the active session's stream + a turn.
        let emit = |active: u32, sql: String, turns: &mut Vec<u32>,
                    a: &mut Vec<String>, b: &mut Vec<String>| {
            match active {
                0 => {
                    a.push(sql);
                    turns.push(TURN_A);
                }
                _ => {
                    b.push(sql);
                    turns.push(TURN_B);
                }
            }
        };
        match step {
            Step::Session(id) => {
                if *id > 1 {
                    return Err("bridge-refused-v2-fanout".into());
                }
                active = *id;
            }
            Step::Ddl(sql) => {
                if interleaving_began {
                    return Err("bridge-refused-v2-lateddl".into());
                }
                // Shared object: run on the boot session so both workers see
                // it through the shared universe.
                setup.push(sql.text.clone());
            }
            Step::Dml(sql) | Step::Query(sql) => {
                interleaving_began = true;
                let text =
                    if null_bug { null_bug_rewrite(&sql.text) } else { sql.text.clone() };
                emit(active, text, &mut turns, &mut session_a, &mut session_b);
            }
            Step::Tx(_) => return Err("bridge-refused-v2-tx".into()),
            Step::Arm(a) => {
                interleaving_began = true;
                emit(active, arm_sql(a), &mut turns, &mut session_a, &mut session_b);
                if matches!(a, ArmCtl::ResetAll) {
                    // RESET ALL nukes search_path on the active connection.
                    emit(
                        active,
                        POST_RESET_STMTS[0].to_string(),
                        &mut turns,
                        &mut session_a,
                        &mut session_b,
                    );
                }
            }
            Step::BeginProperty { .. }
            | Step::EndProperty { .. }
            | Step::Assumption(_)
            | Step::Assertion(_) => {}
            Step::Fault(_) => return Err("bridge-refused-fault".into()),
            Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_) => {
                return Err("bridge-refused-v2-async".into())
            }
        }
    }
    for s in setup.iter().chain(&session_a).chain(&session_b) {
        if s.contains('\n') || s.trim().is_empty() || s.trim_start().starts_with("--") {
            return Err("bridge-refused-unscriptable".into());
        }
    }
    debug_assert_eq!(turns.len(), session_a.len() + session_b.len());
    Ok(TwoSessionScripts { setup, session_a, session_b, turns })
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
    /// SIM-CONVERGE inc-2 (two-session mode only; empty otherwise): session
    /// B's artifacts (sim s3) and the boot session's (s1 — the setup script's
    /// acks, which the merged model walk consumes first).
    pub transcript_b: Vec<u8>,
    pub sentlog_b: Vec<String>,
    pub oplog_b: Vec<u8>,
    pub transcript_s1: Vec<u8>,
    pub sentlog_s1: Vec<String>,
}

/// SIM-CONVERGE inc-2: the two-session corpus shape — s1 runs `setup` to
/// completion (boot session), then s2 (`spec.script` = session A) and s3
/// (`script_b` = session B) interleave under the cross-session turn gate
/// (`turns`, completion-ordered; see sim_net.rs). `gate=false` resurrects the
/// pre-lane RACE (no PGRUST_SIMNET_TURNS) — the order-red arm.
#[derive(Clone, Copy)]
pub struct TwoSessionEnv<'a> {
    pub setup: &'a [String],
    pub script_b: &'a [String],
    pub turns: &'a [u32],
    pub gate: bool,
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
    /// SIM-CONVERGE inc-2: two-session mode (requires `nsession`). None =
    /// every existing leg, byte-identical.
    pub two_session: Option<TwoSessionEnv<'a>>,
    /// Virtual-time ceiling (PGRUST_SIM_VCEIL_S) — the wedge red's named-
    /// verdict bound (SCHEDCEILING instead of a wall-clock kill). None =
    /// existing legs unchanged.
    pub vceil_s: Option<u64>,
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
        // SIM-CONVERGE inc-2 (two-session mode): s1 = the setup script, s3 =
        // session B's script, and the serialized interleaving rides the turn
        // gate (PGRUST_SIMNET_TURNS; gate=false = the order-red race arm).
        // Single-plan mode keeps the historical noise-s3 shape byte-for-byte.
        let (s1_text, s3_text) = match &spec.two_session {
            Some(ts) => {
                let mut a = ts.setup.join("\n");
                a.push('\n');
                let mut b = ts.script_b.join("\n");
                b.push('\n');
                (a, b)
            }
            None => ("SELECT 1\n".to_string(), "SELECT 'sim-noise'\n".to_string()),
        };
        let s1 = wf("s1.sql", &s1_text)?;
        let s2 = wf("s2.sql", &script_text)?;
        let s3 = wf("s3.sql", &s3_text)?;
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
        if let Some(ts) = &spec.two_session {
            cmd.env("PGRUST_SIMNET_SENTLOG", dir.join("s1.sentlog"))
                .env("PGRUST_SIMNET_SENTLOG3", dir.join("s3.sentlog"));
            if ts.gate {
                let turns: Vec<String> = ts.turns.iter().map(|t| t.to_string()).collect();
                cmd.env("PGRUST_SIMNET_TURNS", turns.join(" "));
            }
        }
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
    if let Some(vceil) = spec.vceil_s {
        cmd.env("PGRUST_SIM_VCEIL_S", vceil.to_string());
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
    let read_lines = |p: &Path| -> Vec<String> {
        std::fs::read_to_string(p).unwrap_or_default().lines().map(String::from).collect()
    };
    let two = spec.two_session.is_some();
    Ok(CorpusRun {
        dir: dir.to_path_buf(),
        exit_code,
        timed_out,
        stderr,
        transcript: std::fs::read(&transcript_p).unwrap_or_default(),
        sentlog: read_lines(&sentlog_p),
        oplog: std::fs::read(&oplog_p).unwrap_or_default(),
        schedlog,
        transcript_b: if two {
            std::fs::read(dir.join("s3.transcript")).unwrap_or_default()
        } else {
            Vec::new()
        },
        sentlog_b: if two { read_lines(&dir.join("s3.sentlog")) } else { Vec::new() },
        oplog_b: if two {
            std::fs::read(dir.join("s3.oplog")).unwrap_or_default()
        } else {
            Vec::new()
        },
        transcript_s1: if two {
            std::fs::read(dir.join("s1.transcript")).unwrap_or_default()
        } else {
            Vec::new()
        },
        sentlog_s1: if two { read_lines(&dir.join("s1.sentlog")) } else { Vec::new() },
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
            two_session: None,
            vceil_s: None,
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
        two_session: spec.two_session,
        vceil_s: spec.vceil_s,
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
            two_session: None,
            vceil_s: None,
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
            two_session: None,
            vceil_s: None,
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
            two_session: None,
            vceil_s: None,
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

// ---------------------------------------------------------------------------
// SIM-CONVERGE inc-2: the two-session campaign (one H8 v2 plan under sim)
// ---------------------------------------------------------------------------

/// Zip one session's sent-log with its parsed transcript (the
/// [`entries_from_run`] shape over explicit parts — the two-session merge
/// needs it per session).
pub fn entries_from_parts(
    sentlog: &[String],
    transcript: &[u8],
) -> Result<Vec<(String, ExecOutcome)>, String> {
    let parsed = parse_transcript(transcript)?;
    let acked = parsed.outcomes.len();
    if acked > sentlog.len() {
        return Err(format!(
            "transcript has {acked} completed cycles but only {} statements were sent",
            sentlog.len()
        ));
    }
    let mut entries: Vec<(String, ExecOutcome)> =
        sentlog.iter().take(acked).cloned().zip(parsed.outcomes).collect();
    if let Some(err) = parsed.trailing_error {
        if let Some(sql) = sentlog.get(acked) {
            entries.push((sql.clone(), err));
        }
    }
    Ok(entries)
}

/// Merge a two-session corpus run back into ONE globally-ordered replay
/// stream: s1's entries first (the reset prologue + all setup DDL — the
/// stream [`check_entries`] starts by consuming RESET_STMTS from), then the
/// A/B statement entries interleaved by the turn order. The two worker
/// `SET search_path` prologues (turns 0 and 1) are verified (right text,
/// no error) and DROPPED — they are per-connection plumbing, not plan steps.
pub fn merge_two_session_entries(
    ts: &TwoSessionScripts,
    run: &CorpusRun,
) -> Result<Vec<(String, ExecOutcome)>, String> {
    let s1 = entries_from_parts(&run.sentlog_s1, &run.transcript_s1)?;
    let a = entries_from_parts(&run.sentlog, &run.transcript)?;
    let b = entries_from_parts(&run.sentlog_b, &run.transcript_b)?;
    // Strict alignment: each session must have sent EXACTLY its script (an
    // injected recovery ROLLBACK or a shortfall is a divergence — the
    // milestone scope is error-free plans; error-carrying two-session plans
    // ride inc-3's session-aware replay).
    let texts = |v: &[(String, ExecOutcome)]| -> Vec<String> {
        v.iter().map(|(s, _)| s.clone()).collect::<Vec<_>>()
    };
    if texts(&s1) != ts.setup {
        return Err(format!(
            "s1 sent-stream != setup script ({} vs {} entries)",
            s1.len(),
            ts.setup.len()
        ));
    }
    if texts(&a) != ts.session_a {
        return Err(format!(
            "session A sent-stream != script ({} vs {} entries)",
            a.len(),
            ts.session_a.len()
        ));
    }
    if texts(&b) != ts.session_b {
        return Err(format!(
            "session B sent-stream != script ({} vs {} entries)",
            b.len(),
            ts.session_b.len()
        ));
    }
    let mut ai = a.into_iter();
    let mut bi = b.into_iter();
    let mut merged = s1;
    for (pos, turn) in ts.turns.iter().enumerate() {
        let entry = match *turn {
            2 => ai.next(),
            3 => bi.next(),
            other => return Err(format!("turn {pos}: unknown turn-id {other}")),
        }
        .ok_or_else(|| format!("turn {pos}: session {turn} stream exhausted"))?;
        if pos < 2 {
            // The worker SET prologues: verify, then drop from the stream.
            if entry.0 != POST_RESET_STMTS[0] {
                return Err(format!("turn {pos}: expected SET prologue, got '{}'", entry.0));
            }
            if entry.1.is_error() {
                return Err(format!("turn {pos}: SET prologue errored"));
            }
            continue;
        }
        merged.push(entry);
    }
    Ok(merged)
}

/// The v2 plan with `Session` switches stripped — the SESSION-BLIND step walk
/// whose statement order IS the serialized interleaving. Valid for the inc-2
/// milestone shape only (synchronous, tx-free — synthesize_two_session's
/// refusals guarantee it), where the merged single-stream walk is exact.
pub fn strip_session_steps(plan: &Plan) -> Plan {
    Plan {
        header: plan.header.clone(),
        steps: plan
            .steps
            .iter()
            .filter(|s| !matches!(s, Step::Session(_)))
            .cloned()
            .collect(),
    }
}

/// The built-in inc-2 milestone plan + its oracle context: a v2 TWO-session
/// cross-session choreography whose assertions are ORDER-SENSITIVE — session
/// B reads what session A wrote at exact points of the serialized
/// interleaving, and each read's value is pinned by a slot-addressed
/// `scalar-eq` assertion inside an `M2-CrossSession` property block (the H8
/// posture: multi-session properties assert through SLOTS, never the ledger
/// — pstep.rs's oracle-alignment law). Any cross-session order violation
/// (the order-red arm races the clients) or a doctored read (the NullBug
/// arm) breaks an assertion: property-violation, P1.
///
/// The instance is hand-built (autocommit-only — the generated M2 arms use
/// worker-session transactions, which the merged single-stream walk refuses
/// until inc-3's session-aware replay pool), but it runs through the REAL
/// property machinery: OracleCheckEval alignment, silent-step skipping,
/// SlotStack puts, eval_check.
pub fn fixture_two_session_plan() -> (Plan, bridge::OracleCtx) {
    use crate::oracle::pstep::{
        Mark as PMark, PStep, PropertyInstance, SqlMeta as PSqlMeta, SqlStep,
    };
    use crate::oracle::props::PropertyId;
    use crate::runner::planface::{Mark, PlanHeader, Sql, SqlMeta};
    let sql = |text: &str, mark: Mark| Sql {
        text: text.to_string(),
        mark,
        meta: SqlMeta::default(),
    };
    let assert_json = |slot: u32, value: i64| {
        format!("{{\"kind\":\"scalar-eq\",\"slot\":{slot},\"value\":{value}}}")
    };
    let pstmt = |text: &str, mark: PMark, stackref: Option<u32>| {
        PStep::Sql(SqlStep {
            sql: text.to_string(),
            mark,
            meta: PSqlMeta::default(),
            ledger_op: None,
            probe: None,
            stackref,
        })
    };
    let passert = |slot: u32, value: i64| {
        PStep::Assert(crate::oracle::check::Check::ScalarEq {
            slot,
            value: crate::oracle::check::Value::Int(value),
        })
    };
    // The interleaved region: (plan step, instance step), kept 1:1 so the
    // oracle's alignment cursor walks in lockstep.
    let plan_steps = vec![
        Step::Ddl(sql("CREATE TABLE t (k int, v int)", Mark::Mutation)),
        Step::BeginProperty {
            name: "M2-CrossSession".into(),
            seq: 0,
            tables: vec!["t".into()],
        },
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (1, 10)", Mark::Mutation)),
        Step::Session(1),
        Step::Query(sql("SELECT count(*) FROM t", Mark::Read)), // slot 0
        Step::Assertion(assert_json(0, 1)),
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (2, 20)", Mark::Mutation)),
        Step::Session(1),
        Step::Query(sql("SELECT sum(v) FROM t", Mark::Read)), // slot 1
        Step::Assertion(assert_json(1, 30)),
        Step::Session(0),
        Step::Dml(sql("INSERT INTO t VALUES (3, NULL)", Mark::Mutation)),
        Step::Session(1),
        Step::Query(sql("SELECT count(*) FROM t WHERE v IS NULL", Mark::Read)), // slot 2
        Step::Assertion(assert_json(2, 1)),
        Step::Session(0),
        Step::Query(sql("SELECT count(*) FROM t", Mark::Read)), // slot 3
        Step::Assertion(assert_json(3, 3)),
        Step::EndProperty { seq: 0 },
    ];
    let inst = PropertyInstance {
        property: PropertyId::M2CrossSession,
        steps: vec![
            PStep::Session(0),
            pstmt("INSERT INTO t VALUES (1, 10)", PMark::Mutation, None),
            PStep::Session(1),
            pstmt("SELECT count(*) FROM t", PMark::Read, Some(0)),
            passert(0, 1),
            PStep::Session(0),
            pstmt("INSERT INTO t VALUES (2, 20)", PMark::Mutation, None),
            PStep::Session(1),
            pstmt("SELECT sum(v) FROM t", PMark::Read, Some(1)),
            passert(1, 30),
            PStep::Session(0),
            pstmt("INSERT INTO t VALUES (3, NULL)", PMark::Mutation, None),
            PStep::Session(1),
            pstmt("SELECT count(*) FROM t WHERE v IS NULL", PMark::Read, Some(2)),
            passert(2, 1),
            PStep::Session(0),
            pstmt("SELECT count(*) FROM t", PMark::Read, Some(3)),
            passert(3, 3),
        ],
        tables: ["t".to_string()].into_iter().collect(),
    };
    let plan = Plan {
        header: PlanHeader {
            seed: 0,
            profile: "sim-two-fixture".into(),
            profile_sha256: "0".repeat(64),
            generator: "sim-converge-inc2".into(),
        },
        steps: plan_steps,
    };
    let mut by_seq = std::collections::BTreeMap::new();
    by_seq.insert(0, inst);
    (plan, bridge::OracleCtx { by_seq })
}

pub struct TwoArgs {
    /// v2 plan file to drive; None = the built-in fixture (rendered to
    /// `<out>/plan.plan` either way — the artifact IS plan-format v2 bytes
    /// through the real render/parse round trip).
    pub plan_path: Option<PathBuf>,
    pub sched_seed: u64,
    pub world: SimWorld,
    pub out: PathBuf,
    /// Extra identical repetitions (2 = the x3 law).
    pub x3: u64,
    /// Additional schedule seeds to run (cross-seed observation legs; the
    /// chartered assertion stays "same (plan, sched seed) => identical
    /// bytes" ONLY — see the campaign's disclosure line).
    pub alt_scheds: u64,
    /// Planted red: run WITHOUT the turn gate (the pre-lane race) — the
    /// serialized-order model walk must catch a violation on >=1 of the
    /// probed schedule seeds (probabilistic by nature, like the fsync red).
    pub red_order: bool,
    /// Planted red: a WEDGED turn schedule (a turn owned by a session with
    /// no statements left) — must produce the named SCHEDCEILING verdict,
    /// never a panic.
    pub red_wedge: bool,
    /// Planted red: the NullBug TEETH instrument on the cross-session read.
    pub test_null_bug: bool,
}

fn two_session_spec<'a>(
    script_a: &'a [String],
    ts_env: TwoSessionEnv<'a>,
    sched_seed: u64,
    vceil_s: Option<u64>,
) -> CorpusSpec<'a> {
    CorpusSpec {
        script: script_a,
        sched_seed,
        nsession: true,
        fault_plan_json: None,
        pack_dir: None,
        fsync_off: false,
        ops_report: false,
        seed_durable: false,
        two_session: Some(ts_env),
        vceil_s,
    }
}

/// Load (or build) the plan, run it as the two-session corpus, model-check
/// the merged stream, prove x3 byte-identity, and drive the planted reds.
/// Verdict line: `SIMBRIDGE-TWO-VERDICT|PASS` / `|FAIL:<why>` /
/// `|RED-CAUGHT|<n>` (red arms).
pub fn run_two_session_campaign(a: &TwoArgs) -> i32 {
    let mut census: BTreeMap<String, u64> = BTreeMap::new();
    println!(
        "SIMBRIDGE-TWO|mode|one H8 v2 two-session plan under sim (model-oracle only; \
         determinism law: same (plan, sched seed) => identical bytes; different sched \
         seeds may legally change interleaving-visible results — the turn gate pins \
         STATEMENT order, in-statement scheduling still varies)"
    );
    let _ = std::fs::create_dir_all(&a.out);
    // 1. The plan: file or fixture, always round-tripped through the real
    //    v2 render/parse (the artifact is plan-format bytes, never IR).
    // --plan mode carries no oracle context (a plan file alone has no
    // property instances): alignment + determinism only, checks skip
    // counted. The fixture carries its hand-built M2-CrossSession instance
    // — the teeth path.
    let (plan, ctx) = match &a.plan_path {
        Some(p) => match std::fs::read_to_string(p)
            .map_err(|e| e.to_string())
            .and_then(|t| Plan::parse(&t))
        {
            Ok(pl) => (pl, bridge::OracleCtx::default()),
            Err(e) => {
                println!("SIMBRIDGE-TWO-VERDICT|FAIL:plan-load ({e})");
                return 1;
            }
        },
        None => fixture_two_session_plan(),
    };
    let rendered = plan.render();
    if !rendered.starts_with("-- simharness plan v2 (multi-session)") {
        println!("SIMBRIDGE-TWO-VERDICT|FAIL:plan-not-v2");
        return 1;
    }
    match Plan::parse(&rendered) {
        Ok(back) if back == plan => {}
        Ok(_) => {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:plan-roundtrip-drift");
            return 1;
        }
        Err(e) => {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:plan-roundtrip ({e})");
            return 1;
        }
    }
    let _ = std::fs::write(a.out.join("plan.plan"), &rendered);
    let null_bug = a.test_null_bug;
    let ts = match synthesize_two_session(&plan, null_bug) {
        Ok(t) => t,
        Err(class) => {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:{class}");
            return 1;
        }
    };
    let stripped = strip_session_steps(&plan);

    // --- The wedge red: a turn schedule with one extra B-turn planted where
    //     A's first turn was — B exhausts its script, the cursor parks on a
    //     turn nobody owns, A yields forever, and the scheduler's virtual
    //     ceiling names it: SCHEDCEILING, never a panic.
    if a.red_wedge {
        let mut wedged = ts.turns.clone();
        if let Some(first_a) = wedged.iter().position(|t| *t == 2) {
            wedged[first_a] = 3;
        }
        let env = TwoSessionEnv {
            setup: &ts.setup,
            script_b: &ts.session_b,
            turns: &wedged,
            gate: true,
        };
        let spec = two_session_spec(&ts.session_a, env, a.sched_seed, Some(10));
        return match run_corpus(&a.world, &a.out.join("wedge"), &spec) {
            Ok(run) => {
                let named = run.stderr.contains("SCHEDCEILING");
                let panics = scrape_panics(&run.stderr);
                let died = run.exit_code != Some(0);
                println!("SIMBRIDGE-TWO|wedge|exit={:?} named={named} panics={panics}", run.exit_code);
                if named && died && panics == 0 {
                    println!("SIMBRIDGE-TWO-VERDICT|RED-CAUGHT|SCHEDCEILING");
                    0
                } else {
                    println!("SIMBRIDGE-TWO-VERDICT|FAIL:wedge-not-named (named={named} died={died} panics={panics})");
                    1
                }
            }
            Err(e) => {
                println!("SIMBRIDGE-TWO-VERDICT|FAIL:wedge-run ({e})");
                1
            }
        };
    }

    let env = TwoSessionEnv {
        setup: &ts.setup,
        script_b: &ts.session_b,
        turns: &ts.turns,
        gate: !a.red_order,
    };

    // --- The order red: NO gate — the two clients race. Probe several
    //     schedule seeds; the serialized-order model walk must catch >=1
    //     violation (order divergence is seed-dependent, disclosed).
    if a.red_order {
        let probes = 8u64;
        let mut caught = 0u64;
        for k in 0..probes {
            let seed = a.sched_seed + k;
            let dir = a.out.join(format!("order-red-s{seed}"));
            let spec = two_session_spec(&ts.session_a, env, seed, None);
            let Ok(run) = run_corpus(&a.world, &dir, &spec) else {
                bump(&mut census, "two-run-failed", 1);
                continue;
            };
            if run.timed_out || run.exit_code != Some(0) {
                bump(&mut census, "two-run-failed", 1);
                continue;
            }
            match merge_two_session_entries(&ts, &run) {
                Ok(entries) => match check_entries(&stripped, &ctx, entries, null_bug, false) {
                    Ok(checked) => {
                        let violated = checked
                            .report
                            .failure
                            .as_ref()
                            .is_some_and(|f| f.class == "property-violation")
                            || checked.desync.is_some();
                        if violated {
                            caught += 1;
                            bump(&mut census, "order-red-caught", 1);
                        } else {
                            bump(&mut census, "order-red-serialized-ok", 1);
                        }
                    }
                    Err(_) => bump(&mut census, "two-check-failed", 1),
                },
                // A racing run can complete with a sent-stream that no longer
                // matches the per-session scripts only via recovery injection;
                // treat merge failure as a caught divergence too (loud).
                Err(_) => {
                    caught += 1;
                    bump(&mut census, "order-red-caught-merge", 1);
                }
            }
        }
        for (k, v) in &census {
            println!("SIMHARNESS|{k}|{v}");
        }
        return if caught >= 1 {
            println!("SIMBRIDGE-TWO-VERDICT|RED-CAUGHT|{caught}");
            0
        } else {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:order-red-not-caught (0/{probes})");
            1
        };
    }

    // --- Green (or NullBug) leg: run, merge, model-check.
    let dir = a.out.join(format!("s{}", a.sched_seed));
    let spec = two_session_spec(&ts.session_a, env, a.sched_seed, None);
    let run = match run_corpus(&a.world, &dir.join("r1"), &spec) {
        Ok(r) => r,
        Err(e) => {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:sim-run ({e})");
            return 1;
        }
    };
    if run.timed_out || run.exit_code != Some(0) {
        println!(
            "SIMBRIDGE-TWO-VERDICT|FAIL:sim-exit (exit={:?} timed_out={})",
            run.exit_code, run.timed_out
        );
        return 1;
    }
    let panics = scrape_panics(&run.stderr);
    if panics > 0 {
        println!("SIMBRIDGE-TWO-VERDICT|FAIL:panic-signature ({panics})");
        return 1;
    }
    // x3 determinism: EVERY artifact byte-identical (both sessions + s1 +
    // the SCHEDOP stream).
    let mut identical = 0u64;
    for rep in 0..a.x3 {
        match run_corpus(&a.world, &dir.join(format!("r{}", rep + 2)), &spec) {
            Ok(r) => {
                let same = r.transcript == run.transcript
                    && r.transcript_b == run.transcript_b
                    && r.transcript_s1 == run.transcript_s1
                    && r.oplog == run.oplog
                    && r.oplog_b == run.oplog_b
                    && r.sentlog == run.sentlog
                    && r.sentlog_b == run.sentlog_b
                    && r.schedlog == run.schedlog;
                if same {
                    identical += 1;
                } else {
                    println!("SIMBRIDGE-TWO-VERDICT|FAIL:x3-DIVERGED (rep {})", rep + 2);
                    return 1;
                }
            }
            Err(e) => {
                println!("SIMBRIDGE-TWO-VERDICT|FAIL:x3-run ({e})");
                return 1;
            }
        }
    }
    bump(&mut census, "x3-identical", identical);
    // Cross-seed observation legs (NOT an assertion — disclosed above): run
    // alternate schedule seeds and REPORT whether the parsed outcome streams
    // matched (under the completion-ordered gate they are expected to, but
    // the chartered determinism law does not require it).
    for k in 0..a.alt_scheds {
        let seed = a.sched_seed + 1 + k;
        let alt = two_session_spec(&ts.session_a, env, seed, None);
        match run_corpus(&a.world, &a.out.join(format!("s{seed}")), &alt) {
            Ok(r) if !r.timed_out && r.exit_code == Some(0) => {
                let same = match (merge_two_session_entries(&ts, &run), merge_two_session_entries(&ts, &r)) {
                    (Ok(x), Ok(y)) => x == y,
                    _ => false,
                };
                bump(
                    &mut census,
                    if same { "altsched-outcomes-identical" } else { "altsched-outcomes-differ" },
                    1,
                );
            }
            _ => bump(&mut census, "altsched-run-failed", 1),
        }
    }
    // The model-oracle walk over the merged stream.
    let entries = match merge_two_session_entries(&ts, &run) {
        Ok(e) => e,
        Err(e) => {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:merge ({e})");
            return 1;
        }
    };
    let checked = match check_entries(&stripped, &ctx, entries, null_bug, false) {
        Ok(c) => c,
        Err(e) => {
            println!("SIMBRIDGE-TWO-VERDICT|FAIL:check ({e})");
            return 1;
        }
    };
    if let Some(d) = &checked.desync {
        println!("SIMBRIDGE-TWO-VERDICT|FAIL:desync ({d})");
        return 1;
    }
    for (k, v) in &checked.report.class_counts {
        bump(&mut census, k, *v);
    }
    for (k, v) in &census {
        println!("SIMHARNESS|{k}|{v}");
    }
    if let Some(f) = &checked.report.failure {
        // Under --test-null-bug a property-violation IS the expected catch.
        if null_bug && f.class == "property-violation" {
            println!("SIMBRIDGE-TWO-VERDICT|RED-CAUGHT|property-violation");
            return 0;
        }
        println!("SIMBRIDGE-TWO-VERDICT|FAIL:{} (step {}: {})", f.class, f.step_idx, f.detail);
        return 1;
    }
    if null_bug {
        println!("SIMBRIDGE-TWO-VERDICT|FAIL:null-bug-not-caught");
        return 1;
    }
    println!("SIMBRIDGE-TWO-VERDICT|PASS");
    0
}
