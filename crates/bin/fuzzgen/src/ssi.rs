//! Q4 multi-session concurrency differential (serializable-ssi chunk):
//! an isolationtester-style engine that drives N concurrent sessions per
//! engine through a deterministic, replayable schedule and compares the
//! full event sequence A vs B.
//!
//! Model (charter: docs/design/coverage-differential-fuzzer.md; Q4 lane):
//! a scenario is an ordered list of steps (session-id, SQL). Steps are
//! issued strictly in deck order. A step that BLOCKS (heavyweight lock
//! wait, safe-snapshot wait) is parked and the schedule advances to the
//! next step; parked steps are reaped — in session order — immediately
//! after any later step completes and may have released the lock. The
//! resulting event sequence (issued / completed / blocked / unblocked-
//! completed-after) is therefore a deterministic function of WHICH steps
//! blocked. That function must be identical on both engines: a schedule
//! divergence (step blocks on one engine only) is itself a finding
//! (SCHEDULE class) and comparison for the scenario stops there.
//!
//! Blocked detection: after issuing a step the engine polls for
//! completion; concurrently a monitor connection asks pg_stat_activity
//! whether the session's backend is in a heavyweight-lock wait
//! (wait_event_type = 'Lock') or an SSI safe-snapshot wait (wait_event =
//! 'SafeSnapshot'). If the monitor cannot answer (view/column missing —
//! an inventory gap, reported once, not a finding here) the engine falls
//! back to a fixed wall-clock window: not complete after block_window_ms
//! = blocked. The DETECTION METHOD is recorded per event but is not part
//! of the compare surface; only the blocked/completed classification is.
//!
//! Scheduler-nondeterminism policy (ruled, see the lane charter): where
//! an outcome is legitimately scheduler-dependent — canonically WHICH of
//! two symmetric transactions a deadlock detector victimizes — the deck
//! marks the involved steps with a symmetric `group`. Grouped steps
//! compare as an INVARIANT: the multiset of per-step outcome classes
//! within the group must match (so "exactly one 40P01, the other
//! commits" holds on both engines) but the assignment of outcomes to
//! sessions may differ. Everything ungrouped compares strictly: rows
//! (ordered), command tags, error SQLSTATE (messages recorded, never
//! compared — they carry pids/xids).
//!
//! Determinism aids baked into the decks: every wait-producing step is
//! ordered so the waiter is unambiguous; deadlock scenarios stagger the
//! two lock waits so the earlier waiter's deadlock_timeout fires first;
//! statement_timeout bounds every wait so a rig bug fails loud (57014 =
//! rig error, never a finding).

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::client::{Client, ConnLost};
use crate::diff::StmtOutcome;
use crate::runner::fold_results;

// ------------------------------------------------------------------ deck --

/// One step of a scenario: a statement issued on one session's connection.
#[derive(Clone, Debug)]
pub struct Step {
    /// Step label (unique within the scenario; the findings key).
    pub name: &'static str,
    /// Session index (0-based).
    pub session: usize,
    pub sql: String,
    /// Symmetric group id: steps sharing a group compare as an invariant
    /// (outcome-class multiset) instead of per-step — the scheduler-
    /// nondeterminism escape hatch (deadlock victim identity et al).
    pub group: Option<&'static str>,
}

/// A hand-authored multi-session scenario.
#[derive(Clone, Debug)]
pub struct Scenario {
    pub name: &'static str,
    pub description: &'static str,
    /// Applied on the monitor connection before the sessions connect.
    pub setup: Vec<String>,
    pub sessions: usize,
    /// Per-session SQL applied right after connect (GUCs; identical A/B).
    pub session_setup: Vec<String>,
    pub steps: Vec<Step>,
    /// Final-state probes run on the monitor after all sessions finished:
    /// (label, sql). Deterministic ORDER BY required. Compared strictly.
    pub probes: Vec<(&'static str, String)>,
}

fn step(name: &'static str, session: usize, sql: &str) -> Step {
    Step { name, session, sql: sql.to_string(), group: None }
}

fn gstep(name: &'static str, session: usize, sql: &str, group: &'static str) -> Step {
    Step { name, session, sql: sql.to_string(), group: Some(group) }
}

// ---------------------------------------------------------------- events --

/// How a step's blocked classification was made (recorded, not compared).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockDetect {
    /// pg_stat_activity said Lock / SafeSnapshot.
    Monitor,
    /// Monitor unavailable or silent; block_window elapsed.
    Window,
}

/// Outcome classes for the compare surface. Rows/tags normalized here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutcomeKey {
    /// SELECT rows: column oids + cells.
    Rows(Vec<u32>, Vec<Vec<Option<String>>>),
    /// Command tag ("UPDATE 1", "COMMIT", ...).
    Command(String),
    /// Error SQLSTATE (message intentionally excluded).
    Error(String),
    ConnLost,
}

pub fn outcome_key(o: &StmtOutcome) -> OutcomeKey {
    match o {
        StmtOutcome::Rows { col_oids, rows } => OutcomeKey::Rows(col_oids.clone(), rows.clone()),
        StmtOutcome::Command { tag, .. } => OutcomeKey::Command(tag.clone()),
        StmtOutcome::CopyOut { tag, .. } => OutcomeKey::Command(tag.clone()),
        StmtOutcome::Error { sqlstate, .. } => OutcomeKey::Error(sqlstate.clone()),
        StmtOutcome::ConnLost { .. } => OutcomeKey::ConnLost,
    }
}

fn outcome_msg(o: &StmtOutcome) -> String {
    match o {
        StmtOutcome::Error { sqlstate, message } => format!("{sqlstate}: {message}"),
        StmtOutcome::ConnLost { detail } => format!("connlost: {detail}"),
        _ => String::new(),
    }
}

/// One entry of an engine's event log — the compare surface.
#[derive(Clone, Debug)]
pub enum Event {
    /// Step ran to completion while it was the schedule head.
    Completed { step: usize, key: OutcomeKey, msg: String },
    /// Step did not complete; parked. `detect` recorded only.
    Blocked { step: usize, detect: BlockDetect },
    /// A parked step completed after `after` finished (usize::MAX = the
    /// end-of-scenario drain).
    Unblocked { step: usize, after: usize, key: OutcomeKey, msg: String },
    /// Final-state probe result.
    Probe { label: &'static str, key: OutcomeKey },
}

impl Event {
    /// Schedule shape only (what must match for the schedules to be the
    /// same deterministic function on both engines).
    fn shape(&self) -> String {
        match self {
            Event::Completed { step, .. } => format!("completed[{step}]"),
            Event::Blocked { step, .. } => format!("blocked[{step}]"),
            Event::Unblocked { step, after, .. } => format!("unblocked[{step} after {after}]"),
            Event::Probe { label, .. } => format!("probe[{label}]"),
        }
    }
}

// ---------------------------------------------------------------- engine --

/// Wire endpoint of one engine.
#[derive(Clone)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
}

enum Job {
    Exec(String),
    Quit,
}

struct SessionHandle {
    tx: mpsc::Sender<Job>,
    rx: mpsc::Receiver<StmtOutcome>,
    pid: Option<i64>,
    /// Index of the parked (issued, not yet completed) step, if any.
    parked: Option<usize>,
    joiner: Option<thread::JoinHandle<()>>,
}

fn spawn_session(ep: &Endpoint, setup: &[String]) -> Result<SessionHandle, String> {
    let mut client = Client::connect(&ep.host, ep.port, &ep.db, &ep.user)
        .map_err(|ConnLost(e)| format!("session connect: {e}"))?;
    let mut pid = None;
    match client.simple_query("SELECT pg_backend_pid()") {
        Ok(rs) => {
            if let StmtOutcome::Rows { rows, .. } = fold_results(&rs) {
                pid = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|c| c.as_ref())
                    .and_then(|c| c.parse::<i64>().ok());
            }
        }
        Err(ConnLost(e)) => return Err(format!("pg_backend_pid: {e}")),
    }
    for sql in setup {
        match client.simple_query(sql) {
            Ok(rs) => {
                if let StmtOutcome::Error { sqlstate, message } = fold_results(&rs) {
                    return Err(format!("session setup {sql:?} failed: {sqlstate} {message}"));
                }
            }
            Err(ConnLost(e)) => return Err(format!("session setup: {e}")),
        }
    }
    let (jtx, jrx) = mpsc::channel::<Job>();
    let (otx, orx) = mpsc::channel::<StmtOutcome>();
    let joiner = thread::spawn(move || {
        while let Ok(Job::Exec(sql)) = jrx.recv() {
            let out = match client.simple_query(&sql) {
                Ok(rs) => fold_results(&rs),
                Err(ConnLost(e)) => StmtOutcome::ConnLost { detail: e },
            };
            if otx.send(out).is_err() {
                break;
            }
        }
    });
    Ok(SessionHandle { tx: jtx, rx: orx, pid, parked: None, joiner: Some(joiner) })
}

/// One engine's scenario execution state.
pub struct EngineRun {
    ep: Endpoint,
    monitor: Client,
    /// Monitor wait-state introspection available? Probed once per run.
    monitor_ok: bool,
    sessions: Vec<SessionHandle>,
    pub events: Vec<Event>,
    pub label: &'static str,
}

/// Engine-level tunables (identical A/B; not part of the compare surface).
#[derive(Clone, Copy)]
pub struct Tunables {
    /// Completion poll tick.
    pub tick_ms: u64,
    /// Wall-clock fallback window after which a non-complete, non-Lock-
    /// waiting step is classified blocked anyway.
    pub block_window_ms: u64,
    /// Hard per-step deadline (rig failure past this; statement_timeout
    /// on the sessions is set below it so the server breaks waits first).
    pub step_deadline_ms: u64,
}

impl Default for Tunables {
    fn default() -> Tunables {
        Tunables { tick_ms: 20, block_window_ms: 2_500, step_deadline_ms: 25_000 }
    }
}

impl EngineRun {
    pub fn new(label: &'static str, ep: Endpoint) -> Result<EngineRun, String> {
        let monitor = Client::connect(&ep.host, ep.port, &ep.db, &ep.user)
            .map_err(|ConnLost(e)| format!("{label} monitor connect: {e}"))?;
        Ok(EngineRun { ep, monitor, monitor_ok: true, sessions: Vec::new(), events: Vec::new(), label })
    }

    fn monitor_exec(&mut self, sql: &str) -> Result<StmtOutcome, String> {
        match self.monitor.simple_query(sql) {
            Ok(rs) => Ok(fold_results(&rs)),
            Err(ConnLost(e)) => Err(format!("{} monitor lost: {e}", self.label)),
        }
    }

    /// Is `pid` in a wait state that means "parked on a lock"? None =
    /// monitor cannot tell (introspection gap; fall back to the window).
    fn pid_waiting(&mut self, pid: Option<i64>) -> Option<bool> {
        if !self.monitor_ok {
            return None;
        }
        let pid = pid?;
        let sql = format!(
            "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = {pid}"
        );
        match self.monitor_exec(&sql) {
            Ok(StmtOutcome::Rows { rows, .. }) => {
                let wet = rows.first().and_then(|r| r.first()).cloned().flatten();
                let we = rows.first().and_then(|r| r.get(1)).cloned().flatten();
                Some(
                    wet.as_deref() == Some("Lock")
                        || we.as_deref() == Some("SafeSnapshot"),
                )
            }
            _ => {
                // View/column missing or errored: introspection gap.
                // Report once; never a finding from this rig.
                self.monitor_ok = false;
                None
            }
        }
    }

    fn setup(&mut self, scenario: &Scenario) -> Result<(), String> {
        for sql in &scenario.setup {
            match self.monitor_exec(sql)? {
                StmtOutcome::Error { sqlstate, message } => {
                    return Err(format!(
                        "{} setup {sql:?} failed: {sqlstate} {message}",
                        self.label
                    ))
                }
                _ => {}
            }
        }
        for _ in 0..scenario.sessions {
            let h = spawn_session(&self.ep, &scenario.session_setup)
                .map_err(|e| format!("{}: {e}", self.label))?;
            self.sessions.push(h);
        }
        Ok(())
    }

    /// Reap parked steps that completed after `after` finished. Session
    /// order; loops until a full pass reaps nothing (a reaped COMMIT can
    /// unblock another parked step). The grace wait gives the server time
    /// to grant the freed lock and the freed session time to finish.
    fn reap(&mut self, after: usize, tun: &Tunables) {
        loop {
            let mut reaped = false;
            for s in 0..self.sessions.len() {
                let Some(stepidx) = self.sessions[s].parked else { continue };
                // Grace: poll up to block_window for the unblocked step.
                let deadline = Instant::now() + Duration::from_millis(tun.block_window_ms);
                loop {
                    match self.sessions[s].rx.recv_timeout(Duration::from_millis(tun.tick_ms)) {
                        Ok(out) => {
                            self.sessions[s].parked = None;
                            self.events.push(Event::Unblocked {
                                step: stepidx,
                                after,
                                key: outcome_key(&out),
                                msg: outcome_msg(&out),
                            });
                            reaped = true;
                            break;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            // Still waiting? Then it stays parked.
                            if self.pid_waiting(self.sessions[s].pid) == Some(true)
                                || Instant::now() >= deadline
                            {
                                break;
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            self.sessions[s].parked = None;
                            self.events.push(Event::Unblocked {
                                step: stepidx,
                                after,
                                key: OutcomeKey::ConnLost,
                                msg: "session thread gone".into(),
                            });
                            reaped = true;
                            break;
                        }
                    }
                }
            }
            if !reaped {
                return;
            }
        }
    }

    /// Issue one step and classify complete-vs-blocked.
    fn issue(&mut self, stepidx: usize, st: &Step, tun: &Tunables) -> Result<(), String> {
        let s = st.session;
        if s >= self.sessions.len() {
            return Err(format!("{}: step {} session {} out of range", self.label, st.name, s));
        }
        if let Some(parked_step) = self.sessions[s].parked {
            // The schedule needs this session, whose parked step must
            // resolve on its own first (canonical case: the deadlock
            // detector victimizes the parked waiter). Wait for it —
            // bounded — and record the resolution as Unblocked with
            // `after` = the step being issued (deterministic: both
            // engines reach this point at the same schedule position).
            match self.sessions[s]
                .rx
                .recv_timeout(Duration::from_millis(tun.step_deadline_ms))
            {
                Ok(out) => {
                    self.sessions[s].parked = None;
                    self.events.push(Event::Unblocked {
                        step: parked_step,
                        after: stepidx,
                        key: outcome_key(&out),
                        msg: outcome_msg(&out),
                    });
                }
                Err(_) => {
                    return Err(format!(
                        "{}: step {} targets session {s} whose parked step never resolved (deck ordering bug)",
                        self.label, st.name
                    ))
                }
            }
        }
        self.sessions[s]
            .tx
            .send(Job::Exec(st.sql.clone()))
            .map_err(|_| format!("{}: session {s} thread gone", self.label))?;
        let start = Instant::now();
        let window = Duration::from_millis(tun.block_window_ms);
        let deadline = Duration::from_millis(tun.step_deadline_ms);
        loop {
            match self.sessions[s].rx.recv_timeout(Duration::from_millis(tun.tick_ms)) {
                Ok(out) => {
                    self.events.push(Event::Completed {
                        step: stepidx,
                        key: outcome_key(&out),
                        msg: outcome_msg(&out),
                    });
                    self.reap(stepidx, tun);
                    return Ok(());
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    match self.pid_waiting(self.sessions[s].pid) {
                        Some(true) => {
                            self.sessions[s].parked = Some(stepidx);
                            self.events
                                .push(Event::Blocked { step: stepidx, detect: BlockDetect::Monitor });
                            return Ok(());
                        }
                        Some(false) => {
                            if start.elapsed() > deadline {
                                return Err(format!(
                                    "{}: step {} neither completed nor lock-waiting after {}ms (rig failure)",
                                    self.label, st.name, tun.step_deadline_ms
                                ));
                            }
                        }
                        None => {
                            if start.elapsed() > window {
                                self.sessions[s].parked = Some(stepidx);
                                self.events.push(Event::Blocked {
                                    step: stepidx,
                                    detect: BlockDetect::Window,
                                });
                                return Ok(());
                            }
                        }
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.events.push(Event::Completed {
                        step: stepidx,
                        key: OutcomeKey::ConnLost,
                        msg: "session thread gone".into(),
                    });
                    return Ok(());
                }
            }
        }
    }

    /// End-of-schedule drain: any still-parked step must complete (the
    /// decks always end with the lock holders committing).
    fn drain(&mut self, tun: &Tunables) -> Result<(), String> {
        for s in 0..self.sessions.len() {
            let Some(stepidx) = self.sessions[s].parked else { continue };
            match self.sessions[s]
                .rx
                .recv_timeout(Duration::from_millis(tun.step_deadline_ms))
            {
                Ok(out) => {
                    self.sessions[s].parked = None;
                    self.events.push(Event::Unblocked {
                        step: stepidx,
                        after: usize::MAX,
                        key: outcome_key(&out),
                        msg: outcome_msg(&out),
                    });
                }
                Err(_) => {
                    return Err(format!(
                        "{}: parked step {stepidx} never completed by scenario end (rig failure)",
                        self.label
                    ))
                }
            }
        }
        Ok(())
    }

    fn probes(&mut self, scenario: &Scenario) -> Result<(), String> {
        for (label, sql) in &scenario.probes {
            let out = self.monitor_exec(sql)?;
            self.events.push(Event::Probe { label, key: outcome_key(&out) });
        }
        Ok(())
    }

    fn teardown(&mut self) {
        for s in &mut self.sessions {
            let _ = s.tx.send(Job::Quit);
        }
        for s in &mut self.sessions {
            if let Some(j) = s.joiner.take() {
                let _ = j.join();
            }
        }
        self.sessions.clear();
    }

    pub fn monitor_gap(&self) -> bool {
        !self.monitor_ok
    }
}

/// Run one scenario on one engine, returning its event log.
pub fn run_engine(
    label: &'static str,
    ep: &Endpoint,
    scenario: &Scenario,
    tun: &Tunables,
) -> Result<(Vec<Event>, bool), String> {
    let mut run = EngineRun::new(label, ep.clone())?;
    let result: Result<(), String> = (|| {
        run.setup(scenario)?;
        for (i, st) in scenario.steps.iter().enumerate() {
            run.issue(i, st, tun)?;
        }
        run.drain(tun)?;
        Ok(())
    })();
    run.teardown();
    result?;
    run.probes(scenario)?;
    Ok((run.events.clone(), run.monitor_gap()))
}

// --------------------------------------------------------------- compare --

/// One divergence between the two engines' event logs.
#[derive(Clone, Debug)]
pub struct Divergence {
    /// SCHEDULE | OUTCOME | GROUP-INVARIANT | PROBE | LENGTH
    pub class: &'static str,
    pub detail: String,
}

fn fmt_key(k: &OutcomeKey) -> String {
    match k {
        OutcomeKey::Rows(_, rows) => {
            let cells: Vec<String> = rows
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|c| c.clone().unwrap_or_else(|| "NULL".into()))
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .collect();
            format!("rows[{}]", cells.join("|"))
        }
        OutcomeKey::Command(t) => format!("tag[{t}]"),
        OutcomeKey::Error(s) => format!("error[{s}]"),
        OutcomeKey::ConnLost => "connlost".into(),
    }
}

/// Final per-step digest: did the step ever block, and what did it
/// ultimately produce. The relaxed compare surface for runs whose event
/// shapes legitimately differ inside symmetric groups (a swapped deadlock
/// victim changes WHICH grouped step blocks).
fn step_digest(events: &[Event]) -> HashMap<usize, (bool, OutcomeKey)> {
    let mut blocked: std::collections::HashSet<usize> = Default::default();
    let mut out: HashMap<usize, (bool, OutcomeKey)> = HashMap::new();
    for e in events {
        match e {
            Event::Blocked { step, .. } => {
                blocked.insert(*step);
            }
            Event::Completed { step, key, .. } | Event::Unblocked { step, key, .. } => {
                out.insert(*step, (blocked.contains(step), key.clone()));
            }
            Event::Probe { .. } => {}
        }
    }
    out
}

/// Compare two event logs under the scenario's symmetric groups.
pub fn compare(scenario: &Scenario, a: &[Event], b: &[Event]) -> Vec<Divergence> {
    let mut out = Vec::new();
    // 1. Schedule shape must be identical — else the two engines took
    //    different lock-wait paths and everything downstream is skew.
    //    Exception: a shape difference confined to symmetric-group steps
    //    (deadlock victim swap flips which grouped step blocks) degrades
    //    to the relaxed per-step digest compare instead of a finding.
    let shape_a: Vec<String> = a.iter().map(Event::shape).collect();
    let shape_b: Vec<String> = b.iter().map(Event::shape).collect();
    if shape_a != shape_b {
        let grouped = |i: &usize| scenario.steps.get(*i).and_then(|s| s.group).is_some();
        let da = step_digest(a);
        let db = step_digest(b);
        // Ungrouped steps: blocked-classification and outcome must match
        // exactly; a mismatch there is a genuine schedule divergence.
        for (i, st) in scenario.steps.iter().enumerate() {
            if grouped(&i) {
                continue;
            }
            let ea = da.get(&i);
            let eb = db.get(&i);
            match (ea, eb) {
                (Some((ba, ka)), Some((bb, kb))) => {
                    if ba != bb {
                        out.push(Divergence {
                            class: "SCHEDULE",
                            detail: format!(
                                "step {}: blocked on {} only",
                                st.name,
                                if *ba { "A" } else { "B" }
                            ),
                        });
                    } else if ka != kb {
                        out.push(Divergence {
                            class: "OUTCOME",
                            detail: format!("step {}: A={} B={}", st.name, fmt_key(ka), fmt_key(kb)),
                        });
                    }
                }
                // Never ran on either side (comparison truncated at the
                // same earlier point): not a divergence of this step.
                (None, None) => {}
                _ => {
                    out.push(Divergence {
                        class: "SCHEDULE",
                        detail: format!("step {}: missing completion on one side", st.name),
                    });
                }
            }
        }
        // Grouped steps: outcome multiset per group.
        let mut ga: HashMap<&'static str, Vec<String>> = HashMap::new();
        let mut gb: HashMap<&'static str, Vec<String>> = HashMap::new();
        for (i, st) in scenario.steps.iter().enumerate() {
            let Some(g) = st.group else { continue };
            if let Some((_, k)) = da.get(&i) {
                ga.entry(g).or_default().push(fmt_key(k));
            }
            if let Some((_, k)) = db.get(&i) {
                gb.entry(g).or_default().push(fmt_key(k));
            }
        }
        for (g, mut va) in ga {
            let mut vb = gb.remove(g).unwrap_or_default();
            va.sort();
            vb.sort();
            if va != vb {
                out.push(Divergence {
                    class: "GROUP-INVARIANT",
                    detail: format!(
                        "group {g}: outcome multiset A={{{}}} B={{{}}}",
                        va.join("; "),
                        vb.join("; ")
                    ),
                });
            }
        }
        // Probes: strict, in order.
        let pa: Vec<(&'static str, &OutcomeKey)> = a
            .iter()
            .filter_map(|e| match e {
                Event::Probe { label, key } => Some((*label, key)),
                _ => None,
            })
            .collect();
        let pb: Vec<(&'static str, &OutcomeKey)> = b
            .iter()
            .filter_map(|e| match e {
                Event::Probe { label, key } => Some((*label, key)),
                _ => None,
            })
            .collect();
        if pa.len() != pb.len() {
            out.push(Divergence {
                class: "PROBE",
                detail: format!("probe counts differ: A={} B={}", pa.len(), pb.len()),
            });
        } else {
            for ((la, ka), (_, kb)) in pa.iter().zip(pb.iter()) {
                if ka != kb {
                    out.push(Divergence {
                        class: "PROBE",
                        detail: format!(
                            "final-state probe {la}: A={} B={}",
                            fmt_key(ka),
                            fmt_key(kb)
                        ),
                    });
                }
            }
        }
        return out;
    }
    // 2. Outcomes. Grouped steps: collect per-group multisets; ungrouped
    //    (and all probes): strict.
    let mut ga: HashMap<&'static str, Vec<String>> = HashMap::new();
    let mut gb: HashMap<&'static str, Vec<String>> = HashMap::new();
    for (ea, eb) in a.iter().zip(b.iter()) {
        let (stepidx, ka, kb) = match (ea, eb) {
            (
                Event::Completed { step, key: ka, .. },
                Event::Completed { key: kb, .. },
            )
            | (
                Event::Unblocked { step, key: ka, .. },
                Event::Unblocked { key: kb, .. },
            ) => (Some(*step), ka, kb),
            (Event::Probe { label, key: ka }, Event::Probe { key: kb, .. }) => {
                if ka != kb {
                    out.push(Divergence {
                        class: "PROBE",
                        detail: format!(
                            "final-state probe {label}: A={} B={}",
                            fmt_key(ka),
                            fmt_key(kb)
                        ),
                    });
                }
                continue;
            }
            (Event::Blocked { .. }, Event::Blocked { .. }) => continue,
            _ => continue, // shapes matched, so this is unreachable
        };
        let group = stepidx.and_then(|i| scenario.steps.get(i)).and_then(|s| s.group);
        match group {
            Some(g) => {
                ga.entry(g).or_default().push(fmt_key(ka));
                gb.entry(g).or_default().push(fmt_key(kb));
            }
            None => {
                if ka != kb {
                    let name = stepidx
                        .and_then(|i| scenario.steps.get(i))
                        .map(|s| s.name)
                        .unwrap_or("?");
                    out.push(Divergence {
                        class: "OUTCOME",
                        detail: format!(
                            "step {name}: A={} B={}",
                            fmt_key(ka),
                            fmt_key(kb)
                        ),
                    });
                }
            }
        }
    }
    for (g, mut va) in ga {
        let mut vb = gb.remove(g).unwrap_or_default();
        va.sort();
        vb.sort();
        if va != vb {
            out.push(Divergence {
                class: "GROUP-INVARIANT",
                detail: format!(
                    "group {g}: outcome multiset A={{{}}} B={{{}}}",
                    va.join("; "),
                    vb.join("; ")
                ),
            });
        }
    }
    out
}

/// Render an engine's event log as a human transcript (findings evidence).
pub fn transcript(scenario: &Scenario, events: &[Event]) -> String {
    let mut s = String::new();
    let name = |i: usize| -> String {
        if i == usize::MAX {
            "<end>".into()
        } else {
            scenario
                .steps
                .get(i)
                .map(|st| format!("{} (s{}: {})", st.name, st.session, st.sql))
                .unwrap_or_else(|| format!("step#{i}"))
        }
    };
    for e in events {
        match e {
            Event::Completed { step, key, msg } => {
                s.push_str(&format!("completed  {} -> {}", name(*step), fmt_key(key)));
                if !msg.is_empty() {
                    s.push_str(&format!("  [{msg}]"));
                }
                s.push('\n');
            }
            Event::Blocked { step, detect } => {
                s.push_str(&format!("BLOCKED    {} (detect={detect:?})\n", name(*step)));
            }
            Event::Unblocked { step, after, key, msg } => {
                s.push_str(&format!(
                    "unblocked  {} after {} -> {}",
                    name(*step),
                    if *after == usize::MAX { "<end>".into() } else {
                        scenario.steps.get(*after).map(|st| st.name.to_string())
                            .unwrap_or_else(|| format!("step#{after}"))
                    },
                    fmt_key(key)
                ));
                if !msg.is_empty() {
                    s.push_str(&format!("  [{msg}]"));
                }
                s.push('\n');
            }
            Event::Probe { label, key } => {
                s.push_str(&format!("probe      {label} -> {}\n", fmt_key(key)));
            }
        }
    }
    s
}

// -------------------------------------------------------- scenario decks --

const T_SSI: &str = "DROP TABLE IF EXISTS ssi_t; \
     CREATE TABLE ssi_t (id int PRIMARY KEY, class int, v int); \
     INSERT INTO ssi_t SELECT g, g % 2 + 1, 10 FROM generate_series(1, 200) g";

fn probe_ssi_t() -> (&'static str, String) {
    ("ssi_t", "SELECT id, class, v FROM ssi_t ORDER BY id".to_string())
}

fn begin_srl() -> &'static str {
    "BEGIN ISOLATION LEVEL SERIALIZABLE"
}

/// The full hand-authored deck.
pub fn deck() -> Vec<Scenario> {
    let mut v = Vec::new();

    // 1. Classic write skew: both read the other's class, then update
    //    their own. First committer wins; s2's COMMIT must fail 40001.
    v.push(Scenario {
        name: "write-skew",
        description: "classic SSI write skew: rw-antidependency cycle, second committer aborts 40001",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s1-read", 0, "SELECT sum(v) FROM ssi_t WHERE class = 2"),
            step("s2-read", 1, "SELECT sum(v) FROM ssi_t WHERE class = 1"),
            step("s1-write", 0, "UPDATE ssi_t SET v = v + 100 WHERE class = 1"),
            step("s2-write", 1, "UPDATE ssi_t SET v = v + 100 WHERE class = 2"),
            step("s1-commit", 0, "COMMIT"),
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 2. Write skew through index scans: predicate locks land on btree
    //    pages instead of the relation/tuples of a seqscan.
    v.push(Scenario {
        name: "write-skew-index",
        description: "write skew with forced index scans (btree page predicate locks)",
        setup: vec![
            T_SSI.into(),
            "CREATE INDEX ssi_t_class ON ssi_t (class, id)".into(),
        ],
        sessions: 2,
        session_setup: vec![
            "SET statement_timeout = '20s'".into(),
            "SET enable_seqscan = off".into(),
            "SET enable_bitmapscan = off".into(),
        ],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s1-read", 0, "SELECT count(*) FROM ssi_t WHERE class = 2 AND id < 50"),
            step("s2-read", 1, "SELECT count(*) FROM ssi_t WHERE class = 1 AND id < 50"),
            step("s1-write", 0, "UPDATE ssi_t SET v = v + 1 WHERE class = 1 AND id < 50"),
            step("s2-write", 1, "UPDATE ssi_t SET v = v + 1 WHERE class = 2 AND id < 50"),
            step("s1-commit", 0, "COMMIT"),
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 3. Relation-level predicate locks (whole-table reads) + write skew.
    v.push(Scenario {
        name: "write-skew-relation",
        description: "write skew with whole-table seqscan reads (relation-level predicate locks)",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s1-read", 0, "SELECT sum(v) FROM ssi_t"),
            step("s2-read", 1, "SELECT count(*) FROM ssi_t"),
            step("s1-write", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 7"),
            step("s2-write", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 8"),
            step("s1-commit", 0, "COMMIT"),
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 4a. Three-transaction rw chain whose commit order (T_in first)
    //     leaves a valid serialization order: NOTHING may abort. A
    //     control against over-aggressive conflict detection.
    v.push(Scenario {
        name: "pivot-3txn-safe",
        description: "three-txn rw chain, T_in commits first: serializable, all commit clean",
        setup: vec![T_SSI.into()],
        sessions: 3,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s3-begin", 2, begin_srl()),
            step("s2-read-a", 1, "SELECT v FROM ssi_t WHERE id = 1"),
            step("s2-write-b", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 2"),
            step("s3-read-b", 2, "SELECT v FROM ssi_t WHERE id = 2"),
            step("s3-commit", 2, "COMMIT"),
            step("s1-write-a", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 1"),
            step("s1-commit", 0, "COMMIT"),
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 4b. Same rw chain (T3 -rw-> T2 -rw-> T1) but T_out (T1) commits
    //     FIRST: the dangerous structure is real and SSI must abort a
    //     participant (C's choice of victim/step is the spec; the
    //     schedule is fully ordered so it is deterministic).
    v.push(Scenario {
        name: "pivot-3txn-abort",
        description: "three-txn rw-antidependency chain, T_out commits first: SSI abort",
        setup: vec![T_SSI.into()],
        sessions: 3,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s3-begin", 2, begin_srl()),
            step("s2-read-a", 1, "SELECT v FROM ssi_t WHERE id = 1"),
            step("s2-write-b", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 2"),
            step("s3-read-b", 2, "SELECT v FROM ssi_t WHERE id = 2"),
            step("s1-write-a", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 1"),
            step("s1-commit", 0, "COMMIT"), // T_out commits first
            step("s3-commit", 2, "COMMIT"),
            step("s2-commit", 1, "COMMIT"), // pivot: expect 40001 per C
        ],
        probes: vec![probe_ssi_t()],
    });

    // 5. Read-only anomaly: the read-only transaction creates the
    //    dangerous structure; SSI aborts one participant.
    v.push(Scenario {
        name: "readonly-anomaly",
        description: "read-only snapshot anomaly (batch/receipt shape)",
        setup: vec![
            "DROP TABLE IF EXISTS ssi_acct; CREATE TABLE ssi_acct (name text PRIMARY KEY, bal int); \
             INSERT INTO ssi_acct VALUES ('checking', 0), ('savings', 0)"
                .into(),
        ],
        sessions: 3,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s1-read", 0, "SELECT bal FROM ssi_acct WHERE name IN ('checking','savings') ORDER BY name"),
            step("s2-withdraw", 1, "UPDATE ssi_acct SET bal = bal - 200 WHERE name = 'checking'"),
            step("s2-commit", 1, "COMMIT"),
            step("s3-begin", 2, begin_srl()),
            step("s3-read", 2, "SELECT name, bal FROM ssi_acct ORDER BY name"),
            step("s3-commit", 2, "COMMIT"),
            step("s1-deposit", 0, "UPDATE ssi_acct SET bal = bal + 100 WHERE name = 'savings'"),
            step("s1-commit", 0, "COMMIT"),
        ],
        probes: vec![("ssi_acct", "SELECT name, bal FROM ssi_acct ORDER BY name".into())],
    });

    // 6. Safe snapshot: SERIALIZABLE READ ONLY DEFERRABLE blocks until
    //    concurrent serializable writers finish (SafeSnapshot wait).
    v.push(Scenario {
        name: "safe-snapshot",
        description: "SERIALIZABLE READ ONLY DEFERRABLE waits for safe snapshot; released by writer commit",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s1-write", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 3"),
            step("s2-begin", 1, "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE"),
            step("s2-read", 1, "SELECT sum(v) FROM ssi_t"), // blocks: SafeSnapshot
            // While s2 waits for its safe snapshot, count backends with a
            // nonempty safe-snapshot blocker set (pids never compared).
            step(
                "s1-safe-blocked",
                0,
                "SELECT count(*) FROM pg_stat_activity a WHERE cardinality(pg_safe_snapshot_blocking_pids(a.pid)) > 0",
            ),
            step("s1-commit", 0, "COMMIT"),                  // releases s2
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 7. FOR UPDATE wait then proceed (READ COMMITTED: waiter sees the
    //    committed update via EPQ recheck).
    v.push(Scenario {
        name: "forupdate-wait-rc",
        description: "READ COMMITTED SELECT FOR UPDATE waits, then reads the committed row (EPQ)",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-lock", 0, "SELECT id, v FROM ssi_t WHERE id = 5 FOR UPDATE"),
            step("s1-upd", 0, "UPDATE ssi_t SET v = 77 WHERE id = 5"),
            step("s2-begin", 1, "BEGIN"),
            step("s2-lock", 1, "SELECT id, v FROM ssi_t WHERE id = 5 FOR UPDATE"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 unblocks, sees v=77
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 8. REPEATABLE READ concurrent update: waiter gets 40001 after the
    //    holder commits.
    v.push(Scenario {
        name: "rr-update-conflict",
        description: "REPEATABLE READ UPDATE on concurrently-updated row -> 40001 for the waiter",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s2-begin", 1, "BEGIN ISOLATION LEVEL REPEATABLE READ"),
            step("s1-read", 0, "SELECT v FROM ssi_t WHERE id = 9"),
            step("s2-read", 1, "SELECT v FROM ssi_t WHERE id = 9"),
            step("s1-upd", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 9"),
            step("s2-upd", 1, "UPDATE ssi_t SET v = v + 5 WHERE id = 9"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 unblocks with 40001
            step("s2-rollback", 1, "ROLLBACK"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 9. Deadlock: s1 locks 11 then wants 12; s2 locks 12 then wants 11.
    //    s1 blocks first, so s1's deadlock_timeout fires first and s1 is
    //    victimized — but victim identity is scheduler-adjacent, so the
    //    lock steps and commits share a symmetric group: the INVARIANT is
    //    exactly one 40P01, the survivor commits, final state identical.
    v.push(Scenario {
        name: "deadlock-2way",
        description: "two-session row-lock deadlock; invariant: exactly one 40P01",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec![
            "SET statement_timeout = '20s'".into(),
            "SET deadlock_timeout = '500ms'".into(),
        ],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s1-lock11", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 11"),
            step("s2-lock12", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 12"),
            gstep("s1-want12", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 12", "dl"),
            gstep("s2-want11", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 11", "dl"),
            gstep("s1-end", 0, "COMMIT", "dl"),
            gstep("s2-end", 1, "COMMIT", "dl"),
        ],
        // Final state IS deterministic: the survivor's two updates land,
        // the victim's roll back — and either victim choice yields the
        // same table because the updates are symmetric (+1 to 11 and 12
        // from whichever survives... they are NOT symmetric per-row, so
        // probe on the sum instead of per-row values.
        probes: vec![(
            "ssi_t-sum",
            "SELECT count(*), sum(v) FROM ssi_t WHERE id IN (11, 12)".into(),
        )],
    });

    // 9b. Three-session lock cycle (s1 -> s2 -> s3 -> s1): the deadlock
    //     detector's TopoSort/hard-deadlock path. Same invariant model as
    //     deadlock-2way: exactly one 40P01 among the grouped steps.
    v.push(Scenario {
        name: "deadlock-3way",
        description: "three-session lock cycle; invariant: exactly one 40P01, others commit",
        setup: vec![T_SSI.into()],
        sessions: 3,
        session_setup: vec![
            "SET statement_timeout = '20s'".into(),
            "SET deadlock_timeout = '500ms'".into(),
        ],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s3-begin", 2, "BEGIN"),
            step("s1-lock14", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 14"),
            step("s2-lock15", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 15"),
            step("s3-lock16", 2, "UPDATE ssi_t SET v = v + 1 WHERE id = 16"),
            gstep("s1-want15", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 15", "dl3"),
            gstep("s2-want16", 1, "UPDATE ssi_t SET v = v + 1 WHERE id = 16", "dl3"),
            gstep("s3-want14", 2, "UPDATE ssi_t SET v = v + 1 WHERE id = 14", "dl3"),
            gstep("s1-end", 0, "COMMIT", "dl3"),
            gstep("s2-end", 1, "COMMIT", "dl3"),
            gstep("s3-end", 2, "COMMIT", "dl3"),
        ],
        // Whatever the victim, the two survivors' +1s land on their held
        // row and their wanted row: sum over the three rows is +4.
        probes: vec![(
            "ssi_t-sum",
            "SELECT count(*), sum(v) FROM ssi_t WHERE id IN (14, 15, 16)".into(),
        )],
    });

    // 10. Multixact: two FOR SHARE holders, then an UPDATE waits on the
    //     multixact; one holder commits, then the other; updater proceeds.
    v.push(Scenario {
        name: "multixact-share",
        description: "two FOR SHARE holders (multixact create) + UPDATE waiting on both members",
        setup: vec![T_SSI.into()],
        sessions: 3,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s3-begin", 2, "BEGIN"),
            step("s1-share", 0, "SELECT id FROM ssi_t WHERE id = 21 FOR SHARE"),
            step("s2-share", 1, "SELECT id FROM ssi_t WHERE id = 21 FOR SHARE"),
            step("s3-upd", 2, "UPDATE ssi_t SET v = 99 WHERE id = 21"), // blocks on multixact
            step("s1-commit", 0, "COMMIT"), // s3 still blocked (s2 holds)
            step("s2-commit", 1, "COMMIT"), // s3 unblocks
            step("s3-commit", 2, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 11. Multixact key-share mix + lock upgrade: KEY SHARE holders allow
    //     a NO KEY UPDATE... then a SHARE holder upgrades to UPDATE while
    //     another SHARE holder exists (upgrade waits).
    v.push(Scenario {
        name: "multixact-upgrade",
        description: "FOR KEY SHARE + FOR NO KEY UPDATE coexist; FOR SHARE -> FOR UPDATE upgrade waits on co-holder",
        setup: vec![T_SSI.into()],
        sessions: 3,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s3-begin", 2, "BEGIN"),
            step("s1-keyshare", 0, "SELECT id FROM ssi_t WHERE id = 31 FOR KEY SHARE"),
            step("s2-nokeyupd", 1, "SELECT id FROM ssi_t WHERE id = 31 FOR NO KEY UPDATE"),
            step("s2-release", 1, "ROLLBACK"),
            step("s2-begin2", 1, "BEGIN"),
            step("s2-share", 1, "SELECT id FROM ssi_t WHERE id = 31 FOR SHARE"),
            step("s3-share", 2, "SELECT id FROM ssi_t WHERE id = 31 FOR SHARE"),
            step("s2-upgrade", 1, "SELECT id FROM ssi_t WHERE id = 31 FOR UPDATE"), // waits on s3 + s1
            step("s3-commit", 2, "COMMIT"), // s2 still waits on s1's KEY SHARE? (KEY SHARE conflicts with UPDATE)
            step("s1-commit", 0, "COMMIT"), // s2 unblocks
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 11b. Multixact carrying an UPDATE: FOR KEY SHARE + concurrent
    //      no-key UPDATE coexist (xmax = multixact with an update
    //      member); a reader then resolves the updating xid out of the
    //      multixact (MultiXactIdGetUpdateXid), and a FOR UPDATE waits
    //      on both members.
    v.push(Scenario {
        name: "multixact-keyupdate",
        description: "FOR KEY SHARE + no-key UPDATE multixact; reader resolves update xid; FOR UPDATE waits",
        setup: vec![T_SSI.into()],
        sessions: 3,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s3-begin", 2, "BEGIN"),
            step("s1-keyshare", 0, "SELECT id FROM ssi_t WHERE id = 71 FOR KEY SHARE"),
            // Non-key UPDATE: compatible with KEY SHARE -> multixact
            // {s1 keyshare, s2 nokeyupdate}.
            step("s2-upd", 1, "UPDATE ssi_t SET v = 500 WHERE id = 71"),
            // Reader must resolve the update xid inside the multixact to
            // decide visibility: sees the old version.
            step("s3-read", 2, "SELECT v FROM ssi_t WHERE id = 71"),
            // FOR UPDATE conflicts with both members: waits.
            step("s3-forupd", 2, "SELECT id, v FROM ssi_t WHERE id = 71 FOR UPDATE"),
            step("s2-commit", 1, "COMMIT"), // still blocked on s1
            step("s1-commit", 0, "COMMIT"), // s3 unblocks, sees v=500
            step("s3-commit", 2, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 11c. Another session's temp namespace: pg_is_other_temp_schema
    //      and checkTempNamespaceStatus surfaces.
    v.push(Scenario {
        name: "temp-namespace",
        description: "temp table in s1; s2 probes pg_is_other_temp_schema over pg_namespace",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-mktemp", 0, "CREATE TEMP TABLE q4ssitmp (a int)"),
            step("s1-fill", 0, "INSERT INTO q4ssitmp VALUES (1), (2)"),
            step("s1-own", 0, "SELECT pg_is_other_temp_schema(pg_my_temp_schema())"),
            step(
                "s2-other",
                1,
                "SELECT count(*) FROM pg_namespace n WHERE pg_is_other_temp_schema(n.oid)",
            ),
            step("s1-drop", 0, "DROP TABLE q4ssitmp"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 12. NOWAIT + SKIP LOCKED against held row locks.
    v.push(Scenario {
        name: "nowait-skiplocked",
        description: "FOR UPDATE NOWAIT -> 55P03; SKIP LOCKED returns the unlocked remainder",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-lock", 0, "SELECT id FROM ssi_t WHERE id IN (41, 42) ORDER BY id FOR UPDATE"),
            step("s2-nowait", 1, "SELECT id FROM ssi_t WHERE id IN (41, 43) ORDER BY id FOR UPDATE NOWAIT"),
            step("s2-skip", 1, "SELECT id FROM ssi_t WHERE id BETWEEN 41 AND 45 ORDER BY id FOR UPDATE SKIP LOCKED"),
            step("s2-share-nowait", 1, "SELECT id FROM ssi_t WHERE id = 41 FOR SHARE NOWAIT"),
            step("s2-skip-share", 1, "SELECT id FROM ssi_t WHERE id BETWEEN 40 AND 43 ORDER BY id FOR SHARE SKIP LOCKED"),
            step("s1-commit", 0, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 13. pg_blocking_pids / pg_safe_snapshot_blocking_pids cardinality
    //     during real waits (pids themselves never compared: the probes
    //     compare COUNTS via cardinality()).
    v.push(Scenario {
        name: "blocking-pids",
        description: "pg_blocking_pids cardinality during a lock wait (monitor-side, count only)",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s1-lock", 0, "SELECT id FROM ssi_t WHERE id = 51 FOR UPDATE"),
            step("s2-begin", 1, "BEGIN"),
            step("s2-wait", 1, "SELECT id FROM ssi_t WHERE id = 51 FOR UPDATE"), // blocks
            // While s2 is parked, count how many backends have a nonzero
            // blocker set (should be exactly 1: s2 blocked by s1).
            step(
                "s1-count-blocked",
                0,
                "SELECT count(*) FROM pg_stat_activity a WHERE cardinality(pg_blocking_pids(a.pid)) > 0",
            ),
            step(
                "s1-isolation-blocked",
                0,
                "SELECT count(*) FROM pg_stat_activity a WHERE pg_isolation_test_session_is_blocked(a.pid, '{}')",
            ),
            step("s1-commit", 0, "COMMIT"),
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 14. Unique-key wait: second inserter blocks on the first's xact;
    //     commit -> 23505; and the rollback variant succeeds.
    v.push(Scenario {
        name: "unique-wait",
        description: "duplicate-pk insert waits on inserter's xact; commit -> 23505, rollback -> success",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, "BEGIN"),
            step("s2-begin", 1, "BEGIN"),
            step("s1-ins", 0, "INSERT INTO ssi_t VALUES (1001, 1, 1)"),
            step("s2-ins", 1, "INSERT INTO ssi_t VALUES (1001, 2, 2)"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 unblocks with 23505
            step("s2-rollback", 1, "ROLLBACK"),
            step("s1-begin2", 0, "BEGIN"),
            step("s2-begin2", 1, "BEGIN"),
            step("s1-ins2", 0, "INSERT INTO ssi_t VALUES (1002, 1, 1)"),
            step("s2-ins2", 1, "INSERT INTO ssi_t VALUES (1002, 2, 2)"), // blocks
            step("s1-abort2", 0, "ROLLBACK"), // s2 unblocks, insert succeeds
            step("s2-commit2", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 15. SSI write-write on the same rows: under SERIALIZABLE a plain
    //     write-write conflict surfaces as a lock wait then 40001
    //     ("could not serialize access due to concurrent update").
    v.push(Scenario {
        name: "ssi-ww-samerow",
        description: "SERIALIZABLE write-write on same row: waiter gets 40001 after holder commits",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s2-begin", 1, begin_srl()),
            step("s1-upd", 0, "UPDATE ssi_t SET v = v + 1 WHERE id = 61"),
            step("s2-upd", 1, "UPDATE ssi_t SET v = v + 7 WHERE id = 61"), // blocks
            step("s1-commit", 0, "COMMIT"), // s2 unblocks with 40001
            step("s2-rollback", 1, "ROLLBACK"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // 16. No-conflict serializable control: sequential rw on the same
    //     data must NOT abort (guards against over-aggressive conflict
    //     detection on B).
    v.push(Scenario {
        name: "ssi-no-conflict",
        description: "control: non-overlapping serializable txns commit clean (no false 40001)",
        setup: vec![T_SSI.into()],
        sessions: 2,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps: vec![
            step("s1-begin", 0, begin_srl()),
            step("s1-read", 0, "SELECT sum(v) FROM ssi_t WHERE class = 1"),
            step("s1-write", 0, "UPDATE ssi_t SET v = v + 1 WHERE class = 1 AND id <= 4"),
            step("s1-commit", 0, "COMMIT"),
            step("s2-begin", 1, begin_srl()),
            step("s2-read", 1, "SELECT sum(v) FROM ssi_t WHERE class = 1"),
            step("s2-write", 1, "UPDATE ssi_t SET v = v + 1 WHERE class = 1 AND id <= 4"),
            step("s2-commit", 1, "COMMIT"),
        ],
        probes: vec![probe_ssi_t()],
    });

    // CONCUR lane: EPQ recheck / MERGE re-check / ON CONFLICT races /
    // lock-mode matrix / CIC-REINDEX CONCURRENTLY waits / DETACH
    // CONCURRENTLY cancel — the SQL-reachable concurrency arms from the
    // Antithesis fault-only inventory (see crate::concur).
    v.extend(crate::concur::deck());

    v
}

pub fn scenario_by_name(name: &str) -> Option<Scenario> {
    deck().into_iter().find(|s| s.name == name)
}

// ---------------------------------------------- seeded interleaving mode --

/// Seeded random 2-3 session interleaving over ssi_t. Deterministic given
/// the seed: statements per session are generated first, then merged by a
/// seeded shuffle into one issuance order. Within a transaction every
/// row-locking target set is ASCENDING in id and sessions get disjoint
/// insert key ranges, so no deadlock cycles arise from the generated row
/// locks — deadlock nondeterminism stays in the hand deck where it is
/// invariant-compared. Lock WAITS still happen freely (the point), and
/// the engine's blocked/parked machinery makes the schedule a
/// deterministic function of which statements blocked.
pub fn random_scenario(seed: u64, steps_per_session: usize) -> Scenario {
    let mut rng = crate::rng::Rng::new(seed ^ 0x5551_D1CE);
    let nsessions = 2 + rng.below(2) as usize; // 2 or 3
    let iso = ["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"];
    let mut per_session: Vec<Vec<String>> = Vec::new();
    for s in 0..nsessions {
        let mut stmts = Vec::new();
        let lvl = iso[rng.below_usize(iso.len())];
        stmts.push(format!("BEGIN ISOLATION LEVEL {lvl}"));
        // Ascending lock-target discipline: pick this txn's touch points
        // up front, sort, and emit lock/write statements in that order.
        let mut targets: Vec<u32> = (0..steps_per_session)
            .map(|_| 1 + rng.below(200) as u32)
            .collect();
        targets.sort_unstable();
        targets.dedup();
        for (i, id) in targets.iter().enumerate() {
            let roll = rng.below(100);
            let stmt = if roll < 25 {
                format!("SELECT count(*), COALESCE(sum(v), 0) FROM ssi_t WHERE id BETWEEN {} AND {}", id, id + 1 + rng.below(20) as u32)
            } else if roll < 45 {
                format!("UPDATE ssi_t SET v = v + 1 WHERE id = {id}")
            } else if roll < 60 {
                format!("SELECT id FROM ssi_t WHERE id = {id} FOR UPDATE")
            } else if roll < 72 {
                format!("SELECT id FROM ssi_t WHERE id = {id} FOR SHARE")
            } else if roll < 80 {
                format!("SELECT id FROM ssi_t WHERE id = {id} FOR KEY SHARE")
            } else if roll < 86 {
                format!(
                    "SELECT id FROM ssi_t WHERE id BETWEEN {} AND {} ORDER BY id FOR UPDATE SKIP LOCKED",
                    id,
                    id + 3
                )
            } else if roll < 92 {
                format!("SELECT id FROM ssi_t WHERE id = {id} FOR SHARE NOWAIT")
            } else {
                // Disjoint per-session insert range: no cross-session
                // unique-key deadlocks, waits only via row locks above.
                format!(
                    "INSERT INTO ssi_t VALUES ({}, 1, 0) ON CONFLICT (id) DO UPDATE SET v = ssi_t.v + 1",
                    1000 + s as u32 * 1000 + i as u32
                )
            };
            stmts.push(stmt);
        }
        stmts.push(if rng.below(4) == 0 { "ROLLBACK".into() } else { "COMMIT".into() });
        per_session.push(stmts);
    }
    // Seeded merge preserving per-session order. IMPORTANT determinism
    // rule: a step whose session is parked cannot be issued; the ENGINE
    // handles that by erroring — so the merge instead interleaves whole
    // prefixes: we emit in rounds, and the engine's blocked semantics
    // keep the issuance order fixed regardless of outcomes because a
    // parked session's next step only arrives after a completion that
    // reaps it... To keep the schedule literally identical on both
    // engines even when a session parks, the merged order alternates so
    // that after any step that CAN block, the next steps favor OTHER
    // sessions, and every txn ends with COMMIT/ROLLBACK which reaps.
    // Concretely: weighted round-robin driven only by the seed.
    let mut order: Vec<usize> = Vec::new();
    let mut idx = vec![0usize; nsessions];
    let total: usize = per_session.iter().map(|v| v.len()).sum();
    while order.len() < total {
        let s = rng.below_usize(nsessions);
        if idx[s] < per_session[s].len() {
            order.push(s);
            idx[s] += 1;
        }
    }
    // Materialize steps. Leak the names (static requirement; bounded by
    // scenario size and process lifetime — this is a test driver).
    let mut steps = Vec::new();
    let mut taken = vec![0usize; nsessions];
    for s in order {
        let sql = per_session[s][taken[s]].clone();
        taken[s] += 1;
        let name: &'static str =
            Box::leak(format!("r{s}-{}", taken[s]).into_boxed_str());
        steps.push(Step { name, session: s, sql, group: None });
    }
    Scenario {
        name: Box::leak(format!("rand-{seed}").into_boxed_str()),
        description: "seeded random interleaving over ssi_t (ascending lock targets, no generated deadlocks)",
        setup: vec![T_SSI.into()],
        sessions: nsessions,
        session_setup: vec!["SET statement_timeout = '20s'".into()],
        steps,
        probes: vec![probe_ssi_t()],
    }
}

// ------------------------------------------------------------- run + cmp --

/// Result of one scenario differential.
pub struct ScenarioVerdict {
    pub scenario: &'static str,
    pub divergences: Vec<Divergence>,
    pub transcript_a: String,
    pub transcript_b: String,
    pub monitor_gap_a: bool,
    pub monitor_gap_b: bool,
}

/// Run a scenario on both engines (sequentially — the schedule is the
/// determinism carrier, not wall-clock overlap) and compare.
pub fn run_differential(
    a: &Endpoint,
    b: &Endpoint,
    scenario: &Scenario,
    tun: &Tunables,
) -> Result<ScenarioVerdict, String> {
    let (ev_a, gap_a) = run_engine("A", a, scenario, tun)?;
    let (ev_b, gap_b) = run_engine("B", b, scenario, tun)?;
    let divergences = compare(scenario, &ev_a, &ev_b);
    Ok(ScenarioVerdict {
        scenario: scenario.name,
        divergences,
        transcript_a: transcript(scenario, &ev_a),
        transcript_b: transcript(scenario, &ev_b),
        monitor_gap_a: gap_a,
        monitor_gap_b: gap_b,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deck_is_well_formed() {
        let d = deck();
        assert!(d.len() >= 14);
        for sc in &d {
            let mut names = std::collections::HashSet::new();
            for st in &sc.steps {
                assert!(st.session < sc.sessions, "{}: {}", sc.name, st.name);
                assert!(names.insert(st.name), "{}: dup step {}", sc.name, st.name);
            }
            assert!(!sc.probes.is_empty(), "{}: no final-state probe", sc.name);
        }
    }

    #[test]
    fn random_scenarios_are_deterministic() {
        for seed in [1u64, 7, 42] {
            let a = random_scenario(seed, 6);
            let b = random_scenario(seed, 6);
            assert_eq!(a.sessions, b.sessions);
            let sa: Vec<_> = a.steps.iter().map(|s| (s.session, s.sql.clone())).collect();
            let sb: Vec<_> = b.steps.iter().map(|s| (s.session, s.sql.clone())).collect();
            assert_eq!(sa, sb, "seed {seed} not deterministic");
        }
        // Different seeds should (overwhelmingly) differ.
        let a = random_scenario(1, 6);
        let b = random_scenario(2, 6);
        let sa: Vec<_> = a.steps.iter().map(|s| s.sql.clone()).collect();
        let sb: Vec<_> = b.steps.iter().map(|s| s.sql.clone()).collect();
        assert_ne!(sa, sb);
    }

    #[test]
    fn random_lock_targets_ascend_within_txn() {
        // The no-generated-deadlock discipline: row-lock/write targets
        // within one session's txn must be issued in ascending id order.
        for seed in 0..50u64 {
            let sc = random_scenario(seed, 8);
            for s in 0..sc.sessions {
                let mut last: i64 = -1;
                for st in sc.steps.iter().filter(|st| st.session == s) {
                    let sql = &st.sql;
                    let is_lock = sql.starts_with("UPDATE ssi_t SET v = v + 1 WHERE id = ")
                        || (sql.starts_with("SELECT id FROM ssi_t WHERE id = ")
                            && sql.contains(" FOR ")
                            && !sql.contains("SKIP LOCKED"));
                    if !is_lock {
                        if sql.starts_with("BEGIN") {
                            last = -1;
                        }
                        continue;
                    }
                    let id: i64 = sql
                        .split("id = ")
                        .nth(1)
                        .unwrap()
                        .split(|c: char| !c.is_ascii_digit())
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap();
                    assert!(id > last, "seed {seed} session {s}: {sql} after id {last}");
                    last = id;
                }
            }
        }
    }

    #[test]
    fn compare_flags_schedule_divergence_first() {
        let sc = &deck()[0];
        let a = vec![Event::Completed {
            step: 0,
            key: OutcomeKey::Command("BEGIN".into()),
            msg: String::new(),
        }];
        let b = vec![Event::Blocked { step: 0, detect: BlockDetect::Monitor }];
        let d = compare(sc, &a, &b);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].class, "SCHEDULE");
    }

    #[test]
    fn compare_group_invariant_masks_symmetric_victims() {
        // deadlock-2way: A victimizes s1, B victimizes s2 — same multiset,
        // no divergence.
        let sc = scenario_by_name("deadlock-2way").unwrap();
        let g = |i: usize| sc.steps[i].group;
        assert!(g(4).is_some() && g(5).is_some() && g(6).is_some() && g(7).is_some());
        let mk = |victim_first: bool| -> Vec<Event> {
            let (k1, k2) = if victim_first {
                (OutcomeKey::Error("40P01".into()), OutcomeKey::Command("UPDATE 1".into()))
            } else {
                (OutcomeKey::Command("UPDATE 1".into()), OutcomeKey::Error("40P01".into()))
            };
            let (c1, c2) = if victim_first {
                (OutcomeKey::Command("ROLLBACK".into()), OutcomeKey::Command("COMMIT".into()))
            } else {
                (OutcomeKey::Command("COMMIT".into()), OutcomeKey::Command("ROLLBACK".into()))
            };
            vec![
                Event::Completed { step: 0, key: OutcomeKey::Command("BEGIN".into()), msg: String::new() },
                Event::Completed { step: 1, key: OutcomeKey::Command("BEGIN".into()), msg: String::new() },
                Event::Completed { step: 2, key: OutcomeKey::Command("UPDATE 1".into()), msg: String::new() },
                Event::Completed { step: 3, key: OutcomeKey::Command("UPDATE 1".into()), msg: String::new() },
                Event::Blocked { step: 4, detect: BlockDetect::Monitor },
                Event::Completed { step: 5, key: k2.clone(), msg: String::new() },
                Event::Unblocked { step: 4, after: 5, key: k1.clone(), msg: String::new() },
                Event::Completed { step: 6, key: c1, msg: String::new() },
                Event::Completed { step: 7, key: c2, msg: String::new() },
            ]
        };
        // Same shape, swapped victims: group-invariant must hold.
        let d = compare(&sc, &mk(true), &mk(false));
        assert!(d.is_empty(), "{d:?}");
        // Both aborting IS a divergence.
        let mut both = mk(true);
        // make the survivor also 40P01
        if let Event::Completed { key, .. } = &mut both[5] {
            *key = OutcomeKey::Error("40P01".into());
        }
        if let Event::Completed { key, .. } = &mut both[7] {
            *key = OutcomeKey::Command("ROLLBACK".into());
        }
        let d = compare(&sc, &both, &mk(false));
        assert!(d.iter().any(|x| x.class == "GROUP-INVARIANT"), "{d:?}");
    }

    #[test]
    fn compare_relaxed_masks_victim_shape_swap() {
        // Shapes differ because B's victim is the ISSUED step (errors
        // completed) while A's victim is the PARKED step: confined to the
        // symmetric group -> relaxed compare, no divergence.
        let sc = scenario_by_name("deadlock-2way").unwrap();
        let pre = |ev: &mut Vec<Event>| {
            for i in 0..4 {
                ev.push(Event::Completed {
                    step: i,
                    key: OutcomeKey::Command(if i < 2 { "BEGIN" } else { "UPDATE 1" }.into()),
                    msg: String::new(),
                });
            }
        };
        // A: s1 (parked) victimized.
        let mut a = Vec::new();
        pre(&mut a);
        a.push(Event::Blocked { step: 4, detect: BlockDetect::Monitor });
        a.push(Event::Completed { step: 5, key: OutcomeKey::Command("UPDATE 1".into()), msg: String::new() });
        a.push(Event::Unblocked { step: 4, after: 5, key: OutcomeKey::Error("40P01".into()), msg: String::new() });
        a.push(Event::Completed { step: 6, key: OutcomeKey::Command("ROLLBACK".into()), msg: String::new() });
        a.push(Event::Completed { step: 7, key: OutcomeKey::Command("COMMIT".into()), msg: String::new() });
        a.push(Event::Probe { label: "ssi_t-sum", key: OutcomeKey::Rows(vec![20, 20], vec![vec![Some("2".into()), Some("22".into())]]) });
        // B: s2 (issued) victimized; s1's parked step unblocks after s2-end.
        let mut b = Vec::new();
        pre(&mut b);
        b.push(Event::Blocked { step: 4, detect: BlockDetect::Monitor });
        b.push(Event::Completed { step: 5, key: OutcomeKey::Error("40P01".into()), msg: String::new() });
        b.push(Event::Completed { step: 6, key: OutcomeKey::Command("COMMIT".into()), msg: String::new() });
        b.push(Event::Unblocked { step: 4, after: 6, key: OutcomeKey::Command("UPDATE 1".into()), msg: String::new() });
        b.push(Event::Completed { step: 7, key: OutcomeKey::Command("ROLLBACK".into()), msg: String::new() });
        b.push(Event::Probe { label: "ssi_t-sum", key: OutcomeKey::Rows(vec![20, 20], vec![vec![Some("2".into()), Some("22".into())]]) });
        let d = compare(&sc, &a, &b);
        assert!(d.is_empty(), "{d:?}");
        // But a final-state mismatch under the same shape skew IS caught.
        if let Some(Event::Probe { key, .. }) = b.last_mut() {
            *key = OutcomeKey::Rows(vec![20, 20], vec![vec![Some("2".into()), Some("23".into())]]);
        }
        let d = compare(&sc, &a, &b);
        assert!(d.iter().any(|x| x.class == "PROBE"), "{d:?}");
        // And an ungrouped step blocking on only one side is SCHEDULE.
        let mut c = a.clone();
        c[2] = Event::Blocked { step: 2, detect: BlockDetect::Monitor };
        c.push(Event::Unblocked { step: 2, after: 7, key: OutcomeKey::Command("UPDATE 1".into()), msg: String::new() });
        let d = compare(&sc, &a, &c);
        assert!(d.iter().any(|x| x.class == "SCHEDULE"), "{d:?}");
    }

    #[test]
    fn outcome_key_excludes_error_message() {
        let a = StmtOutcome::Error { sqlstate: "40001".into(), message: "pid 123".into() };
        let b = StmtOutcome::Error { sqlstate: "40001".into(), message: "pid 456".into() };
        assert_eq!(outcome_key(&a), outcome_key(&b));
    }
}
