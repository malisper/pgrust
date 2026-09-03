//! Symmetric crash-with-restart supervisor and hang ladder (sitediff plan
//! §4.3, lane L0.3).
//!
//! One `Supervisor` per side (A = C 18.6, B = pgrust). It is a pure state
//! machine over `Event`s — log lines from the tailer, process exit from
//! `waitpid`, lost connections, elapsed time — that emits `Action`s for
//! the runner to carry out (bank the log tail, restart the server on the
//! same datadir, reconnect the pools, bump the generation). Nothing in
//! here spawns, kills or sleeps; `OsDriver` at the bottom is the only
//! place that touches processes, and it is not exercised by tests.
//!
//! Death detection differs per side and both are covered:
//! * B is one process (thread-model backends): a backend crash takes the
//!   server down, so `ProcessExited` from waitpid is the primary witness,
//!   with the postmaster's `terminated by signal` / `reinitializing` lines
//!   and pgrust's own `pgrust: FATAL: server process was terminated by
//!   signal` crash-handler line as log witnesses (the process may still
//!   be reaping when the line lands).
//! * A is a real postmaster: a backend SIGKILL (the hang ladder's last
//!   rung) prints `server process (PID n) was terminated by signal 9` +
//!   `all server processes terminated; reinitializing` and then recovers
//!   in place (`redo starts at` + `ready to accept connections`) — the
//!   postmaster is alive, no restart command is needed, but every pool
//!   connection is gone and the generation still bumps.
//!
//! The `Crash` record (`contracts::Crash`) is built from the supervisor's
//! ring of last log lines and the signal it saw. After recovery the
//! stream resumes under `liveness = post-crash` until the next probe
//! deck confirms both sides agree again (the runner decides that; the
//! supervisor only reports `post_crash()`).

use std::collections::VecDeque;

use crate::contracts::{Bytes, Cell, Crash, Hang, Side};
use crate::logtail::{signal_name, witness_of, Witness};

/// Per-step deadline default (plan §4.3: 20 s, cell-scaled).
pub const DEFAULT_DEADLINE_MS: u64 = 20_000;

/// Grace after each ladder rung before the next escalation.
pub const LADDER_GRACE_MS: u64 = 5_000;

/// Last log lines kept for the `crash.log_tail` bank.
pub const LOG_TAIL_LINES: usize = 40;

/// The per-step deadline for a cell: 20 s scaled by what makes a step
/// slower on that cell. Deterministic in the cell, so the same cell
/// always hangs at the same budget.
pub fn deadline_ms_for_cell(cell: &Cell) -> u64 {
    let mut scale: u64 = 10; // tenths
    scale = match cell.b_build.as_str() {
        "dev" => scale * 20 / 10,
        "dev-server" => scale * 15 / 10,
        _ => scale,
    };
    if cell.oracle_variants.iter().any(|v| v == "gcov") {
        scale = scale * 15 / 10;
    }
    if cell.logging_min_messages.starts_with("debug") {
        scale = scale * 15 / 10;
    }
    if cell.faults == "antithesis" {
        scale *= 3;
    }
    DEFAULT_DEADLINE_MS * scale / 10
}

/// What the runner feeds the supervisor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// One server-log line (raw text; the prefix is fine, the witness
    /// texts are searched).
    Log(String),
    /// The server process reaped (`waitpid`): exit code or signal.
    ProcessExited { code: Option<i32>, signal: Option<i32> },
    /// A pool connection died mid-step (I/O error / EOF).
    ConnLost { session: String, detail: String },
    /// The runner issued the restart command for a dead server.
    RestartIssued,
    /// The runner reconnected every pool session after recovery.
    PoolsReconnected,
}

/// What the runner must do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Bank the last log lines and the last statement per session.
    BankLogTail,
    /// Spawn the server again on the same datadir (the process is gone).
    Restart,
    /// The server is accepting connections again: reconnect every pool
    /// session (symmetrically — the other side reconnects too).
    ReconnectPools,
    /// The stream resumes under this generation, `post-crash` confidence.
    Generation(u32),
}

/// Health of one side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum State {
    Healthy,
    /// Death witnessed; `process_gone` = a restart command is required
    /// (B, or A's postmaster itself); otherwise A recovers in place.
    Dead { process_gone: bool },
    /// Restart issued (or in-place reinitialization observed); waiting for
    /// the `redo starts at` witness.
    Restarting,
    /// Redo seen; waiting for `ready to accept connections`.
    Recovering,
    /// Ready; waiting for the runner to reconnect the pools.
    Ready,
}

/// One banked crash (the supervisor keeps every one for the run report).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CrashEvent {
    pub generation_before: u32,
    pub signal: Option<String>,
    pub backend_pid: Option<u32>,
    pub log_tail: Vec<Bytes>,
    pub redo_lsn: Option<String>,
    /// Last statement per session at the time of death.
    pub last_statements: Vec<(String, String)>,
}

#[derive(Clone, Debug)]
pub struct Supervisor {
    side: Side,
    state: State,
    generation: u32,
    post_crash: bool,
    tail: VecDeque<Bytes>,
    crashes: Vec<CrashEvent>,
    /// Signal text from the first death witness of the current crash.
    pending_signal: Option<String>,
    pending_pid: Option<u32>,
    pending_redo: Option<String>,
    last_statements: Vec<(String, String)>,
    /// `ready to accept` seen before the runner asked for a restart (the
    /// server recovered in place): no Restart action, straight to Ready.
    in_place: bool,
}

impl Supervisor {
    pub fn new(side: Side) -> Supervisor {
        Supervisor {
            side,
            state: State::Healthy,
            generation: 1,
            post_crash: false,
            tail: VecDeque::with_capacity(LOG_TAIL_LINES),
            crashes: Vec::new(),
            pending_signal: None,
            pending_pid: None,
            pending_redo: None,
            last_statements: Vec::new(),
            in_place: false,
        }
    }

    pub fn side(&self) -> Side {
        self.side
    }

    pub fn state(&self) -> &State {
        &self.state
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// True after a recovery until `clear_post_crash` (the runner clears
    /// it when a full probe deck matched on both sides).
    pub fn post_crash(&self) -> bool {
        self.post_crash
    }

    pub fn clear_post_crash(&mut self) {
        self.post_crash = false;
    }

    pub fn crashes(&self) -> &[CrashEvent] {
        &self.crashes
    }

    /// Ready for steps: healthy (or ready-and-reconnected).
    pub fn is_serving(&self) -> bool {
        self.state == State::Healthy
    }

    /// Record the statement a session just issued (banked on death).
    pub fn note_statement(&mut self, session: &str, sql: &str) {
        match self.last_statements.iter_mut().find(|(s, _)| s == session) {
            Some(slot) => slot.1 = sql.to_string(),
            None => self.last_statements.push((session.to_string(), sql.to_string())),
        }
    }

    /// The `contracts::Crash` for the most recent crash, if any.
    pub fn crash_record(&self) -> Option<Crash> {
        let c = self.crashes.last()?;
        Some(Crash {
            side: self.side,
            generation: self.generation,
            signal: c.signal.clone(),
            log_tail: c.log_tail.clone(),
        })
    }

    fn push_tail(&mut self, line: &str) {
        if self.tail.len() == LOG_TAIL_LINES {
            self.tail.pop_front();
        }
        self.tail.push_back(Bytes::text(line));
    }

    fn bank_crash(&mut self) -> Action {
        self.crashes.push(CrashEvent {
            generation_before: self.generation,
            signal: self.pending_signal.clone(),
            backend_pid: self.pending_pid,
            log_tail: self.tail.iter().cloned().collect(),
            redo_lsn: None,
            last_statements: self.last_statements.clone(),
        });
        Action::BankLogTail
    }

    /// Drive the state machine with one event.
    pub fn observe(&mut self, ev: Event) -> Vec<Action> {
        let mut actions = Vec::new();
        match ev {
            Event::Log(line) => {
                self.push_tail(&line);
                // Between death and recovery every line is part of the
                // crash's witness (reinitializing, recovery, redo, ready):
                // extend the banked tail, bounded.
                if self.state != State::Healthy {
                    if let Some(c) = self.crashes.last_mut() {
                        if c.log_tail.len() < 2 * LOG_TAIL_LINES {
                            c.log_tail.push(Bytes::text(&line));
                        }
                    }
                }
                let Some(w) = witness_of(&line) else { return actions };
                match (&self.state, w) {
                    (State::Healthy, Witness::TerminatedBySignal { pid, signal }) => {
                        self.pending_signal = Some(signal_name(&signal));
                        self.pending_pid = pid;
                        // A backend died. B: the process is going away (the
                        // waitpid event confirms); A: the postmaster will
                        // reinitialize in place.
                        self.state = State::Dead { process_gone: self.side == Side::B };
                        actions.push(self.bank_crash());
                        if self.side == Side::B {
                            actions.push(Action::Restart);
                        }
                    }
                    (State::Healthy, Witness::CrashHandler(signal)) => {
                        self.pending_signal = Some(signal_name(&signal));
                        self.state = State::Dead { process_gone: true };
                        actions.push(self.bank_crash());
                        actions.push(Action::Restart);
                    }
                    (State::Healthy, Witness::Reinitializing) => {
                        // Reinitializing without a preceding signal line
                        // (the line was lost or the death was an exit
                        // code): still a crash.
                        self.state = State::Dead { process_gone: false };
                        actions.push(self.bank_crash());
                        self.state = State::Restarting;
                        self.in_place = true;
                    }
                    (State::Dead { process_gone: false }, Witness::Reinitializing) => {
                        self.state = State::Restarting;
                        self.in_place = true;
                    }
                    (State::Dead { .. } | State::Restarting, Witness::RecoveryInProgress) => {
                        self.state = State::Restarting;
                    }
                    (State::Dead { .. } | State::Restarting, Witness::RedoStartsAt(lsn)) => {
                        self.pending_redo = Some(lsn.clone());
                        if let Some(c) = self.crashes.last_mut() {
                            c.redo_lsn = Some(lsn);
                        }
                        self.state = State::Recovering;
                    }
                    (State::Recovering | State::Restarting | State::Dead { .. }, Witness::ReadyToAccept) => {
                        self.state = State::Ready;
                        actions.push(Action::ReconnectPools);
                    }
                    (State::Healthy, Witness::ShutDown) => {
                        // A clean shutdown while serving is a death without
                        // a signal (e.g. `env:restart` handled elsewhere,
                        // or an unexpected exit).
                        self.state = State::Dead { process_gone: true };
                        actions.push(self.bank_crash());
                        actions.push(Action::Restart);
                    }
                    _ => {}
                }
            }
            Event::ProcessExited { code, signal } => {
                match self.state {
                    State::Healthy => {
                        self.pending_signal = signal.map(|s| signal_name(&s.to_string())).or_else(|| {
                            code.map(|c| format!("exit {c}"))
                        });
                        self.state = State::Dead { process_gone: true };
                        actions.push(self.bank_crash());
                        actions.push(Action::Restart);
                    }
                    State::Dead { process_gone: false } => {
                        // A's postmaster itself is gone after all.
                        self.state = State::Dead { process_gone: true };
                        actions.push(Action::Restart);
                    }
                    State::Dead { process_gone: true } => {
                        // waitpid confirmed what the log already said.
                    }
                    State::Restarting | State::Recovering | State::Ready => {
                        // Died again during recovery: bank and restart once
                        // more (a crash loop is bounded by the runner).
                        self.pending_signal = signal.map(|s| signal_name(&s.to_string()));
                        self.state = State::Dead { process_gone: true };
                        actions.push(self.bank_crash());
                        actions.push(Action::Restart);
                    }
                }
            }
            Event::ConnLost { session, detail } => {
                // A lost connection alone is not a crash (FATAL closes a
                // session too); it is remembered on the last-statement bank
                // so a later death witness carries it.
                self.note_statement(&session, &format!("<conn lost: {detail}>"));
            }
            Event::RestartIssued => {
                if matches!(self.state, State::Dead { .. }) {
                    self.state = State::Restarting;
                    self.in_place = false;
                }
            }
            Event::PoolsReconnected => {
                if self.state == State::Ready {
                    self.generation += 1;
                    self.post_crash = true;
                    self.state = State::Healthy;
                    self.in_place = false;
                    self.pending_signal = None;
                    self.pending_pid = None;
                    self.pending_redo = None;
                    actions.push(Action::Generation(self.generation));
                }
            }
        }
        actions
    }

    /// The redo LSN of the most recent recovery (the restart witness).
    pub fn redo_witness(&self) -> Option<&str> {
        self.crashes.last().and_then(|c| c.redo_lsn.as_deref())
    }
}

// ---------------------------------------------------------------------
// Hang ladder
// ---------------------------------------------------------------------

/// Escalation rung reached on a hung step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rung {
    /// `SELECT pg_cancel_backend(pid)` from the monitor session.
    Cancel,
    /// `SELECT pg_terminate_backend(pid)`.
    Terminate,
    /// SIGKILL of the OS process: A's backend by pid (the postmaster
    /// reinitializes; the supervisor absorbs it), B's whole server.
    SigKill,
}

impl Rung {
    pub fn as_str(self) -> &'static str {
        match self {
            Rung::Cancel => "cancel",
            Rung::Terminate => "terminate",
            Rung::SigKill => "sigkill",
        }
    }

    /// The monitor-session SQL for the first two rungs.
    pub fn sql(self, backend_pid: u32) -> Option<String> {
        match self {
            Rung::Cancel => Some(format!("SELECT pg_cancel_backend({backend_pid});")),
            Rung::Terminate => Some(format!("SELECT pg_terminate_backend({backend_pid});")),
            Rung::SigKill => None,
        }
    }
}

/// Per-step deadline tracker. `elapsed(ms)` is fed by the caller (no
/// clock in here); it returns the rung to fire when a threshold is
/// crossed, at most once per rung, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HangLadder {
    deadline_ms: u64,
    grace_ms: u64,
    reached: Option<Rung>,
}

impl HangLadder {
    pub fn new(deadline_ms: u64, grace_ms: u64) -> HangLadder {
        HangLadder { deadline_ms, grace_ms, reached: None }
    }

    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    /// Highest rung fired so far.
    pub fn reached(&self) -> Option<Rung> {
        self.reached
    }

    /// Feed the elapsed time of the in-flight step. Returns the next rung
    /// to fire, if the elapsed time crossed its threshold.
    pub fn elapsed(&mut self, ms: u64) -> Option<Rung> {
        let next = match self.reached {
            None => (Rung::Cancel, self.deadline_ms),
            Some(Rung::Cancel) => (Rung::Terminate, self.deadline_ms + self.grace_ms),
            Some(Rung::Terminate) => (Rung::SigKill, self.deadline_ms + 2 * self.grace_ms),
            Some(Rung::SigKill) => return None,
        };
        if ms >= next.1 {
            self.reached = Some(next.0);
            Some(next.0)
        } else {
            None
        }
    }

    /// The `contracts::Hang` for a step that hit the deadline.
    pub fn hang_record(&self) -> Option<Hang> {
        self.reached.map(|r| Hang { ms: self.deadline_ms, ladder: Some(r.as_str().to_string()) })
    }

    /// Fresh ladder for the next step.
    pub fn reset(&mut self) {
        self.reached = None;
    }
}

/// Run every rung against a step that never returns: the sequence of
/// rungs fired for a monotone elapsed-time series.
pub fn ladder_trace(deadline_ms: u64, grace_ms: u64, samples: &[u64]) -> Vec<Rung> {
    let mut l = HangLadder::new(deadline_ms, grace_ms);
    samples.iter().filter_map(|ms| l.elapsed(*ms)).collect()
}

// ---------------------------------------------------------------------
// OS driver (the only process-touching code; not exercised by tests)
// ---------------------------------------------------------------------

/// How to restart one side on its datadir.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerSpec {
    pub side: Side,
    pub binary: String,
    pub datadir: String,
    pub socket_dir: String,
    pub port: u16,
    pub flags: Vec<String>,
    /// Log file the server's stderr appends to (the tailer keeps reading it).
    pub log_path: String,
    pub env: Vec<(String, String)>,
}

impl ServerSpec {
    /// The restart command line (what `sitediff-cell.sh up` ran, again).
    pub fn command(&self) -> std::process::Command {
        let mut c = std::process::Command::new(&self.binary);
        c.arg("-D").arg(&self.datadir).arg("-k").arg(&self.socket_dir).arg("-p").arg(self.port.to_string());
        for f in &self.flags {
            c.arg(f);
        }
        for (k, v) in &self.env {
            c.env(k, v);
        }
        c
    }

    /// Render the command for logs/dry runs.
    pub fn command_line(&self) -> String {
        let mut parts = vec![self.binary.clone(), "-D".into(), self.datadir.clone(), "-k".into(), self.socket_dir.clone(), "-p".into(), self.port.to_string()];
        parts.extend(self.flags.iter().cloned());
        parts.join(" ")
    }
}

#[cfg(unix)]
extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
    fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
}

/// Process-level operations for a side. Everything returns plain data so
/// the runner feeds it back as `Event`s.
pub struct OsDriver {
    pub spec: ServerSpec,
    child: Option<std::process::Child>,
    /// Pid recorded in `cell.env` (the process `sitediff-cell.sh` spawned)
    /// when this runner did not spawn the server itself.
    external_pid: Option<i32>,
}

impl OsDriver {
    pub fn attached(spec: ServerSpec, external_pid: Option<i32>) -> OsDriver {
        OsDriver { spec, child: None, external_pid }
    }

    pub fn pid(&self) -> Option<i32> {
        self.child.as_ref().map(|c| c.id() as i32).or(self.external_pid)
    }

    /// Non-blocking reap of the server process. `Some(event)` when it
    /// exited since the last poll.
    #[cfg(unix)]
    pub fn poll_exit(&mut self) -> Option<Event> {
        if let Some(child) = self.child.as_mut() {
            return match child.try_wait() {
                Ok(Some(status)) => {
                    use std::os::unix::process::ExitStatusExt;
                    self.child = None;
                    Some(Event::ProcessExited { code: status.code(), signal: status.signal() })
                }
                _ => None,
            };
        }
        let pid = self.external_pid?;
        // Not our child: waitpid fails; probe liveness with signal 0.
        let mut status = 0i32;
        // SAFETY: plain libc calls with valid arguments; WNOHANG = 1.
        let r = unsafe { waitpid(pid, &mut status as *mut i32, 1) };
        if r == pid {
            self.external_pid = None;
            let sig = status & 0x7f;
            return Some(Event::ProcessExited {
                code: if sig == 0 { Some((status >> 8) & 0xff) } else { None },
                signal: if sig != 0 { Some(sig) } else { None },
            });
        }
        let alive = unsafe { kill(pid, 0) } == 0;
        if alive {
            None
        } else {
            self.external_pid = None;
            Some(Event::ProcessExited { code: None, signal: None })
        }
    }

    #[cfg(not(unix))]
    pub fn poll_exit(&mut self) -> Option<Event> {
        None
    }

    /// Spawn the server again on the same datadir, stderr appended to the
    /// side's log file.
    pub fn restart(&mut self) -> Result<(), String> {
        let log = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.spec.log_path)
            .map_err(|e| format!("open {}: {e}", self.spec.log_path))?;
        let log2 = log.try_clone().map_err(|e| e.to_string())?;
        let mut cmd = self.spec.command();
        cmd.stdout(log).stderr(log2).stdin(std::process::Stdio::null());
        let child = cmd.spawn().map_err(|e| format!("spawn {}: {e}", self.spec.command_line()))?;
        self.child = Some(child);
        self.external_pid = None;
        Ok(())
    }

    /// SIGKILL a pid (A's hung backend, or B's whole server).
    #[cfg(unix)]
    pub fn sigkill(pid: i32) -> bool {
        unsafe { kill(pid, 9) == 0 }
    }

    #[cfg(not(unix))]
    pub fn sigkill(_pid: i32) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(s: &str) -> Event {
        Event::Log(s.to_string())
    }

    #[test]
    fn crash_on_b_restart_generation_bump() {
        let mut s = Supervisor::new(Side::B);
        s.note_statement("s2", "ANALYZE t");
        assert!(s.is_serving());
        assert_eq!(s.observe(log("2026-09-02 10:20:41.902 PDT client backend[41233] [unknown] LOG:  statement: ANALYZE t")), vec![]);
        let a = s.observe(log("2026-09-02 10:20:41.902 PDT postmaster[41200] LOG:  server process (PID 41233) was terminated by signal 6: Abort trap"));
        assert_eq!(a, vec![Action::BankLogTail, Action::Restart]);
        assert_eq!(*s.state(), State::Dead { process_gone: true });
        // waitpid confirms; no second restart.
        assert_eq!(s.observe(Event::ProcessExited { code: None, signal: Some(6) }), vec![]);
        assert_eq!(s.observe(log("2026-09-02 10:20:41.903 PDT postmaster[41200] LOG:  all server processes terminated; reinitializing")), vec![]);
        assert_eq!(s.observe(Event::RestartIssued), vec![]);
        assert_eq!(*s.state(), State::Restarting);
        assert_eq!(s.observe(log("LOG:  database system was not properly shut down; automatic recovery in progress")), vec![]);
        assert_eq!(s.observe(log("LOG:  redo starts at 0/1A2B3C4")), vec![]);
        assert_eq!(*s.state(), State::Recovering);
        assert_eq!(s.observe(log("LOG:  database system is ready to accept connections")), vec![Action::ReconnectPools]);
        assert_eq!(*s.state(), State::Ready);
        assert!(!s.is_serving());
        assert_eq!(s.observe(Event::PoolsReconnected), vec![Action::Generation(2)]);
        assert!(s.is_serving() && s.post_crash());
        assert_eq!(s.generation(), 2);
        assert_eq!(s.redo_witness(), Some("0/1A2B3C4"));
        let c = s.crash_record().unwrap();
        assert_eq!(c.side, Side::B);
        assert_eq!(c.generation, 2);
        assert_eq!(c.signal.as_deref(), Some("SIGABRT"));
        assert_eq!(c.log_tail.len(), 6, "tail banked at death time + the recovery witness lines");
        assert!(String::from_utf8_lossy(&c.log_tail[5].0).contains("ready to accept"));
        assert_eq!(s.crashes()[0].last_statements, vec![("s2".to_string(), "ANALYZE t".to_string())]);
        assert_eq!(s.crashes()[0].backend_pid, Some(41233));
        s.clear_post_crash();
        assert!(!s.post_crash());
    }

    #[test]
    fn crash_on_a_recovers_in_place_without_restart() {
        // The ladder's SIGKILL of a C backend: the postmaster survives.
        let mut s = Supervisor::new(Side::A);
        let a = s.observe(log("2026-09-02 10:20:41.902 PDT postmaster[100] LOG:  server process (PID 120) was terminated by signal 9: Killed"));
        assert_eq!(a, vec![Action::BankLogTail]);
        assert_eq!(*s.state(), State::Dead { process_gone: false });
        assert_eq!(s.observe(log("2026-09-02 10:20:41.903 PDT postmaster[100] LOG:  all server processes terminated; reinitializing")), vec![]);
        assert_eq!(*s.state(), State::Restarting);
        assert_eq!(s.observe(log("2026-09-02 10:20:42.000 PDT startup[130] LOG:  redo starts at 0/2000028")), vec![]);
        assert_eq!(s.observe(log("2026-09-02 10:20:42.100 PDT postmaster[100] LOG:  database system is ready to accept connections")), vec![Action::ReconnectPools]);
        assert_eq!(s.observe(Event::PoolsReconnected), vec![Action::Generation(2)]);
        assert_eq!(s.crash_record().unwrap().signal.as_deref(), Some("SIGKILL"));
        assert!(s.post_crash());
    }

    #[test]
    fn a_postmaster_death_needs_restart() {
        let mut s = Supervisor::new(Side::A);
        assert_eq!(
            s.observe(Event::ProcessExited { code: None, signal: Some(11) }),
            vec![Action::BankLogTail, Action::Restart]
        );
        assert_eq!(s.crash_record().unwrap().signal.as_deref(), Some("SIGSEGV"));
        assert_eq!(s.observe(Event::RestartIssued), vec![]);
        assert_eq!(s.observe(log("LOG:  redo starts at 0/3")), vec![]);
        assert_eq!(s.observe(log("LOG:  database system is ready to accept connections")), vec![Action::ReconnectPools]);
        assert_eq!(s.observe(Event::PoolsReconnected), vec![Action::Generation(2)]);
    }

    #[test]
    fn pgrust_crash_handler_line_is_a_death() {
        let mut s = Supervisor::new(Side::B);
        let a = s.observe(log("pgrust: FATAL: server process was terminated by signal 11"));
        assert_eq!(a, vec![Action::BankLogTail, Action::Restart]);
        assert_eq!(s.crash_record().unwrap().signal.as_deref(), Some("SIGSEGV"));
    }

    #[test]
    fn death_during_recovery_banks_again_and_generation_counts_recoveries() {
        let mut s = Supervisor::new(Side::B);
        s.observe(Event::ProcessExited { code: Some(1), signal: None });
        assert_eq!(s.crashes()[0].signal.as_deref(), Some("exit 1"));
        s.observe(Event::RestartIssued);
        let a = s.observe(Event::ProcessExited { code: None, signal: Some(6) });
        assert_eq!(a, vec![Action::BankLogTail, Action::Restart]);
        assert_eq!(s.crashes().len(), 2);
        s.observe(Event::RestartIssued);
        s.observe(log("LOG:  database system is ready to accept connections"));
        s.observe(Event::PoolsReconnected);
        assert_eq!(s.generation(), 2, "one generation per completed recovery");
    }

    #[test]
    fn conn_lost_alone_is_not_a_crash() {
        let mut s = Supervisor::new(Side::A);
        assert_eq!(s.observe(Event::ConnLost { session: "s1".into(), detail: "EOF".into() }), vec![]);
        assert!(s.is_serving());
        assert!(s.crash_record().is_none());
        assert_eq!(s.observe(Event::PoolsReconnected), vec![], "no-op while healthy");
    }

    #[test]
    fn log_tail_ring_is_bounded() {
        let mut s = Supervisor::new(Side::B);
        for i in 0..100 {
            s.observe(log(&format!("LOG:  line {i}")));
        }
        s.observe(Event::ProcessExited { code: None, signal: Some(9) });
        let tail = &s.crashes()[0].log_tail;
        assert_eq!(tail.len(), LOG_TAIL_LINES);
        assert_eq!(tail[0], Bytes::text("LOG:  line 60"));
    }

    #[test]
    fn hang_ladder_escalates_in_order_once_each() {
        assert_eq!(
            ladder_trace(20_000, 5_000, &[1_000, 19_999, 20_000, 21_000, 24_999, 25_000, 26_000, 30_000, 40_000]),
            vec![Rung::Cancel, Rung::Terminate, Rung::SigKill]
        );
        let mut l = HangLadder::new(20_000, 5_000);
        assert_eq!(l.elapsed(5), None);
        assert!(l.hang_record().is_none());
        assert_eq!(l.elapsed(20_000), Some(Rung::Cancel));
        assert_eq!(l.hang_record(), Some(Hang { ms: 20_000, ladder: Some("cancel".into()) }));
        // A huge jump still fires rungs one at a time per call.
        assert_eq!(l.elapsed(1_000_000), Some(Rung::Terminate));
        assert_eq!(l.elapsed(1_000_000), Some(Rung::SigKill));
        assert_eq!(l.elapsed(1_000_000), None);
        assert_eq!(l.hang_record().unwrap().ladder.as_deref(), Some("sigkill"));
        l.reset();
        assert_eq!(l.reached(), None);
        assert_eq!(Rung::Cancel.sql(77).as_deref(), Some("SELECT pg_cancel_backend(77);"));
        assert_eq!(Rung::Terminate.sql(77).as_deref(), Some("SELECT pg_terminate_backend(77);"));
        assert_eq!(Rung::SigKill.sql(77), None);
    }

    #[test]
    fn deadline_scales_with_the_cell() {
        let mut c = Cell::base();
        assert_eq!(deadline_ms_for_cell(&c), 40_000, "dev build = 2x");
        c.b_build = "release".into();
        assert_eq!(deadline_ms_for_cell(&c), 20_000);
        c.logging_min_messages = "debug1".into();
        assert_eq!(deadline_ms_for_cell(&c), 30_000);
        c.faults = "antithesis".into();
        assert_eq!(deadline_ms_for_cell(&c), 90_000);
    }

    #[test]
    fn server_spec_command_line() {
        let spec = ServerSpec {
            side: Side::A,
            binary: "/opt/pg/bin/postgres".into(),
            datadir: "/w/dda".into(),
            socket_dir: "/w/socka".into(),
            port: 55741,
            flags: vec!["-c".into(), "log_line_prefix=%m %b[%p] %q%a ".into()],
            log_path: "/w/a.log".into(),
            env: vec![],
        };
        assert_eq!(
            spec.command_line(),
            "/opt/pg/bin/postgres -D /w/dda -k /w/socka -p 55741 -c log_line_prefix=%m %b[%p] %q%a "
        );
        let d = OsDriver::attached(spec, Some(4242));
        assert_eq!(d.pid(), Some(4242));
    }
}
