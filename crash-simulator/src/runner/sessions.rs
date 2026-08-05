//! H8 multi-session estate: thread-per-worker sessions + the lazy pool.
//!
//! Design constraints, in order:
//!   * DETERMINISM: the interleaving schedule is PLAN BYTES (`-- SESSION`
//!     steps), never runtime scheduling. Worker threads exist only because a
//!     blocked statement must not block the plan walker; every ordering
//!     decision is serialized in the plan.
//!   * The primary pair (session 0) is untouched: plans without session
//!     steps execute byte-identically to pre-H8 (the pool spawns nothing).
//!   * Blocked-statement hygiene: a worker whose statement is still blocked
//!     at teardown gets its backend CANCELLED (pg_cancel via the wire
//!     CancelToken) before the thread is joined — otherwise the next seed's
//!     `DROP SCHEMA ... CASCADE` blocks behind the orphaned lock holder
//!     (the specconflict-e2e wedge class).
//!   * Wait gates are OBSERVABLE-STATE polls (`WaitUntil` on pg_locks etc.),
//!     never fixed sleeps — the isolation-tester lesson.

use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use super::driver::{ExecOutcome, PgSession, Session};

/// SIM-CONVERGE inc-3: what the dispatcher needs from a pooled worker —
/// synchronous execution (the `Session` supertrait) plus the async
/// dispatch/join pair. Two implementors: the live thread-backed
/// [`WorkerSession`] and the sim bridge's replay twin
/// (`runner::simbridge::ReplayWorker`), which serves recorded outcomes so
/// `execute_plan` can walk v2 multi-session plans NATIVELY over sim
/// artifacts.
pub trait PoolSession: Session {
    /// Dispatch without waiting — the statement is expected to block.
    fn dispatch_async(&mut self, sql: &str) -> Result<(), String>;
    /// Collect the outstanding async statement (bounded).
    fn join_pending(&mut self) -> ExecOutcome;
    /// Explicit supertrait view (avoids relying on dyn upcasting).
    fn as_session(&mut self) -> &mut dyn Session;
}

/// Everything the pool needs to mint a worker connection. Mirrors the
/// primary pair's connect discipline (same session_setup, same SET replay).
#[derive(Debug, Clone)]
pub struct SessionPoolConfig {
    pub dut_conninfo: String,
    pub cpg_conninfo: Option<String>,
    /// Replayed on every fresh worker connection: session_setup plus the
    /// SET-statements of the per-seed reset (search_path — workers must
    /// resolve the same unqualified names as session 0).
    pub session_sql: Vec<String>,
}

enum Cmd {
    Exec(String),
    Quit,
}

enum Resp {
    Token(Option<postgres::CancelToken>),
    Out(ExecOutcome),
}

/// One worker session: a thread owning its own PG connection. Sync execs
/// block the caller (bounded); async execs return immediately and are
/// collected by `join_pending`.
pub struct WorkerSession {
    engine: String,
    tx: mpsc::Sender<Cmd>,
    rx: mpsc::Receiver<Resp>,
    handle: Option<JoinHandle<()>>,
    cancel: Option<postgres::CancelToken>,
    pending: bool,
}

/// Sync statements can sit behind lock waits bounded by statement_timeout;
/// 30s is generous headroom over the harness's 5s statement_timeout.
const SYNC_EXEC_TIMEOUT: Duration = Duration::from_secs(30);

/// Async joins: the blocked statement resolves either by choreography
/// (release) or by statement_timeout (5s) — 15s covers both plus slack.
pub const JOIN_TIMEOUT: Duration = Duration::from_secs(15);

impl WorkerSession {
    pub fn spawn(engine: &str, conninfo: &str, setup: &[String]) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel::<Cmd>();
        let (out_tx, out_rx) = mpsc::channel::<Resp>();
        let eng = engine.to_string();
        let ci = conninfo.to_string();
        let su = setup.to_vec();
        let handle = std::thread::spawn(move || {
            // Connect INSIDE the thread: a dead server surfaces as a
            // ConnectionLost outcome on the first exec (a classified
            // finding), never a harness panic.
            let mut sess = PgSession::connect(&eng, &ci, &su).ok();
            let token = sess.as_ref().and_then(|s| s.cancel_token());
            if out_tx.send(Resp::Token(token)).is_err() {
                return;
            }
            while let Ok(cmd) = cmd_rx.recv() {
                match cmd {
                    Cmd::Exec(sql) => {
                        let out = match sess.as_mut() {
                            Some(s) => s.execute(&sql),
                            None => ExecOutcome::ConnectionLost {
                                message: "worker connect failed".into(),
                            },
                        };
                        if out_tx.send(Resp::Out(out)).is_err() {
                            break;
                        }
                    }
                    Cmd::Quit => break,
                }
            }
        });
        let mut w = WorkerSession {
            engine: engine.to_string(),
            tx: cmd_tx,
            rx: out_rx,
            handle: Some(handle),
            cancel: None,
            pending: false,
        };
        // First message is always the cancel token (None on connect failure).
        if let Ok(Resp::Token(t)) = w.rx.recv_timeout(SYNC_EXEC_TIMEOUT) {
            w.cancel = t;
        }
        w
    }

    pub fn has_pending(&self) -> bool {
        self.pending
    }

    fn cancel_backend(&self) {
        if let Some(tok) = &self.cancel {
            let _ = tok.cancel_query(postgres::NoTls);
        }
    }
}

impl PoolSession for WorkerSession {
    /// Dispatch without waiting — the statement is expected to block.
    fn dispatch_async(&mut self, sql: &str) -> Result<(), String> {
        if self.pending {
            return Err(format!(
                "{}: async dispatch while a statement is outstanding",
                self.engine
            ));
        }
        self.tx
            .send(Cmd::Exec(sql.to_string()))
            .map_err(|_| format!("{}: worker thread gone", self.engine))?;
        self.pending = true;
        Ok(())
    }

    /// Collect the outstanding async statement (bounded).
    fn join_pending(&mut self) -> ExecOutcome {
        if !self.pending {
            // "client:" prefix => harness-fetch (P2) territory: a join
            // without an outstanding statement is a plan-construction bug,
            // not a dead server.
            return ExecOutcome::ConnectionLost {
                message: "client: join without outstanding async statement".into(),
            };
        }
        match self.rx.recv_timeout(JOIN_TIMEOUT) {
            Ok(Resp::Out(out)) => {
                self.pending = false;
                out
            }
            Ok(Resp::Token(_)) => ExecOutcome::ConnectionLost {
                message: "client: worker protocol desync".into(),
            },
            Err(mpsc::RecvTimeoutError::Timeout) => ExecOutcome::ConnectionLost {
                // Still blocked after statement_timeout headroom: cancel so
                // teardown and the next seed never wedge, report as a
                // client-side fetch failure (P2) — the wait-timeout P1 on
                // the choreography's gate is the loud detector, not this.
                message: {
                    self.cancel_backend();
                    "client: join timeout (statement still blocked; backend cancelled)".into()
                },
            },
            Err(mpsc::RecvTimeoutError::Disconnected) => ExecOutcome::ConnectionLost {
                message: "worker thread died".into(),
            },
        }
    }

    fn as_session(&mut self) -> &mut dyn Session {
        self
    }
}

impl Session for WorkerSession {
    fn engine(&self) -> &str {
        &self.engine
    }

    fn execute(&mut self, sql: &str) -> ExecOutcome {
        if self.pending {
            // A sync statement while an async one is outstanding would
            // interleave two statements on one connection: refuse loudly.
            return ExecOutcome::ConnectionLost {
                message: "client: sync exec while async statement outstanding".into(),
            };
        }
        if self.tx.send(Cmd::Exec(sql.to_string())).is_err() {
            return ExecOutcome::ConnectionLost { message: "worker thread died".into() };
        }
        match self.rx.recv_timeout(SYNC_EXEC_TIMEOUT) {
            Ok(Resp::Out(out)) => out,
            Ok(Resp::Token(_)) => {
                ExecOutcome::ConnectionLost { message: "client: worker protocol desync".into() }
            }
            Err(_) => {
                self.cancel_backend();
                ExecOutcome::ConnectionLost { message: "client: worker exec timeout".into() }
            }
        }
    }

    fn reconnect(&mut self) -> Result<(), String> {
        Err("worker sessions do not reconnect (faults are session-0 only)".into())
    }
}

impl Drop for WorkerSession {
    fn drop(&mut self) {
        if self.pending {
            // Never leave a blocked backend holding locks into the next
            // seed (the DROP SCHEMA CASCADE wedge).
            self.cancel_backend();
            // Give the cancel a moment to resolve so the thread can exit.
            let _ = self.rx.recv_timeout(Duration::from_secs(5));
        }
        let _ = self.tx.send(Cmd::Quit);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Lazy worker pool, one slot per (leg, session id >= 1). Slot 0 is the
/// primary pair and never lives here.
///
/// SIM-CONVERGE inc-3: the pool now holds `dyn PoolSession` workers so a
/// PREPARED pool (the sim bridge's replay twin — `ReplayWorker`s built from
/// per-session sim artifacts) can stand in for the live thread-backed
/// workers. Prepared pools never spawn: `ensure` only verifies presence.
pub struct SessionPool {
    cfg: Option<SessionPoolConfig>,
    prepared: bool,
    dut: Vec<Option<Box<dyn PoolSession>>>,
    cpg: Vec<Option<Box<dyn PoolSession>>>,
}

impl SessionPool {
    pub fn new(cfg: Option<SessionPoolConfig>) -> Self {
        SessionPool { cfg, prepared: false, dut: Vec::new(), cpg: Vec::new() }
    }

    /// SIM-CONVERGE inc-3: a pre-populated replay pool — index 0 = session
    /// id 1 (the primary pair never lives here). No cpg legs (diff-c is N/A
    /// inside the sim).
    pub fn prepared(dut: Vec<Option<Box<dyn PoolSession>>>) -> Self {
        SessionPool { cfg: None, prepared: true, dut, cpg: Vec::new() }
    }

    /// Spawn (if needed) the workers for session `id` (>= 1). `diff` mirrors
    /// whether the primary pair runs diff-c. Errors only on missing config
    /// (harness routing bug); connect failures surface as outcomes. On a
    /// PREPARED pool this only verifies the session was provisioned.
    pub fn ensure(&mut self, id: u32, diff: bool) -> Result<(), String> {
        let ix = (id - 1) as usize;
        if self.prepared {
            return if self.dut.get(ix).map(|w| w.is_some()).unwrap_or(false) {
                Ok(())
            } else {
                Err(format!("replay pool has no session {id} (plan uses an unprovisioned id)"))
            };
        }
        let cfg = self
            .cfg
            .as_ref()
            .ok_or("session pool not configured (routing bug: v2 plan without pool config)")?;
        while self.dut.len() <= ix {
            self.dut.push(None);
            self.cpg.push(None);
        }
        if self.dut[ix].is_none() {
            self.dut[ix] = Some(Box::new(WorkerSession::spawn(
                &format!("pgrust-s{id}"),
                &cfg.dut_conninfo,
                &cfg.session_sql,
            )));
        }
        if diff && self.cpg[ix].is_none() {
            if let Some(ci) = &cfg.cpg_conninfo {
                self.cpg[ix] = Some(Box::new(WorkerSession::spawn(
                    &format!("cpg-s{id}"),
                    ci,
                    &cfg.session_sql,
                )));
            }
        }
        Ok(())
    }

    /// Both legs of session `id` (>= 1); `ensure` must have run.
    #[allow(clippy::type_complexity)]
    pub fn pair(&mut self, id: u32) -> (&mut dyn PoolSession, Option<&mut dyn PoolSession>) {
        let ix = (id - 1) as usize;
        let dut: &mut dyn PoolSession = self.dut[ix].as_mut().expect("ensure() ran").as_mut();
        let cpg = match self.cpg.get_mut(ix) {
            Some(Some(w)) => Some(w.as_mut() as &mut dyn PoolSession),
            _ => None,
        };
        (dut, cpg)
    }

    pub fn dut_of(&mut self, id: u32) -> Option<&mut dyn PoolSession> {
        match self.dut.get_mut((id - 1) as usize) {
            Some(Some(w)) => Some(w.as_mut()),
            _ => None,
        }
    }

    pub fn cpg_of(&mut self, id: u32) -> Option<&mut dyn PoolSession> {
        match self.cpg.get_mut((id - 1) as usize) {
            Some(Some(w)) => Some(w.as_mut()),
            _ => None,
        }
    }
}
