//! PostgresSimNetMain (pgrust extension, `--cfg pgrust_sim` builds ONLY):
//! the P4 sim-net session harness — ONE deterministic pgwire session served
//! over the in-memory sim-net transport pair (pqcomm_simnet), driven by an
//! in-process scripted client at the provider's deterministic block points.
//!
//! The boot ladder + session half are stdio_wire's inner fn, VERBATIM (it is
//! transport-blind; the provider was installed by
//! seams_init::init_all_with_transport(Transport::SimNet) at process start).
//! What this file adds is sim-harness plumbing, all of it cfg(pgrust_sim):
//!
//! 1. SimVfs namespace seeding: under pgrust_sim the whole binary statically
//!    dispatches vfs to SimVfs (P1 §1.2), whose namespace starts EMPTY — the
//!    known COMPOSE FINDING 1 boot wall. The harness mirrors the host
//!    datadir (argv -D) into the SimVfs universe KEYED RELATIVE to the
//!    datadir root (the ladder chdir()s into -D and the port addresses
//!    datadir files relatively; SimVfs resolves relative paths against "/"),
//!    plus PGRUST_SIMNET_SEED_DIRS (colon-separated host dirs, e.g. the
//!    timezone share) keyed at their ABSOLUTE paths. Raw-fs boot pieces
//!    (conf read, lockfile) hit the REAL datadir — same image, both planes.
//! 2. The scripted wire client (PGRUST_SIMNET_SQL: one simple-query
//!    statement per line): StartupMessage -> per ReadyForQuery send the next
//!    Query -> Terminate -> Finished (client write side closed).
//! 3. Artifact dump at session exit: the full server->client wire byte
//!    stream (PGRUST_SIMNET_TRANSCRIPT) and the op-sequence-numbered SimNet
//!    op log (PGRUST_SIMNET_OPLOG). The determinism gate byte-compares both
//!    across two runs of the same script (pid pinned via
//!    init_small::globals::process_id sim arm; cancel key via the seeded P2
//!    RNG; clock via the frozen SimClock).
//!
//! The std::fs/env reads below are sim-harness domain (cfg'd out of product
//! builds; the determinism lint censuses production code only).

use ::types_error::PgResult;

const PROGNAME: &str = "postgres";

// ---------------------------------------------------------------------------
// SimVfs seeding (host image -> this thread's sim universe).
// ---------------------------------------------------------------------------

fn cpath(p: &str) -> std::ffi::CString {
    std::ffi::CString::new(p).expect("no NUL in seed paths")
}

fn sim_mkdir_p(path: &str) {
    let mut acc = String::new();
    for comp in path.split('/').filter(|c| !c.is_empty() && *c != ".") {
        if !acc.is_empty() || path.starts_with('/') {
            acc.push('/');
        }
        // Relative seeds keep their first component bare; SimVfs resolves
        // both shapes against "/".
        if acc.is_empty() && !path.starts_with('/') {
            acc = comp.to_string();
        } else {
            acc.push_str(comp);
        }
        let _ = vfs::mkdir(&cpath(&acc), 0o700);
    }
}

fn sim_write_file(sim_path: &str, bytes: &[u8]) {
    let c = cpath(sim_path);
    let fd = vfs::open(&c, libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY, 0o600);
    assert!(fd >= 0, "simvfs seed open failed for {sim_path} (errno {})", vfs::get_errno());
    let mut off = 0usize;
    while off < bytes.len() {
        let n = vfs::pwrite(fd, &bytes[off..], off as libc::off_t);
        assert!(n > 0, "simvfs seed pwrite failed for {sim_path}");
        off += n as usize;
    }
    vfs::close(fd);
}

/// Mirror `host_dir` (recursively) into the SimVfs universe at `sim_prefix`
/// ("" = keys relative to the mirrored root). Deterministic order (sorted
/// dirents). Symlinks are followed (initdb trees are link-free; tz shares
/// are copied link-resolved by the e2e, matching the wasm-boot cp -RL law).
fn mirror_into_simvfs(host_dir: &std::path::Path, sim_prefix: &str) {
    if !sim_prefix.is_empty() {
        sim_mkdir_p(sim_prefix);
    }
    let mut entries: Vec<_> = std::fs::read_dir(host_dir)
        .unwrap_or_else(|e| panic!("seed read_dir {host_dir:?}: {e}"))
        .map(|r| r.expect("seed dirent"))
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for ent in entries {
        let name = ent.file_name();
        let name = name.to_str().expect("utf8 seed names");
        // Host-lifecycle files that must not shadow the live session's raw
        // plane (the lockfile is created/owned by THIS boot on the real fs).
        if name == "postmaster.pid" || name == "postmaster.opts" {
            continue;
        }
        let sim_path = if sim_prefix.is_empty() {
            name.to_string()
        } else {
            format!("{sim_prefix}/{name}")
        };
        let ft = ent.file_type().expect("seed file_type");
        let hp = ent.path();
        if ft.is_dir() {
            sim_mkdir_p(&sim_path);
            mirror_into_simvfs(&hp, &sim_path);
        } else {
            let bytes = std::fs::read(&hp).unwrap_or_else(|e| panic!("seed read {hp:?}: {e}"));
            sim_write_file(&sim_path, &bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// The scripted in-process wire client (the pump).
// ---------------------------------------------------------------------------

fn be_i32(v: i32) -> [u8; 4] {
    v.to_be_bytes()
}

fn startup_message(user: &str, database: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&be_i32(196608)); // protocol 3.0
    for (k, v) in [("user", user), ("database", database)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = Vec::with_capacity(4 + body.len());
    msg.extend_from_slice(&be_i32(4 + body.len() as i32));
    msg.extend_from_slice(&body);
    msg
}

fn query_message(sql: &str) -> Vec<u8> {
    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&be_i32(4 + sql.len() as i32 + 1));
    msg.extend_from_slice(sql.as_bytes());
    msg.push(0);
    msg
}

fn terminate_message() -> Vec<u8> {
    let mut msg = vec![b'X'];
    msg.extend_from_slice(&be_i32(4));
    msg
}

struct SimWireClient {
    stmts: std::collections::VecDeque<String>,
    started: bool,
    terminated: bool,
    /// Everything received, for frame parsing (the provider separately
    /// accumulates the canonical transcript).
    rx: Vec<u8>,
    /// Frame-parse cursor into rx.
    cursor: usize,
    /// Complete ReadyForQuery frames observed.
    zseen: usize,
    /// Queries sent.
    sent: usize,
    /// SIM-CONVERGE: mirror the harness driver's symmetric error-recovery
    /// law (Dispatcher::dispatch / run_ctl_step: any errored statement is
    /// followed by a ROLLBACK) so a simharness plan executed as a scripted
    /// corpus produces the same statement stream a live driver would.
    /// Opt-in (PGRUST_SIMNET_RECOVER=1); default off = pre-lane behavior,
    /// byte-identical.
    recover: bool,
    /// Per-completed-cycle "carried an ErrorResponse" flags (cycle 0 = the
    /// startup exchange; statement k completes in cycle k+1).
    cycle_err: Vec<bool>,
    /// Error seen in the (not yet Z-terminated) current cycle.
    cur_err: bool,
    /// The previously sent statement was an injected ROLLBACK (never chain
    /// injections off an injected statement).
    last_injected: bool,
    /// SIM-CONVERGE inc-2: this session's turn-id in the cross-session turn
    /// schedule (2 = s2 … 5 = s5; the boot session 1 and any noise session
    /// are not part of the schedule).
    turn_id: u32,
    /// SIM-CONVERGE inc-2: the global turn order (typed turns in plan order)
    /// — each client parses the same PGRUST_SIMNET_TURNS (set once before
    /// the session threads spawn) into its own copy; the only shared mutable
    /// state is the [`TURN_POS`] cursor. Empty ⇒ no schedule active.
    turn_order: Vec<Turn>,
    /// SIM-CONVERGE inc-2: precomputed "this client participates in the
    /// cross-session interleaving". A session whose id is absent from the
    /// schedule (boot / noise) is never gated (sends its script freely).
    turn_gated: bool,
    /// SIM-CONVERGE inc-2: this client sent a script statement whose turn is
    /// still HELD — the cursor advances only when the statement's response
    /// cycle completes (ReadyForQuery observed), because the live driver's
    /// synchronous dispatch is COMPLETION-ordered: statement k fully
    /// completes before step k+1 runs. A held turn also spans the driver-law
    /// recovery ROLLBACK (statement + its recovery = one dispatch step).
    turn_held: bool,
    /// SIM-CONVERGE inc-3: the LAST sent statement is exempt from the
    /// recovery injection (async-dispatched statements are JOINED, never
    /// recovered — mirroring run_session_step — and WaitUntil probes are
    /// resent, never recovered).
    sent_norec: bool,
    /// SIM-CONVERGE inc-3: a Poll turn is being served — the probe text to
    /// resend until its cycle completes with scalar 't'.
    poll_stmt: Option<String>,
    /// SIM-CONVERGE inc-3: first DataRow's first column of the CURRENT
    /// (un-Z-terminated) cycle — poll-result capture.
    cur_first: Option<Option<String>>,
    /// Per-completed-cycle first-column capture (index-aligned with
    /// `cycle_err`; cycle 0 = the startup exchange).
    cycle_first: Vec<Option<String>>,
    /// SIM-CONVERGE inc-3: turns this client has RELEASED, against the
    /// precomputed total of its turns in the schedule — a gated client must
    /// not Terminate while it still owns future turns (the early-Terminate
    /// wedge: an async session that quit before its join turn).
    my_turns_done: usize,
    my_turns_total: usize,
}

// SIM-CONVERGE: what this session's wire client actually SENT, in order —
// the alignment artifact the harness bridge uses to consume the transcript
// (script statements + injected ROLLBACKs). One session per thread; dumped
// with the other determinism artifacts.
thread_local! {
    static SENT_LOG: std::cell::RefCell<Vec<String>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn sent_log_push(sql: &str) {
    SENT_LOG.with(|l| l.borrow_mut().push(sql.to_string()));
}

// SIM-CONVERGE inc-2: the CROSS-SESSION TURN SCHEDULE — the substrate the
// two scripted wire clients need to honor a v2 plan's serialized statement-
// order interleaving. inc-1's blocker (worklog §7): the corpus drives each
// session's script INDEPENDENTLY under the seeded permit scheduler, so without
// a gate the two clients race their statement submissions and the
// interleaving-visible results do not match the plan's cross-session order
// contract; and a client whose turn has not come had no LEGAL way to pause —
// the transport panics on a pump step that moves no bytes (by design).
//
// The gate: a process-global ordered list of session turn-ids
// (PGRUST_SIMNET_TURNS, e.g. "2 3 2 3"), one entry per GLOBAL statement in
// plan order, and a shared position cursor [`TURN_POS`]. A gated client sends
// its next SCRIPT statement only when the cursor's entry is ITS turn-id;
// otherwise it parks on the scheduler (`pgsync::thread::sleep` = a TimedPark,
// a legal decision point) and reports `PumpStatus::Yielded` — the provider's
// inc-2 status that says "no byte progress, not done" is legal here. The
// SINGLE owner of each turn advances the cursor, so non-owners' reads never
// race the owner's one advance (and the permit scheduler serializes threads
// regardless) — plain atomics suffice, no lock. A wedged schedule (a turn for
// a session that never runs) parks forever and reaches the scheduler's
// virtual-time ceiling (SCHEDCEILING) — a named verdict, never a panic.
//
// SIM-CONVERGE inc-3: TYPED turns. A bare id ("2") stays the inc-2
// completion-ordered statement turn. Three new kinds:
//   * "dN" DISPATCH — the owner sends its next script statement and the
//     turn releases AT SEND. Async statements (plan AsyncDml) block by
//     design; a completion-ordered turn there would deadlock the schedule
//     (the released session can only be unblocked by a LATER turn's
//     statement). The deadlock red proves the SCHEDCEILING verdict.
//   * "jN" JOIN — no statement moves; session N releases the turn when its
//     outstanding (async-dispatched) statement's response cycle completes.
//     Send-ordered dispatch turns + completion-ordered join turns together
//     restore the live driver's async semantics.
//   * "pN" POLL — the owner sends its next script statement (a WaitUntil
//     probe) and, each time the cycle completes with a scalar other than
//     't', RESENDS the same probe (the turn stays held); the first 't'
//     releases it. The resend count is a seeded function of the schedule.
//
// Opt-in: absent PGRUST_SIMNET_TURNS = the pre-lane independent-pump behavior,
// byte-identical for every existing corpus. Each fresh sim PROCESS starts the
// cursor at 0 (the ×3 determinism reruns are separate processes), so no reset
// across runs is needed.
static TURN_POS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TurnKind {
    Stmt,
    Dispatch,
    Join,
    Poll,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Turn {
    kind: TurnKind,
    sid: u32,
}

/// Parse PGRUST_SIMNET_TURNS into the global turn order (one typed turn per
/// global statement/join). Absent/empty ⇒ no turn gate. Loud on a malformed
/// token (harness domain: a bad schedule is a bug, not a silent no-gate).
fn read_turn_order() -> Vec<Turn> {
    std::env::var("PGRUST_SIMNET_TURNS")
        .ok()
        .map(|s| {
            s.split(|c: char| c == ',' || c.is_whitespace())
                .filter(|t| !t.is_empty())
                .map(|t| {
                    let (kind, rest) = match t.as_bytes()[0] {
                        b'd' => (TurnKind::Dispatch, &t[1..]),
                        b'j' => (TurnKind::Join, &t[1..]),
                        b'p' => (TurnKind::Poll, &t[1..]),
                        _ => (TurnKind::Stmt, t),
                    };
                    let sid = rest
                        .parse::<u32>()
                        .unwrap_or_else(|_| panic!("bad PGRUST_SIMNET_TURNS token {t:?}"));
                    Turn { kind, sid }
                })
                .collect()
        })
        .unwrap_or_default()
}

impl SimWireClient {
    fn new(stmts: Vec<String>, recover: bool, turn_id: u32, turn_order: Vec<Turn>) -> Self {
        let my_turns_total = turn_order.iter().filter(|t| t.sid == turn_id).count();
        let turn_gated = !turn_order.is_empty() && my_turns_total > 0;
        SimWireClient {
            stmts: stmts.into(),
            started: false,
            terminated: false,
            rx: Vec::new(),
            cursor: 0,
            zseen: 0,
            sent: 0,
            recover,
            cycle_err: Vec::new(),
            cur_err: false,
            last_injected: false,
            turn_id,
            turn_order,
            turn_gated,
            turn_held: false,
            sent_norec: false,
            poll_stmt: None,
            cur_first: None,
            cycle_first: Vec::new(),
            my_turns_done: 0,
            my_turns_total,
        }
    }

    fn scan_frames(&mut self) {
        while self.cursor + 5 <= self.rx.len() {
            let ty = self.rx[self.cursor];
            let len = i32::from_be_bytes(
                self.rx[self.cursor + 1..self.cursor + 5].try_into().expect("4 bytes"),
            ) as usize;
            if self.cursor + 1 + len > self.rx.len() {
                break; // incomplete frame
            }
            if ty == b'E' {
                self.cur_err = true;
            }
            if ty == b'D' && self.cur_first.is_none() {
                // SIM-CONVERGE inc-3: capture the cycle's FIRST DataRow's
                // first column (poll-result evidence; pure client-local
                // parsing — no behavior change without poll turns).
                let body = &self.rx[self.cursor + 5..self.cursor + 1 + len];
                let col = if body.len() >= 6 {
                    let ncols = u16::from_be_bytes(body[..2].try_into().expect("2 bytes"));
                    if ncols >= 1 {
                        let l = i32::from_be_bytes(body[2..6].try_into().expect("4 bytes"));
                        if l >= 0 && 6 + l as usize <= body.len() {
                            Some(
                                String::from_utf8_lossy(&body[6..6 + l as usize]).into_owned(),
                            )
                        } else {
                            None // SQL NULL (or truncated): never 't'
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                self.cur_first = Some(col);
            }
            if ty == b'Z' {
                self.zseen += 1;
                self.cycle_err.push(self.cur_err);
                self.cur_err = false;
                self.cycle_first.push(self.cur_first.take().flatten());
            }
            self.cursor += 1 + len;
        }
    }

    /// One pump step at a server block point. Deterministic: state is this
    /// struct + the pair's buffers, nothing ambient.
    fn pump(&mut self) -> pqcomm_simnet::PumpStatus {
        if !self.started {
            self.started = true;
            pqcomm_simnet::client_send(&startup_message("postgres", "postgres"));
            return pqcomm_simnet::PumpStatus::Progress;
        }
        self.rx.extend_from_slice(&pqcomm_simnet::client_recv_all());
        self.scan_frames();
        if self.terminated {
            // Nothing more will ever be sent; the provider maps this to a
            // clean EOF on the server's next read.
            return pqcomm_simnet::PumpStatus::Finished;
        }
        if self.zseen > self.sent {
            // SIM-CONVERGE recovery injection: the just-completed statement
            // (index sent-1, cycle index == sent) errored — send the
            // driver-law ROLLBACK before the next script statement. An
            // injected ROLLBACK never triggers another injection. inc-3:
            // async-dispatched statements and WaitUntil probes are exempt
            // (`sent_norec`) — the driver JOINS asyncs (run_session_step has
            // no recovery arm) and re-polls probes; neither recovers.
            if self.recover
                && self.sent >= 1
                && !self.last_injected
                && !self.sent_norec
                && self.cycle_err.get(self.sent).copied().unwrap_or(false)
            {
                pqcomm_simnet::client_send(&query_message("ROLLBACK"));
                sent_log_push("ROLLBACK");
                self.sent += 1;
                self.last_injected = true;
                return pqcomm_simnet::PumpStatus::Progress;
            }
            // SIM-CONVERGE inc-3, POLL continuation: the just-completed
            // cycle was a WaitUntil probe. Scalar 't' releases the held poll
            // turn (fall through to the release below); anything else — 'f',
            // NULL, no row, even an error — resends the SAME probe with the
            // turn still held. A gate that can never read 't' therefore
            // wedges the schedule deterministically and dies as the named
            // SCHEDCEILING verdict (never a hang, never a panic).
            if let Some(probe) = self.poll_stmt.clone() {
                let is_t = self
                    .cycle_first
                    .get(self.sent)
                    .map(|v| v.as_deref() == Some("t"))
                    .unwrap_or(false);
                if !is_t {
                    pqcomm_simnet::client_send(&query_message(&probe));
                    sent_log_push(&probe);
                    self.sent += 1;
                    self.sent_norec = true;
                    return pqcomm_simnet::PumpStatus::Progress;
                }
                self.poll_stmt = None;
            }
            // SIM-CONVERGE inc-2, turn RELEASE (completion-ordered): reaching
            // here means every sent statement's response cycle has completed
            // (zseen > sent) and no recovery injection is pending — the held
            // turn (statement + any driver-law ROLLBACK, or a whole poll
            // sequence) is done, so advance the shared cursor and release the
            // session owning the next turn. Completion-order is the live
            // driver's synchronous-dispatch semantics: statement k fully
            // completes before step k+1 runs — release-at-SEND would let two
            // statements execute concurrently and break the plan's
            // serialized-interleaving contract (dispatch turns release at
            // send DELIBERATELY: that is the async statement's contract).
            if self.turn_held {
                self.turn_held = false;
                self.my_turns_done += 1;
                TURN_POS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            // SIM-CONVERGE inc-3, JOIN turns: with nothing outstanding on
            // this connection (zseen > sent), any of MY join turns at the
            // cursor are release events — the async statement they wait on
            // has completed. Consecutive joins all release here.
            if self.turn_gated {
                loop {
                    let pos = TURN_POS.load(std::sync::atomic::Ordering::SeqCst);
                    match self.turn_order.get(pos) {
                        Some(t) if t.sid == self.turn_id && t.kind == TurnKind::Join => {
                            self.my_turns_done += 1;
                            TURN_POS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                        _ => break,
                    }
                }
            }
            // Turn WAIT: a gated client may send its next SCRIPT statement
            // only when the global cursor points at its turn-id; otherwise it
            // parks on the scheduler (a TimedPark = a legal decision point)
            // and reports Yielded — no byte progress, but a legal turn-wait,
            // not a protocol stall. inc-3: a gated client whose script AND
            // turns are both exhausted falls through to Terminate (leaving
            // the schedule); one with turns still owed keeps parking (the
            // early-Terminate wedge: an async session must survive to its
            // join turn). Schedule-exhausted (pos past the end) falls through
            // to an ungated send — defensive against a schedule shorter than
            // the script.
            let mut kind = TurnKind::Stmt;
            if self.turn_gated {
                let pos = TURN_POS.load(std::sync::atomic::Ordering::SeqCst);
                if pos < self.turn_order.len() {
                    let t = self.turn_order[pos];
                    if t.sid != self.turn_id {
                        let fully_done =
                            self.stmts.is_empty() && self.my_turns_done >= self.my_turns_total;
                        if !fully_done {
                            pgsync::thread::sleep(std::time::Duration::from_millis(1));
                            return pqcomm_simnet::PumpStatus::Yielded;
                        }
                    } else {
                        kind = t.kind;
                    }
                }
            }
            match self.stmts.pop_front() {
                Some(sql) => {
                    pqcomm_simnet::client_send(&query_message(&sql));
                    sent_log_push(&sql);
                    self.sent += 1;
                    self.last_injected = false;
                    self.sent_norec = false;
                    if self.turn_gated {
                        match kind {
                            // Held until this statement's cycle completes.
                            TurnKind::Stmt => self.turn_held = true,
                            // Released AT SEND: the statement is expected to
                            // block; later turns unblock it.
                            TurnKind::Dispatch => {
                                self.my_turns_done += 1;
                                self.sent_norec = true;
                                TURN_POS
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            }
                            // Held across the whole probe sequence.
                            TurnKind::Poll => {
                                self.poll_stmt = Some(sql);
                                self.turn_held = true;
                                self.sent_norec = true;
                            }
                            // Unreachable: my join turns were consumed above.
                            TurnKind::Join => self.turn_held = true,
                        }
                    }
                }
                None => {
                    pqcomm_simnet::client_send(&terminate_message());
                    self.terminated = true;
                }
            }
        }
        // Progress claims byte movement. Since inc-2 the provider's stall
        // fingerprint EXCLUDES op consults (review observation 2): if this
        // step neither received nor sent a byte (nor closed), the pair is
        // protocol-stalled and the provider panics deterministically at the
        // block point — the charter behavior, not an e2e-watchdog timeout.
        // Every healthy block point moves bytes (the server flushes before
        // parking; a new flush always carries a frame we drain here).
        pqcomm_simnet::PumpStatus::Progress
    }
}

// ---------------------------------------------------------------------------
// The mode entry.
// ---------------------------------------------------------------------------

/// Seed THIS thread's SimVfs universe from the host datadir image.
/// Two addressing conventions coexist in the port: post-chdir RELATIVE
/// paths (md.c-style "base/…", "pg_wal/…") and DataDir-joined ABSOLUTE
/// paths (controldata's "<datadir>/global/pg_control"). Seed the image
/// under BOTH keys; each module is internally consistent about which
/// convention it uses, so reads always find the plane its writes land on.
fn seed_universe(datadir: &str) {
    mirror_into_simvfs(std::path::Path::new(datadir), "");
    mirror_into_simvfs(std::path::Path::new(datadir), datadir.trim_end_matches('/'));
    if let Ok(dirs) = std::env::var("PGRUST_SIMNET_SEED_DIRS") {
        for d in dirs.split(':').filter(|d| !d.is_empty()) {
            mirror_into_simvfs(std::path::Path::new(d), d);
        }
    }
    // SIM-HARNESS-CONVERGE (PGRUST_SIMVFS_SEED_DURABLE=1): make the seeded
    // image DURABLE (files fold their journals, dirs promote their entry
    // images) — the sweep's sim_fsync_tree discipline. Without this, a
    // whole-node kill reverts every never-fsynced seeded file to durable
    // NOTHING and the at-cut pack is empty. Opt-in and set by the fault
    // harness on BOTH the probe and writer legs (the two op streams must
    // stay op-for-op aligned for the cut-point rebasing); every existing
    // corpus is byte-unaffected.
    if std::env::var("PGRUST_SIMVFS_SEED_DURABLE").as_deref() == Ok("1") {
        for (path, entry) in vfs::sim::SimVfs::new().image_dump() {
            let p = path.to_str().expect("utf8 sim paths").to_string();
            let fd = vfs::open(&cpath(&p), libc::O_RDONLY, 0);
            assert!(fd >= 0, "seed fsync open({p}): errno {}", vfs::get_errno());
            assert_eq!(vfs::fsync(fd), 0, "seed fsync({p})");
            assert_eq!(vfs::close(fd), 0);
            let _ = entry;
        }
    }
}

fn read_script(env_name: &str) -> Vec<String> {
    std::env::var(env_name)
        .ok()
        .map(|p| std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {env_name}: {e}")))
        .unwrap_or_else(|| "SELECT 1".to_string())
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("--"))
        .map(String::from)
        .collect()
}

fn install_pump_for(env_sql: &str, turn_id: u32) {
    // SIM-CONVERGE: PGRUST_SIMNET_RECOVER=1 arms the driver-law recovery
    // injection (see SimWireClient::pump); default off, byte-identical.
    let recover = std::env::var("PGRUST_SIMNET_RECOVER").as_deref() == Ok("1");
    // SIM-CONVERGE inc-2: the cross-session turn schedule (PGRUST_SIMNET_TURNS)
    // — empty ⇒ no gate (existing corpora byte-identical). Each client reads
    // the same env (set once before the session threads spawn).
    let turn_order = read_turn_order();
    let mut client = SimWireClient::new(read_script(env_sql), recover, turn_id, turn_order);
    pqcomm_simnet::install_client_pump(move || client.pump());
}

/// Dump THIS thread's session artifacts (transcript + op log are provider
/// thread-locals — one session per thread, one artifact pair per session).
/// SIM-CONVERGE adds the sent-log (statements actually sent, injected
/// ROLLBACKs included) under the TRANSCRIPT→SENTLOG env-name twin.
fn dump_artifacts_env(transcript_env: &str, oplog_env: &str) {
    let (_, received) = pqcomm_simnet::client_transcript();
    if let Ok(path) = std::env::var(transcript_env) {
        let _ = std::fs::write(path, &received);
    }
    if let Ok(path) = std::env::var(oplog_env) {
        let mut out = pqcomm_simnet::op_log().join("\n");
        out.push('\n');
        let _ = std::fs::write(path, out);
    }
    let sent_env = transcript_env.replace("TRANSCRIPT", "SENTLOG");
    if let Ok(path) = std::env::var(&sent_env) {
        let mut out = SENT_LOG.with(|l| l.borrow().join("\n"));
        out.push('\n');
        let _ = std::fs::write(path, out);
    }
}

// Thread-local globals a spawned session thread inherits from the booted
// thread — launch_backend's `Inherited` census (the fork-inheritance list),
// scalar subset; the MAXPGPATH exec-path triple is omitted (nothing in a sim
// wire session consults it; ledgered in notes/permit-s1-demo.md). The spawn
// itself migrates to the launch_backend/pgsync spawn door when WS-CORE's
// registration hook lands — this snapshot is the scaffold stand-in.
macro_rules! sim_inherited {
    ($($field:ident : $ty:ty = $get:ident / $set:ident;)+) => {
        // Clone: the N-session corpus applies one pristine postmaster
        // capture to EVERY spawned session (scalars + a &'static str).
        #[derive(Clone)]
        struct SimInherited {
            data_dir: Option<&'static str>,
            $($field: $ty,)+
        }
        impl SimInherited {
            fn capture() -> Self {
                Self {
                    data_dir: init_small::globals::DataDir(),
                    $($field: init_small::globals::$get(),)+
                }
            }
            fn apply(&self) {
                if let Some(dd) = self.data_dir {
                    init_small::globals::SetDataDir(dd);
                }
                $(init_small::globals::$set(self.$field);)+
            }
        }
    };
}

sim_inherited! {
    data_directory_mode: i32 = data_directory_mode / set_data_directory_mode;
    date_style: i32 = DateStyle / SetDateStyle;
    date_order: i32 = DateOrder / SetDateOrder;
    interval_style: i32 = IntervalStyle / SetIntervalStyle;
    enable_fsync: bool = enableFsync / set_enableFsync;
    allow_system_table_mods: bool = allowSystemTableMods / set_allowSystemTableMods;
    work_mem: i32 = work_mem / set_work_mem;
    hash_mem_multiplier: f64 = hash_mem_multiplier / set_hash_mem_multiplier;
    maintenance_work_mem: i32 = maintenance_work_mem / set_maintenance_work_mem;
    max_parallel_maintenance_workers: i32 =
        max_parallel_maintenance_workers / set_max_parallel_maintenance_workers;
    n_buffers: i32 = NBuffers / SetNBuffers;
    max_connections: i32 = MaxConnections / SetMaxConnections;
    max_worker_processes: i32 = max_worker_processes / set_max_worker_processes;
    max_parallel_workers: i32 = max_parallel_workers / set_max_parallel_workers;
    max_backends: i32 = MaxBackends / SetMaxBackends;
    vacuum_buffer_usage_limit: i32 = VacuumBufferUsageLimit / SetVacuumBufferUsageLimit;
    commit_timestamp_buffers: i32 = commit_timestamp_buffers / set_commit_timestamp_buffers;
    multixact_member_buffers: i32 = multixact_member_buffers / set_multixact_member_buffers;
    multixact_offset_buffers: i32 = multixact_offset_buffers / set_multixact_offset_buffers;
    notify_buffers: i32 = notify_buffers / set_notify_buffers;
    serializable_buffers: i32 = serializable_buffers / set_serializable_buffers;
    subtransaction_buffers: i32 = subtransaction_buffers / set_subtransaction_buffers;
    transaction_buffers: i32 = transaction_buffers / set_transaction_buffers;
}

/// Run one wire session on the CURRENT thread (session half only; the boot
/// half must have completed on the booting thread). Returns the session's
/// exit code; dumps this thread's artifacts on a clean ProcExitThread.
fn run_session_on_this_thread(transcript_env: &str, oplog_env: &str) -> i32 {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> core::convert::Infallible {
            let err = match crate::stdio_wire::stdio_wire_session_half() {
                Ok(never) => match never {},
                Err(err) => err,
            };
            elog::emit_error_report_for(&err);
            ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
        },
    ));
    let payload = match outcome {
        Ok(never) => match never {},
        Err(payload) => payload,
    };
    match payload.downcast_ref::<ipc::ProcExitThread>() {
        Some(p) => {
            // Exit callbacks already ran inline during the unwind
            // (!IsUnderPostmaster drains in place); the session is complete.
            dump_artifacts_env(transcript_env, oplog_env);
            p.code
        }
        None => std::panic::resume_unwind(payload),
    }
}

/// DST-PMCHILD: one under-PM harness session's identity — the env-name
/// triple (scripted SQL in, artifacts out), the synthetic-pid offset, and
/// (when the corpus registers it) the parent-assigned pmchild Backend slot.
/// `child_slot: None` is the pre-lane anonymous-thread shape, byte-identical
/// (the sessions=2 P1/P3 corpus rides it unchanged).
#[derive(Clone, Copy)]
struct SessionSpec {
    thread_name: &'static str,
    sql_env: &'static str,
    transcript_env: &'static str,
    oplog_env: &'static str,
    /// MyProcPid = process_id() + pid_offset (the sim pid pin gives every
    /// thread the same ambient pid; sessions must differ).
    pid_offset: i32,
    /// Some(slot) = this session is a REGISTERED pmchild Backend: the
    /// thread claims the slot via SetMyPMChildSlot (the slot-active
    /// protocol keys off it in InitProcess) and announces its exit so the
    /// reaper's Backend arm releases it — real child accounting.
    child_slot: Option<i32>,
    /// SIM-CONVERGE inc-2: this session's turn-id in the cross-session turn
    /// schedule (PGRUST_SIMNET_TURNS). s2 = 2, s3 = 3; the boot session is 1
    /// and is never part of the schedule (it runs the setup to completion
    /// before the workers spawn).
    turn_id: u32,
}

impl SessionSpec {
    /// The historical session-2 identity (P1/P3 and the parquery leader).
    fn second(child_slot: Option<i32>) -> SessionSpec {
        SessionSpec {
            thread_name: "sim-session-2",
            sql_env: "PGRUST_SIMNET_SQL2",
            transcript_env: "PGRUST_SIMNET_TRANSCRIPT2",
            oplog_env: "PGRUST_SIMNET_OPLOG2",
            pid_offset: 1,
            child_slot,
            turn_id: 2,
        }
    }

    /// The N-session corpus's third session.
    fn third(child_slot: Option<i32>) -> SessionSpec {
        SessionSpec {
            thread_name: "sim-session-3",
            sql_env: "PGRUST_SIMNET_SQL3",
            transcript_env: "PGRUST_SIMNET_TRANSCRIPT3",
            oplog_env: "PGRUST_SIMNET_OPLOG3",
            pid_offset: 2,
            child_slot,
            turn_id: 3,
        }
    }

    /// SIM-CONVERGE inc-3: the fourth/fifth sessions (the pmchild
    /// registration pattern generalizes — the S1-SpecConflict choreography
    /// needs 4 concurrent plan sessions). Spawned only when their SQL env
    /// is present; every existing corpus is byte-identical.
    fn fourth(child_slot: Option<i32>) -> SessionSpec {
        SessionSpec {
            thread_name: "sim-session-4",
            sql_env: "PGRUST_SIMNET_SQL4",
            transcript_env: "PGRUST_SIMNET_TRANSCRIPT4",
            oplog_env: "PGRUST_SIMNET_OPLOG4",
            pid_offset: 3,
            child_slot,
            turn_id: 4,
        }
    }

    fn fifth(child_slot: Option<i32>) -> SessionSpec {
        SessionSpec {
            thread_name: "sim-session-5",
            sql_env: "PGRUST_SIMNET_SQL5",
            transcript_env: "PGRUST_SIMNET_TRANSCRIPT5",
            oplog_env: "PGRUST_SIMNET_OPLOG5",
            pid_offset: 4,
            child_slot,
            turn_id: 5,
        }
    }
}

/// DST-PMCHILD: register an under-PM harness session as a REAL pmchild
/// Backend — the BackendStartup shape (assign the slot, then publish the
/// pid), parent-side BEFORE the spawn so the pid is registry-visible before
/// the session can register bgworkers. This kills the multibackend §1
/// zeroed-notify deviation: BackgroundWorkerStateChange's
/// find_postmaster_child_by_pid validity check now PASSES for the leader's
/// bgw_notify_pid, so ReportBackgroundWorkerPID's SIGUSR1 notify flow runs
/// exactly as in C instead of liveness riding the waiter cadence.
fn register_session_backend(pid: i32) -> i32 {
    let slot = pmchild_seams::assign_postmaster_child_slot::call(
        types_core::init::BackendType::Backend,
    )
    .expect("no free pmchild Backend slot for a harness session");
    pmchild_seams::set_child_pid::call(slot, pid);
    slot
}

/// DST-PMCHILD: the boot thread's virtual-time drain budget (quanta of the
/// 1 ms service cadence). Default matches the old blind poll bound.
fn drain_budget() -> u32 {
    std::env::var("PGRUST_SIM_DRAIN_BUDGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60_000)
}

/// DST-PMCHILD: the shutdown-hang red's catcher — a child that never exits
/// must produce a NAMED watchdog verdict, not a hang. Grep-stable
/// "SHUTDOWNDRAIN:" line + abort (exit 134, the watchdog convention); the
/// SCHEDCEILING virtual-time bound (PGRUST_SIM_VCEIL_S) is the safety net
/// underneath this (it names the parked site from the scheduler side).
fn shutdown_drain_check(quanta: u32, budget: u32, what: &str, detail: impl Fn() -> String) {
    if quanta >= budget {
        eprintln!(
            "SHUTDOWNDRAIN: {what} never exited after {budget} virtual-ms drain quanta ({})",
            detail()
        );
        std::process::abort();
    }
}

/// DST-PMCHILD drain phase A: service the postmaster surrogate until the
/// reaper has processed EVERY registered session's exit announce. The loop
/// condition is the pmchild registry — boot-thread-local modeled state
/// mutated by process_pm_child_exit's Backend arm ON this thread — so the
/// iteration count is a pure function of the seeded schedule; the OS
/// teardown fact (`is_finished()`) the old drain polled is out of the loop
/// (the §7 row-3 wall-coupling retired). The caller then joins through the
/// pgsync JoinHandle (hooked Join parks until the exit hook wakes joiners;
/// the residual raw join is bounded teardown — the NB-2 shape).
fn drain_until_reaped(pids: &[i32], budget: u32) -> u32 {
    let mut quanta = 0u32;
    let live = |pids: &[i32]| -> Vec<i32> {
        pids.iter()
            .copied()
            .filter(|p| pmchild_seams::find_postmaster_child_by_pid::call(*p).is_some())
            .collect()
    };
    while !live(pids).is_empty() {
        postmaster_seams::pm_service_pending::call();
        postmaster_seams::wpool_maintain::call();
        pgsync::thread::sleep(std::time::Duration::from_millis(1));
        quanta += 1;
        shutdown_drain_check(quanta, budget, "registered session backend(s)", || {
            format!("unreaped pids {:?}", live(pids))
        });
    }
    quanta
}

/// DST-PMCHILD drain phase B: the pool drain (P5 teardown shape) with the
/// blind poll bound replaced by the named verdict. POPULATION is modeled
/// state here: the charge drops inside each standby's final quantum
/// (PopulationCharge is declared after the door guard, so it drops before
/// the exit hook), and the reaper's join of rotation announces is the
/// hooked NB-2 join.
fn drain_pool(budget: u32) -> u32 {
    postmaster_seams::wpool_flush::call();
    let mut quanta = 0u32;
    while postmaster_seams::wpool_population::call() > 0 {
        pgsync::thread::sleep(std::time::Duration::from_millis(1));
        postmaster_seams::pm_service_pending::call();
        quanta += 1;
        shutdown_drain_check(quanta, budget, "wpool standbys", || {
            format!("population={}", postmaster_seams::wpool_population::call())
        });
    }
    quanta
}

// ===========================================================================
// SIM-HARNESS-CONVERGE: the fault-plan delivery reader (the t28 activation
// step the harness's runner/faultdriver.rs documents), the at-cut pack
// exporter, and the vfs-op progress report. All opt-in via env; every
// existing corpus is byte-unaffected.
// ===========================================================================

/// Minimal JSON reader for the harness `FaultPlanSpec` wire format
/// (crash-simulator/src/runner/faultdriver.rs — serde_json on the client
/// side; the product side must not grow a serde dependency, so this is a
/// hand-rolled reader for exactly that schema, loud on anything else).
mod fault_spec_json {
    #[derive(Debug, Clone, PartialEq)]
    pub enum J {
        Null,
        Bool(bool),
        Num(i128),
        Str(String),
        Arr(Vec<J>),
        Obj(Vec<(String, J)>),
    }

    pub struct P<'a> {
        b: &'a [u8],
        i: usize,
    }

    impl<'a> P<'a> {
        pub fn new(s: &'a str) -> Self {
            P { b: s.as_bytes(), i: 0 }
        }
        fn ws(&mut self) {
            while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
                self.i += 1;
            }
        }
        fn peek(&mut self) -> Result<u8, String> {
            self.ws();
            self.b.get(self.i).copied().ok_or_else(|| "unexpected end".into())
        }
        fn eat(&mut self, c: u8) -> Result<(), String> {
            if self.peek()? == c {
                self.i += 1;
                Ok(())
            } else {
                Err(format!("expected '{}' at byte {}", c as char, self.i))
            }
        }
        fn string(&mut self) -> Result<String, String> {
            self.eat(b'"')?;
            let mut out = String::new();
            loop {
                let c = *self.b.get(self.i).ok_or("unterminated string")?;
                self.i += 1;
                match c {
                    b'"' => return Ok(out),
                    b'\\' => {
                        let e = *self.b.get(self.i).ok_or("unterminated escape")?;
                        self.i += 1;
                        match e {
                            b'"' => out.push('"'),
                            b'\\' => out.push('\\'),
                            b'/' => out.push('/'),
                            b'n' => out.push('\n'),
                            b't' => out.push('\t'),
                            b'r' => out.push('\r'),
                            other => {
                                return Err(format!(
                                    "unsupported escape '\\{}' (fault-spec strings are plain)",
                                    other as char
                                ))
                            }
                        }
                    }
                    _ => out.push(c as char),
                }
            }
        }
        pub fn value(&mut self) -> Result<J, String> {
            match self.peek()? {
                b'{' => {
                    self.eat(b'{')?;
                    let mut kv = Vec::new();
                    if self.peek()? == b'}' {
                        self.eat(b'}')?;
                        return Ok(J::Obj(kv));
                    }
                    loop {
                        let k = self.string()?;
                        self.eat(b':')?;
                        let v = self.value()?;
                        kv.push((k, v));
                        match self.peek()? {
                            b',' => self.eat(b',')?,
                            b'}' => {
                                self.eat(b'}')?;
                                return Ok(J::Obj(kv));
                            }
                            c => return Err(format!("expected ',' or '}}', got '{}'", c as char)),
                        }
                    }
                }
                b'[' => {
                    self.eat(b'[')?;
                    let mut a = Vec::new();
                    if self.peek()? == b']' {
                        self.eat(b']')?;
                        return Ok(J::Arr(a));
                    }
                    loop {
                        a.push(self.value()?);
                        match self.peek()? {
                            b',' => self.eat(b',')?,
                            b']' => {
                                self.eat(b']')?;
                                return Ok(J::Arr(a));
                            }
                            c => return Err(format!("expected ',' or ']', got '{}'", c as char)),
                        }
                    }
                }
                b'"' => Ok(J::Str(self.string()?)),
                b't' => {
                    self.lit("true")?;
                    Ok(J::Bool(true))
                }
                b'f' => {
                    self.lit("false")?;
                    Ok(J::Bool(false))
                }
                b'n' => {
                    self.lit("null")?;
                    Ok(J::Null)
                }
                _ => self.number(),
            }
        }
        fn lit(&mut self, s: &str) -> Result<(), String> {
            self.ws();
            if self.b[self.i..].starts_with(s.as_bytes()) {
                self.i += s.len();
                Ok(())
            } else {
                Err(format!("expected literal {s}"))
            }
        }
        fn number(&mut self) -> Result<J, String> {
            self.ws();
            let start = self.i;
            if self.b.get(self.i) == Some(&b'-') {
                self.i += 1;
            }
            while self.i < self.b.len() && self.b[self.i].is_ascii_digit() {
                self.i += 1;
            }
            if self.i == start {
                return Err(format!("expected a value at byte {start}"));
            }
            if matches!(self.b.get(self.i), Some(b'.') | Some(b'e') | Some(b'E')) {
                return Err("floats are not in the fault-spec schema".into());
            }
            std::str::from_utf8(&self.b[start..self.i])
                .ok()
                .and_then(|s| s.parse::<i128>().ok())
                .map(J::Num)
                .ok_or_else(|| "bad number".into())
        }
    }

    impl J {
        pub fn get<'j>(&'j self, key: &str) -> Option<&'j J> {
            match self {
                J::Obj(kv) => kv.iter().find(|(k, _)| k == key).map(|(_, v)| v),
                _ => None,
            }
        }
        pub fn num(&self) -> Option<i128> {
            match self {
                J::Num(n) => Some(*n),
                _ => None,
            }
        }
        pub fn strv(&self) -> Option<&str> {
            match self {
                J::Str(s) => Some(s),
                _ => None,
            }
        }
    }
}

/// Map the spec's OpKind name → the engine's. Loud on unknown names
/// (vocab law: never guessed).
fn fault_op_kind(name: &str) -> vfs::sim::OpKind {
    use vfs::sim::OpKind::*;
    match name {
        "Open" => Open,
        "Close" => Close,
        "PReadV" => PReadV,
        "PWriteV" => PWriteV,
        "Fsync" => Fsync,
        "Fdatasync" => Fdatasync,
        "FlushRange" => FlushRange,
        "Ftruncate" => Ftruncate,
        "TruncatePath" => TruncatePath,
        "Fallocate" => Fallocate,
        "FileSize" => FileSize,
        "FadviseWillneed" => FadviseWillneed,
        "Stat" => Stat,
        "Fstat" => Fstat,
        "Lstat" => Lstat,
        "ReadLink" => ReadLink,
        "Unlink" => Unlink,
        "Rename" => Rename,
        "Mkdir" => Mkdir,
        "Rmdir" => Rmdir,
        "ReadDir" => ReadDir,
        other => panic!("PGRUST_SIM_FAULT_PLAN: unknown OpKind '{other}'"),
    }
}

fn fault_path_class(name: &str) -> vfs::sim::PathClass {
    use vfs::sim::PathClass::*;
    match name {
        "Wal" => Wal,
        "Config" => Config,
        "Temp" => Temp,
        "Heap" => Heap,
        "Other" => Other,
        other => panic!("PGRUST_SIM_FAULT_PLAN: unknown PathClass '{other}'"),
    }
}

fn fault_action(j: &fault_spec_json::J) -> vfs::sim::FaultDecision {
    use fault_spec_json::J;
    use vfs::sim::FaultDecision as D;
    match j {
        J::Str(s) if s == "Crash" => D::Crash,
        J::Obj(kv) if kv.len() == 1 => {
            let (k, v) = &kv[0];
            match (k.as_str(), v) {
                ("Errno", J::Num(n)) => D::Errno(*n as i32),
                ("ShortRead", J::Num(n)) => D::ShortRead(*n as usize),
                ("ShortWrite", J::Num(n)) => D::ShortWrite(*n as usize),
                ("TornWrite", obj) => D::TornWrite {
                    persist_prefix: obj
                        .get("persist_prefix")
                        .and_then(|n| n.num())
                        .expect("TornWrite.persist_prefix")
                        as usize,
                },
                (other, _) => panic!("PGRUST_SIM_FAULT_PLAN: unknown action '{other}'"),
            }
        }
        other => panic!("PGRUST_SIM_FAULT_PLAN: malformed action {other:?}"),
    }
}

/// SIM-HARNESS-CONVERGE: the DUT-side reader of the harness FaultDriver's
/// delivery channel (PGRUST_SIM_FAULT_PLAN = FaultPlanSpec JSON) — installs
/// the engine fault plan on THIS corpus's universe (call it after the
/// shared-universe setup so every session's ops consult it) and arms the
/// whole-node kill so the at-cut image is pure (no unwind residue — the
/// inc-3 exposure). `SeededFaultPlan::install` also arms
/// `CrashImage::SeededSubset(seed)`, the documented common harness shape.
fn install_fault_plan_from_env() {
    let Ok(json) = std::env::var("PGRUST_SIM_FAULT_PLAN") else { return };
    if json.trim().is_empty() {
        return;
    }
    let top = fault_spec_json::P::new(&json)
        .value()
        .unwrap_or_else(|e| panic!("PGRUST_SIM_FAULT_PLAN parse: {e}"));
    let seed = top.get("seed").and_then(|n| n.num()).expect("fault spec: seed") as u64;
    let rules_j = match top.get("rules") {
        Some(fault_spec_json::J::Arr(a)) => a,
        _ => panic!("fault spec: rules array"),
    };
    let mut rules = Vec::new();
    for r in rules_j {
        let m = r.get("matcher").expect("rule.matcher");
        let kinds = match m.get("kinds") {
            None | Some(fault_spec_json::J::Null) => None,
            Some(fault_spec_json::J::Arr(a)) => Some(
                a.iter()
                    .map(|k| fault_op_kind(k.strv().expect("kind name")))
                    .collect::<Vec<_>>(),
            ),
            other => panic!("rule.matcher.kinds malformed: {other:?}"),
        };
        let class = match m.get("class") {
            None | Some(fault_spec_json::J::Null) => None,
            Some(j) => Some(fault_path_class(j.strv().expect("class name"))),
        };
        let path_contains = match m.get("path_contains") {
            None | Some(fault_spec_json::J::Null) => None,
            Some(j) => Some(j.strv().expect("path_contains").to_string()),
        };
        rules.push(vfs::sim::FaultRule {
            matcher: vfs::sim::OpMatch { kinds, class, path_contains },
            nth: r.get("nth").and_then(|n| n.num()).expect("rule.nth") as u64,
            action: fault_action(r.get("action").expect("rule.action")),
            sticky: matches!(r.get("sticky"), Some(fault_spec_json::J::Bool(true))),
        });
    }
    let n = rules.len();
    vfs::sim::SeededFaultPlan::install(seed, rules);
    vfs::sim::SimVfs::set_kill_on_cut(true);
    eprintln!("SIMFAULT installed seed={seed} rules={n} kill_on_cut=1");
}

/// SIM-HARNESS-CONVERGE: vfs-op progress evidence for the harness's cut-point
/// selection (opt-in: PGRUST_SIMVFS_OPS_REPORT=1). Grep-stable.
fn ops_report(tag: &str) {
    if std::env::var("PGRUST_SIMVFS_OPS_REPORT").as_deref() == Ok("1") {
        eprintln!("SIMVFS-OPS {tag}={}", vfs::sim::SimVfs::op_seq());
    }
}

/// SIM-HARNESS-CONVERGE: export the universe's DURABLE images to a host-fs
/// pack dir (PGRUST_SIMVFS_PACK) — the at-cut image the reboot leg recovers
/// over. The port keeps TWO addressing conventions for the same datadir
/// (post-chdir RELATIVE and DataDir-joined ABSOLUTE; see seed_universe), so
/// each logical file may have a stale seeded twin. Freshness rule: a twin
/// whose bytes CHANGED from the host seed image is the written (fresh) one;
/// if both changed, the ABSOLUTE plane wins (controldata's convention — the
/// only known abs-plane writer) and the conflict is counted + logged.
fn export_pack_if_requested(datadir: &str) -> Option<(usize, usize)> {
    let dst = std::env::var("PGRUST_SIMVFS_PACK").ok()?;
    if dst.trim().is_empty() {
        return None;
    }
    let dstp = std::path::Path::new(&dst);
    let dd = datadir.trim_end_matches('/');
    // rel path -> (relative-plane bytes, absolute-plane bytes, is_dir)
    #[derive(Default)]
    struct Twin {
        rel: Option<Vec<u8>>,
        abs: Option<Vec<u8>>,
        dir: bool,
    }
    let mut twins: std::collections::BTreeMap<std::path::PathBuf, Twin> =
        std::collections::BTreeMap::new();
    let image = vfs::sim::SimVfs::new().image_dump();
    if std::env::var("PGRUST_SIMVFS_PACK_DEBUG").as_deref() == Ok("1") {
        eprintln!("SIMPACK-DEBUG entries={} dd={dd}", image.len());
        for (p, e) in image.iter() {
            let s = p.to_string_lossy();
            if s.contains("pg_wal") || s.contains("pg_control") {
                eprintln!(
                    "SIMPACK-DEBUG key={} file={} dlen={}",
                    p.display(),
                    e.is_some(),
                    e.as_ref().map(|(_, d)| d.len()).unwrap_or(0)
                );
            }
        }
    }
    // Seed-dir prefixes (tz share): host-absolute planes that are NOT
    // datadir state — never packed.
    let skip_prefixes: Vec<String> = std::env::var("PGRUST_SIMNET_SEED_DIRS")
        .map(|v| v.split(':').filter(|d| !d.is_empty()).map(String::from).collect())
        .unwrap_or_default();
    for (path, entry) in image {
        // Universe key planes (seed_universe's two mirrors, normalized by
        // the vfs against "/"): "<dd>/<rel>" = the DataDir-joined ABSOLUTE
        // convention (controldata); "/<rel>" = the post-chdir RELATIVE
        // convention (md.c/xlog — the plane the data-path writes land on).
        let (key, is_abs) = if let Ok(rel) = path.strip_prefix(dd) {
            (rel.to_path_buf(), true)
        } else {
            let s = path.to_string_lossy();
            if skip_prefixes.iter().any(|p| s.starts_with(p.as_str())) {
                continue; // tz share plane: not datadir state
            }
            match path.strip_prefix("/") {
                Ok(rel) => (rel.to_path_buf(), false),
                Err(_) => (path.clone(), false),
            }
        };
        if key.as_os_str().is_empty() {
            continue; // the datadir root itself
        }
        if key.file_name().is_some_and(|f| f == "postmaster.pid") {
            continue;
        }
        let t = twins.entry(key).or_default();
        match entry {
            None => t.dir = true,
            Some((_volatile, durable)) => {
                if is_abs {
                    t.abs = Some(durable);
                } else {
                    t.rel = Some(durable);
                }
            }
        }
    }
    let mut files = 0usize;
    let mut conflicts = 0usize;
    for (rel, t) in &twins {
        let out = dstp.join(rel);
        if t.dir && t.rel.is_none() && t.abs.is_none() {
            let _ = std::fs::create_dir_all(&out);
            continue;
        }
        let chosen: &Vec<u8> = match (&t.rel, &t.abs) {
            (Some(r), None) => r,
            (None, Some(a)) => a,
            (Some(r), Some(a)) if r == a => r,
            (Some(r), Some(a)) => {
                // Twins differ: prefer the one that changed from the host
                // seed; both-changed prefers the absolute plane, counted.
                let host = std::fs::read(std::path::Path::new(dd).join(rel)).ok();
                match &host {
                    Some(h) if r == h && a != h => a,
                    Some(h) if a == h && r != h => r,
                    _ => {
                        conflicts += 1;
                        eprintln!("SIMPACK-CONFLICT {} (abs plane wins)", rel.display());
                        a
                    }
                }
            }
            (None, None) => continue,
        };
        if let Some(parent) = out.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&out, chosen);
        files += 1;
    }
    Some((files, conflicts))
}

/// SIM-HARNESS-CONVERGE: at-cut evidence + pack, called on the corpus exit
/// paths. Grep-stable SIMCUT line only when the whole-node kill fired.
fn simcut_pack_and_report(datadir: &str) {
    if vfs::sim::SimVfs::killed() {
        let (files, conflicts) = export_pack_if_requested(datadir).unwrap_or((0, 0));
        eprintln!(
            "SIMCUT cuts={} frozen={} packed={files} conflicts={conflicts}",
            vfs::sim::SimVfs::cut_count(),
            vfs::sim::SimVfs::frozen_op_count(),
        );
    }
}

/// Session 2's session half: stdio_wire_session_half's shape for an
/// under-postmaster-style thread — same connection-first order, two
/// deliberate deltas: (1) wedge-ledger W6: the under-postmaster arm runs
/// REAL hba auth, and the sim transport's zeroed raddr ("host \"???\"")
/// matches no hba line — patch the port's peer to an AF_UNIX sockaddr so
/// initdb's `local all all trust` row matches (the transport IS
/// host-supplied, trust-by-construction, same argument as
/// wire_session_initialize's); (2) no single-user signal bridge (native
/// signal dispositions are process-wide; the boot thread owns them).
fn run_second_session_inner() -> ::types_error::PgResult<core::convert::Infallible> {
    // Wedge-ledger W7: the hba table is loaded by the POSTMASTER
    // (PostmasterMain), which this mode does not run — an under-postmaster
    // session thread must load it itself or every entry lookup fails with
    // "no pg_hba.conf entry". Raw-plane read of the initdb-generated file
    // (local/host trust rows), postmaster order: hba then ident.
    assert!(auth_seams::load_hba::call(), "s2: could not load pg_hba.conf");
    let _ = auth_seams::load_ident::call();
    {
        let startup = mcx::MemoryContext::new("WireStartup");
        backend_startup::wire_session_initialize(startup.mcx())?;
    }
    init_small::globals::WithMyProcPort(|p| {
        let mut un: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        un.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let n = std::mem::size_of::<libc::sockaddr_un>().min(p.raddr.addr.len());
        let src = &un as *const libc::sockaddr_un as *const u8;
        unsafe { std::ptr::copy_nonoverlapping(src, p.raddr.addr.as_mut_ptr(), n) };
        p.raddr.salen = n as u32;
    });
    lmgr_proc::InitProcess(miscinit::GetMyBackendType())?;
    let (dbname, username) = init_small::globals::WithMyProcPort(|p| {
        (
            p.database_name.clone().unwrap_or_default(),
            p.user_name.clone().unwrap_or_default(),
        )
    });
    crate::PostgresMain(&dbname, &username)
}

fn run_second_session(spec: &SessionSpec) -> i32 {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> core::convert::Infallible {
            let err = match run_second_session_inner() {
                Ok(never) => match never {},
                Err(err) => err,
            };
            elog::emit_error_report_for(&err);
            ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
        },
    ));
    let payload = match outcome {
        Ok(never) => match never {},
        Err(payload) => payload,
    };
    match payload.downcast_ref::<ipc::ProcExitThread>() {
        Some(p) => {
            // Under-postmaster-style thread: the exit-callback drain is
            // deferred to the thread top — run_child_task's shape.
            let code = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                ipc::run_deferred_exit_callbacks(p.code)
            }))
            .unwrap_or(101);
            dump_artifacts_env(spec.transcript_env, spec.oplog_env);
            // DST-PMCHILD: a REGISTERED session backend reports its exit
            // through the postmaster's waitpid channel — the closure's last
            // act, after the deferred exit drain (C's moment: waitpid sees
            // the child only once fully dead; the shmem-exit callbacks that
            // MarkPostmasterChildInactive have already run above). The
            // reaper's Backend arm releases the pmchild slot; the boot
            // thread's modeled drain keys on that release.
            if spec.child_slot.is_some()
                && postmaster_seams::announce_child_exit::is_installed()
            {
                // SIM-HARNESS-CONVERGE fault mode: announce CLEAN. A
                // nonzero status makes the reaper run the C crash-restart
                // cycle (terminate-all + reinitialize), which reads
                // pg_control through the KILLED vfs and dies before the
                // at-cut pack. The SIMCUT line is the crash evidence; the
                // reaper's only needed act here is the slot release.
                let status =
                    if std::env::var("PGRUST_SIM_FAULT_PLAN").is_ok() { 0 } else { code << 8 };
                postmaster_seams::announce_child_exit::call(
                    init_small::globals::MyProcPid(),
                    status,
                );
            }
            code
        }
        None => {
            // SIM-HARNESS-CONVERGE fault mode: a session that PANICS after
            // the whole-node kill (e.g. a WAL-flush PANIC at the cut) must
            // still dump its artifacts and announce its exit — otherwise
            // the modeled drain waits on a corpse and SHUTDOWNDRAIN aborts
            // before the pack. Gated on the fault plan being armed: byte-
            // zero movement for every existing corpus.
            if std::env::var("PGRUST_SIM_FAULT_PLAN").is_ok() {
                dump_artifacts_env(spec.transcript_env, spec.oplog_env);
                if spec.child_slot.is_some()
                    && postmaster_seams::announce_child_exit::is_installed()
                {
                    postmaster_seams::announce_child_exit::call(
                        init_small::globals::MyProcPid(),
                        0, // clean announce: see the fault-mode note above
                    );
                }
                134
            } else {
                std::panic::resume_unwind(payload)
            }
        }
    }
}

/// The second session thread's prelude: a fresh thread has NONE of the
/// booted thread's thread-local state ("C per-process globals" are
/// thread-locals in the port) and an EMPTY SimVfs universe. Re-derive the
/// cheap config-shaped state the way an EXEC_BACKEND child would, seed the
/// universe, then serve the session. KNOWN LIMIT (wedge-ledger row L1): the
/// universe is PRIVATE — the two sessions do not share a database; the
/// shared plane is process shmem only. Multi-backend sim over ONE shared
/// universe is a substrate follow-on, not a demo-lane fix.
fn second_session_thread(
    argv: Vec<String>,
    datadir: String,
    snap: SimInherited,
    // SIMCORPUS P9: Some(id) = ADOPT the shared process universe instead of
    // seeding a private copy (the parquery leader must see the tables its
    // setup session wrote — one filesystem per simulated process). None =
    // the sessions=2 private-universe behavior, byte-identical.
    adopt: Option<u64>,
    // DST-PMCHILD: which session this thread IS (env triple, pid offset,
    // optional pmchild Backend registration). SessionSpec::second(None) is
    // the historical behavior, op-for-op.
    spec: SessionSpec,
) -> pgsync::thread::JoinHandle<i32> {
    // PERMIT-S1 compose wiring: pgsync spawn — under PGRUST_SIM_SCHED=1 the
    // child registers in-model (synthetic vpid, parent-side spawn fence) so
    // the two backends interleave ONLY at scheduler touches and the SCHEDOP
    // stream is a seeded function of PGRUST_SIM_SEED; plain std passthrough
    // when the scheduler is off (the pre-compose behavior).
    pgsync::thread::Builder::new()
        .name(spec.thread_name.into())
        .stack_size(64 * 1024 * 1024)
        .spawn(move || {
            // SIM-HARNESS-CONVERGE fault mode: after the whole-node kill,
            // even the PRELUDE below can panic (its expects read config
            // files through the killed vfs -> EIO). Contain the whole
            // body so the thread still announces its exit and the boot
            // thread's modeled drain + at-cut pack proceed. Gated on the
            // fault plan env: zero movement for every existing corpus.
            if !std::env::var("PGRUST_SIM_FAULT_PLAN").is_ok() {
                return second_session_body(argv, datadir, snap, adopt, spec);
            }
            let body = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                second_session_body(argv, datadir, snap, adopt, spec)
            }));
            match body {
                Ok(code) => code,
                Err(_) => {
                    dump_artifacts_env(spec.transcript_env, spec.oplog_env);
                    if spec.child_slot.is_some()
                        && postmaster_seams::announce_child_exit::is_installed()
                    {
                        postmaster_seams::announce_child_exit::call(
                            init_small::globals::process_id() as i32 + spec.pid_offset,
                            0, // clean announce (fault mode): a nonzero
                               // status would run the crash-restart cycle
                               // over the killed vfs before the pack
                        );
                    }
                    134
                }
            }
        })
        .expect("spawn sim-session thread")
}

/// The session thread's body (prelude + wire session) — split out so the
/// fault-mode containment above can wrap it whole.
fn second_session_body(
    argv: Vec<String>,
    datadir: String,
    snap: SimInherited,
    adopt: Option<u64>,
    spec: SessionSpec,
) -> i32 {
    {
        {
            snap.apply();
            // The child-init sequence, the launch_backend way: the second
            // session is an UNDER-POSTMASTER-style backend thread (the boot
            // thread played "postmaster + first backend"). Wedge-ledger W1:
            // a bare thread dies at SwitchToSharedLatch's local-latch debug
            // assert — InitPostmasterChild owns the latch trio. Wedge-ledger
            // W2: a STANDALONE-arm session half re-runs StartupXLOG, which
            // reads the booted thread's PROCESS-GLOBAL ControlFile image but
            // looks for its checkpoint in THIS thread's PRIVATE SimVfs
            // universe → "could not locate a valid checkpoint record" PANIC;
            // IsUnderPostmaster=true is the product path that skips it.
            // Distinct synthetic pid: the sim pid pin gives every thread the
            // same ambient pid; sessions must differ.
            // Wedge-ledger W4: seed THIS thread's universe before any GUC
            // processing — timezone validation walks the tz database through
            // the (thread-local) vfs plane. SIMCORPUS P9: with sharing on,
            // ADOPT the process universe instead (the two-line spawn-door
            // pattern's child half; the pgsync::thread wrapper registered
            // this thread, so the adoption runs inside its quantum) — the
            // shared universe already carries the seeded image plus
            // everything session 1 wrote.
            match adopt {
                Some(id) => vfs::sim::SimVfs::adopt_universe(id),
                None => seed_universe(&datadir),
            }
            // Thread-local GUC store: defaults, then argv -c options, then
            // the config files — the EXEC_BACKEND-shaped restore. Wedge-
            // ledger W3: this must run BEFORE InitPostmasterChild flips
            // IsUnderPostmaster (guc_file asserts PGC_POSTMASTER-context
            // reads happen only pre-postmaster).
            guc_seams::initialize_guc_options::call().expect("s2 guc init");
            let mut dbname_arg: Option<String> = None;
            crate::switches::process_postgres_switches_dbname(
                &argv,
                ::types_guc::GucContext::PGC_POSTMASTER as i32 as u8,
                &mut dbname_arg,
            )
            .expect("s2 switches");
            if !guc_seams::select_config_files::call(
                crate::switches::user_d_option().as_deref(),
                PROGNAME,
            )
            .expect("s2 config files")
            {
                return 1;
            }
            // SIM-HARNESS-CONVERGE (PGRUST_SIMNET_LOCALSYNC=1): the corpus
            // runs NO checkpointer, so an under-PM session's forwarded sync
            // requests can never be absorbed (ForwardSyncRequest:
            // checkpointer_pid==0 -> false) and mdunlink's FORGET retry
            // loop (C sync.c's 10-ms WaitLatch) wedges any DROP-carrying
            // script — found by the first bridge plan (statement 17, DROP
            // TABLE). Fix = the C STANDALONE topology: give the session its
            // own pending-ops table so RegisterSyncRequest takes the local
            // RememberSyncRequest branch. InitSync's standalone gate reads
            // the thread-local IsUnderPostmaster, so the call must sit
            // BEFORE InitPostmasterChild flips it. Durability inside the
            // corpus rides WAL flush alone (no checkpoint runs while
            // sessions live), so never-processed local tables are correct
            // for the fault leg too. Opt-in: default off, every existing
            // corpus is op-for-op untouched.
            if std::env::var("PGRUST_SIMNET_LOCALSYNC").as_deref() == Ok("1") {
                sync_seams::init_sync::call().expect("session-local InitSync");
            }
            miscinit::InitPostmasterChild(
                init_small::globals::process_id() as i32 + spec.pid_offset,
            )
            .expect("s2 InitPostmasterChild");
            miscinit::SetMyBackendType(types_core::init::BackendType::Backend);
            // DST-PMCHILD: claim the parent-assigned pmchild Backend slot
            // BEFORE InitProcess — the slot-active protocol
            // (RegisterPostmasterChildActive / MarkPostmasterChildInactive)
            // keys off MyPMChildSlot, exactly as run_child_task does for
            // postmaster-launched children.
            if let Some(slot) = spec.child_slot {
                init_small::globals::SetMyPMChildSlot(slot);
            }
            // Wedge-ledger W5: the postmaster launches backends only after
            // recovery; here the boot thread runs StartupXLOG inside SESSION
            // 1's InitPostgres, so an early second backend reads snapshots
            // "in recovery" (unported KnownAssignedXids panic). Same gate,
            // demo-shaped: wait for the shared recovery state to clear.
            // pgsync sleep = TimedPark under the permit scheduler (a raw
            // std sleep here would hold the permit across the poll and
            // starve session 1 out of ever finishing recovery).
            let mut waited_ms = 0u32;
            while transam_xlog::RecoveryInProgress() {
                pgsync::thread::sleep(std::time::Duration::from_millis(1));
                waited_ms += 1;
                assert!(waited_ms < 60_000, "s2: recovery never completed (W5 gate)");
            }
            install_pump_for(spec.sql_env, spec.turn_id);
            run_second_session(&spec)
        }
    }
}

#[allow(non_snake_case)]
pub fn PostgresSimNetMain(argv: &[String], username: &str) -> ! {
    // ---- PERMIT-S1 demo corpora that need no server boot at all: the
    // planted race (P2) and the watchdog red fixture (P4) divert here.
    crate::sim_sched_demo::maybe_run_demo_corpus_from_env();

    // PERMIT-S1 compose wiring: register the boot/driver thread at the
    // spawn door (core hand-back: registrations from a REGISTERED thread
    // are program-ordered — the deterministic pattern). No-op unless
    // PGRUST_SIM_SCHED=1, so sim-net-e2e and dst-smoke are byte-unaffected.
    let _sched_door = pgsync::sim::spawn_door::register_self("simnet-main");

    // ---- Sim-harness plumbing BEFORE the transport-blind ladder runs.
    // Datadir: the -D argument (the ladder re-parses it itself later).
    let datadir = argv
        .iter()
        .position(|a| a == "-D")
        .and_then(|i| argv.get(i + 1))
        .cloned()
        .expect("--sim-net requires -D <datadir>");
    seed_universe(&datadir);

    // SIMVFS-SHARED (s2 §6 item 1): one filesystem universe per simulated
    // process. Opt-in per corpus (PGRUST_SIMVFS_SHARED=1) so the sessions=2
    // P1/P3 fingerprints — which DEPEND on private per-session universes —
    // are untouched by construction. The seeded universe above (datadir +
    // tz share) MOVES into the process registry as universe 1; every spawn
    // door captures/adopts it (launch_backend, loadsort feeders). Requires
    // the permit scheduler: the injected probe is the access assert, and
    // sharing without the scheduler would be a data race by construction.
    // PGRUST_SIMVFS_RED arms the deliberately-broken-sharing battery
    // (empty = the pre-lane bug resurrected, stale = frozen snapshot).
    if std::env::var("PGRUST_SIMVFS_SHARED").is_ok_and(|v| v == "1") {
        match std::env::var("PGRUST_SIMVFS_RED").as_deref() {
            Ok("empty") => {
                vfs::sim::SimVfs::arm_red_adoption(Some(vfs::sim::RedAdoption::Empty))
            }
            Ok("stale") => {
                vfs::sim::SimVfs::arm_red_adoption(Some(vfs::sim::RedAdoption::Stale))
            }
            _ => {}
        }
        // SIMCORPUS: optional red SCOPE (thread-name substring) — sabotage
        // only the matching class of adopting children (e.g. "pg:standby:"
        // = the wpool parallel workers), so the P9 red breaks the WORKERS
        // while the leader session adopts honestly.
        if let Ok(scope) = std::env::var("PGRUST_SIMVFS_RED_SCOPE") {
            if !scope.is_empty() {
                vfs::sim::SimVfs::arm_red_adoption_scope(Some(&scope));
            }
        }
        vfs::sim::SimVfs::set_shared_access_probe(pgsync::sim::current_thread_holds_permit);
        vfs::sim::SimVfs::share_universe_as(1);
    }

    // SIM-HARNESS-CONVERGE: the harness FaultDriver delivery channel
    // (PGRUST_SIM_FAULT_PLAN) — after the shared-universe setup so every
    // session's ops consult the installed plan. No-op when the env is
    // absent; existing corpora are byte-unaffected. The "armed" op report
    // gives the harness the rule counter's base: a FaultRule counts
    // matches from THIS point, while op_seq also counted the ~thousands
    // of universe-seeding writes above — the probe run reads `armed` and
    // rebases its cut choice onto the rule counter's frame.
    ops_report("armed");
    install_fault_plan_from_env();

    // SIMVFS-SHARED P8: the loadsort-prefetch corpus (no server boot —
    // pure loadsort machinery over the shared universe); never returns.
    if std::env::var("PGRUST_PERMIT_LOADSORT").is_ok_and(|v| v == "1") {
        match pgrcolumnar::loadsort::sim_demo::run_prefetch_corpus() {
            Ok(line) => {
                println!("{line}");
                std::process::exit(0);
            }
            Err(e) => {
                println!("LOADSORT-RED {e}");
                std::process::exit(1);
            }
        }
    }

    // The boot session (turn-id 1) runs the setup script to completion before
    // the workers spawn; it is never part of the cross-session schedule.
    install_pump_for("PGRUST_SIMNET_SQL", 1);

    // Transport fault plan (inc-2): PGRUST_SIMNET_FAULTS carries a
    // parse_fault_spec spec (e.g. "seed=0x5EED Read@12=drop:2"); rules are
    // op-sequence-targeted and every firing is NETFAULT-logged into the same
    // op log the determinism gate byte-compares — fault runs replay too.
    // (Installed on THIS thread: the provider state is thread-local, so the
    // plan governs session 1 — the fault arms run single-session.)
    if let Ok(spec) = std::env::var("PGRUST_SIMNET_FAULTS") {
        if !spec.trim().is_empty() {
            pqcomm_simnet::install_fault_plan_from_spec(&spec);
        }
    }

    let sessions: u32 = std::env::var("PGRUST_SIMNET_SESSIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    // ---- PERMIT-S2 F2: the wpool 2-worker demo. Needs the boot half (the
    // standby prelude restores the postmaster GUC snapshot) but no wire
    // session; never returns.
    if std::env::var("PGRUST_PERMIT_WPOOL").is_ok_and(|v| v == "1") {
        if let Err(err) = crate::stdio_wire::stdio_wire_boot_half(argv, username) {
            elog::emit_error_report_for(&err);
            std::process::exit(1);
        }
        crate::sim_sched_demo::run_wpool_demo();
    }

    // ---- PERMIT-S5: the rtpool 2-worker demo (runtime doors). Boot half
    // for the same reason as wpool (the dispatcher prelude restores the
    // postmaster GUC snapshot); never returns.
    if std::env::var("PGRUST_PERMIT_RTPOOL").is_ok_and(|v| v == "1") {
        if let Err(err) = crate::stdio_wire::stdio_wire_boot_half(argv, username) {
            elog::emit_error_report_for(&err);
            std::process::exit(1);
        }
        crate::sim_sched_demo::run_rtpool_demo();
    }

    // ---- SIMCORPUS P9: the PARALLEL-QUERY corpus (legacy Gather workers
    // over the shared universe). Three roles, one simulated process:
    //   * the BOOT thread plays postmaster + session 1 (standalone arm —
    //     it owns recovery inside its InitPostgres and runs the SETUP
    //     script), then the postmaster's REAPER while the leader runs
    //     (pm_service_pending — without it, DestroyParallelContext waits
    //     forever on slot pids only the reaper clears), then the pool
    //     drain (the P5 teardown shape);
    //   * SESSION 2 is the under-postmaster LEADER (the planner's
    //     parallel gate requires IsUnderPostmaster) — it ADOPTS the
    //     process universe and runs the parallel-query script
    //     (PGRUST_SIMNET_SQL2 / TRANSCRIPT2 / OPLOG2);
    //   * the wpool STANDBYS (ServerLoop-parity maintain from the boot
    //     thread, pristine postmaster capture, BEFORE session 1 runs) are
    //     the Gather workers: registered at their doors, adopted into the
    //     universe, claimed by the leader's LaunchParallelWorkers through
    //     the ordinary pool dispatch.
    if std::env::var("PGRUST_SIMNET_PARQUERY").is_ok_and(|v| v == "1") {
        assert!(
            vfs::sim::SimVfs::shared_universe_active(),
            "PGRUST_SIMNET_PARQUERY requires PGRUST_SIMVFS_SHARED=1 \
             (the workers read the leader's tables through the shared universe)"
        );
        if let Err(err) = crate::stdio_wire::stdio_wire_boot_half(argv, username) {
            elog::emit_error_report_for(&err);
            std::process::exit(1);
        }
        // PostmasterMain parity: the bgworker registry (main_entry.rs runs
        // it after shared memory; the standalone ladder never does) — the
        // leader's RegisterDynamicBackgroundWorker needs it.
        postmaster_seams::bgworker_shmem_init::call();
        postmaster_seams::wpool_maintain::call();
        let snap = SimInherited::capture();
        let code1 =
            run_session_on_this_thread("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
        // DST-MULTIBACKEND: the pool-miss deferral corpus
        // (PGRUST_SIMNET_POOLMISS, run with PGRUST_NO_WORKER_POOL so every
        // parallel registration misses the pool and defers to "postmaster
        // start"). "defer" = run the startup-exit promotion the PM_INIT
        // surrogate never ran (PM_RUN + conns_allowed + the postmaster
        // environment) — C's moment is the startup child's clean exit, and
        // session 1 (which owned recovery) just completed on this thread —
        // so pm_service_pending's maybe_start_bgworkers arm actually STARTS
        // the deferred worker through postmaster_child_launch's existing
        // spawn door (capture/adopt already on that path). Anything else
        // (the red) leaves the surrogate at PM_INIT: the deferred worker
        // can never start (the simcorpus §7 boundary resurrected) and the
        // virtual-time ceiling (PGRUST_SIM_VCEIL_S) names the hang
        // deterministically instead of a wall-clock timeout.
        if std::env::var("PGRUST_SIMNET_POOLMISS").as_deref() == Ok("defer") {
            postmaster_seams::pm_promote_run::call();
        }
        let adopt = vfs::sim::SimVfs::current_universe_id();
        assert!(adopt.is_some(), "parquery: boot thread lost its universe binding");
        // DST-PMCHILD: the leader is a REGISTERED pmchild Backend now — the
        // multibackend §1 zeroed-notify deviation is dead (the notify-pid
        // validity check passes; ReportBackgroundWorkerPID's SIGUSR1s reach
        // the leader instead of liveness riding the waiter cadence).
        let leader_pid = init_small::globals::process_id() as i32 + 1;
        let leader_slot = register_session_backend(leader_pid);
        let s2 = second_session_thread(
            argv.to_vec(),
            datadir.clone(),
            snap,
            adopt,
            SessionSpec::second(Some(leader_slot)),
        );
        // The reaper service loop: virtual-time quanta between reap polls
        // (a raw sleep here would hold the permit and starve the leader).
        // ServerLoop parity, both halves per iteration: reap child exits,
        // then REPLENISH the pool (serverloop.rs runs wpool::maintain every
        // iteration) — a worker that error-rotates (the red arm's shape)
        // must be replaced. A registration that still misses defers to
        // "postmaster start": the service seam's BACKGROUND_WORKER_CHANGE +
        // maybe_start_bgworkers arms honor it (found by the P9 red's
        // Gather-leader hang on NOT_YET_STARTED slots).
        // At target population (the green arm: retention re-parks the
        // standbys) maintain is an atomic-read no-op.
        // DST-PMCHILD: the loop runs until the reaper has processed the
        // leader's exit announce (modeled state; the old `s2.is_finished()`
        // OS-teardown poll — the multibackend §7 row-3 wall coupling — is
        // retired), then the hooked join.
        let budget = drain_budget();
        let qa = drain_until_reaped(&[leader_pid], budget);
        eprintln!("PMDRAIN sessions-reaped quanta={qa}");
        let code2 = s2.join().unwrap_or(101);
        // Any announce racing the leader's exit, then the pool drain.
        postmaster_seams::pm_service_pending::call();
        let qb = drain_pool(budget);
        eprintln!("PMDRAIN pool-drained population=0 quanta={qb}");
        std::process::exit(code1.max(code2))
    }

    // ---- DST-PMCHILD P13: the first N-SESSION corpus — two full SQL
    // sessions CONCURRENT under the permit scheduler over ONE shared
    // universe, both REGISTERED pmchild Backends (real child accounting).
    // This is the sim-side mirror of the H8 harness lane's client-side
    // multi-session plans; the convergence point is simharness multi-session
    // plans driven UNDER sim — the Antithesis-class end state. Roles: the
    // boot thread plays postmaster + session 1 (setup script, owns recovery
    // inside its InitPostgres), is then PROMOTED (pm_promote_run — C's
    // startup-exit moment: PM_RUN + conns_allowed + the postmaster
    // environment; backends launch under a RUNNING postmaster), registers
    // sessions 2 and 3 as pmchild Backends and spawns them CONCURRENT; the
    // scripts (PGRUST_SIMNET_SQL2 / SQL3) interleave at scheduler touches
    // only, so the whole corpus is a seeded function of PGRUST_SIM_SEED.
    // Teardown is the modeled drain (both announces reaped, hooked joins,
    // pool drain, SHUTDOWNDRAIN verdict on a stuck child).
    // PGRUST_SIM_NOREG=1 (the red) resurrects the multibackend §1 deviation:
    // anonymous session threads, no registration, no announces —
    // BackgroundWorkerStateChange zeroes the leader's bgw_notify_pid again
    // ("worker notification PID … is not valid", DEBUG1) and the drain falls
    // back to the wall-coupled is_finished polls (the pre-lane shape,
    // resurrected honestly). Requires PGRUST_SIMVFS_SHARED=1 (one filesystem
    // per simulated process — the sessions see each other's tables).
    if std::env::var("PGRUST_SIMNET_NSESSION").is_ok_and(|v| v == "1") {
        assert!(
            vfs::sim::SimVfs::shared_universe_active(),
            "PGRUST_SIMNET_NSESSION requires PGRUST_SIMVFS_SHARED=1 \
             (the sessions share one universe)"
        );
        if let Err(err) = crate::stdio_wire::stdio_wire_boot_half(argv, username) {
            elog::emit_error_report_for(&err);
            std::process::exit(1);
        }
        // PostmasterMain parity (the parquery pair): bgworker registry +
        // the warm pool — session 2's script includes a real Gather, so the
        // notify flow this corpus proves is exercised end-to-end.
        postmaster_seams::bgworker_shmem_init::call();
        postmaster_seams::wpool_maintain::call();
        let snap = SimInherited::capture();
        let code1 =
            run_session_on_this_thread("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
        // The startup-exit promotion: session 1 owned recovery and just
        // completed on this thread — C's moment for PM_RUN.
        postmaster_seams::pm_promote_run::call();
        ops_report("promote");
        // SIM-HARNESS-CONVERGE (PGRUST_SIMNET_KEEP_INPRODUCTION=1): session
        // 1 is the STANDALONE arm, so its exit just wrote a SHUTDOWN
        // checkpoint and marked pg_control DB_SHUTDOWNED — but this corpus
        // keeps SERVING (sessions 2/3 run next), which is C's
        // under-postmaster topology where the state stays DB_IN_PRODUCTION
        // until the real shutdown. Without the flip, a mid-session
        // crash-cut image reads as cleanly shut down and the reboot SKIPS
        // crash recovery — silently losing every acked commit (found by
        // fault seeds 21/53: committed CREATE+INSERT gone, no "redo
        // starts" line in the reboot log). Lock-free by quiescence: the
        // session threads are not spawned yet. Opt-in, fault legs only.
        if std::env::var("PGRUST_SIMNET_KEEP_INPRODUCTION").as_deref() == Ok("1") {
            transam_xlog::control_file::control_file_update(|cf| {
                cf.state = transam_xlog::DB_IN_PRODUCTION;
            });
            transam_xlog::control_file::UpdateControlFile()
                .expect("keep-inproduction control update");
        }
        let adopt = vfs::sim::SimVfs::current_universe_id();
        assert!(adopt.is_some(), "nsession: boot thread lost its universe binding");
        let noreg = std::env::var("PGRUST_SIM_NOREG").as_deref() == Ok("1");
        let base_pid = init_small::globals::process_id() as i32;
        // SIM-CONVERGE inc-3: the session roster generalizes to N (s4/s5
        // spawn only when their SQL env is present — the S1-SpecConflict
        // choreography's 4 plan sessions; SQL4/SQL5 absent = the exact
        // two-session P13 shape, byte-identical).
        let mut mk_specs: Vec<fn(Option<i32>) -> SessionSpec> =
            vec![SessionSpec::second, SessionSpec::third];
        if std::env::var("PGRUST_SIMNET_SQL4").is_ok() {
            mk_specs.push(SessionSpec::fourth);
            if std::env::var("PGRUST_SIMNET_SQL5").is_ok() {
                mk_specs.push(SessionSpec::fifth);
            }
        }
        let mut pids: Vec<i32> = Vec::new();
        let mut handles: Vec<pgsync::thread::JoinHandle<i32>> = Vec::new();
        for (i, mk) in mk_specs.iter().enumerate() {
            let slot = if noreg {
                None
            } else {
                let pid = base_pid + 1 + i as i32;
                pids.push(pid);
                Some(register_session_backend(pid))
            };
            handles.push(second_session_thread(
                argv.to_vec(),
                datadir.clone(),
                snap.clone(),
                adopt,
                mk(slot),
            ));
        }
        let budget = drain_budget();
        if noreg {
            // The resurrected pre-lane drain (the red rides it): OS-fact
            // polling on cadence — no announces exist to wait for.
            while handles.iter().any(|h| !h.is_finished()) {
                postmaster_seams::pm_service_pending::call();
                postmaster_seams::wpool_maintain::call();
                pgsync::thread::sleep(std::time::Duration::from_millis(1));
            }
        } else {
            let qa = drain_until_reaped(&pids, budget);
            eprintln!("PMDRAIN sessions-reaped quanta={qa}");
        }
        let mut worst = code1;
        for h in handles {
            worst = worst.max(h.join().unwrap_or(101));
        }
        postmaster_seams::pm_service_pending::call();
        let qb = drain_pool(budget);
        eprintln!("PMDRAIN pool-drained population=0 quanta={qb}");
        // SIM-HARNESS-CONVERGE: at-cut pack + evidence (no-op unless the
        // whole-node kill fired), then the op-progress report (opt-in).
        simcut_pack_and_report(&datadir);
        ops_report("final");
        std::process::exit(worst)
    }

    if sessions <= 1 {
        // ---- The single-session path (stdio_wire's inner, verbatim —
        // plus, for RUNTIME corpora only, the postmaster-parity runtime
        // wiring the serverloop would have done: gang PGPROC sizing BEFORE
        // the ladder's InitializeMaxBackends, pool start + gang spawner
        // install after shared memory exists (SIMVFS-SHARED P7: the gang
        // threads then spawn at first engagement, adopt the shared
        // universe, and read the catalog/heap through vfs). Non-runtime
        // corpora take the exact pre-lane path, byte-identical.
        let runtime_corpus = std::env::var("PGRUST_RUNTIME").as_deref() == Ok("1");
        if runtime_corpus {
            init_small::globals::SetRuntimeGangProcs(
                postmaster_seams::rtgang_procs_wanted::call(),
            );
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> core::convert::Infallible {
                let err = match (|| {
                    crate::stdio_wire::stdio_wire_boot_half(argv, username)?;
                    if runtime_corpus {
                        let _ = postmaster_seams::rtpool_start::call();
                        postmaster_seams::rtgang_install::call();
                    }
                    crate::stdio_wire::stdio_wire_session_half()
                })() {
                    Ok(never) => match never {},
                    Err(err) => err,
                };
                elog::emit_error_report_for(&err);
                ipc_seams::proc_exit::call(1, init_small::globals::MyProcPid())
            },
        ));
        let payload = match outcome {
            Ok(never) => match never {},
            Err(payload) => payload,
        };
        match payload.downcast_ref::<ipc::ProcExitThread>() {
            Some(p) => {
                // Exit callbacks (shutdown checkpoint among them) already ran
                // inline during the unwind; the session is complete — dump the
                // determinism artifacts, then take the exit.
                dump_artifacts_env("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
                simcut_pack_and_report(&datadir);
                ops_report("final");
                std::process::exit(p.code)
            }
            None => std::panic::resume_unwind(payload),
        }
    }

    // ---- PERMIT-S1 P1: the two-backend corpus (the first multi-thread sim
    // run). Boot half ONCE on this thread, then two wire sessions: this
    // thread serves session 1 (whose InitPostgres runs StartupXLOG — the
    // "postmaster + first backend" role), a spawned thread serves session 2
    // once recovery clears (W5 gate) — CONCURRENT by default.
    // PGRUST_SIMNET_DUO_SERIAL=1 is the serialized fallback rung for wedge
    // localization: session 1 completes before session 2 spawns.
    assert!(sessions == 2, "PGRUST_SIMNET_SESSIONS supports 1 or 2");
    if let Err(err) = crate::stdio_wire::stdio_wire_boot_half(argv, username) {
        elog::emit_error_report_for(&err);
        std::process::exit(1);
    }
    let snap = SimInherited::capture();
    let serial = std::env::var("PGRUST_SIMNET_DUO_SERIAL").is_ok_and(|v| v == "1");
    let (code1, code2) = if serial {
        let code1 = run_session_on_this_thread("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
        let s2 = second_session_thread(
            argv.to_vec(),
            datadir.clone(),
            snap,
            None,
            SessionSpec::second(None),
        );
        (code1, s2.join().unwrap_or(101))
    } else {
        let s2 = second_session_thread(
            argv.to_vec(),
            datadir.clone(),
            snap,
            None,
            SessionSpec::second(None),
        );
        let code1 = run_session_on_this_thread("PGRUST_SIMNET_TRANSCRIPT", "PGRUST_SIMNET_OPLOG");
        (code1, s2.join().unwrap_or(101))
    };
    std::process::exit(code1.max(code2))
}

#[allow(dead_code)]
fn _progname_used() -> &'static str {
    PROGNAME
}

#[cfg(test)]
mod fault_spec_tests {
    //! The DUT-side FaultPlanSpec reader against the EXACT shapes the
    //! harness's serde_json serialization emits (runner/faultdriver.rs).

    use super::fault_spec_json::{J, P};

    #[test]
    fn parses_the_crash_at_op_shape() {
        let j = r#"{"seed":42,"rules":[{"matcher":{"kinds":null,"class":null,"path_contains":null},"nth":17,"action":"Crash","sticky":false}]}"#;
        let v = P::new(j).value().expect("parse");
        assert_eq!(v.get("seed").and_then(|n| n.num()), Some(42));
        let J::Arr(rules) = v.get("rules").unwrap() else { panic!("rules array") };
        assert_eq!(rules.len(), 1);
        let r = &rules[0];
        assert_eq!(r.get("nth").and_then(|n| n.num()), Some(17));
        assert_eq!(r.get("action").and_then(|a| a.strv()), Some("Crash"));
        assert_eq!(r.get("sticky"), Some(&J::Bool(false)));
        assert_eq!(r.get("matcher").unwrap().get("kinds"), Some(&J::Null));
    }

    #[test]
    fn parses_kinds_class_and_nested_actions() {
        let j = r#"{"seed":7,"rules":[
            {"matcher":{"kinds":["Fsync","Fdatasync"],"class":"Wal","path_contains":"pg_wal"},
             "nth":3,"action":{"Errno":5},"sticky":true},
            {"matcher":{"kinds":["PWriteV"],"class":null,"path_contains":null},
             "nth":1,"action":{"TornWrite":{"persist_prefix":128}},"sticky":false}]}"#;
        let v = P::new(j).value().expect("parse");
        let J::Arr(rules) = v.get("rules").unwrap() else { panic!() };
        let m = rules[0].get("matcher").unwrap();
        let J::Arr(kinds) = m.get("kinds").unwrap() else { panic!() };
        assert_eq!(kinds.iter().filter_map(|k| k.strv()).collect::<Vec<_>>(), ["Fsync", "Fdatasync"]);
        assert_eq!(m.get("class").and_then(|c| c.strv()), Some("Wal"));
        assert_eq!(rules[0].get("action").unwrap().get("Errno").and_then(|n| n.num()), Some(5));
        assert_eq!(
            rules[1]
                .get("action")
                .unwrap()
                .get("TornWrite")
                .and_then(|t| t.get("persist_prefix"))
                .and_then(|n| n.num()),
            Some(128)
        );
    }

    #[test]
    fn rejects_floats_and_truncation_loudly() {
        assert!(P::new("{\"seed\":1.5}").value().is_err());
        assert!(P::new("{\"seed\":1,").value().is_err());
        assert!(P::new("{\"seed\"").value().is_err());
    }
}
