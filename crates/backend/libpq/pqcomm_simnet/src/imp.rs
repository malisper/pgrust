//! SimNet implementation (sim-cfg only). See crate docs for the contract.
//!
//! INC-2: the transport fault-injection engine. Every transport op consults
//! an installed [`NetFaultPlan`] (default [`NoNetFaults`]) exactly once per
//! op step, at the same point the op-sequence counter increments — the
//! conventions mirror the SimVfs fault-plan engine (dst/p4-faults-inc1/2):
//! seeded rule plans ([`SeededNetFaultPlan`]), nth-match firing with
//! first-rule-wins priority, SUPPRESSED notes for consumed losing firings,
//! and seq-numbered `NETFAULT`/`NOTE` lines interleaved into the same op log
//! the `NETOP` lines live in — a fault run replays from the log alone.

use std::cell::RefCell;
use std::collections::VecDeque;

use elog::ereport;
use types_error::{ErrorLocation, PgResult, ERROR};
use types_startup::{ClientSocket, Port};

/// Per-direction buffer capacity. Small enough that multi-row results
/// exercise the write-side pump/backpressure arms, large enough for whole
/// protocol messages to move per pump step. Deterministic constant.
pub const SIMNET_BUF_CAP: usize = 64 * 1024;

/// The virtual listen fd minted by the listen_server_port arm.
pub const SIMNET_LISTEN_FD: i32 = 9000;
/// The virtual per-connection fd carried in ClientSocket.
pub const SIMNET_CONN_FD: i32 = 9001;

/// Upper bound on a [`NetFaultDecision::Delay`] (in op consults). A delayed
/// segment costs one Hold op-log line per consult it outwaits; an unbounded
/// delay would be an unbounded log (and a saturated one a hang). Enforced
/// loudly at decision time — determinism prefers a panic to a wedge.
pub const SIMNET_MAX_DELAY: u64 = 4096;

/// Upper bound on consecutive Hold steps inside one blocking op. Holds only
/// occur while a delayed segment is pending, and every Hold consults (so the
/// op clock advances toward the release point); a run of Holds longer than
/// the maximum legal delay means a plan wedged the pair (e.g. a sticky Delay
/// rule deferring every consult) — deterministic panic, never a spin.
const SIMNET_HOLD_BOUND: u64 = SIMNET_MAX_DELAY + 16;

/// What a pump step reports back to the blocked server op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PumpStatus {
    /// The client may make further progress if pumped again.
    Progress,
    /// The client script is exhausted: nothing more will ever be sent.
    /// Equivalent to the client half-closing its write side.
    Finished,
    /// SIM-CONVERGE inc-2: the client made NO byte progress and is NOT
    /// finished — it is waiting on a CROSS-SESSION turn (the plan's serialized
    /// interleaving) that only advances when ANOTHER registered session runs.
    /// A distinct status from [`PumpStatus::Progress`] so the stall guard
    /// (which panics on a Progress step that moved nothing — a protocol stall)
    /// does not mistake a legal turn-wait for a wedge.
    ///
    /// CONTRACT: a pump returning `Yielded` MUST first have parked on the
    /// permit scheduler (a `pgsync::thread::sleep` = `TimedPark`), so that
    /// re-evaluating readiness advances VIRTUAL TIME toward the turn instead
    /// of tight-spinning. A genuinely unsatisfiable turn (a wedged schedule)
    /// then reaches the scheduler's virtual-time ceiling and is reported as a
    /// named `SCHEDCEILING` verdict — never this crate's deadlock panic. The
    /// provider owns no turn counter and no scheduler dependency: the pump
    /// (the corpus's `SimWireClient`, which holds the shared turn schedule)
    /// decides whose turn it is and does the park; the provider only learns
    /// that "no progress, not done" is a legal answer here.
    Yielded,
}

type Pump = Box<dyn FnMut() -> PumpStatus>;

// ===========================================================================
// The transport fault menu (inc-2) — SimVfs fault-plan conventions applied
// to the wire: op-sequence-targeted, seeded, first-rule-wins, log-replayable.
// ===========================================================================

/// Transport op kinds, exactly the `op=` vocabulary the NETOP log speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetOpKind {
    Read,
    Write,
    Noblock,
    Close,
    Init,
    Listen,
    Accept,
    ClientSend,
    ClientRecv,
    ClientClose,
    ClientConnect,
}

/// Description of the op the fault plan is consulted about.
#[derive(Debug, Clone, Copy)]
pub struct NetOpDesc {
    pub kind: NetOpKind,
    /// Which end drives the op: 'S' (server seam slots) or 'C' (client API).
    pub end: char,
    /// Bytes the op wants to move (0 where size-less).
    pub want: usize,
}

/// What the fault plan wants done to this op. The transport menu:
///
/// - `ShortRead(n)`: deliver at most n bytes on a receiving op (server
///   `Read`, `ClientRecv`) even though more is buffered — the peer's kernel
///   returned a partial read. Clamped to ≥1 when data is available (a
///   0-byte blocking read would be EOF, which is a different fault: `Drop`).
///   On a sending op it is inert (logged, no effect).
/// - `ShortWrite(n)`: accept at most n bytes on a sending op (server
///   `Write`, `ClientSend`) — a partial send. The server-side caller loops
///   (pqcomm::internal_flush_buffer), exactly as on a socket; a shorted
///   ClientSend delivers n bytes now and the remainder as an immediately
///   following segment. Inert on receiving ops.
/// - `Delay(d)`: delayed delivery, expressed as a reorder within the
///   deterministic schedule — the op's bytes are staged and released only
///   after d further op consults (sends), or the op defers one consult
///   (receives). Head-of-line order is preserved: later sends queue behind
///   a staged segment, so the byte stream never reorders internally.
/// - `Drop { keep }`: the peer connection drops mid-message — the client
///   write side dies and only the first `keep` in-flight client→server
///   bytes survive (staged segments are lost). The server drains the kept
///   bytes and then observes EOF, mid-message if `keep` cuts one.
/// - `Reset`: hard connection reset — both directions' buffered and staged
///   bytes are discarded; server reads fail ECONNRESET, server writes fail
///   EPIPE, client ops become logged no-ops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetFaultDecision {
    Proceed,
    ShortRead(usize),
    ShortWrite(usize),
    Delay(u64),
    Drop { keep: usize },
    Reset,
}

/// Consulted once per transport op step. Mutable so plans can count.
pub trait NetFaultPlan {
    fn before_op(&mut self, op: &NetOpDesc) -> NetFaultDecision;

    /// Notes to append to the op log after this op's decision (the SimVfs
    /// N5 convention: a losing rule whose nth firing was consumed by a
    /// higher-priority rule logs a SUPPRESSED line). Default: nothing.
    fn drain_notes(&mut self) -> Vec<String> {
        Vec::new()
    }
}

/// The always-proceed plan (default).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoNetFaults;

impl NetFaultPlan for NoNetFaults {
    fn before_op(&mut self, _op: &NetOpDesc) -> NetFaultDecision {
        NetFaultDecision::Proceed
    }
}

/// Op matcher for [`NetFaultRule`]. Empty (default) matches every op.
#[derive(Debug, Clone, Default)]
pub struct NetOpMatch {
    /// Restrict to these op kinds (None = any).
    pub kinds: Option<Vec<NetOpKind>>,
    /// Restrict to one end ('S'/'C'; None = any).
    pub end: Option<char>,
}

impl NetOpMatch {
    pub fn any() -> Self {
        NetOpMatch::default()
    }

    pub fn kind(kind: NetOpKind) -> Self {
        NetOpMatch { kinds: Some(vec![kind]), end: None }
    }

    fn matches(&self, op: &NetOpDesc) -> bool {
        if let Some(kinds) = &self.kinds {
            if !kinds.contains(&op.kind) {
                return false;
            }
        }
        if let Some(end) = self.end {
            if end != op.end {
                return false;
            }
        }
        true
    }
}

/// One rule of a [`SeededNetFaultPlan`]: on the `nth` (1-based) op matching
/// `matcher`, inject `action`. Non-sticky rules fire exactly once; sticky
/// rules fire on the nth and every later match.
#[derive(Debug, Clone)]
pub struct NetFaultRule {
    pub matcher: NetOpMatch,
    pub nth: u64,
    pub action: NetFaultDecision,
    pub sticky: bool,
}

impl NetFaultRule {
    /// Inject `action` on the nth op matching `matcher` (once).
    pub fn nth_matching(matcher: NetOpMatch, nth: u64, action: NetFaultDecision) -> Self {
        NetFaultRule { matcher, nth, action, sticky: false }
    }
}

/// The deterministic seeded transport fault plan — the SimVfs
/// `SeededFaultPlan` shape on the wire. Same `(seed, rules)` ⇒ the same
/// decision at the same op-sequence number, every run. Every rule counts
/// its matches independently; when several rules would fire on one op, the
/// FIRST in rule order wins (rule order is priority) and the losers' firings
/// are logged as SUPPRESSED notes. The seed is recorded in the op log
/// (`NETPLAN` line) as replay lineage; the current menu is fully explicit
/// (no seeded coins yet), so the seed feeds no decision today.
pub struct SeededNetFaultPlan {
    seed: u64,
    rules: Vec<(NetFaultRule, u64 /* matches so far */)>,
    notes: Vec<String>,
}

impl SeededNetFaultPlan {
    pub fn new(seed: u64, rules: Vec<NetFaultRule>) -> Self {
        SeededNetFaultPlan {
            seed,
            rules: rules.into_iter().map(|r| (r, 0)).collect(),
            notes: Vec::new(),
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Install as the pair's active plan, logging the NETPLAN lineage line.
    pub fn install(seed: u64, rules: Vec<NetFaultRule>) {
        let n = rules.len();
        with(|st| {
            st.plan = Box::new(SeededNetFaultPlan::new(seed, rules));
            st.op_log.push(format!("NETPLAN seed=0x{seed:016x} rules={n}"));
        });
    }
}

impl NetFaultPlan for SeededNetFaultPlan {
    fn before_op(&mut self, op: &NetOpDesc) -> NetFaultDecision {
        let mut decision = NetFaultDecision::Proceed;
        for (i, (rule, matched)) in self.rules.iter_mut().enumerate() {
            if !rule.matcher.matches(op) {
                continue;
            }
            *matched += 1;
            let fire = *matched == rule.nth || (rule.sticky && *matched > rule.nth);
            if !fire {
                continue;
            }
            if decision == NetFaultDecision::Proceed {
                decision = rule.action;
            } else {
                self.notes.push(format!(
                    "SUPPRESSED rule#{i} nth={} action={:?} (lost to an earlier rule)",
                    rule.nth, rule.action
                ));
            }
        }
        decision
    }

    fn drain_notes(&mut self) -> Vec<String> {
        std::mem::take(&mut self.notes)
    }
}

/// Parse a fault-plan spec (the `PGRUST_SIMNET_FAULTS` grammar) into
/// `(seed, rules)`. Whitespace-separated tokens:
///
/// ```text
/// seed=0x5EED                 (optional; default 0)
/// KIND@NTH[!]=ACTION[:ARG]    one rule; `!` = sticky
/// ```
///
/// KIND ∈ the NETOP `op=` vocabulary (`Read`, `Write`, `ClientSend`, ...)
/// or `Any`. ACTION ∈ `shortread:N`, `shortwrite:N`, `delay:N`, `drop:N`
/// (keep N in-flight bytes), `reset`. Examples:
///
/// ```text
/// Read@12=drop:2   ClientSend@3=delay:9   Read@1!=shortread:1
/// ```
///
/// Panics on malformed specs (harness domain: loud beats silent).
pub fn parse_fault_spec(spec: &str) -> (u64, Vec<NetFaultRule>) {
    fn parse_u64(s: &str, what: &str) -> u64 {
        let (digits, radix) =
            if let Some(hex) = s.strip_prefix("0x") { (hex, 16) } else { (s, 10) };
        u64::from_str_radix(digits, radix)
            .unwrap_or_else(|_| panic!("fault spec: bad {what} {s:?}"))
    }
    fn parse_kind(s: &str) -> Option<NetOpKind> {
        Some(match s {
            "Read" => NetOpKind::Read,
            "Write" => NetOpKind::Write,
            "Noblock" => NetOpKind::Noblock,
            "Close" => NetOpKind::Close,
            "Init" => NetOpKind::Init,
            "Listen" => NetOpKind::Listen,
            "Accept" => NetOpKind::Accept,
            "ClientSend" => NetOpKind::ClientSend,
            "ClientRecv" => NetOpKind::ClientRecv,
            "ClientClose" => NetOpKind::ClientClose,
            "ClientConnect" => NetOpKind::ClientConnect,
            "Any" => return None,
            other => panic!("fault spec: unknown op kind {other:?}"),
        })
    }

    let mut seed = 0u64;
    let mut rules = Vec::new();
    for tok in spec.split_whitespace() {
        if let Some(s) = tok.strip_prefix("seed=") {
            seed = parse_u64(s, "seed");
            continue;
        }
        let (lhs, action) =
            tok.split_once('=').unwrap_or_else(|| panic!("fault spec: bad token {tok:?}"));
        let (kind_s, nth_s) =
            lhs.split_once('@').unwrap_or_else(|| panic!("fault spec: bad target {lhs:?}"));
        let (nth_s, sticky) = match nth_s.strip_suffix('!') {
            Some(base) => (base, true),
            None => (nth_s, false),
        };
        let nth = parse_u64(nth_s, "nth");
        assert!(nth >= 1, "fault spec: nth is 1-based ({tok:?})");
        let matcher = match parse_kind(kind_s) {
            Some(k) => NetOpMatch::kind(k),
            None => NetOpMatch::any(),
        };
        let (verb, arg) = match action.split_once(':') {
            Some((v, a)) => (v, Some(a)),
            None => (action, None),
        };
        let need = |what: &str| -> u64 {
            parse_u64(arg.unwrap_or_else(|| panic!("fault spec: {verb} needs :{what}")), what)
        };
        let action = match verb {
            "shortread" => NetFaultDecision::ShortRead(need("bytes") as usize),
            "shortwrite" => NetFaultDecision::ShortWrite(need("bytes") as usize),
            "delay" => NetFaultDecision::Delay(need("ops")),
            "drop" => NetFaultDecision::Drop { keep: need("bytes") as usize },
            "reset" => NetFaultDecision::Reset,
            other => panic!("fault spec: unknown action {other:?}"),
        };
        rules.push(NetFaultRule { matcher, nth, action, sticky });
    }
    (seed, rules)
}

// ===========================================================================
// Pair state
// ===========================================================================

/// A staged (delay-held) segment: released — head-of-line, in order — once
/// `op_seq` reaches `release_at`.
struct Staged {
    release_at: u64,
    bytes: Vec<u8>,
}

struct SimNetState {
    /// client → server bytes (deliverable).
    c2s: VecDeque<u8>,
    /// server → client bytes (deliverable).
    s2c: VecDeque<u8>,
    /// Delay-staged client → server segments (deliver behind `c2s`).
    staged_c2s: VecDeque<Staged>,
    /// Delay-staged server → client segments (deliver behind `s2c`).
    staged_s2c: VecDeque<Staged>,
    /// Client write side live (false = server reads drain to EOF).
    client_open: bool,
    /// Server end live (secure_close flips it).
    server_open: bool,
    /// Hard-reset flag (NetFaultDecision::Reset fired).
    reset: bool,
    /// Server-side noblock mode bit (set_port_noblock).
    noblock: Option<bool>,
    /// The in-process client driven at server block points (serial
    /// increment). P3 replaces this with the scheduler.
    pump: Option<Pump>,
    /// The installed transport fault plan (NoNetFaults by default).
    plan: Box<dyn NetFaultPlan>,
    /// Consulted (incremented) by EVERY transport op; the op log speaks
    /// these numbers — fault rules target them.
    op_seq: u64,
    /// One NETOP line per op (+ NETPLAN/NETFAULT/NOTE lines), byte-stable
    /// across same-script same-plan replays.
    op_log: Vec<String>,
    /// Client-observed transcript: every byte the client end received
    /// (server→client wire bytes, in order).
    client_received: Vec<u8>,
    /// Every byte the client end sent (client→server wire bytes, in order).
    client_sent: Vec<u8>,
    /// Virtual pending-connection queue for the accept arm.
    pending_accepts: VecDeque<()>,
}

impl SimNetState {
    fn new() -> Self {
        SimNetState {
            c2s: VecDeque::new(),
            s2c: VecDeque::new(),
            staged_c2s: VecDeque::new(),
            staged_s2c: VecDeque::new(),
            client_open: true,
            server_open: true,
            reset: false,
            noblock: None,
            pump: None,
            plan: Box::new(NoNetFaults),
            op_seq: 0,
            op_log: Vec::new(),
            client_received: Vec::new(),
            client_sent: Vec::new(),
            pending_accepts: VecDeque::new(),
        }
    }

    /// One transport-op consult: advance the op clock, release staged
    /// segments that came due, ask the plan, log any non-Proceed decision
    /// (NETFAULT + NOTE lines carry this op's seq — the replay contract),
    /// then apply the state-transition decisions (Drop/Reset) centrally.
    fn consult(&mut self, kind: NetOpKind, end: char, want: usize) -> NetFaultDecision {
        self.op_seq += 1;
        self.release_due();
        let op = NetOpDesc { kind, end, want };
        // The plan is a field of self: take it out for the &mut call.
        let mut plan = std::mem::replace(&mut self.plan, Box::new(NoNetFaults));
        let decision = plan.before_op(&op);
        let notes = plan.drain_notes();
        self.plan = plan;
        if decision != NetFaultDecision::Proceed {
            self.op_log.push(format!(
                "NETFAULT seq={} op={kind:?} end={end} want={want} decision={decision:?}",
                self.op_seq
            ));
        }
        for note in notes {
            self.op_log.push(format!("NOTE seq={} {note}", self.op_seq));
        }
        match decision {
            NetFaultDecision::Delay(d) => assert!(
                d <= SIMNET_MAX_DELAY,
                "pqcomm_simnet: Delay({d}) exceeds SIMNET_MAX_DELAY ({SIMNET_MAX_DELAY})"
            ),
            NetFaultDecision::Drop { keep } => self.apply_drop(keep),
            NetFaultDecision::Reset => self.apply_reset(),
            _ => {}
        }
        decision
    }

    /// Head-of-line release: move staged segments whose release point has
    /// passed into the deliverable queues, stopping at the first segment
    /// still held (later segments never overtake it).
    fn release_due(&mut self) {
        while let Some(seg) = self.staged_c2s.front() {
            if seg.release_at > self.op_seq {
                break;
            }
            let seg = self.staged_c2s.pop_front().expect("front checked");
            self.c2s.extend(seg.bytes);
        }
        while let Some(seg) = self.staged_s2c.front() {
            if seg.release_at > self.op_seq {
                break;
            }
            let seg = self.staged_s2c.pop_front().expect("front checked");
            self.s2c.extend(seg.bytes);
        }
    }

    /// `Drop { keep }`: the peer vanishes mid-message. Only the first `keep`
    /// already-in-flight client→server bytes survive; staged (delayed)
    /// segments were still "in the network" and are lost with it.
    fn apply_drop(&mut self, keep: usize) {
        self.client_open = false;
        self.c2s.truncate(keep);
        self.staged_c2s.clear();
    }

    /// `Reset`: hard connection reset — nothing in flight survives, and the
    /// error surface flips (read ECONNRESET / write EPIPE).
    fn apply_reset(&mut self) {
        self.reset = true;
        self.client_open = false;
        self.c2s.clear();
        self.s2c.clear();
        self.staged_c2s.clear();
        self.staged_s2c.clear();
    }

    fn staged_c2s_bytes(&self) -> usize {
        self.staged_c2s.iter().map(|s| s.bytes.len()).sum()
    }

    fn staged_s2c_bytes(&self) -> usize {
        self.staged_s2c.iter().map(|s| s.bytes.len()).sum()
    }

    fn staged_pending(&self) -> bool {
        !self.staged_c2s.is_empty() || !self.staged_s2c.is_empty()
    }

    /// Queue bytes toward the server, behind any staged segment (stream
    /// order is sacred even under delay faults).
    fn queue_c2s(&mut self, bytes: &[u8], release_at: Option<u64>) {
        match release_at {
            Some(at) => self.staged_c2s.push_back(Staged { release_at: at, bytes: bytes.to_vec() }),
            None if self.staged_c2s.is_empty() => self.c2s.extend(bytes.iter().copied()),
            None => self
                .staged_c2s
                .push_back(Staged { release_at: self.op_seq, bytes: bytes.to_vec() }),
        }
    }

    /// Queue bytes toward the client, behind any staged segment.
    fn queue_s2c(&mut self, bytes: &[u8], release_at: Option<u64>) {
        match release_at {
            Some(at) => self.staged_s2c.push_back(Staged { release_at: at, bytes: bytes.to_vec() }),
            None if self.staged_s2c.is_empty() => self.s2c.extend(bytes.iter().copied()),
            None => self
                .staged_s2c
                .push_back(Staged { release_at: self.op_seq, bytes: bytes.to_vec() }),
        }
    }

    fn log(&mut self, op: &str, end: char, want: usize, got: isize, decision: &str) {
        let seq = self.op_seq;
        let c2s = self.c2s.len();
        let s2c = self.s2c.len();
        self.op_log.push(format!(
            "NETOP seq={seq} op={op} end={end} want={want} got={got} c2s={c2s} s2c={s2c} decision={decision}"
        ));
    }
}

thread_local! {
    static SIM: RefCell<SimNetState> = RefCell::new(SimNetState::new());
}

fn with<R>(f: impl FnOnce(&mut SimNetState) -> R) -> R {
    SIM.with(|s| f(&mut s.borrow_mut()))
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// ---------------------------------------------------------------------------
// Harness / client-end API (the other half of the duplex pair).
// ---------------------------------------------------------------------------

/// Reset the pair to a fresh state (tests; one session per universe).
pub fn reset() {
    SIM.with(|s| *s.borrow_mut() = SimNetState::new());
}

/// Register the in-process client pump driven at server block points.
pub fn install_client_pump(pump: impl FnMut() -> PumpStatus + 'static) {
    with(|st| st.pump = Some(Box::new(pump)));
}

/// Install a transport fault plan (harness API; [`NoNetFaults`] by default).
pub fn set_fault_plan(plan: Box<dyn NetFaultPlan>) {
    with(|st| st.plan = plan);
}

/// Parse a [`parse_fault_spec`] spec and install it as a seeded plan.
pub fn install_fault_plan_from_spec(spec: &str) {
    let (seed, rules) = parse_fault_spec(spec);
    SeededNetFaultPlan::install(seed, rules);
}

/// Client end: queue bytes toward the server. Unbounded acceptance is
/// deliberate on this end (the CLIENT is the pump; the server side is where
/// backpressure semantics matter), but the op consults the fault plan and is
/// logged with the real sizes, so partial-send/delay/drop plans target it.
pub fn client_send(bytes: &[u8]) {
    with(|st| {
        let decision = st.consult(NetOpKind::ClientSend, 'C', bytes.len());
        st.client_sent.extend_from_slice(bytes);
        if st.reset || !st.client_open {
            // The connection died under this very op (or earlier): the bytes
            // never reach the wire.
            st.log("ClientSend", 'C', bytes.len(), -1, "Gone");
            return;
        }
        match decision {
            NetFaultDecision::Delay(d) => {
                let at = st.op_seq.saturating_add(d);
                st.queue_c2s(bytes, Some(at));
                st.log("ClientSend", 'C', bytes.len(), bytes.len() as isize, "Delayed");
            }
            NetFaultDecision::ShortWrite(n) => {
                // Partial send: n bytes move now; the remainder follows as
                // its own (immediately releasable) segment.
                let n = n.clamp(1, bytes.len());
                st.queue_c2s(&bytes[..n], None);
                if n < bytes.len() {
                    let at = st.op_seq;
                    st.queue_c2s(&bytes[n..], Some(at));
                }
                st.log("ClientSend", 'C', bytes.len(), n as isize, "Short");
            }
            _ => {
                st.queue_c2s(bytes, None);
                st.log("ClientSend", 'C', bytes.len(), bytes.len() as isize, "Proceed");
            }
        }
    });
}

/// Client end: drain everything the server has written so far (a
/// `ShortRead` plan decision caps the drain; `Delay` defers it wholesale).
pub fn client_recv_all() -> Vec<u8> {
    with(|st| {
        let decision = st.consult(NetOpKind::ClientRecv, 'C', 0);
        let cap = match decision {
            NetFaultDecision::ShortRead(n) => n.max(1),
            NetFaultDecision::Delay(_) => 0,
            _ => usize::MAX,
        };
        let n = st.s2c.len().min(cap);
        let got: Vec<u8> = st.s2c.drain(..n).collect();
        st.client_received.extend_from_slice(&got);
        let decision_str = match decision {
            NetFaultDecision::ShortRead(_) => "Short",
            NetFaultDecision::Delay(_) => "Deferred",
            NetFaultDecision::Reset => "Gone",
            _ => "Proceed",
        };
        st.log("ClientRecv", 'C', 0, got.len() as isize, decision_str);
        got
    })
}

/// Client end: close the write side. Subsequent server reads drain the
/// remaining bytes, then observe clean EOF.
pub fn client_close() {
    with(|st| {
        let _ = st.consult(NetOpKind::ClientClose, 'C', 0);
        st.client_open = false;
        st.log("ClientClose", 'C', 0, 0, "Proceed");
    });
}

/// Queue one virtual pending connection for the accept arm (P3-facing
/// surface; unused by the single-session serial increment's session path).
pub fn client_connect() {
    with(|st| {
        let _ = st.consult(NetOpKind::ClientConnect, 'C', 0);
        st.pending_accepts.push_back(());
        st.log("ClientConnect", 'C', 0, 0, "Proceed");
    });
}

/// The deterministic op log (fault-plan-aligned line format; see crate docs).
pub fn op_log() -> Vec<String> {
    with(|st| st.op_log.clone())
}

/// Ops consulted so far (the op-sequence counter the op log speaks).
pub fn op_seq() -> u64 {
    with(|st| st.op_seq)
}

/// Full client-observed wire transcript: (bytes sent, bytes received).
pub fn client_transcript() -> (Vec<u8>, Vec<u8>) {
    with(|st| (st.client_sent.clone(), st.client_received.clone()))
}

// ---------------------------------------------------------------------------
// The deterministic park: pump the client at the block point.
// ---------------------------------------------------------------------------

/// Fingerprint of the byte/liveness state a pump step may change. op_seq is
/// deliberately EXCLUDED (inc-2, review observation 2): a pump that only
/// consults (e.g. an empty recv) makes no progress the server can ever
/// observe — counting its consults as "progress" turned protocol stalls
/// into unbounded spins. Cumulative transcript lengths make any real byte
/// movement (including delay-staged sends) register.
fn fingerprint(st: &SimNetState) -> (usize, usize, usize, usize, bool, usize, usize, usize) {
    (
        st.c2s.len(),
        st.s2c.len(),
        st.staged_c2s_bytes(),
        st.staged_s2c_bytes(),
        st.client_open,
        st.client_sent.len(),
        st.client_received.len(),
        st.pending_accepts.len(),
    )
}

/// Run one client pump step OUTSIDE the state borrow (the pump re-enters
/// through the client_* API). Returns the step's status; `Finished` marks
/// the client write side closed.
fn pump_once(what: &str) -> PumpStatus {
    let (mut pump, before) = with(|st| (st.pump.take(), fingerprint(st)));
    let Some(p) = pump.as_mut() else {
        // No client registered: a blocked op can never make progress. The
        // serial contract turns this into clean EOF semantics, not a hang.
        return PumpStatus::Finished;
    };
    let status = p();
    with(|st| {
        st.pump = pump;
        match status {
            PumpStatus::Finished => st.client_open = false,
            // SIM-CONVERGE inc-2: a Yielded step is a LEGAL no-progress turn-
            // wait (see the PumpStatus::Yielded contract). The pump already
            // parked on the scheduler, so re-evaluating readiness advances
            // virtual time — a wedged turn reaches SCHEDCEILING (a named
            // verdict), never the deadlock panic below.
            PumpStatus::Yielded => {}
            PumpStatus::Progress => {
                if fingerprint(st) == before {
                    // Deterministic deadlock detection: a Progress step that
                    // moved nothing would spin forever; fail loudly and
                    // reproducibly.
                    panic!("pqcomm_simnet: client pump stalled during blocking {what} (deterministic deadlock)");
                }
            }
        }
    });
    status
}

/// Guard a Hold loop (waiting out a delayed segment): every Hold consults,
/// so the op clock strictly advances toward the release point; more Holds
/// than any legal delay = a wedged plan, and the contract prefers a
/// deterministic panic to a spin.
fn check_hold_bound(holds: u64, what: &str) {
    assert!(
        holds <= SIMNET_HOLD_BOUND,
        "pqcomm_simnet: blocking {what} held past SIMNET_HOLD_BOUND ({SIMNET_HOLD_BOUND}) — wedged fault plan"
    );
}

// ---------------------------------------------------------------------------
// Server end: the seam-slot implementations.
// ---------------------------------------------------------------------------

/// secure_read over the pair: readiness is a pure function of `c2s` bytes +
/// client liveness. Interrupt-processing shape mirrors the other providers.
pub fn secure_read(buf: &mut [u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_read_interrupt::call(false)?;

    let want = buf.len();
    let mut holds = 0u64;
    let res = loop {
        enum Step {
            Got(usize),
            Eof,
            Reset,
            WouldBlock,
            Hold,
            Park,
        }
        let step = with(|st| {
            let decision = st.consult(NetOpKind::Read, 'S', want);
            if st.reset {
                st.log("Read", 'S', want, -1, "Reset");
                return Step::Reset;
            }
            let deferred = matches!(decision, NetFaultDecision::Delay(_));
            let cap = match decision {
                // A short read still reads: ≥1 when data is available.
                NetFaultDecision::ShortRead(n) => n.max(1),
                _ => usize::MAX,
            };
            if !st.c2s.is_empty() && !deferred {
                let n = want.min(st.c2s.len()).min(cap);
                for b in buf.iter_mut().take(n) {
                    *b = st.c2s.pop_front().expect("len checked");
                }
                let short = matches!(decision, NetFaultDecision::ShortRead(_));
                st.log("Read", 'S', want, n as isize, if short { "Short" } else { "Proceed" });
                Step::Got(n)
            } else if deferred || !st.staged_c2s.is_empty() {
                // Delayed delivery: either this very consult was deferred,
                // or bytes exist whose release point is in the future. A
                // noblock read reports "not ready now"; a blocking read
                // waits it out — every consult advances the op clock, so
                // release is a bounded number of Holds away. Pumping here
                // could only run a byte-silent client step (stall panic).
                if st.noblock.unwrap_or(false) {
                    st.log("Read", 'S', want, -1, "WouldBlock");
                    Step::WouldBlock
                } else {
                    st.log("Read", 'S', want, -1, "Hold");
                    Step::Hold
                }
            } else if !st.client_open {
                // Empty peer buffer + no live writer = clean session end.
                st.log("Read", 'S', want, 0, "Eof");
                Step::Eof
            } else if st.noblock.unwrap_or(false) {
                st.log("Read", 'S', want, -1, "WouldBlock");
                Step::WouldBlock
            } else if !st.staged_s2c.is_empty() {
                // Our own delayed output hasn't reached the client yet; a
                // pump now could legally be byte-silent (the client is
                // waiting for those bytes). Hold until release, then pump.
                st.log("Read", 'S', want, -1, "Hold");
                Step::Hold
            } else {
                st.log("Read", 'S', want, -1, "Park");
                Step::Park
            }
        });
        match step {
            Step::Got(n) => break Ok(n),
            Step::Eof => break Ok(0),
            Step::Reset => break Err(libc::ECONNRESET),
            Step::WouldBlock => break Err(libc::EWOULDBLOCK),
            Step::Hold => {
                holds += 1;
                check_hold_bound(holds, "read");
            }
            Step::Park => {
                holds = 0;
                // Deterministic park: drive the client; loop re-evaluates
                // readiness (Finished flips client_open → EOF next pass).
                let _ = pump_once("read");
            }
        }
    };

    postgres_seams::process_client_read_interrupt::call(false)?;

    Ok(res)
}

/// secure_write over the pair: capacity is a pure function of `s2c` room
/// (staged delay segments count against it — they are in the pipe). Partial
/// writes are the caller's loop (pqcomm::internal_flush_buffer), as with
/// every provider.
pub fn secure_write(buf: &[u8]) -> PgResult<Result<usize, i32>> {
    postgres_seams::process_client_write_interrupt::call(false)?;

    let want = buf.len();
    let mut holds = 0u64;
    let res = loop {
        enum Step {
            Put(usize),
            Pipe,
            Reset,
            WouldBlock,
            Hold,
            Park,
        }
        let step = with(|st| {
            let decision = st.consult(NetOpKind::Write, 'S', want);
            if st.reset {
                st.log("Write", 'S', want, -1, "Reset");
                return Step::Reset;
            }
            let cap = match decision {
                NetFaultDecision::ShortWrite(n) => n.max(1),
                _ => usize::MAX,
            };
            let free = SIMNET_BUF_CAP.saturating_sub(st.s2c.len() + st.staged_s2c_bytes());
            if free > 0 {
                let n = want.min(free).min(cap);
                match decision {
                    NetFaultDecision::Delay(d) => {
                        let at = st.op_seq.saturating_add(d);
                        st.queue_s2c(&buf[..n], Some(at));
                        st.log("Write", 'S', want, n as isize, "Delayed");
                    }
                    NetFaultDecision::ShortWrite(_) => {
                        st.queue_s2c(&buf[..n], None);
                        st.log("Write", 'S', want, n as isize, "Short");
                    }
                    _ => {
                        st.queue_s2c(&buf[..n], None);
                        st.log("Write", 'S', want, n as isize, "Proceed");
                    }
                }
                Step::Put(n)
            } else if !st.client_open {
                // Peer gone with the buffer full: the socket arm's EPIPE.
                st.log("Write", 'S', want, -1, "Pipe");
                Step::Pipe
            } else if st.noblock.unwrap_or(false) {
                st.log("Write", 'S', want, -1, "WouldBlock");
                Step::WouldBlock
            } else if st.staged_pending() {
                st.log("Write", 'S', want, -1, "Hold");
                Step::Hold
            } else {
                st.log("Write", 'S', want, -1, "Park");
                Step::Park
            }
        });
        match step {
            Step::Put(n) => break Ok(n),
            Step::Pipe => break Err(libc::EPIPE),
            Step::Reset => break Err(libc::EPIPE),
            Step::WouldBlock => break Err(libc::EWOULDBLOCK),
            Step::Hold => {
                holds += 1;
                check_hold_bound(holds, "write");
            }
            Step::Park => {
                holds = 0;
                let _ = pump_once("write");
            }
        }
    };

    postgres_seams::process_client_write_interrupt::call(false)?;

    Ok(res)
}

fn set_port_noblock(nb: bool) -> bool {
    with(|st| {
        if st.noblock.is_none() {
            // Mirrors the other providers' "no client connection" answer
            // before pq_init.
            return false;
        }
        let _ = st.consult(NetOpKind::Noblock, 'S', nb as usize);
        st.noblock = Some(nb);
        st.log("Noblock", 'S', nb as usize, 0, "Proceed");
        true
    })
}

fn secure_close() {
    with(|st| {
        let _ = st.consult(NetOpKind::Close, 'S', 0);
        st.server_open = false;
        st.log("Close", 'S', 0, 0, "Proceed");
    });
}

/// pq_init, sim shape: no socket, no wait set (readiness never parks on the
/// OS — blocking is the deterministic pump above). Zeroed addresses =
/// "client address unknown", as on the stdio provider.
fn pq_init(client_sock: &ClientSocket) -> PgResult<Port> {
    let port = Port::new(client_sock);
    pqcomm::pq_init_buffers()?;
    with(|st| {
        st.noblock = Some(false);
        let _ = st.consult(NetOpKind::Init, 'S', 0);
        st.log("Init", 'S', 0, 0, "Proceed");
    });
    Ok(port)
}

fn modify_fe_be_wait_set_latch(_latch: types_storage::latch::LatchHandle) -> PgResult<()> {
    Ok(())
}

/// A ClientSocket bound to the pair (virtual fd; zeroed raddr).
pub fn simnet_client_socket() -> ClientSocket {
    ClientSocket { sock: SIMNET_CONN_FD, raddr: ip_zeroed() }
}

fn ip_zeroed() -> ip::SockAddr {
    ip::SockAddr::zeroed()
}

// ---------------------------------------------------------------------------
// Provider install.
// ---------------------------------------------------------------------------

/// Install the sim-net provider into the transport seam slots — the third
/// provider; same boot-time counterpart shape as
/// `pqcomm_stdio::init_transport_seams` / the socket half of
/// `pqcomm::init_socket_seams`. Exactly one provider installs per process
/// (seam_core's install-twice panic enforces it).
///
/// The listen/accept pair installs VIRTUAL arms (the reason
/// pqcomm::init_seams was split): listen mints [`SIMNET_LISTEN_FD`]; accept
/// pops connections queued by [`client_connect`]. The single-session serial
/// increment's session path does not go through them (no postmaster), but
/// the slots are owned and logged so the P3 scheduler can drive
/// multi-session accepts through the same choke points.
pub fn init_transport_seams() {
    be_secure_seams::secure_read::set(secure_read);
    be_secure_seams::secure_write::set(secure_write);
    be_secure_seams::secure_close::set(secure_close);
    be_secure_seams::set_port_noblock::set(set_port_noblock);
    be_secure_seams::be_tls_get_certificate_hash::set(|| {
        ereport(ERROR)
            .errmsg_internal("channel binding is not supported on the sim-net transport")
            .finish(loc("be_tls_get_certificate_hash"))
            .map(|()| Vec::new())
    });
    pqcomm_seams::pq_init::set(pq_init);
    pqcomm_seams::modify_fe_be_wait_set_latch::set(modify_fe_be_wait_set_latch);
    pqcomm_seams::listen_server_port::set(|_host, _port, _dir, listen_sockets, _max| {
        with(|st| {
            let _ = st.consult(NetOpKind::Listen, 'S', 0);
            st.log("Listen", 'S', 0, SIMNET_LISTEN_FD as isize, "Proceed");
        });
        listen_sockets.push(SIMNET_LISTEN_FD);
        Ok(())
    });
    pqcomm_seams::accept_connection::set(|_server_fd| {
        let pending = with(|st| {
            let _ = st.consult(NetOpKind::Accept, 'S', 0);
            let got = st.pending_accepts.pop_front().is_some();
            st.log("Accept", 'S', 0, if got { SIMNET_CONN_FD as isize } else { -1 }, if got { "Proceed" } else { "WouldBlock" });
            got
        });
        if pending {
            Ok(simnet_client_socket())
        } else {
            Err(Box::new(types_error::PgError::new(
                types_error::LOG,
                "no pending sim-net connection",
            )))
        }
    });
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
