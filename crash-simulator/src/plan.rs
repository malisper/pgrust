//! Plan-format v1 (serial single-session) — the H1 contract's stable interface.
//!
//! Owner: WS-GEN. FROZEN after inc-1 (contract §5): post-inc-1 changes need a
//! format-rev note + WS-ORACLE/WS-RUNNER ack.
//!
//! The `.plan` file is the annotated-SQL rendering and IS the failure artifact.
//! Round-trip law: `parse(render(plan)) == plan`, bit-exact on the IR
//! (property-tested in tests/simharness_gen_roundtrip.rs).
//!
//! Format notes beyond the contract §1.5 annotation list (all one-line,
//! machine-parseable, recorded in notes/harness-h1-gen.md):
//!   - `-- STEP ddl|dml|query` precedes every Sql step (kind is explicit, never
//!     re-derived by a classifier — classifier drift would break the round-trip
//!     law on property-supplied SQL).
//!   - `-- MARK <mark> [order-underdetermined] [float-lenient]` carries the
//!     Sql screens metadata as fixed-order optional flags.
//!   - `-- begin property '<name>' seq=<n> [tables=<id>,..]` carries the
//!     property's touched-table set in BIRTH-ID space (rename-chase is thereby
//!     automatic for the shrinker: birth ids never change across RENAME).
//! Reserved tags:
//!   - `-- SESSION ...` (H8): the multi-session estate. A plan that uses any
//!     session-family step (`Session`/`AsyncDml`/`Join`/`WaitUntil`) renders
//!     under the v2 header; a plan without them renders BYTE-IDENTICAL v1
//!     (all pre-H8 records stay stable). Parsing a session-family step under
//!     a v1 header stays the old hard error (the §0 A1 guarantee for v1
//!     files is preserved verbatim).
//!   - `Fault(Crash|TornWrite|Env)` render+parse OK; `FaultPoint::executable_v1`
//!     is false — the runner must refuse execution (H4 tags).

use std::collections::BTreeSet;
use std::fmt::Write as _;

pub const FORMAT_VERSION: u32 = 2;
const HEADER_LINE_1: &str = "-- simharness plan v1 (serial single-session)";
const HEADER_LINE_2V: &str = "-- simharness plan v2 (multi-session)";

/// Highest addressable session id (session 0 = the primary pair). The p3
/// collision shape needs 4 sessions (ctrl + horizon-pin + two inserters);
/// the cap leaves headroom without inviting unbounded fan-out.
pub const MAX_SESSION_ID: u32 = 5;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanError {
    pub line: usize, // 1-based; 0 = not line-specific
    pub msg: String,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.line > 0 {
            write!(f, "plan parse error at line {}: {}", self.line, self.msg)
        } else {
            write!(f, "plan error: {}", self.msg)
        }
    }
}

impl std::error::Error for PlanError {}

fn err<T>(line: usize, msg: impl Into<String>) -> Result<T, PlanError> {
    Err(PlanError { line, msg: msg.into() })
}

// ---------------------------------------------------------------------------
// IR
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanHeader {
    pub seed: u64,
    /// Profile name: non-empty, no whitespace.
    pub profile: String,
    /// Lowercase hex sha256 of the profile file bytes.
    pub profile_sha256: String,
    /// Generator version (git sha or "unknown"); non-empty, no whitespace.
    pub generator: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mark {
    Read,
    Mutation,
    Passthrough,
}

impl Mark {
    pub fn as_str(self) -> &'static str {
        match self {
            Mark::Read => "read",
            Mark::Mutation => "mutation",
            Mark::Passthrough => "passthrough",
        }
    }
}

/// Screens metadata carried on every Sql (contract §2.1.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlFlags {
    /// R2: result order not determined by a same-depth unique-key ORDER BY
    /// while LIMIT/OFFSET is present — ladder must compare shape only.
    pub order_underdetermined: bool,
    /// R7: float aggregate in a compared position, allowed only under a
    /// float-lenient profile; ladder applies epsilon/::numeric rules.
    pub float_lenient: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sql {
    text: String,
    pub mark: Mark,
    pub flags: SqlFlags,
}

impl Sql {
    /// Single-line SQL, non-empty, not a comment, `;`-terminated.
    pub fn new(text: impl Into<String>, mark: Mark, flags: SqlFlags) -> Result<Self, PlanError> {
        let text = text.into();
        if text.is_empty() {
            return err(0, "sql text empty");
        }
        if text.contains('\n') || text.contains('\r') {
            return err(0, "sql text must be single-line");
        }
        if text.starts_with("--") {
            return err(0, "sql text may not start with a comment");
        }
        if !text.ends_with(';') {
            return err(0, "sql text must end with ';'");
        }
        Ok(Sql { text, mark, flags })
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsoLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxCtl {
    Begin(IsoLevel),
    Commit,
    Rollback,
    Savepoint(String),
    RollbackTo(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArmCtl {
    /// `SET <k> = <v>` — serial-safe arm. RESET ALL law: plans that set GUCs
    /// end with ResetAll (generator invariant, not a format rule).
    SetGuc(String, String),
    ResetAll,
}

/// A result-stack check. Payload is a single-line JSON document; semantics are
/// WS-ORACLE's. The plan layer stores/round-trips it verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    json: String,
}

impl Check {
    pub fn new(json: impl Into<String>) -> Result<Self, PlanError> {
        let json = json.into();
        if json.contains('\n') || json.contains('\r') {
            return err(0, "check json must be single-line");
        }
        if serde_json::from_str::<serde_json::Value>(&json).is_err() {
            return err(0, "check payload is not valid JSON");
        }
        Ok(Check { json })
    }

    pub fn json(&self) -> &str {
        &self.json
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultPoint {
    /// v1 executable.
    Disconnect,
    ReconnectServer,
    /// Reserved (H4): render+parse OK, execution must refuse.
    Crash(String),
    TornWrite,
    Env(String),
}

impl FaultPoint {
    pub fn executable_v1(&self) -> bool {
        matches!(self, FaultPoint::Disconnect | FaultPoint::ReconnectServer)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Ddl(Sql),
    Dml(Sql),
    Query(Sql),
    Tx(TxCtl),
    Arm(ArmCtl),
    Assumption(Check),
    Assertion(Check),
    Fault(FaultPoint),
    /// H8: switch the active session (0 = the primary pair; workers are
    /// connected lazily). Part of plan bytes — the interleaving schedule IS
    /// serialized, so replay is deterministic.
    Session(u32),
    /// H8: dispatch a DML on the active session WITHOUT waiting for its
    /// completion (the statement is expected to block; a later `Join`
    /// collects the outcome). Only valid on a worker session (id >= 1).
    AsyncDml(Sql),
    /// H8: collect session k's outstanding async statement (bounded wait);
    /// the joined outcome flows through the normal classification.
    Join(u32),
    /// H8: poll the active session with this query until it returns the
    /// single scalar 't' (bounded) — the observable-state gate the isolation
    /// choreography needs (never fixed sleeps; the specconflict-e2e lesson).
    WaitUntil(Sql),
}

impl Step {
    /// Session-family steps force the v2 header.
    pub fn is_v2(&self) -> bool {
        matches!(
            self,
            Step::Session(_) | Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanItem {
    Step(Step),
    /// A property instance compiled to steps. `tables` is the touched-table
    /// set in birth-id space (table-dependency API for the shrinker).
    Property {
        name: String,
        seq: u32,
        tables: BTreeSet<String>,
        steps: Vec<Step>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub header: PlanHeader,
    pub items: Vec<PlanItem>,
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

fn is_ident(s: &str) -> bool {
    !s.is_empty()
        && s.chars().next().is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && s.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn no_ws(s: &str) -> bool {
    !s.is_empty() && !s.chars().any(|c| c.is_whitespace())
}

fn validate_header(h: &PlanHeader) -> Result<(), PlanError> {
    if !no_ws(&h.profile) {
        return err(0, "header profile must be non-empty, no whitespace");
    }
    if h.profile_sha256.is_empty()
        || !h.profile_sha256.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return err(0, "header profile-sha256 must be lowercase hex");
    }
    if !no_ws(&h.generator) {
        return err(0, "header generator must be non-empty, no whitespace");
    }
    Ok(())
}

fn validate_step(s: &Step) -> Result<(), PlanError> {
    match s {
        Step::Tx(TxCtl::Savepoint(n)) | Step::Tx(TxCtl::RollbackTo(n)) => {
            if !is_ident(n) {
                return err(0, format!("savepoint name '{n}' is not a lowercase identifier"));
            }
        }
        Step::Arm(ArmCtl::SetGuc(k, v)) => {
            if !is_ident(&k.replace('.', "_")) {
                return err(0, format!("guc name '{k}' invalid"));
            }
            if v.is_empty() || v.contains('\n') || v.contains('\r') || v.contains(' ') {
                return err(0, format!("guc value '{v}' must be non-empty, no spaces/newlines"));
            }
        }
        Step::Fault(FaultPoint::Crash(p)) | Step::Fault(FaultPoint::Env(p)) => {
            if !no_ws(p) {
                return err(0, "fault payload must be non-empty, no whitespace");
            }
        }
        Step::Session(id) | Step::Join(id) => {
            if *id > MAX_SESSION_ID {
                return err(0, format!("session id {id} exceeds MAX_SESSION_ID {MAX_SESSION_ID}"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_property(name: &str, tables: &BTreeSet<String>) -> Result<(), PlanError> {
    if name.is_empty() || name.contains('\'') || name.contains('\n') {
        return err(0, format!("property name '{name}' invalid"));
    }
    for t in tables {
        if !is_ident(t) {
            return err(0, format!("property table id '{t}' is not a lowercase identifier"));
        }
    }
    Ok(())
}

pub fn validate(plan: &Plan) -> Result<(), PlanError> {
    validate_header(&plan.header)?;
    for item in &plan.items {
        match item {
            PlanItem::Step(s) => validate_step(s)?,
            PlanItem::Property { name, tables, steps, .. } => {
                validate_property(name, tables)?;
                for s in steps {
                    validate_step(s)?;
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

fn render_sql(out: &mut String, kind: &str, sql: &Sql) {
    let _ = writeln!(out, "-- STEP {kind}");
    let mut mark = format!("-- MARK {}", sql.mark.as_str());
    if sql.flags.order_underdetermined {
        mark.push_str(" order-underdetermined");
    }
    if sql.flags.float_lenient {
        mark.push_str(" float-lenient");
    }
    let _ = writeln!(out, "{mark}");
    let _ = writeln!(out, "{}", sql.text);
}

fn render_step(out: &mut String, step: &Step) {
    match step {
        Step::Ddl(s) => render_sql(out, "ddl", s),
        Step::Dml(s) => render_sql(out, "dml", s),
        Step::Query(s) => render_sql(out, "query", s),
        Step::Tx(tx) => {
            let t = match tx {
                TxCtl::Begin(IsoLevel::ReadCommitted) => "begin-rc".to_string(),
                TxCtl::Begin(IsoLevel::RepeatableRead) => "begin-rr".to_string(),
                TxCtl::Begin(IsoLevel::Serializable) => "begin-ser".to_string(),
                TxCtl::Commit => "commit".to_string(),
                TxCtl::Rollback => "rollback".to_string(),
                TxCtl::Savepoint(n) => format!("savepoint {n}"),
                TxCtl::RollbackTo(n) => format!("rollback-to {n}"),
            };
            let _ = writeln!(out, "-- TX {t}");
        }
        Step::Arm(arm) => {
            let a = match arm {
                ArmCtl::SetGuc(k, v) => format!("set {k}={v}"),
                ArmCtl::ResetAll => "reset-all".to_string(),
            };
            let _ = writeln!(out, "-- ARM {a}");
        }
        Step::Assumption(c) => {
            let _ = writeln!(out, "-- ASSUME {}", c.json);
        }
        Step::Assertion(c) => {
            let _ = writeln!(out, "-- ASSERT {}", c.json);
        }
        Step::Fault(f) => {
            let t = match f {
                FaultPoint::Disconnect => "disconnect".to_string(),
                FaultPoint::ReconnectServer => "reconnect-server".to_string(),
                FaultPoint::Crash(p) => format!("crash {p}"),
                FaultPoint::TornWrite => "torn-write".to_string(),
                FaultPoint::Env(p) => format!("env {p}"),
            };
            let _ = writeln!(out, "-- FAULT {t}");
        }
        Step::Session(id) => {
            let _ = writeln!(out, "-- SESSION switch {id}");
        }
        Step::Join(id) => {
            let _ = writeln!(out, "-- SESSION join {id}");
        }
        Step::AsyncDml(sql) => {
            // Async DML carries the same MARK line as a synchronous statement
            // (mutation), so the round-trip law reuses the marked-SQL parser.
            let _ = writeln!(out, "-- SESSION async-dml");
            let mut mark = format!("-- MARK {}", sql.mark.as_str());
            if sql.flags.order_underdetermined {
                mark.push_str(" order-underdetermined");
            }
            if sql.flags.float_lenient {
                mark.push_str(" float-lenient");
            }
            let _ = writeln!(out, "{mark}");
            let _ = writeln!(out, "{}", sql.text);
        }
        Step::WaitUntil(sql) => {
            let _ = writeln!(out, "-- SESSION wait-until");
            let mut mark = format!("-- MARK {}", sql.mark.as_str());
            if sql.flags.order_underdetermined {
                mark.push_str(" order-underdetermined");
            }
            if sql.flags.float_lenient {
                mark.push_str(" float-lenient");
            }
            let _ = writeln!(out, "{mark}");
            let _ = writeln!(out, "{}", sql.text);
        }
    }
}

/// True when any step forces the v2 (multi-session) header. A plan built
/// entirely from serial steps stays byte-identical to a pre-H8 v1 render.
pub fn plan_is_v2(plan: &Plan) -> bool {
    plan.items.iter().any(|item| match item {
        PlanItem::Step(s) => s.is_v2(),
        PlanItem::Property { steps, .. } => steps.iter().any(Step::is_v2),
    })
}

/// Render a plan to its annotated-SQL `.plan` text. Deterministic: identical
/// plans render to identical bytes (G-G3 rides on this).
pub fn render(plan: &Plan) -> String {
    let mut out = String::new();
    if plan_is_v2(plan) {
        let _ = writeln!(out, "{HEADER_LINE_2V}");
    } else {
        let _ = writeln!(out, "{HEADER_LINE_1}");
    }
    let _ = writeln!(
        out,
        "-- seed: {} profile: {} profile-sha256: {} generator: {}",
        plan.header.seed, plan.header.profile, plan.header.profile_sha256, plan.header.generator
    );
    for item in &plan.items {
        out.push('\n');
        match item {
            PlanItem::Step(step) => render_step(&mut out, step),
            PlanItem::Property { name, seq, tables, steps } => {
                let mut line = format!("-- begin property '{name}' seq={seq}");
                if !tables.is_empty() {
                    let list: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
                    let _ = write!(line, " tables={}", list.join(","));
                }
                let _ = writeln!(out, "{line}");
                for step in steps {
                    render_step(&mut out, step);
                }
                let _ = writeln!(out, "-- end property seq={seq}");
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Parse
// ---------------------------------------------------------------------------

struct Lines<'a> {
    lines: Vec<&'a str>,
    pos: usize,
}

impl<'a> Lines<'a> {
    fn peek(&self) -> Option<&'a str> {
        self.lines.get(self.pos).copied()
    }
    fn next(&mut self) -> Option<&'a str> {
        let l = self.peek();
        if l.is_some() {
            self.pos += 1;
        }
        l
    }
    fn lineno(&self) -> usize {
        self.pos + 1
    }
}

/// Parse the header, returning the plan header plus whether the file declared
/// the v2 (multi-session) format. A v1 header keeps every v1 guarantee: the
/// SESSION-family tags stay hard errors under it (§0 A1).
fn parse_header(lx: &mut Lines) -> Result<(PlanHeader, bool), PlanError> {
    let l1 = lx.next().ok_or(PlanError { line: 1, msg: "empty plan".into() })?;
    let is_v2 = l1 == HEADER_LINE_2V;
    if l1 != HEADER_LINE_1 && !is_v2 {
        // Version discipline: any other version line is a hard, named error.
        if l1.starts_with("-- simharness plan ") {
            return err(1, format!("unsupported plan format version line: '{l1}'"));
        }
        return err(1, "missing plan header line 1");
    }
    let ln = lx.lineno();
    let l2 = lx.next().ok_or(PlanError { line: ln, msg: "missing header line 2".into() })?;
    let rest = l2
        .strip_prefix("-- seed: ")
        .ok_or(PlanError { line: ln, msg: "header line 2 must start '-- seed: '".into() })?;
    let toks: Vec<&str> = rest.split(' ').collect();
    if toks.len() != 7
        || toks[1] != "profile:"
        || toks[3] != "profile-sha256:"
        || toks[5] != "generator:"
    {
        return err(ln, "malformed header line 2");
    }
    let seed: u64 = toks[0]
        .parse()
        .map_err(|_| PlanError { line: ln, msg: format!("bad seed '{}'", toks[0]) })?;
    let header = PlanHeader {
        seed,
        profile: toks[2].to_string(),
        profile_sha256: toks[4].to_string(),
        generator: toks[6].to_string(),
    };
    validate_header(&header).map_err(|e| PlanError { line: ln, msg: e.msg })?;
    Ok((header, is_v2))
}

fn parse_marked_sql(lx: &mut Lines, kind: &str) -> Result<Step, PlanError> {
    let ln = lx.lineno();
    let mark_line = lx
        .next()
        .ok_or(PlanError { line: ln, msg: "expected '-- MARK' after '-- STEP'".into() })?;
    let rest = mark_line
        .strip_prefix("-- MARK ")
        .ok_or(PlanError { line: ln, msg: format!("expected '-- MARK ...', got '{mark_line}'") })?;
    let mut toks = rest.split(' ');
    let mark = match toks.next() {
        Some("read") => Mark::Read,
        Some("mutation") => Mark::Mutation,
        Some("passthrough") => Mark::Passthrough,
        other => return err(ln, format!("bad mark '{}'", other.unwrap_or(""))),
    };
    let mut flags = SqlFlags::default();
    for t in toks {
        match t {
            "order-underdetermined" => {
                if flags.order_underdetermined {
                    return err(ln, "duplicate flag");
                }
                flags.order_underdetermined = true;
            }
            "float-lenient" => {
                if flags.float_lenient {
                    return err(ln, "duplicate flag");
                }
                flags.float_lenient = true;
            }
            other => return err(ln, format!("unknown mark flag '{other}'")),
        }
    }
    // Flags render in fixed order; reject out-of-order to keep the canonical
    // form unique (render∘parse = identity on well-formed files).
    if flags.order_underdetermined && flags.float_lenient {
        let expect = "-- MARK ".to_string() + mark.as_str() + " order-underdetermined float-lenient";
        if mark_line != expect {
            return err(ln, "mark flags out of canonical order");
        }
    }
    let ln = lx.lineno();
    let sql_line = lx
        .next()
        .ok_or(PlanError { line: ln, msg: "expected SQL line after '-- MARK'".into() })?;
    if sql_line.starts_with("--") || sql_line.is_empty() {
        return err(ln, format!("expected SQL statement line, got '{sql_line}'"));
    }
    let sql = Sql::new(sql_line, mark, flags).map_err(|e| PlanError { line: ln, msg: e.msg })?;
    Ok(match kind {
        "ddl" => Step::Ddl(sql),
        "dml" => Step::Dml(sql),
        "query" => Step::Query(sql),
        _ => unreachable!(),
    })
}

/// Parse a `-- MARK`/SQL pair (already consumed the annotation line above it):
/// the shared body of async-dml and wait-until session steps.
fn parse_mark_and_sql(lx: &mut Lines) -> Result<Sql, PlanError> {
    let ln = lx.lineno();
    let mark_line = lx
        .next()
        .ok_or(PlanError { line: ln, msg: "expected '-- MARK' after '-- SESSION'".into() })?;
    let rest = mark_line
        .strip_prefix("-- MARK ")
        .ok_or(PlanError { line: ln, msg: format!("expected '-- MARK ...', got '{mark_line}'") })?;
    let mut toks = rest.split(' ');
    let mark = match toks.next() {
        Some("read") => Mark::Read,
        Some("mutation") => Mark::Mutation,
        Some("passthrough") => Mark::Passthrough,
        other => return err(ln, format!("bad mark '{}'", other.unwrap_or(""))),
    };
    let mut flags = SqlFlags::default();
    for t in toks {
        match t {
            "order-underdetermined" => flags.order_underdetermined = true,
            "float-lenient" => flags.float_lenient = true,
            other => return err(ln, format!("unknown mark flag '{other}'")),
        }
    }
    let ln = lx.lineno();
    let sql_line = lx
        .next()
        .ok_or(PlanError { line: ln, msg: "expected SQL line after '-- MARK'".into() })?;
    if sql_line.starts_with("--") || sql_line.is_empty() {
        return err(ln, format!("expected SQL statement line, got '{sql_line}'"));
    }
    Sql::new(sql_line, mark, flags).map_err(|e| PlanError { line: ln, msg: e.msg })
}

/// Parse one step whose first line is already known to start with `-- `.
/// Returns Ok(None) if the line is a property begin/end marker (caller's job).
fn parse_step(lx: &mut Lines, is_v2: bool) -> Result<Step, PlanError> {
    let ln = lx.lineno();
    let line = lx.next().expect("caller checked");
    if line == "-- SESSION" || line.starts_with("-- SESSION ") {
        if !is_v2 {
            // v1 files keep the §0 A1 guarantee verbatim: SESSION is refused.
            return err(ln, "reserved: multi-session (SessionSwitch is not in plan-format v1)");
        }
        let body = line.strip_prefix("-- SESSION ").unwrap_or("");
        if let Some(id) = body.strip_prefix("switch ") {
            let id: u32 =
                id.parse().map_err(|_| PlanError { line: ln, msg: format!("bad session id '{id}'") })?;
            let step = Step::Session(id);
            validate_step(&step).map_err(|e| PlanError { line: ln, msg: e.msg })?;
            return Ok(step);
        }
        if let Some(id) = body.strip_prefix("join ") {
            let id: u32 =
                id.parse().map_err(|_| PlanError { line: ln, msg: format!("bad session id '{id}'") })?;
            let step = Step::Join(id);
            validate_step(&step).map_err(|e| PlanError { line: ln, msg: e.msg })?;
            return Ok(step);
        }
        return match body {
            "async-dml" => Ok(Step::AsyncDml(parse_mark_and_sql(lx)?)),
            "wait-until" => Ok(Step::WaitUntil(parse_mark_and_sql(lx)?)),
            other => err(ln, format!("unknown session op '{other}'")),
        };
    }
    if let Some(kind) = line.strip_prefix("-- STEP ") {
        return match kind {
            "ddl" | "dml" | "query" => parse_marked_sql(lx, kind),
            other => err(ln, format!("unknown step kind '{other}'")),
        };
    }
    if let Some(t) = line.strip_prefix("-- TX ") {
        let step = if let Some(n) = t.strip_prefix("savepoint ") {
            TxCtl::Savepoint(n.to_string())
        } else if let Some(n) = t.strip_prefix("rollback-to ") {
            TxCtl::RollbackTo(n.to_string())
        } else {
            match t {
                "begin-rc" => TxCtl::Begin(IsoLevel::ReadCommitted),
                "begin-rr" => TxCtl::Begin(IsoLevel::RepeatableRead),
                "begin-ser" => TxCtl::Begin(IsoLevel::Serializable),
                "commit" => TxCtl::Commit,
                "rollback" => TxCtl::Rollback,
                other => return err(ln, format!("unknown tx ctl '{other}'")),
            }
        };
        let step = Step::Tx(step);
        validate_step(&step).map_err(|e| PlanError { line: ln, msg: e.msg })?;
        return Ok(step);
    }
    if let Some(a) = line.strip_prefix("-- ARM ") {
        let step = if a == "reset-all" {
            Step::Arm(ArmCtl::ResetAll)
        } else if let Some(kv) = a.strip_prefix("set ") {
            let (k, v) = kv
                .split_once('=')
                .ok_or(PlanError { line: ln, msg: format!("bad arm '{a}'") })?;
            Step::Arm(ArmCtl::SetGuc(k.to_string(), v.to_string()))
        } else {
            return err(ln, format!("unknown arm ctl '{a}'"));
        };
        validate_step(&step).map_err(|e| PlanError { line: ln, msg: e.msg })?;
        return Ok(step);
    }
    if let Some(j) = line.strip_prefix("-- ASSUME ") {
        let c = Check::new(j).map_err(|e| PlanError { line: ln, msg: e.msg })?;
        return Ok(Step::Assumption(c));
    }
    if let Some(j) = line.strip_prefix("-- ASSERT ") {
        let c = Check::new(j).map_err(|e| PlanError { line: ln, msg: e.msg })?;
        return Ok(Step::Assertion(c));
    }
    if let Some(f) = line.strip_prefix("-- FAULT ") {
        let fp = if let Some(p) = f.strip_prefix("crash ") {
            FaultPoint::Crash(p.to_string())
        } else if let Some(p) = f.strip_prefix("env ") {
            FaultPoint::Env(p.to_string())
        } else {
            match f {
                "disconnect" => FaultPoint::Disconnect,
                "reconnect-server" => FaultPoint::ReconnectServer,
                "torn-write" => FaultPoint::TornWrite,
                other => return err(ln, format!("unknown fault '{other}'")),
            }
        };
        let step = Step::Fault(fp);
        validate_step(&step).map_err(|e| PlanError { line: ln, msg: e.msg })?;
        return Ok(step);
    }
    err(ln, format!("unknown annotation '{line}'"))
}

fn parse_begin_property(line: &str, ln: usize) -> Result<(String, u32, BTreeSet<String>), PlanError> {
    let rest = line
        .strip_prefix("-- begin property '")
        .ok_or(PlanError { line: ln, msg: "bad begin-property line".into() })?;
    let (name, rest) = rest
        .split_once('\'')
        .ok_or(PlanError { line: ln, msg: "unterminated property name".into() })?;
    let rest = rest
        .strip_prefix(" seq=")
        .ok_or(PlanError { line: ln, msg: "missing seq= on begin-property".into() })?;
    let (seq_s, tables_s) = match rest.split_once(" tables=") {
        Some((s, t)) => (s, Some(t)),
        None => (rest, None),
    };
    let seq: u32 = seq_s
        .parse()
        .map_err(|_| PlanError { line: ln, msg: format!("bad seq '{seq_s}'") })?;
    let mut tables = BTreeSet::new();
    if let Some(ts) = tables_s {
        if ts.is_empty() {
            return err(ln, "empty tables= list");
        }
        let mut prev: Option<&str> = None;
        for t in ts.split(',') {
            if let Some(p) = prev {
                if p >= t {
                    return err(ln, "tables= list must be sorted, unique");
                }
            }
            prev = Some(t);
            tables.insert(t.to_string());
        }
    }
    validate_property(&name.to_string(), &tables).map_err(|e| PlanError { line: ln, msg: e.msg })?;
    Ok((name.to_string(), seq, tables))
}

/// Parse a `.plan` file. Strict: unknown annotations, reserved SessionSwitch,
/// malformed headers, and unsorted tables lists are hard errors.
pub fn parse(text: &str) -> Result<Plan, PlanError> {
    let mut lx = Lines { lines: text.lines().collect(), pos: 0 };
    let (header, is_v2) = parse_header(&mut lx)?;
    let mut items: Vec<PlanItem> = Vec::new();
    loop {
        // Skip blank separator lines.
        while matches!(lx.peek(), Some(l) if l.is_empty()) {
            lx.next();
        }
        let Some(line) = lx.peek() else { break };
        let ln = lx.lineno();
        if line.starts_with("-- begin property ") {
            let line = lx.next().unwrap();
            let (name, seq, tables) = parse_begin_property(line, ln)?;
            let mut steps = Vec::new();
            loop {
                let ln2 = lx.lineno();
                let Some(inner) = lx.peek() else {
                    return err(ln2, format!("unterminated property seq={seq}"));
                };
                if inner.is_empty() {
                    return err(ln2, "blank line inside property block");
                }
                if let Some(e) = inner.strip_prefix("-- end property seq=") {
                    let end_seq: u32 = e
                        .parse()
                        .map_err(|_| PlanError { line: ln2, msg: format!("bad end seq '{e}'") })?;
                    if end_seq != seq {
                        return err(ln2, format!("end seq={end_seq} does not match begin seq={seq}"));
                    }
                    lx.next();
                    break;
                }
                if inner.starts_with("-- begin property ") {
                    return err(ln2, "nested property blocks are not allowed");
                }
                if !inner.starts_with("--") {
                    return err(ln2, format!("bare SQL without '-- STEP' annotation: '{inner}'"));
                }
                steps.push(parse_step(&mut lx, is_v2)?);
            }
            items.push(PlanItem::Property { name, seq, tables, steps });
        } else if line.starts_with("--") {
            items.push(PlanItem::Step(parse_step(&mut lx, is_v2)?));
        } else {
            return err(ln, format!("bare SQL without '-- STEP' annotation: '{line}'"));
        }
    }
    let plan = Plan { header, items };
    validate(&plan)?;
    Ok(plan)
}

// ---------------------------------------------------------------------------
// Table-dependency API (for the WS-RUNNER shrinker)
// ---------------------------------------------------------------------------

/// Per-property touched-table sets, keyed by property seq, in birth-id space.
/// Birth ids are stable across `ALTER TABLE .. RENAME`, so rename-chasing is
/// inherent: two properties depend on each other iff their sets intersect.
pub fn property_table_deps(plan: &Plan) -> std::collections::BTreeMap<u32, BTreeSet<String>> {
    let mut out = std::collections::BTreeMap::new();
    for item in &plan.items {
        if let PlanItem::Property { seq, tables, .. } = item {
            out.insert(*seq, tables.clone());
        }
    }
    out
}
