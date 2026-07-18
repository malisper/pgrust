//! SCAFFOLD — plan-format v1 per contract §1.5, implemented locally so
//! WS-RUNNER compiles and gates pre-integration. `src/plan.rs` is WS-GEN's
//! file (contract §5 ownership fence); at integration this module is deleted
//! and `runner::planface` re-exports `crate::plan` instead (one-line swap).
//! Everything here follows the contract's pinned grammar:
//!
//! Header:
//!   -- simharness plan v1 (serial single-session)
//!   -- seed: N profile: NAME profile-sha256: HEX generator: SHA
//!
//! Step annotations (one line each):
//!   -- begin property 'Name' seq=N tables=a,b   / -- end property seq=N
//!   -- ASSUME <check-json>  /  -- ASSERT <check-json>
//!   -- ARM set k=v          /  -- ARM reset-all
//!   -- MARK read|mutation|passthrough [order-underdetermined] [float-lenient]
//!   -- FAULT disconnect|reconnect-server|crash:<pt>|torn-write|env:<what>
//!   -- TX begin-rc|begin-rr|begin-ser|commit|rollback|savepoint <n>|rollback-to <n>
//!   -- SESSION ...  => HARD parse error "reserved: multi-session" (§0 A1)
//!
//! Round-trip law: parse(render(plan)) == plan, bit-exact.
//! Crash/TornWrite/Env fault tags: render+parse OK, EXECUTE refuses (H4).
//! (`tables=` on the begin-property line is this scaffold's rendering of the
//! WS-GEN table-dependency API — flagged in the worklog for reconciliation.)

use std::fmt::Write as _;

pub const PLAN_HEADER_LINE: &str = "-- simharness plan v1 (serial single-session)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanHeader {
    pub seed: u64,
    pub profile: String,
    pub profile_sha256: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlMeta {
    pub order_underdetermined: bool,
    pub float_lenient: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sql {
    pub text: String, // single statement, no trailing ';', no embedded newline
    pub mark: Mark,
    pub meta: SqlMeta,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    SetGuc(String, String),
    ResetAll,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultPoint {
    Disconnect,
    ReconnectServer,
    /// Reserved (H4): render+parse OK, execute refuses.
    Crash(String),
    /// Reserved (H4).
    TornWrite,
    /// Reserved (H4).
    Env(String),
}

impl FaultPoint {
    pub fn reserved(&self) -> bool {
        matches!(self, FaultPoint::Crash(_) | FaultPoint::TornWrite | FaultPoint::Env(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    BeginProperty { name: String, seq: u32, tables: Vec<String> },
    EndProperty { seq: u32 },
    Ddl(Sql),
    Dml(Sql),
    Query(Sql),
    Tx(TxCtl),
    Arm(ArmCtl),
    Assumption(String), // single-line check JSON
    Assertion(String),  // single-line check JSON
    Fault(FaultPoint),
    // SessionSwitch: reserved tag, parse = hard error (contract §0 A1).
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub header: PlanHeader,
    pub steps: Vec<Step>,
}

/// Statement-kind classification, shared by render and parse so the kind is
/// derived (not encoded) and round-trips by construction.
pub fn classify_sql(text: &str) -> StmtKind {
    let head = text.trim_start().split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    match head.as_str() {
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "COMMENT" | "GRANT" | "REVOKE" => StmtKind::Ddl,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "COPY" => StmtKind::Dml,
        _ => StmtKind::Query,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtKind {
    Ddl,
    Dml,
    Query,
}

impl Plan {
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(PLAN_HEADER_LINE);
        out.push('\n');
        let _ = writeln!(
            out,
            "-- seed: {} profile: {} profile-sha256: {} generator: {}",
            self.header.seed, self.header.profile, self.header.profile_sha256, self.header.generator
        );
        for step in &self.steps {
            match step {
                Step::BeginProperty { name, seq, tables } => {
                    let _ = writeln!(out, "-- begin property '{}' seq={} tables={}", name, seq, tables.join(","));
                }
                Step::EndProperty { seq } => {
                    let _ = writeln!(out, "-- end property seq={}", seq);
                }
                Step::Ddl(sql) | Step::Dml(sql) | Step::Query(sql) => {
                    let mut flags = String::new();
                    if sql.meta.order_underdetermined {
                        flags.push_str(" order-underdetermined");
                    }
                    if sql.meta.float_lenient {
                        flags.push_str(" float-lenient");
                    }
                    let _ = writeln!(out, "-- MARK {}{}", sql.mark.as_str(), flags);
                    let _ = writeln!(out, "{};", sql.text);
                }
                Step::Tx(tx) => {
                    let s = match tx {
                        TxCtl::Begin(IsoLevel::ReadCommitted) => "begin-rc".to_string(),
                        TxCtl::Begin(IsoLevel::RepeatableRead) => "begin-rr".to_string(),
                        TxCtl::Begin(IsoLevel::Serializable) => "begin-ser".to_string(),
                        TxCtl::Commit => "commit".to_string(),
                        TxCtl::Rollback => "rollback".to_string(),
                        TxCtl::Savepoint(n) => format!("savepoint {}", n),
                        TxCtl::RollbackTo(n) => format!("rollback-to {}", n),
                    };
                    let _ = writeln!(out, "-- TX {}", s);
                }
                Step::Arm(arm) => match arm {
                    ArmCtl::SetGuc(k, v) => {
                        let _ = writeln!(out, "-- ARM set {}={}", k, v);
                    }
                    ArmCtl::ResetAll => {
                        let _ = writeln!(out, "-- ARM reset-all");
                    }
                },
                Step::Assumption(j) => {
                    let _ = writeln!(out, "-- ASSUME {}", j);
                }
                Step::Assertion(j) => {
                    let _ = writeln!(out, "-- ASSERT {}", j);
                }
                Step::Fault(f) => {
                    let s = match f {
                        FaultPoint::Disconnect => "disconnect".to_string(),
                        FaultPoint::ReconnectServer => "reconnect-server".to_string(),
                        FaultPoint::Crash(pt) => format!("crash:{}", pt),
                        FaultPoint::TornWrite => "torn-write".to_string(),
                        FaultPoint::Env(w) => format!("env:{}", w),
                    };
                    let _ = writeln!(out, "-- FAULT {}", s);
                }
            }
        }
        out
    }

    pub fn parse(text: &str) -> Result<Plan, String> {
        let mut lines = text.lines().peekable();
        match lines.next() {
            Some(l) if l == PLAN_HEADER_LINE => {}
            other => return Err(format!("plan: bad header line: {:?}", other)),
        }
        let hline = lines.next().ok_or("plan: missing seed header line")?;
        let header = parse_header_line(hline)?;
        let mut steps = Vec::new();
        let mut pending_mark: Option<(Mark, SqlMeta)> = None;
        for line in lines {
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("-- ") {
                if pending_mark.is_some() {
                    return Err("plan: MARK line not followed by a statement".into());
                }
                if rest.starts_with("SESSION") {
                    return Err("reserved: multi-session".into());
                }
                if let Some(r) = rest.strip_prefix("begin property '") {
                    let (name, tail) = r.split_once('\'').ok_or("plan: bad begin property")?;
                    let tail = tail.trim();
                    let mut seq = None;
                    let mut tables = Vec::new();
                    for tok in tail.split_whitespace() {
                        if let Some(v) = tok.strip_prefix("seq=") {
                            seq = Some(v.parse::<u32>().map_err(|e| format!("plan: bad seq: {}", e))?);
                        } else if let Some(v) = tok.strip_prefix("tables=") {
                            tables = if v.is_empty() {
                                Vec::new()
                            } else {
                                v.split(',').map(|s| s.to_string()).collect()
                            };
                        } else {
                            return Err(format!("plan: bad begin-property token: {}", tok));
                        }
                    }
                    steps.push(Step::BeginProperty {
                        name: name.to_string(),
                        seq: seq.ok_or("plan: begin property missing seq")?,
                        tables,
                    });
                } else if let Some(r) = rest.strip_prefix("end property seq=") {
                    steps.push(Step::EndProperty {
                        seq: r.trim().parse::<u32>().map_err(|e| format!("plan: bad end seq: {}", e))?,
                    });
                } else if let Some(r) = rest.strip_prefix("MARK ") {
                    let mut toks = r.split_whitespace();
                    let mark = match toks.next() {
                        Some("read") => Mark::Read,
                        Some("mutation") => Mark::Mutation,
                        Some("passthrough") => Mark::Passthrough,
                        other => return Err(format!("plan: bad mark: {:?}", other)),
                    };
                    let mut meta = SqlMeta::default();
                    for t in toks {
                        match t {
                            "order-underdetermined" => meta.order_underdetermined = true,
                            "float-lenient" => meta.float_lenient = true,
                            other => return Err(format!("plan: bad mark flag: {}", other)),
                        }
                    }
                    pending_mark = Some((mark, meta));
                } else if let Some(r) = rest.strip_prefix("TX ") {
                    let tx = match r.trim() {
                        "begin-rc" => TxCtl::Begin(IsoLevel::ReadCommitted),
                        "begin-rr" => TxCtl::Begin(IsoLevel::RepeatableRead),
                        "begin-ser" => TxCtl::Begin(IsoLevel::Serializable),
                        "commit" => TxCtl::Commit,
                        "rollback" => TxCtl::Rollback,
                        other => {
                            if let Some(n) = other.strip_prefix("savepoint ") {
                                TxCtl::Savepoint(n.to_string())
                            } else if let Some(n) = other.strip_prefix("rollback-to ") {
                                TxCtl::RollbackTo(n.to_string())
                            } else {
                                return Err(format!("plan: bad TX: {}", other));
                            }
                        }
                    };
                    steps.push(Step::Tx(tx));
                } else if let Some(r) = rest.strip_prefix("ARM ") {
                    let arm = match r.trim() {
                        "reset-all" => ArmCtl::ResetAll,
                        other => {
                            let kv = other.strip_prefix("set ").ok_or_else(|| format!("plan: bad ARM: {}", other))?;
                            let (k, v) = kv.split_once('=').ok_or_else(|| format!("plan: bad ARM kv: {}", kv))?;
                            ArmCtl::SetGuc(k.to_string(), v.to_string())
                        }
                    };
                    steps.push(Step::Arm(arm));
                } else if let Some(r) = rest.strip_prefix("ASSUME ") {
                    steps.push(Step::Assumption(r.to_string()));
                } else if let Some(r) = rest.strip_prefix("ASSERT ") {
                    steps.push(Step::Assertion(r.to_string()));
                } else if let Some(r) = rest.strip_prefix("FAULT ") {
                    let f = match r.trim() {
                        "disconnect" => FaultPoint::Disconnect,
                        "reconnect-server" => FaultPoint::ReconnectServer,
                        "torn-write" => FaultPoint::TornWrite,
                        other => {
                            if let Some(pt) = other.strip_prefix("crash:") {
                                FaultPoint::Crash(pt.to_string())
                            } else if let Some(w) = other.strip_prefix("env:") {
                                FaultPoint::Env(w.to_string())
                            } else {
                                return Err(format!("plan: bad FAULT: {}", other));
                            }
                        }
                    };
                    steps.push(Step::Fault(f));
                } else {
                    return Err(format!("plan: unknown annotation: {}", rest));
                }
            } else {
                // SQL statement line: requires a pending MARK.
                let (mark, meta) = pending_mark
                    .take()
                    .ok_or_else(|| format!("plan: statement without MARK: {}", line))?;
                let text = line
                    .strip_suffix(';')
                    .ok_or_else(|| format!("plan: statement missing ';': {}", line))?
                    .to_string();
                let sql = Sql { text: text.clone(), mark, meta };
                steps.push(match classify_sql(&text) {
                    StmtKind::Ddl => Step::Ddl(sql),
                    StmtKind::Dml => Step::Dml(sql),
                    StmtKind::Query => Step::Query(sql),
                });
            }
        }
        if pending_mark.is_some() {
            return Err("plan: trailing MARK without statement".into());
        }
        Ok(Plan { header, steps })
    }
}

fn parse_header_line(line: &str) -> Result<PlanHeader, String> {
    let r = line.strip_prefix("-- seed: ").ok_or("plan: bad seed line")?;
    let (seed_s, r) = r.split_once(" profile: ").ok_or("plan: bad seed line (profile)")?;
    let (profile, r) = r.split_once(" profile-sha256: ").ok_or("plan: bad seed line (sha)")?;
    let (sha, generator) = r.split_once(" generator: ").ok_or("plan: bad seed line (generator)")?;
    Ok(PlanHeader {
        seed: seed_s.parse::<u64>().map_err(|e| format!("plan: bad seed: {}", e))?,
        profile: profile.to_string(),
        profile_sha256: sha.to_string(),
        generator: generator.to_string(),
    })
}
