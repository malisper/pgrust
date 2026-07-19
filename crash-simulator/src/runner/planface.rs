//! The runner's single import point for the plan format — POST-INTEGRATION:
//! a flat execution view over WS-GEN's frozen `src/plan.rs` (contract §5).
//!
//! `crate::plan` owns the format (render/parse/validate, round-trip law).
//! The runner's dispatcher, shrinker and bugbase operate on a FLAT step
//! sequence (property blocks opened/closed by explicit markers); this module
//! is the loss-free conversion between the two shapes:
//!
//!   plan::Plan (items, nested property blocks)  <-- to_core / from_core -->
//!   planface::Plan (flat steps with BeginProperty/EndProperty markers)
//!
//! `render`/`parse` here delegate to the frozen format — the bytes on disk
//! are ALWAYS plan-format v1. A flat plan truncated mid-property (shrinker
//! phase 1) nests back with a synthesized end marker, so banked shrunk plans
//! re-parse under the strict format.

use std::collections::BTreeSet;

use crate::plan as core;

pub const PLAN_HEADER_LINE: &str = "-- simharness plan v1 (serial single-session)";

pub use crate::plan::ArmCtl;
pub use crate::plan::{IsoLevel, Mark, PlanHeader, TxCtl};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqlMeta {
    pub order_underdetermined: bool,
    pub float_lenient: bool,
}

/// Flat-view Sql: `text` carries NO trailing ';' (the wire form the driver
/// executes); the core form appends it on nesting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sql {
    pub text: String,
    pub mark: Mark,
    pub meta: SqlMeta,
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
    /// H8 multi-session estate (plan-format v2). Switch the active session,
    /// dispatch a non-blocking DML, join a session's outstanding statement,
    /// or poll a session until a scalar-'t' gate clears.
    Session(u32),
    AsyncDml(Sql),
    Join(u32),
    WaitUntil(Sql),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub header: PlanHeader,
    pub steps: Vec<Step>,
}

/// Statement-kind classification (used by tests and distillation display;
/// the format itself encodes the kind explicitly via `-- STEP <kind>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtKind {
    Ddl,
    Dml,
    Query,
}

pub fn classify_sql(text: &str) -> StmtKind {
    let head = text.trim_start().split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    match head.as_str() {
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "COMMENT" | "GRANT" | "REVOKE" => StmtKind::Ddl,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "COPY" => StmtKind::Dml,
        _ => StmtKind::Query,
    }
}

// ---------------------------------------------------------------- conversion

fn flat_sql(s: &core::Sql) -> Sql {
    Sql {
        text: s.text().trim_end_matches(';').to_string(),
        mark: s.mark,
        meta: SqlMeta {
            order_underdetermined: s.flags.order_underdetermined,
            float_lenient: s.flags.float_lenient,
        },
    }
}

fn core_sql(s: &Sql) -> Result<core::Sql, String> {
    core::Sql::new(
        format!("{};", s.text),
        s.mark,
        core::SqlFlags {
            order_underdetermined: s.meta.order_underdetermined,
            float_lenient: s.meta.float_lenient,
        },
    )
    .map_err(|e| e.to_string())
}

fn flat_fault(f: &core::FaultPoint) -> FaultPoint {
    match f {
        core::FaultPoint::Disconnect => FaultPoint::Disconnect,
        core::FaultPoint::ReconnectServer => FaultPoint::ReconnectServer,
        core::FaultPoint::Crash(p) => FaultPoint::Crash(p.clone()),
        core::FaultPoint::TornWrite => FaultPoint::TornWrite,
        core::FaultPoint::Env(p) => FaultPoint::Env(p.clone()),
    }
}

fn core_fault(f: &FaultPoint) -> core::FaultPoint {
    match f {
        FaultPoint::Disconnect => core::FaultPoint::Disconnect,
        FaultPoint::ReconnectServer => core::FaultPoint::ReconnectServer,
        FaultPoint::Crash(p) => core::FaultPoint::Crash(p.clone()),
        FaultPoint::TornWrite => core::FaultPoint::TornWrite,
        FaultPoint::Env(p) => core::FaultPoint::Env(p.clone()),
    }
}

fn flat_step(s: &core::Step) -> Step {
    match s {
        core::Step::Ddl(q) => Step::Ddl(flat_sql(q)),
        core::Step::Dml(q) => Step::Dml(flat_sql(q)),
        core::Step::Query(q) => Step::Query(flat_sql(q)),
        core::Step::Tx(t) => Step::Tx(t.clone()),
        core::Step::Arm(a) => Step::Arm(a.clone()),
        core::Step::Assumption(c) => Step::Assumption(c.json().to_string()),
        core::Step::Assertion(c) => Step::Assertion(c.json().to_string()),
        core::Step::Fault(f) => Step::Fault(flat_fault(f)),
        core::Step::Session(id) => Step::Session(*id),
        core::Step::AsyncDml(q) => Step::AsyncDml(flat_sql(q)),
        core::Step::Join(id) => Step::Join(*id),
        core::Step::WaitUntil(q) => Step::WaitUntil(flat_sql(q)),
    }
}

fn core_step(s: &Step) -> Result<core::Step, String> {
    Ok(match s {
        Step::Ddl(q) => core::Step::Ddl(core_sql(q)?),
        Step::Dml(q) => core::Step::Dml(core_sql(q)?),
        Step::Query(q) => core::Step::Query(core_sql(q)?),
        Step::Tx(t) => core::Step::Tx(t.clone()),
        Step::Arm(a) => core::Step::Arm(a.clone()),
        Step::Assumption(j) => {
            core::Step::Assumption(core::Check::new(j.clone()).map_err(|e| e.to_string())?)
        }
        Step::Assertion(j) => {
            core::Step::Assertion(core::Check::new(j.clone()).map_err(|e| e.to_string())?)
        }
        Step::Fault(f) => core::Step::Fault(core_fault(f)),
        Step::Session(id) => core::Step::Session(*id),
        Step::AsyncDml(q) => core::Step::AsyncDml(core_sql(q)?),
        Step::Join(id) => core::Step::Join(*id),
        Step::WaitUntil(q) => core::Step::WaitUntil(core_sql(q)?),
        Step::BeginProperty { .. } | Step::EndProperty { .. } => {
            return Err("property markers nest via to_core, not core_step".into())
        }
    })
}

impl Plan {
    pub fn from_core(p: &core::Plan) -> Plan {
        let mut steps = Vec::new();
        for item in &p.items {
            match item {
                core::PlanItem::Step(s) => steps.push(flat_step(s)),
                core::PlanItem::Property { name, seq, tables, steps: inner } => {
                    steps.push(Step::BeginProperty {
                        name: name.clone(),
                        seq: *seq,
                        tables: tables.iter().cloned().collect(),
                    });
                    for s in inner {
                        steps.push(flat_step(s));
                    }
                    steps.push(Step::EndProperty { seq: *seq });
                }
            }
        }
        Plan { header: p.header.clone(), steps }
    }

    pub fn to_core(&self) -> Result<core::Plan, String> {
        let mut items: Vec<core::PlanItem> = Vec::new();
        let mut open: Option<(String, u32, BTreeSet<String>, Vec<core::Step>)> = None;
        for s in &self.steps {
            match s {
                Step::BeginProperty { name, seq, tables } => {
                    if open.is_some() {
                        return Err("nested property blocks are not allowed".into());
                    }
                    open =
                        Some((name.clone(), *seq, tables.iter().cloned().collect(), Vec::new()));
                }
                Step::EndProperty { seq } => match open.take() {
                    Some((name, bseq, tables, steps)) => {
                        if bseq != *seq {
                            return Err(format!(
                                "end property seq={seq} does not match begin seq={bseq}"
                            ));
                        }
                        items.push(core::PlanItem::Property { name, seq: bseq, tables, steps });
                    }
                    None => return Err(format!("end property seq={seq} without begin")),
                },
                other => {
                    let cs = core_step(other)?;
                    match &mut open {
                        Some((_, _, _, steps)) => steps.push(cs),
                        None => items.push(core::PlanItem::Step(cs)),
                    }
                }
            }
        }
        // Shrinker phase-1 can truncate mid-property: close the block so the
        // rendered artifact stays a valid plan-format v1 file.
        if let Some((name, seq, tables, steps)) = open.take() {
            items.push(core::PlanItem::Property { name, seq, tables, steps });
        }
        let plan = core::Plan { header: self.header.clone(), items };
        core::validate(&plan).map_err(|e| e.to_string())?;
        Ok(plan)
    }

    /// Render to plan-format v1 bytes (the failure artifact form).
    pub fn render(&self) -> String {
        core::render(&self.to_core().expect("flat plan nests to valid plan-format v1"))
    }

    /// Parse plan-format v1 bytes into the flat execution view.
    pub fn parse(text: &str) -> Result<Plan, String> {
        let p = core::parse(text).map_err(|e| e.to_string())?;
        Ok(Plan::from_core(&p))
    }
}
