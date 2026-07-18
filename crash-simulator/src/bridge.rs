//! Integration bridge (harness/h1) — the glue the three WS surfaces meet on.
//!
//! - `runner_profile_to_gen`: the runner's profile JSON (contract §4.2 schema,
//!   WS-RUNNER owned) viewed through WS-GEN's `GenProfile` (which owns the
//!   generation-side fields). One file on disk, one sha256, two typed views.
//! - `oracle_registry` / `OraclePropGen`: WS-ORACLE's 14 v1 properties adapted
//!   to WS-GEN's `PropertyGen` trait, lowering the oracle-side `PStep` IR into
//!   frozen plan-format v1 steps (1:1, so run-time context stays aligned).
//! - `OracleCtx` + `generate_plan_with_ctx`: per-seed oracle run context —
//!   the property instances (ledger ops, probe slots, checks) keyed by the
//!   plan's property `seq`. Deterministically regenerable from the seed
//!   (determinism law §0 A3), so it is never serialized into the plan.
//! - `OracleCheckEval`: the runner-side `CheckEval` backed by WS-ORACLE's
//!   ledger + slot-addressed result stack + `eval_check`.
//! - `OracleDiffClassifier`: the runner-side `DiffClassifier` backed by
//!   WS-ORACLE's triage.py-pinned ladder + warts.toml suppression.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use rand::RngCore;

use crate::gen::profile::{
    ColTypeWeights, GenProfile, IsoMix, PlanLen, StatementWeights, TableShape,
};
use crate::gen::schema::SchemaState;
use crate::oracle::check::{
    check_from_json, check_to_json, eval_check, CheckOutcome, HookKind, HookProbe,
    ResultStack as OracleStack, Row, StmtResult, Value,
};
use crate::oracle::classifier::{classify_step, digest_rows, RunStatus};
use crate::oracle::ledger::{reconcile, ApplyOutcome, EngineDmlResult, Ledger};
use crate::oracle::props::{self, ProfileView, SchemaView};
use crate::oracle::pstep::{
    ArmCtl as PArmCtl, IsoLevel as PIso, Mark as PMark, NoiseConstraint, PropertyInstance, PStep,
    SqlMeta, SqlStep, TxCtl as PTxCtl,
};
use crate::oracle::warts::Warts;
use crate::plan::{ArmCtl, Check, IsoLevel, Mark, Plan, PlanItem, Sql, SqlFlags, Step, TxCtl};
use crate::property::{Caps, Footprint, GeneratedProperty, NoiseSource, PropertyGen};
use crate::runner::driver::{CheckEval, CheckVerdict, DiffClassifier, ExecOutcome};
use crate::vocab::Severity;

// ---------------------------------------------------------------------------
// Profile view: runner schema -> gen schema (one file, two typed views)
// ---------------------------------------------------------------------------

pub fn runner_profile_to_gen(p: &crate::runner::profile::Profile) -> GenProfile {
    let w = |k: &str, d: u64| p.statement_weights.get(k).map(|v| *v as u64).unwrap_or(d);
    let iso = |k: &str| p.iso_mix.get(k).map(|v| *v as u64).unwrap_or(0);
    // Preserve SET structure (never flatten): a profile arm set is applied
    // atomically by the generator (H4 arm-set fidelity fix).
    let arm_sets: Vec<Vec<(String, String)>> = p
        .arm_sets
        .iter()
        .map(|set| {
            set.iter()
                .filter_map(|arm| {
                    arm.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))
                })
                .collect()
        })
        .collect();
    let mut property_weights: BTreeMap<String, u64> = p
        .property_weights
        .iter()
        .map(|(k, v)| (k.clone(), *v as u64))
        .collect();
    for k in &p.kill_switches {
        property_weights.insert(k.clone(), 0);
    }
    GenProfile {
        name: p.name.clone(),
        plan_len: PlanLen { min: p.steps_min.max(1) as u64, max: p.steps_max.max(1) as u64 },
        statement_weights: StatementWeights {
            ddl: w("ddl", 0),
            dml: w("dml", 0),
            query: w("query", 0),
            tx: w("tx", 0),
            arm: w("arm", 0),
            fault: w("fault", 0),
            property: w("property", 0),
        },
        table_shape: TableShape {
            min_cols: p.table_shape.cols_min.max(1),
            max_cols: p.table_shape.cols_max.max(1),
            // H6 (H5 find 2 fix): honor the profile's col_types. An empty
            // map keeps the generator defaults; a non-empty map is a FULL
            // specification (missing keys weigh 0). The old hardcoded
            // default (float8=0) made q:float-agg unreachable from every
            // runner profile — float_lenient profiles included.
            col_types: if p.table_shape.col_types.is_empty() {
                ColTypeWeights::default()
            } else {
                let ct = |k: &str| {
                    p.table_shape.col_types.get(k).map(|v| *v as u64).unwrap_or(0)
                };
                ColTypeWeights {
                    int: ct("int"),
                    bigint: ct("bigint"),
                    text: ct("text"),
                    numeric: ct("numeric"),
                    float8: ct("float8"),
                }
            },
            rows_max: p.table_shape.rows_max,
        },
        iso_mix: IsoMix {
            rc: iso("read-committed"),
            rr: iso("repeatable-read"),
            ser: iso("serializable"),
        },
        arm_sets,
        property_weights,
        float_lenient: p.float_lenient,
        test_disable_productions: p.test_disable_productions.clone(),
    }
}

fn profile_view(gp: &GenProfile) -> ProfileView {
    let mut iso_mix = Vec::new();
    // Weighted mix as a proportional pick vector (integer arithmetic only).
    let scale = |w: u64| ((w + 9) / 10).min(16) as usize; // 0 stays 0
    for _ in 0..scale(gp.iso_mix.rc) {
        iso_mix.push(PIso::ReadCommitted);
    }
    for _ in 0..scale(gp.iso_mix.rr) {
        iso_mix.push(PIso::RepeatableRead);
    }
    for _ in 0..scale(gp.iso_mix.ser) {
        iso_mix.push(PIso::Serializable);
    }
    if iso_mix.is_empty() {
        iso_mix.push(PIso::ReadCommitted);
    }
    // Property-side view keeps set structure; empty control sets are not
    // useful to X1/L2 (nothing to SET), so they are filtered here.
    let arm_sets: Vec<Vec<(String, String)>> =
        gp.arm_sets.iter().filter(|s| !s.is_empty()).cloned().collect();
    ProfileView { float_lenient: gp.float_lenient, iso_mix, arm_sets }
}

// ---------------------------------------------------------------------------
// PStep -> plan::Step lowering (1:1; NoiseSlot substituted at lowering time)
// ---------------------------------------------------------------------------

fn lower_mark(m: PMark) -> Mark {
    match m {
        PMark::Read => Mark::Read,
        PMark::Mutation => Mark::Mutation,
        PMark::Passthrough => Mark::Passthrough,
    }
}

fn raise_mark(m: Mark) -> PMark {
    match m {
        Mark::Read => PMark::Read,
        Mark::Mutation => PMark::Mutation,
        Mark::Passthrough => PMark::Passthrough,
    }
}

fn lower_tx(t: &PTxCtl) -> TxCtl {
    match t {
        PTxCtl::Begin(PIso::ReadCommitted) => TxCtl::Begin(IsoLevel::ReadCommitted),
        PTxCtl::Begin(PIso::RepeatableRead) => TxCtl::Begin(IsoLevel::RepeatableRead),
        PTxCtl::Begin(PIso::Serializable) => TxCtl::Begin(IsoLevel::Serializable),
        PTxCtl::Commit => TxCtl::Commit,
        PTxCtl::Rollback => TxCtl::Rollback,
        PTxCtl::Savepoint(n) => TxCtl::Savepoint(n.clone()),
        PTxCtl::RollbackTo(n) => TxCtl::RollbackTo(n.clone()),
    }
}

fn lower_arm(a: &PArmCtl) -> ArmCtl {
    match a {
        PArmCtl::SetGuc(k, v) => ArmCtl::SetGuc(k.clone(), v.clone()),
        PArmCtl::ResetAll => ArmCtl::ResetAll,
    }
}

fn stmt_kind(sql: &str) -> u8 {
    let head = sql.trim_start().split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    match head.as_str() {
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "COMMENT" | "GRANT" | "REVOKE" => 0,
        "INSERT" | "UPDATE" | "DELETE" | "MERGE" | "COPY" => 1,
        _ => 2,
    }
}

fn lower_sql(s: &SqlStep) -> Option<Step> {
    let sql = Sql::new(
        format!("{};", s.sql),
        lower_mark(s.mark),
        SqlFlags {
            order_underdetermined: s.meta.order_underdetermined,
            float_lenient: s.meta.float_lenient,
        },
    )
    .ok()?;
    Some(match stmt_kind(&s.sql) {
        0 => Step::Ddl(sql),
        1 => Step::Dml(sql),
        _ => Step::Query(sql),
    })
}

/// Word-boundary containment: does `text` reference `name` as a standalone
/// identifier token? (Conservative noise constraint — no SQL parsing.)
fn mentions_ident(text: &str, name: &str) -> bool {
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(pos) = text[from..].find(name) {
        let start = from + pos;
        let end = start + name.len();
        let pre_ok = start == 0
            || !(bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
        let post_ok = end >= bytes.len()
            || !(bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_');
        if pre_ok && post_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

// ---------------------------------------------------------------------------
// PropertyGen adapter over the oracle property registry
// ---------------------------------------------------------------------------

/// Static expected footprints for budget steering (approximate by design —
/// steering, not accounting; the generator consumes these against per-kind
/// budgets when a property is instantiated).
fn footprint_of(id: props::PropertyId) -> Footprint {
    use props::PropertyId as P;
    match id {
        P::F1InsertSelect => Footprint { ddl: 2, dml: 1, query: 3, ..Default::default() },
        P::F2UpdateVisibility => Footprint { ddl: 2, dml: 2, query: 2, ..Default::default() },
        P::F3DeleteAbsence => Footprint { ddl: 2, dml: 2, query: 3, ..Default::default() },
        P::F4ConstraintError => Footprint { ddl: 2, dml: 2, query: 1, ..Default::default() },
        P::F5DdlEffects => Footprint { ddl: 3, dml: 1, query: 2, ..Default::default() },
        P::F6AggConsistency => Footprint { ddl: 2, dml: 1, query: 4, ..Default::default() },
        P::F7MemoryBaseline => Footprint { ddl: 2, dml: 1, query: 2, ..Default::default() },
        P::F8ResourceBaseline => Footprint { ddl: 2, dml: 1, query: 2, tx: 2, ..Default::default() },
        P::L1Tlp => Footprint { ddl: 2, dml: 1, query: 5, ..Default::default() },
        P::L2NoRec => Footprint { ddl: 2, dml: 1, query: 3, ..Default::default() },
        P::X1ArmEquivalence => Footprint { ddl: 2, dml: 1, query: 4, arm: 2, ..Default::default() },
        P::X2IndexInvariance => Footprint { ddl: 3, dml: 1, query: 2, ..Default::default() },
        P::X4StatementForm => Footprint { ddl: 2, dml: 1, query: 3, ..Default::default() },
        P::M1ReadYourWrites => Footprint { ddl: 2, dml: 2, query: 5, tx: 4, ..Default::default() },
    }
}

struct OraclePropGen {
    id: props::PropertyId,
    log: Rc<RefCell<Vec<PropertyInstance>>>,
}

impl PropertyGen for OraclePropGen {
    fn name(&self) -> &'static str {
        self.id.as_str()
    }

    fn required_caps(&self) -> Caps {
        // v1 properties are self-local (they create + drop their own tables);
        // no schema capabilities are required.
        Caps::default()
    }

    fn footprint(&self) -> Footprint {
        footprint_of(self.id)
    }

    fn generate(
        &self,
        rng: &mut dyn RngCore,
        _schema: &SchemaState,
        noise: &mut dyn NoiseSource,
        profile: &GenProfile,
    ) -> Option<GeneratedProperty> {
        let pv = profile_view(profile);
        let sv = SchemaView::default();
        let inst = props::generate(self.id, &mut &mut *rng, &sv, &pv);

        // Lower 1:1 into plan steps; substitute noise slots so the recorded
        // instance stays step-aligned with the plan block.
        let mut steps: Vec<Step> = Vec::with_capacity(inst.steps.len());
        let mut lowered: Vec<PStep> = Vec::with_capacity(inst.steps.len());
        for ps in inst.steps.into_iter() {
            match ps {
                PStep::Sql(s) => {
                    steps.push(lower_sql(&s)?);
                    lowered.push(PStep::Sql(s));
                }
                PStep::Tx(t) => {
                    steps.push(Step::Tx(lower_tx(&t)));
                    lowered.push(PStep::Tx(t));
                }
                PStep::Arm(a) => {
                    steps.push(Step::Arm(lower_arm(&a)));
                    lowered.push(PStep::Arm(a));
                }
                PStep::Assume(c) => {
                    steps.push(Step::Assumption(Check::new(check_to_json(&c)).ok()?));
                    lowered.push(PStep::Assume(c));
                }
                PStep::Assert(c) => {
                    steps.push(Step::Assertion(Check::new(check_to_json(&c)).ok()?));
                    lowered.push(PStep::Assert(c));
                }
                PStep::NoiseSlot(constraint) => {
                    let protected: BTreeSet<String> = match &constraint {
                        NoiseConstraint::MustNotTouch(t) => t.clone(),
                        NoiseConstraint::Any => BTreeSet::new(),
                    };
                    let cfn = |q: &Sql| !protected.iter().any(|t| mentions_ident(q.text(), t));
                    let q = noise.noise_query(rng, &cfn).unwrap_or_else(|| {
                        Sql::new("SELECT 1;", Mark::Passthrough, SqlFlags::default())
                            .expect("literal noise fallback")
                    });
                    // Record the substituted noise as an opaque (unmodeled)
                    // Sql PStep so instance/plan step alignment is 1:1.
                    lowered.push(PStep::Sql(SqlStep {
                        sql: q.text().trim_end_matches(';').to_string(),
                        mark: raise_mark(q.mark),
                        meta: SqlMeta {
                            order_underdetermined: q.flags.order_underdetermined,
                            float_lenient: q.flags.float_lenient,
                        },
                        ledger_op: None,
                        probe: None,
                        stackref: None,
                    }));
                    steps.push(Step::Query(q));
                }
            }
        }
        self.log.borrow_mut().push(PropertyInstance {
            property: inst.property,
            steps: lowered,
            tables: inst.tables.clone(),
        });
        Some(GeneratedProperty { steps, tables: inst.tables })
    }
}

pub fn oracle_registry(log: &Rc<RefCell<Vec<PropertyInstance>>>) -> Vec<Box<dyn PropertyGen>> {
    props::v1_set()
        .into_iter()
        .map(|id| Box::new(OraclePropGen { id, log: log.clone() }) as Box<dyn PropertyGen>)
        .collect()
}

// ---------------------------------------------------------------------------
// Oracle run context: instances keyed by plan property seq
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct OracleCtx {
    pub by_seq: BTreeMap<u32, PropertyInstance>,
}

/// Generate the plan AND its oracle context in one deterministic pass.
/// Instances are recorded in generation order, which is the plan's property
/// document order (the generator assigns `seq` monotonically on emission).
pub fn generate_plan_with_ctx(
    seed: u64,
    profile: &GenProfile,
    profile_sha256: &str,
    generator: &str,
) -> (Plan, OracleCtx) {
    let (plan, ctx, _traces) =
        generate_plan_with_ctx_traced(seed, profile, profile_sha256, generator);
    (plan, ctx)
}

/// `generate_plan_with_ctx` + the H5 production traces (rung A). Trace
/// collection consumes no RNG draws — the plan bytes are identical.
pub fn generate_plan_with_ctx_traced(
    seed: u64,
    profile: &GenProfile,
    profile_sha256: &str,
    generator: &str,
) -> (Plan, OracleCtx, crate::gen::prodreg::GenTraces) {
    let log = Rc::new(RefCell::new(Vec::new()));
    let registry = oracle_registry(&log);
    let (plan, traces) =
        crate::gen::generate_plan_traced(seed, profile, profile_sha256, generator, &registry);
    let insts = log.take();
    let mut it = insts.into_iter();
    let mut by_seq = BTreeMap::new();
    for item in &plan.items {
        if let PlanItem::Property { seq, name, .. } = item {
            let inst = it.next().unwrap_or_else(|| {
                panic!("oracle ctx misalignment: no instance for property seq={seq} ({name})")
            });
            debug_assert_eq!(inst.property.as_str(), name, "instance/property name align");
            by_seq.insert(*seq, inst);
        }
    }
    (plan, OracleCtx { by_seq }, traces)
}

// ---------------------------------------------------------------------------
// Runner-facing CheckEval backed by the oracle (ledger + slots + eval_check)
// ---------------------------------------------------------------------------

fn to_stmt_result(o: &ExecOutcome) -> StmtResult {
    match o {
        ExecOutcome::Rows { rows } => StmtResult::Rows {
            rows: rows
                .iter()
                .map(|r| {
                    Row(r
                        .iter()
                        .map(|c| match c {
                            None => Value::Null,
                            Some(s) => s
                                .parse::<i64>()
                                .map(Value::Int)
                                .unwrap_or_else(|_| Value::Text(s.clone())),
                        })
                        .collect())
                })
                .collect(),
        },
        ExecOutcome::Command { tag } => StmtResult::Command {
            affected: tag
                .split_whitespace()
                .last()
                .and_then(|t| t.parse::<u64>().ok())
                .unwrap_or(0),
        },
        ExecOutcome::SqlError { sqlstate, .. } => StmtResult::Error { sqlstate: sqlstate.clone() },
        // A dead connection has no SQL-level result; 08006 (connection
        // failure) is the honest encoding. The driver's crash classification
        // fires before the oracle sees this in practice.
        ExecOutcome::ConnectionLost { .. } => StmtResult::Error { sqlstate: "08006".into() },
    }
}

fn engine_dml_result(res: &StmtResult) -> EngineDmlResult {
    match res {
        StmtResult::Command { affected } => EngineDmlResult::Ok { affected: *affected },
        StmtResult::Rows { rows } => EngineDmlResult::Ok { affected: rows.len() as u64 },
        StmtResult::Error { sqlstate } => EngineDmlResult::Err { sqlstate: sqlstate.clone() },
    }
}

struct OracleEvalState {
    by_seq: BTreeMap<u32, PropertyInstance>,
    ledger: Ledger,
    stack: OracleStack,
    /// (property seq, next pstep index) while inside a property block.
    cur: Option<(u32, usize)>,
    /// Alignment lost (harness bug or ctx-less block): checks skip, counted.
    dead: bool,
}

/// Engine-hook capability set, probed against the LIVE DUT session at
/// connect (contract §0 A5). H1 shipped the `NoHooks` posture — both
/// hook-gated properties (F7/F8) were structurally vacuous (h3: 107 loud
/// `skipped-no-hook` per campaign). H4 wires the Memory channel: presence =
/// the DUT answers `f7_memory_baseline::WATERMARK_SQL`. Resource (F8) stays
/// unprobed pending the fd-budget knob (h3 queue #6).
#[derive(Debug, Clone, Copy, Default)]
pub struct ProbedHooks {
    pub memory: bool,
    pub resource: bool,
}

impl ProbedHooks {
    pub fn none() -> Self {
        ProbedHooks::default()
    }
}

impl HookProbe for ProbedHooks {
    fn has(&self, hook: HookKind) -> bool {
        match hook {
            HookKind::Memory => self.memory,
            HookKind::Resource => self.resource,
        }
    }
}

/// Probe the live DUT session for hook capabilities: one uncompared read per
/// channel. State-neutral (plain SELECTs) and it warms the probe statement
/// in-session, which is exactly the F7 warm-baseline discipline's friend.
pub fn probe_hooks(dut: &mut dyn crate::runner::driver::Session) -> ProbedHooks {
    let memory = !matches!(
        dut.execute(crate::oracle::props::f7_memory_baseline::WATERMARK_SQL),
        ExecOutcome::SqlError { .. } | ExecOutcome::ConnectionLost { .. }
    );
    ProbedHooks { memory, resource: false }
}

pub struct OracleCheckEval {
    inner: RefCell<OracleEvalState>,
    hooks: ProbedHooks,
}

impl OracleCheckEval {
    pub fn new(ctx: &OracleCtx) -> Self {
        Self::with_hooks(ctx, ProbedHooks::none())
    }

    pub fn with_hooks(ctx: &OracleCtx, hooks: ProbedHooks) -> Self {
        OracleCheckEval {
            inner: RefCell::new(OracleEvalState {
                by_seq: ctx.by_seq.clone(),
                ledger: Ledger::new(),
                stack: OracleStack::new(),
                cur: None,
                dead: false,
            }),
            hooks,
        }
    }
}

impl CheckEval for OracleCheckEval {
    fn eval(&self, check_json: &str, _stack: &crate::runner::driver::ResultStack) -> CheckVerdict {
        let mut st = self.inner.borrow_mut();
        let Some((seq, idx)) = st.cur else {
            return CheckVerdict::Skip("no-oracle-ctx".into());
        };
        if st.dead {
            return CheckVerdict::Skip("oracle-ctx-misaligned".into());
        }
        // Alignment: current instance step must be this check.
        let aligned = st
            .by_seq
            .get(&seq)
            .and_then(|inst| inst.steps.get(idx))
            .map(|s| matches!(s, PStep::Assume(_) | PStep::Assert(_)))
            .unwrap_or(false);
        if !aligned {
            st.dead = true;
            eprintln!("simharness: oracle ctx misaligned at property seq={seq} step {idx} (check)");
            return CheckVerdict::Skip("oracle-ctx-misaligned".into());
        }
        st.cur = Some((seq, idx + 1));
        let check = match check_from_json(check_json) {
            Ok(c) => c,
            Err(e) => return CheckVerdict::Fail(format!("bad check json: {e}")),
        };
        match eval_check(&check, &st.stack, &st.ledger, &self.hooks) {
            CheckOutcome::Pass => CheckVerdict::Pass,
            CheckOutcome::Fail(why) => CheckVerdict::Fail(why),
            CheckOutcome::SkipNoHook => CheckVerdict::Skip("no-hook".into()),
        }
    }

    fn on_property_begin(&self, seq: u32) {
        let mut st = self.inner.borrow_mut();
        st.stack.clear();
        st.dead = !st.by_seq.contains_key(&seq);
        st.cur = Some((seq, 0));
    }

    fn on_property_end(&self, _seq: u32) {
        let mut st = self.inner.borrow_mut();
        st.cur = None;
        st.dead = false;
    }

    fn on_step_outcome(&self, outcome: &ExecOutcome) -> Option<String> {
        let mut st = self.inner.borrow_mut();
        let (seq, idx) = st.cur?;
        if st.dead {
            return None;
        }
        let step = match st.by_seq.get(&seq).and_then(|inst| inst.steps.get(idx)) {
            Some(s) => s.clone(),
            None => {
                st.dead = true;
                eprintln!("simharness: oracle ctx misaligned at property seq={seq} step {idx}");
                return None;
            }
        };
        st.cur = Some((seq, idx + 1));
        match step {
            PStep::Sql(s) => {
                let res = to_stmt_result(outcome);
                if let Some(op) = &s.ledger_op {
                    let expected = match st.ledger.apply(op) {
                        Ok(exp) => exp,
                        Err(misuse) => return Some(format!("ledger misuse: {}", misuse.0)),
                    };
                    let verdict = reconcile(&expected, &engine_dml_result(&res));
                    if verdict.is_violation() {
                        return Some(format!("reconcile: {verdict:?} at '{}'", s.sql));
                    }
                    // Advisory expected-error coherence: an expected error is
                    // an Err ApplyOutcome; reconcile above already compared.
                    let _ = matches!(expected, ApplyOutcome::Err(_));
                }
                if let Some(slot) = s.stackref {
                    st.stack.put(slot, res);
                }
                None
            }
            PStep::Tx(ctl) => {
                match ctl {
                    PTxCtl::Begin(iso) => st.ledger.begin(iso),
                    PTxCtl::Commit => st.ledger.commit(),
                    PTxCtl::Rollback => st.ledger.rollback(),
                    PTxCtl::Savepoint(n) => st.ledger.savepoint(&n),
                    PTxCtl::RollbackTo(n) => {
                        if let Err(m) = st.ledger.rollback_to(&n) {
                            return Some(format!("ledger misuse: {}", m.0));
                        }
                    }
                }
                None
            }
            PStep::Arm(_) => None,
            PStep::Assume(_) | PStep::Assert(_) | PStep::NoiseSlot(_) => {
                st.dead = true;
                eprintln!(
                    "simharness: oracle ctx misaligned at property seq={seq} step {idx} (sql-vs-check)"
                );
                None
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Runner-facing DiffClassifier backed by the oracle ladder + warts
// ---------------------------------------------------------------------------

fn to_run_status(o: &ExecOutcome) -> RunStatus {
    match o {
        ExecOutcome::Rows { rows } => RunStatus::Ok(digest_rows(
            rows.first().map(|r| r.len() as u32).unwrap_or(0),
            rows,
            false,
        )),
        ExecOutcome::Command { tag } => {
            RunStatus::Ok(digest_rows(0, &[vec![Some(tag.clone())]], false))
        }
        ExecOutcome::SqlError { sqlstate, message } => RunStatus::Error {
            sqlstate: Some(sqlstate.clone()),
            msg: message.clone(),
        },
        ExecOutcome::ConnectionLost { message } => {
            if let Some(m) = message.strip_prefix("client:") {
                RunStatus::Fetch { msg: m.trim().to_string() }
            } else {
                RunStatus::Crash { msg: message.clone() }
            }
        }
    }
}

pub struct OracleDiffClassifier {
    warts: Warts,
}

impl OracleDiffClassifier {
    pub fn new(warts: Warts) -> Self {
        OracleDiffClassifier { warts }
    }
}

impl DiffClassifier for OracleDiffClassifier {
    fn classify_read(
        &self,
        sql: &crate::runner::planface::Sql,
        dut: &ExecOutcome,
        cpg: &ExecOutcome,
    ) -> (String, String) {
        let v = classify_step(
            PMark::Read,
            &sql.text,
            &to_run_status(dut),
            &to_run_status(cpg),
            &self.warts,
        );
        if let Some(id) = v.wart {
            // Wart hit: suppressed, never invisible (SIMHARNESS|wart:<id>|n).
            return (crate::vocab::wart_counter(&id), "fine".into());
        }
        let sev = match v.severity {
            Severity::Coverage => "fine".to_string(),
            s => s.as_str().to_string(),
        };
        (v.class.as_str().to_string(), sev)
    }
}

/// Load warts.toml: SIMHARNESS_WARTS env, else ./warts.toml, else the
/// crate-local copy, else empty (counted nowhere — an empty allowlist only
/// means nothing is suppressed).
pub fn load_warts() -> Warts {
    let candidates = [
        std::env::var("SIMHARNESS_WARTS").ok(),
        Some("warts.toml".to_string()),
        Some(format!("{}/warts.toml", env!("CARGO_MANIFEST_DIR"))),
    ];
    for c in candidates.into_iter().flatten() {
        let p = std::path::Path::new(&c);
        if p.exists() {
            match Warts::load(p) {
                Ok(w) => return w,
                Err(e) => {
                    eprintln!("simharness: warts {c}: {e} (continuing with empty allowlist)");
                    return Warts::empty();
                }
            }
        }
    }
    Warts::empty()
}
