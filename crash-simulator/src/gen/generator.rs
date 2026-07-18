//! The lazy plan generator (Turso mechanics per spec §1.1, contract §2.1.2).
//!
//! Determinism law (§0 A3, plan tier — unconditional): one ChaCha8 stream
//! seeded from the one u64 seed; all draws in generation order; ordered
//! containers only; integer weight arithmetic; no ambient entropy or clock
//! anywhere in this module. `seed + profile + generator version` =>
//! byte-identical plan.

use std::cell::RefCell;
use std::collections::VecDeque;

use rand::RngCore;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::gen::budget::{Budgets, Kind};
use crate::gen::noise;
use crate::gen::prodreg::{self as pr, GenTraces, ProdPath};
use crate::gen::profile::GenProfile;
use crate::gen::schema::{SchemaSnapshot, SchemaState};
use crate::gen::weights::{range_incl, weighted_index};
use crate::plan::{
    ArmCtl, FaultPoint, IsoLevel, Plan, PlanHeader, PlanItem, Sql, Step, TxCtl,
};
use crate::property::{NoiseSource, PropertyGen};

/// Bounded retry for constrained placeholder noise (contract §2.1.2).
const NOISE_RETRY_BUDGET: usize = 16;

/// A property is offered only while its expected footprint fits the remaining
/// per-kind budgets ("property weights are functions of remaining budgets" —
/// this is what keeps the distribution exact across noise and
/// property-embedded steps).
fn footprint_fits(budgets: &Budgets, fp: &crate::property::Footprint) -> bool {
    budgets.remaining(Kind::Property) >= 1
        && budgets.remaining(Kind::Ddl) >= fp.ddl as u64
        && budgets.remaining(Kind::Dml) >= fp.dml as u64
        && budgets.remaining(Kind::Query) >= fp.query as u64
        && budgets.remaining(Kind::Tx) >= fp.tx as u64
        && budgets.remaining(Kind::Arm) >= fp.arm as u64
        && budgets.remaining(Kind::Fault) >= fp.fault as u64
}

struct NoiseCtx<'a> {
    schema: &'a SchemaState,
    profile: &'a GenProfile,
    /// H5 production-trace sink. Property noise queries are emitted (and
    /// executed) statements from the same query grammar, so their traces are
    /// committed as `stmt:query` paths on ACCEPTANCE only (rejected retry
    /// candidates never reach the plan and must not enter the metric).
    trace: &'a RefCell<GenTraces>,
}

impl NoiseSource for NoiseCtx<'_> {
    fn noise_query(
        &mut self,
        rng: &mut dyn RngCore,
        constraint: &dyn Fn(&Sql) -> bool,
    ) -> Option<Sql> {
        for _ in 0..NOISE_RETRY_BUDGET {
            let mut sub: ProdPath = Vec::new();
            let q = noise::gen_query(self.schema, self.profile, rng, &mut sub)?;
            if constraint(&q) {
                let mut path = vec![pr::STMT_QUERY.to_string()];
                path.extend(sub);
                self.trace.borrow_mut().paths.push(path);
                return Some(q);
            }
        }
        None
    }
}

/// Transaction-visible model state captured at BEGIN / SAVEPOINT and restored
/// on ROLLBACK / ROLLBACK TO / aborting disconnect. DDL and (non-LOCAL) SET
/// are both transactional in PostgreSQL, so both revert with the tx. The
/// schema snapshot covers tables AND the H6 fdw state (extension/server/
/// foreign tables are transactional too).
#[derive(Clone)]
struct TxSnapshot {
    schema: SchemaSnapshot,
    gucs_set: bool,
}

/// Session-visible state the generator must model to stay coherent
/// (serial single-session — §0 A1).
#[derive(Default)]
struct SessionModel {
    in_tx: bool,
    savepoints: Vec<String>,
    next_savepoint: u32,
    gucs_set: bool,
    /// Model state as of BEGIN (`Some` iff `in_tx`): the server reverts to
    /// this on ROLLBACK or on a disconnect that aborts the open tx.
    tx_snapshot: Option<TxSnapshot>,
    /// One snapshot per live savepoint, aligned index-for-index with
    /// `savepoints` (state as of the SAVEPOINT statement; ROLLBACK TO
    /// restores it and keeps the savepoint — and its snapshot — live).
    sp_snapshots: Vec<TxSnapshot>,
}

pub struct Generator<'a> {
    rng: ChaCha8Rng,
    profile: &'a GenProfile,
    registry: &'a [Box<dyn PropertyGen>],
    schema: SchemaState,
    budgets: Budgets,
    session: SessionModel,
    /// H6 planner-knob swarm: per-seed sampled GUC sets (see `gen::knobs`),
    /// appended after `profile.arm_sets` in the arm-set pool. Sampled once
    /// at construction from the SAME seeded stream (draws happen before the
    /// first statement draw, so determinism law A3 is untouched: same seed +
    /// profile => same knob sets => byte-identical plan). Empty when the
    /// profile has no `planner_knobs` block — zero extra draws, so plans for
    /// knob-less profiles are byte-identical to pre-H6 plans.
    sampled_arm_sets: Vec<Vec<(String, String)>>,
    /// Queued items that must follow the current one (fault pairing, tail).
    pending: VecDeque<PlanItem>,
    next_seq: u32,
    emitted_first: bool,
    finished: bool,
    /// H5 rung A: production traces for every emitted statement. Collection
    /// consumes no RNG draws and never touches plan bytes (determinism law
    /// A3 untouched). RefCell: the NoiseCtx borrow runs while `schema` is
    /// shared-borrowed. Cleanup-tail steps (mechanical ROLLBACK/RESET ALL)
    /// are not grammar decisions and are deliberately untraced.
    trace: RefCell<GenTraces>,
}

impl<'a> Generator<'a> {
    pub fn new(seed: u64, profile: &'a GenProfile, registry: &'a [Box<dyn PropertyGen>]) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let total = range_incl(&mut rng, profile.plan_len.min, profile.plan_len.max);
        let budgets =
            Budgets::allocate(&profile.statement_weights, total, !registry.is_empty());
        let mut schema = SchemaState::default();
        // Seed tag for foreign-table CSV paths (part of plan identity, so
        // plan bytes stay a pure function of seed+profile+generator).
        schema.set_plan_seed(seed);
        // H6-GUC: knob sampling draws from the SAME seeded stream; knob-less
        // profiles take the None arm and consume zero draws.
        let sampled_arm_sets = match &profile.planner_knobs {
            Some(cfg) => crate::gen::knobs::sample_knob_sets(&mut rng, cfg),
            None => Vec::new(),
        };
        Generator {
            rng,
            profile,
            registry,
            schema,
            budgets,
            session: SessionModel::default(),
            sampled_arm_sets,
            pending: VecDeque::new(),
            next_seq: 1,
            emitted_first: false,
            finished: false,
            trace: RefCell::new(GenTraces::default()),
        }
    }

    fn commit_path(&self, stmt_node: &str, sub: ProdPath) {
        let mut path = Vec::with_capacity(1 + sub.len());
        path.push(stmt_node.to_string());
        path.extend(sub);
        self.trace.borrow_mut().paths.push(path);
    }

    fn cleanup_tail(&mut self) {
        // Deterministic session cleanup: close any open tx, then RESET ALL if
        // any GUC was set (the 1session GUC-leak law).
        if self.session.in_tx {
            self.pending.push_back(PlanItem::Step(Step::Tx(TxCtl::Rollback)));
            // The tail ROLLBACK reverts the server to the BEGIN snapshot, so
            // whether RESET ALL is still owed is decided by the snapshot's
            // GUC state, not the in-tx state.
            let snap = self.session.tx_snapshot.take().expect("in_tx implies tx snapshot");
            self.schema.restore(snap.schema);
            self.session.gucs_set = snap.gucs_set;
            self.session.in_tx = false;
            self.session.savepoints.clear();
            self.session.sp_snapshots.clear();
        }
        if self.session.gucs_set {
            self.pending.push_back(PlanItem::Step(Step::Arm(ArmCtl::ResetAll)));
            self.session.gucs_set = false;
        }
        self.finished = true;
    }

    /// Kind-choice weights for the current state: remaining budget per kind,
    /// zeroed where the kind is not currently generatable.
    fn kind_weights(&self) -> [u64; 7] {
        let has_table = !self.schema.tables().is_empty();
        let mut w = [
            self.budgets.remaining(Kind::Ddl),
            if has_table { self.budgets.remaining(Kind::Dml) } else { 0 },
            if has_table { self.budgets.remaining(Kind::Query) } else { 0 },
            self.budgets.remaining(Kind::Tx),
            if self.profile.arm_sets.is_empty() && self.sampled_arm_sets.is_empty() {
                0
            } else {
                self.budgets.remaining(Kind::Arm)
            },
            // Disconnect/Reconnect are emitted as a pair: need >= 2.
            if self.budgets.remaining(Kind::Fault) >= 2 {
                self.budgets.remaining(Kind::Fault)
            } else {
                0
            },
            0, // property, filled below
        ];
        if !self.registry.is_empty() && self.budgets.remaining(Kind::Property) > 0 {
            let caps = self.schema.caps();
            let eligible: u64 = self
                .registry
                .iter()
                .filter(|p| {
                    caps.contains(p.required_caps())
                        && p.weight(self.profile) > 0
                        && footprint_fits(&self.budgets, &p.footprint())
                })
                .count() as u64;
            if eligible > 0 {
                w[6] = self.budgets.remaining(Kind::Property);
            }
        }
        w
    }

    fn gen_tx_step(&mut self) -> Step {
        if !self.session.in_tx {
            let iso = &self.profile.iso_mix;
            let level = match weighted_index(&mut self.rng, &[iso.rc, iso.rr, iso.ser]) {
                Some(0) => IsoLevel::ReadCommitted,
                Some(1) => IsoLevel::RepeatableRead,
                Some(2) => IsoLevel::Serializable,
                _ => IsoLevel::ReadCommitted, // all-zero mix: degenerate profile
            };
            let iso_node = match level {
                IsoLevel::ReadCommitted => pr::ISO_RC,
                IsoLevel::RepeatableRead => pr::ISO_RR,
                IsoLevel::Serializable => pr::ISO_SER,
            };
            self.commit_path(pr::STMT_TX, vec![pr::TX_BEGIN.into(), iso_node.into()]);
            self.session.in_tx = true;
            self.session.savepoints.clear();
            self.session.sp_snapshots.clear();
            self.session.tx_snapshot = Some(TxSnapshot {
                schema: self.schema.snapshot(),
                gucs_set: self.session.gucs_set,
            });
            return Step::Tx(TxCtl::Begin(level));
        }
        // In tx: commit 3 / rollback 1 / savepoint 2 / rollback-to 2.
        let has_sp = !self.session.savepoints.is_empty();
        let choice =
            weighted_index(&mut self.rng, &[3, 1, 2, if has_sp { 2 } else { 0 }]).expect("nonzero");
        const TX_SUBS: [&str; 4] =
            [pr::TX_COMMIT, pr::TX_ROLLBACK, pr::TX_SAVEPOINT, pr::TX_ROLLBACK_TO];
        self.commit_path(pr::STMT_TX, vec![TX_SUBS[choice].into()]);
        match choice {
            0 => {
                self.session.in_tx = false;
                self.session.savepoints.clear();
                self.session.sp_snapshots.clear();
                self.session.tx_snapshot = None;
                Step::Tx(TxCtl::Commit)
            }
            1 => {
                // ROLLBACK reverts everything since BEGIN on the server —
                // including DDL and SET — so the model reverts with it
                // (transactional-DDL law; without this the rest of the plan
                // addresses tables the server rolled away).
                let snap = self.session.tx_snapshot.take().expect("in_tx implies tx snapshot");
                self.schema.restore(snap.schema);
                self.session.gucs_set = snap.gucs_set;
                self.session.in_tx = false;
                self.session.savepoints.clear();
                self.session.sp_snapshots.clear();
                Step::Tx(TxCtl::Rollback)
            }
            2 => {
                self.session.next_savepoint += 1;
                let name = format!("sp{}", self.session.next_savepoint);
                self.session.savepoints.push(name.clone());
                self.session.sp_snapshots.push(TxSnapshot {
                    schema: self.schema.snapshot(),
                    gucs_set: self.session.gucs_set,
                });
                Step::Tx(TxCtl::Savepoint(name))
            }
            3 => {
                let i =
                    range_incl(&mut self.rng, 0, self.session.savepoints.len() as u64 - 1) as usize;
                let name = self.session.savepoints[i].clone();
                // ROLLBACK TO keeps the savepoint itself but destroys later
                // ones; the server reverts to the state as of the SAVEPOINT
                // statement, and the snapshot stays live for repeated
                // ROLLBACK TO.
                let snap = self.session.sp_snapshots[i].clone();
                self.schema.restore(snap.schema);
                self.session.gucs_set = snap.gucs_set;
                self.session.savepoints.truncate(i + 1);
                self.session.sp_snapshots.truncate(i + 1);
                Step::Tx(TxCtl::RollbackTo(name))
            }
            _ => unreachable!(),
        }
    }

    fn gen_arm_step(&mut self) -> Step {
        // Occasionally reset if something is set; otherwise apply a
        // serial-safe arm SET from the profile — ATOMICALLY, as consecutive
        // SET steps via the pending queue (H4 arm-set fidelity: flattened
        // one-GUC-per-draw arms almost never compose a multi-GUC set like the
        // parallel-forcing arm within a session — the p6 replant lesson).
        if self.session.gucs_set && range_incl(&mut self.rng, 0, 3) == 0 {
            self.session.gucs_set = false;
            self.commit_path(pr::STMT_ARM, vec![pr::ARM_RESET_ALL.into()]);
            return Step::Arm(ArmCtl::ResetAll);
        }
        // Pool = profile arm sets ++ per-seed sampled planner-knob sets
        // (H6). Trace indexes address the combined pool: indexes >=
        // profile.arm_sets.len() are sampled sets (whose CONTENT varies per
        // seed; the index SPACE is stable per profile).
        let pool_len = self.profile.arm_sets.len() + self.sampled_arm_sets.len();
        let i = range_incl(&mut self.rng, 0, pool_len as u64 - 1) as usize;
        self.trace.borrow_mut().arm_set_hits.push(i);
        let set = if i < self.profile.arm_sets.len() {
            self.profile.arm_sets[i].clone()
        } else {
            self.sampled_arm_sets[i - self.profile.arm_sets.len()].clone()
        };
        let mut it = set.into_iter();
        let Some((k, v)) = it.next() else {
            // Empty set = the profile's base-clean control arm: defaults.
            self.session.gucs_set = false;
            self.commit_path(pr::STMT_ARM, vec![pr::ARM_RESET_ALL.into()]);
            return Step::Arm(ArmCtl::ResetAll);
        };
        self.commit_path(pr::STMT_ARM, vec![pr::ARM_APPLY_SET.into()]);
        for (k2, v2) in it {
            self.pending.push_back(PlanItem::Step(Step::Arm(ArmCtl::SetGuc(k2, v2))));
        }
        self.session.gucs_set = true;
        Step::Arm(ArmCtl::SetGuc(k, v))
    }

    fn gen_property_item(&mut self) -> Option<PlanItem> {
        let caps = self.schema.caps();
        let eligible: Vec<&Box<dyn PropertyGen>> = self
            .registry
            .iter()
            .filter(|p| {
                caps.contains(p.required_caps())
                    && p.weight(self.profile) > 0
                    && footprint_fits(&self.budgets, &p.footprint())
            })
            .collect();
        let weights: Vec<u64> = eligible.iter().map(|p| p.weight(self.profile)).collect();
        let i = weighted_index(&mut self.rng, &weights)?;
        let prop = eligible[i];
        let name = prop.name();
        let fp = prop.footprint();
        // Trace rollback mark: if instantiation fails BELOW (returns None),
        // any noise-query traces it committed describe statements that never
        // reached the plan — truncate back so the metric only sees emissions.
        let mark = {
            let t = self.trace.borrow();
            (t.paths.len(), t.arm_set_hits.len())
        };
        let mut noise_ctx =
            NoiseCtx { schema: &self.schema, profile: self.profile, trace: &self.trace };
        let generated = prop.generate(&mut self.rng, &self.schema, &mut noise_ctx, self.profile);
        // Budget is consumed whether or not instantiation succeeded, so a
        // permanently-unsatisfiable property cannot stall the plan.
        self.budgets.consume_footprint(&fp);
        let Some(generated) = generated else {
            let mut t = self.trace.borrow_mut();
            t.paths.truncate(mark.0);
            t.arm_set_hits.truncate(mark.1);
            return None;
        };
        self.commit_path(pr::STMT_PROPERTY, vec![format!("{}{}", pr::PROP_PREFIX, name)]);
        let seq = self.next_seq;
        self.next_seq += 1;
        Some(PlanItem::Property {
            name: name.to_string(),
            seq,
            tables: generated.tables,
            steps: generated.steps,
        })
    }

    /// Produce the next plan item (lazy generation: the plan is an iterator).
    pub fn next_item(&mut self) -> Option<PlanItem> {
        if let Some(item) = self.pending.pop_front() {
            return Some(item);
        }
        if self.finished {
            return None;
        }
        // First interaction is always CREATE TABLE (spec §1.1).
        if !self.emitted_first {
            self.emitted_first = true;
            let sql = noise::gen_create_table(&mut self.schema, &mut self.rng, self.profile);
            self.commit_path(pr::STMT_DDL, vec![pr::DDL_CREATE_TABLE.into()]);
            self.budgets.consume(Kind::Ddl, 1);
            return Some(PlanItem::Step(Step::Ddl(sql)));
        }
        loop {
            let weights = self.kind_weights();
            let Some(kind_i) = weighted_index(&mut self.rng, &weights) else {
                // Nothing choosable (budgets exhausted or only un-generatable
                // kinds left): emit the cleanup tail and finish.
                self.cleanup_tail();
                return self.pending.pop_front();
            };
            match kind_i {
                0 => {
                    // H6: a DDL decision may emit a short chain of consecutive
                    // statements (the fdw setup); each emitted statement gets
                    // its own trace path and one unit of DDL budget.
                    let stmts =
                        noise::gen_ddl(&mut self.schema, &mut self.rng, self.profile);
                    self.budgets.consume(Kind::Ddl, stmts.len() as u64);
                    let mut it = stmts.into_iter();
                    let (sub, sql) = it.next().expect("gen_ddl emits at least one stmt");
                    self.commit_path(pr::STMT_DDL, sub);
                    for (sub2, sql2) in it {
                        self.commit_path(pr::STMT_DDL, sub2);
                        self.pending.push_back(PlanItem::Step(Step::Ddl(sql2)));
                    }
                    return Some(PlanItem::Step(Step::Ddl(sql)));
                }
                1 => {
                    let mut sub = Vec::new();
                    if let Some(sql) = noise::gen_dml(
                        &mut self.schema,
                        &mut self.rng,
                        self.profile,
                        &mut sub,
                    ) {
                        self.commit_path(pr::STMT_DML, sub);
                        self.budgets.consume(Kind::Dml, 1);
                        return Some(PlanItem::Step(Step::Dml(sql)));
                    }
                    self.budgets.consume(Kind::Dml, 1);
                }
                2 => {
                    let mut sub = Vec::new();
                    if let Some(sql) =
                        noise::gen_query(&self.schema, self.profile, &mut self.rng, &mut sub)
                    {
                        self.commit_path(pr::STMT_QUERY, sub);
                        self.budgets.consume(Kind::Query, 1);
                        return Some(PlanItem::Step(Step::Query(sql)));
                    }
                    self.budgets.consume(Kind::Query, 1);
                }
                3 => {
                    self.budgets.consume(Kind::Tx, 1);
                    let step = self.gen_tx_step();
                    return Some(PlanItem::Step(step));
                }
                4 => {
                    self.budgets.consume(Kind::Arm, 1);
                    let step = self.gen_arm_step();
                    return Some(PlanItem::Step(step));
                }
                5 => {
                    // Disconnect + ReconnectServer as an adjacent pair (v1
                    // policy — the runner reconnects immediately). Session
                    // state does not survive the disconnect, and the server
                    // ABORTS any open tx — its DDL rolls back, so the model's
                    // schema reverts to the BEGIN snapshot with it.
                    self.budgets.consume(Kind::Fault, 2);
                    self.commit_path(pr::STMT_FAULT, vec![pr::FAULT_DISCONNECT_PAIR.into()]);
                    if let Some(snap) = self.session.tx_snapshot.take() {
                        self.schema.restore(snap.schema);
                    }
                    self.session.in_tx = false;
                    self.session.savepoints.clear();
                    self.session.sp_snapshots.clear();
                    self.session.gucs_set = false;
                    self.pending
                        .push_back(PlanItem::Step(Step::Fault(FaultPoint::ReconnectServer)));
                    return Some(PlanItem::Step(Step::Fault(FaultPoint::Disconnect)));
                }
                6 => {
                    // Integration note (harness/h1): v1 properties assume an
                    // AUTOCOMMIT context — expected-error properties (F4, F5)
                    // deliberately error, which would abort an enclosing open
                    // tx and roll the property's own tables away (42P01 FP
                    // class), and tx-opening properties (M1, F8) would nest
                    // BEGIN. Close any open tx first: emit COMMIT, then the
                    // property (via the pending queue).
                    let committed_first = if self.session.in_tx {
                        self.session.in_tx = false;
                        self.session.savepoints.clear();
                        self.session.sp_snapshots.clear();
                        self.session.tx_snapshot = None;
                        true
                    } else {
                        false
                    };
                    let item = self.gen_property_item();
                    if committed_first {
                        if let Some(item) = item {
                            self.pending.push_back(item);
                        }
                        return Some(PlanItem::Step(Step::Tx(TxCtl::Commit)));
                    }
                    if let Some(item) = item {
                        return Some(item);
                    }
                    // Instantiation failed; budget already consumed. Loop.
                }
                _ => unreachable!(),
            }
        }
    }
}

/// Generate a full plan for `seed` under `profile`.
///
/// `profile_sha256` and `generator` go into the header verbatim; both are
/// version pins, not entropy (timestamps never enter plan bytes).
pub fn generate_plan(
    seed: u64,
    profile: &GenProfile,
    profile_sha256: &str,
    generator: &str,
    registry: &[Box<dyn PropertyGen>],
) -> Plan {
    generate_plan_traced(seed, profile, profile_sha256, generator, registry).0
}

/// Like `generate_plan`, additionally returning the H5 production traces
/// (one path per emitted statement + arm-set application indexes). Trace
/// collection consumes no RNG draws: the plan is byte-identical either way.
pub fn generate_plan_traced(
    seed: u64,
    profile: &GenProfile,
    profile_sha256: &str,
    generator: &str,
    registry: &[Box<dyn PropertyGen>],
) -> (Plan, GenTraces) {
    let header = PlanHeader {
        seed,
        profile: profile.name.clone(),
        profile_sha256: profile_sha256.to_string(),
        generator: generator.to_string(),
    };
    let mut g = Generator::new(seed, profile, registry);
    let mut items = Vec::new();
    while let Some(item) = g.next_item() {
        items.push(item);
    }
    let traces = g.trace.into_inner();
    (Plan { header, items }, traces)
}
