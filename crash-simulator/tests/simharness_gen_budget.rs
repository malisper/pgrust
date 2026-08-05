//! G-G1 (part): budget-steering distribution test — a 10k-step plan matches
//! the profile weights within tolerance, including with a property in the
//! registry (property-embedded steps count against the same budgets, so the
//! distribution holds ACROSS noise and property-embedded queries).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rand::RngCore;

use simharness::gen::generate_plan;
use simharness::gen::profile::{
    ColTypeWeights, GenProfile, IsoMix, PlanLen, StatementWeights, TableShape,
};
use simharness::gen::schema::SchemaState;
use simharness::plan::{Plan, PlanItem, Step};
use simharness::property::{
    Caps, Footprint, GeneratedProperty, NoiseSource, PropertyGen,
};

fn test_profile(weights: StatementWeights, len: u64) -> GenProfile {
    GenProfile {
        name: "budget-test".into(),
        plan_len: PlanLen { min: len, max: len },
        statement_weights: weights,
        table_shape: TableShape {
            min_cols: 2,
            max_cols: 4,
            col_types: ColTypeWeights { int: 4, bigint: 2, text: 3, numeric: 2, float8: 0 },
            rows_max: 200,
        },
        iso_mix: IsoMix { rc: 70, rr: 20, ser: 10 },
        arm_sets: vec![vec![("work_mem".into(), "64kB".into())]],
        property_weights: BTreeMap::new(),
        float_lenient: false,
        test_disable_productions: Vec::new(),
        planner_knobs: None,
        multi_session: false,
    }
}

#[derive(Default)]
struct Counts {
    ddl: u64,
    dml: u64,
    query: u64,
    tx: u64,
    arm: u64,
    fault: u64,
    props: u64,
}

fn census(plan: &Plan) -> Counts {
    let mut c = Counts::default();
    let count_step = |c: &mut Counts, s: &Step| match s {
        Step::Ddl(_) => c.ddl += 1,
        Step::Dml(_) => c.dml += 1,
        Step::Query(_) => c.query += 1,
        Step::Tx(_) => c.tx += 1,
        Step::Arm(_) => c.arm += 1,
        Step::Fault(_) => c.fault += 1,
        Step::Assumption(_) | Step::Assertion(_) => {}
        // H8 session-family steps are property-internal choreography; they
        // draw no statement-kind budget.
        Step::Session(_) | Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_) => {}
    };
    for item in &plan.items {
        match item {
            PlanItem::Step(s) => count_step(&mut c, s),
            PlanItem::Property { steps, .. } => {
                c.props += 1;
                for s in steps {
                    count_step(&mut c, s);
                }
            }
        }
    }
    c
}

fn assert_close(actual: u64, expected: u64, total: u64, what: &str) {
    // Tolerance: 1.5% of total steps + small constant slack for the cleanup
    // tail / pairing mechanics. Budgets are hard integer allocations, so the
    // error should be well under this.
    let tol = (total * 15 / 1000).max(8);
    let diff = actual.abs_diff(expected);
    assert!(
        diff <= tol,
        "{what}: actual {actual} vs expected {expected} (tolerance {tol}, total {total})"
    );
}

#[test]
fn budget_distribution_10k_steps_no_properties() {
    let weights =
        StatementWeights { ddl: 5, dml: 30, query: 40, tx: 15, arm: 5, fault: 5, property: 0 };
    let profile = test_profile(weights, 10_000);
    let plan = generate_plan(4242, &profile, "ab", "g", &[]);
    let c = census(&plan);
    let total = 10_000u64;
    assert_close(c.ddl, total * 5 / 100, total, "ddl");
    assert_close(c.dml, total * 30 / 100, total, "dml");
    assert_close(c.query, total * 40 / 100, total, "query");
    assert_close(c.tx, total * 15 / 100, total, "tx");
    assert_close(c.arm, total * 5 / 100, total, "arm");
    assert_close(c.fault, total * 5 / 100, total, "fault");
    assert_eq!(c.props, 0);
}

/// Stub property: 2 constrained noise queries + 1 DML step, touching one table.
struct StubProp;

impl PropertyGen for StubProp {
    fn name(&self) -> &'static str {
        "StubReadRead"
    }

    fn required_caps(&self) -> Caps {
        Caps::HAS_TABLE
    }

    fn footprint(&self) -> Footprint {
        Footprint { query: 2, dml: 1, ..Footprint::default() }
    }

    fn generate(
        &self,
        rng: &mut dyn RngCore,
        schema: &SchemaState,
        noise: &mut dyn NoiseSource,
        _profile: &GenProfile,
    ) -> Option<GeneratedProperty> {
        // Constrained placeholder noise: draw real queries from the workload
        // distribution, constrained to plain (non-aggregate) reads.
        let constraint = |q: &simharness::plan::Sql| !q.text().contains("sum(");
        let q1 = noise.noise_query(rng, &constraint)?;
        let q2 = noise.noise_query(rng, &constraint)?;
        let t = schema.tables().first()?;
        let dml = simharness::plan::Sql::new(
            format!("DELETE FROM {} WHERE id = 0;", t.cur_name),
            simharness::plan::Mark::Mutation,
            simharness::plan::SqlFlags::default(),
        )
        .ok()?;
        let mut tables = BTreeSet::new();
        tables.insert(t.birth_id.clone());
        Some(GeneratedProperty {
            steps: vec![Step::Query(q1), Step::Query(q2), Step::Dml(dml)],
            tables,
        })
    }
}

#[test]
fn budget_distribution_holds_across_property_embedded_steps() {
    let weights =
        StatementWeights { ddl: 5, dml: 25, query: 40, tx: 10, arm: 5, fault: 5, property: 10 };
    let profile = test_profile(weights, 10_000);
    let registry: Vec<Box<dyn PropertyGen>> = vec![Box::new(StubProp)];
    let plan = generate_plan(777, &profile, "ab", "g", &registry);
    let c = census(&plan);
    let total = 10_000u64;
    // Property blocks fired and carry their table deps.
    assert!(c.props > 0, "stub property never fired");
    for item in &plan.items {
        if let PlanItem::Property { name, tables, steps, .. } = item {
            assert_eq!(name, "StubReadRead");
            assert!(!tables.is_empty());
            assert_eq!(steps.len(), 3);
        }
    }
    // The distribution law: property-embedded queries/DML consume the same
    // budgets, so TOTAL query/dml counts (noise + embedded) still track the
    // profile shares.
    assert_close(c.query, total * 40 / 100, total, "query (incl. property-embedded)");
    assert_close(c.dml, total * 25 / 100, total, "dml (incl. property-embedded)");
    // Property budget = 10% of total; each block consumes one property unit.
    // Structural tail-race allowance (measured, H6): a property fire needs
    // remaining query >= 2 AND dml >= 1, so when those pools exhaust first
    // the tail of the property budget is dead — the deficit is ~15% of the
    // property budget and shifts a few blocks with ANY generator-version
    // change (base h5 = 853, h6-grammar = 847 at seed 777). 2% of total
    // keeps teeth against real starvation without pinning the race.
    let props_expected = total * 10 / 100;
    let props_tol = total * 2 / 100;
    assert!(
        c.props.abs_diff(props_expected) <= props_tol,
        "property blocks: actual {} vs expected {props_expected} (tolerance {props_tol}, total {total})",
        c.props
    );
}

#[test]
fn first_interaction_is_always_create_table() {
    for seed in 0..50u64 {
        let profile = test_profile(StatementWeights::default(), 60);
        let plan = generate_plan(seed, &profile, "ab", "g", &[]);
        match plan.items.first() {
            Some(PlanItem::Step(Step::Ddl(sql))) => {
                assert!(
                    sql.text().starts_with("CREATE TABLE "),
                    "seed {seed}: first interaction must be CREATE TABLE, got {}",
                    sql.text()
                );
            }
            other => panic!("seed {seed}: first item not a DDL step: {other:?}"),
        }
    }
}

#[test]
fn plans_end_clean_no_dangling_tx_or_guc() {
    // Session-model invariant: any open tx is closed and set GUCs reset by the
    // deterministic cleanup tail (1session GUC-leak law). The mini-model here
    // is tx-aware: non-LOCAL SET (and RESET ALL) is TRANSACTIONAL in
    // PostgreSQL, so its effect inside a rolled-back tx/subtx reverts on the
    // server — the generator models exactly that (tx-DDL/GUC fix, review
    // BLOCKING-1), and this replay must too. Full statement-level coherence
    // lives in tests/simharness_gen_coherence.rs.
    for seed in 100..150u64 {
        let profile = test_profile(StatementWeights::default(), 80);
        let plan = generate_plan(seed, &profile, "ab", "g", &[]);
        let mut in_tx = false;
        let mut gucs = false;
        let mut gucs_at_begin = false;
        let mut sp_gucs: Vec<(String, bool)> = Vec::new();
        let steps = plan.items.iter().flat_map(|it| match it {
            PlanItem::Step(s) => std::slice::from_ref(s).iter(),
            PlanItem::Property { steps, .. } => steps.iter(),
        });
        for s in steps {
            match s {
                Step::Tx(simharness::plan::TxCtl::Begin(_)) => {
                    in_tx = true;
                    gucs_at_begin = gucs;
                    sp_gucs.clear();
                }
                Step::Tx(simharness::plan::TxCtl::Commit) => {
                    in_tx = false;
                    sp_gucs.clear();
                }
                Step::Tx(simharness::plan::TxCtl::Rollback) => {
                    in_tx = false;
                    gucs = gucs_at_begin;
                    sp_gucs.clear();
                }
                Step::Tx(simharness::plan::TxCtl::Savepoint(n)) => {
                    sp_gucs.push((n.clone(), gucs));
                }
                Step::Tx(simharness::plan::TxCtl::RollbackTo(n)) => {
                    let i = sp_gucs
                        .iter()
                        .rposition(|(sn, _)| sn == n)
                        .unwrap_or_else(|| panic!("seed {seed}: phantom savepoint '{n}'"));
                    gucs = sp_gucs[i].1;
                    sp_gucs.truncate(i + 1);
                }
                Step::Arm(simharness::plan::ArmCtl::SetGuc(..)) => gucs = true,
                Step::Arm(simharness::plan::ArmCtl::ResetAll) => gucs = false,
                Step::Fault(simharness::plan::FaultPoint::Disconnect) => {
                    // Disconnect aborts any open tx and the fresh session
                    // after reconnect has no GUCs set at all.
                    in_tx = false;
                    gucs = false;
                    sp_gucs.clear();
                }
                _ => {}
            }
        }
        assert!(!in_tx, "seed {seed}: plan ends inside a transaction");
        assert!(!gucs, "seed {seed}: plan ends with GUCs set (RESET ALL law)");
    }
}
