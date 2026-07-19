//! L2 NoREC — Non-optimizing Reference Engine Construction (Rigger & Su,
//! ESEC/FSE 2020), the second reference-free metamorphic oracle: take the
//! OPTIMIZED query `SELECT .. FROM t WHERE p` (the planner may push p into an
//! index scan, reorder, etc.) and the NON-OPTIMIZABLE rewrite
//! `SELECT (p) FROM t` (p is in the projection — the planner cannot push it;
//! it is evaluated row-by-row over a plain scan). The optimized query's ROW
//! COUNT must equal the rewrite's TRUE count (`NorecRowCountEq`, counted
//! harness-side). Disagreement = an optimizer bug in the one engine under
//! test — no reference engine consulted.
//!
//! A serial-safe plan-forcing arm from the profile (e.g. enable_seqscan=off)
//! is applied around the OPTIMIZED leg only, then RESET (1session GUC-leak
//! law) — widening the plan diversity the law is checked across.
//!
//! Targets: property-local kvs table (sim-answerable; parity/teeth tests) or
//! a live typed-generator table (campaign-mutated data; probes None).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::l1_tlp::{gen_pred, live_surface};
use crate::oracle::props::{helpers as h, PredColKind, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    partition_sql, ArmCtl, Mark, NoiseConstraint, ProbeSpec, PropertyInstance, PStep,
    SqlMeta, SqlStep, TriSel,
};

fn read_step(sql: String, probe: Option<ProbeSpec>, slot: u32) -> PStep {
    PStep::Sql(SqlStep {
        sql,
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe,
        stackref: Some(slot),
    })
}

/// Push the optimized leg (slot 0) under a random profile arm set, RESET
/// after, then the non-optimizable rewrite (slot 1) and the NoREC assert.
fn norec_tail(
    rng: &mut impl Rng,
    profile: &ProfileView,
    steps: &mut Vec<PStep>,
    optimized: PStep,
    unoptimized: PStep,
) {
    let armed = !profile.arm_sets.is_empty();
    if armed {
        let arm = &profile.arm_sets[rng.gen_range(0..profile.arm_sets.len())];
        for (k, v) in arm {
            steps.push(PStep::Arm(ArmCtl::SetGuc(k.clone(), v.clone())));
        }
    }
    steps.push(optimized);
    if armed {
        steps.push(PStep::Arm(ArmCtl::ResetAll));
    }
    steps.push(unoptimized);
    steps.push(PStep::Assert(Check::NorecRowCountEq { optimized: 0, unoptimized: 1 }));
}

fn generate_local(rng: &mut impl Rng, profile: &ProfileView) -> PropertyInstance {
    let table = h::fresh_table(rng, "l2");
    let n = rng.gen_range(5..=12);
    let rows = h::gen_rows_kvs(rng, n);
    let surface = [(1usize, PredColKind::Int), (2usize, PredColKind::Text)];
    let pred = gen_pred(rng, &surface);

    let optimized = read_step(
        format!(
            "SELECT k FROM {table} WHERE {} ORDER BY k",
            partition_sql(&pred, TriSel::True, &h::KVS_COL_NAMES)
        ),
        Some(ProbeSpec::RowsWherePred {
            table: table.clone(),
            pred: pred.clone(),
            sel: Some(TriSel::True),
        }),
        0,
    );
    let unoptimized = read_step(
        format!("SELECT ({}) FROM {table}", pred.sql(&h::KVS_COL_NAMES)),
        Some(ProbeSpec::PredProjection { table: table.clone(), pred: pred.clone() }),
        1,
    );

    let mut steps = vec![
        h::sql(h::create_kvs(&table)),
        h::sql(h::insert_rows_kvs(&table, &rows)),
        PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
            [table.clone()].into_iter().collect(),
        )),
    ];
    norec_tail(rng, profile, &mut steps, optimized, unoptimized);
    steps.push(h::sql(h::drop_table(&table)));

    PropertyInstance {
        property: PropertyId::L2NoRec,
        steps,
        tables: BTreeSet::from([table]),
    }
}

pub fn generate(
    rng: &mut impl Rng,
    schema: &SchemaView,
    profile: &ProfileView,
) -> PropertyInstance {
    // Live typed-generator target 1/2 of the time when tables exist (empty
    // schema — unit/sim tests — always local).
    if !schema.tables.is_empty() && rng.gen_range(0u32..2) == 0 {
        let t = &schema.tables[rng.gen_range(0..schema.tables.len())];
        let (surface, names) = live_surface(t);
        if !surface.is_empty() {
            let pred = gen_pred(rng, &surface);
            let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            let tname = &t.name;
            let optimized = read_step(
                format!(
                    "SELECT id FROM {tname} WHERE {} ORDER BY id",
                    partition_sql(&pred, TriSel::True, &name_refs)
                ),
                None,
                0,
            );
            let unoptimized =
                read_step(format!("SELECT ({}) FROM {tname}", pred.sql(&name_refs)), None, 1);
            let mut steps = vec![PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
                [tname.clone()].into_iter().collect(),
            ))];
            norec_tail(rng, profile, &mut steps, optimized, unoptimized);
            return PropertyInstance {
                property: PropertyId::L2NoRec,
                steps,
                tables: BTreeSet::from([t.birth_id.clone()]),
            };
        }
    }
    generate_local(rng, profile)
}
