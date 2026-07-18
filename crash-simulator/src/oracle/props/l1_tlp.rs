//! L1 TLP (catalog rank #4): ternary-logic partitioning — for predicate p,
//! |WHERE p| + |WHERE NOT p| + |WHERE p IS NULL| == |all|. Cardinality
//! identity, no ledger state needed (spec §1.2). WHERE-variant at v1.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    Mark, NoiseConstraint, PredSpec, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TriSel,
};

pub(crate) fn pred_sql(pred: &PredSpec, sel: TriSel, col_name: &str) -> String {
    let PredSpec::ColModEq { m, r, .. } = pred;
    match sel {
        TriSel::True => format!("({col_name} % {m}) = {r}"),
        TriSel::False => format!("NOT (({col_name} % {m}) = {r})"),
        TriSel::Null => format!("(({col_name} % {m}) = {r}) IS NULL"),
    }
}

pub(crate) fn count_pred(table: &str, pred: &PredSpec, sel: TriSel, slot: u32) -> SqlStep {
    SqlStep {
        sql: format!("SELECT count(*) FROM {table} WHERE {}", pred_sql(pred, sel, "v")),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::CountWherePred {
            table: table.into(),
            pred: pred.clone(),
            sel,
        }),
        stackref: Some(slot),
    }
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "l1");
    // gen_rows gives ~1/4 NULL v: the IS NULL partition is genuinely
    // exercised (3VL, not decoration).
    let n = rng.gen_range(4..=10);
    let rows = h::gen_rows(rng, n);
    let m = rng.gen_range(2i64..6);
    let r = rng.gen_range(0..m);
    let pred = PredSpec::ColModEq { col: 1, m, r };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
            [table.clone()].into_iter().collect(),
        )),
        h::sql(h::count_all(&table, 0)),
        h::sql(count_pred(&table, &pred, TriSel::True, 1)),
        h::sql(count_pred(&table, &pred, TriSel::False, 2)),
        h::sql(count_pred(&table, &pred, TriSel::Null, 3)),
        PStep::Assert(Check::ScalarSumEq { parts: vec![1, 2, 3], whole: 0 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::L1Tlp,
        steps,
        tables: BTreeSet::from([table]),
    }
}
