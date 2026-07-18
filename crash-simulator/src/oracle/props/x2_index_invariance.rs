//! X2 IndexInvariance: adding an index never changes READ results
//! (multiset-equal before/after CREATE INDEX).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::l1_tlp::count_pred;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    Mark, PredSpec, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TriSel,
};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "x2");
    let n = rng.gen_range(3..=8);
    let rows = h::gen_rows(rng, n);
    let m = rng.gen_range(2i64..5);
    let r = rng.gen_range(0..m);
    let pred = PredSpec::ColModEq { col: 1, m, r };

    let create_index = SqlStep {
        sql: format!("CREATE INDEX {table}_v_idx ON {table} (v)"),
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: None, // index DDL changes no row state; not ledger-modeled
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(h::select_all(&table, 0)),
        h::sql(count_pred(&table, &pred, TriSel::True, 1)),
        h::sql(create_index),
        h::sql(h::select_all(&table, 2)),
        h::sql(count_pred(&table, &pred, TriSel::True, 3)),
        PStep::Assert(Check::MultisetEq { a: 0, b: 2 }),
        PStep::Assert(Check::ScalarPairEq { a: 1, b: 3 }),
        h::sql(h::drop_table(&table)), // drops the index with it
    ];

    PropertyInstance {
        property: PropertyId::X2IndexInvariance,
        steps,
        tables: BTreeSet::from([table]),
    }
}
