//! F6 AggConsistency: cardinality identities over exact types (R7) —
//! COUNT(*) = ledger cardinality, SUM composes over a partition, UNION ALL
//! doubles the multiset. No ledger-side aggregate evaluation: these are
//! identities, not expression modeling (the punt fence holds).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    partition_sql, Mark, PredSpec, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TriSel,
};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f6");
    let n = rng.gen_range(3..=8);
    let rows = h::gen_rows(rng, n);
    let m = rng.gen_range(2i64..5);
    let r = rng.gen_range(0..m);
    // Partition on k (NOT NULL): the IS NULL partition is provably empty,
    // exercising the NULL-sum-is-zero identity leg.
    let pred = PredSpec::ColModEq { col: 0, m, r };

    let sum_probe = |slot: u32, filter: Option<(PredSpec, TriSel)>| {
        let where_clause = match &filter {
            None => String::new(),
            Some((pred, sel)) => {
                format!(" WHERE {}", partition_sql(pred, *sel, &h::KVS_COL_NAMES))
            }
        };
        SqlStep {
            sql: format!("SELECT sum(v) FROM {table}{where_clause}"),
            mark: Mark::Read,
            meta: SqlMeta::default(),
            ledger_op: None,
            probe: Some(ProbeSpec::SumCol { table: table.clone(), col: 1, filter }),
            stackref: Some(slot),
        }
    };

    let union_single = SqlStep {
        sql: format!("SELECT k FROM {table}"),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::SelectColAll { table: table.clone(), col: 0, doubled: false }),
        stackref: Some(5),
    };
    let union_doubled = SqlStep {
        sql: format!("SELECT k FROM {table} UNION ALL SELECT k FROM {table}"),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::SelectColAll { table: table.clone(), col: 0, doubled: true }),
        stackref: Some(6),
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(h::count_all(&table, 0)),
        PStep::Assert(Check::CountMatchesLedger { table: table.clone(), slot: 0 }),
        h::sql(sum_probe(1, None)),
        h::sql(sum_probe(2, Some((pred.clone(), TriSel::True)))),
        h::sql(sum_probe(3, Some((pred.clone(), TriSel::False)))),
        h::sql(sum_probe(4, Some((pred, TriSel::Null)))),
        PStep::Assert(Check::ScalarSumEq { parts: vec![2, 3, 4], whole: 1 }),
        h::sql(union_single),
        h::sql(union_doubled),
        PStep::Assert(Check::UnionDoubling { single: 5, doubled: 6 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::F6AggConsistency,
        steps,
        tables: BTreeSet::from([table]),
    }
}
