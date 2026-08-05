//! F3 DeleteAbsence: a key-addressed DELETE removes exactly that row; the
//! deleted key probes absent and total cardinality matches the ledger.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Value};
use crate::oracle::ledger::LedgerOp;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, NoiseConstraint, PropertyInstance, PStep, SqlMeta, SqlStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f3");
    let n = rng.gen_range(2..=4);
    let rows = h::gen_rows(rng, n);
    let victim = rows[rng.gen_range(0..rows.len())].0[0].clone();

    let delete = SqlStep {
        sql: format!("DELETE FROM {table} WHERE k = {}", victim.sql()),
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: Some(LedgerOp::DeleteByKey { table: table.clone(), key: victim.clone() }),
        probe: None,
        stackref: None,
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(delete),
        PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
            [table.clone()].into_iter().collect(),
        )),
        h::sql(h::count_where_key(&table, &victim, 0)),
        PStep::Assert(Check::ScalarEq { slot: 0, value: Value::Int(0) }),
        h::sql(h::count_all(&table, 1)),
        PStep::Assert(Check::CountMatchesLedger { table: table.clone(), slot: 1 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::F3DeleteAbsence,
        steps,
        tables: BTreeSet::from([table]),
    }
}
