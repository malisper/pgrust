//! F2 UpdateVisibility: before/after probes bracket a key-addressed UPDATE;
//! the after-probe sees the new value and the table matches the ledger.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Value};
use crate::oracle::ledger::LedgerOp;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f2");
    let k = Value::Int(rng.gen_range(0i64..1_000_000));
    let v1 = Value::Int(rng.gen_range(0i64..100));
    let v2 = Value::Int(rng.gen_range(100i64..200)); // disjoint from v1's range
    let row = crate::oracle::check::Row(vec![k.clone(), v1.clone()]);

    let probe = |slot: u32, table: &str, k: &Value| SqlStep {
        sql: format!("SELECT v FROM {table} WHERE k = {}", k.sql()),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::SelectColByKey { table: table.into(), col: 1, key: k.clone() }),
        stackref: Some(slot),
    };

    let update = SqlStep {
        sql: format!("UPDATE {table} SET v = {} WHERE k = {}", v2.sql(), k.sql()),
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: Some(LedgerOp::UpdateByKey {
            table: table.clone(),
            key: k.clone(),
            sets: vec![(1, v2.clone())],
        }),
        probe: None,
        stackref: None,
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &[row])),
        h::sql(probe(0, &table, &k)),
        PStep::Assert(Check::ScalarEq { slot: 0, value: v1 }),
        h::sql(update),
        h::sql(probe(1, &table, &k)),
        PStep::Assert(Check::ScalarEq { slot: 1, value: v2 }),
        h::sql(h::select_all(&table, 2)),
        PStep::Assert(Check::LedgerTable { table: table.clone(), slot: 2 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::F2UpdateVisibility,
        steps,
        tables: BTreeSet::from([table]),
    }
}
