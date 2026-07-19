//! F4 ConstraintError: constraint violations fail with the expected
//! SQLSTATE class (23xxx), checked in both directions — the ledger predicts
//! the error, the engine must produce it (and vice versa on the clean leg).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Row, Value};
use crate::oracle::ledger::LedgerOp;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, PropertyInstance, PStep, SqlMeta, SqlStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f4");
    let k = Value::Int(rng.gen_range(0i64..1_000_000));
    let first = Row(vec![k.clone(), Value::Int(1)]);

    // Variant: duplicate unique key (23505) or NOT NULL violation (23502).
    let dup_variant = rng.gen_bool(0.5);
    let bad_row = if dup_variant {
        Row(vec![k.clone(), Value::Int(2)])
    } else {
        Row(vec![Value::Null, Value::Int(3)])
    };
    let bad_insert = SqlStep {
        sql: format!(
            "INSERT INTO {table} (k, v) VALUES ({}, {})",
            bad_row.0[0].sql(),
            bad_row.0[1].sql()
        ),
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: Some(LedgerOp::InsertValues { table: table.clone(), rows: vec![bad_row] }),
        probe: None,
        stackref: Some(0),
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &[first])),
        h::sql(bad_insert),
        PStep::Assert(Check::SqlStateClass { slot: 0, class: "23".into() }),
        // Statement-level rollback: the failed insert must not have changed
        // the table.
        h::sql(h::select_all(&table, 1)),
        PStep::Assert(Check::LedgerTable { table: table.clone(), slot: 1 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::F4ConstraintError,
        steps,
        tables: BTreeSet::from([table]),
    }
}
