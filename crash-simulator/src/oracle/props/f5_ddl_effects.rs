//! F5 DdlEffects: DDL takes effect and mis-DDL fails with the right
//! SQLSTATE — double CREATE => 42P07, post-DROP probe => 42P01.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Value};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f5");

    let dup_create = {
        let mut s = h::create_kv(&table);
        s.stackref = Some(1);
        // ledger_op stays CreateTable: the ledger predicts 42P07 on the dup.
        s
    };
    let post_drop_probe = SqlStep {
        sql: format!("SELECT count(*) FROM {table}"),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::CountAll { table: table.clone() }),
        stackref: Some(2),
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::count_all(&table, 0)),
        PStep::Assert(Check::ScalarEq { slot: 0, value: Value::Int(0) }),
        h::sql(dup_create),
        PStep::Assert(Check::SqlStateClass { slot: 1, class: "42P07".into() }),
        h::sql(h::drop_table(&table)),
        h::sql(post_drop_probe),
        PStep::Assert(Check::SqlStateClass { slot: 2, class: "42P01".into() }),
    ];

    PropertyInstance {
        property: PropertyId::F5DdlEffects,
        steps,
        tables: BTreeSet::from([table]),
    }
}
