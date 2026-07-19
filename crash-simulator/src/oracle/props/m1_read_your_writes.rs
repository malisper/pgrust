//! M1 ReadYourWrites (step-level, purely serial — the one M-family survivor
//! of contract §0 A1): inside a transaction, the session sees its own
//! uncommitted writes; savepoint rollback un-sees exactly the rolled-back
//! ops; commit makes them durable. This property end-to-end exercises the
//! ledger's snapshot + op-log + savepoint-mark machinery.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, Value};
use crate::oracle::ledger::LedgerOp;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, PropertyInstance, PStep, SqlMeta, SqlStep, TxCtl};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "m1");
    let n = rng.gen_range(1..=3);
    let rows = h::gen_rows(rng, n);
    let pivot = rows[rng.gen_range(0..rows.len())].0[0].clone();
    let iso = profile.iso_mix[rng.gen_range(0..profile.iso_mix.len())];
    let sp = format!("{table}_sp");

    let delete = SqlStep {
        sql: format!("DELETE FROM {table} WHERE k = {}", pivot.sql()),
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: Some(LedgerOp::DeleteByKey { table: table.clone(), key: pivot.clone() }),
        probe: None,
        stackref: None,
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        PStep::Tx(TxCtl::Begin(iso)),
        h::sql(h::insert_rows(&table, &rows)),
        // Read-your-writes: uncommitted insert visible to this session.
        h::sql(h::count_where_key(&table, &pivot, 0)),
        PStep::Assert(Check::ScalarEq { slot: 0, value: Value::Int(1) }),
        PStep::Tx(TxCtl::Savepoint(sp.clone())),
        h::sql(delete),
        h::sql(h::count_where_key(&table, &pivot, 1)),
        PStep::Assert(Check::ScalarEq { slot: 1, value: Value::Int(0) }),
        // Savepoint rollback un-sees the delete, keeps the insert.
        PStep::Tx(TxCtl::RollbackTo(sp)),
        h::sql(h::count_where_key(&table, &pivot, 2)),
        PStep::Assert(Check::ScalarEq { slot: 2, value: Value::Int(1) }),
        PStep::Tx(TxCtl::Commit),
        // Still there after commit; full content matches the ledger.
        h::sql(h::count_where_key(&table, &pivot, 3)),
        PStep::Assert(Check::ScalarEq { slot: 3, value: Value::Int(1) }),
        h::sql(h::select_all(&table, 4)),
        PStep::Assert(Check::LedgerTable { table: table.clone(), slot: 4 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::M1ReadYourWrites,
        steps,
        tables: BTreeSet::from([table]),
    }
}
