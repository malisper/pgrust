//! X4 StatementForm: the same logical query through a different statement
//! form yields the same multiset. v1 form pair: plain SELECT vs
//! PREPARE/EXECUTE (cursor-fetch-all and COPY TO forms need runner-side
//! support and land as follow-on forms).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "x4");
    let stmt_name = format!("{table}_ps");
    let n = rng.gen_range(2..=6);
    let rows = h::gen_rows(rng, n);

    let prepare = SqlStep {
        sql: format!("PREPARE {stmt_name} AS SELECT k, v FROM {table}"),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    };
    let execute = SqlStep {
        sql: format!("EXECUTE {stmt_name}"),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::SelectAll { table: table.clone() }),
        stackref: Some(1),
    };
    let deallocate = SqlStep {
        sql: format!("DEALLOCATE {stmt_name}"),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    };

    let steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(h::select_all(&table, 0)),
        h::sql(prepare),
        h::sql(execute),
        h::sql(deallocate),
        PStep::Assert(Check::MultisetEq { a: 0, b: 1 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::X4StatementForm,
        steps,
        tables: BTreeSet::from([table]),
    }
}
