//! F1 InsertSelect (spec Appendix A): insert fresh rows into a
//! property-local table; through arbitrary noise (must not touch the table),
//! a keyed SELECT still finds the fresh row and the full table matches the
//! ledger multiset. Oracle: ledger row-multiset, fresh-row pivot.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{NoiseConstraint, PropertyInstance, PStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f1");
    let n = rng.gen_range(1..=3);
    let rows = h::gen_rows(rng, n);
    let pivot = rows[rng.gen_range(0..rows.len())].0[0].clone();

    let mut steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        PStep::NoiseSlot(NoiseConstraint::MustNotTouch(
            [table.clone()].into_iter().collect(),
        )),
        h::sql(h::count_where_key(&table, &pivot, 0)),
        PStep::Assert(Check::ScalarEq { slot: 0, value: crate::oracle::check::Value::Int(1) }),
        h::sql(h::select_all(&table, 1)),
        PStep::Assert(Check::LedgerTable { table: table.clone(), slot: 1 }),
    ];
    steps.push(h::sql(h::drop_table(&table)));

    PropertyInstance {
        property: PropertyId::F1InsertSelect,
        steps,
        tables: BTreeSet::from([table]),
    }
}
