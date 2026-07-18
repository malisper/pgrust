//! X1 ArmEquivalence (catalog rank #3): the same READ under different GUC
//! arms is multiset-equal. Exact-type compared positions only by default
//! (R7 — the byref-merge catch row, contract §3.3; must not be weakened).
//! RESET ALL between arms (1session GUC-leak law).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{ArmCtl, PropertyInstance, PStep};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "x1");
    let n = rng.gen_range(3..=8);
    let rows = h::gen_rows(rng, n);

    let mut steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(h::select_all(&table, 0)),
    ];
    let mut slot = 1u32;
    let mut asserts = Vec::new();
    for arm in &profile.arm_sets {
        for (k, v) in arm {
            steps.push(PStep::Arm(ArmCtl::SetGuc(k.clone(), v.clone())));
        }
        steps.push(h::sql(h::select_all(&table, slot)));
        steps.push(PStep::Arm(ArmCtl::ResetAll));
        asserts.push(PStep::Assert(Check::MultisetEq { a: 0, b: slot }));
        slot += 1;
    }
    steps.extend(asserts);
    steps.push(h::sql(h::drop_table(&table)));

    PropertyInstance {
        property: PropertyId::X1ArmEquivalence,
        steps,
        tables: BTreeSet::from([table]),
    }
}
