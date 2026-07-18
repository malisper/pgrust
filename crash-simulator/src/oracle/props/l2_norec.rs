//! L2 NoREC (catalog rank #8): the optimized count (WHERE p) equals the
//! non-optimizable form (sum of CASE WHEN p THEN 1 ELSE 0 END), across a
//! plan-forcing arm. Arms are serial-safe (contract §3.3); RESET ALL after
//! (1session GUC-leak law).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::Check;
use crate::oracle::props::l1_tlp::{count_pred, pred_sql};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{
    ArmCtl, Mark, PredSpec, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TriSel,
};

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "l2");
    let n = rng.gen_range(4..=10);
    let rows = h::gen_rows(rng, n);
    let m = rng.gen_range(2i64..6);
    let r = rng.gen_range(0..m);
    let pred = PredSpec::ColModEq { col: 1, m, r };

    let norec = SqlStep {
        sql: format!(
            "SELECT sum(CASE WHEN {} THEN 1 ELSE 0 END) FROM {table}",
            pred_sql(&pred, TriSel::True, "v")
        ),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::NoRecSum { table: table.clone(), pred: pred.clone() }),
        stackref: Some(1),
    };

    let mut steps = vec![
        h::sql(h::create_kv(&table)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(count_pred(&table, &pred, TriSel::True, 0)),
    ];
    // Re-evaluate the NoREC form under a plan-forcing arm.
    if !profile.arm_sets.is_empty() {
        let arm = &profile.arm_sets[rng.gen_range(0..profile.arm_sets.len())];
        for (k, v) in arm {
            steps.push(PStep::Arm(ArmCtl::SetGuc(k.clone(), v.clone())));
        }
    }
    steps.push(h::sql(norec));
    steps.push(PStep::Arm(ArmCtl::ResetAll));
    steps.push(PStep::Assert(Check::ScalarPairEq { a: 0, b: 1 }));
    steps.push(h::sql(h::drop_table(&table)));

    PropertyInstance {
        property: PropertyId::L2NoRec,
        steps,
        tables: BTreeSet::from([table]),
    }
}
