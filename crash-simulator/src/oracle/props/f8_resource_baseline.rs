//! F8 ResourceBaseline (catalog rank #2): fd/pin/lock/snapshot counters
//! return to baseline at transaction end. Hook-gated exactly like F7
//! (contract §0 A5). The fd leg is vfd-aware by definition (spec §2.1): the
//! engine hook must report above-cache counters; pins/locks/snapshots assert
//! exact return-to-zero at tx end.
//!
//! COORDINATION: probe `pgrust_test_resource_counters()` is a placeholder
//! until the engine mini-lane pins the channel.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, HookKind};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep, TxCtl};

fn counter_probe(slot: u32) -> SqlStep {
    SqlStep {
        sql: "SELECT pgrust_test_resource_counters()".to_string(),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::HookScalar { hook: HookKind::Resource }),
        stackref: Some(slot),
    }
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    profile: &ProfileView,
) -> PropertyInstance {
    let table = h::fresh_table(rng, "f8");
    let n = rng.gen_range(1..=3);
    let rows = h::gen_rows(rng, n);
    let iso = profile.iso_mix[rng.gen_range(0..profile.iso_mix.len())];

    let steps = vec![
        PStep::Assume(Check::HookPresent { hook: HookKind::Resource }),
        h::sql(h::create_kv(&table)),
        h::sql(counter_probe(0)),
        // Tx workload: pins/locks/snapshots must return to baseline at COMMIT.
        PStep::Tx(TxCtl::Begin(iso)),
        h::sql(h::insert_rows(&table, &rows)),
        h::sql(h::count_all(&table, u32::MAX - 1)), // scratch, unasserted
        PStep::Tx(TxCtl::Commit),
        h::sql(counter_probe(1)),
        PStep::Assert(Check::HookBaseline { hook: HookKind::Resource, before: 0, after: 1 }),
        h::sql(h::drop_table(&table)),
    ];

    PropertyInstance {
        property: PropertyId::F8ResourceBaseline,
        steps,
        tables: BTreeSet::from([table]),
    }
}
