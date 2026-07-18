//! F7 MemoryBaseline (catalog rank #1): memory-context accounting returns to
//! its watermark after a workload — via the engine-hook channel (contract
//! §0 A5: sibling engine mini-lane). Hook absent => ASSUME fails => property
//! auto-skipped with `SIMHARNESS|skipped-no-hook|n`, never a silent pass.
//!
//! Warm-baseline discipline (spec §2.1): the workload runs ONCE before the
//! 'before' watermark is taken (first execution legitimately grows
//! catcache/relcache/plancache), then again between the probes.
//!
//! COORDINATION: probe function name `pgrust_test_mcxt_watermark()` is a
//! placeholder until the engine mini-lane pins the channel; the capability
//! probe keys off HookKind::Memory, not the SQL text.

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, HookKind};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep};

fn watermark_probe(slot: u32) -> SqlStep {
    SqlStep {
        sql: "SELECT pgrust_test_mcxt_watermark()".to_string(),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::HookScalar { hook: HookKind::Memory }),
        stackref: Some(slot),
    }
}

fn workload(rng: &mut impl Rng, table: &str) -> Vec<PStep> {
    let rows = h::gen_rows(rng, 2);
    vec![
        h::sql(h::create_kv(table)),
        h::sql(h::insert_rows(table, &rows)),
        h::sql(h::count_all(table, u32::MAX - 1)), // scratch slot, unasserted
        h::sql(h::drop_table(table)),
    ]
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let t_warm = h::fresh_table(rng, "f7w");
    let t_meas = h::fresh_table(rng, "f7m");

    let mut steps = vec![PStep::Assume(Check::HookPresent { hook: HookKind::Memory })];
    // Warm-up execution (baseline discipline).
    steps.extend(workload(rng, &t_warm));
    steps.push(h::sql(watermark_probe(0)));
    // Measured execution.
    steps.extend(workload(rng, &t_meas));
    steps.push(h::sql(watermark_probe(1)));
    steps.push(PStep::Assert(Check::HookBaseline {
        hook: HookKind::Memory,
        before: 0,
        after: 1,
    }));

    PropertyInstance {
        property: PropertyId::F7MemoryBaseline,
        steps,
        tables: BTreeSet::from([t_warm, t_meas]),
    }
}
