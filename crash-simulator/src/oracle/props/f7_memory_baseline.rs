//! F7 MemoryBaseline (catalog rank #1): memory-context accounting returns to
//! its watermark after a workload — via the engine-hook channel (contract
//! §0 A5). Hook absent => ASSUME fails => property auto-skipped with
//! `SIMHARNESS|skipped-no-hook|n`, never a silent pass.
//!
//! CHANNEL (pinned at H4): the watermark is the backend's live memory-context
//! COUNT via the ported `pg_backend_memory_contexts` view (engine SRF
//! `fc_pg_get_backend_memory_contexts`, crates/backend/utils/adt/mcxtfuncs).
//! The p2 leak class (FunctionScan argcontext, ~1 leaked context per SRF
//! execution — h3 evidence: 38 -> 239 view rows after 400 unnest() calls)
//! moves this scalar on the very next probe. The capability probe keys off
//! HookKind::Memory (runner probes WATERMARK_SQL at connect), not the text.
//!
//! Warm-baseline discipline (spec §2.1): the workload runs ONCE before the
//! 'before' watermark is taken (first execution legitimately grows
//! catcache/relcache/plancache) and the probe itself is warmed with a scratch
//! execution, then the measured workload runs between the asserted probes.
//!
//! The workload includes the SRF/FunctionScan step the hook observes
//! (H4 charter: F7 hook + SRF workload — the p2 catch needs both halves).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, HookKind};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep};

/// The F7 watermark channel. Runner-side capability probing executes this at
/// connect; the property executes it as an uncompared Passthrough probe.
pub const WATERMARK_SQL: &str = "SELECT count(*) FROM pg_backend_memory_contexts";

/// Scratch (unasserted) slots; asserted probes use slots 0/1.
const SLOT_SCRATCH_COUNT: u32 = u32::MAX - 1;
const SLOT_SCRATCH_PROBE: u32 = u32::MAX - 2;

fn watermark_probe(slot: u32) -> SqlStep {
    SqlStep {
        sql: WATERMARK_SQL.to_string(),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::HookScalar { hook: HookKind::Memory }),
        stackref: Some(slot),
    }
}

/// FunctionScan SRF step (the p2 argcontext-leak site). Deterministic int
/// unnest; compared (Read) under diff-c — sort-normalized, so array order is
/// never a finding.
fn srf_step(rng: &mut impl Rng) -> SqlStep {
    let a = rng.gen_range(0i64..100);
    let b = rng.gen_range(0i64..100);
    let c = rng.gen_range(0i64..100);
    SqlStep {
        sql: format!("SELECT x FROM unnest(ARRAY[{a}, {b}, {c}]) AS u(x)"),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    }
}

fn workload(rng: &mut impl Rng, table: &str) -> Vec<PStep> {
    let rows = h::gen_rows(rng, 2);
    vec![
        h::sql(h::create_kv(table)),
        h::sql(h::insert_rows(table, &rows)),
        h::sql(h::count_all(table, SLOT_SCRATCH_COUNT)), // scratch slot, unasserted
        h::sql(srf_step(rng)),
        h::sql(srf_step(rng)),
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
    // Warm-up execution (baseline discipline) + probe warm-up (the probe's
    // own first execution grows caches too; scratch slot, unasserted).
    steps.extend(workload(rng, &t_warm));
    steps.push(h::sql(watermark_probe(SLOT_SCRATCH_PROBE)));
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
