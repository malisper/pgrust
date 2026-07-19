//! F8 ResourceBaseline (catalog rank #2): fd-class resource counters return
//! to baseline between statements. Hook-gated exactly like F7 (contract §0
//! A5); the channel was WIRED at H7 (it was a placeholder from H1 through
//! H6 — the property was loudly vacuous via `skipped-no-hook`).
//!
//! CHANNEL (pinned at H7): `SHOW pgrust.resource_counters` — an engine
//! PGC_INTERNAL computed GUC (guc_tables + the fd crate's show hook). The
//! scalar reports the ABOVE-VFD-CACHE counters (spec §2.1 "vfd-aware by
//! definition": LRU-cached vfds legitimately stay open between statements
//! and must not be counted): live allocated transient descs, the
//! allocated-desc cap, the thread's max_safe_fds, max_files_per_process.
//! The probe is Passthrough (uncompared): the C leg has no such GUC and
//! errors 42704 — uncompared by design, and both probes sit outside any tx
//! so the symmetric error-recovery ROLLBACK is a no-op.
//!
//! WORKLOAD (the fd-pressure knob, h3 queue #6): a file_fdw foreign table
//! read through a UNION ALL of `FT_BRANCHES` branches. Append initializes
//! every child Foreign Scan up front, and each file_fdw scan holds one
//! AllocateFile desc for the whole statement — so one statement holds
//! FT_BRANCHES allocated descs SIMULTANEOUSLY. FT_BRANCHES = 20 sits above
//! the FD_MINFREE/3 = 16 cap a backend thread is stuck with when it never
//! inherited the postmaster's probed max_safe_fds (the p4 bug class:
//! "exceeded maxAllocatedDescs (16)"), and comfortably below the scaled
//! max_safe_fds/3 cap of a healthy backend (>= 21 needs max_safe_fds >= 63;
//! the validate rig raises ulimit -n so the probe is ~330 on author boxes).
//! ASSERT StmtOk on the measured run is the detector: the fd-cap refusal is
//! a property-violation P1, not just a rust-err-c-ok P2 census line.
//!
//! Warm-baseline discipline (spec §2.1, the F7 pattern): the pressure query
//! runs ONCE before the 'before' probe (first execution settles the lazily
//! scaled allocated-desc cap and grows catcache/relcache), and the probe is
//! warmed with a scratch execution; the measured run sits between the
//! asserted probes, which must then be equal (allocated descs back to 0,
//! caps and max_safe_fds stable).

use rand::Rng;
use std::collections::BTreeSet;

use crate::oracle::check::{Check, HookKind};
use crate::oracle::props::{helpers as h, ProfileView, PropertyId, SchemaView};
use crate::oracle::pstep::{Mark, ProbeSpec, PropertyInstance, PStep, SqlMeta, SqlStep};

/// The F8 counter channel. Runner-side capability probing executes this at
/// connect; the property executes it as an uncompared Passthrough probe.
pub const COUNTER_SQL: &str = "SHOW pgrust.resource_counters";

/// UNION ALL branches of the pressure query: > 16 (the stranded-thread
/// allocated-desc cap, FD_MINFREE/3) and < the healthy scaled cap.
pub const FT_BRANCHES: usize = 20;

/// Scratch (unasserted) slots; asserted probes use slots 0/1, the measured
/// pressure query uses slot 2.
const SLOT_SCRATCH_WARM: u32 = u32::MAX - 1;
const SLOT_SCRATCH_PROBE: u32 = u32::MAX - 2;
const SLOT_MEASURED: u32 = 2;

fn counter_probe(slot: u32) -> SqlStep {
    SqlStep {
        sql: COUNTER_SQL.to_string(),
        mark: Mark::Passthrough,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::HookScalar { hook: HookKind::Resource }),
        stackref: Some(slot),
    }
}

fn mutation(sql: String) -> SqlStep {
    SqlStep {
        sql,
        mark: Mark::Mutation,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: None,
    }
}

/// The fd-pressure read: one Append over FT_BRANCHES Foreign Scans of the
/// same file_fdw table => FT_BRANCHES simultaneous AllocateFile descs.
/// Compared (Read): count(*) = FT_BRANCHES * rows on both legs.
fn pressure_step(ft: &str, slot: u32) -> SqlStep {
    let branches: Vec<String> =
        (0..FT_BRANCHES).map(|_| format!("SELECT id FROM {ft}")).collect();
    SqlStep {
        sql: format!(
            "SELECT count(*) FROM ({}) AS u",
            branches.join(" UNION ALL ")
        ),
        mark: Mark::Read,
        meta: SqlMeta::default(),
        ledger_op: None,
        probe: Some(ProbeSpec::Opaque),
        stackref: Some(slot),
    }
}

pub fn generate(
    rng: &mut impl Rng,
    _schema: &SchemaView,
    _profile: &ProfileView,
) -> PropertyInstance {
    let ft = h::fresh_table(rng, "f8ft");
    // CSV convention mirrors the H6 noise fdw chain: an absolute /tmp path
    // both legs write with IDENTICAL deterministic bytes (legs execute
    // sequentially, so the double write is a byte-identical overwrite).
    let csv = format!("/tmp/simharness_{ft}.csv");
    let rows = rng.gen_range(5i64..=30);

    let steps = vec![
        PStep::Assume(Check::HookPresent { hook: HookKind::Resource }),
        // file_fdw estate (idempotent; database-global bits use IF NOT
        // EXISTS — same conventions as the generator's fdw setup chain).
        PStep::Sql(mutation("CREATE EXTENSION IF NOT EXISTS file_fdw".into())),
        PStep::Sql(mutation(
            "CREATE SERVER IF NOT EXISTS simharness_fsrv FOREIGN DATA WRAPPER file_fdw".into(),
        )),
        PStep::Sql(mutation(format!(
            "COPY (SELECT g AS id FROM generate_series(1, {rows}) AS g ORDER BY g) \
             TO '{csv}' WITH (FORMAT csv)"
        ))),
        PStep::Sql(mutation(format!(
            "CREATE FOREIGN TABLE {ft} (id bigint) SERVER simharness_fsrv \
             OPTIONS (filename '{csv}', format 'csv')"
        ))),
        // Warm-up execution (settles the lazily scaled desc cap + caches),
        // then probe warm-up — both scratch, unasserted.
        PStep::Sql(pressure_step(&ft, SLOT_SCRATCH_WARM)),
        PStep::Sql(counter_probe(SLOT_SCRATCH_PROBE)),
        PStep::Sql(counter_probe(0)),
        // Measured execution: MUST succeed (the fd-cap refusal fires here on
        // a max_safe_fds-stranded backend) and must restore the baseline.
        // Assert order: baseline first (its `before` slot is the first
        // asserted slot — the planted-red unit leg perturbs that), StmtOk
        // second; both always evaluate over already-filled slots.
        PStep::Sql(pressure_step(&ft, SLOT_MEASURED)),
        PStep::Sql(counter_probe(1)),
        PStep::Assert(Check::HookBaseline { hook: HookKind::Resource, before: 0, after: 1 }),
        PStep::Assert(Check::StmtOk { slot: SLOT_MEASURED }),
        PStep::Sql(mutation(format!("DROP FOREIGN TABLE {ft}"))),
    ];

    PropertyInstance {
        property: PropertyId::F8ResourceBaseline,
        steps,
        tables: BTreeSet::from([ft]),
    }
}
