//! M3 RUNTIME HASH JOIN — the shared-build hash join on the morsel runtime
//! (docs/design/m3-joins.md; parent plan parallelism-redesign-2026-07 §5-M3)
//! + the M3.5 SPILL inc-4/5 batch arm (docs/design/m3.5-spill.md §5: batch
//! files on the SpillSet substrate, PLAN-BATCHES with exact sizes, chained
//! per-leaf build/probe task sets, recursive splits with a depth cap).
//!
//! Shape (phase 1): a SERIAL-plan plain Agg over a HashJoin over two
//! lane-fusible pgrcolumnar SeqScans. UNBATCHED (budget fits — the dormant
//! default) it is THREE runtime task sets:
//!
//!   [0] BUILD-ACCEPT   inner-scan granules → filter/project → per-worker
//!                      JoinBuildLocal (materialize + count; sink accept)
//!   [1] BUILD-COMBINE  256 partitions, deps=[0] — partitioned single-writer
//!                      table construction; finalize publishes the frozen
//!                      table (the ParallelSink pair via sink_tasksets)
//!   [2] PROBE          outer-scan granules, deps=[1] — per row: hash → tag
//!                      → chain → recheck → joinqual/otherqual → null-fill
//!                      arms → the plain-agg partial absorb (M1's
//!                      runtime_partial tail)
//!
//! BATCHED (M3.5, admission estimates nbatch > 1 and the spill arm is on),
//! the same RG grows the batch axis — a STATIC ladder (the runtime's task
//! sets and deps are fixed at submit; sources whose sizes only PLAN-BATCHES
//! knows are DEFERRED — their granule totals are set before their set can
//! publish, sequenced by the deps DAG):
//!
//!   [0] BUILD-ACCEPT   rows route by batch: batch 0 → the Local (as above);
//!                      batch k>0 → the worker's inner batch file (M3.5
//!                      batch records on SpillSet). Batch-0 budget crossing
//!                      DEMOTES batch 0 to a file batch (§5.2) instead of
//!                      refusing.
//!   [1] BUILD-COMBINE  batch-0 table (skipped when demoted).
//!   [2] PLAN-BATCHES   single task: EXACT per-batch sizes from the spill
//!                      directories + router counters → leaf map; batches
//!                      over the envelope enqueue SPLIT rounds.
//!   [3..3+R)           SPLIT-ROUND r: repartition oversized batches by
//!                      deeper remix bits into child files; still-oversized
//!                      children go to the next round; depth cap → refusal.
//!   [P]  PROBE(0)      outer rows route by the FROZEN leaf map: in-memory
//!                      leaf probes inline; file leaves append (hash, tuple)
//!                      to the worker's outer file. The map never changes
//!                      after this point — outer files are never
//!                      repartitioned (§5.2/§5.3).
//!   [P+1] FILL(0)      right-fill family only.
//!   then per leaf slot i (chained, ONE LIVE TABLE at a time — C parity):
//!      ACCEPT(i)       inner leaf extents → per-worker Locals
//!      COMBINE(i)      leaf table build + publish
//!      PROBE(i)        outer leaf extents → probe + absorb
//!      FILL(i)         right-fill family only; drops the leaf table.
//!
//! Engagement layering (identical to M1/M2): PGRUST_RUNTIME=1 +
//! `SET pgrust.runtime_hashjoin_pool = <dop>` + lane master switch, with
//! `PGRUST_RUNTIME_HASHJOIN=0` as the dedicated arm kill and
//! `PGRUST_RUNTIME_HASHJOIN_SPILL=0` restoring the phase-1 nbatch>1 refusal
//! exactly. The plan surface stays the serial plan; every refusal falls
//! through to the serial arms byte-identically (nothing consumed).
//!
//! Ordering contract (Michael's 2026-07-13 directive): order-insensitive
//! emission is the baseline; the probe feeds an order-insensitive plain-agg
//! partial tail, and the gates use tie-normalized comparison. Batch
//! processing order is a scheduling choice, not a semantic one (§9).
//!
//! Memory (§6/§7): admission decides batching with the C combined rule
//! (`exec_choose_hash_table_size_full(try_combined_hash_mem=true)`); each
//! live table — batch 0 and every file leaf — gets the RAW combined
//! envelope `get_hash_memory_limit() × (dop+1)` (one gang-shared table at
//! a time; exec_choose's rebalance-reduced `space_allowed` sized C's
//! per-worker tables and over-fanned splits — the train-14 inc-4/5 ledger
//! item). PLAN-BATCHES checks EXACT file bytes + tuple counts against an
//! exact-arithmetic capacity model (never estimates — the agg/distinct
//! duplicate-inflation class does not exist here); router buffers are
//! bounded constants per worker. Every spill syscall brackets the SpillIo
//! blocking-section facade (inside spillset).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ::executils::{EStateData, ExecSlotId};
use ::nodeagg::runtime_partial::{
    agg_grouped_runtime_combine, agg_runtime_combine, agg_runtime_export_partial_into,
    agg_runtime_partial_admissible, exec_agg_runtime_partials, GroupedRuntimePartial,
    RuntimePartial,
};
use ::nodehashjoin::batch::{
    batch_of, batch_record_push, estimate_batch_table_mem, split_child, BatchRecords, LeafMap,
    LEAF_INMEM,
};
use ::nodehashjoin::shared_build::{
    finish_single_pass, freeze, BudgetExceeded, CombinePlan, FrozenJoinTable, JoinBudget,
    JoinBuildLocal, SharedBuildDir, PARTITIONS,
};
use ::nodehashjoin::shared_exec::{
    shared_build_accept, shared_build_accept_keyed, shared_build_hash_tuple,
    shared_fill_partition, shared_join_admissible, shared_probe_outer, shared_probe_outer_dense,
    shared_probe_outer_hash, shared_probe_outer_hashed,
    shared_saved_outer_slot,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;
use ::types_tuple::MinimalTupleData;

use super::batch_source::{
    heapfeed_v2_enabled, BatchGranuleSource, HeapBatchSource, SeqScanSource,
};
use super::router::{self, ArmClass, ArmCounter};
use super::runtime_agg::ExitBump;
use super::runtime_scan::{exprs_parallel_safe, PgrcolumnarGranuleSource};
use super::stats::{self, RefuseReason, ShapeClass};
use super::{lane_trace, seq_scan_fusible};

// ---------------------------------------------------------------------------
// M3.5 knobs (env-gated, OnceLock like the agg/distinct arms).
// ---------------------------------------------------------------------------

/// Router flush threshold: staged record bytes per worker before an epoch
/// flush (bounded per-participant buffer memory, §7).
const HJ_ROUTER_FLUSH: usize = 8 << 20;
/// Leaf-build chunk growth ceiling: bounds the PLAN-BATCHES capacity
/// model's per-worker last-chunk waste term. SCALED to the envelope
/// (space/8 across the gang, clamped to the Local's 64KB..16MB ladder) —
/// a flat per-worker constant would doom small-budget engagements (the
/// waste term alone would exceed the envelope).
fn leaf_chunk_cap_bytes(space_allowed: usize, dop: u64) -> u64 {
    ((space_allowed as u64) / (8 * dop.max(1))).clamp(64 << 10, 1 << 20)
}
/// Split-node id space (= split-round file partition space).
const MAX_SPLIT_NODES: usize = 1024;

fn min_granules() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_MIN_GRANULES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}

/// The M3.5 join-batch spill arm: ON by default (the refusal→engagement
/// charter); `PGRUST_RUNTIME_HASHJOIN_SPILL=0` restores the phase-1
/// nbatch>1 refusal exactly.
fn hj_spill_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL").map_or(true, |v| v.trim() != "0")
    })
}

/// Leaf cap = max file batches (level-0 and post-split combined) one
/// engagement may carry; beyond it the join refuses to the serial arm
/// (recorded honest limit: max spilled inner ≈ cap × combined envelope).
fn hj_spill_max_batches() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL_BATCHES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(32)
            .clamp(2, 128)
    })
}

/// Split rounds declared past the admission nbatch (§5.3 depth cap).
fn hj_spill_rounds() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL_DEPTH")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(2)
            .clamp(1, 4)
    })
}

/// TEST-ONLY underestimate forcing: engage batched with exactly this many
/// level-0 batches regardless of the admission estimate (the split-path e2e
/// leg needs a deterministic "estimate lied" shape). Absent in production.
fn hj_spill_force_batches() -> Option<u32> {
    static N: OnceLock<Option<u32>> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_SPILL_FORCE_BATCHES")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .filter(|&n| n >= 2)
            .map(|n| n.next_power_of_two())
    })
}

/// m5p1 multibuild (band 88001): the multi-pipeline QuerySpec plan-walk —
/// 2+ build sides in ONE engagement (a TREE of probe-local hash joins over
/// fusible SeqScans feeding the plain-agg sink). ON by default;
/// `PGRUST_RUNTIME_HASHJOIN_MULTIBUILD=0` restores the single-join-only
/// admission (nested trees refuse exactly where they always did).
fn hj_multibuild_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_MULTIBUILD").map_or(true, |v| v.trim() != "0")
    })
}

/// Defensive cap on multibuild tree size (joins per engagement); beyond it
/// the walk refuses to the serial arms (task-set fan bound).
const MB_MAX_JOINS: usize = 8;

/// SINGLE-PASS build (Phase 1a; gather-elimination-plan §1a): fuse the
/// two-pass materialize→COMBINE build into ONE pass — each build tuple is
/// CAS-inserted directly into the shared directory during accept, killing the
/// COMBINE re-read that loses 1.14–1.50× vs PG Parallel Hash / Umbra above
/// ~2M rows. OFF BY DEFAULT: two-pass stays the default until a per-shape
/// fleet A/B proves ≥ parity (the low-distinct/skew CAS-contention crossover
/// must be measured, not assumed — see shared_build.rs contention note).
/// `PGRUST_RUNTIME_HJ_SINGLEPASS=1` engages it for UNBATCHED single-join
/// engagements only; batched/spill and multibuild stay two-pass this phase.
fn hj_singlepass_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_HJ_SINGLEPASS").map_or(false, |v| v.trim() == "1")
    })
}

/// SE-MBSHARED (the GL-MULTIBUILD-1 lane): the shared-probe/shared-build
/// completion of the multibuild walk. DEFAULT ON (flipped-kill, t35 idiom:
/// `PGRUST_LANE_V2_MBSHARED=0|off` restores the pre-letter walk
/// byte-identically). Letter basis (GL-MULTIBUILD-1,
/// notes/gl-multibuild-1-letter.md): strictly-better-or-flat at every
/// re-measured refuted cell (rt/legacy 2.9-6.3x -> 1.2-2.2x, one witnessed
/// win; single-join controls knob-flat; census spotcheck byte-identical
/// both postures; 43q control pair flat). Two halves, one knob:
///
/// 1. PROBE HOIST: `mb_row` stops refcounting the frozen-table Arc per
///    emitted row. The witnessed profile at the refuted grid cells (L1/L2,
///    notes/runtime-cost-ladder-specs.md) put ~2/3 of all busy CPU on the
///    two per-row refcount RMWs — every worker hammering the same two
///    cache lines once per row per probe level is exactly the anti-scaling
///    signature those grids show (worse at dop16 than dop4). The borrow is
///    field-disjoint, so the hoisted walk passes the table by reference —
///    same probe order, same emission, byte-identical results.
/// 2. SINGLE-PASS BUILDS (Phase 1a, multibuild twin): each join's build
///    table gets a [`SharedBuildDir`] sized from ITS OWN planner estimate
///    and charged to ITS OWN budget (the per-table combined envelope —
///    every live table keeps the `work_mem x (dop+1)` rule, C parity with
///    one Hash node per join); accepts CAS-insert directly, the 256
///    combine tasks become no-ops, and the COMBINE re-read bandwidth pass
///    disappears. An estimate the directory cannot afford falls back to
///    the two-pass build for THAT table (never a refusal on this account —
///    the single-join 1a posture verbatim).
///
/// The build scheduling itself is unchanged: every build side is already
/// its own claimable task set, deps-ordered before its probers (the m5p1
/// decomposition below); this knob changes what a claim DOES, not who
/// claims what. Grouped (SE-AGGJOIN) engagements ride the same walk and
/// the same knob.
fn hj_mbshared_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_MBSHARED").as_deref(), Ok("0") | Ok("off"))
    })
}

/// SE-MBSEAT (the GL-MBSEAT-1 lane): per-table dense seats on the
/// multibuild walk's single-pass tables — the order-free CSR built at the
/// freeze barrier from `(packed_ref, key)` pairs
/// (`shared_build::build_seat_single_pass`), probed via the single-join
/// arm's exact `shared_probe_outer_dense` dispatch. Targets the named
/// GL-MULTIBUILD-1 residual: the v1 per-row probe-walk gap (outer-hash
/// interpreter dispatch + bucket/tag lookup + hashvalue prefilter +
/// hashclauses recheck, skipped exactly when int4 key equality IS the
/// hash-match semantics — the `dense_seat_build_col` introspection, per
/// join). Economics per TABLE: probe estimate (the join's OUTER subtree
/// plan_rows) >= SEAT_MIN_PROBE_RATIO x build estimate (the GL-HJSEAT-2
/// constant, PROVISIONAL reuse — GL-MBSEAT-1 re-measures); seat arrays
/// charge each table's OWN budget optionally (forgo, never refuse).
/// DEFAULT ON (flipped-kill, t35 idiom — GL-MBSEAT-1 letter basis:
/// strictly-better-or-flat at every measured walk cell, controls
/// knob-flat, byte-identity across the gates; `=0|off` kills). COMPOSES
/// with MBSHARED — the seat rides the sealed single-pass directory, so a
/// thrown `PGRUST_LANE_V2_MBSHARED=0|off` kill inertly disarms this car
/// too (compose, never contradict).
fn hj_mbseat_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_MBSEAT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// SE-AGGJOIN grouped sink (band 87001): GROUPED (AGG_HASHED) agg roots over
/// the multibuild join walk — per-worker hashed builds exported as
/// self-contained grouped partials, combined on the leader, absorbed into
/// the serial node's own table for the canonical retrieve. ON by default;
/// `PGRUST_RUNTIME_HASHJOIN_GROUPSINK=0` restores the grouped refusal
/// exactly (and un-keys the planner probe — knob coherence, same spelling).
fn hj_groupsink_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_GROUPSINK").map_or(true, |v| v.trim() != "0")
    })
}

/// SE-DECOROOT (the GL-DECOROOT-1 lane) — executor half: engage the
/// grouped-join sink when the Agg sits ONE WHITELISTED DECORATION CHAIN
/// below the plan root (`[Limit] -> [Sort] -> Agg`): the arm fills the full
/// grouped table (fill-only — no first-row retrieve) and the serial
/// Sort/Limit above consumes it off the filled table, exactly the scan-side
/// sinks' standing under-Sort/Limit posture (runtime_distinct's "the Agg
/// need not be the plan root" law). DEFAULT ON (conversion-flips train,
/// GL-DECOROOT-1 — see the planner twin's doc for the letter numbers);
/// `PGRUST_LANE_V2_DECOROOT=0|off` is the kill switch — the SAME spelling
/// as the planner probe (knob coherence: a keyed decorated shape whose arm
/// is disarmed would suppress Gather and land on the serial join build;
/// BOTH read sites flip together).
fn hj_decoroot_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_DECOROOT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// SE-NUMJOIN (the GL-NUMJOIN-1 lane) — executor half: admit the
/// SE-AGGPOLY manifest schema (sum/avg(NUMERIC) over free arg exprs +
/// exportable lane kinds) into the join sinks' export/combine/absorb — the
/// relocated NumericAgg digit-snapshot states, C numeric_avg_combine field
/// law, exact deferred additions at absorb. Plan-covered shapes are
/// UNTOUCHED (the schema derivation tries the fold plan first). DEFAULT
/// ON (conversion-flips train, GL-NUMJOIN-1 — the planner twin's doc carries
/// the letter numbers incl. the named multibuild-numeric@6M ~parity spot);
/// `PGRUST_LANE_V2_AGGJOIN_NUMERIC=0|off` is the kill switch — same
/// spelling as the planner probe (knob coherence; BOTH sites flip
/// together).
fn hj_aggjoin_numeric_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_AGGJOIN_NUMERIC").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// SE-CBKEYS (the GL-CBKEYS-1 lane) — executor half: admit CANONICAL-BYTES
/// text/varchar group keys (deterministic collations only) into the
/// grouped-join sink's export/combine/absorb, bringing the JOIN sink to
/// the scan sinks' key parity (the C3 `group_eq_representational` law:
/// byte equality of the detoasted content IS texteq's verdict). BPCHAR
/// stays a NAMED refusal (its space-stripping equality and trailing-blank
/// representative ties are exactly why the scan sinks exclude it —
/// hashgrouped/merge module docs; a future bpchar tie-law car owns any
/// canonicalization ruling). Word-keyed shapes are byte-untouched (the
/// bytes admissions are tried only after the word admissions refuse).
/// The grouped-join row is spill-DISABLED by construction (the export
/// refuses spill-mode tables), so matrix law 2c (bytes keys disable the
/// spill arm) holds inherently. DEFAULT ON (conversion-flips train, GL-CBKEYS-1
/// — the planner twin's doc carries the letter numbers);
/// `PGRUST_LANE_V2_CBKEYS=0|off` is the kill switch — same spelling as
/// the planner probe (knob coherence; BOTH sites flip together).
fn hj_cbkeys_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_CBKEYS").as_deref(), Ok("0") | Ok("off"))
    })
}

/// SE-BPCHAR (the GL-BPCHAR-1 lane) — the tie-law sub-gate of the cbkeys
/// car: admit `char(n)` (real-typmod bpchar) group keys into the
/// canonical-bytes vocabulary. The ruling (proven in the varchar crate's
/// tie-law corpus against the vendored bpchar_input/bpchareq): same-typmod
/// stored images are exactly-n-characters blank-padded, so
/// equal-under-bpchareq <=> byte-identical images — the stored bytes ARE
/// canonical and no trailing-blank representative tie exists. Sub-knob of
/// CBKEYS (both must be armed; the planner probe reads the same pair).
/// DEFAULT ON (conversion-flips train, GL-BPCHAR-1 — the planner twin's doc
/// carries the letter numbers incl. the byte-identical production canary);
/// `PGRUST_LANE_V2_CBKEYS_BPCHAR=0|off` is the kill switch.
fn hj_bpchar_keys_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_CBKEYS_BPCHAR").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// SE-DECOROOT: resolve the Agg's plan NODE from the leader plan root.
/// `Some` iff the root IS this Agg (the pre-existing law, any knob state),
/// or — knob-armed and `decorated_ok` (grouped engagements only) — the root
/// is a whitelisted decoration chain `[Limit] -> [Sort] -> Agg` whose Agg is
/// this node. The returned node seeds the WORKER pstmt (workers must run
/// the Agg subtree, never the decoration — the serial Sort/Limit above is
/// the leader's). Anything else — other node kinds, deeper chains — is a
/// refusal (fail-closed; the serial arms proceed byte-identically).
fn decorated_agg_plan_node<'mcx>(
    root: ::types_nodes::Node<'mcx>,
    agg: &::nodeagg::AggStateData<'mcx>,
    decorated_ok: bool,
) -> Option<::types_nodes::Node<'mcx>> {
    let is_this_agg = |n: ::types_nodes::Node<'mcx>| {
        n.as_agg().is_some_and(|a| std::ptr::eq(a, agg.plan))
    };
    if is_this_agg(root) {
        return Some(root);
    }
    if !decorated_ok || !hj_decoroot_enabled() {
        return None;
    }
    let mut node = root;
    let mut descended = false;
    if let Some(l) = node.as_limit() {
        node = l.plan.lefttree?;
        descended = true;
    }
    if let Some(s) = node.as_sort() {
        node = s.plan.lefttree?;
        descended = true;
    }
    if descended && is_this_agg(node) {
        return Some(node);
    }
    None
}

/// Grouped-sink export envelope: a worker table above this many groups (or
/// any hashagg spill entry) refuses the whole engagement to the serial arm —
/// the per-morsel cumulative export walk and the leader absorb are both
/// O(groups), and the planner floors keep engaged shapes far below it.
fn mbg_max_groups() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    crate::once_val(&N, || {
        std::env::var("PGRUST_RUNTIME_HASHJOIN_GROUPSINK_MAX_GROUPS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(131_072)
    })
}

// ---------------------------------------------------------------------------
// K2 inc-1 (wave-8 WS-AC): the heap-fed probe/build feed over the
// BatchGranuleSource seam (notes/se-wave8-k2.md).
// ---------------------------------------------------------------------------

/// `PGRUST_LANE_V2_K2_PROBE` (default ON since the SE9-GATES K2 flip;
/// explicit `=0`/`off` is the permanent kill switch; K2 inc-1, R-KNOBS
/// registry spelling): the hash-join heap-feed arm knob. Heap SeqScans
/// admit into this arm only when BOTH this and `PGRUST_LANE_V2_HEAPFEED`
/// are on — with HEAPFEED at its OFF default the flipped default is
/// armed-unengaged (priced at the measurement floor, SE8-GATES item 2c:
/// cbwin +0.0007%); OFF (either knob) the admission gates refuse heap
/// exactly where they always did (one cached-bool branch; the pre-flip
/// bytes, the pre-flip refusal stream). AtomicU8 + `_set_for_tests` idiom
/// (the HEAPFEED precedent) so units can A/B both states in one process.
static K2_PROBE: AtomicU8 = AtomicU8::new(0);

fn k2_probe_enabled() -> bool {
    match K2_PROBE.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => k2_probe_resolve(),
    }
}

#[cold]
#[inline(never)]
fn k2_probe_resolve() -> bool {
    // SE9-GATES K2 FLIP (wave-9 queue item 3, executed on the SE8-GATES
    // banked evidence — notes/se-wave8-gates.md item 2): default ON. The
    // K2 win letter read 13.9x/16.3x instr against BOTH comparands
    // (pgrust-corpus-pairs-1784346410-0a41) and EXACT-FLAT on the K1
    // census channel (cbwin -0.002%, pgrust-corpus-pairs-1784346413-5741);
    // the armed-unengaged arm priced at the measurement floor (+0.0007%,
    // item 2c). Only this default read changes — the explicit `=0`/`off`
    // spelling is the permanent kill switch restoring the pre-flip
    // refusal stream's bytes AND ticks (rowmode FLIP-1/FLIP-2 idiom
    // verbatim, the AE2 precedent; flips never delete knobs).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_K2_PROBE").as_deref(),
        Ok("0") | Ok("off")
    );
    K2_PROBE.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus.
#[cfg(test)]
pub(crate) fn k2_probe_set_for_tests(on: bool) {
    K2_PROBE.store(if on { 2 } else { 1 }, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// HJPROBE-V2 (notes/se-hjprobe-v2.md §4.3 increment 1): the dense-key seat
// on the lane hash-join kernel — key tracking at build accept + the seat
// probe (skips the probe hash eval, bucket/tag lookup, hashvalue prefilter
// and hashclauses recheck on int4eq-keyed unbatched single-join engagements
// — the legacy hj-dense lever's lane twin).
//
// DEFAULT ON since the GL-HJSEAT-2 flip (flipped-kill idiom; letter:
// scratchpad/night/hj-seat-gate-and-floor-rederivation.md, gated census job
// pgrust-fast-tests-f7022d98e0-1784620323-01f4 PASS + the witnessed band
// seat/legacy 0.636-0.764 at 2.5M/5M/10M dop4 + 5M dop16, 2026-07-21).
// `PGRUST_LANE_V2_HJPROBE_V2=0|off` is the kill: it restores the v1 probe
// bytes and ticks exactly — no Local ever arms, no seat ever builds, and
// the probe dispatch reads `has_seat() == false` down the identical v1
// path. The m5_suppress seat-scoped floor lift reads the SAME spelling
// (knob coherence, the GROUPSINK law): killing the knob also restores the
// 2M suppression ceiling.
// ---------------------------------------------------------------------------

/// GL-HJSEAT-2 seat-economics gate: arm the dense seat only when the
/// planner's PROBE-rows estimate is at least SEAT_MIN_PROBE_RATIO x the
/// BUILD-rows estimate. The seat pays O(build) construction (per-Local key
/// tracking + the 3-pass CSR at freeze) and earns O(probes) savings (the
/// outer-hash interpreter dispatch + the per-candidate recheck) — below the
/// ratio the construction is not amortized.
/// PROVENANCE (constants discipline, 2026-07-21): GL-HJSEAT-1 census (fleet
/// job pgrust-fast-tests-15bfe40a57-1784617559-64ac, dop8) + the ratio
/// bracket (hj-seat-census-ab.sh CLASS_FILTER=int4_uniq at 2M/2M and 4M/2M):
/// damped ON/OFF = 0.594 at probe/build=4, 0.766 at 2, 0.930 at 1, and
/// 1.23-1.29 at 0.25 (the census "dup" classes — their planner-chosen build
/// side is the 8M UNIQUE fact table, so they measure ratio 0.25, NOT build
/// duplication; build-side dup was ruled out as the axis: EXPLAIN shows the
/// bucket-stats penalty routes dup-heavy sides to probe, and first-principles
/// says dup makes the seat cheaper, not dearer). The crossover lies in the
/// unmeasured (0.25, 1) interval; the constant sits at the LAST MEASURED
/// WINNING point (1). Lowering it requires its own letter. GL-HJSEAT-2.
const SEAT_MIN_PROBE_RATIO: f64 = 1.0;

static HJPROBE_V2: AtomicU8 = AtomicU8::new(0);

fn hjprobe_v2_enabled() -> bool {
    match HJPROBE_V2.load(Ordering::Relaxed) {
        1 => false,
        2 => true,
        _ => hjprobe_v2_resolve(),
    }
}

#[cold]
#[inline(never)]
fn hjprobe_v2_resolve() -> bool {
    // GL-HJSEAT-2 flip: DEFAULT ON; =0|off is the kill (flipped-kill idiom).
    let on = !matches!(
        std::env::var("PGRUST_LANE_V2_HJPROBE_V2").as_deref(),
        Ok("0") | Ok("off")
    );
    HJPROBE_V2.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (the K2_PROBE idiom; the
/// dualexec-style in-process A/B units arm it when they land — until then
/// the fleet arms ride the env spelling).
#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn hjprobe_v2_set_for_tests(on: bool) {
    HJPROBE_V2.store(if on { 2 } else { 1 }, Ordering::Relaxed);
}

/// K2 inc-1 join-type envelope (hard): the heap feed takes only the four
/// fill-free probe-side types. The right-fill family (RIGHT/FULL/
/// RIGHT_ANTI — the FILL task sets), RIGHT_SEMI (the fail-closed
/// no-otherqual gate in shared_exec.rs) and everything else ride the
/// runtime/pgrcolumnar arm unchanged; heap-fed shapes outside the four
/// refuse by name (`k2-heap-jointype`) and fall through to the serial
/// arms byte-identically.
fn k2_heap_jointype_admits(jt: ::types_nodes::JoinType) -> bool {
    matches!(
        jt,
        ::types_nodes::JoinType::JOIN_INNER
            | ::types_nodes::JoinType::JOIN_LEFT
            | ::types_nodes::JoinType::JOIN_SEMI
            | ::types_nodes::JoinType::JOIN_ANTI
    )
}

// ---------------------------------------------------------------------------
// Shared state: parallel-context private payload + probe task-set work body.
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim — the M1 arm's discipline).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

fn lockm<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

// ---------------------------------------------------------------------------
// M3.5 batch spill state.
// ---------------------------------------------------------------------------

/// A single-writer batch-record router over one SpillFile: per-partition
/// staging buffers, epoch flushes (all non-empty partitions ascending — the
/// substrate contract), EXACT per-partition record counters. Plain data
/// between flush events (the SpillFile open-per-event law); the payload's
/// per-worker-slot Mutex is uncontended during writing (one owner) and
/// serializes later frozen reads.
struct BatchRouter {
    file: ::spillset::SpillFile,
    bufs: Vec<Vec<u8>>,
    counts: Vec<u64>,
    staged: usize,
}

impl BatchRouter {
    fn new(set: &Arc<::spillset::SpillSet>, name: String, nparts: u32) -> BatchRouter {
        BatchRouter {
            file: ::spillset::SpillFile::new(Arc::clone(set), name, nparts),
            bufs: vec![Vec::new(); nparts as usize],
            counts: vec![0; nparts as usize],
            staged: 0,
        }
    }

    fn put(&mut self, part: u32, hashvalue: u32, tuple: &[u8]) -> PgResult<()> {
        let before = self.bufs[part as usize].len();
        batch_record_push(&mut self.bufs[part as usize], hashvalue, tuple);
        self.counts[part as usize] += 1;
        self.staged += self.bufs[part as usize].len() - before;
        if self.staged >= HJ_ROUTER_FLUSH {
            self.flush()?;
        }
        Ok(())
    }

    /// Commit all staged bytes as one epoch (no-op when empty). MUST run on
    /// a thread with temp-file access; the caller's task/seal discipline
    /// guarantees no concurrent writer.
    fn flush(&mut self) -> PgResult<()> {
        if self.staged == 0 {
            return Ok(());
        }
        let ctx = ::mcx::MemoryContext::new("m35-hj-batch-write");
        let mut w = self.file.begin_epoch(ctx.mcx())?;
        for (p, buf) in self.bufs.iter_mut().enumerate() {
            if !buf.is_empty() {
                w.write_part(p as u32, buf)?;
                buf.clear();
            }
        }
        w.finish()?;
        self.staged = 0;
        Ok(())
    }
}

/// One inner-side claim: one committed extent of one router file. Extents
/// are record-aligned by construction (epochs write whole records).
#[derive(Clone, Copy)]
struct InnerClaim {
    src: InnerSrc,
    extent: ::spillset::Extent,
}

#[derive(Clone, Copy)]
enum InnerSrc {
    /// BUILD-ACCEPT inner file of worker `slot`, partition = level-0 batch.
    Accept { slot: usize },
    /// SPLIT-ROUND `round` file of worker `slot`, partition = node id.
    Round { round: usize, slot: usize },
}

/// PLAN-BATCHES mutable state (Mutex — mutated by the plan task and split
/// round finalizers only, all single-threaded by the deps DAG).
struct PlanState {
    map: Option<LeafMap>,
    /// Per assigned leaf slot: its inner claim list.
    leaves: Vec<Vec<InnerClaim>>,
    /// Splits pending for the NEXT round.
    pending: Vec<PendingSplit>,
    /// The shared EMPTY leaf (all zero-tuple batches map to one slot: no
    /// build rows means sharing merges nothing, and it keeps empty level-0
    /// batches from eating the leaf cap or — under tight envelopes where
    /// even the empty-table capacity model refuses — split-recursing).
    empty_leaf: Option<u16>,
}

struct PendingSplit {
    consumed: u32,
    jbits: u32,
    child_base: u32,
    claims: Vec<InnerClaim>,
}

/// One split round's frozen claim schedule.
struct RoundPlan {
    claims: Vec<RoundClaim>,
    /// The splits this round executes (finalize walks their children).
    entries: Vec<RoundEntry>,
}

#[derive(Clone, Copy)]
struct RoundClaim {
    claim: InnerClaim,
    consumed: u32,
    jbits: u32,
    child_base: u32,
}

#[derive(Clone, Copy)]
struct RoundEntry {
    consumed: u32,
    jbits: u32,
    child_base: u32,
}

/// The frozen PLAN-BATCHES output: leaf map + per-leaf inner claims.
struct FrozenPlan {
    map: LeafMap,
    leaves: Vec<Vec<InnerClaim>>,
}

/// Outer claim schedule, built at PROBE(0)'s finalize (outer files frozen).
struct OuterPlan {
    /// Per leaf slot: (worker slot, extent).
    leaves: Vec<Vec<(usize, ::spillset::Extent)>>,
}

/// Deferred morsel source: granule total filled in by an earlier task set's
/// finalize, strictly before this source's set can publish (deps DAG). One
/// claim = one extent (bounded by the router flush threshold).
struct DeferredSource {
    total: AtomicU64,
}

impl DeferredSource {
    fn new() -> Arc<DeferredSource> {
        Arc::new(DeferredSource { total: AtomicU64::new(0) })
    }
}

impl runtime::MorselSource for DeferredSource {
    fn total_granules(&self) -> u64 {
        self.total.load(Ordering::SeqCst)
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(self.total_granules())
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

/// Single-granule source (PLAN-BATCHES).
struct OneGranuleSource;

impl runtime::MorselSource for OneGranuleSource {
    fn total_granules(&self) -> u64 {
        1
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

/// The engagement's M3.5 batch-spill state (None = unbatched — the dormant
/// default; nothing below exists and the DAG is the M3 three-set shape).
struct HjSpill {
    set: Arc<::spillset::SpillSet>,
    /// Level-0 batch count (power of two ≥ 2).
    nbatch: u32,
    log2n: u32,
    /// Combined envelope per live table (C parity: every batch's build gets
    /// the whole combined budget).
    space_allowed: usize,
    dop: u64,
    fill_inner: bool,
    /// Envelope-scaled chunk ceiling for leaf builds (bytes).
    chunk_cap_bytes: u64,
    leaf_cap: usize,
    rounds_max: usize,
    batch0_demoted: AtomicBool,
    /// Per worker slot: BUILD-ACCEPT inner router (nparts = nbatch).
    inner: Vec<Mutex<Option<BatchRouter>>>,
    /// Per worker slot: PROBE(0) outer router (nparts = leaf_cap).
    outer: Vec<Mutex<Option<BatchRouter>>>,
    /// [round][worker slot]: split routers (nparts = MAX_SPLIT_NODES).
    rounds: Vec<Vec<Mutex<Option<BatchRouter>>>>,
    plan: Mutex<PlanState>,
    frozen: OnceLock<Arc<FrozenPlan>>,
    round_plans: Vec<OnceLock<Arc<RoundPlan>>>,
    outer_plan: OnceLock<Arc<OuterPlan>>,
    round_sources: Vec<Arc<DeferredSource>>,
    leaf_in_sources: Vec<Arc<DeferredSource>>,
    leaf_out_sources: Vec<Arc<DeferredSource>>,
    /// Observability (the R4 spill channel).
    splits: AtomicU64,
    max_round: AtomicU64,
    leaves_used: AtomicU64,
}

impl HjSpill {
    fn new(
        set: Arc<::spillset::SpillSet>,
        nbatch: u32,
        space_allowed: usize,
        dop: u64,
        fill_inner: bool,
    ) -> HjSpill {
        let leaf_cap = hj_spill_max_batches();
        let rounds_max = hj_spill_rounds();
        let slots = runtime::MAX_EXTERNAL_LANES;
        HjSpill {
            set,
            nbatch,
            log2n: nbatch.trailing_zeros(),
            space_allowed,
            dop,
            fill_inner,
            chunk_cap_bytes: leaf_chunk_cap_bytes(space_allowed, dop),
            leaf_cap,
            rounds_max,
            batch0_demoted: AtomicBool::new(false),
            inner: (0..slots).map(|_| Mutex::new(None)).collect(),
            outer: (0..slots).map(|_| Mutex::new(None)).collect(),
            rounds: (0..rounds_max)
                .map(|_| (0..slots).map(|_| Mutex::new(None)).collect())
                .collect(),
            plan: Mutex::new(PlanState {
                map: None,
                leaves: Vec::new(),
                pending: Vec::new(),
                empty_leaf: None,
            }),
            frozen: OnceLock::new(),
            round_plans: (0..rounds_max).map(|_| OnceLock::new()).collect(),
            outer_plan: OnceLock::new(),
            round_sources: (0..rounds_max).map(|_| DeferredSource::new()).collect(),
            leaf_in_sources: (0..leaf_cap).map(|_| DeferredSource::new()).collect(),
            leaf_out_sources: (0..leaf_cap).map(|_| DeferredSource::new()).collect(),
            splits: AtomicU64::new(0),
            max_round: AtomicU64::new(0),
            leaves_used: AtomicU64::new(0),
        }
    }

    fn est_fits(&self, bytes: u64, tuples: u64) -> bool {
        estimate_batch_table_mem(bytes, tuples, self.dop, self.chunk_cap_bytes)
            <= self.space_allowed as u64
    }

    /// Spilled-byte census for the completion trace (reads directories only).
    fn spilled_census(&self) -> (u64, u64, u64) {
        let sum = |routers: &Vec<Mutex<Option<BatchRouter>>>| -> u64 {
            routers
                .iter()
                .map(|m| lockm(m).as_ref().map_or(0, |r| r.file.spilled_bytes()))
                .sum()
        };
        let inner = sum(&self.inner);
        let outer = sum(&self.outer);
        let split: u64 = self.rounds.iter().map(sum).sum();
        (inner, outer, split)
    }
}

/// Read one inner claim's extent into an 8-aligned buffer (open-by-name on
/// THIS thread; the file is frozen — the reader task set deps-follows the
/// writer set).
fn read_inner_claim(spill: &HjSpill, claim: &InnerClaim) -> PgResult<AlignedBuf> {
    let guard = match claim.src {
        InnerSrc::Accept { slot } => lockm(&spill.inner[slot]),
        InnerSrc::Round { round, slot } => lockm(&spill.rounds[round][slot]),
    };
    let Some(router) = guard.as_ref() else {
        return Err(PgError::new(ERROR, "join batch claim without a router file").into());
    };
    read_extent_aligned(&router.file, claim.extent)
}

/// 8-aligned byte buffer (u64 backing) — the batch-record reader discipline:
/// every record's tuple image lands MAXALIGNed.
struct AlignedBuf {
    words: Vec<u64>,
    len: usize,
}

impl AlignedBuf {
    fn bytes(&self) -> &[u8] {
        // SAFETY: the word backing owns at least `len` initialized bytes.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }
}

fn read_extent_aligned(
    file: &::spillset::SpillFile,
    extent: ::spillset::Extent,
) -> PgResult<AlignedBuf> {
    let ctx = ::mcx::MemoryContext::new("m35-hj-batch-read");
    let mut rd = file.read_extent(ctx.mcx(), extent)?;
    let total = rd.total_len() as usize;
    let mut words = vec![0u64; total.div_ceil(8)];
    {
        // SAFETY: writing into the owned word backing's byte view.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(words.as_mut_ptr().cast::<u8>(), total)
        };
        let mut filled = 0usize;
        while filled < total {
            let n = rd.read(&mut bytes[filled..])?;
            if n == 0 {
                break;
            }
            filled += n;
        }
        if filled != total {
            return Err(PgError::new(ERROR, "join batch extent short read").into());
        }
    }
    rd.close()?;
    Ok(AlignedBuf { words, len: total })
}

pub(super) struct RuntimeHjShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    pins_base: usize,
    refused: AtomicUsize,
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (every exit path bumps once,
    /// by drop guard) — the inc-2c liveness-reap input: a pinned RG is
    /// invisible to pool workers, so `exited >= launched` with the RG
    /// incomplete means nobody will ever step it.
    exited: AtomicUsize,
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// §6 envelope crossing: abort → LEADER FALLBACK (serial rerun), not an
    /// error (R5). Set before the abort; checked on the Aborted outcome.
    budget_refused: AtomicBool,
    /// Per-ordinal cumulative probe partials (M1 overwrite discipline).
    partials: Vec<Mutex<Option<RuntimePartial>>>,
    /// SE-AGGJOIN (band 87001): per-worker GROUPED partial slots (the
    /// grouped chain terminal's cumulative-overwrite export; empty and
    /// untouched on plain engagements).
    grouped_partials: Vec<Mutex<Option<GroupedRuntimePartial>>>,
    /// The build sink (the ParallelSink of task sets [0]/[1]).
    sink: OnceLock<Arc<JoinBuildSink>>,
    /// m5p1 multibuild: the multi-pipeline engagement descriptor (empty =
    /// the phase-1 single-join arm, byte-identical paths throughout).
    chain: OnceLock<Arc<MbChain>>,
    /// GL-HJSEAT-2 seat-economics verdict, computed once at admission from
    /// the planner's estimates (probe rows >= SEAT_MIN_PROBE_RATIO x build
    /// rows). false suppresses dense-seat ARMING entirely (zero key-tracking
    /// tax); the v1 tag-filtered probe is the unchanged outcome. Always
    /// false for multibuild/batched engagements (the seat never arms there).
    seat_ok: bool,
    /// M3.5 batch state (None = unbatched engagement — dormant default).
    spill: Option<Arc<HjSpill>>,
    /// Per-leaf frozen tables (batched engagements; the batch-0 table lives
    /// in the sink). Dropped by the last set that reads them (one live
    /// table, C parity).
    leaf_tables: Vec<Mutex<Option<Arc<FrozenJoinTable>>>>,
    /// M2 inc-1 standing channel: the live board entry, held for the
    /// PRIVATE_SHUTDOWN standing join (standing_channel, scan discipline).
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
}

impl RuntimeHjShared {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = lockm(&self.error);
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn refuse_budget(&self) {
        self.budget_refused.store(true, Ordering::SeqCst);
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn refuse_budget_traced(&self, why: &str) {
        lane_trace(&format!("runtime-hashjoin: REFUSED ({why}) — serial rerun"));
        self.refuse_budget();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        lockm(&self.error).take()
    }

    fn table(&self) -> Option<Arc<FrozenJoinTable>> {
        self.sink.get().and_then(|s| lockm(&s.table).clone())
    }

    fn drop_table0(&self) {
        if let Some(s) = self.sink.get() {
            lockm(&s.table).take();
        }
    }

    fn worker_slot(&self, worker: usize) -> usize {
        worker - self.pins_base
    }
}

// ---------------------------------------------------------------------------
// The build sink: ParallelSink over the shared_build core. accept_local
// drives the bound helper's INNER scan over the claimed granule range;
// combine/finalize are pure core calls (no executor).
// ---------------------------------------------------------------------------

pub(super) struct JoinBuildSink {
    budget: Arc<JoinBudget>,
    /// Lazily planned at first combine (the SEAL happens inside the sink
    /// plumbing; the sink sees the sealed Locals only at combine time).
    plan: Mutex<Option<Arc<CombinePlan>>>,
    /// Published at finalize; the probe task set (deps=[combine]) reads it.
    table: Mutex<Option<Arc<FrozenJoinTable>>>,
    shared: Weak<RuntimeHjShared>,
    /// SINGLE-PASS (Phase 1a): Some ⇒ workers CAS-insert directly into this
    /// shared directory during accept (no COMBINE). Sized up front from the
    /// planner's inner-rows estimate. None ⇒ the two-pass default. Set only
    /// for UNBATCHED single-join engagements under the kill switch.
    singlepass: Option<Arc<SharedBuildDir>>,
}

impl JoinBuildSink {
    fn fail(&self, e: Box<PgError>) {
        if let Some(s) = self.shared.upgrade() {
            s.fail(e);
        }
    }

    fn failed(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| s.failed.load(Ordering::SeqCst))
    }

    fn demoted(&self) -> bool {
        self.shared
            .upgrade()
            .and_then(|s| s.spill.as_ref().map(|sp| sp.batch0_demoted.load(Ordering::SeqCst)))
            .unwrap_or(false)
    }

    /// The lazily-built combine plan (first combine wins the build; the
    /// mutex is held only for the plan/lookup, never across a partition).
    fn plan_for(&self, locals: &[JoinBuildLocal]) -> Option<Arc<CombinePlan>> {
        let mut g = lockm(&self.plan);
        if let Some(p) = g.as_ref() {
            return Some(Arc::clone(p));
        }
        match CombinePlan::plan(locals, &self.budget) {
            Ok(p) => {
                let p = Arc::new(p);
                *g = Some(Arc::clone(&p));
                Some(p)
            }
            Err(BudgetExceeded) => {
                drop(g);
                // Reachable two ways: (a) spill disarmed — the R5 phase-1
                // refusal, exact posture; (b) an UNBATCHED admission whose
                // true build crossed (HjSpill absent => the seal pre-check
                // demote never runs — it requires `shared.spill`). The
                // admission boundary guard (GL-HJMB-1) keeps the estimated
                // crossing band batched, so an armed-spill hit here means
                // the estimate was wrong-side by more than the guard's
                // headroom; the cost is the measured 5-11x serial-rerun
                // cliff vs legacy Parallel Hash.
                lane_trace("runtime-hashjoin: REFUSED (envelope crossed at seal) — serial rerun");
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget();
                }
                None
            }
        }
    }
}

impl runtime::ParallelSink for JoinBuildSink {
    type Local = JoinBuildLocal;

    fn fork(&self, worker: usize) -> JoinBuildLocal {
        let mut local = JoinBuildLocal::new(worker, Arc::clone(&self.budget));
        if let Some(dir) = &self.singlepass {
            // SINGLE-PASS: this Local links tuples straight into the shared
            // directory in `push` (accept), bypassing part_refs/COMBINE.
            local.attach_shared_dir(Arc::clone(dir));
        }
        local
    }

    fn accept_local(&self, local: &mut JoinBuildLocal, worker: usize, range: runtime::MorselRange) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let r =
            catch_unwind(AssertUnwindSafe(|| build_morsel_body(&shared, local, worker, range)));
        match r {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                // §6 envelope crossing mid-accept: refusal, not error. With
                // the spill arm on AND a BATCHED admission the crossing
                // demotes instead; an UNBATCHED admission has no HjSpill and
                // lands here (GL-HJMB-1 boundary — the admission guard keeps
                // the estimated crossing band batched, so armed hits mean
                // the estimate was wrong-side beyond the guard's headroom).
                lane_trace("runtime-hashjoin: REFUSED (envelope crossed in build) — serial rerun");
                shared.refuse_budget();
            }
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a build morsel")
                        .into(),
                );
            }
        }
    }

    /// SEAL (single-threaded, last-worker-out): the M3.5 batch-0 demote
    /// point — a batch 0 that crossed mid-accept, or whose bucket array
    /// would cross now, is DUMPED to the batch files (each Local's rows to
    /// its own worker's inner file, partition 0) and becomes an ordinary
    /// file batch (§5.2 "batch 0 is dumped"). Cross-Local file appends are
    /// safe here: accept has completed (no concurrent writer) and the
    /// substrate opens by name on this thread.
    fn seal(&self, locals: &mut [JoinBuildLocal]) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let Some(spill) = shared.spill.as_ref() else { return };
        let mut demote = spill.batch0_demoted.load(Ordering::SeqCst);
        if !demote {
            let total: u64 = locals.iter().map(|l| l.tuples()).sum();
            let buckets = 8 * total.max(1).next_power_of_two().clamp(1024, 1 << 31);
            if self.budget.used() as u64 + buckets > spill.space_allowed as u64 {
                spill.batch0_demoted.store(true, Ordering::SeqCst);
                demote = true;
                lane_trace(
                    "runtime-hashjoin: batch 0 crossed the envelope at seal — demoted to a file batch",
                );
            }
        }
        if !demote {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<()> {
            for l in locals.iter_mut() {
                let slot = shared.worker_slot(l.ordinal());
                let mut g = lockm(&spill.inner[slot]);
                let router = g.get_or_insert_with(|| {
                    BatchRouter::new(
                        &spill.set,
                        ::spillset::SpillSet::file_name("hj-in", 0, slot),
                        spill.nbatch,
                    )
                });
                l.drain_records(|h, payload| router.put(0, h, payload))?;
                router.flush()?;
                drop(g);
                l.reset();
            }
            Ok(())
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => self.fail(e),
            Err(_panic) => self.fail(
                PgError::new(ERROR, "runtime hash-join batch-0 demote dump panicked").into(),
            ),
        }
    }

    fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[JoinBuildLocal]) {
        if self.failed() || self.demoted() {
            return;
        }
        // SINGLE-PASS: chains are already CAS-linked during accept — the 256
        // combine tasks are pure no-ops (the seal/freeze happens in finalize).
        if self.singlepass.is_some() {
            return;
        }
        if let Some(plan) = self.plan_for(locals) {
            plan.combine_partition(part, locals);
        }
    }

    fn finalize(&self, locals: &[JoinBuildLocal]) {
        if self.failed() || self.demoted() {
            return;
        }
        // SINGLE-PASS: seal the shared directory (barrier-gated grow_buckets
        // on an underestimate) into a plan the frozen table consumes as-is.
        let plan = if let Some(dir) = &self.singlepass {
            match finish_single_pass(locals, Arc::clone(dir), &self.budget) {
                Ok(p) => Arc::new(p),
                Err(BudgetExceeded) => {
                    lane_trace(
                        "runtime-hashjoin: REFUSED (single-pass grow crossed envelope) — serial rerun",
                    );
                    if let Some(s) = self.shared.upgrade() {
                        s.refuse_budget();
                    }
                    return;
                }
            }
        } else {
            // Zero-granule inner side: no combine morsel ran (empty partition
            // space never happens — PARTITIONS is fixed — but a fully-refused
            // plan slot can be absent after refuse_budget).
            let Some(plan) = self.plan_for(locals) else { return };
            plan
        };
        let table = freeze(plan, locals);
        // HJPROBE-V2 engagement witness (e2e-grepped; the trace can only
        // ever fire with the knob ON — no armed Local exists otherwise).
        if table.has_seat() {
            lane_trace("runtime-hashjoin: dense-seat");
        }
        *lockm(&self.table) = Some(Arc::new(table));
    }
}

// ---------------------------------------------------------------------------
// Worker-side executor (TLS): the whole serial Agg→HashJoin→scans subtree,
// built once per bound helper; build morsels position the INNER scan, probe
// morsels the OUTER scan.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static HJ_WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
    /// The probe payload for the currently-driving helper (set for the
    /// drive's duration; run_morsel bodies read it for the frozen table).
    static HJ_PAYLOAD: std::cell::RefCell<Option<Arc<RuntimeHjShared>>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    HJ_WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// Split the worker plan tree into (agg, hj_state, outer scan, hash state,
/// inner scan) and run `f`. All field borrows are disjoint.
fn with_join_tree<'a, 'mcx, R>(
    estate: &'a mut EStateData<'mcx>,
    planstate: &'a mut Option<crate::procnode::PlanStateNode<'mcx>>,
    f: impl FnOnce(
        &mut EStateData<'mcx>,
        &mut ::nodeagg::AggStateData<'mcx>,
        &mut ::nodehashjoin::HashJoinState<'mcx>,
        &mut ::nodeseqscan::SeqScanState<'mcx>,
        &mut ::nodehash::HashState<'mcx>,
        &mut ::nodeseqscan::SeqScanState<'mcx>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    let Some(crate::procnode::PlanStateNode::Agg(aps)) = planstate.as_mut() else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker plan is not a plain Agg root",
        )));
    };
    let aps: &mut crate::procnode::AggPlanState<'mcx> = aps;
    let crate::procnode::PlanStateNode::HashJoin(hjn) = &mut aps.outer else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker outer node is not a HashJoin",
        )));
    };
    let hjn: &mut crate::procnode::HashJoinNode<'mcx> = hjn;
    let crate::procnode::PlanStateNode::SeqScan(outer_ss) = &mut *hjn.outer else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker probe child is not a SeqScan",
        )));
    };
    let hash: &mut crate::procnode::HashSubNode<'mcx> = &mut hjn.hash;
    let crate::procnode::PlanStateNode::SeqScan(inner_ss) = &mut *hash.child else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join worker build child is not a SeqScan",
        )));
    };
    f(estate, &mut aps.agg, &mut hjn.state, outer_ss, &mut hash.state, inner_ss)
}

fn with_worker_exec<R>(
    ctx: &'static str,
    f: impl for<'mcx> FnOnce(
        &mut EStateData<'mcx>,
        &mut Option<crate::procnode::PlanStateNode<'mcx>>,
    ) -> PgResult<R>,
) -> PgResult<R> {
    HJ_WORKER_EXEC.with(|cell| {
        let b = cell.borrow();
        let Some(ex) = b.as_ref() else {
            return Err(Box::new(PgError::new(ERROR, ctx)));
        };
        crate::querydesc::with_qd(ex.qd, |q| {
            let x = q.exec.as_mut().expect("runtime hash-join worker executor state");
            x.with_mut(|d| f(&mut d.estate, &mut d.planstate))
        })
    })
}

/// The K2 heap-fed BUILD-ACCEPT claim drive (seam branch of
/// `build_morsel_body`): position through the seam, stage page batches,
/// emit-dead word-skip, per-row emit → `shared_build_accept` (which
/// MATERIALIZES the minimal-tuple bytes into the Local — copy at the
/// consumer, so nothing in the build table aliases the pinned page).
/// Unbatched by admission: no router, no batch-0 demotion. `Ok(true)` =
/// envelope crossed (refusal, not error); the caller settles the claim
/// (`end_claim`) on every path.
fn build_claim_heap_seam<'mcx>(
    src: &mut HeapBatchSource<'_, 'mcx>,
    estate: &mut EStateData<'mcx>,
    hstate: &mut ::nodehash::HashState<'mcx>,
    local: &mut JoinBuildLocal,
    range: &runtime::MorselRange,
    dense_col: Option<u16>,
) -> PgResult<bool> {
    src.position(estate, range.clone())?;
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            return Ok(false);
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Emit-dead word skip (see the pgrcolumnar loop): snapshot the
        // words — the emit below re-borrows the source mutably.
        let skip = {
            let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
            src.skip_sel().map(|s| {
                w[..s.len()].copy_from_slice(s);
                w
            })
        };
        // Walk error carrier: `Some(e)` = a real error (rethrown); `None` =
        // the local build crossed its envelope (the R5 refusal).
        let walk = ::exectuples::for_each_live(
            skip.as_ref().map(|w| &w[..]),
            0,
            n,
            |i| -> Result<(), Option<Box<::types_error::PgError>>> {
                let Some(slot_id) = src.emit(estate, i).map_err(Some)? else {
                    return Ok(());
                };
                let accepted = match dense_col {
                    // HJPROBE-V2: key-tracked accept (the dense-seat feed).
                    Some(col) => {
                        shared_build_accept_keyed(hstate, estate, slot_id, local, col)
                    }
                    None => shared_build_accept(hstate, estate, slot_id, local),
                };
                if accepted.map_err(Some)?.is_err() {
                    return Err(None);
                }
                Ok(())
            },
        );
        match walk {
            Ok(()) => {}
            Err(Some(e)) => return Err(e),
            Err(None) => return Ok(true),
        }
    }
}

/// The K2 heap-fed PROBE claim drive (seam branch of `probe_morsel_body`):
/// position through the seam, stage page batches, emit-dead word-skip,
/// per-row emit → probe the batch-0 table → plain-agg partial absorb.
/// Unbatched by admission (no frozen leaf map, no outer router). The only
/// values that outlive a staged batch are the agg's transition copies in
/// its own aggcontext (R3v: consumers copy at the consumer). The caller
/// settles the claim (`end_claim`) on every path.
fn probe_claim_heap_seam<'mcx>(
    src: &mut HeapBatchSource<'_, 'mcx>,
    estate: &mut EStateData<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hstate: &mut ::nodehash::HashState<'mcx>,
    table: &FrozenJoinTable,
    range: &runtime::MorselRange,
) -> PgResult<()> {
    src.position(estate, range.clone())?;
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Emit-dead word skip (see the pgrcolumnar loop): a cleared
        // skip-sel bit is a row `emit` rejects with no observable effect,
        // so the surviving probe stream is identical. Snapshot the words —
        // the emit below re-borrows the source mutably.
        let skip = {
            let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
            src.skip_sel().map(|s| {
                w[..s.len()].copy_from_slice(s);
                w
            })
        };
        let skip = skip.as_ref().map(|w| &w[..]);
        ::exectuples::for_each_live(skip, 0, n, |i| -> PgResult<()> {
            let Some(slot_id) = src.emit(estate, i)? else {
                return Ok(());
            };
            // HJPROBE-V2 dispatch: the seat's existence IS the toggle
            // (knob OFF ⇒ no seat ⇒ the v1 walk, bytes and ticks intact).
            if table.has_seat() {
                shared_probe_outer_dense(hj, hstate, estate, table, slot_id, &mut |_hj, estate, out| {
                    ::nodeagg::agg_plain_build_accept(agg, estate, out)
                })
            } else {
                shared_probe_outer(hj, hstate, estate, table, slot_id, &mut |_hj, estate, out| {
                    ::nodeagg::agg_plain_build_accept(agg, estate, out)
                })
            }
        })?;
    }
}

/// One BUILD-ACCEPT morsel: position the inner scan on the claimed granule
/// range and materialize every surviving row into the Local — or, batched,
/// route it (batch 0 → Local, others → the worker's inner batch file).
/// Ok(false) = envelope crossed with the spill arm OFF (refusal, not error).
fn build_morsel_body(
    shared: &Arc<RuntimeHjShared>,
    local: &mut JoinBuildLocal,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<bool> {
    let spill = shared.spill.clone();
    let slot = shared.worker_slot(worker);
    with_worker_exec("runtime hash-join build morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, _agg, hj, _outer_ss, hstate, inner_ss| {
            // HJPROBE-V2 dense-seat arming (knob default ON since the
            // GL-HJSEAT-2 flip, =0|off kills; single-join
            // UNBATCHED engagements only): every worker computes the same
            // deterministic gate from its own executor state, so all
            // tuple-bearing Locals arm identically (the seat's all-or-none
            // law). Armed accepts record the int4 build key in lockstep.
            let dense_col = if spill.is_none()
                && shared.chain.get().is_none()
                && hjprobe_v2_enabled()
                // GL-HJSEAT-2 economics: only arm when the probe estimate
                // amortizes the seat's O(build) construction (admission
                // computed shared.seat_ok from the planner's estimates).
                && shared.seat_ok
                // SINGLE-PASS forgoes the dense seat (its concurrent chain
                // order is not reproducible — the seat's byte-identity proof
                // does not hold). The Local's attached-dir state is the gate.
                && !local.single_pass()
            {
                ::nodehashjoin::shared_exec::dense_seat_build_col(hj, hstate)
            } else {
                None
            };
            if dense_col.is_some() {
                local.arm_dense_keys();
            }
            // K2 inc-1 invariant: this arm admits pgrcolumnar scans and —
            // behind PGRUST_LANE_V2_HEAPFEED + PGRUST_LANE_V2_K2_PROBE —
            // heap scans. Heap claims ride the storage seam
            // (HeapBatchSource, R3/R3v pin-holding rails); admission
            // guarantees heap-fed engagements are UNBATCHED, so the spill
            // routers never see a heap-fed row.
            if ::nodeseqscan::seq_scan_is_heap(inner_ss) {
                debug_assert!(
                    spill.is_none(),
                    "k2 heap feed admits only unbatched engagements"
                );
                local.begin_run(range.start);
                let mut src = HeapBatchSource::new(inner_ss);
                // WS-O claim-settle guard (the K1 scan-arm discipline):
                // end_claim runs on the ERROR path too — a failed claim
                // must not carry its page pin into the abort drain; the
                // drive error wins the report.
                let drove =
                    build_claim_heap_seam(&mut src, estate, hstate, local, &range, dense_col);
                let settled = src.end_claim(estate);
                let crossed = drove?;
                settled?;
                local.end_run();
                return Ok(!crossed);
            }
            // train-12 composition: AM-dispatched positioner (heap lane
            // rename); pgrcolumnar claims keep today's direct drive.
            ::nodeseqscan::seq_scan_set_morsel_range(
                inner_ss,
                estate,
                range.start,
                range.end,
            )?;
            local.begin_run(range.start);
            // Batched: the worker's inner router, held across the morsel
            // (uncontended — this slot's single writer is this worker).
            let mut router_guard = spill.as_ref().map(|sp| lockm(&sp.inner[slot]));
            let mut crossed = false;
            loop {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(inner_ss, estate)?;
                if n == 0 {
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(inner_ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    break;
                }
                ::postgres_seams::check_for_interrupts::call()?;
                // Emit-dead word skip over the staged qual bitmap (see the
                // probe morsel): the surviving build stream is identical.
                let skip = {
                    let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                    ::nodeseqscan::seq_scan_batch_skip_sel(inner_ss).map(|s| {
                        w[..s.len()].copy_from_slice(s);
                        w
                    })
                };
                // Walk error carrier: `Some(e)` = a real error (rethrown);
                // `None` = the local build crossed its envelope (the loop's
                // former `break`).
                let walk = ::exectuples::for_each_live(
                    skip.as_ref().map(|w| &w[..]),
                    0,
                    n,
                    |i| -> Result<(), Option<Box<::types_error::PgError>>> {
                        let Some(slot_id) =
                            ::nodeseqscan::seq_scan_batch_emit(inner_ss, estate, i)
                                .map_err(Some)?
                        else {
                            return Ok(());
                        };
                        match (&spill, router_guard.as_mut()) {
                            (Some(sp), Some(guard)) => {
                                shared_build_hash_tuple(hstate, estate, slot_id, |h, bytes| {
                                    let b = batch_of(h, sp.log2n);
                                    if b == 0 && !sp.batch0_demoted.load(Ordering::Relaxed) {
                                        match local.push(h, bytes) {
                                            Ok(()) => return Ok(()),
                                            Err(BudgetExceeded) => {
                                                // §5.2: demote at the crossing
                                                // point; this row and later
                                                // batch-0 rows go to the file.
                                                sp.batch0_demoted.store(true, Ordering::SeqCst);
                                                lane_trace(
                                                    "runtime-hashjoin: batch 0 crossed the envelope — demoted to a file batch",
                                                );
                                            }
                                        }
                                    }
                                    let router = guard.get_or_insert_with(|| {
                                        BatchRouter::new(
                                            &sp.set,
                                            ::spillset::SpillSet::file_name("hj-in", 0, slot),
                                            sp.nbatch,
                                        )
                                    });
                                    router.put(b, h, bytes)
                                })
                                .map_err(Some)?;
                            }
                            _ => {
                                let accepted = match dense_col {
                                    // HJPROBE-V2: key-tracked accept.
                                    Some(col) => shared_build_accept_keyed(
                                        hstate, estate, slot_id, local, col,
                                    ),
                                    None => shared_build_accept(hstate, estate, slot_id, local),
                                };
                                if accepted.map_err(Some)?.is_err() {
                                    return Err(None);
                                }
                            }
                        }
                        Ok(())
                    },
                );
                match walk {
                    Ok(()) => {}
                    Err(Some(e)) => return Err(e),
                    Err(None) => crossed = true,
                }
                if crossed {
                    break;
                }
            }
            // Frozen-before-read: PLAN-BATCHES reads the directory right
            // after this set completes — commit staged bytes per morsel.
            if let Some(mut guard) = router_guard {
                if let Some(router) = guard.as_mut() {
                    router.flush()?;
                }
            }
            local.end_run();
            Ok(!crossed)
        })
    })
}

/// Fetch one row's minimal-tuple bytes (outer side routing).
fn fetch_outer_tuple_bytes<'mcx, R>(
    hj_ecxt: ::executils::EcxtId,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    f: impl FnOnce(&[u8]) -> PgResult<R>,
) -> PgResult<R> {
    let query_mcx = estate.es_query_cxt;
    let (slot, scratch_mcx) = estate.slot_and_per_tuple_mcx(slot_id, hj_ecxt);
    let fetched = ::exectuples::exec_fetch_slot_minimal_tuple(slot, query_mcx, scratch_mcx)?;
    let (ptr, t_len): (*const u8, u32) = match &fetched {
        exectuples::FetchedMinimalTuple::Slot(m, _) => {
            // SAFETY: live stored image; header read.
            (m.as_ptr().cast_const().cast(), unsafe { m.as_ref().t_len })
        }
        exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len()),
    };
    // SAFETY: a minimal tuple image is t_len readable bytes.
    let bytes = unsafe { core::slice::from_raw_parts(ptr, t_len as usize) };
    f(bytes)
}

/// One PROBE(0) morsel: position the outer scan and stream every surviving
/// outer row — unbatched, straight against the frozen table; batched, route
/// by the FROZEN leaf map (in-memory leaf probes inline; file leaves append
/// to the worker's outer batch file). Exports the cumulative partial (M1
/// overwrite discipline — the worker's last export precedes its settle).
fn probe_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let spill = payload.spill.clone();
    let frozen = match &spill {
        Some(sp) => Some(Arc::clone(sp.frozen.get().ok_or_else(|| {
            Box::new(PgError::new(ERROR, "runtime hash-join probe without a frozen batch plan"))
        })?)),
        None => None,
    };
    let table = payload.table();
    if table.is_none() {
        // Legal only when batch 0 was demoted (no in-memory leaf).
        let demoted = spill
            .as_ref()
            .is_some_and(|sp| sp.batch0_demoted.load(Ordering::SeqCst));
        if !demoted {
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime hash-join probe ran without a published table",
            )));
        }
    }
    let slot = payload.worker_slot(worker);
    with_worker_exec("runtime hash-join probe morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, outer_ss, hstate, _inner_ss| {
            // K2 inc-1 invariant: this arm admits pgrcolumnar scans and —
            // behind PGRUST_LANE_V2_HEAPFEED + PGRUST_LANE_V2_K2_PROBE —
            // heap scans. Heap claims ride the storage seam
            // (HeapBatchSource, R3/R3v pin-holding rails); admission
            // guarantees heap-fed engagements are UNBATCHED (no spill, no
            // frozen leaf map, no batch-0 demotion), so the heap branch
            // probes the batch-0 table directly and the only
            // batch-outliving values are the plain agg's transition
            // copies (aggcontext — consumers copy at the consumer).
            if ::nodeseqscan::seq_scan_is_heap(outer_ss) {
                debug_assert!(
                    spill.is_none(),
                    "k2 heap feed admits only unbatched engagements"
                );
                let table = table
                    .as_deref()
                    .expect("heap-fed probe requires the batch-0 table (no demotion without spill)");
                let mut src = HeapBatchSource::new(outer_ss);
                // WS-O claim-settle guard (the K1 scan-arm discipline):
                // end_claim runs on the ERROR path too; the drive error
                // wins the report.
                let drove =
                    probe_claim_heap_seam(&mut src, estate, agg, hj, hstate, table, &range);
                let settled = src.end_claim(estate);
                drove?;
                settled?;
                let pslot = worker - payload.pins_base;
                {
                    // Same export-into tail as the pgrcolumnar drive below
                    // (overwrite discipline preserved).
                    let mut g = lockm(&payload.partials[pslot]);
                    agg_runtime_export_partial_into(
                        agg,
                        g.get_or_insert_with(Default::default),
                    )?;
                }
                return Ok(());
            }
            // train-12 composition: AM-dispatched positioner (heap lane
            // rename); pgrcolumnar claims keep today's direct drive.
            ::nodeseqscan::seq_scan_set_morsel_range(
                outer_ss,
                estate,
                range.start,
                range.end,
            )?;
            let mut router_guard = spill.as_ref().map(|sp| lockm(&sp.outer[slot]));
            loop {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(outer_ss, estate)?;
                if n == 0 {
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(outer_ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    break;
                }
                ::postgres_seams::check_for_interrupts::call()?;
                // Emit-dead word skip over the staged qual bitmap: a cleared
                // skip-sel bit is a row `seq_scan_batch_emit` rejects with no
                // observable effect (definitive even under requal), so the
                // surviving probe stream is identical. Snapshot the words —
                // the emit below re-borrows the scan mutably.
                let skip = {
                    let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                    ::nodeseqscan::seq_scan_batch_skip_sel(outer_ss).map(|s| {
                        w[..s.len()].copy_from_slice(s);
                        w
                    })
                };
                let skip = skip.as_ref().map(|w| &w[..]);
                ::exectuples::for_each_live(skip, 0, n, |i| -> PgResult<()> {
                    let Some(slot_id) = ::nodeseqscan::seq_scan_batch_emit(outer_ss, estate, i)?
                    else {
                        return Ok(());
                    };
                    match (&spill, &frozen, router_guard.as_mut()) {
                        (Some(sp), Some(fp), Some(guard)) => {
                            let h = shared_probe_outer_hash(hj, estate, slot_id)?;
                            let leaf = fp.map.resolve(h);
                            if leaf == LEAF_INMEM {
                                shared_probe_outer_hashed(
                                    hj,
                                    hstate,
                                    estate,
                                    table.as_ref().expect("in-memory leaf requires table 0"),
                                    slot_id,
                                    h,
                                    &mut |_hj, estate, out| {
                                        ::nodeagg::agg_plain_build_accept(agg, estate, out)
                                    },
                                )?;
                            } else {
                                let ecxt = hj.ps_ExprContext;
                                fetch_outer_tuple_bytes(ecxt, estate, slot_id, |bytes| {
                                    let router = guard.get_or_insert_with(|| {
                                        BatchRouter::new(
                                            &sp.set,
                                            ::spillset::SpillSet::file_name("hj-out", 0, slot),
                                            sp.leaf_cap as u32,
                                        )
                                    });
                                    router.put(leaf as u32, h, bytes)
                                })?;
                            }
                        }
                        _ => {
                            let t = table.as_ref().expect("unbatched probe requires the table");
                            // HJPROBE-V2 dispatch: seat existence IS the
                            // toggle (knob OFF ⇒ no seat ⇒ v1 verbatim).
                            if t.has_seat() {
                                shared_probe_outer_dense(
                                    hj,
                                    hstate,
                                    estate,
                                    t,
                                    slot_id,
                                    &mut |_hj, estate, out| {
                                        ::nodeagg::agg_plain_build_accept(agg, estate, out)
                                    },
                                )?;
                            } else {
                                shared_probe_outer(
                                    hj,
                                    hstate,
                                    estate,
                                    t,
                                    slot_id,
                                    &mut |_hj, estate, out| {
                                        ::nodeagg::agg_plain_build_accept(agg, estate, out)
                                    },
                                )?;
                            }
                        }
                    }
                    Ok(())
                })?;
            }
            // Frozen-before-read for the leaf probe sets.
            if let Some(mut guard) = router_guard {
                if let Some(router) = guard.as_mut() {
                    router.flush()?;
                }
            }
            let pslot = worker - payload.pins_base;
            {
                // train-12 composition: export-into (retained capacity);
                // overwrite discipline preserved — the export rewrites the
                // slot's partial in place.
                let mut g = lockm(&payload.partials[pslot]);
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for RuntimeHjShared {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let payload = HJ_PAYLOAD.with(|c| c.borrow().clone());
        let Some(payload) = payload else {
            self.fail(PgError::new(ERROR, "runtime hash-join probe without a bound payload").into());
            return;
        };
        let r = catch_unwind(AssertUnwindSafe(|| {
            if self.chain.get().is_some() {
                mb_probe_morsel_body(&payload, worker, range)
            } else {
                probe_morsel_body(&payload, worker, range)
            }
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a probe morsel")
                        .into(),
                );
            }
        }
    }

    /// PROBE(0) epilogue (last-worker-out, single-threaded): batched, the
    /// outer files are now frozen — build the outer claim schedule and, when
    /// no fill set follows, retire table 0 (one live table).
    fn finalize(&self) {
        let Some(spill) = self.spill.as_ref() else { return };
        if self.failed.load(Ordering::SeqCst) {
            return;
        }
        let r = catch_unwind(AssertUnwindSafe(|| {
            let frozen = spill.frozen.get().expect("probe(0) ran after the freeze");
            let nleaves = frozen.leaves.len();
            let mut leaves: Vec<Vec<(usize, ::spillset::Extent)>> = vec![Vec::new(); nleaves];
            for slot in 0..spill.outer.len() {
                let g = lockm(&spill.outer[slot]);
                if let Some(router) = g.as_ref() {
                    for (leaf, out) in leaves.iter_mut().enumerate() {
                        for x in router.file.part_extents(leaf as u32) {
                            out.push((slot, x));
                        }
                    }
                }
            }
            for (leaf, claims) in leaves.iter().enumerate() {
                spill.leaf_out_sources[leaf]
                    .total
                    .store(claims.len() as u64, Ordering::SeqCst);
            }
            let _ = spill.outer_plan.set(Arc::new(OuterPlan { leaves }));
            if !spill.fill_inner {
                self.drop_table0();
            }
        }));
        if r.is_err() {
            self.fail(PgError::new(ERROR, "runtime hash-join outer plan build panicked").into());
        }
    }
}

/// The FILL task set's work (right-fill family only, deps=[probe]):
/// never-matched build tuples of one partition, null-extended, into the
/// same plain-agg tail. The probe set's last-worker-out completion is the
/// match-flag visibility barrier. `leaf` = None → the batch-0 table;
/// Some(i) → leaf table i (M3.5). Batched fills retire their table at
/// finalize (one live table).
struct FillWork {
    payload: Arc<RuntimeHjShared>,
    leaf: Option<usize>,
}

impl FillWork {
    fn table(&self) -> Option<Arc<FrozenJoinTable>> {
        match self.leaf {
            None => self.payload.table(),
            Some(i) => lockm(&self.payload.leaf_tables[i]).clone(),
        }
    }
}

fn fill_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    table: &Arc<FrozenJoinTable>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    with_worker_exec("runtime hash-join fill morsel without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, _outer_ss, hstate, _inner_ss| {
            for part in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                shared_fill_partition(
                    hj,
                    hstate,
                    estate,
                    table,
                    part,
                    &mut |_hj, estate, out| ::nodeagg::agg_plain_build_accept(agg, estate, out),
                )?;
            }
            // Cumulative partial export (same slot as the probe morsels —
            // the worker's agg accumulates across phases; overwrite
            // discipline keeps the last export authoritative).
            let slot = worker - payload.pins_base;
            {
                let mut g = lockm(&payload.partials[slot]);
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for FillWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(table) = self.table() else {
            // The ONLY legal absence: a demoted batch 0 (its rows moved to
            // a leaf; nothing to fill). Everywhere else — unbatched fills
            // and leaf fills (published by the leaf combine, deps-ordered)
            // — a missing table is a shape error.
            let demoted_b0 = self.leaf.is_none()
                && self
                    .payload
                    .spill
                    .as_ref()
                    .is_some_and(|sp| sp.batch0_demoted.load(Ordering::SeqCst));
            let unused_leaf = self.leaf.is_some_and(|leaf| {
                self.payload
                    .spill
                    .as_ref()
                    .and_then(|sp| sp.frozen.get())
                    .is_none_or(|fp| leaf >= fp.leaves.len())
            });
            if !demoted_b0 && !unused_leaf {
                self.payload.fail(
                    PgError::new(ERROR, "runtime hash-join fill ran without a published table")
                        .into(),
                );
            }
            return;
        };
        let payload = &self.payload;
        let r =
            catch_unwind(AssertUnwindSafe(|| fill_morsel_body(payload, &table, worker, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                payload.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                payload.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a fill morsel")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self) {
        // One live table (batched only): the fill is the table's last
        // reader.
        if self.payload.spill.is_some() {
            match self.leaf {
                None => self.payload.drop_table0(),
                Some(i) => {
                    lockm(&self.payload.leaf_tables[i]).take();
                }
            }
        }
    }
}

/// The fill set's morsel space: one claim per partition (the sink
/// plumbing's PartitionSource shape, re-stated here — it is private to
/// runtime::sink).
struct FillPartitionSource;

impl runtime::MorselSource for FillPartitionSource {
    fn total_granules(&self) -> u64 {
        PARTITIONS as u64
    }

    fn next_boundary_after(&self, start: u64) -> u64 {
        (start + 1).min(PARTITIONS as u64)
    }

    fn startup_c0(&self) -> u64 {
        1
    }
}

// ---------------------------------------------------------------------------
// M3.5 PLAN-BATCHES + split rounds + leaf task-set works.
// ---------------------------------------------------------------------------

/// Resolve one batch node: exact sizes → leaf assignment, or a pending
/// split for `next_round`. Caller holds the plan lock.
#[allow(clippy::too_many_arguments)]
fn resolve_node(
    payload: &RuntimeHjShared,
    spill: &HjSpill,
    st: &mut PlanState,
    node: u32,
    consumed: u32,
    level0: bool,
    claims: Vec<InnerClaim>,
    bytes: u64,
    tuples: u64,
    next_round: usize,
) -> bool {
    let map = st.map.as_mut().expect("plan map exists");
    if tuples == 0 && bytes == 0 {
        // Zero build rows: route to the shared empty leaf (probes against
        // an empty table; LEFT/outer null-fill arms behave exactly as an
        // empty batch must).
        let leaf = match st.empty_leaf {
            Some(l) => l,
            None => {
                let leaf = st.leaves.len();
                if leaf >= spill.leaf_cap {
                    payload.refuse_budget_traced("spill leaf cap exceeded");
                    return false;
                }
                st.leaves.push(Vec::new());
                st.empty_leaf = Some(leaf as u16);
                leaf as u16
            }
        };
        map.set_leaf(node, leaf);
        return true;
    }
    if spill.est_fits(bytes, tuples) {
        let leaf = st.leaves.len();
        if leaf >= spill.leaf_cap {
            payload.refuse_budget_traced("spill leaf cap exceeded");
            return false;
        }
        map.set_leaf(node, leaf as u16);
        st.leaves.push(claims);
        return true;
    }
    if next_round >= spill.rounds_max {
        payload.refuse_budget_traced("split depth cap — batch does not shrink");
        return false;
    }
    let est = estimate_batch_table_mem(bytes, tuples, spill.dop, spill.chunk_cap_bytes);
    let ratio = est.div_ceil(spill.space_allowed.max(1) as u64).max(2);
    // Children sized to fit with one-bit headroom; bounded fan (≤16-way).
    let jbits = (64 - ratio.leading_zeros()).clamp(1, 4);
    if map.node_count() + (1usize << jbits) > MAX_SPLIT_NODES {
        payload.refuse_budget_traced("split node space exhausted");
        return false;
    }
    let child_base = if level0 {
        map.split_node(node, jbits)
    } else {
        map.split_child_node(node, consumed, jbits)
    };
    spill.splits.fetch_add(1, Ordering::Relaxed);
    spill
        .max_round
        .fetch_max(next_round as u64 + 1, Ordering::Relaxed);
    st.pending.push(PendingSplit { consumed, jbits, child_base, claims });
    true
}

/// Arm split round `round` from the pending list (claims flattened), or —
/// when nothing is pending — FREEZE the plan (leaf sources armed).
fn arm_round_or_freeze(payload: &RuntimeHjShared, spill: &HjSpill, round: usize) {
    let mut st = lockm(&spill.plan);
    if st.pending.is_empty() {
        let map = st.map.take().expect("plan map exists");
        debug_assert!(map.fully_resolved(), "freeze with pending nodes");
        let leaves = std::mem::take(&mut st.leaves);
        for (i, claims) in leaves.iter().enumerate() {
            spill.leaf_in_sources[i]
                .total
                .store(claims.len() as u64, Ordering::SeqCst);
        }
        spill.leaves_used.store(leaves.len() as u64, Ordering::SeqCst);
        let _ = spill.frozen.set(Arc::new(FrozenPlan { map, leaves }));
        return;
    }
    if round >= spill.rounds_max {
        // Unreachable: resolve_node refuses before pending past the cap.
        payload.refuse_budget_traced("split depth cap — serial rerun");
        return;
    }
    let pending = std::mem::take(&mut st.pending);
    drop(st);
    let mut claims = Vec::new();
    let mut entries = Vec::new();
    for p in &pending {
        for &c in &p.claims {
            claims.push(RoundClaim {
                claim: c,
                consumed: p.consumed,
                jbits: p.jbits,
                child_base: p.child_base,
            });
        }
        entries.push(RoundEntry {
            consumed: p.consumed,
            jbits: p.jbits,
            child_base: p.child_base,
        });
    }
    spill.round_sources[round]
        .total
        .store(claims.len() as u64, Ordering::SeqCst);
    let _ = spill.round_plans[round].set(Arc::new(RoundPlan { claims, entries }));
}

/// Gather one level-0 batch's exact claims/bytes/tuples from the inner
/// accept files.
fn level0_batch_claims(spill: &HjSpill, part: u32) -> (Vec<InnerClaim>, u64, u64) {
    let mut claims = Vec::new();
    let (mut bytes, mut tuples) = (0u64, 0u64);
    for slot in 0..spill.inner.len() {
        let g = lockm(&spill.inner[slot]);
        if let Some(router) = g.as_ref() {
            for x in router.file.part_extents(part) {
                claims.push(InnerClaim { src: InnerSrc::Accept { slot }, extent: x });
            }
            bytes += router.file.part_len(part);
            tuples += router.counts[part as usize];
        }
    }
    (claims, bytes, tuples)
}

/// PLAN-BATCHES (§5.2 step [1]): one bookkeeping task over the EXACT
/// directory sizes; assigns leaves, enqueues round-1 splits, freezes when no
/// splits are needed. Runs strictly after BUILD-COMBINE (deps), so the
/// batch-0 demote decision is final and every inner file is frozen.
struct PlanBatchesWork(Arc<RuntimeHjShared>);

impl runtime::TaskSetWork for PlanBatchesWork {
    fn run_morsel(&self, _worker: usize, _range: runtime::MorselRange) {
        let payload = &self.0;
        if payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(spill) = payload.spill.as_ref() else { return };
        let r = catch_unwind(AssertUnwindSafe(|| {
            let demoted = spill.batch0_demoted.load(Ordering::SeqCst);
            {
                let mut st = lockm(&spill.plan);
                st.map = Some(LeafMap::new(spill.nbatch));
                for b in 0..spill.nbatch {
                    if b == 0 && !demoted {
                        st.map.as_mut().expect("just set").set_leaf(0, LEAF_INMEM);
                        continue;
                    }
                    let (claims, bytes, tuples) = level0_batch_claims(spill, b);
                    if !resolve_node(
                        payload, spill, &mut st, b, spill.log2n, true, claims, bytes, tuples, 0,
                    ) {
                        return;
                    }
                }
            }
            arm_round_or_freeze(payload, spill, 0);
        }));
        if r.is_err() {
            payload.fail(PgError::new(ERROR, "runtime hash-join PLAN-BATCHES panicked").into());
        }
    }

    fn finalize(&self) {}
}

/// SPLIT-ROUND r (§5.3): claims = parent extents; each record routes by the
/// next remix bits into the worker's round file (child node partition).
/// finalize (single-threaded) sizes the children EXACTLY and assigns leaves
/// or the next round.
struct SplitRoundWork {
    payload: Arc<RuntimeHjShared>,
    round: usize,
}

impl runtime::TaskSetWork for SplitRoundWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        let payload = &self.payload;
        if payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(spill) = payload.spill.as_ref() else { return };
        let Some(plan) = spill.round_plans[self.round].get() else {
            payload.fail(PgError::new(ERROR, "split round ran without a round plan").into());
            return;
        };
        let slot = payload.worker_slot(worker);
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<()> {
            let mut guard = lockm(&spill.rounds[self.round][slot]);
            for ordinal in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                let rc = &plan.claims[ordinal as usize];
                let buf = read_inner_claim(spill, &rc.claim)?;
                let mut it = BatchRecords::new(buf.bytes());
                while let Some((h, tuple)) = it.next_rec()? {
                    let child = rc.child_base + split_child(h, rc.consumed, rc.jbits);
                    let router = guard.get_or_insert_with(|| {
                        BatchRouter::new(
                            &spill.set,
                            ::spillset::SpillSet::file_name("hj-split", self.round as u64, slot),
                            MAX_SPLIT_NODES as u32,
                        )
                    });
                    router.put(child, h, tuple)?;
                }
            }
            if let Some(router) = guard.as_mut() {
                router.flush()?;
            }
            Ok(())
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => payload.fail(e),
            Err(_panic) => payload
                .fail(PgError::new(ERROR, "runtime hash-join split round panicked").into()),
        }
    }

    fn finalize(&self) {
        let payload = &self.payload;
        if payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let Some(spill) = payload.spill.as_ref() else { return };
        let Some(plan) = spill.round_plans[self.round].get() else {
            // Unarmed round (no splits reached this depth): nothing to do;
            // the freeze already happened upstream.
            return;
        };
        let r = catch_unwind(AssertUnwindSafe(|| {
            {
                let mut st = lockm(&spill.plan);
                for e in &plan.entries {
                    for c in 0..(1u32 << e.jbits) {
                        let node = e.child_base + c;
                        let mut claims = Vec::new();
                        let (mut bytes, mut tuples) = (0u64, 0u64);
                        for slot in 0..spill.rounds[self.round].len() {
                            let g = lockm(&spill.rounds[self.round][slot]);
                            if let Some(router) = g.as_ref() {
                                for x in router.file.part_extents(node) {
                                    claims.push(InnerClaim {
                                        src: InnerSrc::Round { round: self.round, slot },
                                        extent: x,
                                    });
                                }
                                bytes += router.file.part_len(node);
                                tuples += router.counts[node as usize];
                            }
                        }
                        if !resolve_node(
                            payload,
                            spill,
                            &mut st,
                            node,
                            e.consumed + e.jbits,
                            false,
                            claims,
                            bytes,
                            tuples,
                            self.round + 1,
                        ) {
                            return;
                        }
                    }
                }
            }
            arm_round_or_freeze(payload, spill, self.round + 1);
        }));
        if r.is_err() {
            payload
                .fail(PgError::new(ERROR, "runtime hash-join split finalize panicked").into());
        }
    }
}

/// Per-leaf build sink: accept claims = frozen inner extents; combine =
/// the shared_build partitioned table; finalize publishes into the leaf
/// table slot. Fresh combined budget per leaf (C parity: one live table).
struct LeafBatchSink {
    shared: Weak<RuntimeHjShared>,
    leaf: usize,
    budget: Arc<JoinBudget>,
    plan: Mutex<Option<Arc<CombinePlan>>>,
}

impl LeafBatchSink {
    fn failed(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| s.failed.load(Ordering::SeqCst))
    }

    /// Leaf slots beyond the frozen plan's count are declared-but-unused
    /// ladder capacity: skip their combine plan/freeze entirely (their
    /// accept/probe sources are zero-granule already). Reads the frozen
    /// count, which is set strictly before any leaf set publishes.
    fn unused(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| {
            s.spill
                .as_ref()
                .and_then(|sp| sp.frozen.get())
                .is_none_or(|fp| self.leaf >= fp.leaves.len())
        })
    }

    fn plan_for(&self, locals: &[JoinBuildLocal]) -> Option<Arc<CombinePlan>> {
        let mut g = lockm(&self.plan);
        if let Some(p) = g.as_ref() {
            return Some(Arc::clone(p));
        }
        match CombinePlan::plan(locals, &self.budget) {
            Ok(p) => {
                let p = Arc::new(p);
                *g = Some(Arc::clone(&p));
                Some(p)
            }
            Err(BudgetExceeded) => {
                drop(g);
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget_traced("leaf build crossed the envelope at seal");
                }
                None
            }
        }
    }
}

impl runtime::ParallelSink for LeafBatchSink {
    type Local = JoinBuildLocal;

    fn fork(&self, worker: usize) -> JoinBuildLocal {
        let Some(shared) = self.shared.upgrade() else {
            return JoinBuildLocal::new(worker, Arc::clone(&self.budget));
        };
        let cap_words = shared
            .spill
            .as_ref()
            .map_or(1 << 17, |sp| (sp.chunk_cap_bytes / 8) as usize);
        JoinBuildLocal::with_chunk_cap(worker, Arc::clone(&self.budget), cap_words)
    }

    fn accept_local(&self, local: &mut JoinBuildLocal, _worker: usize, range: runtime::MorselRange) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let Some(spill) = shared.spill.as_ref() else { return };
        let Some(frozen) = spill.frozen.get() else {
            shared.fail(PgError::new(ERROR, "leaf accept without a frozen batch plan").into());
            return;
        };
        let claims = &frozen.leaves[self.leaf];
        let r = catch_unwind(AssertUnwindSafe(|| -> PgResult<bool> {
            for ordinal in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                let buf = read_inner_claim(spill, &claims[ordinal as usize])?;
                local.begin_run(ordinal);
                let mut it = BatchRecords::new(buf.bytes());
                while let Some((h, tuple)) = it.next_rec()? {
                    if local.push(h, tuple).is_err() {
                        local.end_run();
                        return Ok(false);
                    }
                }
                local.end_run();
            }
            Ok(true)
        }));
        match r {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                // Exact-size model missed (fail-closed): refuse to serial —
                // the leader-side twin is PLAN-BATCHES' est_fits, the same
                // arithmetic source.
                shared.refuse_budget_traced("leaf build crossed the envelope in accept");
            }
            Ok(Err(e)) => {
                mark_self_errored();
                shared.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                shared.fail(
                    PgError::new(ERROR, "runtime hash-join leaf build panicked").into(),
                );
            }
        }
    }

    fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[JoinBuildLocal]) {
        if self.failed() || self.unused() {
            return;
        }
        if let Some(plan) = self.plan_for(locals) {
            plan.combine_partition(part, locals);
        }
    }

    fn finalize(&self, locals: &[JoinBuildLocal]) {
        if self.failed() || self.unused() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let Some(plan) = self.plan_for(locals) else { return };
        *lockm(&shared.leaf_tables[self.leaf]) = Some(Arc::new(freeze(plan, locals)));
    }
}

/// PROBE(i): outer leaf extents against the published leaf table, absorbed
/// into the same plain-agg tail; finalize retires the table when no fill
/// set follows.
struct LeafProbeWork {
    payload: Arc<RuntimeHjShared>,
    leaf: usize,
}

fn leaf_probe_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    leaf: usize,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let spill = payload.spill.as_ref().expect("leaf probe requires spill");
    let outer_plan = Arc::clone(spill.outer_plan.get().ok_or_else(|| {
        Box::new(PgError::new(ERROR, "leaf probe without an outer claim plan"))
    })?);
    let table = lockm(&payload.leaf_tables[leaf]).clone().ok_or_else(|| {
        Box::new(PgError::new(ERROR, "leaf probe without a published leaf table"))
    })?;
    with_worker_exec("runtime hash-join leaf probe without a bound executor", |es, ps| {
        with_join_tree(es, ps, |estate, agg, hj, _outer_ss, hstate, _inner_ss| {
            let saved = shared_saved_outer_slot(hj);
            let mcx = estate.es_query_cxt;
            for ordinal in range.clone() {
                ::postgres_seams::check_for_interrupts::call()?;
                let (slot, extent) = outer_plan.leaves[leaf][ordinal as usize];
                let g = lockm(&spill.outer[slot]);
                let Some(router) = g.as_ref() else {
                    return Err(Box::new(PgError::new(
                        ERROR,
                        "leaf probe claim without an outer file",
                    )));
                };
                let buf = read_extent_aligned(&router.file, extent)?;
                drop(g);
                let mut it = BatchRecords::new(buf.bytes());
                while let Some((h, tuple)) = it.next_rec()? {
                    // SAFETY: the aligned buffer's record layout keeps the
                    // tuple image MAXALIGNed and live across this probe (the
                    // buffer outlives the loop; the slot is cleared below
                    // before the buffer drops).
                    unsafe {
                        let mtup = NonNull::new_unchecked(tuple.as_ptr() as *mut MinimalTupleData);
                        ::exectuples::exec_store_minimal_tuple_ptr(
                            &mut estate.es_tupleTable[saved.0 as usize],
                            mcx,
                            mtup,
                        );
                    }
                    shared_probe_outer_hashed(
                        hj,
                        hstate,
                        estate,
                        &table,
                        saved,
                        h,
                        &mut |_hj, estate, out| ::nodeagg::agg_plain_build_accept(agg, estate, out),
                    )?;
                }
                // The saved slot must not outlive the claim's buffer.
                ::exectuples::exec_clear_tuple(estate.slot_mut(saved), mcx);
            }
            let pslot = worker - payload.pins_base;
            {
                let mut g = lockm(&payload.partials[pslot]);
                agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
            }
            Ok(())
        })
    })
}

impl runtime::TaskSetWork for LeafProbeWork {
    fn run_morsel(&self, worker: usize, range: runtime::MorselRange) {
        if self.payload.failed.load(Ordering::SeqCst) {
            return;
        }
        let payload = &self.payload;
        let leaf = self.leaf;
        let r = catch_unwind(AssertUnwindSafe(|| {
            leaf_probe_morsel_body(payload, leaf, worker, range)
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                payload.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                payload.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a leaf probe")
                        .into(),
                );
            }
        }
    }

    fn finalize(&self) {
        // One live table: without a fill set this probe is the last reader.
        let fill = self.payload.spill.as_ref().is_some_and(|sp| sp.fill_inner);
        if !fill {
            lockm(&self.payload.leaf_tables[self.leaf]).take();
        }
    }
}

// ---------------------------------------------------------------------------
// Helper (worker) side: entry task + POST_TASK_PARK drive.
// ---------------------------------------------------------------------------

fn runtime_hj_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_hj_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeHjShared>() else { return };
    // Every LAUNCHED helper bumps `exited` exactly once, on EVERY exit path
    // (the leader's liveness reap counts these against `launched`).
    // HOOK-frame placement (the scan arm's law): the standing driver reuses
    // helper_drive and must NOT bump — standing exits ride the board's
    // claimed/detached accounting.
    let _exit = ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime hash-join helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The standing driver (M2 inc-1, parallel::set_standing_driver): the
/// POST_TASK_PARK body minus the ExitBump; exit-committed unwinds (FATAL)
/// rethrow to the gang glue (a terminated worker must die).
fn runtime_hj_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeHjShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if let Err(unwind) = r {
        payload
            .fail(PgError::new(ERROR, "runtime hash-join standing executor panicked").into());
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            shared.parallel_leader_proc_number,
        ));
        if parallel::standing::is_exit_unwind(&*unwind) {
            std::panic::resume_unwind(unwind);
        }
        return;
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeHjShared>) {
    let _ = shared;
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving; the reap must convert it into
    // a prompt error (scripts/runtime-liveness-e2e.sh).
    super::test_helper_panic("hashjoin");
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-hashjoin: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-hashjoin: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-hashjoin: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let lane = std::cell::RefCell::new(Some(lane));
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg, &mut lane.borrow_mut())
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                // Budget refusals are NOT query errors (the leader falls
                // back to the serial arm); the leader drops any recorded
                // secondary errors on that path.
                payload.fail(e);
                // F1 liveness (the agg-arm wedge mechanism, closed here
                // too): a helper that errored BEFORE joining the drive
                // (build_worker_exec failure) has aborted the RG via
                // fail() — but an aborted PINNED RG still needs a driver to
                // run invalidate/finalize/complete, or the leader parks on
                // its recheck cadence until the reap. Drive the closed
                // generation to completion here (pure protocol cleanup,
                // the drain_rg discipline); post-drive errors find it
                // already complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!(
                    "runtime-hashjoin: helper bind refused: {}",
                    e.message()
                ));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeHjShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
    lane: &mut Option<runtime::ExternalLane>,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    HJ_PAYLOAD.with(|c| *c.borrow_mut() = Some(Arc::clone(payload)));
    let _end = super::standing_channel::drive_pool_serve(&payload.rt, local, rg, lane);
    HJ_PAYLOAD.with(|c| *c.borrow_mut() = None);
    let self_errored =
        HJ_WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    teardown_worker_exec(!self_errored)
}

fn build_worker_exec(payload: &Arc<RuntimeHjShared>) -> PgResult<()> {
    HJ_WORKER_EXEC.with(|cell| -> PgResult<()> {
        if let Some(stale) = cell.borrow_mut().take() {
            crate::querydesc::release_query_desc_seam(stale.qd);
        }
        // SAFETY: leader-arena pstmt, alive until DestroyParallelContext
        // joins this helper (SendConst contract).
        let pstmt: &PlannedStmt<'_> = unsafe { &*payload.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &payload.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let armed = (|| -> PgResult<()> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime hash-join worker ExecutorStart");
                x.with_mut(|d| {
                    if let Some(chain) = payload.chain.get() {
                        // m5p1 multibuild: congruence verify + arm every
                        // scan's staging + begin the plain-agg build.
                        return mb_arm_worker(&mut d.estate, &mut d.planstate, chain);
                    }
                    with_join_tree(&mut d.estate, &mut d.planstate, |estate, agg, hj, outer_ss, hstate, inner_ss| {
                        if !agg_runtime_partial_admissible(agg)
                            && !(hj_aggjoin_numeric_enabled()
                                && ::nodeagg::runtime_partial::agg_poly_partial_admissible(agg))
                        {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime hash-join worker fold plan diverged from the leader's",
                            )));
                        }
                        if !shared_join_admissible(hj, hstate) {
                            return Err(Box::new(PgError::new(
                                ERROR,
                                "runtime hash-join worker join shape diverged from the leader's",
                            )));
                        }
                        // Per-row drive staging on both scans (the census
                        // RowFeed shape: PREWHERE bitmap when kernel-shaped,
                        // stitched tiers on; per-row emits re-check quals).
                        super::arm_scan_staging(
                            outer_ss,
                            estate,
                            super::ScanFeedShape::RowFeed {
                                ctx: "runtime hash-join probe feed",
                                stitch: true,
                            },
                        )?;
                        super::arm_scan_staging(
                            inner_ss,
                            estate,
                            super::ScanFeedShape::RowFeed {
                                ctx: "runtime hash-join build feed",
                                stitch: true,
                            },
                        )?;
                        ::nodeagg::agg_plain_build_begin(agg, estate)?;
                        Ok(())
                    })
                })
            })
        })();
        match armed {
            Ok(()) => {
                *cell.borrow_mut() =
                    Some(WorkerExec { qd, errored: std::cell::Cell::new(false) });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    HJ_WORKER_EXEC.with(|cell| -> PgResult<()> {
        let Some(ex) = cell.borrow_mut().take() else { return Ok(()) };
        if clean {
            let r = crate::execmain::executor_finish_seam(ex.qd)
                .and_then(|()| crate::execmain::executor_end_seam(ex.qd));
            match r {
                Ok(()) => {
                    crate::querydesc::free_query_desc_seam(ex.qd);
                    Ok(())
                }
                Err(e) => {
                    crate::querydesc::release_query_desc_seam(ex.qd);
                    Err(e)
                }
            }
        } else {
            crate::querydesc::release_query_desc_seam(ex.qd);
            Ok(())
        }
    })
}

fn runtime_hj_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeHjShared>() else { return };
    payload.abort_rg();
    // Standing channel (M2 inc-1): complete the standing join on leader
    // unwind paths (standing_channel::shutdown_standing_join).
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    super::standing_channel::shutdown_standing_join(&payload.standing, rg.as_ref(), &|rg| {
        drain_rg(payload.rt, rg)
    });
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_hashjoin_main",
            runtime_hj_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_hj_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_hj_private_shutdown);
    });
}

// ---------------------------------------------------------------------------
// m5p1 MULTIBUILD (band 88001) — the multi-pipeline QuerySpec plan-walk.
//
// The matrix row `hashjoin-multibuild-sql` named the gap: "NO arm admits 2+
// build sides in one engagement (DAG substrate runtime-ready; the
// multi-pipeline QuerySpec plan-walk is the missing piece)". This section is
// that walk: a SERIAL-plan plain Agg over a TREE of probe-local hash joins
// (INNER/LEFT/SEMI/ANTI — no fill task sets, no match-flag barriers) over
// lane-fusible SeqScans decomposes into PIPELINES (Hyper/Umbra decomposition
// at this altitude):
//
//   * one BUILD pipeline per join j: drive the scan at the bottom of j's
//     build subtree, probe every frozen table on the way up INSIDE the build
//     subtree, and accept the emitted rows into table j (ACCEPT/COMBINE task
//     set pair via `runtime::sink_tasksets`, dormant-default machinery);
//   * one PROBE pipeline (last): drive the root outer-descent scan, probe
//     the root-path tables bottom-up, absorb into the plain-agg partial tail
//     (M1's runtime_partial, verbatim).
//
// Pipelines are emitted build-subtrees-first, so every accept's deps (the
// COMBINEs of the tables it probes) precede it — a deps-DAG the DEFAULT
// sequential dispatch executes exactly like the M3.5 spill ladder does
// (PGRUST_RUNTIME_PIPELINE_DAG stays orthogonal: ON overlaps independent
// build subtrees, OFF runs them in emission order).
//
// UNBATCHED ONLY: every build side must fit nbatch==1 under the C combined
// rule — an estimate above it refuses to the serial arms (the spill ladder
// stays single-join; its row owns any future multibuild-spill flip). The
// single-join arm above is byte-untouched: trees divert at the shape gate
// behind `hj_multibuild_enabled` and everything below runs only for them.
// ---------------------------------------------------------------------------

/// Chain-admitted join types: exactly the probe-local four (no right-fill
/// family, no RIGHT_SEMI) — every emission decision is taken during the
/// probe of one outer row, so no cross-task barrier is needed at any level.
fn mb_jointype_admits(jt: ::types_nodes::JoinType) -> bool {
    matches!(
        jt,
        ::types_nodes::JoinType::JOIN_INNER
            | ::types_nodes::JoinType::JOIN_LEFT
            | ::types_nodes::JoinType::JOIN_SEMI
            | ::types_nodes::JoinType::JOIN_ANTI
    )
}

/// One pipeline of the decomposition: drive `scan`, probe `probes` (join
/// indices, bottom-up), terminate in build table `sink` (Some) or the
/// plain-agg absorb (None — the final pipeline, emitted last).
struct MbPipeline {
    scan: usize,
    probes: Vec<usize>,
    sink: Option<usize>,
}

/// The engagement's multibuild descriptor (leader-built at admission; the
/// worker sides read it through the shared payload — indices refer to the
/// PREORDER (self, outer-subtree, build-subtree) enumeration both the plan
/// walk and the worker collector produce).
pub(super) struct MbChain {
    pipelines: Vec<MbPipeline>,
    /// Per join index: its build sink (frozen-table publisher).
    sinks: Vec<Arc<MbBuildSink>>,
    /// Per scan index: its morsel source (per-AM, `k2_task_source`).
    sources: Vec<Arc<dyn runtime::MorselSource>>,
    /// Per join index: the plan node's join type (worker-side congruence
    /// verification — the rebuilt tree must be the admitted tree).
    jointypes: Vec<::types_nodes::JoinType>,
    nscans: usize,
    /// SE-AGGJOIN (band 87001): the final pipeline's terminal is the GROUPED
    /// (AGG_HASHED) sink — per-worker hashed builds + grouped partial export
    /// (false = the m5p1 plain-agg tail, byte-identical).
    grouped: bool,
    /// SE-MBSHARED: the engagement resolved `hj_mbshared_enabled()` at
    /// admission (leader-side, once) — every morsel body reads THIS bit so
    /// a per-row knob probe never enters the walk. False = today's path.
    shared1a: bool,
    /// SE-MBSEAT: knob resolved once at engage (true requires `shared1a` —
    /// compose, never contradict) AND the per-join economics verdicts.
    /// Build morsel bodies consult `mbseat && seat_ok[j]` before the
    /// per-plan `dense_seat_build_col` introspection.
    mbseat: bool,
    seat_ok: Vec<bool>,
}

/// Admission output handed to `engage` (sinks are constructed there — they
/// hold the payload Weak).
pub(super) struct MbInit {
    pipelines: Vec<MbPipeline>,
    sources: Vec<Arc<dyn runtime::MorselSource>>,
    jointypes: Vec<::types_nodes::JoinType>,
    /// Per join index: the gang envelope its build table gets.
    envelopes: Vec<usize>,
    /// SE-MBSHARED: per join index, the planner's build-rows estimate — the
    /// single-pass directory's up-front size (0 rows sizes the minimum
    /// directory; the estimate is a capacity hint, not a correctness input).
    build_rows: Vec<u64>,
    /// SE-MBSEAT: per join index, the seat-economics verdict — the OUTER
    /// subtree's probe estimate amortizes the seat's O(build) construction
    /// (the GL-HJSEAT-2 ratio, per table). Read only on knob-armed
    /// engagements.
    seat_ok: Vec<bool>,
    nscans: usize,
    /// SE-AGGJOIN: grouped (AGG_HASHED) terminal (see `MbChain::grouped`).
    grouped: bool,
}

// --- Plan-tree walk (pass A: shape + parallel safety + sizing inputs) -----

enum MbChild {
    Join(usize),
    Scan(usize),
}

struct MbPlanInfo {
    jointypes: Vec<::types_nodes::JoinType>,
    hash_rows: Vec<f64>,
    hash_widths: Vec<i32>,
    children: Vec<(MbChild, MbChild)>,
    nscans: usize,
    /// SE-MBSEAT sizing inputs: per scan index / per join index, the plan
    /// node's own rows estimate — `outer_rows_of` resolves a join's OUTER
    /// child estimate (its per-table probe volume) from these.
    scan_rows: Vec<f64>,
    join_rows: Vec<f64>,
}

/// SE-MBSEAT: the probe-volume estimate of join `j`'s table = its OUTER
/// child's plan rows (every row of the outer subtree probes table j once).
fn outer_rows_of(info: &MbPlanInfo, j: usize) -> f64 {
    match info.children[j].0 {
        MbChild::Scan(k) => info.scan_rows[k],
        MbChild::Join(i) => info.join_rows[i],
    }
}

/// Recursive plan walk: `None` = shape outside the multibuild envelope
/// (refuse — Gather-suppression never keyed it, so this is the walk's own
/// fail-closed gate). Preorder: reserve this join's slot, then the outer
/// subtree, then the build subtree.
fn mb_plan_walk(
    node: ::types_nodes::Node<'_>,
    info: &mut MbPlanInfo,
) -> PgResult<Option<MbChild>> {
    match node.node_tag() {
        NodeTag::T_SeqScan => {
            let scan = node.as_seq_scan().expect("SeqScan tag");
            if !exprs_parallel_safe(scan.scan.plan.qual.iter())?
                || !exprs_parallel_safe(scan.scan.plan.targetlist.iter())?
            {
                return Ok(None);
            }
            let idx = info.nscans;
            info.nscans += 1;
            info.scan_rows.push(scan.scan.plan.plan_rows);
            Ok(Some(MbChild::Scan(idx)))
        }
        NodeTag::T_HashJoin => {
            let hj = node.as_hash_join().expect("HashJoin tag");
            if !mb_jointype_admits(hj.join.jointype) {
                return Ok(None);
            }
            if !exprs_parallel_safe(hj.hashclauses.iter())?
                || !exprs_parallel_safe(hj.join.joinqual.iter())?
                || !exprs_parallel_safe(hj.join.plan.qual.iter())?
                || !exprs_parallel_safe(hj.join.plan.targetlist.iter())?
            {
                return Ok(None);
            }
            let (Some(outer), Some(hash_node)) =
                (hj.join.plan.lefttree, hj.join.plan.righttree)
            else {
                return Ok(None);
            };
            if hash_node.node_tag() != NodeTag::T_Hash {
                return Ok(None);
            }
            let hash = hash_node.as_hash().expect("Hash tag");
            let Some(inner) = hash.plan.lefttree else { return Ok(None) };
            let j = info.jointypes.len();
            if j >= MB_MAX_JOINS {
                return Ok(None);
            }
            info.jointypes.push(hj.join.jointype);
            info.hash_rows.push(hash.plan.plan_rows);
            info.hash_widths.push(hash.plan.plan_width);
            info.join_rows.push(hj.join.plan.plan_rows);
            info.children.push((MbChild::Scan(usize::MAX), MbChild::Scan(usize::MAX)));
            let Some(oc) = mb_plan_walk(outer, info)? else { return Ok(None) };
            let Some(ic) = mb_plan_walk(inner, info)? else { return Ok(None) };
            info.children[j] = (oc, ic);
            Ok(Some(MbChild::Join(j)))
        }
        _ => Ok(None),
    }
}

/// Pipeline decomposition over the admitted topology (see the section doc):
/// build subtrees first, root outer-descent last — deps-safe emission order
/// by induction (every prober of table j is emitted after j's builder).
fn mb_decompose(info: &MbPlanInfo) -> Vec<MbPipeline> {
    fn emit(
        info: &MbPlanInfo,
        node: &MbChild,
        probes_topdown: &mut Vec<usize>,
        sink: Option<usize>,
        out: &mut Vec<MbPipeline>,
    ) {
        match *node {
            MbChild::Scan(k) => {
                let mut probes = probes_topdown.clone();
                probes.reverse(); // execute bottom-up (deepest join first)
                out.push(MbPipeline { scan: k, probes, sink });
            }
            MbChild::Join(j) => {
                let (ref oc, ref ic) = info.children[j];
                let mut none = Vec::new();
                emit(info, ic, &mut none, Some(j), out);
                probes_topdown.push(j);
                emit(info, oc, probes_topdown, sink, out);
                probes_topdown.pop();
            }
        }
    }
    let mut out = Vec::new();
    emit(info, &MbChild::Join(0), &mut Vec::new(), None, &mut out);
    out
}

// --- State-tree walk (pass B: admissibility + fusibility + AM + geometry) --

/// Sequential mutable state walk, congruent with the plan walk's preorder.
/// `Ok(None)` = refuse (reason already ticked by the caller's funnel).
/// Collects each scan's morsel source and whether any side is heap-fed.
struct MbStateInfo {
    sources: Vec<(u64, Arc<dyn runtime::MorselSource>)>,
    heap_fed: bool,
    njoins: usize,
}

fn mb_state_walk<'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    k2_heap: bool,
    info: &mut MbStateInfo,
) -> PgResult<Option<()>> {
    match node {
        crate::procnode::PlanStateNode::HashJoin(hjn) => {
            let hjn: &mut crate::procnode::HashJoinNode<'mcx> = hjn;
            info.njoins += 1;
            if !shared_join_admissible(&hjn.state, &hjn.hash.state) {
                return Ok(None);
            }
            if !::nodehashjoin::lane_join_untouched(&hjn.state, &hjn.hash.state) {
                return Ok(None);
            }
            if !mb_jointype_admits(hjn.state.plan.join.jointype) {
                return Ok(None);
            }
            if mb_state_walk(&mut hjn.outer, estate, k2_heap, info)?.is_none() {
                return Ok(None);
            }
            let hash: &mut crate::procnode::HashSubNode<'mcx> = &mut hjn.hash;
            mb_state_walk(&mut hash.child, estate, k2_heap, info)
        }
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            if !seq_scan_fusible(ss, estate)?
                || !(::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
                    || (k2_heap && ::nodeseqscan::seq_scan_is_heap(ss)))
            {
                return Ok(None);
            }
            info.heap_fed |= ::nodeseqscan::seq_scan_is_heap(ss);
            let Some(src) = k2_task_source(ss, estate)? else { return Ok(None) };
            info.sources.push(src);
            Ok(Some(()))
        }
        _ => Ok(None),
    }
}

// --- The build sink (per join): JoinBuildSink minus spill/demote ----------

pub(super) struct MbBuildSink {
    join: usize,
    budget: Arc<JoinBudget>,
    plan: Mutex<Option<Arc<CombinePlan>>>,
    table: Mutex<Option<Arc<FrozenJoinTable>>>,
    shared: Weak<RuntimeHjShared>,
    /// SE-MBSHARED single-pass build (Phase 1a, multibuild twin): Some ⇒
    /// workers CAS-insert directly into this shared directory during accept
    /// (the 256 combine tasks no-op; finalize seals via
    /// `finish_single_pass`). Sized up front from THIS join's planner
    /// estimate against THIS join's budget; a directory the estimate cannot
    /// afford leaves None and the table rides the two-pass build (never a
    /// refusal on this account — the single-join 1a posture). None always
    /// when the knob is off.
    singlepass: Option<Arc<SharedBuildDir>>,
}

impl MbBuildSink {
    fn failed(&self) -> bool {
        self.shared.upgrade().is_none_or(|s| s.failed.load(Ordering::SeqCst))
    }

    fn table_clone(&self) -> Option<Arc<FrozenJoinTable>> {
        lockm(&self.table).clone()
    }

    fn plan_for(&self, locals: &[JoinBuildLocal]) -> Option<Arc<CombinePlan>> {
        let mut g = lockm(&self.plan);
        if let Some(p) = g.as_ref() {
            return Some(Arc::clone(p));
        }
        match CombinePlan::plan(locals, &self.budget) {
            Ok(p) => {
                let p = Arc::new(p);
                *g = Some(Arc::clone(&p));
                Some(p)
            }
            Err(BudgetExceeded) => {
                drop(g);
                lane_trace(
                    "runtime-hashjoin: REFUSED (multibuild envelope crossed at seal) — serial rerun",
                );
                if let Some(s) = self.shared.upgrade() {
                    s.refuse_budget();
                }
                None
            }
        }
    }
}

impl runtime::ParallelSink for MbBuildSink {
    type Local = JoinBuildLocal;

    fn fork(&self, worker: usize) -> JoinBuildLocal {
        let mut local = JoinBuildLocal::new(worker, Arc::clone(&self.budget));
        if let Some(dir) = &self.singlepass {
            // SE-MBSHARED: this Local links tuples straight into the shared
            // directory in `push` (accept), bypassing part_refs/COMBINE —
            // the JoinBuildSink single-pass fork verbatim. The dense seat
            // never arms on multibuild Locals (the seat is single-join
            // only), so the attach's exclusivity assert cannot fire.
            local.attach_shared_dir(Arc::clone(dir));
        }
        local
    }

    fn accept_local(&self, local: &mut JoinBuildLocal, worker: usize, range: runtime::MorselRange) {
        if self.failed() {
            return;
        }
        let Some(shared) = self.shared.upgrade() else { return };
        let r = catch_unwind(AssertUnwindSafe(|| {
            mb_accept_morsel_body(&shared, self.join, local, worker, range)
        }));
        match r {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) => {
                lane_trace(
                    "runtime-hashjoin: REFUSED (multibuild envelope crossed in build) — serial rerun",
                );
                shared.refuse_budget();
            }
            Ok(Err(e)) => {
                mark_self_errored();
                shared.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                shared.fail(
                    PgError::new(ERROR, "runtime hash-join worker panicked in a multibuild morsel")
                        .into(),
                );
            }
        }
    }

    fn partitions(&self) -> u64 {
        PARTITIONS as u64
    }

    fn combine(&self, part: u64, _worker: usize, locals: &[JoinBuildLocal]) {
        if self.failed() {
            return;
        }
        // SE-MBSHARED single-pass: chains are already CAS-linked during
        // accept — the 256 combine tasks are pure no-ops (the seal/freeze
        // happens in finalize), exactly the JoinBuildSink posture.
        if self.singlepass.is_some() {
            return;
        }
        if let Some(plan) = self.plan_for(locals) {
            plan.combine_partition(part, locals);
        }
    }

    fn finalize(&self, locals: &[JoinBuildLocal]) {
        if self.failed() {
            return;
        }
        // SE-MBSHARED single-pass: seal the shared directory (barrier-gated
        // grow_buckets on an underestimate) into a plan the frozen table
        // consumes as-is; a grow the envelope cannot afford refuses to the
        // serial arm BY NAME (R5 — the multibuild envelope posture).
        let plan = if let Some(dir) = &self.singlepass {
            match finish_single_pass(locals, Arc::clone(dir), &self.budget) {
                Ok(p) => Arc::new(p),
                Err(BudgetExceeded) => {
                    lane_trace(
                        "runtime-hashjoin: REFUSED (multibuild single-pass grow crossed envelope) — serial rerun",
                    );
                    if let Some(s) = self.shared.upgrade() {
                        s.refuse_budget();
                    }
                    return;
                }
            }
        } else {
            let Some(plan) = self.plan_for(locals) else { return };
            plan
        };
        let table = freeze(plan, locals);
        // SE-MBSEAT engagement witness (e2e-grepped): fires only when the
        // order-free seat actually built (knob + economics + introspection
        // + range/budget gates all passed).
        if table.has_seat() {
            lane_trace(&format!(
                "runtime-hashjoin: multibuild dense-seat (join={})",
                self.join
            ));
        }
        *lockm(&self.table) = Some(Arc::new(table));
    }
}

// --- Worker-side tree collection + pipeline drive --------------------------

/// Disjoint mutable references into the worker's rebuilt join tree, in the
/// SAME preorder as the plan walk. `Option` cells so each drive can `take()`
/// its disjoint set without unsafe splitting.
#[derive(Default)]
struct MbRefs<'a, 'mcx> {
    joins: Vec<
        Option<(
            &'a mut ::nodehashjoin::HashJoinState<'mcx>,
            &'a mut ::nodehash::HashState<'mcx>,
        )>,
    >,
    scans: Vec<Option<&'a mut ::nodeseqscan::SeqScanState<'mcx>>>,
}

fn mb_collect<'a, 'mcx>(
    node: &'a mut crate::procnode::PlanStateNode<'mcx>,
    refs: &mut MbRefs<'a, 'mcx>,
) -> PgResult<()> {
    match node {
        crate::procnode::PlanStateNode::HashJoin(hjn) => {
            let hjn: &'a mut crate::procnode::HashJoinNode<'mcx> = hjn;
            let hash: &'a mut crate::procnode::HashSubNode<'mcx> = &mut hjn.hash;
            refs.joins.push(Some((&mut hjn.state, &mut hash.state)));
            mb_collect(&mut hjn.outer, refs)?;
            mb_collect(&mut hash.child, refs)
        }
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            refs.scans.push(Some(ss));
            Ok(())
        }
        _ => Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join multibuild worker tree diverged from the leader's",
        ))),
    }
}

/// Split the worker root into (agg state, join-tree root) — the chain-mode
/// counterpart of `with_join_tree`'s fixed destructure: two disjoint field
/// borrows of the Agg plan-state node.
fn mb_split_root<'a, 'mcx>(
    planstate: &'a mut Option<crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<(&'a mut ::nodeagg::AggStateData<'mcx>, &'a mut crate::procnode::PlanStateNode<'mcx>)>
{
    let Some(crate::procnode::PlanStateNode::Agg(aps)) = planstate.as_mut() else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join multibuild worker plan is not a plain Agg root",
        )));
    };
    let aps: &'a mut crate::procnode::AggPlanState<'mcx> = aps;
    Ok((&mut aps.agg, &mut aps.outer))
}

/// One probe level of a pipeline: join state + its hash state (candidate
/// slot owner) + the frozen table to probe.
type MbProbe<'x, 'mcx> = (
    &'x mut ::nodehashjoin::HashJoinState<'mcx>,
    &'x mut ::nodehash::HashState<'mcx>,
    Arc<FrozenJoinTable>,
);

/// A pipeline's terminal.
enum MbTerm<'x, 'mcx> {
    Build {
        hs: &'x mut ::nodehash::HashState<'mcx>,
        local: &'x mut JoinBuildLocal,
        /// SE-MBSEAT: Some(build key col) ⇒ keyed accept — the Local
        /// tracks `(ref, key)` pairs for the order-free seat.
        key_col: Option<u16>,
    },
    Agg { agg: &'x mut ::nodeagg::AggStateData<'mcx> },
    /// SE-AGGJOIN grouped terminal: the worker's OWN hashed build (C's
    /// checked per-row transition program; spill-mode entries are caught by
    /// the post-morsel export, which refuses the engagement fail-closed).
    AggHash { agg: &'x mut ::nodeagg::AggStateData<'mcx> },
}

/// One emitted source row through the pipeline's probe levels (depth-first;
/// each level's emit fully consumes before the next candidate overwrites
/// the level's result slot — the serial nesting discipline) into the
/// terminal. A build terminal that crosses its envelope sets `crossed`;
/// later rows short-circuit and the drive refuses (R5 serial rerun).
fn mb_row<'mcx>(
    probes: &mut [MbProbe<'_, 'mcx>],
    term: &mut MbTerm<'_, 'mcx>,
    crossed: &std::cell::Cell<bool>,
    // SE-MBSHARED probe hoist (`MbChain::shared1a`, resolved once at
    // admission): true ⇒ the frozen table is passed by reference — no
    // per-row refcount traffic (the witnessed contention at the refuted
    // grid cells: every worker RMWing the same two counter cache lines
    // once per row per level). false ⇒ today's per-row clone, byte-for-
    // byte (the knob's OFF arm).
    shared1a: bool,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
) -> PgResult<()> {
    if crossed.get() {
        return Ok(());
    }
    let Some((first, rest)) = probes.split_first_mut() else {
        match term {
            MbTerm::Agg { agg } => {
                return ::nodeagg::agg_plain_build_accept(agg, estate, slot);
            }
            MbTerm::AggHash { agg } => {
                return ::nodeagg::agg_hash_build_accept(agg, estate, slot);
            }
            MbTerm::Build { hs, local, key_col } => {
                let accepted = match key_col {
                    Some(col) => shared_build_accept_keyed(hs, estate, slot, local, *col)?,
                    None => shared_build_accept(hs, estate, slot, local)?,
                };
                if accepted.is_err() {
                    crossed.set(true);
                }
                return Ok(());
            }
        }
    };
    let (hj, hs, table) = first;
    if shared1a {
        // SE-MBSEAT dispatch: seat existence IS the toggle (the single-join
        // arm's law) — a seat only ever builds on knob-armed engagements.
        if table.has_seat() {
            return shared_probe_outer_dense(hj, hs, estate, table, slot, &mut |_hj, estate, out| {
                mb_row(rest, term, crossed, true, estate, out)
            });
        }
        // Field-disjoint borrows of `first`: the table rides as `&` while
        // the join/hash states ride as `&mut` — the walk, probe order and
        // emission are unchanged, only the refcount round-trip is gone.
        return shared_probe_outer(hj, hs, estate, table, slot, &mut |_hj, estate, out| {
            mb_row(rest, term, crossed, true, estate, out)
        });
    }
    let table = Arc::clone(table);
    shared_probe_outer(hj, hs, estate, &table, slot, &mut |_hj, estate, out| {
        mb_row(rest, term, crossed, false, estate, out)
    })
}

/// Drive one claimed morsel of one pipeline: position the scan (per-AM),
/// stream surviving rows through `mb_row`. `Ok(true)` = clean; `Ok(false)`
/// = a build terminal crossed its envelope (refusal, not error).
fn mb_drive_claim<'mcx>(
    scan: &mut ::nodeseqscan::SeqScanState<'mcx>,
    probes: &mut [MbProbe<'_, 'mcx>],
    mut term: MbTerm<'_, 'mcx>,
    shared1a: bool,
    estate: &mut EStateData<'mcx>,
    range: &runtime::MorselRange,
) -> PgResult<bool> {
    let crossed = std::cell::Cell::new(false);
    if ::nodeseqscan::seq_scan_is_heap(scan) {
        let mut src = HeapBatchSource::new(scan);
        let drove = (|| -> PgResult<()> {
            src.position(estate, range.clone())?;
            loop {
                let n = src.next_batch(estate)?;
                if n == 0 {
                    return Ok(());
                }
                ::postgres_seams::check_for_interrupts::call()?;
                let skip = {
                    let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                    src.skip_sel().map(|s| {
                        w[..s.len()].copy_from_slice(s);
                        w
                    })
                };
                ::exectuples::for_each_live(
                    skip.as_ref().map(|w| &w[..]),
                    0,
                    n,
                    |i| -> PgResult<()> {
                        let Some(slot_id) = src.emit(estate, i)? else { return Ok(()) };
                        mb_row(probes, &mut term, &crossed, shared1a, estate, slot_id)
                    },
                )?;
                if crossed.get() {
                    return Ok(());
                }
            }
        })();
        let settled = src.end_claim(estate);
        drove?;
        settled?;
        return Ok(!crossed.get());
    }
    ::nodeseqscan::seq_scan_set_morsel_range(scan, estate, range.start, range.end)?;
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(scan, estate)?;
        if n == 0 {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(scan.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let skip = {
            let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
            ::nodeseqscan::seq_scan_batch_skip_sel(scan).map(|s| {
                w[..s.len()].copy_from_slice(s);
                w
            })
        };
        let skip = skip.as_ref().map(|w| &w[..]);
        ::exectuples::for_each_live(skip, 0, n, |i| -> PgResult<()> {
            let Some(slot_id) = ::nodeseqscan::seq_scan_batch_emit(scan, estate, i)? else {
                return Ok(());
            };
            mb_row(probes, &mut term, &crossed, shared1a, estate, slot_id)
        })?;
        if crossed.get() {
            break;
        }
    }
    Ok(!crossed.get())
}

/// Assemble one pipeline's disjoint refs (scan, probe levels with tables,
/// optional build-target hash state) out of the collected tree.
fn mb_take_pipeline<'a, 'mcx>(
    chain: &MbChain,
    p: &MbPipeline,
    refs: &mut MbRefs<'a, 'mcx>,
) -> PgResult<(
    &'a mut ::nodeseqscan::SeqScanState<'mcx>,
    Vec<MbProbe<'a, 'mcx>>,
    // Build target: the join state rides along for the SE-MBSEAT per-plan
    // introspection (`dense_seat_build_col`) at the accept site.
    Option<(
        &'a mut ::nodehashjoin::HashJoinState<'mcx>,
        &'a mut ::nodehash::HashState<'mcx>,
    )>,
)> {
    let stale = || {
        Box::new(PgError::new(
            ERROR,
            "runtime hash-join multibuild pipeline refers outside the worker tree",
        ))
    };
    let scan = refs.scans.get_mut(p.scan).and_then(|c| c.take()).ok_or_else(stale)?;
    let mut probes = Vec::with_capacity(p.probes.len());
    for &j in &p.probes {
        let (hj, hs) = refs.joins.get_mut(j).and_then(|c| c.take()).ok_or_else(stale)?;
        let table = chain
            .sinks
            .get(j)
            .and_then(|s| s.table_clone())
            .ok_or_else(|| {
                Box::new(PgError::new(
                    ERROR,
                    "runtime hash-join multibuild probe ran without its frozen table",
                ))
            })?;
        probes.push((hj, hs, table));
    }
    let target = match p.sink {
        None => None,
        Some(j) => {
            let (hj, hs) = refs.joins.get_mut(j).and_then(|c| c.take()).ok_or_else(stale)?;
            Some((hj, hs))
        }
    };
    Ok((scan, probes, target))
}

/// One BUILD-pipeline morsel (MbBuildSink::accept_local body). `Ok(true)` =
/// clean; `Ok(false)` = envelope crossed (refusal).
fn mb_accept_morsel_body(
    shared: &Arc<RuntimeHjShared>,
    join: usize,
    local: &mut JoinBuildLocal,
    _worker: usize,
    range: runtime::MorselRange,
) -> PgResult<bool> {
    let chain = Arc::clone(shared.chain.get().expect("multibuild accept without a chain"));
    let p = chain
        .pipelines
        .iter()
        .find(|p| p.sink == Some(join))
        .expect("every build table has exactly one pipeline");
    with_worker_exec("runtime hash-join multibuild morsel without a bound executor", |es, ps| {
        let (_agg, tree) = mb_split_root(ps)?;
        let mut refs = MbRefs::default();
        mb_collect(tree, &mut refs)?;
        let (scan, mut probes, target) = mb_take_pipeline(&chain, p, &mut refs)?;
        let (t_hj, hs) = target.expect("build pipeline has a target table");
        // SE-MBSEAT: knob + per-table economics + the per-plan int4-equality
        // introspection — deterministic from this worker's own executor
        // state, so every tuple-bearing Local arms identically (the
        // all-or-none seat law); armed on the FIRST morsel, idempotent
        // after (armed-or-never).
        let key_col = if chain.mbseat && chain.seat_ok[join] {
            ::nodehashjoin::shared_exec::dense_seat_build_col(t_hj, hs)
        } else {
            None
        };
        if key_col.is_some() && local.single_pass() {
            local.arm_singlepass_keys();
        }
        let key_col = key_col.filter(|_| local.singlepass_keys_armed());
        local.begin_run(range.start);
        let clean = mb_drive_claim(
            scan,
            &mut probes,
            MbTerm::Build { hs, local, key_col },
            chain.shared1a,
            es,
            &range,
        )?;
        local.end_run();
        Ok(clean)
    })
}

/// The final PROBE-pipeline morsel (chain branch of the probe task set):
/// root outer-descent scan → root-path probes → plain-agg absorb → the
/// worker's cumulative partial export (M1 overwrite discipline).
fn mb_probe_morsel_body(
    payload: &Arc<RuntimeHjShared>,
    worker: usize,
    range: runtime::MorselRange,
) -> PgResult<()> {
    let chain = Arc::clone(payload.chain.get().expect("multibuild probe without a chain"));
    let p = chain.pipelines.last().expect("decomposition emits the final pipeline last");
    debug_assert!(p.sink.is_none(), "the last pipeline is the agg-terminal one");
    with_worker_exec("runtime hash-join multibuild probe without a bound executor", |es, ps| {
        let (agg, tree) = mb_split_root(ps)?;
        let mut refs = MbRefs::default();
        mb_collect(tree, &mut refs)?;
        let (scan, mut probes, target) = mb_take_pipeline(&chain, p, &mut refs)?;
        debug_assert!(target.is_none());
        if chain.grouped {
            // SE-AGGJOIN grouped terminal: hashed build per row, then the
            // cumulative-overwrite grouped export (the M1 partial-export
            // discipline, grouped twin). An unexportable table (spill entry
            // or group-cap crossing) refuses the WHOLE engagement to the
            // serial arm — R5, exactly the build-envelope posture.
            let clean = mb_drive_claim(scan, &mut probes, MbTerm::AggHash { agg }, chain.shared1a, es, &range)?;
            debug_assert!(clean, "the hashed agg terminal has no envelope to cross");
            let pslot = worker - payload.pins_base;
            let mut g = lockm(&payload.grouped_partials[pslot]);
            let out = g.get_or_insert_with(Default::default);
            if !::nodeagg::agg_hash_export_grouped_into(agg, es, mbg_max_groups(), out)? {
                drop(g);
                payload.refuse_budget_traced("grouped export envelope crossed");
            }
            return Ok(());
        }
        let clean = mb_drive_claim(
            scan,
            &mut probes,
            MbTerm::Agg { agg },
            chain.shared1a,
            es,
            &range,
        )?;
        debug_assert!(clean, "the agg terminal has no envelope to cross");
        let pslot = worker - payload.pins_base;
        {
            let mut g = lockm(&payload.partials[pslot]);
            agg_runtime_export_partial_into(agg, g.get_or_insert_with(Default::default))?;
        }
        Ok(())
    })
}

/// Chain-mode worker arming (`build_worker_exec` branch): verify the rebuilt
/// tree is CONGRUENT with the admitted one (counts + per-join types +
/// per-join admissibility), arm RowFeed staging on every scan, begin the
/// plain-agg build.
fn mb_arm_worker<'mcx>(
    estate: &mut EStateData<'mcx>,
    planstate: &mut Option<crate::procnode::PlanStateNode<'mcx>>,
    chain: &MbChain,
) -> PgResult<()> {
    let (agg, tree) = mb_split_root(planstate)?;
    if chain.grouped {
        // SE-AGGJOIN: grouped congruence — the rebuilt agg must admit the
        // grouped export exactly as the leader's did (plan-based, or the
        // knob-gated poly manifest — SE-NUMJOIN; or the knob-gated
        // bytes-key admissions — SE-CBKEYS; env is process-shared so
        // both sides resolve the same schema and key mode).
        let numeric = hj_aggjoin_numeric_enabled();
        let word_ok = ::nodeagg::agg_grouped_runtime_admissible(agg)
            || (numeric && ::nodeagg::agg_grouped_poly_runtime_admissible(agg));
        let bp = hj_bpchar_keys_enabled();
        let bytes_ok = !word_ok
            && hj_cbkeys_enabled()
            && (::nodeagg::agg_grouped_bytes_runtime_admissible(agg, bp)
                || (numeric && ::nodeagg::agg_grouped_bytes_poly_runtime_admissible(agg, bp)));
        if !word_ok && !bytes_ok {
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime hash-join grouped worker agg diverged from the leader's",
            )));
        }
    } else if !agg_runtime_partial_admissible(agg)
        && !(hj_aggjoin_numeric_enabled()
            && ::nodeagg::runtime_partial::agg_poly_partial_admissible(agg))
    {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join multibuild worker fold plan diverged from the leader's",
        )));
    }
    let mut refs = MbRefs::default();
    mb_collect(tree, &mut refs)?;
    if refs.joins.len() != chain.jointypes.len() || refs.scans.len() != chain.nscans {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime hash-join multibuild worker tree shape diverged from the leader's",
        )));
    }
    for (i, cell) in refs.joins.iter_mut().enumerate() {
        let (hj, hs) = cell.take().expect("collected join");
        if hj.plan.join.jointype != chain.jointypes[i] || !shared_join_admissible(hj, hs) {
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime hash-join multibuild worker join diverged from the leader's",
            )));
        }
    }
    for cell in refs.scans.iter_mut() {
        let ss = cell.take().expect("collected scan");
        super::arm_scan_staging(
            ss,
            estate,
            super::ScanFeedShape::RowFeed { ctx: "runtime hash-join multibuild feed", stitch: true },
        )?;
    }
    if !chain.grouped {
        ::nodeagg::agg_plain_build_begin(agg, estate)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Leader-side admission + engagement.
// ---------------------------------------------------------------------------

/// Leader-side task-set morsel-source construction, per AM (K2 inc-1).
/// `None` = no geometry (empty part / empty relation / foreign AM) — the
/// caller refuses engagement, fail-closed.
///
/// pgrcolumnar: today's `PgrcolumnarGranuleSource` over
/// `seq_scan_cb_granule_geometry`, verbatim (the construction merely moved
/// from `engage_ceremony` to admission — same object, same posture).
/// heap: K1's seam geometry (`SeqScanSource::granule_map` →
/// `GranuleMap::unbounded(nblocks, HEAP_STARTUP_C0)`) wrapped
/// `GranuleMapSource::new(map, whole_boundary=false, coalesce=false)` —
/// the scan arm's heap posture: boundary-free, sizer-truncated claims, and
/// a boundary-free source must never claim whole boundaries (one claim
/// would take the whole pipeline).
fn k2_task_source<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<(u64, Arc<dyn runtime::MorselSource>)>> {
    if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        let Some((granules, starts)) =
            ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
        else {
            return Ok(None);
        };
        return Ok(Some((
            granules,
            Arc::new(PgrcolumnarGranuleSource {
                starts: Arc::new(starts),
                // This arm feeds claims straight into set_granule_range
                // (single-epoch contract); it does not subdivide
                // multi-epoch claims — never coalesce.
                coalesce: false,
            }) as Arc<dyn runtime::MorselSource>,
        )));
    }
    // Heap (admission guarantees the K2 knobs gate this arm): reuse K1's
    // block geometry through the storage seam — no new geometry policy.
    let Some(map) = SeqScanSource::new(ss).granule_map(estate)? else {
        return Ok(None);
    };
    let total = map.total();
    Ok(Some((
        total,
        Arc::new(runtime::GranuleMapSource::new(Arc::new(map), false, false))
            as Arc<dyn runtime::MorselSource>,
    )))
}

/// The runtime hash-join arm. `None` = not engaged (caller falls through to
/// the serial arms byte-identically — nothing was consumed). `Some(row)` =
/// the plain agg's one finalized result row.
pub(super) fn try_own_agg_over_hash_join_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // M5-1: the router is the DOP source (bench GUC verbatim when set; else
    // engine=runtime arms at pgrust.runtime_dop; else 0 = today's path).
    let dop = router::arm_dop(ArmClass::HashJoin);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    // M5-1 refusal funnel: every admission exit names its gate for the
    // router's consolidated taxonomy (previously silent early returns).
    fn refuse(reason: &'static str) {
        router::tick_refused(ArmClass::HashJoin, reason);
    }

    // SE-AGGJOIN (band 87001): GROUPED (AGG_HASHED) roots divert to the
    // grouped multibuild sink — single joins and trees alike. A FILLED
    // table (this arm's completed engagement, or a serial arm's build after
    // an earlier refusal) is an emit-phase repull, never an offer: return
    // un-engaged and let the serial emit paths (breaker composition /
    // exec_agg's canonical retrieve) stream it. The plain paths below are
    // byte-untouched — they only ever saw AGG_PLAIN engagements.
    if ::nodeagg::agg_is_hashed(agg) {
        if ::nodeagg::agg_hash_table_filled(agg) || ::nodeagg::agg_is_done(agg) {
            return Ok(None);
        }
        router::tick(ArmClass::HashJoin, ArmCounter::Offered);
        if !hj_groupsink_enabled() {
            refuse("groupsink-disabled");
            return Ok(None);
        }
        return try_own_multibuild(agg, hj, estate, rt, dop, true, false);
    }
    // Done-repulls (the post-completion pull that exits via agg_is_done
    // below) are not offers — see the scan arm's identical gate.
    if !::nodeagg::agg_is_done(agg) {
        router::tick(ArmClass::HashJoin, ArmCounter::Offered);
    }

    // m5p1 multibuild: a join TREE (either child of the top join is itself
    // a HashJoin) diverts to the multi-pipeline walk; every other shape
    // keeps the phase-1 single-join path below byte-identically.
    if matches!(&*hj.outer, crate::procnode::PlanStateNode::HashJoin(_))
        || matches!(&*hj.hash.child, crate::procnode::PlanStateNode::HashJoin(_))
    {
        if !hj_multibuild_enabled() {
            refuse("multibuild-disabled");
            return Ok(None);
        }
        return try_own_multibuild(agg, hj, estate, rt, dop, false, false);
    }

    // --- Node shape: HashJoin over two lane-fusible pgrcolumnar SeqScans; a
    // fresh (untouched) join; phase-1 join types; subplan/param-free exprs.
    let crate::procnode::PlanStateNode::SeqScan(outer_ss) = &mut *hj.outer else {
        refuse("outer-not-seqscan");
        return Ok(None);
    };
    let hash = &mut *hj.hash;
    let crate::procnode::PlanStateNode::SeqScan(inner_ss) = &mut *hash.child else {
        refuse("inner-not-seqscan");
        return Ok(None);
    };
    if !shared_join_admissible(&hj.state, &hash.state) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        refuse("join-shape");
        return Ok(None);
    }
    if !::nodehashjoin::lane_join_untouched(&hj.state, &hash.state) {
        refuse("join-touched");
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg)
        && !(hj_aggjoin_numeric_enabled()
            && ::nodeagg::runtime_partial::agg_poly_partial_admissible(agg))
    {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        refuse("partials-not-order-insensitive-exact");
        return Ok(None);
    }
    // K2 inc-1 (wave-8 WS-AC): the pgrcolumnar-only admission, widened onto
    // the BatchGranuleSource seam — a heap SeqScan admits IFF BOTH
    // PGRUST_LANE_V2_HEAPFEED and PGRUST_LANE_V2_K2_PROBE are on (cached
    // bools; K2_PROBE default ON since the SE9-GATES K2 flip, `=0`/`off`
    // = permanent kill; HEAPFEED still default OFF, so the bare-default
    // world short-circuits to false and refuses every heap shape exactly
    // where it always did).
    let k2_heap = k2_probe_enabled() && heapfeed_v2_enabled();
    if !seq_scan_fusible(outer_ss, estate)?
        || !(::nodeseqscan::seq_scan_is_pgrcolumnar(outer_ss)
            || (k2_heap && ::nodeseqscan::seq_scan_is_heap(outer_ss)))
    {
        refuse("outer-scan-not-fusible");
        return Ok(None);
    }
    if !seq_scan_fusible(inner_ss, estate)?
        || !(::nodeseqscan::seq_scan_is_pgrcolumnar(inner_ss)
            || (k2_heap && ::nodeseqscan::seq_scan_is_heap(inner_ss)))
    {
        refuse("inner-scan-not-fusible");
        return Ok(None);
    }
    // K2 heap-fed engagement marker (false = the pgrcolumnar arm verbatim).
    let heap_fed = ::nodeseqscan::seq_scan_is_heap(outer_ss)
        || ::nodeseqscan::seq_scan_is_heap(inner_ss);
    if heap_fed && !k2_heap_jointype_admits(hj.state.plan.join.jointype) {
        // Envelope (hard): INNER/LEFT/SEMI/ANTI only on the heap feed; the
        // right-fill family + RIGHT_SEMI ride the runtime/pgrcolumnar arm
        // unchanged, so a heap-fed shape there falls to the serial arms.
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        refuse("k2-heap-jointype");
        return Ok(None);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refuse("instrumented-or-epq");
        return Ok(None);
    }
    if super::runtime_in_parallel_role() {
        refuse("in-parallel-mode");
        return Ok(None);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse("params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refuse("no-plannedstmt");
        return Ok(None);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse("params");
        return Ok(None);
    }
    // Agg must be the plan root; its child the HashJoin; the join's children
    // the two scans (the worker pstmt transfers the whole root subtree).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else { return Ok(None) };
    if !std::ptr::eq(root_agg, agg.plan) {
        return Ok(None);
    }
    let Some(join_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if join_node.node_tag() != NodeTag::T_HashJoin {
        return Ok(None);
    }
    let join_plan = join_node.as_hash_join().expect("HashJoin tag");
    let Some(outer_plan) = join_plan.join.plan.lefttree else { return Ok(None) };
    let Some(hash_plan_node) = join_plan.join.plan.righttree else { return Ok(None) };
    if outer_plan.node_tag() != NodeTag::T_SeqScan
        || hash_plan_node.node_tag() != NodeTag::T_Hash
    {
        return Ok(None);
    }
    let hash_plan = hash_plan_node.as_hash().expect("Hash tag");
    let Some(inner_plan) = hash_plan.plan.lefttree else { return Ok(None) };
    if inner_plan.node_tag() != NodeTag::T_SeqScan {
        return Ok(None);
    }
    // Parallel-safety walk over everything that runs on helpers.
    let outer_scan_plan = outer_plan.as_seq_scan().expect("SeqScan tag");
    let inner_scan_plan = inner_plan.as_seq_scan().expect("SeqScan tag");
    if !exprs_parallel_safe(outer_scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(outer_scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(inner_scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(inner_scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(join_plan.hashclauses.iter())?
        || !exprs_parallel_safe(join_plan.join.joinqual.iter())?
        || !exprs_parallel_safe(join_plan.join.plan.qual.iter())?
        || !exprs_parallel_safe(join_plan.join.plan.targetlist.iter())?
    {
        refuse("exprs-not-parallel-safe");
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refuse("non-mvcc-snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refuse("binder-policy");
        return Ok(None);
    }

    // --- Envelope sizing (§6): C's combined-budget rule. nbatch > 1 now
    // engages the M3.5 batch arm; the spill kill switch restores the
    // phase-1 refusal exactly.
    let (_, nbatch, _, space_allowed) = ::nodehash::exec_choose_hash_table_size_full(
        hash_plan.plan.plan_rows,
        hash_plan.plan.plan_width,
        false, // useskew: C PHJ parity — no skew in parallel
        true,  // try_combined_hash_mem: pooled participant budget
        dop,
    );
    // SPILL ENVELOPE (train-14 inc-4/5 ledger, open item 2): every live
    // table here — batch 0 and every file leaf — is ONE gang-shared table,
    // so its envelope is the raw combined limit `get_hash_memory_limit() ×
    // (dop+1)`. exec_choose's `space_allowed` is NOT that once nbatch > 1:
    // C recurses to PER-WORKER sizing (its PHJ builds per-worker batch
    // tables) and only partially restores it under the buckets-vs-batches
    // rebalance — the reduced envelope over-fanned splits (leg 6 landed
    // exactly at the 32-leaf cap at jbits=4) and manufactured admission
    // refusals a combined-envelope leaf absorbs. `max` keeps the envelope
    // monotonic vs the old value; at nbatch == 1 `space_allowed` IS the
    // combined limit, so unbatched/dormant engagements are bit-identical.
    let combined_limit =
        ::nodehash::get_hash_memory_limit().saturating_mul(dop.max(0) as usize + 1);
    let envelope = space_allowed.max(combined_limit);
    let mut spill_batches: Option<u32> = hj_spill_force_batches();
    if nbatch > 1 && spill_batches.is_none() {
        if !hj_spill_enabled() {
            lane_trace("runtime-hashjoin: REFUSED (estimated nbatch > 1) — serial arm");
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            // M5-1 refusal funnel: the spill kill switch restores the
            // phase-1 refusal — name the gate for the router taxonomy.
            refuse("estimated-multi-batch");
            return Ok(None);
        }
        // Headroomed sizing (§5.3: round-0 splits should be rare): level-0
        // batch count from the estimated inner footprint over the COMBINED
        // envelope each leaf actually gets — C's nbatch is relative to the
        // reduced per-worker budget and over-fans by ~(dop+1)×.
        let est_inner = ::nodehash::estimate_hash_inner_rel_bytes(
            hash_plan.plan.plan_rows,
            hash_plan.plan.plan_width,
        );
        let need = est_inner.div_ceil(envelope.max(1) as u64).max(1);
        let want = u32::try_from(need)
            .unwrap_or(u32::MAX / 2)
            .next_power_of_two()
            .saturating_mul(2)
            .max(2);
        if want as usize > hj_spill_max_batches() {
            lane_trace(&format!(
                "runtime-hashjoin: REFUSED (estimated batches {want} exceed the spill batch cap) — serial arm"
            ));
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            refuse("spill-batch-cap");
            return Ok(None);
        }
        spill_batches = Some(want);
    }
    // BOUNDARY GUARD (GL-HJMB-1 escalation A): an admission whose nbatch
    // estimate is 1 but whose ARM-representation peak crosses the combined
    // envelope has NO demote path — HjSpill is only constructed for batched
    // admissions, so the crossing REFUSES at seal/in-build into an R5
    // serial rerun measured 5-11x worse than legacy Parallel Hash (the
    // witnessed ladder's two boundary cells). exec_choose's nbatch prices
    // C's representation (~40B/tuple at narrow widths); the arm's chunked
    // arena + refs + combine buckets run ~55-65B/tuple, so a band of
    // builds sits estimate-unbatched but truly crossing. Engage BATCHED
    // (2 level-0 batches) instead: the spill machinery owns the build from
    // the first row and the crossing lands on the ordinary demote path —
    // demote-safe by construction. Squeakers (true fit inside the band)
    // pay the 2-batch tax instead of keeping the cliff risk. The kill
    // switch (spill disarmed) keeps the phase-1 posture exactly; heap
    // feeds hit the AC2 refusal below, as any batched estimate does.
    if spill_batches.is_none() && hj_spill_enabled() {
        let est_peak = ::nodehash::estimate_runtime_hj_build_peak_bytes(
            hash_plan.plan.plan_rows,
            hash_plan.plan.plan_width,
        );
        if est_peak > envelope as u64 {
            lane_trace(
                "runtime-hashjoin: demote-unsafe envelope band (boundary guard) — engaging batched nbatch=2",
            );
            spill_batches = Some(2);
        }
    }
    // AC2 spill rung (K2 inc-1, the recorded choice — notes/se-wave8-k2.md
    // §3): the heap feed refuses whenever the engagement would be BATCHED
    // (nbatch>1 estimate under the spill arm, or the FORCE_BATCHES test
    // knob), so HjSpill is never constructed for a heap-fed engagement and
    // the batch-0 demotion crossing (seal dump / BatchRouter file appends)
    // is unreachable by construction — no pinned-page byte can ride into a
    // spill file. The statement rides the existing arms byte-for-byte.
    if heap_fed && spill_batches.is_some() {
        lane_trace(
            "runtime-hashjoin: REFUSED (heap feed admits only unbatched engagements) — serial arm",
        );
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        refuse("k2-heap-multibatch");
        return Ok(None);
    }

    // --- Geometry: the probe side pays the gang; the build side may be a
    // small dimension table (any nonzero geometry admits). Per-AM through
    // the storage seam (K2 inc-1): pgrcolumnar publishes its granule prefix
    // sums and rides PgrcolumnarGranuleSource verbatim; heap REUSES K1's
    // block geometry (`SeqScanSource::granule_map` →
    // `GranuleMap::unbounded(nblocks, HEAP_STARTUP_C0)` — the scan arm's
    // admission call, no new geometry policy) wrapped boundary-free,
    // sizer-truncated, never coalesced. The tiny-input floor below applies
    // to the heap arm identically (granule = one block there; floor knob
    // semantics as proven by the SE6-GATES item-4b letters).
    let Some((outer_granules, outer_source)) = k2_task_source(outer_ss, estate)? else {
        refuse("geometry");
        return Ok(None);
    };
    let Some((_inner_granules, inner_source)) = k2_task_source(inner_ss, estate)? else {
        refuse("geometry");
        return Ok(None);
    };
    if outer_granules < min_granules().max(2 * dop as u64) {
        refuse("tiny-input-floor");
        return Ok(None);
    }
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill: PGRUST_RUNTIME_ELASTIC_DOP=0).
    let dop = super::runtime_scan::elastic_dop(dop, outer_granules);
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // Right-fill family (RIGHT/FULL/RIGHT_ANTI) adds the FILL task set(s).
    let fill_inner = matches!(
        join_plan.join.jointype,
        ::types_nodes::JoinType::JOIN_RIGHT
            | ::types_nodes::JoinType::JOIN_FULL
            | ::types_nodes::JoinType::JOIN_RIGHT_ANTI
    );

    // Router counter choke point (M5-1): Engaged = ceremony entered;
    // Completed = the runtime answered; Fallback = R5 serial rerun.
    router::tick(ArmClass::HashJoin, ArmCounter::Engaged);
    let r = engage(
        agg,
        estate,
        rt,
        dop,
        outer_granules,
        outer_source,
        Some(inner_source),
        envelope,
        hash_plan.plan.plan_rows.max(0.0) as u64,
        outer_scan_plan.scan.plan.plan_rows.max(0.0) as u64,
        fill_inner,
        spill_batches,
        None, // chain: the phase-1 single-join arm
        root, // worker root: the Agg IS the plan root here (checked above)
        false,
    )?;
    router::tick(
        ArmClass::HashJoin,
        if r.is_some() { ArmCounter::Completed } else { ArmCounter::Fallback },
    );
    Ok(r)
}

/// SE-DECOROOT (the GL-DECOROOT-1 lane): the decorated-root FILL entry —
/// called from the serial Sort/Limit feeds when their Agg child sits over a
/// HashJoin. `Ok(true)` = the grouped runtime sink engaged and FILLED the
/// leader table (finished, not retrieved) — the caller's own drain paths
/// consume it exactly as they consume a serially built table. `Ok(false)` =
/// not engaged/refused — the caller falls to the serial join build
/// byte-identically (nothing consumed; a mid-engagement fallback is the R5
/// serial-rerun discipline, table reset). Knob-gated OFF by default.
pub(super) fn try_fill_grouped_agg_over_join_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !hj_decoroot_enabled() || estate.es_epq_active {
        return Ok(false);
    }
    let dop = router::arm_dop(ArmClass::HashJoin);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };
    // Grouped shapes only; a filled/done table is the drain phase (the
    // caller's emit paths own it — never re-engage).
    if !::nodeagg::agg_is_hashed(agg)
        || ::nodeagg::agg_hash_table_filled(agg)
        || ::nodeagg::agg_is_done(agg)
    {
        return Ok(false);
    }
    router::tick(ArmClass::HashJoin, ArmCounter::Offered);
    if !hj_groupsink_enabled() {
        router::tick_refused(ArmClass::HashJoin, "groupsink-disabled");
        return Ok(false);
    }
    Ok(try_own_multibuild(agg, hj, estate, rt, dop, /*grouped=*/ true, /*fill_only=*/ true)?
        .is_some())
}

/// m5p1 multibuild admission (the dispatch gate above verified a nested
/// tree and the knob — or, `grouped`, a hashed root over ANY join shape).
/// Strictly the single-join arm's gates, generalized per-node over the
/// tree; every refusal falls through to the serial arms byte-identically
/// (nothing consumed). SE-AGGJOIN (band 87001): `grouped` swaps the
/// plain-agg tail for the grouped sink (per-worker hashed builds + grouped
/// partial export/combine/absorb) and admits single-join trees too.
fn try_own_multibuild<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    grouped: bool,
    // SE-DECOROOT (CAR 1): fill-only engagement — the Agg sits under a
    // whitelisted Sort/Limit decoration; on completion the leader table is
    // filled/finished but NOT retrieved (the decorated consumer drains it).
    fill_only: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    fn refuse(reason: &'static str) {
        router::tick_refused(ArmClass::HashJoin, reason);
    }
    if grouped {
        // SE-NUMJOIN (CAR 2): plan-based admission FIRST (byte-untouched);
        // the knob-gated poly twin admits the numeric-manifest shapes the
        // relocated NumericAgg export carries. SE-CBKEYS: the bytes-key
        // admissions (canonical text/varchar keys) follow, knob-gated —
        // key mode and trans schema compose freely.
        let numeric = hj_aggjoin_numeric_enabled();
        let word_ok = ::nodeagg::agg_grouped_runtime_admissible(agg)
            || (numeric && ::nodeagg::agg_grouped_poly_runtime_admissible(agg));
        let bp = hj_bpchar_keys_enabled();
        let bytes_ok = !word_ok
            && hj_cbkeys_enabled()
            && (::nodeagg::agg_grouped_bytes_runtime_admissible(agg, bp)
                || (numeric && ::nodeagg::agg_grouped_bytes_poly_runtime_admissible(agg, bp)));
        if !word_ok && !bytes_ok {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
            refuse("grouped-agg-not-exportable");
            return Ok(None);
        }
    } else if !agg_runtime_partial_admissible(agg)
        && !(hj_aggjoin_numeric_enabled()
            && ::nodeagg::runtime_partial::agg_poly_partial_admissible(agg))
    {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
        refuse("partials-not-order-insensitive-exact");
        return Ok(None);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refuse("instrumented-or-epq");
        return Ok(None);
    }
    if super::runtime_in_parallel_role() {
        refuse("in-parallel-mode");
        return Ok(None);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refuse("params");
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refuse("no-plannedstmt");
        return Ok(None);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refuse("params");
        return Ok(None);
    }
    // Agg must be the plan root — or (SE-DECOROOT, grouped only) sit one
    // whitelisted `[Limit] -> [Sort]` decoration below it; the resolved Agg
    // NODE seeds the worker pstmt (workers run the Agg subtree, never the
    // decoration).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(worker_root) = decorated_agg_plan_node(root, agg, grouped) else {
        return Ok(None);
    };
    let Some(join_node) = agg.plan.plan.lefttree else { return Ok(None) };
    // Pass A: plan-tree walk (shape, probe-local join types, parallel
    // safety, per-build sizing inputs, preorder topology).
    let mut pinfo = MbPlanInfo {
        jointypes: Vec::new(),
        hash_rows: Vec::new(),
        hash_widths: Vec::new(),
        children: Vec::new(),
        nscans: 0,
        scan_rows: Vec::new(),
        join_rows: Vec::new(),
    };
    match mb_plan_walk(join_node, &mut pinfo)? {
        Some(MbChild::Join(0)) => {}
        _ => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            refuse("multibuild-plan-shape");
            return Ok(None);
        }
    }
    // Ptr-congruence: the executor tree's top join is the plan's top join.
    let top_plan = join_node.as_hash_join().expect("walk verified the tag");
    if !std::ptr::eq(top_plan, hj.state.plan) {
        return Ok(None);
    }
    debug_assert!(
        grouped || pinfo.jointypes.len() >= 2,
        "the dispatch gate saw a nested child"
    );
    // Knob coherence (grouped trees): 2+ build sides ride the multibuild
    // walk's machinery, so its kill switch must also refuse them here (the
    // probe reads BOTH knobs for the grouped class — suppression must never
    // outrun the walk).
    if grouped && pinfo.jointypes.len() >= 2 && !hj_multibuild_enabled() {
        refuse("multibuild-disabled");
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        refuse("non-mvcc-snapshot");
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        refuse("binder-policy");
        return Ok(None);
    }
    // Per-build sizing (§6): every table must fit UNBATCHED (nbatch == 1)
    // under the C combined rule — the spill ladder stays single-join; an
    // over-estimate keeps the serial arms (and, at plan time, Gather).
    let combined_limit =
        ::nodehash::get_hash_memory_limit().saturating_mul(dop.max(0) as usize + 1);
    let mut envelopes = Vec::with_capacity(pinfo.jointypes.len());
    for j in 0..pinfo.jointypes.len() {
        let (_, nbatch, _, space_allowed) = ::nodehash::exec_choose_hash_table_size_full(
            pinfo.hash_rows[j],
            pinfo.hash_widths[j],
            false, // useskew: C PHJ parity
            true,  // try_combined_hash_mem: pooled participant budget
            dop,
        );
        if nbatch > 1 {
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            refuse("multibuild-nbatch");
            return Ok(None);
        }
        envelopes.push(space_allowed.max(combined_limit));
    }
    // Pass B: state-tree walk, preorder-congruent with pass A — per-join
    // admissibility/untouched/type, per-scan fusibility + AM + geometry.
    let k2_heap = k2_probe_enabled() && heapfeed_v2_enabled();
    let mut sinfo = MbStateInfo { sources: Vec::new(), heap_fed: false, njoins: 0 };
    if !shared_join_admissible(&hj.state, &hj.hash.state)
        || !::nodehashjoin::lane_join_untouched(&hj.state, &hj.hash.state)
        || !mb_jointype_admits(hj.state.plan.join.jointype)
    {
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        refuse("multibuild-join-shape");
        return Ok(None);
    }
    sinfo.njoins += 1;
    if mb_state_walk(&mut hj.outer, estate, k2_heap, &mut sinfo)?.is_none() {
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        refuse("multibuild-state-shape");
        return Ok(None);
    }
    {
        let hash: &mut crate::procnode::HashSubNode<'mcx> = &mut hj.hash;
        if mb_state_walk(&mut hash.child, estate, k2_heap, &mut sinfo)?.is_none() {
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            refuse("multibuild-state-shape");
            return Ok(None);
        }
    }
    if sinfo.njoins != pinfo.jointypes.len() || sinfo.sources.len() != pinfo.nscans {
        refuse("multibuild-state-shape");
        return Ok(None);
    }
    // Decompose into pipelines; floor + elastic DOP on the FINAL (root
    // outer-descent) pipeline's scan — the gang-paying side.
    let pipelines = mb_decompose(&pinfo);
    let last_scan = {
        let last = pipelines.last().expect("root pipeline");
        debug_assert!(last.sink.is_none(), "decomposition emits the agg pipeline last");
        last.scan
    };
    let probe_granules = sinfo.sources[last_scan].0;
    if probe_granules < min_granules().max(2 * dop as u64) {
        refuse("tiny-input-floor");
        return Ok(None);
    }
    let dop = super::runtime_scan::elastic_dop(dop, probe_granules);
    if ::nodeagg::agg_is_done(agg) {
        // Grouped done-repulls exit at the dispatch gate; belt only.
        return if grouped { Ok(None) } else { Ok(Some(None)) };
    }
    router::tick(ArmClass::HashJoin, ArmCounter::Engaged);
    let sources: Vec<Arc<dyn runtime::MorselSource>> =
        sinfo.sources.into_iter().map(|(_, s)| s).collect();
    let probe_source = Arc::clone(&sources[last_scan]);
    // SE-MBSEAT per-table economics: the join's OUTER subtree rows must
    // amortize the seat's O(build) construction (GL-HJSEAT-2's gate, per
    // table; the constant is a PROVISIONAL reuse the letter re-measures).
    let seat_ok: Vec<bool> = (0..pinfo.jointypes.len())
        .map(|j| {
            outer_rows_of(&pinfo, j) >= pinfo.hash_rows[j].max(0.0) * SEAT_MIN_PROBE_RATIO
                && pinfo.hash_rows[j] > 0.0
        })
        .collect();
    let init = MbInit {
        pipelines,
        sources,
        envelopes,
        // SE-MBSHARED: per-join planner estimates size the single-pass
        // directories (knob-armed engagements only; unread otherwise).
        build_rows: pinfo.hash_rows.iter().map(|&r| r.max(0.0) as u64).collect(),
        seat_ok,
        jointypes: pinfo.jointypes,
        nscans: pinfo.nscans,
        grouped,
    };
    let r = engage(
        agg,
        estate,
        rt,
        dop,
        probe_granules,
        probe_source,
        None,  // inner_source: single-join only
        0,     // envelope: single-join only (per-join envelopes ride MbInit)
        0,     // inner_rows_est: multibuild sizes per-join from MbInit
        0,     // outer_rows_est: the seat never arms for multibuild
        false, // fill_inner: probe-local types only
        None,  // spill_batches: unbatched by admission
        Some(init),
        worker_root,
        fill_only,
    )?;
    router::tick(
        ArmClass::HashJoin,
        if r.is_some() { ArmCounter::Completed } else { ArmCounter::Fallback },
    );
    Ok(r)
}

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    outer_granules: u64,
    // Task-set morsel sources, built per-AM at admission (K2 inc-1):
    // pgrcolumnar = PgrcolumnarGranuleSource verbatim; heap = the seam's
    // GranuleMap wrapped boundary-free/non-coalescing (`k2_task_source`).
    outer_source: Arc<dyn runtime::MorselSource>,
    // None ⇔ multibuild (per-scan sources ride `chain`).
    inner_source: Option<Arc<dyn runtime::MorselSource>>,
    // Combined gang envelope per live table (admission's `envelope`; equals
    // exec_choose's space_allowed exactly when nbatch == 1).
    envelope: usize,
    // Planner inner-rows estimate — the single-pass directory's up-front size
    // (Phase 1a). 0 for multibuild (each join sizes from its own MbInit).
    inner_rows_est: u64,
    // Planner outer-rows estimate — the GL-HJSEAT-2 seat-economics input.
    // 0 for multibuild (the seat never arms there).
    outer_rows_est: u64,
    fill_inner: bool,
    spill_batches: Option<u32>,
    // m5p1 multibuild descriptor (None = the phase-1 single-join arm).
    chain: Option<MbInit>,
    // The plan NODE seeding the worker pstmt: the Agg itself (== planTree
    // when the Agg is the root; the resolved decorated-chain Agg node under
    // SE-DECOROOT — workers never see the Sort/Limit decoration).
    worker_root: ::types_nodes::Node<'mcx>,
    // SE-DECOROOT: grouped fill-only — absorb + finish the leader table
    // but do NOT retrieve; the decorated serial consumer drains it.
    fill_only: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();
    debug_assert!(
        chain.is_none() || (spill_batches.is_none() && !fill_inner && inner_source.is_none()),
        "multibuild engagements are unbatched, fill-free, chain-sourced"
    );

    // M3.5: the engagement's SpillSet (leader-side creation — fd substrate
    // guaranteed; a creation failure fail-closes to the phase-1 refusal).
    let spill = match spill_batches {
        Some(n) => match ::spillset::SpillSet::create() {
            Ok(set) => Some(Arc::new(HjSpill::new(
                set,
                n,
                envelope,
                dop as u64,
                fill_inner,
            ))),
            Err(_) => {
                lane_trace(
                    "runtime-hashjoin: spill set creation failed — refusing to the serial arm",
                );
                stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
                return Ok(None);
            }
        },
        None => None,
    };

    let pstmt = crate::execparallel::build_worker_pstmt(estate, worker_root)?;

    let leaf_cap = spill.as_ref().map_or(0, |s| s.leaf_cap);
    let payload = Arc::new(RuntimeHjShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path (the M1 SendConst discipline).
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        pins_base: rt.nthreads(),
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        budget_refused: AtomicBool::new(false),
        partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        grouped_partials: (0..runtime::MAX_EXTERNAL_LANES).map(|_| Mutex::new(None)).collect(),
        sink: OnceLock::new(),
        chain: OnceLock::new(),
        // GL-HJSEAT-2: the seat's O(build) construction must be amortized by
        // the probe estimate (see SEAT_MIN_PROBE_RATIO's provenance).
        seat_ok: outer_rows_est as f64 >= inner_rows_est as f64 * SEAT_MIN_PROBE_RATIO
            && inner_rows_est > 0,
        spill,
        leaf_tables: (0..leaf_cap).map(|_| Mutex::new(None)).collect(),
        standing: Mutex::new(None),
    });
    let single = match (chain, inner_source) {
        (Some(mb), _) => {
            // Multibuild: one MbBuildSink per join (frozen-table publisher);
            // the descriptor rides the payload for every task-set body.
            // SE-MBSHARED (knob-armed): each join's table gets a single-pass
            // shared directory sized from ITS OWN estimate against ITS OWN
            // per-table combined envelope; a directory the estimate cannot
            // afford leaves that table on the two-pass build (traced by
            // name — never a refusal on this account).
            let shared1a = hj_mbshared_enabled();
            // SE-MBSEAT: requires the single-pass world (the seat rides the
            // sealed directory); MBSHARED=0 inertly disarms this car.
            let mbseat = shared1a && hj_mbseat_enabled();
            let sinks: Vec<Arc<MbBuildSink>> = (0..mb.jointypes.len())
                .map(|j| {
                    let budget = JoinBudget::new(mb.envelopes[j]);
                    let singlepass = if shared1a {
                        match SharedBuildDir::with_estimate(mb.build_rows[j], &budget) {
                            Ok(dir) => Some(dir),
                            Err(BudgetExceeded) => {
                                lane_trace(&format!(
                                    "runtime-hashjoin: multibuild single-pass directory over budget (join={j}) — two-pass build"
                                ));
                                None
                            }
                        }
                    } else {
                        None
                    };
                    Arc::new(MbBuildSink {
                        join: j,
                        budget,
                        plan: Mutex::new(None),
                        table: Mutex::new(None),
                        shared: Arc::downgrade(&payload),
                        singlepass,
                    })
                })
                .collect();
            if shared1a {
                // Engagement witness (e2e-grepped): fires only with the
                // knob armed (the default since the GL-MULTIBUILD-1 flip);
                // the =0|off kill posture cannot print it.
                let sp = sinks.iter().filter(|s| s.singlepass.is_some()).count();
                lane_trace(&format!(
                    "runtime-hashjoin: multibuild shared build ENGAGED (singlepass={sp}/{})",
                    sinks.len()
                ));
            }
            payload
                .chain
                .set(Arc::new(MbChain {
                    pipelines: mb.pipelines,
                    sinks,
                    sources: mb.sources,
                    jointypes: mb.jointypes,
                    nscans: mb.nscans,
                    grouped: mb.grouped,
                    shared1a,
                    mbseat,
                    seat_ok: mb.seat_ok,
                }))
                .unwrap_or_else(|_| unreachable!("chain set once"));
            None
        }
        (None, Some(inner_source)) => {
            let budget = JoinBudget::new(envelope);
            // SINGLE-PASS (Phase 1a, kill-switch): UNBATCHED single-join arm
            // only. Size the shared directory from the planner's inner-rows
            // estimate and charge it to the join budget; if it will not fit,
            // fall back to the two-pass build (never a refusal on this
            // account). Batched/spill and multibuild stay two-pass this phase.
            // `spill_batches.is_none()` ⇒ unbatched (nbatch <= 1); batched
            // engagements carry Some(want) and stay two-pass this phase.
            let singlepass = if hj_singlepass_enabled() && spill_batches.is_none() {
                match SharedBuildDir::with_estimate(inner_rows_est, &budget) {
                    Ok(dir) => {
                        lane_trace("runtime-hashjoin: single-pass build ENGAGED");
                        Some(dir)
                    }
                    Err(BudgetExceeded) => None,
                }
            } else {
                None
            };
            let sink = Arc::new(JoinBuildSink {
                budget,
                plan: Mutex::new(None),
                table: Mutex::new(None),
                shared: Arc::downgrade(&payload),
                singlepass,
            });
            payload
                .sink
                .set(Arc::clone(&sink))
                .unwrap_or_else(|_| unreachable!("sink set once"));
            Some((sink, inner_source))
        }
        (None, None) => unreachable!("single-join engagements carry an inner source"),
    };

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        agg,
        estate,
        rt,
        dop,
        outer_granules,
        outer_source,
        single,
        &payload,
        fill_inner,
        fill_only,
    );
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed,
}

/// This arm's standing-channel constants (M2 inc-1; see
/// standing_channel::StandingArm — sinks_gate: PGRUST_RUNTIME_POOLBIND_SINKS).
static STANDING_ARM: super::standing_channel::StandingArm =
    super::standing_channel::StandingArm {
        label: "runtime-hashjoin",
        died: "runtime hash-join standing executors exited before completing the join",
        sinks_gate: true,
    };

/// Shared post-outcome tail (standing and launched channels): the §6/R5
/// envelope refusal takes the whole-attempt serial rerun (secondary errors
/// dropped — the abort races in-flight morsels); worker-phase errors
/// rethrow PLAIN; an unexplained abort surfaces the pending interrupt or
/// reports; completed-but-nobody-participated falls back serially.
fn finish_outcome(
    payload: &Arc<RuntimeHjShared>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
    if payload.budget_refused.load(Ordering::SeqCst) {
        let _ = payload.take_error();
        lane_trace("runtime-hashjoin: envelope refusal — falling back to the serial arm");
        stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
        return Ok(EngageOutcome::Fallback);
    }
    if let Some(e) = payload.take_error() {
        return Err(e);
    }
    if outcome == runtime::RgOutcome::Aborted {
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "runtime hash-join pipeline aborted")));
    }
    if payload.started.load(Ordering::SeqCst) == 0 {
        return Ok(EngageOutcome::Fallback);
    }
    Ok(EngageOutcome::Completed)
}

/// The standing-first channel shared by the single-join and multibuild
/// arms (M2 inc-1): both submit their RG and try the board channels; a
/// decline returns None with the RG untouched and the caller falls
/// through to the serial arm (rung 4: no launched path).
#[allow(clippy::too_many_arguments)]
fn standing_first(
    payload: &Arc<RuntimeHjShared>,
    rt: &'static Arc<runtime::Runtime>,
    pool: Option<Arc<parallel::standing::StandingEngagement>>,
    dop: i32,
    outer_granules: u64,
    census: &str,
    rg: &runtime::RgHandle,
    waiter: &runtime::CompletionWaiter,
) -> PgResult<Option<EngageOutcome>> {
    match super::standing_channel::standing_wait(
        &STANDING_ARM,
        super::standing_channel::StandingLeader {
                // M2 inc-2: the pool-db board attached at submit (None =
                // gang-first, inc-1 exactly).
                pool,
            shared: payload.pcxt_shared.get().expect("pcxt shared set above"),
            slot: &payload.standing,
            started: &payload.started,
            refused: &payload.refused,
            take_error: &|| payload.take_error(),
            drain: &|rg| drain_rg(rt, rg),
            census,
        },
        dop,
        outer_granules,
        rg,
        waiter,
    )? {
        super::standing_channel::StandingWait::Done(outcome) => {
            finish_outcome(payload, outcome).map(Some)
        }
        super::standing_channel::StandingWait::Fallback => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    outer_granules: u64,
    outer_source: Arc<dyn runtime::MorselSource>,
    // Single-join build pair (None ⇔ the payload carries a multibuild chain).
    mut single: Option<(Arc<JoinBuildSink>, Arc<dyn runtime::MorselSource>)>,
    payload: &Arc<RuntimeHjShared>,
    fill_inner: bool,
    // SE-DECOROOT: grouped fill-only (see `engage`).
    fill_only: bool,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_hashjoin_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        // Standing driver dispatch (M2 inc-1): deferred_bind false — this
        // arm binds EAGERLY (with_query_task_binding); the standing serve
        // re-establishes visibility up front and evicts parked sticky.
        parallel::set_standing_driver(pcxt, parallel::standing::StandingDriver {
            drive: runtime_hj_standing_driver,
            deferred_bind: false,
        });

        // m5p1 multibuild ladder: per pipeline in emission order — build
        // pipelines as ACCEPT/COMBINE sink pairs (accept deps = the
        // COMBINEs of every table the pipeline probes on its way up), the
        // final agg pipeline as the probe task set (emitted last by the
        // decomposition). The single-join arm's construction is the `else`
        // arm below, byte-identical.
        if let Some(mb) = payload.chain.get() {
            let mut tasksets: Vec<runtime::TaskSetSpec> =
                Vec::with_capacity(2 * mb.sinks.len() + 1);
            let mut combine_idx = vec![usize::MAX; mb.sinks.len()];
            for p in &mb.pipelines {
                let deps: Vec<usize> = p.probes.iter().map(|&j| combine_idx[j]).collect();
                debug_assert!(
                    deps.iter().all(|&d| d != usize::MAX),
                    "emission order builds every probed table first"
                );
                match p.sink {
                    Some(j) => {
                        let accept_idx = tasksets.len();
                        let runtime::SinkTaskSets { mut accept, combine, probe: _p } =
                            runtime::sink_tasksets(
                                Arc::clone(&mb.sinks[j]),
                                Arc::clone(&mb.sources[p.scan]),
                                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                                accept_idx,
                            );
                        accept.deps = deps;
                        tasksets.push(accept);
                        combine_idx[j] = tasksets.len();
                        tasksets.push(combine);
                    }
                    None => {
                        tasksets.push(runtime::TaskSetSpec {
                            source: Arc::clone(&outer_source),
                            work: Arc::clone(payload) as Arc<dyn runtime::TaskSetWork>,
                            deps,
                        });
                    }
                }
            }
            static NEXT_MB_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
            // M2 inc-2: the POOL-DB channel — built BEFORE submit (the
            // bound descriptor must ride the submission); sinks_gate.
            let pool = super::standing_channel::try_pool_channel(
                payload.pcxt_shared.get().expect("pcxt shared set above"),
                dop,
                /* sinks_gate */ true,
            );
            let spec = runtime::QuerySpec {
                query_id: NEXT_MB_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
                tasksets,
            };
            // rg-set-BEFORE-publish (M2 inc-3 rung 3): payload.rg is stored
            // by on_rg before the bound submission can become pool-visible
            // — no "rg gone" refusal churn window.
            let set_rg = |rg: &runtime::RgHandle| {
                payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
            };
            let (rg, waiter) = match &pool {
                Some((_, descriptor)) => rt.submit_pinned_bound(
                    spec,
                    router::session_affinity_token(),
                    descriptor.clone(),
                    set_rg,
                ),
                None => {
                    let (rg, waiter) =
                        rt.submit_pinned_with_affinity(spec, router::session_affinity_token());
                    set_rg(&rg);
                    (rg, waiter)
                }
            };
            *mut_submitted = Some(rg.clone());

            // M2 inc-1: STANDING engagement first; fallback leaves the RG
            // untouched for the launched path below.
            let census = format!(
                "builds={} (multibuild{})",
                mb.sinks.len(),
                if mb.grouped { " grouped" } else { "" }
            );
            if let Some(outcome) = standing_first(
                payload,
                rt,
                pool.as_ref().map(|(entry, _)| Arc::clone(entry)),
                dop,
                outer_granules,
                &census,
                &rg,
                &waiter,
            )? {
                return Ok(outcome);
            }

            // M2 inc-3 rung 4: the launched-bgworker fallback is DELETED —
            // a board decline goes straight to the serial arm (pool →
            // gang → serial; the NOLAUNCH posture made permanent). Cause
            // attribution ticks the nolaunch-serial floor row inside the
            // shared helper.
            super::standing_channel::launched_fallback_retired(&STANDING_ARM);
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        // Task sets [0]/[1]: the batch-0 build sink pair. The BUILD-ACCEPT
        // source arrives per-AM from admission (K2 inc-1, `k2_task_source`):
        // pgrcolumnar = PgrcolumnarGranuleSource{coalesce:false} verbatim
        // (straight set_granule_range feed, single-epoch contract, never
        // coalesce); heap = the seam's boundary-free GranuleMap, same
        // never-coalesce posture.
        let (sink, inner_source) = single.take().expect("single-join ceremony");
        let runtime::SinkTaskSets { accept, combine, probe: _sink_probe } =
            runtime::sink_tasksets(
                sink,
                inner_source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        let mut tasksets = vec![accept, combine];
        // M3.5 ladder: PLAN-BATCHES + split rounds precede PROBE(0).
        let mut probe0_deps = vec![1usize];
        if let Some(sp) = payload.spill.as_ref() {
            let plan_idx = tasksets.len();
            tasksets.push(runtime::TaskSetSpec {
                source: Arc::new(OneGranuleSource),
                work: Arc::new(PlanBatchesWork(Arc::clone(payload))),
                deps: vec![1],
            });
            let mut prev = plan_idx;
            for r in 0..sp.rounds_max {
                let idx = tasksets.len();
                tasksets.push(runtime::TaskSetSpec {
                    source: Arc::clone(&sp.round_sources[r]) as Arc<dyn runtime::MorselSource>,
                    work: Arc::new(SplitRoundWork { payload: Arc::clone(payload), round: r }),
                    deps: vec![prev],
                });
                prev = idx;
            }
            probe0_deps = vec![prev];
        }
        let probe0_idx = tasksets.len();
        tasksets.push(runtime::TaskSetSpec {
            // PROBE source: per-AM from admission, as above.
            source: outer_source,
            work: Arc::clone(payload) as Arc<dyn runtime::TaskSetWork>,
            deps: probe0_deps,
        });
        let mut tail = probe0_idx;
        if fill_inner {
            // Right-fill family: the unmatched-build walk, after the probe
            // barrier (the match-flag visibility edge).
            let idx = tasksets.len();
            tasksets.push(runtime::TaskSetSpec {
                source: Arc::new(FillPartitionSource),
                work: Arc::new(FillWork { payload: Arc::clone(payload), leaf: None }),
                deps: vec![tail],
            });
            tail = idx;
        }
        // M3.5 leaf ladders: chained ACCEPT(i)/COMBINE(i)/PROBE(i)/FILL(i)
        // — one live table at a time (C parity); unused slots run empty.
        if let Some(sp) = payload.spill.as_ref() {
            for leaf in 0..sp.leaf_cap {
                let leaf_sink = Arc::new(LeafBatchSink {
                    shared: Arc::downgrade(payload),
                    leaf,
                    budget: JoinBudget::new(sp.space_allowed),
                    plan: Mutex::new(None),
                });
                let accept_idx = tasksets.len();
                let runtime::SinkTaskSets { mut accept, combine, probe: _p } =
                    runtime::sink_tasksets(
                        leaf_sink,
                        Arc::clone(&sp.leaf_in_sources[leaf]) as Arc<dyn runtime::MorselSource>,
                        rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                        accept_idx,
                    );
                accept.deps = vec![tail];
                tasksets.push(accept);
                let combine_idx = tasksets.len();
                tasksets.push(combine);
                let probe_idx = tasksets.len();
                tasksets.push(runtime::TaskSetSpec {
                    source: Arc::clone(&sp.leaf_out_sources[leaf])
                        as Arc<dyn runtime::MorselSource>,
                    work: Arc::new(LeafProbeWork { payload: Arc::clone(payload), leaf }),
                    deps: vec![combine_idx],
                });
                tail = probe_idx;
                if fill_inner {
                    let idx = tasksets.len();
                    tasksets.push(runtime::TaskSetSpec {
                        source: Arc::new(FillPartitionSource),
                        work: Arc::new(FillWork {
                            payload: Arc::clone(payload),
                            leaf: Some(leaf),
                        }),
                        deps: vec![tail],
                    });
                    tail = idx;
                }
            }
        }
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        // M2 inc-2: the POOL-DB channel — built BEFORE submit (the bound
        // descriptor must ride the submission); sinks_gate.
        let pool = super::standing_channel::try_pool_channel(
            payload.pcxt_shared.get().expect("pcxt shared set above"),
            dop,
            /* sinks_gate */ true,
        );
        let spec = runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets,
        };
        // rg-set-BEFORE-publish (M2 inc-3 rung 3): payload.rg is stored by
        // on_rg before the bound submission can become pool-visible — no
        // "rg gone" refusal churn window.
        let set_rg = |rg: &runtime::RgHandle| {
            payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
        };
        let (rg, waiter) = match &pool {
            Some((_, descriptor)) => rt.submit_pinned_bound(
                spec,
                router::session_affinity_token(),
                descriptor.clone(),
                set_rg,
            ),
            None => {
                let (rg, waiter) =
                    rt.submit_pinned_with_affinity(spec, router::session_affinity_token());
                set_rg(&rg);
                (rg, waiter)
            }
        };
        *mut_submitted = Some(rg.clone());

        // M2 inc-1: STANDING engagement first; fallback leaves the RG
        // untouched for the launched path below. The census carries the
        // launched line's batched witness ("nbatch=") for the spill legs.
        let census = match payload.spill.as_ref() {
            Some(sp) => format!("nbatch={} (spill)", sp.nbatch),
            None => String::new(),
        };
        if let Some(outcome) = standing_first(
            payload,
            rt,
            pool.as_ref().map(|(entry, _)| Arc::clone(entry)),
            dop,
            outer_granules,
            &census,
            &rg,
            &waiter,
        )? {
            return Ok(outcome);
        }

        // M2 inc-3 rung 4: the launched-bgworker fallback is DELETED — a
        // board decline goes straight to the serial arm (pool → gang →
        // serial; the NOLAUNCH posture made permanent). Cause attribution
        // ticks the nolaunch-serial floor row inside the shared helper.
        super::standing_channel::launched_fallback_retired(&STANDING_ARM);
        drain_rg(rt, &rg);
        Ok(EngageOutcome::Fallback)
    })(&mut submitted);

    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    match outcome {
        EngageOutcome::Fallback => {
            stats::tick_engaged(STANDING_ARM.label, stats::EngageChannel::Serial);
            lane_trace("runtime-hashjoin: fallback to serial arm");
            Ok(None)
        }
        EngageOutcome::Completed => {
            // SE-AGGJOIN grouped adopt: combine the workers' grouped
            // partials and absorb them into the leader's OWN hash table;
            // the canonical retrieve (finalize + HAVING + projection, C's
            // iteration) then emits — first row here, the rest through the
            // serial emit paths (the dispatch's filled-table gate).
            if payload.chain.get().is_some_and(|c| c.grouped) {
                let parts: Vec<GroupedRuntimePartial> = payload
                    .grouped_partials
                    .iter()
                    .filter_map(|m| lockm(m).take())
                    .collect();
                let combined = agg_grouped_runtime_combine(agg, &parts)?;
                if !::nodeagg::exec_agg_grouped_runtime_partials(agg, estate, &combined)? {
                    lane_trace(
                        "runtime-hashjoin: grouped absorb refused — falling back to the serial arm",
                    );
                    stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ParallelGate);
                    return Ok(None);
                }
                stats::tick_owned(ShapeClass::Join);
                lane_trace(&format!(
                    "runtime-hashjoin: complete, grouped partials={} groups={}",
                    parts.len(),
                    combined.len()
                ));
                if fill_only {
                    // SE-DECOROOT: the table is filled + finished; the
                    // decorated serial consumer (Sort/Limit feed) drains it
                    // through the ordinary emit paths. The wrapper maps
                    // Some(_) to "filled"; no row is consumed here.
                    lane_trace("runtime-hashjoin: fill-only complete (decorated root)");
                    return Ok(Some(None));
                }
                return Ok(Some(::nodeagg::agg_hash_retrieve(agg, estate)?));
            }
            let parts: Vec<RuntimePartial> = payload
                .partials
                .iter()
                .filter_map(|m| lockm(m).take())
                .collect();
            let combined = agg_runtime_combine(agg, &parts)?;
            stats::tick_owned(ShapeClass::Join);
            if let Some(sp) = payload.spill.as_ref() {
                // The R4 spill channel: live counters at adopt.
                let (inner, outer, split) = sp.spilled_census();
                lane_trace(&format!(
                    "runtime-hashjoin: SPILLED batches={} leaves={} inner_bytes={inner} outer_bytes={outer} split_bytes={split} splits={} max_round={}",
                    sp.nbatch,
                    sp.leaves_used.load(Ordering::SeqCst),
                    sp.splits.load(Ordering::Relaxed),
                    sp.max_round.load(Ordering::Relaxed),
                ));
            }
            lane_trace(&format!("runtime-hashjoin: complete, partials={}", parts.len()));
            Ok(Some(exec_agg_runtime_partials(agg, estate, &combined)?))
        }
    }
}

/// Abort + BOUNDED drain of a pinned RG nobody will drive (the M1 drain
/// discipline: cleanup driving, not leader work execution).
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-hashjoin: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-hashjoin: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}

// ---------------------------------------------------------------------------
// m5p1 multibuild unit corpus (band 88001).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod mb_tests {
    use super::*;

    /// SE-CARS executor knobs (the GL-DECOROOT-1/GL-NUMJOIN-1 lane): both DEFAULT OFF —
    /// the test binary carries no env, so the live getters resolve OFF
    /// (the decorated-root fill entry and the poly-manifest join admission
    /// are unreachable at default; every pre-existing path byte-identical).
    /// Same spelling as the planner probe (knob-coherence law).
    #[test]
    fn conversion_car_executor_knob_defaults() {
        // conversion-flips: DECOROOT is DEFAULT ON (GL-DECOROOT-1; =0|off kills).
        assert!(hj_decoroot_enabled(), "conversion-flips: unset => ON (GL-DECOROOT-1)");
        assert!(hj_aggjoin_numeric_enabled(), "conversion-flips: unset => ON (GL-NUMJOIN-1)");
        assert!(hj_cbkeys_enabled(), "conversion-flips: unset => ON (GL-CBKEYS-1)");
        assert!(hj_bpchar_keys_enabled(), "conversion-flips: unset => ON (GL-BPCHAR-1)");
    }

    /// Decomposition invariants on a SNOWFLAKE topology
    /// `J0(outer=J1(outer=S0, build=S1), build=J2(outer=S2, build=S3))`
    /// (preorder: joins [J0, J1, J2], scans [S0..S3]): exact emission
    /// order, probes bottom-up, the agg pipeline last, and the deps-safety
    /// induction (every probed table's builder precedes its prober).
    #[test]
    fn mb_decompose_snowflake_deps_safe() {
        let info = MbPlanInfo {
            jointypes: vec![::types_nodes::JoinType::JOIN_INNER; 3],
            hash_rows: vec![0.0; 3],
            hash_widths: vec![0; 3],
            children: vec![
                (MbChild::Join(1), MbChild::Join(2)), // J0
                (MbChild::Scan(0), MbChild::Scan(1)), // J1
                (MbChild::Scan(2), MbChild::Scan(3)), // J2
            ],
            nscans: 4,
            scan_rows: vec![0.0; 4],
            join_rows: vec![0.0; 3],
        };
        let ps = mb_decompose(&info);
        assert_eq!(ps.len(), 4);
        assert_eq!((ps[0].scan, &ps[0].probes[..], ps[0].sink), (3, &[][..], Some(2)));
        assert_eq!((ps[1].scan, &ps[1].probes[..], ps[1].sink), (2, &[2usize][..], Some(0)));
        assert_eq!((ps[2].scan, &ps[2].probes[..], ps[2].sink), (1, &[][..], Some(1)));
        assert_eq!((ps[3].scan, &ps[3].probes[..], ps[3].sink), (0, &[1usize, 0][..], None));
        let mut built = std::collections::BTreeSet::new();
        for p in &ps {
            for j in &p.probes {
                assert!(built.contains(j), "probe of an unbuilt table");
            }
            if let Some(j) = p.sink {
                built.insert(j);
            }
        }
        assert!(ps.last().unwrap().sink.is_none(), "agg pipeline emitted last");
    }

    /// The multibuild kill switch A/B (OnceLock — one state per process, so
    /// only the default is asserted here; the =0 posture is the e2e's leg D).
    #[test]
    fn mb_knob_default_on() {
        assert!(hj_multibuild_enabled());
    }

    /// SE-AGGJOIN (band 87001): grouped-sink knob default + group-cap
    /// default (the =0 / cap postures are e2e restart legs).
    #[test]
    fn mbg_knob_default_on() {
        assert!(hj_groupsink_enabled());
        assert_eq!(mbg_max_groups(), 131_072);
    }

    /// SE-MBSHARED: the shared-probe/shared-build car is DEFAULT ON
    /// (flipped-kill since GL-MULTIBUILD-1) — unset must resolve ON (the
    /// =0|off posture is the e2e's kill boot; OnceLock, one state per
    /// process).
    #[test]
    fn mbshared_knob_default_on() {
        assert!(hj_mbshared_enabled());
    }

    /// SE-MBSEAT: the multibuild seat car is DEFAULT ON (flipped-kill
    /// since GL-MBSEAT-1) — unset must resolve ON (the =0|off posture is
    /// the e2e's kill boot; OnceLock, one state per process). Compose
    /// law: the car additionally requires the MBSHARED world at engage
    /// (`mbseat = shared1a && knob`).
    #[test]
    fn mbseat_knob_default_on() {
        assert!(hj_mbseat_enabled());
    }

    /// SE-AGGJOIN: the SINGLE-join tree decomposes to exactly one build
    /// pipeline + the agg pipeline (the grouped arm admits 1-join trees the
    /// plain dispatch never sends here).
    #[test]
    fn mb_decompose_single_join() {
        let info = MbPlanInfo {
            jointypes: vec![::types_nodes::JoinType::JOIN_INNER],
            hash_rows: vec![0.0],
            hash_widths: vec![0],
            children: vec![(MbChild::Scan(0), MbChild::Scan(1))],
            nscans: 2,
            scan_rows: vec![0.0; 2],
            join_rows: vec![0.0],
        };
        let ps = mb_decompose(&info);
        assert_eq!(ps.len(), 2);
        assert_eq!((ps[0].scan, &ps[0].probes[..], ps[0].sink), (1, &[][..], Some(0)));
        assert_eq!((ps[1].scan, &ps[1].probes[..], ps[1].sink), (0, &[0usize][..], None));
    }
}

// ---------------------------------------------------------------------------
// K2 inc-1 unit corpus (wave-8 WS-AC).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod k2_tests {
    use super::*;
    use ::types_nodes::JoinType;

    /// `PGRUST_LANE_V2_K2_PROBE` A/B lever (AtomicU8 idiom): both states
    /// resolvable in one process; restored to OFF (the state the rest of
    /// the suite assumes — heap admission is dual-gated on HEAPFEED, whose
    /// default stays OFF, so either restore preserves the suite's refusal
    /// stream; OFF keeps the pre-flip memo the suites were written under).
    #[test]
    fn k2_probe_knob_ab() {
        k2_probe_set_for_tests(true);
        assert!(k2_probe_enabled());
        k2_probe_set_for_tests(false);
        assert!(!k2_probe_enabled());
    }

    /// The K2 join-type envelope is EXACTLY the four fill-free probe-side
    /// types — the right-fill family (RIGHT/FULL/RIGHT_ANTI), RIGHT_SEMI
    /// and the planner-internal UNIQUE variants all refuse (they ride the
    /// runtime/pgrcolumnar arm or the serial arms unchanged).
    #[test]
    fn k2_jointype_envelope_exact() {
        let admitted = [
            JoinType::JOIN_INNER,
            JoinType::JOIN_LEFT,
            JoinType::JOIN_SEMI,
            JoinType::JOIN_ANTI,
        ];
        let refused = [
            JoinType::JOIN_FULL,
            JoinType::JOIN_RIGHT,
            JoinType::JOIN_RIGHT_SEMI,
            JoinType::JOIN_RIGHT_ANTI,
            JoinType::JOIN_UNIQUE_OUTER,
            JoinType::JOIN_UNIQUE_INNER,
        ];
        for jt in admitted {
            assert!(k2_heap_jointype_admits(jt), "{jt:?} must admit");
        }
        for jt in refused {
            assert!(!k2_heap_jointype_admits(jt), "{jt:?} must refuse");
        }
    }
}
