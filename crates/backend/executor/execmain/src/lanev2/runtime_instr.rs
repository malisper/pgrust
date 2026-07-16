//! EA-ON-MORSELS instrumentation partials (docs/design/ea-morsels.md §2).
//!
//! One `InstrumentPartial` per (worker, task set), living exactly where the
//! arm's RESULT partials live and obeying the same three disciplines: no
//! shared counters on any hot path (each worker owns its partial; tallies
//! are claim/window-grain); export on the worker's own claim boundary
//! (cumulative overwrite — decision-6, so the final export precedes the
//! settle); generation keying inherited from the channel it rides (an
//! aborted attempt's partials are never merged — the arms read them only on
//! a clean Completed outcome, the runtime_partial.rs invariant).
//!
//! EXACTNESS CONTRACT (runtime-dop1-tax coordination, ea-morsels.md §2):
//! these counters are EXACT, never sampled — they accumulate in the
//! worker's local state and export at claim end, which stays exact under
//! the dop1-tax delta-export scheme by construction. Sampled metering
//! (their fix-5) applies to WFIN/stderr observability only.
//!
//! Dead-when-off: nothing in this module runs unless the engagement was
//! admitted WITH instrumentation (`es_instrument != 0`); the fold drain's
//! tallies are compiled out of the serial instantiation entirely (const-
//! generic `EA=false` — the off-path audit's strongest form).

use ::executils::EStateData;
use ::types_core::instrument::INSTRUMENT_ROWS;

/// Row-funnel tally the scan drains fill (per window, never per row on the
/// bitmap paths; per surviving row inside per-row fallback loops, where a
/// slot emission already happened).
#[derive(Clone, Copy, Default, Debug)]
pub(super) struct EaRowTally {
    /// Rows staged by the scan (pre-qual): C's SeqScan `actual rows` +
    /// `Rows Removed by Filter` sum.
    pub scanned: u64,
    /// Qual survivors handed to the consumer: C's SeqScan `actual rows`.
    pub survived: u64,
}

impl EaRowTally {
    #[inline]
    pub(super) fn add(&mut self, o: &EaRowTally) {
        self.scanned += o.scanned;
        self.survived += o.survived;
    }
}

/// Fold of the worker's pgrcolumnar scan-descriptor counters (the CBSCAN debug
/// line's fields — scan.rs:366-382 at the base): snapshotted whole at claim
/// end (the descriptor is per-worker and cumulative, so the LAST snapshot
/// is the worker's total — the same overwrite discipline as the partial).
/// Order: [granules_scanned, granules_pruned, granules_bloom_pruned,
/// granules_meta, granules_bound_skipped, blocks_pruned, windows_staged].
pub(super) type PruneFold = [u64; 7];

/// Per-(worker, task set) instrumentation partial. Cumulative across the
/// worker's claims; exported by OVERWRITE into the worker's slot after
/// every claim (scan arm) or carried inside the sink Local and read at
/// SEAL (sink arms). All fields are sums/last-snapshots — merging is
/// commutative and order-insensitive-exact, which is what makes this legal
/// on the result channel.
#[derive(Clone, Copy, Default, Debug)]
pub(super) struct InstrumentPartial {
    /// Morsel claims this worker executed in the task set.
    pub claims: u64,
    /// Granules covered by those claims.
    pub granules: u64,
    /// Row funnel (scan stages).
    pub rows: EaRowTally,
    /// Scan-descriptor counter snapshot (absolute worker totals).
    pub prune: PruneFold,
    // --- TIMING ON (inc-3) — zero and never written under TIMING OFF. ---
    /// Sum of per-claim wall ns.
    pub busy_ns: u64,
    /// Engagement-epoch ns of the worker's first claim start (0 = unset;
    /// real values clamped >= 1). One shared epoch per engagement makes
    /// these comparable across workers.
    pub first_start_ns: u64,
    /// Engagement-epoch ns of the worker's last claim end.
    pub last_end_ns: u64,
    /// Min/max per-claim wall ns (0 = unset; real minima clamped >= 1).
    pub morsel_min_ns: u64,
    pub morsel_max_ns: u64,
}

impl InstrumentPartial {
    pub(super) fn is_empty(&self) -> bool {
        self.claims == 0
    }
}

/// Task-set-level merge of every participating worker's partial (last-
/// worker-out-equivalent: the arms call this after completion/SEAL, when
/// every final export is installed).
#[derive(Clone, Copy, Default, Debug)]
pub(super) struct InstrumentMerged {
    /// Workers that executed at least one claim (participation).
    pub workers: u32,
    pub claims: u64,
    pub granules: u64,
    pub rows: EaRowTally,
    pub prune: PruneFold,
    pub busy_ns: u64,
    /// min over workers' first_start_ns (0 when timing off).
    pub first_start_ns: u64,
    /// max over workers' last_end_ns (0 when timing off).
    pub last_end_ns: u64,
    /// min over workers' last_end_ns (0 when timing off) — the WFIN finish
    /// spread is `last_end_ns - min_last_end_ns`.
    pub min_last_end_ns: u64,
    /// min/max over per-claim wall ns (0 when timing off).
    pub morsel_min_ns: u64,
    pub morsel_max_ns: u64,
}

pub(super) fn merge<'a>(
    parts: impl Iterator<Item = &'a InstrumentPartial>,
) -> InstrumentMerged {
    let mut m = InstrumentMerged::default();
    for p in parts {
        if p.is_empty() {
            continue;
        }
        m.workers += 1;
        m.claims += p.claims;
        m.granules += p.granules;
        m.rows.add(&p.rows);
        for (a, b) in m.prune.iter_mut().zip(p.prune.iter()) {
            *a += b;
        }
        m.busy_ns += p.busy_ns;
        if p.first_start_ns != 0
            && (m.first_start_ns == 0 || p.first_start_ns < m.first_start_ns)
        {
            m.first_start_ns = p.first_start_ns;
        }
        m.last_end_ns = m.last_end_ns.max(p.last_end_ns);
        if p.last_end_ns != 0
            && (m.min_last_end_ns == 0 || p.last_end_ns < m.min_last_end_ns)
        {
            m.min_last_end_ns = p.last_end_ns;
        }
        if p.morsel_min_ns != 0
            && (m.morsel_min_ns == 0 || p.morsel_min_ns < m.morsel_min_ns)
        {
            m.morsel_min_ns = p.morsel_min_ns;
        }
        m.morsel_max_ns = m.morsel_max_ns.max(p.morsel_max_ns);
    }
    m
}

/// Fold one claim's clock pair (engagement-epoch ns) into the worker's
/// partial. TIMER mode only — the ONLY cost TIMING ON adds is the two
/// monotonic reads that produced t0/t1 (one pair per CLAIM, never per
/// row/window — ea-morsels.md §5).
pub(super) fn ea_claim_time(ip: &mut InstrumentPartial, t0: u64, t1: u64) {
    let dt = t1.saturating_sub(t0);
    ip.busy_ns += dt;
    if ip.first_start_ns == 0 {
        ip.first_start_ns = t0.max(1);
    }
    ip.last_end_ns = t1.max(1);
    let dt1 = dt.max(1);
    if ip.morsel_min_ns == 0 || dt1 < ip.morsel_min_ns {
        ip.morsel_min_ns = dt1;
    }
    ip.morsel_max_ns = ip.morsel_max_ns.max(dt1);
}

/// Instrumented at all? (EXPLAIN ANALYZE in any mode.)
#[inline]
pub(super) fn ea_active(estate: &EStateData<'_>) -> bool {
    estate.es_instrument != 0
}

/// Admissible instrument modes (ea-morsels.md §5): EXPLAIN (ANALYZE,
/// TIMING OFF) = INSTRUMENT_ROWS exactly (inc-1), and EXPLAIN (ANALYZE,
/// BUFFERS OFF) = INSTRUMENT_TIMER exactly (inc-3 — note PG18 defaults
/// BUFFERS ON, so bare EXPLAIN ANALYZE is TIMER|BUFFERS and still
/// refuses). Any BUFFERS/WAL combination refuses until buffer accounting
/// is threaded.
#[inline]
pub(super) fn ea_mode_admissible(estate: &EStateData<'_>) -> bool {
    estate.es_instrument == INSTRUMENT_ROWS
        || estate.es_instrument == ::types_core::instrument::INSTRUMENT_TIMER
}

/// TIMER mode: the engagement takes one clock pair per claim.
#[inline]
pub(super) fn ea_timer(estate: &EStateData<'_>) -> bool {
    estate.es_instrument == ::types_core::instrument::INSTRUMENT_TIMER
}

/// The refusal reason for an instrument mode the runtime does not admit.
pub(super) fn ea_mode_refuse_reason(estate: &EStateData<'_>) -> &'static str {
    if estate.es_instrument & ::types_core::instrument::INSTRUMENT_BUFFERS != 0 {
        "instrumented-buffers"
    } else {
        // WAL-class combinations (EXPLAIN rejects WAL-without-ANALYZE
        // loudly upstream; anything else unrecognized refuses honestly).
        "instrumented-mode"
    }
}

/// Write the merged funnel into a bypassed member node's Instrumentation
/// (ea-morsels.md §3: rows/loops stay node-exact — breakers know what
/// crossed them). Post-EndLoop fields written directly with running=false,
/// so the explain seam's forced InstrEndLoop is a no-op. TIMING OFF leaves
/// every timing field zero — exactly INSTRUMENT_ROWS output.
pub(super) fn ea_fill_scan_node(
    estate: &mut EStateData<'_>,
    plan_node_id: i32,
    rows: &EaRowTally,
) {
    let Ok(ix) = usize::try_from(plan_node_id) else { return };
    let Some(i) = estate.es_instrumentation.get_mut(ix) else { return };
    i.ntuples += rows.survived as f64;
    i.nfiltered1 += rows.scanned.saturating_sub(rows.survived) as f64;
    i.nloops += 1.0;
}

/// A bypassed pass-through member node (the distinct arm's Sort): rows out =
/// rows in, no filter arms; timing fields untouched (§3 — never divide a
/// fused pipeline's time onto members).
pub(super) fn ea_fill_passthrough_node(
    estate: &mut EStateData<'_>,
    plan_node_id: i32,
    rows_out: u64,
) {
    let Ok(ix) = usize::try_from(plan_node_id) else { return };
    let Some(i) = estate.es_instrumentation.get_mut(ix) else { return };
    i.ntuples += rows_out as f64;
    i.nloops += 1.0;
}

/// Construct the arm's ACCEPT-phase pipeline report (ea-morsels.md §4) from
/// the leader merge. Pushed onto es_runtime_ea_pipelines on clean Completed
/// outcomes only — the same emission-gate law as the refusal records.
#[allow(clippy::too_many_arguments)]
pub(super) fn ea_pipeline_report(
    arm: &'static str,
    root_node_id: i32,
    member_node_id: i32,
    member2_node_id: i32,
    taskset_count: u32,
    partials: u64,
    m: &InstrumentMerged,
) -> ::types_core::instrument::RuntimeEaPipeline {
    ::types_core::instrument::RuntimeEaPipeline {
        root_node_id,
        member_node_id,
        member2_node_id,
        arm,
        role: "accept",
        taskset_index: 1,
        taskset_count,
        workers: m.workers,
        claims: m.claims,
        // = claims until the dop1-tax claim-coalescing split is threaded
        // through the partial (ea-morsels.md §2 coordination contract).
        epoch_segments: m.claims,
        granules: m.granules,
        rows_scanned: m.rows.scanned,
        rows_survived: m.rows.survived,
        partials,
        prune: m.prune,
        busy_ns: m.busy_ns,
        first_start_ns: m.first_start_ns,
        last_end_ns: m.last_end_ns,
        min_last_end_ns: m.min_last_end_ns,
        morsel_min_ns: m.morsel_min_ns,
        morsel_max_ns: m.morsel_max_ns,
    }
}
