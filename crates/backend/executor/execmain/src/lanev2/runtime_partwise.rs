//! PARTWISE-MORSELS increment 1 (night/partitionwise-morsels; Phase-4
//! "Parallel Append / partitionwise" bucket, m5-coverage.tsv row
//! `parallel-append-partitionwise`): partition-as-morsel for the PLAIN
//! (ungrouped) fold shape over a partitioned table.
//!
//! Shape: a SERIAL-plan plain Agg over an Append of per-partition SeqScans
//! (`Agg → Append → SeqScan×N` — what the planner emits for
//! `SELECT count(x), sum(y) FROM part_tab` once the knob-gated probe in
//! `m5_partwise.rs` suppresses the Gather). Executed as ONE runtime TaskSet
//! at DOP N on the EXISTING scan-arm machinery (`runtime_scan::engage`):
//!
//!  * GEOMETRY — the children's granule spaces are CONCATENATED into one
//!    claim space: child c owns granules `[child_starts[c],
//!    child_starts[c+1])`, and every child edge is a HARD BOUNDARY in the
//!    engagement [`runtime::GranuleMap`] (pgrcolumnar children additionally
//!    contribute their interior row-group boundaries, offset-shifted). The
//!    runtime's universal claim rule — a claim never crosses a hard
//!    boundary; coalesced claims are segmented back at those edges inside
//!    `morsel_body` — therefore yields "partition = one or more morsels"
//!    with NO new scheduler machinery: each claim segment resolves to
//!    exactly one child, positioned with a child-local range.
//!  * DRIVE — per segment, the worker resolves the owning child
//!    ([`PartwiseCtx::child_of`]), positions THAT child's scan
//!    (`set_granule_range` semantics are child-relative), and re-enters the
//!    UNCHANGED serial PER-ROW drive (`DriveMode::PerRowFold`:
//!    `seq_scan_batch_emit` fetch + PROJECTION per row +
//!    `agg_plain_build_accept` — Append children carry CP_EXACT_TLIST
//!    projections structurally, and emit returns the projected slot, so
//!    the transition program sees the serial OUTER positions exactly; the
//!    fold-kernel fast path for projection-free children is a measured
//!    follow-up). The Agg transition state accumulates ACROSS children
//!    inside each worker — sound because admission requires the
//!    order-insensitive-exact partial whitelist
//!    (`agg_runtime_partial_admissible`), so per-partition sub-partials and
//!    cross-partition partials combine to the same values.
//!  * COMBINE — byte-identical to the scan arm: per-worker cumulative
//!    partial export per claim, leader absorb + ordinary finalize. The
//!    cross-PARTITION combine IS the cross-WORKER combine; no new fold or
//!    combine vocabulary. (Plan-order emit is vacuous for the plain shape —
//!    one output row; the first-seen-order law for grouped/row-emit
//!    partitionwise shapes is increment ≥2 territory, see the design doc.)
//!
//! Contrast with legacy Parallel Append: C's unit of balance is a whole
//! subplan (non-partial children get max=1 worker; stragglers dominate).
//! Here EVERY child is block/granule-claimable by every worker — the
//! mixed-partial-children weakness disappears by construction.
//!
//! Kill-switch layering (all default OFF — the increment is INERT unless
//! armed):
//!   * `PGRUST_LANE_V2_PARTWISE=1|on` — arms BOTH this engagement arm and
//!     the plan-time probe (`m5_partwise.rs`, same env spelling — the
//!     GROUPSINK knob-coherence law: a keyed shape whose arm is disarmed
//!     would land on serial).
//!   * everything the scan arm already requires (`PGRUST_RUNTIME=1`,
//!     `pgrust.parallel_engine=runtime` / an armed pool DOP).
//!
//! Fail-closed admission (refuse ⇒ the serial Append per-tuple drive,
//! byte-identically). NAMED refusals of this increment — each is a future
//! sub-shape, none is silent: runtime/exec-time pruning
//! (`part_prune_index >= 0`), init-pruned substates, async children,
//! non-SeqScan children (nested Append = sub-partitioning, index/bitmap
//! child plans, FDW children), mixed heap/pgrcolumnar children, child
//! quals (v1 — the probe never keys quals; the arm re-refuses so the
//! emit-path double-evaluation surface stays closed), census / storeless
//! count shapes (no lane columns), poly-manifest shapes, EXPLAIN ANALYZE,
//! params of either kind, non-MVCC snapshots, grouped shapes (the probe
//! never keys them; the entry gate re-refuses).

use std::sync::{Arc, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::nodeagg::runtime_partial::agg_runtime_partial_admissible;
use ::types_error::PgResult;
use ::types_nodes::NodeTag;

use super::batch_source::{BatchGranuleSource, SeqScanSource};
use super::router::{self, ArmClass, ArmCounter};
use super::runtime_scan::{elastic_dop, engage, exprs_parallel_safe, min_granules, whole_claims};
use super::{lane_trace, seq_scan_fusible};

/// The engagement arm's knob — DEFAULT ON since the GL-PARTWISE-1 flip
/// (2026-07-21); `PGRUST_LANE_V2_PARTWISE=0|off` kills (t35 exact-spelling
/// flipped-kill). The plan-time probe (`planner/src/m5_partwise.rs`) reads
/// the IDENTICAL env spelling — knob coherence: the probe must never
/// suppress a Gather this arm won't pick up, and one kill spelling disarms
/// BOTH halves (killed world = keep-Gather, byte-for-byte pre-flip).
pub(super) fn partwise_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_LANE_V2_PARTWISE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// The partition directory of one engagement: child offset prefix sums into
/// the concatenated granule space (len = nchildren+1, `[0] == 0`, last ==
/// total granules). Built by admission on the leader, carried on the
/// engagement payload, read per claim segment by every worker. Immutable.
///
/// Child order IS Append plan order (`appendplans[i]` ⇔ `substates[i]` ⇔
/// `[child_starts[i], child_starts[i+1])`) — the plan-order law for the
/// claim-space directory; workers index `substates` with the resolved child.
pub(super) struct PartwiseCtx {
    child_starts: Vec<u64>,
}

impl PartwiseCtx {
    /// `child_starts` are non-decreasing prefix sums; empty children
    /// (zero-granule partitions) are tolerated as duplicate entries — they
    /// own no granules and are never resolved.
    pub(super) fn new(child_starts: Vec<u64>) -> PartwiseCtx {
        debug_assert!(child_starts.len() >= 2, "a partwise directory needs >=1 child");
        debug_assert_eq!(child_starts.first(), Some(&0));
        debug_assert!(child_starts.windows(2).all(|w| w[0] <= w[1]));
        PartwiseCtx { child_starts }
    }

    pub(super) fn nchildren(&self) -> usize {
        self.child_starts.len() - 1
    }

    pub(super) fn total(&self) -> u64 {
        *self.child_starts.last().expect("non-empty prefix sums")
    }

    /// The child owning granule `g` (`g < total`). With duplicate entries
    /// (empty children) `partition_point` lands past every empty child at
    /// the same offset — the owner is the first child whose END is above
    /// `g`, which is exactly the non-empty one.
    pub(super) fn child_of(&self, g: u64) -> usize {
        debug_assert!(g < self.total(), "granule outside the claim space");
        self.child_starts.partition_point(|&s| s <= g) - 1
    }

    /// Child `c`'s first granule in the concatenated space.
    pub(super) fn child_start(&self, c: usize) -> u64 {
        self.child_starts[c]
    }
}

/// Record-and-refuse: the partwise arm's refusals ride the scan-arm router
/// taxonomy (entry-arm attribution, self-describing `partwise-` reasons)
/// and the engagement trace channel. No EA surface in this increment (EA
/// admission is a named refusal).
fn refused<T>(reason: &'static str) -> PgResult<Option<T>> {
    router::tick_refused(ArmClass::Scan, reason);
    if super::lane_trace_enabled() {
        lane_trace(&format!("runtime-partwise: refused ({reason})"));
    }
    Ok(None)
}

/// Partition-as-morsel engagement for `Agg(plain) → Append → SeqScan×N`.
/// `Some(result)` = the runtime drove the node; `None` = refused (the
/// caller falls through to the unchanged serial Append drive).
///
/// Mirrors `runtime_scan::try_own_plain_agg_runtime`'s gate ladder with the
/// Append-shaped structural walk in place of the single-scan one; every
/// non-structural gate is the scan arm's verbatim (same session envelope,
/// same worker binder policy — the workers ARE scan-arm workers driving a
/// wider plan).
pub(super) fn try_own_plain_agg_partwise<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    apn: &mut crate::procnode::AppendNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    if !partwise_enabled() {
        return Ok(None);
    }
    let dop = router::arm_dop(ArmClass::Scan);
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };
    if !::nodeagg::agg_is_done(agg) {
        router::tick(ArmClass::Scan, ArmCounter::Offered);
    }

    // --- Session gates (fail-closed; every refusal is the serial arm).
    // EA/instrumented trees are a NAMED v1 refusal (the workers run
    // uninstrumented and this arm carries no instr partial slots yet).
    if estate.es_instrument != 0 {
        return refused("partwise-instrumented");
    }
    if estate.es_epq_active {
        return Ok(None);
    }
    if super::runtime_in_parallel_role() {
        return refused("partwise-in-parallel-mode");
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        return refused("partwise-params");
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        return refused("partwise-params");
    }
    // The Agg must be the plan root (workers ExecutorStart the whole worker
    // pstmt — Agg over Append over the child scans, transferred verbatim).
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else {
        return refused("partwise-agg-not-plan-root");
    };
    if !std::ptr::eq(root_agg, agg.plan) {
        return refused("partwise-agg-not-plan-root");
    }
    // Order-insensitive-exact partials only (the existing export/combine
    // vocabulary IS the cross-partition combine); no poly manifest in v1.
    if !agg_runtime_partial_admissible(agg) {
        return refused("partwise-partials-not-order-insensitive-exact");
    }
    // Fold-mode only in v1: the classified plan must read lane columns.
    // Census (no columns) and bare-count storeless shapes keep their
    // refusals — their drive economics are per-AM carve-outs the single-rel
    // arm owns; wiring them across children is a named follow-up.
    if !::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| !p.cols.is_empty()) {
        return refused("partwise-not-fold-shape");
    }

    // --- Append structural walk (plan side).
    let Some(outer_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if outer_node.node_tag() != NodeTag::T_Append {
        return refused("partwise-outer-not-append");
    }
    let Some(append_plan) = outer_node.as_append() else {
        return refused("partwise-outer-not-append");
    };
    if append_plan.nasyncplans != 0 {
        return refused("partwise-async-children");
    }
    // Runtime/exec-time partition pruning is a named v1 refusal: a pruned
    // child must not be scanned, and the concatenated claim space has no
    // per-child validity mask yet. (Plan-time pruning is invisible here —
    // pruned children simply never reach appendplans.)
    if append_plan.part_prune_index != -1 {
        return refused("partwise-runtime-pruning");
    }
    let nplans = append_plan.appendplans.len();
    if nplans < 2 {
        // Single-child Appends are usually elided by setrefs; if one
        // survives, the single-rel scan arm is the right owner. Fail closed.
        return refused("partwise-single-child");
    }
    if apn.substates.len() != nplans {
        return refused("partwise-init-pruned-substates");
    }
    for (i, child) in apn.substates.iter().enumerate() {
        // Belt-and-braces: substate i must be appendplans[i] (no init-time
        // pruning under part_prune_index == -1, but keep the map honest).
        if apn.subplan_origin.get(i).copied() != Some(i as i32) {
            return refused("partwise-init-pruned-substates");
        }
        let _ = child;
    }
    for child in &append_plan.appendplans {
        if child.node_tag() != NodeTag::T_SeqScan {
            return refused("partwise-child-not-seqscan");
        }
        let Some(sp) = child.as_seq_scan() else {
            return refused("partwise-child-not-seqscan");
        };
        // Child scan expressions run on helpers.
        if !exprs_parallel_safe(sp.scan.plan.qual.iter())?
            || !exprs_parallel_safe(sp.scan.plan.targetlist.iter())?
        {
            return refused("partwise-exprs-not-parallel-safe");
        }
    }
    // MVCC snapshot (visibility folding parity with the serial drive).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return refused("partwise-non-mvcc-snapshot");
    }
    // Binder policy sources must be empty (every helper bind would refuse).
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        return refused("partwise-binder-policy");
    }

    // --- Per-child state walk: fusible, uniform AM, qual-free (v1 — the
    // probe never keys quals; refusing here keeps the double-evaluation
    // surface closed defensively). Child PROJECTIONS are ADMITTED: Append
    // children carry CP_EXACT_TLIST projections structurally (createplan),
    // and the per-row drive's `seq_scan_batch_emit` applies them per row,
    // returning the PROJECTED slot — the Agg transition program sees the
    // serial OUTER positions exactly. (A fold-kernel fast path for
    // projection-free children is a measured follow-up.)
    let mut uniform_cb: Option<bool> = None;
    for sub in apn.substates.iter_mut() {
        let crate::procnode::PlanStateNode::SeqScan(ss) = sub else {
            return refused("partwise-child-state-not-seqscan");
        };
        if !seq_scan_fusible(ss, estate)? {
            return refused("partwise-child-not-fusible");
        }
        let is_cb = ::nodeseqscan::seq_scan_is_pgrcolumnar(ss);
        if !(is_cb || ::nodeseqscan::seq_scan_is_heap(ss)) {
            return refused("partwise-child-am");
        }
        match uniform_cb {
            None => uniform_cb = Some(is_cb),
            Some(prev) if prev != is_cb => return refused("partwise-mixed-am"),
            _ => {}
        }
        if ss.ss.qual.is_some() {
            return refused("partwise-child-qual");
        }
    }
    let is_cb = uniform_cb.expect("nplans >= 2 children walked");

    // --- Geometry: concatenate the children's granule spaces. Child edges
    // become hard boundaries; pgrcolumnar children contribute their interior
    // row-group boundaries offset-shifted (reconstructed via boundary_after
    // — GranuleMap keeps its prefix sums private). c0 is per-AM and uniform
    // across children (heap 16 / pgrcolumnar 2); min() is the conservative
    // startup seed either way.
    let mut starts: Vec<u64> = vec![0];
    let mut child_starts: Vec<u64> = vec![0];
    let mut c0 = u64::MAX;
    for sub in apn.substates.iter_mut() {
        let crate::procnode::PlanStateNode::SeqScan(ss) = sub else {
            unreachable!("walked above");
        };
        let Some(m) = SeqScanSource::new(&mut *ss).granule_map(estate)? else {
            return refused("partwise-child-no-geometry");
        };
        let off = *child_starts.last().expect("seeded");
        let total = m.total();
        let mut b = 0u64;
        while b < total {
            let nb = m.boundary_after(b);
            starts.push(off + nb);
            b = nb;
        }
        if total == 0 {
            // Empty child: keep the edge as a (tolerated) duplicate entry so
            // the directory and the boundary map stay index-aligned.
            starts.push(off);
        }
        child_starts.push(off + total);
        c0 = c0.min(m.c0());
    }
    let ctx = Arc::new(PartwiseCtx::new(child_starts));
    let total_granules = ctx.total();
    if total_granules < min_granules().max(2 * dop as u64) {
        return refused("partwise-tiny-input-floor");
    }
    let map = Arc::new(runtime::GranuleMap::with_boundaries(Arc::new(starts), c0));
    let nrgs = map.nbounds();
    // Claim posture is EXPLICIT per arm (the GranuleMapSource contract):
    // pgrcolumnar children keep the scan arm's whole-boundary + coalesce
    // posture (morsel_body segments coalesced claims back at every edge —
    // partition edges included); heap children stay sizer-truncated, never
    // coalesced — the boundary clamp alone confines each claim to one child.
    let source: Arc<dyn runtime::MorselSource> = Arc::new(runtime::GranuleMapSource::new(
        Arc::clone(&map),
        is_cb && whole_claims(),
        is_cb,
    ));

    if ::nodeagg::agg_is_done(agg) {
        // Done-repull after a completed engagement.
        return Ok(Some(None));
    }

    // --- Engage on the scan-arm ceremony (same worker binder, same
    // combine); the partition directory rides the payload.
    let dop = elastic_dop(dop, total_granules);
    let class = if is_cb { ArmClass::Scan } else { ArmClass::Heap };
    router::tick(class, ArmCounter::Engaged);
    if super::lane_trace_enabled() {
        lane_trace(&format!(
            "runtime-partwise: engage dop={dop} children={} granules={total_granules} \
             edges={nrgs}",
            ctx.nchildren()
        ));
    }
    let r = engage(
        agg,
        estate,
        rt,
        dop,
        total_granules,
        nrgs,
        source,
        Some(map),
        None,
        false,
        None,
        Some(ctx),
        None,
    )?;
    router::tick(
        class,
        if r.is_some() { ArmCounter::Completed } else { ArmCounter::Fallback },
    );
    Ok(r)
}

// ---------------------------------------------------------------------------
// Tests: the partition directory math the drive depends on (child
// resolution, prefix-sum construction invariants, empty children).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::PartwiseCtx;

    #[test]
    fn child_of_resolves_plan_order() {
        // Three children: 10, 5, 7 granules.
        let ctx = PartwiseCtx::new(vec![0, 10, 15, 22]);
        assert_eq!(ctx.nchildren(), 3);
        assert_eq!(ctx.total(), 22);
        assert_eq!(ctx.child_of(0), 0);
        assert_eq!(ctx.child_of(9), 0);
        assert_eq!(ctx.child_of(10), 1);
        assert_eq!(ctx.child_of(14), 1);
        assert_eq!(ctx.child_of(15), 2);
        assert_eq!(ctx.child_of(21), 2);
        assert_eq!(ctx.child_start(0), 0);
        assert_eq!(ctx.child_start(1), 10);
        assert_eq!(ctx.child_start(2), 15);
    }

    #[test]
    fn child_of_skips_empty_children() {
        // Child 1 is empty (duplicate prefix entry): granule 3 belongs to
        // child 0, granule 5 to child 2 — never the empty child.
        let ctx = PartwiseCtx::new(vec![0, 5, 5, 12]);
        assert_eq!(ctx.nchildren(), 3);
        assert_eq!(ctx.child_of(4), 0);
        assert_eq!(ctx.child_of(5), 2);
        assert_eq!(ctx.child_of(11), 2);
    }

    #[test]
    fn child_of_leading_empty_child() {
        let ctx = PartwiseCtx::new(vec![0, 0, 8]);
        assert_eq!(ctx.nchildren(), 2);
        assert_eq!(ctx.child_of(0), 1);
        assert_eq!(ctx.child_of(7), 1);
    }

    #[test]
    fn local_range_math_round_trips() {
        let ctx = PartwiseCtx::new(vec![0, 10, 15, 22]);
        // A segment [12, 15) resolves to child 1, local [2, 5).
        let (s, e) = (12u64, 15u64);
        let c = ctx.child_of(s);
        assert_eq!(c, 1);
        let base = ctx.child_start(c);
        assert_eq!((s - base, e - base), (2, 5));
    }
}
