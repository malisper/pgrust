//! Lane executor v2 — the operator→operator batched execution lane (production
//! rebuild). See `docs/design/lane-executor-v2.md`.
//!
//! Control model: **push** (Source → Operator → Sink), with a pull adapter at
//! the pipeline root because PostgreSQL's executor is Volcano/pull — the lane
//! is a push island that doles one tuple per `exec_proc_node` call out of the
//! root adapter's capacity-one buffer. The skeleton (traits + driver + root
//! adapter) lives in `lanev2/push.rs`; this file owns the per-scan refuse-sets
//! and the scan pipelines (source + scalar filter/project operator). The
//! conversion changes ONLY who calls whom: the batch staging primitives, the
//! one-row-at-a-time scalar emit, their order, and the refuse-sets are exactly
//! the Phase-1 pull drive's — byte-identical output.
//!
//! ALL substantive lane logic lives in this module, kept deliberately separate
//! from the byte-identical Volcano row-executor spine (`procnode.rs`,
//! `nodeseqscan`, `nodeagg`, …). The existing executor is touched in only a
//! handful of thin, mechanical spots:
//!   * `procnode::seq_scan_arm` — a 3-line dispatch hook (`if enabled() { if let
//!     Some(r) = try_own_seq_scan()? { return Ok(r) } }`) that falls through to
//!     the UNCHANGED per-tuple path on refuse;
//!   * `nodeseqscan::SeqScanState` — a two-`u32` page-batch cursor + accessors
//!     (the one-tuple-per-call drive needs its position to survive the Volcano
//!     call boundary, so this state must live on the node);
//!   * `executils::BatchSource` — the shared pull seam trait (it cannot live
//!     here: `nodeagg` re-exports it as `AggBatchSource`, and `nodeagg` cannot
//!     depend on `execmain` without a crate cycle, so the trait sits in the
//!     shared `executils` seam both crates already depend on).
//! Disabling or deleting the lane is therefore local: drop this module + the
//! thin hook, and the C-identical executor is exactly as before.
//!
//! Gated OFF by default via the `PGRUST_LANE_V2` env var — deliberately NOT a
//! SQL GUC: a new GUC would add a row to the byte-identical `pg_settings` /
//! `SHOW ALL` output and break the `guc` / `rules` regression tests. Env-var
//! gating mirrors `jit_deform`'s `PGRUST_JIT_DEFORM` switch and is
//! byte-identity-safe. The completeness-gate run sets `PGRUST_LANE_V2=1` to
//! enable the lane across the whole regression suite.

mod push;
mod stats;

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use push::{
    drain_pipeline, drain_pipeline_chain, pull_step, pull_step_chain, Batch, OpStatus, Operator,
    RootAdapter, Sink, SinkFeed, Source, TupleOp,
};
use stats::{RefuseReason, ShapeClass};

/// Master switch for lane-v2. Default OFF; `PGRUST_LANE_V2=1` (or `on`) enables
/// it. Resolved once per process (a boot-time decision, like
/// `jit_deform::available()`).
#[inline]
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2").as_deref(), Ok("1") | Ok("on"))
    })
}

// ===========================================================================
// Standalone scan ownership: DELIBERATELY REFUSED (admission economics,
// design §4; measured on the integration bench 2026-07-11, q9-class).
//
// The `try_own_*` scan entry points are reached only from the per-node
// dispatch arms — i.e. only when the PARENT is a per-tuple Volcano consumer
// (lane breakers drive their scan pipelines directly, never through these
// hooks). A lane-owned scan in that position emits one tuple per pull through
// the capacity-one adapter with NO batch consumer above and NO scan kernels
// wired yet — pure adapter overhead (q9: +3–9%), and for kernel-qual'd scans
// it PREEMPTS the row executor's own fused SoA-bitmap WithQual drive. Until
// the standalone scan pipeline carries a measured kernel advantage (Phase-3
// bitmap/dict kernels), refusing is strictly faster and byte-identical.
//
// This is a deliberate refuse-set entry, expected to SHRINK when Phase-3 scan
// kernels land; the scan pipelines stay fully exercised via the agg/sort
// breaker feeds.
const STANDALONE_SCAN_NO_UPSIDE: bool = true;

// ===========================================================================
// SeqScan ownership (Phase 1 first vertical slice, now push-driven). The
// pipeline is source → filter/project operator → root pull-adapter, over the
// same `BatchSource`-seam primitives the pull drive used
// (`seq_scan_next_pagebatch` / `seq_scan_batch_emit`).
// ===========================================================================

/// Try to let the lane *own* a `SeqScan` (scan→filter→project,
/// scalar-within-lane over row batches).
///
/// `Some(result)` = the lane drove this call (`result` is the tuple-or-end,
/// the ordinary `ExecProcNode` return); `None` = refused, and the caller must
/// run the unchanged `exec_seq_scan`. Refusing is always byte-safe.
#[inline]
pub fn try_own_seq_scan<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(ShapeClass::SeqScan, RefuseReason::AdmissionEconomicsNoConsumer);
        return Ok(None);
    }
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    // Assemble the scan-only push pipeline. Stages are stateless unit structs
    // (cross-call position is node-resident), so per-call assembly is free.
    // End-of-stream mirrors ExecScanExtended's projected-slot clear (the
    // non-projected path returns end-of-scan without clearing).
    let clear_on_finish = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut root = RootAdapter::new(clear_on_finish);
    Ok(Some(pull_step(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 SeqScan pipeline (false → the caller falls
/// through to `exec_seq_scan`, byte-identically). Admits Plain / WithQual /
/// WithProject / WithQualProject over a page-batch-supporting AM, and only
/// when the qual and projection are subplan-free and param-free: the generic
/// per-row emit path runs neither initplan params nor subplan quals, whereas
/// `exec_scan_extended` does, so those shapes must keep the old path.
///
/// Disarms on: EPQ, a backward/mark cursor (init eflags) or a non-forward
/// call, EXPLAIN ANALYZE (instrumented), the Bloom/EPQ variants, and AMs
/// without page-batch support. Parallel scans (leader or worker) are
/// admitted: the batched page feed acquires blocks through the shared DSM
/// block cursor (`parallel_next_block`), exactly as the per-tuple pagemode
/// walk does, so per-worker page batches partition the relation without
/// gaps or overlaps.
fn seq_scan_fusible<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Dynamic per-call gates: these may legitimately vary call to call.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SeqScan, RefuseReason::Epq);
        return Ok(false);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SeqScan, RefuseReason::Backward);
        return Ok(false);
    }
    // Static verdict, memoized on the node at first evaluation: (a) stability
    // — a mid-scan REFUSE→OWN flip would silently skip the staged remainder
    // of the current page batch; (b) the fusibility cascade (expr walks + AM
    // probe) must not run once per pulled tuple on the Volcano hot path.
    // Engagement accounting ticks exactly here — once per memoized decision.
    if let Some(v) = ss.lane_verdict() {
        return Ok(v);
    }
    let refuse = seq_scan_refuse_reason(ss, estate)?;
    let v = match refuse {
        None => {
            stats::tick_owned(ShapeClass::SeqScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::SeqScan, r);
            false
        }
    };
    ss.set_lane_verdict(v);
    Ok(v)
}

/// The call-invariant half of the SeqScan refuse-set: plan shape, init-time
/// eflags, parallel wiring, instrumentation, and AM page-batch support.
/// `None` = admitted; `Some(reason)` = refused (the caller ticks accounting).
fn seq_scan_refuse_reason<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if !ss.batch_allowed() {
        return Ok(Some(RefuseReason::ScrollMark));
    }
    if ss.ss.instr_idx.is_some() {
        return Ok(Some(RefuseReason::Instrumented));
    }
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain
        | ::nodeseqscan::SeqScanVariant::WithQual
        | ::nodeseqscan::SeqScanVariant::WithProject
        | ::nodeseqscan::SeqScanVariant::WithQualProject => {}
        ::nodeseqscan::SeqScanVariant::PlainBloom => {
            return Ok(Some(RefuseReason::BloomVariant))
        }
        ::nodeseqscan::SeqScanVariant::Epq => return Ok(Some(RefuseReason::Epq)),
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        if q.has_subplan() || !q.param_exec_deps().is_empty() {
            return Ok(Some(RefuseReason::SubplanParam));
        }
    }
    if let Some(p) = ss.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return Ok(Some(RefuseReason::SubplanParam));
        }
    }
    // AM must support the page-batch primitives (opens the scan desc once).
    // The parallel-admitting variant: only this lane routes through it; the
    // fused agg/sort/hash drives keep `seq_scan_batch_supported`'s
    // serial-only gate.
    Ok(if ::nodeseqscan::seq_scan_batch_supported_parallel(ss, estate)? {
        None
    } else {
        Some(RefuseReason::NoPageBatch)
    })
}

/// Push source: stages heap page batches (`seq_scan_next_pagebatch` — the
/// same `BatchSource`-seam primitive `SeqScanBatchSource` wraps). Staging
/// resets the node-resident consume cursor: a fresh batch replaces the staged
/// rows.
struct SeqScanSource;

impl<'mcx> Source<'mcx> for SeqScanSource {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: the per-tuple path's getnextslot clears the scan
            // slot on exhaustion (dropping its buffer pin); match it so a
            // lane-owned scan does not hold a pin until rescan/end.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: the scan's scalar filter→project segment. Consumes the
/// staged batch row-by-row via `seq_scan_batch_emit` — `ExecScanExtended`'s
/// body over a staged batch row (reset per-tuple context, store + apply the
/// scan qual scalar-per-row via `execexpr`, project) — pushing each surviving
/// output slot into the sink. Filter and projection stay fused within this
/// one segment operator per the operator-model decision (design §1): the push
/// conversion inverts driver control, never the fused per-row segment. Same
/// tuples, same order, same qual/proj/NULL semantics as `exec_seq_scan` →
/// BYTE-IDENTICAL.
///
/// The consume position over the staged page batch lives on the node
/// (`SeqScanState::lane_cursor`), so a `Paused` pipeline survives the Volcano
/// per-call boundary.
struct SeqScanFilterProject;

impl<'mcx> Operator<'mcx> for SeqScanFilterProject {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            // Match the per-tuple path's interrupt cadence: `exec_scan_fetch`
            // runs `check_for_interrupts` once per tuple attempt. Skipping it
            // in the batched drive would process pending interrupts / cache
            // invalidations at a different cadence than the code the lane
            // replaces; keep it identical.
            ::postgres_seams::check_for_interrupts::call()?;
            node.set_lane_cursor(pos + 1, n);
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(slot, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }
}

// ===========================================================================
// IndexScan ownership (Phase 1 breadth, now push-driven). Same pipeline shape
// over the SAME batch primitives the fused-agg path uses
// (`index_scan_next_tidrun` / `index_scan_batch_fetch`). The admitted shape is
// deliberately narrow — no qual, no projection, no runtime keys, forward btree
// — so the node's output is exactly the stored scan tuple: `exec_index_scan`
// over that shape is `exec_scan_extended::<false,false>` (reset ctx, fetch,
// return the scan slot). Same visible tuples, same index order → BYTE-IDENTICAL.
// ===========================================================================

/// Try to let the lane own an `IndexScan`. `Some` = lane drove this call;
/// `None` = refused (caller runs the unchanged `exec_index_scan`).
#[inline]
pub fn try_own_index_scan<'mcx>(
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(ShapeClass::IndexScan, RefuseReason::AdmissionEconomicsNoConsumer);
        return Ok(None);
    }
    if !index_scan_fusible(is, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(is, &mut IndexScanSource, &mut IndexScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 IndexScan pipeline. Admits only the shape the
/// fused-agg index arm admits (no qual / no projection / no runtime keys /
/// forward index order / btree AM / MVCC), plus the lane-specific disarms:
/// EPQ, a non-forward call, a scrollable/backward or mergejoin-mark cursor
/// (`!batch_allowed` — mark/restore + backward desync the tidrun cursor),
/// parallel, EXPLAIN ANALYZE (instrumented), and any amcanorderbyop reorder
/// (`iss_OrderBy`) which the tidrun path does not reorder.
fn index_scan_fusible<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // This gate is per-call (not node-memoized), so accounting ticks are
    // per-pull decisions for this class — see `stats.rs` tick semantics.
    match index_scan_refuse_reason(is, estate) {
        None => {
            stats::tick_owned(ShapeClass::IndexScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::IndexScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn index_scan_refuse_reason<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if !is.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if is.iss_ParallelAware {
        return Some(RefuseReason::ParallelGate);
    }
    if is.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    // Same-block tidrun batching is only sound under an MVCC snapshot (matches
    // the fused-agg gate; non-MVCC keeps the per-tuple path).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    if is.ss.qual.is_some() || is.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    if is.iss_Runtime.is_some() {
        return Some(RefuseReason::RuntimeKeys);
    }
    if is.iss_OrderBy.is_some() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(is.iss_OrderDir) {
        return Some(RefuseReason::Backward);
    }
    if !is
        .iss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    None
}

/// Push source: stages a same-block TID run (`index_scan_next_tidrun`, which
/// runs `check_for_interrupts` per run, matching the fused-agg drive this
/// reuses). Staging resets the node-resident consume cursor.
struct IndexScanSource;

impl<'mcx> Source<'mcx> for IndexScanSource {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeindexscan::index_scan_next_tidrun(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: C's IndexNext clears the scan slot on exhaustion
            // (dropping its buffer pin); match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: replays the staged TID run one visible tuple at a time
/// (`index_scan_batch_fetch`, sequential: entry `i>0` advances the AM cursor,
/// so the run is consumed 0,1,2,… without gaps). No qual/projection → the
/// pushed tuple is the stored scan slot. The run position lives on the node
/// (`IndexScanState::lane_cursor`) to survive the Volcano call boundary.
struct IndexScanEmit;

impl<'mcx> Operator<'mcx> for IndexScanEmit {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let scan_id = node.ss.ss_ScanTupleSlot;
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            node.set_lane_cursor(pos + 1, n);
            if ::nodeindexscan::index_scan_batch_fetch(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(scan_id, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }
}

// ===========================================================================
// IndexOnlyScan ownership (push-driven). `index_only_scan_batch_next` advances
// to the next VISIBLE index tuple (VM probe / heap fallback / predicate lock —
// C's IndexOnlyNext order) and returns 0 or 1; `index_only_scan_batch_store`
// stages `xs_itup` into the scan slot. The source produces one-row batches, so
// a batch never outlives the driver round that produced it — no node-resident
// cursor. Narrow shape (no qual / no projection / no runtime keys / forward
// btree) → the output is the stored scan tuple, identical to
// `exec_index_only_scan`.
// ===========================================================================

/// Try to let the lane own an `IndexOnlyScan`.
#[inline]
pub fn try_own_index_only_scan<'mcx>(
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(
            ShapeClass::IndexOnlyScan,
            RefuseReason::AdmissionEconomicsNoConsumer,
        );
        return Ok(None);
    }
    if !index_only_scan_fusible(ios, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(ios, &mut IndexOnlyScanSource, &mut IndexOnlyScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 IndexOnlyScan pipeline (mirrors the fused-agg
/// IOS arm + the lane disarms). `!batch_allowed` refuses a scrollable/backward
/// or mergejoin-mark cursor; `ioss_OrderByKeys` non-empty refuses
/// amcanorderbyop (distance-ordered) scans.
fn index_only_scan_fusible<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // Per-call gate: accounting ticks are per-pull decisions for this class.
    match index_only_scan_refuse_reason(ios, estate) {
        None => {
            stats::tick_owned(ShapeClass::IndexOnlyScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::IndexOnlyScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn index_only_scan_refuse_reason<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if !ios.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if ios.ioss_ParallelAware {
        return Some(RefuseReason::ParallelGate);
    }
    if ios.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    if ios.ss.qual.is_some() || ios.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    if ios.ioss_Runtime.is_some() {
        return Some(RefuseReason::RuntimeKeys);
    }
    if !ios.ioss_OrderByKeys.is_empty() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(ios.ioss_OrderDir) {
        return Some(RefuseReason::Backward);
    }
    if !ios
        .ioss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    None
}

/// Push source: one VISIBLE index tuple per batch (`index_only_scan_batch_next`
/// runs `check_for_interrupts` per tuple).
struct IndexOnlyScanSource;

impl<'mcx> Source<'mcx> for IndexOnlyScanSource {
    type Node = ::nodeindexonlyscan::IndexOnlyScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeindexonlyscan::index_only_scan_batch_next(node, estate)?;
        if n == 0 {
            // End of scan: C's IndexOnlyNext clears the scan slot on
            // exhaustion; match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
            return Ok(None);
        }
        debug_assert_eq!(n, 1);
        Ok(Some(Batch { n }))
    }
}

/// Push operator: stages `xs_itup` into the scan slot and pushes it. One-row
/// batches are always fully consumed within the producing driver round, so
/// `pending` is statically `None` (the drive is stateless across the Volcano
/// boundary — no cursor).
struct IndexOnlyScanEmit;

impl<'mcx> Operator<'mcx> for IndexOnlyScanEmit {
    type Node = ::nodeindexonlyscan::IndexOnlyScanState<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        ::nodeindexonlyscan::index_only_scan_batch_store(node, estate)?;
        Ok(match out.accept(node.ss.ss_ScanTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// BitmapHeapScan ownership (push-driven). The bitmap must be built before the
// pipeline runs — the dispatch hook keeps the arm's existing
// `bitmap_table_scan_setup_dispatch` call, then offers the
// (already-initialized) scan to the lane. Same pipeline shape as the SeqScan
// lane over the page-batch primitives (`bitmap_scan_next_pagebatch` /
// `bitmap_scan_batch_fetch`, random-access by `i`); `bitmap_scan_batch_fetch`
// applies the page recheck (`bitmapqualorig`) internally on lossy/recheck
// pages, exactly as `BitmapHeapNext` does. Narrow shape (no scan qual / no
// projection) → the output is the stored scan tuple.
// ===========================================================================

/// Try to let the lane own a `BitmapHeapScan`. The caller must have already
/// run the bitmap setup (the arm does, unconditionally, before this).
#[inline]
pub fn try_own_bitmap_heap_scan<'mcx>(
    bhs: &mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(
            ShapeClass::BitmapHeapScan,
            RefuseReason::AdmissionEconomicsNoConsumer,
        );
        return Ok(None);
    }
    if !bitmap_heap_scan_fusible(bhs, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(bhs, &mut BitmapHeapScanSource, &mut BitmapHeapScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 BitmapHeapScan pipeline (mirrors the fused-agg
/// bitmap arm: no scan qual / no projection). Disarms EPQ, non-forward,
/// parallel (aware or a worker attached to shared state), and EXPLAIN ANALYZE.
/// Also refuses when the page recheck qual (`bitmapqualorig`) carries a subplan
/// or exec-param — the recheck runs a plain `exec_qual` that evaluates neither.
/// Bitmap scans are never scrollable/mark cursors (planner-guaranteed; a SCROLL
/// cursor gets a Material parent), so no eflags gate is needed. Bitmap init
/// asserts an MVCC snapshot, so that is implicit.
fn bitmap_heap_scan_fusible<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // Per-call gate: accounting ticks are per-pull decisions for this class.
    match bitmap_heap_scan_refuse_reason(bhs, estate) {
        None => {
            stats::tick_owned(ShapeClass::BitmapHeapScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::BitmapHeapScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn bitmap_heap_scan_refuse_reason<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if bhs.parallel_aware || bhs.pstate.is_some() {
        return Some(RefuseReason::ParallelGate);
    }
    if bhs.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    if bhs
        .bitmapqualorig
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty())
    {
        return Some(RefuseReason::SubplanParam);
    }
    if bhs.ss.qual.is_some() || bhs.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    None
}

/// Push source: stages the next bitmap page's tuples
/// (`bitmap_scan_next_pagebatch` runs `check_for_interrupts` per page).
/// Staging resets the node-resident consume cursor.
struct BitmapHeapScanSource;

impl<'mcx> Source<'mcx> for BitmapHeapScanSource {
    type Node = ::nodebitmapheapscan::BitmapHeapScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodebitmapheapscan::bitmap_scan_next_pagebatch(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: C's BitmapHeapNext returns ExecClearTuple(slot) on
            // exhaustion (dropping its buffer pin); match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: pushes each surviving row of the staged page
/// (`bitmap_scan_batch_fetch` applies the page recheck on lossy pages). The
/// page-batch position lives on the node (`BitmapHeapScanState::lane_cursor`).
struct BitmapHeapScanEmit;

impl<'mcx> Operator<'mcx> for BitmapHeapScanEmit {
    type Node = ::nodebitmapheapscan::BitmapHeapScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let scan_id = node.ss.ss_ScanTupleSlot;
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            node.set_lane_cursor(pos + 1, n);
            if ::nodebitmapheapscan::bitmap_scan_batch_fetch(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(scan_id, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }
}

// ===========================================================================
// Hash-agg pipeline breaker (Phase-2 vertical slice): the first
// operator→operator composition. Two chained pipelines on one Agg node:
//
//   pipeline N   : SeqScanSource → SeqScanFilterProject → HashAggBuildSink
//   pipeline N+1 : HashAggSource → HashAggEmit → RootAdapter
//
// The breaker node (the Agg) implements Sink for pipeline N (accept = the
// existing per-row transition path via `agg_hash_build_accept`; always
// `NeedMore`) and Source for pipeline N+1 (produce = the existing
// `agg_retrieve_hash_table` read-back — same table, same iteration → same
// output order as C, spill refill included). Chaining is the per-node
// Build→Probe phase flag (`table_filled` — C's own cross-call state), driven
// from the `agg_arm` dispatch hook: the build pipeline drains to completion
// before the first probe tuple, which is C's exact order for free
// (push-executor study, Pattern 3). Spill delegates wholesale to the row-path
// hashagg machinery (§8): `finish()` = spill finish + handoff install; the
// read-back's refill walks PG's spill partitions in PG's order.
// ===========================================================================

/// Memoized structural choice for an Agg-over-SeqScan node, decided at the
/// first call and stable thereafter (a mid-stream flip would desync the
/// build). Dynamic gates (EPQ, direction, the post-build merge handoff) stay
/// per-call in `agg_over_seq_scan_fusible`, evaluated BEFORE the memo.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggLaneChoice {
    /// Admission economics (design §4): no lanefold coverage AND the legacy
    /// fused `exec_agg_batched` arm would engage — the lane must not preempt
    /// the measured-faster fused batch drive (q3/q4-class, integration bench
    /// 2026-07-11). Deliberate refuse-set entry; shrinks as fold coverage
    /// widens.
    Refuse,
    /// Lane owns with the per-row breaker feed: no fold coverage, but no
    /// fused arm to preempt either (shapes the fused arm refuses — scalar
    /// quals, admitted projections).
    PerRow,
    /// Lane owns with the batched build feed: per-batch group probe + the
    /// lanefold whole-batch transition kernels (residual transitions
    /// per-row).
    Fold,
}

::mcx::forget_safe_nodrop!(AggLaneChoice);

/// Try to let the lane own an `Agg` over a `SeqScan` child — the fused
/// scan→filter→hash-agg push pipeline. `Some(result)` = the lane drove this
/// call; `None` = refused (the caller falls through to the existing fused /
/// per-tuple agg paths, byte-identically).
#[inline]
pub fn try_own_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !agg_over_seq_scan_fusible(agg, ss, estate)? {
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_agg_lane(agg, ss, estate)?;
            *choice = Some(c);
            c
        }
    };
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained (the hash
    // iterator is spent; re-iterating would replay groups).
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily on the first call): drain the scan pipeline
    // into the breaker sink, then finalize (delegated). `table_filled` is the
    // phase flag; a rescan rebuild clears it and re-enters here.
    if !::nodeagg::agg_hash_table_filled(agg) {
        // Arm the SoA page-batch deform + kernel-qual bitmap for the fused
        // drive when the whole read prefix is knowable (unprojected scans
        // only: with a projection the agg reads output columns, which are not
        // commensurable with scan-column prefixes). Prefix 0 disarms. The
        // fold feed FORCES the deform when the fold reads lane columns (the
        // <3-column break-even is a deform+gather artifact; the fold consumes
        // the columns directly).
        let soa_prefix = if ss.ss.ps_ProjInfo.is_none() {
            fused_agg_soa_prefix(agg, ss).unwrap_or(0)
        } else {
            0
        };
        // One OWNED tick per lane-owned hash-agg build event (the gate's
        // aggbuild floor counts builds, not calls) — fold-fed and per-row
        // feeds alike.
        stats::tick_owned(ShapeClass::AggBuild);
        if c == AggLaneChoice::Fold {
            let force = ::nodeagg::agg_lanefold_plan(agg)
                .is_some_and(|plan| !plan.cols.is_empty());
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, force);
            agg_hash_build_fold_feed(agg, ss, estate)?;
        } else {
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, false);
            let mut sink = HashAggBuildSink { agg: &mut *agg };
            drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)?;
        }
    }
    // Probe phase (every call): the breaker is now the source of pipeline
    // N+1. One qual-passing group per PG pull, in C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// The structural lane choice (see `AggLaneChoice`), decided once at the
/// first (pre-build) call. Fold-readiness = a classified lanefold plan on an
/// unprojected scan, with the SoA deform armed whenever the plan reads lane
/// columns (a plan of pure `count(*)` transitions reads none).
fn decide_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    let fold_ready = match ::nodeagg::agg_lanefold_plan(agg) {
        Some(plan) if ss.ss.ps_ProjInfo.is_none() => {
            if plan.cols.is_empty() {
                true
            } else {
                // Probe-arm the deform now so an unarmable prefix (non-fixed-
                // width column) is known BEFORE committing to ownership.
                let prefix = fused_agg_soa_prefix(agg, ss).unwrap_or(0);
                ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, true);
                ::nodeseqscan::seq_scan_batch_soa(ss).is_some()
            }
        }
        _ => false,
    };
    if fold_ready {
        return Ok(AggLaneChoice::Fold);
    }
    // Admission economics (design §4): without fold coverage the lane's
    // per-row breaker feed is strictly slower than the legacy fused batched
    // drive it would preempt (the agg hook runs first) — measured +5%
    // (q3/q4-class). Never preempt a measured-faster path.
    if crate::procnode::seq_agg_fusible(agg, ss, estate)
        && ::nodeseqscan::seq_scan_batch_supported(ss, estate)?
    {
        // One tick per memoized structural choice (the choice is decided once
        // per node and stable thereafter).
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
        return Ok(AggLaneChoice::Refuse);
    }
    Ok(AggLaneChoice::PerRow)
}

/// `LaneCols` for a fold plan that reads no lane columns (pure `count(*)`
/// transitions): the kernels never call these.
struct NoCols;

impl ::lanefold::LaneCols for NoCols {
    fn col_values(&self, _c: usize) -> &[::datum::Datum] {
        unreachable!("count(*)-only fold plans read no lane columns")
    }

    fn col_isnull(&self, _c: usize) -> &[bool] {
        unreachable!("count(*)-only fold plans read no lane columns")
    }
}

/// Build feed for the fold-armed breaker (`AggLaneChoice::Fold`): per staged
/// page batch, run the scan's per-row emit + the per-row group probe (with
/// the residual transitions inside the probe), snapshotting each row's
/// pergroup, then fold the admitted transitions whole-batch with
/// `lanefold::fold_rows_grouped`. One CHECK_FOR_INTERRUPTS per staged batch
/// (design §9 batch-operator cadence). Guarded plans re-prove every batch;
/// `Demote` runs the WHOLE batch through the checked per-row program (never
/// mixing a partial fold with per-row transitions — lanefold contract).
///
/// Byte-identity: the same rows flow through the same qual and the same
/// prepare/lookup/spill per-row machinery in the same order; only the
/// transition arithmetic is batched, and every fold kernel is commutative and
/// bit-for-bit equal to C's transition semantics (lanefold's tested
/// contract), so transvalues — and therefore output bytes — are identical.
fn agg_hash_build_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut idxs: Vec<u32> = Vec::new();
    let mut groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>> = Vec::new();
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of scan: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Guarded plans (int2-Var OpExpr admissions): prove the batch before
        // any fold. The proof runs over every staged non-fallback row — a
        // superset of the rows the fold will touch — so a Pass is sound and a
        // Demote at worst conservative (the checked per-row program is always
        // correct; it raises C's error at C's row when a selected row really
        // overflows).
        let mut demote = false;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            if plan.guarded {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("guarded fold plans read lane columns");
                let nwords = (n as usize).div_ceil(64);
                let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
                for (r, fb) in rows[..nwords].iter_mut().zip(soa.fallback_words()) {
                    *r = !fb;
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                demote = ::lanefold::check_guards(plan, soa, &rows[..nwords], |_| None)
                    == ::lanefold::GuardCheck::Demote;
            }
        }
        if demote {
            for i in 0..n {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
                }
            }
            continue;
        }
        idxs.clear();
        groups.clear();
        for i in 0..n {
            let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? else {
                continue;
            };
            // SoA fallback rows carry no lane values: the full per-row
            // program owns them (the order split across transitions is
            // bit-invisible — commutative kernels).
            if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.is_fallback(i)) {
                ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
            } else if let Some(pg) = ::nodeagg::agg_hash_build_probe_resid(agg, estate, slot)? {
                idxs.push(i);
                groups.push(pg);
            }
        }
        if !idxs.is_empty() {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            // SAFETY: `groups[k]` is the live pergroup array the probe just
            // installed for staged row `idxs[k]` (hash entries and their
            // additional blocks are allocation-stable for the table's
            // lifetime; spill mode only redirects NEW groups to the tapes —
            // spilled rows never reach `groups`); non-fallback rows carry
            // valid deformed lane values for every plan column (the SoA
            // prefix covers the evaltrans fetch bound); AvgAccum pergroups
            // hold the catalog's `{0,0}` int8[2] transarray, datum-copied per
            // group at entry initialization; guarded plans passed
            // `check_guards` above.
            match ::nodeseqscan::seq_scan_batch_soa(ss) {
                Some(soa) => unsafe {
                    ::lanefold::fold_rows_grouped(plan, soa, &idxs, &groups)
                },
                None => {
                    debug_assert!(plan.cols.is_empty());
                    unsafe { ::lanefold::fold_rows_grouped(plan, &NoCols, &idxs, &groups) }
                }
            }
        }
    }
    // Finalize (delegated): spill finish, merge handoff, phase flip.
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// Refuse-set for the lane-v2 hash-agg pipeline. Two halves:
///   * scan side: the Phase-1 `seq_scan_fusible` gate verbatim (page-batch AM,
///     uninstrumented, forward, non-parallel, non-EPQ, non-Bloom, subplan- and
///     param-free qual/projection) — WIDER than the legacy fused arm's
///     `seq_agg_fusible` (any scalar qual and any admitted projection run
///     scalar-within-lane, not just kernel quals / outer-read-free tlists);
///   * agg side: `agg_hash_breaker_admissible` (batch-drainable — no grouping
///     sets / DISTINCT-or-ordered-input / merge phase / subplan transitions —
///     AGG_HASHED, initplan-param-free). AGG_PLAIN keeps the existing fused
///     path (no breaker needed: it has no per-group read-back).
/// A post-build merge handoff flips `agg_batch_drainable` false, so later
/// calls refuse here and fall to `exec_agg`'s merged retrieve — exactly the
/// existing `exec_agg_batched` arm's cross-call behavior.
fn agg_over_seq_scan_fusible<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(false);
    }
    // A scan-side refusal ticks under the SeqScan class inside
    // `seq_scan_fusible` (memoized), so it is counted once, not re-attributed.
    seq_scan_fusible(ss, estate)
}

/// Deform prefix for the SoA page-batch deform under the fused agg drive:
/// everything the per-row consumers read from the scan slot — the agg's
/// outer-column bound (transition args + grouping columns; outer slot == scan
/// slot for unprojected scans) and the scan qual's fetch bound. None = a
/// consumer's shape is unknown; the SoA deform stays disarmed (per-row lazy
/// deform, still correct).
fn fused_agg_soa_prefix<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<i32> {
    let mut p = ::nodeagg::agg_batch_outer_prefix(agg)?;
    if let Some(q) = ss.ss.qual.as_deref() {
        p = p.max(q.max_fetch(::execexpr::SlotSrc::Scan)?);
    }
    Some(p)
}

/// The breaker as Sink of pipeline N: accept = the existing hashagg per-row
/// build (prepare/lookup + transition program, spill-mode spilling included);
/// finish = the existing finalize tail (spill finish, handoff install, phase
/// flip). Always `NeedMore` — a breaker consumes its whole input.
struct HashAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
}

impl<'mcx> Sink<'mcx> for HashAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_hash_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_hash_build_finish(self.agg, estate)
    }
}

/// The breaker as Source of pipeline N+1: produce = the existing
/// `agg_retrieve_hash_table` read-back, one final projected group row per
/// batch (the row lives in the agg's result slot — node-side, per the `Batch`
/// contract). Delegation preserves C's group output order exactly (§7's
/// pragmatic rule for this slice: same table, same iteration, same spill
/// refill → same order, so regress stays byte-comparable without the
/// annotated comparator).
struct HashAggSource;

impl<'mcx> Source<'mcx> for HashAggSource {
    type Node = ::nodeagg::AggStateData<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        Ok(::nodeagg::agg_hash_retrieve(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Pass-through operator for the probe pipeline: pushes the produced group
/// row (already finalized + projected into the result slot) to the root.
/// One-row batches never outlive the producing driver round → no cursor.
struct HashAggEmit;

impl<'mcx> Operator<'mcx> for HashAggEmit {
    type Node = ::nodeagg::AggStateData<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        Ok(match out.accept(node.ps_ResultTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Sort pipeline-breaker (Phase 2 operator→operator seam). ONE node
// implementing `Sink` for pipeline N (the feed: scan source → scalar
// filter/project → sort sink) and `Source` for pipeline N+1 (the read-back:
// sort source → RootAdapter), chained by a per-node Feed→Emit phase flag —
// which is exactly the row path's `sort_Done`, so `exec_rescan_sort` resets
// the phase (and delegates tuplesort rescan semantics) unchanged, and falling
// back to `exec_sort` at any call boundary is byte-safe (same node state).
//
// Everything delegates to the row-path `Tuplesort` (design §8: default =
// delegate finalize/read-back to the row-path state): `Sink::accept` =
// `tuplesort_puttupleslot`/`putdatum`, `Sink::finish` =
// `tuplesort_performsort`, `Source::produce` = `tuplesort_gettupleslot`/
// `getdatum` — via `nodesort`'s lane seam, over the SAME `SortState` the
// per-tuple `exec_sort` / fused `exec_sort_batched` use. Output order is
// therefore C's exactly, by construction. The feed is the Phase-1 scan
// pipeline (same sources, same per-row scalar emit) with the breaker as its
// sink instead of the root adapter, so the put sequence equals the per-tuple
// feed's — byte-identical.
// ===========================================================================

/// Try to let the lane own a `Sort` over a lane-fusible scan child. `Some` =
/// the lane drove this call; `None` = refused (caller runs the unchanged
/// `exec_sort`/`exec_sort_batched` paths — byte-safe even mid-stream, since
/// both drive the same node state).
#[inline]
pub fn try_own_sort<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic gates, every call (cheap): EPQ can engage between calls on the
    // same node tree, and only forward pulls keep the tuplesort read-back
    // cursor in step.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Backward);
        return Ok(None);
    }
    if !sort_lane_fusible_memo(s, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecSort entry.
    ::postgres_seams::check_for_interrupts::call()?;

    let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
    sort_feed_if_needed(state, &mut **outer, outer_desc, estate)?;
    // Emit phase (pipeline N+1): the breaker's Source face streams the
    // tuplesort read-back through the root pull-adapter, one tuple per call.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(state, &mut SortEmitSource, &mut SortEmit, &mut root, estate)?))
}

/// Structural sort-breaker verdict, memoized at first call: the fusibility
/// cascade must not run once per pulled tuple, and a mid-stream verdict flip
/// would desync the staged-batch cursors. Refusal accounting ticks exactly
/// here — once per memoized structural verdict (a child-scan refusal's
/// specific reason is ticked under the child's class inside its fusible
/// gate). Shared by the bare sort hook and the wave-4 chains over the sort
/// breaker (Group / Result / SubqueryScan), which admit exactly the sort
/// shapes the breaker admits.
fn sort_lane_fusible_memo<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match s.lane_fusible {
        Some(v) => Ok(v),
        None => {
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            Ok(v)
        }
    }
}

/// Feed phase of the sort breaker (pipeline N), once, lazily: drive the scan
/// pipeline to exhaustion into the breaker sink, then finalize (performsort)
/// — all inside one call, exactly like `exec_sort`'s build leg. `sort_Done`
/// is the phase flag; a rescan clears it and re-enters here. Shared by the
/// bare sort hook and the wave-4 chains over the sort breaker.
fn sort_feed_if_needed<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if state.sort_done() {
        return Ok(());
    }
    // One OWNED tick per lane-owned sort feed event (the gate's sortfeed
    // floor counts feeds, not calls).
    stats::tick_owned(ShapeClass::SortFeed);
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            sort_feed(state, ss, SeqScanSource, SeqScanFilterProject, outer_desc, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            sort_feed(state, is, IndexScanSource, IndexScanEmit, outer_desc, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => sort_feed(
            state,
            &mut **ios,
            IndexOnlyScanSource,
            IndexOnlyScanEmit,
            outer_desc,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            // The bitmap must be built before the heap drive — the same
            // setup the bitmap arm runs before offering the scan.
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            sort_feed(
                state,
                &mut b.scan,
                BitmapHeapScanSource,
                BitmapHeapScanEmit,
                outer_desc,
                estate,
            )
        }
        _ => unreachable!("memoized sort verdict admitted a non-scan child"),
    }
}

/// Structural refuse-set for the sort breaker. Sort-side: refuse
/// `randomAccess` (EXEC_FLAG_REWIND/BACKWARD/MARK at init — scrollable and
/// backward cursors plus the mergejoin-outer mark/restore protocol need
/// tuplesort random access the forward-only emit pipeline doesn't drive);
/// bounded (top-N) IS admitted — `sort_lane_begin` applies
/// ALLOWBOUNDED/set_bound exactly as `exec_sort`. Child-side: the Phase-1
/// scan refuse-sets, verbatim (the feed is the Phase-1 scan pipeline with the
/// breaker as its sink) — these also cover EXPLAIN ANALYZE, since an
/// instrumented tree wraps every node in the `Instrumented` variant, which
/// matches no scan arm. The admitted checks are all init-stable, so the
/// verdict is memoizable; the caller re-checks the dynamic EPQ/direction
/// gates per call.
fn sort_refuse_reason<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if s.state.randomAccess {
        return Ok(Some(RefuseReason::RandomAccess));
    }
    scan_child_fusible(&mut s.outer, estate)
}

/// Shared child-side gate for breakers fed by a Phase-1 scan pipeline (sort
/// and hash-join build/probe feeds): the Phase-1 scan refuse-sets, verbatim.
/// `None` = admitted; `Some(NonScanChild)` = not a lane-fusible scan node
/// type; `Some(ChildScanRefused)` = the child scan's own refuse-set refused
/// (the specific reason is ticked under the child's class inside its fusible
/// gate). These also cover EXPLAIN ANALYZE (an instrumented tree wraps every
/// node in the `Instrumented` variant, which matches no scan arm).
fn scan_child_fusible<'mcx>(
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let child_ok = match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => seq_scan_fusible(ss, estate)?,
        crate::procnode::PlanStateNode::IndexScan(is) => index_scan_fusible(is, estate),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            index_only_scan_fusible(ios, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            bitmap_heap_scan_fusible(&b.scan, estate)
        }
        _ => return Ok(Some(RefuseReason::NonScanChild)),
    };
    Ok(if child_ok {
        None
    } else {
        Some(RefuseReason::ChildScanRefused)
    })
}

/// Feed phase driver: build the tuplesort (`sort_lane_begin` — `exec_sort`'s
/// construction verbatim), then run pipeline N to exhaustion into the breaker
/// sink. Mirrors `exec_sort`'s build leg in forcing a forward child read for
/// the feed's duration (restored on success; an error aborts the query).
fn sort_feed<'mcx, S, O>(
    sort: &mut ::nodesort::SortState<'mcx>,
    scan: &mut S::Node,
    mut src: S,
    mut op: O,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    ::nodesort::sort_lane_begin(sort, outer_desc)?;
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut sink = SortBreakerSink { sort };
    drain_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
    estate.es_direction = dir;
    Ok(())
}

/// The breaker's `Sink` face (pipeline N endpoint). Holds the sort node by
/// `&mut` — the driver threads the SCAN node, so a breaker spanning two nodes
/// needs no driver rework: pipeline N's threaded node is the scan, and the
/// sort node rides in its sink.
struct SortBreakerSink<'a, 'mcx> {
    sort: &'a mut ::nodesort::SortState<'mcx>,
}

impl<'mcx> Sink<'mcx> for SortBreakerSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodesort::sort_lane_put(self.sort, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodesort::sort_lane_finish(self.sort, estate)
    }
}

/// The breaker's `Source` face (pipeline N+1): each produce streams the next
/// tuple of the tuplesort read-back into `ps_ResultTupleSlot` (one-row
/// batches, like the IndexOnlyScan source — always consumed within the
/// producing driver round, so no node-resident cursor is needed; the
/// tuplesort's own read cursor is the cross-call position).
struct SortEmitSource;

impl<'mcx> Source<'mcx> for SortEmitSource {
    type Node = ::nodesort::SortState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        // C's per-ExecSort-call CHECK_FOR_INTERRUPTS: when a chained consumer
        // (Group's duplicate skip, Result's stream) drains several sorted
        // tuples in one PG pull, C would enter ExecSort once per tuple — keep
        // that cadence here rather than once per pull (§9). Pending-gated,
        // exactly C's CHECK_FOR_INTERRUPTS macro: the unconditional seam call
        // per produced tuple measured +17% on the distinct-count shape (q9),
        // where the agg parent pulls this source once per input row.
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        Ok(::nodesort::sort_lane_next(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Push operator for the emit pipeline: pushes the staged result slot.
struct SortEmit;

impl<'mcx> Operator<'mcx> for SortEmit {
    type Node = ::nodesort::SortState<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        Ok(match out.accept(node.ps_ResultTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Hash-join pipeline breaker (Phase 2). The join spans two pipelines plus a
// mid-pipeline streaming stage:
//
//   pipeline N   (build): inner scan source → scalar filter/project →
//                         HashJoinBuildSink   (breaker Sink face)
//   pipeline N+1 (probe): outer scan source → scalar filter/project →
//                         JoinProbe (TupleOp) → sink
//
// The build side is the breaker: `accept` = the row-path per-row hash +
// `ExecHashTableInsert` (`nodehash::lane_build_accept` — spill/growth arms
// included), `finish` = the delegated build tail (`finish_build`,
// empty-build early return, `nbatch_outstart`/`dense_on`, phase flip). The
// probe side is NOT a breaker — it streams: one outer row in, 0..K joined
// rows out, with the intra-row expansion position node-resident on the
// HashJoinState (`hj_CurTuple`/`hj_CurDense` — C's own cross-call state), so
// a mid-expansion pause resumes exactly. The phase flag is `hj_JoinState`
// itself (HJ_BUILD_HASHTABLE → HJ_NEED_NEW_OUTER — C's own state machine).
//
// Spill (§8): the build delegates wholesale to the row-path table, so nbatch
// growth happens exactly as the row path's; the lane then checks the FINAL
// nbatch after the completed build and REFUSES the probe when nbatch > 1 —
// before any lane tuple is emitted, so the fallback `exec_hash_join` resumes
// from HJ_NEED_NEW_OUTER over the identical table (postponing outer tuples
// to batch files exactly as if the row path had built it). Refusing on the
// planner's initial estimate alone would be insufficient: the row path grows
// nbatch mid-build (`ExecHashIncreaseNumBatches`), so only the post-build
// value is authoritative — and checking after a fully delegated build is
// byte-safe precisely because the build is bit-equal to the row path's.
//
// Refused join shapes (assert-refuse set; each needs its own byte-verified
// staging): LEFT/RIGHT/FULL/SEMI/ANTI/RIGHT_SEMI/RIGHT_ANTI (null-fill faces
// + match-driven skips), joinqual/otherqual residuals, multi-batch (above),
// parallel hash, instrumented, subplan/param-bearing hash or projection
// exprs, non-lane-fusible scan children on either side.
// ===========================================================================

/// The breaker's `Sink` face (build pipeline endpoint). Holds the join +
/// hash nodes by `&mut` — the driver threads the inner SCAN node, so the
/// breaker spanning other nodes needs no driver rework (sort-breaker shape).
struct HashJoinBuildSink<'a, 'mcx> {
    hj: &'a mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &'a mut ::nodehash::HashState<'mcx>,
    done: Option<::nodehashjoin::LaneBuildDone>,
}

impl<'mcx> Sink<'mcx> for HashJoinBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodehash::lane_build_accept(self.hs, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.done = Some(::nodehashjoin::lane_build_finish(self.hj, self.hs, estate)?);
        Ok(())
    }
}

/// The join probe as a mid-pipeline `TupleOp`: accept stages one outer row
/// (`lane_probe_accept` — ecxt reset + hash/dense key, C's per-outer-row
/// prologue), then the expansion streams each bucket/dense-chain match
/// through the row-path recheck + projection (`lane_probe_next`) into the
/// downstream sink. Expansion position is node-resident on the join state.
struct JoinProbe<'a, 'mcx> {
    hj: &'a mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &'a mut ::nodehash::HashState<'mcx>,
}

impl<'mcx> JoinProbe<'_, 'mcx> {
    fn emit(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(j) = ::nodehashjoin::lane_probe_next(self.hj, self.hs, estate)? {
            if let SinkFeed::Full = out.accept(j, estate)? {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for JoinProbe<'_, 'mcx> {
    fn pending(&self) -> bool {
        ::nodehashjoin::lane_probe_pending(self.hj)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        ::nodehashjoin::lane_probe_accept(self.hj, self.hs, estate, tuple)?;
        self.emit(out, estate)
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        self.emit(out, estate)
    }
}

/// Build-pipeline driver, generic over the inner scan: table create
/// (delegated, bit-equal to the row path's), drain the scan pipeline into
/// the breaker sink, delegated finish. Returns the post-build verdict inputs
/// (empty / final nbatch).
fn join_build_feed<'mcx, S, O>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    scan: &mut S::Node,
    mut src: S,
    mut op: O,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodehashjoin::LaneBuildDone>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    ::nodehashjoin::lane_build_begin(hj, hs, estate)?;
    let mut sink = HashJoinBuildSink { hj, hs, done: None };
    drain_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
    Ok(sink.done.expect("build sink finished"))
}

/// Dispatch the build feed over the admitted inner-scan child types.
fn join_build_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodehashjoin::LaneBuildDone> {
    // One OWNED tick per lane-owned join build event (the gate's join floor
    // counts builds, not calls) — bare joins and agg-over-join compositions
    // alike.
    stats::tick_owned(ShapeClass::Join);
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            join_build_feed(hj, hs, ss, SeqScanSource, SeqScanFilterProject, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            join_build_feed(hj, hs, is, IndexScanSource, IndexScanEmit, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            join_build_feed(hj, hs, &mut **ios, IndexOnlyScanSource, IndexOnlyScanEmit, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            join_build_feed(hj, hs, &mut b.scan, BitmapHeapScanSource, BitmapHeapScanEmit, estate)
        }
        _ => unreachable!("memoized join verdict admitted a non-scan build child"),
    }
}

/// Probe-pipeline drain (composition): outer scan → filter/project →
/// JoinProbe → the downstream breaker sink (the agg build), to exhaustion.
fn join_probe_drain_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut probe = JoinProbe { hj, hs };
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => drain_pipeline_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => drain_pipeline_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut probe,
                sink,
                estate,
            )
        }
        _ => unreachable!("memoized join verdict admitted a non-scan outer child"),
    }
}

/// Probe-pipeline pull (bare join): one PG pull's worth through the chain
/// into the root adapter — exercising the mid-expansion pause/resume.
fn join_probe_pull_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut probe = JoinProbe { hj, hs };
    let mut root = RootAdapter::new(None);
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => pull_step_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => pull_step_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => pull_step_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            pull_step_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut probe,
                &mut root,
                estate,
            )
        }
        _ => unreachable!("memoized join verdict admitted a non-scan outer child"),
    }
}

/// Structural refuse-set for the lane hash join, memoized on the node at
/// first evaluation (verdict stability: a lane-owned join must stay
/// lane-owned — `lane_join_untouched` in the verdict guarantees the row path
/// never drove this node before the lane, and memoization guarantees the
/// lane drives it ever after). Join side: `lane_join_admissible` (INNER, no
/// residual quals, uninstrumented, subplan/param-free) + serial hash +
/// subplan/param-free build hash. Child side: the Phase-1 scan refuse-sets
/// on BOTH children. The caller re-checks the dynamic EPQ/direction gates
/// per call.
fn hash_join_lane_fusible<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = hj.lane_fusible {
        return Ok(v);
    }
    // Engagement accounting for the structural verdict ticks exactly here —
    // once per memoized decision (a child-scan refusal's specific reason is
    // ticked under the child's class inside its fusible gate). OWNED ticks
    // for the join class count build EVENTS, in `join_build_dispatch`.
    let refuse = hash_join_refuse_reason(hj, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::Join, r);
    }
    let v = refuse.is_none();
    hj.lane_fusible = Some(v);
    Ok(v)
}

/// `None` = admitted; `Some(reason)` = refused.
fn hash_join_refuse_reason<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    // Non-INNER faces, joinqual/otherqual residuals, instrumented,
    // subplan/param-bearing join exprs or projection — plus a node the row
    // path already drove (verdict stability demands whole-life ownership).
    if !::nodehashjoin::lane_join_admissible(state)
        || !::nodehashjoin::lane_join_untouched(state, hstate)
    {
        return Ok(Some(RefuseReason::JoinShape));
    }
    if hstate.parallel_state().is_some() || hstate.is_parallel_aware() {
        return Ok(Some(RefuseReason::ParallelGate));
    }
    if !::nodehash::lane_build_hash_admissible(hstate) {
        return Ok(Some(RefuseReason::SubplanParam));
    }
    if let Some(r) = scan_child_fusible(outer, estate)? {
        return Ok(Some(r));
    }
    scan_child_fusible(child, estate)
}

/// Try to let the lane own a bare `HashJoin` (no lane consumer above): build
/// pipeline once (lazily, phase = the node's own HJ_BUILD_HASHTABLE), then
/// one joined tuple per PG pull through the probe chain. `None` = refused
/// (caller runs the unchanged `exec_hash_join` — byte-safe even after a
/// lane-delegated build, which leaves exactly the row path's post-build node
/// state). The dispatch hook gates this on the legacy fused probe drive NOT
/// engaging (admission economics: never preempt the faster existing path).
#[inline]
pub fn try_own_hash_join<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Admission economics (design §4): the legacy fused probe drive already
    // owns this shape better than the v2 pipeline — never preempt the
    // measured-faster path. Per-PULL tick cadence (the dispatch arm resolves
    // the probe mode before offering the join to the lane). Parallel Hash
    // ticks its own gate.
    match hj.probe_batch.mode() {
        crate::procnode::ProbeBatchMode::Off => {}
        crate::procnode::ProbeBatchMode::Parallel => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            return Ok(None);
        }
        crate::procnode::ProbeBatchMode::Unknown | crate::procnode::ProbeBatchMode::On => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::AdmissionEconomicsFusedDrive);
            return Ok(None);
        }
    }
    // Dynamic per-call gates (mirrors the sort breaker).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Backward);
        return Ok(None);
    }
    if !hash_join_lane_fusible(hj, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecHashJoin entry.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    if ::nodehashjoin::lane_join_phase(state, hstate) == ::nodehashjoin::LaneJoinPhase::Build {
        let done = join_build_dispatch(state, hstate, child, estate)?;
        if done.empty {
            // C's empty-build early return: no output, outer never pulled.
            return Ok(Some(None));
        }
        if done.nbatch > 1 {
            // Spill refuse, before any lane tuple is emitted: the fallback
            // row path resumes from HJ_NEED_NEW_OUTER over the same table.
            stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
            return Ok(None);
        }
    } else {
        match ::nodehashjoin::lane_join_phase(state, hstate) {
            ::nodehashjoin::LaneJoinPhase::EmptyDone => return Ok(Some(None)),
            ::nodehashjoin::LaneJoinPhase::Probe => {
                if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                    stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                    return Ok(None);
                }
            }
            ::nodehashjoin::LaneJoinPhase::Build => unreachable!("handled above"),
        }
    }
    Ok(Some(join_probe_pull_dispatch(state, hstate, outer, estate)?))
}

/// Try to let the lane own `Agg(hashed) → HashJoin(inner) → scans` — the
/// first breaker-to-breaker composition. Three pipelines on two breaker
/// nodes, all phase flags node-resident row-path state:
///
///   1. build:  inner scan → filter/project → HashJoinBuildSink
///   2. probe:  outer scan → filter/project → JoinProbe → HashAggBuildSink
///   3. emit:   HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// `None` = refused (caller falls to the per-tuple `exec_agg` over
/// `exec_hash_join`, byte-identically — including after a lane-delegated
/// join build that then spill-refused).
#[inline]
pub fn try_own_agg_over_hash_join<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the join class (the composition's
    // pipelines all hang off the join's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !hash_join_lane_fusible(hj, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    if !::nodeagg::agg_hash_table_filled(agg) {
        let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
        let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
        // Join build phase (once, lazily; a rescan that rebuilt the inner
        // side re-enters here via the node's own HJ_BUILD_HASHTABLE).
        if ::nodehashjoin::lane_join_phase(state, hstate)
            == ::nodehashjoin::LaneJoinPhase::Build
        {
            let done = join_build_dispatch(state, hstate, child, estate)?;
            if !done.empty && done.nbatch > 1 {
                // Spill refuse before any lane tuple is emitted; the
                // fallback per-tuple agg over exec_hash_join resumes from
                // HJ_NEED_NEW_OUTER over the identical table.
                stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                return Ok(None);
            }
        }
        match ::nodehashjoin::lane_join_phase(state, hstate) {
            ::nodehashjoin::LaneJoinPhase::EmptyDone => {
                // Inner join over an empty build: emits nothing, and the
                // outer child is never pulled (C's early return). The agg
                // finalizes over an empty input.
                stats::tick_owned(ShapeClass::AggBuild);
                let mut sink = HashAggBuildSink { agg: &mut *agg };
                sink.finish(estate)?;
            }
            ::nodehashjoin::LaneJoinPhase::Probe => {
                if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                    stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                    return Ok(None);
                }
                // One OWNED tick per lane-owned agg build event (here the
                // build is fed by the join probe drain).
                stats::tick_owned(ShapeClass::AggBuild);
                let mut sink = HashAggBuildSink { agg: &mut *agg };
                join_probe_drain_dispatch(state, hstate, outer, &mut sink, estate)?;
            }
            ::nodehashjoin::LaneJoinPhase::Build => unreachable!("build ran above"),
        }
    }
    // Agg emit phase (every call): one qual-passing group per PG pull, in
    // C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// ===========================================================================
// Wave-4 streaming glue (Volcano-tail triage, 2026-07-12): three small
// streaming operators hosted where the lane already owns the neighboring
// pipeline — never a new layer over a refused child (admission economics,
// design §4; the Limit/Unique precedent):
//
//   Group        adjacent-row grouping over the SORT breaker's emit — a
//                mid-pipeline `TupleOp` running `exec_group`'s own per-tuple
//                body (`nodegroup::lane_group_feed`: the same
//                grouping-equality program, first-tuple copy, HAVING qual and
//                projection — reused, not reimplemented); state = the
//                node-resident first-tuple slot + have-first/grp_done flags,
//                so a Volcano fallback at any call boundary is byte-safe.
//                NOTE: Group the NODE only — AGG_SORTED / the agg breaker
//                admission are owned elsewhere (wave-4 charter split).
//   Result       the gating/projection node: `resconstantqual` evaluated
//                once (C's rs_checkqual arm, via `noderesult`'s seams), then
//                either the degenerate no-child pipeline (the single no-FROM
//                row) or the child stream projected row-by-row through a
//                `TupleOp` over the sort breaker's emit.
//   SubqueryScan a pass-through filter/project `TupleOp` over the child
//                pipeline (`execscan::lane_scan_accept` — `exec_scan_impl`'s
//                per-tuple qual/proj body, subplan/param arms included):
//                bare over the sort breaker, and spliced mid-pipeline in the
//                agg-over-subquery-over-scan composition so lane pipelines
//                chain through subquery boundaries end to end.
//
// Refused shapes (each byte-safe on the Volcano fallback): EPQ and
// non-forward pulls (dynamic gates, ticked per offered call); instrumented
// nodes (EXPLAIN ANALYZE keeps per-node counters — for the chained shapes an
// instrumented tree wraps every node so the child never matches the Sort/scan
// arms, and the Result no-FROM arm gates on the estate's instrumentation
// explicitly); any child that is not a lane-owned pipeline
// (`child-not-lane-owned`; the child's own refusal reason ticks under the
// child's class). Group/Result/SubqueryScan quals and projections run the
// nodes' OWN evaluation arms (subplan-aware where the node's Volcano body is
// — noderesult and execscan host subplans/params; nodegroup's body is reused
// verbatim), so no subplan-param refusal is needed at this layer.
// ===========================================================================

/// The Group node as a mid-pipeline streaming operator: one sorted tuple in,
/// 0..1 projected group heads out, never pends (no intra-tuple expansion) and
/// never finishes early.
struct GroupOp<'a, 'mcx> {
    group: &'a mut ::nodegroup::GroupState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for GroupOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodegroup::lane_group_feed(self.group, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("GroupOp never pends")
    }
}

/// Try to let the lane own a `Group` over the sort breaker — streaming
/// adjacent-row grouping on the sorted emit. `None` = refused; `exec_group`
/// drives the same GroupState byte-safely at any call boundary.
#[inline]
pub fn try_own_group<'mcx>(
    g: &mut ::mcx::PgBox<'mcx, crate::procnode::GroupNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (Group init asserts !BACKWARD && !MARK, so a
    // non-forward pull should be impossible — gate anyway, like the sort).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Group, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Group, RefuseReason::Backward);
        return Ok(None);
    }
    let g = &mut **g;
    // Sorted grouping's input comes from a Sort in the lane-ownable plans
    // (a presorted index path arrives as a standalone scan, which refuses
    // ownership — C's shape there IS the Volcano Group). Instrumented trees
    // wrap every node, so EXPLAIN ANALYZE never matches the Sort arm.
    let crate::procnode::PlanStateNode::Sort(s) = &mut g.outer else {
        stats::tick_refused(ShapeClass::Group, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    };
    if !sort_lane_fusible_memo(s, estate)? {
        stats::tick_refused(ShapeClass::Group, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    }
    // C's ExecGroup entry interrupt check (conditional, exactly the Volcano
    // entry's), then the drained guard, then the first child pull's ExecSort
    // entry CFI.
    ::nodegroup::lane_group_cfi()?;
    if ::nodegroup::lane_group_done(&g.state) {
        return Ok(Some(None));
    }
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    // One OWNED tick per lane-owned group drive start (= the underlying sort
    // feed event; rescan re-feeds and re-ticks, like the sortfeed class).
    if !sstate.sort_done() {
        stats::tick_owned(ShapeClass::Group);
    }
    sort_feed_if_needed(sstate, &mut **souter, outer_desc, estate)?;
    let mut op = GroupOp { group: &mut g.state };
    let mut root = RootAdapter::new(None);
    let r =
        pull_step_chain(sstate, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?;
    if r.is_none() {
        // exec_group's child-exhausted arm: the node stays drained.
        ::nodegroup::lane_group_eof(&mut g.state);
    }
    Ok(Some(r))
}

/// The Result node's per-row projection as a mid-pipeline streaming operator:
/// one child row in, exactly one projected row out (Result has no per-row
/// qual — C's ExecResult projects every child row). Never pends, never
/// finishes early.
struct ResultOp<'a, 'mcx> {
    ps: &'a mut crate::procnode::PlanStateBase<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for ResultOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // exec_result's per-call body over one pushed child row: per-tuple
        // context reset, stage the outer tuple, project (param hoist +
        // subplan-aware arm inside the seam).
        let ecxt = self.ps.ps_ExprContext.expect("ResultState without ExprContext");
        estate.reset_expr_context(ecxt);
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(tuple);
        let result = crate::noderesult::lane_result_project(self.ps, estate)?;
        Ok(match out.accept(result, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("ResultOp never pends")
    }
}

/// Try to let the lane own a `Result`: the no-FROM single-row arm (degenerate
/// no-child pipeline), or the projection stream over the sort breaker. The
/// one-time `resconstantqual` gate runs BEFORE the child is ever fed, via
/// `noderesult::lane_result_gate` — C's rs_checkqual arm verbatim, so a
/// refusal after the gate ran is still byte-safe (`exec_result` sees the
/// same consumed rs_checkqual / rs_done state its own first call would have
/// left).
#[inline]
pub fn try_own_result<'mcx>(
    rs: &mut crate::noderesult::ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Backward);
        return Ok(None);
    }
    // EXPLAIN ANALYZE refuses by policy (§4). The no-FROM arm has no child
    // whose Instrumented wrapper would break the match, so gate on the
    // estate's instrumentation table directly (non-empty exactly when the
    // plan is instrumented).
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Instrumented);
        return Ok(None);
    }
    match rs.outer.as_deref_mut() {
        None => {
            // The no-FROM row: exec_result's childless body, statement for
            // statement (entry CFI → one-time gate → per-call ctx reset →
            // drained guard → mark done + project the single row).
            crate::cfi()?;
            // One OWNED tick per lane-owned Result execution: the call that
            // consumes the gate and/or emits; the drained tail calls after it
            // don't re-tick.
            if rs.rs_checkqual || !rs.rs_done {
                stats::tick_owned(ShapeClass::ResultNode);
            }
            if rs.rs_checkqual && !crate::noderesult::lane_result_gate(rs, estate)? {
                return Ok(Some(None));
            }
            let ecxt = rs.ps.ps_ExprContext.expect("ResultState without ExprContext");
            estate.reset_expr_context(ecxt);
            if rs.rs_done {
                return Ok(Some(None));
            }
            rs.rs_done = true;
            Ok(Some(Some(crate::noderesult::lane_result_project(&mut rs.ps, estate)?)))
        }
        Some(crate::procnode::PlanStateNode::Sort(_)) => {
            // Child admission BEFORE any state effect. (Instrumented trees
            // wrap every node, so an instrumented Sort never matches — the
            // estate gate above already refused anyway.)
            {
                let Some(crate::procnode::PlanStateNode::Sort(s)) = rs.outer.as_deref_mut()
                else {
                    unreachable!("matched above")
                };
                if !sort_lane_fusible_memo(s, estate)? {
                    stats::tick_refused(ShapeClass::ResultNode, RefuseReason::ChildNotLaneOwned);
                    return Ok(None);
                }
            }
            // exec_result entry: CFI, then the one-time gate (C evaluates it
            // before the child is ever pulled; false = the sort is never fed).
            crate::cfi()?;
            if rs.rs_checkqual && !crate::noderesult::lane_result_gate(rs, estate)? {
                return Ok(Some(None));
            }
            if rs.rs_done {
                return Ok(Some(None));
            }
            let crate::noderesult::ResultState { ps, outer, .. } = rs;
            let Some(crate::procnode::PlanStateNode::Sort(s)) = outer.as_deref_mut() else {
                unreachable!("matched above")
            };
            // C's first child pull enters ExecSort: entry CFI, then the feed.
            ::postgres_seams::check_for_interrupts::call()?;
            let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
            // One OWNED tick per lane-owned Result child-feed event.
            if !sstate.sort_done() {
                stats::tick_owned(ShapeClass::ResultNode);
            }
            sort_feed_if_needed(sstate, &mut **souter, outer_desc, estate)?;
            let mut op = ResultOp { ps };
            let mut root = RootAdapter::new(None);
            Ok(Some(pull_step_chain(
                sstate,
                &mut SortEmitSource,
                &mut SortEmit,
                &mut op,
                &mut root,
                estate,
            )?))
        }
        Some(_) => {
            stats::tick_refused(ShapeClass::ResultNode, RefuseReason::ChildNotLaneOwned);
            Ok(None)
        }
    }
}

/// The SubqueryScan node as a mid-pipeline streaming operator: one subplan
/// row in, 0..1 filtered/projected rows out, via `execscan::lane_scan_accept`
/// — `exec_scan_impl`'s per-tuple qual/projection body (subplan/param arms
/// included), over the same node state (`ss_ScanTupleSlot` repointed at the
/// subplan's slot exactly as `SubqueryNext` does). Never pends, never
/// finishes early.
struct SubqueryScanOp<'a, 'mcx> {
    ss: &'a mut ::execscan::ScanState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for SubqueryScanOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // exec_scan_fetch's conditional per-tuple interrupt check (§9: same
        // cadence as the per-tuple driver this replaces).
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        // SubqueryNext: the subplan's slot goes to the driver uncopied.
        self.ss.ss_ScanTupleSlot = tuple;
        match ::execscan::lane_scan_accept(self.ss, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("SubqueryScanOp never pends")
    }
}

/// Try to let the lane own a bare `SubqueryScan` over the sort breaker —
/// the pass-through filter/project stream on the sorted emit. `None` =
/// refused; `exec_scan` drives the same ScanState byte-safely.
#[inline]
pub fn try_own_subquery_scan<'mcx>(
    s: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates. EPQ substitutes test tuples in the fetch
    // (exec_scan_epq); the lane refuses it wholesale (§4).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Backward);
        return Ok(None);
    }
    let s = &mut **s;
    if s.ss.instr_idx.is_some() {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    let crate::procnode::PlanStateNode::Sort(sort) = &mut *s.subplan else {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    };
    if !sort_lane_fusible_memo(sort, estate)? {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    }
    // C's first fetch: exec_scan_fetch's conditional CFI runs per tuple in
    // the TupleOp; the subplan pull enters ExecSort — entry CFI here.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = sort;
    // One OWNED tick per lane-owned feed event (the child sort feed).
    if !sstate.sort_done() {
        stats::tick_owned(ShapeClass::SubqueryScan);
    }
    sort_feed_if_needed(sstate, &mut **souter, outer_desc, estate)?;
    // End-of-stream mirrors exec_scan_impl's projected-slot clear.
    let clear_on_finish = s.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut op = SubqueryScanOp { ss: &mut s.ss };
    let mut root = RootAdapter::new(clear_on_finish);
    Ok(Some(pull_step_chain(
        sstate,
        &mut SortEmitSource,
        &mut SortEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Feed pipeline for the agg-over-subquery composition: lane scan source →
/// scalar filter/project → SubqueryScanOp (pass-through filter/project) →
/// the hash-agg breaker sink, to exhaustion — dispatched over the admitted
/// scan child types (join-probe-drain shape).
fn subquery_feed_drain_dispatch<'mcx>(
    sqs: &mut crate::procnode::SubqueryScanNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let crate::procnode::SubqueryScanNode { ss, subplan } = sqs;
    let mut op = SubqueryScanOp { ss };
    match &mut **subplan {
        crate::procnode::PlanStateNode::SeqScan(ss2) => drain_pipeline_chain(
            ss2,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            &mut op,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => {
            drain_pipeline_chain(is, &mut IndexScanSource, &mut IndexScanEmit, &mut op, sink, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut op,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut op,
                sink,
                estate,
            )
        }
        _ => unreachable!("composition admitted a non-scan subquery child"),
    }
}

/// Try to let the lane own `Agg(hashed) → SubqueryScan → scan` — lane
/// pipelines chaining through a subquery boundary. Two pipelines on one
/// breaker node:
///
///   1. build: scan source → filter/project → SubqueryScanOp → HashAggBuildSink
///   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// The agg reads the SUBQUERY's output slot (not the scan slot), so the
/// lanefold/SoA fold feed does not apply — the build is the per-row breaker
/// feed. No admission-economics refuse is needed: the legacy fused
/// `exec_agg_batched` arms never match a SubqueryScan outer, so there is no
/// faster drive to preempt. `None` = refused (the caller falls to the
/// per-tuple `exec_agg` over `exec_scan`, byte-identically).
#[inline]
pub fn try_own_agg_over_subquery_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sqs: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the subqueryscan class (the
    // composition's feed hangs off the subquery's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    let sqs = &mut **sqs;
    if sqs.ss.instr_idx.is_some() {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    // The subquery's child must be a lane-fusible scan (the Phase-1 refuse-
    // sets, verbatim; the specific child reason ticks under its class).
    if let Some(r) = scan_child_fusible(&mut sqs.subplan, estate)? {
        stats::tick_refused(ShapeClass::SubqueryScan, r);
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily): drain the scan → subquery chain into the
    // breaker sink, then finalize (delegated). `table_filled` is the phase
    // flag; a rescan rebuild clears it and re-enters here.
    if !::nodeagg::agg_hash_table_filled(agg) {
        // One OWNED tick per lane-owned build event, on both classes the
        // event engages (aggbuild counts builds; subqueryscan counts feeds).
        stats::tick_owned(ShapeClass::AggBuild);
        stats::tick_owned(ShapeClass::SubqueryScan);
        {
            let mut agg_sink = HashAggBuildSink { agg: &mut *agg };
            subquery_feed_drain_dispatch(sqs, &mut agg_sink, estate)?;
        }
        // End-of-scan parity with exec_scan_impl: the projected slot is
        // cleared when the subquery's stream ends (byte-invisible; keeps the
        // node state identical to the per-tuple driver's).
        if let Some(p) = sqs.ss.ps_ProjInfo.as_ref() {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(p.pi_result_slot), mcx);
        }
    }
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// ===========================================================================
// Append hosting (wave 5, 2026-07-12): the serial Append as a lane
// concatenation point — the node's OWN `exec_append` body drives, verbatim
// (subplan choice, `as_begun`, runtime pruning via `choose_next_subplan_
// locally`/`identify_valid_subplans`, and the conditional per-fetch CFI are
// all C's, reused not reimplemented — the wave-4 house rule); only the
// `fetch_subplan` closure changes, pulling one tuple per fetch from the
// CHILD's lane pipeline (`pull_step` over the Phase-1 scan stages) instead of
// `exec_proc_node`. Child N's pipeline exhausting returns `None` to
// `exec_append`, which advances to child N+1 — C's exact
// child-EOF-then-advance order for free. Each child's cross-call position
// (staged page batch + cursor) is node-resident, so the one-tuple-per-pull
// Volcano boundary is safe, and each child's output slot goes to the parent
// exactly as `exec_append` would hand it (Append projects nothing; children
// with differing physical descs already carry their own planner-installed
// projections, which run inside the child pipelines — byte-identical).
//
// Refuse-set (each byte-safe on the Volcano fallback):
//   * parallel Append (Leader/Worker choosers over the shared DSM claim
//     table) — non-serial subplan order; the lane refuses anything not
//     provably ordering-identical serially (`lane_choose_local`). Ticked per
//     offered call (the mode is worker/DSM-init-assigned).
//   * async-capable subplans — unported (`exec_init_append` panics), so no
//     gate is needed; recorded here for the C-diff reader.
//   * dynamic EPQ / non-forward pulls (§4 model-incompatible; per call).
//   * any child that is not a lane-fusible Phase-1 scan
//     (`scan_child_fusible`, verbatim — the child's specific refusal reason
//     ticks under the child's class). v1 policy: MIXED children refuse the
//     WHOLE Append — a per-child owned/Volcano split would need per-child
//     verdict pinning across the shared `exec_append` drive for no measured
//     upside; future work when a real mixed shape shows up.
//
// Runtime partition pruning is ADMITTED: the pruning arms run inside the
// reused `exec_append`/`choose_next_subplan_locally` body itself, so the
// subplan order is C's by construction. The structural verdict conservatively
// probes ALL initialized children (a superset of what pruning may run) —
// probing opens each child scan's descriptor once, which C's lazy first-pull
// open would skip for pruned/LIMIT-cut children: pgstat-only divergence,
// same accepted class as the hash-join build-side probe (design §9 F5).
// ===========================================================================

/// One PG pull's worth from a lane-owned scan child pipeline — the
/// `fetch_subplan` face of the hosted Append (join_probe_pull_dispatch's
/// shape, without a mid-pipeline op). The child's staged batch + cursor are
/// node-resident, so consecutive fetches resume exactly.
fn lane_scan_pull_dispatch<'mcx>(
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            // End-of-stream mirrors ExecScanExtended's projected-slot clear
            // (try_own_seq_scan's shape).
            let clear_on_finish = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
            let mut root = RootAdapter::new(clear_on_finish);
            pull_step(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut root, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            let mut root = RootAdapter::new(None);
            pull_step(is, &mut IndexScanSource, &mut IndexScanEmit, &mut root, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            let mut root = RootAdapter::new(None);
            pull_step(&mut **ios, &mut IndexOnlyScanSource, &mut IndexOnlyScanEmit, &mut root, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            let mut root = RootAdapter::new(None);
            pull_step(&mut b.scan, &mut BitmapHeapScanSource, &mut BitmapHeapScanEmit, &mut root, estate)
        }
        _ => unreachable!("memoized append verdict admitted a non-scan child"),
    }
}

/// Structural Append verdict, memoized on the node at first offer (verdict
/// stability: a lane-driven child carries a staged-batch cursor across the
/// Volcano boundary, so ownership must not flip mid-stream; the child scan
/// verdicts are themselves memoized). ALL children must pass the Phase-1
/// scan refuse-sets — mixed children refuse the whole Append (v1 policy,
/// module doc). Owned accounting ticks exactly here — once per memoized
/// admission (per Append node per (re)init, the seqscan class cadence).
fn append_lane_fusible_memo<'mcx>(
    a: &mut crate::procnode::AppendNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = a.lane_fusible {
        return Ok(v);
    }
    let mut refuse: Option<RefuseReason> = None;
    for child in a.substates.iter_mut() {
        if let Some(r) = scan_child_fusible(child, estate)? {
            refuse = Some(r);
            break;
        }
    }
    match refuse {
        None => stats::tick_owned(ShapeClass::Append),
        Some(r) => stats::tick_refused(ShapeClass::Append, r),
    }
    let v = refuse.is_none();
    a.lane_fusible = Some(v);
    Ok(v)
}

/// Try to let the lane own a serial `Append` over lane-fusible scan children.
/// `Some` = the lane drove this call (via the node's own `exec_append` body
/// over lane child pipelines); `None` = refused (caller runs the unchanged
/// `exec_append` over `exec_proc_node` children, byte-identically).
#[inline]
pub fn try_own_append<'mcx>(
    a: &mut ::mcx::PgBox<'mcx, crate::procnode::AppendNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirror the sort/join breakers). Backward local
    // Append pulls exist in C only under BACKWARD eflags, which the children
    // refuse structurally (ScrollMark) — gate anyway.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Append, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Append, RefuseReason::Backward);
        return Ok(None);
    }
    let a = &mut **a;
    // Parallel Append: the Leader/Worker choosers claim subplans through the
    // shared DSM table in a non-serial order — Volcano keeps it. The mode is
    // assigned at DSM/worker init (before the node's first pull), but gate
    // per call (one flag load) rather than memoize an init-order assumption.
    if !::nodeappend::lane_choose_local(&a.state) {
        stats::tick_refused(ShapeClass::Append, RefuseReason::ParallelGate);
        return Ok(None);
    }
    if !append_lane_fusible_memo(a, estate)? {
        return Ok(None);
    }
    let crate::procnode::AppendNode { state, substates, .. } = a;
    Ok(Some(::nodeappend::exec_append(state, estate, |e, i| {
        lane_scan_pull_dispatch(&mut substates[i], e)
    })?))
}

// ===========================================================================
// ProjectSet: DOCUMENTED WHOLESALE REFUSE (wave-5 evaluation, 2026-07-12).
//
// Verdict: do NOT host. The SRF tlist expansion is per-tuple stateful in
// three ways the lane would have to carry, for zero engagement:
//   * the multi-call protocol itself — `pending_srf_tuples` resumes a
//     half-emitted expansion across `exec_proc_node` calls, `args_valid`
//     pins evaluated arg datums across those calls (query-context armed),
//     and `elemdone` tracks per-element ExprMultipleResult state;
//   * SFRM_Materialize mode parks the whole set in a tuplestore read back
//     one row per call — a second, per-element cross-call cursor;
//   * `ExecProjectSRF` interleaves per-tuple context resets between (not
//     within) expansions — a batched drive would need the exact reset
//     points replayed to keep by-ref datum lifetimes identical.
// An expanding-`TupleOp` hosting (the join-probe pause/resume shape over
// `pending_srf_tuples`) is model-compatible in principle, but it could only
// chain over a lane-owned child pipeline — and ProjectSet children in
// practice are bare scans, which refuse standalone ownership (admission
// economics, STANDALONE_SCAN_NO_UPSIDE), so the hook would engage nowhere.
// Reusing `exec_project_set`'s own body per-tuple would add a lane layer
// over a refused child — exactly the shape §4's economics forbid. Refuse,
// and re-evaluate when the design's "SRFs = expanding operator" phase item
// lands (design doc §4 "Everything else is hostable, staged deliberately").
// ===========================================================================

/// Tick the documented ProjectSet wholesale refuse (module doc above; the
/// `project_set_arm` dispatch hook calls this and always falls through to
/// the unchanged `exec_project_set`).
#[inline]
pub fn refuse_project_set() {
    stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::SrfSetExpansion);
}
