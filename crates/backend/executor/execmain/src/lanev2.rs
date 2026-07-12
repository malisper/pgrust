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

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use push::{
    feed_pipeline, pull_step, Batch, OpStatus, Operator, RootAdapter, Sink, SinkFeed, Source,
};

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
/// call, EXPLAIN ANALYZE (instrumented), parallel scan, the Bloom/EPQ
/// variants, and AMs without page-batch support.
fn seq_scan_fusible<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !ss.batch_allowed()
        || ss.is_parallel()
        || ss.ss.instr_idx.is_some()
    {
        return Ok(false);
    }
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain
        | ::nodeseqscan::SeqScanVariant::WithQual
        | ::nodeseqscan::SeqScanVariant::WithProject
        | ::nodeseqscan::SeqScanVariant::WithQualProject => {}
        ::nodeseqscan::SeqScanVariant::PlainBloom | ::nodeseqscan::SeqScanVariant::Epq => {
            return Ok(false)
        }
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        if q.has_subplan() || !q.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    if let Some(p) = ss.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    // AM must support the page-batch primitives (opens the scan desc once).
    ::nodeseqscan::seq_scan_batch_supported(ss, estate)
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
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !is.batch_allowed()
        || is.iss_ParallelAware
        || is.ss.instr_idx.is_some()
    {
        return false;
    }
    // Same-block tidrun batching is only sound under an MVCC snapshot (matches
    // the fused-agg gate; non-MVCC keeps the per-tuple path).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return false;
    }
    is.ss.qual.is_none()
        && is.ss.ps_ProjInfo.is_none()
        && is.iss_Runtime.is_none()
        && is.iss_OrderBy.is_none()
        && ::types_scan::sdir::ScanDirectionIsForward(is.iss_OrderDir)
        && is
            .iss_RelationDesc
            .as_ref()
            .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
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
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !ios.batch_allowed()
        || ios.ioss_ParallelAware
        || ios.ss.instr_idx.is_some()
    {
        return false;
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return false;
    }
    ios.ss.qual.is_none()
        && ios.ss.ps_ProjInfo.is_none()
        && ios.ioss_Runtime.is_none()
        && ios.ioss_OrderByKeys.is_empty()
        && ::types_scan::sdir::ScanDirectionIsForward(ios.ioss_OrderDir)
        && ios
            .ioss_RelationDesc
            .as_ref()
            .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
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
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || bhs.parallel_aware
        || bhs.pstate.is_some()
        || bhs.ss.instr_idx.is_some()
    {
        return false;
    }
    if bhs
        .bitmapqualorig
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty())
    {
        return false;
    }
    bhs.ss.qual.is_none() && bhs.ss.ps_ProjInfo.is_none()
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
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(None);
    }
    // Structural verdict, memoized at first call: the fusibility cascade must
    // not run once per pulled tuple, and a mid-stream verdict flip would
    // desync the staged-batch cursors.
    let fusible = match s.lane_fusible {
        Some(v) => v,
        None => {
            let v = sort_fusible(s, estate)?;
            s.lane_fusible = Some(v);
            v
        }
    };
    if !fusible {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecSort entry.
    ::postgres_seams::check_for_interrupts::call()?;

    let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
    if !state.sort_done() {
        // Feed phase (pipeline N): drive the scan pipeline to exhaustion into
        // the breaker sink, then finalize (performsort) — all inside this one
        // call, exactly like `exec_sort`'s build leg.
        let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
        match &mut **outer {
            crate::procnode::PlanStateNode::SeqScan(ss) => {
                sort_feed(state, ss, SeqScanSource, SeqScanFilterProject, outer_desc, estate)?
            }
            crate::procnode::PlanStateNode::IndexScan(is) => {
                sort_feed(state, is, IndexScanSource, IndexScanEmit, outer_desc, estate)?
            }
            crate::procnode::PlanStateNode::IndexOnlyScan(ios) => sort_feed(
                state,
                &mut **ios,
                IndexOnlyScanSource,
                IndexOnlyScanEmit,
                outer_desc,
                estate,
            )?,
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
                )?
            }
            _ => unreachable!("memoized sort verdict admitted a non-scan child"),
        }
    }
    // Emit phase (pipeline N+1): the breaker's Source face streams the
    // tuplesort read-back through the root pull-adapter, one tuple per call.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(state, &mut SortEmitSource, &mut SortEmit, &mut root, estate)?))
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
fn sort_fusible<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if s.state.randomAccess {
        return Ok(false);
    }
    match &mut *s.outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => seq_scan_fusible(ss, estate),
        crate::procnode::PlanStateNode::IndexScan(is) => {
            Ok(index_scan_fusible(is, estate))
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            Ok(index_only_scan_fusible(ios, estate))
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            Ok(bitmap_heap_scan_fusible(&b.scan, estate))
        }
        _ => Ok(false),
    }
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
    feed_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
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
