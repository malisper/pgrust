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

/// Engagement trace (verification aid, no perf path): `PGRUST_LANE_V2_TRACE=1`
/// logs lane engagement events to stderr. Resolved once per process.
fn lane_trace(event: &str) {
    static ON: OnceLock<bool> = OnceLock::new();
    if *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lane-v2] {event}");
    }
}

// ===========================================================================
// Phase-3 qual kernel: the vectorized selection-bitmap qual for lane-owned
// filtered scans. This restores the fast path the NON-lane `WithQual` drive
// already has (`scan_batch_probe` → `exec_seq_scan_batch`): a kernel-shaped
// `col CMP const` qual (`Kernel::QualScanVarCmpConst`) is evaluated over the
// whole staged page batch by `qual_bitmap_cmp_const` (execexpr/steps.rs —
// chunked so LLVM can vectorize the compare) into a selection bitmap, and the
// lane's filter/project segment iterates ONLY the survivors instead of
// running `exec_qual` scalar per staged row. All of the staging + bitmap +
// forced-fallback-bit machinery is the EXISTING `BatchSoa` flow in
// `nodeseqscan` (`seq_scan_batch_soa_prepare` / `seq_scan_next_pagebatch` /
// `seq_scan_batch_fetch`); the lane only arms it and consumes the bitmap.
// ===========================================================================

/// Arm the SoA deform + selection-bitmap qual for a lane-owned filtered
/// SeqScan pipeline. Admission is exactly `scan_batch_probe`'s: the qual must
/// be kernel-shaped (`QualScanVarCmpConst` — non-erroring, non-volatile by
/// construction, which is why only kernel shapes are admitted), and
/// `seq_scan_batch_soa_prepare` internally refuses a non-fixed-width column
/// prefix (the scalar per-row path then continues unchanged). `qual_only`:
/// the deform stages the qual column only; surviving rows deform lazily
/// per-row — identical to the non-lane `exec_seq_scan_batch` drive. No-op
/// (memo hit) when already armed, so per-pull callers pay one load+test.
fn arm_seq_scan_qual_bitmap<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ctx: &str,
) {
    if ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
        return;
    }
    let Some(q) = ss.ss.qual.as_deref() else { return };
    let ::execexpr::Kernel::QualScanVarCmpConst { attnum, .. } = q.kernel() else {
        return;
    };
    ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, attnum as i32 + 1, true, false);
    if ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
        lane_trace(&format!("seqscan qual bitmap armed ({ctx})"));
    }
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
// it PREEMPTS the row executor's own fused SoA-bitmap WithQual drive.
//
// Revisited with the Phase-3 qual kernel (2026-07-11): lane-owned filtered
// scans now carry the same selection bitmap, but for a STANDALONE scan the
// incumbent per-node drive is `exec_seq_scan_batch` — the identical bitmap
// over the identical staging, with NO pull-adapter round trip per surviving
// row on top. The lane can therefore only match-or-lose here (the q9-class
// adapter overhead stands), so the refuse stays. It shrinks when standalone
// scans gain a kernel the row drive lacks (dict/PREWHERE-class); the scan
// pipelines stay fully exercised via the agg/sort/join breaker feeds.
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
/// WithProject / WithQualProject over a page-batch-supporting AM.
/// Subplan- and param-bearing quals/projections are admitted (Phase 2):
/// `seq_scan_batch_emit` now runs `exec_scan_impl`'s exact arms for them —
/// pending-initplan param evaluation before the qual (and before the
/// projection, only on qual-passing rows), and the suspension-driven
/// `exec_qual_with_subplans` / `exec_project_with_subplans` drivers — so
/// initplan params demand-evaluate identically and correlated subplans run
/// scalar-per-batch-row through the same `nodesubplan` machinery.
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

/// Push operator: the scan's filter→project segment. Consumes the staged
/// batch via `seq_scan_batch_emit` — `ExecScanExtended`'s body over a staged
/// batch row (reset per-tuple context, store + apply the scan qual via
/// `execexpr`, project) — pushing each surviving output slot into the sink.
/// Kernel-shaped quals (`QualScanVarCmpConst`, armed by
/// `arm_seq_scan_qual_bitmap` or the agg's fused full-prefix deform) run
/// vectorized: the staging computed a whole-batch selection bitmap
/// (`qual_bitmap_cmp_const`), and this operator walks only the survivors;
/// all other quals run scalar per-row. Filter and projection stay fused
/// within this one segment operator per the operator-model decision (design
/// §1): the push conversion inverts driver control, never the fused per-row
/// segment. Same tuples, same order, same qual/proj/NULL semantics as
/// `exec_seq_scan` → BYTE-IDENTICAL.
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
        // Phase-3 qual kernel: when the kernel-shaped qual bitmap is staged
        // for this batch (`seq_scan_next_pagebatch` ran `qual_bitmap_cmp_const`
        // over the SoA qual column at staging), iterate ONLY the selection
        // survivors — bitmap hits plus forced fallback bits, which
        // `seq_scan_batch_fetch` re-checks per-row inside the emit — instead
        // of running the scalar qual on every staged row. Survivors come out
        // in ascending row order: same rows, same order, same per-row
        // emit/projection semantics as the scalar walk → byte-identical (the
        // kernel is non-erroring/non-volatile by admission, so skipped rows
        // have no observable evaluation). The bitmap cursor is node-resident,
        // so a `Paused` pipeline resumes exactly; `lane_cursor` is kept in
        // step for `pending`. Interrupt cadence: one check per survivor and
        // at least one per staged page (no coarser than the page-level check
        // in `heap_fetch_next_buffer` the incumbent batch drive relies on).
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(node) {
            loop {
                ::postgres_seams::check_for_interrupts::call()?;
                let Some(i) = ::nodeseqscan::seq_scan_batch_next_selected(node) else {
                    node.set_lane_cursor(batch.n, batch.n);
                    return Ok(OpStatus::NeedInput);
                };
                node.set_lane_cursor(i + 1, batch.n);
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(node, estate, i)? {
                    if let SinkFeed::Full = out.accept(slot, estate)? {
                        return Ok(OpStatus::Paused);
                    }
                }
            }
        }
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
    /// 2026-07-11). Re-measured with the Phase-3 qual bitmap (2026-07-12):
    /// the lane's per-row breaker feed is STILL slower than the fused arm at
    /// q4's 50% selectivity (+2.5%; only ~-5% at 10% selectivity) — the
    /// dominant cost is the per-row `agg_hash_build_accept` vs the fused
    /// arm's batched drive, which carries the same bitmap. Deliberate
    /// refuse-set entry; shrinks as fold coverage widens.
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
    // AGG_PLAIN (ungrouped) routes to the fold drive: no breaker needed (a
    // single group has no per-group read-back — feed + finalize is the whole
    // node inside one call), but the same staged-batch fold applies, with
    // `lanefold::fold_batch` (the ungrouped kernel, CSE included) in place of
    // the grouped probe+fold.
    if ::nodeagg::agg_plain_fold_admissible(agg) {
        return try_own_plain_agg_over_seq_scan(agg, ss, choice, estate);
    }
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
    agg_seq_scan_build_if_needed(agg, ss, c, estate)?;
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
        // Phase-3 qual kernel: with the selection bitmap staged for this
        // batch, walk ONLY the survivors (bitmap hits + forced fallback bits,
        // re-checked per-row inside the emit) — same rows, same ascending
        // order as the full walk, whose emit would have bit-tested each row
        // anyway. Non-kernel quals keep the full per-row walk.
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(ss) {
            while let Some(i) = ::nodeseqscan::seq_scan_batch_next_selected(ss) {
                agg_fold_feed_row(agg, ss, estate, &mut idxs, &mut groups, i)?;
            }
        } else {
            for i in 0..n {
                agg_fold_feed_row(agg, ss, estate, &mut idxs, &mut groups, i)?;
            }
        }
        if !idxs.is_empty() {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            let aggcx = ::nodeagg::agg_aggcontext(agg);
            // SAFETY: `groups[k]` is the live pergroup array the probe just
            // installed for staged row `idxs[k]` (hash entries and their
            // additional blocks are allocation-stable for the table's
            // lifetime; spill mode only redirects NEW groups to the tapes —
            // spilled rows never reach `groups`); non-fallback rows carry
            // valid deformed lane values for every plan column (the SoA
            // prefix covers the evaltrans fetch bound); AvgAccum pergroups
            // hold the catalog's `{0,0}` int8[2] transarray, datum-copied per
            // group at entry initialization; Int128AvgAccum pergroups are
            // NULL or hold the aggcontext state the transfn chain installed,
            // and `aggcx` is that same aggcontext; guarded plans passed
            // `check_guards` above.
            match ::nodeseqscan::seq_scan_batch_soa(ss) {
                Some(soa) => unsafe {
                    ::lanefold::fold_rows_grouped(plan, soa, &idxs, &groups, aggcx)?
                },
                None => {
                    debug_assert!(plan.cols.is_empty());
                    unsafe {
                        ::lanefold::fold_rows_grouped(plan, &NoCols, &idxs, &groups, aggcx)?
                    }
                }
            }
        }
    }
    // Finalize (delegated): spill finish, merge handoff, phase flip.
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// One staged row of the fold build feed: the per-row emit (per-tuple ctx
/// reset, store, qual), then route — SoA fallback rows to the full per-row
/// transition program (they carry no lane values; the order split across
/// transitions is bit-invisible — commutative kernels), everything else
/// through the group probe with its pergroup snapshotted for the whole-batch
/// fold.
fn agg_fold_feed_row<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    i: u32,
) -> PgResult<()> {
    let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? else {
        return Ok(());
    };
    if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.is_fallback(i)) {
        ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
    } else if let Some(pg) = ::nodeagg::agg_hash_build_probe_resid(agg, estate, slot)? {
        idxs.push(i);
        groups.push(pg);
    }
    Ok(())
}

// ===========================================================================
// Plain-agg (AGG_PLAIN, ungrouped) fold drive — the q2-class
// `SELECT sum(a), avg(b), count(*) FROM t [WHERE ...]` shapes. SIMPLER than
// the hashed breaker: one group, no probe — each staged batch folds straight
// into the single pergroup array via `lanefold::fold_batch` (the ungrouped
// kernel, CSE schedule included), and the retrieve side is the delegated
// `plain_finish` (finalize + HAVING + project, one row, zero-row contract
// included). The whole node runs inside one `exec_proc_node` call, exactly
// like `exec_agg`'s single-group arm.
// ===========================================================================

/// Try to let the lane own an AGG_PLAIN `Agg` over a `SeqScan` child with the
/// batched fold. `Some(result)` = the lane drove this call; `None` = refused
/// (the caller falls through to the fused `exec_agg_batched` / per-tuple
/// paths, byte-identically).
fn try_own_plain_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Scan-side refuse-set: the Phase-1 gate verbatim (dynamic EPQ/direction
    // gates re-checked per call; structural verdict memoized on the node).
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_plain_agg_lane(agg, ss, estate)?;
            *choice = Some(c);
            c
        }
    };
    if c != AggLaneChoice::Fold {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: the one result row is out; a drained agg
    // stays drained until rescan clears `agg_done`.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // One OWNED tick per lane-owned plain-agg build event (the gate's
    // aggbuild floor counts builds, not calls; a plain node builds once per
    // (re)scan — this drive runs the whole feed inside one call).
    stats::tick_owned(ShapeClass::AggBuild);
    agg_plain_fold_feed(agg, ss, estate)?;
    // Retrieve (delegated): finalize + HAVING + project — one row (or none,
    // when the var-free HAVING rejects it), setting `agg_done`.
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// The structural lane choice for an AGG_PLAIN Agg over a SeqScan, decided
/// once at the first call. Fold or Refuse only — the lane never takes plain
/// shapes per-row: the incumbent legacy fused `exec_agg_batched` drive is
/// already batched with per-row transitions, so a per-row lane feed has
/// nothing to win (admission economics, design §4).
fn decide_plain_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    // Every Refuse below is admission economics (§4): the legacy fused
    // `exec_agg_batched` drive (or the per-tuple path) already owns the shape
    // at least as well as a lane feed could. One tick per memoized per-node
    // choice.
    let refuse = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
        Ok(AggLaneChoice::Refuse)
    };
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else {
        return refuse();
    };
    // Projected scans: the agg reads output columns, which are not
    // commensurable with scan-column prefixes — no SoA lane feed (the hashed
    // breaker's scoping, verbatim).
    if ss.ss.ps_ProjInfo.is_some() {
        return refuse();
    }
    // count(*)-only plans read no lane columns: the incumbent fused drive
    // already advances those per batch with zero per-row work (the storeless
    // advance / `qualifying_count` bitmap census), so a fold cannot beat it.
    // Deliberate refuse-set entry.
    if plan.cols.is_empty() {
        return refuse();
    }
    // Probe-arm the deform now so an unarmable prefix (non-fixed-width
    // column) is known BEFORE committing: the plain fold reads the SoA lanes
    // directly, so a disarmed deform keeps the incumbent.
    let prefix = fused_agg_soa_prefix(agg, ss).unwrap_or(0);
    ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, true);
    if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
        return refuse();
    }
    Ok(AggLaneChoice::Fold)
}

/// Feed for the plain fold drive: per staged page batch, compose the row
/// selection and fold the admitted transitions whole-batch with
/// `lanefold::fold_batch` into the single pergroup array. One
/// CHECK_FOR_INTERRUPTS per staged batch (design §9 batch-operator cadence).
/// Guarded plans re-prove every batch; `Demote` runs the WHOLE batch through
/// the checked per-row program (lanefold contract).
///
/// Two per-batch modes:
///   * bitmap: no residual transitions and the qual is absent or staged as
///     the kernel-qual bitmap — the selection is `sel & !fallback` (or
///     `!fallback` with no qual) with NO per-row work for deformed rows (the
///     fold reads the SoA lanes; a per-row emit would only store a slot
///     nothing reads). Forced fallback rows re-check the qual per-row and run
///     the full per-row program off the stored tuple.
///   * per-row emit: a scalar qual and/or residual transitions — the scan's
///     per-row emit applies the qual; surviving deformed rows join the fold
///     selection (+ the residual program per row), fallback rows run the
///     full per-row program.
///
/// Byte-identity: the same rows pass the same qual (the staged bitmap IS the
/// kernel qual's verdict; fallback rows re-run the per-row check), and every
/// fold kernel is commutative and bit-for-bit equal to C's transition
/// semantics on admitted/guard-proven data (lanefold's tested contract), so
/// the single group's transvalues — and the finalized output row — are
/// identical.
fn agg_plain_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Same one-deform staging as the hashed fold feed; the deform is FORCED
    // (count(*)-only plans were refused, so the fold always reads lane
    // columns). Re-preparing with the same shape is a no-op.
    let soa_prefix = fused_agg_soa_prefix(agg, ss).unwrap_or(0);
    ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, true);
    // initialize_aggregates (delegated): fresh initval pergroups; a rescan
    // re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg)?;
    let has_resid =
        ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.resid.is_empty());
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
        let nwords = (n as usize).div_ceil(64);
        // Guarded plans: prove the batch over every staged non-fallback row —
        // a superset of the rows the fold will touch — before any fold (same
        // soundness argument as the hashed fold feed).
        let mut demote = false;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            if plan.guarded {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("plain fold plans read lane columns");
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
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                }
            }
            continue;
        }
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        let bitmap_qual = ::nodeseqscan::seq_scan_batch_qual_sel(ss).is_some();
        if !has_resid && (bitmap_qual || ss.ss.qual.is_none()) {
            let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
            {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("plain fold plans read lane columns");
                let fb = soa.fallback_words();
                let sel = ::nodeseqscan::seq_scan_batch_qual_sel(ss);
                for w in 0..nwords {
                    rows[w] = sel.map_or(!fb[w], |s| s[w] & !fb[w]);
                    fallback[w] = fb[w];
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                    fallback[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
            }
            for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
                while bits != 0 {
                    let i = (w as u32) * 64 + bits.trailing_zeros();
                    bits &= bits - 1;
                    if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                        ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                    }
                }
            }
        } else {
            for i in 0..n {
                let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? else {
                    continue;
                };
                if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.is_fallback(i))
                {
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                } else {
                    rows[(i / 64) as usize] |= 1u64 << (i % 64);
                    if has_resid {
                        ::nodeagg::agg_plain_build_accept_resid(agg, estate, slot)?;
                    }
                }
            }
        }
        if rows[..nwords].iter().any(|w| *w != 0) {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            let aggcx = ::nodeagg::agg_aggcontext(agg);
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("plain fold plans read lane columns");
            // SAFETY: pergroup_base is the node's once-allocated single-group
            // pergroup array covering every transno (initialize_aggregates
            // just wrote it); selected rows are non-fallback, carrying valid
            // deformed lane values for every plan column (the SoA prefix
            // covers the evaltrans fetch bound); AvgAccum pergroups hold the
            // catalog's {0,0} int8[2] transarray, datum-copied at
            // initialize_aggregates; Int128AvgAccum pergroups are NULL or
            // hold the aggcontext state the fold/transfn chain installed, and
            // `aggcx` is that same aggcontext; guarded plans passed
            // `check_guards` above.
            unsafe {
                ::lanefold::fold_batch(
                    plan,
                    soa,
                    &rows[..nwords],
                    n as usize,
                    ::nodeagg::agg_plain_pergroup_base(agg),
                    aggcx,
                )?;
            }
        }
    }
    Ok(())
}

/// Build phase of the hash-agg breaker over a SeqScan feed (once, lazily on
/// the first call), with the choice-dependent feed: drain the scan pipeline
/// into the breaker sink — the lanefold whole-batch feed for
/// `AggLaneChoice::Fold`, the per-row breaker feed otherwise — then finalize
/// (delegated). `table_filled` is the phase flag; a rescan rebuild clears it
/// and re-enters here. Shared by the bare agg hook above and the
/// Limit-over-agg chain (`try_own_limit`), so both drive the identical build.
fn agg_seq_scan_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    c: AggLaneChoice,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert_ne!(c, AggLaneChoice::Refuse);
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(());
    }
    // Arm the SoA page-batch deform + kernel-qual bitmap for the fused
    // drive when the whole read prefix is knowable (unprojected scans
    // only: with a projection the agg reads output columns, which are not
    // commensurable with scan-column prefixes). ONE deform serves both
    // consumers: `seq_scan_batch_soa_prepare` detects the kernel qual
    // inside the prefix and arms the selection bitmap on the same staged
    // SoA the fold lanes read. When no prefix is knowable (projected /
    // shape-unknown), fall back to the qual-only bitmap arm so a
    // kernel-shaped filter still vectorizes (survivors deform lazily
    // per-row). The fold feed FORCES the deform when the fold reads lane
    // columns (the <3-column break-even is a deform+gather artifact; the
    // fold consumes the columns directly).
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
        if soa_prefix > 0 {
            let force = ::nodeagg::agg_lanefold_plan(agg)
                .is_some_and(|plan| !plan.cols.is_empty());
            let was = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, force);
            if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
                // Full-prefix deform unarmable (non-fixed-width column in
                // the prefix) or declined (break-even). A column-reading
                // fold plan cannot get here — `decide_agg_lane` probe-armed
                // this exact prefix before choosing Fold — so the SoA has
                // no fold reader and the qual-only bitmap arm is safe.
                arm_seq_scan_qual_bitmap(ss, estate, "agg fold feed, qual-only");
            } else if !was && ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
                lane_trace("seqscan qual bitmap armed (agg fold fused deform)");
            }
        } else {
            // Fold with no knowable prefix = a plan reading no lane
            // columns (count(*)-only); the bitmap is the only SoA user.
            arm_seq_scan_qual_bitmap(ss, estate, "agg fold feed");
        }
        agg_hash_build_fold_feed(agg, ss, estate)
    } else {
        if soa_prefix > 0 {
            let was = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, false);
            if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
                // Unarmable/declined full prefix; the per-row feed reads
                // no SoA columns, so fall back to the qual-only bitmap.
                arm_seq_scan_qual_bitmap(ss, estate, "agg per-row feed, qual-only");
            } else if !was && ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
                lane_trace("seqscan qual bitmap armed (agg per-row fused deform)");
            }
        } else {
            arm_seq_scan_qual_bitmap(ss, estate, "agg per-row feed");
        }
        let mut sink = HashAggBuildSink { agg };
        drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)
    }
}

/// Refuse-set for the lane-v2 hash-agg pipeline. Two halves:
///   * scan side: the Phase-1 `seq_scan_fusible` gate verbatim (page-batch AM,
///     uninstrumented, forward, non-parallel, non-EPQ, non-Bloom; subplan- and
///     param-bearing quals/projections run scalar-within-lane via
///     `seq_scan_batch_emit`'s hosted arms) — WIDER than the legacy fused arm's
///     `seq_agg_fusible` (any scalar qual and any admitted projection run
///     scalar-within-lane, not just kernel quals / outer-read-free tlists);
///   * agg side: `agg_hash_breaker_admissible` (batch-drainable — no grouping
///     sets / DISTINCT-or-ordered-input / merge phase / subplan transitions —
///     AGG_HASHED, initplan-param-free). AGG_PLAIN routes to the fold drive
///     above (`try_own_plain_agg_over_seq_scan`) before this gate runs.
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
/// would desync the staged-batch cursors. Shared by the bare sort hook and
/// the Limit/Unique-over-sort chains (which admit exactly the sort shapes the
/// breaker admits).
fn sort_lane_fusible_memo<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match s.lane_fusible {
        Some(v) => Ok(v),
        None => {
            // Refusal accounting ticks exactly here — once per memoized
            // structural verdict (a child-scan refusal's specific reason is
            // ticked under the child's class inside its fusible gate).
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
/// bare sort hook and the Limit/Unique-over-sort chains.
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
            arm_seq_scan_qual_bitmap(ss, estate, "sort feed");
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
        // (Unique dedup, Limit's offset skip) drains several sorted tuples in
        // one PG pull, C would enter ExecSort once per tuple — keep that
        // cadence here rather than once per pull (§9).
        ::postgres_seams::check_for_interrupts::call()?;
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
// Sorted-agg (AGG_SORTED) streaming operator (Phase-2 breadth). NOT a
// breaker: input arrives sorted on the grouping keys, so the node emits a
// finalized group at each key boundary and never needs the whole input — it
// sits as a mid-pipeline `TupleOp` between a lane-owned ordered feed and the
// root pull-adapter:
//
//   Agg over Sort (two chained pipelines on the sort breaker):
//     pipeline N   : scan source → filter/project → SortBreakerSink
//     pipeline N+1 : SortEmitSourceCfi → SortEmit → SortedAggOp → RootAdapter
//   Agg over IndexScan / IndexOnlyScan (order from the index, one pipeline):
//     IndexScanSource → IndexScanEmit → SortedAggOp → RootAdapter
//
// The lane owns ONLY control flow; ALL semantics delegate to the row-path
// nodeagg seam (`agg_sorted_*`): the group-boundary comparison is the ported
// grouping-equality ExprState (NULL keys group together through it), the
// per-row transition program, and the finalize/HAVING/project tail are
// `agg_retrieve_sorted`'s own pieces over the node's own persort state
// (first/pending slots + `have_pending`). Because the seam maintains exactly
// the pull loop's node state — every call boundary has the current group
// closed and at most a pending boundary tuple saved — a per-call fallback to
// `exec_agg` (dynamic gates) is byte-safe in both directions.
//
// Per-tuple laziness holds: the capacity-one root buffers the boundary
// group's row, pausing the pipeline (the child feed advances only to the
// boundary tuple, which is saved in the pending slot before the pause).
// End-of-stream uses the driver's `TupleOp::source_exhausted` hook (the
// Finished-vs-more-phases seam) to finalize the last
// open group; `agg_done` (set exactly where the pull loop sets it) makes the
// drained node stay drained.
//
// v1 is deliberately per-row (correctness first): the ordered feeds emit
// one-row batches (sort read-back) or short TID runs, so there is no clean
// whole-batch group-run fold seam here yet; the lanefold `fold_rows_grouped`
// batching over contiguous group runs is a later, measured step.
// ===========================================================================

/// The sorted-agg streaming operator. `group_open` is call-local by
/// construction: the only pauses are group-row emissions (capacity-one root),
/// after which the group is already closed — so at every PG call boundary the
/// open-group flag is false and the cross-call resume state is entirely the
/// node's own `have_pending`/`agg_done`.
struct SortedAggOp<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    group_open: bool,
}

impl<'mcx> SortedAggOp<'_, 'mcx> {
    /// Start the next group from the saved pending boundary tuple.
    fn begin_from_pending(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_sorted_group_begin(self.agg, estate, None)?;
        self.group_open = true;
        Ok(())
    }
}

impl<'mcx> TupleOp<'mcx> for SortedAggOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        // A saved boundary tuple whose group has not started: the resume
        // after the pause that delivered the previous group's row.
        !self.group_open && ::nodeagg::agg_sorted_have_pending(self.agg)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        if !self.group_open {
            // First row of the stream (or after a HAVING-rejected tail): the
            // group prologue — copy, initialize, first transition.
            debug_assert!(!::nodeagg::agg_sorted_have_pending(self.agg));
            ::nodeagg::agg_sorted_group_begin(self.agg, estate, Some(tuple))?;
            self.group_open = true;
            return Ok(OpStatus::NeedInput);
        }
        if ::nodeagg::agg_sorted_same_group(self.agg, estate, tuple)? {
            ::nodeagg::agg_sorted_accept(self.agg, estate, tuple)?;
            return Ok(OpStatus::NeedInput);
        }
        // Group boundary: save the boundary row first (the pull loop's
        // order), then finalize + HAVING + project the completed group.
        ::nodeagg::agg_sorted_save_pending(self.agg, estate, tuple)?;
        self.group_open = false;
        match ::nodeagg::agg_sorted_emit(self.agg, estate)? {
            Some(row) => match out.accept(row, estate)? {
                SinkFeed::Full => Ok(OpStatus::Paused),
                // Non-root sinks (none wired today): start the next group
                // immediately, as the pull loop's next iteration would.
                SinkFeed::NeedMore => {
                    self.begin_from_pending(estate)?;
                    Ok(OpStatus::NeedInput)
                }
            },
            // HAVING rejected the group: no output row; start the next group
            // from the pending boundary tuple (the pull loop's `continue`).
            None => {
                self.begin_from_pending(estate)?;
                Ok(OpStatus::NeedInput)
            }
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // The paused emit already delivered its row; resuming means starting
        // the next group from the saved boundary tuple, then asking for more
        // input.
        debug_assert!(self.pending());
        self.begin_from_pending(estate)?;
        Ok(OpStatus::NeedInput)
    }

    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Input exhausted: agg_done first (the pull loop's fetch-None arms),
        // then finalize the last open group, if any. Zero input rows emit
        // nothing (C's sorted-agg contract — unlike AGG_PLAIN).
        ::nodeagg::agg_sorted_input_done(self.agg);
        if !self.group_open {
            return Ok(OpStatus::Finished);
        }
        self.group_open = false;
        match ::nodeagg::agg_sorted_emit(self.agg, estate)? {
            Some(row) => match out.accept(row, estate)? {
                SinkFeed::Full => Ok(OpStatus::Paused),
                SinkFeed::NeedMore => Ok(OpStatus::Finished),
            },
            None => Ok(OpStatus::Finished),
        }
    }
}

/// Sort read-back source for the sorted-agg emit chain: `SortEmitSource`
/// plus C's per-fetch CHECK_FOR_INTERRUPTS — each row the agg pulls from the
/// sort is one `ExecSort` call in the per-tuple path, which checks at entry
/// (the bare-sort pipeline's equivalent check lives at `try_own_sort`'s
/// entry, once per pull).
struct SortEmitSourceCfi;

impl<'mcx> Source<'mcx> for SortEmitSourceCfi {
    type Node = ::nodesort::SortState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        ::postgres_seams::check_for_interrupts::call()?;
        Ok(::nodesort::sort_lane_next(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Try to let the lane own `Agg(AGG_SORTED) → Sort → scan`: the sort breaker
/// feeds once (pipeline N), then the sorted-agg operator streams the
/// read-back into one group row per PG pull. `None` = refused (caller falls
/// to the per-tuple `exec_agg` over `exec_sort`, byte-safely — see the
/// section doc on call-boundary state compatibility).
#[inline]
pub fn try_own_sorted_agg_over_sort<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirror the bare-sort breaker).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Backward);
        return Ok(None);
    }
    // Agg-side admission (static shape; ticked per offered call, the hashed
    // breaker's AggNotDrainable cadence).
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // Sort-side structural verdict — the bare-sort arm's memo, shared (the
    // refusal ticks once per node whichever arm probes first).
    let fusible = match s.lane_fusible {
        Some(v) => v,
        None => {
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            v
        }
    };
    if !fusible {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
    if !state.sort_done() {
        // C's CHECK_FOR_INTERRUPTS at the feed call's ExecSort entry (the
        // emit chain's source checks per subsequent fetch).
        ::postgres_seams::check_for_interrupts::call()?;
        // One OWNED tick per lane-owned sort feed event, plus one per
        // lane-owned sorted-agg stream start (both count feed/build EVENTS,
        // once per (re)scan).
        stats::tick_owned(ShapeClass::SortFeed);
        stats::tick_owned(ShapeClass::AggBuild);
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
    // Emit phase (every call): sort read-back → sorted-agg operator → root,
    // one qual-passing group row per PG pull.
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        state,
        &mut SortEmitSourceCfi,
        &mut SortEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Try to let the lane own `Agg(AGG_SORTED) → IndexScan` (index order feeds
/// the grouping directly — no Sort node). Engagement accounting: the
/// per-pull indexscan class ticks (owned per admitted feed decision, the
/// class's documented cadence); agg-side refusals tick AggNotDrainable per
/// offered call.
#[inline]
pub fn try_own_sorted_agg_over_index_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // Child refuse-set verbatim (dynamic EPQ/direction gates included; ticks
    // under the indexscan class, per call).
    if !index_scan_fusible(is, estate) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        is,
        &mut IndexScanSource,
        &mut IndexScanEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Try to let the lane own `Agg(AGG_SORTED) → IndexOnlyScan`. Accounting as
/// the IndexScan arm (per-pull indexonlyscan class).
#[inline]
pub fn try_own_sorted_agg_over_index_only_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !index_only_scan_fusible(ios, estate) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        ios,
        &mut IndexOnlyScanSource,
        &mut IndexOnlyScanEmit,
        &mut op,
        &mut root,
        estate,
    )?))
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
// Admitted join types: all eight — INNER, LEFT, SEMI, ANTI plus the
// right-fill family RIGHT, FULL, RIGHT_SEMI, RIGHT_ANTI — with
// joinqual/otherqual residuals evaluated scalar-within-lane through the
// row path's exact `eval_probe_qual` (LEFT/FULL/ANTI null-fill emits happen
// inside `lane_probe_next`'s HJ_FILL_OUTER_TUPLE arm, exactly where C emits
// them). The right-fill types (`hj_fill_inner` — RIGHT/FULL/RIGHT_ANTI) add
// a post-exhaustion phase: when the outer source ends, the probe TupleOp
// becomes a SOURCE of never-matched build tuples (C's HJ_FILL_INNER_TUPLES
// via the driver's `source_exhausted` seam; the walk delegates to the
// row path's exact `ExecScanHashTableForUnmatched` port, so the fill
// emission order is C's bucket order for free; the cursor is C's own
// node-resident `hj_CurBucketNo`/`hj_CurTuple`, so a LIMIT pause mid-fill
// resumes exactly). RIGHT_SEMI needs no fill phase — only the has-match
// skip in the probe arm. Refused join shapes (assert-refuse set):
// multi-batch (above), parallel hash, instrumented, subplan/param-bearing
// hash, residual-qual or projection exprs, non-lane-fusible scan children
// on either side.
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
        let s = self.emit(out, estate)?;
        // A resumed fill scan that just drained is terminal: the driver must
        // not fall through to another source produce — a pulled-past-end
        // heap scan RESTARTS (C never re-pulls a child after NULL).
        if s == OpStatus::NeedInput && ::nodehashjoin::lane_join_finished(self.hj) {
            return Ok(OpStatus::Finished);
        }
        Ok(s)
    }

    /// Outer exhausted: the right-fill types (`hj_fill_inner` —
    /// RIGHT/FULL/RIGHT_ANTI) flip into the unmatched-BUILD fill scan
    /// (C's HJ_FILL_INNER_TUPLES, sequenced exactly where C enters it:
    /// after the probe fully ends) and become a source of null-extended
    /// unmatched inner tuples into the same sink. The prep is idempotent
    /// (no-op unless the join sits at HJ_NEED_NEW_OUTER), the fill cursor
    /// is C's own node-resident `hj_CurBucketNo`/`hj_CurTuple`, and a
    /// mid-fill pause (`Paused`) resumes through the ordinary
    /// `pending()`/`resume()` protocol. Non-fill types emit nothing here.
    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        ::nodehashjoin::lane_fill_inner_prep(self.hj);
        Ok(match self.emit(out, estate)? {
            // The fill scan is drained (or there never was one): nothing
            // further will ever be produced.
            OpStatus::NeedInput => OpStatus::Finished,
            s => s,
        })
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
            arm_seq_scan_qual_bitmap(ss, estate, "join build feed");
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
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_seq_scan_qual_bitmap(ss, estate, "join probe drain");
            drain_pipeline_chain(
                ss,
                &mut SeqScanSource,
                &mut SeqScanFilterProject,
                &mut probe,
                sink,
                estate,
            )
        }
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
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            // Per-pull entry: `arm_seq_scan_qual_bitmap` early-returns once
            // armed (one load+test), and the first pull arms BEFORE any
            // batch is staged, so a staged batch always matches its bitmap.
            arm_seq_scan_qual_bitmap(ss, estate, "join probe pull");
            pull_step_chain(
                ss,
                &mut SeqScanSource,
                &mut SeqScanFilterProject,
                &mut probe,
                &mut root,
                estate,
            )
        }
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
/// lane drives it ever after). Join side: `lane_join_admissible`
/// (all eight join types, subplan/param-free residual quals admitted,
/// uninstrumented, subplan/param-free hash + projection exprs) + serial hash
/// + subplan/param-free build hash. Child side: the Phase-1 scan refuse-sets
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
    // Instrumented, subplan/param-bearing join exprs or projection (all
    // eight join types + residuals are admitted since lane-v2-jointypes /
    // lane-v2-rightjoin) — plus a node the row path already drove (verdict
    // stability demands whole-life ownership).
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

// ===========================================================================
// NestLoop hosting (the deferred §4 bundle). The join is a mid-pipeline
// streaming `TupleOp` — NOT a breaker: one outer row in, 0..K joined rows
// out.
//
//   pipeline: outer scan source → scalar filter/project → NestLoopProbe
//             (TupleOp) → sink (RootAdapter, or the hash-agg breaker)
//
// Per accepted outer row the op runs C's need-new-outer arm
// (`nodenestloop::lane_accept_outer`): bind the outer tuple, assign the
// join's exec params (nestParams → PARAM_EXEC slots), and RESCAN the inner
// child; the expansion then streams each inner row through the joinqual /
// otherqual / projection (`lane_probe_next` = `exec_nest_loop`'s own loop
// body, LEFT/SEMI/ANTI arms included). The INNER child stays a Volcano
// child, driven per-row through the same `NestLoopChild` calls the row path
// uses (scalar-within-lane, per the design's allowance) — so exec-param-
// driven runtime keys on an inner index scan are evaluated in
// `exec_rescan_index_scan`'s preamble exactly as C's ExecReScan path does,
// AUTOMATICALLY. The Phase-1 lane scan gates therefore KEEP refusing runtime
// keys (`iss_Runtime`/`ioss_Runtime`) for LANE-OWNED scans: that relaxation
// belongs to the inner-as-lane-pipeline follow-up, where the lane would have
// to drive the rescan preamble itself. Expansion position across the Volcano
// pull boundary is the node's own `nl_NeedNewOuter`/`nl_MatchedOuter` — C's
// cross-call state; no new fields.
//
// Admission economics (design §4): NestLoop per-tuple in Volcano is already
// cheap; the lane's value is OWNERSHIP CONTINUITY — the outer side stays a
// lane pipeline feeding breakers above. The hooks engage (a) under the
// hash-agg breaker (`try_own_agg_over_nest_loop` — a lane consumer above,
// no fused competitor exists for this shape) and (b) bare
// (`try_own_nest_loop`) where the outer is a lane-fusible scan the join
// pipeline then owns — the bare hash-join precedent; there is no legacy
// fused NestLoop drive to preempt. Refused (assert-refuse set):
// instrumented, subplan/param-bearing joinqual/otherqual/projection,
// row-path-touched nodes (verdict stability), non-lane-fusible outer
// children, EPQ, non-forward. The inner child is unconstrained — it runs
// the identical Volcano calls at the identical points either way.
// ===========================================================================

/// The NestLoop join as a mid-pipeline `TupleOp`: accept stages one outer
/// row (param assignment + inner rescan — C's per-outer-row prologue), then
/// the expansion streams the inner drain through the row-path joinqual /
/// projection arms into the downstream sink. Expansion position is
/// node-resident on the join state (`nl_NeedNewOuter`).
struct NestLoopProbe<'a, 'mcx> {
    nl: &'a mut ::nodenestloop::NestLoopState<'mcx>,
    inner: &'a mut crate::procnode::PlanStateNode<'mcx>,
}

impl<'mcx> NestLoopProbe<'_, 'mcx> {
    fn emit(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(j) = ::nodenestloop::lane_probe_next(self.nl, self.inner, estate)? {
            if let SinkFeed::Full = out.accept(j, estate)? {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for NestLoopProbe<'_, 'mcx> {
    fn pending(&self) -> bool {
        ::nodenestloop::lane_probe_pending(self.nl)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // One OWNED tick per accepted outer row — the unit the lane owns
        // (bind params -> rescan the inner -> drain the expansion).
        stats::tick_owned(ShapeClass::NestLoop);
        ::nodenestloop::lane_accept_outer(self.nl, self.inner, estate, tuple)?;
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

/// One PG pull through outer scan → filter/project → `top` → root adapter,
/// dispatched over the admitted lane-scan child types (join_probe dispatch
/// shape, generic over the mid-pipeline op).
fn scan_chain_pull_dispatch<'mcx>(
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    top: &mut dyn TupleOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut root = RootAdapter::new(None);
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => pull_step_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => pull_step_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => pull_step_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            top,
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
                top,
                &mut root,
                estate,
            )
        }
        _ => unreachable!("memoized lane verdict admitted a non-scan outer child"),
    }
}

/// Full drain of outer scan → filter/project → `top` → breaker sink, same
/// dispatch as `scan_chain_pull_dispatch`.
fn scan_chain_drain_dispatch<'mcx>(
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    top: &mut dyn TupleOp<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => drain_pipeline_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => drain_pipeline_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            top,
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
                top,
                sink,
                estate,
            )
        }
        _ => unreachable!("memoized lane verdict admitted a non-scan outer child"),
    }
}

/// Structural refuse-set for the lane NestLoop, memoized on the node at
/// first evaluation (verdict stability: a lane-owned join must stay
/// lane-owned — `lane_nest_loop_untouched` in the verdict guarantees the row
/// path never drove this node before the lane, and memoization guarantees
/// the lane drives it ever after). Join side: `lane_nest_loop_admissible`
/// (all four ported join types; uninstrumented; subplan/param-free quals +
/// projection). Outer side: the Phase-1 scan refuse-sets. The INNER side is
/// deliberately unconstrained — it stays a Volcano child driven through the
/// identical `NestLoopChild` calls. The caller re-checks the dynamic
/// EPQ/direction gates per call.
fn nest_loop_lane_fusible<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = nl.lane_fusible {
        return Ok(v);
    }
    // Engagement accounting for the structural verdict ticks exactly here —
    // once per memoized decision (a child-scan refusal's specific reason is
    // ticked under the child's class inside its fusible gate). OWNED ticks
    // for the nestloop class count accepted OUTER ROWS, in
    // `NestLoopProbe::accept`.
    let refuse = nest_loop_refuse_reason(nl, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::NestLoop, r);
    }
    let v = refuse.is_none();
    nl.lane_fusible = Some(v);
    Ok(v)
}

/// `None` = admitted; `Some(reason)` = refused.
fn nest_loop_refuse_reason<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let crate::procnode::NestLoopNode { state, outer, .. } = nl;
    // Instrumented, subplan/param-bearing joinqual/otherqual/projection —
    // plus a node the row path already drove (verdict stability demands
    // whole-life ownership).
    if !::nodenestloop::lane_nest_loop_admissible(state)
        || !::nodenestloop::lane_nest_loop_untouched(state, estate)
    {
        return Ok(Some(RefuseReason::JoinShape));
    }
    scan_child_fusible(outer, estate)
}

/// Try to let the lane own a bare `NestLoop` (no lane consumer above): one
/// joined tuple per PG pull through the chain, the mid-inner-drain position
/// riding C's own `nl_NeedNewOuter` across the pull boundary. `None` =
/// refused (caller runs the unchanged `exec_nest_loop` — byte-safe: an
/// untouched-only verdict means the row path owns the node's whole life).
#[inline]
pub fn try_own_nest_loop<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirrors the sort/hash-join breakers).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Backward);
        return Ok(None);
    }
    if !nest_loop_lane_fusible(nl, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecNestLoop entry.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::NestLoopNode { state, outer, inner, .. } = nl;
    let mut probe = NestLoopProbe { nl: state, inner };
    Ok(Some(scan_chain_pull_dispatch(outer, &mut probe, estate)?))
}

/// Try to let the lane own `Agg(hashed) → NestLoop → lane outer scan` (the
/// inner stays Volcano): two pipelines on one breaker node —
///
///   1. build: outer scan → filter/project → NestLoopProbe → HashAggBuildSink
///   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// `None` = refused (caller falls to the per-tuple `exec_agg` over
/// `exec_nest_loop`, byte-identically).
#[inline]
pub fn try_own_agg_over_nest_loop<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the nestloop class (the
    // composition's feed pipeline hangs off the join's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !nest_loop_lane_fusible(nl, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily; a rescan rebuild clears `table_filled` and
    // re-enters — the whole-NestLoop rescan resets `nl_NeedNewOuter` and the
    // outer scan's staged cursor, so the feed restarts coherently).
    if !::nodeagg::agg_hash_table_filled(agg) {
        // One OWNED tick per lane-owned agg build event (here the build is
        // fed by the NestLoop expansion drain).
        stats::tick_owned(ShapeClass::AggBuild);
        let crate::procnode::NestLoopNode { state, outer, inner, .. } = nl;
        let mut probe = NestLoopProbe { nl: state, inner };
        let mut sink = HashAggBuildSink { agg: &mut *agg };
        scan_chain_drain_dispatch(outer, &mut probe, &mut sink, estate)?;
    }
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// Try to let the lane own `Agg(hashed) → HashJoin(admitted type) → scans`
/// — the first breaker-to-breaker composition. Three pipelines on two breaker
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
    if !agg_hash_join_build_if_needed(agg, hj, estate)? {
        return Ok(None);
    }
    // Agg emit phase (every call): one qual-passing group per PG pull, in
    // C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// Build phases of the agg-over-join composition (join build, then the probe
/// drain into the agg breaker sink), once, lazily. `Ok(false)` = multi-batch
/// spill refuse — the caller must refuse ownership; no lane tuple has been
/// emitted, so the fallback per-tuple agg over `exec_hash_join` resumes from
/// HJ_NEED_NEW_OUTER over the identical table. Shared by the bare
/// composition hook above and the Limit-over-agg chain (`try_own_limit`).
fn agg_hash_join_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(true);
    }
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
            return Ok(false);
        }
    }
    match ::nodehashjoin::lane_join_phase(state, hstate) {
        ::nodehashjoin::LaneJoinPhase::EmptyDone => {
            // A non-fill-outer join (INNER/SEMI/RIGHT/RIGHT_SEMI/
            // RIGHT_ANTI) over an empty build: emits nothing — an empty
            // build has no unmatched inner tuples to fill either — and
            // the outer child is never pulled (C's early return;
            // LEFT/FULL/ANTI never take this phase — their empty build
            // proceeds to the probe and null-fills). The agg finalizes
            // over an empty input.
            stats::tick_owned(ShapeClass::AggBuild);
            let mut sink = HashAggBuildSink { agg };
            sink.finish(estate)?;
        }
        ::nodehashjoin::LaneJoinPhase::Probe => {
            if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                return Ok(false);
            }
            // One OWNED tick per lane-owned agg build event (here the
            // build is fed by the join probe drain).
            stats::tick_owned(ShapeClass::AggBuild);
            let mut sink = HashAggBuildSink { agg };
            join_probe_drain_dispatch(state, hstate, outer, &mut sink, estate)?;
        }
        ::nodehashjoin::LaneJoinPhase::Build => unreachable!("build ran above"),
    }
    Ok(true)
}

// ===========================================================================
// Streaming Limit + Unique (Phase-2 breadth): mid-pipeline `TupleOp`s at the
// TOP of an already-lane-owned chain, engaged ONLY where the lane owns the
// child pipeline (admission economics, design §4): a Volcano Limit/Unique is
// already cheap per-tuple and PG's pull already stops a lane pipeline lazily,
// so ownership here buys chain continuity (no per-tuple root adapter between
// the breaker emit and the limit/dedup — and future within-pipeline fusion),
// never a new layer over a refused child.
//
//   Limit  (Pattern 2, DuckDB streaming limit): counts in the node's own
//          LimitState (lstate/position — C's cross-call state, so a Volcano
//          fallback at any call boundary is byte-safe), delivers the boundary
//          tuple via `Paused`, reports `Finished` on the next driver round —
//          the source is never pulled past the boundary tuple's batch, and
//          quals/projections are never evaluated past the limit (C's LIMIT
//          stops calling its child). OFFSET tuples are pulled + discarded,
//          exactly as C's LIMIT_RESCAN skip loop pulls them.
//   Unique (over the sort breaker): adjacent-dedup streaming op — one sorted
//          tuple in, 0..1 group heads out, via `nodeunique::lane_unique_feed`
//          (the SAME grouping-equality program + prev-slot copy exec_unique
//          runs — reused, not reimplemented).
//
// Row-identity note (LIMIT without ORDER BY): C returns whichever rows its
// plan yields first. The lane's owned pipelines emit C's rows in C's order BY
// CONSTRUCTION — scan pipelines walk the same pages/TID runs in the same
// order, and breaker read-backs delegate to the same tuplesort / hash-table
// retrieves — so the lane's first k tuples are C's first k tuples,
// byte-identically (verified by the full regress off/on comparison).
//
// Refused shapes (each byte-safe on the Volcano fallback):
//   * LIMIT ... WITH TIES — needs boundary-tuple retention + the sort-peer
//     equality walk (LIMIT_WINDOWEND_TIES); staged later. (PG's Limit node
//     has no percent-limit form — nothing to gate.)
//   * Limit/Unique over a BARE scan — the scan hooks themselves refuse
//     standalone ownership (per-tuple emission through the pull adapter with
//     no batch consumer above = pure adapter overhead); a Volcano
//     Limit/Unique over the refused scan IS C's shape, so taking ownership
//     adds a layer with no consumer benefit.
//   * Limit over a bare HashJoin — needs a two-TupleOp chain driver
//     (JoinProbe → LimitOp); staged with the next chain generalization.
//   * Backward/scrollable cursors — a scrollable/backward cursor forces
//     randomAccess on the Sort child (refused by `sort_fusible`), Limit
//     never sees EXEC_FLAG_MARK (init assert), Unique never sees
//     BACKWARD/MARK (init assert); the dynamic direction gate refuses any
//     non-forward pull.
//   * Hashed DISTINCT is NOT here: the planner emits Agg (AGG_HASHED, zero
//     aggregates), which the hash-agg breaker already admits
//     (`agg_hash_breaker_admissible` — evaltrans is an empty transition
//     program, subplan- and param-free trivially).
// ===========================================================================

/// The Limit node as a mid-pipeline streaming operator. All window
/// arithmetic delegates to `nodelimit`'s lane seam, which mirrors
/// `exec_limit`'s forward COUNT arms verbatim over the same node state.
struct LimitOp<'a, 'mcx> {
    limit: &'a mut ::nodelimit::LimitState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for LimitOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        // Window complete (the boundary tuple was already delivered via
        // `Paused`): the next driver round must resume() → `Finished`
        // BEFORE the source is pulled again — the Paused-then-Finished rule.
        ::nodelimit::lane_limit_window_done(self.limit)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodelimit::lane_limit_feed(self.limit, tuple) {
            ::nodelimit::LaneLimitFeed::Skip => Ok(OpStatus::NeedInput),
            ::nodelimit::LaneLimitFeed::Emit => Ok(match out.accept(tuple, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
            ::nodelimit::LaneLimitFeed::EmitBoundary => {
                // Paused-then-Finished (`OpStatus::Finished` contract):
                // deliver the boundary tuple now; pending()/resume() report
                // Finished on the next driver round. The downstream sink is
                // always the capacity-one root here (LimitOp tops the chain),
                // so accept necessarily returns Full.
                let fed = out.accept(tuple, estate)?;
                debug_assert_eq!(fed, SinkFeed::Full, "limit chain must end at the root adapter");
                let _ = fed;
                Ok(OpStatus::Paused)
            }
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Only reachable via pending() = window done: flip to LIMIT_WINDOWEND
        // (what C's next ExecLimit call would do) and end the stream.
        ::nodelimit::lane_limit_end_window(self.limit);
        Ok(OpStatus::Finished)
    }
}

/// The Unique node as a mid-pipeline streaming operator: never pends (no
/// intra-tuple expansion) and never finishes early.
struct UniqueOp<'a, 'mcx> {
    unique: &'a mut ::nodeunique::UniqueState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for UniqueOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodeunique::lane_unique_feed(self.unique, estate, tuple)? {
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
        unreachable!("UniqueOp never pends")
    }
}

/// Admission for a hash-agg breaker child under a lane Limit: the agg-side
/// breaker gate × the child gates × (for the SeqScan feed) the memoized
/// `AggLaneChoice` — exactly the bare `agg_arm` hooks' admission
/// (`try_own_agg_over_seq_scan` / `try_own_agg_over_hash_join`), including
/// the economics `Refuse` arm, so a Limit-owned agg chain admits precisely
/// where the agg hook would.
fn agg_child_fusible<'mcx>(
    aps: &mut crate::procnode::AggPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_breaker_admissible(&aps.agg) {
        return Ok(false);
    }
    match &mut aps.outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            if !seq_scan_fusible(ss, estate)? {
                return Ok(false);
            }
            let c = match aps.lane_choice {
                Some(c) => c,
                None => {
                    let c = decide_agg_lane(&aps.agg, ss, estate)?;
                    aps.lane_choice = Some(c);
                    c
                }
            };
            Ok(c != AggLaneChoice::Refuse)
        }
        crate::procnode::PlanStateNode::HashJoin(hj) => hash_join_lane_fusible(hj, estate),
        _ => Ok(false),
    }
}

/// Try to let the lane own a `Limit` over a lane-owned chain — the streaming
/// limit (see the section header above for the protocol, the row-identity
/// argument, and the documented refusals). Admitted children: the sort
/// breaker, and the hash-agg breaker over its admitted feeds (SeqScan, or
/// the hash-join composition). `None` = refused; falling to `exec_limit` is
/// byte-safe at any boundary because the lane drives the SAME LimitState
/// machine C does (including after the prologue below ran — C's own INITIAL
/// arm would have run the same recompute once).
#[inline]
pub fn try_own_limit<'mcx>(
    l: &mut crate::procnode::LimitNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    use ::nodelimit::LimitStateCond::*;
    // Dynamic per-call gates + the limit-side shape gate (COUNT only; the
    // option is init-stable so this refuse is stable too).
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !::nodelimit::lane_limit_admissible(&l.state)
    {
        return Ok(None);
    }
    // Child admission BEFORE any state effect (a refuse must leave the node
    // untouched). Child verdicts are memoized on the child nodes.
    let child_ok = match &mut *l.outer {
        crate::procnode::PlanStateNode::Sort(s) => sort_lane_fusible_memo(s, estate)?,
        crate::procnode::PlanStateNode::Agg(aps) => agg_child_fusible(&mut **aps, estate)?,
        _ => false,
    };
    if !child_ok {
        return Ok(None);
    }
    // C's exec_limit entry: CFI, then the LIMIT_INITIAL recompute (evaluates
    // OFFSET/LIMIT — same negative-value errors — and pushes the tuple bound
    // to the child: the Sort's top-N bound; a no-op for Agg).
    ::nodelimit::lane_limit_prologue(&mut l.state, &mut *l.outer, estate)?;
    match l.state.lstate {
        // Terminal forward states: nothing more to return (C's arms).
        LIMIT_EMPTY | LIMIT_WINDOWEND | LIMIT_SUBPLANEOF => return Ok(Some(None)),
        LIMIT_RESCAN => {
            // LIMIT 0: the window is empty and the child is NEVER pulled
            // (C's `count <= 0 && !noCount` arm) — no feed, no build.
            if ::nodelimit::lane_limit_empty_window(&mut l.state) {
                return Ok(Some(None));
            }
        }
        LIMIT_INWINDOW => {}
        LIMIT_INITIAL => unreachable!("prologue recomputed"),
        // Backward-only states — unreachable under the forward gate + the
        // non-scrollable admitted children; refuse defensively.
        LIMIT_WINDOWEND_TIES | LIMIT_WINDOWSTART => return Ok(None),
    }
    // Run the owned chain: child pipeline → LimitOp → root adapter.
    let r = match &mut *l.outer {
        crate::procnode::PlanStateNode::Sort(s) => {
            // C's first child pull enters ExecSort: entry CFI, then the feed
            // (the tuplesort bound set by the prologue makes it top-N,
            // exactly as C's bounded sort under Limit).
            ::postgres_seams::check_for_interrupts::call()?;
            let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
            sort_feed_if_needed(state, &mut **outer, outer_desc, estate)?;
            let mut op = LimitOp { limit: &mut l.state };
            let mut root = RootAdapter::new(None);
            pull_step_chain(state, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?
        }
        crate::procnode::PlanStateNode::Agg(aps) => {
            let aps = &mut **aps;
            // exec_agg's top-of-call guard: a drained agg stays drained (the
            // hash iterator is spent) — treat as source EOF.
            if ::nodeagg::agg_is_done(&aps.agg) {
                None
            } else {
                let built = match &mut aps.outer {
                    crate::procnode::PlanStateNode::SeqScan(ss) => {
                        let c = aps.lane_choice.expect("admission decided the agg lane choice");
                        agg_seq_scan_build_if_needed(&mut aps.agg, ss, c, estate)?;
                        true
                    }
                    crate::procnode::PlanStateNode::HashJoin(hj) => {
                        agg_hash_join_build_if_needed(&mut aps.agg, &mut **hj, estate)?
                    }
                    _ => unreachable!("agg_child_fusible admitted a non-lane agg feed"),
                };
                if !built {
                    // Join multi-batch spill refuse, before any lane tuple:
                    // exec_limit over the per-tuple agg/join resumes
                    // byte-identically (the recompute above ran once, as C's
                    // INITIAL arm would have).
                    return Ok(None);
                }
                let mut op = LimitOp { limit: &mut l.state };
                let mut root = RootAdapter::new(None);
                pull_step_chain(
                    &mut aps.agg,
                    &mut HashAggSource,
                    &mut HashAggEmit,
                    &mut op,
                    &mut root,
                    estate,
                )?
            }
        }
        _ => unreachable!("admitted a non-lane limit child"),
    };
    if r.is_none() && matches!(l.state.lstate, LIMIT_RESCAN | LIMIT_INWINDOW) {
        // Source exhausted before the window filled — C's subplan-EOF arms.
        ::nodelimit::lane_limit_eof(&mut l.state);
    }
    Ok(Some(r))
}

/// Try to let the lane own a `Unique` over the sort breaker — streaming
/// adjacent-dedup on the sorted emit (see the section header for economics +
/// refusals; hashed DISTINCT plans an Agg and is owned by the agg breaker).
/// `None` = refused; `exec_unique` drives the same UniqueState byte-safely.
#[inline]
pub fn try_own_unique<'mcx>(
    u: &mut crate::procnode::UniqueNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (Unique init asserts !BACKWARD && !MARK, so a
    // non-forward pull should be impossible — gate anyway, like the sort).
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(None);
    }
    let crate::procnode::UniqueNode { state, outer } = u;
    let crate::procnode::PlanStateNode::Sort(s) = outer else {
        return Ok(None);
    };
    if !sort_lane_fusible_memo(s, estate)? {
        return Ok(None);
    }
    // C's ExecUnique entry interrupt check (conditional, exactly the
    // Volcano entry's), then the first child pull's ExecSort entry CFI.
    ::nodeunique::lane_unique_cfi()?;
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    sort_feed_if_needed(sstate, &mut **souter, outer_desc, estate)?;
    let mut op = UniqueOp { unique: state };
    let mut root = RootAdapter::new(None);
    let r = pull_step_chain(sstate, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?;
    if r.is_none() {
        // exec_unique's end-of-stream arm: drop the retained previous tuple
        // and clear both slots.
        ::nodeunique::lane_unique_eof(state, estate);
    }
    Ok(Some(r))
}
