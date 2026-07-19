//! DML lane hosting — wave-2 WS-N inc-1 shipped the seam delegation; wave-3
//! WS-T ships increments 2/2b and the inc-3a stretch on top of the SAME
//! seams (wave-3 contract §6.T; full design + ladder in
//! docs/design/lane-dml-epq.md):
//!
//! * **inc-2 (TupleOp decomposition)**: `MtChildSource` now produces BARE
//!   child rows; `DmlInsertOp: TupleOp` composes the mt_* seams under the
//!   shared `pull_step_rows` driver — `accept = mt_accept_row`, `resume =
//!   mt_row_prologue + the mt_pending/mt_resume deferred-MERGE arm`,
//!   `source_exhausted = mt_source_exhausted`. THE LAW (`mt_row_prologue`
//!   runs BEFORE the child pull, never inside an accept body) is placed
//!   structurally: the driver's resume hook is its only pre-pull
//!   chokepoint, so the op arms `loop_top_owed` at construction and after
//!   every accepted row, and the driver cannot reach `next_row` without
//!   running the loop-top seam composition first. The borrow blocker the
//!   design doc names (`exec_insert` uses `index_eval_cx`, which a
//!   source-held prologue piece would also need) is DISSOLVED rather than
//!   bridged: the prologue piece never leaves the op — `DmlInsertOp` holds
//!   `&mut ModifyTableState` whole (disjoint from the driver-held subplan
//!   field of `ModifyTablePlanState`), so no `MtRowCtx` turn-passing and no
//!   re-borrow token are needed (and no RefCell, no raw pointers — the
//!   FORBIDDEN list). Statement-stream identity with `mt_step` is argued
//!   arm by arm on the impl below.
//! * **inc-2 (lane-fed INSERT..SELECT)**: a SELECT side whose top is a
//!   shape the lane arm dispatch owns stops being Volcano-pulled through
//!   the `exec_proc_node` match and becomes a direct feed
//!   (`MtLaneFedSeqScanSource`) into `DmlInsertOp` — a pure feed-shape
//!   change (the per-row dispatch match is hoisted; the statements are the
//!   seq_scan_arm's own). Admission unchanged.
//! * **inc-2b (LockRows TupleOp)**: `LockRowsOp` re-expresses lock-then-
//!   emit as `accept` over bare child rows via the `nodelockrows::
//!   lr_accept_row` seam, consuming WS-L's PINNED epq_eval-closure shape
//!   (docs/design/rowmode-tail.md §4 — changing it is a reconciler
//!   amendment, wave-3 contract §4.2). `ShapeClass::LockRows = 36` is
//!   SHARED with WS-L's delegation host: this hook ticks at its OWN
//!   verdict chokepoint with mechanism attribution in the trace detail
//!   (`dml-tupleop`), and the procnode arm reaches it only after the
//!   rowmode-tail hook declined (WS-L's knob behavior is unchanged at both
//!   of its arms).
//! * **inc-3a (stretch)**: UPDATE/DELETE admission behind the NESTED
//!   `PGRUST_LANE_V2_DML_UD` knob — verdict-widening ONLY
//!   (`nodemodifytable::mt_lane_shape_refusal`, the renamed+widened probe);
//!   `mt_step`/`DmlInsertOp` already route every operation through
//!   `mt_accept_row`, so there is NO new machinery. TM_Updated rechecks
//!   inside the delegated `exec_update`/`exec_delete` go through the ONE
//!   `epq_eval` closure (§4.2). `RefuseReason::DmlShape = 35` unchanged;
//!   detail strings differentiate.
//!
//! Knobs: `PGRUST_LANE_V2_DML` (the inc-1..3 family knob, default OFF;
//! knob-OFF ticks NOTHING — §2.2) and `PGRUST_LANE_V2_DML_UD` (default OFF;
//! readable ONLY after the DML host knob has already passed — `_UD` alone
//! flips nothing). `PGRUST_LANE_V2_DML_BATCH` is inc-4's and is NOT read
//! here (out of wave-3 scope, contract §0.3).
//!
//! Gate order (contract §4.4, exactly the wave-2 template): knob (OFF =
//! `Ok(None)`, ticks nothing) → `es_epq_active` (EPQ LAW §4.2: an active
//! recheck refuses ALL dml ownership until inc-5) → backward →
//! instrumented → shape probe → `tick_owned` ONCE at the verdict
//! chokepoint → the host drive.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::{PgError, PgResult};

use super::push::{pull_step_rows, OpStatus, RootAdapter, RowSource, Sink, SinkFeed, TupleOp};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_DML` (default OFF): the WS-N family knob for DML hosting
/// increments 1-3 (wave-2 contract §2). Same AtomicU8 idiom as
/// `rowmode.rs`'s knobs for the same same-process A/B test-lever reason.
static DML: AtomicU8 = AtomicU8::new(0);

/// `pub(super)` for the combined arm gate (`lanev2::dml_active`,
/// se2-cost-fix); `#[inline]` + `#[cold]`-outlined resolve so the
/// per-statement modify_table arm check is one relaxed byte load + compare
/// (the outlined shape was part of the se2-dmlcost +123 instr/INSERT).
#[inline]
pub(super) fn dml_enabled() -> bool {
    match DML.load(Relaxed) {
        1 => false,
        2 => true,
        _ => dml_resolve(),
    }
}

#[cold]
#[inline(never)]
fn dml_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_DML").as_deref(),
        Ok("1") | Ok("on")
    );
    DML.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// `PGRUST_LANE_V2_DML_UD` (default OFF; wave-3 contract §2.1): the inc-3a
/// UPDATE/DELETE admission stretch. NESTED knob law: this cell is read
/// ONLY from inside `try_own_modify_table`, after `dml_enabled()` has
/// already passed (and after the arm's `dml_active()` combined gate) —
/// `_UD` alone flips nothing, and at default config this byte is never
/// loaded at all (OFF-first, §2.2).
static DML_UD: AtomicU8 = AtomicU8::new(0);

#[inline]
fn dml_ud_enabled() -> bool {
    match DML_UD.load(Relaxed) {
        1 => false,
        2 => true,
        _ => dml_ud_resolve(),
    }
}

#[cold]
#[inline(never)]
fn dml_ud_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_DML_UD").as_deref(),
        Ok("1") | Ok("on")
    );
    DML_UD.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn dml_set_for_tests(on: bool) {
    DML.store(if on { 2 } else { 1 }, Relaxed);
}

/// Same-process A/B lever for the inc-3a UD stretch units.
#[cfg(test)]
pub(crate) fn dml_ud_set_for_tests(on: bool) {
    DML_UD.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: owned DML drives, per pull.
#[cfg(test)]
pub(crate) static DML_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only refusal probe: DmlShape refusals ticked by `try_own_modify_table`
/// (the unit corpus proves the refusal legs tick without a stats-env dump).
#[cfg(test)]
pub(crate) static DML_SHAPE_REFUSED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only feed-shape probe: owned drives that selected the lane-fed
/// (direct, dispatch-hoisted) child feed rather than the Volcano
/// `exec_proc_node` feed.
#[cfg(test)]
pub(crate) static DML_LANEFED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Test-only engagement probe for the inc-2b LockRows TupleOp host, per
/// owned pull (the LockRows CLASS counter is shared with WS-L's delegation
/// host — this probe is the mechanism-attributed one).
#[cfg(test)]
pub(crate) static DML_LOCKROWS_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

// =============================================================================
// inc-2: the TupleOp decomposition.
// =============================================================================

/// The BARE child feed (inc-2 form of `MtChildSource`, design doc §4): one
/// Volcano pull of the ModifyTable subplan per `next_row`, NO mt statements
/// — the loop-top seams moved into `DmlInsertOp`'s resume face and the row
/// processing into its accept face. `Node` is the `subplan` FIELD of
/// `ModifyTablePlanState` (disjoint from the `mt`/`epq` fields the op
/// borrows — the `LaneProjectSet` disjoint-borrow precedent).
struct MtChildSource;

impl<'mcx> RowSource<'mcx> for MtChildSource {
    type Node = crate::procnode::PlanStateNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        crate::procnode::exec_proc_node(node, estate)
    }
}

/// The lane-fed INSERT..SELECT feed (inc-2, design doc §4 item 2): the
/// SELECT side's top is a SeqScan, so the per-row `exec_proc_node` match
/// dispatch is hoisted and the feed calls the arm's statements DIRECTLY —
/// the lane hook first (when the read lane owns the scan, the child rows
/// come off the lane's own batch pipeline), then the unchanged
/// `exec_seq_scan` fall-through. MUST stay statement-identical to
/// procnode's `seq_scan_arm` body (the ResultRowSource inline-duplicate
/// precedent, se-entrycost); admission is UNCHANGED — this is a pure
/// feed-shape change selected AFTER the ownership verdict.
struct MtLaneFedSeqScanSource;

impl<'mcx> RowSource<'mcx> for MtLaneFedSeqScanSource {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        // seq_scan_arm's exact statements (dispatch match hoisted): the
        // lane-v2 hook, then the UNCHANGED per-tuple path on refuse.
        if super::enabled() {
            if let Some(r) = super::try_own_seq_scan(node, estate)? {
                return Ok(r);
            }
        }
        ::nodeseqscan::exec_seq_scan(node, estate)
    }
}

/// The ModifyTable row processor as a mid-pipeline `TupleOp` over the
/// contract §3.7 seams (inc-2; the doc's `DmlInsertOp`, serving every
/// operation `mt_accept_row` routes — inc-3a widens the ADMISSION verdict
/// only, no change here). Holds the `mt`/`epq` fields of
/// `ModifyTablePlanState`; the driver holds the disjoint `subplan` field.
///
/// Statement-stream identity with `nodemodifytable::mt_step`, arm by arm
/// (`P` = `mt_row_prologue`, the loop-top seam pair):
///
/// * drive start: `loop_top_owed` is true ⇒ the driver's FIRST action is
///   `resume` = P → pending check — exactly mt_step's first loop
///   iteration's head. `resume` then reports `NeedInput`, the driver pulls
///   the child, `accept` runs `mt_accept_row`: P → pull → accept ≡ mt_step.
/// * consumed row (accept → None): `accept` re-arms `loop_top_owed` and
///   reports `NeedInput`; the driver's next round starts at `resume` = P
///   again ≡ mt_step's loop-bottom `continue` → loop-top P.
/// * emitted row (accept → Some): pushed to the capacity-one root ⇒
///   `Paused`, the drive returns the row ≡ mt_step's `return Ok(Some)`.
///   The NEXT owned pull constructs a fresh op with `loop_top_owed` armed ≡
///   the next `exec_modify_table` call entering the loop at P.
/// * deferred MERGE (structurally live, unreachable in the admitted set —
///   no MERGE admission, contract §6.T hard exclusion): `resume` loops
///   P → `mt_pending` → `mt_resume`, re-running P after a non-emitting
///   resume ≡ mt_step's `continue`. `mt_pending`/`mt_resume` are WIRED by
///   this op form but MUST NOT go live for MERGE shapes (the C-side trace
///   pin blocks MERGE admission — §6.T.5).
/// * child exhaustion: driver calls `source_exhausted` =
///   `mt_source_exhausted` (after `resume` already ran P this round) ≡
///   mt_step's P → pull(None) → mt_source_exhausted. Idempotence guard: the
///   `mt_done` latch check mirrors `mt_begin`'s own.
struct DmlInsertOp<'a, 'mcx> {
    mt: &'a mut ::nodemodifytable::ModifyTableState<'mcx>,
    epq: &'a mut crate::epq::EpqState<'mcx>,
    /// The loop-top seam composition (P + the pending arm) is owed before
    /// the next child pull. Armed at construction and after every accepted
    /// row; cleared only by `resume` — the LAW's structural placement (the
    /// prologue can never run inside `accept`, and the driver cannot pull
    /// while this is set without running `resume` first).
    loop_top_owed: bool,
}

impl<'mcx> DmlInsertOp<'_, 'mcx> {
    /// One emitted row into the downstream sink (shared by the accept and
    /// deferred-resume arms).
    #[inline(always)]
    fn push(
        out: &mut dyn Sink<'mcx>,
        row: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        Ok(match out.accept(row, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

impl<'mcx> TupleOp<'mcx> for DmlInsertOp<'_, 'mcx> {
    #[inline(always)]
    fn pending(&self) -> bool {
        self.loop_top_owed
    }

    /// The mt_step loop top as the pre-pull resume face: `mt_row_prologue`
    /// FIRST (the LAW), then the `mt_pending`/`mt_resume` deferred-MERGE
    /// arm. `NeedInput` = loop-top work done, pull the child.
    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert!(self.loop_top_owed);
        let Self { mt, epq, loop_top_owed } = self;
        loop {
            ::nodemodifytable::mt_row_prologue(mt, estate);
            if !::nodemodifytable::mt_pending(mt) {
                *loop_top_owed = false;
                return Ok(OpStatus::NeedInput);
            }
            // The ONE epq_eval recheck-driver closure (contract §4.2),
            // spelled exactly as modify_table_arm's fallback spells it.
            let rslot = ::nodemodifytable::mt_resume(mt, estate, &mut |subs, e, inputslot, rti| {
                epq.result_rti = rti;
                crate::epq::eval_plan_qual(epq, subs, e, inputslot)
            })?;
            if let Some(rslot) = rslot {
                // Row emitted from the deferred action; the loop top stays
                // owed for the next round (mt_step returns Some here and its
                // next call re-enters at the prologue).
                //
                // CAPACITY-ONE-SINK ASSUMPTION (review-flagged latent
                // divergence): this leg leaves `loop_top_owed` set and maps
                // the sink verdict through `push`. Under a sink that answers
                // `NeedMore` (capacity > 1), the driver would pull the child
                // WITHOUT a fresh loop-top prologue — diverging from mt_step
                // and tripping `accept`'s debug_assert. Today the arm is
                // unreachable (MERGE is never admitted — §6.T hard exclusion)
                // and the only sink is the capacity-one RootAdapter, so the
                // verdict is always `Full → Paused`. MUST be restructured
                // (clear/re-arm `loop_top_owed` around a NeedMore feed)
                // before MERGE admission or breaker-sink composition goes
                // live.
                let st = Self::push(out, rslot, estate)?;
                debug_assert!(
                    matches!(st, OpStatus::Paused),
                    "DmlInsertOp::resume emit leg requires a capacity-one sink \
                     (NeedMore here would pull the child with the loop top still owed)"
                );
                return Ok(st);
            }
            // Non-emitting deferred action ≡ mt_step's `continue`: loop-top
            // P again, then the (now clear) pending re-check.
        }
    }

    /// `mt_accept_row` over one bare child row: at most one RETURNING row
    /// out; `None` = row consumed (pull the next). Re-arms the loop top —
    /// NO prologue statements here (the LAW).
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert!(!self.loop_top_owed, "child pulled without the loop-top seams");
        self.loop_top_owed = true;
        let Self { mt, epq, .. } = self;
        // The ONE epq_eval closure again — TM_Updated rechecks initiated by
        // the delegated exec_insert/exec_update/exec_delete drive through
        // it (EPQ LAW distinction, design doc §6).
        let rslot =
            ::nodemodifytable::mt_accept_row(mt, estate, tuple, &mut |subs, e, inputslot, rti| {
                epq.result_rti = rti;
                crate::epq::eval_plan_qual(epq, subs, e, inputslot)
            })?;
        match rslot {
            None => Ok(OpStatus::NeedInput),
            Some(rslot) => Self::push(out, rslot, estate),
        }
    }

    /// `mt_source_exhausted` (columnar flush + AS triggers + the `mt_done`
    /// latch), exactly once per statement — the latch check mirrors
    /// `mt_begin`'s own and makes the possibly-repeated driver calls
    /// idempotent (TupleOp contract).
    fn source_exhausted(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        if !self.mt.mt_done {
            ::nodemodifytable::mt_source_exhausted(self.mt, estate)?;
        }
        Ok(OpStatus::Finished)
    }
}

/// Try to let the DML lane host a ModifyTable pull. `None` = refused; the
/// caller runs the unchanged `exec_modify_table` fallback.
///
/// Gate order per the module doc. The shape probe
/// (`nodemodifytable::mt_lane_shape_refusal`) is a read-only verdict on
/// node state resolved at init — its refusal leaves the node untouched, so
/// the Volcano fall-through is byte-safe trivially. The UD stretch knob is
/// read here and ONLY here, after the host knob passed (nested-knob law).
#[inline]
pub fn try_own_modify_table<'mcx>(
    mps: &mut ::mcx::PgBox<'mcx, crate::procnode::ModifyTablePlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !dml_enabled() {
        return Ok(None);
    }
    // Dynamic per-call gates (the try_own_result cadence; contract §4.4).
    if estate.es_epq_active {
        // EPQ LAW (contract §4.2): an active EvalPlanQual recheck refuses
        // ALL dml ownership through wave 3 (lifted only by inc-5, gated on
        // 100% read-side coverage).
        stats::tick_refused(ShapeClass::ModifyTable, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::ModifyTable, RefuseReason::Backward);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::ModifyTable, RefuseReason::Instrumented);
        return Ok(None);
    }
    let node = &mut **mps;
    // Shape gate: inc-1's admitted INSERT set, widened to UPDATE/DELETE by
    // the inc-3a stretch when the nested UD knob is on, and to the
    // ladder-named ON CONFLICT arms by wave-5 WS-W when the nested OC knob
    // is on (the OC admission entry, wave-5 contract §8.3 — dml.rs-local
    // per §2's preference; knob machinery in the wave-5 append region
    // below). The probe's detail string carries mechanism attribution
    // (contract §1).
    if let Some(detail) = ::nodemodifytable::mt_lane_shape_refusal(
        &node.mt,
        dml_ud_enabled(),
        dml_oc_enabled(),
        dml_rowchain_enabled(),
    ) {
        stats::tick_refused(ShapeClass::ModifyTable, RefuseReason::DmlShape);
        if super::lane_trace_enabled() {
            super::lane_trace(&format!("dml: shape refused ({detail})"));
        }
        #[cfg(test)]
        DML_SHAPE_REFUSED_FOR_TESTS.fetch_add(1, Relaxed);
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::ModifyTable);
    super::lane_trace("dml: modify drive owned");
    #[cfg(test)]
    DML_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // exec_modify_table's per-call head (the mt_begin seam), replayed here
    // so the drive below is exactly the fallback's mt_step. outer_instr_idx
    // is None by construction: instrumented estates were refused above (the
    // fallback computes Some only under EXPLAIN ANALYZE).
    if !::nodemodifytable::mt_begin(&mut node.mt, estate, None)? {
        // mt_done: end-of-set, exactly the fallback's early return.
        return Ok(Some(None));
    }
    // WAVE-7 WS-AA (fusion inc-1a): the ONE trigger-bearing INSERT chain
    // shape — admitted above only under the rowchain knob — dispatches to
    // the stitched row chain. ANY chain refusal (non-aarch64, master kill
    // switch, arena) deopts the WHOLE statement to the DmlInsertOp host
    // below, whose seam-call stream the chain replays exactly (the
    // two-stage x86 law: no semantics exist only in stitched bodies).
    // Knob-OFF zero cost: `dml_rowchain_enabled` is one relaxed byte load
    // and trigger shapes never pass the probe, so this branch is dark.
    if dml_rowchain_enabled() && ::nodemodifytable::mt_rowchain_shape(&node.mt) {
        if let Some(result) = drive_insert_rowchain(node, estate)? {
            return Ok(Some(result));
        }
    }
    // The disjoint-borrow split (LaneProjectSet precedent): the op holds
    // mt + epq, the driver holds the subplan. No clear-on-finish:
    // exec_modify_table returns end-of-set without clearing any result slot.
    let crate::procnode::ModifyTablePlanState { mt, subplan, epq } = node;
    let mut op = DmlInsertOp { mt, epq, loop_top_owed: true };
    let mut root = RootAdapter::new(None);
    match subplan {
        // Lane-fed INSERT..SELECT (and, under the UD stretch, the
        // seqscan-topped UPDATE/DELETE): the dispatch-hoisted direct feed.
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            #[cfg(test)]
            DML_LANEFED_FOR_TESTS.fetch_add(1, Relaxed);
            pull_step_rows(ss, &mut MtLaneFedSeqScanSource, &mut op, &mut root, estate).map(Some)
        }
        // Every other child shape: the bare Volcano feed (byte-identical
        // dispatch through exec_proc_node, exactly the inc-1 statements).
        other => pull_step_rows(other, &mut MtChildSource, &mut op, &mut root, estate).map(Some),
    }
}

// =============================================================================
// inc-2b: the LockRows TupleOp host.
// =============================================================================

/// Bare child feed for the LockRows TupleOp: one Volcano pull of the
/// LockRows outer child per `next_row` (the `outer` FIELD of
/// `LockRowsNode`, disjoint from the `state`/`epq` fields the op borrows).
struct LockRowsChildSource;

impl<'mcx> RowSource<'mcx> for LockRowsChildSource {
    type Node = crate::procnode::PlanStateNode<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        crate::procnode::exec_proc_node(node, estate)
    }
}

/// LockRows as a TupleOp (inc-2b, design doc §5): `accept` runs the
/// `nodelockrows::lr_accept_row` seam — the exec_lock_rows loop body as a
/// pure code move — over one bare child row: lock every rowmark, then emit
/// the row (or the EPQ-substituted row) or skip it (`WouldBlock` /
/// `SelfModified` / concurrent-delete / failed recheck ≡ C's `goto
/// lnext`). The recheck driver is THE PINNED epq_eval closure shape
/// (rowmode-tail.md §4): `|subs, e, inputslot| eval_plan_qual(epq, subs,
/// e, inputslot)` — byte-identical to WS-L's delegation host and to the
/// Volcano arm; `executils::EpqSubs` remains the one EPQ state store.
struct LockRowsOp<'a, 'mcx> {
    lr: &'a mut ::nodelockrows::LockRowsState<'mcx>,
    epq: &'a mut crate::epq::EpqState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for LockRowsOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let Self { lr, epq } = self;
        let emitted =
            ::nodelockrows::lr_accept_row(lr, estate, tuple, &mut |subs, e, inputslot| {
                crate::epq::eval_plan_qual(epq, subs, e, inputslot)
            })?;
        match emitted {
            // Row skipped (the former `continue 'lnext`): pull the next.
            None => Ok(OpStatus::NeedInput),
            Some(row) => Ok(match out.accept(row, estate)? {
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
        debug_assert!(false, "LockRowsOp::resume: pending() is always false");
        Err(Box::new(PgError::error(
            "lane-v2 LockRowsOp resumed with no pending expansion (driver contract violation)"
                .to_string(),
        )))
    }
}

/// Try to let the DML lane host a LockRows pull in TupleOp form (inc-2b).
/// `None` = refused; the caller falls through (procnode's arm order: WS-L's
/// rowmode-tail delegation hook FIRST — its knob behavior is unchanged —
/// then this hook, then the unchanged Volcano fallback).
///
/// Gate order per the wave-2 template. The LockRows class counter is
/// SHARED (§4.4 rule 6): this hook ticks at its own verdict chokepoint;
/// mechanism attribution ("dml-tupleop") rides the trace detail, never a
/// second class or reason.
#[inline]
pub fn try_own_lock_rows_dml<'mcx>(
    l: &mut ::mcx::PgBox<'mcx, crate::procnode::LockRowsNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !dml_enabled() {
        return Ok(None);
    }
    if estate.es_epq_active {
        // EPQ LAW (§4.2): nodes INSIDE a recheck plan are never lane-owned;
        // a recheck INITIATED by this host's own accept path delegates
        // through the one closure above and is byte-safe by construction.
        stats::tick_refused(ShapeClass::LockRows, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::LockRows, RefuseReason::Backward);
        return Ok(None);
    }
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::LockRows, RefuseReason::Instrumented);
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::LockRows);
    if super::lane_trace_enabled() {
        super::lane_trace("lockrows drive owned (dml-tupleop)");
    }
    #[cfg(test)]
    DML_LOCKROWS_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    // exec_lock_rows' per-call entry CFI, replayed so the drive below runs
    // the identical statements the delegated/Volcano call would.
    crate::cfi()?;
    let crate::procnode::LockRowsNode { state, outer, epq } = &mut **l;
    let mut op = LockRowsOp { lr: state, epq };
    // No clear-on-finish: exec_lock_rows returns end-of-set bare.
    let mut root = RootAdapter::new(None);
    pull_step_rows(&mut **outer, &mut LockRowsChildSource, &mut op, &mut root, estate).map(Some)
}

// ===== WAVE-5 APPEND REGION — do not edit above =====
// --- WS-W (wave-5): ON CONFLICT host-arm admission -------------------------
//
// Knob `PGRUST_LANE_V2_DML_OC` (wave-5 contract §3 registry row; default
// OFF, NEVER default during migration): the nested OC admission knob.
// Read ONLY from `try_own_modify_table`'s shape-gate line, after
// `dml_enabled()` has already passed (the `_UD` nested-knob law verbatim)
// — `_OC` alone flips nothing, and at default config this byte is never
// loaded at all (§0.6 OFF-first: the resolve is a one-shot #[cold]
// memoized read; the OFF arm adds zero branches to any hot path because
// the only reader sits behind the already-non-default DML host knob).
//
// ON semantics THIS wave (§8.3): admits ONLY the ladder-named ON CONFLICT
// host arms — INSERT .. ON CONFLICT DO NOTHING and DO UPDATE on the
// already-admitted structural set (single result rel, plain table, no
// triggers, no partition routing, trivial RETURNING). The widening is
// VERDICT-ONLY (the inc-3a `admit_ud` precedent): `mt_step` already
// routes every operation through `mt_accept_row` → `exec_insert`, whose
// four oc_* seams compose the whole speculative-insert ceremony
// identically on both engines — no new machinery, routing stays
// `RefuseReason::DmlShape` (vocab mint: ZERO). MERGE arms refuse even
// knob-ON (the probe's `merge` arm is unconditional; C-side trace pin
// outstanding). EPQ LAW unchanged: `es_epq_active` refuses ALL DML
// ownership BEFORE the shape gate, so OC arms refuse inside rechecks; a
// recheck INITIATED by an owned OC drive (exec_on_conflict_update's
// epq_eval use) goes through the ONE pinned closure `mt_accept_row`
// already carries.
//
// Isolation mapping (contract §8.4, declared here + notes/se-ws-w-dml-oc
// .md): WS-W battery = insert-conflict-do-nothing,
// insert-conflict-do-update{,-2,-3}, insert-conflict-specconflict,
// merge-match-recheck, merge-insert-update, merge-delete, merge-update,
// merge-join — refusal-invariant multi-arm legs this wave (byte-identical
// across knob arms where refusal holds; dualexec-proved where OC arms
// engage: scripts/dualexec/corpus-dml-oc.sql). partition-key-update-1..4
// asserted STAYING REFUSED (partition-routing is still DmlShape).
//
// Capacity-one-sink checkpoint (§8.5): NO wave-5 OC arm composes a
// breaker sink — the OC drive is the existing `DmlInsertOp` +
// `RootAdapter::new(None)` composition, and OC never sets
// `mt_merge_pending_not_matched`, so `resume`'s deferred-emit leg stays
// MERGE-only-unreachable. The debug_assert'd capacity-one assumption in
// `DmlInsertOp::resume` therefore stands UNRESTRUCTURED and
// still-outstanding (recorded in the worklog note; the restructure is
// owed before MERGE admission or any breaker-sink composition).

/// `PGRUST_LANE_V2_DML_OC` (default OFF): the wave-5 WS-W nested OC
/// admission knob. Same AtomicU8 memoized-resolve idiom as `DML`/`DML_UD`.
static DML_OC: AtomicU8 = AtomicU8::new(0);

#[inline]
fn dml_oc_enabled() -> bool {
    match DML_OC.load(Relaxed) {
        1 => false,
        2 => true,
        _ => dml_oc_resolve(),
    }
}

#[cold]
#[inline(never)]
fn dml_oc_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANE_V2_DML_OC").as_deref(),
        Ok("1") | Ok("on")
    );
    DML_OC.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the wave-5 OC admission units.
#[cfg(test)]
pub(crate) fn dml_oc_set_for_tests(on: bool) {
    DML_OC.store(if on { 2 } else { 1 }, Relaxed);
}
// --- end WS-W (wave-5) ------------------------------------------------------

// ===== WAVE-7 APPEND REGION (WS-AA fusion inc-1a) — do not edit above =======
// The trigger-DML stitched row loop (docs/design/rowmode-endgame.md §2.2
// inc-1(a); wave-7 contract §3). The ONE chartered shape: single-result-
// relation plain-heap INSERT with row triggers, ONCONFLICT_NONE, no
// partition routing, RETURNING absent or trivial — exactly the inc-1
// admitted set MINUS the `triggers` refusal arm, which
// `mt_lane_shape_refusal` now lifts ONLY under `admit_row_triggers` (the
// `PGRUST_LANESTITCH_ROWCHAIN` chain-family knob, default OFF; every other
// refusal arm is byte-identical).
//
// Chain form: WAVE-9 WS-AG rung 2 (fusion D1a) REPLACED the wave-7 single
// CALL_ACCEPT_ROW target with the rowmode-endgame §2.2 decomposition — the
// protocol targets are now the five `mt_ins_*` seams (nodemodifytable
// wave-9 append region): CALL_ROW_PROLOGUE = `mt_row_prologue` (+ the
// MERGE-pending contract assert), CALL_INS_STAGE / CALL_INS_BR /
// CALL_INS_WRITE / CALL_INS_EPILOGUE / CALL_INS_RETURNING. The chain
// program is per-statement-SHAPE, selected at admission from
// `mt_rowchain_shape_mask` (BR-armed x RETURNING-present — a closed set of
// four compiled bodies keyed by the mask, compile-once per variant; this
// replaced the wave-7 single OnceLock, E4 decision carried: refusal caches
// per mask FOREVER, notes/se-wave9-ag.md §2). The work-removal channel:
// the per-row drive stops re-deciding the view/partition/leaf/ON-CONFLICT
// arms that are structurally dead for the admitted shape — those checks
// hoisted to the once-per-statement mask probe. Byte-identity per target
// is argued in the nodemodifytable region header (each seam's statements
// are mt_accept_row's / exec_insert's own, in original order, dead-for-
// the-shape branches elided).
//
// Statement-stream identity with DmlInsertOp, per pull:
// * chain loop top = P (+ pending assert)  ≡  resume: P + mt_pending arm
//   (pending is MERGE-only and MERGE never reaches here — loud, not silent).
// * NextRow = the same feed split (lane-fed SeqScan statements verbatim /
//   exec_proc_node)  ≡  MtLaneFedSeqScanSource / MtChildSource.
// * STAGE -> BR -> WRITE -> EPILOGUE -> RETURNING ≡ mt_accept_row's
//   CMD_INSERT statement order (stage projection, exec_insert's BR + write
//   + AR/epilogue, then the RETURNING block): BR suppression -> SkipRow
//   (loop top re-runs P) ≡ exec_insert Ok(None) -> accept NeedInput;
//   RETURNING -> EmitPause ≡ push -> Paused; masks without a step run the
//   identical statements MINUS the step's guard-false body (the guard is
//   hoisted to the mask, not skipped).
// * exhaustion: P already ran this round, then mt_source_exhausted under
//   the mt_done latch  ≡  resume-P -> pull(None) -> source_exhausted.
// Statement-tag identity (canSetTag / es_processed) lives INSIDE
// `mt_ins_epilogue` (exec_insert's own tail statements), unchanged.
//
// Rails: es_epq_active refused ALL ownership before the shape gate (EPQ
// unreachable in this shape — no OC UPDATE — but the rail stands); the
// mt_resume capacity-one-sink trap is NOT restructured — the chain never
// composes a breaker sink and asserts the pending arm instead (class-37
// law unchanged); push.rs untouched (the chain dispatches from here).

/// `PGRUST_LANESTITCH_ROWCHAIN` read for ADMISSION (the same env knob that
/// gates lanestitch's stitcher, cached separately because admission must
/// work off-aarch64 too — x86 permanently runs the DmlInsertOp host on the
/// admitted shape). Nested-knob law: read ONLY after `dml_enabled()` passed
/// (the probe line + the dispatch line); at default config this byte is
/// never loaded.
static DML_ROWCHAIN: AtomicU8 = AtomicU8::new(0);

#[inline]
fn dml_rowchain_enabled() -> bool {
    match DML_ROWCHAIN.load(Relaxed) {
        1 => false,
        2 => true,
        _ => dml_rowchain_resolve(),
    }
}

#[cold]
#[inline(never)]
fn dml_rowchain_resolve() -> bool {
    let on = matches!(
        std::env::var("PGRUST_LANESTITCH_ROWCHAIN").as_deref(),
        Ok("1") | Ok("on")
    );
    DML_ROWCHAIN.store(if on { 2 } else { 1 }, Relaxed);
    on
}

/// Same-process A/B lever for the wave-7 rowchain admission units.
#[cfg(test)]
pub(crate) fn dml_rowchain_set_for_tests(on: bool) {
    DML_ROWCHAIN.store(if on { 2 } else { 1 }, Relaxed);
}

/// Test-only engagement probe: stitched-chain drives (the mechanism-
/// attributed counter; the ModifyTable CLASS counter ticks at the shared
/// verdict chokepoint above regardless of host).
#[cfg(test)]
pub(crate) static DML_ROWCHAIN_DRIVES_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The chain-family protocol call ids (chain-private vocabulary). Wave-9
/// WS-AG rung 2 RETIRED the wave-7 single-ACCEPT id (2) — the id stays
/// reserved, never reused — and minted the §2.2 decomposed targets.
const CALL_ROW_PROLOGUE: u16 = 1;
const CALL_INS_STAGE: u16 = 3;
const CALL_INS_BR: u16 = 4;
const CALL_INS_WRITE: u16 = 5;
const CALL_INS_EPILOGUE: u16 = 6;
const CALL_INS_RETURNING: u16 = 7;

/// The trigger-INSERT chain program for one shape mask (wave-9 rung 2):
/// loop-top prologue, pull, then the §2.2 seam sequence — BR and RETURNING
/// steps present iff the mask carries their bits. Shape-static per mask —
/// one compiled body per variant serves every statement of that shape
/// (hosts vary per drive, never the code).
pub(crate) fn rowchain_insert_prog_for_mask(mask: u8) -> ::lanestitch::Program {
    debug_assert!((mask as usize) < ::nodemodifytable::MT_ROWCHAIN_MASKS);
    let mut p = ::lanestitch::Program::new();
    p.steps.push(::lanestitch::Step::ProtocolCall { call: CALL_ROW_PROLOGUE });
    p.steps.push(::lanestitch::Step::NextRow);
    p.steps.push(::lanestitch::Step::ProtocolCall { call: CALL_INS_STAGE });
    if mask & ::nodemodifytable::MT_ROWCHAIN_BR != 0 {
        p.steps.push(::lanestitch::Step::ProtocolCall { call: CALL_INS_BR });
    }
    p.steps.push(::lanestitch::Step::ProtocolCall { call: CALL_INS_WRITE });
    p.steps.push(::lanestitch::Step::ProtocolCall { call: CALL_INS_EPILOGUE });
    if mask & ::nodemodifytable::MT_ROWCHAIN_RET != 0 {
        p.steps.push(::lanestitch::Step::ProtocolCall { call: CALL_INS_RETURNING });
    }
    p
}

/// One process-global compiled chain body. SAFETY (Send/Sync): a
/// `StitchedRowChain` is immutable after construction — an RX-mapped code
/// block plus plain telemetry fields, no interior mutability; `run` takes
/// `&self` and all mutable state lives in the per-drive host/params.
struct ShareChain(Option<::lanestitch::StitchedRowChain>);
// SAFETY: see ShareChain doc — immutable RX code + POD fields only.
unsafe impl Send for ShareChain {}
// SAFETY: see ShareChain doc.
unsafe impl Sync for ShareChain {}

/// Compile-once accessor, keyed by the shape mask (the wave-9 rung-2 cache
/// replacing the wave-7 single OnceLock). None = the stitched tier refused
/// (non-aarch64, master kill switch, family knob, arena full at first use):
/// every statement of that shape runs the DmlInsertOp portable host instead
/// — permanently for this process and mask (E4 decision, cached-forever:
/// notes/se-wave9-ag.md §2; lanestitch itself never latches — the latch is
/// exactly this cache).
fn rowchain_body_for_mask(mask: u8) -> Option<&'static ::lanestitch::StitchedRowChain> {
    static BODIES: [std::sync::OnceLock<ShareChain>; ::nodemodifytable::MT_ROWCHAIN_MASKS] = [
        std::sync::OnceLock::new(),
        std::sync::OnceLock::new(),
        std::sync::OnceLock::new(),
        std::sync::OnceLock::new(),
    ];
    BODIES[mask as usize]
        .get_or_init(|| {
            // The production entry: the family kill knob stays live here
            // (fault-injection acceptance leg), unlike the parity-only entry.
            ShareChain(::lanestitch::StitchedRowChain::compile(&rowchain_insert_prog_for_mask(
                mask,
            )))
        })
        .0
        .as_ref()
}

/// The chain host: protocol targets are the wave-9 `mt_ins_*` seams (the
/// §2.2 decomposition), the feed is the inc-2 feed split verbatim. Holds
/// the same disjoint borrows as the DmlInsertOp composition (op fields
/// mt+epq; the subplan field feeds). Per-row currency between seams
/// (`staged` plan slot, `insert_slot`, the write's `recheck` list) lives
/// here and is complete before any pause — RETURNING (the only pausing
/// step) runs after write+epilogue, so a fresh host per drive re-enters at
/// the loop top with nothing in flight (the wave-7 cadence, unchanged).
struct MtInsertChainHost<'a, 'mcx> {
    mt: &'a mut ::nodemodifytable::ModifyTableState<'mcx>,
    subplan: &'a mut crate::procnode::PlanStateNode<'mcx>,
    estate: &'a mut EStateData<'mcx>,
    staged: Option<ExecSlotId>,
    insert_slot: Option<ExecSlotId>,
    recheck: Option<::mcx::PgVec<'mcx, ::types_core::Oid>>,
    emitted: Option<ExecSlotId>,
}

impl<'mcx> ::lanestitch::RowChainHost for MtInsertChainHost<'_, 'mcx> {
    fn next_row(&mut self) -> PgResult<bool> {
        let Self { subplan, estate, .. } = self;
        let r = match subplan {
            // Lane-fed INSERT..SELECT: seq_scan_arm's exact statements
            // (dispatch match hoisted) — MtLaneFedSeqScanSource verbatim.
            crate::procnode::PlanStateNode::SeqScan(ss) => {
                let mut fed = None;
                if super::enabled() {
                    fed = super::try_own_seq_scan(ss, estate)?;
                }
                match fed {
                    Some(r) => r,
                    None => ::nodeseqscan::exec_seq_scan(ss, estate)?,
                }
            }
            // Every other child shape: the bare Volcano feed
            // (MtChildSource verbatim).
            other => crate::procnode::exec_proc_node(other, estate)?,
        };
        match r {
            Some(slot) => {
                self.staged = Some(slot);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn protocol_call(&mut self, call: u16) -> PgResult<::lanestitch::ChainVerdict> {
        match call {
            CALL_ROW_PROLOGUE => {
                ::nodemodifytable::mt_row_prologue(self.mt, self.estate);
                if ::nodemodifytable::mt_pending(self.mt) {
                    // MERGE-only deferral arm: structurally unreachable in
                    // the admitted INSERT set (no MERGE admission). Loud,
                    // never silent divergence from mt_step.
                    return Err(Box::new(PgError::error(
                        "lane-v2 rowchain: deferred MERGE action in a trigger-INSERT \
                         chain (admission contract violation)"
                            .to_string(),
                    )));
                }
                Ok(::lanestitch::ChainVerdict::Continue)
            }
            // The §2.2 decomposed targets (wave-9 rung 2). Statement order
            // across the calls ≡ mt_accept_row's CMD_INSERT arm; every
            // erroring statement is the node's own seam (error identity by
            // construction). The epq_eval closure of the wave-7 ACCEPT arm
            // is NOT needed here: the decomposed shape is ONCONFLICT_NONE
            // plain INSERT — exec_insert's only epq_eval consumer is the OC
            // DO-UPDATE dispatch, dead by shape (the mask probe refuses
            // anything else; the DmlInsertOp host still carries the pinned
            // closure for every non-chain shape).
            CALL_INS_STAGE => {
                let plan_slot = self.staged.expect("stage with no staged row");
                let Self { mt, estate, .. } = self;
                let islot = ::nodemodifytable::mt_ins_stage(mt, estate, plan_slot)?;
                self.insert_slot = Some(islot);
                Ok(::lanestitch::ChainVerdict::Continue)
            }
            CALL_INS_BR => {
                let islot = self.insert_slot.expect("BR with no staged insert row");
                let Self { mt, estate, .. } = self;
                if ::nodemodifytable::mt_ins_br_triggers(mt, estate, islot)? {
                    Ok(::lanestitch::ChainVerdict::Continue)
                } else {
                    // BR suppression ≡ exec_insert's Ok(None): the row is
                    // consumed — es_processed and the RETURNING stream see
                    // nothing; the loop top re-runs the prologue.
                    self.insert_slot = None;
                    Ok(::lanestitch::ChainVerdict::SkipRow)
                }
            }
            CALL_INS_WRITE => {
                let islot = self.insert_slot.expect("write with no staged insert row");
                let Self { mt, estate, .. } = self;
                let recheck = ::nodemodifytable::mt_ins_write(mt, estate, islot)?;
                self.recheck = Some(recheck);
                Ok(::lanestitch::ChainVerdict::Continue)
            }
            CALL_INS_EPILOGUE => {
                let islot = self.insert_slot.expect("epilogue with no staged insert row");
                let recheck = self.recheck.take().expect("epilogue with no write recheck list");
                let Self { mt, estate, .. } = self;
                ::nodemodifytable::mt_ins_epilogue(mt, estate, islot, &recheck)?;
                // No-RETURNING masks end the per-row segment here: Continue
                // falls off the program and the body loops to the top —
                // ≡ mt_accept_row Ok(None) ≡ accept -> NeedInput.
                Ok(::lanestitch::ChainVerdict::Continue)
            }
            CALL_INS_RETURNING => {
                let islot = self.insert_slot.expect("returning with no staged insert row");
                let plan_slot = self.staged.expect("returning with no staged plan row");
                let Self { mt, estate, .. } = self;
                let out = ::nodemodifytable::mt_ins_returning(mt, estate, islot, plan_slot)?;
                self.emitted = Some(out);
                Ok(::lanestitch::ChainVerdict::EmitPause)
            }
            other => Err(Box::new(PgError::error(format!(
                "lane-v2 rowchain: unknown protocol call {other}"
            )))),
        }
    }
}

/// One stitched chain drive (one owned pull's worth of work). `None` = the
/// stitched tier refused; the caller falls through to the DmlInsertOp host
/// for the WHOLE statement. `Some(Some(slot))` = an emitted RETURNING row
/// (capacity-one pause); `Some(None)` = end of set (source exhausted +
/// `mt_source_exhausted` under the latch).
fn drive_insert_rowchain<'mcx>(
    node: &mut crate::procnode::ModifyTablePlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // The wave-9 rung-2 shape mask: the once-per-drive hoist of the
    // per-row shape checks. None = the seams' specializations don't hold
    // (defensive — admission normally guarantees them): DmlInsertOp host.
    let Some(mask) = ::nodemodifytable::mt_rowchain_shape_mask(&node.mt) else {
        return Ok(None);
    };
    let Some(chain) = rowchain_body_for_mask(mask) else {
        return Ok(None);
    };
    if super::lane_trace_enabled() {
        super::lane_trace("dml: modify drive owned (rowchain)");
    }
    #[cfg(test)]
    DML_ROWCHAIN_DRIVES_FOR_TESTS.fetch_add(1, Relaxed);
    let crate::procnode::ModifyTablePlanState { mt, subplan, epq: _ } = node;
    let mut host = MtInsertChainHost {
        mt,
        subplan,
        estate,
        staged: None,
        insert_slot: None,
        recheck: None,
        emitted: None,
    };
    match chain.run(&mut host)? {
        ::lanestitch::ChainOutcome::Paused => {
            let r = host.emitted.take();
            debug_assert!(r.is_some(), "chain paused without an emitted row");
            Ok(Some(r))
        }
        ::lanestitch::ChainOutcome::Done => {
            // Idempotence guard mirrors DmlInsertOp::source_exhausted.
            if !host.mt.mt_done {
                ::nodemodifytable::mt_source_exhausted(host.mt, host.estate)?;
            }
            Ok(Some(None))
        }
    }
}
// --- end WS-AA (wave-7) ------------------------------------------------------
