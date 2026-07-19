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
    // (The backward gate retired with the backward-execution wave B11.)
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
    // RB-R1 (SE18): the wave-7 WS-AA stitched trigger-INSERT chain dispatch
    // that sat here is DELETED — the chain never earned a default (wave-7
    // letter +0.47%/+0.54% NO-WIN, wave-9 AG re-read NO-WIN; Michael
    // ratified verdict (b) FLOOR). Trigger-bearing shapes refuse at the
    // probe above again (the pre-wave-7 admission set, byte-identical at
    // every default-config tip since the knob never flipped).
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
    // (The backward gate retired with the backward-execution wave B11.)
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

// ===== WAVE-7 APPEND REGION (WS-AA fusion inc-1a) — DELETED at RB-R1 =======
// The stitched trigger-INSERT row chain lived here: the admission knob
// (`PGRUST_LANESTITCH_ROWCHAIN` read, default OFF at every tip), the
// per-mask chain program builder, the compile-once body cache, the
// MtInsertChainHost protocol host, and `drive_insert_rowchain`. Deleted at
// RB-R1 (SE18, Michael-ratified): the chain never earned a default (wave-7
// letter +0.47%/+0.54% NO-WIN vs Volcano; wave-9 AG re-read NO-WIN).
// Admission narrowed with it: `mt_lane_shape_refusal` lost its
// `admit_row_triggers` arm — trigger-bearing DML refuses the lane again
// unconditionally (the pre-wave-7 set, byte-identical at default config).
// Oracle corpus KEPT: scripts/dualexec/corpus-dml-rowchain.sql prices the
// post-deletion world. The DmlInsertOp leaf (dmlleaf) is untouched — a
// different, letter-flat surface.

