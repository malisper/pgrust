//! DML lane hosting — wave-2 WS-N increment 1 (the wave-2 integration
//! contract §2/§3.7/§6-WS-N; full design + increments 2-5 ladder in
//! docs/design/lane-dml-epq.md).
//!
//! Increment 1 hosts exactly ONE mutation shape: the single-result-relation
//! plain-table **INSERT without triggers** (INSERT .. VALUES / INSERT ..
//! SELECT, defaults, GENERATED columns, identity — everything the planner
//! folds into that plan shape), with at most trivial RETURNING (admitted per
//! contract §6-WS-N(1); the OLD/NEW-alias form is the non-trivial carve-out).
//! Everything else — triggers, ON CONFLICT, partition routing / inherited
//! targets, UPDATE / DELETE / MERGE, non-table targets — refuses LOUDLY with
//! `RefuseReason::DmlShape` and the mechanism-attribution detail string from
//! `nodemodifytable::mt_lane_insert_refusal` (contract §1: attribution rides
//! the detail string, never a second class).
//!
//! Hosting shape: the WS-G delegation-leaf template over the contract §3.7
//! seams. `MtChildSource` is the chartered Phase-2 shipping of the
//! VolcanoRowSource concept **specialized with structural prologue
//! placement** (rowmode-operators.md §7 amendment, contract §6-WS-N(3)): its
//! `next_row` delegates `nodemodifytable::mt_step`, the seam composition
//! whose loop runs `mt_row_prologue` BEFORE every child pull — the
//! prologue-before-pull contract LAW lives structurally inside the seam
//! driver, never in an `accept` body — then the `mt_pending`/`mt_resume`
//! deferred-MERGE arm (structurally present, unreachable for the inc-1
//! INSERT shape), the Volcano child pull, and `mt_accept_row`;
//! `mt_source_exhausted` (columnar flush + AS triggers + the `mt_done`
//! latch) runs on child exhaustion inside the same seam driver. BOTH engines
//! therefore drive the IDENTICAL statement stream (`exec_modify_table` ==
//! `mt_begin` + `mt_step`): byte-identity — rows written, WAL, command tag,
//! RETURNING bytes, trigger side effects (none in the admitted shape) — is
//! by construction, and a Volcano fallback at ANY pull boundary resumes
//! byte-safely because every cross-call state (`mt_done`, `fireBSTriggers`,
//! dispatch cache, EPQ origslot) is node-resident.
//!
//! A genuine TupleOp decomposition (`DmlInsertOp::accept` = `mt_accept_row`
//! with `MtChildSource` producing bare child rows) is increment-2's step —
//! it needs the split-view borrow work (`LaneProjectSet` precedent) so the
//! op and the source can hold disjoint `ModifyTableState` pieces; the seams
//! are already shaped for it (see the design doc §3). Nothing about THIS
//! increment's admitted set changes at that step.
//!
//! Knob: `PGRUST_LANE_V2_DML` (default OFF; contract §2 — the inc-1..3
//! family knob; `PGRUST_LANE_V2_DML_BATCH` is inc-4's and is NOT read here).
//! Knob-OFF ticks NOTHING: ModifyTable has no pre-existing wholesale refuse,
//! so default-config accounting stays byte-identical by construction (§2d).
//!
//! Gate order (contract §3.2, exactly WS-G): knob (OFF = `Ok(None)`, ticks
//! nothing) → `es_epq_active` → Epq (the EPQ LAW §3.5: EPQ refuses ALL dml
//! ownership until WS-N inc-5, which is gated on 100% read-side coverage) →
//! `!forward` → Backward → instrumented → Instrumented → the DmlShape probe
//! → `tick_owned` ONCE → `mt_begin` + `pull_step_rows`. OWNED cadence = once
//! per drive start = once per owned PG pull (§3.3).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::push::{pull_step_rows, PassthroughOp, RootAdapter, RowSource};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_DML` (default OFF): the wave-2 WS-N family knob for DML
/// hosting increments 1-3 (contract §2). Same AtomicU8 idiom as
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

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn dml_set_for_tests(on: bool) {
    DML.store(if on { 2 } else { 1 }, Relaxed);
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

/// The ModifyTable node as a row-mode delegation leaf over the contract
/// §3.7 seams — the chartered VolcanoRowSource specialization (module doc).
/// `next_row` runs the identical statements `modify_table_arm`'s fallback
/// runs after its own `mt_begin` (which `try_own_modify_table` already
/// replayed): one `mt_step` — prologue-before-pull structurally inside,
/// RETURNING rows surface one per call, and the terminal `None` has already
/// run `mt_source_exhausted`. Zero lane-held cross-call state (the
/// shared-slot law §3.8 is moot: no shared slots on this path).
struct MtChildSource;

impl<'mcx> RowSource<'mcx> for MtChildSource {
    type Node = crate::procnode::ModifyTablePlanState<'mcx>;

    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        let crate::procnode::ModifyTablePlanState { mt, subplan, epq } = node;
        ::nodemodifytable::mt_step(
            mt,
            estate,
            &mut |e| crate::procnode::exec_proc_node(subplan, e),
            // The ONE epq_eval recheck-driver closure (contract §3.5),
            // spelled exactly as modify_table_arm's fallback spells it.
            // Structurally live for the seam signature; unreachable in the
            // admitted inc-1 shape (no ON CONFLICT, no UPDATE/DELETE/MERGE
            // arms, and es_epq_active refused ownership above).
            &mut |subs, e, inputslot, rti| {
                // EvalPlanQualSlot keys by the dispatch-current result
                // relation.
                epq.result_rti = rti;
                crate::epq::eval_plan_qual(epq, subs, e, inputslot)
            },
        )
    }
}

/// Try to let the DML lane host a ModifyTable pull. `None` = refused; the
/// caller runs the unchanged `exec_modify_table` fallback.
///
/// Gate order per the module doc. The shape probe
/// (`nodemodifytable::mt_lane_insert_refusal`) is a read-only verdict on
/// node state resolved at init — its refusal leaves the node untouched, so
/// the Volcano fall-through is byte-safe trivially.
#[inline]
pub fn try_own_modify_table<'mcx>(
    mps: &mut ::mcx::PgBox<'mcx, crate::procnode::ModifyTablePlanState<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !dml_enabled() {
        return Ok(None);
    }
    // Dynamic per-call gates (the try_own_result cadence; contract §3.2).
    if estate.es_epq_active {
        // EPQ LAW (contract §3.5): an active EvalPlanQual recheck refuses
        // ALL dml ownership through wave 2 (lifted only by inc-5, gated on
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
    // Shape gate: the inc-1 admitted set (module doc; the probe's detail
    // string carries mechanism attribution, contract §1).
    if let Some(detail) = ::nodemodifytable::mt_lane_insert_refusal(&node.mt) {
        stats::tick_refused(ShapeClass::ModifyTable, RefuseReason::DmlShape);
        if super::lane_trace_enabled() {
            super::lane_trace(&format!("dml: shape refused ({detail})"));
        }
        #[cfg(test)]
        DML_SHAPE_REFUSED_FOR_TESTS.fetch_add(1, Relaxed);
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::ModifyTable);
    super::lane_trace("dml: insert drive owned");
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
    // No clear-on-finish: exec_modify_table returns end-of-set without
    // clearing any result slot.
    let mut op = PassthroughOp;
    let mut root = RootAdapter::new(None);
    pull_step_rows(node, &mut MtChildSource, &mut op, &mut root, estate).map(Some)
}
