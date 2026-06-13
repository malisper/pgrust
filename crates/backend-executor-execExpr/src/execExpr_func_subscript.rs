//! `execExpr-func-subscript` family — function-call, whole-row, subscripting
//! and SubPlan expression init/eval.
//!
//! Owns `ExecInitFunc` (the FUNCEXPR strict/fusage classification that picks
//! the `EEOP_FUNCEXPR*` opcode), `ExecInitSubPlanExpr`, `ExecInitWholeRowVar`,
//! `ExecInitSubscriptingRef` (+ `isAssignmentIndirectionExpr`), and the SubPlan
//! exec-state seams nodeSubplan drives (`testexpr` init/eval, the
//! `projLeft`/`projRight` projections and their result-slot accessors).
//!
//! The two projections and the combining expression's compiled state are
//! execExpr-owned machinery that `SubPlanState` keeps in inherited-opaque
//! [`Opaque`] slots (`projLeft`/`projRight`/`testexpr`). The writer
//! ([`execExpr_domain_agg::build_hash_projections_and_exprs`] for the hashed
//! path and [`sub_init_testexpr`] for the combining expression) stores a
//! [`ProjCarrier`] / [`TestExprCarrier`] in those slots; the seams here read
//! them back and route the heavy lifting — `ExecProject`,
//! `ExecEvalExprSwitchContext`, `ExecClearTuple`, `slot_getattr`,
//! `slot_attisnull` — through the owning unit's seam (execExpr-core for the
//! projection/eval, execTuples for the slot accessors).

extern crate alloc;

use types_core::AttrNumber;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execexpr::{ExprState, ProjectionInfo, SubPlanState};
use types_nodes::execnodes::Opaque;
use types_nodes::{EStateData, EcxtId, SlotId};

use backend_executor_execExpr_seams::{ProjectionKind, SlotAttr};

// --- inherited-opaque carriers ------------------------------------------------
//
// `projLeft`/`projRight`/`testexpr` are `Opaque` (`Box<dyn Any + 'static>`) on
// `SubPlanState` because execExpr/execGrouping were unported when the node type
// was carved. The real payloads carry the EState arena lifetime `'mcx`, which
// `dyn Any`'s `'static` bound forbids; like the partition-pruning plan-data
// carried through `Opaque` in execPartition, the payload is built and dropped
// inside the EState's per-query context, so we erase the lifetime on store and
// reinstate the caller's `'mcx` on borrow. A carrier is only ever read back by
// this owning unit, threaded with the same `&mut EStateData<'mcx>` it was built
// under.

/// `ProjectionInfo *` payload for a `projLeft`/`projRight` `Opaque` slot, plus
/// the EState slot-pool id of `pi_state.resultslot` (the projection's output
/// slot, allocated by `ExecBuildProjectionInfo`). The result slot lives in the
/// EState slot pool in the owned model, so its id is what the slot accessors
/// (`slot_getattr` / `slot_attisnull` / `ExecClearTuple` / natts) address.
pub struct ProjCarrier<'mcx> {
    /// The compiled `ProjectionInfo` (`projLeft`/`projRight`), allocated in the
    /// EState's per-query context.
    pub proj: mcx::PgBox<'mcx, ProjectionInfo<'mcx>>,
    /// `node->proj*->pi_state.resultslot` — the result slot's pool id.
    pub resultslot: SlotId,
}

/// `ExprState *` payload for a `testexpr` (or any combining-expression)
/// `Opaque` slot.
pub struct TestExprCarrier<'mcx> {
    /// The compiled combining `ExprState`, allocated in the EState's per-query
    /// context.
    pub state: mcx::PgBox<'mcx, ExprState<'mcx>>,
}

/// Store a freshly built `ProjCarrier` into a projection's `Opaque` slot,
/// erasing the arena lifetime (see the module note). Used by the projection
/// builder (`build_hash_projections_and_exprs`).
pub fn store_proj_carrier(slot: &mut Opaque, carrier: ProjCarrier<'_>) {
    // SAFETY: the payload's `'mcx` allocations outlive every read, which only
    // happens while the owning EState is alive and threaded back in; the
    // lifetime is reinstated on borrow.
    let erased: ProjCarrier<'static> = unsafe { core::mem::transmute(carrier) };
    *slot = Opaque(Some(alloc::boxed::Box::new(erased)));
}

/// Store a freshly built `TestExprCarrier` into a `testexpr` `Opaque` slot.
pub fn store_testexpr_carrier(slot: &mut Opaque, carrier: TestExprCarrier<'_>) {
    let erased: TestExprCarrier<'static> = unsafe { core::mem::transmute(carrier) };
    *slot = Opaque(Some(alloc::boxed::Box::new(erased)));
}

/// Borrow the `ProjCarrier` out of a projection `Opaque` slot, reinstating the
/// caller's `'mcx`. Panics loudly (the inherited-opaque contract) if the slot
/// is NULL or holds the wrong payload.
fn proj_carrier<'a, 'mcx>(slot: &'a Opaque, which: ProjectionKind) -> &'a ProjCarrier<'mcx> {
    let any = slot
        .0
        .as_ref()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection not built"));
    let erased = any
        .downcast_ref::<ProjCarrier<'static>>()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection is not a ProjCarrier"));
    // SAFETY: reinstate the arena lifetime erased on store.
    unsafe { core::mem::transmute::<&ProjCarrier<'static>, &ProjCarrier<'mcx>>(erased) }
}

/// Mutable form of [`proj_carrier`].
fn proj_carrier_mut<'a, 'mcx>(
    slot: &'a mut Opaque,
    which: ProjectionKind,
) -> &'a mut ProjCarrier<'mcx> {
    let any = slot
        .0
        .as_mut()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection not built"));
    let erased = any
        .downcast_mut::<ProjCarrier<'static>>()
        .unwrap_or_else(|| panic!("SubPlanState {which:?} projection is not a ProjCarrier"));
    unsafe { core::mem::transmute::<&mut ProjCarrier<'static>, &mut ProjCarrier<'mcx>>(erased) }
}

/// Borrow the `TestExprCarrier` out of the `testexpr` `Opaque` slot.
fn testexpr_carrier<'a, 'mcx>(slot: &'a Opaque) -> &'a TestExprCarrier<'mcx> {
    let any = slot
        .0
        .as_ref()
        .expect("SubPlanState testexpr not built");
    let erased = any
        .downcast_ref::<TestExprCarrier<'static>>()
        .expect("SubPlanState testexpr is not a TestExprCarrier");
    unsafe { core::mem::transmute::<&TestExprCarrier<'static>, &TestExprCarrier<'mcx>>(erased) }
}

/// Pick the named projection's `Opaque` slot off the node.
fn proj_slot<'a, 'mcx>(node: &'a SubPlanState<'mcx>, which: ProjectionKind) -> &'a Opaque {
    match which {
        ProjectionKind::Left => &node.projLeft,
        ProjectionKind::Right => &node.projRight,
    }
}

/// Mutable form of [`proj_slot`].
fn proj_slot_mut<'a, 'mcx>(
    node: &'a mut SubPlanState<'mcx>,
    which: ProjectionKind,
) -> &'a mut Opaque {
    match which {
        ProjectionKind::Left => &mut node.projLeft,
        ProjectionKind::Right => &mut node.projRight,
    }
}

/// `sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent)`
/// (nodeSubplan.c:833) — compile the combining expression into `node->testexpr`.
pub fn sub_init_testexpr<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C: sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent);
    //
    // A SubPlan with a NULL testexpr (e.g. EXISTS/EXPR/ARRAY sublinks) leaves
    // testexpr NULL — ExecInitExpr(NULL, ...) returns NULL. Mirror that: an
    // absent combining expression leaves the Opaque slot NULL.
    let subplan = node
        .subplan
        .as_ref()
        .expect("SubPlanState.subplan is NULL in ExecInitSubPlanExpr");
    let testexpr = match subplan.testexpr.as_ref() {
        Some(e) => e,
        None => {
            node.testexpr = Opaque::default();
            return Ok(());
        }
    };

    // The parent PlanState ExecInitExpr threads through is the SubPlan's parent
    // expression context. The owned model lends it as a `&mut PlanStateData`;
    // route through the execExpr-core ExecInitExpr seam (this crate). The
    // parent is the node's own planstate's head (the subselect plan state),
    // which is what carries the EState param/slot context here.
    let parent_node = node
        .planstate
        .as_mut()
        .expect("SubPlanState.planstate is NULL in ExecInitSubPlanExpr");
    let parent = parent_node.ps_head_mut();

    // `ExecInitExpr` is owned by execExpr-core; route through its seam.
    let state = crate::execExpr_core::exec_init_expr(testexpr, parent, estate)?;

    store_testexpr_carrier(&mut node.testexpr, TestExprCarrier { state });
    Ok(())
}

/// `ExecProject(node->projLeft|projRight)` (nodeSubplan.c:133-134 / :605).
pub fn sub_exec_project<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
    which: ProjectionKind,
) -> PgResult<()> {
    // C (projLeft path): node->projLeft->pi_exprContext = econtext;
    //                    slot = ExecProject(node->projLeft);
    // C (projRight path): slot = ExecProject(node->projRight);  // uses the
    //                    node's own innerecontext, set at init — `econtext`
    //                    is ignored on this side (see seam doc).
    {
        let carrier = proj_carrier_mut(proj_slot_mut(node, which), which);
        match which {
            ProjectionKind::Left => {
                // hack alert (per C): retarget the lefthand projection at the
                // supplied per-tuple econtext before projecting.
                carrier.proj.pi_exprContext = Some(econtext);
            }
            ProjectionKind::Right => { /* projRight keeps its innerecontext */ }
        }
    }

    // `ExecProject` is owned by execExpr-core; route through its seam over the
    // explicit ProjectionInfo. The result lands in the projection's result
    // slot (its pool id is `carrier.resultslot`).
    let carrier = proj_carrier(proj_slot(node, which), which);
    let _resultslot = crate::execExpr_core::exec_project_info(&carrier.proj, estate)?;
    Ok(())
}

/// The `SlotId` of the named projection's result slot
/// (`node->proj*->pi_state.resultslot`).
pub fn sub_proj_result_slot_id<'mcx>(
    node: &SubPlanState<'mcx>,
    _estate: &EStateData<'mcx>,
    which: ProjectionKind,
) -> SlotId {
    proj_carrier(proj_slot(node, which), which).resultslot
}

/// `ExecClearTuple(node->proj*->pi_state.resultslot)`.
pub fn sub_clear_proj_result_slot<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: ProjectionKind,
) -> PgResult<()> {
    // C: ExecClearTuple(slot);  /* the projection's result slot */
    let slot_id = proj_carrier(proj_slot(node, which), which).resultslot;
    // `ExecClearTuple` over a pool slot is owned by execTuples; route through
    // its slot accessor. The result slot lives in the EState slot pool.
    let slot = estate.slot_mut(slot_id);
    backend_executor_execTuples_seams::exec_clear_tuple::call(slot)
}

/// `slot->tts_tupleDescriptor->natts` of the named projection's result slot
/// (`slotAllNulls`/`slotNoNulls`).
pub fn proj_result_slot_natts(
    node: &SubPlanState<'_>,
    estate: &EStateData<'_>,
    which: ProjectionKind,
) -> i32 {
    // C: int ncols = slot->tts_tupleDescriptor->natts;
    let slot_id = proj_carrier(proj_slot(node, which), which).resultslot;
    // natts of a pool slot is owned by execTuples; route through its seam.
    backend_executor_execTuples_seams::slot_natts::call(estate, slot_id)
}

/// `slot_attisnull(slot, attnum)` over the named projection's result slot
/// (`slotAllNulls`/`slotNoNulls`).
pub fn proj_result_slot_attisnull<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: ProjectionKind,
    attnum: i32,
) -> PgResult<bool> {
    // C: slot_attisnull(slot, i)  — which deforms up to `i` and reads
    // tts_isnull[i-1]; modeled here as slot_getattr's isnull over the pool slot
    // (`slot_getattr` is the deforming accessor; slot_attisnull is its is-null
    // half).
    let slot_id = proj_carrier(proj_slot(node, which), which).resultslot;
    let attr =
        backend_executor_execTuples_seams::slot_getattr_by_id::call(estate, slot_id, attnum as AttrNumber)?;
    Ok(attr.isnull)
}

/// `slot_getattr(node->projLeft result slot, att, &isnull)`
/// (`execTuplesUnequal` `slot1`).
pub fn proj_left_slot_getattr<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    att: AttrNumber,
) -> PgResult<SlotAttr> {
    // C: attr1 = slot_getattr(slot1, att, &isNull1);  // slot1 == projLeft slot
    let slot_id = proj_carrier(proj_slot(node, ProjectionKind::Left), ProjectionKind::Left)
        .resultslot;
    let attr = backend_executor_execTuples_seams::slot_getattr_by_id::call(estate, slot_id, att)?;
    Ok(SlotAttr {
        value: attr.value,
        isnull: attr.isnull,
    })
}

/// `ExecEvalExprSwitchContext(node->testexpr, econtext, &rownull)`
/// (nodeSubplan.c:399).
pub fn eval_testexpr_switch_context<'mcx>(
    node: &SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    econtext: EcxtId,
) -> PgResult<(Datum, bool)> {
    // C: rowresult = ExecEvalExprSwitchContext(node->testexpr, econtext,
    //                                          &rownull);
    let carrier = testexpr_carrier(&node.testexpr);
    // `ExecEvalExprSwitchContext` is owned by execExpr-core; route through its
    // seam over the compiled combining ExprState.
    crate::execExpr_core::exec_eval_expr_switch_context(&carrier.state, econtext, estate)
}
