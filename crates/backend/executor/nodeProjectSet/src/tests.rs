//! Logic tests for the ProjectSet node, driving the real `ExecProjectSet` /
//! `ExecProjectSRF` / `ExecInitProjectSet` / `ExecReScanProjectSet` against
//! mock installs of the unported owners' seams (execProcnode, execAmi,
//! execTuples, execUtils, execExpr, execSRF, interrupts). Per-test state is
//! `thread_local!` (never a shared static).

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::Once;

use ::mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgVec};
use ::nodes::execexpr::{ExprState, SetExprState};
use ::nodes::execnodes::{ExprContext, PlanStateData};
use ::nodes::executor::TupleSlotKind;
use ::nodes::nodeprojectset::ProjectSet as ProjectSetPlan;
use ::nodes::nodes::Node;
use ::nodes::primnodes::{Const, Expr, FuncExpr, OpExpr, TargetEntry};
use ::nodes::TupleTableSlot;

use super::*;
// The owner's eval/SRF/store-virtual seams now carry the canonical unified
// `types_tuple::…::Datum<'mcx>` (Datum-unification keystone); the mock
// implementations and scripted-result queues below must match that type, so
// shadow the bare-word `Datum` pulled in via `super::*`.
use types_tuple::heaptuple::Datum;

thread_local! {
    /// Rows the mock outer subplan still has to produce.
    static OUTER_SUPPLY: Cell<usize> = const { Cell::new(0) };
    /// child `ExecReScan` invocations.
    static CHILD_RESCANS: Cell<usize> = const { Cell::new(0) };
    /// Scripted `ExecMakeFunctionResultSet` results, popped per call. The
    /// canonical `Datum<'mcx>` is lifetime-parameterized; the scripted scalars
    /// are by-value (`from_i32`/`null`), so they are held as `Datum<'static>`
    /// and coerce to the caller's `'mcx` on the way out.
    static SRF_RESULTS: RefCell<VecDeque<(Datum<'static>, bool, ExprDoneCond)>> =
        const { RefCell::new(VecDeque::new()) };
    /// Scripted `ExecEvalExpr` results, popped per call.
    static EVAL_RESULTS: RefCell<VecDeque<(Datum<'static>, bool)>> =
        const { RefCell::new(VecDeque::new()) };
    /// What the last `store_virtual_values` committed.
    static STORED: RefCell<Option<(Vec<Datum<'static>>, Vec<bool>)>> =
        const { RefCell::new(None) };
}

fn reset_state() {
    OUTER_SUPPLY.with(|c| c.set(0));
    CHILD_RESCANS.with(|c| c.set(0));
    SRF_RESULTS.with(|c| c.borrow_mut().clear());
    EVAL_RESULTS.with(|c| c.borrow_mut().clear());
    STORED.with(|c| *c.borrow_mut() = None);
}

// --- mock seam implementations -------------------------------------------

/// Re-bind a by-value canonical `Datum` to an arbitrary lifetime. The scripted
/// test scalars are all by-value (`from_i32`/`null`), which carry no borrow, so
/// they can cross the `thread_local!` `'static` boundary in either direction.
/// Panics on a by-reference value (the ProjectSet mock scenarios never produce
/// one).
fn rebind<'a>(d: Datum<'_>) -> Datum<'a> {
    Datum::from_usize(d.as_usize())
}

fn mock_check_for_interrupts() -> PgResult<()> {
    Ok(())
}

/// The mock outer child's `ExecProcNode`: produce a non-empty row while the
/// supply lasts, then the C `NULL` return.
fn supply_rows<'mcx>(
    _pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let remaining = OUTER_SUPPLY.with(|c| c.get());
    if remaining == 0 {
        return Ok(None);
    }
    OUTER_SUPPLY.with(|c| c.set(remaining - 1));
    let qcxt = estate.es_query_cxt;
    let id = estate.make_slot({
        let mut slot = TupleTableSlot::new_in(qcxt);
        slot.tts_flags = 0;
        slot
    })?;
    Ok(Some(id))
}

fn mock_exec_init_node<'mcx>(
    _mcx: Mcx<'mcx>,
    node: Option<&'mcx Node<'mcx>>,
    _estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<Option<PgBox<'mcx, PlanStateNode<'mcx>>>> {
    assert!(node.is_none(), "mock_exec_init_node: unexpected child node");
    Ok(None)
}

fn mock_exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let f = node.ps_head().ExecProcNode.expect("ExecProcNode installed");
    f(node, estate)
}

fn mock_exec_end_node<'mcx>(
    _node: &mut PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    Ok(())
}

fn mock_exec_re_scan<'mcx>(
    _node: &mut PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    CHILD_RESCANS.with(|c| c.set(c.get() + 1));
    Ok(())
}

fn mock_assign_expr_context<'mcx>(
    estate: &mut EStateData<'mcx>,
    planstate: &mut PlanStateData<'mcx>,
) -> PgResult<()> {
    let econtext = ExprContext {
        ecxt_scantuple: None,
        ecxt_innertuple: None,
        ecxt_outertuple: None,
        ecxt_oldtuple: None,
        ecxt_newtuple: None,
        ecxt_per_query_memory: estate.es_query_cxt,
        ecxt_per_tuple_memory: estate.es_query_cxt.context().new_child("ExprContext"),
        ecxt_aggvalues: PgVec::new_in(estate.es_query_cxt),
        ecxt_aggnulls: PgVec::new_in(estate.es_query_cxt),
        caseValue_datum: Default::default(),
        caseValue_isNull: true,
        domainValue_datum: Default::default(),
        domainValue_isNull: true,
        ecxt_callbacks: None,
        ecxt_param_list_info: None,
    };
    planstate.ps_ExprContext = Some(estate.add_expr_context(econtext)?);
    Ok(())
}

fn mock_init_result_slot<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    let qcxt = estate.es_query_cxt;
    let id = estate.make_slot(TupleTableSlot::new_in(qcxt))?;
    planstate.ps_ResultTupleSlot = Some(id);
    Ok(())
}

fn mock_clear_tuple<'mcx>(
    estate: &mut ::nodes::EStateData<'mcx>,
    slot: ::nodes::SlotId,
) -> PgResult<()> {
    estate.slot_mut(slot).tts_flags |= ::nodes::executor::TTS_FLAG_EMPTY;
    Ok(())
}

/// `ExecInitExpr`: an empty placeholder `ExprState` for a plain tlist entry.
fn mock_init_expr<'mcx>(
    _node: &Expr,
    _parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    alloc_in(estate.es_query_cxt, ExprState::default())
}

/// `ExecEvalExprSwitchContext`: pop a scripted `(datum, isnull)` result.
fn mock_eval_expr<'mcx>(
    _state: &mut ExprState<'mcx>,
    _econtext: ::nodes::EcxtId,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let (d, isnull) = EVAL_RESULTS
        .with(|c| c.borrow_mut().pop_front())
        .unwrap_or((Datum::null(), false));
    Ok((rebind(d), isnull))
}

/// `ExecInitFunctionResultSet`: a placeholder `SetExprState`.
fn mock_init_srf<'mcx>(
    _expr: &Expr,
    _econtext: ::nodes::EcxtId,
    _parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, SetExprState<'mcx>>> {
    let mut state = alloc_in(estate.es_query_cxt, SetExprState::default())?;
    state.funcReturnsSet = true;
    Ok(state)
}

/// `ExecMakeFunctionResultSet`: pop a scripted `(value, isnull, isdone)`.
fn mock_make_srf<'mcx>(
    _fcache: &mut SetExprState<'mcx>,
    _econtext: ::nodes::EcxtId,
    _arg_context: &MemoryContext,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool, ExprDoneCond)> {
    let (d, isnull, done) = SRF_RESULTS.with(|c| c.borrow_mut().pop_front()).unwrap_or((
        Datum::null(),
        true,
        ExprDoneCond::ExprEndResult,
    ));
    Ok((rebind(d), isnull, done))
}

/// `store_virtual_values`: record the committed values/nulls and mark the slot
/// non-empty (a stored virtual tuple).
fn mock_store_virtual_values<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot: SlotId,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    let stored: Vec<Datum<'static>> = values.iter().map(|d| rebind(d.clone())).collect();
    STORED.with(|c| *c.borrow_mut() = Some((stored, isnull.to_vec())));
    estate.slot_mut(slot).tts_flags &= !::nodes::executor::TTS_FLAG_EMPTY;
    Ok(())
}

fn install_mocks() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tcop_postgres::check_for_interrupts::set(mock_check_for_interrupts);
        execProcnode::exec_init_node::set(mock_exec_init_node);
        execProcnode::exec_proc_node::set(mock_exec_proc_node);
        execProcnode::exec_end_node::set(mock_exec_end_node);
        execAmi::exec_re_scan::set(mock_exec_re_scan);
        execUtils::exec_assign_expr_context::set(mock_assign_expr_context);
        execTuples::exec_init_result_tuple_slot_tl::set(mock_init_result_slot);
        execTuples::exec_clear_tuple::set(mock_clear_tuple);
        execTuples::store_virtual_values::set(mock_store_virtual_values);
        execExpr::exec_init_expr::set(mock_init_expr);
        execExpr::exec_eval_expr_switch_context::set(mock_eval_expr);
        execSRF::exec_init_function_result_set::set(mock_init_srf);
        execSRF::exec_make_function_result_set::set(mock_make_srf);
    });
}

// --- plan / state fixtures ------------------------------------------------

fn funcexpr(funcretset: bool) -> FuncExpr {
    FuncExpr {
        funcid: 0,
        funcresulttype: 0,
        funcretset,
        funcvariadic: false,
        funcformat: ::nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL,
        funccollid: 0,
        inputcollid: 0,
        args: Vec::new(),
        location: -1,
    }
}

fn srf_funcexpr() -> Expr {
    Expr::FuncExpr(funcexpr(true))
}

fn srf_opexpr() -> Expr {
    Expr::OpExpr(OpExpr {
        opretset: true,
        ..Default::default()
    })
}

fn plain_const() -> Expr {
    Expr::Const(Const::default())
}

/// Build a `ProjectSet` plan node whose targetlist carries the given column
/// expressions (each `te->expr`).
fn make_projectset_plan<'mcx>(mcx: Mcx<'mcx>, exprs: &[Expr]) -> PgResult<Node<'mcx>> {
    let mut plan = ProjectSetPlan::default();
    let mut tl = ::mcx::vec_with_capacity_in(mcx, exprs.len())?;
    for e in exprs {
        let mut te = TargetEntry::default();
        te.expr = Some(alloc_in(mcx, e.clone())?);
        tl.push(te);
    }
    plan.plan.targetlist = Some(tl);
    Ok(Node::mk_project_set(mcx, plan))
}

/// Initialize a `ProjectSetState` with the given tlist column expressions and
/// (optionally) an outer child supplying `outer_rows`.
fn init_state<'mcx>(
    mcx: Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    node: &'mcx Node<'mcx>,
) -> PgResult<PgBox<'mcx, ProjectSetState<'mcx>>> {
    ExecInitProjectSet(node, estate, 0)
}

/// Splice a leaf child into an initialized ProjectSetState so the outer-plan
/// path is exercised (init's mock returns None, modelling a leaf).
fn attach_leaf_child<'mcx>(state: &mut ProjectSetState<'mcx>, mcx: Mcx<'mcx>) {
    let mut leaf = ::nodes::noderesult::ResultState::default();
    leaf.ps.ExecProcNode = Some(supply_rows);
    state.ps.lefttree =
        Some(alloc_in(mcx, PlanStateNode::Result(alloc_in(mcx, leaf).unwrap())).unwrap());
}

// --- tests ----------------------------------------------------------------

#[test]
fn expr_returns_set_matches_c_discriminant() {
    assert!(expr_returns_set(&srf_funcexpr()));
    assert!(expr_returns_set(&srf_opexpr()));
    assert!(!expr_returns_set(&Expr::FuncExpr(funcexpr(false))));
    assert!(!expr_returns_set(&Expr::OpExpr(OpExpr {
        opretset: false,
        ..Default::default()
    })));
    assert!(!expr_returns_set(&plain_const()));
}

#[test]
fn init_classifies_elems_and_builds_workspace() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr(), plain_const()]).unwrap();
    let mut estate = EStateData::new_in(mcx);

    let state = init_state(mcx, &mut estate, &node).unwrap();
    assert_eq!(state.nelems, 2);
    let elems = state.elems.as_ref().unwrap();
    assert!(matches!(elems[0], ProjectSetElem::Srf(_)));
    assert!(matches!(elems[1], ProjectSetElem::Plain(_)));
    assert_eq!(state.elemdone.as_ref().unwrap().len(), 2);
    assert!(state.argcontext.is_some());
    assert!(state.ps.ps_ResultTupleSlot.is_some());
    assert!(state.ps.ps_ExprContext.is_some());
    assert!(!state.pending_srf_tuples);
}

/// A single SRF yielding a row (ExprMultipleResult) stores a virtual tuple,
/// sets pending_srf_tuples, and returns the result slot.
#[test]
fn project_srf_single_srf_with_result() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();

    SRF_RESULTS.with(|c| {
        c.borrow_mut()
            .push_back((Datum::from_i32(42), false, ExprDoneCond::ExprMultipleResult))
    });

    let produced = ExecProjectSRF(&mut state, false, &mut estate).unwrap();
    assert_eq!(produced, state.ps.ps_ResultTupleSlot);
    assert!(state.pending_srf_tuples);
    assert_eq!(state.elemdone.as_ref().unwrap()[0], ExprDoneCond::ExprMultipleResult);
    STORED.with(|c| {
        let s = c.borrow();
        let (vals, nulls) = s.as_ref().unwrap();
        assert_eq!(vals.len(), 1);
        assert!(!nulls[0]);
    });
}

/// An SRF returning ExprEndResult (empty set) => no row, returns None, nothing
/// stored.
#[test]
fn project_srf_empty_set_returns_none() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();

    SRF_RESULTS.with(|c| {
        c.borrow_mut()
            .push_back((Datum::null(), true, ExprDoneCond::ExprEndResult))
    });

    let produced = ExecProjectSRF(&mut state, false, &mut estate).unwrap();
    assert!(produced.is_none());
    assert!(!state.pending_srf_tuples);
    assert_eq!(state.elemdone.as_ref().unwrap()[0], ExprDoneCond::ExprEndResult);
    STORED.with(|c| assert!(c.borrow().is_none()));
}

/// continuing=true: an exhausted SRF (ExprEndResult) is filled with NULL
/// without re-evaluation; a second still-producing SRF drives the output row.
#[test]
fn project_srf_continuing_exhausted_emits_null() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr(), srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();

    // elem 0 already exhausted; elem 1 still producing.
    state.elemdone.as_mut().unwrap()[0] = ExprDoneCond::ExprEndResult;
    SRF_RESULTS.with(|c| {
        c.borrow_mut()
            .push_back((Datum::from_i32(7), false, ExprDoneCond::ExprMultipleResult))
    });

    let produced = ExecProjectSRF(&mut state, true, &mut estate).unwrap();
    assert_eq!(produced, state.ps.ps_ResultTupleSlot);
    // elem 0 stays exhausted (was not re-evaluated); elem 1 produced.
    assert_eq!(state.elemdone.as_ref().unwrap()[0], ExprDoneCond::ExprEndResult);
    assert_eq!(state.elemdone.as_ref().unwrap()[1], ExprDoneCond::ExprMultipleResult);
    assert!(state.pending_srf_tuples);
    STORED.with(|c| {
        let s = c.borrow();
        let (vals, nulls) = s.as_ref().unwrap();
        assert!(nulls[0]); // exhausted SRF column => NULL
        assert_eq!(vals[1], Datum::from_i32(7));
        assert!(!nulls[1]);
    });
}

/// A plain expr + an exhausted SRF: plain sets ExprSingleResult, but no SRF
/// produced => no row.
#[test]
fn project_srf_plain_plus_exhausted_no_row() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[plain_const(), srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();

    EVAL_RESULTS.with(|c| c.borrow_mut().push_back((Datum::from_i32(5), false)));
    SRF_RESULTS.with(|c| {
        c.borrow_mut()
            .push_back((Datum::null(), true, ExprDoneCond::ExprEndResult))
    });

    let produced = ExecProjectSRF(&mut state, false, &mut estate).unwrap();
    assert!(produced.is_none());
    assert_eq!(state.elemdone.as_ref().unwrap()[0], ExprDoneCond::ExprSingleResult);
    STORED.with(|c| assert!(c.borrow().is_none()));
}

/// `ExecProjectSet`: a pending continuation returns a row immediately (no outer
/// fetch).
#[test]
fn exec_project_set_pending_continuation_returns_row() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();
    state.pending_srf_tuples = true;

    SRF_RESULTS.with(|c| {
        c.borrow_mut()
            .push_back((Datum::from_i32(9), false, ExprDoneCond::ExprMultipleResult))
    });

    let produced = ExecProjectSet(&mut state, &mut estate).unwrap();
    assert_eq!(produced, state.ps.ps_ResultTupleSlot);
    assert!(produced.is_some());
}

/// `ExecProjectSet`: end of outer plan => None (the C NULL).
#[test]
fn exec_project_set_outer_eof_returns_none() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();
    attach_leaf_child(&mut state, mcx);
    OUTER_SUPPLY.with(|c| c.set(0)); // outer immediately at EOF

    let produced = ExecProjectSet(&mut state, &mut estate).unwrap();
    assert!(produced.is_none());
}

/// `ExecReScanProjectSet`: forgets pending SRFs and rescans the child when its
/// chgParam is null.
#[test]
fn rescan_clears_pending_and_rescans_child() {
    install_mocks();
    reset_state();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = make_projectset_plan(mcx, &[srf_funcexpr()]).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut state = init_state(mcx, &mut estate, &node).unwrap();
    attach_leaf_child(&mut state, mcx);
    state.pending_srf_tuples = true;

    ExecReScanProjectSet(&mut state, &mut estate).unwrap();
    assert!(!state.pending_srf_tuples);
    assert_eq!(CHILD_RESCANS.with(|c| c.get()), 1);
}
