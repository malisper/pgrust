//! Logic tests for the result node, driving the real `ExecResult` /
//! `ExecInitResult` / `ExecReScanResult` / `ExecResult{Mark,Restr}Pos` against
//! mock installs of the unported owners' seams (execProcnode, execAmi,
//! execTuples, execUtils, execExpr, interrupts). Per-test state is
//! `thread_local!` (never a shared static).

use std::cell::Cell;
use std::sync::Once;

use mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgVec};
use types_nodes::execnodes::{ExprContext, PlanStateData};
use types_nodes::executor::TupleSlotKind;
use types_nodes::nodes::Node;

use super::*;
use types_nodes::TupleTableSlot;

thread_local! {
    /// Value the next mock `ExecQual(resconstantqual)` should return.
    static QUAL_RESULT: Cell<bool> = const { Cell::new(true) };
    /// Rows the mock outer subplan still has to produce.
    static OUTER_SUPPLY: Cell<usize> = const { Cell::new(0) };
    /// `ExecProject` invocations (each forms one output row).
    static PROJECTIONS: Cell<usize> = const { Cell::new(0) };
    /// child `ExecReScan` invocations.
    static CHILD_RESCANS: Cell<usize> = const { Cell::new(0) };
    /// child `ExecMarkPos` invocations.
    static CHILD_MARKS: Cell<usize> = const { Cell::new(0) };
    /// child `ExecRestrPos` invocations.
    static CHILD_RESTRS: Cell<usize> = const { Cell::new(0) };
}

fn reset_counters() {
    QUAL_RESULT.with(|c| c.set(true));
    OUTER_SUPPLY.with(|c| c.set(0));
    PROJECTIONS.with(|c| c.set(0));
    CHILD_RESCANS.with(|c| c.set(0));
    CHILD_MARKS.with(|c| c.set(0));
    CHILD_RESTRS.with(|c| c.set(0));
}

// --- mock seam implementations -------------------------------------------

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
    // Result tests splice children explicitly; the planner's outer plan is
    // always NULL here (a leaf Result).
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

fn mock_exec_mark_pos<'mcx>(
    _node: &mut PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    CHILD_MARKS.with(|c| c.set(c.get() + 1));
    Ok(())
}

fn mock_exec_restr_pos<'mcx>(
    _node: &mut PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    CHILD_RESTRS.with(|c| c.set(c.get() + 1));
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

fn mock_assign_projection_info<'mcx>(
    _planstate: &mut PlanStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _input_desc: Option<&types_tuple::heaptuple::TupleDescData<'_>>,
) -> PgResult<()> {
    Ok(())
}

/// `ExecInitQual`: a `None`/empty list compiles to `None` (the C `NULL`
/// always-true ExprState); otherwise an empty placeholder `ExprState`.
fn mock_init_qual<'mcx>(
    qual: Option<&[types_nodes::primnodes::Expr]>,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> {
    let _ = parent;
    match qual {
        Some(q) if !q.is_empty() => {
            Ok(Some(alloc_in(estate.es_query_cxt, types_nodes::execexpr::ExprState::default())?))
        }
        _ => Ok(None),
    }
}

/// `ExecQual`: return the thread-local verdict (the constant qual gate).
fn mock_qual<'mcx>(
    _state: &mut types_nodes::execexpr::ExprState<'mcx>,
    _econtext: types_nodes::EcxtId,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    Ok(QUAL_RESULT.with(|c| c.get()))
}

/// `ExecProject`: count a projection and return the node's result slot.
fn mock_project<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    PROJECTIONS.with(|c| c.set(c.get() + 1));
    Ok(planstate
        .ps_ResultTupleSlot
        .expect("ExecProject: result slot present"))
}

fn install_mocks() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tcop_postgres::check_for_interrupts::set(mock_check_for_interrupts);
        execProcnode::exec_init_node::set(mock_exec_init_node);
        execProcnode::exec_proc_node::set(mock_exec_proc_node);
        execProcnode::exec_end_node::set(mock_exec_end_node);
        execAmi::exec_re_scan::set(mock_exec_re_scan);
        execAmi::exec_mark_pos::set(mock_exec_mark_pos);
        execAmi::exec_restr_pos::set(mock_exec_restr_pos);
        execUtils::exec_assign_expr_context::set(mock_assign_expr_context);
        execUtils::exec_assign_projection_info::set(mock_assign_projection_info);
        execTuples::exec_init_result_tuple_slot_tl::set(mock_init_result_slot);
        execExpr::exec_init_qual::set(mock_init_qual);
        execExpr::exec_qual::set(mock_qual);
        execExpr::exec_project::set(mock_project);
    });
}

/// Build a Result plan node carrying an optional one-line `resconstantqual`
/// (the actual expression content is irrelevant — the mock `ExecQual` returns
/// the thread-local verdict; only presence/absence drives `rs_checkqual`).
fn make_result_plan<'mcx>(mcx: Mcx<'mcx>, with_constqual: bool) -> PgResult<Node<'mcx>> {
    let mut plan = ResultPlan::default();
    if with_constqual {
        let mut list = mcx::vec_with_capacity_in(mcx, 1)?;
        list.push(types_nodes::primnodes::Expr::Const(
            types_nodes::primnodes::Const::default(),
        ));
        plan.resconstantqual = Some(list);
    }
    Ok(Node::Result(plan))
}

/// Splice a leaf child into an initialized ResultState so the
/// outer-plan-present path is exercised (init's mock_exec_init_node always
/// returns None, modelling a leaf Result).
fn attach_leaf_child<'mcx>(node: &mut ResultState<'mcx>, mcx: Mcx<'mcx>) {
    let mut leaf = ResultState::default();
    leaf.ps.ExecProcNode = Some(supply_rows);
    node.ps.lefttree =
        Some(alloc_in(mcx, PlanStateNode::Result(alloc_in(mcx, leaf).unwrap())).unwrap());
}

// --- tests ----------------------------------------------------------------

#[test]
fn init_sets_checkqual_from_resconstantqual_presence() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");

    // resconstantqual present => rs_checkqual = true.
    let plan = make_result_plan(ctx.mcx(), true).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    assert!(!st.rs_done);
    assert!(st.rs_checkqual);
    assert!(st.resconstantqual.is_some(), "constant qual was compiled");
    assert!(st.ps.ps_ExprContext.is_some());
    assert!(st.ps.ps_ResultTupleSlot.is_some());
    assert!(st.ps.lefttree.is_none());
}

#[test]
fn init_without_constqual_leaves_checkqual_false() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), false).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    assert!(!st.rs_checkqual);
    assert!(st.resconstantqual.is_none());
}

#[test]
fn constant_target_list_returns_one_tuple_then_null() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), false).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();

    // No outer plan => generate the constant target list exactly once.
    let first = ExecResult(&mut st, &mut estate).unwrap();
    assert!(first.is_some(), "first call yields the constant tuple");
    assert!(st.rs_done, "rs_done set after producing the constant row");
    assert_eq!(PROJECTIONS.with(|c| c.get()), 1);

    let second = ExecResult(&mut st, &mut estate).unwrap();
    assert!(second.is_none(), "second call yields NULL (already done)");
    assert_eq!(PROJECTIONS.with(|c| c.get()), 1, "no further projection");
}

#[test]
fn one_time_filter_false_returns_empty_set() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), true).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();

    QUAL_RESULT.with(|c| c.set(false));
    let r = ExecResult(&mut st, &mut estate).unwrap();
    assert!(r.is_none(), "false one-time filter returns NULL");
    assert!(!st.rs_checkqual, "qual checked exactly once");
    assert!(st.rs_done, "node marked done on failed constant qual");
    assert_eq!(PROJECTIONS.with(|c| c.get()), 0, "controlled plan not run");

    // Stays empty on subsequent calls.
    assert!(ExecResult(&mut st, &mut estate).unwrap().is_none());
}

#[test]
fn one_time_filter_true_then_runs_constant_projection() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), true).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();

    QUAL_RESULT.with(|c| c.set(true));
    let r = ExecResult(&mut st, &mut estate).unwrap();
    assert!(r.is_some());
    assert!(!st.rs_checkqual);
    assert!(st.rs_done);
    assert_eq!(PROJECTIONS.with(|c| c.get()), 1);
}

#[test]
fn outer_plan_rows_are_projected_then_exhausted() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let plan = make_result_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    attach_leaf_child(&mut st, mcx);

    OUTER_SUPPLY.with(|c| c.set(2));

    // Two rows from the outer plan, each projected; rs_done stays false (an
    // outer plan is present).
    assert!(ExecResult(&mut st, &mut estate).unwrap().is_some());
    assert!(!st.rs_done);
    assert!(ExecResult(&mut st, &mut estate).unwrap().is_some());
    assert!(!st.rs_done);
    assert_eq!(PROJECTIONS.with(|c| c.get()), 2);

    // Outer exhausted => NULL, no projection.
    assert!(ExecResult(&mut st, &mut estate).unwrap().is_none());
    assert_eq!(PROJECTIONS.with(|c| c.get()), 2);
}

#[test]
fn rescan_resets_done_and_checkqual_and_rescans_child() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let plan = make_result_plan(mcx, true).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    attach_leaf_child(&mut st, mcx);

    // Drive it done.
    QUAL_RESULT.with(|c| c.set(true));
    OUTER_SUPPLY.with(|c| c.set(0));
    let _ = ExecResult(&mut st, &mut estate).unwrap();
    st.rs_checkqual = false;

    ExecReScanResult(&mut st, &mut estate).unwrap();
    assert!(!st.rs_done);
    assert!(st.rs_checkqual, "rs_checkqual restored from resconstantqual");
    // child chgParam is None (default) => ExecReScan(outerPlan) was called.
    assert_eq!(CHILD_RESCANS.with(|c| c.get()), 1);
}

#[test]
fn rescan_without_outer_plan_skips_child_rescan() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), false).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    st.rs_done = true;

    ExecReScanResult(&mut st, &mut estate).unwrap();
    assert!(!st.rs_done);
    assert!(!st.rs_checkqual, "no resconstantqual => rs_checkqual false");
    assert_eq!(CHILD_RESCANS.with(|c| c.get()), 0);
}

#[test]
fn markpos_restrpos_delegate_to_child_when_present() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let plan = make_result_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    attach_leaf_child(&mut st, mcx);

    ExecResultMarkPos(&mut st, &mut estate).unwrap();
    ExecResultRestrPos(&mut st, &mut estate).unwrap();
    assert_eq!(CHILD_MARKS.with(|c| c.get()), 1);
    assert_eq!(CHILD_RESTRS.with(|c| c.get()), 1);
}

#[test]
fn markpos_without_child_is_debug_noop() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), false).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();

    // No outer plan: ExecResultMarkPos only elog(DEBUG2) — Ok and no child.
    ExecResultMarkPos(&mut st, &mut estate).unwrap();
    assert_eq!(CHILD_MARKS.with(|c| c.get()), 0);
}

#[test]
fn restrpos_without_child_errors() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), false).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();

    // No outer plan: ExecResultRestrPos elog(ERROR) — Err.
    assert!(ExecResultRestrPos(&mut st, &mut estate).is_err());
    assert_eq!(CHILD_RESTRS.with(|c| c.get()), 0);
}

#[test]
fn end_result_is_ok() {
    install_mocks();
    reset_counters();
    let ctx = MemoryContext::new("per-query");
    let plan = make_result_plan(ctx.mcx(), false).unwrap();
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = ExecInitResult(&plan, &mut estate, 0).unwrap();
    ExecEndResult(&mut st, &mut estate).unwrap();
}
