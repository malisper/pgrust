//! Logic tests for the group node, driving the real `ExecGroup` /
//! `ExecInitGroup` / `ExecEndGroup` / `ExecReScanGroup` against mock installs of
//! the unported owners' seams (execProcnode, execAmi, execTuples, execUtils,
//! execExpr, execGrouping, interrupts). Per-test state is `thread_local!`
//! (never a shared static).

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::Once;

use mcx::{alloc_in, Mcx, MemoryContext, PgBox, PgVec};
use nodes::execnodes::{ExprContext, PlanStateData};
use nodes::executor::TupleSlotKind;
use nodes::nodes::Node;
use nodes::TupleTableSlot;

use super::*;

thread_local! {
    /// Queue of outer-tuple availabilities yielded by the mock child
    /// `ExecProcNode` (`true` => non-empty tuple, `false` => EOF / C NULL).
    static OUTER: RefCell<VecDeque<bool>> = const { RefCell::new(VecDeque::new()) };
    /// Queue of HAVING `ExecQual` verdicts.
    static QUAL: RefCell<VecDeque<bool>> = const { RefCell::new(VecDeque::new()) };
    /// Queue of group-boundary `ExecQualAndReset` verdicts (eqfunction).
    static EQRESET: RefCell<VecDeque<bool>> = const { RefCell::new(VecDeque::new()) };
    /// child `ExecReScan` invocation count.
    static CHILD_RESCANS: Cell<usize> = const { Cell::new(0) };
    /// child `ExecEndNode` invocation count.
    static CHILD_ENDS: Cell<usize> = const { Cell::new(0) };
}

fn reset_queues() {
    OUTER.with(|c| c.borrow_mut().clear());
    QUAL.with(|c| c.borrow_mut().clear());
    EQRESET.with(|c| c.borrow_mut().clear());
    CHILD_RESCANS.with(|c| c.set(0));
    CHILD_ENDS.with(|c| c.set(0));
}

// --- mock seam implementations -------------------------------------------

fn mock_check_for_interrupts() -> PgResult<()> {
    Ok(())
}

/// The mock outer child's `ExecProcNode`: pop the next availability from the
/// OUTER queue; `true` => store a fresh non-empty slot and return it, `false`
/// (or empty queue) => the C `NULL` return.
fn supply_rows<'mcx>(
    _pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let have = OUTER.with(|c| c.borrow_mut().pop_front()).unwrap_or(false);
    if !have {
        return Ok(None);
    }
    let qcxt = estate.es_query_cxt;
    let id = estate.make_slot({
        let mut slot = TupleTableSlot::new_in(qcxt);
        slot.tts_flags = 0;
        slot
    })?;
    Ok(Some(id))
}

fn mock_exec_proc_node<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let f = node.ps_head().ExecProcNode.expect("ExecProcNode installed");
    f(node, estate)
}

fn mock_exec_init_node<'mcx>(
    mcx: Mcx<'mcx>,
    _node: Option<&'mcx Node<'mcx>>,
    _estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<Option<PgBox<'mcx, PlanStateNode<'mcx>>>> {
    // Build a leaf child whose `ExecProcNode` is the scripted `supply_rows`
    // and whose result type is a (trimmed) descriptor so the init path's
    // `ExecGetResultType(outerPlanState(...))` returns a real descriptor. A
    // `Group` state with no child of its own is used purely as a dispatch
    // carrier here.
    let mut leaf = GroupStateData::default();
    leaf.ss.ps.ExecProcNode = Some(supply_rows);
    leaf.ss.ps.ps_ResultTupleDesc = Some(alloc_in(
        mcx,
        types_tuple::heaptuple::TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        },
    )?);
    Ok(Some(alloc_in(
        mcx,
        PlanStateNode::Group(alloc_in(mcx, leaf)?),
    )?))
}

fn mock_exec_end_node<'mcx>(
    _node: &mut PlanStateNode<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    CHILD_ENDS.with(|c| c.set(c.get() + 1));
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

fn mock_create_scan_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanstate: &mut nodes::execnodes::ScanStateData<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    // The C builds the scan slot empty (TTS_EMPTY set).
    let qcxt = estate.es_query_cxt;
    let id = estate.make_slot(TupleTableSlot::new_in(qcxt))?;
    scanstate.ss_ScanTupleSlot = Some(id);
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

fn mock_init_qual<'mcx>(
    qual: Option<&[nodes::primnodes::Expr]>,
    _parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, nodes::execexpr::ExprState<'mcx>>>> {
    match qual {
        Some(q) if !q.is_empty() => Ok(Some(alloc_in(
            estate.es_query_cxt,
            nodes::execexpr::ExprState::default(),
        )?)),
        _ => Ok(None),
    }
}

/// `ExecQual`: the HAVING verdict, popped from the QUAL queue (default true).
fn mock_qual<'mcx>(
    _state: &mut nodes::execexpr::ExprState<'mcx>,
    _econtext: nodes::EcxtId,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    Ok(QUAL.with(|c| c.borrow_mut().pop_front()).unwrap_or(true))
}

/// `ExecQualAndReset`: the group-boundary verdict, popped from the EQRESET
/// queue (default false => boundary).
fn mock_qual_and_reset<'mcx>(
    _state: &mut nodes::execexpr::ExprState<'mcx>,
    _econtext: nodes::EcxtId,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    Ok(EQRESET.with(|c| c.borrow_mut().pop_front()).unwrap_or(false))
}

/// `ExecProject`: return the node's result slot (the C `ExecProject` returned
/// slot).
fn mock_project<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    Ok(planstate
        .ps_ResultTupleSlot
        .expect("ExecProject: result slot present"))
}

/// `ExecCopySlot(dst, src)`: faithful to the C — the destination scan slot now
/// holds a tuple, so it is no longer empty.
fn mock_copy_slot<'mcx>(
    estate: &mut nodes::EStateData<'mcx>,
    dstslot: nodes::SlotId,
    _srcslot: nodes::SlotId,
) -> PgResult<()> {
    estate.slot_mut(dstslot).tts_flags &= !nodes::executor::TTS_FLAG_EMPTY;
    Ok(())
}

/// `ExecClearTuple(slot)`: mark the slot empty (TTS_EMPTY).
fn mock_clear_tuple<'mcx>(
    estate: &mut nodes::EStateData<'mcx>,
    slot: nodes::SlotId,
) -> PgResult<()> {
    estate.slot_mut(slot).tts_flags |= nodes::executor::TTS_FLAG_EMPTY;
    Ok(())
}

/// `ExecGetResultSlotOps(outer, NULL)`: any class; tests don't care.
fn mock_get_result_slot_ops(_planstate: &PlanStateData) -> TupleSlotKind {
    TupleSlotKind::Virtual
}

/// `ExecGetResultType(outer)`: a fresh empty descriptor.
fn mock_get_result_type<'a, 'mcx>(
    planstate: &'a PlanStateData<'mcx>,
) -> Option<&'a types_tuple::heaptuple::TupleDescData<'mcx>> {
    planstate.ps_ResultTupleDesc.as_deref()
}

/// `execTuplesMatchPrepare(...)`: a placeholder ExprState (the inner-loop
/// eqfunction). A zero-column key would be `None` in the C, but the tests use
/// a one-column key so a real state is built.
fn mock_tuples_match_prepare<'mcx>(
    _desc: Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
    num_cols: i32,
    _key_col_idx: &[types_core::primitive::AttrNumber],
    _eq_operators: &[types_core::primitive::Oid],
    _collations: &[types_core::primitive::Oid],
    _parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<PgBox<'mcx, nodes::execexpr::ExprState<'mcx>>>> {
    if num_cols == 0 {
        return Ok(None);
    }
    Ok(Some(alloc_in(
        estate.es_query_cxt,
        nodes::execexpr::ExprState::default(),
    )?))
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
        execUtils::exec_create_scan_slot_from_outer_plan::set(mock_create_scan_slot);
        execUtils::exec_assign_projection_info::set(mock_assign_projection_info);
        execTuples::exec_init_result_tuple_slot_tl::set(mock_init_result_slot);
        execTuples::exec_copy_slot::set(mock_copy_slot);
        execTuples::exec_clear_tuple::set(mock_clear_tuple);
        execTuples::exec_get_result_slot_ops::set(mock_get_result_slot_ops);
        execTuples::exec_get_result_type::set(mock_get_result_type);
        execExpr::exec_init_qual::set(mock_init_qual);
        execExpr::exec_qual::set(mock_qual);
        execExpr::exec_qual_and_reset::set(mock_qual_and_reset);
        execExpr::exec_project::set(mock_project);
        execGrouping::exec_tuples_match_prepare::set(mock_tuples_match_prepare);
    });
}

// --- helpers --------------------------------------------------------------

/// Build a `Group` plan node with one grouping column and an optional HAVING
/// qual (presence drives a non-NULL `ps.qual`).
fn make_group_plan<'mcx>(mcx: Mcx<'mcx>, with_having: bool) -> PgResult<Node<'mcx>> {
    let mut g = Group {
        plan: Default::default(),
        numCols: 1,
        grpColIdx: mcx::vec_with_capacity_in(mcx, 1)?,
        grpOperators: mcx::vec_with_capacity_in(mcx, 1)?,
        grpCollations: mcx::vec_with_capacity_in(mcx, 1)?,
    };
    g.grpColIdx.push(1);
    g.grpOperators.push(96); // BooleanEqualOperator OID, arbitrary
    g.grpCollations.push(0);
    if with_having {
        let mut list = mcx::vec_with_capacity_in(mcx, 1)?;
        list.push(nodes::primnodes::Expr::Const(
            nodes::primnodes::Const::default(),
        ));
        g.plan.qual = Some(list);
    }
    Ok(Node::mk_group(mcx, g))
}

// --- tests ----------------------------------------------------------------

#[test]
fn group_done_short_circuits() {
    install_mocks();
    reset_queues();
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    st.grp_done = true;
    let out = ExecGroup(&mut st, &mut estate).unwrap();
    assert!(out.is_none());
}

#[test]
fn empty_input_returns_none_and_sets_grp_done() {
    install_mocks();
    reset_queues();
    OUTER.with(|c| c.borrow_mut().push_back(false)); // child EOF
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    let out = ExecGroup(&mut st, &mut estate).unwrap();
    assert!(out.is_none());
    assert!(st.grp_done);
}

#[test]
fn first_group_passes_having_projects() {
    install_mocks();
    reset_queues();
    OUTER.with(|c| c.borrow_mut().push_back(true)); // one tuple
    QUAL.with(|c| c.borrow_mut().push_back(true)); // HAVING passes
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, true).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    let out = ExecGroup(&mut st, &mut estate).unwrap();
    assert!(out.is_some());
    assert!(!st.grp_done);
}

#[test]
fn first_group_fails_having_then_scans_to_next_group() {
    install_mocks();
    reset_queues();
    // t1, t2, t3 then EOF is not reached; t1 fails HAVING, scan loop:
    // t2 same group (eqreset true), t3 different (eqreset false) -> break,
    // t3 passes HAVING.
    OUTER.with(|c| c.borrow_mut().extend([true, true, true]));
    QUAL.with(|c| c.borrow_mut().extend([false, true]));
    EQRESET.with(|c| c.borrow_mut().extend([true, false]));
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, true).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    let out = ExecGroup(&mut st, &mut estate).unwrap();
    assert!(out.is_some());
    assert!(!st.grp_done);
}

#[test]
fn second_group_reached_after_returning_first() {
    install_mocks();
    reset_queues();
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, true).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    // First call: one tuple, HAVING passes -> first group returned, firsttuple
    // slot now non-empty.
    OUTER.with(|c| c.borrow_mut().push_back(true));
    QUAL.with(|c| c.borrow_mut().push_back(true));
    let out = ExecGroup(&mut st, &mut estate).unwrap();
    assert!(out.is_some());
    // Follow-up call resumes the scan loop: rest-of-group-1 (eqreset true),
    // first-of-group-2 (eqreset false -> break), group 2 passes HAVING.
    OUTER.with(|c| c.borrow_mut().extend([true, true]));
    EQRESET.with(|c| c.borrow_mut().extend([true, false]));
    QUAL.with(|c| c.borrow_mut().push_back(true));
    let out = ExecGroup(&mut st, &mut estate).unwrap();
    assert!(out.is_some());
}

#[test]
fn end_group_ends_outer() {
    install_mocks();
    reset_queues();
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    ExecEndGroup(&mut st, &mut estate).unwrap();
    assert_eq!(CHILD_ENDS.with(|c| c.get()), 1);
}

#[test]
fn rescan_resets_and_rescans_outer_when_unchanged() {
    install_mocks();
    reset_queues();
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    st.grp_done = true;
    // child chgParam is None (default) => ExecReScan called.
    ExecReScanGroup(&mut st, &mut estate).unwrap();
    assert!(!st.grp_done);
    assert_eq!(CHILD_RESCANS.with(|c| c.get()), 1);
    // scan slot cleared (TTS_EMPTY restored).
    let scan = st.ss.ss_ScanTupleSlot.unwrap();
    assert!(estate.slot(scan).is_empty());
}

#[test]
fn rescan_skips_outer_rescan_when_child_has_chgparam() {
    install_mocks();
    reset_queues();
    let cx = MemoryContext::new("per-query");
    let mcx = cx.mcx();
    let plan = make_group_plan(mcx, false).unwrap();
    let mut estate = EStateData::new_in(mcx);
    let mut st = ExecInitGroup(&plan, &mut estate, 0).unwrap();
    // Give the child a non-empty chgParam so ExecReScan is NOT called.
    {
        let outer = st.ss.ps.lefttree.as_deref_mut().unwrap();
        outer.ps_head_mut().chgParam =
            Some(alloc_in(mcx, nodes::bitmapset::Bitmapset { words: PgVec::new_in(mcx) }).unwrap());
    }
    ExecReScanGroup(&mut st, &mut estate).unwrap();
    assert_eq!(CHILD_RESCANS.with(|c| c.get()), 0);
}
