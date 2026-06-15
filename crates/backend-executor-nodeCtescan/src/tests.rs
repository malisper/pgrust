//! Logic tests for the CteScan node, driving the real `CteScanNext` /
//! `ExecCteScan` / `ExecInitCteScan` / `ExecEndCteScan` / `ExecReScanCteScan`
//! against mock installs of the unported owners' seams (execMain's `cte_*`
//! leader-aliased operations, execScan, execTuples, execUtils, execExpr).
//!
//! The node-machine *control flow* (which operation runs, when, branching on
//! `forward`/`eof_tuplestore`/`eof_cte`, the leader-vs-follower init, and the
//! `ExecReScanCteScan` `chgParam` branch) is the in-crate logic under test; the
//! seams are scripted recorders. Per-test state is `thread_local!`.

use std::cell::{Cell, RefCell};
use std::sync::Once;

use mcx::{Mcx, MemoryContext};
use types_nodes::{ScanDirection, TupleTableSlot};

use super::*;

thread_local! {
    static LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static EOF_CTE: Cell<bool> = const { Cell::new(false) };
    static ATEOF: RefCell<std::collections::VecDeque<bool>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
    static GETTUPLE: RefCell<std::collections::VecDeque<bool>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
    static ADVANCE: RefCell<std::collections::VecDeque<bool>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
    static PROCNODE: RefCell<std::collections::VecDeque<bool>> =
        const { RefCell::new(std::collections::VecDeque::new()) };
    static HAS_RESULT_SLOT: Cell<bool> = const { Cell::new(false) };
    static CHGPARAM: Cell<bool> = const { Cell::new(false) };
    static IS_LEADER: Cell<bool> = const { Cell::new(true) };
}

static INSTALL: Once = Once::new();

fn push(s: &str) {
    LOG.with(|l| l.borrow_mut().push(s.to_string()));
}

fn reset() {
    LOG.with(|l| l.borrow_mut().clear());
    EOF_CTE.with(|c| c.set(false));
    ATEOF.with(|q| q.borrow_mut().clear());
    GETTUPLE.with(|q| q.borrow_mut().clear());
    ADVANCE.with(|q| q.borrow_mut().clear());
    PROCNODE.with(|q| q.borrow_mut().clear());
    HAS_RESULT_SLOT.with(|c| c.set(false));
    CHGPARAM.with(|c| c.set(false));
    IS_LEADER.with(|c| c.set(true));
}

fn log_eq(expected: &[&str]) {
    LOG.with(|l| assert_eq!(l.borrow().as_slice(), expected));
}

fn install() {
    use execMain::*;
    cte_tuplestore_select_read_pointer::set(|_| {
        push("select_read_pointer");
        Ok(())
    });
    cte_tuplestore_ateof::set(|_| {
        push("ateof");
        Ok(ATEOF.with(|q| q.borrow_mut().pop_front().unwrap()))
    });
    cte_leader_eof_cte::set(|_| Ok(EOF_CTE.with(|c| c.get())));
    cte_set_leader_eof_cte::set(|_, v| {
        EOF_CTE.with(|c| c.set(v));
        push("set_leader_eof_cte");
        Ok(())
    });
    cte_tuplestore_advance::set(|_, _| {
        push("advance");
        Ok(ADVANCE.with(|q| q.borrow_mut().pop_front().unwrap()))
    });
    cte_tuplestore_gettupleslot::set(|_, _, _| {
        push("gettupleslot");
        Ok(GETTUPLE.with(|q| q.borrow_mut().pop_front().unwrap()))
    });
    cte_tuplestore_puttupleslot::set(|_, _| {
        push("puttupleslot");
        Ok(())
    });
    cte_copy_tuple_to_scan_slot::set(|_, _| {
        push("CopySlot");
        Ok(())
    });
    cte_exec_proc_node::set(|_, _| {
        push("ExecProcNode");
        Ok(PROCNODE.with(|q| q.borrow_mut().pop_front().unwrap()))
    });
    cte_tuplestore_rescan::set(|_| {
        push("rescan");
        Ok(())
    });
    cte_tuplestore_clear::set(|_| {
        push("clear");
        Ok(())
    });
    cte_tuplestore_end::set(|_| {
        push("end");
        Ok(())
    });
    cte_tuplestore_begin_heap_leader::set(|_| {
        push("begin_heap_leader");
        Ok(())
    });
    cte_tuplestore_alloc_read_pointer_follower::set(|_| {
        push("alloc_read_pointer_follower");
        Ok(())
    });
    cte_link_plan_state::set(|scanstate, _, estate| {
        push("link_cte_plan_state");
        // The real seam links scanstate->cteplanstate from es_subplanstates;
        // the mock stands in a minimal Result subplan so the in-crate
        // ExecGetResultType read in init_scan_tuple_slot_from_cte has a node.
        let sub = mcx::alloc_in(
            estate.es_query_cxt,
            types_nodes::noderesult::ResultState::default(),
        )?;
        scanstate.cteplanstate = Some(mcx::alloc_in(
            estate.es_query_cxt,
            types_nodes::PlanStateNode::Result(sub),
        )?);
        Ok(())
    });
    cte_resolve_leader::set(|_, _, _| {
        push("resolve_cte_leader");
        Ok(IS_LEADER.with(|c| c.get()))
    });
    cte_leader_cteplanstate_chgparam_set::set(|_| Ok(CHGPARAM.with(|c| c.get())));

    // execScan / execTuples / execUtils / execExpr leaf seams.
    execScan::exec_scan_cte::set(|_, _, _, _| {
        push("ExecScan");
        Ok(None)
    });
    execScan::exec_scan_rescan_cte::set(|_, _| {
        push("ExecScanReScan");
        Ok(())
    });
    execScan::exec_assign_scan_projection_info_cte::set(|_, _| {
        push("assign_scan_projection_info");
        Ok(())
    });
    execTuples::exec_clear_tuple::set(|_, _| {
        push("ClearTuple");
        Ok(())
    });
    execTuples::exec_get_result_type::set(|_| None);
    execTuples::exec_init_scan_tuple_slot::set(|_, _, _, _| {
        push("init_scan_tuple_slot");
        Ok(())
    });
    execTuples::exec_init_result_type_tl::set(|_, _| {
        push("init_result_type_tl");
        Ok(())
    });
    execUtils::exec_assign_expr_context::set(|_, _| {
        push("assign_expr_context");
        Ok(())
    });
    execExpr::exec_init_qual::set(|_, _, _| {
        push("init_qual");
        Ok(None)
    });
}

fn setup() {
    INSTALL.call_once(install);
    reset();
}

fn estate<'mcx>(mcx: Mcx<'mcx>) -> EStateData<'mcx> {
    EStateData::new_in(mcx)
}

fn node_in<'mcx>(mcx: Mcx<'mcx>) -> CteScanState<'mcx> {
    CteScanState::new_in(mcx)
}

// --- CteScanNext state-machine arms ---------------------------------------

#[test]
fn next_returns_tuplestore_tuple_when_available() {
    setup();
    ATEOF.with(|q| q.borrow_mut().push_back(false));
    GETTUPLE.with(|q| q.borrow_mut().push_back(true));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    assert!(CteScanNext(&mut node, &mut estate).unwrap());
    log_eq(&["select_read_pointer", "ateof", "gettupleslot"]);
}

#[test]
fn next_pulls_from_subplan_and_appends_when_tuplestore_empty() {
    setup();
    ATEOF.with(|q| q.borrow_mut().push_back(true));
    PROCNODE.with(|q| q.borrow_mut().push_back(true));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    assert!(CteScanNext(&mut node, &mut estate).unwrap());
    log_eq(&[
        "select_read_pointer",
        "ateof",
        "ExecProcNode",
        "select_read_pointer",
        "puttupleslot",
        "CopySlot",
    ]);
}

#[test]
fn next_sets_eof_cte_and_returns_false_when_subplan_exhausted() {
    setup();
    ATEOF.with(|q| q.borrow_mut().push_back(true));
    PROCNODE.with(|q| q.borrow_mut().push_back(false));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    assert!(!CteScanNext(&mut node, &mut estate).unwrap());
    assert!(EOF_CTE.with(|c| c.get()));
    log_eq(&[
        "select_read_pointer",
        "ateof",
        "ExecProcNode",
        "set_leader_eof_cte",
    ]);
}

#[test]
fn next_clears_slot_when_forward_eof_and_cte_already_done() {
    setup();
    EOF_CTE.with(|c| c.set(true));
    ATEOF.with(|q| q.borrow_mut().push_back(false));
    GETTUPLE.with(|q| q.borrow_mut().push_back(false));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    // Scan slot present so the final ExecClearTuple fires.
    let qcxt = estate.es_query_cxt;
    let slot = estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap();
    node.ss.ss_ScanTupleSlot = Some(slot);
    assert!(!CteScanNext(&mut node, &mut estate).unwrap());
    log_eq(&["select_read_pointer", "ateof", "gettupleslot", "ClearTuple"]);
}

#[test]
fn next_backward_at_eof_does_extra_advance() {
    setup();
    ATEOF.with(|q| q.borrow_mut().push_back(true));
    ADVANCE.with(|q| q.borrow_mut().push_back(true));
    GETTUPLE.with(|q| q.borrow_mut().push_back(true));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    estate.es_direction = ScanDirection::BackwardScanDirection;
    let mut node = node_in(ctx.mcx());
    assert!(CteScanNext(&mut node, &mut estate).unwrap());
    log_eq(&["select_read_pointer", "ateof", "advance", "gettupleslot"]);
}

#[test]
fn next_backward_at_eof_empty_store_returns_false() {
    setup();
    ATEOF.with(|q| q.borrow_mut().push_back(true));
    ADVANCE.with(|q| q.borrow_mut().push_back(false));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    estate.es_direction = ScanDirection::BackwardScanDirection;
    let mut node = node_in(ctx.mcx());
    assert!(!CteScanNext(&mut node, &mut estate).unwrap());
    log_eq(&["select_read_pointer", "ateof", "advance"]);
}

// --- CteScanRecheck -------------------------------------------------------

#[test]
fn recheck_always_true() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    assert!(CteScanRecheck(&mut node, &mut estate).unwrap());
}

// --- ExecCteScan ----------------------------------------------------------

#[test]
fn exec_cte_scan_drives_exec_scan() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    assert!(ExecCteScan(&mut node, &mut estate).unwrap().is_none());
    log_eq(&["ExecScan"]);
}

// --- ExecInitCteScan ------------------------------------------------------

#[test]
fn init_leader_path_creates_shared_store_and_runs_setup() {
    setup();
    IS_LEADER.with(|c| c.set(true));
    let ctx = MemoryContext::new("per-query");
    let node_plan = mcx::alloc_in(
        ctx.mcx(),
        types_nodes::nodes::Node::CteScan(CteScan {
            ctePlanId: 1,
            cteParam: 0,
            ..Default::default()
        }),
    )
    .unwrap();
    let mut estate = estate(ctx.mcx());
    let node = ExecInitCteScan(&node_plan, 0, &mut estate).unwrap();
    assert_eq!(node.eflags, EXEC_FLAG_REWIND);
    assert_eq!(node.readptr, 0);
    log_eq(&[
        "link_cte_plan_state",
        "resolve_cte_leader",
        "begin_heap_leader",
        "assign_expr_context",
        "init_scan_tuple_slot",
        "init_result_type_tl",
        "assign_scan_projection_info",
        "init_qual",
    ]);
}

#[test]
fn init_follower_path_allocs_own_read_pointer() {
    setup();
    IS_LEADER.with(|c| c.set(false));
    let ctx = MemoryContext::new("per-query");
    let node_plan = mcx::alloc_in(
        ctx.mcx(),
        types_nodes::nodes::Node::CteScan(CteScan {
            ctePlanId: 1,
            cteParam: 0,
            ..Default::default()
        }),
    )
    .unwrap();
    let mut estate = estate(ctx.mcx());
    let _node = ExecInitCteScan(&node_plan, 0, &mut estate).unwrap();
    log_eq(&[
        "link_cte_plan_state",
        "resolve_cte_leader",
        "alloc_read_pointer_follower",
        "assign_expr_context",
        "init_scan_tuple_slot",
        "init_result_type_tl",
        "assign_scan_projection_info",
        "init_qual",
    ]);
}

// --- ExecEndCteScan -------------------------------------------------------

#[test]
fn end_frees_tuplestore_only_for_leader() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mut node = node_in(ctx.mcx());
    ExecEndCteScan(&mut node, true).unwrap();
    log_eq(&["end"]);
    assert!(node.cte_table.is_none());
}

#[test]
fn end_is_noop_for_follower() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mut node = node_in(ctx.mcx());
    ExecEndCteScan(&mut node, false).unwrap();
    log_eq(&[]);
}

// --- ExecReScanCteScan ----------------------------------------------------

#[test]
fn rescan_clears_store_when_cte_chgparam_set() {
    setup();
    EOF_CTE.with(|c| c.set(true));
    CHGPARAM.with(|c| c.set(true));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    ExecReScanCteScan(&mut node, &mut estate).unwrap();
    assert!(!EOF_CTE.with(|c| c.get()));
    log_eq(&["ExecScanReScan", "clear", "set_leader_eof_cte"]);
}

#[test]
fn rescan_rewinds_own_pointer_when_cte_chgparam_clear() {
    setup();
    CHGPARAM.with(|c| c.set(false));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    ExecReScanCteScan(&mut node, &mut estate).unwrap();
    log_eq(&["ExecScanReScan", "select_read_pointer", "rescan"]);
}

#[test]
fn rescan_clears_result_slot_first() {
    setup();
    CHGPARAM.with(|c| c.set(false));
    let ctx = MemoryContext::new("per-query");
    let mut estate = estate(ctx.mcx());
    let mut node = node_in(ctx.mcx());
    let qcxt = estate.es_query_cxt;
    let slot = estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap();
    node.ss.ps.ps_ResultTupleSlot = Some(slot);
    ExecReScanCteScan(&mut node, &mut estate).unwrap();
    log_eq(&[
        "ClearTuple",
        "ExecScanReScan",
        "select_read_pointer",
        "rescan",
    ]);
}
