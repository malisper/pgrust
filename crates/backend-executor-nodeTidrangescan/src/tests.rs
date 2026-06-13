//! Tests for the `nodeTidrangescan` node logic.
//!
//! The genuinely-external operations are reached through this unit's seam
//! crate; the seam slots are process-global `OnceLock`s, so the test fixtures
//! install them exactly once and route per-test inputs/outputs through
//! thread-locals guarded by a serial lock.

use super::*;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use mcx::MemoryContext;
use types_nodes::executor::TupleTableSlot;
use types_nodes::primnodes::{OpExpr, Var};

thread_local! {
    /// Queue of (block, offset, isnull) yielded in order by
    /// `exec_eval_expr_switch_context`.
    static EVAL_RESULTS: RefCell<VecDeque<(BlockNumber, OffsetNumber, bool)>> =
        RefCell::new(VecDeque::new());
    /// Queue of `table_scan_getnextslot_tidrange` verdicts.
    static GETNEXT: RefCell<VecDeque<bool>> = RefCell::new(VecDeque::new());
    /// Queue of `node_is_opexpr` overrides — unused now that `node_is_opexpr`
    /// is in-crate, kept only to mirror the C qual-cell shapes via the plan.
    static LOG: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALLED: AtomicBool = AtomicBool::new(false);

fn log(s: &'static str) {
    LOG.with(|l| l.borrow_mut().push(s));
}

fn install() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    use seam::*;

    init_plan_state_links::set(|_st, _node| Ok(()));
    check_for_interrupts::set(|| Ok(()));
    es_epq_active_present::set(|_| Ok(false));
    reset_per_tuple_expr_context::set(|_, _| Ok(()));
    set_econtext_scantuple_to_scan_slot::set(|_, _| Ok(()));
    exec_clear_scan_tuple::set(|node, estate| {
        if let Some(id) = node.ss.ss_ScanTupleSlot {
            estate.slot_mut(id).tts_flags |= TTS_FLAG_EMPTY;
        }
        log("exec_clear_scan_tuple");
        Ok(())
    });
    exec_clear_proj_result_slot::set(|_, _| Ok(()));
    exec_qual::set(|_, _| Ok(true));
    exec_project::set(|_, _| Ok(true));
    exec_scan_rescan::set(|_, _| {
        log("exec_scan_rescan");
        Ok(())
    });
    exec_eval_expr_switch_context::set(|_node, _handle, is_null, _estate| {
        let (b, o, n) = EVAL_RESULTS.with(|q| q.borrow_mut().pop_front().unwrap());
        *is_null = n;
        Ok(ItemPointerData::new(b, o))
    });
    table_beginscan_tidrange::set(|_node, _estate| {
        log("table_beginscan_tidrange");
        Ok(())
    });
    table_rescan_tidrange::set(|_, _| {
        log("table_rescan_tidrange");
        Ok(())
    });
    table_scan_getnextslot_tidrange::set(|_node, _estate| {
        let v = GETNEXT.with(|q| q.borrow_mut().pop_front().unwrap_or(false));
        log("table_scan_getnextslot_tidrange");
        Ok(v)
    });
    table_endscan::set(|_, _| {
        log("table_endscan");
        Ok(())
    });
    scan_scanrelid::set(|_| Ok(1));
    epq_param_is_member_of_ext_param::set(|_| Ok(false));
    epq_relsubs_done::set(|_, _| Ok(false));
    epq_set_relsubs_done::set(|_, _, _| Ok(()));
    epq_relsubs_slot_present::set(|_, _| Ok(false));
    epq_load_relsubs_slot::set(|_, _| Ok(()));
    epq_relsubs_rowmark_present::set(|_, _| Ok(false));
    eval_plan_qual_fetch_row_mark::set(|_, _| Ok(false));
    exec_assign_expr_context::set(|_, _| Ok(()));
    exec_open_scan_relation::set(|_, _, _, _| Ok(()));
    exec_init_scan_tuple_slot::set(|_, _| Ok(()));
    exec_init_result_type_tl::set(|_, _| Ok(()));
    exec_assign_scan_projection_info::set(|_, _| Ok(()));
    exec_init_qual::set(|_, _| Ok(()));
    // Each compiled bound resolves to a default handle.
    exec_init_expr::set(|_tidstate, _node, _qual_index, _side| Ok(ExprStateHandle::default()));
}

fn setup() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install();
    EVAL_RESULTS.with(|q| q.borrow_mut().clear());
    GETNEXT.with(|q| q.borrow_mut().clear());
    LOG.with(|l| l.borrow_mut().clear());
    guard
}

fn push_eval(results: &[(BlockNumber, OffsetNumber, bool)]) {
    EVAL_RESULTS.with(|q| q.borrow_mut().extend(results.iter().copied()));
}

fn push_getnext(vals: &[bool]) {
    GETNEXT.with(|q| q.borrow_mut().extend(vals.iter().copied()));
}

fn log_has(s: &str) -> bool {
    LOG.with(|l| l.borrow().iter().any(|e| *e == s))
}

fn bound(exprtype: TidExprType, inclusive: bool) -> TidOpExpr {
    TidOpExpr {
        exprtype,
        exprstate: ExprStateHandle::default(),
        inclusive,
    }
}

/// Set a charged `trss_tidexprs` bound list from plain bounds.
fn set_bounds<'mcx>(st: &mut TidRangeScanState<'mcx>, estate: &EStateData<'mcx>, bounds: &[TidOpExpr]) {
    let mcx = estate.es_query_cxt;
    let mut v = vec_with_capacity_in(mcx, bounds.len()).unwrap();
    for b in bounds {
        v.push(*b);
    }
    st.trss_tidexprs = v;
}

fn empty_state<'mcx>(estate: &EStateData<'mcx>) -> TidRangeScanState<'mcx> {
    let mcx = estate.es_query_cxt;
    TidRangeScanState {
        ss: ScanStateData::default(),
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        trss_tidexprs: vec_with_capacity_in(mcx, 0).unwrap(),
        trss_mintid: ItemPointerData::default(),
        trss_maxtid: ItemPointerData::default(),
        trss_inScan: false,
    }
}

/// A CTID `>=` OpExpr qual cell: `ctid >= something`. `MakeTidOpExpr` classifies
/// it as a (lower-bound, inclusive) bound.
fn ctid_geq_qual() -> Expr {
    Expr::OpExpr(OpExpr {
        opno: TIDGreaterEqOperator,
        args: alloc::vec![
            Expr::Var(Var {
                varattno: SelfItemPointerAttributeNumber,
                ..Default::default()
            }),
            Expr::Const(types_nodes::primnodes::Const::default()),
        ],
    })
}

fn make_tid_range_scan<'mcx>(estate: &EStateData<'mcx>, nquals: usize) -> TidRangeScan<'mcx> {
    let mcx = estate.es_query_cxt;
    let mut quals = vec_with_capacity_in(mcx, nquals).unwrap();
    for _ in 0..nquals {
        quals.push(ctid_geq_qual());
    }
    TidRangeScan {
        scan: types_nodes::nodeindexscan::Scan {
            plan: types_nodes::nodeindexscan::Plan::default(),
            scanrelid: 1,
        },
        tidrangequals: Some(quals),
    }
}

// --- ItemPointer helper parity (itemptr.c) ---

#[test]
fn item_pointer_compare_orders_by_block_then_offset() {
    let a = ItemPointerData::new(1, 1);
    let b = ItemPointerData::new(1, 2);
    let c = ItemPointerData::new(2, 0);
    assert_eq!(ItemPointerCompare(&a, &a), 0);
    assert_eq!(ItemPointerCompare(&a, &b), -1);
    assert_eq!(ItemPointerCompare(&b, &a), 1);
    assert_eq!(ItemPointerCompare(&a, &c), -1);
    assert_eq!(ItemPointerCompare(&c, &b), 1);
}

#[test]
fn item_pointer_inc_rolls_offset_into_next_block() {
    let mut p = ItemPointerData::new(5, PG_UINT16_MAX);
    ItemPointerInc(&mut p);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&p), 6);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&p), 0);

    let mut max = ItemPointerData::new(InvalidBlockNumber, PG_UINT16_MAX);
    ItemPointerInc(&mut max);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&max), InvalidBlockNumber);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&max), PG_UINT16_MAX);
}

#[test]
fn item_pointer_dec_rolls_offset_into_prev_block() {
    let mut p = ItemPointerData::new(5, 0);
    ItemPointerDec(&mut p);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&p), 4);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&p), PG_UINT16_MAX);

    let mut min = ItemPointerData::new(0, 0);
    ItemPointerDec(&mut min);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&min), 0);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&min), 0);
}

// --- primnode reads ---

#[test]
fn ctid_classification_reads() {
    let ctx = MemoryContext::new("per-query");
    let estate = EStateData::new_in(ctx.mcx());
    let node = make_tid_range_scan(&estate, 1);
    assert!(node_is_opexpr(&node, 0));
    assert!(is_ctid_var(&node, 0, OperandSide::Left));
    assert!(!is_ctid_var(&node, 0, OperandSide::Right));
    assert_eq!(opexpr_opno(&node, 0), TIDGreaterEqOperator);
    // Out-of-range cell mirrors the C "list exhausted" — not an OpExpr.
    assert!(!node_is_opexpr(&node, 1));
}

// --- TidRangeEval bound-narrowing ---

#[test]
fn tid_range_eval_narrows_inclusive_bounds() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(3, 4, false), (9, 2, false)]);
    let mut st = empty_state(&estate);
    set_bounds(
        &mut st,
        &estate,
        &[
            bound(TidExprType::LowerBound, true),
            bound(TidExprType::UpperBound, true),
        ],
    );
    let ok = TidRangeEval(&mut st, &mut estate).unwrap();
    assert!(ok);
    assert_eq!(ItemPointerCompare(&st.trss_mintid, &ItemPointerData::new(3, 4)), 0);
    assert_eq!(ItemPointerCompare(&st.trss_maxtid, &ItemPointerData::new(9, 2)), 0);
}

#[test]
fn tid_range_eval_normalizes_exclusive_bounds() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(3, 4, false), (9, 2, false)]);
    let mut st = empty_state(&estate);
    set_bounds(
        &mut st,
        &estate,
        &[
            bound(TidExprType::LowerBound, false),
            bound(TidExprType::UpperBound, false),
        ],
    );
    let ok = TidRangeEval(&mut st, &mut estate).unwrap();
    assert!(ok);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&st.trss_mintid), 3);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&st.trss_mintid), 5);
    assert_eq!(ItemPointerGetBlockNumberNoCheck(&st.trss_maxtid), 9);
    assert_eq!(ItemPointerGetOffsetNumberNoCheck(&st.trss_maxtid), 1);
}

#[test]
fn tid_range_eval_returns_false_on_null_bound() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(0, 0, true)]);
    let mut st = empty_state(&estate);
    set_bounds(&mut st, &estate, &[bound(TidExprType::LowerBound, true)]);
    let ok = TidRangeEval(&mut st, &mut estate).unwrap();
    assert!(!ok);
}

// --- TidRangeNext ---

#[test]
fn tid_range_next_begins_scan_then_fetches() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(5, 1, false)]);
    push_getnext(&[true]);
    let mut st = empty_state(&estate);
    let slot = estate.make_slot(TupleTableSlot::default()).unwrap();
    st.ss.ss_ScanTupleSlot = Some(slot);
    set_bounds(&mut st, &estate, &[bound(TidExprType::LowerBound, true)]);

    let have = TidRangeNext(&mut st, &mut estate).unwrap();
    assert!(have);
    assert!(st.trss_inScan);
    assert!(log_has("table_beginscan_tidrange"));
    assert!(log_has("table_scan_getnextslot_tidrange"));
}

#[test]
fn tid_range_next_exhausted_clears_slot_and_resets_inscan() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(5, 1, false)]);
    push_getnext(&[false]);
    let mut st = empty_state(&estate);
    let slot = estate.make_slot(TupleTableSlot::default()).unwrap();
    st.ss.ss_ScanTupleSlot = Some(slot);
    set_bounds(&mut st, &estate, &[bound(TidExprType::LowerBound, true)]);

    let have = TidRangeNext(&mut st, &mut estate).unwrap();
    assert!(!have);
    assert!(!st.trss_inScan);
    assert!(log_has("exec_clear_scan_tuple"));
}

#[test]
fn tid_range_next_returns_false_when_range_empty() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(0, 0, true)]);
    let mut st = empty_state(&estate);
    let slot = estate.make_slot(TupleTableSlot::default()).unwrap();
    st.ss.ss_ScanTupleSlot = Some(slot);
    set_bounds(&mut st, &estate, &[bound(TidExprType::LowerBound, true)]);

    let have = TidRangeNext(&mut st, &mut estate).unwrap();
    assert!(!have);
    assert!(!st.trss_inScan);
    assert!(!log_has("table_beginscan_tidrange"));
}

// --- TidRangeRecheck ---

#[test]
fn tid_range_recheck_in_range_true() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(1, 1, false), (9, 9, false)]);
    let mut st = empty_state(&estate);
    let mut slot = TupleTableSlot::default();
    slot.tts_tid = ItemPointerData::new(5, 5);
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(slot).unwrap());
    set_bounds(
        &mut st,
        &estate,
        &[
            bound(TidExprType::LowerBound, true),
            bound(TidExprType::UpperBound, true),
        ],
    );
    assert!(TidRangeRecheck(&mut st, &mut estate).unwrap());
}

#[test]
fn tid_range_recheck_out_of_range_false() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(1, 1, false), (9, 9, false)]);
    let mut st = empty_state(&estate);
    let mut slot = TupleTableSlot::default();
    slot.tts_tid = ItemPointerData::new(20, 1);
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(slot).unwrap());
    set_bounds(
        &mut st,
        &estate,
        &[
            bound(TidExprType::LowerBound, true),
            bound(TidExprType::UpperBound, true),
        ],
    );
    assert!(!TidRangeRecheck(&mut st, &mut estate).unwrap());
}

// --- ExecTidRangeScan ---

#[test]
fn exec_tid_range_scan_no_qual_no_proj_returns_tuple() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    push_eval(&[(5, 1, false)]);
    push_getnext(&[true]);
    let mut st = empty_state(&estate);
    let slot = estate.make_slot(TupleTableSlot::default()).unwrap();
    st.ss.ss_ScanTupleSlot = Some(slot);
    set_bounds(&mut st, &estate, &[bound(TidExprType::LowerBound, true)]);
    let have = ExecTidRangeScan(&mut st, &mut estate).unwrap();
    assert!(have);
}

// --- End / ReScan / Init ---

#[test]
fn end_scan_skips_endscan_when_no_scan_desc() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&estate);
    ExecEndTidRangeScan(&mut st, &mut estate).unwrap();
    assert!(!log_has("table_endscan"));
}

#[test]
fn rescan_resets_inscan_and_calls_exec_scan_rescan() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&estate);
    st.trss_inScan = true;
    ExecReScanTidRangeScan(&mut st, &mut estate).unwrap();
    assert!(!st.trss_inScan);
    assert!(log_has("exec_scan_rescan"));
}

#[test]
fn init_builds_bounds_from_quals() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let node = make_tid_range_scan(&estate, 2);
    let st = ExecInitTidRangeScan(&node, &mut estate, 0).unwrap();
    assert_eq!(st.trss_tidexprs.len(), 2);
    for b in st.trss_tidexprs.iter() {
        // CTID `>=` -> lower-bound, inclusive.
        assert_eq!(b.exprtype, TidExprType::LowerBound);
        assert!(b.inclusive);
    }
}

#[test]
fn init_errors_on_non_opexpr_qual() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mcx = estate.es_query_cxt;
    let mut quals = vec_with_capacity_in(mcx, 1).unwrap();
    // A bare Var cell is not an OpExpr -> "could not identify CTID expression".
    quals.push(Expr::Var(Var::default()));
    let node = TidRangeScan {
        scan: types_nodes::nodeindexscan::Scan {
            plan: types_nodes::nodeindexscan::Plan::default(),
            scanrelid: 1,
        },
        tidrangequals: Some(quals),
    };
    assert!(ExecInitTidRangeScan(&node, &mut estate, 0).is_err());
}
