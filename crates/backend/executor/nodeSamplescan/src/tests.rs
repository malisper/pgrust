//! Tests for the `nodeSamplescan` node logic.
//!
//! The genuinely-external operations are reached through this unit's seam
//! crate; the seam slots are process-global, so the fixtures install them
//! exactly once and route per-test inputs/outputs through thread-locals guarded
//! by a serial lock.

use super::*;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use mcx::{vec_with_capacity_in, MemoryContext};
use nodes::executor::TupleTableSlot;
use samplescan::TsmRoutine;

thread_local! {
    /// Queue of `table_scan_sample_next_block` verdicts.
    static BLOCKS: RefCell<VecDeque<bool>> = RefCell::new(VecDeque::new());
    /// Queue of `table_scan_sample_next_tuple` verdicts.
    static TUPLES: RefCell<VecDeque<bool>> = RefCell::new(VecDeque::new());
    static LOG: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
    /// `is_null` to report from the eval seams.
    static EVAL_ISNULL: RefCell<bool> = const { RefCell::new(false) };
    /// Seed handed back by `pg_prng_uint32_global`.
    static PRNG_SEED: RefCell<u32> = const { RefCell::new(0) };
    static HAS_NEXT_BLOCK: RefCell<bool> = const { RefCell::new(false) };
    static HAS_END: RefCell<bool> = const { RefCell::new(false) };
}

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALLED: AtomicBool = AtomicBool::new(false);

fn log(s: &'static str) {
    LOG.with(|l| l.borrow_mut().push(s));
}

/// A leaked, thread-lived `MemoryContext` for minting boxes whose `'mcx`
/// outlives any per-test context.
fn static_mcx() -> mcx::Mcx<'static> {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("test-samplescan")));
    }
    CTX.with(|c| c.mcx())
}

fn zeroed_tsm_routine() -> TsmRoutine {
    TsmRoutine {
        type_: nodes::nodes::NodeTag(0),
        parameterTypes: Vec::new(),
        repeatable_across_queries: false,
        repeatable_across_scans: false,
        SampleScanGetSampleSize: None,
        InitSampleScan: None,
        BeginSampleScan: None,
        NextSampleBlock: None,
        NextSampleTuple: None,
        EndSampleScan: None,
    }
}

fn install() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    use seam::*;
    use tsm::*;

    check_for_interrupts::set(|| Ok(()));
    es_epq_active_present::set(|_| Ok(false));
    reset_per_tuple_expr_context::set(|_| Ok(()));
    set_econtext_scantuple_to_scan_slot::set(|_| Ok(()));
    exec_clear_scan_tuple::set(|node, estate| {
        if let Some(id) = node.ss.ss_ScanTupleSlot {
            estate.slot_mut(id).tts_flags |= TTS_FLAG_EMPTY;
        }
        Ok(())
    });
    exec_clear_proj_result_slot::set(|_, _| Ok(()));
    exec_qual::set(|_, _| Ok(true));
    exec_project::set(|_, _| Ok(true));

    init_plan_state_links::set(|_, _| {
        log("init_plan_state_links");
        Ok(())
    });
    exec_assign_expr_context::set(|_, _| Ok(()));
    exec_open_scan_relation::set(|_, _, _, _| Ok(()));
    exec_init_scan_tuple_slot::set(|_, _| Ok(()));
    exec_init_result_type_tl::set(|_, _| Ok(()));
    exec_assign_scan_projection_info::set(|_, _| Ok(()));
    exec_init_qual::set(|_, _| Ok(()));
    exec_init_expr_list::set(|_, _| Ok(()));
    exec_init_repeatable_expr::set(|_, _| Ok(()));
    exec_scan_rescan::set(|_| {
        log("exec_scan_rescan");
        Ok(())
    });

    exec_eval_arg_in_per_tuple_context::set(|_node, _i, is_null, _estate| {
        log("eval_arg");
        *is_null = EVAL_ISNULL.with(|n| *n.borrow());
        Ok(Datum::null())
    });
    exec_eval_repeatable_in_per_tuple_context::set(|_node, is_null, _estate| {
        log("eval_repeatable");
        *is_null = EVAL_ISNULL.with(|n| *n.borrow());
        Ok(Datum::null())
    });

    pg_prng_uint32_global::set(|| Ok(PRNG_SEED.with(|s| *s.borrow())));
    hashfloat8::set(|_| Ok(0));

    get_tsm_routine_oid::set(|_mcx, _handler| mcx::alloc_in(static_mcx(), zeroed_tsm_routine()));
    tsm_has_init_sample_scan::set(|_| Ok(false));
    tsm_init_sample_scan::set(|_, _| Ok(()));
    tsm_begin_sample_scan::set(|_, _, _| {
        log("tsm_begin_sample_scan");
        Ok(())
    });
    tsm_has_next_sample_block::set(|_| Ok(HAS_NEXT_BLOCK.with(|b| *b.borrow())));
    tsm_has_end_sample_scan::set(|_| Ok(HAS_END.with(|b| *b.borrow())));
    tsm_end_sample_scan::set(|_| {
        log("tsm_end_sample_scan");
        Ok(())
    });

    table_beginscan_sampling::set(|_node, _allow_sync| {
        // The real owner installs `ss_currentScanDesc` here; the test only needs
        // to observe that the begin path was taken (not the descriptor itself,
        // which requires a live relation to construct).
        log("table_beginscan_sampling");
        Ok(())
    });
    table_rescan_set_params::set(|_, _| {
        log("table_rescan_set_params");
        Ok(())
    });
    table_endscan::set(|_| {
        log("table_endscan");
        Ok(())
    });
    table_scan_sample_next_block::set(|_| {
        let v = BLOCKS.with(|q| q.borrow_mut().pop_front().unwrap_or(false));
        log("next_block");
        Ok(v)
    });
    table_scan_sample_next_tuple::set(|_, _| {
        let v = TUPLES.with(|q| q.borrow_mut().pop_front().unwrap_or(false));
        log("next_tuple");
        Ok(v)
    });

    scan_scanrelid::set(|_| Ok(1));
    epq_param_is_member_of_ext_param::set(|_| Ok(false));
    epq_relsubs_done::set(|_, _| Ok(false));
    epq_set_relsubs_done::set(|_, _, _| Ok(()));
    epq_relsubs_slot_present::set(|_, _| Ok(false));
    epq_load_relsubs_slot::set(|_, _| Ok(()));
    epq_relsubs_rowmark_present::set(|_, _| Ok(false));
    eval_plan_qual_fetch_row_mark::set(|_, _| Ok(false));
}

fn setup() -> MutexGuard<'static, ()> {
    let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install();
    BLOCKS.with(|q| q.borrow_mut().clear());
    TUPLES.with(|q| q.borrow_mut().clear());
    LOG.with(|l| l.borrow_mut().clear());
    EVAL_ISNULL.with(|n| *n.borrow_mut() = false);
    PRNG_SEED.with(|s| *s.borrow_mut() = 0);
    HAS_NEXT_BLOCK.with(|b| *b.borrow_mut() = false);
    HAS_END.with(|b| *b.borrow_mut() = false);
    guard
}

fn log_snapshot() -> Vec<&'static str> {
    LOG.with(|l| l.borrow().clone())
}

fn empty_state<'mcx>(estate: &mut EStateData<'mcx>) -> SampleScanState<'mcx> {
    let mcx = estate.es_query_cxt;
    SampleScanState {
        ss: ScanStateData::default(),
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        args: vec_with_capacity_in(mcx, 0).unwrap(),
        repeatable: None,
        tsmroutine: None,
        tsm_state: None,
        use_bulkread: false,
        use_pagemode: false,
        begun: false,
        seed: 0,
        donetuples: 0,
        haveblock: false,
        done: false,
    }
}

fn make_sample_scan<'mcx>(repeatable: bool) -> SampleScan<'mcx> {
    let mut tsc = TableSampleClause::default();
    tsc.tsmhandler = 0;
    if repeatable {
        tsc.repeatable = Some(Box::new(nodes::primnodes::Expr::Const(
            nodes::primnodes::Const::default(),
        )));
    }
    SampleScan {
        scan: nodes::nodeindexscan::Scan::default(),
        tablesample: Some(Box::new(tsc)),
    }
}

#[test]
fn sample_recheck_always_true() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    assert!(SampleRecheck(&mut st, &mut estate).unwrap());
}

#[test]
fn getnext_returns_false_when_done() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(estate.es_query_cxt)).unwrap());
    st.done = true;
    let out = tablesample_getnext(&mut st, &mut estate).unwrap();
    assert!(!out);
    assert!(!log_snapshot().contains(&"next_block"));
}

#[test]
fn getnext_iterates_block_then_tuple_then_returns_slot() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(estate.es_query_cxt)).unwrap());
    // block yes; tuple miss (page exhausted); block yes; tuple hit.
    BLOCKS.with(|q| q.borrow_mut().extend([true, true]));
    TUPLES.with(|q| q.borrow_mut().extend([false, true]));
    let out = tablesample_getnext(&mut st, &mut estate).unwrap();
    assert!(out);
    assert_eq!(st.donetuples, 1);
    assert!(st.haveblock);
    assert_eq!(
        log_snapshot(),
        vec!["next_block", "next_tuple", "next_block", "next_tuple"]
    );
}

#[test]
fn getnext_sets_done_when_relation_exhausted() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(estate.es_query_cxt)).unwrap());
    // no blocks scripted -> first next_block returns false -> done.
    let out = tablesample_getnext(&mut st, &mut estate).unwrap();
    assert!(!out);
    assert!(st.done);
    assert!(!st.haveblock);
    assert_eq!(st.donetuples, 0);
}

#[test]
fn init_rejects_null_repeatable_with_2202g() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.repeatable = Some(mcx::alloc_in(static_mcx(), Default::default()).unwrap());
    EVAL_ISNULL.with(|n| *n.borrow_mut() = true);
    let err = tablesample_init(&mut st, &mut estate).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_REPEAT);
    assert_eq!(
        err.message(),
        "TABLESAMPLE REPEATABLE parameter cannot be null"
    );
}

#[test]
fn init_rejects_null_arg_with_2202h() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    // One-element args list -> the single arg evaluates to NULL.
    st.args
        .push(mcx::alloc_in(static_mcx(), Default::default()).unwrap());
    EVAL_ISNULL.with(|n| *n.borrow_mut() = true);
    let err = tablesample_init(&mut st, &mut estate).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
    assert_eq!(err.message(), "TABLESAMPLE parameter cannot be null");
}

#[test]
fn init_begins_scan_and_sets_begun() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.tsmroutine = Some(mcx::alloc_in(static_mcx(), zeroed_tsm_routine()).unwrap());
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(estate.es_query_cxt)).unwrap());
    // no args, no REPEATABLE -> use the init seed.
    st.seed = 0x1234;
    tablesample_init(&mut st, &mut estate).unwrap();
    assert!(st.begun);
    assert!(st.use_bulkread);
    assert!(st.use_pagemode);
    let log = log_snapshot();
    assert!(log.contains(&"tsm_begin_sample_scan"));
    assert!(log.contains(&"table_beginscan_sampling"));
}

#[test]
fn rescan_resets_flags_and_calls_exec_scan_rescan() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.begun = true;
    st.done = true;
    st.haveblock = true;
    st.donetuples = 7;
    ExecReScanSampleScan(&mut st).unwrap();
    assert!(!st.begun);
    assert!(!st.done);
    assert!(!st.haveblock);
    assert_eq!(st.donetuples, 0);
    assert!(log_snapshot().contains(&"exec_scan_rescan"));
}

#[test]
fn end_scan_skips_endscan_when_no_scan_desc() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.tsmroutine = Some(mcx::alloc_in(static_mcx(), zeroed_tsm_routine()).unwrap());
    ExecEndSampleScan(&mut st).unwrap();
    // No scan descriptor present -> table_endscan is skipped.
    assert!(!log_snapshot().contains(&"table_endscan"));
}

#[test]
fn end_scan_calls_tsm_end_when_present() {
    let _g = setup();
    HAS_END.with(|b| *b.borrow_mut() = true);
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.tsmroutine = Some(mcx::alloc_in(static_mcx(), zeroed_tsm_routine()).unwrap());
    ExecEndSampleScan(&mut st).unwrap();
    // tsm->EndSampleScan present -> the EndSampleScan callback is invoked.
    assert!(log_snapshot().contains(&"tsm_end_sample_scan"));
}

#[test]
fn exec_sample_scan_returns_tuple() {
    let _g = setup();
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state(&mut estate);
    st.begun = true; // skip init; go straight to getnext
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(estate.es_query_cxt)).unwrap());
    BLOCKS.with(|q| q.borrow_mut().push_back(true));
    TUPLES.with(|q| q.borrow_mut().push_back(true));
    // No qual, no projInfo -> raw scan-tuple path.
    let out = ExecSampleScan(&mut st, &mut estate).unwrap();
    assert!(out);
    assert_eq!(st.donetuples, 1);
}

#[test]
fn init_wires_state_no_repeatable_random_seed() {
    let _g = setup();
    PRNG_SEED.with(|s| *s.borrow_mut() = 0xDEAD_BEEF);
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let plan = make_sample_scan(false);
    let out = ExecInitSampleScan(&plan, &mut estate, 0).unwrap();
    assert!(!out.begun);
    assert!(out.tsm_state.is_none());
    assert!(out.tsmroutine.is_some());
    assert_eq!(out.seed, 0xDEAD_BEEF);
    assert!(log_snapshot().contains(&"init_plan_state_links"));
}

#[test]
fn init_with_repeatable_skips_random_seed() {
    let _g = setup();
    PRNG_SEED.with(|s| *s.borrow_mut() = 0xDEAD_BEEF);
    let ctx = MemoryContext::new("t");
    let mut estate = EStateData::new_in(ctx.mcx());
    let plan = make_sample_scan(true);
    let out = ExecInitSampleScan(&plan, &mut estate, 0).unwrap();
    // REPEATABLE present -> no random seed picked at init time.
    assert_eq!(out.seed, 0);
}
