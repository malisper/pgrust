//! Tests for the `nodeWorktablescan` node logic.
//!
//! The node now reaches the ancestor `RecursiveUnion`'s shared working-table
//! tuplestore through the `EState.es_recursive_shared[wtParam]` side-table
//! (`RecursiveUnionSharedState`) rather than a private seam family. These unit
//! tests exercise the side-table-driven legs directly — resolution
//! (`resolve_rustate`), the deposit (`publish_wtparam_slot`), the access method
//! (`WorkTableScanNext`), and rescan — mocking only the sort-storage tuplestore
//! seams the node calls. The full `ExecScan` driver path (qual/projection/EPQ)
//! is covered end-to-end by the regression suite (`with.sql`).

use super::*;

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard};

use ::mcx::MemoryContext;
use ::nodes::execnodes::{RecursiveUnionSharedState, ScanStateData};
use ::nodes::executor::{TupleTableSlot, TTS_FLAG_EMPTY};
use ::nodes::nodeworktablescan::WorkTableScan;
use ::nodes::Tuplestorestate;

thread_local! {
    /// Verdict the mocked `tuplestore_gettupleslot` returns.
    static GETTUPLE: RefCell<bool> = const { RefCell::new(false) };
    /// Set true when the mocked `tuplestore_rescan` fires.
    static RESCANNED: RefCell<bool> = const { RefCell::new(false) };
}

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install mocks for the sort-storage tuplestore seams the node calls.
fn install() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    tuplestore::tuplestore_gettupleslot::set(|_state, _fwd, _copy, slot, estate| {
        let loaded = GETTUPLE.with(|g| *g.borrow());
        let s = estate.slot_mut(slot);
        if loaded {
            s.tts_flags &= !TTS_FLAG_EMPTY;
        } else {
            s.tts_flags |= TTS_FLAG_EMPTY;
        }
        Ok(loaded)
    });
    tuplestore::tuplestore_rescan::set(|_state| {
        RESCANNED.with(|r| *r.borrow_mut() = true);
        Ok(())
    });
    // The generic ExecScanReScan driver (exec_scan_rescan_worktable) lives in the
    // execScan crate; mock it to a no-op here (its behavior is covered by the
    // execScan crate's own tests / the regression suite).
    execScan::exec_scan_rescan_worktable::set(|_node, _estate| Ok(()));
}

fn setup() -> MutexGuard<'static, ()> {
    let g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install();
    GETTUPLE.with(|x| *x.borrow_mut() = false);
    RESCANNED.with(|x| *x.borrow_mut() = false);
    g
}

fn empty_state<'mcx>() -> WorkTableScanStateData<'mcx> {
    WorkTableScanStateData {
        ss: ScanStateData::default(),
        rustate: None,
    }
}

/// Claim `es_recursive_shared[wt_param]` with a working-table tuplestore.
fn claim_shared(estate: &mut EStateData<'_>, wt_param: i32) {
    let idx = wt_param as usize;
    while estate.es_recursive_shared.len() <= idx {
        estate.es_recursive_shared.push(None);
    }
    let working = alloc_in(estate.es_query_cxt, Tuplestorestate::default()).unwrap();
    estate.es_recursive_shared[idx] = Some(RecursiveUnionSharedState {
        working_table: Some(working),
        intermediate_table: None,
        recursing: false,
        intermediate_empty: true,
        result_tupdesc: None,
    });
}

// --- WorkTableScanRecheck ---

#[test]
fn recheck_always_true() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    assert!(WorkTableScanRecheck(&mut st, &mut estate).unwrap());
}

// --- WorkTableScanNext (reads the working_table from the side-table) ---

#[test]
fn next_loads_tuple_from_working_table() {
    let _g = setup();
    GETTUPLE.with(|x| *x.borrow_mut() = true);
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let qcxt = estate.es_query_cxt;
    claim_shared(&mut estate, 0);
    let mut st = empty_state();
    st.rustate = Some(0); // resolved to wtParam 0
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());

    let have = WorkTableScanNext(&mut st, &mut estate).unwrap();
    assert!(have);
    assert_eq!(
        estate.slot(st.ss.ss_ScanTupleSlot.unwrap()).tts_flags & TTS_FLAG_EMPTY,
        0
    );
    // The working_table PgBox was put back after the fetch (take/put).
    assert!(estate.es_recursive_shared[0]
        .as_ref()
        .unwrap()
        .working_table
        .is_some());
}

#[test]
fn next_returns_false_when_exhausted() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let qcxt = estate.es_query_cxt;
    claim_shared(&mut estate, 0);
    let mut st = empty_state();
    st.rustate = Some(0);
    st.ss.ss_ScanTupleSlot = Some(estate.make_slot(TupleTableSlot::new_in(qcxt)).unwrap());

    let have = WorkTableScanNext(&mut st, &mut estate).unwrap();
    assert!(!have);
    assert_ne!(
        estate.slot(st.ss.ss_ScanTupleSlot.unwrap()).tts_flags & TTS_FLAG_EMPTY,
        0
    );
}

// --- resolve_rustate ---

#[test]
fn resolve_records_wtparam_index() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    claim_shared(&mut estate, 3);
    let mut st = empty_state();
    // ss.ps.plan must be a WorkTableScan carrying wtParam=3.
    let plan = WorkTableScan { wtParam: 3, ..Default::default() };
    let node = alloc_in(
        estate.es_query_cxt,
        ::nodes::nodes::Node::mk_work_table_scan(estate.es_query_cxt, plan).unwrap(),
    )
    .unwrap();
    // SAFETY: the Node lives in the per-query context for the test duration.
    st.ss.ps.plan = Some(unsafe { &*(node.as_ref() as *const _) });

    resolve_rustate(&mut st, &mut estate).unwrap();
    assert_eq!(st.rustate, Some(3));
}

#[test]
fn resolve_errors_when_shared_state_absent() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    let plan = WorkTableScan { wtParam: 0, ..Default::default() };
    let node = alloc_in(
        estate.es_query_cxt,
        ::nodes::nodes::Node::mk_work_table_scan(estate.es_query_cxt, plan).unwrap(),
    )
    .unwrap();
    st.ss.ps.plan = Some(unsafe { &*(node.as_ref() as *const _) });

    // No RecursiveUnion has published its shared state -> error (the C Assert).
    assert!(resolve_rustate(&mut st, &mut estate).is_err());
}

// --- publish_wtparam_slot (the deposit) ---

#[test]
fn publish_hoists_tuplestores_into_side_table() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let qcxt = estate.es_query_cxt;
    let mut rustate =
        ::nodes::noderecursiveunion::RecursiveUnionStateData::new_in(qcxt);
    rustate.working_table = Some(alloc_in(qcxt, Tuplestorestate::default()).unwrap());
    rustate.intermediate_table = Some(alloc_in(qcxt, Tuplestorestate::default()).unwrap());
    rustate.recursing = false;
    rustate.intermediate_empty = true;

    publish_wtparam_slot(&mut rustate, &mut estate, 2).unwrap();

    // The tuplestores moved out of the node into es_recursive_shared[2].
    assert!(rustate.working_table.is_none());
    assert!(rustate.intermediate_table.is_none());
    let shared = estate.es_recursive_shared[2].as_ref().unwrap();
    assert!(shared.working_table.is_some());
    assert!(shared.intermediate_table.is_some());
    assert!(shared.intermediate_empty);
}

#[test]
fn publish_rejects_double_claim() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let qcxt = estate.es_query_cxt;
    let mut rustate =
        ::nodes::noderecursiveunion::RecursiveUnionStateData::new_in(qcxt);
    rustate.working_table = Some(alloc_in(qcxt, Tuplestorestate::default()).unwrap());
    publish_wtparam_slot(&mut rustate, &mut estate, 0).unwrap();

    // A second publish for the same wtParam is the C `Assert(prmdata->execPlan
    // == NULL)` violation.
    let mut rustate2 =
        ::nodes::noderecursiveunion::RecursiveUnionStateData::new_in(qcxt);
    rustate2.working_table = Some(alloc_in(qcxt, Tuplestorestate::default()).unwrap());
    assert!(publish_wtparam_slot(&mut rustate2, &mut estate, 0).is_err());
}

// --- ExecReScanWorkTableScan ---

#[test]
fn rescan_rescans_tuplestore_when_rustate_set() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let qcxt = estate.es_query_cxt;
    claim_shared(&mut estate, 0);
    let mut st = empty_state();
    st.rustate = Some(0);
    // ps_ResultTupleSlot left None so the ExecClearTuple branch is skipped (that
    // leg is covered by execTuples' own tests).
    let _ = qcxt;

    ExecReScanWorkTableScan(&mut st, &mut estate).unwrap();
    assert!(RESCANNED.with(|r| *r.borrow()));
    // working_table was put back after the rescan (take/put).
    assert!(estate.es_recursive_shared[0]
        .as_ref()
        .unwrap()
        .working_table
        .is_some());
}

#[test]
fn rescan_skips_tuplestore_when_rustate_none() {
    let _g = setup();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut st = empty_state();
    st.ss.ps.ps_ResultTupleSlot = None;
    st.rustate = None;

    ExecReScanWorkTableScan(&mut st, &mut estate).unwrap();
    assert!(!RESCANNED.with(|r| *r.borrow()));
}
