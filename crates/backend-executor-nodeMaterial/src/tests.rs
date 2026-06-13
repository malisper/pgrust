//! Logic tests for the material node, driving the real `ExecMaterial` state
//! machine against mock installs of the unported owners' seams (tuplestore,
//! execProcnode, execTuples, execUtils, execAmi, work_mem, interrupts). The
//! mocks marshal into a `MockStore` carried in the opaque `Tuplestorestate`;
//! per-test state is `thread_local!` (never a shared static).

use std::cell::Cell;
use std::mem::size_of;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgBox, PgVec};
use types_nodes::execnodes::{BackwardScanDirection, PlanStateData, ScanStateData};
use types_nodes::executor::TTS_FLAG_EMPTY;
use types_nodes::funcapi::Tuplestorestate;
use types_nodes::nodes::Node;
use types_nodes::Bitmapset;

use super::*;
use types_nodes::TupleTableSlot;

/// The fake tuplestore engine behind the opaque carrier: a row count plus the
/// two read-pointer positions nodeMaterial uses (0 = active, 1 = mark).
#[derive(Default)]
struct MockStore {
    eflags: i32,
    extra_ptrs: i32,
    ntuples: usize,
    positions: [usize; 2],
}

fn store_mut<'a>(state: &'a mut Tuplestorestate<'_>) -> &'a mut MockStore {
    state
        .payload_mut()
        .expect("live tuplestore")
        .downcast_mut::<MockStore>()
        .expect("MockStore payload")
}

fn store_ref<'a>(state: &'a Tuplestorestate<'_>) -> &'a MockStore {
    state
        .payload()
        .expect("live tuplestore")
        .downcast_ref::<MockStore>()
        .expect("MockStore payload")
}

thread_local! {
    /// Rows the mock subplan still has to produce.
    static SUPPLY: Cell<usize> = const { Cell::new(0) };
    /// begin_heap minus end: live mock stores.
    static LIVE_STORES: Cell<isize> = const { Cell::new(0) };
    /// exec_re_scan (child rescan) invocations.
    static CHILD_RESCANS: Cell<usize> = const { Cell::new(0) };
}

fn mock_begin_heap<'mcx>(
    mcx: Mcx<'mcx>,
    _random_access: bool,
    _inter_xact: bool,
    _max_kbytes: i32,
) -> PgResult<PgBox<'mcx, Tuplestorestate<'mcx>>> {
    LIVE_STORES.with(|c| c.set(c.get() + 1));
    alloc_in(mcx, Tuplestorestate::begin(mcx, MockStore::default())?)
}

fn mock_set_eflags(state: &mut Tuplestorestate<'_>, eflags: i32) -> PgResult<()> {
    store_mut(state).eflags = eflags;
    Ok(())
}

fn mock_alloc_read_pointer(state: &mut Tuplestorestate<'_>, _eflags: i32) -> PgResult<i32> {
    let s = store_mut(state);
    s.extra_ptrs += 1;
    Ok(s.extra_ptrs)
}

fn mock_ateof(state: &Tuplestorestate<'_>) -> bool {
    let s = store_ref(state);
    s.positions[0] >= s.ntuples
}

fn mock_advance(state: &mut Tuplestorestate<'_>, forward: bool) -> PgResult<bool> {
    let s = store_mut(state);
    if forward {
        if s.positions[0] < s.ntuples {
            s.positions[0] += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    } else if s.positions[0] > 0 {
        s.positions[0] -= 1;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn mock_gettupleslot(
    state: &mut Tuplestorestate<'_>,
    forward: bool,
    _copy: bool,
    slot: &mut TupleTableSlot,
) -> PgResult<bool> {
    let s = store_mut(state);
    let fetched = if forward {
        if s.positions[0] < s.ntuples {
            s.positions[0] += 1;
            true
        } else {
            false
        }
    } else if s.positions[0] > 0 {
        s.positions[0] -= 1;
        true
    } else {
        false
    };
    if fetched {
        slot.tts_flags &= !TTS_FLAG_EMPTY;
    }
    Ok(fetched)
}

fn mock_puttupleslot(state: &mut Tuplestorestate<'_>, _slot: &TupleTableSlot) -> PgResult<()> {
    let s = store_mut(state);
    s.ntuples += 1;
    // The store is at EOF when material appends, so the active read pointer
    // moves forward over the added tuple — same as the real tuplestore.
    s.positions[0] = s.ntuples;
    Ok(())
}

fn mock_copy_read_pointer(state: &mut Tuplestorestate<'_>, src: i32, dst: i32) -> PgResult<()> {
    let s = store_mut(state);
    s.positions[dst as usize] = s.positions[src as usize];
    Ok(())
}

fn mock_trim(_state: &mut Tuplestorestate<'_>) {}

fn mock_rescan(state: &mut Tuplestorestate<'_>) -> PgResult<()> {
    store_mut(state).positions[0] = 0;
    Ok(())
}

fn mock_end(state: PgBox<'_, Tuplestorestate<'_>>) {
    LIVE_STORES.with(|c| c.set(c.get() - 1));
    drop(state);
}

/// The leaf child's `ExecProcNode` callback: produce a (non-empty) row while
/// the thread-local supply lasts, then the C `NULL` return.
fn supply_rows<'mcx>(
    _pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let remaining = SUPPLY.with(|c| c.get());
    if remaining == 0 {
        return Ok(None);
    }
    SUPPLY.with(|c| c.set(remaining - 1));
    let id = estate.make_slot(TupleTableSlot {
        tts_flags: 0,
        ..Default::default()
    })?;
    Ok(Some(id))
}

fn mock_exec_init_node<'mcx>(
    mcx: Mcx<'mcx>,
    node: Option<&Node<'_>>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<Option<PgBox<'mcx, PlanStateNode<'mcx>>>> {
    match node {
        None => Ok(None),
        Some(Node::Material(m)) => {
            let state = ExecInitMaterial(m, estate, eflags)?;
            Ok(Some(alloc_in(mcx, PlanStateNode::Material(state))?))
        }
        Some(_) => unreachable!("only Material nodes exist"),
    }
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

fn mock_init_result_slot<'mcx>(
    planstate: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    let id = estate.make_slot(TupleTableSlot::default())?;
    planstate.ps_ResultTupleSlot = Some(id);
    Ok(())
}

fn mock_clear_tuple(slot: &mut TupleTableSlot) -> PgResult<()> {
    slot.tts_flags |= TTS_FLAG_EMPTY;
    Ok(())
}

fn mock_copy_slot<'mcx>(
    _mcx: Mcx<'mcx>,
    dst: &mut TupleTableSlot,
    src: &TupleTableSlot,
) -> PgResult<()> {
    dst.tts_flags = src.tts_flags;
    Ok(())
}

fn mock_create_scan_slot<'mcx>(
    estate: &mut EStateData<'mcx>,
    scanstate: &mut ScanStateData<'mcx>,
    _tts_ops: TupleSlotKind,
) -> PgResult<()> {
    let id = estate.make_slot(TupleTableSlot::default())?;
    scanstate.ss_ScanTupleSlot = Some(id);
    Ok(())
}

fn install_mocks() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        tcop_postgres::check_for_interrupts::set(|| Ok(()));
        globals::work_mem::set(|| 1024);
        tuplestore::tuplestore_begin_heap::set(mock_begin_heap);
        tuplestore::tuplestore_set_eflags::set(mock_set_eflags);
        tuplestore::tuplestore_alloc_read_pointer::set(mock_alloc_read_pointer);
        tuplestore::tuplestore_ateof::set(mock_ateof);
        tuplestore::tuplestore_advance::set(mock_advance);
        tuplestore::tuplestore_gettupleslot::set(mock_gettupleslot);
        tuplestore::tuplestore_puttupleslot::set(mock_puttupleslot);
        tuplestore::tuplestore_copy_read_pointer::set(mock_copy_read_pointer);
        tuplestore::tuplestore_trim::set(mock_trim);
        tuplestore::tuplestore_rescan::set(mock_rescan);
        tuplestore::tuplestore_end::set(mock_end);
        execProcnode::exec_init_node::set(mock_exec_init_node);
        execProcnode::exec_proc_node::set(mock_exec_proc_node);
        execProcnode::exec_end_node::set(mock_exec_end_node);
        execAmi::exec_re_scan::set(mock_exec_re_scan);
        execTuples::exec_init_result_tuple_slot_tl::set(mock_init_result_slot);
        execTuples::exec_clear_tuple::set(mock_clear_tuple);
        execTuples::exec_copy_slot::set(mock_copy_slot);
        execUtils::exec_create_scan_slot_from_outer_plan::set(mock_create_scan_slot);
    });
}

/// Init a material state in `estate` and splice in a leaf child that produces
/// rows from the thread-local supply.
fn init_with_leaf_child<'mcx>(
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgBox<'mcx, MaterialState<'mcx>> {
    let mcx = estate.es_query_cxt;
    let mat = Material::default();
    let mut matstate = ExecInitMaterial(&mat, estate, eflags).unwrap();
    let mut leaf = MaterialState::default();
    leaf.ss.ps.ExecProcNode = Some(supply_rows);
    matstate.ss.ps.lefttree = Some(
        alloc_in(mcx, PlanStateNode::Material(alloc_in(mcx, leaf).unwrap())).unwrap(),
    );
    matstate
}

#[test]
fn init_material_accounting_is_exact() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mat = Material::default();

    let matstate = ExecInitMaterial(&mat, &mut estate, EXEC_FLAG_REWIND).unwrap();

    assert_eq!(matstate.eflags, EXEC_FLAG_REWIND);
    assert!(matstate.ss.ps.ps_ResultTupleSlot.is_some());
    assert!(matstate.ss.ss_ScanTupleSlot.is_some());
    assert!(matstate.ss.ps.lefttree.is_none());
    assert!(matstate.tuplestorestate.is_none());
    // Every charged byte is identifiable: the state box, the owned plan-node
    // copy, and the slot pool's backing storage.
    assert_eq!(
        ctx.used(),
        size_of::<MaterialState<'static>>()
            + size_of::<Node<'static>>()
            + estate.es_tupleTable.capacity() * size_of::<TupleTableSlot>()
    );
}

#[test]
fn backward_eflag_adds_rewind_and_shields_child() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mat = Material::default();
    let matstate = ExecInitMaterial(&mat, &mut estate, EXEC_FLAG_BACKWARD).unwrap();
    assert_eq!(matstate.eflags, EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND);
}

#[test]
fn eflags_zero_passes_rows_through_without_tuplestore() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut node = init_with_leaf_child(&mut estate, 0);
    SUPPLY.with(|c| c.set(1));

    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    let result = node.ss.ps.ps_ResultTupleSlot.unwrap();
    assert!(!estate.slot(result).is_empty());
    assert!(node.tuplestorestate.is_none(), "eflags==0 stores nothing");

    assert!(!ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(node.eof_underlying);

    // Once the subplan is known exhausted, the next call takes the
    // "nothing left" path: return ExecClearTuple(slot) in C.
    assert!(!ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(estate.slot(result).is_empty(), "C: ExecClearTuple(slot)");
}

#[test]
fn rewind_materializes_then_replays_without_rereading_subplan() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut node = init_with_leaf_child(&mut estate, EXEC_FLAG_REWIND);
    SUPPLY.with(|c| c.set(2));

    let used_before_store = ctx.used();
    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(
        ctx.used() >= used_before_store + size_of::<Tuplestorestate>(),
        "the tuplestore state is charged to the query context"
    );
    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(!ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(node.eof_underlying);
    let ts = node.tuplestorestate.as_deref().unwrap();
    assert_eq!(store_ref(ts).ntuples, 2, "both rows were materialized");

    // Rescan with no chgParam and REWIND supported: rewind the stored output;
    // the subplan is NOT re-read (supply stays 0) and not rescanned.
    let child_rescans = CHILD_RESCANS.with(|c| c.get());
    ExecReScanMaterial(&mut node, &mut estate).unwrap();
    assert_eq!(CHILD_RESCANS.with(|c| c.get()), child_rescans);
    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(!ExecMaterial(&mut node, &mut estate).unwrap());
    assert_eq!(SUPPLY.with(|c| c.get()), 0);
}

#[test]
fn backward_scan_reads_back_from_store() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut node = init_with_leaf_child(&mut estate, EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD);
    SUPPLY.with(|c| c.set(2));

    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(!ExecMaterial(&mut node, &mut estate).unwrap());

    estate.es_direction = BackwardScanDirection;
    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    let ts = node.tuplestorestate.as_deref().unwrap();
    assert_eq!(store_ref(ts).positions[0], 1, "stepped back over a row");
}

#[test]
fn mark_and_restore_copy_read_pointers() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut node = init_with_leaf_child(&mut estate, EXEC_FLAG_REWIND | EXEC_FLAG_MARK);
    SUPPLY.with(|c| c.set(2));

    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    {
        let ts = node.tuplestorestate.as_deref().unwrap();
        assert_eq!(store_ref(ts).extra_ptrs, 1, "MARK allocated read pointer 1");
    }
    ExecMaterialMarkPos(&mut node).unwrap();
    assert_eq!(store_ref(node.tuplestorestate.as_deref().unwrap()).positions[1], 1);

    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    ExecMaterialRestrPos(&mut node).unwrap();
    assert_eq!(
        store_ref(node.tuplestorestate.as_deref().unwrap()).positions[0],
        1,
        "restore rewound the active pointer to the mark"
    );
}

#[test]
fn rescan_with_changed_params_drops_store_and_rescans_child() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let mut estate = EStateData::new_in(ctx.mcx());
    let mut node = init_with_leaf_child(&mut estate, EXEC_FLAG_REWIND);
    SUPPLY.with(|c| c.set(1));

    assert!(ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(!ExecMaterial(&mut node, &mut estate).unwrap());
    assert!(node.tuplestorestate.is_some());

    // chgParam != NULL: forget the stored results; the child is re-scanned by
    // its next ExecProcNode, not here.
    let mcx = estate.es_query_cxt;
    node.ss
        .ps
        .lefttree
        .as_deref_mut()
        .unwrap()
        .ps_head_mut()
        .chgParam = Some(
        alloc_in(
            mcx,
            Bitmapset {
                words: mcx::slice_in(mcx, &[1u64]).unwrap(),
            },
        )
        .unwrap(),
    );
    let child_rescans = CHILD_RESCANS.with(|c| c.get());
    ExecReScanMaterial(&mut node, &mut estate).unwrap();
    assert!(node.tuplestorestate.is_none(), "store was ended");
    assert!(!node.eof_underlying);
    assert_eq!(
        CHILD_RESCANS.with(|c| c.get()),
        child_rescans,
        "chgParam != NULL: ExecReScan is left to the child's next ExecProcNode"
    );
}

#[test]
fn all_bytes_return_on_drop() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    let live_before = LIVE_STORES.with(|c| c.get());
    {
        let mut estate = EStateData::new_in(ctx.mcx());
        let mut node = init_with_leaf_child(&mut estate, EXEC_FLAG_REWIND);
        SUPPLY.with(|c| c.set(2));
        assert!(ExecMaterial(&mut node, &mut estate).unwrap());
        assert!(ExecMaterial(&mut node, &mut estate).unwrap());
        assert!(ctx.used() > 0);

        ExecEndMaterial(&mut node, &mut estate).unwrap();
        assert!(node.tuplestorestate.is_none(), "tuplestore released");
        assert_eq!(
            LIVE_STORES.with(|c| c.get()),
            live_before,
            "tuplestore_end consumed the store"
        );
    }
    assert_eq!(ctx.used(), 0, "dropping the state tree returns every byte");
}

#[test]
fn estate_slot_pool_is_context_allocated() {
    install_mocks();
    let ctx = MemoryContext::new("per-query");
    {
        let mut estate = EStateData::new_in(ctx.mcx());
        assert_eq!(ctx.used(), 0, "an empty executor state allocates nothing");
        let id = estate.make_slot(TupleTableSlot::default()).unwrap();
        assert!(estate.slot(id).is_empty());
        assert_eq!(
            ctx.used(),
            estate.es_tupleTable.capacity() * size_of::<TupleTableSlot>()
        );
        // PgVec is the proof the pool cannot outlive the context.
        let _: &PgVec<'_, TupleTableSlot> = &estate.es_tupleTable;
    }
    assert_eq!(ctx.used(), 0);
}
