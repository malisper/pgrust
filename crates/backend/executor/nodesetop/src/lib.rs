// nodeSetOp.c (PG18 binary form: outer+inner children, no flag column).
// Children live in the execProcnode wrapper and arrive as fetch closures;
// SETOP_HASHED counts left/right dups per group in TupleHashTable additional
// space, SETOP_SORTED merges two sorted inputs over SortSupport comparators.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::execgrouping::TupleHashTable;
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgVec};
use ::tuplesort::{
    apply_sort_comparator_in, prepare_sort_support_from_ordering_op, SortSupport,
    SortSupportInit,
};
use ::types_error::PgResult;
use ::types_nodes::plannodes::SetOp;
use ::types_pathnodes::{
    SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL,
    SETOP_HASHED, SETOP_SORTED,
};
use ::types_slot::{
    SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND,
};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

#[repr(C)]
#[derive(Clone, Copy)]
struct SetOpPerGroup {
    num_left: i64,
    num_right: i64,
}

pub struct SetOpState<'mcx> {
    pub plan: &'mcx SetOp<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    setop_done: bool,
    num_output: i64,
    strategy: StrategyState<'mcx>,
}

enum StrategyState<'mcx> {
    Hashed(HashedState<'mcx>),
    Sorted(SortedState<'mcx>),
}

struct HashedState<'mcx> {
    hashtable: TupleHashTable<'mcx>,
    // C setopstate->tableContext (nodeSetOp.c:453-456): entry tuple images
    // live here so the chgParam rescan can free them wholesale
    // (nodeSetOp.c:724-730). Destructor rides the query context's reset
    // callback (docs/no-drop.md guard rule).
    table_ctx: core::ptr::NonNull<::mcx::MemoryContext>,
    table_filled: bool,
    hashiter: u64,
}

struct SortedState<'mcx> {
    sort_keys: PgVec<'mcx, SortSupport>,
    left: PerInput<'mcx>,
    right: PerInput<'mcx>,
    need_init: bool,
}

struct PerInput<'mcx> {
    first_slot: SlotData<'mcx>,
    next: Option<ExecSlotId>,
    need_group: bool,
    num_tuples: i64,
}

/// The ExecInitSetOp child-eflags adjustment: hashed children need no REWIND
/// (ExecReScanSetOp re-walks the built table).
pub fn child_eflags(strategy: u32, eflags: i32) -> i32 {
    if strategy == SETOP_HASHED {
        eflags & !EXEC_FLAG_REWIND
    } else {
        eflags
    }
}

/// `ExecInitSetOp` minus child linkage: the caller inits both children with
/// [`child_eflags`] and passes the outer result type.
pub fn exec_init_set_op<'mcx>(
    node: &'mcx SetOp<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<SetOpState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let num_cols = node.numCols as usize;
    // Zero cmp columns are legal: empty-select-list set ops (allowed since
    // 9.4) compare on no keys, so every pair of rows matches.
    debug_assert!(
        node.cmpColIdx.len() == num_cols
            && node.cmpOperators.len() == num_cols
            && node.cmpCollations.len() == num_cols
            && node.cmpNullsFirst.len() == num_cols
    );
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);

    let strategy = match node.strategy {
        SETOP_HASHED => {
            debug_assert!(node.numGroups > 0);
            let (eqfuncoids, hashfunctions) =
                ::execgrouping::exec_tuples_hash_prepare(mcx, node.cmpOperators)?;
            let table_ctx = make_table_context(mcx)?;
            let mut hashtable = ::execgrouping::build_tuple_hash_table(
                mcx,
                outer_desc,
                node.cmpColIdx,
                &eqfuncoids,
                &hashfunctions,
                node.cmpCollations,
                node.numGroups.max(1) as usize,
                core::mem::size_of::<SetOpPerGroup>(),
                false,
            )?;
            // C BuildTupleHashTable's tempcxt = the econtext's per-tuple
            // memory (nodeSetOp.c), reset per drained row: probe-time
            // detoasts of compressed by-ref keys must not accumulate in
            // query-lifetime memory.
            // SAFETY: the ExprContext is arena-boxed in the same estate and
            // outlives the table.
            unsafe {
                hashtable.set_temp_ctx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx())
            };
            StrategyState::Hashed(HashedState {
                hashtable,
                table_ctx,
                table_filled: false,
                hashiter: 0,
            })
        }
        SETOP_SORTED => {
            let mut sort_keys: PgVec<'mcx, SortSupport> = vec_with_capacity_in(mcx, num_cols)?;
            for i in 0..num_cols {
                let init = SortSupportInit {
                    ssup_collation: node.cmpCollations[i],
                    ssup_nulls_first: node.cmpNullsFirst[i],
                    ssup_attno: node.cmpColIdx[i],
                };
                sort_keys.push(prepare_sort_support_from_ordering_op(node.cmpOperators[i], &init)?);
            }
            StrategyState::Sorted(SortedState {
                sort_keys,
                left: new_input(mcx, &result_desc),
                right: new_input(mcx, &result_desc),
                need_init: true,
            })
        }
        other => panic!("ExecInitSetOp (nodeSetOp.c): unrecognized strategy: {other}"),
    };

    Ok(SetOpState {
        plan: node,
        ps_ExprContext,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        setop_done: false,
        num_output: 0,
        strategy,
    })
}

fn new_input<'mcx>(mcx: ::mcx::Mcx<'mcx>, desc: &Rc<TupleDescData<'static>>) -> PerInput<'mcx> {
    let first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
    PerInput { first_slot, next: None, need_group: false, num_tuples: 0 }
}

// set_output_count (nodeSetOp.c): SQL92 emit counts.
fn set_output_count(cmd: u32, num_left: i64, num_right: i64) -> i64 {
    match cmd {
        SETOPCMD_INTERSECT => (num_left > 0 && num_right > 0) as i64,
        SETOPCMD_INTERSECT_ALL => num_left.min(num_right),
        SETOPCMD_EXCEPT => (num_left > 0 && num_right == 0) as i64,
        SETOPCMD_EXCEPT_ALL => (num_left - num_right).max(0),
        other => panic!("unrecognized set op: {other}"),
    }
}

/// `ExecSetOp`.
pub fn exec_set_op<'mcx, FO, FI>(
    node: &mut SetOpState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: FO,
    mut fetch_inner: FI,
) -> PgResult<Option<ExecSlotId>>
where
    FO: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    FI: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    if node.num_output > 0 {
        node.num_output -= 1;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
    if node.setop_done {
        return Ok(None);
    }
    match node.strategy {
        StrategyState::Hashed(HashedState { table_filled, .. }) => {
            if !table_filled {
                setop_fill_hash_table(node, estate, &mut fetch_outer, &mut fetch_inner)?;
            }
            setop_retrieve_hash_table(node, estate)
        }
        StrategyState::Sorted(_) => {
            setop_retrieve_sorted(node, estate, &mut fetch_outer, &mut fetch_inner)
        }
    }
}

enum SortedStep {
    Done,
    Skip,
    Emit(i64, i64),
}

// setop_retrieve_sorted (nodeSetOp.c).
fn setop_retrieve_sorted<'mcx, FO, FI>(
    node: &mut SetOpState<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut FO,
    fetch_inner: &mut FI,
) -> PgResult<Option<ExecSlotId>>
where
    FO: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    FI: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    {
        let StrategyState::Sorted(st) = &mut node.strategy else { unreachable!() };
        if st.need_init {
            st.need_init = false;
            st.left.next = fetch_outer(estate)?;
            if st.left.next.is_none() {
                node.setop_done = true;
                return Ok(None);
            }
            st.right.next = fetch_inner(estate)?;
            st.left.need_group = true;
            st.right.need_group = true;
        }
    }

    while !node.setop_done {
        let step = {
            let StrategyState::Sorted(st) = &mut node.strategy else { unreachable!() };
            let SortedState { sort_keys, left, right, .. } = st;
            if left.need_group {
                setop_load_group(left, sort_keys, estate, fetch_outer)?;
            }
            if left.num_tuples == 0 {
                SortedStep::Done
            } else {
                if right.need_group {
                    setop_load_group(right, sort_keys, estate, fetch_inner)?;
                }
                let cmpresult = if right.num_tuples == 0 {
                    -1
                } else {
                    setop_compare_slots(mcx, &mut left.first_slot, &mut right.first_slot, sort_keys)
                };
                if cmpresult < 0 {
                    left.need_group = true;
                    SortedStep::Emit(left.num_tuples, 0)
                } else if cmpresult == 0 {
                    left.need_group = true;
                    right.need_group = true;
                    SortedStep::Emit(left.num_tuples, right.num_tuples)
                } else {
                    right.need_group = true;
                    SortedStep::Skip
                }
            }
        };
        match step {
            SortedStep::Done => {
                node.setop_done = true;
                break;
            }
            SortedStep::Skip => continue,
            SortedStep::Emit(num_left, num_right) => {
                node.num_output = set_output_count(node.plan.cmd, num_left, num_right);
                if node.num_output > 0 {
                    node.num_output -= 1;
                    let result_id = node.ps_ResultTupleSlot;
                    let StrategyState::Sorted(st) = &mut node.strategy else { unreachable!() };
                    emit_group_tuple(&mut st.left.first_slot, result_id, estate)?;
                    return Ok(Some(result_id));
                }
            }
        }
    }

    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    Ok(None)
}

// setop_load_group (nodeSetOp.c): on entry input.next holds the first tuple
// of the next group (None = exhausted); the invariant holds on exit.
fn setop_load_group<'mcx, F>(
    input: &mut PerInput<'mcx>,
    sort_keys: &[SortSupport],
    estate: &mut EStateData<'mcx>,
    fetch: &mut F,
) -> PgResult<()>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    input.need_group = false;
    let mcx = estate.es_query_cxt;
    let Some(next_id) = input.next else {
        exectuples::exec_clear_tuple(&mut input.first_slot, mcx);
        input.num_tuples = 0;
        return Ok(());
    };
    exectuples::exec_copy_slot(&mut input.first_slot, estate.slot_mut(next_id), mcx, mcx)?;
    input.num_tuples = 1;
    loop {
        input.next = fetch(estate)?;
        let Some(id) = input.next else { break };
        let cmpresult =
            setop_compare_slots(mcx, &mut input.first_slot, estate.slot_mut(id), sort_keys);
        debug_assert!(cmpresult <= 0, "SetOp input is mis-sorted");
        if cmpresult != 0 {
            break;
        }
        input.num_tuples += 1;
    }
    Ok(())
}

// setop_compare_slots (nodeSetOp.c); NULLs compare equal within a key.
// mcx feeds the comparison-shim arm (fmgr comparators, e.g. range_cmp).
fn setop_compare_slots(
    mcx: ::mcx::Mcx<'_>,
    s1: &mut SlotData<'_>,
    s2: &mut SlotData<'_>,
    sort_keys: &[SortSupport],
) -> i32 {
    exectuples::slot_getallattrs(s1);
    exectuples::slot_getallattrs(s2);
    let b1 = s1.base();
    let b2 = s2.base();
    for key in sort_keys {
        let a = (key.ssup_attno - 1) as usize;
        let compare = apply_sort_comparator_in(
            mcx,
            b1.tts_values[a],
            b1.tts_isnull[a],
            b2.tts_values[a],
            b2.tts_isnull[a],
            key,
        );
        if compare != 0 {
            return compare;
        }
    }
    0
}

fn emit_group_tuple<'mcx>(
    first_slot: &mut SlotData<'mcx>,
    result_id: ExecSlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let tup = exectuples::exec_fetch_slot_minimal_tuple(first_slot, mcx, mcx)?;
    let ptr = match tup {
        exectuples::FetchedMinimalTuple::Slot(t, _) => t,
        exectuples::FetchedMinimalTuple::Copied(_) => {
            unreachable!("group first slot was materialized by exec_copy_slot")
        }
    };
    let result_slot = estate.slot_mut(result_id);
    // SAFETY: the image lives in first_slot until the next group load, and
    // the result slot is re-stored before every emit (no stale reads).
    unsafe { exectuples::exec_store_minimal_tuple_ptr(result_slot, mcx, ptr) };
    Ok(())
}

fn pergroup(hashtable: &TupleHashTable<'_>, ix: u32) -> core::ptr::NonNull<SetOpPerGroup> {
    hashtable
        .entry_additional(ix)
        .expect("SetOp hash table carries pergroup space")
        .cast::<SetOpPerGroup>()
}

// setop_fill_hash_table (nodeSetOp.c): count outer dups per group, then count
// inner matches against existing groups only.
// nodeagg make_agg_state_node precedent: a droppy MemoryContext inside the
// no-drop query arena gets its destructor from the arena's reset callback.
fn make_table_context(mcx: ::mcx::Mcx<'_>) -> PgResult<core::ptr::NonNull<::mcx::MemoryContext>> {
    use ::mcx::Allocator;
    let layout = core::alloc::Layout::new::<::mcx::MemoryContext>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: core::ptr::NonNull<::mcx::MemoryContext> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(mcx.context().new_child_bump("SetOp hash table")) };
    // SAFETY: fires exactly once, before the arena bytes are reclaimed.
    mcx.context()
        .register_reset_callback(move || unsafe { core::ptr::drop_in_place(p.as_ptr()) });
    Ok(p)
}

fn setop_fill_hash_table<'mcx, FO, FI>(
    node: &mut SetOpState<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut FO,
    fetch_inner: &mut FI,
) -> PgResult<()>
where
    FO: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    FI: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    let ecxt = node.ps_ExprContext;
    let mut have_tuples = false;
    loop {
        let Some(outer_id) = fetch_outer(estate)? else { break };
        have_tuples = true;
        {
            let StrategyState::Hashed(hs) = &mut node.strategy else { unreachable!() };
            let outer_slot = estate.slot_mut(outer_id);
            let hash = hs.hashtable.hash_slot(outer_slot)?;
            // SAFETY: table_ctx lives until the query context resets.
            let table_mcx = unsafe { hs.table_ctx.as_ref() }.mcx();
            let (ix, isnew) = hs.hashtable.lookup(outer_slot, hash, Some(table_mcx), mcx)?;
            let ix = ix.expect("creating lookup always yields an entry");
            let pg = pergroup(&hs.hashtable, ix);
            // SAFETY: the additional block is maxaligned and sized for
            // SetOpPerGroup (execgrouping contract).
            unsafe {
                if isnew {
                    pg.write(SetOpPerGroup { num_left: 0, num_right: 0 });
                }
                (*pg.as_ptr()).num_left += 1;
            }
        }
        estate.reset_expr_context(ecxt);
    }

    if have_tuples {
        loop {
            let Some(inner_id) = fetch_inner(estate)? else { break };
            {
                let StrategyState::Hashed(hs) = &mut node.strategy else { unreachable!() };
                let inner_slot = estate.slot_mut(inner_id);
                let hash = hs.hashtable.hash_slot(inner_slot)?;
                let (ix, _) = hs.hashtable.lookup(inner_slot, hash, None, mcx)?;
                if let Some(ix) = ix {
                    // SAFETY: pergroup was initialized when the entry was
                    // created in the outer pass.
                    unsafe { (*pergroup(&hs.hashtable, ix).as_ptr()).num_right += 1 };
                }
            }
            estate.reset_expr_context(ecxt);
        }
    }

    let StrategyState::Hashed(hs) = &mut node.strategy else { unreachable!() };
    hs.table_filled = true;
    hs.hashiter = 0;
    Ok(())
}

// setop_retrieve_hash_table (nodeSetOp.c).
fn setop_retrieve_hash_table<'mcx>(
    node: &mut SetOpState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    while !node.setop_done {
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        let (counts, tup) = {
            let StrategyState::Hashed(hs) = &mut node.strategy else { unreachable!() };
            let Some(ix) = hs.hashtable.iterate(&mut hs.hashiter) else {
                node.setop_done = true;
                return Ok(None);
            };
            // SAFETY: pergroup was initialized during fill; the block lives
            // as long as the table.
            let counts = unsafe { pergroup(&hs.hashtable, ix).as_ptr().read() };
            (counts, hs.hashtable.entry_tuple(ix))
        };
        node.num_output = set_output_count(node.plan.cmd, counts.num_left, counts.num_right);
        if node.num_output > 0 {
            node.num_output -= 1;
            let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
            // SAFETY: entry images live in the query context for the table's
            // lifetime.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(result_slot, mcx, tup) };
            return Ok(Some(node.ps_ResultTupleSlot));
        }
    }
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    Ok(None)
}

/// `ExecEndSetOp` node-local half; the caller ends both children.
pub fn exec_end_set_op(node: &mut SetOpState<'_>) {
    node.ps_ResultTupleDesc = None;
    match &mut node.strategy {
        StrategyState::Hashed(h) => h.hashtable.release(),
        StrategyState::Sorted(s) => {
            s.left.first_slot.base_mut().tts_tupleDescriptor = None;
            s.right.first_slot.base_mut().tts_tupleDescriptor = None;
        }
    }
}

const _: () = assert!(!core::mem::needs_drop::<SortSupport>());

// Exempt: released in exec_end_set_op (hash table + fn_extras via
// TupleHashTable::release, slot descs cleared).
mcx::forget_safe_struct!(
    SetOpState<'_> { plan, ps_ExprContext, ps_ResultTupleSlot, setop_done,
        num_output; ps_ResultTupleDesc, strategy },
);

/// `ExecReScanSetOp` node-local half; returns true when the caller must
/// rescan both children (chgParam is always NULL until the Param lanes land,
/// so a filled hash table is re-walked, never rebuilt).
pub fn exec_rescan_set_op<'mcx>(
    node: &mut SetOpState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    node.setop_done = false;
    node.num_output = 0;
    match &mut node.strategy {
        StrategyState::Hashed(hs) => {
            hs.hashiter = 0;
            false
        }
        StrategyState::Sorted(st) => {
            st.need_init = true;
            true
        }
    }
}

/// `ExecReScanSetOp` chgParam-nonnull arm: stored groups are stale — hashed
/// resets and refills the table; sorted re-reads both inputs.
pub fn exec_rescan_set_op_chg<'mcx>(node: &mut SetOpState<'mcx>, estate: &mut EStateData<'mcx>) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    node.setop_done = false;
    node.num_output = 0;
    match &mut node.strategy {
        StrategyState::Hashed(hs) => {
            hs.hashiter = 0;
            // C nodeSetOp.c:724-730: MemoryContextReset(tableContext) +
            // ResetTupleHashTable, freeing the prior generation's entries.
            // SAFETY: table_ctx lives until the query context resets; no
            // entry image is reachable once the table is reset.
            unsafe { hs.table_ctx.as_mut() }.reset();
            hs.hashtable.reset();
            hs.table_filled = false;
        }
        StrategyState::Sorted(st) => st.need_init = true,
    }
}
