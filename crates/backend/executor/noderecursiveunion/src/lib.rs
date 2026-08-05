// nodeRecursiveunion.c; the C rustate pointer published through
// es_param_exec_vals[wtParam] is the estate-owned es_worktable_shared entry.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;

use ::execgrouping::TupleHashTable;
use ::executils::{EStateData, EcxtId, ExecSlotId, WorkTableShared};
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::RecursiveUnion;
use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;
use ::tuplestore::Tuplestore;

pub fn init_seams() {}

/// rescan_with_chg is C's innerPlan->chgParam={wtParam} rescan, run eagerly.
pub trait RuChild<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn rescan_with_chg(
        &mut self,
        plan: Node<'mcx>,
        estate: &mut EStateData<'mcx>,
        chg: &Bitmapset<'mcx>,
    ) -> PgResult<()>;
}

pub struct RecursiveUnionState<'mcx> {
    pub plan: &'mcx RecursiveUnion<'mcx>,
    pub inner_plan: Node<'mcx>,
    // C tempContext surrogate: reset after each hashtable lookup.
    pub ps_ExprContext: Option<EcxtId>,
    pub wt_chg: Bitmapset<'mcx>,
    recursing: bool,
    intermediate_empty: bool,
    hashtable: Option<TupleHashTable<'mcx>>,
}

/// The prmdata half of `ExecInitRecursiveUnion`; must run before child init.
pub fn exec_init_recursive_union_shared<'mcx>(
    node: &RecursiveUnion<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_desc: Rc<TupleDescData<'static>>,
) {
    let param = node.wtParam as usize;
    debug_assert!(!estate.es_param_exec_vals[param].exec_plan);
    let work_mem = init_small::globals::work_mem();
    let slot = estate.worktable_shared_slot(param);
    assert!(
        slot.is_none(),
        "ExecInitRecursiveUnion (nodeRecursiveunion.c): es_worktable_shared[{param}] occupied"
    );
    *slot = Some(WorkTableShared {
        working_table: Tuplestore::begin_heap(false, false, work_mem),
        intermediate_table: Tuplestore::begin_heap(false, false, work_mem),
        desc: result_desc,
    });
}

pub fn exec_init_recursive_union<'mcx>(
    node: &'mcx RecursiveUnion<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
) -> PgResult<RecursiveUnionState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    debug_assert!(node.plan.qual.is_nil());
    let mcx = estate.es_query_cxt;
    let inner_plan = node
        .plan
        .righttree
        .unwrap_or_else(|| panic!("ExecInitRecursiveUnion: RecursiveUnion without an inner plan"));

    let mut wt_chg = Bitmapset::empty();
    wt_chg.add_member(mcx, node.wtParam)?;

    let (hashtable, ps_ExprContext) = if node.numCols > 0 {
        debug_assert!(
            node.numGroups > 0
                && node.dupColIdx.len() == node.numCols as usize
                && node.dupOperators.len() == node.numCols as usize
                && node.dupCollations.len() == node.numCols as usize
        );
        let (eqfuncoids, hashfunctions) =
            ::execgrouping::exec_tuples_hash_prepare(mcx, node.dupOperators)?;
        // C divergence (nodesetop precedent): entries live in the query
        // context, not a rescan-reset tableContext.
        let mut hashtable = ::execgrouping::build_tuple_hash_table(
            mcx,
            outer_desc,
            node.dupColIdx,
            &eqfuncoids,
            &hashfunctions,
            node.dupCollations,
            node.numGroups.max(1) as usize,
            0,
            false,
        )?;
        let ps_ExprContext = estate.exec_assign_expr_context();
        // C BuildTupleHashTable's tempcxt = rustate->tempContext, reset
        // after each lookup (`lookup_is_new`): probe-time detoasts of
        // compressed by-ref keys must not accumulate in query memory.
        // SAFETY: the ExprContext is arena-boxed in the same estate and
        // outlives the table.
        unsafe {
            hashtable.set_temp_ctx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx())
        };
        (Some(hashtable), Some(ps_ExprContext))
    } else {
        (None, None)
    };

    Ok(RecursiveUnionState {
        plan: node,
        inner_plan,
        ps_ExprContext,
        wt_chg,
        recursing: false,
        intermediate_empty: true,
        hashtable,
    })
}

fn lookup_is_new<'mcx>(
    node: &mut RecursiveUnionState<'mcx>,
    slot_id: ExecSlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let ht = node.hashtable.as_mut().expect("numCols > 0 implies a hash table");
    let slot = estate.slot_mut(slot_id);
    let hash = ht.hash_slot(slot)?;
    let (_, isnew) = ht.lookup(slot, hash, Some(mcx), mcx)?;
    estate.reset_expr_context(node.ps_ExprContext.expect("hashing implies a temp context"));
    Ok(isnew)
}

fn take_shared<'mcx>(
    node: &RecursiveUnionState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> WorkTableShared {
    let param = node.plan.wtParam as usize;
    estate.worktable_shared_slot(param).take().unwrap_or_else(|| {
        panic!("ExecRecursiveUnion (nodeRecursiveunion.c): es_worktable_shared[{param}] missing")
    })
}

fn put_shared<'mcx>(
    node: &RecursiveUnionState<'mcx>,
    estate: &mut EStateData<'mcx>,
    shared: WorkTableShared,
) {
    *estate.worktable_shared_slot(node.plan.wtParam as usize) = Some(shared);
}

/// The shared entry is back in the estate (take/put) around every child call
/// so descendant WorkTableScans reach it.
pub fn exec_recursive_union<'mcx, O, I>(
    node: &mut RecursiveUnionState<'mcx>,
    outer: &mut O,
    inner: &mut I,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    O: RuChild<'mcx>,
    I: RuChild<'mcx>,
{
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    let mcx = estate.es_query_cxt;
    let num_cols = node.plan.numCols;

    if !node.recursing {
        loop {
            let Some(slot_id) = outer.exec_proc(estate)? else { break };
            if num_cols > 0 && !lookup_is_new(node, slot_id, estate)? {
                continue;
            }
            let mut shared = take_shared(node, estate);
            let put = shared.working_table.puttupleslot(estate.slot_mut(slot_id), mcx);
            put_shared(node, estate, shared);
            put?;
            return Ok(Some(slot_id));
        }
        node.recursing = true;
    }

    loop {
        let Some(slot_id) = inner.exec_proc(estate)? else {
            if node.intermediate_empty {
                break;
            }
            let mut shared = take_shared(node, estate);
            shared.working_table.clear();
            core::mem::swap(&mut shared.working_table, &mut shared.intermediate_table);
            put_shared(node, estate, shared);
            node.intermediate_empty = true;
            inner.rescan_with_chg(node.inner_plan, estate, &node.wt_chg)?;
            continue;
        };

        if num_cols > 0 && !lookup_is_new(node, slot_id, estate)? {
            continue;
        }
        node.intermediate_empty = false;
        let mut shared = take_shared(node, estate);
        let put = shared.intermediate_table.puttupleslot(estate.slot_mut(slot_id), mcx);
        put_shared(node, estate, shared);
        put?;
        return Ok(Some(slot_id));
    }

    Ok(None)
}

pub fn exec_end_recursive_union<'mcx>(
    node: &mut RecursiveUnionState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if let Some(shared) = estate.worktable_shared_slot(node.plan.wtParam as usize).take() {
        shared.working_table.end();
        shared.intermediate_table.end();
    }
    if let Some(ht) = node.hashtable.as_mut() {
        ht.release();
    }
}

/// Node-local half; the caller rescans outer plainly and inner with wt_chg.
pub fn exec_rescan_recursive_union<'mcx>(
    node: &mut RecursiveUnionState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if let Some(ht) = node.hashtable.as_mut() {
        ht.reset();
    }
    node.recursing = false;
    node.intermediate_empty = true;
    if let Some(shared) = estate.worktable_shared_slot(node.plan.wtParam as usize).as_mut() {
        shared.working_table.clear();
        shared.intermediate_table.clear();
    }
}

// Exempt: hashtable released in exec_end_recursive_union.
mcx::forget_safe_struct!(
    RecursiveUnionState<'_> { plan, inner_plan, ps_ExprContext, recursing,
        intermediate_empty; wt_chg, hashtable },
);
