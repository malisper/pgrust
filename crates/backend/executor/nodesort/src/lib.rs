// nodeSort.c. The outer child stays with the ExecProcNode dispatcher: the
// feed loop takes a monomorphized fetch closure (C's ExecProcNode indirect
// call), keeping this crate out of a cycle with the node-enum owner.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::tuplesort::{Tuplesort, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE, TUPLESORT_RANDOMACCESS};
use ::types_error::PgResult;
use ::types_nodes::plannodes::Sort;
use ::types_scan::sdir::{ForwardScanDirection, ScanDirectionIsForward};
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

// C's CHECK_FOR_INTERRUPTS at ExecSort entry.
#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub struct SortState<'mcx> {
    pub plan: &'mcx Sort<'mcx>,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    pub randomAccess: bool,
    pub bounded: bool,
    pub bound: i64,
    sort_Done: bool,
    bounded_Done: bool,
    bound_Done: i64,
    datumSort: bool,
    tuplesortstate: Option<Tuplesort>,
}

/// `ExecInitSort` minus child linkage: the caller (execProcnode's T_Sort arm)
/// inits the outer child with `sort_child_eflags` and passes its result type.
pub fn exec_init_sort<'mcx>(
    node: &'mcx Sort<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<SortState<'mcx>> {
    let randomAccess =
        eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) != 0;
    let ps_ResultTupleSlot = estate
        .exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);
    Ok(SortState {
        plan: node,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        randomAccess,
        bounded: false,
        bound: 0,
        sort_Done: false,
        bounded_Done: false,
        bound_Done: 0,
        datumSort: outer_desc.natts == 1,
        tuplesortstate: None,
    })
}

/// C shields the child from REWIND/BACKWARD/MARK.
pub fn sort_child_eflags(eflags: i32) -> i32 {
    eflags & !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)
}

impl SortState<'_> {
    pub fn sort_done(&self) -> bool {
        self.sort_Done
    }
}

/// Page-batched outer feed for the fused sort drive; `emit` must yield rows in
/// the leaf's per-tuple emission order (line-pointer order for heap batches —
/// tie order and abbrev conversion order depend on it).
pub trait SortFeedSource<'mcx> {
    fn next_batch(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<u32>;
    /// None = qual-filtered; Some = the leaf's output slot (post-projection).
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>>;
    /// True arms `emit_key`: outer column 0 is served straight from the
    /// leaf's staged column array — value/null identical to `emit` +
    /// slot_getsomeattrs(1), no qual, same row order.
    fn key_direct(&mut self, _estate: &mut EStateData<'mcx>) -> bool {
        false
    }
    /// None = staged row not covered (fallback); take the `emit` path.
    fn emit_key(&mut self, _i: u32) -> Option<(Datum, bool)> {
        None
    }
}

/// `ExecSort` over a page-batched leaf (exec-batching rung 3): identical put
/// sequence to `exec_sort`'s pull-one-slot feed, per-tuple node recursion
/// elided. Callers route here only while the sort is unbuilt and the outer
/// shape is fusible; the drain leg matches `exec_sort`.
pub fn exec_sort_batched<'mcx, S: SortFeedSource<'mcx>>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_desc: Rc<TupleDescData<'static>>,
    mut src: S,
) -> PgResult<Option<ExecSlotId>> {
    cfi()?;
    let dir = estate.es_direction;
    let mcx = estate.es_query_cxt;
    debug_assert!(node.datumSort == (outer_desc.natts == 1));

    if !node.sort_Done {
        estate.es_direction = ForwardScanDirection;

        let mut tuplesortopts = TUPLESORT_NONE;
        if node.randomAccess {
            tuplesortopts |= TUPLESORT_RANDOMACCESS;
        }
        if node.bounded {
            tuplesortopts |= TUPLESORT_ALLOWBOUNDED;
        }
        let work_mem = init_small::globals::work_mem();
        let mut ts = if node.datumSort {
            Tuplesort::begin_datum(
                outer_desc.attr(0).atttypid,
                node.plan.sortOperators[0],
                node.plan.collations[0],
                node.plan.nullsFirst[0],
                work_mem,
                tuplesortopts,
            )?
        } else {
            Tuplesort::begin_heap(
                outer_desc,
                node.plan.sortColIdx,
                node.plan.sortOperators,
                node.plan.collations,
                node.plan.nullsFirst,
                work_mem,
                tuplesortopts,
            )?
        };
        if node.bounded {
            ts.set_bound(node.bound);
        }

        if node.datumSort {
            let direct = src.key_direct(estate);
            if ts.datum_sort_is_byref() {
                loop {
                    let n = src.next_batch(estate)?;
                    if n == 0 {
                        break;
                    }
                    for i in 0..n {
                        if direct {
                            if let Some((val, isnull)) = src.emit_key(i) {
                                ts.putdatum(val, isnull)?;
                                continue;
                            }
                        }
                        let Some(id) = src.emit(i, estate)? else { continue };
                        let slot = estate.slot_mut(id);
                        exectuples::slot_getsomeattrs(slot, 1);
                        let base = slot.base();
                        ts.putdatum(base.tts_values[0], base.tts_isnull[0])?;
                    }
                }
            } else {
                ts.putdatum_batch(|p| loop {
                    let n = src.next_batch(estate)?;
                    if n == 0 {
                        return Ok(());
                    }
                    for i in 0..n {
                        if direct {
                            if let Some((val, isnull)) = src.emit_key(i) {
                                p.put(val, isnull)?;
                                continue;
                            }
                        }
                        let Some(id) = src.emit(i, estate)? else { continue };
                        let slot = estate.slot_mut(id);
                        exectuples::slot_getsomeattrs(slot, 1);
                        let base = slot.base();
                        p.put(base.tts_values[0], base.tts_isnull[0])?;
                    }
                })?;
            }
        } else {
            loop {
                let n = src.next_batch(estate)?;
                if n == 0 {
                    break;
                }
                for i in 0..n {
                    let Some(id) = src.emit(i, estate)? else { continue };
                    ts.puttupleslot(estate.slot_mut(id), mcx)?;
                }
            }
        }

        ts.performsort()?;

        let id = node.plan.plan.plan_node_id;
        let stats = ts.get_stats();
        match estate.es_sort_instrumentation.iter_mut().find(|(i, _)| *i == id) {
            Some((_, s)) => *s = stats,
            None => estate.es_sort_instrumentation.push((id, stats)),
        }

        estate.es_direction = dir;
        node.sort_Done = true;
        node.bounded_Done = node.bounded;
        node.bound_Done = node.bound;
        node.tuplesortstate = Some(ts);
    }

    let ts = node.tuplesortstate.as_mut().expect("sort_Done without tuplesortstate");
    let slot_id = node.ps_ResultTupleSlot;
    let slot = estate.slot_mut(slot_id);
    let forward = ScanDirectionIsForward(dir);
    let got = if node.datumSort {
        exectuples::exec_clear_tuple(slot, mcx);
        match ts.getdatum(forward)? {
            Some(nd) => {
                let base = slot.base_mut();
                base.tts_values[0] = if nd.isnull { Datum::null() } else { nd.value };
                base.tts_isnull[0] = nd.isnull;
                exectuples::exec_store_virtual_tuple(slot);
                true
            }
            None => false,
        }
    } else {
        ts.gettupleslot(forward, false, slot, mcx)?
    };
    Ok(if got { Some(slot_id) } else { None })
}

/// `ExecSort`: sort the subplan on first fetch, then feed from tuplesort.
pub fn exec_sort<'mcx, F>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_desc: Rc<TupleDescData<'static>>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    cfi()?;
    let dir = estate.es_direction;
    let mcx = estate.es_query_cxt;
    debug_assert!(node.datumSort == (outer_desc.natts == 1));

    if !node.sort_Done {
        estate.es_direction = ForwardScanDirection;

        let mut tuplesortopts = TUPLESORT_NONE;
        if node.randomAccess {
            tuplesortopts |= TUPLESORT_RANDOMACCESS;
        }
        if node.bounded {
            tuplesortopts |= TUPLESORT_ALLOWBOUNDED;
        }
        let work_mem = init_small::globals::work_mem();
        let mut ts = if node.datumSort {
            Tuplesort::begin_datum(
                outer_desc.attr(0).atttypid,
                node.plan.sortOperators[0],
                node.plan.collations[0],
                node.plan.nullsFirst[0],
                work_mem,
                tuplesortopts,
            )?
        } else {
            Tuplesort::begin_heap(
                outer_desc,
                node.plan.sortColIdx,
                node.plan.sortOperators,
                node.plan.collations,
                node.plan.nullsFirst,
                work_mem,
                tuplesortopts,
            )?
        };
        if node.bounded {
            ts.set_bound(node.bound);
        }

        if node.datumSort {
            if ts.datum_sort_is_byref() {
                // By-ref datums must go through the datumCopy arm — the batch
                // putter parks raw slot pointers the next fetch recycles.
                while let Some(id) = fetch_outer(estate)? {
                    let slot = estate.slot_mut(id);
                    exectuples::slot_getsomeattrs(slot, 1);
                    let base = slot.base();
                    ts.putdatum(base.tts_values[0], base.tts_isnull[0])?;
                }
            } else {
                ts.putdatum_batch(|p| {
                    while let Some(id) = fetch_outer(estate)? {
                        let slot = estate.slot_mut(id);
                        exectuples::slot_getsomeattrs(slot, 1);
                        let base = slot.base();
                        p.put(base.tts_values[0], base.tts_isnull[0])?;
                    }
                    Ok(())
                })?;
            }
        } else {
            while let Some(id) = fetch_outer(estate)? {
                ts.puttupleslot(estate.slot_mut(id), mcx)?;
            }
        }

        ts.performsort()?;

        let id = node.plan.plan.plan_node_id;
        let stats = ts.get_stats();
        match estate.es_sort_instrumentation.iter_mut().find(|(i, _)| *i == id) {
            Some((_, s)) => *s = stats,
            None => estate.es_sort_instrumentation.push((id, stats)),
        }

        estate.es_direction = dir;
        node.sort_Done = true;
        node.bounded_Done = node.bounded;
        node.bound_Done = node.bound;
        node.tuplesortstate = Some(ts);
    }

    let ts = node.tuplesortstate.as_mut().expect("sort_Done without tuplesortstate");
    let slot_id = node.ps_ResultTupleSlot;
    let slot = estate.slot_mut(slot_id);
    let forward = ScanDirectionIsForward(dir);
    let got = if node.datumSort {
        exectuples::exec_clear_tuple(slot, mcx);
        match ts.getdatum(forward)? {
            Some(nd) => {
                let base = slot.base_mut();
                base.tts_values[0] = if nd.isnull { Datum::null() } else { nd.value };
                base.tts_isnull[0] = nd.isnull;
                exectuples::exec_store_virtual_tuple(slot);
                true
            }
            None => false,
        }
    } else {
        ts.gettupleslot(forward, false, slot, mcx)?
    };
    Ok(if got { Some(slot_id) } else { None })
}

/// `ExecEndSort` node-local half; the caller ends the outer child.
pub fn exec_end_sort(node: &mut SortState<'_>) {
    node.tuplesortstate = None;
    node.ps_ResultTupleDesc = None;
}

/// `ExecSortMarkPos`.
pub fn exec_sort_mark_pos(node: &mut SortState<'_>) {
    if !node.sort_Done {
        return;
    }
    node.tuplesortstate.as_mut().unwrap().markpos();
}

/// `ExecSortRestrPos`.
pub fn exec_sort_restr_pos(node: &mut SortState<'_>) {
    if !node.sort_Done {
        return;
    }
    node.tuplesortstate.as_mut().unwrap().restorepos();
}

/// `ExecReScanSort` node-local half. Returns true when the caller must rescan
/// the outer child (C's chgParam is always NULL until the Param lanes land).
/// ExecReScanSort (nodeSort.c), chgParam-nonnull arm: the input changed, so
/// any finished sort is stale.
pub fn exec_rescan_sort_chg<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if node.sort_Done {
        let mcx = estate.es_query_cxt;
        exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    }
    node.sort_Done = false;
    node.tuplesortstate = None;
}

pub fn exec_rescan_sort<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if !node.sort_Done {
        return false;
    }
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);

    if node.bounded != node.bounded_Done || node.bound != node.bound_Done || !node.randomAccess
    {
        node.sort_Done = false;
        node.tuplesortstate = None;
        true
    } else {
        node.tuplesortstate.as_mut().unwrap().rescan();
        false
    }
}

/// The `ExecSetTupleBound` SortState arm (execProcnode.c).
pub fn sort_set_tuple_bound(node: &mut SortState<'_>, tuples_needed: i64) {
    if tuples_needed < 0 {
        node.bounded = false;
    } else {
        node.bounded = true;
        node.bound = tuples_needed;
    }
}

/// `ExecGetResultType` for a Sort node.
pub fn sort_result_type(node: &SortState<'_>) -> Rc<TupleDescData<'static>> {
    node.ps_ResultTupleDesc.clone().expect("sort already ended")
}

// Exempt: released in exec_end_sort.
mcx::forget_safe_struct!(
    SortState<'_> { plan, ps_ResultTupleSlot, randomAccess, bounded, bound,
        sort_Done, bounded_Done, bound_Done, datumSort;
        ps_ResultTupleDesc, tuplesortstate },
);
