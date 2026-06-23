//! Port of `src/backend/executor/nodeIncrementalSort.c` — routines to handle
//! incremental sorting of relations.
//!
//! Incremental sort is an optimized variant of multikey sort for cases when the
//! input is already sorted by a prefix of the sort keys. When a sort by
//! `(key1, … keyN)` is requested and the input is already sorted by
//! `(key1, … keyM)`, `M < N`, the input is divided into groups where the
//! presorted keys are equal and only the remaining columns are sorted.
//!
//! The node runs a two-mode hybrid state machine:
//!   * full-sort mode (`INCSORT_LOADFULLSORT` / `INCSORT_READFULLSORT`):
//!     accumulate a minimum number of tuples and sort on all columns;
//!   * presorted-prefix mode (`INCSORT_LOADPREFIXSORT` /
//!     `INCSORT_READPREFIXSORT`): once we believe we've hit a large single
//!     prefix-key group, sort only on the unsorted suffix keys.
//!
//! INTERFACE ROUTINES
//! - [`ExecInitIncrementalSort`]  - initialize node and subnodes
//! - [`ExecIncrementalSort`]      - the `ExecProcNode` callback
//! - [`ExecEndIncrementalSort`]   - shutdown node and subnodes
//! - [`ExecReScanIncrementalSort`] - rescan the sorted output
//! - parallel-query support: [`ExecIncrementalSortEstimate`] /
//!   [`ExecIncrementalSortInitializeDSM`] /
//!   [`ExecIncrementalSortInitializeWorker`] /
//!   [`ExecIncrementalSortRetrieveInstrumentation`]
//!
//! Incremental sort doesn't support backward scans or mark/restore (the current
//! sort state holds only one batch), so there are no `MarkPos`/`RestrPos`
//! routines.
//!
//! Calls into unported owners (tuplesort.c, execProcnode.c, execTuples.c,
//! execUtils.c, execAmi.c, tcop/postgres.c's interrupts, globals.c's `work_mem`,
//! lsyscache.c / fmgr.c lookups, and the parallel-executor / shm subsystems) go
//! through those owners' seam crates and panic until the owners land.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use execAmi_seams as execAmi;
use execParallel_support_seams as parallel_sup;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use postgres_seams as tcop_postgres;
use lsyscache_seams as lsyscache;
use fmgr_seams as fmgr;
use init_small_seams as globals;
use tuplesort_seams as tuplesort;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::{AttrNumber, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use execparallel::{
    ParallelContextHandle, ParallelWorkerContextHandle, PlanStateHandle,
};
use ::nodes::execnodes::{ForwardScanDirection, ScanDirectionIsForward};
use ::nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use ::nodes::nodeincrementalsort::{
    IncrementalSort, IncrementalSortGroupInfo, IncrementalSortInfo, IncrementalSortStateData,
    PresortedKeyData, SharedIncrementalSortInfo, SharedIncrementalSortInfoHeader,
    INCSORT_LOADFULLSORT, INCSORT_LOADPREFIXSORT, INCSORT_READFULLSORT, INCSORT_READPREFIXSORT,
};
use types_parallel::shared_dsm_object;
use ::nodes::nodesort::{
    TuplesortInstrumentation, TuplesortSpaceType, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE,
};
use nodes::{EStateData, PlanStateNode, SlotData, SlotId, TupleSlotKind};

/// `offsetof(SharedIncrementalSortInfo, sinfo) + nworkers *
/// sizeof(IncrementalSortInfo)` — the byte size of a `SharedIncrementalSortInfo`
/// carrying `nworkers` per-worker slots. (`offsetof(SharedIncrementalSortInfo,
/// sinfo)` is `sizeof(SharedIncrementalSortInfoHeader)` MAXALIGN'd up to
/// `IncrementalSortInfo`'s alignment.)
#[inline]
fn shared_incremental_sort_info_size(nworkers: usize) -> usize {
    use core::mem::{align_of, size_of};
    let h = size_of::<SharedIncrementalSortInfoHeader>();
    let a = align_of::<IncrementalSortInfo>();
    let off = (h + a - 1) & !(a - 1);
    off + nworkers * size_of::<IncrementalSortInfo>()
}

/// `&shared_info->sinfo[worker_index]` — the in-segment address of this worker's
/// slot in the DSM `SharedIncrementalSortInfo` flex array.
#[inline]
fn sinfo_slot_cursor(
    chunk: execparallel::SerializeCursor,
    worker_index: i32,
) -> execparallel::SerializeCursor {
    use core::mem::{align_of, size_of};
    let h = size_of::<SharedIncrementalSortInfoHeader>();
    let a = align_of::<IncrementalSortInfo>();
    let off = (h + a - 1) & !(a - 1);
    execparallel::SerializeCursor(
        chunk.0 + off + (worker_index as usize) * size_of::<IncrementalSortInfo>(),
    )
}

/// `DEFAULT_MIN_GROUP_SIZE` (nodeIncrementalSort.c) — the minimum number of
/// tuples to accumulate before starting a new group; sorting many small groups
/// with tuplesort is inefficient.
const DEFAULT_MIN_GROUP_SIZE: i64 = 32;

/// `DEFAULT_MAX_FULL_SORT_GROUP_SIZE` (nodeIncrementalSort.c) — the heuristic
/// cutoff (`2 * DEFAULT_MIN_GROUP_SIZE`) for deciding we've likely encountered a
/// large presorted-prefix group and should transition to prefix mode.
const DEFAULT_MAX_FULL_SORT_GROUP_SIZE: i64 = 2 * DEFAULT_MIN_GROUP_SIZE;

/// `Min(a, b)` (c.h).
#[inline]
fn min_i64(a: i64, b: i64) -> i64 {
    if a < b {
        a
    } else {
        b
    }
}

/// Install this crate's seam implementations. nodeIncrementalSort owns the
/// inward parallel-instrumentation hooks declared in
/// `backend-executor-nodeIncrementalSort-seams` (the parallel executor
/// dispatches to them by node tag).
pub fn init_seams() {
    nodeIncrementalSort_seams::exec_incrementalsort_estimate::set(
        exec_incrementalsort_estimate_shim,
    );
    nodeIncrementalSort_seams::exec_incrementalsort_initialize_dsm::set(
        exec_incrementalsort_initialize_dsm_shim,
    );
    nodeIncrementalSort_seams::exec_incrementalsort_initialize_worker::set(
        exec_incrementalsort_initialize_worker_shim,
    );
    nodeIncrementalSort_seams::exec_incrementalsort_retrieve_instrumentation::set(
        exec_incrementalsort_retrieve_instrumentation_shim,
    );
}

// ===========================================================================
// instrumentSortedGroup.
// ===========================================================================

/// `instrumentSortedGroup(groupInfo, sortState)` — capture tuplesort stats each
/// time a sort state is finalized, for later EXPLAIN ANALYZE output.
fn instrument_sorted_group(
    group_info: &mut IncrementalSortGroupInfo,
    sort_state: &TuplesortInstrumentation,
) {
    group_info.groupCount += 1;

    let sort_instr = *sort_state;

    // Calculate total and maximum memory and disk space used.
    match sort_instr.spaceType {
        TuplesortSpaceType::SORT_SPACE_TYPE_DISK => {
            group_info.totalDiskSpaceUsed += sort_instr.spaceUsed;
            if sort_instr.spaceUsed > group_info.maxDiskSpaceUsed {
                group_info.maxDiskSpaceUsed = sort_instr.spaceUsed;
            }
        }
        TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY => {
            group_info.totalMemorySpaceUsed += sort_instr.spaceUsed;
            if sort_instr.spaceUsed > group_info.maxMemorySpaceUsed {
                group_info.maxMemorySpaceUsed = sort_instr.spaceUsed;
            }
        }
    }

    // Track each sort method we've used.
    group_info.sortMethods |= sort_instr.sortMethod as i32 as u32;
}

/// `INSTRUMENT_SORT_GROUP(node, fullsort)` (nodeIncrementalSort.c macro): read
/// the full sort state's stats and fold them into the correct group-info slot.
fn instrument_sort_group_fullsort(node: &mut IncrementalSortStateData<'_>) -> PgResult<()> {
    if node.ss.ps.instrument.is_none() {
        return Ok(());
    }
    let stats = {
        let ts = node
            .fullsort_state
            .as_deref_mut()
            .ok_or_else(|| missing_sort_state("fullsort"))?;
        tuplesort::tuplesort_get_stats::call(ts)
    };
    fold_group_info(node, &stats, |info| &mut info.fullsortGroupInfo)
}

/// `INSTRUMENT_SORT_GROUP(node, prefixsort)` — the prefix-sort variant.
fn instrument_sort_group_prefixsort(node: &mut IncrementalSortStateData<'_>) -> PgResult<()> {
    if node.ss.ps.instrument.is_none() {
        return Ok(());
    }
    let stats = {
        let ts = node
            .prefixsort_state
            .as_deref_mut()
            .ok_or_else(|| missing_sort_state("prefixsort"))?;
        tuplesort::tuplesort_get_stats::call(ts)
    };
    fold_group_info(node, &stats, |info| &mut info.prefixsortGroupInfo)
}

/// The C `INSTRUMENT_SORT_GROUP` macro selects either the shared-info worker
/// slot (`node->shared_info && node->am_worker`) or the node's local
/// `incsort_info`, then folds `sortState`'s stats into the chosen group-info via
/// `instrumentSortedGroup`. `pick` selects the full/prefix group-info sub-field
/// of an `IncrementalSortInfo`.
///
/// When the worker is attached to DSM (`Dsm` arm), it folds into ITS OWN
/// `sinfo[ParallelWorkerNumber]` slot in the shared segment via `with_mut` (the
/// worker is the sole writer of that element); otherwise into the node-local
/// `incsort_info`.
fn fold_group_info(
    node: &mut IncrementalSortStateData<'_>,
    stats: &TuplesortInstrumentation,
    pick: impl Fn(&mut IncrementalSortInfo) -> &mut IncrementalSortGroupInfo,
) -> PgResult<()> {
    if node.am_worker {
        if let Some(SharedIncrementalSortInfo::Dsm {
            chunk,
            seg,
            num_workers,
        }) = node.shared_info.as_ref()
        {
            // Assert(IsParallelWorker());
            // Assert(ParallelWorkerNumber <= node->shared_info->num_workers);
            let worker_number = transam_parallel::parallel_worker_number();
            debug_assert!(worker_number >= 0 && worker_number <= *num_workers);
            if worker_number < 0 || worker_number >= *num_workers {
                return Err(worker_slot_oob());
            }
            let elem = sinfo_slot_cursor(*chunk, worker_number);
            shared_dsm_object::with_mut::<IncrementalSortInfo, ()>(*seg, elem, |sinfo| {
                instrument_sorted_group(pick(sinfo), stats);
            });
            return Ok(());
        }
    }
    instrument_sorted_group(pick(&mut node.incsort_info), stats);
    Ok(())
}

// ===========================================================================
// preparePresortedCols.
// ===========================================================================

/// `preparePresortedCols(node)` — prepare the cached comparison functions used
/// by `isCurrentGroup` for the presorted-key columns.
fn prepare_presorted_cols<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    let keys = {
        let plannode = incremental_sort_plan(node)?;
        let n = plannode.nPresortedCols;
        let mut keys: PgVec<PresortedKeyData> =
            vec_with_capacity_in(estate.es_query_cxt, n.max(0) as usize)?;
        // Pre-cache comparison functions for each pre-sorted key.
        for i in 0..n as usize {
            // key->attno = plannode->sort.sortColIdx[i];
            let attno = *plannode
                .sort
                .sortColIdx
                .get(i)
                .ok_or_else(|| missing_sortkey())?;

            // equalityOp = get_equality_op_for_ordering_op(
            //                  plannode->sort.sortOperators[i], NULL);
            let sort_op = *plannode
                .sort
                .sortOperators
                .get(i)
                .ok_or_else(|| missing_sortkey())?;
            let collation = *plannode
                .sort
                .collations
                .get(i)
                .ok_or_else(|| missing_sortkey())?;

            let equality_op = match lsyscache::get_equality_op_for_ordering_op::call(sort_op)? {
                Some((eqop, _reverse)) if oid_is_valid(eqop) => eqop,
                // if (!OidIsValid(equalityOp))
                //     elog(ERROR, "missing equality operator for ordering operator %u", ...);
                _ => return Err(missing_equality_operator(sort_op)),
            };

            // equalityFunc = get_opcode(equalityOp);
            // if (!OidIsValid(equalityFunc))
            //     elog(ERROR, "missing function for operator %u", equalityOp);
            let equality_func = lsyscache::get_opcode::call(equality_op)?;
            if !oid_is_valid(equality_func) {
                return Err(missing_function_for_operator(equality_op));
            }

            // fmgr_info_cxt(equalityFunc, &key->flinfo, CurrentMemoryContext);
            // (verify the lookup; the owned model re-resolves by OID at call time)
            fmgr::fmgr_info_check::call(equality_func)?;

            keys.push(PresortedKeyData {
                eq_func: equality_func,
                collation,
                attno: attno as AttrNumber,
            });
        }
        keys
    };
    node.presorted_keys = Some(keys);
    Ok(())
}

// ===========================================================================
// isCurrentGroup.
// ===========================================================================

/// `slot_getattr(slot, attno, &isnull)` for one of the (possibly standalone)
/// slots involved in `isCurrentGroup` / `switchToPresortedPrefixMode`.
#[derive(Clone, Copy)]
enum CmpSlot {
    /// `node->group_pivot` — a standalone slot.
    GroupPivot,
    /// `node->transfer_tuple` — a standalone slot.
    TransferTuple,
    /// An `es_tupleTable` pool slot (the outer node's result slot).
    Pool(SlotId),
}

/// Fetch attribute `attno` of the named slot as `(datum, isnull)`.
fn cmp_slot_getattr<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: CmpSlot,
    attno: AttrNumber,
) -> PgResult<(types_tuple::heaptuple::Datum<'mcx>, bool)> {
    match which {
        CmpSlot::GroupPivot => {
            let mcx = estate.es_query_cxt;
            let slot = node
                .group_pivot
                .as_deref_mut()
                .ok_or_else(|| missing_standalone_slot("group_pivot"))?;
            execTuples::slot_getattr_standalone::call(mcx, slot, attno)
        }
        CmpSlot::TransferTuple => {
            let mcx = estate.es_query_cxt;
            let slot = node
                .transfer_tuple
                .as_deref_mut()
                .ok_or_else(|| missing_standalone_slot("transfer_tuple"))?;
            execTuples::slot_getattr_standalone::call(mcx, slot, attno)
        }
        CmpSlot::Pool(id) => execTuples::slot_getattr::call(estate, id, attno),
    }
}

/// `isCurrentGroup(node, pivot, tuple)` — does `tuple` belong to the current
/// sort group, i.e. do its presorted-column values equal those of `pivot`?
///
/// Compares starting from the last presorted column (tail keys are more likely
/// to change), short-circuiting on the first inequality.
fn is_current_group<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    pivot: CmpSlot,
    tuple: CmpSlot,
) -> PgResult<bool> {
    let n_presorted_cols = incremental_sort_plan(node)?.nPresortedCols;

    for i in (0..n_presorted_cols as usize).rev() {
        // attno = node->presorted_keys[i].attno;
        let key = *node
            .presorted_keys
            .as_ref()
            .ok_or_else(|| missing_standalone_slot("presorted_keys"))?
            .get(i)
            .ok_or_else(|| missing_sortkey())?;
        let attno = key.attno;

        // datumA = slot_getattr(pivot, attno, &isnullA);
        // datumB = slot_getattr(tuple, attno, &isnullB);
        let (datum_a, isnull_a) = cmp_slot_getattr(node, estate, pivot, attno)?;
        let (datum_b, isnull_b) = cmp_slot_getattr(node, estate, tuple, attno)?;

        // Special case for NULL-vs-NULL, else use standard comparison.
        if isnull_a || isnull_b {
            if isnull_a == isnull_b {
                continue;
            } else {
                return Ok(false);
            }
        }

        // result = FunctionCallInvoke(key->fcinfo); the equality op is strict, so
        // a NULL result (the C `elog(ERROR, "function %u returned NULL")`) is
        // carried on `Err` by the fmgr seam.
        let mcx = estate.es_query_cxt;
        let result =
            fmgr::function_call2_coll_datum::call(mcx, key.eq_func, key.collation, datum_a, datum_b)?;

        // if (!DatumGetBool(result)) return false;
        if !result.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

// ===========================================================================
// switchToPresortedPrefixMode.
// ===========================================================================

/// `switchToPresortedPrefixMode(pstate)` — transition from full-sort mode into
/// presorted-prefix mode after we've concluded we're likely in a large single
/// prefix-key group.
fn switch_to_presorted_prefix_mode<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // dir = node->ss.ps.state->es_direction;
    let dir = estate.es_direction;
    // tupDesc = ExecGetResultType(outerNode);
    // (resolved inside the begin path below)

    // Configure the prefix sort state the first time around.
    if node.prefixsort_state.is_none() {
        // Optimize the sort by assuming the prefix columns are all equal and thus
        // only sort by the remaining suffix columns:
        //   tuplesort_begin_heap(tupDesc, numCols - nPresortedCols,
        //       &sortColIdx[nPresortedCols], &sortOperators[nPresortedCols],
        //       &collations[nPresortedCols], &nullsFirst[nPresortedCols],
        //       work_mem, NULL,
        //       node->bounded ? TUPLESORT_ALLOWBOUNDED : TUPLESORT_NONE);
        let sortopt = if node.bounded {
            TUPLESORT_ALLOWBOUNDED
        } else {
            TUPLESORT_NONE
        };
        let work_mem = globals::work_mem::call();
        let mcx = estate.es_query_cxt;
        let prefixsort_state = {
            let plannode = incremental_sort_plan(node)?;
            let n_presorted = plannode.nPresortedCols as usize;
            let num_cols = plannode.sort.numCols;
            let outer = outer_plan_state(node)?;
            let tupdesc = execTuples::exec_get_result_type::call(&outer.ps_head())
                .ok_or_else(|| missing_result_type())?;
            tuplesort::tuplesort_begin_heap::call(
                mcx,
                tupdesc,
                num_cols - n_presorted as i32,
                &plannode.sort.sortColIdx[n_presorted..],
                &plannode.sort.sortOperators[n_presorted..],
                &plannode.sort.collations[n_presorted..],
                &plannode.sort.nullsFirst[n_presorted..],
                work_mem,
                sortopt,
            )?
        };
        node.prefixsort_state = Some(alloc_in(mcx, prefixsort_state)?);
    } else {
        // Next group of presorted data.
        let ts = node
            .prefixsort_state
            .as_deref_mut()
            .expect("checked is_some");
        tuplesort::tuplesort_reset::call(ts)?;
    }

    // If the current node has a bound, configure the tuplesort to allow for the
    // bounded-sort optimization.
    if node.bounded {
        let bound = node.bound - node.bound_Done;
        let ts = node
            .prefixsort_state
            .as_deref_mut()
            .expect("prefixsort_state set above");
        tuplesort::tuplesort_set_bound::call(ts, bound)?;
    }

    // Copy as many tuples as we can (i.e., in the same prefix key group) from the
    // full sort state to the prefix sort state.
    let mut n_tuples: i64 = 0;
    while n_tuples < node.n_fullsort_remaining {
        // When we encounter multiple prefix key groups inside the full sort
        // tuplesort we have to carry over the last read tuple into the next batch.
        if n_tuples == 0 && !transfer_tuple_is_null(node) {
            // tuplesort_puttupleslot(node->prefixsort_state, node->transfer_tuple);
            put_standalone(node, estate.es_query_cxt, SortState::Prefix, StandaloneSlot::TransferTuple)?;
            // ExecCopySlot(node->group_pivot, node->transfer_tuple);
            copy_standalone(node, estate, StandaloneSlot::GroupPivot, StandaloneSlot::TransferTuple)?;
        } else {
            // tuplesort_gettupleslot(node->fullsort_state,
            //     ScanDirectionIsForward(dir), false, node->transfer_tuple, NULL);
            get_standalone(
                node,
                SortState::Full,
                ScanDirectionIsForward(dir),
                false,
                StandaloneSlot::TransferTuple,
            )?;

            // If this is our first time through, save the first tuple we get as
            // our new group pivot.
            if group_pivot_is_null(node) {
                copy_standalone(
                    node,
                    estate,
                    StandaloneSlot::GroupPivot,
                    StandaloneSlot::TransferTuple,
                )?;
            }

            if is_current_group(node, estate, CmpSlot::GroupPivot, CmpSlot::TransferTuple)? {
                put_standalone(node, estate.es_query_cxt, SortState::Prefix, StandaloneSlot::TransferTuple)?;
            } else {
                // The tuple isn't part of the current batch so we carry it over
                // into the next batch (it's already in transfer_tuple). Reset the
                // group pivot since we've finished the current prefix key group.
                clear_standalone(node, StandaloneSlot::GroupPivot)?;
                // Break out of for-loop early.
                break;
            }
        }
        n_tuples += 1;
    }

    // Track how many tuples remain in the full sort batch.
    node.n_fullsort_remaining -= n_tuples;

    if node.n_fullsort_remaining == 0 {
        // All remaining tuples in the full sort batch are in the same prefix key
        // group and have been moved into the prefix tuplesort. Save our pivot and
        // continue fetching from the outer node into the prefix tuplesort.
        copy_standalone(node, estate, StandaloneSlot::GroupPivot, StandaloneSlot::TransferTuple)?;
        node.execution_status = INCSORT_LOADPREFIXSORT;

        // Clear the transfer tuple slot so next time we don't incorrectly assume
        // we have a tuple carried over from the previous group.
        clear_standalone(node, StandaloneSlot::TransferTuple)?;
    } else {
        // We finished a group but didn't consume all the tuples from the full sort
        // state, so we'll sort this batch, let the outer node read it out, then
        // come back around to find another batch.
        {
            let ts = node
                .prefixsort_state
                .as_deref_mut()
                .expect("prefixsort_state set above");
            tuplesort::tuplesort_performsort::call(ts)?;
        }

        instrument_sort_group_prefixsort(node)?;

        if node.bounded {
            // If the node has a bound and we've sorted n tuples, the functional
            // bound remaining is (original bound - n).
            node.bound_Done = min_i64(node.bound, node.bound_Done + n_tuples);
        }

        node.execution_status = INCSORT_READPREFIXSORT;
    }

    Ok(())
}

// ===========================================================================
// ExecIncrementalSort — the ExecProcNode callback.
// ===========================================================================

/// `ExecIncrementalSort(pstate)` — the `PlanState.ExecProcNode` callback.
///
/// Assuming the outer subtree returns tuples presorted by some prefix of the
/// target sort columns, performs incremental sort. Returns `Ok(true)` when the
/// node's result slot holds a tuple, `Ok(false)` when it is empty (the C
/// `TupIsNull(slot)` end condition the parent observes).
pub fn ExecIncrementalSort<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    tcop_postgres::check_for_interrupts::call()?;

    // estate = node->ss.ps.state; dir = estate->es_direction;
    let dir = estate.es_direction;

    let mut n_tuples: i64 = 0;

    // If a previous iteration has sorted a batch, check for remaining tuples we
    // can return before moving on to other execution states.
    let exec_status = node.execution_status;
    if exec_status == INCSORT_READFULLSORT || exec_status == INCSORT_READPREFIXSORT {
        // Return next tuple from the current sorted group set if available.
        let read_which = if exec_status == INCSORT_READFULLSORT {
            SortState::Full
        } else {
            SortState::Prefix
        };
        let result_slot = result_slot_id(node)?;

        // We populate the slot from the tuplesort before checking outerNodeDone
        // because it sets the slot to NULL if no more tuples remain.
        let got = get_into_pool(node, estate, read_which, ScanDirectionIsForward(dir), false, result_slot)?;
        if got || node.outerNodeDone {
            return Ok(result_slot_has_tuple(node, estate));
        } else if node.n_fullsort_remaining > 0 {
            // We still have tuples remaining in the full sort state, so re-execute
            // the prefix mode transition to pull out the next prefix key group.
            switch_to_presorted_prefix_mode(node, estate)?;
        } else {
            // No sorted tuples to read and not transitioning into prefix mode, so
            // start over by building a new group in the full sort state.
            node.execution_status = INCSORT_LOADFULLSORT;
        }
    }

    // Scan the subplan in the forward direction while creating the sorted data.
    estate.es_direction = ForwardScanDirection;

    // Load tuples into the full sort state.
    if node.execution_status == INCSORT_LOADFULLSORT {
        // Initialize sorting structures.
        if node.fullsort_state.is_none() {
            // Initialize presorted column support structures for isCurrentGroup().
            // Correct to do this with the full sort state init since we always
            // load the full sort state first.
            prepare_presorted_cols(node, estate)?;

            // Setup the full sort tuplesort to sort by all requested sort keys.
            let sortopt = if node.bounded {
                TUPLESORT_ALLOWBOUNDED
            } else {
                TUPLESORT_NONE
            };
            let work_mem = globals::work_mem::call();
            let mcx = estate.es_query_cxt;
            let fullsort_state = {
                let plannode = incremental_sort_plan(node)?;
                let num_cols = plannode.sort.numCols;
                let outer = outer_plan_state(node)?;
                let tupdesc = execTuples::exec_get_result_type::call(&outer.ps_head())
                    .ok_or_else(|| missing_result_type())?;
                tuplesort::tuplesort_begin_heap::call(
                    mcx,
                    tupdesc,
                    num_cols,
                    &plannode.sort.sortColIdx,
                    &plannode.sort.sortOperators,
                    &plannode.sort.collations,
                    &plannode.sort.nullsFirst,
                    work_mem,
                    sortopt,
                )?
            };
            node.fullsort_state = Some(alloc_in(mcx, fullsort_state)?);
        } else {
            // Reset sort for the next batch.
            let ts = node.fullsort_state.as_deref_mut().expect("checked is_some");
            tuplesort::tuplesort_reset::call(ts)?;
        }

        // Calculate the remaining tuples left if bounded and configure both
        // bounded sort and the minimum group size accordingly.
        let min_group_size: i64 = if node.bounded {
            let current_bound = node.bound - node.bound_Done;

            // Bounded sort isn't likely useful for full sort mode; only set the
            // bound when it's below the minimum group size.
            if current_bound < DEFAULT_MIN_GROUP_SIZE {
                let ts = node.fullsort_state.as_deref_mut().expect("set above");
                tuplesort::tuplesort_set_bound::call(ts, current_bound)?;
            }

            min_i64(DEFAULT_MIN_GROUP_SIZE, current_bound)
        } else {
            DEFAULT_MIN_GROUP_SIZE
        };

        // On subsequent groups we have to carry over the extra tuple we read to
        // detect the new prefix key group, and add it to the new group's sort
        // before reading any new tuples from the outer node.
        if !group_pivot_is_null(node) {
            put_standalone(node, estate.es_query_cxt, SortState::Full, StandaloneSlot::GroupPivot)?;
            n_tuples += 1;

            // We can't assume the group pivot tuple will remain the same -- unless
            // we're using a minimum group size of 1.
            if n_tuples != min_group_size {
                clear_standalone(node, StandaloneSlot::GroupPivot)?;
            }
        }

        // Pull as many tuples from the outer node as possible given our mode.
        loop {
            // slot = ExecProcNode(outerNode);
            let slot = next_outer_slot(node, estate)?;

            // If the outer node can't provide more tuples, sort the current group.
            if slot.is_none() {
                // Remember the outer node completed so we can distinguish "done
                // with a batch" from "done with the whole node".
                node.outerNodeDone = true;

                {
                    let ts = node.fullsort_state.as_deref_mut().expect("set above");
                    tuplesort::tuplesort_performsort::call(ts)?;
                }

                instrument_sort_group_fullsort(node)?;

                node.execution_status = INCSORT_READFULLSORT;
                break;
            }
            let outer_slot = slot.expect("checked is_some");

            // Accumulate the next group of presorted tuples.
            if n_tuples < min_group_size {
                // Until we hit our target minimum group size we don't check for
                // inclusion in the current prefix group; we'll full sort this batch
                // to avoid lots of tiny inefficient sorts.
                put_pool(node, estate, SortState::Full, outer_slot)?;
                n_tuples += 1;

                // Once we've reached our minimum group size, store the most recent
                // tuple as a pivot.
                if n_tuples == min_group_size {
                    copy_pool_into_standalone(node, estate, StandaloneSlot::GroupPivot, outer_slot)?;
                }
            } else if is_current_group(node, estate, CmpSlot::GroupPivot, CmpSlot::Pool(outer_slot))? {
                // As long as the prefix keys match the pivot, load the tuple.
                put_pool(node, estate, SortState::Full, outer_slot)?;
                n_tuples += 1;
            } else {
                // The fetched tuple isn't part of the current prefix key group;
                // use the group_pivot slot to carry it over to the next batch.
                copy_pool_into_standalone(node, estate, StandaloneSlot::GroupPivot, outer_slot)?;

                if node.bounded {
                    node.bound_Done = min_i64(node.bound, node.bound_Done + n_tuples);
                }

                // Once we find changed prefix keys we can complete the sort and
                // transition to reading out the sorted tuples.
                {
                    let ts = node.fullsort_state.as_deref_mut().expect("set above");
                    tuplesort::tuplesort_performsort::call(ts)?;
                }

                instrument_sort_group_fullsort(node)?;

                node.execution_status = INCSORT_READFULLSORT;
                break;
            }

            // If we've read at least DEFAULT_MAX_FULL_SORT_GROUP_SIZE tuples and
            // haven't found the final tuple in the prefix key group, transition
            // into presorted prefix mode.
            if n_tuples > DEFAULT_MAX_FULL_SORT_GROUP_SIZE
                && node.execution_status != INCSORT_READFULLSORT
            {
                // The group pivot we stored has already been put into the
                // tuplesort; let the mode transition function manage that state.
                clear_standalone(node, StandaloneSlot::GroupPivot)?;

                // The tuplesort API requires a performed sort before retrieval.
                {
                    let ts = node.fullsort_state.as_deref_mut().expect("set above");
                    tuplesort::tuplesort_performsort::call(ts)?;
                }

                instrument_sort_group_fullsort(node)?;

                // If the full sort happened to switch into top-n heapsort mode we
                // can only retrieve currentBound tuples; clamp accordingly.
                let used_bound = {
                    let ts = node.fullsort_state.as_deref().expect("set above");
                    tuplesort::tuplesort_used_bound::call(ts)
                };
                if used_bound {
                    let current_bound = node.bound - node.bound_Done;
                    n_tuples = min_i64(current_bound, n_tuples);
                }

                // Tell the transition function to move from full sort to presorted
                // prefix sort.
                node.n_fullsort_remaining = n_tuples;

                // Transition the tuples to the presorted prefix tuplesort.
                switch_to_presorted_prefix_mode(node, estate)?;

                // The appropriate execution status was set by the transition
                // function, so drop out of the loop here.
                break;
            }
        }
    }

    if node.execution_status == INCSORT_LOADPREFIXSORT {
        // We only enter this state after the transition function confirmed all
        // remaining full-sort tuples share the same prefix and moved them to the
        // prefix sort state, and set a group pivot (already in the prefix state).
        debug_assert!(!group_pivot_is_null(node));

        // Read tuples from the outer node into the prefix sort state until we
        // encounter a tuple whose prefix keys don't match the group_pivot.
        loop {
            let slot = next_outer_slot(node, estate)?;

            // If we've exhausted outer-node tuples we're done loading.
            let outer_slot = match slot {
                None => {
                    node.outerNodeDone = true;
                    break;
                }
                Some(id) => id,
            };

            // If the tuple's prefix keys match the pivot, load it; otherwise carry
            // it over to the next batch via group_pivot.
            if is_current_group(node, estate, CmpSlot::GroupPivot, CmpSlot::Pool(outer_slot))? {
                put_pool(node, estate, SortState::Prefix, outer_slot)?;
                n_tuples += 1;
            } else {
                copy_pool_into_standalone(node, estate, StandaloneSlot::GroupPivot, outer_slot)?;
                break;
            }
        }

        // Perform the sort and begin returning tuples to the parent.
        {
            let ts = node
                .prefixsort_state
                .as_deref_mut()
                .ok_or_else(|| missing_sort_state("prefixsort"))?;
            tuplesort::tuplesort_performsort::call(ts)?;
        }

        instrument_sort_group_prefixsort(node)?;

        node.execution_status = INCSORT_READPREFIXSORT;

        if node.bounded {
            node.bound_Done = min_i64(node.bound, node.bound_Done + n_tuples);
        }
    }

    // Restore to user specified direction.
    estate.es_direction = dir;

    // Get the first or next tuple from tuplesort. Returns NULL if no more tuples.
    let read_which = if node.execution_status == INCSORT_READFULLSORT {
        SortState::Full
    } else {
        SortState::Prefix
    };
    let result_slot = result_slot_id(node)?;
    let _ = get_into_pool(node, estate, read_which, ScanDirectionIsForward(dir), false, result_slot)?;
    Ok(result_slot_has_tuple(node, estate))
}

/// The `PlanState.ExecProcNode` callback installed by
/// [`ExecInitIncrementalSort`]: `castNode(IncrementalSortState, pstate)` then
/// run [`ExecIncrementalSort`], returning the result slot id (the C `return
/// slot`) or `None`.
fn exec_incremental_sort_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::IncrementalSort(node) => node,
        other => panic!("castNode(IncrementalSortState, pstate) failed: {other:?}"),
    };
    if ExecIncrementalSort(node, estate)? {
        Ok(node.ss.ps.ps_ResultTupleSlot)
    } else {
        Ok(None)
    }
}

// ===========================================================================
// ExecInitIncrementalSort.
// ===========================================================================

/// `ExecInitIncrementalSort(node, estate, eflags)` — create the run-time state
/// for the incremental-sort plan node and initialize its outer subtree.
pub fn ExecInitIncrementalSort<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, IncrementalSortStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // Incremental sort can't be used with EXEC_FLAG_BACKWARD or EXEC_FLAG_MARK,
    // because the current sort state contains only one sort batch rather than
    // the full result set.
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    // castNode(IncrementalSort, node).
    let _ = node.expect_incrementalsort();

    // Initialize state structure.
    //   incrsortstate = makeNode(IncrementalSortState);
    //   incrsortstate->ss.ps.plan = (Plan *) node;
    //   incrsortstate->ss.ps.state = estate;
    //   incrsortstate->ss.ps.ExecProcNode = ExecIncrementalSort;
    let mut incrsortstate = alloc_in(mcx, IncrementalSortStateData::default())?;
    incrsortstate.ss.ps.plan = Some(node);
    incrsortstate.ss.ps.ExecProcNode = Some(exec_incremental_sort_node);

    incrsortstate.execution_status = INCSORT_LOADFULLSORT;
    incrsortstate.bounded = false;
    incrsortstate.outerNodeDone = false;
    incrsortstate.bound_Done = 0;
    incrsortstate.fullsort_state = None;
    incrsortstate.prefixsort_state = None;
    incrsortstate.group_pivot = None;
    incrsortstate.transfer_tuple = None;
    incrsortstate.n_fullsort_remaining = 0;
    incrsortstate.presorted_keys = None;

    if incrsortstate.ss.ps.instrument.is_some() {
        // Zero the group-info structures (the makeNode default already zeroed
        // them, but mirror the C explicit initialization for fidelity).
        incrsortstate.incsort_info.fullsortGroupInfo = IncrementalSortGroupInfo::default();
        incrsortstate.incsort_info.prefixsortGroupInfo = IncrementalSortGroupInfo::default();
    }

    // Miscellaneous initialization.
    //
    // Sort nodes don't initialize their ExprContexts because they never call
    // ExecQual or ExecProject.

    // Initialize child nodes. Incremental sort doesn't support backwards scans
    // and mark/restore, but we allow passing REWIND because child nodes may use
    // it; so we don't strip eflags here.
    //   outerPlanState(incrsortstate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = incremental_sort_plan_of(node)?.sort.plan.lefttree.as_deref();
    incrsortstate.ss.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // Initialize scan slot and type.
    //   ExecCreateScanSlotFromOuterPlan(estate, &incrsortstate->ss, &TTSOpsMinimalTuple);
    execUtils::exec_create_scan_slot_from_outer_plan::call(
        estate,
        &mut incrsortstate.ss,
        TupleSlotKind::MinimalTuple,
    )?;

    // Initialize return slot and type. No need to initialize projection info
    // because we don't do any projections.
    //   ExecInitResultTupleSlotTL(&incrsortstate->ss.ps, &TTSOpsMinimalTuple);
    //   incrsortstate->ss.ps.ps_ProjInfo = NULL;
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut incrsortstate.ss.ps,
        estate,
        TupleSlotKind::MinimalTuple,
    )?;
    incrsortstate.ss.ps.ps_ProjInfo = None;

    // Initialize standalone slots to store a tuple for pivot prefix keys and for
    // carrying over a tuple from one batch to the next:
    //   incrsortstate->group_pivot =
    //       MakeSingleTupleTableSlot(ExecGetResultType(outerPlanState(...)),
    //                                &TTSOpsMinimalTuple);
    //   incrsortstate->transfer_tuple = (same);
    let outer_tupdesc = {
        let outer = outer_plan_state(&incrsortstate)?;
        match execTuples::exec_get_result_type::call(&outer.ps_head()) {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        }
    };
    let group_pivot =
        execTuples::make_single_tuple_table_slot::call(mcx, clone_desc(mcx, &outer_tupdesc)?, TupleSlotKind::MinimalTuple)?;
    let transfer_tuple =
        execTuples::make_single_tuple_table_slot::call(mcx, outer_tupdesc, TupleSlotKind::MinimalTuple)?;
    incrsortstate.group_pivot = Some(alloc_in(mcx, group_pivot)?);
    incrsortstate.transfer_tuple = Some(alloc_in(mcx, transfer_tuple)?);

    Ok(incrsortstate)
}

// ===========================================================================
// ExecEndIncrementalSort.
// ===========================================================================

/// `ExecEndIncrementalSort(node)` — shut down the sort node and release its
/// resources.
pub fn ExecEndIncrementalSort<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecDropSingleTupleTableSlot(node->group_pivot);
    // ExecDropSingleTupleTableSlot(node->transfer_tuple);
    if let Some(slot) = node.group_pivot.take() {
        execTuples::exec_drop_single_tuple_table_slot::call(PgBox::into_inner(slot))?;
    }
    if let Some(slot) = node.transfer_tuple.take() {
        execTuples::exec_drop_single_tuple_table_slot::call(PgBox::into_inner(slot))?;
    }

    // Release tuplesort resources.
    //   if (node->fullsort_state != NULL) { tuplesort_end(...); node->fullsort_state = NULL; }
    //   if (node->prefixsort_state != NULL) { tuplesort_end(...); node->prefixsort_state = NULL; }
    if let Some(ts) = node.fullsort_state.take() {
        tuplesort::tuplesort_end::call(ts)?;
    }
    if let Some(ts) = node.prefixsort_state.take() {
        tuplesort::tuplesort_end::call(ts)?;
    }

    // Shut down the subplan.
    //   ExecEndNode(outerPlanState(node));
    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| missing_outer_plan_state())?;
    execProcnode::exec_end_node::call(outer, estate)
}

// ===========================================================================
// ExecReScanIncrementalSort.
// ===========================================================================

/// `ExecReScanIncrementalSort(node)` — reset all state and re-execute the sort
/// along with the child node (incremental sort can't rescan efficiently).
pub fn ExecReScanIncrementalSort<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // must drop pointer to sort result tuple
    //   ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    let result_slot = result_slot_id(node)?;
    execTuples::exec_clear_tuple::call(estate, result_slot)?;

    if node.group_pivot.is_some() {
        clear_standalone(node, StandaloneSlot::GroupPivot)?;
    }
    if node.transfer_tuple.is_some() {
        clear_standalone(node, StandaloneSlot::TransferTuple)?;
    }

    node.outerNodeDone = false;
    node.n_fullsort_remaining = 0;
    node.bound_Done = 0;
    node.execution_status = INCSORT_LOADFULLSORT;

    // If we've set up either of the sort states yet, reset them. We don't drop
    // the owned states (re-using the setup is cheaper, and ExecIncrementalSort
    // guards presorted column functions on the full sort state being present, so
    // dropping here could leak).
    if let Some(ts) = node.fullsort_state.as_deref_mut() {
        tuplesort::tuplesort_reset::call(ts)?;
    }
    if let Some(ts) = node.prefixsort_state.as_deref_mut() {
        tuplesort::tuplesort_reset::call(ts)?;
    }

    // If chgParam of subnode is not null, the plan will be re-scanned by the
    // first ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chgparam_present = node
        .ss
        .ps
        .lefttree
        .as_deref()
        .ok_or_else(|| missing_outer_plan_state())?
        .ps_head()
        .chgParam
        .is_some();
    if !outer_chgparam_present {
        let outer = node
            .ss
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(|| missing_outer_plan_state())?;
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    Ok(())
}

// ===========================================================================
// Parallel query support.
// ===========================================================================

/// `ExecIncrementalSortEstimate(node, pcxt)` — estimate the shared-memory space
/// required to propagate sort statistics.
pub fn ExecIncrementalSortEstimate<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    //   if (!node->ss.ps.instrument || pcxt->nworkers == 0) return;
    if node.ss.ps.instrument.is_none() || parallel_sup::pcxt_nworkers::call(pcxt) == 0 {
        return Ok(());
    }

    let nworkers = transam_parallel::pcxt_nworkers(pcxt) as usize;

    //   size = mul_size(pcxt->nworkers, sizeof(IncrementalSortInfo));
    //   size = add_size(size, offsetof(SharedIncrementalSortInfo, sinfo));
    let size = shared_dsm_object::estimate_flex(shared_incremental_sort_info_size(nworkers));

    //   shm_toc_estimate_chunk(&pcxt->estimator, size);
    //   shm_toc_estimate_keys(&pcxt->estimator, 1);
    let estimator = transam_parallel::pcxt_estimator(pcxt);
    transam_parallel::shm_toc_estimate_chunk(estimator, size);
    transam_parallel::shm_toc_estimate_keys(estimator, 1);
    Ok(())
}

/// `ExecIncrementalSortInitializeDSM(node, pcxt)` — initialize DSM space for
/// sort statistics.
///
/// The leader `shm_toc_allocate`s a `SharedIncrementalSortInfo` chunk in DSM,
/// zero-fills it, sets `num_workers`, and registers it under
/// `node->ss.ps.plan->plan_node_id`, stashing the DSM cursor in
/// `node->shared_info` (the `Dsm` arm). Mirrors nodeSort's
/// `ExecSortInitializeDSM` exactly.
pub fn ExecIncrementalSortInitializeDSM<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    // don't need this if not instrumenting or no workers
    let nworkers = transam_parallel::pcxt_nworkers(pcxt);
    if node.ss.ps.instrument.is_none() || nworkers == 0 {
        return Ok(());
    }

    let plan_node_id = incremental_sort_plan(node)?.sort.plan.plan_node_id;

    //   size = offsetof(SharedIncrementalSortInfo, sinfo)
    //          + pcxt->nworkers * sizeof(IncrementalSortInfo);
    let size = shared_incremental_sort_info_size(nworkers as usize);

    //   node->shared_info = shm_toc_allocate(pcxt->toc, size);
    let toc = transam_parallel::pcxt_toc(pcxt);
    let chunk =
        transam_parallel::shm_toc_allocate(toc, shared_dsm_object::estimate_flex(size));

    // A parallel query with instrument-bearing workers always has a real DSM
    // segment.
    let seg = transam_parallel::pcxt_seg(pcxt).expect(
        "ExecIncrementalSortInitializeDSM: instrumenting parallel query without a DSM segment",
    );

    //   /* ensure any unfilled slots will contain zeroes */
    //   memset(node->shared_info, 0, size);
    //   node->shared_info->num_workers = pcxt->nworkers;
    let (_hdr, _tail) =
        shared_dsm_object::place_flex::<SharedIncrementalSortInfoHeader, IncrementalSortInfo>(
            seg,
            chunk,
            nworkers as usize,
            SharedIncrementalSortInfoHeader { num_workers: nworkers },
            |_i| IncrementalSortInfo::default(),
        );

    //   shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, node->shared_info);
    transam_parallel::shm_toc_insert(toc, plan_node_id as u64, chunk);

    node.shared_info = Some(SharedIncrementalSortInfo::Dsm {
        chunk,
        seg,
        num_workers: nworkers,
    });
    Ok(())
}

/// `ExecIncrementalSortInitializeWorker(node, pwcxt)` — attach a worker to DSM
/// space for sort statistics.
///
/// `node->shared_info = shm_toc_lookup(pwcxt->toc, plan_node_id, true);
/// node->am_worker = true;`
///
/// The lookup is `noError = true` (missing_ok): when the leader is NOT
/// instrumenting (no EXPLAIN ANALYZE) `ExecIncrementalSortInitializeDSM`
/// returns early without inserting any chunk, so this lookup finds nothing and
/// the C leaves `node->shared_info == NULL`. Otherwise the worker attaches the
/// leader's DSM `SharedIncrementalSortInfo` (the `Dsm` arm) and later folds ONLY
/// its own `sinfo[ParallelWorkerNumber]` slot. Mirrors nodeSort's
/// `ExecSortInitializeWorker`.
pub fn ExecIncrementalSortInitializeWorker<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    let plan_node_id = incremental_sort_plan(node)?.sort.plan.plan_node_id;
    let toc = transam_parallel::pwcxt_toc(pwcxt);
    node.am_worker = true;
    match transam_parallel::shm_toc_lookup(toc, plan_node_id as u64, true) {
        None => {
            // Leader was not instrumenting: no shared stats area to attach to.
            node.shared_info = None;
        }
        Some(chunk) => {
            // Attach to the leader's DSM `SharedIncrementalSortInfo`: recover
            // num_workers from the in-segment header.
            let seg = transam_parallel::pwcxt_seg(pwcxt);
            let (hdr, _tail) = shared_dsm_object::attach_flex::<
                SharedIncrementalSortInfoHeader,
                IncrementalSortInfo,
            >(seg, chunk, 0);
            let num_workers = hdr.get().num_workers;
            node.shared_info = Some(SharedIncrementalSortInfo::Dsm {
                chunk,
                seg,
                num_workers,
            });
        }
    }
    Ok(())
}

/// `ExecIncrementalSortRetrieveInstrumentation(node)` — transfer sort statistics
/// from DSM to private memory.
///
/// `if (node->shared_info == NULL) return;` runs directly. C then `palloc`s a
/// private `SharedIncrementalSortInfo` and `memcpy`s the DSM bytes into it. The
/// DSM segment is still mapped here (the C runs this before detach): read the
/// flex array out of the segment and snapshot it into a backend-local `PgVec`;
/// `node->shared_info` then becomes the `Local` arm. Mirrors nodeSort's
/// `ExecSortRetrieveInstrumentation`.
pub fn ExecIncrementalSortRetrieveInstrumentation<'mcx>(
    mcx: Mcx<'mcx>,
    node: &mut IncrementalSortStateData<'mcx>,
) -> PgResult<()> {
    //   if (node->shared_info == NULL) return;
    let (chunk, seg, num_workers) = match node.shared_info {
        Some(SharedIncrementalSortInfo::Dsm {
            chunk,
            seg,
            num_workers,
        }) => (chunk, seg, num_workers),
        // Already a backend-local copy, or NULL: nothing to retrieve.
        _ => return Ok(()),
    };

    //   size = offsetof(SharedIncrementalSortInfo, sinfo)
    //          + node->shared_info->num_workers * sizeof(IncrementalSortInfo);
    //   si = palloc(size); memcpy(si, node->shared_info, size); node->shared_info = si;
    let (_hdr, tail) =
        shared_dsm_object::attach_flex::<SharedIncrementalSortInfoHeader, IncrementalSortInfo>(
            seg,
            chunk,
            num_workers as usize,
        );

    let mut copy: PgVec<'mcx, IncrementalSortInfo> =
        PgVec::with_capacity_in(num_workers as usize, mcx);
    for &elem in tail.get().iter() {
        copy.push(elem);
    }
    node.shared_info = Some(SharedIncrementalSortInfo::Local {
        num_workers,
        sinfo: copy,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Seam shims installed into `backend-executor-nodeIncrementalSort-seams`.
//
// `execParallel` dispatches the per-node parallel hooks generically, holding a
// `PlanState *` (the opaque [`PlanStateHandle`]); the C
// `ExecIncrementalSortEstimate` etc. begin with the `(IncrementalSortState *)
// node` cast. Recovering the live state from the handle is the executor's
// `PlanState` pointer registry (the unported executor surface, cf. #165/#169),
// so each shim panics through `resolve_incremental_sort_state` and then runs the
// real, owned-typed entry point above. This mirrors nodeSort's shims exactly.
// ---------------------------------------------------------------------------

/// `(IncrementalSortState *) node` — recover the live state a `PlanStateHandle`
/// refers to. The executor `PlanState` pointer registry that backs this lookup
/// is not yet ported.
fn resolve_incremental_sort_state<'mcx>(
    _node: PlanStateHandle,
) -> &'mcx mut IncrementalSortStateData<'mcx> {
    panic!(
        "backend-executor-nodeIncrementalSort: resolving a PlanStateHandle to the live \
         IncrementalSortState needs the executor PlanState pointer registry (unported); the \
         (IncrementalSortState *) node cast in the ExecIncrementalSort* parallel hooks cannot \
         run yet"
    );
}

fn exec_incrementalsort_estimate_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecIncrementalSortEstimate(resolve_incremental_sort_state(node), pcxt)
}

fn exec_incrementalsort_initialize_dsm_shim(
    node: PlanStateHandle,
    pcxt: ParallelContextHandle,
) -> PgResult<()> {
    ExecIncrementalSortInitializeDSM(resolve_incremental_sort_state(node), pcxt)
}

fn exec_incrementalsort_initialize_worker_shim(
    node: PlanStateHandle,
    pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    ExecIncrementalSortInitializeWorker(resolve_incremental_sort_state(node), pwcxt)
}

fn exec_incrementalsort_retrieve_instrumentation_shim(node: PlanStateHandle) -> PgResult<()> {
    ExecIncrementalSortRetrieveInstrumentation(
        resolve_retrieve_mcx(node),
        resolve_incremental_sort_state(node),
    )
}

/// `CurrentMemoryContext` (`planstate->state->es_query_cxt`) at the
/// `ExecIncrementalSortRetrieveInstrumentation` call site — recovered from the
/// same unported executor surface that backs `resolve_incremental_sort_state`,
/// so it shares that panic. (The live executor dispatches
/// `ExecIncrementalSortRetrieveInstrumentation` directly over its owned state
/// from `ExecParallelRetrieveInstrumentation`, threading the mcx in; this
/// handle-shim path is only reached by a hypothetical pointer-registry caller.)
fn resolve_retrieve_mcx<'mcx>(_node: PlanStateHandle) -> Mcx<'mcx> {
    panic!(
        "backend-executor-nodeIncrementalSort: the CurrentMemoryContext for \
         ExecIncrementalSortRetrieveInstrumentation's palloc'd copy is recovered from the unported \
         executor surface (PlanState pointer registry); cannot run yet"
    );
}

// ===========================================================================
// In-crate helpers.
// ===========================================================================

/// Which of the two owned sort states to act on.
#[derive(Clone, Copy)]
enum SortState {
    Full,
    Prefix,
}

/// Which standalone (`MakeSingleTupleTableSlot`) node slot to act on.
#[derive(Clone, Copy)]
enum StandaloneSlot {
    GroupPivot,
    TransferTuple,
}

/// `tuplesort_puttupleslot(state, slot)` for a standalone node slot.
fn put_standalone<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    mcx: Mcx<'mcx>,
    state: SortState,
    which: StandaloneSlot,
) -> PgResult<()> {
    let (slot_opt, ts_opt) = match (which, state) {
        (StandaloneSlot::GroupPivot, SortState::Full) => {
            (&mut node.group_pivot, &mut node.fullsort_state)
        }
        (StandaloneSlot::GroupPivot, SortState::Prefix) => {
            (&mut node.group_pivot, &mut node.prefixsort_state)
        }
        (StandaloneSlot::TransferTuple, SortState::Full) => {
            (&mut node.transfer_tuple, &mut node.fullsort_state)
        }
        (StandaloneSlot::TransferTuple, SortState::Prefix) => {
            (&mut node.transfer_tuple, &mut node.prefixsort_state)
        }
    };
    let slot = slot_opt
        .as_deref_mut()
        .ok_or_else(|| missing_standalone_slot("standalone slot"))?;
    // As put_pool: the standalone slot (group_pivot / transfer_tuple) is filled
    // by ExecCopySlot as a lazily-materialized MinimalTuple (tts_nvalid == 0), so
    // it must be fully deformed before the owned puttupleslot seam reads its
    // tts_values/tts_isnull arrays.
    let _ = execTuples::slot_getallattrs::call(mcx, slot)?;
    let ts = ts_opt
        .as_deref_mut()
        .ok_or_else(|| missing_sort_state("sort state"))?;
    tuplesort::tuplesort_puttupleslot_standalone::call(ts, slot)
}

/// `tuplesort_puttupleslot(state, slot)` for an `es_tupleTable` pool slot.
fn put_pool<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    state: SortState,
    slot: SlotId,
) -> PgResult<()> {
    // C's tuplesort_puttupleslot does ExecCopySlotMinimalTuple(slot), whose
    // per-kind copy_minimal_tuple callback deconstructs the source slot first.
    // The owned puttupleslot seam instead reads the slot's tts_values/tts_isnull
    // arrays directly, so a lazily-materialized slot (e.g. a MinimalTuple result
    // slot from an outer Sort node, with tts_nvalid == 0) must be fully deformed
    // first or we'd form a tuple of stale (zero) values. Mirrors nodeSort.
    let _ = execTuples::slot_getallattrs_by_id::call(estate, slot)?;
    let ts = sort_state_mut(node, state)?;
    tuplesort::tuplesort_puttupleslot::call(ts, estate.slot(slot))
}

/// `tuplesort_gettupleslot(state, forward, copy, transfer_tuple, NULL)` into the
/// standalone transfer slot.
fn get_standalone<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    state: SortState,
    forward: bool,
    copy: bool,
    which: StandaloneSlot,
) -> PgResult<bool> {
    // Split-borrow: the sort state and the standalone slot are disjoint fields.
    match (which, state) {
        (StandaloneSlot::TransferTuple, SortState::Full) => {
            let slot = node
                .transfer_tuple
                .as_deref_mut()
                .ok_or_else(|| missing_standalone_slot("transfer_tuple"))?;
            let ts = node
                .fullsort_state
                .as_deref_mut()
                .ok_or_else(|| missing_sort_state("fullsort"))?;
            tuplesort::tuplesort_gettupleslot_standalone::call(ts, forward, copy, slot)
        }
        (StandaloneSlot::TransferTuple, SortState::Prefix) => {
            let slot = node
                .transfer_tuple
                .as_deref_mut()
                .ok_or_else(|| missing_standalone_slot("transfer_tuple"))?;
            let ts = node
                .prefixsort_state
                .as_deref_mut()
                .ok_or_else(|| missing_sort_state("prefixsort"))?;
            tuplesort::tuplesort_gettupleslot_standalone::call(ts, forward, copy, slot)
        }
        (StandaloneSlot::GroupPivot, _) => Err(internal_error(
            "tuplesort_gettupleslot into group_pivot is never requested",
        )),
    }
}

/// `tuplesort_gettupleslot(state, forward, copy, ps_ResultTupleSlot, NULL)` into
/// the node's result pool slot.
fn get_into_pool<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    state: SortState,
    forward: bool,
    copy: bool,
    slot: SlotId,
) -> PgResult<bool> {
    let ts = sort_state_mut(node, state)?;
    tuplesort::tuplesort_gettupleslot::call(ts, forward, copy, estate.slot_data_mut(slot))
}

/// `ExecCopySlot(group_pivot, transfer_tuple)` — both standalone.
fn copy_standalone<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &EStateData<'mcx>,
    dst: StandaloneSlot,
    src: StandaloneSlot,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    match (dst, src) {
        (StandaloneSlot::GroupPivot, StandaloneSlot::TransferTuple) => {
            // Disjoint fields group_pivot / transfer_tuple.
            let (dst_slot, src_slot) = split_pivot_transfer(node)?;
            execTuples::exec_copy_slot_standalone::call(mcx, dst_slot, src_slot)
        }
        _ => Err(internal_error("unsupported standalone ExecCopySlot pair")),
    }
}

/// Disjoint mutable borrows of `group_pivot` and `transfer_tuple`.
fn split_pivot_transfer<'a, 'mcx>(
    node: &'a mut IncrementalSortStateData<'mcx>,
) -> PgResult<(&'a mut SlotData<'mcx>, &'a mut SlotData<'mcx>)> {
    let dst = node
        .group_pivot
        .as_deref_mut()
        .ok_or_else(|| missing_standalone_slot("group_pivot"))? as *mut SlotData<'mcx>;
    let src = node
        .transfer_tuple
        .as_deref_mut()
        .ok_or_else(|| missing_standalone_slot("transfer_tuple"))?;
    // SAFETY: `group_pivot` and `transfer_tuple` are distinct struct fields, so
    // the two `PgBox` payloads never alias; reborrowing `dst` through the raw
    // pointer recovers a mutable reference disjoint from `src`.
    let dst = unsafe { &mut *dst };
    Ok((dst, src))
}

/// `ExecCopySlot(group_pivot, pool_slot)` — standalone dst, pool src.
fn copy_pool_into_standalone<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    dst: StandaloneSlot,
    src: SlotId,
) -> PgResult<()> {
    match dst {
        StandaloneSlot::GroupPivot => {
            let dst_slot = node
                .group_pivot
                .as_deref_mut()
                .ok_or_else(|| missing_standalone_slot("group_pivot"))?;
            execTuples::exec_copy_pool_slot_into_standalone::call(estate, dst_slot, src)
        }
        StandaloneSlot::TransferTuple => Err(internal_error(
            "ExecCopySlot into transfer_tuple from a pool slot is never requested",
        )),
    }
}

/// `ExecClearTuple(group_pivot/transfer_tuple)` — standalone.
fn clear_standalone<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    which: StandaloneSlot,
) -> PgResult<()> {
    let slot = match which {
        StandaloneSlot::GroupPivot => node
            .group_pivot
            .as_deref_mut()
            .ok_or_else(|| missing_standalone_slot("group_pivot"))?,
        StandaloneSlot::TransferTuple => node
            .transfer_tuple
            .as_deref_mut()
            .ok_or_else(|| missing_standalone_slot("transfer_tuple"))?,
    };
    execTuples::exec_clear_tuple_standalone::call(slot)
}

/// `&mut node->{fullsort,prefixsort}_state`.
fn sort_state_mut<'a, 'mcx>(
    node: &'a mut IncrementalSortStateData<'mcx>,
    state: SortState,
) -> PgResult<&'a mut ::nodes::Tuplesortstate<'mcx>> {
    match state {
        SortState::Full => node
            .fullsort_state
            .as_deref_mut()
            .ok_or_else(|| missing_sort_state("fullsort")),
        SortState::Prefix => node
            .prefixsort_state
            .as_deref_mut()
            .ok_or_else(|| missing_sort_state("prefixsort")),
    }
}

/// `slot = ExecProcNode(outerNode); TupIsNull(slot) ? None : Some(slot)`.
fn next_outer_slot<'mcx>(
    node: &mut IncrementalSortStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = node
        .ss
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(|| missing_outer_plan_state())?;
    let slot = execProcnode::exec_proc_node::call(outer, estate)?;
    match slot {
        Some(id) if !estate.slot(id).is_empty() => Ok(Some(id)),
        _ => Ok(None),
    }
}

/// `(IncrementalSort *) node->ss.ps.plan`.
fn incremental_sort_plan<'a, 'mcx>(
    node: &'a IncrementalSortStateData<'mcx>,
) -> PgResult<&'a IncrementalSort<'mcx>> {
    match node.ss.ps.plan {
        Some(n) => incremental_sort_plan_of(n),
        None => Err(missing_plan()),
    }
}

/// `castNode(IncrementalSort, node)`.
fn incremental_sort_plan_of<'a, 'mcx>(
    node: &'a ::nodes::nodes::Node<'mcx>,
) -> PgResult<&'a IncrementalSort<'mcx>> {
    Ok(node.expect_incrementalsort())
}

/// `outerPlanState(node)` — `node->ss.ps.lefttree`.
fn outer_plan_state<'a, 'mcx>(
    node: &'a IncrementalSortStateData<'mcx>,
) -> PgResult<&'a PlanStateNode<'mcx>> {
    node.ss
        .ps
        .lefttree
        .as_deref()
        .ok_or_else(|| missing_outer_plan_state())
}

/// `node->ss.ps.ps_ResultTupleSlot`.
fn result_slot_id(node: &IncrementalSortStateData<'_>) -> PgResult<SlotId> {
    node.ss
        .ps
        .ps_ResultTupleSlot
        .ok_or_else(|| missing_result_slot())
}

/// `!TupIsNull(node->ss.ps.ps_ResultTupleSlot)`.
fn result_slot_has_tuple(node: &IncrementalSortStateData<'_>, estate: &EStateData<'_>) -> bool {
    match node.ss.ps.ps_ResultTupleSlot {
        None => false,
        Some(id) => !estate.slot(id).is_empty(),
    }
}

/// `TupIsNull(node->group_pivot)`.
fn group_pivot_is_null(node: &IncrementalSortStateData<'_>) -> bool {
    match node.group_pivot.as_deref() {
        None => true,
        Some(s) => s.base().is_empty(),
    }
}

/// `TupIsNull(node->transfer_tuple)`.
fn transfer_tuple_is_null(node: &IncrementalSortStateData<'_>) -> bool {
    match node.transfer_tuple.as_deref() {
        None => true,
        Some(s) => s.base().is_empty(),
    }
}

/// `OidIsValid(oid)`.
#[inline]
fn oid_is_valid(oid: Oid) -> bool {
    types_core::primitive::OidIsValid(oid)
}

/// Clone an `Option<PgBox<TupleDescData>>` for the second standalone slot
/// (`MakeSingleTupleTableSlot` takes ownership of the descriptor each time).
fn clone_desc<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>,
) -> PgResult<Option<PgBox<'mcx, types_tuple::heaptuple::TupleDescData<'mcx>>>> {
    match desc.as_deref() {
        Some(d) => Ok(Some(alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

// --- recoverable errors (internal-error ereports) -------------------------

fn ereport_internal(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

fn internal_error(msg: &'static str) -> PgError {
    ereport_internal(msg)
}

fn missing_plan() -> PgError {
    ereport_internal("IncrementalSort node has no plan back-link")
}
fn missing_outer_plan_state() -> PgError {
    ereport_internal("IncrementalSort node has no outer plan state")
}
fn missing_result_slot() -> PgError {
    ereport_internal("IncrementalSort node result slot not initialized")
}
fn missing_result_type() -> PgError {
    ereport_internal("IncrementalSort outer node result type not set")
}
fn missing_sortkey() -> PgError {
    ereport_internal("IncrementalSort plan node has too few sort keys")
}
fn missing_sort_state(_which: &'static str) -> PgError {
    ereport_internal("IncrementalSort tuplesort state not initialized")
}
fn missing_standalone_slot(_which: &'static str) -> PgError {
    ereport_internal("IncrementalSort standalone slot not initialized")
}
fn missing_equality_operator(_op: Oid) -> PgError {
    ereport_internal("missing equality operator for ordering operator")
}
fn missing_function_for_operator(_op: Oid) -> PgError {
    ereport_internal("missing function for operator")
}
fn worker_slot_oob() -> PgError {
    ereport_internal("IncrementalSort worker instrumentation slot out of range")
}

#[cfg(test)]
mod tests;
