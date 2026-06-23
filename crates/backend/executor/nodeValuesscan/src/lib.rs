//! Port of `src/backend/executor/nodeValuesscan.c` — support routines for
//! scanning `VALUES` lists (`VALUES (...), (...), ...` appearing in the range
//! table).
//!
//! INTERFACE ROUTINES
//! - [`ExecValuesScan`]        - scans a values list
//! - [`ExecInitValuesScan`]    - creates and initializes a valuesscan node
//! - [`ExecReScanValuesScan`]  - rescans the values list
//!
//! The workhorse access method [`ValuesNext`] (plus [`ValuesRecheck`]) and the
//! node entry points are ported 1:1. `ExecValuesScan` calls the `execScan.c`
//! generic driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`); that
//! driver is a static-inline header (`executor/execScan.h`) the C compiler
//! links into `nodeValuesscan.o`, so it is reproduced here as private functions
//! (specialized to the values access/recheck ABI), with every leaf operation
//! routed through its owning crate's seam. A `ValuesScan` has no executor
//! children (`Assert(outerPlan == NULL && innerPlan == NULL)`), so there is no
//! child `ExecProcNode` recursion.
//!
//! The node state machine is held as an owned [`ValuesScanState`] mutated
//! through `&mut` borrows; the C `PlanState.state` back-pointer is replaced by
//! threading `&mut EStateData` explicitly. The access method returns
//! `Ok(Some(slot))` (the C `return slot`) / `Ok(None)` (the C empty/`NULL`
//! slot). Calls into not-yet-ported owners (execScan.c's `ExecScanReScan`,
//! execTuples.c's slot ops, execExpr.c's qual/projection/eval, execUtils.c's
//! context helpers, execMain.c's EvalPlanQual, clauses.c's `contain_subplans`,
//! expandeddatum.c's `MakeExpandedObjectReadOnlyInternal`, tcop/postgres.c's
//! interrupts) go through those owners' seam crates and panic until they land.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

use execExpr_seams as execExpr;
use execMain_seams as execMain;
use execScan_seams as execScan;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use nodes_core_seams as bitmapset;
use clauses_seams as clauses;
use postgres_seams as tcop_postgres;
use misc2_seams as expandeddatum;

use mcx::alloc_in;
use types_error::PgResult;
use execparallel::PGJIT_NONE;
use nodes::nodes::Node;
use nodes::nodevaluesscan::{ValuesScan, ValuesScanState};
use nodes::{EStateData, ScanDirectionIsForward, SlotId, TupleSlotKind};

/// Access-method "function pointer" (C `ExecScanAccessMtd`): the next-tuple
/// routine `ExecScan` re-enters. Returns the scan slot id when a tuple is
/// stored, `None` at end-of-scan (the C `NULL`). Within this crate it is always
/// [`ValuesNext`].
type AccessMtd<'mcx> = fn(&mut ValuesScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<Option<SlotId>>;
/// Recheck-method "function pointer" (C `ExecScanRecheckMtd`): rechecks the
/// tuple in the node's scan slot. Within this crate it is always
/// [`ValuesRecheck`].
type RecheckMtd<'mcx> = fn(&mut ValuesScanState<'mcx>, &mut EStateData<'mcx>, SlotId) -> PgResult<bool>;

/// nodeValuesscan owns no inward seam crate: its only cross-cycle callers are
/// execProcnode's dispatch tables, which (like nodeTableFuncscan) reach it
/// directly once wired, never across a cycle. So there is nothing to install.
pub fn init_seams() {}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `ValuesNext(node)` — the workhorse for `ExecValuesScan`.
///
/// Advances `curr_idx` in the current scan direction, then — if the new index
/// is in range — resets the per-sublist context, builds (or reuses) the row's
/// expression eval state, evaluates every expression into the scan slot's
/// virtual `values`/`isnull` arrays (forcing R/W expanded datums read-only),
/// and stores the virtual tuple. Returns `Some(slot)` when a virtual tuple was
/// stored, or `None` when the scan is exhausted (the C `return slot`, which the
/// caller treats as empty via `TupIsNull`).
fn ValuesNext<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // get information from the estate and scan state:
    //   estate = node->ss.ps.state; direction = estate->es_direction;
    //   slot = node->ss.ss_ScanTupleSlot; econtext = node->rowcontext;
    let direction = estate.es_direction;
    let slot = node
        .ss
        .ss_ScanTupleSlot
        .expect("ValuesNext: ss_ScanTupleSlot not initialized");
    let econtext = node
        .rowcontext
        .expect("ValuesNext: rowcontext not initialized");

    // Get the next tuple. Return NULL if no more tuples.
    if ScanDirectionIsForward(direction) {
        if node.curr_idx < node.array_len {
            node.curr_idx += 1;
        }
    } else if node.curr_idx >= 0 {
        node.curr_idx -= 1;
    }

    // Always clear the result slot; this is appropriate if we are at the end of
    // the data, and if we're not, we still need it as the first step of the
    // store-virtual-tuple protocol. It seems wise to clear the slot before we
    // reset the context it might have pointers into.
    //   ExecClearTuple(slot);
    execTuples::exec_clear_tuple::call(estate, slot)?;

    let curr_idx = node.curr_idx;
    if curr_idx >= 0 && curr_idx < node.array_len {
        let row = curr_idx as usize;

        // Get rid of any prior cycle's leftovers. We use ReScanExprContext not
        // just ResetExprContext because we want any registered shutdown
        // callbacks to be called.
        //   ReScanExprContext(econtext);
        execUtils::re_scan_expr_context::call(estate, econtext)?;

        // Do per-VALUES-row work in the per-tuple context.
        //   oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        //
        // The owned model has no ambient current context; the seamed
        // ExecInitExprList / ExecEvalExprSwitchContext allocate against the
        // econtext's per-tuple memory by construction.

        // Unless we already made the expression eval state for this row, build
        // it. For rows containing SubPlans the eval state was already built at
        // plan startup (parent = my plan node); for everything else it is built
        // here with parent = NULL, which also disables JIT (a win for single-use
        // expressions).
        //   if (exprstatelist == NIL)
        //       exprstatelist = ExecInitExprList(exprlist, NULL);
        if node.exprstatelists[row].is_empty() {
            // ExecInitExprList(exprlist, NULL) — the row's expressions, compiled
            // with no parent plan-state.
            let built = {
                let mcx = estate.es_query_cxt;
                // The row's `Expr`s are read through a shared borrow of the
                // node's `exprlists`; the resulting `Option<&Expr>` refs live
                // only for the seam call. The compiled states are allocated in
                // the per-query context by the seam.
                let exprs = &node.exprlists[row];
                let mut refs: mcx::PgVec<'mcx, Option<&nodes::primnodes::Expr<'mcx>>> =
                    mcx::vec_with_capacity_in(mcx, exprs.len())?;
                for e in exprs.iter() {
                    refs.push(Some(e));
                }
                execExpr_seams::exec_init_expr_list_no_parent::call(&refs, estate)?
            };
            node.exprstatelists[row] = built;
        }

        // parser should have checked all sublists are the same length:
        //   Assert(list_length(exprstatelist) == slot->tts_tupleDescriptor->natts);
        let ncols = node.exprstatelists[row].len();

        // `attr->attlen` per output column, for the MakeExpandedObjectReadOnly
        // typlen test. The scan slot's descriptor is below this crate's layer
        // (the slot payload lives in execTuples), so it is read through the
        // owner's seam. `Assert(ncols == natts)` is the same shared length.
        let attlens: mcx::PgVec<'mcx, i16> = {
            let mcx = estate.es_query_cxt;
            let desc = execTuples::exec_scan_slot_descriptor::call(mcx, &node.ss, estate)?;
            let mut v = mcx::vec_with_capacity_in(mcx, ncols)?;
            match desc.as_deref() {
                Some(td) => {
                    debug_assert_eq!(ncols as i32, td.natts);
                    for i in 0..ncols {
                        v.push(td.compact_attrs.get(i).map(|a| a.attlen).unwrap_or(0));
                    }
                }
                None => {
                    for _ in 0..ncols {
                        v.push(0);
                    }
                }
            }
            v
        };

        // Compute the expressions and build a virtual result tuple. We already
        // did ExecClearTuple(slot). `values = slot->tts_values; isnull =
        // slot->tts_isnull;`
        //
        //   resind = 0; foreach(lc, exprstatelist) { ...; resind++; }
        let mcx = estate.es_query_cxt;
        let mut values: mcx::PgVec<'mcx, types_tuple::heaptuple::Datum<'mcx>> =
            mcx::vec_with_capacity_in(mcx, ncols)?;
        let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, ncols)?;
        for resind in 0..ncols {
            // values[resind] = ExecEvalExpr(estate, econtext, &isnull[resind]);
            //
            // The row's compiled `ExprState` is owned by the node; evaluation is
            // seamed into the expression interpreter (execExpr.c).
            let (value, col_isnull) = {
                // The row's compiled `ExprState` is borrowed mutably from the
                // node's owned `exprstatelists`; the seam also takes `estate`
                // mutably, but the two borrows are disjoint (the eval call
                // never touches the node's `exprstatelists`).
                let state = node.exprstatelists[row][resind]
                    .as_mut()
                    .expect("ValuesNext: row ExprState cell is NULL after build");
                execExpr_seams::exec_eval_expr_switch_context::call(
                    state, econtext, estate,
                )?
            };

            // We must force any R/W expanded datums to read-only state, in case
            // they are multiply referenced in the plan node's output
            // expressions, or in case we skip the output projection and the
            // output column is multiply referenced in higher plan nodes.
            //   values[resind] = MakeExpandedObjectReadOnly(value, isnull, attr->attlen);
            let value = MakeExpandedObjectReadOnly(estate, value, col_isnull, attlens[resind])?;

            values.push(value);
            isnull.push(col_isnull);
        }

        // MemoryContextSwitchTo(oldContext);  (folded into the seam boundaries)

        // And return the virtual tuple. The per-column `values`/`isnull` are
        // written into the (owned-by-execTuples) slot payload and the slot is
        // marked as holding a valid virtual tuple. The C wrote `slot->tts_values`
        // directly during the loop and then `ExecStoreVirtualTuple(slot)`; the
        // store seam re-clears before filling, an equivalent end state for the
        // freshly-cleared virtual slot.
        //   ExecStoreVirtualTuple(slot);
        execTuples::store_virtual_values::call(estate, slot, values.as_slice(), isnull.as_slice())?;

        return Ok(Some(slot));
    }

    // return slot;  (cleared above — the C empty slot / `NULL` to the caller)
    Ok(None)
}

/// `ValuesRecheck(node, slot)` — access-method routine to recheck a tuple in
/// EvalPlanQual. Nothing to check for a values scan, so it always succeeds.
fn ValuesRecheck<'mcx>(
    _node: &mut ValuesScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _slot: SlotId,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

/// `MakeExpandedObjectReadOnly(d, isnull, typlen)` (expandeddatum.h):
///
/// ```c
/// #define MakeExpandedObjectReadOnly(d, isnull, typlen) \
///     (((isnull) || (typlen) != -1) ? (d) : \
///      MakeExpandedObjectReadOnlyInternal(d))
/// ```
///
/// The short-circuit branch (null datum or non-varlena type) is node-layer
/// logic handled in-crate; only the read-only conversion of an expanded varlena
/// (`typlen == -1`, non-null) is below the node layer, routed through the
/// expandeddatum owner's seam (the `Datum` pointer dereference is its job).
fn MakeExpandedObjectReadOnly<'mcx>(
    estate: &mut EStateData<'mcx>,
    d: types_tuple::heaptuple::Datum<'mcx>,
    isnull: bool,
    typlen: i16,
) -> PgResult<types_tuple::heaptuple::Datum<'mcx>> {
    if isnull || typlen != -1 {
        Ok(d)
    } else {
        let mcx = estate.es_query_cxt;
        expandeddatum::make_expanded_object_read_only_internal_v::call(mcx, &d)
    }
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitValuesScan`]:
/// `castNode(ValuesScanState, pstate)` then run [`ExecValuesScan`].
fn exec_values_scan_node<'mcx>(
    pstate: &mut nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        nodes::PlanStateNode::ValuesScan(node) => node,
        other => panic!("castNode(ValuesScanState, pstate) failed: {other:?}"),
    };
    ExecValuesScan(node, estate)
}

/// `ExecValuesScan(pstate)` — scans the values lists sequentially and returns
/// the next qualifying tuple, by calling [`ExecScan`] with the values
/// access/recheck methods.
pub fn ExecValuesScan<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // return ExecScan(&node->ss, (ExecScanAccessMtd) ValuesNext,
    //                 (ExecScanRecheckMtd) ValuesRecheck);
    ExecScan(node, ValuesNext, ValuesRecheck, estate)
}

/// `ExecInitValuesScan(node, estate, eflags)` — create and initialize a
/// `ValuesScanState`.
///
/// Takes the enclosing plan-tree [`Node`] (the C `ValuesScan *`): the state's
/// plan back-link aliases the shared, read-only plan tree exactly as C's
/// `scanstate->ss.ps.plan = (Plan *) node`. Panics if the node is not a
/// `ValuesScan` (the C `castNode`).
///
/// `eflags` mirrors the C `int eflags`, which `ExecInitValuesScan` never
/// inspects (a values scan supports no special exec flags).
pub fn ExecInitValuesScan<'mcx>(
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<mcx::PgBox<'mcx, ValuesScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let plan: &'mcx ValuesScan<'mcx> = match node.as_valuesscan() {
        Some(v) => v,
        None => panic!("castNode(ValuesScan, node) failed: {node:?}"),
    };

    // ValuesScan should not have any children.
    //   Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(plan.scan.plan.lefttree.is_none());
    debug_assert!(plan.scan.plan.righttree.is_none());

    // create new ScanState for node:
    //   scanstate = makeNode(ValuesScanState);
    //   scanstate->ss.ps.plan = (Plan *) node;  scanstate->ss.ps.state = estate;
    //   scanstate->ss.ps.ExecProcNode = ExecValuesScan;
    let mut scanstate = alloc_in(mcx, ValuesScanState::new_in(mcx))?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_values_scan_node);

    // Create expression contexts. We need two, one for per-sublist processing
    // and one for execScan.c to use for quals and projections. We cheat a little
    // by using ExecAssignExprContext() to build both.
    //   ExecAssignExprContext(estate, planstate);
    //   scanstate->rowcontext = planstate->ps_ExprContext;
    //   ExecAssignExprContext(estate, planstate);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;
    scanstate.rowcontext = scanstate.ss.ps.ps_ExprContext;
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    // Get info about values list, initialize scan slot with it.
    //   tupdesc = ExecTypeFromExprList((List *) linitial(node->values_lists));
    //   ExecInitScanTupleSlot(estate, &scanstate->ss, tupdesc, &TTSOpsVirtual);
    let tupdesc = {
        let first: &[nodes::primnodes::Expr<'mcx>] = plan
            .values_lists
            .first()
            .map(|l| l.as_slice())
            .unwrap_or(&[]);
        execTuples::exec_type_from_expr_list::call(mcx, first)?
    };
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut scanstate.ss,
        tupdesc,
        TupleSlotKind::Virtual,
    )?;

    // Initialize result type and projection.
    //   ExecInitResultTypeTL(&scanstate->ss.ps);
    //   ExecAssignScanProjectionInfo(&scanstate->ss);
    execTuples::exec_init_result_type_tl::call(&mut scanstate.ss.ps, estate)?;
    execScan::exec_assign_scan_projection_info::call(&mut scanstate.ss, estate)?;

    // initialize child expressions
    //   scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, scanstate);
    {
        let qual = plan.scan.plan.qual.as_deref();
        scanstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut scanstate.ss.ps, estate)?;
    }

    // Other node-specific setup:
    //   scanstate->curr_idx = -1;
    //   scanstate->array_len = list_length(node->values_lists);
    scanstate.curr_idx = -1;
    scanstate.array_len = plan.values_lists.len() as i32;

    // Convert the list of expression sublists into an array for easier
    // addressing at runtime. Also, detect whether any sublists contain SubPlans;
    // for just those sublists, go ahead and do expression initialization. (This
    // avoids problems with SubPlans wanting to connect themselves up to the
    // outer plan tree.)
    //   scanstate->exprlists = (List **) palloc(array_len * sizeof(List *));
    //   scanstate->exprstatelists = (List **) palloc0(array_len * sizeof(List *));
    let array_len = plan.values_lists.len();
    {
        let mut exprlists = mcx::vec_with_capacity_in(mcx, array_len)?;
        let mut exprstatelists = mcx::vec_with_capacity_in(mcx, array_len)?;
        for i in 0..array_len {
            // scanstate->exprlists[i] = exprs;  (a copy of the row's expr list,
            // owned by the node in the per-query context — the plan tree is
            // read-only)
            let src = &plan.values_lists[i];
            let mut row: mcx::PgVec<'mcx, nodes::primnodes::Expr<'mcx>> =
                mcx::vec_with_capacity_in(mcx, src.len())?;
            for e in src.iter() {
                // C aliases the plan's expr list (`exprlists[i] = exprs`); here
                // the node owns its copy in the per-query context. Deep-copy via
                // clone_in — the derived `Expr::clone` panics on an owned-subtree
                // child (a VALUES column may be a SubLink/SubPlan).
                row.push(e.clone_in(mcx)?);
            }
            exprlists.push(row);
            // scanstate->exprstatelists[i] = NULL;  (palloc0 -> empty cell)
            exprstatelists.push(mcx::PgVec::new_in(mcx));
        }
        scanstate.exprlists = exprlists;
        scanstate.exprstatelists = exprstatelists;
    }

    // i = 0; foreach(vtl, node->values_lists) { ... i++; }
    for i in 0..array_len {
        // We can avoid the cost of a contain_subplans() scan in the simple case
        // where there are no SubPlans anywhere.
        //   if (estate->es_subplanstates && contain_subplans((Node *) exprs))
        if !estate.es_subplanstates.is_empty()
            && clauses::contain_subplans::call(scanstate.exprlists[i].as_slice())
        {
            // As these expressions are only used once, disable JIT for them.
            // Note that this doesn't prevent use of JIT *within* a subplan, since
            // that's initialized separately; this just affects the upper-level
            // subexpressions.
            //   saved_jit_flags = estate->es_jit_flags;
            //   estate->es_jit_flags = PGJIT_NONE;
            let saved_jit_flags = estate.es_jit_flags;
            estate.es_jit_flags = PGJIT_NONE;

            //   scanstate->exprstatelists[i] =
            //       ExecInitExprList(exprs, &scanstate->ss.ps);
            // Split-borrow the node: the shared `exprlists` borrow (for the
            // row's `Expr`s) and the mutable `ss` borrow (for `ss.ps`) are
            // disjoint fields, so they coexist without aliasing. Binding each
            // field explicitly makes the disjointness visible to the borrow
            // checker (an indexed expr through the whole struct would not).
            let ValuesScanState {
                exprlists,
                ss,
                exprstatelists,
                ..
            } = &mut *scanstate;
            let exprs = &exprlists[i];
            let mut refs: mcx::PgVec<'mcx, Option<&nodes::primnodes::Expr<'mcx>>> =
                mcx::vec_with_capacity_in(mcx, exprs.len())?;
            for e in exprs.iter() {
                refs.push(Some(e));
            }
            let built = execExpr::exec_init_expr_list::call(&refs, &mut ss.ps, estate)?;
            exprstatelists[i] = built;

            //   estate->es_jit_flags = saved_jit_flags;
            estate.es_jit_flags = saved_jit_flags;
        }
    }

    Ok(scanstate)
}

/// `ExecReScanValuesScan(node)` — rescans the relation.
///
/// ```c
/// void
/// ExecReScanValuesScan(ValuesScanState *node)
/// {
///     if (node->ss.ps.ps_ResultTupleSlot)
///         ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
///     ExecScanReScan(&node->ss);
///     node->curr_idx = -1;
/// }
/// ```
pub fn ExecReScanValuesScan<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(result_slot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, result_slot)?;
    }

    // ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_ss::call(&mut node.ss, estate)?;

    node.curr_idx = -1;
    Ok(())
}

// ===========================================================================
// `execScan.h` driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`),
// inlined into `nodeValuesscan.o` in C; reproduced here as private functions
// (the owned-tree callback ABI cannot be driven generically). Leaf ops go
// through their owners' seams.
// ===========================================================================

/// `ExecScanFetch` — check interrupts & fetch next potential tuple. Returns the
/// slot id of the fetched tuple, or `None` (the C `NULL` / cleared slot).
fn ExecScanFetch<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    epq_active: bool,
    access_mtd: AccessMtd<'mcx>,
    recheck_mtd: RecheckMtd<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    if epq_active {
        // We are inside an EvalPlanQual recheck.
        //   Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;
        let scanrelid = scan_scanrelid(node);

        if scanrelid == 0 {
            // ForeignScan/CustomScan which has pushed down a join.
            //   if (bms_is_member(epqstate->epqParam, node->ps.plan->extParam))
            let epq_param = epq_param(estate);
            let is_member = {
                let ext_param = node
                    .ss
                    .ps
                    .plan
                    .map(|p| p.plan_head())
                    .and_then(|ph| ph.extParam.as_deref());
                bitmapset::bms_is_member::call(epq_param, ext_param)
            };
            if is_member {
                // The recheck method stores the correct tuple in the slot.
                //   TupleTableSlot *slot = node->ss_ScanTupleSlot;
                let slot = node
                    .ss
                    .ss_ScanTupleSlot
                    .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
                //   if (!(*recheckMtd)(node, slot)) ExecClearTuple(slot);
                if !recheck_mtd(node, estate, slot)? {
                    execTuples::exec_clear_tuple::call(estate, slot)?;
                }
                //   return slot;
                return Ok(Some(slot));
            }
        } else if epq_relsubs_done(estate, scanrelid - 1) {
            // Either there is no EPQ tuple for this rel or we already returned
            // it: return ExecClearTuple(slot).
            let slot = node
                .ss
                .ss_ScanTupleSlot
                .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
            execTuples::exec_clear_tuple::call(estate, slot)?;
            return Ok(None);
        } else if let Some(epq_slot) = epq_relsubs_slot(estate, scanrelid - 1) {
            // Return replacement tuple provided by the EPQ caller.
            //   TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];
            //   Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);
            debug_assert!(!epq_relsubs_rowmark_present(estate, scanrelid - 1));
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(epq_slot).is_empty() {
                return Ok(None);
            }
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            if !recheck_mtd(node, estate, epq_slot)? {
                execTuples::exec_clear_tuple::call(estate, epq_slot)?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(epq_slot));
        } else if epq_relsubs_rowmark_present(estate, scanrelid - 1) {
            // Fetch and return replacement tuple using a non-locking rowmark.
            let slot = node
                .ss
                .ss_ScanTupleSlot
                .expect("ExecScanFetch: ss_ScanTupleSlot not initialized");
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);
            //   if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)) return NULL;
            if !execMain::eval_plan_qual_fetch_row_mark::call(estate, scanrelid, slot)? {
                return Ok(None);
            }
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(slot).is_empty() {
                return Ok(None);
            }
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            if !recheck_mtd(node, estate, slot)? {
                execTuples::exec_clear_tuple::call(estate, slot)?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(slot));
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    //   return (*accessMtd)(node);
    access_mtd(node, estate)
}

/// `ExecScanExtended` — the qual/projection scan loop.
fn ExecScanExtended<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    access_mtd: AccessMtd<'mcx>,
    recheck_mtd: RecheckMtd<'mcx>,
    epq_active: bool,
    has_qual: bool,
    has_proj_info: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // ExprContext *econtext = node->ps.ps_ExprContext;  (interrupt checks are in
    // ExecScanFetch)

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        // ResetExprContext(econtext);
        execUtils::reset_per_tuple_expr_context::call(estate, &node.ss.ps)?;
        return ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate);
    }

    // Reset per-tuple memory context to free any expression-evaluation storage
    // allocated in the previous tuple cycle.
    execUtils::reset_per_tuple_expr_context::call(estate, &node.ss.ps)?;

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let slot = ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate)?;

        // If the slot returned by the accessMtd contains NULL, there is nothing
        // more to scan, so return an empty slot --- being careful to use the
        // projection result slot so it has the correct tupleDesc.
        let Some(slot) = slot else {
            if has_proj_info {
                // return ExecClearTuple(projInfo->pi_state.resultslot);
                let result_slot = node
                    .ss
                    .ps
                    .ps_ResultTupleSlot
                    .expect("ExecScanExtended: ps_ResultTupleSlot not initialized");
                execTuples::exec_clear_tuple::call(estate, result_slot)?;
                return Ok(Some(result_slot));
            } else {
                return Ok(None);
            }
        };

        // Place the current tuple into the expr context.
        //   econtext->ecxt_scantuple = slot;
        if let Some(ecxt) = node.ss.ps.ps_ExprContext {
            if let Some(Some(ec)) = estate.es_exprcontexts.get_mut(ecxt.0 as usize) {
                ec.ecxt_scantuple = Some(slot);
            }
        }

        // Check that the current tuple satisfies the qual-clause.
        //   if (qual == NULL || ExecQual(qual, econtext))
        let passes = if !has_qual {
            true
        } else {
            match (node.ss.ps.qual.as_deref_mut(), node.ss.ps.ps_ExprContext) {
                (Some(state), Some(econtext)) => execExpr::exec_qual::call(state, econtext, estate)?,
                _ => true,
            }
        };
        if passes {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result slot, return it.
                //   return ExecProject(projInfo);
                return Ok(Some(execExpr::exec_project::call(&mut node.ss.ps, estate)?));
            } else {
                // Not projecting, so just return the scan tuple.
                return Ok(Some(slot));
            }
        }
        // else InstrCountFiltered1(node, 1);  (instrumentation arrives with its
        // consumer; the count does not affect control flow.)

        // Tuple fails qual, so free per-tuple memory and try again.
        execUtils::reset_per_tuple_expr_context::call(estate, &node.ss.ps)?;
    }
}

/// `ExecScan(node, accessMtd, recheckMtd)` — the generic scan driver.
fn ExecScan<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    access_mtd: AccessMtd<'mcx>,
    recheck_mtd: RecheckMtd<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // epqstate = node->ps.state->es_epq_active;
    // qual = node->ps.qual; projInfo = node->ps.ps_ProjInfo;
    let epq_active = estate.es_epq_active.is_some();
    let has_qual = node.ss.ps.qual.is_some();
    let has_proj_info = node.ss.ps.ps_ProjInfo.is_some();
    ExecScanExtended(
        node,
        access_mtd,
        recheck_mtd,
        epq_active,
        has_qual,
        has_proj_info,
        estate,
    )
}

// ===========================================================================
// In-crate helpers: owned-data reads off the EState/plan.
// ===========================================================================

/// `((Scan *) node->ss.ps.plan)->scanrelid`, read from the node's owned plan
/// view.
#[inline]
fn scan_scanrelid(node: &ValuesScanState<'_>) -> u32 {
    match node.ss.ps.plan {
        Some(p) if p.is_valuesscan() => p.expect_valuesscan().scan.scanrelid,
        Some(other) => panic!("ValuesScanState.plan is not a ValuesScan: {other:?}"),
        None => panic!("ValuesScanState.plan is not set"),
    }
}

/// `epqstate->epqParam`.
#[inline]
fn epq_param(estate: &EStateData<'_>) -> i32 {
    estate
        .es_epq_active
        .as_deref()
        .map(|e| e.epqParam)
        .expect("epq_param: es_epq_active not set")
}

/// `epqstate->relsubs_done[idx]`.
#[inline]
fn epq_relsubs_done(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_done.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `epqstate->relsubs_done[idx] = value`.
#[inline]
fn epq_set_relsubs_done(estate: &mut EStateData<'_>, idx: u32, value: bool) {
    if let Some(e) = estate.es_epq_active.as_deref_mut() {
        if let Some(v) = e.relsubs_done.as_mut() {
            if let Some(slot) = v.get_mut(idx as usize) {
                *slot = value;
            }
        }
    }
}

/// `epqstate->relsubs_slot[idx]` (`Some` = a non-NULL C entry).
#[inline]
fn epq_relsubs_slot(estate: &EStateData<'_>, idx: u32) -> Option<SlotId> {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_slot.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .flatten()
}

/// `epqstate->relsubs_rowmark[idx] != NULL`.
#[inline]
fn epq_relsubs_rowmark_present(estate: &EStateData<'_>, idx: u32) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_rowmark.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}
