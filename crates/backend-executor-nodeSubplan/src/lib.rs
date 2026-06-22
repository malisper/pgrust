//! Port of `executor/nodeSubplan.c` — routines to support sub-selects appearing
//! in expressions.
//!
//! This module executes `SubPlan` expression nodes (not sub-SELECTs in FROM).
//! SubPlans split into "initplans" (one evaluation per query) and "regular"
//! subplans (re-evaluated whenever their result is needed).
//!
//! The crate owns the control flow ported 1:1 from PostgreSQL 18.3. The
//! subsystems it reaches into are not all ported, so their operations go
//! through per-owner seam crates (they panic until the owner lands, which is
//! correct):
//!
//! - the child subselect plan's `ExecProcNode`/`ExecReScan` (execProcnode /
//!   execAmi),
//! - the compiled expression states and projections, and the hashed-subplan
//!   setup (execExpr),
//! - the `TupleHashTable`s and their probing (execGrouping),
//! - the `ArrayBuildStateAny` accumulation (arrayfuncs),
//! - `clamp_cardinality_to_long` (costsize), the `Bitmapset` ops (nodes-core),
//!   `CHECK_FOR_INTERRUPTS` (tcop).
//!
//! The owned-executor model threads `&mut EStateData` and the expression
//! context `EcxtId` explicitly (in place of C's `PlanState.state` /
//! `ExprContext` back-pointers); `ParamExecData` reads/writes and the scan
//! direction are taken directly off the `EState`, per the execnodes vocabulary.

#![allow(non_snake_case)]
// `PgError` is a large enum carried by value in `PgResult`, matching the other
// executor-node crates.
#![allow(clippy::result_large_err)]
// The NULL-result cascade in `ExecHashSubPlan` mirrors the C `if`/`else if`
// chain literally: each branch tests a different condition but several share the
// `isNull = true` action, and the short-circuit order is significant.
#![allow(clippy::if_same_then_else)]

extern crate alloc;

use backend_executor_execAmi_seams as exec_ami;
use backend_executor_execExpr_seams as exec_expr;
use backend_executor_execGrouping_seams as exec_grouping;
use backend_executor_execProcnode_seams as exec_procnode;
// The PARAM_EXEC `execPlan`-link plumbing seams live under their real owner
// (execMain): they operate on the executor-owned `es_param_exec_vals` /
// `es_subplanstates`, not on any execProcnode.c function.
use backend_executor_execMain_seams as exec_main;
use backend_executor_execTuples_seams as exec_tuples;
use backend_executor_execUtils_seams as executils;
use backend_access_common_tupdesc_seams as tupdesc;
use backend_nodes_core_seams as bms;
use backend_optimizer_path_costsize_seams as costsize;
use backend_tcop_postgres_seams as tcop;
use backend_utils_adt_arrayfuncs_seams as arrayfuncs;
use backend_utils_fmgr_fmgr_seams as fmgr;

use exec_expr::ProjectionKind;
use mcx::{vec_with_capacity_in, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::{AttrNumber, Oid};
// The fmgr-call ABI / array-seam contracts still hand and return the bare-word
// `Datum` newtype at their (un-migrated owner) edges: `FunctionCall2Coll`
// (fmgr), and `accum_array_result_any` / `make_array_result_any` /
// `pfree_array_datum` (arrayfuncs). Those words enter/leave via the canonical
// value's by-value arm; this crate touches `types_datum::Datum` ONLY to bridge
// across those still-word seams (see `word`/`from_word`). Everything internal
// — the `result` register, the per-param values, `curArray` — is the canonical
// unified value.
use types_datum::Datum as Word;
// The canonical unified value type (Datum-unification keystone): what the
// `result` register, `ParamExecData.value`, `SubPlanState.curArray`, and the
// migrated seam returns all carry.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_INTERNAL_ERROR};
use types_nodes::execexpr::SubPlanState;
use types_nodes::primnodes::SubLinkType;
use types_nodes::{Bitmapset, EStateData, EcxtId};

type Astate<'mcx> = arrayfuncs::ArrayBuildStateAnyHandle<'mcx>;
type HashTable<'mcx> = types_nodes::nodeagg::TupleHashTable<'mcx>;

// ===========================================================================
// postgres.h inline helpers (pure node-layer).
// ===========================================================================

/// `BoolGetDatum(X)` (postgres.h) — onto the canonical value's by-value arm.
#[inline]
fn BoolGetDatum<'mcx>(x: bool) -> Datum<'mcx> {
    Datum::from_bool(x)
}

/// `DatumGetBool(X)` (postgres.h) — `(X) != 0`.
#[inline]
fn DatumGetBool(x: &Datum<'_>) -> bool {
    x.as_bool()
}

/// Project a canonical scalar value onto the bare machine word
/// (`types_datum::Datum`) for the still-bare-word array/fmgr seams
/// (`accum_array_result_any` `dvalue`, `function_call2_coll` args). The columns
/// fed to those edges are scalar (by-value) words; a by-reference value would
/// `panic` here exactly as the C would misread a pointer image as a scalar —
/// the canonical-carrier follow-on (#113) is what lets these owners take a
/// by-reference value, and is recorded as the remaining bare-word edge.
#[inline]
fn word(d: &Datum<'_>) -> Word {
    Word::from_usize(d.as_usize())
}

/// Carry a bare machine word returned by a still-word seam
/// (`make_array_result_any`) into the canonical value's by-value arm.
#[inline]
fn from_word<'mcx>(w: Word) -> Datum<'mcx> {
    Datum::from_usize(w.as_usize())
}

// ===========================================================================
// ExecSubPlan — main entry point for a regular SubPlan.
// ===========================================================================

/// `ExecSubPlan(node, econtext, isNull)` — process a sub-select. The boolean
/// `isNull` out-parameter is returned alongside the result `Datum` as
/// `(Datum, bool)`.
pub fn ExecSubPlan<'mcx>(
    node: &mut SubPlanState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let subplan = subplan_ref(node)?;
    let subLinkType = subplan.subLinkType;
    let has_setparam = !subplan.setParam.is_empty();
    let useHashTable = subplan.useHashTable;

    // EState   *estate = node->planstate->state;
    // ScanDirection dir = estate->es_direction;
    let dir = estate.es_direction;

    tcop::check_for_interrupts::call()?;

    // Set non-null as default
    let mut isNull = false;

    // Sanity checks
    if subLinkType == SubLinkType::Cte {
        return Err(elog_internal("CTE subplans should not be executed via ExecSubPlan"));
    }
    if has_setparam && subLinkType != SubLinkType::MultiExpr {
        return Err(elog_internal("cannot set parent params from subquery"));
    }

    // Force forward-scan mode for evaluation
    estate.es_direction = types_nodes::ScanDirection::ForwardScanDirection;

    // Select appropriate evaluation strategy
    let retval = if useHashTable {
        ExecHashSubPlan(node, econtext, estate, &mut isNull)
    } else {
        ExecScanSubPlan(node, econtext, estate, &mut isNull)
    };

    // restore scan direction
    estate.es_direction = dir;

    // The internal `result` register is already the canonical unified value
    // (scalar results on the by-value arm; an ARRAY result carries the array
    // word on the by-value arm too, matching the C `Datum`).
    let result = retval?;
    Ok((result, isNull))
}

/// `ExecHashSubPlan` — store subselect result in an in-memory hash table.
fn ExecHashSubPlan<'mcx>(
    node: &mut SubPlanState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
    isNull: &mut bool,
) -> PgResult<Datum<'mcx>> {
    let mut result = false;

    let subplan = subplan_ref(node)?;
    // Shouldn't have any direct correlation Vars
    if !subplan.parParam.is_empty() || !subplan.args.is_empty() {
        return Err(elog_internal("hashed subplan with direct correlation not supported"));
    }

    // If first time through or we need to rescan the subplan, build the hash
    // table.  (node->hashtable == NULL || planstate->chgParam != NULL)
    let need_build = node.hashtable.is_none() || planstate_chgparam_set(node, estate);
    if need_build {
        buildSubPlanHash(node, econtext, estate)?;
    }

    // The result for an empty subplan is always FALSE; no need to evaluate
    // lefthand side.
    *isNull = false;
    if !node.havehashrows && !node.havenullrows {
        return Ok(BoolGetDatum(false));
    }

    // Evaluate lefthand expressions and form a projection tuple. First we have
    // to set the econtext to use (hack alert!):
    //   node->projLeft->pi_exprContext = econtext;
    //   slot = ExecProject(node->projLeft);
    exec_expr::sub_exec_project::call(node, estate, econtext, ProjectionKind::Left)?;

    let havehashrows = node.havehashrows;
    let havenullrows = node.havenullrows;
    let has_hashnulls = node.hashnulls.is_some();

    // If the LHS is all non-null, probe for an exact match in the main hash
    // table.  If we find one, TRUE. Otherwise scan the partly-null table for an
    // UNKNOWN; otherwise FALSE.
    if slotNoNulls(node, estate, ProjectionKind::Left)? {
        if havehashrows && find_tuple_hash_entry_main(node, estate)? {
            result = true;
        } else if havenullrows && findPartialMatch(node, estate, true)? {
            *isNull = true;
        }
    }
    // When the LHS is partly or wholly NULL, we can never return TRUE.
    else if !has_hashnulls {
        // just return FALSE
    } else if slotAllNulls(node, estate, ProjectionKind::Left)? {
        *isNull = true;
    }
    // Scan partly-null table first, since more likely to get a match.
    else if havenullrows && findPartialMatch(node, estate, true)? {
        *isNull = true;
    } else if havehashrows && findPartialMatch(node, estate, false)? {
        *isNull = true;
    }

    // Explicitly clear the projected tuple before returning (per-tuple context
    // double-free guard).
    exec_expr::sub_clear_proj_result_slot::call(node, estate, ProjectionKind::Left)?;

    // Also must reset the hashtempcxt after each hashtable lookup.
    if let Some(c) = node.hashtempcxt.as_mut() {
        c.reset();
    }

    Ok(BoolGetDatum(result))
}

/// `ExecScanSubPlan` — default case where we have to rescan the subplan each
/// time.
fn ExecScanSubPlan<'mcx>(
    node: &mut SubPlanState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
    isNull: &mut bool,
) -> PgResult<Datum<'mcx>> {
    let subplan = subplan_ref(node)?;
    let subLinkType = subplan.subLinkType;
    let firstColType = subplan.firstColType;
    let mut found = false; // true if got at least one subplan tuple
    let mut astate: Astate<'mcx> = None;

    // Initialize ArrayBuildStateAny in caller's context, if needed.
    //   astate = initArrayResultAny(subplan->firstColType,
    //                               CurrentMemoryContext, true);
    // `CurrentMemoryContext` at entry to ExecScanSubPlan — which is invoked
    // from expression evaluation — is `econtext->ecxt_per_tuple_memory`, the
    // short-lived per-tuple eval context. The whole array (accumulator and the
    // final `makeArrayResultAny`) is built there (C `oldcontext`), so the result
    // lives only until the caller resets that context on the next outer tuple.
    if subLinkType == SubLinkType::Array {
        astate = arrayfuncs::init_array_result_any::call(
            estate,
            econtext,
            arrayfuncs::ArrayBuildCtx::PerTuple,
            firstColType,
        )?;
    }

    // We are probably in a short-lived expression-evaluation context. The
    // per-query context is where the child plan's chgParam / ExecProcNode work
    // happens; in the owned model that allocator is `estate.es_query_cxt`.
    let per_query = estate.es_query_cxt;

    // We rely on the caller to evaluate plan correlation values; we still record
    // that the values (might have) changed, else ExecReScan() below won't know
    // nodes need rescanning.
    //   foreach(l, subplan->parParam)
    //       planstate->chgParam = bms_add_member(planstate->chgParam, paramid);
    let parParam = clone_int_list(estate.es_query_cxt, &subplan_ref(node)?.parParam)?;
    for paramid in parParam {
        let ps = planstate_head_mut(node, estate)?;
        let old = ps.chgParam.take();
        ps.chgParam = Some(bms::bms_add_member::call(per_query, old, paramid)?);
    }

    // with that done, we can reset the subplan
    exec_re_scan_child(node, estate)?;

    // For all sublink types except EXPR and ARRAY, the result is boolean. We
    // combine across tuples with OR (ANY) or AND (ALL) semantics. The result for
    // no input tuples is FALSE for ANY, TRUE for ALL, NULL for ROWCOMPARE.
    let mut result = BoolGetDatum(subLinkType == SubLinkType::All);
    *isNull = false;

    let paramIds = clone_int_list(estate.es_query_cxt, &subplan_ref(node)?.paramIds)?;
    let setParam = clone_int_list(estate.es_query_cxt, &subplan_ref(node)?.setParam)?;

    while let Some(slot) = exec_proc_node_child(node, estate)? {
        if subLinkType == SubLinkType::Exists {
            found = true;
            result = BoolGetDatum(true);
            break;
        }

        if subLinkType == SubLinkType::Expr {
            // cannot allow multiple input tuples for EXPR sublink
            if found {
                return Err(cardinality_violation());
            }
            found = true;

            // Copy the subplan's tuple in case the result is pass-by-ref;
            // node->curTuple keeps it for eventual freeing.
            exec_tuples::replace_cur_tuple_from_slot::call(node, estate, slot)?;

            // result = heap_getattr(node->curTuple, 1, tdesc, isNull);
            let attr = exec_tuples::cur_tuple_getattr::call(node, estate, slot, 1)?;
            result = attr.value;
            *isNull = attr.isnull;
            // keep scanning subplan to make sure there's only one tuple
            continue;
        }

        if subLinkType == SubLinkType::MultiExpr {
            // cannot allow multiple input tuples for MULTIEXPR sublink
            if found {
                return Err(cardinality_violation());
            }
            found = true;

            exec_tuples::replace_cur_tuple_from_slot::call(node, estate, slot)?;

            // Now set all the setParam params from the columns of the tuple.
            let mut col: AttrNumber = 1;
            for &paramid in setParam.iter() {
                let attr = exec_tuples::cur_tuple_getattr::call(node, estate, slot, col)?;
                set_exec_param(estate, paramid, attr.value, attr.isnull)?;
                col += 1;
            }
            // keep scanning subplan to make sure there's only one tuple
            continue;
        }

        if subLinkType == SubLinkType::Array {
            found = true;
            // stash away current value: dvalue = slot_getattr(slot, 1, &disnull)
            let attr = exec_tuples::slot_getattr_by_id::call(estate, slot, 1)?;
            //   astate = accumArrayResultAny(astate, dvalue, disnull,
            //                                subplan->firstColType, oldcontext);
            // `oldcontext` is the entry-time per-tuple eval context.
            astate = arrayfuncs::accum_array_result_any::call(
                estate,
                econtext,
                arrayfuncs::ArrayBuildCtx::PerTuple,
                astate,
                attr.value,
                attr.isnull,
                firstColType,
            )?;
            // keep scanning subplan to collect all values
            continue;
        }

        // cannot allow multiple input tuples for ROWCOMPARE sublink either
        if subLinkType == SubLinkType::RowCompare && found {
            return Err(cardinality_violation());
        }

        found = true;

        // For ALL, ANY, ROWCOMPARE: load the Params representing the sub-select
        // columns, then evaluate the combining expression.
        let mut col: AttrNumber = 1;
        for &paramid in paramIds.iter() {
            let attr = exec_tuples::slot_getattr_by_id::call(estate, slot, col)?;
            set_exec_param(estate, paramid, attr.value, attr.isnull)?;
            col += 1;
        }

        // The testexpr seam already returns the canonical unified value.
        let (rowresult, rownull) =
            exec_expr::eval_testexpr_switch_context::call(node, estate, econtext)?;

        if subLinkType == SubLinkType::Any {
            // combine across rows per OR semantics
            if rownull {
                *isNull = true;
            } else if DatumGetBool(&rowresult) {
                result = BoolGetDatum(true);
                *isNull = false;
                break; // needn't look at any more rows
            }
        } else if subLinkType == SubLinkType::All {
            // combine across rows per AND semantics
            if rownull {
                *isNull = true;
            } else if !DatumGetBool(&rowresult) {
                result = BoolGetDatum(false);
                *isNull = false;
                break; // needn't look at any more rows
            }
        } else {
            // must be ROWCOMPARE_SUBLINK
            result = rowresult;
            *isNull = rownull;
        }
    }

    if subLinkType == SubLinkType::Array {
        // We return the result in the caller's context.
        //   result = makeArrayResultAny(astate, oldcontext, true);
        // C builds the final array varlena in `oldcontext` (the entry-time
        // per-tuple eval context) and relies on the caller's bulk
        // per-tuple-context reset to free it on the next outer tuple. In the
        // owned model that reset asserts every allocation it owns has already
        // been dropped, so a varlena that must survive into the caller's
        // projection (i.e. past the next reset) cannot live in per-tuple
        // memory. We therefore build the result array in the per-query context
        // — a longer-lived allocation reclaimed at query end — exactly as the
        // ARRAY[] constructor (execExprInterp eval_array) and array subscripting
        // already do for the same reason. The transient `astate` accumulator
        // stays in per-tuple memory (built/dropped within this call, so its
        // charge balances before any reset). An array is pass-by-reference, so
        // the unified `_v` seam hands back a `Datum::ByRef` carrying the array
        // bytes — the form that rides the by-ref fmgr lane of any downstream
        // array function.
        //
        // Companion fix (this commit): the *element* copies the per-tuple
        // accumulator makes for a pass-by-ref element type (e.g. text[]) must
        // likewise not be charged to the caller's per-tuple context, else its
        // reset asserts a still-charged leak. accumArrayResult now keeps those
        // copies in the build state's own `byref_storage` (arrayfuncs), mirroring
        // C's private-subcontext datumCopy.
        result = arrayfuncs::make_array_result_any_v::call(
            estate,
            econtext,
            arrayfuncs::ArrayBuildCtx::PerQuery,
            astate,
        )?;
    } else if !found {
        // deal with empty subplan result.  result/isNull were previously
        // initialized correctly for all sublink types except EXPR and
        // ROWCOMPARE; for those, return NULL.
        if subLinkType == SubLinkType::Expr || subLinkType == SubLinkType::RowCompare {
            result = Datum::null();
            *isNull = true;
        } else if subLinkType == SubLinkType::MultiExpr {
            // We don't care about function result, but set the setParams.
            for &paramid in setParam.iter() {
                set_exec_param(estate, paramid, Datum::null(), true)?;
            }
        }
    }

    Ok(result)
}

/// `buildSubPlanHash` — load hash table by scanning subplan output.
fn buildSubPlanHash<'mcx>(
    node: &mut SubPlanState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let subplan = subplan_ref(node)?;
    debug_assert!(subplan.subLinkType == SubLinkType::Any);
    let ncols = node.numCols;
    let unknownEqFalse = subplan.unknownEqFalse;

    // If we already had any hash tables, reset 'em; otherwise create empty hash
    // table(s).  The input slot for each hash table is the ExecProject() result,
    // so we use TTSOpsVirtual for the input ops.
    if let Some(c) = node.hashtablecxt.as_mut() {
        c.reset();
    }
    node.havehashrows = false;
    node.havenullrows = false;

    // nbuckets = clamp_cardinality_to_long(planstate->plan->plan_rows);
    let plan_rows = planstate_plan_rows(node, estate)?;
    let mut nbuckets = costsize::clamp_cardinality_to_long::call(plan_rows);
    if nbuckets < 1 {
        nbuckets = 1;
    }

    if node.hashtable.is_some() {
        exec_grouping::reset_tuple_hash_table::call(node.hashtable.as_mut().unwrap())?;
    } else {
        let ht = build_subplan_hash_table(node, estate, nbuckets)?;
        node.hashtable = Some(ht);
    }

    if !unknownEqFalse {
        if ncols == 1 {
            nbuckets = 1; // there can only be one entry
        } else {
            nbuckets /= 16;
            if nbuckets < 1 {
                nbuckets = 1;
            }
        }

        if node.hashnulls.is_some() {
            exec_grouping::reset_tuple_hash_table::call(node.hashnulls.as_mut().unwrap())?;
        } else {
            let ht = build_subplan_hash_table(node, estate, nbuckets)?;
            node.hashnulls = Some(ht);
        }
    } else {
        node.hashnulls = None;
    }

    // The C switches to the per-query context (econtext->ecxt_per_query_memory
    // == estate.es_query_cxt) for the child-plan manipulation below; the owned
    // model has no ambient current context, and each operation takes its own
    // allocator/estate, so no switch is needed.

    // Reset subplan to start.
    exec_re_scan_child(node, estate)?;

    let paramIds = clone_int_list(estate.es_query_cxt, &subplan_ref(node)?.paramIds)?;

    // Scan the subplan and load the hash table(s).  Duplicate rows are stored
    // only once.
    while let Some(slot) = exec_proc_node_child(node, estate)? {
        // Load up the Params representing the raw sub-select outputs, then form
        // the projection tuple to store in the hashtable.
        //   prmdata = &(innerecontext->ecxt_param_exec_vals[paramid]);
        //   prmdata->value = slot_getattr(slot, col, &(prmdata->isnull));
        let mut col: AttrNumber = 1;
        for &paramid in paramIds.iter() {
            let attr = exec_tuples::slot_getattr_by_id::call(estate, slot, col)?;
            set_exec_param(estate, paramid, attr.value, attr.isnull)?;
            col += 1;
        }
        // slot = ExecProject(node->projRight);
        exec_expr::sub_exec_project::call(node, estate, econtext, ProjectionKind::Right)?;

        // If result contains any nulls, store separately or not at all.
        if slotNoNulls(node, estate, ProjectionKind::Right)? {
            let slot = exec_expr::sub_proj_result_slot_id::call(node, estate, ProjectionKind::Right);
            exec_grouping::lookup_tuple_hash_entry::call(
                node.hashtable.as_mut().unwrap(),
                slot,
                estate,
                &mut |_e, _add| {},
            )?;
            node.havehashrows = true;
        } else if node.hashnulls.is_some() {
            let slot = exec_expr::sub_proj_result_slot_id::call(node, estate, ProjectionKind::Right);
            exec_grouping::lookup_tuple_hash_entry::call(
                node.hashnulls.as_mut().unwrap(),
                slot,
                estate,
                &mut |_e, _add| {},
            )?;
            node.havenullrows = true;
        }

        // Reset innerecontext after each inner tuple to free ExecProject memory.
        reset_inner_expr_context(node, estate)?;

        // Also must reset the hashtempcxt after each hashtable lookup.
        if let Some(c) = node.hashtempcxt.as_mut() {
            c.reset();
        }
    }

    // Clear the projRight result slot before any chance of a sub-query context
    // reset (double-free guard).
    exec_expr::sub_clear_proj_result_slot::call(node, estate, ProjectionKind::Right)?;

    Ok(())
}

/// `BuildTupleHashTable(node->parent, node->descRight, &TTSOpsVirtual, ncols,
/// node->keyColIdx, node->tab_eq_funcoids, node->tab_hash_funcs,
/// node->tab_collations, nbuckets, 0, node->planstate->state->es_query_cxt,
/// node->hashtablecxt, node->hashtempcxt, false)` (buildSubPlanHash). Both the
/// main and the nulls table are built with identical arguments (only `nbuckets`
/// differs), so the call is factored here.
///
/// The input descriptor is `node->descRight`; the C `BuildTupleHashTable` makes
/// its own `CreateTupleDescCopy`, so we hand it a fresh copy and keep the node's
/// `descRight` intact for the second build.
fn build_subplan_hash_table<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    nbuckets: i64,
) -> PgResult<alloc::boxed::Box<HashTable<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let ncols = node.numCols;

    // node->descRight (input row type); copy it for the table to own.
    let desc_src = node
        .descRight
        .as_deref()
        .ok_or_else(|| elog_internal("subplan descRight is NULL"))?;
    let input_desc = Some(tupdesc::create_tupledesc_copy::call(mcx, desc_src)?);

    let key_col_idx = node.keyColIdx.as_deref().unwrap_or(&[]);
    let tab_eq_funcoids = node.tab_eq_funcoids.as_deref().unwrap_or(&[]);
    let tab_hash_funcs = node.tab_hash_funcs.as_deref().unwrap_or(&[]);
    let tab_collations = node.tab_collations.as_deref().unwrap_or(&[]);

    // C: BuildTupleHashTable(node->parent, ..., es_query_cxt /*metacxt*/,
    //    node->hashtablecxt /*tablecxt*/, node->hashtempcxt /*tempcxt*/, false).
    // The seam borrows these three contexts (matching C's pointer aliasing): the
    // node keeps owning `hashtablecxt`/`hashtempcxt` and resets them itself, and
    // both the main and the nulls table alias the SAME caller-owned pair.
    //   metacxt  = &estate->es_query_cxt's MemoryContext
    //   tablecxt = &node->hashtablecxt
    //   tempcxt  = &node->hashtempcxt
    let metacxt = estate.es_query_cxt.context();
    let tablecxt = node
        .hashtablecxt
        .as_ref()
        .ok_or_else(|| elog_internal("subplan hashtablecxt is NULL"))?;
    let tempcxt = node
        .hashtempcxt
        .as_ref()
        .ok_or_else(|| elog_internal("subplan hashtempcxt is NULL"))?;

    exec_grouping::build_tuple_hash_table::call(
        mcx,
        None, // C node->parent (PlanState) is not carried in the owned model.
        input_desc,
        types_nodes::TupleSlotKind::Virtual,
        ncols,
        key_col_idx,
        tab_eq_funcoids,
        tab_hash_funcs,
        tab_collations,
        nbuckets,
        0,
        metacxt,
        tablecxt,
        tempcxt,
        false,
    )
}

/// `execTuplesUnequal` — return true if two tuples are definitely unequal in the
/// indicated fields (nodeSubplan.c:657-713).
///
/// Nulls are neither equal nor unequal to anything else. A true result is
/// obtained only if there are non-null fields that compare not-equal. We compare
/// last-to-first (least-significant sort key first), which is most likely to
/// differ for sorted input, and can report a non-match as soon as one is found.
fn execTuplesUnequal<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    is_nulls: bool,
    num_cols: i32,
) -> PgResult<bool> {
    // The hashtable whose tableslot holds the entry being compared (slot2 in
    // the C) and whose keyColIdx/tab_collations/tempcxt drive the comparison.
    let ht = subplan_hash_table_mut(node, is_nulls);

    // Reset (and conceptually switch into) the table's temp/eval context
    // (`hashtable->tempcxt`).
    if let Some(c) = ht.tempcxt.as_mut() {
        c.reset();
    }
    // hashtable->tableslot — where findPartialMatch stored the scanned entry.
    let tableslot = ht
        .tableslot
        .ok_or_else(|| elog_internal("subplan hashtable has no tableslot"))?;

    let mut result = false;

    // for (i = numCols; --i >= 0;)
    let mut i = num_cols;
    loop {
        i -= 1;
        if i < 0 {
            break;
        }

        // att = matchColIdx[i] (= hashtable->keyColIdx[i]).
        let att = subplan_hash_table_ref(node, is_nulls).keyColIdx.as_ref().unwrap()[i as usize];

        let attr1 = exec_expr::proj_left_slot_getattr::call(node, estate, att)?;
        if attr1.isnull {
            continue; // can't prove anything here
        }

        // attr2 = slot_getattr(hashtable->tableslot, att, &isNull2);
        let attr2 = exec_tuples::slot_getattr_by_id::call(estate, tableslot, att)?;
        if attr2.isnull {
            continue; // can't prove anything here
        }

        // Apply the type-specific equality function.
        //   FunctionCall2Coll(&eqfunctions[i], collations[i], attr1, attr2)
        // eqfunctions are node->cur_eq_funcs (the caller-provided cross-type
        // functions); collations are hashtable->tab_collations.
        let collation = subplan_hash_table_ref(node, is_nulls).tab_collations.as_ref().unwrap()[i as usize];
        let fn_oid = node.cur_eq_funcs.as_ref().unwrap()[i as usize].fn_oid;
        // FunctionCall2Coll(&eqfunctions[i], collations[i], attr1, attr2). The
        // equality function's arguments can be by-reference (e.g. text), so they
        // must cross the fmgr boundary as canonical Datums (not bare words);
        // bare-word extraction would mis-read a varlena pointer as a scalar.
        let per_query = estate.es_query_cxt;
        if !DatumGetBool(&fmgr::function_call2_coll_datum::call(
            per_query,
            fn_oid,
            collation,
            attr1.value.clone(),
            attr2.value.clone(),
        )?) {
            result = true; // they are unequal
            break;
        }
    }

    Ok(result)
}

/// `findPartialMatch` — does the hashtable contain an entry that is not provably
/// distinct from the tuple? (nodeSubplan.c:726-753).
///
/// We must scan the whole hashtable; hashkeys can't guide probing because
/// partial matches can occur on unrelated hashkeys.
fn findPartialMatch<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    is_nulls: bool,
) -> PgResult<bool> {
    // int numCols = hashtable->numCols;
    let num_cols = subplan_hash_table_ref(node, is_nulls).numCols;

    // InitTupleHashIterator(hashtable, &hashiter);
    let iter = exec_grouping::init_tuple_hash_iterator::call(subplan_hash_table_mut(node, is_nulls));
    node.hashiter = iter;

    loop {
        // entry = ScanTupleHashTable(...); if NULL break;
        // ExecStoreMinimalTuple(TupleHashEntryGetTuple(entry), tableslot, false)
        // is folded into scan_tuple_hash_table, which returns whether an entry
        // was produced (and, if so, stored its tuple in the table's tableslot).
        let mut iter = node.hashiter;
        let produced = exec_grouping::scan_tuple_hash_table::call(
            subplan_hash_table_mut(node, is_nulls),
            &mut iter,
            estate,
            &mut |_e, _add| {},
        )?;
        node.hashiter = iter;
        if !produced {
            break;
        }

        tcop::check_for_interrupts::call()?;

        if !execTuplesUnequal(node, estate, is_nulls, num_cols)? {
            exec_grouping::term_tuple_hash_iterator::call(&mut node.hashiter);
            return Ok(true);
        }
    }
    // No TermTupleHashIterator call needed here.
    Ok(false)
}

/// `FindTupleHashEntry(node->hashtable, slot, node->cur_eq_comp,
/// node->lhs_hash_expr) != NULL` (ExecHashSubPlan) — probe the main table for an
/// exact match of the projected LHS tuple.
fn find_tuple_hash_entry_main<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // slot = projLeft result slot; the canonical seam takes its SlotId.
    let slot = exec_expr::sub_proj_result_slot_id::call(node, estate, ProjectionKind::Left);
    // Borrow the table mutably alongside the two compiled ExprStates; these are
    // disjoint fields, so destructure to take simultaneous disjoint borrows.
    let SubPlanState { hashtable, cur_eq_comp, lhs_hash_expr, .. } = node;
    let ht = hashtable.as_mut().unwrap();
    let eqcomp = cur_eq_comp.as_deref_mut().expect("SubPlanState cur_eq_comp not built");
    let hashexpr = lhs_hash_expr.as_deref_mut().expect("SubPlanState lhs_hash_expr not built");
    exec_grouping::find_tuple_hash_entry::call(ht, slot, eqcomp, hashexpr, estate)
}

/// `&mut *node->hashtable` or `&mut *node->hashnulls`, selected by `is_nulls`.
#[inline]
fn subplan_hash_table_mut<'a, 'mcx>(
    node: &'a mut SubPlanState<'mcx>,
    is_nulls: bool,
) -> &'a mut HashTable<'mcx> {
    if is_nulls {
        node.hashnulls.as_mut().unwrap()
    } else {
        node.hashtable.as_mut().unwrap()
    }
}

/// `&*node->hashtable` or `&*node->hashnulls`, selected by `is_nulls`.
#[inline]
fn subplan_hash_table_ref<'a, 'mcx>(
    node: &'a SubPlanState<'mcx>,
    is_nulls: bool,
) -> &'a HashTable<'mcx> {
    if is_nulls {
        node.hashnulls.as_ref().unwrap()
    } else {
        node.hashtable.as_ref().unwrap()
    }
}

/// `slotAllNulls` — is the (projection result) slot completely NULL?
fn slotAllNulls<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: ProjectionKind,
) -> PgResult<bool> {
    let ncols = exec_expr::proj_result_slot_natts::call(node, estate, which);
    let mut i = 1;
    while i <= ncols {
        if !exec_expr::proj_result_slot_attisnull::call(node, estate, which, i)? {
            return Ok(false);
        }
        i += 1;
    }
    Ok(true)
}

/// `slotNoNulls` — is the (projection result) slot entirely not NULL?
fn slotNoNulls<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    which: ProjectionKind,
) -> PgResult<bool> {
    let ncols = exec_expr::proj_result_slot_natts::call(node, estate, which);
    let mut i = 1;
    while i <= ncols {
        if exec_expr::proj_result_slot_attisnull::call(node, estate, which, i)? {
            return Ok(false);
        }
        i += 1;
    }
    Ok(true)
}

// ===========================================================================
// ExecInitSubPlan
// ===========================================================================

/// `ExecInitSubPlan(subplan, parent)` — create a `SubPlanState` for a `SubPlan`.
///
/// This is the SubPlan-specific part of `ExecInitExpr()`. The node is built and
/// linked here; the executor-owned `es_subplanstates` linkage and the parent
/// back-reference are resolved through seams (the owned model threads the parent
/// state and estate explicitly).
pub fn ExecInitSubPlan<'mcx>(
    subplan: PgBox<'mcx, types_nodes::primnodes::SubPlan<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SubPlanState<'mcx>> {
    // sstate = makeNode(SubPlanState); sstate->subplan = subplan;
    let mut sstate = SubPlanState::default();
    let plan_id = subplan.plan_id;
    let has_setparam = !subplan.setParam.is_empty();
    let no_parparam = subplan.parParam.is_empty();
    let subLinkType = subplan.subLinkType;
    let useHashTable = subplan.useHashTable;
    let setParam = clone_int_list(estate.es_query_cxt, &subplan.setParam)?;
    sstate.subplan = Some(subplan);

    // Verify the subplan state tree (es_subplanstates[plan_id - 1]) was
    // initialized; the SubPlanState reaches it by index at exec time (the
    // executor owns that list).
    exec_main::link_subplan_planstate::call(estate, plan_id)?;

    // sstate->testexpr = ExecInitExpr((Expr *) subplan->testexpr, parent);
    exec_expr::sub_init_testexpr::call(&mut sstate, estate)?;

    // initialize my state (everything else already zeroed by makeNode /
    // Default; the hash arrays are None until the useHashTable branch fills
    // them).
    sstate.curTuple = None;
    sstate.curArray = Datum::null();

    // If this is an initplan with output params and no direct correlation (and
    // not a CTE), mark those params as needing evaluation.  We don't set
    // parent->chgParam here.
    if has_setparam && no_parparam && subLinkType != SubLinkType::Cte {
        for &paramid in setParam.iter() {
            // prm = &(estate->es_param_exec_vals[paramid]);  prm->execPlan = sstate;
            // The `sstate` is this subplan's state; its stable identity is the
            // subplan's 1-based `plan_id` (the index into es_subplanstates).
            mark_exec_param_needs_eval(estate, paramid, plan_id)?;
        }
    }

    // If we are going to hash the subquery output, initialize relevant stuff.
    // (We don't create the hashtable until needed.)
    if useHashTable {
        // Memory contexts for the hash table(s) + a short-lived exprcontext
        // (nodeSubplan.c:897-907). The C creates two AllocSet children of
        // CurrentMemoryContext (here the per-query context) plus a fresh
        // ExprContext.
        //   sstate->hashtablecxt = AllocSetContextCreate(CurrentMemoryContext,
        //       "Subplan HashTable Context", ALLOCSET_DEFAULT_SIZES);
        //   sstate->hashtempcxt = AllocSetContextCreate(CurrentMemoryContext,
        //       "Subplan HashTable Temp Context", ALLOCSET_SMALL_SIZES);
        //   sstate->innerecontext = CreateExprContext(estate);
        sstate.hashtablecxt = Some(estate.es_query_cxt.context().new_child("Subplan HashTable Context"));
        sstate.hashtempcxt =
            Some(estate.es_query_cxt.context().new_child("Subplan HashTable Temp Context"));
        sstate.innerecontext = Some(executils::create_expr_context::call(estate)?);

        // Combining-operator list classification:
        //   IsA(testexpr, OpExpr)   -> one combining operator
        //   is_andclause(testexpr)  -> BoolExpr.args combining operators
        //   else                    -> elog(ERROR, "unrecognized testexpr type")
        let ncols = match exec_expr::classify_testexpr::call(&sstate) {
            exec_expr::CombiningTestExpr::SingleOp => 1,
            exec_expr::CombiningTestExpr::AndClause { ncols } => ncols,
            exec_expr::CombiningTestExpr::Unrecognized { node_tag } => {
                return Err(elog_unrecognized_testexpr(node_tag));
            }
        };

        // lefttlist = righttlist = NIL;
        // sstate->numCols = ncols;
        // sstate->keyColIdx     = palloc(ncols * sizeof(AttrNumber));
        // sstate->tab_eq_funcoids = palloc(ncols * sizeof(Oid));
        // sstate->tab_collations  = palloc(ncols * sizeof(Oid));
        // sstate->tab_hash_funcs  = palloc(ncols * sizeof(FmgrInfo));
        // lhs_hash_funcs          = palloc(ncols * sizeof(FmgrInfo));
        // sstate->cur_eq_funcs    = palloc(ncols * sizeof(FmgrInfo));
        // cross_eq_funcoids       = palloc(ncols * sizeof(Oid));
        //
        // The control arrays are this crate's own concretely-typed
        // `SubPlanState` fields, so they are allocated and written here, not
        // behind a seam. `lhs_hash_funcs` / `cross_eq_funcoids` are the two
        // transient arrays the C keeps on the stack ("not in sstate"); they are
        // built here and handed to the execExpr projection/ExprState builder.
        let mcx = estate.es_query_cxt;
        let n = ncols as usize;
        sstate.numCols = ncols;
        let mut key_col_idx: PgVec<'mcx, AttrNumber> = vec_with_capacity_in(mcx, n)?;
        let mut tab_eq_funcoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, n)?;
        let mut tab_collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, n)?;
        let mut tab_hash_funcs: PgVec<'mcx, FmgrInfo> = vec_with_capacity_in(mcx, n)?;
        let mut cur_eq_funcs: PgVec<'mcx, FmgrInfo> = vec_with_capacity_in(mcx, n)?;
        let mut lhs_hash_funcs: PgVec<'mcx, FmgrInfo> = vec_with_capacity_in(mcx, n)?;
        let mut cross_eq_funcoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, n)?;

        // foreach(l, oplist) with i = 1..
        let mut i: i32 = 1;
        while i <= ncols {
            let idx = (i - 1) as usize;
            // Resolve the per-column combining op (opfuncid, RHS eq op, hash
            // functions, collation); the catalog "could not find ..." errors
            // propagate from inside the seam (lsyscache-owned lookups:
            // get_compatible_hash_operators / get_opcode / get_op_hash_functions).
            let info = exec_expr::resolve_combining_op::call(&sstate, idx)?;

            // cross_eq_funcoids[i-1] = opexpr->opfuncid;
            cross_eq_funcoids.push(info.opfuncid);
            // fmgr_info(opexpr->opfuncid, &sstate->cur_eq_funcs[i-1]);
            // fmgr_info_set_expr((Node *) opexpr, &sstate->cur_eq_funcs[i-1]);
            // (the trimmed FmgrInfo carries only fn_oid; the expr back-link is
            // not represented, so fmgr_info_set_expr is a no-op here.)
            cur_eq_funcs.push(FmgrInfo { fn_oid: info.opfuncid, ..Default::default() });
            // sstate->tab_eq_funcoids[i-1] = get_opcode(rhs_eq_oper);
            tab_eq_funcoids.push(info.rhs_eq_funcoid);
            // fmgr_info(left_hashfn,  &lhs_hash_funcs[i-1]);
            // fmgr_info(right_hashfn, &sstate->tab_hash_funcs[i-1]);
            lhs_hash_funcs.push(FmgrInfo { fn_oid: info.left_hashfn, ..Default::default() });
            tab_hash_funcs.push(FmgrInfo { fn_oid: info.right_hashfn, ..Default::default() });
            // sstate->tab_collations[i-1] = opexpr->inputcollid;
            tab_collations.push(info.inputcollid);
            // keyColIdx is just column numbers 1..n
            //   sstate->keyColIdx[i-1] = i;
            key_col_idx.push(i as AttrNumber);

            i += 1;
        }

        sstate.keyColIdx = Some(key_col_idx);
        sstate.tab_eq_funcoids = Some(tab_eq_funcoids);
        sstate.tab_collations = Some(tab_collations);
        sstate.tab_hash_funcs = Some(tab_hash_funcs);
        sstate.cur_eq_funcs = Some(cur_eq_funcs);

        // Construct tupdescs, slots and projection nodes for left and right
        // sides, and build the lhs_hash_expr / cur_eq_comp ExprStates
        // (nodeSubplan.c:1009-1053). The lefthand/righthand tlists are assembled
        // from the combining `oplist` (makeTargetEntry over each OpExpr's two
        // args) and then fed to ExecTypeFromTL / ExecBuildProjectionInfo /
        // ExecBuildHash32FromAttrs / ExecBuildGroupingEqual — all execExpr-owned
        // machinery that also reads the raw `subplan->testexpr` Expr tree. The
        // two transient fmgr arrays the C keeps on the stack are handed over:
        // `lhs_hash_funcs` (for ExecBuildHash32FromAttrs) and `cross_eq_funcoids`
        // (for ExecBuildGroupingEqual).
        exec_expr::build_hash_projections_and_exprs::call(
            &mut sstate,
            estate,
            &lhs_hash_funcs,
            &cross_eq_funcoids,
        )?;
    }

    Ok(sstate)
}

// ===========================================================================
// ExecSetParamPlan / ExecSetParamPlanMulti
// ===========================================================================

/// `ExecSetParamPlan(node, econtext)` — execute a subplan and set its output
/// parameters. Called from `ExecEvalParamExec()` for lazy evaluation of
/// initplans.
pub fn ExecSetParamPlan<'mcx>(
    node: &mut SubPlanState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // The C uses `econtext` only for its `ecxt_per_query_memory` switch (== the
    // EState's per-query context) and to evaluate any down-passed params; this
    // path rejects correlated subplans (no down-passed params) and allocates in
    // `estate.es_query_cxt`, which is that same context. The parameter is kept
    // for signature parity with the C and `ExecEvalParamExec` callers.
    let _ = econtext;
    let subplan = subplan_ref(node)?;
    let subLinkType = subplan.subLinkType;
    let firstColType = subplan.firstColType;
    let has_parparam = !subplan.parParam.is_empty();
    let has_args = !subplan.args.is_empty();

    // ScanDirection dir = estate->es_direction;
    let dir = estate.es_direction;
    let mut found = false;
    let mut astate: Astate<'mcx> = None;

    if subLinkType == SubLinkType::Any || subLinkType == SubLinkType::All {
        return Err(elog_internal("ANY/ALL subselect unsupported as initplan"));
    }
    if subLinkType == SubLinkType::Cte {
        return Err(elog_internal("CTE subplans should not be executed via ExecSetParamPlan"));
    }
    if has_parparam || has_args {
        return Err(elog_internal("correlated subplans should not be executed via ExecSetParamPlan"));
    }

    // Enforce forward scan direction regardless of caller.
    estate.es_direction = types_nodes::ScanDirection::ForwardScanDirection;

    // Initialize ArrayBuildStateAny in caller's context, if needed.
    //   astate = initArrayResultAny(subplan->firstColType,
    //                               CurrentMemoryContext, true);
    // ExecSetParamPlan runs an initplan; its entry `CurrentMemoryContext` and
    // the `oldcontext` it captures are the per-query context, and the final
    // `makeArrayResultAny` is built explicitly in
    // `econtext->ecxt_per_query_memory` (== es_query_cxt) so the array survives
    // until query end (stashed in node->curArray for cross-call reuse).
    if subLinkType == SubLinkType::Array {
        astate = arrayfuncs::init_array_result_any::call(
            estate,
            econtext,
            arrayfuncs::ArrayBuildCtx::PerQuery,
            firstColType,
        )?;
    }

    let setParam = clone_int_list(estate.es_query_cxt, &subplan_ref(node)?.setParam)?;

    // Run the plan.  (If it needs rescanning, the first ExecProcNode handles it.)
    while let Some(slot) = exec_proc_node_child(node, estate)? {
        let mut i: AttrNumber = 1;

        if subLinkType == SubLinkType::Exists {
            // There can be only one setParam...
            let paramid = linitial_int(&setParam)?;
            set_exec_param_clear_execplan(estate, paramid, BoolGetDatum(true), false)?;
            found = true;
            break;
        }

        if subLinkType == SubLinkType::Array {
            found = true;
            // stash away current value
            let attr = exec_tuples::slot_getattr_by_id::call(estate, slot, 1)?;
            astate = arrayfuncs::accum_array_result_any::call(
                estate,
                econtext,
                arrayfuncs::ArrayBuildCtx::PerQuery,
                astate,
                attr.value,
                attr.isnull,
                firstColType,
            )?;
            // keep scanning subplan to collect all values
            continue;
        }

        if found
            && (subLinkType == SubLinkType::Expr
                || subLinkType == SubLinkType::MultiExpr
                || subLinkType == SubLinkType::RowCompare)
        {
            return Err(cardinality_violation());
        }

        found = true;

        // Copy the subplan's tuple into our own context, in case any params are
        // pass-by-ref; node->curTuple keeps it for eventual freeing.
        exec_tuples::replace_cur_tuple_from_slot::call(node, estate, slot)?;

        // Now set all the setParam params from the columns of the tuple.
        for &paramid in setParam.iter() {
            let attr = exec_tuples::cur_tuple_getattr::call(node, estate, slot, i)?;
            set_exec_param_clear_execplan(estate, paramid, attr.value, attr.isnull)?;
            i += 1;
        }
    }

    if subLinkType == SubLinkType::Array {
        // There can be only one setParam...
        let paramid = linitial_int(&setParam)?;

        // Build the result array in query context; to avoid leaking memory
        // across calls, remember the latest value (as for curTuple).
        //   node->curArray = makeArrayResultAny(astate,
        //                                       econtext->ecxt_per_query_memory,
        //                                       true);
        // pfree(DatumGetPointer(node->curArray)) — a no-op in the owned model
        // (the array bytes live in their owning context). The previous curArray
        // is simply dropped/overwritten below.
        arrayfuncs::pfree_array_datum::call(&node.curArray);
        // An array is pass-by-reference; the unified `_v` seam returns a
        // `Datum::ByRef` carrying the array bytes so it rides the by-ref fmgr
        // lane of any downstream array function.
        let arr = arrayfuncs::make_array_result_any_v::call(
            estate,
            econtext,
            arrayfuncs::ArrayBuildCtx::PerQuery,
            astate,
        )?;
        // node->curArray holds the freshly built array value for cross-call reuse.
        node.curArray = arr.clone();
        set_exec_param_clear_execplan(estate, paramid, arr, false)?;
    } else if !found {
        if subLinkType == SubLinkType::Exists {
            // There can be only one setParam...
            let paramid = linitial_int(&setParam)?;
            set_exec_param_clear_execplan(estate, paramid, BoolGetDatum(false), false)?;
        } else {
            // For other sublink types, set all the output params to NULL.
            for &paramid in setParam.iter() {
                set_exec_param_clear_execplan(estate, paramid, Datum::null(), true)?;
            }
        }
    }

    // restore scan direction
    estate.es_direction = dir;

    Ok(())
}

/// `ExecSetParamPlanMulti(params, econtext)` — apply [`ExecSetParamPlan`] to
/// evaluate any not-yet-evaluated initplan output parameters whose ParamIDs are
/// listed in `params`. Any listed params that are not initplan outputs are
/// ignored.
///
/// In the owned model the not-yet-evaluated subplan is held in the EState's
/// param array (`es_param_exec_vals[paramid].execPlan`); resolving and
/// re-entering `ExecSetParamPlan` over it belongs to the executor (it owns the
/// `SubPlanState` pool), so it goes through a seam.
pub fn ExecSetParamPlanMulti<'mcx>(
    params: Option<&Bitmapset<'mcx>>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // paramid = -1; while ((paramid = bms_next_member(params, paramid)) >= 0)
    let mut paramid: i32 = -1;
    loop {
        paramid = bms::bms_next_member::call(params, paramid);
        if paramid < 0 {
            break;
        }
        if exec_param_execplan_pending(estate, paramid) {
            // Parameter not evaluated yet, so go do it.
            exec_main::exec_set_param_plan_for_pending::call(econtext, paramid, estate)?;
            // ExecSetParamPlan should have processed this param...
            debug_assert!(!exec_param_execplan_pending(estate, paramid));
        }
    }
    Ok(())
}

/// `ExecReScanSetParamPlan(node, parent)` — mark an initplan as needing
/// recalculation.
///
/// `parent`'s `chgParam` and the `EState`-rooted param array are split out in
/// the owned model: the parent's `chgParam` slot is passed by `&mut`, and the
/// param array is the threaded estate.
pub fn ExecReScanSetParamPlan<'mcx>(
    node: &mut SubPlanState<'mcx>,
    parent_chg_param: &mut Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let subplan = subplan_ref(node)?;
    let has_parparam = !subplan.parParam.is_empty();
    let is_cte = subplan.subLinkType == SubLinkType::Cte;

    // sanity checks
    if has_parparam {
        return Err(elog_internal("direct correlated subquery unsupported as initplan"));
    }
    if subplan.setParam.is_empty() {
        return Err(elog_internal("setParam list of initplan is empty"));
    }
    // if (bms_is_empty(planstate->plan->extParam)) elog(ERROR, ...)
    if planstate_extparam_is_empty(node, estate)? {
        return Err(elog_internal("extParam set of initplan is empty"));
    }

    // Don't actually re-scan: it'll happen inside ExecSetParamPlan if needed.

    // Mark this subplan's output parameters as needing recalculation.  CTE
    // subplans are never executed via parameter recalculation; don't mark their
    // output dirty, but do set the chgParam bit so dependent nodes rescan.
    let mark_dirty = !is_cte;
    let per_query = estate.es_query_cxt;
    let plan_id = subplan_ref(node)?.plan_id;
    let setParam = clone_int_list(estate.es_query_cxt, &subplan_ref(node)?.setParam)?;
    for paramid in setParam {
        if mark_dirty {
            // prm->execPlan = node;
            mark_exec_param_needs_eval(estate, paramid, plan_id)?;
        }
        // parent->chgParam = bms_add_member(parent->chgParam, paramid);
        let old = parent_chg_param.take();
        *parent_chg_param = Some(bms::bms_add_member::call(per_query, old, paramid)?);
    }

    Ok(())
}

// ===========================================================================
// Node-layer helpers (operate on the owned EState / node).
// ===========================================================================

/// Clone a subplan integer list (`parParam`/`paramIds`/`setParam`) into a
/// working `Vec<i32>` charged to `mcx`, so the algorithm can iterate it without
/// holding a borrow of `node`/`subplan` across the per-element seam calls (which
/// also borrow `node`). Allocation failure surfaces the executor's OOM error.
fn clone_int_list<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    src: &mcx::PgVec<'mcx, i32>,
) -> PgResult<mcx::PgVec<'mcx, i32>> {
    mcx::slice_in(mcx, src).map_err(|_| mcx.oom(src.len() * core::mem::size_of::<i32>()))
}

/// Borrow `node->subplan`, erroring loudly if absent (the C dereferences
/// `node->subplan` unconditionally — it is always set by `ExecInitSubPlan`).
#[inline]
fn subplan_ref<'a, 'mcx>(
    node: &'a SubPlanState<'mcx>,
) -> PgResult<&'a types_nodes::primnodes::SubPlan<'mcx>> {
    node.subplan
        .as_deref()
        .ok_or_else(|| elog_internal("SubPlanState has no subplan"))
}

/// `node->planstate` head, mutably (the C dereferences it unconditionally).
#[inline]
/// `node->planstate` — the subselect plan's state tree, owned by
/// `EState.es_subplanstates` and addressed by the subplan's 1-based `plan_id`
/// (`list_nth(es_subplanstates, plan_id - 1)`). The owned model keeps ownership
/// in `es_subplanstates` (teardown owner `ExecEndPlan`), so the `SubPlanState`
/// reaches its child state by index rather than holding an aliasing box.
#[inline]
fn child_plan_id(node: &SubPlanState<'_>) -> PgResult<i32> {
    node.subplan
        .as_deref()
        .map(|sp| sp.plan_id)
        .ok_or_else(|| elog_internal("SubPlanState has no subplan"))
}

#[inline]
fn child_planstate_idx(node: &SubPlanState<'_>) -> PgResult<usize> {
    (child_plan_id(node)? as usize)
        .checked_sub(1)
        .ok_or_else(|| elog_internal("SubPlanState has no planstate"))
}

#[inline]
fn child_planstate<'a, 'mcx>(
    node: &SubPlanState<'mcx>,
    estate: &'a EStateData<'mcx>,
) -> PgResult<&'a types_nodes::planstate::PlanStateNode<'mcx>> {
    let idx = child_planstate_idx(node)?;
    estate
        .es_subplanstates
        .get(idx)
        .and_then(|b| b.as_deref())
        .ok_or_else(|| elog_internal("SubPlanState has no planstate"))
}

#[inline]
fn child_planstate_mut<'a, 'mcx>(
    node: &SubPlanState<'mcx>,
    estate: &'a mut EStateData<'mcx>,
) -> PgResult<&'a mut types_nodes::planstate::PlanStateNode<'mcx>> {
    let idx = child_planstate_idx(node)?;
    estate
        .es_subplanstates
        .get_mut(idx)
        .and_then(|b| b.as_deref_mut())
        .ok_or_else(|| elog_internal("SubPlanState has no planstate"))
}

fn planstate_head_mut<'a, 'mcx>(
    node: &SubPlanState<'mcx>,
    estate: &'a mut EStateData<'mcx>,
) -> PgResult<&'a mut types_nodes::execnodes::PlanStateData<'mcx>> {
    Ok(child_planstate_mut(node, estate)?.ps_head_mut())
}

/// `planstate->chgParam != NULL` — used to decide whether to rebuild the hash
/// table.
#[inline]
fn planstate_chgparam_set<'mcx>(node: &SubPlanState<'mcx>, estate: &EStateData<'mcx>) -> bool {
    child_planstate(node, estate)
        .map(|ps| ps.ps_head().chgParam.is_some())
        .unwrap_or(false)
}

/// `planstate->plan->plan_rows` (`buildSubPlanHash`).
#[inline]
fn planstate_plan_rows<'mcx>(node: &SubPlanState<'mcx>, estate: &EStateData<'mcx>) -> PgResult<f64> {
    let ps = child_planstate(node, estate)?;
    let plan = ps
        .ps_head()
        .plan
        .ok_or_else(|| elog_internal("subplan planstate has no plan"))?;
    Ok(plan.plan_head().plan_rows)
}

/// `bms_is_empty(planstate->plan->extParam)` (`ExecReScanSetParamPlan`).
#[inline]
fn planstate_extparam_is_empty<'mcx>(node: &SubPlanState<'mcx>, estate: &EStateData<'mcx>) -> PgResult<bool> {
    let ps = child_planstate(node, estate)?;
    let plan = ps
        .ps_head()
        .plan
        .ok_or_else(|| elog_internal("subplan planstate has no plan"))?;
    let ext = plan.plan_head().extParam.as_deref();
    Ok(bms::bms_is_empty::call(ext))
}

/// Move the child subplan's owned plan-state box out of `es_subplanstates`
/// (leaving the slot `None`), so it can be run with a live `&mut estate` (no
/// self-alias). Pair with [`put_subplanstate`].
#[inline]
fn take_subplanstate<'mcx>(
    estate: &mut EStateData<'mcx>,
    idx: usize,
) -> PgResult<mcx::PgBox<'mcx, types_nodes::planstate::PlanStateNode<'mcx>>> {
    estate
        .es_subplanstates
        .get_mut(idx)
        .and_then(|slot| slot.take())
        .ok_or_else(|| elog_internal("SubPlanState has no planstate"))
}

#[inline]
fn put_subplanstate<'mcx>(
    estate: &mut EStateData<'mcx>,
    idx: usize,
    ps: mcx::PgBox<'mcx, types_nodes::planstate::PlanStateNode<'mcx>>,
) {
    estate.es_subplanstates[idx] = Some(ps);
}

/// `ExecReScan(node->planstate)` over the child subselect tree.
#[inline]
fn exec_re_scan_child<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let idx = child_planstate_idx(node)?;
    let mut ps = take_subplanstate(estate, idx)?;
    let r = exec_ami::exec_re_scan::call(&mut ps, estate);
    put_subplanstate(estate, idx, ps);
    r
}

/// `slot = ExecProcNode(node->planstate)` over the child subselect tree;
/// `Some(slot)` while `!TupIsNull(slot)`, `None` at end.
#[inline]
fn exec_proc_node_child<'mcx>(
    node: &mut SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<types_nodes::SlotId>> {
    let idx = child_planstate_idx(node)?;
    let mut ps = take_subplanstate(estate, idx)?;
    let r = exec_procnode::exec_proc_node::call(&mut ps, estate);
    put_subplanstate(estate, idx, ps);
    // The C subplan tuple loops are `for (slot = ExecProcNode(planstate);
    // !TupIsNull(slot); slot = ExecProcNode(planstate))`. ExecProcNode at
    // end-of-scan returns a *non-NULL* but cleared slot (e.g. ExecScan's
    // `return ExecClearTuple(resultslot)` when a qual filtered out every row),
    // so the `while let Some(slot)` loops at the call sites must treat an empty
    // slot as end-of-scan. Apply TupIsNull here (map a cleared slot to `None`)
    // so all three callers (ExecScanSubPlan / buildSubPlanHash / ExecSetParamPlan)
    // see the C `!TupIsNull` semantics. Without this an EXISTS whose subquery
    // returns zero rows wrongly reports TRUE.
    Ok(r?.filter(|&s| !estate.slot(s).is_empty()))
}

/// `ResetExprContext(node->innerecontext)` — reset the inner exprcontext's
/// per-tuple memory (`MemoryContextReset`).
#[inline]
fn reset_inner_expr_context<'mcx>(
    node: &SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node
        .innerecontext
        .ok_or_else(|| elog_internal("subplan innerecontext is NULL"))?;
    reset_per_tuple_memory(estate, ecxt)
}

/// `MemoryContextReset(econtext->ecxt_per_tuple_memory)` for the given context
/// id in the EState pool.
#[inline]
fn reset_per_tuple_memory(estate: &mut EStateData<'_>, ecxt: EcxtId) -> PgResult<()> {
    let slot = estate
        .es_exprcontexts
        .get_mut(ecxt.0 as usize)
        .and_then(|e| e.as_mut())
        .ok_or_else(|| elog_internal("ExprContext id out of range / freed"))?;
    slot.ecxt_per_tuple_memory.reset();
    Ok(())
}

/// `prmdata = &econtext->ecxt_param_exec_vals[paramid]; prmdata->value = v;
/// prmdata->isnull = n;` — write a PARAM_EXEC slot in the EState param array
/// (the C reaches it via the econtext alias of `estate->es_param_exec_vals`).
#[inline]
fn set_exec_param<'mcx>(
    estate: &mut EStateData<'mcx>,
    paramid: i32,
    value: Datum<'mcx>,
    isnull: bool,
) -> PgResult<()> {
    let prm = exec_param_mut(estate, paramid)?;
    // `ParamExecData.value` is the canonical unified value; a by-reference
    // column value rides through verbatim (no longer flattened to a word).
    prm.value = value;
    prm.isnull = isnull;
    Ok(())
}

/// As [`set_exec_param`] but also clears the `execPlan` link
/// (`prm->execPlan = NULL`) — the form used by `ExecSetParamPlan` after
/// evaluating an initplan's output.
#[inline]
fn set_exec_param_clear_execplan<'mcx>(
    estate: &mut EStateData<'mcx>,
    paramid: i32,
    value: Datum<'mcx>,
    isnull: bool,
) -> PgResult<()> {
    let prm = exec_param_mut(estate, paramid)?;
    // As above: the canonical value is stored verbatim.
    prm.value = value;
    prm.isnull = isnull;
    // prm->execPlan = NULL; — the execPlan link is modeled by the executor's
    // param-pending seam below; the value/isnull writes above are the data.
    exec_main::clear_param_execplan::call(estate, paramid)?;
    Ok(())
}

/// `prm->execPlan = sstate` — mark a PARAM_EXEC as needing evaluation by this
/// subplan. The `execPlan` link is modeled on `ParamExecData` as the subplan's
/// identity (`plan_id`, the index into `es_subplanstates`); the executor owns the
/// param array, so installing the link goes through a seam.
#[inline]
fn mark_exec_param_needs_eval(
    estate: &mut EStateData<'_>,
    paramid: i32,
    plan_id: i32,
) -> PgResult<()> {
    exec_main::mark_param_execplan_pending::call(estate, paramid, plan_id)
}

/// `econtext->ecxt_param_exec_vals[paramid].execPlan != NULL` — is the param
/// not yet evaluated? The `execPlan` link is executor-owned, so the test goes
/// through a seam.
#[inline]
fn exec_param_execplan_pending(estate: &EStateData<'_>, paramid: i32) -> bool {
    exec_main::param_execplan_pending::call(estate, paramid)
}

/// `&estate->es_param_exec_vals[paramid]` — the param slot, mutably. The C
/// indexes unconditionally; an out-of-range id is a caller/planner bug.
#[inline]
fn exec_param_mut<'a, 'mcx>(
    estate: &'a mut EStateData<'mcx>,
    paramid: i32,
) -> PgResult<&'a mut types_nodes::execnodes::ParamExecData<'mcx>> {
    estate
        .es_param_exec_vals
        .get_mut(paramid as usize)
        .ok_or_else(|| elog_internal("PARAM_EXEC id out of range"))
}

/// `linitial_int(list)` — the first integer cell of a non-empty integer list.
#[inline]
fn linitial_int(list: &[i32]) -> PgResult<i32> {
    list.first()
        .copied()
        .ok_or_else(|| elog_internal("setParam list of initplan is empty"))
}

// ===========================================================================
// Error helpers (elog/ereport with exact text + SQLSTATE).
// ===========================================================================

/// `elog(ERROR, msg)` — an internal "can't happen" error (`errmsg_internal`,
/// untranslated; `ERRCODE_INTERNAL_ERROR`).
fn elog_internal(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `ereport(ERROR, (errcode(ERRCODE_CARDINALITY_VIOLATION), errmsg(...)))`.
fn cardinality_violation() -> PgError {
    PgError::error("more than one row returned by a subquery used as an expression")
        .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
}

/// `elog(ERROR, "unrecognized testexpr type: %d", (int) nodeTag(testexpr))`
/// (nodeSubplan.c:935-936).
fn elog_unrecognized_testexpr(tag: i32) -> PgError {
    PgError::error(alloc::format!("unrecognized testexpr type: {tag}"))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install every seam owned by this crate (declared in
/// `backend-executor-nodeSubplan-seams`).
pub fn init_seams() {
    backend_executor_nodeSubplan_seams::exec_re_scan_set_param_plan::set(ExecReScanSetParamPlan);
    backend_executor_nodeSubplan_seams::exec_set_param_plan::set(ExecSetParamPlan);
    backend_executor_nodeSubplan_seams::exec_sub_plan::set(ExecSubPlan);
    backend_executor_nodeSubplan_seams::exec_init_sub_plan::set(ExecInitSubPlan);

    // `ExecSetParamPlanMulti(params, econtext)` (nodeSubplan.c) on the parallel
    // executor's `execParallel-support` surface. The support seam carries a
    // present `params` set (`bms_next_member` over it); adapt to the body's
    // `Option<&Bitmapset>` C-nullability.
    backend_executor_execParallel_support_seams::exec_set_param_plan_multi::set(
        |params, econtext, estate| ExecSetParamPlanMulti(Some(params), econtext, estate),
    );
}
