//! Port of `src/backend/executor/nodeMergejoin.c` — routines supporting merge
//! joins.
//!
//! INTERFACE ROUTINES
//! - [`ExecMergeJoin`]        - merge join outer and inner relations.
//! - [`ExecInitMergeJoin`]    - initialize the merge join.
//! - [`ExecEndMergeJoin`]     - shut down the merge join.
//! - [`ExecReScanMergeJoin`]  - rescan the merge join.
//!
//! Merge join joins two pre-sorted inputs on `(= outerKey innerKey)` clauses.
//! The executor synchronises the two cursors, marking and restoring the inner
//! position when the outer side has duplicate keys, and null-fills for outer
//! joins. The whole `EXEC_MJ_*` state machine, the merge-key comparison
//! (`MJCompare` over the per-clause `SortSupportData`), the mark/restore
//! bookkeeping, the null-fill logic, and the const-qual classifier
//! (`check_constant_qual`) are this crate's owned logic. Operations below the
//! executor-node layer go through the owners' seam crates:
//!
//! - interrupt servicing (`CHECK_FOR_INTERRUPTS`) → tcop/postgres;
//! - child dispatch / init / teardown / rescan / mark / restore
//!   (`ExecProcNode` / `ExecInitNode` / `ExecEndNode` / `ExecReScan` /
//!   `ExecMarkPos` / `ExecRestrPos`) → execProcnode / execAmi;
//! - expression compilation and evaluation
//!   (`ExecInitExpr` / `ExecInitQual` / `ExecEvalExpr` / `ExecQual` /
//!   `ExecProject`) → execExpr;
//! - econtext / slot / projection setup (`ExecAssignExprContext` /
//!   `CreateExprContext` / `ExecAssignProjectionInfo` /
//!   `ExecInitResultTupleSlotTL` / `ExecInitExtraTupleSlot` /
//!   `ExecInitNullTupleSlot` / `ExecGetResultType` / `ExecGetResultSlotOps` /
//!   `ExecCopySlot` / `ExecClearTuple`) → execUtils / execTuples;
//! - the merge-clause catalog / sortsupport setup
//!   (`get_op_opfamily_properties` / `get_opfamily_method` /
//!   `get_opfamily_proc` / `IndexAmTranslateStrategy` / `OidFunctionCall1` /
//!   `PrepareSortSupportComparisonShim` and the comparator call) → lsyscache /
//!   amapi / sortsupport.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use backend_access_index_amapi_seams as amapi;
use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_sort_sortsupport_seams as sortsupport;

use mcx::{alloc_in, PgBox};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use types_nodes::jointype::{
    JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_RIGHT_ANTI, JOIN_SEMI,
};
use types_nodes::nodemergejoin::{
    EXEC_MJ_ENDINNER, EXEC_MJ_ENDOUTER, EXEC_MJ_INITIALIZE_INNER, EXEC_MJ_INITIALIZE_OUTER,
    EXEC_MJ_JOINTUPLES, EXEC_MJ_NEXTINNER, EXEC_MJ_NEXTOUTER, EXEC_MJ_SKIPINNER_ADVANCE,
    EXEC_MJ_SKIPOUTER_ADVANCE, EXEC_MJ_SKIP_TEST, EXEC_MJ_TESTOUTER,
};
use types_nodes::primnodes::Expr;
use types_nodes::{
    EStateData, EcxtId, MergeJoin, MergeJoinClauseData, MergeJoinStateData, PlanStateNode,
    SlotId, TupleSlotKind,
};
use types_sortsupport::{BTORDER_PROC, BTSORTSUPPORT_PROC, COMPARE_EQ};

/// `MJEvalResult` — result of [`MJEvalOuterValues`] / [`MJEvalInnerValues`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum MJEvalResult {
    /// `MJEVAL_MATCHABLE` — normal, potentially matchable tuple.
    Matchable,
    /// `MJEVAL_NONMATCHABLE` — tuple cannot join because it has a null.
    NonMatchable,
    /// `MJEVAL_ENDOFJOIN` — end of input (physical or effective).
    EndOfJoin,
}

/// Which inner slot a [`MJEvalInnerValues`] call reads from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum InnerSlot {
    /// the true current inner (`mj_InnerTupleSlot`).
    Live,
    /// the marked inner (`mj_MarkedTupleSlot`).
    Marked,
}

/// Install this crate's implementations into its seam slots. nodeMergejoin has
/// no `<unit>-seams` crate: its functions are reached through the executor
/// dispatch (execProcnode), which can depend on this crate directly without a
/// cycle.
pub fn init_seams() {}

/// `TupIsNull(slot)` — true if `slot` is NULL or marked empty (`TTS_EMPTY`).
/// The slot is an id into `estate.es_tupleTable`.
#[inline]
fn tup_is_null(slot: Option<SlotId>, estate: &EStateData<'_>) -> bool {
    match slot {
        None => true,
        Some(id) => estate.slot(id).is_empty(),
    }
}

/// `INVERT_COMPARE_RESULT(var)` (utils/sortsupport.h):
/// `((var) = ((var) < 0) ? 1 : -(var))`.
#[inline]
fn invert_compare_result(var: i32) -> i32 {
    if var < 0 {
        1
    } else {
        var.wrapping_neg()
    }
}

/// `ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED), ...)` for the
/// unsupported-join diagnostics.
fn feature(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// Plain `elog(ERROR, ...)` for the "should not happen" internal diagnostics.
fn elog(message: alloc::string::String) -> PgError {
    PgError::error(message)
}

// ===========================================================================
// MJExamineQuals
//
// Deconstruct the list of mergejoinable "leftexpr = rightexpr" expression trees
// into the node's mj_Clauses array, describing how to compare each merge key.
// ===========================================================================
fn MJExamineQuals<'mcx>(
    mergestate: &mut MergeJoinStateData<'mcx>,
    node: &MergeJoin<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let n_clauses = node.mergeclauses.len();

    // clauses = (MergeJoinClause) palloc0(nClauses * sizeof(MergeJoinClauseData));
    let mut clauses: mcx::PgVec<'mcx, MergeJoinClauseData<'mcx>> =
        mcx::vec_with_capacity_in(mcx, n_clauses)?;

    for i_clause in 0..n_clauses {
        // OpExpr *qual = (OpExpr *) lfirst(cl);
        // if (!IsA(qual, OpExpr)) elog(ERROR, "mergejoin clause is not an OpExpr");
        let qual = match &node.mergeclauses[i_clause] {
            Expr::OpExpr(op) => op,
            _ => return Err(elog("mergejoin clause is not an OpExpr".into())),
        };

        let opfamily = node.mergeFamilies[i_clause];
        let collation = node.mergeCollations[i_clause];
        let reversed = node.mergeReversals[i_clause];
        let nulls_first = node.mergeNullsFirst[i_clause];

        let mut clause = MergeJoinClauseData::zeroed(mcx);

        // Prepare the input expressions for execution.
        //   clause->lexpr = ExecInitExpr((Expr *) linitial(qual->args), parent);
        //   clause->rexpr = ExecInitExpr((Expr *) lsecond(qual->args), parent);
        clause.lexpr =
            Some(execExpr::exec_init_expr::call(&qual.args[0], &mut mergestate.js.ps, estate)?);
        clause.rexpr =
            Some(execExpr::exec_init_expr::call(&qual.args[1], &mut mergestate.js.ps, estate)?);

        // Set up sort support data (ssup_cxt set by zeroed(mcx)).
        clause.ssup.ssup_collation = collation;
        clause.ssup.ssup_reverse = reversed;
        clause.ssup.ssup_nulls_first = nulls_first;

        // Extract the operator's declared left/right datatypes.
        //   get_op_opfamily_properties(qual->opno, opfamily, false, ...);
        let (op_strategy, op_lefttype, op_righttype) =
            lsyscache::get_op_opfamily_properties::call(qual.opno, opfamily, false)?
                .expect("get_op_opfamily_properties(missing_ok=false) returned None");

        // if (IndexAmTranslateStrategy(op_strategy, get_opfamily_method(opfamily),
        //         opfamily, true) != COMPARE_EQ) elog(ERROR, ...);
        let amoid = lsyscache::get_opfamily_method::call(opfamily)?;
        if amapi::index_am_translate_strategy::call(op_strategy, amoid, opfamily, true)?
            != COMPARE_EQ
        {
            return Err(elog(alloc::format!(
                "cannot merge using non-equality operator {}",
                qual.opno
            )));
        }

        // sortsupport routine must know whether abbreviation applies; never for
        // merge joins.
        clause.ssup.abbreviate = false;

        // And get the matching support or comparison function.
        //   Assert(clause->ssup.comparator == NULL);
        debug_assert!(clause.ssup.comparator.is_none());
        let mut sortfunc = lsyscache::get_opfamily_proc::call(
            opfamily,
            op_lefttype,
            op_righttype,
            BTSORTSUPPORT_PROC,
        )?;
        if sortfunc != 0 {
            // The sort support function can provide a comparator.
            //   OidFunctionCall1(sortfunc, PointerGetDatum(&clause->ssup));
            sortsupport::oid_function_call_1_sortsupport::call(sortfunc, &mut clause.ssup)?;
        }
        if clause.ssup.comparator.is_none() {
            // support not available, get comparison func.
            sortfunc = lsyscache::get_opfamily_proc::call(
                opfamily,
                op_lefttype,
                op_righttype,
                BTORDER_PROC,
            )?;
            if sortfunc == 0 {
                return Err(elog(alloc::format!(
                    "missing support function {}({},{}) in opfamily {}",
                    BTORDER_PROC, op_lefttype, op_righttype, opfamily
                )));
            }
            // We'll use a shim to call the old-style btree comparator.
            sortsupport::prepare_sort_support_comparison_shim::call(sortfunc, &mut clause.ssup)?;
        }

        clauses.push(clause);
    }

    mergestate.mj_Clauses = clauses;
    Ok(())
}

// ===========================================================================
// MJEvalOuterValues — compute the mergejoined expression values for the
// current outer tuple, detecting unmatchability (a NULL input) and
// end-of-input.
// ===========================================================================
fn MJEvalOuterValues<'mcx>(
    mergestate: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<MJEvalResult> {
    let mut result = MJEvalResult::Matchable;

    // Check for end of outer subplan.
    if tup_is_null(mergestate.mj_OuterTupleSlot, estate) {
        return Ok(MJEvalResult::EndOfJoin);
    }

    // econtext = mergestate->mj_OuterEContext; ResetExprContext(econtext);
    // econtext->ecxt_outertuple = mergestate->mj_OuterTupleSlot;
    let econtext = mergestate
        .mj_OuterEContext
        .expect("MJEvalOuterValues: mj_OuterEContext not created");
    let outer_slot = mergestate.mj_OuterTupleSlot;
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_per_tuple_memory.reset();
        ec.ecxt_outertuple = outer_slot;
    }

    let num_clauses = mergestate.mj_NumClauses;
    for i in 0..num_clauses as usize {
        // clause->ldatum = ExecEvalExpr(clause->lexpr, econtext, &clause->lisnull);
        let lexpr = mergestate.mj_Clauses[i]
            .lexpr
            .as_deref_mut()
            .expect("MJEvalOuterValues: clause lexpr not compiled");
        let (ldatum, lisnull) =
            execExpr::exec_eval_expr_switch_context::call(lexpr, econtext, estate)?;
        let clause = &mut mergestate.mj_Clauses[i];
        // `ExecEvalExpr` now hands back a canonical `Datum<'mcx>`; store it
        // directly into the clause's `ldatum`.
        clause.ldatum = ldatum;
        clause.lisnull = lisnull;
        if lisnull {
            // match is impossible; can we end the join early?
            if i == 0 && !clause.ssup.ssup_nulls_first && !mergestate.mj_FillOuter {
                result = MJEvalResult::EndOfJoin;
            } else if result == MJEvalResult::Matchable {
                result = MJEvalResult::NonMatchable;
            }
        }
    }

    Ok(result)
}

// ===========================================================================
// MJEvalInnerValues — same as MJEvalOuterValues, but for the inner tuple, from
// the caller-chosen slot (true current inner, or the marked inner).
// ===========================================================================
fn MJEvalInnerValues<'mcx>(
    mergestate: &mut MergeJoinStateData<'mcx>,
    innerslot: InnerSlot,
    estate: &mut EStateData<'mcx>,
) -> PgResult<MJEvalResult> {
    let mut result = MJEvalResult::Matchable;

    let slot = match innerslot {
        InnerSlot::Live => mergestate.mj_InnerTupleSlot,
        InnerSlot::Marked => mergestate.mj_MarkedTupleSlot,
    };
    // Check for end of inner subplan.
    if tup_is_null(slot, estate) {
        return Ok(MJEvalResult::EndOfJoin);
    }

    // econtext = mergestate->mj_InnerEContext; ResetExprContext(econtext);
    // econtext->ecxt_innertuple = innerslot;
    let econtext = mergestate
        .mj_InnerEContext
        .expect("MJEvalInnerValues: mj_InnerEContext not created");
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_per_tuple_memory.reset();
        ec.ecxt_innertuple = slot;
    }

    let num_clauses = mergestate.mj_NumClauses;
    for i in 0..num_clauses as usize {
        // clause->rdatum = ExecEvalExpr(clause->rexpr, econtext, &clause->risnull);
        let rexpr = mergestate.mj_Clauses[i]
            .rexpr
            .as_deref_mut()
            .expect("MJEvalInnerValues: clause rexpr not compiled");
        let (rdatum, risnull) =
            execExpr::exec_eval_expr_switch_context::call(rexpr, econtext, estate)?;
        let clause = &mut mergestate.mj_Clauses[i];
        // `ExecEvalExpr` now hands back a canonical `Datum<'mcx>`; store it
        // directly into the clause's `rdatum`.
        clause.rdatum = rdatum;
        clause.risnull = risnull;
        if risnull {
            // match is impossible; can we end the join early?
            if i == 0 && !clause.ssup.ssup_nulls_first && !mergestate.mj_FillInner {
                result = MJEvalResult::EndOfJoin;
            } else if result == MJEvalResult::Matchable {
                result = MJEvalResult::NonMatchable;
            }
        }
    }

    Ok(result)
}

// ===========================================================================
// ApplySortComparator (utils/sortsupport.h) — compare two datums per the sort
// support data. The null/reverse arithmetic is inlined exactly as the C macro;
// the comparator-function invocation goes through the sortsupport seam.
// ===========================================================================
fn ApplySortComparator<'mcx>(
    clause: &MergeJoinClauseData<'mcx>,
) -> PgResult<i32> {
    let is_null1 = clause.lisnull;
    let is_null2 = clause.risnull;
    let nulls_first = clause.ssup.ssup_nulls_first;
    let reverse = clause.ssup.ssup_reverse;

    let compare = if is_null1 {
        if is_null2 {
            0 // NULL "=" NULL
        } else if nulls_first {
            -1 // NULL "<" NOT_NULL
        } else {
            1 // NULL ">" NOT_NULL
        }
    } else if is_null2 {
        if nulls_first {
            1 // NOT_NULL ">" NULL
        } else {
            -1 // NOT_NULL "<" NULL
        }
    } else {
        // compare = ssup->comparator(datum1, datum2, ssup);
        // The comparator seam takes the bare-word `Datum` (`types_datum::Datum`);
        // the canonical clause values are non-null scalars here (`!is_null{1,2}`),
        // so the by-value scalar word crosses back out.
        let datum1 = types_datum::Datum::from_usize(clause.ldatum.as_usize());
        let datum2 = types_datum::Datum::from_usize(clause.rdatum.as_usize());
        let mut compare = sortsupport::apply_sort_comparator::call(
            datum1,
            datum2,
            &clause.ssup,
        )?;
        if reverse {
            compare = invert_compare_result(compare);
        }
        compare
    };
    Ok(compare)
}

// ===========================================================================
// MJCompare — compare the mergejoinable values of the current two input
// tuples: 0 if equal, >0 if outer > inner, <0 if outer < inner.
// ===========================================================================
fn MJCompare<'mcx>(
    mergestate: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    let mut result: i32 = 0;
    let mut nulleqnull = false;

    // Call the comparison functions in short-lived context, in case they leak.
    //   econtext = mergestate->js.ps.ps_ExprContext; ResetExprContext(econtext);
    if let Some(econtext) = mergestate.js.ps.ps_ExprContext {
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }

    let num_clauses = mergestate.mj_NumClauses;
    for i in 0..num_clauses as usize {
        // Special case for NULL-vs-NULL, else use standard comparison.
        let clause = &mergestate.mj_Clauses[i];
        if clause.lisnull && clause.risnull {
            nulleqnull = true; // NULL "=" NULL
            continue;
        }

        result = ApplySortComparator(clause)?;

        if result != 0 {
            break;
        }
    }

    // If we had any NULL-vs-NULL inputs, or a constant-false joinqual, do not
    // report equality: change a 0 result to +1 to advance the inner side.
    if result == 0 && (nulleqnull || mergestate.mj_ConstFalseJoin) {
        result = 1;
    }

    Ok(result)
}

// ===========================================================================
// MarkInnerTuple — copy the current inner tuple into the marked slot.
//   ExecCopySlot(mergestate->mj_MarkedTupleSlot, innerTupleSlot)
// ===========================================================================
fn MarkInnerTuple<'mcx>(
    inner_tuple_slot: SlotId,
    mergestate: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let marked = mergestate
        .mj_MarkedTupleSlot
        .expect("MarkInnerTuple: mj_MarkedTupleSlot not initialized");
    let mcx = estate.es_query_cxt;
    let (dst, src) = estate.slot_pair_mut(marked, inner_tuple_slot);
    execTuples::exec_copy_slot::call(mcx, dst, src)
}

// ===========================================================================
// MJFillOuter — generate a fake join tuple with nulls for the inner tuple, and
// return it if it passes the non-join quals.
//
// Returns `Ok(true)` when a projected tuple is available in the node's result
// slot (the C `return ExecProject(...)`), `Ok(false)` otherwise.
// ===========================================================================
fn MJFillOuter<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("MJFillOuter: ps_ExprContext not created");

    // ResetExprContext(econtext);
    // econtext->ecxt_outertuple = node->mj_OuterTupleSlot;
    // econtext->ecxt_innertuple = node->mj_NullInnerTupleSlot;
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_per_tuple_memory.reset();
        ec.ecxt_outertuple = node.mj_OuterTupleSlot;
        ec.ecxt_innertuple = node.mj_NullInnerTupleSlot;
    }

    // if (ExecQual(otherqual, econtext)) return ExecProject(...);
    if exec_qual_or_true(node.js.ps.qual.as_deref_mut(), econtext, estate)? {
        exec_project_into_result(&mut node.js.ps, estate)?;
        Ok(true)
    } else {
        instr_count_filtered2(node);
        Ok(false)
    }
}

// ===========================================================================
// MJFillInner — generate a fake join tuple with nulls for the outer tuple, and
// return it if it passes the non-join quals.
// ===========================================================================
fn MJFillInner<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("MJFillInner: ps_ExprContext not created");

    // ResetExprContext(econtext);
    // econtext->ecxt_outertuple = node->mj_NullOuterTupleSlot;
    // econtext->ecxt_innertuple = node->mj_InnerTupleSlot;
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_per_tuple_memory.reset();
        ec.ecxt_outertuple = node.mj_NullOuterTupleSlot;
        ec.ecxt_innertuple = node.mj_InnerTupleSlot;
    }

    if exec_qual_or_true(node.js.ps.qual.as_deref_mut(), econtext, estate)? {
        exec_project_into_result(&mut node.js.ps, estate)?;
        Ok(true)
    } else {
        instr_count_filtered2(node);
        Ok(false)
    }
}

/// `ExecQual(state, econtext)` with the C short-circuit: a `NULL` qual is
/// always-true.
#[inline]
fn exec_qual_or_true<'mcx>(
    state: Option<&mut types_nodes::execexpr::ExprState<'mcx>>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match state {
        None => Ok(true),
        Some(s) => execExpr::exec_qual::call(s, econtext, estate),
    }
}

/// `ExecProject(node->js.ps.ps_ProjInfo)` — form the projection into the node's
/// result slot.
#[inline]
fn exec_project_into_result<'mcx>(
    ps: &mut types_nodes::execnodes::PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<SlotId> {
    execExpr::exec_project::call(ps, estate)
}

/// `InstrCountFiltered1(node, 1)` — bump the join-qual-filtered counter when
/// instrumentation is active. The owned model holds the `Instrumentation` in
/// the node's `ps.instrument`.
#[inline]
fn instr_count_filtered1(node: &mut MergeJoinStateData<'_>) {
    if let Some(instr) = node.js.ps.instrument.as_deref_mut() {
        instr.nfiltered1 += 1.0;
    }
}

/// `InstrCountFiltered2(node, 1)` — bump the other-qual-filtered counter.
#[inline]
fn instr_count_filtered2(node: &mut MergeJoinStateData<'_>) {
    if let Some(instr) = node.js.ps.instrument.as_deref_mut() {
        instr.nfiltered2 += 1.0;
    }
}

// ===========================================================================
// check_constant_qual — check that a qual condition is constant true or
// constant false. If constant false (or null), set `*is_const_false`. Returns
// false if the qual contains a non-Const term.
// ===========================================================================
fn check_constant_qual(qual: Option<&[Expr]>, is_const_false: &mut bool) -> bool {
    let Some(qual) = qual else {
        return true; // NIL list — constant true
    };
    for cell in qual {
        // Const *con = (Const *) lfirst(lc);
        // if (!con || !IsA(con, Const)) return false;
        let con = match cell {
            Expr::Const(c) => c,
            _ => return false,
        };
        // if (con->constisnull || !DatumGetBool(con->constvalue)) *is_const_false = true;
        if con.constisnull || !con.constvalue.as_bool() {
            *is_const_false = true;
        }
    }
    true
}

// ===========================================================================
// ExecMergeJoin — the PlanState.ExecProcNode callback.
// ===========================================================================

/// `ExecMergeJoin(pstate)` — drive the merge-join state machine, returning
/// `Ok(Some(id))` (the node's result-slot id, C's returned `TupleTableSlot *`)
/// when the next qualifying join tuple has been projected, `Ok(None)` when the
/// join is exhausted (the C `return NULL`).
pub fn ExecMergeJoin<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    // get information from node
    let joinqual_present = node.js.joinqual.is_some();
    let otherqual_present = node.js.ps.qual.is_some();
    let do_fill_outer = node.mj_FillOuter;
    let do_fill_inner = node.mj_FillInner;

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle. ResetExprContext(econtext).
    if let Some(econtext) = node.js.ps.ps_ExprContext {
        estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();
    }

    // ok, everything is setup .. let's go to work
    loop {
        match node.mj_JoinState {
            // EXEC_MJ_INITIALIZE_OUTER — first call: fetch first outer tuple.
            EXEC_MJ_INITIALIZE_OUTER => {
                node.mj_OuterTupleSlot = exec_outer(node, estate)?;

                match MJEvalOuterValues(node, estate)? {
                    MJEvalResult::Matchable => {
                        // OK to go get the first inner tuple.
                        node.mj_JoinState = EXEC_MJ_INITIALIZE_INNER;
                    }
                    MJEvalResult::NonMatchable => {
                        // Stay in same state to fetch next outer tuple.
                        if do_fill_outer && MJFillOuter(node, estate)? {
                            return Ok(node.js.ps.ps_ResultTupleSlot);
                        }
                    }
                    MJEvalResult::EndOfJoin => {
                        // No more outer tuples.
                        if do_fill_inner {
                            // Need to emit right-join tuples for remaining
                            // inner tuples. Set MatchedInner = true to force
                            // ENDOUTER to advance inner.
                            node.mj_JoinState = EXEC_MJ_ENDOUTER;
                            node.mj_MatchedInner = true;
                            continue;
                        }
                        return Ok(None);
                    }
                }
            }

            EXEC_MJ_INITIALIZE_INNER => {
                node.mj_InnerTupleSlot = exec_inner(node, estate)?;

                match MJEvalInnerValues(node, InnerSlot::Live, estate)? {
                    MJEvalResult::Matchable => {
                        // OK, we have the initial tuples. Begin by skipping
                        // non-matching tuples.
                        node.mj_JoinState = EXEC_MJ_SKIP_TEST;
                    }
                    MJEvalResult::NonMatchable => {
                        // Mark before advancing, if wanted.
                        if node.mj_ExtraMarks {
                            exec_mark_pos_inner(node, estate)?;
                        }
                        // Stay in same state to fetch next inner tuple.
                        if do_fill_inner && MJFillInner(node, estate)? {
                            return Ok(node.js.ps.ps_ResultTupleSlot);
                        }
                    }
                    MJEvalResult::EndOfJoin => {
                        // No more inner tuples.
                        if do_fill_outer {
                            // Need to emit left-join tuples for all outer
                            // tuples, including the one we just fetched. Set
                            // MatchedOuter = false to force ENDINNER to emit
                            // first tuple before advancing outer.
                            node.mj_JoinState = EXEC_MJ_ENDINNER;
                            node.mj_MatchedOuter = false;
                            continue;
                        }
                        return Ok(None);
                    }
                }
            }

            // EXEC_MJ_JOINTUPLES — two tuples satisfied the merge clause; join
            // them and proceed to get the next inner tuple.
            EXEC_MJ_JOINTUPLES => {
                // Set the next state machine state.
                node.mj_JoinState = EXEC_MJ_NEXTINNER;

                // We don't bother with a ResetExprContext here, on the
                // assumption that we just did one while checking the merge
                // qual. We do have to set up the econtext links to the tuples
                // for ExecQual/ExecProject to use.
                //   econtext->ecxt_outertuple = node->mj_OuterTupleSlot;
                //   econtext->ecxt_innertuple = node->mj_InnerTupleSlot;
                {
                    let econtext = node
                        .js
                        .ps
                        .ps_ExprContext
                        .expect("EXEC_MJ_JOINTUPLES: ps_ExprContext not created");
                    let ec = estate.ecxt_mut(econtext);
                    ec.ecxt_outertuple = node.mj_OuterTupleSlot;
                    ec.ecxt_innertuple = node.mj_InnerTupleSlot;
                }

                // Check the extra qual conditions. We must distinguish the
                // additional joinquals (which must pass to consider the tuples
                // "matched") from the otherquals (which must pass before
                // returning the tuple).
                let qual_result = !joinqual_present || exec_joinqual(node, estate)?;

                if qual_result {
                    node.mj_MatchedOuter = true;
                    node.mj_MatchedInner = true;

                    // In an antijoin, we never return a matched tuple.
                    if node.js.jointype == JOIN_ANTI {
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                        continue;
                    }

                    // If we only need the first matching inner tuple, advance
                    // to next outer after we've processed this one.
                    if node.js.single_match {
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }

                    // In a right-antijoin, we never return a matched tuple. If
                    // not inner_unique, stay on the current outer tuple to keep
                    // scanning the inner side for matches.
                    if node.js.jointype == JOIN_RIGHT_ANTI {
                        continue;
                    }

                    let qual_result = !otherqual_present || exec_otherqual(node, estate)?;

                    if qual_result {
                        // qualification succeeded; form and return the
                        // projection.
                        exec_project_into_result(&mut node.js.ps, estate)?;
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    } else {
                        instr_count_filtered2(node);
                    }
                } else {
                    instr_count_filtered1(node);
                }
            }

            // EXEC_MJ_NEXTINNER — advance the inner scan to the next tuple.
            EXEC_MJ_NEXTINNER => {
                if do_fill_inner && !node.mj_MatchedInner {
                    // Emit a fill tuple with nulls for the outer side.
                    node.mj_MatchedInner = true; // do it only once
                    if MJFillInner(node, estate)? {
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    }
                }

                // Get the next inner tuple, if any.
                // NB: must NOT do "extraMarks" here.
                node.mj_InnerTupleSlot = exec_inner(node, estate)?;
                node.mj_MatchedInner = false;

                match MJEvalInnerValues(node, InnerSlot::Live, estate)? {
                    MJEvalResult::Matchable => {
                        // Test the new inner tuple against outer.
                        let compare_result = MJCompare(node, estate)?;

                        if compare_result == 0 {
                            node.mj_JoinState = EXEC_MJ_JOINTUPLES;
                        } else if compare_result < 0 {
                            node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                        } else {
                            // compareResult > 0 should not happen
                            return Err(elog("mergejoin input data is out of order".into()));
                        }
                    }
                    MJEvalResult::NonMatchable => {
                        // Contains a NULL: can't match any outer; assume
                        // greater.
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                    MJEvalResult::EndOfJoin => {
                        // No more inner tuples. Force mj_InnerTupleSlot to null
                        // (this might be only effective end of inner).
                        node.mj_InnerTupleSlot = None;
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                }
            }

            // EXEC_MJ_NEXTOUTER — advance the outer scan, then test against the
            // marked tuple.
            EXEC_MJ_NEXTOUTER => {
                if do_fill_outer && !node.mj_MatchedOuter {
                    // Emit a fill tuple with nulls for the inner side.
                    node.mj_MatchedOuter = true; // do it only once
                    if MJFillOuter(node, estate)? {
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    }
                }

                // Get the next outer tuple, if any.
                node.mj_OuterTupleSlot = exec_outer(node, estate)?;
                node.mj_MatchedOuter = false;

                match MJEvalOuterValues(node, estate)? {
                    MJEvalResult::Matchable => {
                        // Go test the new tuple against the marked tuple.
                        node.mj_JoinState = EXEC_MJ_TESTOUTER;
                    }
                    MJEvalResult::NonMatchable => {
                        // Can't match, so fetch next outer tuple.
                        node.mj_JoinState = EXEC_MJ_NEXTOUTER;
                    }
                    MJEvalResult::EndOfJoin => {
                        // No more outer tuples.
                        if do_fill_inner && !tup_is_null(node.mj_InnerTupleSlot, estate) {
                            // Need to emit right-join tuples for remaining
                            // inners.
                            node.mj_JoinState = EXEC_MJ_ENDOUTER;
                            continue;
                        }
                        return Ok(None);
                    }
                }
            }

            // EXEC_MJ_TESTOUTER — compare the new outer tuple with the marked
            // inner tuple to decide whether to restore the inner scan.
            EXEC_MJ_TESTOUTER => {
                // Compare the outer tuple with the marked inner tuple. (We can
                // ignore the result of MJEvalInnerValues, since the marked
                // inner tuple is certainly matchable.)
                let _ = MJEvalInnerValues(node, InnerSlot::Marked, estate)?;

                let compare_result = MJCompare(node, estate)?;

                if compare_result == 0 {
                    // The merge clause matched: restore the inner scan position
                    // to the first mark, and go join that tuple to the new
                    // outer.
                    if !node.mj_SkipMarkRestore {
                        exec_restr_pos_inner(node, estate)?;

                        // ExecRestrPos doesn't give back a new slot, so use the
                        // marked slot.
                        node.mj_InnerTupleSlot = node.mj_MarkedTupleSlot;
                        // we need not do MJEvalInnerValues again
                    }

                    node.mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if compare_result > 0 {
                    // The new outer tuple didn't match the marked inner tuple:
                    // all subsequent outer tuples will be larger than our marked
                    // inners, so proceed to look for a match to the current
                    // inner.

                    // reload comparison data for current inner
                    match MJEvalInnerValues(node, InnerSlot::Live, estate)? {
                        MJEvalResult::Matchable => {
                            // proceed to compare it to the current outer
                            node.mj_JoinState = EXEC_MJ_SKIP_TEST;
                        }
                        MJEvalResult::NonMatchable => {
                            // current inner can't possibly match any outer;
                            // better to advance the inner scan than the outer.
                            node.mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                        }
                        MJEvalResult::EndOfJoin => {
                            // No more inner tuples.
                            if do_fill_outer {
                                // Need to emit left-join tuples for remaining
                                // outers.
                                node.mj_JoinState = EXEC_MJ_ENDINNER;
                                continue;
                            }
                            return Ok(None);
                        }
                    }
                } else {
                    // compareResult < 0 should not happen
                    return Err(elog("mergejoin input data is out of order".into()));
                }
            }

            // EXEC_MJ_SKIP_TEST — compare tuples; if they don't match, skip the
            // lesser.
            EXEC_MJ_SKIP_TEST => {
                // Before we advance, make sure the current tuples do not
                // satisfy the mergeclauses. If they do, update the marked tuple
                // and join.
                let compare_result = MJCompare(node, estate)?;

                if compare_result == 0 {
                    if !node.mj_SkipMarkRestore {
                        exec_mark_pos_inner(node, estate)?;
                    }

                    let inner = node
                        .mj_InnerTupleSlot
                        .expect("EXEC_MJ_SKIP_TEST: mj_InnerTupleSlot is null");
                    MarkInnerTuple(inner, node, estate)?;

                    node.mj_JoinState = EXEC_MJ_JOINTUPLES;
                } else if compare_result < 0 {
                    node.mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                } else {
                    // compareResult > 0
                    node.mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                }
            }

            // EXEC_MJ_SKIPOUTER_ADVANCE — advance over an outer tuple known not
            // to join to any inner tuple.
            EXEC_MJ_SKIPOUTER_ADVANCE => {
                if do_fill_outer && !node.mj_MatchedOuter {
                    // Emit a fill tuple with nulls for the inner side.
                    node.mj_MatchedOuter = true; // do it only once
                    if MJFillOuter(node, estate)? {
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    }
                }

                // Get the next outer tuple, if any.
                node.mj_OuterTupleSlot = exec_outer(node, estate)?;
                node.mj_MatchedOuter = false;

                match MJEvalOuterValues(node, estate)? {
                    MJEvalResult::Matchable => {
                        // Go test the new tuple against the current inner.
                        node.mj_JoinState = EXEC_MJ_SKIP_TEST;
                    }
                    MJEvalResult::NonMatchable => {
                        // Can't match, so fetch next outer tuple.
                        node.mj_JoinState = EXEC_MJ_SKIPOUTER_ADVANCE;
                    }
                    MJEvalResult::EndOfJoin => {
                        // No more outer tuples.
                        if do_fill_inner && !tup_is_null(node.mj_InnerTupleSlot, estate) {
                            // Need to emit right-join tuples for remaining
                            // inners.
                            node.mj_JoinState = EXEC_MJ_ENDOUTER;
                            continue;
                        }
                        return Ok(None);
                    }
                }
            }

            // EXEC_MJ_SKIPINNER_ADVANCE — advance over an inner tuple known not
            // to join to any outer tuple.
            EXEC_MJ_SKIPINNER_ADVANCE => {
                if do_fill_inner && !node.mj_MatchedInner {
                    // Emit a fill tuple with nulls for the outer side.
                    node.mj_MatchedInner = true; // do it only once
                    if MJFillInner(node, estate)? {
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    }
                }

                // Mark before advancing, if wanted.
                if node.mj_ExtraMarks {
                    exec_mark_pos_inner(node, estate)?;
                }

                // Get the next inner tuple, if any.
                node.mj_InnerTupleSlot = exec_inner(node, estate)?;
                node.mj_MatchedInner = false;

                match MJEvalInnerValues(node, InnerSlot::Live, estate)? {
                    MJEvalResult::Matchable => {
                        // proceed to compare it to the current outer
                        node.mj_JoinState = EXEC_MJ_SKIP_TEST;
                    }
                    MJEvalResult::NonMatchable => {
                        // current inner can't possibly match any outer; better
                        // to advance the inner scan than the outer.
                        node.mj_JoinState = EXEC_MJ_SKIPINNER_ADVANCE;
                    }
                    MJEvalResult::EndOfJoin => {
                        // No more inner tuples.
                        if do_fill_outer && !tup_is_null(node.mj_OuterTupleSlot, estate) {
                            // Need to emit left-join tuples for remaining
                            // outers.
                            node.mj_JoinState = EXEC_MJ_ENDINNER;
                            continue;
                        }
                        return Ok(None);
                    }
                }
            }

            // EXEC_MJ_ENDOUTER — out of outer tuples; null-fill remaining
            // unmatched inner tuples (right/right-anti/full join).
            EXEC_MJ_ENDOUTER => {
                debug_assert!(do_fill_inner);

                if !node.mj_MatchedInner {
                    // Emit a fill tuple with nulls for the outer side.
                    node.mj_MatchedInner = true; // do it only once
                    if MJFillInner(node, estate)? {
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    }
                }

                // Mark before advancing, if wanted.
                if node.mj_ExtraMarks {
                    exec_mark_pos_inner(node, estate)?;
                }

                // Get the next inner tuple, if any.
                node.mj_InnerTupleSlot = exec_inner(node, estate)?;
                node.mj_MatchedInner = false;

                if tup_is_null(node.mj_InnerTupleSlot, estate) {
                    return Ok(None);
                }

                // Else remain in ENDOUTER state and process next tuple.
            }

            // EXEC_MJ_ENDINNER — out of inner tuples; null-fill remaining
            // unmatched outer tuples (left/full join).
            EXEC_MJ_ENDINNER => {
                debug_assert!(do_fill_outer);

                if !node.mj_MatchedOuter {
                    // Emit a fill tuple with nulls for the inner side.
                    node.mj_MatchedOuter = true; // do it only once
                    if MJFillOuter(node, estate)? {
                        return Ok(node.js.ps.ps_ResultTupleSlot);
                    }
                }

                // Get the next outer tuple, if any.
                node.mj_OuterTupleSlot = exec_outer(node, estate)?;
                node.mj_MatchedOuter = false;

                if tup_is_null(node.mj_OuterTupleSlot, estate) {
                    return Ok(None);
                }

                // Else remain in ENDINNER state and process next tuple.
            }

            other => {
                return Err(elog(alloc::format!("unrecognized mergejoin state: {other}")));
            }
        }
    }
}

/// `ExecProcNode(outerPlanState(node))` — fetch the next outer tuple.
fn exec_outer<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = node
        .js
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecMergeJoin: no outer plan state");
    execProcnode::exec_proc_node::call(outer, estate)
}

/// `ExecProcNode(innerPlanState(node))` — fetch the next inner tuple.
fn exec_inner<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let inner = node
        .js
        .ps
        .righttree
        .as_deref_mut()
        .expect("ExecMergeJoin: no inner plan state");
    execProcnode::exec_proc_node::call(inner, estate)
}

/// `ExecMarkPos(innerPlan)` — mark the inner child's current scan position.
fn exec_mark_pos_inner<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let inner = node
        .js
        .ps
        .righttree
        .as_deref_mut()
        .expect("ExecMarkPos: no inner plan state");
    execAmi::exec_mark_pos::call(inner, estate)
}

/// `ExecRestrPos(innerPlan)` — restore the inner child to its marked position.
fn exec_restr_pos_inner<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let inner = node
        .js
        .ps
        .righttree
        .as_deref_mut()
        .expect("ExecRestrPos: no inner plan state");
    execAmi::exec_restr_pos::call(inner, estate)
}

/// `ExecQual(node->js.joinqual, econtext)` over the node's regular econtext.
/// The econtext slot links (`ecxt_outertuple`/`ecxt_innertuple`) are set by the
/// caller (the EXEC_MJ_JOINTUPLES arm) before this runs, matching the C order.
/// The caller short-circuits a NULL joinqual.
fn exec_joinqual<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("exec_joinqual: ps_ExprContext not created");
    let joinqual = node
        .js
        .joinqual
        .as_deref_mut()
        .expect("exec_joinqual: joinqual present checked by caller");
    execExpr::exec_qual::call(joinqual, econtext, estate)
}

/// `ExecQual(node->js.ps.qual, econtext)` over the node's regular econtext (the
/// slot links are already set by the preceding `exec_joinqual`). The caller
/// short-circuits a NULL otherqual.
fn exec_otherqual<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .js
        .ps
        .ps_ExprContext
        .expect("exec_otherqual: ps_ExprContext not created");
    let otherqual = node
        .js
        .ps
        .qual
        .as_deref_mut()
        .expect("exec_otherqual: qual present checked by caller");
    execExpr::exec_qual::call(otherqual, econtext, estate)
}

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitMergeJoin`]:
/// `castNode(MergeJoinState, pstate)` then run [`ExecMergeJoin`].
fn exec_mergejoin_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::MergeJoin(node) => node,
        other => panic!("castNode(MergeJoinState, pstate) failed: {other:?}"),
    };
    ExecMergeJoin(node, estate)
}

// ===========================================================================
// ExecInitMergeJoin
// ===========================================================================

/// `ExecInitMergeJoin(node, estate, eflags)` — create and initialize the
/// merge-join run-time state.
pub fn ExecInitMergeJoin<'mcx>(
    plan_node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, MergeJoinStateData<'mcx>>> {
    // check for unsupported flags
    //   Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);

    let mcx = estate.es_query_cxt;

    let node: &'mcx MergeJoin<'mcx> = match plan_node {
        types_nodes::nodes::Node::MergeJoin(m) => m,
        other => panic!("castNode(MergeJoin, node) failed: {other:?}"),
    };

    // create state structure: makeNode(MergeJoinState)
    //   mergestate->js.ps.plan = (Plan *) node;
    //   mergestate->js.ps.state = estate;
    //   mergestate->js.ps.ExecProcNode = ExecMergeJoin;
    //   mergestate->js.jointype = node->join.jointype;
    //   mergestate->mj_ConstFalseJoin = false;
    let mut mergestate = alloc_in(mcx, MergeJoinStateData::new(mcx))?;
    mergestate.js.ps.plan = Some(plan_node);
    mergestate.js.ps.ExecProcNode = Some(exec_mergejoin_node);
    mergestate.js.jointype = node.join.jointype;
    mergestate.mj_ConstFalseJoin = false;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &mergestate->js.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut mergestate.js.ps)?;

    // We need two additional econtexts in which we can compute the join
    // expressions from the left and right input tuples.
    //   mergestate->mj_OuterEContext = CreateExprContext(estate);
    //   mergestate->mj_InnerEContext = CreateExprContext(estate);
    mergestate.mj_OuterEContext = Some(execUtils::create_expr_context::call(estate)?);
    mergestate.mj_InnerEContext = Some(execUtils::create_expr_context::call(estate)?);

    // initialize child nodes
    //   Assert(node->join.joinqual == NIL || !node->skip_mark_restore);
    debug_assert!(
        node.join.joinqual.as_ref().map_or(true, |q| q.is_empty())
            || !node.skip_mark_restore
    );
    mergestate.mj_SkipMarkRestore = node.skip_mark_restore;

    // outerPlanState(mergestate) = ExecInitNode(outerPlan(node), estate, eflags);
    let outer_plan = node.join.plan.lefttree.as_deref();
    mergestate.js.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;
    // outerDesc = ExecGetResultType(outerPlanState(mergestate)); copied into the
    // per-query context (C shares the child's TupleDesc pointer; the owned model
    // gives each consuming slot its own copy).
    let outer_desc: types_tuple::heaptuple::TupleDesc<'mcx> = {
        let outer = mergestate
            .js
            .ps
            .lefttree
            .as_deref()
            .expect("ExecInitMergeJoin: ExecInitNode(outer) returned None");
        match execTuples::exec_get_result_type::call(outer.ps_head()) {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        }
    };

    // innerPlanState(mergestate) = ExecInitNode(innerPlan(node), estate,
    //     mergestate->mj_SkipMarkRestore ? eflags : (eflags | EXEC_FLAG_MARK));
    let inner_eflags = if mergestate.mj_SkipMarkRestore {
        eflags
    } else {
        eflags | EXEC_FLAG_MARK
    };
    let inner_plan = node.join.plan.righttree.as_deref();
    mergestate.js.ps.righttree =
        execProcnode::exec_init_node::call(mcx, inner_plan, estate, inner_eflags)?;
    let inner_desc: types_tuple::heaptuple::TupleDesc<'mcx> = {
        let inner = mergestate
            .js
            .ps
            .righttree
            .as_deref()
            .expect("ExecInitMergeJoin: ExecInitNode(inner) returned None");
        match execTuples::exec_get_result_type::call(inner.ps_head()) {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        }
    };
    let inner_ops = {
        let inner = mergestate
            .js
            .ps
            .righttree
            .as_deref()
            .expect("ExecInitMergeJoin: ExecInitNode(inner) returned None");
        execTuples::exec_get_result_slot_ops::call(inner.ps_head())
    };

    /// clone an owned TupleDesc copy into `mcx` (each slot owns its copy).
    fn clone_desc<'mcx>(
        src: &types_tuple::heaptuple::TupleDesc<'mcx>,
        mcx: mcx::Mcx<'mcx>,
    ) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
        Ok(match src {
            Some(d) => Some(alloc_in(mcx, d.clone_in(mcx)?)?),
            None => None,
        })
    }

    // For certain inner child node types it is advantageous to issue MARK every
    // time we advance past an inner tuple we will never return to. Only
    // Material wants the extra MARKs, and only if eflags doesn't specify
    // REWIND. (For IndexScan/IndexOnlyScan we must NOT set mj_ExtraMarks.)
    let inner_is_material = matches!(
        node.join.plan.righttree.as_deref(),
        Some(types_nodes::nodes::Node::Material(_))
    );
    mergestate.mj_ExtraMarks = inner_is_material
        && (eflags & EXEC_FLAG_REWIND) == 0
        && !mergestate.mj_SkipMarkRestore;

    // Initialize result slot, type and projection.
    //   ExecInitResultTupleSlotTL(&mergestate->js.ps, &TTSOpsVirtual);
    //   ExecAssignProjectionInfo(&mergestate->js.ps, NULL);
    execTuples::exec_init_result_tuple_slot_tl::call(
        &mut mergestate.js.ps,
        estate,
        TupleSlotKind::Virtual,
    )?;
    execUtils::exec_assign_projection_info::call(&mut mergestate.js.ps, estate, None)?;

    // tuple table initialization
    //   innerOps = ExecGetResultSlotOps(innerPlanState(mergestate), NULL);
    //   mergestate->mj_MarkedTupleSlot =
    //       ExecInitExtraTupleSlot(estate, innerDesc, innerOps);
    mergestate.mj_MarkedTupleSlot =
        Some(execTuples::exec_init_extra_tuple_slot::call(estate, clone_desc(&inner_desc, mcx)?, inner_ops)?);

    // initialize child expressions
    //   mergestate->js.ps.qual = ExecInitQual(node->join.plan.qual, mergestate);
    //   mergestate->js.joinqual = ExecInitQual(node->join.joinqual, mergestate);
    let otherqual_list = node.join.plan.qual.as_deref();
    mergestate.js.ps.qual =
        execExpr::exec_init_qual::call(otherqual_list, &mut mergestate.js.ps, estate)?;
    let joinqual_list = node.join.joinqual.as_deref();
    mergestate.js.joinqual =
        execExpr::exec_init_qual::call(joinqual_list, &mut mergestate.js.ps, estate)?;
    // mergeclauses are handled below

    // detect whether we need only consider the first matching inner tuple
    //   mergestate->js.single_match =
    //       (node->join.inner_unique || node->join.jointype == JOIN_SEMI);
    mergestate.js.single_match = node.join.inner_unique || node.join.jointype == JOIN_SEMI;

    // set up null tuples for outer joins, if needed
    match node.join.jointype {
        JOIN_INNER | JOIN_SEMI => {
            mergestate.mj_FillOuter = false;
            mergestate.mj_FillInner = false;
        }
        JOIN_LEFT | JOIN_ANTI => {
            mergestate.mj_FillOuter = true;
            mergestate.mj_FillInner = false;
            mergestate.mj_NullInnerTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&inner_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);
        }
        JOIN_RIGHT | JOIN_RIGHT_ANTI => {
            mergestate.mj_FillOuter = false;
            mergestate.mj_FillInner = true;
            mergestate.mj_NullOuterTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&outer_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);

            // Can't handle right/right-anti/full join with non-constant extra
            // joinclauses. This should have been caught by planner.
            let mut const_false = mergestate.mj_ConstFalseJoin;
            let ok = check_constant_qual(node.join.joinqual.as_deref(), &mut const_false);
            mergestate.mj_ConstFalseJoin = const_false;
            if !ok {
                return Err(feature(
                    "RIGHT JOIN is only supported with merge-joinable join conditions",
                ));
            }
        }
        JOIN_FULL => {
            mergestate.mj_FillOuter = true;
            mergestate.mj_FillInner = true;
            mergestate.mj_NullOuterTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&outer_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);
            mergestate.mj_NullInnerTupleSlot = Some(execTuples::exec_init_null_tuple_slot::call(
                estate,
                clone_desc(&inner_desc, mcx)?,
                TupleSlotKind::Virtual,
            )?);

            // Can't handle right/right-anti/full join with non-constant extra
            // joinclauses. This should have been caught by planner.
            let mut const_false = mergestate.mj_ConstFalseJoin;
            let ok = check_constant_qual(node.join.joinqual.as_deref(), &mut const_false);
            mergestate.mj_ConstFalseJoin = const_false;
            if !ok {
                return Err(feature(
                    "FULL JOIN is only supported with merge-joinable join conditions",
                ));
            }
        }
        other => {
            return Err(elog(alloc::format!("unrecognized join type: {}", other as u32)));
        }
    }

    // preprocess the merge clauses
    //   mergestate->mj_NumClauses = list_length(node->mergeclauses);
    //   mergestate->mj_Clauses = MJExamineQuals(...);
    mergestate.mj_NumClauses = node.mergeclauses.len() as i32;
    MJExamineQuals(&mut mergestate, node, estate)?;

    // initialize join state
    mergestate.mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    mergestate.mj_MatchedOuter = false;
    mergestate.mj_MatchedInner = false;
    mergestate.mj_OuterTupleSlot = None;
    mergestate.mj_InnerTupleSlot = None;

    Ok(mergestate)
}

// ===========================================================================
// ExecEndMergeJoin — shut down the subplans.
// ===========================================================================
pub fn ExecEndMergeJoin<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecEndNode(innerPlanState(node));
    if let Some(inner) = node.js.ps.righttree.as_deref_mut() {
        execProcnode::exec_end_node::call(inner, estate)?;
    }
    // ExecEndNode(outerPlanState(node));
    if let Some(outer) = node.js.ps.lefttree.as_deref_mut() {
        execProcnode::exec_end_node::call(outer, estate)?;
    }
    Ok(())
}

// ===========================================================================
// ExecReScanMergeJoin — rescan the merge-join node.
// ===========================================================================
pub fn ExecReScanMergeJoin<'mcx>(
    node: &mut MergeJoinStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExecClearTuple(node->mj_MarkedTupleSlot);
    if let Some(marked) = node.mj_MarkedTupleSlot {
        execTuples::exec_clear_tuple::call(estate.slot_mut(marked))?;
    }

    node.mj_JoinState = EXEC_MJ_INITIALIZE_OUTER;
    node.mj_MatchedOuter = false;
    node.mj_MatchedInner = false;
    node.mj_OuterTupleSlot = None;
    node.mj_InnerTupleSlot = None;

    // if chgParam of subnodes is not null then plans will be re-scanned by
    // first ExecProcNode.
    let outer_chg_null = node
        .js
        .ps
        .lefttree
        .as_deref()
        .map(|p| p.ps_head().chgParam.is_none())
        .unwrap_or(true);
    if outer_chg_null {
        if let Some(outer) = node.js.ps.lefttree.as_deref_mut() {
            execAmi::exec_re_scan::call(outer, estate)?;
        }
    }
    let inner_chg_null = node
        .js
        .ps
        .righttree
        .as_deref()
        .map(|p| p.ps_head().chgParam.is_none())
        .unwrap_or(true);
    if inner_chg_null {
        if let Some(inner) = node.js.ps.righttree.as_deref_mut() {
            execAmi::exec_re_scan::call(inner, estate)?;
        }
    }
    Ok(())
}

extern crate alloc;

#[cfg(test)]
mod tests;
