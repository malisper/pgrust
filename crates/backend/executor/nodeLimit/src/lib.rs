//! Port of `src/backend/executor/nodeLimit.c` — routines to handle limiting of
//! query results where appropriate.
//!
//! INTERFACE ROUTINES
//! - [`ExecLimit`]       - extract a limited range of tuples
//! - [`ExecInitLimit`]   - initialize node and subnodes
//! - [`ExecEndLimit`]    - shutdown node and subnodes
//! - [`ExecReScanLimit`] - rescan the node
//!
//! A Limit node performs LIMIT/OFFSET filtering on the stream of tuples returned
//! by a subplan. The bulk of the file is the [`ExecLimit`] state machine over the
//! owned [`LimitStateData`] (`execnodes.h`'s genuine `LimitState`), mutated
//! through `&mut`. There are no opaque handles: `subSlot` / `last_slot` are arena
//! `SlotId`s into `estate.es_tupleTable` (a C `TupleTableSlot *` that may be NULL
//! or `TTS_EMPTY` is `Option<SlotId>` normalized by [`fetch_subplan_tuple`]).
//!
//! Everything below the node layer is reached through per-owner seam crates
//! (execProcnode/execTuples/execUtils/execAmi for child dispatch and slot/expr
//! setup, execExpr for expression compile+eval, execGrouping for the WITH-TIES
//! equality `ExprState`, tcop/postgres for `CHECK_FOR_INTERRUPTS`). The plain
//! `LIMIT n` / `OFFSET n` path runs end-to-end; the WITH-TIES equality machinery
//! routes through its owners' seams (execGrouping/execExpr), which panic loudly
//! until those owners install real implementations.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use execAmi_seams as execAmi;
use execExpr_seams as execExpr;
use execGrouping_seams as execGrouping;
use execProcnode_seams as execProcnode;
use execTuples_seams as execTuples;
use execUtils_seams as execUtils;
use postgres_seams as tcop_postgres;

use mcx::{alloc_in, PgBox};
use types_core::int64;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_error::{
    ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE, ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
};
use ::nodes::execnodes::ScanDirectionIsForward;
use ::nodes::executor::EXEC_FLAG_MARK;
use ::nodes::nodelimit::{
    Limit, LimitStateData, LimitStateCond, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES,
};
use nodes::{EStateData, PlanStateNode, SlotId};

/// Install this crate's seam implementations.
///
/// nodeLimit owns no `*-seams` crate: nodeLimit.c is a leaf executor node whose
/// interface routines (`ExecInitLimit`/`ExecEndLimit`/`ExecReScanLimit` and the
/// `ExecLimit` callback) are dispatched by the executor (execProcnode.c /
/// execAmi.c) through their own already-declared per-node-tag switches — those
/// dispatch crates depend on this crate directly and call these functions, which
/// does not cycle (this crate depends only on `*-seams` crates, never on the
/// dispatch crates themselves). There are therefore no inward seams to install,
/// and `init_seams()` is empty (invoked by `seams-init::init_all()`).
pub fn init_seams() {}

// ===========================================================================
// ExecLimit — the PlanState.ExecProcNode callback (state machine).
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitLimit`]:
/// `castNode(LimitState, pstate)` then run [`ExecLimit`].
fn exec_limit_node<'mcx>(
    pstate: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate {
        PlanStateNode::Limit(node) => node,
        other => panic!("castNode(LimitState, pstate) failed: {other:?}"),
    };
    ExecLimit(node, estate)
}

/// `ExecLimit(pstate)` — extract a limited range of tuples.
///
/// This is a very simple node which just performs LIMIT/OFFSET filtering on the
/// stream of tuples returned by a subplan. The main logic is a simple state
/// machine. Ported 1:1 from `ExecLimit` in nodeLimit.c.
///
/// Returns `Ok(None)` for the C `return NULL` paths and `Ok(Some(id))` for the
/// "return the current tuple" tail (C returns the same `TupleTableSlot *` the
/// subplan handed up — the arena id held in `node.subSlot`).
pub fn ExecLimit<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    // get information from the node
    //   direction = node->ps.state->es_direction;
    //   outerPlan = outerPlanState(node);
    let forward = ScanDirectionIsForward(estate.es_direction);

    // The main logic is a simple state machine.
    //
    // Rust has no C fall-through, so the C `switch` (which has several FALL THRU
    // edges, all proceeding to the textually-next case) is modeled with a loop
    // over the current `lstate`: each arm either returns directly, `break
    // 'state`s to the "return current tuple" tail with the chosen slot left in
    // `node.subSlot`, or `continue`s to re-dispatch on a freshly-set `lstate`
    // (the C `/* FALL THRU */` edges).
    'state: loop {
        match node.lstate {
            LimitStateCond::LIMIT_INITIAL => {
                // First call for this node, so compute limit/offset. (We can't
                // do this any earlier, because parameters from upper nodes will
                // not be set during ExecInitLimit.) This also sets position = 0
                // and changes the state to LIMIT_RESCAN.
                recompute_limits(node, estate)?;

                // FALL THRU -> LIMIT_RESCAN
                continue 'state;
            }

            LimitStateCond::LIMIT_RESCAN => {
                // If backwards scan, just return NULL without changing state.
                if !forward {
                    return Ok(None);
                }

                // Check for empty window; if so, treat like empty subplan.
                if node.count <= 0 && !node.noCount {
                    node.lstate = LimitStateCond::LIMIT_EMPTY;
                    return Ok(None);
                }

                // Fetch rows from subplan until we reach position > offset.
                loop {
                    let Some(s) = fetch_subplan_tuple(node, estate)? else {
                        // The subplan returns too few tuples for us to produce
                        // any output at all.
                        node.lstate = LimitStateCond::LIMIT_EMPTY;
                        return Ok(None);
                    };

                    // Tuple at limit is needed for comparison in subsequent
                    // execution to detect ties.
                    if node.limitOption == LIMIT_OPTION_WITH_TIES
                        && node.position - node.offset == node.count - 1
                    {
                        copy_last_slot(node, s, estate)?;
                    }
                    node.subSlot = Some(s);
                    node.position += 1;
                    if node.position > node.offset {
                        break;
                    }
                }

                // Okay, we have the first tuple of the window.
                node.lstate = LimitStateCond::LIMIT_INWINDOW;
                break 'state;
            }

            LimitStateCond::LIMIT_EMPTY => {
                // The subplan is known to return no tuples (or not more than
                // OFFSET tuples, in general). So we return no tuples.
                return Ok(None);
            }

            LimitStateCond::LIMIT_INWINDOW => {
                if forward {
                    // Forwards scan, so check for stepping off end of window. At
                    // the end of the window, the behavior depends on whether WITH
                    // TIES was specified: if so, change the state machine to
                    // WINDOWEND_TIES and fall through to that case. If not, return
                    // NULL without advancing the subplan or the position
                    // variable, but change the state machine to record having
                    // done so.
                    if !node.noCount && node.position - node.offset >= node.count {
                        if node.limitOption == LIMIT_OPTION_COUNT {
                            node.lstate = LimitStateCond::LIMIT_WINDOWEND;
                            return Ok(None);
                        } else {
                            node.lstate = LimitStateCond::LIMIT_WINDOWEND_TIES;
                            // we'll fall through to the next case
                        }
                    } else {
                        // Get next tuple from subplan, if any.
                        let Some(s) = fetch_subplan_tuple(node, estate)? else {
                            node.lstate = LimitStateCond::LIMIT_SUBPLANEOF;
                            return Ok(None);
                        };

                        // If WITH TIES is active, and this is the last in-window
                        // tuple, save it to be used in subsequent WINDOWEND_TIES
                        // processing.
                        if node.limitOption == LIMIT_OPTION_WITH_TIES
                            && node.position - node.offset == node.count - 1
                        {
                            copy_last_slot(node, s, estate)?;
                        }
                        node.subSlot = Some(s);
                        node.position += 1;
                        break 'state;
                    }
                } else {
                    // Backwards scan, so check for stepping off start of window.
                    // As above, only change state-machine status if so.
                    if node.position <= node.offset + 1 {
                        node.lstate = LimitStateCond::LIMIT_WINDOWSTART;
                        return Ok(None);
                    }

                    // Get previous tuple from subplan; there should be one!
                    let Some(s) = fetch_subplan_tuple(node, estate)? else {
                        return Err(impossible_backwards());
                    };
                    node.subSlot = Some(s);
                    node.position -= 1;
                    break 'state;
                }

                // Assert(node->lstate == LIMIT_WINDOWEND_TIES);
                debug_assert!(node.lstate == LimitStateCond::LIMIT_WINDOWEND_TIES);
                // FALL THRU -> LIMIT_WINDOWEND_TIES
                continue 'state;
            }

            LimitStateCond::LIMIT_WINDOWEND_TIES => {
                if forward {
                    // Advance the subplan until we find the first row with
                    // different ORDER BY pathkeys.
                    let Some(s) = fetch_subplan_tuple(node, estate)? else {
                        node.lstate = LimitStateCond::LIMIT_SUBPLANEOF;
                        return Ok(None);
                    };

                    // Test if the new tuple and the last tuple match. If so we
                    // return the tuple.
                    //   econtext->ecxt_innertuple = slot;
                    //   econtext->ecxt_outertuple = node->last_slot;
                    //   if (ExecQualAndReset(node->eqfunction, econtext)) ...
                    if ties_match(node, s, estate)? {
                        node.subSlot = Some(s);
                        node.position += 1;
                    } else {
                        node.lstate = LimitStateCond::LIMIT_WINDOWEND;
                        return Ok(None);
                    }
                } else {
                    // Backwards scan, so check for stepping off start of window.
                    // Change only state-machine status if so.
                    if node.position <= node.offset + 1 {
                        node.lstate = LimitStateCond::LIMIT_WINDOWSTART;
                        return Ok(None);
                    }

                    // Get previous tuple from subplan; there should be one! And
                    // change state-machine status.
                    let Some(s) = fetch_subplan_tuple(node, estate)? else {
                        return Err(impossible_backwards());
                    };
                    node.subSlot = Some(s);
                    node.position -= 1;
                    node.lstate = LimitStateCond::LIMIT_INWINDOW;
                }
                break 'state;
            }

            LimitStateCond::LIMIT_SUBPLANEOF => {
                if forward {
                    return Ok(None);
                }

                // Backing up from subplan EOF, so re-fetch previous tuple; there
                // should be one! Note previous tuple must be in window.
                let Some(s) = fetch_subplan_tuple(node, estate)? else {
                    return Err(impossible_backwards());
                };
                node.subSlot = Some(s);
                node.lstate = LimitStateCond::LIMIT_INWINDOW;
                // position does not change 'cause we didn't advance it before
                break 'state;
            }

            LimitStateCond::LIMIT_WINDOWEND => {
                if forward {
                    return Ok(None);
                }

                // We already past one position to detect ties so re-fetch
                // previous tuple; there should be one! Note previous tuple must
                // be in window.
                if node.limitOption == LIMIT_OPTION_WITH_TIES {
                    let Some(s) = fetch_subplan_tuple(node, estate)? else {
                        return Err(impossible_backwards());
                    };
                    node.subSlot = Some(s);
                    node.lstate = LimitStateCond::LIMIT_INWINDOW;
                } else {
                    // Backing up from window end: simply re-return the last tuple
                    // fetched from the subplan (already in node.subSlot).
                    //   slot = node->subSlot;
                    node.lstate = LimitStateCond::LIMIT_INWINDOW;
                    // position does not change 'cause we didn't advance it before
                }
                break 'state;
            }

            LimitStateCond::LIMIT_WINDOWSTART => {
                // if (!ScanDirectionIsForward(direction)) return NULL;
                if !forward {
                    return Ok(None);
                }

                // Advancing after having backed off window start: simply
                // re-return the last tuple fetched from the subplan (already in
                // node.subSlot).
                //   slot = node->subSlot;
                node.lstate = LimitStateCond::LIMIT_INWINDOW;
                // position does not change 'cause we didn't change it before
                break 'state;
            }
        }
    }

    // Return the current tuple.
    //   Assert(!TupIsNull(slot));
    //   return slot;
    // The chosen tuple's arena id is in `node.subSlot`; the C returns the same
    // slot pointer the node holds.
    debug_assert!(node.subSlot.is_some(), "ExecLimit: Assert(!TupIsNull(slot))");
    Ok(node.subSlot)
}

/// `slot = ExecProcNode(outerPlan)` + `TupIsNull(slot)` normalization: pull the
/// next tuple from the child through the central dispatch seam and fold the C
/// `TupIsNull` (NULL pointer OR `TTS_EMPTY` slot) into `None`.
fn fetch_subplan_tuple<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let outer = node
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(missing_outer_plan_state)?;
    let slot = execProcnode::exec_proc_node::call(outer, estate)?;
    Ok(match slot {
        Some(id) if !estate.slot(id).is_empty() => Some(id),
        _ => None,
    })
}

/// `ExecCopySlot(node->last_slot, slot)` — datum-content copy of the tuple in
/// arena slot `src` into the node's persistent `last_slot`, remembering the last
/// in-window tuple for subsequent tie detection. Only reached on a WITH-TIES proc
/// path.
fn copy_last_slot<'mcx>(
    node: &mut LimitStateData<'mcx>,
    src: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // node->last_slot is set during ExecInitLimit on the WITH-TIES path.
    let dst = node
        .last_slot
        .expect("ExecCopySlot: WITH-TIES last_slot must be initialized");
        execTuples::exec_copy_slot::call(estate, dst, src)
}

/// The WITH-TIES tie test, mirroring the C:
///   econtext->ecxt_innertuple = slot;
///   econtext->ecxt_outertuple = node->last_slot;
///   return ExecQualAndReset(node->eqfunction, econtext);
/// Returns whether the candidate `inner` row ties the saved last in-window row.
/// Only reached on a WITH-TIES proc path.
fn ties_match<'mcx>(
    node: &mut LimitStateData<'mcx>,
    inner: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecLimit: ps_ExprContext must be set (ExecAssignExprContext ran in init)");
    let last_slot = node
        .last_slot
        .expect("ExecLimit: WITH-TIES last_slot must be initialized");
    {
        let ec = estate.ecxt_mut(econtext);
        ec.ecxt_innertuple = Some(inner);
        ec.ecxt_outertuple = Some(last_slot);
    }
    // node->eqfunction is set during ExecInitLimit on the WITH-TIES path; a
    // zero-column key compiles to None (C NULL), which never reaches here
    // because WITH-TIES always carries uniqNumCols > 0.
    let eqfunction = node
        .eqfunction
        .as_deref_mut()
        .expect("ExecLimit: WITH-TIES eqfunction must be initialized");
    execExpr::exec_qual_and_reset::call(eqfunction, econtext, estate)
}

/// `recompute_limits(node)` — evaluate the limit/offset expressions, done at
/// startup or rescan. This is also a handy place to reset the current-position
/// state info. Ported 1:1 from nodeLimit.c.
fn recompute_limits<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // ExprContext *econtext = node->ps.ps_ExprContext;
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("recompute_limits: ps_ExprContext must be set (ExecAssignExprContext ran in init)");

    if let Some(limit_offset) = node.limitOffset.as_deref_mut() {
        // val = ExecEvalExprSwitchContext(node->limitOffset, econtext, &isNull);
        let (val, is_null) =
            execExpr::exec_eval_expr_switch_context::call(limit_offset, econtext, estate)?;
        // Interpret NULL offset as no offset.
        if is_null {
            node.offset = 0;
        } else {
            // node->offset = DatumGetInt64(val);
            node.offset = val.as_i64();
            if node.offset < 0 {
                // ereport(ERROR,
                //   (errcode(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
                //    errmsg("OFFSET must not be negative")));
                return Err(PgError::error("OFFSET must not be negative")
                    .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE));
            }
        }
    } else {
        // No OFFSET supplied.
        node.offset = 0;
    }

    if let Some(limit_count) = node.limitCount.as_deref_mut() {
        // val = ExecEvalExprSwitchContext(node->limitCount, econtext, &isNull);
        let (val, is_null) =
            execExpr::exec_eval_expr_switch_context::call(limit_count, econtext, estate)?;
        // Interpret NULL count as no count (LIMIT ALL).
        if is_null {
            node.count = 0;
            node.noCount = true;
        } else {
            // node->count = DatumGetInt64(val);
            node.count = val.as_i64();
            if node.count < 0 {
                // ereport(ERROR,
                //   (errcode(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
                //    errmsg("LIMIT must not be negative")));
                return Err(PgError::error("LIMIT must not be negative")
                    .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE));
            }
            node.noCount = false;
        }
    } else {
        // No COUNT supplied.
        node.count = 0;
        node.noCount = true;
    }

    // Reset position to start-of-scan.
    node.position = 0;
    node.subSlot = None;

    // Set state-machine state.
    node.lstate = LimitStateCond::LIMIT_RESCAN;

    // Notify child node about limit. Note: think not to "optimize" by skipping
    // ExecSetTupleBound if compute_tuples_needed returns < 0. We must update the
    // child node anyway, in case this is a rescan and the previous time we got a
    // different result.
    //   ExecSetTupleBound(compute_tuples_needed(node), outerPlanState(node));
    let tuples_needed = compute_tuples_needed(node);
    let outer = node
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(missing_outer_plan_state)?;
    execProcnode::exec_set_tuple_bound::call(tuples_needed, outer, estate)
}

/// `compute_tuples_needed(node)` — compute the maximum number of tuples needed to
/// satisfy this Limit node. Returns a negative value if there is not a
/// determinable limit. Ported 1:1 from nodeLimit.c.
fn compute_tuples_needed(node: &LimitStateData<'_>) -> int64 {
    // if ((node->noCount) || (node->limitOption == LIMIT_OPTION_WITH_TIES))
    //     return -1;
    if node.noCount || node.limitOption == LIMIT_OPTION_WITH_TIES {
        return -1;
    }
    // Note: if this overflows, we'll return a negative value, which is OK.
    //   return node->count + node->offset;
    node.count.wrapping_add(node.offset)
}

// ===========================================================================
// ExecInitLimit / ExecEndLimit / ExecReScanLimit.
// ===========================================================================

/// `ExecInitLimit(node, estate, eflags)` — initialize the limit node state
/// structures and the node's subplan. Ported 1:1 from nodeLimit.c.
///
/// Allocated in `estate.es_query_cxt` (C: `makeNode` in the per-query context),
/// so fallible on OOM. Panics if `node` is not a `Limit` (the C `castNode`).
pub fn ExecInitLimit<'mcx>(
    node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<PgBox<'mcx, LimitStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let limit: &'mcx Limit<'mcx> = node.expect_limit();

    // check for unsupported flags
    //   Assert(!(eflags & EXEC_FLAG_MARK));
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // create state structure
    //   limitstate = makeNode(LimitState);
    //   limitstate->ps.plan = (Plan *) node;
    //   limitstate->ps.state = estate;
    //   limitstate->ps.ExecProcNode = ExecLimit;
    //   limitstate->lstate = LIMIT_INITIAL;
    let mut limitstate = alloc_in(mcx, LimitStateData::default())?;
    limitstate.ps.plan = Some(node);
    limitstate.ps.ExecProcNode = Some(exec_limit_node);
    limitstate.lstate = LimitStateCond::LIMIT_INITIAL;

    // Miscellaneous initialization
    //
    // Limit nodes never call ExecQual or ExecProject, but they need an
    // exprcontext anyway to evaluate the limit/offset parameters in.
    //   ExecAssignExprContext(estate, &limitstate->ps);
    execUtils::exec_assign_expr_context::call(estate, &mut limitstate.ps)?;

    // initialize outer plan
    //   outerPlan = outerPlan(node);
    //   outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);
    let outer_plan = limit.plan.lefttree.as_deref();
    limitstate.ps.lefttree = execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // initialize child expressions
    //   limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset,
    //                                           (PlanState *) limitstate);
    //   limitstate->limitCount  = ExecInitExpr((Expr *) node->limitCount,
    //                                           (PlanState *) limitstate);
    //   limitstate->limitOption = node->limitOption;
    if let Some(off) = limit.limitOffset.as_deref() {
        limitstate.limitOffset =
            Some(execExpr::exec_init_expr::call(off, &mut limitstate.ps, estate)?);
    }
    if let Some(cnt) = limit.limitCount.as_deref() {
        limitstate.limitCount =
            Some(execExpr::exec_init_expr::call(cnt, &mut limitstate.ps, estate)?);
    }
    limitstate.limitOption = limit.limitOption;

    // Initialize result type.
    //   ExecInitResultTypeTL(&limitstate->ps);
    execTuples::exec_init_result_type_tl::call(&mut limitstate.ps, estate)?;

    //   limitstate->ps.resultopsset = true;
    //   limitstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(limitstate),
    //                                                    &limitstate->ps.resultopsfixed);
    limitstate.ps.resultopsset = true;
    let (child_ops, child_fixed) = {
        let outer = limitstate
            .ps
            .lefttree
            .as_deref()
            .ok_or_else(missing_outer_plan_state)?;
        // ExecGetResultSlotOps(outer, &resultopsfixed) — the full C function
        // (incl. the ps_ResultTupleSlot TTS_FIXED fallback for !resultopsset),
        // returning both the ops and the isfixed out-flag.
        execUtils::exec_get_result_slot_ops_isfixed::call(outer.ps_head(), estate)
    };
    limitstate.ps.resultops = Some(child_ops);
    limitstate.ps.resultopsfixed = child_fixed;

    // limit nodes do no projections, so initialize projection info for this node
    // appropriately
    //   limitstate->ps.ps_ProjInfo = NULL;
    limitstate.ps.ps_ProjInfo = None;

    // Initialize the equality evaluation, to detect ties.
    if limit.limitOption == LIMIT_OPTION_WITH_TIES {
        // desc = ExecGetResultType(outerPlanState(limitstate));
        // ops  = ExecGetResultSlotOps(outerPlanState(limitstate), NULL);
        // limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc, ops);
        let (desc, ops) = {
            let outer = limitstate
                .ps
                .lefttree
                .as_deref()
                .ok_or_else(missing_outer_plan_state)?;
            let head = outer.ps_head();
            let desc = Some(alloc_in(
                mcx,
                execTuples::exec_get_result_type::call(head)
                    .ok_or_else(missing_result_type)?
                    .clone_in(mcx)?,
            )?);
            (desc, execTuples::exec_get_result_slot_ops::call(head))
        };
        limitstate.last_slot = Some(execTuples::exec_init_extra_tuple_slot::call(estate, desc, ops)?);

        // limitstate->eqfunction = execTuplesMatchPrepare(desc, node->uniqNumCols,
        //     node->uniqColIdx, node->uniqOperators, node->uniqCollations,
        //     &limitstate->ps);
        let uniq_col_idx = limit.uniqColIdx.as_deref().unwrap_or(&[]);
        let uniq_operators = limit.uniqOperators.as_deref().unwrap_or(&[]);
        let uniq_collations = limit.uniqCollations.as_deref().unwrap_or(&[]);
        // The descriptor must be re-derived: it was moved into the slot above.
        let desc = {
            let outer = limitstate
                .ps
                .lefttree
                .as_deref()
                .ok_or_else(missing_outer_plan_state)?;
            Some(alloc_in(
                mcx,
                execTuples::exec_get_result_type::call(outer.ps_head())
                    .ok_or_else(missing_result_type)?
                    .clone_in(mcx)?,
            )?)
        };
        limitstate.eqfunction = execGrouping::exec_tuples_match_prepare::call(
            desc,
            limit.uniqNumCols,
            uniq_col_idx,
            uniq_operators,
            uniq_collations,
            &mut limitstate.ps,
            estate,
        )?;
    }

    Ok(limitstate)
}

/// `ExecEndLimit(node)` — shut down the subplan and free resources allocated to
/// this node. Ported 1:1 from nodeLimit.c.
///
/// ```c
/// void
/// ExecEndLimit(LimitState *node)
/// {
///     ExecEndNode(outerPlanState(node));
/// }
/// ```
pub fn ExecEndLimit<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let outer = node
        .ps
        .lefttree
        .as_deref_mut()
        .ok_or_else(missing_outer_plan_state)?;
    execProcnode::exec_end_node::call(outer, estate)
}

/// `ExecReScanLimit(node)` — rescan the node. Ported 1:1 from nodeLimit.c.
///
/// ```c
/// void
/// ExecReScanLimit(LimitState *node)
/// {
///     PlanState  *outerPlan = outerPlanState(node);
///
///     recompute_limits(node);
///
///     if (outerPlan->chgParam == NULL)
///         ExecReScan(outerPlan);
/// }
/// ```
pub fn ExecReScanLimit<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Recompute limit/offset in case parameters changed, and reset the state
    // machine. We must do this before rescanning our child node, in case it's a
    // Sort that we are passing the parameters down to.
    recompute_limits(node, estate)?;

    // if chgParam of subnode is not null then plan will be re-scanned by first
    // ExecProcNode.
    //   if (outerPlan->chgParam == NULL) ExecReScan(outerPlan);
    let outer_chgparam_present = node
        .ps
        .lefttree
        .as_deref()
        .ok_or_else(missing_outer_plan_state)?
        .ps_head()
        .chgParam
        .is_some();
    if !outer_chgparam_present {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .ok_or_else(missing_outer_plan_state)?;
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    Ok(())
}

// ===========================================================================
// In-crate node-layer helpers (internal-error ereports).
// ===========================================================================

/// `elog(ERROR, "LIMIT subplan failed to run backwards")` — the impossible
/// backwards path. In C this is `elog(ERROR, ...)`, which carries
/// `ERRCODE_INTERNAL_ERROR`.
fn impossible_backwards() -> PgError {
    PgError::error("LIMIT subplan failed to run backwards").with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `outerPlanState(node)` was NULL — every Limit plan must have an outer subplan;
/// dereferencing a NULL `outerPlanState` in C would crash. Surfaced as an
/// internal error rather than a panic so the executor's error path can run.
fn missing_outer_plan_state() -> PgError {
    PgError::error("ExecLimit: Limit node has no outer (lefttree) subplan")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `ExecGetResultType(outerPlanState(node))` returned no descriptor.
fn missing_result_type() -> PgError {
    PgError::error("ExecInitLimit: outer plan has no result tuple descriptor")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}
