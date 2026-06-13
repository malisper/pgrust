//! Port of `src/backend/executor/nodeLimit.c` — the executor node that performs
//! LIMIT/OFFSET filtering on the tuple stream returned by its subplan.
//!
//! INTERFACE ROUTINES
//! - [`ExecLimit`]       - extract a limited range of tuples (the
//!   `ExecProcNode` callback)
//! - [`ExecInitLimit`]   - initialize node and subnodes
//! - [`ExecEndLimit`]    - shutdown node and subnodes
//! - [`ExecReScanLimit`] - rescan the node
//!
//! The node is held as an owned [`LimitStateData`] mutated through `&mut`; the
//! C `PlanState.state` back-pointer is replaced by threading `&mut EStateData`
//! explicitly. A C `TupleTableSlot *` that may be NULL or `TTS_EMPTY`
//! (`TupIsNull`) is modeled as `Option<SlotId>`: `None` is the C NULL pointer,
//! and a `Some(id)` whose resolved slot is empty is the `TTS_EMPTY` half;
//! [`fetch_subplan_tuple`] normalizes both to `None`.
//!
//! Calls into unported owners (execProcnode.c's `ExecInitNode` /
//! `ExecProcNode` / `ExecEndNode` / `ExecSetTupleBound`, execTuples.c's
//! `ExecInitResultTypeTL` / `ExecCopySlot` / `ExecInitExtraTupleSlot`,
//! execExpr/execExprInterp's `ExecInitExpr` / `ExecEvalExprSwitchContext` /
//! `ExecQualAndReset`, execGrouping.c's `execTuplesMatchPrepare`, execAmi.c's
//! `ExecReScan`, tcop/postgres.c's `CHECK_FOR_INTERRUPTS`) go through those
//! owners' seam crates and panic until the owners land. `ExecAssignExprContext`
//! / `ExecGetResultType` / `ExecGetResultSlotOps` are direct calls into the
//! ported `backend-executor-execUtils`.

#![allow(non_snake_case)]

use backend_executor_execAmi_seams as execAmi;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execGrouping_seams as execGrouping;
use backend_executor_execProcnode_seams as execProcnode;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;

use mcx::alloc_in;
use types_core::int64;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
    ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
};
use types_nodes::execnodes::ScanDirectionIsForward;
use types_nodes::executor::EXEC_FLAG_MARK;
use types_nodes::nodelimit::{
    Limit, LimitStateCond, LimitStateData, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES,
};
use types_nodes::{EStateData, PlanStateData, PlanStateNode, SlotId};

/// Install this crate's seam implementations.
///
/// nodeLimit has no `<unit>-seams` crate: callers that will need these
/// functions (execProcnode's dispatch tables) can depend on this crate
/// directly without a cycle, since this crate reaches outward only through
/// per-owner seam crates and the ported execUtils.
pub fn init_seams() {}

// ===========================================================================
// ExecLimit — the PlanState.ExecProcNode callback (state machine).
// ===========================================================================

/// `ExecLimit(pstate)` — extract a limited range of tuples.
///
/// This is a very simple node which just performs LIMIT/OFFSET filtering on the
/// stream of tuples returned by a subplan. The main logic is a simple state
/// machine.
///
/// Returns `Ok(None)` for the C `return NULL` paths and `Ok(Some(id))` for
/// "return the current tuple" (the slot id the subplan handed up; C returns the
/// same `TupleTableSlot *`).
pub fn ExecLimit<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    tcop_postgres::check_for_interrupts::call()?;

    // get information from the node
    //   direction = node->ps.state->es_direction;
    let forward = ScanDirectionIsForward(estate.es_direction);

    // The main logic is a simple state machine.
    //
    // Rust has no C fall-through, so the C `switch` (which has several FALL THRU
    // edges, all proceeding to the textually-next case) is modeled with a loop
    // over the current `lstate`. Each arm either returns directly, `break`s to
    // the "return current tuple" tail with the chosen slot in `node.subSlot`, or
    // `continue`s to re-dispatch on a freshly-set `lstate` (the C FALL THRU
    // edges).
    'state: loop {
        match node.lstate {
            LimitStateCond::LIMIT_INITIAL => {
                // First call for this node, so compute limit/offset. (We can't do
                // this any earlier, because parameters from upper nodes will not
                // be set during ExecInitLimit.) This also sets position = 0 and
                // changes the state to LIMIT_RESCAN.
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
                    // WINDOWEND_TIES and fall through to that case. If not
                    // (nothing was specified, or ONLY was) return NULL without
                    // advancing the subplan or the position, but change the state
                    // machine to record having done so.
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
                    node.lstate = LimitStateCond::LIMIT_INWINDOW;
                    // position does not change 'cause we didn't advance it before
                }
                break 'state;
            }

            LimitStateCond::LIMIT_WINDOWSTART => {
                // C: `if (!ScanDirectionIsForward(direction)) return NULL;`
                if !forward {
                    return Ok(None);
                }

                // Advancing after having backed off window start: simply
                // re-return the last tuple fetched from the subplan (already in
                // node.subSlot).
                node.lstate = LimitStateCond::LIMIT_INWINDOW;
                // position does not change 'cause we didn't change it before
                break 'state;
            }
        }
    }

    // Return the current tuple.
    //   Assert(!TupIsNull(slot));
    // The chosen tuple's slot id is in `node.subSlot`; the C returns the same
    // slot pointer the node holds, so hand back the same id. The C `Assert`
    // guards the WINDOWEND/WINDOWSTART re-return paths, which require a live
    // `subSlot` from an earlier window step.
    debug_assert!(
        node.subSlot.is_some(),
        "ExecLimit reached return tail with NULL subSlot"
    );
    Ok(node.subSlot)
}

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
        .expect("ExecLimit: Limit has no outer plan state");
    Ok(match execProcnode::exec_proc_node::call(outer, estate)? {
        Some(id) if !estate.slot(id).is_empty() => Some(id),
        _ => None,
    })
}

/// `ExecCopySlot(node->last_slot, slot)` — save the boundary tuple for WITH-TIES
/// peer detection. Both slots are ids in the EState pool; the copy allocates in
/// the destination slot's context.
fn copy_last_slot<'mcx>(
    node: &mut LimitStateData<'mcx>,
    src: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let last_slot = node
        .last_slot
        .expect("ExecLimit: WITH TIES requires last_slot to be initialized");
    let mcx = estate.es_query_cxt;
    let (dst, src) = estate.slot_pair_mut(last_slot, src);
    execTuples::exec_copy_slot::call(mcx, dst, src)
}

/// The WITH-TIES tie test:
///
/// ```c
/// econtext->ecxt_innertuple = slot;
/// econtext->ecxt_outertuple = node->last_slot;
/// if (ExecQualAndReset(node->eqfunction, econtext)) ...
/// ```
fn ties_match<'mcx>(
    node: &mut LimitStateData<'mcx>,
    slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("ExecLimit: Limit node has no ExprContext");
    {
        let ecxt = estate.ecxt_mut(econtext);
        ecxt.ecxt_innertuple = Some(slot);
        ecxt.ecxt_outertuple = node.last_slot;
    }
    let eqfunction = node
        .eqfunction
        .as_deref()
        .expect("ExecLimit: WITH TIES requires eqfunction to be initialized");
    execExpr::exec_qual_and_reset::call(eqfunction, econtext, estate)
}

/// `recompute_limits(node)` — evaluate the limit/offset expressions, done at
/// startup or rescan. This is also a handy place to reset the current-position
/// state info.
fn recompute_limits<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let econtext = node
        .ps
        .ps_ExprContext
        .expect("recompute_limits: Limit node has no ExprContext");

    if let Some(limit_offset) = node.limitOffset.as_deref() {
        // val = ExecEvalExprSwitchContext(node->limitOffset, econtext, &isNull);
        let (val, is_null) =
            execExpr::exec_eval_expr_switch_context::call(limit_offset, econtext, estate)?;
        // Interpret NULL offset as no offset.
        if is_null {
            node.offset = 0;
        } else {
            node.offset = val.as_i64(); // DatumGetInt64(val)
            if node.offset < 0 {
                return Err(PgError::error("OFFSET must not be negative")
                    .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE));
            }
        }
    } else {
        // No OFFSET supplied.
        node.offset = 0;
    }

    if let Some(limit_count) = node.limitCount.as_deref() {
        // val = ExecEvalExprSwitchContext(node->limitCount, econtext, &isNull);
        let (val, is_null) =
            execExpr::exec_eval_expr_switch_context::call(limit_count, econtext, estate)?;
        // Interpret NULL count as no count (LIMIT ALL).
        if is_null {
            node.count = 0;
            node.noCount = true;
        } else {
            node.count = val.as_i64(); // DatumGetInt64(val)
            if node.count < 0 {
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
        .expect("recompute_limits: Limit has no outer plan state");
    execProcnode::exec_set_tuple_bound::call(tuples_needed, outer, estate)?;

    Ok(())
}

/// `compute_tuples_needed(node)` — compute the maximum number of tuples needed to
/// satisfy this Limit node. Returns a negative value if there is not a
/// determinable limit.
fn compute_tuples_needed(node: &LimitStateData) -> int64 {
    if node.noCount || node.limitOption == LIMIT_OPTION_WITH_TIES {
        return -1;
    }
    // Note: if this overflows, we'll return a negative value, which is OK.
    node.count.wrapping_add(node.offset)
}

// ===========================================================================
// ExecInitLimit / ExecEndLimit / ExecReScanLimit.
// ===========================================================================

/// `ExecInitLimit(node, estate, eflags)` — initialize the limit node state
/// structures and the node's subplan.
///
/// Takes the enclosing plan-tree [`Node`](types_nodes::nodes::Node): the
/// state's plan back-link aliases the shared, read-only plan tree exactly as
/// C's `limitstate->ps.plan = (Plan *) node`. Panics if the node is not a
/// `Limit` (the C `castNode`). The state tree is allocated in
/// `estate.es_query_cxt` (C: `makeNode` in the per-query context), so init is
/// fallible on OOM.
pub fn ExecInitLimit<'mcx>(
    node: &'mcx types_nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<mcx::PgBox<'mcx, LimitStateData<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let limit: &'mcx Limit<'mcx> = match node {
        types_nodes::nodes::Node::Limit(l) => l,
        other => panic!("castNode(Limit, node) failed: {other:?}"),
    };

    // check for unsupported flags
    //   Assert(!(eflags & EXEC_FLAG_MARK));
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // create state structure
    //   limitstate = makeNode(LimitState);
    //   limitstate->ps.plan = (Plan *) node; limitstate->ps.state = estate;
    //   limitstate->ps.ExecProcNode = ExecLimit; limitstate->lstate = LIMIT_INITIAL;
    let mut limitstate = alloc_in(mcx, LimitStateData::default())?;
    limitstate.ps.plan = Some(node);
    limitstate.ps.ExecProcNode = Some(exec_limit_node);
    limitstate.lstate = LimitStateCond::LIMIT_INITIAL;

    // Miscellaneous initialization
    //
    // Limit nodes never call ExecQual or ExecProject, but they need an
    // exprcontext anyway to evaluate the limit/offset parameters in.
    //   ExecAssignExprContext(estate, &limitstate->ps);
    execUtils::ExecAssignExprContext(estate, &mut limitstate.ps)?;

    // initialize outer plan
    //   outerPlan = outerPlan(node);
    //   outerPlanState(limitstate) = ExecInitNode(outerPlan, estate, eflags);
    let outer_plan = limit.plan.lefttree.as_deref();
    limitstate.ps.lefttree =
        execProcnode::exec_init_node::call(mcx, outer_plan, estate, eflags)?;

    // initialize child expressions
    //   limitstate->limitOffset = ExecInitExpr((Expr *) node->limitOffset, ...);
    //   limitstate->limitCount  = ExecInitExpr((Expr *) node->limitCount, ...);
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
    execTuples::exec_init_result_type_tl::call(mcx, &mut limitstate.ps)?;

    //   limitstate->ps.resultopsset = true;
    //   limitstate->ps.resultops = ExecGetResultSlotOps(outerPlanState(limitstate),
    //                                                    &limitstate->ps.resultopsfixed);
    {
        let outer = limitstate
            .ps
            .lefttree
            .as_deref()
            .expect("ExecInitLimit: outer plan state must exist");
        let mut fixed = false;
        let ops = execUtils::ExecGetResultSlotOps(outer.ps_head(), estate, Some(&mut fixed));
        limitstate.ps.resultopsset = true;
        limitstate.ps.resultops = Some(ops);
        limitstate.ps.resultopsfixed = fixed;
    }

    // limit nodes do no projections, so initialize projection info for this node
    // appropriately
    //   limitstate->ps.ps_ProjInfo = NULL;
    limitstate.ps.ps_ProjInfo = None;

    // Initialize the equality evaluation, to detect ties.
    if limit.limitOption == LIMIT_OPTION_WITH_TIES {
        // desc = ExecGetResultType(outerPlanState(limitstate));
        // ops  = ExecGetResultSlotOps(outerPlanState(limitstate), NULL);
        let ops = {
            let outer = limitstate
                .ps
                .lefttree
                .as_deref()
                .expect("ExecInitLimit: outer plan state must exist");
            execUtils::ExecGetResultSlotOps(outer.ps_head(), estate, None)
        };
        let desc = outer_result_desc(&limitstate.ps, mcx)?;

        // limitstate->last_slot = ExecInitExtraTupleSlot(estate, desc, ops);
        limitstate.last_slot =
            Some(execTuples::exec_init_extra_tuple_slot::call(estate, desc, ops)?);

        // limitstate->eqfunction = execTuplesMatchPrepare(desc, node->uniqNumCols,
        //     node->uniqColIdx, node->uniqOperators, node->uniqCollations,
        //     &limitstate->ps);
        let desc = outer_result_desc(&limitstate.ps, mcx)?;
        limitstate.eqfunction = execGrouping::exec_tuples_match_prepare::call(
            desc,
            limit.uniqNumCols,
            limit.uniqColIdx.as_deref().unwrap_or(&[]),
            limit.uniqOperators.as_deref().unwrap_or(&[]),
            limit.uniqCollations.as_deref().unwrap_or(&[]),
            &mut limitstate.ps,
            estate,
        )?;
    }

    Ok(limitstate)
}

/// `ExecEndLimit(node)` — shut down the subplan and free resources allocated to
/// this node.
///
/// ```c
/// void ExecEndLimit(LimitState *node) { ExecEndNode(outerPlanState(node)); }
/// ```
pub fn ExecEndLimit<'mcx>(
    node: &mut LimitStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let outer = node
        .ps
        .lefttree
        .as_deref_mut()
        .expect("ExecEndLimit: Limit has no outer plan state");
    execProcnode::exec_end_node::call(outer, estate)
}

/// `ExecReScanLimit(node)` — rescan the node.
///
/// ```c
/// void ExecReScanLimit(LimitState *node)
/// {
///     PlanState  *outerPlan = outerPlanState(node);
///     recompute_limits(node);
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
    let chgparam_is_null = node
        .ps
        .lefttree
        .as_deref()
        .expect("ExecReScanLimit: Limit has no outer plan state")
        .ps_head()
        .chgParam
        .is_none();
    if chgparam_is_null {
        let outer = node
            .ps
            .lefttree
            .as_deref_mut()
            .expect("ExecReScanLimit: Limit has no outer plan state");
        execAmi::exec_re_scan::call(outer, estate)?;
    }

    Ok(())
}

/// `elog(ERROR, "LIMIT subplan failed to run backwards")` — the impossible
/// backwards path. `elog(ERROR)` carries `ERRCODE_INTERNAL_ERROR` by default.
fn impossible_backwards() -> PgError {
    PgError::error("LIMIT subplan failed to run backwards")
}

/// `desc = ExecGetResultType(outerPlanState(limitstate))` materialized into an
/// owned, `'mcx`-allocated `TupleDesc` (C shares the descriptor pointer;
/// `ExecInitExtraTupleSlot` / `execTuplesMatchPrepare` then reference it). The
/// owned model copies it into the per-query context.
fn outer_result_desc<'mcx>(
    ps: &PlanStateData<'mcx>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    let outer = ps
        .lefttree
        .as_deref()
        .expect("ExecInitLimit: outer plan state must exist");
    match execUtils::ExecGetResultType(outer.ps_head()) {
        Some(d) => Ok(Some(alloc_in(mcx, d.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}
