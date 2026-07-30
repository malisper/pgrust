// nodeLimit.c. The outer child stays with the ExecProcNode dispatcher; the
// LimitChild trait carries the two child operations (proc, set-tuple-bound)
// monomorphized from the node-enum owner.
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::rc::Rc;

use ::execexpr::{exec_build_grouping_equal, exec_eval_expr, exec_qual, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, PgBox, PgVec};
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
    ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
};
use ::types_nodes::plannodes::Limit;
use ::types_nodes::LimitOption;
use ::types_scan::sdir::ScanDirectionIsForward;
use ::types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_MARK};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

// C's CHECK_FOR_INTERRUPTS at ExecLimit entry.
#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

#[cfg(test)]
mod tests;

pub trait LimitChild<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn set_tuple_bound(&mut self, tuples_needed: i64);
}

/// execnodes.h LimitStateCond.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitStateCond {
    LIMIT_INITIAL,
    LIMIT_RESCAN,
    LIMIT_EMPTY,
    LIMIT_INWINDOW,
    LIMIT_WINDOWEND_TIES,
    LIMIT_SUBPLANEOF,
    LIMIT_WINDOWEND,
    // LIMIT_WINDOWSTART deleted (backward-execution wave B4): the state was
    // reachable only by walking the window backwards, which the run seam
    // refuses since deletion-prep B1 (C execnodes.h keeps it for the
    // backward arms C retains; ratified strategy divergence, Michael's
    // 2026-07-17 SCROLL/WITH-HOLD decision).
}
use LimitStateCond::*;

pub struct LimitState<'mcx> {
    pub plan: &'mcx Limit<'mcx>,
    pub ps_ExprContext: EcxtId,
    limitOffset: Option<PgBox<'mcx, ExprState<'mcx>>>,
    limitCount: Option<PgBox<'mcx, ExprState<'mcx>>>,
    limitOption: LimitOption,
    offset: i64,
    count: i64,
    noCount: bool,
    pub lstate: LimitStateCond,
    position: i64,
    pub subSlot: Option<ExecSlotId>,
    // WITH TIES: last in-window tuple + the tie-detection equality program
    // (C: last_slot + eqfunction from execTuplesMatchPrepare).
    last_slot: Option<SlotData<'mcx>>,
    eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

/// `ExecInitLimit` minus child linkage (caller inits the outer child with the
/// unmodified eflags, as C does). `outer_desc` is the outer child's result
/// type; only WITH TIES reads it.
pub fn exec_init_limit<'mcx>(
    node: &'mcx Limit<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: Option<&Rc<TupleDescData<'static>>>,
) -> PgResult<LimitState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();
    let params = estate.param_bind();
    let mut limitOffset = ::execexpr::exec_init_expr(mcx, node.limitOffset, params)?;
    let mut limitCount = ::execexpr::exec_init_expr(mcx, node.limitCount, params)?;
    for st in [limitOffset.as_mut(), limitCount.as_mut()].into_iter().flatten() {
        // C recomputes LIMIT/OFFSET in ps_ExprContext's per-tuple memory;
        // by-ref intermediates ride the armed result mcx.
        // SAFETY: the ExprContext outlives the programs (same estate).
        unsafe { st.arm_result_mcx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx()) };
    }
    let (last_slot, eq) = if node.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
        let desc = outer_desc.expect("WITH TIES requires the outer result type");
        let num_cols = node.uniqNumCols as usize;
        debug_assert!(node.uniqColIdx.len() == num_cols);
        let mut eqfuncoids: PgVec<'mcx, u32> = vec_with_capacity_in(mcx, num_cols)?;
        for &op in node.uniqOperators {
            eqfuncoids.push(lsyscache::get_opcode(op)?);
        }
        let eq = exec_build_grouping_equal(
            mcx,
            desc,
            desc,
            node.uniqColIdx,
            &eqfuncoids,
            node.uniqCollations,
        )?;
        let last_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc.clone()));
        (Some(last_slot), Some(eq))
    } else {
        (None, None)
    };
    Ok(LimitState {
        plan: node,
        ps_ExprContext,
        limitOffset,
        limitCount,
        limitOption: node.limitOption,
        offset: 0,
        count: 0,
        noCount: false,
        lstate: LIMIT_INITIAL,
        position: 0,
        subSlot: None,
        last_slot,
        eq,
    })
}

// ExecCopySlot(node->last_slot, slot): retain the boundary tuple for tie
// comparison.
fn save_last_slot<'mcx>(
    node: &mut LimitState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let last = node.last_slot.as_mut().expect("WITH TIES has a last slot");
    let src = estate.slot_mut(slot);
    exectuples::exec_copy_slot(last, src, mcx, mcx)?;
    Ok(())
}

/// `ExecLimit`; C's switch fall-throughs become `continue` re-dispatch.
/// Forward-only (backward-execution wave B4): C nodeLimit.c's backward arms
/// (the `!ScanDirectionIsForward` legs and their "LIMIT subplan failed to
/// run backwards" errors, nodeLimit.c:211/269/286/305) are deleted — the
/// run seam refuses backward entry (deletion-prep B1), so this node never
/// sees a backward pull.
pub fn exec_limit<'mcx, C: LimitChild<'mcx>>(
    node: &mut LimitState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    cfi()?;
    debug_assert!(
        ScanDirectionIsForward(estate.es_direction),
        "backward drive below the forward-only run seam (deletion-prep B1)"
    );

    loop {
        match node.lstate {
            LIMIT_INITIAL => {
                recompute_limits(node, child, estate)?;
                continue;
            }
            LIMIT_RESCAN => {
                if node.count <= 0 && !node.noCount {
                    node.lstate = LIMIT_EMPTY;
                    return Ok(None);
                }
                loop {
                    let Some(slot) = child.exec_proc(estate)? else {
                        node.lstate = LIMIT_EMPTY;
                        return Ok(None);
                    };
                    if node.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
                        && node.position - node.offset == node.count - 1
                    {
                        save_last_slot(node, estate, slot)?;
                    }
                    node.subSlot = Some(slot);
                    node.position += 1;
                    if node.position > node.offset {
                        break;
                    }
                }
                node.lstate = LIMIT_INWINDOW;
                break;
            }
            LIMIT_EMPTY => return Ok(None),
            LIMIT_INWINDOW => {
                if !node.noCount && node.position - node.offset >= node.count {
                    if node.limitOption == LimitOption::LIMIT_OPTION_COUNT {
                        node.lstate = LIMIT_WINDOWEND;
                        return Ok(None);
                    }
                    node.lstate = LIMIT_WINDOWEND_TIES;
                    continue;
                }
                let Some(slot) = child.exec_proc(estate)? else {
                    node.lstate = LIMIT_SUBPLANEOF;
                    return Ok(None);
                };
                if node.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES
                    && node.position - node.offset == node.count - 1
                {
                    save_last_slot(node, estate, slot)?;
                }
                node.subSlot = Some(slot);
                node.position += 1;
                break;
            }
            LIMIT_WINDOWEND_TIES => {
                // Advance until the first row whose ORDER BY keys differ
                // from the retained boundary tuple.
                let Some(slot) = child.exec_proc(estate)? else {
                    node.lstate = LIMIT_SUBPLANEOF;
                    return Ok(None);
                };
                let eq = node.eq.as_mut().expect("WITH TIES has an eq program");
                // SAFETY: the per-tuple context outlives this evaluation
                // and is reset right after (C: ExecQualAndReset;
                // packed-capable eq procs detoast-expand into it).
                unsafe { eq.arm_result_mcx_raw(estate.ecxt(node.ps_ExprContext).per_tuple_mcx()) };
                let last = node.last_slot.as_mut().expect("WITH TIES has a last slot");
                let new_slot = estate.slot_mut(slot);
                // C: ecxt_innertuple = incoming, ecxt_outertuple = last_slot.
                let mut slots = EvalSlots {
                    scan: None,
                    inner: Some(&mut *new_slot),
                    outer: Some(last),
                };
                let matched = exec_qual(Some(&mut **eq), &mut slots)?;
                estate.reset_expr_context(node.ps_ExprContext);
                if matched {
                    node.subSlot = Some(slot);
                    node.position += 1;
                    break;
                }
                node.lstate = LIMIT_WINDOWEND;
                return Ok(None);
            }
            LIMIT_SUBPLANEOF | LIMIT_WINDOWEND => return Ok(None),
        }
    }

    debug_assert!(node.subSlot.is_some());
    Ok(node.subSlot)
}

fn eval_limit_expr<'mcx>(
    expr: &mut ExprState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<::datum::NullableDatum> {
    // C's ExecEvalExprSwitchContext per-tuple context: reset, then eval with
    // no tuple slots (limit expressions reference no relation columns).
    estate.reset_expr_context(ecxt);
    let mut slots = EvalSlots {
        scan: None,
        inner: None,
        outer: None,
    };
    exec_eval_expr(expr, &mut slots)
}

/// `recompute_limits` (also the reset path of `ExecReScanLimit`).
pub fn recompute_limits<'mcx, C: LimitChild<'mcx>>(
    node: &mut LimitState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node.ps_ExprContext;
    if let Some(expr) = node.limitOffset.as_deref_mut() {
        let val = eval_limit_expr(expr, estate, ecxt)?;
        if val.isnull {
            node.offset = 0;
        } else {
            node.offset = val.value.as_i64();
            if node.offset < 0 {
                return Err(negative_offset());
            }
        }
    } else {
        node.offset = 0;
    }

    if let Some(expr) = node.limitCount.as_deref_mut() {
        let val = eval_limit_expr(expr, estate, ecxt)?;
        if val.isnull {
            node.count = 0;
            node.noCount = true;
        } else {
            node.count = val.value.as_i64();
            if node.count < 0 {
                return Err(negative_limit());
            }
            node.noCount = false;
        }
    } else {
        node.count = 0;
        node.noCount = true;
    }

    node.position = 0;
    node.subSlot = None;
    node.lstate = LIMIT_RESCAN;

    // C: always notify the child, even for a negative (no-limit) result.
    child.set_tuple_bound(compute_tuples_needed(node));
    Ok(())
}

/// `compute_tuples_needed`.
fn compute_tuples_needed(node: &LimitState<'_>) -> i64 {
    if node.noCount || node.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
        return -1;
    }
    node.count.wrapping_add(node.offset)
}

// ===========================================================================
// Lane-executor-v2 streaming-limit seam (design §Architecture 1; push-executor
// study Pattern 2). The lane's LimitOp lives in `execmain/src/lanev2.rs`;
// these thin entry points keep ALL limit semantics here, operating on the
// SAME LimitState fields (lstate/position/subSlot) the Volcano `exec_limit`
// drives — so falling back to `exec_limit` at any call boundary sees exactly
// C's cross-call state. Forward-only, LIMIT_OPTION_COUNT-only (gated by
// `lane_limit_admissible`); each function mirrors one arm of `exec_limit`'s
// forward state machine verbatim.
// ===========================================================================

/// What the lane streaming-limit operator should do with one child tuple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneLimitFeed {
    /// Still inside OFFSET: discard. C's LIMIT_RESCAN skip loop pulls and
    /// discards offset tuples too — same child work, same volatile-function
    /// invocation counts.
    Skip,
    /// In-window tuple: emit downstream.
    Emit,
    /// The window's LAST tuple: emit, then stop — the lane operator delivers
    /// it via `Paused` and reports `Finished` on the next driver round, so
    /// the source is never pulled past the boundary tuple's batch.
    EmitBoundary,
}

/// Limit-side lane admission: plain COUNT only. WITH TIES needs the
/// boundary-tuple retention + sort-peer equality walk (LIMIT_WINDOWEND_TIES)
/// — refused, staged later. (PostgreSQL's Limit node has no percent-limit
/// form, so no gate is needed for one.)
pub fn lane_limit_admissible(node: &LimitState<'_>) -> bool {
    node.limitOption == LimitOption::LIMIT_OPTION_COUNT
}

/// The window's total input-row bound (offset + count) for the LIMIT-k-no-
/// ORDER group-admission freeze (the band-2a composition class): `Some` only for a
/// plain-COUNT window with a computed positive count and a non-negative
/// offset. Call AFTER the prologue (the LIMIT_INITIAL recompute evaluated
/// the expressions). Saturating: an absurd offset+count simply exceeds every
/// arming ceiling downstream.
pub fn lane_limit_total_bound(node: &LimitState<'_>) -> Option<i64> {
    (node.limitOption == LimitOption::LIMIT_OPTION_COUNT
        && !node.noCount
        && node.count > 0
        && node.offset >= 0)
        .then(|| node.count.saturating_add(node.offset))
}

/// Per-call prologue, mirroring `exec_limit`'s forward entry exactly: C's
/// ExecLimit CHECK_FOR_INTERRUPTS, then the LIMIT_INITIAL recompute — which
/// evaluates the OFFSET/LIMIT expressions (raising C's negative-value errors)
/// and pushes the tuple bound to the child (the Sort's top-N bound; a silent
/// no-op for Agg — `exec_set_tuple_bound`'s dispatch, unchanged).
pub fn lane_limit_prologue<'mcx, C: LimitChild<'mcx>>(
    node: &mut LimitState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    cfi()?;
    if node.lstate == LIMIT_INITIAL {
        recompute_limits(node, child, estate)?;
    }
    Ok(())
}

/// `exec_limit`'s LIMIT_RESCAN `count <= 0 && !noCount` arm: an empty window
/// (LIMIT 0) flips to LIMIT_EMPTY and the child is never pulled.
pub fn lane_limit_empty_window(node: &mut LimitState<'_>) -> bool {
    debug_assert_eq!(node.lstate, LIMIT_RESCAN);
    if node.count <= 0 && !node.noCount {
        node.lstate = LIMIT_EMPTY;
        return true;
    }
    false
}

/// One child tuple through the window arithmetic — `exec_limit`'s forward
/// LIMIT_RESCAN skip loop + LIMIT_INWINDOW body verbatim (position/subSlot
/// bookkeeping and the RESCAN→INWINDOW flip included).
pub fn lane_limit_feed(node: &mut LimitState<'_>, slot: ExecSlotId) -> LaneLimitFeed {
    debug_assert!(matches!(node.lstate, LIMIT_RESCAN | LIMIT_INWINDOW));
    debug_assert_eq!(node.limitOption, LimitOption::LIMIT_OPTION_COUNT);
    node.subSlot = Some(slot);
    node.position += 1;
    if node.position <= node.offset {
        return LaneLimitFeed::Skip;
    }
    node.lstate = LIMIT_INWINDOW;
    if !node.noCount && node.position - node.offset >= node.count {
        LaneLimitFeed::EmitBoundary
    } else {
        LaneLimitFeed::Emit
    }
}

/// True once the window is complete: C's next ExecLimit call would flip to
/// LIMIT_WINDOWEND and return NULL without pulling the child.
pub fn lane_limit_window_done(node: &LimitState<'_>) -> bool {
    node.lstate == LIMIT_INWINDOW
        && !node.noCount
        && node.position - node.offset >= node.count
}

/// The deferred LIMIT_WINDOWEND flip — the `Finished` half of the lane's
/// Paused-then-Finished protocol (the boundary tuple was already delivered).
pub fn lane_limit_end_window(node: &mut LimitState<'_>) {
    debug_assert!(lane_limit_window_done(node));
    node.lstate = LIMIT_WINDOWEND;
}

/// Child exhausted before the window filled: `exec_limit`'s subplan-EOF arms
/// (LIMIT_RESCAN → LIMIT_EMPTY; LIMIT_INWINDOW → LIMIT_SUBPLANEOF).
pub fn lane_limit_eof(node: &mut LimitState<'_>) {
    debug_assert!(matches!(node.lstate, LIMIT_RESCAN | LIMIT_INWINDOW));
    node.lstate = if node.lstate == LIMIT_RESCAN { LIMIT_EMPTY } else { LIMIT_SUBPLANEOF };
}

/// `ExecReScanLimit` minus the outer rescan (caller rescans the child after,
/// chgParam being NULL until the Param lanes land).
pub fn exec_rescan_limit<'mcx, C: LimitChild<'mcx>>(
    node: &mut LimitState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    recompute_limits(node, child, estate)
}

#[track_caller]
#[cold]
#[inline(never)]
fn negative_offset() -> Box<PgError> {
    Box::new(
        PgError::error("OFFSET must not be negative")
            .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn negative_limit() -> Box<PgError> {
    Box::new(
        PgError::error("LIMIT must not be negative")
            .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
    )
}

/// `ExecEndLimit`.
pub fn exec_end_limit(node: &mut LimitState<'_>) {
    node.limitOffset = None;
    node.limitCount = None;
    if let Some(eq) = node.eq.as_mut() {
        eq.release_frames();
    }
    node.eq = None;
    if let Some(last) = node.last_slot.as_mut() {
        last.base_mut().tts_tupleDescriptor = None;
    }
}

mcx::forget_safe_nodrop!(LimitStateCond);

// Exempt: limitOffset/limitCount/eq are released in exec_end_limit (eq via
// release_frames), last_slot's descriptor likewise; LimitOption is no-drop,
// const-proven below.
const _: () = assert!(!core::mem::needs_drop::<LimitOption>());
mcx::forget_safe_struct!(
    LimitState<'_> { plan, ps_ExprContext, offset, count, noCount, lstate,
        position, subSlot; limitOffset, limitCount, limitOption, last_slot, eq },
);
