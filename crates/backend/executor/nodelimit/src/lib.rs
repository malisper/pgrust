// nodeLimit.c. The outer child stays with the ExecProcNode dispatcher; the
// LimitChild trait carries the two child operations (proc, set-tuple-bound)
// monomorphized from the node-enum owner.
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use ::execexpr::{exec_eval_expr, EvalSlots, ExprState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::PgBox;
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE,
    ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE,
};
use ::types_nodes::plannodes::Limit;
use ::types_nodes::LimitOption;
use ::types_scan::sdir::ScanDirectionIsForward;
use ::types_slot::EXEC_FLAG_MARK;

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
    LIMIT_WINDOWSTART,
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
}

/// `ExecInitLimit` minus child linkage (caller inits the outer child with the
/// unmodified eflags, as C does).
pub fn exec_init_limit<'mcx>(
    node: &'mcx Limit<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<LimitState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    let mcx = estate.es_query_cxt;
    let ps_ExprContext = estate.exec_assign_expr_context();
    let params = estate.param_bind();
    let limitOffset = ::execexpr::exec_init_expr(mcx, node.limitOffset, params)?;
    let limitCount = ::execexpr::exec_init_expr(mcx, node.limitCount, params)?;
    if node.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
        panic!(
            "ExecInitLimit (nodeLimit.c): WITH TIES lane needs \
             execTuplesMatchPrepare (execGrouping.c), not ported"
        );
    }
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
    })
}

#[cold]
#[inline(never)]
fn ties_lane_unreachable() -> ! {
    panic!("nodeLimit: WITH TIES tuple-match lane unreachable (init panics first)")
}

#[cold]
#[inline(never)]
fn backwards_failed() -> Box<PgError> {
    Box::new(PgError::error("LIMIT subplan failed to run backwards"))
}

/// `ExecLimit`; C's switch fall-throughs become `continue` re-dispatch.
pub fn exec_limit<'mcx, C: LimitChild<'mcx>>(
    node: &mut LimitState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    cfi()?;
    let direction = estate.es_direction;
    let forward = ScanDirectionIsForward(direction);

    loop {
        match node.lstate {
            LIMIT_INITIAL => {
                recompute_limits(node, child, estate)?;
                continue;
            }
            LIMIT_RESCAN => {
                if !forward {
                    return Ok(None);
                }
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
                        ties_lane_unreachable();
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
                if forward {
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
                        ties_lane_unreachable();
                    }
                    node.subSlot = Some(slot);
                    node.position += 1;
                    break;
                }
                if node.position <= node.offset + 1 {
                    node.lstate = LIMIT_WINDOWSTART;
                    return Ok(None);
                }
                let Some(slot) = child.exec_proc(estate)? else {
                    return Err(backwards_failed());
                };
                node.subSlot = Some(slot);
                node.position -= 1;
                break;
            }
            LIMIT_WINDOWEND_TIES => {
                if forward {
                    ties_lane_unreachable();
                }
                if node.position <= node.offset + 1 {
                    node.lstate = LIMIT_WINDOWSTART;
                    return Ok(None);
                }
                let Some(slot) = child.exec_proc(estate)? else {
                    return Err(backwards_failed());
                };
                node.subSlot = Some(slot);
                node.position -= 1;
                node.lstate = LIMIT_INWINDOW;
                break;
            }
            LIMIT_SUBPLANEOF => {
                if forward {
                    return Ok(None);
                }
                let Some(slot) = child.exec_proc(estate)? else {
                    return Err(backwards_failed());
                };
                node.subSlot = Some(slot);
                node.lstate = LIMIT_INWINDOW;
                break;
            }
            LIMIT_WINDOWEND => {
                if forward {
                    return Ok(None);
                }
                if node.limitOption == LimitOption::LIMIT_OPTION_WITH_TIES {
                    ties_lane_unreachable();
                }
                node.lstate = LIMIT_INWINDOW;
                break;
            }
            LIMIT_WINDOWSTART => {
                if !forward {
                    return Ok(None);
                }
                node.lstate = LIMIT_INWINDOW;
                break;
            }
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

/// `ExecReScanLimit` minus the outer rescan (caller rescans the child after,
/// chgParam being NULL until the Param lanes land).
pub fn exec_rescan_limit<'mcx, C: LimitChild<'mcx>>(
    node: &mut LimitState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    recompute_limits(node, child, estate)
}

#[cold]
#[inline(never)]
fn negative_offset() -> Box<PgError> {
    Box::new(
        PgError::error("OFFSET must not be negative")
            .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_RESULT_OFFSET_CLAUSE),
    )
}

#[cold]
#[inline(never)]
fn negative_limit() -> Box<PgError> {
    Box::new(
        PgError::error("LIMIT must not be negative")
            .with_sqlstate(ERRCODE_INVALID_ROW_COUNT_IN_LIMIT_CLAUSE),
    )
}
