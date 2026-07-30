// nodeTidrangescan.c.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{exec_eval_expr, exec_init_expr, exec_init_qual, EvalSlots, ExprState};
use ::execscan::{exec_scan, exec_scan_rescan, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox, PgVec};
use ::tableam::{
    table_beginscan_tidrange, table_endscan, table_rescan_tidrange,
    table_scan_getnextslot_tidrange, table_slot_callbacks,
};
use ::types_core::primitive::InvalidBlockNumber;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::TidRangeScan;
use ::types_nodes::Node;
use ::types_tuple::itemptr::{
    ItemPointerCompare, ItemPointerData, ItemPointerDec, ItemPointerInc,
};

pub fn init_seams() {}

const SELF_ITEM_POINTER_ATTR: i16 = -1;

const TID_LESS_OPERATOR: u32 = 2799;
const TID_GREATER_OPERATOR: u32 = 2800;
const TID_LESS_EQ_OPERATOR: u32 = 2801;
const TID_GREATER_EQ_OPERATOR: u32 = 2802;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TidExprType {
    UpperBound,
    LowerBound,
}

pub struct TidOpExpr<'mcx> {
    exprtype: TidExprType,
    exprstate: PgBox<'mcx, ExprState<'mcx>>,
    inclusive: bool,
}

pub struct TidRangeScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    trss_tidexprs: PgVec<'mcx, TidOpExpr<'mcx>>,
    trss_mintid: ItemPointerData,
    trss_maxtid: ItemPointerData,
    trss_inScan: bool,
}

fn is_ctid_var(node: Node<'_>) -> bool {
    node.as_var().is_some_and(|v| v.varattno == SELF_ITEM_POINTER_ATTR)
}

#[track_caller]
#[cold]
#[inline(never)]
fn elog_internal(message: &'static str) -> Box<PgError> {
    Box::new(PgError::error(message.to_string()))
}

// MakeTidOpExpr + TidExprListCreate (nodeTidrangescan.c).
fn tid_expr_list_create<'mcx>(
    mcx: Mcx<'mcx>,
    node: &TidRangeScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgVec<'mcx, TidOpExpr<'mcx>>> {
    let params = estate.param_bind();
    let mut tidexprs: PgVec<'mcx, TidOpExpr<'mcx>> = PgVec::new_in(mcx);

    for expr in &node.tidrangequals {
        let Some(op) = expr.as_op_expr() else {
            return Err(elog_internal("could not identify CTID expression"));
        };
        let arg1 = op.args.nth(0);
        let arg2 = op.args.nth(1);
        let (other, invert) = if is_ctid_var(arg1) {
            (arg2, false)
        } else if is_ctid_var(arg2) {
            (arg1, true)
        } else {
            return Err(elog_internal("could not identify CTID variable"));
        };
        let exprstate =
            exec_init_expr(mcx, Some(other), params)?.expect("tid bound exprstate");

        let (exprtype, inclusive) = match op.opno {
            TID_LESS_EQ_OPERATOR | TID_LESS_OPERATOR => (
                if invert { TidExprType::LowerBound } else { TidExprType::UpperBound },
                op.opno == TID_LESS_EQ_OPERATOR,
            ),
            TID_GREATER_EQ_OPERATOR | TID_GREATER_OPERATOR => (
                if invert { TidExprType::UpperBound } else { TidExprType::LowerBound },
                op.opno == TID_GREATER_EQ_OPERATOR,
            ),
            _ => return Err(elog_internal("could not identify CTID operator")),
        };
        tidexprs.push(TidOpExpr { exprtype, exprstate, inclusive });
    }
    Ok(tidexprs)
}

impl<'mcx> TidRangeScanState<'mcx> {
    // TidRangeEval (nodeTidrangescan.c): false = the range matches nothing.
    fn tid_range_eval(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let ecxt = self.ss.ps_ExprContext;
        let mut lower_bound = ItemPointerData::new(0, 0);
        let mut upper_bound = ItemPointerData::new(InvalidBlockNumber, u16::MAX);

        for i in 0..self.trss_tidexprs.len() {
            let state = &mut self.trss_tidexprs[i].exprstate;
            let deps = state.param_exec_deps();
            if !deps.is_empty() {
                ::executils::exec_eval_param_exec_params(estate, deps)?;
            }
            let te = &mut self.trss_tidexprs[i];
            // SAFETY: the per-tuple context object outlives the plan.
            unsafe { te.exprstate.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
            let mut slots = EvalSlots { scan: None, inner: None, outer: None };
            let nd = exec_eval_expr(&mut te.exprstate, &mut slots)?;
            if nd.isnull {
                return Ok(false);
            }
            // SAFETY: non-null tid datum points at an ItemPointerData.
            let itemptr = unsafe { *(nd.value.as_usize() as *const ItemPointerData) };

            match te.exprtype {
                TidExprType::LowerBound => {
                    let mut lb = itemptr;
                    // Non-inclusive bounds normalize inclusive; may not be a
                    // valid item pointer.
                    if !te.inclusive {
                        ItemPointerInc(&mut lb);
                    }
                    if ItemPointerCompare(&lb, &lower_bound) > 0 {
                        lower_bound = lb;
                    }
                }
                TidExprType::UpperBound => {
                    let mut ub = itemptr;
                    if !te.inclusive {
                        ItemPointerDec(&mut ub);
                    }
                    if ItemPointerCompare(&ub, &upper_bound) < 0 {
                        upper_bound = ub;
                    }
                }
            }
        }

        self.trss_mintid = lower_bound;
        self.trss_maxtid = upper_bound;
        Ok(true)
    }
}

impl<'mcx> ScanNode<'mcx> for TidRangeScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `TidRangeRecheck`.
    fn epq_recheck(&mut self, estate: &mut EStateData<'mcx>, slot: ExecSlotId) -> PgResult<bool> {
        if !self.tid_range_eval(estate)? {
            return Ok(false);
        }
        let tid = estate.slot(slot).base().tts_tid;
        Ok(ItemPointerCompare(&tid, &self.trss_mintid) >= 0
            && ItemPointerCompare(&tid, &self.trss_maxtid) <= 0)
    }

    /// `TidRangeNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = estate.es_direction;

        if !self.trss_inScan {
            if !self.tid_range_eval(estate)? {
                return Ok(false);
            }
            if self.ss.ss_currentScanDesc.is_none() {
                let snapshot = estate.es_snapshot.clone();
                self.ss.ss_currentScanDesc = Some(table_beginscan_tidrange(
                    mcx,
                    self.ss.ss_currentRelation.as_ref().expect("tidrangescan has a relation"),
                    snapshot,
                    &self.trss_mintid,
                    &self.trss_maxtid,
                )?);
            } else {
                let scan = self.ss.ss_currentScanDesc.as_mut().unwrap();
                table_rescan_tidrange(mcx, scan, &self.trss_mintid, &self.trss_maxtid)?;
            }
            self.trss_inScan = true;
        }

        let slot_id = self.ss.ss_ScanTupleSlot;
        let scan = self.ss.ss_currentScanDesc.as_mut().unwrap();
        let found =
            table_scan_getnextslot_tidrange(mcx, scan, direction, estate.slot_mut(slot_id))?;
        if !found {
            self.trss_inScan = false;
            exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
        }
        Ok(found)
    }
}

/// `ExecTidRangeScan`.
pub fn exec_tid_range_scan<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    exec_scan(node, estate)
}

/// `ExecInitTidRangeScan`.
pub fn exec_init_tid_range_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &TidRangeScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<TidRangeScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let rel = estate.exec_get_range_table_relation(node.scan.scanrelid, false)?.alias();
    let ps_ExprContext = estate.exec_assign_expr_context();
    let kind = table_slot_callbacks(&rel);
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), kind);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    let params = estate.param_bind();
    ss.qual = ::executils::with_subplan_compile_env(estate, |env| {
        ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)
    })?;

    let trss_tidexprs = tid_expr_list_create(mcx, node, estate)?;

    Ok(TidRangeScanState {
        ss,
        trss_tidexprs,
        trss_mintid: ItemPointerData::invalid(),
        trss_maxtid: ItemPointerData::invalid(),
        trss_inScan: false,
    })
}

/// `ExecEndTidRangeScan`.
pub fn exec_end_tid_range_scan(node: &mut TidRangeScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.trss_tidexprs.clear();
    Ok(())
}

/// `ExecReScanTidRangeScan`; table_rescan_tidrange waits for the next fetch.
pub fn exec_rescan_tid_range_scan<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.trss_inScan = false;
    exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

// exprtype/mintid/maxtid are exempt: Copy, no-drop (const-proven).
const _: () = assert!(!core::mem::needs_drop::<TidExprType>());
const _: () = assert!(!core::mem::needs_drop::<ItemPointerData>());
mcx::forget_safe_struct!(
    TidOpExpr<'_> { inclusive; exprstate, exprtype },
    TidRangeScanState<'_> { trss_inScan; ss, trss_tidexprs, trss_mintid, trss_maxtid },
);
