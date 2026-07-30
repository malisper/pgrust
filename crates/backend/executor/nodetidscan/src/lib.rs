// nodeTidscan.c. The TID list lives in the query arena (C pallocs in the
// executor context and pfrees on rescan; arena reset covers the free).
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{exec_eval_expr, exec_init_expr, exec_init_qual, EvalSlots, ExprState};
use ::execscan::{exec_scan, exec_scan_rescan, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox, PgVec};
use ::tableam::{
    table_beginscan_tid, table_endscan, table_rescan, table_slot_callbacks,
    table_tuple_fetch_row_version, table_tuple_get_latest_tid, table_tuple_tid_valid,
};
use ::types_core::catalog::TIDOID;
use ::types_error::{PgError, PgResult};
use ::types_nodes::plannodes::TidScan;
use ::types_nodes::Node;
use ::types_scan::sdir::ScanDirection;
use ::types_tuple::itemptr::{ItemPointerCompare, ItemPointerData};

pub fn init_seams() {}

const SELF_ITEM_POINTER_ATTR: i16 = -1;

#[derive(Clone, Copy)]
enum TidExprKind<'mcx> {
    Single,
    Array,
    CurrentOf { cursor_name: Option<&'mcx str>, cursor_param: i32 },
}

pub struct TidExpr<'mcx> {
    exprstate: Option<PgBox<'mcx, ExprState<'mcx>>>,
    kind: TidExprKind<'mcx>,
}

pub struct TidScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    tss_isCurrentOf: bool,
    tss_TidPtr: i64,
    tss_TidList: Option<PgVec<'mcx, ItemPointerData>>,
    tss_tidexprs: PgVec<'mcx, TidExpr<'mcx>>,
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

// TidExprListCreate (nodeTidscan.c).
fn tid_expr_list_create<'mcx>(
    mcx: Mcx<'mcx>,
    node: &TidScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(PgVec<'mcx, TidExpr<'mcx>>, bool)> {
    let mut tidexprs: PgVec<'mcx, TidExpr<'mcx>> = PgVec::new_in(mcx);
    let mut is_current_of = false;
    let params = estate.param_bind();

    for expr in &node.tidquals {
        let tidexpr = if let Some(op) = expr.as_op_expr() {
            let arg1 = op.args.nth(0);
            let arg2 = op.args.nth(1);
            let other = if is_ctid_var(arg1) {
                arg2
            } else if is_ctid_var(arg2) {
                arg1
            } else {
                return Err(elog_internal("could not identify CTID variable"));
            };
            TidExpr {
                exprstate: exec_init_expr(mcx, Some(other), params)?,
                kind: TidExprKind::Single,
            }
        } else if let Some(saex) = expr.as_scalar_array_op_expr() {
            debug_assert!(is_ctid_var(saex.args.nth(0)));
            TidExpr {
                exprstate: exec_init_expr(mcx, Some(saex.args.nth(1)), params)?,
                kind: TidExprKind::Array,
            }
        } else if let Some(cexpr) = expr.as_current_of_expr() {
            is_current_of = true;
            TidExpr {
                exprstate: None,
                kind: TidExprKind::CurrentOf {
                    cursor_name: cexpr.cursor_name,
                    cursor_param: cexpr.cursor_param,
                },
            }
        } else {
            return Err(elog_internal("could not identify CTID expression"));
        };
        tidexprs.push(tidexpr);
    }

    debug_assert!(tidexprs.len() == 1 || !is_current_of);
    Ok((tidexprs, is_current_of))
}

// DatumGetArrayTypeP: force any non-4B-uncompressed varlena flat.
fn datum_array_bytes<'m>(mcx: Mcx<'m>, v: ::datum::Datum) -> PgResult<&'m [u8]> {
    let p = v.as_usize() as *const u8;
    // SAFETY: non-null pass-by-ref varlena datum, readable through its
    // header-declared size.
    unsafe {
        if (*p) & 0x03 == 0 {
            return Ok(core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p)));
        }
        let image = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p));
        let flat = ::detoast::detoast_attr(mcx, image)?;
        Ok(flat.leak())
    }
}

// fetch_cursor_param_value (execCurrent.c). Params are materialized
// (no paramFetch hook), so the REFCURSOR type check cannot fail without a
// planner bug — that arm is a loud panic, not C's 42804.
fn fetch_cursor_param_value<'m>(
    mcx: Mcx<'m>,
    estate: &EStateData<'m>,
    param_id: i32,
) -> PgResult<&'m str> {
    const REFCURSOROID: ::types_core::Oid = 1790;
    if let Some(params) = estate.es_param_list_info {
        if param_id > 0 && (param_id as usize) <= params.len() {
            let prm = &params[param_id as usize - 1];
            if prm.ptype != ::types_core::InvalidOid && !prm.isnull {
                if prm.ptype != REFCURSOROID {
                    panic!(
                        "fetch_cursor_param_value (execCurrent.c): parameter {param_id} \
                         has type {} not refcursor",
                        prm.ptype
                    );
                }
                let image = datum_array_bytes(mcx, prm.value)?;
                let payload = ::varlena::open_image(mcx, image)?;
                let bytes = payload.as_bytes();
                let mut owned = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
                ::mcx::vec_append_bytes(&mut owned, bytes)?;
                return Ok(core::str::from_utf8(owned.leak()).expect("refcursor value utf8"));
            }
        }
    }
    Err(Box::new(
        PgError::error(format!("no value found for parameter {param_id}"))
            .with_sqlstate(::types_error::ERRCODE_UNDEFINED_OBJECT),
    ))
}

impl<'mcx> TidScanState<'mcx> {
    fn ensure_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.ss.ss_currentScanDesc.is_none() {
            let mcx = estate.es_query_cxt;
            let snapshot = estate.es_snapshot.clone();
            self.ss.ss_currentScanDesc = Some(table_beginscan_tid(
                mcx,
                self.ss.ss_currentRelation.as_ref().expect("tidscan has a relation"),
                snapshot,
            )?);
        }
        Ok(())
    }

    // TidListEval (nodeTidscan.c).
    fn tid_list_eval(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        self.ensure_scandesc(estate)?;

        let mut tid_list: PgVec<'mcx, ItemPointerData> = PgVec::new_in(mcx);
        tid_list.reserve(self.tss_tidexprs.len());
        let ecxt = self.ss.ps_ExprContext;
        let table_oid = self.ss.ss_currentRelation.as_ref().expect("relation").rd_id;

        for i in 0..self.tss_tidexprs.len() {
            let kind = self.tss_tidexprs[i].kind;
            match kind {
                TidExprKind::Single | TidExprKind::Array => {
                    let state =
                        self.tss_tidexprs[i].exprstate.as_deref_mut().expect("exprstate");
                    let deps = state.param_exec_deps();
                    if !deps.is_empty() {
                        ::executils::exec_eval_param_exec_params(estate, deps)?;
                    }
                    let state = self.tss_tidexprs[i].exprstate.as_deref_mut().unwrap();
                    // SAFETY: the per-tuple context object outlives the plan.
                    unsafe { state.arm_result_mcx_raw(estate.ecxt(ecxt).per_tuple_mcx()) };
                    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
                    let nd = exec_eval_expr(state, &mut slots)?;
                    if nd.isnull {
                        continue;
                    }
                    if matches!(kind, TidExprKind::Single) {
                        // SAFETY: non-null tid datum points at an ItemPointerData.
                        let itemptr =
                            unsafe { *(nd.value.as_usize() as *const ItemPointerData) };
                        let scan = self.ss.ss_currentScanDesc.as_mut().unwrap();
                        // AM-invalid TIDs are silently discarded (C contract).
                        if table_tuple_tid_valid(scan, &itemptr) {
                            tid_list.push(itemptr);
                        }
                    } else {
                        let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
                        let image = datum_array_bytes(per_tuple, nd.value)?;
                        let (ipdatums, ipnulls) = ::arrayfuncs::deconstruct_array_builtin(
                            per_tuple, image, TIDOID, true,
                        )?;
                        tid_list.reserve(ipdatums.len());
                        for (d, isnull) in ipdatums.iter().zip(ipnulls.iter()) {
                            if *isnull {
                                continue;
                            }
                            // SAFETY: non-null tid array element datum.
                            let itemptr =
                                unsafe { *(d.as_usize() as *const ItemPointerData) };
                            let scan = self.ss.ss_currentScanDesc.as_mut().unwrap();
                            if table_tuple_tid_valid(scan, &itemptr) {
                                tid_list.push(itemptr);
                            }
                        }
                    }
                }
                TidExprKind::CurrentOf { cursor_name, cursor_param } => {
                    let table_name = self.ss.ss_currentRelation.as_ref().unwrap().name();
                    let name = match cursor_name {
                        Some(n) => n,
                        None => fetch_cursor_param_value(mcx, estate, cursor_param)?,
                    };
                    if let Some(tid) = execmain_seams::exec_current_of::call(
                        Some(name),
                        cursor_param,
                        table_oid,
                        table_name,
                    )? {
                        tid_list.push(tid);
                    }
                }
            }
        }

        // Sort+dedupe: OR semantics; sorted order visits the heap best.
        if tid_list.len() > 1 {
            debug_assert!(!self.tss_isCurrentOf);
            tid_list.sort_unstable_by(|a, b| ItemPointerCompare(a, b).cmp(&0));
            let mut w = 1;
            for r in 1..tid_list.len() {
                if ItemPointerCompare(&tid_list[r], &tid_list[w - 1]) != 0 {
                    tid_list[w] = tid_list[r];
                    w += 1;
                }
            }
            tid_list.truncate(w);
        }

        self.tss_TidList = Some(tid_list);
        self.tss_TidPtr = -1;
        Ok(())
    }
}

impl<'mcx> ScanNode<'mcx> for TidScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `TidRecheck`: WHERE CURRENT OF always resolves to the latest tuple;
    /// otherwise the tuple must be in the TID list.
    fn epq_recheck(&mut self, estate: &mut EStateData<'mcx>, slot: ExecSlotId) -> PgResult<bool> {
        if self.tss_isCurrentOf {
            return Ok(true);
        }
        if self.tss_TidList.is_none() {
            self.tid_list_eval(estate)?;
        }
        let tid = estate.slot(slot).base().tts_tid;
        let list = self.tss_TidList.as_deref().unwrap();
        Ok(list
            .binary_search_by(|probe| ItemPointerCompare(probe, &tid).cmp(&0))
            .is_ok())
    }

    /// `TidNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = estate.es_direction;

        if self.tss_TidList.is_none() {
            self.tid_list_eval(estate)?;
        }
        let num_tids = self.tss_TidList.as_deref().unwrap().len() as i64;

        let backward = direction == ScanDirection::BackwardScanDirection;
        if backward {
            if self.tss_TidPtr < 0 {
                self.tss_TidPtr = num_tids - 1;
            } else {
                self.tss_TidPtr -= 1;
            }
        } else if self.tss_TidPtr < 0 {
            self.tss_TidPtr = 0;
        } else {
            self.tss_TidPtr += 1;
        }

        let slot_id = self.ss.ss_ScanTupleSlot;
        while self.tss_TidPtr >= 0 && self.tss_TidPtr < num_tids {
            let mut tid = self.tss_TidList.as_deref().unwrap()[self.tss_TidPtr as usize];

            // CURRENT OF: chase to the version current under our snapshot.
            if self.tss_isCurrentOf {
                let scan = self.ss.ss_currentScanDesc.as_mut().unwrap();
                table_tuple_get_latest_tid(mcx, scan, &mut tid)?;
            }

            let EStateData { es_tupleTable, es_snapshot, es_query_cxt, .. } = estate;
            let found = table_tuple_fetch_row_version(
                *es_query_cxt,
                self.ss.ss_currentRelation.as_ref().expect("relation"),
                &tid,
                es_snapshot,
                &mut es_tupleTable[slot_id.0 as usize],
            )?;
            if found {
                return Ok(true);
            }

            if backward {
                self.tss_TidPtr -= 1;
            } else {
                self.tss_TidPtr += 1;
            }
        }

        exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
        Ok(false)
    }
}

/// `ExecTidScan`.
pub fn exec_tid_scan<'mcx>(
    node: &mut TidScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    exec_scan(node, estate)
}

/// `ExecInitTidScan`.
pub fn exec_init_tid_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &TidScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<TidScanState<'mcx>> {
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

    let (tss_tidexprs, tss_isCurrentOf) = tid_expr_list_create(mcx, node, estate)?;

    Ok(TidScanState {
        ss,
        tss_isCurrentOf,
        tss_TidPtr: -1,
        tss_TidList: None,
        tss_tidexprs,
    })
}

/// `ExecEndTidScan`.
pub fn exec_end_tid_scan(node: &mut TidScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.tss_tidexprs.clear();
    node.tss_TidList = None;
    Ok(())
}

/// `ExecReScanTidScan`.
pub fn exec_rescan_tid_scan<'mcx>(
    node: &mut TidScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.tss_TidList = None;
    node.tss_TidPtr = -1;

    let mcx = estate.es_query_cxt;
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        table_rescan(mcx, scan, None)?;
    }
    exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

// SAFETY: Copy, no-drop (const-proven), owns nothing.
unsafe impl ::mcx::ForgetSafe for TidExprKind<'_> {}
const _: () = assert!(!core::mem::needs_drop::<TidExprKind<'static>>());
mcx::forget_safe_struct!(
    TidExpr<'_> { kind; exprstate },
    TidScanState<'_> { tss_isCurrentOf, tss_TidPtr; ss, tss_TidList, tss_tidexprs },
);
