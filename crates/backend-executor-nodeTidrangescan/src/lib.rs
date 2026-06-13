//! Port of `nodeTidrangescan.c` — routines to support TID range scans of
//! relations.
//!
//! The node state machine is ported 1:1 from PostgreSQL 18.3 — the scan
//! workhorses [`TidRangeNext`] / [`TidRangeRecheck`], the bound-range evaluator
//! [`TidRangeEval`], the qual-compilation helpers [`TidExprListCreate`] /
//! [`MakeTidOpExpr`], the `ExecProcNode` callback [`ExecTidRangeScan`], and the
//! public [`ExecInitTidRangeScan`] / [`ExecEndTidRangeScan`] /
//! [`ExecReScanTidRangeScan`]. The `storage/itemptr.h` item-pointer arithmetic
//! (`ItemPointerCompare` / `Inc` / `Dec` / `Set` / `Copy`) used by the bound
//! narrowing is reproduced in-crate, operating on owned
//! [`ItemPointerData`] values.
//!
//! `ExecScan` is the non-inlined `execScan.c` driver that [`ExecTidRangeScan`]
//! calls; its body (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`) is
//! reproduced here as private functions of this crate, so the qual / projection
//! / EvalPlanQual logic stays faithful while every leaf operation goes through a
//! seam.
//!
//! The planner-primnode field reads (`IsCTIDVar`, `get_leftop`/`get_rightop`,
//! `OpExpr.opno`, `IsA(node, OpExpr)`) are pure data reads on the owned
//! `TidRangeScan` plan node, so they are implemented in-crate. Every operation
//! in a subsystem below the node layer (expression compile/eval, the
//! execUtils/execScan init helpers, the table access methods, the `execScan.c`
//! leaf operations / EvalPlanQual machinery) goes through this crate's seam
//! surface, defaulting to a loud panic until the owner installs it.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use backend_executor_nodeTidrangescan_seams as seam;
use mcx::vec_with_capacity_in;
use types_core::primitive::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_OUT_OF_MEMORY};
use types_nodes::execnodes::{EStateData, ScanStateData};
use types_nodes::executor::TTS_FLAG_EMPTY;
use types_nodes::nodetidrangescan::TidRangeScan;
use types_nodes::primnodes::Expr;
use types_tuple::heaptuple::{ItemPointerData, SelfItemPointerAttributeNumber};
use types_tidrange::{ExprStateHandle, OperandSide, TidExprType, TidOpExpr, TidRangeScanState};

// ===========================================================================
// Catalog operator OIDs (`pg_operator.dat`) used to classify the TID range op.
// ===========================================================================

/// `TIDLessOperator` — `<` for `tid`.
const TIDLessOperator: u32 = 2799;
/// `TIDGreaterOperator` — `>` for `tid`.
const TIDGreaterOperator: u32 = 2800;
/// `TIDLessEqOperator` — `<=` for `tid`.
const TIDLessEqOperator: u32 = 2801;
/// `TIDGreaterEqOperator` — `>=` for `tid`.
const TIDGreaterEqOperator: u32 = 2802;

/// `PG_UINT16_MAX` (`c.h`).
const PG_UINT16_MAX: OffsetNumber = u16::MAX;

// ===========================================================================
// `storage/itemptr.h` / `itemptr.c` helpers (1:1).
// ===========================================================================

#[inline]
fn ItemPointerGetBlockNumberNoCheck(pointer: &ItemPointerData) -> BlockNumber {
    pointer.ip_blkid.block_number()
}

#[inline]
fn ItemPointerGetOffsetNumberNoCheck(pointer: &ItemPointerData) -> OffsetNumber {
    pointer.ip_posid
}

#[inline]
fn ItemPointerIsValid(pointer: &ItemPointerData) -> bool {
    pointer.ip_posid != 0
}

#[inline]
fn ItemPointerSet(pointer: &mut ItemPointerData, block_number: BlockNumber, off_num: OffsetNumber) {
    *pointer = ItemPointerData::new(block_number, off_num);
}

#[inline]
fn ItemPointerCopy(from_pointer: &ItemPointerData, to_pointer: &mut ItemPointerData) {
    *to_pointer = *from_pointer;
}

/// `ItemPointerCompare` — generic btree-style comparison for item pointers.
fn ItemPointerCompare(arg1: &ItemPointerData, arg2: &ItemPointerData) -> i32 {
    let b1 = ItemPointerGetBlockNumberNoCheck(arg1);
    let b2 = ItemPointerGetBlockNumberNoCheck(arg2);
    if b1 < b2 {
        -1
    } else if b1 > b2 {
        1
    } else if ItemPointerGetOffsetNumberNoCheck(arg1) < ItemPointerGetOffsetNumberNoCheck(arg2) {
        -1
    } else if ItemPointerGetOffsetNumberNoCheck(arg1) > ItemPointerGetOffsetNumberNoCheck(arg2) {
        1
    } else {
        0
    }
}

/// `ItemPointerInc` — increment by 1, respecting only the type's range limits.
fn ItemPointerInc(pointer: &mut ItemPointerData) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);
    if off == PG_UINT16_MAX {
        if blk != InvalidBlockNumber {
            off = 0;
            blk += 1;
        }
    } else {
        off += 1;
    }
    ItemPointerSet(pointer, blk, off);
}

/// `ItemPointerDec` — decrement by 1, respecting only the type's range limits.
fn ItemPointerDec(pointer: &mut ItemPointerData) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);
    if off == 0 {
        if blk != 0 {
            off = PG_UINT16_MAX;
            blk -= 1;
        }
    } else {
        off -= 1;
    }
    ItemPointerSet(pointer, blk, off);
}

// ===========================================================================
// Planner-primnode field reads — pure data reads on the owned `TidRangeScan`.
// ===========================================================================

/// The `qual_index`-th cell of `node->tidrangequals`, expected to be an
/// `OpExpr`. `None` if the index is out of range (the C `lfirst` would not run
/// past the list).
fn qual_cell<'a, 'mcx>(node: &'a TidRangeScan<'mcx>, qual_index: usize) -> Option<&'a Expr> {
    node.tidrangequals.as_ref().and_then(|q| q.get(qual_index))
}

/// `IsA(opexpr, OpExpr)` for the `qual_index`-th cell of `node->tidrangequals`.
fn node_is_opexpr(node: &TidRangeScan, qual_index: usize) -> bool {
    matches!(qual_cell(node, qual_index), Some(Expr::OpExpr(_)))
}

/// `IsCTIDVar(get_leftop/get_rightop(expr))` for the `qual_index`-th qual
/// `OpExpr`'s `side` operand — true iff that operand is a `Var` for the CTID
/// system column (`varattno == SelfItemPointerAttributeNumber`). A missing
/// operand mirrors the C `NULL` arg (`IsCTIDVar` returns false).
fn is_ctid_var(node: &TidRangeScan, qual_index: usize, side: OperandSide) -> bool {
    let Some(Expr::OpExpr(op)) = qual_cell(node, qual_index) else {
        return false;
    };
    let arg = match side {
        OperandSide::Left => op.args.first(),
        OperandSide::Right => op.args.get(1),
    };
    matches!(arg, Some(Expr::Var(v)) if v.varattno == SelfItemPointerAttributeNumber)
}

/// `expr->opno` for the `qual_index`-th qual `OpExpr`.
fn opexpr_opno(node: &TidRangeScan, qual_index: usize) -> u32 {
    match qual_cell(node, qual_index) {
        Some(Expr::OpExpr(op)) => op.opno,
        _ => 0,
    }
}

// ===========================================================================
// `nodeTidrangescan.c` — static helpers (1:1).
// ===========================================================================

/// `MakeTidOpExpr` — for the given `qual_index`-th qual node (an `OpExpr`),
/// build an appropriate [`TidOpExpr`] taking into account the operator and
/// operand order.
fn MakeTidOpExpr<'mcx>(
    tidstate: &mut TidRangeScanState<'mcx>,
    node: &TidRangeScan<'mcx>,
    qual_index: usize,
) -> PgResult<TidOpExpr> {
    // Node *arg1 = get_leftop((Expr *) expr);
    // Node *arg2 = get_rightop((Expr *) expr);
    let arg1_is_ctid = is_ctid_var(node, qual_index, OperandSide::Left);
    let arg2_is_ctid = is_ctid_var(node, qual_index, OperandSide::Right);

    let exprstate: ExprStateHandle;
    let mut invert = false;

    if arg1_is_ctid {
        // exprstate = ExecInitExpr((Expr *) arg2, &tidstate->ss.ps);
        exprstate = seam::exec_init_expr::call(tidstate, node, qual_index, OperandSide::Right)?;
    } else if arg2_is_ctid {
        // exprstate = ExecInitExpr((Expr *) arg1, &tidstate->ss.ps);
        exprstate = seam::exec_init_expr::call(tidstate, node, qual_index, OperandSide::Left)?;
        invert = true;
    } else {
        return Err(elog_internal("could not identify CTID variable"));
    }

    // tidopexpr->inclusive = false;  /* for now */
    let mut inclusive = false;
    let exprtype: TidExprType;

    match opexpr_opno(node, qual_index) {
        TIDLessEqOperator => {
            inclusive = true;
            // fall through
            exprtype = if invert {
                TidExprType::LowerBound
            } else {
                TidExprType::UpperBound
            };
        }
        TIDLessOperator => {
            exprtype = if invert {
                TidExprType::LowerBound
            } else {
                TidExprType::UpperBound
            };
        }
        TIDGreaterEqOperator => {
            inclusive = true;
            // fall through
            exprtype = if invert {
                TidExprType::UpperBound
            } else {
                TidExprType::LowerBound
            };
        }
        TIDGreaterOperator => {
            exprtype = if invert {
                TidExprType::UpperBound
            } else {
                TidExprType::LowerBound
            };
        }
        _ => return Err(elog_internal("could not identify CTID operator")),
    }

    Ok(TidOpExpr {
        exprtype,
        exprstate,
        inclusive,
    })
}

/// `TidExprListCreate` — extract the qual subexpressions that yield TIDs to
/// search for, and compile them into `ExprState`s.
///
/// The compiled-bound list (the C `List *tidexprs`) is built into a [`PgVec`]
/// charged to the executor's per-query context (the `'mcx` the EState carries),
/// then handed off to `trss_tidexprs` — the idiomatic analog of `palloc`ing
/// each `TidOpExpr` and `lappend`ing it under `CurrentMemoryContext`. The
/// context is reset at `ExecEndNode`, reclaiming the list.
fn TidExprListCreate<'mcx>(
    tidrangestate: &mut TidRangeScanState<'mcx>,
    node: &TidRangeScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // List *tidexprs = NIL;  (built fallibly into the per-query context)
    let nquals = node.tidrangequals.as_ref().map_or(0, |q| q.len());
    let mut tidexprs = vec_with_capacity_in(mcx, nquals)?;

    // foreach(l, node->tidrangequals)
    for qual_index in 0..nquals {
        // if (!IsA(opexpr, OpExpr)) elog(ERROR, "could not identify CTID expression");
        if !node_is_opexpr(node, qual_index) {
            return Err(elog_internal("could not identify CTID expression"));
        }

        // tidopexpr = MakeTidOpExpr(opexpr, tidrangestate);
        let tidopexpr = MakeTidOpExpr(tidrangestate, node, qual_index)?;

        // tidexprs = lappend(tidexprs, tidopexpr);
        tidexprs
            .try_reserve(1)
            .map_err(|_| out_of_memory("TID range qual expression list"))?;
        tidexprs.push(tidopexpr);
    }

    // tidrangestate->trss_tidexprs = tidexprs;
    tidrangestate.trss_tidexprs = tidexprs;
    Ok(())
}

/// `TidRangeEval` — compute and set the node's block/offset range to scan by
/// evaluating `node->trss_tidexprs`.
///
/// Returns `Ok(false)` if the range cannot contain any tuples; `Ok(true)`
/// otherwise. We don't validate `mintid <= maxtid` — the `scan_set_tidrange`
/// table AM function handles that.
fn TidRangeEval<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mut lower_bound = ItemPointerData::default();
    let mut upper_bound = ItemPointerData::default();

    // Set the upper and lower bounds to the absolute limits of the range of the
    // ItemPointer type. Below we narrow this on either side.
    ItemPointerSet(&mut lower_bound, 0, 0);
    ItemPointerSet(&mut upper_bound, InvalidBlockNumber, PG_UINT16_MAX);

    // foreach(l, node->trss_tidexprs)
    let n = node.trss_tidexprs.len();
    for i in 0..n {
        let exprstate = node.trss_tidexprs[i].exprstate;
        let exprtype = node.trss_tidexprs[i].exprtype;
        let inclusive = node.trss_tidexprs[i].inclusive;

        let mut is_null = false;
        // itemptr = (ItemPointer) DatumGetPointer(
        //     ExecEvalExprSwitchContext(tidopexpr->exprstate, econtext, &isNull));
        let itemptr =
            seam::exec_eval_expr_switch_context::call(node, exprstate, &mut is_null, estate)?;

        // If the bound is NULL, *nothing* matches the qual.
        if is_null {
            return Ok(false);
        }

        if exprtype == TidExprType::LowerBound {
            // ItemPointerData lb; ItemPointerCopy(itemptr, &lb);
            let mut lb = ItemPointerData::default();
            ItemPointerCopy(&itemptr, &mut lb);

            // Normalize non-inclusive ranges to become inclusive. The result may
            // not be a valid item pointer.
            if !inclusive {
                ItemPointerInc(&mut lb);
            }

            // Check if we can narrow the range using this qual.
            if ItemPointerCompare(&lb, &lower_bound) > 0 {
                ItemPointerCopy(&lb, &mut lower_bound);
            }
        } else if exprtype == TidExprType::UpperBound {
            // ItemPointerData ub; ItemPointerCopy(itemptr, &ub);
            let mut ub = ItemPointerData::default();
            ItemPointerCopy(&itemptr, &mut ub);

            // Normalize non-inclusive ranges to become inclusive. The result may
            // not be a valid item pointer.
            if !inclusive {
                ItemPointerDec(&mut ub);
            }

            // Check if we can narrow the range using this qual.
            if ItemPointerCompare(&ub, &upper_bound) < 0 {
                ItemPointerCopy(&ub, &mut upper_bound);
            }
        }
    }

    // ItemPointerCopy(&lowerBound, &node->trss_mintid);
    // ItemPointerCopy(&upperBound, &node->trss_maxtid);
    ItemPointerCopy(&lower_bound, &mut node.trss_mintid);
    ItemPointerCopy(&upper_bound, &mut node.trss_maxtid);

    Ok(true)
}

/// `TidRangeNext` — retrieve a tuple from the node's `currentRelation` using the
/// TIDs in the `TidRangeScanState` information.
///
/// On success the visible tuple lives in `node.ss.ss_ScanTupleSlot`; returns
/// `Ok(true)` if a tuple is available, `Ok(false)` when the relation is
/// exhausted.
fn TidRangeNext<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !node.trss_inScan {
        // First time through, compute TID range to scan.
        if !TidRangeEval(node, estate)? {
            return Ok(false);
        }

        if node.ss_currentScanDesc.is_none() {
            // scandesc = table_beginscan_tidrange(node->ss.ss_currentRelation,
            //     estate->es_snapshot, &node->trss_mintid, &node->trss_maxtid);
            // node->ss.ss_currentScanDesc = scandesc;
            seam::table_beginscan_tidrange::call(node, estate)?;
        } else {
            // rescan with the updated TID range
            // table_rescan_tidrange(scandesc, &node->trss_mintid, &node->trss_maxtid);
            seam::table_rescan_tidrange::call(node, estate)?;
        }

        node.trss_inScan = true;
    }

    // Fetch the next tuple.
    // if (!table_scan_getnextslot_tidrange(scandesc, direction, slot)) {
    //     node->trss_inScan = false; ExecClearTuple(slot);
    // }
    if !seam::table_scan_getnextslot_tidrange::call(node, estate)? {
        node.trss_inScan = false;
        seam::exec_clear_scan_tuple::call(node, estate)?;
        return Ok(false);
    }

    Ok(true)
}

/// `TidRangeRecheck` — access-method routine to recheck a tuple in
/// `EvalPlanQual`.
fn TidRangeRecheck<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !TidRangeEval(node, estate)? {
        return Ok(false);
    }

    // Assert(ItemPointerIsValid(&slot->tts_tid));
    let tts_tid = match node.ss.ss_ScanTupleSlot {
        Some(id) => estate.slot(id).tts_tid,
        None => return Ok(false),
    };
    debug_assert!(ItemPointerIsValid(&tts_tid));

    // Recheck the ctid is still within range.
    if ItemPointerCompare(&tts_tid, &node.trss_mintid) < 0
        || ItemPointerCompare(&tts_tid, &node.trss_maxtid) > 0
    {
        return Ok(false);
    }

    Ok(true)
}

// ===========================================================================
// `execScan.c` driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`),
// linked into `nodeTidrangescan.o` in C; reproduced here as private functions.
// ===========================================================================

type AccessMtd =
    for<'mcx> fn(&mut TidRangeScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;
type RecheckMtd =
    for<'mcx> fn(&mut TidRangeScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

/// `ExecScanFetch` — check interrupts and fetch the next potential tuple.
fn ExecScanFetch<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    epq_active: bool,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    seam::check_for_interrupts::call()?;

    if epq_active {
        // We are inside an EvalPlanQual recheck.
        let scanrelid: u32 = seam::scan_scanrelid::call(node)?;

        if scanrelid == 0 {
            // ForeignScan/CustomScan that pushed a join to the remote side.
            if seam::epq_param_is_member_of_ext_param::call(node)? {
                if !recheck_mtd(node, estate)? {
                    seam::exec_clear_scan_tuple::call(node, estate)?;
                }
                return Ok(!scan_tuple_is_null(node, estate));
            }
        } else if seam::epq_relsubs_done::call(node, scanrelid - 1)? {
            // Return empty slot, as either there is no EPQ tuple for this rel or
            // we already returned it.
            seam::exec_clear_scan_tuple::call(node, estate)?;
            return Ok(false);
        } else if seam::epq_relsubs_slot_present::call(node, scanrelid - 1)? {
            // Return replacement tuple provided by the EPQ caller.
            seam::epq_load_relsubs_slot::call(node, scanrelid - 1)?;

            // Mark to remember that we shouldn't return it again.
            seam::epq_set_relsubs_done::call(node, scanrelid - 1, true)?;

            // Return empty slot if we haven't got a test tuple.
            if scan_tuple_is_null(node, estate) {
                return Ok(false);
            }

            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate)? {
                seam::exec_clear_scan_tuple::call(node, estate)?;
                return Ok(false);
            }
            return Ok(true);
        } else if seam::epq_relsubs_rowmark_present::call(node, scanrelid - 1)? {
            // Fetch and return replacement tuple using a non-locking rowmark.
            seam::epq_set_relsubs_done::call(node, scanrelid - 1, true)?;

            if !seam::eval_plan_qual_fetch_row_mark::call(node, scanrelid)? {
                return Ok(false);
            }

            // Return empty slot if we haven't got a test tuple.
            if scan_tuple_is_null(node, estate) {
                return Ok(false);
            }

            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate)? {
                seam::exec_clear_scan_tuple::call(node, estate)?;
                return Ok(false);
            }
            return Ok(true);
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    access_mtd(node, estate)
}

/// `ExecScanExtended` — scan using the specified access method; optionally check
/// the tuple against `qual` and apply `proj_info`.
fn ExecScanExtended<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    epq_active: bool,
    has_qual: bool,
    has_proj_info: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // interrupt checks are in ExecScanFetch

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        seam::reset_per_tuple_expr_context::call(node, estate)?;
        return ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate);
    }

    // Reset per-tuple memory context to free any expression evaluation storage
    // allocated in the previous tuple cycle.
    seam::reset_per_tuple_expr_context::call(node, estate)?;

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let have_tuple = ExecScanFetch(node, epq_active, access_mtd, recheck_mtd, estate)?;

        // If the slot returned by the accessMtd contains NULL, then it means
        // there is nothing more to scan so we just return an empty slot, being
        // careful to use the projection result slot so it has correct tupleDesc.
        if !have_tuple {
            if has_proj_info {
                seam::exec_clear_proj_result_slot::call(node, estate)?;
                return Ok(false);
            } else {
                return Ok(false);
            }
        }

        // Place the current tuple into the expr context.
        seam::set_econtext_scantuple_to_scan_slot::call(node, estate)?;

        // Check that the current tuple satisfies the qual-clause.
        if !has_qual || seam::exec_qual::call(node, estate)? {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result tuple slot and
                // return it.
                return seam::exec_project::call(node, estate);
            } else {
                // Here, we aren't projecting, so just return scan tuple.
                return Ok(true);
            }
        } else {
            InstrCountFiltered1(node, 1);
        }

        // Tuple fails qual, so free per-tuple memory and try again.
        seam::reset_per_tuple_expr_context::call(node, estate)?;
    }
}

/// `ExecScan` — the non-inlined `execScan.c` driver. Equivalent to
/// `ExecScanExtended(node, access, recheck, node->ps.state->es_epq_active,
/// node->ps.qual, node->ps.ps_ProjInfo)`.
fn ExecScan<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let epq_active = seam::es_epq_active_present::call(node)?;
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

/// `InstrCountFiltered1(node, delta)` — bump `node->ps.instrument->nfiltered1`
/// when instrumentation is enabled.
#[inline]
fn InstrCountFiltered1(node: &mut TidRangeScanState, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered1 += delta as f64;
    }
}

/// `TupIsNull(slot)` — true if the slot is absent or marked empty
/// (`TTS_FLAG_EMPTY`).
#[inline]
fn scan_tuple_is_null(node: &TidRangeScanState, estate: &EStateData) -> bool {
    match node.ss.ss_ScanTupleSlot.map(|id| estate.slot(id)) {
        None => true,
        Some(slot) => (slot.tts_flags & TTS_FLAG_EMPTY) != 0,
    }
}

// ===========================================================================
// Public node entry points (1:1).
// ===========================================================================

/// `ExecTidRangeScan(node)` — scans the relation using TIDs and returns whether
/// the next qualifying tuple is available (stored in the node's scan/result
/// slot). Calls [`ExecScan`], passing the appropriate access and recheck method
/// callbacks ([`TidRangeNext`] / [`TidRangeRecheck`]).
pub fn ExecTidRangeScan<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    ExecScan(node, TidRangeNext, TidRangeRecheck, estate)
}

/// `ExecReScanTidRangeScan(node)` — reset the scan for re-execution.
pub fn ExecReScanTidRangeScan<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // mark scan as not in progress, and tid range list as not computed yet
    node.trss_inScan = false;

    // We must wait until TidRangeNext before calling table_rescan_tidrange.
    seam::exec_scan_rescan::call(node, estate)
}

/// `ExecEndTidRangeScan` — release any storage allocated through C routines.
pub fn ExecEndTidRangeScan<'mcx>(
    node: &mut TidRangeScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // TableScanDesc scan = node->ss.ss_currentScanDesc;
    // if (scan != NULL) table_endscan(scan);
    if node.ss_currentScanDesc.is_some() {
        seam::table_endscan::call(node, estate)?;
    }
    Ok(())
}

/// `ExecInitTidRangeScan` — initialize the TID range scan's state information,
/// create scan keys, and open the scan relation.
///
/// `node` is the `TidRangeScan` plan node produced by the planner; `eflags` is
/// the executor flags. In C this allocates the node via `makeNode`; here we
/// build the owned [`TidRangeScanState`] and return it by value.
pub fn ExecInitTidRangeScan<'mcx>(
    node: &TidRangeScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<TidRangeScanState<'mcx>> {
    let mcx = estate.es_query_cxt;

    // create state structure (makeNode(TidRangeScanState))
    let mut tidrangestate = TidRangeScanState {
        ss: ScanStateData::default(),
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        trss_tidexprs: vec_with_capacity_in(mcx, 0)?,
        trss_mintid: ItemPointerData::default(),
        trss_maxtid: ItemPointerData::default(),
        trss_inScan: false,
    };

    // tidrangestate->ss.ps.plan = (Plan *) node;
    // tidrangestate->ss.ps.state = estate;
    // tidrangestate->ss.ps.ExecProcNode = ExecTidRangeScan;
    //
    // The plan-node and EState links and the ExecProcNode install are wired by
    // the executor node factory (it owns the EState/plan-tree references and the
    // `ExecProcNodeMtd` slot, none of which are this crate's to construct).
    seam::init_plan_state_links::call(&mut tidrangestate, node)?;

    // Miscellaneous initialization: create expression context for node.
    // ExecAssignExprContext(estate, &tidrangestate->ss.ps);
    seam::exec_assign_expr_context::call(&mut tidrangestate, estate)?;

    // mark scan as not in progress, and TID range as not computed yet
    tidrangestate.trss_inScan = false;

    // open the scan relation
    // currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    // tidrangestate->ss.ss_currentRelation = currentRelation;
    seam::exec_open_scan_relation::call(&mut tidrangestate, node, eflags, estate)?;
    // tidrangestate->ss.ss_currentScanDesc = NULL;  /* no table scan here */
    tidrangestate.ss_currentScanDesc = None;

    // get the scan type from the relation descriptor.
    // ExecInitScanTupleSlot(estate, &tidrangestate->ss,
    //     RelationGetDescr(currentRelation), table_slot_callbacks(currentRelation));
    seam::exec_init_scan_tuple_slot::call(&mut tidrangestate, estate)?;

    // Initialize result type and projection.
    seam::exec_init_result_type_tl::call(&mut tidrangestate, estate)?;
    seam::exec_assign_scan_projection_info::call(&mut tidrangestate, estate)?;

    // initialize child expressions
    // tidrangestate->ss.ps.qual =
    //     ExecInitQual(node->scan.plan.qual, (PlanState *) tidrangestate);
    seam::exec_init_qual::call(&mut tidrangestate, node)?;

    TidExprListCreate(&mut tidrangestate, node, estate)?;

    // all done.
    Ok(tidrangestate)
}

/// Soft `elog(ERROR, msg)` — internal error (`ERRCODE_INTERNAL_ERROR`).
fn elog_internal(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `errcode(ERRCODE_OUT_OF_MEMORY)` for an allocation-safety failure.
fn out_of_memory(what: &str) -> PgError {
    PgError::error(alloc::format!("out of memory ({what})")).with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Install every seam this crate owns. Wired into `seams-init::init_all()`.
///
/// This node calls outward through the seams in
/// `backend-executor-nodeTidrangescan-seams` (expression compile/eval, the
/// execUtils/execScan init helpers, the table access methods, and the
/// `execScan.c` leaf operations / EvalPlanQual machinery); those slots are
/// installed by their owning subsystems when they land. The node itself owns no
/// inward-facing seam, so there is nothing to `set()` here yet.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
