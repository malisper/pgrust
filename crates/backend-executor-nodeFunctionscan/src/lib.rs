//! Port of `src/backend/executor/nodeFunctionscan.c` — support routines for
//! scanning `RangeFunctions` (set-returning functions in the range table).
//!
//! INTERFACE ROUTINES
//! - [`ExecFunctionScan`]        - scans a function
//! - [`ExecInitFunctionScan`]    - creates and initializes a functionscan node
//! - [`ExecEndFunctionScan`]     - releases any storage allocated
//! - [`ExecReScanFunctionScan`]  - rescans the function
//!
//! plus the file-scope statics [`FunctionNext`] (the workhorse the generic
//! `execScan.c` driver re-enters) and [`FunctionRecheck`].
//!
//! The node state is the owned [`FunctionScanState`] mutated through `&mut`
//! borrows; the C `PlanState.state` back-pointer is replaced by threading
//! `&mut EStateData` explicitly. Calls into unported / cyclic owners go through
//! their owners' seam crates: the generic scan driver (`execScan.c`),
//! expression init / qual init (`execExpr.c`), tuple-slot ops + result-type
//! setup (`execTuples.c` / `execUtils.c`), descriptor construction
//! (`tupdesc.c`), result-type lookup (`funcapi.c`/`nodeFuncs.c`), the output
//! tuplestore (`tuplestore.c`), and `exprCollation` (`nodeFuncs.c`).
//!
//! ## K2 BLOCKED runtime SRF call
//!
//! The runtime heart of a function scan — [`FunctionNext`] reading all rows
//! from the SRF into a tuplestore via `ExecMakeTableFunctionResult` (execSRF.c)
//! — is gated on the **#349 K2** keystone: the frame-based SRF invoke seam that
//! threads a live `&mut ReturnSetInfo` through by-OID `PGFunction` dispatch (the
//! #327 dual-fcinfo-home keystone). `ExecMakeTableFunctionResult` is wired here
//! through the `execSRF-seams::exec_make_table_function_result` seam, which
//! `execSRF.c` installs once it lands; until then a call into it is a CLEARLY
//! DOCUMENTED, loud seam-panic (NOT a `todo!`/fake row). So `ExecInitFunctionScan`
//! works, the node is reachable + downcastable, `ExecEndFunctionScan` /
//! `ExecReScanFunctionScan` work, and only the per-row SRF evaluation panics
//! into the not-yet-landed K2 owner. See the memory note
//! `execSRF-blocked-on-resultinfo-srf-callconv-keystone.md`.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use backend_access_common_toastdesc_seams as toastdesc;
use backend_access_common_tupdesc_seams as tupdesc;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execScan_seams as execScan;
use backend_executor_execSRF_seams as execSRF;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_nodes_core_seams as nodes_core;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_nodes_nodeFuncs_seams as nodeFuncs;
use backend_utils_sort_storage_seams as tuplestore;

use mcx::{alloc_in, vec_with_capacity_in, PgVec};
use types_core::primitive::AttrNumber;
use types_error::PgResult;
use types_nodes::executor::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use types_nodes::funcapi::TypeFuncClass;
use types_nodes::nodes::Node;
use types_nodes::nodefunctionscan::FunctionScan;
use types_nodes::{
    EStateData, FunctionScanPerFuncState, FunctionScanState, ScanDirectionIsForward, SlotId,
    TupleSlotKind,
};
use types_tuple::backend_access_common_heaptuple::Datum;

/// `RECORDOID` (catalog/pg_type_d.h) — the pseudo-type OID of an anonymous
/// composite (RECORD) type.
const RECORDOID: types_core::Oid = 2249;
/// `INT8OID` (catalog/pg_type_d.h) — the `bigint` type OID (the WITH ORDINALITY
/// column).
const INT8OID: types_core::Oid = 20;

/// nodeFunctionscan owns no inward seam crate: its only cross-cycle callers are
/// execProcnode's dispatch tables, which (like nodeTableFuncscan /
/// nodeValuesscan) reach it directly once wired, never across a cycle. So there
/// is nothing to install.
pub fn init_seams() {}

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `FunctionNext(node)` — the workhorse for [`ExecFunctionScan`].
///
/// On the first call (per function) reads all the SRF's rows into a tuplestore
/// via `ExecMakeTableFunctionResult`; subsequent calls fetch from the
/// tuplestore. The fast `simple` path fetches straight into the scan slot; the
/// general path fetches each function's rows into its `func_slot`, copies the
/// columns into the scan slot, and appends the WITH ORDINALITY column. Returns
/// `Some(scanslot)` (the C `return scanslot`, possibly cleared/empty) — the
/// caller treats an empty slot as end-of-scan via `TupIsNull`.
fn FunctionNext<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // get information from the estate and scan state:
    //   estate = node->ss.ps.state; direction = estate->es_direction;
    //   scanslot = node->ss.ss_ScanTupleSlot;
    let direction = estate.es_direction;
    let scanslot = node
        .ss
        .ss_ScanTupleSlot
        .expect("FunctionNext: ss_ScanTupleSlot not initialized");

    if node.simple {
        // Fast path for the trivial case: the function return type and scan
        // result type are the same, so we fetch the function result straight
        // into the scan result slot. No need to update ordinality or rowcounts
        // either.
        //   Tuplestorestate *tstore = node->funcstates[0].tstore;
        //   if (tstore == NULL) { node->funcstates[0].tstore = tstore =
        //       ExecMakeTableFunctionResult(...); tuplestore_rescan(tstore); }
        if node.funcstates[0].tstore.is_none() {
            make_table_function_result_into(node, 0, scanslot, estate)?;
            let tstore = node.funcstates[0]
                .tstore
                .as_deref_mut()
                .expect("FunctionNext: tstore set by ExecMakeTableFunctionResult");
            tuplestore::tuplestore_rescan::call(tstore)?;
        }

        // Get the next tuple from tuplestore.
        //   (void) tuplestore_gettupleslot(tstore,
        //       ScanDirectionIsForward(direction), false, scanslot);
        //   return scanslot;
        let forward = ScanDirectionIsForward(direction);
        let tstore = node.funcstates[0]
            .tstore
            .as_deref_mut()
            .expect("FunctionNext: simple tstore present");
        tuplestore::tuplestore_gettupleslot::call(tstore, forward, false, scanslot, estate)?;
        return Ok(Some(scanslot));
    }

    // Increment or decrement ordinal counter before checking for end-of-data,
    // so that we can move off either end of the result by 1 (and no more than
    // 1) without losing correct count.
    //   oldpos = node->ordinal;
    //   if (ScanDirectionIsForward(direction)) node->ordinal++; else node->ordinal--;
    let oldpos = node.ordinal;
    if ScanDirectionIsForward(direction) {
        node.ordinal += 1;
    } else {
        node.ordinal -= 1;
    }

    // Main loop over functions.
    //
    // We fetch the function results into func_slots (which match the function
    // return types), and then copy the values to scanslot (which matches the
    // scan result type), setting the ordinal column (if any) as well.
    //   ExecClearTuple(scanslot); att = 0; alldone = true;
    execTuples::exec_clear_tuple::call(estate, scanslot)?;

    let nfuncs = node.nfuncs as usize;
    // The scan slot's column values / nulls (the C wrote `scanslot->tts_values`
    // directly in the loop and then `ExecStoreVirtualTuple`); the owned slot
    // payload lives in execTuples, so we accumulate into local arrays and store
    // them via the `store_virtual_values` seam at the end.
    let mut scan_values: PgVec<'mcx, Datum<'mcx>> = PgVec::new_in(estate.es_query_cxt);
    let mut scan_isnull: PgVec<'mcx, bool> = PgVec::new_in(estate.es_query_cxt);

    let mut alldone = true;
    for funcno in 0..nfuncs {
        // If first time through, read all tuples from function and put them in
        // a tuplestore. Subsequent calls just fetch tuples from tuplestore.
        //   if (fs->tstore == NULL) { fs->tstore =
        //       ExecMakeTableFunctionResult(...); tuplestore_rescan(fs->tstore); }
        if node.funcstates[funcno].tstore.is_none() {
            // ExecMakeTableFunctionResult writes into the per-function result
            // slot (fs->func_slot), the function's own tupdesc descriptor.
            let func_slot = node.funcstates[funcno]
                .func_slot
                .expect("FunctionNext: func_slot not initialized for non-simple scan");
            make_table_function_result_into(node, funcno, func_slot, estate)?;
            let tstore = node.funcstates[funcno]
                .tstore
                .as_deref_mut()
                .expect("FunctionNext: tstore set by ExecMakeTableFunctionResult");
            tuplestore::tuplestore_rescan::call(tstore)?;
        }

        // Get the next tuple from tuplestore.
        //
        // If we have a rowcount for the function, and we know the previous read
        // position was out of bounds, don't try the read. This allows backward
        // scan to work when there are mixed row counts present.
        let func_slot = node.funcstates[funcno]
            .func_slot
            .expect("FunctionNext: func_slot not initialized for non-simple scan");
        if node.funcstates[funcno].rowcount != -1 && node.funcstates[funcno].rowcount < oldpos {
            //   ExecClearTuple(fs->func_slot);
            execTuples::exec_clear_tuple::call(estate, func_slot)?;
        } else {
            //   (void) tuplestore_gettupleslot(fs->tstore,
            //       ScanDirectionIsForward(direction), false, fs->func_slot);
            let forward = ScanDirectionIsForward(direction);
            let tstore = node.funcstates[funcno]
                .tstore
                .as_deref_mut()
                .expect("FunctionNext: tstore present");
            tuplestore::tuplestore_gettupleslot::call(tstore, forward, false, func_slot, estate)?;
        }

        let colcount = node.funcstates[funcno].colcount as usize;

        // if (TupIsNull(fs->func_slot)) { ... nulls ... } else { ... copy ... }
        if estate.slot(func_slot).is_empty() {
            // If we ran out of data for this function in the forward direction
            // then we now know how many rows it returned. The row count we store
            // is actually 1+ the actual number, because we have to position the
            // tuplestore 1 off its end sometimes.
            //   if (ScanDirectionIsForward(direction) && fs->rowcount == -1)
            //       fs->rowcount = node->ordinal;
            if ScanDirectionIsForward(direction) && node.funcstates[funcno].rowcount == -1 {
                node.funcstates[funcno].rowcount = node.ordinal;
            }

            // populate the result cols with nulls
            //   for (i = 0; i < fs->colcount; i++) { scanslot->tts_values[att] =
            //       (Datum) 0; scanslot->tts_isnull[att] = true; att++; }
            for _ in 0..colcount {
                scan_values.push(Datum::from_i32(0));
                scan_isnull.push(true);
            }
        } else {
            // we have a result, so just copy it to the result cols.
            //   slot_getallattrs(fs->func_slot);
            //   for (i = 0; i < fs->colcount; i++) {
            //       scanslot->tts_values[att] = fs->func_slot->tts_values[i];
            //       scanslot->tts_isnull[att] = fs->func_slot->tts_isnull[i]; att++; }
            let cols = execTuples::slot_getallattrs_by_id::call(estate, func_slot)?;
            for i in 0..colcount {
                let (value, isnull) = &cols[i];
                scan_values.push(value.clone());
                scan_isnull.push(*isnull);
            }

            // We're not done until every function result is exhausted; we pad
            // the shorter results with nulls until then.
            //   alldone = false;
            alldone = false;
        }
    }

    // ordinal col is always last, per spec.
    //   if (node->ordinality) { scanslot->tts_values[att] =
    //       Int64GetDatumFast(node->ordinal); scanslot->tts_isnull[att] = false; }
    if node.ordinality {
        scan_values.push(Datum::from_i64(node.ordinal));
        scan_isnull.push(false);
    }

    // If alldone, we just return the previously-cleared scanslot. Otherwise,
    // finish creating the virtual tuple.
    //   if (!alldone) ExecStoreVirtualTuple(scanslot);
    //   return scanslot;
    if !alldone {
        execTuples::store_virtual_values::call(
            estate,
            scanslot,
            scan_values.as_slice(),
            scan_isnull.as_slice(),
        )?;
    }

    Ok(Some(scanslot))
}

/// `ExecMakeTableFunctionResult(fs->setexpr, node->ss.ps.ps_ExprContext,
/// node->argcontext, fs->tupdesc, node->eflags & EXEC_FLAG_BACKWARD)` for the
/// `funcno`-th function, storing the resulting tuplestore in
/// `node.funcstates[funcno].tstore`.
///
/// K2 BLOCKED: the `exec_make_table_function_result` seam is owned by the
/// not-yet-ported `execSRF.c` (the #349 K2 frame-based SRF invoke keystone), so
/// the call into it is a documented loud seam-panic until that owner lands.
fn make_table_function_result_into<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    funcno: usize,
    _result_slot: SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .expect("FunctionNext: ps_ExprContext not initialized");
    let random_access = (node.eflags & EXEC_FLAG_BACKWARD) != 0;

    // Borrow the argcontext, the per-function setexpr, and the per-function
    // tupdesc; the seam consumes them while reading back the live
    // ReturnSetInfo. The C reads `fs->tupdesc` (the expected descriptor); the
    // descriptor stays owned by the node.
    let FunctionScanState {
        funcstates,
        argcontext,
        ..
    } = &mut *node;
    let argcontext = argcontext
        .as_mut()
        .expect("FunctionNext: argcontext not initialized");
    let fs = &mut funcstates[funcno];
    let setexpr = fs
        .setexpr
        .as_deref_mut()
        .expect("FunctionNext: setexpr not initialized");
    let expected_desc = fs
        .tupdesc
        .as_deref()
        .expect("FunctionNext: per-function tupdesc not initialized");

    let tstore = execSRF::exec_make_table_function_result::call(
        setexpr,
        econtext,
        argcontext,
        expected_desc,
        random_access,
        estate,
    )?;
    node.funcstates[funcno].tstore = Some(tstore);
    Ok(())
}

/// `FunctionRecheck(node, slot)` — access-method routine to recheck a tuple in
/// EvalPlanQual. Nothing to check for a function scan, so it always succeeds.
fn FunctionRecheck<'mcx>(
    _node: &mut FunctionScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // nothing to check
    Ok(true)
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// The `PlanState.ExecProcNode` callback installed by [`ExecInitFunctionScan`]:
/// `castNode(FunctionScanState, pstate)` then run [`ExecFunctionScan`].
fn exec_function_scan_node<'mcx>(
    pstate: &mut types_nodes::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    let node = match pstate.as_function_scan_state_mut() {
        Some(node) => node,
        None => panic!("castNode(FunctionScanState, pstate) failed: {pstate:?}"),
    };
    ExecFunctionScan(node, estate)
}

/// `ExecFunctionScan(pstate)` — scans the function sequentially and returns the
/// next qualifying tuple, by calling [`ExecScan`] with the function-scan
/// access/recheck methods.
pub fn ExecFunctionScan<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    // return ExecScan(&node->ss, (ExecScanAccessMtd) FunctionNext,
    //                 (ExecScanRecheckMtd) FunctionRecheck);
    execScan::exec_scan_function::call(node, estate, FunctionNext, FunctionRecheck)
}

/// `ExecInitFunctionScan(node, estate, eflags)` — create and initialize a
/// `FunctionScanState`.
///
/// The state tree is allocated in `estate.es_query_cxt` (C: `makeNode` in the
/// per-query context current during `ExecInitNode`), so initialization is
/// fallible on OOM. The plan back-link aliases the shared, read-only plan tree.
/// Panics if the node is not a `FunctionScan` (the C `castNode`).
pub fn ExecInitFunctionScan<'mcx>(
    node: &'mcx Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<mcx::PgBox<'mcx, FunctionScanState<'mcx>>> {
    let mcx = estate.es_query_cxt;

    let plan: &'mcx FunctionScan<'mcx> = match node {
        Node::FunctionScan(f) => f,
        other => panic!("castNode(FunctionScan, node) failed: {other:?}"),
    };

    //   int nfuncs = list_length(node->functions);
    let functions: &[types_nodes::rawnodes::RangeTblFunction<'mcx>] = match &plan.functions {
        Some(v) => v.as_slice(),
        None => &[],
    };
    let nfuncs = functions.len();

    // check for unsupported flags
    //   Assert(!(eflags & EXEC_FLAG_MARK));
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);

    // FunctionScan should not have any children.
    //   Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(plan.scan.plan.lefttree.is_none());
    debug_assert!(plan.scan.plan.righttree.is_none());

    // create new ScanState for node
    //   scanstate = makeNode(FunctionScanState);
    //   scanstate->ss.ps.plan = (Plan *) node; scanstate->ss.ps.state = estate;
    //   scanstate->ss.ps.ExecProcNode = ExecFunctionScan;
    //   scanstate->eflags = eflags;
    let mut scanstate = alloc_in(mcx, FunctionScanState::new_in(mcx))?;
    scanstate.ss.ps.plan = Some(node);
    scanstate.ss.ps.ExecProcNode = Some(exec_function_scan_node);
    scanstate.eflags = eflags;

    // are we adding an ordinality column?
    //   scanstate->ordinality = node->funcordinality;
    scanstate.ordinality = plan.funcordinality;

    //   scanstate->nfuncs = nfuncs;
    //   if (nfuncs == 1 && !node->funcordinality) scanstate->simple = true;
    //   else scanstate->simple = false;
    scanstate.nfuncs = nfuncs as i32;
    scanstate.simple = nfuncs == 1 && !plan.funcordinality;

    // Ordinal 0 represents the "before the first row" position.
    //   scanstate->ordinal = 0;
    scanstate.ordinal = 0;

    // Miscellaneous initialization: create expression context for node.
    //   ExecAssignExprContext(estate, &scanstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut scanstate.ss.ps)?;

    //   scanstate->funcstates = palloc(nfuncs * sizeof(FunctionScanPerFuncState));
    {
        let mut fss = vec_with_capacity_in(mcx, nfuncs)?;
        for _ in 0..nfuncs {
            fss.push(FunctionScanPerFuncState::new());
        }
        scanstate.funcstates = fss;
    }

    //   natts = 0; i = 0; foreach(lc, node->functions) { ... i++; }
    let mut natts: i32 = 0;
    for (i, rtfunc) in functions.iter().enumerate() {
        //   Node *funcexpr = rtfunc->funcexpr;
        //   int colcount = rtfunc->funccolcount;
        let funcexpr = rtfunc.funcexpr.as_deref();
        let colcount = rtfunc.funccolcount;

        //   fs->setexpr = ExecInitTableFunctionResult((Expr *) funcexpr,
        //       scanstate->ss.ps.ps_ExprContext, &scanstate->ss.ps);
        let econtext = scanstate
            .ss
            .ps
            .ps_ExprContext
            .expect("ExecInitFunctionScan: ps_ExprContext not set by ExecAssignExprContext");
        let setexpr = {
            let expr = funcexpr
                .and_then(|n| n.as_expr())
                .expect("ExecInitFunctionScan: funcexpr is not an Expr node");
            execSRF::exec_init_table_function_result::call(
                expr,
                econtext,
                &mut scanstate.ss.ps,
                estate,
            )?
        };
        scanstate.funcstates[i].setexpr = Some(setexpr);

        // Don't allocate the tuplestores; the actual calls to the functions do
        // that. NULL means that we have not called the function yet.
        //   fs->tstore = NULL; fs->rowcount = -1;
        scanstate.funcstates[i].tstore = None;
        scanstate.funcstates[i].rowcount = -1;

        // Now build a tupdesc showing the result type we expect from the
        // function. If we have a coldeflist then that takes priority; otherwise
        // use get_expr_result_type.
        //   if (rtfunc->funccolnames != NIL) { tupdesc = BuildDescFromLists(...);
        //       BlessTupleDesc(tupdesc); } else { ... }
        let tupdesc = if !rtfunc.funccolnames.is_empty() {
            //   tupdesc = BuildDescFromLists(rtfunc->funccolnames,
            //       rtfunc->funccoltypes, rtfunc->funccoltypmods,
            //       rtfunc->funccolcollations);
            let mut names: PgVec<'mcx, mcx::PgString<'mcx>> =
                vec_with_capacity_in(mcx, rtfunc.funccolnames.len())?;
            for n in rtfunc.funccolnames.iter() {
                let s = match &**n {
                    Node::String(sn) => sn.sval.clone_in(mcx)?,
                    other => panic!("FunctionScan funccolnames entry is not a String: {other:?}"),
                };
                names.push(s);
            }
            let td = toastdesc::build_desc_from_lists::call(
                mcx,
                names.as_slice(),
                rtfunc.funccoltypes.as_slice(),
                rtfunc.funccoltypmods.as_slice(),
                rtfunc.funccolcollations.as_slice(),
            )?;

            // For RECORD results, make sure a typmod has been assigned. (The
            // function should do this for itself, but let's cover things in case
            // it doesn't.)
            //   BlessTupleDesc(tupdesc);
            execTuples::bless_tuple_desc::call(mcx, td)?
        } else {
            //   functypclass = get_expr_result_type(funcexpr, &funcrettype, &tupdesc);
            let resolved = funcapi::get_expr_result_type::call(mcx, funcexpr)?;
            let functypclass = resolved.class;
            let funcrettype = resolved.result_type_id.unwrap_or(0);

            match functypclass {
                Some(TypeFuncClass::Composite) | Some(TypeFuncClass::CompositeDomain) => {
                    // Composite data type, e.g. a table's row type.
                    //   Assert(tupdesc); tupdesc = CreateTupleDescCopy(tupdesc);
                    let src = resolved
                        .result_tuple_desc
                        .as_deref()
                        .expect("get_expr_result_type: COMPOSITE class with NULL tupdesc");
                    Some(tupdesc::create_tupledesc_copy::call(mcx, src)?)
                }
                Some(TypeFuncClass::Scalar) => {
                    // Base data type, i.e. scalar.
                    //   tupdesc = CreateTemplateTupleDesc(1);
                    //   TupleDescInitEntry(tupdesc, 1, NULL, funcrettype, -1, 0);
                    //   TupleDescInitEntryCollation(tupdesc, 1, exprCollation(funcexpr));
                    let mut td = toastdesc::create_template_tuple_desc::call(mcx, 1)?;
                    toastdesc::tuple_desc_init_entry::call(
                        &mut td,
                        1 as AttrNumber,
                        "",
                        funcrettype,
                        -1,
                        0,
                    )?;
                    let collation = {
                        let expr = funcexpr
                            .and_then(|n| n.as_expr())
                            .expect("ExecInitFunctionScan: scalar funcexpr is not an Expr node");
                        nodeFuncs::exprCollation::call(expr)
                    };
                    tupdesc::tuple_desc_init_entry_collation::call(&mut td, 1 as AttrNumber, collation)?;
                    Some(alloc_in(mcx, td)?)
                }
                _ => {
                    // crummy error message, but parser should have caught this
                    //   elog(ERROR, "function in FROM has unsupported return type");
                    return Err(types_error::PgError::error(
                        "function in FROM has unsupported return type",
                    ));
                }
            }
        };

        //   fs->tupdesc = tupdesc; fs->colcount = colcount;
        scanstate.funcstates[i].tupdesc = tupdesc;
        scanstate.funcstates[i].colcount = colcount;

        // We only need separate slots for the function results if we are doing
        // ordinality or multiple functions; otherwise, we'll fetch function
        // results directly into the scan slot.
        //   if (!scanstate->simple) fs->func_slot = ExecInitExtraTupleSlot(estate,
        //       fs->tupdesc, &TTSOpsMinimalTuple); else fs->func_slot = NULL;
        if !scanstate.simple {
            let desc = clone_tupdesc(&scanstate.funcstates[i].tupdesc, mcx)?;
            let slot =
                execTuples::exec_init_extra_tuple_slot::call(estate, desc, TupleSlotKind::MinimalTuple)?;
            scanstate.funcstates[i].func_slot = Some(slot);
        } else {
            scanstate.funcstates[i].func_slot = None;
        }

        //   natts += colcount; i++;
        natts += colcount;
    }

    // Create the combined TupleDesc.
    //
    // If there is just one function without ordinality, the scan result tupdesc
    // is the same as the function result tupdesc --- except that we may stuff
    // new names into it below, so drop any rowtype label.
    let scan_tupdesc = if scanstate.simple {
        //   scan_tupdesc = CreateTupleDescCopy(scanstate->funcstates[0].tupdesc);
        //   scan_tupdesc->tdtypeid = RECORDOID; scan_tupdesc->tdtypmod = -1;
        let src = scanstate.funcstates[0]
            .tupdesc
            .as_deref()
            .expect("ExecInitFunctionScan: simple func tupdesc is NULL");
        let mut copy = tupdesc::create_tupledesc_copy::call(mcx, src)?;
        copy.tdtypeid = RECORDOID;
        copy.tdtypmod = -1;
        Some(copy)
    } else {
        //   AttrNumber attno = 0; if (node->funcordinality) natts++;
        //   scan_tupdesc = CreateTemplateTupleDesc(natts);
        let mut natts_total = natts;
        if plan.funcordinality {
            natts_total += 1;
        }
        let mut scan_tupdesc = toastdesc::create_template_tuple_desc::call(mcx, natts_total)?;

        let mut attno: AttrNumber = 0;
        //   for (i = 0; i < nfuncs; i++) {
        //       TupleDesc tupdesc = scanstate->funcstates[i].tupdesc;
        //       int colcount = scanstate->funcstates[i].colcount;
        //       for (j = 1; j <= colcount; j++)
        //           TupleDescCopyEntry(scan_tupdesc, ++attno, tupdesc, j); }
        for i in 0..nfuncs {
            let colcount = scanstate.funcstates[i].colcount;
            // The per-function descriptor is read while `scan_tupdesc` is
            // written; they are disjoint (scan_tupdesc is the local, the source
            // is `funcstates[i].tupdesc`), so split the borrow by binding the
            // source ref to a clone-free view via the node.
            for j in 1..=colcount {
                attno += 1;
                let src = scanstate.funcstates[i]
                    .tupdesc
                    .as_deref()
                    .expect("ExecInitFunctionScan: per-function tupdesc is NULL");
                tupdesc::tuple_desc_copy_entry::call(
                    &mut scan_tupdesc,
                    attno,
                    src,
                    j as AttrNumber,
                )?;
            }
        }

        // If doing ordinality, add a column of type "bigint" at the end.
        //   if (node->funcordinality) TupleDescInitEntry(scan_tupdesc, ++attno,
        //       NULL, INT8OID, -1, 0);
        if plan.funcordinality {
            attno += 1;
            toastdesc::tuple_desc_init_entry::call(&mut scan_tupdesc, attno, "", INT8OID, -1, 0)?;
        }

        //   Assert(attno == natts);
        debug_assert_eq!(attno as i32, natts_total);
        Some(alloc_in(mcx, scan_tupdesc)?)
    };

    // Initialize scan slot and type.
    //   ExecInitScanTupleSlot(estate, &scanstate->ss, scan_tupdesc,
    //       &TTSOpsMinimalTuple);
    execTuples::exec_init_scan_tuple_slot::call(
        estate,
        &mut scanstate.ss,
        scan_tupdesc,
        TupleSlotKind::MinimalTuple,
    )?;

    // Initialize result slot, type and projection.
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

    // Create a memory context that ExecMakeTableFunctionResult can use to
    // evaluate function arguments in. We can't use the per-tuple context for
    // this because it gets reset too often; but we don't want to leak evaluation
    // results into the query-lifespan context either.
    //   scanstate->argcontext = AllocSetContextCreate(CurrentMemoryContext,
    //       "Table function arguments", ALLOCSET_DEFAULT_SIZES);
    scanstate.argcontext = Some(mcx.context().new_child("Table function arguments"));

    Ok(scanstate)
}

/// `ExecEndFunctionScan(node)` — frees any storage allocated through C routines,
/// releasing each function's tuplestore.
pub fn ExecEndFunctionScan<'mcx>(node: &mut FunctionScanState<'mcx>) -> PgResult<()> {
    // Release slots and tuplestore resources.
    //   for (i = 0; i < node->nfuncs; i++) {
    //       FunctionScanPerFuncState *fs = &node->funcstates[i];
    //       if (fs->tstore != NULL) { tuplestore_end(node->funcstates[i].tstore);
    //           fs->tstore = NULL; } }
    let nfuncs = node.nfuncs as usize;
    for i in 0..nfuncs {
        if let Some(tstore) = node.funcstates[i].tstore.take() {
            tuplestore::tuplestore_end::call(tstore);
        }
    }
    Ok(())
}

/// `ExecReScanFunctionScan(node)` — rescans the relation.
pub fn ExecReScanFunctionScan<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    //   FunctionScan *scan = (FunctionScan *) node->ss.ps.plan;
    //   Bitmapset *chgparam = node->ss.ps.chgParam;
    let chgparam = node.ss.ps.chgParam.is_some();

    //   if (node->ss.ps.ps_ResultTupleSlot)
    //       ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    if let Some(result_slot) = node.ss.ps.ps_ResultTupleSlot {
        execTuples::exec_clear_tuple::call(estate, result_slot)?;
    }

    //   for (i = 0; i < node->nfuncs; i++) {
    //       FunctionScanPerFuncState *fs = &node->funcstates[i];
    //       if (fs->func_slot) ExecClearTuple(fs->func_slot); }
    let nfuncs = node.nfuncs as usize;
    for i in 0..nfuncs {
        if let Some(func_slot) = node.funcstates[i].func_slot {
            execTuples::exec_clear_tuple::call(estate, func_slot)?;
        }
    }

    //   ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan_function::call(node, estate)?;

    // Here we have a choice whether to drop the tuplestores (and recompute the
    // function outputs) or just rescan them. We must recompute if an expression
    // contains changed parameters, else we rescan.
    //   if (chgparam) { i = 0; foreach(lc, scan->functions) { RangeTblFunction
    //       *rtfunc = lfirst(lc); if (bms_overlap(chgparam, rtfunc->funcparams))
    //       { if (node->funcstates[i].tstore != NULL) { tuplestore_end(...);
    //       node->funcstates[i].tstore = NULL; } node->funcstates[i].rowcount =
    //       -1; } i++; } }
    if chgparam {
        // The plan's per-function `funcparams` bitmapsets decide which functions
        // recompute. The plan is read through the node's owned plan back-link.
        let chg = node.ss.ps.chgParam.as_deref();
        let overlaps: alloc::vec::Vec<bool> = match node.ss.ps.plan {
            Some(Node::FunctionScan(scan)) => match &scan.functions {
                Some(list) => list
                    .iter()
                    .map(|rtfunc| {
                        nodes_core::bms_overlap::call(chg, rtfunc.funcparams.as_deref())
                    })
                    .collect(),
                None => alloc::vec::Vec::new(),
            },
            _ => panic!("ExecReScanFunctionScan: plan is not a FunctionScan node"),
        };
        for (i, overlap) in overlaps.into_iter().enumerate() {
            if overlap {
                if let Some(tstore) = node.funcstates[i].tstore.take() {
                    tuplestore::tuplestore_end::call(tstore);
                }
                node.funcstates[i].rowcount = -1;
            }
        }
    }

    // Reset ordinality counter.
    //   node->ordinal = 0;
    node.ordinal = 0;

    // Make sure we rewind any remaining tuplestores.
    //   for (i = 0; i < node->nfuncs; i++)
    //       if (node->funcstates[i].tstore != NULL)
    //           tuplestore_rescan(node->funcstates[i].tstore);
    for i in 0..nfuncs {
        if let Some(tstore) = node.funcstates[i].tstore.as_deref_mut() {
            tuplestore::tuplestore_rescan::call(tstore)?;
        }
    }

    Ok(())
}

// ===========================================================================
// Small in-crate helpers.
// ===========================================================================

/// Deep-copy a per-function `TupleDesc` into `mcx` (the C passes `fs->tupdesc`
/// by pointer to `ExecInitExtraTupleSlot`, which fixes the slot to that
/// descriptor; the owned model gives the slot its own copy so the per-function
/// `tupdesc` stays owned by the node).
fn clone_tupdesc<'mcx>(
    src: &types_tuple::heaptuple::TupleDesc<'mcx>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<types_tuple::heaptuple::TupleDesc<'mcx>> {
    match src.as_deref() {
        Some(td) => Ok(Some(tupdesc::create_tupledesc_copy::call(mcx, td)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests;
