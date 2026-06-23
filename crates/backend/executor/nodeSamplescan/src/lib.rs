//! Port of `nodeSamplescan.c` — support routines for sample scans of relations
//! (`TABLESAMPLE`).
//!
//! The node state machine is ported 1:1 from PostgreSQL 18.3 — the scan
//! workhorses [`SampleNext`] / [`SampleRecheck`], the `ExecProcNode` callback
//! [`ExecSampleScan`], the public [`ExecInitSampleScan`] / [`ExecEndSampleScan`]
//! / [`ExecReScanSampleScan`], and the two file-scope statics
//! [`tablesample_init`] / [`tablesample_getnext`].
//!
//! `ExecScan` is the non-inlined `execScan.c` driver that [`ExecSampleScan`]
//! calls; its body (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`) is
//! reproduced here as private functions of this crate, so the qual / projection
//! / EvalPlanQual logic stays faithful while every leaf operation goes through a
//! seam. Every operation in a subsystem below the node layer (the table access
//! methods, expression compile/eval, the execUtils/execScan init helpers, the
//! tablesample-method registry/callbacks, the PRNG/hash helpers, and the
//! `execScan.c` leaf operations / EvalPlanQual machinery) goes through this
//! crate's seam surface, defaulting to a loud panic until the owner installs it.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use tablesample_core_seams as tsm;
use nodeSamplescan_seams as seam;
use ::mcx::vec_with_capacity_in;
use ::datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT, ERRCODE_INVALID_TABLESAMPLE_REPEAT,
    ERRCODE_OUT_OF_MEMORY,
};
use ::nodes::execnodes::{EStateData, ScanStateData};
use ::nodes::executor::TTS_FLAG_EMPTY;
use samplescan::{SampleScan, SampleScanState, TableSampleClause};

// ===========================================================================
// Internal access/recheck method types.
//
// In C these are `ExecScanAccessMtd` / `ExecScanRecheckMtd` (function pointers
// reinterpreted from `SampleNext` / `SampleRecheck`). Within this crate the scan
// helpers always use `SampleNext` / `SampleRecheck`, so we model the method
// "pointers" as plain function items.
//
// `SampleNext` stores the next tuple in the node's own `ss_ScanTupleSlot` and
// returns `true` when a tuple is available; `false` means "no more tuples" (the
// C `accessMtd` returns NULL). `SampleRecheck` rechecks the current scan tuple.
// ===========================================================================

type AccessMtd =
    for<'mcx> fn(&mut SampleScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;
type RecheckMtd = for<'mcx> fn(&mut SampleScanState<'mcx>, &mut EStateData<'mcx>) -> PgResult<bool>;

// ===========================================================================
//                              Scan Support
// ===========================================================================

/// `SampleNext` — the workhorse for [`ExecSampleScan`]. On the first call within
/// a scan it runs [`tablesample_init`], then fetches the next tuple from the
/// TABLESAMPLE method and stores it in the result slot.
///
/// Returns `true` if a tuple was stored in `node.ss.ss_ScanTupleSlot`, or
/// `false` when the relation is exhausted.
fn SampleNext<'mcx>(
    node: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // if this is first call within a scan, initialize
    if !node.begun {
        tablesample_init(node, estate)?;
    }

    // get the next tuple, and store it in our result slot
    tablesample_getnext(node, estate)
}

/// `SampleRecheck` — access-method routine to recheck a tuple in EvalPlanQual.
///
/// No need to recheck for SampleScan, since like SeqScan we don't pass any
/// checkable keys to `heap_beginscan`.
fn SampleRecheck<'mcx>(
    _node: &mut SampleScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    Ok(true)
}

/// `ExecSampleScan(node)` — scans the relation using the sampling method and
/// returns whether the next qualifying tuple is available (stored in the node's
/// scan/result slot). Calls [`ExecScan`] passing the appropriate access-method
/// functions; installed into `PlanState.ExecProcNode` by [`ExecInitSampleScan`].
pub fn ExecSampleScan<'mcx>(
    node: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    ExecScan(node, SampleNext, SampleRecheck, estate)
}

// ===========================================================================
// `execScan.c` driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`),
// linked into `nodeSamplescan.o` in C; reproduced here as private functions.
// ===========================================================================

/// `ExecScanFetch` — check interrupts and fetch the next potential tuple.
///
/// Substitutes a test tuple if inside an EvalPlanQual recheck; otherwise runs
/// the access method's next-tuple routine.
fn ExecScanFetch<'mcx>(
    node: &mut SampleScanState<'mcx>,
    epq_active: bool,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    seam::check_for_interrupts::call()?;

    if epq_active {
        // We are inside an EvalPlanQual recheck. Return the test tuple if one is
        // available, after rechecking any access-method-specific conditions.
        let scanrelid: u32 = seam::scan_scanrelid::call(node)?;

        if scanrelid == 0 {
            // ForeignScan/CustomScan that pushed a join to the remote side. If it
            // is a descendant node in the EPQ recheck plan tree, run the recheck
            // method; otherwise fall through to the access method below.
            if seam::epq_param_is_member_of_ext_param::call(node, estate)? {
                // The recheck method is responsible not only for rechecking the
                // scan/join quals but also for storing the correct tuple.
                if !recheck_mtd(node, estate)? {
                    seam::exec_clear_scan_tuple::call(node, estate)?; // would not be returned by scan
                }
                return Ok(!scan_tuple_is_null(node, estate));
            }
        } else if seam::epq_relsubs_done::call(node, scanrelid - 1, estate)? {
            // Return empty slot, as either there is no EPQ tuple for this rel or
            // we already returned it.
            seam::exec_clear_scan_tuple::call(node, estate)?;
            return Ok(false);
        } else if seam::epq_relsubs_slot_present::call(node, scanrelid - 1, estate)? {
            // Return replacement tuple provided by the EPQ caller.
            seam::epq_load_relsubs_slot::call(node, scanrelid - 1, estate)?;

            // Mark to remember that we shouldn't return it again.
            seam::epq_set_relsubs_done::call(node, scanrelid - 1, true, estate)?;

            // Return empty slot if we haven't got a test tuple.
            if scan_tuple_is_null(node, estate) {
                return Ok(false);
            }

            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate)? {
                seam::exec_clear_scan_tuple::call(node, estate)?; // would not be returned by scan
                return Ok(false);
            }
            return Ok(true);
        } else if seam::epq_relsubs_rowmark_present::call(node, scanrelid - 1, estate)? {
            // Fetch and return replacement tuple using a non-locking rowmark.
            //
            // Mark to remember that we shouldn't return more.
            seam::epq_set_relsubs_done::call(node, scanrelid - 1, true, estate)?;

            if !seam::eval_plan_qual_fetch_row_mark::call(node, scanrelid, estate)? {
                return Ok(false);
            }

            // Return empty slot if we haven't got a test tuple.
            if scan_tuple_is_null(node, estate) {
                return Ok(false);
            }

            // Check if it meets the access-method conditions.
            if !recheck_mtd(node, estate)? {
                seam::exec_clear_scan_tuple::call(node, estate)?; // would not be returned by scan
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
    node: &mut SampleScanState<'mcx>,
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
        //
        // Check for non-null qual here to avoid a function call to ExecQual()
        // when the qual is null.
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
    node: &mut SampleScanState<'mcx>,
    access_mtd: AccessMtd,
    recheck_mtd: RecheckMtd,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let epq_active = seam::es_epq_active_present::call(node, estate)?;
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
/// when instrumentation is enabled (`InstrCountFiltered1` macro in executor.h).
#[inline]
fn InstrCountFiltered1(node: &mut SampleScanState, delta: u64) {
    if let Some(instr) = node.ss.ps.instrument.as_mut() {
        instr.nfiltered1 += delta as f64;
    }
}

/// `TupIsNull(node->ss.ss_ScanTupleSlot)` for the node's scan slot (a `SlotId`
/// resolved through the estate arena) — true if absent or marked empty.
#[inline]
fn scan_tuple_is_null(node: &SampleScanState, estate: &EStateData) -> bool {
    match node.ss.ss_ScanTupleSlot.map(|id| estate.slot(id)) {
        None => true,
        Some(slot) => (slot.tts_flags & TTS_FLAG_EMPTY) != 0,
    }
}

// ===========================================================================
//                          Public node entry points
// ===========================================================================

/// `node->tablesample` — the `TableSampleClause` the `SampleScan` points at, or
/// `None`. In C this is a `TableSampleClause *` reached straight off the
/// `SampleScan`; the typed model keeps that link directly.
#[inline]
fn sample_clause<'a, 'mcx>(node: &'a SampleScan<'mcx>) -> Option<&'a TableSampleClause<'mcx>> {
    node.tablesample.as_deref()
}

/// `ExecInitSampleScan` — create and initialize a `SampleScanState`.
///
/// In C this allocates the node via `makeNode(SampleScanState)`; here we build
/// the owned [`SampleScanState`] and return it. The plan-node / EState links and
/// the `ExecProcNode` install are wired by the executor node factory (it owns
/// those references) through [`seam::init_plan_state_links`].
pub fn ExecInitSampleScan<'mcx>(
    node: &SampleScan<'mcx>,
    plan_node: &'mcx ::nodes::nodes::Node<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<SampleScanState<'mcx>> {
    // Assert(outerPlan(node) == NULL); Assert(innerPlan(node) == NULL);
    debug_assert!(node.scan.plan.lefttree.is_none());
    debug_assert!(node.scan.plan.righttree.is_none());

    let mcx = estate.es_query_cxt;

    // create state structure (makeNode(SampleScanState))
    let mut scanstate = SampleScanState {
        ss: ScanStateData::default(),
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        args: vec_with_capacity_in(mcx, 0)?,
        repeatable: None,
        tsmroutine: None,
        tsm_state: None,
        use_bulkread: false,
        use_pagemode: false,
        begun: false,
        seed: 0,
        donetuples: 0,
        haveblock: false,
        done: false,
    };

    // scanstate->ss.ps.plan = (Plan *) node; The plan back-link aliases the
    // caller's read-only plan node (the `&Node` the dispatcher holds).
    scanstate.ss.ps.plan = Some(plan_node);
    // scanstate->ss.ps.state = estate;
    // scanstate->ss.ps.ExecProcNode = ExecSampleScan; The ExecProcNode slot is
    // an `ExecProcNodeMtd` over the central `PlanStateNode`, which this crate
    // cannot name, so the dispatch crate installs it through this seam.
    seam::init_plan_state_links::call(&mut scanstate, node)?;

    // Miscellaneous initialization: create expression context for node.
    seam::exec_assign_expr_context::call(&mut scanstate, estate)?;

    // open the scan relation
    seam::exec_open_scan_relation::call(&mut scanstate, node, eflags, estate)?;

    // we won't set up the HeapScanDesc till later
    scanstate.ss_currentScanDesc = None;

    // and create slot with appropriate rowtype
    // ExecInitScanTupleSlot(estate, &scanstate->ss,
    //     RelationGetDescr(rel), table_slot_callbacks(rel));
    seam::exec_init_scan_tuple_slot::call(&mut scanstate, estate)?;

    // Initialize result type and projection.
    seam::exec_init_result_type_tl::call(&mut scanstate, estate)?;
    seam::exec_assign_scan_projection_info::call(&mut scanstate, estate)?;

    // initialize child expressions
    // scanstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, scanstate);
    seam::exec_init_qual::call(&mut scanstate, node, estate)?;

    // scanstate->args = ExecInitExprList(tsc->args, scanstate);
    seam::exec_init_expr_list::call(&mut scanstate, node, estate)?;
    // scanstate->repeatable = ExecInitExpr(tsc->repeatable, scanstate);
    seam::exec_init_repeatable_expr::call(&mut scanstate, node, estate)?;

    // If we don't have a REPEATABLE clause, select a random seed. We want to do
    // this just once, since the seed shouldn't change over rescans.
    let no_repeatable = sample_clause(node)
        .map(|tsc| tsc.repeatable.is_none())
        .unwrap_or(true);
    if no_repeatable {
        scanstate.seed = seam::pg_prng_uint32_global::call()?;
    }

    // Finally, initialize the TABLESAMPLE method handler.
    // tsm = GetTsmRoutine(tsc->tsmhandler);
    // The TsmRoutine is charged to the node's per-query context; the real
    // owner (access/tablesample/tablesample.c GetTsmRoutine) reads only the
    // handler OID, so this goes through the OID-keyed seam (shared with the
    // parser's transformRangeTableSample).
    let tsmhandler = sample_clause(node).map(|tsc| tsc.tsmhandler).unwrap_or(0);
    scanstate.tsmroutine = Some(tsm::get_tsm_routine_oid::call(estate.es_query_cxt, tsmhandler)?);
    scanstate.tsm_state = None;

    // if (tsm->InitSampleScan) tsm->InitSampleScan(scanstate, eflags);
    if tsm::tsm_has_init_sample_scan::call(&scanstate)? {
        tsm::tsm_init_sample_scan::call(&mut scanstate, eflags)?;
    }

    // We'll do BeginSampleScan later; we can't evaluate params yet.
    scanstate.begun = false;

    Ok(scanstate)
}

/// `ExecEndSampleScan` — free any storage allocated through C routines.
pub fn ExecEndSampleScan<'mcx>(node: &mut SampleScanState<'mcx>) -> PgResult<()> {
    // Tell sampling function that we finished the scan.
    // if (node->tsmroutine->EndSampleScan) node->tsmroutine->EndSampleScan(node);
    if tsm::tsm_has_end_sample_scan::call(node)? {
        tsm::tsm_end_sample_scan::call(node)?;
    }

    // close heap scan
    if node.ss_currentScanDesc.is_some() {
        seam::table_endscan::call(node)?;
    }
    Ok(())
}

/// `ExecReScanSampleScan` — rescan the relation.
pub fn ExecReScanSampleScan<'mcx>(
    node: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Remember we need to do BeginSampleScan again (if we did it at all).
    node.begun = false;
    node.done = false;
    node.haveblock = false;
    node.donetuples = 0;

    seam::exec_scan_rescan::call(node, estate)
}

// ===========================================================================
//          File-scope statics: tablesample_init / tablesample_getnext
// ===========================================================================

/// Initialize the TABLESAMPLE method: evaluate params and call `BeginSampleScan`.
///
/// The `params` array (C's `palloc(nargs * sizeof(Datum))` / `pfree(params)`) is
/// built into the executor's per-query memory context — the faithful repo analog
/// of the C node's transient `palloc`, reclaimed when that context is reset at
/// `ExecEndNode`.
fn tablesample_init<'mcx>(
    scanstate: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    scanstate.donetuples = 0;

    // params = (Datum *) palloc(list_length(scanstate->args) * sizeof(Datum));
    let mcx = estate.es_query_cxt;
    let nargs = scanstate.args.len();
    let mut params = vec_with_capacity_in::<Datum>(mcx, nargs)
        .map_err(|_| out_of_memory("TABLESAMPLE parameter array"))?;

    // i = 0; foreach(arg, scanstate->args) { params[i] = ...; i++; }
    for i in 0..nargs {
        let mut isnull = false;
        // params[i] = ExecEvalExprSwitchContext(argstate, econtext, &isnull);
        let datum = seam::exec_eval_arg_in_per_tuple_context::call(scanstate, i, &mut isnull, estate)?;
        if isnull {
            return Err(PgError::error("TABLESAMPLE parameter cannot be null")
                .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT));
        }
        params.push(datum);
    }

    let seed: u32 = if scanstate.repeatable.is_some() {
        let mut isnull = false;
        // datum = ExecEvalExprSwitchContext(scanstate->repeatable, econtext, &isnull);
        let datum = seam::exec_eval_repeatable_in_per_tuple_context::call(scanstate, &mut isnull, estate)?;
        if isnull {
            return Err(
                PgError::error("TABLESAMPLE REPEATABLE parameter cannot be null")
                    .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_REPEAT),
            );
        }

        // The REPEATABLE parameter has been coerced to float8 by the parser. We
        // use hashfloat8() to convert the supplied value into a suitable seed;
        // for regression testing that gives REPEATABLE(0) a machine-independent
        // result. `seed = DatumGetUInt32(DirectFunctionCall1(hashfloat8, datum));`
        seam::hashfloat8::call(datum)?
    } else {
        // Use the seed selected by ExecInitSampleScan.
        scanstate.seed
    };

    // Set default values for params that BeginSampleScan can adjust.
    scanstate.use_bulkread = true;
    scanstate.use_pagemode = true;

    // Let tablesample method do its thing.
    // tsm->BeginSampleScan(scanstate, params, list_length(args), seed);
    tsm::tsm_begin_sample_scan::call(scanstate, &params, seed)?;

    // We'll use syncscan if there's no NextSampleBlock function.
    // allow_sync = (tsm->NextSampleBlock == NULL);
    let allow_sync = !tsm::tsm_has_next_sample_block::call(scanstate)?;

    // Now we can create or reset the HeapScanDesc.
    if scanstate.ss_currentScanDesc.is_none() {
        // scanstate->ss.ss_currentScanDesc = table_beginscan_sampling(rel,
        //     es_snapshot, 0, NULL, use_bulkread, allow_sync, use_pagemode);
        seam::table_beginscan_sampling::call(scanstate, allow_sync, estate)?;
    } else {
        // table_rescan_set_params(scan, NULL, use_bulkread, allow_sync,
        //                         use_pagemode);
        seam::table_rescan_set_params::call(scanstate, allow_sync, estate)?;
    }

    // pfree(params): the per-query context reclaims it at ExecEndNode.

    // And we're initialized.
    scanstate.begun = true;

    Ok(())
}

/// Get next tuple from TABLESAMPLE method.
///
/// On success the visible tuple lives in `scanstate.ss.ss_ScanTupleSlot`;
/// returns `Ok(true)` if a tuple is available, `Ok(false)` when the relation is
/// exhausted (the C `return slot;` / `return NULL;`).
fn tablesample_getnext<'mcx>(
    scanstate: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // ExecClearTuple(slot);
    seam::exec_clear_scan_tuple::call(scanstate, estate)?;

    if scanstate.done {
        return Ok(false);
    }

    loop {
        if !scanstate.haveblock {
            if !seam::table_scan_sample_next_block::call(scanstate, estate)? {
                scanstate.haveblock = false;
                scanstate.done = true;

                // exhausted relation
                return Ok(false);
            }

            scanstate.haveblock = true;
        }

        if !seam::table_scan_sample_next_tuple::call(scanstate, estate)? {
            // If we get here, it means we've exhausted the items on this page and
            // it's time to move to the next.
            scanstate.haveblock = false;
            continue;
        }

        // Found visible tuple, return it.
        break;
    }

    scanstate.donetuples += 1;

    Ok(true)
}

/// `errcode(ERRCODE_OUT_OF_MEMORY)` for an allocation-safety failure.
fn out_of_memory(what: &str) -> PgError {
    PgError::error(alloc::format!("out of memory ({what})")).with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// Erase an owned `SampleScanState` into the central
/// `PlanStateNode::SampleScan` carrier (`PgBox<dyn SampleScanStateLive>`).
/// `SampleScanState` lives in `types-samplescan` (ABOVE `types-nodes`), so the
/// enum carries it type-erased; this is the same `into_raw`/`from_raw` unsize
/// the `erase_agg_state` carrier uses (the `allocator_api2` `PgBox` does not
/// auto-coerce unsized types on stable). The concrete type is recovered via the
/// tag-checked `downcast_sample_scan_state_*` helpers.
pub fn erase_sample_scan_state<'mcx>(
    boxed: ::mcx::PgBox<'mcx, SampleScanState<'mcx>>,
) -> ::mcx::PgBox<'mcx, dyn ::nodes::samplescanstate_carrier::SampleScanStateLive<'mcx> + 'mcx> {
    let (ptr, alloc) = ::mcx::PgBox::into_raw_with_allocator(boxed);
    // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast only
    // attaches the `dyn SampleScanStateLive` vtable (the established erase
    // pattern, identical to `erase_agg_state`).
    unsafe {
        ::mcx::PgBox::from_raw_in(
            ptr as *mut (dyn ::nodes::samplescanstate_carrier::SampleScanStateLive<'mcx> + 'mcx),
            alloc,
        )
    }
}

/// Install every seam this crate owns. Wired into `seams-init::init_all()`.
///
/// This node calls outward through the seams in
/// `backend-executor-nodeSamplescan-seams` (the table access methods,
/// expression compile/eval, the execUtils/execScan init helpers, the
/// tablesample-method registry/callbacks, the PRNG/hash helpers, and the
/// `execScan.c` leaf operations / EvalPlanQual machinery); those slots are
/// installed by their owning subsystems when they land. The node itself owns no
/// inward-facing seam, so there is nothing to `set()` here yet.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
