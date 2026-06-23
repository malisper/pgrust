//! Port of `src/backend/executor/execCurrent.c` — executor support for
//! `WHERE CURRENT OF cursor`.
//!
//! Entry point (1:1 with the C):
//! - [`exec_current_of`] (`execCurrentOf`)
//!
//! file-static helpers, ported in-crate as their owned logic:
//! - `fetch_cursor_param_value` — the numParams/param_id bounds and the
//!   `OidIsValid && !isnull` gate, and the type-mismatch / no-value error
//!   construction. The `paramFetch`-hook dispatch + `refcursor` text decode is
//!   the one foreign-live-state step (`fetch_cursor_param` seam → execMain).
//! - `search_plan_tree` — the `PlanState`-tree walk to the scan node for the
//!   target table, dispatching on the owned [`PlanStateNode`] enum. The scan /
//!   `Append` / `Result` / `Limit` / `SubqueryScan` node-state variants are
//!   added to `PlanStateNode` as their executor units land; until then those
//!   cases are the C `default:` (no descent) — the wildcard arm.
//! - `item_pointer_is_valid` — lives in `types_tuple` (`ItemPointerIsValid`).
//!
//! `execCurrentOf` navigates the *live* executor state of a running cursor —
//! the portal manager's `PortalData`, the running query's `EState`
//! (`es_rowmarks`, range table, slot pool) and its `PlanState` tree. Those are
//! owned by `portalmem` / `execMain`, so the decision logic runs inside the
//! callback the [`with_running_cursor`](portalmem_seams::with_running_cursor)
//! seam lends; the per-scan-type TID extraction (which reaches the concrete
//! scan-node states and `slot_getsysattr`) is the `scan_node_extract_tid` seam
//! (execMain). Relation/type-name lookups go through the `lsyscache` /
//! `format_type` seams.
//!
//! All error paths reproduce the C SQLSTATEs and messages: `34000` (cursor does
//! not exist), `24000` (not a SELECT / held / no row / not a simply updatable
//! scan / multiple or missing FOR UPDATE refs), `42704` (no value for a
//! cursor-name parameter), `42804` (parameter type changed since planning), and
//! the bare `elog(ERROR)` internal "cache lookup failed for relation %u".

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;
use alloc::format;

use execMain_seams as execMain;
use format_type_seams as format_type;
use lsyscache_seams as lsyscache;
use portalmem_seams as portalmem;

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_CURSOR_STATE,
    ERRCODE_UNDEFINED_CURSOR, ERRCODE_UNDEFINED_OBJECT,
};
use ::nodes::execnodes::{
    CurrentOfTid, FetchedCursorParam, RunningCursorState, ScanStateData, ScanTidOutcome,
};
use ::nodes::nodes::{
    T_AppendState, T_BitmapHeapScanState, T_CustomScanState, T_ForeignScanState,
    T_IndexOnlyScanState, T_IndexScanState, T_LimitState, T_ResultState, T_SampleScanState,
    T_SeqScanState, T_SubqueryScanState, T_TidRangeScanState, T_TidScanState,
};
use nodes::{CurrentOfExpr, EStateData, ExprContext, PlanStateNode};
use types_tuple::heaptuple::{item_pointer_is_valid, REFCURSOROID};

/// `PORTAL_ONE_SELECT` (`utils/portal.h`, the first `PortalStrategy`
/// enumerator) — the only strategy `execCurrentOf` accepts.
const PORTAL_ONE_SELECT: u32 = 0;

/// Install this crate's implementations into its seam slots.
pub fn init_seams() {
    execCurrent_seams::exec_current_of::set(exec_current_of_seam);
}

/// Seam adapter for [`execCurrent_seams::exec_current_of`].
///
/// The seam contract (consumed by `nodeTidscan`) is the C `execCurrentOf`
/// surface: `(cexpr, econtext, table_oid, &current_tid) -> bool`, modelled here
/// as `EcxtId` + `&mut EStateData` in, `Option<ItemPointerData>` out (the C
/// out-parameter + boolean). This thin shim resolves the `EcxtId` against the
/// EState's ExprContext pool and the per-query memory context, forwards to the
/// real [`exec_current_of`], and maps [`CurrentOfTid`] back to the contract's
/// `Option`: `Found(tid)` is the C `true` (`*current_tid` set), `NotOnThisTable`
/// is the C `false`.
fn exec_current_of_seam<'mcx>(
    cexpr: &CurrentOfExpr,
    econtext: ::nodes::EcxtId,
    table_oid: Oid,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<types_tuple::heaptuple::ItemPointerData>> {
    // mcx is Copy; copy it out before borrowing the ExprContext pool.
    let mcx = estate.es_query_cxt;
    let econtext_ref: &ExprContext = estate.ecxt(econtext);
    match exec_current_of(mcx, cexpr, econtext_ref, table_oid)? {
        CurrentOfTid::Found(tid) => Ok(Some(tid)),
        CurrentOfTid::NotOnThisTable => Ok(None),
    }
}

/// `execCurrentOf`
///
/// Given a `CURRENT OF` expression and the OID of a table, determine which row
/// of the table is currently being scanned by the cursor named by `CURRENT OF`,
/// and return the row's TID.
///
/// Returns [`CurrentOfTid::Found`] if a row was identified, or
/// [`CurrentOfTid::NotOnThisTable`] if the cursor is valid for the table but is
/// not currently scanning a row of it (a legal inheritance case). Returns `Err`
/// for the C `ereport(ERROR)` paths (the cursor is not a valid updatable scan
/// of the specified table).
///
/// Allocates the working cursor/table names in `mcx` (C: `pstrdup`/`palloc`
/// into `CurrentMemoryContext`), so it is fallible (OOM).
pub fn exec_current_of(
    mcx: Mcx<'_>,
    cexpr: &CurrentOfExpr,
    econtext: &ExprContext<'_>,
    table_oid: Oid,
) -> PgResult<CurrentOfTid> {
    // Get the cursor name --- may have to look up a parameter reference.
    let cursor_name: PgString = match &cexpr.cursor_name {
        Some(name) => PgString::from_str_in(name, mcx)?,
        None => fetch_cursor_param_value(mcx, econtext, cexpr.cursor_param)?,
    };
    let cursor_name = cursor_name.as_str();

    // Fetch table name for possible use in error messages.
    let table_name = match lsyscache::get_rel_name::call(mcx, table_oid)? {
        Some(name) => name,
        // elog(ERROR, "cache lookup failed for relation %u", table_oid)
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {table_oid}"
            )));
        }
    };
    let table_name = table_name.as_str();

    // Find the cursor's portal and run the decision logic against its live
    // state, which the portal owner lends for the callback's duration.
    let mut f = |cursor: Option<RunningCursorState>| -> PgResult<CurrentOfTid> {
        exec_current_of_resolve(mcx, cursor, cursor_name, table_name, table_oid)
    };
    portalmem::with_running_cursor::call(cursor_name, &mut f)
}

/// The decision logic of `execCurrentOf` once the working names are resolved and
/// the portal owner has lent the cursor's live state (`cursor` is the C
/// `GetPortalByName` outcome; `None` is `!PortalIsValid`).
fn exec_current_of_resolve(
    mcx: Mcx<'_>,
    cursor: Option<RunningCursorState>,
    cursor_name: &str,
    table_name: &str,
    table_oid: Oid,
) -> PgResult<CurrentOfTid> {
    // if (!PortalIsValid(portal)) ereport(ERROR, ERRCODE_UNDEFINED_CURSOR, ...)
    let portal = match cursor {
        Some(p) => p,
        None => {
            return Err(PgError::error(format!("cursor \"{cursor_name}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_CURSOR));
        }
    };

    // We have to watch out for non-SELECT queries as well as held cursors, both
    // of which may have null queryDesc.
    if portal.strategy != PORTAL_ONE_SELECT {
        return Err(PgError::error(format!("cursor \"{cursor_name}\" is not a SELECT query"))
            .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE));
    }
    // queryDesc == NULL || queryDesc->estate == NULL
    if !portal.has_live_query {
        return Err(PgError::error(format!(
            "cursor \"{cursor_name}\" is held from a previous transaction"
        ))
        .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE));
    }
    // has_live_query == true guarantees queryDesc->estate != NULL.
    let estate = portal
        .estate
        .expect("RunningCursorState::estate is Some when has_live_query");

    // We have two different strategies depending on whether the cursor uses FOR
    // UPDATE/SHARE or not.
    if !estate.es_rowmarks.is_empty() {
        resolve_rowmark_strategy(&portal, estate, cursor_name, table_name, table_oid)
    } else {
        resolve_scan_strategy(mcx, &portal, estate, cursor_name, table_name, table_oid)
    }
}

/// The FOR UPDATE/SHARE strategy: the query must have exactly one FOR
/// UPDATE/SHARE reference to the target table, and we dig the ctid info out of
/// that.
fn resolve_rowmark_strategy(
    portal: &RunningCursorState,
    estate: &EStateData,
    cursor_name: &str,
    table_name: &str,
    table_oid: Oid,
) -> PgResult<CurrentOfTid> {
    // erm = NULL; for (i = 0; i < es_range_table_size; i++) { ... }
    let mut erm = None;
    for i in 0..estate.es_range_table_size {
        let thiserm = match estate.es_rowmarks.get(i).and_then(|e| e.as_ref()) {
            Some(e) => e,
            // ignore non-FOR UPDATE/SHARE items: thiserm == NULL ...
            None => continue,
        };
        // ... || !RowMarkRequiresRowShareLock(thiserm->markType)
        if !::nodes::nodelockrows::RowMarkRequiresRowShareLock(thiserm.markType) {
            continue;
        }
        if thiserm.relid == table_oid {
            if erm.is_some() {
                return Err(PgError::error(format!(
                    "cursor \"{cursor_name}\" has multiple FOR UPDATE/SHARE references to table \"{table_name}\""
                ))
                .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE));
            }
            erm = Some(thiserm);
        }
    }

    let erm = match erm {
        Some(e) => e,
        None => {
            return Err(PgError::error(format!(
                "cursor \"{cursor_name}\" does not have a FOR UPDATE/SHARE reference to table \"{table_name}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE));
        }
    };

    // The cursor must have a current result row: per the SQL spec, it's an error
    // if not.
    if portal.at_start || portal.at_end {
        return Err(not_positioned_error(cursor_name));
    }

    // Return the currently scanned TID, if there is one.
    if item_pointer_is_valid(&erm.curCtid) {
        Ok(CurrentOfTid::Found(erm.curCtid))
    } else {
        // This table didn't produce the cursor's current row; some other
        // inheritance child of the same parent must have. Signal caller to do
        // nothing on this table.
        Ok(CurrentOfTid::NotOnThisTable)
    }
}

/// The plain-scan strategy: dig through the cursor's plan to find the scan node.
/// Fail if it's not there or buried underneath aggregation.
fn resolve_scan_strategy(
    mcx: Mcx<'_>,
    portal: &RunningCursorState,
    estate: &EStateData,
    cursor_name: &str,
    table_name: &str,
    table_oid: Oid,
) -> PgResult<CurrentOfTid> {
    let mut pending_rescan = false;
    let scannode = portal
        .planstate
        .and_then(|root| search_plan_tree(root, table_oid, &mut pending_rescan));
    let scannode = match scannode {
        Some(s) => s,
        None => {
            return Err(PgError::error(format!(
                "cursor \"{cursor_name}\" is not a simply updatable scan of table \"{table_name}\""
            ))
            .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE));
        }
    };
    // search_plan_tree only ever returns a relation-scan node, so as_scan_state
    // is always Some here (the C cast `(ScanState *) node`).
    let scanstate: &ScanStateData = scannode
        .as_scan_state()
        .expect("search_plan_tree returns a relation-scan node");

    // The cursor must have a current result row: per the SQL spec, it's an error
    // if not. We test this at the top level, rather than at the scan node level,
    // because in inheritance cases any one table scan could easily not be on a
    // row. We want to return NotOnThisTable, not raise error, if the passed-in
    // table OID is for one of the inactive scans.
    if portal.at_start || portal.at_end {
        return Err(not_positioned_error(cursor_name));
    }

    // Now OK to return NotOnThisTable if we found an inactive scan. It is
    // inactive either if it's not positioned on a row, or there's a rescan
    // pending for it. TupIsNull(ss_ScanTupleSlot) is the empty/absent slot.
    let scan_slot_is_null = match scanstate.ss_ScanTupleSlot {
        None => true,
        Some(slot_id) => match estate.es_tupleTable.get(slot_id.0 as usize) {
            Some(slot) => slot.base().is_empty(),
            None => true,
        },
    };
    if scan_slot_is_null || pending_rescan {
        return Ok(CurrentOfTid::NotOnThisTable);
    }

    // Extract TID of the scan's current row. The mechanism is scan-type
    // dependent; for IndexOnlyScan the tuple may be virtual without a ctid
    // column, so the TID comes from xs_heaptid, otherwise from the scan tuple's
    // SelfItemPointerAttributeNumber. Both reach the concrete scan-node state,
    // owned by execMain.
    // For an IndexOnlyScan, the tuple in ss_ScanTupleSlot may be a virtual tuple
    // without a ctid column, so the TID comes from the scan descriptor's
    // xs_heaptid; read it here (we hold the concrete scan node) and hand it to
    // the owner. The default path passes None and digs the TID out of the slot.
    let index_only_tid = if scannode.tag() == T_IndexOnlyScanState {
        let ioss = scannode
            .as_index_only_scan_state()
            .expect("T_IndexOnlyScanState node is an IndexOnlyScanState");
        ioss.ioss_ScanDesc.as_ref().map(|sd| sd.xs_heaptid)
    } else {
        None
    };
    match execMain::scan_node_extract_tid::call(
        mcx,
        estate,
        scanstate.ss_ScanTupleSlot,
        index_only_tid,
    )? {
        ScanTidOutcome::Tid(tid) => {
            debug_assert!(item_pointer_is_valid(&tid));
            Ok(CurrentOfTid::Found(tid))
        }
        // A null tableoid / self-ctid is the C "not a simply updatable scan"
        // error, raised here with the table name in scope.
        ScanTidOutcome::NotUpdatable => Err(PgError::error(format!(
            "cursor \"{cursor_name}\" is not a simply updatable scan of table \"{table_name}\""
        ))
        .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE)),
    }
}

/// `ereport(ERROR, ERRCODE_INVALID_CURSOR_STATE, "cursor \"%s\" is not
/// positioned on a row")` — shared by both strategies.
#[inline]
fn not_positioned_error(cursor_name: &str) -> PgError {
    PgError::error(format!("cursor \"{cursor_name}\" is not positioned on a row"))
        .with_sqlstate(ERRCODE_INVALID_CURSOR_STATE)
}

/// `fetch_cursor_param_value`
///
/// Fetch the string value of a param, verifying it is of type `REFCURSOR`.
///
/// The C reads `econtext->ecxt_param_list_info`, optionally calls the dynamic
/// `paramFetch` hook, type-checks `REFCURSOROID`, and decodes the `text` Datum.
/// The numParams/param_id bounds and the `OidIsValid && !isnull` gate are
/// in-crate here; the hook dispatch + `text` decode is the `fetch_cursor_param`
/// seam (execMain), which returns the classified outcome. The result string is
/// allocated in `mcx`.
fn fetch_cursor_param_value<'mcx>(
    mcx: Mcx<'mcx>,
    econtext: &ExprContext<'mcx>,
    param_id: i32,
) -> PgResult<PgString<'mcx>> {
    // paramInfo && paramId > 0 && paramId <= paramInfo->numParams. The seam
    // checks the bounds against the live ParamListInfo and returns `None` when
    // there is no usable param (no param list, out of range, OID-invalid, or
    // NULL) — i.e. the C falls through to "no value found".
    if param_id > 0 {
        if let Some(resolved) = execMain::fetch_cursor_param::call(mcx, econtext, param_id)? {
            match resolved {
                // We know that refcursor uses text's I/O routines.
                FetchedCursorParam::RefCursor(value) => return Ok(value),
                // safety check in case hook did something unexpected
                FetchedCursorParam::WrongType(ptype) => {
                    let actual = format_type::format_type_be::call(mcx, ptype)?;
                    let expected = format_type::format_type_be::call(mcx, REFCURSOROID)?;
                    return Err(PgError::error(format!(
                        "type of parameter {param_id} ({actual}) does not match that when preparing the plan ({expected})"
                    ))
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH));
                }
            }
        }
    }

    Err(PgError::error(format!("no value found for parameter {param_id}"))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT))
}

/// `search_plan_tree`
///
/// Search through a `PlanState` tree for a scan node on the specified table.
/// Returns `None` if not found or multiple candidates.
///
/// CAUTION (from the C): this function is not charged simply with finding some
/// candidate scan, but with ensuring that that scan returned the plan tree's
/// current output row. That's why multiple-match cases are rejected.
///
/// If a candidate is found, set `*pending_rescan` to true if that candidate or
/// any node above it has a pending rescan action (`chgParam != NULL`). The
/// caller must initialize `*pending_rescan` to false and should not trust it if
/// multiple candidates are found.
fn search_plan_tree<'a, 'mcx>(
    node: &'a PlanStateNode<'mcx>,
    table_oid: Oid,
    pending_rescan: &mut bool,
) -> Option<&'a PlanStateNode<'mcx>> {
    let mut result: Option<&'a PlanStateNode<'mcx>> = None;

    match node.tag() {
        // Relation scan nodes can all be treated alike: check whether they are
        // scanning the specified table. ForeignScan and CustomScan might not
        // have a currentRelation, in which case we ignore them (and dare not
        // descend to their children).
        //
        // These node-state variants are added to `PlanStateNode` as their
        // executor units land; until then they cannot occur in a live tree and
        // are handled by the wildcard arm below (the C `default:`).
        T_SeqScanState
        | T_SampleScanState
        | T_IndexScanState
        | T_IndexOnlyScanState
        | T_BitmapHeapScanState
        | T_TidScanState
        | T_TidRangeScanState
        | T_ForeignScanState
        | T_CustomScanState => {
            if let Some(sstate) = node.as_scan_state() {
                if let Some(rel) = sstate.ss_currentRelation.as_ref() {
                    if rel.rd_id == table_oid {
                        result = Some(node);
                    }
                }
            }
        }

        // For Append, check each input node. It is safe to descend because only
        // the input that resulted in the Append's current output node could be
        // positioned on a tuple at all; the others are at EOF or not started.
        // Watch out for multiple matches (possible from UNION ALL). We can NOT
        // descend through MergeAppend similarly (its inputs are likely all
        // active and we don't know which returned the current output tuple).
        T_AppendState => {
            if let Some(children) = node.append_input_states() {
                for child in children {
                    let elem = search_plan_tree(child, table_oid, pending_rescan);
                    let elem = match elem {
                        Some(e) => e,
                        None => continue,
                    };
                    if result.is_some() {
                        return None; // multiple matches
                    }
                    result = Some(elem);
                }
            }
        }

        // Result and Limit can be descended through (these always return their
        // input's current row).
        T_ResultState | T_LimitState => {
            if let Some(outer) = node.outer_plan_state() {
                result = search_plan_tree(outer, table_oid, pending_rescan);
            }
        }

        // SubqueryScan too, but it keeps the child in a different place.
        T_SubqueryScanState => {
            if let Some(subplan) = node.subquery_subplan_state() {
                result = search_plan_tree(subplan, table_oid, pending_rescan);
            }
        }

        // Otherwise, assume we can't descend through it (the C `default:`). The
        // scan/Append/Result/Limit/SubqueryScan node-state variants not yet
        // present in `PlanStateNode` also reach here until their units land.
        _ => {}
    }

    // If we found a candidate at or below this node, then this node's chgParam
    // indicates a pending rescan that will affect the candidate.
    if result.is_some() && node.ps_head().chgParam.is_some() {
        *pending_rescan = true;
    }

    result
}
