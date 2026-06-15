//! Port of `src/backend/executor/nodeTidscan.c` — routines to support direct
//! TID scans of relations.
//!
//! INTERFACE ROUTINES
//! - [`ExecTidScan`]        - scans a relation using tids
//! - [`ExecInitTidScan`]    - creates and initializes state info
//! - [`ExecReScanTidScan`]  - rescans the tid relation
//! - [`ExecEndTidScan`]     - releases all storage
//!
//! The node state machine is held as an owned [`TidScanState`] mutated through
//! `&mut` borrows; the C `PlanState.state` back-pointer is replaced by
//! threading `&mut EStateData` explicitly. Working allocations (`tss_tidexprs`,
//! `tss_TidList`) live in the executor per-query context (`es_query_cxt`), the
//! C `CurrentMemoryContext` while the node runs.
//!
//! `ExecScanExtended` / `ExecScanFetch` are the `execScan.h` `static inline`
//! driver inlined into this translation unit in C; they are reproduced here as
//! private functions (see DESIGN_DEBT.md). Every leaf operation in subsystems
//! not owned by this unit goes through that owner's seam crate and panics until
//! the owner lands.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::Index;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::execnodes::EStateData;
use types_nodes::nodeindexscan::TidScan;
use types_nodes::primnodes::Expr;
use types_nodes::SlotId;
use types_tuple::heaptuple::{ItemPointerData, SelfItemPointerAttributeNumber};

use backend_access_table_tableam as tableam;
use backend_executor_execCurrent_seams as execCurrent;
use backend_executor_execExpr_seams as execExpr;
use backend_executor_execMain_seams as execMain;
use backend_executor_nodeTidscan_seams as execScan;
use backend_executor_execTuples_seams as execTuples;
use backend_executor_execUtils_seams as execUtils;
use backend_tcop_postgres_seams as tcop_postgres;
use backend_utils_adt_arrayfuncs_seams as arrayfuncs;

/// `TIDOID` — the OID of the `tid` type (pg_type.h). Used to deconstruct the
/// `tid[]` array of a `ctid = ANY (array)` qual.
pub const TIDOID: types_core::Oid = 27;

/// This crate reaches outward only through per-owner seam crates, so it
/// declares no seams of its own and installs nothing.
pub fn init_seams() {}

/// Extract the bare machine word from an array `Datum` for a seam ABI edge that
/// still trades in the bare-word `types_datum::Datum`.
///
/// The `deconstruct_tid_array` seam (owned by the out-of-this-wave
/// backend-utils-adt-arrayfuncs unit) takes the `DatumGetArrayTypeP` argument as
/// a bare `types_datum::Datum`. C threads the raw `Datum` machine word straight
/// across this boundary; for a `tid[]` array the word is the varlena pointer.
/// We forward that word from the canonical carrier rather than forge one. A
/// `ByRef` image (a detoasted array materialized into bytes) cannot cross this
/// still-bare-word edge — threading the canonical carrier through is the
/// execTuples canonical-carrier / arrayfuncs-seam migration follow-on (#113).
/// On this path the interpreter never produces a `ByRef` array value
/// (ExecEvalArrayExpr mirror-panics at the `construct_md_array` owner boundary),
/// so a `ByRef` value here would be a contract violation, matching C.
#[inline]
fn array_datum_bare_word(d: &types_tuple::Datum<'_>) -> types_datum::Datum {
    match d {
        types_tuple::Datum::ByVal(w) => types_datum::Datum::from_usize(*w),
        types_tuple::Datum::ByRef(_) => panic!(
            "tid[] array value crossed the bare-word deconstruct_tid_array seam \
             edge as a by-reference image (execTuples canonical-carrier follow-on \
             #113)"
        ),
    }
}

// ===========================================================================
// `TidExpr` / `TidScanState` — node-state and helper vocabulary.
//
// These structs relocated DOWN into `types-nodes` (so the central
// `PlanStateNode` dispatch enum can name `TidScanState` as a variant — the
// slot-vocab F0 "Edge A": `types-nodes` now depends on `types-tableam`). They
// are re-exported here so the executor logic below names them at their historical
// `backend_executor_nodeTidscan::{TidExpr, TidScanState}` paths unchanged.
// ===========================================================================

pub use types_nodes::nodetidscan::{TidExpr, TidScanState};

// ===========================================================================
// `IsCTIDVar` / `is_opclause` / `get_leftop` / `get_rightop` — node tests over
// the real `Expr` enum (no seams: this crate names the planner vocabulary).
// ===========================================================================

/// `#define IsCTIDVar(node)` — true iff `node` is a `Var` for the CTID system
/// column (`varattno == SelfItemPointerAttributeNumber`). Any `Var` in the
/// relation scan qual must be for our table.
fn is_ctid_var(node: Option<&Expr>) -> bool {
    matches!(node, Some(Expr::Var(v)) if v.varattno == SelfItemPointerAttributeNumber)
}

/// `get_leftop(expr)` — left arg of a binary opclause (only arg of a unary).
fn get_leftop(args: &[Expr]) -> Option<&Expr> {
    args.first()
}

/// `get_rightop(expr)` — right arg of a binary opclause (NULL if unary).
fn get_rightop(args: &[Expr]) -> Option<&Expr> {
    if args.len() >= 2 {
        Some(&args[1])
    } else {
        None
    }
}

// ===========================================================================
// `nodeTidscan.c` static helpers (1:1).
// ===========================================================================

/// `itemptr_comparator(a, b)` — qsort comparator for `ItemPointerData` items.
fn itemptr_comparator(ipa: &ItemPointerData, ipb: &ItemPointerData) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    let ba = ipa.ip_blkid.block_number();
    let bb = ipb.ip_blkid.block_number();
    let oa = ipa.ip_posid;
    let ob = ipb.ip_posid;
    if ba < bb {
        Ordering::Less
    } else if ba > bb {
        Ordering::Greater
    } else if oa < ob {
        Ordering::Less
    } else if oa > ob {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// `qunique(array, n, ..., itemptr_comparator)` (lib/qunique.h) — remove
/// adjacent duplicate elements from a sorted slice, returning the new length.
fn qunique(array: &mut [ItemPointerData]) -> usize {
    let elements = array.len();
    if elements <= 1 {
        return elements;
    }
    let mut j = 0usize;
    for i in 1..elements {
        if itemptr_comparator(&array[i], &array[j]) != core::cmp::Ordering::Equal {
            j += 1;
            if j != i {
                array[j] = array[i];
            }
        }
    }
    j + 1
}

/// `bsearch(key, tss_TidList, tss_NumTids, ..., itemptr_comparator)` — true if
/// `key` is present in the sorted TID list.
fn tid_list_contains(tid_list: &[ItemPointerData], key: &ItemPointerData) -> bool {
    tid_list
        .binary_search_by(|probe| itemptr_comparator(probe, key))
        .is_ok()
}

/// `TidExprListCreate(tidstate)` — extract the qual subexpressions that yield
/// TIDs to search for, and compile them into `ExprState`s if they're ordinary
/// expressions. CURRENT OF is dropped into the list as-is.
fn TidExprListCreate<'mcx>(
    tidstate: &mut TidScanState<'mcx>,
    node: &TidScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // tidstate->tss_tidexprs = NIL; tidstate->tss_isCurrentOf = false;
    tidstate.tss_tidexprs.clear();
    tidstate.tss_isCurrentOf = false;

    // foreach(l, node->tidquals)
    let nquals = node.tidquals.as_ref().map_or(0, |q| q.len());
    for i in 0..nquals {
        // Expr *expr = (Expr *) lfirst(l);
        // TidExpr *tidexpr = (TidExpr *) palloc0(sizeof(TidExpr));
        let mut tidexpr = TidExpr::default();

        // Read the qual's shape (it is a fixed, materialized list).
        let quals = node.tidquals.as_ref().unwrap();
        let expr = &quals[i];

        if let Expr::OpExpr(op) = expr {
            // is_opclause(expr)
            let arg1 = get_leftop(&op.args);
            let arg2 = get_rightop(&op.args);
            if is_ctid_var(arg1) {
                // tidexpr->exprstate = ExecInitExpr((Expr *) arg2, &tidstate->ss.ps);
                let arg2 = arg2.ok_or_else(|| elog_internal("could not identify CTID variable"))?;
                tidexpr.exprstate =
                    Some(execExpr::exec_init_expr::call(arg2, &mut tidstate.ss.ps, estate)?);
            } else if is_ctid_var(arg2) {
                // tidexpr->exprstate = ExecInitExpr((Expr *) arg1, &tidstate->ss.ps);
                let arg1 = arg1.ok_or_else(|| elog_internal("could not identify CTID variable"))?;
                tidexpr.exprstate =
                    Some(execExpr::exec_init_expr::call(arg1, &mut tidstate.ss.ps, estate)?);
            } else {
                return Err(elog_internal("could not identify CTID variable"));
            }
            tidexpr.isarray = false;
        } else if let Expr::ScalarArrayOpExpr(saex) = expr {
            // Assert(IsCTIDVar(linitial(saex->args)));
            debug_assert!(is_ctid_var(saex.args.first()));
            // tidexpr->exprstate = ExecInitExpr(lsecond(saex->args), &tidstate->ss.ps);
            let array_arg = saex
                .args
                .get(1)
                .ok_or_else(|| elog_internal("could not identify CTID expression"))?;
            tidexpr.exprstate =
                Some(execExpr::exec_init_expr::call(array_arg, &mut tidstate.ss.ps, estate)?);
            tidexpr.isarray = true;
        } else if let Expr::CurrentOfExpr(cexpr) = expr {
            // tidexpr->cexpr = cexpr; tidstate->tss_isCurrentOf = true;
            tidexpr.cexpr = Some(cexpr.clone());
            tidstate.tss_isCurrentOf = true;
        } else {
            return Err(elog_internal("could not identify CTID expression"));
        }

        // tidstate->tss_tidexprs = lappend(tidstate->tss_tidexprs, tidexpr);
        let mcx = *tidstate.tss_tidexprs.allocator();
        tidstate
            .tss_tidexprs
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<TidExpr>()))?;
        tidstate.tss_tidexprs.push(tidexpr);
    }

    // CurrentOfExpr could never appear OR'd with something else.
    debug_assert!(tidstate.tss_tidexprs.len() == 1 || !tidstate.tss_isCurrentOf);
    Ok(())
}

/// `TidListEval(tidstate)` — compute the list of TIDs to be visited, by
/// evaluating the expressions for them. (The result is an array, not a list.)
fn TidListEval<'mcx>(tidstate: &mut TidScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<()> {
    // ExprContext *econtext = tidstate->ss.ps.ps_ExprContext;
    let econtext = tidstate
        .ss
        .ps
        .ps_ExprContext
        .expect("TidListEval: node has no ExprContext");

    // Start scan on-demand - initializing a scan isn't free, so delay it until
    // needed (the node might never get executed).
    if tidstate.ss_currentScanDesc.is_none() {
        let rel = tidstate
            .ss
            .ss_currentRelation
            .as_ref()
            .expect("TidListEval: scan relation not opened");
        tidstate.ss_currentScanDesc = Some(tableam::table_beginscan_tid(
            rel,
            estate.es_snapshot.as_ref().map(|s| (**s).clone()),
        )?);
    }

    // We initialize the array with enough slots for the case that all quals are
    // simple OpExprs or CurrentOfExprs. If there are ScalarArrayOpExprs, we may
    // have to enlarge the array.
    let mcx = estate.es_query_cxt;
    let num_alloc_tids = tidstate.tss_tidexprs.len();
    let mut tid_list: PgVec<ItemPointerData> = vec_with_capacity_in(mcx, num_alloc_tids)?;

    // foreach(l, tidstate->tss_tidexprs)
    let nexprs = tidstate.tss_tidexprs.len();
    for i in 0..nexprs {
        let econtext_id = econtext;
        let isarray = tidstate.tss_tidexprs[i].isarray;
        let has_exprstate = tidstate.tss_tidexprs[i].exprstate.is_some();

        if has_exprstate && !isarray {
            // itemptr = (ItemPointer) DatumGetPointer(
            //     ExecEvalExprSwitchContext(tidexpr->exprstate, econtext, &isNull));
            let (itemptr, is_null) = {
                let exprstate = tidstate.tss_tidexprs[i].exprstate.as_deref_mut().unwrap();
                execExpr::exec_eval_tid_expr_switch_context::call(exprstate, econtext_id, estate)?
            };
            if is_null {
                continue;
            }

            // Silently discard any TIDs the AM considers invalid.
            let scan = tidstate.ss_currentScanDesc.as_deref_mut().unwrap();
            if !tableam::table_tuple_tid_valid(scan, &itemptr)? {
                continue;
            }

            // if (numTids >= numAllocTids) { numAllocTids *= 2; repalloc(...); }
            // tidList[numTids++] = *itemptr;
            push_tid(&mut tid_list, mcx, itemptr)?;
        } else if has_exprstate && isarray {
            // arraydatum = ExecEvalExprSwitchContext(...); if (isNull) continue;
            // itemarray = DatumGetArrayTypeP(arraydatum);
            // deconstruct_array_builtin(itemarray, TIDOID, &ipdatums, &ipnulls, &ndatums);
            let (arraydatum, is_null) = {
                let exprstate = tidstate.tss_tidexprs[i].exprstate.as_deref_mut().unwrap();
                execExpr::exec_eval_array_expr_switch_context::call(exprstate, econtext_id, estate)?
            };
            if is_null {
                continue;
            }
            // itemarray = DatumGetArrayTypeP(arraydatum);
            // deconstruct_array_builtin(itemarray, TIDOID, ...);
            //
            // The `deconstruct_tid_array` seam is owned by the (out-of-this-wave)
            // backend-utils-adt-arrayfuncs unit and still trades in the bare-word
            // `types_datum::Datum` (the `DatumGetArrayTypeP` argument). C threads
            // the raw `Datum` machine word straight across this boundary; for a
            // `tid[]` array the word is the varlena pointer. We extract that word
            // from the canonical carrier here rather than forge one. A `ByRef`
            // image cannot cross this still-bare-word edge — materializing the
            // array into ByRef bytes is the execTuples canonical-carrier /
            // arrayfuncs-seam migration follow-on (#113); on this path the
            // interpreter never produces one (ExecEvalArrayExpr mirror-panics at
            // the construct_md_array owner boundary), so a ByRef value here would
            // be a contract violation, matching C.
            let arraydatum = array_datum_bare_word(&arraydatum);
            let items = arrayfuncs::deconstruct_tid_array::call(mcx, arraydatum)?;

            // for (i = 0; i < ndatums; i++)
            for (itemptr, isnull) in items.iter().copied() {
                // if (ipnulls[i]) continue;
                if isnull {
                    continue;
                }
                // itemptr = (ItemPointer) DatumGetPointer(ipdatums[i]);
                // if (!table_tuple_tid_valid(scan, itemptr)) continue;
                let scan = tidstate.ss_currentScanDesc.as_deref_mut().unwrap();
                if !tableam::table_tuple_tid_valid(scan, &itemptr)? {
                    continue;
                }
                // tidList[numTids++] = *itemptr;
                push_tid(&mut tid_list, mcx, itemptr)?;
            }
        } else {
            // Assert(tidexpr->cexpr);
            let cexpr = tidstate.tss_tidexprs[i]
                .cexpr
                .clone()
                .ok_or_else(|| elog_internal("TID CURRENT OF expression has no cexpr"))?;
            // if (execCurrentOf(tidexpr->cexpr, econtext,
            //         RelationGetRelid(tidstate->ss.ss_currentRelation), &cursor_tid))
            //     tidList[numTids++] = cursor_tid;
            let relid = tidstate
                .ss
                .ss_currentRelation
                .as_ref()
                .expect("TidListEval: scan relation not opened")
                .rd_id;
            if let Some(cursor_tid) =
                execCurrent::exec_current_of::call(&cexpr, econtext_id, relid, estate)?
            {
                push_tid(&mut tid_list, mcx, cursor_tid)?;
            }
        }
    }

    // Sort the array of TIDs into order, and eliminate duplicates.
    let mut num_tids = tid_list.len();
    if num_tids > 1 {
        // CurrentOfExpr could never appear OR'd with something else.
        debug_assert!(!tidstate.tss_isCurrentOf);
        tid_list.as_mut_slice().sort_by(itemptr_comparator);
        num_tids = qunique(tid_list.as_mut_slice());
        tid_list.truncate(num_tids);
    }

    // tidstate->tss_TidList = tidList; tss_NumTids = numTids; tss_TidPtr = -1;
    tidstate.tss_NumTids = num_tids as i32;
    tidstate.tss_TidList = Some(tid_list);
    tidstate.tss_TidPtr = -1;
    Ok(())
}

/// Append one TID, growing the array fallibly (the C `palloc`/`repalloc`).
fn push_tid(
    tid_list: &mut PgVec<ItemPointerData>,
    mcx: Mcx,
    itemptr: ItemPointerData,
) -> PgResult<()> {
    tid_list
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<ItemPointerData>()))?;
    tid_list.push(itemptr);
    Ok(())
}

/// `TidNext(node)` — retrieve a tuple from the TidScan node's `currentRelation`
/// using the TIDs in the `TidScanState` information.
///
/// On success the visible tuple lives in `node.ss.ss_ScanTupleSlot`; returns
/// `Ok(Some(slot))` if a tuple is available, `Ok(None)` when exhausted (the C
/// `return ExecClearTuple(slot)` clears the slot and the caller sees NULL).
fn TidNext<'mcx>(node: &mut TidScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<Option<SlotId>> {
    // estate = node->ss.ps.state; direction = estate->es_direction;
    // snapshot = estate->es_snapshot; heapRelation = node->ss.ss_currentRelation;
    // slot = node->ss.ss_ScanTupleSlot;
    let direction = estate.es_direction;
    let slot = node.ss.ss_ScanTupleSlot;

    // First time through, compute the list of TIDs to be visited.
    if node.tss_TidList.is_none() {
        TidListEval(node, estate)?;
    }

    let num_tids = node.tss_NumTids;

    // Initialize or advance scan position, depending on direction.
    let b_backward = types_scan::sdir::ScanDirectionIsBackward(direction);
    if b_backward {
        if node.tss_TidPtr < 0 {
            // initialize for backward scan
            node.tss_TidPtr = num_tids - 1;
        } else {
            node.tss_TidPtr -= 1;
        }
    } else if node.tss_TidPtr < 0 {
        // initialize for forward scan
        node.tss_TidPtr = 0;
    } else {
        node.tss_TidPtr += 1;
    }

    while node.tss_TidPtr >= 0 && node.tss_TidPtr < num_tids {
        // ItemPointerData tid = tidList[node->tss_TidPtr];
        let mut tid = node.tss_TidList.as_ref().unwrap()[node.tss_TidPtr as usize];

        // For WHERE CURRENT OF, the cursor's tuple might since have been
        // updated; fetch the version current according to our snapshot.
        if node.tss_isCurrentOf {
            let scan = node.ss_currentScanDesc.as_deref_mut().unwrap();
            tableam::table_tuple_get_latest_tid(scan, &mut tid)?;
        }

        // if (table_tuple_fetch_row_version(heapRelation, &tid, snapshot, slot))
        //     return slot;
        let found = {
            let rel = node.ss.ss_currentRelation.as_ref().unwrap();
            let snapshot: types_tableam::tableam::Snapshot =
                estate.es_snapshot.as_ref().map(|s| (**s).clone());
            let slot_id = slot.expect("TidNext: node has no scan tuple slot");
            tableam::table_tuple_fetch_row_version(rel, &tid, &snapshot, estate.slot_mut(slot_id))?
        };
        if found {
            return Ok(slot);
        }

        // Bad TID or failed snapshot qual; try next.
        if b_backward {
            node.tss_TidPtr -= 1;
        } else {
            node.tss_TidPtr += 1;
        }

        // CHECK_FOR_INTERRUPTS();
        tcop_postgres::check_for_interrupts::call()?;
    }

    // The TID scan failed, so we are at the end of the scan: clear the slot.
    if let Some(slot_id) = slot {
        execTuples::exec_clear_tuple::call(estate, slot_id)?;
    }
    Ok(None)
}

/// `TidRecheck(node, slot)` — access-method routine to recheck a tuple in
/// `EvalPlanQual`. The `slot` is the operative tuple the EPQ driver wants to
/// recheck: on the EvalPlanQual replacement path that is
/// `epqstate->relsubs_slot[scanrelid - 1]` (a *different* slot from the node's
/// own scan slot), so C reads `slot->tts_tid` from the passed slot — not from
/// `node->ss_ScanTupleSlot`.
fn TidRecheck<'mcx>(
    node: &mut TidScanState<'mcx>,
    slot: Option<SlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // WHERE CURRENT OF always intends to resolve to the latest tuple.
    if node.tss_isCurrentOf {
        return Ok(true);
    }

    if node.tss_TidList.is_none() {
        TidListEval(node, estate)?;
    }

    // Binary search the TidList to see if this ctid is mentioned.
    // match = bsearch(&slot->tts_tid, node->tss_TidList, node->tss_NumTids, ...);
    let key = match slot {
        Some(id) => estate.slot(id).tts_tid,
        None => return Ok(false),
    };
    let num_tids = node.tss_NumTids.max(0) as usize;
    let found = match node.tss_TidList.as_ref() {
        Some(list) => tid_list_contains(&list[..num_tids.min(list.len())], &key),
        None => false,
    };
    Ok(found)
}

// ===========================================================================
// `execScan.h` driver (`ExecScan` -> `ExecScanExtended` -> `ExecScanFetch`),
// inlined into this translation unit in C; reproduced here (DESIGN_DEBT.md).
// ===========================================================================

/// `ExecScanFetch` (execScan.h) — check interrupts and fetch the next potential
/// tuple, substituting a test tuple if inside an EvalPlanQual recheck.
///
/// Reproduced inline here exactly as `execScan.h` inlines it into the TidScan
/// translation unit. The EvalPlanQual replacement-slot branch must recheck
/// against the EPQ-supplied `epqstate->relsubs_slot[scanrelid - 1]` — a slot
/// distinct from the node's own `ss_ScanTupleSlot` — and return *that* slot, so
/// `TidRecheck` is passed the operative slot rather than always reading the base
/// scan tuple.
fn ExecScanFetch<'mcx>(node: &mut TidScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<Option<SlotId>> {
    // CHECK_FOR_INTERRUPTS();
    tcop_postgres::check_for_interrupts::call()?;

    if estate.es_epq_active.is_some() {
        // We are inside an EvalPlanQual recheck.
        // Index scanrelid = ((Scan *) node->ps.plan)->scanrelid;
        let scanrelid = node_scanrelid(node);

        if scanrelid == 0 {
            // ForeignScan/CustomScan that pushed a join to the remote side. A
            // TidScan plan node always carries a positive base-relation
            // scanrelid, so this branch is unreachable for this node; the
            // bms_is_member(epqParam, plan->extParam) test and recheck-into-
            // ss_ScanTupleSlot it would gate are therefore not modeled here.
            // (Fall through to the access method below, as the C does when the
            // node is not a descendant of the EPQ recheck plan tree.)
        } else if epq_relsubs_done(estate, scanrelid - 1) {
            // Return empty slot, as either there is no EPQ tuple for this rel or
            // we already returned it.
            //   TupleTableSlot *slot = node->ss_ScanTupleSlot;
            //   return ExecClearTuple(slot);
            let slot = node.ss.ss_ScanTupleSlot;
            if let Some(id) = slot {
                execTuples::exec_clear_tuple::call(estate, id)?;
            }
            return Ok(slot);
        } else if let Some(epq_slot) = epq_relsubs_slot(estate, scanrelid - 1) {
            // Return replacement tuple provided by the EPQ caller.
            //   TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];
            //   Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);
            debug_assert!(!epq_relsubs_rowmark_present(estate, scanrelid - 1));

            // Mark to remember that we shouldn't return it again.
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);

            // Return empty slot if we haven't got a test tuple.
            //   if (TupIsNull(slot)) return NULL;
            if estate.slot(epq_slot).is_empty() {
                return Ok(None);
            }

            // Check if it meets the access-method conditions.
            //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
            // The recheck reads the PASSED replacement slot's tts_tid, not the
            // node's own scan slot.
            if !TidRecheck(node, Some(epq_slot), estate)? {
                execTuples::exec_clear_tuple::call(estate, epq_slot)?;
                return Ok(None);
            }
            //   return slot;
            return Ok(Some(epq_slot));
        } else if epq_relsubs_rowmark_present(estate, scanrelid - 1) {
            // Fetch and return replacement tuple using a non-locking rowmark.
            //   TupleTableSlot *slot = node->ss_ScanTupleSlot;
            let slot = node.ss.ss_ScanTupleSlot;

            // Mark to remember that we shouldn't return more.
            //   epqstate->relsubs_done[scanrelid - 1] = true;
            epq_set_relsubs_done(estate, scanrelid - 1, true);

            //   if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot)) return NULL;
            match slot {
                Some(id) => {
                    if !execMain::eval_plan_qual_fetch_row_mark::call(estate, scanrelid, id)? {
                        return Ok(None);
                    }
                    // Return empty slot if we haven't got a test tuple.
                    //   if (TupIsNull(slot)) return NULL;
                    if estate.slot(id).is_empty() {
                        return Ok(None);
                    }
                    // Check if it meets the access-method conditions.
                    //   if (!(*recheckMtd)(node, slot)) return ExecClearTuple(slot);
                    if !TidRecheck(node, slot, estate)? {
                        execTuples::exec_clear_tuple::call(estate, id)?;
                        return Ok(None);
                    }
                    //   return slot;
                    return Ok(slot);
                }
                None => return Ok(None),
            }
        }
    }

    // Run the node-type-specific access method function to get the next tuple.
    TidNext(node, estate)
}

/// `epqstate->relsubs_done[idx]` — read the active `EPQState`'s "already
/// returned / none" flag. `false` for a `None` array (the C `NULL`).
#[inline]
fn epq_relsubs_done(estate: &EStateData<'_>, idx: Index) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_done.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `epqstate->relsubs_done[idx] = value`.
#[inline]
fn epq_set_relsubs_done(estate: &mut EStateData<'_>, idx: Index, value: bool) {
    if let Some(e) = estate.es_epq_active.as_deref_mut() {
        if let Some(v) = e.relsubs_done.as_mut() {
            if let Some(slot) = v.get_mut(idx as usize) {
                *slot = value;
            }
        }
    }
}

/// `epqstate->relsubs_slot[idx]` (`Some` = a non-NULL C entry).
#[inline]
fn epq_relsubs_slot(estate: &EStateData<'_>, idx: Index) -> Option<SlotId> {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_slot.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .flatten()
}

/// `epqstate->relsubs_rowmark[idx] != NULL`.
#[inline]
fn epq_relsubs_rowmark_present(estate: &EStateData<'_>, idx: Index) -> bool {
    estate
        .es_epq_active
        .as_deref()
        .and_then(|e| e.relsubs_rowmark.as_ref())
        .and_then(|v| v.get(idx as usize).copied())
        .unwrap_or(false)
}

/// `ExecScanExtended` (execScan.h) — scan using the access method, optionally
/// checking the tuple against `qual` and applying `projInfo`.
fn ExecScanExtended<'mcx>(node: &mut TidScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<Option<SlotId>> {
    // qual = node->ps.qual; projInfo = node->ps.ps_ProjInfo; (read each cycle)
    let has_qual = node.ss.ps.qual.is_some();
    let has_proj_info = node.ss.ps.ps_ProjInfo.is_some();
    let econtext = node
        .ss
        .ps
        .ps_ExprContext
        .expect("ExecScanExtended: node has no ExprContext");

    // If we have neither a qual to check nor a projection to do, just skip all
    // the overhead and return the raw scan tuple.
    if !has_qual && !has_proj_info {
        // ResetExprContext(econtext);
        execUtils::reset_expr_context::call(estate, econtext)?;
        return ExecScanFetch(node, estate);
    }

    // Reset per-tuple memory context to free expression-evaluation storage
    // allocated in the previous tuple cycle.
    execUtils::reset_expr_context::call(estate, econtext)?;

    // Get a tuple from the access method. Loop until we obtain a tuple that
    // passes the qualification.
    loop {
        let slot = ExecScanFetch(node, estate)?;

        // If the slot returned by the accessMtd contains NULL, there is nothing
        // more to scan; return an empty slot using the projection result slot.
        if slot.is_none() {
            if has_proj_info {
                // return ExecClearTuple(projInfo->pi_state.resultslot);
                if let Some(result_slot) = node.ss.ps.ps_ResultTupleSlot {
                    execTuples::exec_clear_tuple::call(estate, result_slot)?;
                    return Ok(Some(result_slot));
                }
            }
            return Ok(None);
        }

        // econtext->ecxt_scantuple = slot;
        // `slot` is whatever ExecScanFetch returned — the node's scan slot on
        // the normal path, or the EPQ replacement slot
        // (`relsubs_slot[scanrelid - 1]`) on the EvalPlanQual path.
        set_econtext_scantuple(node, estate, slot);

        // Check that the current tuple satisfies the qual-clause.
        let passes = if !has_qual {
            true
        } else {
            let qual = node.ss.ps.qual.as_deref_mut().unwrap();
            execExpr::exec_qual::call(qual, econtext, estate)?
        };
        if passes {
            // Found a satisfactory scan tuple.
            if has_proj_info {
                // Form a projection tuple, store it in the result slot, return.
                return Ok(Some(execExpr::exec_project::call(
                    &mut node.ss.ps,
                    estate,
                )?));
            } else {
                // Not projecting, so just return the scan tuple.
                return Ok(node.ss.ss_ScanTupleSlot);
            }
        }

        // Tuple fails qual, so free per-tuple memory and try again.
        execUtils::reset_expr_context::call(estate, econtext)?;
    }
}

/// `econtext->ecxt_scantuple = slot` — point the per-tuple expression context
/// at the slot `ExecScanFetch` just returned. On the normal path that is the
/// node's `ss_ScanTupleSlot`; on the EvalPlanQual replacement path it is the
/// EPQ-supplied `relsubs_slot[scanrelid - 1]`, a different slot, so we must use
/// the returned slot rather than unconditionally re-deriving `ss_ScanTupleSlot`.
fn set_econtext_scantuple<'mcx>(
    node: &mut TidScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: Option<SlotId>,
) {
    if let (Some(ecxt), Some(slot)) = (node.ss.ps.ps_ExprContext, slot) {
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(slot);
    }
}

/// `((Scan *) node->ps.plan)->scanrelid`. The trimmed `PlanStateData` does not
/// retain the `plan` back-link, so `ExecInitTidScan` captured the plan's
/// `scanrelid` onto the node-state; return it here. For a `TidScan` this is
/// always the positive base-relation RTE index, never the `0`
/// ForeignScan/CustomScan pushed-down-join sentinel.
fn node_scanrelid(node: &TidScanState) -> Index {
    node.scanrelid
}

// ===========================================================================
// Public node entry points (1:1).
// ===========================================================================

/// `ExecTidScan(pstate)` — scan the relation using TIDs and return the next
/// qualifying tuple (in the node's scan/result slot), or `None` when exhausted.
/// Equivalent to `ExecScan(&node->ss, TidNext, TidRecheck)`.
pub fn ExecTidScan<'mcx>(node: &mut TidScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<Option<SlotId>> {
    ExecScanExtended(node, estate)
}

/// `ExecReScanTidScan(node)` — rescan the TID relation.
pub fn ExecReScanTidScan<'mcx>(node: &mut TidScanState<'mcx>, estate: &mut EStateData<'mcx>) -> PgResult<()> {
    // if (node->tss_TidList) pfree(node->tss_TidList);
    // node->tss_TidList = NULL; tss_NumTids = 0; tss_TidPtr = -1;
    node.tss_TidList = None;
    node.tss_NumTids = 0;
    node.tss_TidPtr = -1;

    // not really necessary, but seems good form
    // if (node->ss.ss_currentScanDesc) table_rescan(node->ss.ss_currentScanDesc, NULL);
    if let Some(scan) = node.ss_currentScanDesc.as_deref_mut() {
        tableam::table_rescan(scan, None)?;
    }

    // ExecScanReScan(&node->ss);
    execScan::exec_scan_rescan::call(&mut node.ss, estate)
}

/// `ExecEndTidScan(node)` — release any storage allocated through C routines.
pub fn ExecEndTidScan(node: &mut TidScanState) -> PgResult<()> {
    // if (node->ss.ss_currentScanDesc) table_endscan(node->ss.ss_currentScanDesc);
    if let Some(scan) = node.ss_currentScanDesc.take() {
        tableam::table_endscan(scan)?;
    }
    Ok(())
}

/// `ExecInitTidScan(node, estate, eflags)` — initialize the TID scan's state
/// information, create scan keys, and open the base relation.
pub fn ExecInitTidScan<'mcx>(
    node: &TidScan<'mcx>,
    eflags: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, TidScanState<'mcx>>> {
    // create state structure (makeNode(TidScanState))
    let mcx = estate.es_query_cxt;
    let mut tidstate = mcx::alloc_in(mcx, TidScanState::new_in(mcx))?;

    // tidstate->ss.ps.plan = (Plan *) node;  (the executor wires the plan
    // back-link / ExecProcNode dispatch slot when it installs this node; the
    // owned `PlanStateData.plan` borrow is set by the executor's node factory.)
    // Because the trimmed PlanStateData does not retain that back-link, capture
    // the plan's `scanrelid` onto the node-state now so the EvalPlanQual path
    // (`ExecScanFetch` -> `node_scanrelid`) can recover it. C reads it via
    // `((Scan *) node->ps.plan)->scanrelid`.
    tidstate.scanrelid = node.scan.scanrelid;

    // Miscellaneous initialization: create expression context for node.
    // ExecAssignExprContext(estate, &tidstate->ss.ps);
    execUtils::exec_assign_expr_context::call(estate, &mut tidstate.ss.ps)?;

    // mark tid list as not computed yet
    tidstate.tss_TidList = None;
    tidstate.tss_NumTids = 0;
    tidstate.tss_TidPtr = -1;

    // open the scan relation
    // currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);
    // tidstate->ss.ss_currentRelation = currentRelation;
    let current_relation =
        execUtils::exec_open_scan_relation::call(estate, node.scan.scanrelid, eflags)?;
    tidstate.ss.ss_currentRelation = Some(current_relation);
    // tidstate->ss.ss_currentScanDesc = NULL;  /* no heap scan here */
    tidstate.ss_currentScanDesc = None;

    // get the scan type from the relation descriptor.
    // ExecInitScanTupleSlot(estate, &tidstate->ss, RelationGetDescr(currentRelation),
    //     table_slot_callbacks(currentRelation));
    let (tupdesc, tts_ops) = {
        let rel = tidstate
            .ss
            .ss_currentRelation
            .as_ref()
            .expect("ExecInitTidScan: scan relation not opened");
        let tts_ops = tableam::table_slot_callbacks(rel);
        let tupdesc = Some(mcx::alloc_in(mcx, rel.rd_att.clone_in(mcx)?)?);
        (tupdesc, tts_ops)
    };
    execTuples::exec_init_scan_tuple_slot::call(estate, &mut tidstate.ss, tupdesc, tts_ops)?;

    // Initialize result type and projection.
    // ExecInitResultTypeTL(&tidstate->ss.ps);
    execTuples::exec_init_result_type_tl::call(&mut tidstate.ss.ps, estate)?;
    // ExecAssignScanProjectionInfo(&tidstate->ss);
    let scanrelid = node.scan.scanrelid;
    execScan::exec_assign_scan_projection_info::call(&mut tidstate.ss, estate, scanrelid)?;

    // initialize child expressions
    // tidstate->ss.ps.qual = ExecInitQual(node->scan.plan.qual, (PlanState *) tidstate);
    let qual = node.scan.plan.qual.as_deref();
    tidstate.ss.ps.qual = execExpr::exec_init_qual::call(qual, &mut tidstate.ss.ps, estate)?;

    // TidExprListCreate(tidstate);
    TidExprListCreate(&mut tidstate, node, estate)?;

    // all done.
    Ok(tidstate)
}

// ===========================================================================
// Error helpers.
// ===========================================================================

/// `elog(ERROR, msg)` — internal error (`ERRCODE_INTERNAL_ERROR`).
fn elog_internal(message: &'static str) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

#[cfg(test)]
mod tests;
