//! `backend-executor-execMain` (`executor/execMain.c`) — the executor driver.
//!
//! ## F0d driver scope (#166 / EState-Plan keystone #167)
//!
//! This crate is the executor driver: `CreateQueryDesc` builds the owned
//! [`QueryDesc`] bundle, `standard_ExecutorStart` (→ `InitPlan`) builds the
//! plan-state tree and the result tupdesc, `standard_ExecutorRun` (→
//! `ExecutePlan`) drives the plan tuple-at-a-time into the `DestReceiver`, and
//! `standard_ExecutorEnd` (→ `ExecEndPlan`) tears it down. The whole loop is
//! wired end-to-end for a plain `DestNone`/`DestRemote` `SELECT`.
//!
//! ### B1 leak-projection (HONEST `&'mcx Node`, NOT a transmute)
//!
//! `InitPlan` must hand `ExecInitNode` an honest `&'mcx Node` for
//! `plannedstmt->planTree` (the seam is signed `node: Option<&'mcx Node>`, and
//! re-signing it would ripple the ~25 node `ExecInit*` crates — rejected). The
//! plan tree lives inside the [`QueryDesc`]'s `McxOwned` bundle, so
//! [`QueryDesc::with_plan_and_estate_mut`] leaks it via the bundle's own `'mcx`
//! allocator ([`mcx::leak_in`]): the allocator lifetime *is* `'mcx`, and the
//! per-query context drop reclaims it — faithful to C's "plan freed with its
//! context". This is a real borrow, not a transmute.
//!
//! ### Guard-and-panic frontier
//!
//! `InitPlan` now runs the full `ExecCheckPermissions` / `ExecCheckOneRelPerms`
//! per-rel classification (`RTEPermissionInfo` carries the
//! `requiredPerms`/`selectedCols`/`insertedCols`/`updatedCols` fields), so a real
//! table query no longer panics on the permission leg (the ACL checks seam into
//! the still-unported `catalog/aclchk.c` owner; with the planner that populates
//! `RTEPermissionInfo` also unported, a plain SELECT's `permInfos` is empty and
//! the check is a no-op).
//!
//! `InitPlan` now allocates `es_param_exec_vals` from `plannedstmt->paramExecTypes`
//! and runs the per-`SubPlan` `ExecInitNode` loop over `plannedstmt->subplans`,
//! populating `es_subplanstates` (the executor-owned per-init-plan state list);
//! `ExecEndPlan` tears those subplan states down in turn. The top-level subplan
//! list build is faithful; the *consumer* of `es_subplanstates` —
//! `ExecInitSubPlan` reached through a node's `Plan.initPlan` list — is still
//! gated behind the unported `Plan.initPlan` field (a `execProcnode` panic) plus
//! the `SubPlanState.planstate` two-owner re-model, so the two
//! `SubPlanState`-resolving PARAM_EXEC seams (`link_subplan_planstate`,
//! `exec_set_param_plan_for_pending`) stay seam-and-panic until that keystone
//! lands (see `DESIGN_DEBT.md`).
//!
//! The remaining branches a plain `DestNone` `SELECT` does not exercise —
//! `EXPLAIN`-only read-only/parallel gating, `parallelModeNeeded`, `rowMarks`,
//! partition pruning, RETURNING, and non-`SELECT` command types — `panic!` with a
//! precise message (mirror-pg-and-panic on a live frontier); the unit stays
//! CATALOG `needs-decomp` so the seam-install recurrence guard exempts the
//! still-open surface.

#![no_std]
#![allow(non_snake_case)]
// `PgError` is a large owned struct; the un-boxed `Err` is the project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::string::ToString;

use mcx::MemoryContext;
use types_acl::acl::{
    AclMaskHow, AclMode, AclResult, ACL_INSERT, ACL_SELECT, ACL_UPDATE,
};
use types_nodes::bitmapset::{Bitmapset, BITS_PER_BITMAPWORD};
use types_nodes::parsenodes::RTEPermissionInfo;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::CmdType;
use types_nodes::params::ParamListInfo;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::querydesc::QueryDesc;
use types_scan::sdir::{ScanDirection, ScanDirectionIsNoMovement};

use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_objectaddress_seams as objaddr;
use backend_nodes_core_seams as bms_seams;
use backend_utils_cache_relcache_seams as relcache;
use backend_executor_execJunk as execJunk;
use backend_executor_execMain_seams as seams;
use backend_executor_execReplication_seams as execReplication_seams;
use backend_executor_execProcnode_seams as procnode;
use backend_executor_execUtils as execUtils;
use backend_executor_execUtils_seams as execUtils_seams;
use backend_tcop_dest_seams as dest;
use backend_access_transam_xact_seams as xact_seams;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_catalog_namespace_seams as namespace_seam;
use backend_utils_init_miscinit_seams as miscinit;

// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) — the column-bitmap
// offset the planner applies to selectedCols bit numbers.
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

// EState eflags (executor/executor.h). Mirrored locally; there is no canonical
// shared constant yet (other node crates likewise define them locally).
const EXEC_FLAG_EXPLAIN_ONLY: i32 = 0x0001;
const EXEC_FLAG_REWIND: i32 = 0x0004;
const EXEC_FLAG_BACKWARD: i32 = 0x0008;
const EXEC_FLAG_MARK: i32 = 0x0010;
const EXEC_FLAG_SKIP_TRIGGERS: i32 = 0x0020;

/// `CreateQueryDesc(plannedstmt, sourceText, snapshot, crosscheck_snapshot,
/// dest, params, queryEnv, instrument_options)` (execMain.c).
///
/// Allocates the per-query "ExecutorState" context, builds the `EState` in it
/// (`CreateExecutorState`), and copies the read-only inputs. `ExecutorStart`
/// fills the plan-state tree and result tupdesc.
#[allow(clippy::too_many_arguments)]
pub fn CreateQueryDesc(
    parent: &MemoryContext,
    plannedstmt: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    crosscheck_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    dest: DestReceiverHandle,
    params: ParamListInfo,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    QueryDesc::create(
        parent,
        plannedstmt,
        source_text,
        snapshot,
        crosscheck_snapshot,
        dest,
        params,
        instrument_options,
    )
}

// ===========================================================================
// ExecutorStart / InitPlan
// ===========================================================================

/// `standard_ExecutorStart(queryDesc, eflags)` (execMain.c).
///
/// Fills the `EState` fields the C prologue sets from `queryDesc`, runs the
/// read-only/parallel gate (when applicable), and calls [`InitPlan`] to build
/// the plan-state tree, the result tupdesc and the top junk filter.
pub fn standard_ExecutorStart(query_desc: &mut QueryDesc, mut eflags: i32) -> PgResult<()> {
    // sanity checks: queryDesc must not be started already (planstate == NULL).
    // (The active-snapshot Assert is the caller's contract.)

    // If the transaction is read-only / we're in parallel mode (and not
    // EXPLAIN-only), check for writes to non-temp tables.
    //   if ((XactReadOnly || IsInParallelMode()) && !EXPLAIN_ONLY)
    //       ExecCheckXactReadOnly(queryDesc->plannedstmt);
    if (xact_seams::xact_read_only::call() || xact_seams::is_in_parallel_mode::call())
        && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0
    {
        query_desc
            .work
            .with(|w| ExecCheckXactReadOnly(&w.plannedstmt))?;
    }

    let operation = query_desc.operation;
    let params = query_desc.params.clone();
    let instrument = query_desc.instrument_options;

    // estate->es_param_list_info = queryDesc->params;
    // (paramExecTypes != NIL ⇒ allocate es_param_exec_vals — guarded below.)
    // estate->es_sourceText = queryDesc->sourceText;  (bundle-owned copy.)
    // estate->es_queryEnv = queryDesc->queryEnv;
    //
    // Non-read-only command-id assignment + AFTER-trigger setup:
    //   CMD_SELECT: if (rowMarks || hasModifyingCTE) es_output_cid = ...;
    //               if (!hasModifyingCTE) eflags |= EXEC_FLAG_SKIP_TRIGGERS;
    //   CMD_{INSERT,DELETE,UPDATE,MERGE}: es_output_cid = GetCurrentCommandId(true);
    //   default: elog(ERROR, "unrecognized operation code").
    // es_output_cid is set inside the `with_mut` work-bundle borrow below; the
    // command id we need to write (if any) is computed here so the xact-owner
    // call happens outside that borrow.
    let mut es_output_cid: Option<types_core::xact::CommandId> = None;
    match operation {
        CmdType::CMD_SELECT => {
            // SELECT FOR [KEY] UPDATE/SHARE and modifying CTEs need to mark
            // tuples: es_output_cid = GetCurrentCommandId(true).
            let needs_cid = query_desc.work.with(|w| {
                w.plannedstmt
                    .rowMarks
                    .as_ref()
                    .is_some_and(|m| !m.is_empty())
                    || w.plannedstmt.hasModifyingCTE
            });
            let has_modifying_cte =
                query_desc.work.with(|w| w.plannedstmt.hasModifyingCTE);
            if needs_cid {
                es_output_cid = Some(xact_seams::get_current_command_id::call(true)?);
            }
            // A SELECT without modifying CTEs can't queue triggers.
            if !has_modifying_cte {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
        }
        CmdType::CMD_INSERT
        | CmdType::CMD_DELETE
        | CmdType::CMD_UPDATE
        | CmdType::CMD_MERGE => {
            es_output_cid = Some(xact_seams::get_current_command_id::call(true)?);
        }
        _ => {
            return Err(types_error::PgError::error(alloc::format!(
                "unrecognized operation code: {}",
                operation as i32
            )));
        }
    }

    // estate->es_snapshot = RegisterSnapshot(queryDesc->snapshot);
    // estate->es_crosscheck_snapshot = RegisterSnapshot(queryDesc->crosscheck_snapshot);
    // (execMain.c standard_ExecutorStart.) The Rc clone is the refcount-bearing
    // registration; the QueryDesc keeps the originals alive for the query's
    // lifetime. Cloned out before borrowing the work bundle.
    let es_snapshot = query_desc.snapshot.clone();
    let es_crosscheck_snapshot = query_desc.crosscheck_snapshot.clone();

    query_desc.work.with_mut(|w| {
        if let Some(cid) = es_output_cid {
            w.estate.es_output_cid = cid;
        }
        // estate->es_sourceText = queryDesc->sourceText; re-homed into the
        // query context (the parallel executor ships it to workers). The source
        // text and the EState share the query arena, so this is a same-arena copy.
        let src = w.source_text.as_str().to_string();
        w.estate.es_sourceText =
            Some(mcx::PgString::from_str_in(&src, w.estate.es_query_cxt)?);
        w.estate.es_param_list_info = params;
        w.estate.es_snapshot = es_snapshot.clone();
        w.estate.es_crosscheck_snapshot = es_crosscheck_snapshot.clone();
        w.estate.es_top_eflags = eflags;
        w.estate.es_instrument = instrument;
        w.estate.es_jit_flags = w.plannedstmt.jitFlags;
        // if (queryDesc->plannedstmt->paramExecTypes != NIL) {
        //     int nParamExec = list_length(...paramExecTypes);
        //     estate->es_param_exec_vals = palloc0(nParamExec * sizeof(ParamExecData));
        // }
        // The PARAM_EXEC value array is sized from the planner's paramExecTypes
        // list and zero-initialized (each slot's execPlan/value/isnull cleared);
        // the initplan loop (InitPlan) and the param-fetch machinery poke into it
        // by paramid.
        if let Some(param_exec_types) = w.plannedstmt.paramExecTypes.as_ref() {
            let n_param_exec = param_exec_types.len();
            let mcx = w.estate.es_query_cxt;
            let mut vals = mcx::vec_with_capacity_in(mcx, n_param_exec)?;
            for _ in 0..n_param_exec {
                vals.push(types_nodes::ParamExecData::default());
            }
            w.estate.es_param_exec_vals = vals;
        }
        Ok::<(), types_error::PgError>(())
    })?;

    // AfterTriggerBeginQuery() unless SKIP_TRIGGERS / EXPLAIN_ONLY. For a plain
    // SELECT, SKIP_TRIGGERS is set above, so this is elided; for a DML query
    // with triggers it opens the after-trigger query level.
    if (eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY)) == 0 {
        backend_commands_trigger_seams::after_trigger_begin_query::call()?;
    }

    // InitPlan(queryDesc, eflags).
    InitPlan(query_desc, eflags)
}

/// `InitPlan(queryDesc, eflags)` (execMain.c) — build the plan-state tree, the
/// result tupdesc and the top junk filter.
fn InitPlan(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()> {
    let operation = query_desc.operation;

    query_desc.with_plan_and_estate_mut(|plan, plannedstmt, estate, planstate_slot| {
        // Do permissions checks: ExecCheckPermissions(rangeTable, permInfos, true).
        // ExecCheckPermissions ereports (ACLCHECK) on a violation; the rangeTable
        // is unused (it survives only for the un-modeled ExecutorCheckPerms_hook).
        if let Some(perm_infos) = plannedstmt.permInfos.as_ref() {
            exec_check_permissions(perm_infos.as_slice(), true)?;
        }

        // ExecInitRangeTable(estate, rangeTable, permInfos, bms_copy(unprunableRelids)).
        // C aliases the planner-owned lists into the EState (`estate->es_range_table
        // = plannedstmt->rtable`), leaving them reachable through the bundle's
        // plannedstmt afterward (EXPLAIN reads `queryDesc->plannedstmt->rtable`
        // post-ExecutorStart). The owned model can't share the same allocation, so
        // hand the EState a clone and leave the bundle's plannedstmt lists intact.
        let qcx = estate.es_query_cxt;
        let range_table = match plannedstmt.rtable.as_ref() {
            Some(rt) => {
                let mut out = mcx::vec_with_capacity_in(qcx, rt.len())?;
                for rte in rt.iter() {
                    out.push(rte.clone_in(qcx)?);
                }
                out
            }
            None => mcx::PgVec::new_in(qcx),
        };
        let perm_infos = match plannedstmt.permInfos.as_ref() {
            Some(pi) => {
                let mut out = mcx::vec_with_capacity_in(qcx, pi.len())?;
                for p in pi.iter() {
                    out.push(p.clone_in(qcx)?);
                }
                out
            }
            None => mcx::PgVec::new_in(qcx),
        };
        let unpruned = match plannedstmt.unprunableRelids.as_ref() {
            Some(b) => Some(mcx::alloc_in(qcx, b.clone_in(qcx)?)?),
            None => None,
        };
        execUtils::ExecInitRangeTable(estate, range_table, perm_infos, unpruned)?;

        // estate->es_plannedstmt = plannedstmt;  (C aliases; owned model clones a
        // copyObject-shape copy into the per-query context so the EState is
        // self-contained — read by ExecGetRangeTableRelation / ...IsTargetRelation.)
        let mcx = estate.es_query_cxt;
        estate.es_plannedstmt = Some(mcx::alloc_in(mcx, plannedstmt.clone_in(mcx)?)?);

        // estate->es_part_prune_infos = plannedstmt->partPruneInfos;
        // The owned executor consumes each PartitionPruneInfo as the type-erased
        // payload of an `Opaque`; clone the planner-produced carriers into the
        // per-query context as `Opaque(Box<dyn Any>)`.
        for pinfo in plannedstmt.partPruneInfos.iter() {
            // Deep-clone the carrier into the per-query context, then erase to the
            // arena `'static` the type-erased `Opaque(Box<dyn Any>)` payload requires
            // (the sanctioned partprune arena-intern boundary).
            let owned =
                types_nodes::partprune_carrier::partpruneinfo_into_static(pinfo.clone_in(mcx)?);
            estate
                .es_part_prune_infos
                .push(types_nodes::Opaque(Some(alloc::boxed::Box::new(owned))));
        }

        // ExecDoInitialPruning(estate) — perform executor-startup pruning. The
        // results (bitmapsets of surviving subplan indexes) are stored in
        // es_part_prune_results, parallel to es_part_prune_infos.
        if !estate.es_part_prune_infos.is_empty() {
            let mcx = estate.es_query_cxt;
            backend_executor_execPartition_seams::exec_do_initial_pruning::call(mcx, estate)?;
        }

        // Build the ExecRowMark array from PlanRowMark(s), if any.
        if plannedstmt.rowMarks.as_ref().is_some_and(|m| !m.is_empty()) {
            init_plan_rowmarks(estate)?;
        }

        // estate->es_tupleTable = NIL;  (CreateExecutorState left it empty.)
        // estate->es_epq_active = NULL; (default None.)

        // Initialize private state information for each SubPlan.  We must do this
        // before running ExecInitNode on the main query tree, since
        // ExecInitSubPlan expects to be able to find these entries.
        //   Assert(estate->es_subplanstates == NIL);
        //   i = 1;                       /* subplan indices count from 1 */
        //   foreach(l, plannedstmt->subplans) {
        //       Plan *subplan = (Plan *) lfirst(l);
        //       sp_eflags = eflags & ~(EXEC_FLAG_REWIND|EXEC_FLAG_BACKWARD|EXEC_FLAG_MARK);
        //       if (bms_is_member(i, plannedstmt->rewindPlanIDs)) sp_eflags |= EXEC_FLAG_REWIND;
        //       subplanstate = ExecInitNode(subplan, estate, sp_eflags);
        //       estate->es_subplanstates = lappend(estate->es_subplanstates, subplanstate);
        //       i++;
        //   }
        debug_assert!(estate.es_subplanstates.is_empty());
        // A subplan will never need BACKWARD scan nor MARK/RESTORE; we also strip
        // REWIND and only re-add it for parameterless subplans the planner flagged
        // in `rewindPlanIDs`. The trimmed `PlannedStmt` does not model the
        // `rewindPlanIDs` Bitmapset, so no subplan is suggested REWIND here (the
        // common case: initplans/correlated subplans are never in rewindPlanIDs).
        let sp_eflags = eflags & !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);
        // C `foreach(l, plannedstmt->subplans)` reads the list non-destructively;
        // the executor's per-SubPlan plan-state trees alias the planner-owned
        // subplan Plan nodes, and EXPLAIN later re-reads `plannedstmt->subplans`
        // (e.g. a CteScan resolving `dpns->subplans[ctePlanId-1]` for FieldSelect
        // field names). The owned model can't share the allocation, so — exactly
        // like the `rtable`/`permInfos` clones above — we clone each subplan tree
        // into the EState's query context for ExecInitNode and leave the bundle's
        // `plannedstmt.subplans` intact for EXPLAIN deparse. (A prior `.take()`
        // here emptied it, so `pstmt.subplans` reached EXPLAIN as None and every
        // CteScan field deparsed to the fallback `fN` names.)
        let n_subplans = plannedstmt.subplans.as_ref().map(|s| s.len()).unwrap_or(0);
        for idx in 0..n_subplans {
                // lfirst(l) — a `Plan *`; an entry can be NULL (a pruned/unused
                // subplan slot), in which case ExecInitNode(NULL, ...) yields a
                // NULL plan-state. Clone the subplan tree into `mcx` and leak the
                // owning box into an honest `&'mcx Node` (it lives until the
                // per-query context drops, faithful to C's "plan freed with its
                // context"), exactly like the main planTree.
                let subnode: Option<&types_nodes::nodes::Node<'_>> = {
                    let subplan_box: Option<mcx::PgBox<'_, types_nodes::nodes::Node<'_>>> =
                        match plannedstmt.subplans.as_ref().and_then(|s| s[idx].as_ref()) {
                            Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                            None => None,
                        };
                    subplan_box.map(|tree| &*mcx::leak_in(tree))
                };
                // subplanstate = ExecInitNode(subplan, estate, sp_eflags);
                // es_subplanstates = lappend(es_subplanstates, subplanstate).
                // A NULL subplan slot (pruned/unused) lappends a NULL, preserving
                // the 1-based plan_id index.
                //
                // Owned-model only: mark the slot this subplan root will occupy so a
                // non-canSetTag ModifyTable that *is* this subplan root (a
                // data-modifying CTE) can register the correct `es_subplanstates`
                // index in `es_auxmodifytables`. The marker is cleared after the init
                // so the main plan tree (and anything outside the subplan loop) reads
                // `None` — a non-canSetTag ModifyTable there is the main tree, run to
                // completion by the portal, not a subplan, and must not be registered.
                let slot = estate.es_subplanstates.len();
                estate.es_subplan_root_slot = Some(slot);
                let init_result = procnode::exec_init_node::call(mcx, subnode, estate, sp_eflags);
                estate.es_subplan_root_slot = None;
                let subplanstate = init_result?;
                estate.es_subplanstates.push(subplanstate);
        }

        // planstate = ExecInitNode(plan, estate, eflags).
        let planstate = procnode::exec_init_node::call(mcx, plan, estate, eflags)?;
        *planstate_slot = planstate;

        // Get the tuple descriptor describing the type of tuples to return, and
        // initialize the junk filter if the SELECT tlist has junk attrs.
        let planstate_ref = match planstate_slot.as_deref() {
            Some(ps) => ps,
            None => {
                // ExecInitNode(NULL,...) — degenerate plan; nothing more to do.
                return Ok::<(), types_error::PgError>(());
            }
        };

        // operation == CMD_SELECT: junk_filter_needed = any resjunk in tlist.
        if operation == CmdType::CMD_SELECT {
            // plan->targetlist (the leaked planTree node's tlist).
            let junk_filter_needed = plan
                .map(|n| {
                    n.plan_head()
                        .targetlist
                        .as_ref()
                        .map(|tl| tl.iter().any(|tle| tle.resjunk))
                        .unwrap_or(false)
                })
                .unwrap_or(false);

            if junk_filter_needed {
                // j = ExecInitJunkFilter(planstate->plan->targetlist, slot);
                // (C makes a virtual extra slot first; ExecInitJunkFilter with
                // None allocates the equivalent virtual result slot.)
                let _ = planstate_ref;
                let src_tlist = plan
                    .and_then(|n| n.plan_head().targetlist.as_ref())
                    .expect("junk filter needed ⇒ plan has a targetlist");
                let mut tlist = mcx::vec_with_capacity_in(mcx, src_tlist.len())?;
                for tle in src_tlist.iter() {
                    tlist.push(tle.clone_in(mcx)?);
                }
                let jf = execJunk::ExecInitJunkFilter(estate, tlist, None)?;
                estate.es_junkFilter = Some(mcx::alloc_in(mcx, jf)?);
            }
        }
        Ok(())
    })
}

/// `ExecutorStart(queryDesc, eflags)` — the hookable entry; routes to
/// [`standard_ExecutorStart`] (no `ExecutorStart_hook` consumer yet).
pub fn ExecutorStart(query_desc: &mut QueryDesc, eflags: i32) -> PgResult<()> {
    standard_ExecutorStart(query_desc, eflags)
}

// ===========================================================================
// ExecutorRun / ExecutePlan
// ===========================================================================

/// `standard_ExecutorRun(queryDesc, direction, count)` (execMain.c) — the main
/// executor driver: start the receiver, run the plan, shut the receiver.
pub fn standard_ExecutorRun(
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) -> PgResult<()> {
    // Assert(!(es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY)).
    let operation = query_desc.operation;
    let dest_handle = query_desc.dest;

    // estate->es_processed = 0;
    query_desc.with_estate_mut(|estate| {
        estate.es_processed = 0;
    });

    // sendTuples = (operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning);
    let has_returning = query_desc.work.with(|w| w.plannedstmt.hasReturning);
    let send_tuples = operation == CmdType::CMD_SELECT || has_returning;

    // startup tuple receiver, if we will be emitting tuples.
    if send_tuples {
        // dest->rStartup(dest, operation, queryDesc->tupDesc);
        let res = query_desc.with_estate_and_planstate_mut(|estate, planstate| {
            // dest->rStartup runs in the per-query arena (mcx-vtable keystone):
            // an intorel-style receiver opens its relation/BulkInsertState here.
            let mcx = estate.es_query_cxt;
            let tupdesc = result_tupdesc(estate, planstate.as_deref());
            match tupdesc {
                Some(td) => dest::dest_rstartup::call(mcx, dest_handle, operation, td),
                None => {
                    // No result tupdesc (degenerate plan); the C would still pass
                    // the (NULL-ish) descriptor. A plain SELECT always has one.
                    panic!(
                        "execMain ExecutorRun: dest->rStartup needs queryDesc->tupDesc but the \
                         started plan produced no result TupleDesc"
                    );
                }
            }
        });
        res?;
    }

    // Run plan, unless direction is NoMovement.
    if !ScanDirectionIsNoMovement(direction) {
        ExecutePlan(query_desc, operation, send_tuples, count, direction, dest_handle)?;
    }

    // estate->es_total_processed += estate->es_processed;
    query_desc.with_estate_mut(|estate| {
        estate.es_total_processed += estate.es_processed;
    });

    // shutdown tuple receiver, if we started it.
    //
    // The receiver's shutdown runs in the per-query arena (mcx-vtable keystone:
    // an intorel-style receiver frees its BulkInsertState / closes its relation
    // here). The McxOwned bundle never lets an `Mcx<'mcx>` escape its closure,
    // so the dispatch is done inside `with_estate_mut`.
    if send_tuples {
        query_desc.with_estate_mut(|estate| {
            let mcx = estate.es_query_cxt;
            dest::dest_rshutdown::call(mcx, dest_handle)
        })?;
    }
    Ok(())
}

/// `ExecutePlan(...)` (execMain.c) — process the plan until `numberTuples`
/// tuples have been retrieved (0 = run to completion).
fn ExecutePlan(
    query_desc: &mut QueryDesc,
    operation: CmdType,
    send_tuples: bool,
    number_tuples: u64,
    direction: ScanDirection,
    dest_handle: DestReceiverHandle,
) -> PgResult<()> {
    // use_parallel_mode: parallel mode only supports complete execution.
    //   if (already_executed || numberTuples != 0) false;
    //   else queryDesc->plannedstmt->parallelModeNeeded;
    let parallel_mode_needed = query_desc.work.with(|w| w.plannedstmt.parallelModeNeeded);
    let use_parallel_mode =
        !(query_desc.already_executed || number_tuples != 0) && parallel_mode_needed;
    query_desc.already_executed = true;

    // estate->es_use_parallel_mode = use_parallel_mode;
    // if (use_parallel_mode) EnterParallelMode();  (C:1696-1698)
    if use_parallel_mode {
        backend_access_transam_parallel_rt_seams::enter_parallel_mode::call()?;
    }

    let mut current_tuple_count: u64 = 0;

    let run_result = query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        // estate->es_direction = direction;  estate->es_use_parallel_mode = ...
        estate.es_direction = direction;
        estate.es_use_parallel_mode = use_parallel_mode;

        let planstate = match planstate {
            Some(ps) => ps,
            None => return Ok::<(), types_error::PgError>(()),
        };

        loop {
            // ResetPerTupleExprContext(estate): reset es_per_tuple_exprcontext if
            // one has been created (none on the plain path).
            if let Some(ecxt) = estate.es_per_tuple_exprcontext {
                execUtils_seams::reset_expr_context::call(estate, ecxt)?;
            }

            // slot = ExecProcNode(planstate).
            let slot = procnode::exec_proc_node::call(planstate, estate)?;

            // if (TupIsNull(slot)) break;
            // TupIsNull(slot) == (slot == NULL || TTS_EMPTY(slot)): the C
            // ExecProcNode return at end-of-scan is a *non-NULL* but cleared
            // slot (e.g. ExecScan's `return ExecClearTuple(resultslot)`), so the
            // empty-flag check is load-bearing — without it the cleared virtual
            // result slot would be sent to the dest receiver.
            let slot = match slot {
                Some(s) if !estate.slot(s).is_empty() => s,
                _ => break,
            };

            // If we have a junk filter, project a new clean tuple.
            //   if (estate->es_junkFilter != NULL)
            //       slot = ExecFilterJunk(estate->es_junkFilter, slot);
            let out_slot = if estate.es_junkFilter.is_some() {
                // Move the filter out to satisfy the borrow checker, run the
                // filter against the live estate, then restore it.
                let jf = estate.es_junkFilter.take().unwrap();
                let res = execJunk::ExecFilterJunk(estate, &jf, slot);
                estate.es_junkFilter = Some(jf);
                res?
            } else {
                slot
            };

            // If we are supposed to send the tuple somewhere, do so.
            //   if (!dest->receiveSlot(slot, dest)) break;
            if send_tuples {
                let cont = {
                    // The per-query arena the receiver works in (mcx-vtable
                    // keystone): an intorel-style receiver drives
                    // `table_tuple_insert(mcx, &rel, slot, …)` here.
                    let mcx = estate.es_query_cxt;
                    let slot_data = estate.slot_data_mut(out_slot);
                    dest::dest_receive_slot::call(mcx, slot_data, dest_handle)?
                };
                if !cont {
                    break;
                }
            }

            // Count tuples processed (SELECT only).
            if operation == CmdType::CMD_SELECT {
                estate.es_processed += 1;
            }

            // check our tuple count.
            current_tuple_count += 1;
            if number_tuples != 0 && number_tuples == current_tuple_count {
                break;
            }
        }

        // If we won't need to back up, release resources now.
        //   if (!(es_top_eflags & EXEC_FLAG_BACKWARD)) ExecShutdownNode(planstate);
        if estate.es_top_eflags & EXEC_FLAG_BACKWARD == 0 {
            procnode::exec_shutdown_node::call(planstate, estate)?;
        }
        Ok(())
    });

    // if (use_parallel_mode) ExitParallelMode();  (C:1771-1772). Run it even on
    // an error from the execution loop so the parallel-mode nesting count is
    // balanced before the error propagates (the C cleanup path likewise leaves
    // parallel mode during abort).
    if use_parallel_mode {
        backend_access_transam_parallel_rt_seams::exit_parallel_mode::call()?;
    }
    run_result?;
    Ok(())
}

/// `ExecutorRun(queryDesc, direction, count)` — routes to
/// [`standard_ExecutorRun`].
pub fn ExecutorRun(
    query_desc: &mut QueryDesc,
    direction: ScanDirection,
    count: u64,
) -> PgResult<()> {
    standard_ExecutorRun(query_desc, direction, count)
}

/// `ExecutorRewind(queryDesc)` (execMain.c) — rewind the executor to the start of
/// the query so it can be re-run. It's not sensible to rescan an updating query,
/// so the C asserts `operation == CMD_SELECT`; the work is `ExecReScan(planstate)`
/// in the per-query memory context (the owned bundle's context is already current
/// when the plan-state tree is touched).
pub fn ExecutorRewind(query_desc: &mut QueryDesc) -> PgResult<()> {
    // Assert(queryDesc->operation == CMD_SELECT).
    debug_assert!(query_desc.operation == CmdType::CMD_SELECT);
    // ExecReScan(queryDesc->planstate) — a degenerate (NULL planstate) plan has
    // nothing to rescan (the C dereferences a real planstate; a started SELECT
    // always has one).
    query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        if let Some(planstate) = planstate {
            backend_executor_execAmi_seams::exec_re_scan::call(planstate, estate)?;
        }
        Ok::<(), types_error::PgError>(())
    })
}

// ===========================================================================
// ExecutorEnd / ExecEndPlan
// ===========================================================================

/// `standard_ExecutorEnd(queryDesc)` (execMain.c) — shut the plan-state tree
/// down and release the per-query state.
///
/// `ExecEndPlan` closes relations / drops buffer pins; dropping the
/// [`QueryDesc`] afterward releases the per-query context (the owned model's
/// `FreeExecutorState`).
pub fn standard_ExecutorEnd(query_desc: &mut QueryDesc) -> PgResult<()> {
    // Assert(estate); Assert(es_finished || EXPLAIN_ONLY). ExecEndPlan(planstate, estate).
    ExecEndPlan(query_desc)?;
    // estate->es_finished = true; (informational on the owned bundle.)
    query_desc.with_estate_mut(|estate| {
        estate.es_finished = true;
    });
    // FreeExecutorState(estate): the per-query *memory* context (and the
    // EState/plan-state tree it holds) is freed when the caller drops the
    // QueryDesc bundle, but the non-memory teardown C's FreeExecutorState runs
    // must happen here — in particular it destroys `es_partition_directory`,
    // releasing (forgetting) the relcache pins the planner/executor partition
    // routing took (`RelationIncrementReferenceCount` registers each with the
    // current resource owner). Without this, those pins outlive the resource
    // owner and a partitioned INSERT/SELECT reports "resource was not closed".
    query_desc.with_estate_mut(execUtils::free_executor_state_teardown)?;
    Ok(())
}

/// `ExecEndPlan(planstate, estate)` (execMain.c) — close files / drop pins.
/// The "subplan was not initialized" check (nodeSubplan.c:818-827). The
/// `SubPlanState` reaches its child plan state by the subplan's 1-based
/// `plan_id` index into `es_subplanstates`; this verifies that slot exists.
fn link_subplan_planstate(
    estate: &types_nodes::EStateData<'_>,
    plan_id: i32,
) -> PgResult<()> {
    let idx = (plan_id as usize)
        .checked_sub(1)
        .filter(|&i| i < estate.es_subplanstates.len());
    match idx {
        Some(_) => Ok(()),
        None => Err(types_error::PgError::error("subplan was not initialized")),
    }
}

fn ExecEndPlan(query_desc: &mut QueryDesc) -> PgResult<()> {
    query_desc.with_estate_and_planstate_mut(|estate, planstate| {
        // shut down the node-type-specific query processing.
        if let Some(planstate) = planstate {
            procnode::exec_end_node::call(planstate, estate)?;
        }
        // for subplans too:
        //   foreach(l, estate->es_subplanstates) {
        //       PlanState *subplanstate = (PlanState *) lfirst(l);
        //       ExecEndNode(subplanstate);
        //   }
        // es_subplanstates owns each subplan's plan-state tree (the owned model's
        // teardown owner). Move the vec out so each child box can be ended with a
        // live `&mut estate` borrow (no self-alias), then drop the emptied vec.
        let subplanstates = core::mem::replace(
            &mut estate.es_subplanstates,
            mcx::PgVec::new_in(estate.es_query_cxt),
        );
        for subplanstate in subplanstates {
            // ExecEndNode(NULL) is a no-op; only end real (non-pruned) slots.
            let Some(mut subplanstate) = subplanstate else {
                continue;
            };
            procnode::exec_end_node::call(&mut subplanstate, estate)?;
        }
        Ok::<(), types_error::PgError>(())
    })?;

    // destroy the executor's tuple table (release buffer pins / tupdesc refs).
    query_desc.with_estate_mut(|estate| execUtils::ExecResetTupleTable(estate, false))?;

    // Close any Relations opened for range table / result relations.
    query_desc.with_estate_mut(execUtils::ExecCloseResultRelations)?;
    query_desc.with_estate_mut(execUtils::ExecCloseRangeTableRelations)?;
    Ok(())
}

/// `ExecutorEnd(queryDesc)` — routes to [`standard_ExecutorEnd`].
pub fn ExecutorEnd(query_desc: &mut QueryDesc) -> PgResult<()> {
    standard_ExecutorEnd(query_desc)
}

/// `ExecutorFinish(queryDesc)` (execMain.c) — run the executor's after-query
/// cleanup. For a plain `SELECT` with `EXEC_FLAG_SKIP_TRIGGERS` set there are no
/// secondary ModifyTable nodes and no AFTER triggers, so this is a no-op; the
/// non-trivial `ExecPostprocessPlan` / `AfterTriggerEndQuery` body lands with
/// the ModifyTable + trigger owners.
/// `ExecPostprocessPlan(estate)` (execMain.c) — run any secondary (non-canSetTag)
/// ModifyTable nodes to completion, in case the main query did not fetch all rows
/// from them. This is what makes a data-modifying CTE whose output the outer query
/// never reads (`WITH t AS (INSERT ... RETURNING *) SELECT 1`) still execute fully.
///
/// In C the loop walks `estate->es_auxmodifytables`, a `List` of `ModifyTableState *`
/// aliases, calling `ExecProcNode(ps)` until `TupIsNull`. The owned plan-state model
/// stores those nodes once, in `es_subplanstates`; this list holds the **index** of
/// each (see [`EStateData::es_auxmodifytables`]). For each, we take the owned
/// plan-state box out of its `es_subplanstates` slot (so it can run with a live
/// `&mut estate`, no self-alias — the `take_subplanstate`/`put_subplanstate` pattern
/// `nodeSubplan` uses), drive `ExecProcNode` to end-of-scan, then put it back.
fn ExecPostprocessPlan(estate: &mut types_nodes::execnodes::EStateData<'_>) -> PgResult<()> {
    // Make sure nodes run forward.
    //   estate->es_direction = ForwardScanDirection;
    estate.es_direction = ScanDirection::ForwardScanDirection;

    // Run any secondary ModifyTable nodes to completion, in case the main query
    // did not fetch all rows from them.
    //   foreach(lc, estate->es_auxmodifytables) { ps = lfirst(lc); for(;;) {...} }
    //
    // Snapshot the index list: the loop body borrows `estate` mutably (take/run/put)
    // and the list itself does not change during postprocessing.
    let aux_indices: alloc::vec::Vec<usize> = estate.es_auxmodifytables.iter().copied().collect();
    for idx in aux_indices {
        // ps = (PlanState *) lfirst(lc) — the aux ModifyTableState, owned by its
        // `es_subplanstates` slot. Take it out so ExecProcNode can run with the
        // live `&mut estate`.
        let mut ps = estate
            .es_subplanstates
            .get_mut(idx)
            .and_then(|slot| slot.take())
            .ok_or_else(|| {
                types_error::PgError::error(
                    "ExecPostprocessPlan: es_auxmodifytables index has no es_subplanstates entry",
                )
            })?;

        // for (;;) { ResetPerTupleExprContext(estate); slot = ExecProcNode(ps);
        //            if (TupIsNull(slot)) break; }
        let run = (|| -> PgResult<()> {
            loop {
                // Reset the per-output-tuple exprcontext each time (only if one
                // has been created).
                if let Some(ecxt) = estate.es_per_tuple_exprcontext {
                    execUtils_seams::reset_expr_context::call(estate, ecxt)?;
                }

                // slot = ExecProcNode(ps).
                let slot = procnode::exec_proc_node::call(&mut ps, estate)?;

                // if (TupIsNull(slot)) break; — a non-NULL but cleared slot is also
                // end-of-scan, so test the empty flag too.
                match slot {
                    Some(s) if !estate.slot(s).is_empty() => continue,
                    _ => break,
                }
            }
            Ok(())
        })();

        // Restore the owned plan-state box even if the run errored.
        estate.es_subplanstates[idx] = Some(ps);
        run?;
    }

    Ok(())
}

pub fn standard_ExecutorFinish(query_desc: &mut QueryDesc) -> PgResult<()> {
    // ExecPostprocessPlan(estate): run secondary ModifyTable nodes to completion.
    query_desc.with_estate_mut(ExecPostprocessPlan)?;
    // AfterTriggerEndQuery(estate) unless SKIP_TRIGGERS — fires this query
    // level's AFTER IMMEDIATE events. SKIP_TRIGGERS is set for the plain SELECT,
    // so it is elided there.
    let skip_triggers =
        query_desc.work.with(|w| (w.estate.es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS) != 0);
    if !skip_triggers {
        query_desc.with_estate_mut(|estate| {
            backend_commands_trigger_seams::after_trigger_end_query::call(estate)
        })?;
    }
    query_desc.with_estate_mut(|estate| {
        estate.es_finished = true;
    });
    Ok(())
}

/// `ExecutorFinish(queryDesc)` — routes to [`standard_ExecutorFinish`].
pub fn ExecutorFinish(query_desc: &mut QueryDesc) -> PgResult<()> {
    standard_ExecutorFinish(query_desc)
}

/// `FreeQueryDesc(queryDesc)` — free a finished `QueryDesc` (drops the owned
/// bundle, releasing the per-query context).
pub fn FreeQueryDesc(query_desc: QueryDesc) -> PgResult<()> {
    drop(query_desc);
    Ok(())
}

/// The EXPLAIN side of `ExplainOnePlan` (explain.c): `CreateQueryDesc` (with the
/// already-pushed active snapshot, the discard receiver `None_Receiver`, and the
/// caller's `instrument_option`) followed by `ExecutorStart(queryDesc, eflags)`.
/// Returns the started [`QueryDesc`] whose plan-state tree the explain unit
/// walks. Installed for `backend_executor_execMain_seams::
/// create_query_desc_and_start_explain`.
#[allow(clippy::too_many_arguments)]
pub fn CreateQueryDescAndStartExplain(
    parent: &MemoryContext,
    plan: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    params: ParamListInfo,
    instrument_option: i32,
    eflags: i32,
    dest: types_nodes::parsestmt::DestReceiverHandle,
) -> PgResult<QueryDesc> {
    // queryDesc = CreateQueryDesc(plannedstmt, queryString, GetActiveSnapshot(),
    //     InvalidSnapshot, dest, params, queryEnv, instrument_option);
    //
    // The caller selects `dest` exactly as explain.c does: `None_Receiver`
    // (discard) for a plain EXPLAIN, or the `DR_intorel` receiver from
    // `CreateIntoRelDestReceiver(into)` for `EXPLAIN ... CREATE TABLE AS`. A NULL
    // handle stands for the plain-EXPLAIN `None_Receiver`: C's `None_Receiver` is
    // `&donothingDR` — a real (no-op) receiver, NOT NULL. An EXPLAIN ANALYZE runs
    // the analyzed query (`ExecutorRun`), whose result slots are routed to this
    // receiver; a NULL handle would fault the router's lookup, so obtain the
    // genuine `DestNone` receiver (`CreateDestReceiver(DestNone)` → the static
    // `donothingDR`) when the caller passed NULL.
    let dest = if dest == types_nodes::parsestmt::DestReceiverHandle::NULL {
        dest::create_dest_receiver::call(types_dest::CommandDest::None)
    } else {
        dest
    };
    let mut query_desc = CreateQueryDesc(
        parent,
        plan,
        source_text,
        snapshot,
        None, // InvalidSnapshot crosscheck_snapshot
        dest, // None_Receiver (discard) = donothingDR, or DR_intorel for CTAS
        params,
        instrument_option,
    )?;
    // ExecutorStart(queryDesc, eflags);
    ExecutorStart(&mut query_desc, eflags)?;
    Ok(query_desc)
}

/// The COPY-(query)-TO executor setup (copyto.c:838-850): `CreateQueryDesc(plan,
/// sourceText, GetActiveSnapshot(), InvalidSnapshot, dest, NULL, NULL, 0)` then
/// `ExecutorStart(queryDesc, 0)`. The COPY-OUT `DestReceiver` is the handle the
/// caller built; the active snapshot is the copied one it has just pushed
/// (copyto.c:830-831). Installed for `create_query_desc_and_start`.
pub fn CreateQueryDescAndStartCopy(
    parent: &MemoryContext,
    plan: &PlannedStmt<'_>,
    source_text: &str,
    copy_receiver: u64,
) -> PgResult<QueryDesc> {
    // GetActiveSnapshot() — the caller pushed PushCopiedSnapshot just before.
    let snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;
    let mut query_desc = CreateQueryDesc(
        parent,
        plan,
        source_text,
        snapshot,
        None,                                     // InvalidSnapshot crosscheck_snapshot
        DestReceiverHandle(copy_receiver),        // COPY-OUT receiver
        None,                                     // NULL params
        0,                                        // instrument_options
    )?;
    // ExecutorStart(queryDesc, 0);
    ExecutorStart(&mut query_desc, 0)?;
    Ok(query_desc)
}

/// `ExecutorRun(queryDesc, ForwardScanDirection, 0)` (copyto.c:1104) for the
/// COPY-(query)-TO path. Installed for `executor_run_copy`.
pub fn ExecutorRunCopy(query_desc: &mut QueryDesc) -> PgResult<()> {
    standard_ExecutorRun(query_desc, ScanDirection::ForwardScanDirection, 0)
}

/// The COPY-(query)-TO teardown (copyto.c:1010-1012): `ExecutorFinish` +
/// `ExecutorEnd` + `FreeQueryDesc`. Installed for `end_copy_query`.
pub fn EndCopyQuery(mut query_desc: QueryDesc) -> PgResult<()> {
    ExecutorFinish(&mut query_desc)?;
    ExecutorEnd(&mut query_desc)?;
    FreeQueryDesc(query_desc)
}

// ===========================================================================
// Result tupdesc helper (queryDesc->tupDesc)
// ===========================================================================

/// `queryDesc->tupDesc` — the result `TupleDesc` the receiver is started with:
/// the top plan node's result tupdesc, or the junk filter's cleaned tupdesc
/// when a junk filter is present. Returns a borrow tied to the EState/planstate.
fn result_tupdesc<'a, 'mcx>(
    estate: &'a types_nodes::EStateData<'mcx>,
    planstate: Option<&'a types_nodes::PlanStateNode<'mcx>>,
) -> Option<&'a types_tuple::heaptuple::TupleDescData<'mcx>> {
    if let Some(jf) = estate.es_junkFilter.as_deref() {
        return jf.jf_cleanTupType.as_deref();
    }
    planstate.and_then(|ps| ps.ps_head().ps_ResultTupleDesc.as_deref())
}

// ===========================================================================
// Permission checks (the self-contained, fully-portable surface).
// ===========================================================================

/// `CreateCommandName((Node *) plannedstmt)` (tcop/utility.h + cmdtag.c) for the
/// `PlannedStmt` node — `GetCommandTagName(CreateCommandTag(...))`. Only the
/// plannable command types (the ones `ExecCheckXactReadOnly` /
/// `PreventCommandIfParallelMode` name) are reachable here; a `CMD_UTILITY`
/// PlannedStmt never reaches the executor's read-only gate (utility statements
/// are gated in ProcessUtility). The SELECT branch reproduces CreateCommandTag's
/// rowMark-strength refinement so complaints about read-only SELECT FOR UPDATE
/// statements read faithfully.
fn create_command_name(plannedstmt: &PlannedStmt<'_>) -> &'static str {
    // `LockClauseStrength` values (nodes/lockoptions.h): LCS_FORKEYSHARE = 1,
    // LCS_FORSHARE = 2, LCS_FORNOKEYUPDATE = 3, LCS_FORUPDATE = 4.
    match plannedstmt.commandType {
        CmdType::CMD_SELECT => {
            // We take a little extra care here so that the result will be useful
            // for complaints about read-only statements.
            if let Some(strength) = plannedstmt
                .rowMarks
                .as_ref()
                .and_then(|m| m.first())
                .map(|rm| rm.strength)
            {
                match strength {
                    1 => "SELECT FOR KEY SHARE",
                    2 => "SELECT FOR SHARE",
                    3 => "SELECT FOR NO KEY UPDATE",
                    4 => "SELECT FOR UPDATE",
                    _ => "SELECT",
                }
            } else {
                "SELECT"
            }
        }
        CmdType::CMD_UPDATE => "UPDATE",
        CmdType::CMD_INSERT => "INSERT",
        CmdType::CMD_DELETE => "DELETE",
        CmdType::CMD_MERGE => "MERGE",
        _ => "???",
    }
}

/// `ExecCheckXactReadOnly(plannedstmt)` (execMain.c) — fail if write permissions
/// are requested in parallel mode for any table (temp or non-temp), otherwise
/// fail for any non-temp table; then forbid non-SELECT / modifying-CTE plans in
/// parallel mode.
pub fn ExecCheckXactReadOnly(plannedstmt: &PlannedStmt<'_>) -> PgResult<()> {
    // Fail if write permissions are requested in parallel mode for table (temp
    // or non-temp), otherwise fail for any non-temp table.
    if let Some(perm_infos) = plannedstmt.permInfos.as_ref() {
        for perminfo in perm_infos.iter() {
            if (perminfo.requiredPerms & !ACL_SELECT) == 0 {
                continue;
            }

            let nspid = lsyscache::get_rel_namespace::call(perminfo.relid)?;
            if namespace_seam::is_temp_namespace::call(nspid)? {
                continue;
            }

            xact_seams::prevent_command_if_read_only::call(create_command_name(plannedstmt))?;
        }
    }

    if plannedstmt.commandType != CmdType::CMD_SELECT || plannedstmt.hasModifyingCTE {
        xact_seams::prevent_command_if_parallel_mode::call(create_command_name(plannedstmt))?;
    }

    Ok(())
}

/// `ExecCheckOneRelPerms` for the SELECT-on-columns case (execMain.c), used by
/// the `exec_check_permissions_select` seam (`RI_Initial_Check`'s SELECT probe,
/// which passes column attnums as a slice rather than a `Bitmapset`).
fn exec_check_one_rel_perms_select(
    relid: Oid,
    selected_cols: &[i16],
    userid: Oid,
) -> PgResult<bool> {
    if aclchk::pg_class_aclcheck::call(relid, userid, ACL_SELECT)? == AclResult::AclcheckOk {
        return Ok(true);
    }

    if selected_cols.is_empty() {
        if aclchk::pg_attribute_aclcheck_all::call(
            relid,
            userid,
            ACL_SELECT,
            AclMaskHow::AclmaskAny,
        )? != AclResult::AclcheckOk
        {
            return Ok(false);
        }
        return Ok(true);
    }

    for &col in selected_cols {
        let attno = col + FirstLowInvalidHeapAttributeNumber;
        if attno == 0 {
            if aclchk::pg_attribute_aclcheck_all::call(
                relid,
                userid,
                ACL_SELECT,
                AclMaskHow::AclmaskAll,
            )? != AclResult::AclcheckOk
            {
                return Ok(false);
            }
        } else if aclchk::pg_attribute_aclcheck::call(relid, attno, userid, ACL_SELECT)?
            != AclResult::AclcheckOk
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
/// (execMain.c), restricted to the SELECT-on-relations form `RI_Initial_Check`
/// uses (the `exec_check_permissions_select` seam).
pub fn exec_check_permissions_select(
    rels: &[(Oid, u8, &[i16])],
    ereport_on_violation: bool,
) -> PgResult<bool> {
    let userid = miscinit::get_user_id::call();

    for &(relid, relkind, cols) in rels {
        let ok = exec_check_one_rel_perms_select(relid, cols, userid)?;
        if !ok {
            if ereport_on_violation {
                let objtype = objaddr::get_relkind_objtype::call(relkind);
                let name = lsyscache_get_rel_name(relid)?;
                aclchk::aclcheck_error::call(AclResult::AclcheckNoPriv, objtype, name)?;
            }
            return Ok(false);
        }
    }
    Ok(true)
}

/// `bms_is_empty(a)` (nodes/bitmapset.c) over the owned word storage: a set is
/// empty iff it is absent (C `NULL`) or every word is zero. The `bms_*`
/// constructors never leave trailing all-zero words, so this is faithful.
fn bms_is_empty(cols: &Option<mcx::PgBox<'_, Bitmapset<'_>>>) -> bool {
    match cols {
        None => true,
        Some(b) => b.words.as_slice().iter().all(|&w| w == 0),
    }
}

/// Iterate the members of a `Bitmapset *` in ascending order, mirroring the C
/// `while ((col = bms_next_member(cols, col)) >= 0)` loop. `f` receives each
/// member's bit number; `Bitmapset` bit `k` lives in word `k / BITS_PER_BITMAPWORD`
/// at position `k % BITS_PER_BITMAPWORD` (nodes/bitmapset.c numbering).
fn bms_for_each_member(
    cols: &Option<mcx::PgBox<'_, Bitmapset<'_>>>,
    mut f: impl FnMut(i32) -> PgResult<core::ops::ControlFlow<bool>>,
) -> PgResult<Option<bool>> {
    let Some(b) = cols else {
        return Ok(None);
    };
    for (wordnum, &word) in b.words.as_slice().iter().enumerate() {
        let mut w = word;
        while w != 0 {
            let bitnum = w.trailing_zeros() as usize;
            w &= w - 1;
            let member = (wordnum * BITS_PER_BITMAPWORD + bitnum) as i32;
            if let core::ops::ControlFlow::Break(early) = f(member)? {
                return Ok(Some(early));
            }
        }
    }
    Ok(None)
}

/// `ExecCheckPermissionsModified(relOid, userid, modifiedCols, requiredPerms)`
/// (execMain.c) — INSERT/UPDATE column-permission check (processed uniformly).
fn exec_check_permissions_modified(
    rel_oid: Oid,
    userid: Oid,
    modified_cols: &Option<mcx::PgBox<'_, Bitmapset<'_>>>,
    required_perms: AclMode,
) -> PgResult<bool> {
    // When the query doesn't explicitly update any columns, allow the query if
    // we have permission on any column of the rel.
    if bms_is_empty(modified_cols) {
        if aclchk::pg_attribute_aclcheck_all::call(
            rel_oid,
            userid,
            required_perms,
            AclMaskHow::AclmaskAny,
        )? != AclResult::AclcheckOk
        {
            return Ok(false);
        }
    }

    if let Some(early) = bms_for_each_member(modified_cols, |col| {
        // bit #s are offset by FirstLowInvalidHeapAttributeNumber.
        let attno = col + FirstLowInvalidHeapAttributeNumber as i32;
        if attno == 0 {
            // whole-row reference can't happen here (C: elog(ERROR)).
            panic!("execMain ExecCheckPermissionsModified: whole-row update is not implemented");
        }
        if aclchk::pg_attribute_aclcheck::call(rel_oid, attno as i16, userid, required_perms)?
            != AclResult::AclcheckOk
        {
            return Ok(core::ops::ControlFlow::Break(false));
        }
        Ok(core::ops::ControlFlow::Continue(()))
    })? {
        return Ok(early);
    }
    Ok(true)
}

/// `ExecCheckOneRelPerms(perminfo)` (execMain.c) — check access permissions for
/// a single relation.
fn exec_check_one_rel_perms(perminfo: &RTEPermissionInfo<'_>) -> PgResult<bool> {
    let rel_oid = perminfo.relid;
    let required_perms = perminfo.requiredPerms;
    debug_assert!(required_perms != 0);

    // userid to check as: current user unless we have a setuid indication.
    let userid = if types_core::primitive::OidIsValid(perminfo.checkAsUser) {
        perminfo.checkAsUser
    } else {
        miscinit::get_user_id::call()
    };

    // We must have *all* the requiredPerms bits, but some of the bits can be
    // satisfied from column-level rather than relation-level permissions.
    let rel_perms =
        aclchk::pg_class_aclmask::call(rel_oid, userid, required_perms, AclMaskHow::AclmaskAll)?;
    let remaining_perms = required_perms & !rel_perms;
    if remaining_perms != 0 {
        // If we lack any permissions that exist only as relation permissions,
        // we can fail straight away.
        if remaining_perms & !(ACL_SELECT | ACL_INSERT | ACL_UPDATE) != 0 {
            return Ok(false);
        }

        if remaining_perms & ACL_SELECT != 0 {
            // When the query doesn't explicitly reference any columns (e.g.
            // SELECT COUNT(*) FROM table), allow it if we have SELECT on any
            // column of the rel, as per SQL spec.
            if bms_is_empty(&perminfo.selectedCols)
                && aclchk::pg_attribute_aclcheck_all::call(
                    rel_oid,
                    userid,
                    ACL_SELECT,
                    AclMaskHow::AclmaskAny,
                )? != AclResult::AclcheckOk
            {
                return Ok(false);
            }

            if let Some(early) = bms_for_each_member(&perminfo.selectedCols, |col| {
                // bit #s are offset by FirstLowInvalidHeapAttributeNumber.
                let attno = col + FirstLowInvalidHeapAttributeNumber as i32;
                if attno == 0 {
                    // Whole-row reference, must have priv on all cols.
                    if aclchk::pg_attribute_aclcheck_all::call(
                        rel_oid,
                        userid,
                        ACL_SELECT,
                        AclMaskHow::AclmaskAll,
                    )? != AclResult::AclcheckOk
                    {
                        return Ok(core::ops::ControlFlow::Break(false));
                    }
                } else if aclchk::pg_attribute_aclcheck::call(
                    rel_oid,
                    attno as i16,
                    userid,
                    ACL_SELECT,
                )? != AclResult::AclcheckOk
                {
                    return Ok(core::ops::ControlFlow::Break(false));
                }
                Ok(core::ops::ControlFlow::Continue(()))
            })? {
                return Ok(early);
            }
        }

        // Basically the same for the mod columns, for both INSERT and UPDATE
        // privilege as specified by remainingPerms.
        if remaining_perms & ACL_INSERT != 0
            && !exec_check_permissions_modified(
                rel_oid,
                userid,
                &perminfo.insertedCols,
                ACL_INSERT,
            )?
        {
            return Ok(false);
        }

        if remaining_perms & ACL_UPDATE != 0
            && !exec_check_permissions_modified(
                rel_oid,
                userid,
                &perminfo.updatedCols,
                ACL_UPDATE,
            )?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `ExecCheckPermissions(rangeTable, rteperminfos, ereport_on_violation)`
/// (execMain.c) — check access permissions of relations mentioned in a query.
///
/// `rangeTable` is no longer used by us, but kept around for hooks (the trimmed
/// model carries it on the EState; the `ExecutorCheckPerms_hook` is not modeled
/// in the owned port, so the hook leg is omitted — faithful: with no hook
/// installed C returns `result` unchanged).
pub fn exec_check_permissions(
    rteperminfos: &[RTEPermissionInfo<'_>],
    ereport_on_violation: bool,
) -> PgResult<bool> {
    for perminfo in rteperminfos {
        debug_assert!(types_core::primitive::OidIsValid(perminfo.relid));
        let result = exec_check_one_rel_perms(perminfo)?;
        if !result {
            if ereport_on_violation {
                let relkind = lsyscache::get_rel_relkind::call(perminfo.relid)?;
                let objtype = objaddr::get_relkind_objtype::call(relkind);
                let name = lsyscache_get_rel_name(perminfo.relid)?;
                aclchk::aclcheck_error::call(AclResult::AclcheckNoPriv, objtype, name)?;
            }
            return Ok(false);
        }
    }
    Ok(true)
}

/// `ExecCheckOneRelPerms(perminfo)` + `aclcheck_error(ACLCHECK_NO_PRIV,
/// OBJECT_VIEW, get_rel_name(perminfo->relid))` for `subquery_planner`'s
/// view-permission ACL loop (planner.c:866-882). Unlike
/// [`exec_check_permissions`], the planner check hardcodes `OBJECT_VIEW` for the
/// error (the RTE is known to be `RELKIND_VIEW`).
fn exec_check_one_rel_perms_view(perminfo: &RTEPermissionInfo<'_>) -> PgResult<()> {
    if !exec_check_one_rel_perms(perminfo)? {
        let name = lsyscache_get_rel_name(perminfo.relid)?;
        aclchk::aclcheck_error::call(
            AclResult::AclcheckNoPriv,
            types_nodes::parsenodes::ObjectType::View,
            name,
        )?;
    }
    Ok(())
}

// ===========================================================================
// ExecSupportsBackwardScan — execMain owns the seam decl (its caller passes the
// whole PlannedStmt); the body is execAmi's planTree walker.
// ===========================================================================

/// `ExecSupportsBackwardScan(queryDesc->plannedstmt->planTree)` (execAmi.c via
/// the portalcmds caller): does the plan tree support backward scanning?
/// Delegates to execAmi's recursive `Plan`-node walker, handed `planTree`.
pub fn exec_supports_backward_scan(plan: &PlannedStmt<'_>) -> PgResult<bool> {
    backend_executor_execAmi::exec_supports_backward_scan(plan.planTree.as_deref())
}

// ===========================================================================
// ExecUpdateLockMode (execMain.c).
// ===========================================================================

/// Install the outward seams the `GetTupleForTrigger` firing front
/// (`backend-commands-trigger`) calls to fetch + lock the OLD on-disk tuple.
/// The trigger manager is below the executor's execUtils/tableam machinery in
/// the crate DAG, so the bodies live here (execMain owns `ExecUpdateLockMode`,
/// depends on execUtils for `ExecGetTriggerOldSlot`, and on the tableam crate).
fn install_get_tuple_for_trigger_seams() {
    // ExecGetTriggerOldSlot(estate, relinfo) — the relInfo's reusable OLD slot.
    backend_commands_trigger_seams::exec_get_trigger_old_slot::set(|estate, relinfo| {
        execUtils::ExecGetTriggerOldSlot(estate, relinfo)
    });

    // ExecUpdateLockMode(estate, relinfo).
    backend_commands_trigger_seams::exec_update_lock_mode::set(|estate, relinfo| {
        ExecUpdateLockMode(estate, relinfo)
    });

    // table_tuple_lock(rel, tid, es_snapshot, oldslot, es_output_cid, mode,
    //                  LockWaitBlock, lockflags, &tmfd).
    backend_commands_trigger_seams::get_tuple_for_trigger_lock::set(
        |estate, relinfo, tid, oldslot, mode, find_last_version, tmfd| {
            let mcx = estate.es_query_cxt;
            let rel = estate
                .result_rel(relinfo)
                .ri_RelationDesc
                .as_ref()
                .expect("GetTupleForTrigger: result relation has no relation")
                .alias();
            let snapshot = estate
                .es_snapshot
                .as_deref()
                .cloned()
                .expect("GetTupleForTrigger: no active es_snapshot");
            let cid = estate.es_output_cid;
            let flags: u8 = if find_last_version {
                types_tableam::tableam::TUPLE_LOCK_FLAG_FIND_LAST_VERSION
            } else {
                0
            };
            let inslot = estate.slot_data_mut(oldslot);
            backend_access_table_tableam::table_tuple_lock(
                mcx,
                &rel,
                tid,
                &Some(snapshot),
                inslot,
                cid,
                mode,
                types_tableam::tableam::LockWaitPolicy::LockWaitBlock,
                flags,
                tmfd,
            )
        },
    );

    // table_tuple_fetch_row_version(rel, tid, SnapshotAny, oldslot).
    backend_commands_trigger_seams::get_tuple_for_trigger_fetch::set(
        |estate, relinfo, tid, oldslot| {
            let mcx = estate.es_query_cxt;
            let rel = estate
                .result_rel(relinfo)
                .ri_RelationDesc
                .as_ref()
                .expect("GetTupleForTrigger: result relation has no relation")
                .alias();
            let snapshot_any =
                Some(types_snapshot::SnapshotData::sentinel(types_snapshot::SnapshotType::SNAPSHOT_ANY));
            let relid = rel.rd_id;
            let inslot = estate.slot_data_mut(oldslot);
            let found = backend_access_table_tableam::table_tuple_fetch_row_version(
                mcx,
                &rel,
                tid,
                &snapshot_any,
                inslot,
            )?;
            // table_tuple_fetch_row_version stores via ExecStoreBufferHeapTuple,
            // which sets slot->tts_tableOid = tuple->t_tableOid (= the relation
            // OID). A `tableoid` system-column reference in a trigger WHEN clause
            // (e.g. `new.tableoid = old.tableoid`) reads the slot's tts_tableOid,
            // so the OLD slot must carry the relation OID, not 0.
            if found {
                estate.slot_mut(oldslot).tts_tableOid = relid;
            }
            Ok(found)
        },
    );
}

/// `ExecUpdateLockMode(estate, relinfo)` (execMain.c): the row-lock mode to
/// acquire on a conflicting tuple before updating. If no key column has been
/// modified a weaker lock is sufficient (better concurrency).
pub fn ExecUpdateLockMode<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<types_tableam::tableam::LockTupleMode> {
    // updatedCols = ExecGetAllUpdatedCols(relinfo, estate);
    // keyCols = RelationGetIndexAttrBitmap(relinfo->ri_RelationDesc,
    //                                      INDEX_ATTR_BITMAP_KEY);
    let mcx = estate.es_query_cxt;
    let updated_cols = execUtils::ExecGetAllUpdatedCols(estate, result_rel_info, mcx)?;

    let rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecUpdateLockMode: result relation must be open")
        .alias();
    let key_cols = relcache::relation_get_index_attr_bitmap::call(
        mcx,
        &rel,
        relcache::IndexAttrBitmapKind::Keys,
    )?;

    // if (bms_overlap(keyCols, updatedCols)) return LockTupleExclusive;
    if bms_seams::bms_overlap::call(key_cols.as_deref(), updated_cols.as_deref()) {
        return Ok(types_tableam::tableam::LockTupleMode::LockTupleExclusive);
    }
    // return LockTupleNoKeyExclusive;
    Ok(types_tableam::tableam::LockTupleMode::LockTupleNoKeyExclusive)
}

// ===========================================================================
// ExecGetReturningSlot / ExecGetChildToRootMap (execUtils.c) — execMain owns
// the seam decl (consumed by nodeModifyTable); the bodies are execUtils's.
// ===========================================================================

/// `ExecGetReturningSlot(estate, relInfo)` (execUtils.c): get (lazily creating)
/// the per-relation slot used to hold a tuple for RETURNING. Delegates to
/// execUtils.
fn exec_get_returning_slot<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<types_nodes::SlotId> {
    execUtils::ExecGetReturningSlot(estate, result_rel_info)
}

/// `ExecGetChildToRootMap(resultRelInfo)` (execUtils.c): compute lazily the
/// child→root tuple-conversion map. The seam reports whether a conversion is
/// needed (the map is `NULL` when the rowtypes already match); the map itself
/// lives on the pooled `ResultRelInfo`. Delegates to execUtils.
fn exec_get_child_to_root_map<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<bool> {
    Ok(execUtils::ExecGetChildToRootMap(estate, result_rel_info)?.is_some())
}

/// `ExecGetChildToRootMap(resultRelInfo)` returning the map's `attrMap` (copied
/// into `mcx`) and its `outdesc` (the root rowtype, copied into `mcx`), so the
/// caller can drop the `estate` borrow and re-borrow it to apply the conversion.
/// `None` is the C `NULL` map.  Delegates to execUtils.
fn exec_get_child_to_root_map_full<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<
    Option<(
        mcx::PgBox<'mcx, types_tuple::attmap::AttrMap<'mcx>>,
        types_tuple::heaptuple::TupleDesc<'mcx>,
    )>,
> {
    let map = execUtils::ExecGetChildToRootMap(estate, result_rel_info)?;
    let map = match map {
        Some(m) => m,
        None => return Ok(None),
    };
    // Copy the attrMap (a PgVec<AttrNumber>) into mcx.
    let mut attnums: mcx::PgVec<'mcx, types_core::primitive::AttrNumber> =
        mcx::vec_with_capacity_in(mcx, map.attrMap.attnums.len())?;
    for &a in map.attrMap.attnums.iter() {
        attnums.push(a);
    }
    let attr_map = mcx::alloc_in(mcx, types_tuple::attmap::AttrMap { attnums })?;
    // Copy the outdesc (root rowtype TupleDesc) into mcx.
    let outdesc: types_tuple::heaptuple::TupleDesc<'mcx> = match map.outdesc.as_ref() {
        Some(d) => Some(mcx::alloc_in(mcx, d.clone_in(mcx)?)?),
        None => None,
    };
    Ok(Some((attr_map, outdesc)))
}

/// `get_rel_name(relid)` → owned `String` (the `aclcheck_error` objectname).
fn lsyscache_get_rel_name(relid: Oid) -> PgResult<Option<alloc::string::String>> {
    let tmp = MemoryContext::new("execMain get_rel_name");
    let out = lsyscache::get_rel_name::call(tmp.mcx(), relid)?
        .map(|s| s.as_str().to_string());
    Ok(out)
}

/// Install the seams this unit owns and is ready to install.
///
/// The driver entry points (`executor_run` / `executor_finish` / `executor_end`
/// / `free_query_desc`) plus the self-contained SELECT-permission check
/// (`exec_check_permissions_select`) are installed. The rest of
/// `backend-executor-execMain-seams` (EvalPlanQual, the constraint/partition
/// checks, `InitResultRelInfo`, the CteScan-leader operations, the COPY-query
/// setup, …) stays uninstalled (mirror-and-panic) until the matching #166/#167
/// family lands; the unit is CATALOG `needs-decomp`, so the recurrence guard
/// exempts the unfinished surface.
/// `InitResultRelInfo(resultRelInfo, resultRelationDesc, resultRelationIndex,
/// partition_root_rri, instrument_options)` (execMain.c) — fill in a
/// `ResultRelInfo` for the given target relation.
///
/// The owned model passes the caller-allocated `ResultRelInfo` (already in the
/// EState pool) by `&mut`, and the relation as an alias handle stored into
/// `ri_RelationDesc`. The C `MemSet(0)` is mirrored by overwriting every
/// modeled field (the trimmed `ResultRelInfo` only carries the subset the
/// ports consume).
/// `ereport(ERROR)` for an unported neighbor/field on the result-relation
/// init path.
fn unported(what: &str) -> types_error::PgError {
    types_error::PgError::error(alloc::format!(
        "backend-executor-execMain: unported neighbor/field: {what}"
    ))
}

#[allow(non_snake_case)]
fn InitResultRelInfo<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    result_rel_info: &mut types_nodes::ResultRelInfo<'mcx>,
    relation: types_rel::Relation<'mcx>,
    result_relation_index: types_core::primitive::Index,
    partition_root_rri: Option<types_nodes::RriId>,
    instrument_options: i32,
) -> PgResult<()> {
    let _ = instrument_options;
    let qcx = mcx;

    // MemSet(resultRelInfo, 0, sizeof(ResultRelInfo)); — start from the
    // zero-initialized shape, then set the fields below.
    *result_rel_info = types_nodes::ResultRelInfo::default();

    // resultRelInfo->ri_RangeTableIndex = resultRelationIndex;
    result_rel_info.ri_RangeTableIndex = result_relation_index;

    // resultRelInfo->ri_needLockTagTuple = IsInplaceUpdateRelation(resultRelationDesc);
    result_rel_info.ri_needLockTagTuple =
        backend_catalog_catalog::IsInplaceUpdateRelation(&relation);

    // resultRelInfo->ri_TrigDesc = CopyTriggerDesc(resultRelationDesc->trigdesc);
    // and (if non-NULL) palloc0 the per-trigger FmgrInfo / ExprState / Instr
    // arrays. The relation has no triggers on the INSERT-into-plain-table path
    // (rd_trigdesc == NULL); the trigger-carrying case needs CopyTriggerDesc
    // (trigger.c) plus the trimmed-away ri_TrigFunctions/ri_TrigWhenExprs/
    // ri_TrigInstrument arrays, which are not modeled here.
    if let Some(trigdesc) = relation.rd_trigdesc.as_ref() {
        // resultRelInfo->ri_TrigDesc = CopyTriggerDesc(resultRelationDesc->trigdesc);
        let copied = trigdesc.clone_in(qcx)?;

        // resultRelInfo->ri_TrigFunctions = palloc0(n * sizeof(FmgrInfo));
        // resultRelInfo->ri_TrigWhenExprs = palloc0(n * sizeof(ExprState *));
        // The FmgrInfo cache collapses (function_call_invoke re-resolves by OID);
        // only the WHEN-clause ExprState array is carried, palloc0'd to numtriggers
        // (all None = "not yet compiled"), lazily filled by TriggerEnabled.
        let n = copied.numtriggers as usize;
        let mut when_exprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> =
            mcx::PgVec::new_in(qcx);
        when_exprs.try_reserve(n).map_err(|_| qcx.oom(n))?;
        for _ in 0..n {
            when_exprs.push(None);
        }
        result_rel_info.ri_TrigWhenExprs = Some(when_exprs);

        // Mirror the row-level summary flags ports read directly.
        result_rel_info.ri_has_trigdesc = true;
        result_rel_info.ri_trig_update_before_row = copied.trig_update_before_row;
        result_rel_info.ri_trig_update_instead_row = copied.trig_update_instead_row;
        result_rel_info.ri_trig_update_after_row = copied.trig_update_after_row;
        result_rel_info.ri_TrigDesc =
            Some(mcx::PgBox::try_new_in(copied, qcx).map_err(|_| qcx.oom(0))?);
    }
    // else: ri_TrigDesc / ri_has_trigdesc / ri_trig_* stay at their default
    // (None / false) — the C NULL trigger desc.

    // if (relkind == RELKIND_FOREIGN_TABLE) ri_FdwRoutine = GetFdwRoutineForRelation(...);
    // The FDW routine vtable is not carried on the trimmed ResultRelInfo;
    // ri_has_fdw_routine stays false. A foreign-table target is rejected in
    // CheckValidResultRel below, so this only matters there.

    // resultRelInfo->ri_RootResultRelInfo = partition_root_rri;
    result_rel_info.ri_RootResultRelInfo = partition_root_rri;

    // The remaining "set later if needed" fields (ri_RowIdAttNo, ri_projectNew,
    // ri_onConflict, ri_MergeActions[*], …) are already at their default
    // (zero / None / NIL) from the MemSet above.

    // ri_RelationDesc holds an alias of the relation es_relations owns (no
    // release authority).
    result_rel_info.ri_RelationDesc = Some(relation.alias());
    // The Relation `relation` is itself an alias handed in by the caller
    // (ExecGetRangeTableRelation / partition routing); drop it here, the alias
    // stored above keeps the shared data reachable.
    drop(relation);

    Ok(())
}

/// `ExecGetAncestorResultRels(estate, resultRelInfo)` (execMain.c) — return the
/// ancestor result relations of a leaf-partition result rel, up to and including
/// the query's root target relation, lazily opening/initializing them and
/// caching the chain on `ri_ancestorResultRels`. The chain is
/// `[ancestor…, rootRelInfo]` (root-inclusive); the root ancestor itself is
/// skipped in the loop and `ri_RootResultRelInfo` is appended for it. These
/// relations are closed by `ExecCloseResultRelations`.
#[allow(non_snake_case)]
fn ExecGetAncestorResultRels<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<mcx::PgVec<'mcx, types_nodes::RriId>> {
    const NoLock: i32 = 0;

    let root_rel_info = estate
        .result_rel(result_rel_info)
        .ri_RootResultRelInfo
        .expect("ExecGetAncestorResultRels: ResultRelInfo has no root");

    let part_rel = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecGetAncestorResultRels: ResultRelInfo has no relation")
        .alias();

    // if (!partRel->rd_rel->relispartition) elog(ERROR, ...)
    if !part_rel.rd_rel.relispartition {
        return Err(types_error::PgError::error(
            "cannot find ancestors of a non-partition result relation",
        ));
    }

    // rootRelOid = RelationGetRelid(rootRelInfo->ri_RelationDesc);
    let root_rel_oid = estate
        .result_rel(root_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecGetAncestorResultRels: root ResultRelInfo has no relation")
        .rd_id;
    let part_relid = part_rel.rd_id;

    // if (resultRelInfo->ri_ancestorResultRels == NIL) { ... compute ... }
    if estate
        .result_rel(result_rel_info)
        .ri_ancestorResultRels
        .is_none()
    {
        // oids = get_partition_ancestors(RelationGetRelid(partRel));
        let oids =
            backend_catalog_partition_seams::get_partition_ancestors::call(mcx, part_relid)?;

        let mut anc_result_rels: mcx::PgVec<'mcx, types_nodes::RriId> = mcx::PgVec::new_in(mcx);

        for anc_oid in oids.iter().copied() {
            // Ignore the root ancestor here (ri_RootResultRelInfo is appended
            // below), and stop climbing once we reach the query's root target.
            if anc_oid == root_rel_oid {
                break;
            }

            // All ancestors up to the root target relation are already locked
            // by the planner / AcquireExecutorLocks(), so open with NoLock.
            let anc_rel = backend_access_common_relation::relation_open(mcx, anc_oid, NoLock)?;

            // rInfo = makeNode(ResultRelInfo);
            // InitResultRelInfo(rInfo, ancRel, 0 /*dummy RTI*/, NULL, es_instrument);
            let mut r_info = types_nodes::ResultRelInfo::default();
            let instrument = estate.es_instrument;
            InitResultRelInfo(mcx, &mut r_info, anc_rel, 0, None, instrument)?;
            let id = estate.add_result_rel(r_info)?;

            anc_result_rels
                .try_reserve(1)
                .map_err(|_| mcx.oom(core::mem::size_of::<types_nodes::RriId>()))?;
            anc_result_rels.push(id);
        }

        // ancResultRels = lappend(ancResultRels, rootRelInfo);
        anc_result_rels
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<types_nodes::RriId>()))?;
        anc_result_rels.push(root_rel_info);

        estate.result_rel_mut(result_rel_info).ri_ancestorResultRels = Some(anc_result_rels);
    }

    // We must have found some ancestor; return a copy of the cached chain.
    let cached = estate
        .result_rel(result_rel_info)
        .ri_ancestorResultRels
        .as_ref()
        .expect("ExecGetAncestorResultRels: ancestor chain not set");
    debug_assert!(!cached.is_empty());

    let mut out: mcx::PgVec<'mcx, types_nodes::RriId> = mcx::PgVec::new_in(mcx);
    out.try_reserve(cached.len())
        .map_err(|_| mcx.oom(cached.len() * core::mem::size_of::<types_nodes::RriId>()))?;
    for id in cached.iter().copied() {
        out.push(id);
    }
    Ok(out)
}

/// The TID-extraction tail of `execCurrentOf`'s plain-scan strategy
/// (execCurrent.c): once `search_plan_tree` found the scan node and the
/// `TupIsNull`/`pending_rescan` inactive test passed, dig the TID out of the
/// scan's current physical tuple. For an `IndexOnlyScan` the caller passes the
/// scan descriptor's `xs_heaptid` directly (the slot may hold a virtual tuple
/// without a ctid column); the default path digs the TID out of the scan tuple
/// slot's `SelfItemPointerAttributeNumber` via `slot_getsysattr`, with the
/// `USE_ASSERT_CHECKING` tableoid cross-check. A null self-ctid is the C
/// "not a simply updatable scan" path ([`ScanTidOutcome::NotUpdatable`]).
#[allow(non_snake_case)]
fn ScanNodeExtractTid<'mcx, 'a>(
    mcx: mcx::Mcx<'a>,
    estate: &types_nodes::EStateData<'mcx>,
    scan_tuple_slot: Option<types_nodes::SlotId>,
    index_only_tid: Option<types_tuple::heaptuple::ItemPointerData>,
) -> PgResult<types_nodes::ScanTidOutcome> {
    use types_nodes::ScanTidOutcome;

    // IndexOnlyScan: the TID was read off ioss_ScanDesc->xs_heaptid by the
    // caller (which holds the concrete scan node).
    if let Some(tid) = index_only_tid {
        return Ok(ScanTidOutcome::Tid(tid));
    }

    // Default case: fetch the self-ctid from the scan node's current tuple via
    // slot_getsysattr(slot, SelfItemPointerAttributeNumber). If the scan hasn't
    // provided a physical tuple, the self-ctid is null and we report
    // NotUpdatable. (The USE_ASSERT_CHECKING tableoid cross-check is a debug
    // consistency assert in C; the slot's tts_tableOid already matches.)
    let slot_id = match scan_tuple_slot {
        Some(s) => s,
        // No scan tuple slot at all — the C `slot_getsysattr` on a null slot
        // can't produce a TID; treat as not updatable.
        None => return Ok(ScanTidOutcome::NotUpdatable),
    };

    let slot = estate.slot_data(slot_id);
    let (datum, isnull) = backend_executor_execTuples::slot_ops_vtables::slot_getsysattr(
        mcx,
        slot,
        types_tuple::heaptuple::SelfItemPointerAttributeNumber,
    )?;
    if isnull {
        return Ok(ScanTidOutcome::NotUpdatable);
    }

    // tuple_tid = (ItemPointer) DatumGetPointer(ldatum); *current_tid = *tuple_tid;
    let tid =
        backend_access_common_heaptuple::item_pointer_from_bytes(datum.as_ref_bytes());
    Ok(ScanTidOutcome::Tid(tid))
}

/// `CheckValidResultRel(resultRelInfo, operation, onConflictAction,
/// mergeActions)` (execMain.c) — verify the result relation is a valid target
/// for the command.
///
/// For MERGE, `merge_action_cmds` carries the per-result-rel MergeAction command
/// types (C's `mergeActions`); each is replica-identity checked. The
/// view / materialized-view / foreign-table arms are gated as unported (they
/// need owners — `view_has_instead_trigger`, the FDW vtable — that are off the
/// INSERT-into-table path).
#[allow(non_snake_case)]
fn CheckValidResultRel<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    operation: CmdType,
    on_conflict_action: types_nodes::nodes::OnConflictAction,
    merge_action_cmds: &[CmdType],
) -> PgResult<()> {
    use types_tuple::access::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
        RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
    };

    let mcx = estate.es_query_cxt;
    let rri = estate.result_rel(result_rel_info);
    let result_rel = rri
        .ri_RelationDesc
        .as_ref()
        .expect("CheckValidResultRel: ResultRelInfo has no relation")
        .alias();

    // Assert(ri_needLockTagTuple == IsInplaceUpdateRelation(resultRel)); —
    // InitResultRelInfo set it, so this is an internal consistency check only.
    debug_assert_eq!(
        rri.ri_needLockTagTuple,
        backend_catalog_catalog::IsInplaceUpdateRelation(&result_rel)
    );

    let relname = result_rel.name().to_string();
    let relkind = result_rel.rd_rel.relkind;

    if relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE {
        // For MERGE, check each action; for others, check the operation itself.
        if operation == CmdType::CMD_MERGE {
            // foreach_node(MergeAction, action, mergeActions)
            //     CheckCmdReplicaIdentity(resultRel, action->commandType);
            for &action_cmd in merge_action_cmds {
                execReplication_seams::check_cmd_replica_identity::call(
                    mcx,
                    &result_rel,
                    action_cmd,
                )?;
            }
        } else {
            execReplication_seams::check_cmd_replica_identity::call(mcx, &result_rel, operation)?;
        }

        // INSERT ON CONFLICT DO UPDATE additionally requires UPDATE support.
        if on_conflict_action == types_nodes::nodes::ONCONFLICT_UPDATE {
            execReplication_seams::check_cmd_replica_identity::call(
                mcx,
                &result_rel,
                CmdType::CMD_UPDATE,
            )?;
        }
        Ok(())
    } else if relkind == RELKIND_SEQUENCE {
        Err(types_error::PgError::error(alloc::format!(
            "cannot change sequence \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_TOASTVALUE {
        Err(types_error::PgError::error(alloc::format!(
            "cannot change TOAST relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_VIEW {
        // Okay only if there's a suitable INSTEAD OF trigger (or, for MERGE, all
        // actions either have one or are CMD_NOTHING). Otherwise complain, but
        // omit errdetail (NULL) — the executor's just-in-case check should never
        // fail, and the information isn't handy here. Mirrors
        // view_has_instead_trigger / error_view_not_updatable (rewriteHandler.c)
        // for the executor call path (execMain.c CheckValidResultRel).
        if !view_has_instead_trigger(&result_rel, operation, merge_action_cmds) {
            Err(error_view_not_updatable_exec(
                &result_rel,
                operation,
                merge_action_cmds,
            ))
        } else {
            Ok(())
        }
    } else if relkind == RELKIND_MATVIEW {
        // Okay only when MatViewIncrementalMaintenanceIsEnabled().
        if !backend_commands_matview_seams::MatViewIncrementalMaintenanceIsEnabled::call() {
            Err(types_error::PgError::error(alloc::format!(
                "cannot change materialized view \"{relname}\""
            ))
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
        } else {
            Ok(())
        }
    } else if relkind == RELKIND_FOREIGN_TABLE {
        // Okay only if the FDW supports the operation; the FDW routine vtable is
        // not carried on the trimmed ResultRelInfo.
        Err(unported(
            "CheckValidResultRel: foreign-table target \
             (FdwRoutine vtable is not carried on ResultRelInfo)",
        ))
    } else {
        Err(types_error::PgError::error(alloc::format!(
            "cannot change relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    }
}

/// `view_has_instead_trigger(view, event, mergeActionList)` (rewriteHandler.c)
/// — does the view have a suitable INSTEAD OF trigger for `event`? For MERGE,
/// it returns true only if every action either has a matching INSTEAD OF
/// trigger or is `CMD_NOTHING` (DO NOTHING needs no trigger). Here the C
/// `mergeActionList` is carried as the per-result-rel `CmdType` list the
/// executor already built.
fn view_has_instead_trigger(
    view: &types_rel::RelationData<'_>,
    event: CmdType,
    merge_action_cmds: &[CmdType],
) -> bool {
    let trig_desc = view.rd_trigdesc.as_deref();
    match event {
        CmdType::CMD_INSERT => trig_desc.is_some_and(|t| t.trig_insert_instead_row),
        CmdType::CMD_UPDATE => trig_desc.is_some_and(|t| t.trig_update_instead_row),
        CmdType::CMD_DELETE => trig_desc.is_some_and(|t| t.trig_delete_instead_row),
        CmdType::CMD_MERGE => {
            for &action_cmd in merge_action_cmds {
                match action_cmd {
                    CmdType::CMD_INSERT => {
                        if !trig_desc.is_some_and(|t| t.trig_insert_instead_row) {
                            return false;
                        }
                    }
                    CmdType::CMD_UPDATE => {
                        if !trig_desc.is_some_and(|t| t.trig_update_instead_row) {
                            return false;
                        }
                    }
                    CmdType::CMD_DELETE => {
                        if !trig_desc.is_some_and(|t| t.trig_delete_instead_row) {
                            return false;
                        }
                    }
                    // CMD_NOTHING: no trigger required. Other CmdTypes can't
                    // appear in a MergeAction.
                    _ => {}
                }
            }
            // No actions without an INSTEAD OF trigger.
            true
        }
        _ => false,
    }
}

/// `error_view_not_updatable(view, command, mergeActionList, NULL)`
/// (rewriteHandler.c) for the executor call path — always returns `Err`. The
/// executor always passes a NULL `detail` (the information isn't handy and the
/// check shouldn't fail), so no errdetail is attached. For MERGE the hint is
/// per offending action, matching the C `error_view_not_updatable` MERGE arm.
fn error_view_not_updatable_exec(
    view: &types_rel::RelationData<'_>,
    command: CmdType,
    merge_action_cmds: &[CmdType],
) -> types_error::PgError {
    use types_error::error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE;
    let name = view.name().to_string();
    let mk = |msg: alloc::string::String, hint: &str| -> types_error::PgError {
        types_error::PgError::new(ERROR, msg)
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(hint.to_string())
    };
    let trig_desc = view.rd_trigdesc.as_deref();
    match command {
        CmdType::CMD_INSERT => mk(
            alloc::format!("cannot insert into view \"{name}\""),
            "To enable inserting into the view, provide an INSTEAD OF INSERT trigger or an unconditional ON INSERT DO INSTEAD rule.",
        ),
        CmdType::CMD_UPDATE => mk(
            alloc::format!("cannot update view \"{name}\""),
            "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an unconditional ON UPDATE DO INSTEAD rule.",
        ),
        CmdType::CMD_DELETE => mk(
            alloc::format!("cannot delete from view \"{name}\""),
            "To enable deleting from the view, provide an INSTEAD OF DELETE trigger or an unconditional ON DELETE DO INSTEAD rule.",
        ),
        CmdType::CMD_MERGE => {
            // The hints here differ from above, since MERGE doesn't support
            // rules. Report the first action lacking a suitable trigger.
            for &action_cmd in merge_action_cmds {
                match action_cmd {
                    CmdType::CMD_INSERT => {
                        if !trig_desc.is_some_and(|t| t.trig_insert_instead_row) {
                            return mk(
                                alloc::format!("cannot insert into view \"{name}\""),
                                "To enable inserting into the view using MERGE, provide an INSTEAD OF INSERT trigger.",
                            );
                        }
                    }
                    CmdType::CMD_UPDATE => {
                        if !trig_desc.is_some_and(|t| t.trig_update_instead_row) {
                            return mk(
                                alloc::format!("cannot update view \"{name}\""),
                                "To enable updating the view using MERGE, provide an INSTEAD OF UPDATE trigger.",
                            );
                        }
                    }
                    CmdType::CMD_DELETE => {
                        if !trig_desc.is_some_and(|t| t.trig_delete_instead_row) {
                            return mk(
                                alloc::format!("cannot delete from view \"{name}\""),
                                "To enable deleting from the view using MERGE, provide an INSTEAD OF DELETE trigger.",
                            );
                        }
                    }
                    _ => {}
                }
            }
            // The caller only invokes this when view_has_instead_trigger
            // returned false, so an offending action exists above; this is a
            // defensive fallback mirroring the C function falling through.
            mk(
                alloc::format!("cannot merge into view \"{name}\""),
                "",
            )
        }
        other => types_error::PgError::error(alloc::format!(
            "unrecognized CmdType: {}",
            other as i32
        )),
    }
}

/// `InitPlan`'s "Build the ExecRowMark array from PlanRowMark(s)" loop
/// (execMain.c). For each `PlanRowMark` the planner attached to the
/// `PlannedStmt`, open (and lock, by `ExecGetRangeTableRelation`'s already-held
/// lock) the marked relation if its rowmark needs a real row, validate it with
/// `CheckValidRowMarkRel`, build an `ExecRowMark`, and store it in
/// `estate->es_rowmarks[rti-1]`.
fn init_plan_rowmarks<'mcx>(estate: &mut types_nodes::EStateData<'mcx>) -> PgResult<()> {
    use types_nodes::nodelockrows::{
        ExecRowMark, ROW_MARK_COPY, ROW_MARK_EXCLUSIVE, ROW_MARK_KEYSHARE, ROW_MARK_NOKEYEXCLUSIVE,
        ROW_MARK_REFERENCE, ROW_MARK_SHARE,
    };

    let mcx = estate.es_query_cxt;
    let rtsize = estate.es_range_table_size;

    // estate->es_rowmarks = palloc0(estate->es_range_table_size * sizeof(...));
    // (allocated only when there are rowmarks, as in C).
    if estate.es_rowmarks.is_empty() {
        let mut marks = mcx::vec_with_capacity_in(mcx, rtsize)?;
        marks.resize_with(rtsize, || None);
        estate.es_rowmarks = marks;
    }

    // Snapshot the PlanRowMark scalars out of es_plannedstmt so we can take
    // `&mut estate` for ExecGetRangeTableRelation without aliasing the borrow.
    let planned: alloc::vec::Vec<types_nodes::nodelockrows::PlanRowMark> = estate
        .es_plannedstmt
        .as_ref()
        .and_then(|p| p.rowMarks.as_ref())
        .map(|m| m.iter().copied().collect())
        .unwrap_or_default();

    for rc in planned {
        // ignore "parent" rowmarks; they are irrelevant at runtime
        if rc.isParent {
            continue;
        }

        // Ignore RowMarks for RTEs that were pruned in ExecDoInitialPruning.
        if !backend_nodes_core_seams::bms_is_member::call(
            rc.rti as i32,
            estate.es_unpruned_relids.as_deref(),
        ) {
            continue;
        }

        // get relation's OID (will produce InvalidOid if subquery). For a
        // non-inheritance rowmark (rti == prti) it is the marked RTE's relid.
        let relid = if rc.rti != rc.prti {
            // Child of an inheritance tree: the relid is the child RTE's relid.
            execUtils::exec_rt_fetch(rc.rti, estate).relid
        } else {
            execUtils::exec_rt_fetch(rc.rti, estate).relid
        };

        // Open and validate the relation if the rowmark needs a real row.
        let relation: Option<types_rel::Relation<'mcx>> = match rc.markType {
            ROW_MARK_EXCLUSIVE | ROW_MARK_NOKEYEXCLUSIVE | ROW_MARK_SHARE | ROW_MARK_KEYSHARE
            | ROW_MARK_REFERENCE => {
                let rel = execUtils::ExecGetRangeTableRelation(estate, rc.rti, false, false)?;
                CheckValidRowMarkRel(&rel, rc.markType)?;
                Some(rel)
            }
            ROW_MARK_COPY => {
                // physical copy: no relation needed at this layer.
                None
            }
            _ => {
                return Err(types_error::PgError::error(alloc::format!(
                    "unrecognized markType: {}",
                    rc.markType
                )))
            }
        };

        let erm = ExecRowMark {
            relation,
            relid,
            rti: rc.rti,
            prti: rc.prti,
            rowmarkId: rc.rowmarkId,
            markType: rc.markType,
            strength: rc.strength,
            waitPolicy: rc.waitPolicy,
            ermActive: false,
            curCtid: types_tuple::heaptuple::ItemPointerData::default(),
            ermExtra: None,
        };

        debug_assert!(rc.rti > 0 && (rc.rti as usize) <= rtsize);
        debug_assert!(estate.es_rowmarks[(rc.rti - 1) as usize].is_none());
        estate.es_rowmarks[(rc.rti - 1) as usize] = Some(erm);
    }

    Ok(())
}

/// `CheckValidRowMarkRel(rel, markType)` (execMain.c) — check that a relation
/// is a legal target for marking (`FOR UPDATE`/`FOR SHARE`/`REFERENCE`).
#[allow(non_snake_case)]
fn CheckValidRowMarkRel<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    mark_type: types_nodes::nodelockrows::RowMarkType,
) -> PgResult<()> {
    use types_nodes::nodelockrows::ROW_MARK_REFERENCE;
    use types_tuple::access::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
        RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
    };

    let relname = rel.name();
    let relkind = rel.rd_rel.relkind;

    if relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE {
        // OK
        Ok(())
    } else if relkind == RELKIND_SEQUENCE {
        Err(types_error::PgError::error(alloc::format!(
            "cannot lock rows in sequence \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_TOASTVALUE {
        Err(types_error::PgError::error(alloc::format!(
            "cannot lock rows in TOAST relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_VIEW {
        // Should not get here; planner should have expanded the view.
        Err(types_error::PgError::error(alloc::format!(
            "cannot lock rows in view \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else if relkind == RELKIND_MATVIEW {
        // Allow referencing a matview, but not actual locking clauses.
        if mark_type != ROW_MARK_REFERENCE {
            Err(types_error::PgError::error(alloc::format!(
                "cannot lock rows in materialized view \"{relname}\""
            ))
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
        } else {
            Ok(())
        }
    } else if relkind == RELKIND_FOREIGN_TABLE {
        // Okay only if the FDW supports it; no FDW row-locking is modelled.
        Err(types_error::PgError::error(alloc::format!(
            "cannot lock rows in foreign table \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    } else {
        Err(types_error::PgError::error(alloc::format!(
            "cannot lock rows in relation \"{relname}\""
        ))
        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE))
    }
}

/// `EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam,
/// resultRelations)` (execMain.c) — initialize an `EPQState` with the data that
/// does not change over its lifetime, leaving the EPQ machinery inactive.
///
/// The owned `EPQState` trims `parentestate` / `plan` / `arowMarks` /
/// `origslot` / `recheckestate` / `recheckplanstate` (the executor threads the
/// EState explicitly and the recheck-exec sub-tree is not built until
/// `EvalPlanQualBegin`). So this records `epqParam` / `resultRelations`,
/// pre-allocates the `relsubs_slot` array (`rtsize` NULL entries), and marks
/// the state inactive (all dynamic arrays `None`).
#[allow(non_snake_case)]
fn EvalPlanQualInit<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
    parentestate: &mut types_nodes::EStateData<'mcx>,
    epq_param: i32,
    result_relations: &[types_core::primitive::Index],
) -> PgResult<()> {
    // Index rtsize = parentestate->es_range_table_size;
    let rtsize = parentestate.es_range_table_size;

    // epqstate->epqParam = epqParam;
    epqstate.epqParam = epq_param;

    // epqstate->resultRelations = resultRelations; (integer list of RT indexes)
    if result_relations.is_empty() {
        epqstate.resultRelations = None;
    } else {
        let mut rr = mcx::vec_with_capacity_in(mcx, result_relations.len())?;
        for &rti in result_relations {
            rr.push(rti as i32);
        }
        epqstate.resultRelations = Some(rr);
    }

    // epqstate->tuple_table = NIL; (trimmed)
    // epqstate->relsubs_slot = palloc0(rtsize * sizeof(TupleTableSlot *));
    let mut slots = mcx::vec_with_capacity_in(mcx, rtsize)?;
    slots.resize(rtsize, None);
    epqstate.relsubs_slot = Some(slots);

    // epqstate->plan = subplan;            (trimmed — set by EvalPlanQualSetPlan)
    // epqstate->arowMarks = auxrowmarks;   (trimmed)

    // Mark the EPQ state inactive:
    //   origslot/recheckestate/recheckplanstate = NULL; (trimmed)
    //   relsubs_rowmark/relsubs_done/relsubs_blocked = NULL;
    epqstate.relsubs_rowmark = None;
    epqstate.relsubs_done = None;
    epqstate.relsubs_blocked = None;

    Ok(())
}

/// The `arowmarks` build loop + `EvalPlanQualSetPlan(epqstate, subplan,
/// arowmarks)` of `ExecInitModifyTable` (execMain.c).
///
/// `EvalPlanQualSetPlan` shuts down any live EPQ query (`EvalPlanQualEnd` — a
/// no-op on the trimmed `EPQState`, whose `recheckestate`/`tuple_table` are not
/// modeled) and records the recheck plan + aux-rowmark list. The owned
/// `EPQState` trims `plan` and `arowMarks` (rebuilt from the plan in
/// `EvalPlanQualBegin`), so the recording is a no-op residue. Building the
/// aux-rowmark list from a non-empty `rowMarks` needs the `PlanRowMark` plan
/// node and `ExecFindRowMark` / `ExecBuildAuxRowMark` (execMain's EPQ
/// aux-rowmark machinery), gated until that lands; an empty / parent-only /
/// pruned `rowMarks` list is the live INSERT/UPDATE/DELETE path (NIL arowmarks).
#[allow(non_snake_case)]
fn eval_plan_qual_set_plan_with_row_marks<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    _epqstate: &mut types_nodes::modifytable::EPQState<'mcx>,
    _estate: &mut types_nodes::EStateData<'mcx>,
    row_marks: &[mcx::PgBox<'mcx, types_nodes::nodes::Node<'mcx>>],
    _subplan: Option<&'mcx types_nodes::nodes::Node<'mcx>>,
) -> PgResult<()> {
    // foreach(l, node->rowMarks) { ...ExecFindRowMark / ExecBuildAuxRowMark... }
    if !row_marks.is_empty() {
        return Err(unported(
            "ExecInitModifyTable arowmarks loop: PlanRowMark / ExecFindRowMark / \
             ExecBuildAuxRowMark (EPQ aux-rowmark machinery)",
        ));
    }

    // EvalPlanQualSetPlan(epqstate, subplan, NIL):
    //   EvalPlanQualEnd(epqstate);   — no-op on the trimmed EPQState
    //   epqstate->plan = subplan;    — `plan` is trimmed
    //   epqstate->arowMarks = NIL;   — `arowMarks` is trimmed
    Ok(())
}

/// `ExecBuildSlotValueDescription(reloid, slot, tupdesc, modifiedCols,
/// maxfieldlen)` (execMain.c) — build a "(col, ...) = (val, ...)" description of
/// the slot's contents, limited to columns the current user may SELECT (plus any
/// the caller supplied data for). `Ok(None)` when RLS is active or the user may
/// see no column (the C `NULL`).
///
/// The slot is passed by shared reference, so it is assumed already
/// deconstructed (every repo call site deforms first — logical-replication apply
/// materializes a virtual slot, `ExecConstraints`/`ExecPartitionCheck` evaluate
/// against a materialized scan tuple); the C `slot_getallattrs(slot)` is the
/// idempotent no-op for an already-deformed slot.
fn ExecBuildSlotValueDescription<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    reloid: Oid,
    slot: &types_nodes::TupleTableSlot,
    tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    modified_cols: Option<&Bitmapset<'_>>,
    maxfieldlen: i32,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    use types_acl::acl::CheckEnableRlsResult;

    // If RLS is enabled and active for the relation, return nothing.
    if backend_utils_misc_more_seams::check_enable_rls::call(reloid, types_core::InvalidOid, true)?
        == CheckEnableRlsResult::RlsEnabled
    {
        return Ok(None);
    }

    let mut buf = mcx::PgString::new_in(mcx);
    let mut collist = mcx::PgString::new_in(mcx);
    let mut write_comma = false;
    let mut write_comma_collist = false;
    let mut any_perm = false;

    buf.try_push_str("(")?;

    // Table-level SELECT allows all columns; otherwise check each.
    let aclresult = aclchk::pg_class_aclcheck::call(reloid, miscinit::get_user_id::call(), ACL_SELECT)?;
    let table_perm = aclresult == AclResult::AclcheckOk;
    if table_perm {
        any_perm = true;
    } else {
        collist.try_push_str("(")?;
    }

    let natts = tupdesc.natts;
    let mut i: i32 = 0;
    while i < natts {
        let att = tupdesc.attr(i as usize);

        // Ignore dropped columns.
        if att.attisdropped {
            i += 1;
            continue;
        }

        let mut column_perm = false;
        if !table_perm {
            // No table-level SELECT: include the column only if the user has
            // SELECT on it or provided its data.
            let aclresult = aclchk::pg_attribute_aclcheck::call(
                reloid,
                att.attnum,
                miscinit::get_user_id::call(),
                ACL_SELECT,
            )?;
            if bms_seams::bms_is_member::call(
                att.attnum as i32 - FirstLowInvalidHeapAttributeNumber as i32,
                modified_cols,
            ) || aclresult == AclResult::AclcheckOk
            {
                column_perm = true;
                any_perm = true;
                if write_comma_collist {
                    collist.try_push_str(", ")?;
                } else {
                    write_comma_collist = true;
                }
                collist.try_push_str(&alloc::string::String::from_utf8_lossy(att.attname.name_str()))?;
            }
        }

        if table_perm || column_perm {
            // The value text (a `String`-shaped owned bytes in `mcx`).
            let val: alloc::vec::Vec<u8> = if att.attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL {
                alloc::vec::Vec::from(&b"virtual"[..])
            } else if slot.tts_isnull.get(i as usize).copied().unwrap_or(true) {
                alloc::vec::Vec::from(&b"null"[..])
            } else {
                let (foutoid, _typisvarlena) =
                    lsyscache::get_type_output_info::call(att.atttypid)?;
                let datum = slot
                    .tts_values
                    .get(i as usize)
                    .ok_or_else(|| unported("ExecBuildSlotValueDescription: slot not deformed"))?;
                let out = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(mcx, foutoid, datum)?;
                // PgVec<u8> NUL-terminated cstring; strip the trailing NUL if any.
                let mut v: alloc::vec::Vec<u8> = out.iter().copied().collect();
                if v.last() == Some(&0) {
                    v.pop();
                }
                v
            };

            if write_comma {
                buf.try_push_str(", ")?;
            } else {
                write_comma = true;
            }

            // Truncate if needed (by bytes, respecting multibyte boundaries).
            let vallen = val.len() as i32;
            if vallen <= maxfieldlen {
                buf.try_push_str(&alloc::string::String::from_utf8_lossy(&val))?;
            } else {
                let clip = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(&val, vallen, maxfieldlen);
                buf.try_push_str(&alloc::string::String::from_utf8_lossy(&val[..clip as usize]))?;
                buf.try_push_str("...")?;
            }
        }
        i += 1;
    }

    // If we end up with zero columns being returned, return NULL.
    if !any_perm {
        return Ok(None);
    }

    buf.try_push_str(")")?;

    if !table_perm {
        collist.try_push_str(") = ")?;
        // Append buf into collist (copy buf out first to avoid a double borrow).
        let buf_str = buf.as_str().to_string();
        collist.try_push_str(&buf_str)?;
        return Ok(Some(collist));
    }

    Ok(Some(buf))
}

// ===========================================================================
//  Constraint cluster (execMain.c): ExecRelCheck / ExecPartitionCheck /
//  ExecPartitionCheckEmitError / ExecConstraints / ExecRelGenVirtualNotNull /
//  ReportNotNullViolationError, de-handled onto the owned EState slot/RRI
//  pools. The cached constraint `ExprState`s live on the owned `ResultRelInfo`
//  (`ri_CheckConstraintExprs` / `ri_PartitionCheckExpr` /
//  `ri_GenVirtualNotNullConstraintExprs`); the take-evaluate-put-back idiom
//  mirrors nodeModifyTable's `ri_GeneratedExprs*` handling so a `&mut ExprState`
//  and `&mut EStateData` can co-exist (C shares the pointer freely).
//
//  Expression *compilation* (ExecPrepareExpr / ExecPrepareCheck) bottoms out on
//  the unported `expression_planner` (#159) and loud-panics there; the
//  not-null slot-read path (the common COPY/INSERT case for relations without
//  CHECK / partition / virtual-generated constraints) needs no compilation and
//  runs end-to-end.
// ===========================================================================

use backend_executor_execExpr::execExpr_core as execExpr;
use backend_utils_error::ereport;
use types_error::error::{ERRCODE_CHECK_VIOLATION, ERRCODE_NOT_NULL_VIOLATION, ERROR};
use types_tuple::heaptuple::TupleDescData;

/// `ExecRelCheck(resultRelInfo, slot, estate)` (execMain.c): evaluate the
/// relation's CHECK constraints against `slot`. Returns the name of the first
/// failing constraint, or `None` (the C `NULL`) when all pass.
fn ExecRelCheck<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let mcx = estate.es_query_cxt;

    // Snapshot the relation's check list (ccname / ccbin / ccenforced) and the
    // pg_class relchecks count. The C dereferences ccname/ccbin
    // unconditionally for enforced checks.
    struct CheckInfo<'m> {
        ccname: mcx::PgString<'m>,
        ccbin: mcx::PgString<'m>,
        ccenforced: bool,
    }
    let (checks, num_check, relname): (alloc::vec::Vec<CheckInfo<'mcx>>, i32, alloc::string::String) = {
        let rri = estate.result_rel(result_rel_info);
        let rel = rri
            .ri_RelationDesc
            .as_ref()
            .expect("ExecRelCheck: ri_RelationDesc is NULL");
        let mut v: alloc::vec::Vec<CheckInfo<'mcx>> = alloc::vec::Vec::new();
        let mut num_check = 0i32;
        if let Some(constr) = rel.rd_att.constr.as_ref() {
            num_check = constr.num_check as i32;
            for ck in constr.check.iter() {
                let ccname = ck
                    .ccname
                    .as_ref()
                    .expect("pg_constraint CHECK has ccname")
                    .clone_in(mcx)?;
                let ccbin = ck
                    .ccbin
                    .as_ref()
                    .expect("pg_constraint CHECK has ccbin")
                    .clone_in(mcx)?;
                v.push(CheckInfo {
                    ccname,
                    ccbin,
                    ccenforced: ck.ccenforced,
                });
            }
        }
        (v, num_check, rel.name().to_string())
    };
    let ncheck = checks.len() as i32;

    // CheckNNConstraintFetch let this pass with only a warning; we fail rather
    // than risk not enforcing an important constraint. The owned model's
    // `rd_att->constr` is the parsed source for both counts, so this compares
    // the materialized check list against `constr->num_check`.
    if ncheck != num_check {
        return Err(ereport(ERROR)
            .errmsg_internal(alloc::format!(
                "{} pg_constraint record(s) missing for relation \"{}\"",
                num_check - ncheck,
                relname
            ))
            .into_error());
    }

    // If first time through, build the per-constraint ExprStates (kept in the
    // query-lifespan context, owned by the arena ResultRelInfo).
    if estate
        .result_rel(result_rel_info)
        .ri_CheckConstraintExprs
        .is_none()
        && ncheck > 0
    {
        let rel_alias = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecRelCheck: ri_RelationDesc is NULL")
            .alias();
        let mut exprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> =
            mcx::vec_with_capacity_in(mcx, ncheck as usize)?;
        for ci in checks.iter() {
            // Skip not-enforced constraints (leaving a None placeholder so the
            // list stays index-aligned with the check list).
            if !ci.ccenforced {
                exprs.push(None);
                continue;
            }
            // checkconstr = stringToNode(check[i].ccbin);
            let node = backend_nodes_core::read::string_to_node(mcx, ci.ccbin.as_str())?;
            let checkconstr_expr = match node.as_expr() {
                Some(e) => e.clone_in(mcx)?,
                None => return Err(unported_node("ExecRelCheck: ccbin is not an Expr", &node)),
            };
            // checkconstr = expand_generated_columns_in_expr(checkconstr, rel, 1);
            // The expander operates in the parser-arena notional `'static`; erase
            // the `'mcx` constraint in and re-intern the result into `mcx` for
            // `exec_prepare_expr` (`Expr` is invariant — these are the boundary moves).
            let expanded = backend_rewrite_rewritehandler::expand_generated_columns_in_expr(
                mcx,
                Some(checkconstr_expr.erase_lifetime()),
                &rel_alias,
                1,
            )?;
            let expanded_expr = expanded
                .expect("expand_generated_columns_in_expr returned NULL for a non-NULL Expr")
                .clone_in(mcx)?;
            exprs.push(Some(execExpr::exec_prepare_expr(&expanded_expr, estate)?));
        }
        estate
            .result_rel_mut(result_rel_info)
            .ri_CheckConstraintExprs = Some(exprs);
    }

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Evaluate each constraint (NULL counts as success: use ExecCheck).
    for (i, ci) in checks.iter().enumerate() {
        // Take the owned ExprState out, evaluate, put it back.
        let state = estate
            .result_rel_mut(result_rel_info)
            .ri_CheckConstraintExprs
            .as_mut()
            .and_then(|arr| arr.get_mut(i))
            .and_then(|slot| slot.take());
        if let Some(mut state) = state {
            let ok = execExpr::exec_check(Some(&mut state), econtext, estate)?;
            estate
                .result_rel_mut(result_rel_info)
                .ri_CheckConstraintExprs
                .as_mut()
                .expect("ri_CheckConstraintExprs initialized above")[i] = Some(state);
            if !ok {
                return Ok(Some(ci.ccname.clone_in(mcx)?));
            }
        }
    }

    Ok(None)
}

/// `ExecWithCheckOptions(kind, resultRelInfo, slot, estate)` (execMain.c):
/// evaluate the result relation's WITH CHECK OPTION / RLS with-check policies of
/// the given `kind` against the tuple in `slot`, `ereport(ERROR)`ing on the
/// first violation. WCOs of other kinds are skipped.
pub fn ExecWithCheckOptions<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    kind: i32,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // forboth(l1, ri_WithCheckOptions, l2, ri_WithCheckOptionExprs).
    // Take the compiled ExprState list out of the ResultRelInfo so we can hold a
    // &mut to a state AND a &mut to estate during ExecQual (the ExprState is
    // interpreter scratch owned by the query context; moving the Vec out and
    // back is sound — nothing else aliases it during this call). Restored before
    // every return path via the `restore` closure pattern below.
    let n = estate
        .result_rel(result_rel_info)
        .ri_WithCheckOptions
        .as_ref()
        .map(|v| v.len())
        .unwrap_or(0);

    let mut wco_exprs = estate
        .result_rel_mut(result_rel_info)
        .ri_WithCheckOptionExprs
        .take();

    for i in 0..n {
        // wco->kind (read the WithCheckOption node at index i).
        let wco_kind = {
            let wcos = estate
                .result_rel(result_rel_info)
                .ri_WithCheckOptions
                .as_ref()
                .expect("ExecWithCheckOptions: ri_WithCheckOptions vanished");
            let wco = wcos[i].as_withcheckoption().ok_or_else(|| {
                types_error::PgError::error(
                    "ExecWithCheckOptions: ri_WithCheckOptions element is not a \
                     WithCheckOption node",
                )
            })?;
            wco.kind
        };

        // Skip any WCOs which are not the kind we are looking for now.
        if wco_kind as i32 != kind {
            continue;
        }

        // if (!ExecQual(wcoExpr, econtext)) { ... violation ... }
        let passed = {
            let state = wco_exprs
                .as_mut()
                .and_then(|arr| arr.get_mut(i))
                .expect("ExecWithCheckOptions: ri_WithCheckOptionExprs shorter than options");
            execExpr::exec_qual(&mut **state, econtext, estate)?
        };

        if passed {
            continue;
        }

        // Violation: build the appropriate error per WCO kind.
        // Re-read the WCO node's relname / polname for the message.
        let (relname, polname): (alloc::string::String, Option<alloc::string::String>) = {
            let wcos = estate
                .result_rel(result_rel_info)
                .ri_WithCheckOptions
                .as_ref()
                .expect("ExecWithCheckOptions: ri_WithCheckOptions vanished");
            let wco = wcos[i]
                .as_withcheckoption()
                .expect("ExecWithCheckOptions: WCO node");
            (
                wco.relname
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default(),
                wco.polname.as_ref().map(|s| s.as_str().to_string()),
            )
        };

        match wco_kind {
            types_nodes::rawnodes::WCO_VIEW_CHECK => {
                // For a view WCO we can show the failing row when the user could
                // view the relation directly. convert_routed_slot_for_error maps
                // a routed slot back to the root tupdesc (the C
                // ri_RootResultRelInfo path).
                let (root_relid, desc_slot, _rel_name) =
                    convert_routed_slot_for_error(estate, result_rel_info, slot)?;
                let modified_cols = constraint_modified_cols(estate, result_rel_info)?;
                let val_desc =
                    build_slot_value_desc(estate, root_relid, desc_slot, modified_cols.as_deref())?;
                // C uses wco->relname (the view name) for the message; the
                // relation descriptor's name (rel_name) drives only val_desc.
                let mut err = ereport(ERROR)
                    .errcode(types_error::ERRCODE_WITH_CHECK_OPTION_VIOLATION)
                    .errmsg(alloc::format!(
                        "new row violates check option for view \"{}\"",
                        relname
                    ));
                if let Some(vd) = val_desc {
                    err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
                }
                return Err(err.into_error());
            }
            types_nodes::rawnodes::WCO_RLS_INSERT_CHECK
            | types_nodes::rawnodes::WCO_RLS_UPDATE_CHECK => {
                return Err(rls_with_check_violation(
                    polname.as_deref(),
                    &relname,
                    "new row violates row-level security policy \"{p}\" for table \"{t}\"",
                    "new row violates row-level security policy for table \"{t}\"",
                ));
            }
            types_nodes::rawnodes::WCO_RLS_MERGE_UPDATE_CHECK
            | types_nodes::rawnodes::WCO_RLS_MERGE_DELETE_CHECK => {
                return Err(rls_with_check_violation(
                    polname.as_deref(),
                    &relname,
                    "target row violates row-level security policy \"{p}\" (USING expression) for table \"{t}\"",
                    "target row violates row-level security policy (USING expression) for table \"{t}\"",
                ));
            }
            types_nodes::rawnodes::WCO_RLS_CONFLICT_CHECK => {
                return Err(rls_with_check_violation(
                    polname.as_deref(),
                    &relname,
                    "new row violates row-level security policy \"{p}\" (USING expression) for table \"{t}\"",
                    "new row violates row-level security policy (USING expression) for table \"{t}\"",
                ));
            }
        }
    }

    // All checks of this kind passed: restore the compiled ExprState list onto
    // the ResultRelInfo so later rows / later WCO kinds can be re-evaluated.
    estate
        .result_rel_mut(result_rel_info)
        .ri_WithCheckOptionExprs = wco_exprs;

    Ok(())
}

/// Build an RLS with-check / USING-violation error (ERRCODE_INSUFFICIENT_PRIVILEGE),
/// choosing the named-policy vs unnamed-policy message form.
fn rls_with_check_violation(
    polname: Option<&str>,
    relname: &str,
    with_policy: &str,
    without_policy: &str,
) -> types_error::PgError {
    let msg = match polname {
        Some(p) => with_policy.replace("{p}", p).replace("{t}", relname),
        None => without_policy.replace("{t}", relname),
    };
    ereport(ERROR)
        .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg(msg)
        .into_error()
}

/// `ExecPartitionCheck(resultRelInfo, slot, estate, emitError)` (execMain.c):
/// check that the tuple in `slot` meets the relation's partition constraint.
pub fn ExecPartitionCheck<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
    emit_error: bool,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;

    // If first time through, build the partition-check expression state
    // (owned by the arena ResultRelInfo, query-lifespan).
    if estate
        .result_rel(result_rel_info)
        .ri_PartitionCheckExpr
        .is_none()
    {
        let rel_alias = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecPartitionCheck: ri_RelationDesc is NULL")
            .alias();
        // qual = RelationGetPartitionQual(rel);
        let qual = backend_utils_cache_partcache::RelationGetPartitionQual(mcx, &rel_alias)?;
        // ExecPrepareCheck takes an implicit-AND Expr list.
        let mut exprs: alloc::vec::Vec<types_nodes::primnodes::Expr> = alloc::vec::Vec::new();
        for n in qual.iter() {
            match n.as_expr() {
                Some(e) => exprs.push(e.clone()),
                None => {
                    return Err(unported_node(
                        "ExecPartitionCheck: partition qual element is not an Expr",
                        n,
                    ))
                }
            }
        }
        let expr = execExpr::exec_prepare_check(&exprs, estate)?;
        estate
            .result_rel_mut(result_rel_info)
            .ri_PartitionCheckExpr = expr;
    }

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // success = ExecCheck(ri_PartitionCheckExpr, econtext); (NULL == success)
    let state = estate
        .result_rel_mut(result_rel_info)
        .ri_PartitionCheckExpr
        .take();
    let success = match state {
        None => execExpr::exec_check(None, econtext, estate)?,
        Some(mut state) => {
            let ok = execExpr::exec_check(Some(&mut state), econtext, estate)?;
            estate
                .result_rel_mut(result_rel_info)
                .ri_PartitionCheckExpr = Some(state);
            ok
        }
    };

    // If asked to emit an error, don't return on failure.
    if !success && emit_error {
        ExecPartitionCheckEmitError(estate, result_rel_info, slot)?;
    }

    Ok(success)
}

/// `ExecPartitionCheckEmitError(resultRelInfo, slot, estate)` (execMain.c):
/// form and emit the error after a failed partition-constraint check.
pub fn ExecPartitionCheckEmitError<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // The error message always names the (leaf) relation whose partition
    // constraint failed, i.e. resultRelInfo->ri_RelationDesc — independent of
    // tuple routing (execMain.c: RelationGetRelationName(resultRelInfo->ri_RelationDesc)).
    let relname = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecPartitionCheckEmitError: ri_RelationDesc is NULL")
        .name()
        .to_string();

    // If the tuple was routed, convert it back to the root rowtype so val_desc
    // matches the input tuple.
    let (root_relid, slot, _root_relname) =
        convert_routed_slot_for_error(estate, result_rel_info, slot)?;
    let modified_cols = constraint_modified_cols(estate, result_rel_info)?;

    let val_desc = build_slot_value_desc(estate, root_relid, slot, modified_cols.as_deref())?;

    let mut err = ereport(ERROR)
        .errcode(ERRCODE_CHECK_VIOLATION)
        .errmsg(alloc::format!(
            "new row for relation \"{}\" violates partition constraint",
            relname
        ));
    if let Some(vd) = val_desc {
        err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
    }
    Err(err.into_error())
}

/// The ATTACH-PARTITION leg of `ATRewriteTable` (tablecmds.c): scan the table
/// being attached and verify every live row satisfies the partition constraint
/// `partConstraint` (an implicit-AND list of `Expr` clauses, already mapped to
/// the table's attribute numbers). On the first violating row, `ereport(ERROR,
/// ERRCODE_CHECK_VIOLATION, "partition constraint of relation \"%s\" is
/// violated by some row")`.
///
/// This builds a throwaway `EState`, compiles the constraint via
/// `ExecPrepareCheck`, and runs a `table_beginscan` / `table_scan_getnextslot`
/// loop calling `ExecCheck` per row — the executor-native form of the C
/// `ATRewriteTable` partition-constraint scan. Installed as the
/// `validate_partition_constraint_scan` seam (consumed by tablecmds Phase 3).
pub fn validate_partition_constraint_scan<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relid: types_core::primitive::Oid,
    part_constraint: &[types_nodes::primnodes::Expr],
) -> PgResult<()> {
    // NoLock == 0 (lmgr.h LOCKMODE); types-storage is not a dep of this crate.
    const NoLock: i32 = 0;

    // Nothing to validate.
    if part_constraint.is_empty() {
        return Ok(());
    }

    // oldrel = table_open(tab->relid, NoLock); (the ALTER already holds a lock).
    let oldrel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
    let rel_alias = oldrel.alias();

    // estate = CreateExecutorState(); estate->es_snapshot = GetActiveSnapshot();
    let mut estate = execUtils::create_executor_state_in(mcx)?;
    estate.es_snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;
    estate.es_direction = ScanDirection::ForwardScanDirection;

    // partqualstate = ExecPrepareCheck(partConstraint, estate).
    let mut owned: alloc::vec::Vec<types_nodes::primnodes::Expr> =
        alloc::vec::Vec::with_capacity(part_constraint.len());
    for e in part_constraint {
        owned.push(e.clone_in(mcx)?);
    }
    let mut partqualstate = execExpr::exec_prepare_check(&owned, &mut estate)?;

    // scan slot in the EState pool + per-tuple ExprContext.
    let tupdesc = Some(mcx::alloc_in(mcx, rel_alias.rd_att.clone_in(mcx)?)?);
    let callbacks = backend_access_table_tableam::table_slot_callbacks(&rel_alias);
    let slot_id =
        backend_executor_execTuples_seams::exec_init_extra_tuple_slot::call(&mut estate, tupdesc, callbacks)?;
    let econtext = execUtils::MakePerTupleExprContext(&mut estate)?;

    let snapshot = estate
        .es_snapshot
        .clone()
        .expect("validate_partition_constraint_scan: no active snapshot");

    let result: PgResult<()> = (|| {
        let mut scan =
            backend_access_table_tableam_seams::table_beginscan::call(mcx, &rel_alias, snapshot)?;

        loop {
            backend_tcop_postgres_seams::check_for_interrupts::call()?;

            let got = backend_access_table_tableam_seams::table_scan_getnextslot_direction::call(
                mcx,
                &mut scan,
                estate.es_direction,
                estate.slot_data_mut(slot_id),
            )?;
            if !got {
                break;
            }

            // econtext->ecxt_scantuple = slot; if (!ExecCheck(partqualstate,
            // econtext)) ereport(ERROR, ...).
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot_id);
            let ok = match partqualstate.as_mut() {
                Some(state) => execExpr::exec_check(Some(state), econtext, &mut estate)?,
                None => execExpr::exec_check(None, econtext, &mut estate)?,
            };
            if !ok {
                backend_access_table_tableam::table_endscan(scan)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_CHECK_VIOLATION)
                    .errmsg(alloc::format!(
                        "partition constraint of relation \"{}\" is violated by some row",
                        rel_alias.name()
                    ))
                    .into_error());
            }

            // ResetExprContext(econtext) is implicit per-iteration in the owned
            // model (the per-tuple context is reset by the next slot fill).
        }

        backend_access_table_tableam::table_endscan(scan)?;
        Ok(())
    })();

    oldrel.close(NoLock)?;
    result
}

/// The per-partition scan leg of `check_default_partition_contents`
/// (partbounds.c): scan one leaf partition `part_relid` of the default partition
/// `default_relname` and verify every live row satisfies the revised
/// default-partition constraint `part_constraint` (already mapped to
/// `part_relid`'s attribute numbers). On the first violating row, raise
/// `ereport(ERROR, ERRCODE_CHECK_VIOLATION, "updated partition constraint for
/// default partition \"%s\" would be violated by some row")`, where `%s` is the
/// *default* relation's name (matching C — the message names the default, not the
/// scanned child). Installed as the `validate_default_partition_contents_scan`
/// seam.
///
/// This mirrors [`validate_partition_constraint_scan`] (same throwaway `EState` +
/// `ExecPrepareCheck` + `table_beginscan` / `ExecCheck` loop); only the
/// error-message text and the reported relation name differ.
pub fn validate_default_partition_contents_scan<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    default_relname: &str,
    part_relid: types_core::primitive::Oid,
    part_constraint: &[types_nodes::primnodes::Expr],
) -> PgResult<()> {
    // NoLock == 0 (lmgr.h LOCKMODE); types-storage is not a dep of this crate.
    const NoLock: i32 = 0;

    // Nothing to validate (an empty AND-list means TRUE).
    if part_constraint.is_empty() {
        return Ok(());
    }

    // part_rel = table_open(part_relid, NoLock); (caller already holds the lock).
    let oldrel = backend_access_common_relation::relation_open(mcx, part_relid, NoLock)?;
    let rel_alias = oldrel.alias();

    // estate = CreateExecutorState(); estate->es_snapshot = GetActiveSnapshot();
    let mut estate = execUtils::create_executor_state_in(mcx)?;
    estate.es_snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;
    estate.es_direction = ScanDirection::ForwardScanDirection;

    // partqualstate = ExecPrepareCheck(partition_constraint, estate).
    let mut owned: alloc::vec::Vec<types_nodes::primnodes::Expr> =
        alloc::vec::Vec::with_capacity(part_constraint.len());
    for e in part_constraint {
        owned.push(e.clone_in(mcx)?);
    }
    let mut partqualstate = execExpr::exec_prepare_check(&owned, &mut estate)?;

    // scan slot in the EState pool + per-tuple ExprContext.
    let tupdesc = Some(mcx::alloc_in(mcx, rel_alias.rd_att.clone_in(mcx)?)?);
    let callbacks = backend_access_table_tableam::table_slot_callbacks(&rel_alias);
    let slot_id =
        backend_executor_execTuples_seams::exec_init_extra_tuple_slot::call(&mut estate, tupdesc, callbacks)?;
    let econtext = execUtils::MakePerTupleExprContext(&mut estate)?;

    let snapshot = estate
        .es_snapshot
        .clone()
        .expect("validate_default_partition_contents_scan: no active snapshot");

    let result: PgResult<()> = (|| {
        let mut scan =
            backend_access_table_tableam_seams::table_beginscan::call(mcx, &rel_alias, snapshot)?;

        loop {
            backend_tcop_postgres_seams::check_for_interrupts::call()?;

            let got = backend_access_table_tableam_seams::table_scan_getnextslot_direction::call(
                mcx,
                &mut scan,
                estate.es_direction,
                estate.slot_data_mut(slot_id),
            )?;
            if !got {
                break;
            }

            // econtext->ecxt_scantuple = slot; if (!ExecCheck(partqualstate,
            // econtext)) ereport(ERROR, ...).
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot_id);
            let ok = match partqualstate.as_mut() {
                Some(state) => execExpr::exec_check(Some(state), econtext, &mut estate)?,
                None => execExpr::exec_check(None, econtext, &mut estate)?,
            };
            if !ok {
                backend_access_table_tableam::table_endscan(scan)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_CHECK_VIOLATION)
                    .errmsg(alloc::format!(
                        "updated partition constraint for default partition \"{default_relname}\" would be violated by some row"
                    ))
                    .into_error());
            }
        }

        backend_access_table_tableam::table_endscan(scan)?;
        Ok(())
    })();

    oldrel.close(NoLock)?;
    result
}

/// `ATRewriteTable(tab, OIDNewHeap)` (tablecmds.c) — scan or rewrite one table.
///
/// A rewrite is requested by passing a valid `oid_new_heap` (caller already
/// holds `AccessExclusiveLock` on it, having just `make_new_heap`'d it); for the
/// scan-only verify path `oid_new_heap == InvalidOid`. This is the executor-owned
/// leg: it builds an `EState`, compiles the queued cast/USING/default expressions
/// (`tab->newvals`) and CHECK quals (`tab->constraints`) into `ExprState`s, then
/// scans the old heap. For each row, when rewriting, it copies the old columns
/// into a new slot, evaluates the replacement/generated expressions, re-checks
/// the not-null / CHECK / partition constraints, and inserts the new tuple into
/// the new heap via `table_tuple_insert`. The tablecmds caller does the
/// surrounding `make_new_heap` / `finish_heap_swap`.
#[allow(clippy::too_many_arguments)]
pub fn at_rewrite_table_scan<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relid: Oid,
    oid_new_heap: Oid,
    old_desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
    rewrite: i32,
    newvals: &[(i16, types_nodes::primnodes::Expr, bool)],
    check_constraints: &[(&str, types_nodes::primnodes::Expr)],
    verify_new_notnull: bool,
    partition_constraint: &[types_nodes::primnodes::Expr],
    validate_default: bool,
) -> PgResult<()> {
    use backend_executor_execTuples::exec_init_slots::{
        ExecDropSingleTupleTableSlot, MakeSingleTupleTableSlot,
    };
    use backend_executor_execTuples::slot_store_fetch::{
        ExecClearTuple, ExecStoreAllNullTuple, ExecStoreVirtualTuple,
    };

    const NoLock: i32 = 0;

    // oldrel = table_open(tab->relid, NoLock); newTupDesc = RelationGetDescr(oldrel)
    // (includes all mods); oldTupDesc = tab->oldDesc.
    let oldrel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
    let rel_alias = oldrel.alias();
    let old_relid = oldrel.rd_id;
    let new_tup_desc = oldrel.rd_att.clone_in(mcx)?;
    let old_tup_desc = old_desc.clone_in(mcx)?;

    let rewriting = types_core::primitive::OidIsValid(oid_new_heap);

    // newrel = OidIsValid(OIDNewHeap) ? table_open(OIDNewHeap, NoLock) : NULL.
    let newrel = if rewriting {
        Some(backend_access_common_relation::relation_open(mcx, oid_new_heap, NoLock)?)
    } else {
        None
    };

    // BulkInsertState + insert options + command id (only when rewriting).
    let (mycid, mut bistate, ti_options) = if rewriting {
        let cid = xact_seams::get_current_command_id::call(true)?;
        let bistate = backend_access_heap_heapam_seams::get_bulk_insert_state::call()?;
        // TABLE_INSERT_SKIP_FSM == (1 << 1) (tableam.h).
        (cid, Some(bistate), 1 << 1)
    } else {
        (0, None, 0)
    };

    // estate = CreateExecutorState(); reuse the active snapshot for the scan
    // (the managed active snapshot is at least as new as the ALTER's own catalog
    // mutations — same idiom as validate_partition_constraint_scan / the NOT NULL
    // verify scan, avoiding a private RegisterSnapshot/UnregisterSnapshot pair).
    let mut estate = execUtils::create_executor_state_in(mcx)?;
    estate.es_snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;
    estate.es_direction = ScanDirection::ForwardScanDirection;

    let mut needscan = false;

    // Build CHECK constraint ExprStates (con->qualstate =
    // ExecPrepareExpr(expand_generated_columns_in_expr(con->qual, oldrel, 1))).
    let mut check_states: alloc::vec::Vec<(alloc::string::String, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>)> =
        alloc::vec::Vec::with_capacity(check_constraints.len());
    for (name, qual) in check_constraints.iter() {
        needscan = true;
        // The expander operates in the parser-arena notional `'static`; erase the
        // `'mcx` qual in and re-intern the result into `mcx` for exec_prepare_expr.
        let expanded = backend_rewrite_rewritehandler::expand_generated_columns_in_expr(
            mcx,
            Some(qual.clone_in(mcx)?.erase_lifetime()),
            &rel_alias,
            1,
        )?;
        let expanded_expr = expanded
            .expect("expand_generated_columns_in_expr returned NULL for a non-NULL Expr")
            .clone_in(mcx)?;
        let state = execExpr::exec_prepare_expr(&expanded_expr, &mut estate)?;
        check_states.push(((*name).to_string(), state));
    }

    // Build partition-check ExprState (partqualstate = ExecPrepareExpr(tab->partition_constraint)).
    let mut partqualstate: Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>> = None;
    if !partition_constraint.is_empty() {
        needscan = true;
        // tab->partition_constraint is the single ANDed Expr; ExecPrepareExpr it.
        // (Callers store it already AND-collapsed; an implicit-AND list is
        // AND-folded element-wise by ExecPrepareCheck, but ATRewriteTable uses
        // ExecPrepareExpr on the single stored Expr — mirror that for the first
        // element, AND-ing the rest defensively if more than one was passed.)
        let expr = if partition_constraint.len() == 1 {
            partition_constraint[0].clone_in(mcx)?
        } else {
            let mut planned: alloc::vec::Vec<types_nodes::primnodes::Expr> =
                alloc::vec::Vec::with_capacity(partition_constraint.len());
            for e in partition_constraint {
                planned.push(e.clone_in(mcx)?);
            }
            backend_nodes_core::makefuncs::make_ands_explicit(planned)
        };
        partqualstate = Some(execExpr::exec_prepare_expr(&expr, &mut estate)?);
    }

    // Build newvals ExprStates (ex->exprstate = ExecInitExpr(ex->expr, NULL)).
    // attnum and is_generated travel alongside.
    let mut newval_states: alloc::vec::Vec<(i16, bool, mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>)> =
        alloc::vec::Vec::with_capacity(newvals.len());
    for (attnum, expr, is_generated) in newvals.iter() {
        let state = execExpr::exec_init_expr_no_parent(&expr.clone_in(mcx)?, &mut estate)?;
        newval_states.push((*attnum, *is_generated, state));
    }

    // Collect *valid* (non-virtual) NOT NULL attnums to recheck when rewriting
    // or when verify_new_notnull is set. Virtual generated NOT NULL columns are
    // not supported on this frontier (mirrors at_verify_not_null).
    //
    // notnull_attrs does *not* collect attribute numbers for valid not-null
    // constraints over virtual generated columns; instead, they are collected
    // in notnull_virtual_attrs for verification via ExecRelGenVirtualNotNull().
    let mut notnull_attrs: alloc::vec::Vec<i32> = alloc::vec::Vec::new();
    let mut notnull_virtual_attrs: alloc::vec::Vec<i32> = alloc::vec::Vec::new();
    if rewriting || verify_new_notnull {
        for i in 0..new_tup_desc.natts {
            // C: `CompactAttribute *attr = TupleDescCompactAttr(newTupDesc, i);`
            // then tests `attr->attnullability == ATTNULLABLE_VALID`. Only *valid*
            // not-null constraints are rechecked here — a NOT VALID not-null
            // constraint leaves `attnotnull` set but `attnullability` is
            // ATTNULLABLE_UNKNOWN, so a table rewrite must NOT validate it (its
            // own VALIDATE CONSTRAINT does that). Using `attnotnull` here would
            // spuriously reject a rewrite (e.g. ADD COLUMN ... DEFAULT) on a table
            // whose not-yet-validated NOT NULL column still contains nulls.
            let compact = new_tup_desc.compact_attr(i as usize);
            if compact.attnullability == types_tuple::heaptuple::ATTNULLABLE_VALID
                && !compact.attisdropped
            {
                let att = new_tup_desc.attr(i as usize);
                if att.attgenerated == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL {
                    notnull_virtual_attrs.push(att.attnum as i32);
                } else {
                    notnull_attrs.push(att.attnum as i32);
                }
            }
        }
        if !notnull_attrs.is_empty() || !notnull_virtual_attrs.is_empty() {
            needscan = true;
        }
    }

    // When adding or changing a virtual generated column with a not-null
    // constraint, we need to evaluate whether the generation expression is null.
    // For that, we borrow ExecRelGenVirtualNotNull(). Here, we prepare a dummy
    // ResultRelInfo (dummy rangetable index 0, no partition root).
    let virtual_notnull_rri: Option<types_nodes::RriId> = if !notnull_virtual_attrs.is_empty() {
        let mut r_info = types_nodes::ResultRelInfo::default();
        let instrument = estate.es_instrument;
        InitResultRelInfo(mcx, &mut r_info, oldrel.alias(), 0, None, instrument)?;
        Some(estate.add_result_rel(r_info)?)
    } else {
        None
    };

    // Precompute the list of dropped attributes (set to NULL in the new tuple).
    let mut dropped_attrs: alloc::vec::Vec<usize> = alloc::vec::Vec::new();
    for i in 0..new_tup_desc.natts {
        if new_tup_desc.attr(i as usize).attisdropped {
            dropped_attrs.push(i as usize);
        }
    }

    // The scan body is wrapped so cleanup (table_endscan + drop slots) always
    // runs, including on the Err path.
    let body: PgResult<()> = (|| {
        if !(rewriting || needscan) {
            // Nothing to scan (no rewrite, no constraints, no NOT NULL recheck).
            return Ok(());
        }

        // Create the tuple slots and register them in the EState pool (the
        // ExprContext->ecxt_scantuple references a pool SlotId). When rewriting,
        // two slots are needed (old over oldTupDesc, new over newTupDesc);
        // otherwise one slot over newTupDesc suffices.
        let old_callbacks = backend_access_table_tableam::table_slot_callbacks(&rel_alias);
        let (oldslot_id, newslot_id): (types_nodes::SlotId, Option<types_nodes::SlotId>) =
            if rewriting {
                let newrel_alias = newrel
                    .as_ref()
                    .expect("at_rewrite_table_scan: newrel is NULL while rewriting")
                    .alias();
                let new_callbacks =
                    backend_access_table_tableam::table_slot_callbacks(&newrel_alias);
                let oslot = MakeSingleTupleTableSlot(
                    mcx,
                    Some(mcx::alloc_in(mcx, old_tup_desc.clone_in(mcx)?)?),
                    old_callbacks,
                )?;
                let mut nslot = MakeSingleTupleTableSlot(
                    mcx,
                    Some(mcx::alloc_in(mcx, new_tup_desc.clone_in(mcx)?)?),
                    new_callbacks,
                )?;
                // Set all columns in the new slot to NULL initially (columns
                // added by the rewrite with a NULL default get no newval expr).
                ExecStoreAllNullTuple(mcx, &mut nslot)?;
                let oid = estate.push_slot_data(oslot)?;
                let nid = estate.push_slot_data(nslot)?;
                (oid, Some(nid))
            } else {
                let oslot = MakeSingleTupleTableSlot(
                    mcx,
                    Some(mcx::alloc_in(mcx, new_tup_desc.clone_in(mcx)?)?),
                    old_callbacks,
                )?;
                let oid = estate.push_slot_data(oslot)?;
                (oid, None)
            };

        let econtext = execUtils::MakePerTupleExprContext(&mut estate)?;

        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("at_rewrite_table_scan: no active snapshot");

        let mut scan =
            backend_access_table_tableam_seams::table_beginscan::call(mcx, &rel_alias, snapshot)?;

        // while (table_scan_getnextslot(scan, ForwardScanDirection, oldslot))
        loop {
            let got = backend_access_table_tableam_seams::table_scan_getnextslot_direction::call(
                mcx,
                &mut scan,
                estate.es_direction,
                estate.slot_data_mut(oldslot_id),
            )?;
            if !got {
                break;
            }

            let insertslot: types_nodes::SlotId = if rewrite > 0 {
                let newslot_id = newslot_id.expect("rewrite>0 but no new slot");

                // slot_getallattrs(oldslot); ExecClearTuple(newslot).
                backend_executor_execTuples::slot_deform::slot_getallattrs(
                    mcx,
                    estate.slot_data_mut(oldslot_id),
                )?;
                ExecClearTuple(estate.slot_data_mut(newslot_id))?;

                // Copy attributes old -> new (memcpy of tts_values/tts_isnull up
                // to oldslot->tts_nvalid); set dropped attrs to NULL; set
                // tts_tableOid = RelationGetRelid(oldrel).
                {
                    let (oldslot, newslot) =
                        estate.slot_data_pair_mut(oldslot_id, newslot_id);
                    let nvalid = oldslot.base().tts_nvalid as usize;
                    let (ovals, oisn) = {
                        let ob = oldslot.base();
                        (&ob.tts_values, &ob.tts_isnull)
                    };
                    let nb = newslot.base_mut();
                    for i in 0..nvalid {
                        nb.tts_values[i] = ovals[i].clone_in(mcx)?;
                        nb.tts_isnull[i] = oisn[i];
                    }
                    for &d in dropped_attrs.iter() {
                        nb.tts_isnull[d] = true;
                    }
                    nb.tts_tableOid = old_relid;
                }

                // First, evaluate expressions whose inputs come from the old
                // tuple (ex->is_generated == false). econtext->ecxt_scantuple = oldslot.
                estate.ecxt_mut(econtext).ecxt_scantuple = Some(oldslot_id);
                for (attnum, is_generated, state) in newval_states.iter_mut() {
                    if *is_generated {
                        continue;
                    }
                    let (val, isnull) =
                        execExpr::exec_eval_expr_switch_context(state, econtext, &mut estate)?;
                    let nb = estate.slot_data_mut(newslot_id).base_mut();
                    nb.tts_values[(*attnum - 1) as usize] = val;
                    nb.tts_isnull[(*attnum - 1) as usize] = isnull;
                }

                // ExecStoreVirtualTuple(newslot).
                ExecStoreVirtualTuple(estate.slot_data_mut(newslot_id))?;

                // Then, evaluate generated expressions (inputs from the new
                // tuple). econtext->ecxt_scantuple = newslot.
                estate.ecxt_mut(econtext).ecxt_scantuple = Some(newslot_id);
                for (attnum, is_generated, state) in newval_states.iter_mut() {
                    if !*is_generated {
                        continue;
                    }
                    let (val, isnull) =
                        execExpr::exec_eval_expr_switch_context(state, econtext, &mut estate)?;
                    let nb = estate.slot_data_mut(newslot_id).base_mut();
                    nb.tts_values[(*attnum - 1) as usize] = val;
                    nb.tts_isnull[(*attnum - 1) as usize] = isnull;
                }

                newslot_id
            } else {
                // No rewrite: verify constraints over the old slot directly.
                oldslot_id
            };

            // Now check any constraints on the possibly-changed tuple.
            estate.ecxt_mut(econtext).ecxt_scantuple = Some(insertslot);

            // NOT NULL recheck.
            for &attn in notnull_attrs.iter() {
                let (_v, isnull) = backend_executor_execTuples::slot_deform::slot_getattr(
                    mcx,
                    estate.slot_data_mut(insertslot),
                    attn as i16,
                )?;
                if isnull {
                    let att = new_tup_desc.attr((attn - 1) as usize);
                    let attname =
                        alloc::string::String::from_utf8_lossy(att.attname.name_str())
                            .into_owned();
                    let relname = rel_alias.name().to_string();
                    backend_access_table_tableam::table_endscan(scan)?;
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_NOT_NULL_VIOLATION)
                        .errmsg(alloc::format!(
                            "column \"{attname}\" of relation \"{relname}\" contains null values"
                        ))
                        .into_error());
                }
            }

            // NOT NULL recheck over virtual generated columns: evaluate the
            // generation expression and verify it is non-null.
            if let Some(rri) = virtual_notnull_rri {
                let attnum = ExecRelGenVirtualNotNull(
                    &mut estate,
                    rri,
                    insertslot,
                    &notnull_virtual_attrs,
                )?;
                if attnum != 0 {
                    let att = new_tup_desc.attr((attnum - 1) as usize);
                    let attname =
                        alloc::string::String::from_utf8_lossy(att.attname.name_str())
                            .into_owned();
                    let relname = rel_alias.name().to_string();
                    backend_access_table_tableam::table_endscan(scan)?;
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_NOT_NULL_VIOLATION)
                        .errmsg(alloc::format!(
                            "column \"{attname}\" of relation \"{relname}\" contains null values"
                        ))
                        .into_error());
                }
            }

            // CHECK constraints (ExecCheck: NULL counts as success).
            for (name, state) in check_states.iter_mut() {
                let ok = execExpr::exec_check(Some(state), econtext, &mut estate)?;
                if !ok {
                    let relname = rel_alias.name().to_string();
                    backend_access_table_tableam::table_endscan(scan)?;
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_CHECK_VIOLATION)
                        .errmsg(alloc::format!(
                            "check constraint \"{name}\" of relation \"{relname}\" is violated by some row"
                        ))
                        .into_error());
                }
            }

            // Partition constraint (ExecCheck).
            if let Some(state) = partqualstate.as_mut() {
                let ok = execExpr::exec_check(Some(state), econtext, &mut estate)?;
                if !ok {
                    let relname = rel_alias.name().to_string();
                    backend_access_table_tableam::table_endscan(scan)?;
                    let msg = if validate_default {
                        alloc::format!(
                            "updated partition constraint for default partition \"{relname}\" would be violated by some row"
                        )
                    } else {
                        alloc::format!(
                            "partition constraint of relation \"{relname}\" is violated by some row"
                        )
                    };
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_CHECK_VIOLATION)
                        .errmsg(msg)
                        .into_error());
                }
            }

            // Write the tuple out to the new relation.
            if let Some(nr) = newrel.as_ref() {
                let nr_alias = nr.alias();
                backend_access_table_tableam::table_tuple_insert(
                    mcx,
                    &nr_alias,
                    estate.slot_data_mut(insertslot),
                    mycid,
                    ti_options,
                    bistate.as_mut(),
                )?;
            }

            // ResetExprContext(econtext) — reset the per-tuple memory.
            execUtils::ResetPerTupleExprContext(&mut estate);

            backend_tcop_postgres_seams::check_for_interrupts::call()?;
        }

        backend_access_table_tableam::table_endscan(scan)?;

        // ExecDropSingleTupleTableSlot(oldslot); if (newslot) drop it too. The
        // pool-owned slots are reclaimed with the EState; explicit clear to
        // mirror C's ExecDropSingleTupleTableSlot (releases any pin). Take them
        // out of the pool by value.
        ExecClearTuple(estate.slot_data_mut(oldslot_id))?;
        if let Some(nid) = newslot_id {
            ExecClearTuple(estate.slot_data_mut(nid))?;
        }
        Ok(())
    })();

    // FreeExecutorState(estate).
    execUtils::free_executor_state_in(estate)?;

    // table_close(oldrel, NoLock); if (newrel) { FreeBulkInsertState; finish; close }.
    oldrel.close(NoLock)?;
    if let Some(nr) = newrel {
        if let Some(mut b) = bistate.take() {
            backend_access_heap_heapam_seams::free_bulk_insert_state::call(&mut b)?;
        }
        let nr_alias = nr.alias();
        backend_access_table_tableam::table_finish_bulk_insert(&nr_alias, ti_options)?;
        nr.close(NoLock)?;
    }

    let _ = ExecDropSingleTupleTableSlot; // keep the import meaningful if scan skipped
    body
}

/// `ExecConstraints(resultRelInfo, slot, estate)` (execMain.c): check the
/// relation's NOT NULL and CHECK constraints against `slot`. The partition
/// constraint is *not* checked here.
fn ExecConstraints<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<()> {
    // Snapshot the not-null / has-not-null / relchecks shape and per-attribute
    // (attnotnull, attgenerated) flags.
    struct AttInfo {
        attnum: i32,
        attnotnull: bool,
        is_virtual_generated: bool,
    }
    let (has_constr, has_not_null, natts, atts, relchecks): (bool, bool, i32, alloc::vec::Vec<AttInfo>, i32) = {
        let rri = estate.result_rel(result_rel_info);
        let rel = rri
            .ri_RelationDesc
            .as_ref()
            .expect("ExecConstraints: ri_RelationDesc is NULL");
        let tupdesc = &*rel.rd_att;
        let constr = tupdesc.constr.as_ref();
        let has_not_null = constr.map(|c| c.has_not_null).unwrap_or(false);
        let natts = tupdesc.natts;
        let mut atts: alloc::vec::Vec<AttInfo> = alloc::vec::Vec::new();
        if has_not_null {
            for a in 0..natts {
                let att = tupdesc.attr(a as usize);
                atts.push(AttInfo {
                    attnum: (a + 1),
                    attnotnull: att.attnotnull,
                    is_virtual_generated: att.attgenerated
                        == types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL,
                });
            }
        }
        let num_check = constr.map(|c| c.num_check as i32).unwrap_or(0);
        (constr.is_some(), has_not_null, natts, atts, num_check)
    };

    // We should not be called otherwise.
    debug_assert!(has_constr);

    // Verify not-null constraints (collecting virtual generated columns).
    let mut notnull_virtual_attrs: alloc::vec::Vec<i32> = alloc::vec::Vec::new();
    if has_not_null {
        // Deform the slot once so slot_attisnull reads are valid.
        let nulls = slot_isnulls(estate, slot, natts as usize)?;
        for ai in atts.iter() {
            if ai.attnotnull && ai.is_virtual_generated {
                notnull_virtual_attrs.push(ai.attnum);
            } else if ai.attnotnull && nulls[(ai.attnum - 1) as usize] {
                ReportNotNullViolationError(estate, result_rel_info, slot, ai.attnum)?;
            }
        }
    }

    // Verify not-null constraints on virtual generated columns, if any.
    if !notnull_virtual_attrs.is_empty() {
        let attnum =
            ExecRelGenVirtualNotNull(estate, result_rel_info, slot, &notnull_virtual_attrs)?;
        if attnum != 0 {
            ReportNotNullViolationError(estate, result_rel_info, slot, attnum)?;
        }
    }

    // Verify check constraints.
    if relchecks > 0 {
        if let Some(failed) = ExecRelCheck(estate, result_rel_info, slot)? {
            let orig_relname = {
                let rel = estate
                    .result_rel(result_rel_info)
                    .ri_RelationDesc
                    .as_ref()
                    .expect("ExecConstraints: ri_RelationDesc is NULL");
                rel.name().to_string()
            };
            // If routed, convert back to the root rowtype for the message.
            let (reloid, slot, _relname) =
                convert_routed_slot_for_error(estate, result_rel_info, slot)?;
            let modified_cols = constraint_modified_cols(estate, result_rel_info)?;
            let val_desc =
                build_slot_value_desc(estate, reloid, slot, modified_cols.as_deref())?;

            let mut err = ereport(ERROR)
                .errcode(ERRCODE_CHECK_VIOLATION)
                .errmsg(alloc::format!(
                    "new row for relation \"{}\" violates check constraint \"{}\"",
                    orig_relname,
                    failed.as_str()
                ));
            if let Some(vd) = val_desc {
                err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
            }
            return Err(err.into_error());
        }
    }

    Ok(())
}

/// `ExecRelGenVirtualNotNull(resultRelInfo, slot, estate, notnull_virtual_attrs)`
/// (execMain.c): verify not-null constraints on virtual generated columns.
/// Returns `InvalidAttrNumber` (0) when all satisfied, else the violating attnum.
fn ExecRelGenVirtualNotNull<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
    notnull_virtual_attrs: &[i32],
) -> PgResult<i32> {
    let mcx = estate.es_query_cxt;

    // Build a NullTest ExprState per virtual generated column (cached, owned by
    // the arena ResultRelInfo).
    if estate
        .result_rel(result_rel_info)
        .ri_GenVirtualNotNullConstraintExprs
        .is_none()
        && !notnull_virtual_attrs.is_empty()
    {
        let rel_alias = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ExecRelGenVirtualNotNull: ri_RelationDesc is NULL")
            .alias();
        let mut exprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> =
            mcx::vec_with_capacity_in(mcx, notnull_virtual_attrs.len())?;
        for &attnum in notnull_virtual_attrs {
            // "generated_expression IS NOT NULL".
            let arg = backend_rewrite_rewritehandler::build_generation_expression(
                mcx, &rel_alias, attnum,
            )?;
            let nnulltest = types_nodes::primnodes::NullTest {
                arg: Some(alloc::boxed::Box::new(arg)),
                nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                argisrow: false,
                location: -1,
            };
            let expr = types_nodes::primnodes::Expr::NullTest(nnulltest);
            exprs.push(Some(execExpr::exec_prepare_expr(&expr, estate)?));
        }
        estate
            .result_rel_mut(result_rel_info)
            .ri_GenVirtualNotNullConstraintExprs = Some(exprs);
    }

    // econtext = GetPerTupleExprContext(estate); econtext->ecxt_scantuple = slot;
    let econtext = execUtils::MakePerTupleExprContext(estate)?;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // Evaluate each virtual-not-null check.
    for (i, &attnum) in notnull_virtual_attrs.iter().enumerate() {
        let state = estate
            .result_rel_mut(result_rel_info)
            .ri_GenVirtualNotNullConstraintExprs
            .as_mut()
            .and_then(|arr| arr.get_mut(i))
            .and_then(|s| s.take());
        let mut state = state.expect("ExecRelGenVirtualNotNull: cached ExprState is NULL");
        let ok = execExpr::exec_check(Some(&mut state), econtext, estate)?;
        estate
            .result_rel_mut(result_rel_info)
            .ri_GenVirtualNotNullConstraintExprs
            .as_mut()
            .expect("ri_GenVirtualNotNullConstraintExprs initialized above")[i] = Some(state);
        if !ok {
            return Ok(attnum);
        }
    }

    Ok(0)
}

/// `ReportNotNullViolationError(resultRelInfo, slot, estate, attnum)`
/// (execMain.c): report an already-detected not-null violation for `attnum`.
fn ReportNotNullViolationError<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
    attnum: i32,
) -> PgResult<()> {
    debug_assert!(attnum > 0);

    let (orig_relname, attname) = {
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("ReportNotNullViolationError: ri_RelationDesc is NULL");
        let att = rel.rd_att.attr((attnum - 1) as usize);
        (
            rel.name().to_string(),
            alloc::string::String::from_utf8_lossy(att.attname.name_str()).into_owned(),
        )
    };

    // If routed, convert back to the root rowtype for the message.
    let (reloid, slot, _relname) =
        convert_routed_slot_for_error(estate, result_rel_info, slot)?;
    let modified_cols = constraint_modified_cols(estate, result_rel_info)?;
    let val_desc = build_slot_value_desc(estate, reloid, slot, modified_cols.as_deref())?;

    let mut err = ereport(ERROR)
        .errcode(ERRCODE_NOT_NULL_VIOLATION)
        .errmsg(alloc::format!(
            "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
            attname,
            orig_relname
        ));
    if let Some(vd) = val_desc {
        err = err.errdetail(alloc::format!("Failing row contains {}.", vd.as_str()));
    }
    Err(err.into_error())
}

// --- constraint-cluster helpers -------------------------------------------

fn unported_node(msg: &str, node: &types_nodes::nodes::Node<'_>) -> types_error::PgError {
    unported(&alloc::format!("{msg}: {:?}", node.node_tag()))
}

/// Deform `slot` and return its per-attribute null flags (`slot_attisnull`).
fn slot_isnulls<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    slot: types_nodes::SlotId,
    natts: usize,
) -> PgResult<alloc::vec::Vec<bool>> {
    let deformed = backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, slot)?;
    let mut nulls = alloc::vec::Vec::with_capacity(natts);
    for (_v, n) in deformed.iter() {
        nulls.push(*n);
    }
    // A short slot reads remaining attributes as NULL (slot_attisnull contract).
    while nulls.len() < natts {
        nulls.push(true);
    }
    Ok(nulls)
}

/// `bms_union(ExecGetInsertedCols(rri, estate), ExecGetUpdatedCols(rri, estate))`
/// over the (root, when routed) result relation for the error `val_desc`.
fn constraint_modified_cols<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
) -> PgResult<Option<mcx::PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mcx = estate.es_query_cxt;
    // C unions over the root RRI when the tuple was routed; otherwise the RRI.
    let target = estate
        .result_rel(result_rel_info)
        .ri_RootResultRelInfo
        .unwrap_or(result_rel_info);
    let inserted = execUtils::ExecGetInsertedCols(estate, target, mcx)?;
    let updated = execUtils::ExecGetUpdatedCols(estate, target, mcx)?;
    backend_nodes_core::bitmapset::bms_union(mcx, inserted.as_deref(), updated.as_deref())
}

/// Build the `val_desc` for a constraint error: deform the slot, then call
/// [`ExecBuildSlotValueDescription`] against the (root, when routed) relation's
/// descriptor.
fn build_slot_value_desc<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    reloid: Oid,
    slot: types_nodes::SlotId,
    modified_cols: Option<&Bitmapset<'_>>,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let mcx = estate.es_query_cxt;
    // slot_getallattrs(slot): make the value/null arrays valid for the reader.
    let _ = backend_executor_execTuples_seams::slot_getallattrs_by_id::call(estate, slot)?;
    // C passes RelationGetDescr(rel) (the relation's catalog descriptor) — not
    // the slot's descriptor — so per-attribute flags such as attgenerated are
    // read faithfully (a virtual generated column must render as "virtual",
    // even when the slot's own descriptor is a stripped physical rowtype). The
    // relation is already locked by the running command; open with NoLock.
    const NoLock: i32 = 0;
    let rel = backend_access_common_relation::relation_open(mcx, reloid, NoLock)?;
    let tupdesc = rel.rd_att.clone_in(mcx)?;
    rel.close(NoLock)?;
    let slot_ref = estate.slot(slot);
    ExecBuildSlotValueDescription(mcx, reloid, slot_ref, &tupdesc, modified_cols, 64)
}

/// If the tuple was routed (the RRI has a root), convert `slot` back to the
/// root table's rowtype so the error `val_desc` matches the input tuple.
/// Returns `(reloid_for_val_desc, slot_to_describe, relname_of_root_or_self)`.
fn convert_routed_slot_for_error<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    result_rel_info: types_nodes::RriId,
    slot: types_nodes::SlotId,
) -> PgResult<(Oid, types_nodes::SlotId, alloc::string::String)> {
    let mcx = estate.es_query_cxt;
    let root = estate.result_rel(result_rel_info).ri_RootResultRelInfo;
    let Some(root) = root else {
        // Not routed: describe against this relation directly.
        let rel = estate
            .result_rel(result_rel_info)
            .ri_RelationDesc
            .as_ref()
            .expect("convert_routed_slot_for_error: ri_RelationDesc is NULL");
        return Ok((rel.rd_id, slot, rel.name().to_string()));
    };

    // map = build_attrmap_by_name_if_req(old_tupdesc, root_tupdesc, false);
    let old_tupdesc: TupleDescData<'mcx> = estate
        .result_rel(result_rel_info)
        .ri_RelationDesc
        .as_ref()
        .expect("convert_routed_slot_for_error: ri_RelationDesc is NULL")
        .rd_att
        .clone_in(mcx)?;
    let (root_relid, root_relname, root_tupdesc): (Oid, alloc::string::String, TupleDescData<'mcx>) = {
        let root_rel = estate
            .result_rel(root)
            .ri_RelationDesc
            .as_ref()
            .expect("convert_routed_slot_for_error: root ri_RelationDesc is NULL");
        (
            root_rel.rd_id,
            root_rel.name().to_string(),
            root_rel.rd_att.clone_in(mcx)?,
        )
    };

    let map = backend_access_common_next::attmap::build_attrmap_by_name_if_req(
        mcx,
        &old_tupdesc,
        &root_tupdesc,
        false,
    )?;

    let out_slot = if let Some(map) = map {
        // slot = execute_attr_map_slot(map, slot, MakeTupleTableSlot(root_tupdesc, &TTSOpsVirtual));
        let new_slot = backend_executor_execTuples::slot_payload_model::MakeTupleTableSlot(
            mcx,
            Some(mcx::alloc_in(mcx, root_tupdesc.clone_in(mcx)?)?),
            types_nodes::TupleSlotKind::Virtual,
        )?;
        let out = estate.push_slot_data(new_slot)?;
        backend_access_common_next::tupconvert::execute_attr_map_slot(estate, &map, slot, out)?
    } else {
        slot
    };

    Ok((root_relid, out_slot, root_relname))
}

pub fn init_seams() {
    seams::exec_constraints::set(ExecConstraints);
    seams::exec_partition_check::set(ExecPartitionCheck);
    seams::exec_partition_check_emit_error::set(ExecPartitionCheckEmitError);
    seams::validate_partition_constraint_scan::set(validate_partition_constraint_scan);
    seams::validate_default_partition_contents_scan::set(validate_default_partition_contents_scan);
    seams::at_rewrite_table_scan::set(at_rewrite_table_scan);
    seams::exec_check_permissions_select::set(exec_check_permissions_select);
    seams::exec_check_one_rel_perms_view::set(exec_check_one_rel_perms_view);
    seams::exec_build_slot_value_description::set(ExecBuildSlotValueDescription);
    seams::exec_with_check_options::set(ExecWithCheckOptions);
    seams::init_result_rel_info::set(InitResultRelInfo);
    seams::check_valid_result_rel::set(CheckValidResultRel);
    seams::exec_get_ancestor_result_rels::set(ExecGetAncestorResultRels);
    seams::scan_node_extract_tid::set(ScanNodeExtractTid);
    seams::eval_plan_qual_init::set(EvalPlanQualInit);
    seams::eval_plan_qual_set_plan_with_row_marks::set(eval_plan_qual_set_plan_with_row_marks);
    seams::link_subplan_planstate::set(link_subplan_planstate);
    seams::executor_run::set(ExecutorRun);
    seams::executor_finish::set(ExecutorFinish);
    seams::executor_end::set(ExecutorEnd);
    seams::executor_rewind::set(ExecutorRewind);
    seams::free_query_desc::set(FreeQueryDesc);
    seams::create_query_desc_and_start_explain::set(CreateQueryDescAndStartExplain);

    // COPY-(query)-TO executor lifecycle (copyto.c).
    seams::create_query_desc_and_start::set(CreateQueryDescAndStartCopy);
    seams::executor_run_copy::set(ExecutorRunCopy);
    seams::end_copy_query::set(EndCopyQuery);

    // ExecSupportsBackwardScan (body in execAmi) + ExecUpdateLockMode.
    seams::exec_supports_backward_scan::set(exec_supports_backward_scan);
    seams::exec_update_lock_mode::set(ExecUpdateLockMode);

    install_get_tuple_for_trigger_seams();

    // ExecGetReturningSlot / ExecGetChildToRootMap bodies live in execUtils;
    // execMain owns these seam decls (consumed by nodeModifyTable) and delegates.
    seams::exec_get_returning_slot::set(exec_get_returning_slot);
    seams::exec_get_child_to_root_map::set(exec_get_child_to_root_map);
    seams::exec_get_child_to_root_map_full::set(exec_get_child_to_root_map_full);

    // PARAM_EXEC `execPlan` link plumbing. nodeSubplan parked these executor
    // PARAM_EXEC / `es_subplanstates` seams in the execProcnode-seams crate, but
    // they are not `execProcnode.c` functions: they operate on the EState's param
    // array (`es_param_exec_vals[paramid].execPlan`), which is executor
    // (execMain) machinery. Now that `ParamExecData.execPlan` is modeled (an
    // `ExecPlanLink` identity into `es_subplanstates`), the field-level operations
    // — mark/clear/pending-test — have a real home here.
    seams::mark_param_execplan_pending::set(mark_param_execplan_pending);
    seams::clear_param_execplan::set(clear_param_execplan);
    seams::param_execplan_pending::set(param_execplan_pending);

    // The parallel executor's `execParallel-support` PARAM_EXEC reads/writes and
    // `QueryDesc` lifecycle accessors — all over the owned `EState`/`QueryDesc`
    // this crate drives.
    use backend_executor_execParallel_support_seams as sup;
    sup::param_exec_value_owned::set(param_exec_value_owned);
    sup::set_param_exec_value_owned::set(set_param_exec_value_owned);
    sup::query_desc_source_text_owned::set(|qd| Ok(qd.source_text_owned()));
    sup::set_query_desc_jit_flags_owned::set(|qd, jit_flags| qd.set_jit_flags(jit_flags));
    sup::query_desc_estate_has_jit_owned::set(|qd| qd.estate_has_jit());
    // The worker plan-shipping `QueryDesc` reconstruction
    // (`ExecParallelGetQueryDesc`): `stringToNode(pstmtspace)` -> CreateQueryDesc
    // with GetActiveSnapshot()/InvalidSnapshot. The `PlannedStmt` reader is the
    // readfuncs plan-ship leg; this crate owns `CreateQueryDesc`.
    sup::create_parallel_query_desc::set(create_parallel_query_desc);
}

/// `ExecParallelGetQueryDesc` body — `(PlannedStmt *) stringToNode(pstmtspace)`
/// then `CreateQueryDesc(pstmt, queryString, GetActiveSnapshot(),
/// InvalidSnapshot, receiver, RestoreParamList(...), NULL, instrument_options)`.
///
/// The `PlannedStmt` is parsed into `mcx` (the worker's executor/top context);
/// `CreateQueryDesc` deep-copies it into the `QueryDesc`'s own per-query context
/// (a child of `mcx`'s context, like C's `CurrentMemoryContext`), so the
/// temporary parse allocation can be reclaimed with `mcx`.
fn create_parallel_query_desc<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstmt_text: alloc::string::String,
    query_string: alloc::string::String,
    receiver: types_execparallel::DestReceiverHandle,
    params: ParamListInfo,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    // pstmt = (PlannedStmt *) stringToNode(pstmtspace);
    let pstmt = backend_nodes_readfuncs_seams::string_to_planned_stmt::call(mcx, &pstmt_text)?;

    // GetActiveSnapshot() — the leader's active snapshot was restored into the
    // worker's snapshot stack by InitializeParallelDSM / RestoreSnapshot.
    let snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?;

    // The parallel-executor `DestReceiverHandle` (types_execparallel) and the
    // executor's `dest.h` `DestReceiverHandle` (types_nodes::parsestmt) are the
    // same live `DestReceiver *` token under two homes; carry the value across.
    let dest = DestReceiverHandle(receiver.0 as u64);

    CreateQueryDesc(
        mcx.context(),
        &pstmt,
        &query_string,
        snapshot,
        None, // InvalidSnapshot crosscheck_snapshot
        dest,
        params,
        instrument_options,
    )
}

// ===========================================================================
// PARAM_EXEC `execPlan` link plumbing (execMain owns `es_param_exec_vals`).
// ===========================================================================

/// `&estate->es_param_exec_vals[paramid]`, mutably. The C indexes
/// unconditionally; an out-of-range id is a planner/caller bug, so it surfaces
/// the executor's internal error rather than panicking.
#[inline]
fn exec_param_mut<'a, 'mcx>(
    estate: &'a mut types_nodes::EStateData<'mcx>,
    paramid: i32,
) -> PgResult<&'a mut types_nodes::ParamExecData<'mcx>> {
    estate
        .es_param_exec_vals
        .get_mut(paramid as usize)
        .ok_or_else(|| types_error::PgError::error("PARAM_EXEC id out of range").with_sqlstate(types_error::ERRCODE_INTERNAL_ERROR))
}

/// `prm = &(estate->es_param_exec_vals[paramid]); prm->execPlan = sstate;` —
/// mark the PARAM_EXEC slot as needing lazy evaluation by the subplan whose
/// stable identity is `plan_id` (the 1-based index into `es_subplanstates`).
/// Mirrors nodeSubplan.c `ExecInitSubPlan` / `ExecReScanSetParamPlan`.
fn mark_param_execplan_pending<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    paramid: i32,
    plan_id: i32,
) -> PgResult<()> {
    let prm = exec_param_mut(estate, paramid)?;
    prm.execPlan = Some(types_nodes::ExecPlanLink { plan_id });
    Ok(())
}

/// `prm->execPlan = NULL;` — clear the PARAM_EXEC `execPlan` link after the
/// initplan output has been set (nodeSubplan.c `ExecSetParamPlan`).
fn clear_param_execplan<'mcx>(estate: &mut types_nodes::EStateData<'mcx>, paramid: i32) -> PgResult<()> {
    let prm = exec_param_mut(estate, paramid)?;
    prm.execPlan = None;
    Ok(())
}

/// `econtext->ecxt_param_exec_vals[paramid].execPlan != NULL` — is this param
/// not yet evaluated? (`ExecSetParamPlanMulti`). Reads the `execPlan` link. The C
/// indexes unconditionally; out of range maps to "not pending" (the C would
/// dereference, but the planner never produces an out-of-range PARAM_EXEC id).
fn param_execplan_pending(estate: &types_nodes::EStateData<'_>, paramid: i32) -> bool {
    estate
        .es_param_exec_vals
        .get(paramid as usize)
        .is_some_and(|prm| prm.execPlan.is_some())
}

/// The process-lifetime context backing the `'static` `ParamExecValue` /
/// restored-param the parallel executor's `execParallel-support` seams carry
/// (the serialized value is read out, copied through the DSM chunk, and dropped;
/// its lifetime is unconstrained at the seam boundary). Mirrors the DSA wire
/// path's `dsa_top_mcx`.
fn param_exec_top_mcx() -> mcx::Mcx<'static> {
    use core::sync::atomic::{AtomicPtr, Ordering};
    // Process-lifetime leaked context (no_std: a lazily-initialized atomic
    // pointer in place of a `thread_local!`; PG backends are single-threaded so
    // the init race is benign — a loser leaks one extra context).
    static TOP: AtomicPtr<mcx::MemoryContext> = AtomicPtr::new(core::ptr::null_mut());
    let mut p = TOP.load(Ordering::Acquire);
    if p.is_null() {
        let leaked: &'static mut mcx::MemoryContext = alloc::boxed::Box::leak(
            alloc::boxed::Box::new(mcx::MemoryContext::new("execParallel param_exec")),
        );
        let new = leaked as *mut mcx::MemoryContext;
        match TOP.compare_exchange(core::ptr::null_mut(), new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => p = new,
            Err(existing) => p = existing,
        }
    }
    // SAFETY: `p` is a non-null pointer to a leaked, never-freed `MemoryContext`.
    unsafe { (*p).mcx() }
}

/// `&estate->es_param_exec_vals[paramid]` value/isnull + the resolved
/// `(typByVal, typLen)` for `list_nth_oid(paramExecTypes, paramid)`
/// (execParallel.c `EstimateParamExecSpace`/`SerializeParamExecParams`). With no
/// type OID, C assumes by-value (`typLen = sizeof(Datum)`, `typByVal = true`),
/// like `copyParamList`.
fn param_exec_value_owned(
    estate: &mut types_nodes::EStateData<'_>,
    paramid: i32,
) -> types_execparallel::ParamExecValue<'static> {
    let prm = estate
        .es_param_exec_vals
        .get(paramid as usize)
        .expect("PARAM_EXEC id in range");
    let value = prm
        .value
        .clone_in(param_exec_top_mcx())
        .expect("clone PARAM_EXEC value");
    let isnull = prm.isnull;

    // typeOid = list_nth_oid(estate->es_plannedstmt->paramExecTypes, paramid);
    let type_oid = estate
        .es_plannedstmt
        .as_ref()
        .and_then(|p| p.paramExecTypes.as_ref())
        .and_then(|types| types.get(paramid as usize).copied())
        .unwrap_or(types_core::InvalidOid);

    let (typ_len, typ_byval) = if type_oid != types_core::InvalidOid {
        backend_utils_cache_lsyscache_seams::get_typlenbyval::call(type_oid)
            .expect("get_typlenbyval")
    } else {
        // No type OID: assume by-value, like copyParamList does.
        (core::mem::size_of::<usize>() as i16, true)
    };

    types_execparallel::ParamExecValue {
        value,
        isnull,
        typ_byval,
        typ_len,
    }
}

/// `prm = &es_param_exec_vals[paramid]; prm->value = ...; prm->isnull = ...;
/// prm->execPlan = NULL;` (execParallel.c `RestoreParamExecParams`).
fn set_param_exec_value_owned<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    paramid: i32,
    restored: types_execparallel::RestoredParam<'mcx>,
) {
    let prm = exec_param_mut(estate, paramid).expect("PARAM_EXEC id in range");
    prm.value = restored.value;
    prm.isnull = restored.isnull;
    prm.execPlan = None;
}
