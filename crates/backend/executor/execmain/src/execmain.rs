use std::rc::Rc;

use ::executils::EStateData;
use ::mcx::{McxOwned, MemoryContext};
use ::tcop_dest::DestReceiver;
use ::types_core::CommandId;
use ::types_error::{PgError, PgResult};
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::RTEPermissionInfo;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_portal::{ParamListHandle, QueryDescHandle};
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsNoMovement};
use ::types_slot::{
    SlotData, EXEC_FLAG_BACKWARD, EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_SKIP_TRIGGERS,
};
use ::types_tuple::TupleDescData;

use crate::procnode::{
    exec_end_node, exec_init_node, exec_proc_node, exec_shutdown_node, PlanStateNode,
};
use crate::querydesc::{self, ExecData, ExecTy, ExecutorHandle, QueryDescData};

// ExecutorStart/Run/Finish/End_hook (hook-surface.md section 2). Empty by
// default (S1 ships no consumer); the QueryDescHandle is the cheap identity
// a future pgss buckets counters on (queryId lives behind it). Ratified
// 2026-07-08: a parked-portal rearm (executor_rearm_seam below) has no C
// counterpart and would otherwise be invisible to a counting consumer of
// tap_executor_start — the rearm site fires it too on a successful reuse, so
// a future pgss sees one "start" per user-visible execution regardless of
// path.
seam_core::tap!(pub fn tap_executor_start(h: QueryDescHandle));
seam_core::tap!(pub fn tap_executor_run(h: QueryDescHandle));
seam_core::tap!(pub fn tap_executor_finish(h: QueryDescHandle));
seam_core::tap!(pub fn tap_executor_end(h: QueryDescHandle));
// _leave taps: C consumers wrap standard_ExecutorRun/Finish in PG_TRY to
// track nesting depth; the seam guarantees the leave fires on the error path
// too (PG_FINALLY parity).
seam_core::tap!(pub fn tap_executor_run_leave(h: QueryDescHandle));
seam_core::tap!(pub fn tap_executor_finish_leave(h: QueryDescHandle));

// One parked ExecutorState context (C's context_freelists): raw pointer keeps
// the TLS payload !needs_drop; nested executors overflow to a plain delete.
mod exec_ctx_pool {
    use ::mcx::MemoryContext;

    thread_local! {
        static SLOT: core::cell::Cell<*mut MemoryContext> =
            const { core::cell::Cell::new(core::ptr::null_mut()) };
        static TEARDOWN_REGISTERED: core::cell::Cell<bool> =
            const { core::cell::Cell::new(false) };
    }

    pub(crate) fn take() -> Option<Box<MemoryContext>> {
        let p = SLOT.with(|s| s.replace(core::ptr::null_mut()));
        // SAFETY: parked via Box::into_raw below; slot nulled above (sole owner).
        (!p.is_null()).then(|| unsafe { Box::from_raw(p) })
    }

    pub(crate) fn park(ctx: Box<MemoryContext>) {
        // Session-memory teardown (FPBUDGET-1): a parked skeleton context
        // must not outlive its session thread.
        if !TEARDOWN_REGISTERED.replace(true) {
            ::mcx::register_session_cleanup(Box::new(|| drop(take())));
        }
        let old = SLOT.with(|s| s.replace(Box::into_raw(ctx)));
        if !old.is_null() {
            // SAFETY: parked via Box::into_raw; displaced (nested executor) — delete.
            drop(unsafe { Box::from_raw(old) });
        }
    }
}

// One parked executor skeleton for a cached plan (no C counterpart: C
// rebuilds the whole executor state per EXECUTE). v2 scope: SELECT plans
// over Result/Limit/SeqScan/IndexScan/IndexOnlyScan trees, extern params
// allowed; no initplans/subplans, no instrumentation. The estate + planstate
// + compiled expressions stay wired; everything per-run is redone on reuse:
// permission checks, snapshot registration, relation pins, scan descriptors,
// param values (restamped into the estate-stable buffer), rescan. The parked
// entry pins its plan with a plancache refcount; a key mismatch discards it.
mod exec_skeleton {
    use std::rc::Rc;

    use ::types_portal::CachedPlanHandle;
    use ::types_tuple::TupleDescData;

    use crate::querydesc::ExecutorHandle;

    pub(crate) struct Skeleton {
        pub pstmt: *const (),
        pub cplan: CachedPlanHandle,
        pub eflags: i32,
        pub exec: Box<ExecutorHandle>,
        pub tup_desc: Rc<TupleDescData<'static>>,
    }

    thread_local! {
        // Raw pointer keeps the TLS payload !needs_drop (leak at backend
        // exit matches C's memory-context lifetime).
        static SLOT: core::cell::Cell<*mut Skeleton> =
            const { core::cell::Cell::new(core::ptr::null_mut()) };
    }

    pub(crate) fn take_if_match(
        pstmt: *const (),
        cplan: CachedPlanHandle,
        eflags: i32,
    ) -> Option<Skeleton> {
        let p = SLOT.with(|s| s.get());
        if p.is_null() {
            return None;
        }
        // SAFETY: parked via Box::into_raw below; slot nulled before the box
        // leaves this module.
        let matches = unsafe { (*p).pstmt == pstmt && (*p).cplan == cplan && (*p).eflags == eflags };
        if !matches {
            return None;
        }
        SLOT.with(|s| s.set(core::ptr::null_mut()));
        // SAFETY: sole owner (slot nulled above).
        let sk = unsafe { Box::from_raw(p) };
        // The running portal holds its own plan refcount; drop the pin here
        // and re-take it if the skeleton parks again.
        plancache_portal_seams::release_cached_plan::call(sk.cplan);
        Some(*sk)
    }

    pub(crate) fn park(sk: Skeleton) {
        // Session-memory teardown (FPBUDGET-1): the parked skeleton (whole
        // executor bundle + plancache pin) must not outlive its session.
        thread_local! {
            static TEARDOWN_REGISTERED: core::cell::Cell<bool> =
                const { core::cell::Cell::new(false) };
        }
        if !TEARDOWN_REGISTERED.replace(true) {
            ::mcx::register_session_cleanup(Box::new(|| {
                let p = SLOT.with(|s| s.replace(core::ptr::null_mut()));
                if !p.is_null() {
                    // SAFETY: parked via Box::into_raw; slot nulled (sole owner).
                    let sk = unsafe { Box::from_raw(p) };
                    plancache_portal_seams::release_cached_plan::call(sk.cplan);
                    drop(sk);
                }
            }));
        }
        plancache_portal_seams::incr_cached_plan::call(sk.cplan);
        let old = SLOT.with(|s| s.replace(Box::into_raw(Box::new(sk))));
        if !old.is_null() {
            // SAFETY: parked via Box::into_raw; displaced — release the pin
            // and drop the executor bundle on its normal (non-arena) path.
            let old = unsafe { Box::from_raw(old) };
            plancache_portal_seams::release_cached_plan::call(old.cplan);
            drop(old);
        }
    }
}

// pg_am.dat btree (transcribed like execexpr's ACL_EXECUTE): index_parkscan
// keeps the parked scan descriptor's AM workspace, btree-only.
const BTREE_AM_OID: ::types_core::Oid = 403;

// Skeleton walk whitelist: every node type here can be fully disarmed at
// park (no per-run state survives) and re-armed at reuse. Anything else
// takes the normal teardown path.
fn skeleton_parkable(node: &PlanStateNode<'_>) -> bool {
    match node {
        PlanStateNode::Result(rs) => rs.outer.as_deref().is_none_or(skeleton_parkable),
        PlanStateNode::Limit(l) => skeleton_parkable(&l.outer),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::skeleton_parkable(ss),
        PlanStateNode::IndexScan(is) => {
            is.iss_RelationDesc.as_ref().is_some_and(|r| r.rd_rel.relam == BTREE_AM_OID)
        }
        PlanStateNode::IndexOnlyScan(ios) => {
            ios.ioss_RelationDesc.as_ref().is_some_and(|r| r.rd_rel.relam == BTREE_AM_OID)
        }
        _ => false,
    }
}

// Park-side disarm: quiesce scan descriptors and release node-held relation
// pins; runs only after skeleton_parkable admitted the whole tree.
fn skeleton_park_tree(node: &mut PlanStateNode<'_>) -> PgResult<()> {
    match node {
        PlanStateNode::Result(rs) => match rs.outer.as_deref_mut() {
            Some(outer) => skeleton_park_tree(outer),
            None => Ok(()),
        },
        PlanStateNode::Limit(l) => skeleton_park_tree(&mut l.outer),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::skeleton_park(ss),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::skeleton_park(is),
        PlanStateNode::IndexOnlyScan(ios) => ::nodeindexonlyscan::skeleton_park(ios),
        _ => unreachable!("skeleton_park_tree on a non-parkable node"),
    }
}

// Reuse-side re-arm: re-pin relations per execution (C re-runs the
// ExecInit* open paths); scan descriptors re-open lazily with the fresh
// snapshot; the exec_re_scan pass after this re-evaluates runtime keys.
fn skeleton_rebind_tree<'mcx>(
    node: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match node {
        PlanStateNode::Result(rs) => match rs.outer.as_deref_mut() {
            Some(outer) => skeleton_rebind_tree(outer, estate),
            None => Ok(()),
        },
        PlanStateNode::Limit(l) => skeleton_rebind_tree(&mut l.outer, estate),
        PlanStateNode::SeqScan(ss) => ::nodeseqscan::skeleton_rebind(ss, estate),
        PlanStateNode::IndexScan(is) => ::nodeindexscan::skeleton_rebind(is, estate),
        PlanStateNode::IndexOnlyScan(ios) => ::nodeindexonlyscan::skeleton_rebind(ios, estate),
        _ => unreachable!("skeleton_rebind_tree on a non-parkable node"),
    }
}

// Re-arm a retained initialized executor for a fresh execution; everything
// C's standard_ExecutorStart re-derives per cached-plan execution is redone
// here: permission checks, param restamp, snapshot registration, relation
// re-pins, scan re-arm. false = param shape/type mismatch (caller builds
// fresh; that compile re-runs C's checks and errors with C's texts).
#[inline(never)]
fn skeleton_rearm_exec(qd: &mut QueryDescData, exec: &mut ExecutorHandle) -> PgResult<bool> {
    // P1: C's InitPlan runs ExecCheckPermissions on every cached-plan
    // execution — reuse must too (REVOKE/SET ROLE between EXECUTEs).
    exec_check_permissions(qd.plannedstmt())?;
    let source_text = qd.source_text();
    // SAFETY: the registered params live in the portal context, which
    // outlives this execution (values are copied out below).
    let new_params =
        (!qd.params.is_null()).then(|| unsafe { types_portal::params::resolve(qd.params) });
    let snapshot = qd.snapshot.clone();
    let reused = exec.with_mut(|data| -> PgResult<bool> {
        let ExecData { estate, planstate } = data;
        if !estate.param_stable_restamp(new_params) {
            return Ok(false);
        }
        let es_snapshot = snapmgr::RegisterSnapshot(snapshot.as_ref())?;
        estate.es_snapshot = es_snapshot;
        estate.es_sourceText = Some(source_text);
        estate.es_processed = 0;
        estate.es_total_processed = 0;
        estate.es_finished = false;
        let ps = planstate.as_mut().expect("skeleton holds a plan state");
        skeleton_rebind_tree(ps, estate)?;
        crate::execami::exec_re_scan(ps, estate)?;
        Ok(true)
    })?;
    if reused {
        qd.already_executed = false;
    }
    Ok(reused)
}

// Portal-retention reuse: the QueryDesc kept its executor across statements
// (parked in place at ExecutorFinish time); rearm it under this execution's
// snapshot and params.
pub(crate) fn executor_rearm_seam(
    h: QueryDescHandle,
    snapshot: Option<::snapmgr::Snapshot>,
    params: ParamListHandle,
) -> PgResult<bool> {
    let reused = querydesc::with_qd(h, |qd| {
        backend_status_seams::pgstat_report_query_id::call(qd.plannedstmt().queryId.get(), false);
        // CreateQueryDesc parity: the QueryDesc owns a registration on its
        // snapshot for the life of this execution.
        qd.snapshot = snapmgr::RegisterSnapshot(snapshot.as_ref())?;
        qd.params = params;
        let mut exec = qd.exec.take().expect("executor_rearm on a QueryDesc with no executor");
        let reused = skeleton_rearm_exec(qd, &mut exec);
        qd.exec = Some(exec);
        if let Ok(false) = reused {
            // Fallback path releases this QueryDesc without FreeQueryDesc:
            // drop the registration taken above.
            snapmgr::UnregisterSnapshot(qd.snapshot.take().as_ref());
        }
        reused
    })?;
    // Ratified 2026-07-08: a rearm has no C counterpart and bypasses
    // executor_start_seam entirely, so a counting consumer of
    // tap_executor_start would silently undercount reused executions of a
    // parked prepared statement unless the tap fires here too.
    if reused {
        tap_executor_start::call_if(|f| f(h));
    }
    Ok(reused)
}

// Portal-retention park: ExecutorFinish + the skeleton disarm, leaving the
// executor attached to the QueryDesc (the TLS-slot variant in
// standard_executor_end moves it out instead). false = ran the normal
// ExecutorEnd + free path; the caller's cleanup hook is consumed either way.
pub(crate) fn executor_finish_and_park_seam(h: QueryDescHandle) -> PgResult<bool> {
    // This path replaces ExecutorFinish + ExecutorEnd for parked portals, so
    // it must fire the same taps the split seams do or a consumer
    // (pg_stat_statements) silently loses these executions.
    tap_executor_finish::call_if(|f| f(h));
    let r = (|| -> PgResult<()> {
        let fire_triggers = querydesc::with_qd(h, standard_executor_finish)?;
        if fire_triggers {
            ::trigger::AfterTriggerEndQuery()?;
        }
        Ok(())
    })();
    tap_executor_finish_leave::call_if(|f| f(h));
    r?;
    tap_executor_end::call_if(|f| f(h));
    let parked = querydesc::with_qd(h, |qd| -> PgResult<bool> {
        if skeleton_disarm_in_place(qd)?.is_none() {
            standard_executor_end(qd)?;
            return Ok(false);
        }
        // CreateQueryDesc registered this snapshot; the parked QueryDesc
        // skips FreeQueryDesc, so release it here (rearm re-registers).
        snapmgr::UnregisterSnapshot(qd.snapshot.take().as_ref());
        qd.params = ParamListHandle::NULL;
        Ok(true)
    })?;
    if !parked {
        querydesc::free_query_desc_seam(h);
    }
    Ok(parked)
}

// Retained-executor arena cap (see the growth-bound comment below): generous
// vs a healthy parkable estate (Result/Limit/scan trees measure <100KB), so
// only pathological per-execution growth trips it. 256KiB caps worst-case
// retention at PARKED_PORTAL_MAX+1 shells per backend while keeping the
// rebuild amortization negligible (a 600B/exec grower rebuilds every ~400
// executions).
const SKELETON_RETAIN_MAX_BYTES: usize = 256 * 1024;

// PROCPERF P2 compile-economy threshold (see the economy_window call site in
// standard_executor_start): plans whose total_cost is below this run their
// expression compiles without the per-row-payoff ready passes. 0.0 disables.
// Latched once per process.
fn execexpr_economy_threshold() -> f64 {
    static T: pgsync::OnceLock<f64> = pgsync::OnceLock::new();
    crate::once_val(&T, || match std::env::var("PGRUST_EXECEXPR_ECONOMY") {
        Err(_) => 1000.0,
        Ok(v) => match v.trim() {
            "" => 1000.0,
            "0" | "off" | "false" => 0.0,
            s => s.parse().unwrap_or(1000.0),
        },
    })
}

// replanfix increment-1 kill switch: PGRUST_EXEC_SKELETON_CUSTOM_GATE=0
// restores the pre-gate behavior (custom plans pay skeleton-candidate
// ceremony at executor start). Latched once per process.
fn skeleton_custom_gate_disabled() -> bool {
    static DISABLED: pgsync::OnceLock<bool> = pgsync::OnceLock::new();
    crate::once_val(&DISABLED, || {
        matches!(
            std::env::var("PGRUST_EXEC_SKELETON_CUSTOM_GATE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

// Park-side disarm on the QueryDesc's own executor: the eligibility gates and
// per-run-state release of standard_executor_end's TLS-park branch, in place.
#[inline(never)]
fn skeleton_disarm_in_place(qd: &mut QueryDescData) -> PgResult<Option<i32>> {
    if qd.cplan.is_null()
        || qd.operation != CmdType::CMD_SELECT
        || qd.instrument_options != 0
        || !qd.query_env.is_null()
        || qd.crosscheck_snapshot.is_some()
        || qd.tup_desc.is_none()
        // A one-shot custom plan never comes back from GetCachedPlan: reuse
        // keys on the exact CachedPlan, so parking it can never hit and only
        // pins the custom plan's arena until displacement. is_installed:
        // test fixtures shim only the seams they use.
        || !plancache_portal_seams::is_source_generic_plan::is_installed()
        || !plancache_portal_seams::is_source_generic_plan::call(qd.cplan)
    {
        return Ok(None);
    }
    let Some(exec) = qd.exec.as_mut() else { return Ok(None) };
    // Retention growth bound: a parked estate's bump arena is never reset
    // while the skeleton lives (the planstate is allocated in it), so any
    // per-execution allocation routed through es_query_cxt accumulates
    // across reuses. C frees the ExecutorState on every execution; retention
    // is only sound if the arena stays at its post-first-execution size.
    // Statements whose executions grow the arena (e.g. PL/pgSQL function
    // calls: SELECT f(...) under a generic plan — the stored-proc OLTP P1 leak,
    // notes/memleak-tpcc-lane.md) get their executor torn down on the normal
    // reset/recycle path once the arena crosses the cap; non-growing
    // statements (point selects) park forever and pay only this load+cmp.
    if exec.context().used() > SKELETON_RETAIN_MAX_BYTES {
        return Ok(None);
    }
    exec.with_mut(|data| -> PgResult<Option<i32>> {
        let ExecData { estate, planstate } = data;
        let eligible = planstate.is_some()
            && estate.es_subplanstates.is_empty()
            && estate.es_subplan_expr_states.is_empty()
            && estate.es_param_exec_vals.is_empty()
            && estate.es_result_relations.is_empty()
            && estate.es_rowmarks.is_empty()
            && estate.es_part_prune_results.is_empty()
            && estate.es_epq.is_none()
            && estate.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY == 0
            && estate.es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS != 0
            && estate.es_instrument == 0
            && estate.es_crosscheck_snapshot.is_none()
            && skeleton_parkable(planstate.as_ref().expect("probed above"));
        if !eligible {
            return Ok(None);
        }
        skeleton_park_tree(planstate.as_mut().expect("probed above"))?;
        // Relations close per run, exactly as C's ExecutorEnd (locks are
        // kept; the next execution re-acquires via AcquireExecutorLocks).
        estate.exec_close_range_table_relations()?;
        let mcx = estate.es_query_cxt;
        for slot in estate.es_tupleTable.iter_mut() {
            ::exectuples::exec_clear_tuple(slot, mcx);
            // Materialize scratch can come from the statement's own
            // context (dest receivers); never park it.
            ::exectuples::exec_drop_slot_scratch(slot, mcx);
        }
        snapmgr::UnregisterSnapshot(estate.es_snapshot.take().as_ref());
        // The source text lives in the portal, freed before the skeleton
        // is reused; never hold it across the park.
        estate.es_sourceText = None;
        Ok(Some(estate.es_top_eflags))
    })
}

pub(crate) fn executor_start_seam(h: QueryDescHandle, eflags: i32) -> PgResult<()> {
    tap_executor_start::call_if(|f| f(h));
    querydesc::with_qd(h, |qd| {
        backend_status_seams::pgstat_report_query_id::call(qd.plannedstmt().queryId.get(), false);
        standard_executor_start(qd, eflags)
    })
}

// ---------------------------------------------------------------------------
// SERIAL LEASE — v2 engine lives in crate::slease (GL-SLEASE-2: floor-gated
// admission + I/O donation; the module doc there is the design authority).
// This file keeps only the RAII bracket in executor_run_seam below and the
// engagement-yield re-export for lanev2's standing channel.
// ---------------------------------------------------------------------------
pub(crate) use crate::slease::{serial_lease_yield_for_engagement, SerialLease};

/// GL-STMTTASK-2 change 3 × GL-SLEASE-2 (t44 composition bridge): true ⇔
/// THIS top-level run currently HOLDS a serial-lease permit — the session
/// is already seat-accounted, so the inline-execute path must not borrow a
/// SECOND seat (double-count; a saturated pool would refuse inline for
/// exactly the sessions the lease already admitted). Under the v2 floor
/// semantics a sub-floor run is deliberately permit-less (S_PENDING) and
/// returns false — the inline borrow IS its seat accounting until the
/// floor crosses. Implemented on the v2 state authority in crate::slease
/// (stmt-task-2 was written against the v1 execmain TLS the v2 module
/// replaced).
pub(crate) use crate::slease::serial_lease_currently_held;

pub(crate) fn executor_run_seam(
    h: QueryDescHandle,
    direction: ScanDirection,
    count: u64,
    dest: &mut DestReceiver<'_>,
) -> PgResult<()> {
    tap_executor_run::call_if(|f| f(h));
    let _lease = SerialLease::enter();
    let r = querydesc::with_qd(h, |qd| standard_executor_run(qd, direction, count, dest));
    tap_executor_run_leave::call_if(|f| f(h));
    r
}

pub(crate) fn executor_finish_seam(h: QueryDescHandle) -> PgResult<()> {
    tap_executor_finish::call_if(|f| f(h));
    // The registry borrow must drop before the after-trigger firing loop:
    // RI checks re-enter the executor through SPI (fresh QueryDesc entries).
    // C divergence: C runs AfterTriggerEndQuery inside the totaltime
    // Instr window (standard_ExecutorFinish); here it falls outside, so a
    // consumer's per-statement time/bufusage exclude after-trigger work.
    let r = (|| -> PgResult<()> {
        let fire_triggers = querydesc::with_qd(h, standard_executor_finish)?;
        if fire_triggers {
            ::trigger::AfterTriggerEndQuery()?;
        }
        Ok(())
    })();
    tap_executor_finish_leave::call_if(|f| f(h));
    r
}

/// `ExecutorRewind` (execMain.c).
pub(crate) fn executor_rewind_seam(h: QueryDescHandle) -> PgResult<()> {
    querydesc::with_qd(h, |qd| {
        debug_assert_eq!(qd.operation, CmdType::CMD_SELECT);
        let exec = qd.exec.as_mut().expect("ExecutorRewind before ExecutorStart");
        exec.with_mut(|data| {
            let ExecData { estate, planstate } = data;
            let ps = planstate.as_mut().expect("ExecutorRewind without a plan state");
            crate::execami::exec_re_scan(ps, estate)
        })
    })
}

pub(crate) fn executor_end_seam(h: QueryDescHandle) -> PgResult<()> {
    tap_executor_end::call_if(|f| f(h));
    querydesc::with_qd(h, standard_executor_end)
}

#[track_caller]
#[cold]
#[inline(never)]
fn unrecognized_operation(operation: CmdType) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "unrecognized operation code: {}",
        operation as i32
    )))
}

// acl.h values (transcribed like execexpr's ACL_EXECUTE).
const ACL_INSERT: u64 = 1 << 0;
const ACL_SELECT: u64 = 1 << 1;
const ACLCHECK_OK: i32 = 0;

// ExecCheckXactReadOnly (execMain.c); temp-table writes pass (session-local).
fn exec_check_xact_read_only(pstmt: &PlannedStmt<'_>) -> PgResult<()> {
    for pi_node in pstmt.permInfos.iter() {
        let pi = pi_node.as_rte_permission_info().expect("permInfos cell");
        if pi.requiredPerms & !ACL_SELECT == 0 {
            continue;
        }
        let namespace_id = syscache_seams::lookup_pg_class_ls_shape::call(pi.relid)?
            .map(|s| s.relnamespace)
            .unwrap_or(::types_core::InvalidOid);
        if namespace_seams::is_temp_namespace::call(namespace_id) {
            continue;
        }
        xact::PreventCommandIfReadOnly(create_command_name(pstmt))?;
    }
    if pstmt.commandType != CmdType::CMD_SELECT || pstmt.hasModifyingCTE {
        xact::PreventCommandIfParallelMode(create_command_name(pstmt))?;
    }
    Ok(())
}

// CreateCommandName over a PlannedStmt: the CreateCommandTag commandType arm.
fn create_command_name(pstmt: &PlannedStmt<'_>) -> &'static str {
    match pstmt.commandType {
        CmdType::CMD_SELECT => "SELECT",
        CmdType::CMD_INSERT => "INSERT",
        CmdType::CMD_UPDATE => "UPDATE",
        CmdType::CMD_DELETE => "DELETE",
        CmdType::CMD_MERGE => "MERGE",
        _ => "???",
    }
}

/// `ExecCheckPermissions` (ereport_on_violation arm only; no hook). Hot path
/// (InitPlan/skeleton_rearm_exec): a direct `&PlannedStmt` parameter, not the
/// bare permInfos list, keeps this identical to C's call shape and avoids an
/// inlining shift from routing every call through the field-extraction seam.
pub(crate) fn exec_check_permissions(pstmt: &PlannedStmt<'_>) -> PgResult<()> {
    exec_check_permissions_over_perminfos(&pstmt.permInfos)
}

/// Bare-permInfos-list variant backing the `execmain_seams::exec_check_permissions`
/// seam: COPY (copy.c DoCopy) checks per-column privileges without a
/// PlannedStmt.
pub(crate) fn exec_check_permissions_over_perminfos(
    perm_infos: &::types_nodes::NodeList<'_>,
) -> PgResult<()> {
    for pi_node in perm_infos.iter() {
        let pi = pi_node.as_rte_permission_info().expect("permInfos cell");
        debug_assert!(pi.relid != 0);
        if !exec_check_one_rel_perms(pi)? {
            permission_denied(pi.relid)?;
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn permission_denied(relid: ::types_core::Oid) -> PgResult<()> {
    use types_nodes::parsenodes::ObjectType;
    // aclcheck_error(ACLCHECK_NO_PRIV, get_relkind_objtype(get_rel_relkind()),
    // get_rel_name()).
    const RELKIND_SEQUENCE: i8 = b'S' as i8;
    const RELKIND_VIEW: i8 = b'v' as i8;
    const RELKIND_MATVIEW: i8 = b'm' as i8;
    const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
    let shape = syscache_seams::lookup_pg_class_ls_shape::call(relid)?;
    let objtype = match shape.map(|s| s.relkind) {
        Some(RELKIND_SEQUENCE) => ObjectType::OBJECT_SEQUENCE,
        Some(RELKIND_VIEW) => ObjectType::OBJECT_VIEW,
        Some(RELKIND_MATVIEW) => ObjectType::OBJECT_MATVIEW,
        Some(RELKIND_FOREIGN_TABLE) => ObjectType::OBJECT_FOREIGN_TABLE,
        _ => ObjectType::OBJECT_TABLE,
    };
    let name = syscache_seams::pg_class_relname::call(relid)?;
    let name = name
        .as_ref()
        .map(|n| core::str::from_utf8(n.name_str()).unwrap_or(""))
        .unwrap_or("");
    aclchk_seams::aclcheck_error::call(1, objtype as i32, name)
}

/// `ExecCheckOneRelPerms` (execMain.c). Exported for subquery_planner's
/// planner-startup view permission check.
pub fn exec_check_one_rel_perms(pi: &RTEPermissionInfo<'_>) -> PgResult<bool> {
    use types_nodes::parsenodes::ACL_UPDATE;
    const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 = -7;

    let required = pi.requiredPerms;
    debug_assert!(required != 0);
    let userid = if pi.checkAsUser != 0 {
        pi.checkAsUser
    } else {
        miscinit_seams::get_user_id::call()
    };

    let rel_perms = aclchk_seams::pg_class_aclmask::call(pi.relid, userid, required, true)?;
    let remaining = required & !rel_perms;
    if remaining == 0 {
        return Ok(true);
    }

    // Only SELECT/INSERT/UPDATE can be satisfied at column level.
    if remaining & !(ACL_SELECT | ACL_INSERT | ACL_UPDATE) != 0 {
        return Ok(false);
    }

    if remaining & ACL_SELECT != 0 {
        // No column referenced (e.g. count(*)): SELECT on any column will do.
        if pi.selectedCols.is_empty()
            && aclchk_seams::pg_attribute_aclcheck_all::call(pi.relid, userid, ACL_SELECT, false)?
                != ACLCHECK_OK
        {
            return Ok(false);
        }
        let mut col = -1i32;
        loop {
            col = pi.selectedCols.next_member(col);
            if col < 0 {
                break;
            }
            let attno = col + FIRST_LOW_INVALID_HEAP_ATTNUM;
            if attno == 0 {
                // Whole-row reference: need SELECT on all columns.
                if aclchk_seams::pg_attribute_aclcheck_all::call(
                    pi.relid, userid, ACL_SELECT, true,
                )? != ACLCHECK_OK
                {
                    return Ok(false);
                }
            } else if aclchk_seams::pg_attribute_aclcheck::call(
                pi.relid,
                attno as i16,
                userid,
                ACL_SELECT,
            )? != ACLCHECK_OK
            {
                return Ok(false);
            }
        }
    }

    if remaining & ACL_INSERT != 0
        && !exec_check_permissions_modified(pi.relid, userid, &pi.insertedCols, ACL_INSERT)?
    {
        return Ok(false);
    }
    if remaining & ACL_UPDATE != 0
        && !exec_check_permissions_modified(pi.relid, userid, &pi.updatedCols, ACL_UPDATE)?
    {
        return Ok(false);
    }
    Ok(true)
}

/// `ExecCheckPermissionsModified` (execMain.c).
fn exec_check_permissions_modified(
    relid: ::types_core::Oid,
    userid: ::types_core::Oid,
    modified_cols: &::types_nodes::Bitmapset<'_>,
    required_perms: u64,
) -> PgResult<bool> {
    const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 = -7;
    // No explicit column list (SELECT FOR UPDATE, corner-case UPDATEs):
    // permission on any column suffices.
    if modified_cols.is_empty() {
        return Ok(aclchk_seams::pg_attribute_aclcheck_all::call(
            relid,
            userid,
            required_perms,
            false,
        )? == ACLCHECK_OK);
    }
    let mut col = -1i32;
    loop {
        col = modified_cols.next_member(col);
        if col < 0 {
            break;
        }
        let attno = col + FIRST_LOW_INVALID_HEAP_ATTNUM;
        if attno == 0 {
            return Err(Box::new(PgError::error(
                "whole-row update is not implemented".to_string(),
            )));
        }
        if aclchk_seams::pg_attribute_aclcheck::call(relid, attno as i16, userid, required_perms)?
            != ACLCHECK_OK
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// `standard_ExecutorStart` (execMain.c).
pub fn standard_executor_start(qd: &mut QueryDescData, mut eflags: i32) -> PgResult<()> {
    assert!(qd.exec.is_none(), "ExecutorStart: query already started");
    // WS-P node-census entry hook (wave-2 flip machinery, lanev2/census.rs):
    // when PGRUST_LANE_V2_NODE_CENSUS is armed, ride WS-C's EngineEvent
    // capture (attribution-only; emission-gate law in executils) so the
    // ExecutorEnd census can join plan nodes to their engine verdicts.
    // Disarmed cost: one memoized-bool load + branch (default OFF; flagged
    // for the select1 instruction-pair fleet gate in notes/se-ws-p-flip.md).
    // Before the skeleton probe on purpose: the flag participates in the
    // skeleton's eflags match key like every other entry flag.
    if crate::lanev2::census_armed() {
        eflags |= ::types_slot::EXEC_FLAG_ENGINE_REPORT;
    }
    #[cfg(debug_assertions)]
    if let Some(s) = &qd.snapshot {
        if snapmgr::ActiveSnapshotSet() {
            debug_assert!(Rc::ptr_eq(s, &snapmgr::GetActiveSnapshot()));
        }
    }
    let pstmt = qd.plannedstmt();

    // M5 unified admission router, query-start decision (docs/design/
    // m5-planner.md §2): under pgrust.parallel_engine=runtime resolve the
    // engine once — no pool degrades to legacy with a loud-once LOG line
    // (M5-0); the routing decision is counted per query (M5-1): a legacy
    // parallel plan (Gather machinery) executes on the legacy engine
    // byte-untouched (the runtime arm shapes are disjoint from Gather plans
    // by construction until M5-3's suppression), a serial-shaped plan
    // routes to the arm offers at their sites. One TLS read + cmp on the
    // legacy default (measured +8 instr/q on select1).
    crate::lanev2::router_query_start(pstmt.parallelModeNeeded);

    if (guc_tables::vars::XactReadOnly.read() || xact::IsInParallelMode())
        && eflags & EXEC_FLAG_EXPLAIN_ONLY == 0
    {
        exec_check_xact_read_only(pstmt)?;
    }

    let query_env = qd.query_env;

    let mut output_cid: CommandId = 0;
    match qd.operation {
        CmdType::CMD_SELECT => {
            if !pstmt.rowMarks.is_nil() || pstmt.hasModifyingCTE {
                output_cid = xact::GetCurrentCommandId(true)?;
            }
            if !pstmt.hasModifyingCTE {
                eflags |= EXEC_FLAG_SKIP_TRIGGERS;
            }
        }
        CmdType::CMD_INSERT | CmdType::CMD_DELETE | CmdType::CMD_UPDATE | CmdType::CMD_MERGE => {
            output_cid = xact::GetCurrentCommandId(true)?;
        }
        other => return Err(unrecognized_operation(other)),
    }

    let skeleton_candidate = !qd.cplan.is_null()
        && qd.operation == CmdType::CMD_SELECT
        && qd.instrument_options == 0
        && query_env.is_null()
        && qd.crosscheck_snapshot.is_none();

    if skeleton_candidate {
        if let Some(sk) =
            exec_skeleton::take_if_match(qd.plannedstmt() as *const _ as *const (), qd.cplan, eflags)
        {
            let mut exec = sk.exec;
            let reused = skeleton_rearm_exec(qd, &mut exec)?;
            if reused {
                qd.tup_desc = Some(sk.tup_desc);
                qd.exec = Some(exec);
                return Ok(());
            }
            // Param mismatch: discard the skeleton (displacement path) and
            // build fresh.
            drop(exec);
        }
    }

    let es_snapshot = snapmgr::RegisterSnapshot(qd.snapshot.as_ref())?;
    let es_crosscheck = snapmgr::RegisterSnapshot(qd.crosscheck_snapshot.as_ref())?;

    if eflags & (EXEC_FLAG_SKIP_TRIGGERS | EXEC_FLAG_EXPLAIN_ONLY) == 0 {
        ::trigger::AfterTriggerBeginQuery();
    }

    let source_text = qd.source_text();
    let instrument = qd.instrument_options;
    let operation = qd.operation;
    let params = qd.params;
    // One-shot CUSTOM plans can never hit the skeleton slot (reuse keys on
    // the exact CachedPlan; BuildCachedPlan mints a fresh handle per replan)
    // and the park side already refuses them (skeleton_disarm_in_place /
    // try_park) — so don't pay the estate-owned param_stable_install copy on
    // their behalf. Checked HERE, on the build-fresh path only, so a generic
    // skeleton HIT (the prepared-statement hot loop, already returned above)
    // pays nothing new; the seam call lands once per generic build/displace
    // and once per custom execution, where it buys back the param copy.
    // Same predicate the park gate trusts; is_installed: test fixtures shim
    // only the seams they use. Kill switch PGRUST_EXEC_SKELETON_CUSTOM_GATE=0
    // restores pre-gate behavior (customs pay the stable-copy again).
    let skeleton_stable_params = skeleton_candidate
        && (skeleton_custom_gate_disabled()
            || (plancache_portal_seams::is_source_generic_plan::is_installed()
                && plancache_portal_seams::is_source_generic_plan::call(qd.cplan)));

    let ctx = exec_ctx_pool::take()
        .unwrap_or_else(|| Box::new(MemoryContext::new_bump("ExecutorState")));
    let mut exec = McxOwned::<ExecTy>::try_new_in_place_boxed(
        ctx,
        |mcx, slot| {
            let d = slot.as_mut_ptr();
            // SAFETY: field-wise init of the whole uninit slot; sret lands
            // EStateData directly in the arena (no ~1.2KB stack round trip).
            unsafe {
                (&raw mut (*d).estate).write(EStateData::new_in(mcx));
                (&raw mut (*d).planstate).write(None);
            }
            Ok(())
        },
    )?;
    let tup_desc = exec.with_mut_mcx(|_mcx, data| {
        // SAFETY: lifetime shortening of the read-only plan tree (PlannedStmt
        // is invariant only through its lists' GAT pointers); the retention
        // contract keeps it alive past this bundle (pquery::stmt_list shape).
        let pstmt = unsafe { querydesc::shorten_pstmt(pstmt) };
        let es = &mut data.estate;
        // SAFETY: the registered params live in the portal context, which
        // outlives this executor state (PortalDrop frees the handle after
        // PortalCleanup's ExecutorEnd).
        es.es_param_list_info = if params.is_null() {
            None
        } else {
            let src = unsafe { types_portal::params::resolve(params) };
            if skeleton_stable_params {
                // Parkable candidates compile ParamExtern steps against an
                // estate-owned copy, not the portal's per-EXECUTE array.
                // One-shot customs (never parked) reference the portal array
                // directly, like non-candidates.
                Some(es.param_stable_install(src)?)
            } else {
                Some(src)
            }
        };
        let n_exec = pstmt.paramExecTypes.len();
        if n_exec > 0 {
            es.es_param_exec_vals
                .try_reserve_exact(n_exec)
                .map_err(|_| _mcx.oom(n_exec))?;
            es.es_param_exec_vals
                .extend(core::iter::repeat_n(types_portal::params::ParamExecData::EMPTY, n_exec));
            es.es_param_subplans
                .try_reserve_exact(n_exec)
                .map_err(|_| _mcx.oom(n_exec))?;
            es.es_param_subplans.extend(core::iter::repeat_n(None, n_exec));
        }
        if !query_env.is_null() {
            // SAFETY: the registrant keeps the environment alive across this
            // query's execution (queryenvironment::hold contract).
            es.es_queryEnv = Some(unsafe { ::queryenvironment::hold::resolve(query_env) });
        }
        es.es_sourceText = Some(source_text);
        es.es_output_cid = output_cid;
        es.es_snapshot = es_snapshot;
        es.es_crosscheck_snapshot = es_crosscheck;
        es.es_top_eflags = eflags;
        es.es_instrument = instrument;
        // EXPLAIN (ENGINE) capture arm (single-executor Phase 0.2) costs this
        // entry path nothing: `engine_capture()` derives from the
        // EXEC_FLAG_ENGINE_REPORT bit in es_top_eflags (stored above
        // regardless), tested only at the lanev2 verdict chokepoints
        // (se-entrycost). False on every path but ExplainOnePlanRef with the
        // ENGINE option, so es_engine_events stays empty everywhere else
        // (the emission gate).
        es.es_jit_flags = pstmt.jitFlags;
        // PROCPERF P2 compile economy: OLTP-cheap statements recompile their
        // expression programs on every execution (SPI statements in stored
        // procedure bodies, unprepared point queries), and the per-row-payoff
        // ready passes (lane-v2 censuses + fusion peephole) never amortize at
        // point-plan row counts — C runs no equivalent work. Arm execexpr's
        // economy window over InitPlan when the planner's own work estimate
        // is below the threshold; same thread-local-window shape as the jit
        // flags below. Kill switch / tuning: PGRUST_EXECEXPR_ECONOMY=0
        // disables, =<cost> retunes (default 1000).
        let economy_threshold = execexpr_economy_threshold();
        let _economy = ::execexpr::economy_window(
            economy_threshold > 0.0
                && pstmt
                    .planTree
                    .and_then(|n| n.as_plan())
                    .is_some_and(|p| p.total_cost < economy_threshold),
        );
        // C jit_compile_expr reads es_jit_flags through the PlanState parent;
        // expression compile has no estate linkage here, so the flags ride a
        // thread-local window over InitPlan and the kernels come back through
        // the session collector onto the estate (C's es_jit JitContext).
        // Below the cost gate (jitFlags == 0) the window stays closed: the
        // select1/point compile path pays only this branch.
        let r = if pstmt.jitFlags == 0 {
            init_plan(data, pstmt, operation, eflags)
        } else {
            ::execexpr::jit::session_begin(pstmt.jitFlags);
            let r = init_plan(data, pstmt, operation, eflags);
            let jc = ::execexpr::jit::session_end();
            data.estate.es_jit_blocks = jc.blocks;
            data.estate.es_jit_instr = jc.instr;
            r
        };
        // se-delegtax SH-F: the row-mode LEAF fast-admit byte — computed
        // once here (all inputs per-execution static; see the refresh doc).
        crate::lanev2::refresh_lane_leaf_fast(&mut data.estate);
        r
    })?;
    qd.tup_desc = Some(tup_desc);
    qd.exec = Some(Box::new(exec));
    Ok(())
}

/// `InitPlan` (execMain.c).
pub(crate) fn init_plan<'mcx>(
    data: &mut ExecData<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
    operation: CmdType,
    eflags: i32,
) -> PgResult<Rc<TupleDescData<'static>>> {
    exec_check_permissions(pstmt)?;
    // C's bms_copy: the estate owns its pruning set (extended by ExecDoInitialPruning).
    let unpruned = pstmt
        .unprunableRelids
        .clone_in(data.estate.es_query_cxt)?;
    data.estate
        .exec_init_range_table(&pstmt.rtable, &pstmt.permInfos, unpruned)?;
    data.estate.es_plannedstmt = Some(pstmt);
    if !pstmt.partPruneInfos.is_nil() {
        ::execpartition::pruning::exec_do_initial_pruning(&mut data.estate)?;
    }
    if !pstmt.rowMarks.is_nil() {
        let estate = &mut data.estate;
        let n = estate.es_range_table_size as usize;
        estate.es_rowmarks.reserve(n);
        estate.es_rowmarks.extend(core::iter::repeat_n(None, n));
        for rc_node in &pstmt.rowMarks {
            let rc = rc_node.as_plan_row_mark().expect("rowMarks cell is a PlanRowMark");
            if rc.isParent {
                continue;
            }
            let rte = estate.exec_rt_fetch(rc.rti);
            if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
                && !estate.es_unpruned_relids.is_member(rc.rti as i32)
            {
                continue;
            }
            use types_nodes::plannodes::RowMarkType::*;
            match rc.markType {
                ROW_MARK_EXCLUSIVE | ROW_MARK_NOKEYEXCLUSIVE | ROW_MARK_SHARE
                | ROW_MARK_KEYSHARE | ROW_MARK_REFERENCE => {
                    let rel = estate.exec_get_range_table_relation(rc.rti, false)?;
                    check_valid_row_mark_rel(rel, rc.markType)?;
                }
                // C: no physical table access is required (relation = NULL).
                ROW_MARK_COPY => {}
            }
            let erm = ::executils::ExecRowMark {
                relid: rte.relid,
                rti: rc.rti,
                prti: rc.prti,
                rowmarkId: rc.rowmarkId,
                markType: rc.markType,
                strength: rc.strength,
                waitPolicy: rc.waitPolicy,
                ermActive: false,
                curCtid: ::types_tuple::ItemPointerData::default(),
            };
            let cell = &mut estate.es_rowmarks[(rc.rti - 1) as usize];
            debug_assert!(cell.is_none());
            *cell = Some(erm);
        }
    }
    if !pstmt.subplans.is_nil() {
        // Hooks precede the subplan-init loop: a subplan's own tree can hold
        // nested SubPlan expressions (plan_id strictly below its own, so the
        // es_subplanstates lookup mirrors C's incremental lappend order).
        // Subplan-free plans skip the install; whole-row eref aliasing gets
        // its SubplanCompileEnv from the rtable gate in executils instead.
        data.estate.es_subplan_hook = Some(crate::nodesubplan::subplan_hook);
        data.estate.es_cte_proc_hook = Some(crate::nodesubplan::cte_proc_hook);
        data.estate.es_subplan_init_hook = Some(crate::nodesubplan::subplan_expr_init_hook);
        data.estate.es_subplan_eval_hook = Some(crate::nodesubplan::subplan_expr_eval_hook);
        for (i, subplan) in pstmt.subplans.iter().enumerate() {
            let mut sp_eflags = eflags
                & !(types_slot::EXEC_FLAG_REWIND
                    | EXEC_FLAG_BACKWARD
                    | types_slot::EXEC_FLAG_MARK);
            if pstmt.rewindPlanIDs.is_member((i + 1) as i32) {
                sp_eflags |= types_slot::EXEC_FLAG_REWIND;
            }
            // A NULL cell is ExecSerializePlan's parallel-unsafe hole: no
            // init, and the None state makes any reference loud in
            // ExecInitSubPlan (C: ExecInitNode(NULL) == NULL).
            let ps = match subplan {
                Some(subplan) => Some(
                    exec_init_node(Some(subplan), &mut data.estate, sp_eflags)?
                        .expect("subplans cells are plan trees"),
                ),
                None => None,
            };
            // C registers inside ExecInitModifyTable (!canSetTag → lcons onto
            // es_auxmodifytables); a wCTE ModifyTable is always its subplan's
            // root here, so it registers where the state cell exists.
            let is_aux_mt = match &ps {
                Some(ps) => {
                    let mut root = ps;
                    if let crate::PlanStateNode::Instrumented(instr) = root {
                        root = &instr.inner;
                    }
                    matches!(root, crate::PlanStateNode::ModifyTable(m) if !m.mt.canSetTag)
                }
                None => false,
            };
            // Arena-cell ownership (not a struct field) so the type-erased
            // pointer never aliases a live &mut ExecData; the PlanState's Rc
            // releases run in standard_executor_end's explicit take+drop
            // (abort-path leak is the registry hazard class; see CATALOG).
            let mut cell = ::mcx::alloc_in(data.estate.es_query_cxt, ps)?;
            let raw: *mut Option<crate::PlanStateNode<'_>> = &mut *cell;
            core::mem::forget(cell);
            let cell = ::executils::SubplanStateCell(
                // SAFETY: raw comes from a live arena allocation.
                unsafe { core::ptr::NonNull::new_unchecked(raw) }.cast(),
            );
            data.estate.es_subplanstates.push(cell);
            if is_aux_mt {
                data.estate.es_auxmodifytables.push(cell);
            }
        }
    }

    let plan_node = pstmt.planTree.expect("PlannedStmt without planTree");
    let planstate = exec_init_node(Some(plan_node), &mut data.estate, eflags)?
        .expect("ExecInitNode of a non-NULL planTree");

    let plan = plan_node.as_plan().expect("planTree is a Plan node");
    let mut tup_type = planstate.exec_get_result_type(plan)?;

    if operation == CmdType::CMD_SELECT {
        // A parallel worker's junk columns must reach the leader: C clears
        // resjunk on a plan copy in ExecSerializePlan; the shared plan tree
        // is immutable here, so the filter is suppressed instead. The TLS
        // read hides behind the resjunk scan (junk tlists are the rare case).
        let junk_filter_needed = plan.targetlist.iter().any(|tle_node| {
            tle_node
                .as_target_entry()
                .expect("targetlist entry is a TargetEntry")
                .resjunk
        }) && !parallel::IsParallelWorker();
        if junk_filter_needed {
            let slot = data
                .estate
                .exec_init_extra_tuple_slot(None, types_slot::TupleSlotKind::Virtual);
            let clean = crate::exec_clean_type_from_tl(&plan.targetlist)?;
            tup_type = clean.clone();
            let j = execjunk::exec_init_junk_filter(
                &mut data.estate,
                &plan.targetlist,
                clean,
                slot,
            )?;
            data.estate.es_junkFilter = Some(j);
        }
    }

    data.planstate = Some(planstate);
    Ok(tup_type)
}

/// `standard_ExecutorRun` (execMain.c).
pub fn standard_executor_run<'m>(
    qd: &mut QueryDescData,
    direction: ScanDirection,
    count: u64,
    dest: &mut DestReceiver<'m>,
) -> PgResult<()> {
    let operation = qd.operation;
    let pstmt = qd.plannedstmt();
    let send_tuples = operation == CmdType::CMD_SELECT || pstmt.hasReturning;
    // C decides parallel mode and sets already_executed inside ExecutePlan
    // (execMain.c), so a NoMovement run does neither; hoisted here only
    // because `exec` borrows qd through the closure.
    let no_movement = ScanDirectionIsNoMovement(direction);
    let use_parallel_mode = if no_movement {
        false
    } else {
        let upm =
            if qd.already_executed || count != 0 { false } else { pstmt.parallelModeNeeded };
        qd.already_executed = true;
        upm
    };
    if let Some(t) = qd.totaltime.as_deref_mut() {
        ::instrument::instr_start_node(t);
    }
    let tup_desc = qd.tup_desc.clone();
    let exec = qd.exec.as_mut().expect("ExecutorRun before ExecutorStart");
    let nprocessed = exec.with_mut_mcx(|_mcx, data| {
        debug_assert!(data.estate.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY == 0);
        data.estate.es_processed = 0;
        if send_tuples {
            let desc = tup_desc.as_deref().expect("sendTuples without a result tupdesc");
            dest.startup(operation as i32, desc)?;
        }
        if !no_movement {
            execute_plan(data, operation, send_tuples, count, direction, use_parallel_mode, dest)?;
        }
        data.estate.es_total_processed += data.estate.es_processed;
        if send_tuples {
            dest.shutdown()?;
        }
        Ok::<u64, Box<types_error::PgError>>(data.estate.es_processed)
    })?;
    if let Some(t) = qd.totaltime.as_deref_mut() {
        ::instrument::instr_stop_node(t, nprocessed as f64);
    }
    Ok(())
}

/// `ExecutePlan` (execMain.c): THE per-tuple loop.
pub(crate) fn execute_plan<'m, 'mcx>(
    data: &mut ExecData<'mcx>,
    operation: CmdType,
    send_tuples: bool,
    number_tuples: u64,
    direction: ScanDirection,
    use_parallel_mode: bool,
    dest: &mut DestReceiver<'m>,
) -> PgResult<()> {
    let ExecData { estate, planstate } = data;
    let planstate = planstate.as_mut().expect("ExecutorRun without a plan state");
    estate.es_direction = direction;
    estate.es_use_parallel_mode = use_parallel_mode;
    // === wave-9 shared-file marker (contract §7; sub-regions AG, AH, AI, AJ) ===
    // (the execute_plan run seam — the §6 shared AI/AJ region; all four labels
    // per the §7 protocol so the integrator splice is purely mechanical)
    // --- WS-AG wave-9 sub-region (reserved) ------------------------------------
    // --- end WS-AG wave-9 -------------------------------------------------------
    // --- WS-AH wave-9 sub-region (reserved) ------------------------------------
    // --- end WS-AH wave-9 -------------------------------------------------------
    // --- WS-AI wave-9 (forward-pull cursors inc-1; contract §3, band 92001+) ---
    // Per-run emission budget, written UNCONDITIONALLY like es_direction
    // above it (None on knob-OFF and count-0 runs, which answer at the
    // callee's first test; a None overwrite means no stale budget survives
    // an error unwind or estate reuse). See lanev2/push.rs WS-AI region for
    // the gate + serial law. inc-1b: the install now runs the NAMED
    // cursor-admission classifier (forward / non-scroll eflags / serial;
    // refusals tick the ShapeClass::Cursor taxonomy, knob-ON only) and the
    // budgeted-run suspension SETTLES at the end of this function (the park
    // walker below the loop); a parked pipeline repossesses at the next
    // entry (the resume walk right here).
    estate.es_cursor_run_budget = crate::lanev2::cursor_run_budget_install(
        operation == CmdType::CMD_SELECT,
        ::types_scan::sdir::ScanDirectionIsForward(direction),
        number_tuples,
        use_parallel_mode,
        estate.es_top_eflags,
    );
    // inc-1b re-entry (lane-cursors.md §2 "repossess on resume"): one bool
    // load per run knob-OFF/never-parked (the flag is set only by a knob-ON
    // budgeted settle). Restages every parked scan's suspended page batch
    // before the first pull touches staged state.
    if estate.es_lane_cursor_parked {
        estate.es_lane_cursor_parked = false;
        crate::lanev2::cursor_park_resume(planstate, estate)?;
    }
    // --- end WS-AI wave-9 -------------------------------------------------------
    // --- WS-AJ wave-9 sub-region (SPI Stage-A seam, se/spi-stage-a; lane-spi.md
    // §1/§3) -----------------------------------------------------------------------
    // Per-run SPI emission budget, written UNCONDITIONALLY like the WS-AI
    // field above it (None on knob-OFF / tcount-0 / non-SPI-dest runs, which
    // answer at the callee's first tests; the None overwrite means no stale
    // budget survives an error unwind or estate reuse). TWO producers of a
    // count-limited `CommandDest::Spi` run reach here (review re-baseline,
    // notes/se-spi-stage-a.md §8): `_SPI_pquery`'s tcount-limited run
    // (spi/src/execute.rs:562, STOP-then-END) AND `SPI_cursor_fetch`'s
    // per-fetch receiver threaded through PortalRunFetch → PortalRunSelect
    // (pquery/src/lib.rs:594-630) — the plpgsql FOR-loop cadence, which
    // RESUMES on the same QueryDesc/estate. The dest compare IS the
    // seam-visible SPI signal — no SPI-layer code change (design §3
    // Stage A). The install runs the NAMED SPI-admission classifier
    // (refusals tick the ShapeClass::Spi taxonomy, knob-ON only); the
    // budgeted run's settle sits below the drive loop (WS-AJ block beside
    // the WS-AI park walker) and arms the SHARED `es_lane_cursor_parked`
    // resume signal, repossessed by the WS-AI resume walk at the next
    // entry above. Knob-OFF cost, per RUN and never per tuple: the eager
    // argument set (one `mydest` enum match + the direction compare) plus
    // the callee's count/select/dest register tests; the knob cell loads
    // only for count-limited SPI-dest SELECTs.
    estate.es_spi_run_budget = crate::lanev2::spi_run_budget_install(
        operation == CmdType::CMD_SELECT,
        dest.mydest() == ::types_dest::CommandDest::Spi,
        ::types_scan::sdir::ScanDirectionIsForward(direction),
        number_tuples,
        use_parallel_mode,
        estate.es_top_eflags,
    );
    // --- end WS-AJ wave-9 -------------------------------------------------------
    // === wave-10 shared-file marker (cursors inc-2 contract §8; sub-regions CA, CB, CC) ===
    // --- WS-CA wave-10 sub-region (reserved) ------------------------------------
    // --- end WS-CA wave-10 --------------------------------------------------------
    // --- WS-CB wave-10 (cursors inc-2: batch store fill + §6 staging; band 95001+) ---
    // §6 deletion rider row 4 EXECUTED (se/deletion-prep B1): the run seam
    // is FORWARD-ONLY. The §6 staging (a) evidence counter still ticks
    // FIRST (the bake instrument keeps counting every refused attempt —
    // `counter run-seam-backward` must read 0 across all corpora at
    // defaults, where the portal tuplestore serves every backward fetch),
    // then the drive errors loudly (0A000). This is the keystone of the
    // backward-execution deletion wave: with backward entry refused at
    // this one seam, every node-level backward arm below it (heapam
    // stepping, nodelimit/nodematerial/nodesort backward reads, the
    // tuplestore-scan backward arms, the lanev2 per-pull direction
    // chokepoints) is unreachable dead code. Reaching this error requires
    // a kill-switch world (`PGRUST_LANE_V2_CURSORS=0` / `PGRUST_LANE_V2=0`
    // scroll cursors — pquery's non-store backward leg); the default world
    // cannot reach it (SE13 flip + the D-row retirement). NoMovement never
    // enters this function (standard_executor_run gates), so the backward
    // test is the whole check.
    if ::types_scan::sdir::ScanDirectionIsBackward(direction) {
        crate::lanev2::run_seam_backward_evidence();
        return Err(Box::new(
            PgError::error(
                "backward scan is not supported: the executor's backward drive was \
                 deleted (single-executor migration, cursors inc-2 §6 deletion rider); \
                 backward cursor reads are served by the portal tuplestore",
            )
            .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    // §2.1/§2.3 batch store fill: a budgeted (cursor-FETCH-cadence) run
    // whose receiver is the portal store may be driven as a lane batch
    // pipeline into the store — batches, not the capacity-one per-row
    // pull ceremony. Engagement is decided by the standard admission
    // hooks inside `cursor_store_batch_fill`; refusals (and every
    // non-store, non-budgeted run) take the per-tuple loop below,
    // byte-identically (§2.3 fetch-invisibility). Knob-OFF/count-0 runs
    // read one None here — per-RUN cost only, never per-tuple (the
    // instruction-invisibility law).
    //
    // SE-R41 (notes/se-r41-retire.md §3): a capture-batchable eligible
    // fill arms the receiver with the §4.2 identity sidecar; the batch
    // fill then captures per accepted row inside the sink, and a refused
    // capture-armed run takes the CAPTURE row loop below (per-RUN branch;
    // the row loop captures after each accepted row) — the store and
    // sidecar stay aligned no matter which engine drove. Knob-OFF and
    // unarmed fills read one more None here, per RUN only.
    // World-B parallel passthrough funnel (gather-elimination Phase 2, Stage 3;
    // default ON since the GL-FUNNEL-4 flip — PGRUST_RUNTIME_ROW_FUNNEL=0
    // kills). Runs a lane-ownable bare passthrough SeqScan in parallel through
    // the runtime funnel and streams the rows to `dest` (dest.startup/shutdown
    // stay the caller's). Fail-closed: any ineligibility returns false and the
    // serial per-tuple loop below runs byte-identically. `!use_parallel_mode`
    // covers only the leader of a parallel plan; a parallel WORKER's fragment
    // clears parallelModeNeeded, so the funnel's own in-parallel-machinery
    // gate does the worker-side refusal.
    //
    // GL-STMTTASK-2: the inline-execute run cargo (change 3: the borrowed
    // seat; the quantum-yield span when the experiment is armed) — held
    // across the serial loop below when the statement-task hook answers
    // Inline; released at frame exit on every path (RAII).
    let mut _stmt_inline_seat: Option<crate::lanev2::StmtInlineRun> = None;
    if operation == CmdType::CMD_SELECT && send_tuples && !use_parallel_mode {
        if crate::lanev2::try_passthrough_funnel(estate, planstate, number_tuples, dest)? {
            return Ok(());
        }
        // GL-STMTTASK-1 (serial statement as a dop-1 pool task; kill knob
        // PGRUST_STMT_TASK, default OFF): the armed simple-protocol
        // statement's top-level run executes on a pool worker and streams
        // its rows back through the row funnel; this thread drains to
        // `dest` (startup/shutdown stay the caller's). Fail-closed: any
        // ineligibility (or no serving channel) returns Incumbent and the
        // serial per-tuple loop below runs byte-identically. Placed AFTER
        // the passthrough funnel deliberately: shapes inside the funnel's
        // proven band keep the stronger engine. Knob-OFF cost here is one
        // thread-local read (the armed flag OFF can never set).
        //
        // GL-STMTTASK-2 change 3 (inline-execute): the Inline verdict
        // hands back a borrowed pool seat — THIS thread runs the ordinary
        // serial loop below (literally the incumbent code, so parity and
        // cancel identity are structural), holding the seat for the span
        // of the run (governed accounting: one fewer pool step can run
        // while the session thread executes).
        match crate::lanev2::try_stmt_task(estate, planstate, number_tuples, dest)? {
            crate::lanev2::StmtTaskVerdict::Handled => return Ok(()),
            crate::lanev2::StmtTaskVerdict::Inline(run) => {
                _stmt_inline_seat = Some(run);
            }
            crate::lanev2::StmtTaskVerdict::Incumbent => {}
        }
    }
    let mut cursor_capture_sidecar: Option<::types_portal::TuplestoreHandle> = None;
    let cursor_fill_engaged = if estate.es_cursor_run_budget.is_some()
        && send_tuples
        && estate.es_junkFilter.is_none()
    {
        cursor_capture_sidecar = dest.tuplestore_capture_sidecar();
        crate::lanev2::cursor_store_batch_fill(planstate, estate, dest, cursor_capture_sidecar)?
    } else {
        false
    };
    // --- end WS-CB wave-10 ----------------------------------------------------------
    // --- WS-CC wave-10 sub-region (reserved) ------------------------------------
    // --- end WS-CC wave-10 --------------------------------------------------------
    if use_parallel_mode {
        enter_parallel_mode_outlined();
    }

    let mut current_tuple_count: u64 = 0;
    // WS-CB wave-10: an engaged batch fill consumed the run's budget inside
    // the sink — the per-tuple loop must not drive the plan again this run.
    // One branch per RUN (hoisted out of the loop, never per tuple).
    //
    // SE-R41: the capture split is likewise one branch per RUN — the
    // knob-OFF/unarmed world runs the loop below byte-identically; a
    // capture-armed run the batch fill refused runs the capture variant
    // (per-row identity append after each accepted row — the row-chain
    // capture moved inside the run, notes/se-r41-retire.md §3.7).
    if !cursor_fill_engaged {
        if let Some(sidecar) = cursor_capture_sidecar {
            loop {
                estate.reset_per_tuple_expr_context();

                let Some(slot_id) = exec_proc_node(planstate, estate)? else {
                    break;
                };
                // Capture-armed fills are budgeted store fills: SELECT,
                // junk-free (the dispatch gate above), send_tuples — the
                // plain loop's junk/send branches degenerate accordingly.
                debug_assert!(send_tuples && estate.es_junkFilter.is_none());

                {
                    let slot = estate.slot_mut(slot_id);
                    // SAFETY: lifetime bridge at the seam boundary — the
                    // plain loop's receive_slot arm verbatim.
                    let slot: &mut SlotData<'m> =
                        unsafe { &mut *(slot as *mut SlotData<'mcx>).cast::<SlotData<'m>>() };
                    if !dest.receive_slot(slot)? {
                        break;
                    }
                }
                crate::execcurrent::capture_current_into_sidecar(planstate, estate, sidecar)?;

                if operation == CmdType::CMD_SELECT {
                    estate.es_processed += 1;
                }

                current_tuple_count += 1;
                if number_tuples != 0 && number_tuples == current_tuple_count {
                    break;
                }
            }
        } else {
            loop {
                estate.reset_per_tuple_expr_context();

                let Some(mut slot_id) = exec_proc_node(planstate, estate)? else {
                    break;
                };

                if estate.es_junkFilter.is_some() {
                    slot_id = execjunk::exec_filter_junk(estate, slot_id);
                }

                if send_tuples {
                    let slot = estate.slot_mut(slot_id);
                    // SAFETY: lifetime bridge at the seam boundary (C passes a raw
                    // TupleTableSlot*). The receiver only copies datums out during
                    // the call and retains no borrow of the slot (printtup keeps an
                    // address token + its own wire buffer).
                    let slot: &mut SlotData<'m> =
                        unsafe { &mut *(slot as *mut SlotData<'mcx>).cast::<SlotData<'m>>() };
                    if !dest.receive_slot(slot)? {
                        break;
                    }
                }

                if operation == CmdType::CMD_SELECT {
                    estate.es_processed += 1;
                }

                current_tuple_count += 1;
                if number_tuples != 0 && number_tuples == current_tuple_count {
                    break;
                }
            }
        }
    }

    // --- WS-AI wave-9.5 (cursors inc-1b): the §2 park shape's settle point.
    // A budgeted (cursor-FETCH-cadence) run that stops with the pipeline
    // suspended SETTLES here: lane-staged claims retire through the
    // claim-release chain (HeapBatchSource-class staged page → the R3
    // zero-pins-at-settle law), position recorded node-resident; the next
    // run's resume walk (entry, above) repossesses. Knob-OFF / count-0 /
    // FETCH_ALL runs read one None and skip (per-run cost only, never
    // per-tuple). EPQ law: an EPQ recheck drive never enters execute_plan,
    // and the walker independently refuses under es_epq_active (the budget
    // belongs to the outer run — the inc-1a §5 design note, pinned in
    // units).
    if estate.es_cursor_run_budget.is_some() {
        if crate::lanev2::cursor_run_park(planstate, estate)? {
            estate.es_lane_cursor_parked = true;
        }
    }
    // --- end WS-AI wave-9.5 -----------------------------------------------------
    // --- WS-AJ wave-9.5 (SPI Stage-A): the settle point. A budgeted
    // (tcount-limited SPI-dest) run that stops here retires lane-staged
    // claims through the same claim-release chain the cursor walker owns —
    // BEFORE executor_finish/end return control toward the plancache release
    // points (lane-spi.md INVARIANT 5; post-t26 release-point map in
    // notes/se-wave9-aj.md §11.3) — and ticks the spi-plan-refused roll-up
    // when the plan carried no lane engagement. The park flag IS armed
    // (review re-baseline, notes/se-spi-stage-a.md §8): the portal-fetch
    // producer (SPI_cursor_fetch → PortalRunSelect, the plpgsql FOR-loop
    // cadence) RESUMES this QueryDesc/estate, and the WS-AI resume walk at
    // the next entry repossesses the parked position — dropping the bit
    // would resume an un-inited scan. For a true _SPI_pquery run the flag
    // is dead state torn down by the immediately-following ExecutorEnd
    // (the parked-then-close path cursors already ride). Never CLEARED
    // here: under the composed arm the WS-AI walker above may have armed
    // it already (settle is release-only/idempotent). Knob-OFF / tcount-0
    // / non-SPI runs read one None and skip (per-run cost only, never
    // per-tuple). EPQ law shared with the WS-AI walker (the walk refuses
    // under es_epq_active).
    if estate.es_spi_run_budget.is_some() && crate::lanev2::spi_run_settle(planstate, estate)? {
        estate.es_lane_cursor_parked = true;
    }
    // --- end WS-AJ wave-9.5 -----------------------------------------------------
    if estate.es_top_eflags & EXEC_FLAG_BACKWARD == 0 {
        exec_shutdown_node(planstate, estate)?;
    }
    if use_parallel_mode {
        exit_parallel_mode_outlined();
    }
    Ok(())
}

// The pre-parallel shape kept these arms compiler-cold (panic!); serial
// queries pay only the predicted-false test, as C does.
#[cold]
#[inline(never)]
fn enter_parallel_mode_outlined() {
    xact::EnterParallelMode();
}

#[cold]
#[inline(never)]
fn exit_parallel_mode_outlined() {
    xact::ExitParallelMode();
}

/// `standard_ExecutorFinish` (execMain.c).
// C fires AfterTriggerEndQuery before setting es_finished; the caller fires
// it after this returns (registry-borrow discipline) — es_finished has no
// reader during the firing loop.
pub fn standard_executor_finish(qd: &mut QueryDescData) -> PgResult<bool> {
    if let Some(t) = qd.totaltime.as_deref_mut() {
        ::instrument::instr_start_node(t);
    }
    let exec = qd.exec.as_mut().expect("ExecutorFinish before ExecutorStart");
    let fire = exec.with_mut(|data| {
        let es = &mut data.estate;
        debug_assert!(es.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY == 0);
        assert!(!es.es_finished, "ExecutorFinish called twice");
        exec_postprocess_plan(es)?;
        es.es_finished = true;
        Ok::<bool, Box<types_error::PgError>>(es.es_top_eflags & EXEC_FLAG_SKIP_TRIGGERS == 0)
    })?;
    if let Some(t) = qd.totaltime.as_deref_mut() {
        ::instrument::instr_stop_node(t, 0.0);
    }
    Ok(fire)
}

// ExecPostprocessPlan (execMain.c): run wCTE ModifyTable subplans to
// completion so unread RETURNING rows still execute their modifications.
// Reverse registration order == C's lcons ordering (later-initialized nodes
// shut down first, preserving RETURNING rows a later CTE subplan may read).
fn exec_postprocess_plan(estate: &mut EStateData<'_>) -> PgResult<()> {
    estate.es_direction = ScanDirection::ForwardScanDirection;
    for i in (0..estate.es_auxmodifytables.len()).rev() {
        let cell = estate.es_auxmodifytables[i];
        // SAFETY: an es_subplanstates cell installed by InitPlan on this
        // estate; same take-out protocol as cte_proc_hook.
        let slot =
            unsafe { &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr() };
        let mut ps = slot
            .take()
            .unwrap_or_else(|| panic!("recursive CTE plan execution (nodeCtescan.c)"));
        let result: PgResult<()> = (|| loop {
            estate.reset_per_tuple_expr_context();
            if exec_proc_node(&mut ps, estate)?.is_none() {
                return Ok(());
            }
        })();
        *slot = Some(ps);
        result?;
    }
    Ok(())
}

/// WS-P armed-path body of the ExecutorEnd census hook, outlined
/// `#[cold]`/`#[inline(never)]` (se2-cost-fix): `standard_executor_end` is
/// `#[inline]` into two callers, and keeping this walk (plus its `with_mut`
/// closure) inline there perturbed the DISARMED per-query codegen the
/// select1/prepared knob-OFF pair letters pin. Never reached at default
/// config (`census_armed()` gates the call).
#[cold]
#[inline(never)]
fn census_record_at_end(qd: &mut QueryDescData) {
    let pstmt = qd.plannedstmt();
    if let Some(exec) = qd.exec.as_mut() {
        exec.with_mut(|data| {
            crate::lanev2::census_record(pstmt, &data.estate, data.planstate.as_ref());
        });
    }
}

/// `standard_ExecutorEnd` (execMain.c); dropping the bundle is
/// `FreeExecutorState` (MemoryContextDelete of es_query_cxt).
// inline: the second caller (executor_finish_and_park's refusal arm) must not
// cost the per-query seam its base inlining (select1 attribution, +42/q).
#[inline]
pub fn standard_executor_end(qd: &mut QueryDescData) -> PgResult<()> {
    // execMain.c:487-489.
    if let Some(exec) = qd.exec.as_mut() {
        exec.with_mut(|data| {
            let es = &data.estate;
            if es.es_parallel_workers_to_launch > 0 {
                pgstat::database::pgstat_update_parallel_workers_stats(
                    es.es_parallel_workers_to_launch as i64,
                    es.es_parallel_workers_launched as i64,
                );
            }
        });
    }
    // WS-P node-census exit hook (lanev2/census.rs): with the census armed,
    // walk the plan tree and append one TSV row per plan node, joined to the
    // execution's EngineEvents. Before the skeleton park AND before teardown
    // (both need the estate + planstate alive); best-effort, never a query
    // error. Disarmed cost: one memoized-byte load + branch — the armed body
    // is `#[cold]`-outlined (se2-cost-fix): standard_executor_end inlines
    // into two callers, and carrying the census walk inline here cost the
    // DISARMED select1/prepared pair codegen (the +42/q history above).
    if crate::lanev2::census_armed() {
        census_record_at_end(qd);
    }
    // Executor-skeleton park (v2 gates mirror the reuse gates in
    // standard_executor_start; everything per-run — scan descriptors,
    // relation pins, snapshot, source text — is released here).
    if let Some(eflags) = skeleton_disarm_in_place(qd)? {
        let exec = qd.exec.take().expect("disarm probed the executor");
        let tup_desc = qd.tup_desc.take().expect("finished query has a tupdesc");
        exec_skeleton::park(exec_skeleton::Skeleton {
            pstmt: qd.plannedstmt() as *const _ as *const (),
            cplan: qd.cplan,
            eflags,
            exec,
            tup_desc,
        });
        return Ok(());
    }
    let mut exec = qd.exec.take().expect("ExecutorEnd before ExecutorStart");

    exec.with_mut(|data| -> PgResult<()> {
        let ExecData { estate, planstate } = data;
        debug_assert!(
            estate.es_finished || estate.es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY != 0
        );
        if let Some(ps) = planstate.as_mut() {
            exec_end_node(ps, estate)?;
        }
        for i in 0..estate.es_subplanstates.len() {
            let cell = estate.es_subplanstates[i];
            // SAFETY: init_plan created this arena cell; exclusive here (no
            // subplan can be mid-run during ExecutorEnd).
            let slot = unsafe {
                &mut *cell.0.cast::<Option<crate::PlanStateNode<'_>>>().as_ptr()
            };
            if let Some(mut ps) = slot.take() {
                exec_end_node(&mut ps, estate)?;
                // Dropping runs the Rc releases arena reset can't (no-drop rule).
            }
        }
        while let Some((p, dropper)) = estate.es_subplan_expr_states.pop() {
            // SAFETY: registered by exec_init_sub_plan_expr; dropped once here.
            unsafe { dropper(p) };
        }
        estate.exec_reset_tuple_table(false);
        estate.exec_close_result_relations();
        estate.exec_close_range_table_relations()?;
        snapmgr::UnregisterSnapshot(estate.es_snapshot.take().as_ref());
        snapmgr::UnregisterSnapshot(estate.es_crosscheck_snapshot.take().as_ref());
        estate.teardown();
        debug_assert!(estate.owners_released());
        Ok(())
    })?;
    // FreeExecutorState: one context reset, no per-object glue (the walk
    // above released every census-exempt owner; Drop stays the abort path);
    // the reset context parks for the next ExecutorStart (C context_freelists).
    exec_ctx_pool::park((*exec).free_recycle());
    qd.tup_desc = None;
    Ok(())
}

// Compile-time check that the seam impls match the declared signatures.
const _: () = {
    let _: execmain_seams::executor_run::Signature = executor_run_seam;
    let _: execmain_seams::executor_start::Signature = executor_start_seam;
};

// CheckValidRowMarkRel (execMain.c); the FDW arm is loud.
fn check_valid_row_mark_rel(
    rel: &::types_rel::Relation<'_>,
    mark_type: ::types_nodes::plannodes::RowMarkType,
) -> PgResult<()> {
    use ::types_nodes::plannodes::RowMarkType;
    use ::types_rel::{
        RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
        RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
    };
    let what = match rel.rd_rel.relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => return Ok(()),
        RELKIND_SEQUENCE => "sequence",
        RELKIND_TOASTVALUE => "TOAST relation",
        RELKIND_VIEW => "view",
        RELKIND_MATVIEW => {
            if mark_type == RowMarkType::ROW_MARK_REFERENCE {
                return Ok(());
            }
            "materialized view"
        }
        RELKIND_FOREIGN_TABLE => panic!(
            "CheckValidRowMarkRel (execMain.c): foreign-table RefetchForeignRow \
             probe; FDW lane"
        ),
        _ => "relation",
    };
    Err(cannot_lock_rows_in(what, rel))
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_lock_rows_in(what: &str, rel: &::types_rel::Relation<'_>) -> Box<PgError> {
    use ::types_error::{ErrorLocation, ERRCODE_WRONG_OBJECT_TYPE};
    let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
    Box::new(
        PgError::error(format!("cannot lock rows in {what} \"{relname}\""))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "CheckValidRowMarkRel")),
    )
}
