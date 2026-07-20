// pquery.c — portal execution (PG 18.3). Executor/utility surfaces are seams
// (their lanes are in flight); portal->stmts resolves through stmt_list.
#![allow(non_snake_case)]

use core::cell::RefCell;

use ::elog::ereport;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_dest::CommandDest;
use ::types_error::{
    PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use ::types_nodes::node_tree::Node;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::primnodes::TargetEntry;
use ::types_nodes::NodeTag;
use ::types_portal::{
    FetchDirection, ParamListHandle, Portal, PortalData, PortalStrategy, QueryCompletion,
    QueryDescHandle, QueryEnvHandle, StmtListHandle, TuplestoreHandle, CMDTAG_DELETE,
    CMDTAG_INSERT, CMDTAG_MERGE, CMDTAG_SELECT, CMDTAG_UNKNOWN, CMDTAG_UPDATE, CURSOR_OPT_HOLD,
    CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL, FETCH_ALL, PORTAL_DEFINED, PORTAL_MULTI_QUERY,
    PORTAL_ONE_MOD_WITH, PORTAL_ONE_RETURNING, PORTAL_ONE_SELECT, PORTAL_READY,
    PORTAL_UTIL_SELECT,
};
use ::types_scan::sdir::{
    BackwardScanDirection, ForwardScanDirection, NoMovementScanDirection, ScanDirection,
    ScanDirectionIsForward, ScanDirectionIsNoMovement,
};
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_REWIND};

use ::cmdtag::InitializeQueryCompletion;
use ::snapmgr::Snapshot;
use ::tcop_dest::DestReceiver;
use ::utility_seams::{PROCESS_UTILITY_QUERY, PROCESS_UTILITY_TOPLEVEL};

pub mod stmt_list;
#[cfg(test)]
mod tests;

pub use pquery_seams::TargetEntrySummary;

pub fn init_seams() {
    pquery_seams::fetch_portal_target_list::set(FetchPortalTargetList);
    pquery_seams::fetch_utility_statement_target_list::set(FetchUtilityStatementTargetList);
    pquery_seams::stmt_list_free::set(stmt_list::free);
    pquery_seams::ensure_portal_snapshot_exists::set(EnsurePortalSnapshotExists);
}

thread_local! {
    static ACTIVE_PORTAL: RefCell<Option<Portal<'static>>> = const { RefCell::new(None) };
}

pub fn ActivePortal() -> Option<Portal<'static>> {
    ACTIVE_PORTAL.with(|p| p.borrow().clone())
}

fn swap_active_portal(new: Option<Portal<'static>>) -> Option<Portal<'static>> {
    ACTIVE_PORTAL.with(|p| p.replace(new))
}

#[inline]
fn set_query_completion(qc: &mut QueryCompletion, tag: types_core::CommandTag, nprocessed: u64) {
    qc.commandTag = tag;
    qc.nprocessed = nprocessed;
}

// The PG_TRY/PG_CATCH shared by PortalStart/PortalRun/PortalRunFetch: set
// ActivePortal + CurrentResourceOwner = portal->resowner, run, MarkPortalFailed
// on Err or panic, restore both either way. (PortalContext /
// MemoryContextSwitchTo dissolve under RAII + explicit Mcx.)
// may_commit renders PortalRun's restore rule: a utility inside the portal can
// commit and destroy the saved owner, so a saved TopTransactionResourceOwner
// re-targets the exit-time one (pquery.c:816).
pub fn run_protected<R>(
    portal: &Portal<'static>,
    may_commit: bool,
    body: impl FnOnce() -> PgResult<R>,
) -> PgResult<R> {
    let save = swap_active_portal(Some(portal.clone()));
    let save_owner = resowner_seams::current_resource_owner::call();
    let save_top_owner = resowner_seams::top_transaction_resource_owner::call();
    let portal_owner = portal.borrow().resowner;
    if !portal_owner.is_null() {
        resowner_seams::set_current_resource_owner::call(portal_owner);
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body));
    let restore = |save: Option<Portal<'static>>| {
        if may_commit && save_owner == save_top_owner {
            resowner_seams::set_current_resource_owner::call(
                resowner_seams::top_transaction_resource_owner::call(),
            );
        } else {
            resowner_seams::set_current_resource_owner::call(save_owner);
        }
        swap_active_portal(save);
    };
    match outcome {
        Ok(Ok(r)) => {
            restore(save);
            Ok(r)
        }
        Ok(Err(e)) => {
            let _ = portalmem::MarkPortalFailed(portal);
            restore(save);
            Err(e)
        }
        Err(payload) => {
            let _ = portalmem::MarkPortalFailed(portal);
            restore(save);
            std::panic::resume_unwind(payload);
        }
    }
}

// The registry entry is owning; both Err returns and loud panics between
// create and free must release it, or the EState's relcache refs survive past
// AtEOXact_RelationCache and the abort path trips C's refcount assert
// (relcache.c AtEOXact_cleanup) after ProcArrayEndTransaction already ran.
pub struct QueryDescOwner(pub QueryDescHandle);

impl QueryDescOwner {
    pub fn disarm(&mut self) {
        self.0 = QueryDescHandle::NULL;
    }
}

impl Drop for QueryDescOwner {
    fn drop(&mut self) {
        if !self.0.is_null() {
            execmain_seams::release_query_desc::call(self.0);
        }
    }
}

fn with_source_text<R>(portal: &Portal<'static>, f: impl FnOnce(&str) -> R) -> R {
    let p = portal.borrow();
    f(p.sourceText.as_ref().map(|s| s.as_str()).unwrap_or(""))
}

pub fn CreateQueryDesc<'p, 'a, 's>(
    plannedstmt: &'p PlannedStmt<'a>,
    source_text: &'s str,
    snapshot: Option<Snapshot>,
    crosscheck_snapshot: Option<Snapshot>,
    dest: CommandDest,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    instrument_options: i32,
) -> PgResult<QueryDescHandle> {
    execmain_seams::create_query_desc::call(
        plannedstmt,
        source_text,
        snapshot,
        crosscheck_snapshot,
        dest,
        params,
        query_env,
        instrument_options,
    )
}

pub fn FreeQueryDesc(query_desc: QueryDescHandle) {
    execmain_seams::free_query_desc::call(query_desc);
}

fn ProcessQuery(
    plan: &PlannedStmt<'_>,
    source_text: &str,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &mut DestReceiver<'_>,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let query_desc = CreateQueryDesc(
        plan,
        source_text,
        Some(snapmgr::GetActiveSnapshot()),
        None, /* InvalidSnapshot */
        dest.mydest(),
        params,
        query_env,
        0,
    )?;

    let mut owner = QueryDescOwner(query_desc);
    run_process_query(query_desc, dest, qc)?;
    owner.disarm();

    FreeQueryDesc(query_desc);

    Ok(())
}

fn run_process_query(
    query_desc: QueryDescHandle,
    dest: &mut DestReceiver<'_>,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    execmain_seams::executor_start::call(query_desc, 0)?;

    execmain_seams::executor_run::call(query_desc, ForwardScanDirection, 0, dest)?;

    if let Some(qc) = qc {
        let es_processed = execmain_seams::query_desc_es_processed::call(query_desc);
        let tag = match execmain_seams::query_desc_operation::call(query_desc) {
            CmdType::CMD_SELECT => CMDTAG_SELECT,
            CmdType::CMD_INSERT => CMDTAG_INSERT,
            CmdType::CMD_UPDATE => CMDTAG_UPDATE,
            CmdType::CMD_DELETE => CMDTAG_DELETE,
            CmdType::CMD_MERGE => CMDTAG_MERGE,
            _ => CMDTAG_UNKNOWN,
        };
        set_query_completion(qc, tag, es_processed);
    }

    execmain_seams::executor_finish::call(query_desc)?;
    execmain_seams::executor_end::call(query_desc)
}

pub fn ChoosePortalStrategy(stmts: &[PlannedStmt<'_>]) -> PortalStrategy {
    if stmts.len() == 1 {
        let pstmt = &stmts[0];
        if pstmt.canSetTag {
            if pstmt.commandType == CmdType::CMD_SELECT {
                if pstmt.hasModifyingCTE {
                    return PORTAL_ONE_MOD_WITH;
                }
                return PORTAL_ONE_SELECT;
            }
            if pstmt.commandType == CmdType::CMD_UTILITY {
                let u = pstmt.utilityStmt.expect("CMD_UTILITY stmt has utilityStmt");
                if utility_seams::utility_returns_tuples::call(u) {
                    return PORTAL_UTIL_SELECT;
                }
                return PORTAL_MULTI_QUERY;
            }
        }
    }

    let mut n_set_tag = 0i32;
    for pstmt in stmts {
        if pstmt.canSetTag {
            n_set_tag += 1;
            if n_set_tag > 1 {
                return PORTAL_MULTI_QUERY;
            }
            if pstmt.commandType == CmdType::CMD_UTILITY || !pstmt.hasReturning {
                return PORTAL_MULTI_QUERY;
            }
        }
    }
    if n_set_tag == 1 {
        return PORTAL_ONE_RETURNING;
    }

    PORTAL_MULTI_QUERY
}

pub fn PortalGetPrimaryStmt(stmts: &[PlannedStmt<'_>]) -> Option<usize> {
    stmts.iter().position(|s| s.canSetTag)
}

pub fn FetchPortalTargetList<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    portal: &'a PortalData<'a>,
) -> PgResult<PgVec<'mcx, TargetEntrySummary>> {
    let mut out: PgVec<'mcx, TargetEntrySummary> = PgVec::new_in(mcx);
    if portal.strategy == PORTAL_MULTI_QUERY || portal.stmts.is_null() {
        return Ok(out);
    }
    stmt_list::with(portal.stmts, |stmts| -> PgResult<()> {
        let Some(primary) = PortalGetPrimaryStmt(stmts) else {
            return Ok(());
        };
        let pstmt = &stmts[primary];
        if pstmt.commandType == CmdType::CMD_UTILITY {
            out = FetchUtilityStatementTargetList(mcx, pstmt.utilityStmt)?;
            return Ok(());
        }
        if pstmt.commandType == CmdType::CMD_SELECT || pstmt.hasReturning {
            let plan = pstmt
                .planTree
                .and_then(Node::as_plan)
                .expect("PlannedStmt has a planTree");
            out.try_reserve(plan.targetlist.len())
                .map_err(|_| mcx.oom(plan.targetlist.len()))?;
            for node in plan.targetlist.iter() {
                let tle = node
                    .as_variant::<TargetEntry>()
                    .expect("targetlist entry is a TargetEntry");
                out.push(TargetEntrySummary {
                    resjunk: tle.resjunk,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                });
            }
        }
        Ok(())
    })?;
    Ok(out)
}

// C FetchStatementTargetList, utilityStmt tail: MOVE and anything besides
// FETCH/EXECUTE return NIL (e.g. plain EXPLAIN, described via
// ExplainResultDesc rather than a targetlist).
pub fn FetchUtilityStatementTargetList<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    utility_stmt: Option<Node<'a>>,
) -> PgResult<PgVec<'mcx, TargetEntrySummary>> {
    match utility_stmt.map(Node::node_tag) {
        Some(NodeTag::T_FetchStmt) => {
            let fstmt =
                utility_stmt.and_then(Node::as_fetch_stmt).expect("utilityStmt is FetchStmt");
            if fstmt.ismove {
                return Ok(PgVec::new_in(mcx));
            }
            let sub =
                portalmem::GetPortalByName(fstmt.portalname).expect("PortalIsValid(subportal)");
            let p = sub.borrow();
            let out = FetchPortalTargetList(mcx, &p);
            drop(p);
            out
        }
        Some(NodeTag::T_ExecuteStmt) => {
            let name = utility_stmt
                .and_then(Node::as_execute_stmt)
                .expect("utilityStmt is ExecuteStmt")
                .name
                .expect("EXECUTE has a name");
            let psrc = prepare_seams::fetch_prepared_statement_plansource::call(name, true)?
                .expect("throw_error=true never returns None");
            plancache::CachedPlanGetTargetList(mcx, psrc, QueryEnvHandle::NULL)
        }
        _ => Ok(PgVec::new_in(mcx)),
    }
}

pub fn PortalStart(
    portal: &Portal<'static>,
    params: ParamListHandle,
    eflags: i32,
    snapshot: Option<Snapshot>,
) -> PgResult<()> {
    debug_assert_eq!(portal.borrow().status, PORTAL_DEFINED);

    run_protected(portal, false, || -> PgResult<()> {
        portal.borrow_mut().portalParams = params;

        let stmts_handle = portal.borrow().stmts;
        let stmts: &[PlannedStmt<'static>] = if stmts_handle.is_null() {
            &[]
        } else {
            stmt_list::resolve(stmts_handle)
        };
        let strategy = ChoosePortalStrategy(stmts);
        portal.borrow_mut().strategy = strategy;

        match strategy {
            PORTAL_ONE_SELECT => {
                match &snapshot {
                    Some(snap) => snapmgr::PushActiveSnapshot(snap)?,
                    None => {
                        let snap = snapmgr::GetTransactionSnapshot()?;
                        snapmgr::PushActiveSnapshot(&snap)?;
                    }
                }

                let query_desc = {
                    let p = portal.borrow();
                    let source_text = p.sourceText.as_ref().map(|s| s.as_str()).unwrap_or("");
                    let query_env = p.queryEnv;
                    // installed() guard: test fixtures shim only the seams they use.
                    if !p.cplan.is_null() && execmain_seams::note_cplan_for_query_desc::is_installed()
                    {
                        // Skeleton-cache key: the plan backing this QueryDesc.
                        execmain_seams::note_cplan_for_query_desc::call(p.cplan);
                    }
                    CreateQueryDesc(
                        &stmts[0], /* linitial_node(PlannedStmt, portal->stmts) */
                        source_text,
                        Some(snapmgr::GetActiveSnapshot()),
                        None, /* InvalidSnapshot */
                        CommandDest::None,
                        params,
                        query_env,
                        0,
                    )?
                };

                // WS-CA wave-10 (contract §3.1): a store-armed SCROLL portal
                // is a plain forward plan — backward is consumed by the
                // store, rewind is a store rescan — so the child gets NEITHER
                // flag. Knob-OFF keeps C's arm verbatim (deleted at flip,
                // contract §6 item 1).
                //
                // D-CA-2 (worklog): CURRENT-OF-ELIGIBLE armed portals keep
                // C's flags. Their fill must be the row chain (§3.3 — the
                // per-row identity capture reads the scan state, which only
                // the row chain maintains); the batch engine's standing
                // eflags refusal (batch_allowed = no BACKWARD|MARK) is the
                // in-fence mechanism forcing that AT BOTH lane surfaces
                // (per-pull ownership and the batch-fill dispatch). The
                // store still serves every fetch either way
                // (fill-strategy invisibility, §2.3).
                //
                // SEAM-WIRING (SE10-GATES item 1): the CA/CB interface
                // review KEEPS the eflags fence as THE one armed fence (the
                // CB F1 keep-exactly-one-fence constraint: retiring it in
                // favor of reason-41 dispatch routing would expose
                // lane-parked eligible fills to the settle walker's slot
                // hygiene AND leave per-pull ownership unfenced). The §3.3
                // reason-41 tick is armed as the ACCOUNTING for this routing
                // decision at fill_to's eligible branch; eligibility itself
                // is AM-narrowed in the probe (pgrcolumnar scans carry no
                // tids — execcurrent.rs), which is what opens the lane
                // batch-fill breadth. Knob read = the CB seam (THE single
                // knob cell; the portalmem duplicate is retired).
                let scroll = (portal.borrow().cursorOptions & CURSOR_OPT_SCROLL) != 0;
                let store_armed = scroll
                    && execmain_seams::cursor_store_fill_enabled::is_installed()
                    && execmain_seams::cursor_store_fill_enabled::call();
                let current_of_eligible = store_armed
                    && execmain_seams::cursor_plan_current_of_eligible::is_installed()
                    && execmain_seams::cursor_plan_current_of_eligible::call(&stmts[0]);
                // R1b == B2 (night/se-b2-r1b; scratchpad/night/r1-cursors-design.md
                // §4, notes/se-b2-safety-proof.md): the D-CA-2 fence is DELETED.
                // R1a moved §4.2 (tableoid,ctid) capture IN-RUN for EVERY
                // eligible shape (batch sink for the lane-owned bare-SeqScan
                // cell; the run-seam capture row loop for every other eligible
                // shape), so the `current_of_eligible && !capture_batch` disjunct
                // that forced `batch_allowed=false` (REWIND|BACKWARD ⇒
                // ScrollMark ⇒ row chain) has NO correctness job left: with the
                // fence down every eligible shape still captures at its emit
                // surface (per the per-ShapeClass safety proof — SeqScan sink /
                // Volcano-standalone scans / lane-owned Append with per-row scan
                // slot stores). So `batch_allowed` now flips TRUE for
                // CURRENT-OF-eligible SCROLL cursors. `cursor_plan_capture_batch_fill`
                // (the bare-SeqScan sub-gate) and the `cursorCaptureBatch` portal
                // field are now VESTIGIAL — the fill dispatch selects the sink by
                // planstate node type (`cursor_store_batch_fill`), not this flag.
                //
                // The surviving `!store_armed` arm is ORTHOGONAL to the fence: a
                // NON-store-armed SCROLL cursor still needs C's real
                // REWIND|BACKWARD eflags for backward EXECUTION (the store is not
                // serving its fetches). That is the backward-execution wave
                // (B1-B11) territory, NOT landed on this R1a base — untouched here.
                let myeflags = if scroll && !store_armed {
                    eflags | EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD
                } else {
                    eflags
                };

                // Not yet reachable from the portal: owned until it is.
                let mut qd_owner = QueryDescOwner(query_desc);
                execmain_seams::executor_start::call(query_desc, myeflags)?;

                let tup_desc = execmain_seams::query_desc_result_tupdesc::call(query_desc);
                let mut p = portal.borrow_mut();
                p.queryDesc = query_desc;
                qd_owner.disarm();
                p.tupDesc = tup_desc;
                p.atStart = true;
                p.atEnd = false; /* allow fetches */
                p.portalPos = 0;
                // WS-CA wave-10: arming + the §4.1 eligibility answer are
                // both PortalStart-fixed (the fill and execCurrentOf read
                // them; the wildcard planstate walk at capture time agrees
                // with the plan-shape answer by construction).
                p.cursorStoreArmed = store_armed;
                if store_armed {
                    p.currentOfEligible = Some(current_of_eligible);
                    // R1b == B2: `cursorCaptureBatch` is vestigial (the D-CA-2
                    // fence it fed is deleted); left at its init default `false`.
                    // Field removal is a follow-up (R1c), mirroring R1a leaving
                    // the `cursor_capture_current` seam registered-but-uncalled.
                }
                drop(p);
                // SEAM-WIRING (SE10-GATES item 1): note the arming decision
                // once per armed portal — arms the run seam's §6
                // forward-only debug assert (a store-armed knob-ON world
                // never legally drives the executor backward).
                if store_armed {
                    execmain_seams::cursor_store_armed_note::call();
                }

                snapmgr::PopActiveSnapshot()?;
            }
            PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH => {
                let primary = PortalGetPrimaryStmt(stmts)
                    .expect("PORTAL_ONE_RETURNING portal has a primary stmt");
                let tup_desc = execmain_seams::exec_clean_type_from_tl::call(&stmts[primary])?;
                let mut p = portal.borrow_mut();
                p.tupDesc = Some(tup_desc);
                p.atStart = true;
                p.atEnd = false;
                p.portalPos = 0;
            }
            PORTAL_UTIL_SELECT => {
                let primary = PortalGetPrimaryStmt(stmts)
                    .expect("PORTAL_UTIL_SELECT portal has a primary stmt");
                let pstmt = &stmts[primary];
                debug_assert_eq!(pstmt.commandType, CmdType::CMD_UTILITY);
                let u = pstmt.utilityStmt.expect("utility stmt present");
                let tup_desc = utility_seams::utility_tuple_descriptor::call(u)?;
                let mut p = portal.borrow_mut();
                p.tupDesc = tup_desc;
                p.atStart = true;
                p.atEnd = false;
                p.portalPos = 0;
            }
            PORTAL_MULTI_QUERY => {
                portal.borrow_mut().tupDesc = None;
            }
        }
        Ok(())
    })?;

    portal.borrow_mut().status = PORTAL_READY;

    Ok(())
}

/// Portal-retention start (no C counterpart): the taken shell's plan matched
/// this bind's GetCachedPlan result, so the retained QueryDesc rearms in
/// place of CreateQueryDesc + ExecutorStart. Falls back to the full
/// PortalStart on a param-shape mismatch (whose compile re-runs C's checks).
pub fn PortalStartParked(portal: &Portal<'static>, params: ParamListHandle) -> PgResult<()> {
    debug_assert_eq!(portal.borrow().status, PORTAL_DEFINED);
    debug_assert_eq!(portal.borrow().strategy, PORTAL_ONE_SELECT);
    let rearmed = run_protected(portal, false, || -> PgResult<bool> {
        portal.borrow_mut().portalParams = params;
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        let query_desc = portal.borrow().queryDesc;
        let rearmed = execmain_seams::executor_rearm::call(
            query_desc,
            Some(snapmgr::GetActiveSnapshot()),
            params,
        )?;
        if rearmed {
            let mut p = portal.borrow_mut();
            p.atStart = true;
            p.atEnd = false;
            p.portalPos = 0;
        }
        snapmgr::PopActiveSnapshot()?;
        Ok(rearmed)
    })?;
    if rearmed {
        let mut p = portal.borrow_mut();
        p.cleanup = types_portal::PortalCleanupHook::PortalCleanup;
        p.status = PORTAL_READY;
        return Ok(());
    }
    let query_desc = {
        let mut p = portal.borrow_mut();
        p.tupDesc = None;
        p.cleanup = types_portal::PortalCleanupHook::PortalCleanup;
        core::mem::replace(&mut p.queryDesc, QueryDescHandle::NULL)
    };
    execmain_seams::release_query_desc::call(query_desc);
    PortalStart(portal, params, 0, None)
}

pub fn PortalSetResultFormat(portal: &Portal<'static>, formats: &[i16]) -> PgResult<()> {
    let n_formats = formats.len();

    let natts = match portal.borrow().tupDesc.as_ref() {
        None => return Ok(()),
        Some(td) => td.natts as usize,
    };

    let mut p = portal.borrow_mut();
    p.formats.clear();
    p.formats
        .try_reserve_exact(natts)
        .map_err(|_| mcx::oom_named("TopPortalContext", natts * 2))?;
    if n_formats > 1 {
        if n_formats != natts {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_PROTOCOL_VIOLATION)
                .errmsg(format!(
                    "bind message has {n_formats} result formats but query has {natts} columns"
                ))
                .into_error()
                .into());
        }
        for &f in &formats[..natts] {
            p.formats.push(f);
        }
    } else if n_formats > 0 {
        for _ in 0..natts {
            p.formats.push(formats[0]);
        }
    } else {
        for _ in 0..natts {
            p.formats.push(0);
        }
    }
    Ok(())
}

pub fn PortalRun<'mcx>(
    portal: &Portal<'static>,
    count: i64,
    is_top_level: bool,
    dest: &mut DestReceiver<'mcx>,
    mut altdest: Option<&mut DestReceiver<'mcx>>,
    mut qc: Option<&mut QueryCompletion>,
) -> PgResult<bool> {
    if let Some(qc) = qc.as_deref_mut() {
        InitializeQueryCompletion(qc);
    }

    let strategy = portal.borrow().strategy;
    let log_stats = guc_tables::backing::log_executor_stats();
    if log_stats && strategy != PORTAL_MULTI_QUERY {
        postgres_seams::reset_usage::call();
    }

    portalmem::MarkPortalActive(portal)?;

    let result = run_protected(portal, true, || -> PgResult<bool> {
        match strategy {
            PORTAL_ONE_SELECT | PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH
            | PORTAL_UTIL_SELECT => {
                if strategy != PORTAL_ONE_SELECT && portal.borrow().holdStore.is_null() {
                    FillPortalStore(portal, is_top_level)?;
                }

                let nprocessed = PortalRunSelect(portal, true, count, dest)?;

                if let Some(qc) = qc.as_deref_mut() {
                    let portal_qc = portal.borrow().qc;
                    if portal_qc.commandTag != CMDTAG_UNKNOWN {
                        *qc = portal_qc;
                        qc.nprocessed = nprocessed;
                    }
                }

                portal.borrow_mut().status = PORTAL_READY;

                Ok(portal.borrow().atEnd)
            }
            PORTAL_MULTI_QUERY => {
                PortalRunMulti(
                    portal,
                    is_top_level,
                    false,
                    dest,
                    altdest.as_deref_mut(),
                    qc.as_deref_mut(),
                )?;

                portalmem::MarkPortalDone(portal)?;

                Ok(true)
            }
        }
    })?;

    if log_stats && strategy != PORTAL_MULTI_QUERY {
        postgres_seams::show_usage::call("EXECUTOR STATISTICS")?;
    }

    Ok(result)
}

fn PortalRunSelect(
    portal: &Portal<'static>,
    forward: bool,
    mut count: i64,
    dest: &mut DestReceiver<'_>,
) -> PgResult<u64> {
    let query_desc = portal.borrow().queryDesc;
    let hold_store = portal.borrow().holdStore;
    // WS-CA wave-10: a store-armed portal with a live executor serves every
    // fetch from the cursor store (fill_to + RunFromStore). Once the executor
    // is gone (post-persist), the armed portal falls through to the ordinary
    // holdStore arm — same store, same reads.
    let store_armed = portal.borrow().cursorStoreArmed && !query_desc.is_null();

    debug_assert!(!query_desc.is_null() || !hold_store.is_null());

    // C forces queryDesc->dest = dest here (MOVE passes DestNone); the enum
    // receiver threads into executor_run instead — same per-fetch override.

    let nprocessed: u64;
    let direction: ScanDirection;

    if forward {
        if portal.borrow().atEnd || count <= 0 {
            direction = NoMovementScanDirection;
            count = 0; /* don't pass negative count to executor */
        } else {
            direction = ForwardScanDirection;
        }

        if count == FETCH_ALL {
            count = 0;
        }

        if store_armed {
            // §2.2: fill exactly as far as the fetch demands — never further
            // (count 0 / FETCH_ALL ⇒ fill to EOF) — then replay from the
            // store. NoMovement touches neither fill nor store rows.
            if !ScanDirectionIsNoMovement(direction) {
                let target = if count == 0 {
                    0
                } else {
                    portal.borrow().portalPos.saturating_add(count as u64)
                };
                fill_portal_store_to(portal, target)?;
            }
            nprocessed =
                RunFromStore(portal, direction, count as u64, dest, cursor_read_store(portal))?;
        } else if !hold_store.is_null() {
            nprocessed = RunFromStore(portal, direction, count as u64, dest, hold_store)?;
        } else {
            let snap = execmain_seams::query_desc_snapshot::call(query_desc)
                .expect("queryDesc->snapshot set while executor is active");
            snapmgr::PushActiveSnapshot(&snap)?;
            execmain_seams::executor_run::call(query_desc, direction, count as u64, dest)?;
            nprocessed = execmain_seams::query_desc_es_processed::call(query_desc);
            snapmgr::PopActiveSnapshot()?;
        }

        if !ScanDirectionIsNoMovement(direction) {
            let mut p = portal.borrow_mut();
            if nprocessed > 0 {
                p.atStart = false; /* OK to go backward now */
            }
            if count == 0 || nprocessed < count as u64 {
                p.atEnd = true; /* we retrieved 'em all */
            }
            p.portalPos += nprocessed;
        }
    } else {
        if (portal.borrow().cursorOptions & CURSOR_OPT_NO_SCROLL) != 0 {
            return Err(no_scroll_error());
        }

        if portal.borrow().atStart || count <= 0 {
            direction = NoMovementScanDirection;
            count = 0;
        } else {
            direction = BackwardScanDirection;
        }

        if count == FETCH_ALL {
            count = 0;
        }

        if store_armed {
            // §2.2: backward is a pure store seek — zero executor contact.
            // The store exists whenever atStart is false (a forward fetch
            // filled it); the NoMovement arm never touches store rows.
            nprocessed =
                RunFromStore(portal, direction, count as u64, dest, cursor_read_store(portal))?;
        } else if !hold_store.is_null() {
            nprocessed = RunFromStore(portal, direction, count as u64, dest, hold_store)?;
        } else {
            let snap = execmain_seams::query_desc_snapshot::call(query_desc)
                .expect("queryDesc->snapshot set while executor is active");
            snapmgr::PushActiveSnapshot(&snap)?;
            execmain_seams::executor_run::call(query_desc, direction, count as u64, dest)?;
            nprocessed = execmain_seams::query_desc_es_processed::call(query_desc);
            snapmgr::PopActiveSnapshot()?;
        }

        if !ScanDirectionIsNoMovement(direction) {
            let mut p = portal.borrow_mut();
            if nprocessed > 0 && p.atEnd {
                p.atEnd = false; /* OK to go forward now */
                p.portalPos += 1; /* adjust for endpoint case */
            }
            if count == 0 || nprocessed < count as u64 {
                p.atStart = true; /* we retrieved 'em all */
                p.portalPos = 0;
            } else {
                p.portalPos -= nprocessed;
            }
        }
    }

    Ok(nprocessed)
}

fn FillPortalStore(portal: &Portal<'static>, is_top_level: bool) -> PgResult<()> {
    let mut qc = QueryCompletion::default();
    InitializeQueryCompletion(&mut qc);

    portalmem::PortalCreateHoldStore(portal)?;
    // C also passes holdContext; it lives inside the store behind the handle.
    let mut treceiver = tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
    tcop_dest::SetTuplestoreDestReceiverParams(&mut treceiver, portal.borrow().holdStore, false);

    let strategy = portal.borrow().strategy;
    match strategy {
        PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH => {
            let mut none = tcop_dest::DestReceiver::DoNothing;
            PortalRunMulti(
                portal,
                is_top_level,
                true,
                &mut treceiver,
                Some(&mut none),
                Some(&mut qc),
            )?;
        }
        PORTAL_UTIL_SELECT => {
            let h = portal.borrow().stmts;
            PortalRunUtility(portal, h, 0, is_top_level, true, &mut treceiver, Some(&mut qc))?;
        }
        other => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unsupported portal strategy: {}", other as u32))
                .into_error()
                .into());
        }
    }

    if qc.commandTag != CMDTAG_UNKNOWN {
        portal.borrow_mut().qc = qc;
    }

    treceiver.destroy();

    Ok(())
}

// WS-CA wave-10: the store to read is now a parameter — the holdStore for
// held/RETURNING/UTIL portals (unchanged), the cursor store for store-armed
// portals. A NULL handle is legal only under NoMovement (loop never entered).
fn RunFromStore(
    portal: &Portal<'static>,
    direction: ScanDirection,
    count: u64,
    dest: &mut DestReceiver<'_>,
    hold_store: TuplestoreHandle,
) -> PgResult<u64> {
    let mut current_tuple_count: u64 = 0;

    let tup_desc = portal
        .borrow()
        .tupDesc
        .clone()
        .expect("RunFromStore: portal has a tupDesc");

    // C builds the slot in CurrentMemoryContext (== portalContext here).
    // SAFETY: portalContext is PgBox'd for address stability and outlives this
    // call (freed only in PortalDrop); the Ref is released before use.
    let ctx: &MemoryContext = unsafe {
        let p = portal.borrow();
        &*(&**p.portalContext.as_ref().expect("portal has portalContext")
            as *const MemoryContext)
    };
    let mcx = ctx.mcx();

    dest.startup(CmdType::CMD_SELECT as i32, &tup_desc)?;

    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(tup_desc));

    if ScanDirectionIsNoMovement(direction) {
    } else {
        let fwd = ScanDirectionIsForward(direction);
        loop {
            let ok = tuplestore_hold_seams::tuplestore_gettupleslot::call(
                hold_store, fwd, false, &mut slot,
            )?;
            if !ok {
                break;
            }

            if !dest.receive_slot(&mut slot)? {
                break;
            }

            exectuples::exec_clear_tuple(&mut slot, mcx);

            current_tuple_count += 1;
            if count != 0 && count == current_tuple_count {
                break;
            }
        }
    }

    dest.shutdown()?;

    drop(slot);

    Ok(current_tuple_count)
}

fn PortalRunUtility(
    portal: &Portal<'static>,
    stmts: StmtListHandle,
    idx: usize,
    is_top_level: bool,
    set_hold_snapshot: bool,
    dest: &mut DestReceiver<'_>,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    // One validated resolve for the whole call; ProcessUtility runs with the
    // slice live exactly as the previous with()-scoped form did.
    let pstmt = &stmt_list::resolve(stmts)[idx];
    let requires_snapshot = PlannedStmtRequiresSnapshot(pstmt);

    if requires_snapshot {
        let mut snapshot = snapmgr::GetTransactionSnapshot()?;

        if set_hold_snapshot {
            let registered = snapmgr::RegisterSnapshot(Some(&snapshot))?
                .expect("RegisterSnapshot of a live snapshot");
            portal.borrow_mut().holdSnapshot = Some(registered.clone());
            snapshot = registered;
        }

        let create_level = portal.borrow().createLevel;
        snapmgr::PushActiveSnapshotWithLevel(&snapshot, create_level)?;
        portal.borrow_mut().portalSnapshot = Some(snapmgr::GetActiveSnapshot());
    } else {
        portal.borrow_mut().portalSnapshot = None;
    }

    let context = if is_top_level {
        PROCESS_UTILITY_TOPLEVEL
    } else {
        PROCESS_UTILITY_QUERY
    };
    let read_only_tree = !portal.borrow().cplan.is_null(); /* protect tree if in plancache */

    // C switches into PortalContext around ProcessUtility.
    // SAFETY: portalContext is PgBox'd for address stability and outlives this
    // call (freed only in PortalDrop); the Ref is released before use.
    let ctx: &MemoryContext = unsafe {
        let p = portal.borrow();
        &*(&**p.portalContext.as_ref().expect("portal has portalContext")
            as *const MemoryContext)
    };
    let mcx = ctx.mcx();

    // No portal Ref may be held across ProcessUtility: VACUUM commits its
    // transaction mid-command and PreCommit_Portals re-enters this portal.
    // SAFETY: sourceText is set at portal define time, address-stable in the
    // portal's memory, and never mutated while the portal runs (C contract).
    let source_text: &str = unsafe {
        let p = portal.borrow();
        core::mem::transmute::<&str, &str>(
            p.sourceText.as_ref().map(|s| s.as_str()).unwrap_or(""),
        )
    };
    let (params, query_env) = {
        let p = portal.borrow();
        (p.portalParams, p.queryEnv)
    };
    utility_seams::process_utility::call(
        mcx,
        pstmt,
        source_text,
        read_only_tree,
        context,
        params,
        query_env,
        dest,
        qc,
    )?;

    let portal_snapshot = portal.borrow_mut().portalSnapshot.take();
    if let Some(snap) = portal_snapshot {
        if snapmgr::ActiveSnapshotSet() {
            debug_assert!(std::rc::Rc::ptr_eq(&snap, &snapmgr::GetActiveSnapshot()));
            snapmgr::PopActiveSnapshot()?;
        }
    }

    Ok(())
}

fn PortalRunMulti<'mcx>(
    portal: &Portal<'static>,
    is_top_level: bool,
    set_hold_snapshot: bool,
    dest: &mut DestReceiver<'mcx>,
    mut altdest: Option<&mut DestReceiver<'mcx>>,
    mut qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let mut active_snapshot_set = false;

    let mut none_dest = DestReceiver::DoNothing;
    let mut none_alt = DestReceiver::DoNothing;
    let demote_dest = dest.mydest() == CommandDest::RemoteExecute;
    let demote_alt = match altdest.as_deref() {
        Some(a) => a.mydest() == CommandDest::RemoteExecute,
        None => demote_dest,
    };

    let stmts = portal.borrow().stmts;
    let nstmts = if stmts.is_null() {
        0
    } else {
        stmt_list::resolve(stmts).len()
    };

    for i in 0..nstmts {
        postgres_seams::check_for_interrupts::call()?;

        // Re-resolved per iteration: a utility in an earlier statement can
        // release the portal's stmts (the null check below mirrors C).
        let pstmt = &stmt_list::resolve(stmts)[i];
        let (is_plannable, can_set_tag) = (pstmt.utilityStmt.is_none(), pstmt.canSetTag);

        if is_plannable {
            if guc_tables::backing::log_executor_stats() {
                postgres_seams::reset_usage::call();
            }

            if !active_snapshot_set {
                let mut snapshot = snapmgr::GetTransactionSnapshot()?;

                if set_hold_snapshot {
                    let registered = snapmgr::RegisterSnapshot(Some(&snapshot))?
                        .expect("RegisterSnapshot of a live snapshot");
                    portal.borrow_mut().holdSnapshot = Some(registered.clone());
                    snapshot = registered;
                }

                snapmgr::PushCopiedSnapshot(&snapshot)?;
                active_snapshot_set = true;
            } else {
                snapmgr::UpdateActiveSnapshotCommandId()?;
            }

            let receiver: &mut DestReceiver<'mcx> = if can_set_tag {
                if demote_dest { &mut none_dest } else { &mut *dest }
            } else {
                match altdest.as_deref_mut() {
                    Some(a) if !demote_alt => a,
                    Some(_) => &mut none_alt,
                    None => {
                        if demote_dest { &mut none_dest } else { &mut *dest }
                    }
                }
            };
            let stmt_qc = if can_set_tag { qc.as_deref_mut() } else { None };

            let (params, query_env) = {
                let p = portal.borrow();
                (p.portalParams, p.queryEnv)
            };
            with_source_text(portal, |source_text| {
                ProcessQuery(pstmt, source_text, params, query_env, receiver, stmt_qc)
            })?;

            if guc_tables::backing::log_executor_stats() {
                postgres_seams::show_usage::call("EXECUTOR STATISTICS")?;
            }
        } else {
            if can_set_tag {
                debug_assert!(!active_snapshot_set);
                let receiver: &mut DestReceiver<'mcx> =
                    if demote_dest { &mut none_dest } else { &mut *dest };
                PortalRunUtility(
                    portal,
                    stmts,
                    i,
                    is_top_level,
                    false,
                    receiver,
                    qc.as_deref_mut(),
                )?;
            } else {
                let receiver: &mut DestReceiver<'mcx> = match altdest.as_deref_mut() {
                    Some(a) if !demote_alt => a,
                    Some(_) => &mut none_alt,
                    None => {
                        if demote_dest { &mut none_dest } else { &mut *dest }
                    }
                };
                PortalRunUtility(portal, stmts, i, is_top_level, false, receiver, None)?;
            }
        }


        if portal.borrow().stmts.is_null() {
            break;
        }

        if i + 1 < nstmts {
            xact::CommandCounterIncrement()?;
        }
    }

    if active_snapshot_set {
        snapmgr::PopActiveSnapshot()?;
    }

    if let Some(qc) = qc {
        let portal_qc = portal.borrow().qc;
        if qc.commandTag == CMDTAG_UNKNOWN && portal_qc.commandTag != CMDTAG_UNKNOWN {
            *qc = portal_qc;
        }
    }

    Ok(())
}

#[cold]
#[inline(never)]
fn no_scroll_error() -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cursor can only scan forward")
            .errhint("Declare it with SCROLL option to enable backward scan.")
            .into_error(),
    )
}

pub fn PortalRunFetch(
    portal: &Portal<'static>,
    fdirection: FetchDirection,
    count: i64,
    dest: &mut DestReceiver<'_>,
) -> PgResult<u64> {
    portalmem::MarkPortalActive(portal)?;

    let result = run_protected(portal, false, || -> PgResult<u64> {
        let strategy = portal.borrow().strategy;
        match strategy {
            PORTAL_ONE_SELECT => DoPortalRunFetch(portal, fdirection, count, dest),
            PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH | PORTAL_UTIL_SELECT => {
                if portal.borrow().holdStore.is_null() {
                    FillPortalStore(portal, false)?;
                }
                DoPortalRunFetch(portal, fdirection, count, dest)
            }
            other => Err(ereport(ERROR)
                .errmsg_internal(format!("unsupported portal strategy: {}", other as u32))
                .into_error()
                .into()),
        }
    })?;

    portal.borrow_mut().status = PORTAL_READY;

    Ok(result)
}

fn DoPortalRunFetch(
    portal: &Portal<'static>,
    mut fdirection: FetchDirection,
    mut count: i64,
    dest: &mut DestReceiver<'_>,
) -> PgResult<u64> {
    match fdirection {
        FetchDirection::FETCH_FORWARD => {
            if count < 0 {
                fdirection = FetchDirection::FETCH_BACKWARD;
                count = -count;
            }
        }
        FetchDirection::FETCH_BACKWARD => {
            if count < 0 {
                fdirection = FetchDirection::FETCH_FORWARD;
                count = -count;
            }
        }
        FetchDirection::FETCH_ABSOLUTE => {
            let mut none = DestReceiver::DoNothing;
            if count > 0 {
                // Rewind + advance count-1, unless the goal is past halfway
                // (then scan from here); either way fetch the target forwards.
                // portalPos >= i64::MAX excluded so counts never look like
                // FETCH_ALL.
                let portal_pos = portal.borrow().portalPos;
                if (count - 1) as u64 <= portal_pos / 2 || portal_pos >= i64::MAX as u64 {
                    DoPortalRewind(portal)?;
                    if count > 1 {
                        PortalRunSelect(portal, true, count - 1, &mut none)?;
                    }
                } else {
                    let mut pos = portal_pos as i64;
                    if portal.borrow().atEnd {
                        pos += 1; /* need one extra fetch if off end */
                    }
                    if count <= pos {
                        PortalRunSelect(portal, false, pos - count + 1, &mut none)?;
                    } else if count > pos + 1 {
                        PortalRunSelect(portal, true, count - pos - 1, &mut none)?;
                    }
                }
                return PortalRunSelect(portal, true, 1, dest);
            } else if count < 0 {
                // Advance to end, back up abs(count)-1, return the prior row.
                PortalRunSelect(portal, true, FETCH_ALL, &mut none)?;
                if count < -1 {
                    PortalRunSelect(portal, false, -count - 1, &mut none)?;
                }
                return PortalRunSelect(portal, false, 1, dest);
            } else {
                DoPortalRewind(portal)?;
                return PortalRunSelect(portal, true, 0, dest);
            }
        }
        FetchDirection::FETCH_RELATIVE => {
            let mut none = DestReceiver::DoNothing;
            if count > 0 {
                if count > 1 {
                    PortalRunSelect(portal, true, count - 1, &mut none)?;
                }
                return PortalRunSelect(portal, true, 1, dest);
            } else if count < 0 {
                if count < -1 {
                    PortalRunSelect(portal, false, -count - 1, &mut none)?;
                }
                return PortalRunSelect(portal, false, 1, dest);
            } else {
                /* Same as FETCH FORWARD 0. */
                fdirection = FetchDirection::FETCH_FORWARD;
            }
        }
    }

    let mut forward = fdirection == FetchDirection::FETCH_FORWARD;

    // Zero count re-fetches the current row, if any (per SQL).
    if count == 0 {
        let on_row = {
            let p = portal.borrow();
            !p.atStart && !p.atEnd
        };
        if dest.mydest() == CommandDest::None {
            // MOVE 0 reports whether FETCH 0 would return a row.
            return Ok(u64::from(on_row));
        }
        if on_row {
            let mut none = DestReceiver::DoNothing;
            PortalRunSelect(portal, false, 1, &mut none)?;
            count = 1;
            forward = true;
        }
    }

    // MOVE BACKWARD ALL is a rewind.
    if !forward && count == FETCH_ALL && dest.mydest() == CommandDest::None {
        let mut result = portal.borrow().portalPos;
        if result > 0 && !portal.borrow().atEnd {
            result -= 1;
        }
        DoPortalRewind(portal)?;
        return Ok(result);
    }

    PortalRunSelect(portal, forward, count, dest)
}

fn DoPortalRewind(portal: &Portal<'static>) -> PgResult<()> {
    {
        let p = portal.borrow();
        if p.atStart && !p.atEnd {
            return Ok(());
        }
        if (p.cursorOptions & CURSOR_OPT_NO_SCROLL) != 0 {
            return Err(no_scroll_error());
        }
    }

    // WS-CA wave-10 (contract §5 D1): a store-armed portal rewinds the STORE
    // and keeps the executor at the fill high-water mark — re-fetch after
    // rewind is a store replay, never a re-execution. C rewinds the executor
    // (pquery.c DoPortalRewind -> ExecutorRewind); streams are identical by
    // replay and strictly more deterministic for volatile queries.
    if portal.borrow().cursorStoreArmed {
        let store = cursor_read_store(portal);
        if !store.is_null() {
            tuplestore_hold_seams::tuplestore_rescan::call(store)?;
        }
        let mut p = portal.borrow_mut();
        p.atStart = true;
        p.atEnd = false;
        p.portalPos = 0;
        return Ok(());
    }

    let hold_store = portal.borrow().holdStore;
    if !hold_store.is_null() {
        tuplestore_hold_seams::tuplestore_rescan::call(hold_store)?;
    }

    let query_desc = portal.borrow().queryDesc;
    if !query_desc.is_null() {
        let snap = execmain_seams::query_desc_snapshot::call(query_desc)
            .expect("queryDesc->snapshot set while executor is active");
        snapmgr::PushActiveSnapshot(&snap)?;
        execmain_seams::executor_rewind::call(query_desc)?;
        snapmgr::PopActiveSnapshot()?;
    }

    let mut p = portal.borrow_mut();
    p.atStart = true;
    p.atEnd = false;
    p.portalPos = 0;
    Ok(())
}

// --- WS-CA wave-10 (cursors inc-2): the portal-boundary cursor store ---------
//
// Contract §1 (store class/creation), §2.2 (fill_to laziness), §2.3 (fill
// strategy invisible to fetch), §4.2 (row-identity capture). CA-1 is the
// row-chain fill (the executor drives rows into the store through the
// ordinary tuplestore DestReceiver); WS-CB's lane batch sink replaces the
// drive INSIDE executor_run without changing a byte here (fill-strategy
// invisibility is the §7.2 asserted gate).

/// The store a store-armed portal reads: `cursorStore` (SCROLL without HOLD)
/// or the early-created `holdStore` (SCROLL + HOLD).
fn cursor_read_store(portal: &Portal<'static>) -> TuplestoreHandle {
    let p = portal.borrow();
    if !p.cursorStore.is_null() {
        p.cursorStore
    } else {
        p.holdStore
    }
}

/// §1.1 creation matrix, executed at first fill demand:
/// * SCROLL, no HOLD — `begin_heap(random_access=true, inter_xact=false,
///   work_mem)` (C provenance: portalmem.c:331 store shape minus the
///   cross-transaction properties it doesn't need); dies at PortalDrop.
/// * SCROLL + HOLD — the portal's holdStore via PortalCreateHoldStore
///   verbatim (holdContext under TopPortalContext, inter_xact=true), created
///   NOW instead of at commit; the fill receiver detoasts on append
///   (portalcmds.c:326's obligation moved to append time).
///
/// Plus the §4 eligibility probe (once) and the tid sidecar for eligible
/// plans.
fn ensure_cursor_store(portal: &Portal<'static>) -> PgResult<()> {
    debug_assert!(portal.borrow().cursorStoreArmed);
    let hold = (portal.borrow().cursorOptions & CURSOR_OPT_HOLD) != 0;
    let store_missing = {
        let p = portal.borrow();
        p.cursorStore.is_null() && p.holdStore.is_null()
    };
    if store_missing {
        if hold {
            portalmem::PortalCreateHoldStore(portal)?;
        } else {
            let store = tuplestore_hold_seams::tuplestore_begin_heap_cursor::call(true, false)?;
            portal.borrow_mut().cursorStore = store;
        }
    }
    // §4.1: the eligibility answer was fixed at PortalStart (plan shape).
    // Checked independently of store creation: a never-run WITH HOLD cursor
    // reaches its first fill at COMMIT with the holdStore already minted by
    // HoldPortal — the sidecar must still ride along.
    if portal.borrow().currentOfEligible == Some(true)
        && portal.borrow().cursorTidStore.is_null()
    {
        let sidecar = tuplestore_hold_seams::tuplestore_begin_heap_cursor::call(true, hold)?;
        portal.borrow_mut().cursorTidStore = sidecar;
    }
    Ok(())
}

/// Contract §2.2 — the laziness contract, verbatim:
///
/// ```text
/// fill_to(portal, target_rows):
///     if fill_exhausted or store.tuple_count() >= target_rows: return
///     deficit = target_rows - store.tuple_count()   # target 0 = fill to EOF
///     executor_run(queryDesc, Forward, deficit, dest = store sink)
///     if nprocessed < deficit (or deficit was 0): fill_exhausted = true
/// ```
///
/// The fill's ExecutorRun goes through the standard run seam, so WS-AI's
/// budget install arms it (`es_cursor_run_budget = Some(deficit)` — the
/// WS-AI hand-off point). R1a (§2a reason-41 completion): EVERY
/// CURRENT-OF-eligible plan runs ONE budgeted forward drive with the §4.2
/// identity sidecar armed on the receiver — the identity is captured
/// IN-RUN at the emit surface (batch sink for the lane-owned capture-batch
/// cell; the run seam's capture row loop for every shape the D-CA-2 fence
/// keeps on the row chain), never from a post-run `ss_ScanTupleSlot` read.
/// The row-at-a-time arm-B loop (post-run capture) is retired.
///
/// eof-pointer invariant (why appends are always visible to ptr0): the
/// tuplestore keeps the ACTIVE eof reader at EOF across appends
/// (tuplestore.c puttuple_common). ptr0 reaches eof_reached only via an
/// overshooting read, and RunFromStore can overshoot only when the store is
/// short of the fetch — which fill_to only permits at fill exhaustion, after
/// which no append ever happens (the flag is never cleared, rewind included).
///
/// pub: portalcmds' PersistHoldablePortal drives the §2.4 commit-time
/// `fill_to(EOF)` through this same function.
pub fn fill_portal_store_to(portal: &Portal<'static>, target_rows: u64) -> PgResult<()> {
    ensure_cursor_store(portal)?;
    if portal.borrow().cursorFillExhausted {
        return Ok(());
    }
    let store = cursor_read_store(portal);
    let have = tuplestore_hold_seams::tuplestore_tuple_count::call(store) as u64;
    if target_rows != 0 && have >= target_rows {
        return Ok(());
    }
    let query_desc = portal.borrow().queryDesc;
    debug_assert!(!query_desc.is_null(), "fill_to on a portal without an executor");
    let deficit = if target_rows == 0 { 0 } else { target_rows - have };
    let hold = (portal.borrow().cursorOptions & CURSOR_OPT_HOLD) != 0;
    let eligible = portal.borrow().currentOfEligible == Some(true);
    let tid_store = portal.borrow().cursorTidStore;

    let mut treceiver = tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
    // detoast=true exactly for the holdStore shape (§1.1): same bytes
    // (detoasting is deterministic), earlier cost, no re-execution at commit.
    tcop_dest::SetTuplestoreDestReceiverParams(&mut treceiver, store, hold);

    // Same snapshot discipline as the executor arm of PortalRunSelect; on
    // error the active snapshot unwinds with the (now FAILED) transaction.
    let snap = execmain_seams::query_desc_snapshot::call(query_desc)
        .expect("queryDesc->snapshot set while executor is active");
    snapmgr::PushActiveSnapshot(&snap)?;
    if eligible {
        // R1a (night/r1a-impl; §2a reason-41 completion): capture
        // UNIVERSALISATION. EVERY CURRENT-OF-eligible cursor fills through
        // ONE budgeted forward run with the §4.2 identity sidecar armed on
        // the receiver — the old three-arm split (capture-batch sink vs.
        // the row-chain arm-B loop) collapses to this single drive. The
        // §4.2 identity is captured INSIDE the run, at the emit surface,
        // regardless of which engine drove:
        //  - a shape the lane OWNS (the capture-batch bare-heap SeqScan
        //    cell — plain store-armed eflags at PortalStart, so its scan
        //    inits `batch_allowed=true`) captures per accepted row in the
        //    batch sink (SE-R41 v2, byte-proven);
        //  - a shape the lane REFUSES (the D-CA-2 fence: PortalStart handed
        //    it REWIND|BACKWARD eflags so its scan inits `batch_allowed=
        //    false` — every eligible shape but the capture-batch cell while
        //    the fence is UP) falls to the run seam's CAPTURE ROW LOOP
        //    (execmain.rs), which appends the SAME `capture_positioned`
        //    identity per emitted row, in-run, before the settle point.
        // This replaces arm B's post-run `cursor_capture_current` read of
        // `ss_ScanTupleSlot` — the single load-bearing hazard the study
        // named. The fill is always ForwardScanDirection, so the WS-AI
        // cursor budget installs unconditionally (cursor_admission_refusal
        // admits every forward run), which is what arms the capture row
        // loop for the refused shapes. The reason-41 tick is retired with
        // arm B (the mechanism it accounted for no longer exists — the
        // capture site is in-run, not fenced-to-the-row-chain).
        //
        // Cadence equivalence to the deleted arm B (see notes/r1a-impl.md):
        // arm B did N x executor_run(1) + a post-run capture per run and
        // set cursorFillExhausted when a run returned es_processed==0; this
        // single run of count=deficit sets it via `nprocessed < deficit`.
        // Both yield identical store rows, identical row-aligned sidecar
        // tids (SAME capture_positioned source), and identical exhaustion:
        //  R>=D  -> B breaks at filled==D (not exhausted); here nprocessed==D
        //          (not exhausted).
        //  R<D   -> B's (R+1)th run returns 0 (exhausted); here nprocessed<D
        //          (exhausted).
        //  D==0  -> both fill to EOF and mark exhausted.
        // deficit 0 (FETCH ALL / §2.4 persist) is a u64::MAX-count budgeted
        // run: count semantics are identical for any count > rowcount and
        // the capture dispatch stays armed — a fill-to-EOF MUST still
        // capture (MOVE BACKWARD after FETCH ALL resolves CURRENT OF from
        // the sidecar).
        debug_assert!(!tid_store.is_null());
        tcop_dest::SetTuplestoreCaptureSidecar(&mut treceiver, tid_store);
        let effective = if deficit == 0 { u64::MAX } else { deficit };
        execmain_seams::executor_run::call(
            query_desc,
            ForwardScanDirection,
            effective,
            &mut treceiver,
        )?;
        let nprocessed = execmain_seams::query_desc_es_processed::call(query_desc);
        if deficit == 0 || nprocessed < deficit {
            portal.borrow_mut().cursorFillExhausted = true;
        }
    } else {
        execmain_seams::executor_run::call(
            query_desc,
            ForwardScanDirection,
            deficit,
            &mut treceiver,
        )?;
        let nprocessed = execmain_seams::query_desc_es_processed::call(query_desc);
        if deficit == 0 || nprocessed < deficit {
            portal.borrow_mut().cursorFillExhausted = true;
        }
    }
    treceiver.destroy();
    snapmgr::PopActiveSnapshot()?;
    Ok(())
}

/// §2.4 auto-held arm (the HoldPinnedPortals adversarial class): a pinned
/// plpgsql cursor auto-held at intra-procedure COMMIT was armed WITHOUT
/// CURSOR_OPT_HOLD, so its store is the transaction-scoped `cursorStore`
/// (inter_xact=false, NOT detoast-on-append). Persist copies the filled
/// store into the fresh holdStore through the detoasting receiver (same
/// bytes C would re-execute for; the source rows are read pre-COMMIT while
/// their toast data is alive), then drops the transaction-scoped store and
/// sidecar (their spill files must not survive the transaction). No-op for
/// DECLARE'd WITH HOLD portals (their store already IS the holdStore).
pub fn cursor_store_persist_into_hold(portal: &Portal<'static>) -> PgResult<()> {
    let src = portal.borrow().cursorStore;
    if src.is_null() {
        return Ok(());
    }
    let dst = portal.borrow().holdStore;
    debug_assert!(!dst.is_null(), "HoldPortal creates the holdStore before persist");
    let tup_desc = portal
        .borrow()
        .tupDesc
        .clone()
        .expect("cursor portal has a tupDesc");
    // SAFETY: portalContext is PgBox'd for address stability and outlives
    // this call (freed only in PortalDrop) — the RunFromStore pattern.
    let ctx: &MemoryContext = unsafe {
        let p = portal.borrow();
        &*(&**p.portalContext.as_ref().expect("portal has portalContext")
            as *const MemoryContext)
    };
    let mcx = ctx.mcx();
    let mut treceiver = tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
    tcop_dest::SetTuplestoreDestReceiverParams(&mut treceiver, dst, true);
    treceiver.startup(CmdType::CMD_SELECT as i32, &tup_desc)?;
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(tup_desc));
    tuplestore_hold_seams::tuplestore_rescan::call(src)?;
    while tuplestore_hold_seams::tuplestore_gettupleslot::call(src, true, false, &mut slot)? {
        treceiver.receive_slot(&mut slot)?;
        exectuples::exec_clear_tuple(&mut slot, mcx);
    }
    treceiver.shutdown()?;
    treceiver.destroy();
    drop(slot);
    let (src, sidecar) = {
        let mut p = portal.borrow_mut();
        (
            core::mem::replace(&mut p.cursorStore, TuplestoreHandle::NULL),
            core::mem::replace(&mut p.cursorTidStore, TuplestoreHandle::NULL),
        )
    };
    tuplestore_hold_seams::tuplestore_end::call(src);
    if !sidecar.is_null() {
        tuplestore_hold_seams::tuplestore_end::call(sidecar);
    }
    Ok(())
}
// --- end WS-CA wave-10 --------------------------------------------------------

pub fn PlannedStmtRequiresSnapshot(pstmt: &PlannedStmt<'_>) -> bool {
    let Some(utility_stmt) = pstmt.utilityStmt else {
        return true;
    };

    !matches!(
        utility_stmt.node_tag(),
        NodeTag::T_TransactionStmt
            | NodeTag::T_LockStmt
            | NodeTag::T_VariableSetStmt
            | NodeTag::T_VariableShowStmt
            | NodeTag::T_ConstraintsSetStmt
            | NodeTag::T_FetchStmt
            | NodeTag::T_ListenStmt
            | NodeTag::T_NotifyStmt
            | NodeTag::T_UnlistenStmt
            | NodeTag::T_CheckPointStmt
    )
}

pub fn EnsurePortalSnapshotExists() -> PgResult<()> {
    if snapmgr::ActiveSnapshotSet() {
        return Ok(());
    }

    let Some(portal) = ActivePortal() else {
        return Err(ereport(ERROR)
            .errmsg_internal("cannot execute SQL without an outer snapshot or portal")
            .into_error()
            .into());
    };
    debug_assert!(portal.borrow().portalSnapshot.is_none());

    let snapshot = snapmgr::GetTransactionSnapshot()?;
    let create_level = portal.borrow().createLevel;
    snapmgr::PushActiveSnapshotWithLevel(&snapshot, create_level)?;
    portal.borrow_mut().portalSnapshot = Some(snapmgr::GetActiveSnapshot());
    Ok(())
}
