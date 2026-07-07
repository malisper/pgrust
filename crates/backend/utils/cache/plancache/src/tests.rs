use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

use mcx::MemoryContext;
use types_core::{Oid, INT4OID};
use types_nodes::node_tree::Node;
use types_nodes::plannodes::{Plan, Result as ResultPlan};
use types_nodes::primnodes::{Const, TargetEntry};
use types_nodes::rawnodes::SelectStmt;
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::lock::LOCKACQUIRE_OK;

use super::*;

const TEST_RELID: Oid = 50001;
// A funcid the generic plan depends on (setrefs would key its PlanInvalItem on
// PROCOID); the hash value the object-inval message carries.
const TEST_FUNC_HASH: u32 = 0x00AB_CDEF;

thread_local! {
    static PLANNER_CALLS: Cell<u32> = const { Cell::new(0) };
    static PLANNER_COST: Cell<f64> = const { Cell::new(1.0) };
    static PLAN_CACHE_MODE_VAR: Cell<i32> = const { Cell::new(0) };
    static LOCKS_TAKEN: Cell<u32> = const { Cell::new(0) };
}

fn stub_planner<'a, 'mcx>(
    mcx: mcx::Mcx<'mcx>,
    parse: &'mcx mut Query<'mcx>,
    _query_string: &'a str,
    _cursor_options: i32,
    _bound_params: ParamListHandle,
) -> PgResult<PlannedStmt<'mcx>> {
    PLANNER_CALLS.with(|c| c.set(c.get() + 1));
    let tree = Node::mk(
        mcx,
        ResultPlan {
            plan: Plan { total_cost: PLANNER_COST.with(Cell::get), ..Plan::default() },
            resconstantqual: None,
        },
    )?;
    let mut relation_oids = types_nodes::list::OidList::nil();
    relation_oids.lappend(mcx, TEST_RELID)?;
    // The generic plan carries a PROCOID PlanInvalItem the way setrefs records
    // one for a user function; the source querytree does not (source-side
    // collection is the re-analysis lane), so this drives the generic-plan arm.
    let mut inval_items = types_nodes::list::NodeList::nil();
    inval_items.lappend(
        mcx,
        Node::mk(mcx, types_nodes::plannodes::PlanInvalItem { cacheId: PROCOID, hashValue: TEST_FUNC_HASH })?,
    )?;
    Ok(PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        canSetTag: parse.canSetTag,
        planTree: Some(tree),
        rtable: parse.rtable.clone_in(mcx)?,
        relationOids: relation_oids,
        invalItems: inval_items,
        ..PlannedStmt::default()
    })
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        planner_seams::planner::set(stub_planner);
        syscache_seams::lookup_pg_type_shape::set(|_typid| {
            Ok(Some(types_tuple::tupdesc::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typstorage: b'p' as i8,
                typcollation: types_core::InvalidOid,
            }))
        });
        miscinit_seams::get_user_id::set(|| 10);
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        syscache_seams::lookup_authid_rolname::set(|_, _| Ok(None));
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|_| Ok(types_core::InvalidOid));
        syscache_seams::pg_namespace_nspname::set(|_| Ok(None));
        inval_seams::accept_invalidation_messages::set(|| Ok(()));
        lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
            LOCKS_TAKEN.with(|c| c.set(c.get() + 1));
            Ok(LOCKACQUIRE_OK)
        });
        lock_seams::lock_release::set(|_, _, _| Ok(true));
        lock_seams::mark_lock_clear::set(|_, _| {});
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        catalog_seams::is_shared_relation::set(|_| false);
        {
            guc_tables::vars::plan_cache_mode.install_if_absent(guc_tables::GucVarAccessors {
                get: || PLAN_CACHE_MODE_VAR.with(Cell::get),
                set: |v| PLAN_CACHE_MODE_VAR.with(|c| c.set(v)),
            });
        }
        if !guc_tables::vars::cpu_operator_cost.installed() {
            guc_tables::vars::cpu_operator_cost.install(guc_tables::GucVarAccessors {
                get: || 0.0025,
                set: |_| {},
            });
        }
    });
    miscinit::SetUserIdAndSecContext(10, 0);
    PLANNER_CALLS.with(|c| c.set(0));
    PLAN_CACHE_MODE_VAR.with(|c| c.set(guc_tables::consts::PLAN_CACHE_MODE_AUTO));
}

fn test_mcx() -> mcx::Mcx<'static> {
    Box::leak(Box::new(MemoryContext::new("plancache-test"))).mcx()
}

// No matching Pop: SnapshotResetXmin needs MyProc (procarray boot); the
// pushed sentinel just satisfies ActiveSnapshotSet for the test thread.
fn push_snapshot() {
    let mcx = test_mcx();
    let snap: snapmgr::Snapshot =
        Rc::new(SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC));
    snapmgr::PushActiveSnapshot(&snap).unwrap();
}

fn select_raw(mcx: mcx::Mcx<'static>) -> RawStmt<'static> {
    RawStmt {
        stmt: Some(Node::mk(mcx, SelectStmt::default()).unwrap()),
        stmt_location: 0,
        stmt_len: 0,
    }
}

fn select_query(mcx: mcx::Mcx<'static>, with_rel: bool) -> Query<'static> {
    let konst = Node::mk(
        mcx,
        Const {
            consttype: INT4OID,
            consttypmod: -1,
            constcollid: types_core::InvalidOid,
            constlen: 4,
            constvalue: datum::Datum::from_i32(1),
            constisnull: false,
            constbyval: true,
            location: -1,
        },
    )
    .unwrap();
    let tle = Node::mk(
        mcx,
        TargetEntry {
            expr: konst,
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: types_core::InvalidOid,
            resorigcol: 0,
            resjunk: false,
        },
    )
    .unwrap();
    let mut target_list = types_nodes::list::NodeList::nil();
    target_list.lappend(mcx, tle).unwrap();
    let mut rtable = types_nodes::list::NodeList::nil();
    if with_rel {
        let rte = Node::mk(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid: TEST_RELID,
                rellockmode: 1,
                ..RangeTblEntry::default()
            },
        )
        .unwrap();
        rtable.lappend(mcx, rte).unwrap();
    }
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        targetList: target_list,
        rtable,
        ..Query::default()
    }
}

fn make_saved_source(with_rel: bool) -> CachedPlanSourceHandle {
    let scratch = test_mcx();
    let raw = select_raw(scratch);
    let h = CreateCachedPlan(Some(&raw), "SELECT 1", CommandTag::SELECT).unwrap();
    let qmcx = SourceQueryMcx(h);
    let mut qlist = PgVec::new_in(qmcx);
    qlist.push(select_query(qmcx, with_rel));
    CompleteCachedPlan(h, qlist, &[], types_portal::CURSOR_OPT_PARALLEL_OK, true).unwrap();
    SaveCachedPlan(h).unwrap();
    h
}

#[test]
fn cache_ids_match_c_headers() {
    assert_eq!(PROCOID, 47);
    assert_eq!(TYPEOID, 82);
    assert_eq!(NAMESPACEOID, 38);
    assert_eq!(OPEROID, 40);
    assert_eq!(AMOPOPID, 3);
    assert_eq!(FOREIGNSERVEROID, 32);
    assert_eq!(FOREIGNDATAWRAPPEROID, 30);
}

#[test]
fn seams_route_to_this_crate() {
    init_seams();
    assert!(plancache_portal_seams::release_cached_plan::is_installed());
    assert!(plancache_portal_seams::init_plan_cache::is_installed());
}

#[test]
fn generic_plan_built_once_and_warm_hit_reuses_it() {
    install();
    push_snapshot();
    let h = make_saved_source(true);
    assert!(CachedPlanIsValid(h));
    assert!(CachedPlanResultDesc(h).is_some());

    let p1 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1);
    let stmts = CachedPlanStmtList(p1);
    assert_eq!(stmts.len(), 1);
    assert_eq!(stmts[0].commandType, CmdType::CMD_SELECT);

    let locks_before = LOCKS_TAKEN.with(Cell::get);
    let p2 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(p1, p2);
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1);
    // Warm hit: planner+executor lock walks, one lock per rtable rel each.
    assert_eq!(LOCKS_TAKEN.with(Cell::get), locks_before + 2);

    ReleaseCachedPlan(p1);
    ReleaseCachedPlan(p2);
    DropCachedPlan(h);
}

#[test]
fn rel_inval_flushes_saved_plan_and_nonmatching_does_not() {
    install();
    push_snapshot();
    let h = make_saved_source(true);
    let p = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    ReleaseCachedPlan(p);

    PlanCacheRelCallback(Datum::from_oid(InvalidOid), TEST_RELID + 1);
    assert!(CachedPlanIsValid(h));

    PlanCacheRelCallback(Datum::from_oid(InvalidOid), TEST_RELID);
    assert!(!CachedPlanIsValid(h));

    DropCachedPlan(h);
}

// The function-dependency inval loop end-to-end through the committed inval
// machinery: a saved plan with a generic plan carrying a PROCOID PlanInvalItem,
// a PROCOID inval message dispatched via CallSyscacheCallbacks, then the next
// GetCachedPlan re-plans (the generic-plan arm; no re-analysis needed).
#[test]
fn func_inval_invalidates_generic_plan_and_replans() {
    install();
    InitPlanCache().unwrap();
    push_snapshot();
    let h = make_saved_source(true);

    let p1 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1);
    ReleaseCachedPlan(p1);

    // A non-matching hash on PROCOID must not invalidate.
    inval::invalidate::CallSyscacheCallbacks(PROCOID, TEST_FUNC_HASH ^ 1).unwrap();
    let p2 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1, "warm hit: no re-plan on miss");
    ReleaseCachedPlan(p2);

    // Matching PROCOID inval: source stays valid, generic plan is invalidated.
    inval::invalidate::CallSyscacheCallbacks(PROCOID, TEST_FUNC_HASH).unwrap();
    assert!(CachedPlanIsValid(h), "source querytree stays valid");

    let p3 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 2, "generic plan re-planned after inval");
    let stmts = CachedPlanStmtList(p3);
    assert_eq!(stmts.len(), 1);
    ReleaseCachedPlan(p3);
    DropCachedPlan(h);
}

#[test]
fn invalid_oid_inval_flushes_any_rel_dependent_plan() {
    install();
    push_snapshot();
    let h = make_saved_source(true);
    PlanCacheRelCallback(Datum::from_oid(InvalidOid), InvalidOid);
    assert!(!CachedPlanIsValid(h));
    DropCachedPlan(h);
}

#[test]
fn reset_plan_cache_skips_non_revalidatable_sources() {
    install();
    let h_txn = CreateCachedPlan(None, "", types_portal::CMDTAG_UNKNOWN).unwrap();
    let qmcx = SourceQueryMcx(h_txn);
    CompleteCachedPlan(h_txn, PgVec::new_in(qmcx), &[], 0, false).unwrap();
    SaveCachedPlan(h_txn).unwrap();

    push_snapshot();
    let h_sel = make_saved_source(false);

    ResetPlanCache();
    assert!(CachedPlanIsValid(h_txn));
    assert!(!CachedPlanIsValid(h_sel));

    DropCachedPlan(h_txn);
    DropCachedPlan(h_sel);
}

#[test]
#[should_panic(expected = "RevalidateCachedQuery (plancache.c)")]
fn invalidated_source_replan_arm_is_loud() {
    install();
    push_snapshot();
    let h = make_saved_source(false);
    PlanCacheSysCallback(Datum::from_oid(InvalidOid), NAMESPACEOID, 0);
    assert!(!CachedPlanIsValid(h));
    let _ = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL);
}

thread_local! {
    static REANALYZE_CALLS: Cell<u32> = const { Cell::new(0) };
}

fn stub_reanalyze(
    qmcx: mcx::Mcx<'static>,
    _query_string: &'static str,
    _param_types: &'static [Oid],
    arg: i32,
) -> PgResult<PgVec<'static, Query<'static>>> {
    assert_eq!(arg, 7);
    REANALYZE_CALLS.with(|c| c.set(c.get() + 1));
    let mut v = PgVec::new_in(qmcx);
    v.push(select_query(qmcx, false));
    Ok(v)
}

// Sinval flushes the querytree; the next fetch re-analyzes via the installed
// hook, rebuilds deps/search_path, and replans.
#[test]
fn invalidated_source_reanalyzes_via_installed_hook() {
    install();
    push_snapshot();
    REANALYZE_CALLS.with(|c| c.set(0));
    let h = make_saved_source(false);
    SetCachedPlanReanalyze(h, stub_reanalyze, 7);

    let p1 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(REANALYZE_CALLS.with(Cell::get), 0);
    ReleaseCachedPlan(p1);

    PlanCacheSysCallback(Datum::from_oid(InvalidOid), NAMESPACEOID, 0);
    assert!(!CachedPlanIsValid(h));

    let planner_before = PLANNER_CALLS.with(Cell::get);
    let p2 = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(REANALYZE_CALLS.with(Cell::get), 1);
    assert!(CachedPlanIsValid(h));
    assert_eq!(PLANNER_CALLS.with(Cell::get), planner_before + 1);
    assert_eq!(CachedPlanStmtList(p2).len(), 1);
    assert_eq!(SourceQueryList(h).len(), 1);
    ReleaseCachedPlan(p2);
    DropCachedPlan(h);
}

#[test]
fn requires_reanalysis_probe_tracks_invalidation() {
    install();
    push_snapshot();
    let h = make_saved_source(false);
    assert!(!CachedPlanSourceRequiresReanalysis(h).unwrap());
    ResetPlanCache();
    assert!(CachedPlanSourceRequiresReanalysis(h).unwrap());
    DropCachedPlan(h);

    let h_txn = CreateCachedPlan(None, "", types_portal::CMDTAG_UNKNOWN).unwrap();
    let qmcx = SourceQueryMcx(h_txn);
    CompleteCachedPlan(h_txn, PgVec::new_in(qmcx), &[], 0, false).unwrap();
    assert!(!CachedPlanSourceRequiresReanalysis(h_txn).unwrap());
    DropCachedPlan(h_txn);
}

#[test]
fn release_to_zero_frees_and_stale_handle_is_loud() {
    install();
    push_snapshot();
    let h = make_saved_source(false);
    let p = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    ReleaseCachedPlan(p); // portal's count; gplan still holds one
    DropCachedPlan(h); // releases the gplan refcount -> frees the plan
    let stale = std::panic::catch_unwind(|| CachedPlanStmtList(p));
    assert!(stale.is_err());
    let stale_src = std::panic::catch_unwind(|| CachedPlanIsValid(h));
    assert!(stale_src.is_err());
}

// The exact C fivefold heuristic: five custom plans first, then generic wins
// iff its cost undercuts the average custom cost (which carries the planning
// charge).
#[test]
fn choose_custom_plan_fivefold_heuristic() {
    install();
    push_snapshot();
    let params = ParamListHandle(0xDEAD);

    let h = make_saved_source(false);
    for expected_calls in 1..=5 {
        let p = GetCachedPlan(h, params, None, QueryEnvHandle::NULL).unwrap();
        ReleaseCachedPlan(p);
        assert_eq!(PLANNER_CALLS.with(Cell::get), expected_calls);
    }
    // Sixth call: builds the generic plan, generic_cost (1.0) < avg custom
    // (1.0 + planning charge) -> generic chosen and cached.
    let p6 = GetCachedPlan(h, params, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 6);
    let p7 = GetCachedPlan(h, params, None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 6);
    assert_eq!(p6, p7);
    ReleaseCachedPlan(p6);
    ReleaseCachedPlan(p7);
    DropCachedPlan(h);

    // Expensive generic: after the probe build, choose_custom_plan flips back
    // to custom (C's recheck wart) and the losing generic is kept unexecuted.
    let h2 = make_saved_source(false);
    for _ in 0..5 {
        PLANNER_COST.with(|c| c.set(1.0));
        let p = GetCachedPlan(h2, params, None, QueryEnvHandle::NULL).unwrap();
        ReleaseCachedPlan(p);
    }
    PLANNER_COST.with(|c| c.set(1000.0));
    let calls_before = PLANNER_CALLS.with(Cell::get);
    let p = GetCachedPlan(h2, params, None, QueryEnvHandle::NULL).unwrap();
    // Generic probe build + custom rebuild.
    assert_eq!(PLANNER_CALLS.with(Cell::get), calls_before + 2);
    ReleaseCachedPlan(p);
    PLANNER_COST.with(|c| c.set(1.0));
    DropCachedPlan(h2);
}

#[test]
fn force_generic_plan_mode_skips_customs() {
    install();
    push_snapshot();
    PLAN_CACHE_MODE_VAR.with(|c| c.set(guc_tables::consts::PLAN_CACHE_MODE_FORCE_GENERIC_PLAN));
    let h = make_saved_source(false);
    let p = GetCachedPlan(h, ParamListHandle(0xBEEF), None, QueryEnvHandle::NULL).unwrap();
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1);
    ReleaseCachedPlan(p);
    DropCachedPlan(h);
}

#[test]
fn drop_with_live_plan_reference_defers_source_free() {
    install();
    push_snapshot();
    let h = make_saved_source(false);
    let p = GetCachedPlan(h, ParamListHandle::NULL, None, QueryEnvHandle::NULL).unwrap();
    DropCachedPlan(h);
    // Plan survives its dropped source until the last release (C refcount).
    assert!(!CachedPlanStmtList(p).is_empty());
    ReleaseCachedPlan(p);
}
