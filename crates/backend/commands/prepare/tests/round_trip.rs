use std::cell::Cell;
use std::sync::Once;

use mcx::MemoryContext;
use types_core::{CommandTag, Oid, INT4OID};
use types_dest::CommandDest;
use types_nodes::list::NodeList;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use types_nodes::plannodes::{Plan, PlannedStmt, Result as ResultPlan};
use types_nodes::primnodes::{Const, TargetEntry};
use types_nodes::rawnodes::SelectStmt;
use types_nodes::NodeTag;
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::lock::LOCKACQUIRE_OK;
use types_portal::CMDTAG_SELECT;

use prepare::*;
use types_error::{PgResult, ERRCODE_DUPLICATE_PSTATEMENT, ERRCODE_INVALID_PSTATEMENT_DEFINITION, ERRCODE_UNDEFINED_PSTATEMENT};
use types_nodes::parsenodes::{DeallocateStmt, ExecuteStmt, PrepareStmt};
use types_nodes::rawnodes::RawStmt;
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK};

const TEST_RELID: Oid = 60001;
const SOURCE: &str = "PREPARE p1 AS SELECT 1";

thread_local! {
    static PLANNER_CALLS: Cell<u32> = const { Cell::new(0) };
    static QD_CREATES: Cell<u32> = const { Cell::new(0) };
    static QD_RUNS: Cell<u32> = const { Cell::new(0) };
    static QD_SNAPSHOT: std::cell::RefCell<Option<execmain_seams::Snapshot>> =
        const { std::cell::RefCell::new(None) };
    static PORTALS_ENABLED: Cell<bool> = const { Cell::new(false) };
    static PLAN_CACHE_MODE_VAR: Cell<i32> = const { Cell::new(0) };
}

fn select_query(mcx: mcx::Mcx<'_>) -> Query<'_> {
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
    let mut target_list = NodeList::nil();
    target_list.lappend(mcx, tle).unwrap();
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
    let mut rtable = NodeList::nil();
    rtable.lappend(mcx, rte).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        targetList: target_list,
        rtable,
        ..Query::default()
    }
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
        ResultPlan { plan: Plan { total_cost: 0.01, ..Plan::default() }, resconstantqual: None },
    )?;
    let mut relation_oids = types_nodes::list::OidList::nil();
    relation_oids.lappend(mcx, TEST_RELID)?;
    Ok(PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        canSetTag: parse.canSetTag,
        planTree: Some(tree),
        rtable: parse.rtable.clone_in(mcx)?,
        relationOids: relation_oids,
        ..PlannedStmt::default()
    })
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        plancache::init_seams();
        utility::init_seams();
        guc_tables::init_seams();
        miscinit::init_seams();
        parallel_seams::is_parallel_worker::set(|| false);
        transam_xlog_seams::recovery_in_progress::set(|| false);
        postgres_seams::check_for_interrupts::set(|| Ok(()));

        parser_seams::raw_parser::set(|mcx, query_string, _mode| {
            assert_eq!(query_string, SOURCE);
            let select = Node::mk(mcx, SelectStmt::default())?;
            let prep = Node::mk(
                mcx,
                PrepareStmt { name: Some("p1"), argtypes: NodeList::nil(), query: Some(select) },
            )?;
            let mut v = mcx::PgVec::new_in(mcx);
            v.push(RawStmt { stmt: Some(prep), stmt_location: 0, stmt_len: 0 });
            Ok(v)
        });
        analyze_seams::parse_analyze_fixedparams::set(|mcx, parse_tree, _src, _types, _env| {
            assert_eq!(
                parse_tree.stmt.map(|s| s.node_tag()),
                Some(NodeTag::T_SelectStmt)
            );
            Ok(select_query(mcx))
        });
        analyze_seams::parse_analyze_varparams::set(|mcx, parse_tree, _src, param_types, _env| {
            assert_eq!(
                parse_tree.stmt.map(|s| s.node_tag()),
                Some(NodeTag::T_SelectStmt)
            );
            let mut resolved = mcx::PgVec::new_in(mcx);
            for &t in param_types {
                resolved.push(t);
            }
            Ok((select_query(mcx), resolved))
        });
        analyze_seams::analyze_requires_snapshot::set(|raw| {
            parser_analyze::analyze_requires_snapshot(raw)
        });
        resowner_seams::current_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        resowner_seams::set_current_resource_owner::set(|_| {});
        resowner_seams::top_transaction_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        rewrite_handler_seams::query_rewrite::set(|mcx, query| {
            let mut v = mcx::PgVec::new_in(mcx);
            v.push(query);
            Ok(v)
        });
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
        aclchk_seams::pg_class_aclmask::set(|_relid, _roleid, mask, _how_all| Ok(mask));
        aclchk_seams::object_aclcheck::set(|_, _, _, _| Ok(0));
        syscache_seams::lookup_authid_rolname::set(|_, _| Ok(None));
        syscache_seams::lookup_pg_namespace_oid_by_name::set(|_| Ok(types_core::InvalidOid));
        syscache_seams::pg_namespace_nspname::set(|_| Ok(None));
        inval_seams::accept_invalidation_messages::set(|| Ok(()));
        lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| Ok(LOCKACQUIRE_OK));
        lock_seams::lock_release::set(|_, _, _| Ok(true));
        lock_seams::mark_lock_clear::set(|_, _| {});
        catalog_seams::is_shared_relation::set(|_| false);
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        xact_portal_seams::get_current_statement_start_timestamp::set(|| 424_242);
        resowner_portal_seams::resource_owner_create_portal::set(|| {
            types_resowner::ResourceOwner::from_parts(1, 1)
        });
        resowner_portal_seams::resource_owner_release::set(|_, _, _, _| {});
        resowner_portal_seams::resource_owner_delete::set(|_| {});
        ipc_portal_seams::shmem_exit_inprogress::set(|| false);
        portalcmds_seams::portal_cleanup::set(|_| Ok(()));

        execmain_seams::create_query_desc::set(
            |pstmt, _src, snapshot, _cross, _dest, _params, _env, _instr| {
                assert_eq!(pstmt.commandType, CmdType::CMD_SELECT);
                assert_eq!(
                    pstmt.planTree.map(|t| t.node_tag()),
                    Some(NodeTag::T_Result),
                    "EXECUTE must run the cached Result plan"
                );
                QD_CREATES.with(|c| c.set(c.get() + 1));
                QD_SNAPSHOT.with(|s| *s.borrow_mut() = snapshot);
                Ok(types_portal::QueryDescHandle(7))
            },
        );
        execmain_seams::free_query_desc::set(|_| {});
        execmain_seams::release_query_desc::set(|_| {});
        pquery::init_seams();
        execmain_seams::executor_start::set(|_, _| Ok(()));
        execmain_seams::executor_run::set(|_, _, _, _| {
            QD_RUNS.with(|c| c.set(c.get() + 1));
            Ok(())
        });
        execmain_seams::executor_finish::set(|_| Ok(()));
        execmain_seams::executor_end::set(|_| Ok(()));
        execmain_seams::query_desc_es_processed::set(|_| 1);
        execmain_seams::query_desc_snapshot::set(|_| QD_SNAPSHOT.with(|s| s.borrow().clone()));
        execmain_seams::query_desc_result_tupdesc::set(|_| None);
        execmain_seams::query_desc_operation::set(|_| CmdType::CMD_SELECT);

        if !guc_tables::vars::plan_cache_mode.installed() {
            guc_tables::vars::plan_cache_mode.install(guc_tables::GucVarAccessors {
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
    if !PORTALS_ENABLED.with(Cell::get) {
        portalmem::EnablePortalManager();
        PORTALS_ENABLED.with(|c| c.set(true));
    }
    let base = Box::leak(Box::new(MemoryContext::new("prepare-test"))).mcx();
    let snap: snapmgr::Snapshot =
        std::rc::Rc::new(SnapshotData::sentinel(base, SnapshotType::SNAPSHOT_MVCC));
    snapmgr::PushActiveSnapshot(&snap).unwrap();
    PLANNER_CALLS.with(|c| c.set(0));
    QD_CREATES.with(|c| c.set(0));
    QD_RUNS.with(|c| c.set(0));
}

fn run_utility(node: Node<'_>, source: &str) -> PgResult<QueryCompletion> {
    let ctx = MemoryContext::new("stmt");
    let pstmt = PlannedStmt {
        commandType: CmdType::CMD_UTILITY,
        canSetTag: true,
        utilityStmt: Some(node),
        stmt_location: 0,
        stmt_len: source.len() as i32,
        ..PlannedStmt::default()
    };
    let mut receiver = tcop_dest::CreateDestReceiver(CommandDest::None);
    let mut qc = QueryCompletion::default();
    utility::ProcessUtility(
        ctx.mcx(),
        &pstmt,
        source,
        false,
        utility_seams::PROCESS_UTILITY_TOPLEVEL,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        &mut receiver,
        Some(&mut qc),
    )?;
    Ok(qc)
}

fn node_mk<'mcx, T: types_nodes::node_tree::NodeVariant<'mcx>>(
    ctx: &'mcx MemoryContext,
    payload: T,
) -> Node<'mcx> {
    Node::mk(ctx.mcx(), payload).unwrap()
}

#[test]
fn prepare_execute_deallocate_round_trip() {
    install();
    let ctx = MemoryContext::new("t");

    // PREPARE through the utility dispatch and the real plancache.
    let select = node_mk(&ctx, SelectStmt::default());
    let prep = node_mk(
        &ctx,
        PrepareStmt { name: Some("p1"), argtypes: NodeList::nil(), query: Some(select) },
    );
    run_utility(prep, SOURCE).unwrap();
    let entry = FetchPreparedStatement("p1", true).unwrap().unwrap();
    assert!(entry.from_sql);
    assert_eq!(entry.prepare_time, 0); // real xact stmt-start ts, unset in tests
    assert!(plancache::CachedPlanIsValid(entry.plansource));
    assert_eq!(PLANNER_CALLS.with(Cell::get), 0);

    // Duplicate PREPARE: C's 42P05.
    let select2 = node_mk(&ctx, SelectStmt::default());
    let prep2 = node_mk(
        &ctx,
        PrepareStmt { name: Some("p1"), argtypes: NodeList::nil(), query: Some(select2) },
    );
    let err = run_utility(prep2, SOURCE).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_PSTATEMENT);

    // First EXECUTE builds the generic plan and runs it.
    let exec = node_mk(&ctx, ExecuteStmt { name: Some("p1"), params: NodeList::nil() });
    let qc = run_utility(exec, "EXECUTE p1").unwrap();
    assert_eq!(qc.commandTag, CMDTAG_SELECT);
    assert_eq!(qc.nprocessed, 1);
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1);
    assert_eq!(QD_RUNS.with(Cell::get), 1);

    // Second EXECUTE is the warm hit: no replan, same cached stmt list.
    let exec2 = node_mk(&ctx, ExecuteStmt { name: Some("p1"), params: NodeList::nil() });
    let qc = run_utility(exec2, "EXECUTE p1").unwrap();
    assert_eq!(qc.nprocessed, 1);
    assert_eq!(PLANNER_CALLS.with(Cell::get), 1);
    assert_eq!(QD_RUNS.with(Cell::get), 2);

    // UtilityReturnsTuples/UtilityTupleDescriptor see the prepared entry.
    let exec3 = node_mk(&ctx, ExecuteStmt { name: Some("p1"), params: NodeList::nil() });
    assert!(utility::UtilityReturnsTuples(exec3));
    assert!(utility::UtilityTupleDescriptor(exec3).unwrap().is_some());

    // DEALLOCATE drops the entry and the plancache source.
    let dealloc = node_mk(
        &ctx,
        DeallocateStmt { name: Some("p1"), isall: false, location: -1 },
    );
    run_utility(dealloc, "DEALLOCATE p1").unwrap();
    assert!(FetchPreparedStatement("p1", false).unwrap().is_none());

    let exec4 = node_mk(&ctx, ExecuteStmt { name: Some("p1"), params: NodeList::nil() });
    let err = run_utility(exec4, "EXECUTE p1").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_PSTATEMENT);
}

#[test]
fn inval_callback_flushes_the_saved_plan() {
    install();
    let ctx = MemoryContext::new("t");
    let select = node_mk(&ctx, SelectStmt::default());
    let prep = node_mk(
        &ctx,
        PrepareStmt { name: Some("p_inval"), argtypes: NodeList::nil(), query: Some(select) },
    );
    // The stub parser always names p1; store under p_inval via direct calls.
    let _ = prep;
    let tag = CommandTag::SELECT;
    let raw = RawStmt { stmt: Some(select), stmt_location: 0, stmt_len: 0 };
    let plansource = plancache::CreateCachedPlan(Some(&raw), SOURCE, tag).unwrap();
    let qmcx = plancache::SourceQueryMcx(plansource);
    let mut qlist = mcx::PgVec::new_in(qmcx);
    qlist.push(select_query(qmcx));
    plancache::CompleteCachedPlan(plansource, qlist, &[], CURSOR_OPT_PARALLEL_OK, true).unwrap();
    StorePreparedStatement("p_inval", plansource, true).unwrap();

    let exec = node_mk(&ctx, ExecuteStmt { name: Some("p_inval"), params: NodeList::nil() });
    run_utility(exec, "EXECUTE p_inval").unwrap();
    assert!(plancache::CachedPlanIsValid(plansource));

    plancache::PlanCacheRelCallback(datum::Datum::from_oid(types_core::InvalidOid), TEST_RELID);
    assert!(!plancache::CachedPlanIsValid(plansource));

    DropPreparedStatement("p_inval", true).unwrap();
}

#[test]
fn deallocate_all_drops_everything() {
    install();
    let select = {
        let mcx = Box::leak(Box::new(MemoryContext::new("da"))).mcx();
        Node::mk(mcx, SelectStmt::default()).unwrap()
    };
    for name in ["da1", "da2"] {
        let raw = RawStmt { stmt: Some(select), stmt_location: 0, stmt_len: 0 };
        let plansource =
            plancache::CreateCachedPlan(Some(&raw), SOURCE, CommandTag::SELECT).unwrap();
        let qmcx = plancache::SourceQueryMcx(plansource);
        let mut qlist = mcx::PgVec::new_in(qmcx);
        qlist.push(select_query(qmcx));
        plancache::CompleteCachedPlan(plansource, qlist, &[], CURSOR_OPT_PARALLEL_OK, true)
            .unwrap();
        StorePreparedStatement(name, plansource, true).unwrap();
    }
    let ctx = MemoryContext::new("t");
    let dealloc = node_mk(&ctx, DeallocateStmt { name: None, isall: true, location: -1 });
    run_utility(dealloc, "DEALLOCATE ALL").unwrap();
    assert!(FetchPreparedStatement("da1", false).unwrap().is_none());
    assert!(FetchPreparedStatement("da2", false).unwrap().is_none());
}

#[test]
fn empty_statement_name_is_rejected() {
    install();
    let ctx = MemoryContext::new("t");
    let select = node_mk(&ctx, SelectStmt::default());
    let stmt = PrepareStmt { name: Some(""), argtypes: NodeList::nil(), query: Some(select) };
    let err = PrepareQuery(SOURCE, &stmt, 0, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PSTATEMENT_DEFINITION);
}

// EvaluateParams' arity check (C prepare.c 42601).
#[test]
fn execute_with_wrong_parameter_count_is_42601() {
    install();
    let ctx = MemoryContext::new("t");

    let select = node_mk(&ctx, SelectStmt::default());
    let rawstmt = RawStmt { stmt: Some(select), stmt_location: 0, stmt_len: 0 };
    let plansource =
        plancache::CreateCachedPlan(Some(&rawstmt), "PREPARE pn(int) AS SELECT $1", CMDTAG_SELECT)
            .unwrap();
    let qmcx = plancache::SourceQueryMcx(plansource);
    let mut qlist = mcx::PgVec::new_in(qmcx);
    qlist.push(select_query(qmcx));
    plancache::CompleteCachedPlan(plansource, qlist, &[INT4OID], CURSOR_OPT_PARALLEL_OK, true)
        .unwrap();
    StorePreparedStatement("pn", plansource, true).unwrap();

    let exec = ExecuteStmt { name: Some("pn"), params: NodeList::nil() };
    let mut dest = tcop_dest::CreateDestReceiver(CommandDest::None);
    let err = ExecuteQuery(
        ctx.mcx(),
        &exec,
        "EXECUTE pn",
        ParamListHandle::NULL,
        None,
        &mut dest,
        None,
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert!(err.message().contains("wrong number of parameters"));
}

#[test]
fn deallocate_command_tags() {
    install();
    let ctx = MemoryContext::new("t");
    let one = node_mk(&ctx, DeallocateStmt { name: Some("x"), isall: false, location: -1 });
    let all = node_mk(&ctx, DeallocateStmt { name: None, isall: true, location: -1 });
    assert_eq!(cmdtag::GetCommandTagName(utility::CreateCommandTag(one)), "DEALLOCATE");
    assert_eq!(cmdtag::GetCommandTagName(utility::CreateCommandTag(all)), "DEALLOCATE ALL");
}
