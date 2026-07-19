use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Once;

use datum::{Datum, VarlenaRef};
use mcx::{Mcx, MemoryContext};
use tcop_dest::DestReceiver;
use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{DefElem, ExplainStmt, Query, TransactionStmt};
use types_nodes::primnodes::FromExpr;
use types_nodes::Node;
use types_portal::{
    CachedPlanHandle, ParamListHandle, Portal, PortalCleanupHook, PortalData, PortalStatus,
    PortalStrategy, QueryCompletion, QueryDescHandle, QueryEnvHandle, StmtListHandle,
    TuplestoreHandle, CMDTAG_UNKNOWN,
};
use types_snapshot::{SnapshotData, SNAPSHOT_MVCC};

use crate::*;

const INT4OID: u32 = 23;
const INT4OUT: u32 = 43;
const TEXTOUT: u32 = 47;
const JSONOUT: u32 = 322;
const XMLOUT: u32 = 2894;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
}

fn textout_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    // SAFETY: test datum is a live 4B-header text varlena.
    let v = unsafe { VarlenaRef::from_ptr(fcinfo.arg(0).as_usize() as *const u8) };
    let mut s = v.data().to_vec();
    s.push(0);
    Ok(Datum::from_usize(
        Box::leak(s.into_boxed_slice()).as_ptr() as usize
    ))
}

fn int4out_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let mut s = fcinfo.arg(0).as_i32().to_string().into_bytes();
    s.push(0);
    Ok(Datum::from_usize(
        Box::leak(s.into_boxed_slice()).as_ptr() as usize
    ))
}

// Proc/shmem substrate for snapmgr's MyProc xmin writes (snapmgr tests' shape).
fn install_proc_fixture() {
    use init_small::globals as g;
    // Proc slots are never released; every #[test] thread claims one, so the
    // budget must exceed the test count.
    g::SetMaxConnections(64);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(64 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(777);

    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    pg_sema_seams::pg_semaphore_lock::set(|_| {});
    pg_sema_seams::pg_semaphore_unlock::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    latch_seams::own_latch::set(|_| {});
    latch_seams::disown_latch::set(|_| {});
    latch_seams::set_latch::set(|_| {});
    latch_seams::set_latch_my_latch::set(|| {});
    latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
    latch_seams::reset_latch_my_latch::set(|| {});
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    autovacuum_seams::wake_autovacuum_launcher::set(|| {});
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| Ok(()));
    timeout_seams::disable_timeouts::set(|_| {});
    shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
    shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
    shmem_seams::shmem_alloc::set(|size| {
        Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
    });
    transam_xlog_seams::recovery_in_progress::set(|| false);
    subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
    syscache_seams::relation_invalidates_snapshots_only::set(|_| false);
    syscache_seams::relation_has_sys_cache::set(|_| true);

    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    procarray::init_seams();
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();
    snapmgr::init_seams();
}

// MyProc is per-thread; every test thread registers its own proc.
fn my_backend() {
    thread_local! {
        static THREAD_PROC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if !THREAD_PROC.get() {
        init_small::globals::SetMyProcPid(777);
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
        THREAD_PROC.set(true);
    }
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        install_proc_fixture();
        crate::init_seams();
        planner::init_seams();
        rewrite_handler::init_seams();
        execmain::init_seams();
        xact::init_seams();
        elog::init_seams();
        backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
        backend_status_seams::pgstat_report_query_id::set(|_, _| {});
        relcache_seams::relation_get_stat_ext_list::set(|mcx, _relid| Ok(mcx::PgVec::new_in(mcx)));
        resowner_seams::current_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        resowner_seams::set_current_resource_owner::set(|_| {});
        resowner_seams::top_transaction_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        resowner_seams::resource_owner_enlarge::set(|_| Ok(()));
        resowner_seams::resource_owner_remember_snapshot::set(|_, _| {});
        resowner_seams::resource_owner_forget_snapshot::set(|_, _| {});
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(types_tuple::PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: types_tuple::TYPALIGN_INT,
                    typstorage: types_tuple::TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                types_core::TEXTOID => Some(types_tuple::PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: types_tuple::TYPALIGN_INT,
                    typstorage: b'x' as i8,
                    typcollation: 100,
                }),
                crate::JSONOID | crate::XMLOID => Some(types_tuple::PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: types_tuple::TYPALIGN_INT,
                    typstorage: b'x' as i8,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            Ok(0)
        });
        mbutils_seams::server_to_client_conversion_needed::set(|| false);
        mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
        lsyscache_seams::get_type_output_info::set(|oid| match oid {
            types_core::TEXTOID => Ok((TEXTOUT, true)),
            crate::JSONOID => Ok((JSONOUT, true)),
            crate::XMLOID => Ok((XMLOUT, true)),
            _ => panic!("get_type_output_info: unexpected oid {oid}"),
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(syscache_seams::PgTypeIoShape {
                    oid: INT4OID,
                    typinput: 42,
                    typoutput: INT4OUT,
                    typreceive: 2406,
                    typsend: 2407,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: 0,
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                types_core::TEXTOID => Some(syscache_seams::PgTypeIoShape {
                    oid: types_core::TEXTOID,
                    typinput: 46,
                    typoutput: TEXTOUT,
                    typreceive: 2414,
                    typsend: 2415,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: 0,
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                crate::JSONOID => Some(syscache_seams::PgTypeIoShape {
                    oid: crate::JSONOID,
                    typinput: 321,
                    typoutput: JSONOUT,
                    typreceive: 3805,
                    typsend: 3804,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: 0,
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                crate::XMLOID => Some(syscache_seams::PgTypeIoShape {
                    oid: crate::XMLOID,
                    typinput: 2893,
                    typoutput: XMLOUT,
                    typreceive: 2896,
                    typsend: 2897,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: 0,
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }),
                _ => None,
            })
        });
        namespace_seams::type_is_visible::set(|_| Ok(true));
        namespace_seams::is_temp_namespace::set(|_| false);
        syscache_seams::pg_type_typnamespace::set(|_| Ok(Some(11)));
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            let mk = |name: &str| {
                let mut typname = types_tuple::NameData::default();
                typname.namestrcpy(name);
                syscache_seams::PgTypeTypcacheShape {
                    typname,
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typtype: b'b' as i8,
                    typisdefined: true,
                    typrelid: 0,
                    typsubscript: 0,
                    typelem: 0,
                    typarray: 0,
                    typcollation: 0,
                }
            };
            Ok(match typid {
                INT4OID => Some(mk("int4")),
                types_core::TEXTOID => Some(mk("text")),
                _ => None,
            })
        });
        fmgr_seams::fmgr_info::set(|oid| match oid {
            TEXTOUT => Ok(FmgrInfo::new(textout_fn, TEXTOUT, 1, true, false)),
            INT4OUT => Ok(FmgrInfo::new(int4out_fn, INT4OUT, 1, true, false)),
            // json_out/xml_out share textout's varlena-passthrough shape.
            JSONOUT => Ok(FmgrInfo::new(textout_fn, JSONOUT, 1, true, false)),
            XMLOUT => Ok(FmgrInfo::new(textout_fn, XMLOUT, 1, true, false)),
            _ => panic!("fmgr_info: unexpected oid {oid}"),
        });
        guc_tables::vars::standard_conforming_strings.install(guc_tables::GucVarAccessors {
            get: || true,
            set: |_| {},
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("explain-test")));
    m.mcx()
}

// The analyzer's output for `SELECT 1` (planner tests' fixture shape).
fn select_1_query(mcx: Mcx<'_>) -> Query<'_> {
    let konst = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
    let jointree =
        mcx::alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 8,
        ..Query::default()
    }
}

fn union_all_query(mcx: Mcx<'static>) -> Query<'static> {
    use types_nodes::list::{IntList, OidList};
    let leaf = |v: i32| {
        let konst = Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap();
        let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
        let jointree =
            mcx::alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            targetList: NodeList::make1(mcx, tle).unwrap(),
            stmt_location: 0,
            stmt_len: 8,
            ..Query::default()
        }
    };
    let rte = |q: Query<'static>, name: &'static str| {
        let colnames = NodeList::make1(mcx, Node::mk_string(mcx, "?column?").unwrap()).unwrap();
        let eref = mcx::alloc_leak_in(
            mcx,
            types_nodes::primnodes::Alias { aliasname: Some(name), colnames },
        )
        .unwrap();
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = types_nodes::parsenodes::RTEKind::RTE_SUBQUERY;
        rte.subquery = Some(mcx::alloc_leak_in(mcx, q).unwrap());
        rte.eref = Some(eref);
        rte.alias = Some(eref);
        rte.seal()
    };
    let mut rtable = NodeList::make1(mcx, rte(leaf(1), "*SELECT* 1")).unwrap();
    rtable.lappend(mcx, rte(leaf(2), "*SELECT* 2")).unwrap();
    let stmt = Node::mk(
        mcx,
        types_nodes::parsenodes::SetOperationStmt {
            op: types_nodes::parsenodes::SetOperation::SETOP_UNION,
            all: true,
            larg: Some(Node::mk_range_tbl_ref(mcx, 1).unwrap()),
            rarg: Some(Node::mk_range_tbl_ref(mcx, 2).unwrap()),
            colTypes: OidList::make1(mcx, INT4OID).unwrap(),
            colTypmods: IntList::make1(mcx, -1).unwrap(),
            colCollations: OidList::make1(mcx, 0).unwrap(),
            groupClauses: NodeList::nil(),
        },
    )
    .unwrap();
    let v = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, v, 1, Some("?column?"), false).unwrap();
    let jointree =
        mcx::alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: NodeList::make1(mcx, tle).unwrap(),
        setOperations: Some(stmt),
        stmt_location: 0,
        stmt_len: 27,
        ..Query::default()
    }
}

fn opt<'mcx>(mcx: Mcx<'mcx>, name: &'static str, arg: Option<Node<'mcx>>) -> Node<'mcx> {
    Node::mk(mcx, DefElem { defname: Some(name), arg, ..DefElem::default() }).unwrap()
}

fn explain_stmt<'mcx>(mcx: Mcx<'mcx>, options: &[Node<'mcx>]) -> ExplainStmt<'mcx> {
    let query = Node::mk(mcx, select_1_query(mcx)).unwrap();
    let options = if options.is_empty() {
        NodeList::nil()
    } else {
        NodeList::from_slice(mcx, options).unwrap()
    };
    ExplainStmt { query: Some(query), options }
}

fn make_portal(mcx: Mcx<'_>) -> Portal<'_> {
    Portal::new(PortalData {
        name: mcx::PgString::new_in(mcx),
        prepStmtName: None,
        portalContext: None,
        plansource: ::types_portal::PlanSourceHandle::NULL,
        planContext: core::ptr::null_mut(),
        resowner: Default::default(),
        cleanup: PortalCleanupHook::None,
        createSubid: 0,
        activeSubid: 0,
        createLevel: 0,
        sourceText: None,
        commandTag: CMDTAG_UNKNOWN,
        qc: QueryCompletion::default(),
        stmts: StmtListHandle::NULL,
        cplan: CachedPlanHandle::NULL,
        portalParams: ParamListHandle::NULL,
        queryEnv: QueryEnvHandle::NULL,
        strategy: PortalStrategy::default(),
        cursorOptions: 0,
        status: PortalStatus::default(),
        portalPinned: false,
        autoHeld: false,
        queryDesc: QueryDescHandle::NULL,
        tupDesc: None,
        formats: mcx::PgVec::new_in(mcx),
        portalSnapshot: None,
        holdStore: TuplestoreHandle::NULL,
        holdContext: None,
        holdSnapshot: None,
        atStart: true,
        atEnd: false,
        portalPos: 0,
        creation_time: 0,
        visible: false,
        // WS-CA wave-10 (cursors inc-2): mechanical literal completion only.
        cursorStoreArmed: false,
        cursorStore: TuplestoreHandle::NULL,
        cursorFillExhausted: false,
        currentOfEligible: None,
        cursorCaptureBatch: false,
        cursorTidStore: TuplestoreHandle::NULL,
    })
}

fn sent_rows() -> Vec<String> {
    SENT.with(|s| {
        s.borrow()
            .iter()
            .filter(|(t, _)| *t == b'D')
            .map(|(_, b)| {
                assert_eq!(i16::from_be_bytes([b[0], b[1]]), 1);
                let len = i32::from_be_bytes([b[2], b[3], b[4], b[5]]) as usize;
                String::from_utf8(b[6..6 + len].to_vec()).unwrap()
            })
            .collect()
    })
}

// Runs ExplainQuery end-to-end (rewrite -> plan -> ExecutorStart -> text ->
// printtup) and returns the emitted QUERY PLAN rows.
fn run_explain_stmt(mcx: Mcx<'static>, stmt: &ExplainStmt<'static>) -> Vec<String> {
    install_fixtures();
    my_backend();
    SENT.with(|s| s.borrow_mut().clear());

    let snap = Rc::new(SnapshotData::sentinel(leaked_mcx(), SNAPSHOT_MVCC));
    snapmgr::PushActiveSnapshot(&snap).unwrap();

    let mut dr = printtup::printtup_create_DR(types_dest::CommandDest::RemoteExecute);
    printtup::SetRemoteDestReceiverParams(&mut dr, make_portal(mcx));
    let mut dest = DestReceiver::PrintTup(dr);

    let result = ExplainQuery(
        mcx,
        stmt,
        "EXPLAIN SELECT 1",
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        &mut dest,
    );
    snapmgr::PopActiveSnapshot().unwrap();
    result.unwrap();
    sent_rows()
}

fn run_explain(options: &[&'static str]) -> Vec<String> {
    install_fixtures();
    let mcx = leaked_mcx();
    let opts: Vec<Node<'_>> = options.iter().map(|n| opt(mcx, n, None)).collect();
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    run_explain_stmt(mcx, stmt)
}

// Expected lines pinned against real PostgreSQL 18.3 (psql -c 'EXPLAIN ...',
// captured 2026-07-02).
#[test]
fn explain_select_1_matches_pg() {
    assert_eq!(run_explain(&[]), ["Result  (cost=0.00..0.01 rows=1 width=4)"]);
}

#[test]
fn explain_union_all_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let query = Node::mk(mcx, union_all_query(mcx)).unwrap();
    let stmt = mcx::alloc_leak_in(
        mcx,
        ExplainStmt { query: Some(query), options: NodeList::nil() },
    )
    .unwrap();
    assert_eq!(
        run_explain_stmt(mcx, stmt),
        [
            "Append  (cost=0.00..0.03 rows=2 width=4)",
            "  ->  Result  (cost=0.00..0.01 rows=1 width=4)",
            "  ->  Result  (cost=0.00..0.01 rows=1 width=4)",
        ]
    );
}

#[test]
fn get_const_expr_matches_ruleutils() {
    install_fixtures();
    let mcx = leaked_mcx();
    let deparse = |c: Node<'static>| {
        ruleutils::deparse_expression_pretty(mcx, c, types_core::InvalidOid, false, 0).unwrap()
    };

    let int = |v: i32, isnull: bool| {
        Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), isnull, true).unwrap()
    };
    assert_eq!(deparse(int(1, false)), "1");
    assert_eq!(deparse(int(-42, false)), "'-42'::integer");
    assert_eq!(deparse(int(0, true)), "NULL::integer");

    let text = |s: &str| {
        let hdr = (((4 + s.len()) as u32) << 2).to_le_bytes();
        let mut image = hdr.to_vec();
        image.extend_from_slice(s.as_bytes());
        let d = Datum::from_usize(Box::leak(image.into_boxed_slice()).as_ptr() as usize);
        Node::mk_const(mcx, types_core::TEXTOID, -1, 0, -1, d, false, false).unwrap()
    };
    assert_eq!(deparse(text("hello")), "'hello'::text");
    assert_eq!(deparse(text("it's")), "'it''s'::text");
}

#[test]
fn explain_verbose_matches_pg() {
    assert_eq!(
        run_explain(&["verbose"]),
        ["Result  (cost=0.00..0.01 rows=1 width=4)", "  Output: 1"]
    );
}

#[test]
fn explain_costs_off_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let off = Node::mk_boolean(mcx, false).unwrap();
    let opts = [opt(mcx, "costs", Some(off)), opt(mcx, "verbose", None)];
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    assert_eq!(run_explain_stmt(mcx, stmt), ["Result", "  Output: 1"]);
}

#[test]
fn explain_summary_appends_planning_time() {
    let rows = run_explain(&["summary"]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], "Result  (cost=0.00..0.01 rows=1 width=4)");
    assert!(rows[1].starts_with("Planning Time: "), "{}", rows[1]);
    assert!(rows[1].ends_with(" ms"), "{}", rows[1]);
}

#[test]
fn explain_utility_statement_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let begin = Node::mk(mcx, TransactionStmt::default()).unwrap();
    let query = Node::mk(
        mcx,
        Query {
            commandType: CmdType::CMD_UTILITY,
            canSetTag: true,
            utilityStmt: Some(begin),
            ..Query::default()
        },
    )
    .unwrap();
    let stmt =
        mcx::alloc_leak_in(mcx, ExplainStmt { query: Some(query), options: NodeList::nil() })
            .unwrap();
    assert_eq!(run_explain_stmt(mcx, stmt), ["Utility statements have no plan structure"]);
}

#[test]
fn option_errors_match_c_sqlstates() {
    install_fixtures();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "bogus", None)).unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_SYNTAX_ERROR);

    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "timing", None)).unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

    let mut es = NewExplainState(mcx).unwrap();
    let bogus = Node::mk_string(mcx, "bogus").unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "format", Some(bogus))).unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

    let mut es = NewExplainState(mcx).unwrap();
    let opts =
        NodeList::from_slice(mcx, &[opt(mcx, "generic_plan", None), opt(mcx, "analyze", None)])
            .unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn option_defaults_match_c() {
    install_fixtures();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut es = NewExplainState(mcx).unwrap();
    ParseExplainOptionList(&mut es, mcx, &NodeList::nil(), "").unwrap();
    assert!(es.costs);
    assert!(!es.verbose && !es.analyze && !es.timing && !es.summary && !es.buffers);
    assert_eq!(es.format, EXPLAIN_FORMAT_TEXT);
    assert_eq!(es.serialize, EXPLAIN_SERIALIZE_NONE);

    // ANALYZE defaults timing/buffers/summary on.
    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "analyze", None)).unwrap();
    ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap();
    assert!(es.analyze && es.timing && es.buffers && es.summary);
}

// pgrust-only EXPLAIN (ENGINE) (single-executor migration Phase 0.2):
// default-absent, ordinary boolean parse, requires ANALYZE in increment 1
// (the TIMING requires-analyze validation shape).
#[test]
fn engine_option_parses_and_requires_analyze() {
    install_fixtures();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    // Default absent — and stays absent under bare ANALYZE.
    let mut es = NewExplainState(mcx).unwrap();
    ParseExplainOptionList(&mut es, mcx, &NodeList::nil(), "").unwrap();
    assert!(!es.engine);
    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "analyze", None)).unwrap();
    ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap();
    assert!(!es.engine);

    // ENGINE without ANALYZE errors with the requires-ANALYZE sqlstate.
    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "engine", None)).unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

    // ENGINE OFF without ANALYZE is fine (matches WAL/TIMING-off semantics).
    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, off(mcx, "engine")).unwrap();
    ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap();
    assert!(!es.engine);

    // ENGINE + ANALYZE parses.
    let mut es = NewExplainState(mcx).unwrap();
    let opts =
        NodeList::from_slice(mcx, &[opt(mcx, "engine", None), opt(mcx, "analyze", None)])
            .unwrap();
    ParseExplainOptionList(&mut es, mcx, &opts, "").unwrap();
    assert!(es.engine && es.analyze);
}

#[test]
fn result_desc_is_one_text_column() {
    install_fixtures();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let stmt = ExplainStmt::default();
    let desc = ExplainResultDesc(mcx, &stmt).unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(desc.attr(0).atttypid, types_core::TEXTOID);
    assert_eq!(desc.attr(0).attname.name_str(), b"QUERY PLAN");
}

fn off<'mcx>(mcx: Mcx<'mcx>, name: &'static str) -> Node<'mcx> {
    let b = Node::mk_boolean(mcx, false).unwrap();
    opt(mcx, name, Some(b))
}

fn es_text<'a>(es: &'a ExplainState<'_>) -> &'a str {
    std::str::from_utf8(es.str.as_bytes()).unwrap()
}

// Pinned against C 18.3 ExplainPrintJIT (explain.c) text output; flag bits
// are jit.h's PGJIT_*.
#[test]
fn jit_block_matches_pg() {
    install_fixtures();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let expr_deform = (1 << 0) | (1 << 3) | (1 << 4);

    let mut es = NewExplainState(mcx).unwrap();
    es.analyze = true;
    es.timing = true;
    crate::ExplainPrintJIT(&mut es, expr_deform, 3, 1_234_567);
    assert_eq!(
        es_text(&es),
        "JIT:\n\
         \x20 Functions: 3\n\
         \x20 Options: Inlining false, Optimization false, Expressions true, Deforming true\n\
         \x20 Timing: Generation 1.235 ms (Deform 0.000 ms), Inlining 0.000 ms, \
         Optimization 0.000 ms, Emission 0.000 ms, Total 1.235 ms\n"
    );

    // Plain EXPLAIN (no analyze/timing) omits the Timing line.
    let mut es = NewExplainState(mcx).unwrap();
    crate::ExplainPrintJIT(&mut es, 0b11111, 1, 0);
    assert_eq!(
        es_text(&es),
        "JIT:\n\
         \x20 Functions: 1\n\
         \x20 Options: Inlining true, Optimization true, Expressions true, Deforming true\n"
    );

    // created_functions == 0 suppresses the whole block.
    let mut es = NewExplainState(mcx).unwrap();
    es.analyze = true;
    es.timing = true;
    crate::ExplainPrintJIT(&mut es, expr_deform, 0, 55);
    assert_eq!(es_text(&es), "");
}

// Pinned against real PostgreSQL 18.3: EXPLAIN (ANALYZE, TIMING OFF,
// SUMMARY OFF, BUFFERS OFF) SELECT 1 (captured 2026-07-02, Homebrew 18.3).
#[test]
fn explain_analyze_timing_off_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let opts = [
        opt(mcx, "analyze", None),
        off(mcx, "timing"),
        off(mcx, "summary"),
        off(mcx, "buffers"),
    ];
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    assert_eq!(
        run_explain_stmt(mcx, stmt),
        ["Result  (cost=0.00..0.01 rows=1 width=4) (actual rows=1.00 loops=1)"]
    );
}

// Pinned against real PostgreSQL 18.3 EXPLAIN (ANALYZE, BUFFERS OFF) SELECT 1
// with actual/planning/execution times normalized (the regress-harness rule:
// row counts, loops, and shape exact; times variable).
#[test]
fn explain_analyze_timed_shape_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let opts = [opt(mcx, "analyze", None), off(mcx, "buffers")];
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    let rows = run_explain_stmt(mcx, stmt);
    assert_eq!(rows.len(), 3, "{rows:?}");
    let head = "Result  (cost=0.00..0.01 rows=1 width=4) (actual time=";
    let tail = " rows=1.00 loops=1)";
    assert!(rows[0].starts_with(head) && rows[0].ends_with(tail), "{}", rows[0]);
    let times = &rows[0][head.len()..rows[0].len() - tail.len()];
    let (start, total) = times.split_once("..").expect("time=START..TOTAL");
    for t in [start, total] {
        let (_, frac) = t.split_once('.').expect("ms with fraction");
        assert_eq!(frac.len(), 3, "%.3f millisecond format: {t}");
    }
    assert!(rows[1].starts_with("Planning Time: ") && rows[1].ends_with(" ms"), "{}", rows[1]);
    assert!(rows[2].starts_with("Execution Time: ") && rows[2].ends_with(" ms"), "{}", rows[2]);
}

// Pinned against real PostgreSQL 18.3: EXPLAIN (ANALYZE, TIMING OFF, SUMMARY
// OFF, BUFFERS OFF, COSTS OFF) SELECT 1 LIMIT 0.
#[test]
fn explain_analyze_never_executed_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let zero = Node::mk_const(mcx, 20, -1, 0, 8, Datum::from_i64(0), false, true).unwrap();
    let query =
        Node::mk(mcx, Query { limitCount: Some(zero), ..select_1_query(mcx) }).unwrap();
    let opts: Vec<Node<'_>> = ["timing", "summary", "buffers", "costs"]
        .iter()
        .map(|n| off(mcx, n))
        .chain([opt(mcx, "analyze", None)])
        .collect();
    let stmt = mcx::alloc_leak_in(
        mcx,
        ExplainStmt { query: Some(query), options: NodeList::from_slice(mcx, &opts).unwrap() },
    )
    .unwrap();
    assert_eq!(
        run_explain_stmt(mcx, stmt),
        ["Limit (actual rows=0.00 loops=1)", "  ->  Result (never executed)"]
    );
}

#[test]
#[should_panic(expected = "xloginsert lane")]
fn analyze_wal_is_loud() {
    install_fixtures();
    let mcx = leaked_mcx();
    let opts = [opt(mcx, "analyze", None), opt(mcx, "wal", None)];
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    let _ = run_explain_stmt(mcx, stmt);
}

// show_buffer_usage text arm, values chosen to cover C's comma placement.
#[test]
fn show_buffer_usage_matches_c_shape() {
    install_fixtures();
    let mcx = leaked_mcx();
    let mut es = NewExplainState(mcx).unwrap();
    let mut u = types_core::instrument::BufferUsage {
        shared_blks_hit: 3,
        shared_blks_read: 2,
        temp_blks_written: 5,
        ..Default::default()
    };
    assert!(crate::peek_buffer_usage(&es, &u));
    crate::show_buffer_usage(&mut es, &u);
    assert_eq!(es_text(&es), "Buffers: shared hit=3 read=2, temp written=5\n");

    u = Default::default();
    assert!(!crate::peek_buffer_usage(&es, &u));
    let mut es = NewExplainState(mcx).unwrap();
    crate::show_buffer_usage(&mut es, &u);
    assert_eq!(es_text(&es), "");
}

fn fmt<'mcx>(mcx: Mcx<'mcx>, name: &'static str) -> Node<'mcx> {
    let s = Node::mk_string(mcx, name).unwrap();
    opt(mcx, "format", Some(s))
}

fn run_explain_fmt(format: &'static str, extra: &[&'static str]) -> Vec<String> {
    install_fixtures();
    let mcx = leaked_mcx();
    let mut opts = vec![fmt(mcx, format)];
    opts.extend(extra.iter().map(|n| opt(mcx, n, None)));
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    run_explain_stmt(mcx, stmt)
}

// Expected strings derived from explain_format.c/explain.c REL_18_3 emitters
// over the same plan the TEXT tests pin against real PostgreSQL 18.3.
#[test]
fn explain_format_json_matches_pg() {
    assert_eq!(
        run_explain_fmt("json", &[]),
        [concat!(
            "[\n",
            "  {\n",
            "    \"Plan\": {\n",
            "      \"Node Type\": \"Result\",\n",
            "      \"Parallel Aware\": false,\n",
            "      \"Async Capable\": false,\n",
            "      \"Startup Cost\": 0.00,\n",
            "      \"Total Cost\": 0.01,\n",
            "      \"Plan Rows\": 1,\n",
            "      \"Plan Width\": 4,\n",
            "      \"Disabled\": false\n",
            "    }\n",
            "  }\n",
            "]"
        )]
    );
}

#[test]
fn explain_format_json_costs_off_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let costs_off = off(mcx, "costs");
    let opts = [fmt(mcx, "json"), costs_off];
    let stmt = mcx::alloc_leak_in(mcx, explain_stmt(mcx, &opts)).unwrap();
    assert_eq!(
        run_explain_stmt(mcx, stmt),
        [concat!(
            "[\n",
            "  {\n",
            "    \"Plan\": {\n",
            "      \"Node Type\": \"Result\",\n",
            "      \"Parallel Aware\": false,\n",
            "      \"Async Capable\": false,\n",
            "      \"Disabled\": false\n",
            "    }\n",
            "  }\n",
            "]"
        )]
    );
}

#[test]
fn explain_format_json_verbose_matches_pg() {
    assert_eq!(
        run_explain_fmt("json", &["verbose"]),
        [concat!(
            "[\n",
            "  {\n",
            "    \"Plan\": {\n",
            "      \"Node Type\": \"Result\",\n",
            "      \"Parallel Aware\": false,\n",
            "      \"Async Capable\": false,\n",
            "      \"Startup Cost\": 0.00,\n",
            "      \"Total Cost\": 0.01,\n",
            "      \"Plan Rows\": 1,\n",
            "      \"Plan Width\": 4,\n",
            "      \"Disabled\": false,\n",
            "      \"Output\": [\"1\"]\n",
            "    }\n",
            "  }\n",
            "]"
        )]
    );
}

#[test]
fn explain_format_yaml_matches_pg() {
    assert_eq!(
        run_explain_fmt("yaml", &[]),
        [concat!(
            "- Plan: \n",
            "    Node Type: \"Result\"\n",
            "    Parallel Aware: false\n",
            "    Async Capable: false\n",
            "    Startup Cost: 0.00\n",
            "    Total Cost: 0.01\n",
            "    Plan Rows: 1\n",
            "    Plan Width: 4\n",
            "    Disabled: false"
        )]
    );
}

#[test]
fn explain_format_xml_matches_pg() {
    assert_eq!(
        run_explain_fmt("xml", &[]),
        [concat!(
            "<explain xmlns=\"http://www.postgresql.org/2009/explain\">\n",
            "  <Query>\n",
            "    <Plan>\n",
            "      <Node-Type>Result</Node-Type>\n",
            "      <Parallel-Aware>false</Parallel-Aware>\n",
            "      <Async-Capable>false</Async-Capable>\n",
            "      <Startup-Cost>0.00</Startup-Cost>\n",
            "      <Total-Cost>0.01</Total-Cost>\n",
            "      <Plan-Rows>1</Plan-Rows>\n",
            "      <Plan-Width>4</Plan-Width>\n",
            "      <Disabled>false</Disabled>\n",
            "    </Plan>\n",
            "  </Query>\n",
            "</explain>"
        )]
    );
}

#[test]
fn explain_format_json_union_all_matches_pg() {
    install_fixtures();
    let mcx = leaked_mcx();
    let query = Node::mk(mcx, union_all_query(mcx)).unwrap();
    let opts = NodeList::make1(mcx, fmt(mcx, "json")).unwrap();
    let stmt =
        mcx::alloc_leak_in(mcx, ExplainStmt { query: Some(query), options: opts }).unwrap();
    let member = |relationship: &str| {
        format!(
            concat!(
                "        {{\n",
                "          \"Node Type\": \"Result\",\n",
                "          \"Parent Relationship\": \"{}\",\n",
                "          \"Parallel Aware\": false,\n",
                "          \"Async Capable\": false,\n",
                "          \"Startup Cost\": 0.00,\n",
                "          \"Total Cost\": 0.01,\n",
                "          \"Plan Rows\": 1,\n",
                "          \"Plan Width\": 4,\n",
                "          \"Disabled\": false\n",
                "        }}"
            ),
            relationship
        )
    };
    let expected = format!(
        concat!(
            "[\n",
            "  {{\n",
            "    \"Plan\": {{\n",
            "      \"Node Type\": \"Append\",\n",
            "      \"Parallel Aware\": false,\n",
            "      \"Async Capable\": false,\n",
            "      \"Startup Cost\": 0.00,\n",
            "      \"Total Cost\": 0.03,\n",
            "      \"Plan Rows\": 2,\n",
            "      \"Plan Width\": 4,\n",
            "      \"Disabled\": false,\n",
            "      \"Subplans Removed\": 0,\n",
            "      \"Plans\": [\n",
            "{},\n",
            "{}\n",
            "      ]\n",
            "    }}\n",
            "  }}\n",
            "]"
        ),
        member("Member"),
        member("Member")
    );
    assert_eq!(run_explain_stmt(mcx, stmt), [expected]);
}

// EXPLAIN SELECT pk FROM t ORDER BY val LIMIT 2 through the REAL pipeline
// (rewrite -> planner ORDER BY/LIMIT lane -> ExecutorStart(EXPLAIN_ONLY) ->
// text). Expected lines pinned against real PostgreSQL 18.3 (psql EXPLAIN over
// a 45-page table, rescaled to this 100-page fixture; formulas verified
// identical 2026-07-02).
mod order_by_limit_e2e {
    use super::*;
    use types_nodes::parsenodes::{RTEKind, SortGroupClause};
    use types_nodes::primnodes::Alias;

    const TBL: u32 = 24680;
    const IDX: u32 = 24681;
    const INT4EQ_OP: u32 = 96;
    const INT4_LT_OP: u32 = 97;
    const INT4_GT_OP: u32 = 521;
    const INT4_BTREE_FAM: u32 = 1976;
    const INT4_BTREE_OPCLASS: u32 = 1978;
    const INT8OID: u32 = 20;

    fn install_scan_fixtures() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            relation_seams::relation_open::set(|mcx, relid, _lockmode| {
                assert_eq!(relid, TBL);
                Ok(make_heap_rel(mcx))
            });
            bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| Ok(100));
            syscache_seams::pg_class_relname::set(|relid| {
                let mut n = types_tuple::NameData::default();
                match relid {
                    TBL => n.namestrcpy("t"),
                    IDX => n.namestrcpy("t_pk_idx"),
                    _ => return Ok(None),
                }
                Ok(Some(n))
            });
            syscache_seams::pg_operator_oprname::set(|opno| {
                let mut n = types_tuple::NameData::default();
                match opno {
                    INT4EQ_OP => n.namestrcpy("="),
                    _ => return Ok(None),
                }
                Ok(Some(n))
            });
            syscache_seams::lookup_pg_class_ls_shape::set(|relid| {
                Ok((relid == TBL).then(|| syscache_seams::PgClassLsShape {
                    relnamespace: 2200,
                    reltype: 0,
                    relam: 2,
                    reltablespace: 0,
                    relnatts: 2,
                    relkind: b'r' as i8,
                    relpersistence: b'p' as i8,
                    relispartition: false,
                    relhassubclass: false,
                }))
            });
            syscache_seams::lookup_pg_attribute_shape::set(|relid, attnum| {
                if relid != TBL {
                    return Ok(None);
                }
                let mut n = types_tuple::NameData::default();
                match attnum {
                    1 => n.namestrcpy("pk"),
                    2 => n.namestrcpy("val"),
                    _ => return Ok(None),
                }
                Ok(Some(syscache_seams::PgAttributeLsShape {
                    attname: n,
                    atttypid: INT4OID,
                    atttypmod: -1,
                    attcollation: 0,
                    attgenerated: 0,
                }))
            });
            syscache_seams::lookup_pg_statistic_shape::set(|_, _, _| Ok(None));
            syscache_seams::lookup_pg_statistic_bundle::set(|_, _, _, _| Ok(None));
            syscache_seams::pg_statistic_stawidth::set(|_, _, _| Ok(None));
            syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
                let mut v = ::mcx::PgVec::new_in(mcx);
                if opno == INT4EQ_OP || opno == INT4_LT_OP {
                    v.push(syscache_seams::PgAmopMemberShape {
                        amopfamily: INT4_BTREE_FAM,
                        amoplefttype: INT4OID,
                        amoprighttype: INT4OID,
                        amopstrategy: if opno == INT4EQ_OP { 3 } else { 1 },
                        amopmethod: 403,
                    });
                }
                Ok(v)
            });
            syscache_seams::lookup_pg_opfamily_shape::set(|opfid| {
                Ok((opfid == INT4_BTREE_FAM).then(|| syscache_seams::PgOpfamilyShape {
                    opfmethod: 403,
                    opfname: types_tuple::NameData::default(),
                }))
            });
            syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
                Ok(match (opfamily, left, right, strategy) {
                    (INT4_BTREE_FAM, INT4OID, INT4OID, 1) => INT4_LT_OP,
                    (INT4_BTREE_FAM, INT4OID, INT4OID, 3) => INT4EQ_OP,
                    (INT4_BTREE_FAM, INT4OID, INT4OID, 5) => INT4_GT_OP,
                    _ => 0,
                })
            });
            syscache_seams::syscache_hash_value_typeoid::set(|typid| {
                Ok(typid.wrapping_mul(0x9e37_79b1))
            });
            indexcmds_seams::get_default_opclass::set(|typid, am| {
                Ok(if typid == INT4OID && am == 403 { INT4_BTREE_OPCLASS } else { 0 })
            });
            syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
                Ok((opclass == INT4_BTREE_OPCLASS).then(|| syscache_seams::PgOpclassShape {
                    opcmethod: 403,
                    opcfamily: INT4_BTREE_FAM,
                    opcintype: INT4OID,
                    // int4 opclasses store no separate key type (pg_opclass: 0).
                    opckeytype: ::types_core::InvalidOid,
                }))
            });
        });
    }

    fn int4_attr(attnum: i16, name: &str) -> types_tuple::FormData_pg_attribute {
        let mut attname = types_tuple::NameData::default();
        attname.namestrcpy(name);
        types_tuple::FormData_pg_attribute {
            attrelid: TBL,
            attname,
            atttypid: INT4OID,
            attlen: 4,
            attnum,
            atttypmod: -1,
            attbyval: true,
            attalign: types_tuple::TYPALIGN_INT,
            attstorage: types_tuple::TYPSTORAGE_PLAIN,
            attislocal: true,
            ..Default::default()
        }
    }

    fn make_heap_rel<'mcx>(mcx: Mcx<'mcx>) -> types_rel::Relation<'mcx> {
        use std::cell::Cell;
        let mut attrs = ::mcx::PgVec::new_in(mcx);
        attrs.push(int4_attr(1, "pk"));
        attrs.push(int4_attr(2, "val"));
        let mut compact_attrs = ::mcx::PgVec::new_in(mcx);
        for a in attrs.iter() {
            compact_attrs.push(types_tuple::CompactAttribute::populate_from(a));
        }
        let rd_att = Rc::new(types_tuple::TupleDescData {
            natts: 2,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs,
            attrs,
        });
        let mut relname = types_tuple::NameData::default();
        relname.namestrcpy("t");
        let rd_rel = types_rel::FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: TBL,
            reltablespace: 0,
            relpages: 100,
            reltuples: 10000.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: b'p',
            relkind: types_rel::RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: types_rel::REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        };
        types_rel::Relation::open(
            types_rel::RelationData {
                rd_locator: Default::default(),
                rd_smgr: Default::default(),
                rd_id: TBL,
                rd_backend: types_core::INVALID_PROC_NUMBER,
                rd_islocaltemp: false,
                rd_isvalid: Cell::new(true),
                rd_createSubid: Cell::new(0),
                rd_newRelfilelocatorSubid: Cell::new(0),
                rd_firstRelfilelocatorSubid: Cell::new(0),
                rd_droppedSubid: Cell::new(0),
                rd_lockInfo: types_rel::LockInfoData {
                    lockRelId: types_rel::LockRelId { relId: TBL, dbId: 5 },
                },
                rd_rel,
                rd_att,
                rd_index: None,
                rd_opcintype: ::mcx::PgVec::new_in(mcx),
                rd_opfamily: ::mcx::PgVec::new_in(mcx),
                rd_indoption: ::mcx::PgVec::new_in(mcx),
                rd_indcollation: ::mcx::PgVec::new_in(mcx),
                rd_options: None,
                pgstat_enabled: Cell::new(false),
                pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
                rd_amcache: Default::default(),
                rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
                rd_support: ::mcx::PgVec::new_in(mcx),
                rd_supportinfo: Default::default(),
                rd_opcoptions: Default::default(),
                rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
            },
            None,
        )
    }

    // Analyzer output for `SELECT pk FROM t ORDER BY val LIMIT 2`.
    fn order_by_limit_query(mcx: Mcx<'static>) -> Query<'static> {
        let mut colnames = NodeList::nil();
        colnames.lappend(mcx, Node::mk_string(mcx, "pk").unwrap()).unwrap();
        colnames.lappend(mcx, Node::mk_string(mcx, "val").unwrap()).unwrap();
        let eref =
            mcx::alloc_leak_in(mcx, Alias { aliasname: Some("t"), colnames }).unwrap();
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = TBL;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.inh = false;
        rte.eref = Some(eref);
        let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
        let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
        let jointree = mcx::alloc_leak_in(
            mcx,
            FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
        )
        .unwrap();

        let pk = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
        let val = Node::mk_var(mcx, 1, 2, INT4OID, -1, 0, 0).unwrap();
        let mut tl = NodeList::make1(
            mcx,
            Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap(),
        )
        .unwrap();
        tl.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: val,
                    resno: 2,
                    resname: Some("val"),
                    ressortgroupref: 1,
                    resorigtbl: 0,
                    resorigcol: 0,
                    resjunk: true,
                },
            )
            .unwrap(),
        )
        .unwrap();

        Query {
            commandType: CmdType::CMD_SELECT,
            canSetTag: true,
            jointree: Some(jointree),
            rtable,
            targetList: tl,
            sortClause: NodeList::make1(
                mcx,
                Node::mk(
                    mcx,
                    SortGroupClause {
                        tleSortGroupRef: 1,
                        eqop: INT4EQ_OP,
                        sortop: INT4_LT_OP,
                        reverse_sort: false,
                        nulls_first: false,
                        hashable: true,
                    },
                )
                .unwrap(),
            )
            .unwrap(),
            limitCount: Some(
                Node::mk_const(mcx, INT8OID, -1, 0, 8, Datum::from_i64(2), false, true).unwrap(),
            ),
            limitOption: types_nodes::nodes_enums::LimitOption::LIMIT_OPTION_COUNT,
            stmt_location: 0,
            stmt_len: 38,
            ..Query::default()
        }
    }

    #[test]
    fn explain_order_by_limit_matches_pg() {
        install_fixtures();
        install_scan_fixtures();
        let mcx = leaked_mcx();
        let query = Node::mk(mcx, order_by_limit_query(mcx)).unwrap();
        let stmt = mcx::alloc_leak_in(
            mcx,
            ExplainStmt { query: Some(query), options: NodeList::nil() },
        )
        .unwrap();
        assert_eq!(
            run_explain_stmt(mcx, stmt),
            [
                "Limit  (cost=300.00..300.01 rows=2 width=8)",
                "  ->  Sort  (cost=300.00..325.00 rows=10000 width=8)",
                "        Sort Key: val",
                "        ->  Seq Scan on t  (cost=0.00..200.00 rows=10000 width=8)",
            ]
        );
    }

    // C 18: EXPLAIN SELECT * FROM t WHERE pk = 5 (unique index t_pk_idx on pk),
    // and the covering variant for the IOS shape.
    fn index_scan_pstmt<'mcx>(mcx: Mcx<'mcx>, index_only: bool) -> &'mcx PlannedStmt<'mcx> {
        let mut eref_cols = NodeList::make1(mcx, Node::mk_string(mcx, "pk").unwrap()).unwrap();
        eref_cols.lappend(mcx, Node::mk_string(mcx, "payload").unwrap()).unwrap();
        let eref = mcx::alloc_leak_in(
            mcx,
            Alias { aliasname: Some("t"), colnames: eref_cols },
        )
        .unwrap();
        let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
        rte.rtekind = RTEKind::RTE_RELATION;
        rte.relid = TBL;
        rte.relkind = b'r';
        rte.rellockmode = 1;
        rte.eref = Some(eref);
        let rtable = NodeList::make1(mcx, rte.seal()).unwrap();

        // Index Cond deparse (generate_operator_name) needs a booted catcache;
        // its byte-compare vs live C lives in scripts/explain-verbose-e2e.sh.
        let plan_tree = if index_only {
            let ios_var =
                Node::mk_var(mcx, types_nodes::primnodes::INDEX_VAR, 1, INT4OID, -1, 0, 0).unwrap();
            let tle = Node::mk_target_entry(mcx, ios_var, 1, Some("pk"), false).unwrap();
            let itl_var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
            let itl_tle = Node::mk_target_entry(mcx, itl_var, 1, None, false).unwrap();
            let mut s = Node::build::<types_nodes::plannodes::IndexOnlyScan>(mcx).unwrap();
            s.scan.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
            s.scan.plan.startup_cost = 0.29;
            s.scan.plan.total_cost = 8.30;
            s.scan.plan.plan_rows = 1.0;
            s.scan.plan.plan_width = 4;
            s.scan.scanrelid = 1;
            s.indexid = IDX;
            s.indexorderdir = 1;
            s.indextlist = NodeList::make1(mcx, itl_tle).unwrap();
            s.seal()
        } else {
            let v1 = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
            let v2 = Node::mk_var(mcx, 1, 2, 20, -1, 0, 0).unwrap();
            let tle1 = Node::mk_target_entry(mcx, v1, 1, Some("pk"), false).unwrap();
            let tle2 = Node::mk_target_entry(mcx, v2, 2, Some("payload"), false).unwrap();
            let mut tl = NodeList::make1(mcx, tle1).unwrap();
            tl.lappend(mcx, tle2).unwrap();
            let mut s = Node::build::<types_nodes::plannodes::IndexScan>(mcx).unwrap();
            s.scan.plan.targetlist = tl;
            s.scan.plan.startup_cost = 0.29;
            s.scan.plan.total_cost = 8.30;
            s.scan.plan.plan_rows = 1.0;
            s.scan.plan.plan_width = 12;
            s.scan.scanrelid = 1;
            s.indexid = IDX;
            s.indexorderdir = 1;
            s.seal()
        };

        mcx::alloc_leak_in(
            mcx,
            PlannedStmt {
                commandType: CmdType::CMD_SELECT,
                canSetTag: true,
                planTree: Some(plan_tree),
                rtable,
                ..PlannedStmt::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn explain_index_scan_matches_pg() {
        super::install_fixtures();
        install_scan_fixtures();
        let mcx = leaked_mcx();
        let pstmt = index_scan_pstmt(mcx, false);
        let mut es = crate::state::NewExplainState(mcx).unwrap();
        crate::node::ExplainPrintPlan(mcx, &mut es, pstmt).unwrap();
        assert_eq!(
            es_text(&es),
            "Index Scan using t_pk_idx on t  (cost=0.29..8.30 rows=1 width=12)\n"
        );
    }

    #[test]
    fn explain_index_only_scan_matches_pg() {
        super::install_fixtures();
        install_scan_fixtures();
        let mcx = leaked_mcx();
        let pstmt = index_scan_pstmt(mcx, true);
        let mut es = crate::state::NewExplainState(mcx).unwrap();
        crate::node::ExplainPrintPlan(mcx, &mut es, pstmt).unwrap();
        assert_eq!(
            es_text(&es),
            "Index Only Scan using t_pk_idx on t  (cost=0.29..8.30 rows=1 width=4)\n"
        );
    }

    #[test]
    fn explain_format_json_sort_matches_pg() {
        install_fixtures();
        install_scan_fixtures();
        let mcx = leaked_mcx();
        let query = Node::mk(mcx, order_by_limit_query(mcx)).unwrap();
        let opts = NodeList::make1(mcx, super::fmt(mcx, "json")).unwrap();
        let stmt =
            mcx::alloc_leak_in(mcx, ExplainStmt { query: Some(query), options: opts }).unwrap();
        assert_eq!(
            run_explain_stmt(mcx, stmt),
            [concat!(
                "[\n",
                "  {\n",
                "    \"Plan\": {\n",
                "      \"Node Type\": \"Limit\",\n",
                "      \"Parallel Aware\": false,\n",
                "      \"Async Capable\": false,\n",
                "      \"Startup Cost\": 300.00,\n",
                "      \"Total Cost\": 300.01,\n",
                "      \"Plan Rows\": 2,\n",
                "      \"Plan Width\": 8,\n",
                "      \"Disabled\": false,\n",
                "      \"Plans\": [\n",
                "        {\n",
                "          \"Node Type\": \"Sort\",\n",
                "          \"Parent Relationship\": \"Outer\",\n",
                "          \"Parallel Aware\": false,\n",
                "          \"Async Capable\": false,\n",
                "          \"Startup Cost\": 300.00,\n",
                "          \"Total Cost\": 325.00,\n",
                "          \"Plan Rows\": 10000,\n",
                "          \"Plan Width\": 8,\n",
                "          \"Disabled\": false,\n",
                "          \"Sort Key\": [\"val\"],\n",
                "          \"Plans\": [\n",
                "            {\n",
                "              \"Node Type\": \"Seq Scan\",\n",
                "              \"Parent Relationship\": \"Outer\",\n",
                "              \"Parallel Aware\": false,\n",
                "              \"Async Capable\": false,\n",
                "              \"Relation Name\": \"t\",\n",
                "              \"Alias\": \"t\",\n",
                "              \"Startup Cost\": 0.00,\n",
                "              \"Total Cost\": 200.00,\n",
                "              \"Plan Rows\": 10000,\n",
                "              \"Plan Width\": 8,\n",
                "              \"Disabled\": false\n",
                "            }\n",
                "          ]\n",
                "        }\n",
                "      ]\n",
                "    }\n",
                "  }\n",
                "]"
            )]
        );
    }

    fn nestloop_pstmt<'mcx>(mcx: Mcx<'mcx>) -> &'mcx PlannedStmt<'mcx> {
        // explicit=true mirrors the parser's rte->alias for "from t, t u";
        // without it set_rtable_names falls back to get_rel_name.
        let mk_rte = |alias: &'static str, explicit: bool| {
            let mut colnames = NodeList::make1(mcx, Node::mk_string(mcx, "pk").unwrap()).unwrap();
            colnames.lappend(mcx, Node::mk_string(mcx, "val").unwrap()).unwrap();
            let eref =
                mcx::alloc_leak_in(mcx, Alias { aliasname: Some(alias), colnames }).unwrap();
            let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
            rte.rtekind = RTEKind::RTE_RELATION;
            rte.relid = TBL;
            rte.relkind = b'r';
            rte.rellockmode = 1;
            rte.eref = Some(eref);
            if explicit {
                rte.alias = Some(
                    mcx::alloc_leak_in(
                        mcx,
                        Alias { aliasname: Some(alias), colnames: NodeList::nil() },
                    )
                    .unwrap(),
                );
            }
            rte.seal()
        };
        let mut rtable = NodeList::make1(mcx, mk_rte("t", false)).unwrap();
        rtable.lappend(mcx, mk_rte("u", true)).unwrap();

        let mk_scan = |scanrelid: u32| {
            let mut s = Node::build::<types_nodes::plannodes::SeqScan>(mcx).unwrap();
            s.scan.plan.startup_cost = 0.0;
            s.scan.plan.total_cost = 1.01;
            s.scan.plan.plan_rows = 2.0;
            s.scan.plan.plan_width = 4;
            s.scan.scanrelid = scanrelid;
            s.seal()
        };
        let mut nl = Node::build::<types_nodes::plannodes::NestLoop>(mcx).unwrap();
        nl.join.plan.startup_cost = 0.0;
        nl.join.plan.total_cost = 4.06;
        nl.join.plan.plan_rows = 4.0;
        nl.join.plan.plan_width = 8;
        nl.join.plan.lefttree = Some(mk_scan(1));
        nl.join.plan.righttree = Some(mk_scan(2));
        nl.join.jointype = types_nodes::JoinType::JOIN_INNER;
        nl.join.inner_unique = false;
        let plan_tree = nl.seal();

        mcx::alloc_leak_in(
            mcx,
            PlannedStmt {
                commandType: CmdType::CMD_SELECT,
                canSetTag: true,
                planTree: Some(plan_tree),
                rtable,
                ..PlannedStmt::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn explain_format_json_join_matches_pg() {
        install_fixtures();
        install_scan_fixtures();
        let mcx = leaked_mcx();
        let pstmt = nestloop_pstmt(mcx);
        let mut es = crate::state::NewExplainState(mcx).unwrap();
        es.format = crate::state::EXPLAIN_FORMAT_JSON;
        crate::format::ExplainBeginOutput(&mut es);
        crate::format::ExplainOpenGroup("Query", None, true, &mut es);
        crate::node::ExplainPrintPlan(mcx, &mut es, pstmt).unwrap();
        crate::format::ExplainCloseGroup("Query", None, true, &mut es);
        crate::format::ExplainEndOutput(&mut es);
        assert_eq!(
            es_text(&es),
            concat!(
                "[\n",
                "  {\n",
                "    \"Plan\": {\n",
                "      \"Node Type\": \"Nested Loop\",\n",
                "      \"Parallel Aware\": false,\n",
                "      \"Async Capable\": false,\n",
                "      \"Join Type\": \"Inner\",\n",
                "      \"Startup Cost\": 0.00,\n",
                "      \"Total Cost\": 4.06,\n",
                "      \"Plan Rows\": 4,\n",
                "      \"Plan Width\": 8,\n",
                "      \"Disabled\": false,\n",
                "      \"Inner Unique\": false,\n",
                "      \"Plans\": [\n",
                "        {\n",
                "          \"Node Type\": \"Seq Scan\",\n",
                "          \"Parent Relationship\": \"Outer\",\n",
                "          \"Parallel Aware\": false,\n",
                "          \"Async Capable\": false,\n",
                "          \"Relation Name\": \"t\",\n",
                "          \"Alias\": \"t\",\n",
                "          \"Startup Cost\": 0.00,\n",
                "          \"Total Cost\": 1.01,\n",
                "          \"Plan Rows\": 2,\n",
                "          \"Plan Width\": 4,\n",
                "          \"Disabled\": false\n",
                "        },\n",
                "        {\n",
                "          \"Node Type\": \"Seq Scan\",\n",
                "          \"Parent Relationship\": \"Inner\",\n",
                "          \"Parallel Aware\": false,\n",
                "          \"Async Capable\": false,\n",
                "          \"Relation Name\": \"t\",\n",
                "          \"Alias\": \"u\",\n",
                "          \"Startup Cost\": 0.00,\n",
                "          \"Total Cost\": 1.01,\n",
                "          \"Plan Rows\": 2,\n",
                "          \"Plan Width\": 4,\n",
                "          \"Disabled\": false\n",
                "        }\n",
                "      ]\n",
                "    }\n",
                "  }\n",
                "]"
            )
        );
    }
}
