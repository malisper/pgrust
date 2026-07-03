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
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
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
        resowner_seams::current_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
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
                _ => None,
            })
        });
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
fn get_const_expr_matches_ruleutils() {
    install_fixtures();
    let mcx = leaked_mcx();
    let deparse = |c: Node<'static>| {
        let mut buf = mcx::PgString::new_in(mcx);
        crate::node::get_const_expr(c.as_const().unwrap(), &mut buf, 0).unwrap();
        buf.as_str().to_string()
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
    let err = ParseExplainOptionList(&mut es, mcx, &opts).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_SYNTAX_ERROR);

    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "timing", None)).unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

    let mut es = NewExplainState(mcx).unwrap();
    let bogus = Node::mk_string(mcx, "bogus").unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "format", Some(bogus))).unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);

    let mut es = NewExplainState(mcx).unwrap();
    let opts =
        NodeList::from_slice(mcx, &[opt(mcx, "generic_plan", None), opt(mcx, "analyze", None)])
            .unwrap();
    let err = ParseExplainOptionList(&mut es, mcx, &opts).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn option_defaults_match_c() {
    install_fixtures();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut es = NewExplainState(mcx).unwrap();
    ParseExplainOptionList(&mut es, mcx, &NodeList::nil()).unwrap();
    assert!(es.costs);
    assert!(!es.verbose && !es.analyze && !es.timing && !es.summary && !es.buffers);
    assert_eq!(es.format, EXPLAIN_FORMAT_TEXT);
    assert_eq!(es.serialize, EXPLAIN_SERIALIZE_NONE);

    // ANALYZE defaults timing/buffers/summary on.
    let mut es = NewExplainState(mcx).unwrap();
    let opts = NodeList::make1(mcx, opt(mcx, "analyze", None)).unwrap();
    ParseExplainOptionList(&mut es, mcx, &opts).unwrap();
    assert!(es.analyze && es.timing && es.buffers && es.summary);
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

#[test]
#[should_panic(expected = "instrument lane")]
fn analyze_is_loud() {
    let _ = run_explain(&["analyze"]);
}

#[test]
#[should_panic(expected = "non-text format lane")]
fn json_format_is_loud() {
    install_fixtures();
    let mcx = leaked_mcx();
    let json = Node::mk_string(mcx, "json").unwrap();
    let opts = [opt(mcx, "format", Some(json))];
    let stmt = explain_stmt(mcx, &opts);
    let mut dest = DestReceiver::DoNothing;
    let _ = ExplainQuery(mcx, &stmt, "q", ParamListHandle::NULL, QueryEnvHandle::NULL, &mut dest);
}
