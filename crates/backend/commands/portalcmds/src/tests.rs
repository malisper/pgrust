use std::cell::RefCell;
use std::sync::Once;

use ::mcx::{Mcx, MemoryContext};
use ::types_dest::CommandDest;
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::{
    ClosePortalStmt, DeclareCursorStmt, FetchDirection::*, FetchStmt, Query,
};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_portal::{
    CachedPlanHandle, ParamListHandle, QueryCompletion, QueryEnvHandle, CMDTAG_FETCH, CMDTAG_MOVE,
    CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL, FETCH_ALL, PORTAL_ONE_SELECT,
};

use crate::{PerformCursorOpen, PerformPortalClose, PerformPortalFetch};

const INT4OID: u32 = 23;
const F_INT4OUT: u32 = 43;

thread_local! {
    static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
}

fn int4out_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<datum::Datum> {
    let mut s = fcinfo.arg(0).as_i32().to_string().into_bytes();
    s.push(0);
    Ok(datum::Datum::from_usize(
        Box::leak(s.into_boxed_slice()).as_ptr() as usize,
    ))
}

// Proc/shmem substrate for snapmgr's MyProc xmin writes (pquery e2e's shape).
fn install_proc_fixture() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(778);

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

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        // Fake global-vis BEFORE procarray::init_seams (its set is guarded;
        // scanfix's controllable fake must win).
        procarray_seams::global_vis_test_for::set(|_r| types_core::GlobalVisStateHandle::new(0));
        install_proc_fixture();
        crate::init_seams();
        pquery::init_seams();
        planner::init_seams();
        scan_fgram::init_seams();
        parser_seams::raw_parser::set(|mcx, q, mode| {
            let list = gram_core::raw_parser(mcx, q, mode)?;
            let mut v = mcx::PgVec::new_in(mcx);
            v.try_reserve_exact(list.len()).map_err(|_| mcx.oom(list.len()))?;
            for n in list.iter() {
                let rs = n.as_raw_stmt().expect("raw_parser yields RawStmt");
                v.push(::types_nodes::rawnodes::RawStmt {
                    stmt: rs.stmt,
                    stmt_location: rs.stmt_location,
                    stmt_len: rs.stmt_len,
                });
            }
            Ok(v)
        });
        parse_expr::init_seams();
        parser_analyze::init_seams();
        rewrite_handler::init_seams();
        execmain::init_seams();
        xact::init_seams();
        elog::init_seams();
        utility::init_seams();
        tuplestore::init_seams();
        init_small::init_seams();
        guc_tables::init_seams();
        guc_tables::option_sets::archive_mode_options.install(&[]);
        guc_tables::option_sets::dynamic_shared_memory_options.install(&[]);
        guc_tables::option_sets::io_method_options.install(&[]);
        guc_tables::option_sets::wal_sync_method_options.install(&[]);
        guc::init_seams();
        scalar_seams::parse_bool::set(|value| match value.to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "1" => Some(true),
            "off" | "false" | "no" | "0" => Some(false),
            _ => None,
        });
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
        mbutils_seams::get_database_encoding::set(|| 6 /* UTF8 */);
        timestamp_seams::get_current_timestamp::set(|| 42);
        backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
        backend_status_seams::pgstat_report_query_id::set(|_, _| {});
        resowner_seams::current_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        resowner_seams::set_current_resource_owner::set(|_| {});
        resowner_seams::top_transaction_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        resowner_seams::resource_owner_enlarge::set(|_| Ok(()));
        resowner_seams::resource_owner_remember_snapshot::set(|_, _| {});
        resowner_seams::resource_owner_forget_snapshot::set(|_, _| {});
        resowner_portal_seams::resource_owner_create_portal::set(|| {
            types_resowner::ResourceOwner::from_parts(1, 1)
        });
        resowner_portal_seams::resource_owner_release::set(|_, _, _, _| {});
        resowner_portal_seams::resource_owner_delete::set(|_| {});
        parallel_seams::is_parallel_worker::set(|| false);
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == INT4OID).then_some(types_tuple::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: types_tuple::TYPALIGN_INT,
                typstorage: types_tuple::TYPSTORAGE_PLAIN,
                typcollation: 0,
            }))
        });
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            SENT.with(|s| s.borrow_mut().push((msgtype, body.to_vec())));
            Ok(0)
        });
        mbutils_seams::server_to_client_conversion_needed::set(|| false);
        mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
        lsyscache_seams::get_type_output_info::set(|oid| match oid {
            INT4OID => Ok((F_INT4OUT, true)),
            _ => panic!("get_type_output_info: unexpected oid {oid}"),
        });
        fmgr_seams::fmgr_info::set(|oid| match oid {
            F_INT4OUT => Ok(FmgrInfo::new(int4out_fn, F_INT4OUT, 1, true, false)),
            _ => panic!("fmgr_info: unexpected oid {oid}"),
        });
        scanfix::install();
    });
    thread_local! {
        static THREAD_UP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    if !THREAD_UP.get() {
        init_small::globals::SetMyProcPid(778);
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
        portalmem::EnablePortalManager();
        miscinit::SetUserIdAndSecContext(types_core::BOOTSTRAP_SUPERUSERID, 0);
        guc::initialize_guc_options().unwrap();
        THREAD_UP.set(true);
    }
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("portalcmds-e2e")));
    m.mcx()
}

// The analyzer's output for `SELECT 1` (pquery e2e's fixture shape).
fn select_1_query(mcx: Mcx<'static>) -> Query<'static> {
    let konst =
        Node::mk_const(mcx, INT4OID, -1, 0, 4, datum::Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
    let jointree = mcx::alloc_leak_in(
        mcx,
        ::types_nodes::primnodes::FromExpr { fromlist: NodeList::nil(), quals: None },
    )
    .unwrap();
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

fn mk_declare(
    mcx: Mcx<'static>,
    name: &'static str,
    options: i32,
) -> &'static DeclareCursorStmt<'static> {
    let query = Node::mk(mcx, select_1_query(mcx)).unwrap();
    let mut d = Node::build::<DeclareCursorStmt>(mcx).unwrap();
    d.portalname = Some(name);
    d.options = options;
    d.query = Some(query);
    d.seal_ref()
}

fn push_snapshot() {
    let snap = snapmgr::GetTransactionSnapshot().unwrap();
    snapmgr::PushActiveSnapshot(&snap).unwrap();
}

fn data_rows(sent: &[(u8, Vec<u8>)]) -> Vec<String> {
    sent.iter()
        .filter(|(t, _)| *t == b'D')
        .map(|(_, b)| {
            assert_eq!(i16::from_be_bytes([b[0], b[1]]), 1);
            let len = i32::from_be_bytes([b[2], b[3], b[4], b[5]]) as usize;
            String::from_utf8(b[6..6 + len].to_vec()).unwrap()
        })
        .collect()
}

fn fetch(
    portal_name: &'static str,
    direction: ::types_nodes::parsenodes::FetchDirection,
    how_many: i64,
    ismove: bool,
) -> (QueryCompletion, Vec<String>) {
    SENT.with(|s| s.borrow_mut().clear());
    let stmt =
        FetchStmt { direction, howMany: how_many, portalname: Some(portal_name), ismove };
    let portal = portalmem::GetPortalByName(Some(portal_name)).expect("portal exists");
    let mut dest = tcop_dest::CreateDestReceiver(CommandDest::RemoteExecute);
    tcop_dest::SetRemoteDestReceiverParams(&mut dest, portal);
    let mut qc = QueryCompletion::default();
    PerformPortalFetch(&stmt, &mut dest, Some(&mut qc)).unwrap();
    (qc, data_rows(&SENT.with(|s| s.borrow().clone())))
}

fn pos(name: &str) -> (bool, bool, u64) {
    let portal = portalmem::GetPortalByName(Some(name)).expect("portal exists");
    let p = portal.borrow();
    (p.atStart, p.atEnd, p.portalPos)
}

// DECLARE (real rewriter + committed planner) → FETCH → CLOSE over SELECT 1;
// the one-row Result plan takes the auto-NO_SCROLL leg of the heuristic.
#[test]
fn declare_fetch_close_select1_e2e() {
    install_fixtures();
    let mcx = leaked_mcx();
    push_snapshot();

    let before = execmain::registry_len();
    let cstmt = mk_declare(mcx, "c1", 0);
    PerformCursorOpen(
        mcx,
        cstmt,
        "DECLARE c1 CURSOR FOR SELECT 1",
        "DECLARE c1 CURSOR FOR SELECT 1",
        ParamListHandle::NULL,
        false,
    )
    .unwrap();
    snapmgr::PopActiveSnapshot().unwrap();
    assert_eq!(execmain::registry_len(), before + 1);

    let portal = portalmem::GetPortalByName(Some("c1")).expect("cursor portal exists");
    {
        let p = portal.borrow();
        assert_eq!(p.strategy, PORTAL_ONE_SELECT);
        assert_ne!(p.cursorOptions & CURSOR_OPT_NO_SCROLL, 0, "Result plan can't back up");
        assert_eq!(p.sourceText.as_ref().unwrap().as_str(), "DECLARE c1 CURSOR FOR SELECT 1");
    }
    drop(portal);

    let (qc, rows) = fetch("c1", FETCH_FORWARD, 1, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
    assert_eq!(rows, ["1"]);
    assert_eq!(pos("c1"), (false, false, 1));

    let (qc, rows) = fetch("c1", FETCH_FORWARD, FETCH_ALL, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 0));
    assert!(rows.is_empty());
    assert_eq!(pos("c1"), (false, true, 1));

    // NO_SCROLL cursor refuses to back up (55000).
    let portal = portalmem::GetPortalByName(Some("c1")).unwrap();
    let mut none = tcop_dest::DestReceiver::DoNothing;
    let err = pquery::PortalRunFetch(&portal, FETCH_BACKWARD, 1, &mut none).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
    drop(portal);

    // The failed portal is closeable; CLOSE tears down the live executor
    // through PortalCleanup and reclaims the QueryDesc registry entry.
    PerformPortalClose(Some("c1")).unwrap();
    assert!(portalmem::GetPortalByName(Some("c1")).is_none());
    assert_eq!(execmain::registry_len(), before);
}

#[test]
fn declare_fetch_move_close_through_utility_dispatch() {
    install_fixtures();
    let mcx = leaked_mcx();

    let run = |node: Node<'static>, source: &str| -> QueryCompletion {
        let pstmt = PlannedStmt {
            commandType: CmdType::CMD_UTILITY,
            canSetTag: true,
            utilityStmt: Some(node),
            ..PlannedStmt::default()
        };
        let mut dest = tcop_dest::DestReceiver::DoNothing;
        let mut qc = QueryCompletion::default();
        utility::ProcessUtility(
            mcx,
            &pstmt,
            source,
            false,
            utility_seams::PROCESS_UTILITY_QUERY,
            ParamListHandle::NULL,
            QueryEnvHandle::NULL,
            &mut dest,
            Some(&mut qc),
        )
        .unwrap();
        qc
    };

    push_snapshot();
    let declare = {
        let query = Node::mk(mcx, select_1_query(mcx)).unwrap();
        Node::mk(
            mcx,
            DeclareCursorStmt { portalname: Some("c2"), options: 0, query: Some(query) },
        )
        .unwrap()
    };
    assert_eq!(
        cmdtag::GetCommandTagName(utility::CreateCommandTag(declare)),
        "DECLARE CURSOR"
    );
    run(declare, "DECLARE c2 CURSOR FOR SELECT 1");
    snapmgr::PopActiveSnapshot().unwrap();
    assert!(portalmem::GetPortalByName(Some("c2")).is_some());

    let move_stmt = Node::mk(
        mcx,
        FetchStmt {
            direction: FETCH_FORWARD,
            howMany: 1,
            portalname: Some("c2"),
            ismove: true,
        },
    )
    .unwrap();
    assert_eq!(cmdtag::GetCommandTagName(utility::CreateCommandTag(move_stmt)), "MOVE");
    let qc = run(move_stmt, "MOVE c2");
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 1));
    assert_eq!(pos("c2"), (false, false, 1));

    let close = Node::mk(mcx, ClosePortalStmt { portalname: Some("c2") }).unwrap();
    assert_eq!(cmdtag::GetCommandTagName(utility::CreateCommandTag(close)), "CLOSE CURSOR");
    run(close, "CLOSE c2");
    assert!(portalmem::GetPortalByName(Some("c2")).is_none());

    let close_all = Node::mk(mcx, ClosePortalStmt { portalname: None }).unwrap();
    assert_eq!(
        cmdtag::GetCommandTagName(utility::CreateCommandTag(close_all)),
        "CLOSE CURSOR ALL"
    );
}

#[test]
fn declare_requires_transaction_block_at_top_level() {
    install_fixtures();
    let mcx = leaked_mcx();
    push_snapshot();
    let cstmt = mk_declare(mcx, "c3", 0);
    let err = PerformCursorOpen(
        mcx,
        cstmt,
        "DECLARE c3 CURSOR FOR SELECT 1",
        "DECLARE c3 CURSOR FOR SELECT 1",
        ParamListHandle::NULL,
        true,
    )
    .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_NO_ACTIVE_SQL_TRANSACTION);
    snapmgr::PopActiveSnapshot().unwrap();
}

#[test]
fn cursor_name_errors_match_c_sqlstates() {
    install_fixtures();
    let mcx = leaked_mcx();

    let cstmt = {
        let mut d = Node::build::<DeclareCursorStmt>(mcx).unwrap();
        d.portalname = Some("");
        d.seal_ref()
    };
    let err = PerformCursorOpen(mcx, cstmt, "DECLARE", "DECLARE", ParamListHandle::NULL, false)
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_CURSOR_NAME);

    let stmt = FetchStmt { portalname: Some("no-such"), howMany: 1, ..FetchStmt::default() };
    let mut none = tcop_dest::DestReceiver::DoNothing;
    let err = PerformPortalFetch(&stmt, &mut none, None).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_CURSOR);

    let err = PerformPortalClose(Some("no-such")).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_UNDEFINED_CURSOR);

    let err = PerformPortalClose(Some("")).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_CURSOR_NAME);
}

// SCROLL cursor over a live seqscan executor with the store knob OFF: the
// FETCH/MOVE sequence pins C's cursor semantics, with FETCH_ABSOLUTE's
// rewind leg and MOVE BACKWARD ALL driving ExecutorRewind → ExecReScan
// through the real plan tree. Backward-FETCH legs were retired by the
// backward-execution deletion (se/deletion-prep B1: the run seam is
// forward-only; in this knob-OFF world a backward FETCH now errors 0A000 —
// pinned separately below; the store-armed world's backward reads are the
// w10ca 94001 pins). Rewind is NOT backward (rescan machinery) and stays.
#[test]
fn live_seqscan_cursor_full_fetch_sequence() {
    install_fixtures();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Pin the knob-OFF world explicitly (the cell is process-global and the
    // SE13 flip made the env-absent default ON; without the pin this test's
    // world depends on sibling-test order — the KnobGuard precedent).
    execmain::cursor_store_fill_set_for_tests(false);
    let mcx = leaked_mcx();

    let relid: u32 = 71001;
    scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
    let pstmt = mk_seqscan_pstmt(mcx, relid);

    let before = execmain::registry_len();
    // Hand-defined cursor portal (the committed planner has no catalog to
    // plan a table scan from); PerformCursorOpen's own path is pinned by the
    // SELECT 1 tests above.
    // SAFETY: pstmt is arena-backed by the leaked mcx.
    let stmts = unsafe { pquery::stmt_list::register(core::slice::from_ref(pstmt)) };
    let portal = portalmem::CreatePortal("lc", false, false).unwrap();
    portalmem::PortalDefineQuery(
        &portal,
        None,
        "DECLARE lc SCROLL CURSOR FOR SELECT a FROM t",
        types_portal::CMDTAG_SELECT,
        stmts,
        CachedPlanHandle::NULL,
    )
    .unwrap();
    portal.borrow_mut().cursorOptions = CURSOR_OPT_SCROLL;
    push_snapshot();
    pquery::PortalStart(&portal, ParamListHandle::NULL, 0, Some(snapmgr::GetActiveSnapshot()))
        .unwrap();
    snapmgr::PopActiveSnapshot().unwrap();
    drop(portal);
    assert_eq!(execmain::registry_len(), before + 1);

    let (qc, rows) = fetch("lc", FETCH_FORWARD, 2, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 2));
    assert_eq!(rows, ["1", "2"]);
    assert_eq!(pos("lc"), (false, false, 2));

    let (qc, rows) = fetch("lc", FETCH_ABSOLUTE, 3, true);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 1));
    assert!(rows.is_empty(), "MOVE sends no rows");
    assert_eq!(pos("lc"), (false, false, 3));

    let (qc, rows) = fetch("lc", FETCH_FORWARD, FETCH_ALL, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 2));
    assert_eq!(rows, ["4", "5"]);
    assert_eq!(pos("lc"), (false, true, 5));

    // Goal at most halfway back: DoPortalRewind → live ExecutorRewind.
    let (qc, rows) = fetch("lc", FETCH_ABSOLUTE, 2, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
    assert_eq!(rows, ["2"]);
    assert_eq!(pos("lc"), (false, false, 2));

    let (qc, rows) = fetch("lc", FETCH_RELATIVE, 2, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
    assert_eq!(rows, ["4"]);
    assert_eq!(pos("lc"), (false, false, 4));

    // (B1: the FETCH BACKWARD ALL leg that walked 3,2,1 here retired with
    // the backward drive — pos continues from 4, so MOVE FORWARD ALL now
    // traverses the single remaining row.)
    let (qc, _) = fetch("lc", FETCH_FORWARD, FETCH_ALL, true);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 1));
    assert_eq!(pos("lc"), (false, true, 5));

    // MOVE BACKWARD ALL = rewind (second live ExecutorRewind).
    let (qc, _) = fetch("lc", FETCH_BACKWARD, FETCH_ALL, true);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 5));
    assert_eq!(pos("lc"), (true, false, 0));

    let (qc, rows) = fetch("lc", FETCH_FORWARD, 1, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
    assert_eq!(rows, ["1"]);

    let closed_before = scanfix::CLOSED.load(std::sync::atomic::Ordering::Relaxed);
    PerformPortalClose(Some("lc")).unwrap();
    assert!(portalmem::GetPortalByName(Some("lc")).is_none());
    assert_eq!(execmain::registry_len(), before);
    assert_eq!(
        scanfix::CLOSED.load(std::sync::atomic::Ordering::Relaxed) - closed_before,
        1,
        "PortalCleanup shut down the executor and closed the scan relation"
    );
    scanfix::quiesced();
}

// se/deletion-prep B1 degradation pin: with the cursor store knob OFF (the
// kill-switch world), a backward FETCH on a SCROLL cursor reaches the
// forward-only run seam and errors 0A000 BEFORE any plan work (the seam
// refusal is entry-first, so no pins are held and the portal still closes
// cleanly). At defaults this path is unreachable — the store serves every
// backward read (w10ca 94001).
#[test]
fn live_seqscan_cursor_backward_errors_without_store_b1() {
    install_fixtures();
    let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // The kill-switch world under test: store knob explicitly OFF (the
    // process-global cell would otherwise resolve to the flipped default).
    execmain::cursor_store_fill_set_for_tests(false);
    let mcx = leaked_mcx();

    let relid: u32 = 71002;
    scanfix::register_table(relid, &[&[1, 2, 3]]);
    let pstmt = mk_seqscan_pstmt(mcx, relid);
    // SAFETY: pstmt is arena-backed by the leaked mcx.
    let stmts = unsafe { pquery::stmt_list::register(core::slice::from_ref(pstmt)) };
    let portal = portalmem::CreatePortal("lcb", false, false).unwrap();
    portalmem::PortalDefineQuery(
        &portal,
        None,
        "DECLARE lcb SCROLL CURSOR FOR SELECT a FROM t",
        types_portal::CMDTAG_SELECT,
        stmts,
        CachedPlanHandle::NULL,
    )
    .unwrap();
    portal.borrow_mut().cursorOptions = CURSOR_OPT_SCROLL;
    push_snapshot();
    pquery::PortalStart(&portal, ParamListHandle::NULL, 0, Some(snapmgr::GetActiveSnapshot()))
        .unwrap();
    snapmgr::PopActiveSnapshot().unwrap();
    drop(portal);

    // Drain to the end first: the exhausted scan released its page pin, so
    // the seam refusal below happens with ZERO pins held (the unit fixture
    // has no abort-side resowner release; a mid-scan error would park the
    // pin on machinery this fixture cannot run).
    let (qc, rows) = fetch("lcb", FETCH_FORWARD, FETCH_ALL, false);
    assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 3));
    assert_eq!(rows, ["1", "2", "3"]);

    // The backward fetch: seam-refused, loudly.
    SENT.with(|s| s.borrow_mut().clear());
    let stmt = FetchStmt {
        direction: FETCH_BACKWARD,
        howMany: 1,
        portalname: Some("lcb"),
        ismove: false,
    };
    let p = portalmem::GetPortalByName(Some("lcb")).expect("portal exists");
    let mut dest = tcop_dest::CreateDestReceiver(CommandDest::RemoteExecute);
    tcop_dest::SetRemoteDestReceiverParams(&mut dest, p);
    let mut qc = QueryCompletion::default();
    let err = PerformPortalFetch(&stmt, &mut dest, Some(&mut qc)).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert!(
        err.message().contains("backward scan is not supported"),
        "unexpected error: {}",
        err.message()
    );

    // The failed portal still drops (abort-parity: MarkPortalFailed already
    // detached the executor; production pin release rides the resowner at
    // abort — here the exhausted scan holds none, proven by quiesced).
    PerformPortalClose(Some("lcb")).unwrap();
    assert!(portalmem::GetPortalByName(Some("lcb")).is_none());
    scanfix::quiesced();
}

fn mk_seqscan_pstmt<'mcx>(mcx: Mcx<'mcx>, relid: u32) -> &'mcx PlannedStmt<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};

    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let tlist = NodeList::make1(mcx, tle).unwrap();
    let scan_node = Node::mk(
        mcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan { plan: Plan { targetlist: tlist, ..Default::default() }, scanrelid: 1 },
        },
    )
    .unwrap();

    let rte = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_RELATION,
            relid,
            relkind: ::types_rel::RELKIND_RELATION,
            rellockmode: ::types_rel::AccessShareLock,
            perminfoindex: 1,
            inFromCl: true,
            ..Default::default()
        },
    )
    .unwrap();
    let perminfo = Node::mk(
        mcx,
        RTEPermissionInfo {
            relid,
            requiredPerms: 1 << 1, // ACL_SELECT
            ..Default::default()
        },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();

    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(scan_node);
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
    pstmt.seal_ref()
}

// Fake buffer/heap substrate feeding the REAL heapam/seqscan stack
// (execmain's scanfix, single-column shape).
mod scanfix {
    use core::ptr::NonNull;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use ::mcx::{Mcx, PgVec};
    use ::types_core::{
        Buffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    };
    use ::types_rel::{
        FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
        RELKIND_RELATION,
    };
    use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, HEAP_XMAX_INVALID,
        TYPALIGN_INT, TYPSTORAGE_PLAIN,
    };

    pub static CLOSED: AtomicUsize = AtomicUsize::new(0);
    pub static TEST_LOCK: Mutex<()> = Mutex::new(());

    struct Fake {
        tables: HashMap<Oid, Vec<Buffer>>,
        pages: Vec<usize>,
        pins: Vec<i32>,
    }

    static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

    fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
        let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
        f(g.get_or_insert_with(|| Fake {
            tables: HashMap::new(),
            pages: Vec::new(),
            pins: Vec::new(),
        }))
    }

    pub fn install() {
        bufmgr_seams::read_buffer::set(|rel, block| {
            with_fake(|f| {
                let buf = f.tables[&rel.rd_id][block as usize];
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::read_buffer_strategy::set(|rel, block, _strategy| {
            bufmgr_seams::read_buffer::call(rel, block)
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| {
                for pages in f.tables.values() {
                    if let Some(i) = pages.iter().position(|b| *b == buf) {
                        return i as u32;
                    }
                }
                panic!("unknown buffer {buf}")
            })
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            let addr = with_fake(|f| {
                assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
                f.pages[(buf - 1) as usize]
            });
            NonNull::new(addr as *mut u8).unwrap()
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| {
                let p = &mut f.pins[(buf - 1) as usize];
                assert!(*p > 0, "double release of buffer {buf}");
                *p -= 1;
            });
            Ok(())
        });
        bufmgr_seams::incr_buffer_ref_count::set(|buf| {
            with_fake(|f| f.pins[(buf - 1) as usize] += 1);
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::get_access_strategy::set(|_| None);
        bufmgr_seams::free_access_strategy::set(|_| {});
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
            with_fake(|f| Ok(f.tables[&rel.rd_id].len() as u32))
        });

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(|_h, _s, _b| Ok(true));
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(|_h, _s, _b, _m| Ok(true));
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_h, _v| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_h| Ok(false));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_r, _t, _s, _x| Ok(()));
        pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));

        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::pg_class_aclmask::set(|_relid, _roleid, mask, _how_all| Ok(mask));
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));

        relation_seams::relation_open::set(fake_relation_open);
    }

    fn tuple_image(val: i32) -> Vec<u8> {
        let mut img = vec![0u8; 28];
        img[0..4].copy_from_slice(&10u32.to_ne_bytes());
        img[18..20].copy_from_slice(&1u16.to_ne_bytes());
        img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
        img[22] = 24;
        img[24..28].copy_from_slice(&val.to_ne_bytes());
        img
    }

    #[repr(align(8))]
    struct TestPage([u8; BLCKSZ]);

    fn build_page(rows: &[i32]) -> Box<TestPage> {
        let mut page = Box::new(TestPage([0u8; BLCKSZ]));
        let n = rows.len();
        let lower = SizeOfPageHeaderData + n * 4;
        let mut upper = BLCKSZ;
        for (i, row) in rows.iter().enumerate() {
            let img = tuple_image(*row);
            upper = (upper - img.len()) & !7;
            page.0[upper..upper + img.len()].copy_from_slice(&img);
            let id = ItemIdData::new(upper as u16, LP_NORMAL, img.len() as u16);
            let off = SizeOfPageHeaderData + i * 4;
            // SAFETY: repr(transparent) over u32.
            let raw: u32 = unsafe { core::mem::transmute(id) };
            page.0[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
        }
        page.0[12..14].copy_from_slice(&(lower as u16).to_ne_bytes());
        page.0[14..16].copy_from_slice(&(upper as u16).to_ne_bytes());
        page.0[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
        page.0[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
        page
    }

    pub fn register_table(relid: Oid, pages: &[&[i32]]) {
        with_fake(|f| {
            let mut bufs = Vec::new();
            for vals in pages {
                let addr = Box::leak(build_page(vals)).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(0);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(relid, bufs);
        });
    }

    pub fn quiesced() {
        with_fake(|f| {
            assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        });
    }

    fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: 23,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    fn record_close(_relid: Oid, _lockmode: LOCKMODE) -> ::types_error::PgResult<()> {
        CLOSED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn fake_relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        _lockmode: LOCKMODE,
    ) -> ::types_error::PgResult<Relation<'mcx>> {
        let mut relname = NameData::default();
        relname.namestrcpy("t");
        let rd_rel = FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: tableam::HEAP_TABLE_AM_OID,
            relfilenode: relid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        };
        let data = RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
            rd_id: relid,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: std::cell::Cell::new(true),
            rd_createSubid: std::cell::Cell::new(0),
            rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_droppedSubid: std::cell::Cell::new(0),
            rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: relid, dbId: 5 } },
            rd_rel,
            rd_att: int4_tupdesc(mcx),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: std::cell::Cell::new(true),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        };
        Ok(Relation::open(data, Some(record_close)))
    }
}

// --- WS-CA wave-10 (cursors inc-2, contract §1/§2/§4; band 94001+) -------------
//
// The store-armed SCROLL cursor pins: same live-seqscan substrate as the
// knob-OFF sequence test above, PGRUST_LANE_V2_CURSORS ON via the portalmem
// memo lever. Every test holds scanfix::TEST_LOCK (the knob cell is
// process-global; the guard restores OFF on every exit path).
mod cursors_w10_ca {
    use super::*;
    use ::types_portal::CURSOR_OPT_HOLD;

    struct KnobGuard;
    impl KnobGuard {
        fn on() -> KnobGuard {
            execmain::cursor_store_fill_set_for_tests(true);
            KnobGuard
        }
    }
    impl Drop for KnobGuard {
        fn drop(&mut self) {
            execmain::cursor_store_fill_set_for_tests(false);
        }
    }

    fn mk_armed_cursor(mcx: Mcx<'static>, name: &'static str, relid: u32, options: i32) {
        let pstmt = mk_seqscan_pstmt(mcx, relid);
        // SAFETY: pstmt is arena-backed by the leaked mcx.
        let stmts = unsafe { pquery::stmt_list::register(core::slice::from_ref(pstmt)) };
        let portal = portalmem::CreatePortal(name, false, false).unwrap();
        portalmem::PortalDefineQuery(
            &portal,
            None,
            "DECLARE (wave-10 store-armed test cursor)",
            types_portal::CMDTAG_SELECT,
            stmts,
            CachedPlanHandle::NULL,
        )
        .unwrap();
        portal.borrow_mut().cursorOptions = options;
        push_snapshot();
        pquery::PortalStart(&portal, ParamListHandle::NULL, 0, Some(snapmgr::GetActiveSnapshot()))
            .unwrap();
        snapmgr::PopActiveSnapshot().unwrap();
    }

    fn store_count(name: &str) -> i64 {
        let portal = portalmem::GetPortalByName(Some(name)).unwrap();
        let store = {
            let p = portal.borrow();
            if !p.cursorStore.is_null() { p.cursorStore } else { p.holdStore }
        };
        assert!(!store.is_null(), "store-armed portal has a cursor store");
        tuplestore_hold_seams::tuplestore_tuple_count::call(store)
    }

    fn fill_exhausted(name: &str) -> bool {
        portalmem::GetPortalByName(Some(name)).unwrap().borrow().cursorFillExhausted
    }

    /// 94001: the full knob-OFF fetch sequence byte-for-byte (fetch
    /// invisibility, contract §2.3) PLUS the §2.2 laziness ledger: the store
    /// grows exactly as far as each fetch demands; backward/rewind/absolute
    /// below the high-water are pure replays (fill count frozen).
    #[test]
    fn w10ca_94001_store_armed_scroll_sequence_and_laziness() {
        install_fixtures();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _knob = KnobGuard::on();
        let mcx = leaked_mcx();
        let relid: u32 = 94001;
        scanfix::register_table(relid, &[&[1, 2, 3], &[4, 5]]);
        mk_armed_cursor(mcx, "w10a", relid, CURSOR_OPT_SCROLL);
        {
            let portal = portalmem::GetPortalByName(Some("w10a")).unwrap();
            assert!(portal.borrow().cursorStoreArmed, "knob-ON SCROLL ONE_SELECT arms");
        }

        let (qc, rows) = fetch("w10a", FETCH_FORWARD, 2, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 2));
        assert_eq!(rows, ["1", "2"]);
        assert_eq!(pos("w10a"), (false, false, 2));
        assert_eq!(store_count("w10a"), 2, "§2.2: filled exactly as far as the fetch demands");
        assert!(!fill_exhausted("w10a"));
        {
            // Non-hold SCROLL: the §1.1 cursorStore, not the holdStore.
            let portal = portalmem::GetPortalByName(Some("w10a")).unwrap();
            assert!(!portal.borrow().cursorStore.is_null());
            assert!(portal.borrow().holdStore.is_null());
            // Bare seqscan is CURRENT-OF-eligible: sidecar rides along.
            assert_eq!(portal.borrow().currentOfEligible, Some(true));
            assert!(!portal.borrow().cursorTidStore.is_null());
        }

        let (qc, rows) = fetch("w10a", FETCH_BACKWARD, 1, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
        assert_eq!(rows, ["1"]);
        assert_eq!(pos("w10a"), (false, false, 1));
        assert_eq!(store_count("w10a"), 2, "backward fetch = pure store seek");

        let (qc, rows) = fetch("w10a", FETCH_ABSOLUTE, 3, true);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 1));
        assert!(rows.is_empty(), "MOVE sends no rows");
        assert_eq!(pos("w10a"), (false, false, 3));
        assert_eq!(store_count("w10a"), 3, "MOVE ABSOLUTE 3 fills to row 3, never further");

        let (qc, rows) = fetch("w10a", FETCH_FORWARD, FETCH_ALL, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 2));
        assert_eq!(rows, ["4", "5"]);
        assert_eq!(pos("w10a"), (false, true, 5));
        assert_eq!(store_count("w10a"), 5);
        assert!(fill_exhausted("w10a"), "count-0 drain exhausts the fill");

        // FETCH ABSOLUTE 2 takes the rewind leg: store replay, no re-execution
        // (§5 D1 — the fill high-water is kept).
        let (qc, rows) = fetch("w10a", FETCH_ABSOLUTE, 2, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
        assert_eq!(rows, ["2"]);
        assert_eq!(pos("w10a"), (false, false, 2));
        assert_eq!(store_count("w10a"), 5, "rewind-refetch replays; nothing re-executes");

        let (qc, rows) = fetch("w10a", FETCH_RELATIVE, 2, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
        assert_eq!(rows, ["4"]);
        assert_eq!(pos("w10a"), (false, false, 4));

        let (qc, rows) = fetch("w10a", FETCH_BACKWARD, FETCH_ALL, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 3));
        assert_eq!(rows, ["3", "2", "1"]);
        assert_eq!(pos("w10a"), (true, false, 0));

        let (qc, _) = fetch("w10a", FETCH_FORWARD, FETCH_ALL, true);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 5));
        assert_eq!(pos("w10a"), (false, true, 5));

        let (qc, _) = fetch("w10a", FETCH_BACKWARD, FETCH_ALL, true);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 5));
        assert_eq!(pos("w10a"), (true, false, 0));

        let (qc, rows) = fetch("w10a", FETCH_FORWARD, 1, false);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_FETCH, 1));
        assert_eq!(rows, ["1"]);
        assert_eq!(store_count("w10a"), 5);

        PerformPortalClose(Some("w10a")).unwrap();
        assert!(portalmem::GetPortalByName(Some("w10a")).is_none());
        scanfix::quiesced();
    }

    /// 94002: §2.4 arm 1 — SCROLL+HOLD persist keeps the fetched prefix
    /// (fill_to(EOF) resume, no rewind/re-execution), tears the executor
    /// down, and post-persist fetches serve from the same holdStore with the
    /// cursor position intact (§5 D2).
    #[test]
    fn w10ca_94002_scroll_hold_persist_keeps_prefix() {
        install_fixtures();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _knob = KnobGuard::on();
        let mcx = leaked_mcx();
        let relid: u32 = 94002;
        scanfix::register_table(relid, &[&[10, 20, 30], &[40, 50]]);
        mk_armed_cursor(mcx, "w10h", relid, CURSOR_OPT_SCROLL | CURSOR_OPT_HOLD);

        let (_, rows) = fetch("w10h", FETCH_FORWARD, 2, false);
        assert_eq!(rows, ["10", "20"]);
        {
            // SCROLL+HOLD is holdStore-resident from FIRST RUN (§1.1 row 3).
            let portal = portalmem::GetPortalByName(Some("w10h")).unwrap();
            assert!(portal.borrow().cursorStore.is_null());
            assert!(!portal.borrow().holdStore.is_null());
        }
        assert_eq!(store_count("w10h"), 2);

        let portal = portalmem::GetPortalByName(Some("w10h")).unwrap();
        crate::PersistHoldablePortal(&portal).unwrap();
        assert!(portal.borrow().queryDesc.is_null(), "persist tore the executor down");
        drop(portal);
        assert_eq!(store_count("w10h"), 5, "persist = fill_to(EOF) resume from high-water");
        assert!(fill_exhausted("w10h"));
        assert_eq!(pos("w10h"), (false, false, 2), "cursor position survives persist");

        let (_, rows) = fetch("w10h", FETCH_FORWARD, FETCH_ALL, false);
        assert_eq!(rows, ["30", "40", "50"]);
        assert_eq!(pos("w10h"), (false, true, 5));

        let (_, rows) = fetch("w10h", FETCH_BACKWARD, 2, false);
        assert_eq!(rows, ["50", "40"], "backward across COMMIT-persisted store");

        PerformPortalClose(Some("w10h")).unwrap();
        scanfix::quiesced();
    }

    /// 94003: §4 — WHERE CURRENT OF over a store-armed cursor resolves the
    /// tid from the sidecar at portalPos-1 (the fill high-water mark is
    /// several rows ahead: reading the scan state would target the wrong
    /// row — the portals.sql FETCH ABSOLUTE 12 / ABSOLUTE 8 hazard shape).
    #[test]
    fn w10ca_94003_current_of_resolves_from_sidecar() {
        install_fixtures();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _knob = KnobGuard::on();
        let mcx = leaked_mcx();
        let relid: u32 = 94003;
        scanfix::register_table(relid, &[&[7, 8, 9], &[11, 12]]);
        mk_armed_cursor(mcx, "w10c", relid, CURSOR_OPT_SCROLL);

        // Drive the fill to row 4 (block 1 offset 1), then move BACKWARD to
        // row 2 (block 0 offset 2): scan sits at high-water 4, cursor at 2.
        let (_, rows) = fetch("w10c", FETCH_ABSOLUTE, 4, false);
        assert_eq!(rows, ["11"]);
        let (qc, _) = fetch("w10c", FETCH_ABSOLUTE, 2, true);
        assert_eq!((qc.commandTag, qc.nprocessed), (CMDTAG_MOVE, 1));
        assert_eq!(pos("w10c"), (false, false, 2));
        assert_eq!(store_count("w10c"), 4, "high-water stays at 4");

        let tid = execmain_seams::exec_current_of::call(Some("w10c"), 0, relid, "t")
            .unwrap()
            .expect("cursor is on a row of this table");
        assert_eq!(
            (types_tuple::ItemPointerGetBlockNumber(&tid), tid.ip_posid),
            (0, 2),
            "CURRENT OF answers the CURSOR row, not the scan's high-water row"
        );

        // Another table's oid: the per-table search fails exactly as in C.
        let err = execmain_seams::exec_current_of::call(Some("w10c"), 0, relid + 1, "other")
            .unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_CURSOR_STATE);

        // Off-row (atEnd): C's "not positioned on a row".
        let (_, _) = fetch("w10c", FETCH_FORWARD, FETCH_ALL, true);
        assert_eq!(pos("w10c"), (false, true, 5));
        let err =
            execmain_seams::exec_current_of::call(Some("w10c"), 0, relid, "t").unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_CURSOR_STATE);

        PerformPortalClose(Some("w10c")).unwrap();
        scanfix::quiesced();
    }

    /// 94004: knob-OFF world untouched — SCROLL portals arm nothing and run
    /// the legacy executor-backward path (the sequence test above pins its
    /// bytes); the memo lever answers exactly what the tests set.
    #[test]
    fn w10ca_94004_knob_off_arms_nothing() {
        install_fixtures();
        let _fixture = scanfix::TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        execmain::cursor_store_fill_set_for_tests(false);
        let mcx = leaked_mcx();
        let relid: u32 = 94004;
        scanfix::register_table(relid, &[&[1, 2]]);
        mk_armed_cursor(mcx, "w10off", relid, CURSOR_OPT_SCROLL);
        let (_, rows) = fetch("w10off", FETCH_FORWARD, 1, false);
        assert_eq!(rows, ["1"]);
        {
            let portal = portalmem::GetPortalByName(Some("w10off")).unwrap();
            let p = portal.borrow();
            assert!(!p.cursorStoreArmed);
            assert!(p.cursorStore.is_null());
            assert!(p.cursorTidStore.is_null());
            assert_eq!(p.currentOfEligible, None);
        }
        PerformPortalClose(Some("w10off")).unwrap();
        scanfix::quiesced();
    }
}
// --- end WS-CA wave-10 (band 94001+) --------------------------------------------
