use super::*;
use types_nodes::plannodes::PlannedStmt;

fn select_stmt() -> PlannedStmt<'static> {
    PlannedStmt {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        ..PlannedStmt::default()
    }
}

#[test]
fn choose_strategy_one_select() {
    let stmts = [select_stmt()];
    assert_eq!(ChoosePortalStrategy(&stmts), PORTAL_ONE_SELECT);
}

#[test]
fn choose_strategy_mod_with() {
    let mut s = select_stmt();
    s.hasModifyingCTE = true;
    assert_eq!(ChoosePortalStrategy(&[s]), PORTAL_ONE_MOD_WITH);
}

#[test]
fn choose_strategy_empty_is_multi() {
    assert_eq!(ChoosePortalStrategy(&[]), PORTAL_MULTI_QUERY);
}

#[test]
fn choose_strategy_returning() {
    let ins = PlannedStmt {
        commandType: CmdType::CMD_INSERT,
        canSetTag: true,
        hasReturning: true,
        ..PlannedStmt::default()
    };
    assert_eq!(ChoosePortalStrategy(&[ins]), PORTAL_ONE_RETURNING);
    // Two canSetTag statements collapse to MULTI.
    let two = [
        PlannedStmt {
            commandType: CmdType::CMD_INSERT,
            canSetTag: true,
            hasReturning: true,
            ..PlannedStmt::default()
        },
        PlannedStmt {
            commandType: CmdType::CMD_INSERT,
            canSetTag: true,
            hasReturning: true,
            ..PlannedStmt::default()
        },
    ];
    assert_eq!(ChoosePortalStrategy(&two), PORTAL_MULTI_QUERY);
    // A canSetTag stmt without RETURNING collapses too.
    let no_ret = [PlannedStmt {
        commandType: CmdType::CMD_UPDATE,
        canSetTag: true,
        ..PlannedStmt::default()
    }];
    assert_eq!(ChoosePortalStrategy(&no_ret), PORTAL_MULTI_QUERY);
}

#[test]
fn primary_stmt_is_can_set_tag() {
    let aux = PlannedStmt {
        commandType: CmdType::CMD_INSERT,
        canSetTag: false,
        ..PlannedStmt::default()
    };
    let stmts = [aux, select_stmt()];
    assert_eq!(PortalGetPrimaryStmt(&stmts), Some(1));
    assert_eq!(PortalGetPrimaryStmt(&[]), None);
}

#[test]
fn planned_stmt_requires_snapshot_non_utility() {
    assert!(PlannedStmtRequiresSnapshot(&select_stmt()));
}

#[test]
fn stmt_list_roundtrip_and_staleness() {
    let stmts = vec![select_stmt()];
    // SAFETY: `stmts` outlives the handle; freed below before drop.
    let h = unsafe { stmt_list::register(&stmts) };
    assert!(stmt_list::is_live(h));
    let n = stmt_list::with(h, |s| s.len());
    assert_eq!(n, 1);
    // Re-entrant access must not deadlock/panic.
    stmt_list::with(h, |_| stmt_list::with(h, |s| assert!(s[0].canSetTag)));
    stmt_list::free(h);
    assert!(!stmt_list::is_live(h));
    let stale = std::panic::catch_unwind(|| stmt_list::with(h, |s| s.len()));
    assert!(stale.is_err());
    assert!(!stmt_list::is_live(types_portal::StmtListHandle::NULL));
}

#[test]
fn stmt_list_reset_all_clears() {
    let stmts = vec![select_stmt()];
    let h = unsafe { stmt_list::register(&stmts) };
    stmt_list::reset_all();
    assert!(!stmt_list::is_live(h));
}

// EXPLAIN SELECT 1 and SHOW work_mem end-to-end through a portal:
// PortalStart(UTIL_SELECT) -> FillPortalStore -> tuplestore receiver ->
// RunFromStore -> printtup 'D' wire bytes, pinned against live PG 18.3.
mod e2e {
    use std::cell::RefCell;
    use std::sync::Once;

    use datum::{Datum, VarlenaRef};
    use mcx::{Mcx, MemoryContext};
    use types_dest::CommandDest;
    use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
    use types_nodes::list::NodeList;
    use types_nodes::node_tree::Node;
    use types_nodes::nodes_enums::CmdType;
    use types_nodes::parsenodes::{ExplainStmt, Query, VariableShowStmt};
    use types_nodes::plannodes::PlannedStmt;
    use types_nodes::primnodes::FromExpr;
    use types_portal::{
        CachedPlanHandle, ParamListHandle, QueryCompletion, FETCH_ALL,
    };

    use crate::{stmt_list, PortalRun, PortalSetResultFormat, PortalStart};

    const INT4OID: u32 = 23;
    const TEXTOUT: u32 = 47;

    thread_local! {
        static SENT: RefCell<Vec<(u8, Vec<u8>)>> = const { RefCell::new(Vec::new()) };
    }

    // C textout detoasts: handle the packed 1B short form the minimal-tuple
    // roundtrip produces as well as the 4B form.
    fn textout_fn(
        _flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut FunctionCallInfoBaseData,
    ) -> types_error::PgResult<Datum> {
        let p = fcinfo.arg(0).as_usize() as *const u8;
        // SAFETY: test datum is a live text varlena image.
        let data = unsafe {
            if *p & 0x01 == 1 {
                std::slice::from_raw_parts(p.add(1), (*p >> 1) as usize - 1)
            } else {
                let v = VarlenaRef::from_ptr(p);
                v.data()
            }
        };
        let mut s = data.to_vec();
        s.push(0);
        Ok(Datum::from_usize(Box::leak(s.into_boxed_slice()).as_ptr() as usize))
    }

    // Proc/shmem substrate for snapmgr's MyProc xmin writes (explain tests' shape).
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

    fn install_fixtures() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
            install_proc_fixture();
            crate::init_seams();
            planner::init_seams();
            rewrite_handler::init_seams();
            execmain::init_seams();
            xact::init_seams();
            elog::init_seams();
            utility::init_seams();
            explain::init_seams();
            tuplestore::init_seams();
            init_small::init_seams();
            guc_tables::init_seams();
            guc_tables::option_sets::archive_mode_options.install(&[]);
            guc_tables::option_sets::dynamic_shared_memory_options.install(&[]);
            guc_tables::option_sets::io_method_options.install(&[]);
            guc_tables::option_sets::wal_sync_method_options.install(&[]);
            guc::init_seams();
            variable::init_seams();
            pgtz::init_seams();
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
            resowner_seams::top_transaction_resource_owner::set(|| {
                types_resowner::ResourceOwner::NULL
            });
            resowner_seams::resource_owner_enlarge::set(|_| Ok(()));
            resowner_seams::resource_owner_remember_snapshot::set(|_, _| {});
            resowner_seams::resource_owner_forget_snapshot::set(|_, _| {});
            resowner_portal_seams::resource_owner_create_portal::set(|| {
                types_resowner::ResourceOwner::from_parts(1, 1)
            });
            resowner_portal_seams::resource_owner_release::set(|_, _, _, _| {});
            resowner_portal_seams::resource_owner_delete::set(|_| {});
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
            fmgr_seams::fmgr_info::set(|oid| match oid {
                TEXTOUT => Ok(FmgrInfo::new(textout_fn, TEXTOUT, 1, true, false)),
                _ => panic!("fmgr_info: unexpected oid {oid}"),
            });
        });
        // Thread-locals: each test thread gets its own backend state.
        thread_local! {
            static THREAD_UP: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
        }
        if !THREAD_UP.get() {
            init_small::globals::SetMyProcPid(777);
            lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
            procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
            portalmem::EnablePortalManager();
            miscinit::SetUserIdAndSecContext(types_core::BOOTSTRAP_SUPERUSERID, 0);
            guc::initialize_guc_options().unwrap();
            THREAD_UP.set(true);
        }
    }

    fn leaked_mcx() -> Mcx<'static> {
        let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("pquery-e2e")));
        m.mcx()
    }

    // The analyzer's output for `SELECT 1` (explain tests' fixture shape).
    fn select_1_query(mcx: Mcx<'static>) -> Query<'static> {
        let konst =
            Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
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

    fn utility_pstmt(u: Node<'static>) -> &'static [PlannedStmt<'static>] {
        Vec::leak(vec![PlannedStmt {
            commandType: CmdType::CMD_UTILITY,
            canSetTag: true,
            utilityStmt: Some(u),
            ..PlannedStmt::default()
        }])
    }

    fn run_utility_portal(
        name: &str,
        source: &str,
        stmts: &'static [PlannedStmt<'static>],
    ) -> (QueryCompletion, Vec<(u8, Vec<u8>)>) {
        install_fixtures();
        SENT.with(|s| s.borrow_mut().clear());
        // SAFETY: stmts is leaked 'static.
        let h = unsafe { stmt_list::register(stmts) };
        let tag = utility::CreateCommandTag(stmts[0].utilityStmt.unwrap());
        let portal = portalmem::CreatePortal(name, false, false).unwrap();
        portalmem::PortalDefineQuery(&portal, None, source, tag, h, CachedPlanHandle::NULL)
            .unwrap();
        PortalStart(&portal, ParamListHandle::NULL, 0, None).unwrap();
        PortalSetResultFormat(&portal, &[]).unwrap();
        let mut dest = tcop_dest::CreateDestReceiver(CommandDest::RemoteExecute);
        tcop_dest::SetRemoteDestReceiverParams(&mut dest, portal.clone());
        let mut qc = QueryCompletion::default();
        let complete =
            PortalRun(&portal, FETCH_ALL, true, &mut dest, None, Some(&mut qc)).unwrap();
        assert!(complete);
        (qc, SENT.with(|s| s.borrow().clone()))
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

    #[test]
    fn dropped_portal_releases_query_desc_registry_entry() {
        install_fixtures();
        let mcx = leaked_mcx();
        let query = select_1_query(mcx);
        let pstmt =
            planner::planner(mcx, mcx::leak_in(mcx::alloc_in(mcx, query).unwrap()), "SELECT 1", 0, ParamListHandle::NULL).unwrap();
        let stmts: &'static [PlannedStmt<'static>] = Vec::leak(vec![pstmt]);
        // SAFETY: stmts is leaked 'static.
        let h = unsafe { stmt_list::register(stmts) };
        let portal = portalmem::CreatePortal("qd-abort-e2e", false, false).unwrap();
        portalmem::PortalDefineQuery(
            &portal,
            None,
            "SELECT 1",
            types_portal::CMDTAG_SELECT,
            h,
            CachedPlanHandle::NULL,
        )
        .unwrap();

        let before = execmain::registry_len();
        PortalStart(&portal, ParamListHandle::NULL, 0, None).unwrap();
        assert_eq!(execmain::registry_len(), before + 1);

        // Abort shape: AtCleanup_Portals clears an unrun cleanup hook before
        // PortalDrop; the drop must reclaim the owning registry entry (C
        // frees the QueryDesc with the portal context).
        portal.borrow_mut().cleanup = types_portal::PortalCleanupHook::None;
        portalmem::PortalDrop(&portal, false).unwrap();
        assert_eq!(execmain::registry_len(), before);
    }

    #[test]
    fn show_work_mem_wire_bytes_through_portal() {
        install_fixtures();
        let mcx = leaked_mcx();
        let show = Node::mk(mcx, VariableShowStmt { name: Some("work_mem") }).unwrap();
        let stmts = utility_pstmt(show);
        let (qc, sent) = run_utility_portal("show-e2e", "SHOW work_mem", stmts);

        let rows: Vec<&Vec<u8>> =
            sent.iter().filter(|(t, _)| *t == b'D').map(|(_, b)| b).collect();
        assert_eq!(rows.len(), 1);
        // 'D' body pinned byte-for-byte: 1 column, 3 bytes, "4MB".
        assert_eq!(rows[0][..], [0, 1, 0, 0, 0, 3, b'4', b'M', b'B']);
        assert_eq!(cmdtag::GetCommandTagName(qc.commandTag), "SHOW");
        assert_eq!(qc.nprocessed, 1);
    }

    fn start_scroll_show_portal(name: &'static str) -> types_portal::Portal<'static> {
        start_scroll_portal(name, "work_mem")
    }

    fn start_scroll_portal(
        name: &'static str,
        guc_name: &'static str,
    ) -> types_portal::Portal<'static> {
        install_fixtures();
        SENT.with(|s| s.borrow_mut().clear());
        let mcx = leaked_mcx();
        let show = Node::mk(mcx, VariableShowStmt { name: Some(guc_name) }).unwrap();
        let stmts = utility_pstmt(show);
        // SAFETY: stmts is leaked 'static.
        let h = unsafe { stmt_list::register(stmts) };
        let tag = utility::CreateCommandTag(stmts[0].utilityStmt.unwrap());
        let portal = portalmem::CreatePortal(name, false, false).unwrap();
        portalmem::PortalDefineQuery(&portal, None, "SHOW work_mem", tag, h, CachedPlanHandle::NULL)
            .unwrap();
        // DECLARE SCROLL's option set; CreatePortal defaults to NO_SCROLL.
        portal.borrow_mut().cursorOptions = types_portal::CURSOR_OPT_SCROLL;
        crate::PortalStart(&portal, ParamListHandle::NULL, 0, None).unwrap();
        PortalSetResultFormat(&portal, &[]).unwrap();
        portal
    }

    fn remote_dest(portal: &types_portal::Portal<'static>) -> tcop_dest::DestReceiver<'static> {
        let mut dest = tcop_dest::CreateDestReceiver(CommandDest::RemoteExecute);
        tcop_dest::SetRemoteDestReceiverParams(&mut dest, portal.clone());
        dest
    }

    fn pos(portal: &types_portal::Portal<'static>) -> (bool, bool, u64) {
        let p = portal.borrow();
        (p.atStart, p.atEnd, p.portalPos)
    }

    #[test]
    fn fetch_forward_backward_through_held_store() {
        use types_nodes::parsenodes::{FetchDirection, FetchStmt};

        let portal = start_scroll_show_portal("fetch-e2e");
        let mut dest = remote_dest(&portal);

        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 1, &mut dest).unwrap(),
            1
        );
        assert_eq!(pos(&portal), (false, false, 1));

        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 1, &mut dest).unwrap(),
            0
        );
        assert_eq!(pos(&portal), (false, true, 1));

        // Backward from EOF re-returns the last row (C endpoint adjustment).
        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_BACKWARD, 1, &mut dest).unwrap(),
            1
        );
        assert_eq!(pos(&portal), (false, false, 1));

        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_BACKWARD, 1, &mut dest).unwrap(),
            0
        );
        assert_eq!(pos(&portal), (true, false, 0));

        assert_eq!(data_rows(&SENT.with(|s| s.borrow().clone())), ["4MB", "4MB"]);

        // The live-portal arms of utility's FetchStmt surfaces.
        let mcx = leaked_mcx();
        let f = Node::mk(
            mcx,
            FetchStmt { portalname: Some("fetch-e2e"), ..FetchStmt::default() },
        )
        .unwrap();
        assert!(utility::UtilityReturnsTuples(f));
        assert!(utility::UtilityTupleDescriptor(f).unwrap().is_some());
    }

    #[test]
    fn move_arms_and_backward_all_rewind() {
        use types_nodes::parsenodes::FetchDirection;

        let portal = start_scroll_show_portal("move-e2e");
        let mut none = tcop_dest::DestReceiver::DoNothing;

        // MOVE 0 off-row reports 0.
        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 0, &mut none).unwrap(),
            0
        );

        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, FETCH_ALL, &mut none)
                .unwrap(),
            1
        );
        assert_eq!(pos(&portal), (false, true, 1));

        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_BACKWARD, FETCH_ALL, &mut none)
                .unwrap(),
            1
        );
        assert_eq!(pos(&portal), (true, false, 0));

        // The rewound store replays from the start.
        let mut dest = remote_dest(&portal);
        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 2, &mut dest).unwrap(),
            1
        );
        assert_eq!(pos(&portal), (false, true, 1));
        assert_eq!(data_rows(&SENT.with(|s| s.borrow().clone())), ["4MB"]);
    }

    #[test]
    fn fetch_zero_refetches_current_row() {
        use types_nodes::parsenodes::FetchDirection;

        let portal = start_scroll_show_portal("refetch-e2e");
        let mut dest = remote_dest(&portal);

        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 1, &mut dest).unwrap(),
            1
        );

        // MOVE 0 on-row reports 1 and does not move.
        let mut none = tcop_dest::DestReceiver::DoNothing;
        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 0, &mut none).unwrap(),
            1
        );
        assert_eq!(pos(&portal), (false, false, 1));

        // FETCH 0 backs up and re-fetches the current row.
        assert_eq!(
            crate::PortalRunFetch(&portal, FetchDirection::FETCH_FORWARD, 0, &mut dest).unwrap(),
            1
        );
        assert_eq!(pos(&portal), (false, false, 1));
        assert_eq!(data_rows(&SENT.with(|s| s.borrow().clone())), ["4MB", "4MB"]);
    }

    fn first_cols(sent: &[(u8, Vec<u8>)]) -> Vec<String> {
        sent.iter()
            .filter(|(t, _)| *t == b'D')
            .map(|(_, b)| {
                let len = i32::from_be_bytes([b[2], b[3], b[4], b[5]]) as usize;
                String::from_utf8(b[6..6 + len].to_vec()).unwrap()
            })
            .collect()
    }

    fn fetch_rows(
        portal: &types_portal::Portal<'static>,
        direction: types_nodes::parsenodes::FetchDirection,
        count: i64,
    ) -> (u64, Vec<String>) {
        SENT.with(|s| s.borrow_mut().clear());
        let mut dest = remote_dest(portal);
        let n = crate::PortalRunFetch(portal, direction, count, &mut dest).unwrap();
        (n, first_cols(&SENT.with(|s| s.borrow().clone())))
    }

    // FETCH_ABSOLUTE/RELATIVE arm math (pquery.c DoPortalRunFetch) over a
    // multi-row held store: SHOW ALL, first column = GUC name.
    #[test]
    fn fetch_absolute_and_relative_through_held_store() {
        use types_nodes::parsenodes::FetchDirection::*;

        let portal = start_scroll_portal("absolute-e2e", "all");
        let mut none = tcop_dest::DestReceiver::DoNothing;

        let (n, all) = fetch_rows(&portal, FETCH_FORWARD, FETCH_ALL);
        assert!(n >= 5, "SHOW ALL yields a multi-row store");
        assert_eq!(all.len() as u64, n);
        assert_eq!(
            crate::PortalRunFetch(&portal, FETCH_BACKWARD, FETCH_ALL, &mut none).unwrap(),
            n
        );
        assert_eq!(pos(&portal), (true, false, 0));

        // Forward-from-here leg: goal past halfway.
        assert_eq!(fetch_rows(&portal, FETCH_ABSOLUTE, 3), (1, vec![all[2].clone()]));
        assert_eq!(pos(&portal), (false, false, 3));

        // Rewind leg: goal at most halfway back.
        assert_eq!(fetch_rows(&portal, FETCH_ABSOLUTE, 2), (1, vec![all[1].clone()]));
        assert_eq!(pos(&portal), (false, false, 2));

        assert_eq!(fetch_rows(&portal, FETCH_ABSOLUTE, 4), (1, vec![all[3].clone()]));
        assert_eq!(pos(&portal), (false, false, 4));

        // Negative: advance to end, return the last row.
        assert_eq!(
            fetch_rows(&portal, FETCH_ABSOLUTE, -1),
            (1, vec![all[all.len() - 1].clone()])
        );
        assert_eq!(pos(&portal), (false, false, n));

        // Zero: rewind, zero rows.
        assert_eq!(fetch_rows(&portal, FETCH_ABSOLUTE, 0), (0, vec![]));
        assert_eq!(pos(&portal), (true, false, 0));

        assert_eq!(fetch_rows(&portal, FETCH_RELATIVE, 3), (1, vec![all[2].clone()]));
        assert_eq!(pos(&portal), (false, false, 3));

        assert_eq!(fetch_rows(&portal, FETCH_RELATIVE, -2), (1, vec![all[0].clone()]));
        assert_eq!(pos(&portal), (false, false, 1));

        // RELATIVE 0 == FETCH FORWARD 0: re-fetch the current row.
        assert_eq!(fetch_rows(&portal, FETCH_RELATIVE, 0), (1, vec![all[0].clone()]));
        assert_eq!(pos(&portal), (false, false, 1));
    }

    // Expected line pinned against real PostgreSQL 18.3 (the explain crate's
    // differential fixture).
    #[test]
    fn explain_select_1_wire_bytes_through_portal() {
        install_fixtures();
        let mcx = leaked_mcx();
        let query = Node::mk(mcx, select_1_query(mcx)).unwrap();
        let estmt =
            Node::mk(mcx, ExplainStmt { query: Some(query), options: NodeList::nil() }).unwrap();
        let stmts = utility_pstmt(estmt);
        let (qc, sent) = run_utility_portal("explain-e2e", "EXPLAIN SELECT 1", stmts);

        assert_eq!(data_rows(&sent), ["Result  (cost=0.00..0.01 rows=1 width=4)"]);
        assert_eq!(cmdtag::GetCommandTagName(qc.commandTag), "EXPLAIN");
        assert_eq!(qc.nprocessed, 1);
    }
}
