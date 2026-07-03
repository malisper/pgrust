// Byte-level extended-query sessions driven through the real message loop
// (SocketBackend -> dispatch -> exec_parse/bind/execute/describe) against the
// frozen live-PG 18.3 trace in fixtures/extended_query_trace.txt (captured by
// scripts/capture_extended_query_trace.py). Catalog access is faked at the
// syscache/fmgr boundary (int4 only); everything else is the shipped code.
use std::cell::{Cell, RefCell};
use std::sync::Once;

use mcx::MemoryContext;
use types_dest::CommandDest;
use types_fmgr::FmgrInfo;

use crate::main_loop::{error_recovery, run_one_iteration, LoopState};

const INT4OID: u32 = 23;
const INT4IN: u32 = 42;
const INT4OUT: u32 = 43;

thread_local! {
    static WIRE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static INPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static INPUT_POS: Cell<usize> = const { Cell::new(0) };
}

// The process-wide stub set shared by every test module in this crate (the
// seam slots are set-once per process; tests.rs and switches.rs route here).
pub(crate) static LOCK_TIMEOUT_INDICATOR: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub(crate) static STMT_TIMEOUT_INDICATOR: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub(crate) fn install_shared_stubs() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        use std::sync::atomic::Ordering;
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
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        pmsignal_seams::register_postmaster_child_active::set(|| {});
        syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
        condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
        autovacuum_seams::wake_autovacuum_launcher::set(|| {});
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
        lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
            Ok(types_storage::lock::LOCKACQUIRE_OK)
        });
        lock_seams::lock_release::set(|_, _, _| Ok(true));
        lock_seams::mark_lock_clear::set(|_, _| {});
        timeout_seams::disable_timeouts::set(|_| {});
        timeout_seams::disable_all_timeouts::set(|_| Ok(()));
        timeout_seams::get_timeout_active::set(|_| false);
        timeout_seams::disable_timeout::set(|_, _| Ok(()));
        timeout_seams::get_timeout_indicator::set(|id, reset| {
            let slot = match id {
                timeout_seams::LOCK_TIMEOUT => &LOCK_TIMEOUT_INDICATOR,
                timeout_seams::STATEMENT_TIMEOUT => &STMT_TIMEOUT_INDICATOR,
                _ => return false,
            };
            if reset {
                slot.swap(false, Ordering::Relaxed)
            } else {
                slot.load(Ordering::Relaxed)
            }
        });
        timeout_seams::get_timeout_finish_time::set(|_| 0);
        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });
        transam_xlog_seams::recovery_in_progress::set(|| false);
        subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
        syscache_seams::relation_invalidates_snapshots_only::set(|_| false);
        syscache_seams::relation_has_sys_cache::set(|_| true);
        sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
        xloginsert_seams::xlog_reset_insertion::set(|| {});
        twophase_seams::at_abort_twophase::set(|| {});
        transam_xlog_seams::set_xact_last_rec_end::set(|_| {});
        transam_xlog_seams::xlog_logical_info_active::set(|| false);
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        transam_xlog_seams::xact_last_rec_end::set(|| 0);
        inval_seams::accept_invalidation_messages::set(|| Ok(()));
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        miscinit::init_seams();
        init_small::init_seams();
    });
}

pub(crate) fn install_shared_proc_fixture() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        use init_small::globals as g;
        g::SetMaxConnections(16);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
        g::SetMyProcPid(778);
        g::SetMyDatabaseId(5);

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procsignal::ProcSignalShmemInit();
        procarray::init_seams();
        varsup::VarsupShmemInit();
        procarray::ProcArrayShmemInit();
        snapmgr::init_seams();
    });
}

fn install_proc_fixture() {
    install_shared_stubs();
    install_shared_proc_fixture();
    if !guc_tables::vars::plan_cache_mode.installed() {
        thread_local! {
            static PLAN_CACHE_MODE: Cell<i32> = const { Cell::new(0) };
        }
        guc_tables::vars::plan_cache_mode.install(guc_tables::GucVarAccessors {
            get: || PLAN_CACHE_MODE.with(Cell::get),
            set: |v| PLAN_CACHE_MODE.with(|c| c.set(v)),
        });
    }
}

fn install_xact_periphery_seams() {
    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _for_commit| {
        Ok(mcx::PgVec::new_in(mcx))
    });
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    combocid_seams::at_eoxact_combocid::set(|| {});
    multixact_seams::at_eoxact_multixact::set(|| {});
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    parallel_seams::is_parallel_worker::set(|| false);
    parallel_seams::at_eoxact_parallel::set(|_| Ok(()));
    origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_status_seams::pgstat_report_activity::set(|_, _| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
    ps_status_seams::set_ps_display::set(|_| {});
}

fn int4_type_shape() -> types_tuple::PgTypeShape {
    types_tuple::PgTypeShape {
        typlen: 4,
        typbyval: true,
        typalign: types_tuple::TYPALIGN_INT,
        typstorage: types_tuple::TYPSTORAGE_PLAIN,
        typcollation: 0,
    }
}

fn install_catalog_fixture() {
    syscache_seams::lookup_pg_type_shape::set(|typid| {
        Ok((typid == INT4OID).then(int4_type_shape))
    });
    syscache_seams::pg_type_base_shape::set(|typid| {
        Ok(Some(syscache_seams::PgTypeBaseShape {
            typtype: if typid == 705 { b'p' as i8 } else { b'b' as i8 },
            typbasetype: 0,
            typtypmod: -1,
            typelem: 0,
            typsubscript: 0,
        }))
    });
    syscache_seams::pg_type_io_shape::set(|typid| {
        Ok((typid == INT4OID).then(|| syscache_seams::PgTypeIoShape {
            oid: INT4OID,
            typinput: INT4IN,
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
        }))
    });
    fmgr_seams::fmgr_info::set(|oid| match oid {
        INT4IN => Ok(FmgrInfo::new(adt_int::builtins::fc_int4in, INT4IN, 1, true, false)),
        INT4OUT => Ok(FmgrInfo::new(adt_int::builtins::fc_int4out, INT4OUT, 1, true, false)),
        other => panic!("fmgr_info: unexpected oid {other}"),
    });
    mbutils_seams::server_to_client_conversion_needed::set(|| false);
    mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo");
        install_proc_fixture();
        install_xact_periphery_seams();
        install_catalog_fixture();

        crate::init_seams();
        pqcomm::init_seams();
        scan_fgram::init_seams();
        parser_seams::raw_parser::set(|mcx, q, mode| {
            let list = gram_core::raw_parser(mcx, q, mode)?;
            let mut v = mcx::PgVec::new_in(mcx);
            v.try_reserve_exact(list.len()).map_err(|_| mcx.oom(list.len()))?;
            for n in list.iter() {
                let rs = n.as_raw_stmt().expect("raw_parser yields RawStmt");
                v.push(types_nodes::rawnodes::RawStmt {
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
        planner::init_seams();
        execmain::init_seams();
        xact::init_seams();
        elog::init_seams();
        utility::init_seams();
        prepare::init_seams();
        plancache::init_seams();
        pquery::init_seams();
        lsyscache::init_seams();
        tuplestore::init_seams();
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
        resowner_seams::current_resource_owner::set(|| types_resowner::ResourceOwner::NULL);
        resowner_seams::resource_owner_enlarge::set(|_| Ok(()));
        resowner_seams::resource_owner_remember_snapshot::set(|_, _| {});
        resowner_seams::resource_owner_forget_snapshot::set(|_, _| {});
        resowner_portal_seams::resource_owner_create_portal::set(|| {
            types_resowner::ResourceOwner::from_parts(1, 1)
        });
        resowner_portal_seams::resource_owner_release::set(|_, _, _, _| {});
        resowner_portal_seams::resource_owner_delete::set(|_| {});
        // C PortalCleanup (portalcmds.c), executor half: shut the query down so
        // its registered snapshot is released (the portalcmds unit owns the
        // real hook).
        portalcmds_seams::portal_cleanup::set(|portal| {
            let (qd, failed) = {
                let mut p = portal.borrow_mut();
                (
                    core::mem::replace(&mut p.queryDesc, types_portal::QueryDescHandle::NULL),
                    p.status == types_portal::PortalStatus::PORTAL_FAILED,
                )
            };
            if !qd.is_null() {
                if !failed {
                    execmain_seams::executor_finish::call(qd)?;
                    execmain_seams::executor_end::call(qd)?;
                    execmain_seams::free_query_desc::call(qd);
                } else {
                    execmain_seams::release_query_desc::call(qd);
                }
            }
            Ok(())
        });

        be_secure_seams::secure_read::set(|buf| {
            INPUT.with(|q| {
                let q = q.borrow();
                let pos = INPUT_POS.with(Cell::get);
                let n = (q.len() - pos).min(buf.len());
                buf[..n].copy_from_slice(&q[pos..pos + n]);
                INPUT_POS.with(|c| c.set(pos + n));
                Ok(Ok(n))
            })
        });
        be_secure_seams::secure_write::set(|buf| {
            WIRE.with(|w| w.borrow_mut().extend_from_slice(buf));
            Ok(Ok(buf.len()))
        });
        be_secure_seams::set_port_noblock::set(|_| true);
    });

    thread_local! {
        static THREAD_UP: Cell<bool> = const { Cell::new(false) };
    }
    if !THREAD_UP.get() {
        init_small::globals::SetMyProcPid(778);
        miscinit::InitProcessLocalLatch();
        lmgr_proc::InitProcess(types_core::BackendType::Backend).expect("InitProcess");
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).expect("ProcArrayAdd");
        portalmem::EnablePortalManager();
        miscinit::SetUserIdAndSecContext(types_core::BOOTSTRAP_SUPERUSERID, 0);
        guc::initialize_guc_options().unwrap();
        pqcomm::pq_init_buffers().unwrap();
        elog::config::set_where_to_send_output(CommandDest::Remote);
        THREAD_UP.set(true);
    }
}

// PostgresMain's for(;;) body (postgres_main_inner), test rendering: same
// dispatch + error-recovery discipline, terminated by the EOF proc_exit.
fn run_session(input: Vec<u8>) -> Vec<u8> {
    WIRE.with(|w| w.borrow_mut().clear());
    INPUT.with(|q| *q.borrow_mut() = input);
    INPUT_POS.with(|c| c.set(0));

    let mut message_context = MemoryContext::new_bump("MessageContext-test");
    let mut state = LoopState {
        send_ready_for_query: true,
        idle_in_transaction_timeout_enabled: false,
        idle_session_timeout_enabled: false,
    };

    for _ in 0..200 {
        message_context.reset();
        let mcx = message_context.mcx();
        match run_one_iteration(mcx, &mut state) {
            Ok(()) => {}
            Err(err) => {
                if err.message.contains("proc_exit(0)") {
                    break;
                }
                assert!(
                    err.level() < types_error::FATAL,
                    "session died: {}",
                    err.message
                );
                error_recovery(&err, &mut state).expect("error recovery settles");
                if !crate::ignore_till_sync() {
                    state.send_ready_for_query = true;
                }
            }
        }
    }

    WIRE.with(|w| w.borrow().clone())
}

fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut v = vec![t];
    v.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

fn parse_msg(name: &str, query: &str, oids: &[u32]) -> Vec<u8> {
    let mut b = cstr(name);
    b.extend(cstr(query));
    b.extend((oids.len() as u16).to_be_bytes());
    for o in oids {
        b.extend(o.to_be_bytes());
    }
    msg(b'P', &b)
}

fn bind_msg(portal: &str, stmt: &str, params: &[&[u8]]) -> Vec<u8> {
    let mut b = cstr(portal);
    b.extend(cstr(stmt));
    b.extend(0u16.to_be_bytes()); /* all-text param formats */
    b.extend((params.len() as u16).to_be_bytes());
    for p in params {
        b.extend((p.len() as i32).to_be_bytes());
        b.extend(*p);
    }
    b.extend(0u16.to_be_bytes()); /* all-text result formats */
    msg(b'B', &b)
}

fn execute_msg(portal: &str, max_rows: i32) -> Vec<u8> {
    let mut b = cstr(portal);
    b.extend(max_rows.to_be_bytes());
    msg(b'E', &b)
}

fn describe_msg(kind: u8, name: &str) -> Vec<u8> {
    let mut b = vec![kind];
    b.extend(cstr(name));
    msg(b'D', &b)
}

fn sync_msg() -> Vec<u8> {
    msg(b'S', &[])
}

// Splits a server byte stream into (msgtype, body) frames.
fn frames(wire: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < wire.len() {
        let t = wire[i];
        let len = u32::from_be_bytes(wire[i + 1..i + 5].try_into().unwrap()) as usize;
        out.push((t, wire[i + 5..i + 1 + len].to_vec()));
        i += 1 + len;
    }
    out
}

// Batches replayed against fixtures/extended_query_trace.txt; MUST stay in
// lockstep with scripts/capture_extended_query_trace.py.
fn scripted_batches() -> Vec<(&'static str, Vec<u8>)> {
    let cat = |parts: &[Vec<u8>]| parts.concat();
    vec![
        (
            "unnamed_param_select",
            cat(&[
                parse_msg("", "SELECT $1", &[INT4OID]),
                bind_msg("", "", &[b"42"]),
                execute_msg("", 0),
                sync_msg(),
            ]),
        ),
        (
            "named_parse_describe",
            cat(&[
                parse_msg("s1", "SELECT $1", &[INT4OID]),
                describe_msg(b'S', "s1"),
                sync_msg(),
            ]),
        ),
        (
            "named_bind_exec_1",
            cat(&[bind_msg("", "s1", &[b"42"]), execute_msg("", 0), sync_msg()]),
        ),
        (
            "named_bind_exec_2",
            cat(&[bind_msg("", "s1", &[b"7"]), execute_msg("", 0), sync_msg()]),
        ),
        (
            "empty_query",
            cat(&[parse_msg("", "", &[]), bind_msg("", "", &[]), execute_msg("", 0), sync_msg()]),
        ),
        (
            "row_count_suspension",
            cat(&[
                parse_msg("", "SELECT $1", &[INT4OID]),
                bind_msg("", "", &[b"42"]),
                execute_msg("", 1),
                execute_msg("", 1),
                sync_msg(),
            ]),
        ),
        (
            "describe_portal",
            cat(&[
                parse_msg("", "SELECT $1", &[INT4OID]),
                bind_msg("", "", &[b"42"]),
                describe_msg(b'P', ""),
                execute_msg("", 0),
                sync_msg(),
            ]),
        ),
        (
            "bind_missing_stmt_skip_till_sync",
            cat(&[bind_msg("", "nope", &[b"1"]), execute_msg("", 0), sync_msg()]),
        ),
    ]
}

fn fixture_replies() -> Vec<(String, Vec<u8>)> {
    include_str!("../fixtures/extended_query_trace.txt")
        .lines()
        .map(|line| {
            let (name, hex) = line.split_once('\t').expect("name<TAB>hex");
            let bytes = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect();
            (name.to_string(), bytes)
        })
        .collect()
}

// One field of a v3 ErrorResponse body.
fn error_field(body: &[u8], code: u8) -> Option<String> {
    let mut i = 0;
    while i < body.len() && body[i] != 0 {
        let f = body[i];
        let end = body[i + 1..].iter().position(|&b| b == 0).unwrap() + i + 1;
        if f == code {
            return Some(String::from_utf8_lossy(&body[i + 1..end]).into_owned());
        }
        i = end + 1;
    }
    None
}

#[test]
fn extended_query_session_matches_live_pg_trace() {
    install_fixtures();

    let batches = scripted_batches();
    let input: Vec<u8> = batches.iter().flat_map(|(_, b)| b.iter().copied()).collect();
    let wire = run_session(input);

    // Split the reply stream at each ReadyForQuery ('Z') into per-batch chunks.
    let all = frames(&wire);
    let mut chunks: Vec<Vec<(u8, Vec<u8>)>> = Vec::new();
    let mut cur = Vec::new();
    for f in all {
        let is_z = f.0 == b'Z';
        cur.push(f);
        if is_z {
            chunks.push(core::mem::take(&mut cur));
        }
    }

    // The startup ReadyForQuery (sent before any client message; the fixture
    // capture consumed it with the greeting).
    let startup = chunks.remove(0);
    assert_eq!(startup, vec![(b'Z', b"I".to_vec())], "initial ReadyForQuery");

    let expected = fixture_replies();
    assert_eq!(chunks.len(), expected.len(), "one reply chunk per batch");

    for ((name, exp_bytes), got) in expected.iter().zip(&chunks) {
        let exp = frames(exp_bytes);
        if name == "bind_missing_stmt_skip_till_sync" {
            // Error text carries file/line provenance; compare the protocol
            // shape and the C-parity fields instead of raw bytes.
            let types: Vec<u8> = got.iter().map(|(t, _)| *t).collect();
            assert_eq!(types, vec![b'E', b'Z'], "{name}: reply shape");
            let (_, body) = &got[0];
            assert_eq!(error_field(body, b'S').as_deref(), Some("ERROR"), "{name}");
            assert_eq!(error_field(body, b'C').as_deref(), Some("26000"), "{name}");
            assert_eq!(
                error_field(body, b'M').as_deref(),
                Some("prepared statement \"nope\" does not exist"),
                "{name}"
            );
            assert_eq!(got.last().unwrap().1, exp.last().unwrap().1, "{name}: Z status");
            continue;
        }
        let got_flat: Vec<u8> = got
            .iter()
            .flat_map(|(t, b)| {
                let mut v = vec![*t];
                v.extend_from_slice(&(b.len() as u32 + 4).to_be_bytes());
                v.extend_from_slice(b);
                v
            })
            .collect();
        assert_eq!(
            got_flat,
            *exp_bytes,
            "{name}: wire bytes differ\n got: {}\n exp: {}",
            hex(&got_flat),
            hex(exp_bytes)
        );
    }

    // Named-statement reuse hit the plan cache: one CachedPlanSource served
    // both Executes as custom plans (bound params fold to Consts).
    let (generic, custom) = crate::extended_query::plan_cache_counts("s1").unwrap();
    assert_eq!((generic, custom), (0, 2), "s1 plan-cache probe");
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
