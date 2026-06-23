use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Once;

use ::mcx::{MemoryContext, PgString};
use ::types_core::{
    InvalidOid, Oid, UserContext, INVALID_PROC_NUMBER, MAX_CANCEL_KEY_LENGTH, PG_DIR_MODE_OWNER,
    SECURITY_LOCAL_USERID_CHANGE, SECURITY_RESTRICTED_OPERATION, USER_CONTEXT_NO_NEST_LEVEL,
    USE_ISO_DATES,
};
use ::types_error::{PgError, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_OBJECT, ERROR};
use ::types_storage::latch::Latch;

use crate::globals;
use crate::usercontext::{RestoreUserContext, SwitchToUntrustedUser};

// ----- globals.c -----

#[test]
fn defaults_match_globals_c() {
    // Run on a dedicated thread so other tests' writes can't be seen.
    std::thread::spawn(|| {
        assert_eq!(globals::FrontendProtocol(), 0);
        assert!(!globals::InterruptPending());
        assert!(!globals::QueryCancelPending());
        assert!(!globals::ProcDiePending());
        assert!(!globals::CheckClientConnectionPending());
        assert!(!globals::ClientConnectionLost());
        assert!(!globals::IdleInTransactionSessionTimeoutPending());
        assert!(!globals::TransactionTimeoutPending());
        assert!(!globals::IdleSessionTimeoutPending());
        assert!(!globals::ProcSignalBarrierPending());
        assert!(!globals::LogMemoryContextPending());
        assert!(!globals::IdleStatsUpdateTimeoutPending());
        assert_eq!(globals::InterruptHoldoffCount(), 0);
        assert_eq!(globals::QueryCancelHoldoffCount(), 0);
        assert_eq!(globals::CritSectionCount(), 0);
        assert_eq!(globals::MyProcPid(), 0);
        assert_eq!(globals::MyStartTime(), 0);
        assert_eq!(globals::MyStartTimestamp(), 0);
        assert!(!globals::MyClientSocketIsSet());
        assert!(!globals::MyProcPortIsSet());
        assert_eq!(globals::MyCancelKey(), [0; MAX_CANCEL_KEY_LENGTH]);
        assert_eq!(globals::MyCancelKeyLength(), 0);
        assert_eq!(globals::MyPMChildSlot(), 0);
        assert!(!globals::MyLatchIsSet());
        assert_eq!(globals::DataDir(), None);
        assert_eq!(globals::data_directory_mode(), PG_DIR_MODE_OWNER);
        assert_eq!(globals::OutputFileName(), [0; ::types_core::MAXPGPATH]);
        assert_eq!(globals::MyProcNumber(), INVALID_PROC_NUMBER);
        assert_eq!(globals::ParallelLeaderProcNumber(), INVALID_PROC_NUMBER);
        assert_eq!(globals::MyDatabaseId(), InvalidOid);
        assert_eq!(globals::MyDatabaseTableSpace(), InvalidOid);
        assert!(!globals::MyDatabaseHasLoginEventTriggers());
        assert_eq!(globals::DatabasePath(), None);
        assert_eq!(globals::PostmasterPid(), 0);
        assert!(!globals::IsPostmasterEnvironment());
        assert!(!globals::IsUnderPostmaster());
        assert!(!globals::IsBinaryUpgrade());
        assert!(!globals::ExitOnAnyError());
        assert_eq!(globals::DateStyle(), USE_ISO_DATES);
        assert_eq!(globals::DateOrder(), ::types_core::DATEORDER_MDY);
        assert_eq!(globals::IntervalStyle(), ::types_core::INTSTYLE_POSTGRES);
        assert!(globals::enableFsync());
        assert!(!globals::allowSystemTableMods());
        assert_eq!(globals::work_mem(), 4096);
        assert_eq!(globals::hash_mem_multiplier(), 2.0);
        assert_eq!(globals::maintenance_work_mem(), 65536);
        assert_eq!(globals::max_parallel_maintenance_workers(), 2);
        assert_eq!(globals::NBuffers(), 16384);
        assert_eq!(globals::MaxConnections(), 100);
        assert_eq!(globals::max_worker_processes(), 8);
        assert_eq!(globals::max_parallel_workers(), 8);
        assert_eq!(globals::MaxBackends(), 0);
        assert_eq!(globals::VacuumBufferUsageLimit(), 2048);
        assert_eq!(globals::VacuumCostPageHit(), 1);
        assert_eq!(globals::VacuumCostPageMiss(), 2);
        assert_eq!(globals::VacuumCostPageDirty(), 20);
        assert_eq!(globals::VacuumCostLimit(), 200);
        assert_eq!(globals::VacuumCostDelay(), 0.0);
        assert_eq!(globals::VacuumCostBalance(), 0);
        assert!(!globals::VacuumCostActive());
        assert_eq!(globals::commit_timestamp_buffers(), 0);
        assert_eq!(globals::multixact_member_buffers(), 32);
        assert_eq!(globals::multixact_offset_buffers(), 16);
        assert_eq!(globals::notify_buffers(), 16);
        assert_eq!(globals::serializable_buffers(), 32);
        assert_eq!(globals::subtransaction_buffers(), 0);
        assert_eq!(globals::transaction_buffers(), 0);
    })
    .join()
    .unwrap();
}

#[test]
fn accessors_update_backend_local_state() {
    std::thread::spawn(|| {
        globals::set_work_mem(8192);
        assert_eq!(globals::work_mem(), 8192);

        globals::SetInterruptPending(true);
        assert!(globals::InterruptPending());

        // MyLatch is a handle to a shared latch object: the getter hands
        // back the same object (pointer copy), not a value copy.
        let latch = std::sync::Arc::new(Latch::new(true, 42));
        globals::SetMyLatch(Some(latch.clone()));
        assert!(globals::MyLatchIsSet());
        assert!(std::sync::Arc::ptr_eq(
            &globals::MyLatch().unwrap(),
            &latch
        ));
        latch
            .is_set
            .store(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            globals::MyLatch()
                .unwrap()
                .is_set
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert!(std::sync::Arc::ptr_eq(
            &globals::TakeMyLatch().unwrap(),
            &latch
        ));
        assert!(!globals::MyLatchIsSet());

        globals::SetDataDir(Some(String::from("/var/lib/pgdata")));
        assert_eq!(globals::DataDir().as_deref(), Some("/var/lib/pgdata"));
        globals::SetDataDir(None);
        assert_eq!(globals::DataDir(), None);
    })
    .join()
    .unwrap();
}

#[test]
fn elog_visible_globals_share_a_single_store() {
    // FrontendProtocol / CritSectionCount / IsUnderPostmaster /
    // ExitOnAnyError / OutputFileName are one variable each in C, read by
    // elog.c; the accessors must hit the same store elog.c uses.
    std::thread::spawn(|| {
        globals::SetExitOnAnyError(true);
        assert!(utils_error::config::exit_on_any_error());
        assert!(globals::ExitOnAnyError());

        globals::SetCritSectionCount(3);
        assert_eq!(utils_error::config::crit_section_count(), 3);
        // errfinish's ERROR recovery writes the shared store; the C-named
        // reader must observe it.
        utils_error::config::set_crit_section_count(0);
        assert_eq!(globals::CritSectionCount(), 0);

        globals::SetIsUnderPostmaster(true);
        assert!(utils_error::config::is_under_postmaster());

        globals::SetFrontendProtocol(0x0003_0000);
        assert_eq!(
            utils_error::config::frontend_protocol(),
            0x0003_0000
        );

        globals::SetOutputFileNameStr("/tmp/out.log");
        assert_eq!(
            utils_error::config::output_file_name().as_deref(),
            Some("/tmp/out.log")
        );
        let buf = globals::OutputFileName();
        assert_eq!(&buf[..12], b"/tmp/out.log");
        assert_eq!(buf[12], 0);

        let mut raw = [0u8; ::types_core::MAXPGPATH];
        raw[..7].copy_from_slice(b"out.txt");
        globals::SetOutputFileName(raw);
        assert_eq!(
            utils_error::config::output_file_name().as_deref(),
            Some("out.txt")
        );
        globals::SetOutputFileName([0; ::types_core::MAXPGPATH]);
        assert_eq!(utils_error::config::output_file_name(), None);
        assert_eq!(globals::OutputFileName(), [0; ::types_core::MAXPGPATH]);
    })
    .join()
    .unwrap();
}

#[test]
fn globals_are_per_backend_thread() {
    let a = std::thread::spawn(|| {
        globals::SetMyDatabaseId(111);
        globals::MyDatabaseId()
    });
    let b = std::thread::spawn(globals::MyDatabaseId);
    assert_eq!(a.join().unwrap(), 111);
    assert_eq!(b.join().unwrap(), InvalidOid);
}

#[test]
fn init_seams_installs_work_mem() {
    crate::init_seams();
    assert!(init_small_seams::work_mem::is_installed());
    std::thread::spawn(|| {
        globals::set_work_mem(1234);
        assert_eq!(init_small_seams::work_mem::call(), 1234);
    })
    .join()
    .unwrap();
}

// ----- usercontext.c -----
//
// The outward calls (miscinit / acl / guc) go through seams; install
// process-wide fakes once, backed by per-test (per-thread) state.

thread_local! {
    static CURRENT: Cell<(Oid, i32)> = const { Cell::new((InvalidOid, 0)) };
    static SET_CALLS: RefCell<Vec<(Oid, i32)>> = const { RefCell::new(Vec::new()) };
    static MEMBERSHIPS: RefCell<BTreeSet<(Oid, Oid)>> = const { RefCell::new(BTreeSet::new()) };
    static ROLE_NAMES: RefCell<BTreeMap<Oid, String>> = const { RefCell::new(BTreeMap::new()) };
    static NEXT_NEST_LEVEL: Cell<i32> = const { Cell::new(1) };
    static ABORTED_NEST_LEVELS: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
}

fn install_fakes() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        miscinit_seams::get_user_id_and_sec_context::set(|| CURRENT.get());
        miscinit_seams::set_user_id_and_sec_context::set(|userid, sec| {
            CURRENT.set((userid, sec));
            SET_CALLS.with_borrow_mut(|calls| calls.push((userid, sec)));
        });
        miscinit_seams::get_user_name_from_id::set(|mcx, roleid, noerr| {
            ROLE_NAMES.with_borrow(|names| match names.get(&roleid) {
                Some(name) => Ok(Some(PgString::from_str_in(name, mcx)?)),
                None if noerr => Ok(None),
                None => Err(PgError::error(format!(
                    "role with OID {roleid} does not exist"
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)),
            })
        });
        acl_seams::member_can_set_role::set(|member, role| {
            Ok(MEMBERSHIPS.with_borrow(|m| m.contains(&(member, role))))
        });
        guc_seams::new_guc_nest_level::set(|| NEXT_NEST_LEVEL.get());
        guc_seams::at_eoxact_guc::set(|is_commit, nest_level| {
            assert!(!is_commit);
            ABORTED_NEST_LEVELS.with_borrow_mut(|v| v.push(nest_level));
            Ok(())
        });
    });
}

fn setup_session(userid: Oid, sec_context: i32) {
    install_fakes();
    CURRENT.set((userid, sec_context));
    ROLE_NAMES.with_borrow_mut(|m| {
        m.insert(userid, String::from("current"));
    });
}

fn add_role(oid: Oid, name: &str) {
    ROLE_NAMES.with_borrow_mut(|m| {
        m.insert(oid, String::from(name));
    });
}

fn allow_set_role(member: Oid, role: Oid) {
    MEMBERSHIPS.with_borrow_mut(|m| {
        m.insert((member, role));
    });
}

#[test]
fn reciprocal_role_switch_has_no_guc_nest_level() {
    setup_session(10, SECURITY_LOCAL_USERID_CHANGE);
    add_role(20, "target");
    allow_set_role(10, 20);
    allow_set_role(20, 10);
    let ctx = MemoryContext::new("test");
    let mut context = UserContext::default();

    SwitchToUntrustedUser(ctx.mcx(), 20, &mut context).unwrap();

    assert_eq!(context.save_userid, 10);
    assert_eq!(context.save_sec_context, SECURITY_LOCAL_USERID_CHANGE);
    assert_eq!(context.save_nestlevel, USER_CONTEXT_NO_NEST_LEVEL);
    assert_eq!(CURRENT.get(), (20, SECURITY_LOCAL_USERID_CHANGE));

    RestoreUserContext(&context).unwrap();

    assert!(ABORTED_NEST_LEVELS.with_borrow(Vec::is_empty));
    assert_eq!(CURRENT.get(), (10, SECURITY_LOCAL_USERID_CHANGE));
}

#[test]
fn one_way_role_switch_imposes_security_restricted_operation() {
    setup_session(10, SECURITY_LOCAL_USERID_CHANGE);
    add_role(20, "target");
    allow_set_role(10, 20);
    NEXT_NEST_LEVEL.set(42);
    let ctx = MemoryContext::new("test");
    let mut context = UserContext::default();

    SwitchToUntrustedUser(ctx.mcx(), 20, &mut context).unwrap();

    assert_eq!(context.save_userid, 10);
    assert_eq!(context.save_sec_context, SECURITY_LOCAL_USERID_CHANGE);
    assert_eq!(context.save_nestlevel, 42);
    assert_eq!(
        CURRENT.get(),
        (
            20,
            SECURITY_LOCAL_USERID_CHANGE | SECURITY_RESTRICTED_OPERATION
        )
    );

    RestoreUserContext(&context).unwrap();

    assert_eq!(ABORTED_NEST_LEVELS.with_borrow(Clone::clone), vec![42]);
    assert_eq!(CURRENT.get(), (10, SECURITY_LOCAL_USERID_CHANGE));
}

#[test]
fn disallowed_role_switch_matches_postgres_error() {
    setup_session(10, 0);
    add_role(10, "alice");
    add_role(20, "bob");
    let ctx = MemoryContext::new("test");
    let mut context = UserContext::default();

    let error = SwitchToUntrustedUser(ctx.mcx(), 20, &mut context).unwrap_err();

    assert_eq!(error.level, ERROR);
    assert_eq!(error.sqlstate, ERRCODE_INSUFFICIENT_PRIVILEGE);
    assert_eq!(error.message, "role \"alice\" cannot SET ROLE to \"bob\"");
    assert_eq!(context.save_userid, 10);
    assert_eq!(context.save_sec_context, 0);
    assert_eq!(context.save_nestlevel, USER_CONTEXT_NO_NEST_LEVEL);
    assert_eq!(CURRENT.get(), (10, 0));
    assert!(SET_CALLS.with_borrow(Vec::is_empty));
}

#[test]
fn role_name_lookup_error_propagates() {
    install_fakes();
    CURRENT.set((10, 0));
    add_role(20, "bob");
    let ctx = MemoryContext::new("test");
    let mut context = UserContext::default();

    let error = SwitchToUntrustedUser(ctx.mcx(), 20, &mut context).unwrap_err();

    assert_eq!(error.message, "role with OID 10 does not exist");
}

#[test]
fn restore_uses_saved_values() {
    install_fakes();
    CURRENT.set((InvalidOid, 99));
    let context = UserContext::new(30, 7, 3);

    RestoreUserContext(&context).unwrap();

    assert_eq!(ABORTED_NEST_LEVELS.with_borrow(Clone::clone), vec![3]);
    assert_eq!(SET_CALLS.with_borrow(Clone::clone), vec![(30, 7)]);
}
