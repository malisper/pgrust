use std::cell::Cell;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgString};
use types_core::{
    InvalidOid, Oid, UserContext, DATEORDER_MDY, INTSTYLE_POSTGRES, INVALID_PROC_NUMBER,
    MAXPGPATH, MAX_CANCEL_KEY_LENGTH, PG_DIR_MODE_OWNER, SECURITY_RESTRICTED_OPERATION,
    USER_CONTEXT_NO_NEST_LEVEL, USE_ISO_DATES,
};
use types_error::{PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE};

use crate::globals;

thread_local! {
    static CURRENT_USER: Cell<(Oid, i32)> = const { Cell::new((InvalidOid, 0)) };
    static CAN_SET_FORWARD: Cell<bool> = const { Cell::new(false) };
    static CAN_SET_REVERSE: Cell<bool> = const { Cell::new(false) };
    static LAST_EOXACT: Cell<Option<(bool, i32)>> = const { Cell::new(None) };
}

const SAVE_USER: Oid = 10;
const TARGET_USER: Oid = 20;
const NEST_LEVEL: i32 = 7;

fn can_set_role(member: Oid, role: Oid) -> PgResult<bool> {
    Ok(match (member, role) {
        (SAVE_USER, TARGET_USER) => CAN_SET_FORWARD.get(),
        (TARGET_USER, SAVE_USER) => CAN_SET_REVERSE.get(),
        _ => false,
    })
}

fn user_name<'mcx>(mcx: Mcx<'mcx>, roleid: Oid, _noerr: bool) -> PgResult<Option<PgString<'mcx>>> {
    Ok(Some(PgString::from_str_in(&format!("role{roleid}"), mcx)?))
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        miscinit_seams::get_user_id_and_sec_context::set(|| CURRENT_USER.get());
        miscinit_seams::set_user_id_and_sec_context::set(|userid, sec_context| {
            CURRENT_USER.set((userid, sec_context))
        });
        miscinit_seams::get_user_name_from_id::set(user_name);
        acl_seams::member_can_set_role::set(can_set_role);
        guc_seams::new_guc_nest_level::set(|| NEST_LEVEL);
        guc_seams::at_eoxact_guc::set(|is_commit, nest_level| {
            LAST_EOXACT.set(Some((is_commit, nest_level)));
            Ok(())
        });
    });
}

#[test]
fn defaults_match_c_initializers() {
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
    assert_eq!(globals::MyCancelKey(), [0; MAX_CANCEL_KEY_LENGTH]);
    assert_eq!(globals::MyCancelKeyLength(), 0);
    assert_eq!(globals::MyPMChildSlot(), 0);
    assert_eq!(globals::data_directory_mode(), PG_DIR_MODE_OWNER);
    assert_eq!(globals::OutputFileName(), [0; MAXPGPATH]);
    assert_eq!(globals::my_exec_path(), [0; MAXPGPATH]);
    assert_eq!(globals::pkglib_path(), [0; MAXPGPATH]);
    assert_eq!(globals::MyProcNumber(), INVALID_PROC_NUMBER);
    assert_eq!(globals::ParallelLeaderProcNumber(), INVALID_PROC_NUMBER);
    assert_eq!(globals::MyDatabaseId(), InvalidOid);
    assert_eq!(globals::MyDatabaseTableSpace(), InvalidOid);
    assert!(!globals::MyDatabaseHasLoginEventTriggers());
    assert_eq!(globals::PostmasterPid(), 0);
    assert!(!globals::IsPostmasterEnvironment());
    assert!(!globals::IsUnderPostmaster());
    assert!(!globals::IsBinaryUpgrade());
    assert!(!globals::ExitOnAnyError());
    assert_eq!(globals::DateStyle(), USE_ISO_DATES);
    assert_eq!(globals::DateOrder(), DATEORDER_MDY);
    assert_eq!(globals::IntervalStyle(), INTSTYLE_POSTGRES);
    assert!(globals::enableFsync());
    assert!(!globals::allowSystemTableMods());
    assert_eq!(globals::work_mem(), 4096);
    assert_eq!(globals::hash_mem_multiplier(), 2.0);
    assert_eq!(globals::maintenance_work_mem(), 65536);
    assert_eq!(globals::max_parallel_maintenance_workers(), 2);
    assert_eq!(globals::NBuffers(), 16384);
    assert_eq!(globals::MaxConnections(), 100);
    assert_eq!(globals::max_worker_processes(), 16);
    assert_eq!(globals::max_parallel_workers(), 16);
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
    assert_eq!(globals::DataDir(), None);
    assert_eq!(globals::DatabasePath(), None);
}

#[test]
fn scalar_roundtrip() {
    globals::SetMyProcPid(4242);
    assert_eq!(globals::MyProcPid(), 4242);
    globals::SetMyDatabaseId(16384);
    assert_eq!(globals::MyDatabaseId(), 16384);
    let mut key = [0u8; MAX_CANCEL_KEY_LENGTH];
    key[..4].copy_from_slice(&[1, 2, 3, 4]);
    globals::SetMyCancelKey(key);
    globals::SetMyCancelKeyLength(4);
    assert_eq!(globals::MyCancelKey()[..4], [1, 2, 3, 4]);
    assert_eq!(globals::MyCancelKeyLength(), 4);
}

#[test]
fn paths_set_and_replace() {
    globals::SetDataDir("/tmp/pgdata");
    assert_eq!(globals::DataDir(), Some("/tmp/pgdata"));
    globals::SetDataDir("/tmp/pgdata2");
    assert_eq!(globals::DataDir(), Some("/tmp/pgdata2"));
    globals::SetDatabasePath("base/5");
    assert_eq!(globals::DatabasePath(), Some("base/5"));
}

#[test]
fn interrupt_helpers() {
    assert!(globals::InterruptsCanBeProcessed());
    globals::HoldInterrupts();
    globals::HoldInterrupts();
    assert_eq!(globals::InterruptHoldoffCount(), 2);
    assert!(!globals::InterruptsCanBeProcessed());
    globals::ResumeInterrupts();
    globals::ResumeInterrupts();
    assert!(globals::InterruptsCanBeProcessed());

    globals::HoldCancelInterrupts();
    assert!(!globals::InterruptsCanBeProcessed());
    globals::ResumeCancelInterrupts();

    globals::StartCriticalSection();
    assert_eq!(globals::CritSectionCount(), 1);
    assert!(!globals::InterruptsCanBeProcessed());
    globals::EndCriticalSection();
    assert!(globals::InterruptsCanBeProcessed());
}

#[test]
#[should_panic(expected = "InterruptHoldoffCount underflow")]
fn resume_interrupts_underflow() {
    globals::ResumeInterrupts();
}

#[test]
#[should_panic(expected = "QueryCancelHoldoffCount underflow")]
fn resume_cancel_interrupts_underflow() {
    globals::ResumeCancelInterrupts();
}

#[test]
#[should_panic(expected = "CritSectionCount underflow")]
fn end_critical_section_underflow() {
    globals::EndCriticalSection();
}

#[test]
fn guc_var_slots_reach_globals() {
    setup();
    use guc_tables::vars;
    assert_eq!(vars::work_mem.read(), 4096);
    vars::work_mem.write(8192);
    assert_eq!(globals::work_mem(), 8192);
    globals::set_work_mem(4096);
    assert_eq!(vars::NBuffers.read(), 16384);
    // ExitOnAnyError slot ownership moved to elog (deferred.md double-install).
    assert_eq!(vars::VacuumCostDelay.read(), 0.0);
    assert_eq!(vars::IntervalStyle.read(), INTSTYLE_POSTGRES);
    assert_eq!(init_small_seams::my_proc_pid::call(), globals::MyProcPid());
}

#[test]
fn switch_to_untrusted_user_refused() {
    setup();
    let ctx = MemoryContext::new("test");
    CURRENT_USER.set((SAVE_USER, 0));
    CAN_SET_FORWARD.set(false);
    let mut uc = UserContext::default();
    let err = crate::SwitchToUntrustedUser(ctx.mcx(), TARGET_USER, &mut uc).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
    assert_eq!(
        err.message(),
        "role \"role10\" cannot SET ROLE to \"role20\""
    );
    assert_eq!(CURRENT_USER.get(), (SAVE_USER, 0));
}

#[test]
fn switch_to_untrusted_user_mutual_trust() {
    setup();
    let ctx = MemoryContext::new("test");
    CURRENT_USER.set((SAVE_USER, 0));
    CAN_SET_FORWARD.set(true);
    CAN_SET_REVERSE.set(true);
    let mut uc = UserContext::default();
    crate::SwitchToUntrustedUser(ctx.mcx(), TARGET_USER, &mut uc).unwrap();
    assert_eq!(CURRENT_USER.get(), (TARGET_USER, 0));
    assert_eq!(uc.save_nestlevel, USER_CONTEXT_NO_NEST_LEVEL);

    LAST_EOXACT.set(None);
    crate::RestoreUserContext(&uc).unwrap();
    assert_eq!(CURRENT_USER.get(), (SAVE_USER, 0));
    assert_eq!(LAST_EOXACT.get(), None);
}

#[test]
fn switch_to_untrusted_user_one_way_trust() {
    setup();
    let ctx = MemoryContext::new("test");
    CURRENT_USER.set((SAVE_USER, 0));
    CAN_SET_FORWARD.set(true);
    CAN_SET_REVERSE.set(false);
    let mut uc = UserContext::default();
    crate::SwitchToUntrustedUser(ctx.mcx(), TARGET_USER, &mut uc).unwrap();
    assert_eq!(
        CURRENT_USER.get(),
        (TARGET_USER, SECURITY_RESTRICTED_OPERATION)
    );
    assert_eq!(uc.save_nestlevel, NEST_LEVEL);

    LAST_EOXACT.set(None);
    crate::RestoreUserContext(&uc).unwrap();
    assert_eq!(CURRENT_USER.get(), (SAVE_USER, 0));
    assert_eq!(LAST_EOXACT.get(), Some((false, NEST_LEVEL)));
}

mod wretain_state {
    use crate::wretain;
    use types_core::InvalidOid;

    // One test fn: TLS state is per-thread and #[test] threads are distinct.
    #[test]
    fn park_claim_lifecycle() {
        // Fresh thread: nothing armed.
        assert!(!wretain::candidate());
        assert!(!wretain::warm_claim());
        assert!(!wretain::identity_held());

        // First task, retention on: cold init.
        wretain::begin_task(true);
        assert!(wretain::candidate());
        assert!(!wretain::warm_claim());

        // Clean park: both arms report retention.
        wretain::set_retained_db(5);
        wretain::request_park(3);
        assert!(wretain::parking());
        wretain::note_proc_retained();
        wretain::note_sinval_retained();
        assert!(wretain::confirm_parked());
        assert!(wretain::identity_held());
        assert_eq!(wretain::retained_db(), 5);
        assert_eq!(wretain::parked_barrier_gen(), 3);

        // Next claim is warm.
        wretain::begin_task(true);
        assert!(wretain::warm_claim());
        wretain::refuse_park();
        assert!(!wretain::candidate());
        assert!(!wretain::warm_claim());
        assert!(!wretain::parking());

        wretain::begin_task(true);
        // Partial park (one arm missed): not parked; identity marks stay for
        // the release path (which ends with clear_identity).
        wretain::request_park(4);
        wretain::note_proc_retained();
        assert!(!wretain::confirm_parked());
        assert!(wretain::proc_retained());
        assert!(!wretain::sinval_retained());
        assert!(wretain::identity_held());
        wretain::clear_identity();
        assert!(!wretain::identity_held());
        assert_eq!(wretain::retained_db(), InvalidOid);

        // Retention disabled at begin_task: never a warm claim, never parks.
        wretain::begin_task(false);
        assert!(!wretain::candidate());
        assert!(!wretain::warm_claim());
        assert!(!wretain::confirm_parked());
    }

    // Uncommitted-catalog taint (the rolled-back-TRUNCATE parallel-scan
    // locator fix): set when a task binds a leader transaction with
    // unbroadcast invalidation messages; must survive park + the next
    // begin_task (the poison lives in the thread's caches, not the task) and
    // clear only via claim-side blanket invalidation or retirement.
    // Runs on its own #[test] thread, so TLS starts fresh.
    #[test]
    fn caches_taint_lifecycle() {
        assert!(!wretain::caches_tainted());

        wretain::begin_task(true);
        wretain::note_caches_tainted();
        wretain::request_park(1);
        wretain::note_proc_retained();
        wretain::note_sinval_retained();
        assert!(wretain::confirm_parked());
        assert!(wretain::caches_tainted());

        // Warm claim: taint still standing — the consumer must blanket.
        wretain::begin_task(true);
        assert!(wretain::warm_claim());
        assert!(wretain::caches_tainted());

        // The blanket ran: trustworthy again.
        wretain::clear_caches_taint();
        assert!(!wretain::caches_tainted());

        // Retirement drops the mark with the identity.
        wretain::note_caches_tainted();
        wretain::clear_identity();
        assert!(!wretain::caches_tainted());
    }
}
