use std::cell::RefCell;
use std::sync::Once;

use mcx::{MemoryContext, PgString};
use types_core::{uaSCRAM, BackendType, InvalidOid, ProcessingMode, SECURITY_RESTRICTED_OPERATION};
use types_error::{PgResult, ERRCODE_UNDEFINED_OBJECT};

use crate::lockfile::DIRECTORY_LOCK_FILE;
use crate::*;

const ALICE: u32 = 401;
const BOB: u32 = 402;

thread_local! {
    static GUC_SETS: RefCell<Vec<(String, String)>> = const { RefCell::new(Vec::new()) };
}

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        guc_tables::init_seams();
        elog::init_seams();
        crate::init_seams();
        guc_seams::set_config_option_internal_dynamic_default::set(|name, value| {
            GUC_SETS.with_borrow_mut(|v| v.push((name.to_string(), value.to_string())));
            Ok(())
        });
        syscache_seams::lookup_authid_rolname::set(|mcx, roleid| match roleid {
            ALICE => Ok(Some(PgString::from_str_in("alice", mcx)?)),
            _ => Ok(None),
        });
        ipc_seams::on_proc_exit::set(|_callback, _arg| {});
        pgstat_seams::pgstat_set_session_end_cause_fatal::set(|| {});
        init_small_seams::my_proc_pid::set(|| std::process::id() as i32);
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
    });
}

#[test]
fn processing_mode_transitions() {
    setup();
    assert_eq!(GetProcessingMode(), ProcessingMode::InitProcessing);
    assert!(IsInitProcessingMode());
    SetProcessingMode(ProcessingMode::BootstrapProcessing);
    assert!(IsBootstrapProcessingMode());
    assert!(miscinit_seams::is_bootstrap_processing_mode::call());
    SetProcessingMode(ProcessingMode::NormalProcessing);
    assert!(IsNormalProcessingMode());
    SetProcessingMode(ProcessingMode::InitProcessing);
}

#[test]
fn backend_type_and_desc() {
    setup();
    assert_eq!(GetMyBackendType(), BackendType::Invalid);
    assert_eq!(GetBackendTypeDesc(BackendType::Invalid), "not initialized");
    assert_eq!(GetBackendTypeDesc(BackendType::Backend), "client backend");
    assert_eq!(GetBackendTypeDesc(BackendType::WalSummarizer), "walsummarizer");
    assert!(!IgnoreSystemIndexes());
    SetIgnoreSystemIndexes(true);
    assert!(IgnoreSystemIndexes());
    SetIgnoreSystemIndexes(false);
}

#[test]
fn user_id_sec_context_roundtrip_and_flags() {
    setup();
    let (uid, ctx) = GetUserIdAndSecContext();
    assert_eq!((uid, ctx), (InvalidOid, 0));

    SetUserIdAndSecContext(ALICE, 0);
    assert_eq!(GetUserId(), ALICE);
    assert!(!InLocalUserIdChange());
    assert!(!InSecurityRestrictedOperation());
    assert!(!InNoForceRLSOperation());

    SetUserIdAndContext(BOB, true).unwrap();
    assert!(InLocalUserIdChange());
    assert_eq!(GetUserIdAndContext(), (BOB, true));
    SetUserIdAndContext(ALICE, false).unwrap();
    assert!(!InLocalUserIdChange());

    SetUserIdAndSecContext(InvalidOid, 0);
}

#[test]
fn sec_context_guard_restores_on_both_paths() {
    setup();
    SetUserIdAndSecContext(ALICE, 0);

    let guard = SecContextGuard::security_restricted(BOB);
    assert_eq!(GetUserIdAndSecContext(), (BOB, SECURITY_RESTRICTED_OPERATION));
    assert!(InSecurityRestrictedOperation());
    assert!(SetUserIdAndContext(ALICE, true).is_err());
    assert_eq!(guard.saved(), (ALICE, 0));
    guard.restore();
    assert_eq!(GetUserIdAndSecContext(), (ALICE, 0));

    // Drop is the abort path.
    {
        let _guard = SecContextGuard::set(BOB, SECURITY_RESTRICTED_OPERATION);
        assert_eq!(GetUserIdAndSecContext(), (BOB, SECURITY_RESTRICTED_OPERATION));
    }
    assert_eq!(GetUserIdAndSecContext(), (ALICE, 0));

    SetUserIdAndSecContext(InvalidOid, 0);
}

#[test]
fn session_authorization_and_set_role() {
    setup();
    GUC_SETS.with_borrow_mut(Vec::clear);

    SetSessionAuthorization(ALICE, false).unwrap();
    assert_eq!(GetSessionUserId(), ALICE);
    assert!(!GetSessionUserIsSuperuser());
    assert_eq!(GetOuterUserId(), ALICE);
    assert_eq!(GetUserId(), ALICE);
    assert_eq!(GetCurrentRoleId(), InvalidOid);
    GUC_SETS.with_borrow(|v| {
        assert_eq!(v.last().unwrap(), &("is_superuser".to_string(), "off".to_string()));
    });

    SetCurrentRoleId(BOB, true).unwrap();
    assert_eq!(GetCurrentRoleId(), BOB);
    assert_eq!(GetOuterUserId(), BOB);
    assert_eq!(GetUserId(), BOB);
    SetSessionAuthorization(ALICE, false).unwrap();
    assert_eq!(GetUserId(), BOB);

    SetCurrentRoleId(InvalidOid, false).unwrap();
    assert_eq!(GetCurrentRoleId(), InvalidOid);
    assert_eq!(GetUserId(), ALICE);

    SetUserIdAndSecContext(InvalidOid, 0);
}

#[test]
fn get_user_name_from_id_paths() {
    setup();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let name = GetUserNameFromId(mcx, ALICE, false).unwrap().unwrap();
    assert_eq!(name.as_str(), "alice");
    assert!(GetUserNameFromId(mcx, BOB, true).unwrap().is_none());
    let err = GetUserNameFromId(mcx, BOB, false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
    let via_seam = miscinit_seams::get_user_name_from_id::call(mcx, ALICE, false)
        .unwrap()
        .unwrap();
    assert_eq!(via_seam.as_str(), "alice");
}

#[test]
fn system_user_format() {
    setup();
    assert!(GetSystemUser().is_none());
    InitializeSystemUser("bob", "scram-sha-256");
    assert_eq!(GetSystemUser(), Some("scram-sha-256:bob"));
}

#[test]
fn client_connection_info_roundtrip() {
    setup();
    assert_eq!(EstimateClientConnectionInfoSpace(), 8);
    set_client_connection_info(Some("md5:carol"), uaSCRAM);
    let need = EstimateClientConnectionInfoSpace();
    assert_eq!(need, 8 + "md5:carol".len() + 1);
    let mut buf = vec![0u8; need];
    SerializeClientConnectionInfo(&mut buf);

    set_client_connection_info(None, 0);
    RestoreClientConnectionInfo(&buf).unwrap();
    let (authn_id, method) = client_connection_info();
    assert_eq!(authn_id, Some("md5:carol"));
    assert_eq!(method, uaSCRAM);

    // NULL authn_id serializes as len -1 with no body.
    set_client_connection_info(None, uaSCRAM);
    let mut buf = vec![0u8; EstimateClientConnectionInfoSpace()];
    SerializeClientConnectionInfo(&mut buf);
    assert_eq!(i32::from_ne_bytes(buf[..4].try_into().unwrap()), -1);
    RestoreClientConnectionInfo(&buf).unwrap();
    assert_eq!(client_connection_info().0, None);
}

#[test]
fn local_latch_home() {
    setup();
    assert!(init_small::globals::MyLatch().is_none());
    InitProcessLocalLatch();
    let first = init_small::globals::MyLatch().unwrap();
    // Re-init reuses the slot (C's file-scope LocalLatchData).
    InitProcessLocalLatch();
    assert_eq!(init_small::globals::MyLatch(), Some(first));
    latch::SetLatch(first);
    assert!(latch::latch_ref(first).is_set());
    latch::InitLatch(first);
    assert!(!latch::latch_ref(first).is_set());
}

fn fatals(f: impl FnOnce() -> PgResult<()>) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err()
}

#[test]
fn validate_pg_version() {
    setup();
    let dir = scratch_dir("pgversion");
    assert!(fatals(|| ValidatePgVersion(&dir)));

    std::fs::write(format!("{dir}/PG_VERSION"), "18\n").unwrap();
    ValidatePgVersion(&dir).unwrap();
    std::fs::write(format!("{dir}/PG_VERSION"), "18.3\n").unwrap();
    ValidatePgVersion(&dir).unwrap();

    std::fs::write(format!("{dir}/PG_VERSION"), "17\n").unwrap();
    assert!(fatals(|| ValidatePgVersion(&dir)));
    std::fs::write(format!("{dir}/PG_VERSION"), "junk\n").unwrap();
    assert!(fatals(|| ValidatePgVersion(&dir)));
}

fn scratch_dir(tag: &str) -> String {
    let dir = std::env::temp_dir().join(format!("pgrust_miscinit_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_owned()
}

#[test]
fn lockfile_lifecycle() {
    setup();
    let dir = scratch_dir("lockfile");
    // Only this test changes cwd (postmaster.pid is cwd-relative).
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(&dir);
    init_small::globals::SetMyStartTime(1_700_000_000);

    CreateDataDirLockFile(true).unwrap();
    let contents = std::fs::read_to_string(DIRECTORY_LOCK_FILE).unwrap();
    let lines: Vec<&str> = contents.split('\n').collect();
    assert_eq!(lines[0], format!("{}", std::process::id()));
    assert_eq!(lines[1], dir);
    assert_eq!(lines[2], "1700000000");
    assert_eq!(lines[3], "5432");
    assert_eq!(lines[4], "");

    CreateDataDirLockFile(true).unwrap(); // own PID = false match, recreated

    assert!(RecheckDataDirLockFile().unwrap());

    AddToDataDirLockFile(6, "127.0.0.1").unwrap();
    AddToDataDirLockFile(7, "54321001 1234567").unwrap();
    let contents = std::fs::read_to_string(DIRECTORY_LOCK_FILE).unwrap();
    let lines: Vec<&str> = contents.split('\n').collect();
    assert_eq!(lines[5], "127.0.0.1");
    assert_eq!(lines[6], "54321001 1234567");
    AddToDataDirLockFile(6, "192.168.0.1").unwrap();
    let contents = std::fs::read_to_string(DIRECTORY_LOCK_FILE).unwrap();
    assert!(contents.contains("192.168.0.1\n54321001 1234567"));

    let socketfile = format!("{dir}/.s.PGSQL.5432");
    CreateSocketLockFile(&socketfile, true, "/tmp").unwrap();
    assert!(std::fs::metadata(format!("{socketfile}.lock")).is_ok());
    TouchSocketLockFiles();

    std::fs::write(DIRECTORY_LOCK_FILE, "1\n").unwrap(); // wrong PID
    assert!(!RecheckDataDirLockFile().unwrap());

    UnlinkLockFiles(0, 0);
    assert!(std::fs::metadata(DIRECTORY_LOCK_FILE).is_err());
    assert!(std::fs::metadata(format!("{socketfile}.lock")).is_err());
}

#[test]
fn stale_lockfile_from_dead_pid_is_replaced() {
    setup();
    let dir = scratch_dir("stalelock");
    let lockfile = format!("{dir}/dead.lock");
    // 999999999: kill(pid,0) => ESRCH on any sane box.
    std::fs::write(&lockfile, "999999999\n/nowhere\n0\n0\n\n").unwrap();
    CreateSocketLockFile(&format!("{dir}/dead"), false, "/tmp").unwrap();
    let contents = std::fs::read_to_string(&lockfile).unwrap();
    assert!(contents.starts_with(&format!("-{}\n", std::process::id())));
}
