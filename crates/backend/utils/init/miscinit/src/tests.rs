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
        // GL-SHMSEAM-1: the shipped foreign-segment probe, not a stub — these
        // tests are the regression bar for the migrate-from-C boot path.
        sysv_shmem::init_seams();
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

// Serializes the tests that touch the process-global local-latch free list,
// so slot-reuse assertions can't race another test's allocation.
static LATCH_SLAB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn local_latch_home() {
    setup();
    let _g = LATCH_SLAB_LOCK.lock().unwrap();
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

#[test]
fn local_latch_release_guard_recycles_slot() {
    setup();
    let _g = LATCH_SLAB_LOCK.lock().unwrap();

    let backend = || {
        std::thread::spawn(|| {
            let _release = LocalLatchReleaseGuard::new();
            InitProcessLocalLatch();
            init_small::globals::MyLatch().unwrap()
        })
        .join()
        .unwrap()
    };

    // Normal exit returns the slot; the next backend thread reuses it.
    let first = backend();
    assert_eq!(backend(), first);

    // Panic unwind returns the slot too.
    let (tx, rx) = std::sync::mpsc::channel();
    let crashed = std::thread::spawn(move || {
        let _release = LocalLatchReleaseGuard::new();
        InitProcessLocalLatch();
        tx.send(init_small::globals::MyLatch().unwrap()).unwrap();
        panic!("simulated backend crash");
    });
    assert!(crashed.join().is_err());
    assert_eq!(rx.recv().unwrap(), first);
    assert_eq!(backend(), first);

    // A guard on a thread that never allocated is a no-op.
    std::thread::spawn(|| drop(LocalLatchReleaseGuard::new())).join().unwrap();
    assert_eq!(backend(), first);
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

// ---------------------------------------------------------------------------
// Datadir first contact: a postmaster.pid written by C PostgreSQL.
//
// C writes eight lines (pidfile.h) and line 7 carries the System V shared
// memory key and id. pgrust creates no SysV segment and leaves that line
// blank, so before GL-SHMSEAM-1 the orphaned-segment interlock below reached
// an uninstalled seam and the server *panicked* on first contact with any data
// directory a C postmaster had ever held. Every case here asserts the clean
// C-shaped FATAL instead: message, hint, SQLSTATE, exit code 1, and no panic
// out of the seam machinery. Release-effective — no debug_assert.
// ---------------------------------------------------------------------------

use types_error::{PgError, SqlState, ERRCODE_LOCK_FILE_EXISTS};

thread_local! {
    static EMITTED: RefCell<Vec<(SqlState, String, Option<String>)>> =
        const { RefCell::new(Vec::new()) };
}

fn record_emitted(error: &PgError, _output_to_server: &mut bool) {
    EMITTED.with_borrow_mut(|v| {
        v.push((error.sqlstate, error.message.clone(), error.hint.clone()))
    });
}

#[derive(Debug)]
struct Refusal {
    sqlstate: SqlState,
    message: String,
    hint: Option<String>,
    /// The panic payload the FATAL path exits through. `setup()` installs
    /// proc_exit as `panic!("proc_exit({code})")`, so this both proves the exit
    /// code and separates a clean refusal from a seam-not-installed panic.
    exit: String,
}

/// Runs `f`, which must refuse with a FATAL, and returns what it reported.
fn refuses(f: impl FnOnce() -> PgResult<()>) -> Refusal {
    EMITTED.with_borrow_mut(Vec::clear);
    let previous = elog::set_emit_log_hook(Some(record_emitted));
    let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
    elog::set_emit_log_hook(previous);

    let payload = match unwound {
        Ok(_) => panic!("expected a FATAL refusal; the call returned instead"),
        Err(payload) => payload,
    };
    let exit = match payload.downcast::<String>() {
        Ok(s) => *s,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(s) => (*s).to_owned(),
            Err(_) => "<non-string panic payload>".to_owned(),
        },
    };
    let (sqlstate, message, hint) = EMITTED
        .with_borrow(|v| v.last().cloned())
        .expect("the FATAL was reported before exiting");
    Refusal { sqlstate, message, hint, exit }
}

/// A System V segment standing in for one a C postmaster left behind.
struct CSegment {
    id: libc::c_int,
    attached: Option<*mut libc::c_void>,
}

impl CSegment {
    /// Creates the segment and stamps the header C's PGSharedMemoryCreate
    /// writes for `datadir`, keeping it attached: shm_nattch stays nonzero,
    /// which is exactly the orphaned-backend shape the interlock exists for.
    fn held_by(datadir: &str) -> CSegment {
        use std::os::unix::fs::MetadataExt;
        use types_storage::{PGShmemHeader, PGShmemMagic};

        // SAFETY: IPC_PRIVATE mints a fresh key; nothing else can name it yet.
        let id = unsafe {
            libc::shmget(
                libc::IPC_PRIVATE,
                std::mem::size_of::<PGShmemHeader>(),
                libc::IPC_CREAT | 0o600,
            )
        };
        assert!(id >= 0, "shmget: {}", std::io::Error::last_os_error());
        // SAFETY: our own segment, kernel-chosen address.
        let addr = unsafe { libc::shmat(id, std::ptr::null(), 0) };
        assert!(addr as isize != -1, "shmat: {}", std::io::Error::last_os_error());

        let meta = std::fs::metadata(datadir).unwrap();
        let hdr = PGShmemHeader {
            magic: PGShmemMagic,
            creatorPID: 424242,
            totalsize: 0,
            freeoffset: 0,
            dsm_control: 0,
            index: std::ptr::null_mut(),
            device: meta.dev() as libc::dev_t,
            inode: meta.ino() as libc::ino_t,
        };
        // SAFETY: `addr` maps size_of::<PGShmemHeader>() bytes we just created.
        unsafe { std::ptr::write(addr as *mut PGShmemHeader, hdr) };
        CSegment { id, attached: Some(addr) }
    }
}

impl Drop for CSegment {
    fn drop(&mut self) {
        if let Some(addr) = self.attached.take() {
            // SAFETY: our own mapping.
            unsafe { libc::shmdt(addr) };
        }
        if self.id >= 0 {
            // SAFETY: our own segment id.
            unsafe { libc::shmctl(self.id, libc::IPC_RMID, std::ptr::null_mut()) };
            self.id = -1;
        }
    }
}

/// A gone segment id: created and immediately removed, so shmctl reports
/// EINVAL (or, on Linux, sometimes EIDRM — port/linux.h's known kernel bug).
fn removed_segment_id() -> libc::c_int {
    // SAFETY: IPC_PRIVATE mints a fresh key.
    let id = unsafe { libc::shmget(libc::IPC_PRIVATE, 4096, libc::IPC_CREAT | 0o600) };
    assert!(id >= 0, "shmget: {}", std::io::Error::last_os_error());
    // SAFETY: our own segment id, never attached.
    unsafe { libc::shmctl(id, libc::IPC_RMID, std::ptr::null_mut()) };
    id
}

// C: sprintf(line, "%9lu %9lu", memKey, shmid) — right-aligned in width 9, so
// the line the parser must cope with carries leading spaces.
fn c_shmem_key_line(key: u64, id: libc::c_int) -> String {
    format!("{key:>9} {id:>9}")
}

/// Writes the eight-line postmaster.pid a C postmaster leaves behind.
fn write_c_pidfile(path: &str, encoded_pid: i32, datadir: &str, shmem_key: &str) {
    std::fs::write(
        path,
        format!("{encoded_pid}\n{datadir}\n1700000000\n5432\n/tmp\n*\n{shmem_key}\nready   \n"),
    )
    .unwrap();
}

/// A pid that is not ours, not our parent's, and reliably dead.
const DEAD_PID: i32 = 999_999_999;

fn first_contact_dir(tag: &str) -> String {
    let dir = scratch_dir(tag);
    init_small::globals::SetDataDir(&dir);
    init_small::globals::SetMyStartTime(1_700_000_000);
    dir
}

fn create_dd_lock_file(dir: &str) -> PgResult<()> {
    crate::lockfile::CreateLockFile(&format!("{dir}/postmaster.pid"), true, "", true, dir)
}

#[test]
fn c_pidfile_with_a_still_attached_segment_refuses_cleanly() {
    setup();
    let dir = first_contact_dir("cpid_attached");
    let path = format!("{dir}/postmaster.pid");
    let seg = CSegment::held_by(&dir);
    write_c_pidfile(&path, DEAD_PID, &dir, &c_shmem_key_line(5_432_001, seg.id));

    let refusal = refuses(|| create_dd_lock_file(&dir));

    assert!(
        !refusal.exit.contains("seam not installed"),
        "the interlock panicked instead of refusing: {refusal:?}"
    );
    assert_eq!(refusal.exit, "proc_exit(1)");
    assert_eq!(refusal.sqlstate, ERRCODE_LOCK_FILE_EXISTS);
    assert_eq!(
        refusal.message,
        format!(
            "pre-existing shared memory block (key 5432001, ID {}) is still in use",
            seg.id
        )
    );
    assert_eq!(
        refusal.hint.as_deref(),
        Some(
            format!("Terminate any old server processes associated with data directory \"{dir}\".")
                .as_str()
        )
    );
    // The refusal must leave the foreign pid file alone.
    assert!(std::fs::read_to_string(&path).unwrap().starts_with(&format!("{DEAD_PID}\n")));
}

#[test]
fn c_pidfile_with_a_vanished_segment_is_recycled() {
    setup();
    let dir = first_contact_dir("cpid_stale");
    let path = format!("{dir}/postmaster.pid");
    let gone = removed_segment_id();
    write_c_pidfile(&path, DEAD_PID, &dir, &c_shmem_key_line(5_432_002, gone));

    // Nobody's home: C unlinks and takes the data directory over.
    create_dd_lock_file(&dir).unwrap();
    assert!(std::fs::read_to_string(&path)
        .unwrap()
        .starts_with(&format!("{}\n{dir}\n", std::process::id())));
}

#[test]
fn c_pidfile_whose_segment_belongs_to_another_datadir_is_recycled() {
    setup();
    let other = scratch_dir("cpid_foreign_other");
    let seg = CSegment::held_by(&other);
    let dir = first_contact_dir("cpid_foreign");
    let path = format!("{dir}/postmaster.pid");
    write_c_pidfile(&path, DEAD_PID, &dir, &c_shmem_key_line(5_432_003, seg.id));

    // Accidental key matches are common; the device/inode test in the segment
    // header is what keeps one from blocking an unrelated cluster's startup.
    create_dd_lock_file(&dir).unwrap();
    assert!(std::fs::read_to_string(&path)
        .unwrap()
        .starts_with(&format!("{}\n", std::process::id())));
}

#[test]
fn c_pidfile_with_a_blank_shmem_key_line_is_recycled() {
    setup();
    let dir = first_contact_dir("cpid_blank");
    let path = format!("{dir}/postmaster.pid");
    // pgrust's own pid file, and C's before PGSharedMemoryCreate has run:
    // no key to probe, so C cannot treat it as an error.
    write_c_pidfile(&path, DEAD_PID, &dir, "");

    create_dd_lock_file(&dir).unwrap();
    assert!(std::fs::read_to_string(&path)
        .unwrap()
        .starts_with(&format!("{}\n", std::process::id())));
}

#[test]
fn a_live_owner_refuses_before_the_segment_is_ever_probed() {
    setup();
    let dir = first_contact_dir("cpid_live");
    let path = format!("{dir}/postmaster.pid");
    // A live process of our own uid: kill(pid, 0) succeeds. The key line names
    // a segment that no longer exists, so if the pid arm did not win first the
    // probe would recycle the directory out from under a running server.
    let mut child = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn a live process");
    let live = child.id() as i32;
    let gone = removed_segment_id();

    write_c_pidfile(&path, live, &dir, &c_shmem_key_line(5_432_004, gone));
    let refusal = refuses(|| create_dd_lock_file(&dir));
    assert_eq!(refusal.exit, "proc_exit(1)");
    assert_eq!(refusal.sqlstate, ERRCODE_LOCK_FILE_EXISTS);
    assert_eq!(refusal.message, format!("lock file \"{path}\" already exists"));
    assert_eq!(
        refusal.hint.as_deref(),
        Some(
            format!("Is another postmaster (PID {live}) running in data directory \"{dir}\"?")
                .as_str()
        )
    );

    // A standalone backend records its pid negated (pidfile.h line 1).
    write_c_pidfile(&path, -live, &dir, &c_shmem_key_line(5_432_004, gone));
    let refusal = refuses(|| create_dd_lock_file(&dir));
    assert_eq!(
        refusal.hint.as_deref(),
        Some(
            format!("Is another postgres (PID {live}) running in data directory \"{dir}\"?")
                .as_str()
        )
    );

    let _ = child.kill();
    let _ = child.wait();
}
