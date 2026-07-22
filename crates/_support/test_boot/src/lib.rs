// Test-only full WAL boot (insert_select.rs's recipe, extracted): real
// control file, XLOGShmemInit, LWLocks, PGPROC — enough for code under test
// to run heap/index WAL inserts through the real XLogInsert. Process state is
// booted once; thread_local backend state (globals, vfd cache, MyProc) is
// booted once per calling thread, so multi-threaded test harnesses work.
// Stub seams are install-if-absent: a test's own stubs win if installed first.
use std::path::PathBuf;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Once, OnceLock};

use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{BackendType, XLogRecPtr};

pub const WAL_SEG_SIZE: i32 = 16 * 1024 * 1024;
pub const SYS_ID: u64 = 0x5544_3322_1100_AACD;
pub const END_OF_LOG: XLogRecPtr = 2 * WAL_SEG_SIZE as u64;

static DATA_DIR: OnceLock<PathBuf> = OnceLock::new();
static PROCESS: Once = Once::new();

macro_rules! stub {
    ($seam:path, $imp:expr) => {{
        use $seam as __s;
        if !__s::is_installed() {
            __s::set($imp);
        }
    }};
}

pub fn data_dir() -> &'static PathBuf {
    DATA_DIR.get().expect("boot_wal not called")
}

/// Idempotent per process and per thread. Call it before the first WAL-driven
/// operation on every thread that performs one; test-specific seam stubs must
/// be installed before the first call.
pub fn boot_wal(tag: &str) {
    let dir = DATA_DIR.get_or_init(|| {
        std::env::temp_dir().join(format!("pgrust_boot_{tag}_{}", std::process::id()))
    });
    thread_globals(dir.to_str().unwrap());
    PROCESS.call_once(|| process_boot(dir));
    thread_proc();
}

fn thread_globals(dir: &'static str) {
    use init_small::globals as g;
    std::thread_local! {
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    ARMED.with(|armed| {
        if armed.get() {
            return;
        }
        g::SetMaxConnections(64);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(64 + 3 + 2 + 2 + 2);
        g::SetMyProcPid(779);
        g::SetMyDatabaseId(5);
        g::set_transaction_buffers(64);
        g::set_subtransaction_buffers(64);
        g::SetDataDir(dir);
        g::set_enableFsync(false);
        armed.set(true);
    });
}

fn thread_proc() {
    std::thread_local! {
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    ARMED.with(|armed| {
        if armed.get() {
            return;
        }
        fd::InitFileAccess();
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
        armed.set(true);
    });
}

fn process_boot(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
    for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(dir).unwrap();

    install_stub_seams();
    shmem::init_seams();
    fd::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    xlogutils::init_seams();
    lmgr_proc::init_seams();
    guc::store::initialize_guc_options().unwrap();
    // Pre-boot tests ran with a `standby_info_active == false` stub; wal_level
    // minimal keeps that behavior with the real seam installed.
    guc_tables::vars::wal_level.write(transam_xlog::WAL_LEVEL_MINIMAL);

    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });

    write_control_file(dir);
    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    let prev_rec: XLogRecPtr = WAL_SEG_SIZE as u64 + 40;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(END_OF_LOG), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(prev_rec), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.InitializedUpTo.store(END_OF_LOG, Relaxed);
    ctl.logInsertResult.store(END_OF_LOG, Relaxed);
    ctl.logWriteResult.store(END_OF_LOG, Relaxed);
    ctl.logFlushResult.store(END_OF_LOG, Relaxed);
    ctl.LogwrtRqstWrite.store(END_OF_LOG, Relaxed);
    ctl.LogwrtRqstFlush.store(END_OF_LOG, Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    xlogutils::set_in_recovery(false);
    assert!(transam_xlog::XLogInsertAllowed());
}

fn install_stub_seams() {
    stub!(pg_sema_seams::pg_semaphore_create, |_| {});
    stub!(pg_sema_seams::pg_semaphore_reset, |_| {});
    stub!(pg_sema_seams::pg_semaphore_lock, |_| {});
    stub!(pg_sema_seams::pg_semaphore_unlock, |_| {});
    stub!(s_lock_seams::perform_spin_delay, |_| std::thread::yield_now());
    stub!(s_lock_seams::finish_spin_delay, |_| {});
    stub!(s_lock_seams::set_spins_per_delay, |_| {});
    stub!(s_lock_seams::update_spins_per_delay, |v| v);
    stub!(latch_seams::own_latch, |_| {});
    stub!(latch_seams::disown_latch, |_| {});
    stub!(latch_seams::set_latch, |_| {});
    stub!(latch_seams::set_latch_my_latch, || {});
    stub!(latch_seams::wait_latch_my_latch, |_, _, _| 0);
    stub!(latch_seams::reset_latch_my_latch, || {});
    stub!(miscinit_seams::switch_to_shared_latch, || {});
    stub!(miscinit_seams::switch_back_to_local_latch, || {});
    stub!(miscinit_seams::get_user_id, || 10);
    stub!(miscinit_seams::is_bootstrap_processing_mode, || false);
    stub!(waitevent_seams::pgstat_set_wait_event_storage, |_| {});
    stub!(waitevent_seams::pgstat_report_wait_start, |_| {});
    stub!(waitevent_seams::pgstat_report_wait_end, || {});
    stub!(waitevent_seams::pgstat_reset_wait_event_storage, || {});
    stub!(ipc_seams::on_shmem_exit, |_, _| {});
    stub!(deadlock_seams::init_dead_lock_checking, || Ok(()));
    stub!(pmsignal_seams::register_postmaster_child_active, || {});
    stub!(syncrep_seams::sync_rep_cleanup_at_proc_exit, || {});
    stub!(condition_variable_seams::condition_variable_cancel_sleep, || false);
    stub!(autovacuum_seams::wake_autovacuum_launcher, || {});
    stub!(lock_seams::abort_strong_lock_acquire, || {});
    stub!(lock_seams::get_awaited_lock_hashcode, || None);
    stub!(lock_seams::lock_release_all, |_, _| Ok(()));
    stub!(lock_seams::lock_acquire_extended, |_, _, _, _, _, _| Ok(
        types_storage::lock::LOCKACQUIRE_OK
    ));
    stub!(timeout_seams::disable_timeouts, |_| {});
    stub!(aio_seams::pgaio_closing_fd, |_| {});
    stub!(aio_seams::pgaio_io_start_readv, |_, _, _| Ok(()));
    stub!(aio_seams::at_eoxact_aio, |_| {});
    stub!(aio_seams::pgaio_error_cleanup, || {});
    stub!(sync_seams::register_sync_request, |_, _, _| Ok(true));
    stub!(sinval_seams::receive_shared_invalid_messages, |_, _| Ok(()));
    stub!(xact_seams::mark_current_transaction_id_logged_if_any, || {});
    stub!(xact_seams::mark_subxact_top_xid_logged, || {});
    stub!(xact_seams::is_in_parallel_mode, || false);
    stub!(xact_seams::get_current_transaction_nest_level, || 1);
    stub!(timestamp_seams::get_current_timestamp, || 777_000_000);
    stub!(postgres_seams::check_for_interrupts, || Ok(()));
}

fn write_control_file(dir: &std::path::Path) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = WAL_SEG_SIZE as u64 + 40;
    cf.checkPointCopy.redo = WAL_SEG_SIZE as u64 + 40;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = WAL_SEG_SIZE as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
}
