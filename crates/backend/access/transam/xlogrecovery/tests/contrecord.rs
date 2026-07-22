// Aborted-contrecord recovery proof: WAL ends in a torn continuation record
// (crash lost the pages holding the record's tail). Cycle A boots the torn
// datadir through the real StartupXLOG, which must complete recovery and
// write XLOG_OVERWRITE_CONTRECORD as the first record of the missing-contrec
// page (XLP_FIRST_IS_OVERWRITE_CONTRECORD set). Cycle B models a second crash
// after that record but before the end-of-recovery checkpoint took effect
// (old pg_control restored): replay must skip the torn record, verify the
// overwrite record (xlogrecovery_redo), and finish.
//
// NATIVE ARM ONLY (std::fs fixture plumbing; see crash_recovery.rs note).
#![cfg(not(pgrust_sim))]

use std::sync::atomic::Ordering::Relaxed;

use mcx::PgVec;
use transam_xlog::{
    SizeOfXLogRecord, XLogRecPtrToBytePos, DB_IN_PRODUCTION, MAXALIGN, RM_XLOG_ID,
    WAL_LEVEL_REPLICA, XLOG_CHECKPOINT_SHUTDOWN, XLOG_OVERWRITE_CONTRECORD,
    XLP_FIRST_IS_OVERWRITE_CONTRECORD, XLP_LONG_HEADER,
};
use types_core::{BackendType, ForkNumber, InvalidBlockNumber, Oid, XLogRecPtr, BLCKSZ};
use types_storage::bufpage::PageMut;
use types_storage::RelFileLocator;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_ACED;
const REL_SMALL: Oid = 62000;
const REL_TORN: Oid = 62001;
const RLOC_SMALL: RelFileLocator = RelFileLocator::new(1663, 5, REL_SMALL);
const RLOC_TORN: RelFileLocator = RelFileLocator::new(1663, 5, REL_TORN);

const CKPT_LOC: XLogRecPtr = SEG as u64 + 40;
const CKPT_TOT_LEN: usize = SizeOfXLogRecord + 2 + controldata_utils::SIZEOF_CHECKPOINT;

const CHILD_ENV: &str = "PGRUST_CONTREC_DD";

fn install_stub_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(781);
    g::SetMyDatabaseId(5);
    g::SetNBuffers(128);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);

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
    miscinit_seams::get_user_id::set(|| 10);
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
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
    // No heavyweight lock table here, but the real fastpath VXID slot
    // must clear at end of xact.
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_release::set(|_, _, _| Ok(true));
    timeout_seams::disable_timeouts::set(|_| {});
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });

    // xact-engine periphery: owning units absent, end-of-xact state empty.
    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    timestamp_seams::timestamptz_to_str::set(|t| format!("<ts {t}>"));
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    // No shmem sinval segment in this rig (single backend).
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _for_commit| {
        Ok(PgVec::new_in(mcx))
    });
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    combocid_seams::at_eoxact_combocid::set(|| {});
    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    multixact_seams::at_eoxact_multixact::set(|| {});
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    relcache_seams::relation_cache_init_file_remove::set(|| {});
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    parallel_seams::is_parallel_worker::set(|| false);
    parallel_seams::at_eoxact_parallel::set(|_| Ok(()));
    origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    commit_ts_seams::transaction_tree_set_commit_ts_data::set(|_, _, _, _| Ok(()));
    commit_ts_seams::extend_commit_ts::set(|_| Ok(()));
    syncrep_seams::sync_rep_wait_for_lsn::set(|_, _| Ok(()));
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));
    predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
    freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(InvalidBlockNumber)
    });
    catalog_seams::is_catalog_relation::set(|_rel| false);
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
    // base/<db> exists in this rig; C's fn only mkdirs it.
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));

    // Startup-process hooks owned by postmaster_startup (absent here).
    startup_seams::begin_startup_progress_phase::set(|| {});
    postgres_seams::check_for_interrupts::set(|| Ok(()));
    startup_seams::process_startup_proc_interrupts::set(|| Ok(()));

    // WAL summarizer is absent here; the end-of-recovery checkpoint reaches
    // both. 0 = InvalidXLogRecPtr: no summarizer, KeepLogSeg skips its clamp.
    walsummarizer_seams::wakeup_wal_summarizer::set(|| {});
    walsummarizer_seams::get_oldest_unsummarized_lsn::set(|| Ok(0));
}

// The production init_seams this composition reaches; real machinery only.
fn install_real() {
    shmem::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    clog::init_seams();
    subtrans::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    walsender_config::init_seams();
    twophase_config::init_seams();
    guc_tables::vars::max_locks_per_xact.install(guc_tables::GucVarAccessors {
        get: || 64,
        set: |_| {},
    });
    guc_tables::vars::WalWriterFlushAfter.install(guc_tables::GucVarAccessors {
        get: || 128,
        set: |_| {},
    });
    snapmgr::init_seams();
    procarray::init_seams();
    inval::init_seams();
    pgstat::init_seams();
    relpath::init_seams();
    smgr::init_seams();
    sync::init_seams();
    xloginsert::init_seams();
    xlogreader::init_seams();
    xlogutils::init_seams();
    xlogprefetcher::init_seams();
    xlogprefetcher::XLogPrefetchShmemInit();
    // variable.rs owns this slot; its init_seams conflicts with this rig.
    guc_tables::vars::maintenance_io_concurrency.install(guc_tables::GucVarAccessors {
        get: || 10,
        set: |_| {},
    });
    xlogrecovery::init_seams();
    timeline::init_seams();
    guc::store::initialize_guc_options().unwrap();

    fd::init_seams();
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();
    clog::CLOGShmemInit().unwrap();
    subtrans::SUBTRANSShmemInit().unwrap();
    bufmgr::BufferManagerShmemInit().unwrap();
    bufmgr::init_seams();
    sync::InitSync().unwrap();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    if resowner::CurrentResourceOwner().is_null() {
        let owner = resowner::ResourceOwnerCreate(
            types_resowner::ResourceOwner::NULL,
            "contrecord-test",
        )
        .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
}

fn make_checkpoint() -> controldata_utils::CheckPoint {
    let mut ckpt = controldata_utils::CheckPoint::ZEROED;
    ckpt.redo = CKPT_LOC;
    ckpt.ThisTimeLineID = 1;
    ckpt.PrevTimeLineID = 1;
    ckpt.fullPageWrites = true;
    ckpt.wal_level = WAL_LEVEL_REPLICA;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    ckpt.oldestXid = 3;
    ckpt
}

fn write_control_file(dir: &std::path::Path, ckpt: &controldata_utils::CheckPoint) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = transam_xlog::control_file::PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = CKPT_LOC;
    cf.checkPointCopy = *ckpt;
    cf.unloggedLSN = transam_xlog::control_file::FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = transam_xlog::control_file::FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = transam_xlog::control_file::TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; transam_xlog::control_file::PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
}

fn write_segment_with_checkpoint(dir: &std::path::Path, ckpt: &controldata_utils::CheckPoint) {
    let segno = CKPT_LOC / SEG as u64;
    let page_addr = CKPT_LOC - CKPT_LOC % 8192;
    let mut seg = vec![0u8; SEG as usize];
    seg[0..2].copy_from_slice(&0xD118u16.to_ne_bytes());
    seg[2..4].copy_from_slice(&XLP_LONG_HEADER.to_ne_bytes());
    seg[4..8].copy_from_slice(&1u32.to_ne_bytes());
    seg[8..16].copy_from_slice(&page_addr.to_ne_bytes());
    seg[24..32].copy_from_slice(&SYS_ID.to_ne_bytes());
    seg[32..36].copy_from_slice(&(SEG as u32).to_ne_bytes());
    seg[36..40].copy_from_slice(&8192u32.to_ne_bytes());

    let mut rec = vec![0u8; CKPT_TOT_LEN];
    rec[0..4].copy_from_slice(&(CKPT_TOT_LEN as u32).to_ne_bytes());
    rec[8..16].copy_from_slice(&(CKPT_LOC - 0x28).to_ne_bytes());
    rec[16] = XLOG_CHECKPOINT_SHUTDOWN;
    rec[17] = RM_XLOG_ID;
    rec[24] = 255; // XLR_BLOCK_ID_DATA_SHORT
    rec[25] = controldata_utils::SIZEOF_CHECKPOINT as u8;
    rec[26..26 + controldata_utils::SIZEOF_CHECKPOINT].copy_from_slice(&ckpt.to_bytes());
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
        &rec[..20],
    ));
    rec[20..24].copy_from_slice(&crc.to_ne_bytes());

    let off = (CKPT_LOC % SEG as u64) as usize;
    seg[off..off + rec.len()].copy_from_slice(&rec);
    let name = transam_xlog::XLogFileName(1, segno, SEG);
    std::fs::write(dir.join("pg_wal").join(name), &seg).unwrap();
}

fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let to = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).unwrap();
        }
    }
}

fn run_child(dd: &std::path::Path) -> (bool, String) {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "contrecord_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(CHILD_ENV, dd.to_str().unwrap())
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success() && text.contains("CONTRECORD_CHILD_OK"), text)
}

// Child process body: the real recovery boot over $PGRUST_CONTREC_DD.
#[test]
#[ignore]
fn contrecord_child() {
    let Ok(dd) = std::env::var(CHILD_ENV) else { return };
    let dd = std::path::PathBuf::from(dd);
    std::env::set_current_dir(&dd).unwrap();
    init_small::globals::SetDataDir(dd.to_str().unwrap());
    init_small::globals::set_enableFsync(true);

    install_stub_seams();
    install_real();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    transam_xlog::StartupXLOG().unwrap();

    let cf = *transam_xlog::control_file::control_file();
    assert_eq!(cf.state, DB_IN_PRODUCTION);
    assert!(cf.checkPoint > CKPT_LOC, "end-of-recovery checkpoint advanced");

    println!("CONTRECORD_CHILD_OK");
}

#[test]
fn overwrite_contrecord_round_trip() {
    if std::env::var(CHILD_ENV).is_ok() {
        return; // never recurse
    }
    let base = std::env::temp_dir().join(format!("pgrust_contrec_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dd1 = base.join("dd1");
    let dd2 = base.join("dd2");
    let dd3 = base.join("dd3");
    for sub in [
        "global",
        "pg_wal",
        "pg_wal/archive_status",
        "pg_wal/summaries",
        "pg_xact",
        "pg_subtrans",
        "base/5",
        // StartupXLOG opens pg_tblspc at ERROR; real initdb always creates it.
        "pg_tblspc",
    ] {
        std::fs::create_dir_all(dd1.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dd1).unwrap();
    init_small::globals::SetDataDir(dd1.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_stub_seams();
    install_real();

    let ckpt = make_checkpoint();
    write_control_file(&dd1, &ckpt);
    write_segment_with_checkpoint(&dd1, &ckpt);
    clog::BootStrapCLOG().unwrap();
    subtrans::BootStrapSUBTRANS().unwrap();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    let end_of_log: XLogRecPtr = CKPT_LOC + MAXALIGN(CKPT_TOT_LEN) as u64;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(CKPT_LOC), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(CKPT_LOC, Relaxed);
    ctl.RedoRecPtr.store(CKPT_LOC, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState
        .store(transam_xlog::RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    // StartupXLOG's partial-tail setup for a mid-page insert position.
    {
        let page_begin = end_of_log - end_of_log % 8192;
        let idx = transam_xlog::ctl::XLogRecPtrToBufIdx(end_of_log) as usize;
        let seg_bytes = std::fs::read(
            dd1.join("pg_wal").join(transam_xlog::XLogFileName(1, CKPT_LOC / SEG as u64, SEG)),
        )
        .unwrap();
        let off = (page_begin % SEG as u64) as usize;
        let len = (end_of_log - page_begin) as usize;
        let dst = ctl.page_ptr(idx);
        // SAFETY: single-threaded rig; ctl page buffers are XLOG_BLCKSZ.
        unsafe {
            core::ptr::copy_nonoverlapping(seg_bytes[off..].as_ptr(), dst, len);
            core::ptr::write_bytes(dst.add(len), 0, 8192 - len);
        }
        ctl.xlblocks[idx].store(page_begin + 8192, std::sync::atomic::Ordering::Release);
        ctl.InitializedUpTo.store(page_begin + 8192, Relaxed);
    }
    xlogutils::set_in_recovery(false);
    procarray::TransamVariables()
        .nextXid
        .store(types_core::FullTransactionId::from_epoch_and_xid(0, 3).value, Relaxed);
    subtrans::StartupSUBTRANS(3).unwrap();
    assert!(transam_xlog::XLogInsertAllowed());

    // A complete small record recovery must replay (hole-compressed FPI),
    // then the record the crash tears: a full-page FPI (page_std=false =>
    // no hole removal; tot_len > XLOG_BLCKSZ guarantees a continuation).
    let mut small_page = init_page();
    xloginsert::log_newpage(&RLOC_SMALL, ForkNumber::MAIN_FORKNUM, 0, &mut small_page, true)
        .unwrap();

    let mut torn_page = full_page();
    let end_lsn =
        xloginsert::log_newpage(&RLOC_TORN, ForkNumber::MAIN_FORKNUM, 0, &mut torn_page, false)
            .unwrap();
    let start_lsn = transam_xlog::ProcLastRecPtr();
    transam_xlog::XLogFlush(end_lsn).unwrap();

    // The torn record must cross at least one WAL page boundary.
    let missing_contrec = start_lsn - start_lsn % 8192 + 8192;
    assert!(
        end_lsn > missing_contrec,
        "torn record does not cross a page boundary (start {start_lsn:#x} end {end_lsn:#x})"
    );

    // Crash copy with the continuation pages lost: zero the segment from the
    // first page boundary after the torn record's start.
    copy_dir(&dd1, &dd2);
    let segname = transam_xlog::XLogFileName(1, CKPT_LOC / SEG as u64, SEG);
    let segpath = dd2.join("pg_wal").join(&segname);
    let mut seg = std::fs::read(&segpath).unwrap();
    let zero_from = (missing_contrec % SEG as u64) as usize;
    seg[zero_from..].fill(0);
    std::fs::write(&segpath, &seg).unwrap();

    let pre_recovery_control = std::fs::read(dd2.join("global/pg_control")).unwrap();

    // Cycle A: recovery over the torn WAL must complete and write the
    // overwrite record (unported => this child panics at StartupXLOG).
    let (ok, text) = run_child(&dd2);
    assert!(ok, "cycle-A recovery child failed:\n{text}");

    // The datadir's WAL now carries C's shape: XLP_FIRST_IS_OVERWRITE_CONTRECORD
    // on the missing-contrec page, and XLOG_OVERWRITE_CONTRECORD as its first
    // record naming the torn record's start LSN.
    let seg = std::fs::read(dd2.join("pg_wal").join(&segname)).unwrap();
    let page_off = (missing_contrec % SEG as u64) as usize;
    let xlp_info = u16::from_ne_bytes(seg[page_off + 2..page_off + 4].try_into().unwrap());
    assert!(
        xlp_info & XLP_FIRST_IS_OVERWRITE_CONTRECORD != 0,
        "page header must carry XLP_FIRST_IS_OVERWRITE_CONTRECORD (xlp_info={xlp_info:#x})"
    );
    let rec_off = page_off + transam_xlog::SizeOfXLogShortPHD;
    // xl_tot_len = SizeOfXLogRecord + short data header + 16-byte payload.
    let tot_len = u32::from_ne_bytes(seg[rec_off..rec_off + 4].try_into().unwrap());
    assert_eq!(tot_len as usize, SizeOfXLogRecord + 2 + 16);
    assert_eq!(seg[rec_off + 16], XLOG_OVERWRITE_CONTRECORD, "xl_info");
    assert_eq!(seg[rec_off + 17], RM_XLOG_ID, "xl_rmid");
    let payload = rec_off + SizeOfXLogRecord + 2;
    let overwritten_lsn =
        u64::from_ne_bytes(seg[payload..payload + 8].try_into().unwrap());
    assert_eq!(overwritten_lsn, start_lsn, "overwritten_lsn names the torn record");
    let overwrite_time =
        i64::from_ne_bytes(seg[payload + 8..payload + 16].try_into().unwrap());
    assert_eq!(overwrite_time, 777_000_000, "stubbed GetCurrentTimestamp");

    // Cycle B: crash again after the overwrite record but before the
    // end-of-recovery checkpoint took effect (old pg_control restored).
    // Replay now reads the torn record, skips it via the page flag, and the
    // XLOG_OVERWRITE_CONTRECORD redo arm verifies the skipped LSN.
    copy_dir(&dd2, &dd3);
    std::fs::write(dd3.join("global/pg_control"), &pre_recovery_control).unwrap();
    let (ok, text) = run_child(&dd3);
    assert!(ok, "cycle-B recovery child failed:\n{text}");
    let expect = format!(
        "successfully skipped missing contrecord at {:X}/{:X}, overwritten at ",
        start_lsn >> 32,
        start_lsn as u32
    );
    assert!(
        text.contains(&expect),
        "cycle-B log must carry the skip message ({expect:?}):\n{text}"
    );

    let _ = std::fs::remove_dir_all(&base);
}

fn init_page() -> [u8; BLCKSZ] {
    #[repr(align(8))]
    struct P([u8; BLCKSZ]);
    let mut p = P([0u8; BLCKSZ]);
    // SAFETY: aligned, exclusively owned stack page.
    let mut pm = unsafe { PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap()) };
    pm.init(0);
    p.0
}

// A fully non-zero page: with page_std=false log_newpage keeps all 8192
// bytes, so the record always spans a WAL page boundary.
fn full_page() -> [u8; BLCKSZ] {
    let mut p = init_page();
    for b in p[24..].iter_mut() {
        *b = 0xA5;
    }
    p
}
