// M4 crash-recovery proof: real inserts+delete+commit over the real
// bufmgr/smgr/xloginsert/xact, datadir copied mid-run (pages unflushed, heap
// file truncated), then a child process boots the copy through the real
// StartupXLOG/PerformWalRecovery and verifies page bytes + MVCC visibility.
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use mcx::{Mcx, MemoryContext, PgVec};
use tableam_vocab::{TM_FailureData, TM_Result};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{
    SizeOfXLogRecord, XLogRecPtrToBytePos, DB_IN_PRODUCTION, MAXALIGN, RM_XLOG_ID,
    WAL_LEVEL_REPLICA, XLOG_CHECKPOINT_SHUTDOWN, XLP_LONG_HEADER,
};
use types_core::{
    BackendType, BlockNumber, ForkNumber, InvalidBlockNumber, Oid, XLogRecPtr, BLCKSZ,
    INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::bufpage::{PageMut, PageRef};
use types_storage::RelFileLocator;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, ItemPointerData, NameData,
    TupleDescData,
};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AACE;
const REL_OID: Oid = 61000;
const REL2_OID: Oid = 61001;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RLOC2: RelFileLocator = RelFileLocator::new(1663, 5, REL2_OID);

const CHILD_ENV: &str = "PGRUST_CRASH_RECOVERY_DD";

fn install_stub_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(779);
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
    aio_seams::pgaio_io_start_readv::set(|_, _, _| {});
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });

    // xact-engine periphery: owning units absent, end-of-xact state empty.
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
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| false);
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
}

// Every production init_seams this composition reaches; real machinery only.
fn install_real() {
    shmem::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    heapam_visibility::init_seams();
    clog::init_seams();
    subtrans::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    walsender_config::init_seams();
    twophase_config::init_seams();
    // max_locks_per_xact's home is the lock crate; its full init_seams
    // conflicts with this rig's heavyweight-lock stubs, so back the slot only.
    guc_tables::vars::max_locks_per_xact.install(guc_tables::GucVarAccessors {
        get: || 64,
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
    xlogrecovery::init_seams();
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
}

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
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

fn test_relation<'mcx>(mcx: Mcx<'mcx>) -> RelationData<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: REL_OID,
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
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: REL_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: REL_OID, dbId: 5 },
        },
        rd_rel,
        rd_att: int4_tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        rd_amcache: Default::default(),
        rd_supportinfo: Default::default(),
        rd_indexlist: Default::default(),
    }
}

const CKPT_LOC: XLogRecPtr = SEG as u64 + 40;
// SizeOfXLogRecord + short data header + sizeof(CheckPoint).
const CKPT_TOT_LEN: usize = SizeOfXLogRecord + 2 + controldata_utils::SIZEOF_CHECKPOINT;

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
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = CKPT_LOC;
    cf.checkPointCopy = *ckpt;
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
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

fn read_page_from_buffer(rel: &RelationData<'_>, blkno: BlockNumber) -> [u8; BLCKSZ] {
    let buf = bufmgr::ReadBuffer(rel, blkno).unwrap();
    let mut out = [0u8; BLCKSZ];
    // SAFETY: pinned page image, BLCKSZ bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(
            bufmgr::BufferGetPagePtr(buf).as_ptr(),
            out.as_mut_ptr(),
            BLCKSZ,
        )
    };
    bufmgr::ReleaseBuffer(buf).unwrap();
    out
}

// cmax is not WAL-logged: replay stamps FirstCommandId where the writer had
// its real command id (C heap_xlog_delete does the same), so the deleted
// tuple's t_cid word is excluded from the byte comparison.
fn normalize_page(page: &mut [u8]) {
    let r = unsafe { PageRef::from_raw(core::ptr::NonNull::new(page.as_mut_ptr()).unwrap()) };
    let lp = r.item_id(2);
    let off = lp.lp_off() as usize;
    page[off + 8..off + 12].fill(0);
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

fn mvcc_snapshot<'m>(mcx: Mcx<'m>) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    s.xmin = 10;
    s.xmax = 20;
    s.regd_count.set(1);
    s
}

fn page_tuple(page_addr: *mut u8, off: u16) -> HeapTupleData<'static> {
    // SAFETY: pinned buffer page, held across the visibility check.
    let page = unsafe { PageRef::from_raw(core::ptr::NonNull::new(page_addr).unwrap()) };
    let id = page.item_id(off);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page image under the caller's pin.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(0, off), REL_OID) }
}

fn fpi_source_page() -> [u8; BLCKSZ] {
    #[repr(align(8))]
    struct P([u8; BLCKSZ]);
    let mut p = P([0u8; BLCKSZ]);
    // SAFETY: aligned, exclusively owned stack page.
    let mut pm = unsafe { PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap()) };
    pm.init(0);
    pm.set_prune_xid(0xBEEF);
    p.0
}

// Child process body: crash recovery over the copied datadir.
#[test]
#[ignore]
fn crash_recovery_child() {
    let Ok(dd) = std::env::var(CHILD_ENV) else { return };
    let dd = std::path::PathBuf::from(dd);
    std::env::set_current_dir(&dd).unwrap();
    init_small::globals::SetDataDir(dd.to_str().unwrap());
    init_small::globals::set_enableFsync(true);

    install_stub_seams();
    install_real();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    // The whole real boot path: crash detection, SyncDataDirectory,
    // InitWalRecovery, PerformWalRecovery, FinishWalRecovery, and the
    // end-of-recovery checkpoint (no checkpointer installed => in-process).
    transam_xlog::StartupXLOG().unwrap();

    let cf = *transam_xlog::control_file::control_file();
    assert_eq!(cf.state, DB_IN_PRODUCTION);
    assert!(cf.checkPoint > CKPT_LOC, "end-of-recovery checkpoint advanced");

    // xact_redo committed xid 3 into the real clog; xid 4 never committed.
    assert!(transam::TransactionIdDidCommit(3).unwrap());
    assert!(!transam::TransactionIdDidCommit(4).unwrap());

    // MVCC visibility through the real stack: committed insert visible,
    // committed delete and the uncommitted (crashed) insert invisible.
    let ctx = MemoryContext::new("verify");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);
    let buf = bufmgr::ReadBuffer(&rel, 0).unwrap();
    let page_addr = bufmgr::BufferGetPagePtr(buf).as_ptr();
    let snap = mvcc_snapshot(mcx);
    let visible = |off: u16| {
        let mut t = page_tuple(page_addr, off);
        heapam_visibility_seams::heap_tuple_satisfies_visibility::call(&mut t, &snap, buf).unwrap()
    };
    assert!(visible(1), "committed insert (41) visible");
    assert!(!visible(2), "deleted tuple (42) invisible");
    assert!(visible(3), "committed insert (43) visible");
    assert!(!visible(4), "uncommitted insert (44) invisible");
    bufmgr::ReleaseBuffer(buf).unwrap();

    println!("CRASH_RECOVERY_CHILD_OK");
}

#[test]
fn crash_recovery_replays_dml_to_precrash_state() {
    if std::env::var(CHILD_ENV).is_ok() {
        return; // never recurse
    }
    let base = std::env::temp_dir().join(format!("pgrust_crashrec_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let dd1 = base.join("dd1");
    let dd2 = base.join("dd2");
    for sub in [
        "global",
        "pg_wal",
        "pg_wal/archive_status",
        "pg_wal/summaries",
        "pg_xact",
        "pg_subtrans",
        "base/5",
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

    let ctx = MemoryContext::new("crash_recovery");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);
    let tupdesc = int4_tupdesc(mcx);
    smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(
        types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
        ForkNumber::MAIN_FORKNUM,
        false,
    )
    .unwrap();

    // Transaction 1 (real xact): inserts 41,42,43 then delete (0,2), commit.
    xact::StartTransactionCommand().unwrap();
    let insert = |val: i32, cid: u32| {
        let mut tup =
            heaptuple::heap_form_tuple(mcx, &tupdesc, &[datum::Datum::from_i32(val)], &[false])
                .unwrap();
        heapam::heap_insert(&rel, tup.as_tuple_mut(), cid, 0).unwrap();
        tup.as_tuple().t_self
    };
    assert_eq!(insert(41, 0), ItemPointerData::new(0, 1));
    assert_eq!(insert(42, 0), ItemPointerData::new(0, 2));
    assert_eq!(insert(43, 0), ItemPointerData::new(0, 3));
    let xid1 = xact::GetTopTransactionIdIfAny();
    assert_eq!(xid1, 3, "first real xid from the checkpoint's nextXid");
    let mut tmfd = TM_FailureData::default();
    let r = heapam::heap_delete(
        &rel,
        &ItemPointerData::new(0, 2),
        1,
        None,
        true,
        &mut tmfd,
        false,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    xact::CommitTransactionCommand().unwrap();
    assert!(transam::TransactionIdDidCommit(xid1).unwrap());

    // Transaction 2: insert 44, never committed (lost in the crash).
    xact::StartTransactionCommand().unwrap();
    assert_eq!(insert(44, 0), ItemPointerData::new(0, 4));
    assert_eq!(xact::GetTopTransactionIdIfAny(), 4);

    // An XLOG_FPI for a second relation (xlog_redo's restore arm).
    let mut fpi_page = fpi_source_page();
    let fpi_lsn =
        xloginsert::log_newpage(&RLOC2, ForkNumber::MAIN_FORKNUM, 0, &mut fpi_page, true)
            .unwrap();

    let flush_to = fpi_lsn.max(transam_xlog_seams::xact_last_rec_end::call());
    transam_xlog::XLogFlush(flush_to).unwrap();

    // Pre-crash truth: the page as the buffer holds it (never flushed).
    let expected_page = read_page_from_buffer(&rel, 0);
    std::fs::write(base.join("expected_page.bin"), expected_page).unwrap();

    // Crash copy: heap pages live only in shared buffers; the truncate models
    // a crash that also lost the file extension.
    copy_dir(&dd1, &dd2);
    let heap_file = dd2.join("base/5").join(REL_OID.to_string());
    assert_eq!(std::fs::metadata(&heap_file).unwrap().len(), BLCKSZ as u64);
    let zeros = std::fs::read(&heap_file).unwrap();
    assert!(zeros.iter().all(|b| *b == 0), "heap page must not be flushed pre-crash");
    std::fs::File::options()
        .write(true)
        .open(&heap_file)
        .unwrap()
        .set_len(0)
        .unwrap();
    assert!(!dd2.join("base/5").join(REL2_OID.to_string()).exists());

    // Phase 2 in a fresh process (fresh shmem/TLS): the real recovery boot.
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_recovery_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(CHILD_ENV, dd2.to_str().unwrap())
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success() && text.contains("CRASH_RECOVERY_CHILD_OK"),
        "recovery child failed:\n{text}"
    );

    let mut replayed = std::fs::read(&heap_file).unwrap();
    assert_eq!(replayed.len(), BLCKSZ);
    let mut expected = expected_page.to_vec();
    normalize_page(&mut replayed);
    normalize_page(&mut expected);
    if replayed != expected {
        let first = replayed.iter().zip(&expected).position(|(a, b)| a != b).unwrap();
        panic!(
            "replayed page differs from pre-crash page at byte {first}: got {:02x?} want {:02x?}",
            &replayed[first..(first + 16).min(BLCKSZ)],
            &expected[first..(first + 16).min(BLCKSZ)]
        );
    }

    // The FPI-restored page: byte-equal to the logged image.
    let restored = std::fs::read(dd2.join("base/5").join(REL2_OID.to_string())).unwrap();
    assert_eq!(restored.len(), BLCKSZ);
    assert_eq!(restored, fpi_page.to_vec(), "FPI restore is byte-exact");

    let _ = std::fs::remove_dir_all(&base);
}
