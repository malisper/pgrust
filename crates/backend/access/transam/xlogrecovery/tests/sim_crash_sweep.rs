// DST P4 inc-2 — the PRODUCT-SHAPED crash-recovery property sweep, sim-cfg
// only. inc-1's own verdict named its harness-transcribed recovery step the
// biggest fidelity gap; this file closes it:
//
//   * the datadir is minted INSIDE the SimVfs namespace (product
//     controldata/WAL-segment builders + the product's own BootStrapCLOG/
//     BootStrapSUBTRANS writing through fd -> vfs -> SimVfs);
//   * the workload drives WAL through the PRODUCT paths — heap_insert /
//     CommitTransactionCommand (RecordTransactionCommit -> XLogFlush ->
//     issue_xlog_fsync) + a mid-run product CreateCheckPoint — over the
//     same fd/vfs data plane the server uses;
//   * the CUT is the P4 fault model's crash-image primitive
//     (FaultRule::crash_at_op swept over every workload op, seeded-subset
//     survivor images per point);
//   * recovery is THE PRODUCT'S StartupXLOG (InitWalRecovery /
//     PerformWalRecovery / end-of-recovery checkpoint) booted over the
//     post-crash image inside a fresh sim universe.
//
// C provenance for "committed" at each cut class: xact.c's
// RecordTransactionCommit — a transaction is durably committed iff its
// commit record is flushed to WAL (XLogFlush returned; wal_sync_method
// fdatasync under sim — the O_DSYNC open_datasync arm is not modeled by
// SimVfs, same law as the wasm lane's harness). An ACK to the client
// happens only after that flush, so at every cut: every acked txn must be
// clog-committed and MVCC-visible after StartupXLOG; unacked txns may be
// either (their record may or may not have made the flush) but the visible
// set must equal EXACTLY the clog-committed prefix (internal consistency;
// no torn/garbage tuple applies).
//
// Process shape (fresh statics per boot, the M4 crash_recovery.rs pattern):
// each sweep point spawns a WRITER child (mint + workload + cut + pack the
// post-crash sim image to a real-fs dir — the cp -RL/pack shape from the
// wasm lanes) and a RECOVER child (import the pack into a fresh sim
// universe, boot the product recovery, verify the properties). The pack
// import helper doubles as the real-initdb-datadir importer (see
// initdb_datadir_boots_under_sim below).
//
// Run with: RUSTFLAGS='--cfg pgrust_sim' cargo test -p xlogrecovery --test sim_crash_sweep
#![cfg(pgrust_sim)]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use mcx::{Mcx, MemoryContext, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{
    SizeOfXLogRecord, XLogRecPtrToBytePos, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE,
    CHECKPOINT_WAIT, DB_IN_PRODUCTION, MAXALIGN, RM_XLOG_ID, WAL_LEVEL_REPLICA,
    XLOG_CHECKPOINT_SHUTDOWN, XLP_LONG_HEADER,
};
use types_core::{
    BackendType, ForkNumber, InvalidBlockNumber, Oid, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, RelationData, LOCKMODE, RELKIND_RELATION,
};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::RelFileLocator;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, ItemPointerData, NameData,
    TupleDescData,
};

use vfs::sim::{CrashImage, FaultRule, SeededFaultPlan, SimVfs};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5EED_FA17_0002;
const REL_OID: Oid = 61000;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const CKPT_LOC: XLogRecPtr = SEG as u64 + 40;
const CKPT_TOT_LEN: usize = SizeOfXLogRecord + 2 + controldata_utils::SIZEOF_CHECKPOINT;

/// Transactions per workload run; txn t inserts int4 value t and commits.
/// First real xid is 3 (the minted checkpoint's nextXid), so txn t <-> xid
/// t+2. A product CreateCheckPoint runs after txn CKPT_AFTER.
const TXNS: u32 = 6;
const CKPT_AFTER: u32 = 3;

const SEED: u64 = 0x5EED_FA17_0002;

const ROLE_ENV: &str = "PGRUST_SIM_SWEEP_ROLE";
const K_ENV: &str = "PGRUST_SIM_SWEEP_K";
const PACK_ENV: &str = "PGRUST_SIM_SWEEP_PACK";
const RED_ENV: &str = "PGRUST_SIM_SWEEP_RED";
const INITDB_ENV: &str = "PGRUST_SIM_SWEEP_INITDB_DD";

fn per_point_seed(k: u64) -> u64 {
    SEED ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// ---------------------------------------------------------------------------
// sim-side fs helpers (all through the active vfs = SimVfs under this cfg)
// ---------------------------------------------------------------------------

fn cpath(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path).unwrap()
}

fn vfs_mkdir_p(path: &str) {
    let mut prefix = String::new();
    for comp in path.split('/') {
        if comp.is_empty() {
            continue;
        }
        prefix.push('/');
        prefix.push_str(comp);
        let rc = vfs::mkdir(&cpath(&prefix), 0o700);
        assert!(
            rc == 0 || vfs::get_errno() == libc::EEXIST,
            "vfs_mkdir_p({prefix}): errno {}",
            vfs::get_errno()
        );
    }
}

fn vfs_write_file(path: &str, data: &[u8]) {
    let fd = vfs::open(&cpath(path), libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o600);
    assert!(fd >= 0, "vfs_write_file open({path}): errno {}", vfs::get_errno());
    if !data.is_empty() {
        assert_eq!(vfs::pwrite(fd, data, 0), data.len() as isize, "{path}");
    }
    assert_eq!(vfs::close(fd), 0);
}

fn vfs_read_range(path: &str, off: i64, len: usize) -> Vec<u8> {
    let fd = vfs::open(&cpath(path), libc::O_RDONLY, 0);
    assert!(fd >= 0, "vfs_read_range open({path}): errno {}", vfs::get_errno());
    let mut buf = vec![0u8; len];
    let n = vfs::pread(fd, &mut buf, off);
    assert!(n >= 0, "{path}");
    buf.truncate(n as usize);
    assert_eq!(vfs::close(fd), 0);
    buf
}

/// Make the WHOLE current sim tree durable (files fold their journals, dirs
/// promote their entry images) — the mint is the pre-history the sweep never
/// cuts into, exactly like inc-1's bootstrap() discipline. Raw vfs fsyncs:
/// deliberately NOT pg_fsync, so the red arm's enableFsync=false cannot
/// weaken the mint.
fn sim_fsync_tree() {
    for (path, entry) in SimVfs::new().image_dump() {
        let p = path.to_str().unwrap().to_string();
        let fd = vfs::open(&cpath(&p), libc::O_RDONLY, 0);
        assert!(fd >= 0, "fsync_tree open({p}): errno {}", vfs::get_errno());
        assert_eq!(vfs::fsync(fd), 0, "fsync_tree fsync({p})");
        assert_eq!(vfs::close(fd), 0);
        let _ = entry;
    }
}

/// Export the sim tree's DURABLE images to a real-fs directory (the pack).
/// Harness-side plumbing: the pack crosses the process boundary between the
/// writer's post-cut universe and the recover child's fresh one.
fn export_sim_tree(dst: &std::path::Path) {
    for (path, entry) in SimVfs::new().image_dump() {
        let rel = path.strip_prefix("/").unwrap();
        let out = dst.join(rel);
        match entry {
            None => std::fs::create_dir_all(&out).unwrap(),
            Some((_volatile, durable)) => {
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&out, &durable).unwrap();
            }
        }
    }
}

/// Import a real-fs directory tree into the sim namespace at `sim_base`
/// ("/" = the sim root). The cp -RL shape from the wasm lanes: symlinks are
/// dereferenced (std::fs::metadata follows), entries imported in sorted
/// order (determinism). This is ALSO the real-initdb-datadir importer.
fn import_tree_into_sim(src: &std::path::Path, sim_base: &str) {
    let mut entries: Vec<_> = std::fs::read_dir(src)
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e.file_name().into_string().unwrap();
        let sim_path = if sim_base == "/" {
            format!("/{name}")
        } else {
            format!("{sim_base}/{name}")
        };
        let meta = std::fs::metadata(e.path()).unwrap(); // follows symlinks
        if meta.is_dir() {
            vfs_mkdir_p(&sim_path);
            import_tree_into_sim(&e.path(), &sim_path);
        } else {
            vfs_write_file(&sim_path, &std::fs::read(e.path()).unwrap());
        }
    }
}

// ---------------------------------------------------------------------------
// the rig (M4 crash_recovery.rs shape, vfs-ified): stub seams + real units
// ---------------------------------------------------------------------------

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
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));

    startup_seams::begin_startup_progress_phase::set(|| {});
    postgres_seams::check_for_interrupts::set(|| Ok(()));
    startup_seams::process_startup_proc_interrupts::set(|| Ok(()));

    walsummarizer_seams::wakeup_wal_summarizer::set(|| {});
    walsummarizer_seams::get_oldest_unsummarized_lsn::set(|| Ok(0));

    // Mid-run CreateCheckPoint (wal_level=replica) logs a running-xacts
    // snapshot for hot standby; the standby unit is absent in this rig and
    // crash recovery does not need the record. CreateCheckPoint ignores the
    // returned LSN.
    standby_seams::log_standby_snapshot::set(|| Ok(0));
}

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
    guc_tables::vars::maintenance_io_concurrency.install(guc_tables::GucVarAccessors {
        get: || 10,
        set: |_| {},
    });
    xlogrecovery::init_seams();
    timeline::init_seams();
    guc::store::initialize_guc_options().unwrap();
    // SIM HARNESS LAW (same as the wasm lane's wal_sync_method trap): an
    // O_DSYNC-style open_datasync arm would fold the WAL durability point
    // into an open flag SimVfs does not model — pin fdatasync (the port's
    // default, stamped explicitly through the product's own assign path)
    // so every commit's durability is an explicit vfs fdatasync the fault
    // model can see, gate and journal.
    transam_xlog::stamp_wal_sync_method(transam_xlog::WAL_SYNC_METHOD_FDATASYNC);

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
            "sim-crash-sweep",
        )
        .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
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

fn test_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> RelationData<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: oid,
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
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
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
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
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

/// The control-file image, built by the PRODUCT's controldata layout code,
/// written into the sim namespace.
fn mint_control_file(ckpt: &controldata_utils::CheckPoint) {
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
    vfs_write_file("/global/pg_control", &image);
}

fn mint_wal_segment(ckpt: &controldata_utils::CheckPoint) {
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
    vfs_write_file(&format!("/pg_wal/{name}"), &seg);
}

/// Mint the datadir inside the sim namespace and boot the WRITER rig
/// (insert-capable XLogCtl, StartupXLOG skipped — the M4 writer shape).
/// Everything here happens BEFORE the fault plan installs: it is the
/// durable pre-history at every sweep point.
fn mint_and_boot_writer() {
    for d in [
        "/global",
        "/pg_wal",
        "/pg_wal/archive_status",
        "/pg_wal/summaries",
        "/pg_xact",
        "/pg_subtrans",
        "/base/5",
        "/pg_tblspc",
    ] {
        vfs_mkdir_p(d);
    }
    init_small::globals::SetDataDir("/");
    init_small::globals::set_enableFsync(true);

    install_stub_seams();
    install_real();

    let ckpt = make_checkpoint();
    mint_control_file(&ckpt);
    mint_wal_segment(&ckpt);
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
    {
        let page_begin = end_of_log - end_of_log % 8192;
        let idx = transam_xlog::ctl::XLogRecPtrToBufIdx(end_of_log) as usize;
        let name = transam_xlog::XLogFileName(1, CKPT_LOC / SEG as u64, SEG);
        let off = (page_begin % SEG as u64) as i64;
        let len = (end_of_log - page_begin) as usize;
        let tail = vfs_read_range(&format!("/pg_wal/{name}"), off, len);
        assert_eq!(tail.len(), len);
        let dst = ctl.page_ptr(idx);
        // SAFETY: single-threaded rig; ctl page buffers are XLOG_BLCKSZ.
        unsafe {
            core::ptr::copy_nonoverlapping(tail.as_ptr(), dst, len);
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

    // The heap relation file (initdb-side artifact: exists before the sweep).
    smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(
        types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
        ForkNumber::MAIN_FORKNUM,
        false,
    )
    .unwrap();

    // The mint is durable pre-history: fold every journal, promote every dir.
    sim_fsync_tree();
}

// ---------------------------------------------------------------------------
// WRITER child: workload through product paths, cut, pack the crash image
// ---------------------------------------------------------------------------

/// One committed transaction through the product commit protocol. Any Err is
/// the engine stopping (the PANIC-equivalent for a correct engine).
fn run_one_txn<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    t: u32,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    let mut tup = heaptuple::heap_form_tuple(
        mcx,
        tupdesc,
        &[datum::Datum::from_i32(t as i32)],
        &[false],
    )?;
    heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
    xact::CommitTransactionCommand()?;
    Ok(())
}

#[test]
#[ignore]
fn sim_sweep_writer_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("writer") {
        return;
    }
    let k: u64 = std::env::var(K_ENV).unwrap().parse().unwrap();
    let pack = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());
    let red = std::env::var(RED_ENV).as_deref() == Ok("1");

    SimVfs::reset();
    mint_and_boot_writer();

    if red {
        // PRODUCT-SHAPED RED ARM: disable the product's fsync layer through
        // its own knob (fd::pg_fsync and issue_xlog_fsync both consult
        // enableFsync). Every commit will claim durability it never bought.
        init_small::globals::set_enableFsync(false);
        SimVfs::set_crash_image(CrashImage::DropAll);
    } else if k > 0 {
        SeededFaultPlan::install(per_point_seed(k), vec![FaultRule::crash_at_op(k)]);
    }

    let ops_at_start = SimVfs::op_seq();

    let ctx = MemoryContext::new("sim_sweep_writer");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx, REL_OID);
    let tupdesc = int4_tupdesc(mcx);

    let mut acked: u32 = 0;
    let mut stopped: Option<String> = None;
    for t in 1..=TXNS {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_one_txn(mcx, &rel, &tupdesc, t)
        }));
        match r {
            Ok(Ok(())) => {
                // Commit returned success to the "client": ACKED — even if a
                // cut fired inside the call. If the record is not durable the
                // property harness flags it (that would be a real finding).
                acked += 1;
            }
            Ok(Err(e)) => {
                stopped = Some(format!("txn {t} error: {e:?}"));
                break;
            }
            Err(_) => {
                stopped = Some(format!("txn {t} panicked (engine stop)"));
                break;
            }
        }
        if SimVfs::cut_count() > 0 {
            break; // power is gone; do not touch the image any further
        }
        if t == CKPT_AFTER {
            // The PRODUCT's checkpoint: CheckPointGuts (clog/subtrans/buffer
            // flushes + sync requests) and the in-place control update. The
            // checkpointer's buffer pins need a live owner (C: the aux
            // process resource owner); commits null the current one out.
            if resowner::CurrentResourceOwner().is_null() {
                let owner = resowner::ResourceOwnerCreate(
                    types_resowner::ResourceOwner::NULL,
                    "sim-sweep-ckpt",
                )
                .unwrap();
                resowner::SetCurrentResourceOwner(owner);
            }
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                transam_xlog::CreateCheckPoint(
                    CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT,
                )
            }));
            match r {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    stopped = Some(format!("checkpoint after txn {t} error: {e:?}"));
                    break;
                }
                Err(_) => {
                    stopped = Some(format!("checkpoint after txn {t} panicked (engine stop)"));
                    break;
                }
            }
            if SimVfs::cut_count() > 0 {
                break;
            }
        }
    }

    let ops_used = SimVfs::op_seq() - ops_at_start;
    if red && SimVfs::cut_count() == 0 {
        // Power loss after the "successful" run: the red arm's cut.
        SimVfs::cut();
    }

    // Pack the post-crash image (durable == volatile after a cut) plus meta.
    let _ = std::fs::remove_dir_all(&pack);
    std::fs::create_dir_all(pack.join("root")).unwrap();
    export_sim_tree(&pack.join("root"));
    let mut meta = String::new();
    meta.push_str(&format!("k={k}\n"));
    meta.push_str(&format!("seed={:#x}\n", per_point_seed(k)));
    meta.push_str(&format!("acked={acked}\n"));
    meta.push_str(&format!("ops={ops_used}\n"));
    meta.push_str(&format!("cuts={}\n", SimVfs::cut_count()));
    meta.push_str(&format!("stopped={}\n", stopped.as_deref().unwrap_or("-")));
    for l in SimVfs::fault_log() {
        meta.push_str(&format!("faultlog: {l}\n"));
    }
    std::fs::write(pack.join("meta.txt"), meta).unwrap();
    println!("SIM_WRITER_DONE k={k} acked={acked} ops={ops_used} cuts={}", SimVfs::cut_count());
}

// ---------------------------------------------------------------------------
// RECOVER child: import the crash image, boot the PRODUCT recovery, verify
// ---------------------------------------------------------------------------

fn mvcc_snapshot<'m>(mcx: Mcx<'m>) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    // Every workload xid (3..) is in the past: visibility = did it commit.
    s.xmin = 1000;
    s.xmax = 1000;
    s.regd_count.set(1);
    s
}

fn page_tuple(page_addr: *mut u8, off: u16) -> HeapTupleData<'static> {
    // SAFETY: pinned buffer page, held across the visibility check.
    let page = unsafe {
        types_storage::bufpage::PageRef::from_raw(
            core::ptr::NonNull::new(page_addr).unwrap(),
        )
    };
    let id = page.item_id(off);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page image under the caller's pin.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(0, off), REL_OID) }
}

#[test]
#[ignore]
fn sim_sweep_recover_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("recover") {
        return;
    }
    let pack = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    let get = |key: &str| -> String {
        meta.lines()
            .find(|l| l.starts_with(&format!("{key}=")))
            .map(|l| l.split('=').nth(1).unwrap().to_string())
            .unwrap()
    };
    let acked: u32 = get("acked").parse().unwrap();
    let k = get("k");
    let seed = get("seed");
    let tag = format!("k={k} seed={seed}");
    let mut violations: Vec<String> = Vec::new();

    SimVfs::reset();
    import_tree_into_sim(&pack.join("root"), "/");
    init_small::globals::SetDataDir("/");
    init_small::globals::set_enableFsync(true);
    install_stub_seams();
    install_real();

    // The PRODUCT boot path over the crash image. A failure anywhere here is
    // a recovery-completeness violation (property 1) — a FINDING.
    let boot = std::panic::catch_unwind(|| -> PgResult<()> {
        transam_xlog::ReadControlFile()?;
        transam_xlog::XLOGShmemInit();
        transam_xlog::StartupXLOG()?;
        Ok(())
    });
    match boot {
        Ok(Ok(())) => {}
        Ok(Err(e)) => violations.push(format!("{tag}: RECOVERY FAILED: {e:?}")),
        Err(_) => violations.push(format!("{tag}: RECOVERY PANICKED")),
    }

    if violations.is_empty() {
        let cf = *transam_xlog::control_file::control_file();
        if cf.state != DB_IN_PRODUCTION {
            violations.push(format!("{tag}: post-recovery control state {}", cf.state));
        }

        // Property 3 scaffold: clog-committed txns must be a PREFIX 1..=h
        // (commits are strictly sequential in this workload).
        let mut h: u32 = 0;
        let mut prefix_ok = true;
        for t in 1..=TXNS {
            let xid = t + 2;
            let committed = transam::TransactionIdDidCommit(xid).unwrap_or(false);
            if committed {
                if t != h + 1 {
                    prefix_ok = false;
                    violations.push(format!(
                        "{tag}: clog commit gap — txn {t} committed but txn {} is not",
                        h + 1
                    ));
                }
                h = t;
            }
        }

        // Property 2: every ACKED txn survives (commit ack = WAL flush OK).
        if prefix_ok && h < acked {
            for t in (h + 1)..=acked {
                violations.push(format!(
                    "{tag}: ACKED txn {t} lost (recovered horizon {h})"
                ));
            }
        }

        // Property 3: the MVCC-visible heap content equals EXACTLY the fold
        // of the clog-committed prefix — uncommitted absent, nothing torn.
        let ctx = MemoryContext::new("sim_sweep_verify");
        let mcx = ctx.mcx();
        let rel = test_relation(mcx, REL_OID);
        smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
        let key = types_storage::RelFileLocatorBackend {
            locator: RLOC,
            backend: INVALID_PROC_NUMBER,
        };
        let nblocks = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM).unwrap();
        let snap = mvcc_snapshot(mcx);
        let mut visible: Vec<i32> = Vec::new();
        for b in 0..nblocks {
            let buf = bufmgr::ReadBuffer(&rel, b).unwrap();
            let page_addr = bufmgr::BufferGetPagePtr(buf).as_ptr();
            // SAFETY: pinned page image.
            let page = unsafe {
                types_storage::bufpage::PageRef::from_raw(
                    core::ptr::NonNull::new(page_addr).unwrap(),
                )
            };
            for off in 1..=page.max_offset_number() {
                let id = page.item_id(off);
                if !id.is_normal() {
                    continue;
                }
                let mut t = page_tuple(page_addr, off);
                let vis = heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
                    &mut t, &snap, buf,
                )
                .unwrap();
                if vis {
                    let (ptr, _len) = page.item_raw(id);
                    // SAFETY: heap tuple in-page: t_hoff byte at offset 22,
                    // int4 datum right after the (aligned) header.
                    let val = unsafe {
                        let hoff = *ptr.add(22) as usize;
                        ptr.add(hoff).cast::<i32>().read_unaligned()
                    };
                    visible.push(val);
                }
            }
            bufmgr::ReleaseBuffer(buf).unwrap();
        }
        visible.sort_unstable();
        let expected: Vec<i32> = (1..=h as i32).collect();
        if visible != expected {
            violations.push(format!(
                "{tag}: visible heap fold diverges — got {visible:?}, want 1..={h}"
            ));
        }

        // Determinism digest (FNV over the visible fold + horizons).
        let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
        let mut mix = |b: u64| {
            digest ^= b;
            digest = digest.wrapping_mul(0x1000_0000_01b3);
        };
        mix(h as u64);
        mix(acked as u64);
        for v in &visible {
            mix(*v as u64);
        }
        mix(cf.checkPoint);
        println!(
            "SIM_RECOVER_STATE h={h} acked={acked} visible={:?} digest={digest:016x}",
            visible
        );
    }

    for v in &violations {
        println!("VIOLATION: {v}");
    }
    println!("SIM_RECOVER_DONE violations={}", violations.len());
}

// ---------------------------------------------------------------------------
// the orchestrating tests
// ---------------------------------------------------------------------------

fn spawn_child(test_name: &str, envs: &[(&str, String)]) -> (bool, String) {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args([test_name, "--exact", "--ignored", "--test-threads=1", "--nocapture"]);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn run_point(base: &std::path::Path, k: u64, red: bool) -> (String, String) {
    let pack = base.join(format!("pack_k{k}{}", if red { "_red" } else { "" }));
    let (ok, wtext) = spawn_child(
        "sim_sweep_writer_child",
        &[
            (ROLE_ENV, "writer".into()),
            (K_ENV, k.to_string()),
            (PACK_ENV, pack.to_str().unwrap().into()),
            (RED_ENV, if red { "1" } else { "0" }.into()),
        ],
    );
    assert!(
        ok && wtext.contains("SIM_WRITER_DONE"),
        "writer child failed at k={k}:\n{wtext}"
    );
    let (ok, rtext) = spawn_child(
        "sim_sweep_recover_child",
        &[
            (ROLE_ENV, "recover".into()),
            (PACK_ENV, pack.to_str().unwrap().into()),
        ],
    );
    assert!(
        ok && rtext.contains("SIM_RECOVER_DONE"),
        "recover child failed at k={k}:\n{rtext}"
    );
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    let _ = std::fs::remove_dir_all(&pack);
    (rtext, meta)
}

fn meta_field(meta: &str, key: &str) -> String {
    meta.lines()
        .find(|l| l.starts_with(&format!("{key}=")))
        .map(|l| l.split('=').nth(1).unwrap().to_string())
        .unwrap()
}

/// THE PAYOFF (inc-2): cut at EVERY product-workload op; the PRODUCT's
/// StartupXLOG must recover every image and the committed/uncommitted/
/// consistency properties must hold at every point.
#[test]
fn product_shaped_crash_recovery_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return; // never recurse
    }
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // Fault-free baseline defines the op span and proves the writer rig.
    let (rtext, meta) = run_point(&base, 0, false);
    assert_eq!(meta_field(&meta, "acked"), TXNS.to_string(), "baseline acks all: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "0", "baseline must not cut: {meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "baseline recovery must be clean:\n{rtext}"
    );
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    assert!(n > 20, "workload too small to sweep ({n} ops)");

    let mut failures: Vec<String> = Vec::new();
    let mut cut_points = 0u64;
    for k in 1..=n {
        let (rtext, meta) = run_point(&base, k, false);
        if meta_field(&meta, "cuts") == "0" {
            failures.push(format!("k={k}: planned cut never fired ({})", meta_field(&meta, "ops")));
            continue;
        }
        cut_points += 1;
        for line in rtext.lines() {
            if let Some(v) = line.strip_prefix("VIOLATION: ") {
                failures.push(v.to_string());
            }
        }
    }
    let _ = std::fs::remove_dir_all(&base);

    eprintln!(
        "PRODUCT-SHAPED SWEEP: {cut_points} cut points over {n} product-workload ops \
         ({TXNS} txns + 1 product checkpoint), {} violations",
        failures.len()
    );
    assert_eq!(cut_points, n, "every sweep point must cut");
    assert!(
        failures.is_empty(),
        "PRODUCT-SHAPED CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Same point, three fresh universes: byte-identical packs, metas (incl. the
/// fault log) and recovered state (the replay-determinism gate, x3).
#[test]
fn sweep_point_replay_determinism_x3() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simdet_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // A mid-workload point (inside txn 2's commit): re-derive it from the
    // baseline op count so the pin survives workload evolution.
    let (_, meta) = run_point(&base, 0, false);
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    let k = n / 2;

    fn tree_digest(dir: &std::path::Path, acc: &mut Vec<(String, Vec<u8>)>) {
        let mut entries: Vec<_> =
            std::fs::read_dir(dir).unwrap().map(|e| e.unwrap()).collect();
        entries.sort_by_key(|e| e.file_name());
        for e in entries {
            if e.file_type().unwrap().is_dir() {
                tree_digest(&e.path(), acc);
            } else {
                acc.push((
                    e.path().to_str().unwrap().rsplit('/').next().unwrap().to_string(),
                    std::fs::read(e.path()).unwrap(),
                ));
            }
        }
    }

    let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
    for rep in 0..3 {
        let pack = base.join(format!("det_rep{rep}"));
        let (ok, _) = spawn_child(
            "sim_sweep_writer_child",
            &[
                (ROLE_ENV, "writer".into()),
                (K_ENV, k.to_string()),
                (PACK_ENV, pack.to_str().unwrap().into()),
                (RED_ENV, "0".into()),
            ],
        );
        assert!(ok);
        let (ok, rtext) = spawn_child(
            "sim_sweep_recover_child",
            &[
                (ROLE_ENV, "recover".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
            ],
        );
        assert!(ok, "recover rep {rep} failed:\n{rtext}");
        let mut tree = Vec::new();
        tree_digest(&pack.join("root"), &mut tree);
        let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
        let state = rtext
            .lines()
            .find(|l| l.starts_with("SIM_RECOVER_STATE"))
            .unwrap_or("MISSING")
            .to_string();
        runs.push((tree, meta, state));
        let _ = std::fs::remove_dir_all(&pack);
    }
    let _ = std::fs::remove_dir_all(&base);

    assert!(!runs[0].1.is_empty());
    assert!(runs[0].1.contains("cuts=1"), "the determinism point must cut: {}", runs[0].1);
    for rep in 1..3 {
        assert_eq!(runs[0].0, runs[rep].0, "post-crash image trees must be byte-identical");
        assert_eq!(runs[0].1, runs[rep].1, "meta (incl. fault log) must be byte-identical");
        assert_eq!(runs[0].2, runs[rep].2, "recovered state must be identical");
    }
}

/// PRODUCT-SHAPED RED: disable the product's fsync layer through its own
/// knob (enableFsync gates fd::pg_fsync and issue_xlog_fsync — the exact
/// surface a lost fsync bug would occupy) and prove the sweep CATCHES the
/// resulting acked-data loss. If this arm ever comes back clean, the
/// product-shaped harness lost its teeth.
#[test]
fn red_product_fsync_disabled_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simred_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (rtext, meta) = run_point(&base, 0, true);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(
        meta_field(&meta, "acked"),
        TXNS.to_string(),
        "the fsync-less engine happily acks everything: {meta}"
    );
    assert!(
        rtext.contains("VIOLATION:") && rtext.contains("ACKED txn"),
        "the sweep must flag the acked-data loss of the fsync-disabled arm:\n{rtext}"
    );
}

/// Item 1 of the inc-2 charter: a REAL (C PostgreSQL 18) initdb'd datadir
/// imported into the SimVfs namespace via the cp -RL/pack importer, booted
/// through the PRODUCT's ReadControlFile + StartupXLOG entirely inside sim.
///
/// COMPOSE FINDING 1 scope note: the finding's tzdata/share blocker lives in
/// whole-server boots (postgresql.conf -> timezone GUCs -> pgtz scan). The
/// recovery entry point never consults share/timezone, so NO provider
/// slice is needed for THIS lane; the full-server sim boot stays blocked on
/// the finding (dst-p3-scheduler REQUIRED section), untouched here.
///
/// Needs a native C initdb (the wasm-boot prereq): set PGRUST_SIM_SWEEP_INITDB_DD
/// to a pre-minted datadir, or have initdb on the wasm-boot probe paths.
/// Skips LOUDLY when absent.
#[test]
fn initdb_datadir_boots_under_sim() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    // Find or mint a native datadir.
    let dd: Option<std::path::PathBuf> = match std::env::var(INITDB_ENV) {
        Ok(p) => Some(std::path::PathBuf::from(p)),
        Err(_) => {
            let mut found = None;
            for cand in ["/tmp/pgrust_pginstall/bin/initdb", "/opt/homebrew/bin/initdb"] {
                if std::path::Path::new(cand).exists() {
                    found = Some(cand);
                    break;
                }
            }
            found.map(|initdb| {
                let dd = std::env::temp_dir()
                    .join(format!("pgrust_sim_initdb_{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dd);
                let out = std::process::Command::new(initdb)
                    .args([
                        "-D",
                        dd.to_str().unwrap(),
                        "--no-locale",
                        "--encoding=UTF8",
                        "-U",
                        "postgres",
                        "-A",
                        "trust",
                    ])
                    .output()
                    .expect("initdb spawn");
                assert!(
                    out.status.success(),
                    "initdb failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                dd
            })
        }
    };
    let Some(dd) = dd else {
        eprintln!(
            "SKIP initdb_datadir_boots_under_sim: no C PostgreSQL 18 initdb found \
             (set {INITDB_ENV} or install to /opt/homebrew/bin) — flagged, not silent"
        );
        return;
    };

    // Import + boot in a CHILD (fresh statics), reusing the recover child
    // over a pack whose root IS the initdb datadir.
    let base = std::env::temp_dir().join(format!("pgrust_sim_initdbpack_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (ok, text) = spawn_child(
        "sim_initdb_boot_child",
        &[
            (ROLE_ENV, "initdb-boot".into()),
            (PACK_ENV, dd.to_str().unwrap().into()),
        ],
    );
    let _ = std::fs::remove_dir_all(&base);
    assert!(
        ok && text.contains("SIM_INITDB_BOOT_OK"),
        "real-initdb datadir failed to boot the product recovery under sim:\n{text}"
    );
}

#[test]
#[ignore]
fn sim_initdb_boot_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("initdb-boot") {
        return;
    }
    let dd = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());

    SimVfs::reset();
    // The cp -RL/pack import: the whole real initdb'd datadir into the sim
    // namespace (symlinks dereferenced by the importer).
    import_tree_into_sim(&dd, "/");
    init_small::globals::SetDataDir("/");
    init_small::globals::set_enableFsync(true);
    install_stub_seams();
    install_real();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();
    transam_xlog::StartupXLOG().unwrap();

    let cf = *transam_xlog::control_file::control_file();
    assert_eq!(cf.state, DB_IN_PRODUCTION, "post-boot control state");
    println!(
        "SIM_INITDB_BOOT_OK sysid={:#x} ckpt={:#x} tli={}",
        cf.system_identifier, cf.checkPoint, cf.checkPointCopy.ThisTimeLineID
    );
}
