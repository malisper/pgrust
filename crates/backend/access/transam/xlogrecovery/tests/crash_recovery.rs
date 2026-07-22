// M4 crash-recovery proof: real inserts+delete+commit over the real
// bufmgr/smgr/xloginsert/xact, datadir copied mid-run (pages unflushed, heap
// file truncated), then a child process boots the copy through the real
// StartupXLOG/PerformWalRecovery and verifies page bytes + MVCC visibility.
//
// NATIVE ARM ONLY: this rig mixes std::fs fixture plumbing with the fd/vfs
// data plane, which under `--cfg pgrust_sim` would split across two worlds
// (real disk vs the SimVfs namespace). The sim-cfg twin — the same product
// write/recovery paths driven entirely inside SimVfs, swept with the P4
// fault model — is tests/sim_crash_sweep.rs.
#![cfg(not(pgrust_sim))]

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
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use types_nbtree::{BTPageOpaqueData, BTP_LEAF, P_NONE};
use types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_RELATION,
};
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
const REL3_OID: Oid = 61002;
const IDX_OID: Oid = 61003;
const REL5_OID: Oid = 61005;
const REL6_OID: Oid = 61006;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RLOC2: RelFileLocator = RelFileLocator::new(1663, 5, REL2_OID);
const RLOC3: RelFileLocator = RelFileLocator::new(1663, 5, REL3_OID);
const RLOC4: RelFileLocator = RelFileLocator::new(1663, 5, IDX_OID);
const RLOC5: RelFileLocator = RelFileLocator::new(1663, 5, REL5_OID);
const RLOC6: RelFileLocator = RelFileLocator::new(1663, 5, REL6_OID);
const WIDE: usize = 1536;
// evens then odds: forces rightmost and interior leaf splits (SPLIT_R,
// SPLIT_L + right-sibling arm, INSERT_UPPER) plus NEWROOT.
const NIDX_KEYS: i32 = 900;
// MAXALIGN(SizeOfPageHeaderData): first VM map byte; heap block 0's
// all-visible bit is its low bit.
const VM_FIRST_MAP_BYTE: usize = 24;

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
    // Real aio: recovery's cold reads run the pgaio pipeline (stubbing
    // pgaio_io_start_readv leaves handles handed-out and the read wait
    // errors out).
    ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
    aio_core::init_seams();
    guc_tables::vars::io_max_combine_limit.install_if_absent(guc_tables::GucVarAccessors {
        get: || 16,
        set: |_| {},
    });
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
    // vm_extend's CacheInvalidateSmgr (immediate smgr inval, C
    // visibilitymap.c) — single-process rig, nobody to notify.
    sinval_seams::send_shared_invalid_messages::set(|_| Ok(()));
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
    // walwriter's slot; the index lane's WAL volume crosses page-init writes.
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
    aio_core::AioShmemSize().unwrap();
    aio_core::AioShmemInit().unwrap();
    sync::InitSync().unwrap();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    aio_core::pgaio_init_backend();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    // Buffer pins register with CurrentResourceOwner; recovery runs under the
    // aux-process owner in C (CreateAuxProcessResourceOwner).
    if resowner::CurrentResourceOwner().is_null() {
        let owner = resowner::ResourceOwnerCreate(
            types_resowner::ResourceOwner::NULL,
            "crash-recovery-test",
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
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

fn test_int4cmp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let a = fcinfo.arg(0).as_i32();
    let b = fcinfo.arg(1).as_i32();
    Ok(datum::Datum::from_i32((a > b) as i32 - (a < b) as i32))
}

fn index_rel<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t_idx");
    let one = |v: Oid| {
        let mut vec = PgVec::new_in(mcx);
        vec.push(v);
        vec
    };
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1i16);
    let mut indoption = PgVec::new_in(mcx);
    indoption.push(0i16);
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: IDX_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: IDX_OID, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: types_core::BTREE_AM_OID,
            relfilenode: IDX_OID,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_INDEX,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: int4_tupdesc(mcx),
        rd_index: Some(FormData_pg_index {
            indexrelid: IDX_OID,
            indrelid: REL_OID,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indkey,
            has_indpred: false,
        indexprs_src: None,
        indpred_src: None,
        }),
        rd_opcintype: one(23),
        rd_opfamily: one(1976),
        rd_indoption: indoption,
        rd_indcollation: one(0),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    let rel = Relation::open(data, Some(noop_close));
    rel.rd_supportinfo
        .borrow_mut()
        .push(Some(FmgrInfo::new(test_int4cmp, 351, 2, true, false)));
    rel
}

fn bt_opaque(page: &PageRef<'_>) -> BTPageOpaqueData {
    let off = page.pd_special() as usize;
    // SAFETY: in-bounds 4-aligned special area of a btree page.
    unsafe { page.as_ptr().add(off).cast::<BTPageOpaqueData>().read() }
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
    let rel = test_relation(mcx, REL_OID);
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

    // The XLH_INSERT_ALL_VISIBLE_CLEARED replay: PD_ALL_VISIBLE gone, tuple
    // re-added, VM bit cleared — all through the live buffer manager.
    let rel3 = test_relation(mcx, REL3_OID);
    let buf3 = bufmgr::ReadBuffer(&rel3, 0).unwrap();
    {
        // SAFETY: pinned page image.
        let page = unsafe { PageRef::from_raw(bufmgr::BufferGetPagePtr(buf3)) };
        assert!(!page.is_all_visible(), "replay cleared PD_ALL_VISIBLE");
        assert_eq!(page.max_offset_number(), 2, "replay re-added tuple (0,2)");
    }
    bufmgr::ReleaseBuffer(buf3).unwrap();
    let vmbuf = bufmgr::ReadBufferExtended(
        &rel3,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        0,
        types_storage::ReadBufferMode::Normal,
        None,
    )
    .unwrap();
    // SAFETY: pinned page image.
    let vm_byte = unsafe {
        *bufmgr::BufferGetPagePtr(vmbuf).as_ptr().add(VM_FIRST_MAP_BYTE)
    };
    bufmgr::ReleaseBuffer(vmbuf).unwrap();
    assert_eq!(vm_byte, 0, "replay cleared the VM all-visible bit");

    // The COPY FREEZE replay lane (rel6): MULTI_INSERT+INIT_PAGE with
    // XLH_INSERT_ALL_FROZEN_SET rebuilds the page all-visible with the frozen
    // rows; the following XLOG_HEAP2_VISIBLE record rebuilds the VM bits —
    // all through the live buffer manager. A VM bit without its heap rows
    // would be a wrong-results class (index-only scans trust the VM).
    let rel6 = test_relation(mcx, REL6_OID);
    let buf6 = bufmgr::ReadBuffer(&rel6, 0).unwrap();
    {
        // SAFETY: pinned page image.
        let page = unsafe { PageRef::from_raw(bufmgr::BufferGetPagePtr(buf6)) };
        assert!(page.is_all_visible(), "replay kept PD_ALL_VISIBLE on the frozen page");
        assert_eq!(page.max_offset_number(), 3, "replay re-added the frozen rows");
        for off in 1..=3u16 {
            let id = page.item_id(off);
            let (ptr, len) = page.item_raw(id);
            // SAFETY: in-page image under the pin.
            let t = unsafe {
                HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(0, off), REL6_OID)
            };
            assert!(t.t_data().xmin_frozen(), "replayed row {off} keeps its frozen xmin");
        }
    }
    bufmgr::ReleaseBuffer(buf6).unwrap();
    let vmbuf6 = bufmgr::ReadBufferExtended(
        &rel6,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        0,
        types_storage::ReadBufferMode::Normal,
        None,
    )
    .unwrap();
    // SAFETY: pinned page image.
    let vm_byte6 = unsafe {
        *bufmgr::BufferGetPagePtr(vmbuf6).as_ptr().add(VM_FIRST_MAP_BYTE)
    };
    bufmgr::ReleaseBuffer(vmbuf6).unwrap();
    assert_eq!(vm_byte6, 0x03, "replay set the VM all-visible|all-frozen bits");

    // btree_redo replay: chain-walk the rebuilt leaf level through the live
    // buffer manager and assert the full ordered key set survives.
    let idx = index_rel(mcx);
    let idx_key = types_storage::RelFileLocatorBackend {
        locator: RLOC4,
        backend: INVALID_PROC_NUMBER,
    };
    smgr::smgropen(RLOC4, INVALID_PROC_NUMBER).unwrap();
    let idx_nblocks = smgr::smgrnblocks(idx_key, ForkNumber::MAIN_FORKNUM).unwrap();
    let mut leftmost = None;
    for b in 1..idx_nblocks {
        let buf = bufmgr::ReadBuffer(&idx, b).unwrap();
        // SAFETY: pinned page image.
        let page = unsafe { PageRef::from_raw(bufmgr::BufferGetPagePtr(buf)) };
        let opaque = bt_opaque(&page);
        if opaque.btpo_flags & BTP_LEAF != 0 && opaque.btpo_prev == P_NONE {
            assert!(leftmost.is_none(), "one leftmost leaf");
            leftmost = Some(b);
        }
        bufmgr::ReleaseBuffer(buf).unwrap();
    }
    let mut blk = leftmost.expect("leftmost leaf found");
    let mut keys: Vec<i32> = Vec::new();
    loop {
        let buf = bufmgr::ReadBuffer(&idx, blk).unwrap();
        // SAFETY: pinned page image, held for the walk of this page.
        let page = unsafe { PageRef::from_raw(bufmgr::BufferGetPagePtr(buf)) };
        let opaque = bt_opaque(&page);
        let first = if opaque.btpo_next == P_NONE { 1 } else { 2 };
        for off in first..=page.max_offset_number() {
            let id = page.item_id(off);
            let (ptr, _) = page.item_raw(id);
            // SAFETY: int4 key at the 8-byte index-tuple header boundary.
            keys.push(unsafe { ptr.add(8).cast::<i32>().read_unaligned() });
        }
        let next = opaque.btpo_next;
        bufmgr::ReleaseBuffer(buf).unwrap();
        if next == P_NONE {
            break;
        }
        blk = next;
    }
    assert_eq!(keys.len(), NIDX_KEYS as usize, "replayed index holds every key");
    assert!(
        keys.iter().copied().eq(1..=NIDX_KEYS),
        "replayed index scan order is 1..={NIDX_KEYS}"
    );

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

    let ctx = MemoryContext::new("crash_recovery");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx, REL_OID);
    let rel3 = test_relation(mcx, REL3_OID);
    let rel5 = test_relation(mcx, REL5_OID);
    let rel6 = test_relation(mcx, REL6_OID);
    let tupdesc = int4_tupdesc(mcx);
    for rloc in [RLOC, RLOC3, RLOC5, RLOC6] {
        smgr::smgropen(rloc, INVALID_PROC_NUMBER).unwrap();
        smgr::smgrcreate(
            types_storage::RelFileLocatorBackend { locator: rloc, backend: INVALID_PROC_NUMBER },
            ForkNumber::MAIN_FORKNUM,
            false,
        )
        .unwrap();
    }

    // Transaction 1 (real xact): inserts 41,42,43 then delete (0,2), commit.
    xact::StartTransactionCommand().unwrap();
    let insert_into = |r: &RelationData<'_>, val: i32, cid: u32| {
        let mut tup =
            heaptuple::heap_form_tuple(mcx, &tupdesc, &[datum::Datum::from_i32(val)], &[false])
                .unwrap();
        heapam::heap_insert(r, tup.as_tuple_mut(), cid, 0, None).unwrap();
        tup.as_tuple().t_self
    };
    let insert = |val: i32, cid: u32| insert_into(&rel, val, cid);
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
    // cid 0: cmin is not WAL-logged and the INIT_PAGE replay re-stamps
    // FirstCommandId, so a nonzero cid would break the byte comparison.
    assert_eq!(insert_into(&rel3, 51, 0), ItemPointerData::new(0, 1));

    // rel5 update lane: wide rows, a HOT update (same page), then a wide
    // update that overflows the page — the write side emits xl_heap_lock on
    // the old row plus a cross-page XLOG_HEAP_UPDATE|INIT_PAGE.
    {
        let wide_tuple = |fill: u8| {
            let mut img = vec![0u8; WIDE];
            img[18..20].copy_from_slice(&1u16.to_ne_bytes()); // natts
            img[20..22].copy_from_slice(&types_tuple::HEAP_XMAX_INVALID.to_ne_bytes());
            img[22] = 24; // t_hoff
            img[24..].fill(fill);
            let words = WIDE.div_ceil(8);
            // Leaked (test-only): moving a Box would invalidate the pointer.
            let buf: &'static mut [u64] = Box::leak(vec![0u64; words].into_boxed_slice());
            // SAFETY: buf is words*8 >= img.len() writable bytes.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    img.as_ptr(),
                    buf.as_mut_ptr().cast::<u8>(),
                    img.len(),
                )
            };
            // SAFETY: 8-aligned leaked image, header-complete, unique.
            unsafe {
                HeapTupleData::from_raw_parts(
                    buf.as_mut_ptr().cast::<u8>(),
                    WIDE as u32,
                    ItemPointerData::invalid(),
                    0,
                )
            }
        };
        for v in 1u8..=4 {
            let mut tup = wide_tuple(v);
            heapam::heap_insert(&rel5, &mut tup, 0, 0, None).unwrap();
            assert_eq!(tup.t_self, ItemPointerData::new(0, v as u16));
        }
        let mut lockmode = tableam_vocab::LockTupleMode::LockTupleNoKeyExclusive;
        let mut update_indexes = tableam_vocab::TU_UpdateIndexes::TU_None;
        // HOT: 1540 fits the 2008 bytes free on page 0.
        let mut tup = wide_tuple(0x22);
        let r = heapam::heap_update(
            &rel5,
            &ItemPointerData::new(0, 2),
            &mut tup,
            1,
            None,
            true,
            &mut tmfd,
            &mut lockmode,
            &mut update_indexes,
        )
        .unwrap();
        assert_eq!(r, TM_Result::TM_Ok);
        assert_eq!(tup.t_self, ItemPointerData::new(0, 5));
        // Non-HOT: 1540 > the 468 bytes now free forces the new page.
        let mut tup = wide_tuple(0x33);
        let r = heapam::heap_update(
            &rel5,
            &ItemPointerData::new(0, 1),
            &mut tup,
            2,
            None,
            true,
            &mut tmfd,
            &mut lockmode,
            &mut update_indexes,
        )
        .unwrap();
        assert_eq!(r, TM_Result::TM_Ok);
        assert_eq!(tup.t_self, ItemPointerData::new(1, 1));
    }

    // rel6 COPY FREEZE lane: heap_multi_insert with HEAP_INSERT_FROZEN onto a
    // page started empty sets PD_ALL_VISIBLE + the VM all-visible|all-frozen
    // bits at insert time (heapam.c:2460-2654), emitting two records —
    // MULTI_INSERT+INIT_PAGE carrying XLH_INSERT_ALL_FROZEN_SET, then
    // XLOG_HEAP2_VISIBLE from visibilitymap_set. Neither the heap page nor
    // the VM page is ever flushed pre-crash: replay must rebuild both.
    {
        let mk_slot = |val: i32| {
            let mut slot = exectuples::make_tuple_table_slot(
                mcx,
                types_slot::TupleSlotKind::HeapTuple,
                Some(tupdesc.clone()),
            );
            let tup = heaptuple::heap_form_tuple(
                mcx,
                &tupdesc,
                &[datum::Datum::from_i32(val)],
                &[false],
            )
            .unwrap();
            exectuples::exec_store_heap_tuple_owned(&mut slot, mcx, tup);
            slot
        };
        let mut s1 = mk_slot(61);
        let mut s2 = mk_slot(62);
        let mut s3 = mk_slot(63);
        let mut slots = [&mut s1, &mut s2, &mut s3];
        heapam::heap_multi_insert(
            mcx,
            &rel6,
            &mut slots,
            0,
            heapam::hio::HEAP_INSERT_FROZEN,
            None,
        )
        .unwrap();

        let buf6 = bufmgr::ReadBuffer(&rel6, 0).unwrap();
        {
            // SAFETY: pinned page image.
            let page = unsafe { PageRef::from_raw(bufmgr::BufferGetPagePtr(buf6)) };
            assert!(page.is_all_visible(), "COPY FREEZE marked the buffered page all-visible");
            assert_eq!(page.max_offset_number(), 3);
        }
        bufmgr::ReleaseBuffer(buf6).unwrap();
        let vmbuf = bufmgr::ReadBufferExtended(
            &rel6,
            ForkNumber::VISIBILITYMAP_FORKNUM,
            0,
            types_storage::ReadBufferMode::Normal,
            None,
        )
        .unwrap();
        // SAFETY: pinned page image.
        let byte = unsafe {
            *bufmgr::BufferGetPagePtr(vmbuf).as_ptr().add(VM_FIRST_MAP_BYTE)
        };
        bufmgr::ReleaseBuffer(vmbuf).unwrap();
        assert_eq!(byte, 0x03, "COPY FREEZE set the buffered VM all-visible|all-frozen bits");
    }

    xact::CommitTransactionCommand().unwrap();
    assert!(transam::TransactionIdDidCommit(xid1).unwrap());

    // Vacuum's outcome by hand (the CLEAR direction needs pre-existing
    // all-visible state on disk; the SET direction is WAL-driven and covered
    // by the rel6 COPY FREEZE lane): PD_ALL_VISIBLE on rel3's flushed page
    // and a VM fork file with block 0 all-visible.
    // Read-only xact: buffer pins need a live resource owner; no xid taken.
    {
        xact::StartTransactionCommand().unwrap();
        let buf = bufmgr::ReadBuffer(&rel3, 0).unwrap();
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_EXCLUSIVE).unwrap();
        // SAFETY: pinned + exclusively locked page.
        let mut pm = unsafe {
            PageMut::from_raw(bufmgr::BufferGetPagePtr(buf))
        };
        pm.set_all_visible();
        bufmgr::MarkBufferDirty(buf).unwrap();
        bufmgr::FlushOneBuffer(buf).unwrap();
        bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_UNLOCK).unwrap();
        bufmgr::ReleaseBuffer(buf).unwrap();

        #[repr(align(8))]
        struct P([u8; BLCKSZ]);
        let mut p = P([0u8; BLCKSZ]);
        // SAFETY: aligned, exclusively owned stack page.
        let mut vm = unsafe {
            PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap())
        };
        vm.init(0);
        p.0[VM_FIRST_MAP_BYTE] = 0x01;
        std::fs::write(dd1.join("base/5").join(format!("{REL3_OID}_vm")), p.0).unwrap();
        xact::CommitTransactionCommand().unwrap();
    }

    // Index lane: empty-metapage fixture (btbuild's unlogged image, C shape),
    // then committed btinserts across leaf splits and a root split; crash
    // replay must rebuild every index page through btree_redo alone (buffer
    // contents are never flushed pre-crash).
    let idx = index_rel(mcx);
    let idx_key = types_storage::RelFileLocatorBackend {
        locator: RLOC4,
        backend: INVALID_PROC_NUMBER,
    };
    smgr::smgropen(RLOC4, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(idx_key, ForkNumber::MAIN_FORKNUM, false).unwrap();
    {
        #[repr(align(8))]
        struct P([u8; BLCKSZ]);
        let mut p = P([0u8; BLCKSZ]);
        // SAFETY: aligned, exclusively owned stack page.
        let mut pm =
            unsafe { PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap()) };
        nbtree::bt_initmetapage(&mut pm, P_NONE, 0, false);
        smgr::smgrextend(idx_key, ForkNumber::MAIN_FORKNUM, 0, &p.0, false).unwrap();
    }
    {
        xact::StartTransactionCommand().unwrap();
        let insert_key = |k: i32| {
            let icx = MemoryContext::new("btins");
            nbtree::btinsert(
                icx.mcx(),
                &idx,
                &[datum::Datum::from_i32(k)],
                &[false],
                &ItemPointerData::new((k as u32 - 1) / 200, ((k - 1) % 200 + 1) as u16),
                &idx,
                types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO,
                false,
            )
            .unwrap();
        };
        let mut k = 2;
        while k <= NIDX_KEYS {
            insert_key(k);
            k += 2;
        }
        k = 1;
        while k <= NIDX_KEYS {
            insert_key(k);
            k += 2;
        }
        xact::CommitTransactionCommand().unwrap();
    }
    smgr::smgropen(RLOC4, INVALID_PROC_NUMBER).unwrap();
    let idx_nblocks = smgr::smgrnblocks(idx_key, ForkNumber::MAIN_FORKNUM).unwrap();
    assert!(idx_nblocks >= 5, "splits + root split happened (nblocks={idx_nblocks})");
    let mut idx_expected: Vec<u8> = Vec::new();
    {
        // Read-only xact: buffer pins need a live resource owner.
        xact::StartTransactionCommand().unwrap();
        for b in 0..idx_nblocks {
            idx_expected.extend_from_slice(&read_page_from_buffer(&idx, b));
        }
        xact::CommitTransactionCommand().unwrap();
    }
    std::fs::write(base.join("expected_idx.bin"), &idx_expected).unwrap();

    // Transaction 2: insert 44, never committed (lost in the crash). The rel3
    // insert lands on an all-visible page: heap_insert clears PD_ALL_VISIBLE
    // and the VM bit (buffers only) and stamps XLH_INSERT_ALL_VISIBLE_CLEARED.
    xact::StartTransactionCommand().unwrap();
    assert_eq!(insert(44, 0), ItemPointerData::new(0, 4));
    assert_eq!(insert_into(&rel3, 52, 0), ItemPointerData::new(0, 2));
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
    let expected_page3 = read_page_from_buffer(&rel3, 0);
    std::fs::write(base.join("expected_page3.bin"), expected_page3).unwrap();
    let expected_page5: Vec<[u8; BLCKSZ]> =
        (0..2).map(|b| read_page_from_buffer(&rel5, b)).collect();
    let expected_page6 = read_page_from_buffer(&rel6, 0);

    // Clean(-shutdown) control for the VM: the buffered map byte is cleared.
    {
        let vmbuf = bufmgr::ReadBufferExtended(
            &rel3,
            ForkNumber::VISIBILITYMAP_FORKNUM,
            0,
            types_storage::ReadBufferMode::Normal,
            None,
        )
        .unwrap();
        // SAFETY: pinned page image.
        let byte = unsafe {
            *bufmgr::BufferGetPagePtr(vmbuf).as_ptr().add(VM_FIRST_MAP_BYTE)
        };
        bufmgr::ReleaseBuffer(vmbuf).unwrap();
        assert_eq!(byte, 0, "heap_insert cleared the buffered VM bit");
    }

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

    // rel3's crash-state files: the flushed heap page still all-visible with
    // one tuple, the VM file still carrying the set bit (clears were only in
    // buffers).
    {
        let mut disk = std::fs::read(dd2.join("base/5").join(REL3_OID.to_string())).unwrap();
        assert_eq!(disk.len(), BLCKSZ);
        let r = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(disk.as_mut_ptr()).unwrap())
        };
        assert!(r.is_all_visible(), "pre-crash disk page keeps PD_ALL_VISIBLE");
        assert_eq!(r.max_offset_number(), 1, "second insert never flushed");
        let vm = std::fs::read(dd2.join("base/5").join(format!("{REL3_OID}_vm"))).unwrap();
        assert_eq!(vm[VM_FIRST_MAP_BYTE], 0x01, "pre-crash disk VM bit set");
    }

    // rel6 crash-state: neither the heap page nor the VM bit was ever
    // flushed — post-crash they must come back from WAL replay alone. The
    // dangerous direction (a VM bit on disk covering rows that only exist in
    // lost buffers) must be impossible: the bit is WAL-first.
    {
        let disk6 = std::fs::read(dd2.join("base/5").join(REL6_OID.to_string())).unwrap();
        assert!(
            disk6.iter().all(|b| *b == 0),
            "rel6 heap page must not be flushed pre-crash"
        );
        if let Ok(vm6) = std::fs::read(dd2.join("base/5").join(format!("{REL6_OID}_vm"))) {
            if vm6.len() > VM_FIRST_MAP_BYTE {
                assert_eq!(
                    vm6[VM_FIRST_MAP_BYTE] & 0x03,
                    0,
                    "rel6 VM bit must not be flushed pre-crash"
                );
            }
        }
    }

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

    // The VM-clear replay: heap page byte-equal to pre-crash truth (tuple 52
    // re-added, PD_ALL_VISIBLE cleared), VM bit cleared on disk.
    let replayed3 = std::fs::read(dd2.join("base/5").join(REL3_OID.to_string())).unwrap();
    assert_eq!(replayed3, expected_page3.to_vec(), "VM-cleared heap page is byte-exact");

    // The update lane: HOT + cross-page (lock, INIT_PAGE) replay byte-exact.
    // cmin/cmax (t_field3) of the update-touched tuples and the PD_PAGE_FULL
    // writer hint are not WAL-logged; both sides are normalized (C-exact).
    let replayed5 = std::fs::read(dd2.join("base/5").join(REL5_OID.to_string())).unwrap();
    assert_eq!(replayed5.len(), 2 * BLCKSZ);
    let cid_tuples: [&[u16]; 2] = [&[1, 2, 5], &[1]];
    for b in 0..2usize {
        let mut got = replayed5[b * BLCKSZ..(b + 1) * BLCKSZ].to_vec();
        let mut want = expected_page5[b].to_vec();
        for &off in cid_tuples[b] {
            for page in [&mut got, &mut want] {
                let r = unsafe {
                    PageRef::from_raw(core::ptr::NonNull::new(page.as_mut_ptr()).unwrap())
                };
                let o = r.item_id(off).lp_off() as usize;
                page[o + 8..o + 12].fill(0);
            }
        }
        got[10] &= !0x02;
        want[10] &= !0x02;
        if got != want {
            let first = got.iter().zip(&want).position(|(a, c)| a != c).unwrap();
            panic!(
                "replayed update-lane block {b} differs at byte {first}: got {:02x?} want {:02x?}",
                &got[first..(first + 16).min(BLCKSZ)],
                &want[first..(first + 16).min(BLCKSZ)]
            );
        }
    }
    let vm3 = std::fs::read(dd2.join("base/5").join(format!("{REL3_OID}_vm"))).unwrap();
    assert_eq!(vm3[VM_FIRST_MAP_BYTE], 0, "replay cleared the VM bit");

    // The COPY FREEZE lane: the replayed heap page is byte-equal to the
    // pre-crash buffered truth (PD_ALL_VISIBLE included), and the VM bits
    // reached disk via the end-of-recovery checkpoint.
    let replayed6 = std::fs::read(dd2.join("base/5").join(REL6_OID.to_string())).unwrap();
    assert_eq!(replayed6, expected_page6.to_vec(), "COPY FREEZE heap page is byte-exact");
    let vm6 = std::fs::read(dd2.join("base/5").join(format!("{REL6_OID}_vm"))).unwrap();
    assert_eq!(
        vm6[VM_FIRST_MAP_BYTE], 0x03,
        "replay set the VM all-visible|all-frozen bits on disk"
    );

    // The btree replay: every index block byte-equal to the pre-crash buffer
    // images (none of which had been flushed).
    let replayed_idx = std::fs::read(dd2.join("base/5").join(IDX_OID.to_string())).unwrap();
    assert_eq!(replayed_idx.len(), idx_expected.len());
    for b in 0..idx_nblocks as usize {
        let got = &replayed_idx[b * BLCKSZ..(b + 1) * BLCKSZ];
        let want = &idx_expected[b * BLCKSZ..(b + 1) * BLCKSZ];
        if got != want {
            let first = got.iter().zip(want).position(|(a, c)| a != c).unwrap();
            panic!(
                "replayed index block {b} differs at byte {first}: got {:02x?} want {:02x?}",
                &got[first..(first + 16).min(BLCKSZ)],
                &want[first..(first + 16).min(BLCKSZ)]
            );
        }
    }

    let _ = std::fs::remove_dir_all(&base);
}
