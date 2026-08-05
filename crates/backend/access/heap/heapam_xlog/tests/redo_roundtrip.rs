// Redo round-trip per the nbtree_xlog precedent: real heapam DML over a fake
// bufmgr writes real WAL (xloginsert + transam_xlog on disk, real xact xids),
// the fake storage is wiped, and every heap/heap2 record is replayed through
// the real rmgr dispatch into fresh pages; reconstructed blocks must equal
// the write-side pages byte-exact. Covers INSERT (+INIT_PAGE), HOT_UPDATE,
// LOCK + cross-page UPDATE (+INIT_PAGE), DELETE, and the C-writer shapes our
// write side does not emit yet: MULTI_INSERT (init + offsets variants, with
// SHORTALIGN padding), LOCK_UPDATED, CONFIRM, and a prefix/suffix-compressed
// same-page UPDATE.
#![allow(non_upper_case_globals)]
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use heapam::{heap_delete, heap_insert, heap_update};
use heapam_xlog::{
    XLH_UPDATE_PREFIX_FROM_OLD, XLH_UPDATE_SUFFIX_FROM_OLD, XLHL_KEYS_UPDATED,
    XLHL_XMAX_KEYSHR_LOCK, XLHL_XMAX_LOCK_ONLY, XLOG_HEAP2_LOCK_UPDATED,
    XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE,
    XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INSERT, XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, XLOG_HEAP_UPDATE,
};
use mcx::{Mcx, MemoryContext, PgVec};
use tableam_vocab::{LockTupleMode, TM_FailureData, TM_Result};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, Oid, TimeLineID,
    XLogRecPtr, XLogSegNo, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION,
};
use types_storage::bufpage::{PageMut, PageRef, PAI_IS_HEAP, PAI_OVERWRITE};
use types_storage::RelFileLocator;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, HeapTupleHeaderData,
    ItemPointerData, NameData, TupleDescData, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_UPDATED,
    HEAP_XMAX_BITS, HEAP_XMAX_INVALID, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;
use xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AAD0;
const REL_OID: Oid = 61010;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RM_HEAP_ID: u8 = rmgr::RmgrIds::RM_HEAP_ID as u8;
const RM_HEAP2_ID: u8 = rmgr::RmgrIds::RM_HEAP2_ID as u8;
const XID: u32 = 3; // checkpoint nextXid: the first assigned real xid
const WIDE: usize = 1536;
const SizeofHeapTupleHeader: usize = 23;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>,
    pins: Vec<i32>,
    locks: Vec<i32>,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    pages: Vec::new(),
    pins: Vec::new(),
    locks: Vec::new(),
});

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn new_page() -> usize {
    Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize
}

fn page_bytes(block: usize) -> [u8; BLCKSZ] {
    let addr = with_fake(|f| f.pages[block]);
    // SAFETY: leaked test page, always live.
    unsafe { *(addr as *const [u8; BLCKSZ]) }
}

fn page_mut(block: usize) -> PageMut<'static> {
    let addr = with_fake(|f| f.pages[block]);
    // SAFETY: leaked test page; the test is single-threaded.
    unsafe { PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap()) }
}

fn tuple_hdr(block: usize, off: u16) -> &'static mut HeapTupleHeaderData {
    let pm = page_mut(block);
    let page = pm.as_ref();
    let id = page.item_id(off);
    let (ptr, _len) = page.item_raw(id);
    // SAFETY: in-page normal item; the test is single-threaded.
    unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) }
}

fn install_fake_bufmgr() {
    bufmgr_seams::read_buffer::set(|_rel, block| {
        with_fake(|f| {
            assert!((block as usize) < f.pages.len());
            f.pins[block as usize] += 1;
            Ok(block as Buffer + 1)
        })
    });
    bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
    bufmgr_seams::buffer_get_page::set(|buf| {
        let addr = with_fake(|f| {
            assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
            f.pages[(buf - 1) as usize]
        });
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::release_buffer::set(|buf| {
        with_fake(|f| {
            let p = &mut f.pins[(buf - 1) as usize];
            assert!(*p > 0, "double release of buffer {buf}");
            *p -= 1;
        });
        Ok(())
    });
    bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
        if buf != types_core::InvalidBuffer {
            if buf == blkno as Buffer + 1 {
                return Ok(buf);
            }
            bufmgr_seams::release_buffer::call(buf)?;
        }
        bufmgr_seams::read_buffer::call(rel, blkno)
    });
    bufmgr_seams::incr_buffer_ref_count::set(|buf| {
        with_fake(|f| f.pins[(buf - 1) as usize] += 1);
    });
    bufmgr_seams::lock_buffer::set(|buf, mode| {
        with_fake(|f| {
            let l = &mut f.locks[(buf - 1) as usize];
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(*l > 0, "unlock without lock");
                    *l -= 1;
                }
                _ => {
                    assert_eq!(*l, 0, "double content lock");
                    *l += 1;
                }
            }
        });
        Ok(())
    });
    bufmgr_seams::conditional_lock_buffer::set(|buf| {
        with_fake(|f| f.locks[(buf - 1) as usize] += 1);
        Ok(true)
    });
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
        with_fake(|f| Ok(f.pages.len() as BlockNumber))
    });
    bufmgr_seams::extend_buffered_rel_by::set(|_rel, _fork, _strategy, flags, extend_by| {
        assert_eq!(extend_by, 1);
        assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
        Ok(with_fake(|f| {
            f.pages.push(new_page());
            f.pins.push(1);
            f.locks.push(1);
            (f.pages.len() as Buffer, 1)
        }))
    });

    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
    freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(InvalidBlockNumber)
    });
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    catalog_seams::is_catalog_relation::set(|_rel| false);
    catalog_seams::is_toast_relation::set(|_rel| false);
    origin_seams::replorigin_session_origin::set(|| 0);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));

    xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_with_flags::set(|rmid, info, flags, fragments| {
        xloginsert::insert_record(rmid, info, flags, fragments, &[])
    });
    // xloginsert marshal for fake buffers -> the real record-assembly path.
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let addr = with_fake(|f| f.pages[(b.buffer - 1) as usize]);
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RLOC,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: (b.buffer - 1) as BlockNumber,
                // SAFETY: leaked test page, BLCKSZ, pinned by the caller.
                page: unsafe { core::slice::from_raw_parts(addr as *const u8, BLCKSZ) },
                flags: b.flags,
                bufdata: b.bufdata,
            });
        }
        xloginsert::insert_record(rmid, info, flags, main_data, &blocks)
    });

    // Replay lane: XLogReadBufferExtended over the same fake registry.
    smgr_seams::smgr_create::set(|_, _, _| Ok(()));
    smgr_seams::smgr_nblocks::set(|_, _| Ok(with_fake(|f| f.pages.len() as BlockNumber)));
    smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| 0);
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
    smgr_seams::smgr_exists::set(|_loc, _fork| Ok(false));
    bufmgr_seams::read_recent_buffer::set(|_, _, _, _| Ok(false));
    // ZeroAndLock modes hand back an already-locked buffer (bufmgr contract).
    bufmgr_seams::read_buffer_without_relcache::set(|_loc, _fork, blkno, mode, _strat, _perm| {
        with_fake(|f| {
            assert!((blkno as usize) < f.pages.len());
            f.pins[blkno as usize] += 1;
            if mode != types_storage::ReadBufferMode::Normal {
                f.locks[blkno as usize] += 1;
            }
            Ok(blkno as Buffer + 1)
        })
    });
    bufmgr_seams::extend_buffered_rel_to::set(|_smgr, _fork, _strat, _flags, extend_to, mode| {
        with_fake(|f| {
            while f.pages.len() < extend_to as usize {
                f.pages.push(new_page());
                f.pins.push(0);
                f.locks.push(0);
            }
            f.pins[(extend_to - 1) as usize] += 1;
            if mode != types_storage::ReadBufferMode::Normal {
                f.locks[(extend_to - 1) as usize] += 1;
            }
            Ok(extend_to as Buffer)
        })
    });
    bufmgr_seams::buffer_page_is_new::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.is_new()
    });
    bufmgr_seams::buffer_page_get_lsn::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::buffer_page_set_lsn::set(|buf, lsn| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page; replay is single-threaded.
        let mut pm =
            unsafe { PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
        pm.set_lsn(lsn);
    });
    bufmgr_seams::flush_one_buffer::set(|_| Ok(()));
    xlogrecovery_seams::reached_consistency::set(|| false);
}

fn install_proc_boot_seams() {
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
    lock_seams::lock_release_all::set(|_, _| Ok(()));
    lock_seams::lock_release::set(|_, _, _| Ok(true));
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });
    timeout_seams::disable_timeouts::set(|_| {});
    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    sync_seams::register_sync_request::set(|_, _, _| Ok(true));
    parallel_seams::is_parallel_worker::set(|| false);
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
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
    xlogreader::init_seams();
    xlogutils::init_seams();
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
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    if resowner::CurrentResourceOwner().is_null() {
        let owner = resowner::ResourceOwnerCreate(
            types_resowner::ResourceOwner::NULL,
            "heap-redo-roundtrip",
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

fn write_control_file(dir: &std::path::Path) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = SEG as u64 + 40;
    cf.checkPointCopy.redo = SEG as u64 + 40;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, XID);
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

struct SegFileRead {
    wal_dir: std::path::PathBuf,
}

impl XLogSegmentRoutine for SegFileRead {
    fn segment_open(
        &mut self,
        _v: &mut ReaderView,
        _segno: XLogSegNo,
        _tli: &mut TimeLineID,
    ) -> PgResult<()> {
        unreachable!()
    }
    fn segment_close(&mut self, _v: &mut ReaderView) {}
}

impl XLogReaderRoutine for SegFileRead {
    fn page_read(
        &mut self,
        v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        _req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        let segno = target_page_ptr / SEG as u64;
        let off = (target_page_ptr % SEG as u64) as usize;
        let name = transam_xlog::XLogFileName(1, segno, SEG);
        let bytes = std::fs::read(self.wal_dir.join(name)).expect("segment readable");
        cur_page[..BLCKSZ].copy_from_slice(&bytes[off..off + BLCKSZ]);
        v.seg.ws_tli = 1;
        Ok(BLCKSZ as i32)
    }
}

// hoff 24, natts 1, data bytes follow; total = 24 + data.len().
fn raw_tuple(xmin: u32, ctid: (u32, u16), data: &[u8]) -> Vec<u8> {
    let mut img = vec![0u8; 24 + data.len()];
    img[0..4].copy_from_slice(&xmin.to_ne_bytes());
    img[12..14].copy_from_slice(&((ctid.0 >> 16) as u16).to_ne_bytes());
    img[14..16].copy_from_slice(&(ctid.0 as u16).to_ne_bytes());
    img[16..18].copy_from_slice(&ctid.1.to_ne_bytes());
    img[18..20].copy_from_slice(&1u16.to_ne_bytes());
    img[20..22].copy_from_slice(&HEAP_XMAX_INVALID.to_ne_bytes());
    img[22] = 24;
    img[24..].copy_from_slice(data);
    img
}

fn make_writable_tuple(img: &[u8]) -> HeapTupleData<'static> {
    let words = img.len().div_ceil(8);
    // Leaked (test-only): moving a Box would invalidate the derived pointer.
    let buf: &'static mut [u64] = Box::leak(vec![0u64; words].into_boxed_slice());
    // SAFETY: buf is words*8 >= img.len() writable bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(img.as_ptr(), buf.as_mut_ptr().cast::<u8>(), img.len())
    };
    // SAFETY: 8-aligned leaked image, header-complete, unique.
    unsafe {
        HeapTupleData::from_raw_parts(
            buf.as_mut_ptr().cast::<u8>(),
            img.len() as u32,
            ItemPointerData::invalid(),
            0,
        )
    }
}

// cmin/cmax are not WAL-logged; replay stamps FirstCommandId (C-exact), so
// t_field3 of tuples written with cid > 0 is excluded from the comparison.
fn zero_cid(page: &mut [u8], off: u16) {
    let r =
        // SAFETY: BLCKSZ page copy owned by the caller.
        unsafe { PageRef::from_raw(NonNull::new(page.as_mut_ptr()).unwrap()) };
    let lp = r.item_id(off);
    let o = lp.lp_off() as usize;
    page[o + 8..o + 12].fill(0);
}

fn insert_wal(rmid: u8, info: u8, main: &[u8], block: usize, flags: u8, bufdata: &[&[u8]]) -> u64 {
    let recptr = xloginsert_seams::xlog_insert_record::call(
        rmid,
        info,
        0,
        &[main],
        &[XLogRegBuf {
            block_id: 0,
            buffer: block as Buffer + 1,
            flags,
            bufdata,
        }],
    )
    .unwrap();
    page_mut(block).set_lsn(recptr);
    recptr
}

#[test]
fn heap_redo_rebuilds_pages_byte_exact() {
    let dir = std::env::temp_dir().join(format!("pgrust_heapxlog_redo_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_proc_boot_seams();
    install_real();
    install_fake_bufmgr();

    write_control_file(&dir);
    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();
    clog::BootStrapCLOG().unwrap();
    subtrans::BootStrapSUBTRANS().unwrap();

    let end_of_log: XLogRecPtr = 2 * SEG as u64;
    let prev_rec: XLogRecPtr = SEG as u64 + 40;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(prev_rec), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    xlogutils::set_in_recovery(false);
    procarray::TransamVariables()
        .nextXid
        .store(types_core::FullTransactionId::from_epoch_and_xid(0, XID).value, Relaxed);
    subtrans::StartupSUBTRANS(XID).unwrap();
    assert!(transam_xlog::XLogInsertAllowed());

    let ctx = MemoryContext::new("heap_redo_roundtrip");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);

    xact::StartTransactionCommand().unwrap();

    // Page 0: four wide inserts (first INIT_PAGE); free = 8168 - 4*1540 = 2008.
    for v in 1u8..=4 {
        let img = raw_tuple(0, (0, 0), &vec![v; WIDE - 24]);
        let mut tup = make_writable_tuple(&img);
        heap_insert(&rel, &mut tup, 0, 0, None).unwrap();
        assert_eq!(tup.t_self, ItemPointerData::new(0, v as u16));
    }
    assert_eq!(xact::GetTopTransactionIdIfAny(), XID);

    // HOT update (0,2) -> (0,5): 1540 <= 2008 free keeps it same-page.
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
    let mut update_indexes = tableam_vocab::TU_UpdateIndexes::TU_None;
    let img = raw_tuple(0, (0, 0), &vec![0x22; WIDE - 24]);
    let mut newtup = make_writable_tuple(&img);
    let r = heap_update(
        &rel,
        &ItemPointerData::new(0, 2),
        &mut newtup,
        1,
        None,
        true,
        &mut tmfd,
        &mut lockmode,
        &mut update_indexes,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    assert_eq!(newtup.t_self, ItemPointerData::new(0, 5));
    assert!(tuple_hdr(0, 2).is_hot_updated());

    // Non-HOT update (0,1) -> (1,1): 1904 > 468 free forces the cross-page
    // path (xl_heap_lock on the old tuple, then UPDATE|INIT_PAGE).
    let img = raw_tuple(0, (0, 0), &vec![0x33; 1900 - 24]);
    let mut newtup = make_writable_tuple(&img);
    let r = heap_update(
        &rel,
        &ItemPointerData::new(0, 1),
        &mut newtup,
        2,
        None,
        true,
        &mut tmfd,
        &mut lockmode,
        &mut update_indexes,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    assert_eq!(newtup.t_self, ItemPointerData::new(1, 1));
    assert!(!tuple_hdr(0, 1).is_hot_updated());

    let r = heap_delete(&rel, &ItemPointerData::new(0, 3), 3, None, true, &mut tmfd, false)
        .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);

    // Real row-lock producer (heap_lock_tuple, the SELECT FOR UPDATE shape).
    let (r, pin) = heapam::dml::heap_lock_tuple(
        &rel,
        &ItemPointerData::new(0, 4),
        4,
        LockTupleMode::LockTupleExclusive,
        tableam_vocab::LockWaitPolicy::LockWaitBlock,
        false,
        &mut tmfd,
    )
    .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);
    drop(pin);

    // Writer-driven speculative legs on page 1: insert+CONFIRM, then
    // insert+super-DELETE. The parked token is unlogged, so both must
    // resolve before the byte-compare (redo of the INSERT reconstructs
    // t_ctid = self); abort's prune hint is TransactionXmin, which must
    // equal the record xid the redo side stamps.
    procarray::set_transaction_xmin(XID);
    let img = raw_tuple(0, (0, 0), &[0x44; 8]);
    let mut spec1 = make_writable_tuple(&img);
    spec1.t_data_mut().set_speculative_token(4242);
    heap_insert(&rel, &mut spec1, 0, heapam::hio::HEAP_INSERT_SPECULATIVE, None).unwrap();
    assert_eq!(spec1.t_self, ItemPointerData::new(1, 2));
    assert!(tuple_hdr(1, 2).is_speculative());
    heapam::heap_finish_speculative(&rel, &spec1.t_self).unwrap();
    assert!(!tuple_hdr(1, 2).is_speculative());

    let img = raw_tuple(0, (0, 0), &[0x55; 8]);
    let mut spec2 = make_writable_tuple(&img);
    spec2.t_data_mut().set_speculative_token(4243);
    heap_insert(&rel, &mut spec2, 0, heapam::hio::HEAP_INSERT_SPECULATIVE, None).unwrap();
    assert_eq!(spec2.t_self, ItemPointerData::new(1, 3));
    heapam::heap_abort_speculative(&rel, &spec2.t_self).unwrap();
    assert_eq!(tuple_hdr(1, 3).xmin_raw(), 0);

    // Page 2 carries the C-writer record shapes, applied to the write-side
    // page exactly as C's write paths would and hand-encoded per heapam_xlog.h.
    with_fake(|f| {
        f.pages.push(new_page());
        f.pins.push(0);
        f.locks.push(0);
    });
    page_mut(2).init(0);

    let tuples: Vec<Vec<u8>> = (1u16..=5)
        .map(|i| raw_tuple(XID, (2, i), &[i as u8, 2, 3, 4, 5, 6, 7]))
        .collect();
    // datalen 8 makes each xl_multi_insert_tuple entry 15 bytes: the next
    // entry needs C's SHORTALIGN pad.
    let encode_multi = |imgs: &[Vec<u8>]| -> Vec<u8> {
        let mut d = Vec::new();
        for img in imgs {
            if d.len() % 2 != 0 {
                d.push(0);
            }
            let datalen = (img.len() - SizeofHeapTupleHeader) as u16;
            d.extend_from_slice(&datalen.to_ne_bytes());
            d.extend_from_slice(&img[18..20]); // t_infomask2
            d.extend_from_slice(&img[20..22]); // t_infomask
            d.push(img[22]); // t_hoff
            d.extend_from_slice(&img[SizeofHeapTupleHeader..]);
        }
        d
    };

    {
        let mut pm = page_mut(2);
        for (i, img) in tuples[..3].iter().enumerate() {
            pm.add_item(img, i as u16 + 1, PAI_OVERWRITE | PAI_IS_HEAP).unwrap();
        }
        let mut main = vec![0u8; 4];
        main[2..4].copy_from_slice(&3u16.to_ne_bytes());
        let data = encode_multi(&tuples[..3]);
        insert_wal(
            RM_HEAP2_ID,
            XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE,
            &main,
            2,
            REGBUF_STANDARD | REGBUF_WILL_INIT,
            &[&data],
        );
    }
    {
        let mut pm = page_mut(2);
        for (i, img) in tuples[3..].iter().enumerate() {
            pm.add_item(img, i as u16 + 4, PAI_OVERWRITE | PAI_IS_HEAP).unwrap();
        }
        let mut main = vec![0u8; 8];
        main[2..4].copy_from_slice(&2u16.to_ne_bytes());
        main[4..6].copy_from_slice(&4u16.to_ne_bytes());
        main[6..8].copy_from_slice(&5u16.to_ne_bytes());
        let data = encode_multi(&tuples[3..]);
        insert_wal(RM_HEAP2_ID, XLOG_HEAP2_MULTI_INSERT, &main, 2, REGBUF_STANDARD, &[&data]);
    }

    // XLOG_HEAP2_LOCK_UPDATED on (2,2): key-share lock an updated version.
    {
        let htup = tuple_hdr(2, 2);
        htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
        htup.t_infomask |= HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_LOCK_ONLY;
        htup.set_xmax(77);
        let mut main = [0u8; 8];
        main[0..4].copy_from_slice(&77u32.to_ne_bytes());
        main[4..6].copy_from_slice(&2u16.to_ne_bytes());
        main[6] = XLHL_XMAX_LOCK_ONLY | XLHL_XMAX_KEYSHR_LOCK;
        insert_wal(RM_HEAP2_ID, XLOG_HEAP2_LOCK_UPDATED, &main, 2, REGBUF_STANDARD, &[]);
    }

    // XLOG_HEAP_CONFIRM on (2,3): the write side parked a speculative token
    // in t_ctid (unlogged, as C does); confirm resets it to self.
    {
        tuple_hdr(2, 3).set_speculative_token(4242);
        tuple_hdr(2, 3).t_ctid = ItemPointerData::new(2, 3);
        let mut main = [0u8; 2];
        main[0..2].copy_from_slice(&3u16.to_ne_bytes());
        insert_wal(RM_HEAP_ID, XLOG_HEAP_CONFIRM, &main, 2, REGBUF_STANDARD, &[]);
    }

    // Prefix/suffix-compressed same-page UPDATE (2,1) -> (2,6): data
    // [1,2,3,4,5,6,7] -> [1,2,9,9,5,6,7], prefix 2 and suffix 3 elided.
    {
        let newtid = ItemPointerData::new(2, 6);
        let htup = tuple_hdr(2, 1);
        htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
        htup.clear_hot_updated();
        htup.t_infomask2 |= HEAP_KEYS_UPDATED;
        htup.set_xmax(XID);
        htup.set_cmax(0, false);
        htup.t_ctid = newtid;
        let mut pm = page_mut(2);
        pm.set_prune_xid(XID);

        let mut newimg = raw_tuple(XID, (2, 6), &[1, 2, 9, 9, 5, 6, 7]);
        newimg[20..22]
            .copy_from_slice(&(HEAP_XMAX_INVALID | HEAP_UPDATED).to_ne_bytes());
        pm.add_item(&newimg, 6, PAI_OVERWRITE | PAI_IS_HEAP).unwrap();

        let mut main = [0u8; 14];
        main[0..4].copy_from_slice(&XID.to_ne_bytes());
        main[4..6].copy_from_slice(&1u16.to_ne_bytes());
        main[6] = XLHL_KEYS_UPDATED;
        main[7] = XLH_UPDATE_PREFIX_FROM_OLD | XLH_UPDATE_SUFFIX_FROM_OLD;
        main[12..14].copy_from_slice(&6u16.to_ne_bytes());

        let mut data = Vec::new();
        data.extend_from_slice(&2u16.to_ne_bytes()); // prefixlen
        data.extend_from_slice(&3u16.to_ne_bytes()); // suffixlen
        data.extend_from_slice(&newimg[18..20]);
        data.extend_from_slice(&newimg[20..22]);
        data.push(newimg[22]);
        data.push(newimg[23]); // bitmap/padding chunk (hoff - header)
        data.extend_from_slice(&[9, 9]); // the unshared middle
        insert_wal(RM_HEAP_ID, XLOG_HEAP_UPDATE, &main, 2, REGBUF_STANDARD, &[&data]);
    }

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });

    let nblocks = with_fake(|f| f.pages.len());
    assert_eq!(nblocks, 3);
    let expected: Vec<[u8; BLCKSZ]> = (0..nblocks).map(page_bytes).collect();
    let last_lsn = expected
        .iter()
        .map(|p| {
            // SAFETY: stack copy of a page image.
            unsafe { PageRef::from_raw(NonNull::new(p.as_ptr().cast_mut()).unwrap()) }.lsn()
        })
        .max()
        .unwrap();
    transam_xlog::XLogFlush(last_lsn).unwrap();

    // Wipe the storage: replay must rebuild every block purely from WAL.
    with_fake(|f| {
        f.pages.clear();
        f.pins.clear();
        f.locks.clear();
    });
    xlogutils::set_in_recovery(true);

    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };
    reader.XLogBeginRead(end_of_log + 40);

    let mut heap_seen = [0u32; 8];
    let mut heap2_seen = [0u32; 8];
    while reader.v.EndRecPtr < last_lsn {
        reader.XLogReadRecord(&mut routine).unwrap().unwrap();
        let rmid = reader.XLogRecGetRmid();
        let op = ((reader.XLogRecGetInfo() & XLOG_HEAP_OPMASK) >> 4) as usize;
        match rmid {
            x if x == RM_HEAP_ID => heap_seen[op] += 1,
            x if x == RM_HEAP2_ID => heap2_seen[op] += 1,
            // The stream also carries xid-assignment periphery (clog);
            // replay it through the same rmgr dispatch.
            _ => {}
        }
        (rmgr::GetRmgr(rmid).unwrap().rm_redo)(&mut reader.v).unwrap();
    }
    assert_eq!(reader.v.EndRecPtr, last_lsn);

    assert_eq!(heap_seen[(XLOG_HEAP_INSERT >> 4) as usize], 6, "INSERT x6");
    assert_eq!(heap_seen[(XLOG_HEAP_HOT_UPDATE >> 4) as usize], 1, "HOT_UPDATE");
    assert_eq!(heap_seen[(XLOG_HEAP_LOCK >> 4) as usize], 2, "LOCK x2");
    assert_eq!(heap_seen[(XLOG_HEAP_UPDATE >> 4) as usize], 2, "UPDATE x2");
    assert_eq!(heap_seen[(XLOG_HEAP_DELETE >> 4) as usize], 2, "DELETE x2 (one super)");
    assert_eq!(heap_seen[(XLOG_HEAP_CONFIRM >> 4) as usize], 2, "CONFIRM x2");
    assert_eq!(heap2_seen[(XLOG_HEAP2_MULTI_INSERT >> 4) as usize], 2, "MULTI_INSERT x2");
    assert_eq!(heap2_seen[(XLOG_HEAP2_LOCK_UPDATED >> 4) as usize], 1, "LOCK_UPDATED");

    with_fake(|f| assert!(f.pins.iter().all(|p| *p == 0), "replay leaked pins"));
    assert_eq!(with_fake(|f| f.pages.len()), nblocks);

    let cid_tuples: [&[(usize, u16)]; 3] =
        [&[(0, 1), (0, 2), (0, 3), (0, 4), (0, 5)], &[(1, 1), (1, 2), (1, 3)], &[]];
    for blk in 0..nblocks {
        let mut got = page_bytes(blk).to_vec();
        let mut want = expected[blk].to_vec();
        for &(b, off) in cid_tuples[blk] {
            assert_eq!(b, blk);
            zero_cid(&mut got, off);
            zero_cid(&mut want, off);
        }
        // PD_PAGE_FULL is a writer-side hint, not WAL-logged; C redo leaves
        // it unset too.
        got[10] &= !0x02;
        want[10] &= !0x02;
        if got != want {
            let first = got.iter().zip(&want).position(|(a, b)| a != b).unwrap();
            panic!(
                "replayed block {blk} differs at byte {first}: got {:02x?} want {:02x?}",
                &got[first..(first + 16).min(BLCKSZ)],
                &want[first..(first + 16).min(BLCKSZ)]
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
