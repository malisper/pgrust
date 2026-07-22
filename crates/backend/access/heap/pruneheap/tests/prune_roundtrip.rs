// M4 composition proof for pruning: build a dead HOT chain through the
// committed heapam DML (insert + HOT updates + delete over a fake bufmgr),
// commit and advance the horizon through the real procarray, run
// heap_page_prune_opt through its real guards, then verify: pruned page bytes
// vs the C reference TU (bench/cref/prune_page_ref.c), the
// XLOG_HEAP2_PRUNE_ON_ACCESS record decoded off disk with the real xlogreader,
// and a visibilitymap set/clear round trip through the real WAL path.
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::Mutex;

use heapam::{heap_delete, heap_insert, heap_update};
use heaptuple::heap_form_tuple;
use mcx::{Mcx, MemoryContext, PgVec};
use tableam_vocab::{LockTupleMode, TM_FailureData, TM_Result, TU_UpdateIndexes};
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
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::bufpage::{PageMut, PageRef, LP_DEAD, LP_NORMAL, LP_REDIRECT, LP_UNUSED};
use types_storage::RelFileLocator;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleHeaderData, ItemPointerData, NameData,
    TupleDescData,
};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AADD;
const REL_OID: Oid = 61001;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const COMMITTED_XID: u32 = 3;
const ABORTED_XID: u32 = 4;
const UPDATER_XID: u32 = 5;

const RM_HEAP2_ID: u8 = rmgr::RmgrIds::RM_HEAP2_ID as u8;

static CURRENT_XID: AtomicU32 = AtomicU32::new(COMMITTED_XID);

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>,    // main-fork pages; index = buffer - 1
    vm_pages: Vec<usize>, // VM-fork pages; buffer = 100 + blkno
    pins: Vec<i32>,
    locks: Vec<i32>,
    vm_pins: Vec<i32>,
    vm_locks: Vec<i32>,
    cleanup_locked: bool,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    pages: Vec::new(),
    vm_pages: Vec::new(),
    pins: Vec::new(),
    locks: Vec::new(),
    vm_pins: Vec::new(),
    vm_locks: Vec::new(),
    cleanup_locked: false,
});

const VM_BUF_BASE: Buffer = 100;

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn fake_page_addr(f: &Fake, buf: Buffer) -> usize {
    if buf >= VM_BUF_BASE {
        f.vm_pages[(buf - VM_BUF_BASE) as usize]
    } else {
        f.pages[(buf - 1) as usize]
    }
}

fn install_seams() {
    bufmgr_seams::read_buffer::set(|_rel, block| {
        with_fake(|f| {
            assert!((block as usize) < f.pages.len());
            f.pins[block as usize] += 1;
            Ok(block as Buffer + 1)
        })
    });
    bufmgr_seams::read_buffer_extended::set(|_rel, fork, block, _mode, _strategy| {
        assert_eq!(fork, ForkNumber::VISIBILITYMAP_FORKNUM);
        with_fake(|f| {
            assert!((block as usize) < f.vm_pages.len());
            f.vm_pins[block as usize] += 1;
            Ok(VM_BUF_BASE + block as Buffer)
        })
    });
    bufmgr_seams::relation_smgr_locator::set(|_rel| types_storage::RelFileLocatorBackend {
        locator: RLOC,
        backend: INVALID_PROC_NUMBER,
    });
    smgr_seams::smgr_cached_nblocks::set(|_rloc, _fork| {
        with_fake(|f| f.vm_pages.len() as BlockNumber)
    });
    smgr_seams::smgr_exists::set(|_rloc, _fork| with_fake(|f| Ok(!f.vm_pages.is_empty())));
    smgr_seams::smgr_nblocks::set(|_rloc, _fork| {
        with_fake(|f| Ok(f.vm_pages.len() as BlockNumber))
    });
    smgr_seams::smgr_set_cached_nblocks::set(|_rloc, _fork, _v| Ok(()));
    bufmgr_seams::buffer_get_block_number::set(|buf| {
        if buf >= VM_BUF_BASE { (buf - VM_BUF_BASE) as BlockNumber } else { (buf - 1) as BlockNumber }
    });
    bufmgr_seams::buffer_get_page::set(|buf| {
        let addr = with_fake(|f| fake_page_addr(f, buf));
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::release_buffer::set(|buf| {
        with_fake(|f| {
            let p = if buf >= VM_BUF_BASE {
                &mut f.vm_pins[(buf - VM_BUF_BASE) as usize]
            } else {
                &mut f.pins[(buf - 1) as usize]
            };
            assert!(*p > 0, "double release of buffer {buf}");
            *p -= 1;
        });
        Ok(())
    });
    bufmgr_seams::incr_buffer_ref_count::set(|buf| {
        with_fake(|f| f.pins[(buf - 1) as usize] += 1);
    });
    bufmgr_seams::lock_buffer::set(|buf, mode| {
        with_fake(|f| {
            let l = if buf >= VM_BUF_BASE {
                &mut f.vm_locks[(buf - VM_BUF_BASE) as usize]
            } else {
                &mut f.locks[(buf - 1) as usize]
            };
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(*l > 0, "unlock without lock");
                    *l -= 1;
                    if buf < VM_BUF_BASE {
                        f.cleanup_locked = false;
                    }
                }
                _ => {
                    assert_eq!(*l, 0, "double content lock");
                    *l += 1;
                }
            }
        });
        Ok(())
    });
    bufmgr_seams::conditional_lock_buffer_for_cleanup::set(|buf| {
        Ok(with_fake(|f| {
            let l = &mut f.locks[(buf - 1) as usize];
            if *l != 0 || f.pins[(buf - 1) as usize] != 1 {
                return false;
            }
            *l += 1;
            f.cleanup_locked = true;
            true
        }))
    });
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let addr = with_fake(|f| fake_page_addr(f, buf));
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
            let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(1);
            f.locks.push(1);
            (f.pages.len() as Buffer, 1)
        }))
    });

    xact_seams::get_current_transaction_id::set(|| Ok(CURRENT_XID.load(Relaxed)));
    xact_seams::is_in_parallel_mode::set(|| false);
    // DML always runs as "another backend's committed xact" here: no xid is
    // ever current, so combo-cid arms stay cold (wal_roundtrip precedent).
    xact_seams::transaction_id_is_current_transaction_id::set(|_| false);
    xact_seams::mark_current_transaction_id_logged_if_any::set(|| {});
    xact_seams::get_current_sub_transaction_id::set(|| 1);

    transam_seams::transaction_id_did_commit::set(|xid| Ok(xid == COMMITTED_XID || xid == UPDATER_XID));
    transam_seams::transaction_id_get_commit_lsn::set(|_| Ok(0));
    subtrans_seams::sub_trans_get_topmost_transaction::set(Ok);
    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(InvalidBlockNumber)
    });
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    catalog_seams::is_catalog_relation::set(|_rel| false);
    origin_seams::replorigin_session_origin::set(|| 0);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));

    // The horizon: GlobalVisTestIsRemovableXid answers from the real
    // procarray's RecentXmin, advanced by taking snapshots (GetSnapshotData
    // updates the GlobalVis horizons alongside it).
    procarray_seams::global_vis_test_for::set(|_rel| types_core::GlobalVisStateHandle::new(3));
    procarray_seams::global_vis_test_is_removable_xid::set(|_vistest, xid| {
        Ok(types_core::xact::TransactionIdPrecedes(xid, procarray::RecentXmin()))
    });

    // xloginsert marshal for fake buffers (wal_roundtrip precedent).
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let (addr, fork, block) = with_fake(|f| {
                if b.buffer >= VM_BUF_BASE {
                    (
                        f.vm_pages[(b.buffer - VM_BUF_BASE) as usize],
                        ForkNumber::VISIBILITYMAP_FORKNUM,
                        (b.buffer - VM_BUF_BASE) as BlockNumber,
                    )
                } else {
                    (
                        f.pages[(b.buffer - 1) as usize],
                        ForkNumber::MAIN_FORKNUM,
                        (b.buffer - 1) as BlockNumber,
                    )
                }
            });
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RLOC,
                forknum: fork,
                block,
                // SAFETY: leaked test page, BLCKSZ, pinned by the caller.
                page: unsafe { core::slice::from_raw_parts(addr as *const u8, BLCKSZ) },
                flags: b.flags,
                bufdata: b.bufdata,
            });
        }
        xloginsert::insert_record(rmid, info, flags, main_data, &blocks)
    });
}

fn install_proc_boot_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(781);

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
    timeout_seams::disable_timeouts::set(|_| {});
}

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_INT,
        attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
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
        relam: ::tableam_vocab::HEAP_TABLE_AM_OID,
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
    // nextXid 6: xids 3..5 are assigned (completed or aborted) at boot, so
    // latestCompletedXid covers them and only the procarray governs progress.
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 6);
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

fn page0_ref() -> PageRef<'static> {
    let addr = with_fake(|f| f.pages[0]);
    // SAFETY: leaked test page, always live.
    unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }
}

fn page0_mut() -> PageMut<'static> {
    let addr = with_fake(|f| f.pages[0]);
    // SAFETY: leaked test page; single-threaded test.
    unsafe { PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap()) }
}

fn page0_header(off: u16) -> &'static HeapTupleHeaderData {
    let page = page0_ref();
    let id = page.item_id(off);
    let (ptr, _) = page.item_raw(id);
    // SAFETY: LP_NORMAL in-page tuple.
    unsafe { &*ptr.cast::<HeapTupleHeaderData>() }
}

fn mvcc_snapshot<'m>(mcx: Mcx<'m>) -> SnapshotData<'m> {
    let s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    s.regd_count.set(1);
    s
}

#[test]
fn prune_dead_hot_chain_wal_and_page_parity() {
    let dir = std::env::temp_dir().join(format!("pgrust_prune_wal_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_proc_boot_seams();
    shmem::init_seams();
    guc_tables::init_seams();
    transam_xlog::init_seams();
    xlogutils::init_seams();
    heapam_visibility::init_seams();
    pruneheap::init_seams();
    install_seams();
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    procarray::init_seams();
    varsup::VarsupShmemInit();
    // StartupXLOG's latestCompletedXid = nextXid - 1 seeding: xids 3..5 are
    // assigned; the procarray alone governs their progress.
    {
        use std::sync::atomic::Ordering::Relaxed as R;
        let tv = procarray::TransamVariables();
        tv.nextXid.store(types_core::FullTransactionId::from_epoch_and_xid(0, 6).value, R);
        tv.latestCompletedXid
            .store(types_core::FullTransactionId::from_epoch_and_xid(0, 5).value, R);
    }
    procarray::ProcArrayShmemInit();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    let myproc = lmgr_proc::MyProc().unwrap();

    write_control_file(&dir);
    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

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
    assert!(transam_xlog::XLogInsertAllowed());

    let ctx = MemoryContext::new("prune_roundtrip");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);
    let tupdesc = int4_tupdesc(mcx);

    for (i, val) in [41i32, 42, 43, 44].iter().enumerate() {
        CURRENT_XID.store(if i == 3 { ABORTED_XID } else { COMMITTED_XID }, Relaxed);
        let mut tup =
            heap_form_tuple(mcx, &tupdesc, &[::datum::Datum::from_i32(*val)], &[false]).unwrap();
        heap_insert(&rel, tup.as_tuple_mut(), 7, 0, None).unwrap();
        assert_eq!(tup.as_tuple().t_self, ItemPointerData::new(0, (i + 1) as u16));
    }

    // Dead HOT chain: (0,1) -> (0,5) -> (0,6); versions 1 and 5 die. The
    // updater/deleter is a distinct committed xid so the removed tuples carry
    // a real conflict horizon (xmax != xmin).
    CURRENT_XID.store(UPDATER_XID, Relaxed);
    let mut tmfd = TM_FailureData::default();
    for (cid, (old_off, val)) in [(1u16, 141i32), (5, 241)].iter().enumerate() {
        let mut newtup =
            heap_form_tuple(mcx, &tupdesc, &[::datum::Datum::from_i32(*val)], &[false]).unwrap();
        let mut lockmode = LockTupleMode::LockTupleNoKeyExclusive;
        let mut update_indexes = TU_UpdateIndexes::TU_None;
        let r = heap_update(
            &rel,
            &ItemPointerData::new(0, *old_off),
            newtup.as_tuple_mut(),
            8 + cid as u32,
            None,
            true,
            &mut tmfd,
            &mut lockmode,
            &mut update_indexes,
        )
        .unwrap();
        assert_eq!(r, TM_Result::TM_Ok);
        assert_eq!(update_indexes, TU_UpdateIndexes::TU_None); // HOT
        assert_eq!(newtup.as_tuple().t_self, ItemPointerData::new(0, 5 + cid as u16));
    }
    assert!(page0_header(1).is_hot_updated());
    assert!(page0_header(5).is_heap_only());
    assert!(page0_header(6).is_heap_only());

    let r = heap_delete(&rel, &ItemPointerData::new(0, 2), 10, None, true, &mut tmfd, false)
        .unwrap();
    assert_eq!(r, TM_Result::TM_Ok);

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });

    // Register the updater xid as running in the real procarray: the horizon
    // stays behind it and the guard chain must refuse to prune.
    lmgr_proc::GetPGProcByNumber(myproc).xid.value.store(UPDATER_XID, Relaxed);
    procarray::ProcArrayAdd(myproc).unwrap();
    let buf: Buffer = bufmgr_seams::read_buffer::call(&rel, 0).unwrap();
    {
        let mut pm = page0_mut();
        pm.set_full(); // C: an UPDATE that couldn't fit marks the page full
    }
    assert_eq!(page0_ref().prune_xid(), UPDATER_XID);
    {
        let mut snap = mvcc_snapshot(mcx);
        procarray::GetSnapshotData(&mut snap, mcx).unwrap();
    }
    assert_eq!(procarray::RecentXmin(), UPDATER_XID); // xid 5 still running
    pruneheap::heap_page_prune_opt(&rel, buf).unwrap();
    assert_eq!(page0_ref().item_id(1).lp_flags(), LP_NORMAL, "pruned too early");

    // Commit xid 5 (latestCompletedXid then covers 3, 4 and 5), then advance
    // the horizon by taking a fresh snapshot (RecentXmin moves past them).
    procarray::ProcArrayEndTransaction(myproc, UPDATER_XID).unwrap();
    {
        let mut snap = mvcc_snapshot(mcx);
        procarray::GetSnapshotData(&mut snap, mcx).unwrap();
    }
    assert!(types_core::xact::TransactionIdPrecedes(
        COMMITTED_XID,
        procarray::RecentXmin()
    ));

    // The real deal: through the guards, cleanup lock, prune, WAL.
    pruneheap::heap_page_prune_opt(&rel, buf).unwrap();

    let page = page0_ref();
    assert_eq!(page.max_offset_number(), 6);
    assert_eq!(page.item_id(1).lp_flags(), LP_REDIRECT);
    assert_eq!(page.item_id(1).lp_off(), 6);
    assert_eq!(page.item_id(2).lp_flags(), LP_DEAD);
    assert_eq!(page.item_id(3).lp_flags(), LP_NORMAL);
    assert_eq!(page.item_id(4).lp_flags(), LP_DEAD);
    assert_eq!(page.item_id(5).lp_flags(), LP_UNUSED);
    assert_eq!(page.item_id(6).lp_flags(), LP_NORMAL);
    assert_eq!(page.prune_xid(), 0);
    assert!(!page.is_full());
    assert!(page.has_free_line_pointers());
    assert_eq!(page.pd_upper(), (BLCKSZ - 2 * 32) as u16);
    with_fake(|f| {
        assert!(!f.cleanup_locked);
        assert!(f.locks.iter().all(|l| *l == 0));
    });

    // Page bytes == the C reference page (pd_lsn compared separately).
    let last_lsn = page.lsn();
    assert_ne!(last_lsn, 0);
    let mut got = vec![0u8; BLCKSZ];
    // SAFETY: leaked test page.
    got.copy_from_slice(unsafe {
        core::slice::from_raw_parts(with_fake(|f| f.pages[0]) as *const u8, BLCKSZ)
    });
    got[0..8].fill(0);
    // Ground-truth dump for regenerating bench/cref/prune_page_ref.c.
    std::fs::write(std::env::temp_dir().join("pgrust_prune_page_rust.bin"), &got).unwrap();
    let want: &[u8] = include_bytes!("fixtures/prune_page_c.bin");
    assert_eq!(want.len(), BLCKSZ);
    if got != want {
        let first = got.iter().zip(want).position(|(a, b)| a != b).unwrap();
        panic!(
            "page differs from C reference at byte {first}: rust {:02x?} c {:02x?}",
            &got[first..(first + 16).min(BLCKSZ)],
            &want[first..(first + 16).min(BLCKSZ)]
        );
    }

    transam_xlog::XLogFlush(last_lsn).unwrap();

    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };
    reader.XLogBeginRead(end_of_log + 40);
    for _ in 0..7 {
        reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    }
    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.XLogRecGetRmid(), RM_HEAP2_ID);
    assert_eq!(
        reader.XLogRecGetInfo() & !0x0F,
        pruneheap::XLOG_HEAP2_PRUNE_ON_ACCESS
    );
    let main = reader.XLogRecGetData();
    assert_eq!(main.len(), 6);
    assert_eq!(main[0], pruneheap::PruneReason::PruneOnAccess as u8);
    assert_eq!(
        main[1],
        pruneheap::XLHP_CLEANUP_LOCK
            | pruneheap::XLHP_HAS_CONFLICT_HORIZON
            | pruneheap::XLHP_HAS_REDIRECTIONS
            | pruneheap::XLHP_HAS_DEAD_ITEMS
            | pruneheap::XLHP_HAS_NOW_UNUSED_ITEMS
    );
    assert_eq!(
        u32::from_ne_bytes(main[2..6].try_into().unwrap()),
        UPDATER_XID
    );
    let (loc, fork, blk, _) = reader.XLogRecGetBlockTagExtended(0).unwrap();
    assert_eq!((loc, fork, blk), (RLOC, ForkNumber::MAIN_FORKNUM, 0));
    let bd = reader.XLogRecGetBlockData(0).unwrap();
    let words: Vec<u16> = bd
        .chunks_exact(2)
        .map(|c| u16::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    assert_eq!(words, vec![1, 1, 6, 2, 2, 4, 1, 5]);
    assert_eq!(reader.v.EndRecPtr, last_lsn);

    // VM set/clear round-trips with WAL: mark the page all-visible, set the
    // VM bit through the real xloginsert, decode it, then clear.
    with_fake(|f| {
        let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
        // SAFETY: fresh leaked page.
        unsafe { PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.init(0);
        f.vm_pages.push(addr);
        f.vm_pins.push(0);
        f.vm_locks.push(0);
    });
    page0_mut().set_all_visible();

    let mut vmbuf = visibilitymap::VmBuffer::new();
    visibilitymap::visibilitymap_pin(&rel, 0, &mut vmbuf).unwrap();
    let prev = visibilitymap::visibilitymap_set(
        &rel,
        0,
        buf,
        0,
        &vmbuf,
        9,
        visibilitymap::VISIBILITYMAP_ALL_VISIBLE,
    )
    .unwrap();
    assert_eq!(prev, 0);
    assert_eq!(
        visibilitymap::visibilitymap_get_status(&rel, 0, &mut vmbuf).unwrap(),
        visibilitymap::VISIBILITYMAP_ALL_VISIBLE
    );

    let vm_lsn = {
        let addr = with_fake(|f| f.vm_pages[0]);
        // SAFETY: leaked page.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    };
    assert_ne!(vm_lsn, 0);
    transam_xlog::XLogFlush(vm_lsn).unwrap();
    // Fresh reader: the first one cached the WAL page from before this record
    // was written.
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    reader.XLogBeginRead(last_lsn);
    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.XLogRecGetRmid(), RM_HEAP2_ID);
    assert_eq!(reader.XLogRecGetInfo() & !0x0F, 0x40); // XLOG_HEAP2_VISIBLE
    let main = reader.XLogRecGetData();
    assert_eq!(main.len(), 5); // xl_heap_visible { horizon; flags }
    assert_eq!(u32::from_ne_bytes(main[0..4].try_into().unwrap()), 9);
    assert_eq!(main[4], visibilitymap::VISIBILITYMAP_ALL_VISIBLE);
    let (_, fork, blk, _) = reader.XLogRecGetBlockTagExtended(0).unwrap();
    assert_eq!((fork, blk), (ForkNumber::VISIBILITYMAP_FORKNUM, 0));
    let (_, fork, blk, _) = reader.XLogRecGetBlockTagExtended(1).unwrap();
    assert_eq!((fork, blk), (ForkNumber::MAIN_FORKNUM, 0));
    assert_eq!(reader.v.EndRecPtr, vm_lsn);

    assert!(visibilitymap::visibilitymap_clear(
        &rel,
        0,
        &vmbuf,
        visibilitymap::VISIBILITYMAP_VALID_BITS
    )
    .unwrap());
    assert_eq!(visibilitymap::visibilitymap_get_status(&rel, 0, &mut vmbuf).unwrap(), 0);
    vmbuf.release();

    bufmgr_seams::release_buffer::call(buf).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
