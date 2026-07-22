// Write-path composition proof: btinsert over a fake bufmgr, WAL through the
// real xloginsert/transam_xlog, decoded off disk with the real xlogreader.
// The XLOG_BTREE_SPLIT record's right-page item stream is replayed
// (_bt_restore_page semantics) and compared byte-for-byte against the live
// right page; both NEWROOT records and the leaf-insert stream are decoded.
// Not miri-runnable: XLogFileInit does real segment file I/O.
#![cfg(not(miri))]
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use mcx::{MemoryContext, Mcx, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, ForkNumber, Oid, TimeLineID,
    XLogRecPtr, XLogSegNo, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use types_nbtree::genam::IndexUniqueCheck;
use types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_META, BTREE_MAGIC, BTREE_VERSION, P_NONE,
    XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_NEWROOT, XLOG_BTREE_SPLIT_R,
};
use types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX,
};
use types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use types_storage::RelFileLocator;
use types_tuple::itemptr::ItemPointerData;
use types_tuple::tupdesc::CompactAttribute;
use types_tuple::{NameData, TupleDescData};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AABB;
const REL_OID: Oid = 62000;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RM_BTREE_ID: u8 = rmgr::RmgrIds::RM_BTREE_ID as u8;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>,
    pins: Vec<i32>,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    pages: Vec::new(),
    pins: Vec::new(),
});

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

// 8-aligned copy: PageRef reads (lsn) require page alignment.
fn page_bytes(block: usize) -> TestPage {
    let addr = with_fake(|f| f.pages[block]);
    // SAFETY: leaked test page, always live.
    TestPage(unsafe { *(addr as *const [u8; BLCKSZ]) })
}

fn install_seams() {
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
    bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
    bufmgr_seams::conditional_lock_buffer::set(|_buf| Ok(true));
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::extend_buffered_rel_by::set(|_rel, _fork, _strategy, flags, extend_by| {
        assert_eq!(extend_by, 1);
        assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
        Ok(with_fake(|f| {
            let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(1);
            (f.pages.len() as Buffer, 1)
        }))
    });
    bufmgr_seams::relation_smgr_locator::set(|_rel| ::types_storage::RelFileLocatorBackend {
        locator: RLOC,
        backend: INVALID_PROC_NUMBER,
    });
    smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| 0);
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
    smgr_seams::smgr_exists::set(|_loc, _fork| Ok(false));

    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));

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
    origin_seams::replorigin_session_origin::set(|| 0);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    xact_seams::mark_current_transaction_id_logged_if_any::set(|| {});
    xact_seams::get_current_sub_transaction_id::set(|| 1);
}

fn int4_tupdesc(mcx: Mcx<'_>) -> Rc<TupleDescData<'_>> {
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute {
        attcacheoff: Cell::new(-1),
        attlen: 4,
        attbyval: true,
        attispackable: false,
        atthasmissing: false,
        attisdropped: false,
        attgenerated: false,
        attnullability: 0,
        attalignby: 4,
    });
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs: PgVec::new_in(mcx),
    })
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
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
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: types_core::BTREE_AM_OID,
            relfilenode: REL_OID,
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
            indexrelid: REL_OID,
            indrelid: REL_OID - 1,
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
    Relation::open(data, Some(noop_close))
}

fn test_int4cmp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<::datum::Datum> {
    let a = fcinfo.arg(0).as_i32();
    let b = fcinfo.arg(1).as_i32();
    Ok(::datum::Datum::from_i32((a > b) as i32 - (a < b) as i32))
}

fn empty_meta_page() -> Box<TestPage> {
    let mut p = Box::new(TestPage([0u8; BLCKSZ]));
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    p.0[12..14].copy_from_slice(&(SizeOfPageHeaderData as u16).to_ne_bytes());
    p.0[14..16].copy_from_slice(&(special as u16).to_ne_bytes());
    p.0[16..18].copy_from_slice(&(special as u16).to_ne_bytes());
    p.0[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
    // SAFETY: owned page, in-bounds aligned writes.
    unsafe {
        p.0.as_mut_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<BTMetaPageData>()
            .write(BTMetaPageData {
                btm_magic: BTREE_MAGIC,
                btm_version: BTREE_VERSION,
                btm_root: P_NONE,
                btm_level: 0,
                btm_fastroot: P_NONE,
                btm_fastlevel: 0,
                btm_last_cleanup_num_delpages: 0,
                btm_last_cleanup_num_heap_tuples: -1.0,
                btm_allequalimage: false,
            });
        p.0.as_mut_ptr()
            .add(special)
            .cast::<BTPageOpaqueData>()
            .write(BTPageOpaqueData {
                btpo_prev: P_NONE,
                btpo_next: P_NONE,
                btpo_level: 0,
                btpo_flags: BTP_META,
                btpo_cycleid: 0,
            });
    }
    p
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
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
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

// _bt_restore_page: replay a right-page/root item stream into a fresh page.
fn restore_page(items: &[u8], opaque_from: &[u8; BLCKSZ], lsn: XLogRecPtr) -> [u8; BLCKSZ] {
    let mut img = Box::new(TestPage([0u8; BLCKSZ]));
    // SAFETY: owned aligned scratch page.
    let mut page = unsafe { PageMut::from_raw(NonNull::new(img.0.as_mut_ptr()).unwrap()) };
    page.init(core::mem::size_of::<BTPageOpaqueData>());
    // the stream is page-memory order (descending offnum); C adds in reverse.
    let mut bounds = Vec::new();
    let mut off = 0usize;
    while off < items.len() {
        let t_info = u16::from_ne_bytes(items[off + 6..off + 8].try_into().unwrap());
        let size = (t_info & 0x1FFF) as usize;
        bounds.push((off, size));
        off += (size + 7) & !7;
    }
    assert_eq!(off, items.len());
    for (i, (off, size)) in bounds.iter().enumerate().rev() {
        let offnum = (bounds.len() - i) as u16;
        page.add_item(&items[*off..*off + *size], offnum, 0)
            .expect("restore add_item");
    }
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    img.0[special..].copy_from_slice(&opaque_from[special..]);
    page.set_lsn(lsn);
    img.0
}

#[test]
fn btree_split_wal_roundtrip() {
    let dir = std::env::temp_dir().join(format!("pgrust_nbtree_wal_{}", std::process::id()));
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
    procarray::ProcArrayShmemInit();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

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

    with_fake(|f| {
        let addr = Box::leak(empty_meta_page()).0.as_mut_ptr() as usize;
        f.pages.push(addr);
        f.pins.push(0);
    });

    let ctx = MemoryContext::new("nbtree_wal");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx);
    rel.rd_supportinfo
        .borrow_mut()
        .push(Some(FmgrInfo::new(test_int4cmp, 351, 2, true, false)));

    // Insert ascending keys until the root leaf splits (right page = block 2,
    // new root = block 3 appear from the same insert), then stop.
    let mut nkeys = 0u32;
    while with_fake(|f| f.pages.len()) < 4 {
        nkeys += 1;
        let icx = MemoryContext::new("ins");
        nbtree::btinsert(
            icx.mcx(),
            &rel,
            &[::datum::Datum::from_i32(nkeys as i32)],
            &[false],
            &ItemPointerData::new(nkeys, 1),
            &rel,
            IndexUniqueCheck::UNIQUE_CHECK_NO,
            false,
        )
        .unwrap();
    }
    with_fake(|f| assert!(f.pins.iter().all(|p| *p == 0), "leaked pins"));

    let left = page_bytes(1);
    let right = page_bytes(2);
    let root = page_bytes(3);
    let left_ref = unsafe { PageRef::from_raw(NonNull::new(left.0.as_ptr().cast_mut()).unwrap()) };
    let right_ref = unsafe { PageRef::from_raw(NonNull::new(right.0.as_ptr().cast_mut()).unwrap()) };
    let last_lsn = right_ref.lsn().max(root.0.as_ptr() as u64 * 0 + {
        let r = unsafe { PageRef::from_raw(NonNull::new(root.0.as_ptr().cast_mut()).unwrap()) };
        r.lsn()
    });
    transam_xlog::XLogFlush(last_lsn).unwrap();

    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };
    reader.XLogBeginRead(end_of_log + 40);

    // record 1: NEWROOT (root creation), block 0 root WILL_INIT, block 2 meta.
    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.XLogRecGetRmid(), RM_BTREE_ID);
    assert_eq!(reader.XLogRecGetInfo() & !0x0F, XLOG_BTREE_NEWROOT);
    let main = reader.XLogRecGetData();
    assert_eq!(main.len(), 8);
    assert_eq!(u32::from_ne_bytes(main[0..4].try_into().unwrap()), 1); // rootblk
    assert_eq!(u32::from_ne_bytes(main[4..8].try_into().unwrap()), 0); // level
    let md = reader.XLogRecGetBlockData(2).unwrap();
    assert_eq!(md.len(), 28);
    assert_eq!(u32::from_ne_bytes(md[0..4].try_into().unwrap()), BTREE_VERSION);
    assert_eq!(u32::from_ne_bytes(md[4..8].try_into().unwrap()), 1); // root

    // records 2..=nkeys: INSERT_LEAF for each key before the split, with the
    // logged tuple carrying the key value.
    for k in 1..nkeys {
        reader.XLogReadRecord(&mut routine).unwrap().unwrap();
        assert_eq!(reader.XLogRecGetRmid(), RM_BTREE_ID);
        assert_eq!(
            reader.XLogRecGetInfo() & !0x0F,
            XLOG_BTREE_INSERT_LEAF,
            "record for key {k}"
        );
        let main = reader.XLogRecGetData();
        assert_eq!(main.len(), 2);
        assert_eq!(u16::from_ne_bytes(main[0..2].try_into().unwrap()), k as u16);
        let itup = reader.XLogRecGetBlockData(0).unwrap();
        assert_eq!(itup.len(), 16);
        assert_eq!(
            i32::from_ne_bytes(itup[8..12].try_into().unwrap()),
            k as i32
        );
    }

    // split record: rightmost leaf split, newitem on the right (SPLIT_R).
    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.XLogRecGetRmid(), RM_BTREE_ID);
    assert_eq!(reader.XLogRecGetInfo() & !0x0F, XLOG_BTREE_SPLIT_R);
    let split_lsn = reader.v.EndRecPtr;
    let main = reader.XLogRecGetData();
    assert_eq!(main.len(), 10);
    assert_eq!(u32::from_ne_bytes(main[0..4].try_into().unwrap()), 0); // level
    let firstrightoff = u16::from_ne_bytes(main[4..6].try_into().unwrap());
    let newitemoff = u16::from_ne_bytes(main[6..8].try_into().unwrap());
    assert_eq!(u16::from_ne_bytes(main[8..10].try_into().unwrap()), 0); // postingoff
    assert_eq!(newitemoff as u32, nkeys); // ascending insert goes at the end
    assert!(firstrightoff <= newitemoff);

    let (_, _, lblk, _) = reader.XLogRecGetBlockTagExtended(0).unwrap();
    let (_, _, rblk, _) = reader.XLogRecGetBlockTagExtended(1).unwrap();
    assert_eq!((lblk, rblk), (1, 2));

    // block 0 data: the left page's new high key (no newitem: SPLIT_R).
    let hk_logged = reader.XLogRecGetBlockData(0).unwrap();
    let hk_id = left_ref.item_id(1); // P_HIKEY
    let (hk_ptr, _) = left_ref.item_raw(hk_id);
    // SAFETY: leaked page image.
    let hk_live = unsafe {
        core::slice::from_raw_parts(hk_ptr, (hk_id.lp_len() as usize + 7) & !7)
    };
    assert_eq!(hk_logged, hk_live);

    // block 1 data restores the right page image byte-for-byte.
    let ritems = reader.XLogRecGetBlockData(1).unwrap();
    let restored = restore_page(ritems, &right.0, split_lsn);
    assert_eq!(restored[..], right.0[..], "restored right page image differs");

    // final record: NEWROOT for the root split; its item stream restores the
    // new root page image.
    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.XLogRecGetRmid(), RM_BTREE_ID);
    assert_eq!(reader.XLogRecGetInfo() & !0x0F, XLOG_BTREE_NEWROOT);
    let root_lsn = reader.v.EndRecPtr;
    let main = reader.XLogRecGetData();
    assert_eq!(u32::from_ne_bytes(main[0..4].try_into().unwrap()), 3); // rootblk
    assert_eq!(u32::from_ne_bytes(main[4..8].try_into().unwrap()), 1); // level
    let rootitems = reader.XLogRecGetBlockData(0).unwrap();
    let restored_root = restore_page(rootitems, &root.0, root_lsn);
    assert_eq!(restored_root[..], root.0[..], "restored root page image differs");
    let md = reader.XLogRecGetBlockData(2).unwrap();
    assert_eq!(u32::from_ne_bytes(md[4..8].try_into().unwrap()), 3); // meta root
    assert_eq!(u32::from_ne_bytes(md[8..12].try_into().unwrap()), 1); // meta level

    assert_eq!(reader.v.EndRecPtr, last_lsn);
    let _ = std::fs::remove_dir_all(&dir);
}
