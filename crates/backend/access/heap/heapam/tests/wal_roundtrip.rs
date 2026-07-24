// M4 composition proof: heap_insert/heap_delete over a fake bufmgr, WAL
// through the real xloginsert/transam_xlog, decoded off disk with the real
// xlogreader; page bytes vs the C reference TU (bench/cref/heap_page_ref.c);
// visibility through the real heapam_visibility (committed visible, aborted
// invisible). One process-global test: the WAL rig owns cwd and shmem.
// Not miri-runnable: XLogFileInit does real segment file I/O.
#![cfg(not(miri))]
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::Mutex;

use heapam::{heap_delete, heap_insert};
use heaptuple::heap_form_tuple;
use mcx::{Mcx, MemoryContext, PgVec};
use tableam_vocab::{TM_FailureData, TM_Result};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, Oid, TimeLineID, XLogRecPtr,
    XLogSegNo, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_RELATION};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::bufpage::PageRef;
use types_storage::RelFileLocator;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, ItemPointerData, NameData,
    TupleDescData, HEAP_XMAX_INVALID,
};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AACC;
const REL_OID: Oid = 61000;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const COMMITTED_XID: u32 = 3; // FirstNormal: procarray answers not-in-progress
const ABORTED_XID: u32 = 4;
const CID: u32 = 7;

const XLOG_HEAP_INSERT: u8 = 0x00;
const XLOG_HEAP_DELETE: u8 = 0x10;
const XLOG_HEAP_INIT_PAGE: u8 = 0x80;
const XLHL_KEYS_UPDATED: u8 = 0x10;
const RM_HEAP_ID: u8 = rmgr::RmgrIds::RM_HEAP_ID as u8;

static CURRENT_XID: AtomicU32 = AtomicU32::new(COMMITTED_XID);

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>, // leaked page addresses; index = buffer - 1
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
            let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(1);
            f.locks.push(1);
            (f.pages.len() as Buffer, 1)
        }))
    });

    xact_seams::get_current_transaction_id::set(|| Ok(CURRENT_XID.load(Relaxed)));
    xact_seams::is_in_parallel_mode::set(|| false);
    xact_seams::transaction_id_is_current_transaction_id::set(
        ::xact::TransactionIdIsCurrentTransactionId,
    );
    xact_seams::mark_current_transaction_id_logged_if_any::set(|| {});
    xact_seams::get_current_sub_transaction_id::set(|| 1);

    transam_seams::transaction_id_did_commit::set(|xid| Ok(xid == COMMITTED_XID));
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
}

fn install_proc_boot_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(779);

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
        rd_locator: Cell::new(RLOC),
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
            lockRelId: LockRelId {
                relId: REL_OID,
                dbId: 5,
            },
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

fn page0() -> &'static [u8; BLCKSZ] {
    let addr = with_fake(|f| f.pages[0]);
    // SAFETY: leaked test page, always live.
    unsafe { &*(addr as *const [u8; BLCKSZ]) }
}

fn page0_tuple(off: u16) -> HeapTupleData<'static> {
    let addr = with_fake(|f| f.pages[0]);
    // SAFETY: leaked test page, always live.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    let id = page.item_id(off);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page image.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(0, off), REL_OID) }
}

fn mvcc_snapshot<'m>(mcx: Mcx<'m>) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    s.xmin = 10;
    s.xmax = 20;
    s.regd_count.set(1);
    s
}

#[test]
fn dml_wal_roundtrip_page_parity_and_visibility() {
    let dir = std::env::temp_dir().join(format!("pgrust_heapam_wal_{}", std::process::id()));
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
    heapam_visibility::init_seams();
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
    ctl.Insert
        .CurrBytePos
        .store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert
        .PrevBytePos
        .store(XLogRecPtrToBytePos(prev_rec), Relaxed);
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

    let ctx = MemoryContext::new("wal_roundtrip");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);
    let tupdesc = int4_tupdesc(mcx);

    // Three committed-xid inserts, one aborted-xid insert.
    let mut tids = Vec::new();
    for (i, val) in [41i32, 42, 43, 44].iter().enumerate() {
        CURRENT_XID.store(if i == 3 { ABORTED_XID } else { COMMITTED_XID }, Relaxed);
        let mut tup =
            heap_form_tuple(mcx, &tupdesc, &[::datum::Datum::from_i32(*val)], &[false]).unwrap();
        heap_insert(&rel, tup.as_tuple_mut(), CID, 0, None).unwrap();
        tids.push(tup.as_tuple().t_self);
    }
    assert_eq!(tids[0], ItemPointerData::new(0, 1));
    assert_eq!(tids[3], ItemPointerData::new(0, 4));

    // Delete (0,2) through the real heapam_visibility satisfies_update.
    CURRENT_XID.store(COMMITTED_XID, Relaxed);
    let mut tmfd = TM_FailureData::default();
    let del_tid = ItemPointerData::new(0, 2);
    let r = heap_delete(&rel, &del_tid, CID, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, TM_Result::TM_Ok);

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(
            f.locks.iter().all(|l| *l == 0),
            "leaked locks: {:?}",
            f.locks
        );
    });

    // Page bytes == the C reference page (pd_lsn compared separately).
    let page = page0();
    let last_lsn = {
        let addr = with_fake(|f| f.pages[0]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    };
    assert_ne!(last_lsn, 0);
    let mut got = page.to_vec();
    got[0..8].fill(0);
    let want: &[u8] = include_bytes!("fixtures/heap_page_c.bin");
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

    // Decode all five records off disk with the real xlogreader.
    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead {
        wal_dir: dir.join("pg_wal"),
    };

    let first_rec = end_of_log + 40; // long page header on the fresh segment
    reader.XLogBeginRead(first_rec);

    for (i, val) in [41i32, 42, 43, 44].iter().enumerate() {
        let offnum = (i + 1) as u16;
        reader.XLogReadRecord(&mut routine).unwrap().unwrap();
        assert_eq!(reader.XLogRecGetRmid(), RM_HEAP_ID);
        let want_info = if i == 0 {
            XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE
        } else {
            XLOG_HEAP_INSERT
        };
        assert_eq!(reader.XLogRecGetInfo() & !0x0F, want_info & !0x0F);
        assert_eq!(reader.XLogRecGetInfo() & 0x0F, 0);
        // xl_heap_insert { OffsetNumber offnum; uint8 flags; }
        let main = reader.XLogRecGetData();
        assert_eq!(main.len(), 3);
        assert_eq!(u16::from_ne_bytes(main[0..2].try_into().unwrap()), offnum);
        assert_eq!(main[2], 0);
        let (loc, fork, blk, _) = reader.XLogRecGetBlockTagExtended(0).unwrap();
        assert_eq!((loc, fork, blk), (RLOC, ForkNumber::MAIN_FORKNUM, 0));
        // REGBUF_WILL_INIT implies REGBUF_NO_IMAGE (C xloginsert.h): even on
        // a fresh page whose LSN 0 <= RedoRecPtr, C takes no FPI for an
        // INIT_PAGE record — redo rebuilds the page from the block data
        // (pinned against pg_waldump 18.3: INSERT+INIT is 59 bytes, no FPW).
        assert!(!reader.XLogRecHasBlockImage(0));
        // xl_heap_header { infomask2; infomask; t_hoff } + tuple body
        let bd = reader.XLogRecGetBlockData(0).unwrap();
        assert_eq!(bd.len(), 5 + 5); // header + (t_len 28 - SizeofHeapTupleHeader 23)
        assert_eq!(u16::from_ne_bytes(bd[0..2].try_into().unwrap()), 1); // natts
        assert_eq!(
            u16::from_ne_bytes(bd[2..4].try_into().unwrap()),
            HEAP_XMAX_INVALID
        );
        assert_eq!(bd[4], 24); // t_hoff
        assert_eq!(i32::from_ne_bytes(bd[6..10].try_into().unwrap()), *val);
    }

    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.v.EndRecPtr, last_lsn);
    assert_eq!(reader.XLogRecGetRmid(), RM_HEAP_ID);
    assert_eq!(reader.XLogRecGetInfo() & !0x0F, XLOG_HEAP_DELETE);
    // xl_heap_delete { xmax; offnum; infobits_set; flags }
    let main = reader.XLogRecGetData();
    assert_eq!(main.len(), 8);
    assert_eq!(
        u32::from_ne_bytes(main[0..4].try_into().unwrap()),
        COMMITTED_XID
    );
    assert_eq!(u16::from_ne_bytes(main[4..6].try_into().unwrap()), 2);
    assert_eq!(main[6], XLHL_KEYS_UPDATED);
    assert_eq!(main[7], 0);
    let (loc, _, blk, _) = reader.XLogRecGetBlockTagExtended(0).unwrap();
    assert_eq!((loc, blk), (RLOC, 0));
    assert!(reader.XLogRecGetBlockData(0).is_none());

    // Visibility through the real heapam_visibility: committed insert is
    // visible, the committed delete and the aborted insert are not.
    let buf = bufmgr_seams::read_buffer::call(&rel, 0).unwrap();
    let snap = mvcc_snapshot(mcx);
    let mut t1 = page0_tuple(1);
    assert!(
        heapam_visibility_seams::heap_tuple_satisfies_visibility::call(&mut t1, &snap, buf)
            .unwrap()
    );
    let mut t2 = page0_tuple(2);
    assert!(
        !heapam_visibility_seams::heap_tuple_satisfies_visibility::call(&mut t2, &snap, buf)
            .unwrap()
    );
    let mut t4 = page0_tuple(4);
    assert!(
        !heapam_visibility_seams::heap_tuple_satisfies_visibility::call(&mut t4, &snap, buf)
            .unwrap()
    );
    bufmgr_seams::release_buffer::call(buf).unwrap();

    let _ = std::fs::remove_dir_all(&dir);
}
