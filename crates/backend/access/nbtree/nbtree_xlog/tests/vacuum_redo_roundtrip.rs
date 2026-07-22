// Vacuum-lane redo round-trip: btinsert builds a multi-level tree over a
// fork-aware fake bufmgr (FSM traffic runs the real freespace code), then
// btbulkdelete deletes every TID (posting-tuple update included on a
// hand-planted posting leaf), _bt_pagedel unlinks emptied leaves, and
// btvacuumcleanup writes META_CLEANUP. The main-fork storage is wiped and
// every btree record is replayed through the real btree_redo; reconstructed
// blocks must equal the write-side pages byte-exact. Covers VACUUM (deletes +
// posting updates), MARK_PAGE_HALFDEAD, UNLINK_PAGE, UNLINK_PAGE_META, and
// META_CLEANUP.
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
    BackendType, BlockNumber, Buffer, ForkNumber, Oid, TimeLineID, XLogRecPtr, XLogSegNo, BLCKSZ,
    INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use types_nbtree::genam::IndexUniqueCheck;
use types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_META, BTREE_MAGIC, BTREE_VERSION, P_NONE,
    XLOG_BTREE_MARK_PAGE_HALFDEAD, XLOG_BTREE_META_CLEANUP, XLOG_BTREE_UNLINK_PAGE,
    XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX,
};
use types_storage::bufpage::{PageRef, SizeOfPageHeaderData};
use types_storage::RelFileLocator;
use types_tuple::itemptr::ItemPointerData;
use types_tuple::tupdesc::CompactAttribute;
use types_tuple::{NameData, TupleDescData};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AACD;
const REL_OID: Oid = 62002;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RM_BTREE_ID: u8 = rmgr::RmgrIds::RM_BTREE_ID as u8;

const FORK_STRIDE: Buffer = 100_000;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

#[derive(Default)]
struct ForkPages {
    pages: Vec<usize>,
    pins: Vec<i32>,
}

struct Fake {
    forks: [ForkPages; 4],
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    forks: [
        ForkPages { pages: Vec::new(), pins: Vec::new() },
        ForkPages { pages: Vec::new(), pins: Vec::new() },
        ForkPages { pages: Vec::new(), pins: Vec::new() },
        ForkPages { pages: Vec::new(), pins: Vec::new() },
    ],
});

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn new_page() -> usize {
    Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize
}

fn buf_of(fork: ForkNumber, block: BlockNumber) -> Buffer {
    fork as Buffer * FORK_STRIDE + block as Buffer + 1
}

fn decode(buf: Buffer) -> (usize, usize) {
    (((buf - 1) / FORK_STRIDE) as usize, ((buf - 1) % FORK_STRIDE) as usize)
}

fn pin_read(fork: usize, block: usize) -> Buffer {
    with_fake(|f| {
        let fp = &mut f.forks[fork];
        assert!(block < fp.pages.len(), "read past fork {fork} end: {block}");
        fp.pins[block] += 1;
        fork as Buffer * FORK_STRIDE + block as Buffer + 1
    })
}

fn page_bytes(block: usize) -> [u8; BLCKSZ] {
    let addr = with_fake(|f| f.forks[0].pages[block]);
    // SAFETY: leaked test page, always live.
    unsafe { *(addr as *const [u8; BLCKSZ]) }
}

fn install_seams() {
    bufmgr_seams::read_buffer::set(|_rel, block| Ok(pin_read(0, block as usize)));
    bufmgr_seams::read_buffer_extended::set(|_rel, fork, block, mode, _strategy| {
        let f = fork as usize;
        if mode == types_storage::ReadBufferMode::ZeroOnError
            || mode == types_storage::ReadBufferMode::Normal
        {
            Ok(pin_read(f, block as usize))
        } else {
            unreachable!("mode {mode:?}");
        }
    });
    bufmgr_seams::buffer_get_block_number::set(|buf| decode(buf).1 as BlockNumber);
    bufmgr_seams::buffer_get_page::set(|buf| {
        let (fork, block) = decode(buf);
        let addr = with_fake(|f| {
            assert!(f.forks[fork].pins[block] > 0, "page access without pin");
            f.forks[fork].pages[block]
        });
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::release_buffer::set(|buf| {
        let (fork, block) = decode(buf);
        with_fake(|f| {
            let p = &mut f.forks[fork].pins[block];
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
        let (fork, block) = decode(buf);
        with_fake(|f| f.forks[fork].pins[block] += 1);
    });
    bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
    bufmgr_seams::conditional_lock_buffer::set(|_buf| Ok(true));
    bufmgr_seams::lock_buffer_for_cleanup::set(|_buf| Ok(()));
    bufmgr_seams::conditional_lock_buffer_for_cleanup::set(|_buf| Ok(true));
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let (fork, block) = decode(buf);
        let addr = with_fake(|f| f.forks[fork].pages[block]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::extend_buffered_rel_by::set(|_rel, fork, _strategy, flags, extend_by| {
        assert_eq!(extend_by, 1);
        assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
        Ok(with_fake(|f| {
            let fp = &mut f.forks[fork as usize];
            fp.pages.push(new_page());
            fp.pins.push(1);
            (buf_of(fork, (fp.pages.len() - 1) as BlockNumber), 1)
        }))
    });
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, fork| {
        Ok(with_fake(|f| f.forks[fork as usize].pages.len() as BlockNumber))
    });
    bufmgr_seams::relation_smgr_locator::set(|_rel| ::types_storage::RelFileLocatorBackend {
        locator: RLOC,
        backend: INVALID_PROC_NUMBER,
    });
    smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| 0);
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
    smgr_seams::smgr_exists::set(|_loc, fork| {
        Ok(with_fake(|f| !f.forks[fork as usize].pages.is_empty()))
    });

    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));
    predicate_seams::predicate_lock_page_combine::set(|_rel, _o, _n| Ok(()));
    catalog_seams::is_catalog_relation::set(|_rel| false);
    bufmgr_seams::extend_buffered_rel_to_rel::set(|rel, fork, strategy, flags, extend_to, mode| {
        bufmgr_seams::extend_buffered_rel_to::call(
            bufmgr_seams::relation_smgr_locator::call(rel),
            fork,
            strategy,
            flags,
            extend_to,
            mode,
        )
    });

    // xloginsert marshal for fake buffers -> the real record-assembly path.
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let (fork, block) = decode(b.buffer);
            assert_eq!(fork, 0, "only main-fork pages are WAL-registered here");
            let addr = with_fake(|f| f.forks[0].pages[block]);
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RLOC,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: block as BlockNumber,
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
    smgr_seams::smgr_nblocks::set(|_, fork| {
        Ok(with_fake(|f| f.forks[fork as usize].pages.len() as BlockNumber))
    });
    bufmgr_seams::read_recent_buffer::set(|_, _, _, _| Ok(false));
    bufmgr_seams::read_buffer_without_relcache::set(|_loc, fork, blkno, _mode, _strat, _perm| {
        Ok(pin_read(fork as usize, blkno as usize))
    });
    bufmgr_seams::extend_buffered_rel_to::set(|_smgr, fork, _strat, _flags, extend_to, _mode| {
        with_fake(|f| {
            let fp = &mut f.forks[fork as usize];
            while fp.pages.len() < extend_to as usize {
                fp.pages.push(new_page());
                fp.pins.push(0);
            }
            fp.pins[(extend_to - 1) as usize] += 1;
            Ok(buf_of(fork, extend_to - 1))
        })
    });
    bufmgr_seams::buffer_page_is_new::set(|buf| {
        let (fork, block) = decode(buf);
        let addr = with_fake(|f| f.forks[fork].pages[block]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.is_new()
    });
    bufmgr_seams::buffer_page_get_lsn::set(|buf| {
        let (fork, block) = decode(buf);
        let addr = with_fake(|f| f.forks[fork].pages[block]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::buffer_page_set_lsn::set(|buf, lsn| {
        let (fork, block) = decode(buf);
        let addr = with_fake(|f| f.forks[fork].pages[block]);
        // SAFETY: leaked test page; replay is single-threaded.
        let mut pm = unsafe {
            types_storage::bufpage::PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap())
        };
        pm.set_lsn(lsn);
    });
    bufmgr_seams::flush_one_buffer::set(|_| Ok(()));
    bufmgr_seams::overwrite_buffer_page::set(|buf, page| {
        let (fork, block) = decode(buf);
        let addr = with_fake(|f| f.forks[fork].pages[block]);
        // SAFETY: leaked test page; replay is single-threaded.
        unsafe { core::ptr::copy_nonoverlapping(page.as_ptr(), addr as *mut u8, page.len()) };
    });
    relpath_seams::relpathperm::set(|rlocator, forknum| {
        format!("base/{}/{}#{:?}", rlocator.dbOid, rlocator.relNumber, forknum)
    });
    xlogrecovery_seams::reached_consistency::set(|| false);
}

fn install_proc_boot_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(784);

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
        rd_trigdesc: Default::default(),
        rd_hastriggers: false, rd_hasrules: false,
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
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
    let img = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: P_NONE,
        btm_level: 0,
        btm_fastroot: P_NONE,
        btm_fastlevel: 0,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: false,
    }
    .page_image();
    p.0[SizeOfPageHeaderData..SizeOfPageHeaderData + 48].copy_from_slice(&img);
    // SAFETY: owned page, in-bounds aligned writes.
    unsafe {
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
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 100);
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

// Plant a 3-TID posting tuple (key `key`) on the leaf page that owns the
// keyspace, exactly the image C's deduplication would build.
fn plant_posting_tuple(leaf_blk: usize, key: i32) {
    let addr = with_fake(|f| f.forks[0].pages[leaf_blk]);
    // key tuple: 8B header + 4B int4 + 4B pad = 16, posting offset 16,
    // 3 TIDs * 6B = 18 -> newsize MAXALIGN(34) = 40.
    let mut img = [0u8; 40];
    let nhtids: u16 = 3 | types_nbtree::BT_IS_POSTING;
    let postingoff: u32 = 16;
    img[0..2].copy_from_slice(&((postingoff >> 16) as u16).to_ne_bytes());
    img[2..4].copy_from_slice(&((postingoff & 0xffff) as u16).to_ne_bytes());
    img[4..6].copy_from_slice(&nhtids.to_ne_bytes());
    let info: u16 = 40 | types_nbtree::INDEX_ALT_TID_MASK;
    img[6..8].copy_from_slice(&info.to_ne_bytes());
    img[8..12].copy_from_slice(&key.to_ne_bytes());
    for (i, blk) in [9000u16, 9001, 9002].iter().enumerate() {
        let off = 16 + i * 6;
        img[off..off + 2].copy_from_slice(&0u16.to_ne_bytes());
        img[off + 2..off + 4].copy_from_slice(&blk.to_ne_bytes());
        img[off + 4..off + 6].copy_from_slice(&1u16.to_ne_bytes());
    }
    // SAFETY: leaked test page; test is single-threaded here.
    let mut pm = unsafe {
        types_storage::bufpage::PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap())
    };
    // Insert as the first data item on the page (lowest key on this leaf).
    let opaque_off = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    // SAFETY: special-space read of a live btree page.
    let opaque = unsafe {
        (addr as *const u8).add(opaque_off).cast::<BTPageOpaqueData>().read()
    };
    let first_data = if opaque.btpo_next != P_NONE || opaque.btpo_prev != P_NONE {
        2 // has a high key at offset 1 unless rightmost+leftmost
    } else {
        1
    };
    let off = pm
        .add_item(&img, first_data, 0)
        .expect("posting tuple fits");
    assert!(off == first_data);
}

#[test]
fn btree_vacuum_redo_rebuilds_pages_byte_exact() {
    let dir =
        std::env::temp_dir().join(format!("pgrust_nbtxlog_vacredo_{}", std::process::id()));
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
    xlogreader::init_seams();
    xlogutils::init_seams();
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
    // Recycling never becomes safe in this run: newly deleted pages stay
    // pending, so btvacuumcleanup records a nonzero num_delpages
    // (META_CLEANUP producer arm).
    procarray_seams::global_vis_check_removable_full_xid::set(|_, _| Ok(false));
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
        f.forks[0].pages.push(Box::leak(empty_meta_page()).0.as_mut_ptr() as usize);
        f.forks[0].pins.push(0);
    });

    let ctx = MemoryContext::new("nbtxlog_vacredo");
    let mcx = ctx.mcx();
    let rel = index_rel(mcx);
    rel.rd_supportinfo
        .borrow_mut()
        .push(Some(FmgrInfo::new(test_int4cmp, 351, 2, true, false)));

    let insert = |key: i32| {
        let icx = MemoryContext::new("ins");
        nbtree::btinsert(
            icx.mcx(),
            &rel,
            &[::datum::Datum::from_i32(key)],
            &[false],
            &ItemPointerData::new(key as u32, 1),
            &rel,
            IndexUniqueCheck::UNIQUE_CHECK_NO,
            false,
        )
        .unwrap();
    };

    // Grow a 2-level tree: meta + several leaves + root.
    let mut key = 0i32;
    while with_fake(|f| f.forks[0].pages.len()) < 6 {
        key += 2;
        insert(key);
    }
    let max_key = key;

    // Rightmost leaf: follow btpo_next from block 1.
    let opaque_off = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    let rightmost_leaf = {
        let mut blk = 1usize;
        loop {
            let addr = with_fake(|f| f.forks[0].pages[blk]);
            // SAFETY: special-space read of a live btree page.
            let next = unsafe {
                (addr as *const u8).add(opaque_off).cast::<BTPageOpaqueData>().read().btpo_next
            };
            if next == P_NONE {
                break blk;
            }
            blk = next as usize;
        }
    };

    // Plant a posting tuple on the rightmost leaf with one of its 3 TIDs among
    // the dead set, then FPI-log the page (the plant itself has no WAL
    // producer; dedup is unported). It keeps that leaf nonempty so the level
    // collapses to exactly one page and UNLINK_PAGE_META fires.
    plant_posting_tuple(rightmost_leaf, max_key + 2);
    {
        let _pin = pin_read(0, rightmost_leaf);
        let fpi_lsn = xloginsert_seams::xlog_insert_record::call(
            rmgr::RmgrIds::RM_XLOG_ID as u8,
            transam_xlog::XLOG_FPI,
            0,
            &[],
            &[xloginsert_seams::XLogRegBuf {
                block_id: 0,
                buffer: buf_of(ForkNumber::MAIN_FORKNUM, rightmost_leaf as u32),
                flags: xloginsert_seams::REGBUF_FORCE_IMAGE | xloginsert_seams::REGBUF_STANDARD,
                bufdata: &[],
            }],
        )
        .unwrap();
        let addr = with_fake(|f| f.forks[0].pages[rightmost_leaf]);
        // SAFETY: leaked test page; single-threaded.
        let mut pm = unsafe {
            types_storage::bufpage::PageMut::from_raw(NonNull::new(addr as *mut u8).unwrap())
        };
        pm.set_lsn(fpi_lsn);
        bufmgr_seams::release_buffer::call(buf_of(ForkNumber::MAIN_FORKNUM, rightmost_leaf as u32))
            .unwrap();
    }

    with_fake(|f| assert!(f.forks[0].pins.iter().all(|p| *p == 0), "leaked pins"));

    // Dead set: every inserted key (empties every leaf except the rightmost,
    // which keeps the planted posting tuple -> the last unlink sees
    // leftsib == P_NONE with a rightmost right sibling and must update the
    // metapage fast root) plus one TID of the planted posting tuple.
    let mut dead: Vec<ItemPointerData> = Vec::new();
    let mut k = 2i32;
    while k <= max_key {
        dead.push(ItemPointerData::new(k as u32, 1));
        k += 2;
    }
    dead.push(ItemPointerData::new(9001, 1));
    dead.sort_by(|a, b| {
        types_tuple::itemptr::ItemPointerCompare(a, b).cmp(&0)
    });

    let info = nbtree::IndexVacuumInfo {
        index: &rel,
        heaprel: &rel,
        analyze_only: false,
        estimated_count: true,
        num_heap_tuples: max_key as f64 / 2.0,
        strategy: None,
    };
    let vcx = MemoryContext::new("vac");
    let stats = nbtree::btbulkdelete(vcx.mcx(), &info, None, &dead).unwrap();
    assert!(stats.tuples_removed > 0.0, "bulkdelete removed TIDs");
    assert!(stats.pages_newly_deleted > 0, "page deletion ran");
    let stats = nbtree::btvacuumcleanup(vcx.mcx(), &info, Some(stats)).unwrap().unwrap();
    assert!(stats.pages_deleted >= stats.pages_free);

    with_fake(|f| assert!(f.forks[0].pins.iter().all(|p| *p == 0), "leaked pins post-vacuum"));

    let nblocks = with_fake(|f| f.forks[0].pages.len());
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

    // Wipe main-fork storage: replay must rebuild every block purely from WAL.
    with_fake(|f| {
        f.forks[0].pages.clear();
        f.forks[0].pins.clear();
    });
    xlogutils::set_in_recovery(true);

    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };
    reader.XLogBeginRead(end_of_log + 40);

    let mut seen = [0u32; 16];
    while reader.v.EndRecPtr < last_lsn {
        reader.XLogReadRecord(&mut routine).unwrap().unwrap();
        let rmid = reader.XLogRecGetRmid();
        if rmid == RM_BTREE_ID {
            let info = reader.XLogRecGetInfo() & !0x0F;
            seen[(info >> 4) as usize] += 1;
        }
        (rmgr::GetRmgr(rmid).unwrap().rm_redo)(&mut reader.v).unwrap();
    }
    assert_eq!(reader.v.EndRecPtr, last_lsn);

    assert!(seen[(XLOG_BTREE_VACUUM >> 4) as usize] > 0, "VACUUM replayed");
    assert!(
        seen[(XLOG_BTREE_MARK_PAGE_HALFDEAD >> 4) as usize] > 0,
        "MARK_PAGE_HALFDEAD replayed"
    );
    assert!(
        seen[(XLOG_BTREE_UNLINK_PAGE >> 4) as usize] > 0,
        "UNLINK_PAGE replayed"
    );
    assert!(
        seen[(XLOG_BTREE_UNLINK_PAGE_META >> 4) as usize] > 0,
        "UNLINK_PAGE_META replayed"
    );
    assert!(
        seen[(XLOG_BTREE_META_CLEANUP >> 4) as usize] > 0,
        "META_CLEANUP replayed"
    );

    with_fake(|f| assert!(f.forks[0].pins.iter().all(|p| *p == 0), "replay leaked pins"));
    assert_eq!(with_fake(|f| f.forks[0].pages.len()), nblocks);

    // btree_mask subset (nbtxlog.c): zero the pd_lower..pd_upper hole (the
    // write side keeps stale bytes where redo re-inits) and the unlogged
    // hint state (BTP_HAS_GARBAGE/BTP_SPLIT_END, btpo_cycleid).
    let mask = |mut p: [u8; BLCKSZ]| -> [u8; BLCKSZ] {
        let lower = u16::from_ne_bytes([p[12], p[13]]) as usize;
        let upper = u16::from_ne_bytes([p[14], p[15]]) as usize;
        if lower <= upper && upper <= BLCKSZ {
            p[lower..upper].fill(0);
        }
        let opq = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
        let flags_off = opq + 12;
        let mut flags = u16::from_ne_bytes([p[flags_off], p[flags_off + 1]]);
        flags &= !(types_nbtree::BTP_HAS_GARBAGE | types_nbtree::BTP_SPLIT_END);
        p[flags_off..flags_off + 2].copy_from_slice(&flags.to_ne_bytes());
        p[opq + 14..opq + 16].fill(0); // btpo_cycleid
        p
    };

    for blk in 0..nblocks {
        let got = mask(page_bytes(blk));
        let expected_blk = mask(expected[blk]);
        if got[..] != expected_blk[..] {
            let first = got.iter().zip(&expected_blk).position(|(a, b)| a != b).unwrap();
            panic!(
                "replayed block {blk} differs at byte {first}: got {:02x?} want {:02x?}",
                &got[first..(first + 16).min(BLCKSZ)],
                &expected_blk[first..(first + 16).min(BLCKSZ)]
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
