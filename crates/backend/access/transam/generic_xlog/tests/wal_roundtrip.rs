// Cross-replay proof: Start/Register/Finish over a fake bufmgr, WAL through
// the real xloginsert/transam_xlog, decoded with the real xlogreader; the
// delta and FULL_IMAGE blocks replay byte-exact against the live pages.
// Not miri-runnable: XLogFileInit does real segment file I/O.
#![cfg(not(miri))]
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use generic_xlog::{GenericXLogFinish, GenericXLogStart, GENERIC_XLOG_FULL_IMAGE};
use mcx::{Mcx, MemoryContext, PgVec};
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
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE, RELKIND_RELATION,
};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_storage::RelFileLocator;
use types_tuple::tupdesc::CompactAttribute;
use types_tuple::{NameData, TupleDescData};
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_CCDD;
const REL_OID: Oid = 63000;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const RM_GENERIC_ID: u8 = types_core::RmgrIds::RM_GENERIC_ID as u8;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>,
    dirty: Vec<Buffer>,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake { pages: Vec::new(), dirty: Vec::new() });

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn page_bytes(block: usize) -> TestPage {
    let addr = with_fake(|f| f.pages[block]);
    // SAFETY: leaked test page, always live.
    TestPage(unsafe { *(addr as *const [u8; BLCKSZ]) })
}

fn install_seams() {
    bufmgr_seams::buffer_get_page::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::mark_buffer_dirty::set(|buf| {
        with_fake(|f| f.dirty.push(buf));
        Ok(())
    });
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let addr = with_fake(|f| f.pages[(b.buffer - 1) as usize]);
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RLOC,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: (b.buffer - 1) as BlockNumber,
                // SAFETY: leaked test page, BLCKSZ, held by the caller.
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
    g::SetMyProcPid(783);

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
    smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| 0);
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
    smgr_seams::smgr_exists::set(|_loc, _fork| Ok(false));
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

fn plain_rel(mcx: Mcx<'_>) -> Relation<'_> {
    let mut relname = NameData::default();
    relname.namestrcpy("t_generic");
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
            relam: 2,
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
        },
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
    };
    Relation::open(data, Some(noop_close))
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

// Standard page with LSN already past RedoRecPtr: the delta lane takes no FPI.
fn fixture_page(lsn: XLogRecPtr, fill: u8, nlower: usize) -> Box<TestPage> {
    let mut p = Box::new(TestPage([0u8; BLCKSZ]));
    p.0[0..4].copy_from_slice(&((lsn >> 32) as u32).to_ne_bytes());
    p.0[4..8].copy_from_slice(&(lsn as u32).to_ne_bytes());
    let lower = (SizeOfPageHeaderData + nlower) as u16;
    let upper = (BLCKSZ - 128) as u16;
    p.0[12..14].copy_from_slice(&lower.to_ne_bytes());
    p.0[14..16].copy_from_slice(&upper.to_ne_bytes());
    p.0[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
    p.0[18..20].copy_from_slice(&(BLCKSZ as u16 | 4).to_ne_bytes());
    p.0[SizeOfPageHeaderData..lower as usize].fill(fill);
    p.0[upper as usize..].fill(fill.wrapping_add(1));
    p
}

#[test]
fn generic_wal_roundtrip() {
    let dir = std::env::temp_dir().join(format!("pgrust_genxlog_wal_{}", std::process::id()));
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
        f.pages.push(Box::leak(fixture_page(end_of_log, 0x5A, 256)).0.as_mut_ptr() as usize);
        f.pages.push(Box::leak(fixture_page(end_of_log, 0x6B, 64)).0.as_mut_ptr() as usize);
    });
    let pristine0 = page_bytes(0);

    let ctx = MemoryContext::new("genxlog_wal");
    let mcx = ctx.mcx();
    let rel = plain_rel(mcx);

    let mut state = GenericXLogStart(mcx, &rel).unwrap();
    assert!(state.is_logged());

    // buffer 1 (block 0): delta lane — grow pd_lower, rewrite the page tail.
    {
        let img = state.register_buffer(1, 0).unwrap();
        let lower = u16::from_ne_bytes([img[12], img[13]]) as usize;
        img[lower..lower + 8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        img[12..14].copy_from_slice(&((lower + 8) as u16).to_ne_bytes());
        img[BLCKSZ - 16..].fill(0xEE);
    }
    // buffer 2 (block 1): FULL_IMAGE lane.
    {
        let img = state.register_buffer(2, GENERIC_XLOG_FULL_IMAGE).unwrap();
        img[SizeOfPageHeaderData] = 0x99;
    }
    // re-registering an already-registered buffer hands back the same image
    assert_eq!(state.register_buffer(1, 0).unwrap()[BLCKSZ - 1], 0xEE);

    let lsn = GenericXLogFinish(state).unwrap();
    assert_ne!(lsn, 0);
    with_fake(|f| assert_eq!(f.dirty, vec![1, 2]));

    let live0 = page_bytes(0);
    let live1 = page_bytes(1);
    let live0_lsn = {
        let hi = u32::from_ne_bytes(live0.0[0..4].try_into().unwrap()) as u64;
        let lo = u32::from_ne_bytes(live0.0[4..8].try_into().unwrap()) as u64;
        (hi << 32) | lo
    };
    assert_eq!(live0_lsn, lsn);

    transam_xlog::XLogFlush(lsn).unwrap();

    let reader_ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(reader_ctx.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };
    reader.XLogBeginRead(end_of_log + 40);

    reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.XLogRecGetRmid(), RM_GENERIC_ID);
    assert_eq!(reader.XLogRecGetInfo(), 0);
    assert_eq!(reader.XLogRecGetDataLen(), 0);
    assert_eq!(reader.v.EndRecPtr, lsn);

    // block 0: delta replays onto the pristine image, byte-exact vs live.
    assert!(!reader.XLogRecHasBlockImage(0));
    let delta = reader.XLogRecGetBlockData(0).unwrap().to_vec();
    let mut replayed = pristine0.0;
    generic_xlog::redo_page_transform(&mut replayed, &delta, lsn);
    assert_eq!(replayed[..], live0.0[..], "replayed delta page differs from live page");

    // block 1: forced image (pd_lsn is stamped after the image is taken, so
    // compare past it; redo stamps it via BLK_RESTORED).
    assert!(reader.XLogRecHasBlockImage(1));
    let mut restored = Box::new(TestPage([0u8; BLCKSZ]));
    assert!(reader.RestoreBlockImage(1, &mut restored.0));
    assert_eq!(restored.0[8..], live1.0[8..], "restored full image differs from live page");
    assert_eq!(restored.0[SizeOfPageHeaderData], 0x99);

    // xlogstats buckets the same decoded record.
    let mut stats = Box::new(xlogstats::XLogStats::ZEROED);
    xlogstats::XLogRecStoreStats(&mut stats, &reader.v);
    let (rec_len, fpi_len) = xlogstats::XLogRecGetLen(&reader.v);
    assert_eq!(stats.count, 1);
    assert_eq!(stats.rmgr_stats[RM_GENERIC_ID as usize].count, 1);
    assert_eq!(stats.rmgr_stats[RM_GENERIC_ID as usize].fpi_len, fpi_len as u64);
    assert_eq!(stats.record_stats[RM_GENERIC_ID as usize][0].rec_len, rec_len as u64);
    assert!(fpi_len > 0);
    assert!(rec_len as usize > delta.len());

    let _ = std::fs::remove_dir_all(&dir);
}
