// Redo round-trip for XLOG_INVALIDATIONS: LogStandbyInvalidations writes a
// real record through xloginsert + transam_xlog on disk; the record is read
// back with the real xlogreader, its layout asserted against standbydefs.h,
// rendered through the landed standbydesc, and dispatched through
// standby_redo (a no-op while not in hot standby, as in C).
use std::sync::atomic::Ordering::Relaxed;

use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{XLogRecPtr, XLogSegNo, BLCKSZ};
use types_storage::sinval::SharedInvalidationMessage;
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AB05;
const RM_STANDBY_ID: u8 = 8;
const XLOG_INVALIDATIONS: u8 = 0x20;

struct SegFileRead {
    wal_dir: std::path::PathBuf,
}

impl XLogSegmentRoutine for SegFileRead {
    fn segment_open(
        &mut self,
        _v: &mut ReaderView,
        _segno: XLogSegNo,
        _tli: &mut types_core::TimeLineID,
    ) -> types_error::PgResult<()> {
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
    ) -> types_error::PgResult<i32> {
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

#[test]
fn invalidations_record_roundtrips_through_reader_desc_and_redo() {
    let dir =
        std::env::temp_dir().join(format!("pgrust_standby_inval_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    shmem::init_seams();
    guc_tables::init_seams();
    transam_xlog::init_seams();
    xloginsert::init_seams();
    standby::init_seams();
    origin_seams::replorigin_session_origin::set(|| 0);
    xact_seams::mark_current_transaction_id_logged_if_any::set(|| {});
    xact_seams::get_current_sub_transaction_id::set(|| 1);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();

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

    init_small::globals::SetMyDatabaseId(5);
    init_small::globals::SetMyDatabaseTableSpace(1663);

    let msgs = [
        SharedInvalidationMessage::Catcache(types_storage::sinval::SharedInvalCatcacheMsg {
            id: 57,
            dbId: 5,
            hashValue: 0xDEAD_BEEF,
        }),
        SharedInvalidationMessage::Relcache(types_storage::sinval::SharedInvalRelcacheMsg {
            dbId: 5,
            relId: 16384,
        }),
    ];
    standby_seams::log_standby_invalidations::call(&msgs, true).unwrap();
    let lsn = transam_xlog::XactLastRecEnd();
    transam_xlog::XLogFlush(lsn).unwrap();

    let context: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("standby inval reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(context.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead { wal_dir: dir.join("pg_wal") };

    let first_rec = end_of_log + 40;
    reader.XLogBeginRead(first_rec);
    let got = reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(got, first_rec);
    assert_eq!(reader.XLogRecGetRmid(), RM_STANDBY_ID);
    assert_eq!(reader.XLogRecGetInfo() & !0x0F, XLOG_INVALIDATIONS);
    assert_eq!(reader.XLogRecGetXid(), 0);
    assert!(!reader.XLogRecHasAnyBlockRefs());

    // xl_invalidations: dbId 0, tsId 4, relcacheInitFileInval 8, nmsgs 12,
    // msgs[] 16 (standbydefs.h).
    let data = reader.XLogRecGetData();
    assert_eq!(data.len(), 16 + 2 * 16);
    assert_eq!(u32::from_ne_bytes(data[0..4].try_into().unwrap()), 5);
    assert_eq!(u32::from_ne_bytes(data[4..8].try_into().unwrap()), 1663);
    assert_eq!(data[8], 1);
    assert_eq!(&data[9..12], &[0, 0, 0]);
    assert_eq!(i32::from_ne_bytes(data[12..16].try_into().unwrap()), 2);
    for (i, msg) in msgs.iter().enumerate() {
        assert_eq!(&data[16 + i * 16..32 + i * 16], &msg.to_wire_bytes());
    }

    let ctx = Box::leak(Box::new(mcx::MemoryContext::new("desc")));
    let mut buf = stringinfo::StringInfo::new_in(ctx.mcx()).unwrap();
    rmgrdesc::standbydesc::standby_desc(&mut buf, &reader.v).unwrap();
    assert_eq!(
        String::from_utf8(buf.as_bytes().to_vec()).unwrap(),
        "; relcache init file inval dbid 5 tsid 1663; inval msgs: catcache 57 relcache 16384"
    );

    assert_eq!(xlogutils::standby_state(), xlogutils::STANDBY_DISABLED);
    standby::standby_redo(&mut reader.v).unwrap();
}
