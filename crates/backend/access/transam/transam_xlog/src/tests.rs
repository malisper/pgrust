use std::mem::{offset_of, size_of};

use crate::control_file::*;
use crate::*;

// Layout ground truth from a C compile of pg_control.h (REL_18_3).
#[test]
fn control_file_layout_matches_c() {
    assert_eq!(size_of::<CheckPoint>(), 88);
    assert_eq!(size_of::<ControlFileData>(), 296);
    assert_eq!(offset_of!(ControlFileData, crc), 292);
    assert_eq!(offset_of!(ControlFileData, state), 16);
    assert_eq!(offset_of!(ControlFileData, time), 24);
    assert_eq!(offset_of!(ControlFileData, checkPointCopy), 40);
    assert_eq!(offset_of!(ControlFileData, unloggedLSN), 128);
    assert_eq!(offset_of!(ControlFileData, mock_authentication_nonce), 257);
    assert_eq!(offset_of!(CheckPoint, nextXid), 24);
    assert_eq!(offset_of!(CheckPoint, time), 64);
    assert_eq!(offset_of!(CheckPoint, oldestActiveXid), 80);
}

#[test]
fn checkpoint_byte_roundtrip() {
    let mut ckpt = CheckPoint::ZEROED;
    ckpt.redo = 0x0123_4567_89AB_CDEF;
    ckpt.ThisTimeLineID = 7;
    ckpt.PrevTimeLineID = 6;
    ckpt.fullPageWrites = true;
    ckpt.wal_level = WAL_LEVEL_REPLICA;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(2, 1234);
    ckpt.nextOid = 24576;
    ckpt.oldestXid = 3;
    ckpt.time = 1_700_000_000;
    ckpt.oldestActiveXid = 99;
    let bytes = ckpt.to_bytes().to_vec();
    assert_eq!(bytes.len(), 88);
    assert_eq!(CheckPoint::from_bytes(&bytes), ckpt);
}

fn init_seams_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        shmem::init_seams();
        guc_tables::init_seams();
        crate::init_seams();
    });
}

fn with_seg(size: i32, f: impl FnOnce()) {
    set_wal_segment_size(size);
    f();
}

#[test]
fn bytepos_recptr_roundtrip() {
    with_seg(16 * 1024 * 1024, || {
        for bytepos in [
            0u64,
            1,
            (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64 - 1,
            (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64,
            (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64 + 1,
            UsableBytesInPage * 3 + 17,
            UsableBytesInSegment() - 1,
            UsableBytesInSegment(),
            UsableBytesInSegment() + 12345,
            UsableBytesInSegment() * 5 + 7,
        ] {
            let ptr = XLogBytePosToRecPtr(bytepos);
            assert_eq!(XLogRecPtrToBytePos(ptr), bytepos, "bytepos {bytepos}");
        }
    });
}

#[test]
fn bytepos_end_recptr_page_boundary() {
    with_seg(16 * 1024 * 1024, || {
        // End position at exactly a page boundary points before the header.
        let one_page = (XLOG_BLCKSZ - SizeOfXLogLongPHD) as u64;
        let end = XLogBytePosToEndRecPtr(one_page);
        assert_eq!(end % XLOG_BLCKSZ as u64, 0);
        let start = XLogBytePosToRecPtr(one_page);
        assert_eq!(start % XLOG_BLCKSZ as u64, SizeOfXLogShortPHD as u64);
        assert_eq!(XLogBytePosToEndRecPtr(0), 0);
        assert_eq!(XLogBytePosToRecPtr(0), SizeOfXLogLongPHD as u64);
    });
}

#[test]
fn segment_arithmetic() {
    let seg = 16 * 1024 * 1024;
    assert_eq!(XLogSegmentsPerXLogId(seg), 256);
    assert_eq!(XLByteToSeg(seg as u64 * 3 + 5, seg), 3);
    assert_eq!(XLByteToPrevSeg(seg as u64 * 3, seg), 2);
    assert!(XLByteInPrevSeg(seg as u64 * 3, 2, seg));
    assert_eq!(XLogSegmentOffset(seg as u64 + 42, seg), 42);
    assert_eq!(XLogFileName(1, 1, seg), "000000010000000000000001");
    assert_eq!(XLogFileName(1, 256, seg), "000000010000000100000000");
    assert_eq!(XLogFilePath(1, 1, seg), "pg_wal/000000010000000000000001");
    assert!(IsValidWalSegSize(seg));
    assert!(!IsValidWalSegSize(seg - 1));
    assert!(!IsValidWalSegSize(512 * 1024));
}

#[test]
fn insert_freespace_and_align() {
    assert_eq!(INSERT_FREESPACE(0), 0);
    assert_eq!(INSERT_FREESPACE(1), XLOG_BLCKSZ - 1);
    assert_eq!(INSERT_FREESPACE(XLOG_BLCKSZ as u64), 0);
    assert_eq!(MAXALIGN(1), 8);
    assert_eq!(MAXALIGN(8), 8);
    assert_eq!(MAXALIGN64(9), 16);
}

#[test]
fn control_file_crc_detects_corruption() {
    let mut cf = ControlFileData::ZEROED;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.system_identifier = 0xDEADBEEF;
    let crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    cf.crc = crc;
    let mut other = cf;
    other.system_identifier ^= 1;
    let other_crc = controldata_utils::crc_of_image(&other.to_disk_bytes());
    assert_ne!(crc, other_crc);
}

#[test]
fn record_header_offsets() {
    // XLogRecord (xlogrecord.h): tot_len@0 xid@4 prev@8 info@16 rmid@17 crc@20.
    assert_eq!(SizeOfXLogRecord, 24);
}

#[test]
fn xlog_checkpoint_flags_match_c() {
    assert_eq!(CHECKPOINT_IS_SHUTDOWN, 0x0001);
    assert_eq!(CHECKPOINT_END_OF_RECOVERY, 0x0002);
    assert_eq!(CHECKPOINT_IMMEDIATE, 0x0004);
    assert_eq!(CHECKPOINT_FORCE, 0x0008);
    assert_eq!(CHECKPOINT_FLUSH_ALL, 0x0010);
    assert_eq!(CHECKPOINT_WAIT, 0x0020);
    assert_eq!(CHECKPOINT_CAUSE_XLOG, 0x0080);
    assert_eq!(CHECKPOINT_CAUSE_TIME, 0x0100);
}

// End-to-end single-backend smoke: control-file round trip, XLogCtl init to
// the clean-shutdown production state, record insert (small + page-crossing),
// flush, and on-disk verification. One test fn: shares process-global state.
#[test]
fn insert_flush_smoke() {
    use crate::control_file::*;
    use crate::ctl::*;
    use std::sync::atomic::Ordering::Relaxed;

    let dir = std::env::temp_dir().join(format!("pgrust_xlog_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal/archive_status", "pg_wal/summaries"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    init_seams_once();
    xact_seams::mark_current_transaction_id_logged_if_any::set(|| {});
    xact_seams::get_current_sub_transaction_id::set(|| 1);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();

    let seg = 16 * 1024 * 1024;
    let redo = seg as u64 + SizeOfXLogLongPHD as u64;
    let ckpt_len = MAXALIGN(SizeOfXLogRecord + 2 + size_of::<CheckPoint>());
    let end_of_log = redo + ckpt_len as u64;

    // Fabricate a clean-shutdown pg_control and read it back through the
    // real validation path.
    {
        let mut cf = ControlFileData::ZEROED;
        cf.system_identifier = 0x1122_3344_5566_7788;
        cf.pg_control_version = PG_CONTROL_VERSION;
        cf.catalog_version_no = CATALOG_VERSION_NO;
        cf.state = DB_SHUTDOWNED;
        cf.checkPoint = redo;
        cf.checkPointCopy.redo = redo;
        cf.checkPointCopy.ThisTimeLineID = 1;
        cf.checkPointCopy.PrevTimeLineID = 1;
        cf.checkPointCopy.fullPageWrites = true;
        cf.checkPointCopy.wal_level = WAL_LEVEL_REPLICA;
        cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
        cf.checkPointCopy.oldestXid = 3;
        cf.unloggedLSN = FirstNormalUnloggedLSN;
        cf.maxAlign = 8;
        cf.floatFormat = FLOATFORMAT_VALUE;
        cf.blcksz = 8192;
        cf.relseg_size = 131072;
        cf.xlog_blcksz = 8192;
        cf.xlog_seg_size = seg as u32;
        cf.nameDataLen = 64;
        cf.indexMaxKeys = 32;
        cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
        cf.loblksize = 2048;
        cf.float8ByVal = true;
        cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
        let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
        image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA]
            .copy_from_slice(&cf.to_disk_bytes());
        std::fs::write(dir.join("global/pg_control"), &image).unwrap();
    }
    ReadControlFile().unwrap();
    assert_eq!(GetSystemIdentifier(), 0x1122_3344_5566_7788);
    assert_eq!(wal_segment_size(), seg);
    assert!(CheckPointSegments() >= 1);

    // XLOGShmemInit + the StartupXLOG clean-shutdown tail.
    XLOGShmemInit();
    let ctl = XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(redo), Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(redo, Relaxed);
    ctl.RedoRecPtr.store(redo, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    // Partial last page: seed the buffer for the block holding end_of_log.
    let first_idx = XLogRecPtrToBufIdx(end_of_log) as usize;
    let page_begin = end_of_log - end_of_log % XLOG_BLCKSZ as u64;
    unsafe {
        let page = ctl.page_ptr(first_idx);
        std::ptr::write_bytes(page, 0, XLOG_BLCKSZ);
        crate::insert::write_u16(page, 0, XLOG_PAGE_MAGIC);
        crate::insert::write_u16(page, 2, XLP_LONG_HEADER);
        crate::insert::write_u32(page, 4, 1);
        crate::insert::write_u64(page, 8, page_begin);
    }
    ctl.xlblocks[first_idx].store(page_begin + XLOG_BLCKSZ as u64, Relaxed);
    ctl.InitializedUpTo.store(page_begin + XLOG_BLCKSZ as u64, Relaxed);
    crate::write::set_logwrt_result(end_of_log, end_of_log);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    crate::insert::set_local_redo_rec_ptr(redo);
    crate::insert::set_do_page_writes(true);
    crate::startup::SetInstallXLogFileSegmentActive().unwrap();
    xlogutils::set_in_recovery(false);

    assert!(!RecoveryInProgress());
    assert!(XLogInsertAllowed());

    // Record 1: small NOOP-shaped record.
    let body1: Vec<u8> = (0..64u8).collect();
    let tot_len1 = SizeOfXLogRecord + body1.len();
    let mut hdr = [0u8; 24];
    hdr[0..4].copy_from_slice(&(tot_len1 as u32).to_ne_bytes());
    hdr[16] = XLOG_NOOP;
    hdr[17] = RM_XLOG_ID;
    let body_crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &body1);
    hdr[20..24].copy_from_slice(&body_crc.to_ne_bytes());

    let end1 = XLogInsertRecord(&mut hdr, &[&body1], 0, 0, 0, false).unwrap();
    assert_eq!(end1, end_of_log + MAXALIGN(tot_len1) as u64);
    assert_eq!(crate::ProcLastRecPtr(), end_of_log);
    assert_eq!(crate::XactLastRecEnd(), end1);
    // xl_prev must point at the previous (checkpoint) record.
    let prev = u64::from_ne_bytes(hdr[8..16].try_into().unwrap());
    assert_eq!(prev, redo);
    // Full record CRC must verify like xlogreader does.
    let crc_in_hdr = u32::from_ne_bytes(hdr[20..24].try_into().unwrap());
    let expect = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &body1),
        &hdr[..20],
    ));
    assert_eq!(crc_in_hdr, expect);

    // Record 2: crosses a page boundary; contrecord machinery must fire.
    let body2 = vec![0xABu8; XLOG_BLCKSZ];
    let tot_len2 = SizeOfXLogRecord + body2.len();
    let mut hdr2 = [0u8; 24];
    hdr2[0..4].copy_from_slice(&(tot_len2 as u32).to_ne_bytes());
    hdr2[16] = XLOG_NOOP;
    hdr2[17] = RM_XLOG_ID;
    let body_crc2 = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &body2);
    hdr2[20..24].copy_from_slice(&body_crc2.to_ne_bytes());
    let end2 = XLogInsertRecord(&mut hdr2, &[&body2], 0, 0, 0, false).unwrap();
    let prev2 = u64::from_ne_bytes(hdr2[8..16].try_into().unwrap());
    assert_eq!(prev2, end_of_log);
    assert!(end2 > end1);

    // In-buffer verification (runs under Miri; file IO below does not).
    unsafe {
        let idx = crate::ctl::XLogRecPtrToBufIdx(end_of_log) as usize;
        let page = crate::ctl::XLogCtl().page_ptr(idx);
        let off = (end_of_log % XLOG_BLCKSZ as u64) as usize;
        let got = std::slice::from_raw_parts(page.add(off), 24);
        assert_eq!(got, &hdr);
        let got_body = std::slice::from_raw_parts(page.add(off + 24), body1.len());
        assert_eq!(got_body, &body1[..]);
    }
    if cfg!(miri) {
        return;
    }

    // Flush and verify on disk.
    XLogFlush(end2).unwrap();
    assert!(!XLogNeedsFlush(end2));

    // Pinning: the flush tail must request a walsender wakeup even under
    // open_sync/open_datasync wal_sync_method (C signals WalSndWakeupRequest
    // OUTSIDE the sync-method guard, xlog.c:2553 — no explicit fsync happens
    // on that path but walsenders still need the wakeup).
    {
        use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
        static WAKEUPS: AtomicUsize = AtomicUsize::new(0);
        static MWS: AtomicI32 = AtomicI32::new(10);
        walsender_seams::wal_snd_wakeup::set(|_, _| {
            WAKEUPS.fetch_add(1, Ordering::Relaxed);
        });
        guc_tables::vars::max_wal_senders.install_if_absent(guc_tables::GucVarAccessors {
            get: || MWS.load(Ordering::Relaxed),
            set: |v| MWS.store(v, Ordering::Relaxed),
        });
        let saved_method = guc_tables::vars::wal_sync_method.read();
        guc_tables::vars::wal_sync_method.write(WAL_SYNC_METHOD_OPEN_DSYNC);
        let body3: Vec<u8> = (0..32u8).collect();
        let tot_len3 = SizeOfXLogRecord + body3.len();
        let mut hdr3 = [0u8; 24];
        hdr3[0..4].copy_from_slice(&(tot_len3 as u32).to_ne_bytes());
        hdr3[16] = XLOG_NOOP;
        hdr3[17] = RM_XLOG_ID;
        let body_crc3 = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &body3);
        hdr3[20..24].copy_from_slice(&body_crc3.to_ne_bytes());
        let end3 = XLogInsertRecord(&mut hdr3, &[&body3], 0, 0, 0, false).unwrap();
        let before = WAKEUPS.load(Ordering::Relaxed);
        XLogFlush(end3).unwrap();
        assert!(
            WAKEUPS.load(Ordering::Relaxed) > before,
            "flush under open_datasync must still wake walsenders (xlog.c:2553)"
        );
        guc_tables::vars::wal_sync_method.write(saved_method);
    }

    // commit_delay/commit_siblings group-commit gate (xlog.c XLogFlush):
    // the flush sleeps commit_delay before writing ONLY when commit_delay > 0
    // AND fsync is enabled AND MinimumActiveBackends(commit_siblings) — the
    // procarray seam. Legs: gate not consulted with commit_delay=0; not
    // consulted with fsync off; consulted-no-sleep without siblings;
    // consulted-and-slept with siblings.
    {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        static GATE_CALLS: AtomicUsize = AtomicUsize::new(0);
        static GATE_ANSWER: AtomicBool = AtomicBool::new(false);
        procarray_seams::minimum_active_backends::set(|min| {
            assert_eq!(min, guc_tables::vars::CommitSiblings.read());
            GATE_CALLS.fetch_add(1, Ordering::Relaxed);
            GATE_ANSWER.load(Ordering::Relaxed)
        });
        let mut insert_noop = |fill: u8| {
            let body: Vec<u8> = vec![fill; 48];
            let tot_len = SizeOfXLogRecord + body.len();
            let mut h = [0u8; 24];
            h[0..4].copy_from_slice(&(tot_len as u32).to_ne_bytes());
            h[16] = XLOG_NOOP;
            h[17] = RM_XLOG_ID;
            let crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &body);
            h[20..24].copy_from_slice(&crc.to_ne_bytes());
            XLogInsertRecord(&mut h, &[&body], 0, 0, 0, false).unwrap()
        };

        // commit_delay = 0 (shipped default): gate never consulted.
        assert_eq!(guc_tables::vars::CommitDelay.read(), 0);
        let end = insert_noop(0x01);
        XLogFlush(end).unwrap();
        assert_eq!(GATE_CALLS.load(Ordering::Relaxed), 0);

        // commit_delay > 0 but fsync disabled: still not consulted (C's
        // conjunct order: CommitDelay > 0 && enableFsync && gate).
        guc_tables::vars::CommitDelay.write(100_000);
        assert!(!init_small::globals::enableFsync());
        let end = insert_noop(0x02);
        XLogFlush(end).unwrap();
        assert_eq!(GATE_CALLS.load(Ordering::Relaxed), 0);

        // fsync on, gate says too few siblings: consulted, no delay taken.
        init_small::globals::set_enableFsync(true);
        let end = insert_noop(0x03);
        XLogFlush(end).unwrap();
        assert_eq!(GATE_CALLS.load(Ordering::Relaxed), 1);

        // Siblings present: the flush must sleep >= commit_delay (100ms)
        // before writing. thread::sleep guarantees the lower bound.
        GATE_ANSWER.store(true, Ordering::Relaxed);
        let end = insert_noop(0x04);
        let t0 = std::time::Instant::now();
        XLogFlush(end).unwrap();
        assert_eq!(GATE_CALLS.load(Ordering::Relaxed), 2);
        assert!(
            t0.elapsed() >= std::time::Duration::from_micros(100_000),
            "commit_delay sleep did not happen: {:?}",
            t0.elapsed()
        );

        // Restore the substrate posture (delay off, fsync off).
        GATE_ANSWER.store(false, Ordering::Relaxed);
        guc_tables::vars::CommitDelay.write(0);
        init_small::globals::set_enableFsync(false);
    }
    let segpath = dir.join(format!("pg_wal/{}", XLogFileName(1, XLByteToSeg(end_of_log, seg), seg)));
    let file = std::fs::read(&segpath).unwrap_or_else(|e| {
        let names: Vec<_> = std::fs::read_dir(dir.join("pg_wal")).unwrap().map(|x| x.unwrap().file_name()).collect();
        panic!("segment missing: {e}; pg_wal = {names:?}")
    });
    assert_eq!(file.len(), seg as usize);
    // Record 1 header on disk at its in-segment offset.
    let off1 = (end_of_log % seg as u64) as usize;
    assert_eq!(&file[off1..off1 + 24], &hdr);
    assert_eq!(&file[off1 + 24..off1 + 24 + body1.len()], &body1[..]);
    // The next page must be a contrecord page: xlp_info bit + rem_len set.
    let page2 = (off1 / XLOG_BLCKSZ + 1) * XLOG_BLCKSZ;
    let info = u16::from_ne_bytes(file[page2 + 2..page2 + 4].try_into().unwrap());
    assert!(info & XLP_FIRST_IS_CONTRECORD != 0);
    let rem = u32::from_ne_bytes(file[page2 + 16..page2 + 20].try_into().unwrap());
    assert!(rem > 0 && (rem as usize) < tot_len2);
    let magic = u16::from_ne_bytes(file[page2..page2 + 2].try_into().unwrap());
    assert_eq!(magic, XLOG_PAGE_MAGIC);

    // Crash-cycle reset over the maximally dirty XLogCtl: boot image restored.
    XLOGShmemResetAfterCrash();
    assert_eq!(ctl.Insert.CurrBytePos.load(Relaxed), 0);
    assert_eq!(ctl.Insert.PrevBytePos.load(Relaxed), 0);
    assert_eq!(ctl.Insert.RedoRecPtr.load(Relaxed), InvalidXLogRecPtr);
    assert!(!ctl.Insert.fullPageWrites.load(Relaxed));
    for l in &ctl.Insert.WALInsertLocks {
        assert_eq!(l.lock.state.load(Relaxed), lwlock::LW_FLAG_RELEASE_OK);
        assert_eq!(l.insertingAt.load(Relaxed), InvalidXLogRecPtr);
        assert_eq!(l.lastImportantAt.load(Relaxed), InvalidXLogRecPtr);
    }
    assert_eq!(ctl.RedoRecPtr.load(Relaxed), InvalidXLogRecPtr);
    assert_eq!(ctl.LogwrtRqstWrite.load(Relaxed), 0);
    assert_eq!(ctl.LogwrtRqstFlush.load(Relaxed), 0);
    assert_eq!(ctl.logInsertResult.load(Relaxed), InvalidXLogRecPtr);
    assert_eq!(ctl.logWriteResult.load(Relaxed), InvalidXLogRecPtr);
    assert_eq!(ctl.logFlushResult.load(Relaxed), InvalidXLogRecPtr);
    assert_eq!(ctl.InitializedUpTo.load(Relaxed), InvalidXLogRecPtr);
    assert_eq!(ctl.InsertTimeLineID.load(Relaxed), 0);
    assert_eq!(ctl.SharedRecoveryState.load(Relaxed), RECOVERY_STATE_CRASH);
    assert!(!ctl.InstallXLogFileSegmentActive.load(Relaxed));
    for b in ctl.xlblocks.iter() {
        assert_eq!(b.load(Relaxed), InvalidXLogRecPtr);
    }
    unsafe {
        let page = ctl.page_ptr(first_idx);
        assert!(std::slice::from_raw_parts(page, XLOG_BLCKSZ).iter().all(|&b| b == 0));
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "child of checkpoint_without_sync_seams_is_loud"]
fn checkpoint_no_sync_seams_child() {
    use crate::control_file::*;
    use std::sync::atomic::Ordering::Relaxed;

    let dir = std::env::temp_dir().join(format!("pgrust_ckpt_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal/archive_status", "pg_wal/summaries"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);
    shmem::init_seams();
    guc_tables::init_seams();
    crate::init_seams();
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();

    let seg = 16 * 1024 * 1024;
    let redo = seg as u64 + SizeOfXLogLongPHD as u64;
    let mut cf = ControlFileData::ZEROED;
    cf.system_identifier = 0x1122_3344_5566_7788;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = CATALOG_VERSION_NO;
    cf.state = DB_SHUTDOWNED;
    cf.checkPoint = redo;
    cf.checkPointCopy.redo = redo;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = seg as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
    ReadControlFile().unwrap();
    XLOGShmemInit();
    crate::ctl::XLogCtl().SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    xlogutils::set_in_recovery(false);

    // Sync seams deliberately NOT installed: the checkpoint must panic
    // loudly, never report success without fsync.
    let _ = crate::CreateCheckPoint(CHECKPOINT_IMMEDIATE);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn checkpoint_without_sync_seams_is_loud() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::checkpoint_no_sync_seams_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "checkpoint must not succeed: {out:?}");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("seam not installed: sync_seams::"),
        "must fail loudly at the sync seam, got: {text}"
    );
}

#[test]
fn xlog_filename_parse_roundtrip() {
    let seg = 16 * 1024 * 1024;
    with_seg(seg, || {
        for segno in [1u64, 255, 256, 0xFF_FFFF, 0x1_0000_0000 / seg as u64, 12345678] {
            let name = XLogFileName(3, segno, seg);
            assert!(crate::removal::IsXLogFileName(&name), "{name}");
            assert_eq!(crate::removal::XLogFromFileName(&name, seg), (3, segno));
        }
        assert!(!crate::removal::IsXLogFileName("00000001000000000000000g"));
        assert!(!crate::removal::IsXLogFileName("000000010000000000000001.partial"));
        assert!(crate::removal::IsPartialXLogFileName("000000010000000000000001.partial"));
    });
}

#[test]
fn keep_log_seg_matches_c() {
    let seg = 16 * 1024 * 1024;
    with_seg(seg, || {
        init_seams_once();
        let recptr = 100 * seg as u64 + 1234;

        // No slots, no wal_keep_size: horizon untouched.
        guc_tables::vars::wal_keep_size_mb.write(0);
        guc_tables::vars::max_slot_wal_keep_size_mb.write(-1);
        let mut segno = 90;
        assert!(!crate::removal::keep_log_seg_with(recptr, &mut segno, InvalidXLogRecPtr));
        assert_eq!(segno, 90);

        // A slot restart_lsn pins its segment.
        let mut segno = 90;
        assert!(!crate::removal::keep_log_seg_with(recptr, &mut segno, 40 * seg as u64 + 7));
        assert_eq!(segno, 40);

        // max_slot_wal_keep_size caps the slot horizon and reports it.
        guc_tables::vars::max_slot_wal_keep_size_mb.write(16 * 10);
        let mut segno = 90;
        assert!(crate::removal::keep_log_seg_with(recptr, &mut segno, 40 * seg as u64 + 7));
        assert_eq!(segno, 100 - 10);

        // wal_keep_size holds segments back without any slot.
        guc_tables::vars::max_slot_wal_keep_size_mb.write(-1);
        guc_tables::vars::wal_keep_size_mb.write(16 * 5);
        let mut segno = 99;
        assert!(!crate::removal::keep_log_seg_with(recptr, &mut segno, InvalidXLogRecPtr));
        assert_eq!(segno, 95);

        // wal_keep_size larger than history bottoms out at segment 1.
        guc_tables::vars::wal_keep_size_mb.write(16 * 200);
        let mut segno = 99;
        assert!(!crate::removal::keep_log_seg_with(recptr, &mut segno, InvalidXLogRecPtr));
        assert_eq!(segno, 1);
        guc_tables::vars::wal_keep_size_mb.write(0);
    });
}

#[test]
fn xlog_fileslop_clamps_to_wal_size_bounds() {
    let seg = 16 * 1024 * 1024;
    with_seg(seg, || {
        init_seams_once();
        guc_tables::vars::min_wal_size_mb.write(5 * 16);
        guc_tables::vars::max_wal_size_mb.write(64 * 16);
        guc_tables::vars::CheckPointCompletionTarget.write(0.9);
        let lastredo = 100 * seg as u64;

        // Zero distance estimate: floor at min_wal_size worth of segments.
        crate::removal::UpdateCheckPointDistanceEstimate(0);
        assert_eq!(crate::removal::XLOGfileslop(lastredo), 100 + 5 - 1);

        // Huge estimate: ceiling at max_wal_size worth of segments.
        crate::removal::UpdateCheckPointDistanceEstimate(10_000 * seg as u64);
        assert_eq!(crate::removal::XLOGfileslop(lastredo), 100 + 64 - 1);
    });
}

#[test]
fn wal_consistency_checking_hook_rejects_cleanly() {
    // Any non-empty value refuses via the GUC protocol (Ok(false) -> clean
    // ERROR at SET / FATAL at boot), never a panic: the FPW cross-check is
    // unported and a silent accept would be a silent skip.
    let mut extra = None;
    for v in ["all", "heap", "nonsense"] {
        let mut newval = Some(v.to_string());
        assert_eq!(
            crate::check_wal_consistency_checking_hook(
                &mut newval,
                &mut extra,
                types_guc::GucSource::PGC_S_TEST,
            )
            .unwrap(),
            false,
            "non-empty \"{v}\" must refuse"
        );
    }
    // The disabled settings stay accepted.
    for newval in [None, Some(String::new())] {
        let mut newval = newval;
        assert!(crate::check_wal_consistency_checking_hook(
            &mut newval,
            &mut extra,
            types_guc::GucSource::PGC_S_TEST,
        )
        .unwrap());
    }
}
