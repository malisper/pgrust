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

// Tests that take ControlFileLock run without proc seams: any contention on
// it is C's "cannot wait without a PGPROC structure" PANIC, so they serialize
// here instead of racing.
static CONTROL_FILE_LOCK_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn control_file_lock_gate() -> std::sync::MutexGuard<'static, ()> {
    CONTROL_FILE_LOCK_GATE.lock().unwrap_or_else(|e| e.into_inner())
}

// CreateLWLocks publishes MainLWLockArray exactly once per process; tests
// sharing the process reuse the published table.
fn create_lwlocks_once() {
    if lwlock::published_lwlock_table().is_none() {
        lwlock::CreateLWLocks(false).unwrap();
    }
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

    let _gate = control_file_lock_gate();

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
    create_lwlocks_once();
    // C: XLogFlush runs with MyProc set (WaitXLogInsertionsToFinish PANICs
    // "cannot wait without a PGPROC structure" otherwise, xlog.c:1516-1517);
    // this backend is proc 0 for the harness.
    init_small::globals::SetMyProcNumber(0);

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
        // Audit a186-candidate-fp-transam-xlog-p1-3a18fce763f2c559db63-1:
        // C processes the wakeup request in XLogFlush AFTER LWLockRelease
        // (WALWriteLock) and END_CRIT_SECTION (xlog.c:2905-2913); XLogWrite
        // only sets the flag (2482/2554). Count wakeups delivered while the
        // flusher still holds WALWriteLock or is inside a critical section.
        static WAKEUPS_UNDER_WAL_WRITE_LOCK: AtomicUsize = AtomicUsize::new(0);
        static WAKEUPS_IN_CRIT_SECTION: AtomicUsize = AtomicUsize::new(0);
        static MWS: AtomicI32 = AtomicI32::new(10);
        walsender_seams::wal_snd_wakeup::set(|_, _| {
            WAKEUPS.fetch_add(1, Ordering::Relaxed);
            if lwlock::LWLockHeldByMe(crate::ctl::WALWriteLock()) {
                WAKEUPS_UNDER_WAL_WRITE_LOCK.fetch_add(1, Ordering::Relaxed);
            }
            if init_small::globals::CritSectionCount() > 0 {
                WAKEUPS_IN_CRIT_SECTION.fetch_add(1, Ordering::Relaxed);
            }
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
        assert_eq!(
            WAKEUPS_UNDER_WAL_WRITE_LOCK.load(Ordering::Relaxed),
            0,
            "walsender wakeups must be processed after WALWriteLock is released (xlog.c:2905-2913)"
        );
        assert_eq!(
            WAKEUPS_IN_CRIT_SECTION.load(Ordering::Relaxed),
            0,
            "walsender wakeups must be processed after END_CRIT_SECTION (xlog.c:2910-2913)"
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
    // upstream 5b3f63a1bf59 (18.4): a record that ends exactly on a page
    // boundary. GetXLogInsertRecPtr then points past the next page header
    // (nothing is flushable there: "xlog flush request ... is not
    // satisfied"), which is what gistGetFakeLSN used to stamp pages with;
    // GetXLogInsertEndRecPtr is the boundary itself and flushes.
    {
        let insert_end = GetXLogInsertEndRecPtr();
        assert_eq!(insert_end, GetXLogInsertRecPtr(), "mid-page: both agree");
        let fill = INSERT_FREESPACE(insert_end);
        assert!(fill >= SizeOfXLogRecord + 8, "need room for one record on this page");
        let body: Vec<u8> = vec![0x5A; fill - SizeOfXLogRecord];
        let tot_len = SizeOfXLogRecord + body.len();
        assert_eq!(MAXALIGN(tot_len), tot_len);
        let mut h = [0u8; 24];
        h[0..4].copy_from_slice(&(tot_len as u32).to_ne_bytes());
        h[16] = XLOG_NOOP;
        h[17] = RM_XLOG_ID;
        let crc = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &body);
        h[20..24].copy_from_slice(&crc.to_ne_bytes());
        let end = XLogInsertRecord(&mut h, &[&body], 0, 0, 0, false).unwrap();
        assert_eq!(end % XLOG_BLCKSZ as u64, 0, "record ends on the page boundary");
        assert_eq!(GetXLogInsertEndRecPtr(), end);
        assert_eq!(GetXLogInsertRecPtr(), end + SizeOfXLogShortPHD as u64);
        XLogFlush(GetXLogInsertEndRecPtr()).unwrap();
        let err = XLogFlush(GetXLogInsertRecPtr()).unwrap_err();
        assert_eq!(
            err.message,
            format!(
                "xlog flush request {:X}/{:X} is not satisfied --- flushed only to {:X}/{:X}",
                (end + SizeOfXLogShortPHD as u64) >> 32,
                (end + SizeOfXLogShortPHD as u64) as u32,
                end >> 32,
                end as u32
            )
        );
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
    // The checkpoint's XLogFlush needs a PGPROC (xlog.c:1516-1517).
    init_small::globals::SetMyProcNumber(0);

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

    // CheckpointStats.ckpt_start_t (xlog.c:6964) is stamped before the
    // sync seams are reached.
    timestamp_seams::get_current_timestamp::set(|| 0);

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

// WaitXLogInsertionsToFinish (xlog.c:1516-1517): `if (MyProc == NULL)
// elog(PANIC, "cannot wait without a PGPROC structure")` before any other
// work. Runs as a child process: the PANIC unwinds PanicExitThread. Audit
// a186-candidate-fp-transam-xlog-p1-06bad52eb57d336f184d-1.
#[test]
#[ignore = "child of wait_xlog_insertions_without_pgproc_is_a_panic"]
fn wait_xlog_insertions_without_pgproc_child() {
    use crate::ctl::XLOGShmemInit;

    init_seams_once();
    elog::init_seams();
    fd::InitFileAccess();
    create_lwlocks_once();
    XLOGShmemInit();
    assert_eq!(init_small::globals::MyProcNumber(), types_core::INVALID_PROC_NUMBER);

    // Nothing has been inserted (logInsertResult == 0), so a request past it
    // reaches the wait loop; C never gets that far without a PGPROC.
    let r = std::panic::catch_unwind(|| crate::insert::WaitXLogInsertionsToFinish(1));
    match r {
        Err(payload) if payload.is::<types_error::PanicExitThread>() => std::process::exit(42),
        _ => std::process::exit(0),
    }
}

#[test]
fn wait_xlog_insertions_without_pgproc_is_a_panic() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::wait_xlog_insertions_without_pgproc_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(42),
        "WaitXLogInsertionsToFinish without a PGPROC must PANIC (xlog.c:1516-1517): {out:?}"
    );
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("PANIC:  cannot wait without a PGPROC structure"),
        "missing the C PANIC report, got: {text}"
    );
}

// ReadControlFile (xlog.c:4541-4547) reports an invalid segment size with
// errmsg_plural: xlog_seg_size = 1 reads "(1 byte)". Runs as a child process
// because ReadControlFile reads the process-global DataDir that
// insert_flush_smoke also owns.
// Audit a186-candidate-fp-transam-xlog-p2-44ba836f77f0e758da0c-1.
#[test]
#[ignore = "child of read_control_file_one_byte_segment_size_is_singular"]
fn read_control_file_one_byte_segment_child() {
    use crate::control_file::*;

    let dir = std::env::temp_dir().join(format!("pgrust_ctl1_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("global")).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_seams_once();
    fd::InitFileAccess();

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
    cf.xlog_seg_size = 1;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();

    let err = ReadControlFile().unwrap_err();
    let _ = std::fs::remove_dir_all(&dir);
    println!(
        "CTL1 sqlstate_is_22023={} message={}",
        err.sqlstate() == types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        err.message()
    );
}

#[test]
fn read_control_file_one_byte_segment_size_is_singular() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::read_control_file_one_byte_segment_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "child failed: {text}");
    assert!(
        text.contains("CTL1 sqlstate_is_22023=true message=invalid WAL segment size in control file (1 byte)\n"),
        "C prints '(1 byte)' for xlog_seg_size = 1, got: {text}"
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

// A stand-in maskable-rmgr table so the parse can resolve real names without
// pulling the whole rmgr crate into these unit tests. Ids mirror rmgrlist.h
// (Heap=10, Btree=11, BRIN=18).
fn test_maskable_rmgrs() -> Vec<(&'static str, u8)> {
    vec![("Heap", 10), ("Btree", 11), ("BRIN", 18)]
}

fn check(spec: Option<&str>) -> (bool, Option<guc_tables::GucHookExtra>) {
    let mut newval = spec.map(|s| s.to_string());
    let mut extra = None;
    let ok = crate::check_wal_consistency_checking_hook(
        &mut newval,
        &mut extra,
        types_guc::GucSource::PGC_S_TEST,
    )
    .unwrap();
    (ok, extra)
}

fn flags_of(extra: &Option<guc_tables::GucHookExtra>) -> [bool; crate::RM_N_IDS] {
    *extra
        .as_ref()
        .unwrap()
        .downcast_ref::<[bool; crate::RM_N_IDS]>()
        .unwrap()
}

// The maskable-rmgr seam installs once per test process (a second `set`
// panics "seam installed twice"); every test that parses the GUC shares it.
fn install_test_maskable_rmgrs() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| transam_xlog_seams::wal_consistency_maskable_rmgrs::set(test_maskable_rmgrs));
}

#[test]
fn wal_consistency_checking_hook_accepts_and_parses() {
    install_test_maskable_rmgrs();

    // Disabled settings: accepted, all-false.
    for spec in [None, Some(""), Some("   ")] {
        let (ok, extra) = check(spec);
        assert!(ok, "{spec:?} must be accepted");
        assert!(flags_of(&extra).iter().all(|&b| !b), "{spec:?} => all false");
    }

    // "all" selects exactly the maskable rmgrs.
    let (ok, extra) = check(Some("all"));
    assert!(ok);
    let f = flags_of(&extra);
    assert!(f[10] && f[11] && f[18]);
    assert_eq!(f.iter().filter(|&&b| b).count(), 3);

    // Case-insensitive single name, and a whitespace-padded comma list.
    let (ok, extra) = check(Some("HeAp"));
    assert!(ok);
    let f = flags_of(&extra);
    assert!(f[10] && !f[11] && !f[18]);

    let (ok, extra) = check(Some("  heap ,  brin "));
    assert!(ok);
    let f = flags_of(&extra);
    assert!(f[10] && f[18] && !f[11]);

    // Unknown keyword and malformed list are rejected (Ok(false)).
    for bad in ["nonsense", "heap,,brin", "heap,", ",heap", "he ap"] {
        let (ok, _) = check(Some(bad));
        assert!(!ok, "{bad:?} must be rejected");
    }

    // assign applies the parsed array to the per-thread flag state.
    let (_ok, extra) = check(Some("brin"));
    crate::assign_wal_consistency_checking_hook(Some("brin"), extra.as_ref());
    assert!(crate::wal_consistency_checking(18));
    assert!(!crate::wal_consistency_checking(10));

    // Clearing the GUC turns the hot-path gate back off.
    let (_ok, extra) = check(Some(""));
    crate::assign_wal_consistency_checking_hook(Some(""), extra.as_ref());
    assert!(!crate::wal_consistency_checking(18));
}

// check_wal_consistency_checking (xlog.c:4722) parses the list with
// SplitIdentifierString (varlena.c:3581): a double-quoted element is taken
// verbatim with `""` collapsed to `"`, an unquoted element is downcased
// (the rejection detail echoes the downcased word), and anything after a
// closing quote that is not whitespace / the separator is "List syntax is
// invalid.". Audit a186-candidate-fp-transam-xlog-p2-f05906940910de3dddbf-1.
#[test]
fn wal_consistency_checking_parses_quoted_identifiers_like_c() {
    install_test_maskable_rmgrs();
    let parse = crate::parse_wal_consistency_checking;

    // SET wal_consistency_checking = '"heap"': C accepts (pg_strcasecmp on
    // the unquoted name).
    let f = parse("\"heap\"").expect("\"heap\" is a valid quoted rmgr name");
    assert!(f[10] && !f[11] && !f[18]);

    // Quoted names keep their case and are still matched case-insensitively.
    let f = parse("heap, \"Brin\" ,BTREE").expect("mixed quoted/unquoted list");
    assert!(f[10] && f[11] && f[18]);

    // The detail echoes the DOWNCASED unquoted word (downcase_truncate_identifier).
    assert_eq!(parse("Foo").unwrap_err(), "Unrecognized key word: \"foo\".");
    // A quoted word is echoed verbatim, with "" collapsed to ".
    assert_eq!(parse("\"he\"\"ap\"").unwrap_err(), "Unrecognized key word: \"he\"ap\".");
    assert_eq!(parse("\"\"").unwrap_err(), "Unrecognized key word: \"\".");
    // Trailing junk after a closing quote and an unterminated quote are
    // SplitIdentifierString `false` returns.
    assert_eq!(parse("\"heap\"x").unwrap_err(), "List syntax is invalid.");
    assert_eq!(parse("\"heap").unwrap_err(), "List syntax is invalid.");
}

// get_sync_bit (xlog.c:8666-8671): debug_io_direct=wal ORs PG_O_DIRECT into
// the open flags regardless of fsync, except in the walreceiver. Audit
// a186-candidate-fp-transam-xlog-p4-4a3a3de1fd8cbf2deb7c-1.
#[test]
fn get_sync_bit_sets_o_direct_for_debug_io_direct_wal() {
    let saved_flags = fd::io_direct_flags();
    let saved_fsync = init_small::globals::enableFsync();
    fd::set_io_direct_flags(::types_storage::IO_DIRECT_WAL);
    init_small::globals::set_enableFsync(false);
    let bit = crate::write::get_sync_bit(guc_tables::consts::WAL_SYNC_METHOD_FDATASYNC);
    fd::set_io_direct_flags(saved_flags);
    init_small::globals::set_enableFsync(saved_fsync);
    assert_eq!(bit, vfs::PG_O_DIRECT, "io_direct_flags & IO_DIRECT_WAL must open WAL with PG_O_DIRECT");
}

// update_checkpoint_display (xlog.c:6864-6885): the ps activity is
// "performing %s%s%s" for end-of-recovery / shutdown checkpoints and
// shutdown restartpoints; other checkpoints leave the title alone. Audit
// a186-candidate-fp-transam-xlog-p3-a70efef0e3d3bf63c50e-1.
#[test]
fn checkpoint_display_activity_matches_c_titles() {
    use crate::startup::checkpoint_display_activity as act;
    assert_eq!(act(CHECKPOINT_IS_SHUTDOWN, false), "performing shutdown checkpoint");
    assert_eq!(act(CHECKPOINT_END_OF_RECOVERY, false), "performing end-of-recovery checkpoint");
    assert_eq!(
        act(CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IS_SHUTDOWN, false),
        "performing end-of-recovery shutdown checkpoint"
    );
    assert_eq!(act(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE, true), "performing shutdown restartpoint");
}

// issue_xlog_fsync (xlog.c:8771-8785): the PANIC text names the primitive
// that failed — fsync, write-through fsync or fdatasync. Audit
// a186-candidate-fp-transam-xlog-p4-a1f6c559dd318ae09d31-1.
#[test]
fn issue_xlog_fsync_failure_messages_match_c() {
    use crate::write::fsync_failure_message;
    assert_eq!(fsync_failure_message(WAL_SYNC_METHOD_FSYNC), "could not fsync file \"%s\": %m");
    assert_eq!(
        fsync_failure_message(WAL_SYNC_METHOD_FSYNC_WRITETHROUGH),
        "could not fsync write-through file \"%s\": %m"
    );
    assert_eq!(
        fsync_failure_message(WAL_SYNC_METHOD_FDATASYNC),
        "could not fdatasync file \"%s\": %m"
    );
}

// ResetInstallXLogFileSegmentActive (xlog.c:9556-9561) flips the flag under
// ControlFileLock (LW_EXCLUSIVE), like SetInstallXLogFileSegmentActive and
// the InstallXLogFileSegment readers. With the lock held by this thread, a
// reset from another thread must NOT flip the flag; without a PGPROC (no proc
// seams in this harness) its contended acquire surfaces as the C "cannot wait
// without a PGPROC structure" PANIC error instead of blocking. Audit
// a186-candidate-fp-transam-xlog-p4-fc5d4d08f28f95c28b83-1.
#[test]
fn reset_install_xlog_file_segment_active_takes_control_file_lock() {
    use crate::ctl::{ControlFileLock, XLogCtl, XLOGShmemInit};
    use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
    use std::sync::atomic::Ordering::Relaxed;

    let _gate = control_file_lock_gate();
    init_seams_once();
    fd::InitFileAccess();
    create_lwlocks_once();
    XLOGShmemInit();

    crate::startup::SetInstallXLogFileSegmentActive().unwrap();
    assert!(XLogCtl().InstallXLogFileSegmentActive.load(Relaxed));

    LWLockAcquire(ControlFileLock(), LW_EXCLUSIVE, 0).unwrap();
    let reset = std::thread::spawn(crate::startup::ResetInstallXLogFileSegmentActive)
        .join()
        .unwrap();
    let flag_while_held = XLogCtl().InstallXLogFileSegmentActive.load(Relaxed);
    LWLockRelease(ControlFileLock()).unwrap();
    assert!(
        flag_while_held,
        "ResetInstallXLogFileSegmentActive cleared the flag while ControlFileLock was held elsewhere"
    );
    let err = reset.expect_err("contended acquire without a PGPROC is a PANIC error");
    assert!(
        err.message().contains("cannot wait without a PGPROC structure"),
        "got: {}",
        err.message()
    );

    crate::startup::ResetInstallXLogFileSegmentActive().unwrap();
    assert!(!XLogCtl().InstallXLogFileSegmentActive.load(Relaxed));
}

// xlog.c:8452-8454 (XLOG_CHECKPOINT_ONLINE): the replayed checkpoint's
// oldestXid is adopted iff TransactionIdPrecedes(TransamVariables->oldestXid,
// checkPoint.oldestXid) — a MODULAR comparison — and adopted through
// SetTransactionIdLimit, which also recomputes the vac/warn/stop/wrap
// limits. A primary that froze past the 2^32 boundary ships an oldestXid
// that is numerically SMALLER than the standby's pre-wrap value but
// modularly LATER; an unsigned `<` leaves the standby's horizon stuck on
// the pre-wrap value. Audit a186-candidate-fp-transam-xlog-p4-daa29892ce672c518c5c-1.
fn redo_seams_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        xlogutils::init_seams();
        xlogrecovery_seams::get_current_replay_rec_ptr::set(|| (0, 1));
        smgr_seams::smgr_destroy_all::set(|| Ok(()));
        varsup::VarsupShmemInit();
    });
}

fn online_checkpoint_record(
    ckpt: &CheckPoint,
    buf: &mut Vec<u8>,
) -> xlogreader_seams::XLogReaderState {
    *buf = ckpt.to_bytes().to_vec();
    let rec = xlogreader_seams::DecodedXLogRecord {
        xl_info: XLOG_CHECKPOINT_ONLINE,
        xl_rmid: RM_XLOG_ID,
        main_data: buf.as_ptr(),
        main_data_len: buf.len() as u32,
        ..Default::default()
    };
    xlogreader_seams::XLogReaderState { record: Some(rec), ..Default::default() }
}

#[test]
fn online_checkpoint_redo_advances_oldest_xid_across_wraparound() {
    use crate::ctl::XLOGShmemInit;
    use std::sync::atomic::Ordering::Relaxed;

    let _gate = control_file_lock_gate();
    init_seams_once();
    create_lwlocks_once();
    redo_seams_once();
    XLOGShmemInit();

    let tv = varsup::TransamVariables();
    // Standby state from its startup checkpoint: horizon just below 2^32,
    // counter already wrapped into epoch 1.
    let pre_wrap_oldest: types_core::TransactionId = 4_294_900_000;
    tv.oldestXid.store(pre_wrap_oldest, Relaxed);
    tv.oldestXidDB.store(1, Relaxed);
    tv.nextXid
        .store(types_core::FullTransactionId::from_epoch_and_xid(1, 2000).value, Relaxed);

    // The primary froze everything after wrapping: oldestXid = 1500 is
    // modularly later than 4_294_900_000 (TransactionIdPrecedes is true)
    // though numerically smaller.
    let mut ckpt = CheckPoint::ZEROED;
    ckpt.ThisTimeLineID = 1;
    ckpt.PrevTimeLineID = 1;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(1, 2000);
    ckpt.oldestXid = 1500;
    ckpt.oldestXidDB = 5;
    ckpt.nextMulti = 1;
    ckpt.nextMultiOffset = 1;
    ckpt.oldestMulti = 1;
    ckpt.oldestMultiDB = 1;
    assert!(types_core::TransactionIdPrecedes(pre_wrap_oldest, ckpt.oldestXid));

    let mut buf = Vec::new();
    let mut state = online_checkpoint_record(&ckpt, &mut buf);
    crate::redo::xlog_redo(&mut state).unwrap();

    assert_eq!(
        tv.oldestXid.load(Relaxed),
        1500,
        "online checkpoint redo must adopt a modularly-later oldestXid across wraparound"
    );
    assert_eq!(tv.oldestXidDB.load(Relaxed), 5, "SetTransactionIdLimit carries oldestXidDB");
    // SetTransactionIdLimit (varsup.c:424-444) recomputes the wrap limit
    // from the adopted horizon.
    assert_eq!(
        tv.xidWrapLimit.load(Relaxed),
        1500u32.wrapping_add(types_core::MaxTransactionId >> 1)
    );

    // And a checkpoint whose oldestXid is modularly OLDER (a stale primary
    // checkpoint) must not move the horizon backwards.
    let mut stale = ckpt;
    stale.oldestXid = 4_294_950_000;
    let mut buf2 = Vec::new();
    let mut state2 = online_checkpoint_record(&stale, &mut buf2);
    crate::redo::xlog_redo(&mut state2).unwrap();
    assert_eq!(tv.oldestXid.load(Relaxed), 1500);
}
