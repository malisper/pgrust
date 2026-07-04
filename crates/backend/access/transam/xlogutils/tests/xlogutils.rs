use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Mutex, Once};

// NBLOCKS / PAGE_LSN are process-global fake knobs; serialize their users.
static FAKE_LOCK: Mutex<()> = Mutex::new(());

use types_core::{ForkNumber, InvalidBuffer};
use types_storage::{ReadBufferMode, RelFileLocator};
use xlogreader_seams::{
    DecodedBkpBlock, DecodedXLogRecord, WALOpenSegment, WALReadError, XLogReaderState,
    BKPBLOCK_WILL_INIT,
};
use xlogutils::*;

static NBLOCKS: AtomicU32 = AtomicU32::new(0);
static PAGE_LSN: AtomicI64 = AtomicI64::new(0);

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        xlogutils::init_seams();

        relpath_seams::relpathperm::set(|rlocator, forknum| {
            format!(
                "base/{}/{}#{:?}",
                rlocator.dbOid, rlocator.relNumber, forknum
            )
        });
        xlogrecovery_seams::reached_consistency::set(|| false);
        xlogrecovery_seams::get_xlog_replay_rec_ptr::set(|| (0x4000, 1));

        transam_xlog_seams::recovery_in_progress::set(|| true);
        transam_xlog_seams::wal_segment_size::set(|| 16 * 1024 * 1024);

        smgr_seams::smgr_create::set(|_, _, _| Ok(()));
        smgr_seams::smgr_nblocks::set(|_, _| Ok(NBLOCKS.load(Ordering::Relaxed)));
        smgr_seams::smgr_destroy_all::set(|| Ok(()));

        bufmgr_seams::read_recent_buffer::set(|_, _, _, _| Ok(false));
        bufmgr_seams::read_buffer_without_relcache::set(|_, _, blkno, _, _, _| {
            Ok(blkno as i32 + 1)
        });
        bufmgr_seams::extend_buffered_rel_to::set(|_, _, _, _, extend_to, _| Ok(extend_to as i32));
        bufmgr_seams::release_buffer::set(|_| Ok(()));
        bufmgr_seams::mark_buffer_dirty::set(|_| Ok(()));
        bufmgr_seams::flush_one_buffer::set(|_| Ok(()));
        bufmgr_seams::lock_buffer::set(|_, _| Ok(()));
        bufmgr_seams::lock_buffer_for_cleanup::set(|_| Ok(()));
        bufmgr_seams::buffer_page_is_new::set(|_| false);
        bufmgr_seams::buffer_page_get_lsn::set(|_| PAGE_LSN.load(Ordering::Relaxed) as u64);
        bufmgr_seams::buffer_page_set_lsn::set(|_, _| ());

        xlogreader_seams::restore_block_image::set(|_, _, _| Ok(Ok(())));
        xlogreader_seams::wal_read::set(|_, buf, _, count, _| {
            buf[..count].fill(0xAB);
            Ok(Ok(()))
        });

        timeline_seams::read_timeline_history::set(|mcx, tli| {
            let mut v = mcx::PgVec::new_in(mcx);
            v.push(timeline_seams::TimeLineHistoryEntry {
                tli,
                begin: 0,
                end: 0,
            });
            Ok(v)
        });
        timeline_seams::tli_of_point_in_history::set(|_, history| Ok(history[0].tli));
        timeline_seams::tli_switch_point::set(|_, _| Ok((0, 0)));
    });
}

fn locator(rel: u32) -> RelFileLocator {
    RelFileLocator::new(1663, 5, rel)
}

fn record_with_block(end_rec_ptr: u64, flags: u8, apply_image: bool) -> XLogReaderState {
    let mut rec = DecodedXLogRecord::default();
    rec.max_block_id = 0;
    rec.blocks[0] = DecodedBkpBlock {
        in_use: true,
        rlocator: locator(42),
        forknum: ForkNumber::MAIN_FORKNUM,
        blkno: 3,
        prefetch_buffer: 0,
        flags,
        has_image: apply_image,
        apply_image,
        ..DecodedBkpBlock::EMPTY
    };
    XLogReaderState {
        EndRecPtr: end_rec_ptr,
        record: Some(rec),
        ..Default::default()
    }
}

#[test]
fn recovery_and_standby_state_accessors() {
    install_seams();
    assert!(!in_recovery());
    set_in_recovery(true);
    assert!(xlogutils_seams::in_recovery::call());
    set_in_recovery(false);

    assert_eq!(standby_state(), STANDBY_DISABLED);
    assert!(!InHotStandby());
    set_standby_state(STANDBY_SNAPSHOT_PENDING);
    assert!(InHotStandby());
    set_standby_state(STANDBY_DISABLED);

    assert!(!guc_tables::vars::ignore_invalid_pages.read());
    guc_tables::vars::ignore_invalid_pages.write(true);
    assert!(ignore_invalid_pages());
    set_ignore_invalid_pages(false);
}

#[test]
fn invalid_page_log_and_forget_flow() {
    let _g = FAKE_LOCK.lock().unwrap();
    install_seams();
    set_in_recovery(true);

    // Missing page in RBM_NORMAL logs an invalid-page entry.
    NBLOCKS.store(0, Ordering::Relaxed);
    let buf = XLogReadBufferExtended(
        locator(100),
        ForkNumber::MAIN_FORKNUM,
        7,
        ReadBufferMode::Normal,
        InvalidBuffer,
    )
    .unwrap();
    assert_eq!(buf, InvalidBuffer);
    assert!(XLogHaveInvalidPages());

    // RBM_NORMAL_NO_LOG does not log.
    let buf = XLogReadBufferExtended(
        locator(101),
        ForkNumber::MAIN_FORKNUM,
        7,
        ReadBufferMode::NormalNoLog,
        InvalidBuffer,
    )
    .unwrap();
    assert_eq!(buf, InvalidBuffer);

    // A truncate below the block keeps the entry; at/above forgets it.
    XLogTruncateRelation(locator(100), ForkNumber::MAIN_FORKNUM, 8).unwrap();
    assert!(XLogHaveInvalidPages());
    XLogDropRelation(locator(100), ForkNumber::MAIN_FORKNUM).unwrap();
    assert!(!XLogHaveInvalidPages());

    // Database-wide forget.
    let _ = XLogReadBufferExtended(
        locator(102),
        ForkNumber::MAIN_FORKNUM,
        9,
        ReadBufferMode::Normal,
        InvalidBuffer,
    )
    .unwrap();
    assert!(XLogHaveInvalidPages());
    XLogDropDatabase(5).unwrap();
    assert!(!XLogHaveInvalidPages());

    XLogCheckInvalidPages().unwrap();
    set_in_recovery(false);
}

#[test]
fn read_buffer_extended_reads_and_extends() {
    let _g = FAKE_LOCK.lock().unwrap();
    install_seams();
    set_in_recovery(true);

    NBLOCKS.store(10, Ordering::Relaxed);
    let buf = XLogReadBufferExtended(
        locator(200),
        ForkNumber::MAIN_FORKNUM,
        3,
        ReadBufferMode::Normal,
        InvalidBuffer,
    )
    .unwrap();
    assert_eq!(buf, 4); // fake ReadBufferWithoutRelcache: blkno + 1

    NBLOCKS.store(2, Ordering::Relaxed);
    let buf = XLogReadBufferExtended(
        locator(200),
        ForkNumber::MAIN_FORKNUM,
        5,
        ReadBufferMode::ZeroAndLock,
        InvalidBuffer,
    )
    .unwrap();
    assert_eq!(buf, 6); // fake ExtendBufferedRelTo: extend_to (blkno + 1)
    set_in_recovery(false);
}

#[test]
fn read_buffer_for_redo_done_needs_redo_and_restored() {
    let _g = FAKE_LOCK.lock().unwrap();
    install_seams();

    NBLOCKS.store(10, Ordering::Relaxed);
    PAGE_LSN.store(0x100, Ordering::Relaxed);

    let record = record_with_block(0x80, 0, false);
    let (action, buf) = XLogReadBufferForRedo(&record, 0).unwrap();
    assert_eq!(action, BLK_DONE);
    assert_eq!(buf, 4);

    let record = record_with_block(0x200, 0, false);
    let (action, _) = XLogReadBufferForRedo(&record, 0).unwrap();
    assert_eq!(action, BLK_NEEDS_REDO);

    let record = record_with_block(0x200, 0, true);
    let (action, _) = XLogReadBufferForRedo(&record, 0).unwrap();
    assert_eq!(action, BLK_RESTORED);

    let record = record_with_block(0x200, BKPBLOCK_WILL_INIT, false);
    set_in_recovery(true);
    let buf = XLogInitBufferForRedo(&record, 0).unwrap();
    assert_eq!(buf, 4);
    set_in_recovery(false);
}

#[test]
fn read_buffer_for_redo_not_found() {
    let _g = FAKE_LOCK.lock().unwrap();
    install_seams();
    NBLOCKS.store(0, Ordering::Relaxed);
    set_in_recovery(true);
    let record = record_with_block(0x200, 0, false);
    let (action, buf) = XLogReadBufferForRedo(&record, 0).unwrap();
    assert_eq!(action, BLK_NOTFOUND);
    assert_eq!(buf, InvalidBuffer);
    XLogDropRelation(locator(42), ForkNumber::MAIN_FORKNUM).unwrap();
    set_in_recovery(false);
}

#[test]
fn determine_timeline_early_returns_and_history_lookup() {
    install_seams();
    let segsize = 16 * 1024 * 1024;

    // Already-read page: untouched.
    let mut state = XLogReaderState {
        seg: WALOpenSegment {
            ws_file: -1,
            ws_segno: 1,
            ws_tli: 1,
        },
        segcxt: xlogreader_seams::WALSegmentContext { ws_segsize: segsize },
        segoff: 8192,
        readLen: 8192,
        currTLI: 1,
        ..Default::default()
    };
    let want_page = segsize as u64 + 8192;
    XLogReadDetermineTimeline(&mut state, want_page, 100, 1).unwrap();
    assert_eq!(state.currTLI, 1);

    // Reading forward on the current timeline: untouched.
    XLogReadDetermineTimeline(&mut state, want_page + 8192, 8192, 1).unwrap();
    assert_eq!(state.currTLIValidUntil, 0);

    // New timeline forces a history lookup (fakes: tli = requested currTLI).
    let mut state = XLogReaderState {
        segcxt: xlogreader_seams::WALSegmentContext { ws_segsize: segsize },
        currTLI: 1,
        ..Default::default()
    };
    XLogReadDetermineTimeline(&mut state, 8192, 8192, 2).unwrap();
    assert_eq!(state.currTLI, 2);
}

#[test]
fn read_local_xlog_page_reads_within_replay_limit() {
    install_seams();
    let mut state = XLogReaderState {
        segcxt: xlogreader_seams::WALSegmentContext {
            ws_segsize: 16 * 1024 * 1024,
        },
        currTLI: 1,
        ..Default::default()
    };
    let mut page = vec![0u8; 8192];
    // Fake replay pointer is 0x4000: a full page is available at 0x2000.
    let n = read_local_xlog_page(&mut state, 0x2000, 100, 0, &mut page).unwrap();
    assert_eq!(n, 0x2000);
    assert!(page.iter().all(|&b| b == 0xAB));

    // Past the replay pointer without waiting: end-of-WAL flag + -1.
    let n = read_local_xlog_page_no_wait(&mut state, 0x4000, 100, 0, &mut page).unwrap();
    assert_eq!(n, -1);
    assert!(state.private_end_of_wal);
}

#[test]
fn wal_read_raise_error_messages() {
    install_seams();
    let seg = WALOpenSegment {
        ws_file: -1,
        ws_segno: 5,
        ws_tli: 1,
    };
    let err = WALReadRaiseError(&WALReadError {
        wre_errno: 0,
        wre_off: 32768,
        wre_req: 8192,
        wre_read: 0,
        wre_seg: seg,
    })
    .unwrap_err();
    assert_eq!(
        err.message,
        "could not read from WAL segment 000000010000000000000005, offset 32768: read 0 of 8192"
    );

    let ok = WALReadRaiseError(&WALReadError {
        wre_errno: 0,
        wre_off: 0,
        wre_req: 8192,
        wre_read: 8192,
        wre_seg: seg,
    });
    assert!(ok.is_ok());
}
