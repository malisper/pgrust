use super::*;
use std::sync::atomic::Ordering::Relaxed;
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::XLogSegNo;
use xlogreader::{XLogReaderRoutine, XLogSegmentRoutine};
use xlogreader_seams::XLogReaderState as ReaderView;

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AABB;

#[test]
fn header_scratch_size_matches_c() {
    // 24 + 27*33 + 5 + 3 + 5 (xlogrecord.h arithmetic).
    assert_eq!(HEADER_SCRATCH_SIZE, 928);
    assert_eq!(MaxSizeOfXLogRecordBlockHeader, 27);
    assert_eq!(COMPRESS_BUFSIZE, 8196);
    assert_eq!(XLogRecordMaxSize, 1_069_547_520);
}

#[test]
fn page_helpers() {
    let mut page = [0u8; BLCKSZ];
    assert!(page_is_new(&page));
    assert_eq!(page_lsn(&page), 0);
    page_set_lsn(&mut page, 0x0102_0304_0506_0708);
    assert_eq!(page_lsn(&page), 0x0102_0304_0506_0708);
}

// Every unsafe assembly pattern (erased fragments, split_at_mut header
// patch), no ctl/file IO: runs under Miri.
#[test]
fn assemble_unsafe_patterns() {
    init_once();

    let mut scratch = Scratch {
        hdr: Box::new([0u8; HEADER_SCRATCH_SIZE]),
        rdatas: Vec::with_capacity(XLR_NORMAL_RDATAS),
        compressed: Vec::new(),
    };
    let (lower, upper) = (64u16, 8000u16);
    let page = standard_page(0x3F, lower, upper);
    let main = [0x99u8; 40];
    let bufdata = [0x21u8; 6];
    let rloc = RelFileLocator::new(1663, 5, 24576);

    let asm = assemble(
        &mut scratch,
        RM_XLOG_ID,
        XLOG_FPI,
        0,
        true,
        0,
        &[&main],
        &[
            RegBlock {
                block_id: 0,
                rlocator: rloc,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: 3,
                page: &page[..],
                flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                bufdata: &[],
            },
            RegBlock {
                block_id: 1,
                rlocator: rloc,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: 4,
                page: &page[..],
                flags: REGBUF_NO_IMAGE,
                bufdata: &[&bufdata],
            },
        ],
    )
    .unwrap();

    assert_eq!(asm.num_fpi, 1);
    assert_eq!(asm.fpw_lsn, InvalidXLogRecPtr);
    assert!(!asm.topxid_included);

    // The insert_record split + patch, then a CopyXLogRecordToWAL-style walk.
    let (hdr24, rest) = scratch.hdr.split_at_mut(SizeOfXLogRecord);
    scratch.rdatas[0] = erased(&rest[..asm.hdr_len - SizeOfXLogRecord]);
    hdr24[8..16].copy_from_slice(&0x1234u64.to_ne_bytes());

    let mut flat = hdr24.to_vec();
    for rd in &scratch.rdatas {
        flat.extend_from_slice(rd);
    }
    let tot_len = u32::from_ne_bytes(flat[0..4].try_into().unwrap());
    assert_eq!(flat.len(), tot_len as usize);
    let stored = u32::from_ne_bytes(flat[20..24].try_into().unwrap());
    let recomputed = crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &flat[SizeOfXLogRecord..]);
    assert_eq!(stored, recomputed);
    let image_bytes = BLCKSZ - (upper - lower) as usize;
    assert_eq!(
        tot_len as usize,
        asm.hdr_len + image_bytes + bufdata.len() + main.len()
    );
    scratch.rdatas.clear();
}

#[test]
fn misordered_block_ids_error_in_release() {
    init_once();
    let mut scratch = Scratch {
        hdr: Box::new([0u8; HEADER_SCRATCH_SIZE]),
        rdatas: Vec::with_capacity(XLR_NORMAL_RDATAS),
        compressed: Vec::new(),
    };
    let page = standard_page(0x00, 64, 8000);
    let rloc = RelFileLocator::new(1663, 5, 24576);
    let mk = |id: u8| RegBlock {
        block_id: id,
        rlocator: rloc,
        forknum: ForkNumber::MAIN_FORKNUM,
        block: id as BlockNumber,
        page: &page[..],
        flags: REGBUF_NO_IMAGE,
        bufdata: &[],
    };

    for ids in [[1u8, 0u8], [1u8, 1u8]] {
        let err = assemble(
            &mut scratch,
            RM_XLOG_ID,
            0x20,
            0,
            false,
            0,
            &[],
            &[mk(ids[0]), mk(ids[1])],
        );
        let err = match err {
            Err(e) => e,
            Ok(_) => panic!("misordered block IDs must fail"),
        };
        assert!(
            err.message().contains("ascending order"),
            "got: {}",
            err.message()
        );
        scratch.rdatas.clear();
    }
}

#[test]
#[ignore = "child of include_origin_works_without_origin_seam"]
fn include_origin_uninstalled_child() {
    // Runs in a fresh process: origin seam deliberately NOT installed.
    assert!(!origin_seams::replorigin_session_origin::is_installed());
    shmem::init_seams();
    guc_tables::init_seams();
    transam_xlog::init_seams();
    let mut scratch = Scratch {
        hdr: Box::new([0u8; HEADER_SCRATCH_SIZE]),
        rdatas: Vec::with_capacity(XLR_NORMAL_RDATAS),
        compressed: Vec::new(),
    };
    let main = [0x77u8; 10];
    let asm = assemble(
        &mut scratch,
        RM_XLOG_ID,
        0x20,
        0,
        false,
        transam_xlog::XLOG_INCLUDE_ORIGIN,
        &[&main],
        &[],
    )
    .unwrap();
    // InvalidRepOriginId default: record header + short main-data header only.
    assert_eq!(asm.hdr_len, SizeOfXLogRecord + 2);
    scratch.rdatas.clear();
}

#[test]
fn include_origin_works_without_origin_seam() {
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "tests::include_origin_uninstalled_child",
            "--exact",
            "--ignored",
            "--test-threads=1",
        ])
        .output()
        .unwrap();
    assert!(out.status.success(), "child failed: {out:?}");
}

struct SegFileRead {
    wal_dir: std::path::PathBuf,
}

impl XLogSegmentRoutine for SegFileRead {
    fn segment_open(
        &mut self,
        _v: &mut ReaderView,
        _segno: XLogSegNo,
        _tli: &mut types_core::TimeLineID,
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

fn init_once() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        shmem::init_seams();
        guc_tables::init_seams();
        transam_xlog::init_seams();
        init_seams();
        origin_seams::replorigin_session_origin::set(|| 0);
        xact_seams::mark_current_transaction_id_logged_if_any::set(|| {});
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        fd::InitFileAccess();
        lwlock::CreateLWLocks(false).unwrap();
    });
}

fn standard_page(fill: u8, lower: u16, upper: u16) -> Box<[u8; BLCKSZ]> {
    let mut page = Box::new([fill; BLCKSZ]);
    page[0..8].copy_from_slice(&1u64.to_ne_bytes());
    page[8..12].copy_from_slice(&[0; 4]);
    page[12..14].copy_from_slice(&lower.to_ne_bytes());
    page[14..16].copy_from_slice(&upper.to_ne_bytes());
    page
}

fn expected_restored(page: &[u8; BLCKSZ], lower: u16, upper: u16) -> Vec<u8> {
    let mut want = page.to_vec();
    want[lower as usize..upper as usize].fill(0);
    want
}

// One process-global e2e: assemble through the real XLogInsertRecord/XLogFlush
// and decode back off disk with the real xlogreader.
#[test]
fn assemble_insert_decode_roundtrip() {
    let dir = std::env::temp_dir().join(format!("pgrust_xloginsert_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);
    init_once();

    // End of log parked on the segment-2 boundary so the insert path
    // initializes every page it touches itself.
    {
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

    // Record 1: main-data-only through the xlog_insert seam.
    let frag_a = [0xA1u8; 3];
    let frag_b: Vec<u8> = (0..100u8).collect();
    let lsn1 =
        xloginsert_seams::xlog_insert::call(RM_XLOG_ID, 0x20 /* XLOG_NOOP */, &[&frag_a, &frag_b])
            .unwrap();

    // Record 2: forced full-page image of a standard page with a hole.
    let (lower, upper) = (64u16, 8000u16);
    let page2 = standard_page(0x5C, lower, upper);
    let rloc = RelFileLocator::new(1663, 5, 16384);
    let lsn2 = insert_record(
        RM_XLOG_ID,
        XLOG_FPI,
        0,
        &[],
        &[RegBlock {
            block_id: 0,
            rlocator: rloc,
            forknum: ForkNumber::MAIN_FORKNUM,
            block: 7,
            page: &page2[..],
            flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
            bufdata: &[],
        }],
    )
    .unwrap();

    // Record 3: same, with pglz FPW compression armed.
    guc_tables::vars::wal_compression.write(guc_tables::consts::WAL_COMPRESSION_PGLZ);
    let page3 = standard_page(0x00, lower, upper);
    let lsn3 = insert_record(
        RM_XLOG_ID,
        XLOG_FPI,
        0,
        &[],
        &[RegBlock {
            block_id: 0,
            rlocator: rloc,
            forknum: ForkNumber::MAIN_FORKNUM,
            block: 8,
            page: &page3[..],
            flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
            bufdata: &[],
        }],
    )
    .unwrap();
    guc_tables::vars::wal_compression.write(guc_tables::consts::WAL_COMPRESSION_NONE);

    // Record 4: two same-rel NO_IMAGE blocks with data + main data.
    let d0 = [0x11u8; 5];
    let d1a = [0x22u8; 7];
    let d1b = [0x33u8; 9];
    let main4 = [0x44u8; 300];
    let page4 = standard_page(0x77, lower, upper);
    let lsn4 = insert_record(
        RM_XLOG_ID,
        0x20,
        0,
        &[&main4],
        &[
            RegBlock {
                block_id: 0,
                rlocator: rloc,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: 9,
                page: &page4[..],
                flags: REGBUF_NO_IMAGE,
                bufdata: &[&d0],
            },
            RegBlock {
                block_id: 1,
                rlocator: rloc,
                forknum: ForkNumber::MAIN_FORKNUM,
                block: 10,
                page: &page4[..],
                flags: REGBUF_NO_IMAGE,
                bufdata: &[&d1a, &d1b],
            },
        ],
    )
    .unwrap();

    // Record 5: XLOG_INCLUDE_ORIGIN with the session origin at its C default
    // (InvalidRepOriginId) must emit no origin field.
    let main5 = [0x55u8; 8];
    let lsn5 = xloginsert_seams::xlog_insert_with_flags::call(
        RM_XLOG_ID,
        0x20,
        transam_xlog::XLOG_INCLUDE_ORIGIN,
        &[&main5],
    )
    .unwrap();

    transam_xlog::XLogFlush(lsn5).unwrap();

    // Decode everything back off disk.
    let context: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("xloginsert test reader")));
    let mut reader = xlogreader::XLogReaderState::allocate(context.mcx(), SEG).unwrap();
    reader.system_identifier = SYS_ID;
    let mut routine = SegFileRead {
        wal_dir: dir.join("pg_wal"),
    };

    let first_rec = end_of_log + 40; // long page header on the fresh segment
    reader.XLogBeginRead(first_rec);

    let got1 = reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(got1, first_rec);
    assert_eq!(reader.v.EndRecPtr, lsn1);
    assert_eq!(reader.XLogRecGetRmid(), RM_XLOG_ID);
    assert_eq!(reader.XLogRecGetInfo(), 0x20);
    assert_eq!(reader.XLogRecGetXid(), 0);
    let mut want_main = frag_a.to_vec();
    want_main.extend_from_slice(&frag_b);
    assert_eq!(reader.XLogRecGetData(), &want_main[..]);
    assert!(!reader.XLogRecHasAnyBlockRefs());

    let _ = reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.v.EndRecPtr, lsn2);
    assert_eq!(reader.XLogRecGetInfo(), XLOG_FPI);
    assert!(reader.XLogRecHasBlockImage(0));
    assert!(reader.XLogRecBlockImageApply(0));
    let (got_loc, got_fork, got_blk, _) = reader.XLogRecGetBlockTagExtended(0).unwrap();
    assert_eq!((got_loc, got_fork, got_blk), (rloc, ForkNumber::MAIN_FORKNUM, 7));
    let mut restored = vec![0u8; BLCKSZ];
    assert!(reader.RestoreBlockImage(0, &mut restored));
    assert_eq!(restored, expected_restored(&page2, lower, upper));

    let _ = reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.v.EndRecPtr, lsn3);
    // The compressed record must be materially smaller than a raw FPI.
    assert!(reader.XLogRecGetTotalLen() < 1024);
    let mut restored3 = vec![0u8; BLCKSZ];
    assert!(reader.RestoreBlockImage(0, &mut restored3));
    assert_eq!(restored3, expected_restored(&page3, lower, upper));

    let _ = reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.v.EndRecPtr, lsn4);
    assert_eq!(reader.XLogRecGetData(), &main4[..]);
    assert!(!reader.XLogRecHasBlockImage(0));
    assert_eq!(reader.XLogRecGetBlockData(0).unwrap(), &d0[..]);
    let mut want_d1 = d1a.to_vec();
    want_d1.extend_from_slice(&d1b);
    assert_eq!(reader.XLogRecGetBlockData(1).unwrap(), &want_d1[..]);
    let (loc1, _, blk1, _) = reader.XLogRecGetBlockTagExtended(1).unwrap();
    assert_eq!((loc1, blk1), (rloc, 10)); // BKPBLOCK_SAME_REL round-trips

    let _ = reader.XLogReadRecord(&mut routine).unwrap().unwrap();
    assert_eq!(reader.v.EndRecPtr, lsn5);
    assert_eq!(reader.XLogRecGetData(), &main5[..]);
    assert_eq!(reader.XLogRecGetOrigin(), 0); // no origin block emitted

    let _ = std::fs::remove_dir_all(&dir);
}
