use super::*;
use mcx::MemoryContext;
use std::cell::Cell;
use std::sync::{Mutex, Once};
use transam_xlog::{SizeOfXLogLongPHD, SizeOfXLogRecord, MAXALIGN, XLP_LONG_HEADER};
use types_core::{TimeLineID, XLogSegNo};
use xlogreader::XLogSegmentRoutine;
use xlogreader_seams::XLogReaderState as ReaderView;

const SEGSZ: i32 = 1024 * 1024;
const SYSID: u64 = 0x0102_0304_0506_0708;
const XLOG_PAGE_MAGIC: u16 = 0xD118;
const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
const BKPBLOCK_HAS_DATA: u8 = 0x20;
const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_APPLY: u8 = 0x02;
const RM_HEAP_ID: u8 = 10;

static STATS_LOCK: Mutex<()> = Mutex::new(());
static RIG: Once = Once::new();

thread_local! {
    static FD_INIT: Cell<bool> = const { Cell::new(false) };
}

fn rig() {
    RIG.call_once(|| {
        XLogPrefetchShmemInit();
        guc_tables::vars::maintenance_io_concurrency
            .install_if_absent(guc_tables::GucVarAccessors { get: || 10, set: |_| {} });
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        relpath::init_seams();
        fd::init_seams();
    });
    FD_INIT.with(|f| {
        if !f.get() {
            fd::InitFileAccess();
            f.set(true);
        }
    });
}

struct WalSim {
    buf: Vec<u8>,
    base: u64,
    insert: u64,
    prev: u64,
}

impl WalSim {
    fn new() -> WalSim {
        let base = SEGSZ as u64;
        let mut w = WalSim { buf: vec![0u8; SEGSZ as usize], base, insert: 0, prev: 0 };
        w.buf[0..2].copy_from_slice(&XLOG_PAGE_MAGIC.to_ne_bytes());
        w.buf[2..4].copy_from_slice(&XLP_LONG_HEADER.to_ne_bytes());
        w.buf[4..8].copy_from_slice(&1u32.to_ne_bytes());
        w.buf[8..16].copy_from_slice(&base.to_ne_bytes());
        w.buf[24..32].copy_from_slice(&SYSID.to_ne_bytes());
        w.buf[32..36].copy_from_slice(&(SEGSZ as u32).to_ne_bytes());
        w.buf[36..40].copy_from_slice(&8192u32.to_ne_bytes());
        w.insert = base + SizeOfXLogLongPHD as u64;
        w
    }

    fn append(&mut self, rmid: u8, info: u8, body: &[u8]) -> u64 {
        let start = self.insert;
        let tot_len = (SizeOfXLogRecord + body.len()) as u32;
        let mut rec = Vec::with_capacity(tot_len as usize);
        rec.extend_from_slice(&tot_len.to_ne_bytes());
        rec.extend_from_slice(&7u32.to_ne_bytes());
        rec.extend_from_slice(&self.prev.to_ne_bytes());
        rec.push(info);
        rec.push(rmid);
        rec.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
        rec.extend_from_slice(body);
        let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
            crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
            &rec[..20],
        ));
        rec[20..24].copy_from_slice(&crc.to_ne_bytes());
        let off = (start - self.base) as usize;
        assert!(off + rec.len() < 8192, "test WAL must stay on the first page");
        self.buf[off..off + rec.len()].copy_from_slice(&rec);
        self.prev = start;
        self.insert = start + MAXALIGN(rec.len()) as u64;
        start
    }
}

struct BlockSpec {
    rlocator: (u32, u32, u32),
    blkno: u32,
    will_init: bool,
    fpw: bool,
}

fn block_body(blocks: &[BlockSpec], main: &[u8]) -> Vec<u8> {
    let mut hdr = Vec::new();
    let mut payload = Vec::new();
    for (id, b) in blocks.iter().enumerate() {
        let mut fork_flags: u8 = 0; // MAIN_FORKNUM
        let data = [0xAAu8; 2];
        fork_flags |= BKPBLOCK_HAS_DATA;
        if b.will_init {
            fork_flags |= BKPBLOCK_WILL_INIT;
        }
        if b.fpw {
            fork_flags |= BKPBLOCK_HAS_IMAGE;
        }
        hdr.push(id as u8);
        hdr.push(fork_flags);
        hdr.extend_from_slice(&(data.len() as u16).to_ne_bytes());
        if b.fpw {
            // 12 live bytes + hole covering the rest of the 8192-byte page.
            let img = [0xBBu8; 12];
            hdr.extend_from_slice(&(img.len() as u16).to_ne_bytes());
            hdr.extend_from_slice(&8u16.to_ne_bytes());
            hdr.push(BKPIMAGE_HAS_HOLE | BKPIMAGE_APPLY);
            payload.extend_from_slice(&img);
        }
        hdr.extend_from_slice(&b.rlocator.0.to_ne_bytes());
        hdr.extend_from_slice(&b.rlocator.1.to_ne_bytes());
        hdr.extend_from_slice(&b.rlocator.2.to_ne_bytes());
        hdr.extend_from_slice(&b.blkno.to_ne_bytes());
        payload.extend_from_slice(&data);
    }
    let mut body = hdr;
    if !main.is_empty() {
        body.push(XLR_BLOCK_ID_DATA_SHORT);
        body.push(main.len() as u8);
    }
    body.extend_from_slice(&payload);
    body.extend_from_slice(main);
    body
}

struct SimRead<'w> {
    wal: &'w WalSim,
    end: u64,
}

impl XLogSegmentRoutine for SimRead<'_> {
    fn segment_open(&mut self, _: &mut ReaderView, _: XLogSegNo, _: &mut TimeLineID) -> PgResult<()> {
        unreachable!("in-memory reader")
    }
    fn segment_close(&mut self, _: &mut ReaderView) {}
}

impl XLogReaderRoutine for SimRead<'_> {
    fn page_read(
        &mut self,
        _v: &mut ReaderView,
        target_page_ptr: XLogRecPtr,
        req_len: i32,
        _target_rec_ptr: XLogRecPtr,
        cur_page: &mut [u8],
    ) -> PgResult<i32> {
        if target_page_ptr < self.wal.base || target_page_ptr + req_len as u64 > self.end {
            return Ok(-1);
        }
        let count = 8192u64.min(self.end - target_page_ptr) as usize;
        let o = (target_page_ptr - self.wal.base) as usize;
        cur_page[..count].copy_from_slice(&self.wal.buf[o..o + count]);
        Ok(count as i32)
    }
}

fn reader(cx: &MemoryContext) -> XLogReaderState<'_> {
    let mut r = XLogReaderState::allocate(cx.mcx(), SEGSZ).unwrap();
    r.system_identifier = SYSID;
    r
}

fn snapshot() -> [u64; 6] {
    let s = shared_stats();
    [
        s.prefetch.load(Relaxed),
        s.hit.load(Relaxed),
        s.skip_init.load(Relaxed),
        s.skip_new.load(Relaxed),
        s.skip_fpw.load(Relaxed),
        s.skip_rep.load(Relaxed),
    ]
}

#[test]
fn rmgr_ids_pin_c_rows() {
    assert_eq!(rmgr::RmgrTable[RM_SMGR_ID as usize].rm_name, "Storage");
    assert_eq!(rmgr::RmgrTable[RM_DBASE_ID as usize].rm_name, "Database");
    assert_eq!(transam_xlog::RM_XLOG_ID, 0);
}

#[test]
fn lrq_admission_and_completion_match_c() {
    let cx = MemoryContext::new("t");
    let mut lrq = lrq_alloc(cx.mcx(), 8, 2).unwrap();
    assert_eq!(lrq.size, 9);

    // Script: NO_IO@10, IO@20, IO@30 (hits max_inflight), rest unreached.
    let script = [
        (LsnReadQueueNextStatus::NoIo, 10u64),
        (LsnReadQueueNextStatus::Io, 20),
        (LsnReadQueueNextStatus::Io, 30),
        (LsnReadQueueNextStatus::Io, 40),
    ];
    let mut i = 0;
    lrq_prefetch(&mut lrq, |lsn| {
        let (st, l) = script[i];
        i += 1;
        *lsn = l;
        Ok(st)
    })
    .unwrap();
    assert_eq!((lrq.inflight, lrq.completed, lrq.head, lrq.tail), (2, 1, 3, 0));
    assert_eq!(i, 3, "stops at max_inflight");

    // Replaying past 25 retires 10 (no-io) and 20 (io); disabled: no refill.
    lrq_complete_lsn(&mut lrq, 25, false, |_| unreachable!("disabled")).unwrap();
    assert_eq!((lrq.inflight, lrq.completed, lrq.head, lrq.tail), (1, 0, 3, 2));

    // Enabled refill resumes admission until Again.
    let mut calls = 0;
    lrq_complete_lsn(&mut lrq, 35, true, |lsn| {
        calls += 1;
        *lsn = 100;
        Ok(if calls == 1 { LsnReadQueueNextStatus::NoIo } else { LsnReadQueueNextStatus::Again })
    })
    .unwrap();
    assert_eq!((lrq.inflight, lrq.completed, lrq.tail), (0, 1, 3));
    assert_eq!(calls, 2);
}

#[test]
fn filter_lsn_horizon_semantics() {
    rig();
    let cx = MemoryContext::new("t");
    let mut pf = XLogPrefetcher::XLogPrefetcherAllocate(cx.mcx());

    // `a` lives in db 6 so the db-5 wide filter below never shadows it.
    let a = RelFileLocator::new(1663, 6, 1001);
    let db_wide = RelFileLocator::new(InvalidOid, 5, InvalidOid);

    pf.XLogPrefetcherAddFilter(a, 7, 100).unwrap();
    assert!(pf.XLogPrefetcherIsFiltered(a, 7));
    assert!(pf.XLogPrefetcherIsFiltered(a, 8));
    assert!(!pf.XLogPrefetcherIsFiltered(a, 6));

    // Update: lifetime extends, block bound keeps the minimum.
    pf.XLogPrefetcherAddFilter(a, 3, 200).unwrap();
    assert!(pf.XLogPrefetcherIsFiltered(a, 3));
    assert!(!pf.XLogPrefetcherIsFiltered(a, 2));

    // Whole-database filter catches every relation in db 5 (lsns only grow
    // in WAL order — completion drains the queue tail on that invariant).
    pf.XLogPrefetcherAddFilter(db_wide, 0, 250).unwrap();
    let other = RelFileLocator::new(1663, 5, 9999);
    assert!(pf.XLogPrefetcherIsFiltered(other, 0));
    let other_db = RelFileLocator::new(1663, 6, 9999);
    assert!(!pf.XLogPrefetcherIsFiltered(other_db, 0));

    // C: filters drop only when filter_until_replayed < replaying_lsn.
    pf.XLogPrefetcherCompleteFilters(200);
    assert!(pf.XLogPrefetcherIsFiltered(a, 3), "until==replaying keeps the filter");
    pf.XLogPrefetcherCompleteFilters(201);
    assert!(!pf.XLogPrefetcherIsFiltered(a, 3));
    assert!(pf.XLogPrefetcherIsFiltered(other, 0));
    pf.XLogPrefetcherCompleteFilters(251);
    assert!(!pf.XLogPrefetcherIsFiltered(other, 0));
    assert!(pf.filter_queue.is_empty() && pf.filter_table.is_empty());
}

#[test]
fn guc_check_and_assign_arms() {
    assert!(check_recovery_prefetch(RECOVERY_PREFETCH_OFF).is_ok());
    assert!(check_recovery_prefetch(RECOVERY_PREFETCH_TRY).is_ok());
    assert_eq!(check_recovery_prefetch(RECOVERY_PREFETCH_ON).is_ok(), USE_PREFETCH);

    assign_recovery_prefetch(RECOVERY_PREFETCH_OFF);
    assert_eq!(recovery_prefetch(), RECOVERY_PREFETCH_OFF);
    assign_recovery_prefetch(RECOVERY_PREFETCH_TRY);
    assert_eq!(recovery_prefetch(), RECOVERY_PREFETCH_TRY);
}

#[test]
fn recovery_prefetch_off_bypasses_analysis() {
    rig();
    let _g = STATS_LOCK.lock().unwrap();
    let mut w = WalSim::new();
    let l1 = w.append(RM_HEAP_ID, 0x30, &block_body(&[], b"one"));
    let l2 = w.append(
        RM_HEAP_ID,
        0x30,
        &block_body(&[BlockSpec { rlocator: (1663, 5, 42), blkno: 0, will_init: false, fpw: true }], b""),
    );
    let l3 = w.append(RM_HEAP_ID, 0x30, &block_body(&[], b"three"));
    let mut src = SimRead { wal: &w, end: w.insert };

    assign_recovery_prefetch(RECOVERY_PREFETCH_OFF);
    let before = snapshot();
    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    let mut pf = XLogPrefetcher::XLogPrefetcherAllocate(cx.mcx());
    pf.XLogPrefetcherBeginRead(&mut r, l1);
    assert_eq!(pf.XLogPrefetcherReadRecord(&mut r, &mut src).unwrap(), Some(l1));
    assert_eq!(pf.XLogPrefetcherReadRecord(&mut r, &mut src).unwrap(), Some(l2));
    assert_eq!(pf.XLogPrefetcherReadRecord(&mut r, &mut src).unwrap(), Some(l3));
    assert_eq!(pf.XLogPrefetcherReadRecord(&mut r, &mut src).unwrap(), None);

    // Off: records decode one at a time, nothing is analyzed or counted.
    let lrq = pf.streaming_read.as_ref().unwrap();
    assert_eq!((lrq.max_inflight, lrq.size), (1, 2));
    assert_eq!(snapshot(), before);
    assign_recovery_prefetch(RECOVERY_PREFETCH_TRY);
}

#[test]
fn stats_classify_skips_like_c() {
    rig();
    let _g = STATS_LOCK.lock().unwrap();
    assign_recovery_prefetch(RECOVERY_PREFETCH_TRY);
    if !USE_PREFETCH {
        return;
    }

    let x = (1663u32, 5u32, 77001u32);
    let y = (1663u32, 5u32, 77002u32);
    let z = (1663u32, 5u32, 77003u32);

    let mut w = WalSim::new();
    let l1 = w.append(RM_HEAP_ID, 0x30, &block_body(&[], b"start"));
    let _fpw = w.append(
        RM_HEAP_ID,
        0x30,
        &block_body(&[BlockSpec { rlocator: y, blkno: 1, will_init: false, fpw: true }], b""),
    );
    let _init = w.append(
        RM_HEAP_ID,
        0x30,
        &block_body(&[BlockSpec { rlocator: y, blkno: 2, will_init: true, fpw: false }], b""),
    );
    // SMGR_CREATE(x, main fork) filters x before its block is scanned.
    let mut create = Vec::new();
    create.extend_from_slice(&x.0.to_ne_bytes());
    create.extend_from_slice(&x.1.to_ne_bytes());
    create.extend_from_slice(&x.2.to_ne_bytes());
    create.extend_from_slice(&0i32.to_ne_bytes());
    let _smgr = w.append(RM_SMGR_ID, storage_xlog::XLOG_SMGR_CREATE, &block_body(&[], &create));
    let _filtered = w.append(
        RM_HEAP_ID,
        0x30,
        &block_body(&[BlockSpec { rlocator: x, blkno: 9, will_init: false, fpw: false }], b""),
    );
    // y is absent on disk: smgrexists=false => skip_new + filter.
    let _missing = w.append(
        RM_HEAP_ID,
        0x30,
        &block_body(&[BlockSpec { rlocator: y, blkno: 3, will_init: false, fpw: false }], b""),
    );
    // z is pre-seeded in the recent window => skip_rep before smgr probes.
    let _rep = w.append(
        RM_HEAP_ID,
        0x30,
        &block_body(&[BlockSpec { rlocator: z, blkno: 4, will_init: false, fpw: false }], b""),
    );
    let end = w.append(RM_HEAP_ID, 0x30, &block_body(&[], b"end"));
    let mut src = SimRead { wal: &w, end: w.insert };

    let before = snapshot();
    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    let mut pf = XLogPrefetcher::XLogPrefetcherAllocate(cx.mcx());
    // Slot 3: the missing-y probe consumes window slot 0 before z is scanned.
    pf.recent_rlocator[3] = RelFileLocator::new(z.0, z.1, z.2);
    pf.recent_block[3] = 4;

    pf.XLogPrefetcherBeginRead(&mut r, l1);
    let mut got = Vec::new();
    while let Some(lsn) = pf.XLogPrefetcherReadRecord(&mut r, &mut src).unwrap() {
        got.push(lsn);
        if lsn == end {
            break;
        }
    }
    assert_eq!(got.first(), Some(&l1));
    assert_eq!(got.last(), Some(&end));
    assert_eq!(got.len(), 8);

    let after = snapshot();
    let delta: Vec<u64> = after.iter().zip(before.iter()).map(|(a, b)| a - b).collect();
    // [prefetch, hit, skip_init, skip_new, skip_fpw, skip_rep]
    assert_eq!(delta, vec![0, 0, 1, 2, 1, 1]);

    // Replay passed every filter's horizon; ReadRecord completed them all.
    assert!(pf.filter_table.is_empty() && pf.filter_queue.is_empty());
}

#[test]
fn shmem_reset_zeroes_counters_and_gauges() {
    rig();
    let _g = STATS_LOCK.lock().unwrap();
    let s = shared_stats();
    s.prefetch.store(5, Relaxed);
    s.io_depth.store(3, Relaxed);
    s.wal_distance.store(9, Relaxed);
    XLogPrefetchShmemResetAfterCrash();
    assert_eq!(s.prefetch.load(Relaxed), 0);
    assert_eq!(s.io_depth.load(Relaxed), 0);
    assert_eq!(s.wal_distance.load(Relaxed), 0);
    assert!(XLogPrefetchShmemSize() >= 7 * 8 + 3 * 4);
}
