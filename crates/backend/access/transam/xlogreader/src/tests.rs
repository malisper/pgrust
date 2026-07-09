use super::*;
use mcx::MemoryContext;

const SEGSZ: i32 = 1024 * 1024;
const PG: u64 = XLOG_BLCKSZ as u64;
const SYSID: u64 = 0x1122_3344_5566_7788;

// In-memory WAL image builder mirroring xloginsert's page layout rules.
struct WalSim {
    buf: Vec<u8>,
    base: u64,
    tli: u32,
    insert: u64,
    prev: u64,
}

impl WalSim {
    fn new() -> WalSim {
        let base = SEGSZ as u64;
        let mut w = WalSim {
            buf: vec![0u8; 2 * SEGSZ as usize],
            base,
            tli: 1,
            insert: base,
            prev: 0,
        };
        w.page_init(base, 0);
        w.insert = base + SIZE_OF_XLOG_LONG_PHD as u64;
        w
    }

    fn off(&self, lsn: u64) -> usize {
        (lsn - self.base) as usize
    }

    fn page_init(&mut self, pageaddr: u64, rem_len: u32) {
        let long = pageaddr % SEGSZ as u64 == 0;
        let mut info: u16 = 0;
        if long {
            info |= XLP_LONG_HEADER;
        }
        if rem_len > 0 {
            info |= XLP_FIRST_IS_CONTRECORD;
        }
        let o = self.off(pageaddr);
        self.buf[o..o + 2].copy_from_slice(&XLOG_PAGE_MAGIC.to_ne_bytes());
        self.buf[o + 2..o + 4].copy_from_slice(&info.to_ne_bytes());
        self.buf[o + 4..o + 8].copy_from_slice(&self.tli.to_ne_bytes());
        self.buf[o + 8..o + 16].copy_from_slice(&pageaddr.to_ne_bytes());
        self.buf[o + 16..o + 20].copy_from_slice(&rem_len.to_ne_bytes());
        if long {
            self.buf[o + 24..o + 32].copy_from_slice(&SYSID.to_ne_bytes());
            self.buf[o + 32..o + 36].copy_from_slice(&(SEGSZ as u32).to_ne_bytes());
            self.buf[o + 36..o + 40].copy_from_slice(&(XLOG_BLCKSZ as u32).to_ne_bytes());
        }
    }

    fn append(&mut self, rmid: u8, info: u8, xid: u32, body: &[u8]) -> u64 {
        if self.insert % PG == 0 {
            self.page_init(self.insert, 0);
            self.insert += XLogPageHeaderSize(if self.insert % SEGSZ as u64 == 0 {
                XLP_LONG_HEADER
            } else {
                0
            }) as u64;
        }
        let start = self.insert;
        let tot_len = (SIZE_OF_XLOG_RECORD + body.len()) as u32;

        let mut rec = Vec::with_capacity(tot_len as usize);
        rec.extend_from_slice(&tot_len.to_ne_bytes());
        rec.extend_from_slice(&xid.to_ne_bytes());
        rec.extend_from_slice(&self.prev.to_ne_bytes());
        rec.push(info);
        rec.push(rmid);
        rec.extend_from_slice(&[0, 0]);
        rec.extend_from_slice(&[0, 0, 0, 0]);
        rec.extend_from_slice(body);
        let crc = record_crc(&parse_xlog_record(&rec), &rec);
        rec[20..24].copy_from_slice(&crc.to_ne_bytes());

        let mut pos = start;
        let mut written = 0usize;
        while written < rec.len() {
            if pos % PG == 0 {
                self.page_init(pos, (rec.len() - written) as u32);
                pos += XLogPageHeaderSize(if pos % SEGSZ as u64 == 0 {
                    XLP_LONG_HEADER
                } else {
                    0
                }) as u64;
            }
            let pgfree = (PG - pos % PG) as usize;
            let take = pgfree.min(rec.len() - written);
            let o = self.off(pos);
            self.buf[o..o + take].copy_from_slice(&rec[written..written + take]);
            pos += take as u64;
            written += take;
        }

        self.prev = start;
        self.insert = pos + (MAXALIGN(pos as usize) - pos as usize) as u64;
        start
    }

    fn recompute_crc(&mut self, lsn: u64) {
        let o = self.off(lsn);
        let tot = read_u32(&self.buf, o) as usize;
        let rec = self.buf[o..o + tot].to_vec();
        let crc = record_crc(&parse_xlog_record(&rec), &rec);
        self.buf[o + 20..o + 24].copy_from_slice(&crc.to_ne_bytes());
    }
}

fn main_data_body(data: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    if data.len() <= 255 {
        b.push(XLR_BLOCK_ID_DATA_SHORT);
        b.push(data.len() as u8);
    } else {
        b.push(XLR_BLOCK_ID_DATA_LONG);
        b.extend_from_slice(&(data.len() as u32).to_ne_bytes());
    }
    b.extend_from_slice(data);
    b
}

struct BlockSpec<'a> {
    block_id: u8,
    rlocator: (u32, u32, u32),
    blkno: u32,
    data: &'a [u8],
    image: Option<(&'a [u8], u16, u16, u8)>, // (bytes, hole_offset, hole_length, extra bimg_info)
}

fn block_body(blocks: &[BlockSpec<'_>], main: &[u8]) -> Vec<u8> {
    let mut hdr = Vec::new();
    let mut payload = Vec::new();
    for b in blocks {
        let mut fork_flags: u8 = 0; // MAIN_FORKNUM
        if !b.data.is_empty() {
            fork_flags |= BKPBLOCK_HAS_DATA;
        }
        if b.image.is_some() {
            fork_flags |= BKPBLOCK_HAS_IMAGE;
        }
        hdr.push(b.block_id);
        hdr.push(fork_flags);
        hdr.extend_from_slice(&(b.data.len() as u16).to_ne_bytes());
        if let Some((img, hole_off, hole_len, extra)) = b.image {
            assert_eq!(img.len(), BLCKSZ - hole_len as usize);
            hdr.extend_from_slice(&(img.len() as u16).to_ne_bytes());
            hdr.extend_from_slice(&hole_off.to_ne_bytes());
            hdr.push(if hole_len > 0 { BKPIMAGE_HAS_HOLE } else { 0 } | BKPIMAGE_APPLY | extra);
            payload.extend_from_slice(img);
        }
        hdr.extend_from_slice(&b.rlocator.0.to_ne_bytes());
        hdr.extend_from_slice(&b.rlocator.1.to_ne_bytes());
        hdr.extend_from_slice(&b.rlocator.2.to_ne_bytes());
        hdr.extend_from_slice(&b.blkno.to_ne_bytes());
        payload.extend_from_slice(b.data);
    }
    let mut body = hdr;
    if !main.is_empty() {
        if main.len() <= 255 {
            body.push(XLR_BLOCK_ID_DATA_SHORT);
            body.push(main.len() as u8);
        } else {
            body.push(XLR_BLOCK_ID_DATA_LONG);
            body.extend_from_slice(&(main.len() as u32).to_ne_bytes());
        }
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
        let count = (XLOG_BLCKSZ as u64).min(self.end - target_page_ptr) as usize;
        let o = self.wal.off(target_page_ptr);
        cur_page[..count].copy_from_slice(&self.wal.buf[o..o + count]);
        Ok(count as i32)
    }
}

fn reader<'mcx>(cx: &'mcx MemoryContext) -> XLogReaderState<'mcx> {
    let mut r = XLogReaderState::allocate(cx.mcx(), SEGSZ).unwrap();
    r.system_identifier = SYSID;
    r
}

#[test]
fn reads_record_sequence() {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0x10, 100, &main_data_body(b"first"));
    let l2 = w.append(0, 0x20, 101, &main_data_body(b"second record"));
    let l3 = w.append(0, 0x30, 102, &main_data_body(b"third"));
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);

    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.XLogRecGetXid(), 100);
    assert_eq!(r.XLogRecGetInfo(), 0x10);
    assert_eq!(r.XLogRecGetRmid(), 0);
    assert_eq!(r.XLogRecGetData(), b"first");
    assert_eq!(r.XLogRecGetDataLen(), 5);
    assert!(!r.XLogRecHasAnyBlockRefs());

    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l2));
    assert_eq!(r.XLogRecGetData(), b"second record");
    assert_eq!(r.XLogRecGetPrev(), l1);

    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l3));
    assert_eq!(r.v.ReadRecPtr, l3);
    assert!(r.v.EndRecPtr > l3);

    // Past end-of-WAL: the callback fails without a decode message.
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), None);
    assert_eq!(r.errormsg(), None);
}

#[test]
fn crc_failure_reports_checksum_error() {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0x10, 1, &main_data_body(b"ok"));
    let l2 = w.append(0, 0x10, 2, &main_data_body(b"corrupt me"));
    let o = w.off(l2) + SIZE_OF_XLOG_RECORD + 2;
    w.buf[o] ^= 0xFF;
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), None);
    let msg = r.errormsg().unwrap();
    assert!(
        msg.contains("incorrect resource manager data checksum in record at"),
        "{msg}"
    );
}

// Decodes a single record whose raw body is `body` (framed + CRC'd by
// WalSim::append) and returns the reader's deferred error message.
fn decode_corrupt_body(body: &[u8]) -> String {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0, 0, body);
    let mut src = SimRead { wal: &w, end: w.insert };
    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    let res = r.XLogReadRecord(&mut src).unwrap();
    assert_eq!(res, None, "corrupt record must fail to decode");
    r.errormsg().expect("decode failure reports an error").to_owned()
}

// Cargo-fuzz decoder crashes (2026-07-08): wire-controlled u32/u16 arithmetic
// in decode_record must reject the record with C's report_invalid_record
// error, not abort (overflow-checks) or panic (unconditional slice bound).
// Bytes are the minimized libFuzzer repros; message strings are byte-identical
// to xlogreader.c DecodeXLogRecord (PG 18.3). The LSN is 0/0 because C
// prints state->ReadRecPtr, which only advances on record consumption — it
// is still InvalidXLogRecPtr while decoding the first record.
#[test]
fn fuzz_add_overflow_lib1827_reports_invalid_length() {
    // DATA_LONG main_data_len = 0xFFFFFFFF wraps datatotal (l177 27-byte repro).
    let body = &[
        0, 32, 4, 0, 1, 0, 0, 0, 0, 91, 0, 0, 46, 255, 2, 104, 105, 100, 97, 116, 254, 255, 255,
        255, 255, 255, 255, 255, 0, 0, 255, 2, 104, 105, 100, 97, 116, 97,
    ];
    assert_eq!(
        decode_corrupt_body(body),
        "record with invalid length at 0/0"
    );
}

#[test]
fn fuzz_subtract_underflow_lib1890_reports_invalid_length() {
    // bimg_len 0xc200 > BLCKSZ; BLCKSZ-bimg_len underflow (l177 9-byte repro).
    let body = &[0x06, 0xfb, 0x01, 0x00, 0x00, 0xc2, 0x1e, 0x00, 0x00];
    assert_eq!(
        decode_corrupt_body(body),
        "BKPIMAGE_HAS_HOLE not set, but hole offset 30 length 24064 at 0/0"
    );
}

#[test]
fn fuzz_bimg_slice_oob_wave2_reports_invalid_length() {
    // HAS_IMAGE bimg_len=8 then DATA_LONG main_data_len=0xFFFFFFFF wraps
    // datatotal past the aggregate gate; the payload copy must re-check the
    // per-fragment length (wave2 37-byte repro). C silently heap-overreads on
    // this CRC-valid record (UB); we deliberately reject it — the ruled
    // divergence, so this message is NOT byte-identical to C by design.
    let body = &[
        0, 29, 0, 0, 8, 0, 0, 0, 4, 255, 0, 1, 8, 39, 4, 9, 170, 170, 170, 170, 170, 170, 170, 0,
        1, 254, 255, 255, 255, 255, 2, 0, 8, 0, 255, 0, 0,
    ];
    assert_eq!(
        decode_corrupt_body(body),
        "record with invalid length at 0/0"
    );
}

#[test]
fn bad_prev_link_detected() {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0x10, 1, &main_data_body(b"aaaa"));
    let l2 = w.append(0, 0x10, 2, &main_data_body(b"bbbb"));
    let o = w.off(l2) + 8;
    w.buf[o..o + 8].copy_from_slice(&(l1 + 8).to_ne_bytes());
    w.recompute_crc(l2);
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), None);
    assert!(r.errormsg().unwrap().contains("record with incorrect prev-link"));
}

#[test]
fn record_spanning_pages_reassembles() {
    let mut w = WalSim::new();
    let big: Vec<u8> = (0..20_000u32).map(|i| (i * 7) as u8).collect();
    let l1 = w.append(0, 0x10, 7, &main_data_body(&big));
    let l2 = w.append(0, 0x10, 8, &main_data_body(b"after"));
    assert!(l2 - l1 > PG, "record must span pages");
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.XLogRecGetData(), &big[..]);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l2));
    assert_eq!(r.XLogRecGetData(), b"after");
}

#[test]
fn block_references_decode_and_marshal() {
    let mut w = WalSim::new();
    let img: Vec<u8> = (0..BLCKSZ - 200).map(|i| (i % 251) as u8).collect();
    let body = block_body(
        &[
            BlockSpec {
                block_id: 0,
                rlocator: (1663, 5, 16384),
                blkno: 42,
                data: b"blockdata",
                image: None,
            },
            BlockSpec {
                block_id: 2,
                rlocator: (1663, 5, 16385),
                blkno: 7,
                data: &[],
                image: Some((&img, 100, 200, 0)),
            },
        ],
        b"maindata",
    );
    let l1 = w.append(10, 0x00, 9, &body);
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));

    assert_eq!(r.XLogRecMaxBlockId(), 2);
    assert!(r.XLogRecHasBlockRef(0));
    assert!(!r.XLogRecHasBlockRef(1));
    assert!(r.XLogRecHasBlockRef(2));
    let (loc, fork, blkno, _) = r.XLogRecGetBlockTagExtended(0).unwrap();
    assert_eq!((loc.spcOid, loc.dbOid, loc.relNumber), (1663, 5, 16384));
    assert_eq!(fork, ForkNumber::MAIN_FORKNUM);
    assert_eq!(blkno, 42);
    assert_eq!(r.XLogRecGetBlockData(0).unwrap(), b"blockdata");
    assert_eq!(r.XLogRecGetBlockData(1), None);
    assert_eq!(r.XLogRecGetBlockData(2), None);
    assert!(r.XLogRecHasBlockImage(2));
    assert!(r.XLogRecBlockImageApply(2));
    assert!(!r.XLogRecHasBlockImage(0));
    assert_eq!(r.XLogRecGetData(), b"maindata");

    // The marshaled view hands out the same bytes through the pointer form
    // (the unsafe record-buffer kernel; Miri-checked).
    let vr = r.v.record.as_ref().unwrap();
    assert_eq!(vr.max_block_id, 2);
    assert_eq!(vr.xl_xid, 9);
    // SAFETY: the reader's current record is unchanged.
    unsafe {
        assert_eq!(vr.main_data_bytes(), b"maindata");
        assert_eq!(vr.blocks[0].data_bytes(), b"blockdata");
        assert_eq!(vr.blocks[2].bkp_image_bytes(), &img[..]);
    }
    assert_eq!(vr.blocks[2].hole_offset, 100);
    assert_eq!(vr.blocks[2].hole_length, 200);

    // RestoreBlockImage re-inserts the hole as zeroes.
    let mut page = [0xAAu8; BLCKSZ];
    assert!(r.RestoreBlockImage(2, &mut page));
    assert_eq!(&page[..100], &img[..100]);
    assert!(page[100..300].iter().all(|&b| b == 0));
    assert_eq!(&page[300..], &img[100..]);

    assert!(!r.RestoreBlockImage(0, &mut page));
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), None);
    assert!(r.errormsg().unwrap().contains("could not restore image"));
}

#[test]
fn find_next_record_skips_into_valid_boundary() {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0x10, 1, &main_data_body(b"one"));
    let l2 = w.append(0, 0x10, 2, &main_data_body(b"two"));
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    let found = r.XLogFindNextRecord(&mut src, l1 + 4).unwrap();
    assert_eq!(found, l2);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l2));
}

#[test]
fn oversized_and_ring_full_accounting() {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0x10, 1, &main_data_body(b"first"));
    let _l2 = w.append(0, 0x10, 2, &main_data_body(b"second"));
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    // Too small for any record's required space: every decode is oversized
    // (blocking) and read-ahead would block (nonblocking).
    r.XLogReaderSetDecodeBuffer(1024);
    r.XLogBeginRead(l1);

    assert_eq!(r.XLogReadAhead(&mut src, true).unwrap(), None);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.XLogRecGetData(), b"first");
    assert!(r.current.as_ref().unwrap().oversized);
}

#[test]
fn page_header_validation_failures() {
    let mut w = WalSim::new();
    let l1 = w.append(0, 0x10, 1, &main_data_body(b"one"));
    // Force the record to cross into page 2, then corrupt page 2's magic.
    let big: Vec<u8> = vec![3u8; 9000];
    let l2 = w.append(0, 0x10, 2, &main_data_body(&big));
    let page2 = w.base + PG;
    let o = w.off(page2);
    w.buf[o] = 0x77;
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), None);
    let msg = r.errormsg().unwrap();
    assert!(msg.contains("invalid magic number"), "{msg}");
    assert_eq!(r.abortedRecPtr, l2);
    assert_eq!(r.missingContrecPtr, page2);
}

#[test]
fn xlog_switch_skips_to_segment_boundary() {
    let mut w = WalSim::new();
    let l1 = w.append(0, XLOG_SWITCH, 0, &main_data_body(b""));
    // Fill the tail of the segment with zeroes (as XLOG_SWITCH leaves it),
    // then continue at the next segment boundary.
    w.insert = 2 * SEGSZ as u64;
    let l2 = w.append(0, 0x10, 3, &main_data_body(b"next seg"));
    let mut src = SimRead { wal: &w, end: w.insert };

    let cx = MemoryContext::new("t");
    let mut r = reader(&cx);
    r.XLogBeginRead(l1);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l1));
    assert_eq!(r.v.EndRecPtr, 2 * SEGSZ as u64);
    assert_eq!(r.XLogReadRecord(&mut src).unwrap(), Some(l2));
    assert_eq!(r.XLogRecGetData(), b"next seg");
}

#[cfg(not(miri))]
#[test]
fn wal_read_preads_across_segments() {
    use std::io::Write;
    use std::os::unix::io::IntoRawFd;

    let mut w = WalSim::new();
    w.append(0, 0x10, 1, &main_data_body(b"payload"));

    let dir = std::env::temp_dir().join(format!("xlogreader-walread-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let seg_path = dir.join("seg1");
    std::fs::File::create(&seg_path)
        .unwrap()
        .write_all(&w.buf[..SEGSZ as usize])
        .unwrap();

    struct FileSegs {
        path: std::path::PathBuf,
    }
    impl XLogSegmentRoutine for FileSegs {
        fn segment_open(
            &mut self,
            v: &mut ReaderView,
            next_seg_no: XLogSegNo,
            _tli: &mut TimeLineID,
        ) -> PgResult<()> {
            assert_eq!(next_seg_no, 1);
            let f = std::fs::File::open(&self.path).unwrap();
            v.seg.ws_file = f.into_raw_fd();
            Ok(())
        }
        fn segment_close(&mut self, v: &mut ReaderView) {
            // SAFETY: closing the fd segment_open produced.
            unsafe { libc::close(v.seg.ws_file) };
            v.seg.ws_file = -1;
        }
    }

    let mut v = ReaderView {
        segcxt: WALSegmentContext { ws_segsize: SEGSZ },
        ..Default::default()
    };
    let mut out = vec![0u8; 4096];
    let startptr = w.base + 100;
    let res = WALRead(&mut v, &mut FileSegs { path: seg_path }, &mut out, startptr, 4096, 1)
        .unwrap();
    assert!(res.is_ok());
    assert_eq!(&out[..], &w.buf[100..100 + 4096]);
    assert_eq!(v.seg.ws_segno, 1);
    assert!(v.seg.ws_file >= 0);
    unsafe { libc::close(v.seg.ws_file) };
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn restore_image_pglz_roundtrips() {
    let mut page_orig = [0u8; BLCKSZ];
    let phrase = b"wal page payload / wal page payload % ";
    for (i, b) in page_orig.iter_mut().enumerate() {
        *b = phrase[i % phrase.len()];
    }

    // No hole: compress the whole page.
    let mut comp = [core::mem::MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(BLCKSZ)];
    let n =
        pglz::pglz_compress_into(&page_orig, &mut comp, &pglz::PGLZ_STRATEGY_DEFAULT).unwrap();
    // SAFETY: first n bytes written by the compressor.
    let image =
        unsafe { core::slice::from_raw_parts(comp.as_ptr().cast::<u8>(), n) };
    let mut page = [0xAAu8; BLCKSZ];
    restore_image_core(image, BKPIMAGE_COMPRESS_PGLZ, 0, 0, &mut page).unwrap();
    assert_eq!(page, page_orig);

    // With a hole: source is the page minus the hole; hole comes back zeroed.
    let (hole_offset, hole_length) = (100usize, 800usize);
    let mut holed: Vec<u8> = Vec::new();
    holed.extend_from_slice(&page_orig[..hole_offset]);
    holed.extend_from_slice(&page_orig[hole_offset + hole_length..]);
    let n = pglz::pglz_compress_into(&holed, &mut comp, &pglz::PGLZ_STRATEGY_DEFAULT).unwrap();
    // SAFETY: first n bytes written by the compressor.
    let image =
        unsafe { core::slice::from_raw_parts(comp.as_ptr().cast::<u8>(), n) };
    let mut page = [0xAAu8; BLCKSZ];
    restore_image_core(
        image,
        BKPIMAGE_COMPRESS_PGLZ | BKPIMAGE_HAS_HOLE,
        hole_offset,
        hole_length,
        &mut page,
    )
    .unwrap();
    assert_eq!(&page[..hole_offset], &page_orig[..hole_offset]);
    assert!(page[hole_offset..hole_offset + hole_length].iter().all(|&b| b == 0));
    assert_eq!(&page[hole_offset + hole_length..], &page_orig[hole_offset + hole_length..]);

    // Corrupt stream fails with the C decompress message, not a panic.
    let mut page = [0u8; BLCKSZ];
    let err = restore_image_core(&image[..n / 2], BKPIMAGE_COMPRESS_PGLZ, 0, 0, &mut page)
        .unwrap_err();
    assert!(matches!(err, RestoreErr::DecompressFailure));
    assert!(restore_err_msg(err, 0x1_0000_0000, 3).contains("could not decompress image"));
}
