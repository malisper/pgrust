//! Byte-level decoder fuzz drivers for pgrust-fast.
//!
//! Each `fn` takes an arbitrary byte slice (from libFuzzer) and pushes it
//! through one of pgrust's byte-level decoders. A Rust panic in any of these
//! is a real P1: in pgrust's thread-per-backend model a decoder panic aborts
//! the backend thread (a WAL-redo panic during recovery, or a wire-parse panic
//! from an untrusted client, is a server abort). PgError values (the ereport
//! Err path) are expected and discarded — only panics/UB are bugs.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use mcx::MemoryContext;

// ---------------------------------------------------------------------------
// Target 1: WAL record decode (xlogreader: header walk + TLV block decode).
// ---------------------------------------------------------------------------
//
// Random bytes never pass the record CRC, so a naive "feed bytes as a WAL
// page" harness would never reach the interesting TLV block-reference decoder
// (decode_record). Instead we treat the fuzz input as the *record body* (the
// post-header TLV payload: block headers, main-data length markers, images),
// frame it into one valid record on one valid long-header WAL page, and
// recompute the CRC so the reader always reaches the block-decode loop. The
// fuzzer then explores block_id sequences, fork flags, data/image lengths,
// hole offsets — exactly the overflow / out-of-order / bad-length surface.

use xlogreader::{
    XLogReaderRoutine, XLogReaderState, XLogSegmentRoutine, SIZE_OF_XLOG_LONG_PHD,
    SIZE_OF_XLOG_RECORD, XLOG_PAGE_MAGIC,
};
use xlogreader_seams::{XLogReaderState as ReaderView, XLR_MAX_BLOCK_ID, XLOG_BLCKSZ};

const FUZZ_SEG_SIZE: i32 = 1024 * 1024;
const FUZZ_SYSID: u64 = 0x1122_3344_5566_7788;
// xlogreader-private page-info bit (XLP_LONG_HEADER); stated here as a constant
// because the record must sit on a valid long-header page for the reader.
const XLP_LONG_HEADER: u16 = 0x0002;

/// A one-page in-memory WAL image; `page_read` copies out of it.
struct PageSrc {
    page: Vec<u8>,
    base: u64,
    end: u64,
}

impl XLogSegmentRoutine for PageSrc {
    fn segment_open(&mut self, _: &mut ReaderView, _: u64, _: &mut u32) -> types_error::PgResult<()> {
        // Never called: the whole record lives on the single loaded page.
        Ok(())
    }
    fn segment_close(&mut self, _: &mut ReaderView) {}
}

impl XLogReaderRoutine for PageSrc {
    fn page_read(
        &mut self,
        _v: &mut ReaderView,
        target_page_ptr: u64,
        req_len: i32,
        _target_rec_ptr: u64,
        cur_page: &mut [u8],
    ) -> types_error::PgResult<i32> {
        if target_page_ptr < self.base || target_page_ptr + req_len as u64 > self.end {
            return Ok(-1);
        }
        let o = (target_page_ptr - self.base) as usize;
        let count = ((self.end - target_page_ptr) as usize).min(XLOG_BLCKSZ as usize);
        cur_page[..count].copy_from_slice(&self.page[o..o + count]);
        Ok(count as i32)
    }
}

fn build_wal_page(base: u64, seg: i32, body: &[u8]) -> Vec<u8> {
    let mut buf = vec![0u8; XLOG_BLCKSZ as usize];

    // Long page header (SIZE_OF_XLOG_LONG_PHD bytes at page start).
    buf[0..2].copy_from_slice(&XLOG_PAGE_MAGIC.to_ne_bytes());
    buf[2..4].copy_from_slice(&XLP_LONG_HEADER.to_ne_bytes());
    buf[4..8].copy_from_slice(&1u32.to_ne_bytes()); // xlp_tli
    buf[8..16].copy_from_slice(&base.to_ne_bytes()); // xlp_pageaddr
    buf[16..20].copy_from_slice(&0u32.to_ne_bytes()); // xlp_rem_len (no contrecord)
    buf[24..32].copy_from_slice(&FUZZ_SYSID.to_ne_bytes());
    buf[32..36].copy_from_slice(&(seg as u32).to_ne_bytes());
    buf[36..40].copy_from_slice(&(XLOG_BLCKSZ as u32).to_ne_bytes());

    // Record header (SIZE_OF_XLOG_RECORD bytes) at the long-header boundary.
    let ro = SIZE_OF_XLOG_LONG_PHD; // 40, MAXALIGN-clean
    let tot = SIZE_OF_XLOG_RECORD + body.len();
    buf[ro..ro + 4].copy_from_slice(&(tot as u32).to_ne_bytes()); // xl_tot_len
    buf[ro + 4..ro + 8].copy_from_slice(&0u32.to_ne_bytes()); // xl_xid
    buf[ro + 8..ro + 16].copy_from_slice(&0u64.to_ne_bytes()); // xl_prev (< rec_ptr, rand access)
    buf[ro + 16] = 0; // xl_info
    buf[ro + 17] = 0; // xl_rmid = RM_XLOG_ID (valid → reaches decode)
                      // ro+18..20 padding (0); ro+20..24 xl_crc filled below.
    let data_off = ro + SIZE_OF_XLOG_RECORD;
    buf[data_off..data_off + body.len()].copy_from_slice(body);

    // record CRC = crc(body) then crc(header[..offsetof xl_crc]) (xlogreader.c).
    let mut crc = 0xFFFF_FFFFu32;
    crc = crc32c::pg_comp_crc32c(crc, &buf[data_off..data_off + body.len()]);
    crc = crc32c::pg_comp_crc32c(crc, &buf[ro..ro + 20]);
    crc ^= 0xFFFF_FFFF;
    buf[ro + 20..ro + 24].copy_from_slice(&crc.to_ne_bytes());

    buf
}

pub fn wal_record(data: &[u8]) {
    // Keep the record on a single page: page - long-header - record-header.
    let cap = XLOG_BLCKSZ as usize - SIZE_OF_XLOG_LONG_PHD - SIZE_OF_XLOG_RECORD;
    let body = if data.len() > cap { &data[..cap] } else { data };

    let base = FUZZ_SEG_SIZE as u64;
    let page = build_wal_page(base, FUZZ_SEG_SIZE, body);

    let cx = MemoryContext::new("wal_fuzz");
    let mut r = match XLogReaderState::allocate(cx.mcx(), FUZZ_SEG_SIZE) {
        Ok(r) => r,
        Err(_) => return,
    };
    r.system_identifier = FUZZ_SYSID;

    let rec_ptr = base + SIZE_OF_XLOG_LONG_PHD as u64;
    r.XLogBeginRead(rec_ptr);

    let mut src = PageSrc {
        page,
        base,
        end: base + XLOG_BLCKSZ as u64,
    };

    if r.XLogReadRecord(&mut src).ok().flatten().is_some() {
        // Exercise the borrowed-range accessors: these resolve index ranges
        // into the decode scratch, a second slice-bounds surface.
        let _ = r.XLogRecGetData();
        let _ = r.XLogRecGetDataLen();
        let _ = r.XLogRecHasAnyBlockRefs();
        for id in 0..=XLR_MAX_BLOCK_ID {
            let id = id as u8;
            if r.XLogRecHasBlockRef(id) {
                let _ = r.XLogRecGetBlockData(id);
                let _ = r.XLogRecHasBlockImage(id);
                let _ = r.XLogRecBlockImageApply(id);
                let _ = r.XLogRecGetBlockFlags(id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Target 2: wire protocol message parsing (pqformat getters + mb verifiers).
// ---------------------------------------------------------------------------
//
// The first byte selects a server encoding; the per-encoding multibyte
// verifier (the classic panic surface: length walks past buffer end,
// incomplete trailing chars) is exercised directly, then the remaining bytes
// drive the pqformat message-getter state machine — the exact primitives the
// extended-query dispatch (Parse/Bind/Execute/Describe) reads frontend
// messages with. An in-message opcode byte selects each getter; every step
// consumes >=1 byte so the loop always terminates.

use pqformat as pqf;
use stringinfo::StringInfo;

pub fn wire_pqformat(data: &[u8]) {
    let (enc_sel, rest) = match data.split_first() {
        Some(x) => x,
        None => return,
    };

    // Per-encoding byte verification (mbutils), no_error=true → returns bool.
    let enc = (*enc_sel as i32) % wchar::_PG_LAST_ENCODING_;
    let _ = mbutils::pg_verify_mbstr(enc, rest, true);
    let _ = mbutils::pg_verify_mbstr_len(enc, rest, true);
    let _ = mbutils::pg_mbstrlen(rest);

    let cx = MemoryContext::new("wire_fuzz");
    let mcx = cx.mcx();

    // Build a StringInfo whose payload is `rest`, then run the getter machine.
    let mut vec = match mcx::vec_with_capacity_in::<u8>(mcx, rest.len()) {
        Ok(v) => v,
        Err(_) => return,
    };
    if mcx::vec_append_bytes(&mut vec, rest).is_err() {
        return;
    }
    let mut msg = match StringInfo::from_vec(vec) {
        Ok(m) => m,
        Err(_) => return,
    };

    loop {
        let op = match pqf::pq_getmsgbyte(&mut msg) {
            Ok(b) => b as u8,
            Err(_) => break,
        };
        match op % 12 {
            0 => {
                let _ = pqf::pq_getmsgint(&mut msg, 1);
            }
            1 => {
                let _ = pqf::pq_getmsgint(&mut msg, 2);
            }
            2 => {
                let _ = pqf::pq_getmsgint(&mut msg, 4);
            }
            3 => {
                let _ = pqf::pq_getmsgint64(&mut msg);
            }
            4 => {
                let _ = pqf::pq_getmsgfloat4(&mut msg);
            }
            5 => {
                let _ = pqf::pq_getmsgfloat8(&mut msg);
            }
            6 => {
                let n = pqf::pq_getmsgbyte(&mut msg).unwrap_or(0) as usize;
                let _ = pqf::pq_getmsgbytes(&mut msg, n);
            }
            7 => {
                let _ = pqf::pq_getmsgstring(mcx, &mut msg);
            }
            8 => {
                let _ = pqf::pq_getmsgrawstring(&mut msg);
            }
            9 => {
                let n = pqf::pq_getmsgbyte(&mut msg).unwrap_or(0) as usize;
                let _ = pqf::pq_getmsgtext(mcx, &mut msg, n);
            }
            10 => {
                let _ = pqf::pq_getmsgend(&mut msg);
            }
            _ => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic seed inputs: the drivers must not panic on these, and this
    // gives a stable-toolchain smoke check without cargo-fuzz.
    #[test]
    fn wal_record_seeds() {
        wal_record(b"");
        wal_record(b"\xff\x05first"); // DATA_SHORT block id + len + payload
        wal_record(&[0u8; 300]);
        wal_record(&[0xffu8; 9000]); // over one-page cap → truncated
        for i in 0u8..=255 {
            wal_record(&[i, i.wrapping_add(1), i.wrapping_mul(3), 0, 1, 2, 3]);
        }
    }

    // Regression: crashes found by the first wal_record libFuzzer campaign
    // (2026-07-08). Both are wire-controlled u32/u16 arithmetic overflows in
    // decode_record that abort under overflow-checks; fixed to C's defined
    // wraparound (BLCKSZ-bimg_len and the datatotal accumulations).
    #[test]
    fn wal_record_overflow_regressions() {
        // bimg_len > BLCKSZ → `BLCKSZ as u16 - bimg_len` underflow.
        wal_record(&[6, 251, 1, 0, 0, 194, 254, 1, 0, 72, 6]);
        // DATA_LONG main_data_len = 0xFFFFFFFF → `datatotal += ...` overflow.
        wal_record(&[
            0, 32, 4, 0, 1, 0, 0, 0, 0, 91, 0, 0, 46, 255, 2, 104, 105, 100, 97, 116, 254, 255,
            255, 255, 255, 255, 255, 255, 0, 0, 255, 2, 104, 105, 100, 97, 116, 97,
        ]);
        // wave2: HAS_IMAGE bimg_len=8 then DATA_LONG main_data_len=0xFFFFFFFF
        // wraps datatotal past the gate → payload-copy slice-OOB (now a clean
        // invalid-length reject; asserted in xlogreader's own tests).
        wal_record(&[
            0, 29, 0, 0, 8, 0, 0, 0, 4, 255, 0, 1, 8, 39, 4, 9, 170, 170, 170, 170, 170, 170,
            170, 0, 1, 254, 255, 255, 255, 255, 2, 0, 8, 0, 255, 0, 0,
        ]);
    }

    #[test]
    fn wire_pqformat_seeds() {
        wire_pqformat(b"");
        wire_pqformat(b"\x06hello\x00world\x00");
        wire_pqformat(&[6, 7, 0, 1, 2, 3, 8, 9, 10, 255, 0, 0, 0, 4]);
        for i in 0u8..=255 {
            wire_pqformat(&[i, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
        }
    }
}
