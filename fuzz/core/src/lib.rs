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

// Differential targets (shipped Rust vs vendored PostgreSQL C oracle):
// float4in/float8in, float4out/float8out, point_out/on_ppath. See diff.rs.
/// Serialize `cargo test` cases that drive the in-process C oracles.
///
/// The vendored C carries process-global mutable state with no C-side
/// synchronization (C Postgres is one-thread-per-backend and never sees
/// concurrency). On the wave-3 train, high `--test-threads` runs corrupt the
/// float oracle's degree-constant statics — a deterministic spurious dcotd
/// "divergence" (C returned -1.6e-303 for cotd(-1e308), true value 0.4877…)
/// — with jsonbio_diff implicated (its oracle externs into pg_float_io.c);
/// seam-env installs also TOCTOU-race across modules. Fuzz binaries are
/// one-target-per-process and unaffected. A follow-up owns finding the
/// racing writer; this lock makes the `cargo test` signal deterministic
/// without masking single-run divergences. Poison-tolerant: a divergence
/// panic in one test must not cascade "poisoned Mutex" noise into siblings.
#[cfg(test)]
pub(crate) fn c_oracle_serial() -> std::sync::MutexGuard<'static, ()> {
    static M: std::sync::Mutex<()> = std::sync::Mutex::new(());
    M.lock().unwrap_or_else(|e| e.into_inner())
}

// stubs: the shared stub-pin facility (stub:guc / stub:clock / stub:prng /
// stub:workmem) — both-sides pinned session state for state-dependent
// differential targets. See fuzz/STUBS.md and csrc/stubshims/.
pub mod stubs;

pub mod diff;
pub use diff::{
    float_in_diff, float_math2_diff, float_math_diff, float_misc_diff, float_out_diff, geo_diff,
};
pub mod diff_charbool;
pub use diff_charbool::{bool_diff, char_diff};
pub mod pseudo_diff;
pub use pseudo_diff::pseudotypes_diff;
pub mod lsn_diff;
pub use lsn_diff::pg_lsn_diff;

// p1-lanec string-family batch (common/{string,archive,percentrepl,relpath,
// wait_error}) vs vendored 18.3 C. See strfam.rs.
pub mod strfam;
pub use strfam::strfam_diff;

// Lane-0B differential targets (100%-coverage campaign, proofs/p1-lane0b):
pub mod cash_diff;
pub mod mac_diff;
pub mod name_diff;
pub mod uuid_diff;
pub use cash_diff::cash_diff;
pub use mac_diff::mac_diff;
pub use name_diff::name_diff;
pub use uuid_diff::uuid_diff;

// hashenc_diff (p1-lanee): base64/md5/sha1/sha2/hmac/scram/to_ascii/crc
// family vs vendored 18.3 C (csrc/hashenc/). See hashenc.rs.
pub mod hashenc;
pub use hashenc::hashenc_diff;

// cryptofam_diff (p1-lanef crypto/hash family batch): md5/sha1/hmac/scram +
// adt/cryptohashfuncs fmgr wrappers vs vendored 18.3 C. See cryptofam.rs.
pub mod cryptofam;
pub use cryptofam::cryptofam_diff;

// tablesfam_diff (p1-lanef tables batch): keywords + unicode_category vs
// vendored 18.3 C. See tablesfam.rs.
pub mod tablesfam;
pub use tablesfam::tablesfam_diff;

// enc_tables_diff (p1-laneg batch): base64 / to_ascii / keywords vs
// vendored 18.3 C. See enc_tables.rs.
pub mod enc_tables;
pub use enc_tables::enc_tables_diff;

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

// encode_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-encode_diff.md.
pub mod encode_diff;
pub use encode_diff::encode_diff;

// datetime_io_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-datetime_io_diff.md.
pub mod datetime_convert_diff;
pub mod datetime_engine_diff;
pub mod datetime_io_diff;
pub mod interval_engine_diff;
pub use datetime_io_diff::datetime_io_diff;

// quote_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-quote_diff.md.
pub mod quote_diff;
pub use quote_diff::quote_diff;

// geo_io_diff: geo text-I/O family (p1-laner) — see core/src/geo_io_diff.rs.
pub mod geo_io_diff;
pub use geo_io_diff::geo_io_diff;

// json_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-json_diff.md.
pub mod json_diff;
pub use json_diff::json_diff;
// arrayfuncs_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-arrayfuncs_diff.md.
pub mod arrayfuncs_diff;
pub use arrayfuncs_diff::arrayfuncs_diff;
// mbconv_diff: encoding-conversion family (p1-lanez) — differential fuzz +
// native exhaustive-diff driver vs the proofs/mbconv vendored 18.3 C.
pub mod mbconv_diff;
pub use mbconv_diff::mbconv_diff;
// numutils_diff (p1-laneaj adt/numutils batch): strtoint/uint*in_subr parse
// family + the itoa/ultostr emit family vs vendored 18.3 C. See
// core/src/numutils_diff.rs.
pub mod numutils_diff;
pub use numutils_diff::numutils_diff;
// oraclefam_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-oraclefam_diff.md.
pub mod oraclefam_diff;
pub use oraclefam_diff::oraclefam_diff;
// p1-laneah: common/wchar + mb/mbutils dual-exec differential target
// (oracle csrc/pg_wcharfam.c + csrc/wcharfam/, verbatim 18.3)
pub mod wcharfam;
pub use wcharfam::wcharfam_diff;
// rowtypes_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-rowtypes_diff.md.
pub mod rowtypes_diff;
pub use rowtypes_diff::rowtypes_diff;
// tupaccess_diff: heaptuple/tupdesc/attmap/tupconvert differential (p1-tupaccess).
pub mod tupaccess_diff;
pub use tupaccess_diff::tupaccess_diff;

// array_userfuncs_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-array_userfuncs_diff.md.
pub mod array_userfuncs_diff;
pub use array_userfuncs_diff::array_userfuncs_diff;
// jsonbio_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-jsonbio_diff.md.
pub mod jsonbio_diff;
pub use jsonbio_diff::jsonbio_diff;
// jsonbops_diff: two-doc ops/mutate/getfield sibling (p1-lanev).
pub mod jsonbops_diff;
pub use jsonbops_diff::jsonbops_diff;
// pg_prng_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-pg_prng_diff.md.
pub mod pg_prng_diff;
pub use pg_prng_diff::pg_prng_diff;

// arrayutils_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-arrayutils_diff.md.
pub mod arrayutils_diff;
pub use arrayutils_diff::arrayutils_diff;

// hashfn_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-hashfn_diff.md.
pub mod hashfn_diff;
pub use hashfn_diff::hashfn_diff;
// timestamp_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-timestamp_diff.md.
pub mod timestamp_diff;
pub use timestamp_diff::timestamp_diff;
// jsonpath_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-jsonpath_diff.md.
pub mod jsonpath_diff;
pub use jsonpath_diff::jsonpath_diff;

// jsonpathexec_diff (p1-laneaa, adt/jsonpath_exec) — see
// ../../README-TODO-jsonpathexec_diff.md.
pub mod jsonpathexec_diff;
pub use jsonpathexec_diff::jsonpathexec_diff;
// rangetypes_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-rangetypes_diff.md.
pub mod rangetypes_diff;
pub use rangetypes_diff::rangetypes_diff;

// multirangetypes_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-multirangetypes_diff.md.
pub mod multirangetypes_diff;
pub use multirangetypes_diff::multirangetypes_diff;

/// SHARED detoast-seam installer for the range family targets.
///
/// `seam_core::seam!`'s `set()` PANICS on a second install, and both
/// rangetypes_diff and multirangetypes_diff need the seam. Two independent
/// `Once` guards therefore raced to a "seam installed twice" panic as soon as
/// both drivers ran in one process — which is exactly what the shared
/// `cargo test` binary does, and what a multi-target fuzz job would do. One
/// `Once` for the process, called by both.
///
/// The seam is ENVIRONMENT; the detoast logic is COMPUTATION and is the SHIPPED
/// implementation, never a mock (minimal-seaming rule).
pub fn install_detoast_seam_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // First-wins across ALL lanes sharing the test binary (json/jsonb/
        // array harnesses install this seam too; installs are serialized by
        // c_oracle_serial in tests). Every installed impl is the identity
        // copy for the inline images these harnesses exchange, so losing
        // the race is fine — and set() must stay unpanicked so this Once
        // never poisons.
        if !detoast_seams::detoast_attr::is_installed() {
            let _ = std::panic::catch_unwind(|| {
                detoast_seams::detoast_attr::set(detoast::detoast_attr)
            });
        }
    });
}
// numericfam (p1-laneu adt/numeric campaign): whole-numeric.c oracle,
// two targets (io + ops) over one pg_diff_num_call ABI.
pub mod numericfam;
pub use numericfam::{numeric_io_diff, numeric_ops_diff};

// datetime_closeout_diff: p1-lanel2 closeout (adt_date extract numeric faces
// vs C + the owed fc-wrapper plane) — see core/src/datetime_closeout_diff.rs.
pub mod datetime_closeout_diff;
pub use datetime_closeout_diff::datetime_closeout_diff;
// like_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-like_diff.md.
pub mod like_diff;
pub use like_diff::like_diff;

// regexp_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-regexp_diff.md.
pub mod regexp_diff;
// miscfam_diff (p1-mb-miscfam): cmdtag/pg_class/earthdistance/pg_rusage/
// xlogstats/stringinfo six-crate family.
pub mod miscfam_diff;
pub use miscfam_diff::miscfam_diff;
pub use regexp_diff::regexp_diff;

// libfam_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-libfam_diff.md.
pub mod libfam_diff;
pub use libfam_diff::libfam_diff;

// radixtree_diff (p1-mb-lib): backend/lib/radixtree vs verbatim 18.3
// lib/radixtree.h template (two instantiations in csrc/pg_radixtree_io.c).
pub mod radixtree_diff;
pub use radixtree_diff::radixtree_diff;
// portfam_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-portfam_diff.md.
pub mod portfam_diff;
pub use portfam_diff::portfam_diff;
// tzfam_diff: p1-mb-tzfam (strftime + tzparser + ts_locale vs vendored
// 18.3 C) — see core/src/tzfam_diff.rs.
pub mod tzfam_diff;
pub use tzfam_diff::tzfam_diff;
// netfam_diff (p1-mb-netfam): libpq ifaddr + pqformat two-crate family.
pub mod netfam_diff;
pub use netfam_diff::netfam_diff;
// contribb_diff (p1-mb-contribb): contrib/seg + contrib/cube vs vendored
// 18.3 C (incl. the generated flex/bison parsers) — see
// core/src/contribb_diff.rs.
pub mod contribb_diff;
pub use contribb_diff::contribb_diff;
// hstorefam_diff (p1-mb-contribc): contrib/hstore vs vendored 18.3 C.
pub mod hstorefam_diff;
pub use hstorefam_diff::hstorefam_diff;
// wparserfam_diff (p1-mb-contribc): tsearch/wparser_def tokenizer vs
// vendored 18.3 C.
pub mod wparserfam_diff;
pub use wparserfam_diff::wparserfam_diff;
// contriba_diff (p1-mb-contriba): contrib fuzzystrmatch + isn two-crate family.
pub mod contriba_diff;
pub use contriba_diff::contriba_diff;

// define_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-define_diff.md.
pub mod define_diff;
pub use define_diff::define_diff;
