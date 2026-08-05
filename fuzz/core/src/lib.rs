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
/// Serialize every path into the in-process C oracles.
///
/// The vendored C carries process-global mutable state with no C-side
/// synchronization (C Postgres is one-thread-per-backend and never sees
/// concurrency). On the wave-3 train, high `--test-threads` runs corrupt the
/// float oracle's degree-constant statics — a deterministic spurious dcotd
/// "divergence" (C returned -1.6e-303 for cotd(-1e308), true value 0.4877…)
/// — with jsonbio_diff implicated (its oracle externs into pg_float_io.c);
/// seam-env installs also TOCTOU-race across modules. Fuzz binaries are
/// one-target-per-process and unaffected (the uncontended lock is noise
/// next to an exec).
///
/// 2026-08-02 (fix/mutants-rail): serialization moved from test discipline to
/// the ORACLE ENTRY itself — every `pub fn *_diff`/`*_replay` driver takes
/// `oracle_serial()` on entry, so a caller that forgets the test-side guard
/// (that is how crypt-des.c:662's `static char output[21]` got raced) is
/// still safe, and a suite's verdict no longer depends on `--test-threads`.
/// Reentrant per thread: tests hold `c_oracle_serial()` and then call a
/// driver, and drivers may call sibling drivers — an inner acquisition on
/// the owning thread is a no-op instead of a self-deadlock.
///
/// 2026-08-02 (task #125, oracle-serial guards): the discipline is now
/// MECHANICALLY enforced on both planes, because the hand sweep above still
/// left 25 unguarded test entry points (docs/conformance/
/// scribbler-investigation-2026-08-02.md §4; one was a live cross-thread
/// free() generator via pg_tzf_reset):
///   - statically, scripts/lint-oracle-serial.py walks every #[test] in
///     fuzz/core/src to any reachable C oracle extern and fails on a path
///     with no oracle_serial() frame (wired into scripts/lint-gates.sh);
///   - at runtime, this guard publishes its holder thread to the C side
///     (csrc/pg_oracle_guard.c), and instrumented shim entries verify the
///     CALLING thread is the holder — release-effective (never a
///     debug_assert; the debug-assert masking law), panicking through
///     `pgf_oracle_guard_violation` with the entry and test name.
static ORACLE_M: std::sync::Mutex<()> = std::sync::Mutex::new(());
thread_local! {
    static ORACLE_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

extern "C" {
    // csrc/pg_oracle_guard.c: process-global holder cell for the runtime
    // holder check. Both calls are made while ORACLE_M is held.
    fn pg_oracle_guard_enter();
    fn pg_oracle_guard_exit();
}

// pub (not pub(crate)): integration-test crates under fuzz/core/tests/ hold
// this guard too, via `decoder_fuzz::c_oracle_serial()` (task #144).
pub struct OracleSerial(#[allow(dead_code)] Option<std::sync::MutexGuard<'static, ()>>);

extern "C" {
    /// H0 SCRIBBLER detector (task #112): exact validity predicate over the
    /// timestamp oracle's `datecache`/`deltacache` statics — every legal
    /// entry is NULL or points into `datetktbl`/`deltatktbl`. 0 = sane;
    /// else 100+i / 200+i names the first poisoned slot (and the caches are
    /// cleared so the poison cannot cascade). Defined in
    /// csrc/pg_timestamp_io.c; see
    /// docs/conformance/scribbler-investigation-2026-08-02.md §5 H0.
    fn pg_tsdiff_cache_check() -> i32;
    /// H6 detector (task #112, after ATTRIBUTION): guard band + capacity
    /// invariant over the float-in shim's message buffer — the truncating
    /// version of that buffer WAS the scribbler's writer (§8). 0 = intact;
    /// 1 = a truncating shim is back; 2+off = a verbatim body indexed past
    /// the string it was handed. Defined in csrc/pg_float_io.c.
    fn pg_diff_msgbuf_check() -> i32;
    /// Same detector over the network oracle's pstrdup buffer — the SECOND
    /// fixed-buffer pstrdup in the tree (shim-contract census, task
    /// #129/#131). Defined in csrc/pg_network_io.c.
    fn pg_network_msgbuf_check() -> i32;
}

impl Drop for OracleSerial {
    fn drop(&mut self) {
        let depth = ORACLE_DEPTH.with(|d| {
            let v = d.get() - 1;
            d.set(v);
            v
        });
        // H0 detector: on final oracle exit (still holding ORACLE_M — the
        // guard field drops after this body), verify the timestamp caches
        // and name the poisoning test at the moment of the corrupting write.
        // Release-effective by design: no debug_assert, no sanitizer.
        if depth == 0 {
            // Clear the C-side holder first (before H0 can panic and before
            // the mutex guard field drops): both stores happen with the
            // lock held, so enter/exit never race each other.
            unsafe { pg_oracle_guard_exit() };
            let code = unsafe { pg_tsdiff_cache_check() };
            if code != 0 {
                let t = std::thread::current();
                let msg = format!(
                    "SCRIBBLER H0: timestamp oracle cache poisoned (code {code}: \
                     {} slot {}) detected at oracle exit in test thread {:?}",
                    if code < 200 { "datecache" } else { "deltacache" },
                    code % 100,
                    t.name().unwrap_or("<unnamed>"),
                );
                if std::thread::panicking() {
                    // Don't double-panic (abort) while unwinding a test
                    // failure that held the guard; the report still lands.
                    eprintln!("{msg}");
                } else {
                    panic!("{msg}");
                }
            }
            // H6 detector: the shim message buffer's guard band. Catches the
            // ATTRIBUTED scribbler class at the writer's own TU instead of at
            // a downstream victim — see csrc/pg_float_io.c pstrdup.
            let code = unsafe { pg_diff_msgbuf_check() };
            if code != 0 {
                let t = std::thread::current();
                let msg = format!(
                    "SCRIBBLER H6: float-in shim message buffer overrun (code \
                     {code}: {}) detected at oracle exit in test thread {:?} — \
                     a verbatim body indexed past the string pstrdup handed it; \
                     see csrc/pg_float_io.c and docs/conformance/\
                     scribbler-investigation-2026-08-02.md §8",
                    if code == 1 {
                        "capacity < string length (truncating shim is back)".to_string()
                    } else {
                        format!("guard byte +{} clobbered", code - 2)
                    },
                    t.name().unwrap_or("<unnamed>"),
                );
                if std::thread::panicking() {
                    eprintln!("{msg}");
                } else {
                    panic!("{msg}");
                }
            }
            // Same band over the network oracle's pstrdup buffer (the tree's
            // second fixed-buffer pstrdup; shim-contract census #129/#131).
            let code = unsafe { pg_network_msgbuf_check() };
            if code != 0 {
                let t = std::thread::current();
                let msg = format!(
                    "SCRIBBLER H6 (network): shim pstrdup buffer overrun \
                     (code {code}: {}) detected at oracle exit in test thread \
                     {:?} — see csrc/pg_network_io.c pstrdup",
                    if code == 1 {
                        "capacity < string length (truncating shim is back)".to_string()
                    } else {
                        format!("guard byte +{} clobbered", code - 2)
                    },
                    t.name().unwrap_or("<unnamed>"),
                );
                if std::thread::panicking() {
                    eprintln!("{msg}");
                } else {
                    panic!("{msg}");
                }
            }
        }
    }
}

pub(crate) fn oracle_serial() -> OracleSerial {
    let depth = ORACLE_DEPTH.with(|d| {
        let v = d.get();
        d.set(v + 1);
        v
    });
    if depth == 0 {
        // Poison-tolerant: a divergence panic in one test must not cascade
        // "poisoned Mutex" noise into siblings.
        let guard = ORACLE_M.lock().unwrap_or_else(|e| e.into_inner());
        unsafe { pg_oracle_guard_enter() };
        OracleSerial(Some(guard))
    } else {
        OracleSerial(None)
    }
}

/// Violation hook for csrc/pg_oracle_guard.c's holder check: a C oracle
/// entry ran on a thread that does not hold `oracle_serial()`. Panics with
/// the C entry name and the calling thread's name (under cargo test, the
/// offending test). "C-unwind" so the panic propagates through the C entry
/// frame back into the calling test where unwind tables exist; where they
/// don't, the runtime aborts — still loud, message already printed.
///
/// `oracle_guard_trap` redirects the next violation into a cell instead of
/// panicking — the must-fail control (oracle_guard_tests.rs) uses it to
/// prove the check fires without killing the suite.
#[no_mangle]
pub extern "C-unwind" fn pgf_oracle_guard_violation(entry: *const std::os::raw::c_char) {
    let entry = if entry.is_null() {
        "<null>".to_string()
    } else {
        unsafe { std::ffi::CStr::from_ptr(entry) }
            .to_string_lossy()
            .into_owned()
    };
    if let Ok(mut trap) = ORACLE_GUARD_TRAP.lock() {
        if let Some(cell) = trap.as_mut() {
            cell.push(entry);
            return;
        }
    }
    let thread = std::thread::current();
    panic!(
        "ORACLE GUARD VIOLATION: C oracle entry `{entry}` called on a thread \
         that does not hold oracle_serial() (thread/test: {:?}). Take \
         `let _g = crate::c_oracle_serial();` on THIS thread before calling \
         into the C oracle — see fuzz/core/src/lib.rs and \
         scripts/lint-oracle-serial.py.",
        thread.name().unwrap_or("<unnamed>")
    );
}

static ORACLE_GUARD_TRAP: std::sync::Mutex<Option<Vec<String>>> = std::sync::Mutex::new(None);

/// Arm the trap (test-only): violations are recorded, not panicked. Returns
/// the recorded entries when disarmed.
#[cfg(test)]
pub(crate) fn oracle_guard_trap_arm() {
    *ORACLE_GUARD_TRAP.lock().unwrap() = Some(Vec::new());
}

#[cfg(test)]
pub(crate) fn oracle_guard_trap_disarm() -> Vec<String> {
    ORACLE_GUARD_TRAP.lock().unwrap().take().unwrap_or_default()
}

/// Test-side spelling of [`oracle_serial`]. In-crate tests take
/// `crate::c_oracle_serial()`; integration-test crates under
/// fuzz/core/tests/ (their own crates — `crate::` is not decoder_fuzz there,
/// and `#[cfg(test)]` items don't exist in the library they link) take
/// `decoder_fuzz::c_oracle_serial()`. That is why this is `pub` and not
/// cfg(test)-gated: wcharfam_exhaustive.rs shipped 11 unguarded tests while
/// the guard was unreachable from outside the crate (task #144).
pub fn c_oracle_serial() -> OracleSerial {
    oracle_serial()
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

    /// Must-fail control for the regex_diff/regexp_diff seam-race fix
    /// (c9e3c10911b, task #155): that fix makes the IDENTITY install
    /// first-wins by swallowing the "seam installed twice" panic with
    /// `catch_unwind` at both drivers' init sites. This control pins the two
    /// properties the swallow relies on, in THIS binary:
    ///   1. a genuinely CONFLICTING second install is still DETECTED —
    ///      `seam_core::set()`'s double-install panic must stay live, and
    ///   2. it fails WITHOUT clobbering the shipped impl (set() is a
    ///      compare_exchange from the stub, so the loser never writes) —
    /// which is what makes it safe to run in parallel with the live regex
    /// tests. If someone ever "cleans up" the double-install panic or lets a
    /// second installer win the slot, this test fails.
    #[test]
    #[should_panic(expected = "seam installed twice: regex_core_seams::pg_regcomp")]
    fn conflicting_regex_core_seam_install_still_panics() {
        fn conflicting(
            _pattern: &[types_core::PgWChar],
            _cflags: i32,
            _collation: types_core::Oid,
        ) -> types_error::PgResult<regex::RegcompResult> {
            unreachable!("a conflicting install must never win the seam slot")
        }
        // Identity install first, exactly as the drivers do (first-wins; a
        // benefactor family may already have installed the shipped impls —
        // the identical fn pointers either way).
        let _ = std::panic::catch_unwind(regex_core::init_seams);
        assert!(regex_core_seams::pg_regcomp::is_installed());
        // The conflicting install must still fail loudly.
        regex_core_seams::pg_regcomp::set(conflicting);
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

// vltext_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-vltext_diff.md.
pub mod vltext_diff;
pub use vltext_diff::vltext_diff;

// vlbytea_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-vlbytea_diff.md.
pub mod vlbytea_diff;
pub use vlbytea_diff::vlbytea_diff;

// vlmisc_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-vlmisc_diff.md.
pub mod vlmisc_diff;
pub use vlmisc_diff::vlmisc_diff;

// quote_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-quote_diff.md.
pub mod quote_diff;
pub use quote_diff::quote_diff;

// fmt_dch_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-fmt_dch_diff.md.
pub mod fmt_dch_diff;
pub use fmt_dch_diff::fmt_dch_diff;

// fmt_num_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-fmt_num_diff.md.
pub mod fmt_num_diff;
pub use fmt_num_diff::fmt_num_diff;

// geo_io_diff: geo text-I/O family (p1-laner) — see core/src/geo_io_diff.rs.
pub mod geo_io_diff;
pub use geo_io_diff::geo_io_diff;

// tsquery_core_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-tsquery_core_diff.md.
pub mod tsquery_core_diff;
pub use tsquery_core_diff::tsquery_core_diff;

// tsqrw_diff: scaffolded by fuzz/scaffold.py (p1-lanef handoff, taken over by
// p1-laneaf, rewritten by task #135) — see ../../README-TODO-tsqrw_diff.md.
pub mod tsqrw_diff;
pub use tsqrw_diff::tsqrw_diff;

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
// stub:* — shared constructed-state builder facilities (fuzz/stub-constructed):
// state-shaped inputs built identically on the Rust and C-oracle sides from
// the same fuzz bytes. See fuzz/STUBS.md.
pub mod stub_encoding;
#[cfg(test)]
mod stub_controls_tests;
// H0 SCRIBBLER detector controls (task #112): clean-path + must-fail poison.
#[cfg(test)]
mod scribbler_h0_tests;
// SCRIBBLER attribution harness: deterministic single-thread seed bisect.
#[cfg(test)]
mod scribbler_bisect_tests;
// must-fail controls for the oracle-serialization holder check
// (csrc/pg_oracle_guard.c; see oracle_serial() above).
#[cfg(test)]
mod oracle_guard_tests;
pub mod stub_nodes;
pub mod stub_snapshot;
pub mod stub_syscache;
pub mod stub_syscache_harvest;
pub mod stub_tupdesc;

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

/// SHARED check_for_interrupts-seam installer (no-op impl: fuzz harnesses
/// have no interrupt plane).
///
/// The tsvec targets (tsvector_core_diff arm 7 match, tsrank_diff cover
/// walks) reach the CHECK_FOR_INTERRUPTS calls restored in
/// tsvector_core::execute (229915b8d7); without an installed seam every
/// such exec panics "seam not installed". In the shared test binary the
/// panic hid behind other modules (regexp_diff/jsonpathexec_diff/...)
/// installing the seam first — filtered runs and the standalone fuzz
/// binaries had no such benefactor. Same first-wins/no-poison discipline
/// as install_detoast_seam_once above.
pub fn install_check_for_interrupts_seam_once() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        if !postgres_seams::check_for_interrupts::is_installed() {
            let _ = std::panic::catch_unwind(|| {
                postgres_seams::check_for_interrupts::set(|| Ok(()))
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
// regex_diff (p1-regexcore): Spencer ENGINE differential — shipped
// regex_core vs the verbatim 18.3 engine under csrc/regexfam/vendor/
// (second, pristine-symbol engine copy; the regexp_diff family's copy is
// rxo_-renamed — see build.rs).
pub mod regex_diff;
pub use regex_diff::regex_diff;
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
// spellfam_diff (p1-spell): tsearch/spell ispell/hunspell dictionary loader
// + normalizer vs vendored 18.3 C.
pub mod spellfam_diff;
pub use spellfam_diff::spellfam_diff;
// contriba_diff (p1-mb-contriba): contrib fuzzystrmatch + isn two-crate family.
pub mod contriba_diff;
pub use contriba_diff::contriba_diff;

// define_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-define_diff.md.
pub mod define_diff;
pub use define_diff::define_diff;

// nodesfam_diff (p1-nodes): readfuncs/outfuncs/copyfuncs node walkers vs
// vendored 18.3 C (read->out->copy->out round-trip, all planes).
pub mod nodesfam_diff;

// tsvector_core_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-tsvector_core_diff.md.
pub mod tsq_gen;
pub mod tsvector_core_diff;
pub use tsvector_core_diff::tsvector_core_diff;

// tsrank_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-tsrank_diff.md.
pub mod tsrank_diff;
pub use tsrank_diff::tsrank_diff;

// int_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-int_diff.md.
pub mod int_diff;
pub use int_diff::int_diff;

// network_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-network_diff.md.
pub mod network_diff;
pub use network_diff::network_diff;

// pgcryptofam (p1-pgcryptofam): FFI surface of the verbatim 18.3
// contrib/pgcrypto crypt()/gen_salt()/armor oracle (csrc/pgcryptofam/).
// Declarations + thin wrappers only; the differential driver is a
// separate step.
pub mod pgcryptofam;

// pgcryptofam_diff (p1-pgcryptofam): the differential driver over the oracle
// above, plus the exhaustive-domain sweeps for its file-static helpers.
pub mod pgcryptofam_diff;
pub use pgcryptofam_diff::pgcryptofam_diff;
#[cfg(test)]
mod pgcryptofam_sweeps;
// guc_file_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-guc_file_diff.md.
pub mod guc_file_diff;
pub use guc_file_diff::guc_file_diff;

// timeline_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-timeline_diff.md.
pub mod timeline_diff;
pub use timeline_diff::timeline_diff;

// spgquad_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-spgquad_diff.md.
pub mod spgquad_diff;
pub use spgquad_diff::spgquad_diff;

// guc_units_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-guc_units_diff.md.
pub mod guc_units_diff;
pub use guc_units_diff::guc_units_diff;

// spgbox_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-spgbox_diff.md.
pub mod spgbox_diff;
pub use spgbox_diff::spgbox_diff;
// tsm_system_rows_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-tsm_system_rows_diff.md.
pub mod tsm_system_rows_diff;
pub use tsm_system_rows_diff::tsm_system_rows_diff;

// tsm_system_time_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-tsm_system_time_diff.md.
pub mod tsm_system_time_diff;
pub use tsm_system_time_diff::tsm_system_time_diff;

// tablesample_diff (p1-wavea): backend/access/tablesample (BERNOULLI/SYSTEM)
// vs verbatim bernoulli.c/system.c oracle (csrc/pg_tablesample_io.c).
pub mod tablesample_diff;
pub use tablesample_diff::tablesample_diff;

// crypt_be_diff (p1-wavea): backend/libpq/crypt vs verbatim crypt.c oracle.
pub mod cryptbe_diff;
pub use cryptbe_diff::crypt_be_diff;

// instrument_diff (p1-wavea): backend/executor/instrument vs verbatim
// instrument.c oracle (csrc/pg_instrbe_io.c, assembled by
// csrc/gen/assemble_instrbe.sh).
pub mod instrument_diff;
pub use instrument_diff::instrument_diff;

// ltree_diff (p1-ltree-t74, task #74): contrib/ltree label-tree IO + operator
// family vs the whole-TU verbatim 18.3 oracle (csrc/pg_ltreefam_io.c).
pub mod ltree_diff;
pub use ltree_diff::ltree_diff;

// trgm_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-trgm_diff.md.
pub mod trgm_diff;
pub use trgm_diff::trgm_diff;

// scalarxid_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-scalarxid_diff.md.
pub mod scalarxid_diff;
pub use scalarxid_diff::scalarxid_diff;

// snapio_diff: scaffolded by fuzz/scaffold.py — see ../../README-TODO-snapio_diff.md.
pub mod snapio_diff;
pub use snapio_diff::snapio_diff;
