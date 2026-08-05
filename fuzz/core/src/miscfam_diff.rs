//! miscfam_diff: differential fuzz driver for six small "misc family"
//! crates vs verbatim vendored PostgreSQL 18.3 C (csrc/pg_miscfam_io.c,
//! upstream sha 62d6c7d3df; lane p1-mb-miscfam). One selector arm per
//! crate; selector = data[0] % 8 (cmdtag owns 3 arms):
//!
//!   0 cmdtag props     — crates/backend/tcop/cmdtag: tag -> (name, namelen,
//!                        event_trigger_ok, table_rewrite_ok,
//!                        display_rowcount) vs cmdtag.c row loads; plus
//!                        InitializeQueryCompletion every exec.
//!   1 cmdtag enum      — GetCommandTagEnum bsearch over arbitrary bytes
//!                        (case-insensitive; interior NUL terminates on
//!                        both sides).
//!   2 cmdtag build_qc  — BuildQueryCompletionString: len + buffer bytes
//!                        incl. the trailing NUL ("INSERT 0 N" compat arm).
//!   3 pg_class         — crates/backend/catalog/pg_class:
//!                        errdetail_relkind_not_supported over the full u8
//!                        relkind domain; Ok detail STRING compared to C's
//!                        captured errdetail value; unknown relkind -> both
//!                        sides error (C elog(ERROR) == XX000 == pgrust
//!                        PgError::error default). Exhaustively swept over
//!                        all 256 relkinds by `tests::pg_class_exhaustive`
//!                        (a0-strength; the fuzz arm keeps the lines in the
//!                        measured-coverage denominator).
//!   4 earthdistance    — crates/contrib/earthdistance:
//!                        geo_distance_internal over 4 raw f64 bit
//!                        patterns; result compared as raw BITS (both
//!                        sides call the one in-process libm — same
//!                        "identical wrapper logic over one libm" posture
//!                        as pg_prng_diff arm 12; C built -ffp-contract=off
//!                        so gcc/clang cannot fuse what rustc never fuses).
//!                        fc plane: fc_geo_distance via the dfmgr-registered
//!                        library lookup (contrib fn — no fixed pg_proc oid,
//!                        no builtins.rs) on a LocalFcinfo frame with
//!                        16-byte point datums.
//!   5 pg_rusage        — crates/backend/utils/misc/pg_rusage:
//!                        pg_rusage_show_delta over two fixture snapshots
//!                        (12 decoded i64 fields folded into the real
//!                        getrusage/gettimeofday domain: usec in
//!                        [0,1_000_000), sec in [0,2^42)); full formatted
//!                        string compared. The OS clock read
//!                        (pg_rusage_init / RUSAGE_WHO / the pg_rusage_show
//!                        wrapper) is the excluded-state carve on both
//!                        sides (C oracle injects the same fixture).
//!   6 xlogstats        — crates/backend/access/transam/xlogstats:
//!                        XLogRecGetLen + XLogRecStoreStats over decoded-
//!                        record fixtures (1..=4 records per exec; rmid and
//!                        info bytes free over the full u8 domain,
//!                        max_block_id in -1..=32, per-block
//!                        in_use/has_image/bimg_len). Fixture validity
//!                        constraint (xlogreader contract): xl_tot_len =
//!                        fpi sum + extra, so rec_len never underflows —
//!                        real decoded records always satisfy this.
//!                        Compared per record: (rec_len, fpi_len) and the
//!                        touched rmgr/record stat cells + total count.
//!   7 stringinfo       — crates/_support/types/stringinfo: op-driver vs
//!                        verbatim src/common/stringinfo.c. Init variants
//!                        (default 1024 / explicit initsize / from_vec),
//!                        then an op stream: append_bytes/appendBinary,
//!                        append_bytes_nt/appendBinaryNT, append_byte/Char,
//!                        append_spaces/Spaces, append_str/String (UTF-8
//!                        lossy, NUL-free), reset, enlarge (incl. the
//!                        MaxAllocSize 54000 error zone, driven so the
//!                        error always fires BEFORE any 1GB allocation),
//!                        truncate (the rmgrdesc roll-back idiom),
//!                        write_fixed (pq_writeintN shape),
//!                        append_bytes_z (appendBinaryStringInfoNT with
//!                        the NUL counted). After EVERY op: len, capacity
//!                        (== C maxlen: PgVec try_reserve_exact ==
//!                        palloc(newlen), doubling policy identical —
//!                        verified), bytes[0..len], and the NUL at [len]
//!                        when the last op defines it (after _nt /
//!                        write_fixed it is transient garbage in C too, as
//!                        in PG). Total appended bytes capped at 64 KiB.
//!
//! Comparison planes: value bytes/bits + error verdict + errcode class
//! (XX000 / 54000); message text out of scope (pg_class's errDETAIL string
//! is a VALUE — the function's entire output — and IS compared). No-panic
//! everywhere.
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - cmdtag arms 0/2: tag folded into 0..193 (the CommandTag enum domain;
//!     out-of-range is UB table over-read in C, index-panic in Rust).
//!   - pg_rusage arm 5: fixture folded into the real clock domain (see
//!     above); outside it C's `(int)` narrowing of the usec quotient and
//!     -fwrapv sec arithmetic are C-implementation artifacts real
//!     getrusage output can never produce (tv_usec < 1e6 by POSIX).
//!   - earthdistance arm 4: NaN-BITS CARVE — when BOTH sides return NaN the
//!     bit patterns are not compared (sign/payload propagation through
//!     commutative FP ops is compiler codegen, not C semantics; C-vs-C
//!     differs by compiler). Everything non-NaN stays bit-exact. See the
//!     block comment above geo_bits_match and
//!     tests::geo_nan_carve_narrowness (lane p1-nanadj, 2026-08-01).
//!   - xlogstats arm 6: tot_len >= fpi sum (decoded-record invariant).
//!   - stringinfo arm 7: enlarge error-zone needed values chosen >=
//!     MaxAllocSize - len so the 54000 guard fires without a gigabyte
//!     repalloc on the C side (guard-boundary crossings still exercised:
//!     needed - (MaxAllocSize - len) sweeps through 0).
//!
//! from_vec (arm 7 init variant 2) is a pgrust-only constructor (C has no
//! counterpart taking an existing buffer); its postconditions (bytes kept,
//! capacity > len, NUL at [len]) are asserted in-driver, then the C side is
//! aligned to the resulting (len, capacity) and the diff continues.
//! write_fixed's `assert!` capacity precondition is driven only when both
//! sides have room (C pq_writeintN asserts the same precondition).

#![allow(dead_code)]

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::Once;

use types_core::CommandTag;
use types_portal::{QueryCompletion, COMPLETION_TAG_BUFSIZE};
use xlogreader_seams::{DecodedBkpBlock, DecodedXLogRecord, XLogReaderState, XLR_MAX_BLOCK_ID};

extern "C" {
    fn pg_mf_cmdtag_props(
        tag: i32,
        namelen: *mut u64,
        evtrgok: *mut i32,
        rwrok: *mut i32,
        rowcnt: *mut i32,
    ) -> *const c_char;
    fn pg_mf_cmdtag_enum(commandname: *const c_char) -> i32;
    fn pg_mf_init_qc(tag: *mut i32, nprocessed: *mut u64);
    fn pg_mf_build_qc(tag: i32, nprocessed: u64, nameonly: i32, buff: *mut c_char) -> u64;
    fn pg_mf_relkind_detail(relkind: u8, out: *mut c_char, outsz: i32) -> i32;
    fn pg_mf_geo_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64;
    fn pg_mf_rusage_show(ru0: *const i64, ru1: *const i64, out: *mut c_char);
    fn pg_mf_xlog_reset();
    fn pg_mf_xlog_store(
        rmid: u8,
        info: u8,
        tot_len: u32,
        max_block_id: i32,
        in_use: *const u8,
        has_image: *const u8,
        bimg_len: *const u16,
        out_rec_len: *mut u32,
        out_fpi_len: *mut u32,
    );
    fn pg_mf_xlog_count() -> u64;
    fn pg_mf_xlog_cell(rmid: i32, recid: i32, rmgr_out: *mut u64, rec_out: *mut u64);
    fn pg_mf_si_init_default() -> i32;
    fn pg_mf_si_init_ext(initsize: i32) -> i32;
    fn pg_mf_si_reset() -> i32;
    fn pg_mf_si_append_bin(data: *const u8, datalen: i32) -> i32;
    fn pg_mf_si_append_bin_nt(data: *const u8, datalen: i32) -> i32;
    fn pg_mf_si_append_char(ch: u8) -> i32;
    fn pg_mf_si_append_spaces(count: i32) -> i32;
    fn pg_mf_si_append_string(s: *const c_char) -> i32;
    fn pg_mf_si_enlarge(needed: i32) -> i32;
    fn pg_mf_si_write_fixed(data: *const u8, n: i32) -> i32;
    fn pg_mf_si_truncate(newlen: i32);
    fn pg_mf_si_get(len: *mut i32, maxlen: *mut i32) -> *const c_char;
}

const NTAGS: u32 = 193; // lengthof(tag_behavior), asserted in tests
const C_ERR_INTERNAL: i32 = 1; // XX000
const C_ERR_PROGRAM_LIMIT: i32 = 2; // 54000

/// Byte-cursor over the fuzz payload; exhausted reads return zeros so every
/// input length is valid.
struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, pos: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }
    fn u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        for s in &mut b {
            *s = self.u8();
        }
        u32::from_le_bytes(b)
    }
    fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        for s in &mut b {
            *s = self.u8();
        }
        u64::from_le_bytes(b)
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let start = self.pos.min(self.d.len());
        let end = (self.pos + n).min(self.d.len());
        self.pos += n;
        &self.d[start..end]
    }
    fn done(&self) -> bool {
        self.pos >= self.d.len()
    }
}

// ---------------- arm 0/2 helpers: cmdtag ----------------

fn check_cmdtag_props(tag: i32) {
    let (mut nl, mut ev, mut rw, mut rc) = (0u64, 0i32, 0i32, 0i32);
    let cname = unsafe { pg_mf_cmdtag_props(tag, &mut nl, &mut ev, &mut rw, &mut rc) };
    let cname = unsafe { std::ffi::CStr::from_ptr(cname) }.to_bytes();

    let t = CommandTag(tag);
    let (rname, rlen) = cmdtag::GetCommandTagNameAndLen(t);
    assert_eq!(rname.as_bytes(), cname, "tag {tag} name");
    assert_eq!(rname, cmdtag::GetCommandTagName(t), "tag {tag} name/len coherence");
    assert_eq!(rlen as u64, nl, "tag {tag} namelen");
    assert_eq!(cmdtag::command_tag_event_trigger_ok(t), ev != 0, "tag {tag} evtrgok");
    assert_eq!(cmdtag::command_tag_table_rewrite_ok(t), rw != 0, "tag {tag} rwrok");
    assert_eq!(cmdtag::command_tag_display_rowcount(t), rc != 0, "tag {tag} rowcount");
}

fn check_cmdtag_enum(name: &[u8]) {
    // C receives the bytes NUL-terminated; interior NUL truncates there on
    // both sides (the Rust entry trims at the first NUL itself).
    let mut c = Vec::with_capacity(name.len() + 1);
    c.extend_from_slice(name);
    c.push(0);
    let ctag = unsafe { pg_mf_cmdtag_enum(c.as_ptr().cast()) };
    let rtag = cmdtag::GetCommandTagEnum(name);
    assert_eq!(rtag.0, ctag, "GetCommandTagEnum({:?})", String::from_utf8_lossy(name));
}

fn check_build_qc(tag: i32, nprocessed: u64, nameonly: bool) {
    let mut cbuf = [0u8; COMPLETION_TAG_BUFSIZE];
    let clen = unsafe {
        pg_mf_build_qc(tag, nprocessed, nameonly as i32, cbuf.as_mut_ptr().cast())
    } as usize;

    let mut rbuf = [0u8; COMPLETION_TAG_BUFSIZE];
    let qc = QueryCompletion { commandTag: CommandTag(tag), nprocessed };
    let rlen = cmdtag::BuildQueryCompletionString(&mut rbuf, &qc, nameonly);

    assert_eq!(rlen, clen, "build_qc len (tag {tag} n {nprocessed} nameonly {nameonly})");
    assert_eq!(
        &rbuf[..=rlen],
        &cbuf[..=clen],
        "build_qc bytes (tag {tag} n {nprocessed} nameonly {nameonly})"
    );
}

fn check_init_qc() {
    let (mut ctag, mut cn) = (0i32, 0u64);
    unsafe { pg_mf_init_qc(&mut ctag, &mut cn) };
    let mut qc = QueryCompletion { commandTag: CommandTag(42), nprocessed: 42 };
    cmdtag::InitializeQueryCompletion(&mut qc);
    assert_eq!((qc.commandTag.0, qc.nprocessed), (ctag, cn), "InitializeQueryCompletion");
}

// ---------------- arm 3: pg_class ----------------

fn pg_class_setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        let _ = std::panic::catch_unwind(pg_class::init_seams);
    });
}

fn check_relkind(relkind: u8) {
    pg_class_setup();
    let mut cbuf = [0u8; 256];
    let cst = unsafe { pg_mf_relkind_detail(relkind, cbuf.as_mut_ptr().cast(), 256) };
    let r = pg_class::errdetail_relkind_not_supported(relkind);
    match r {
        Ok(s) => {
            assert_eq!(cst, 0, "relkind {relkind}: C errored, Rust ok");
            let cdetail = unsafe { std::ffi::CStr::from_ptr(cbuf.as_ptr().cast()) }.to_bytes();
            assert_eq!(s.as_bytes(), cdetail, "relkind {relkind} detail");
        }
        Err(e) => {
            assert_eq!(cst, C_ERR_INTERNAL, "relkind {relkind}: C ok, Rust errored");
            assert_eq!(
                e.sqlstate,
                types_error::ERRCODE_INTERNAL_ERROR,
                "relkind {relkind} sqlstate"
            );
        }
    }
}

// ---------------- arm 4: earthdistance ----------------

fn earthdistance_setup() {
    static SEAMS: Once = Once::new();
    SEAMS.call_once(|| {
        let _ = std::panic::catch_unwind(contrib_earthdistance::init_seams);
    });
}

// ---------------------------------------------------------------------------
// NaN-BITS CARVE (earthdistance arm 4 — both-sides-NaN ONLY; lane p1-nanadj
// adjudication 2026-08-01 of campaign pgrust-fuzz-campaign-1785567297-1fa3-
// 54439 @ 8eb0b002e3: 4,855 divergences, all one class, all NaN-vs-NaN;
// RATIFIED Michael 2026-08-01 — non-surface, do NOT re-open. The load-bearing
// argument: C disagrees with ITSELF — the verbatim C body compiled with clang
// -O2 -ffp-contract=off returns the Rust bit pattern while the fleet gcc
// returns the other, so there is no single C answer to match and the NaN
// sign/payload is not a PostgreSQL surface. Precedent format: multirange
// numeric tie-representative (RATIFIED 2026-07-31) and the build.rs
// FP-CONTRACTION CARVE 2026-07-30. Post-carve fleet CONFIRM GREEN: job
// pgrust-fuzz-campaign-1785599625-787d-28826 @ 94762ffd59 — 10,604,155
// execs, corpus resumed at 2,224 inputs including the divergence-bearing
// NaN corpus, 0 divergences, 0 sanitizer artifacts, cov_lines 1057, rc=0.)
// ---------------------------------------------------------------------------
//
// Bit-exact comparison is the DEFAULT and stays mandatory for every value
// that is not a NaN on BOTH sides — including infinities, signed zeros, and
// every finite value. The carve is exactly `r.is_nan() && c.is_nan()`.
//
// Why NaN bits are a non-surface here: geo_distance_internal's Rust body is
// operation-for-operation identical to the C body and the C oracle is built
// -ffp-contract=off, so both sides execute the same abstract op sequence.
// When a NaN payload enters (raw-bit fuzz inputs), the surviving payload and
// sign are decided by (a) which operand of a commutative FADD/FMUL the
// compiler emits first (aarch64 ProcessNaNs propagates op1's NaN; LLVM and
// gcc canonicalize commutative operand order differently) and (b) whether
// the surviving copy passed through fabs (the only sign-clearing op). IEEE
// 754 leaves NaN sign/payload propagation unspecified and C imposes nothing:
// the SAME verbatim C compiled with clang -O2 -ffp-contract=off (macOS
// aarch64) returns 0x7FFF494C41495204 for artifact crash-004eebee... —
// agreeing with pgrust and DISAGREEING with the fleet gcc build's
// 0xFFFF494C41495204. C Postgres disagrees with itself across compilers, so
// the NaN bit pattern is not a PostgreSQL surface.
//
// Why the carve is not a mask (see tests::geo_nan_carve_narrowness):
//   * NaN-vs-finite in EITHER direction stays a hard divergence — the carve
//     requires BOTH sides NaN, so a wrongly-produced (or wrongly-absent) NaN
//     still fails.
//   * Every finite bit difference (down to 1 ulp) and signed-zero difference
//     still fails: the comparison is bit-exact outside the carve.
//   * NaN-in => NaN-out on both sides is structural (every op propagates
//     NaN; both branch comparisons are false-on-NaN in both languages), so
//     the carve cannot hide a control-flow divergence.
fn geo_bits_match(r: f64, c: f64) -> bool {
    r.to_bits() == c.to_bits() || (r.is_nan() && c.is_nan())
}

fn check_geo_distance(x1: f64, y1: f64, x2: f64, y2: f64) {
    use types_core::geo::Point;
    let p1 = Point { x: x1, y: y1 };
    let p2 = Point { x: x2, y: y2 };
    let r = contrib_earthdistance::geo_distance_internal(&p1, &p2);
    let c = unsafe { pg_mf_geo_distance(x1, y1, x2, y2) };
    assert!(
        geo_bits_match(r, c),
        "geo_distance_internal({x1:?},{y1:?},{x2:?},{y2:?}): rust {:#018x} c {:#018x}",
        r.to_bits(),
        c.to_bits()
    );

    // fc plane: the dfmgr-registered wrapper on a LocalFcinfo frame.
    earthdistance_setup();
    // lookup-miss arm of the crate's dfmgr lookup table.
    assert!(matches!(
        dfmgr::load_external_function("earthdistance", "no_such_function", false),
        Ok(None)
    ));
    let f = dfmgr::load_external_function("earthdistance", "geo_distance", true)
        .expect("earthdistance library registered")
        .expect("geo_distance resolves");
    let a1: [u8; 16] = {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&x1.to_ne_bytes());
        b[8..].copy_from_slice(&y1.to_ne_bytes());
        b
    };
    let a2: [u8; 16] = {
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&x2.to_ne_bytes());
        b[8..].copy_from_slice(&y2.to_ne_bytes());
        b
    };
    let mut fcinfo = types_fmgr::LocalFcinfo::<2>::new(0);
    fcinfo.set_arg(0, datum::Datum::from_usize(a1.as_ptr() as usize));
    fcinfo.set_arg(1, datum::Datum::from_usize(a2.as_ptr() as usize));
    let d = f(None, &mut fcinfo).expect("fc_geo_distance never errors");
    assert_eq!(d.as_f64().to_bits(), r.to_bits(), "fc_geo_distance wrapper plane");
}

// ---------------- arm 5: pg_rusage ----------------

/// Fold a raw u64 into the real-clock domain: sec in [0, 2^42), usec in
/// [0, 1_000_000) — see DOMAIN CARVES.
fn fold_snapshot(r: &mut Rdr) -> [i64; 6] {
    let mut out = [0i64; 6];
    for i in 0..3 {
        out[2 * i] = (r.u64() & ((1 << 42) - 1)) as i64;
        out[2 * i + 1] = (r.u32() % 1_000_000) as i64;
    }
    out
}

fn check_rusage(ru0f: [i64; 6], ru1f: [i64; 6]) {
    let mut cbuf = [0u8; 100];
    unsafe { pg_mf_rusage_show(ru0f.as_ptr(), ru1f.as_ptr(), cbuf.as_mut_ptr().cast()) };
    let cstr = unsafe { std::ffi::CStr::from_ptr(cbuf.as_ptr().cast()) }.to_bytes();

    let mk = |f: [i64; 6]| pg_rusage::PgRUsage {
        tv_sec: f[0],
        tv_usec: f[1],
        ru_utime_sec: f[2],
        ru_utime_usec: f[3],
        ru_stime_sec: f[4],
        ru_stime_usec: f[5],
    };
    let shown = pg_rusage::pg_rusage_show_delta(&mk(ru0f), mk(ru1f));
    assert_eq!(
        shown.as_str().as_bytes(),
        cstr,
        "pg_rusage_show_delta({ru0f:?}, {ru1f:?})"
    );

    // Live clock-read leg (pg_rusage_init + the pg_rusage_show wrapper):
    // no-panic + shape only — the OS getrusage/gettimeofday values are
    // nondeterministic, so the value plane is owned by show_delta above
    // (the C oracle carves the same seam via its fixture pg_rusage_init).
    let live0 = pg_rusage::pg_rusage_init();
    let live = pg_rusage::pg_rusage_show(&live0);
    assert!(live.as_str().starts_with("CPU: user: "), "{}", live.as_str());
}

// ---------------- arm 6: xlogstats ----------------

fn run_xlogstats(r: &mut Rdr) {
    xlogstats::init_seams(); // no-op body, executed for the record
    unsafe { pg_mf_xlog_reset() };
    let mut stats = Box::new(xlogstats::XLogStats::ZEROED);
    let nrec = (r.u8() % 4) + 1;
    for _ in 0..nrec {
        let rmid = r.u8();
        let info = r.u8();
        let max_block_id = (r.u8() as i32 % 34) - 1; // -1..=32
        let mut in_use = [0u8; XLR_MAX_BLOCK_ID + 1];
        let mut has_image = [0u8; XLR_MAX_BLOCK_ID + 1];
        let mut bimg_len = [0u16; XLR_MAX_BLOCK_ID + 1];
        let mut fpi_sum: u32 = 0;
        let mut rec = DecodedXLogRecord::default();
        rec.xl_rmid = rmid;
        rec.xl_info = info;
        rec.max_block_id = max_block_id as i8;
        for i in 0..=max_block_id.max(-1) {
            if i < 0 {
                break;
            }
            let i = i as usize;
            let flags = r.u8();
            in_use[i] = flags & 1;
            has_image[i] = (flags >> 1) & 1;
            bimg_len[i] = r.u16();
            let mut blk = DecodedBkpBlock::EMPTY;
            blk.in_use = in_use[i] != 0;
            blk.has_image = has_image[i] != 0;
            blk.bimg_len = bimg_len[i];
            rec.blocks[i] = blk;
            if in_use[i] != 0 && has_image[i] != 0 {
                fpi_sum += bimg_len[i] as u32;
            }
        }
        // Fixture validity: tot_len >= fpi (decoded-record invariant).
        let tot_len = fpi_sum + (r.u32() % (1 << 20));
        rec.xl_tot_len = tot_len;

        let reader = XLogReaderState { record: Some(rec), ..Default::default() };
        let (r_rec_len, r_fpi_len) = xlogstats::XLogRecGetLen(&reader);
        xlogstats::XLogRecStoreStats(&mut stats, &reader);

        let (mut c_rec_len, mut c_fpi_len) = (0u32, 0u32);
        unsafe {
            pg_mf_xlog_store(
                rmid,
                info,
                tot_len,
                max_block_id,
                in_use.as_ptr(),
                has_image.as_ptr(),
                bimg_len.as_ptr(),
                &mut c_rec_len,
                &mut c_fpi_len,
            )
        };
        assert_eq!((r_rec_len, r_fpi_len), (c_rec_len, c_fpi_len), "XLogRecGetLen rmid {rmid}");

        // recid per C's ground truth (RM_XACT_ID == 1, rmgrlist.h order).
        let mut recid = info >> 4;
        if rmid == 1 {
            recid &= 0x07;
        }
        let (mut c_rmgr, mut c_rec) = ([0u64; 3], [0u64; 3]);
        unsafe {
            pg_mf_xlog_cell(rmid as i32, recid as i32, c_rmgr.as_mut_ptr(), c_rec.as_mut_ptr())
        };
        let rs = &stats.rmgr_stats[rmid as usize];
        assert_eq!([rs.count, rs.rec_len, rs.fpi_len], c_rmgr, "rmgr cell {rmid}");
        let rr = &stats.record_stats[rmid as usize][recid as usize];
        assert_eq!([rr.count, rr.rec_len, rr.fpi_len], c_rec, "record cell {rmid}/{recid}");
        assert_eq!(stats.count, unsafe { pg_mf_xlog_count() }, "stats.count");
    }
}

// ---------------- arm 7: stringinfo ----------------

const SI_APPEND_CAP: usize = 1 << 16;

fn si_err_class(e: &types_error::PgError) -> i32 {
    if e.sqlstate == types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        C_ERR_PROGRAM_LIMIT
    } else {
        99
    }
}

fn run_stringinfo(r: &mut Rdr) {
    let ctx = mcx::MemoryContext::new("miscfam_si");
    let m = ctx.mcx();

    // NUL at data[len] is transient after the NT-style ops (both sides).
    let mut nul_valid;

    let variant = r.u8() % 3;
    let mut si = match variant {
        0 => {
            let cst = unsafe { pg_mf_si_init_default() };
            assert_eq!(cst, 0, "initStringInfo errored");
            nul_valid = true;
            stringinfo::StringInfo::new_in(m).expect("new_in(1024)")
        }
        1 => {
            let initsize = (r.u16() as usize % 8192) + 1; // 1..=8192
            let cst = unsafe { pg_mf_si_init_ext(initsize as i32) };
            assert_eq!(cst, 0, "initStringInfoExt errored");
            nul_valid = true;
            stringinfo::StringInfo::with_capacity_in(m, initsize).expect("with_capacity_in")
        }
        _ => {
            // pgrust-only constructor: postconditions asserted here, then
            // the C side is aligned to the resulting (len, capacity).
            let n = r.u8() as usize;
            let seed = r.bytes(n);
            let mut v = mcx::PgVec::new_in(m);
            v.try_reserve_exact(n.max(1)).expect("seed vec");
            v.extend_from_slice(seed);
            let take = v.len();
            let si = stringinfo::StringInfo::from_vec(v).expect("from_vec");
            assert_eq!(si.len(), take, "from_vec keeps len");
            assert!(si.capacity() > si.len(), "from_vec capacity invariant");
            assert_eq!(si.as_bytes(), seed, "from_vec keeps bytes");
            // C alignment: same capacity, same contents.
            let cst = unsafe { pg_mf_si_init_ext(si.capacity() as i32) };
            assert_eq!(cst, 0);
            if !seed.is_empty() {
                let cst = unsafe { pg_mf_si_append_bin(seed.as_ptr(), seed.len() as i32) };
                assert_eq!(cst, 0);
            }
            nul_valid = true;
            si
        }
    };

    let mut total: usize = si.len();

    macro_rules! compare_state {
        ($op:expr) => {{
            let (mut clen, mut cmax) = (0i32, 0i32);
            let cdata = unsafe { pg_mf_si_get(&mut clen, &mut cmax) };
            assert_eq!(si.len(), clen as usize, "len after {}", $op);
            assert_eq!(si.capacity(), cmax as usize, "capacity after {}", $op);
            let take = if nul_valid { clen as usize + 1 } else { clen as usize };
            let cbytes = unsafe { std::slice::from_raw_parts(cdata.cast::<u8>(), take) };
            let rbytes =
                unsafe { std::slice::from_raw_parts(si.as_bytes().as_ptr(), take) };
            assert_eq!(rbytes, cbytes, "bytes after {}", $op);
        }};
    }

    compare_state!("init");
    assert_eq!(si.is_empty(), si.len() == 0, "is_empty/len coherence");
    // allocator()/mcx() both hand back the construction context.
    let _ = si.allocator();
    let _ = si.mcx();

    while !r.done() && total <= SI_APPEND_CAP {
        let op = r.u8() % 11;
        match op {
            0 | 1 => {
                let n = r.u16() as usize % 2048;
                let chunk = r.bytes(n).to_vec();
                total += chunk.len();
                let (rres, cst, name) = if op == 0 {
                    (si.append_bytes(&chunk), unsafe {
                        pg_mf_si_append_bin(chunk.as_ptr(), chunk.len() as i32)
                    }, "append_bytes")
                } else {
                    (si.append_bytes_nt(&chunk), unsafe {
                        pg_mf_si_append_bin_nt(chunk.as_ptr(), chunk.len() as i32)
                    }, "append_bytes_nt")
                };
                assert!(rres.is_ok() && cst == 0, "{name} unexpectedly errored");
                nul_valid = op == 0;
                compare_state!(name);
            }
            2 => {
                let ch = r.u8();
                let rres = si.append_byte(ch);
                let cst = unsafe { pg_mf_si_append_char(ch) };
                assert!(rres.is_ok() && cst == 0, "append_byte errored");
                total += 1;
                nul_valid = true;
                compare_state!("append_byte");
            }
            3 => {
                let count = r.u16() as usize % 4096; // 0 arm included
                let rres = si.append_spaces(count);
                let cst = unsafe { pg_mf_si_append_spaces(count as i32) };
                assert!(rres.is_ok() && cst == 0, "append_spaces errored");
                total += count;
                if count > 0 {
                    nul_valid = true;
                }
                compare_state!("append_spaces");
            }
            4 => {
                let n = r.u16() as usize % 512;
                let raw = r.bytes(n);
                let s: String = String::from_utf8_lossy(raw).replace('\0', " ");
                total += s.len();
                let rres = si.append_str(&s);
                let cs = CString::new(s.clone()).expect("NUL-free by construction");
                let cst = unsafe { pg_mf_si_append_string(cs.as_ptr()) };
                assert!(rres.is_ok() && cst == 0, "append_str errored");
                nul_valid = true;
                compare_state!("append_str");
            }
            5 => {
                si.reset();
                let cst = unsafe { pg_mf_si_reset() };
                assert_eq!(cst, 0, "reset errored");
                nul_valid = true;
                compare_state!("reset");
            }
            6 => {
                let raw = r.u16();
                let (needed, expect_err) = if raw == 0xACE1 && si.len() < 4096 {
                    // Rare gated zone (magic value + seed): force the
                    // doubling loop to overshoot MaxAllocSize so the clamp
                    // arm executes (stringinfo lib.rs:104 == C
                    // enlargeStringInfo's clamp). needed = MaxAllocSize/2
                    // + 1 makes 2^k jump straight past MaxAllocSize; the
                    // resulting ~1 GiB reserve is lazy (pages untouched:
                    // only len+1 bytes are ever written/compared).
                    (mcx::MAX_ALLOC_SIZE / 2 + 1, false)
                } else if raw & 1 == 1 && si.len() >= 64 {
                    // 54000 zone: needed >= MaxAllocSize - len always holds
                    // (len >= 64 > k), boundary margin sweeps through 0.
                    (mcx::MAX_ALLOC_SIZE - (raw >> 1) as usize % 64, true)
                } else {
                    ((raw >> 1) as usize % 8192, false)
                };
                let rres = si.enlarge(needed);
                let cst = unsafe { pg_mf_si_enlarge(needed as i32) };
                if expect_err {
                    let e = rres.expect_err("enlarge past MaxAllocSize must error");
                    assert_eq!(si_err_class(&e), cst, "enlarge error class");
                    assert_eq!(cst, C_ERR_PROGRAM_LIMIT, "enlarge error class is 54000");
                } else {
                    assert!(rres.is_ok() && cst == 0, "enlarge errored");
                }
                compare_state!("enlarge");
            }
            7 => {
                // Includes the > len no-op arm.
                let newlen = r.u32() as usize % (si.len() + 2);
                si.truncate(newlen);
                unsafe { pg_mf_si_truncate(newlen as i32) };
                compare_state!("truncate");
            }
            8 => {
                // pq_writeintN shape: precondition capacity - len >= N on
                // both sides (identical by the capacity plane).
                if si.capacity() - si.len() >= 4 {
                    let b = [r.u8(), r.u8(), r.u8(), r.u8()];
                    si.write_fixed::<4>(b);
                    let cst = unsafe { pg_mf_si_write_fixed(b.as_ptr(), 4) };
                    assert_eq!(cst, 0);
                    total += 4;
                    nul_valid = false;
                    compare_state!("write_fixed");
                }
            }
            9 => {
                // append_bytes_z == appendBinaryStringInfoNT(s, len+1) with
                // the source's own NUL (cmdtag.c-style usage).
                let n = r.u16() as usize % 512;
                let mut chunk = r.bytes(n).to_vec();
                total += chunk.len() + 1;
                let rres = si.append_bytes_z(&chunk);
                chunk.push(0);
                let cst =
                    unsafe { pg_mf_si_append_bin_nt(chunk.as_ptr(), chunk.len() as i32) };
                assert!(rres.is_ok() && cst == 0, "append_bytes_z errored");
                // len now counts the NUL; data[len] is past it (garbage).
                nul_valid = false;
                compare_state!("append_bytes_z");
            }
            _ => {
                // is_empty tracks len through mutation.
                assert_eq!(si.is_empty(), si.len() == 0, "is_empty after ops");
            }
        }
    }

    // into_vec teardown: the buffer hand-off keeps length and bytes.
    let final_len = si.len();
    let v = si.into_vec();
    assert_eq!(v.len(), final_len, "into_vec keeps len");
}

// ---------------- entry ----------------

pub fn miscfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let mut r = Rdr::new(payload);
    match sel % 8 {
        0 => {
            check_init_qc();
            let tag = (r.u16() as u32 % NTAGS) as i32;
            check_cmdtag_props(tag);
        }
        1 => {
            let name = r.bytes(payload.len());
            check_cmdtag_enum(name);
        }
        2 => {
            let tag = (r.u16() as u32 % NTAGS) as i32;
            let nprocessed = r.u64();
            let nameonly = r.u8() & 1 == 1;
            check_build_qc(tag, nprocessed, nameonly);
        }
        3 => {
            let relkind = r.u8();
            check_relkind(relkind);
        }
        4 => {
            let (x1, y1) = (f64::from_bits(r.u64()), f64::from_bits(r.u64()));
            let (x2, y2) = (f64::from_bits(r.u64()), f64::from_bits(r.u64()));
            check_geo_distance(x1, y1, x2, y2);
        }
        5 => {
            let ru0 = fold_snapshot(&mut r);
            let ru1 = fold_snapshot(&mut r);
            check_rusage(ru0, ru1);
        }
        6 => run_xlogstats(&mut r),
        _ => run_stringinfo(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EXHAUSTIVE-DIFF (a0): the full u8 relkind domain against the vendored
    /// C oracle — every value compared, loop coverage asserted.
    #[test]
    fn pg_class_exhaustive() {
        let _serial = crate::c_oracle_serial();
        let mut covered = 0u32;
        for relkind in 0u8..=255 {
            check_relkind(relkind);
            covered += 1;
        }
        assert_eq!(covered, 256, "exhaustive sweep must visit all 256 relkinds");
    }

    /// EXHAUSTIVE-DIFF: every CommandTag row (name/len/flags), the
    /// name->enum roundtrip (exact + case-flipped), and both
    /// BuildQueryCompletionString modes for every tag.
    #[test]
    fn cmdtag_exhaustive() {
        let _serial = crate::c_oracle_serial();
        let mut covered = 0u32;
        for tag in 0..NTAGS as i32 {
            check_cmdtag_props(tag);
            let name = cmdtag::GetCommandTagName(CommandTag(tag));
            check_cmdtag_enum(name.as_bytes());
            let lower = name.to_ascii_lowercase();
            check_cmdtag_enum(lower.as_bytes());
            for n in [0u64, 1, 9, 10, 12345, u64::MAX] {
                check_build_qc(tag, n, false);
                check_build_qc(tag, n, true);
            }
            covered += 1;
        }
        assert_eq!(covered, NTAGS, "exhaustive sweep must visit all tags");
        check_cmdtag_enum(b"");
        check_cmdtag_enum(b"NOT A TAG");
        check_cmdtag_enum(b"SELEC");
        check_cmdtag_enum(b"SELECTS");
        check_init_qc();
    }

    /// NaN-BITS CARVE narrowness proof (lane p1-nanadj 2026-08-01).
    ///
    /// (1) Replays fleet artifact crash-004eebee328bc0acd20886abbc42cac0
    /// 8119cc3c (campaign pgrust-fuzz-campaign-1785567297-1fa3-54439): y1 =
    /// 0xFFFF494C41495204 (negative quiet NaN, payload from raw fuzz bytes
    /// "RIALI"); fleet rust returned 0x7FFF494C41495204 vs fleet gcc C
    /// 0xFFFF494C41495204 — sign-of-NaN only. Must pass under the carve.
    ///
    /// (2) INJECTION PROOF: a real finite-value defect — a deliberately
    /// broken Rust geo_distance (EARTH_RADIUS off in the last decimal)
    /// compared against the REAL C oracle through the SAME carved
    /// comparator — is still detected, as are 1-ulp perturbations of the C
    /// result, NaN-vs-finite in both directions, and signed-zero
    /// differences. The carve is exactly both-sides-NaN and nothing wider.
    #[test]
    fn geo_nan_carve_narrowness() {
        let _serial = crate::c_oracle_serial();
        // (1) fleet artifact replay: both sides NaN => carved, no panic.
        // (Exercises the real divergence class end-to-end, incl. fc plane.)
        check_geo_distance(
            2.56754520541733e-289,
            f64::from_bits(0xFFFF494C41495204),
            8.49644828181543e-275,
            1.2241677834226036e-250,
        );
        // NaN in each coordinate position, sign/payload varied.
        for bits in [0x7FF8000000000001u64, 0xFFF0000000000204, 0x7FF9424142430204] {
            let n = f64::from_bits(bits);
            check_geo_distance(n, 51.508, 2.3522, 48.8566);
            check_geo_distance(-0.1257, n, 2.3522, 48.8566);
            check_geo_distance(-0.1257, 51.508, n, 48.8566);
            check_geo_distance(-0.1257, 51.508, 2.3522, n);
        }

        // (2) injected finite-value defect vs the REAL C oracle.
        let (x1, y1, x2, y2) = (-0.1257f64, 51.508, 2.3522, 48.8566);
        let c = unsafe { pg_mf_geo_distance(x1, y1, x2, y2) };
        assert!(c.is_finite(), "sanity: London-Paris distance is finite");
        // defective port: EARTH_RADIUS transcribed 3958.747717 (real C
        // constant is 3958.747716) — the classic finite-value port defect.
        let defective = {
            let degtorad = |d: f64| (d / 360.0) * (2.0 * std::f64::consts::PI);
            let (long1, lat1, long2, lat2) =
                (degtorad(x1), degtorad(y1), degtorad(x2), degtorad(y2));
            let mut longdiff = (long1 - long2).abs();
            if longdiff > std::f64::consts::PI {
                longdiff = 2.0 * std::f64::consts::PI - longdiff;
            }
            let half_lat = (lat1 - lat2).abs() / 2.0;
            let mut sino = (half_lat.sin() * half_lat.sin()
                + lat1.cos() * lat2.cos() * (longdiff / 2.0).sin() * (longdiff / 2.0).sin())
            .sqrt();
            if sino > 1.0 {
                sino = 1.0;
            }
            2.0 * 3958.747717 * sino.asin()
        };
        assert!(
            !geo_bits_match(defective, c),
            "carve MUST NOT mask an injected finite-value defect"
        );
        // 1-ulp finite perturbation still detected.
        assert!(
            !geo_bits_match(f64::from_bits(c.to_bits() ^ 1), c),
            "carve MUST NOT mask a 1-ulp finite difference"
        );
        // NaN-vs-finite is a hard divergence in BOTH directions.
        assert!(!geo_bits_match(f64::NAN, c), "rust-NaN vs C-finite must fail");
        assert!(!geo_bits_match(c, f64::NAN), "rust-finite vs C-NaN must fail");
        // Signed zero and infinities stay bit-exact (carve is NaN-only).
        assert!(!geo_bits_match(0.0, -0.0), "signed zero stays bit-exact");
        assert!(!geo_bits_match(f64::INFINITY, f64::NEG_INFINITY));
        // And the carve itself: distinct NaN bit patterns are accepted.
        assert!(geo_bits_match(
            f64::from_bits(0x7FFF494C41495204),
            f64::from_bits(0xFFFF494C41495204)
        ));
    }

    /// Deterministic seeds through every arm (also a smoke for the planes).
    #[test]
    fn arm_smoke() {
        // cmdtag props/enum/build
        miscfam_diff(&[0, 7, 0]);
        miscfam_diff(b"\x01SELECT");
        miscfam_diff(&[2, 158, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0]);
        // pg_class
        miscfam_diff(&[3, b'r']);
        miscfam_diff(&[3, b'z']);
        // earthdistance: two real cities + a wrap pair
        let mut v = vec![4u8];
        for f in [-0.1257f64, 51.508, 2.3522, 48.8566] {
            v.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        miscfam_diff(&v);
        // pg_rusage: borrow arms
        let mut v = vec![5u8];
        for _ in 0..3 {
            v.extend_from_slice(&10u64.to_le_bytes());
            v.extend_from_slice(&999_999u32.to_le_bytes());
        }
        for _ in 0..3 {
            v.extend_from_slice(&11u64.to_le_bytes());
            v.extend_from_slice(&3u32.to_le_bytes());
        }
        miscfam_diff(&v);
        // xlogstats: one XACT record with an imaged block
        miscfam_diff(&[6, 0, 1, 0x85, 1, 3, 0x00, 0x20, 64, 0, 0, 0]);
        // stringinfo: default init + a few appends + enlarge error zone
        let mut v = vec![7u8, 0];
        v.push(0);
        v.extend_from_slice(&5u16.to_le_bytes());
        v.extend_from_slice(b"hello");
        v.push(2);
        v.push(b'!');
        miscfam_diff(&v);
    }
}
