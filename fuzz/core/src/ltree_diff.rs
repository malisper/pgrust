//! ltree_diff: differential fuzz driver — shipped Rust `contrib/ltree` vs
//! verbatim vendored PostgreSQL 18.3 C (csrc/pg_ltreefam_io.c, Stamp-18.3,
//! upstream sha 62d6c7d3df; lane p1-ltree-t74, task #74).
//!
//! Selector = data[0] % 8:
//!   0/1/2  in/out/send family — ltree_in / lquery_in / ltxtq_in over
//!          NUL-free cstring bytes, hard AND soft (ErrorSaveNode vs the C
//!          escontext shim) modes per a flag byte; verdict + exact sqlstate
//!          + image bytes compared; on success the SAME image drives
//!          *_out (cstring) and *_send (wire bytes) on both sides, and the
//!          send wire round-trips through *_recv on both sides.
//!   3      recv raw — ltree_recv / lquery_recv / ltxtq_recv over raw wire
//!          bytes (version byte included, malformed input first-class).
//!   4      two-ltree ops — cmp + all six comparators, isparent/risparent,
//!          addltree, lca(2), hash_ltree, hash_ltree_extended(seed),
//!          nlevel, ltree2text, ltree_index (2- and 3-arg), subltree,
//!          subpath (2- and 3-arg), text2ltree, ltree_addtext /
//!          ltree_textadd (text arg from raw payload).
//!   5      matching — ltree vs lquery (ltq_regex/ltq_rregex) or ltree vs
//!          ltxtquery (ltxtq_exec/ltxtq_rexec) per a flag bit.
//!   6      arrays — up to 4 parsed ltrees packed into a driver-built 1-D
//!          ltree[] image (same bytes to BOTH sides, afx precedent; a flag
//!          drives the ndim=2 and null-bitmap reject arms): _ltree_isparent,
//!          _ltree_risparent, _ltq_regex, _ltxtq_exec, lt_q_regex +
//!          _lt_q_regex over a driver-built lquery[], the four _extract
//!          variants (image-or-NULL plane), _lca, and the r-variants.
//!   7      crc — ltree_crc32_sz over the raw payload bytes (LOWER_NODE
//!          fold), value plane u32. The cfg(test) sweep below closes the
//!          full 1-byte and 2-byte domains exhaustively (a0 candidate from
//!          the lane FLEET-QUEUE).
//!
//! Comparison planes: value bytes (type images, out cstrings, send wire,
//! text payloads) + error verdict + EXACT sqlstate (both sides carry the
//! real MAKE_SQLSTATE word) + soft-error-occurred flag + the
//! c-escontext-branch-executed witness (pg_lt_soft_fired > 0 whenever the
//! C side reports a soft error — vacuity rule) + no-panic.
//!
//! CAPACITY CARVE (claim-row frame-budget gap, recorded not carved-silent):
//! the recursive parsers/matchers are stack-guarded on both sides, but
//! pgrust frames are ~5x larger (measured 3741 vs 18665 at max_stack_depth
//! 2048kB), so in the gap band ONE side raises 54001 where the other
//! succeeds. Any exec where either side raises 54001 skips the value
//! comparison; everything else about the exec still asserts. Counted in
//! CAPACITY_CARVES.
//!
//! Every carve below keeps a counter, and `tests::carve_counters_are_not_dead`
//! READS them: it drives one input per carve and fails if the counter did not
//! move. Until that test existed the counters were incremented into the void,
//! which is the dead-counter class — a carve nobody can observe is how a bound
//! silently widens until it swallows the band it was meant to trim.
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - arms 0-2/4-6: parser inputs are NUL-free (cstring contract).
//!   - arm 6: array images are driver-built and well-formed apart from the
//!     deliberate ndim/null reject shapes; corrupt array headers belong to
//!     arrayfuncs_diff. Packed ltree/lquery element images come from the
//!     agreed parse of the SAME exec (repr.rs packed-reader hardening is
//!     the claim row's open item 3, out of this target's scope).
//!   - encoding pinned UTF8, ctype/collation pinned C on both sides (the
//!     docker probe-DB pin, dead-lane finding 4).
//!
//! CARVED OUT (claim row): gist.rs (index AM), ltreeparentsel (planner).

#![allow(dead_code)]

use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Once;

use datum::Datum;
use types_fmgr::{ErrorSaveNode, PGFunction};

extern "C" {
    fn pg_lt_reset();
    fn pg_lt_sqlstate() -> c_int;
    fn pg_lt_soft_sqlstate() -> c_int;
    fn pg_lt_soft_fired() -> c_int;
    fn pg_lt_in(
        which: c_int,
        s: *const c_char,
        soft: c_int,
        img: *mut *const u8,
        len: *mut c_int,
    ) -> c_int;
    fn pg_lt_out(which: c_int, img: *const u8, out: *mut *const c_char) -> c_int;
    fn pg_lt_send(which: c_int, img: *const u8, out: *mut *const u8, len: *mut c_int) -> c_int;
    fn pg_lt_recv(
        which: c_int,
        wire: *const u8,
        wirelen: c_int,
        img: *mut *const u8,
        len: *mut c_int,
    ) -> c_int;
    fn pg_lt_cmp(a: *const u8, b: *const u8, cmp: *mut i32, bools: *mut u8) -> c_int;
    fn pg_lt_isparent(rev: c_int, a: *const u8, b: *const u8, out: *mut c_int) -> c_int;
    fn pg_lt_hash(a: *const u8, out: *mut u32) -> c_int;
    fn pg_lt_hash_ext(a: *const u8, seed: u64, out: *mut u64) -> c_int;
    fn pg_lt_nlevel(a: *const u8, out: *mut i32) -> c_int;
    fn pg_lt_addltree(a: *const u8, b: *const u8, img: *mut *const u8, len: *mut c_int) -> c_int;
    fn pg_lt_addtext(
        which: c_int,
        a: *const u8,
        txt: *const u8,
        txtlen: c_int,
        img: *mut *const u8,
        len: *mut c_int,
    ) -> c_int;
    fn pg_lt_text2ltree(txt: *const u8, txtlen: c_int, img: *mut *const u8, len: *mut c_int)
        -> c_int;
    fn pg_lt_ltree2text(a: *const u8, payload: *mut *const u8, len: *mut c_int) -> c_int;
    fn pg_lt_subltree(
        a: *const u8,
        s: i32,
        e: i32,
        img: *mut *const u8,
        len: *mut c_int,
    ) -> c_int;
    fn pg_lt_subpath(
        a: *const u8,
        s: i32,
        l: i32,
        nargs: c_int,
        img: *mut *const u8,
        len: *mut c_int,
    ) -> c_int;
    fn pg_lt_index(
        a: *const u8,
        b: *const u8,
        start: i32,
        nargs: c_int,
        out: *mut i32,
    ) -> c_int;
    fn pg_lt_lca2(
        a: *const u8,
        b: *const u8,
        img: *mut *const u8,
        len: *mut c_int,
        isnull: *mut c_int,
    ) -> c_int;
    fn pg_lt_match(which: c_int, l: *const u8, r: *const u8, out: *mut c_int) -> c_int;
    fn pg_lt_arr(
        which: c_int,
        arr: *const u8,
        rhs: *const u8,
        bout: *mut c_int,
        img: *mut *const u8,
        len: *mut c_int,
        isnull: *mut c_int,
    ) -> c_int;
    fn pg_lt_crc(buf: *const u8, len: c_int, out: *mut u32) -> c_int;
    /// csrc/pg_wcharfam.c: SetDatabaseEncoding's assignment for the verbatim
    /// wfam_ mblen walkers the oracle resolves against (default is
    /// SQL_ASCII, i.e. single-byte — leaving it unset made the C side see
    /// every multibyte label as bytes and disagree on encoding errors).
    fn wfam_x_set_db_encoding(encoding: c_int);
}

const SQLSTATE_54001: i32 = sqlstate_word(b"54001");

const fn sqlstate_word(s: &[u8; 5]) -> i32 {
    // MAKE_SQLSTATE (elog.h)
    (((s[0] - b'0') as i32) & 0x3f)
        + ((((s[1] - b'0') as i32) & 0x3f) << 6)
        + ((((s[2] - b'0') as i32) & 0x3f) << 12)
        + ((((s[3] - b'0') as i32) & 0x3f) << 18)
        + ((((s[4] - b'0') as i32) & 0x3f) << 24)
}

static CAPACITY_CARVES: AtomicU64 = AtomicU64::new(0);

// ---------------- plumbing (hstorefam precedent) ----------------

fn init_env() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(ltree::init_seams);
        // the shipped mblen/encoding + case-fold seams the crate's io/crc
        // paths dispatch through (tolerate another module owning them in a
        // shared test binary, netfam precedent)
        let _ = std::panic::catch_unwind(mbutils::init_seams);
        let _ = std::panic::catch_unwind(adt_formatting::init_seams);
        stack_depth_core::assign_max_stack_depth(2048);
    });
    // FLEET-QUEUE mandate: arm the Rust-side stack guard per worker thread —
    // stack_is_too_deep() short-circuits on base==0 and the guard is INERT
    // without this (a campaign without it is INCOMPLETE).
    thread_local! {
        static THREAD_ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    THREAD_ARMED.with(|armed| {
        if !armed.get() {
            let _ = stack_depth_core::set_stack_base();
            // C-ctype/C-collation pin (thread-local defaults; matches the C
            // oracle's { ctype_is_c = true } locale — dead-lane finding 4)
            pg_locale::set_default_locale_c_for_tests();
            pg_locale::set_database_ctype_is_c(true);
            armed.set(true);
        }
    });
}

fn pin_encoding() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).expect("PG_UTF8 valid");
    let _ = mbutils::SetClientEncoding(wchar::PG_UTF8);
    // SAME pin on the C side (thread-local there too).
    unsafe { wfam_x_set_db_encoding(wchar::PG_UTF8 as c_int) };
}

fn fc(name: &str) -> PGFunction {
    dfmgr::load_external_function("ltree", name, true)
        .expect("ltree library registered")
        .unwrap_or_else(|| panic!("ltree fn {name} resolves"))
}

fn run_fc<const N: usize>(
    f: PGFunction,
    mcx: mcx::Mcx<'_>,
    args: &[(Datum, bool); N],
    esc: Option<&mut ErrorSaveNode>,
) -> (types_error::PgResult<Datum>, bool) {
    let mut fci = types_fmgr::LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `mcx` outlives this call.
    unsafe { fci.set_result_mcx(mcx) };
    for (i, (d, isnull)) in args.iter().enumerate() {
        if *isnull {
            fci.set_arg_null(i);
        } else {
            fci.set_arg(i, *d);
        }
    }
    if let Some(node) = esc {
        fci.context = node.fm_node_ptr();
    }
    let r = f(None, &mut fci);
    let isnull = fci.isnull;
    (r, isnull)
}

fn image_of<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live 4B-header varlena datum built by the fc call.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, total)
    }
}

fn cstring_of<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: a live NUL-terminated cstring datum built by the fc call.
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const c_char) }.to_bytes()
}

fn varlena_payload<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena datum built by the fc call.
    unsafe {
        let total = types_tuple::varatt::varsize_any(p);
        let hdr = if types_tuple::varatt::varatt_is_1b(p) { 1 } else { 4 };
        core::slice::from_raw_parts(p.add(hdr), total - hdr)
    }
}

/// Byte-plane equality with a TRIAGEABLE failure message: a wrap-band lquery
/// image is ~1.6 MB, and `assert_eq!` on two of those prints ~10 MB of decimal
/// per divergence — enough to bury the finding and the fleet log. Report the
/// lengths, the first differing offset, and a 24-byte window either side.
fn assert_bytes_eq(got: &[u8], want: &[u8], what: &str) {
    if got == want {
        return;
    }
    let at = got
        .iter()
        .zip(want.iter())
        .position(|(a, b)| a != b)
        .unwrap_or(got.len().min(want.len()));
    let lo = at.saturating_sub(8);
    let win = |b: &[u8]| -> String {
        let hi = (at + 24).min(b.len());
        format!("{:x?}", &b[lo.min(b.len())..hi])
    };
    panic!(
        "{what}: byte plane differs at offset {at} (rust len {}, C len {})\n  rust[{lo}..]: {}\n  C   [{lo}..]: {}",
        got.len(),
        want.len(),
        win(got),
        win(want),
    );
}

fn sqlstate_of(e: &types_error::PgError) -> i32 {
    e.sqlstate().0
}

fn is_capacity(state: i32) -> bool {
    state == SQLSTATE_54001
}

fn capacity_carve() {
    CAPACITY_CARVES.fetch_add(1, Ordering::Relaxed);
}

/// Build a plain 4B-header varlena (text) image over payload bytes.
fn text_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + 4;
    let mut v = Vec::with_capacity(total);
    v.extend_from_slice(&((total as u32) << 2).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

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
    fn i32(&mut self) -> i32 {
        i32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }
    fn u64(&mut self) -> u64 {
        let mut b = [0u8; 8];
        for s in &mut b {
            *s = self.u8();
        }
        u64::from_le_bytes(b)
    }
    fn rest(&mut self) -> &'a [u8] {
        let r = &self.d[self.pos.min(self.d.len())..];
        self.pos = self.d.len();
        r
    }
}

/// NUL-free cstring text, capped (deep-recursion seeds need room; the
/// stack guards on both sides carve past their limits).
fn cstr_text(raw: &[u8], cap: usize) -> Vec<u8> {
    raw.iter().copied().filter(|&b| b != 0).take(cap).collect()
}

/// Directed-seed expander: a payload starting 0xFE reads
/// `[0xFE][u16 n][u8 unit_len][unit bytes][tail...]` and yields the unit
/// repeated `n` times followed by the tail, capped at 300_000 bytes. This
/// lets a ~10-byte committed seed reach the label-/item-count and word-length
/// LIMIT arms (LTREE_MAX_LEVELS 65535, lquery item cap, ltxtquery 0xffff word
/// cap) that a literal input under the 16 KiB text cap cannot express. Both
/// sides always receive the SAME expanded text, so the C oracle is untouched.
/// Any other payload passes through unchanged.
fn expand_text(raw: &[u8]) -> Option<Vec<u8>> {
    const CAP: usize = 300_000;
    if raw.first() != Some(&0xFE) || raw.len() < 4 {
        return None;
    }
    let n = u16::from_le_bytes([raw[1], raw[2]]) as usize;
    let ul = raw[3] as usize;
    if raw.len() < 4 + ul || ul == 0 {
        return None;
    }
    let unit = &raw[4..4 + ul];
    let tail = &raw[4 + ul..];
    // ORACLE-SIDE RESOURCE BOUND (not a pgrust limit). Upstream 18.3
    // ltree_io.c parse_lquery counts `numOR` over the WHOLE string (never
    // reset per level, lines 273/299) and then palloc0()s
    // sizeof(nodeitem) * (numOR + 1) FOR EACH LEVEL (lines 321/328), so the C
    // oracle's peak allocation is O(levels x total-ORs) -- quadratic. A
    // mutated 300 KB `a.a|a|`-shaped input made the C side allocate 3.9 GB and
    // OOM-killed a fleet campaign (job -64906) while pgrust parsed the same
    // text in 1.8 ms with linear memory (crate-internal probe, 2026-08-01).
    // pgrust is NOT wrong here and no compared plane diverges -- it is an
    // upstream memory-amplification property, recorded in the lane report.
    // The expander exists to reach the u16 LIMIT arms, which need many '.'
    // and no '|', so bounding the product costs no reachability.
    const CAP_MIXED: usize = 8_192;
    let cap = if unit.contains(&b'|') && unit.contains(&b'.') {
        CAP_MIXED
    } else {
        CAP
    };
    let mut out = Vec::with_capacity((n * ul + tail.len()).min(cap));
    for _ in 0..n {
        if out.len() + ul > cap {
            break;
        }
        out.extend_from_slice(unit);
    }
    out.extend_from_slice(&tail[..tail.len().min(cap - out.len().min(cap))]);
    Some(out)
}

/// `cstr_text` with the expander applied: expanded payloads get the large
/// cap (they exist to cross the u16 limits), literal payloads keep `cap`.
fn cstr_text_x(raw: &[u8], cap: usize) -> Vec<u8> {
    match expand_text(raw) {
        Some(x) => cstr_text(&x, 300_000),
        None => cstr_text(raw, cap),
    }
}

// ---------------- C-result helper ----------------

struct COut {
    ret: i32,
    bytes: Vec<u8>,
    sqlstate: i32,
}

fn c_bytes(ret: i32, p: *const u8, len: i32) -> COut {
    let bytes = if ret == 0 {
        // SAFETY: arena pointer valid until the next pg_lt_reset.
        unsafe { core::slice::from_raw_parts(p, len as usize) }.to_vec()
    } else {
        Vec::new()
    };
    COut { ret, bytes, sqlstate: unsafe { pg_lt_sqlstate() } }
}

// ---------------- shared: both-sides *_in ----------------

const IN_NAMES: [&str; 3] = ["ltree_in", "lquery_in", "ltxtq_in"];
const OUT_NAMES: [&str; 3] = ["ltree_out", "lquery_out", "ltxtq_out"];
const SEND_NAMES: [&str; 3] = ["ltree_send", "lquery_send", "ltxtq_send"];
const RECV_NAMES: [&str; 3] = ["ltree_recv", "lquery_recv", "ltxtq_recv"];

/// Parse `text` as type `which` on both sides; assert verdict + sqlstate
/// (+ soft plane); return the agreed image on agreed success.
/// A 54001 on either side is the capacity carve: comparison skipped.
/// UPSTREAM u16-OVERFLOW CARVE (lquery only), decoded from fleet job -42152.
///
/// `lquery_level.numvar` is a **uint16** (contrib/ltree/ltree.h:92). C's
/// parse_lquery stores the per-level variant count into it with no ceiling
/// check (unlike `num`, which IS checked against LQUERY_MAX_LEVELS), so a
/// level carrying >= 65536 '|'-separated variants makes C WRAP the count and
/// emit a truncated image: the witness input (one level, 100,001 variants)
/// wrapped to 34,465 and produced 551,472 bytes where pgrust wrote all
/// 100,001 (1,600,048 bytes). C's own output is corrupt in that band —
/// `SELECT (repeat('a|',100000)||'a')::lquery` is the SQL-reachable shape.
///
/// RULED 2026-08-03 (R1 = YES, adopt the C-exact on-disk format): the port
/// now truncates `numvar` to the stored uint16 and walks to the next level by
/// `MAXALIGN(the stored totallen)` exactly as C does, so the STORED IMAGE
/// agrees byte-for-byte in the wrap band too and the `in` plane is NO LONGER
/// CARVED. What remains carved is only the DEPARSE path (`*_out`/`*_send`),
/// because there C writes past its own heap buffer - see the ASan witness
/// below. Narrowed from "skip the whole exec" to "skip out/send".
/// SECOND, WORSE WITNESS (fleet job -741, ASan): `lquery_level.totallen` is
/// **also uint16** (ltree.h:90) and `deparse_lquery` sizes its output buffer
/// from it —
///   `totallen += 1 + (curqlevel->numvar * 4) + curqlevel->totallen;`
///   `ptr = buf = (char *) palloc(totallen);`
/// — so once a single level's serialized length exceeds 65535 the stored
/// value WRAPS, the palloc is undersized, and the emit loop WRITES PAST THE
/// HEAP BUFFER. AddressSanitizer caught it directly: "heap-buffer-overflow,
/// WRITE of size 5, 0 bytes after 213607-byte region", from the SQL-reachable
/// literal shape `(repeat('a_g_|a',50000))::lquery`. That is an upstream
/// PostgreSQL 18.3 memory-safety defect in verbatim vendored C, not a pgrust
/// bug — pgrust does not wrap and deparses the level correctly, which is why
/// the out-text plane disagrees in this band.
///
/// Both fields wrap together, so ONE predicate carves both: a level whose
/// serialized size can reach 65536. Serialized size is bounded above by
/// LQL_HDRSIZE + variants * MAXALIGN(LVAR_HDRSIZE + name) + names, so the
/// over-approximation below carves slightly wider than strictly necessary
/// (deliberate: it must never let the C side scribble).
fn lquery_level_u16_wraps(text: &[u8]) -> bool {
    text.split(|&b| b == b'.').any(|lvl| {
        let variants = lvl.iter().filter(|&&b| b == b'|').count() + 1;
        variants >= 65535 || 8 + variants * 16 + lvl.len() >= 65536
    })
}

static U16_WRAP_CARVES: AtomicU64 = AtomicU64::new(0);
static AMPLIFICATION_CARVES: AtomicU64 = AtomicU64::new(0);
static INDEX_COST_CARVES: AtomicU64 = AtomicU64::new(0);

/// ORACLE-SIDE RESOURCE BOUND for `lquery_in`/`lquery_recv` (upstream memory
/// amplification, lane finding 3; docs/upstream/
/// bug-103b-ltree-parse-lquery-amplification.txt).
///
/// 18.3 `parse_lquery` counts `numOR` over the WHOLE string — never reset per
/// level — and then `palloc0`s `sizeof(nodeitem) * (numOR + 1)` FOR EACH
/// level, so the C oracle's peak allocation is O(levels x total-ORs) where
/// pgrust is linear. The v6 FLOOR (job -94508) died on exactly this: an
/// expander-built `.`-heavy text with an OR-heavy tail. Neither the pgrust
/// side nor any compared plane is wrong here — the exec simply cannot be run
/// without the C side allocating gigabytes, so bound the PRODUCT and count
/// the skip. The u16 LIMIT arms the expander exists to reach need many '.'
/// and no '|', so this costs no reachability (verified: the pure-'.' and
/// pure-'|' shapes are both far under budget).
///
/// Budget is in nodeitems: 3M x 24 bytes = ~72 MB peak, which keeps a unit
/// well inside libFuzzer's `-rss_limit_mb` AND under its 1s slow-unit bar.
/// `ltree.numlevel` out of a serialized image (offset 4, native-endian
/// uint16 — the on-disk header both sides write).
fn img_numlevel(img: &[u8]) -> u16 {
    if img.len() >= 6 {
        u16::from_ne_bytes([img[4], img[5]])
    } else {
        0
    }
}

fn lquery_amplification_ok(text: &[u8]) -> bool {
    const NODEITEM_BUDGET: u64 = 3_000_000;
    // ALLOCATION-COUNT bound, and the second half of the v6 FLOOR autopsy.
    // C's parse_lquery makes ONE palloc per level and the port makes one Vec
    // per level, so a 65,535-level lquery is ~130k allocations in a single
    // exec on top of the bytes bounded above. Under libFuzzer's default ASan
    // build (the campaign's own configuration) the allocator never returns
    // its per-chunk metadata pages, so a handful of such execs ratchet RSS
    // permanently: the local repro reached the 2048 MB `-rss_limit_mb` with
    // only 34 MB LIVE, 115 MB quarantined and 2.6M retained chunks — an
    // allocator-metadata OOM, not a leak and not a big allocation. That is
    // exactly what killed the v6 FLOOR (job -94508 died `crashed-early` at
    // 7.76M of 10M with an OOM artifact whose input is the EMPTY string, the
    // classic libFuzzer bystander).
    //
    // Level counts ABOVE LQUERY_MAX_LEVELS are deliberately NOT bounded: C
    // raises the label-count error from its COUNTING pass, before it allocates
    // anything per level, so the limit arm the expander exists to reach stays
    // fully reachable (and free). Only the band that parses for real is
    // capped, and levels in that band are structurally identical to each
    // other — the interesting boundary is the count check itself.
    const LEVEL_ALLOC_CAP: u64 = 1_024;
    let levels = text.iter().filter(|&&b| b == b'.').count() as u64 + 1;
    let ors = text.iter().filter(|&&b| b == b'|').count() as u64 + 1;
    if levels > LEVEL_ALLOC_CAP && levels <= LQUERY_MAX_LEVELS {
        return false;
    }
    levels.saturating_mul(ors) <= NODEITEM_BUDGET
}

/// `LQUERY_MAX_LEVELS` (== `PG_UINT16_MAX`, contrib/ltree/ltree.h).
const LQUERY_MAX_LEVELS: u64 = 65535;

fn diff_in(which: usize, text: &[u8], soft: bool, mcx: mcx::Mcx<'_>) -> Option<Vec<u8>> {
    debug_assert!(!text.contains(&0));
    if which == 1 && !lquery_amplification_ok(text) {
        AMPLIFICATION_CARVES.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    let cs = CString::new(text).expect("NUL-free by construction");

    let (mut cip, mut cil): (*const u8, c_int) = (std::ptr::null(), 0);
    let cret = unsafe { pg_lt_in(which as c_int, cs.as_ptr(), soft as c_int, &mut cip, &mut cil) };
    let c = c_bytes(if cret == 0 { 0 } else { -1 }, cip, cil);
    let c_soft_state = unsafe { pg_lt_soft_sqlstate() };
    let c_soft_fires = unsafe { pg_lt_soft_fired() };

    let mut esc = ErrorSaveNode::new(true);
    let (rres, r_isnull) = run_fc::<1>(
        fc(IN_NAMES[which]),
        mcx,
        &[(Datum::from_usize(cs.as_ptr() as usize), false)],
        soft.then_some(&mut esc),
    );
    let dbg = || {
        format!(
            "{} soft={soft} input={:?}",
            IN_NAMES[which],
            String::from_utf8_lossy(&text[..text.len().min(120)])
        )
    };

    match (&rres, cret) {
        (Err(e), -1) => {
            if is_capacity(sqlstate_of(e)) || is_capacity(c.sqlstate) {
                capacity_carve();
                return None;
            }
            assert_eq!(sqlstate_of(e), c.sqlstate, "in: hard-error sqlstate ({})", dbg());
            None
        }
        (Ok(_), 1) => {
            assert!(soft, "C soft error outside soft mode ({})", dbg());
            // vacuity witness: the C escontext branch must have EXECUTED
            assert!(c_soft_fires > 0, "in: C soft path did not fire ({})", dbg());
            assert!(esc.ctx.error_occurred(), "in: C soft error, Rust ok ({})", dbg());
            let re = esc.ctx.error().expect("occurred implies error");
            assert_eq!(sqlstate_of(re), c_soft_state, "in: soft sqlstate ({})", dbg());
            assert!(r_isnull, "in: soft-error result must be NULL ({})", dbg());
            None
        }
        (Ok(d), 0) => {
            assert!(
                !soft || !esc.ctx.error_occurred(),
                "in: Rust soft error, C ok ({})",
                dbg()
            );
            assert!(!r_isnull, "in: Rust NULL on C success ({})", dbg());
            let rimg = image_of(*d).to_vec();
            assert_bytes_eq(&rimg, &c.bytes, &format!("in: image bytes ({})", dbg()));
            Some(rimg)
        }
        (Err(e), 1) => {
            if is_capacity(sqlstate_of(e)) {
                capacity_carve();
                return None;
            }
            panic!("in: C soft error but Rust HARD error {} ({})", e.message, dbg())
        }
        (Err(e), 0) => {
            if is_capacity(sqlstate_of(e)) {
                capacity_carve();
                return None;
            }
            panic!("in: Rust error {} vs C ok ({})", e.message, dbg())
        }
        (Ok(_), -1) => {
            if is_capacity(c.sqlstate) {
                capacity_carve();
                return None;
            }
            panic!("in: Rust ok vs C error sqlstate {} ({})", c.sqlstate, dbg())
        }
        (_, r) => panic!("in: unexpected C ret {r} ({})", dbg()),
    }
}

/// out + send + send->recv roundtrip over an agreed image.
fn diff_outsend(which: usize, img: &[u8], mcx: mcx::Mcx<'_>) {
    // out
    let mut cop: *const c_char = std::ptr::null();
    let coret = unsafe { pg_lt_out(which as c_int, img.as_ptr(), &mut cop) };
    let (rres, _) = run_fc::<1>(
        fc(OUT_NAMES[which]),
        mcx,
        &[(Datum::from_usize(img.as_ptr() as usize), false)],
        None,
    );
    match (&rres, coret) {
        (Ok(d), 0) => {
            let rtext = cstring_of(*d);
            // SAFETY: arena cstring valid until pg_lt_reset.
            let ctext = unsafe { std::ffi::CStr::from_ptr(cop) }.to_bytes();
            assert_eq!(rtext, ctext, "{}: out text", OUT_NAMES[which]);
        }
        (Err(e), -1) => {
            let c_sql = unsafe { pg_lt_sqlstate() };
            if is_capacity(sqlstate_of(e)) || is_capacity(c_sql) {
                capacity_carve();
                return;
            }
            assert_eq!(sqlstate_of(e), c_sql, "{}: sqlstate", OUT_NAMES[which]);
        }
        // ltxtq_out's infix() recurses per tree node and is stack-guarded on
        // both sides, so a deep operator chain lands in the same capacity
        // band as the parsers (pgrust's frames are ~5x larger).
        (Ok(_), _) => {
            if is_capacity(unsafe { pg_lt_sqlstate() }) {
                capacity_carve();
                return;
            }
            panic!("{}: Rust ok vs C error", OUT_NAMES[which])
        }
        (Err(e), _) => {
            if is_capacity(sqlstate_of(e)) {
                capacity_carve();
                return;
            }
            panic!("{}: Rust error {} vs C ok", OUT_NAMES[which], e.message)
        }
    }

    // send
    let (mut csp, mut csl): (*const u8, c_int) = (std::ptr::null(), 0);
    let csret = unsafe { pg_lt_send(which as c_int, img.as_ptr(), &mut csp, &mut csl) };
    let c = c_bytes(csret, csp, csl);
    let (rres, _) = run_fc::<1>(
        fc(SEND_NAMES[which]),
        mcx,
        &[(Datum::from_usize(img.as_ptr() as usize), false)],
        None,
    );
    let wire = match (&rres, c.ret) {
        (Ok(d), 0) => {
            let rw = image_of(*d).to_vec();
            assert_eq!(rw, c.bytes, "{}: wire bytes", SEND_NAMES[which]);
            Some(rw)
        }
        (Err(e), -1) => {
            if is_capacity(sqlstate_of(e)) || is_capacity(c.sqlstate) {
                capacity_carve();
                return;
            }
            assert_eq!(sqlstate_of(e), c.sqlstate, "{}: sqlstate", SEND_NAMES[which]);
            None
        }
        (Ok(_), _) => {
            if is_capacity(c.sqlstate) {
                capacity_carve();
                return;
            }
            panic!("{}: Rust ok vs C error", SEND_NAMES[which])
        }
        (Err(e), _) => {
            if is_capacity(sqlstate_of(e)) {
                capacity_carve();
                return;
            }
            panic!("{}: Rust error {} vs C ok", SEND_NAMES[which], e.message)
        }
    };

    // recv over the agreed wire (skip the 4-byte bytea header)
    if let Some(w) = wire {
        diff_recv(which, &w[4..], mcx);
    }
}

/// recv over raw wire bytes (both sides; verdict + sqlstate + image plane).
fn diff_recv(which: usize, wire: &[u8], mcx: mcx::Mcx<'_>) {
    // lquery_recv parses the SAME text parse_lquery does, so it inherits both
    // oracle-side bounds (see diff_in).
    if which == 1 {
        let text: Vec<u8> = wire.iter().copied().skip(1).collect();
        if lquery_level_u16_wraps(&text) {
            U16_WRAP_CARVES.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if !lquery_amplification_ok(&text) {
            AMPLIFICATION_CARVES.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }
    let (mut cip, mut cil): (*const u8, c_int) = (std::ptr::null(), 0);
    let cret =
        unsafe { pg_lt_recv(which as c_int, wire.as_ptr(), wire.len() as c_int, &mut cip, &mut cil) };
    let c = c_bytes(cret, cip, cil);

    let mut si = stringinfo::StringInfo::with_capacity_in(mcx, wire.len() + 1)
        .expect("stringinfo alloc");
    si.append_bytes(wire).expect("stringinfo append");
    let (rres, _) = run_fc::<1>(
        fc(RECV_NAMES[which]),
        mcx,
        &[(Datum::from_usize(core::ptr::addr_of_mut!(si) as usize), false)],
        None,
    );
    let dbg = || format!("{} wire={:x?}", RECV_NAMES[which], &wire[..wire.len().min(64)]);
    match (&rres, c.ret) {
        (Ok(d), 0) => {
            assert_bytes_eq(image_of(*d), &c.bytes, &format!("recv image ({})", dbg()));
        }
        (Err(e), -1) => {
            if is_capacity(sqlstate_of(e)) || is_capacity(c.sqlstate) {
                capacity_carve();
                return;
            }
            assert_eq!(sqlstate_of(e), c.sqlstate, "recv sqlstate ({})", dbg());
        }
        (Ok(_), -1) => {
            if is_capacity(c.sqlstate) {
                capacity_carve();
                return;
            }
            panic!("recv: Rust ok vs C error {} ({})", c.sqlstate, dbg())
        }
        (Err(e), 0) => {
            if is_capacity(sqlstate_of(e)) {
                capacity_carve();
                return;
            }
            panic!("recv: Rust error {} vs C ok ({})", e.message, dbg())
        }
        (_, r) => panic!("recv: unexpected C ret {r} ({})", dbg()),
    }
}

// ---------------- generic dual-run comparator ----------------

/// Assert (verdict, value) parity between one C entry result and one Rust
/// fc call, with the 54001 capacity carve.
fn assert_parity<T: PartialEq + std::fmt::Debug>(
    what: &str,
    cret: i32,
    cval: T,
    rres: &types_error::PgResult<Datum>,
    rval: impl FnOnce(Datum) -> T,
) {
    let c_sql = unsafe { pg_lt_sqlstate() };
    match (rres, cret) {
        (Ok(d), 0) => {
            let rv = rval(*d);
            assert_eq!(rv, cval, "{what}: value");
        }
        (Err(e), -1) => {
            if is_capacity(sqlstate_of(e)) || is_capacity(c_sql) {
                capacity_carve();
                return;
            }
            assert_eq!(sqlstate_of(e), c_sql, "{what}: sqlstate");
        }
        (Ok(_), -1) => {
            if is_capacity(c_sql) {
                capacity_carve();
                return;
            }
            panic!("{what}: Rust ok vs C error sqlstate {c_sql}")
        }
        (Err(e), 0) => {
            if is_capacity(sqlstate_of(e)) {
                capacity_carve();
                return;
            }
            panic!("{what}: Rust error {} vs C ok", e.message)
        }
        (_, r) => panic!("{what}: unexpected C ret {r}"),
    }
}

fn datum_bool(d: Datum) -> bool {
    d.as_usize() != 0
}

// ---------------- arm 4: two-ltree ops ----------------

fn ops2_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let seed = r.u64();
    let s0 = r.i32();
    let s1 = r.i32();
    let split = r.u16() as usize;
    let rest = r.rest();
    let (ra, rb) = rest.split_at(split.min(rest.len()));
    // expander: lets compact seeds build two ~32k-label trees so the
    // addltree/lca level-count LIMIT arms (numlevel sum > LTREE_MAX_LEVELS)
    // are reachable; literal payloads keep the 4096 cap.
    let ta = cstr_text_x(ra, 4096);
    let tb = cstr_text_x(rb, 4096);

    let ctx = mcx::MemoryContext::new("ltree-ops2");
    let mcx = ctx.mcx();

    let Some(a) = diff_in(0, &ta, false, mcx) else { return };
    let Some(b) = diff_in(0, &tb, false, mcx) else { return };

    // cmp + six comparators (one C call, seven Rust calls)
    let (mut ccmp, mut cbools): (i32, u8) = (0, 0);
    let cret = unsafe { pg_lt_cmp(a.as_ptr(), b.as_ptr(), &mut ccmp, &mut cbools) };
    let args = [
        (Datum::from_usize(a.as_ptr() as usize), false),
        (Datum::from_usize(b.as_ptr() as usize), false),
    ];
    let (rres, _) = run_fc::<2>(fc("ltree_cmp"), mcx, &args, None);
    assert_parity("ltree_cmp", cret, ccmp, &rres, |d| d.as_usize() as u32 as i32);
    for (i, name) in ["ltree_lt", "ltree_le", "ltree_eq", "ltree_ne", "ltree_ge", "ltree_gt"]
        .iter()
        .enumerate()
    {
        let cbit = cbools & (1 << i) != 0;
        let (rres, _) = run_fc::<2>(fc(name), mcx, &args, None);
        assert_parity(name, cret, cbit, &rres, datum_bool);
    }

    // isparent / risparent
    for (rev, name) in [(0, "ltree_isparent"), (1, "ltree_risparent")] {
        let mut cb: c_int = 0;
        let cret = unsafe { pg_lt_isparent(rev, a.as_ptr(), b.as_ptr(), &mut cb) };
        let (rres, _) = run_fc::<2>(fc(name), mcx, &args, None);
        assert_parity(name, cret, cb != 0, &rres, datum_bool);
    }

    // addltree
    {
        let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
        let cret = unsafe { pg_lt_addltree(a.as_ptr(), b.as_ptr(), &mut ip, &mut il) };
        let c = c_bytes(cret, ip, il);
        let (rres, _) = run_fc::<2>(fc("ltree_addltree"), mcx, &args, None);
        assert_parity("ltree_addltree", c.ret, c.bytes.clone(), &rres, |d| {
            image_of(d).to_vec()
        });
    }

    // lca(a,b) — NULL-able result
    {
        let (mut ip, mut il, mut inull): (*const u8, c_int, c_int) = (std::ptr::null(), 0, 0);
        let cret = unsafe { pg_lt_lca2(a.as_ptr(), b.as_ptr(), &mut ip, &mut il, &mut inull) };
        assert_eq!(cret, 0, "lca: C errored (sqlstate {})", unsafe { pg_lt_sqlstate() });
        let (rres, r_isnull) = run_fc::<2>(fc("lca"), mcx, &args, None);
        let rimg = rres.as_ref().expect("lca cannot hard-error on parsed inputs");
        assert_eq!(r_isnull, inull != 0, "lca: null verdict");
        if inull == 0 {
            let c = c_bytes(0, ip, il);
            assert_eq!(image_of(*rimg), &c.bytes[..], "lca: image");
        }
    }

    // hash + hash_extended
    {
        let mut ch: u32 = 0;
        let cret = unsafe { pg_lt_hash(a.as_ptr(), &mut ch) };
        let arg1 = [(Datum::from_usize(a.as_ptr() as usize), false)];
        let (rres, _) = run_fc::<1>(fc("hash_ltree"), mcx, &arg1, None);
        assert_parity("hash_ltree", cret, ch, &rres, |d| d.as_usize() as u32);

        let mut che: u64 = 0;
        let cret = unsafe { pg_lt_hash_ext(a.as_ptr(), seed, &mut che) };
        let arg2 = [
            (Datum::from_usize(a.as_ptr() as usize), false),
            (Datum::from_usize(seed as usize), false),
        ];
        let (rres, _) = run_fc::<2>(fc("hash_ltree_extended"), mcx, &arg2, None);
        assert_parity("hash_ltree_extended", cret, che, &rres, |d| d.as_usize() as u64);
    }

    // nlevel + ltree2text
    {
        let mut cn: i32 = 0;
        let cret = unsafe { pg_lt_nlevel(a.as_ptr(), &mut cn) };
        let arg1 = [(Datum::from_usize(a.as_ptr() as usize), false)];
        let (rres, _) = run_fc::<1>(fc("nlevel"), mcx, &arg1, None);
        assert_parity("nlevel", cret, cn, &rres, |d| d.as_usize() as u32 as i32);

        let (mut pp, mut pl): (*const u8, c_int) = (std::ptr::null(), 0);
        let cret = unsafe { pg_lt_ltree2text(a.as_ptr(), &mut pp, &mut pl) };
        let c = c_bytes(cret, pp, pl);
        let (rres, _) = run_fc::<1>(fc("ltree2text"), mcx, &arg1, None);
        assert_parity("ltree2text", c.ret, c.bytes.clone(), &rres, |d| {
            varlena_payload(d).to_vec()
        });
    }

    // ltree_index 2-arg and 3-arg.
    //
    // COST BOUND (not a carve of behaviour): `ltree_index` is O(an x bn) on
    // BOTH sides (C ltree_op.c restarts the b-walk at every a offset, and the
    // port reproduces that walk), so the expander's two ~32k-level trees —
    // which exist to reach the addltree/lca numlevel-sum LIMIT arms above —
    // make this one operator 550M level compares: 1.2s and 2.4s units, which
    // is what libFuzzer reported as the v6 FLOOR's two slow-unit artifacts
    // (job -94508). Skip only this operator past the budget; everything else
    // in the arm still compares at full scale.
    let index_pairs = (img_numlevel(&a) as u64) * (img_numlevel(&b) as u64);
    for nargs in if index_pairs > 4_000_000 {
        INDEX_COST_CARVES.fetch_add(1, Ordering::Relaxed);
        &[][..]
    } else {
        &[2, 3][..]
    } {
        let nargs = *nargs;
        let mut ci: i32 = 0;
        let cret = unsafe { pg_lt_index(a.as_ptr(), b.as_ptr(), s0, nargs, &mut ci) };
        let what = format!("ltree_index/{nargs}");
        if nargs == 2 {
            let (rres, _) = run_fc::<2>(fc("ltree_index"), mcx, &args, None);
            assert_parity(&what, cret, ci, &rres, |d| d.as_usize() as u32 as i32);
        } else {
            let a3 = [args[0], args[1], (Datum::from_usize(s0 as u32 as usize), false)];
            let (rres, _) = run_fc::<3>(fc("ltree_index"), mcx, &a3, None);
            assert_parity(&what, cret, ci, &rres, |d| d.as_usize() as u32 as i32);
        }
    }

    // subltree(a, s0, s1) + subpath 2/3-arg
    {
        let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
        let cret = unsafe { pg_lt_subltree(a.as_ptr(), s0, s1, &mut ip, &mut il) };
        let c = c_bytes(cret, ip, il);
        let a3 = [
            (Datum::from_usize(a.as_ptr() as usize), false),
            (Datum::from_usize(s0 as u32 as usize), false),
            (Datum::from_usize(s1 as u32 as usize), false),
        ];
        let (rres, _) = run_fc::<3>(fc("subltree"), mcx, &a3, None);
        assert_parity("subltree", c.ret, c.bytes.clone(), &rres, |d| image_of(d).to_vec());

        for nargs in [2, 3] {
            let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
            let cret = unsafe { pg_lt_subpath(a.as_ptr(), s0, s1, nargs, &mut ip, &mut il) };
            let c = c_bytes(cret, ip, il);
            let what = format!("subpath/{nargs}");
            if nargs == 2 {
                let a2 = [a3[0], a3[1]];
                let (rres, _) = run_fc::<2>(fc("subpath"), mcx, &a2, None);
                assert_parity(&what, c.ret, c.bytes.clone(), &rres, |d| image_of(d).to_vec());
            } else {
                let (rres, _) = run_fc::<3>(fc("subpath"), mcx, &a3, None);
                assert_parity(&what, c.ret, c.bytes.clone(), &rres, |d| image_of(d).to_vec());
            }
        }
    }

    // text2ltree + addtext/textadd over the RAW tb payload (text, not
    // necessarily a valid ltree — the parse-inside-op error path)
    {
        let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
        let cret = unsafe { pg_lt_text2ltree(tb.as_ptr(), tb.len() as c_int, &mut ip, &mut il) };
        let c = c_bytes(cret, ip, il);
        let timg = text_image(&tb);
        let targ = [(Datum::from_usize(timg.as_ptr() as usize), false)];
        let (rres, _) = run_fc::<1>(fc("text2ltree"), mcx, &targ, None);
        assert_parity("text2ltree", c.ret, c.bytes.clone(), &rres, |d| image_of(d).to_vec());

        if flags & 1 != 0 {
            for (which, name) in [(0, "ltree_addtext"), (1, "ltree_textadd")] {
                let (mut ip, mut il): (*const u8, c_int) = (std::ptr::null(), 0);
                let cret = unsafe {
                    pg_lt_addtext(which, a.as_ptr(), tb.as_ptr(), tb.len() as c_int, &mut ip, &mut il)
                };
                let c = c_bytes(cret, ip, il);
                let a2 = if which == 0 { [args[0], targ[0]] } else { [targ[0], args[0]] };
                let (rres, _) = run_fc::<2>(fc(name), mcx, &a2, None);
                assert_parity(name, c.ret, c.bytes.clone(), &rres, |d| image_of(d).to_vec());
            }
        }
    }
}

// ---------------- arm 5: matching ----------------

fn match_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    let split = r.u16() as usize;
    let rest = r.rest();
    let (rl, rr) = rest.split_at(split.min(rest.len()));
    let tl = cstr_text(rl, 4096);
    let tr = cstr_text(rr, 8192);

    let ctx = mcx::MemoryContext::new("ltree-match");
    let mcx = ctx.mcx();

    let Some(tree) = diff_in(0, &tl, false, mcx) else { return };
    let txt = flags & 1 != 0; // lquery vs ltxtquery
    let Some(q) = diff_in(if txt { 2 } else { 1 }, &tr, false, mcx) else { return };

    let cases: [(c_int, &str, bool); 2] = if txt {
        [(2, "ltxtq_exec", false), (3, "ltxtq_rexec", true)]
    } else {
        [(0, "ltq_regex", false), (1, "ltq_rregex", true)]
    };
    for (which, name, rev) in cases {
        let (l, rgt) = if rev { (&q, &tree) } else { (&tree, &q) };
        let mut cb: c_int = 0;
        let cret = unsafe { pg_lt_match(which, l.as_ptr(), rgt.as_ptr(), &mut cb) };
        let args = [
            (Datum::from_usize(l.as_ptr() as usize), false),
            (Datum::from_usize(rgt.as_ptr() as usize), false),
        ];
        let (rres, _) = run_fc::<2>(fc(name), mcx, &args, None);
        assert_parity(name, cret, cb != 0, &rres, datum_bool);
    }
}

// ---------------- arm 6: arrays ----------------

/// Driver-built 1-D (or deliberately-malformed) array image over packed
/// varlena element images; identical bytes feed both sides.
fn build_array(items: &[Vec<u8>], shape: u8) -> Vec<u8> {
    let ndim: i32 = if shape == 1 { 2 } else { 1 };
    let with_nulls = shape == 2 || shape == 4;
    let n = items.len() as i32;
    let nd = ndim as usize;
    let mut v = Vec::new();
    v.extend_from_slice(&[0u8; 4]); // vl_len_ patched below
    v.extend_from_slice(&ndim.to_le_bytes());
    v.extend_from_slice(&[0u8; 4]); // dataoffset patched below if with_nulls
    v.extend_from_slice(&0u32.to_le_bytes()); // elemtype (unread by family fns)
    if ndim == 2 {
        v.extend_from_slice(&n.to_le_bytes());
        v.extend_from_slice(&1i32.to_le_bytes());
        v.extend_from_slice(&1i32.to_le_bytes());
        v.extend_from_slice(&1i32.to_le_bytes());
    } else {
        v.extend_from_slice(&n.to_le_bytes());
        v.extend_from_slice(&1i32.to_le_bytes());
    }
    if with_nulls {
        // null bitmap: shape 2 marks element 0 NULL (bit clear); shape 4
        // keeps every element present (bitmap attached, nothing null).
        let nbytes = items.len().div_ceil(8);
        let mut bm = vec![0xffu8; nbytes];
        if shape == 2 && !items.is_empty() {
            bm[0] &= !1;
        }
        v.extend_from_slice(&bm);
        let off = (v.len() + 7) & !7;
        v.resize(off, 0);
        let off32 = off as i32;
        v[8..12].copy_from_slice(&off32.to_le_bytes());
    } else {
        let off = (16 + 2 * nd * 4 + 7) & !7;
        v.resize(off, 0);
    }
    for it in items {
        while v.len() % 4 != 0 {
            v.push(0);
        }
        v.extend_from_slice(it);
    }
    let total = v.len() as u32;
    v[0..4].copy_from_slice(&(total << 2).to_le_bytes());
    v
}

fn arrays_case(r: &mut Rdr<'_>) {
    let flags = r.u8();
    // 0/3 well-formed, 1 ndim=2, 2 null bitmap (elem0 NULL);
    // flags&0x20: bitmap PRESENT but all-valid (drives the C
    // array_contains_nulls bitmap-scan-finds-nothing arm and Rust
    // array.rs contains_nulls -> false tail).
    let shape = if flags & 0x20 != 0 {
        4
    } else {
        match flags & 3 {
            3 => 0,
            s => s,
        }
    };
    // flags&0x10: EMPTY array (nitems=0) — C's array_iterator num==0 falls
    // through to NULL, Rust lca_inner len==0 -> None; parity on the NULL
    // verdict plane.
    let nitems = if flags & 0x10 != 0 {
        0
    } else {
        (r.u8() % 4) as usize + 1
    };
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    for _ in 0..nitems {
        let l = r.u16() as usize % 512;
        // Rdr::u8 advances pos past the end (returning 0s), so pos itself
        // must be clamped before it indexes — not just the length.
        let start = r.pos.min(r.d.len());
        let take = l.min(r.d.len() - start);
        let raw = &r.d[start..start + take];
        r.pos = start + take;
        chunks.push(cstr_text(raw, 512));
    }
    let rhs_raw = cstr_text(r.rest(), 4096);

    let ctx = mcx::MemoryContext::new("ltree-arrays");
    let mcx = ctx.mcx();

    let mut elems = Vec::new();
    for ch in &chunks {
        match diff_in(0, ch, false, mcx) {
            Some(img) => elems.push(img),
            None => return, // any disagreement already asserted inside
        }
    }
    let arr = build_array(&elems, shape);

    let kind = (flags >> 2) % 3; // 0 ltree, 1 lquery, 2 ltxtquery
    let Some(rhs) = diff_in(kind as usize, &rhs_raw, false, mcx) else { return };

    let arrd = (Datum::from_usize(arr.as_ptr() as usize), false);
    let rhsd = (Datum::from_usize(rhs.as_ptr() as usize), false);

    let bool_cases: &[(c_int, &str, bool)] = match kind {
        0 => &[
            (0, "_ltree_isparent", false),
            (1, "_ltree_risparent", false),
            (11, "_ltree_r_isparent", true),
            (12, "_ltree_r_risparent", true),
        ],
        1 => &[(2, "_ltq_regex", false), (13, "_ltq_rregex", true)],
        _ => &[(3, "_ltxtq_exec", false), (14, "_ltxtq_rexec", true)],
    };
    for &(which, name, rev) in bool_cases {
        let mut cb: c_int = 0;
        let (mut ip, mut il, mut inull): (*const u8, c_int, c_int) = (std::ptr::null(), 0, 0);
        // The r-variants swap their own args (DirectFunctionCall2(f, arg1,
        // arg0)), so BOTH sides must receive them in the caller's order —
        // feeding C (arr, rhs) while Rust got (rhs, arr) made C iterate the
        // rhs as if it were the array (harness defect, caught by the seed
        // rail; see the injection-sweep note).
        let (cl, cr) = if rev {
            (rhs.as_ptr(), arr.as_ptr())
        } else {
            (arr.as_ptr(), rhs.as_ptr())
        };
        let cret = unsafe { pg_lt_arr(which, cl, cr, &mut cb, &mut ip, &mut il, &mut inull) };
        let args = if rev { [rhsd, arrd] } else { [arrd, rhsd] };
        let (rres, _) = run_fc::<2>(fc(name), mcx, &args, None);
        assert_parity(name, cret, cb != 0, &rres, datum_bool);
    }

    let ext_cases: &[(c_int, &str)] = match kind {
        0 => &[(6, "_ltree_extract_isparent"), (7, "_ltree_extract_risparent")],
        1 => &[(8, "_ltq_extract_regex")],
        _ => &[(9, "_ltxtq_extract_exec")],
    };
    for &(which, name) in ext_cases {
        let mut cb: c_int = 0;
        let (mut ip, mut il, mut inull): (*const u8, c_int, c_int) = (std::ptr::null(), 0, 0);
        let cret = unsafe {
            pg_lt_arr(which, arr.as_ptr(), rhs.as_ptr(), &mut cb, &mut ip, &mut il, &mut inull)
        };
        let (rres, r_isnull) = run_fc::<2>(fc(name), mcx, &[arrd, rhsd], None);
        let c_sql = unsafe { pg_lt_sqlstate() };
        match (&rres, cret) {
            (Ok(d), 0) => {
                assert_eq!(r_isnull, inull != 0, "{name}: null verdict");
                if inull == 0 {
                    let c = c_bytes(0, ip, il);
                    assert_eq!(image_of(*d), &c.bytes[..], "{name}: image");
                }
            }
            (Err(e), -1) => {
                if is_capacity(sqlstate_of(e)) || is_capacity(c_sql) {
                    capacity_carve();
                    continue;
                }
                assert_eq!(sqlstate_of(e), c_sql, "{name}: sqlstate");
            }
            (Ok(_), -1) => {
                if is_capacity(c_sql) {
                    capacity_carve();
                    continue;
                }
                panic!("{name}: Rust ok vs C error {c_sql}")
            }
            (Err(e), 0) => {
                if is_capacity(sqlstate_of(e)) {
                    capacity_carve();
                    continue;
                }
                panic!("{name}: Rust error {} vs C ok", e.message)
            }
            (_, rr) => panic!("{name}: unexpected C ret {rr}"),
        }
    }

    // _lca(arr)
    {
        let mut cb: c_int = 0;
        let (mut ip, mut il, mut inull): (*const u8, c_int, c_int) = (std::ptr::null(), 0, 0);
        let cret = unsafe {
            pg_lt_arr(10, arr.as_ptr(), arr.as_ptr(), &mut cb, &mut ip, &mut il, &mut inull)
        };
        let (rres, r_isnull) = run_fc::<1>(fc("_lca"), mcx, &[arrd], None);
        match (&rres, cret) {
            (Ok(d), 0) => {
                assert_eq!(r_isnull, inull != 0, "_lca: null verdict");
                if inull == 0 {
                    let c = c_bytes(0, ip, il);
                    assert_eq!(image_of(*d), &c.bytes[..], "_lca: image");
                }
            }
            (Err(e), -1) => {
                assert_eq!(sqlstate_of(e), unsafe { pg_lt_sqlstate() }, "_lca: sqlstate");
            }
            (Ok(_), -1) => panic!("_lca: Rust ok vs C error"),
            (Err(e), 0) => panic!("_lca: Rust error {} vs C ok", e.message),
            (_, rr) => panic!("_lca: unexpected C ret {rr}"),
        }
    }

    // lquery[] family: lt_q_regex / _lt_q_regex / r-variants
    // (needs a tree element; the empty-array shape has none)
    if kind == 1 && !elems.is_empty() {
        let qarr = build_array(std::slice::from_ref(&rhs), 0);
        let qarrd = (Datum::from_usize(qarr.as_ptr() as usize), false);
        let tree0 = &elems[0];
        let tree0d = (Datum::from_usize(tree0.as_ptr() as usize), false);
        let cases: [(c_int, &str, [(Datum, bool); 2], *const u8, *const u8); 4] = [
            (4, "lt_q_regex", [tree0d, qarrd], tree0.as_ptr(), qarr.as_ptr()),
            (15, "lt_q_rregex", [qarrd, tree0d], qarr.as_ptr(), tree0.as_ptr()),
            (5, "_lt_q_regex", [arrd, qarrd], arr.as_ptr(), qarr.as_ptr()),
            (16, "_lt_q_rregex", [qarrd, arrd], qarr.as_ptr(), arr.as_ptr()),
        ];
        for (which, name, args, cl, cr) in cases {
            let mut cb: c_int = 0;
            let (mut ip, mut il, mut inull): (*const u8, c_int, c_int) = (std::ptr::null(), 0, 0);
            let cret = unsafe { pg_lt_arr(which, cl, cr, &mut cb, &mut ip, &mut il, &mut inull) };
            let (rres, _) = run_fc::<2>(fc(name), mcx, &args, None);
            assert_parity(name, cret, cb != 0, &rres, datum_bool);
        }
    }
}

// ---------------- arm 7: crc ----------------

fn crc_case(r: &mut Rdr<'_>) {
    let raw = r.rest();
    let take = &raw[..raw.len().min(4096)];
    let mut cc: u32 = 0;
    let cret = unsafe { pg_lt_crc(take.as_ptr(), take.len() as c_int, &mut cc) };
    assert_eq!(cret, 0, "crc: C errored");
    let rc = ltree::crc::ltree_crc32_sz(take);
    assert_eq!(rc, cc, "ltree_crc32_sz: value (input {:x?})", &take[..take.len().min(64)]);
}

// ---------------- entry ----------------

pub fn ltree_diff(data: &[u8]) {
    if data.is_empty() {
        return;
    }
    // Oracle serialization (task #125): the vendored contrib/ltree TU carries
    // process-global statics (the arena allocation list, the setjmp ereport
    // channel, the soft-error counters) with no C-side synchronization. Taken
    // at the DRIVER entry so every arm and every test entry point inherits it;
    // reentrant per thread. scripts/lint-oracle-serial.py fences this.
    let _oracle = crate::oracle_serial();
    init_env();
    pin_encoding();
    unsafe { pg_lt_reset() };

    let sel = data[0] % 8;
    let mut r = Rdr::new(&data[1..]);
    match sel {
        0 | 1 | 2 => {
            let which = sel as usize;
            let flags = r.u8();
            let soft = flags & 1 != 0;
            let text = cstr_text_x(r.rest(), 16384);
            let ctx = mcx::MemoryContext::new("ltree-inout");
            let mcx = ctx.mcx();
            if let Some(img) = diff_in(which, &text, soft, mcx) {
                // The `in` plane (stored image bytes) compares in EVERY band -
                // that is what the C-exact on-disk adoption bought. Only the
                // deparse path is withheld: in the wrap band C's
                // deparse_lquery writes past its heap buffer (bug 103a), so
                // running it would corrupt the fuzzer, not compare anything.
                if which == 1 && lquery_level_u16_wraps(&text) {
                    U16_WRAP_CARVES.fetch_add(1, Ordering::Relaxed);
                } else {
                    diff_outsend(which, &img, mcx);
                }
            }
        }
        3 => {
            let which = (r.u8() % 3) as usize;
            let wire = r.rest().to_vec();
            let ctx = mcx::MemoryContext::new("ltree-recv");
            let mcx = ctx.mcx();
            diff_recv(which, &wire, mcx);
        }
        4 => ops2_case(&mut r),
        5 => match_case(&mut r),
        6 => arrays_case(&mut r),
        _ => crc_case(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exec(data: &[u8]) {
        ltree_diff(data);
    }

    #[test]
    fn smoke_arms() {
        exec(b"\x00\x00Top.Science.Astronomy");
        exec(b"\x01\x00Top.*{1,3}.science|astro@*");
        exec(b"\x02\x00(Astro & !Physics) | Science");
        exec(b"\x03\x00\x01Top.Science");
        let mut v = vec![4u8, 1];
        v.extend_from_slice(&7u64.to_le_bytes());
        v.extend_from_slice(&0i32.to_le_bytes());
        v.extend_from_slice(&2i32.to_le_bytes());
        v.extend_from_slice(&5u16.to_le_bytes());
        v.extend_from_slice(b"a.b.caa.b");
        exec(&v);
        let mut v = vec![5u8, 0];
        v.extend_from_slice(&3u16.to_le_bytes());
        v.extend_from_slice(b"a.ba.*");
        exec(&v);
        let mut v = vec![6u8, 4, 2];
        v.extend_from_slice(&3u16.to_le_bytes());
        v.extend_from_slice(b"a.b");
        v.extend_from_slice(&1u16.to_le_bytes());
        v.extend_from_slice(b"aa.b.c");
        exec(&v);
        exec(b"\x07AbCdEf_123-xyz");
    }

    /// a0 EXHAUSTIVE-DIFF (FLEET-QUEUE item 4): ltree_crc32_sz over the FULL
    /// 1-byte domain (256) and FULL 2-byte domain (65536) — total closure of
    /// the fold+CRC kernel at these lengths, both sides executed per case.
    #[test]
    fn crc_exhaustive_1_2_bytes() {
        let _oracle = crate::c_oracle_serial();
        init_env();
        pin_encoding();
        unsafe { pg_lt_reset() };
        for b in 0..=255u8 {
            let buf = [b];
            let mut cc: u32 = 0;
            assert_eq!(unsafe { pg_lt_crc(buf.as_ptr(), 1, &mut cc) }, 0);
            assert_eq!(ltree::crc::ltree_crc32_sz(&buf), cc, "crc mismatch at byte {b:#x}");
        }
        for w in 0..=65535u16 {
            let buf = w.to_le_bytes();
            let mut cc: u32 = 0;
            assert_eq!(unsafe { pg_lt_crc(buf.as_ptr(), 2, &mut cc) }, 0);
            assert_eq!(ltree::crc::ltree_crc32_sz(&buf), cc, "crc mismatch at word {w:#x}");
        }
    }

    /// CARVE VACUITY CHECK (dead-counter class). Every carve in this driver
    /// keeps a counter, and until this test existed NOTHING READ ANY OF THEM —
    /// the module doc says they are "counted for the campaign report" and they
    /// were incremented into the void. A carve nobody can observe is exactly
    /// how a bound silently widens until it swallows the band it was supposed
    /// to trim, and here the band in question is the one the R1 adoption just
    /// opened up for comparison.
    ///
    /// So: drive one input per carve and assert the counter MOVED, and — the
    /// load-bearing half — assert the wrap band is NOT skipped wholesale, i.e.
    /// a wrap-band input still reaches the `in` comparison. If someone widens
    /// `lquery_level_u16_wraps` or tightens `lquery_amplification_ok` far
    /// enough to skip the whole exec again, this fails.
    #[test]
    fn carve_counters_are_not_dead() {
        let wrap0 = U16_WRAP_CARVES.load(Ordering::Relaxed);
        let amp0 = AMPLIFICATION_CARVES.load(Ordering::Relaxed);
        let idx0 = INDEX_COST_CARVES.load(Ordering::Relaxed);

        // (a) wrap band: 4,096 one-byte variants -> per-level size > 65535.
        // The out/send plane is withheld, so the counter moves; the `in` plane
        // still compares (that is R1), which the witness test above proves.
        let mut wrapin = vec![1u8, 0];
        wrapin.extend_from_slice(("a".to_string() + &"|a".repeat(4095)).as_bytes());
        run_on_worker(wrapin);
        assert!(
            U16_WRAP_CARVES.load(Ordering::Relaxed) > wrap0,
            "the uint16 wrap carve never fired — either the predicate stopped \
             recognising the band or the band stopped being reachable"
        );

        // (b) amplification bound: expander-built '.'-heavy text (levels above
        // the alloc cap but at or below LQUERY_MAX_LEVELS) — the shape that
        // killed the v6 floor.
        let amp: Vec<u8> = vec![1u8, 0, 0xFE, 0x00, 0x40, 0x02, b'a', b'.', b'a'];
        run_on_worker(amp);
        assert!(
            AMPLIFICATION_CARVES.load(Ordering::Relaxed) > amp0,
            "the upstream-amplification bound never fired — the v6 OOM shape is \
             unbounded again"
        );

        // (c) ltree_index cost bound: two expander-built ~32k-level trees, the
        // shape that produced the v6 slow-units.
        let mut ops = vec![4u8, 0];
        ops.extend_from_slice(&0u64.to_le_bytes()); // seed
        ops.extend_from_slice(&0i32.to_le_bytes()); // s0
        ops.extend_from_slice(&0i32.to_le_bytes()); // s1
        // NOTE the trailing label: the expander emits unit x n THEN the tail,
        // and "a." x n ends on a dot, which ltree rejects as a syntax error —
        // diff_in would return None and the arm would never reach the operator.
        // (Caught by this very assertion on the first attempt.)
        let a: Vec<u8> = vec![0xFE, 0xFF, 0x7F, 0x02, b'a', b'.', b'a'];
        let b: Vec<u8> = vec![0xFE, 0xFF, 0x7F, 0x02, b'b', b'.', b'b'];
        ops.extend_from_slice(&(a.len() as u16).to_le_bytes()); // split
        ops.extend_from_slice(&a);
        ops.extend_from_slice(&b);
        run_on_worker(ops);
        assert!(
            INDEX_COST_CARVES.load(Ordering::Relaxed) > idx0,
            "the ltree_index cost bound never fired — the v6 slow-unit shape is \
             unbounded again"
        );
    }

    /// Production stack pairing (the lane's durable lesson): 8 MiB worker vs a
    /// 2048 kB max_stack_depth, so the guard can fire before exhaustion.
    fn run_on_worker(data: Vec<u8>) {
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(move || ltree_diff(&data))
            .unwrap()
            .join()
            .unwrap();
    }

    /// R1 ADOPTION WITNESS (RULED 2026-08-03): the stored lquery image must be
    /// byte-identical to PostgreSQL's in the `uint16` wrap band, which is only
    /// true because the port truncates `numvar` to the stored field and walks
    /// levels by `MAXALIGN(the stored totallen)`. Drives `lquery_in` at 4,096+
    /// one-byte variants in ONE level (per-level serialized size 65,552 > u16)
    /// straight through the uncarved `in` plane; `out`/`send` stay withheld
    /// (C's deparse overflows there, bug 103a).
    ///
    /// This is the shape whose value plane the lane carved wholesale. If the
    /// truncation or the stride regresses, the image bytes disagree here and
    /// this test fails (verified by disarming both, one at a time).
    #[test]
    fn r1_cexact_ondisk_wrap_band_image() {
        // numvar itself wraps only past 65535 variants, which needs a 131 KB
        // text - past the arm's literal cap, so it goes through the expander:
        // [0xFE][u16 n=65535][u8 ul=2]["a|"] + tail "a" builds "a|" x 65535
        // plus "a" = exactly 65,536 variants in ONE level. C stores
        // numvar = 65536 mod 65536 = 0, and numvar == 0 means '*' in this
        // format, so PostgreSQL turns a 65,536-way alternation into a star
        // level. The port must store the same 0.
        let numvar_wrap: Vec<u8> = vec![1u8, 0, 0xFE, 0xFF, 0xFF, 0x02, b'a', b'|', b'a'];
        std::thread::Builder::new()
            .stack_size(8 << 20)
            .spawn(move || ltree_diff(&numvar_wrap))
            .unwrap()
            .join()
            .unwrap();

        // The level STRIDE only shows up with a second level behind a wrapped
        // one: 4,095 one-byte variants make level 1's totallen exactly 65,536,
        // which the uint16 stores as 0, so C's LQL_NEXT advances ZERO bytes and
        // writes level 2's header straight over level 1's. The port has to
        // land in the same place.
        for lead in [4094usize, 4095, 4096, 8191] {
            let mut q = String::from("a");
            for _ in 1..lead {
                q.push_str("|a");
            }
            q.push_str(".b.c|d");
            let mut text = vec![1u8, 0];
            text.extend_from_slice(q.as_bytes());
            std::thread::Builder::new()
                .stack_size(8 << 20)
                .spawn(move || ltree_diff(&text))
                .unwrap()
                .join()
                .unwrap();
        }

        for variants in [4094usize, 4095, 4096, 4100, 8192] {
            let mut text = vec![2u8, 0]; // arm 2 placeholder, patched below
            text[0] = 1; // lquery_in
            let mut q = String::from("a");
            for _ in 1..variants {
                q.push_str("|a");
            }
            text.extend_from_slice(q.as_bytes());
            std::thread::Builder::new()
                .stack_size(8 << 20)
                .spawn(move || ltree_diff(&text))
                .unwrap()
                .join()
                .unwrap();
        }
    }

    /// Regression seeds: the five fixed defect shapes from the claim row
    /// (SQL-reachable panic inputs, atoi truncation, 22023 limit errors)
    /// plus soft-mode sweeps — must run divergence-free.
    #[test]
    fn fixed_defect_shapes() {
        for q in ["a&", "a|", "!", "a &", "(a)&", "", " "] {
            let mut v = vec![2u8, 0];
            v.extend_from_slice(q.as_bytes());
            exec(&v);
            v[1] = 1; // soft mode
            exec(&v);
        }
        for q in [
            "*{4294967301}", "*{4294967296}", "a{4294967301,4294967301}", "*{4294967295}",
            "a{2,1}", "*{65536}", "a@|b*|c%", "!a.!b{,}.*{2,}",
        ] {
            let mut v = vec![1u8, 0];
            v.extend_from_slice(q.as_bytes());
            exec(&v);
            v[1] = 1;
            exec(&v);
        }
        // deep ltxtquery nesting (capacity band exercises the 54001 carve)
        for depth in [10usize, 200, 900] {
            let mut q = String::new();
            for _ in 0..depth {
                q.push('(');
            }
            q.push('a');
            for _ in 0..depth {
                q.push(')');
            }
            let mut v = vec![2u8, 0];
            v.extend_from_slice(q.as_bytes());
            exec(&v);
        }
        // uppercase labels through the LOWER_NODE crc path (lquery INCASE)
        let mut v = vec![1u8, 0];
        v.extend_from_slice(b"AbC@|DeF*");
        exec(&v);
    }
}

