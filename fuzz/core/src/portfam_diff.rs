//! portfam_diff: differential fuzz driver for the p1-microbatch PORTFAM
//! bucket — shipped Rust vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream
//! sha 62d6c7d3df) C (csrc/pg_portfam_io.c + csrc/portfam/**, all bodies
//! VERBATIM whole-file copies including EVERY header static inline).
//!
//! Crates under test:
//!   crates/port/pg_bitutils               (C src/port/pg_bitutils.c
//!                                          + src/include/port/pg_bitutils.h)
//!   crates/port/crc32c                    (C src/port/pg_crc32c_sb8.c
//!                                          + src/backend/utils/hash/pg_crc.c)
//!   crates/port/pgstrcasecmp              (C src/port/pgstrcasecmp.c)
//!   crates/port/pg_path                   (C src/port/path.c)
//!   crates/backend/access/common/bufmask  (C .../access/common/bufmask.c)
//!
//! COMPARISON PLANES: value bytes/bits on every arm; error VERDICT on the
//! one arm where either side can raise (bufmask mask_unused_space:
//! C `elog(ERROR, "invalid page ...")` vs the Rust `assert!` — compared as
//! raised/not-raised). There is NO SQLSTATE plane: `elog` (not `ereport`)
//! carries no errcode, so ERRCODE_INTERNAL_ERROR is the only class possible
//! and it is implied by the verdict. FC-WRAPPER PLANE: not applicable —
//! none of the five crates has a builtins.rs / fc_* surface (all are
//! non-SQL port/support crates; routes oid `-`).
//!
//! Input layout: [selector][payload]; `selector % 15` picks the arm.
//! Payload fields are little-endian, missing bytes read as zero — every
//! input length is a valid input.
//!
//!   0  bitutils32   u32 word -> popcount/ceil_log2/leftmost/rightmost/
//!                   prevpower2/nextpower2
//!   1  bitutils64   u64 word -> the same six, 64-bit
//!   2  rotate32     u32 word x n(1..=31) -> rotate_right32 / rotate_left32
//!   3  popcount     byte buffer (+ start offset, + mask) -> pg_popcount,
//!                   pg_popcount_masked
//!   4  crc32c       init crc x byte buffer -> sb8 arm, shipped dispatch,
//!                   hardware arm (where the host has it), FIN
//!   5  crc32legacy  byte buffer -> traditional_crc32, legacy_crc32_lexeme
//!   6  strcase      two byte strings (+ n) -> pg_strcasecmp, pg_strncasecmp
//!   7  casefold     one byte -> pg_tolower/toupper/ascii_tolower/ascii_toupper
//!   8  pathpred     two path strings -> is_absolute_path, first/last_dir_
//!                   separator, first_path_var_separator,
//!                   path_contains_parent_reference,
//!                   path_is_relative_and_below_cwd, path_is_prefix_of_path
//!   9  canon        path string -> canonicalize_path
//!   10 join         head, tail -> join_path_components, then
//!                   get_parent_directory on the result
//!   11 relpath      my_exec_path x which(0..=10) -> get_{share,etc,include,
//!                   pkginclude,includeserver,lib,pkglib,locale,doc,html,
//!                   man}_path (the make_relative_path core)
//!   12 maskhdr      page image -> mask_page_lsn_and_checksum,
//!                   mask_page_hint_bits, mask_page_content
//!   13 maskunused   page image -> mask_unused_space (VALUE + ERROR VERDICT)
//!   14 masklp       page image -> mask_lp_flags
//!
//! DOMAIN CARVES (each fences C UB or a documented C caller contract, never
//! pgrust behavior — the Rust side stays total and is not weakened):
//!   - rotate32 (arm 2): C `pg_rotate_right32` computes
//!     `(word >> n) | (word << (32 - n))`; n == 0 or n >= 32 shifts by >=
//!     the width, UB in C. The documented contract is 0 < n < 32; the driver
//!     folds n into 1..=31. Rust's rotate_right/left mask n mod 32 and are
//!     total — a deliberate, documented divergence outside C's domain.
//!   - bitutils (arms 0/1): pg_leftmost_one_pos*/pg_rightmost_one_pos*/
//!     pg_prevpower2_* require word != 0 (C __builtin_clz/ctz are UB at 0);
//!     pg_nextpower2_* require 0 < num <= PG_UINT{32,64}_MAX / 2 + 1. The
//!     same guards gate both sides.
//!   - path arms (8..=11): C canonicalize_path / join_path_components /
//!     make_relative_path work in-place in MAXPGPATH (1024) stack buffers and
//!     truncate at that bound (strlcpy/snprintf); the Rust API returns
//!     unbounded Strings. The driver keeps every input short enough that no
//!     C truncation can occur, so truncation is never the compared surface.
//!     Path bytes come from a printable alphabet with no interior NUL (a NUL
//!     terminates the C string but is an ordinary byte to &str — a
//!     representation difference, not a behavior surface).
//!   - bufmask (arms 12..=14): pd_lower/pd_upper/pd_special are clamped into
//!     [0, BLCKSZ] before the call, and mask_lp_flags additionally gets
//!     pd_lower clamped to a real line-pointer array: C walks
//!     (pd_lower - SizeOfPageHeaderData)/4 pointers with NO bound check, so
//!     an unclamped uint16 reads past the page on BOTH sides (OOB in C,
//!     panic in Rust) — a caller contract, not a behavior difference.
//!     mask_unused_space's own validity check is NOT carved: its error
//!     verdict is a compared plane (see arm_maskunused for the one HARNESS
//!     carve — libfuzzer-sys's abort-on-panic hook, which moves the Rust
//!     half of that plane into the `cargo test` build).
//!   - crc32c hardware arms: armv8/sse42 run only where the host CPU has the
//!     feature; the portable sb8 arm is the oracle everywhere, and every
//!     host compares at least sb8 + the shipped dispatch entry.
//!   - pgstrcasecmp: `isupper`/`tolower` read the PROCESS locale on both
//!     sides inside one process — locale is environment, never computation
//!     (the minimal-seaming rule), so the high-bit arm is genuinely
//!     compared rather than modelled.
//!
//! SKIPPED (outside the claimed crates' shipped surface — exception rows):
//!   - path.c WIN32 arms (skip_drive/debackslash_path/pg_sjis_mblen/
//!     make_native_path/cleanup_path/has_drive_prefix), get_progname,
//!     get_home_path, make_absolute_path — Windows-only or cwd/$HOME-reading;
//!     not ported by crates/port/pg_path.
//!   - pg_path validate_exec / normalize_exec_path / find_my_exec — stat/
//!     access/realpath/$PATH-reading (excluded-state carve).
//!   - crc32c pg_comp_crc32c_avx512 — the 18.3 runtime-check arm, not ported.

#![allow(dead_code)]

use std::ffi::CString;

use types_core::BLCKSZ;
use types_storage::bufpage::{PageHeaderData, PageMut, SizeOfPageHeaderData};

const MAXPGPATH: usize = 1024;

extern "C" {
    // pg_bitutils
    fn pg_diff_pf_bitutils32(word: u32, out: *mut u64);
    fn pg_diff_pf_bitutils64(word: u64, out: *mut u64);
    fn pg_diff_pf_rotate_right32(word: u32, n: i32) -> u32;
    fn pg_diff_pf_rotate_left32(word: u32, n: i32) -> u32;
    fn pg_diff_pf_popcount(buf: *const u8, bytes: i32) -> u64;
    fn pg_diff_pf_popcount_masked(buf: *const u8, bytes: i32, mask: u8) -> u64;
    // crc32c
    fn pg_diff_pf_crc32c_sb8(crc: u32, data: *const u8, len: usize) -> u32;
    fn pg_diff_pf_crc32_traditional(data: *const u8, len: usize) -> u32;
    fn pg_diff_pf_crc32_legacy(data: *const u8, len: usize) -> u32;
    // pgstrcasecmp
    fn pg_diff_pf_strcasecmp(s1: *const u8, s2: *const u8) -> i32;
    fn pg_diff_pf_strncasecmp(s1: *const u8, s2: *const u8, n: usize) -> i32;
    fn pg_diff_pf_toupper(ch: i32) -> i32;
    fn pg_diff_pf_tolower(ch: i32) -> i32;
    fn pg_diff_pf_ascii_toupper(ch: i32) -> i32;
    fn pg_diff_pf_ascii_tolower(ch: i32) -> i32;
    // path
    fn pg_diff_pf_canonicalize(buf: *mut u8);
    fn pg_diff_pf_join(head: *const u8, tail: *const u8, ret: *mut u8);
    fn pg_diff_pf_parent_dir(buf: *mut u8);
    fn pg_diff_pf_first_dir_sep(s: *const u8) -> i64;
    fn pg_diff_pf_last_dir_sep(s: *const u8) -> i64;
    fn pg_diff_pf_first_path_var_sep(s: *const u8) -> i64;
    fn pg_diff_pf_contains_parent_ref(s: *const u8) -> i32;
    fn pg_diff_pf_rel_below_cwd(s: *const u8) -> i32;
    fn pg_diff_pf_prefix_of(p1: *const u8, p2: *const u8) -> i32;
    fn pg_diff_pf_get_rel_path(which: i32, my_exec_path: *const u8, ret: *mut u8);
    // bufmask
    fn pg_diff_pf_mask_page_lsn_and_checksum(page: *mut u8);
    fn pg_diff_pf_mask_page_hint_bits(page: *mut u8);
    fn pg_diff_pf_mask_unused_space(page: *mut u8) -> i32;
    fn pg_diff_pf_mask_lp_flags(page: *mut u8);
    fn pg_diff_pf_mask_page_content(page: *mut u8);
}

// ---------------------------------------------------------------- helpers

/// Little-endian u64 field reader; missing bytes read as zero.
fn u64_at(p: &[u8], idx: usize) -> u64 {
    let mut b = [0u8; 8];
    let off = idx * 8;
    for (i, slot) in b.iter_mut().enumerate() {
        if let Some(&v) = p.get(off + i) {
            *slot = v;
        }
    }
    u64::from_le_bytes(b)
}

fn u32_at(p: &[u8], idx: usize) -> u32 {
    u64_at(p, idx) as u32
}

fn p_get(payload: &[u8], i: usize) -> u8 {
    payload.get(i).copied().unwrap_or(0)
}

/// 8KB page image, MAXALIGNed exactly as a real buffer-pool page.
#[repr(align(8))]
struct AlignedPage([u8; BLCKSZ]);

impl AlignedPage {
    fn new() -> Box<Self> {
        Box::new(AlignedPage([0u8; BLCKSZ]))
    }

    fn clone_box(&self) -> Box<Self> {
        let mut q = AlignedPage::new();
        q.0.copy_from_slice(&self.0);
        q
    }
}

fn page_mut(p: &mut AlignedPage) -> PageMut<'_> {
    let ptr = core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap();
    // SAFETY: owned MAXALIGNed BLCKSZ image, exclusively borrowed.
    unsafe { PageMut::from_raw(ptr) }
}

fn set_hdr_u16(p: &mut AlignedPage, off: usize, v: u16) {
    p.0[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

fn get_hdr_u16(p: &AlignedPage, off: usize) -> u16 {
    u16::from_le_bytes([p.0[off], p.0[off + 1]])
}

/// Path alphabet: every byte the Unix arms of path.c branch on, plus filler.
/// NUL is deliberately absent (see DOMAIN CARVES).
const PATH_ALPHABET: &[u8] = b"/.:abAB-_ \\\t/./..";

/// Build a path-shaped ASCII string of at most `cap` bytes from `src`.
/// Printable ASCII passes through unchanged so real path literals from the
/// dictionary and corpus survive mutation; other bytes fold through the
/// alphabet (structural mode).
fn path_str(src: &[u8], cap: usize) -> String {
    let mut out = String::with_capacity(src.len().min(cap));
    for &b in src.iter().take(cap) {
        if (0x20..0x7f).contains(&b) {
            out.push(b as char);
        } else {
            out.push(PATH_ALPHABET[b as usize % PATH_ALPHABET.len()] as char);
        }
    }
    out
}

fn cstr(s: &str) -> CString {
    // Interior NULs are excluded by path_str.
    CString::new(s.as_bytes()).unwrap_or_else(|_| CString::new("").unwrap())
}

fn c_buf_to_string(buf: &[u8]) -> String {
    let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    buf[..end].iter().map(|&c| c as u8 as char).collect()
}

/// C `canonicalize_path` on a MAXPGPATH scratch buffer (the C API is
/// in-place on a caller buffer).
fn c_canonicalize(s: &str) -> String {
    let mut buf = vec![0u8; MAXPGPATH];
    let bytes = s.as_bytes();
    assert!(bytes.len() < MAXPGPATH, "driver must cap path inputs");
    for (i, &b) in bytes.iter().enumerate() {
        buf[i] = b;
    }
    // SAFETY: buf is MAXPGPATH bytes and NUL-terminated (zero-initialized).
    unsafe { pg_diff_pf_canonicalize(buf.as_mut_ptr()) };
    c_buf_to_string(&buf)
}

// ------------------------------------------------------------------ arms

fn arm_bitutils32(word: u32) {
    let mut c = [0u64; 6];
    // SAFETY: `c` is the 6-slot out array the C entry documents.
    unsafe { pg_diff_pf_bitutils32(word, c.as_mut_ptr()) };
    assert_eq!(pg_bitutils::pg_popcount32(word) as u64, c[0], "popcount32");
    assert_eq!(pg_bitutils::pg_ceil_log2_32(word) as u64, c[1], "ceil_log2_32");
    if word != 0 {
        assert_eq!(
            pg_bitutils::pg_leftmost_one_pos32(word) as u64,
            c[2],
            "leftmost_one_pos32"
        );
        assert_eq!(
            pg_bitutils::pg_rightmost_one_pos32(word) as u64,
            c[3],
            "rightmost_one_pos32"
        );
        assert_eq!(pg_bitutils::pg_prevpower2_32(word) as u64, c[4], "prevpower2_32");
    }
    if word > 0 && word <= u32::MAX / 2 + 1 {
        assert_eq!(pg_bitutils::pg_nextpower2_32(word) as u64, c[5], "nextpower2_32");
    }
}

fn arm_bitutils64(word: u64) {
    let mut c = [0u64; 6];
    // SAFETY: `c` is the 6-slot out array the C entry documents.
    unsafe { pg_diff_pf_bitutils64(word, c.as_mut_ptr()) };
    assert_eq!(pg_bitutils::pg_popcount64(word) as u64, c[0], "popcount64");
    assert_eq!(pg_bitutils::pg_ceil_log2_64(word), c[1], "ceil_log2_64");
    if word != 0 {
        assert_eq!(
            pg_bitutils::pg_leftmost_one_pos64(word) as u64,
            c[2],
            "leftmost_one_pos64"
        );
        assert_eq!(
            pg_bitutils::pg_rightmost_one_pos64(word) as u64,
            c[3],
            "rightmost_one_pos64"
        );
        assert_eq!(pg_bitutils::pg_prevpower2_64(word), c[4], "prevpower2_64");
    }
    if word > 0 && word <= u64::MAX / 2 + 1 {
        assert_eq!(pg_bitutils::pg_nextpower2_64(word), c[5], "nextpower2_64");
    }
}

fn arm_rotate32(word: u32, n_raw: u32) {
    let n = (n_raw % 31 + 1) as i32; // 1..=31, see DOMAIN CARVES
    // SAFETY: n is inside the C-defined 0 < n < 32 domain.
    let (cr, cl) = unsafe {
        (
            pg_diff_pf_rotate_right32(word, n),
            pg_diff_pf_rotate_left32(word, n),
        )
    };
    assert_eq!(pg_bitutils::pg_rotate_right32(word, n), cr, "rotate_right32");
    assert_eq!(pg_bitutils::pg_rotate_left32(word, n), cl, "rotate_left32");
}

fn arm_popcount(buf: &[u8], skip: usize, mask: u8) {
    let s = skip.min(buf.len());
    let sl = &buf[s..];
    // SAFETY: sl.len() bytes are readable at sl.as_ptr(); len fits an i32.
    let (c_pop, c_mask) = unsafe {
        (
            pg_diff_pf_popcount(sl.as_ptr(), sl.len() as i32),
            pg_diff_pf_popcount_masked(sl.as_ptr(), sl.len() as i32, mask),
        )
    };
    assert_eq!(pg_bitutils::pg_popcount(sl), c_pop, "pg_popcount");
    assert_eq!(
        pg_bitutils::pg_popcount_masked(sl, mask),
        c_mask,
        "pg_popcount_masked"
    );
}

fn arm_crc32c(init: u32, buf: &[u8]) {
    // SAFETY: buf.len() bytes are readable at buf.as_ptr().
    let c = unsafe { pg_diff_pf_crc32c_sb8(init, buf.as_ptr(), buf.len()) };
    assert_eq!(crc32c::pg_comp_crc32c_sb8(init, buf), c, "crc32c sb8");
    // The shipped dispatch entry (hardware arm where build/host provide it)
    // must agree with the portable oracle byte-for-byte.
    assert_eq!(crc32c::pg_comp_crc32c(init, buf), c, "crc32c dispatch");
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: guarded by the runtime feature probe.
        let hw = unsafe { crc32c::pg_comp_crc32c_armv8(init, buf) };
        assert_eq!(hw, c, "crc32c armv8");
    }
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        // SAFETY: guarded by the runtime feature probe.
        let hw = unsafe { crc32c::pg_comp_crc32c_sse42(init, buf) };
        assert_eq!(hw, c, "crc32c sse42");
    }
    // FIN_CRC32C / INIT_CRC32C parity (the whole pipeline, not just COMP).
    assert_eq!(crc32c::fin_crc32c(c), c ^ 0xFFFF_FFFF, "fin_crc32c");
    assert_eq!(crc32c::CRC32C_INIT, 0xFFFF_FFFF, "CRC32C_INIT");
}

fn arm_crc32_legacy(buf: &[u8]) {
    // SAFETY: buf.len() bytes are readable at buf.as_ptr().
    let (ct, cl) = unsafe {
        (
            pg_diff_pf_crc32_traditional(buf.as_ptr(), buf.len()),
            pg_diff_pf_crc32_legacy(buf.as_ptr(), buf.len()),
        )
    };
    assert_eq!(crc32c::traditional_crc32(buf), ct, "traditional_crc32");
    assert_eq!(crc32c::legacy_crc32_lexeme(buf), cl, "legacy_crc32_lexeme");
}

/// Truncate at the first interior NUL so both sides see the same logical
/// string (C's is NUL-terminated; the Rust API takes the byte slice).
fn c_string_from(bytes: &[u8]) -> (Vec<u8>, CString) {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    let body = bytes[..end].to_vec();
    let cs = CString::new(body.clone()).expect("no interior NUL by construction");
    (body, cs)
}

fn arm_strcase(a: &[u8], b: &[u8], n: usize) {
    let (ra, ca) = c_string_from(a);
    let (rb, cb) = c_string_from(b);
    // SAFETY: both pointers are NUL-terminated CString buffers.
    let (c_cmp, c_ncmp) = unsafe {
        (
            pg_diff_pf_strcasecmp(ca.as_ptr().cast(), cb.as_ptr().cast()),
            pg_diff_pf_strncasecmp(ca.as_ptr().cast(), cb.as_ptr().cast(), n),
        )
    };
    let r_cmp = pgstrcasecmp::pg_strcasecmp(&ra, &rb);
    let r_ncmp = pgstrcasecmp::pg_strncasecmp(&ra, &rb, n);
    assert_eq!(r_cmp, c_cmp, "pg_strcasecmp");
    assert_eq!(r_ncmp, c_ncmp, "pg_strncasecmp n={n}");
    // Sign plane: every caller only tests the sign, so pin that too.
    assert_eq!(r_cmp.signum(), c_cmp.signum(), "pg_strcasecmp sign");
    assert_eq!(r_ncmp.signum(), c_ncmp.signum(), "pg_strncasecmp sign");
}

fn arm_casefold(ch: u8) {
    // SAFETY: plain scalar C calls.
    unsafe {
        assert_eq!(
            pgstrcasecmp::pg_tolower(ch) as i32,
            pg_diff_pf_tolower(ch as i32),
            "pg_tolower({ch})"
        );
        assert_eq!(
            pgstrcasecmp::pg_toupper(ch) as i32,
            pg_diff_pf_toupper(ch as i32),
            "pg_toupper({ch})"
        );
        assert_eq!(
            pgstrcasecmp::pg_ascii_tolower(ch) as i32,
            pg_diff_pf_ascii_tolower(ch as i32),
            "pg_ascii_tolower({ch})"
        );
        assert_eq!(
            pgstrcasecmp::pg_ascii_toupper(ch) as i32,
            pg_diff_pf_ascii_toupper(ch as i32),
            "pg_ascii_toupper({ch})"
        );
    }
}

fn arm_pathpred(a: &str, b: &str) {
    let (ca, cb) = (cstr(a), cstr(b));
    // SAFETY: both pointers are NUL-terminated CString buffers.
    unsafe {
        let c_first = pg_diff_pf_first_dir_sep(ca.as_ptr().cast());
        let c_last = pg_diff_pf_last_dir_sep(ca.as_ptr().cast());
        let c_var = pg_diff_pf_first_path_var_sep(ca.as_ptr().cast());
        let c_par = pg_diff_pf_contains_parent_ref(ca.as_ptr().cast());
        let c_below = pg_diff_pf_rel_below_cwd(ca.as_ptr().cast());
        let c_prefix = pg_diff_pf_prefix_of(ca.as_ptr().cast(), cb.as_ptr().cast());

        assert_eq!(
            pg_path::first_dir_separator(a).map_or(-1i64, |i| i as i64),
            c_first,
            "first_dir_separator"
        );
        assert_eq!(
            pg_path::last_dir_separator(a).map_or(-1i64, |i| i as i64),
            c_last,
            "last_dir_separator"
        );
        assert_eq!(
            pg_path::first_path_var_separator(a).map_or(-1i64, |i| i as i64),
            c_var,
            "first_path_var_separator"
        );
        assert_eq!(
            pg_path::path_contains_parent_reference(a) as i32,
            c_par,
            "path_contains_parent_reference"
        );
        assert_eq!(
            pg_path::path_is_relative_and_below_cwd(a) as i32,
            c_below,
            "path_is_relative_and_below_cwd"
        );
        assert_eq!(
            pg_path::path_is_prefix_of_path(a, b) as i32,
            c_prefix,
            "path_is_prefix_of_path"
        );
    }
    // is_absolute_path is a C macro (port.h): assert the shipped helper
    // matches its Unix definition.
    assert_eq!(
        pg_path::is_absolute_path(a),
        a.as_bytes().first() == Some(&b'/'),
        "is_absolute_path"
    );
}

fn arm_canon(s: &str) {
    assert_eq!(
        pg_path::canonicalize_path(s),
        c_canonicalize(s),
        "canonicalize_path"
    );
}

fn arm_join(head: &str, tail: &str) {
    let (ch, ct) = (cstr(head), cstr(tail));
    let mut buf = vec![0u8; MAXPGPATH];
    // SAFETY: buf is a MAXPGPATH output area, exactly the C contract.
    unsafe { pg_diff_pf_join(ch.as_ptr().cast(), ct.as_ptr().cast(), buf.as_mut_ptr()) };
    let c_joined = c_buf_to_string(&buf);
    let r_joined = pg_path::join_path_components(head, tail);
    assert_eq!(r_joined, c_joined, "join_path_components");

    // get_parent_directory over that result (in-place C API).
    let mut pbuf = vec![0u8; MAXPGPATH];
    for (i, &b) in r_joined.as_bytes().iter().enumerate() {
        pbuf[i] = b;
    }
    // SAFETY: pbuf is MAXPGPATH bytes and NUL-terminated.
    unsafe { pg_diff_pf_parent_dir(pbuf.as_mut_ptr()) };
    assert_eq!(
        pg_path::get_parent_directory(&r_joined),
        c_buf_to_string(&pbuf),
        "get_parent_directory"
    );
}

fn arm_relpath(which: u8, my_exec_path: &str) {
    let w = (which % 11) as i32;
    let cp = cstr(my_exec_path);
    let mut buf = vec![0u8; MAXPGPATH];
    // SAFETY: buf is a MAXPGPATH output area, exactly the C contract.
    unsafe { pg_diff_pf_get_rel_path(w, cp.as_ptr().cast(), buf.as_mut_ptr()) };
    let c_out = c_buf_to_string(&buf);
    let r_out = match w {
        0 => pg_path::get_share_path(my_exec_path),
        1 => pg_path::get_etc_path(my_exec_path),
        2 => pg_path::get_include_path(my_exec_path),
        3 => pg_path::get_pkginclude_path(my_exec_path),
        4 => pg_path::get_includeserver_path(my_exec_path),
        5 => pg_path::get_lib_path(my_exec_path),
        6 => pg_path::get_pkglib_path(my_exec_path),
        7 => pg_path::get_locale_path(my_exec_path),
        8 => pg_path::get_doc_path(my_exec_path),
        9 => pg_path::get_html_path(my_exec_path),
        _ => pg_path::get_man_path(my_exec_path),
    };
    assert_eq!(r_out, c_out, "get_*_path which={w}");
}

/// Page image from fuzz bytes with the header pointers clamped into the page
/// (see DOMAIN CARVES); their ORDER stays free so mask_unused_space's
/// validity check is genuinely fuzzed.
fn build_page(payload: &[u8]) -> Box<AlignedPage> {
    let mut p = AlignedPage::new();
    if !payload.is_empty() {
        for (i, slot) in p.0.iter_mut().enumerate() {
            *slot = payload[i % payload.len()];
        }
    }
    let lower_off = core::mem::offset_of!(PageHeaderData, pd_lower);
    let upper_off = core::mem::offset_of!(PageHeaderData, pd_upper);
    let special_off = core::mem::offset_of!(PageHeaderData, pd_special);
    let m = BLCKSZ as u32 + 1;
    let lo = (u16::from_le_bytes([p_get(payload, 0), p_get(payload, 1)]) as u32 % m) as u16;
    let up = (u16::from_le_bytes([p_get(payload, 2), p_get(payload, 3)]) as u32 % m) as u16;
    let sp = (u16::from_le_bytes([p_get(payload, 4), p_get(payload, 5)]) as u32 % m) as u16;
    set_hdr_u16(&mut p, lower_off, lo);
    set_hdr_u16(&mut p, upper_off, up);
    set_hdr_u16(&mut p, special_off, sp);
    p
}

fn arm_maskhdr(payload: &[u8]) {
    for which in 0..3u8 {
        let mut r = build_page(payload);
        let mut c = r.clone_box();
        // SAFETY: c.0 is a full BLCKSZ MAXALIGNed page image.
        unsafe {
            match which {
                0 => pg_diff_pf_mask_page_lsn_and_checksum(c.0.as_mut_ptr()),
                1 => pg_diff_pf_mask_page_hint_bits(c.0.as_mut_ptr()),
                _ => pg_diff_pf_mask_page_content(c.0.as_mut_ptr()),
            }
        }
        match which {
            0 => bufmask::mask_page_lsn_and_checksum(&mut r.0),
            1 => bufmask::mask_page_hint_bits(&mut r.0),
            _ => bufmask::mask_page_content(&mut r.0),
        }
        assert!(r.0 == c.0, "bufmask header arm {which} page image");
    }
}

/// The invalid-page predicate, transcribed from the vendored C
/// `mask_unused_space` sanity check (bufmask.c) — an INDEPENDENT third
/// statement of the condition, so asserting C's raise against it is a real
/// comparison of C's check and not a tautology.
fn page_is_invalid(p: &AlignedPage) -> bool {
    let lo = get_hdr_u16(p, core::mem::offset_of!(PageHeaderData, pd_lower)) as usize;
    let up = get_hdr_u16(p, core::mem::offset_of!(PageHeaderData, pd_upper)) as usize;
    let sp = get_hdr_u16(p, core::mem::offset_of!(PageHeaderData, pd_special)) as usize;
    lo > up || sp < up || lo < SizeOfPageHeaderData || sp > BLCKSZ
}

fn arm_maskunused(payload: &[u8]) {
    let mut r = build_page(payload);
    let mut c = r.clone_box();
    let expect_raise = page_is_invalid(&r);
    // SAFETY: c.0 is a full BLCKSZ MAXALIGNed page image.
    let c_raised = unsafe { pg_diff_pf_mask_unused_space(c.0.as_mut_ptr()) } != 0;

    // ERROR VERDICT plane, half 1 (runs in EVERY build, fuzz included):
    // the vendored C raise must agree with the independently transcribed
    // predicate on every input.
    assert_eq!(c_raised, expect_raise, "C mask_unused_space raise verdict");

    // ERROR VERDICT plane, half 2: the Rust port must raise on exactly the
    // same inputs. HARNESS CARVE (2026-08-01): libfuzzer-sys installs a
    // panic hook that ABORTS the process, so catch_unwind cannot observe a
    // Rust panic under `cargo fuzz` — the abort is reported as a crash even
    // though raising IS the correct C-parity behavior. The unwinding
    // comparison therefore runs under `cargo test` (where it is exact) over
    // the committed corpus and the witness pairs; the fuzz build compares
    // the C half and the value plane, and skips the raising inputs.
    #[cfg(not(fuzzing))]
    {
        let r_raised = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bufmask::mask_unused_space(&mut r.0)
        }))
        .is_err();
        assert_eq!(r_raised, c_raised, "mask_unused_space error verdict");
        if !r_raised {
            assert!(r.0 == c.0, "mask_unused_space page image");
        }
        return;
    }
    #[cfg(fuzzing)]
    if !expect_raise {
        bufmask::mask_unused_space(&mut r.0);
        assert!(r.0 == c.0, "mask_unused_space page image");
    }
}

fn arm_masklp(payload: &[u8]) {
    let mut r = build_page(payload);
    // mask_lp_flags walks (pd_lower - SizeOfPageHeaderData)/4 line pointers
    // with NO bound check in C — clamp pd_lower into the page (carve).
    let lower_off = core::mem::offset_of!(PageHeaderData, pd_lower);
    let clamped = get_hdr_u16(&r, lower_off).clamp(SizeOfPageHeaderData as u16, BLCKSZ as u16);
    set_hdr_u16(&mut r, lower_off, clamped);
    let mut c = r.clone_box();
    // SAFETY: c.0 is a full BLCKSZ MAXALIGNed page image whose pd_lower
    // names a line-pointer array inside the page.
    unsafe { pg_diff_pf_mask_lp_flags(c.0.as_mut_ptr()) };
    bufmask::mask_lp_flags(&mut r.0);
    assert!(r.0 == c.0, "mask_lp_flags page image");
}

// ------------------------------------------------------------- dispatcher

pub fn portfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };

    match sel % 15 {
        0 => arm_bitutils32(u32_at(payload, 0)),
        1 => arm_bitutils64(u64_at(payload, 0)),
        2 => arm_rotate32(u32_at(payload, 0), u32_at(payload, 1)),
        3 => {
            let skip = p_get(payload, 0) as usize % 9;
            let mask = p_get(payload, 1);
            arm_popcount(&payload[payload.len().min(2)..], skip, mask);
        }
        4 => {
            // [init u32][skip][data...] — `skip` walks the buffer start
            // through all 8 alignments so both the sb8 4-byte pre-align loop
            // and the armv8 1/2/4-byte pre-align arms are reachable.
            let init = u32_at(payload, 0);
            let rest = &payload[payload.len().min(4)..];
            let skip = p_get(rest, 0) as usize % 9;
            let body = &rest[rest.len().min(1)..];
            arm_crc32c(init, &body[body.len().min(skip)..]);
        }
        5 => arm_crc32_legacy(payload),
        6 => {
            // [len_a][n][a bytes][b bytes]
            let la = p_get(payload, 0) as usize;
            let n = p_get(payload, 1) as usize;
            let rest = &payload[payload.len().min(2)..];
            let split = la.min(rest.len());
            arm_strcase(&rest[..split], &rest[split..], n);
        }
        7 => arm_casefold(p_get(payload, 0)),
        8 => {
            let la = p_get(payload, 0) as usize;
            let rest = &payload[payload.len().min(1)..];
            let split = la.min(rest.len()).min(256);
            arm_pathpred(
                &path_str(&rest[..split], 256),
                &path_str(&rest[split..], 256),
            );
        }
        9 => arm_canon(&path_str(payload, 512)),
        10 => {
            let la = p_get(payload, 0) as usize;
            let rest = &payload[payload.len().min(1)..];
            let split = la.min(rest.len()).min(200);
            arm_join(
                &path_str(&rest[..split], 200),
                &path_str(&rest[split..], 200),
            );
        }
        11 => {
            let which = p_get(payload, 0);
            arm_relpath(which, &path_str(&payload[payload.len().min(1)..], 400));
        }
        12 => arm_maskhdr(payload),
        13 => arm_maskunused(payload),
        _ => arm_masklp(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed sweep: every arm executes against the C oracle with real values
    /// on every `cargo test` run (link + shim smoke).
    #[test]
    fn arm_sweep() {
        let _serial = crate::c_oracle_serial();
        let payloads: [&[u8]; 8] = [
            b"",
            b"\x00",
            b"\x01\x02\x03\x04\x05\x06\x07\x08",
            b"/usr/local/pgsql/bin/postgres",
            b"../a/./b//c/..",
            b"SELECT\x00select",
            &[0xFFu8; 40],
            &[0xAAu8; 97],
        ];
        for sel in 0u8..15 {
            for p in payloads {
                let mut data = vec![sel];
                data.extend_from_slice(p);
                portfam_diff(&data);
            }
        }
    }

    /// Single-field witness pairs (skill obligation): every field that merges
    /// into a compared verdict must be shown to steer it on its own.
    #[test]
    fn single_field_witness_pairs() {
        let _serial = crate::c_oracle_serial();

        // bit-position ops: each single-bit word has its own image.
        let mut images = std::collections::HashSet::new();
        for bit in 0..32u32 {
            let w = 1u32 << bit;
            arm_bitutils32(w);
            assert!(
                images.insert((
                    pg_bitutils::pg_leftmost_one_pos32(w),
                    pg_bitutils::pg_rightmost_one_pos32(w),
                    pg_bitutils::pg_prevpower2_32(w),
                )),
                "single-bit delta not witnessed at bit {bit}"
            );
        }
        // rotate: the shift dimension steers the image on its own.
        let mut rimages = std::collections::HashSet::new();
        for n in 1..32u32 {
            arm_rotate32(0x8000_0001, n - 1);
            assert!(
                rimages.insert(pg_bitutils::pg_rotate_right32(0x8000_0001, n as i32)),
                "shift {n} delta not witnessed"
            );
        }
        // crc: flipping any single input byte must move the checksum.
        let base = [0x11u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99];
        let mut cimages = std::collections::HashSet::new();
        assert!(cimages.insert(crc32c::pg_comp_crc32c_sb8(crc32c::CRC32C_INIT, &base)));
        for i in 0..base.len() {
            let mut v = base;
            v[i] ^= 1;
            arm_crc32c(crc32c::CRC32C_INIT, &v);
            arm_crc32_legacy(&v);
            assert!(
                cimages.insert(crc32c::pg_comp_crc32c_sb8(crc32c::CRC32C_INIT, &v)),
                "byte {i} delta not witnessed by crc32c"
            );
        }
        // crc: every buffer start alignment (the sb8 pre-align loop and the
        // armv8 1/2/4-byte pre-align arms key off it).
        let wide = [0x5Au8; 40];
        for skip in 0..9usize {
            arm_crc32c(crc32c::CRC32C_INIT, &wide[skip..]);
            arm_crc32_legacy(&wide[skip..]);
        }
        // popcount: every byte position contributes independently, at every
        // start offset (the C word loop keys off buffer alignment).
        for i in 0..24usize {
            let mut buf = [0u8; 24];
            buf[i] = 0x0F;
            for skip in 0..8usize {
                arm_popcount(&buf, skip, 0x33);
            }
            assert_eq!(pg_bitutils::pg_popcount(&buf), 4);
        }
        // bufmask: each header field independently steers the masked image.
        for field in 0..6usize {
            let mut payload = vec![0u8; 64];
            payload[field] = 0x20;
            arm_maskhdr(&payload);
            arm_maskunused(&payload);
            arm_masklp(&payload);
        }
        // casefold: every byte class.
        for ch in [b'A', b'Z', b'a', b'z', b'0', 0x7f, 0x80, 0xC0, 0xE0, 0xFF] {
            arm_casefold(ch);
        }
        // path: single-character deltas around each structural byte.
        for base in ["/a/b", "a/b", "../a", "a/..", "//a//b//", "/"] {
            arm_canon(base);
            arm_pathpred(base, "/a");
            arm_join(base, "c");
            arm_relpath(0, base);
            for i in 0..base.len() {
                let mut v: Vec<u8> = base.as_bytes().to_vec();
                v[i] = b'.';
                let s = String::from_utf8(v).unwrap();
                arm_canon(&s);
                arm_pathpred(&s, base);
            }
        }
    }

    /// EXHAUSTIVE-DIFF a0 (cheap tier, in the normal suite): the FULL u8
    /// casefold domain and the FULL one-char x one-char compare domain.
    #[test]
    fn exhaustive_casefold_and_onechar_compare() {
        let _serial = crate::c_oracle_serial();
        let mut n = 0u64;
        for ch in 0..=255u8 {
            arm_casefold(ch);
            n += 1;
        }
        assert_eq!(n, 256, "casefold sweep must cover the full u8 domain");

        // 0x00 is the string terminator on the C side, so the one-char
        // string domain is 1..=255 on each side.
        let mut m = 0u64;
        for a in 1..=255u8 {
            for b in 1..=255u8 {
                arm_strcase(&[a], &[b], 2);
                m += 1;
            }
        }
        assert_eq!(m, 255 * 255, "one-char compare sweep domain count");
    }

    /// EXHAUSTIVE-DIFF a0: the FULL u32 word domain over the pg_bitutils
    /// word ops (2^32 dual calls). Run explicitly and bank the log:
    ///   cargo test -p decoder_fuzz --release exhaustive_bitutils32 \
    ///     -- --ignored --nocapture
    #[test]
    #[ignore = "a0 full-domain sweep: minutes-scale, run explicitly"]
    fn exhaustive_bitutils32() {
        let _serial = crate::c_oracle_serial();
        let t0 = std::time::Instant::now();
        let mut n: u64 = 0;
        let mut w: u32 = 0;
        loop {
            arm_bitutils32(w);
            n += 1;
            if w == u32::MAX {
                break;
            }
            w += 1;
        }
        assert_eq!(n, 1u64 << 32, "must cover the FULL u32 domain");
        println!(
            "a0 exhaustive_bitutils32: {n} words, {:?} wall, arch {}",
            t0.elapsed(),
            std::env::consts::ARCH
        );
    }

    /// EXHAUSTIVE-DIFF a0: the FULL rotate shift domain (1..=31) crossed with
    /// 2^27 words spread over every high-bit class — 2^32 compared pairs.
    #[test]
    #[ignore = "a0 full-domain sweep: minutes-scale, run explicitly"]
    fn exhaustive_rotate32_shifts() {
        let _serial = crate::c_oracle_serial();
        let t0 = std::time::Instant::now();
        let mut n: u64 = 0;
        for hi in 0..32u32 {
            for lo in 0..(1u32 << 22) {
                let w = (hi << 27) | lo;
                for shift in 1..32u32 {
                    arm_rotate32(w, shift - 1);
                    n += 1;
                }
            }
        }
        assert_eq!(n, 32u64 * (1 << 22) * 31, "rotate sweep domain count");
        println!("a0 exhaustive_rotate32_shifts: {n} pairs, {:?}", t0.elapsed());
    }

    /// EXHAUSTIVE-DIFF a0: all 2-byte x 2-byte case-compare pairs (~2^32).
    #[test]
    #[ignore = "a0 full-domain sweep: minutes-scale, run explicitly"]
    fn exhaustive_two_byte_compare() {
        let _serial = crate::c_oracle_serial();
        let t0 = std::time::Instant::now();
        let mut n: u64 = 0;
        for a0 in 1..=255u8 {
            for a1 in 1..=255u8 {
                for b0 in 1..=255u8 {
                    for b1 in 1..=255u8 {
                        arm_strcase(&[a0, a1], &[b0, b1], 3);
                        n += 1;
                    }
                }
            }
        }
        assert_eq!(n, 255u64 * 255 * 255 * 255, "two-byte compare domain count");
        println!("a0 exhaustive_two_byte_compare: {n} pairs, {:?}", t0.elapsed());
    }

    /// Replay every checked-in seed (catches shim/link drift before the
    /// fleet campaign). Corpus is COMMITTED.
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/portfam_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/portfam_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                portfam_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}
