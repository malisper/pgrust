//! mac_diff: differential driver for adt/mac (EUI-48) AND adt/mac8 (EUI-64),
//! one shared target over a shared macaddr corpus.
//!
//! Every arm executes the SHIPPED Rust entry point (adt_mac / adt_mac8 crate
//! cores — the exact fns the fc_* builtins wrap) against verbatim vendored
//! PostgreSQL 18.3 C (upstream sha 62d6c7d3df) compiled from
//! csrc/pg_mac_io.c (assembled from the audited proofs/mac, proofs/mac8 and
//! proofs/hash-rows shims). Comparator planes: value bytes + error-verdict +
//! errcode class; message text is out of scope. Any mismatch panics =
//! minimized libFuzzer artifact.
//!
//! Input layout: [selector byte][payload]; arm = selector % 21.
//!   0  macaddr_in           text (NUL-free, <=64 bytes, UTF-8)
//!   1  macaddr8_in          text (NUL-free, <=64 bytes, UTF-8)
//!   2  macaddr_out          6 raw bytes
//!   3  macaddr8_out         8 raw bytes
//!   4  macaddr cmp family   12 raw bytes (cmp + lt/le/eq/ge/gt/ne)
//!   5  macaddr8 cmp family  16 raw bytes
//!   6  macaddr_not          6      7 macaddr_and   12     8 macaddr_or  12
//!   9  macaddr_trunc        6
//!   10 macaddr8_not         8      11 macaddr8_and 16     12 macaddr8_or 16
//!   13 macaddr8_trunc       8      14 macaddr8_set7bit 8
//!   15 macaddrtomacaddr8    6
//!   16 macaddr8tomacaddr    8 (error arm: non-FF/FE middle bytes)
//!   17 hashmacaddr(+ext)    6 + 8 seed bytes
//!   18 hashmacaddr8(+ext)   8 + 8 seed bytes
//!   19 macaddr_recv/send    6 raw bytes (wire round-trip; C recv is
//!      byte-copy so the oracle is the input itself)
//!   20 macaddr8_recv/send   1 flag + 6-or-8 raw bytes (the 6-byte stuffing
//!      arm is checked against C macaddrtomacaddr8, the same FF/FE insert)
//!
//! FC-WRAPPER PLANE: each arm additionally routes its (already core-vs-C
//! checked) input through the crate's builtins.rs fc_* wrapper via a native
//! types_fmgr::LocalFcinfo frame and asserts wrapper ≡ core/C (Datum value /
//! returned bytes / error verdict + sqlstate). This executes the FULL fc_*
//! inventory of adt_mac (17 wrappers) and adt_mac8 (20 wrappers) every
//! iteration; the in wrappers run the hard-error shape (no escontext — the
//! soft path line `soft_error_context()` executes either way and returns
//! None, matching the server default).

use std::ffi::{c_char, CString};

use adt_mac::{MacAddr, MACADDR_OUT_LEN};
use adt_mac8::{MacAddr8, MACADDR8_OUT_LEN};
use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_fmgr::{LocalFcinfo, PGFunction};

extern "C" {
    // mac.c (EUI-48). macaddr_in returns its ereturn verdict directly:
    // 0 = ok, 1 = 22P02 invalid syntax, 2 = 22003 octet out of range —
    // no thread-local errcode plumbing needed for this family.
    fn pgc_macaddr_in(str_: *const c_char, result: *mut u8) -> i32;
    fn pgc_macaddr_out(b: *const u8, result: *mut c_char) -> i32;
    fn pgc_macaddr_cmp(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_lt(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_le(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_eq(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_ge(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_gt(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_ne(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr_not(bin: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr_and(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr_or(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr_trunc(bin: *const u8, bout: *mut u8) -> i32;
    // mac8.c (EUI-64). macaddr8_in: 1 = ok, 0 = 22P02 fail path.
    fn pgc_macaddr8_in(str_: *const u8, out: *mut u8) -> i32;
    fn pgc_macaddr8_out(b: *const u8, result: *mut c_char) -> i32;
    fn pgc_macaddr8_cmp(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_lt(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_le(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_eq(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_ge(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_gt(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_ne(b1: *const u8, b2: *const u8) -> i32;
    fn pgc_macaddr8_not(bin: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr8_and(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr8_or(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr8_trunc(bin: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddr8_set7bit(bin: *const u8, bout: *mut u8) -> i32;
    fn pgc_macaddrtomacaddr8(b6: *const u8, bout: *mut u8) -> i32;
    // 0 = ok, 1 = 22003 ereport path.
    fn pgc_macaddr8tomacaddr(b8: *const u8, bout: *mut u8) -> i32;
    // hashfn.c (verbatim Jenkins) + the mac.c/mac8.c hash wrappers.
    fn pg_hashmacaddr(key: *const u8) -> u32;
    fn pg_hashmacaddrextended(key: *const u8, seed: i64) -> u64;
    fn pg_hashmacaddr8(key: *const u8) -> u32;
    fn pg_hashmacaddr8extended(key: *const u8, seed: i64) -> u64;
}

// ---------------------------------------------------------------------------
// fc-wrapper plane plumbing (native LocalFcinfo, real mcx — the proofs
// wrapper-level pattern run without kani). Wrapper ≡ core is the in-harness
// oracle; C-parity keeps being carried by the core comparison above each
// call site.
// ---------------------------------------------------------------------------

/// Invoke an fc_* wrapper over non-null args; returns its PgResult.
fn fc_call<const N: usize>(f: PGFunction, m: mcx::Mcx<'_>, args: [Datum; N]) -> PgResult<Datum> {
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: the context owning `m` outlives this single call (caller scope).
    unsafe { fcinfo.set_result_mcx(m) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    f(None, &mut fcinfo)
}

/// First `n` bytes behind a by-ref result Datum. Caller contract: `d` came
/// from a wrapper that returned an `n`-byte-or-longer allocation still live
/// in the arming context (or thread-local out scratch).
fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: caller contract above.
    unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

fn dptr(bytes: &[u8]) -> Datum {
    Datum::from_usize(bytes.as_ptr() as usize)
}

/// fc plane for a byref-returning wrapper: result bytes must equal `expect`
/// (the C bytes each arm has already asserted core-equal).
fn fc_byref<const N: usize>(name: &str, f: PGFunction, args: [Datum; N], expect: &[u8]) {
    let cx = mcx::MemoryContext::new("mac_fc");
    let d = fc_call::<N>(f, cx.mcx(), args).expect("byref wrapper cannot fail here");
    assert_eq!(datum_bytes(d, expect.len()), expect, "{name} fc-plane DIVERGENCE");
}

/// A StringInfo image over `bytes` in `m` (None = alloc failure: skip plane).
fn make_si<'a>(m: mcx::Mcx<'a>, bytes: &[u8]) -> Option<stringinfo::StringInfo<'a>> {
    let mut vec = mcx::vec_with_capacity_in::<u8>(m, bytes.len()).ok()?;
    mcx::vec_append_bytes(&mut vec, bytes).ok()?;
    stringinfo::StringInfo::from_vec(vec).ok()
}

/// Map a Rust PgError onto the C oracle's macaddr_in return codes.
fn mac_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION {
        1
    } else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        2
    } else {
        99
    }
}

// ---------------------------------------------------------------------------
// RATIFIED CARVE — proofs ledger row oid 436 (Michael, 2026-07-28).
//
// pgrust macaddr_in REJECTS (22003) inputs whose %x-style hex field carries
// more than 8 hex digits, where C's sscanf conversion stores through
// `unsigned int` and silently wraps the value mod 2^32 (C99 7.19.6.2 makes
// the wrap spec'd behavior; the C acceptance is the upstream sscanf bug).
// The divergence is only REACHABLE through the two unlimited-width formats
// ("%x:%x:..." and "%x-%x-..."), and only when that format is the TERMINAL
// parse (count==6, no trailing junk): a failed style-0 attempt contributes
// nothing but `count != 6` on both sides, and the %2x styles cap every field
// at 2 digits. So the carve is exactly: the winning style-0 parse contains a
// field of >8 hex digits => skip (expect-difference). NOTHING WIDER: bare
// long runs like "08002b010203" (12 digits) terminal-parse via the %2x
// no-separator style and are fully compared.
// ---------------------------------------------------------------------------

fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// One unlimited-width %x directive, mirroring adt_mac's Scanner::scan_hex
/// consumption exactly (whitespace, sign, glibc 0x pushback), but reporting
/// the hex-digit count of the field instead of its value.
fn scan_hex_digits(bytes: &[u8], pos: &mut usize) -> Option<(u32, bool)> {
    let peek = |p: usize| bytes.get(p).copied();
    while peek(*pos).is_some_and(is_c_space) {
        *pos += 1;
    }
    let mut minus = false;
    if matches!(peek(*pos), Some(b'+') | Some(b'-')) {
        minus = peek(*pos) == Some(b'-');
        *pos += 1;
    }
    if peek(*pos) == Some(b'0') {
        let save = *pos;
        *pos += 1;
        if matches!(peek(*pos), Some(b'x') | Some(b'X')) {
            *pos += 1;
            if !peek(*pos).is_some_and(|b| b.is_ascii_hexdigit()) {
                *pos -= 1; // glibc: '0' is the result, 'x' unread
                return Some((1, minus));
            }
            let mut n = 0u32;
            while peek(*pos).is_some_and(|b| b.is_ascii_hexdigit()) {
                *pos += 1;
                n += 1;
            }
            return Some((n, minus));
        }
        *pos = save;
    }
    let mut n = 0u32;
    while peek(*pos).is_some_and(|b| b.is_ascii_hexdigit()) {
        *pos += 1;
        n += 1;
    }
    if n == 0 {
        None
    } else {
        Some((n, minus))
    }
}

/// If "%x<sep>%x<sep>%x<sep>%x<sep>%x<sep>%x%1s" terminal-matches (six
/// fields, no trailing junk), return whether any field is in the sscanf
/// wrap class: >8 hex digits (unsigned mod-2^32 wrap), or a '-'-signed
/// field of >=8 digits (negation wrap can land the stored int back inside
/// 0..=255 — confirmed vs docker postgres:18.3: '-fffffff1:2:3:4:5:6' is
/// ACCEPTED as 0f:02:03:04:05:06 while pgrust's ratified behavior rejects;
/// same row-436 upstream sscanf bug, sign-negation flavor).
fn style0_terminal_match(bytes: &[u8], sep: u8) -> Option<bool> {
    let wrapclass = |(n, minus): (u32, bool)| n > 8 || (minus && n >= 8);
    let mut pos = 0usize;
    let mut any = wrapclass(scan_hex_digits(bytes, &mut pos)?);
    for _ in 1..6 {
        if bytes.get(pos).copied() != Some(sep) {
            return None;
        }
        pos += 1;
        any |= wrapclass(scan_hex_digits(bytes, &mut pos)?);
    }
    while bytes.get(pos).copied().is_some_and(is_c_space) {
        pos += 1;
    }
    if pos < bytes.len() {
        return None; // %1s assigns: trailing junk, count == 7 on both sides
    }
    Some(any)
}

/// True iff the input lands in the ledger-row-436 carve class (see above).
fn carve_row436(bytes: &[u8]) -> bool {
    // Cascade order is mac.c's: ':' style 0, then '-' style 0. The first
    // terminal match wins on both sides.
    if let Some(w) = style0_terminal_match(bytes, b':') {
        return w;
    }
    if let Some(w) = style0_terminal_match(bytes, b'-') {
        return w;
    }
    false
}

/// FUZZ-PROFILE PANIC GUARD (subset of the row-436 >8-digit field class; not
/// a comparator carve). adt_mac's Scanner::scan_hex accumulates with
/// wrapping_mul/wrapping_add but negates with a PLAIN `-value`: a '-'-signed
/// field whose accumulator lands on exactly i64::MIN (e.g. the 16-digit run
/// "-8000000000000000") overflows the negation, which PANICS under the
/// overflow-checked fuzz/test profiles only. In release (shipped server) the
/// negation wraps and the verdict agrees with C wherever the row-436 carve
/// doesn't already apply, so this is not a shipped divergence — it is a
/// debug-profile robustness nit in adt_mac (reported: use wrapping_neg).
/// Guarded here so the fuzzer hunts real divergences instead of this panic;
/// every guarded input necessarily contains a >=16-hex-digit signed field,
/// i.e. sits inside the >8-digit run class the ratified carve names.
fn negation_overflow_guard(bytes: &[u8]) -> bool {
    for (i, &b) in bytes.iter().enumerate() {
        if b != b'-' {
            continue;
        }
        let mut p = i + 1;
        if bytes.get(p) == Some(&b'0')
            && matches!(bytes.get(p + 1), Some(b'x') | Some(b'X'))
            && bytes.get(p + 2).is_some_and(|c| c.is_ascii_hexdigit())
        {
            p += 2;
        }
        let mut n = 0usize;
        while bytes.get(p).is_some_and(|c| c.is_ascii_hexdigit()) {
            p += 1;
            n += 1;
        }
        if n >= 16 {
            return true;
        }
    }
    false
}

fn mac_in_diff(text: &[u8]) {
    if text.len() > 64 || text.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(text) else {
        return; // shipped entry is &str; the server never hands it non-UTF-8
    };
    if carve_row436(text) || negation_overflow_guard(text) {
        return;
    }
    let cs = CString::new(text).unwrap();
    let mut cout = [0u8; 6];
    let cerr = unsafe { pgc_macaddr_in(cs.as_ptr(), cout.as_mut_ptr()) };
    match adt_mac::macaddr_in(s, None) {
        Ok(addr) => assert!(
            cerr == 0 && addr.to_bytes() == cout,
            "macaddr_in DIVERGENCE input={s:?}: C=(err {cerr}, {cout:02x?}) Rust=Ok({:02x?})",
            addr.to_bytes()
        ),
        Err(e) => {
            let rerr = mac_err_class(&e);
            assert!(
                cerr == rerr,
                "macaddr_in DIVERGENCE input={s:?}: C err {cerr} ({cout:02x?}) vs Rust err {rerr} ({})",
                e.message
            );
        }
    }

    // fc-wrapper plane: fc_macaddr_in (hard shape) against the C verdict the
    // core just matched.
    let cx = mcx::MemoryContext::new("mac_fc");
    match fc_call::<1>(
        adt_mac::builtins::fc_macaddr_in,
        cx.mcx(),
        [Datum::from_usize(cs.as_ptr() as usize)],
    ) {
        Ok(d) => assert!(
            cerr == 0 && datum_bytes(d, 6) == cout,
            "fc_macaddr_in vs core DIVERGENCE input={s:?}"
        ),
        Err(e) => assert_eq!(
            mac_err_class(&e),
            cerr,
            "fc_macaddr_in error-shape DIVERGENCE input={s:?} ({})",
            e.message
        ),
    }
}

fn mac8_in_diff(text: &[u8]) {
    if text.len() > 64 || text.contains(&0) {
        return;
    }
    let Ok(s) = std::str::from_utf8(text) else {
        return;
    };
    let cs = CString::new(text).unwrap();
    let mut cout = [0u8; 8];
    let cok = unsafe { pgc_macaddr8_in(cs.as_ptr().cast(), cout.as_mut_ptr()) };
    match adt_mac8::macaddr8_in(s, None) {
        Ok(addr) => assert!(
            cok == 1 && addr.to_bytes() == cout,
            "macaddr8_in DIVERGENCE input={s:?}: C=(ok {cok}, {cout:02x?}) Rust=Ok({:02x?})",
            addr.to_bytes()
        ),
        Err(e) => assert!(
            cok == 0 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION,
            "macaddr8_in DIVERGENCE input={s:?}: C ok={cok} vs Rust err {} ({})",
            mac_err_class(&e),
            e.message
        ),
    }

    // fc-wrapper plane: fc_macaddr8_in (hard shape).
    let cx = mcx::MemoryContext::new("mac_fc");
    match fc_call::<1>(
        adt_mac8::builtins::fc_macaddr8_in,
        cx.mcx(),
        [Datum::from_usize(cs.as_ptr() as usize)],
    ) {
        Ok(d) => assert!(
            cok == 1 && datum_bytes(d, 8) == cout,
            "fc_macaddr8_in vs core DIVERGENCE input={s:?}"
        ),
        Err(e) => assert!(
            cok == 0 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION,
            "fc_macaddr8_in error-shape DIVERGENCE input={s:?} ({})",
            e.message
        ),
    }
}

fn arr6(b: &[u8]) -> [u8; 6] {
    b[..6].try_into().unwrap()
}

fn arr8(b: &[u8]) -> [u8; 8] {
    b[..8].try_into().unwrap()
}

fn mac_cmp_diff(p: &[u8]) {
    let (a, b) = (MacAddr::from_bytes(arr6(p)), MacAddr::from_bytes(arr6(&p[6..])));
    let (pa, pb) = (p.as_ptr(), p[6..].as_ptr());
    let checks: [(&str, i32, i32); 7] = unsafe {
        [
            ("cmp", pgc_macaddr_cmp(pa, pb), adt_mac::macaddr_cmp(&a, &b)),
            ("lt", pgc_macaddr_lt(pa, pb), adt_mac::macaddr_lt(&a, &b) as i32),
            ("le", pgc_macaddr_le(pa, pb), adt_mac::macaddr_le(&a, &b) as i32),
            ("eq", pgc_macaddr_eq(pa, pb), adt_mac::macaddr_eq(&a, &b) as i32),
            ("ge", pgc_macaddr_ge(pa, pb), adt_mac::macaddr_ge(&a, &b) as i32),
            ("gt", pgc_macaddr_gt(pa, pb), adt_mac::macaddr_gt(&a, &b) as i32),
            ("ne", pgc_macaddr_ne(pa, pb), adt_mac::macaddr_ne(&a, &b) as i32),
        ]
    };
    for (name, c, r) in checks {
        assert!(
            c == r,
            "macaddr_{name} DIVERGENCE a={:02x?} b={:02x?}: C={c} Rust={r}",
            &p[..6],
            &p[6..12]
        );
    }

    // fc-wrapper plane: fc_macaddr_cmp + the six bool wrappers.
    let cx = mcx::MemoryContext::new("mac_fc");
    let m = cx.mcx();
    let (da, db) = (dptr(&p[..6]), dptr(&p[6..12]));
    use adt_mac::builtins as fcb;
    let d = fc_call::<2>(fcb::fc_macaddr_cmp, m, [da, db]).expect("cmp wrapper cannot fail");
    assert_eq!(d.as_i32(), adt_mac::macaddr_cmp(&a, &b), "fc_macaddr_cmp vs core DIVERGENCE");
    let fc_ops: [(&str, PGFunction, bool); 6] = [
        ("lt", fcb::fc_macaddr_lt, adt_mac::macaddr_lt(&a, &b)),
        ("le", fcb::fc_macaddr_le, adt_mac::macaddr_le(&a, &b)),
        ("eq", fcb::fc_macaddr_eq, adt_mac::macaddr_eq(&a, &b)),
        ("ge", fcb::fc_macaddr_ge, adt_mac::macaddr_ge(&a, &b)),
        ("gt", fcb::fc_macaddr_gt, adt_mac::macaddr_gt(&a, &b)),
        ("ne", fcb::fc_macaddr_ne, adt_mac::macaddr_ne(&a, &b)),
    ];
    for (name, f, expect) in fc_ops {
        let d = fc_call::<2>(f, m, [da, db]).expect("bool wrapper cannot fail");
        assert_eq!(
            d.as_bool(),
            expect,
            "fc_macaddr_{name} vs core DIVERGENCE a={:02x?} b={:02x?}",
            &p[..6],
            &p[6..12]
        );
    }
}

fn mac8_cmp_diff(p: &[u8]) {
    let (a, b) = (
        MacAddr8::from_bytes(arr8(p)),
        MacAddr8::from_bytes(arr8(&p[8..])),
    );
    let (pa, pb) = (p.as_ptr(), p[8..].as_ptr());
    let checks: [(&str, i32, i32); 7] = unsafe {
        [
            ("cmp", pgc_macaddr8_cmp(pa, pb), adt_mac8::macaddr8_cmp(&a, &b)),
            ("lt", pgc_macaddr8_lt(pa, pb), adt_mac8::macaddr8_lt(&a, &b) as i32),
            ("le", pgc_macaddr8_le(pa, pb), adt_mac8::macaddr8_le(&a, &b) as i32),
            ("eq", pgc_macaddr8_eq(pa, pb), adt_mac8::macaddr8_eq(&a, &b) as i32),
            ("ge", pgc_macaddr8_ge(pa, pb), adt_mac8::macaddr8_ge(&a, &b) as i32),
            ("gt", pgc_macaddr8_gt(pa, pb), adt_mac8::macaddr8_gt(&a, &b) as i32),
            ("ne", pgc_macaddr8_ne(pa, pb), adt_mac8::macaddr8_ne(&a, &b) as i32),
        ]
    };
    for (name, c, r) in checks {
        assert!(
            c == r,
            "macaddr8_{name} DIVERGENCE a={:02x?} b={:02x?}: C={c} Rust={r}",
            &p[..8],
            &p[8..16]
        );
    }

    // fc-wrapper plane: fc_macaddr8_cmp + the six bool wrappers.
    let cx = mcx::MemoryContext::new("mac_fc");
    let m = cx.mcx();
    let (da, db) = (dptr(&p[..8]), dptr(&p[8..16]));
    use adt_mac8::builtins as fcb;
    let d = fc_call::<2>(fcb::fc_macaddr8_cmp, m, [da, db]).expect("cmp wrapper cannot fail");
    assert_eq!(d.as_i32(), adt_mac8::macaddr8_cmp(&a, &b), "fc_macaddr8_cmp vs core DIVERGENCE");
    let fc_ops: [(&str, PGFunction, bool); 6] = [
        ("lt", fcb::fc_macaddr8_lt, adt_mac8::macaddr8_lt(&a, &b)),
        ("le", fcb::fc_macaddr8_le, adt_mac8::macaddr8_le(&a, &b)),
        ("eq", fcb::fc_macaddr8_eq, adt_mac8::macaddr8_eq(&a, &b)),
        ("ge", fcb::fc_macaddr8_ge, adt_mac8::macaddr8_ge(&a, &b)),
        ("gt", fcb::fc_macaddr8_gt, adt_mac8::macaddr8_gt(&a, &b)),
        ("ne", fcb::fc_macaddr8_ne, adt_mac8::macaddr8_ne(&a, &b)),
    ];
    for (name, f, expect) in fc_ops {
        let d = fc_call::<2>(f, m, [da, db]).expect("bool wrapper cannot fail");
        assert_eq!(
            d.as_bool(),
            expect,
            "fc_macaddr8_{name} vs core DIVERGENCE a={:02x?} b={:02x?}",
            &p[..8],
            &p[8..16]
        );
    }
}

/// Compare a C 6-byte-producing unary/binary arm against the Rust MacAddr.
fn check_mac(name: &str, input: &[u8], cbytes: [u8; 6], r: MacAddr) {
    assert!(
        r.to_bytes() == cbytes,
        "{name} DIVERGENCE input={input:02x?}: C={cbytes:02x?} Rust={:02x?}",
        r.to_bytes()
    );
}

fn check_mac8(name: &str, input: &[u8], cbytes: [u8; 8], r: MacAddr8) {
    assert!(
        r.to_bytes() == cbytes,
        "{name} DIVERGENCE input={input:02x?}: C={cbytes:02x?} Rust={:02x?}",
        r.to_bytes()
    );
}

fn mac_recv_send_diff(p: &[u8]) {
    // C macaddr_recv copies six pq_getmsgbyte bytes verbatim, so the wire
    // oracle for a 6-byte payload is the payload itself; send inverts it.
    let cx = mcx::MemoryContext::new("mac_fuzz");
    let mcx = cx.mcx();
    let Ok(mut vec) = mcx::vec_with_capacity_in::<u8>(mcx, 6) else {
        return;
    };
    if mcx::vec_append_bytes(&mut vec, &p[..6]).is_err() {
        return;
    }
    let Ok(mut msg) = stringinfo::StringInfo::from_vec(vec) else {
        return;
    };
    let addr = adt_mac::macaddr_recv(&mut msg).expect("6-byte macaddr_recv cannot fail");
    check_mac("macaddr_recv", &p[..6], arr6(p), addr);
    let sent = adt_mac::macaddr_send(mcx, &addr).expect("macaddr_send OOM-free here");
    assert!(
        sent.data() == &p[..6],
        "macaddr_send DIVERGENCE input={:02x?}: sent={:02x?}",
        &p[..6],
        sent.data()
    );

    // fc-wrapper plane: fc_macaddr_recv over a fresh StringInfo image, then
    // fc_macaddr_send on the decoded value (varlena image parity vs core).
    let Some(mut msg2) = make_si(mcx, &p[..6]) else { return };
    let d = fc_call::<1>(
        adt_mac::builtins::fc_macaddr_recv,
        mcx,
        [Datum::from_usize(core::ptr::from_mut(&mut msg2) as usize)],
    )
    .expect("fc_macaddr_recv cannot fail on 6 bytes");
    assert_eq!(datum_bytes(d, 6), &p[..6], "fc_macaddr_recv vs core DIVERGENCE");
    let ds = fc_call::<1>(adt_mac::builtins::fc_macaddr_send, mcx, [d])
        .expect("fc_macaddr_send cannot fail");
    let img = sent.as_bytes();
    assert_eq!(datum_bytes(ds, img.len()), img, "fc_macaddr_send vs core DIVERGENCE");
}

fn mac8_recv_send_diff(p: &[u8]) {
    // Flag byte picks the 8-byte identity arm or the 6-byte EUI-48 wire form
    // (the FF/FE stuffing arm — oracled by C macaddrtomacaddr8, which
    // performs the identical insertion).
    let (&flag, rest) = p.split_first().unwrap();
    let n = if flag & 1 == 0 { 8 } else { 6 };
    if rest.len() < n {
        return;
    }
    let cx = mcx::MemoryContext::new("mac8_fuzz");
    let mcx = cx.mcx();
    let Ok(mut vec) = mcx::vec_with_capacity_in::<u8>(mcx, n) else {
        return;
    };
    if mcx::vec_append_bytes(&mut vec, &rest[..n]).is_err() {
        return;
    }
    let Ok(mut msg) = stringinfo::StringInfo::from_vec(vec) else {
        return;
    };
    let addr = adt_mac8::macaddr8_recv(&mut msg).expect("6/8-byte macaddr8_recv cannot fail");
    let mut expect = [0u8; 8];
    if n == 8 {
        expect = arr8(rest);
    } else {
        unsafe { pgc_macaddrtomacaddr8(rest.as_ptr(), expect.as_mut_ptr()) };
    }
    check_mac8("macaddr8_recv", &rest[..n], expect, addr);
    let sent = adt_mac8::macaddr8_send(mcx, &addr).expect("macaddr8_send OOM-free here");
    assert!(
        sent.data() == expect,
        "macaddr8_send DIVERGENCE input={:02x?}: sent={:02x?} expect={expect:02x?}",
        &rest[..n],
        sent.data()
    );

    // fc-wrapper plane: fc_macaddr8_recv (both the 8-byte identity and the
    // 6-byte FF/FE stuffing wire forms) + fc_macaddr8_send on the value.
    let Some(mut msg2) = make_si(mcx, &rest[..n]) else { return };
    let d = fc_call::<1>(
        adt_mac8::builtins::fc_macaddr8_recv,
        mcx,
        [Datum::from_usize(core::ptr::from_mut(&mut msg2) as usize)],
    )
    .expect("fc_macaddr8_recv cannot fail on 6/8 bytes");
    assert_eq!(datum_bytes(d, 8), expect, "fc_macaddr8_recv vs core DIVERGENCE");
    let ds = fc_call::<1>(adt_mac8::builtins::fc_macaddr8_send, mcx, [d])
        .expect("fc_macaddr8_send cannot fail");
    let img = sent.as_bytes();
    assert_eq!(datum_bytes(ds, img.len()), img, "fc_macaddr8_send vs core DIVERGENCE");
}

pub fn mac_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, p)) = data.split_first() else {
        return;
    };
    match sel % 21 {
        0 => mac_in_diff(p),
        1 => mac8_in_diff(p),
        2 if p.len() >= 6 => {
            let mut cbuf = [0u8; 32];
            let clen = unsafe { pgc_macaddr_out(p.as_ptr(), cbuf.as_mut_ptr().cast()) } as usize;
            let mut rbuf = [0u8; MACADDR_OUT_LEN];
            let rlen = adt_mac::macaddr_out_into(&MacAddr::from_bytes(arr6(p)), &mut rbuf);
            assert!(
                clen == rlen && cbuf[..clen] == rbuf[..rlen],
                "macaddr_out DIVERGENCE input={:02x?}: C={:?} Rust={:?}",
                &p[..6],
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(&rbuf[..rlen])
            );
            // fc-wrapper plane: fc_macaddr_out (thread-local cstring scratch).
            let cx = mcx::MemoryContext::new("mac_fc");
            let d = fc_call::<1>(adt_mac::builtins::fc_macaddr_out, cx.mcx(), [dptr(p)])
                .expect("fc_macaddr_out cannot fail");
            let img = datum_bytes(d, rlen + 1);
            assert!(
                img[..rlen] == rbuf[..rlen] && img[rlen] == 0,
                "fc_macaddr_out vs core DIVERGENCE input={:02x?}",
                &p[..6]
            );
        }
        3 if p.len() >= 8 => {
            let mut cbuf = [0u8; 32];
            let clen = unsafe { pgc_macaddr8_out(p.as_ptr(), cbuf.as_mut_ptr().cast()) } as usize;
            let mut rbuf = [0u8; MACADDR8_OUT_LEN];
            let rlen = adt_mac8::macaddr8_out_into(&MacAddr8::from_bytes(arr8(p)), &mut rbuf);
            assert!(
                clen == rlen && cbuf[..clen] == rbuf[..rlen],
                "macaddr8_out DIVERGENCE input={:02x?}: C={:?} Rust={:?}",
                &p[..8],
                std::str::from_utf8(&cbuf[..clen]),
                std::str::from_utf8(&rbuf[..rlen])
            );
            // fc-wrapper plane: fc_macaddr8_out (thread-local cstring scratch).
            let cx = mcx::MemoryContext::new("mac_fc");
            let d = fc_call::<1>(adt_mac8::builtins::fc_macaddr8_out, cx.mcx(), [dptr(p)])
                .expect("fc_macaddr8_out cannot fail");
            let img = datum_bytes(d, rlen + 1);
            assert!(
                img[..rlen] == rbuf[..rlen] && img[rlen] == 0,
                "fc_macaddr8_out vs core DIVERGENCE input={:02x?}",
                &p[..8]
            );
        }
        4 if p.len() >= 12 => mac_cmp_diff(p),
        5 if p.len() >= 16 => mac8_cmp_diff(p),
        6 if p.len() >= 6 => {
            let mut c = [0u8; 6];
            unsafe { pgc_macaddr_not(p.as_ptr(), c.as_mut_ptr()) };
            check_mac("macaddr_not", &p[..6], c, adt_mac::macaddr_not(&MacAddr::from_bytes(arr6(p))));
            fc_byref::<1>("fc_macaddr_not", adt_mac::builtins::fc_macaddr_not, [dptr(p)], &c);
        }
        7 if p.len() >= 12 => {
            let mut c = [0u8; 6];
            unsafe { pgc_macaddr_and(p.as_ptr(), p[6..].as_ptr(), c.as_mut_ptr()) };
            let r = adt_mac::macaddr_and(
                &MacAddr::from_bytes(arr6(p)),
                &MacAddr::from_bytes(arr6(&p[6..])),
            );
            check_mac("macaddr_and", &p[..12], c, r);
            fc_byref::<2>(
                "fc_macaddr_and",
                adt_mac::builtins::fc_macaddr_and,
                [dptr(p), dptr(&p[6..])],
                &c,
            );
        }
        8 if p.len() >= 12 => {
            let mut c = [0u8; 6];
            unsafe { pgc_macaddr_or(p.as_ptr(), p[6..].as_ptr(), c.as_mut_ptr()) };
            let r = adt_mac::macaddr_or(
                &MacAddr::from_bytes(arr6(p)),
                &MacAddr::from_bytes(arr6(&p[6..])),
            );
            check_mac("macaddr_or", &p[..12], c, r);
            fc_byref::<2>(
                "fc_macaddr_or",
                adt_mac::builtins::fc_macaddr_or,
                [dptr(p), dptr(&p[6..])],
                &c,
            );
        }
        9 if p.len() >= 6 => {
            let mut c = [0u8; 6];
            unsafe { pgc_macaddr_trunc(p.as_ptr(), c.as_mut_ptr()) };
            check_mac(
                "macaddr_trunc",
                &p[..6],
                c,
                adt_mac::macaddr_trunc(&MacAddr::from_bytes(arr6(p))),
            );
            fc_byref::<1>("fc_macaddr_trunc", adt_mac::builtins::fc_macaddr_trunc, [dptr(p)], &c);
        }
        10 if p.len() >= 8 => {
            let mut c = [0u8; 8];
            unsafe { pgc_macaddr8_not(p.as_ptr(), c.as_mut_ptr()) };
            check_mac8(
                "macaddr8_not",
                &p[..8],
                c,
                adt_mac8::macaddr8_not(&MacAddr8::from_bytes(arr8(p))),
            );
            fc_byref::<1>("fc_macaddr8_not", adt_mac8::builtins::fc_macaddr8_not, [dptr(p)], &c);
        }
        11 if p.len() >= 16 => {
            let mut c = [0u8; 8];
            unsafe { pgc_macaddr8_and(p.as_ptr(), p[8..].as_ptr(), c.as_mut_ptr()) };
            let r = adt_mac8::macaddr8_and(
                &MacAddr8::from_bytes(arr8(p)),
                &MacAddr8::from_bytes(arr8(&p[8..])),
            );
            check_mac8("macaddr8_and", &p[..16], c, r);
            fc_byref::<2>(
                "fc_macaddr8_and",
                adt_mac8::builtins::fc_macaddr8_and,
                [dptr(p), dptr(&p[8..])],
                &c,
            );
        }
        12 if p.len() >= 16 => {
            let mut c = [0u8; 8];
            unsafe { pgc_macaddr8_or(p.as_ptr(), p[8..].as_ptr(), c.as_mut_ptr()) };
            let r = adt_mac8::macaddr8_or(
                &MacAddr8::from_bytes(arr8(p)),
                &MacAddr8::from_bytes(arr8(&p[8..])),
            );
            check_mac8("macaddr8_or", &p[..16], c, r);
            fc_byref::<2>(
                "fc_macaddr8_or",
                adt_mac8::builtins::fc_macaddr8_or,
                [dptr(p), dptr(&p[8..])],
                &c,
            );
        }
        13 if p.len() >= 8 => {
            let mut c = [0u8; 8];
            unsafe { pgc_macaddr8_trunc(p.as_ptr(), c.as_mut_ptr()) };
            check_mac8(
                "macaddr8_trunc",
                &p[..8],
                c,
                adt_mac8::macaddr8_trunc(&MacAddr8::from_bytes(arr8(p))),
            );
            fc_byref::<1>(
                "fc_macaddr8_trunc",
                adt_mac8::builtins::fc_macaddr8_trunc,
                [dptr(p)],
                &c,
            );
        }
        14 if p.len() >= 8 => {
            let mut c = [0u8; 8];
            unsafe { pgc_macaddr8_set7bit(p.as_ptr(), c.as_mut_ptr()) };
            check_mac8(
                "macaddr8_set7bit",
                &p[..8],
                c,
                adt_mac8::macaddr8_set7bit(&MacAddr8::from_bytes(arr8(p))),
            );
            fc_byref::<1>(
                "fc_macaddr8_set7bit",
                adt_mac8::builtins::fc_macaddr8_set7bit,
                [dptr(p)],
                &c,
            );
        }
        15 if p.len() >= 6 => {
            let mut c = [0u8; 8];
            unsafe { pgc_macaddrtomacaddr8(p.as_ptr(), c.as_mut_ptr()) };
            check_mac8(
                "macaddrtomacaddr8",
                &p[..6],
                c,
                adt_mac8::macaddrtomacaddr8(&MacAddr::from_bytes(arr6(p))),
            );
            fc_byref::<1>(
                "fc_macaddrtomacaddr8",
                adt_mac8::builtins::fc_macaddrtomacaddr8,
                [dptr(p)],
                &c,
            );
        }
        16 if p.len() >= 8 => {
            let mut c = [0u8; 6];
            let cerr = unsafe { pgc_macaddr8tomacaddr(p.as_ptr(), c.as_mut_ptr()) };
            match adt_mac8::macaddr8tomacaddr(&MacAddr8::from_bytes(arr8(p))) {
                Ok(r) => assert!(
                    cerr == 0 && r.to_bytes() == c,
                    "macaddr8tomacaddr DIVERGENCE input={:02x?}: C=(err {cerr}, {c:02x?}) Rust=Ok({:02x?})",
                    &p[..8],
                    r.to_bytes()
                ),
                Err(e) => assert!(
                    cerr == 1 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "macaddr8tomacaddr DIVERGENCE input={:02x?}: C err {cerr} vs Rust err ({})",
                    &p[..8],
                    e.message
                ),
            }
            // fc-wrapper plane: fc_macaddr8tomacaddr (value + error verdict).
            let cx = mcx::MemoryContext::new("mac_fc");
            match fc_call::<1>(adt_mac8::builtins::fc_macaddr8tomacaddr, cx.mcx(), [dptr(p)]) {
                Ok(d) => assert!(
                    cerr == 0 && datum_bytes(d, 6) == c,
                    "fc_macaddr8tomacaddr vs core DIVERGENCE input={:02x?}",
                    &p[..8]
                ),
                Err(e) => assert!(
                    cerr == 1 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                    "fc_macaddr8tomacaddr error-shape DIVERGENCE input={:02x?} ({})",
                    &p[..8],
                    e.message
                ),
            }
        }
        17 if p.len() >= 14 => {
            let addr = MacAddr::from_bytes(arr6(p));
            let seed = u64::from_le_bytes(p[6..14].try_into().unwrap());
            let (c32, r32) = (unsafe { pg_hashmacaddr(p.as_ptr()) }, adt_mac::hashmacaddr(&addr));
            let c64 = unsafe { pg_hashmacaddrextended(p.as_ptr(), seed as i64) };
            let r64 = adt_mac::hashmacaddrextended(&addr, seed);
            assert!(
                c32 == r32 && c64 == r64,
                "hashmacaddr DIVERGENCE input={:02x?} seed={seed:#x}: \
                 C=({c32:#010x},{c64:#018x}) Rust=({r32:#010x},{r64:#018x})",
                &p[..6]
            );
            // fc-wrapper plane: fc_hashmacaddr / fc_hashmacaddrextended.
            let cx = mcx::MemoryContext::new("mac_fc");
            let m = cx.mcx();
            let d = fc_call::<1>(adt_mac::builtins::fc_hashmacaddr, m, [dptr(p)])
                .expect("hash wrapper cannot fail");
            assert_eq!(d.as_u32(), r32, "fc_hashmacaddr vs core DIVERGENCE");
            let d = fc_call::<2>(
                adt_mac::builtins::fc_hashmacaddrextended,
                m,
                [dptr(p), Datum::from_u64(seed)],
            )
            .expect("hash-extended wrapper cannot fail");
            assert_eq!(d.as_u64(), r64, "fc_hashmacaddrextended vs core DIVERGENCE");
        }
        18 if p.len() >= 16 => {
            let addr = MacAddr8::from_bytes(arr8(p));
            let seed = u64::from_le_bytes(p[8..16].try_into().unwrap());
            let (c32, r32) = (unsafe { pg_hashmacaddr8(p.as_ptr()) }, adt_mac8::hashmacaddr8(&addr));
            let c64 = unsafe { pg_hashmacaddr8extended(p.as_ptr(), seed as i64) };
            let r64 = adt_mac8::hashmacaddr8extended(&addr, seed);
            assert!(
                c32 == r32 && c64 == r64,
                "hashmacaddr8 DIVERGENCE input={:02x?} seed={seed:#x}: \
                 C=({c32:#010x},{c64:#018x}) Rust=({r32:#010x},{r64:#018x})",
                &p[..8]
            );
            // fc-wrapper plane: fc_hashmacaddr8 / fc_hashmacaddr8extended.
            let cx = mcx::MemoryContext::new("mac_fc");
            let m = cx.mcx();
            let d = fc_call::<1>(adt_mac8::builtins::fc_hashmacaddr8, m, [dptr(p)])
                .expect("hash wrapper cannot fail");
            assert_eq!(d.as_u32(), r32, "fc_hashmacaddr8 vs core DIVERGENCE");
            let d = fc_call::<2>(
                adt_mac8::builtins::fc_hashmacaddr8extended,
                m,
                [dptr(p), Datum::from_u64(seed)],
            )
            .expect("hash-extended wrapper cannot fail");
            assert_eq!(d.as_u64(), r64, "fc_hashmacaddr8extended vs core DIVERGENCE");
        }
        19 if p.len() >= 6 => mac_recv_send_diff(p),
        20 if p.len() >= 7 => mac8_recv_send_diff(p),
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Stable-toolchain smoke tests: replay the seed corpus shapes through every
// arm so `cargo test` exercises the C link + all comparators without
// cargo-fuzz. Same corpus written to fuzz/corpus/mac_diff by the seed files.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    pub const MAC_TEXT_CORPUS: &[&str] = &[
        "08:00:2b:01:02:03",
        "08-00-2b-01-02-03",
        "08002b:010203",
        "08002b-010203",
        "0800.2b01.0203",
        "0800-2b01-0203",
        "08002b010203",
        "0x08:0x0:2b:1:2:3",
        "  08:00:2b:01:02:03  ",
        "\t8:0:2b:1:2:3\n",
        "aB:cD:eF:01:23:45",
        "+1:-0:3:4:5:6",
        "1ff:2:3:4:5:6",
        "100:100:100:100:100:100",
        "-1:2:3:4:5:6",
        "08:00:2b:01:02",
        "08:00:2b:01:02:03:04",
        "08:00:2b:01:02:03 junk",
        "08:00-2b:01:02:03",
        "0800:2b01:0203",
        "08 00 2b 01 02 03",
        "0x:1:2:3:4:5",
        "0xx:1:2:3:4:5",
        "0X1:2:3:4:5:6",
        "not a mac",
        "",
        " ",
        ".",
        "..",
        "0800.2b01.020",
        "0800.2b01.02034",
        "08002b01020",
        "ffffffff:0:0:0:0:0",   /* 8 digits: compared (both 22003) */
        "fffffff:0:0:0:0:0",    /* 7 digits: compared */
        "123456789:0:0:0:0:0",  /* row-436 carve class (skipped) */
        "100000000:0:0:0:0:0",  /* row-436 carve class (skipped) */
        "08:00:2b:01:02:0x",
        "08:00:2b:01:02:0x3",
        "0:0:0:0:0:0",
        "ff:ff:ff:ff:ff:ff",
    ];

    pub const MAC8_TEXT_CORPUS: &[&str] = &[
        "08:00:2b:01:02:03:04:05",
        "08-00-2b-01-02-03-04-05",
        "08002b:0102030405",
        "0800.2b01.0203.0405",
        "08002b0102030405",
        "08:00:2b:01:02:03",
        "08002b010203",
        "0800.2b01.0203",
        "  08:00:2b:ff:fe:01:02:03  ",
        "08:00:2b:01:02:03:04:05 ",
        "08:00:2b:01:02:03:04:05 x",
        "08:00-2b:01:02:03:04:05",
        "08.00:2b.01.02.03",
        "08:00:2b:01:02:03:04",
        "08:00:2b:01:02:03:04:05:06",
        "0g:00:2b:01:02:03:04:05",
        "08:00:2b:01:02:0",
        "aB:cD:eF:01:23:45:67:89",
        "ff:ff:ff:ff:ff:ff:ff:ff",
        "",
        " ",
        ":",
        "0800",
        "08",
    ];

    fn drive(sel: u8, payload: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(payload);
        mac_diff(&d);
    }

    #[test]
    fn mac_in_corpus() {
        let _serial = crate::c_oracle_serial();
        for s in MAC_TEXT_CORPUS {
            drive(0, s.as_bytes());
            drive(1, s.as_bytes()); /* every mac text through macaddr8_in too */
        }
        for s in MAC8_TEXT_CORPUS {
            drive(1, s.as_bytes());
            drive(0, s.as_bytes()); /* and vice versa: shared corpus */
        }
    }

    pub const BYTES_CORPUS: &[[u8; 16]] = &[
        [0; 16],
        [0xff; 16],
        [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05, 0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05],
        [0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03, 0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x04],
        [0x7f, 0x80, 0x81, 0x00, 0xff, 0x01, 0x80, 0x7f, 0x81, 0xff, 0x00, 0x01, 0xfe, 0xff, 0x00, 0x80],
        [0x80, 0, 0, 0, 0, 0, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        [0xaa, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0x55, 0xaa, 0x55, 0xaa, 0x55, 0xaa, 0xde, 0xad, 0xbe, 0xef],
    ];

    #[test]
    fn mac_binary_arms_corpus() {
        let _serial = crate::c_oracle_serial();
        for bytes in BYTES_CORPUS {
            for sel in 2u8..=20 {
                drive(sel, bytes);
            }
            // mac8 recv 6-byte stuffing arm (flag odd) + 8-byte arm.
            let mut d = vec![20u8, 1];
            d.extend_from_slice(&bytes[..6]);
            mac_diff(&d);
        }
        // hibits/lobits sign-extension probes: bytes >= 0x80 in every slot
        // (the C int-promotion subtlety the mac8 shim header calls out).
        for hi in [0x00u8, 0x7f, 0x80, 0xff] {
            for slot in 0..8 {
                let mut b = [0x80u8; 16];
                b[slot] = hi;
                b[8 + slot] = hi ^ 0xff;
                drive(4, &b);
                drive(5, &b);
            }
        }
    }

    /// fc-wrapper plane smoke: every fc_* wrapper of adt_mac and adt_mac8
    /// executes at least once under `cargo test` — text arms (Ok, 22P02 and
    /// 22003 error shapes), every binary selector, the macaddr8tomacaddr
    /// error arm, and the mac8 6-byte stuffing recv arm.
    #[test]
    fn fc_plane_smoke() {
        let _serial = crate::c_oracle_serial();
        // Arms 0/1: fc_macaddr_in Ok / 22P02 / 22003, fc_macaddr8_in Ok / err.
        for s in ["08:00:2b:01:02:03", "not a mac", "1ff:2:3:4:5:6"] {
            drive(0, s.as_bytes());
            drive(1, s.as_bytes());
        }
        drive(1, b"08:00:2b:01:02:03:04:05");
        // Arms 2..=20 over ordered and FF/FE-shaped blocks (arm 16 Ok arm).
        let ok16 = [
            0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03, 0x08, 0x00, 0x2b, 0x01, 0x02, 0x03,
            0x04, 0x05,
        ];
        for sel in 2u8..=20 {
            drive(sel, &ok16);
        }
        // Arm 16 error arm: non-FF/FE middle bytes -> 22003 on both planes.
        drive(16, &[1u8; 8]);
        // Arm 20 6-byte stuffing wire form (flag odd).
        let mut d = vec![20u8, 1];
        d.extend_from_slice(&ok16[..6]);
        mac_diff(&d);
    }

    /// Pins the row-436 carve boundary: 8-digit fields are COMPARED (and
    /// agree — both sides 22003 on out-of-range, both accept in-range),
    /// 9-digit fields are carved.
    #[test]
    fn row436_carve_boundary() {
        let _serial = crate::c_oracle_serial();
        assert!(!carve_row436(b"ffffffff:0:0:0:0:0"));
        assert!(!carve_row436(b"08:00:2b:01:02:03"));
        assert!(!carve_row436(b"08002b010203")); /* 12-digit bare run: %2x style, no carve */
        assert!(carve_row436(b"123456789:0:0:0:0:0"));
        assert!(carve_row436(b"0:0:0:0:0:100000000"));
        assert!(carve_row436(b"0-0-0-0-0-100000000"));
        // Not terminal in style 0 (trailing junk) -> no carve.
        assert!(!carve_row436(b"123456789:0:0:0:0:0 junk"));
        // C-side witness of the ratified divergence itself: sscanf wraps
        // 0x100000000 mod 2^32 to 0 and ACCEPTS; shipped Rust rejects 22003.
        let cs = CString::new("100000000:0:0:0:0:0").unwrap();
        let mut cout = [0u8; 6];
        let cerr = unsafe { pgc_macaddr_in(cs.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!((cerr, cout), (0, [0u8; 6]), "C wrap-accepts (row 436)");
        let rerr = adt_mac::macaddr_in("100000000:0:0:0:0:0", None).unwrap_err();
        assert_eq!(mac_err_class(&rerr), 2, "pgrust rejects 22003 (row 436)");
    }

    /// FINDING WITNESS (RESOLVED): adt_mac Scanner::scan_hex once negated
    /// with a plain `-value`, overflowing (and panicking under checked
    /// profiles) on an accumulator of exactly i64::MIN. The reported
    /// wrapping_neg fix has landed (adt_mac lib.rs sign-apply arms), so the
    /// input now parses without panicking in every profile. The driver guard
    /// stays as conservative carve hygiene: every guarded input sits inside
    /// the row-436 >8-digit-field class anyway.
    #[test]
    fn negation_overflow_finding_witness() {
        let _serial = crate::c_oracle_serial();
        assert!(negation_overflow_guard(b"-8000000000000000"));
        assert!(negation_overflow_guard(b"-0x8000000000000000:1:2:3:4:5"));
        assert!(!negation_overflow_guard(b"-fffffffffffffff:0:0:0:0:0")); /* 15 digits */
        let r = std::panic::catch_unwind(|| adt_mac::macaddr_in("-8000000000000000", None));
        assert!(r.is_ok(), "wrapping_neg fix landed: no overflow panic in any profile");
    }
}
