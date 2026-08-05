//! contribb_diff: differential fuzz driver for crates/contrib/seg and
//! crates/contrib/cube vs verbatim vendored PostgreSQL 18.3 C
//! (csrc/pg_contribb_io.c + the generated parser TUs under csrc/contribb/,
//! upstream sha 62d6c7d3df; lane p1-mb-contribb). Selector = data[0] % 10:
//!
//!   0 seg_in       — flex/bison text parse (both sides), incl. the
//!                    significant-digits capture and the swapped-boundaries
//!                    22023 arm; on success the 12-byte SEG images are
//!                    compared and roundtripped through seg_out.
//!   1 seg_out      — raw 12-byte SEG image -> text (restore's sigfig
//!                    formatting) over the full byte domain.
//!   2 seg binops   — all 16 two-seg C entry points (cmp/lt/le/gt/ge/same/
//!                    different/contains/contained/overlap/left/right/
//!                    over_left/over_right/union/inter) on raw images,
//!                    incl. seg_cmp's "bogus boundary types" XX000 arm.
//!   3 seg unops    — center/lower/upper/size, float4 images compared as
//!                    BITS.
//!   4 cube_in      — flex/bison text parse; on success images compared +
//!                    roundtrip through cube_out and cube_send.
//!   5 cube unary io — cube_out / cube_send / cube_dim / cube_is_point on
//!                    a driver-built VALID image (see domain carve).
//!   6 cube_recv    — wire-format decode: structured header variant plus
//!                    raw-tail truncations (08P01) and the >100-dim 54000
//!                    arm.
//!   7 cube binops  — cmp/eq/ne/lt/gt/le/ge/contains/contained/overlap/
//!                    union/inter/distance/taxicab/chebyshev on two valid
//!                    images; distances compared as f64 BITS.
//!   8 cube unops   — ll/ur_coord, coord, coord_llur (2202E arms), size,
//!                    enlarge, cube_f8, cube_f8_f8, cube_c_f8, cube_c_f8_f8
//!                    (54000 can't-extend arm).
//!   9 cube arrays  — cube_a_f8_f8 / cube_a_f8 / cube_subset over
//!                    driver-built float8[]/int4[] ArrayType images, incl.
//!                    the NULL-element 2202E, >100-dim 54000, length-
//!                    mismatch 2202E and index-out-of-bounds 2202E arms.
//!
//! Comparison planes: value bytes (SEG/NDBOX images, output text, send
//! payloads) + float bits (f32/f64 images bit-exact; C -ffp-contract=off)
//! + error verdict + exact SQLSTATE (C small-int codes mapped in
//! `map_state`; message text out of scope). The Rust plane always runs the
//! shipped fc_* wrappers through the dfmgr-registered library lookup, the
//! way real fmgr dispatch reaches contrib functions.
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - cube images are driver-BUILT (header = dim | point-bit, unused bits
//!     8-30 zero, x[] fully populated): a stored NDBOX always satisfies
//!     this; corrupt headers make C's x[DIM(cube)] an over-read (UB).
//!     cube_recv is the arm where hostile raw headers ARE driven (C
//!     validates there, exactly as PG does on the wire path).
//!   - seg images are raw 12 bytes (no carve: every byte pattern is
//!     C-defined; the oracle is compiled -funsigned-char to pin
//!     plain-char signedness to the ratified Linux/aarch64 PG build).
//!   - array images are driver-built valid float8[]/int4[] (elemtype/dims
//!     honest, nulls via a real bitmap): C's ARRNELEMS/ARRPTR trust the
//!     header; hostile array headers are array_in's territory, not cube's.
//!   - parser inputs are capped (seg 256 B, cube 512 B): the flex list
//!     accumulation is quadratic (known fuzz-only wall, lane charter).
//!
//! No-panic everywhere: a Rust-side panic IS a finding (fc wrappers return
//! PgResult); divergences assert with full operand context.

#![allow(dead_code)]

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::OnceLock;

use datum::{Datum, NullableDatum};
use types_error::PgError;
use types_fmgr::{LocalFcinfo, PGFunction};

extern "C" {
    fn pg_cb_reset();
    fn pg_cb_seg_in(str_: *const c_char, out12: *mut u8) -> i32;
    fn pg_cb_seg_out(seg12: *const u8, out: *mut c_char, outsz: i32) -> i32;
    fn pg_cb_seg_binop(
        op: i32,
        a12: *const u8,
        b12: *const u8,
        iout: *mut i32,
        segout12: *mut u8,
    ) -> i32;
    fn pg_cb_seg_unop(op: i32, a12: *const u8, bits: *mut u32) -> i32;
    fn pg_cb_cube_in(str_: *const c_char, out: *mut u8, cap: i32, outlen: *mut i32) -> i32;
    fn pg_cb_cube_out(img: *const u8, len: i32, out: *mut c_char, outsz: i32) -> i32;
    fn pg_cb_cube_send(img: *const u8, len: i32, out: *mut u8, cap: i32, outlen: *mut i32) -> i32;
    fn pg_cb_cube_recv(msg: *const u8, msglen: i32, out: *mut u8, cap: i32, outlen: *mut i32)
        -> i32;
    fn pg_cb_cube_binop(
        op: i32,
        a: *const u8,
        alen: i32,
        b: *const u8,
        blen: i32,
        iout: *mut i32,
        fbits: *mut u64,
        imgout: *mut u8,
        imgoutlen: *mut i32,
    ) -> i32;
    fn pg_cb_cube_unop(
        op: i32,
        img: *const u8,
        len: i32,
        n: i32,
        f1bits: u64,
        f2bits: u64,
        iout: *mut i32,
        fbits: *mut u64,
        imgout: *mut u8,
        imgoutlen: *mut i32,
    ) -> i32;
    fn pg_cb_cube_arrayop(
        op: i32,
        arr1: *const u8,
        len1: i32,
        arr2: *const u8,
        len2: i32,
        imgout: *mut u8,
        imgoutlen: *mut i32,
    ) -> i32;
}

// ---------------------------------------------------------------------------
// SQLSTATE mapping (C oracle small-int codes; include/postgres.h table)
// ---------------------------------------------------------------------------

fn map_state(code: i32) -> types_error::SqlState {
    match code {
        1 => types_error::ERRCODE_INVALID_TEXT_REPRESENTATION,
        2 => types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        3 => types_error::ERRCODE_SYNTAX_ERROR,
        4 => types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        5 => types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
        6 => types_error::ERRCODE_ARRAY_ELEMENT_ERROR,
        7 => types_error::ERRCODE_INTERNAL_ERROR,
        8 => types_error::ERRCODE_PROTOCOL_VIOLATION,
        _ => panic!("unknown C oracle errcode {code}"),
    }
}

// ---------------------------------------------------------------------------
// fc plumbing (datetime_closeout fc_call pattern; context kept alive so
// byref results stay readable through the compare)
// ---------------------------------------------------------------------------

struct FcResult {
    result: Result<Datum, Box<PgError>>,
    isnull: bool,
    _cx: mcx::MemoryContext,
}

fn fc_call<const N: usize>(f: PGFunction, args: [Datum; N]) -> FcResult {
    let cx = mcx::MemoryContext::new("contribb_fc");
    let mut fcinfo = LocalFcinfo::<N>::new(0);
    // SAFETY: cx outlives the returned FcResult (moved into it).
    unsafe { fcinfo.set_result_mcx(cx.mcx()) };
    for (i, a) in args.into_iter().enumerate() {
        fcinfo.args[i] = NullableDatum::value(a);
    }
    let result = f(None, &mut fcinfo);
    FcResult {
        result,
        isnull: fcinfo.isnull,
        _cx: cx,
    }
}

fn datum_bytes<'a>(d: Datum, n: usize) -> &'a [u8] {
    // SAFETY: byref result live in the FcResult's context.
    unsafe { std::slice::from_raw_parts(d.as_usize() as *const u8, n) }
}

fn datum_cstr<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: cstring result live in the FcResult's context.
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const c_char).to_bytes() }
}

fn datum_varlena_payload<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: 4B-header varlena result live in the FcResult's context.
    unsafe {
        let p = d.as_usize() as *const u8;
        let hdr = u32::from_ne_bytes([*p, *p.add(1), *p.add(2), *p.add(3)]);
        let len = (hdr >> 2) as usize;
        std::slice::from_raw_parts(p.add(4), len - 4)
    }
}

struct Fns {
    seg: Vec<(&'static str, PGFunction)>,
    cube: Vec<(&'static str, PGFunction)>,
}

fn lookup(lib: &str, name: &'static str) -> PGFunction {
    dfmgr::load_external_function(lib, name, true)
        .expect("library registered")
        .expect("function resolves")
}

fn fns() -> &'static Fns {
    static FNS: OnceLock<Fns> = OnceLock::new();
    FNS.get_or_init(|| {
        contrib_seg::init_seams();
        contrib_cube::init_seams();
        // lookup-miss arm of both crates' dfmgr tables
        assert!(matches!(
            dfmgr::load_external_function("seg", "no_such_function", false),
            Ok(None)
        ));
        assert!(matches!(
            dfmgr::load_external_function("cube", "no_such_function", false),
            Ok(None)
        ));
        let seg_names = [
            "seg_in",
            "seg_out",
            "seg_center",
            "seg_lower",
            "seg_upper",
            "seg_size",
            "seg_cmp",
            "seg_lt",
            "seg_le",
            "seg_gt",
            "seg_ge",
            "seg_same",
            "seg_different",
            "seg_contains",
            "seg_contained",
            "seg_overlap",
            "seg_left",
            "seg_right",
            "seg_over_left",
            "seg_over_right",
            "seg_union",
            "seg_inter",
        ];
        let cube_names = [
            "cube_in",
            "cube_out",
            "cube_send",
            "cube_recv",
            "cube_a_f8_f8",
            "cube_a_f8",
            "cube_subset",
            "cube_f8",
            "cube_f8_f8",
            "cube_c_f8",
            "cube_c_f8_f8",
            "cube_cmp",
            "cube_eq",
            "cube_ne",
            "cube_lt",
            "cube_gt",
            "cube_le",
            "cube_ge",
            "cube_contains",
            "cube_contained",
            "cube_overlap",
            "cube_union",
            "cube_inter",
            "cube_size",
            "cube_distance",
            "distance_taxicab",
            "distance_chebyshev",
            "cube_is_point",
            "cube_dim",
            "cube_ll_coord",
            "cube_ur_coord",
            "cube_coord",
            "cube_coord_llur",
            "cube_enlarge",
        ];
        Fns {
            seg: seg_names.iter().map(|n| (*n, lookup("seg", n))).collect(),
            cube: cube_names.iter().map(|n| (*n, lookup("cube", n))).collect(),
        }
    })
}

fn segf(name: &str) -> PGFunction {
    fns().seg.iter().find(|(n, _)| *n == name).unwrap().1
}

fn cubef(name: &str) -> PGFunction {
    fns().cube.iter().find(|(n, _)| *n == name).unwrap().1
}

// ---------------------------------------------------------------------------
// input reader
// ---------------------------------------------------------------------------

struct Rdr<'a> {
    d: &'a [u8],
    i: usize,
}

impl<'a> Rdr<'a> {
    fn new(d: &'a [u8]) -> Self {
        Rdr { d, i: 0 }
    }
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.i).copied().unwrap_or(0);
        self.i += 1;
        v
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }
    fn u64(&mut self) -> u64 {
        (self.u32() as u64) | ((self.u32() as u64) << 32)
    }
    fn i32(&mut self) -> i32 {
        self.u32() as i32
    }
    fn f64bits(&mut self) -> u64 {
        self.u64()
    }
    fn rest(&mut self) -> &'a [u8] {
        let r = &self.d[self.i.min(self.d.len())..];
        self.i = self.d.len();
        r
    }
    fn take(&mut self, n: usize) -> &'a [u8] {
        let end = (self.i + n).min(self.d.len());
        let r = &self.d[self.i.min(self.d.len())..end];
        self.i = end;
        r
    }
}

/// Interior-NUL-free CString from raw bytes (both sides see the same text).
fn cstring_of(b: &[u8], cap: usize) -> CString {
    let mut v: Vec<u8> = b.iter().copied().filter(|&c| c != 0).take(cap).collect();
    v.push(0);
    CString::from_vec_with_nul(v).unwrap()
}

// ---------------------------------------------------------------------------
// compare helpers
// ---------------------------------------------------------------------------

/// C rc vs Rust result: both-ok -> run `ok`, both-err -> sqlstate equal,
/// else panic with context.
fn check_verdict<T>(
    what: &str,
    ctx: &dyn std::fmt::Debug,
    crc: i32,
    r: &Result<T, Box<PgError>>,
) -> bool {
    match (crc, r) {
        (0, Ok(_)) => true,
        (code, Err(e)) if code != 0 => {
            let cs = map_state(code);
            let rs = e.sqlstate;
            assert_eq!(
                cs, rs,
                "{what}: SQLSTATE diverged (C {cs:?} rust {rs:?}, msg {:?}) ctx={ctx:?}",
                e.message
            );
            false
        }
        (code, _) => panic!(
            "{what}: error verdict diverged (C rc={code} rust ok={}) ctx={ctx:?}",
            r.is_ok()
        ),
    }
}

// ---------------------------------------------------------------------------
// cube image construction (valid-image domain carve; see module docs)
// ---------------------------------------------------------------------------

const CUBE_MAX_DIM: usize = 100;

fn build_cube_image(r: &mut Rdr) -> Vec<u8> {
    let dim = (r.u8() as usize) % (CUBE_MAX_DIM + 1);
    let point = r.u8() & 1 != 0;
    let nitems = if point || dim == 0 { dim } else { 2 * dim };
    let size = 8 + 8 * nitems;
    let mut img = Vec::with_capacity(size);
    let header: u32 = (dim as u32) | if point { 0x8000_0000 } else { 0 };
    img.extend_from_slice(&(((size as u32) << 2).to_ne_bytes()));
    img.extend_from_slice(&header.to_ne_bytes());
    for _ in 0..nitems {
        img.extend_from_slice(&r.u64().to_ne_bytes());
    }
    img
}

// 1-D (or 0-D when n == 0) builtin-element ArrayType image; elemsz 8 or 4.
// null_at: Some(k) punches a null hole at element k (real null bitmap).
fn build_array_image(elemtype: u32, elems: &[u64], elemsz: usize, null_at: Option<usize>) -> Vec<u8> {
    let n = elems.len();
    let ndim = if n == 0 { 0 } else { 1 };
    let maxalign = |x: usize| (x + 7) & !7;
    let overhead = if null_at.is_some() {
        maxalign(16 + 8 * ndim + n.div_ceil(8))
    } else {
        maxalign(16 + 8 * ndim)
    };
    let size = overhead + elemsz * n;
    let mut v = Vec::with_capacity(size);
    v.extend_from_slice(&(((size as u32) << 2).to_ne_bytes()));
    v.extend_from_slice(&(ndim as i32).to_ne_bytes());
    let dataoffset: i32 = if null_at.is_some() { overhead as i32 } else { 0 };
    v.extend_from_slice(&dataoffset.to_ne_bytes());
    v.extend_from_slice(&elemtype.to_ne_bytes());
    if ndim == 1 {
        v.extend_from_slice(&(n as i32).to_ne_bytes());
        v.extend_from_slice(&1i32.to_ne_bytes()); // lbound
    }
    if let Some(k) = null_at {
        let mut bitmap = vec![0xffu8; n.div_ceil(8)];
        if n > 0 {
            let k = k % n;
            bitmap[k / 8] &= !(1 << (k % 8));
        }
        v.extend_from_slice(&bitmap);
    }
    while v.len() < overhead {
        v.push(0);
    }
    for e in elems {
        v.extend_from_slice(&e.to_ne_bytes()[..elemsz]);
    }
    // NB: with a null hole the C data area holds n elements anyway (the
    // driver never reaches the data on the null path — both sides error
    // first), so keeping all n slots is fine and keeps images identical.
    v
}

// ---------------------------------------------------------------------------
// arms
// ---------------------------------------------------------------------------

fn arm_seg_in(r: &mut Rdr) {
    let s = cstring_of(r.rest(), 256);
    let mut cimg = [0u8; 12];
    let crc = unsafe { pg_cb_seg_in(s.as_ptr(), cimg.as_mut_ptr()) };

    let fc = fc_call(segf("seg_in"), [Datum::from_usize(s.as_ptr() as usize)]);
    if !check_verdict("seg_in", &s, crc, &fc.result) {
        return;
    }
    let d = *fc.result.as_ref().unwrap();
    let rimg = datum_bytes(d, 12);
    assert_eq!(rimg, &cimg[..], "seg_in image diverged for {s:?}");

    // roundtrip through seg_out (parse-produced sigd/ext domain)
    arm_seg_out_on(&cimg, &s);
}

fn arm_seg_out_on(img: &[u8; 12], ctx: &dyn std::fmt::Debug) {
    let mut cbuf = [0u8; 64];
    let crc = unsafe { pg_cb_seg_out(img.as_ptr(), cbuf.as_mut_ptr().cast(), 64) };
    assert_eq!(crc, 0, "seg_out C errored ctx={ctx:?}");
    let cs = &cbuf[..cbuf.iter().position(|&c| c == 0).unwrap()];

    let fc = fc_call(segf("seg_out"), [Datum::from_usize(img.as_ptr() as usize)]);
    let d = fc.result.as_ref().unwrap_or_else(|e| {
        panic!("seg_out rust errored ({}) ctx={ctx:?}", e.message)
    });
    let rs = datum_cstr(*d);

    // LIBC-SPELLING CARVE (macOS host only, two-sided): the ratified oracle
    // platform is Linux/aarch64 glibc, whose printf spells sign-bit NaNs
    // "-nan"; Darwin libc omits the sign, and the local oracle links Darwin
    // libc (the -funsigned-char pin cannot reach printf). The crate follows
    // glibc, so on a macOS host we canonicalize "-nan" -> "nan" on BOTH
    // sides before comparing; the fleet (Linux) comparison stays exact.
    #[cfg(target_os = "macos")]
    let (rs, cs) = (
        canon_darwin_nan(rs),
        canon_darwin_nan(cs),
    );
    #[cfg(target_os = "macos")]
    let (rs, cs) = (rs.as_slice(), cs.as_slice());

    assert_eq!(rs, cs, "seg_out text diverged ctx={ctx:?}");
}

/// Canonicalize every NaN coordinate of a cube varlena image (8-byte
/// little-endian f64s from offset 8) to the positive quiet-NaN pattern
/// 0x7FF8000000000000. Two-sided — applied to BOTH images at the
/// cube_enlarge compare; see the NAN-PAYLOAD CARVE comment there.
fn canon_cube_nan(img: &[u8]) -> Vec<u8> {
    let mut out = img.to_vec();
    if out.len() > 8 {
        for c in out[8..].chunks_exact_mut(8) {
            if f64::from_bits(u64::from_le_bytes(c.try_into().unwrap())).is_nan() {
                c.copy_from_slice(&0x7FF8_0000_0000_0000u64.to_le_bytes());
            }
        }
    }
    out
}

/// Rewrite every "-nan" to "nan" (Darwin-host canonicalization; see the
/// carve comment in `arm_seg_out_on`).
#[cfg(target_os = "macos")]
fn canon_darwin_nan(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'-' && s[i + 1..].starts_with(b"nan") {
            out.extend_from_slice(b"nan");
            i += 4;
        } else {
            out.push(s[i]);
            i += 1;
        }
    }
    out
}

fn arm_seg_out(r: &mut Rdr) {
    let mut img = [0u8; 12];
    for (i, v) in r.take(12).iter().enumerate() {
        img[i] = *v;
    }
    arm_seg_out_on(&img, &img);
}

const SEG_BINOPS: [&str; 16] = [
    "seg_cmp",
    "seg_lt",
    "seg_le",
    "seg_gt",
    "seg_ge",
    "seg_same",
    "seg_different",
    "seg_contains",
    "seg_contained",
    "seg_overlap",
    "seg_left",
    "seg_right",
    "seg_over_left",
    "seg_over_right",
    "seg_union",
    "seg_inter",
];

fn arm_seg_binops(r: &mut Rdr) {
    let mut a = [0u8; 12];
    let mut b = [0u8; 12];
    for (i, v) in r.take(12).iter().enumerate() {
        a[i] = *v;
    }
    for (i, v) in r.take(12).iter().enumerate() {
        b[i] = *v;
    }
    for (op, name) in SEG_BINOPS.iter().enumerate() {
        let mut iv: i32 = 0;
        let mut simg = [0u8; 12];
        let crc = unsafe {
            pg_cb_seg_binop(op as i32, a.as_ptr(), b.as_ptr(), &mut iv, simg.as_mut_ptr())
        };
        let fc = fc_call(
            segf(name),
            [
                Datum::from_usize(a.as_ptr() as usize),
                Datum::from_usize(b.as_ptr() as usize),
            ],
        );
        let ctx = (name, a, b);
        if !check_verdict(name, &ctx, crc, &fc.result) {
            continue;
        }
        let d = *fc.result.as_ref().unwrap();
        match op {
            14 | 15 => assert_eq!(datum_bytes(d, 12), &simg[..], "{name} image diverged {ctx:?}"),
            0 => assert_eq!(d.as_i32(), iv, "{name} diverged {ctx:?}"),
            _ => assert_eq!(d.as_bool(), iv != 0, "{name} diverged {ctx:?}"),
        }
    }
}

fn arm_seg_unops(r: &mut Rdr) {
    let mut a = [0u8; 12];
    for (i, v) in r.take(12).iter().enumerate() {
        a[i] = *v;
    }
    for (op, name) in ["seg_center", "seg_lower", "seg_upper", "seg_size"].iter().enumerate() {
        let mut bits: u32 = 0;
        let crc = unsafe { pg_cb_seg_unop(op as i32, a.as_ptr(), &mut bits) };
        assert_eq!(crc, 0, "{name} C errored {a:?}");
        let fc = fc_call(segf(name), [Datum::from_usize(a.as_ptr() as usize)]);
        let d = fc.result.as_ref().unwrap_or_else(|e| {
            panic!("{name} rust errored ({}) {a:?}", e.message)
        });
        assert_eq!(
            d.as_usize() as u32,
            bits,
            "{name} float4 bits diverged {a:?}"
        );
    }
}

fn arm_cube_in(r: &mut Rdr) {
    let s = cstring_of(r.rest(), 512);
    let mut cimg = vec![0u8; 2048];
    let mut clen: i32 = 0;
    let crc = unsafe { pg_cb_cube_in(s.as_ptr(), cimg.as_mut_ptr(), 2048, &mut clen) };

    let fc = fc_call(cubef("cube_in"), [Datum::from_usize(s.as_ptr() as usize)]);
    if !check_verdict("cube_in", &s, crc, &fc.result) {
        return;
    }
    let d = *fc.result.as_ref().unwrap();
    let cimg = &cimg[..clen as usize];
    let rimg = datum_bytes(d, clen as usize);
    // compare via C-measured length, then confirm rust image length agrees
    let rhdr = u32::from_ne_bytes(rimg[0..4].try_into().unwrap()) >> 2;
    assert_eq!(rhdr as usize, clen as usize, "cube_in image length diverged for {s:?}");
    assert_eq!(rimg, cimg, "cube_in image diverged for {s:?}");
    drop(fc);
    arm_cube_io_on(cimg, &s);
}

/// cube_out + cube_send + dim/is_point on a known-valid image.
fn arm_cube_io_on(img: &[u8], ctx: &dyn std::fmt::Debug) {
    let mut cbuf = vec![0u8; 8192];
    let crc = unsafe { pg_cb_cube_out(img.as_ptr(), img.len() as i32, cbuf.as_mut_ptr().cast(), 8192) };
    assert_eq!(crc, 0, "cube_out C errored ctx={ctx:?}");
    let cs = &cbuf[..cbuf.iter().position(|&c| c == 0).unwrap()];
    let fc = fc_call(cubef("cube_out"), [Datum::from_usize(img.as_ptr() as usize)]);
    let d = fc.result.as_ref().unwrap_or_else(|e| {
        panic!("cube_out rust errored ({}) ctx={ctx:?}", e.message)
    });
    assert_eq!(datum_cstr(*d), cs, "cube_out text diverged ctx={ctx:?}");
    drop(fc);

    let mut csend = vec![0u8; 2048];
    let mut csendlen: i32 = 0;
    let crc = unsafe {
        pg_cb_cube_send(img.as_ptr(), img.len() as i32, csend.as_mut_ptr(), 2048, &mut csendlen)
    };
    assert_eq!(crc, 0, "cube_send C errored ctx={ctx:?}");
    let fc = fc_call(cubef("cube_send"), [Datum::from_usize(img.as_ptr() as usize)]);
    let d = fc.result.as_ref().unwrap_or_else(|e| {
        panic!("cube_send rust errored ({}) ctx={ctx:?}", e.message)
    });
    assert_eq!(
        datum_varlena_payload(*d),
        &csend[..csendlen as usize],
        "cube_send payload diverged ctx={ctx:?}"
    );
    drop(fc);

    for (op, name) in [(0, "cube_dim"), (1, "cube_is_point")] {
        let mut iv: i32 = 0;
        let mut fbits: u64 = 0;
        let mut dummy = [0u8; 8];
        let mut dlen = 0i32;
        let crc = unsafe {
            pg_cb_cube_unop(
                op,
                img.as_ptr(),
                img.len() as i32,
                0,
                0,
                0,
                &mut iv,
                &mut fbits,
                dummy.as_mut_ptr(),
                &mut dlen,
            )
        };
        assert_eq!(crc, 0, "{name} C errored ctx={ctx:?}");
        let fc = fc_call(cubef(name), [Datum::from_usize(img.as_ptr() as usize)]);
        let d = fc.result.as_ref().unwrap();
        if op == 0 {
            assert_eq!(d.as_i32(), iv, "{name} diverged ctx={ctx:?}");
        } else {
            assert_eq!(d.as_bool(), iv != 0, "{name} diverged ctx={ctx:?}");
        }
    }
}

fn arm_cube_unary_io(r: &mut Rdr) {
    let img = build_cube_image(r);
    arm_cube_io_on(&img, &img);
}

fn arm_cube_recv(r: &mut Rdr) {
    let structured = r.u8() & 1 == 0;
    let msg: Vec<u8> = if structured {
        let dim = (r.u8() as u32) % 104; // 101..103 drive the 54000 arm
        let point = r.u8() & 1 != 0;
        let truncate = r.u8();
        let header: u32 = dim | if point { 0x8000_0000 } else { 0 };
        let nitems = if point { dim } else { 2 * dim } as usize;
        let mut m = Vec::with_capacity(4 + 8 * nitems);
        m.extend_from_slice(&header.to_be_bytes());
        for _ in 0..nitems {
            m.extend_from_slice(&r.u64().to_be_bytes());
        }
        if truncate > 0 {
            let cut = (truncate as usize) % (m.len() + 1);
            m.truncate(m.len() - cut); // drive the 08P01 arm
        }
        m
    } else {
        r.take(1700).to_vec()
    };

    let mut cimg = vec![0u8; 2048];
    let mut clen: i32 = 0;
    let crc = unsafe { pg_cb_cube_recv(msg.as_ptr(), msg.len() as i32, cimg.as_mut_ptr(), 2048, &mut clen) };

    // Rust plane: StringInfo over the message bytes.
    let cx = mcx::MemoryContext::new("contribb_recv");
    let mut vbuf: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(cx.mcx(), msg.len() + 1).unwrap();
    mcx::vec_append_bytes(&mut vbuf, &msg).unwrap();
    let mut si = stringinfo::StringInfo::from_vec(vbuf).unwrap();
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    let rcx = mcx::MemoryContext::new("contribb_recv_result");
    // SAFETY: rcx outlives the call + compare below.
    unsafe { fcinfo.set_result_mcx(rcx.mcx()) };
    fcinfo.args[0] = NullableDatum::value(Datum::from_usize(&mut si as *mut _ as usize));
    let result = cubef("cube_recv")(None, &mut fcinfo);

    if !check_verdict("cube_recv", &msg, crc, &result) {
        return;
    }
    let d = result.unwrap();
    let rimg = datum_bytes(d, clen as usize);
    assert_eq!(rimg, &cimg[..clen as usize], "cube_recv image diverged for {msg:?}");
}

const CUBE_BINOPS: [&str; 15] = [
    "cube_cmp",
    "cube_eq",
    "cube_ne",
    "cube_lt",
    "cube_gt",
    "cube_le",
    "cube_ge",
    "cube_contains",
    "cube_contained",
    "cube_overlap",
    "cube_union",
    "cube_inter",
    "cube_distance",
    "distance_taxicab",
    "distance_chebyshev",
];

fn arm_cube_binops(r: &mut Rdr) {
    let a = build_cube_image(r);
    let b = build_cube_image(r);
    for (op, name) in CUBE_BINOPS.iter().enumerate() {
        let mut iv: i32 = 0;
        let mut fbits: u64 = 0;
        let mut imgout = vec![0u8; 2048];
        let mut imgoutlen: i32 = 0;
        let crc = unsafe {
            pg_cb_cube_binop(
                op as i32,
                a.as_ptr(),
                a.len() as i32,
                b.as_ptr(),
                b.len() as i32,
                &mut iv,
                &mut fbits,
                imgout.as_mut_ptr(),
                &mut imgoutlen,
            )
        };
        let fc = fc_call(
            cubef(name),
            [
                Datum::from_usize(a.as_ptr() as usize),
                Datum::from_usize(b.as_ptr() as usize),
            ],
        );
        let ctx = (name, &a, &b);
        if !check_verdict(name, &ctx, crc, &fc.result) {
            continue;
        }
        let d = *fc.result.as_ref().unwrap();
        match op {
            10 | 11 => assert_eq!(
                datum_bytes(d, imgoutlen as usize),
                &imgout[..imgoutlen as usize],
                "{name} image diverged {ctx:?}"
            ),
            12..=14 => assert_eq!(
                d.as_usize() as u64,
                fbits,
                "{name} float8 bits diverged {ctx:?}"
            ),
            0 => assert_eq!(d.as_i32(), iv, "{name} diverged {ctx:?}"),
            _ => assert_eq!(d.as_bool(), iv != 0, "{name} diverged {ctx:?}"),
        }
    }

    // op 15: cube_union with the SAME pointer as both args — the
    // pointer-identity trivial case (cube_union_v0's `if (a == b) return a`
    // / fc_cube_union's shortcut), which returns the input UNNORMALIZED.
    {
        let mut iv: i32 = 0;
        let mut fbits: u64 = 0;
        let mut imgout = vec![0u8; 2048];
        let mut imgoutlen: i32 = 0;
        let crc = unsafe {
            pg_cb_cube_binop(
                15,
                a.as_ptr(),
                a.len() as i32,
                a.as_ptr(),
                a.len() as i32,
                &mut iv,
                &mut fbits,
                imgout.as_mut_ptr(),
                &mut imgoutlen,
            )
        };
        let d = Datum::from_usize(a.as_ptr() as usize);
        let fc = fc_call(cubef("cube_union"), [d, d]);
        assert_eq!(crc, 0, "cube_union same-ptr C errored {a:?}");
        let du = *fc.result.as_ref().expect("cube_union same-ptr rust ok");
        assert_eq!(
            datum_bytes(du, imgoutlen as usize),
            &imgout[..imgoutlen as usize],
            "cube_union same-ptr image diverged {a:?}"
        );
    }
}

fn arm_cube_unops(r: &mut Rdr) {
    let img = build_cube_image(r);
    let n = r.i32();
    let f1 = r.f64bits();
    let f2 = r.f64bits();

    // (op, name, nargs-shape) — see pg_cb_cube_unop's op table
    for (op, name) in [
        (2, "cube_size"),
        (3, "cube_ll_coord"),
        (4, "cube_ur_coord"),
        (5, "cube_coord"),
        (6, "cube_coord_llur"),
        (7, "cube_enlarge"),
        (10, "cube_c_f8"),
        (11, "cube_c_f8_f8"),
    ] {
        let mut iv: i32 = 0;
        let mut fbits: u64 = 0;
        let mut imgout = vec![0u8; 2048];
        let mut imgoutlen: i32 = 0;
        let crc = unsafe {
            pg_cb_cube_unop(
                op,
                img.as_ptr(),
                img.len() as i32,
                n,
                f1,
                f2,
                &mut iv,
                &mut fbits,
                imgout.as_mut_ptr(),
                &mut imgoutlen,
            )
        };
        let imgd = Datum::from_usize(img.as_ptr() as usize);
        let fc = match op {
            2 => fc_call(cubef(name), [imgd]),
            3..=6 => fc_call(cubef(name), [imgd, Datum::from_i32(n)]),
            7 => fc_call(
                cubef(name),
                [imgd, Datum::from_u64(f1), Datum::from_i32(n)],
            ),
            10 => fc_call(cubef(name), [imgd, Datum::from_u64(f1)]),
            11 => fc_call(cubef(name), [imgd, Datum::from_u64(f1), Datum::from_u64(f2)]),
            _ => unreachable!(),
        };
        let ctx = (name, &img, n, f1, f2);
        if !check_verdict(name, &ctx, crc, &fc.result) {
            continue;
        }
        let d = *fc.result.as_ref().unwrap();
        match op {
            // NAN-PAYLOAD CARVE (two-sided): cube_enlarge does coordinate
            // ARITHMETIC (x - r / x + r / midpoint). With a NaN operand,
            // IEEE 754 leaves the result NaN's payload and sign unspecified,
            // and gcc/rustc legally commute the operands, so the two sides
            // return different NaN BIT PATTERNS for the same NaN-class value
            // (fleet leg pgrust-fuzz-campaign-1785628325-413a-3336: 6,534
            // unique inputs, every one "cube_enlarge image diverged", every
            // differing coordinate NaN on both sides). No cube operation
            // reads NaN payloads, so canonicalize every NaN coordinate in
            // BOTH images before the byte compare; NaN-vs-value and
            // value-vs-value differences still diverge.
            7 => assert_eq!(
                canon_cube_nan(datum_bytes(d, imgoutlen as usize)),
                canon_cube_nan(&imgout[..imgoutlen as usize]),
                "{name} image diverged {ctx:?}"
            ),
            10 | 11 => assert_eq!(
                datum_bytes(d, imgoutlen as usize),
                &imgout[..imgoutlen as usize],
                "{name} image diverged {ctx:?}"
            ),
            _ => assert_eq!(
                d.as_usize() as u64,
                fbits,
                "{name} float8 bits diverged {ctx:?}"
            ),
        }
    }

    // pure-scalar constructors (no cube arg)
    for (op, name) in [(8, "cube_f8"), (9, "cube_f8_f8")] {
        let mut iv: i32 = 0;
        let mut fbits: u64 = 0;
        let mut imgout = vec![0u8; 2048];
        let mut imgoutlen: i32 = 0;
        let crc = unsafe {
            pg_cb_cube_unop(
                op,
                img.as_ptr(),
                img.len() as i32,
                n,
                f1,
                f2,
                &mut iv,
                &mut fbits,
                imgout.as_mut_ptr(),
                &mut imgoutlen,
            )
        };
        let fc = if op == 8 {
            fc_call(cubef(name), [Datum::from_u64(f1)])
        } else {
            fc_call(cubef(name), [Datum::from_u64(f1), Datum::from_u64(f2)])
        };
        let ctx = (name, f1, f2);
        assert_eq!(crc, 0, "{name} C errored {ctx:?}");
        let d = *fc.result.as_ref().unwrap_or_else(|e| {
            panic!("{name} rust errored ({}) {ctx:?}", e.message)
        });
        assert_eq!(
            datum_bytes(d, imgoutlen as usize),
            &imgout[..imgoutlen as usize],
            "{name} image diverged {ctx:?}"
        );
    }
}

fn arm_cube_arrays(r: &mut Rdr) {
    let variant = r.u8() % 3;
    match variant {
        0 => {
            // cube_a_f8_f8(ur float8[], ll float8[])
            let n1 = (r.u8() as usize) % 104;
            let n2 = if r.u8() & 3 == 0 { (r.u8() as usize) % 104 } else { n1 };
            let null1 = if r.u8() & 7 == 0 { Some(r.u8() as usize) } else { None };
            let e1: Vec<u64> = (0..n1).map(|_| r.u64()).collect();
            let e2: Vec<u64> = (0..n2).map(|_| r.u64()).collect();
            let ur = build_array_image(701, &e1, 8, null1);
            let ll = build_array_image(701, &e2, 8, None);
            run_arrayop(0, "cube_a_f8_f8", &ur, &ll, &(n1, n2, null1));
        }
        1 => {
            // cube_a_f8(float8[])
            let n1 = (r.u8() as usize) % 104;
            let null1 = if r.u8() & 7 == 0 { Some(r.u8() as usize) } else { None };
            let e1: Vec<u64> = (0..n1).map(|_| r.u64()).collect();
            let ur = build_array_image(701, &e1, 8, null1);
            run_arrayop(1, "cube_a_f8", &ur, &[], &(n1, null1));
        }
        _ => {
            // cube_subset(cube, int4[])
            let cube = build_cube_image(r);
            let n1 = (r.u8() as usize) % 104;
            let null1 = if r.u8() & 7 == 0 { Some(r.u8() as usize) } else { None };
            // bias indexes toward the valid range, keep raw arm too
            let raw = r.u8() & 3 == 0;
            let e1: Vec<u64> = (0..n1)
                .map(|_| {
                    let v = r.u32();
                    if raw {
                        v as u64
                    } else {
                        ((v % 128) as u64).wrapping_sub((v >> 30 == 3) as u64)
                    }
                })
                .collect();
            let idx = build_array_image(23, &e1, 4, null1);
            run_arrayop(2, "cube_subset", &idx, &cube, &(n1, null1, raw));
        }
    }
}

fn run_arrayop(op: i32, name: &str, arr1: &[u8], arr2: &[u8], ctx: &dyn std::fmt::Debug) {
    let mut imgout = vec![0u8; 2048];
    let mut imgoutlen: i32 = 0;
    let crc = unsafe {
        pg_cb_cube_arrayop(
            op,
            arr1.as_ptr(),
            arr1.len() as i32,
            arr2.as_ptr(),
            arr2.len() as i32,
            imgout.as_mut_ptr(),
            &mut imgoutlen,
        )
    };
    let a1 = Datum::from_usize(arr1.as_ptr() as usize);
    let fc = match op {
        0 => fc_call(
            cubef(name),
            [a1, Datum::from_usize(arr2.as_ptr() as usize)],
        ),
        1 => fc_call(cubef(name), [a1]),
        _ => fc_call(
            cubef(name),
            [Datum::from_usize(arr2.as_ptr() as usize), a1],
        ),
    };
    if !check_verdict(name, &ctx, crc, &fc.result) {
        return;
    }
    let d = *fc.result.as_ref().unwrap();
    assert_eq!(
        datum_bytes(d, imgoutlen as usize),
        &imgout[..imgoutlen as usize],
        "{name} image diverged {ctx:?}"
    );
}

// ---------------------------------------------------------------------------
// entry
// ---------------------------------------------------------------------------

pub fn contribb_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.is_empty() {
        return;
    }
    fns();
    unsafe { pg_cb_reset() };
    let mut r = Rdr::new(&data[1..]);
    match data[0] % 10 {
        0 => arm_seg_in(&mut r),
        1 => arm_seg_out(&mut r),
        2 => arm_seg_binops(&mut r),
        3 => arm_seg_unops(&mut r),
        4 => arm_cube_in(&mut r),
        5 => arm_cube_unary_io(&mut r),
        6 => arm_cube_recv(&mut r),
        7 => arm_cube_binops(&mut r),
        8 => arm_cube_unops(&mut r),
        _ => arm_cube_arrays(&mut r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn c_dist_repro() {
        let _serial = crate::c_oracle_serial();
        fns();
        unsafe { pg_cb_reset() };
        let mut img = Vec::new();
        let size = 8 + 8 * 53;
        img.extend_from_slice(&(((size as u32) << 2).to_ne_bytes()));
        img.extend_from_slice(&(53u32 | 0x8000_0000).to_ne_bytes());
        let mut coords = vec![0u64; 53];
        coords[0] = 0xFFF70000000000FC;
        coords[1] = 0xE00000000000FCFF;
        coords[2] = 0xE00000000000003F;
        coords[3] = 0xBF;
        for c in &coords { img.extend_from_slice(&c.to_ne_bytes()); }
        let img_b: Vec<u8> = [(8u32 << 2).to_ne_bytes(), 0u32.to_ne_bytes()].concat();
        let (mut iv, mut fb, mut io, mut il) = (0i32, 0u64, vec![0u8; 2048], 0i32);
        let rc = unsafe { pg_cb_cube_binop(12, img.as_ptr(), img.len() as i32, img_b.as_ptr(), 8, &mut iv, &mut fb, io.as_mut_ptr(), &mut il) };
        // The clang -O3 NaN-lane witness (see build.rs -O2 pin): at the
        // pinned -O2 the C oracle matches scalar IEEE semantics (+Inf).
        assert_eq!((rc, fb), (0, f64::INFINITY.to_bits()), "iv={iv} il={il}");
    }

    #[test]
    fn fc_dist_repro() {
        fns();
        let mut coords = vec![0u64; 53];
        coords[0] = 0xFFF70000000000FC;
        coords[1] = (-2.6815615860270834e+154f64).to_bits();
        let mut img = Vec::new();
        let size = 8 + 8 * 53;
        img.extend_from_slice(&(((size as u32) << 2).to_ne_bytes()));
        img.extend_from_slice(&(53u32 | 0x8000_0000).to_ne_bytes());
        for c in &coords { img.extend_from_slice(&c.to_ne_bytes()); }
        let img_b: Vec<u8> = [(8u32 << 2).to_ne_bytes(), 0u32.to_ne_bytes()].concat();
        let fc = fc_call(
            cubef("cube_distance"),
            [Datum::from_usize(img.as_ptr() as usize), Datum::from_usize(img_b.as_ptr() as usize)],
        );
        let d = *fc.result.as_ref().unwrap();
        assert_eq!(d.as_usize() as u64, f64::INFINITY.to_bits());
        assert!(!fc.isnull);
    }

    fn run(sel: u8, body: &[u8]) {
        let mut v = vec![sel];
        v.extend_from_slice(body);
        contribb_diff(&v);
    }

    #[test]
    fn seg_out_signbit_nan() {
        // fleet regression (asan-treewide pass, 19 hits): a sign-bit NaN
        // boundary prints "-nan" under the ratified glibc oracle; the Rust
        // side used to drop the sign. Corpus seed
        // 010799a828f11daebc4210d799e387007afdaef9. On a macOS host the
        // driver canonicalizes the Darwin libc spelling on both sides.
        let mut body = Vec::new();
        body.extend_from_slice(&0xFFC0_0000u32.to_le_bytes()); // lower = -NaN
        body.extend_from_slice(&1.0f32.to_le_bytes()); // upper = 1.0
        body.extend_from_slice(&[1, 1, 0, 0]); // sigd/ext
        run(1, &body);
        // positive-NaN and quiet-payload variants
        body[0..4].copy_from_slice(&0x7FC0_0001u32.to_le_bytes());
        run(1, &body);
        body[4..8].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // upper -NaN too
        run(1, &body);
    }

    #[test]
    fn cube_enlarge_nan_payload_canon() {
        // fleet regression (job pgrust-fuzz-campaign-1785628325-413a-3336,
        // 6,534 unique inputs, all one shape): cube_enlarge with NaN r or
        // NaN coordinates returns NaN coordinates whose payload/sign bits
        // are operand-order dependent (gcc keeps the coordinate operand's
        // payload, rustc/LLVM keeps r's). canon_cube_nan must equalize
        // payload-only NaN differences and NOTHING else.
        fn img(coords: &[u64]) -> Vec<u8> {
            let mut v = vec![0u8; 8]; // varlena + dim header, irrelevant here
            for c in coords {
                v.extend_from_slice(&c.to_le_bytes());
            }
            v
        }
        let a = img(&[0xFFFF_FFFF_FFFF_FFFF, 1.5f64.to_bits()]); // -NaN(payload)
        let b = img(&[0xFFFF_FFFF_002D_6530, 1.5f64.to_bits()]); // NaN(ascii payload)
        let c = img(&[0x7FF8_0000_0000_0000, 1.5f64.to_bits()]); // canonical qNaN
        assert_eq!(canon_cube_nan(&a), canon_cube_nan(&b));
        assert_eq!(canon_cube_nan(&a), canon_cube_nan(&c));
        // negative controls: the carve must not mask value differences.
        let d = img(&[2.0f64.to_bits(), 1.5f64.to_bits()]); // NaN vs value
        assert_ne!(canon_cube_nan(&a), canon_cube_nan(&d));
        let e = img(&[0xFFFF_FFFF_FFFF_FFFF, 2.5f64.to_bits()]); // other coord
        assert_ne!(canon_cube_nan(&a), canon_cube_nan(&e));
        // non-NaN images pass through byte-identical.
        assert_eq!(canon_cube_nan(&d), d);
        // fleet crash shape end-to-end: cube_enlarge(1-dim point cube with a
        // payload-NaN coordinate, r = -NaN, n = -1) must agree under the
        // carve (arm_cube_unops layout: dim, point, coords, n, f1, f2).
        let mut body = Vec::new();
        body.push(1); // dim = 1
        body.push(1); // point
        body.extend_from_slice(&0xFFFF_FFFF_002D_6530u64.to_le_bytes()); // coord = NaN
        body.extend_from_slice(&(-1i32).to_le_bytes()); // n = -1
        body.extend_from_slice(&0xFFFF_FFFF_FFFF_FFFFu64.to_le_bytes()); // r = -NaN
        body.extend_from_slice(&0u64.to_le_bytes()); // f2
        run(8, &body);
    }

    #[test]
    fn seg_in_basic() {
        for s in [
            &b"1.5 .. 2.5"[..],
            b"<3 .. >4.0e1",
            b"5(+-)0.3",
            b"2.0 '+-' 1",
            b".. 5",
            b"5 ..",
            b"4 .. 2",
            b"garbage",
            b"",
            b"1e40",
            b"~3.5",
            b"0.000001 .. 100000",
        ] {
            run(0, s);
        }
    }

    #[test]
    fn seg_raw_images() {
        // raw sigd/ext domain incl. the XX000 bogus-boundary arm and
        // high-bit sigd (the -funsigned-char pin)
        run(1, &[0, 0, 0x80, 0x3f, 0, 0, 0xc0, 0x3f, 200, 3, b'<', b'-']);
        let mut b = vec![];
        b.extend_from_slice(&[0u8; 8]);
        b.extend_from_slice(&[5, 5, b'!', 0]);
        b.extend_from_slice(&[0u8; 8]);
        b.extend_from_slice(&[5, 5, b'?', 0]);
        run(2, &b);
        run(3, &[0, 0, 0x80, 0xff, 1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn cube_in_basic() {
        for s in [
            &b"(1,2),(3,4)"[..],
            b"[(1,2),(3,4)]",
            b"1,2,3",
            b"()",
            b"(inf, nan, -infinity)",
            b"(1,2),(3)",
            b"bogus",
            b"",
            b"[(1),(2)",
        ] {
            run(4, s);
        }
    }

    #[test]
    fn cube_ops_smoke() {
        let mut body = vec![3u8, 0]; // dim 3, non-point
        for i in 0..12u64 {
            body.extend_from_slice(&f64::from(i as u32).to_bits().to_le_bytes());
        }
        run(5, &body);
        run(7, &body);
        run(8, &body);
        run(9, &body);
        run(9, &[0, 5, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]);
        run(6, &[0, 2, 1, 0, 9, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 8]);
        run(6, &[0, 102, 0, 0]); // 54000 arm
        run(6, &[0, 2, 0, 3, 1, 2]); // truncation -> 08P01 arm
    }
}
