//! arrayfuncs_diff: differential fuzz driver — shipped Rust `arrayfuncs` vs vendored
//! PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_arrayfuncs_io.c). Crate under test: crates/backend/utils/adt/arrayfuncs.
//!
//! Comparison planes (float_in_diff conventions): value bytes/bits (array
//! images compared as exact bytes; array_out as exact cstring bytes;
//! element datums as i32 for byval int4 / full varlena bytes for text),
//! error-verdict, and errcode/sqlstate class. Message text is out of scope.
//! Errcode classes (must match csrc/pg_arrayfuncs_io.c):
//!   1 = 22P02 invalid_text_representation
//!   2 = 54000 program_limit_exceeded
//!   3 = 2202E array_subscript_error / array_element_error (one sqlstate)
//!   5 = 0A000 feature_not_supported
//!   6 = 22004 null_value_not_allowed
//!   7 = 22023 invalid_parameter_value
//!   8 = 22003 numeric_value_out_of_range
//!
//! Input layout: [sel][esel][payload]; sel % 11 picks the arm. `esel` selects
//! the pinned ELEMENT META, shared with the C oracle's pg_afx_metatab
//! (pg_type.dat values @ this stamp):
//!   0 int4 | 1 text | 2 "char" | 3 int2 | 4 int8 | 5 float4 | 6 float8
//!   7 name (byref fixed-len 64) | 8 oid | 9 tid (byref fixed-len 6)
//!   10 bool | 11 xid | 12 cstring (-2)
//! Selectors 2..12 drive the IMAGE-OPS arms only (2..8); those C/Rust bodies
//! never call an element input/output function. Arms 0/1 (array_in/array_out)
//! clamp to 0/1, the only selectors with in/out procs; arm 9 uses esel % 3
//! with 1 = float8 and 2 = text-through-the-variable-width path.
//!
//! MODE BYTE: image-ops arms 2..7 read a mode byte after the image. Bit 0 =
//! FIXED-LENGTH CONTAINER (pass a positive arraytyplen = elmlen * k, byval
//! metas only — drives the fixed-length branches and the two
//! "…of fixed-length arrays not implemented" 0A000 arms). Bit 1 = WIDE
//! BOUNDS (subscripts/bounds drawn as full-range i32 instead of i8 — drives
//! the 54000 "array size exceeds the maximum allowed" overflow arms). Bit 2
//! on the deconstruct arm = BUILTIN-TABLE MODE (route through
//! deconstruct_array_builtin so the hardcoded meta table is itself
//! dual-executed against construct.rs builtin_meta). Bit 2 on the construct
//! arm = BUILTIN-TABLE MODE too: mode bits 2|3 route construct through
//! construct_array (bit 2 alone) or construct_array_builtin (bits 2+3), so
//! BOTH of C's hardcoded meta tables are dual-executed. C carries TWO
//! DIFFERENT tables — construct_array_builtin accepts 12 element types,
//! deconstruct_array_builtin only 8 (no float4/int8/name/regtype/xid) —
//! while the crate shares one builtin_meta for both. See KNOWN-DIV-4.
//!
//! PLATFORM CARVE (1-byte metas, esel 2 and 10): C's fetch_att 1-byte arm is
//! CharGetDatum(*(const char *)T) — `char` signedness is
//! implementation-defined (PG documents the variance and uses `signed char` /
//! `unsigned char` casts explicitly where it matters). It is SIGNED on
//! macOS/arm64 and on x86-64 Linux, but UNSIGNED on Linux aarch64, which is
//! where the fleet campaign runs. The value plane for 1-byte byval metas
//! therefore compares the LOW 8 BITS (u8 width) — the width the type
//! actually defines — and the upper Datum bits are not asserted for those
//! two selectors only. Every other width asserts the full Datum word.
//! Array images for arms 1..8 are
//! BUILT on the Rust side from the payload with the crate's own
//! construct_md_array (bounded: ndim <= 3, dims <= 2 per dim — except 1-D
//! arrays get up to 32 elements so null-bitmap copies cross byte
//! boundaries — text payloads <= 8 bytes), then handed byte-identically to
//! both sides.
//!   0 array_in            payload = literal bytes (cut at first NUL; must
//!                         be UTF-8 — the Rust entry takes &str; encoding
//!                         validation is upstream of the crate in pgwire).
//!                         escontext=None (hard) compared to C on all three
//!                         planes; PLUS a Rust-only soft-vs-hard consistency
//!                         plane through a fresh ErrorSaveNode.
//!   1 array_out           image -> cstring, exact bytes.
//!   2 array_get_element   nsub (0..6) + i8 subscripts; value + isnull.
//!   3 array_get_slice     i8 bounds + provided bitmask; image bytes (and
//!                         the C-scribbled upper/lower arrays).
//!   4 array_set_element   i8 subscripts + elem-or-null; image bytes
//!                         (1-D extension + null-insertion paths included).
//!   5 array_set_slice     two built images + i8 bounds + provided bits.
//!   6 deconstruct_array   allow_nulls flag; per-element datum/null compare.
//!   7 construct_md_array  raw ndims (incl. <0 and >MAXDIM probes), dims %3,
//!                         FULL-RANGE i32 lbs (ArrayCheckBounds overflow
//!                         plane), per-element null bits. nitems==0 covers
//!                         construct_empty_array.
//!   8 array_contains_nulls image -> bool.
//!   9 width_bucket_array  esel%3: int4 (fixed path) / float8 (dedicated
//!                         path) / text with C collation (variable path;
//!                         oracle comparator = verbatim-transcribed
//!                         varstr_cmp collate-is-c arm).
//!  10 array_get_integer_typmods  cstring list (ASCII alphabet) + shape
//!                         selector (normal / wrong-elemtype / 2-D / with
//!                         NULL) driving all three error arms + values.
//!
//! WIDE-BOUNDS DOMAIN BOUND: with bit 1 set, bounds are full-range i32 but
//! the arms that would ALLOCATE before the overflow check are fenced. Both
//! sides run their pg_sub/add_s32_overflow (Rust: overflowing_sub/add)
//! checks BEFORE any palloc of the new image, so the 54000 arms are reached
//! without a huge allocation. The one path that sizes an allocation from
//! bounds without an intervening overflow check is a 1-D set_element/
//! set_slice EXTENSION whose new dim is large but not overflowing (e.g.
//! indx[0] = 2^30): C would palloc ~GB. The driver therefore rejects wide
//! candidates whose implied new dimension exceeds PG_AFX_WIDE_DIM_CAP
//! (1<<20) while staying inside i32 — i.e. it drives exactly the OVERFLOW
//! arms, not the merely-large ones. Documented rather than silently clamped.
//!
//! DRIVER PRECONDITION CARVE (documented in the oracle header): the C
//! oracle reads out of bounds on corrupt array headers (as C does), so
//! every image handed to C is a well-formed plain 4B-header image built by
//! the crate's construct_md_array. The raw-fuzz-bytes-as-image plane is
//! therefore NOT driven; header production is covered via array_in and
//! construct_md_array instead.
//!
//! FC-WRAPPER PLANE: NOT driven for the io/element entries (the core
//! entries are compared against C directly; the builtins.rs fc_* wrappers
//! for array_in/array_out need catalog-backed get_type_io_data state that
//! is out of this pure phase-1 target's scope). width_bucket_array IS
//! driven through its real wrapper ops::fc_width_bucket_array (native
//! LocalFcinfo + typcache-cached comparator).
//!
//! SKIPPED rows (with reasons):
//!   - array_recv/array_send: wire-format entries are a separate lane
//!     (pqformat plumbing); not chartered for this target.
//!   - CopyArrayEls-only / expanded-array (array_get_element_expanded,
//!     array_set_element_expanded, expand_array): expanded datums are
//!     executor-state machinery, unreachable from flat images (the C
//!     oracle stubs them with abort()).
//!   - width_bucket_array over collation-sensitive text (non-C collations):
//!     locale machinery is out of pure phase-1 scope; the C-collation
//!     memcmp path is driven.
//!   - array_in/array_out for element metas 2..12: would need a full
//!     in/out proc per type (adt_int int2/int8, float4/float8 out, name,
//!     tid, bool). Those element codecs are OTHER crates' lanes; the array
//!     level is already driven for both a byval and a byref codec. The
//!     image-ops arms cover every meta.
//!   - io.rs call1_armed (lines 18-23): NOT reachable from this target's
//!     drive path. array_in's element dispatch goes through
//!     types_fmgr::input_function_call_safe, and array_out's through
//!     call1_armed ONLY from the `fc_*` builtins wrapper layer
//!     (builtins.rs), which this target carves (needs catalog-backed
//!     get_type_io_data). Reported as a builtins-carve line, not closed
//!     here.

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_error::{
    PgError, SqlState, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_fmgr::{ErrorSaveNode, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, LocalFcinfo};

use arrayfuncs::construct::builtin_meta;
use arrayfuncs::{
    array_contains_nulls, array_get_element, array_get_integer_typmods, array_get_slice,
    array_in, array_out, array_set_element, array_set_slice, construct_md_array,
    deconstruct_array, ArrayIoMeta, MAXDIM,
};
use types_core::{
    BOOLOID, CHAROID, C_COLLATION_OID, CSTRINGOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, NAMEOID, OIDOID, Oid, TEXTOID, TIDOID, XIDOID,
};
use types_error::PgResult;

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;

    fn pg_diff_array_in(
        elemsel: i32,
        s: *const u8,
        typmod: i32,
        out_img: *mut *const u8,
        out_len: *mut usize,
    ) -> i32;
    fn pg_diff_array_out(
        elemsel: i32,
        img: *const u8,
        len: usize,
        out_str: *mut *const u8,
    ) -> i32;
    fn pg_diff_array_get_element(
        elemsel: i32,
        img: *const u8,
        len: usize,
        nsub: i32,
        indx: *const i32,
        arraytyplen: i32,
        out_val: *mut u64,
        out_ptr: *mut *const u8,
        out_size: *mut usize,
        out_isnull: *mut i32,
    ) -> i32;
    fn pg_diff_array_get_slice(
        elemsel: i32,
        img: *const u8,
        len: usize,
        nsub: i32,
        upper: *mut i32,
        lower: *mut i32,
        upper_provided: *const u8,
        lower_provided: *const u8,
        arraytyplen: i32,
        out_img: *mut *const u8,
        out_len: *mut usize,
    ) -> i32;
    fn pg_diff_array_set_element(
        elemsel: i32,
        img: *const u8,
        len: usize,
        nsub: i32,
        indx: *const i32,
        elem: *const u8,
        elem_len: usize,
        elem_isnull: i32,
        arraytyplen: i32,
        out_img: *mut *const u8,
        out_len: *mut usize,
    ) -> i32;
    fn pg_diff_array_set_slice(
        elemsel: i32,
        img: *const u8,
        len: usize,
        nsub: i32,
        upper: *mut i32,
        lower: *mut i32,
        upper_provided: *const u8,
        lower_provided: *const u8,
        src: *const u8,
        src_len: usize,
        arraytyplen: i32,
        out_img: *mut *const u8,
        out_len: *mut usize,
    ) -> i32;
    fn pg_diff_deconstruct_array(
        elemsel: i32,
        img: *const u8,
        len: usize,
        allow_nulls: i32,
        builtin_mode: i32,
        out_vals: *mut *const u64,
        out_nulls: *mut *const u8,
        out_n: *mut i32,
    ) -> i32;
    fn pg_diff_construct_md_array(
        elemsel: i32,
        elem_data: *const u8,
        elem_lens: *const i32,
        nulls: *const u8,
        nitems: i32,
        ndims: i32,
        dims: *const i32,
        lbs: *const i32,
        wrapper_1d: i32,
        out_img: *mut *const u8,
        out_len: *mut usize,
    ) -> i32;
    fn pg_diff_array_contains_nulls(img: *const u8, len: usize) -> i32;
    fn pg_diff_width_bucket_array(
        elemsel: i32,
        operand_bits: u64,
        operand_payload: *const u8,
        operand_len: usize,
        img: *const u8,
        len: usize,
        out_result: *mut i32,
    ) -> i32;
    fn pg_diff_array_get_integer_typmods(
        nelems: i32,
        strs: *const *const u8,
        shape: i32,
        out_vals: *mut *const i32,
        out_n: *mut i32,
    ) -> i32;
}

// ---------------------------------------------------------------------------
// Element-type plumbing (pinned metas + local text codec, exactly the shape
// crates/backend/utils/adt/arrayfuncs/src/tests.rs:20-110 drives).
// ---------------------------------------------------------------------------

/// Pinned element-meta table; MUST stay row-for-row identical to the C
/// oracle's pg_afx_metatab (pg_type.dat values @ 62d6c7d3df).
/// (oid, typlen, typbyval, typalign)
const METATAB: [(Oid, i32, bool, u8); NSEL] = [
    (INT4OID, 4, true, b'i'),      // 0  int4
    (TEXTOID, -1, false, b'i'),    // 1  text
    (CHAROID, 1, true, b'c'),      // 2  "char"
    (INT2OID, 2, true, b's'),      // 3  int2
    (INT8OID, 8, true, b'd'),      // 4  int8
    (FLOAT4OID, 4, true, b'i'),    // 5  float4
    (FLOAT8OID, 8, true, b'd'),    // 6  float8
    (NAMEOID, 64, false, b'c'),    // 7  name (byref fixed-len)
    (OIDOID, 4, true, b'i'),       // 8  oid
    (TIDOID, 6, false, b's'),      // 9  tid (byref fixed-len)
    (BOOLOID, 1, true, b'c'),      // 10 bool
    (XIDOID, 4, true, b'i'),       // 11 xid
    (CSTRINGOID, -2, false, b'c'), // 12 cstring
];
const NSEL: usize = 13;

/// Element types whose in/out procs this target shims (arms 0/1 only).
const NSEL_IO: i32 = 2;

fn meta_for(elemsel: i32) -> ArrayIoMeta {
    let (oid, typlen, typbyval, typalign) = METATAB[elemsel as usize];
    ArrayIoMeta {
        element_type: oid,
        typlen,
        typbyval,
        typalign,
        typdelim: b',',
        typioparam: oid,
    }
}

/// Bytes the driver supplies per element for this meta (fixed widths exact,
/// variable widths bounded small).
fn elem_width(elemsel: i32, r: &mut Rdr<'_>) -> usize {
    let (_, typlen, _, _) = METATAB[elemsel as usize];
    if typlen > 0 {
        typlen as usize
    } else {
        (r.u8() % 9) as usize
    }
}

/// Read `w` element bytes, normalised so BOTH sides receive identical
/// content. cstring (typlen -2) is NUL-terminated: an embedded NUL would make
/// C see a shorter string than the driver handed it — a driver-ENCODING
/// artifact, not a behavior difference — so NULs are mapped to '.' here,
/// before either side sees the bytes.
fn read_elem_bytes(elemsel: i32, w: usize, r: &mut Rdr<'_>) -> std::vec::Vec<u8> {
    let mut b = r.bytes(w);
    if METATAB[elemsel as usize].1 == -2 {
        for x in b.iter_mut() {
            if *x == 0 {
                *x = b'.';
            }
        }
    }
    b
}

/// Build one element Datum for `elemsel` from `bytes` (already elem_width
/// long for fixed types). byval goes through the crate's own fetch_att so the
/// word shape is the crate's, never the driver's.
fn make_elem<'mcx>(mcx: Mcx<'mcx>, elemsel: i32, bytes: &[u8]) -> Datum {
    let (_, typlen, typbyval, _) = METATAB[elemsel as usize];
    if typbyval {
        let mut word = [0u8; 8];
        word[..typlen as usize].copy_from_slice(&bytes[..typlen as usize]);
        return arrayfuncs::foundation::fetch_att(word.as_ptr(), true, typlen);
    }
    match typlen {
        -1 => build_varlena(mcx, bytes),
        -2 => {
            // cstring: NUL-terminated. Bytes arrive already NUL-free from
            // read_elem_bytes, so both sides hold the same string.
            let mut v: std::vec::Vec<u8> = bytes.to_vec();
            debug_assert!(!v.contains(&0));
            v.push(0);
            let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, v.len()).expect("alloc");
            buf.extend_from_slice(&v);
            let d = Datum::from_usize(buf.as_ptr() as usize);
            core::mem::forget(buf);
            d
        }
        n => {
            // fixed-length byref (name 64, tid 6): raw blob
            let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, n as usize).expect("alloc");
            buf.extend_from_slice(&bytes[..n as usize]);
            let d = Datum::from_usize(buf.as_ptr() as usize);
            core::mem::forget(buf);
            d
        }
    }
}

/// Compare one element Datum against the C oracle's word/bytes for `elemsel`.
/// See the PLATFORM CARVE note in the module header for the 1-byte metas.
fn assert_elem_eq(elemsel: i32, rd: Datum, cval: u64, cptr: *const u8, csize: usize, ctx: &str) {
    let (_, typlen, typbyval, _) = METATAB[elemsel as usize];
    if typbyval {
        if typlen == 1 {
            // PLATFORM CARVE: C `char` signedness differs between the local
            // host (signed) and the fleet's Linux aarch64 (unsigned).
            assert!(
                rd.as_usize() as u8 == cval as u8,
                "{ctx} DIVERGENCE (value, u8 width) esel={elemsel}: C={cval:#x} Rust={:#x}",
                rd.as_usize(),
            );
        } else {
            assert!(
                rd.as_usize() as u64 == cval,
                "{ctx} DIVERGENCE (value) esel={elemsel}: C={cval:#x} Rust={:#x}",
                rd.as_usize(),
            );
        }
        return;
    }
    let cb = unsafe { core::slice::from_raw_parts(cptr, csize) };
    let p = rd.as_usize() as *const u8;
    let n = match typlen {
        -1 => arrayfuncs::foundation::varsize_any(p),
        -2 => unsafe { core::ffi::CStr::from_ptr(p as *const core::ffi::c_char) }
            .to_bytes()
            .len()
            + 1,
        w => w as usize,
    };
    let rb = unsafe { core::slice::from_raw_parts(p, n) };
    assert!(
        rb == cb,
        "{ctx} DIVERGENCE (byref bytes) esel={elemsel}: C={} Rust={}",
        hex(cb),
        hex(rb),
    );
}

fn build_varlena<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> Datum {
    let total = ::datum::VARHDRSZ + payload.len();
    let mut img = mcx::vec_with_capacity_in(mcx, total).expect("mcx alloc");
    img.extend_from_slice(&::datum::varlena::set_varsize_4b(total));
    img.extend_from_slice(payload);
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    d
}

fn varlena_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    let total = arrayfuncs::foundation::varsize_any(p);
    // SAFETY: d points at a live plain varlena in a context that outlives
    // the current iteration.
    unsafe { core::slice::from_raw_parts(p, total) }
}

std::thread_local! {
    static TEXT_SCRATCH: core::cell::RefCell<std::vec::Vec<u8>> =
        const { core::cell::RefCell::new(std::vec::Vec::new()) };
}

fn fc_mytextin(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes().to_vec();
    Ok(build_varlena(fcinfo.result_mcx(), &s))
}
fn fc_mytextout(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = varlena_bytes(fcinfo.arg(0));
    let payload = v[::datum::VARHDRSZ..].to_vec();
    TEXT_SCRATCH.with(|c| {
        let mut b = c.borrow_mut();
        b.clear();
        b.extend_from_slice(&payload);
        b.push(0);
        Ok(Datum::from_usize(b.as_ptr() as usize))
    })
}

fn in_proc(elemsel: i32) -> FmgrInfo {
    assert!(elemsel < NSEL_IO, "in_proc for an image-ops-only meta");
    if elemsel == 0 {
        FmgrInfo::new(adt_int::builtins::fc_int4in, 42, 1, true, false)
    } else {
        FmgrInfo::new(fc_mytextin, 46, 1, true, false)
    }
}
fn out_proc(elemsel: i32) -> FmgrInfo {
    assert!(elemsel < NSEL_IO, "out_proc for an image-ops-only meta");
    if elemsel == 0 {
        FmgrInfo::new(adt_int::builtins::fc_int4out, 43, 1, true, false)
    } else {
        FmgrInfo::new(fc_mytextout, 47, 1, true, false)
    }
}

/// Map the Rust PgError sqlstate to the oracle's errcode class.
fn class_of(e: &PgError) -> i32 {
    let ss: SqlState = e.sqlstate();
    if ss == ERRCODE_INVALID_TEXT_REPRESENTATION {
        1
    } else if ss == ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        2
    } else if ss == ERRCODE_ARRAY_SUBSCRIPT_ERROR {
        3
    } else if ss == ERRCODE_FEATURE_NOT_SUPPORTED {
        5
    } else if ss == ERRCODE_NULL_VALUE_NOT_ALLOWED {
        6
    } else if ss == ERRCODE_INVALID_PARAMETER_VALUE {
        7
    } else if ss == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        8
    } else if ss == types_error::ERRCODE_INTERNAL_ERROR {
        9
    } else {
        0 // unmapped: always a divergence against the oracle's classes
    }
}

// ---------------------------------------------------------------------------
// Payload reader (zero-extended past end, arrayutils_diff conventions).
// ---------------------------------------------------------------------------

struct Rdr<'a> {
    d: &'a [u8],
    pos: usize,
}

impl<'a> Rdr<'a> {
    fn u8(&mut self) -> u8 {
        let v = self.d.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }
    fn i32le(&mut self) -> i32 {
        let mut b = [0u8; 4];
        for (i, s) in b.iter_mut().enumerate() {
            *s = self.d.get(self.pos + i).copied().unwrap_or(0);
        }
        self.pos += 4;
        i32::from_le_bytes(b)
    }
    fn u64le(&mut self) -> u64 {
        let mut b = [0u8; 8];
        for (i, s) in b.iter_mut().enumerate() {
            *s = self.d.get(self.pos + i).copied().unwrap_or(0);
        }
        self.pos += 8;
        u64::from_le_bytes(b)
    }
    fn bytes(&mut self, n: usize) -> std::vec::Vec<u8> {
        let mut v = std::vec::Vec::with_capacity(n);
        for _ in 0..n {
            v.push(self.u8());
        }
        v
    }
}

/// Build a well-formed array image from the payload with the crate's own
/// construct_md_array (bounded; see module header). Also returns the raw
/// element bytes/lens/nulls in the encoding the C oracle's construct entry
/// expects, so an identical image can be built on the C side when needed.
fn build_image<'mcx>(mcx: Mcx<'mcx>, elemsel: i32, r: &mut Rdr<'_>) -> PgVec<'mcx, u8> {
    build_image_full(mcx, elemsel, r).0
}

#[allow(clippy::type_complexity)]
fn build_image_full<'mcx>(
    mcx: Mcx<'mcx>,
    elemsel: i32,
    r: &mut Rdr<'_>,
) -> (PgVec<'mcx, u8>, std::vec::Vec<u8>, std::vec::Vec<i32>, std::vec::Vec<u8>) {
    let nd_byte = r.u8();
    let ndim = (nd_byte % 4) as i32; // 0..=3 (low bits: stable for the bank)
    // High bit picks the dim scale: 0..2 (may collapse to the empty array) or
    // 1..4 (always non-empty, and big enough that an interior slice leaves a
    // non-zero `dist` — the multi-dim seek/copy branches in slice_size,
    // extract_slice and insert_slice).
    let big = nd_byte & 0x80 != 0;
    let mut dims = [0i32; MAXDIM];
    let mut lbs = [1i32; MAXDIM];
    for i in 0..ndim as usize {
        dims[i] = if big {
            (r.u8() % 4) as i32 + 1
        } else {
            (r.u8() % 3) as i32
        };
        lbs[i] = (r.u8() as i8) as i32;
    }
    if ndim == 1 {
        // Wider 1-D arrays so null-bitmap copies cross byte boundaries.
        dims[0] = (r.u8() % 33) as i32;
    }
    let nitems: i64 = if ndim == 0 {
        0
    } else {
        dims[..ndim as usize]
            .iter()
            .fold(1i64, |acc, &d| acc.saturating_mul(d as i64))
    };
    let nitems = nitems as usize;
    let nullbits = r.u64le();
    let mut elems: std::vec::Vec<Datum> = std::vec::Vec::with_capacity(nitems);
    let mut nulls: std::vec::Vec<bool> = std::vec::Vec::with_capacity(nitems);
    let mut c_data: std::vec::Vec<u8> = std::vec::Vec::new();
    let mut c_lens: std::vec::Vec<i32> = std::vec::Vec::new();
    let mut c_nulls: std::vec::Vec<u8> = std::vec::Vec::new();
    for i in 0..nitems {
        let isnull = (nullbits >> (i % 64)) & 1 == 1;
        nulls.push(isnull);
        c_nulls.push(isnull as u8);
        if isnull {
            elems.push(Datum::null());
            c_lens.push(0);
            continue;
        }
        let w = elem_width(elemsel, r);
        let b = read_elem_bytes(elemsel, w, r);
        elems.push(make_elem(mcx, elemsel, &b));
        c_lens.push(b.len() as i32);
        c_data.extend_from_slice(&b);
    }
    let (elmtype, elmlen, elmbyval, elmalign) = METATAB[elemsel as usize];
    let img = construct_md_array(
        mcx, &elems, Some(&nulls), ndim, &dims, &lbs, elmtype, elmlen, elmbyval, elmalign,
    )
    .expect("bounded build_image inputs must construct");
    (img, c_data, c_lens, c_nulls)
}

/// Mode byte for the image-ops arms (see module header).
#[derive(Clone, Copy)]
struct Mode {
    fixed: bool,
    wide: bool,
    alt: bool,
    alt2: bool,
}

/// Implied-new-dimension cap for wide bounds: above this a 1-D extension
/// would ask C's palloc for a huge block before any overflow check fires
/// (module header WIDE-BOUNDS DOMAIN BOUND).
const WIDE_DIM_CAP: i64 = 1 << 20;

fn read_mode(elemsel: i32, r: &mut Rdr<'_>) -> Mode {
    let m = r.u8();
    let (_, _, byval, _) = METATAB[elemsel as usize];
    Mode {
        // fixed-length containers are byval-only in practice (C computes
        // arraytyplen / elmlen and stores in place); byref metas would need a
        // container type that does not exist in pg_type.
        fixed: m & 1 == 1 && byval,
        wide: m & 2 == 2,
        alt: m & 4 == 4,
        alt2: m & 8 == 8,
    }
}

/// Element oids C's deconstruct_array_builtin table accepts (arrayfuncs.c
/// 3696..3764) — a STRICT SUBSET of construct_array_builtin's 12 rows.
const C_DECONSTRUCT_BUILTIN_OIDS: [Oid; 8] =
    [CHAROID, CSTRINGOID, FLOAT8OID, INT2OID, INT4OID, OIDOID, TEXTOID, TIDOID];

/// Element oids C's construct_array_builtin table accepts (arrayfuncs.c
/// 3380..3492) — 12 rows; identical to the crate's builtin_meta rows.
const C_CONSTRUCT_BUILTIN_OIDS: [Oid; 12] = [
    CHAROID, CSTRINGOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NAMEOID, OIDOID,
    TEXTOID, TIDOID, XIDOID,
];

/// KNOWN-DIV-4/5 FIXED (RATIFIED Michael 2026-07-31): the crate now keeps
/// C's two asymmetric builtin tables (construct 12+regtype rows /
/// deconstruct 8 rows) and ERRORS XX000 (class 9) on unlisted oids exactly
/// like C's elog default arms — so every meta, including bool (in neither
/// table), dual-executes strict-parity through both builtin routes.
fn builtin_route_ok(_oid: Oid, _deconstruct: bool) -> bool {
    true
}

/// arraytyplen for the fixed-length container mode: elmlen * k, k in 1..=8.
fn fixed_typlen(elemsel: i32, r: &mut Rdr<'_>) -> i32 {
    let (_, elmlen, _, _) = METATAB[elemsel as usize];
    elmlen * ((r.u8() % 8) as i32 + 1)
}

/// Subscripts that make `lb - indx` or `indx - (dim + lb)` overflow i32 sit
/// within a few counts of the i32 ends, so a uniform i32 draw never finds
/// them (~2^-31). Wide mode therefore draws from this extremes table most of
/// the time and uniformly otherwise — the standard extremes-dictionary trick,
/// and the only way the 54000 "array size exceeds the maximum allowed" arms
/// in the 1-D extension paths are reachable.
const WIDE_EXTREMES: [i32; 12] = [
    i32::MIN,
    i32::MIN + 1,
    i32::MIN + 2,
    i32::MAX,
    i32::MAX - 1,
    i32::MAX - 2,
    1 << 30,
    -(1 << 30),
    (1 << 30) + 1,
    -((1 << 30) + 1),
    0,
    -1,
];

/// A subscript/bound: i8-derived normally, extremes-biased i32 in wide mode.
fn subscript(mode: Mode, r: &mut Rdr<'_>) -> i32 {
    if mode.wide {
        let sel = r.u8();
        if sel < 208 {
            WIDE_EXTREMES[(sel as usize) % WIDE_EXTREMES.len()]
        } else {
            r.i32le()
        }
    } else {
        (r.u8() as i8) as i32
    }
}

/// Reject wide candidates that are merely LARGE (huge allocation on both
/// sides) rather than overflowing: see WIDE-BOUNDS DOMAIN BOUND.
fn wide_alloc_safe(img: &[u8], nsub: usize, idx: &[i32]) -> bool {
    let (ndim, dims, lbs) = arrayfuncs::read_dims_lbounds(img);
    if ndim != 1 || nsub != 1 {
        return true; // multi-dim paths error before sizing anything
    }
    let (d0, l0) = (dims[0] as i64, lbs[0] as i64);
    let i0 = idx[0] as i64;
    let newdim = if i0 < l0 {
        d0 + (l0 - i0)
    } else if i0 >= d0 + l0 {
        d0 + (i0 - (d0 + l0) + 1)
    } else {
        d0
    };
    // in-i32 and large => skip; overflowing (outside i32) => keep, that is
    // exactly the 54000 arm we want.
    !(newdim > WIDE_DIM_CAP && newdim <= i32::MAX as i64)
}

fn cerr() -> i32 {
    unsafe { pg_diff_errcode_get() }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

fn init_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    // Pinned ENVIRONMENT for the width_bucket arm (never the computation):
    // fc_width_bucket_array flattens its array argument through the detoast
    // seam and resolves its comparator through typcache -> syscache/opclass
    // seams -> fmgr_info. Install the real detoast + fmgr units and a pinned
    // pg_type/pg_opclass/pg_amproc fixture carrying EXACTLY the pg_*.dat
    // rows for int4 (btree opclass 1978, family 1976, proc 351 btint4cmp)
    // and text (btree opclass 3126, family 1994, proc 360 bttextcmp).
    // catch_unwind tolerates another lane's oracle installing a seam first
    // (double-install panics; all lanes share one test binary).
    ONCE.call_once(|| {
        let _ = std::panic::catch_unwind(detoast::init_seams);
        let _ = std::panic::catch_unwind(fmgr_core::init_seams);
        let _ = std::panic::catch_unwind(|| {
            syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
                let mk = |name: &str, typlen: i16, typbyval: bool, typalign: u8| {
                    let mut typname = types_tuple::NameData::default();
                    typname.namestrcpy(name);
                    syscache_seams::PgTypeTypcacheShape {
                        typname,
                        typlen,
                        typbyval,
                        typalign: typalign as i8,
                        typstorage: if typlen < 0 { b'x' as i8 } else { b'p' as i8 },
                        typtype: b'b' as i8,
                        typisdefined: true,
                        typrelid: types_core::InvalidOid,
                        typsubscript: types_core::InvalidOid,
                        typelem: types_core::InvalidOid,
                        typarray: types_core::InvalidOid,
                        typcollation: if typid == TEXTOID {
                            types_core::catalog::DEFAULT_COLLATION_OID
                        } else {
                            types_core::InvalidOid
                        },
                    }
                };
                Ok(match typid {
                    INT4OID => Some(mk("int4", 4, true, b'i')),
                    TEXTOID => Some(mk("text", -1, false, b'i')),
                    _ => None,
                })
            });
        });
        let _ = std::panic::catch_unwind(|| {
            syscache_seams::syscache_hash_value_typeoid::set(|typid| {
                Ok(typid.wrapping_mul(0x9e37_79b1))
            });
        });
        let _ = std::panic::catch_unwind(|| {
            syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
                Ok(match opclass {
                    1978 => Some(syscache_seams::PgOpclassShape {
                        opcmethod: 403, // BTREE_AM_OID
                        opcfamily: 1976,
                        opcintype: INT4OID,
                        opckeytype: 0,
                    }),
                    3126 => Some(syscache_seams::PgOpclassShape {
                        opcmethod: 403,
                        opcfamily: 1994,
                        opcintype: TEXTOID,
                        opckeytype: 0,
                    }),
                    _ => None,
                })
            });
        });
        let _ = std::panic::catch_unwind(|| {
            syscache_seams::lookup_pg_amproc::set(|opfamily, lefttype, righttype, procnum| {
                Ok(match (opfamily, lefttype, righttype, procnum) {
                    (1976, INT4OID, INT4OID, 1) => 351, // btint4cmp
                    (1994, TEXTOID, TEXTOID, 1) => 360, // bttextcmp
                    _ => types_core::InvalidOid,
                })
            });
        });
        let _ = std::panic::catch_unwind(|| {
            indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
                Ok(match (type_id, am_id) {
                    (INT4OID, 403) => 1978,
                    (TEXTOID, 403) => 3126,
                    _ => types_core::InvalidOid,
                })
            });
        });
    });
}

pub fn arrayfuncs_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    init_seams();
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&esel_raw, payload)) = rest.split_first() else {
        return;
    };
    let esel = (esel_raw as usize % NSEL) as i32;
    let ctx = MemoryContext::new_bump("arrayfuncs_diff");
    let mcx = ctx.mcx();
    let mut r = Rdr { d: payload, pos: 0 };
    match sel % 11 {
        // arms 0/1 need element in/out procs: clamp to the two shimmed metas
        0 => array_in_diff(mcx, esel % NSEL_IO, payload),
        1 => array_out_diff(mcx, esel % NSEL_IO, &mut r, payload),
        2 => get_element_diff(mcx, esel, &mut r, payload),
        3 => get_slice_diff(mcx, esel, &mut r, payload),
        4 => set_element_diff(mcx, esel, &mut r, payload),
        5 => set_slice_diff(mcx, esel, &mut r, payload),
        6 => deconstruct_diff(mcx, esel, &mut r, payload),
        7 => construct_diff(mcx, esel, &mut r, payload),
        8 => contains_nulls_diff(mcx, esel, &mut r, payload),
        9 => width_bucket_diff(mcx, (esel_raw % 3) as i32, &mut r, payload),
        _ => typmods_diff(mcx, &mut r, payload),
    }
}

// ---------------------------------------------------------------------------
// Arm 0: array_in (oid 750)
// ---------------------------------------------------------------------------

fn array_in_diff(mcx: Mcx<'_>, esel: i32, payload: &[u8]) {
    let s = match payload.iter().position(|&b| b == 0) {
        Some(p) => &payload[..p],
        None => payload,
    };
    let Ok(text) = core::str::from_utf8(s) else {
        return; // encoding validation is upstream of the crate (see header)
    };
    let mut cstr = s.to_vec();
    cstr.push(0);

    let mut cimg: *const u8 = core::ptr::null();
    let mut clen: usize = 0;
    let cst = unsafe { pg_diff_array_in(esel, cstr.as_ptr(), -1, &mut cimg, &mut clen) };
    let ce = cerr();

    let meta = meta_for(esel);
    let mut proc = in_proc(esel);
    let hard = array_in(mcx, text, &meta, &mut proc, -1, None);

    match (&hard, cst) {
        (Ok(Some(img)), 0) => {
            let cbytes = unsafe { core::slice::from_raw_parts(cimg, clen) };
            assert!(
                &img[..] == cbytes,
                "array_in DIVERGENCE (value) esel={esel} input={text:?}: \
                 C img={} Rust img={}",
                hex(cbytes),
                hex(img),
            );
        }
        (Err(e), c) if c != 0 => {
            let rc = class_of(e);
            // KNOWN-DIV-3 FIXED (fuzz/DIVERGENCE-NOTES-arrayfuncs.md):
            // read_dimension_int now honors the strtol no-consume contract
            // for a bare sign; strict class parity.
            assert!(
                rc == c,
                "array_in DIVERGENCE (errcode) esel={esel} input={text:?}: \
                 C class {c} vs Rust class {rc} (sqlstate {:?})",
                e.sqlstate(),
            );
        }
        _ => panic!(
            "array_in DIVERGENCE (verdict) esel={esel} input={text:?}: \
             C st={cst} err={ce} vs Rust {:?}",
            hard.as_ref().map(|o| o.is_some()).map_err(|e| e.sqlstate()),
        ),
    }

    // Rust-side-only consistency plane: soft (ErrorSaveNode) vs hard. The
    // details_wanted=false shape is driven too (it takes the
    // mark_error_occurred branch instead of save()).
    let details = !s.is_empty() && s[0] & 1 == 0;
    let mut node = ErrorSaveNode::new(details);
    let mut proc2 = in_proc(esel);
    let soft = array_in(mcx, text, &meta, &mut proc2, -1, Some(&mut node));
    match (&hard, &soft) {
        (Ok(Some(h)), Ok(Some(sv))) => {
            assert!(
                h[..] == sv[..],
                "array_in soft/hard image mismatch input={text:?}"
            );
            assert!(!node.ctx.error_occurred());
        }
        (Err(e), Ok(None)) => {
            assert!(
                node.ctx.error_occurred(),
                "array_in soft path lost the error input={text:?}"
            );
            // details_wanted=false records only the flag, no PgError
            if !details {
                assert!(node.ctx.error().is_none());
            }
            if let Some(se) = node.ctx.error() {
                assert!(
                    se.sqlstate() == e.sqlstate(),
                    "array_in soft/hard sqlstate mismatch input={text:?}: \
                     hard {:?} soft {:?}",
                    e.sqlstate(),
                    se.sqlstate(),
                );
            }
        }
        (h, s2) => panic!(
            "array_in soft/hard verdict mismatch input={text:?}: \
             hard ok={:?} soft ok={:?} soft_err={}",
            h.as_ref().map(|o| o.is_some()).map_err(|e| e.sqlstate()),
            s2.as_ref().map(|o| o.is_some()).map_err(|e| e.sqlstate()),
            node.ctx.error_occurred(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 1: array_out (oid 751)
// ---------------------------------------------------------------------------

fn array_out_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let img = build_image(mcx, esel, r);
    let mut cstr: *const u8 = core::ptr::null();
    let cst = unsafe { pg_diff_array_out(esel, img.as_ptr(), img.len(), &mut cstr) };
    let mut proc = out_proc(esel);
    let rres = array_out(mcx, &img, &meta_for(esel), &mut proc);
    match (rres, cst) {
        (Ok(rs), 0) => {
            let cs = unsafe { core::ffi::CStr::from_ptr(cstr as *const core::ffi::c_char) };
            let cb = cs.to_bytes();
            assert!(
                &rs[..rs.len() - 1] == cb,
                "array_out DIVERGENCE esel={esel} payload={}: C={:?} Rust={:?}",
                hex(payload),
                String::from_utf8_lossy(cb),
                String::from_utf8_lossy(&rs[..rs.len() - 1]),
            );
        }
        (rr, c) => panic!(
            "array_out DIVERGENCE (verdict) esel={esel} payload={}: C st={c} \
             err={} Rust ok={}",
            hex(payload),
            cerr(),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 2: array_get_element
// ---------------------------------------------------------------------------

fn get_element_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let img = build_image(mcx, esel, r);
    let mode = read_mode(esel, r);
    let arraytyplen = if mode.fixed { fixed_typlen(esel, r) } else { -1 };
    let nsub = (r.u8() % 7) as usize; // 0..=6
    let mut indx = [0i32; MAXDIM];
    for v in indx.iter_mut().take(nsub) {
        *v = subscript(mode, r);
    }
    // fixed-length containers read arraytyplen bytes from the datum start:
    // the image must be at least that long (C reads the raw blob).
    if mode.fixed && img.len() < arraytyplen as usize {
        return;
    }
    let mut cval: u64 = 0;
    let mut cptr: *const u8 = core::ptr::null();
    let mut csize: usize = 0;
    let mut cnull: i32 = 0;
    let cst = unsafe {
        pg_diff_array_get_element(
            esel,
            img.as_ptr(),
            img.len(),
            nsub as i32,
            indx.as_ptr(),
            arraytyplen,
            &mut cval,
            &mut cptr,
            &mut csize,
            &mut cnull,
        )
    };
    assert!(cst == 0, "array_get_element oracle unexpectedly errored ({cst})");
    let meta = meta_for(esel);
    let (rd, risnull) = array_get_element(
        &img,
        &indx[..nsub],
        arraytyplen,
        meta.typlen,
        meta.typbyval,
        meta.typalign,
    );
    assert!(
        risnull == (cnull != 0),
        "array_get_element DIVERGENCE (isnull) esel={esel} payload={}: C={} Rust={}",
        hex(payload),
        cnull,
        risnull,
    );
    if !risnull {
        // KNOWN-DIV-2 FIXED: fetch_att sign-extends like C's Int32GetDatum,
        // so full Datum-word parity is asserted for every width except the
        // 1-byte platform carve (module header).
        assert_elem_eq(
            esel,
            rd,
            cval,
            cptr,
            csize,
            &format!("array_get_element payload={}", hex(payload)),
        );
    }
}

// ---------------------------------------------------------------------------
// Arm 3: array_get_slice
// ---------------------------------------------------------------------------

fn slice_bounds(
    mode: Mode,
    r: &mut Rdr<'_>,
) -> (usize, [i32; MAXDIM], [i32; MAXDIM], [bool; MAXDIM], [bool; MAXDIM]) {
    slice_bounds_for(mode, r, None)
}

/// With `img`, a coin flip draws INTERIOR bounds (a strict sub-rectangle of
/// the array) instead of free bounds: only a strict sub-rectangle leaves a
/// non-zero step distance, which is what reaches the multi-dim
/// seek/copy/bitmap-copy branches of slice_size / extract_slice /
/// insert_slice. Free bounds stay in the mix for the truncate/empty paths.
fn slice_bounds_for(
    mode: Mode,
    r: &mut Rdr<'_>,
    img: Option<&[u8]>,
) -> (usize, [i32; MAXDIM], [i32; MAXDIM], [bool; MAXDIM], [bool; MAXDIM]) {
    let nsub = (r.u8() % 7) as usize;
    let mut upper = [0i32; MAXDIM];
    let mut lower = [0i32; MAXDIM];
    let interior = img.is_some() && !mode.wide && r.u8() & 1 == 1;
    if interior {
        let (ndim, dims, lbs) = arrayfuncs::read_dims_lbounds(img.unwrap());
        for i in 0..nsub {
            if i < ndim as usize && dims[i] > 0 {
                let span = dims[i];
                let a = (r.u8() as i32).rem_euclid(span);
                let b = (r.u8() as i32).rem_euclid(span);
                lower[i] = lbs[i] + a.min(b);
                upper[i] = lbs[i] + a.max(b);
            } else {
                lower[i] = subscript(mode, r);
                upper[i] = subscript(mode, r);
            }
        }
    } else {
        for i in 0..nsub {
            lower[i] = subscript(mode, r);
            upper[i] = subscript(mode, r);
        }
    }
    let bits = r.u8();
    let mut upb = [false; MAXDIM];
    let mut lob = [false; MAXDIM];
    for i in 0..nsub {
        lob[i] = (bits >> i) & 1 == 1;
        upb[i] = (bits >> (i + 1)) & 1 == 1 || i % 2 == 0;
    }
    (nsub, upper, lower, upb, lob)
}

fn get_slice_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let img = build_image(mcx, esel, r);
    let mode = read_mode(esel, r);
    let arraytyplen = if mode.fixed { fixed_typlen(esel, r) } else { -1 };
    let (nsub, upper, lower, upb, lob) = slice_bounds_for(mode, r, Some(&img));
    let mut cup = upper;
    let mut clo = lower;
    let cupb: [u8; MAXDIM] = core::array::from_fn(|i| upb[i] as u8);
    let clob: [u8; MAXDIM] = core::array::from_fn(|i| lob[i] as u8);
    let mut cimg: *const u8 = core::ptr::null();
    let mut clen: usize = 0;
    let cst = unsafe {
        pg_diff_array_get_slice(
            esel,
            img.as_ptr(),
            img.len(),
            nsub as i32,
            cup.as_mut_ptr(),
            clo.as_mut_ptr(),
            cupb.as_ptr(),
            clob.as_ptr(),
            arraytyplen,
            &mut cimg,
            &mut clen,
        )
    };
    let meta = meta_for(esel);
    let mut rup = upper;
    let mut rlo = lower;
    let rres = array_get_slice(
        mcx,
        &img,
        nsub as i32,
        &mut rup,
        &mut rlo,
        &upb,
        &lob,
        arraytyplen,
        meta.typlen,
        meta.typalign,
    );
    match (rres, cst) {
        (Ok(rimg), 0) => {
            let cb = unsafe { core::slice::from_raw_parts(cimg, clen) };
            assert!(
                &rimg[..] == cb,
                "array_get_slice DIVERGENCE (image) esel={esel} payload={}: \
                 C={} Rust={}",
                hex(payload),
                hex(cb),
                hex(&rimg),
            );
            assert!(
                rup[..nsub] == cup[..nsub] && rlo[..nsub] == clo[..nsub],
                "array_get_slice DIVERGENCE (scribbled bounds) esel={esel} payload={}",
                hex(payload),
            );
        }
        (Err(e), c) if c != 0 => {
            assert!(
                class_of(&e) == c,
                "array_get_slice DIVERGENCE (errcode) esel={esel} payload={}: \
                 C class {c} Rust {:?}",
                hex(payload),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "array_get_slice DIVERGENCE (verdict) esel={esel} payload={}: \
             C st={c} Rust ok={}",
            hex(payload),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 4: array_set_element
// ---------------------------------------------------------------------------

fn set_element_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let img = build_image(mcx, esel, r);
    let mode = read_mode(esel, r);
    let arraytyplen = if mode.fixed { fixed_typlen(esel, r) } else { -1 };
    let nsub = (r.u8() % 7) as usize;
    let mut indx = [0i32; MAXDIM];
    for v in indx.iter_mut().take(nsub) {
        *v = subscript(mode, r);
    }
    let isnull = r.u8() & 1 == 1;
    let w = elem_width(esel, r);
    let elem_bytes = read_elem_bytes(esel, w, r);
    let rdatum = make_elem(mcx, esel, &elem_bytes);
    if mode.fixed && img.len() < arraytyplen as usize {
        return;
    }
    if mode.wide && !wide_alloc_safe(&img, nsub, &indx) {
        return; // merely-large extension: see WIDE-BOUNDS DOMAIN BOUND
    }

    let mut cimg: *const u8 = core::ptr::null();
    let mut clen: usize = 0;
    let cst = unsafe {
        pg_diff_array_set_element(
            esel,
            img.as_ptr(),
            img.len(),
            nsub as i32,
            indx.as_ptr(),
            elem_bytes.as_ptr(),
            elem_bytes.len(),
            isnull as i32,
            arraytyplen,
            &mut cimg,
            &mut clen,
        )
    };
    let meta = meta_for(esel);
    let rres = array_set_element(
        mcx,
        &img,
        &indx[..nsub],
        if isnull { Datum::null() } else { rdatum },
        isnull,
        arraytyplen,
        meta.typlen,
        meta.typbyval,
        meta.typalign,
    );
    match (rres, cst) {
        (Ok(rimg), 0) => {
            let cb = unsafe { core::slice::from_raw_parts(cimg, clen) };
            assert!(
                &rimg[..] == cb,
                "array_set_element DIVERGENCE (image) esel={esel} payload={}: \
                 C={} Rust={}",
                hex(payload),
                hex(cb),
                hex(&rimg),
            );
        }
        (Err(e), c) if c != 0 => {
            assert!(
                class_of(&e) == c,
                "array_set_element DIVERGENCE (errcode) esel={esel} payload={}: \
                 C class {c} Rust {:?}",
                hex(payload),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "array_set_element DIVERGENCE (verdict) esel={esel} payload={}: \
             C st={c} err={} Rust ok={}",
            hex(payload),
            cerr(),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 5: array_set_slice
// ---------------------------------------------------------------------------

fn set_slice_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let img = build_image(mcx, esel, r);
    let src = build_image(mcx, esel, r);
    let mode = read_mode(esel, r);
    let arraytyplen = if mode.fixed { fixed_typlen(esel, r) } else { -1 };
    let (nsub0, upper, lower, upb, lob) = slice_bounds_for(mode, r, Some(&img));
    // DOMAIN CARVE: nSubscripts >= 1. C's ndim==1 arm carries
    // Assert(nSubscripts == 1) — a debug-only caller contract (SQL
    // subscripting always supplies >= 1 subscript); the shipped Rust keeps
    // that contract as an unconditional assert!, so nsub==0 panics Rust
    // while NDEBUG C proceeds. Recorded in DIVERGENCE-NOTES-arrayfuncs.md.
    let nsub = nsub0.max(1);
    if mode.fixed && img.len() < arraytyplen as usize {
        return;
    }
    if mode.wide && !wide_alloc_safe(&img, nsub, &lower) {
        return; // merely-large extension: see WIDE-BOUNDS DOMAIN BOUND
    }
    if mode.wide && !wide_alloc_safe(&img, nsub, &upper) {
        return;
    }
    let mut cup = upper;
    let mut clo = lower;
    let cupb: [u8; MAXDIM] = core::array::from_fn(|i| upb[i] as u8);
    let clob: [u8; MAXDIM] = core::array::from_fn(|i| lob[i] as u8);
    let mut cimg: *const u8 = core::ptr::null();
    let mut clen: usize = 0;
    let cst = unsafe {
        pg_diff_array_set_slice(
            esel,
            img.as_ptr(),
            img.len(),
            nsub as i32,
            cup.as_mut_ptr(),
            clo.as_mut_ptr(),
            cupb.as_ptr(),
            clob.as_ptr(),
            src.as_ptr(),
            src.len(),
            arraytyplen,
            &mut cimg,
            &mut clen,
        )
    };
    let meta = meta_for(esel);
    let mut rup = upper;
    let mut rlo = lower;
    let rres = array_set_slice(
        mcx,
        &img,
        nsub as i32,
        &mut rup,
        &mut rlo,
        &upb,
        &lob,
        &src,
        arraytyplen,
        meta.typlen,
        meta.typbyval,
        meta.typalign,
    );
    match (rres, cst) {
        (Ok(rimg), 0) => {
            let cb = unsafe { core::slice::from_raw_parts(cimg, clen) };
            assert!(
                &rimg[..] == cb,
                "array_set_slice DIVERGENCE (image) esel={esel} payload={}: \
                 C={} Rust={}",
                hex(payload),
                hex(cb),
                hex(&rimg),
            );
        }
        (Err(e), c) if c != 0 => {
            assert!(
                class_of(&e) == c,
                "array_set_slice DIVERGENCE (errcode) esel={esel} payload={}: \
                 C class {c} Rust {:?}",
                hex(payload),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "array_set_slice DIVERGENCE (verdict) esel={esel} payload={}: \
             C st={c} err={} Rust ok={}",
            hex(payload),
            cerr(),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 6: deconstruct_array
// ---------------------------------------------------------------------------

fn deconstruct_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let img = build_image(mcx, esel, r);
    let mode = read_mode(esel, r);
    let allow_nulls = r.u8() & 1 == 1;
    // BUILTIN-TABLE MODE: route both sides through the *_builtin entry so the
    // hardcoded (elmlen, elmbyval, elmalign) table is dual-executed
    // (construct.rs builtin_meta vs the pasted C switch).
    // KNOWN-DIV-4 FIXED (RATIFIED Michael 2026-07-31): deconstruct's table
    // is now C-exactly the 8-row subset, so the 5 construct-only types error
    // class 9 on BOTH sides — strict parity in the main match below.
    let builtin = mode.alt && builtin_route_ok(METATAB[esel as usize].0, true);
    let mut cvals: *const u64 = core::ptr::null();
    let mut cnulls: *const u8 = core::ptr::null();
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_deconstruct_array(
            esel,
            img.as_ptr(),
            img.len(),
            allow_nulls as i32,
            builtin as i32,
            &mut cvals,
            &mut cnulls,
            &mut cn,
        )
    };
    let meta = meta_for(esel);
    let rres = if builtin {
        arrayfuncs::deconstruct_array_builtin(mcx, &img, meta.element_type, allow_nulls)
    } else {
        deconstruct_array(
            mcx,
            &img,
            meta.typlen,
            meta.typbyval,
            meta.typalign,
            allow_nulls,
        )
    };
    match (rres, cst) {
        (Ok((relems, rnulls)), 0) => {
            assert!(
                relems.len() == cn as usize,
                "deconstruct_array DIVERGENCE (count) esel={esel} payload={}: \
                 C={cn} Rust={}",
                hex(payload),
                relems.len(),
            );
            let cv = unsafe { core::slice::from_raw_parts(cvals, cn as usize) };
            let cnl = unsafe { core::slice::from_raw_parts(cnulls, cn as usize) };
            for i in 0..cn as usize {
                assert!(
                    rnulls[i] == (cnl[i] != 0),
                    "deconstruct_array DIVERGENCE (null[{i}]) esel={esel} payload={}",
                    hex(payload),
                );
                if rnulls[i] {
                    continue;
                }
                // KNOWN-DIV-2 FIXED: full Datum-word parity except the 1-byte
                // platform carve (module header). byref widths compare bytes.
                let (_, typlen, typbyval, _) = METATAB[esel as usize];
                let (cptr, csize) = if typbyval {
                    (core::ptr::null(), 0usize)
                } else {
                    let p = cv[i] as usize as *const u8;
                    let n = match typlen {
                        -1 => arrayfuncs::foundation::varsize_any(p),
                        -2 => unsafe {
                            core::ffi::CStr::from_ptr(p as *const core::ffi::c_char)
                        }
                        .to_bytes()
                        .len()
                            + 1,
                        w => w as usize,
                    };
                    (p, n)
                };
                assert_elem_eq(
                    esel,
                    relems[i],
                    cv[i],
                    cptr,
                    csize,
                    &format!("deconstruct_array[{i}] builtin={builtin} payload={}", hex(payload)),
                );
            }
        }
        (Err(e), c) if c != 0 => {
            assert!(
                class_of(&e) == c,
                "deconstruct_array DIVERGENCE (errcode) esel={esel} payload={}: \
                 C class {c} Rust {:?}",
                hex(payload),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "deconstruct_array DIVERGENCE (verdict) esel={esel} payload={}: \
             C st={c} Rust ok={}",
            hex(payload),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 7: construct_md_array (+ construct_empty_array via nitems==0)
// ---------------------------------------------------------------------------

fn construct_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let mode = read_mode(esel, r);
    // 1-D WRAPPER MODE: bit 2 = construct_array (nulls unsupported,
    // dims[0] = nelems, lbs[0] = 1); bits 2+3 = construct_array_builtin
    // (C looks the meta up in its own 12-row table).
    let wrapper: i32 = match (mode.alt, mode.alt2) {
        // builtin route only for metas inside C's construct table (see
        // builtin_route_ok / KNOWN-DIV-5)
        (true, true) if builtin_route_ok(METATAB[esel as usize].0, false) => 2,
        (true, _) => 1,
        _ => 0,
    };
    let raw = r.u8();
    let ndims: i32 = if raw >= 250 {
        -(((raw % 7) as i32) + 1)
    } else {
        (raw % 8) as i32 // 0..=7 (7 probes the >MAXDIM arm)
    };
    let mut dims = [0i32; MAXDIM];
    let mut lbs = [1i32; MAXDIM];
    let nd_use = ndims.clamp(0, MAXDIM as i32) as usize;
    for i in 0..nd_use {
        dims[i] = (r.u8() % 3) as i32;
        lbs[i] = r.i32le(); // FULL-RANGE: drives ArrayCheckBounds overflow
    }
    // WIDE mode on this arm: full-range dims too, driving ArrayGetNItems'
    // int64-widened product overflow (54000) before any allocation.
    if mode.wide {
        for i in 0..nd_use {
            dims[i] = r.i32le();
        }
    }
    // Saturating product: wide dims overflow i64 too, and the count only
    // decides how many elements to materialize (both sides reject oversized
    // dims in ArrayGetNItems before touching the element array).
    let nitems: i64 = if ndims <= 0 || ndims as usize > MAXDIM {
        0
    } else {
        dims[..nd_use]
            .iter()
            .fold(1i64, |acc, &d| acc.saturating_mul(d as i64))
    };
    // Element-materialization contract: C's construct_md_array reads ALL
    // nelems datums in its size pass, and only dims that ArrayGetNItemsSafe
    // itself rejects (any dim < 0, or product > MaxArraySize = 134217727)
    // are safe to leave unmaterialized. A product in 4097..=MaxArraySize is
    // ACCEPTED by both sides, so leaving it hollow made C read
    // unmaterialized memory (replay SEGV 2026-07-31) — clamp such dims down
    // instead so the product stays materializable.
    const MAX_ARRAY_SIZE: i64 = 0x3fffffff / 8;
    let rejected = dims[..nd_use].iter().any(|&d| d < 0) || nitems > MAX_ARRAY_SIZE;
    let nitems = if rejected {
        0
    } else if nitems > 4096 {
        // Fold accepted-but-large dims back into the small domain (3^7 =
        // 2187 <= 4096) and recompute, so dims-product == materialized count.
        for d in dims[..nd_use].iter_mut() {
            *d = d.rem_euclid(3);
        }
        dims[..nd_use]
            .iter()
            .fold(1i64, |acc, &d| acc.saturating_mul(d as i64)) as usize
    } else {
        nitems as usize
    };
    let nullbits = r.u64le();
    let mut elems: std::vec::Vec<Datum> = std::vec::Vec::new();
    let mut nulls: std::vec::Vec<bool> = std::vec::Vec::new();
    let mut c_elem_data: std::vec::Vec<u8> = std::vec::Vec::new();
    let mut c_elem_lens: std::vec::Vec<i32> = std::vec::Vec::new();
    let mut c_nulls: std::vec::Vec<u8> = std::vec::Vec::new();
    for i in 0..nitems {
        // construct_array (wrapper mode) does not support nulls: C passes a
        // NULL nulls[] there, so keep every element non-null in that mode.
        let isnull = wrapper == 0 && (nullbits >> (i % 64)) & 1 == 1;
        nulls.push(isnull);
        c_nulls.push(isnull as u8);
        if isnull {
            elems.push(Datum::null());
            c_elem_lens.push(0);
            continue;
        }
        let w = elem_width(esel, r);
        let b = read_elem_bytes(esel, w, r);
        elems.push(make_elem(mcx, esel, &b));
        c_elem_lens.push(b.len() as i32);
        c_elem_data.extend_from_slice(&b);
    }

    let mut cimg: *const u8 = core::ptr::null();
    let mut clen: usize = 0;
    let cst = unsafe {
        pg_diff_construct_md_array(
            esel,
            c_elem_data.as_ptr(),
            c_elem_lens.as_ptr(),
            c_nulls.as_ptr(),
            nitems as i32,
            ndims,
            dims.as_ptr(),
            lbs.as_ptr(),
            wrapper,
            &mut cimg,
            &mut clen,
        )
    };
    let (elmtype, elmlen, elmbyval, elmalign) = METATAB[esel as usize];
    let rres = if wrapper == 2 {
        // The crate has NO construct_array_builtin; its equivalent is
        // construct_array over builtin_meta(elmtype). Composing them here
        // dual-executes builtin_meta against C's construct table, including
        // the unlisted-oid XX000 arm (KNOWN-DIV-5 FIXED).
        builtin_meta(elmtype)
            .and_then(|(bl, bb, ba)| arrayfuncs::construct_array(mcx, &elems, elmtype, bl, bb, ba))
    } else if wrapper == 1 {
        arrayfuncs::construct_array(mcx, &elems, elmtype, elmlen, elmbyval, elmalign)
    } else {
        construct_md_array(
            mcx,
            &elems,
            Some(&nulls),
            ndims,
            &dims,
            &lbs,
            elmtype,
            elmlen,
            elmbyval,
            elmalign,
        )
    };
    match (rres, cst) {
        (Ok(rimg), 0) => {
            let cb = unsafe { core::slice::from_raw_parts(cimg, clen) };
            assert!(
                &rimg[..] == cb,
                "construct_md_array DIVERGENCE (image) esel={esel} wrapper={wrapper} \
                 payload={}: C={} Rust={}",
                hex(payload),
                hex(cb),
                hex(&rimg),
            );
        }
        (Err(e), c) if c != 0 => {
            // KNOWN-DIV-1 FIXED (fuzz/DIVERGENCE-NOTES-arrayfuncs.md):
            // ndims<0 now carries 22023 on both sides; strict class parity.
            assert!(
                class_of(&e) == c,
                "construct_md_array DIVERGENCE (errcode) esel={esel} ndims={ndims} \
                 payload={}: C class {c} Rust class {} (sqlstate {:?})",
                hex(payload),
                class_of(&e),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "construct_md_array DIVERGENCE (verdict) esel={esel} ndims={ndims} \
             payload={}: C st={c} Rust ok={}",
            hex(payload),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 8: array_contains_nulls
// ---------------------------------------------------------------------------

fn contains_nulls_diff(mcx: Mcx<'_>, esel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    let built = build_image(mcx, esel, r);
    // A construct_md_array image only carries a null bitmap when it actually
    // has a null, so the "bitmap present but every bit set" shape — the one
    // that exercises the bit-scan loop all the way to its `false` return — is
    // only reachable by OVERWRITING the nulls of an array that had them.
    // Chain array_set_element for that, then feed the result to both sides.
    let overwrite = r.u8() & 1 == 1;
    let img = if overwrite && arrayfuncs::arr_hasnull(&built) {
        let (ndim, dims, lbs) = arrayfuncs::read_dims_lbounds(&built);
        let meta0 = meta_for(esel);
        let mut cur: PgVec<u8> = built;
        if ndim == 1 && dims[0] > 0 {
            let w = elem_width(esel, r);
            let b = read_elem_bytes(esel, w, r);
            let d = make_elem(mcx, esel, &b);
            for k in 0..dims[0] {
                match array_set_element(
                    mcx,
                    &cur,
                    &[lbs[0] + k],
                    d,
                    false,
                    -1,
                    meta0.typlen,
                    meta0.typbyval,
                    meta0.typalign,
                ) {
                    Ok(next) => cur = next,
                    Err(_) => break,
                }
            }
        }
        cur
    } else {
        built
    };
    let cst = unsafe { pg_diff_array_contains_nulls(img.as_ptr(), img.len()) };
    let rv = array_contains_nulls(&img);
    let cv = match cst {
        -1 => true,
        -2 => false,
        c => panic!("array_contains_nulls oracle errored ({c})"),
    };
    assert!(
        rv == cv,
        "array_contains_nulls DIVERGENCE esel={esel} payload={}: C={cv} Rust={rv}",
        hex(payload),
    );
}

// ---------------------------------------------------------------------------
// Arm 9: width_bucket_array (oid 3218), driven through the REAL wrapper
// ops::fc_width_bucket_array (fc plane) vs the pasted C dispatcher.
// ---------------------------------------------------------------------------

fn width_bucket_diff(mcx: Mcx<'_>, wsel: i32, r: &mut Rdr<'_>, payload: &[u8]) {
    // Thresholds image: element type per wsel. build_image elemsel: 0=int4,
    // 1=text, 2=float8 — remap (wsel 1 = float8 -> build 2; wsel 2 = text).
    // thresholds element type per wsel: int4 (fixed path) / float8
    // (dedicated path) / text (variable path).
    let build_sel = match wsel {
        0 => 0,
        1 => 6, // float8 row of METATAB
        _ => 1,
    };
    let img = build_image(mcx, build_sel, r);

    let (operand_bits, operand_payload): (u64, std::vec::Vec<u8>) = match wsel {
        0 => (Datum::from_i32(r.i32le()).as_usize() as u64, std::vec::Vec::new()),
        1 => (r.u64le(), std::vec::Vec::new()),
        _ => {
            let n = (r.u8() % 9) as usize;
            (0, r.bytes(n))
        }
    };

    let mut cres: i32 = 0;
    let cst = unsafe {
        pg_diff_width_bucket_array(
            wsel,
            operand_bits,
            operand_payload.as_ptr(),
            operand_payload.len(),
            img.as_ptr(),
            img.len(),
            &mut cres,
        )
    };

    let operand: Datum = match wsel {
        0 | 1 => Datum::from_usize(operand_bits as usize),
        _ => build_varlena(mcx, &operand_payload),
    };
    let collation = if wsel == 2 { C_COLLATION_OID } else { 0 };
    let mut fl = FmgrInfo::new(arrayfuncs::ops::fc_width_bucket_array, 3218, 2, true, false);
    let mut fcinfo = LocalFcinfo::<2>::fresh(collation);
    // SAFETY: ctx owning mcx outlives this call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.set_arg(0, operand);
    fcinfo.set_arg(1, Datum::from_usize(img.as_ptr() as usize));
    let rres = arrayfuncs::ops::fc_width_bucket_array(Some(&mut fl), &mut fcinfo);

    match (rres, cst) {
        (Ok(d), 0) => {
            let rv = d.as_usize() as i32;
            assert!(
                rv == cres,
                "width_bucket_array DIVERGENCE (value) wsel={wsel} payload={}: \
                 C={cres} Rust={rv}",
                hex(payload),
            );
        }
        (Err(e), c) if c != 0 => {
            assert!(
                class_of(&e) == c,
                "width_bucket_array DIVERGENCE (errcode) wsel={wsel} payload={}: \
                 C class {c} Rust {:?}",
                hex(payload),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "width_bucket_array DIVERGENCE (verdict) wsel={wsel} payload={}: \
             C st={c} Rust ok={}",
            hex(payload),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Arm 10: ArrayGetIntegerTypmods
// ---------------------------------------------------------------------------

const TYPMOD_ALPHABET: &[u8] = b"0123456789+- _xXoObB\t,148";

fn typmods_diff(mcx: Mcx<'_>, r: &mut Rdr<'_>, payload: &[u8]) {
    let nelems = (r.u8() % 5) as usize;
    let shape = (r.u8() % 4) as i32;
    let mut strs: std::vec::Vec<std::vec::Vec<u8>> = std::vec::Vec::new();
    for _ in 0..nelems {
        let n = (r.u8() % 12) as usize;
        let mut s = std::vec::Vec::with_capacity(n + 1);
        for _ in 0..n {
            s.push(TYPMOD_ALPHABET[(r.u8() as usize) % TYPMOD_ALPHABET.len()]);
        }
        s.push(0);
        strs.push(s);
    }
    let ptrs: std::vec::Vec<*const u8> = strs.iter().map(|s| s.as_ptr()).collect();

    let mut cvals: *const i32 = core::ptr::null();
    let mut cn: i32 = 0;
    let cst = unsafe {
        pg_diff_array_get_integer_typmods(nelems as i32, ptrs.as_ptr(), shape, &mut cvals, &mut cn)
    };

    // Mirror the oracle driver's image construction exactly (same shapes).
    let elems: std::vec::Vec<Datum> = strs
        .iter()
        .map(|s| {
            let mut v = mcx::vec_with_capacity_in::<u8>(mcx, s.len()).expect("alloc");
            v.extend_from_slice(s);
            let d = Datum::from_usize(v.as_ptr() as usize);
            core::mem::forget(v);
            d
        })
        .collect();
    let mut nulls = std::vec::Vec::new();
    nulls.resize(nelems, false);
    let (ndims, dims, lbs): (i32, [i32; MAXDIM], [i32; MAXDIM]) =
        if shape == 2 && nelems >= 2 && nelems % 2 == 0 {
            let mut d = [0i32; MAXDIM];
            d[0] = (nelems / 2) as i32;
            d[1] = 2;
            (2, d, [1i32; MAXDIM])
        } else {
            let mut d = [0i32; MAXDIM];
            d[0] = nelems as i32;
            (if nelems > 0 { 1 } else { 0 }, d, [1i32; MAXDIM])
        };
    if shape == 3 && nelems > 0 {
        nulls[0] = true;
    }
    let elmtype = if shape == 1 { INT4OID } else { CSTRINGOID };
    let arr = construct_md_array(
        mcx, &elems, Some(&nulls), ndims, &dims, &lbs, elmtype, -2, false, b'c',
    )
    .expect("cstring image construction");

    let rres = array_get_integer_typmods(mcx, &arr);
    match (rres, cst) {
        (Ok(rv), 0) => {
            let cv = unsafe { core::slice::from_raw_parts(cvals, cn as usize) };
            assert!(
                &rv[..] == cv,
                "ArrayGetIntegerTypmods DIVERGENCE (values) payload={}: \
                 C={cv:?} Rust={:?}",
                hex(payload),
                &rv[..],
            );
        }
        (Err(e), c) if c != 0 => {
            assert!(
                class_of(&e) == c,
                "ArrayGetIntegerTypmods DIVERGENCE (errcode) shape={shape} payload={}: \
                 C class {c} Rust class {} (sqlstate {:?})",
                hex(payload),
                class_of(&e),
                e.sqlstate(),
            );
        }
        (rr, c) => panic!(
            "ArrayGetIntegerTypmods DIVERGENCE (verdict) shape={shape} payload={}: \
             C st={c} Rust ok={}",
            hex(payload),
            rr.is_ok(),
        ),
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed sweep: every arm executes against the C oracle every test run,
    /// with ok- and error-shaped payloads.
    #[test]
    fn arm_sweep() {
        let _serial = crate::c_oracle_serial();
        let payloads: [&[u8]; 5] = [
            &[],
            &[0xff; 128],
            &[0x01; 128],
            &[0x02, 0x02, 0x02, 0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8],
            b"\x01\x02{1,2,3}",
        ];
        for sel in 0u8..11 {
            for esel in [0u8, 1, 2] {
                for p in payloads {
                    let mut data = vec![sel, esel];
                    data.extend_from_slice(p);
                    arrayfuncs_diff(&data);
                }
            }
        }
    }

    /// array_in ok + error shapes for both element types, incl. the soft
    /// consistency plane.
    #[test]
    fn array_in_smoke() {
        let _serial = crate::c_oracle_serial();
        for esel in [0u8, 1] {
            for lit in [
                "{1,2,3}",
                "{{1,2},{3,4}}",
                "[2:4]={7,8,9}",
                "{1,NULL,3}",
                "  { 42 }  ",
                "{}",
                "{\"a b\",\"c,d\"}",
                "{1,2",   // unexpected end
                "{1,,2}", // unexpected delim
                "junk",
                "[1:2147483647]={1}",
                "{999999999999999}", // int4 out of range
            ] {
                let mut data = vec![0u8, esel];
                data.extend_from_slice(lit.as_bytes());
                arrayfuncs_diff(&data);
            }
        }
    }

    /// Replay every checked-in seed (catches shim/link errors before the
    /// nightly fuzz campaign). Corpus is COMMITTED.
    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/arrayfuncs_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/arrayfuncs_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                arrayfuncs_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 40, "expected >=40 seeds, found {n}");
    }
}
