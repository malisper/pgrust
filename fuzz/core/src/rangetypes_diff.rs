//! rangetypes_diff: differential fuzz driver — shipped Rust `adt_rangetypes`
//! vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_rangetypes_io.c). Crate under test:
//! crates/backend/utils/adt/rangetypes.
//!
//! ===================== TYPCACHE MOCK (sanctioned) =====================
//! The crate's only typcache dependency is the RangeInfo / RangeIOData it
//! memoizes in flinfo.fn_extra. Both sides pin the SAME three concrete
//! instantiations (the campaign carve leaves typcache lookup internals out
//! of scope):
//!   type tag 0: int4range (3904) elem int4    (4,  byval, 'i', 'p')
//!               cmp=btint4cmp  canonical=int4range_canonical
//!   type tag 1: int8range (3926) elem int8    (8,  byval, 'd', 'p')
//!               cmp=btint8cmp  canonical=int8range_canonical
//!   type tag 2: numrange  (3906) elem numeric (-1, byref, 'i', 'm')
//!               cmp=numeric_cmp canonical=INVALID (continuous type)
//! daterange (3912, elem date) rides ONLY the canonical + subdiff arms.
//! The Rust side pre-seeds flinfo.fn_extra with a hand-built RangeInfo /
//! RangeIOData mirroring the C oracle's static TypeCacheEntry values, so
//! `RangeInfo::lookup` (the typcache seam) never fires — the identical
//! construction the proofs/typcache-inst Kani probe uses.
//!
//! numrange BOUND MINTING: for image-input arms, numeric bound images are
//! parsed ONCE through the shipped fc_numeric_in and the SAME bytes feed
//! both sides' range images (numeric parse parity is the adt/numeric
//! lane's surface). The numrange TEXT-IO arm still diffs range_in
//! end-to-end, where each side runs its own numeric_in — so numeric parse
//! parity on the range path is witnessed there too.
//!
//! Comparison planes: value bytes/bits (serialized range images, output
//! text, wire bytes, bool/i32/u32/u64/f64-bit results), error verdict, and
//! errcode/sqlstate CLASS (err_class below = the oracle's table). Message
//! text out of scope.
//!
//! Input layout: [sel][typ][payload]; sel % 11 picks the arm, typ % 3 the
//! instantiation (arms 9/10 repurpose typ as their own selector):
//!   0 text io:    range_in(payload-as-literal) image + errclass;
//!                 on Ok, range_out roundtrip text          (3834/3835/3833)
//!   1 binary io:  range_recv(payload-as-wire) image + errclass;
//!                 on Ok, range_send roundtrip wire bytes   (3836/3837)
//!   2 ctor2:      range_constructor2 over bound datums + null bits (3840..)
//!   3 ctor3:      range_constructor3 (+2 raw flag-string bytes)    (3841..)
//!   4 accessors:  lower/upper/isempty/lower_inc/upper_inc/lower_inf/
//!                 upper_inf over one built image           (3848-3854)
//!   5 ops:        eq ne lt le gt ge cmp overlaps contains contained_by
//!                 before after adjacent overleft overright over an image
//!                 pair — flags byte FULLY ARBITRARY incl. NULL /
//!                 CONTAIN_EMPTY bits (the proved harnesses' full-flags
//!                 domain)                                  (3855-3874)
//!   6 elem:       range_contains_elem + elem_contained_by  (3858/3860)
//!   7 setops:     union / intersect / minus / merge        (3867-3869/4057)
//!   8 hash:       hash_range + hash_range_extended(seed)   (3902/3417)
//!   9 canonical:  int4range/int8range/daterange canonical  (3914/3928/3915)
//!  10 subdiff:    int4/int8/num/date/ts/tstz subdiffs      (3922-3930)
//!
//! fc-wrapper plane: every arm drives the crate's builtins.rs fc_* wrapper
//! on a native LocalFcinfo (cash_diff pattern) — the wrapper IS the shipped
//! entry, so builtins.rs/io.rs/ops.rs/lib.rs execute under the diff.
//!
//! FLAGS DOMAIN: fully arbitrary (all 256 values) for the byval
//! instantiations; the two vestigial `RANGE_*_NULL` bits are fenced off for
//! byref (numrange) only — see `fence_flags` for why C cannot produce such an
//! image and why this is a domain restriction rather than a skipped compare.
//!
//! SKIPPED rows (see phase1-routes.tsv exceptions): range_intersect_agg
//! transfn (agg-state carve), planner support fns (engine carve),
//! range_sortsupport (unported panic stub), unnest/agg (multirange crate).
//! Known non-surface: C's fn_extra memo HIT path (both sides run fresh
//! flinfos per iteration; the memo is a pure cache).

use std::ffi::CString;

use adt_rangetypes as rt;
use adt_rangetypes::builtins as rb;
use datum::Datum;
use mcx::MemoryContext;
use stringinfo::StringInfo;
use types_core::fmgr::{AggFnArgTypes, FnExprErased};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction};

extern "C" {
    fn pg_diff_range_in(
        typ: i32,
        s: *const core::ffi::c_char,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_out(
        img: *const u8,
        out: *mut core::ffi::c_char,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_recv(
        typ: i32,
        wire: *const u8,
        wirelen: i32,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_send(img: *const u8, out: *mut u8, outlen: *mut i32, outcap: i32) -> i32;
    fn pg_diff_range_ctor(
        typ: i32,
        nargs: i32,
        v1: i64,
        v2: i64,
        n1: *const u8,
        n2: *const u8,
        null1: i32,
        null2: i32,
        flags_txt: *const u8,
        flags_len: i32,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_accessors(
        img: *const u8,
        lower_out: *mut u8,
        lower_len: *mut i32,
        lower_null: *mut i32,
        upper_out: *mut u8,
        upper_len: *mut i32,
        upper_null: *mut i32,
        bools: *mut u8,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_ops(img1: *const u8, img2: *const u8, res: *mut i32) -> i32;
    fn pg_diff_range_contains_elem(
        img: *const u8,
        v: i64,
        numptr: *const u8,
        contains: *mut i32,
        contained: *mut i32,
    ) -> i32;
    fn pg_diff_range_setop(
        which: i32,
        img1: *const u8,
        img2: *const u8,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_hash_range(img: *const u8, h: *mut u32) -> i32;
    fn pg_diff_hash_range_extended(img: *const u8, seed: u64, h: *mut u64) -> i32;
    fn pg_diff_range_canonical(
        typ: i32,
        img: *const u8,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_subdiff(
        which: i32,
        a: i64,
        b: i64,
        na: *const u8,
        nb: *const u8,
        out: *mut f64,
    ) -> i32;
}

const INT4RANGEOID: Oid = 3904;
const INT8RANGEOID: Oid = 3926;
const NUMRANGEOID: Oid = 3906;
const DATERANGEOID: Oid = 3912;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const NUMERICOID: Oid = 1700;
const DATEOID: Oid = 1082;

/// Oracle out-buffer capacity. Sized for the WORST-CASE numrange text, not for
/// typical values: `numeric_out` can emit ~147k digits (weight up to
/// NUMERIC_WEIGHT_MAX = 32767 NBASE digits = 131068 integer digits, plus dscale
/// up to 16383 fractional digits), `range_bound_escape` can double every
/// character, and a range carries two bounds — so the text form reaches
/// hundreds of KiB from a 20-byte literal like `[94771e10506,)`. A too-small
/// cap made the oracle return its -1 capacity sentinel, which the range_out arm
/// then mis-read as an error class and reported as a divergence (harness
/// defect, 2026-07-31). `vec![0u8; OUTCAP]` is alloc_zeroed, so the untouched
/// tail costs no page faults.
const OUTCAP: usize = 2 << 20;

/// The oracle's "caller buffer too small" sentinel. Never an error class and
/// never a divergence: it is a harness bug, so every arm asserts on it loudly
/// instead of comparing it.
const C_BUFCAP: i32 = -1;

/// sqlstate -> the oracle's errcode CLASS (pg_rangetypes_io.c header table).
fn err_class(e: &PgError) -> i32 {
    use types_error as te;
    if e.sqlstate == te::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
        1
    } else if e.sqlstate == te::ERRCODE_INVALID_TEXT_REPRESENTATION {
        2
    } else if e.sqlstate == te::ERRCODE_PROTOCOL_VIOLATION {
        3
    } else if e.sqlstate == te::ERRCODE_DATA_EXCEPTION {
        4
    } else if e.sqlstate == te::ERRCODE_UNDEFINED_FUNCTION {
        5
    } else if e.sqlstate == te::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE {
        6
    } else if e.sqlstate == te::ERRCODE_INVALID_BINARY_REPRESENTATION {
        7
    } else if e.sqlstate == te::ERRCODE_FEATURE_NOT_SUPPORTED {
        8
    } else if e.sqlstate == te::ERRCODE_SYNTAX_ERROR {
        9
    } else if e.sqlstate == te::ERRCODE_DATATYPE_MISMATCH {
        10
    } else if e.sqlstate == te::ERRCODE_INVALID_PARAMETER_VALUE {
        11
    } else {
        98
    }
}

// ---------------------------------------------------------------------------
// Pinned instantiations (the typcache mock, Rust side)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Pin {
    rngtypid: Oid,
    elem_typid: Oid,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    typstorage: u8,
}

const PINS: [Pin; 3] = [
    Pin {
        rngtypid: INT4RANGEOID,
        elem_typid: INT4OID,
        typlen: 4,
        typbyval: true,
        typalign: b'i',
        typstorage: b'p',
    },
    Pin {
        rngtypid: INT8RANGEOID,
        elem_typid: INT8OID,
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        typstorage: b'p',
    },
    Pin {
        rngtypid: NUMRANGEOID,
        elem_typid: NUMERICOID,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        typstorage: b'm',
    },
];

fn cmp_finfo(t: usize) -> FmgrInfo {
    match t {
        0 => FmgrInfo::new(nbt_compare::builtins::fc_btint4cmp, 351, 2, true, false),
        1 => FmgrInfo::new(nbt_compare::builtins::fc_btint8cmp, 842, 2, true, false),
        _ => FmgrInfo::new(adt_numeric::builtins::fc_numeric_cmp, 1769, 2, true, false),
    }
}

fn hash_finfo(t: usize) -> FmgrInfo {
    match t {
        0 => FmgrInfo::new(adt_int::builtins::fc_hashint4, 450, 1, true, false),
        1 => FmgrInfo::new(adt_int8::builtins::fc_hashint8, 949, 1, true, false),
        _ => FmgrInfo::new(adt_numeric::builtins::fc_hash_numeric, 432, 1, true, false),
    }
}

fn hash_ext_finfo(t: usize) -> FmgrInfo {
    match t {
        0 => FmgrInfo::new(adt_int::builtins::fc_hashint4extended, 425, 2, true, false),
        1 => FmgrInfo::new(adt_int8::builtins::fc_hashint8extended, 442, 2, true, false),
        _ => FmgrInfo::new(adt_numeric::builtins::fc_hash_numeric_extended, 780, 2, true, false),
    }
}

const F_CANONICAL: [Oid; 3] = [3914, 3928, 0 /* numrange: continuous */];

fn range_info(t: usize) -> rt::RangeInfo {
    let p = PINS[t];
    rt::RangeInfo {
        pin: None,
        rngtypid: p.rngtypid,
        collation: 0,
        elem_typid: p.elem_typid,
        elem: rt::ElemInfo {
            typlen: p.typlen,
            typbyval: p.typbyval,
            typalign: p.typalign,
            typstorage: p.typstorage,
        },
        cmp: cmp_finfo(t),
        canonical_oid: F_CANONICAL[t],
        elem_hash: Some(hash_finfo(t)),
        elem_hash_extended: Some(hash_ext_finfo(t)),
        own_typlen: -1,
        own_typbyval: false,
        own_typalign: b'd',
    }
}

/// flinfo pre-seeded with the RangeInfo memo (cached_range_info hits; the
/// typcache seam never fires) + the constructor rettype carrier.
fn ops_flinfo(t: usize) -> FmgrInfo {
    let mut fl = FmgrInfo::new(rb::fc_range_eq, 0, 2, true, false);
    fl.set_fn_extra(range_info(t));
    let carrier: &'static AggFnArgTypes =
        Box::leak(Box::new(AggFnArgTypes { rettype: PINS[t].rngtypid, argtypes: &[] }));
    // SAFETY: leaked 'static carrier outlives every read.
    fl.fn_expr = Some(unsafe { FnExprErased::from_node_ref(carrier) });
    fl
}

fn io_finfo(t: usize, sel: lsyscache::IOFuncSelector) -> FmgrInfo {
    use lsyscache::IOFuncSelector as S;
    let (f, oid): (PGFunction, Oid) = match (t, sel) {
        (0, S::IOFunc_input) => (adt_int::builtins::fc_int4in, 42),
        (0, S::IOFunc_output) => (adt_int::builtins::fc_int4out, 43),
        (0, S::IOFunc_receive) => (adt_int::builtins::fc_int4recv, 2406),
        (0, S::IOFunc_send) => (adt_int::builtins::fc_int4send, 2407),
        (1, S::IOFunc_input) => (adt_int8::builtins::fc_int8in, 460),
        (1, S::IOFunc_output) => (adt_int8::builtins::fc_int8out, 461),
        (1, S::IOFunc_receive) => (adt_int8::builtins::fc_int8recv, 2408),
        (1, S::IOFunc_send) => (adt_int8::builtins::fc_int8send, 2409),
        (_, S::IOFunc_input) => (adt_numeric::builtins::fc_numeric_in, 1701),
        (_, S::IOFunc_output) => (adt_numeric::builtins::fc_numeric_out, 1702),
        (_, S::IOFunc_receive) => (adt_numeric::builtins::fc_numeric_recv, 2460),
        (_, S::IOFunc_send) => (adt_numeric::builtins::fc_numeric_send, 2461),
    };
    let nargs = match sel {
        S::IOFunc_input | S::IOFunc_receive => 3,
        _ => 1,
    };
    FmgrInfo::new(f, oid, nargs, true, false)
}

/// flinfo for the io wrappers: fn_extra pre-seeded with RangeIOData.
fn io_flinfo(t: usize, sel: lsyscache::IOFuncSelector) -> FmgrInfo {
    let mut fl = FmgrInfo::new(rb::fc_range_in, 0, 3, true, false);
    fl.set_fn_extra(rt::io::RangeIOData {
        ri: range_info(t),
        typioproc: io_finfo(t, sel),
        typioparam: PINS[t].elem_typid,
    });
    fl
}

// ---------------------------------------------------------------------------
// fc-call plumbing (cash_diff pattern, plus SQL-NULL visibility)
// ---------------------------------------------------------------------------

struct FcOut {
    result: PgResult<Datum>,
    isnull: bool,
}

fn fc_call<const N: usize>(
    f: PGFunction,
    flinfo: Option<&mut FmgrInfo>,
    mcx: mcx::Mcx<'_>,
    args: [Option<Datum>; N],
) -> FcOut {
    let mut fcinfo = LocalFcinfo::<N>::fresh(0);
    // SAFETY: the arming context outlives this single call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    for (i, a) in args.into_iter().enumerate() {
        match a {
            Some(d) => fcinfo.set_arg(i, d),
            None => fcinfo.set_arg_null(i),
        }
    }
    let result = f(flinfo, &mut fcinfo);
    FcOut { result, isnull: fcinfo.isnull }
}

/// Read a flat varlena image out of a result Datum (live in the armed mcx).
fn datum_varlena_bytes<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: fc varlena results are live flat images read before mcx drop.
    unsafe {
        let n = types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, n)
    }
}

fn datum_cstring_bytes<'a>(d: Datum) -> &'a [u8] {
    // SAFETY: fc cstring results are live NUL-terminated in the armed mcx.
    unsafe { std::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) }.to_bytes()
}

// ---------------------------------------------------------------------------
// payload decoding + image building
// ---------------------------------------------------------------------------

struct Rd<'a>(&'a [u8], usize);

impl<'a> Rd<'a> {
    fn u8(&mut self) -> u8 {
        let v = self.0.get(self.1).copied().unwrap_or(0);
        self.1 += 1;
        v
    }
    fn i32(&mut self) -> i32 {
        let mut b = [0u8; 4];
        for x in &mut b {
            *x = self.u8();
        }
        i32::from_le_bytes(b)
    }
    fn i64(&mut self) -> i64 {
        let mut b = [0u8; 8];
        for x in &mut b {
            *x = self.u8();
        }
        i64::from_le_bytes(b)
    }
    fn bytes(&mut self, n: usize) -> &'a [u8] {
        let s = self.1.min(self.0.len());
        let e = (self.1 + n).min(self.0.len());
        self.1 += n;
        &self.0[s..e]
    }
}

/// Mint a numeric bound image from payload bytes via the SHIPPED numeric_in
/// (both sides then consume identical bytes; module header). None = literal
/// didn't parse: the iteration is skipped.
fn mint_numeric(mcx: mcx::Mcx<'_>, lit: &[u8]) -> Option<Vec<u8>> {
    if lit.is_empty() || lit.contains(&0) {
        return None;
    }
    let cs = CString::new(lit).ok()?;
    let out = fc_call(
        adt_numeric::builtins::fc_numeric_in,
        None,
        mcx,
        [
            Some(Datum::from_usize(cs.as_ptr() as usize)),
            Some(Datum::from_u32(0)),
            Some(Datum::from_i32(-1)),
        ],
    );
    let d = out.result.ok()?;
    Some(datum_varlena_bytes(d).to_vec())
}

/// One bound for one instantiation.
enum Bound {
    ByVal(i64),
    Num(Vec<u8>),
}

impl Bound {
    fn decode(t: usize, rd: &mut Rd, mcx: mcx::Mcx<'_>) -> Option<Bound> {
        match t {
            0 => Some(Bound::ByVal(rd.i32() as i64)),
            1 => Some(Bound::ByVal(rd.i64())),
            _ => {
                let n = (rd.u8() % 20) as usize + 1;
                let lit = rd.bytes(n).to_vec();
                mint_numeric(mcx, &lit).map(Bound::Num)
            }
        }
    }
    fn c_args(&self) -> (i64, *const u8) {
        match self {
            Bound::ByVal(v) => (*v, core::ptr::null()),
            Bound::Num(b) => (0, b.as_ptr()),
        }
    }
    fn rust_datum(&self) -> Datum {
        match self {
            Bound::ByVal(v) => Datum::from_i64(*v),
            Bound::Num(b) => Datum::from_usize(b.as_ptr() as usize),
        }
    }
}

/// BY-REF SUBTYPE FLAGS FENCE (rangetypes.h: `RANGE_LB_NULL 0x20 /* lower bound
/// is null (NOT USED) */`, same for `RANGE_UB_NULL 0x40`).
///
/// These two bits are vestigial: NOTHING in rangetypes.c / multirangetypes.c
/// ever sets them (`range_serialize` builds the flags byte from scratch and
/// `range_recv` masks them off), so no C code path can produce an image
/// carrying them. They are read only by RANGE_HAS_L/UBOUND, where they mean
/// "this bound is absent from the image" WITHOUT the matching `*_INF` bit that
/// would mark it infinite. `range_deserialize` therefore hands back
/// `val = (Datum) 0` with `infinite = false`, and every consumer treats that
/// as a live element value:
///   * for a BYVAL subtype that is the integer 0 — perfectly well defined, so
///     the FULL 256-value flags domain stays in scope (matching the
///     full-symbolic-flags domain of the proofs/typcache-inst harnesses);
///   * for a BYREF subtype it is a NULL element POINTER, which C's own
///     `range_lower`/`range_upper` return unflagged and C's own comparators
///     dereference. The verbatim oracle segfaults on it exactly as the shipped
///     Rust does — there is no C behavior to compare against, because C cannot
///     construct the input in the first place.
///
/// So the two bits are masked off for byref instantiations ONLY. This is a
/// domain restriction to inputs the oracle actually specifies, not a skipped
/// comparison: no flags value is dropped for int4range/int8range, and for
/// numrange the other 64 combinations are all still compared.
fn fence_flags(t: usize, flags: u8) -> u8 {
    if PINS[t].typbyval {
        flags
    } else {
        flags & !(rt::RANGE_LB_NULL | rt::RANGE_UB_NULL)
    }
}

/// Hand-build a serialized range image (on-disk spec: 4B varlena header,
/// range oid, bounds present iff RANGE_HAS_L/UBOUND(flags), zero pad bytes
/// for alignment before an upper bound, flags byte last). The flags byte is
/// FULLY ARBITRARY (incl. NULL/CONTAIN_EMPTY bits) — both sides consume the
/// identical image, mirroring the proved harnesses' full-flags domain.
fn build_image(t: usize, flags: u8, lo: &Bound, up: &Bound) -> Vec<u8> {
    let p = PINS[t];
    let flags = fence_flags(t, flags);
    let mut img = vec![0u8; 8];
    img[4..8].copy_from_slice(&p.rngtypid.to_ne_bytes());
    let has_l = flags & (rt::RANGE_EMPTY | rt::RANGE_LB_NULL | rt::RANGE_LB_INF) == 0;
    let has_u = flags & (rt::RANGE_EMPTY | rt::RANGE_UB_NULL | rt::RANGE_UB_INF) == 0;
    let push = |img: &mut Vec<u8>, b: &Bound| match b {
        Bound::ByVal(v) => {
            if p.typlen == 4 {
                while img.len() % 4 != 0 {
                    img.push(0);
                }
                img.extend_from_slice(&(*v as i32).to_le_bytes());
            } else {
                while img.len() % 8 != 0 {
                    img.push(0);
                }
                img.extend_from_slice(&v.to_le_bytes());
            }
        }
        Bound::Num(bytes) => {
            while img.len() % 4 != 0 {
                img.push(0);
            }
            img.extend_from_slice(bytes);
        }
    };
    if has_l {
        push(&mut img, lo);
    }
    if has_u {
        push(&mut img, up);
    }
    img.push(flags);
    let n = img.len();
    img[0..4].copy_from_slice(&datum::set_varsize_4b(n));
    img
}

/// On-disk-legal flags for arms whose C side re-serializes from the input
/// image (canonical functions assume make_range-produced flags).
fn wf_flags(raw: u8) -> u8 {
    if raw & rt::RANGE_EMPTY != 0 {
        rt::RANGE_EMPTY
    } else {
        let mut f =
            raw & (rt::RANGE_LB_INC | rt::RANGE_UB_INC | rt::RANGE_LB_INF | rt::RANGE_UB_INF);
        if f & rt::RANGE_LB_INF != 0 {
            f &= !rt::RANGE_LB_INC;
        }
        if f & rt::RANGE_UB_INF != 0 {
            f &= !rt::RANGE_UB_INC;
        }
        f
    }
}

/// Oracle class for "the C body returned SQL NULL" (see PG_DIFF_ISNULL in
/// csrc/pg_rangetypes_io.c). The nullness plane is COMPARED, never skipped.
const C_ISNULL: i32 = 97;

/// Compare a C entry outcome (ret + image bytes) with a Rust fc outcome
/// producing a range image. Planes: nullness, error class, image bytes.
fn compare_range_result(name: &str, cret: i32, cbytes: &[u8], r: &FcOut, dbg: &str) {
    assert!(cret != C_BUFCAP, "{name}: oracle buffer too small (harness bug) {dbg}");
    match &r.result {
        Ok(d) => {
            if r.isnull || cret == C_ISNULL {
                assert!(
                    r.isnull && cret == C_ISNULL,
                    "{name} NULLNESS DIVERGENCE {dbg}: C ret {cret} vs Rust isnull={}",
                    r.isnull
                );
                return;
            }
            assert!(cret == 0, "{name} DIVERGENCE {dbg}: C err {cret} vs Rust Ok");
            let rbytes = datum_varlena_bytes(*d);
            assert!(
                rbytes == cbytes,
                "{name} DIVERGENCE {dbg}: image C={cbytes:02x?} Rust={rbytes:02x?}"
            );
        }
        Err(e) => {
            let rc = err_class(e);
            assert!(
                cret == rc,
                "{name} DIVERGENCE {dbg}: C err {cret} vs Rust err {rc} ({})",
                e.message
            );
        }
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn rangetypes_diff(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&typb, payload)) = rest.split_first() else {
        return;
    };
    let t = (typb % 3) as usize;
    let ctx = MemoryContext::new("rangetypes_fuzz");
    let mcx = ctx.mcx();

    match sel % 11 {
        0 => arm_text_io(t, payload, mcx),
        1 => arm_binary_io(t, payload, mcx),
        2 => arm_ctor(t, payload, mcx, false),
        3 => arm_ctor(t, payload, mcx, true),
        4 => arm_accessors(t, payload, mcx),
        5 => arm_ops(t, payload, mcx),
        6 => arm_elem(t, payload, mcx),
        7 => arm_setops(t, payload, mcx),
        8 => arm_hash(t, payload, mcx),
        9 => arm_canonical(typb, payload, mcx),
        10 => arm_subdiff(typb, payload, mcx),
        _ => unreachable!(),
    }
}

fn arm_text_io(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    if payload.len() > 256 || payload.contains(&0) {
        return;
    }
    let Ok(cs) = CString::new(payload) else { return };
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret = unsafe {
        pg_diff_range_in(t as i32, cs.as_ptr(), cbuf.as_mut_ptr(), &mut clen, OUTCAP as i32)
    };
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_input);
    let r = fc_call(
        rb::fc_range_in,
        Some(&mut fl),
        mcx,
        [
            Some(Datum::from_usize(cs.as_ptr() as usize)),
            Some(Datum::from_u32(PINS[t].rngtypid)),
            Some(Datum::from_i32(-1)),
        ],
    );
    let dbg = format!("t={t} lit={:?}", String::from_utf8_lossy(payload));
    compare_range_result("range_in", cret, &cbuf[..clen as usize], &r, &dbg);
    if cret != 0 {
        return;
    }
    // range_out roundtrip over the (identical) image
    let img = cbuf[..clen as usize].to_vec();
    let mut tbuf = vec![0u8; OUTCAP];
    let mut tlen = 0i32;
    let tret = unsafe {
        pg_diff_range_out(img.as_ptr(), tbuf.as_mut_ptr().cast(), &mut tlen, OUTCAP as i32)
    };
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_output);
    let r = fc_call(
        rb::fc_range_out,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    assert!(tret != C_BUFCAP, "range_out: oracle buffer too small (harness bug) {dbg}");
    match &r.result {
        Ok(d) => {
            if r.isnull || tret == C_ISNULL {
                assert!(
                    r.isnull && tret == C_ISNULL,
                    "range_out NULLNESS DIVERGENCE {dbg}: C ret {tret} vs Rust isnull={}",
                    r.isnull
                );
                return;
            }
            assert!(tret == 0, "range_out DIVERGENCE {dbg}: C err {tret} vs Ok");
            let rbytes = datum_cstring_bytes(*d);
            assert!(
                rbytes == &tbuf[..tlen as usize],
                "range_out DIVERGENCE {dbg}: C={:?} Rust={:?}",
                String::from_utf8_lossy(&tbuf[..tlen as usize]),
                String::from_utf8_lossy(rbytes)
            );
        }
        Err(e) => {
            assert!(
                tret == err_class(e),
                "range_out DIVERGENCE {dbg}: C err {tret} vs Rust err {} ({})",
                err_class(e),
                e.message
            );
        }
    }
}

fn arm_binary_io(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    if payload.len() > 512 {
        return;
    }
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret = unsafe {
        pg_diff_range_recv(
            t as i32,
            payload.as_ptr(),
            payload.len() as i32,
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
        )
    };
    let mut si = StringInfo::with_capacity_in(mcx, payload.len() + 1).unwrap();
    si.append_bytes(payload).unwrap();
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_receive);
    let r = fc_call(
        rb::fc_range_recv,
        Some(&mut fl),
        mcx,
        [
            Some(Datum::from_usize(&mut si as *mut StringInfo as usize)),
            Some(Datum::from_u32(PINS[t].rngtypid)),
            Some(Datum::from_i32(-1)),
        ],
    );
    let dbg = format!("t={t} wire={payload:02x?}");
    compare_range_result("range_recv", cret, &cbuf[..clen as usize], &r, &dbg);
    if cret != 0 {
        return;
    }
    let img = cbuf[..clen as usize].to_vec();
    let mut wbuf = vec![0u8; OUTCAP];
    let mut wlen = 0i32;
    let wret =
        unsafe { pg_diff_range_send(img.as_ptr(), wbuf.as_mut_ptr(), &mut wlen, OUTCAP as i32) };
    assert!(wret != C_BUFCAP, "range_send: oracle buffer too small (harness bug) {dbg}");
    assert!(wret == 0, "range_send errored on a recv-produced image {dbg}");
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_send);
    let r = fc_call(
        rb::fc_range_send,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    let d = r.result.expect("range_send infallible on a recv-produced image");
    assert!(
        !r.isnull && wret != C_ISNULL,
        "range_send NULLNESS DIVERGENCE {dbg}: C ret {wret} vs Rust isnull={}",
        r.isnull
    );
    let rbytes = datum_varlena_bytes(d);
    assert!(
        &rbytes[4..] == &wbuf[..wlen as usize],
        "range_send DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
        &wbuf[..wlen as usize],
        &rbytes[4..]
    );
}

fn arm_ctor(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>, three: bool) {
    let mut rd = Rd(payload, 0);
    let nullbits = rd.u8();
    let f1 = rd.u8();
    let f2 = rd.u8();
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let null1 = nullbits & 1 != 0;
    let null2 = nullbits & 2 != 0;
    let flags_txt = [f1, f2];
    let (v1, n1) = lo.c_args();
    let (v2, n2) = up.c_args();
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret = unsafe {
        pg_diff_range_ctor(
            t as i32,
            if three { 3 } else { 2 },
            v1,
            v2,
            n1,
            n2,
            null1 as i32,
            null2 as i32,
            flags_txt.as_ptr(),
            2,
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
        )
    };
    let mut fl = ops_flinfo(t);
    let a0 = if null1 { None } else { Some(lo.rust_datum()) };
    let a1 = if null2 { None } else { Some(up.rust_datum()) };
    let r = if three {
        let mut tv = vec![0u8; 6];
        tv[0..4].copy_from_slice(&datum::set_varsize_4b(6));
        tv[4..6].copy_from_slice(&flags_txt);
        fc_call(
            rb::fc_range_constructor3,
            Some(&mut fl),
            mcx,
            [a0, a1, Some(Datum::from_usize(tv.as_ptr() as usize))],
        )
    } else {
        fc_call(rb::fc_range_constructor2, Some(&mut fl), mcx, [a0, a1])
    };
    let dbg = format!("t={t} three={three} nulls={nullbits:x} flags={flags_txt:?}");
    compare_range_result("range_ctor", cret, &cbuf[..clen as usize], &r, &dbg);
}

fn arm_accessors(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let flags = rd.u8();
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let img = build_image(t, flags, &lo, &up);
    let mut lob = vec![0u8; OUTCAP];
    let mut upb = vec![0u8; OUTCAP];
    let (mut lol, mut lon, mut upl, mut upn) = (0i32, 0i32, 0i32, 0i32);
    let mut bools = [0u8; 5];
    let cret = unsafe {
        pg_diff_range_accessors(
            img.as_ptr(),
            lob.as_mut_ptr(),
            &mut lol,
            &mut lon,
            upb.as_mut_ptr(),
            &mut upl,
            &mut upn,
            bools.as_mut_ptr(),
            OUTCAP as i32,
        )
    };
    assert!(cret != C_BUFCAP, "accessors: oracle buffer too small (harness bug) {}", flags);
    assert!(cret == 0, "accessors: C errored ({cret}) on a built image");
    let dbg = format!("t={t} flags={flags:02x}");
    let acc: [(&str, PGFunction, i32, &Vec<u8>, i32); 2] = [
        ("lower", rb::fc_range_lower, lon, &lob, lol),
        ("upper", rb::fc_range_upper, upn, &upb, upl),
    ];
    for (which, fc, cnull, cbytes, clen) in acc {
        let mut fl = ops_flinfo(t);
        let r = fc_call(fc, Some(&mut fl), mcx, [Some(Datum::from_usize(img.as_ptr() as usize))]);
        let d = r.result.expect("lower/upper infallible");
        assert!(
            r.isnull == (cnull != 0),
            "range_{which} NULLNESS DIVERGENCE {dbg}: C null={cnull} Rust null={}",
            r.isnull
        );
        if !r.isnull {
            if PINS[t].typbyval {
                let cv = i64::from_le_bytes(cbytes[..8].try_into().unwrap());
                let rv = if PINS[t].typlen == 4 { d.as_i32() as i64 } else { d.as_i64() };
                assert!(rv == cv, "range_{which} DIVERGENCE {dbg}: C={cv} Rust={rv}");
            } else {
                let rbytes = datum_varlena_bytes(d);
                assert!(
                    rbytes == &cbytes[..clen as usize],
                    "range_{which} DIVERGENCE {dbg}: C={:02x?} Rust={rbytes:02x?}",
                    &cbytes[..clen as usize]
                );
            }
        }
    }
    let bfcs: [PGFunction; 5] = [
        rb::fc_range_empty,
        rb::fc_range_lower_inc,
        rb::fc_range_upper_inc,
        rb::fc_range_lower_inf,
        rb::fc_range_upper_inf,
    ];
    for (i, fc) in bfcs.into_iter().enumerate() {
        let mut fl = ops_flinfo(t);
        let r = fc_call(fc, Some(&mut fl), mcx, [Some(Datum::from_usize(img.as_ptr() as usize))]);
        let d = r.result.expect("bool accessors infallible");
        assert!(
            (d.as_usize() != 0) == (bools[i] != 0),
            "bool accessor {i} DIVERGENCE {dbg}: C={} Rust={}",
            bools[i],
            d.as_usize()
        );
    }
}

fn arm_ops(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let flags1 = rd.u8();
    let flags2 = rd.u8();
    let lo1 = Bound::decode(t, &mut rd, mcx);
    let up1 = Bound::decode(t, &mut rd, mcx);
    let lo2 = Bound::decode(t, &mut rd, mcx);
    let up2 = Bound::decode(t, &mut rd, mcx);
    let (Some(lo1), Some(up1), Some(lo2), Some(up2)) = (lo1, up1, lo2, up2) else {
        return;
    };
    let img1 = build_image(t, flags1, &lo1, &up1);
    let img2 = build_image(t, flags2, &lo2, &up2);
    let mut cres = [0i32; 15];
    let cret = unsafe { pg_diff_range_ops(img1.as_ptr(), img2.as_ptr(), cres.as_mut_ptr()) };
    let dbg = format!("t={t} f1={flags1:02x} f2={flags2:02x}");
    let fcs: [(&str, PGFunction, bool); 15] = [
        ("eq", rb::fc_range_eq, true),
        ("ne", rb::fc_range_ne, true),
        ("lt", rb::fc_range_lt, true),
        ("le", rb::fc_range_le, true),
        ("gt", rb::fc_range_gt, true),
        ("ge", rb::fc_range_ge, true),
        ("cmp", rb::fc_range_cmp, false),
        ("overlaps", rb::fc_range_overlaps, true),
        ("contains", rb::fc_range_contains, true),
        ("contained_by", rb::fc_range_contained_by, true),
        ("before", rb::fc_range_before, true),
        ("after", rb::fc_range_after, true),
        ("adjacent", rb::fc_range_adjacent, true),
        ("overleft", rb::fc_range_overleft, true),
        ("overright", rb::fc_range_overright, true),
    ];
    for (i, (name, fc, isbool)) in fcs.into_iter().enumerate() {
        let mut fl = ops_flinfo(t);
        let r = fc_call(
            fc,
            Some(&mut fl),
            mcx,
            [
                Some(Datum::from_usize(img1.as_ptr() as usize)),
                Some(Datum::from_usize(img2.as_ptr() as usize)),
            ],
        );
        match r.result {
            Ok(d) => {
                assert!(cret == 0, "range_{name} DIVERGENCE {dbg}: C err {cret} vs Ok");
                let rv = if isbool { (d.as_usize() != 0) as i32 } else { d.as_i32() };
                assert!(rv == cres[i], "range_{name} DIVERGENCE {dbg}: C={} Rust={rv}", cres[i]);
            }
            Err(e) => {
                // The C bundle aborts at its FIRST error; every arm sees the
                // same images, so any Rust error must match the C class.
                assert!(
                    cret == err_class(&e),
                    "range_{name} DIVERGENCE {dbg}: C err {cret} vs Rust {} ({})",
                    err_class(&e),
                    e.message
                );
                return;
            }
        }
    }
}

fn arm_elem(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let flags = rd.u8();
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let el = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up), Some(el)) = (lo, up, el) else { return };
    let img = build_image(t, flags, &lo, &up);
    let (ev, en) = el.c_args();
    let (mut c1, mut c2) = (0i32, 0i32);
    let cret = unsafe { pg_diff_range_contains_elem(img.as_ptr(), ev, en, &mut c1, &mut c2) };
    let dbg = format!("t={t} flags={flags:02x}");
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        rb::fc_range_contains_elem,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize)), Some(el.rust_datum())],
    );
    match r.result {
        Ok(d) => assert!(
            cret == 0 && (d.as_usize() != 0) as i32 == c1,
            "contains_elem DIVERGENCE {dbg}: C=({cret},{c1}) Rust={}",
            d.as_usize()
        ),
        Err(e) => assert!(
            cret == err_class(&e),
            "contains_elem DIVERGENCE {dbg}: C err {cret} vs {}",
            err_class(&e)
        ),
    }
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        rb::fc_elem_contained_by_range,
        Some(&mut fl),
        mcx,
        [Some(el.rust_datum()), Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    match r.result {
        Ok(d) => assert!(
            cret == 0 && (d.as_usize() != 0) as i32 == c2,
            "elem_contained_by DIVERGENCE {dbg}: C=({cret},{c2}) Rust={}",
            d.as_usize()
        ),
        Err(e) => assert!(
            cret == err_class(&e),
            "elem_contained_by DIVERGENCE {dbg}: C err {cret} vs {}",
            err_class(&e)
        ),
    }
}

fn arm_setops(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let flags1 = rd.u8();
    let flags2 = rd.u8();
    let lo1 = Bound::decode(t, &mut rd, mcx);
    let up1 = Bound::decode(t, &mut rd, mcx);
    let lo2 = Bound::decode(t, &mut rd, mcx);
    let up2 = Bound::decode(t, &mut rd, mcx);
    let (Some(lo1), Some(up1), Some(lo2), Some(up2)) = (lo1, up1, lo2, up2) else {
        return;
    };
    let img1 = build_image(t, flags1, &lo1, &up1);
    let img2 = build_image(t, flags2, &lo2, &up2);
    let fcs: [(&str, PGFunction); 4] = [
        ("union", rb::fc_range_union),
        ("intersect", rb::fc_range_intersect),
        ("minus", rb::fc_range_minus),
        ("merge", rb::fc_range_merge),
    ];
    for (which, (name, fc)) in fcs.into_iter().enumerate() {
        let mut cbuf = vec![0u8; OUTCAP];
        let mut clen = 0i32;
        let cret = unsafe {
            pg_diff_range_setop(
                which as i32,
                img1.as_ptr(),
                img2.as_ptr(),
                cbuf.as_mut_ptr(),
                &mut clen,
                OUTCAP as i32,
            )
        };
        let mut fl = ops_flinfo(t);
        let r = fc_call(
            fc,
            Some(&mut fl),
            mcx,
            [
                Some(Datum::from_usize(img1.as_ptr() as usize)),
                Some(Datum::from_usize(img2.as_ptr() as usize)),
            ],
        );
        let dbg = format!("t={t} {name} f1={flags1:02x} f2={flags2:02x}");
        compare_range_result("range_setop", cret, &cbuf[..clen as usize], &r, &dbg);
    }
}

fn arm_hash(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    // CONTAIN_EMPTY (0x80) masked: a GiST-internal bit never present in
    // stored ranges, and C's `(uint32) flags` hash sign-extends it on
    // signed-char hosts (Apple arm64) — a platform artifact, not a surface.
    let flags = rd.u8() & 0x7f;
    let seed = rd.i64() as u64;
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let img = build_image(t, flags, &lo, &up);
    let mut ch = 0u32;
    let cret = unsafe { pg_diff_hash_range(img.as_ptr(), &mut ch) };
    assert!(cret == 0, "hash_range: C errored ({cret})");
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        rb::fc_hash_range,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    let d = r.result.expect("hash_range infallible under seeded finfos");
    assert!(
        d.as_u32() == ch,
        "hash_range DIVERGENCE t={t} flags={flags:02x}: C={ch:#x} Rust={:#x}",
        d.as_u32()
    );
    let mut che = 0u64;
    let cret = unsafe { pg_diff_hash_range_extended(img.as_ptr(), seed, &mut che) };
    assert!(cret == 0, "hash_range_extended: C errored ({cret})");
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        rb::fc_hash_range_extended,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize)), Some(Datum::from_i64(seed as i64))],
    );
    let d = r.result.expect("hash_range_extended infallible");
    assert!(
        d.as_u64() == che,
        "hash_range_extended DIVERGENCE t={t} flags={flags:02x} seed={seed:#x}: C={che:#x} Rust={:#x}",
        d.as_u64()
    );
}

fn arm_canonical(typb: u8, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let ct = (typb % 3) as usize; // 0=int4range, 1=int8range, 2=daterange
    let mut rd = Rd(payload, 0);
    let flags = wf_flags(rd.u8());
    let bt = if ct == 2 { 0 } else { ct }; // date shares the byval-4 shape
    let lo = Bound::decode(bt, &mut rd, mcx);
    let up = Bound::decode(bt, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let mut img = build_image(bt, flags, &lo, &up);
    let (fc, roid): (PGFunction, Oid) = match ct {
        0 => (rb::fc_int4range_canonical, INT4RANGEOID),
        1 => (rb::fc_int8range_canonical, INT8RANGEOID),
        _ => (rb::fc_daterange_canonical, DATERANGEOID),
    };
    if ct == 2 {
        img[4..8].copy_from_slice(&roid.to_ne_bytes());
    }
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret = unsafe {
        pg_diff_range_canonical(
            if ct == 2 { 3 } else { ct as i32 },
            img.as_ptr(),
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
        )
    };
    let mut fl = if ct == 2 {
        let mut fl = FmgrInfo::new(fc, 0, 1, true, false);
        fl.set_fn_extra(rt::RangeInfo {
            pin: None,
            rngtypid: DATERANGEOID,
            collation: 0,
            elem_typid: DATEOID,
            elem: rt::ElemInfo { typlen: 4, typbyval: true, typalign: b'i', typstorage: b'p' },
            cmp: FmgrInfo::new(adt_date::builtins::fc_date_cmp, 1092, 2, true, false),
            canonical_oid: 3915,
            elem_hash: None,
            elem_hash_extended: None,
            own_typlen: -1,
            own_typbyval: false,
            own_typalign: b'd',
        });
        fl
    } else {
        ops_flinfo(ct)
    };
    let r = fc_call(fc, Some(&mut fl), mcx, [Some(Datum::from_usize(img.as_ptr() as usize))]);
    let dbg = format!("ct={ct} flags={flags:02x}");
    compare_range_result("range_canonical", cret, &cbuf[..clen as usize], &r, &dbg);
}

fn arm_subdiff(typb: u8, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let which = (typb % 6) as usize;
    let mut rd = Rd(payload, 0);
    let mut a = 0i64;
    let mut b = 0i64;
    let mut na: *const u8 = core::ptr::null();
    let mut nb: *const u8 = core::ptr::null();
    let n1v;
    let n2v;
    if which == 2 {
        let x = Bound::decode(2, &mut rd, mcx);
        let y = Bound::decode(2, &mut rd, mcx);
        let (Some(Bound::Num(x)), Some(Bound::Num(y))) = (x, y) else { return };
        n1v = x;
        n2v = y;
        na = n1v.as_ptr();
        nb = n2v.as_ptr();
    } else {
        a = rd.i64();
        b = rd.i64();
        n1v = Vec::new();
        n2v = Vec::new();
    }
    let mut cout = 0f64;
    let cret = unsafe { pg_diff_range_subdiff(which as i32, a, b, na, nb, &mut cout) };
    let fcs: [PGFunction; 6] = [
        rb::fc_int4range_subdiff,
        rb::fc_int8range_subdiff,
        rb::fc_numrange_subdiff,
        rb::fc_daterange_subdiff,
        rb::fc_tsrange_subdiff,
        rb::fc_tstzrange_subdiff,
    ];
    let args: [Option<Datum>; 2] = match which {
        2 => [
            Some(Datum::from_usize(n1v.as_ptr() as usize)),
            Some(Datum::from_usize(n2v.as_ptr() as usize)),
        ],
        0 | 3 => [Some(Datum::from_i32(a as i32)), Some(Datum::from_i32(b as i32))],
        _ => [Some(Datum::from_i64(a)), Some(Datum::from_i64(b))],
    };
    let r = fc_call(fcs[which], None, mcx, args);
    let dbg = format!("which={which} a={a} b={b}");
    match r.result {
        Ok(d) => {
            assert!(cret == 0, "subdiff DIVERGENCE {dbg}: C err {cret} vs Ok");
            let rv = f64::from_bits(d.as_u64());
            assert!(
                rv.to_bits() == cout.to_bits(),
                "subdiff DIVERGENCE {dbg}: C={cout} Rust={rv}"
            );
        }
        Err(e) => assert!(
            cret == err_class(&e),
            "subdiff DIVERGENCE {dbg}: C err {cret} vs Rust {} ({})",
            err_class(&e),
            e.message
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sel: u8, typ: u8, payload: &[u8]) {
        let mut v = vec![sel, typ];
        v.extend_from_slice(payload);
        rangetypes_diff(&v);
    }

    #[test]
    fn smoke_text_io() {
        for t in 0..3u8 {
            run(0, t, b"[1,2)");
            run(0, t, b"empty");
            run(0, t, b"(,)");
            run(0, t, b"[-3, 17]");
            run(0, t, b"[1,)");
            run(0, t, b"(,99]");
            run(0, t, b"garbage");
            run(0, t, b"[2,1)");
            run(0, t, b"[1 2)");
            run(0, t, b"[\"1\",\"2\"]");
            run(0, t, b" [ 1 , 2 ) ");
        }
        run(0, 2, b"[1.5,2.75)");
        run(0, 2, b"[-1e10,)");
        run(0, 2, b"[NaN,NaN]");
    }

    #[test]
    fn smoke_ctors_accessors() {
        for t in 0..3u8 {
            let mut p = vec![0u8, b'[', b')'];
            p.extend_from_slice(&[1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0]);
            p.extend_from_slice(b"\x027 \x0212 ");
            run(2, t, &p);
            run(3, t, &p);
            let mut p = vec![0x02u8];
            p.extend_from_slice(&[1, 0, 0, 0, 2, 0, 0, 0, 9, 0, 0, 0, 9, 0, 0, 0]);
            p.extend_from_slice(b"\x027 \x0212 ");
            run(4, t, &p);
        }
        run(2, 0, &[3, b'[', b')', 1, 2, 3, 4, 5, 6, 7, 8]);
        run(3, 0, &[0, b'x', b')', 1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn smoke_ops_setops_hash_elem() {
        for t in 0..3u8 {
            for f1 in [0u8, 1, 2, 4, 6, 8, 0x10, 0x18, 0x80, 0x20, 0x40, 0xff] {
                let mut p = vec![f1, f1 ^ 0x06];
                p.extend_from_slice(&[1, 0, 0, 0, 9, 0, 0, 0, 2, 0, 0, 0, 8, 0, 0, 0]);
                p.extend_from_slice(b"\x021 \x025 \x022 \x024 \x029 ");
                run(5, t, &p);
                run(7, t, &p);
                run(8, t, &p);
                run(6, t, &p);
                run(4, t, &p);
            }
        }
    }

    #[test]
    fn smoke_canonical_subdiff() {
        for ct in 0..3u8 {
            for f in [0u8, 2, 4, 6, 1, 8, 0x10] {
                let mut p = vec![f];
                p.extend_from_slice(&[5, 0, 0, 0, 9, 0, 0, 0]);
                run(9, ct, &p);
            }
            // canonical overflow cells
            let mut p = vec![4u8]; // UB_INC -> upper+1 overflow candidate
            p.extend_from_slice(&i32::MAX.to_le_bytes());
            p.extend_from_slice(&i32::MAX.to_le_bytes());
            run(9, ct, &p);
        }
        for w in 0..6u8 {
            let mut p = vec![];
            p.extend_from_slice(&(-5i64).to_le_bytes());
            p.extend_from_slice(&(1234i64).to_le_bytes());
            if w == 2 {
                p = vec![3, b'1', b'.', b'5', 3, b'0', b'.', b'5'];
            }
            run(10, w, &p);
        }
    }

    #[test]
    fn smoke_binary_io() {
        for t in 0..3u8 {
            run(1, t, &[0x01]);
            run(1, t, &[0x00]);
            run(1, t, &[]);
            if t == 0 {
                // well-formed int4 wire: flags 0x06 ([] inclusive both)
                let mut w = vec![0x06u8];
                w.extend_from_slice(&4u32.to_be_bytes());
                w.extend_from_slice(&1i32.to_be_bytes());
                w.extend_from_slice(&4u32.to_be_bytes());
                w.extend_from_slice(&9i32.to_be_bytes());
                run(1, t, &w);
            }
        }
    }

    /// Single-field-difference witness pairs (skill OBLIGATION): image pairs
    /// differing in exactly one field — each field, both orders — so every
    /// field's contribution to eq/cmp/hash verdicts is witnessed.
    #[test]
    fn witness_pairs() {
        for t in 0..2u8 {
            let base: &[u8] = &[1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0];
            let lower_delta: &[u8] = &[2, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0];
            let upper_delta: &[u8] = &[1, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0];
            for (b1, b2) in [
                (base, lower_delta),
                (lower_delta, base),
                (base, upper_delta),
                (upper_delta, base),
            ] {
                for (f1, f2) in [(6u8, 6u8), (6, 4), (4, 6), (2, 6), (6, 2)] {
                    let mut p = vec![f1, f2];
                    p.extend_from_slice(b1);
                    p.extend_from_slice(b2);
                    run(5, t, &p);
                    run(8, t, &p);
                }
            }
        }
    }
}
