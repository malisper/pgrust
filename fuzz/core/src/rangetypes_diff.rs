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
//! Input layout: [sel][typ][payload]; sel % 12 picks the arm, typ % 3 the
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
//!  11 make_range:  internal-API arm, hard+soft escontext (the real range_in
//!                  caller shape; the only soft route to canonicalize's
//!                  per-type dispatch — constructors hardcode NULL escontext)
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
    fn pg_diff_range_in_soft(
        typ: i32,
        s: *const core::ffi::c_char,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
        soft_class: *mut i32,
        isnull_out: *mut i32,
    ) -> i32;
    fn pg_diff_range_canonical_soft(
        typ: i32,
        img: *const u8,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
        soft_class: *mut i32,
        isnull_out: *mut i32,
    ) -> i32;
    fn pg_diff_make_range(
        typ: i32,
        v1: i64,
        v2: i64,
        flags: i32,
        soft: i32,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
        soft_class: *mut i32,
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
        null3: i32,
        soft: i32,
        soft_class: *mut i32,
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
        errs: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_range_ops(
        img1: *const u8,
        img2: *const u8,
        res: *mut i32,
        errs: *mut i32,
    ) -> i32;
    fn pg_diff_range_contains_elem(
        img: *const u8,
        v: i64,
        numptr: *const u8,
        contains: *mut i32,
        contained: *mut i32,
        err_contains: *mut i32,
        err_contained: *mut i32,
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
pub(crate) fn err_class(e: &PgError) -> i32 {
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
    } else if e.sqlstate == te::ERRCODE_PROGRAM_LIMIT_EXCEEDED {
        // StringInfo's MaxAllocSize ceiling: reachable from numrange text io,
        // where a bound with a huge exponent prints past 1 GiB.
        12
    } else if e.sqlstate == te::ERRCODE_DATA_CORRUPTED {
        // XX001: corrupt inline-compressed (pglz) bound — shared class 15
        15
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

/// Instantiations driven by the arms that need an element I/O function
/// (text/binary io). daterange is excluded there: its element I/O is date_in /
/// date_out, which this oracle does not vendor.
const NPINS_IO: usize = 3;
/// Instantiations driven by the arms that only need bound DATUMS (constructors,
/// images, operators) — daterange included.
const NPINS: usize = 4;

const PINS: [Pin; NPINS] = [
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
    // daterange: a DISCRETE type with a canonicalize function, which is the
    // whole point of driving it — canonicalize()'s F_DATERANGE_CANONICAL
    // dispatch arm is unreachable from the three pins above (int4/int8 take
    // their own arms, numrange is continuous and has no canonical function).
    // date is int4-shaped (typlen 4, byval, 'i', 'p'), so the byval bound
    // decode path serves it unchanged.
    Pin {
        rngtypid: DATERANGEOID,
        elem_typid: DATEOID,
        typlen: 4,
        typbyval: true,
        typalign: b'i',
        typstorage: b'p',
    },
];


fn cmp_finfo(t: usize) -> FmgrInfo {
    match t {
        0 => FmgrInfo::new(nbt_compare::builtins::fc_btint4cmp, 351, 2, true, false),
        1 => FmgrInfo::new(nbt_compare::builtins::fc_btint8cmp, 842, 2, true, false),
        3 => FmgrInfo::new(adt_date::builtins::fc_date_cmp, 1092, 2, true, false),
        _ => FmgrInfo::new(adt_numeric::builtins::fc_numeric_cmp, 1769, 2, true, false),
    }
}

fn hash_finfo(t: usize) -> FmgrInfo {
    match t {
        0 => FmgrInfo::new(adt_int::builtins::fc_hashint4, 450, 1, true, false),
        1 => FmgrInfo::new(adt_int8::builtins::fc_hashint8, 949, 1, true, false),
        3 => FmgrInfo::new(adt_int::builtins::fc_hashint4, 450, 1, true, false),
        _ => FmgrInfo::new(adt_numeric::builtins::fc_hash_numeric, 432, 1, true, false),
    }
}

fn hash_ext_finfo(t: usize) -> FmgrInfo {
    match t {
        0 => FmgrInfo::new(adt_int::builtins::fc_hashint4extended, 425, 2, true, false),
        1 => FmgrInfo::new(adt_int8::builtins::fc_hashint8extended, 442, 2, true, false),
        3 => FmgrInfo::new(adt_int::builtins::fc_hashint4extended, 425, 2, true, false),
        _ => FmgrInfo::new(adt_numeric::builtins::fc_hash_numeric_extended, 780, 2, true, false),
    }
}

const F_CANONICAL: [Oid; NPINS] =
    [3914, 3928, 0 /* numrange: continuous */, 3915 /* daterange_canonical */];

/// fn_expr rettype carriers for the constructor arms. STATICS, not a per-call
/// `Box::leak`: the leaked form cost 24 bytes per ops_flinfo() call and the
/// fleet's LeakSanitizer killed the first 10M campaign at 8 execs
/// (pgrust-fuzz-campaign-1785516178-4344-37961, 360 bytes in 15 objects).
/// PINS is const and there are exactly three instantiations, so nothing needs
/// allocating at all. Name matches the sibling multirange target's
/// `RNG_RETTYPE` so the two can be reconciled at merge.
static RNG_RETTYPE: [AggFnArgTypes; NPINS] = [
    AggFnArgTypes { rettype: INT4RANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: INT8RANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: NUMRANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: DATERANGEOID, argtypes: &[] },
];


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
    // SAFETY: &'static statics outlive every read of the carrier.
    fl.fn_expr = Some(unsafe { FnExprErased::from_node_ref(&RNG_RETTYPE[t]) });
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
    match out.result {
        Ok(d) => Some(datum_varlena_bytes(d).to_vec()),
        Err(_) => {
            STATS.with(|s| s.borrow_mut().mint_failed += 1);
            None
        }
    }
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
                // Try the raw bytes FIRST, so odd literals (exponents, signs,
                // NaN/Infinity, underscores, hex prefixes) keep reaching
                // numeric_in on the range path. Random bytes rarely parse
                // though (measured: 464k mint failures per 1M execs, i.e. the
                // byref instantiation — the one that carries the packed-short
                // layout — was getting ~10x less coverage than the byval ones),
                // so on failure fall back to a digit-mapped literal built from
                // the SAME bytes. Deterministic, and it only adds inputs.
                if let Some(b) = mint_numeric(mcx, &lit) {
                    return Some(Bound::Num(b));
                }
                let digits: Vec<u8> = lit
                    .iter()
                    .map(|b| match b % 12 {
                        10 => b'.',
                        11 => b'e',
                        d => b'0' + d,
                    })
                    .collect();
                mint_numeric(mcx, &digits).map(Bound::Num)
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


// ---------------------------------------------------------------------------
// Image builders: TWO layouts, deliberately
// ---------------------------------------------------------------------------
//
// The hand builder below writes byref bounds with their 4-byte varlena header
// and pads to element alignment. PG's `datum_write` does NOT do that for a
// PACKABLE element: numeric is typstorage 'm', so a small bound is converted
// to a 1-BYTE SHORT header with NO alignment. Measured for `numrange [1.5,2.5)`:
//
//   serializer : 5c000000 420f0000 0f 80800100 8813 0f 80800200 8813 02
//   hand-built : 7c000000 420f0000 28000000 80800100 8813 0000 28000000 ...
//
// Both images are read IDENTICALLY by both sides (verified: the C accessors
// entry returns ret=0 with all per-call errcodes 0 and hands back exactly the
// 4-byte-header bounds), so the hand builder is not malformed — but it never
// produces the packed-short layout that real stored ranges carry, leaving
// fetch_att / att_addlength_pointer / att_align_pointer's VARATT_IS_1B arms
// unexercised. That is a COVERAGE GAP, not a correctness defect.
//
// So both layouts are fuzzed, chosen by a payload bit:
//   * `build_image`      — arbitrary flags byte (all 256 for byval), the
//                          full range_deserialize flags lattice;
//   * `build_image_ctor` — bytes straight out of the SHIPPED
//                          fc_range_constructor3, i.e. exactly what
//                          `numrange(1.5, 2.5, '[)')` stores, packed short
//                          headers included. Builder/serializer skew is
//                          structurally impossible on this path.
// (Credit: the constructor-built path is the sibling multirange lane's fix.)

/// Build a range image through the SHIPPED constructor, so its layout is
/// whatever `range_serialize`/`datum_write` actually emit. `None` = the
/// constructor rejected the pair (lower > upper) or returned SQL NULL.
fn build_image_ctor(
    t: usize,
    flags_txt: [u8; 2],
    lo: &Bound,
    up: &Bound,
    null_lo: bool,
    null_up: bool,
    mcx: mcx::Mcx<'_>,
) -> Option<Vec<u8>> {
    let mut tv = vec![0u8; 6];
    tv[0..4].copy_from_slice(&datum::set_varsize_4b(6));
    tv[4..6].copy_from_slice(&flags_txt);
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        rb::fc_range_constructor3,
        Some(&mut fl),
        mcx,
        [
            if null_lo { None } else { Some(lo.rust_datum()) },
            if null_up { None } else { Some(up.rust_datum()) },
            Some(Datum::from_usize(tv.as_ptr() as usize)),
        ],
    );
    let d = r.result.ok()?;
    if r.isnull {
        return None;
    }
    Some(datum_varlena_bytes(d).to_vec())
}

/// Legal 2-byte flags text for the constructor path.
fn ctor_flags_txt(raw: u8) -> [u8; 2] {
    [
        if raw & 1 != 0 { b'[' } else { b'(' },
        if raw & 2 != 0 { b']' } else { b')' },
    ]
}

/// ANTI-VACUITY COUNTERS. A builder that silently declined to construct
/// anything would hand back a clean run that proved nothing (the gate-blindness
/// class), so each layout/instantiation actually compared is counted and the
/// tests assert the counts are non-zero. `PGRUST_FUZZ_RT_STATS=1` dumps them.
#[derive(Default)]
pub struct BuildStats {
    /// hand-built images compared, per instantiation
    pub hand: [u64; 3],
    /// constructor-built images compared, per instantiation
    pub ctor: [u64; 3],
    /// constructor calls that declined (lower > upper, or SQL NULL)
    pub ctor_declined: u64,
    /// numrange bounds that failed to mint from the payload literal
    pub mint_failed: u64,
    /// range_in comparisons run in SOFT-error (escontext) mode
    pub soft_mode: u64,
    /// of those, ones where a soft error was actually captured (the edge that
    /// the hard-mode plane can never reach)
    pub soft_captured: u64,
    /// constructor3 calls with a SQL-NULL flags argument (non-strict arm)
    pub null_flags_arg: u64,
    /// constructor3 calls with a flags text whose length is not 2
    pub flags_len_off: u64,
    /// images built at the daterange instantiation (canonicalize dispatch)
    pub daterange_built: u64,
    /// bound images fed through the toast seam, by kind: [ondisk, pglz, short]
    pub toast_built: [u64; 3],
}

thread_local! {
    pub static STATS: core::cell::RefCell<BuildStats> =
        core::cell::RefCell::new(BuildStats::default());
}

fn note_hand(t: usize) {
    STATS.with(|s| s.borrow_mut().hand[t] += 1);
}

fn note_ctor(t: usize) {
    STATS.with(|s| s.borrow_mut().ctor[t] += 1);
}

fn bump(f: impl FnOnce(&mut BuildStats)) {
    STATS.with(|s| f(&mut s.borrow_mut()));
}

/// Re-pack a 4-byte-header range image into the SHORT (1-byte) varlena header
/// form, which is what a small stored range actually carries on disk: range
/// types are typlen -1 with typstorage 'x', so datum_write packs them short in
/// a tuple. Feeding one exercises the detoast-on-argument path
/// (`arg_range`'s RangeArg::Owned arm, builtins.rs:47) that a 4B-only driver
/// never reaches; the C oracle's PG_GETARG_RANGE_P expands short headers
/// through pg_rt_detoast, so both sides see the same logical value.
/// `None` = too big for the short form (>126 bytes payload).
fn to_short_header(img: &[u8]) -> Option<Vec<u8>> {
    let payload = &img[4..];
    let total = payload.len() + 1;
    if total > 126 {
        return None;
    }
    let mut out = Vec::with_capacity(total);
    out.push(((total as u8) << 1) | 0x01);
    out.extend_from_slice(payload);
    Some(out)
}

/// Wrap a 4-byte-header varlena into the INLINE-COMPRESSED (4B_C) form, minted
/// with the SHIPPED pglz compressor. This is the second on-disk shape a stored
/// byref bound can take (typstorage 'm'): header (total<<2)|0b10, then
/// va_tcinfo = decompressed data size (method bits 00 = pglz), then the
/// compressed stream. Both sides consume the IDENTICAL bytes; decompression is
/// the compared computation (C: verbatim vendored pglz_decompress; Rust: the
/// shipped detoast). `None` = pglz declines (incompressible/too small), which
/// mirrors real storage: such values are kept uncompressed.
fn to_compressed(img: &[u8]) -> Option<Vec<u8>> {
    use core::mem::MaybeUninit;
    let payload = &img[4..];
    let mut dst: Vec<MaybeUninit<u8>> =
        vec![MaybeUninit::uninit(); pglz::pglz_max_output(payload.len())];
    // STRATEGY_ALWAYS, not DEFAULT: numeric bound images are mostly under the
    // 32-byte default minimum and DEFAULT would decline nearly every one. The
    // compressed FORM is what is under test (decompress + bound handling), not
    // the storage policy that decides when to compress; a 4B_C image is legal
    // input to the detoast path regardless of how small its payload is.
    let clen = pglz::pglz_compress_into(payload, &mut dst, &pglz::PGLZ_STRATEGY_ALWAYS)?;
    let total = 8 + clen;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(((total as u32) << 2) | 0x02).to_ne_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_ne_bytes());
    // SAFETY: pglz_compress_into initialized the first clen bytes.
    out.extend_from_slice(unsafe { core::slice::from_raw_parts(dst.as_ptr().cast::<u8>(), clen) });
    Some(out)
}

/// One image for an arm, in one of the two layouts. `None` = skip this exec.
fn image_for(
    t: usize,
    layout_ctor: bool,
    flags: u8,
    lo: &Bound,
    up: &Bound,
    mcx: mcx::Mcx<'_>,
) -> Option<Vec<u8>> {
    if layout_ctor {
        // infinite bounds ride as SQL NULL args, matching the SQL surface
        let null_lo = flags & rt::RANGE_LB_INF != 0;
        let null_up = flags & rt::RANGE_UB_INF != 0;
        match build_image_ctor(t, ctor_flags_txt(flags >> 1), lo, up, null_lo, null_up, mcx) {
            Some(img) => {
                note_ctor(t);
                Some(img)
            }
            None => {
                STATS.with(|s| s.borrow_mut().ctor_declined += 1);
                None
            }
        }
    } else {
        note_hand(t);
        Some(build_image(t, flags, lo, up))
    }
}

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

/// Periodic non-vacuity report. `PGRUST_FUZZ_RT_STATS=1` makes a fuzz run print
/// the per-(layout x instantiation) build counts, so a clean campaign can be
/// SHOWN to have exercised the numrange/packed-short arms rather than assumed to
/// (a builder that quietly declined everything would otherwise pass vacuously —
/// the gate-blindness class).
fn maybe_report_stats() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static EXECS: AtomicU64 = AtomicU64::new(0);
    let n = EXECS.fetch_add(1, Ordering::Relaxed) + 1;
    if n % 250_000 != 0 {
        return;
    }
    if std::env::var_os("PGRUST_FUZZ_RT_STATS").is_none() {
        return;
    }
    STATS.with(|s| {
        let st = s.borrow();
        eprintln!(
            "[rt-stats] execs={n} hand(int4/int8/num)={:?} ctor(int4/int8/num)={:?} \
             ctor_declined={} mint_failed={} soft_mode={} soft_captured={} \
             null_flags_arg={} flags_len_off={} daterange={} toast(ondisk/pglz/short)={:?}",
            st.hand,
            st.ctor,
            st.ctor_declined,
            st.mint_failed,
            st.soft_mode,
            st.soft_captured,
            st.null_flags_arg,
            st.flags_len_off,
            st.daterange_built,
            st.toast_built
        );
    });
}

/// `arg_range` detoasts a non-flat range argument through the detoast seam.
/// Install the SHIPPED implementation: the seam is ENVIRONMENT, the detoast
/// logic is COMPUTATION and must never be mocked (Michael's minimal-seaming
/// rule). Short-header images built by this driver are expanded by it; the
/// external-TOAST fetch below it is never reached, since no arm mints a toast
/// pointer (see GAPS-p1-laneac.md for why that arm stays out of scope).
fn install_seams() {
    crate::install_detoast_seam_once();
}

pub fn rangetypes_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    // This harness's contract is "the typcache seam never fires" (fn_extra
    // memos everywhere). In the SHARED cargo-test binary another lane's
    // module (array_userfuncs_diff / rowtypes_diff) may own
    // lookup_pg_type_typcache_shape with a fixture that cannot resolve this
    // lane's pinned range oids, and residual lookups then produce false
    // "type does not exist" divergences. Same convention as rowtypes_diff:
    // whichever module owns the env wins; our drivers become no-ops there
    // (run `cargo test rangetypes_diff` for the full rail; fuzz binaries
    // are one-target-per-process and unaffected). RESIDUE: a composite
    // typcache fixture covering all lanes' pins would retire this skip.
    if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
        return;
    }
    install_seams();
    maybe_report_stats();
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&typb, payload)) = rest.split_first() else {
        return;
    };
    // NPINS_IO, not NPINS: the text/binary io arms need the ELEMENT's I/O
    // functions, and daterange's (date_in/date_out) are not vendored in this
    // oracle. The constructor arm opts daterange in for itself.
    let t = (typb % NPINS_IO as u8) as usize;
    let ctx = MemoryContext::new("rangetypes_fuzz");
    let mcx = ctx.mcx();

    match sel % 12 {
        0 => arm_text_io(t, payload, mcx, typb & 0x80 != 0),
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
        11 => arm_make_range(typb, payload, mcx),
        _ => unreachable!(),
    }
}

/// SOFT-ERROR (escontext) PLANE — the fourth comparison plane.
///
/// The surface behind `pg_input_is_valid()` and `COPY ... ON_ERROR ignore`: with
/// an ErrorSaveNode armed, an invalid literal is NOT thrown, it is CAPTURED, and
/// range_in returns normally. Neither driver exercised this at all before, so
/// every `Ok(None)` soft edge in io.rs/lib.rs was dead to the differential.
///
/// Compared here:
///   (a) soft-error OCCURRED flag — C's SOFT_ERROR_OCCURRED(escontext) vs the
///       Rust ErrorSaveNode's ctx.error_occurred();
///   (b) the captured sqlstate CLASS (same table as the thrown plane);
///   (c) the image bytes when the literal IS valid — soft mode must not perturb
///       a successful parse;
///   (d) VERDICT AGREEMENT between the two modes: a literal is valid in soft
///       mode iff it is valid in hard mode. This is the property that actually
///       matters to callers, and it cannot be checked from either mode alone.
///
/// NOT compared: fcinfo.isnull on the soft-failure edge. See the SOFT-ISNULL
/// note in fuzz/divergences/rangetypes_diff/FINDINGS.md — C reaches
/// PG_RETURN_NULL (isnull=true) on the parse and element-input edges but returns
/// a NULL RangeType pointer with isnull=false on the make_range edge, so C is
/// not self-consistent here either; every in-tree caller checks the context and
/// never reads the result, so the flag is not an observable surface. The
/// OCCURRED flag and the class ARE compared, which is what callers act on.
fn arm_text_in_soft(t: usize, payload: &[u8], cs: &CString, mcx: mcx::Mcx<'_>) {
    bump(|st| st.soft_mode += 1);

    // ---- C side: escontext armed
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let mut csoft = 0i32;
    let mut cisnull = 0i32;
    let cret = unsafe {
        pg_diff_range_in_soft(
            t as i32,
            cs.as_ptr(),
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
            &mut csoft,
            &mut cisnull,
        )
    };

    // ---- Rust side: the shipped fc wrapper with an ErrorSaveNode in context
    let mut esc = types_fmgr::ErrorSaveNode::new(true);
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_input);
    let mut fcinfo = LocalFcinfo::<3>::fresh(0);
    // SAFETY: mcx and the node both outlive this single call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.context = esc.fm_node_ptr();
    fcinfo.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
    fcinfo.set_arg(1, Datum::from_u32(PINS[t].rngtypid));
    fcinfo.set_arg(2, Datum::from_i32(-1));
    let rres = rb::fc_range_in(Some(&mut fl), &mut fcinfo);

    let dbg = format!("t={t} soft lit={:?}", String::from_utf8_lossy(payload));
    assert!(cret != C_BUFCAP, "range_in/soft: oracle buffer too small (harness bug) {dbg}");

    let r_occurred = esc.ctx.error_occurred();
    let c_occurred = csoft != 0;

    // A HARD error in soft mode is still possible (arms that do not take an
    // escontext); both sides must agree on that too.
    match &rres {
        Err(e) => {
            let rc = err_class(e);
            assert!(
                !c_occurred && cret == rc,
                "range_in/soft HARD-ERROR DIVERGENCE {dbg}: C=(ret {cret}, soft {csoft}) \
                 Rust=hard err {rc} ({})",
                e.message
            );
            return;
        }
        Ok(_) => assert!(
            cret == 0 || cret == C_ISNULL,
            "range_in/soft DIVERGENCE {dbg}: C hard err {cret} vs Rust Ok"
        ),
    }

    // (a) OCCURRED flag
    assert!(
        r_occurred == c_occurred,
        "range_in/soft OCCURRED DIVERGENCE {dbg}: C soft_class={csoft} (occurred={c_occurred}) \
         Rust occurred={r_occurred}"
    );

    if r_occurred {
        bump(|st| st.soft_captured += 1);
        // (b) captured sqlstate class
        let rc = esc.ctx.error().map(err_class).unwrap_or(98);
        assert!(
            rc == csoft,
            "range_in/soft CLASS DIVERGENCE {dbg}: C={csoft} Rust={rc}"
        );
    } else {
        // (c) a valid literal must produce the identical image under soft mode
        let d = rres.expect("checked Ok above");
        assert!(
            !fcinfo.isnull && cret == 0,
            "range_in/soft NULLNESS DIVERGENCE {dbg}: C ret {cret} vs Rust isnull={}",
            fcinfo.isnull
        );
        let rbytes = datum_varlena_bytes(d);
        assert!(
            rbytes == &cbuf[..clen as usize],
            "range_in/soft IMAGE DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
            &cbuf[..clen as usize],
            rbytes
        );
    }

    // (d) soft/hard verdict agreement, both sides independently.
    let mut hbuf = vec![0u8; OUTCAP];
    let mut hlen = 0i32;
    let hret = unsafe {
        pg_diff_range_in(t as i32, cs.as_ptr(), hbuf.as_mut_ptr(), &mut hlen, OUTCAP as i32)
    };
    let c_hard_bad = hret != 0 && hret != C_ISNULL;
    assert!(
        c_hard_bad == c_occurred,
        "range_in SOFT/HARD DISAGREEMENT (C) {dbg}: hard ret {hret} vs soft class {csoft}"
    );
    let mut fl2 = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_input);
    let rhard = fc_call(
        rb::fc_range_in,
        Some(&mut fl2),
        mcx,
        [
            Some(Datum::from_usize(cs.as_ptr() as usize)),
            Some(Datum::from_u32(PINS[t].rngtypid)),
            Some(Datum::from_i32(-1)),
        ],
    );
    assert!(
        rhard.result.is_err() == r_occurred,
        "range_in SOFT/HARD DISAGREEMENT (Rust) {dbg}: hard err={} vs soft occurred={r_occurred}",
        rhard.result.is_err()
    );
}

fn arm_text_io(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>, soft: bool) {
    if payload.len() > 256 || payload.contains(&0) {
        return;
    }
    let Ok(cs) = CString::new(payload) else { return };
    if soft {
        arm_text_in_soft(t, payload, &cs, mcx);
        // fall through: the SAME literal is then run in hard mode below, so the
        // soft/hard verdict-agreement plane compares two real executions rather
        // than a modelled one.
    }
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
    // The constructor arms need only bound DATUMS, so daterange rides here: a
    // 4th instantiation whose canonical function is daterange_canonical, which
    // is what reaches canonicalize()'s F_DATERANGE_CANONICAL dispatch.
    let t = if nullbits & 0x40 != 0 { NPINS - 1 } else { t };
    // FLAGS TEXT LENGTH is driven, not fixed at 2: range_parse_flags rejects on
    // length before it ever looks at the characters, and a fixed-2 driver can
    // only ever reach the character arms.
    let flags_len = match nullbits >> 4 & 0x3 {
        0 => 2,
        1 => 0,
        2 => 1,
        _ => 3,
    };
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let null1 = nullbits & 1 != 0;
    let null2 = nullbits & 2 != 0;
    // range_constructor3 is NON-STRICT (pg_proc), so a SQL-NULL flags argument
    // really reaches the body and both sides must raise 22000 there.
    let null3 = three && nullbits & 4 != 0;
    let flags_txt = [f1, f2, b')'];
    // INLINE-COMPRESSED bound (numrange only: the byval pins have no varlena
    // bound to compress). Reaches detoast_bound_packed's compressed arm
    // (lib.rs:415-419) AND numeric_cmp's full-detoast of a compressed argument,
    // because range_serialize compares the bounds before it detoasts them.
    let (lo, up) = if t == 2 && nullbits & 0x80 != 0 {
        let squeeze = |b: Bound| match b {
            Bound::Num(img) => match to_compressed(&img) {
                Some(c) => {
                    bump(|st| st.toast_built[1] += 1);
                    Bound::Num(c)
                }
                None => Bound::Num(img),
            },
            byval => byval,
        };
        (squeeze(lo), squeeze(up))
    } else {
        (lo, up)
    };
    let (v1, n1) = lo.c_args();
    let (v2, n2) = up.c_args();
    if t == NPINS - 1 {
        bump(|st| st.daterange_built += 1);
    }
    if null3 {
        bump(|st| st.null_flags_arg += 1);
    }
    if three && flags_len != 2 {
        bump(|st| st.flags_len_off += 1);
    }
    // ARMED-BUT-IGNORED escontext on the constructor. NOTE what this does and
    // does not do: BOTH implementations hardcode NULL for make_range's
    // escontext here (rangetypes.c range_constructor2/3 pass NULL; the shipped
    // fc_range_constructor2/3 pass None), so a soft error can never be captured
    // on this path and this arm does NOT reach canonicalize's soft edges — the
    // daterange one (lib.rs:535) still needs date_in vendored so daterange can
    // join the text-io arms. What it DOES pin is that neither side consults an
    // escontext the C function ignores: if pgrust ever started threading it
    // here, C would still throw hard while Rust captured softly and the
    // OCCURRED assert below would fire.
    let soft = nullbits & 0x20 != 0;
    let mut csoft = 0i32;
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
            flags_len as i32,
            null3 as i32,
            soft as i32,
            &mut csoft,
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
        )
    };
    let mut fl = ops_flinfo(t);
    let a0 = if null1 { None } else { Some(lo.rust_datum()) };
    let a1 = if null2 { None } else { Some(up.rust_datum()) };
    let r = if three {
        let n = flags_len as usize;
        let mut tv = vec![0u8; 4 + n];
        tv[0..4].copy_from_slice(&datum::set_varsize_4b(4 + n));
        tv[4..4 + n].copy_from_slice(&flags_txt[..n]);
        let a2 = if null3 { None } else { Some(Datum::from_usize(tv.as_ptr() as usize)) };
        fc_call(rb::fc_range_constructor3, Some(&mut fl), mcx, [a0, a1, a2])
    } else {
        fc_call(rb::fc_range_constructor2, Some(&mut fl), mcx, [a0, a1])
    };
    let dbg = format!(
        "t={t} three={three} nulls={nullbits:x} flags={:?} flags_len={flags_len} \
         null3={null3} soft={soft}",
        &flags_txt[..flags_len as usize]
    );
    if !soft {
        compare_range_result("range_ctor", cret, &cbuf[..clen as usize], &r, &dbg);
        return;
    }
    // Soft-mode constructor: run the shipped wrapper with an armed
    // ErrorSaveNode and compare the soft planes.
    bump(|st| st.soft_mode += 1);
    let mut esc = types_fmgr::ErrorSaveNode::new(true);
    let mut fl2 = ops_flinfo(t);
    let sres = if three {
        let n = flags_len as usize;
        let mut tv = vec![0u8; 4 + n];
        tv[0..4].copy_from_slice(&datum::set_varsize_4b(4 + n));
        tv[4..4 + n].copy_from_slice(&flags_txt[..n]);
        let mut fcinfo = LocalFcinfo::<3>::fresh(0);
        // SAFETY: mcx and the node both outlive this call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        fcinfo.context = esc.fm_node_ptr();
        if null1 { fcinfo.set_arg_null(0) } else { fcinfo.set_arg(0, lo.rust_datum()) }
        if null2 { fcinfo.set_arg_null(1) } else { fcinfo.set_arg(1, up.rust_datum()) }
        if null3 {
            fcinfo.set_arg_null(2)
        } else {
            fcinfo.set_arg(2, Datum::from_usize(tv.as_ptr() as usize))
        }
        rb::fc_range_constructor3(Some(&mut fl2), &mut fcinfo)
    } else {
        let mut fcinfo = LocalFcinfo::<2>::fresh(0);
        // SAFETY: mcx and the node both outlive this call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        fcinfo.context = esc.fm_node_ptr();
        if null1 { fcinfo.set_arg_null(0) } else { fcinfo.set_arg(0, lo.rust_datum()) }
        if null2 { fcinfo.set_arg_null(1) } else { fcinfo.set_arg(1, up.rust_datum()) }
        rb::fc_range_constructor2(Some(&mut fl2), &mut fcinfo)
    };
    match &sres {
        Err(e) => {
            let rc = err_class(e);
            assert!(
                csoft == 0 && cret == rc,
                "range_ctor/soft HARD-ERROR DIVERGENCE {dbg}: C=(ret {cret}, soft {csoft}) \
                 Rust=hard {rc} ({})",
                e.message
            );
        }
        Ok(d) => {
            let r_occurred = esc.ctx.error_occurred();
            assert!(
                r_occurred == (csoft != 0),
                "range_ctor/soft OCCURRED DIVERGENCE {dbg}: C={csoft} Rust={r_occurred}"
            );
            if r_occurred {
                bump(|st| st.soft_captured += 1);
                let rc = esc.ctx.error().map(err_class).unwrap_or(98);
                assert!(rc == csoft, "range_ctor/soft CLASS DIVERGENCE {dbg}: C={csoft} Rust={rc}");
            } else if cret == 0 && d.as_usize() != 0 {
                let rbytes = datum_varlena_bytes(*d);
                assert!(
                    rbytes == &cbuf[..clen as usize],
                    "range_ctor/soft IMAGE DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
                    &cbuf[..clen as usize],
                    rbytes
                );
            }
        }
    }
}

fn arm_accessors(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let sel = rd.u8();
    let (flags, layout_ctor) = (sel, sel & 0x80 != 0);
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let Some(img4) = image_for(t, layout_ctor, flags, &lo, &up, mcx) else { return };
    // A stored small range carries a SHORT (1-byte) varlena header; feed that
    // form on a payload bit so the detoast-on-argument path is compared.
    let short = sel & 0x40 != 0;
    let img = match if short { to_short_header(&img4) } else { None } {
        Some(s) => {
            bump(|st| st.toast_built[2] += 1);
            s
        }
        None => img4,
    };
    let mut lob = vec![0u8; OUTCAP];
    let mut upb = vec![0u8; OUTCAP];
    let (mut lol, mut lon, mut upl, mut upn) = (0i32, 0i32, 0i32, 0i32);
    let mut bools = [0u8; 5];
    let mut aerrs = [0i32; 7];
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
            aerrs.as_mut_ptr(),
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
    for (idx, (which, fc, cnull, cbytes, clen)) in acc.into_iter().enumerate() {
        let mut fl = ops_flinfo(t);
        let r = fc_call(fc, Some(&mut fl), mcx, [Some(Datum::from_usize(img.as_ptr() as usize))]);
        let d = match r.result {
            Ok(d) => {
                assert!(
                    aerrs[idx] == 0,
                    "range_{which} DIVERGENCE {dbg}: C err {} vs Rust Ok",
                    aerrs[idx]
                );
                d
            }
            Err(e) => {
                assert!(
                    aerrs[idx] == err_class(&e),
                    "range_{which} DIVERGENCE {dbg}: C err {} vs Rust {} ({})",
                    aerrs[idx],
                    err_class(&e),
                    e.message
                );
                continue;
            }
        };
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
        let d = match r.result {
            Ok(d) => {
                assert!(
                    aerrs[2 + i] == 0,
                    "bool accessor {i} DIVERGENCE {dbg}: C err {} vs Rust Ok",
                    aerrs[2 + i]
                );
                d
            }
            Err(e) => {
                assert!(
                    aerrs[2 + i] == err_class(&e),
                    "bool accessor {i} DIVERGENCE {dbg}: C err {} vs Rust {}",
                    aerrs[2 + i],
                    err_class(&e)
                );
                continue;
            }
        };
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
    let layout_ctor = flags1 & 0x80 != 0;
    let lo1 = Bound::decode(t, &mut rd, mcx);
    let up1 = Bound::decode(t, &mut rd, mcx);
    let lo2 = Bound::decode(t, &mut rd, mcx);
    let up2 = Bound::decode(t, &mut rd, mcx);
    let (Some(lo1), Some(up1), Some(lo2), Some(up2)) = (lo1, up1, lo2, up2) else {
        return;
    };
    let Some(img1) = image_for(t, layout_ctor, flags1, &lo1, &up1, mcx) else { return };
    let Some(img2) = image_for(t, layout_ctor, flags2, &lo2, &up2, mcx) else { return };
    let mut cres = [0i32; 15];
    let mut cerrs = [0i32; 15];
    // PER-OPERATOR errcodes: range_adjacent can legitimately raise 22003 via
    // int4range_canonical at INT32_MAX while its 14 siblings succeed on the
    // same pair, so one shared code cannot express the outcome.
    let cret =
        unsafe { pg_diff_range_ops(img1.as_ptr(), img2.as_ptr(), cres.as_mut_ptr(), cerrs.as_mut_ptr()) };
    assert!(cret == 0, "range_ops: oracle entry failed ({cret})");
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
                assert!(
                    cerrs[i] == 0,
                    "range_{name} DIVERGENCE {dbg}: C err {} vs Rust Ok",
                    cerrs[i]
                );
                let rv = if isbool { (d.as_usize() != 0) as i32 } else { d.as_i32() };
                assert!(rv == cres[i], "range_{name} DIVERGENCE {dbg}: C={} Rust={rv}", cres[i]);
            }
            Err(e) => {
                assert!(
                    cerrs[i] == err_class(&e),
                    "range_{name} DIVERGENCE {dbg}: C err {} vs Rust {} ({})",
                    cerrs[i],
                    err_class(&e),
                    e.message
                );
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
    let Some(img) = image_for(t, flags & 0x80 != 0, flags, &lo, &up, mcx) else { return };
    let (ev, en) = el.c_args();
    let (mut c1, mut c2) = (0i32, 0i32);
    let (mut e1, mut e2) = (0i32, 0i32);
    let cret = unsafe {
        pg_diff_range_contains_elem(img.as_ptr(), ev, en, &mut c1, &mut c2, &mut e1, &mut e2)
    };
    assert!(cret == 0, "contains_elem: oracle entry failed ({cret})");
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
            e1 == 0 && (d.as_usize() != 0) as i32 == c1,
            "contains_elem DIVERGENCE {dbg}: C=({e1},{c1}) Rust={}",
            d.as_usize()
        ),
        Err(e) => assert!(
            e1 == err_class(&e),
            "contains_elem DIVERGENCE {dbg}: C err {e1} vs {}",
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
            e2 == 0 && (d.as_usize() != 0) as i32 == c2,
            "elem_contained_by DIVERGENCE {dbg}: C=({e2},{c2}) Rust={}",
            d.as_usize()
        ),
        Err(e) => assert!(
            e2 == err_class(&e),
            "elem_contained_by DIVERGENCE {dbg}: C err {e2} vs {}",
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
    let layout_ctor = flags1 & 0x80 != 0;
    let Some(img1) = image_for(t, layout_ctor, flags1, &lo1, &up1, mcx) else { return };
    let Some(img2) = image_for(t, layout_ctor, flags2, &lo2, &up2, mcx) else { return };
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
    let raw = rd.u8();
    let flags = raw & 0x7f;
    // the layout bit cannot ride 0x80 here (masked above), so take its own byte
    let layout_ctor = rd.u8() & 1 != 0;
    let seed = rd.i64() as u64;
    let lo = Bound::decode(t, &mut rd, mcx);
    let up = Bound::decode(t, &mut rd, mcx);
    let (Some(lo), Some(up)) = (lo, up) else { return };
    let Some(img) = image_for(t, layout_ctor, flags, &lo, &up, mcx) else { return };
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

    // SOFT-ERROR mode on the same image. Not a harness invention: make_range
    // builds a frame by hand specifically to pass escontext into
    // rng_canonical_finfo, so the canonical body really does run with a soft
    // context. This is the only way to reach the canonical wrapper's own
    // Ok(None) soft edges (builtins.rs:467/472) and, for daterange, the
    // F_DATERANGE_CANONICAL soft edge in canonicalize (lib.rs:535).
    if rd.u8() & 1 == 1 {
        bump(|st| st.soft_mode += 1);
        let mut sbuf = vec![0u8; OUTCAP];
        let mut slen = 0i32;
        let mut csoft = 0i32;
        let mut cisnull = 0i32;
        let sret = unsafe {
            pg_diff_range_canonical_soft(
                if ct == 2 { 3 } else { ct as i32 },
                img.as_ptr(),
                sbuf.as_mut_ptr(),
                &mut slen,
                OUTCAP as i32,
                &mut csoft,
                &mut cisnull,
            )
        };
        let mut esc = types_fmgr::ErrorSaveNode::new(true);
        let mut fcinfo = LocalFcinfo::<1>::fresh(0);
        // SAFETY: mcx and the node both outlive this single call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        fcinfo.context = esc.fm_node_ptr();
        fcinfo.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
        let sres = fc(Some(&mut fl), &mut fcinfo);
        let sdbg = format!("{dbg} soft");
        match &sres {
            Err(e) => {
                let rc = err_class(e);
                assert!(
                    csoft == 0 && sret == rc,
                    "range_canonical/soft HARD-ERROR DIVERGENCE {sdbg}: \
                     C=(ret {sret}, soft {csoft}) Rust=hard {rc} ({})",
                    e.message
                );
            }
            Ok(d) => {
                let r_occurred = esc.ctx.error_occurred();
                assert!(
                    r_occurred == (csoft != 0),
                    "range_canonical/soft OCCURRED DIVERGENCE {sdbg}: C={csoft} Rust={r_occurred}"
                );
                if r_occurred {
                    bump(|st| st.soft_captured += 1);
                    let rc = esc.ctx.error().map(err_class).unwrap_or(98);
                    assert!(
                        rc == csoft,
                        "range_canonical/soft CLASS DIVERGENCE {sdbg}: C={csoft} Rust={rc}"
                    );
                } else {
                    assert!(sret == 0, "range_canonical/soft {sdbg}: C err {sret} vs Rust Ok");
                    let rb = datum_varlena_bytes(*d);
                    assert!(
                        rb == &sbuf[..slen as usize],
                        "range_canonical/soft IMAGE DIVERGENCE {sdbg}: C={:02x?} Rust={:02x?}",
                        &sbuf[..slen as usize],
                        rb
                    );
                }
            }
        }
    }
}

/// INTERNAL-API arm: make_range directly, hard AND soft, over the three
/// discrete byval-bound instantiations (int4range / int8range / daterange).
/// make_range is the exact function real range_in calls, so this models a real
/// caller shape rather than inventing one; it is the ONLY route to
/// canonicalize's per-type dispatch under a soft context, because the fmgr
/// constructors hardcode a NULL escontext in verbatim C and shipped Rust alike,
/// and range_in cannot carry daterange here (date_in is not vendored). In
/// particular this reaches canonicalize's F_DATERANGE_CANONICAL soft edge
/// (lib.rs:535), whose int4/int8 siblings ride soft range_in.
fn arm_make_range(typb: u8, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let ct = (typb % 3) as usize; // 0=int4range, 1=int8range, 2=daterange
    let mut rd = Rd(payload, 0);
    let flags = wf_flags(rd.u8()) & 0x1f; // EMPTY|LB_INC|UB_INC|LB_INF|UB_INF
    let soft = rd.u8() & 1 == 1;
    let (v1, v2) = if ct == 1 {
        (rd.i64(), rd.i64())
    } else {
        (rd.i32() as i64, rd.i32() as i64)
    };
    let (ctyp, t) = match ct {
        0 => (0, 0),
        1 => (1, 1),
        _ => (3, NPINS - 1),
    };
    if soft {
        bump(|st| st.soft_mode += 1);
    }
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let mut csoft = 0i32;
    let cret = unsafe {
        pg_diff_make_range(
            ctyp,
            v1,
            v2,
            flags as i32,
            soft as i32,
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
            &mut csoft,
        )
    };
    let mut ri = range_info(t);
    let mk_bound = |v: i64, lower: bool| rt::RangeBound {
        val: if ct == 1 { Datum::from_i64(v) } else { Datum::from_i32(v as i32) },
        infinite: flags & (if lower { rt::RANGE_LB_INF } else { rt::RANGE_UB_INF }) != 0,
        inclusive: flags & (if lower { rt::RANGE_LB_INC } else { rt::RANGE_UB_INC }) != 0,
        lower,
    };
    let mut lower = mk_bound(v1, true);
    let mut upper = mk_bound(v2, false);
    let mut esc = types_error::SoftErrorContext::new(true);
    let ctx = if soft { Some(&mut esc) } else { None };
    let rres = rt::make_range(mcx, &mut ri, &mut lower, &mut upper, flags & 0x01 != 0, ctx);
    let dbg = format!("ct={ct} flags={flags:02x} soft={soft} v1={v1} v2={v2}");
    match &rres {
        Err(e) => {
            let rc = err_class(e);
            assert!(
                csoft == 0 && cret == rc,
                "make_range HARD-ERROR DIVERGENCE {dbg}: C=(ret {cret}, soft {csoft}) \
                 Rust=hard {rc} ({})",
                e.message
            );
        }
        Ok(None) => {
            // soft failure captured on the Rust side
            assert!(soft, "make_range returned Ok(None) without a soft context {dbg}");
            bump(|st| st.soft_captured += 1);
            assert!(
                csoft != 0,
                "make_range OCCURRED DIVERGENCE {dbg}: Rust captured, C did not"
            );
            let rc = esc.error().map(err_class).unwrap_or(98);
            assert!(rc == csoft, "make_range CLASS DIVERGENCE {dbg}: C={csoft} Rust={rc}");
        }
        Ok(Some(img)) => {
            assert!(
                csoft == 0 && cret == 0,
                "make_range DIVERGENCE {dbg}: C=(ret {cret}, soft {csoft}) vs Rust Ok"
            );
            assert!(
                !esc.error_occurred(),
                "make_range {dbg}: Rust succeeded but marked its soft context"
            );
            assert!(
                img[..] == cbuf[..clen as usize],
                "make_range IMAGE DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
                &cbuf[..clen as usize],
                &img[..]
            );
        }
    }
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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


#[cfg(test)]
mod vacuity {
    use super::*;

    /// ANTI-VACUITY GATE for the arms added when the coverage gaps were closed.
    /// Each one is a NEW path; a new arm that silently never fires is worse than
    /// a known gap, because the campaign then reports coverage it does not have.
    /// Every counter here must ADVANCE over a fixed input set.
    #[test]
    fn gap_closing_arms_all_fire() {
        let _serial = crate::c_oracle_serial();
        if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
            // Foreign module owns the typcache env: drivers no-op (see the
            // entry guard), so arm-vacuity cannot be asserted in the shared
            // binary. Run `cargo test <module>` filtered for the real rail.
            return;
        }
        let before = STATS.with(|s| {
            let st = s.borrow();
            (
                st.soft_mode,
                st.soft_captured,
                st.null_flags_arg,
                st.flags_len_off,
                st.daterange_built,
                st.toast_built[2],
                st.toast_built[1],
            )
        });

        // soft-mode range_in: valid, and each malformed class
        for lit in [
            &b"[1,10)"[..],
            &b"garbage"[..],
            &b"[abc,2)"[..],
            &b"[5,1)"[..],
            &b"[1,2147483647]"[..],
        ] {
            let mut v = vec![0u8, 0x81];
            v.extend_from_slice(lit);
            rangetypes_diff(&v);
        }
        // constructor3: NULL flags arg (0x04), off-length flags (0x10/0x20),
        // daterange instantiation (0x40), soft (0x20 shares with flags_len —
        // driven separately below)
        for nb in [0x04u8, 0x14, 0x24, 0x34, 0x40, 0x44] {
            rangetypes_diff(&[3, 0, nb, b'[', b']', 1, 0, 0, 0, 9, 0, 0, 0]);
        }
        // accessors with a SHORT-header outer image (sel bit 0x40)
        for sel in [0x40u8, 0x46, 0x4a] {
            rangetypes_diff(&[4, 0, sel, 1, 0, 0, 0, 9, 0, 0, 0]);
        }
        // canonical arm in soft mode (trailing byte odd)
        rangetypes_diff(&[9, 0, 0x06, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0x7f, 1]);
        // compressed numrange bound (ctor arm, nullbits 0x80, t=2).
        // Bound::decode length byte is (b % 20) + 1, so 17 => an 18-byte
        // literal. REPETITIVE digits on purpose: pglz declines when it cannot
        // shrink the input (the mint falls back to the uncompressed bound),
        // and a short random literal never compresses — the first version of
        // this test used "1234" and this very gate caught the arm not firing.
        let mut v = vec![2u8, 2, 0x80, b'[', b')'];
        v.push(17);
        v.extend_from_slice(b"111111111111111111");
        v.push(17);
        v.extend_from_slice(b"999999999999999999");
        rangetypes_diff(&v);
        // make_range internal-API arm, soft, daterange overflow
        let mut v = vec![11u8, 2, 0x06, 1];
        v.extend_from_slice(&0i32.to_le_bytes());
        v.extend_from_slice(&2145031948i32.to_le_bytes());
        rangetypes_diff(&v);

        let after = STATS.with(|s| {
            let st = s.borrow();
            (
                st.soft_mode,
                st.soft_captured,
                st.null_flags_arg,
                st.flags_len_off,
                st.daterange_built,
                st.toast_built[2],
                st.toast_built[1],
            )
        });
        assert!(after.0 > before.0, "soft-mode arm never fired");
        assert!(after.1 > before.1, "soft-mode arm never CAPTURED a soft error");
        assert!(after.2 > before.2, "NULL-flags-argument arm never fired");
        assert!(after.3 > before.3, "off-length flags-text arm never fired");
        assert!(after.4 > before.4, "daterange instantiation never built");
        assert!(after.5 > before.5, "short-header image arm never fired");
        assert!(after.6 > before.6, "compressed (pglz) bound arm never fired");
    }

    /// ANTI-VACUITY GATE. A clean fuzz run proves nothing if the builders quietly
    /// declined to construct anything, so this drives both layouts across all
    /// three instantiations and asserts every cell was actually built and
    /// compared. If a future change makes numrange images stop being produced,
    /// this fails instead of the campaign going vacuously green.
    #[test]
    fn both_layouts_reach_every_instantiation() {
        let _serial = crate::c_oracle_serial();
        if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
            // Foreign module owns the typcache env: drivers no-op (see the
            // entry guard), so arm-vacuity cannot be asserted in the shared
            // binary. Run `cargo test <module>` filtered for the real rail.
            return;
        }
        STATS.with(|s| *s.borrow_mut() = BuildStats::default());
        // ordered bound payloads per instantiation (lower <= upper, so the
        // constructor accepts them; a swapped pair legitimately declines)
        fn bounds(t: u8) -> Vec<u8> {
            match t {
                0 => {
                    let mut v = Vec::new();
                    for x in [1i32, 9, 2, 8, 5] {
                        v.extend_from_slice(&x.to_le_bytes());
                    }
                    v
                }
                1 => {
                    let mut v = Vec::new();
                    for x in [1i64, 9, 2, 8, 5] {
                        v.extend_from_slice(&x.to_le_bytes());
                    }
                    v
                }
                _ => b"\x031.5 \x032.5 \x031.0 \x032.0 \x031.2 ".to_vec(),
            }
        }
        for t in 0..3u8 {
            for layout in [0x00u8, 0x80u8] {
                let b = bounds(t);
                // arms 4/5/6/7 read [flags1][flags2?] then bounds
                for sel in [5u8, 7, 6, 4] {
                    let mut v = vec![sel, t, 0x03 | layout, 0x03 | layout];
                    v.extend_from_slice(&b);
                    rangetypes_diff(&v);
                }
                // hash: [flags][layout byte][8-byte seed][bounds]
                let mut v = vec![8u8, t, 0x03, layout >> 7];
                v.extend_from_slice(&[0u8; 8]);
                v.extend_from_slice(&b);
                rangetypes_diff(&v);
            }
        }
        STATS.with(|s| {
            let st = s.borrow();
            for t in 0..3 {
                assert!(st.hand[t] > 0, "no HAND-built images compared at t={t}: vacuous");
                assert!(st.ctor[t] > 0, "no CTOR-built images compared at t={t}: vacuous");
            }
            eprintln!(
                "hand={:?} ctor={:?} ctor_declined={} mint_failed={}",
                st.hand, st.ctor, st.ctor_declined, st.mint_failed
            );
        });
    }

    /// The two layouts must genuinely DIFFER for a byref element (that is the
    /// whole point: the constructor path packs short headers, the hand builder
    /// does not). Guards against the ctor path silently degrading to the hand one.
    #[test]
    fn numrange_layouts_differ() {
        let _serial = crate::c_oracle_serial();
        if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
            // Foreign module owns the typcache env: drivers no-op (see the
            // entry guard), so arm-vacuity cannot be asserted in the shared
            // binary. Run `cargo test <module>` filtered for the real rail.
            return;
        }
        let ctx = MemoryContext::new("layouts");
        let mcx = ctx.mcx();
        let lo = Bound::Num(mint_numeric(mcx, b"1.5").unwrap());
        let up = Bound::Num(mint_numeric(mcx, b"2.5").unwrap());
        let hand = build_image(2, rt::RANGE_LB_INC, &lo, &up);
        let ctor = build_image_ctor(2, [b'[', b')'], &lo, &up, false, false, mcx).unwrap();
        assert!(
            hand != ctor,
            "numrange hand and ctor layouts are identical — the packed-short \
             path is no longer being covered"
        );
        // the constructor image is the SHORT-header one: strictly smaller
        assert!(
            ctor.len() < hand.len(),
            "ctor image {} bytes vs hand {} bytes: expected packed-short to be smaller",
            ctor.len(),
            hand.len()
        );
    }
}

#[cfg(test)]
mod packed_short {
    use super::*;

    /// PACKED-SHORT ACCESSOR/OUT PATHS. Constructor-built numrange images store
    /// bounds with 1-byte short headers; `range_lower`/`range_upper` hand back a
    /// pointer INTO the image, i.e. a packed-short datum that the shipped code
    /// returns as-is. Both sides must copy it out by VARSIZE_ANY (short size),
    /// not expand or truncate it — and range_out/send must render it the same.
    #[test]
    fn short_header_bounds_round_trip() {
        let _serial = crate::c_oracle_serial();
        let ctx = MemoryContext::new("short");
        let mcx = ctx.mcx();
        let lo = Bound::Num(mint_numeric(mcx, b"1.5").unwrap());
        let up = Bound::Num(mint_numeric(mcx, b"2.5").unwrap());
        let img = build_image_ctor(2, [b'[', b')'], &lo, &up, false, false, mcx).unwrap();
        // the bound really is short-header (low bit set on its first byte)
        let first_bound_byte = img[8];
        assert!(
            first_bound_byte & 0x01 == 0x01,
            "expected a packed-short bound header, got {first_bound_byte:#04x}"
        );
        // drive the accessor + out + hash + ops arms over it; each compares
        // C against the shipped wrappers internally and panics on any skew
        for sel in [4u8, 5, 6, 7, 8] {
            let mut v = vec![sel, 2u8];
            v.push(0x83); // ctor layout, '[' ')'
            if sel == 8 {
                v.push(1);
                v.extend_from_slice(&[0u8; 8]);
            } else {
                v.push(0x83);
            }
            v.extend_from_slice(b"\x031.5 \x032.5 \x031.0 \x032.0 \x031.2 ");
            rangetypes_diff(&v);
        }
        // and the text/binary io arms over a literal that stores short bounds
        rangetypes_diff(&[0u8, 2].iter().copied().chain(b"[1.5,2.5)".iter().copied()).collect::<Vec<_>>());
    }
}

#[cfg(test)]
mod carriers {
    use super::*;

    /// RNG_RETTYPE is indexed by the SAME `t` as PINS, so a reordering of either
    /// array would silently hand fc_range_constructor2/3 the wrong result type —
    /// the constructor would then build (and both sides would agree on) images of
    /// the wrong range type, which no value/verdict/sqlstate plane can detect.
    /// Pin the correspondence.
    #[test]
    fn rettype_carriers_match_pins() {
        let _serial = crate::c_oracle_serial();
        for t in 0..3 {
            assert_eq!(
                RNG_RETTYPE[t].rettype, PINS[t].rngtypid,
                "RNG_RETTYPE[{t}] carries {:#x} but PINS[{t}] is {:#x}",
                RNG_RETTYPE[t].rettype, PINS[t].rngtypid
            );
            assert!(RNG_RETTYPE[t].argtypes.is_empty());
        }
    }

    /// No allocation escapes per call: the rettype carriers are statics, so
    /// ops_flinfo must not allocate for fn_expr at all. Guards the regression
    /// the fleet caught (24 bytes leaked per ops_flinfo call).
    #[test]
    fn ops_flinfo_carrier_is_static() {
        let _serial = crate::c_oracle_serial();
        let a = ops_flinfo(0);
        let b = ops_flinfo(0);
        let pa = a.fn_expr.as_ref().unwrap().downcast_ref::<AggFnArgTypes>().unwrap()
            as *const AggFnArgTypes;
        let pb = b.fn_expr.as_ref().unwrap().downcast_ref::<AggFnArgTypes>().unwrap()
            as *const AggFnArgTypes;
        assert_eq!(pa, pb, "each ops_flinfo() minted a FRESH carrier: still allocating");
        assert_eq!(pa, &RNG_RETTYPE[0] as *const AggFnArgTypes);
    }
}

#[cfg(test)]
mod shared_errclass_table {
    use types_error as te;

    /// The range and multirange oracles are ONE translation unit
    /// (pg_multirangetypes_io.c #includes pg_rangetypes_io.c), so their
    /// PG_DIFF_ERR_* class numbers form one table — and each Rust driver's
    /// err_class() must mirror it identically. The two halves of this lane
    /// were built in parallel and both minted a class 12 with DIFFERENT
    /// meanings (PROGRAM_LIMIT_EXCEEDED vs CARDINALITY_VIOLATION); the merge
    /// surfaced it only because C rejects a macro redefinition. Had the
    /// numbering drifted without a redefinition, every error-plane assert
    /// would have compared incomparable integers and passed vacuously.
    ///
    /// This test is the durable guard: any sqlstate BOTH drivers classify
    /// must get the same number from both.
    #[test]
    fn cross_target_err_class_agreement() {
        let _serial = crate::c_oracle_serial();
        let shared = [
            ("NUMERIC_VALUE_OUT_OF_RANGE", te::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
            ("INVALID_TEXT_REPRESENTATION", te::ERRCODE_INVALID_TEXT_REPRESENTATION),
            ("PROTOCOL_VIOLATION", te::ERRCODE_PROTOCOL_VIOLATION),
            ("DATA_EXCEPTION", te::ERRCODE_DATA_EXCEPTION),
            ("UNDEFINED_FUNCTION", te::ERRCODE_UNDEFINED_FUNCTION),
            ("DATETIME_VALUE_OUT_OF_RANGE", te::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
            ("INVALID_BINARY_REPRESENTATION", te::ERRCODE_INVALID_BINARY_REPRESENTATION),
            ("FEATURE_NOT_SUPPORTED", te::ERRCODE_FEATURE_NOT_SUPPORTED),
            ("SYNTAX_ERROR", te::ERRCODE_SYNTAX_ERROR),
            ("DATATYPE_MISMATCH", te::ERRCODE_DATATYPE_MISMATCH),
            ("INVALID_PARAMETER_VALUE", te::ERRCODE_INVALID_PARAMETER_VALUE),
            ("PROGRAM_LIMIT_EXCEEDED", te::ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            ("DATA_CORRUPTED", te::ERRCODE_DATA_CORRUPTED),
        ];
        for (name, sqlstate) in shared {
            let e = te::PgError::error("x").with_sqlstate(sqlstate);
            let rt = super::err_class(&e);
            let mr = crate::multirangetypes_diff::err_class(&e);
            assert_eq!(
                rt, mr,
                "{name}: rangetypes_diff class {rt} != multirangetypes_diff class {mr} \
                 — the shared oracle errcode table has drifted"
            );
            assert_ne!(rt, 98, "{name} must be classified, not fall through to 98");
        }
    }
}

#[cfg(test)]
mod soft_isnull_contract {
    use super::*;

    /// PINS the soft-mode `fcinfo.isnull` deviation documented in
    /// fuzz/divergences/rangetypes_diff/FINDINGS.md (SOFT-ISNULL). It is
    /// deliberately EXCLUDED from the fuzz comparator, so it is pinned here
    /// instead of going unwatched: if either side changes, this test fails and
    /// the finding must be revisited rather than silently drifting.
    ///
    /// C range_in reaches `PG_RETURN_NULL()` (isnull=true) on the range_parse
    /// and InputFunctionCallSafe soft edges, but returns a NULL RangeType
    /// POINTER with isnull=false on the make_range soft edge — C is not
    /// self-consistent. pgrust is uniformly isnull=false. Not observable: every
    /// caller (InputFunctionCallSafe itself, pg_input_is_valid,
    /// pg_input_error_info, COPY ON_ERROR ignore) tests SOFT_ERROR_OCCURRED
    /// BEFORE looking at the result and never reads isnull — verified in the
    /// vendored C and ground-truthed on postgres:18.3.
    ///
    /// The OCCURRED flag and the sqlstate class, which callers DO act on, are
    /// compared at full strength by arm_text_in_soft and asserted here too.
    #[test]
    fn soft_isnull_is_the_only_deviation() {
        let _serial = crate::c_oracle_serial();
        // (literal, expect_soft, c_class, c_isnull_on_soft_edge)
        let cases = [
            ("garbage", true, 2, 1),   // range_parse edge      -> C isnull=1
            ("[abc,2)", true, 2, 1),   // element-input edge    -> C isnull=1
            ("[5,1)", true, 4, 0),     // make_range edge       -> C isnull=0
            ("[1,2)", false, 0, 0),    // valid
            ("empty", false, 0, 0),    // valid
        ];
        for (lit, want_soft, want_class, want_c_isnull) in cases {
            let cs = std::ffi::CString::new(lit).unwrap();
            let mut cbuf = vec![0u8; OUTCAP];
            let mut clen = 0i32;
            let mut csoft = 0i32;
            let mut cisnull = 0i32;
            let cret = unsafe {
                pg_diff_range_in_soft(
                    0,
                    cs.as_ptr(),
                    cbuf.as_mut_ptr(),
                    &mut clen,
                    OUTCAP as i32,
                    &mut csoft,
                    &mut cisnull,
                )
            };
            assert_eq!(cret, 0, "{lit}: soft mode must not raise a hard error");
            assert_eq!(csoft, want_class, "{lit}: C soft class");
            assert_eq!(cisnull, want_c_isnull, "{lit}: C isnull on the soft edge");

            let ctx = MemoryContext::new("soft-contract");
            let mcx = ctx.mcx();
            let mut esc = types_fmgr::ErrorSaveNode::new(true);
            let mut fl = io_flinfo(0, lsyscache::IOFuncSelector::IOFunc_input);
            let mut fcinfo = LocalFcinfo::<3>::fresh(0);
            // SAFETY: mcx and the node outlive this call.
            unsafe { fcinfo.set_result_mcx(mcx) };
            fcinfo.context = esc.fm_node_ptr();
            fcinfo.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
            fcinfo.set_arg(1, Datum::from_u32(PINS[0].rngtypid));
            fcinfo.set_arg(2, Datum::from_i32(-1));
            let r = rb::fc_range_in(Some(&mut fl), &mut fcinfo);
            assert!(r.is_ok(), "{lit}: soft mode must not throw on the Rust side");

            // COMPARED planes agree.
            assert_eq!(esc.ctx.error_occurred(), want_soft, "{lit}: occurred flag");
            assert_eq!(csoft != 0, esc.ctx.error_occurred(), "{lit}: occurred parity");
            if want_soft {
                let rc = esc.ctx.error().map(err_class).unwrap_or(98);
                assert_eq!(rc, csoft, "{lit}: captured sqlstate class parity");
            }
            // THE DEVIATION, pinned: pgrust is uniformly isnull=false.
            assert!(!fcinfo.isnull, "{lit}: pgrust soft-mode isnull is always false");
        }
    }
}
