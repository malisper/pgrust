//! multirangetypes_diff: differential fuzz driver — shipped Rust
//! `adt_multirangetypes` vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha
//! 62d6c7d3df) C (csrc/pg_multirangetypes_io.c, which is ONE translation unit
//! with the range oracle — see that file's header). Crate under test:
//! crates/backend/utils/adt/multirangetypes.
//!
//! ===================== TYPCACHE MOCK (sanctioned) =====================
//! The crate's only typcache dependency is the MultirangeInfo /
//! MultirangeIOData it memoizes in flinfo.fn_extra. Both sides pin the SAME
//! three concrete instantiations (typcache lookup internals are the campaign
//! carve):
//!   type tag 0: int4multirange (4451, align 'i') over int4range (3904)
//!   type tag 1: int8multirange (4536, align 'd') over int8range (3926)
//!   type tag 2: nummultirange  (4532, align 'i') over numrange  (3906)
//! oids from pg_type.dat @ the pinned sha. The Rust side pre-seeds
//! flinfo.fn_extra with a hand-built MultirangeInfo / MultirangeIOData
//! mirroring the C oracle's static TypeCacheEntry values, so
//! `MultirangeInfo::lookup` (the typcache seam) never fires. The nested range
//! and element I/O finfos are pre-seeded the same way, so neither does the
//! range crate's own seam.
//!
//! ===================== THE NORMALIZATION KERNEL =======================
//! make_multirange / multirange_canonicalize is why this crate exists: sort
//! the input ranges, drop empties, merge adjacent and overlapping neighbours.
//! EVERY arm that needs a multirange value builds it through `agreed_image`,
//! which runs BOTH sides' multirange_constructor2 over the same variadic array
//! of deliberately DENORMALIZED ranges (unsorted, overlapping, nested,
//! duplicated, empty-containing) and asserts the canonicalized image bytes are
//! identical. So the kernel is differentially tested on every iteration and
//! every downstream arm consumes a real make_multirange image rather than a
//! hand-forged one.
//!
//! NUMERIC REPRESENTATION TIE (fleet-found; FINDINGS D1). The fence below was
//! written for the arms whose ranges the driver BUILDS. It does NOT cover the
//! text/binary io arms, whose bounds come from user literal/wire bytes: those
//! can carry value-equal-but-byte-different numerics (`2` vs `2.0000`), and
//! multirange canonicalization then picks different representatives across C's
//! unstable qsort and pgrust's stable sort. `compare_mr_image` handles that
//! precisely — byte-exact by default, a gated value-level fallback (t==2 only,
//! still asserting count+flags+bound-values, counted) when and only when a
//! representation tie is what differs. See its comment and D1.
//!
//! WITHIN-TIE ORDER FENCE (ratified non-surface; GL-PARMERGE-1 precedent).
//! C canonicalizes with qsort_arg — vendored verbatim, so the algorithm
//! matches — but the shipped Rust uses a stable sort. For two input ranges
//! that COMPARE EQUAL yet carry DIFFERENT bytes, which representative survives
//! the merge is an ordering artifact, not a behavioral surface. The driver
//! removes that ambiguity BY CONSTRUCTION rather than asserting over it:
//!   - range flags are normalized through `wf_flags` (RANGE_CONTAIN_EMPTY and
//!     the RANGE_xB_NULL bits masked off) — the shape make_range itself emits,
//!     so equal bounds imply equal flags;
//!   - numrange bounds entering a multirange are minted from INTEGER literals
//!     only, so numerics that compare equal are byte-identical. (`1.0` vs
//!     `1.00` compare equal with different dscale; that surface belongs to the
//!     adt/numeric lane and to rangetypes_diff's single-range arms, and here it
//!     is fed to the RANGE operand of the r x mr arm, never into a
//!     multirange's sort input.)
//! For the arms the driver BUILDS, ties are therefore between byte-identical
//! ranges and the surviving representative is immaterial. This fence covers
//! ONLY those arms. It does NOT hold for text/binary io, where the bounds are
//! user bytes — value-equal-but-byte-different numerics DO occur there, and the
//! driver's own text seed corpus mints them, which is exactly how the fleet
//! found D1. Those arms use `compare_mr_image`'s gated value-level fallback.
//!
//! Comparison planes: value bytes/bits (canonicalized multirange images,
//! output text, wire bytes, element datum images, bool/i32/u32/u64 results),
//! error verdict, and errcode/sqlstate CLASS (`err_class` mirrors the oracle's
//! table: 1..11 from the range oracle, plus 12=21000 cardinality,
//! 13=22004 null-not-allowed, 14=54000 program-limit, 99=elog/internal).
//! Message text out of scope.
//!
//! Input layout: [sel][typ][payload]; sel % 11 picks the arm, typ % 3 the
//! instantiation:
//!   0 text io:     multirange_in(payload-as-literal) image + errclass;
//!                  on Ok, multirange_out roundtrip text     (4231/4232/4230)
//!   1 binary io:   multirange_recv(payload-as-wire) image + errclass;
//!                  on Ok, multirange_send roundtrip wire    (4233/4234)
//!   2 ctors:       constructor0 / constructor1(range) /
//!                  constructor2(variadic array), plus the NULL-member,
//!                  wrong-elemtype and multidimensional error arms
//!                                                  (4280-4298 family)
//!   3 accessors:   lower/upper/isempty/lower_inc/upper_inc/lower_inf/
//!                  upper_inf                                (4235-4241)
//!   4 mr x mr:     eq ne lt le ge gt cmp overlaps contains contained_by
//!                  adjacent before after overleft overright
//!                                     (4244/4245/4274-4277/4273/4248/4251/
//!                                      4254/4256/4260/4263/4266/4269)
//!   5 r x mr:      all sixteen range x multirange / multirange x range forms
//!                                     (4246/4247/4250/4541/4253/4542/4255/
//!                                      4257/4258/4259/4261/4262/4264/4265/
//!                                      4267/4268)
//!   6 elem:        multirange_contains_elem + elem_contained_by_multirange
//!                                                           (4249/4252)
//!   7 setops:      union / minus / intersect                (4270-4272)
//!   8 hash:        hash_multirange + hash_multirange_extended(seed)
//!                                                           (4278/4279)
//!   9 merge:       range_merge(multirange) -> union range         (4228)
//!  10 internals:   multirange_get_range(i) / get_union_range / count /
//!                  is_empty — the support API the fmgr entries do not reach
//!                  directly (the item offset/length stride walk)
//!
//! fc-wrapper plane: every arm drives the crate's builtins.rs fc_* wrapper on
//! a native LocalFcinfo, so builtins.rs/io.rs/lib.rs all execute under the
//! diff (the wrapper IS the shipped entry point).
//!
//! SKIPPED rows (recorded as exception rows in phase1-routes.tsv; agg-state /
//! SRF carve): unnest (1293), range_agg_transfn/finalfn (4299/4300),
//! multirange_agg_transfn/finalfn (6225/6226),
//! multirange_intersect_agg_transfn (4388) — their pure delegates
//! (multirange_intersect_internal, make_multirange) ARE reached through the
//! non-aggregate entries above. Also skipped: multirange_typanalyze (4242) and
//! multirangesel (4243), engine carves.
//! Known non-surface: C's fn_extra memo HIT path (both sides run fresh flinfos
//! per iteration; the memo is a pure cache with no behavioral surface).

use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};

use adt_multirangetypes as mrt;
use adt_multirangetypes::builtins as mb;
use adt_rangetypes as rt;
use datum::Datum;
use mcx::MemoryContext;
use types_core::fmgr::{AggFnArgTypes, FnExprErased};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction};

extern "C" {
    fn pg_diff_mr_in(
        typ: i32,
        s: *const core::ffi::c_char,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_mr_in_soft(
        typ: i32,
        s: *const core::ffi::c_char,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
        soft_class: *mut i32,
        isnull_out: *mut i32,
    ) -> i32;
    fn pg_diff_mr_out(
        img: *const u8,
        out: *mut core::ffi::c_char,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_mr_recv(
        typ: i32,
        wire: *const u8,
        wirelen: i32,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_mr_send(img: *const u8, out: *mut u8, outlen: *mut i32, outcap: i32) -> i32;
    fn pg_diff_mr_ctor(
        typ: i32,
        nargs: i32,
        rng: *const u8,
        arr: *const u8,
        argnull: i32,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_mr_accessors(
        typ: i32,
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
    fn pg_diff_mr_ops(img1: *const u8, img2: *const u8, res: *mut i32) -> i32;
    fn pg_diff_mr_range_ops(rimg: *const u8, mimg: *const u8, res: *mut i32) -> i32;
    fn pg_diff_mr_elem(
        img: *const u8,
        v: i64,
        numptr: *const u8,
        contains: *mut i32,
        contained: *mut i32,
    ) -> i32;
    fn pg_diff_mr_setop(
        which: i32,
        img1: *const u8,
        img2: *const u8,
        out: *mut u8,
        outlen: *mut i32,
        outcap: i32,
    ) -> i32;
    fn pg_diff_mr_hash(img: *const u8, h: *mut u32, seed: u64, he: *mut u64) -> i32;
    fn pg_diff_mr_merge(img: *const u8, out: *mut u8, outlen: *mut i32, outcap: i32) -> i32;
    fn pg_diff_mr_internals(
        typ: i32,
        img: *const u8,
        i: i32,
        count: *mut u32,
        is_empty: *mut i32,
        range_out: *mut u8,
        range_len: *mut i32,
        union_out: *mut u8,
        union_len: *mut i32,
        outcap: i32,
    ) -> i32;
}

const INT4RANGEOID: Oid = 3904;
const INT8RANGEOID: Oid = 3926;
const NUMRANGEOID: Oid = 3906;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const NUMERICOID: Oid = 1700;

const INT4MULTIRANGEOID: Oid = 4451;
const INT8MULTIRANGEOID: Oid = 4536;
const NUMMULTIRANGEOID: Oid = 4532;

const OUTCAP: usize = 8192;
/// Buffers for the TEXT-IO arm, which is the only arm whose output size is not
/// bounded by its input size: one `1e16383` bound expands to a ~16 KB decimal
/// string through numeric_out, so a handful of members can produce hundreds of
/// kilobytes of image and text. Sized to make overflow unreachable rather than
/// skipped — a size-conditional `return` would be a vacuous pass. Reused across
/// iterations (a fresh multi-MiB zeroed Vec per exec would dominate the run).
const BIGCAP: usize = 4 << 20;
/// Literal length cap for the text arm. multirange_in's surface is brace /
/// quote / escape / member-splitting structure, all of which lives in a few
/// tens of bytes; the bound-value parsing surface below it belongs to
/// rangetypes_diff and the adt/numeric lane.
const TEXT_LIT_CAP: usize = 192;

thread_local! {
    /// image scratch for the text arm (borrowed alone; never aliased with TEXT_BUF)
    static IMG_BUF: core::cell::RefCell<Vec<u8>> =
        core::cell::RefCell::new(vec![0u8; BIGCAP]);
    /// output-text scratch for the text arm
    /// Element type is c_char, NOT a fixed signedness: c_char is i8 on macOS
    /// aarch64 but u8 on aarch64-unknown-linux-gnu, and this buffer is handed
    /// straight to the oracle's `char *` out-parameters. Hard-coding i8 built
    /// on the laptop and failed the first Linux compile on the fleet.
    static TEXT_BUF: core::cell::RefCell<Vec<core::ffi::c_char>> =
        core::cell::RefCell::new(vec![0 as core::ffi::c_char; BIGCAP]);
}
/// Max ranges fed into one multirange (keeps every image inside OUTCAP).
const MAX_RANGES: usize = 6;

/// sqlstate -> the oracle's errcode CLASS (pg_multirangetypes_io.c header).
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
        // 12 is the RANGE oracle's number for this class and the two oracles
        // share one table (pg_multirangetypes_io.c #includes it) — see the
        // shared-table note there, and cross_target_err_class_agreement in
        // rangetypes_diff.rs.
        12
    } else if e.sqlstate == te::ERRCODE_CARDINALITY_VIOLATION {
        13
    } else if e.sqlstate == te::ERRCODE_NULL_VALUE_NOT_ALLOWED {
        14
    } else if e.sqlstate == te::ERRCODE_INTERNAL_ERROR {
        // C's elog(ERROR) records class 99 and PgError::error defaults to
        // XX000, so the two elog-equivalent paths MUST land on the same class
        // — otherwise every elog arm the crate has (wrong constructor type,
        // not-a-multirange) reads as a false divergence.
        99
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
    mltrngtypid: Oid,
    rngtypid: Oid,
    elem_typid: Oid,
    typlen: i16,
    typbyval: bool,
    typalign: u8,
    typstorage: u8,
}

// fn_expr rettype carriers. STATICS, not per-call Box::leak: the fleet's
// LSan run flagged a 24-byte leak per iteration (PINS is const and there are
// only three instantiations, so nothing needs to be allocated at all). Leaks
// abort a libFuzzer campaign, and macOS has no LSan, so this class is
// invisible on the laptop — the fleet Linux run is the only detector.
static MR_RETTYPE: [AggFnArgTypes; 3] = [
    AggFnArgTypes { rettype: INT4MULTIRANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: INT8MULTIRANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: NUMMULTIRANGEOID, argtypes: &[] },
];
static RNG_RETTYPE: [AggFnArgTypes; 3] = [
    AggFnArgTypes { rettype: INT4RANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: INT8RANGEOID, argtypes: &[] },
    AggFnArgTypes { rettype: NUMRANGEOID, argtypes: &[] },
];

const PINS: [Pin; 3] = [
    Pin {
        mltrngtypid: INT4MULTIRANGEOID,
        rngtypid: INT4RANGEOID,
        elem_typid: INT4OID,
        typlen: 4,
        typbyval: true,
        typalign: b'i',
        typstorage: b'p',
    },
    Pin {
        mltrngtypid: INT8MULTIRANGEOID,
        rngtypid: INT8RANGEOID,
        elem_typid: INT8OID,
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        typstorage: b'p',
    },
    Pin {
        mltrngtypid: NUMMULTIRANGEOID,
        rngtypid: NUMRANGEOID,
        elem_typid: NUMERICOID,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        typstorage: b'm',
    },
];

const F_CANONICAL: [Oid; 3] = [3914, 3928, 0 /* numrange: continuous */];

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

fn multirange_info(t: usize) -> mrt::MultirangeInfo {
    mrt::MultirangeInfo { pin: None, mltrngtypid: PINS[t].mltrngtypid, rng: range_info(t) }
}

/// flinfo pre-seeded with the MultirangeInfo memo (cached_multirange_info
/// hits, the typcache seam never fires) plus the rettype carrier that the
/// constructors and every multirange_get_typcache caller read.
fn ops_flinfo(t: usize) -> FmgrInfo {
    let mut fl = FmgrInfo::new(mb::fc_multirange_eq, 0, 2, true, false);
    fl.set_fn_extra(multirange_info(t));
    let carrier: &'static AggFnArgTypes = &MR_RETTYPE[t];
    // SAFETY: leaked 'static carrier outlives every read.
    fl.fn_expr = Some(unsafe { FnExprErased::from_node_ref(carrier) });
    fl
}

/// The ELEMENT type's I/O (int4/int8/numeric) — one level below the range's.
fn elem_io_finfo(t: usize, sel: lsyscache::IOFuncSelector) -> FmgrInfo {
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

/// The RANGE type's own I/O: a multirange's element I/O, exactly as
/// cached_multirange_io_data resolves it. Its fn_extra is pre-seeded too, so
/// the range crate's typcache seam does not fire either.
fn range_io_finfo(t: usize, sel: lsyscache::IOFuncSelector) -> FmgrInfo {
    use lsyscache::IOFuncSelector as S;
    let (f, oid): (PGFunction, Oid) = match sel {
        S::IOFunc_input => (rt::builtins::fc_range_in, 3834),
        S::IOFunc_output => (rt::builtins::fc_range_out, 3835),
        S::IOFunc_receive => (rt::builtins::fc_range_recv, 3836),
        S::IOFunc_send => (rt::builtins::fc_range_send, 3837),
    };
    let nargs = match sel {
        S::IOFunc_input | S::IOFunc_receive => 3,
        _ => 1,
    };
    let mut fl = FmgrInfo::new(f, oid, nargs, true, false);
    fl.set_fn_extra(rt::io::RangeIOData {
        ri: range_info(t),
        typioproc: elem_io_finfo(t, sel),
        typioparam: PINS[t].elem_typid,
    });
    fl
}

/// flinfo for the multirange io wrappers: fn_extra = MultirangeIOData.
fn io_flinfo(t: usize, sel: lsyscache::IOFuncSelector) -> FmgrInfo {
    let mut fl = FmgrInfo::new(mb::fc_multirange_in, 0, 3, true, false);
    fl.set_fn_extra(mrt::io::MultirangeIOData {
        mi: multirange_info(t),
        typioproc: range_io_finfo(t, sel),
        typioparam: PINS[t].rngtypid,
    });
    fl
}

// ---------------------------------------------------------------------------
// fc-call plumbing (cash_diff / rangetypes_diff pattern)
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

/// Mint a numeric image from an INTEGER value via the shipped numeric_in.
/// Integer-only by design — see the within-tie fence in the module header.
fn mint_numeric_int(mcx: mcx::Mcx<'_>, v: i64) -> Option<Vec<u8>> {
    let cs = CString::new(format!("{v}")).ok()?;
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
    Some(datum_varlena_bytes(out.result.ok()?).to_vec())
}

/// Mint a numeric image from arbitrary payload bytes (dscale-diverse). Used
/// only for operands that never enter a multirange's sort input.
fn mint_numeric_lit(mcx: mcx::Mcx<'_>, lit: &[u8]) -> Option<Vec<u8>> {
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
    Some(datum_varlena_bytes(out.result.ok()?).to_vec())
}

enum Bound {
    ByVal(i64),
    Num(Vec<u8>),
}

impl Bound {
    fn datum(&self) -> Datum {
        match self {
            Bound::ByVal(v) => Datum::from_i64(*v),
            Bound::Num(b) => Datum::from_usize(b.as_ptr() as usize),
        }
    }
}

/// On-disk-legal range flags: the shape make_range emits (no CONTAIN_EMPTY, no
/// xB_NULL, no INC on an infinite side). See the within-tie fence.
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

/// Build a serialized RANGE image through the SHIPPED range constructor
/// (fc_range_constructor3 -> make_range -> range_serialize -> datum_write), so
/// the bytes are exactly what a SQL `numrange(1,2,'[)')` produces.
///
/// A hand-rolled serializer is NOT good enough here and the first smoke run
/// proved it: PG's datum_write PACKS a packable byref bound (numeric,
/// typstorage 'm') into a SHORT 1-byte varlena header and then applies NO
/// alignment padding (datum_compute_size's VARATT_CAN_MAKE_SHORT arm +
/// att_align_datum's VARATT_IS_SHORT arm). A builder that stores the 4-byte
/// form behind 4-byte padding produces an image no make_range would ever emit,
/// and C's range_deserialize then reads the pad bytes as a varlena header —
/// which SEGV'd inside numeric_cmp on a garbage bound pointer. Constructing
/// through the shipped serializer makes builder/serializer skew impossible.
/// A NULL bound argument means an infinite bound, exactly as in SQL.
fn build_range_image(
    t: usize,
    flags: u8,
    lo: &Bound,
    up: &Bound,
    mcx: mcx::Mcx<'_>,
) -> Option<Vec<u8>> {
    let empty = flags & rt::RANGE_EMPTY != 0;
    // flags text: "[]" / "[)" / "(]" / "()" — range_parse_flags' whole domain.
    // An EMPTY range is requested as equal bounds with both sides exclusive,
    // which make_range canonicalizes to the empty range on every subtype.
    let inc_l = !empty && flags & rt::RANGE_LB_INC != 0;
    let inc_u = !empty && flags & rt::RANGE_UB_INC != 0;
    let ftxt = [if inc_l { b'[' } else { b'(' }, if inc_u { b']' } else { b')' }];
    // text varlena for the flags argument (4B header + 2 bytes)
    let mut ftext = Vec::with_capacity(6);
    ftext.extend_from_slice(&datum::set_varsize_4b(6));
    ftext.extend_from_slice(&ftxt);

    let inf_l = !empty && flags & rt::RANGE_LB_INF != 0;
    let inf_u = !empty && flags & rt::RANGE_UB_INF != 0;
    let (lo_d, up_d) = if empty {
        // equal bounds, both exclusive => the empty range
        (Some(lo.datum()), Some(lo.datum()))
    } else {
        (
            if inf_l { None } else { Some(lo.datum()) },
            if inf_u { None } else { Some(up.datum()) },
        )
    };

    let mut fl = range_probe_flinfo(t);
    let r = fc_call(
        rt::builtins::fc_range_constructor3,
        Some(&mut fl),
        mcx,
        [lo_d, up_d, Some(Datum::from_usize(ftext.as_ptr() as usize))],
    );
    r.result.ok().map(|d| datum_varlena_bytes(d).to_vec())
}

/// Decode one range for a multirange's sort input: normalized flags and bounds
/// from a SMALL integer domain, so adjacency / overlap / nesting / duplicate
/// cases are hit densely instead of vanishingly.
fn decode_range(t: usize, rd: &mut Rd, mcx: mcx::Mcx<'_>) -> Option<Vec<u8>> {
    let flags = wf_flags(rd.u8());
    let a = (rd.u8() % 24) as i64;
    let b = (rd.u8() % 24) as i64;
    let (lo_v, up_v) = if a <= b { (a, b) } else { (b, a) };
    let (lo, up) = match t {
        0 | 1 => (Bound::ByVal(lo_v), Bound::ByVal(up_v)),
        _ => (Bound::Num(mint_numeric_int(mcx, lo_v)?), Bound::Num(mint_numeric_int(mcx, up_v)?)),
    };
    build_range_image(t, flags, &lo, &up, mcx)
}

/// A range whose bounds may be full-width / dscale-diverse: the RANGE operand
/// of the r x mr arm, which never enters a multirange's sort input. Rejected
/// if the two sides would not agree it is a valid range at all (lower > upper
/// is the range crate's error plane, owned by rangetypes_diff).
fn decode_wide_range(t: usize, rd: &mut Rd, mcx: mcx::Mcx<'_>) -> Option<Vec<u8>> {
    let flags = wf_flags(rd.u8());
    let (lo, up) = match t {
        0 => (Bound::ByVal(rd.i32() as i64), Bound::ByVal(rd.i32() as i64)),
        1 => (Bound::ByVal(rd.i64()), Bound::ByVal(rd.i64())),
        _ => {
            let n1 = (rd.u8() % 12) as usize + 1;
            let l1 = rd.bytes(n1).to_vec();
            let n2 = (rd.u8() % 12) as usize + 1;
            let l2 = rd.bytes(n2).to_vec();
            (Bound::Num(mint_numeric_lit(mcx, &l1)?), Bound::Num(mint_numeric_lit(mcx, &l2)?))
        }
    };
    // The shipped constructor rejects lower > upper itself (the range crate's
    // error plane, owned by rangetypes_diff), so a None here just means this
    // iteration had no valid range operand to offer.
    build_range_image(t, flags, &lo, &up, mcx)
}

/// flinfo for probing a RANGE (fn_extra = RangeInfo, as the range crate's own
/// wrappers expect).
fn range_probe_flinfo(t: usize) -> FmgrInfo {
    let mut fl = FmgrInfo::new(rt::builtins::fc_range_eq, 0, 2, true, false);
    fl.set_fn_extra(range_info(t));
    let carrier: &'static AggFnArgTypes =
        &RNG_RETTYPE[t];
    // SAFETY: leaked 'static carrier outlives every read.
    fl.fn_expr = Some(unsafe { FnExprErased::from_node_ref(carrier) });
    fl
}

fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// Build a 1-D ArrayType image of range elements with no NULL bitmap — the
/// shape a `variadic int4range[]` constructor call receives. `elemtype` and
/// `ndim` are parameters so the wrong-elemtype and multidimensional error arms
/// are reachable.
fn build_range_array(ranges: &[Vec<u8>], elemtype: Oid, ndim: i32) -> Vec<u8> {
    build_range_array_nulls(ranges, elemtype, ndim, &[])
}

/// Re-pack a 4-byte-header range image into SHORT (1-byte) varlena form. Array
/// MEMBERS are packed short by the array builder whenever they fit, so this is
/// the on-disk shape of a real `int4range[]`, and it is what reaches
/// multirange_constructor2's member-expand arm (builtins.rs:226-229). Short
/// members also carry NO alignment padding, which the array walker must honour.
/// `None` = too large for the short form.
fn to_short_member(img: &[u8]) -> Option<Vec<u8>> {
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

/// Array image whose members are SHORT-form (1-byte header, no alignment pad).
fn build_range_array_short(ranges: &[Vec<u8>], elemtype: Oid) -> Option<Vec<u8>> {
    let shorts: Option<Vec<Vec<u8>>> = ranges.iter().map(|r| to_short_member(r)).collect();
    let shorts = shorts?;
    let mut img = vec![0u8; 16];
    img[4..8].copy_from_slice(&1i32.to_ne_bytes());
    img[12..16].copy_from_slice(&elemtype.to_ne_bytes());
    img.extend_from_slice(&(shorts.len() as i32).to_ne_bytes());
    img.extend_from_slice(&1i32.to_ne_bytes());
    while img.len() != maxalign(img.len()) {
        img.push(0);
    }
    for r in &shorts {
        // NO alignment padding before a short-form member: that is the whole
        // point of the packed form, and mis-padding here would desync the two
        // sides' array walks.
        img.extend_from_slice(r);
    }
    let n = img.len();
    img[0..4].copy_from_slice(&datum::set_varsize_4b(n));
    Some(img)
}

/// `null_at` = element indexes to mark NULL in the array's null bitmap. A
/// non-empty list is what reaches multirange_constructor2's per-element
/// null_member arm (builtins.rs:220): the argisnull(0) arm rejects a NULL
/// ARGUMENT, but a non-null array carrying a NULL MEMBER is a different, also
/// SQL-reachable path (`select int4multirange(ARRAY[NULL::int4range])`).
fn build_range_array_nulls(
    ranges: &[Vec<u8>],
    elemtype: Oid,
    ndim: i32,
    null_at: &[usize],
) -> Vec<u8> {
    let nelems = ranges.len();
    let has_nulls = !null_at.is_empty();
    let mut img = vec![0u8; 16];
    img[4..8].copy_from_slice(&ndim.to_ne_bytes());
    img[12..16].copy_from_slice(&elemtype.to_ne_bytes());
    if ndim > 0 {
        img.extend_from_slice(&(nelems as i32).to_ne_bytes()); // dims[0]
        img.extend_from_slice(&1i32.to_ne_bytes()); // lbound[0]
    }
    if has_nulls {
        // ArrayType null bitmap: one bit per element, LSB-first, 1 = NOT null.
        let nbytes = (nelems + 7) / 8;
        let mut bits = vec![0u8; nbytes];
        for i in 0..nelems {
            if !null_at.contains(&i) {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        img.extend_from_slice(&bits);
    }
    while img.len() != maxalign(img.len()) {
        img.push(0);
    }
    if has_nulls {
        // dataoffset != 0 is the flag that a null bitmap is present, and it
        // must be the MAXALIGN'd offset of the data area.
        let off = img.len() as i32;
        img[8..12].copy_from_slice(&off.to_ne_bytes());
    }
    if ndim > 0 {
        let mut first = true;
        for (i, r) in ranges.iter().enumerate() {
            if null_at.contains(&i) {
                continue; // NULL elements occupy no payload bytes
            }
            if !first {
                // element alignment: the range type's own typalign is 'd'
                while img.len() % 8 != 0 {
                    img.push(0);
                }
            }
            first = false;
            img.extend_from_slice(r);
        }
    }
    let n = img.len();
    img[0..4].copy_from_slice(&datum::set_varsize_4b(n));
    img
}

// ---------------------------------------------------------------------------
// comparators
// ---------------------------------------------------------------------------

fn compare_image(name: &str, cret: i32, cbytes: &[u8], r: &FcOut, dbg: &str) {
    assert!(cret >= 0, "{name}: oracle buffer overflow (harness bug) {dbg}");
    match &r.result {
        Ok(d) => {
            assert!(cret == 0, "{name} DIVERGENCE {dbg}: C err {cret} vs Rust Ok");
            assert!(!r.isnull, "{name} DIVERGENCE {dbg}: Rust returned SQL NULL");
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
// numeric-representative tie fallback (FINDINGS D1 — RATIFIED Michael
// 2026-07-31: numeric tie-representative choice in multirange canonicalization
// = non-surface, value-preserving; pgrust keeps its stable sort)
// ---------------------------------------------------------------------------
//
// Byte-exact image comparison is the DEFAULT and stays mandatory. It is
// relaxed to a value-level structural comparison in EXACTLY ONE situation, and
// only after the relaxation itself proves the situation holds:
//
//   nummultirange canonicalization ties. multirange_canonicalize sorts input
//   ranges with range_compare, which compares numeric bounds BY VALUE, so `2`
//   and `2.0000` TIE. C's qsort_arg is UNSTABLE and range_union_internal
//   returns a fixed side on a value tie; pgrust's canonicalize uses a STABLE
//   sort. When the input carries two value-equal-but-byte-different numeric
//   bounds, the two implementations keep different (value-equal) byte
//   representatives. multirange_in and multirange_recv parse such bounds
//   straight from user text/wire, so this is reachable there; the
//   integer-minted arms cannot produce it (value-equal => byte-equal for int).
//
// Why the value-level check is not a vacuous pass:
//   * It fires only for t == 2. A byte difference on int4/int8 multirange has
//     NO numeric representation to differ and is always a hard divergence.
//   * When it fires it still asserts, per output range: equal range COUNT,
//     equal FLAGS, and equal bound VALUE via numeric_cmp (infinite bounds must
//     match infinite). A dropped/added/reordered range, a wrong flag, or a
//     wrong value all still hard-fail.
//   * Soundness of "same value+flags+count but different bytes => tie": inside
//     multirange_in / multirange_recv / multirange_canonicalize a bound is
//     NEVER re-serialized — range_union_internal on a tie returns one input
//     image verbatim (UnionResult::Input1/Input2). So a value-equal
//     byte-different output bound can only be an INPUT representative selected
//     by the tie. A dscale corruption (which WOULD be a bug) would have to
//     originate in range_in/numeric_in, where it also breaks the single-range
//     case and is caught by rangetypes_diff's byte-exact check.
//   * Every fire is counted; the count is printed periodically so the fleet
//     can confirm it stays a rare fraction and never fires for t != 2.
static NUMERIC_TIE_FALLBACKS: AtomicU64 = AtomicU64::new(0);

pub fn numeric_tie_fallback_count() -> u64 {
    NUMERIC_TIE_FALLBACKS.load(Ordering::Relaxed)
}

/// ANTI-VACUITY COUNTERS for the arms added when the coverage gaps were closed.
/// A new arm that silently never fires would hand back a clean campaign that
/// proved nothing (the gate-blindness class), so each is counted and the
/// vacuity tests assert the counts advance.
static SOFT_MODE: AtomicU64 = AtomicU64::new(0);
static SOFT_CAPTURED: AtomicU64 = AtomicU64::new(0);
static NULL_ARG: AtomicU64 = AtomicU64::new(0);
static NULL_MEMBER: AtomicU64 = AtomicU64::new(0);
static UNION_RANGE_EMPTY: AtomicU64 = AtomicU64::new(0);
static SHORT_MEMBER: AtomicU64 = AtomicU64::new(0);

pub fn null_member_count() -> u64 {
    NULL_MEMBER.load(Ordering::Relaxed)
}

pub fn union_range_empty_count() -> u64 {
    UNION_RANGE_EMPTY.load(Ordering::Relaxed)
}

pub fn soft_mode_count() -> u64 {
    SOFT_MODE.load(Ordering::Relaxed)
}

pub fn soft_captured_count() -> u64 {
    SOFT_CAPTURED.load(Ordering::Relaxed)
}

struct MrStats;

fn bump(f: impl FnOnce(&MrStats)) {
    f(&MrStats);
}

impl MrStats {
    #[allow(non_snake_case)]
    fn soft_mode_inc(&self) {
        SOFT_MODE.fetch_add(1, Ordering::Relaxed);
    }
    fn soft_captured_inc(&self) {
        SOFT_CAPTURED.fetch_add(1, Ordering::Relaxed);
    }
    fn null_arg_inc(&self) {
        NULL_ARG.fetch_add(1, Ordering::Relaxed);
    }
    fn null_member_inc(&self) {
        NULL_MEMBER.fetch_add(1, Ordering::Relaxed);
    }
    fn union_range_empty_inc(&self) {
        UNION_RANGE_EMPTY.fetch_add(1, Ordering::Relaxed);
    }
    fn short_member_inc(&self) {
        SHORT_MEMBER.fetch_add(1, Ordering::Relaxed);
    }
}

/// Total driver iterations, for the fallback-rate line below.
static ITERS: AtomicU64 = AtomicU64::new(0);

/// Print the numeric-tie fallback tally on a power-of-two-ish cadence so the
/// fleet log shows it stays a rare fraction of executions (and, since the
/// fallback is gated to t==2, never fires for a byval instantiation). Cheap:
/// one relaxed increment per exec, a stderr line only at the checkpoints.
fn report_tie_fallbacks() {
    let n = ITERS.fetch_add(1, Ordering::Relaxed) + 1;
    if n & (n - 1) == 0 && n >= 1 << 16 {
        eprintln!(
            "multirangetypes_diff: numeric-tie fallbacks {} / {} execs; \
             soft_mode={} soft_captured={} null_arg={} null_member={} union_empty={} short_member={}",
            NUMERIC_TIE_FALLBACKS.load(Ordering::Relaxed),
            n,
            SOFT_MODE.load(Ordering::Relaxed),
            SOFT_CAPTURED.load(Ordering::Relaxed),
            NULL_ARG.load(Ordering::Relaxed),
            NULL_MEMBER.load(Ordering::Relaxed),
            UNION_RANGE_EMPTY.load(Ordering::Relaxed),
            SHORT_MEMBER.load(Ordering::Relaxed)
        );
    }
}

/// Do the two multirange images denote the SAME multirange value — same range
/// count, and each range's bounds equal in VALUE and in flags (inclusive /
/// infinite)? Computed with the SHIPPED `multirange_cmp`, i.e. the exact
/// value-equality the code under test defines: it walks range-by-range and
/// compares each bound through the element cmp (numeric_cmp with the correct
/// packed-short detoast, which a raw fc call on the stored bound datum
/// mishandles). cmp == 0 therefore certifies count + flags + every bound value
/// all agree; a dropped/added/reordered range, a wrong flag, or a wrong value
/// makes it non-zero. This is NOT a blanket "ignore bytes": it is only ever
/// consulted after a byte difference on t == 2, and a non-zero result is a hard
/// divergence.
fn multiranges_value_equal(t: usize, a: &[u8], b: &[u8], mcx: mcx::Mcx<'_>) -> bool {
    if mrt::multirange_count(a) != mrt::multirange_count(b) {
        return false;
    }
    let mut fl = ops_flinfo(t);
    let o = fc_call(
        mb::fc_multirange_cmp,
        Some(&mut fl),
        mcx,
        [
            Some(Datum::from_usize(a.as_ptr() as usize)),
            Some(Datum::from_usize(b.as_ptr() as usize)),
        ],
    );
    matches!(o.result, Ok(d) if d.as_i32() == 0)
}

/// Compare a multirange-image result. Byte-exact by default; on a byte
/// difference, the numeric-representation-tie fallback above (precise, gated to
/// t == 2, counted). Use this — not compare_image — for every arm whose result
/// is a canonicalized MULTIRANGE image.
fn compare_mr_image(
    name: &str,
    t: usize,
    cret: i32,
    cbytes: &[u8],
    r: &FcOut,
    mcx: mcx::Mcx<'_>,
    dbg: &str,
) {
    assert!(cret >= 0, "{name}: oracle buffer overflow (harness bug) {dbg}");
    match &r.result {
        Ok(d) => {
            assert!(cret == 0, "{name} DIVERGENCE {dbg}: C err {cret} vs Rust Ok");
            assert!(!r.isnull, "{name} DIVERGENCE {dbg}: Rust returned SQL NULL");
            let rbytes = datum_varlena_bytes(*d);
            if rbytes == cbytes {
                return; // byte-exact: the default and overwhelmingly common path
            }
            assert!(
                t == 2,
                "{name} DIVERGENCE {dbg}: byte difference on a byval instantiation \
                 (no numeric representation to differ) C={cbytes:02x?} Rust={rbytes:02x?}"
            );
            assert!(
                multiranges_value_equal(t, cbytes, rbytes, mcx),
                "{name} DIVERGENCE {dbg}: multiranges differ by VALUE, not just numeric \
                 representation C={cbytes:02x?} Rust={rbytes:02x?}"
            );
            // Same multirange VALUE, byte-different only in value-equal numeric
            // bounds: the unstable-qsort vs stable-sort tie (FINDINGS D1).
            NUMERIC_TIE_FALLBACKS.fetch_add(1, Ordering::Relaxed);
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

fn compare_scalar(name: &str, cret: i32, cval: i32, r: &FcOut, is_int: bool, dbg: &str) {
    match &r.result {
        Ok(d) => {
            assert!(cret == 0, "{name} DIVERGENCE {dbg}: C err {cret} vs Rust Ok");
            let rv = if is_int { d.as_i32() } else { i32::from(d.as_bool()) };
            assert!(cval == rv, "{name} DIVERGENCE {dbg}: C={cval} Rust={rv}");
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
// the agreed canonical image (this is the normalization-kernel diff)
// ---------------------------------------------------------------------------

/// Run BOTH sides' multirange_constructor2 over the same variadic array of
/// (deliberately denormalized) ranges, assert the canonicalized image bytes
/// match, and return the agreed bytes for downstream arms. `None` = both sides
/// errored identically, so there is nothing to hand on.
fn agreed_image(t: usize, ranges: &[Vec<u8>], mcx: mcx::Mcx<'_>) -> Option<Vec<u8>> {
    let ndim = i32::from(!ranges.is_empty());
    let arr = build_range_array(ranges, PINS[t].rngtypid, ndim);
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret = unsafe {
        pg_diff_mr_ctor(
            t as i32,
            1,
            core::ptr::null(),
            arr.as_ptr(),
            0,
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
        )
    };
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        mb::fc_multirange_constructor2,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(arr.as_ptr() as usize))],
    );
    let dbg = format!("t={t} n={} (canonicalize)", ranges.len());
    // constructor2 inputs here are integer-minted (decode_range), so
    // value-equality implies byte-equality and the tie fallback never fires;
    // routed through the tie-aware comparator anyway so no multirange-image
    // comparison in the driver is silently weaker than another.
    compare_mr_image("multirange_constructor2", t, cret, &cbuf[..clen as usize], &r, mcx, &dbg);
    r.result.ok().map(|d| datum_varlena_bytes(d).to_vec())
}

/// Decode 0..=MAX_RANGES ranges from the payload, then canonicalize on both
/// sides via `agreed_image`.
fn agreed_from_payload(t: usize, rd: &mut Rd, mcx: mcx::Mcx<'_>) -> Option<Vec<u8>> {
    let n = (rd.u8() % (MAX_RANGES as u8 + 1)) as usize;
    let mut ranges = Vec::with_capacity(n);
    for _ in 0..n {
        ranges.push(decode_range(t, rd, mcx)?);
    }
    agreed_image(t, &ranges, mcx)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

/// multirange_constructor2 detoasts its variadic array through the detoast
/// seam. Install the SHIPPED implementation — the seam is environment, the
/// detoast logic is computation and must never be mocked. The arrays this
/// driver builds are flat, so the external-TOAST fetch seam below it is never
/// reached.
fn install_seams() {
    crate::install_detoast_seam_once();
}

pub fn multirangetypes_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    // Skip when a foreign module owns the typcache env — see the identical
    // guard in rangetypes_diff (rowtypes_diff convention; RESIDUE: composite
    // fixture would retire this).
    if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
        return;
    }
    install_seams();
    report_tie_fallbacks();
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&typb, payload)) = rest.split_first() else {
        return;
    };
    let t = (typb % 3) as usize;
    let ctx = MemoryContext::new("multirangetypes_fuzz");
    let mcx = ctx.mcx();

    match sel % 11 {
        0 => arm_text_io(t, payload, mcx, typb & 0x80 != 0),
        1 => arm_binary_io(t, payload, mcx),
        2 => arm_ctors(t, payload, mcx),
        3 => arm_accessors(t, payload, mcx),
        4 => arm_mr_ops(t, payload, mcx),
        5 => arm_range_ops(t, payload, mcx),
        6 => arm_elem(t, payload, mcx),
        7 => arm_setops(t, payload, mcx),
        8 => arm_hash(t, payload, mcx),
        9 => arm_merge(t, payload, mcx),
        10 => arm_internals(t, payload, mcx),
        _ => unreachable!(),
    }
}

/// SOFT-ERROR (escontext) PLANE for multirange_in — the surface behind
/// `pg_input_is_valid()` and `COPY ... ON_ERROR ignore`. Mirrors the range
/// target's arm_text_in_soft: compares the soft-error OCCURRED flag, the
/// captured sqlstate class, the image when the literal is valid, and
/// soft/hard verdict agreement on each side independently.
///
/// `fcinfo.isnull` on the soft edge is deliberately NOT compared — see the
/// SOFT-ISNULL finding and the soft_isnull_contract test in rangetypes_diff.rs;
/// C is not self-consistent about it and no caller reads it.
fn arm_text_in_soft(t: usize, payload: &[u8], cs: &CString, mcx: mcx::Mcx<'_>) {
    bump(|st| st.soft_mode_inc());
    let dbg = format!("t={t} soft in={:?}", String::from_utf8_lossy(payload));

    let (cret, cimg, csoft) = IMG_BUF.with(|b| {
        let mut cbuf = b.borrow_mut();
        let mut clen = 0i32;
        let mut csoft = 0i32;
        let mut cisnull = 0i32;
        let cret = unsafe {
            pg_diff_mr_in_soft(
                t as i32,
                cs.as_ptr(),
                cbuf.as_mut_ptr(),
                &mut clen,
                BIGCAP as i32,
                &mut csoft,
                &mut cisnull,
            )
        };
        (cret, cbuf[..clen.max(0) as usize].to_vec(), csoft)
    });
    assert!(cret >= 0, "multirange_in/soft: oracle buffer overflow {dbg}");

    let mut esc = types_fmgr::ErrorSaveNode::new(true);
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_input);
    let mut fcinfo = LocalFcinfo::<3>::fresh(0);
    // SAFETY: mcx and the node both outlive this single call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    fcinfo.context = esc.fm_node_ptr();
    fcinfo.set_arg(0, Datum::from_usize(cs.as_ptr() as usize));
    fcinfo.set_arg(1, Datum::from_u32(PINS[t].mltrngtypid.into()));
    fcinfo.set_arg(2, Datum::from_i32(-1));
    let rres = mb::fc_multirange_in(Some(&mut fl), &mut fcinfo);

    let c_occurred = csoft != 0;
    match &rres {
        Err(e) => {
            let rc = err_class(e);
            assert!(
                !c_occurred && cret == rc,
                "multirange_in/soft HARD-ERROR DIVERGENCE {dbg}: C=(ret {cret}, soft {csoft}) \
                 Rust=hard err {rc} ({})",
                e.message
            );
            return;
        }
        Ok(_) => assert!(
            cret == 0,
            "multirange_in/soft DIVERGENCE {dbg}: C hard err {cret} vs Rust Ok"
        ),
    }
    let r_occurred = esc.ctx.error_occurred();
    assert!(
        r_occurred == c_occurred,
        "multirange_in/soft OCCURRED DIVERGENCE {dbg}: C soft_class={csoft} Rust={r_occurred}"
    );
    if r_occurred {
        bump(|st| st.soft_captured_inc());
        let rc = esc.ctx.error().map(err_class).unwrap_or(98);
        assert!(rc == csoft, "multirange_in/soft CLASS DIVERGENCE {dbg}: C={csoft} Rust={rc}");
    } else {
        // A valid literal must serialize identically under soft mode. Uses the
        // tie-aware comparator: the D1 value-equal-representative carve applies
        // here exactly as it does in hard mode.
        let out = FcOut { result: rres, isnull: fcinfo.isnull };
        compare_mr_image("multirange_in/soft", t, cret, &cimg, &out, mcx, &dbg);
    }

    // soft/hard verdict agreement, each side independently.
    let (hret, _) = IMG_BUF.with(|b| {
        let mut hbuf = b.borrow_mut();
        let mut hlen = 0i32;
        let hret = unsafe {
            pg_diff_mr_in(t as i32, cs.as_ptr(), hbuf.as_mut_ptr(), &mut hlen, BIGCAP as i32)
        };
        (hret, hlen)
    });
    assert!(
        (hret != 0) == c_occurred,
        "multirange_in SOFT/HARD DISAGREEMENT (C) {dbg}: hard {hret} vs soft {csoft}"
    );
    let mut fl2 = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_input);
    let rhard = fc_call(
        mb::fc_multirange_in,
        Some(&mut fl2),
        mcx,
        [
            Some(Datum::from_usize(cs.as_ptr() as usize)),
            Some(Datum::from_u32(PINS[t].mltrngtypid.into())),
            Some(Datum::from_i32(-1)),
        ],
    );
    assert!(
        rhard.result.is_err() == r_occurred,
        "multirange_in SOFT/HARD DISAGREEMENT (Rust) {dbg}: hard err={} vs soft {r_occurred}",
        rhard.result.is_err()
    );
}

fn arm_text_io(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>, soft: bool) {
    if payload.len() > TEXT_LIT_CAP || payload.contains(&0) {
        return;
    }
    let Ok(cs) = CString::new(payload) else { return };
    let dbg = format!("t={t} in={:?}", String::from_utf8_lossy(payload));
    if soft {
        arm_text_in_soft(t, payload, &cs, mcx);
        // then fall through and run the same literal in hard mode, so the
        // soft/hard verdict-agreement plane compares two real executions.
    }

    // multirange_in: image + errclass
    let img = IMG_BUF.with(|b| {
        let mut cbuf = b.borrow_mut();
        let mut clen = 0i32;
        let cret = unsafe {
            pg_diff_mr_in(t as i32, cs.as_ptr(), cbuf.as_mut_ptr(), &mut clen, BIGCAP as i32)
        };
        let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_input);
        let r = fc_call(
            mb::fc_multirange_in,
            Some(&mut fl),
            mcx,
            [
                Some(Datum::from_usize(cs.as_ptr() as usize)),
                Some(Datum::from_u32(PINS[t].mltrngtypid.into())),
                Some(Datum::from_i32(-1)),
            ],
        );
        // TIE-REACHABLE arm: the literal carries user-chosen numeric bounds
        // (e.g. `2` and `2.0000`), so nummultirange canonicalization can select
        // different value-equal representatives (FINDINGS D1).
        compare_mr_image("multirange_in", t, cret, &cbuf[..clen as usize], &r, mcx, &dbg);
        r.result.ok().map(|d| datum_varlena_bytes(d).to_vec())
    });
    let Some(img) = img else { return };

    // multirange_out roundtrip over the agreed image
    TEXT_BUF.with(|b| {
        let mut ctxt = b.borrow_mut();
        let mut colen = 0i32;
        let cret2 = unsafe {
            pg_diff_mr_out(img.as_ptr(), ctxt.as_mut_ptr(), &mut colen, BIGCAP as i32)
        };
        let mut flo = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_output);
        let ro = fc_call(
            mb::fc_multirange_out,
            Some(&mut flo),
            mcx,
            [Some(Datum::from_usize(img.as_ptr() as usize))],
        );
        assert!(cret2 >= 0, "multirange_out: oracle buffer overflow {dbg}");
        match &ro.result {
            Ok(od) => {
                assert!(cret2 == 0, "multirange_out DIVERGENCE {dbg}: C err {cret2} vs Rust Ok");
                let rtxt = datum_cstring_bytes(*od);
                let ctext: &[u8] = unsafe {
                    core::slice::from_raw_parts(ctxt.as_ptr() as *const u8, colen as usize)
                };
                assert!(
                    rtxt == ctext,
                    "multirange_out DIVERGENCE {dbg}: C={:?} Rust={:?}",
                    String::from_utf8_lossy(ctext),
                    String::from_utf8_lossy(rtxt)
                );
            }
            Err(e) => {
                let rc = err_class(e);
                assert!(
                    cret2 == rc,
                    "multirange_out DIVERGENCE {dbg}: C err {cret2} vs Rust {rc}"
                );
            }
        }
    });
}

/// Wire range_count above which the binary-io arm stops comparing.
///
/// PREALLOCATION CARVE (narrow, documented; the P2 finding of this lane).
/// multirange_recv reads range_count off the wire and preallocates range_count
/// pointers BEFORE validating the rest of the message — C does this too
/// (`palloc(range_count * sizeof(RangeType *))`), so the ORDERING is C-parity,
/// not a defect. What differs is only what each allocator does with an absurd
/// size: C's palloc succeeds for anything under MaxAllocSize (and the oracle's
/// arena succeeds regardless) while PgVec's fallible reserve fails and surfaces
/// an alloc-size error. That is a resource surface, not a value surface, and it
/// is recorded separately as P2. Counts up to this bound keep the whole
/// wire-parsing surface — element lengths, truncation, the zero-length element
/// that was P1 — under full comparison.
const RECV_COUNT_CARVE: u32 = 4096;

fn arm_binary_io(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    if payload.len() > 512 {
        return;
    }
    if payload.len() >= 4 {
        let count = u32::from_be_bytes(payload[..4].try_into().unwrap());
        if count > RECV_COUNT_CARVE {
            return;
        }
    }
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret = unsafe {
        pg_diff_mr_recv(
            t as i32,
            payload.as_ptr(),
            payload.len() as i32,
            cbuf.as_mut_ptr(),
            &mut clen,
            OUTCAP as i32,
        )
    };
    let mut buf = stringinfo::StringInfo::new_in(mcx).expect("stringinfo");
    buf.append_bytes(payload).expect("append");
    let mut fl = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_receive);
    let r = fc_call(
        mb::fc_multirange_recv,
        Some(&mut fl),
        mcx,
        [
            Some(Datum::from_usize(&mut buf as *mut _ as usize)),
            Some(Datum::from_u32(PINS[t].mltrngtypid.into())),
            Some(Datum::from_i32(-1)),
        ],
    );
    let dbg = format!("t={t} wire={payload:02x?}");
    // TIE-REACHABLE arm: wire numeric bounds are user-chosen, same as text io.
    compare_mr_image("multirange_recv", t, cret, &cbuf[..clen as usize], &r, mcx, &dbg);

    let Ok(d) = &r.result else { return };
    let img = datum_varlena_bytes(*d).to_vec();
    let mut wbuf = vec![0u8; OUTCAP];
    let mut wlen = 0i32;
    let cret2 =
        unsafe { pg_diff_mr_send(img.as_ptr(), wbuf.as_mut_ptr(), &mut wlen, OUTCAP as i32) };
    let mut fls = io_flinfo(t, lsyscache::IOFuncSelector::IOFunc_send);
    let rs = fc_call(
        mb::fc_multirange_send,
        Some(&mut fls),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    assert!(cret2 >= 0, "multirange_send: oracle buffer overflow {dbg}");
    match &rs.result {
        Ok(sd) => {
            assert!(cret2 == 0, "multirange_send DIVERGENCE {dbg}: C err {cret2} vs Rust Ok");
            let rb = datum_varlena_bytes(*sd);
            // bytea result: skip the 4-byte varlena header
            assert!(
                rb[4..] == wbuf[..wlen as usize],
                "multirange_send DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
                &wbuf[..wlen as usize],
                &rb[4..]
            );
        }
        Err(e) => {
            let rc = err_class(e);
            assert!(cret2 == rc, "multirange_send DIVERGENCE {dbg}: C err {cret2} vs Rust {rc}");
        }
    }
}

fn arm_ctors(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    match rd.u8() % 4 {
        // constructor0: the niladic empty multirange
        0 => {
            let mut cbuf = vec![0u8; OUTCAP];
            let mut clen = 0i32;
            let cret = unsafe {
                pg_diff_mr_ctor(
                    t as i32,
                    0,
                    core::ptr::null(),
                    core::ptr::null(),
                    0,
                    cbuf.as_mut_ptr(),
                    &mut clen,
                    OUTCAP as i32,
                )
            };
            let mut fl = ops_flinfo(t);
            let r = fc_call::<0>(mb::fc_multirange_constructor0, Some(&mut fl), mcx, []);
            // empty multirange (count 0): no bounds, tie fallback cannot fire.
            compare_mr_image(
                "multirange_constructor0",
                t,
                cret,
                &cbuf[..clen as usize],
                &r,
                mcx,
                &format!("t={t}"),
            );
        }
        // constructor1: a single range, plus the NULL-member error arm
        1 => {
            let argnull = rd.u8() & 1 == 1;
            let Some(rimg) = decode_range(t, &mut rd, mcx) else { return };
            let mut cbuf = vec![0u8; OUTCAP];
            let mut clen = 0i32;
            let cret = unsafe {
                pg_diff_mr_ctor(
                    t as i32,
                    1,
                    rimg.as_ptr(),
                    core::ptr::null(),
                    i32::from(argnull),
                    cbuf.as_mut_ptr(),
                    &mut clen,
                    OUTCAP as i32,
                )
            };
            let mut fl = ops_flinfo(t);
            let arg = if argnull { None } else { Some(Datum::from_usize(rimg.as_ptr() as usize)) };
            let r = fc_call(mb::fc_multirange_constructor1, Some(&mut fl), mcx, [arg]);
            let dbg = format!("t={t} argnull={argnull}");
            if argnull {
                // SQLSTATE CARVE on ONE defensive arm, verdict-only compare.
                // C's null-member guard is `elog(ERROR, ...)` (XX000, class 99)
                // — multirangetypes.c multirange_constructor1, under the comment
                // "This check should be guaranteed by our signature, but let's
                // do it just in case" — while pgrust raises 22004
                // (ERRCODE_NULL_VALUE_NOT_ALLOWED, class 13). Both are the same
                // defensive refusal and NEITHER is SQL-reachable: the builtin is
                // registered strict, so fmgr never delivers a NULL here. The arm
                // is still driven (the shipped line executes and is covered) but
                // only the error VERDICT is compared, because the class
                // difference is a conformance nit on an unreachable arm rather
                // than a behavioral divergence. Recorded as an exception row.
                assert!(cret > 0, "multirange_constructor1 DIVERGENCE {dbg}: C ok on NULL member");
                assert!(
                    r.result.is_err(),
                    "multirange_constructor1 DIVERGENCE {dbg}: Rust ok on NULL member"
                );
            } else {
                // single range from decode_range (integer-minted): no tie.
                compare_mr_image(
                    "multirange_constructor1",
                    t,
                    cret,
                    &cbuf[..clen as usize],
                    &r,
                    mcx,
                    &dbg,
                );
            }
        }
        // constructor2 over the variadic array = THE normalization kernel
        2 => {
            let _ = agreed_from_payload(t, &mut rd, mcx);
        }
        // constructor2 error arms: NULL argument / NULL member / multidim /
        // wrong element type
        _ => {
            let sel = rd.u8();
            let multidim = sel & 1 == 1;
            let wrong_elem = sel & 2 == 2;
            // Only the NULL-MEMBER arm is driven. The two NULL paths look
            // alike but are NOT both reachable:
            //   * argisnull(0) — multirange_constructor2 is proisstrict=t
            //     (ground-truthed on postgres:18.3), so fmgr returns SQL NULL
            //     without entering the body: `select int4multirange(NULL::int4range)`
            //     yields NULL, not an error. C's own comment says as much
            //     ("should be guaranteed by our signature, but let's do it just
            //     in case") and its arm is a bare elog, i.e. XX000, where pgrust
            //     raises 22004. Driving it FABRICATED a state real PG cannot
            //     produce and reported that sqlstate difference as a divergence.
            //     Excepted instead: see phase1-exceptions.tsv.
            //   * a NULL MEMBER inside a non-null array IS reachable through
            //     the variadic form — `select int4multirange('[1,2)', NULL)`
            //     raises 22004 on 18.3 — and both sides agree there.
            let arg_null = false;
            let member_null = sel & 8 == 8;
            let Some(rimg) = decode_range(t, &mut rd, mcx) else { return };
            let elemtype = if wrong_elem { INT4OID } else { PINS[t].rngtypid };
            let ndim = if multidim { 2 } else { 1 };
            let null_at: &[usize] = if member_null { &[0] } else { &[] };
            // A SHORT-form member is the real on-disk shape and reaches the
            // member-expand arm; only meaningful without a null bitmap and at
            // ndim 1.
            let short_member = sel & 0x10 == 0x10 && !member_null && ndim == 1;
            let arr = match if short_member {
                build_range_array_short(&[rimg.clone()], elemtype)
            } else {
                None
            } {
                Some(a) => {
                    bump(|st| st.short_member_inc());
                    a
                }
                None => build_range_array_nulls(&[rimg], elemtype, ndim, null_at),
            };
            if arg_null {
                bump(|st| st.null_arg_inc());
            }
            if member_null {
                bump(|st| st.null_member_inc());
            }
            let mut cbuf = vec![0u8; OUTCAP];
            let mut clen = 0i32;
            let cret = unsafe {
                pg_diff_mr_ctor(
                    t as i32,
                    1,
                    core::ptr::null(),
                    arr.as_ptr(),
                    i32::from(arg_null),
                    cbuf.as_mut_ptr(),
                    &mut clen,
                    OUTCAP as i32,
                )
            };
            let mut fl = ops_flinfo(t);
            let a0 = if arg_null { None } else { Some(Datum::from_usize(arr.as_ptr() as usize)) };
            let r = fc_call(mb::fc_multirange_constructor2, Some(&mut fl), mcx, [a0]);
            compare_mr_image(
                "multirange_constructor2_err",
                t,
                cret,
                &cbuf[..clen as usize],
                &r,
                mcx,
                &format!(
                    "t={t} multidim={multidim} wrong_elem={wrong_elem} \
                     arg_null={arg_null} member_null={member_null}"
                ),
            );
        }
    }
}

fn arm_accessors(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let mut lo = vec![0u8; OUTCAP];
    let mut up = vec![0u8; OUTCAP];
    let (mut ll, mut ul, mut ln, mut un) = (0i32, 0i32, 0i32, 0i32);
    let mut bools = [0u8; 5];
    let cret = unsafe {
        pg_diff_mr_accessors(
            t as i32,
            img.as_ptr(),
            lo.as_mut_ptr(),
            &mut ll,
            &mut ln,
            up.as_mut_ptr(),
            &mut ul,
            &mut un,
            bools.as_mut_ptr(),
            OUTCAP as i32,
        )
    };
    assert!(cret >= 0, "mr_accessors: oracle buffer overflow t={t}");
    let dbg = format!("t={t} img={:02x?}", &img[..img.len().min(48)]);

    let accs: [(&str, PGFunction, i32, i32, &Vec<u8>); 2] = [
        ("multirange_lower", mb::fc_multirange_lower, ll, ln, &lo),
        ("multirange_upper", mb::fc_multirange_upper, ul, un, &up),
    ];
    for (name, fc, clen, cnull, cbuf) in accs {
        let mut fl = ops_flinfo(t);
        let r = fc_call(fc, Some(&mut fl), mcx, [Some(Datum::from_usize(img.as_ptr() as usize))]);
        match &r.result {
            Ok(d) => {
                assert!(cret == 0, "{name} DIVERGENCE {dbg}: C err {cret} vs Rust Ok");
                assert!(
                    r.isnull == (cnull != 0),
                    "{name} DIVERGENCE {dbg}: null C={} Rust={}",
                    cnull != 0,
                    r.isnull
                );
                if !r.isnull {
                    if PINS[t].typbyval {
                        let cv = i64::from_le_bytes(cbuf[..8].try_into().unwrap());
                        let (rv, cv) = if PINS[t].typlen == 4 {
                            (d.as_i32() as i64, cv as i32 as i64)
                        } else {
                            (d.as_i64(), cv)
                        };
                        assert!(rv == cv, "{name} DIVERGENCE {dbg}: C={cv} Rust={rv}");
                    } else {
                        let rb = datum_varlena_bytes(*d);
                        assert!(
                            rb == &cbuf[..clen as usize],
                            "{name} DIVERGENCE {dbg}: C={:02x?} Rust={rb:02x?}",
                            &cbuf[..clen as usize]
                        );
                    }
                }
            }
            Err(e) => {
                let rc = err_class(e);
                assert!(cret == rc, "{name} DIVERGENCE {dbg}: C err {cret} vs Rust err {rc}");
            }
        }
    }

    let boolfns: [(&str, PGFunction); 5] = [
        ("isempty", mb::fc_multirange_empty),
        ("lower_inc", mb::fc_multirange_lower_inc),
        ("upper_inc", mb::fc_multirange_upper_inc),
        ("lower_inf", mb::fc_multirange_lower_inf),
        ("upper_inf", mb::fc_multirange_upper_inf),
    ];
    for (i, (name, fc)) in boolfns.into_iter().enumerate() {
        let mut fl = ops_flinfo(t);
        let r = fc_call(fc, Some(&mut fl), mcx, [Some(Datum::from_usize(img.as_ptr() as usize))]);
        compare_scalar(name, cret, bools[i] as i32, &r, false, &dbg);
    }
}

fn arm_mr_ops(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img1) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let Some(img2) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let mut res = [0i32; 15];
    let cret = unsafe { pg_diff_mr_ops(img1.as_ptr(), img2.as_ptr(), res.as_mut_ptr()) };
    let fns: [(&str, PGFunction); 15] = [
        ("multirange_eq", mb::fc_multirange_eq),
        ("multirange_ne", mb::fc_multirange_ne),
        ("multirange_lt", mb::fc_multirange_lt),
        ("multirange_le", mb::fc_multirange_le),
        ("multirange_ge", mb::fc_multirange_ge),
        ("multirange_gt", mb::fc_multirange_gt),
        ("multirange_cmp", mb::fc_multirange_cmp),
        ("mr_overlaps_mr", mb::fc_multirange_overlaps_multirange),
        ("mr_contains_mr", mb::fc_multirange_contains_multirange),
        ("mr_contained_by_mr", mb::fc_multirange_contained_by_multirange),
        ("mr_adjacent_mr", mb::fc_multirange_adjacent_multirange),
        ("mr_before_mr", mb::fc_multirange_before_multirange),
        ("mr_after_mr", mb::fc_multirange_after_multirange),
        ("mr_overleft_mr", mb::fc_multirange_overleft_multirange),
        ("mr_overright_mr", mb::fc_multirange_overright_multirange),
    ];
    let dbg = format!("t={t} n1={} n2={}", img1.len(), img2.len());
    for (i, (name, fc)) in fns.into_iter().enumerate() {
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
        compare_scalar(name, cret, res[i], &r, i == 6, &dbg);
    }
}

fn arm_range_ops(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(rimg) = decode_wide_range(t, &mut rd, mcx) else { return };
    let Some(mimg) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let mut res = [0i32; 16];
    let cret = unsafe { pg_diff_mr_range_ops(rimg.as_ptr(), mimg.as_ptr(), res.as_mut_ptr()) };
    // (name, fc, mr_first) — arg order follows the NAME, exactly as in C.
    let fns: [(&str, PGFunction, bool); 16] = [
        ("r_overlaps_mr", mb::fc_range_overlaps_multirange, false),
        ("mr_overlaps_r", mb::fc_multirange_overlaps_range, true),
        ("mr_contains_r", mb::fc_multirange_contains_range, true),
        ("r_contains_mr", mb::fc_range_contains_multirange, false),
        ("r_contained_by_mr", mb::fc_range_contained_by_multirange, false),
        ("mr_contained_by_r", mb::fc_multirange_contained_by_range, true),
        ("r_adjacent_mr", mb::fc_range_adjacent_multirange, false),
        ("mr_adjacent_r", mb::fc_multirange_adjacent_range, true),
        ("r_before_mr", mb::fc_range_before_multirange, false),
        ("mr_before_r", mb::fc_multirange_before_range, true),
        ("r_after_mr", mb::fc_range_after_multirange, false),
        ("mr_after_r", mb::fc_multirange_after_range, true),
        ("r_overleft_mr", mb::fc_range_overleft_multirange, false),
        ("mr_overleft_r", mb::fc_multirange_overleft_range, true),
        ("r_overright_mr", mb::fc_range_overright_multirange, false),
        ("mr_overright_r", mb::fc_multirange_overright_range, true),
    ];
    let dbg = format!("t={t} r={:02x?}", &rimg[..rimg.len().min(32)]);
    for (i, (name, fc, mr_first)) in fns.into_iter().enumerate() {
        let (a, b) = if mr_first { (&mimg, &rimg) } else { (&rimg, &mimg) };
        let mut fl = ops_flinfo(t);
        let r = fc_call(
            fc,
            Some(&mut fl),
            mcx,
            [
                Some(Datum::from_usize(a.as_ptr() as usize)),
                Some(Datum::from_usize(b.as_ptr() as usize)),
            ],
        );
        compare_scalar(name, cret, res[i], &r, false, &dbg);
    }
}

fn arm_elem(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let (v, num) = match t {
        0 => (rd.i32() as i64, None),
        1 => (rd.i64(), None),
        _ => {
            let n = (rd.u8() % 12) as usize + 1;
            let lit = rd.bytes(n).to_vec();
            let Some(b) = mint_numeric_lit(mcx, &lit) else { return };
            (0i64, Some(b))
        }
    };
    let numptr = num.as_ref().map_or(core::ptr::null(), |b| b.as_ptr());
    let (mut contains, mut contained) = (0i32, 0i32);
    let cret = unsafe { pg_diff_mr_elem(img.as_ptr(), v, numptr, &mut contains, &mut contained) };
    let elem = match &num {
        Some(b) => Datum::from_usize(b.as_ptr() as usize),
        None => Datum::from_i64(v),
    };
    let dbg = format!("t={t} v={v}");
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        mb::fc_multirange_contains_elem,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize)), Some(elem)],
    );
    compare_scalar("mr_contains_elem", cret, contains, &r, false, &dbg);
    let mut fl2 = ops_flinfo(t);
    let r2 = fc_call(
        mb::fc_elem_contained_by_multirange,
        Some(&mut fl2),
        mcx,
        [Some(elem), Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    compare_scalar("elem_contained_by_mr", cret, contained, &r2, false, &dbg);
}

fn arm_setops(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img1) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let Some(img2) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let fns: [(&str, PGFunction); 3] = [
        ("multirange_union", mb::fc_multirange_union),
        ("multirange_minus", mb::fc_multirange_minus),
        ("multirange_intersect", mb::fc_multirange_intersect),
    ];
    for (which, (name, fc)) in fns.into_iter().enumerate() {
        let mut cbuf = vec![0u8; OUTCAP];
        let mut clen = 0i32;
        let cret = unsafe {
            pg_diff_mr_setop(
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
        // set-op operands are integer-minted (agreed_from_payload -> decode_range),
        // so no value-equal-but-byte-different bound can arise and the tie
        // fallback never fires; routed through it for uniform strength.
        compare_mr_image(name, t, cret, &cbuf[..clen as usize], &r, mcx, &format!("t={t}"));
    }
}

fn arm_hash(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let seed = rd.i64() as u64;
    let (mut ch, mut che) = (0u32, 0u64);
    // AUDIT (value-vs-byte tie): hashing is over BYTES, so two value-equal
    // byte-different multiranges WOULD hash differently — but both sides hash
    // the SAME `img` (agreed_image over integer-minted ranges is byte-identical
    // C==Rust), so the input can carry no representation tie and the hashes
    // must match exactly. If a hash arm ever consumed a text/recv image (which
    // CAN tie), it would need the value-level path, not a byte hash compare.
    let cret = unsafe { pg_diff_mr_hash(img.as_ptr(), &mut ch, seed, &mut che) };
    let dbg = format!("t={t} seed={seed:#x}");
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        mb::fc_hash_multirange,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    match &r.result {
        Ok(d) => {
            assert!(cret == 0, "hash_multirange DIVERGENCE {dbg}: C err {cret} vs Rust Ok");
            let rh = d.as_u32();
            assert!(rh == ch, "hash_multirange DIVERGENCE {dbg}: C={ch:#x} Rust={rh:#x}");
        }
        Err(e) => {
            let rc = err_class(e);
            assert!(cret == rc, "hash_multirange DIVERGENCE {dbg}: C {cret} Rust {rc}");
        }
    }
    let mut fl2 = ops_flinfo(t);
    let r2 = fc_call(
        mb::fc_hash_multirange_extended,
        Some(&mut fl2),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize)), Some(Datum::from_i64(seed as i64))],
    );
    match &r2.result {
        Ok(d) => {
            assert!(cret == 0, "hash_multirange_ext DIVERGENCE {dbg}: C err {cret}");
            let rh = d.as_u64();
            assert!(rh == che, "hash_multirange_ext DIVERGENCE {dbg}: C={che:#x} Rust={rh:#x}");
        }
        Err(e) => {
            let rc = err_class(e);
            assert!(cret == rc, "hash_multirange_ext DIVERGENCE {dbg}: C {cret} Rust {rc}");
        }
    }
}

fn arm_merge(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let mut cbuf = vec![0u8; OUTCAP];
    let mut clen = 0i32;
    let cret =
        unsafe { pg_diff_mr_merge(img.as_ptr(), cbuf.as_mut_ptr(), &mut clen, OUTCAP as i32) };
    let mut fl = ops_flinfo(t);
    let r = fc_call(
        mb::fc_range_merge_from_multirange,
        Some(&mut fl),
        mcx,
        [Some(Datum::from_usize(img.as_ptr() as usize))],
    );
    // AUDIT (value-vs-byte tie): output is a single RANGE, not a multirange,
    // and range_merge does not canonicalize a tied multiset — it spans
    // lower-of-first..upper-of-last of an already-canonical input whose bounds
    // are integer-minted here. No value-equal-but-byte-different bound can
    // reach it, so byte-exact is correct and cannot mask a tie. (A dscale tie
    // on the RANGE surface, if it ever mattered, is rangetypes_diff's to own.)
    compare_image(
        "range_merge_from_multirange",
        cret,
        &cbuf[..clen as usize],
        &r,
        &format!("t={t}"),
    );
}

/// The support API the fmgr entries do not reach directly:
/// multirange_get_range(i) walks the item offset/length stride table, and
/// multirange_get_union_range spans first..last.
fn arm_internals(t: usize, payload: &[u8], mcx: mcx::Mcx<'_>) {
    let mut rd = Rd(payload, 0);
    let Some(img) = agreed_from_payload(t, &mut rd, mcx) else { return };
    let idx = rd.u8() as i32;
    let (mut count, mut is_empty) = (0u32, 0i32);
    let mut rbuf = vec![0u8; OUTCAP];
    let mut ubuf = vec![0u8; OUTCAP];
    let (mut rlen, mut ulen) = (0i32, 0i32);
    let cret = unsafe {
        pg_diff_mr_internals(
            t as i32,
            img.as_ptr(),
            idx,
            &mut count,
            &mut is_empty,
            rbuf.as_mut_ptr(),
            &mut rlen,
            ubuf.as_mut_ptr(),
            &mut ulen,
            OUTCAP as i32,
        )
    };
    assert!(cret >= 0, "mr_internals: oracle buffer overflow t={t}");
    let dbg = format!("t={t} idx={idx}");

    let rcount = mrt::multirange_count(&img);
    assert!(rcount == count, "multirange_count DIVERGENCE {dbg}: C={count} Rust={rcount}");
    let rempty = mrt::multirange_is_empty(&img);
    assert!(
        rempty == (is_empty != 0),
        "multirange_is_empty DIVERGENCE {dbg}: C={} Rust={rempty}",
        is_empty != 0
    );
    let mut mi = multirange_info(t);
    if count > 0 {
        let i = (idx as usize) % (count as usize);
        let got = mrt::multirange_get_range(mcx, &mut mi.rng, &img, i).expect("get_range");
        assert!(
            got[..] == rbuf[..rlen as usize],
            "multirange_get_range DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
            &rbuf[..rlen as usize],
            &got[..]
        );
    } else {
        bump(|st| st.union_range_empty_inc());
    }
    // union_range is compared for the EMPTY multirange too: it has a dedicated
    // make_empty_range arm (lib.rs:395) that the old `count == 0 => return`
    // guard made unreachable on both sides.
    let un = mrt::multirange_get_union_range(mcx, &mut mi.rng, &img).expect("union_range");
    assert!(
        un[..] == ubuf[..ulen as usize],
        "multirange_get_union_range DIVERGENCE {dbg}: C={:02x?} Rust={:02x?}",
        &ubuf[..ulen as usize],
        &un[..]
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ANTI-VACUITY GATE for the arms added when the coverage gaps were closed:
    /// soft-error mode, the NULL-MEMBER array arm, the short-form array member,
    /// and union_range over an EMPTY multirange. A new arm that silently never
    /// fires is worse than a known gap.
    #[test]
    fn gap_closing_arms_all_fire() {
        let _serial = crate::c_oracle_serial();
        if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
            // Foreign module owns the typcache env: drivers no-op (see the
            // entry guard), so arm-vacuity cannot be asserted in the shared
            // binary. Run `cargo test <module>` filtered for the real rail.
            return;
        }
        let b = (
            soft_mode_count(),
            soft_captured_count(),
            null_member_count(),
            union_range_empty_count(),
        );
        // soft-mode multirange_in: valid + malformed
        for lit in [&b"{[1,5),[10,20)}"[..], &b"{garbage}"[..], &b"{[5,1)}"[..]] {
            let mut v = vec![0u8, 0x81];
            v.extend_from_slice(lit);
            multirangetypes_diff(&v);
        }
        // ctor2 error arm: NULL member (0x08) and short-form member (0x10)
        for sel in [0x08u8, 0x10] {
            multirangetypes_diff(&[2, 0, 3, sel, 1, 5, 0, 0, 0, 0, 10, 0, 0, 0]);
        }
        // internals on an EMPTY multirange -> union_range's make_empty_range
        multirangetypes_diff(&[10, 0, 0, 0]);
        let a = (
            soft_mode_count(),
            soft_captured_count(),
            null_member_count(),
            union_range_empty_count(),
        );
        assert!(a.0 > b.0, "soft-mode arm never fired");
        assert!(a.1 > b.1, "soft-mode arm never CAPTURED a soft error");
        assert!(a.2 > b.2, "NULL-member array arm never fired");
        assert!(a.3 > b.3, "empty-multirange union_range arm never fired");
    }

    fn run(bytes: &[u8]) {
        multirangetypes_diff(bytes);
    }

    /// Every arm x every instantiation.
    #[test]
    fn all_arms_smoke() {
        let _serial = crate::c_oracle_serial();
        for sel in 0..11u8 {
            for typ in 0..3u8 {
                for pad in [0u8, 1, 7, 0x40, 0xff] {
                    run(&[sel, typ, pad, 2, 3, 5, 8, 13, 21, pad, 1, 4, 9, pad, 2, 6]);
                }
            }
        }
    }

    #[test]
    fn text_io_literals() {
        let _serial = crate::c_oracle_serial();
        for lit in [
            &b"{}"[..],
            b"{[1,2)}",
            b"{[1,2),[3,4)}",
            b"{[1,2),[2,3)}", // adjacent -> must merge
            b"{[1,5),[2,3)}", // nested -> must merge
            b"{[3,4),[1,2)}", // unsorted -> must sort
            b"{empty}",       // empty member -> must vanish
            b"{(,)}",         // unbounded
            b"{[1,2),[1,2)}", // duplicate
            b"{",             // malformed
            b"[1,2)",         // malformed: no braces
            b"{[1,2)",        // malformed: unterminated
            b"{\"[1,2)\"}",   // quoted member
        ] {
            for t in 0..3u8 {
                let mut v = vec![0u8, t];
                v.extend_from_slice(lit);
                run(&v);
            }
        }
    }


    /// D1: the numrange canonicalization tie. Value-equal-but-byte-different
    /// bounds in a nummultirange literal (`2` vs `2.0000`, dscale 0 vs 4) make
    /// C (unstable qsort) and pgrust (stable sort) keep different value-equal
    /// representatives. The comparator must NOT flag this, MUST take the gated
    /// value-level fallback, and the fallback counter must advance — proving
    /// the path is live and not silently short-circuited. The same shapes on
    /// int4/int8 multirange must NOT trip the fallback (value-equal is
    /// byte-equal for byval).
    #[test]
    fn numeric_representation_tie_D1() {
        let _serial = crate::c_oracle_serial();
        if syscache_seams::lookup_pg_type_typcache_shape::is_installed() {
            // Foreign module owns the typcache env: drivers no-op (see the
            // entry guard), so arm-vacuity cannot be asserted in the shared
            // binary. Run `cargo test <module>` filtered for the real rail.
            return;
        }
        let before = numeric_tie_fallback_count();
        // nummultirange (t=2) literals that ACTUALLY diverge: 3+ value groups
        // with dscale variety, ordered so C's unstable qsort keeps a different
        // representative than pgrust's stable sort (verified to fire). The last
        // is the verbatim fleet reproducer for D1.
        for lit in [
            &b"{[5,6),[3,4),[1,2),[5,6.0),[3,4.0),[1,2.0),[5,6.00),[3,4.00)}"[..],
            b"{[7,8),[7,8.0),[7,8.00),[1,2),[1,2.0),[9,10),[9,10.00)}",
            b"{[20,0204),[\t0,2),[1,2),[3,42),[\t1,2),[3,42),[\t1,2),[3,42),[3,4),[3,42\n),[3,44),[\t1,2),[1,2.0000)}",
        ] {
            let mut v = vec![0u8, 2];
            v.extend_from_slice(lit);
            run(&v);
        }
        assert!(
            numeric_tie_fallback_count() > before,
            "the D1 tie fallback did not fire — the value-level path is dead \
             (a byte-exact-only comparator would have panicked on the divergence instead)"
        );
        // byval instantiations: the same literal shapes must never fallback.
        let mid = numeric_tie_fallback_count();
        for t in [0u8, 1] {
            for lit in [&b"{[1,2),[1,2)}"[..], b"{[0,10),[0,10),[3,4)}", b"{[3,4),[1,2)}"] {
                let mut v = vec![0u8, t];
                v.extend_from_slice(lit);
                run(&v);
            }
        }
        assert_eq!(
            numeric_tie_fallback_count(),
            mid,
            "a byval instantiation took the numeric-tie fallback — it must not"
        );
    }

    /// WITNESS PAIRS (skill obligation): inputs differing in EXACTLY one field
    /// of one range, both orders — the only way a per-field contribution to a
    /// canonicalized image or a comparison verdict is ever witnessed. Line
    /// coverage and exec volume cannot detect their absence.
    #[test]
    fn single_field_witness_pairs() {
        let _serial = crate::c_oracle_serial();
        // arm 2 sub-selector 2 = the constructor2 canonicalize path.
        // after [sel,typ]: [subsel][n][flags,lo,hi][flags,lo,hi]...
        let base: [u8; 9] = [2, 0, 2, 2, 0x02, 3, 7, 0x02, 9];
        for t in 0..3u8 {
            for field in 3..base.len() {
                for delta in [1u8, 0xff] {
                    let mut a = base;
                    a[1] = t;
                    let mut b = a;
                    b[field] = b[field].wrapping_add(delta);
                    // both orders: the pair must be witnessed each way
                    run(&a);
                    run(&b);
                    run(&b);
                    run(&a);
                }
            }
        }
        // adjacency boundary: [3,7)+[7,9) merge; [3,7)+[8,9) stay separate.
        for hi in [6u8, 7, 8] {
            for t in 0..3u8 {
                run(&[2, t, 2, 2, 0x02, 3, 7, 0x02, hi, 9]);
            }
        }
        // one-field deltas on the mr x mr operand pair
        for t in 0..3u8 {
            let pair: [u8; 16] = [4, t, 2, 0x02, 1, 4, 0x02, 6, 9, 2, 0x02, 3, 7, 0x02, 8, 11];
            for field in 2..pair.len() {
                let mut b = pair;
                b[field] = b[field].wrapping_add(1);
                run(&pair);
                run(&b);
            }
        }
    }

    #[test]
    fn operator_bundles() {
        let _serial = crate::c_oracle_serial();
        for t in 0..3u8 {
            // two 2-range multiranges: overlapping / disjoint / equal
            run(&[4, t, 2, 0x02, 1, 4, 0x02, 6, 9, 2, 0x02, 3, 7, 0x02, 8, 11]);
            run(&[4, t, 2, 0x02, 1, 2, 0x02, 3, 4, 2, 0x02, 5, 6, 0x02, 7, 8]);
            run(&[4, t, 1, 0x02, 1, 9, 1, 0x02, 1, 9]);
            // range x multirange
            run(&[5, t, 0x02, 2, 5, 2, 0x02, 1, 4, 0x02, 6, 9]);
        }
    }

    #[test]
    fn setops_hash_merge_internals() {
        let _serial = crate::c_oracle_serial();
        for t in 0..3u8 {
            run(&[7, t, 2, 0x02, 1, 5, 0x02, 7, 9, 2, 0x02, 3, 8, 0x02, 10, 12]);
            run(&[8, t, 2, 0x02, 1, 5, 0x02, 7, 9, 1, 2, 3, 4, 5, 6, 7, 8]);
            run(&[9, t, 3, 0x02, 1, 2, 0x02, 4, 5, 0x02, 7, 8]);
            run(&[10, t, 3, 0x02, 1, 2, 0x02, 4, 5, 0x02, 7, 8, 1]);
            run(&[3, t, 3, 0x02, 1, 2, 0x02, 4, 5, 0x02, 7, 8]);
            run(&[6, t, 2, 0x02, 1, 5, 0x02, 7, 9, 3, 0, 0, 0]);
        }
    }

    #[test]
    fn ctor_arms() {
        let _serial = crate::c_oracle_serial();
        for t in 0..3u8 {
            run(&[2, t, 0]); // constructor0
            run(&[2, t, 1, 0, 0x02, 1, 5]); // constructor1
            run(&[2, t, 1, 1, 0x02, 1, 5]); // constructor1, NULL member
            run(&[2, t, 3, 1, 0, 0x02, 1, 5]); // multidimensional array
            run(&[2, t, 3, 0, 1, 0x02, 1, 5]); // wrong element type
            run(&[2, t, 2, 0]); // constructor2 over an EMPTY array
        }
    }

    #[test]
    fn binary_io_wire() {
        let _serial = crate::c_oracle_serial();
        for t in 0..3u8 {
            run(&[1, t, 0, 0, 0, 0]); // range_count = 0
            run(&[1, t, 0, 0]); // truncated count
            run(&[1, t, 0, 0, 0, 1, 0, 0, 0, 0]); // zero-length element (P1 shape)
            run(&[1, t, 0, 0, 0, 1, 0xEB, 0xff, 0xff, 0xff]); // oversized element
        }
    }
}

#[cfg(test)]
mod carrier_tests {
    use super::*;

    /// The rettype carriers are parallel arrays to PINS; a reordering of
    /// either would silently hand the wrong rettype to every constructor.
    #[test]
    fn rettype_carriers_track_pins() {
        let _serial = crate::c_oracle_serial();
        for t in 0..3 {
            assert_eq!(MR_RETTYPE[t].rettype, PINS[t].mltrngtypid, "MR_RETTYPE[{t}]");
            assert_eq!(RNG_RETTYPE[t].rettype, PINS[t].rngtypid, "RNG_RETTYPE[{t}]");
            assert!(MR_RETTYPE[t].argtypes.is_empty());
            assert!(RNG_RETTYPE[t].argtypes.is_empty());
        }
    }
}
