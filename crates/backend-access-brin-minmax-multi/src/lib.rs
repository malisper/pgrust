//! Idiomatic port of `src/backend/access/brin/brin_minmax_multi.c`
//! (PostgreSQL 18.3).
//!
//! A variant of the minmax opclass where the summary is composed of multiple
//! smaller intervals, so outliers do not blow up the whole range. The summary
//! tracks up to `values_per_range` boundary values (regular ranges use two,
//! collapsed single-point ranges use one); when the insert buffer fills, the
//! closest intervals (by the "distance" support procedure) are merged. The
//! buffer is oversized (`MINMAX_BUFFER_FACTOR`x the target) and compacted to
//! the requested number of values once, at `brin_form_tuple` time, through the
//! `bv_serialize` callback — this is the design, NOT eager-serialize.
//!
//! Reached by the BRIN AM (`brin.c`, unported) through the
//! `backend-access-brin-entry-seams` opclass-dispatch seams, which the sibling
//! `backend-access-brin-minmax` crate installs (it is the single dispatch
//! installer for all built-in opclasses). That crate also registers this
//! crate's distance functions as fmgr builtins and installs the `brin_serialize`
//! seam over [`brin_minmax_multi_serialize`].
//!
//! ## Carrier decision (S4)
//!
//! The live in-memory summary ([`types_brin::MinmaxMultiRanges`]) is kept in
//! `column.bv_mem_value` across `add_value` calls (C's `bv_mem_value` Datum of
//! an expanded object). The per-attribute procinfo cache
//! ([`types_brin::MinmaxMultiOpaque`]) lives in `bd_info[..].oi_opaque`; each
//! cached `FmgrInfo` is reduced to the resolved function's `Oid` (the BRIN
//! fmgr-call seam re-resolves by OID). cmp/strategy and distance calls dispatch
//! by OID through `function_call2_coll_datum` (the canonical-`Datum` lane that
//! carries by-value scalars AND by-reference byte images, as the inclusion
//! opclass does).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_brin::{
    BrinDesc, BrinMemValue, BrinOpcInfo, BrinValues, MinmaxMultiOpaque, OpaqueOpcInfo,
};
use types_core::primitive::{AttrNumber, Oid};
use types_error::error::{
    ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
};
use types_error::PgResult;
use types_scan::scankey::ScanKeyData;
use types_storage::bufpage::MaxHeapTuplesPerPage;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_error::ereport;

use backend_access_index_indexam_seams as indexam;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_fmgr_fmgr_seams as fmgr;

// ---------------------------------------------------------------------------
// stratnum.h — B-tree strategy numbers (brin_minmax_multi.c uses the btree set).
// ---------------------------------------------------------------------------

const BTLessStrategyNumber: u16 = 1;
const BTLessEqualStrategyNumber: u16 = 2;
const BTEqualStrategyNumber: u16 = 3;
const BTGreaterEqualStrategyNumber: u16 = 4;
const BTGreaterStrategyNumber: u16 = 5;
const BTMaxStrategyNumber: u16 = 5;

/// `InvalidOid` (postgres_ext.h).
const INVALID_OID: Oid = 0;

// ---------------------------------------------------------------------------
// Constants (brin_minmax_multi.c:92-127).
// ---------------------------------------------------------------------------

/// `PROCNUM_DISTANCE` (brin_minmax_multi.c:93): required, distance between values.
const PROCNUM_DISTANCE: u16 = 11;
/// `PROCNUM_BASE` (brin_minmax_multi.c:99): subtracted from procnum to index the
/// MinmaxMultiOpaque arrays.
const PROCNUM_BASE: u16 = 11;

/// `MINMAX_BUFFER_FACTOR` (brin_minmax_multi.c:106).
const MINMAX_BUFFER_FACTOR: i32 = 10;
/// `MINMAX_BUFFER_MIN` (brin_minmax_multi.c:107).
const MINMAX_BUFFER_MIN: i32 = 256;
/// `MINMAX_BUFFER_MAX` (brin_minmax_multi.c:108).
const MINMAX_BUFFER_MAX: i32 = 8192;
/// `MINMAX_BUFFER_LOAD_FACTOR` (brin_minmax_multi.c:109).
const MINMAX_BUFFER_LOAD_FACTOR: f64 = 0.5;

/// `MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE` (brin_minmax_multi.c:127).
const MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE: i32 = 32;

/// `BrinGetPagesPerRange` trim-gap default (`BRIN_DEFAULT_PAGES_PER_RANGE`,
/// brin.h): the per-attribute opclass options + reloptions are not yet threaded
/// through the relcache trim, so the dispatcher passes this default — the same
/// convention as `brin_bloom`. See the module note in `backend-access-brin-bloom`.
pub const PAGES_PER_RANGE_DEFAULT: u32 = 128;

/// `PG_BRIN_MINMAX_MULTI_SUMMARYOID` (pg_type.dat): the summary pseudo-type Oid.
const PG_BRIN_MINMAX_MULTI_SUMMARYOID: Oid = 4601;

// ---------------------------------------------------------------------------
// Built-in support-procedure OIDs (pg_proc.dat) — the opclass entry points.
// ---------------------------------------------------------------------------

/// `brin_minmax_multi_opcinfo` (pg_proc.dat oid 4616).
pub const F_BRIN_MINMAX_MULTI_OPCINFO: Oid = 4616;
/// `brin_minmax_multi_add_value` (pg_proc.dat oid 4617).
pub const F_BRIN_MINMAX_MULTI_ADD_VALUE: Oid = 4617;
/// `brin_minmax_multi_consistent` (pg_proc.dat oid 4618).
pub const F_BRIN_MINMAX_MULTI_CONSISTENT: Oid = 4618;
/// `brin_minmax_multi_union` (pg_proc.dat oid 4619).
pub const F_BRIN_MINMAX_MULTI_UNION: Oid = 4619;

// ---------------------------------------------------------------------------
// Datum <-> bool / float8 / word helpers (postgres.h / fmgr seam edge).
// ---------------------------------------------------------------------------

/// `DatumGetBool(X)` == `((bool) ((X) & 1))` — the low bit of an fmgr result.
#[inline]
fn datum_get_bool(d: &Datum) -> bool {
    (d.as_usize() & 1) != 0
}

/// `DatumGetFloat8(X)` — reinterpret the by-value fmgr result word as a double.
#[inline]
fn datum_get_float8(d: &Datum) -> f64 {
    f64::from_bits(d.as_usize() as u64)
}

/// `FunctionCall2Coll(cmpFn, colloid, a, b)` over the canonical-`Datum` lane,
/// returning the boolean comparison result. `function_id` is the cached
/// comparison-procedure OID; the args may be by-value or by-reference.
fn call_strategy2<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    colloid: Oid,
    a: &Datum<'mcx>,
    b: &Datum<'mcx>,
) -> PgResult<bool> {
    let r =
        fmgr::function_call2_coll_datum::call(mcx, function_id, colloid, a.clone(), b.clone())?;
    Ok(datum_get_bool(&r))
}

/// `FunctionCall2Coll(distanceFn, colloid, a, b)` over the canonical-`Datum`
/// lane, returning the float8 distance result.
fn call_distance2<'mcx>(
    mcx: Mcx<'mcx>,
    function_id: Oid,
    colloid: Oid,
    a: &Datum<'mcx>,
    b: &Datum<'mcx>,
) -> PgResult<f64> {
    let r =
        fmgr::function_call2_coll_datum::call(mcx, function_id, colloid, a.clone(), b.clone())?;
    Ok(datum_get_float8(&r))
}

/// Borrow the [`MinmaxMultiOpaque`] cache out of
/// `bdesc.bd_info[attno - 1].oi_opaque` (created by [`brin_minmax_multi_opcinfo`]).
fn minmax_multi_opaque<'a>(bdesc: &'a BrinDesc<'_>, attno: AttrNumber) -> &'a MinmaxMultiOpaque {
    match bdesc.bd_info[(attno - 1) as usize].oi_opaque.as_ref() {
        Some(OpaqueOpcInfo::MinmaxMulti(o)) => o,
        _ => panic!("brin_minmax_multi: oi_opaque is not a MinmaxMultiOpaque cache"),
    }
}

// ===========================================================================
// minmax_multi_get_procinfo / minmax_multi_get_strategy_procinfo
//   (brin_minmax_multi.c:2862 / 2898)
// ===========================================================================

/// `minmax_multi_get_procinfo(bdesc, attno, procnum)` (brin_minmax_multi.c:2862):
/// cache and return the support-procedure OID for `procnum`, raising
/// `"invalid opclass definition"` when the opclass lacks it.
fn minmax_multi_get_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: AttrNumber,
    procnum: u16,
) -> PgResult<Oid> {
    let opaque = minmax_multi_opaque(bdesc, attno);
    let basenum = (procnum - PROCNUM_BASE) as usize;

    if opaque.extra_procinfos[basenum].get() == INVALID_OID {
        // index_getprocinfo(bd_index, attno, procnum) — the registered support
        // procedure's FmgrInfo; its fn_oid is InvalidOid when the opclass is
        // missing the proc (C tests RegProcedureIsValid(index_getprocid(...))).
        let finfo = indexam::index_getprocinfo::call(&bdesc.bd_index, attno, procnum)?;
        if finfo.fn_oid != INVALID_OID {
            opaque.extra_procinfos[basenum].set(finfo.fn_oid);
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg_internal("invalid opclass definition")
                .errdetail_internal(format!(
                    "The operator class is missing support function {procnum} for column {attno}."
                ))
                .into_error());
        }
    }

    Ok(opaque.extra_procinfos[basenum].get())
}

/// `minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, strategynum)`
/// (brin_minmax_multi.c:2898): cache and return the comparison-procedure OID for
/// the given strategy, invalidating the whole cache when `subtype` changes.
///
/// Mirrors `minmax_get_strategy_procinfo`: the `SearchSysCache4(AMOPSTRATEGY)` +
/// `amopopr` + `get_opcode` + `fmgr_info_cxt` resolution is the lsyscache
/// `get_opfamily_member` + `get_opcode` pair.
fn minmax_multi_get_strategy_procinfo(
    bdesc: &BrinDesc<'_>,
    attno: AttrNumber,
    subtype: Oid,
    strategynum: u16,
) -> PgResult<Oid> {
    debug_assert!((1..=BTMaxStrategyNumber).contains(&strategynum));

    let opaque = minmax_multi_opaque(bdesc, attno);

    if opaque.cached_subtype.get() != subtype {
        for i in 1..=BTMaxStrategyNumber {
            opaque.strategy_procinfos[(i - 1) as usize].set(INVALID_OID);
        }
        opaque.cached_subtype.set(subtype);
    }

    if opaque.strategy_procinfos[(strategynum - 1) as usize].get() == INVALID_OID {
        let opfamily = bdesc.bd_index.rd_opfamily[(attno - 1) as usize];
        let atttypid = bdesc.bd_tupdesc.attr((attno - 1) as usize).atttypid;

        let oprid =
            lsyscache::get_opfamily_member::call(opfamily, atttypid, subtype, strategynum as i16)?;
        if oprid == INVALID_OID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg_internal(format!(
                    "missing operator {strategynum}({atttypid},{subtype}) in opfamily {opfamily}"
                ))
                .into_error());
        }

        let opcode = lsyscache::get_opcode::call(oprid)?;
        debug_assert!(opcode != INVALID_OID);
        opaque.strategy_procinfos[(strategynum - 1) as usize].set(opcode);
    }

    Ok(opaque.strategy_procinfos[(strategynum - 1) as usize].get())
}

mod codec;
mod distance;
mod ranges;

pub use distance::register_distance_builtins;

use codec::{deserialize_summary, serialize_summary};
use ranges::*;

// ===========================================================================
// brin_minmax_multi_opcinfo (brin_minmax_multi.c:1858)
// ===========================================================================

/// `brin_minmax_multi_opcinfo(typoid)` (brin_minmax_multi.c:1858): one stored
/// column (the serialized BYTEA-like summary), regular NULL handling, a fresh
/// (`palloc0`-zeroed) [`MinmaxMultiOpaque`], and the type-cache slot set to
/// `lookup_type_cache(PG_BRIN_MINMAX_MULTI_SUMMARYOID, 0)`.
///
/// The C ignores the input type argument (the summary always has type
/// `brin_minmax_multi_summary`); the seam still passes it for signature parity.
pub fn brin_minmax_multi_opcinfo<'mcx>(
    mcx: Mcx<'mcx>,
    _typoid: Oid,
) -> PgResult<PgBox<'mcx, BrinOpcInfo<'mcx>>> {
    let tce = backend_utils_cache_typcache_seams::lookup_type_cache::call(
        PG_BRIN_MINMAX_MULTI_SUMMARYOID,
        0,
    )?;
    let mut oi_typcache: PgVec<'mcx, _> = vec_with_capacity_in(mcx, 1)?;
    oi_typcache.push(tce);

    mcx::alloc_in(
        mcx,
        BrinOpcInfo {
            oi_nstored: 1,
            oi_regular_nulls: true,
            oi_opaque: Some(OpaqueOpcInfo::MinmaxMulti(MinmaxMultiOpaque::default())),
            oi_typcache,
        },
    )
}

/// `MinMaxMultiGetValuesPerRange(opts)` (brin_minmax_multi.c:129): the
/// `values_per_range` reloption, or the default. `opts = None` is the trim-gap
/// stand-in (the opclass-options carrier is not threaded through the relcache
/// yet — same convention as bloom).
fn brin_minmax_multi_get_values(opts: Option<i32>) -> i32 {
    match opts {
        Some(v) if v != 0 => v,
        _ => MINMAX_MULTI_DEFAULT_VALUES_PER_PAGE,
    }
}

// ===========================================================================
// brin_minmax_multi_add_value (brin_minmax_multi.c:2412)
// ===========================================================================

/// `brin_minmax_multi_add_value` (brin_minmax_multi.c:2412): add `newval` to the
/// column's range list, returning whether the summary changed. On the first
/// non-null value it initializes the oversized insert buffer and registers the
/// serialize callback (`bv_has_serialize`); otherwise it deserializes the
/// existing on-disk summary once and keeps it live in `bv_mem_value`.
///
/// `opts` is `PG_GET_OPCLASS_OPTIONS()`'s `values_per_range` (trim-gap `None`),
/// `pages_per_range` is `BrinGetPagesPerRange(bd_index)` (trim-gap default).
pub fn brin_minmax_multi_add_value<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    column: &mut BrinValues<'mcx>,
    newval: &Datum<'mcx>,
    isnull: bool,
    colloid: Oid,
    opts: Option<i32>,
    pages_per_range: u32,
) -> PgResult<bool> {
    debug_assert!(!isnull);

    let attno = column.bv_attno;
    let attr = bdesc.bd_tupdesc.attr((attno - 1) as usize);
    let attr_typid = attr.atttypid;
    let attr_byval = attr.attbyval;
    let attr_len = attr.attlen;

    let mut modified = false;

    // If this is the first non-null value, initialize the range list. Otherwise
    // extract the existing range list from BrinValues (deserialize once).
    if column.bv_allnulls {
        let target_maxvalues = brin_minmax_multi_get_values(opts);
        let maxvalues = clamp_buffer_size(target_maxvalues, pages_per_range);

        let mut ranges = minmax_multi_init(mcx, maxvalues)?;
        ranges.attno = attno;
        ranges.colloid = colloid;
        ranges.typid = attr_typid;
        ranges.target_maxvalues = target_maxvalues;

        // we'll certainly need the comparator, so just look it up now
        ranges.cmp =
            minmax_multi_get_strategy_procinfo(bdesc, attno, attr_typid, BTLessStrategyNumber)?;

        column.bv_allnulls = false;
        modified = true;

        column.bv_mem_value = Some(BrinMemValue::MinmaxMultiRanges(ranges));
        column.bv_has_serialize = true;
    } else if column.bv_mem_value.is_none() {
        let serialized = deserialize_summary(mcx, &column.bv_values[0])?;
        let maxvalues = clamp_buffer_size(serialized.maxvalues, pages_per_range);

        let mut ranges = brin_range_deserialize(mcx, maxvalues, &serialized)?;
        ranges.attno = attno;
        ranges.colloid = colloid;
        ranges.typid = attr_typid;

        ranges.cmp =
            minmax_multi_get_strategy_procinfo(bdesc, attno, attr_typid, BTLessStrategyNumber)?;

        column.bv_mem_value = Some(BrinMemValue::MinmaxMultiRanges(ranges));
        column.bv_has_serialize = true;
    }

    // Try to add the new value to the (now live) range buffer.
    let ranges = match column.bv_mem_value.as_mut() {
        Some(BrinMemValue::MinmaxMultiRanges(r)) => r,
        _ => panic!("brin_minmax_multi: bv_mem_value is not a live Ranges buffer"),
    };

    modified |= range_add_value(
        mcx, bdesc, colloid, attno, attr_typid, attr_byval, attr_len, ranges, newval,
    )?;

    Ok(modified)
}

/// Determine the insert-buffer size: 10x the target, capped to the heap-range
/// tuple count, floored at the target and `MINMAX_BUFFER_MIN`, capped at
/// `MINMAX_BUFFER_MAX` (brin_minmax_multi.c:2461-2469 / 2507-2515).
fn clamp_buffer_size(target_maxvalues: i32, pages_per_range: u32) -> i32 {
    let mut maxvalues = (target_maxvalues * MINMAX_BUFFER_FACTOR)
        .min(MaxHeapTuplesPerPage as i32 * pages_per_range as i32);
    maxvalues = maxvalues.max(target_maxvalues);
    maxvalues = maxvalues.max(MINMAX_BUFFER_MIN);
    maxvalues = maxvalues.min(MINMAX_BUFFER_MAX);
    maxvalues
}

// ===========================================================================
// brin_minmax_multi_consistent (brin_minmax_multi.c:2548)
// ===========================================================================

/// `brin_minmax_multi_consistent` (brin_minmax_multi.c:2548): whether the scan
/// keys are consistent with the column's ranges/values. The multi-key form
/// (`fn_nargs >= 4`), so it receives the whole `keys` slice.
pub fn brin_minmax_multi_consistent<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    column: &BrinValues<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    colloid: Oid,
) -> PgResult<bool> {
    let serialized = deserialize_summary(mcx, &column.bv_values[0])?;
    let ranges = brin_range_deserialize(mcx, serialized.maxvalues, &serialized)?;

    // inspect the ranges, and for each one evaluate the scan keys
    for rangeno in 0..ranges.nranges as usize {
        let minval = &ranges.values[2 * rangeno];
        let maxval = &ranges.values[2 * rangeno + 1];

        let mut matching = true;

        for key in keys.iter() {
            // NULL keys are handled and filtered-out in bringetbitmap
            debug_assert!((key.sk_flags & types_scan::scankey::SK_ISNULL) == 0);

            let attno = key.sk_attno;
            let subtype = key.sk_subtype;
            let value = &key.sk_argument;

            let matches = match key.sk_strategy {
                BTLessStrategyNumber | BTLessEqualStrategyNumber => {
                    let finfo =
                        minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, key.sk_strategy)?;
                    // first value from the array
                    call_strategy2(mcx, finfo, colloid, minval, value)?
                }
                BTEqualStrategyNumber => {
                    // by default this range does not match
                    let mut matches = false;
                    // min > value -> smaller than the smallest value in this range
                    let cmp_fn = minmax_multi_get_strategy_procinfo(
                        bdesc,
                        attno,
                        subtype,
                        BTGreaterStrategyNumber,
                    )?;
                    if !call_strategy2(mcx, cmp_fn, colloid, minval, value)? {
                        // max < value -> larger than the largest value in this range
                        let cmp_fn = minmax_multi_get_strategy_procinfo(
                            bdesc,
                            attno,
                            subtype,
                            BTLessStrategyNumber,
                        )?;
                        if !call_strategy2(mcx, cmp_fn, colloid, maxval, value)? {
                            matches = true;
                        }
                    }
                    matches
                }
                BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
                    let finfo =
                        minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, key.sk_strategy)?;
                    // last value from the array
                    call_strategy2(mcx, finfo, colloid, maxval, value)?
                }
                other => {
                    return Err(invalid_strategy(other as i32));
                }
            };

            matching &= matches;
            if !matching {
                break;
            }
        }

        if matching {
            return Ok(true);
        }
    }

    // and now inspect the individual values
    for i in 0..ranges.nvalues as usize {
        let val = &ranges.values[(2 * ranges.nranges) as usize + i];

        let mut matching = true;

        for key in keys.iter() {
            // we've already dealt with NULL keys at the beginning
            if (key.sk_flags & types_scan::scankey::SK_ISNULL) != 0 {
                continue;
            }

            let attno = key.sk_attno;
            let subtype = key.sk_subtype;
            let value = &key.sk_argument;

            let matches = match key.sk_strategy {
                BTLessStrategyNumber
                | BTLessEqualStrategyNumber
                | BTEqualStrategyNumber
                | BTGreaterEqualStrategyNumber
                | BTGreaterStrategyNumber => {
                    let finfo =
                        minmax_multi_get_strategy_procinfo(bdesc, attno, subtype, key.sk_strategy)?;
                    call_strategy2(mcx, finfo, colloid, val, value)?
                }
                other => {
                    return Err(invalid_strategy(other as i32));
                }
            };

            matching &= matches;
            if !matching {
                break;
            }
        }

        if matching {
            return Ok(true);
        }
    }

    Ok(false)
}

// ===========================================================================
// brin_minmax_multi_union (brin_minmax_multi.c:2734)
// ===========================================================================

/// `brin_minmax_multi_union` (brin_minmax_multi.c:2734): update `col_a` so that
/// it becomes the union of the summaries in `col_a` and `col_b`; `col_b` is
/// untouched. The merged summary is re-serialized into `col_a.bv_values[0]`.
pub fn brin_minmax_multi_union<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    col_a: &mut BrinValues<'mcx>,
    col_b: &BrinValues<'mcx>,
    colloid: Oid,
) -> PgResult<()> {
    debug_assert_eq!(col_a.bv_attno, col_b.bv_attno);
    debug_assert!(!col_a.bv_allnulls && !col_b.bv_allnulls);

    let attno = col_a.bv_attno;
    let attr_typid = bdesc.bd_tupdesc.attr((attno - 1) as usize).atttypid;

    let serialized_a = deserialize_summary(mcx, &col_a.bv_values[0])?;
    let serialized_b = deserialize_summary(mcx, &col_b.bv_values[0])?;

    let mut ranges_a = brin_range_deserialize(mcx, serialized_a.maxvalues, &serialized_a)?;
    let ranges_b = brin_range_deserialize(mcx, serialized_b.maxvalues, &serialized_b)?;

    let neranges_total =
        (ranges_a.nranges + ranges_a.nvalues) + (ranges_b.nranges + ranges_b.nvalues);

    let mut eranges: PgVec<'mcx, ExpandedRange<'mcx>> =
        vec_with_capacity_in(mcx, neranges_total as usize)?;
    for _ in 0..neranges_total {
        eranges.push(ExpandedRange::empty());
    }

    // fill from the first range, then from the second
    let n_a = ranges_a.nranges + ranges_a.nvalues;
    fill_expanded_ranges(&mut eranges[..n_a as usize], n_a, &ranges_a);
    let n_b = ranges_b.nranges + ranges_b.nvalues;
    fill_expanded_ranges(&mut eranges[n_a as usize..], n_b, &ranges_b);

    let cmp = minmax_multi_get_strategy_procinfo(bdesc, attno, attr_typid, BTLessStrategyNumber)?;

    // sort the expanded ranges
    let mut neranges = sort_expanded_ranges(mcx, cmp, colloid, &mut eranges, neranges_total)?;

    // merge overlapping ranges
    neranges = merge_overlapping_ranges(mcx, cmp, colloid, &mut eranges, neranges)?;

    AssertCheckExpandedRanges(mcx, cmp, colloid, &eranges, neranges)?;

    // build distances and reduce to the first range's maxvalues
    let distance = minmax_multi_get_procinfo(bdesc, attno, PROCNUM_DISTANCE)?;
    let distances = build_distances(mcx, distance, colloid, &eranges, neranges)?;

    neranges = reduce_expanded_ranges(
        mcx,
        cmp,
        colloid,
        &mut eranges,
        neranges,
        distances.as_deref(),
        ranges_a.maxvalues,
    )?;

    AssertCheckExpandedRanges(mcx, cmp, colloid, &eranges, neranges)?;

    // update the first range summary
    store_expanded_ranges(&mut ranges_a, &eranges, neranges);

    // cleanup and re-serialize into col_a
    let s = brin_range_serialize(mcx, cmp, colloid, &mut ranges_a)?;
    col_a.bv_values[0] = serialize_summary(mcx, &s)?;

    Ok(())
}

// ===========================================================================
// brin_minmax_multi_serialize (brin_minmax_multi.c:2379) — bv_serialize callback
// ===========================================================================

/// `brin_minmax_multi_serialize(bdesc, src, dst)` (brin_minmax_multi.c:2379):
/// the `bv_serialize` callback. In batch mode it compacts the accumulated
/// buffer to `target_maxvalues`, then serializes the result into `dst[0]`.
pub fn brin_minmax_multi_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    bdesc: &BrinDesc<'mcx>,
    mem_value: &mut BrinMemValue<'mcx>,
    dst: &mut [Datum<'mcx>],
) -> PgResult<()> {
    let ranges = match mem_value {
        BrinMemValue::MinmaxMultiRanges(r) => r,
        _ => panic!("brin_minmax_multi_serialize: bv_mem_value is not a live Ranges buffer"),
    };

    // resolve the cmp + distance procinfos (cached on the attribute's opaque)
    let cmp =
        minmax_multi_get_strategy_procinfo(bdesc, ranges.attno, ranges.typid, BTLessStrategyNumber)?;
    let distance = minmax_multi_get_procinfo(bdesc, ranges.attno, PROCNUM_DISTANCE)?;

    // compress the accumulated values to the requested number of values/ranges
    let target = ranges.target_maxvalues;
    compactify_ranges(mcx, cmp, distance, ranges, target)?;

    // everything has to be fully sorted now
    debug_assert_eq!(ranges.nsorted, ranges.nvalues);

    let s = brin_range_serialize(mcx, cmp, ranges.colloid, ranges)?;
    dst[0] = serialize_summary(mcx, &s)?;
    Ok(())
}

/// `elog(ERROR, "invalid strategy number %d", ...)` — the `elog`-default
/// SQLSTATE is `ERRCODE_INTERNAL_ERROR`.
fn invalid_strategy(strategy: i32) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg_internal(format!("invalid strategy number {strategy}"))
        .into_error()
}

#[cfg(test)]
mod tests;
