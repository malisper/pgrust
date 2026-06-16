//! `access/gist/gistproc.c` — GiST support procedures for 2-D objects (boxes,
//! polygons, circles, points), giving R-tree behaviour with Guttman's poly-time
//! split.
//!
//! This crate is the single installer of the GiST typed opclass-support-proc
//! dispatch declared in `backend-access-gist-dispatch-seams`. It installs the
//! box/point opclass bodies, keyed on their `pg_proc.dat` support-proc OIDs
//! (`F_GIST_BOX_*` / `F_GIST_POINT_*`); a dispatch to any other OID errors
//! ("unrecognized GiST support function OID"). The range/inet/tsvector
//! opclasses fold their OIDs into the same dispatcher when their owners land.
//!
//! The box/point GiST index always stores keys as boxes; the [`GISTENTRY`]
//! `key` carries the `BOX` by-reference image (32 bytes). The geometric box and
//! point boolean operators (`box_left`/`box_overlap`/...) reach
//! `backend-utils-adt-geo-ops` through its seam crate, exactly as the C code
//! reaches them via `DirectFunctionCall2`.
//!
//! The `gist_poly_*` / `gist_circle_*` support procedures (and the
//! polygon/circle strategy groups of `gist_point_consistent`) reach the
//! `POLYGON` / `CIRCLE` predicates (`poly_contain_pt` / `circle_contain_pt`) and
//! the polygon bounding box through `backend-utils-adt-geo-ops` (now landed).
//! Both opclasses store boxes as GiST index entries, so the leaf and inner
//! consistency checks share `rtree_internal_consistent`; the query polygon is
//! carried as its in-memory varlena image and decoded inside the geo-ops owner.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_access_gist_dispatch_seams as dispatch;
use backend_utils_adt_network_gist_seams as inet_gist;
use backend_utils_adt_geo_ops_seams as geo;
use types_network::{inet_struct, GistInetKey};
use dispatch::{GistConsistentResult, GistDistanceResult, StrategyNumber};
use mcx::{Mcx, PgBox};
use types_core::geo::{Point, CIRCLE, BOX};
use types_core::primitive::{Oid, OffsetNumber};
use types_error::{PgError, PgResult};
use types_gist::{GistEntryVector, GISTENTRY, GIST_SPLITVEC};
use types_sortsupport::SortSupportData;
use types_tuple::backend_access_common_heaptuple::Datum;

// ---------------------------------------------------------------------------
// pg_proc.dat support-proc OIDs for the box and point opclasses.
// ---------------------------------------------------------------------------

/// `gist_box_consistent` (pg_proc.dat oid 2578).
pub const F_GIST_BOX_CONSISTENT: Oid = 2578;
/// `gist_box_penalty` (pg_proc.dat oid 2581).
pub const F_GIST_BOX_PENALTY: Oid = 2581;
/// `gist_box_picksplit` (pg_proc.dat oid 2582).
pub const F_GIST_BOX_PICKSPLIT: Oid = 2582;
/// `gist_box_union` (pg_proc.dat oid 2583).
pub const F_GIST_BOX_UNION: Oid = 2583;
/// `gist_box_same` (pg_proc.dat oid 2584).
pub const F_GIST_BOX_SAME: Oid = 2584;
/// `gist_box_distance` (pg_proc.dat oid 3998).
pub const F_GIST_BOX_DISTANCE: Oid = 3998;

/// `gist_poly_consistent` (pg_proc.dat oid 2585).
pub const F_GIST_POLY_CONSISTENT: Oid = 2585;
/// `gist_poly_compress` (pg_proc.dat oid 2586).
pub const F_GIST_POLY_COMPRESS: Oid = 2586;
/// `gist_poly_distance` (pg_proc.dat oid 3288).
pub const F_GIST_POLY_DISTANCE: Oid = 3288;

/// `gist_circle_consistent` (pg_proc.dat oid 2591).
pub const F_GIST_CIRCLE_CONSISTENT: Oid = 2591;
/// `gist_circle_compress` (pg_proc.dat oid 2592).
pub const F_GIST_CIRCLE_COMPRESS: Oid = 2592;
/// `gist_circle_distance` (pg_proc.dat oid 3280).
pub const F_GIST_CIRCLE_DISTANCE: Oid = 3280;

/// `gist_point_compress` (pg_proc.dat oid 1030).
pub const F_GIST_POINT_COMPRESS: Oid = 1030;
/// `gist_point_fetch` (pg_proc.dat oid 3282).
pub const F_GIST_POINT_FETCH: Oid = 3282;
/// `gist_point_consistent` (pg_proc.dat oid 2179).
pub const F_GIST_POINT_CONSISTENT: Oid = 2179;
/// `gist_point_distance` (pg_proc.dat oid 3064).
pub const F_GIST_POINT_DISTANCE: Oid = 3064;
/// `gist_point_sortsupport` (pg_proc.dat oid 3435).
pub const F_GIST_POINT_SORTSUPPORT: Oid = 3435;

/// `inet_gist_consistent` (pg_proc.dat oid 3553).
pub const F_INET_GIST_CONSISTENT: Oid = 3553;
/// `inet_gist_union` (pg_proc.dat oid 3554).
pub const F_INET_GIST_UNION: Oid = 3554;
/// `inet_gist_compress` (pg_proc.dat oid 3555).
pub const F_INET_GIST_COMPRESS: Oid = 3555;
/// `inet_gist_penalty` (pg_proc.dat oid 3557).
pub const F_INET_GIST_PENALTY: Oid = 3557;
/// `inet_gist_picksplit` (pg_proc.dat oid 3558).
pub const F_INET_GIST_PICKSPLIT: Oid = 3558;
/// `inet_gist_same` (pg_proc.dat oid 3559).
pub const F_INET_GIST_SAME: Oid = 3559;
/// `inet_gist_fetch` (pg_proc.dat oid 3573).
pub const F_INET_GIST_FETCH: Oid = 3573;

// ---------------------------------------------------------------------------
// access/stratnum.h — R-tree strategy numbers consumed below.
// ---------------------------------------------------------------------------

const RTLeftStrategyNumber: StrategyNumber = 1;
const RTOverLeftStrategyNumber: StrategyNumber = 2;
const RTOverlapStrategyNumber: StrategyNumber = 3;
const RTOverRightStrategyNumber: StrategyNumber = 4;
const RTRightStrategyNumber: StrategyNumber = 5;
const RTSameStrategyNumber: StrategyNumber = 6;
const RTContainsStrategyNumber: StrategyNumber = 7;
const RTContainedByStrategyNumber: StrategyNumber = 8;
const RTOverBelowStrategyNumber: StrategyNumber = 9;
const RTBelowStrategyNumber: StrategyNumber = 10;
const RTAboveStrategyNumber: StrategyNumber = 11;
const RTOverAboveStrategyNumber: StrategyNumber = 12;
const RTOldBelowStrategyNumber: StrategyNumber = 29;
const RTOldAboveStrategyNumber: StrategyNumber = 30;

/// `#define LIMIT_RATIO 0.3` (gistproc.c) — minimum accepted ratio of a split.
const LIMIT_RATIO: f64 = 0.3;

// gistproc.c gist_point_consistent strategy-group classification.
const GeoStrategyNumberOffset: StrategyNumber = 20;
const PointStrategyNumberGroup: StrategyNumber = 0;
const BoxStrategyNumberGroup: StrategyNumber = 1;
const PolygonStrategyNumberGroup: StrategyNumber = 2;
const CircleStrategyNumberGroup: StrategyNumber = 3;

// ---------------------------------------------------------------------------
// utils/float.h helpers used by the box ops (plain IEEE arithmetic; the C
// macros add no checks beyond the operation itself).
// ---------------------------------------------------------------------------

/// `float8_max(a, b)` (float.h) — IEEE maximum with C's NaN handling.
#[inline]
fn float8_max(a: f64, b: f64) -> f64 {
    // C: `float8_max` returns the larger; with a NaN operand the comparison is
    // false so it returns `b` (matching `a > b ? a : b`).
    if a > b {
        a
    } else {
        b
    }
}

/// `float8_min(a, b)` (float.h).
#[inline]
fn float8_min(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}

/// `float8_lt(a, b)` (float.h) — plain IEEE `<`.
#[inline]
fn float8_lt(a: f64, b: f64) -> bool {
    a < b
}

/// `float8_gt(a, b)` (float.h).
#[inline]
fn float8_gt(a: f64, b: f64) -> bool {
    a > b
}

/// `float8_le(a, b)` (float.h).
#[inline]
fn float8_le(a: f64, b: f64) -> bool {
    a <= b
}

/// `float8_ge(a, b)` (float.h).
#[inline]
fn float8_ge(a: f64, b: f64) -> bool {
    a >= b
}

/// `float8_eq(a, b)` (float.h) — plain IEEE `==` (so `NaN != NaN`).
#[inline]
fn float8_eq(a: f64, b: f64) -> bool {
    a == b
}

/// `float8_mi(a, b)` (float.h) — `a - b` (the float8 difference is unchecked).
#[inline]
fn float8_mi(a: f64, b: f64) -> f64 {
    a - b
}

/// `float8_mul(a, b)` (float.h).
#[inline]
fn float8_mul(a: f64, b: f64) -> f64 {
    a * b
}

/// `float8_div(a, b)` (float.h).
#[inline]
fn float8_div(a: f64, b: f64) -> f64 {
    a / b
}

/// `float4_div(a, b)` (float.h) — single-precision division.
#[inline]
fn float4_div(a: f32, b: f32) -> f32 {
    a / b
}

/// `float8_cmp_internal(a, b)` (float.c) — total ordering used by the interval
/// and common-entry sorts: NaN sorts greatest, all NaNs equal.
fn float8_cmp_internal(a: f64, b: f64) -> i32 {
    if a > b {
        1
    } else if a < b {
        -1
    } else if a == b {
        0
    } else {
        // At least one operand is NaN.
        if a.is_nan() {
            if b.is_nan() {
                0
            } else {
                1
            }
        } else {
            -1
        }
    }
}

/// `FPeq(A, B)` (geo_decls.h): fuzzy equality, the one fuzzy comparator the
/// geo-ops seam crate does not export (it exports the strict `FP{lt,le,gt,ge}`).
const EPSILON: f64 = 1.0e-06;

#[inline]
fn FPeq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= EPSILON
}

// ---------------------------------------------------------------------------
// GISTENTRY helpers.
// ---------------------------------------------------------------------------

/// `DatumGetBoxP(entry->key)` for a box-keyed GiST entry — decode the 32-byte
/// `BOX` image carried by the entry's key [`Datum`].
#[inline]
fn entry_box(entry: &GISTENTRY<'_>) -> BOX {
    BOX::from_datum_bytes(entry.key.as_ref_bytes())
}

/// `gistentryinit(e, key, r, pg, o, l)` (gist.h) — initialise a GiST entry.
#[inline]
fn gistentryinit<'mcx>(
    key: Datum<'mcx>,
    rel: Oid,
    page: types_core::primitive::BlockNumber,
    offset: OffsetNumber,
    leafkey: bool,
) -> GISTENTRY<'mcx> {
    GISTENTRY {
        key,
        rel,
        page,
        offset,
        leafkey,
    }
}

/// Decode every entry key in `entryvec->vector` as a `GistInetKey`
/// (`DatumGetInetKeyP(ent[i].key)`). Index 0 of the picksplit/union vector is
/// the C `entryvec->vector[0]`; the inet methods only read `1..=maxoff`, but a
/// slot must exist at index 0, so the whole vector is decoded.
fn inet_keys_from_vec<'mcx>(entryvec: &GistEntryVector<'mcx>) -> Vec<GistInetKey> {
    entryvec
        .vector
        .iter()
        .map(|e| GistInetKey::from_datum_bytes(e.key.as_ref_bytes()))
        .collect()
}

/// Pack a `GistInetKey` into a by-reference key `Datum` (`InetKeyPGetDatum`).
fn inet_key_datum<'mcx>(mcx: Mcx<'mcx>, k: &GistInetKey) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &k.to_datum_bytes())?))
}

/// Wrap a `BOX` into a by-reference key [`Datum`] (`BoxPGetDatum`).
#[inline]
fn box_datum<'mcx>(mcx: Mcx<'mcx>, b: &BOX) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &b.to_datum_bytes())?))
}

/// `point_distance` -> `point_dt` (geo_ops.c): `HYPOT(p1.x - p2.x, p1.y -
/// p2.y)`, routed through the geo-ops `HYPOT` seam (mirrors the SP-GiST port).
#[inline]
fn point_point_distance(p1: &Point, p2: &Point) -> PgResult<f64> {
    geo::HYPOT::call(p1.x - p2.x, p1.y - p2.y)
}

fn unrecognized_strategy(strategy: StrategyNumber) -> PgError {
    PgError::error(format!("unrecognized strategy number: {strategy}"))
}

// ===========================================================================
// Box ops
// ===========================================================================

/// `rt_box_union(n, a, b)` (gistproc.c) — union of two boxes into `*n`.
fn rt_box_union(a: &BOX, b: &BOX) -> BOX {
    BOX {
        high: Point {
            x: float8_max(a.high.x, b.high.x),
            y: float8_max(a.high.y, b.high.y),
        },
        low: Point {
            x: float8_min(a.low.x, b.low.x),
            y: float8_min(a.low.y, b.low.y),
        },
    }
}

/// `size_box(box)` (gistproc.c) — size of a box for penalty calculation. The
/// result can be `+Infinity` but not `NaN`.
fn size_box(b: &BOX) -> f64 {
    // Check for zero-width cases; a zero-by-infinity box has size zero.
    if float8_le(b.high.x, b.low.x) || float8_le(b.high.y, b.low.y) {
        return 0.0;
    }

    // NaN is treated as larger than +Infinity.
    if b.high.x.is_nan() || b.high.y.is_nan() {
        return f64::INFINITY;
    }
    float8_mul(float8_mi(b.high.x, b.low.x), float8_mi(b.high.y, b.low.y))
}

/// `box_penalty(original, new)` (gistproc.c) — amount by which the union grows.
fn box_penalty(original: &BOX, new: &BOX) -> f64 {
    let unionbox = rt_box_union(original, new);
    float8_mi(size_box(&unionbox), size_box(original))
}

/// `adjustBox(b, addon)` (gistproc.c) — grow box `b` to include `addon`.
fn adjustBox(b: &mut BOX, addon: &BOX) {
    if float8_lt(b.high.x, addon.high.x) {
        b.high.x = addon.high.x;
    }
    if float8_gt(b.low.x, addon.low.x) {
        b.low.x = addon.low.x;
    }
    if float8_lt(b.high.y, addon.high.y) {
        b.high.y = addon.high.y;
    }
    if float8_gt(b.low.y, addon.low.y) {
        b.low.y = addon.low.y;
    }
}

/// `gist_box_consistent` (gistproc.c) — the GiST consistent method for boxes.
fn gist_box_consistent<'mcx>(
    entry: &GISTENTRY<'mcx>,
    is_leaf: bool,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistConsistentResult> {
    // All cases served by this function are exact.
    // (NULL key / NULL query never occur in the owned model — the key is always
    // a 32-byte BOX image; C's `DatumGetBoxP(entry->key) == NULL` guard is for a
    // genuinely null index entry, not reachable through this typed dispatch.)
    let key = entry_box(entry);
    let q = BOX::from_datum_bytes(query.as_ref_bytes());

    let matched = if is_leaf {
        gist_box_leaf_consistent(&key, &q, strategy)?
    } else {
        rtree_internal_consistent(&key, &q, strategy)?
    };
    Ok(GistConsistentResult {
        matched,
        recheck: false,
    })
}

/// `gist_box_union` (gistproc.c) — the GiST union method for boxes.
fn gist_box_union<'mcx>(mcx: Mcx<'mcx>, entryvec: &GistEntryVector<'mcx>) -> PgResult<Datum<'mcx>> {
    let numranges = entryvec.n;
    let mut pageunion = entry_box(&entryvec.vector[0]);
    let mut i = 1;
    while i < numranges {
        let cur = entry_box(&entryvec.vector[i as usize]);
        adjustBox(&mut pageunion, &cur);
        i += 1;
    }
    box_datum(mcx, &pageunion)
}

/// `gist_box_penalty` (gistproc.c) — the GiST penalty method for boxes (also
/// used for points).
fn gist_box_penalty(origentry: &GISTENTRY<'_>, newentry: &GISTENTRY<'_>) -> f32 {
    let origbox = entry_box(origentry);
    let newbox = entry_box(newentry);
    box_penalty(&origbox, &newbox) as f32
}

/// `fallbackSplit(entryvec, v)` (gistproc.c) — trivial split: first half left,
/// rest right.
fn fallbackSplit<'mcx>(
    mcx: Mcx<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
) -> PgResult<()> {
    let maxoff: i32 = entryvec.n - 1;

    v.spl_left = Vec::new();
    v.spl_right = Vec::new();
    let mut unionL: Option<BOX> = None;
    let mut unionR: Option<BOX> = None;

    // FirstOffsetNumber == 1.
    let mut i: i32 = 1;
    while i <= maxoff {
        let cur = entry_box(&entryvec.vector[i as usize]);

        if i <= (maxoff - 1 + 1) / 2 {
            v.spl_left.push(i as OffsetNumber);
            match unionL.as_mut() {
                None => unionL = Some(cur),
                Some(u) => adjustBox(u, &cur),
            }
        } else {
            v.spl_right.push(i as OffsetNumber);
            match unionR.as_mut() {
                None => unionR = Some(cur),
                Some(u) => adjustBox(u, &cur),
            }
        }
        i += 1;
    }

    v.spl_ldatum = Some(box_datum(mcx, &unionL.unwrap_or_default())?);
    v.spl_rdatum = Some(box_datum(mcx, &unionR.unwrap_or_default())?);
    Ok(())
}

/// `CommonEntry` (gistproc.c) — an entry placeable into either group.
#[derive(Clone, Copy)]
struct CommonEntry {
    index: i32,
    delta: f64,
}

/// `ConsiderSplitContext` (gistproc.c) — currently selected split + general
/// info.
#[derive(Clone, Copy, Default)]
struct ConsiderSplitContext {
    entriesCount: i32,
    boundingBox: BOX,
    first: bool,
    leftUpper: f64,
    rightLower: f64,
    ratio: f32,
    overlap: f32,
    dim: i32,
    range: f64,
}

/// `SplitInterval` (gistproc.c) — projection of a box to an axis.
#[derive(Clone, Copy, Default)]
struct SplitInterval {
    lower: f64,
    upper: f64,
}

/// `non_negative(val)` (gistproc.c) — replace negative (or NaN) with zero.
#[inline]
fn non_negative(val: f32) -> f32 {
    if val >= 0.0f32 {
        val
    } else {
        0.0f32
    }
}

/// `g_box_consider_split(context, dimNum, rightLower, minLeftCount, leftUpper,
/// maxLeftCount)` (gistproc.c).
fn g_box_consider_split(
    context: &mut ConsiderSplitContext,
    dimNum: i32,
    rightLower: f64,
    minLeftCount: i32,
    leftUpper: f64,
    maxLeftCount: i32,
) {
    let leftCount;
    if minLeftCount >= (context.entriesCount + 1) / 2 {
        leftCount = minLeftCount;
    } else if maxLeftCount <= context.entriesCount / 2 {
        leftCount = maxLeftCount;
    } else {
        leftCount = context.entriesCount / 2;
    }
    let rightCount = context.entriesCount - leftCount;

    let ratio = float4_div(leftCount.min(rightCount) as f32, context.entriesCount as f32);

    if ratio > LIMIT_RATIO as f32 {
        let mut selectthis = false;

        let range = if dimNum == 0 {
            float8_mi(context.boundingBox.high.x, context.boundingBox.low.x)
        } else {
            float8_mi(context.boundingBox.high.y, context.boundingBox.low.y)
        };

        let overlap = float8_div(float8_mi(leftUpper, rightLower), range) as f32;

        if context.first {
            selectthis = true;
        } else if context.dim == dimNum {
            if overlap < context.overlap
                || (overlap == context.overlap && ratio > context.ratio)
            {
                selectthis = true;
            }
        } else if non_negative(overlap) < non_negative(context.overlap)
            || (range > context.range
                && non_negative(overlap) <= non_negative(context.overlap))
        {
            selectthis = true;
        }

        if selectthis {
            context.first = false;
            context.ratio = ratio;
            context.range = range;
            context.overlap = overlap;
            context.rightLower = rightLower;
            context.leftUpper = leftUpper;
            context.dim = dimNum;
        }
    }
}

/// `gist_box_picksplit` (gistproc.c) — the double-sorting split algorithm, used
/// for both boxes and points.
fn gist_box_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
    v: &mut GIST_SPLITVEC<'mcx>,
) -> PgResult<()> {
    let mut context = ConsiderSplitContext::default();

    let maxoff: i32 = entryvec.n - 1;
    // nentries = maxoff - FirstOffsetNumber + 1
    let nentries = maxoff - 1 + 1;
    context.entriesCount = nentries;

    let mut intervalsLower = vec![SplitInterval::default(); nentries as usize];
    let mut intervalsUpper = vec![SplitInterval::default(); nentries as usize];

    // Overall minimum bounding box.
    let mut i: i32 = 1;
    while i <= maxoff {
        let b = entry_box(&entryvec.vector[i as usize]);
        if i == 1 {
            context.boundingBox = b;
        } else {
            adjustBox(&mut context.boundingBox, &b);
        }
        i += 1;
    }

    context.first = true;
    for dim in 0..2 {
        // Project each entry as an interval on the selected axis.
        let mut i: i32 = 1;
        while i <= maxoff {
            let b = entry_box(&entryvec.vector[i as usize]);
            let idx = (i - 1) as usize;
            if dim == 0 {
                intervalsLower[idx].lower = b.low.x;
                intervalsLower[idx].upper = b.high.x;
            } else {
                intervalsLower[idx].lower = b.low.y;
                intervalsLower[idx].upper = b.high.y;
            }
            i += 1;
        }

        intervalsUpper[..nentries as usize]
            .copy_from_slice(&intervalsLower[..nentries as usize]);
        intervalsLower.sort_by(|a, b| {
            float8_cmp_internal(a.lower, b.lower).cmp(&0)
        });
        intervalsUpper.sort_by(|a, b| {
            float8_cmp_internal(a.upper, b.upper).cmp(&0)
        });

        // Iterate over lower bound of right group, finding smallest possible
        // upper bound of left group.
        let mut i1: i32 = 0;
        let mut i2: i32 = 0;
        let mut rightLower = intervalsLower[i1 as usize].lower;
        let mut leftUpper = intervalsUpper[i2 as usize].lower;
        loop {
            while i1 < nentries && float8_eq(rightLower, intervalsLower[i1 as usize].lower) {
                if float8_lt(leftUpper, intervalsLower[i1 as usize].upper) {
                    leftUpper = intervalsLower[i1 as usize].upper;
                }
                i1 += 1;
            }
            if i1 >= nentries {
                break;
            }
            rightLower = intervalsLower[i1 as usize].lower;

            while i2 < nentries && float8_le(intervalsUpper[i2 as usize].upper, leftUpper) {
                i2 += 1;
            }

            g_box_consider_split(&mut context, dim, rightLower, i1, leftUpper, i2);
        }

        // Iterate over upper bound of left group finding greatest possible
        // lower bound of right group.
        let mut i1: i32 = nentries - 1;
        let mut i2: i32 = nentries - 1;
        let mut rightLower = intervalsLower[i1 as usize].upper;
        let mut leftUpper = intervalsUpper[i2 as usize].upper;
        loop {
            while i2 >= 0 && float8_eq(leftUpper, intervalsUpper[i2 as usize].upper) {
                if float8_gt(rightLower, intervalsUpper[i2 as usize].lower) {
                    rightLower = intervalsUpper[i2 as usize].lower;
                }
                i2 -= 1;
            }
            if i2 < 0 {
                break;
            }
            leftUpper = intervalsUpper[i2 as usize].upper;

            while i1 >= 0 && float8_ge(intervalsLower[i1 as usize].lower, rightLower) {
                i1 -= 1;
            }

            g_box_consider_split(&mut context, dim, rightLower, i1 + 1, leftUpper, i2 + 1);
        }
    }

    // If we failed to find any acceptable splits, use trivial split.
    if context.first {
        return fallbackSplit(mcx, entryvec, v);
    }

    v.spl_left = Vec::new();
    v.spl_right = Vec::new();

    let mut leftBox = BOX::default();
    let mut rightBox = BOX::default();

    let mut commonEntries: Vec<CommonEntry> = Vec::new();

    // Distribute unambiguous entries, collect common entries.
    let mut i: i32 = 1;
    while i <= maxoff {
        let b = entry_box(&entryvec.vector[i as usize]);
        let (lower, upper) = if context.dim == 0 {
            (b.low.x, b.high.x)
        } else {
            (b.low.y, b.high.y)
        };

        if float8_le(upper, context.leftUpper) {
            if float8_ge(lower, context.rightLower) {
                // Common entry.
                commonEntries.push(CommonEntry { index: i, delta: 0.0 });
            } else {
                // PLACE_LEFT(b, i)
                if !v.spl_left.is_empty() {
                    adjustBox(&mut leftBox, &b);
                } else {
                    leftBox = b;
                }
                v.spl_left.push(i as OffsetNumber);
            }
        } else {
            // Should fit in the right group (C asserts float8_ge(lower, rightLower)).
            // PLACE_RIGHT(b, i)
            if !v.spl_right.is_empty() {
                adjustBox(&mut rightBox, &b);
            } else {
                rightBox = b;
            }
            v.spl_right.push(i as OffsetNumber);
        }
        i += 1;
    }

    // Distribute common entries.
    if !commonEntries.is_empty() {
        let commonEntriesCount = commonEntries.len() as i32;
        let m = (LIMIT_RATIO * nentries as f64).ceil() as i32;

        for ce in commonEntries.iter_mut() {
            let b = entry_box(&entryvec.vector[ce.index as usize]);
            ce.delta =
                float8_mi(box_penalty(&leftBox, &b), box_penalty(&rightBox, &b)).abs();
        }

        commonEntries.sort_by(|a, b| float8_cmp_internal(a.delta, b.delta).cmp(&0));

        for (j, ce) in commonEntries.iter().enumerate() {
            let b = entry_box(&entryvec.vector[ce.index as usize]);
            let remaining = commonEntriesCount - j as i32;

            if v.spl_left.len() as i32 + remaining <= m {
                if !v.spl_left.is_empty() {
                    adjustBox(&mut leftBox, &b);
                } else {
                    leftBox = b;
                }
                v.spl_left.push(ce.index as OffsetNumber);
            } else if v.spl_right.len() as i32 + remaining <= m {
                if !v.spl_right.is_empty() {
                    adjustBox(&mut rightBox, &b);
                } else {
                    rightBox = b;
                }
                v.spl_right.push(ce.index as OffsetNumber);
            } else if box_penalty(&leftBox, &b) < box_penalty(&rightBox, &b) {
                if !v.spl_left.is_empty() {
                    adjustBox(&mut leftBox, &b);
                } else {
                    leftBox = b;
                }
                v.spl_left.push(ce.index as OffsetNumber);
            } else {
                if !v.spl_right.is_empty() {
                    adjustBox(&mut rightBox, &b);
                } else {
                    rightBox = b;
                }
                v.spl_right.push(ce.index as OffsetNumber);
            }
        }
    }

    v.spl_ldatum = Some(box_datum(mcx, &leftBox)?);
    v.spl_rdatum = Some(box_datum(mcx, &rightBox)?);
    Ok(())
}

/// `gist_box_same` (gistproc.c) — exact box equality (cannot be fuzzy without
/// breaking index consistency).
fn gist_box_same(a: &Datum<'_>, b: &Datum<'_>) -> bool {
    // Both are always present in the typed dispatch; C's NULL/NULL branch is
    // unreachable here (the keys are always a 32-byte BOX image).
    let b1 = BOX::from_datum_bytes(a.as_ref_bytes());
    let b2 = BOX::from_datum_bytes(b.as_ref_bytes());
    float8_eq(b1.low.x, b2.low.x)
        && float8_eq(b1.low.y, b2.low.y)
        && float8_eq(b1.high.x, b2.high.x)
        && float8_eq(b1.high.y, b2.high.y)
}

/// `gist_box_leaf_consistent(key, query, strategy)` (gistproc.c) — leaf-level
/// box consistency: apply the query operator directly.
fn gist_box_leaf_consistent(key: &BOX, query: &BOX, strategy: StrategyNumber) -> PgResult<bool> {
    let retval = match strategy {
        RTLeftStrategyNumber => geo::box_left::call(key, query),
        RTOverLeftStrategyNumber => geo::box_overleft::call(key, query),
        RTOverlapStrategyNumber => geo::box_overlap::call(key, query),
        RTOverRightStrategyNumber => geo::box_overright::call(key, query),
        RTRightStrategyNumber => geo::box_right::call(key, query),
        RTSameStrategyNumber => geo::box_same::call(key, query),
        RTContainsStrategyNumber => geo::box_contain::call(key, query),
        RTContainedByStrategyNumber => geo::box_contained::call(key, query),
        RTOverBelowStrategyNumber => geo::box_overbelow::call(key, query),
        RTBelowStrategyNumber => geo::box_below::call(key, query),
        RTAboveStrategyNumber => geo::box_above::call(key, query),
        RTOverAboveStrategyNumber => geo::box_overabove::call(key, query),
        _ => return Err(unrecognized_strategy(strategy)),
    };
    Ok(retval)
}

/// `rtree_internal_consistent(key, query, strategy)` (gistproc.c) —
/// internal-page consistency for boxes/polygons/circles (all use bounding
/// boxes).
fn rtree_internal_consistent(
    key: &BOX,
    query: &BOX,
    strategy: StrategyNumber,
) -> PgResult<bool> {
    let retval = match strategy {
        RTLeftStrategyNumber => !geo::box_overright::call(key, query),
        RTOverLeftStrategyNumber => !geo::box_right::call(key, query),
        RTOverlapStrategyNumber => geo::box_overlap::call(key, query),
        RTOverRightStrategyNumber => !geo::box_left::call(key, query),
        RTRightStrategyNumber => !geo::box_overleft::call(key, query),
        RTSameStrategyNumber | RTContainsStrategyNumber => geo::box_contain::call(key, query),
        RTContainedByStrategyNumber => geo::box_overlap::call(key, query),
        RTOverBelowStrategyNumber => !geo::box_above::call(key, query),
        RTBelowStrategyNumber => !geo::box_overabove::call(key, query),
        RTAboveStrategyNumber => !geo::box_overbelow::call(key, query),
        RTOverAboveStrategyNumber => !geo::box_below::call(key, query),
        _ => return Err(unrecognized_strategy(strategy)),
    };
    Ok(retval)
}

// ===========================================================================
// Polygon ops
// ===========================================================================

/// `gist_poly_compress` (gistproc.c) — represent a polygon by its bounding box.
fn gist_poly_compress<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    if entry.leafkey {
        // POLYGON *in = DatumGetPolygonP(entry->key);
        // r = palloc(sizeof(BOX)); memcpy(r, &in->boundbox, sizeof(BOX));
        let r = geo::poly_query_boundbox::call(entry.key.as_ref_bytes());
        let retval = gistentryinit(box_datum(mcx, &r)?, entry.rel, entry.page, entry.offset, false);
        return mcx::alloc_in(mcx, retval);
    }
    mcx::alloc_in(mcx, entry.clone())
}

/// `gist_poly_consistent` (gistproc.c) — the GiST consistent method for polygons.
/// All cases are inexact (`*recheck = true`). The index entries are bounding
/// boxes, so even leaf nodes use `rtree_internal_consistent`.
fn gist_poly_consistent<'mcx>(
    entry: &GISTENTRY<'mcx>,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistConsistentResult> {
    // All cases served by this function are inexact.
    // (The C NULL key / NULL query guard is unreachable in the typed dispatch:
    // the key is always a 32-byte BOX image and the query a POLYGON image.)
    let key = entry_box(entry);
    let bbox = geo::poly_query_boundbox::call(query.as_ref_bytes());
    let result = rtree_internal_consistent(&key, &bbox, strategy)?;
    Ok(GistConsistentResult {
        matched: result,
        recheck: true,
    })
}

// ===========================================================================
// Circle ops
// ===========================================================================

/// `gist_circle_compress` (gistproc.c) — represent a circle by its bounding box.
fn gist_circle_compress<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    if entry.leafkey {
        let in_ = CIRCLE::from_datum_bytes(entry.key.as_ref_bytes());
        let r = circle_bbox(&in_);
        let retval = gistentryinit(box_datum(mcx, &r)?, entry.rel, entry.page, entry.offset, false);
        return mcx::alloc_in(mcx, retval);
    }
    mcx::alloc_in(mcx, entry.clone())
}

/// The bounding box of a circle (`gist_circle_compress` / `gist_circle_consistent`).
#[inline]
fn circle_bbox(c: &CIRCLE) -> BOX {
    BOX {
        high: Point {
            x: c.center.x + c.radius,
            y: c.center.y + c.radius,
        },
        low: Point {
            x: c.center.x - c.radius,
            y: c.center.y - c.radius,
        },
    }
}

/// `gist_circle_consistent` (gistproc.c) — the GiST consistent method for circles.
/// Inexact (`*recheck = true`); the index entries are bounding boxes, so even
/// leaf nodes use `rtree_internal_consistent`.
fn gist_circle_consistent<'mcx>(
    entry: &GISTENTRY<'mcx>,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistConsistentResult> {
    // All cases served by this function are inexact.
    let key = entry_box(entry);
    let q = CIRCLE::from_datum_bytes(query.as_ref_bytes());
    let bbox = circle_bbox(&q);
    let result = rtree_internal_consistent(&key, &bbox, strategy)?;
    Ok(GistConsistentResult {
        matched: result,
        recheck: true,
    })
}

// ===========================================================================
// Point ops
// ===========================================================================

/// `gist_point_compress` (gistproc.c) — store a leaf point as a degenerate box.
fn gist_point_compress<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    if entry.leafkey {
        // Point, actually.
        let point = Point::from_datum_bytes(entry.key.as_ref_bytes());
        let b = BOX {
            high: point,
            low: point,
        };
        let retval = gistentryinit(
            box_datum(mcx, &b)?,
            entry.rel,
            entry.page,
            entry.offset,
            false,
        );
        return mcx::alloc_in(mcx, retval);
    }
    mcx::alloc_in(mcx, entry.clone())
}

/// `gist_point_fetch` (gistproc.c) — reconstruct the point from its bounding
/// box.
fn gist_point_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    let in_ = entry_box(entry);
    let r = Point {
        x: in_.high.x,
        y: in_.high.y,
    };
    let retval = gistentryinit(
        Datum::ByRef(mcx::slice_in(mcx, &r.to_datum_bytes())?),
        entry.rel,
        entry.page,
        entry.offset,
        false,
    );
    mcx::alloc_in(mcx, retval)
}

/// `computeDistance(isLeaf, box, point)` (gistproc.c) — distance from a point to
/// a box (or to the box's low point at a leaf).
fn computeDistance(isLeaf: bool, b: &BOX, point: &Point) -> PgResult<f64> {
    if isLeaf {
        // Simple point-to-point distance.
        return point_point_distance(point, &b.low);
    }

    if point.x <= b.high.x && point.x >= b.low.x && point.y <= b.high.y && point.y >= b.low.y {
        // Point inside the box.
        return Ok(0.0);
    }

    if point.x <= b.high.x && point.x >= b.low.x {
        // Point is over or below the box.
        if point.y > b.high.y {
            return Ok(float8_mi(point.y, b.high.y));
        } else if point.y < b.low.y {
            return Ok(float8_mi(b.low.y, point.y));
        } else {
            return Err(PgError::error("inconsistent point values"));
        }
    }

    if point.y <= b.high.y && point.y >= b.low.y {
        // Point is to the left or right of the box.
        if point.x > b.high.x {
            return Ok(float8_mi(point.x, b.high.x));
        } else if point.x < b.low.x {
            return Ok(float8_mi(b.low.x, point.x));
        } else {
            return Err(PgError::error("inconsistent point values"));
        }
    }

    // Closest point will be a vertex.
    let mut result = point_point_distance(point, &b.low)?;

    let subresult = point_point_distance(point, &b.high)?;
    if result > subresult {
        result = subresult;
    }

    let p = Point {
        x: b.low.x,
        y: b.high.y,
    };
    let subresult = point_point_distance(point, &p)?;
    if result > subresult {
        result = subresult;
    }

    let p = Point {
        x: b.high.x,
        y: b.low.y,
    };
    let subresult = point_point_distance(point, &p)?;
    if result > subresult {
        result = subresult;
    }

    Ok(result)
}

/// `gist_point_consistent_internal(strategy, isLeaf, key, query)` (gistproc.c).
fn gist_point_consistent_internal(
    strategy: StrategyNumber,
    isLeaf: bool,
    key: &BOX,
    query: &Point,
) -> PgResult<bool> {
    let result = match strategy {
        RTLeftStrategyNumber => geo::FPlt::call(key.low.x, query.x),
        RTRightStrategyNumber => geo::FPgt::call(key.high.x, query.x),
        RTAboveStrategyNumber => geo::FPgt::call(key.high.y, query.y),
        RTBelowStrategyNumber => geo::FPlt::call(key.low.y, query.y),
        RTSameStrategyNumber => {
            if isLeaf {
                // key.high must equal key.low, so we can disregard it.
                FPeq(key.low.x, query.x) && FPeq(key.low.y, query.y)
            } else {
                geo::FPle::call(query.x, key.high.x)
                    && geo::FPge::call(query.x, key.low.x)
                    && geo::FPle::call(query.y, key.high.y)
                    && geo::FPge::call(query.y, key.low.y)
            }
        }
        _ => return Err(unrecognized_strategy(strategy)),
    };
    Ok(result)
}

/// `gist_point_consistent` (gistproc.c).
fn gist_point_consistent<'mcx>(
    entry: &GISTENTRY<'mcx>,
    is_leaf: bool,
    query: &Datum<'mcx>,
    mut strategy: StrategyNumber,
) -> PgResult<GistConsistentResult> {
    // Remap the old strategy spellings.
    if strategy == RTOldBelowStrategyNumber {
        strategy = RTBelowStrategyNumber;
    } else if strategy == RTOldAboveStrategyNumber {
        strategy = RTAboveStrategyNumber;
    }

    let strategyGroup = strategy / GeoStrategyNumberOffset;
    match strategyGroup {
        PointStrategyNumberGroup => {
            let key = entry_box(entry);
            let q = Point::from_datum_bytes(query.as_ref_bytes());
            let result = gist_point_consistent_internal(
                strategy % GeoStrategyNumberOffset,
                is_leaf,
                &key,
                &q,
            )?;
            Ok(GistConsistentResult {
                matched: result,
                recheck: false,
            })
        }
        BoxStrategyNumberGroup => {
            // The only operator here is point <@ box (on_pb), using exact (not
            // fuzzy) comparisons. Leaf keys have high == low, so the same code
            // serves both.
            let q = BOX::from_datum_bytes(query.as_ref_bytes());
            let key = entry_box(entry);
            let result = key.high.x >= q.low.x
                && key.low.x <= q.high.x
                && key.high.y >= q.low.y
                && key.low.y <= q.high.y;
            Ok(GistConsistentResult {
                matched: result,
                recheck: false,
            })
        }
        PolygonStrategyNumberGroup => {
            // POLYGON *query = PG_GETARG_POLYGON_P(1);
            // result = DirectFunctionCall5(gist_poly_consistent, entry, query,
            //          RTOverlapStrategyNumber, 0, &recheck);
            let r = gist_poly_consistent(entry, query, RTOverlapStrategyNumber)?;
            let mut result = r.matched;
            let mut recheck = r.recheck;

            if is_leaf && result {
                // Leaf page: quick check showed overlap of the polygon's
                // bounding box and the point. Confirm the point is in/on the
                // polygon. The leaf key has high == low (a degenerate box / the
                // point itself).
                let box_ = entry_box(entry);
                debug_assert!(box_.high.x == box_.low.x && box_.high.y == box_.low.y);
                result = geo::poly_contain_pt_image::call(query.as_ref_bytes(), &box_.high)?;
                recheck = false;
            }
            Ok(GistConsistentResult {
                matched: result,
                recheck,
            })
        }
        CircleStrategyNumberGroup => {
            // CIRCLE *query = PG_GETARG_CIRCLE_P(1);
            // result = DirectFunctionCall5(gist_circle_consistent, entry, query,
            //          RTOverlapStrategyNumber, 0, &recheck);
            let q = CIRCLE::from_datum_bytes(query.as_ref_bytes());
            let r = gist_circle_consistent(entry, query, RTOverlapStrategyNumber)?;
            let mut result = r.matched;
            let mut recheck = r.recheck;

            if is_leaf && result {
                let box_ = entry_box(entry);
                debug_assert!(box_.high.x == box_.low.x && box_.high.y == box_.low.y);
                result = geo::circle_contain_pt::call(&q, &box_.high)?;
                recheck = false;
            }
            Ok(GistConsistentResult {
                matched: result,
                recheck,
            })
        }
        _ => Err(unrecognized_strategy(strategy)),
    }
}

/// `gist_point_distance` (gistproc.c).
fn gist_point_distance<'mcx>(
    entry: &GISTENTRY<'mcx>,
    is_leaf: bool,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistDistanceResult> {
    let strategyGroup = strategy / GeoStrategyNumberOffset;
    let distance = match strategyGroup {
        PointStrategyNumberGroup => {
            let key = entry_box(entry);
            let q = Point::from_datum_bytes(query.as_ref_bytes());
            computeDistance(is_leaf, &key, &q)?
        }
        _ => return Err(unrecognized_strategy(strategy)),
    };
    // gist_point_distance does not set *recheck (it is left as the caller
    // initialised it; the AM treats point distance as exact).
    Ok(GistDistanceResult {
        distance,
        recheck: false,
    })
}

/// `gist_bbox_distance(entry, query, strategy)` (gistproc.c) — lossy distance
/// from a point to the bounding box of an index entry.
fn gist_bbox_distance(entry: &GISTENTRY<'_>, query: &Point, strategy: StrategyNumber) -> PgResult<f64> {
    let strategyGroup = strategy / GeoStrategyNumberOffset;
    match strategyGroup {
        PointStrategyNumberGroup => {
            let key = entry_box(entry);
            computeDistance(false, &key, query)
        }
        _ => Err(unrecognized_strategy(strategy)),
    }
}

/// `gist_box_distance` (gistproc.c) — distance for the box opclass (exact: no
/// recheck).
fn gist_box_distance<'mcx>(
    entry: &GISTENTRY<'mcx>,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistDistanceResult> {
    let q = Point::from_datum_bytes(query.as_ref_bytes());
    let distance = gist_bbox_distance(entry, &q, strategy)?;
    Ok(GistDistanceResult {
        distance,
        recheck: false,
    })
}

/// `gist_circle_distance` (gistproc.c) — the inexact GiST distance for circles
/// (lossy: distance from the point to the entry's MBR; `*recheck = true`).
fn gist_circle_distance<'mcx>(
    entry: &GISTENTRY<'mcx>,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistDistanceResult> {
    let q = Point::from_datum_bytes(query.as_ref_bytes());
    let distance = gist_bbox_distance(entry, &q, strategy)?;
    Ok(GistDistanceResult {
        distance,
        recheck: true,
    })
}

/// `gist_poly_distance` (gistproc.c) — the inexact GiST distance for polygons
/// (lossy: distance from the point to the entry's MBR; `*recheck = true`).
fn gist_poly_distance<'mcx>(
    entry: &GISTENTRY<'mcx>,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
) -> PgResult<GistDistanceResult> {
    let q = Point::from_datum_bytes(query.as_ref_bytes());
    let distance = gist_bbox_distance(entry, &q, strategy)?;
    Ok(GistDistanceResult {
        distance,
        recheck: true,
    })
}

// ===========================================================================
// Z-order routines for fast index build (gist_point_sortsupport).
// ===========================================================================

/// `point_zorder_internal(x, y)` (gistproc.c) — Z-order (Morton) value of a
/// point.
pub fn point_zorder_internal(x: f32, y: f32) -> u64 {
    let ix = ieee_float32_to_uint32(x) as u64;
    let iy = ieee_float32_to_uint32(y) as u64;
    part_bits32_by2(ix) | (part_bits32_by2(iy) << 1)
}

/// `part_bits32_by2(x)` (gistproc.c) — interleave 32 bits with zeroes.
fn part_bits32_by2(x: u64) -> u64 {
    let mut n = x & 0x0000_0000_FFFF_FFFF;
    n = (n | (n << 16)) & 0x0000_FFFF_0000_FFFF;
    n = (n | (n << 8)) & 0x00FF_00FF_00FF_00FF;
    n = (n | (n << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    n = (n | (n << 2)) & 0x3333_3333_3333_3333;
    n = (n | (n << 1)) & 0x5555_5555_5555_5555;
    n
}

/// `ieee_float32_to_uint32(f)` (gistproc.c) — order-preserving float→uint32.
pub fn ieee_float32_to_uint32(f: f32) -> u32 {
    if f.is_nan() {
        return 0xFFFF_FFFF;
    }
    let mut i = f.to_bits();
    if (i & 0x8000_0000) != 0 {
        // Negative: flip all bits (maps to range 0-7FFFFFFF).
        i ^= 0xFFFF_FFFF;
    } else {
        // Positive (or 0): set the sign bit (maps to range 80000000-FFFFFFFF).
        i |= 0x8000_0000;
    }
    i
}

/// `gist_bbox_zorder_cmp(a, b)` (gistproc.c) — compare the Z-order of the low
/// points of two box keys.
pub fn gist_bbox_zorder_cmp(a: &BOX, b: &BOX) -> i32 {
    let p1 = &a.low;
    let p2 = &b.low;

    // Quick equality check (a worthwhile tie-breaker with abbreviated keys).
    if p1.x == p2.x && p1.y == p2.y {
        return 0;
    }

    let z1 = point_zorder_internal(p1.x as f32, p1.y as f32);
    let z2 = point_zorder_internal(p2.x as f32, p2.y as f32);
    if z1 > z2 {
        1
    } else if z1 < z2 {
        -1
    } else {
        0
    }
}

// ===========================================================================
// Typed support-proc dispatch (mirrors BRIN/SP-GiST opclass-by-OID dispatch).
//
// This crate is the single installer of the GiST core dispatch seams. The
// box/point opclass arms are below; other opclasses (range/inet/tsvector) fold
// their OIDs into these dispatchers when they land.
// ===========================================================================

fn unrecognized_proc(proc_oid: Oid) -> PgError {
    PgError::error(format!("unrecognized GiST support function OID: {proc_oid}"))
}

fn dispatch_consistent<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    entry: &GISTENTRY<'mcx>,
    is_leaf: bool,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
    _subtype: Oid,
) -> PgResult<GistConsistentResult> {
    match proc_oid {
        F_GIST_BOX_CONSISTENT => gist_box_consistent(entry, is_leaf, query, strategy),
        F_GIST_POINT_CONSISTENT => gist_point_consistent(entry, is_leaf, query, strategy),
        F_GIST_POLY_CONSISTENT => gist_poly_consistent(entry, query, strategy),
        F_GIST_CIRCLE_CONSISTENT => gist_circle_consistent(entry, query, strategy),
        F_INET_GIST_CONSISTENT => {
            let key = GistInetKey::from_datum_bytes(entry.key.as_ref_bytes());
            let q = inet_struct::from_datum_bytes(query.as_ref_bytes());
            let (matched, recheck) = inet_gist::inet_gist_consistent::call(key, q, strategy, is_leaf)?;
            Ok(GistConsistentResult { matched, recheck })
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_union<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    entryvec: &GistEntryVector<'mcx>,
) -> PgResult<Datum<'mcx>> {
    match proc_oid {
        F_GIST_BOX_UNION => gist_box_union(mcx, entryvec),
        F_INET_GIST_UNION => {
            let keys = inet_keys_from_vec(entryvec);
            let u = inet_gist::inet_gist_union::call(keys);
            inet_key_datum(mcx, &u)
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_compress<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    match proc_oid {
        F_GIST_POINT_COMPRESS => gist_point_compress(mcx, entry),
        F_INET_GIST_COMPRESS => {
            // inet_gist_compress (network_gist.c:541): only leaf keys are
            // converted; inner entries pass through unchanged.
            if entry.leafkey {
                let in_ = match &entry.key {
                    // DatumGetPointer(entry->key) != NULL
                    Datum::ByRef(_) => Some(inet_struct::from_datum_bytes(entry.key.as_ref_bytes())),
                    // DatumGetPointer(entry->key) == NULL
                    Datum::ByVal(_) => None,
                };
                let r = inet_gist::inet_gist_compress::call(in_);
                let key = match r {
                    Some(k) => inet_key_datum(mcx, &k)?,
                    None => Datum::ByVal(0),
                };
                let retval = gistentryinit(key, entry.rel, entry.page, entry.offset, false);
                return mcx::alloc_in(mcx, retval);
            }
            mcx::alloc_in(mcx, entry.clone())
        }
        F_GIST_POLY_COMPRESS => gist_poly_compress(mcx, entry),
        F_GIST_CIRCLE_COMPRESS => gist_circle_compress(mcx, entry),
        // The box opclass has no compress proc (gistproc.c: "we store boxes as
        // boxes ... so we do not need compress").
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_decompress<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    _entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    // The box/point opclass has no decompress proc (the AM uses the identity
    // decompress when none is registered). A registered decompress OID for
    // another opclass folds in here.
    Err(unrecognized_proc(proc_oid))
}

fn dispatch_penalty<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    origentry: &GISTENTRY<'mcx>,
    newentry: &GISTENTRY<'mcx>,
) -> PgResult<f32> {
    match proc_oid {
        F_GIST_BOX_PENALTY => Ok(gist_box_penalty(origentry, newentry)),
        F_INET_GIST_PENALTY => {
            let orig = GistInetKey::from_datum_bytes(origentry.key.as_ref_bytes());
            let new_ = GistInetKey::from_datum_bytes(newentry.key.as_ref_bytes());
            Ok(inet_gist::inet_gist_penalty::call(orig, new_))
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_picksplit<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    entryvec: &GistEntryVector<'mcx>,
    splitvec: &mut GIST_SPLITVEC<'mcx>,
) -> PgResult<()> {
    match proc_oid {
        F_GIST_BOX_PICKSPLIT => gist_box_picksplit(mcx, entryvec, splitvec),
        F_INET_GIST_PICKSPLIT => {
            let keys = inet_keys_from_vec(entryvec);
            let sv = inet_gist::inet_gist_picksplit::call(keys)?;
            splitvec.spl_left = sv.spl_left;
            splitvec.spl_right = sv.spl_right;
            splitvec.spl_ldatum = Some(inet_key_datum(mcx, &sv.spl_ldatum)?);
            splitvec.spl_ldatum_exists = false;
            splitvec.spl_rdatum = Some(inet_key_datum(mcx, &sv.spl_rdatum)?);
            splitvec.spl_rdatum_exists = false;
            Ok(())
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_same<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    a: &Datum<'mcx>,
    b: &Datum<'mcx>,
) -> PgResult<bool> {
    match proc_oid {
        F_GIST_BOX_SAME => Ok(gist_box_same(a, b)),
        F_INET_GIST_SAME => {
            let left = GistInetKey::from_datum_bytes(a.as_ref_bytes());
            let right = GistInetKey::from_datum_bytes(b.as_ref_bytes());
            Ok(inet_gist::inet_gist_same::call(left, right))
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_distance<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    entry: &GISTENTRY<'mcx>,
    is_leaf: bool,
    query: &Datum<'mcx>,
    strategy: StrategyNumber,
    _subtype: Oid,
) -> PgResult<GistDistanceResult> {
    match proc_oid {
        F_GIST_POINT_DISTANCE => gist_point_distance(entry, is_leaf, query, strategy),
        F_GIST_BOX_DISTANCE => gist_box_distance(entry, query, strategy),
        F_GIST_CIRCLE_DISTANCE => gist_circle_distance(entry, query, strategy),
        F_GIST_POLY_DISTANCE => gist_poly_distance(entry, query, strategy),
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _collation: Oid,
    entry: &GISTENTRY<'mcx>,
) -> PgResult<PgBox<'mcx, GISTENTRY<'mcx>>> {
    match proc_oid {
        F_GIST_POINT_FETCH => gist_point_fetch(mcx, entry),
        F_INET_GIST_FETCH => {
            let key = GistInetKey::from_datum_bytes(entry.key.as_ref_bytes());
            let dst = inet_gist::inet_gist_fetch::call(key);
            let retval = gistentryinit(
                Datum::ByRef(mcx::slice_in(mcx, &dst.to_datum_bytes())?),
                entry.rel,
                entry.page,
                entry.offset,
                false,
            );
            mcx::alloc_in(mcx, retval)
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

fn dispatch_options<'mcx>(
    _mcx: Mcx<'mcx>,
    proc_oid: Oid,
    _relopts: &mut Vec<u8>,
) -> PgResult<()> {
    // box/point have no options procedure; a registered options OID for another
    // opclass folds in here.
    Err(unrecognized_proc(proc_oid))
}

fn dispatch_sortsupport<'mcx>(
    proc_oid: Oid,
    _ssup: &mut SortSupportData<'mcx>,
) -> PgResult<()> {
    match proc_oid {
        F_GIST_POINT_SORTSUPPORT => {
            // gist_point_sortsupport sets ssup->comparator (=
            // gist_bbox_zorder_cmp), and the abbreviation hooks
            // (abbrev_converter = gist_bbox_zorder_abbrev_convert,
            // abbrev_abort, abbrev_full_comparator). The trimmed
            // `types_sortsupport::SortSupportData` carrier has no by-pointer
            // comparator / abbreviation fields and no install path for them,
            // and this leg is reached only from the sorted index-build path,
            // itself gated on `table_index_build_scan`. The z-order comparison
            // logic itself is fully ported above (`gist_bbox_zorder_cmp` /
            // `point_zorder_internal` / `ieee_float32_to_uint32`); only the
            // SortSupportData install is carrier-blocked. Mirror-PG-and-panic
            // until the carrier carries the comparator/abbrev callbacks.
            panic!(
                "gist_point_sortsupport: SortSupportData carrier lacks the \
                 comparator/abbreviation callback fields (sorted GiST build is \
                 gated on table_index_build_scan; z-order logic is ported)"
            )
        }
        _ => Err(unrecognized_proc(proc_oid)),
    }
}

/// Install every GiST opclass-dispatch seam this crate owns.
pub fn init_seams() {
    dispatch::gist_consistent::set(dispatch_consistent);
    dispatch::gist_union::set(dispatch_union);
    dispatch::gist_compress::set(dispatch_compress);
    dispatch::gist_decompress::set(dispatch_decompress);
    dispatch::gist_penalty::set(dispatch_penalty);
    dispatch::gist_picksplit::set(dispatch_picksplit);
    dispatch::gist_same::set(dispatch_same);
    dispatch::gist_distance::set(dispatch_distance);
    dispatch::gist_fetch::set(dispatch_fetch);
    dispatch::gist_options::set(dispatch_options);
    dispatch::gist_sortsupport::set(dispatch_sortsupport);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zorder_orders_floats() {
        // ieee_float32_to_uint32 is order-preserving across the real line.
        assert!(ieee_float32_to_uint32(-1.0) < ieee_float32_to_uint32(0.0));
        assert!(ieee_float32_to_uint32(0.0) < ieee_float32_to_uint32(1.0));
        assert_eq!(ieee_float32_to_uint32(f32::NAN), 0xFFFF_FFFF);
    }

    #[test]
    fn box_union_grows() {
        let a = BOX {
            high: Point { x: 1.0, y: 1.0 },
            low: Point { x: 0.0, y: 0.0 },
        };
        let b = BOX {
            high: Point { x: 3.0, y: 2.0 },
            low: Point { x: 2.0, y: -1.0 },
        };
        let u = rt_box_union(&a, &b);
        assert_eq!(u.high.x, 3.0);
        assert_eq!(u.high.y, 2.0);
        assert_eq!(u.low.x, 0.0);
        assert_eq!(u.low.y, -1.0);
    }

    #[test]
    fn circle_bbox_expands_by_radius() {
        let c = CIRCLE {
            center: Point { x: 5.0, y: 3.0 },
            radius: 2.0,
        };
        let b = circle_bbox(&c);
        assert_eq!(b.high.x, 7.0);
        assert_eq!(b.high.y, 5.0);
        assert_eq!(b.low.x, 3.0);
        assert_eq!(b.low.y, 1.0);
    }

    #[test]
    fn circle_datum_roundtrip() {
        let c = CIRCLE {
            center: Point { x: -1.5, y: 2.25 },
            radius: 4.0,
        };
        let back = CIRCLE::from_datum_bytes(&c.to_datum_bytes());
        assert_eq!(c, back);
    }

    #[test]
    fn size_box_zero_width() {
        let degenerate = BOX {
            high: Point { x: 1.0, y: 1.0 },
            low: Point { x: 1.0, y: 0.0 },
        };
        assert_eq!(size_box(&degenerate), 0.0);
    }
}
