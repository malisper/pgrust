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

pub mod fmgr_builtins;

use dispatch_seams as dispatch;
use gist_proc_seams as gist_sortsupport_seams;
use network_gist_seams as inet_gist;
use geo_ops_seams as geo;
use rangetypes_gist as range_gist;
use tsgistidx as tsgist;
use tsquery_core::gist as tsqgist;
use tsearch::tsearch::TSQuerySign;
use tsearch::tsgistidx::SignTsVector;
use types_network::GistInetKey;
use dispatch::{GistConsistentResult, GistDistanceResult, StrategyNumber};
use mcx::{Mcx, PgBox};
use types_core::geo::{Point, CIRCLE, BOX};
use types_core::primitive::{Oid, OffsetNumber};
use types_error::{PgError, PgResult};
use gist::{GistEntryVector, GISTENTRY, GIST_SPLITVEC};
use types_sortsupport::SortSupportData;
use types_tuple::heaptuple::Datum;

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

/// `gist_bbox_zorder_cmp(Datum a, Datum b, SortSupport ssup)` (gistproc.c) at
/// the sort-engine boundary: the operands are pass-by-reference `BOX` images
/// (`DatumGetBoxP(a)->low`), so each canonical [`Datum`] crosses as `ByRef`. We
/// decode the `BOX` from its bytes and run the pure z-order comparison. `ssup`
/// is unused (the kernel reads neither it nor any per-call state).
fn gist_bbox_zorder_cmp_datum(a: Datum<'_>, b: Datum<'_>) -> i32 {
    let a = BOX::from_datum_bytes(a.as_ref_bytes());
    let b = BOX::from_datum_bytes(b.as_ref_bytes());
    gist_bbox_zorder_cmp(&a, &b)
}

/// `gist_bbox_zorder_abbrev_convert(Datum original, SortSupport ssup)`
/// (gistproc.c) — the Z-order abbreviated key of a box's low point. The
/// `original` Datum is the pass-by-reference `BOX` image (`ByRef`); the result
/// is the pass-by-value Z-order word.
///
/// C `#if SIZEOF_DATUM == 8` returns `(Datum) z` (the full 64-bit Z-order). This
/// is the only platform target (matching the int8-by-value assumption elsewhere
/// in the tree), so we return the full word. `ssup` is unused.
fn gist_bbox_zorder_abbrev_convert(original: Datum<'_>) -> Datum<'static> {
    let b = BOX::from_datum_bytes(original.as_ref_bytes());
    let z = point_zorder_internal(b.low.x as f32, b.low.y as f32);
    // C: `return (Datum) z;` on SIZEOF_DATUM == 8 — the unsigned word verbatim.
    Datum::from_u64(z)
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

// ---------------------------------------------------------------------------
// range_ops / multirange_ops opclass support-proc OIDs (pg_proc.dat) and the
// marshalling between the GiST core's by-reference key [`Datum`] lane and the
// range ADT's `DatumGetRangeTypeP` pointer-word convention.
//
// Range GiST keys are stored as plain (never-toasted) `RangeType *`; the range
// bodies read `entry->key` as `DatumGetRangeTypeP(key)` — i.e. they treat the
// key Datum's word as the address of a `RangeType` varlena. The GiST core hands
// us the key as a by-reference [`Datum::ByRef`] image (the on-disk varlena
// bytes), so we materialize that image into an 8-byte-aligned `mcx` buffer and
// hand the body a `RangeTypeP { ptr }` over it (mirroring
// `range_bytes_to_arg_word` in `rangetypes`'s fmgr boundary). Symmetrically a
// `RangeType` RESULT (union / picksplit union keys) comes back as a pointer
// word into `mcx`; we copy the full varlena image off it onto the by-reference
// result lane as `Datum::ByRef`.
// ---------------------------------------------------------------------------

/// `range_gist_consistent` (pg_proc.dat oid 3875).
const F_RANGE_GIST_CONSISTENT: Oid = 3875;
/// `range_gist_union` (pg_proc.dat oid 3876).
const F_RANGE_GIST_UNION: Oid = 3876;
/// `range_gist_penalty` (pg_proc.dat oid 3879).
const F_RANGE_GIST_PENALTY: Oid = 3879;
/// `range_gist_picksplit` (pg_proc.dat oid 3880).
const F_RANGE_GIST_PICKSPLIT: Oid = 3880;
/// `range_gist_same` (pg_proc.dat oid 3881).
const F_RANGE_GIST_SAME: Oid = 3881;
/// `range_sortsupport` (pg_proc.dat oid 6391).
const F_RANGE_GIST_SORTSUPPORT: Oid = 6391;
/// `multirange_gist_consistent` (pg_proc.dat oid 6154).
const F_MULTIRANGE_GIST_CONSISTENT: Oid = 6154;
/// `multirange_gist_compress` (pg_proc.dat oid 6156).
const F_MULTIRANGE_GIST_COMPRESS: Oid = 6156;

// ---------------------------------------------------------------------------
// tsvector_ops opclass support-proc OIDs (pg_proc.dat) and the marshalling
// between the GiST core's by-reference key [`Datum`] lane and the
// [`SignTsVector`] GiST key (`tsgistidx.c`).
//
// The leaf input value is a `tsvector` varlena image (compress reads its
// lexemes via `ARRPTR`/`STRPTR`); every other key is a `SignTSVector` varlena
// image which round-trips through [`SignTsVector::from_image`] /
// [`SignTsVector::to_image`] (the on-disk `flag`+`data[]` form). The consistent
// query is a `tsquery` varlena image decoded into `QueryItem`s by `ts-small`.
//
// DIVERGENCE (faithful, deferred): the configured `siglen` opclass option
// (`GET_SIGLEN()` = `fcinfo->flinfo->fn_opts`) is not threaded to the GiST
// support procs in the owned model — the dispatch seams carry no `fn_opts`, and
// the relcache deliberately drops `set_fn_opclass_options` on the support
// `FmgrInfo` (see `backend-utils-cache-relcache` `index_getprocinfo`). The
// build-side procs (compress/union/penalty/picksplit) therefore use
// `SIGLEN_DEFAULT`. This is correctness-preserving for queries: every
// `gtsvector_consistent` returns `recheck = true` (the heap recheck makes the
// scan exact regardless of the signature length), and the read-side consistent
// reads `siglen` from the stored key itself. Only the physical signature length
// of an index built with an explicit non-default `siglen` differs from C.
// ---------------------------------------------------------------------------

/// `gtsvector_compress` (pg_proc.dat oid 3648).
const F_GTSVECTOR_COMPRESS: Oid = 3648;
/// `gtsvector_decompress` (pg_proc.dat oid 3649).
const F_GTSVECTOR_DECOMPRESS: Oid = 3649;
/// `gtsvector_picksplit` (pg_proc.dat oid 3650).
const F_GTSVECTOR_PICKSPLIT: Oid = 3650;
/// `gtsvector_union` (pg_proc.dat oid 3651).
const F_GTSVECTOR_UNION: Oid = 3651;
/// `gtsvector_same` (pg_proc.dat oid 3652).
const F_GTSVECTOR_SAME: Oid = 3652;
/// `gtsvector_penalty` (pg_proc.dat oid 3653).
const F_GTSVECTOR_PENALTY: Oid = 3653;
/// `gtsvector_consistent` (pg_proc.dat oid 3654).
const F_GTSVECTOR_CONSISTENT: Oid = 3654;
/// `gtsvector_consistent` obsolete 4-arg signature (pg_proc.dat oid 3790).
const F_GTSVECTOR_CONSISTENT_OLDSIG: Oid = 3790;
/// `gtsvector_options` (pg_proc.dat oid 3434).
const F_GTSVECTOR_OPTIONS: Oid = 3434;

/// Decode the GiST by-reference key image into a [`SignTsVector`] (the
/// `DatumGetPointer(entry->key)` form the gtsvector bodies read). C's
/// `gtsvector_decompress` first `PG_DETOAST_DATUM`s the key, and every body then
/// reads `VARDATA(x)` over a plain 4-byte-header image; the stored key may carry
/// a 1-byte ("short") header or be compressed, so we detoast to a canonical
/// 4-byte-header image and strip the header (`VARDATA_ANY`).
fn signtsvector_from_key<'mcx>(
    mcx: Mcx<'mcx>,
    key: &Datum<'mcx>,
) -> PgResult<SignTsVector> {
    let image = detoast_seams::detoast_attr::call(mcx, key.as_ref_bytes())?;
    SignTsVector::from_image(&image[4..])
        .ok_or_else(|| PgError::error("corrupt gtsvector GiST key".to_string()))
}

/// Decode an entry-vector key, tolerating the index-0 placeholder slot
/// (`entryvec->vector[0]`, carried as `Datum::ByVal(0)`) that the C union /
/// picksplit methods keep present but never read (they index 1-based from
/// `FirstOffsetNumber`). A placeholder decodes to an empty `ARRKEY` — a valid
/// `SignTsVector` the methods skip.
fn signtsvector_from_key_or_placeholder<'mcx>(
    mcx: Mcx<'mcx>,
    key: &Datum<'mcx>,
) -> PgResult<SignTsVector> {
    if matches!(key, Datum::ByVal(_)) {
        return Ok(SignTsVector {
            flag: tsearch::tsgistidx::ARRKEY,
            data: tsearch::tsgistidx::SignTsVectorData::Arr(Vec::new()),
        });
    }
    signtsvector_from_key(mcx, key)
}

/// Copy a [`SignTsVector`] result onto the GiST by-reference key lane as its
/// full varlena image (`PointerGetDatum(res)`).
fn signtsvector_result_datum<'mcx>(
    mcx: Mcx<'mcx>,
    key: &SignTsVector,
) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &key.to_image())?))
}

// ---------------------------------------------------------------------------
// tsquery_ops opclass support-proc OIDs (pg_proc.dat). The GiST key is a
// [`TSQuerySign`] (`uint64`) carried by value (`TSQuerySignGetDatum` =
// `Int64GetDatum`), so the key [`Datum`] is `ByVal`. The leaf compress input is
// a `tsquery` varlena image; consistent's query is a `tsquery` varlena. The
// signature is a fixed `TSQS_SIGLEN`-bit word with no opclass options.
// ---------------------------------------------------------------------------

/// `gtsquery_compress` (pg_proc.dat oid 3695).
const F_GTSQUERY_COMPRESS: Oid = 3695;
/// `gtsquery_picksplit` (pg_proc.dat oid 3697).
const F_GTSQUERY_PICKSPLIT: Oid = 3697;
/// `gtsquery_union` (pg_proc.dat oid 3698).
const F_GTSQUERY_UNION: Oid = 3698;
/// `gtsquery_same` (pg_proc.dat oid 3699).
const F_GTSQUERY_SAME: Oid = 3699;
/// `gtsquery_penalty` (pg_proc.dat oid 3700).
const F_GTSQUERY_PENALTY: Oid = 3700;
/// `gtsquery_consistent` (pg_proc.dat oid 3701).
const F_GTSQUERY_CONSISTENT: Oid = 3701;
/// `gtsquery_consistent` obsolete 4-arg signature (pg_proc.dat oid 3793).
const F_GTSQUERY_CONSISTENT_OLDSIG: Oid = 3793;

/// `DatumGetTSQuerySign(key)` — read the by-value `uint64` signature word.
fn tsquerysign_from_key(key: &Datum<'_>) -> TSQuerySign {
    match key {
        Datum::ByVal(w) => *w as TSQuerySign,
        // The signature is by-value; a by-reference key would be a corruption.
        _ => key.as_usize() as TSQuerySign,
    }
}

/// `VARSIZE_4B(ptr)` — the total image length of a plain (uncompressed, 4-byte
/// header) varlena. A serialized `RangeType` / `MultirangeType` always carries a
/// plain 4B header (`SET_VARSIZE`), so this is the exact byte length.
///
/// # Safety
/// `ptr` must point at a valid plain 4B varlena header.
#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    let word = (ptr as *const u32).read_unaligned();
    ((word >> 2) & 0x3FFF_FFFF) as usize
}

/// Materialize a by-reference varlena `image` (the GiST key bytes, header and
/// all) into an 8-byte-aligned (`MAXALIGN`) `mcx` copy in canonical 4-byte-header
/// form and return its address word. The range/multirange ADTs read `VARSIZE_4B`,
/// the `rangetypid` at the fixed `sizeof(RangeType)` header offset, and the bound
/// payload past it, and their relative-offset payload accounting only matches
/// absolute-address reads when the base is `MAXALIGN(8)`-aligned (the alignment
/// `range_serialize` produces).
///
/// C's `DatumGetRangeTypeP`/`DatumGetMultirangeTypeP` are `PG_DETOAST_DATUM`,
/// which un-packs a short (1-byte) header to the 4-byte form. This port stores
/// varlenas header-ful while `SHORT_VARLENA_PACKING` is off (every key is already
/// 4B, so the copy is verbatim); but once the flag is on `index_form_tuple`
/// short-packs a small key and `fetchatt` hands the support proc the verbatim
/// short-headed on-disk image. Un-pack short -> 4B here so the ADT's fixed-offset
/// reads land correctly. (Compressed/external images are detoasted upstream — the
/// GiST decompress / `gistdentryinit` path — so only the short<->4B inline forms
/// reach this boundary.)
fn materialize_varlena<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<*const u8> {
    use allocator_api2::alloc::Allocator;
    use core::alloc::Layout;

    // VARATT_IS_1B (short header) and not VARATT_IS_1B_E (0x01, external): un-pack
    // the 1-byte-header payload into a fresh 4-byte-header image.
    let short = matches!(image.first(), Some(&h) if h != 0x01 && (h & 0x01) == 0x01);
    if short {
        // VARSIZE_1B(image) = (va_header >> 1) & 0x7F covers the 1-byte header +
        // payload; the payload is that minus VARHDRSZ_SHORT (1).
        let total_1b = ((image[0] >> 1) & 0x7F) as usize;
        let data_size = total_1b.saturating_sub(1);
        let new_size = data_size + 4; // VARHDRSZ
        mcx::check_alloc_size(new_size)?;
        let layout = Layout::from_size_align(new_size.max(1), 8)
            .expect("valid varlena image layout");
        let block = mcx.allocate(layout).map_err(|_| mcx.oom(new_size))?;
        let dst = block.as_ptr() as *mut u8;
        // SAFETY: `dst` heads a freshly allocated new_size-byte region; write the
        // 4-byte length word (SET_VARSIZE) then copy the short payload past it.
        unsafe {
            let word = (new_size as u32) << 2; // low 2 bits 00 = plain 4B header
            core::ptr::copy_nonoverlapping(word.to_ne_bytes().as_ptr(), dst, 4);
            core::ptr::copy_nonoverlapping(image.as_ptr().add(1), dst.add(4), data_size);
        }
        return Ok(dst as *const u8);
    }

    mcx::check_alloc_size(image.len())?;
    let layout = Layout::from_size_align(image.len().max(1), 8)
        .expect("valid varlena image layout");
    let block = mcx.allocate(layout).map_err(|_| mcx.oom(image.len()))?;
    let dst = block.as_ptr() as *mut u8;
    // SAFETY: `dst` heads a freshly allocated image.len()-byte region.
    unsafe {
        core::ptr::copy_nonoverlapping(image.as_ptr(), dst, image.len());
    }
    Ok(dst as *const u8)
}

/// `DatumGetRangeTypeP(entry->key)` at the GiST dispatch boundary: materialize
/// the by-reference key image into `mcx` and build a `RangeTypeP` over it.
fn range_key_from_entry<'mcx>(
    mcx: Mcx<'mcx>,
    key: &Datum<'mcx>,
) -> PgResult<types_rangetypes::RangeTypeP<'mcx>> {
    let ptr = materialize_varlena(mcx, key.as_ref_bytes())?;
    Ok(types_rangetypes::RangeTypeP {
        ptr: ptr as *const types_rangetypes::RangeType,
        _marker: core::marker::PhantomData,
    })
}

/// `DatumGetMultirangeTypeP(query)` at the GiST dispatch boundary.
fn multirange_from_datum<'mcx>(
    mcx: Mcx<'mcx>,
    d: &Datum<'mcx>,
) -> PgResult<types_rangetypes::MultirangeTypeP<'mcx>> {
    let ptr = materialize_varlena(mcx, d.as_ref_bytes())?;
    Ok(types_rangetypes::MultirangeTypeP {
        ptr: ptr as *const types_rangetypes::MultirangeType,
        _marker: core::marker::PhantomData,
    })
}

/// Build the [`range_gist::GistQuery`] for a range/multirange consistent call
/// from the GiST core's by-reference query [`Datum`] and the operator's
/// right-hand-side `subtype` (`PG_GETARG_OID(3)`).
///
/// In C, the decode lives *inside* each consistent function, so an **invalid
/// subtype means the query type matches the index key type** (rangetypes_gist.c
/// "Note that invalid subtype means that query type matches key type"):
///   * `range_gist_consistent`:      invalid/`ANYRANGEOID` => `DatumGetRangeTypeP`,
///                                   `ANYMULTIRANGEOID`     => `DatumGetMultirangeTypeP`,
///                                   else                   => bare element.
///   * `multirange_gist_consistent`: invalid/`ANYMULTIRANGEOID` => `DatumGetMultirangeTypeP`,
///                                   `ANYRANGEOID`              => `DatumGetRangeTypeP`,
///                                   else                       => bare element.
///
/// `key_is_multirange` selects which of those two dispatch tables to use (true
/// for `multirange_gist_consistent`, where the indexed key — and the default
/// query — is a multirange).
fn gist_range_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Datum<'mcx>,
    subtype: Oid,
    key_is_multirange: bool,
) -> PgResult<range_gist::GistQuery<'mcx>> {
    let invalid = !types_core::primitive::OidIsValid(subtype);
    if key_is_multirange {
        if invalid || subtype == range_gist::ANYMULTIRANGEOID {
            Ok(range_gist::GistQuery::Multirange(multirange_from_datum(mcx, query)?))
        } else if subtype == range_gist::ANYRANGEOID {
            Ok(range_gist::GistQuery::Range(range_key_from_entry(mcx, query)?))
        } else {
            Ok(range_gist::GistQuery::Elem(elem_word(query)))
        }
    } else if invalid || subtype == range_gist::ANYRANGEOID {
        Ok(range_gist::GistQuery::Range(range_key_from_entry(mcx, query)?))
    } else if subtype == range_gist::ANYMULTIRANGEOID {
        Ok(range_gist::GistQuery::Multirange(multirange_from_datum(mcx, query)?))
    } else {
        // The bare element value (`query`), as the range ADT's bare-word Datum.
        Ok(range_gist::GistQuery::Elem(elem_word(query)))
    }
}

/// Convert the GiST core's by-reference / by-value query [`Datum`] into the
/// range ADT's bare-word `Datum` (`datum::datum::Datum`). A by-value
/// element (e.g. `int4`) carries its word directly; a by-reference element
/// carries the address of its (already-detoasted) image. The element comparison
/// proc the range body invokes reads exactly that word (`DatumGetX`).
fn elem_word(d: &Datum<'_>) -> datum::datum::Datum {
    let word = match d {
        Datum::ByVal(w) => *w,
        // A by-reference element's word is the address of its image bytes; the
        // bytes are owned by the caller's lane for the duration of the consistent
        // call, so the pointer is valid across it.
        _ => d.as_ref_bytes().as_ptr() as usize,
    };
    datum::datum::Datum::from_usize(word)
}

/// Copy a `RangeType` RESULT (a pointer word into `mcx`) off onto the GiST
/// by-reference key lane as the full varlena image (`Datum::ByRef`), the form a
/// GiST union / split key takes (`RangeTypePGetDatum`).
fn range_result_datum<'mcx>(
    mcx: Mcx<'mcx>,
    r: types_rangetypes::RangeTypeP<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // SAFETY: `r.ptr` is a plain `RangeType` varlena the body allocated in `mcx`
    // (it lives for `'mcx`).
    let bytes = unsafe {
        let len = varsize_4b(r.ptr as *const u8);
        core::slice::from_raw_parts(r.ptr as *const u8, len)
    };
    Ok(Datum::ByRef(mcx::slice_in(mcx, bytes)?))
}

/// Copy a pointer-word `RangeType *` key [`Datum`] (the form the range body
/// emits via `RangeTypePGetDatum`) onto the GiST by-reference key lane as its
/// full varlena image (`Datum::ByRef`).
fn range_word_datum_to_byref<'mcx>(
    mcx: Mcx<'mcx>,
    d: &Datum<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let r = types_rangetypes::RangeTypeP {
        ptr: d.as_usize() as *const types_rangetypes::RangeType,
        _marker: core::marker::PhantomData,
    };
    range_result_datum(mcx, r)
}

/// Rebuild `entryvec` with each entry's key materialized into `mcx` as a
/// pointer-word `Datum` (the `DatumGetRangeTypeP` form `entry_range` reads).
/// The range union / picksplit bodies index `entryvec->vector[i].key` and call
/// `DatumGetRangeTypeP` on the word, so every key must be a live `RangeType *`
/// address (not the on-disk by-reference image the GiST core supplies).
fn range_entryvec<'mcx>(
    mcx: Mcx<'mcx>,
    entryvec: &GistEntryVector<'mcx>,
) -> PgResult<GistEntryVector<'mcx>> {
    let mut vector = Vec::with_capacity(entryvec.vector.len());
    for e in &entryvec.vector {
        // A null/by-value key (the placeholder slot at index 0 the methods skip,
        // or a NULL key) has no by-reference image; pass its word through.
        let key = match &e.key {
            Datum::ByRef(_) => {
                let ptr = materialize_varlena(mcx, e.key.as_ref_bytes())?;
                Datum::from_usize(ptr as usize)
            }
            other => other.clone(),
        };
        vector.push(GISTENTRY {
            key,
            rel: e.rel,
            page: e.page,
            offset: e.offset,
            leafkey: e.leafkey,
        });
    }
    Ok(GistEntryVector {
        n: entryvec.n,
        vector,
    })
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
            // The query is a real `inet`/`cidr` SQL varlena (`PG_GETARG_INET_PP`),
            // so it must be detoasted and decoded at `VARDATA_ANY` rather than read
            // as the header-less `GistInetKey`/`inet` byte image the index key uses.
            let q = network_seams::inet::datum_get_inet_pp::call(_mcx, query)?;
            let (matched, recheck) = inet_gist::inet_gist_consistent::call(key, q, strategy, is_leaf)?;
            Ok(GistConsistentResult { matched, recheck })
        }
        F_RANGE_GIST_CONSISTENT => {
            let key = range_key_from_entry(_mcx, &entry.key)?;
            let q = gist_range_query(_mcx, query, _subtype, false)?;
            let (matched, recheck) =
                range_gist::range_gist_consistent(_mcx, is_leaf, key, &q, strategy, _subtype)?;
            Ok(GistConsistentResult { matched, recheck })
        }
        F_MULTIRANGE_GIST_CONSISTENT => {
            let key = range_key_from_entry(_mcx, &entry.key)?;
            let q = gist_range_query(_mcx, query, _subtype, true)?;
            let (matched, recheck) =
                range_gist::multirange_gist_consistent(_mcx, is_leaf, key, &q, strategy, _subtype)?;
            Ok(GistConsistentResult { matched, recheck })
        }
        F_GTSVECTOR_CONSISTENT | F_GTSVECTOR_CONSISTENT_OLDSIG => {
            // gtsvector_consistent(entry, query, strategy, subtype, recheck):
            // the key is a detoasted SignTSVector; the query is a tsquery
            // varlena decoded into QueryItems. `strategy`/`subtype`/`is_leaf`
            // are unused by the body (it dispatches on the key's flag).
            let key = signtsvector_from_key(_mcx, &entry.key)?;
            let query_image = query.as_ref_bytes();
            let query_size = ts_small::util::tsq_size(query_image);
            let query_items = ts_small::util::get_query(query_image)?;
            let (matched, recheck) =
                tsgist::gtsvector_consistent(_mcx, &key, &query_items, query_size)?;
            Ok(GistConsistentResult { matched, recheck })
        }
        F_GTSQUERY_CONSISTENT | F_GTSQUERY_CONSISTENT_OLDSIG => {
            // gtsquery_consistent(entry, query, strategy, subtype, recheck): the
            // key is the by-value TSQuerySign; the query is a tsquery varlena.
            let key = tsquerysign_from_key(&entry.key);
            let q = query.as_ref_bytes();
            let (matched, recheck) = tsqgist::gtsquery_consistent(key, q, strategy, is_leaf)?;
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
        F_RANGE_GIST_UNION => {
            let evec = range_entryvec(mcx, entryvec)?;
            let r = range_gist::range_gist_union(mcx, &evec)?;
            range_result_datum(mcx, r)
        }
        F_GTSVECTOR_UNION => {
            // gtsvector_union iterates GETENTRY(entryvec, i) for i in 0..n.
            let n = entryvec.n as usize;
            let mut keys: Vec<SignTsVector> = Vec::with_capacity(n);
            for e in entryvec.vector.iter().take(n) {
                keys.push(signtsvector_from_key_or_placeholder(mcx, &e.key)?);
            }
            let refs: Vec<&SignTsVector> = keys.iter().collect();
            let u = tsgist::gtsvector_union(&refs, tsgist::SIGLEN_DEFAULT);
            signtsvector_result_datum(mcx, &u)
        }
        F_GTSQUERY_UNION => {
            // gtsquery_union ORs every entry's by-value TSQuerySign.
            let n = entryvec.n as usize;
            let signs: Vec<TSQuerySign> = entryvec
                .vector
                .iter()
                .take(n)
                .map(|e| tsquerysign_from_key(&e.key))
                .collect();
            let u = tsqgist::gtsquery_union(&signs);
            Ok(Datum::from_u64(u))
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
                    // DatumGetPointer(entry->key) != NULL. The leaf key is the
                    // original `inet`/`cidr` column varlena (`PG_GETARG_INET_PP`),
                    // so detoast and decode at `VARDATA_ANY` rather than reading the
                    // header-less `GistInetKey` byte image.
                    Datum::ByRef(_) => {
                        Some(network_seams::inet::datum_get_inet_pp::call(mcx, &entry.key)?)
                    }
                    // DatumGetPointer(entry->key) == NULL
                    Datum::ByVal(_) => None,
                    Datum::Cstring(_) | Datum::Composite(_) | Datum::Expanded(_) | Datum::Internal(_) => {
                        panic!("inet_gist_compress: non-ByVal/ByRef Datum key not yet produced — wave 2")
                    }
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
        F_MULTIRANGE_GIST_COMPRESS => {
            // multirange_gist_compress: a leaf multirange is approximated by its
            // union range; an inner entry passes through unchanged. The leaf key
            // arrives as the on-disk MultirangeType by-reference image, which the
            // body reads as `DatumGetMultirangeTypeP(entry->key)`.
            if entry.leafkey {
                let mr = multirange_from_datum(mcx, &entry.key)?;
                let staged = GISTENTRY {
                    key: Datum::from_usize(0),
                    rel: entry.rel,
                    page: entry.page,
                    offset: entry.offset,
                    leafkey: entry.leafkey,
                };
                let out = range_gist::multirange_gist_compress(mcx, &staged, mr)?;
                // The body packed the union RangeType as a pointer-word key; copy
                // its varlena image onto the by-reference key lane.
                let r = types_rangetypes::RangeTypeP {
                    ptr: out.key.as_usize() as *const types_rangetypes::RangeType,
                    _marker: core::marker::PhantomData,
                };
                let key = range_result_datum(mcx, r)?;
                let retval = gistentryinit(key, out.rel, out.page, out.offset, out.leafkey);
                return mcx::alloc_in(mcx, retval);
            }
            mcx::alloc_in(mcx, entry.clone())
        }
        F_GTSVECTOR_COMPRESS => {
            if entry.leafkey {
                // The leaf value is a `tsvector` varlena (C: `DatumGetTSVector`
                // = PG_DETOAST_DATUM); detoast to a plain 4-byte-header image and
                // extract its per-lexeme byte slices (ARRPTR/STRPTR) to build the
                // array key. See the DIVERGENCE note: build uses SIGLEN_DEFAULT.
                use tsvector_core::access::{arrptr, lexeme, tsv_size};
                let detoasted = detoast_seams::detoast_attr::call(
                    mcx,
                    entry.key.as_ref_bytes(),
                )?;
                let image: &[u8] = &detoasted;
                let size = tsv_size(image);
                let mut lexemes: Vec<&[u8]> = Vec::with_capacity(size.max(0) as usize);
                for i in 0..size as usize {
                    let e = arrptr(image, i);
                    lexemes.push(lexeme(image, size, e));
                }
                let res = tsgist::gtsvector_compress_leaf(&lexemes, tsgist::SIGLEN_DEFAULT);
                let key = signtsvector_result_datum(mcx, &res)?;
                let retval = gistentryinit(key, entry.rel, entry.page, entry.offset, false);
                return mcx::alloc_in(mcx, retval);
            }
            // Inner entry: rewrite an all-0xff SIGNKEY as ALLISTRUE; otherwise
            // pass through unchanged. A NULL key (`DatumGetPointer(entry->key)`
            // == NULL, carried as `Datum::ByVal(0)`) is not a SIGNKEY, so C's
            // `else if (ISSIGNKEY(..) && !ISALLTRUE(..))` falls through to
            // `retval = entry` — pass it through without decoding.
            if matches!(entry.key, Datum::ByVal(_)) {
                return mcx::alloc_in(mcx, entry.clone());
            }
            let key = signtsvector_from_key(mcx, &entry.key)?;
            if key.is_signkey() && !key.is_alltrue() {
                // The all-0xff scan walks the stored signature; use its own
                // length (the build-time siglen baked into the key).
                let key_siglen = key.sign().len() as i32;
                if let Some(res) =
                    tsgist::gtsvector_compress_inner_alltrue(&key, key_siglen)
                {
                    let datum = signtsvector_result_datum(mcx, &res)?;
                    let retval = gistentryinit(datum, entry.rel, entry.page, entry.offset, false);
                    return mcx::alloc_in(mcx, retval);
                }
            }
            mcx::alloc_in(mcx, entry.clone())
        }
        F_GTSQUERY_COMPRESS => {
            // gtsquery_compress (tsquery_gist.c:30): leaf — turn the tsquery
            // image into its by-value TSQuerySign; non-leaf — identity.
            if entry.leafkey {
                let sign = tsqgist::gtsquery_compress_leaf(entry.key.as_ref_bytes())?;
                let retval =
                    gistentryinit(Datum::from_u64(sign), entry.rel, entry.page, entry.offset, false);
                return mcx::alloc_in(mcx, retval);
            }
            mcx::alloc_in(mcx, entry.clone())
        }
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
    // gtsvector_decompress (tsgistidx.c:242) only detoasts the stored key; on
    // the owned by-reference lane the key is already a plain image, so it is
    // the identity (return the entry unchanged).
    if proc_oid == F_GTSVECTOR_DECOMPRESS {
        return mcx::alloc_in(_mcx, _entry.clone());
    }
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
        F_RANGE_GIST_PENALTY => {
            let orig = range_key_from_entry(_mcx, &origentry.key)?;
            let new_ = range_key_from_entry(_mcx, &newentry.key)?;
            range_gist::range_gist_penalty(_mcx, orig, new_)
        }
        F_GTSVECTOR_PENALTY => {
            // origval is always ISSIGNKEY (the build-time signature); newval is
            // an ARRKEY (leaf) or a SIGNKEY (inner). siglen is the stored
            // signature length (DEFAULT for an ALLISTRUE origval which carries
            // no payload).
            let origval = signtsvector_from_key(_mcx, &origentry.key)?;
            let newval = signtsvector_from_key(_mcx, &newentry.key)?;
            let siglen = if origval.is_signkey() && !origval.is_alltrue() {
                origval.sign().len() as i32
            } else {
                tsgist::SIGLEN_DEFAULT
            };
            tsgist::gtsvector_penalty(_mcx, &origval, &newval, siglen)
        }
        F_GTSQUERY_PENALTY => {
            let orig = tsquerysign_from_key(&origentry.key);
            let new_ = tsquerysign_from_key(&newentry.key);
            Ok(tsqgist::gtsquery_penalty(orig, new_))
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
        F_RANGE_GIST_PICKSPLIT => {
            let evec = range_entryvec(mcx, entryvec)?;
            let sv = range_gist::range_gist_picksplit(mcx, &evec)?;
            splitvec.spl_left = sv.spl_left;
            splitvec.spl_right = sv.spl_right;
            // The body packed the per-group union keys as pointer-word
            // `RangeType *`; copy their varlena images onto the by-reference key
            // lane (the form the GiST core's split machinery consumes).
            splitvec.spl_ldatum = match sv.spl_ldatum {
                Some(d) => Some(range_word_datum_to_byref(mcx, &d)?),
                None => None,
            };
            splitvec.spl_ldatum_exists = sv.spl_ldatum_exists;
            splitvec.spl_rdatum = match sv.spl_rdatum {
                Some(d) => Some(range_word_datum_to_byref(mcx, &d)?),
                None => None,
            };
            splitvec.spl_rdatum_exists = sv.spl_rdatum_exists;
            Ok(())
        }
        F_GTSVECTOR_PICKSPLIT => {
            // The body indexes entries[FirstOffsetNumber..=entryvec_n-1] (1-based
            // offset numbers); decode every key into a parallel Vec and pass
            // borrowed refs in the same index order (index 0 is the unread
            // placeholder slot).
            let n = entryvec.n as usize;
            let mut keys: Vec<SignTsVector> = Vec::with_capacity(n);
            for e in entryvec.vector.iter().take(n) {
                keys.push(signtsvector_from_key_or_placeholder(mcx, &e.key)?);
            }
            let refs: Vec<&SignTsVector> = keys.iter().collect();
            let sv =
                tsgist::gtsvector_picksplit(mcx, &refs, entryvec.n, tsgist::SIGLEN_DEFAULT)?;
            splitvec.spl_left = sv.spl_left;
            splitvec.spl_right = sv.spl_right;
            splitvec.spl_ldatum = match sv.spl_ldatum {
                Some(k) => Some(signtsvector_result_datum(mcx, &k)?),
                None => None,
            };
            splitvec.spl_ldatum_exists = false;
            splitvec.spl_rdatum = match sv.spl_rdatum {
                Some(k) => Some(signtsvector_result_datum(mcx, &k)?),
                None => None,
            };
            splitvec.spl_rdatum_exists = false;
            Ok(())
        }
        F_GTSQUERY_PICKSPLIT => {
            // entrysign holds every entry's by-value TSQuerySign indexed exactly
            // as GETENTRY(entryvec, pos) (entrysign.len() == entryvec->n).
            let n = entryvec.n as usize;
            let signs: Vec<TSQuerySign> = entryvec
                .vector
                .iter()
                .take(n)
                .map(|e| tsquerysign_from_key(&e.key))
                .collect();
            let sv = tsqgist::gtsquery_picksplit(mcx, &signs)?;
            splitvec.spl_left = sv.spl_left;
            splitvec.spl_right = sv.spl_right;
            splitvec.spl_ldatum = Some(Datum::from_u64(sv.spl_ldatum));
            splitvec.spl_ldatum_exists = false;
            splitvec.spl_rdatum = Some(Datum::from_u64(sv.spl_rdatum));
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
        F_RANGE_GIST_SAME => {
            let r1 = range_key_from_entry(_mcx, a)?;
            let r2 = range_key_from_entry(_mcx, b)?;
            range_gist::range_gist_same(r1, r2)
        }
        F_GTSVECTOR_SAME => {
            let ka = signtsvector_from_key(_mcx, a)?;
            let kb = signtsvector_from_key(_mcx, b)?;
            // Both keys are the same form (the AM only compares like keys);
            // siglen is the stored signature length (DEFAULT for two ALLISTRUE
            // keys, which the body short-circuits before reading it).
            let siglen = if ka.is_signkey() && !ka.is_alltrue() {
                ka.sign().len() as i32
            } else {
                tsgist::SIGLEN_DEFAULT
            };
            Ok(tsgist::gtsvector_same(&ka, &kb, siglen))
        }
        F_GTSQUERY_SAME => {
            let ka = tsquerysign_from_key(a);
            let kb = tsquerysign_from_key(b);
            Ok(tsqgist::gtsquery_same(ka, kb))
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
    ssup: &mut SortSupportData<'mcx>,
) -> PgResult<()> {
    match proc_oid {
        F_GIST_POINT_SORTSUPPORT => {
            // gist_point_sortsupport(ssup) (gistproc.c). The `comparator` /
            // `abbrev_*` slots are `Copy` tokens only the sort substrate mints,
            // so each field write is delegated to a substrate-owned install seam
            // (mirroring nbtcompare's `install_sortsupport_*` precedent). The
            // z-order comparison / converter kernels themselves are pure and
            // local; the substrate supplies `ssup_datum_unsigned_cmp` and the
            // always-false `gist_bbox_zorder_abbrev_abort`.
            if ssup.abbreviate {
                // C:
                //   ssup->comparator           = ssup_datum_unsigned_cmp;
                //   ssup->abbrev_converter     = gist_bbox_zorder_abbrev_convert;
                //   ssup->abbrev_abort         = gist_bbox_zorder_abbrev_abort;
                //   ssup->abbrev_full_comparator = gist_bbox_zorder_cmp;
                gist_sortsupport_seams::install_gist_sortsupport_abbrev::call(
                    ssup,
                    gist_bbox_zorder_cmp_datum,
                    gist_bbox_zorder_abbrev_convert,
                );
            } else {
                // C: ssup->comparator = gist_bbox_zorder_cmp;
                gist_sortsupport_seams::install_gist_sortsupport_comparator::call(
                    ssup,
                    gist_bbox_zorder_cmp_datum,
                );
            }
            Ok(())
        }
        F_RANGE_GIST_SORTSUPPORT => {
            // range_sortsupport (rangetypes.c:1297): `ssup->comparator =
            // range_fast_cmp;` — no abbreviation. Install the range comparator
            // through the substrate seam (the GiST sorted build sorts leaf keys
            // with it).
            gist_sortsupport_seams::install_gist_sortsupport_comparator::call(
                ssup,
                range_gist::range_fast_cmp,
            );
            Ok(())
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

    // Register the fmgr builtin rows for this crate's GiST opclass support
    // procs (C: their `fmgr_builtins[]` rows). Without these, `fmgr_info`
    // (reached via `index_getprocinfo` in `initGISTstate`) cannot resolve the
    // internal-language prosrc names, and `CREATE INDEX ... USING gist` errors
    // `internal function "..." is not in internal lookup table`.
    fmgr_builtins::register_gist_proc_builtins();
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
