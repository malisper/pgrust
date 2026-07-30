//! gistproc.c: GiST support procs for 2-D objects. Live: box_ops
//! (consistent/union/penalty/picksplit/same), point_ops compress/fetch/
//! consistent (all strategy groups), poly_ops and circle_ops
//! compress/consistent, distance procs (point exact; box/poly/circle bbox
//! lower bounds, poly/circle lossy). LOUD: sortsupport (sorted-build lane).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod qsort;

use ::adt_float::float8_cmp_internal;
use ::adt_geo::{adjust_box, box_contain_box, box_ov, point_eq_point, rt_box_union, FPeq, FPge, FPgt, FPle, FPlt, PolyRef};
use ::datum::Datum;
use ::types_core::geo::{Point, BOX, CIRCLE};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{byref_result, datum_varlena_packed, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_gist::{GistEntryVector, GistSplitVec, GISTENTRY};

use qsort::pg_qsort;

const LIMIT_RATIO: f64 = 0.3;

const RTLeft: u16 = 1;
const RTOverLeft: u16 = 2;
const RTOverlap: u16 = 3;
const RTOverRight: u16 = 4;
const RTRight: u16 = 5;
const RTSame: u16 = 6;
const RTContains: u16 = 7;
const RTContainedBy: u16 = 8;
const RTOverBelow: u16 = 9;
const RTBelow: u16 = 10;
const RTAbove: u16 = 11;
const RTOverAbove: u16 = 12;
const RTOldBelow: u16 = 29;
const RTOldAbove: u16 = 30;

const GeoStrategyNumberOffset: u16 = 20;

// SAFETY helpers over the gist fmgr protocol: arg0/arg1 pointers are live for
// the call (GistState frames), keys point at page items or temp allocations.
unsafe fn entry_arg<'a>(fcinfo: &Fcinfo, i: usize) -> &'a GISTENTRY {
    &*(fcinfo.arg(i).as_usize() as *const GISTENTRY)
}

unsafe fn box_at(d: Datum) -> Option<BOX> {
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return None;
    }
    Some(BOX::from_datum_bytes(core::slice::from_raw_parts(p, 32)))
}

unsafe fn point_at(d: Datum) -> Option<Point> {
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return None;
    }
    Some(Point::from_datum_bytes(core::slice::from_raw_parts(p, 16)))
}

unsafe fn circle_at(d: Datum) -> Option<CIRCLE> {
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return None;
    }
    Some(CIRCLE::from_datum_bytes(core::slice::from_raw_parts(p, 24)))
}

// Polygon datums are varlena: external/compressed images detoast into the
// armed result mcx (C's DatumGetPolygonP).
unsafe fn poly_at<'a>(d: Datum, fcinfo: &'a Fcinfo) -> PgResult<Option<PolyRef<'a>>> {
    if d.as_usize() == 0 {
        return Ok(None);
    }
    let v = unsafe { datum_varlena_packed(d, fcinfo.result_mcx()) }?;
    Ok(Some(PolyRef::from_payload(v.data())))
}

// C evaluation (and error) order: high.x, low.x, high.y, low.y.
fn circle_bbox(c: &CIRCLE) -> PgResult<BOX> {
    let high_x = ::adt_float::float8_pl(c.center.x, c.radius)?;
    let low_x = ::adt_float::float8_mi(c.center.x, c.radius)?;
    let high_y = ::adt_float::float8_pl(c.center.y, c.radius)?;
    let low_y = ::adt_float::float8_mi(c.center.y, c.radius)?;
    Ok(BOX {
        high: Point { x: high_x, y: high_y },
        low: Point { x: low_x, y: low_y },
    })
}

fn box_result(fcinfo: &Fcinfo, b: &BOX) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &b.to_datum_bytes())
}

fn point_result(fcinfo: &Fcinfo, p: &Point) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &p.to_datum_bytes())
}

fn entry_result(fcinfo: &Fcinfo, e: &GISTENTRY) -> PgResult<Datum> {
    // GISTENTRY is Copy/no-drop; byref image copy at palloc alignment.
    let bytes = unsafe {
        core::slice::from_raw_parts(
            (e as *const GISTENTRY).cast::<u8>(),
            core::mem::size_of::<GISTENTRY>(),
        )
    };
    byref_result(fcinfo.result_mcx(), bytes)
}

#[track_caller]
#[cold]
fn unrecognized_strategy(strategy: u16) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "unrecognized strategy number: {strategy}"
    )))
}

fn rt_box_size(b: &BOX) -> PgResult<f64> {
    // size_box: zero-width special case first (0×inf is 0, not NaN).
    if float8_le_raw(b.high.x, b.low.x) || float8_le_raw(b.high.y, b.low.y) {
        return Ok(0.0);
    }
    if b.high.x.is_nan() || b.high.y.is_nan() {
        return Ok(f64::INFINITY);
    }
    ::adt_float::float8_mul(
        ::adt_float::float8_mi(b.high.x, b.low.x)?,
        ::adt_float::float8_mi(b.high.y, b.low.y)?,
    )
}

// float8_le (float.h): NaN-aware totally-ordered compare (NaN greatest).
fn float8_le_raw(a: f64, b: f64) -> bool {
    float8_cmp_internal(a, b) <= 0
}

fn box_penalty(original: &BOX, new: &BOX) -> PgResult<f64> {
    let unionbox = rt_box_union(original, new);
    ::adt_float::float8_mi(rt_box_size(&unionbox)?, rt_box_size(original)?)
}

// gist_box_leaf_consistent.
fn gist_box_leaf_consistent(key: &BOX, query: &BOX, strategy: u16) -> PgResult<bool> {
    Ok(match strategy {
        RTLeft => FPlt(key.high.x, query.low.x),
        RTOverLeft => FPle(key.high.x, query.high.x),
        RTOverlap => box_ov(key, query),
        RTOverRight => FPge(key.low.x, query.low.x),
        RTRight => FPgt(key.low.x, query.high.x),
        RTSame => {
            point_eq_point(&key.high, &query.high) && point_eq_point(&key.low, &query.low)
        }
        RTContains => box_contain_box(key, query),
        RTContainedBy => box_contain_box(query, key),
        RTOverBelow => FPle(key.high.y, query.high.y),
        RTBelow => FPlt(key.high.y, query.low.y),
        RTAbove => FPgt(key.low.y, query.high.y),
        RTOverAbove => FPge(key.low.y, query.low.y),
        other => return Err(unrecognized_strategy(other)),
    })
}

// rtree_internal_consistent.
fn rtree_internal_consistent(key: &BOX, query: &BOX, strategy: u16) -> PgResult<bool> {
    Ok(match strategy {
        RTLeft => !FPge(key.low.x, query.low.x),   // !box_overright
        RTOverLeft => !FPgt(key.low.x, query.high.x), // !box_right
        RTOverlap => box_ov(key, query),
        RTOverRight => !FPlt(key.high.x, query.low.x), // !box_left
        RTRight => !FPle(key.high.x, query.high.x),    // !box_overleft
        RTSame | RTContains => box_contain_box(key, query),
        RTContainedBy => box_ov(key, query),
        RTOverBelow => !FPgt(key.low.y, query.high.y), // !box_above
        RTBelow => !FPge(key.low.y, query.low.y),      // !box_overabove
        RTAbove => !FPle(key.high.y, query.high.y),    // !box_overbelow
        RTOverAbove => !FPlt(key.high.y, query.low.y), // !box_below
        other => return Err(unrecognized_strategy(other)),
    })
}

fn fc_gist_box_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol (module contract).
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let query = unsafe { box_at(fcinfo.arg(1)) };
    let strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param is a live &mut bool in the caller frame.
    unsafe { *recheck = false };

    let key = unsafe { box_at(entry.key) };
    let (Some(key), Some(query)) = (key, query) else {
        return Ok(Datum::from_bool(false));
    };
    let r = if entry.page_is_leaf {
        gist_box_leaf_consistent(&key, &query, strategy)?
    } else {
        rtree_internal_consistent(&key, &query, strategy)?
    };
    Ok(Datum::from_bool(r))
}

fn fc_gist_box_union(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let sizep = fcinfo.arg(1).as_usize() as *mut i32;

    let numranges = entryvec.n as usize;
    // SAFETY: non-null keys per union contract.
    let mut pageunion = unsafe { box_at(entryvec.vector[0].key) }.expect("union key");
    for i in 1..numranges {
        // SAFETY: as above.
        let cur = unsafe { box_at(entryvec.vector[i].key) }.expect("union key");
        adjust_box(&mut pageunion, &cur);
    }
    // SAFETY: sizep out-param is a live &mut i32 in the caller frame.
    unsafe { *sizep = 32 };
    box_result(fcinfo, &pageunion)
}

fn fc_gist_box_penalty(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let origentry = unsafe { entry_arg(fcinfo, 0) };
    let newentry = unsafe { entry_arg(fcinfo, 1) };
    let result = fcinfo.arg(2).as_usize() as *mut f32;
    let origbox = unsafe { box_at(origentry.key) }.expect("penalty orig key");
    let newbox = unsafe { box_at(newentry.key) }.expect("penalty new key");
    // SAFETY: result out-param is a live &mut f32 in the caller frame.
    unsafe { *result = box_penalty(&origbox, &newbox)? as f32 };
    Ok(fcinfo.arg(2))
}

fn fc_gist_box_same(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol; args are key datums.
    let b1 = unsafe { box_at(fcinfo.arg(0)) };
    let b2 = unsafe { box_at(fcinfo.arg(1)) };
    let result = fcinfo.arg(2).as_usize() as *mut bool;
    // C (gistproc.c gist_box_same) compares with float8_eq, which is
    // NaN-aware (NaN == NaN is TRUE there); raw `==` is never true for
    // NaN, so a NaN-cornered box compared with itself reported "not same"
    // and GiST would keep re-splitting instead of recognizing the key.
    // Same NaN-comparator class as adjust_box (proofs/gist-geo).
    let r = match (b1, b2) {
        (Some(b1), (Some(b2))) => {
            ::adt_float::float8_eq(b1.low.x, b2.low.x)
                && ::adt_float::float8_eq(b1.low.y, b2.low.y)
                && ::adt_float::float8_eq(b1.high.x, b2.high.x)
                && ::adt_float::float8_eq(b1.high.y, b2.high.y)
        }
        (None, None) => true,
        _ => false,
    };
    // SAFETY: result out-param is a live &mut bool in the caller frame.
    unsafe { *result = r };
    Ok(fcinfo.arg(2))
}

// fallbackSplit.
fn fallback_split(entryvec: &GistEntryVector, v: &mut GistSplitVec, fcinfo: &Fcinfo) -> PgResult<()> {
    let maxoff = (entryvec.n - 1) as usize;
    v.spl_left = Vec::with_capacity(maxoff + 2);
    v.spl_right = Vec::with_capacity(maxoff + 2);
    let mut union_l: Option<BOX> = None;
    let mut union_r: Option<BOX> = None;

    for i in 1..=maxoff {
        // SAFETY: gist fmgr protocol.
        let cur = unsafe { box_at(entryvec.vector[i].key) }.expect("picksplit key");
        if i <= maxoff / 2 {
            v.spl_left.push(i as u16);
            match union_l.as_mut() {
                None => union_l = Some(cur),
                Some(u) => adjust_box(u, &cur),
            }
        } else {
            v.spl_right.push(i as u16);
            match union_r.as_mut() {
                None => union_r = Some(cur),
                Some(u) => adjust_box(u, &cur),
            }
        }
    }
    v.spl_ldatum = box_result(fcinfo, &union_l.expect("left union"))?;
    v.spl_rdatum = box_result(fcinfo, &union_r.expect("right union"))?;
    Ok(())
}

#[derive(Clone, Copy)]
struct CommonEntry {
    index: usize,
    delta: f64,
}

#[derive(Clone, Copy, Default)]
struct SplitInterval {
    lower: f64,
    upper: f64,
}

struct ConsiderSplitContext {
    entries_count: i32,
    bounding_box: BOX,
    first: bool,
    left_upper: f64,
    right_lower: f64,
    ratio: f32,
    overlap: f32,
    dim: i32,
    range: f64,
}

fn non_negative(val: f32) -> f32 {
    if val >= 0.0 {
        val
    } else {
        0.0
    }
}

// g_box_consider_split.
fn g_box_consider_split(
    context: &mut ConsiderSplitContext,
    dim_num: i32,
    right_lower: f64,
    min_left_count: i32,
    left_upper: f64,
    max_left_count: i32,
) -> PgResult<()> {
    let left_count = if min_left_count >= (context.entries_count + 1) / 2 {
        min_left_count
    } else if max_left_count <= context.entries_count / 2 {
        max_left_count
    } else {
        context.entries_count / 2
    };
    let right_count = context.entries_count - left_count;

    let ratio = ::adt_float::float4_div(
        left_count.min(right_count) as f32,
        context.entries_count as f32,
    )?;

    // C compares float4 ratio against the double literal 0.3.
    if ratio as f64 > LIMIT_RATIO {
        let mut selectthis = false;
        let range = if dim_num == 0 {
            ::adt_float::float8_mi(context.bounding_box.high.x, context.bounding_box.low.x)?
        } else {
            ::adt_float::float8_mi(context.bounding_box.high.y, context.bounding_box.low.y)?
        };
        let overlap =
            (::adt_float::float8_div(::adt_float::float8_mi(left_upper, right_lower)?, range)?)
                as f32;

        if context.first {
            selectthis = true;
        } else if context.dim == dim_num {
            if overlap < context.overlap
                || (overlap == context.overlap && ratio > context.ratio)
            {
                selectthis = true;
            }
        } else if non_negative(overlap) < non_negative(context.overlap)
            || (range > context.range && non_negative(overlap) <= non_negative(context.overlap))
        {
            selectthis = true;
        }

        if selectthis {
            context.first = false;
            context.ratio = ratio;
            context.range = range;
            context.overlap = overlap;
            context.right_lower = right_lower;
            context.left_upper = left_upper;
            context.dim = dim_num;
        }
    }
    Ok(())
}

// gist_box_picksplit: the double-sorting split algorithm.
fn fc_gist_box_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entryvec = unsafe { &*(fcinfo.arg(0).as_usize() as *const GistEntryVector) };
    let v = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut GistSplitVec) };

    let maxoff = (entryvec.n - 1) as usize;
    let nentries = maxoff; // maxoff - FirstOffsetNumber + 1

    let key_at = |i: usize| -> BOX {
        // SAFETY: gist fmgr protocol; all keys non-null in picksplit.
        unsafe { box_at(entryvec.vector[i].key) }.expect("picksplit key")
    };

    let mut context = ConsiderSplitContext {
        entries_count: nentries as i32,
        bounding_box: BOX::default(),
        first: true,
        left_upper: 0.0,
        right_lower: 0.0,
        ratio: 0.0,
        overlap: 0.0,
        dim: 0,
        range: 0.0,
    };

    for i in 1..=maxoff {
        let b = key_at(i);
        if i == 1 {
            context.bounding_box = b;
        } else {
            adjust_box(&mut context.bounding_box, &b);
        }
    }

    let mut intervals_lower = vec![SplitInterval::default(); nentries];
    let mut intervals_upper = vec![SplitInterval::default(); nentries];

    for dim in 0..2i32 {
        for i in 1..=maxoff {
            let b = key_at(i);
            intervals_lower[i - 1] = if dim == 0 {
                SplitInterval { lower: b.low.x, upper: b.high.x }
            } else {
                SplitInterval { lower: b.low.y, upper: b.high.y }
            };
        }
        intervals_upper.copy_from_slice(&intervals_lower);
        pg_qsort(&mut intervals_lower, |a, b| {
            float8_cmp_internal(a.lower, b.lower)
        });
        pg_qsort(&mut intervals_upper, |a, b| {
            float8_cmp_internal(a.upper, b.upper)
        });

        // lower bound of right group sweep
        let mut i1 = 0usize;
        let mut i2 = 0usize;
        let mut right_lower = intervals_lower[i1].lower;
        let mut left_upper = intervals_upper[i2].lower;
        loop {
            while i1 < nentries && right_lower == intervals_lower[i1].lower {
                if left_upper < intervals_lower[i1].upper {
                    left_upper = intervals_lower[i1].upper;
                }
                i1 += 1;
            }
            if i1 >= nentries {
                break;
            }
            right_lower = intervals_lower[i1].lower;

            while i2 < nentries && intervals_upper[i2].upper <= left_upper {
                i2 += 1;
            }

            g_box_consider_split(&mut context, dim, right_lower, i1 as i32, left_upper, i2 as i32)?;
        }

        // upper bound of left group sweep
        let mut i1 = nentries as isize - 1;
        let mut i2 = nentries as isize - 1;
        let mut right_lower = intervals_lower[i1 as usize].upper;
        let mut left_upper = intervals_upper[i2 as usize].upper;
        loop {
            while i2 >= 0 && left_upper == intervals_upper[i2 as usize].upper {
                if right_lower > intervals_upper[i2 as usize].lower {
                    right_lower = intervals_upper[i2 as usize].lower;
                }
                i2 -= 1;
            }
            if i2 < 0 {
                break;
            }
            left_upper = intervals_upper[i2 as usize].upper;

            while i1 >= 0 && intervals_lower[i1 as usize].lower >= right_lower {
                i1 -= 1;
            }

            g_box_consider_split(
                &mut context,
                dim,
                right_lower,
                (i1 + 1) as i32,
                left_upper,
                (i2 + 1) as i32,
            )?;
        }
    }

    if context.first {
        fallback_split(entryvec, v, fcinfo)?;
        return Ok(fcinfo.arg(1));
    }

    v.spl_left = Vec::with_capacity(nentries);
    v.spl_right = Vec::with_capacity(nentries);

    let mut left_box = BOX::default();
    let mut right_box = BOX::default();
    let mut common_entries: Vec<CommonEntry> = Vec::with_capacity(nentries);

    macro_rules! place_left {
        ($b:expr, $off:expr) => {{
            if !v.spl_left.is_empty() {
                adjust_box(&mut left_box, &$b);
            } else {
                left_box = $b;
            }
            v.spl_left.push($off as u16);
        }};
    }
    macro_rules! place_right {
        ($b:expr, $off:expr) => {{
            if !v.spl_right.is_empty() {
                adjust_box(&mut right_box, &$b);
            } else {
                right_box = $b;
            }
            v.spl_right.push($off as u16);
        }};
    }

    for i in 1..=maxoff {
        let b = key_at(i);
        let (lower, upper) = if context.dim == 0 {
            (b.low.x, b.high.x)
        } else {
            (b.low.y, b.high.y)
        };

        if upper <= context.left_upper {
            if lower >= context.right_lower {
                common_entries.push(CommonEntry { index: i, delta: 0.0 });
            } else {
                place_left!(b, i);
            }
        } else {
            debug_assert!(lower >= context.right_lower);
            place_right!(b, i);
        }
    }

    if !common_entries.is_empty() {
        let m = (LIMIT_RATIO * nentries as f64).ceil() as usize;

        for ce in common_entries.iter_mut() {
            let b = key_at(ce.index);
            ce.delta = (box_penalty(&left_box, &b)? - box_penalty(&right_box, &b)?).abs();
        }
        pg_qsort(&mut common_entries, |a, b| {
            float8_cmp_internal(a.delta, b.delta)
        });

        let n_common = common_entries.len();
        for i in 0..n_common {
            let idx = common_entries[i].index;
            let b = key_at(idx);
            if v.spl_left.len() + (n_common - i) <= m {
                place_left!(b, idx);
            } else if v.spl_right.len() + (n_common - i) <= m {
                place_right!(b, idx);
            } else if box_penalty(&left_box, &b)? < box_penalty(&right_box, &b)? {
                place_left!(b, idx);
            } else {
                place_right!(b, idx);
            }
        }
    }

    v.spl_ldatum = box_result(fcinfo, &left_box)?;
    v.spl_rdatum = box_result(fcinfo, &right_box)?;
    Ok(fcinfo.arg(1))
}

// gist_point_compress.
fn fc_gist_point_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if entry.leafkey {
        // SAFETY: leaf key is a point datum.
        let point = unsafe { point_at(entry.key) }.expect("point key");
        let b = BOX {
            high: point,
            low: point,
        };
        let key = box_result(fcinfo, &b)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    Ok(fcinfo.arg(0))
}

// gist_point_fetch.
fn fc_gist_point_fetch(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let b = unsafe { box_at(entry.key) }.expect("fetch key");
    let r = Point {
        x: b.high.x,
        y: b.high.y,
    };
    let key = point_result(fcinfo, &r)?;
    let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
    entry_result(fcinfo, &retval)
}

// gist_point_consistent_internal.
fn gist_point_consistent_internal(
    strategy: u16,
    is_leaf: bool,
    key: &BOX,
    query: &Point,
) -> PgResult<bool> {
    Ok(match strategy {
        RTLeft => FPlt(key.low.x, query.x),
        RTRight => FPgt(key.high.x, query.x),
        RTAbove => FPgt(key.high.y, query.y),
        RTBelow => FPlt(key.low.y, query.y),
        RTSame => {
            if is_leaf {
                FPeq(key.low.x, query.x) && FPeq(key.low.y, query.y)
            } else {
                FPle(query.x, key.high.x)
                    && FPge(query.x, key.low.x)
                    && FPle(query.y, key.high.y)
                    && FPge(query.y, key.low.y)
            }
        }
        other => return Err(unrecognized_strategy(other)),
    })
}

fn fc_gist_point_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let mut strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;

    if strategy == RTOldBelow {
        strategy = RTBelow;
    } else if strategy == RTOldAbove {
        strategy = RTAbove;
    }

    let strategy_group = strategy / GeoStrategyNumberOffset;
    let result = match strategy_group {
        0 => {
            // SAFETY: point-group query arg is a point datum.
            let query = unsafe { point_at(fcinfo.arg(1)) }.expect("point query");
            let key = unsafe { box_at(entry.key) }.expect("point key box");
            let r = gist_point_consistent_internal(
                strategy % GeoStrategyNumberOffset,
                entry.page_is_leaf,
                &key,
                &query,
            )?;
            // SAFETY: recheck out-param live in the caller frame.
            unsafe { *recheck = false };
            r
        }
        1 => {
            // point <@ box (on_pb): non-fuzzy overlap test
            // SAFETY: box-group query arg is a box datum.
            let query = unsafe { box_at(fcinfo.arg(1)) }.expect("box query");
            let key = unsafe { box_at(entry.key) }.expect("point key box");
            // SAFETY: recheck out-param live in the caller frame.
            unsafe { *recheck = false };
            key.high.x >= query.low.x
                && key.low.x <= query.high.x
                && key.high.y >= query.low.y
                && key.low.y <= query.high.y
        }
        2 => {
            // point op polygon rides gist_poly_consistent at RTOverlap; on a
            // leaf hit the exact poly_contain_pt test replaces the recheck
            // (leaf keys are degenerate boxes: high == low == the point).
            // SAFETY: recheck out-param live in the caller frame.
            unsafe { *recheck = true };
            let key = unsafe { box_at(entry.key) };
            // SAFETY: query arg is a polygon varlena datum.
            let query = unsafe { poly_at(fcinfo.arg(1), fcinfo) }?;
            match (key, query) {
                (Some(key), Some(query)) => {
                    let mut r = rtree_internal_consistent(&key, &query.boundbox, RTOverlap)?;
                    if entry.page_is_leaf && r {
                        debug_assert!(key.high.x == key.low.x && key.high.y == key.low.y);
                        r = ::adt_geo::poly::poly_contain_pt(&query, &key.high)?;
                        // SAFETY: as above.
                        unsafe { *recheck = false };
                    }
                    r
                }
                _ => false,
            }
        }
        3 => {
            // point op circle rides gist_circle_consistent at RTOverlap; on a
            // leaf hit the exact circle_contain_pt test replaces the recheck.
            // SAFETY: recheck out-param live in the caller frame.
            unsafe { *recheck = true };
            let key = unsafe { box_at(entry.key) };
            // SAFETY: query arg is a circle datum (fixed 24 bytes).
            let query = unsafe { circle_at(fcinfo.arg(1)) };
            match (key, query) {
                (Some(key), Some(query)) => {
                    let bbox = circle_bbox(&query)?;
                    let mut r = rtree_internal_consistent(&key, &bbox, RTOverlap)?;
                    if entry.page_is_leaf && r {
                        debug_assert!(key.high.x == key.low.x && key.high.y == key.low.y);
                        r = ::adt_geo::circle::circle_contain_pt(&query, &key.high)?;
                        // SAFETY: as above.
                        unsafe { *recheck = false };
                    }
                    r
                }
                _ => false,
            }
        }
        _ => return Err(unrecognized_strategy(strategy)),
    };
    Ok(Datum::from_bool(result))
}

// computeDistance (gistproc.c:1221): point-to-box distance; on a leaf the box
// is degenerate (low == high == the point).
fn compute_distance(is_leaf: bool, b: &BOX, point: &Point) -> PgResult<f64> {
    if is_leaf {
        return ::adt_geo::point_dt(point, &b.low);
    }
    if point.x <= b.high.x && point.x >= b.low.x && point.y <= b.high.y && point.y >= b.low.y {
        return Ok(0.0);
    }
    if point.x <= b.high.x && point.x >= b.low.x {
        if point.y > b.high.y {
            return ::adt_float::float8_mi(point.y, b.high.y);
        }
        if point.y < b.low.y {
            return ::adt_float::float8_mi(b.low.y, point.y);
        }
        return Err(Box::new(PgError::error("inconsistent point values".to_string())));
    }
    if point.y <= b.high.y && point.y >= b.low.y {
        if point.x > b.high.x {
            return ::adt_float::float8_mi(point.x, b.high.x);
        }
        if point.x < b.low.x {
            return ::adt_float::float8_mi(b.low.x, point.x);
        }
        return Err(Box::new(PgError::error("inconsistent point values".to_string())));
    }
    // closest point is a vertex
    let mut result = ::adt_geo::point_dt(point, &b.low)?;
    for p in [
        b.high,
        Point { x: b.low.x, y: b.high.y },
        Point { x: b.high.x, y: b.low.y },
    ] {
        let sub = ::adt_geo::point_dt(point, &p)?;
        if result > sub {
            result = sub;
        }
    }
    Ok(result)
}

// gist_bbox_distance (gistproc.c:1479).
fn gist_bbox_distance(entry: &GISTENTRY, query: Datum, strategy: u16) -> PgResult<f64> {
    match strategy / GeoStrategyNumberOffset {
        0 => {
            // SAFETY: point-group query arg is a point datum.
            let query = unsafe { point_at(query) }.expect("point query");
            let key = unsafe { box_at(entry.key) }.expect("bbox key");
            compute_distance(false, &key, &query)
        }
        _ => Err(unrecognized_strategy(strategy)),
    }
}

fn fc_gist_box_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u16();
    let distance = gist_bbox_distance(entry, fcinfo.arg(1), strategy)?;
    Ok(Datum::from_f64(distance))
}

fn fc_gist_point_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u16();
    let distance = match strategy / GeoStrategyNumberOffset {
        0 => {
            // SAFETY: point-group query arg is a point datum.
            let query = unsafe { point_at(fcinfo.arg(1)) }.expect("point query");
            let key = unsafe { box_at(entry.key) }.expect("point key box");
            compute_distance(entry.page_is_leaf, &key, &query)?
        }
        _ => return Err(unrecognized_strategy(strategy)),
    };
    Ok(Datum::from_f64(distance))
}

// gistproc.c:1745-1761: C fills the SortSupport fn pointers; here the sorted
// build resolves proc 3435 to SortComparator::GistPointZorder (tuplesort
// ssup over types_core::geo::gist_bbox_zorder_cmp), so the fmgr entry —
// reachable only through a raw-SortSupport datum this port never forms —
// stays a guard.
fn fc_gist_point_sortsupport(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    panic!(
        "gist_point_sortsupport: sorted builds ride SortComparator::GistPointZorder, \
         never through fmgr"
    )
}

// gist_poly_compress: represent a polygon by its bounding box.
fn fc_gist_poly_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if entry.leafkey {
        // SAFETY: leaf key is a polygon varlena datum.
        let poly = unsafe { poly_at(entry.key, fcinfo) }?.expect("poly key");
        let key = box_result(fcinfo, &poly.boundbox)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    Ok(fcinfo.arg(0))
}

// gist_poly_consistent: index entries are bounding boxes, so leaf and
// internal pages both use rtree_internal_consistent; always inexact.
fn fc_gist_poly_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param is a live &mut bool in the caller frame.
    unsafe { *recheck = true };

    let key = unsafe { box_at(entry.key) };
    // SAFETY: query arg is a polygon varlena datum.
    let query = unsafe { poly_at(fcinfo.arg(1), fcinfo) }?;
    let (Some(key), Some(query)) = (key, query) else {
        return Ok(Datum::from_bool(false));
    };
    let r = rtree_internal_consistent(&key, &query.boundbox, strategy)?;
    Ok(Datum::from_bool(r))
}

// gist_poly_distance: keys are bounding boxes, so the distance is a lower
// bound — always lossy (*recheck = true).
fn fc_gist_poly_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    let distance = gist_bbox_distance(entry, fcinfo.arg(1), strategy)?;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = true };
    Ok(Datum::from_f64(distance))
}

// gist_circle_compress: represent a circle by its bounding box.
fn fc_gist_circle_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    if entry.leafkey {
        // SAFETY: leaf key is a circle datum (fixed 24 bytes).
        let circle = unsafe { circle_at(entry.key) }.expect("circle key");
        let b = circle_bbox(&circle)?;
        let key = box_result(fcinfo, &b)?;
        let retval = GISTENTRY::init(key, entry.offset, false, entry.page_is_leaf);
        return entry_result(fcinfo, &retval);
    }
    Ok(fcinfo.arg(0))
}

// gist_circle_consistent: index entries are bounding boxes, so leaf and
// internal pages both use rtree_internal_consistent; always inexact.
fn fc_gist_circle_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    // SAFETY: recheck out-param is a live &mut bool in the caller frame.
    unsafe { *recheck = true };

    let key = unsafe { box_at(entry.key) };
    // SAFETY: query arg is a circle datum (fixed 24 bytes).
    let query = unsafe { circle_at(fcinfo.arg(1)) };
    let (Some(key), Some(query)) = (key, query) else {
        return Ok(Datum::from_bool(false));
    };
    let bbox = circle_bbox(&query)?;
    let r = rtree_internal_consistent(&key, &bbox, strategy)?;
    Ok(Datum::from_bool(r))
}

// gist_circle_distance: as gist_poly_distance — bbox lower bound, lossy.
fn fc_gist_circle_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: gist fmgr protocol.
    let entry = unsafe { entry_arg(fcinfo, 0) };
    let strategy = fcinfo.arg(2).as_u16();
    let recheck = fcinfo.arg(4).as_usize() as *mut bool;
    let distance = gist_bbox_distance(entry, fcinfo.arg(1), strategy)?;
    // SAFETY: recheck out-param live in the caller frame.
    unsafe { *recheck = true };
    Ok(Datum::from_f64(distance))
}

fn fc_gist_translate_cmptype_common(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let cmptype = fcinfo.arg(0).as_i32();
    Ok(Datum::from_u16(::gist::gist_translate_cmptype_common(
        cmptype,
    )))
}

fn fc_gisthandler(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    panic!("gisthandler: the closed AM set dispatches via IndexAmKind, never through fmgr")
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

pub const GISTPROC_BUILTINS: &[FmgrBuiltin] = &[
    b(332, "gisthandler", 1, fc_gisthandler),
    b(1030, "gist_point_compress", 1, fc_gist_point_compress),
    b(2179, "gist_point_consistent", 5, fc_gist_point_consistent),
    b(2578, "gist_box_consistent", 5, fc_gist_box_consistent),
    b(2581, "gist_box_penalty", 3, fc_gist_box_penalty),
    b(2582, "gist_box_picksplit", 2, fc_gist_box_picksplit),
    b(2583, "gist_box_union", 2, fc_gist_box_union),
    b(2584, "gist_box_same", 3, fc_gist_box_same),
    b(2585, "gist_poly_consistent", 5, fc_gist_poly_consistent),
    b(2586, "gist_poly_compress", 1, fc_gist_poly_compress),
    b(2591, "gist_circle_consistent", 5, fc_gist_circle_consistent),
    b(2592, "gist_circle_compress", 1, fc_gist_circle_compress),
    b(3064, "gist_point_distance", 5, fc_gist_point_distance),
    b(3280, "gist_circle_distance", 5, fc_gist_circle_distance),
    b(3282, "gist_point_fetch", 1, fc_gist_point_fetch),
    b(3288, "gist_poly_distance", 5, fc_gist_poly_distance),
    b(3435, "gist_point_sortsupport", 1, fc_gist_point_sortsupport),
    b(3998, "gist_box_distance", 5, fc_gist_box_distance),
    b(6347, "gist_translate_cmptype_common", 1, fc_gist_translate_cmptype_common),
];

#[cfg(test)]
mod nan_same_regression {
    /// C (gistproc.c gist_box_same) compares via float8_eq, under which
    /// NaN == NaN is TRUE; raw `==` made a NaN-cornered box differ from
    /// itself.
    #[test]
    fn float8_eq_treats_nan_as_equal() {
        let nan = f64::NAN;
        assert!(::adt_float::float8_eq(nan, nan), "C semantics: NaN == NaN");
        assert!(!::adt_float::float8_eq(nan, 1.0));
        assert!(!(nan == nan), "raw == is the behavior we removed");
    }
}
