//! geo_spgist.c: 4-dimensional quad tree over boxes — the box_ops opclass,
//! plus the bbox config/compress procs that index polygons by bounding box.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::adt_geo::boxes::{
    box_above, box_below, box_contain, box_contained, box_left, box_overabove, box_overbelow,
    box_overlap, box_overleft, box_overright, box_right, box_same,
};
use ::adt_geo::{pg_hypot, FPge, FPgt, FPle, FPlt};
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::geo::{Point, BOX};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_scan::scankey::{
    RTAboveStrategyNumber, RTBelowStrategyNumber, RTContainedByStrategyNumber,
    RTContainsStrategyNumber, RTLeftStrategyNumber, RTOverAboveStrategyNumber,
    RTOverBelowStrategyNumber, RTOverLeftStrategyNumber, RTOverRightStrategyNumber,
    RTOverlapStrategyNumber, RTRightStrategyNumber, RTSameStrategyNumber, ScanKeyData,
};
use ::types_spgist::spgConfigOut;
use ::types_spgist::state::{
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};

const BOXOID: Oid = 603;
const POLYGONOID: Oid = 604;
const VOIDOID: Oid = 2278;
// fmgroids.h F_DIST_POLYP (dist_polyp).
const F_DIST_POLYP: Oid = 3292;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct Range {
    pub low: f64,
    pub high: f64,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct RangeBox {
    pub left: Range,
    pub right: Range,
}

// The SP-GiST traversal value; passed between inner_consistent calls as a raw
// pointer into traversalMemoryContext, C-shaped.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RectBox {
    pub range_box_x: RangeBox,
    pub range_box_y: RangeBox,
}

// SAFETY: datum points at a live 32-byte box image (opclass protocol).
#[inline]
unsafe fn box_at(d: Datum) -> BOX {
    BOX::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 32))
}

// SAFETY: datum points at a live 16-byte point image (orderby argument).
#[inline]
unsafe fn point_at(d: Datum) -> Point {
    Point::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 16))
}

// geo_spgist.c pointToRectBoxDistance (no NaN leg, as in C).
fn pointToRectBoxDistance(point: &Point, rect_box: &RectBox) -> PgResult<f64> {
    let dx = if point.x < rect_box.range_box_x.left.low {
        rect_box.range_box_x.left.low - point.x
    } else if point.x > rect_box.range_box_x.right.high {
        point.x - rect_box.range_box_x.right.high
    } else {
        0.0
    };
    let dy = if point.y < rect_box.range_box_y.left.low {
        rect_box.range_box_y.left.low - point.y
    } else if point.y > rect_box.range_box_y.right.high {
        point.y - rect_box.range_box_y.right.high
    } else {
        0.0
    };
    pg_hypot(dx, dy)
}

// One per-node distances row for an ordered scan, in the armed mcx.
fn inner_distances_row(
    mcx: Mcx<'_>,
    input: &spgInnerConsistentIn,
    rect_box: &RectBox,
) -> PgResult<*const f64> {
    // SAFETY: norderbys orderby scankeys per protocol.
    let orderbys = unsafe {
        core::slice::from_raw_parts(input.orderbys, input.norderbys.max(0) as usize)
    };
    let mut row: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, orderbys.len())?;
    for sk in orderbys {
        // SAFETY: point-typed orderby argument.
        let pt = unsafe { point_at(sk.sk_argument) };
        row.push(pointToRectBoxDistance(&pt, rect_box)?);
    }
    let p = row.as_ptr();
    core::mem::forget(row);
    Ok(p)
}

// BoxPGetDatum of a fresh box: 8-aligned like C's palloc'd BOX.
fn form_box_datum(mcx: Mcx<'_>, b: &BOX) -> PgResult<Datum> {
    let mut buf: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, 4)?;
    buf.push(b.high.x);
    buf.push(b.high.y);
    buf.push(b.low.x);
    buf.push(b.low.y);
    let ptr = buf.as_ptr() as usize;
    core::mem::forget(buf);
    Ok(Datum::from_usize(ptr))
}

pub fn getQuadrant(centroid: &BOX, inBox: &BOX) -> u8 {
    let mut quadrant: u8 = 0;
    if inBox.low.x > centroid.low.x {
        quadrant |= 0x8;
    }
    if inBox.high.x > centroid.high.x {
        quadrant |= 0x4;
    }
    if inBox.low.y > centroid.low.y {
        quadrant |= 0x2;
    }
    if inBox.high.y > centroid.high.y {
        quadrant |= 0x1;
    }
    quadrant
}

pub fn getRangeBox(b: &BOX) -> RangeBox {
    RangeBox {
        left: Range { low: b.low.x, high: b.high.x },
        right: Range { low: b.low.y, high: b.high.y },
    }
}

pub fn initRectBox() -> RectBox {
    let inf = f64::INFINITY;
    let full = RangeBox {
        left: Range { low: -inf, high: inf },
        right: Range { low: -inf, high: inf },
    };
    RectBox { range_box_x: full, range_box_y: full }
}

pub fn nextRectBox(rect_box: &RectBox, centroid: &RangeBox, quadrant: u8) -> RectBox {
    let mut next = *rect_box;

    if quadrant & 0x8 != 0 {
        next.range_box_x.left.low = centroid.left.low;
    } else {
        next.range_box_x.left.high = centroid.left.low;
    }
    if quadrant & 0x4 != 0 {
        next.range_box_x.right.low = centroid.left.high;
    } else {
        next.range_box_x.right.high = centroid.left.high;
    }
    if quadrant & 0x2 != 0 {
        next.range_box_y.left.low = centroid.right.low;
    } else {
        next.range_box_y.left.high = centroid.right.low;
    }
    if quadrant & 0x1 != 0 {
        next.range_box_y.right.low = centroid.right.high;
    } else {
        next.range_box_y.right.high = centroid.right.high;
    }
    next
}

pub fn overlap2D(range_box: &RangeBox, query: &Range) -> bool {
    FPge(range_box.right.high, query.low) && FPle(range_box.left.low, query.high)
}

pub fn overlap4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overlap2D(&rect_box.range_box_x, &query.left) && overlap2D(&rect_box.range_box_y, &query.right)
}

pub fn contain2D(range_box: &RangeBox, query: &Range) -> bool {
    FPge(range_box.right.high, query.high) && FPle(range_box.left.low, query.low)
}

pub fn contain4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    contain2D(&rect_box.range_box_x, &query.left) && contain2D(&rect_box.range_box_y, &query.right)
}

pub fn contained2D(range_box: &RangeBox, query: &Range) -> bool {
    FPle(range_box.left.low, query.high)
        && FPge(range_box.left.high, query.low)
        && FPle(range_box.right.low, query.high)
        && FPge(range_box.right.high, query.low)
}

pub fn contained4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    contained2D(&rect_box.range_box_x, &query.left)
        && contained2D(&rect_box.range_box_y, &query.right)
}

pub fn lower2D(range_box: &RangeBox, query: &Range) -> bool {
    FPlt(range_box.left.low, query.low) && FPlt(range_box.right.low, query.low)
}

pub fn overLower2D(range_box: &RangeBox, query: &Range) -> bool {
    FPle(range_box.left.low, query.high) && FPle(range_box.right.low, query.high)
}

pub fn higher2D(range_box: &RangeBox, query: &Range) -> bool {
    FPgt(range_box.left.high, query.high) && FPgt(range_box.right.high, query.high)
}

pub fn overHigher2D(range_box: &RangeBox, query: &Range) -> bool {
    FPge(range_box.left.high, query.low) && FPge(range_box.right.high, query.low)
}

pub fn left4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    lower2D(&rect_box.range_box_x, &query.left)
}

pub fn overLeft4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overLower2D(&rect_box.range_box_x, &query.left)
}

pub fn right4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    higher2D(&rect_box.range_box_x, &query.left)
}

pub fn overRight4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overHigher2D(&rect_box.range_box_x, &query.left)
}

pub fn below4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    lower2D(&rect_box.range_box_y, &query.right)
}

pub fn overBelow4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overLower2D(&rect_box.range_box_y, &query.right)
}

pub fn above4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    higher2D(&rect_box.range_box_y, &query.right)
}

pub fn overAbove4D(rect_box: &RectBox, query: &RangeBox) -> bool {
    overHigher2D(&rect_box.range_box_y, &query.right)
}

fn fc_spg_box_quad_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol — args are live in/out structs.
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    cfg.prefixType = BOXOID;
    cfg.labelType = VOIDOID;
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
    Ok(Datum::null())
}

fn fc_spg_box_quad_choose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgChooseIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgChooseOut) };

    // nodeN is set by core when allTheSame.
    let nodeN = if input.allTheSame {
        0
    } else {
        // SAFETY: box-typed prefix/leaf datums per config.
        let (centroid, b) = unsafe { (box_at(input.prefixDatum), box_at(input.leafDatum)) };
        getQuadrant(&centroid, &b) as i32
    };
    *out = spgChooseOut::MatchNode { nodeN, levelAdd: 0, restDatum: input.leafDatum };
    Ok(Datum::null())
}

fn fc_spg_box_quad_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgPickSplitIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgPickSplitOut) };
    let mcx = fcinfo.result_mcx();
    let n = input.nTuples as usize;

    // SAFETY: nTuples box datums per protocol.
    let datums = unsafe { core::slice::from_raw_parts(input.datums, n) };

    let mut lowXs: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut highXs: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut lowYs: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut highYs: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for &d in datums {
        // SAFETY: box-typed leaf datums.
        let b = unsafe { box_at(d) };
        lowXs.push(b.low.x);
        highXs.push(b.high.x);
        lowYs.push(b.low.y);
        highYs.push(b.high.y);
    }

    // C qsorts with a non-total comparator (NaNs land arbitrarily); total_cmp
    // matches it on all non-NaN inputs and only moves the median pick — index
    // shape, never results — when NaN/-0.0 coordinates are present.
    lowXs.sort_unstable_by(f64::total_cmp);
    highXs.sort_unstable_by(f64::total_cmp);
    lowYs.sort_unstable_by(f64::total_cmp);
    highYs.sort_unstable_by(f64::total_cmp);

    let median = n / 2;
    let centroid = BOX {
        high: ::types_core::geo::Point { x: highXs[median], y: highYs[median] },
        low: ::types_core::geo::Point { x: lowXs[median], y: lowYs[median] },
    };

    out.hasPrefix = true;
    out.prefixDatum = form_box_datum(mcx, &centroid)?;
    out.nNodes = 16;
    out.nodeLabels = core::ptr::null();

    let mut map: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut leaf: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for &d in datums {
        // SAFETY: box-typed leaf datums.
        let b = unsafe { box_at(d) };
        leaf.push(d);
        map.push(getQuadrant(&centroid, &b) as i32);
    }
    out.mapTuplesToNodes = map.as_mut_ptr();
    core::mem::forget(map);
    out.leafTupleDatums = leaf.as_ptr();
    core::mem::forget(leaf);
    Ok(Datum::null())
}

fn is_bounding_box_test_exact(strategy: u16) -> bool {
    matches!(
        strategy,
        RTLeftStrategyNumber
            | RTOverLeftStrategyNumber
            | RTOverRightStrategyNumber
            | RTRightStrategyNumber
            | RTOverBelowStrategyNumber
            | RTBelowStrategyNumber
            | RTAboveStrategyNumber
            | RTOverAboveStrategyNumber
    )
}

// spg_box_quad_get_scankey_bbox: a polygon key queries by its bounding box.
fn scankey_bbox(mcx: Mcx<'_>, sk: &ScanKeyData, recheck: Option<&mut bool>) -> PgResult<BOX> {
    match sk.sk_subtype {
        BOXOID => {
            // SAFETY: box-typed scankey argument.
            Ok(unsafe { box_at(sk.sk_argument) })
        }
        POLYGONOID => {
            if let Some(r) = recheck {
                if !is_bounding_box_test_exact(sk.sk_strategy) {
                    *r = true;
                }
            }
            // SAFETY: polygon-typed (varlena) scankey argument, live for the call.
            let poly = unsafe { ::types_fmgr::datum_varlena_packed(sk.sk_argument, mcx) }?;
            Ok(BOX::from_datum_bytes(&poly.data()[4..36]))
        }
        other => panic!("unrecognized scankey subtype: {other}"),
    }
}

fn fc_spg_box_quad_inner_consistent(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgInnerConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgInnerConsistentOut) };
    let mcx = fcinfo.result_mcx();

    let rect_box = if input.traversalValue != 0 {
        // SAFETY: traversal value written by a previous call below, still live
        // in traversalMemoryContext.
        unsafe { *(input.traversalValue as *const RectBox) }
    } else {
        initRectBox()
    };

    if input.allTheSame {
        let n = input.nNodes as usize;
        let mut nums: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
        for i in 0..n {
            nums.push(i as i32);
        }
        out.nNodes = input.nNodes;
        out.nodeNumbers = nums.as_ptr();
        core::mem::forget(nums);
        if input.norderbys > 0 && n > 0 {
            let mut rows: ::mcx::PgVec<'_, *const f64> = ::mcx::vec_with_capacity_in(mcx, n)?;
            for _ in 0..n {
                rows.push(inner_distances_row(mcx, input, &rect_box)?);
            }
            out.distances = rows.as_ptr();
            core::mem::forget(rows);
        }
        return Ok(Datum::null());
    }

    // SAFETY: box-typed prefix per config.
    let centroid = getRangeBox(unsafe { &box_at(input.prefixDatum) });
    let scankeys =
        // SAFETY: nkeys scankeys per protocol.
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };
    let mut queries: ::mcx::PgVec<'_, RangeBox> =
        ::mcx::vec_with_capacity_in(mcx, scankeys.len())?;
    for sk in scankeys {
        queries.push(getRangeBox(&scankey_bbox(mcx, sk, None)?));
    }

    let n_nodes = input.nNodes.max(0) as usize;
    let mut nums: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n_nodes)?;
    let mut traversals: ::mcx::PgVec<'_, usize> = ::mcx::vec_with_capacity_in(mcx, n_nodes)?;
    let mut rows: ::mcx::PgVec<'_, *const f64> = ::mcx::vec_with_capacity_in(mcx, n_nodes)?;

    for quadrant in 0..n_nodes as u8 {
        let next_rect_box = nextRectBox(&rect_box, &centroid, quadrant);
        let mut flag = true;

        for (sk, query) in scankeys.iter().zip(queries.iter()) {
            flag = match sk.sk_strategy {
                RTOverlapStrategyNumber => overlap4D(&next_rect_box, query),
                RTContainsStrategyNumber => contain4D(&next_rect_box, query),
                RTSameStrategyNumber | RTContainedByStrategyNumber => {
                    contained4D(&next_rect_box, query)
                }
                RTLeftStrategyNumber => left4D(&next_rect_box, query),
                RTOverLeftStrategyNumber => overLeft4D(&next_rect_box, query),
                RTRightStrategyNumber => right4D(&next_rect_box, query),
                RTOverRightStrategyNumber => overRight4D(&next_rect_box, query),
                RTAboveStrategyNumber => above4D(&next_rect_box, query),
                RTOverAboveStrategyNumber => overAbove4D(&next_rect_box, query),
                RTBelowStrategyNumber => below4D(&next_rect_box, query),
                RTOverBelowStrategyNumber => overBelow4D(&next_rect_box, query),
                other => panic!("unrecognized strategy number: {other}"),
            };
            if !flag {
                break;
            }
        }

        if flag {
            // The kept traversal value lives in traversalMemoryContext (C
            // contract: it outlives this call, until the child is visited).
            let mut tv: ::mcx::PgVec<'_, RectBox> =
                ::mcx::vec_with_capacity_in(input.traversalMemoryContext, 1)?;
            tv.push(next_rect_box);
            let ptr = tv.as_ptr() as usize;
            core::mem::forget(tv);
            traversals.push(ptr);
            nums.push(quadrant as i32);
            if input.norderbys > 0 {
                rows.push(inner_distances_row(mcx, input, &next_rect_box)?);
            }
        }
    }

    out.nNodes = nums.len() as i32;
    out.nodeNumbers = nums.as_ptr();
    core::mem::forget(nums);
    out.traversalValues = traversals.as_ptr();
    core::mem::forget(traversals);
    if input.norderbys > 0 {
        out.distances = rows.as_ptr();
        core::mem::forget(rows);
    }
    Ok(Datum::null())
}

fn fc_spg_box_quad_leaf_consistent(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgLeafConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgLeafConsistentOut) };
    let mcx = fcinfo.result_mcx();

    // All tests are exact (a polygon key flips recheck in scankey_bbox).
    out.recheck = false;

    // Don't return leafValue unless told to: for the polygon opclass the leaf
    // datum is a box, not the indexed type.
    if input.returnData {
        out.leafValue = input.leafDatum;
    }

    // SAFETY: box-typed leaf datum.
    let leaf = unsafe { box_at(input.leafDatum) };
    let scankeys =
        // SAFETY: nkeys scankeys per protocol.
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let mut flag = true;
    for sk in scankeys {
        let query = scankey_bbox(mcx, sk, Some(&mut out.recheck))?;
        flag = match sk.sk_strategy {
            RTOverlapStrategyNumber => box_overlap(&leaf, &query),
            RTContainsStrategyNumber => box_contain(&leaf, &query),
            RTContainedByStrategyNumber => box_contained(&leaf, &query),
            RTSameStrategyNumber => box_same(&leaf, &query),
            RTLeftStrategyNumber => box_left(&leaf, &query),
            RTOverLeftStrategyNumber => box_overleft(&leaf, &query),
            RTRightStrategyNumber => box_right(&leaf, &query),
            RTOverRightStrategyNumber => box_overright(&leaf, &query),
            RTAboveStrategyNumber => box_above(&leaf, &query),
            RTOverAboveStrategyNumber => box_overabove(&leaf, &query),
            RTBelowStrategyNumber => box_below(&leaf, &query),
            RTOverBelowStrategyNumber => box_overbelow(&leaf, &query),
            other => panic!("unrecognized strategy number: {other}"),
        };
        if !flag {
            break;
        }
    }

    if flag && input.norderbys > 0 {
        // SAFETY: norderbys orderby scankeys per protocol.
        let orderbys = unsafe {
            core::slice::from_raw_parts(input.orderbys, input.norderbys.max(0) as usize)
        };
        // The leaf key is a box even for the polygon opclass, hence is_leaf=false.
        let row =
            ::spgist_proc::spg_key_orderbys_distances(mcx, input.leafDatum, false, orderbys)?;
        out.distances = row.as_ptr();
        core::mem::forget(row);
        // Recheck is necessary when computing distance to a polygon (F_DIST_POLYP).
        out.recheckDistances = orderbys[0].sk_func.fn_oid == F_DIST_POLYP;
    }
    Ok(Datum::from_bool(flag))
}

fn fc_spg_bbox_quad_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    cfg.prefixType = BOXOID;
    cfg.labelType = VOIDOID;
    cfg.leafType = BOXOID;
    cfg.canReturnData = false;
    cfg.longValuesOK = false;
    Ok(Datum::null())
}

fn fc_spg_poly_quad_compress(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 is a non-null polygon varlena, live for the call.
    let poly = unsafe { fcinfo.arg_varlena_packed(0) }?;
    // boundbox sits after the npts field in the POLYGON payload.
    let boundbox = BOX::from_datum_bytes(&poly.data()[4..36]);
    form_box_datum(fcinfo.result_mcx(), &boundbox)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SPGIST_BOX_BUILTINS: &[FmgrBuiltin] = &[
    b(5010, "spg_bbox_quad_config", 2, fc_spg_bbox_quad_config),
    b(5011, "spg_poly_quad_compress", 1, fc_spg_poly_quad_compress),
    b(5012, "spg_box_quad_config", 2, fc_spg_box_quad_config),
    b(5013, "spg_box_quad_choose", 2, fc_spg_box_quad_choose),
    b(5014, "spg_box_quad_picksplit", 2, fc_spg_box_quad_picksplit),
    b(5015, "spg_box_quad_inner_consistent", 2, fc_spg_box_quad_inner_consistent),
    b(5016, "spg_box_quad_leaf_consistent", 2, fc_spg_box_quad_leaf_consistent),
];

#[cfg(test)]
mod tests;
