//! spgquadtreeproc.c: quad tree over points — the quad_point_ops opclass.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::adt_geo::point::{point_above, point_below, point_eq, point_horiz, point_left,
    point_right, point_vert};
use ::adt_geo::proximity::box_contain_pt;
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::geo::{Point, BOX};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use ::types_scan::scankey::{
    RTAboveStrategyNumber, RTBelowStrategyNumber, RTContainedByStrategyNumber,
    RTLeftStrategyNumber, RTOldAboveStrategyNumber, RTOldBelowStrategyNumber,
    RTRightStrategyNumber, RTSameStrategyNumber,
};
use ::types_spgist::spgConfigOut;
use ::types_spgist::state::{
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgLeafConsistentIn,
    spgLeafConsistentOut, spgPickSplitIn, spgPickSplitOut,
};

const POINTOID: Oid = 600;
const VOIDOID: Oid = 2278;

// SAFETY: datum points at a live 16-byte point image (opclass protocol).
#[inline]
pub(crate) unsafe fn point_at(d: Datum) -> Point {
    Point::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 16))
}

// SAFETY: datum points at a live 32-byte box image (opclass protocol).
#[inline]
pub(crate) unsafe fn box_at(d: Datum) -> BOX {
    BOX::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 32))
}

// PointPGetDatum of a fresh point: 8-aligned like C's palloc'd Point.
pub(crate) fn form_point_datum(mcx: Mcx<'_>, p: &Point) -> PgResult<Datum> {
    let mut buf: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    buf.push(p.x);
    buf.push(p.y);
    let ptr = buf.as_ptr() as usize;
    core::mem::forget(buf);
    Ok(Datum::from_usize(ptr))
}

// BoxPGetDatum of a fresh box: 8-aligned like C's palloc'd BOX.
pub(crate) fn form_box_datum(mcx: Mcx<'_>, b: &BOX) -> PgResult<Datum> {
    let mut buf: ::mcx::PgVec<'_, f64> = ::mcx::vec_with_capacity_in(mcx, 4)?;
    buf.push(b.high.x);
    buf.push(b.high.y);
    buf.push(b.low.x);
    buf.push(b.low.y);
    let ptr = buf.as_ptr() as usize;
    core::mem::forget(buf);
    Ok(Datum::from_usize(ptr))
}

// The KNN traversal bounding box: infinite at the root, else the box saved
// in the parent's traversalValue.
pub(crate) fn orderby_traversal_bbox(input: &spgInnerConsistentIn<'_>) -> BOX {
    if input.level == 0 {
        let inf = f64::INFINITY;
        BOX { high: Point { x: inf, y: inf }, low: Point { x: -inf, y: -inf } }
    } else {
        debug_assert!(input.traversalValue != 0);
        // SAFETY: the parent stored a box in traversalValue on this path.
        unsafe { box_at(Datum::from_usize(input.traversalValue)) }
    }
}

// Child-box traversalValue (in the scan-lifetime traversal context) plus its
// distances row (in the armed per-call mcx).
pub(crate) fn orderby_node_outputs(
    result_mcx: Mcx<'_>,
    input: &spgInnerConsistentIn<'_>,
    child_box: &BOX,
) -> PgResult<(usize, *const f64)> {
    let box_datum = form_box_datum(input.traversalMemoryContext, child_box)?;
    // SAFETY: norderbys orderby scankeys per protocol.
    let orderbys = unsafe {
        core::slice::from_raw_parts(input.orderbys, input.norderbys.max(0) as usize)
    };
    let row = ::spgist_proc::spg_key_orderbys_distances(result_mcx, box_datum, false, orderbys)?;
    let row_ptr = row.as_ptr();
    core::mem::forget(row);
    Ok((box_datum.as_usize(), row_ptr))
}

pub fn getQuadrant(centroid: &Point, tst: &Point) -> i16 {
    if (point_above(tst, centroid) || point_horiz(tst, centroid))
        && (point_right(tst, centroid) || point_vert(tst, centroid))
    {
        return 1;
    }
    if point_below(tst, centroid) && (point_right(tst, centroid) || point_vert(tst, centroid)) {
        return 2;
    }
    if (point_below(tst, centroid) || point_horiz(tst, centroid)) && point_left(tst, centroid) {
        return 3;
    }
    if point_above(tst, centroid) && point_left(tst, centroid) {
        return 4;
    }
    panic!("getQuadrant: impossible case");
}

pub fn getQuadrantArea(bbox: &BOX, centroid: &Point, quadrant: i32) -> BOX {
    let mut result = BOX::default();
    match quadrant {
        1 => {
            result.high = bbox.high;
            result.low = *centroid;
        }
        2 => {
            result.high.x = bbox.high.x;
            result.high.y = centroid.y;
            result.low.x = centroid.x;
            result.low.y = bbox.low.y;
        }
        3 => {
            result.high = *centroid;
            result.low = bbox.low;
        }
        4 => {
            result.high.x = centroid.x;
            result.high.y = bbox.high.y;
            result.low.x = bbox.low.x;
            result.low.y = centroid.y;
        }
        _ => {}
    }
    result
}

pub fn spg_quad_config(cfg: &mut spgConfigOut) {
    cfg.prefixType = POINTOID;
    cfg.labelType = VOIDOID;
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

fn fc_spg_quad_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol — args are live in/out structs.
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    spg_quad_config(cfg);
    Ok(Datum::null())
}

fn fc_spg_quad_choose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgChooseIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgChooseOut) };

    // restDatum = PointPGetDatum(inPoint): C hands back the input pointer.
    if input.allTheSame {
        *out = spgChooseOut::MatchNode { nodeN: 0, levelAdd: 0, restDatum: input.datum };
        return Ok(Datum::null());
    }

    debug_assert!(input.hasPrefix);
    debug_assert!(input.nNodes == 4);
    // SAFETY: point-typed datum/prefixDatum per config.
    let (in_point, centroid) = unsafe { (point_at(input.datum), point_at(input.prefixDatum)) };

    *out = spgChooseOut::MatchNode {
        nodeN: getQuadrant(&centroid, &in_point) as i32 - 1,
        levelAdd: 0,
        restDatum: input.datum,
    };
    Ok(Datum::null())
}

fn fc_spg_quad_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgPickSplitIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgPickSplitOut) };
    let mcx = fcinfo.result_mcx();
    let n = input.nTuples as usize;

    // SAFETY: nTuples point datums per protocol.
    let datums = unsafe { core::slice::from_raw_parts(input.datums, n) };

    let mut centroid = Point { x: 0.0, y: 0.0 };
    for &d in datums {
        // SAFETY: point-typed leaf datums.
        let p = unsafe { point_at(d) };
        centroid.x += p.x;
        centroid.y += p.y;
    }
    centroid.x /= n as f64;
    centroid.y /= n as f64;

    out.hasPrefix = true;
    out.prefixDatum = form_point_datum(mcx, &centroid)?;
    out.nNodes = 4;
    out.nodeLabels = core::ptr::null();

    let mut map: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut leaf: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for &d in datums {
        // SAFETY: point-typed leaf datums.
        let p = unsafe { point_at(d) };
        leaf.push(d);
        map.push(getQuadrant(&centroid, &p) as i32 - 1);
    }
    out.mapTuplesToNodes = map.as_mut_ptr();
    core::mem::forget(map);
    out.leafTupleDatums = leaf.as_ptr();
    core::mem::forget(leaf);
    Ok(Datum::null())
}

fn fc_spg_quad_inner_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgInnerConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgInnerConsistentOut) };
    let mcx = fcinfo.result_mcx();

    debug_assert!(input.hasPrefix);
    // SAFETY: point-typed prefix per config.
    let centroid = unsafe { point_at(input.prefixDatum) };

    if input.allTheSame {
        let n = input.nNodes as usize;
        let mut nums: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
        for i in 0..n {
            nums.push(i as i32);
        }
        out.nNodes = input.nNodes;
        out.nodeNumbers = nums.as_ptr();
        core::mem::forget(nums);
        if input.norderbys > 0 {
            // Use the parent quadrant box as every child's traversalValue.
            let bbox = orderby_traversal_bbox(input);
            let mut tvals: ::mcx::PgVec<'_, usize> = ::mcx::vec_with_capacity_in(mcx, n)?;
            let mut rows: ::mcx::PgVec<'_, *const f64> = ::mcx::vec_with_capacity_in(mcx, n)?;
            for _ in 0..n {
                let (tv, row) = orderby_node_outputs(mcx, input, &bbox)?;
                tvals.push(tv);
                rows.push(row);
            }
            out.traversalValues = tvals.as_ptr();
            core::mem::forget(tvals);
            out.distances = rows.as_ptr();
            core::mem::forget(rows);
        }
        return Ok(Datum::null());
    }

    debug_assert!(input.nNodes == 4);
    let scankeys =
        // SAFETY: nkeys scankeys per protocol.
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let mut which: i32 = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 4);
    for key in scankeys {
        // SAFETY: point-typed (box for ContainedBy) scankey arguments.
        let query = unsafe { point_at(key.sk_argument) };
        match key.sk_strategy {
            RTLeftStrategyNumber => {
                if point_right(&centroid, &query) {
                    which &= (1 << 3) | (1 << 4);
                }
            }
            RTRightStrategyNumber => {
                if point_left(&centroid, &query) {
                    which &= (1 << 1) | (1 << 2);
                }
            }
            RTSameStrategyNumber => {
                which &= 1 << getQuadrant(&centroid, &query);
            }
            RTBelowStrategyNumber | RTOldBelowStrategyNumber => {
                if point_above(&centroid, &query) {
                    which &= (1 << 2) | (1 << 3);
                }
            }
            RTAboveStrategyNumber | RTOldAboveStrategyNumber => {
                if point_below(&centroid, &query) {
                    which &= (1 << 1) | (1 << 4);
                }
            }
            RTContainedByStrategyNumber => {
                // SAFETY: query is a box for this strategy (C's DatumGetBoxP cheat).
                let boxQuery = unsafe { box_at(key.sk_argument) };
                if box_contain_pt(&boxQuery, &centroid) {
                    // centroid in box: all quadrants stay
                } else {
                    let mut r = 0;
                    let mut p = boxQuery.low;
                    r |= 1 << getQuadrant(&centroid, &p);
                    p.y = boxQuery.high.y;
                    r |= 1 << getQuadrant(&centroid, &p);
                    p = boxQuery.high;
                    r |= 1 << getQuadrant(&centroid, &p);
                    p.x = boxQuery.low.x;
                    r |= 1 << getQuadrant(&centroid, &p);
                    which &= r;
                }
            }
            other => panic!("unrecognized strategy number: {other}"),
        }
        if which == 0 {
            break;
        }
    }

    let mut level_adds: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, 4)?;
    level_adds.resize(4, 1);
    out.levelAdds = level_adds.as_ptr();
    core::mem::forget(level_adds);

    let bbox = if input.norderbys > 0 { Some(orderby_traversal_bbox(input)) } else { None };
    let mut nums: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, 4)?;
    let mut tvals: ::mcx::PgVec<'_, usize> = ::mcx::vec_with_capacity_in(mcx, 4)?;
    let mut rows: ::mcx::PgVec<'_, *const f64> = ::mcx::vec_with_capacity_in(mcx, 4)?;
    for i in 1..=4 {
        if which & (1 << i) != 0 {
            nums.push(i - 1);
            if let Some(bbox) = &bbox {
                let quadrant = getQuadrantArea(bbox, &centroid, i);
                let (tv, row) = orderby_node_outputs(mcx, input, &quadrant)?;
                tvals.push(tv);
                rows.push(row);
            }
        }
    }
    out.nNodes = nums.len() as i32;
    out.nodeNumbers = nums.as_ptr();
    core::mem::forget(nums);
    if input.norderbys > 0 {
        out.traversalValues = tvals.as_ptr();
        core::mem::forget(tvals);
        out.distances = rows.as_ptr();
        core::mem::forget(rows);
    }
    Ok(Datum::null())
}

fn fc_spg_quad_leaf_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgLeafConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgLeafConsistentOut) };

    out.recheck = false;
    out.leafValue = input.leafDatum;
    // SAFETY: point-typed leaf datum.
    let datum = unsafe { point_at(input.leafDatum) };

    let scankeys =
        // SAFETY: nkeys scankeys per protocol.
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let mut res = true;
    for key in scankeys {
        // SAFETY: point-typed (box for ContainedBy) scankey arguments.
        let query = unsafe { point_at(key.sk_argument) };
        res = match key.sk_strategy {
            RTLeftStrategyNumber => point_left(&datum, &query),
            RTRightStrategyNumber => point_right(&datum, &query),
            RTSameStrategyNumber => point_eq(&datum, &query),
            RTBelowStrategyNumber | RTOldBelowStrategyNumber => point_below(&datum, &query),
            RTAboveStrategyNumber | RTOldAboveStrategyNumber => point_above(&datum, &query),
            RTContainedByStrategyNumber => {
                // SAFETY: query is a box for this strategy.
                let boxQuery = unsafe { box_at(key.sk_argument) };
                box_contain_pt(&boxQuery, &datum)
            }
            other => panic!("unrecognized strategy number: {other}"),
        };
        if !res {
            break;
        }
    }

    if res && input.norderbys > 0 {
        // it passes -> compute the distances
        // SAFETY: norderbys orderby scankeys per protocol.
        let orderbys = unsafe {
            core::slice::from_raw_parts(input.orderbys, input.norderbys.max(0) as usize)
        };
        let row = ::spgist_proc::spg_key_orderbys_distances(
            fcinfo.result_mcx(),
            input.leafDatum,
            true,
            orderbys,
        )?;
        out.distances = row.as_ptr();
        core::mem::forget(row);
    }
    Ok(Datum::from_bool(res))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SPGIST_QUAD_BUILTINS: &[FmgrBuiltin] = &[
    b(4018, "spg_quad_config", 2, fc_spg_quad_config),
    b(4019, "spg_quad_choose", 2, fc_spg_quad_choose),
    b(4020, "spg_quad_picksplit", 2, fc_spg_quad_picksplit),
    b(4021, "spg_quad_inner_consistent", 2, fc_spg_quad_inner_consistent),
    b(4022, "spg_quad_leaf_consistent", 2, fc_spg_quad_leaf_consistent),
];
