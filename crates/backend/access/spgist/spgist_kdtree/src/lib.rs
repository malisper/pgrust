//! spgkdtreeproc.c: k-d tree over points — the kd_point_ops opclass.
//! leaf_consistent is spg_quad_leaf_consistent (oid 4022), registered by
//! spgist_quadtree; pg_amproc points the kd opclass there, as in C.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::adt_geo::{FPgt, FPlt};
use ::datum::Datum;
use ::types_core::geo::Point;
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
    spgChooseIn, spgChooseOut, spgInnerConsistentIn, spgInnerConsistentOut, spgPickSplitIn,
    spgPickSplitOut,
};

const FLOAT8OID: Oid = 701;
const VOIDOID: Oid = 2278;

// SAFETY: datum points at a live 16-byte point image (opclass protocol).
#[inline]
unsafe fn point_at(d: Datum) -> Point {
    Point::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 16))
}

// SAFETY: datum points at a live 32-byte box image (opclass protocol).
#[inline]
unsafe fn box_at(d: Datum) -> ::types_core::geo::BOX {
    ::types_core::geo::BOX::from_datum_bytes(core::slice::from_raw_parts(
        d.as_usize() as *const u8,
        32,
    ))
}

#[inline]
fn getSide(coord: f64, is_x: bool, tst: &Point) -> i32 {
    let tstcoord = if is_x { tst.x } else { tst.y };
    if coord == tstcoord {
        0
    } else if coord > tstcoord {
        1
    } else {
        -1
    }
}

pub fn spg_kd_config(cfg: &mut spgConfigOut) {
    cfg.prefixType = FLOAT8OID;
    cfg.labelType = VOIDOID;
    cfg.canReturnData = true;
    cfg.longValuesOK = false;
}

fn fc_spg_kd_config(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol — args are live in/out structs.
    let cfg = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgConfigOut) };
    spg_kd_config(cfg);
    Ok(Datum::null())
}

fn fc_spg_kd_choose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgChooseIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgChooseOut) };

    if input.allTheSame {
        panic!("allTheSame should not occur for k-d trees");
    }
    debug_assert!(input.hasPrefix);
    debug_assert!(input.nNodes == 2);
    // SAFETY: point-typed input datum per config.
    let in_point = unsafe { point_at(input.datum) };
    let coord = input.prefixDatum.as_f64();

    *out = spgChooseOut::MatchNode {
        nodeN: if getSide(coord, input.level % 2 != 0, &in_point) > 0 { 0 } else { 1 },
        levelAdd: 1,
        // restDatum = PointPGetDatum(inPoint): the input pointer, as in C.
        restDatum: input.datum,
    };
    Ok(Datum::null())
}

fn fc_spg_kd_picksplit(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgPickSplitIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgPickSplitOut) };
    let mcx = fcinfo.result_mcx();
    let n = input.nTuples as usize;

    // SAFETY: nTuples point datums per protocol.
    let datums = unsafe { core::slice::from_raw_parts(input.datums, n) };

    let mut sorted: ::mcx::PgVec<'_, (Point, i32)> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for (i, &d) in datums.iter().enumerate() {
        // SAFETY: point-typed leaf datums.
        sorted.push((unsafe { point_at(d) }, i as i32));
    }

    // C's x_cmp/y_cmp under qsort (unstable); stable sort here — equal-coord
    // points at the median may map to the other node than C's, which the C
    // header comment declares acceptable (inner_consistent visits both sides).
    let by_x = input.level % 2 != 0;
    if by_x {
        sorted.sort_by(|a, b| cmp_coord(a.0.x, b.0.x));
    } else {
        sorted.sort_by(|a, b| cmp_coord(a.0.y, b.0.y));
    }
    let middle = n >> 1;
    let coord = if by_x { sorted[middle].0.x } else { sorted[middle].0.y };

    out.hasPrefix = true;
    out.prefixDatum = Datum::from_f64(coord);
    out.nNodes = 2;
    out.nodeLabels = core::ptr::null();

    let mut map: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, n)?;
    map.resize(n, 0);
    let mut leaf: ::mcx::PgVec<'_, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    leaf.resize(n, Datum::null());
    for (i, &(_, orig)) in sorted.iter().enumerate() {
        map[orig as usize] = if i < middle { 0 } else { 1 };
        leaf[orig as usize] = datums[orig as usize];
    }
    out.mapTuplesToNodes = map.as_mut_ptr();
    core::mem::forget(map);
    out.leafTupleDatums = leaf.as_ptr();
    core::mem::forget(leaf);
    Ok(Datum::null())
}

// C comparator shape: 0 on ==, 1 on >, else -1 (NaNs sort low).
#[inline]
fn cmp_coord(a: f64, b: f64) -> core::cmp::Ordering {
    if a == b {
        core::cmp::Ordering::Equal
    } else if a > b {
        core::cmp::Ordering::Greater
    } else {
        core::cmp::Ordering::Less
    }
}

fn fc_spg_kd_inner_consistent(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: spgist opclass fmgr protocol.
    let input = unsafe { &*(fcinfo.arg(0).as_usize() as *const spgInnerConsistentIn) };
    let out = unsafe { &mut *(fcinfo.arg(1).as_usize() as *mut spgInnerConsistentOut) };
    let mcx = fcinfo.result_mcx();

    debug_assert!(input.hasPrefix);
    let coord = input.prefixDatum.as_f64();
    if input.allTheSame {
        panic!("allTheSame should not occur for k-d trees");
    }
    debug_assert!(input.nNodes == 2);

    let scankeys =
        // SAFETY: nkeys scankeys per protocol.
        unsafe { core::slice::from_raw_parts(input.scankeys, input.nkeys.max(0) as usize) };

    let by_x = input.level % 2 != 0;
    let mut which: i32 = (1 << 1) | (1 << 2);
    for key in scankeys {
        // SAFETY: point-typed (box for ContainedBy) scankey arguments.
        let query = unsafe { point_at(key.sk_argument) };
        match key.sk_strategy {
            RTLeftStrategyNumber => {
                if by_x && FPlt(query.x, coord) {
                    which &= 1 << 1;
                }
            }
            RTRightStrategyNumber => {
                if by_x && FPgt(query.x, coord) {
                    which &= 1 << 2;
                }
            }
            RTSameStrategyNumber => {
                let q = if by_x { query.x } else { query.y };
                if FPlt(q, coord) {
                    which &= 1 << 1;
                } else if FPgt(q, coord) {
                    which &= 1 << 2;
                }
            }
            RTBelowStrategyNumber | RTOldBelowStrategyNumber => {
                if !by_x && FPlt(query.y, coord) {
                    which &= 1 << 1;
                }
            }
            RTAboveStrategyNumber | RTOldAboveStrategyNumber => {
                if !by_x && FPgt(query.y, coord) {
                    which &= 1 << 2;
                }
            }
            RTContainedByStrategyNumber => {
                // SAFETY: query is a box for this strategy (C's DatumGetBoxP cheat).
                let boxQuery = unsafe { box_at(key.sk_argument) };
                let (hi, lo) = if by_x {
                    (boxQuery.high.x, boxQuery.low.x)
                } else {
                    (boxQuery.high.y, boxQuery.low.y)
                };
                if FPlt(hi, coord) {
                    which &= 1 << 1;
                } else if FPgt(lo, coord) {
                    which &= 1 << 2;
                }
            }
            other => panic!("unrecognized strategy number: {other}"),
        }
        if which == 0 {
            break;
        }
    }

    out.nNodes = 0;
    if which == 0 {
        return Ok(Datum::null());
    }

    // Split bounding boxes for the two children (KNN traversal values).
    let bboxes = if input.norderbys > 0 {
        use ::types_core::geo::BOX;
        let area = if input.level == 0 {
            let inf = f64::INFINITY;
            BOX {
                high: Point { x: inf, y: inf },
                low: Point { x: -inf, y: -inf },
            }
        } else {
            debug_assert!(input.traversalValue != 0);
            // SAFETY: the parent stored a box in traversalValue on this path.
            unsafe { box_at(Datum::from_usize(input.traversalValue)) }
        };
        let mut b0 = BOX { high: area.high, low: area.low };
        let mut b1 = BOX { high: area.high, low: area.low };
        if by_x {
            b0.high.x = coord;
            b1.low.x = coord;
        } else {
            b0.high.y = coord;
            b1.low.y = coord;
        }
        Some([b0, b1])
    } else {
        None
    };

    let mut nums: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    let mut tvals: ::mcx::PgVec<'_, usize> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    let mut rows: ::mcx::PgVec<'_, *const f64> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    for i in 1..=2 {
        if which & (1 << i) != 0 {
            nums.push(i - 1);
            if let Some(bboxes) = &bboxes {
                let b = &bboxes[(i - 1) as usize];
                let mut buf: ::mcx::PgVec<'_, f64> =
                    ::mcx::vec_with_capacity_in(input.traversalMemoryContext, 4)?;
                buf.push(b.high.x);
                buf.push(b.high.y);
                buf.push(b.low.x);
                buf.push(b.low.y);
                let box_datum = Datum::from_usize(buf.as_ptr() as usize);
                core::mem::forget(buf);
                // SAFETY: norderbys orderby scankeys per protocol.
                let orderbys = unsafe {
                    core::slice::from_raw_parts(
                        input.orderbys,
                        input.norderbys.max(0) as usize,
                    )
                };
                let row = ::spgist_proc::spg_key_orderbys_distances(
                    mcx, box_datum, false, orderbys,
                )?;
                tvals.push(box_datum.as_usize());
                rows.push(row.as_ptr());
                core::mem::forget(row);
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

    let mut level_adds: ::mcx::PgVec<'_, i32> = ::mcx::vec_with_capacity_in(mcx, 2)?;
    level_adds.resize(2, 1);
    out.levelAdds = level_adds.as_ptr();
    core::mem::forget(level_adds);
    Ok(Datum::null())
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: ::types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SPGIST_KD_BUILTINS: &[FmgrBuiltin] = &[
    b(4023, "spg_kd_config", 2, fc_spg_kd_config),
    b(4024, "spg_kd_choose", 2, fc_spg_kd_choose),
    b(4025, "spg_kd_picksplit", 2, fc_spg_kd_picksplit),
    b(4026, "spg_kd_inner_consistent", 2, fc_spg_kd_inner_consistent),
];
