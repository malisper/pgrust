//! Port of `src/backend/access/spgist/spgproc.c` (PostgreSQL 18.3) -- the common
//! supporting procedures shared by the SP-GiST point opclasses.
//!
//! This file owns three pieces of logic:
//!
//! * `point_box_distance` (static, spgproc.c:30) -- point-to-box distance for an
//!   axis-aligned box.
//! * `spg_key_orderbys_distances` (spgproc.c:62) -- distances from a key (leaf
//!   point or inner box) to an array of ordering scan keys (points).
//! * `box_copy` (spgproc.c:81) -- `palloc` a copy of a `BOX` (Rust value-copy).
//!
//! plus the `point_point_distance(p1,p2)` macro (spgproc.c:25), expanded inline
//! as the `point_distance` core it stands for.  The geometry `point_distance` /
//! `point_dt` (geo_ops.c) computes `HYPOT(pt1->x - pt2->x, pt1->y - pt2->y)`;
//! `geo_ops.c` is unported, so the single `HYPOT` (`pg_hypot`, geo_decls.h:91)
//! call is routed through the `backend-utils-adt-geo-ops-seams` `HYPOT` seam,
//! exactly as the sibling `geo_spgist.c` port does.  `pg_hypot` can
//! `ereport(ERROR)` (overflow/underflow), so the distance functions return
//! `PgResult`.
//!
//! This crate OWNS and installs the `spg_key_orderbys_distances` inward seam
//! (declared by `backend-access-spg-proc-seams`), consumed by the SP-GiST point
//! opclass procs (`spgkdtreeproc.c` / `spgquadtreeproc.c`) and `geo_spgist.c`.

#![allow(non_snake_case)]

use types_core::geo::{Point, SpgKey, BOX};
use types_error::PgResult;

use geo_ops_seams::HYPOT;

/// `get_float8_nan()` (float.c) -- `f64::NAN`.  Inlined as in the sibling
/// `geo_spgist.c` port (the float seam owner is not a dependency here).
#[inline]
fn get_float8_nan() -> f64 {
    f64::NAN
}

/// `point_point_distance(p1, p2)` (spgproc.c:25):
///
/// ```c
/// #define point_point_distance(p1,p2) \
///     DatumGetFloat8(DirectFunctionCall2(point_distance, \
///                                        PointPGetDatum(p1), PointPGetDatum(p2)))
/// ```
///
/// `point_distance` -> `point_dt` (geo_ops.c) is
/// `HYPOT(pt1->x - pt2->x, pt1->y - pt2->y)`.
#[inline]
fn point_point_distance(p1: &Point, p2: &Point) -> PgResult<f64> {
    HYPOT::call(p1.x - p2.x, p1.y - p2.y)
}

/// `point_box_distance` (spgproc.c:30) -- point-box distance in the assumption
/// that the box is aligned by axis.
fn point_box_distance(point: &Point, b: &BOX) -> PgResult<f64> {
    let dx: f64;
    let dy: f64;

    // if (isnan(point->x) || isnan(box->low.x) ||
    //     isnan(point->y) || isnan(box->low.y))
    //     return get_float8_nan();
    if point.x.is_nan() || b.low.x.is_nan() || point.y.is_nan() || b.low.y.is_nan() {
        return Ok(get_float8_nan());
    }

    // if (point->x < box->low.x)        dx = box->low.x - point->x;
    // else if (point->x > box->high.x)  dx = point->x - box->high.x;
    // else                              dx = 0.0;
    if point.x < b.low.x {
        dx = b.low.x - point.x;
    } else if point.x > b.high.x {
        dx = point.x - b.high.x;
    } else {
        dx = 0.0;
    }

    // if (point->y < box->low.y)        dy = box->low.y - point->y;
    // else if (point->y > box->high.y)  dy = point->y - box->high.y;
    // else                              dy = 0.0;
    if point.y < b.low.y {
        dy = b.low.y - point.y;
    } else if point.y > b.high.y {
        dy = point.y - b.high.y;
    } else {
        dy = 0.0;
    }

    // return HYPOT(dx, dy);
    HYPOT::call(dx, dy)
}

/// `spg_key_orderbys_distances` (spgproc.c:62).
///
/// Returns distances from the given `key` to an array of ordering scan keys.
/// Leaf key is a point, non-leaf key a box; scan-key arguments are points.
/// The `Datum`-decoding of `key` / `orderbys[i].sk_argument` is the fmgr
/// boundary; the inputs arrive already decoded (`SpgKey` + `&[Point]`).
pub fn spg_key_orderbys_distances(key: &SpgKey, orderby_points: &[Point]) -> PgResult<Vec<f64>> {
    // double *distances = palloc(norderbys * sizeof(double)), *distance = distances;
    let mut distances = Vec::with_capacity(orderby_points.len());

    // for (sk_num = 0; sk_num < norderbys; ++sk_num, ++orderbys, ++distance)
    for point in orderby_points {
        // *distance = isLeaf ? point_point_distance(point, DatumGetPointP(key))
        //                    : point_box_distance(point, DatumGetBoxP(key));
        let distance = match key {
            SpgKey::LeafPoint(k) => point_point_distance(point, k)?,
            SpgKey::InnerBox(k) => point_box_distance(point, k)?,
        };
        distances.push(distance);
    }

    // return distances;
    Ok(distances)
}

/// `box_copy` (spgproc.c:81): `palloc` a copy of `orig`.  Rust value-copy.
pub fn box_copy(orig: &BOX) -> BOX {
    *orig
}

/// Install this crate's inward seams.
pub fn init_seams() {
    spg_proc_seams::spg_key_orderbys_distances::set(spg_key_orderbys_distances);
}
