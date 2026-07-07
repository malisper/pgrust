//! spgproc.c: SP-GiST point-opclass distance helpers for ordered (KNN) scans.
#![allow(non_snake_case)]

use ::adt_float::get_float8_nan;
use ::adt_geo::{pg_hypot, point_dt};
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::geo::{Point, BOX};
use ::types_error::PgResult;
use ::types_scan::scankey::ScanKeyData;

pub fn point_box_distance(point: &Point, b: &BOX) -> PgResult<f64> {
    if point.x.is_nan() || b.low.x.is_nan() || point.y.is_nan() || b.low.y.is_nan() {
        return Ok(get_float8_nan());
    }

    let dx = if point.x < b.low.x {
        b.low.x - point.x
    } else if point.x > b.high.x {
        point.x - b.high.x
    } else {
        0.0
    };
    let dy = if point.y < b.low.y {
        b.low.y - point.y
    } else if point.y > b.high.y {
        point.y - b.high.y
    } else {
        0.0
    };
    pg_hypot(dx, dy)
}

// Leaf key is a point, non-leaf key a box; orderby arguments are points.
pub fn spg_key_orderbys_distances<'mcx>(
    mcx: Mcx<'mcx>,
    key: Datum,
    is_leaf: bool,
    orderbys: &[ScanKeyData],
) -> PgResult<PgVec<'mcx, f64>> {
    let mut distances: PgVec<'mcx, f64> = ::mcx::vec_with_capacity_in(mcx, orderbys.len())?;
    for sk in orderbys {
        // SAFETY: orderby args are point images; key is point (leaf) / box (inner).
        let point = unsafe { point_at(sk.sk_argument) };
        let d = if is_leaf {
            point_dt(&point, unsafe { &point_at(key) })?
        } else {
            point_box_distance(&point, unsafe { &box_at(key) })?
        };
        distances.push(d);
    }
    Ok(distances)
}

// SAFETY: datum points at a live 16-byte point image.
#[inline]
unsafe fn point_at(d: Datum) -> Point {
    Point::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 16))
}

// SAFETY: datum points at a live 32-byte box image.
#[inline]
unsafe fn box_at(d: Datum) -> BOX {
    BOX::from_datum_bytes(core::slice::from_raw_parts(d.as_usize() as *const u8, 32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_box() {
        let b = BOX { high: Point { x: 2.0, y: 3.0 }, low: Point { x: -1.0, y: -1.0 } };
        assert_eq!(point_box_distance(&Point { x: 0.0, y: 0.0 }, &b).unwrap(), 0.0);
        assert_eq!(point_box_distance(&Point { x: 5.0, y: 3.0 }, &b).unwrap(), 3.0);
        assert_eq!(point_box_distance(&Point { x: 2.0, y: 7.0 }, &b).unwrap(), 4.0);
        let d = point_box_distance(&Point { x: 5.0, y: 7.0 }, &b).unwrap();
        assert_eq!(d, 5.0);
        assert!(point_box_distance(&Point { x: f64::NAN, y: 0.0 }, &b).unwrap().is_nan());
    }
}
