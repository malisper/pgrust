//! Port of the PostgreSQL `earthdistance` contrib module
//! (`contrib/earthdistance/earthdistance.c`).
//!
//! Computes the great-circle distance between two points on the Earth's
//! surface, treating each [`Point`]'s `x` coordinate as longitude (degrees
//! west of Greenwich) and `y` coordinate as latitude (degrees above the
//! equator). The result is in statute miles.
//!
//! The C module's fmgr boundary (`PG_MODULE_MAGIC_EXT`,
//! `PG_FUNCTION_INFO_V1`, the `Datum`-returning wrapper) is not reproduced;
//! [`geo_distance_datum`] covers callers that still speak the fmgr
//! float8/point representation.

use types_core::geo::Point;
use types_tuple::backend_access_common_heaptuple::Datum;

/// `float8` is C `double`, i.e. Rust `f64`.
pub type Float8 = f64;

/// Earth's radius is in statute miles.
pub const EARTH_RADIUS: f64 = 3958.747716;

const TWO_PI: f64 = 2.0 * std::f64::consts::PI;

/// `degtorad` - convert degrees to radians.
pub fn degtorad(degrees: f64) -> f64 {
    (degrees / 360.0) * TWO_PI
}

/// `geo_distance_internal` - distance between the points in miles on earth's
/// surface.
///
/// For each point, the x-coordinate is longitude in degrees west of Greenwich
/// and the y-coordinate is latitude in degrees above the equator.
pub fn geo_distance_internal(pt1: &Point, pt2: &Point) -> f64 {
    // Convert degrees to radians.
    let long1 = degtorad(pt1.x);
    let lat1 = degtorad(pt1.y);

    let long2 = degtorad(pt2.x);
    let lat2 = degtorad(pt2.y);

    // Compute difference in longitudes - want < 180 degrees.
    let mut longdiff = (long1 - long2).abs();
    if longdiff > std::f64::consts::PI {
        longdiff = TWO_PI - longdiff;
    }

    let latdiff_half = (lat1 - lat2).abs() / 2.0;
    let mut sino = (latdiff_half.sin() * latdiff_half.sin()
        + lat1.cos() * lat2.cos() * (longdiff / 2.0).sin() * (longdiff / 2.0).sin())
    .sqrt();
    if sino > 1.0 {
        sino = 1.0;
    }

    2.0 * EARTH_RADIUS * sino.asin()
}

/// SQL-level `geo_distance(point, point) -> float8`.
pub fn geo_distance(pt1: &Point, pt2: &Point) -> Float8 {
    geo_distance_internal(pt1, pt2)
}

/// `Datum`-level entry point mirroring the C `geo_distance(PG_FUNCTION_ARGS)`
/// wrapper: the two point operands in, the float8 result datum out
/// (`PG_RETURN_FLOAT8`).
pub fn geo_distance_datum(pt1: &Point, pt2: &Point) -> Datum<'static> {
    Datum::from_f64(geo_distance_internal(pt1, pt2))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_distance(pt1: Point, pt2: Point, expected: f64) {
        let actual = geo_distance_internal(&pt1, &pt2);
        assert!(
            (actual - expected).abs() < 0.00001,
            "actual {actual}, expected {expected}"
        );
    }

    #[test]
    fn degtorad_converts_degrees_to_radians() {
        assert_eq!(degtorad(0.0), 0.0);
        assert!((degtorad(180.0) - std::f64::consts::PI).abs() < f64::EPSILON);
        assert!((degtorad(360.0) - TWO_PI).abs() < f64::EPSILON);
    }

    #[test]
    fn geo_distance_matches_postgres_reference_values() {
        assert_distance(Point { x: 0.0, y: 0.0 }, Point { x: 0.0, y: 0.0 }, 0.0);
        assert_distance(
            Point { x: 0.0, y: 0.0 },
            Point { x: 180.0, y: 0.0 },
            12436.77274,
        );
        assert_distance(
            Point { x: 0.0, y: 0.0 },
            Point { x: 0.0, y: 90.0 },
            6218.38637,
        );
        assert_distance(Point { x: 0.0, y: 0.0 }, Point { x: 1.0, y: 0.0 }, 69.09318);
        assert_distance(
            Point { x: 87.6, y: 41.8 },
            Point { x: 106.7, y: 35.1 },
            1129.18983,
        );
    }

    #[test]
    fn geo_distance_is_symmetric() {
        let a = Point { x: 87.6, y: 41.8 };
        let b = Point { x: 106.7, y: 35.1 };
        assert_eq!(geo_distance(&a, &b), geo_distance(&b, &a));
    }

    #[test]
    fn geo_distance_datum_round_trips_through_float8_datum() {
        let pt1 = Point { x: 0.0, y: 0.0 };
        let pt2 = Point { x: 1.0, y: 0.0 };

        let datum = geo_distance_datum(&pt1, &pt2);
        let result = datum.as_f64();

        assert_eq!(result, geo_distance_internal(&pt1, &pt2));
        assert!((result - 69.09318).abs() < 0.00001);
    }
}
