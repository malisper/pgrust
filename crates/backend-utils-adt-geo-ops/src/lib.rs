//! `utils/adt/geo_ops.c` (partial) -- the geometric box/point predicate
//! operators used by the `box`/`point` GiST and SP-GiST opclasses.
//!
//! This crate owns and installs the seams declared in
//! `backend-utils-adt-geo-ops-seams`: the fuzzy float comparators
//! (`geo_decls.h` `FPlt`/`FPle`/`FPgt`/`FPge`), `pg_hypot`/`HYPOT`, the
//! box<->box and point<->point boolean operators, and `box_contain_pt`.
//!
//! Only the predicate subset of `geo_ops.c` is ported here -- the input/output
//! parsers, distance/area functions and the remaining geometric types land
//! when their consumers are ported (mirror-PG-and-panic until then). The
//! constructors (`box_construct`, etc.) are likewise out of scope; the
//! predicates do not use them.

#![allow(non_snake_case)]

use types_core::geo::{Point, BOX};
use types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

// ---------------------------------------------------------------------------
// Fuzzy float comparators (geo_decls.h, the EPSILON-defined branch).
// ---------------------------------------------------------------------------

/// `EPSILON` (geo_decls.h): the tolerance for the fuzzy float comparators.
const EPSILON: f64 = 1.0e-06;

/// `FPeq(A, B)` (geo_decls.h): fuzzy `A == B`.
#[inline]
fn FPeq(a: f64, b: f64) -> bool {
    a == b || (a - b).abs() <= EPSILON
}

/// `FPlt(A, B)` (geo_decls.h): fuzzy `A < B`.
#[inline]
fn FPlt(a: f64, b: f64) -> bool {
    a + EPSILON < b
}

/// `FPle(A, B)` (geo_decls.h): fuzzy `A <= B`.
#[inline]
fn FPle(a: f64, b: f64) -> bool {
    a <= b + EPSILON
}

/// `FPgt(A, B)` (geo_decls.h): fuzzy `A > B`.
#[inline]
fn FPgt(a: f64, b: f64) -> bool {
    a > b + EPSILON
}

/// `FPge(A, B)` (geo_decls.h): fuzzy `A >= B`.
#[inline]
fn FPge(a: f64, b: f64) -> bool {
    a + EPSILON >= b
}

// ---------------------------------------------------------------------------
// pg_hypot (geo_ops.c).
// ---------------------------------------------------------------------------

/// `float_overflow_error()` (float.c): `ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
/// "value out of range: overflow")`.
fn float_overflow_error() -> PgError {
    PgError::error("value out of range: overflow").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `float_underflow_error()` (float.c): `ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
/// "value out of range: underflow")`.
fn float_underflow_error() -> PgError {
    PgError::error("value out of range: underflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `pg_hypot(x, y)` (geo_ops.c:5519): `sqrt(x*x + y*y)` computed so as to avoid
/// intermediate over/underflow, with IEEE INF/NaN handling.
pub fn pg_hypot(mut x: f64, mut y: f64) -> PgResult<f64> {
    // Handle INF and NaN properly
    if x.is_infinite() || y.is_infinite() {
        // get_float8_infinity()
        return Ok(f64::INFINITY);
    }

    if x.is_nan() || y.is_nan() {
        // get_float8_nan()
        return Ok(f64::NAN);
    }

    // Else, drop any minus signs
    x = x.abs();
    y = y.abs();

    // Swap x and y if needed to make x the larger one
    if x < y {
        let temp = x;
        x = y;
        y = temp;
    }

    // If y is zero, the hypotenuse is x.  This test saves a few cycles in such
    // cases, but more importantly it also protects against divide-by-zero
    // errors, since now x >= y.
    if y == 0.0 {
        return Ok(x);
    }

    // Determine the hypotenuse
    let yx = y / x;
    let result = x * (1.0 + (yx * yx)).sqrt();

    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 {
        return Err(float_underflow_error());
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Point equality helper (geo_ops.c:1976).
// ---------------------------------------------------------------------------

/// `point_eq_point(pt1, pt2)` (geo_ops.c:1976): are the two points the same?
fn point_eq_point(pt1: &Point, pt2: &Point) -> bool {
    // If any NaNs are involved, insist on exact equality.  C uses float8_eq,
    // which is plain IEEE `==` (so NaN != NaN), exactly mirrored here.
    if pt1.x.is_nan() || pt1.y.is_nan() || pt2.x.is_nan() || pt2.y.is_nan() {
        return pt1.x == pt2.x && pt1.y == pt2.y;
    }

    FPeq(pt1.x, pt2.x) && FPeq(pt1.y, pt2.y)
}

// ---------------------------------------------------------------------------
// box <-> box boolean operators (geo_ops.c).
// ---------------------------------------------------------------------------

/// `box_ov(box1, box2)` (geo_ops.c:571): do the boxes overlap?
fn box_ov(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.low.x, box2.high.x)
        && FPle(box2.low.x, box1.high.x)
        && FPle(box1.low.y, box2.high.y)
        && FPle(box2.low.y, box1.high.y)
}

/// `box_contain_box(contains_box, contained_box)` (geo_ops.c:703): is the
/// second box in the first box or on its border?
fn box_contain_box(contains_box: &BOX, contained_box: &BOX) -> bool {
    FPge(contains_box.high.x, contained_box.high.x)
        && FPle(contains_box.low.x, contained_box.low.x)
        && FPge(contains_box.high.y, contained_box.high.y)
        && FPle(contains_box.low.y, contained_box.low.y)
}

/// `box_same(box1, box2)` (geo_ops.c:551): are two boxes identical?
pub fn box_same(box1: &BOX, box2: &BOX) -> bool {
    point_eq_point(&box1.high, &box2.high) && point_eq_point(&box1.low, &box2.low)
}

/// `box_overlap(box1, box2)` (geo_ops.c:563): does box1 overlap box2?
pub fn box_overlap(box1: &BOX, box2: &BOX) -> bool {
    box_ov(box1, box2)
}

/// `box_left(box1, box2)` (geo_ops.c:583): is box1 strictly left of box2?
pub fn box_left(box1: &BOX, box2: &BOX) -> bool {
    FPlt(box1.high.x, box2.low.x)
}

/// `box_overleft(box1, box2)` (geo_ops.c:598): is the right edge of box1 at or
/// left of the right edge of box2?
pub fn box_overleft(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.x, box2.high.x)
}

/// `box_right(box1, box2)` (geo_ops.c:609): is box1 strictly right of box2?
pub fn box_right(box1: &BOX, box2: &BOX) -> bool {
    FPgt(box1.low.x, box2.high.x)
}

/// `box_overright(box1, box2)` (geo_ops.c:624): is the left edge of box1 at or
/// right of the left edge of box2?
pub fn box_overright(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.x, box2.low.x)
}

/// `box_below(box1, box2)` (geo_ops.c:635): is box1 strictly below box2?
pub fn box_below(box1: &BOX, box2: &BOX) -> bool {
    FPlt(box1.high.y, box2.low.y)
}

/// `box_overbelow(box1, box2)` (geo_ops.c:647): is the upper edge of box1 at or
/// below the upper edge of box2?
pub fn box_overbelow(box1: &BOX, box2: &BOX) -> bool {
    FPle(box1.high.y, box2.high.y)
}

/// `box_above(box1, box2)` (geo_ops.c:658): is box1 strictly above box2?
pub fn box_above(box1: &BOX, box2: &BOX) -> bool {
    FPgt(box1.low.y, box2.high.y)
}

/// `box_overabove(box1, box2)` (geo_ops.c:670): is the lower edge of box1 at or
/// above the lower edge of box2?
pub fn box_overabove(box1: &BOX, box2: &BOX) -> bool {
    FPge(box1.low.y, box2.low.y)
}

/// `box_contained(box1, box2)` (geo_ops.c:681): is box1 contained by box2?
pub fn box_contained(box1: &BOX, box2: &BOX) -> bool {
    box_contain_box(box2, box1)
}

/// `box_contain(box1, box2)` (geo_ops.c:692): does box1 contain box2?
pub fn box_contain(box1: &BOX, box2: &BOX) -> bool {
    box_contain_box(box1, box2)
}

// ---------------------------------------------------------------------------
// point <-> point boolean operators (geo_ops.c).
// ---------------------------------------------------------------------------

/// `point_left(pt1, pt2)` (geo_ops.c:1901).
pub fn point_left(pt1: &Point, pt2: &Point) -> bool {
    FPlt(pt1.x, pt2.x)
}

/// `point_right(pt1, pt2)` (geo_ops.c:1910).
pub fn point_right(pt1: &Point, pt2: &Point) -> bool {
    FPgt(pt1.x, pt2.x)
}

/// `point_above(pt1, pt2)` (geo_ops.c:1919).
pub fn point_above(pt1: &Point, pt2: &Point) -> bool {
    FPgt(pt1.y, pt2.y)
}

/// `point_below(pt1, pt2)` (geo_ops.c:1928).
pub fn point_below(pt1: &Point, pt2: &Point) -> bool {
    FPlt(pt1.y, pt2.y)
}

/// `point_vert(pt1, pt2)` (geo_ops.c:1937): vertically aligned (same x)?
pub fn point_vert(pt1: &Point, pt2: &Point) -> bool {
    FPeq(pt1.x, pt2.x)
}

/// `point_horiz(pt1, pt2)` (geo_ops.c:1946): horizontally aligned (same y)?
pub fn point_horiz(pt1: &Point, pt2: &Point) -> bool {
    FPeq(pt1.y, pt2.y)
}

/// `point_eq(pt1, pt2)` (geo_ops.c:1955).
pub fn point_eq(pt1: &Point, pt2: &Point) -> bool {
    point_eq_point(pt1, pt2)
}

// ---------------------------------------------------------------------------
// box <-> point containment (geo_ops.c).
// ---------------------------------------------------------------------------

/// `box_contain_point(box, point)` (geo_ops.c:3130): does the box contain the
/// point?  Note: uses exact (`>=`/`<=`), not fuzzy, comparisons -- exactly as
/// the C code does.
fn box_contain_point(b: &BOX, point: &Point) -> bool {
    b.high.x >= point.x
        && b.low.x <= point.x
        && b.high.y >= point.y
        && b.low.y <= point.y
}

/// `box_contain_pt(box, point)` (geo_ops.c:3146).
pub fn box_contain_pt(b: &BOX, p: &Point) -> bool {
    box_contain_point(b, p)
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install every seam owned by this crate.
pub fn init_seams() {
    use backend_utils_adt_geo_ops_seams as seams;

    seams::FPlt::set(FPlt);
    seams::FPle::set(FPle);
    seams::FPgt::set(FPgt);
    seams::FPge::set(FPge);

    seams::HYPOT::set(pg_hypot);

    seams::box_overlap::set(box_overlap);
    seams::box_contain::set(box_contain);
    seams::box_contained::set(box_contained);
    seams::box_same::set(box_same);
    seams::box_left::set(box_left);
    seams::box_overleft::set(box_overleft);
    seams::box_right::set(box_right);
    seams::box_overright::set(box_overright);
    seams::box_above::set(box_above);
    seams::box_overabove::set(box_overabove);
    seams::box_below::set(box_below);
    seams::box_overbelow::set(box_overbelow);

    seams::point_left::set(point_left);
    seams::point_right::set(point_right);
    seams::point_above::set(point_above);
    seams::point_below::set(point_below);
    seams::point_horiz::set(point_horiz);
    seams::point_vert::set(point_vert);
    seams::point_eq::set(point_eq);

    seams::box_contain_pt::set(box_contain_pt);
}
