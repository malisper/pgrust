//! Geometric data types from `src/include/utils/geo_decls.h`.
//!
//! Only the items consumed by ported crates are present; the remaining
//! geometric types land when their consumers are ported.

/// `Point` (geo_decls.h) -- a 2-D point `(x, y)`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

/// `BOX` (geo_decls.h) -- a rectangle given by two corner points, sorted so
/// that `high` holds the upper-right and `low` the lower-left corner.  Field
/// order matches the C struct (`high`, then `low`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BOX {
    pub high: Point,
    pub low: Point,
}

/// The SP-GiST ordering-scan key value passed to `spg_key_orderbys_distances`
/// (spgproc.c): the C function takes a `Datum key` plus a `bool isLeaf`; a leaf
/// key is a `point`, a non-leaf (inner) key is a `box`.  This enum carries the
/// decoded key, replacing the `Datum`/`bool` pair.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SpgKey {
    /// `isLeaf == true`: `DatumGetPointP(key)`.
    LeafPoint(Point),
    /// `isLeaf == false`: `DatumGetBoxP(key)`.
    InnerBox(BOX),
}
