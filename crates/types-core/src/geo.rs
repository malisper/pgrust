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

impl Point {
    /// `DatumGetPointP(datum)` analogue: decode a `point`'s by-reference image
    /// (`struct Point { float8 x, y; }`, native byte order, 16 bytes) into the
    /// owned value. Mirrors `palloc`'d `Point *` the C opclass entry points get.
    ///
    /// Panics if `bytes` is shorter than `sizeof(Point)` — a caller bug, exactly
    /// as the C code would misread a too-short image.
    #[inline]
    pub fn from_datum_bytes(bytes: &[u8]) -> Point {
        let mut x = [0u8; 8];
        let mut y = [0u8; 8];
        x.copy_from_slice(&bytes[0..8]);
        y.copy_from_slice(&bytes[8..16]);
        Point {
            x: f64::from_ne_bytes(x),
            y: f64::from_ne_bytes(y),
        }
    }

    /// `PointPGetDatum(p)` analogue: serialize this point to its by-reference
    /// on-disk image (`struct Point`, native byte order, 16 bytes). The caller
    /// wraps the bytes in a `Datum::ByRef`.
    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&self.x.to_ne_bytes());
        out[8..16].copy_from_slice(&self.y.to_ne_bytes());
        out
    }
}

/// `BOX` (geo_decls.h) -- a rectangle given by two corner points, sorted so
/// that `high` holds the upper-right and `low` the lower-left corner.  Field
/// order matches the C struct (`high`, then `low`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BOX {
    pub high: Point,
    pub low: Point,
}

impl BOX {
    /// `DatumGetBoxP(datum)` analogue: decode a `box`'s by-reference image
    /// (`struct BOX { Point high; Point low; }`, native byte order, 32 bytes).
    ///
    /// Panics on a too-short image — a caller bug, as C would misread too.
    #[inline]
    pub fn from_datum_bytes(bytes: &[u8]) -> BOX {
        BOX {
            high: Point::from_datum_bytes(&bytes[0..16]),
            low: Point::from_datum_bytes(&bytes[16..32]),
        }
    }

    /// `BoxPGetDatum(b)` analogue: serialize this box to its by-reference
    /// on-disk image (`struct BOX`, native byte order, 32 bytes).
    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[0..16].copy_from_slice(&self.high.to_datum_bytes());
        out[16..32].copy_from_slice(&self.low.to_datum_bytes());
        out
    }
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
