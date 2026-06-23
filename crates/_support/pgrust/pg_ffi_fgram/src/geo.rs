//! Geometric data types from `src/include/utils/geo_decls.h`.
//!
//! These are the ABI struct layouts used by the built-in geometric types
//! (`point`, `lseg`, `line`, `box`, `path`, `polygon`, `circle`).  The
//! fixed-size, pass-by-reference types (`Point`, `LSEG`, `LINE`, `BOX`,
//! `CIRCLE`) are plain `#[repr(C)]` value structs.
//!
//! `PATH` and `POLYGON` are toastable varlena types with a C
//! `FLEXIBLE_ARRAY_MEMBER` of points (`Point p[FLEXIBLE_ARRAY_MEMBER]`).  Only
//! their fixed *header* layout is expressed here (`PathHeader` / `PolygonHeader`)
//! so that `offsetof(PATH, p)` / `offsetof(POLYGON, p)` and `SET_VARSIZE`
//! computations are ABI-exact; the points are stored out of line by the safe
//! Rust crate.

// `float8` is C `double` (== Rust `f64`) and `int32` is C `int32` (== `i32`);
// the FFI crate has no aliases for these, so the primitive types are used
// directly to keep the ABI layout exact.

/// `Point` (geo_decls.h:96) -- a 2-D point `(x, y)`.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
#[repr(C)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

/// `LSEG` (geo_decls.h:106) -- a line segment specified by its two endpoints.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
#[repr(C)]
pub struct LSEG {
    pub p: [Point; 2],
}

/// `LINE` (geo_decls.h:128) -- the general line equation `Ax + By + C = 0`.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
#[repr(C)]
pub struct LINE {
    pub A: f64,
    pub B: f64,
    pub C: f64,
}

/// `BOX` (geo_decls.h:140) -- two opposite corners (sorted: `high` >= `low`).
#[derive(Copy, Clone, Debug, Default, PartialEq)]
#[repr(C)]
pub struct BOX {
    pub high: Point,
    pub low: Point,
}

/// `CIRCLE` (geo_decls.h:162) -- a center point and a radius.
#[derive(Copy, Clone, Debug, Default, PartialEq)]
#[repr(C)]
pub struct CIRCLE {
    pub center: Point,
    pub radius: f64,
}

/// Fixed header of the varlena `PATH` type (geo_decls.h:115).  The C struct is
///
/// ```c
/// typedef struct {
///     int32  vl_len_;   /* varlena header */
///     int32  npts;
///     int32  closed;    /* is this a closed polygon? */
///     int32  dummy;     /* padding to make it double align */
///     Point  p[FLEXIBLE_ARRAY_MEMBER];
/// } PATH;
/// ```
///
/// `offsetof(PATH, p)` therefore equals `size_of::<PathHeader>()` (16 bytes).
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct PathHeader {
    pub vl_len_: i32,
    pub npts: i32,
    pub closed: i32,
    pub dummy: i32,
}

/// `offsetof(PATH, p)` -- the size of the fixed PATH header, before the
/// flexible array of `Point`.
pub const PATH_HEADER_SIZE: usize = core::mem::size_of::<PathHeader>();

/// Fixed header of the varlena `POLYGON` type (geo_decls.h:151).  The C struct
/// is
///
/// ```c
/// typedef struct {
///     int32  vl_len_;   /* varlena header */
///     int32  npts;
///     BOX    boundbox;
///     Point  p[FLEXIBLE_ARRAY_MEMBER];
/// } POLYGON;
/// ```
///
/// `offsetof(POLYGON, p)` therefore equals `size_of::<PolygonHeader>()`
/// (40 bytes).
#[derive(Copy, Clone, Debug)]
#[repr(C)]
pub struct PolygonHeader {
    pub vl_len_: i32,
    pub npts: i32,
    pub boundbox: BOX,
}

/// `offsetof(POLYGON, p)` -- the size of the fixed POLYGON header, before the
/// flexible array of `Point`.
pub const POLYGON_HEADER_SIZE: usize = core::mem::size_of::<PolygonHeader>();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_layouts_match_c() {
        // float8 == double == 8 bytes.
        assert_eq!(core::mem::size_of::<Point>(), 16);
        assert_eq!(core::mem::size_of::<LSEG>(), 32);
        assert_eq!(core::mem::size_of::<LINE>(), 24);
        assert_eq!(core::mem::size_of::<BOX>(), 32);
        assert_eq!(core::mem::size_of::<CIRCLE>(), 24);

        // offsetof(PATH, p) == 16, offsetof(POLYGON, p) == 40.
        assert_eq!(PATH_HEADER_SIZE, 16);
        assert_eq!(POLYGON_HEADER_SIZE, 40);
    }
}
