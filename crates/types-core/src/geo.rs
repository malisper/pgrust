//! Geometric data types from `src/include/utils/geo_decls.h`.
//!
//! Only the items consumed by ported crates are present; the remaining
//! geometric types land when their consumers are ported.

/// `Point` (geo_decls.h) -- a 2-D point `(x, y)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}
