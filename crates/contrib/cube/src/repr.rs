//! contrib/cube NDBOX on-disk representation (cubedata.h).
//!
//! The stored image is byte-for-byte C's NDBOX:
//!
//! ```text
//! [ int32 vl_len_ | u32 header | f64 x[dim] (ll) | f64 x[dim] (ur, unless point) ]
//! ```
//!
//! header bits 0-30 = dimension count, bit 31 = point flag. When the point
//! flag is set the upper-right coordinates are NOT stored: they equal the
//! lower-left ones (POINT_SIZE vs CUBE_SIZE). All reads/writes go through
//! byte copies (`from_ne_bytes`) so alignment of the source image is
//! irrelevant.

pub const CUBE_MAX_DIM: usize = 100;
pub const POINT_BIT: u32 = 0x8000_0000;
pub const DIM_MASK: u32 = 0x7fff_ffff;

/// `offsetof(NDBOX, x)` = 8 (4-byte varlena header + 4-byte header word).
pub const NDBOX_HDR: usize = 8;

pub const fn point_size(dim: usize) -> usize {
    NDBOX_HDR + 8 * dim
}
pub const fn cube_size(dim: usize) -> usize {
    NDBOX_HDR + 16 * dim
}

/// C's `Min`/`Max` macros on doubles: `(a < b) ? a : b`. NOT `f64::min` —
/// the C macros return the SECOND operand when the comparison is false
/// (including any NaN comparison), and cube's semantics depend on it.
#[inline]
pub fn c_min(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}
#[inline]
pub fn c_max(a: f64, b: f64) -> f64 {
    if a > b {
        a
    } else {
        b
    }
}

/// Read-only view over a cube's PAYLOAD bytes (the image minus its 4-byte
/// varlena header): `[ u32 header | f64 coords... ]`.
#[derive(Clone, Copy)]
pub struct CubeView<'a> {
    payload: &'a [u8],
}

impl<'a> CubeView<'a> {
    pub fn from_payload(payload: &'a [u8]) -> Self {
        CubeView { payload }
    }

    #[inline]
    pub fn header(&self) -> u32 {
        u32::from_ne_bytes(self.payload[0..4].try_into().unwrap())
    }

    #[inline]
    pub fn dim(&self) -> usize {
        (self.header() & DIM_MASK) as usize
    }

    #[inline]
    pub fn is_point(&self) -> bool {
        self.header() & POINT_BIT != 0
    }

    /// Raw coordinate slot `x[i]` (cubedata.h `(cube)->x[i]`).
    #[inline]
    pub fn x(&self, i: usize) -> f64 {
        let off = 4 + 8 * i;
        f64::from_ne_bytes(self.payload[off..off + 8].try_into().unwrap())
    }

    /// `LL_COORD(cube, i)`.
    #[inline]
    pub fn ll(&self, i: usize) -> f64 {
        self.x(i)
    }

    /// `UR_COORD(cube, i)`.
    #[inline]
    pub fn ur(&self, i: usize) -> f64 {
        if self.is_point() {
            self.x(i)
        } else {
            self.x(i + self.dim())
        }
    }

    /// `cube_is_point_internal`: point flag, or ll==ur on every dimension
    /// (pre-9.4 on-disk images may lack the flag).
    pub fn is_point_internal(&self) -> bool {
        if self.is_point() {
            return true;
        }
        for i in 0..self.dim() {
            if self.ll(i) != self.ur(i) {
                return false;
            }
        }
        true
    }
}

/// Build a full cube image (WITH the 4-byte varlena header) from raw slots.
/// `coords` carries `dim` values for a point, `2*dim` otherwise.
pub fn build_image(dim: usize, point: bool, coords: &[f64]) -> Vec<u8> {
    debug_assert_eq!(coords.len(), if point { dim } else { 2 * dim });
    let size = if point { point_size(dim) } else { cube_size(dim) };
    let mut img = Vec::with_capacity(size);
    img.extend_from_slice(&datum::varlena::set_varsize_4b(size));
    let header = (dim as u32 & DIM_MASK) | if point { POINT_BIT } else { 0 };
    img.extend_from_slice(&header.to_ne_bytes());
    for &c in coords {
        img.extend_from_slice(&c.to_ne_bytes());
    }
    img
}

/// Shrink a full (ll+ur) coordinate set to a point image when every ur
/// equals its ll — C's "resize the cube we constructed" paths, which just
/// flip the flag and cut VARSIZE without rewriting coordinates.
pub fn build_image_auto(dim: usize, coords: &[f64]) -> Vec<u8> {
    debug_assert_eq!(coords.len(), 2 * dim);
    let mut point = true;
    for i in 0..dim {
        if coords[i] != coords[i + dim] {
            point = false;
            break;
        }
    }
    if point {
        build_image(dim, true, &coords[..dim])
    } else {
        build_image(dim, false, coords)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_image_layout_matches_c() {
        // 2-D point (1,2): size = 8 + 16 = 24, header = 2 | POINT_BIT.
        let img = build_image(2, true, &[1.0, 2.0]);
        assert_eq!(img.len(), 24);
        let v = CubeView::from_payload(&img[4..]);
        assert!(v.is_point());
        assert_eq!(v.dim(), 2);
        assert_eq!(v.ll(0), 1.0);
        assert_eq!(v.ur(0), 1.0);
        assert_eq!(v.ll(1), 2.0);
        assert_eq!(v.ur(1), 2.0);
        // varlena word: size << 2 on LE builds.
        assert_eq!(
            u32::from_ne_bytes(img[0..4].try_into().unwrap()) >> 2,
            24
        );
        assert_eq!(
            u32::from_ne_bytes(img[4..8].try_into().unwrap()),
            2 | POINT_BIT
        );
    }

    #[test]
    fn box_image_layout_matches_c() {
        let img = build_image(2, false, &[0.0, 0.0, 1.0, 1.0]);
        assert_eq!(img.len(), 40); // 8 + 2*2*8
        let v = CubeView::from_payload(&img[4..]);
        assert!(!v.is_point());
        assert!(!v.is_point_internal());
        assert_eq!(v.dim(), 2);
        assert_eq!(v.ur(1), 1.0);
    }

    #[test]
    fn auto_collapses_to_point() {
        let img = build_image_auto(2, &[3.0, 4.0, 3.0, 4.0]);
        assert_eq!(img.len(), 24);
        assert!(CubeView::from_payload(&img[4..]).is_point());
        // NaN != NaN keeps the box form (C: point &= (x == y)).
        let img = build_image_auto(1, &[f64::NAN, f64::NAN]);
        assert_eq!(img.len(), 24); // 8 + 2*8
        assert!(!CubeView::from_payload(&img[4..]).is_point());
    }

    #[test]
    fn c_min_max_nan_semantics() {
        // C: Min(a,b) = (a<b) ? a : b — NaN comparisons are false, so the
        // SECOND operand wins.
        assert!(c_min(f64::NAN, 1.0) == 1.0);
        assert!(c_max(f64::NAN, 1.0) == 1.0);
        assert!(c_min(1.0, f64::NAN).is_nan());
        assert!(c_max(1.0, f64::NAN).is_nan());
    }
}
