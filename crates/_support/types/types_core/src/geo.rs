#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    // Panics on a too-short image — a caller bug, as C would misread too.
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

    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&self.x.to_ne_bytes());
        out[8..16].copy_from_slice(&self.y.to_ne_bytes());
        out
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LSEG {
    pub p: [Point; 2],
}

impl LSEG {
    #[inline]
    pub fn from_datum_bytes(bytes: &[u8]) -> LSEG {
        LSEG {
            p: [
                Point::from_datum_bytes(&bytes[0..16]),
                Point::from_datum_bytes(&bytes[16..32]),
            ],
        }
    }

    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[0..16].copy_from_slice(&self.p[0].to_datum_bytes());
        out[16..32].copy_from_slice(&self.p[1].to_datum_bytes());
        out
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LINE {
    pub A: f64,
    pub B: f64,
    pub C: f64,
}

impl LINE {
    #[inline]
    pub fn from_datum_bytes(bytes: &[u8]) -> LINE {
        let f = |o: usize| {
            let mut a = [0u8; 8];
            a.copy_from_slice(&bytes[o..o + 8]);
            f64::from_ne_bytes(a)
        };
        LINE {
            A: f(0),
            B: f(8),
            C: f(16),
        }
    }

    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0..8].copy_from_slice(&self.A.to_ne_bytes());
        out[8..16].copy_from_slice(&self.B.to_ne_bytes());
        out[16..24].copy_from_slice(&self.C.to_ne_bytes());
        out
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct CIRCLE {
    pub center: Point,
    pub radius: f64,
}

impl CIRCLE {
    #[inline]
    pub fn from_datum_bytes(bytes: &[u8]) -> CIRCLE {
        let mut radius = [0u8; 8];
        radius.copy_from_slice(&bytes[16..24]);
        CIRCLE {
            center: Point::from_datum_bytes(&bytes[0..16]),
            radius: f64::from_ne_bytes(radius),
        }
    }

    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 24] {
        let mut out = [0u8; 24];
        out[0..16].copy_from_slice(&self.center.to_datum_bytes());
        out[16..24].copy_from_slice(&self.radius.to_ne_bytes());
        out
    }
}

// `offsetof(PATH, p)`: fixed header before the flexible `Point` array.
pub const PATH_HEADER_SIZE: usize = 16;

// `offsetof(POLYGON, p)`: fixed header before the flexible `Point` array.
pub const POLYGON_HEADER_SIZE: usize = 40;

// `high` = upper-right, `low` = lower-left; field order is the C image order.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct BOX {
    pub high: Point,
    pub low: Point,
}

impl BOX {
    #[inline]
    pub fn from_datum_bytes(bytes: &[u8]) -> BOX {
        BOX {
            high: Point::from_datum_bytes(&bytes[0..16]),
            low: Point::from_datum_bytes(&bytes[16..32]),
        }
    }

    #[inline]
    pub fn to_datum_bytes(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[0..16].copy_from_slice(&self.high.to_datum_bytes());
        out[16..32].copy_from_slice(&self.low.to_datum_bytes());
        out
    }
}

// The decoded SP-GiST ordering-scan key: leaf key = point, inner key = box.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SpgKey {
    LeafPoint(Point),
    InnerBox(BOX),
}

// gistproc.c:1575-1581 point_zorder_internal: Morton code over the two
// float32-narrowed coordinates (C's float4 parameters narrow the point's
// float8 members at the call).
pub fn point_zorder_internal(x: f32, y: f32) -> u64 {
    let ix = ieee_float32_to_uint32(x);
    let iy = ieee_float32_to_uint32(y);
    part_bits32_by2(ix) | (part_bits32_by2(iy) << 1)
}

// gistproc.c:1586-1596 part_bits32_by2.
fn part_bits32_by2(x: u32) -> u64 {
    let mut n = x as u64;
    n = (n | (n << 16)) & 0x0000FFFF0000FFFF;
    n = (n | (n << 8)) & 0x00FF00FF00FF00FF;
    n = (n | (n << 4)) & 0x0F0F0F0F0F0F0F0F;
    n = (n | (n << 2)) & 0x3333333333333333;
    n = (n | (n << 1)) & 0x5555555555555555;
    n
}

// gistproc.c:1603-1671 ieee_float32_to_uint32: order-preserving sign-magnitude
// map; negatives bit-flipped into 0-7FFFFFFE, positives/zeros ORed into
// 80000000-FFFFFFFE, every NaN to FFFFFFFF.
fn ieee_float32_to_uint32(f: f32) -> u32 {
    if f.is_nan() {
        return 0xFFFFFFFF;
    }
    let i = f.to_bits();
    if (i & 0x80000000) != 0 {
        i ^ 0xFFFFFFFF
    } else {
        i | 0x80000000
    }
}

// gistproc.c:1681-1701 gist_bbox_zorder_cmp over the leaf-key box's low
// corner; the leading float8 equality check is C's abbrev tie-break fast path.
pub fn gist_bbox_zorder_cmp(a: &BOX, b: &BOX) -> i32 {
    let (p1, p2) = (&a.low, &b.low);
    if p1.x == p2.x && p1.y == p2.y {
        return 0;
    }
    let z1 = point_zorder_internal(p1.x as f32, p1.y as f32);
    let z2 = point_zorder_internal(p2.x as f32, p2.y as f32);
    (z1 > z2) as i32 - (z1 < z2) as i32
}

#[cfg(test)]
mod zorder_tests {
    use super::*;

    fn pbox(x: f64, y: f64) -> BOX {
        let p = Point { x, y };
        BOX { high: p, low: p }
    }

    // Hand-computed interleavings: float32 map first (0.0 -> 0x80000000,
    // 1.0 -> 0xBF800000, -1.0 -> 0x407FFFFF, NaN -> 0xFFFFFFFF), then each
    // bit k lands at 2k (x) / 2k+1 (y).
    #[test]
    fn zorder_bit_exact() {
        assert_eq!(point_zorder_internal(0.0, 0.0), 0xC000000000000000);
        assert_eq!(point_zorder_internal(1.0, 1.0), 0xCFFFC00000000000);
        assert_eq!(point_zorder_internal(-1.0, 0.0), 0x9000155555555555);
        assert_eq!(point_zorder_internal(f32::NAN, f32::NAN), 0xFFFFFFFFFFFFFFFF);
        assert_eq!(part_bits32_by2(0xFFFFFFFF), 0x5555555555555555);
        assert_eq!(part_bits32_by2(0b11), 0b101);
        assert_eq!(ieee_float32_to_uint32(-0.0), 0x7FFFFFFF);
        assert_eq!(ieee_float32_to_uint32(0.0), 0x80000000);
        assert_eq!(ieee_float32_to_uint32(f32::NEG_INFINITY), 0x007FFFFF);
        assert_eq!(ieee_float32_to_uint32(f32::INFINITY), 0xFF800000);
    }

    #[test]
    fn bbox_cmp_ordering() {
        assert_eq!(gist_bbox_zorder_cmp(&pbox(0.0, 0.0), &pbox(0.0, 0.0)), 0);
        assert_eq!(gist_bbox_zorder_cmp(&pbox(1.0, 1.0), &pbox(0.0, 0.0)), 1);
        assert_eq!(gist_bbox_zorder_cmp(&pbox(-1.0, 0.0), &pbox(0.0, 0.0)), -1);
        // NaNs: float8 equality fails, z-values tie at all-ones.
        assert_eq!(gist_bbox_zorder_cmp(&pbox(f64::NAN, f64::NAN), &pbox(f64::NAN, f64::NAN)), 0);
        assert_eq!(gist_bbox_zorder_cmp(&pbox(f64::NAN, f64::NAN), &pbox(1e30, 1e30)), 1);
        // y's bits interleave above x's: (2,3) carries bit45, (3,2) bit44.
        assert_eq!(gist_bbox_zorder_cmp(&pbox(2.0, 3.0), &pbox(3.0, 2.0)), 1);
    }
}
