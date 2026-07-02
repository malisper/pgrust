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

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LINE {
    pub A: f64,
    pub B: f64,
    pub C: f64,
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
