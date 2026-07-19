//! contrib/seg SEG on-disk representation (segdata.h): a fixed-length
//! 12-byte pass-by-reference struct.
//!
//! ```text
//! [ f32 lower | f32 upper | u8 l_sigd | u8 u_sigd | u8 l_ext | u8 u_ext ]
//! ```
//!
//! `*_sigd` = number of significant digits captured at input time (drives
//! the display precision), `*_ext` = boundary extension: 0 exact, `'-'`
//! open (±infinity), `'<'`/`'>'`/`'~'` inexactness markers.

pub const SEG_SIZE: usize = 12;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Seg {
    pub lower: f32,
    pub upper: f32,
    pub l_sigd: u8,
    pub u_sigd: u8,
    pub l_ext: u8,
    pub u_ext: u8,
}

impl Seg {
    pub fn from_bytes(b: &[u8]) -> Self {
        Seg {
            lower: f32::from_ne_bytes(b[0..4].try_into().unwrap()),
            upper: f32::from_ne_bytes(b[4..8].try_into().unwrap()),
            l_sigd: b[8],
            u_sigd: b[9],
            l_ext: b[10],
            u_ext: b[11],
        }
    }

    pub fn to_bytes(&self) -> [u8; SEG_SIZE] {
        let mut out = [0u8; SEG_SIZE];
        out[0..4].copy_from_slice(&self.lower.to_ne_bytes());
        out[4..8].copy_from_slice(&self.upper.to_ne_bytes());
        out[8] = self.l_sigd;
        out[9] = self.u_sigd;
        out[10] = self.l_ext;
        out[11] = self.u_ext;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twelve_byte_roundtrip() {
        let s = Seg {
            lower: 1.5,
            upper: 2.5,
            l_sigd: 2,
            u_sigd: 3,
            l_ext: b'<',
            u_ext: 0,
        };
        let b = s.to_bytes();
        assert_eq!(b.len(), 12);
        assert_eq!(Seg::from_bytes(&b), s);
        // field offsets match C's struct layout
        assert_eq!(f32::from_ne_bytes(b[0..4].try_into().unwrap()), 1.5);
        assert_eq!(b[10], b'<');
    }
}
