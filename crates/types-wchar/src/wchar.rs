use alloc::vec::Vec;

pub use types_core::PgWChar;

pub type pg_wchar = PgWChar;

/// `LC_ISO8859_1` (`mb/pg_wchar.h`): the mule-internal lead-charset byte for
/// ISO-8859 Latin 1, used as the `lc` prefix in `latin2mic`/`mic2latin`.
pub const LC_ISO8859_1: u8 = 0x81;

/// `LC_ISO8859_2` (`mb/pg_wchar.h`): the mule-internal lead-charset byte for
/// ISO-8859 Latin 2.
pub const LC_ISO8859_2: u8 = 0x82;

/// `LC_ISO8859_3` (`mb/pg_wchar.h`): the mule-internal lead-charset byte for
/// ISO-8859 Latin 3.
pub const LC_ISO8859_3: u8 = 0x83;

/// `LC_ISO8859_4` (`mb/pg_wchar.h`): the mule-internal lead-charset byte for
/// ISO-8859 Latin 4.
pub const LC_ISO8859_4: u8 = 0x84;

/// `LC_KOI8_R` (`mb/pg_wchar.h`): the mule-internal lead-charset byte for
/// Cyrillic KOI8-R (also the mule-internal charset for Cyrillic).
pub const LC_KOI8_R: u8 = 0x8b;

/// `LC_ISO8859_5` (`mb/pg_wchar.h`): the mule-internal lead-charset byte for
/// ISO-8859 Cyrillic.
pub const LC_ISO8859_5: u8 = 0x8c;

/// A range of Unicode code points (`struct mbinterval` in `wchar.c`), used by
/// the display-width tables consulted by `mbbisearch`/`ucs_wcwidth`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct mbinterval {
    pub first: u32,
    pub last: u32,
}

/// `pg_mb_radix_tree` (`mb/pg_wchar.h`): the radix-tree driver structure used
/// by the generated UTF-8 <-> local conversion maps. In C the `chars16`/
/// `chars32` arrays are raw `const uint16 */const uint32 *` blobs; here they are
/// owned `Vec`s (only one is non-empty, matching C's "only one of chars16 or
/// chars32 is used"). The remaining fields are the per-byte-count root offsets
/// and the inclusive [lower, upper] byte bounds, field-for-field with the C
/// struct.
#[derive(Clone, Debug)]
pub struct pg_mb_radix_tree {
    pub chars16: Vec<u16>,
    pub chars32: Vec<u32>,

    pub b1root: u32,
    pub b1_lower: u8,
    pub b1_upper: u8,

    pub b2root: u32,
    pub b2_1_lower: u8,
    pub b2_1_upper: u8,
    pub b2_2_lower: u8,
    pub b2_2_upper: u8,

    pub b3root: u32,
    pub b3_1_lower: u8,
    pub b3_1_upper: u8,
    pub b3_2_lower: u8,
    pub b3_2_upper: u8,
    pub b3_3_lower: u8,
    pub b3_3_upper: u8,

    pub b4root: u32,
    pub b4_1_lower: u8,
    pub b4_1_upper: u8,
    pub b4_2_lower: u8,
    pub b4_2_upper: u8,
    pub b4_3_lower: u8,
    pub b4_3_upper: u8,
    pub b4_4_lower: u8,
    pub b4_4_upper: u8,
}

/// `pg_utf_to_local_combined` (`mb/pg_wchar.h`): UTF-8 -> local conversion map
/// entry for combined characters (sorted by `(utf1, utf2)`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct pg_utf_to_local_combined {
    pub utf1: u32,
    pub utf2: u32,
    pub code: u32,
}

/// `pg_local_to_utf_combined` (`mb/pg_wchar.h`): local -> UTF-8 conversion map
/// entry for combined characters (sorted by `code`).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct pg_local_to_utf_combined {
    pub code: u32,
    pub utf1: u32,
    pub utf2: u32,
}

/// Null-terminated PostgreSQL wide-character string.
#[derive(Debug, Eq, PartialEq)]
pub struct PgWCharStr<'a> {
    chars: &'a [PgWChar],
}

impl<'a> PgWCharStr<'a> {
    /// Creates a `PgWCharStr` when `chars` contains a terminating zero.
    pub fn from_slice(chars: &'a [PgWChar]) -> Option<Self> {
        chars.contains(&0).then_some(Self { chars })
    }

    /// Creates a `PgWCharStr` without checking for a terminating zero.
    ///
    /// Logical precondition (not a memory-safety one, so this is a safe fn):
    /// `chars` should contain a zero terminator. If it does not, [`len`]
    /// falls back to the full slice length.
    ///
    /// [`len`]: Self::len
    pub fn from_slice_unchecked(chars: &'a [PgWChar]) -> Self {
        Self { chars }
    }

    pub fn as_slice_with_nul(&self) -> &'a [PgWChar] {
        self.chars
    }

    pub fn len(&self) -> usize {
        self.chars
            .iter()
            .position(|&wchar| wchar == 0)
            .unwrap_or(self.chars.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
