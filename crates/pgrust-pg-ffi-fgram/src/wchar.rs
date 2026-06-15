use crate::encoding::{pg_enc, _PG_LAST_ENCODING_};
use crate::types::PgWChar;

pub type pg_wchar = PgWChar;
pub type utf_local_conversion_func = Option<unsafe extern "C" fn(u32) -> u32>;

pub type mb2wchar_with_len_converter = Option<
    unsafe fn(*const core::ffi::c_uchar, *mut pg_wchar, core::ffi::c_int) -> core::ffi::c_int,
>;
pub type wchar2mb_with_len_converter = Option<
    unsafe fn(*const pg_wchar, *mut core::ffi::c_uchar, core::ffi::c_int) -> core::ffi::c_int,
>;
pub type mblen_converter = Option<unsafe fn(*const core::ffi::c_uchar) -> core::ffi::c_int>;
pub type mbdisplaylen_converter = Option<unsafe fn(*const core::ffi::c_uchar) -> core::ffi::c_int>;
pub type mbchar_verifier =
    Option<unsafe fn(*const core::ffi::c_uchar, core::ffi::c_int) -> core::ffi::c_int>;
pub type mbstr_verifier =
    Option<unsafe fn(*const core::ffi::c_uchar, core::ffi::c_int) -> core::ffi::c_int>;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct pg_wchar_tbl {
    pub mb2wchar_with_len: mb2wchar_with_len_converter,
    pub wchar2mb_with_len: wchar2mb_with_len_converter,
    pub mblen: mblen_converter,
    pub dsplen: mbdisplaylen_converter,
    pub mbverifychar: mbchar_verifier,
    pub mbverifystr: mbstr_verifier,
    pub maxmblen: core::ffi::c_int,
}

impl pg_wchar_tbl {
    pub const fn maxmblen(&self) -> core::ffi::c_int {
        self.maxmblen
    }
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct mbinterval {
    pub first: core::ffi::c_uint,
    pub last: core::ffi::c_uint,
}

#[derive(Copy, Clone)]
#[repr(C)]
pub struct pg_mb_radix_tree {
    pub chars16: *const u16,
    pub chars32: *const u32,
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

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct pg_utf_to_local_combined {
    pub utf1: u32,
    pub utf2: u32,
    pub code: u32,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(C)]
pub struct pg_local_to_utf_combined {
    pub code: u32,
    pub utf1: u32,
    pub utf2: u32,
}

pub const MAX_MULTIBYTE_CHAR_LEN: usize = 4;
pub const SS2: u8 = 0x8e;
pub const SS3: u8 = 0x8f;
pub const LC_ISO8859_1: u8 = 0x81;
pub const LC_ISO8859_2: u8 = 0x82;
pub const LC_ISO8859_3: u8 = 0x83;
pub const LC_ISO8859_4: u8 = 0x84;
pub const LC_JISX0201K: u8 = 0x89;
pub const LC_JISX0201R: u8 = 0x8a;
pub const LC_KOI8_R: u8 = 0x8b;
pub const LC_ISO8859_5: u8 = 0x8c;
pub const LC_JISX0208_1978: u8 = 0x90;
pub const LC_GB2312_80: u8 = 0x91;
pub const LC_JISX0208: u8 = 0x92;
pub const LC_KS5601: u8 = 0x93;
pub const LC_JISX0212: u8 = 0x94;
pub const LC_CNS11643_1: u8 = 0x95;
pub const LC_CNS11643_2: u8 = 0x96;
pub const LCPRV2_B: u8 = 0x9d;
pub const LC_CNS11643_3: u8 = 0xf6;
pub const LC_CNS11643_4: u8 = 0xf7;
pub const LC_CNS11643_5: u8 = 0xf8;
pub const LC_CNS11643_6: u8 = 0xf9;
pub const LC_CNS11643_7: u8 = 0xfa;

pub const fn pg_wchar_table_len() -> usize {
    _PG_LAST_ENCODING_ as usize
}

pub const fn pg_wchar_table_index(encoding: pg_enc) -> usize {
    if encoding >= 0 && encoding < _PG_LAST_ENCODING_ {
        encoding as usize
    } else {
        0
    }
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
    /// Use this only when the caller has already guaranteed that `chars`
    /// contains a zero terminator.
    pub unsafe fn from_slice_unchecked(chars: &'a [PgWChar]) -> Self {
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
