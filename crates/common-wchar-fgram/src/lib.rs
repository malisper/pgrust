#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(
    dead_code,
    improper_ctypes_definitions,
    improper_ctypes,
    unused_imports,
    unused_assignments,
    unused_mut,
    unused_unsafe,
    unused_variables,
    clippy::all,
    clippy::pedantic
)]

#[cfg(not(target_family = "wasm"))]
use libc::{memchr, strnlen};

// wasm-only stubs: `libc::memchr` and `libc::strnlen` are absent on the
// wasm64-unknown-unknown target. These re-implement the C-ABI symbols with
// identical signatures and semantics so the call sites compile and behave
// identically.
#[cfg(target_family = "wasm")]
#[inline]
unsafe extern "C" fn memchr(
    s: *const ::core::ffi::c_void,
    c: ::core::ffi::c_int,
    n: size_t,
) -> *mut ::core::ffi::c_void {
    let needle = c as u8;
    let bytes = s as *const u8;
    let mut i: size_t = 0;
    while i < n {
        if *bytes.add(i) == needle {
            return bytes.add(i) as *mut ::core::ffi::c_void;
        }
        i += 1;
    }
    ::core::ptr::null_mut()
}

#[cfg(target_family = "wasm")]
#[inline]
unsafe extern "C" fn strnlen(s: *const ::core::ffi::c_char, maxlen: size_t) -> size_t {
    let bytes = s as *const u8;
    let mut i: size_t = 0;
    while i < maxlen {
        if *bytes.add(i) == 0 {
            return i;
        }
        i += 1;
    }
    maxlen
}
pub use pgrust_pg_ffi::{
    mb2wchar_with_len_converter, mbchar_verifier, mbdisplaylen_converter, mbinterval,
    mblen_converter, mbstr_verifier, pg_enc, pg_wchar, pg_wchar_tbl, wchar2mb_with_len_converter,
    PG_BIG5, PG_EUC_CN, PG_EUC_JIS_2004, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK,
    PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_JOHAB, PG_KOI8R, PG_KOI8U,
    PG_LATIN1, PG_LATIN10, PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7,
    PG_LATIN8, PG_LATIN9, PG_MULE_INTERNAL, PG_SHIFT_JIS_2004, PG_SJIS, PG_SQL_ASCII, PG_UHC,
    PG_UTF8, PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255, PG_WIN1256,
    PG_WIN1257, PG_WIN1258, PG_WIN866, PG_WIN874, _PG_LAST_ENCODING_,
};
pub type __darwin_size_t = usize;
pub type uint8_t = u8;
pub type uint32_t = u32;
pub type size_t = __darwin_size_t;
pub type uint8 = uint8_t;
pub type uint32 = uint32_t;
pub type Vector8 = [uint8; 16];
pub const NULL: *mut ::core::ffi::c_void = __DARWIN_NULL;
pub const __DARWIN_NULL: *mut ::core::ffi::c_void = ::core::ptr::null_mut::<::core::ffi::c_void>();
pub const true_0: ::core::ffi::c_int = 1 as ::core::ffi::c_int;
pub const false_0: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
pub const INT_MAX: ::core::ffi::c_int = 2147483647 as ::core::ffi::c_int;
pub const HIGHBIT: ::core::ffi::c_int = 0x80 as ::core::ffi::c_int;
pub const SS2: ::core::ffi::c_int = 0x8e as ::core::ffi::c_int;
pub const SS3: ::core::ffi::c_int = 0x8f as ::core::ffi::c_int;
pub const LCPRV1_A: ::core::ffi::c_int = 0x9a as ::core::ffi::c_int;
pub const LCPRV1_B: ::core::ffi::c_int = 0x9b as ::core::ffi::c_int;
pub const LCPRV2_A: ::core::ffi::c_int = 0x9c as ::core::ffi::c_int;
pub const LCPRV2_B: ::core::ffi::c_int = 0x9d as ::core::ffi::c_int;
#[inline]
unsafe fn utf8_to_unicode(mut c: *const ::core::ffi::c_uchar) -> pg_wchar {
    if *c as ::core::ffi::c_int & 0x80 as ::core::ffi::c_int == 0 as ::core::ffi::c_int {
        return *c.offset(0 as ::core::ffi::c_int as isize) as pg_wchar;
    } else if *c as ::core::ffi::c_int & 0xe0 as ::core::ffi::c_int == 0xc0 as ::core::ffi::c_int {
        return ((*c.offset(0 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            & 0x1f as ::core::ffi::c_int)
            << 6 as ::core::ffi::c_int
            | *c.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                & 0x3f as ::core::ffi::c_int) as pg_wchar;
    } else if *c as ::core::ffi::c_int & 0xf0 as ::core::ffi::c_int == 0xe0 as ::core::ffi::c_int {
        return ((*c.offset(0 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            & 0xf as ::core::ffi::c_int)
            << 12 as ::core::ffi::c_int
            | (*c.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                & 0x3f as ::core::ffi::c_int)
                << 6 as ::core::ffi::c_int
            | *c.offset(2 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                & 0x3f as ::core::ffi::c_int) as pg_wchar;
    } else if *c as ::core::ffi::c_int & 0xf8 as ::core::ffi::c_int == 0xf0 as ::core::ffi::c_int {
        return ((*c.offset(0 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            & 0x7 as ::core::ffi::c_int)
            << 18 as ::core::ffi::c_int
            | (*c.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                & 0x3f as ::core::ffi::c_int)
                << 12 as ::core::ffi::c_int
            | (*c.offset(2 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                & 0x3f as ::core::ffi::c_int)
                << 6 as ::core::ffi::c_int
            | *c.offset(3 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                & 0x3f as ::core::ffi::c_int) as pg_wchar;
    } else {
        return 0xffffffff as pg_wchar;
    };
}
#[inline]
unsafe fn unicode_to_utf8(
    mut c: pg_wchar,
    mut utf8string: *mut ::core::ffi::c_uchar,
) -> *mut ::core::ffi::c_uchar {
    if c <= 0x7f as pg_wchar {
        *utf8string.offset(0 as ::core::ffi::c_int as isize) = c as ::core::ffi::c_uchar;
    } else if c <= 0x7ff as pg_wchar {
        *utf8string.offset(0 as ::core::ffi::c_int as isize) = (0xc0 as pg_wchar
            | c >> 6 as ::core::ffi::c_int & 0x1f as pg_wchar)
            as ::core::ffi::c_uchar;
        *utf8string.offset(1 as ::core::ffi::c_int as isize) =
            (0x80 as pg_wchar | c & 0x3f as pg_wchar) as ::core::ffi::c_uchar;
    } else if c <= 0xffff as pg_wchar {
        *utf8string.offset(0 as ::core::ffi::c_int as isize) = (0xe0 as pg_wchar
            | c >> 12 as ::core::ffi::c_int & 0xf as pg_wchar)
            as ::core::ffi::c_uchar;
        *utf8string.offset(1 as ::core::ffi::c_int as isize) = (0x80 as pg_wchar
            | c >> 6 as ::core::ffi::c_int & 0x3f as pg_wchar)
            as ::core::ffi::c_uchar;
        *utf8string.offset(2 as ::core::ffi::c_int as isize) =
            (0x80 as pg_wchar | c & 0x3f as pg_wchar) as ::core::ffi::c_uchar;
    } else {
        *utf8string.offset(0 as ::core::ffi::c_int as isize) = (0xf0 as pg_wchar
            | c >> 18 as ::core::ffi::c_int & 0x7 as pg_wchar)
            as ::core::ffi::c_uchar;
        *utf8string.offset(1 as ::core::ffi::c_int as isize) = (0x80 as pg_wchar
            | c >> 12 as ::core::ffi::c_int & 0x3f as pg_wchar)
            as ::core::ffi::c_uchar;
        *utf8string.offset(2 as ::core::ffi::c_int as isize) = (0x80 as pg_wchar
            | c >> 6 as ::core::ffi::c_int & 0x3f as pg_wchar)
            as ::core::ffi::c_uchar;
        *utf8string.offset(3 as ::core::ffi::c_int as isize) =
            (0x80 as pg_wchar | c & 0x3f as pg_wchar) as ::core::ffi::c_uchar;
    }
    return utf8string;
}
#[inline]
fn vector8_broadcast(c: uint8) -> Vector8 {
    [c; 16]
}
#[inline]
fn vector8_is_highbit_set(v: Vector8) -> bool {
    v.iter().any(|byte| *byte > 0x7f)
}
#[inline]
fn vector8_or(v1: Vector8, v2: Vector8) -> Vector8 {
    let mut out = [0; 16];
    let mut i = 0;
    while i < 16 {
        out[i] = v1[i] | v2[i];
        i += 1;
    }
    out
}
#[inline]
fn vector8_eq(v1: Vector8, v2: Vector8) -> Vector8 {
    let mut out = [0; 16];
    let mut i = 0;
    while i < 16 {
        out[i] = if v1[i] == v2[i] { u8::MAX } else { 0 };
        i += 1;
    }
    out
}
fn is_valid_ascii(s: &[u8]) -> bool {
    for &byte in s {
        if byte == 0 || byte & 0x80 != 0 {
            return false;
        }
    }
    true
}
pub const NONUTF8_INVALID_BYTE0: ::core::ffi::c_int = 0x8d as ::core::ffi::c_int;
pub const NONUTF8_INVALID_BYTE1: ::core::ffi::c_int = ' ' as i32;
unsafe fn pg_ascii2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        let fresh91 = from;
        from = from.offset(1);
        let fresh92 = to;
        to = to.offset(1);
        *fresh92 = *fresh91 as pg_wchar;
        len -= 1;
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
unsafe fn pg_ascii_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return 1 as ::core::ffi::c_int;
}
unsafe fn pg_ascii_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    if *s as ::core::ffi::c_int == '\0' as i32 {
        return 0 as ::core::ffi::c_int;
    }
    if (*s as ::core::ffi::c_int) < 0x20 as ::core::ffi::c_int
        || *s as ::core::ffi::c_int == 0x7f as ::core::ffi::c_int
    {
        return -(1 as ::core::ffi::c_int);
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn pg_euc2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        if *from as ::core::ffi::c_int == SS2 {
            if len < 2 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh63 = from;
            from = from.offset(1);
            *to = (SS2 << 8 as ::core::ffi::c_int | *fresh63 as ::core::ffi::c_int) as pg_wchar;
            len -= 2 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int == SS3 {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh64 = from;
            from = from.offset(1);
            *to = (SS3 << 16 as ::core::ffi::c_int
                | (*fresh64 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int)
                as pg_wchar;
            let fresh65 = from;
            from = from.offset(1);
            *to |= *fresh65 as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int & HIGHBIT != 0 {
            if len < 2 as ::core::ffi::c_int {
                break;
            }
            let fresh66 = from;
            from = from.offset(1);
            *to = ((*fresh66 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int) as pg_wchar;
            let fresh67 = from;
            from = from.offset(1);
            *to |= *fresh67 as pg_wchar;
            len -= 2 as ::core::ffi::c_int;
        } else {
            let fresh68 = from;
            from = from.offset(1);
            *to = *fresh68 as pg_wchar;
            len -= 1;
        }
        to = to.offset(1);
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
#[inline]
unsafe fn pg_euc_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int == SS2 {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == SS3 {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
#[inline]
unsafe fn pg_euc_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int == SS2 {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == SS3 {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_eucjp2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    return pg_euc2wchar_with_len(from, to, len);
}
unsafe fn pg_eucjp_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return pg_euc_mblen(s);
}
unsafe fn pg_eucjp_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int == SS2 {
        len = 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == SS3 {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_euckr2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    return pg_euc2wchar_with_len(from, to, len);
}
unsafe fn pg_euckr_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return pg_euc_mblen(s);
}
unsafe fn pg_euckr_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return pg_euc_dsplen(s);
}
unsafe fn pg_euccn2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        if *from as ::core::ffi::c_int == SS2 {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh84 = from;
            from = from.offset(1);
            *to = (SS2 << 16 as ::core::ffi::c_int
                | (*fresh84 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int)
                as pg_wchar;
            let fresh85 = from;
            from = from.offset(1);
            *to |= *fresh85 as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int == SS3 {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh86 = from;
            from = from.offset(1);
            *to = (SS3 << 16 as ::core::ffi::c_int
                | (*fresh86 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int)
                as pg_wchar;
            let fresh87 = from;
            from = from.offset(1);
            *to |= *fresh87 as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int & HIGHBIT != 0 {
            if len < 2 as ::core::ffi::c_int {
                break;
            }
            let fresh88 = from;
            from = from.offset(1);
            *to = ((*fresh88 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int) as pg_wchar;
            let fresh89 = from;
            from = from.offset(1);
            *to |= *fresh89 as pg_wchar;
            len -= 2 as ::core::ffi::c_int;
        } else {
            let fresh90 = from;
            from = from.offset(1);
            *to = *fresh90 as pg_wchar;
            len -= 1;
        }
        to = to.offset(1);
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
unsafe fn pg_euccn_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int == SS2 {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == SS3 {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_euccn_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_euctw2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        if *from as ::core::ffi::c_int == SS2 {
            if len < 4 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh74 = from;
            from = from.offset(1);
            *to = ((SS2 as uint32) << 24 as ::core::ffi::c_int
                | ((*fresh74 as ::core::ffi::c_int) << 16 as ::core::ffi::c_int) as uint32)
                as pg_wchar;
            let fresh75 = from;
            from = from.offset(1);
            *to |= ((*fresh75 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int) as pg_wchar;
            let fresh76 = from;
            from = from.offset(1);
            *to |= *fresh76 as pg_wchar;
            len -= 4 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int == SS3 {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh77 = from;
            from = from.offset(1);
            *to = (SS3 << 16 as ::core::ffi::c_int
                | (*fresh77 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int)
                as pg_wchar;
            let fresh78 = from;
            from = from.offset(1);
            *to |= *fresh78 as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int & HIGHBIT != 0 {
            if len < 2 as ::core::ffi::c_int {
                break;
            }
            let fresh79 = from;
            from = from.offset(1);
            *to = ((*fresh79 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int) as pg_wchar;
            let fresh80 = from;
            from = from.offset(1);
            *to |= *fresh80 as pg_wchar;
            len -= 2 as ::core::ffi::c_int;
        } else {
            let fresh81 = from;
            from = from.offset(1);
            *to = *fresh81 as pg_wchar;
            len -= 1;
        }
        to = to.offset(1);
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
unsafe fn pg_euctw_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int == SS2 {
        len = 4 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == SS3 {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_euctw_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int == SS2 {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == SS3 {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_wchar2euc_with_len(
    mut from: *const pg_wchar,
    mut to: *mut ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from != 0 {
        let mut c: ::core::ffi::c_uchar = 0;
        c = (*from >> 24 as ::core::ffi::c_int) as ::core::ffi::c_uchar;
        if c != 0 {
            let fresh53 = to;
            to = to.offset(1);
            *fresh53 = c;
            let fresh54 = to;
            to = to.offset(1);
            *fresh54 =
                (*from >> 16 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            let fresh55 = to;
            to = to.offset(1);
            *fresh55 =
                (*from >> 8 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            let fresh56 = to;
            to = to.offset(1);
            *fresh56 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 4 as ::core::ffi::c_int;
        } else {
            c = (*from >> 16 as ::core::ffi::c_int) as ::core::ffi::c_uchar;
            if c != 0 {
                let fresh57 = to;
                to = to.offset(1);
                *fresh57 = c;
                let fresh58 = to;
                to = to.offset(1);
                *fresh58 =
                    (*from >> 8 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
                let fresh59 = to;
                to = to.offset(1);
                *fresh59 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
                cnt += 3 as ::core::ffi::c_int;
            } else {
                c = (*from >> 8 as ::core::ffi::c_int) as ::core::ffi::c_uchar;
                if c != 0 {
                    let fresh60 = to;
                    to = to.offset(1);
                    *fresh60 = c;
                    let fresh61 = to;
                    to = to.offset(1);
                    *fresh61 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
                    cnt += 2 as ::core::ffi::c_int;
                } else {
                    let fresh62 = to;
                    to = to.offset(1);
                    *fresh62 = *from as ::core::ffi::c_uchar;
                    cnt += 1;
                }
            }
        }
        from = from.offset(1);
        len -= 1;
    }
    *to = 0 as ::core::ffi::c_uchar;
    return cnt;
}
unsafe fn pg_johab_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return pg_euc_mblen(s);
}
unsafe fn pg_johab_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return pg_euc_dsplen(s);
}
unsafe fn pg_utf2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    let mut c1: uint32 = 0;
    let mut c2: uint32 = 0;
    let mut c3: uint32 = 0;
    let mut c4: uint32 = 0;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        if *from as ::core::ffi::c_int & 0x80 as ::core::ffi::c_int == 0 as ::core::ffi::c_int {
            let fresh37 = from;
            from = from.offset(1);
            *to = *fresh37 as pg_wchar;
            len -= 1;
        } else if *from as ::core::ffi::c_int & 0xe0 as ::core::ffi::c_int
            == 0xc0 as ::core::ffi::c_int
        {
            if len < 2 as ::core::ffi::c_int {
                break;
            }
            let fresh38 = from;
            from = from.offset(1);
            c1 = (*fresh38 as ::core::ffi::c_int & 0x1f as ::core::ffi::c_int) as uint32;
            let fresh39 = from;
            from = from.offset(1);
            c2 = (*fresh39 as ::core::ffi::c_int & 0x3f as ::core::ffi::c_int) as uint32;
            *to = (c1 << 6 as ::core::ffi::c_int | c2) as pg_wchar;
            len -= 2 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int & 0xf0 as ::core::ffi::c_int
            == 0xe0 as ::core::ffi::c_int
        {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            let fresh40 = from;
            from = from.offset(1);
            c1 = (*fresh40 as ::core::ffi::c_int & 0xf as ::core::ffi::c_int) as uint32;
            let fresh41 = from;
            from = from.offset(1);
            c2 = (*fresh41 as ::core::ffi::c_int & 0x3f as ::core::ffi::c_int) as uint32;
            let fresh42 = from;
            from = from.offset(1);
            c3 = (*fresh42 as ::core::ffi::c_int & 0x3f as ::core::ffi::c_int) as uint32;
            *to = (c1 << 12 as ::core::ffi::c_int | c2 << 6 as ::core::ffi::c_int | c3) as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int & 0xf8 as ::core::ffi::c_int
            == 0xf0 as ::core::ffi::c_int
        {
            if len < 4 as ::core::ffi::c_int {
                break;
            }
            let fresh43 = from;
            from = from.offset(1);
            c1 = (*fresh43 as ::core::ffi::c_int & 0x7 as ::core::ffi::c_int) as uint32;
            let fresh44 = from;
            from = from.offset(1);
            c2 = (*fresh44 as ::core::ffi::c_int & 0x3f as ::core::ffi::c_int) as uint32;
            let fresh45 = from;
            from = from.offset(1);
            c3 = (*fresh45 as ::core::ffi::c_int & 0x3f as ::core::ffi::c_int) as uint32;
            let fresh46 = from;
            from = from.offset(1);
            c4 = (*fresh46 as ::core::ffi::c_int & 0x3f as ::core::ffi::c_int) as uint32;
            *to = (c1 << 18 as ::core::ffi::c_int
                | c2 << 12 as ::core::ffi::c_int
                | c3 << 6 as ::core::ffi::c_int
                | c4) as pg_wchar;
            len -= 4 as ::core::ffi::c_int;
        } else {
            let fresh47 = from;
            from = from.offset(1);
            *to = *fresh47 as pg_wchar;
            len -= 1;
        }
        to = to.offset(1);
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
unsafe fn pg_wchar2utf_with_len(
    mut from: *const pg_wchar,
    mut to: *mut ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from != 0 {
        let mut char_len: ::core::ffi::c_int = 0;
        unicode_to_utf8(*from, to);
        char_len = pg_utf_mblen_private_ptr(to);
        cnt += char_len;
        to = to.offset(char_len as isize);
        from = from.offset(1);
        len -= 1;
    }
    *to = 0 as ::core::ffi::c_uchar;
    return cnt;
}
unsafe fn pg_utf_mblen_private_ptr(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & 0x80 as ::core::ffi::c_int == 0 as ::core::ffi::c_int {
        len = 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & 0xe0 as ::core::ffi::c_int == 0xc0 as ::core::ffi::c_int {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & 0xf0 as ::core::ffi::c_int == 0xe0 as ::core::ffi::c_int {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & 0xf8 as ::core::ffi::c_int == 0xf0 as ::core::ffi::c_int {
        len = 4 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
fn mbbisearch(ucs: pg_wchar, table: &[mbinterval]) -> ::core::ffi::c_int {
    let mut min: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    let mut max = table.len() as ::core::ffi::c_int - 1;
    if table.is_empty() || ucs < table[0].first || ucs > table[max as usize].last {
        return 0 as ::core::ffi::c_int;
    }
    while max >= min {
        let mid = (min + max) / 2 as ::core::ffi::c_int;
        let interval = table[mid as usize];
        if ucs > interval.last {
            min = mid + 1 as ::core::ffi::c_int;
        } else if ucs < interval.first {
            max = mid - 1 as ::core::ffi::c_int;
        } else {
            return 1 as ::core::ffi::c_int;
        }
    }
    return 0 as ::core::ffi::c_int;
}
fn ucs_wcwidth(ucs: pg_wchar) -> ::core::ffi::c_int {
    static nonspacing: [mbinterval; 334] = [
        mbinterval {
            first: 0xad as ::core::ffi::c_uint,
            last: 0xad as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x300 as ::core::ffi::c_uint,
            last: 0x36f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x483 as ::core::ffi::c_uint,
            last: 0x489 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x591 as ::core::ffi::c_uint,
            last: 0x5bd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x5bf as ::core::ffi::c_uint,
            last: 0x5bf as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x5c1 as ::core::ffi::c_uint,
            last: 0x5c2 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x5c4 as ::core::ffi::c_uint,
            last: 0x5c5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x5c7 as ::core::ffi::c_uint,
            last: 0x5c7 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x600 as ::core::ffi::c_uint,
            last: 0x605 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x610 as ::core::ffi::c_uint,
            last: 0x61a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x61c as ::core::ffi::c_uint,
            last: 0x61c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x64b as ::core::ffi::c_uint,
            last: 0x65f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x670 as ::core::ffi::c_uint,
            last: 0x670 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x6d6 as ::core::ffi::c_uint,
            last: 0x6dd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x6df as ::core::ffi::c_uint,
            last: 0x6e4 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x6e7 as ::core::ffi::c_uint,
            last: 0x6e8 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x6ea as ::core::ffi::c_uint,
            last: 0x6ed as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x70f as ::core::ffi::c_uint,
            last: 0x70f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x711 as ::core::ffi::c_uint,
            last: 0x711 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x730 as ::core::ffi::c_uint,
            last: 0x74a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x7a6 as ::core::ffi::c_uint,
            last: 0x7b0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x7eb as ::core::ffi::c_uint,
            last: 0x7f3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x7fd as ::core::ffi::c_uint,
            last: 0x7fd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x816 as ::core::ffi::c_uint,
            last: 0x819 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x81b as ::core::ffi::c_uint,
            last: 0x823 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x825 as ::core::ffi::c_uint,
            last: 0x827 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x829 as ::core::ffi::c_uint,
            last: 0x82d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x859 as ::core::ffi::c_uint,
            last: 0x85b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x890 as ::core::ffi::c_uint,
            last: 0x89f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x8ca as ::core::ffi::c_uint,
            last: 0x902 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x93a as ::core::ffi::c_uint,
            last: 0x93a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x93c as ::core::ffi::c_uint,
            last: 0x93c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x941 as ::core::ffi::c_uint,
            last: 0x948 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x94d as ::core::ffi::c_uint,
            last: 0x94d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x951 as ::core::ffi::c_uint,
            last: 0x957 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x962 as ::core::ffi::c_uint,
            last: 0x963 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x981 as ::core::ffi::c_uint,
            last: 0x981 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x9bc as ::core::ffi::c_uint,
            last: 0x9bc as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x9c1 as ::core::ffi::c_uint,
            last: 0x9c4 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x9cd as ::core::ffi::c_uint,
            last: 0x9cd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x9e2 as ::core::ffi::c_uint,
            last: 0x9e3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x9fe as ::core::ffi::c_uint,
            last: 0xa02 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa3c as ::core::ffi::c_uint,
            last: 0xa3c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa41 as ::core::ffi::c_uint,
            last: 0xa51 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa70 as ::core::ffi::c_uint,
            last: 0xa71 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa75 as ::core::ffi::c_uint,
            last: 0xa75 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa81 as ::core::ffi::c_uint,
            last: 0xa82 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xabc as ::core::ffi::c_uint,
            last: 0xabc as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xac1 as ::core::ffi::c_uint,
            last: 0xac8 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xacd as ::core::ffi::c_uint,
            last: 0xacd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xae2 as ::core::ffi::c_uint,
            last: 0xae3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xafa as ::core::ffi::c_uint,
            last: 0xb01 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xb3c as ::core::ffi::c_uint,
            last: 0xb3c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xb3f as ::core::ffi::c_uint,
            last: 0xb3f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xb41 as ::core::ffi::c_uint,
            last: 0xb44 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xb4d as ::core::ffi::c_uint,
            last: 0xb56 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xb62 as ::core::ffi::c_uint,
            last: 0xb63 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xb82 as ::core::ffi::c_uint,
            last: 0xb82 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xbc0 as ::core::ffi::c_uint,
            last: 0xbc0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xbcd as ::core::ffi::c_uint,
            last: 0xbcd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc00 as ::core::ffi::c_uint,
            last: 0xc00 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc04 as ::core::ffi::c_uint,
            last: 0xc04 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc3c as ::core::ffi::c_uint,
            last: 0xc3c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc3e as ::core::ffi::c_uint,
            last: 0xc40 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc46 as ::core::ffi::c_uint,
            last: 0xc56 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc62 as ::core::ffi::c_uint,
            last: 0xc63 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xc81 as ::core::ffi::c_uint,
            last: 0xc81 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xcbc as ::core::ffi::c_uint,
            last: 0xcbc as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xcbf as ::core::ffi::c_uint,
            last: 0xcbf as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xcc6 as ::core::ffi::c_uint,
            last: 0xcc6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xccc as ::core::ffi::c_uint,
            last: 0xccd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xce2 as ::core::ffi::c_uint,
            last: 0xce3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xd00 as ::core::ffi::c_uint,
            last: 0xd01 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xd3b as ::core::ffi::c_uint,
            last: 0xd3c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xd41 as ::core::ffi::c_uint,
            last: 0xd44 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xd4d as ::core::ffi::c_uint,
            last: 0xd4d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xd62 as ::core::ffi::c_uint,
            last: 0xd63 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xd81 as ::core::ffi::c_uint,
            last: 0xd81 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xdca as ::core::ffi::c_uint,
            last: 0xdca as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xdd2 as ::core::ffi::c_uint,
            last: 0xdd6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xe31 as ::core::ffi::c_uint,
            last: 0xe31 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xe34 as ::core::ffi::c_uint,
            last: 0xe3a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xe47 as ::core::ffi::c_uint,
            last: 0xe4e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xeb1 as ::core::ffi::c_uint,
            last: 0xeb1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xeb4 as ::core::ffi::c_uint,
            last: 0xebc as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xec8 as ::core::ffi::c_uint,
            last: 0xece as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf18 as ::core::ffi::c_uint,
            last: 0xf19 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf35 as ::core::ffi::c_uint,
            last: 0xf35 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf37 as ::core::ffi::c_uint,
            last: 0xf37 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf39 as ::core::ffi::c_uint,
            last: 0xf39 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf71 as ::core::ffi::c_uint,
            last: 0xf7e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf80 as ::core::ffi::c_uint,
            last: 0xf84 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf86 as ::core::ffi::c_uint,
            last: 0xf87 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf8d as ::core::ffi::c_uint,
            last: 0xfbc as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfc6 as ::core::ffi::c_uint,
            last: 0xfc6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x102d as ::core::ffi::c_uint,
            last: 0x1030 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1032 as ::core::ffi::c_uint,
            last: 0x1037 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1039 as ::core::ffi::c_uint,
            last: 0x103a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x103d as ::core::ffi::c_uint,
            last: 0x103e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1058 as ::core::ffi::c_uint,
            last: 0x1059 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x105e as ::core::ffi::c_uint,
            last: 0x1060 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1071 as ::core::ffi::c_uint,
            last: 0x1074 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1082 as ::core::ffi::c_uint,
            last: 0x1082 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1085 as ::core::ffi::c_uint,
            last: 0x1086 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x108d as ::core::ffi::c_uint,
            last: 0x108d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x109d as ::core::ffi::c_uint,
            last: 0x109d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x135d as ::core::ffi::c_uint,
            last: 0x135f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1712 as ::core::ffi::c_uint,
            last: 0x1714 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1732 as ::core::ffi::c_uint,
            last: 0x1733 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1752 as ::core::ffi::c_uint,
            last: 0x1753 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1772 as ::core::ffi::c_uint,
            last: 0x1773 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x17b4 as ::core::ffi::c_uint,
            last: 0x17b5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x17b7 as ::core::ffi::c_uint,
            last: 0x17bd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x17c6 as ::core::ffi::c_uint,
            last: 0x17c6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x17c9 as ::core::ffi::c_uint,
            last: 0x17d3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x17dd as ::core::ffi::c_uint,
            last: 0x17dd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x180b as ::core::ffi::c_uint,
            last: 0x180f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1885 as ::core::ffi::c_uint,
            last: 0x1886 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x18a9 as ::core::ffi::c_uint,
            last: 0x18a9 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1920 as ::core::ffi::c_uint,
            last: 0x1922 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1927 as ::core::ffi::c_uint,
            last: 0x1928 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1932 as ::core::ffi::c_uint,
            last: 0x1932 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1939 as ::core::ffi::c_uint,
            last: 0x193b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a17 as ::core::ffi::c_uint,
            last: 0x1a18 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a1b as ::core::ffi::c_uint,
            last: 0x1a1b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a56 as ::core::ffi::c_uint,
            last: 0x1a56 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a58 as ::core::ffi::c_uint,
            last: 0x1a60 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a62 as ::core::ffi::c_uint,
            last: 0x1a62 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a65 as ::core::ffi::c_uint,
            last: 0x1a6c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1a73 as ::core::ffi::c_uint,
            last: 0x1a7f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1ab0 as ::core::ffi::c_uint,
            last: 0x1b03 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b34 as ::core::ffi::c_uint,
            last: 0x1b34 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b36 as ::core::ffi::c_uint,
            last: 0x1b3a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b3c as ::core::ffi::c_uint,
            last: 0x1b3c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b42 as ::core::ffi::c_uint,
            last: 0x1b42 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b6b as ::core::ffi::c_uint,
            last: 0x1b73 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b80 as ::core::ffi::c_uint,
            last: 0x1b81 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1ba2 as ::core::ffi::c_uint,
            last: 0x1ba5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1ba8 as ::core::ffi::c_uint,
            last: 0x1ba9 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1bab as ::core::ffi::c_uint,
            last: 0x1bad as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1be6 as ::core::ffi::c_uint,
            last: 0x1be6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1be8 as ::core::ffi::c_uint,
            last: 0x1be9 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1bed as ::core::ffi::c_uint,
            last: 0x1bed as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1bef as ::core::ffi::c_uint,
            last: 0x1bf1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1c2c as ::core::ffi::c_uint,
            last: 0x1c33 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1c36 as ::core::ffi::c_uint,
            last: 0x1c37 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1cd0 as ::core::ffi::c_uint,
            last: 0x1cd2 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1cd4 as ::core::ffi::c_uint,
            last: 0x1ce0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1ce2 as ::core::ffi::c_uint,
            last: 0x1ce8 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1ced as ::core::ffi::c_uint,
            last: 0x1ced as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1cf4 as ::core::ffi::c_uint,
            last: 0x1cf4 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1cf8 as ::core::ffi::c_uint,
            last: 0x1cf9 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1dc0 as ::core::ffi::c_uint,
            last: 0x1dff as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x200b as ::core::ffi::c_uint,
            last: 0x200f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x202a as ::core::ffi::c_uint,
            last: 0x202e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2060 as ::core::ffi::c_uint,
            last: 0x206f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x20d0 as ::core::ffi::c_uint,
            last: 0x20f0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2cef as ::core::ffi::c_uint,
            last: 0x2cf1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2d7f as ::core::ffi::c_uint,
            last: 0x2d7f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2de0 as ::core::ffi::c_uint,
            last: 0x2dff as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x302a as ::core::ffi::c_uint,
            last: 0x302d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3099 as ::core::ffi::c_uint,
            last: 0x309a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa66f as ::core::ffi::c_uint,
            last: 0xa672 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa674 as ::core::ffi::c_uint,
            last: 0xa67d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa69e as ::core::ffi::c_uint,
            last: 0xa69f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa6f0 as ::core::ffi::c_uint,
            last: 0xa6f1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa802 as ::core::ffi::c_uint,
            last: 0xa802 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa806 as ::core::ffi::c_uint,
            last: 0xa806 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa80b as ::core::ffi::c_uint,
            last: 0xa80b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa825 as ::core::ffi::c_uint,
            last: 0xa826 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa82c as ::core::ffi::c_uint,
            last: 0xa82c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa8c4 as ::core::ffi::c_uint,
            last: 0xa8c5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa8e0 as ::core::ffi::c_uint,
            last: 0xa8f1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa8ff as ::core::ffi::c_uint,
            last: 0xa8ff as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa926 as ::core::ffi::c_uint,
            last: 0xa92d as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa947 as ::core::ffi::c_uint,
            last: 0xa951 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa980 as ::core::ffi::c_uint,
            last: 0xa982 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa9b3 as ::core::ffi::c_uint,
            last: 0xa9b3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa9b6 as ::core::ffi::c_uint,
            last: 0xa9b9 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa9bc as ::core::ffi::c_uint,
            last: 0xa9bd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa9e5 as ::core::ffi::c_uint,
            last: 0xa9e5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaa29 as ::core::ffi::c_uint,
            last: 0xaa2e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaa31 as ::core::ffi::c_uint,
            last: 0xaa32 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaa35 as ::core::ffi::c_uint,
            last: 0xaa36 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaa43 as ::core::ffi::c_uint,
            last: 0xaa43 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaa4c as ::core::ffi::c_uint,
            last: 0xaa4c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaa7c as ::core::ffi::c_uint,
            last: 0xaa7c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaab0 as ::core::ffi::c_uint,
            last: 0xaab0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaab2 as ::core::ffi::c_uint,
            last: 0xaab4 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaab7 as ::core::ffi::c_uint,
            last: 0xaab8 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaabe as ::core::ffi::c_uint,
            last: 0xaabf as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaac1 as ::core::ffi::c_uint,
            last: 0xaac1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaaec as ::core::ffi::c_uint,
            last: 0xaaed as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xaaf6 as ::core::ffi::c_uint,
            last: 0xaaf6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xabe5 as ::core::ffi::c_uint,
            last: 0xabe5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xabe8 as ::core::ffi::c_uint,
            last: 0xabe8 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xabed as ::core::ffi::c_uint,
            last: 0xabed as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfb1e as ::core::ffi::c_uint,
            last: 0xfb1e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfe00 as ::core::ffi::c_uint,
            last: 0xfe0f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfe20 as ::core::ffi::c_uint,
            last: 0xfe2f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfeff as ::core::ffi::c_uint,
            last: 0xfeff as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfff9 as ::core::ffi::c_uint,
            last: 0xfffb as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x101fd as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x101fd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x102e0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x102e0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10376 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1037a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10a01 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10a0f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10a38 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10a3f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10ae5 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10ae6 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10d24 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10d27 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10d69 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10d6d as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10eab as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10eac as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10efc as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10eff as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10f46 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10f50 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x10f82 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x10f85 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11001 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11001 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11038 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11046 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11070 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11070 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11073 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11074 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1107f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11081 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x110b3 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x110b6 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x110b9 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x110ba as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x110bd as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x110bd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x110c2 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x110cd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11100 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11102 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11127 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1112b as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1112d as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11134 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11173 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11173 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11180 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11181 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x111b6 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x111be as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x111c9 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x111cc as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x111cf as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x111cf as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1122f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11231 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11234 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11234 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11236 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11237 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1123e as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1123e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11241 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11241 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x112df as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x112df as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x112e3 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x112ea as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11300 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11301 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1133b as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1133c as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11340 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11340 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11366 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11374 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x113bb as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x113c0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x113ce as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x113ce as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x113d0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x113d0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x113d2 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x113d2 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x113e1 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x113e2 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11438 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1143f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11442 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11444 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11446 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11446 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1145e as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1145e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x114b3 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x114b8 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x114ba as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x114ba as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x114bf as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x114c0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x114c2 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x114c3 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x115b2 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x115b5 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x115bc as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x115bd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x115bf as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x115c0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x115dc as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x115dd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11633 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1163a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1163d as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1163d as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1163f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11640 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x116ab as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x116ab as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x116ad as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x116ad as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x116b0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x116b5 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x116b7 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x116b7 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1171d as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1171d as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1171f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1171f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11722 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11725 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11727 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1172b as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1182f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11837 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11839 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1183a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1193b as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1193c as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1193e as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1193e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11943 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11943 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x119d4 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x119db as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x119e0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x119e0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a01 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a0a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a33 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a38 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a3b as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a3e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a47 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a47 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a51 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a56 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a59 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a5b as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a8a as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a96 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11a98 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11a99 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11c30 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11c3d as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11c3f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11c3f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11c92 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11ca7 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11caa as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11cb0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11cb2 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11cb3 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11cb5 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11cb6 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11d31 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11d45 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11d47 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11d47 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11d90 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11d91 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11d95 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11d95 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11d97 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11d97 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11ef3 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11ef4 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11f00 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11f01 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11f36 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11f3a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11f40 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11f40 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11f42 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11f42 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x11f5a as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x11f5a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x13430 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x13440 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x13447 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x13455 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1611e as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16129 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1612d as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1612f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16af0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16af4 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16b30 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16b36 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16f4f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16f4f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16f8f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16f92 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16fe4 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16fe4 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1bc9d as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1bc9e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1bca0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1bca3 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1cf00 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1cf46 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d167 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d169 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d173 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d182 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d185 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d18b as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d1aa as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d1ad as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d242 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d244 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1da00 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1da36 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1da3b as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1da6c as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1da75 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1da75 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1da84 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1da84 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1da9b as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1daaf as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e000 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e02a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e08f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e08f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e130 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e136 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e2ae as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e2ae as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e2ec as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e2ef as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e4ec as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e4ef as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e5ee as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e5ef as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e8d0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e8d6 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1e944 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1e94a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xe0001 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0xe01ef as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
    ];
    static east_asian_fw: [mbinterval; 122] = [
        mbinterval {
            first: 0x1100 as ::core::ffi::c_uint,
            last: 0x115f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x231a as ::core::ffi::c_uint,
            last: 0x231b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2329 as ::core::ffi::c_uint,
            last: 0x232a as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x23e9 as ::core::ffi::c_uint,
            last: 0x23ec as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x23f0 as ::core::ffi::c_uint,
            last: 0x23f0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x23f3 as ::core::ffi::c_uint,
            last: 0x23f3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x25fd as ::core::ffi::c_uint,
            last: 0x25fe as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2614 as ::core::ffi::c_uint,
            last: 0x2615 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2630 as ::core::ffi::c_uint,
            last: 0x2637 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2648 as ::core::ffi::c_uint,
            last: 0x2653 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x267f as ::core::ffi::c_uint,
            last: 0x267f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x268a as ::core::ffi::c_uint,
            last: 0x268f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2693 as ::core::ffi::c_uint,
            last: 0x2693 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26a1 as ::core::ffi::c_uint,
            last: 0x26a1 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26aa as ::core::ffi::c_uint,
            last: 0x26ab as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26bd as ::core::ffi::c_uint,
            last: 0x26be as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26c4 as ::core::ffi::c_uint,
            last: 0x26c5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26ce as ::core::ffi::c_uint,
            last: 0x26ce as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26d4 as ::core::ffi::c_uint,
            last: 0x26d4 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26ea as ::core::ffi::c_uint,
            last: 0x26ea as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26f2 as ::core::ffi::c_uint,
            last: 0x26f3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26f5 as ::core::ffi::c_uint,
            last: 0x26f5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26fa as ::core::ffi::c_uint,
            last: 0x26fa as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x26fd as ::core::ffi::c_uint,
            last: 0x26fd as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2705 as ::core::ffi::c_uint,
            last: 0x2705 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x270a as ::core::ffi::c_uint,
            last: 0x270b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2728 as ::core::ffi::c_uint,
            last: 0x2728 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x274c as ::core::ffi::c_uint,
            last: 0x274c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x274e as ::core::ffi::c_uint,
            last: 0x274e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2753 as ::core::ffi::c_uint,
            last: 0x2755 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2757 as ::core::ffi::c_uint,
            last: 0x2757 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2795 as ::core::ffi::c_uint,
            last: 0x2797 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x27b0 as ::core::ffi::c_uint,
            last: 0x27b0 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x27bf as ::core::ffi::c_uint,
            last: 0x27bf as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2b1b as ::core::ffi::c_uint,
            last: 0x2b1c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2b50 as ::core::ffi::c_uint,
            last: 0x2b50 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2b55 as ::core::ffi::c_uint,
            last: 0x2b55 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2e80 as ::core::ffi::c_uint,
            last: 0x2e99 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2e9b as ::core::ffi::c_uint,
            last: 0x2ef3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2f00 as ::core::ffi::c_uint,
            last: 0x2fd5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x2ff0 as ::core::ffi::c_uint,
            last: 0x303e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3041 as ::core::ffi::c_uint,
            last: 0x3096 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3099 as ::core::ffi::c_uint,
            last: 0x30ff as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3105 as ::core::ffi::c_uint,
            last: 0x312f as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3131 as ::core::ffi::c_uint,
            last: 0x318e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3190 as ::core::ffi::c_uint,
            last: 0x31e5 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x31ef as ::core::ffi::c_uint,
            last: 0x321e as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3220 as ::core::ffi::c_uint,
            last: 0x3247 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x3250 as ::core::ffi::c_uint,
            last: 0xa48c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa490 as ::core::ffi::c_uint,
            last: 0xa4c6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xa960 as ::core::ffi::c_uint,
            last: 0xa97c as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xac00 as ::core::ffi::c_uint,
            last: 0xd7a3 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xf900 as ::core::ffi::c_uint,
            last: 0xfaff as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfe10 as ::core::ffi::c_uint,
            last: 0xfe19 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfe30 as ::core::ffi::c_uint,
            last: 0xfe52 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfe54 as ::core::ffi::c_uint,
            last: 0xfe66 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xfe68 as ::core::ffi::c_uint,
            last: 0xfe6b as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xff01 as ::core::ffi::c_uint,
            last: 0xff60 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0xffe0 as ::core::ffi::c_uint,
            last: 0xffe6 as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16fe0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16fe4 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x16ff0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x16ff1 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x17000 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x187f7 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x18800 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x18cd5 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x18cff as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x18d08 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1aff0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1aff3 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1aff5 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1affb as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1affd as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1affe as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b000 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1b122 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b132 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1b132 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b150 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1b152 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b155 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1b155 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b164 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1b167 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1b170 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1b2fb as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d300 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d356 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1d360 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1d376 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f004 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f004 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f0cf as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f0cf as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f18e as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f18e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f191 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f19a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f200 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f202 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f210 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f23b as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f240 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f248 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f250 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f251 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f260 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f265 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f300 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f320 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f32d as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f335 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f337 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f37c as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f37e as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f393 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f3a0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f3ca as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f3cf as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f3d3 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f3e0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f3f0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f3f4 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f3f4 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f3f8 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f43e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f440 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f440 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f442 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f4fc as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f4ff as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f53d as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f54b as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f54e as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f550 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f567 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f57a as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f57a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f595 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f596 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f5a4 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f5a4 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f5fb as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f64f as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f680 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6c5 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f6cc as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6cc as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f6d0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6d2 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f6d5 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6d7 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f6dc as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6df as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f6eb as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6ec as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f6f4 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f6fc as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f7e0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f7eb as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f7f0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f7f0 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f90c as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f93a as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f93c as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f945 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1f947 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1f9ff as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1fa70 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1fa7c as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1fa80 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1fa89 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1fa8f as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1fac6 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1face as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1fadc as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1fadf as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1fae9 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x1faf0 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x1faf8 as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x20000 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x2fffd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
        mbinterval {
            first: 0x30000 as ::core::ffi::c_int as ::core::ffi::c_uint,
            last: 0x3fffd as ::core::ffi::c_int as ::core::ffi::c_uint,
        },
    ];
    if ucs == 0 as pg_wchar {
        return 0 as ::core::ffi::c_int;
    }
    if ucs < 0x20 as pg_wchar
        || ucs >= 0x7f as pg_wchar && ucs < 0xa0 as pg_wchar
        || ucs > 0x10ffff as ::core::ffi::c_int as pg_wchar
    {
        return -(1 as ::core::ffi::c_int);
    }
    if mbbisearch(ucs, &nonspacing) != 0 {
        return 0 as ::core::ffi::c_int;
    }
    if mbbisearch(ucs, &east_asian_fw) != 0 {
        return 2 as ::core::ffi::c_int;
    }
    return 1 as ::core::ffi::c_int;
}
unsafe fn pg_utf_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return ucs_wcwidth(utf8_to_unicode(s));
}
unsafe fn pg_mule2wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        if *from as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
            && *from as ::core::ffi::c_int <= 0x8d as ::core::ffi::c_int
        {
            if len < 2 as ::core::ffi::c_int {
                break;
            }
            let fresh25 = from;
            from = from.offset(1);
            *to = ((*fresh25 as ::core::ffi::c_int) << 16 as ::core::ffi::c_int) as pg_wchar;
            let fresh26 = from;
            from = from.offset(1);
            *to |= *fresh26 as pg_wchar;
            len -= 2 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int == LCPRV1_A || *from as ::core::ffi::c_int == LCPRV1_B
        {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh27 = from;
            from = from.offset(1);
            *to = ((*fresh27 as ::core::ffi::c_int) << 16 as ::core::ffi::c_int) as pg_wchar;
            let fresh28 = from;
            from = from.offset(1);
            *to |= *fresh28 as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int >= 0x90 as ::core::ffi::c_int
            && *from as ::core::ffi::c_int <= 0x99 as ::core::ffi::c_int
        {
            if len < 3 as ::core::ffi::c_int {
                break;
            }
            let fresh29 = from;
            from = from.offset(1);
            *to = ((*fresh29 as ::core::ffi::c_int) << 16 as ::core::ffi::c_int) as pg_wchar;
            let fresh30 = from;
            from = from.offset(1);
            *to |= ((*fresh30 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int) as pg_wchar;
            let fresh31 = from;
            from = from.offset(1);
            *to |= *fresh31 as pg_wchar;
            len -= 3 as ::core::ffi::c_int;
        } else if *from as ::core::ffi::c_int == LCPRV2_A || *from as ::core::ffi::c_int == LCPRV2_B
        {
            if len < 4 as ::core::ffi::c_int {
                break;
            }
            from = from.offset(1);
            let fresh32 = from;
            from = from.offset(1);
            *to = ((*fresh32 as ::core::ffi::c_int) << 16 as ::core::ffi::c_int) as pg_wchar;
            let fresh33 = from;
            from = from.offset(1);
            *to |= ((*fresh33 as ::core::ffi::c_int) << 8 as ::core::ffi::c_int) as pg_wchar;
            let fresh34 = from;
            from = from.offset(1);
            *to |= *fresh34 as pg_wchar;
            len -= 4 as ::core::ffi::c_int;
        } else {
            let fresh35 = from;
            from = from.offset(1);
            *to = *fresh35 as pg_wchar;
            len -= 1;
        }
        to = to.offset(1);
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
unsafe fn pg_wchar2mule_with_len(
    mut from: *const pg_wchar,
    mut to: *mut ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from != 0 {
        let mut lb: ::core::ffi::c_uchar = 0;
        lb = (*from >> 16 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
        if lb as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
            && lb as ::core::ffi::c_int <= 0x8d as ::core::ffi::c_int
        {
            let fresh5 = to;
            to = to.offset(1);
            *fresh5 = lb;
            let fresh6 = to;
            to = to.offset(1);
            *fresh6 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 2 as ::core::ffi::c_int;
        } else if lb as ::core::ffi::c_int >= 0x90 as ::core::ffi::c_int
            && lb as ::core::ffi::c_int <= 0x99 as ::core::ffi::c_int
        {
            let fresh7 = to;
            to = to.offset(1);
            *fresh7 = lb;
            let fresh8 = to;
            to = to.offset(1);
            *fresh8 = (*from >> 8 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            let fresh9 = to;
            to = to.offset(1);
            *fresh9 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 3 as ::core::ffi::c_int;
        } else if lb as ::core::ffi::c_int >= 0xa0 as ::core::ffi::c_int
            && lb as ::core::ffi::c_int <= 0xdf as ::core::ffi::c_int
        {
            let fresh10 = to;
            to = to.offset(1);
            *fresh10 = LCPRV1_A as ::core::ffi::c_uchar;
            let fresh11 = to;
            to = to.offset(1);
            *fresh11 = lb;
            let fresh12 = to;
            to = to.offset(1);
            *fresh12 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 3 as ::core::ffi::c_int;
        } else if lb as ::core::ffi::c_int >= 0xe0 as ::core::ffi::c_int
            && lb as ::core::ffi::c_int <= 0xef as ::core::ffi::c_int
        {
            let fresh13 = to;
            to = to.offset(1);
            *fresh13 = LCPRV1_B as ::core::ffi::c_uchar;
            let fresh14 = to;
            to = to.offset(1);
            *fresh14 = lb;
            let fresh15 = to;
            to = to.offset(1);
            *fresh15 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 3 as ::core::ffi::c_int;
        } else if lb as ::core::ffi::c_int >= 0xf0 as ::core::ffi::c_int
            && lb as ::core::ffi::c_int <= 0xf4 as ::core::ffi::c_int
        {
            let fresh16 = to;
            to = to.offset(1);
            *fresh16 = LCPRV2_A as ::core::ffi::c_uchar;
            let fresh17 = to;
            to = to.offset(1);
            *fresh17 = lb;
            let fresh18 = to;
            to = to.offset(1);
            *fresh18 =
                (*from >> 8 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            let fresh19 = to;
            to = to.offset(1);
            *fresh19 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 4 as ::core::ffi::c_int;
        } else if lb as ::core::ffi::c_int >= 0xf5 as ::core::ffi::c_int
            && lb as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int
        {
            let fresh20 = to;
            to = to.offset(1);
            *fresh20 = LCPRV2_B as ::core::ffi::c_uchar;
            let fresh21 = to;
            to = to.offset(1);
            *fresh21 = lb;
            let fresh22 = to;
            to = to.offset(1);
            *fresh22 =
                (*from >> 8 as ::core::ffi::c_int & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            let fresh23 = to;
            to = to.offset(1);
            *fresh23 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 4 as ::core::ffi::c_int;
        } else {
            let fresh24 = to;
            to = to.offset(1);
            *fresh24 = (*from & 0xff as pg_wchar) as ::core::ffi::c_uchar;
            cnt += 1 as ::core::ffi::c_int;
        }
        from = from.offset(1);
        len -= 1;
    }
    *to = 0 as ::core::ffi::c_uchar;
    return cnt;
}
unsafe fn pg_mule_mblen_ptr(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0x8d as ::core::ffi::c_int
    {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == LCPRV1_A || *s as ::core::ffi::c_int == LCPRV1_B {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int >= 0x90 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0x99 as ::core::ffi::c_int
    {
        len = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == LCPRV2_A || *s as ::core::ffi::c_int == LCPRV2_B {
        len = 4 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_mule_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0x8d as ::core::ffi::c_int
    {
        len = 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == LCPRV1_A || *s as ::core::ffi::c_int == LCPRV1_B {
        len = 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int >= 0x90 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0x99 as ::core::ffi::c_int
    {
        len = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int == LCPRV2_A || *s as ::core::ffi::c_int == LCPRV2_B {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_latin12wchar_with_len(
    mut from: *const ::core::ffi::c_uchar,
    mut to: *mut pg_wchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from as ::core::ffi::c_int != 0 {
        let fresh3 = from;
        from = from.offset(1);
        let fresh4 = to;
        to = to.offset(1);
        *fresh4 = *fresh3 as pg_wchar;
        len -= 1;
        cnt += 1;
    }
    *to = 0 as pg_wchar;
    return cnt;
}
unsafe fn pg_wchar2single_with_len(
    mut from: *const pg_wchar,
    mut to: *mut ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut cnt: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
    while len > 0 as ::core::ffi::c_int && *from != 0 {
        let fresh1 = from;
        from = from.offset(1);
        let fresh2 = to;
        to = to.offset(1);
        *fresh2 = *fresh1 as ::core::ffi::c_uchar;
        len -= 1;
        cnt += 1;
    }
    *to = 0 as ::core::ffi::c_uchar;
    return cnt;
}
unsafe fn pg_latin1_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return 1 as ::core::ffi::c_int;
}
unsafe fn pg_latin1_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    return pg_ascii_dsplen(s);
}
unsafe fn pg_sjis_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0xdf as ::core::ffi::c_int
    {
        len = 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_sjis_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0xdf as ::core::ffi::c_int
    {
        len = 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_big5_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_big5_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_gbk_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_gbk_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_uhc_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = 1 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_uhc_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_gb18030_mblen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT == 0 {
        len = 1 as ::core::ffi::c_int;
    } else if *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
        >= 0x30 as ::core::ffi::c_int
        && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            <= 0x39 as ::core::ffi::c_int
    {
        len = 4 as ::core::ffi::c_int;
    } else {
        len = 2 as ::core::ffi::c_int;
    }
    return len;
}
unsafe fn pg_gb18030_dsplen(mut s: *const ::core::ffi::c_uchar) -> ::core::ffi::c_int {
    let mut len: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT != 0 {
        len = 2 as ::core::ffi::c_int;
    } else {
        len = pg_ascii_dsplen(s);
    }
    return len;
}
unsafe fn pg_ascii_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    return 1 as ::core::ffi::c_int;
}
unsafe fn pg_ascii_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut nullpos: *const ::core::ffi::c_uchar = memchr(
        s as *const ::core::ffi::c_void,
        0 as ::core::ffi::c_int,
        len as size_t,
    ) as *const ::core::ffi::c_uchar;
    if nullpos.is_null() {
        return len;
    } else {
        return nullpos.offset_from(s) as ::core::ffi::c_long as ::core::ffi::c_int;
    };
}
unsafe fn pg_eucjp_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_uchar = 0;
    let mut c2: ::core::ffi::c_uchar = 0;
    let fresh48 = s;
    s = s.offset(1);
    c1 = *fresh48;
    match c1 as ::core::ffi::c_int {
        SS2 => {
            l = 2 as ::core::ffi::c_int;
            if l > len {
                return -(1 as ::core::ffi::c_int);
            }
            let fresh49 = s;
            s = s.offset(1);
            c2 = *fresh49;
            if (c2 as ::core::ffi::c_int) < 0xa1 as ::core::ffi::c_int
                || c2 as ::core::ffi::c_int > 0xdf as ::core::ffi::c_int
            {
                return -(1 as ::core::ffi::c_int);
            }
        }
        SS3 => {
            l = 3 as ::core::ffi::c_int;
            if l > len {
                return -(1 as ::core::ffi::c_int);
            }
            let fresh50 = s;
            s = s.offset(1);
            c2 = *fresh50;
            if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
            {
                return -(1 as ::core::ffi::c_int);
            }
            let fresh51 = s;
            s = s.offset(1);
            c2 = *fresh51;
            if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
            {
                return -(1 as ::core::ffi::c_int);
            }
        }
        _ => {
            if c1 as ::core::ffi::c_int & HIGHBIT != 0 {
                l = 2 as ::core::ffi::c_int;
                if l > len {
                    return -(1 as ::core::ffi::c_int);
                }
                if !(c1 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                    && c1 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
                {
                    return -(1 as ::core::ffi::c_int);
                }
                let fresh52 = s;
                s = s.offset(1);
                c2 = *fresh52;
                if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                    && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
                {
                    return -(1 as ::core::ffi::c_int);
                }
            } else {
                l = 1 as ::core::ffi::c_int;
            }
        }
    }
    return l;
}
unsafe fn pg_eucjp_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_eucjp_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_euckr_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_uchar = 0;
    let mut c2: ::core::ffi::c_uchar = 0;
    let fresh82 = s;
    s = s.offset(1);
    c1 = *fresh82;
    if c1 as ::core::ffi::c_int & HIGHBIT != 0 {
        l = 2 as ::core::ffi::c_int;
        if l > len {
            return -(1 as ::core::ffi::c_int);
        }
        if !(c1 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
            && c1 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
        {
            return -(1 as ::core::ffi::c_int);
        }
        let fresh83 = s;
        s = s.offset(1);
        c2 = *fresh83;
        if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
            && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
        {
            return -(1 as ::core::ffi::c_int);
        }
    } else {
        l = 1 as ::core::ffi::c_int;
    }
    return l;
}
unsafe fn pg_euckr_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_euckr_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_euctw_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_uchar = 0;
    let mut c2: ::core::ffi::c_uchar = 0;
    let fresh69 = s;
    s = s.offset(1);
    c1 = *fresh69;
    match c1 as ::core::ffi::c_int {
        SS2 => {
            l = 4 as ::core::ffi::c_int;
            if l > len {
                return -(1 as ::core::ffi::c_int);
            }
            let fresh70 = s;
            s = s.offset(1);
            c2 = *fresh70;
            if (c2 as ::core::ffi::c_int) < 0xa1 as ::core::ffi::c_int
                || c2 as ::core::ffi::c_int > 0xa7 as ::core::ffi::c_int
            {
                return -(1 as ::core::ffi::c_int);
            }
            let fresh71 = s;
            s = s.offset(1);
            c2 = *fresh71;
            if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
            {
                return -(1 as ::core::ffi::c_int);
            }
            let fresh72 = s;
            s = s.offset(1);
            c2 = *fresh72;
            if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
            {
                return -(1 as ::core::ffi::c_int);
            }
        }
        SS3 => return -(1 as ::core::ffi::c_int),
        _ => {
            if c1 as ::core::ffi::c_int & HIGHBIT != 0 {
                l = 2 as ::core::ffi::c_int;
                if l > len {
                    return -(1 as ::core::ffi::c_int);
                }
                let fresh73 = s;
                s = s.offset(1);
                c2 = *fresh73;
                if !(c2 as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
                    && c2 as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
                {
                    return -(1 as ::core::ffi::c_int);
                }
            } else {
                l = 1 as ::core::ffi::c_int;
            }
        }
    }
    return l;
}
unsafe fn pg_euctw_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_euctw_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_johab_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut mbl: ::core::ffi::c_int = 0;
    let mut c: ::core::ffi::c_uchar = 0;
    mbl = pg_johab_mblen(s);
    l = mbl;
    if len < l {
        return -(1 as ::core::ffi::c_int);
    }
    if *s as ::core::ffi::c_int & HIGHBIT == 0 {
        return mbl;
    }
    loop {
        l -= 1;
        if !(l > 0 as ::core::ffi::c_int) {
            break;
        }
        s = s.offset(1);
        c = *s;
        if !(c as ::core::ffi::c_int >= 0xa1 as ::core::ffi::c_int
            && c as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int)
        {
            return -(1 as ::core::ffi::c_int);
        }
    }
    return mbl;
}
unsafe fn pg_johab_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_johab_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_mule_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut mbl: ::core::ffi::c_int = 0;
    let mut c: ::core::ffi::c_uchar = 0;
    mbl = pg_mule_mblen_ptr(s);
    l = mbl;
    if len < l {
        return -(1 as ::core::ffi::c_int);
    }
    loop {
        l -= 1;
        if !(l > 0 as ::core::ffi::c_int) {
            break;
        }
        s = s.offset(1);
        c = *s;
        if c as ::core::ffi::c_int & HIGHBIT == 0 {
            return -(1 as ::core::ffi::c_int);
        }
    }
    return mbl;
}
unsafe fn pg_mule_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_mule_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_latin1_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    return 1 as ::core::ffi::c_int;
}
unsafe fn pg_latin1_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut nullpos: *const ::core::ffi::c_uchar = memchr(
        s as *const ::core::ffi::c_void,
        0 as ::core::ffi::c_int,
        len as size_t,
    ) as *const ::core::ffi::c_uchar;
    if nullpos.is_null() {
        return len;
    } else {
        return nullpos.offset_from(s) as ::core::ffi::c_long as ::core::ffi::c_int;
    };
}
unsafe fn pg_sjis_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut mbl: ::core::ffi::c_int = 0;
    let mut c1: ::core::ffi::c_uchar = 0;
    let mut c2: ::core::ffi::c_uchar = 0;
    mbl = pg_sjis_mblen(s);
    l = mbl;
    if len < l {
        return -(1 as ::core::ffi::c_int);
    }
    if l == 1 as ::core::ffi::c_int {
        return mbl;
    }
    let fresh0 = s;
    s = s.offset(1);
    c1 = *fresh0;
    c2 = *s;
    if !(c1 as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
        && c1 as ::core::ffi::c_int <= 0x9f as ::core::ffi::c_int
        || c1 as ::core::ffi::c_int >= 0xe0 as ::core::ffi::c_int
            && c1 as ::core::ffi::c_int <= 0xfc as ::core::ffi::c_int)
        || !(c2 as ::core::ffi::c_int >= 0x40 as ::core::ffi::c_int
            && c2 as ::core::ffi::c_int <= 0x7e as ::core::ffi::c_int
            || c2 as ::core::ffi::c_int >= 0x80 as ::core::ffi::c_int
                && c2 as ::core::ffi::c_int <= 0xfc as ::core::ffi::c_int)
    {
        return -(1 as ::core::ffi::c_int);
    }
    return mbl;
}
unsafe fn pg_sjis_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_sjis_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_big5_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut mbl: ::core::ffi::c_int = 0;
    mbl = pg_big5_mblen(s);
    l = mbl;
    if len < l {
        return -(1 as ::core::ffi::c_int);
    }
    if l == 2 as ::core::ffi::c_int
        && *s.offset(0 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            == NONUTF8_INVALID_BYTE0
        && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            == NONUTF8_INVALID_BYTE1
    {
        return -(1 as ::core::ffi::c_int);
    }
    loop {
        l -= 1;
        if !(l > 0 as ::core::ffi::c_int) {
            break;
        }
        s = s.offset(1);
        if *s as ::core::ffi::c_int == '\0' as i32 {
            return -(1 as ::core::ffi::c_int);
        }
    }
    return mbl;
}
unsafe fn pg_big5_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_big5_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_gbk_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut mbl: ::core::ffi::c_int = 0;
    mbl = pg_gbk_mblen(s);
    l = mbl;
    if len < l {
        return -(1 as ::core::ffi::c_int);
    }
    if l == 2 as ::core::ffi::c_int
        && *s.offset(0 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            == NONUTF8_INVALID_BYTE0
        && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            == NONUTF8_INVALID_BYTE1
    {
        return -(1 as ::core::ffi::c_int);
    }
    loop {
        l -= 1;
        if !(l > 0 as ::core::ffi::c_int) {
            break;
        }
        s = s.offset(1);
        if *s as ::core::ffi::c_int == '\0' as i32 {
            return -(1 as ::core::ffi::c_int);
        }
    }
    return mbl;
}
unsafe fn pg_gbk_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_gbk_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_uhc_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    let mut mbl: ::core::ffi::c_int = 0;
    mbl = pg_uhc_mblen(s);
    l = mbl;
    if len < l {
        return -(1 as ::core::ffi::c_int);
    }
    if l == 2 as ::core::ffi::c_int
        && *s.offset(0 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            == NONUTF8_INVALID_BYTE0
        && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            == NONUTF8_INVALID_BYTE1
    {
        return -(1 as ::core::ffi::c_int);
    }
    loop {
        l -= 1;
        if !(l > 0 as ::core::ffi::c_int) {
            break;
        }
        s = s.offset(1);
        if *s as ::core::ffi::c_int == '\0' as i32 {
            return -(1 as ::core::ffi::c_int);
        }
    }
    return mbl;
}
unsafe fn pg_uhc_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_uhc_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_gb18030_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & HIGHBIT == 0 {
        l = 1 as ::core::ffi::c_int;
    } else if len >= 4 as ::core::ffi::c_int
        && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            >= 0x30 as ::core::ffi::c_int
        && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            <= 0x39 as ::core::ffi::c_int
    {
        if *s as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
            && *s as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int
            && *s.offset(2 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                >= 0x81 as ::core::ffi::c_int
            && *s.offset(2 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                <= 0xfe as ::core::ffi::c_int
            && *s.offset(3 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                >= 0x30 as ::core::ffi::c_int
            && *s.offset(3 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                <= 0x39 as ::core::ffi::c_int
        {
            l = 4 as ::core::ffi::c_int;
        } else {
            l = -(1 as ::core::ffi::c_int);
        }
    } else if len >= 2 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int >= 0x81 as ::core::ffi::c_int
        && *s as ::core::ffi::c_int <= 0xfe as ::core::ffi::c_int
    {
        if *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
            >= 0x40 as ::core::ffi::c_int
            && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                <= 0x7e as ::core::ffi::c_int
            || *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                >= 0x80 as ::core::ffi::c_int
                && *s.offset(1 as ::core::ffi::c_int as isize) as ::core::ffi::c_int
                    <= 0xfe as ::core::ffi::c_int
        {
            l = 2 as ::core::ffi::c_int;
        } else {
            l = -(1 as ::core::ffi::c_int);
        }
    } else {
        l = -(1 as ::core::ffi::c_int);
    }
    return l;
}
unsafe fn pg_gb18030_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_gb18030_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
unsafe fn pg_utf8_verifychar(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut l: ::core::ffi::c_int = 0;
    if *s as ::core::ffi::c_int & 0x80 as ::core::ffi::c_int == 0 as ::core::ffi::c_int {
        if *s as ::core::ffi::c_int == '\0' as i32 {
            return -(1 as ::core::ffi::c_int);
        }
        return 1 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & 0xe0 as ::core::ffi::c_int == 0xc0 as ::core::ffi::c_int {
        l = 2 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & 0xf0 as ::core::ffi::c_int == 0xe0 as ::core::ffi::c_int {
        l = 3 as ::core::ffi::c_int;
    } else if *s as ::core::ffi::c_int & 0xf8 as ::core::ffi::c_int == 0xf0 as ::core::ffi::c_int {
        l = 4 as ::core::ffi::c_int;
    } else {
        l = 1 as ::core::ffi::c_int;
    }
    if l > len {
        return -(1 as ::core::ffi::c_int);
    }
    if !pg_utf8_islegal_ptr(s, l) {
        return -(1 as ::core::ffi::c_int);
    }
    return l;
}
pub const ERR: ::core::ffi::c_int = 0 as ::core::ffi::c_int;
pub const BGN: ::core::ffi::c_int = 11 as ::core::ffi::c_int;
pub const CS1: ::core::ffi::c_int = 16 as ::core::ffi::c_int;
pub const CS2: ::core::ffi::c_int = 1 as ::core::ffi::c_int;
pub const CS3: ::core::ffi::c_int = 5 as ::core::ffi::c_int;
pub const P3A: ::core::ffi::c_int = 6 as ::core::ffi::c_int;
pub const P3B: ::core::ffi::c_int = 20 as ::core::ffi::c_int;
pub const P4A: ::core::ffi::c_int = 25 as ::core::ffi::c_int;
pub const P4B: ::core::ffi::c_int = 30 as ::core::ffi::c_int;
pub const END: ::core::ffi::c_int = BGN;
pub const ASC: ::core::ffi::c_int = END << BGN;
pub const L2A: ::core::ffi::c_int = CS1 << BGN;
pub const L3A: ::core::ffi::c_int = P3A << BGN;
pub const L3B: ::core::ffi::c_int = CS2 << BGN;
pub const L3C: ::core::ffi::c_int = P3B << BGN;
pub const L4A: ::core::ffi::c_int = P4A << BGN;
pub const L4B: ::core::ffi::c_int = CS3 << BGN;
pub const L4C: ::core::ffi::c_int = P4B << BGN;
pub const CR1: ::core::ffi::c_int = END << CS1 | CS1 << CS2 | CS2 << CS3 | CS1 << P3B | CS2 << P4B;
pub const CR2: ::core::ffi::c_int = END << CS1 | CS1 << CS2 | CS2 << CS3 | CS1 << P3B | CS2 << P4A;
pub const CR3: ::core::ffi::c_int = END << CS1 | CS1 << CS2 | CS2 << CS3 | CS1 << P3A | CS2 << P4A;
pub const ILL: ::core::ffi::c_int = ERR;
static Utf8Transition: [uint32; 256] = [
    ILL as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    ASC as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR1 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR2 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    CR3 as uint32,
    ILL as uint32,
    ILL as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L2A as uint32,
    L3A as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3B as uint32,
    L3C as uint32,
    L3B as uint32,
    L3B as uint32,
    L4A as uint32,
    L4B as uint32,
    L4B as uint32,
    L4B as uint32,
    L4C as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
    ILL as uint32,
];
unsafe fn utf8_advance(
    mut s: *const ::core::ffi::c_uchar,
    mut state: *mut uint32,
    mut len: ::core::ffi::c_int,
) {
    while len > 0 as ::core::ffi::c_int {
        let fresh36 = s;
        s = s.offset(1);
        *state = Utf8Transition[*fresh36 as usize] >> (*state & 31 as uint32);
        len -= 1;
    }
    *state &= 31 as uint32;
}
unsafe fn pg_utf8_verifystr(
    mut s: *const ::core::ffi::c_uchar,
    mut len: ::core::ffi::c_int,
) -> ::core::ffi::c_int {
    let mut start: *const ::core::ffi::c_uchar = s;
    let orig_len: ::core::ffi::c_int = len;
    let mut state: uint32 = BGN as uint32;
    if len as usize >= STRIDE_LENGTH {
        while len as usize >= STRIDE_LENGTH {
            let stride = unsafe { core::slice::from_raw_parts(s, STRIDE_LENGTH) };
            if state != END as uint32 || !is_valid_ascii(stride) {
                utf8_advance(s, &raw mut state, STRIDE_LENGTH as ::core::ffi::c_int);
            }
            s = s.offset(STRIDE_LENGTH as isize);
            len = (len as ::core::ffi::c_ulong).wrapping_sub(STRIDE_LENGTH as ::core::ffi::c_ulong)
                as ::core::ffi::c_int as ::core::ffi::c_int;
        }
        if state == ERR as uint32 {
            len = orig_len;
            s = start;
        } else if state != END as uint32 {
            loop {
                s = s.offset(-1);
                len += 1;
                if !(pg_utf_mblen_private_ptr(s) <= 1 as ::core::ffi::c_int) {
                    break;
                }
            }
        }
    }
    while len > 0 as ::core::ffi::c_int {
        let mut l: ::core::ffi::c_int = 0;
        if *s as ::core::ffi::c_int & HIGHBIT == 0 {
            if *s as ::core::ffi::c_int == '\0' as i32 {
                break;
            }
            l = 1 as ::core::ffi::c_int;
        } else {
            l = pg_utf8_verifychar(s, len);
            if l == -(1 as ::core::ffi::c_int) {
                break;
            }
        }
        s = s.offset(l as isize);
        len -= l;
    }
    return s.offset_from(start) as ::core::ffi::c_long as ::core::ffi::c_int;
}
pub const STRIDE_LENGTH: usize =
    (2 as usize).wrapping_mul(::core::mem::size_of::<Vector8>() as usize);
unsafe fn pg_utf8_islegal_ptr(
    mut source: *const ::core::ffi::c_uchar,
    mut length: ::core::ffi::c_int,
) -> bool {
    let mut a: ::core::ffi::c_uchar = 0;
    let mut current_block_23: u64;
    match length {
        4 => {
            a = *source.offset(3 as ::core::ffi::c_int as isize);
            if (a as ::core::ffi::c_int) < 0x80 as ::core::ffi::c_int
                || a as ::core::ffi::c_int > 0xbf as ::core::ffi::c_int
            {
                return false_0 != 0;
            }
            current_block_23 = 6349404588491062651;
        }
        3 => {
            current_block_23 = 6349404588491062651;
        }
        2 => {
            current_block_23 = 3193882696737305577;
        }
        1 => {
            current_block_23 = 8507085208433427277;
        }
        _ => return false_0 != 0,
    }
    match current_block_23 {
        6349404588491062651 => {
            a = *source.offset(2 as ::core::ffi::c_int as isize);
            if (a as ::core::ffi::c_int) < 0x80 as ::core::ffi::c_int
                || a as ::core::ffi::c_int > 0xbf as ::core::ffi::c_int
            {
                return false_0 != 0;
            }
            current_block_23 = 3193882696737305577;
        }
        _ => {}
    }
    match current_block_23 {
        3193882696737305577 => {
            a = *source.offset(1 as ::core::ffi::c_int as isize);
            match *source as ::core::ffi::c_int {
                224 => {
                    if (a as ::core::ffi::c_int) < 0xa0 as ::core::ffi::c_int
                        || a as ::core::ffi::c_int > 0xbf as ::core::ffi::c_int
                    {
                        return false_0 != 0;
                    }
                }
                237 => {
                    if (a as ::core::ffi::c_int) < 0x80 as ::core::ffi::c_int
                        || a as ::core::ffi::c_int > 0x9f as ::core::ffi::c_int
                    {
                        return false_0 != 0;
                    }
                }
                240 => {
                    if (a as ::core::ffi::c_int) < 0x90 as ::core::ffi::c_int
                        || a as ::core::ffi::c_int > 0xbf as ::core::ffi::c_int
                    {
                        return false_0 != 0;
                    }
                }
                244 => {
                    if (a as ::core::ffi::c_int) < 0x80 as ::core::ffi::c_int
                        || a as ::core::ffi::c_int > 0x8f as ::core::ffi::c_int
                    {
                        return false_0 != 0;
                    }
                }
                _ => {
                    if (a as ::core::ffi::c_int) < 0x80 as ::core::ffi::c_int
                        || a as ::core::ffi::c_int > 0xbf as ::core::ffi::c_int
                    {
                        return false_0 != 0;
                    }
                }
            }
        }
        _ => {}
    }
    a = *source;
    if a as ::core::ffi::c_int >= 0x80 as ::core::ffi::c_int
        && (a as ::core::ffi::c_int) < 0xc2 as ::core::ffi::c_int
    {
        return false_0 != 0;
    }
    if a as ::core::ffi::c_int > 0xf4 as ::core::ffi::c_int {
        return false_0 != 0;
    }
    return true_0 != 0;
}
pub fn pg_encoding_set_invalid(encoding: ::core::ffi::c_int, dst: &mut [u8]) -> Option<()> {
    if dst.len() < 2 {
        return None;
    }

    dst[0] = if encoding == PG_UTF8 {
        0xc0
    } else {
        NONUTF8_INVALID_BYTE0 as u8
    };
    dst[1] = NONUTF8_INVALID_BYTE1 as u8;
    Some(())
}
pub static pg_wchar_table: [pg_wchar_tbl; 42] = [
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_ascii2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(pg_ascii_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_ascii_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_ascii_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_ascii_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_eucjp2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2euc_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(pg_eucjp_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_eucjp_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_eucjp_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_eucjp_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 3 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_euccn2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2euc_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(pg_euccn_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_euccn_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_euckr_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_euckr_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 3 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_euckr2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2euc_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(pg_euckr_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_euckr_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_euckr_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_euckr_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 3 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_euctw2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2euc_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(pg_euctw_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_euctw_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_euctw_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_euctw_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 4 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_eucjp2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2euc_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(pg_eucjp_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_eucjp_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_eucjp_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_eucjp_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 3 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_utf2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2utf_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_utf_mblen_private_ptr
                as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(pg_utf_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        mbverifychar: Some(
            pg_utf8_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_utf8_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 4 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_mule2wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2mule_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_mule_mblen_ptr as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_mule_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_mule_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_mule_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 4 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: Some(
            pg_latin12wchar_with_len
                as unsafe fn(
                    *const ::core::ffi::c_uchar,
                    *mut pg_wchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        wchar2mb_with_len: Some(
            pg_wchar2single_with_len
                as unsafe fn(
                    *const pg_wchar,
                    *mut ::core::ffi::c_uchar,
                    ::core::ffi::c_int,
                ) -> ::core::ffi::c_int,
        ),
        mblen: Some(
            pg_latin1_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_latin1_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_latin1_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_latin1_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 1 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_sjis_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_sjis_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_sjis_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_sjis_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 2 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_big5_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_big5_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_big5_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_big5_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 2 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_gbk_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(pg_gbk_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        mbverifychar: Some(
            pg_gbk_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_gbk_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 2 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_uhc_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(pg_uhc_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        mbverifychar: Some(
            pg_uhc_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_uhc_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 2 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(
            pg_gb18030_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        dsplen: Some(
            pg_gb18030_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_gb18030_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_gb18030_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 4 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_johab_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_johab_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_johab_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_johab_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 3 as ::core::ffi::c_int,
    },
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: Some(pg_sjis_mblen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int),
        dsplen: Some(
            pg_sjis_dsplen as unsafe fn(*const ::core::ffi::c_uchar) -> ::core::ffi::c_int,
        ),
        mbverifychar: Some(
            pg_sjis_verifychar
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        mbverifystr: Some(
            pg_sjis_verifystr
                as unsafe fn(*const ::core::ffi::c_uchar, ::core::ffi::c_int) -> ::core::ffi::c_int,
        ),
        maxmblen: 2 as ::core::ffi::c_int,
    },
];

pub fn pg_utf_mblen_private(s: &[u8]) -> Option<::core::ffi::c_int> {
    let first = s.first()?;
    let ptr = first as *const u8;
    Some(unsafe { pg_utf_mblen_private_ptr(ptr) })
}

pub fn pg_mule_mblen(s: &[u8]) -> Option<::core::ffi::c_int> {
    let first = s.first()?;
    let ptr = first as *const u8;
    Some(unsafe { pg_mule_mblen_ptr(ptr) })
}

pub fn pg_utf8_islegal(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }

    unsafe { pg_utf8_islegal_ptr(s.as_ptr(), s.len() as ::core::ffi::c_int) }
}

fn table_index(encoding: ::core::ffi::c_int) -> usize {
    if encoding >= 0 && encoding < _PG_LAST_ENCODING_ {
        encoding as usize
    } else {
        PG_SQL_ASCII as usize
    }
}

pub fn pg_encoding_mblen(encoding: ::core::ffi::c_int, mbstr: &[u8]) -> Option<::core::ffi::c_int> {
    let first = mbstr.first()?;
    let func = pg_wchar_table[table_index(encoding)].mblen?;
    let ptr = first as *const u8;
    Some(unsafe { func(ptr) })
}

pub fn pg_encoding_mblen_or_incomplete(
    encoding: ::core::ffi::c_int,
    mbstr: &[u8],
) -> ::core::ffi::c_int {
    if mbstr.is_empty()
        || encoding == PG_GB18030
            && mbstr[0] as ::core::ffi::c_int & HIGHBIT != 0
            && mbstr.len() < 2
    {
        return INT_MAX;
    }

    pg_encoding_mblen(encoding, mbstr).unwrap_or(INT_MAX)
}

pub fn pg_encoding_mblen_bounded(
    encoding: ::core::ffi::c_int,
    mbstr: &[u8],
) -> Option<::core::ffi::c_int> {
    let mblen = pg_encoding_mblen(encoding, mbstr)? as usize;
    let bounded = mbstr
        .iter()
        .take(mblen)
        .position(|byte| *byte == 0)
        .unwrap_or(mblen);
    Some(bounded as ::core::ffi::c_int)
}

pub fn pg_encoding_dsplen(
    encoding: ::core::ffi::c_int,
    mbstr: &[u8],
) -> Option<::core::ffi::c_int> {
    let first = mbstr.first()?;
    let func = pg_wchar_table[table_index(encoding)].dsplen?;
    let ptr = first as *const u8;
    Some(unsafe { func(ptr) })
}

pub fn pg_encoding_verifymbchar(encoding: ::core::ffi::c_int, mbstr: &[u8]) -> ::core::ffi::c_int {
    let func = match pg_wchar_table[table_index(encoding)].mbverifychar {
        Some(func) => func,
        None => return 0,
    };
    unsafe { func(mbstr.as_ptr(), mbstr.len() as ::core::ffi::c_int) }
}

pub fn pg_encoding_verifymbstr(encoding: ::core::ffi::c_int, mbstr: &[u8]) -> ::core::ffi::c_int {
    let func = match pg_wchar_table[table_index(encoding)].mbverifystr {
        Some(func) => func,
        None => return 0,
    };
    unsafe { func(mbstr.as_ptr(), mbstr.len() as ::core::ffi::c_int) }
}

pub fn pg_encoding_max_length(encoding: ::core::ffi::c_int) -> ::core::ffi::c_int {
    pg_wchar_table[table_index(encoding)].maxmblen
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, size_of};

    #[test]
    fn wchar_table_has_one_entry_per_encoding() {
        assert_eq!(pg_wchar_table.len(), _PG_LAST_ENCODING_ as usize);
        assert_eq!(pg_encoding_max_length(PG_SQL_ASCII), 1);
        assert_eq!(pg_encoding_max_length(PG_UTF8), 4);
        assert_eq!(pg_encoding_max_length(PG_GB18030), 4);
    }

    #[test]
    fn ffi_table_layout_is_c_compatible_shape() {
        assert_eq!(size_of::<pg_wchar>(), size_of::<u32>());
        assert_eq!(align_of::<pg_wchar>(), align_of::<u32>());
        assert_eq!(size_of::<pg_wchar_tbl>(), 56);
        assert_eq!(align_of::<pg_wchar_tbl>(), align_of::<usize>());
    }

    #[test]
    fn utf8_lengths_match_postgres_rules() {
        assert_eq!(pg_utf_mblen_private(b"a"), Some(1));
        assert_eq!(pg_utf_mblen_private("é".as_bytes()), Some(2));
        assert_eq!(pg_utf_mblen_private("€".as_bytes()), Some(3));
        assert_eq!(pg_utf_mblen_private("😀".as_bytes()), Some(4));
        assert_eq!(pg_utf_mblen_private(&[]), None);
    }

    #[test]
    fn verifies_utf8_and_rejects_invalid_sequences() {
        assert_eq!(pg_encoding_verifymbstr(PG_UTF8, "hello".as_bytes()), 5);
        assert_eq!(pg_encoding_verifymbstr(PG_UTF8, "é".as_bytes()), 2);
        assert_eq!(pg_encoding_verifymbstr(PG_UTF8, &[0xc0, b' ']), 0);
    }

    #[test]
    fn invalid_marker_matches_encoding_family() {
        let mut utf8 = [0; 2];
        pg_encoding_set_invalid(PG_UTF8, &mut utf8).unwrap();
        assert_eq!(utf8, [0xc0, b' ']);

        let mut non_utf8 = [0; 2];
        pg_encoding_set_invalid(PG_LATIN1, &mut non_utf8).unwrap();
        assert_eq!(non_utf8, [0x8d, b' ']);
    }

    #[test]
    fn bounded_lengths_stop_at_nul() {
        assert_eq!(pg_encoding_mblen(PG_UTF8, b"\0abc"), Some(1));
        assert_eq!(pg_encoding_mblen_bounded(PG_UTF8, b"\0abc"), Some(0));
        assert_eq!(pg_encoding_mblen_or_incomplete(PG_GB18030, &[]), i32::MAX);
    }
}
