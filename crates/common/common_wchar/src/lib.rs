//! Idiomatic port of PostgreSQL's `src/common/wchar.c`.
//!
//! This is a safe-Rust rewrite of the multibyte/wide-character routines.  All
//! routines that the C code expresses with raw `const unsigned char *` /
//! `pg_wchar *` pointers are expressed here with byte slices (`&[u8]`) and
//! wide-character slices (`&[pg_wchar]` / `&mut [pg_wchar]`).  Behavior,
//! including byte counting, NUL termination, and the exact accept/reject rules
//! of every encoding, is preserved from `postgres-18.3`.
//!
//! No raw pointers, `extern "C"`, `c_void`, or `libc` are used: the only place
//! the C uses `memchr` (the ASCII / LATIN1 string verifiers) is replaced with a
//! safe slice scan.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

// Re-export the encoding identifiers and the wide-character vocabulary so
// callers of this crate can name them without also depending on `types`
// directly, matching the source crate's re-export surface.
pub use ::types_wchar::encoding::{
    pg_enc, PG_BIG5, PG_EUC_CN, PG_EUC_JIS_2004, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030,
    PG_GBK, PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_JOHAB, PG_KOI8R,
    PG_KOI8U, PG_LATIN1, PG_LATIN10, PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6,
    PG_LATIN7, PG_LATIN8, PG_LATIN9, PG_MULE_INTERNAL, PG_SHIFT_JIS_2004, PG_SJIS, PG_SQL_ASCII,
    PG_UHC, PG_UTF8, PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255,
    PG_WIN1256, PG_WIN1257, PG_WIN1258, PG_WIN866, PG_WIN874, _PG_LAST_ENCODING_,
};
pub use ::types_wchar::wchar::{mbinterval, pg_wchar};

// ---------------------------------------------------------------------------
// Converter function-pointer vocabulary (idiomatic, slice based).
//
// The C `pg_wchar_tbl` stores six function-pointer slots; we mirror that with
// Rust function pointers that take slices instead of raw pointers.  Keeping
// them as plain `fn` pointers (not boxed closures) preserves the table's
// `'static` and `Copy`/`Sync` nature and its C-equivalent size/alignment.
// ---------------------------------------------------------------------------

/// Convert a multibyte string to a wide-character string.
///
/// `from` is the (NUL-terminated) input bounded by `from.len()` available bytes;
/// `len` is the byte budget the C API was passed.  Writes into `to` (which the
/// caller sizes large enough) and returns the number of wide characters
/// produced, exactly like the C `mb2wchar_with_len_converter`.
pub type Mb2WcharWithLenConverter = fn(from: &[u8], to: &mut [pg_wchar], len: i32) -> i32;

/// Convert a wide-character string to a multibyte string.
pub type Wchar2MbWithLenConverter = fn(from: &[pg_wchar], to: &mut [u8], len: i32) -> i32;

/// Return the byte length of the leading multibyte character of `mbstr`.
pub type MblenConverter = fn(mbstr: &[u8]) -> i32;

/// Return the display width of the leading multibyte character of `mbstr`.
pub type MbdisplaylenConverter = fn(mbstr: &[u8]) -> i32;

/// Verify a single multibyte character, returning its length or -1.
pub type MbcharVerifier = fn(mbstr: &[u8], len: i32) -> i32;

/// Verify a multibyte string, returning the number of valid leading bytes.
pub type MbstrVerifier = fn(mbstr: &[u8], len: i32) -> i32;

/// Per-encoding conversion routines (`pg_wchar_tbl` in `pg_wchar.h`).
///
/// All slots are non-optional here, unlike the C structure where the
/// `mb2wchar_with_len` and `wchar2mb_with_len` routines are `NULL` for the
/// client-only encodings (SJIS, BIG5, GBK, UHC, GB18030, JOHAB,
/// SHIFT_JIS_2004).  Because Rust function pointers cannot be null, the table
/// below populates those two slots with [`pg_mb2wchar_unreachable`] /
/// [`pg_wchar2mb_unreachable`], which panic if ever called.  This preserves the
/// C contract that those routines are never dispatched (a client-only encoding
/// is never the server encoding, so `pg_mb2wchar_with_len` cannot reach them)
/// while turning the C NULL-deref into a loud, descriptive panic.  Every other
/// slot mirrors `pg_wchar_table` in `wchar.c` exactly.
#[derive(Clone, Copy)]
pub struct pg_wchar_tbl {
    pub mb2wchar_with_len: Mb2WcharWithLenConverter,
    pub wchar2mb_with_len: Wchar2MbWithLenConverter,
    pub mblen: MblenConverter,
    pub dsplen: MbdisplaylenConverter,
    pub mbverifychar: MbcharVerifier,
    pub mbverifystr: MbstrVerifier,
    pub maxmblen: i32,
}

// ---------------------------------------------------------------------------
// Constants from wchar.c / pg_wchar.h.
// ---------------------------------------------------------------------------

const HIGHBIT: u8 = 0x80;
const SS2: i32 = 0x8e;
const SS3: i32 = 0x8f;
const LCPRV1_A: i32 = 0x9a;
const LCPRV1_B: i32 = 0x9b;
const LCPRV2_A: i32 = 0x9c;
const LCPRV2_B: i32 = 0x9d;

/// Invalid-marker bytes used by `pg_encoding_set_invalid` and the CJK verifiers
/// (`NONUTF8_INVALID_BYTE0`/`BYTE1` in wchar.c).
const NONUTF8_INVALID_BYTE0: i32 = 0x8d;
const NONUTF8_INVALID_BYTE1: i32 = b' ' as i32;

/// `INT_MAX`, returned by `pg_encoding_mblen_or_incomplete` for incomplete data.
pub const INT_MAX: i32 = i32::MAX;

// ---------------------------------------------------------------------------
// SIMD-free 16-byte "vector" helpers (Vector8) used by the UTF-8 fast path.
// ---------------------------------------------------------------------------

type Vector8 = [u8; 16];

/// Two `Vector8`s worth of bytes are scanned per stride in the UTF-8 verifier.
pub const STRIDE_LENGTH: usize = 2 * core::mem::size_of::<Vector8>();

#[inline]
fn is_valid_ascii(s: &[u8]) -> bool {
    for &byte in s {
        if byte == 0 || byte & 0x80 != 0 {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// UTF-8 <-> Unicode scalar helpers (utf8_to_unicode / unicode_to_utf8).
// ---------------------------------------------------------------------------

/// Decode the leading UTF-8 sequence at `c` into a Unicode code point.
///
/// `c` must have at least as many bytes as the leading byte's length implies
/// (the C version reads exactly that many bytes and never checks the length).
#[inline]
fn utf8_to_unicode(c: &[u8]) -> pg_wchar {
    let b0 = c[0] as i32;
    if b0 & 0x80 == 0 {
        c[0] as pg_wchar
    } else if b0 & 0xe0 == 0xc0 {
        (((c[0] as i32 & 0x1f) << 6) | (c[1] as i32 & 0x3f)) as pg_wchar
    } else if b0 & 0xf0 == 0xe0 {
        (((c[0] as i32 & 0x0f) << 12) | ((c[1] as i32 & 0x3f) << 6) | (c[2] as i32 & 0x3f))
            as pg_wchar
    } else if b0 & 0xf8 == 0xf0 {
        (((c[0] as i32 & 0x07) << 18)
            | ((c[1] as i32 & 0x3f) << 12)
            | ((c[2] as i32 & 0x3f) << 6)
            | (c[3] as i32 & 0x3f)) as pg_wchar
    } else {
        0xffffffff
    }
}

/// Encode `c` as UTF-8 into the start of `utf8string`.
///
/// `utf8string` must be large enough for the encoding (1-4 bytes); the C code
/// writes without bounds checks.
#[inline]
fn unicode_to_utf8(c: pg_wchar, utf8string: &mut [u8]) {
    if c <= 0x7f {
        utf8string[0] = c as u8;
    } else if c <= 0x7ff {
        utf8string[0] = (0xc0 | ((c >> 6) & 0x1f)) as u8;
        utf8string[1] = (0x80 | (c & 0x3f)) as u8;
    } else if c <= 0xffff {
        utf8string[0] = (0xe0 | ((c >> 12) & 0x0f)) as u8;
        utf8string[1] = (0x80 | ((c >> 6) & 0x3f)) as u8;
        utf8string[2] = (0x80 | (c & 0x3f)) as u8;
    } else {
        utf8string[0] = (0xf0 | ((c >> 18) & 0x07)) as u8;
        utf8string[1] = (0x80 | ((c >> 12) & 0x3f)) as u8;
        utf8string[2] = (0x80 | ((c >> 6) & 0x3f)) as u8;
        utf8string[3] = (0x80 | (c & 0x3f)) as u8;
    }
}

#[inline]
fn pg_utf_mblen_byte(s: u8) -> i32 {
    let b = s as i32;
    if b & 0x80 == 0 {
        1
    } else if b & 0xe0 == 0xc0 {
        2
    } else if b & 0xf0 == 0xe0 {
        3
    } else if b & 0xf8 == 0xf0 {
        4
    } else {
        1
    }
}

// ---------------------------------------------------------------------------
// SQL_ASCII
// ---------------------------------------------------------------------------

fn pg_ascii2wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        to[ti] = from[fi] as pg_wchar;
        fi += 1;
        ti += 1;
        len -= 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_ascii_mblen(_s: &[u8]) -> i32 {
    1
}

fn pg_ascii_dsplen(s: &[u8]) -> i32 {
    let c = s[0] as i32;
    if c == 0 {
        return 0;
    }
    if c < 0x20 || c == 0x7f {
        return -1;
    }
    1
}

// ---------------------------------------------------------------------------
// EUC family
// ---------------------------------------------------------------------------

fn pg_euc2wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi] as i32;
        if b == SS2 {
            if len < 2 {
                break;
            }
            fi += 1; // skip SS2
            to[ti] = ((SS2 << 8) | from[fi] as i32) as pg_wchar;
            fi += 1;
            len -= 2;
        } else if b == SS3 {
            if len < 3 {
                break;
            }
            fi += 1; // skip SS3
            to[ti] = ((SS3 << 16) | ((from[fi] as i32) << 8)) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 3;
        } else if (b as u8) & HIGHBIT != 0 {
            if len < 2 {
                break;
            }
            to[ti] = ((from[fi] as i32) << 8) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 2;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

#[inline]
fn pg_euc_mblen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if b == SS2 {
        2
    } else if b == SS3 {
        3
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

#[inline]
fn pg_euc_dsplen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if b == SS2 {
        2
    } else if b == SS3 {
        2
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

// EUC_JP

fn pg_eucjp2wchar_with_len(from: &[u8], to: &mut [pg_wchar], len: i32) -> i32 {
    pg_euc2wchar_with_len(from, to, len)
}

fn pg_eucjp_mblen(s: &[u8]) -> i32 {
    pg_euc_mblen(s)
}

fn pg_eucjp_dsplen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if b == SS2 {
        1
    } else if b == SS3 {
        2
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

// EUC_KR

fn pg_euckr2wchar_with_len(from: &[u8], to: &mut [pg_wchar], len: i32) -> i32 {
    pg_euc2wchar_with_len(from, to, len)
}

fn pg_euckr_mblen(s: &[u8]) -> i32 {
    pg_euc_mblen(s)
}

fn pg_euckr_dsplen(s: &[u8]) -> i32 {
    pg_euc_dsplen(s)
}

// EUC_CN

fn pg_euccn2wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi] as i32;
        if b == SS2 {
            if len < 3 {
                break;
            }
            fi += 1; // skip SS2
            to[ti] = ((SS2 << 16) | ((from[fi] as i32) << 8)) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 3;
        } else if b == SS3 {
            if len < 3 {
                break;
            }
            fi += 1; // skip SS3
            to[ti] = ((SS3 << 16) | ((from[fi] as i32) << 8)) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 3;
        } else if (b as u8) & HIGHBIT != 0 {
            if len < 2 {
                break;
            }
            to[ti] = ((from[fi] as i32) << 8) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 2;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_euccn_mblen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if b == SS2 {
        3
    } else if b == SS3 {
        3
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

fn pg_euccn_dsplen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

// EUC_TW

fn pg_euctw2wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi] as i32;
        if b == SS2 {
            if len < 4 {
                break;
            }
            fi += 1; // skip SS2
            to[ti] = ((SS2 as u32) << 24 | (((from[fi] as i32) << 16) as u32)) as pg_wchar;
            fi += 1;
            to[ti] |= ((from[fi] as i32) << 8) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 4;
        } else if b == SS3 {
            if len < 3 {
                break;
            }
            fi += 1; // skip SS3
            to[ti] = ((SS3 << 16) | ((from[fi] as i32) << 8)) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 3;
        } else if (b as u8) & HIGHBIT != 0 {
            if len < 2 {
                break;
            }
            to[ti] = ((from[fi] as i32) << 8) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 2;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_euctw_mblen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if b == SS2 {
        4
    } else if b == SS3 {
        3
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

fn pg_euctw_dsplen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if b == SS2 {
        2
    } else if b == SS3 {
        2
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

/// Convert a wide-character (EUC) string to multibyte (`pg_wchar2euc_with_len`).
fn pg_wchar2euc_with_len(from: &[pg_wchar], to: &mut [u8], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let w = from[fi];
        let c = (w >> 24) as u8;
        if c != 0 {
            to[ti] = c;
            ti += 1;
            to[ti] = ((w >> 16) & 0xff) as u8;
            ti += 1;
            to[ti] = ((w >> 8) & 0xff) as u8;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 4;
        } else {
            let c = (w >> 16) as u8;
            if c != 0 {
                to[ti] = c;
                ti += 1;
                to[ti] = ((w >> 8) & 0xff) as u8;
                ti += 1;
                to[ti] = (w & 0xff) as u8;
                ti += 1;
                cnt += 3;
            } else {
                let c = (w >> 8) as u8;
                if c != 0 {
                    to[ti] = c;
                    ti += 1;
                    to[ti] = (w & 0xff) as u8;
                    ti += 1;
                    cnt += 2;
                } else {
                    to[ti] = w as u8;
                    ti += 1;
                    cnt += 1;
                }
            }
        }
        fi += 1;
        len -= 1;
    }
    to[ti] = 0;
    cnt
}

// JOHAB shares EUC mblen/dsplen.

fn pg_johab_mblen(s: &[u8]) -> i32 {
    pg_euc_mblen(s)
}

fn pg_johab_dsplen(s: &[u8]) -> i32 {
    pg_euc_dsplen(s)
}

// ---------------------------------------------------------------------------
// UTF-8
// ---------------------------------------------------------------------------

fn pg_utf2wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi] as i32;
        if b & 0x80 == 0 {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        } else if b & 0xe0 == 0xc0 {
            if len < 2 {
                break;
            }
            let c1 = (from[fi] as i32 & 0x1f) as u32;
            fi += 1;
            let c2 = (from[fi] as i32 & 0x3f) as u32;
            fi += 1;
            to[ti] = ((c1 << 6) | c2) as pg_wchar;
            len -= 2;
        } else if b & 0xf0 == 0xe0 {
            if len < 3 {
                break;
            }
            let c1 = (from[fi] as i32 & 0x0f) as u32;
            fi += 1;
            let c2 = (from[fi] as i32 & 0x3f) as u32;
            fi += 1;
            let c3 = (from[fi] as i32 & 0x3f) as u32;
            fi += 1;
            to[ti] = ((c1 << 12) | (c2 << 6) | c3) as pg_wchar;
            len -= 3;
        } else if b & 0xf8 == 0xf0 {
            if len < 4 {
                break;
            }
            let c1 = (from[fi] as i32 & 0x07) as u32;
            fi += 1;
            let c2 = (from[fi] as i32 & 0x3f) as u32;
            fi += 1;
            let c3 = (from[fi] as i32 & 0x3f) as u32;
            fi += 1;
            let c4 = (from[fi] as i32 & 0x3f) as u32;
            fi += 1;
            to[ti] = ((c1 << 18) | (c2 << 12) | (c3 << 6) | c4) as pg_wchar;
            len -= 4;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_wchar2utf_with_len(from: &[pg_wchar], to: &mut [u8], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        unicode_to_utf8(from[fi], &mut to[ti..]);
        let char_len = pg_utf_mblen_byte(to[ti]);
        cnt += char_len;
        ti += char_len as usize;
        fi += 1;
        len -= 1;
    }
    to[ti] = 0;
    cnt
}

// ---------------------------------------------------------------------------
// Display-width tables and lookup (ucs_wcwidth / mbbisearch).
// ---------------------------------------------------------------------------

fn mbbisearch(ucs: pg_wchar, table: &[mbinterval]) -> i32 {
    let mut min: i32 = 0;
    let mut max = table.len() as i32 - 1;
    if table.is_empty() || ucs < table[0].first || ucs > table[max as usize].last {
        return 0;
    }
    while max >= min {
        let mid = (min + max) / 2;
        let interval = table[mid as usize];
        if ucs > interval.last {
            min = mid + 1;
        } else if ucs < interval.first {
            max = mid - 1;
        } else {
            return 1;
        }
    }
    0
}

fn ucs_wcwidth(ucs: pg_wchar) -> i32 {
    if ucs == 0 {
        return 0;
    }
    if ucs < 0x20 || (ucs >= 0x7f && ucs < 0xa0) || ucs > 0x10ffff {
        return -1;
    }
    if mbbisearch(ucs, &NONSPACING) != 0 {
        return 0;
    }
    if mbbisearch(ucs, &EAST_ASIAN_FW) != 0 {
        return 2;
    }
    1
}

fn pg_utf_dsplen(s: &[u8]) -> i32 {
    ucs_wcwidth(utf8_to_unicode(s))
}

// ---------------------------------------------------------------------------
// MULE_INTERNAL
// ---------------------------------------------------------------------------

fn pg_mule2wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi] as i32;
        if (0x81..=0x8d).contains(&b) {
            if len < 2 {
                break;
            }
            to[ti] = ((from[fi] as i32) << 16) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 2;
        } else if b == LCPRV1_A || b == LCPRV1_B {
            if len < 3 {
                break;
            }
            fi += 1; // skip LCPRV1
            to[ti] = ((from[fi] as i32) << 16) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 3;
        } else if (0x90..=0x99).contains(&b) {
            if len < 3 {
                break;
            }
            to[ti] = ((from[fi] as i32) << 16) as pg_wchar;
            fi += 1;
            to[ti] |= ((from[fi] as i32) << 8) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 3;
        } else if b == LCPRV2_A || b == LCPRV2_B {
            if len < 4 {
                break;
            }
            fi += 1; // skip LCPRV2
            to[ti] = ((from[fi] as i32) << 16) as pg_wchar;
            fi += 1;
            to[ti] |= ((from[fi] as i32) << 8) as pg_wchar;
            fi += 1;
            to[ti] |= from[fi] as pg_wchar;
            fi += 1;
            len -= 4;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_wchar2mule_with_len(from: &[pg_wchar], to: &mut [u8], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let w = from[fi];
        let lb = ((w >> 16) & 0xff) as u8;
        let lbi = lb as i32;
        if (0x81..=0x8d).contains(&lbi) {
            to[ti] = lb;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 2;
        } else if (0x90..=0x99).contains(&lbi) {
            to[ti] = lb;
            ti += 1;
            to[ti] = ((w >> 8) & 0xff) as u8;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 3;
        } else if (0xa0..=0xdf).contains(&lbi) {
            to[ti] = LCPRV1_A as u8;
            ti += 1;
            to[ti] = lb;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 3;
        } else if (0xe0..=0xef).contains(&lbi) {
            to[ti] = LCPRV1_B as u8;
            ti += 1;
            to[ti] = lb;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 3;
        } else if (0xf0..=0xf4).contains(&lbi) {
            to[ti] = LCPRV2_A as u8;
            ti += 1;
            to[ti] = lb;
            ti += 1;
            to[ti] = ((w >> 8) & 0xff) as u8;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 4;
        } else if (0xf5..=0xfe).contains(&lbi) {
            to[ti] = LCPRV2_B as u8;
            ti += 1;
            to[ti] = lb;
            ti += 1;
            to[ti] = ((w >> 8) & 0xff) as u8;
            ti += 1;
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 4;
        } else {
            to[ti] = (w & 0xff) as u8;
            ti += 1;
            cnt += 1;
        }
        fi += 1;
        len -= 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_mule_mblen_byte(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if (0x81..=0x8d).contains(&b) {
        2
    } else if b == LCPRV1_A || b == LCPRV1_B {
        3
    } else if (0x90..=0x99).contains(&b) {
        3
    } else if b == LCPRV2_A || b == LCPRV2_B {
        4
    } else {
        1
    }
}

fn pg_mule_dsplen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if (0x81..=0x8d).contains(&b) {
        1
    } else if b == LCPRV1_A || b == LCPRV1_B {
        1
    } else if (0x90..=0x99).contains(&b) {
        2
    } else if b == LCPRV2_A || b == LCPRV2_B {
        2
    } else {
        1
    }
}

// ---------------------------------------------------------------------------
// Single-byte encodings (LATIN1 etc.)
// ---------------------------------------------------------------------------

fn pg_latin12wchar_with_len(from: &[u8], to: &mut [pg_wchar], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        to[ti] = from[fi] as pg_wchar;
        fi += 1;
        ti += 1;
        len -= 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_wchar2single_with_len(from: &[pg_wchar], to: &mut [u8], mut len: i32) -> i32 {
    let mut cnt = 0;
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        to[ti] = from[fi] as u8;
        fi += 1;
        ti += 1;
        len -= 1;
        cnt += 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_latin1_mblen(_s: &[u8]) -> i32 {
    1
}

fn pg_latin1_dsplen(s: &[u8]) -> i32 {
    pg_ascii_dsplen(s)
}

// ---------------------------------------------------------------------------
// SJIS / BIG5 / GBK / UHC / GB18030 (client-only) mblen + dsplen
// ---------------------------------------------------------------------------

fn pg_sjis_mblen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if (0xa1..=0xdf).contains(&b) {
        1
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

fn pg_sjis_dsplen(s: &[u8]) -> i32 {
    let b = s[0] as i32;
    if (0xa1..=0xdf).contains(&b) {
        1
    } else if (b as u8) & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_big5_mblen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

fn pg_big5_dsplen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_gbk_mblen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

fn pg_gbk_dsplen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_uhc_mblen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        1
    }
}

fn pg_uhc_dsplen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_gb18030_mblen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT == 0 {
        1
    } else {
        // The C code dereferences s[1] unconditionally here; callers guarantee a
        // valid GB18030 string with the lookahead byte present.  We read it
        // defensively (treating an absent byte as 0, i.e. not in 0x30..=0x39).
        let s1 = s.get(1).copied().unwrap_or(0) as i32;
        if (0x30..=0x39).contains(&s1) {
            4
        } else {
            2
        }
    }
}

fn pg_gb18030_dsplen(s: &[u8]) -> i32 {
    if s[0] & HIGHBIT != 0 {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

// ---------------------------------------------------------------------------
// Verifiers
// ---------------------------------------------------------------------------

/// Scan `s[..len]` for the first NUL, returning its index or `len`.  Replaces the
/// C `memchr` used by the ASCII / LATIN1 string verifiers.
fn nul_pos(s: &[u8], len: i32) -> i32 {
    let len = len as usize;
    match s[..len].iter().position(|&b| b == 0) {
        Some(p) => p as i32,
        None => len as i32,
    }
}

fn pg_ascii_verifychar(_s: &[u8], _len: i32) -> i32 {
    1
}

fn pg_ascii_verifystr(s: &[u8], len: i32) -> i32 {
    nul_pos(s, len)
}

fn pg_eucjp_verifychar(s: &[u8], len: i32) -> i32 {
    let mut idx = 0;
    let c1 = s[idx];
    idx += 1;
    let l;
    match c1 as i32 {
        x if x == SS2 => {
            l = 2;
            if l > len {
                return -1;
            }
            let c2 = s[idx];
            if (c2 as i32) < 0xa1 || c2 as i32 > 0xdf {
                return -1;
            }
        }
        x if x == SS3 => {
            l = 3;
            if l > len {
                return -1;
            }
            let c2 = s[idx];
            idx += 1;
            if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
                return -1;
            }
            let c2 = s[idx];
            if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
                return -1;
            }
        }
        _ => {
            if c1 & HIGHBIT != 0 {
                l = 2;
                if l > len {
                    return -1;
                }
                if !(c1 as i32 >= 0xa1 && c1 as i32 <= 0xfe) {
                    return -1;
                }
                let c2 = s[idx];
                if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
                    return -1;
                }
            } else {
                l = 1;
            }
        }
    }
    l
}

fn pg_eucjp_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_eucjp_verifychar)
}

fn pg_euckr_verifychar(s: &[u8], len: i32) -> i32 {
    let mut idx = 0;
    let c1 = s[idx];
    idx += 1;
    let l;
    if c1 & HIGHBIT != 0 {
        l = 2;
        if l > len {
            return -1;
        }
        if !(c1 as i32 >= 0xa1 && c1 as i32 <= 0xfe) {
            return -1;
        }
        let c2 = s[idx];
        if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
            return -1;
        }
    } else {
        l = 1;
    }
    l
}

fn pg_euckr_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_euckr_verifychar)
}

fn pg_euctw_verifychar(s: &[u8], len: i32) -> i32 {
    let mut idx = 0;
    let c1 = s[idx];
    idx += 1;
    let l;
    match c1 as i32 {
        x if x == SS2 => {
            l = 4;
            if l > len {
                return -1;
            }
            let c2 = s[idx];
            idx += 1;
            if (c2 as i32) < 0xa1 || c2 as i32 > 0xa7 {
                return -1;
            }
            let c2 = s[idx];
            idx += 1;
            if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
                return -1;
            }
            let c2 = s[idx];
            if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
                return -1;
            }
        }
        x if x == SS3 => return -1,
        _ => {
            if c1 & HIGHBIT != 0 {
                l = 2;
                if l > len {
                    return -1;
                }
                let c2 = s[idx];
                if !(c2 as i32 >= 0xa1 && c2 as i32 <= 0xfe) {
                    return -1;
                }
            } else {
                l = 1;
            }
        }
    }
    l
}

fn pg_euctw_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_euctw_verifychar)
}

fn pg_johab_verifychar(s: &[u8], len: i32) -> i32 {
    let mbl = pg_johab_mblen(s);
    let mut l = mbl;
    if len < l {
        return -1;
    }
    if s[0] & HIGHBIT == 0 {
        return mbl;
    }
    let mut idx = 0;
    loop {
        l -= 1;
        if !(l > 0) {
            break;
        }
        idx += 1;
        let c = s[idx];
        if !(c as i32 >= 0xa1 && c as i32 <= 0xfe) {
            return -1;
        }
    }
    mbl
}

fn pg_johab_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_johab_verifychar)
}

fn pg_mule_verifychar(s: &[u8], len: i32) -> i32 {
    let mbl = pg_mule_mblen_byte(s);
    let mut l = mbl;
    if len < l {
        return -1;
    }
    let mut idx = 0;
    loop {
        l -= 1;
        if !(l > 0) {
            break;
        }
        idx += 1;
        let c = s[idx];
        if c & HIGHBIT == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_mule_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_mule_verifychar)
}

fn pg_latin1_verifychar(_s: &[u8], _len: i32) -> i32 {
    1
}

fn pg_latin1_verifystr(s: &[u8], len: i32) -> i32 {
    nul_pos(s, len)
}

fn pg_sjis_verifychar(s: &[u8], len: i32) -> i32 {
    let mbl = pg_sjis_mblen(s);
    let l = mbl;
    if len < l {
        return -1;
    }
    if l == 1 {
        return mbl;
    }
    let c1 = s[0];
    let c2 = s[1];
    if !((c1 as i32 >= 0x81 && c1 as i32 <= 0x9f) || (c1 as i32 >= 0xe0 && c1 as i32 <= 0xfc))
        || !((c2 as i32 >= 0x40 && c2 as i32 <= 0x7e) || (c2 as i32 >= 0x80 && c2 as i32 <= 0xfc))
    {
        return -1;
    }
    mbl
}

fn pg_sjis_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_sjis_verifychar)
}

fn pg_big5_verifychar(s: &[u8], len: i32) -> i32 {
    let mbl = pg_big5_mblen(s);
    let mut l = mbl;
    if len < l {
        return -1;
    }
    if l == 2 && s[0] as i32 == NONUTF8_INVALID_BYTE0 && s[1] as i32 == NONUTF8_INVALID_BYTE1 {
        return -1;
    }
    let mut idx = 0;
    loop {
        l -= 1;
        if !(l > 0) {
            break;
        }
        idx += 1;
        if s[idx] == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_big5_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_big5_verifychar)
}

fn pg_gbk_verifychar(s: &[u8], len: i32) -> i32 {
    let mbl = pg_gbk_mblen(s);
    let mut l = mbl;
    if len < l {
        return -1;
    }
    if l == 2 && s[0] as i32 == NONUTF8_INVALID_BYTE0 && s[1] as i32 == NONUTF8_INVALID_BYTE1 {
        return -1;
    }
    let mut idx = 0;
    loop {
        l -= 1;
        if !(l > 0) {
            break;
        }
        idx += 1;
        if s[idx] == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_gbk_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_gbk_verifychar)
}

fn pg_uhc_verifychar(s: &[u8], len: i32) -> i32 {
    let mbl = pg_uhc_mblen(s);
    let mut l = mbl;
    if len < l {
        return -1;
    }
    if l == 2 && s[0] as i32 == NONUTF8_INVALID_BYTE0 && s[1] as i32 == NONUTF8_INVALID_BYTE1 {
        return -1;
    }
    let mut idx = 0;
    loop {
        l -= 1;
        if !(l > 0) {
            break;
        }
        idx += 1;
        if s[idx] == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_uhc_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_uhc_verifychar)
}

fn pg_gb18030_verifychar(s: &[u8], len: i32) -> i32 {
    if s[0] & HIGHBIT == 0 {
        1
    } else if len >= 4 && (0x30..=0x39).contains(&(s[1] as i32)) {
        if (0x81..=0xfe).contains(&(s[0] as i32))
            && (0x81..=0xfe).contains(&(s[2] as i32))
            && (0x30..=0x39).contains(&(s[3] as i32))
        {
            4
        } else {
            -1
        }
    } else if len >= 2 && (0x81..=0xfe).contains(&(s[0] as i32)) {
        if (0x40..=0x7e).contains(&(s[1] as i32)) || (0x80..=0xfe).contains(&(s[1] as i32)) {
            2
        } else {
            -1
        }
    } else {
        -1
    }
}

fn pg_gb18030_verifystr(s: &[u8], len: i32) -> i32 {
    verify_str(s, len, pg_gb18030_verifychar)
}

fn pg_utf8_verifychar(s: &[u8], len: i32) -> i32 {
    let b = s[0] as i32;
    let l;
    if b & 0x80 == 0 {
        if b == 0 {
            return -1;
        }
        return 1;
    } else if b & 0xe0 == 0xc0 {
        l = 2;
    } else if b & 0xf0 == 0xe0 {
        l = 3;
    } else if b & 0xf8 == 0xf0 {
        l = 4;
    } else {
        l = 1;
    }
    if l > len {
        return -1;
    }
    if !pg_utf8_islegal_bytes(s, l) {
        return -1;
    }
    l
}

// ---------------------------------------------------------------------------
// Shared multibyte string verifier (matches every *_verifystr in wchar.c).
//
// The per-encoding string verifiers are byte-for-byte identical except for the
// character verifier they call, so we factor the loop into one helper.
// ---------------------------------------------------------------------------

fn verify_str(s: &[u8], mut len: i32, verifychar: MbcharVerifier) -> i32 {
    let mut pos = 0usize;
    while len > 0 {
        let l;
        if s[pos] & HIGHBIT == 0 {
            if s[pos] == 0 {
                break;
            }
            l = 1;
        } else {
            l = verifychar(&s[pos..], len);
            if l == -1 {
                break;
            }
        }
        pos += l as usize;
        len -= l;
    }
    pos as i32
}

// ---------------------------------------------------------------------------
// UTF-8 string verifier with the wchar.c "Utf8Transition" fast path.
// ---------------------------------------------------------------------------

fn utf8_advance(s: &[u8], state: &mut u32, len: i32) {
    let mut idx = 0usize;
    let mut remaining = len;
    while remaining > 0 {
        *state = UTF8_TRANSITION[s[idx] as usize] >> (*state & 31);
        idx += 1;
        remaining -= 1;
    }
    *state &= 31;
}

fn pg_utf8_verifystr(s: &[u8], mut len: i32) -> i32 {
    let orig_len = len;
    let mut pos = 0usize;
    let mut state: u32 = BGN as u32;
    if len as usize >= STRIDE_LENGTH {
        while len as usize >= STRIDE_LENGTH {
            let stride = &s[pos..pos + STRIDE_LENGTH];
            if state != END as u32 || !is_valid_ascii(stride) {
                utf8_advance(stride, &mut state, STRIDE_LENGTH as i32);
            }
            pos += STRIDE_LENGTH;
            len -= STRIDE_LENGTH as i32;
        }
        if state == ERR as u32 {
            len = orig_len;
            pos = 0;
        } else if state != END as u32 {
            // Back up to the start of the incomplete trailing character.
            loop {
                pos -= 1;
                len += 1;
                if !(pg_utf_mblen_byte(s[pos]) <= 1) {
                    break;
                }
            }
        }
    }
    while len > 0 {
        let l;
        if s[pos] & HIGHBIT == 0 {
            if s[pos] == 0 {
                break;
            }
            l = 1;
        } else {
            l = pg_utf8_verifychar(&s[pos..], len);
            if l == -1 {
                break;
            }
        }
        pos += l as usize;
        len -= l;
    }
    pos as i32
}

// ---------------------------------------------------------------------------
// UTF-8 legality check (pg_utf8_islegal).
// ---------------------------------------------------------------------------

fn pg_utf8_islegal_bytes(source: &[u8], length: i32) -> bool {
    let mut check_byte2 = false;
    match length {
        4 => {
            let a = source[3];
            if (a as i32) < 0x80 || a as i32 > 0xbf {
                return false;
            }
            // fallthrough to byte 3 check
            let a = source[2];
            if (a as i32) < 0x80 || a as i32 > 0xbf {
                return false;
            }
            check_byte2 = true;
        }
        3 => {
            let a = source[2];
            if (a as i32) < 0x80 || a as i32 > 0xbf {
                return false;
            }
            check_byte2 = true;
        }
        2 => {
            check_byte2 = true;
        }
        1 => {}
        _ => return false,
    }
    if check_byte2 {
        let a = source[1];
        match source[0] as i32 {
            224 => {
                if (a as i32) < 0xa0 || a as i32 > 0xbf {
                    return false;
                }
            }
            237 => {
                if (a as i32) < 0x80 || a as i32 > 0x9f {
                    return false;
                }
            }
            240 => {
                if (a as i32) < 0x90 || a as i32 > 0xbf {
                    return false;
                }
            }
            244 => {
                if (a as i32) < 0x80 || a as i32 > 0x8f {
                    return false;
                }
            }
            _ => {
                if (a as i32) < 0x80 || a as i32 > 0xbf {
                    return false;
                }
            }
        }
    }
    let a = source[0];
    if a as i32 >= 0x80 && (a as i32) < 0xc2 {
        return false;
    }
    if a as i32 > 0xf4 {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Utf8Transition state-machine constants (wchar.c).
// ---------------------------------------------------------------------------

const ERR: i32 = 0;
const BGN: i32 = 11;
const CS1: i32 = 16;
const CS2: i32 = 1;
const CS3: i32 = 5;
const P3A: i32 = 6;
const P3B: i32 = 20;
const P4A: i32 = 25;
const P4B: i32 = 30;
const END: i32 = BGN;
const ASC: i32 = END << BGN;
const L2A: i32 = CS1 << BGN;
const L3A: i32 = P3A << BGN;
const L3B: i32 = CS2 << BGN;
const L3C: i32 = P3B << BGN;
const L4A: i32 = P4A << BGN;
const L4B: i32 = CS3 << BGN;
const L4C: i32 = P4B << BGN;
const CR1: i32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4B);
const CR2: i32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4A);
const CR3: i32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3A) | (CS2 << P4A);
const ILL: i32 = ERR;

// ---------------------------------------------------------------------------
// Public, slice-based dispatch API.
// ---------------------------------------------------------------------------

/// Replace the first two bytes of `dst` with an "invalid character" marker for
/// `encoding`.  Returns `None` if `dst` is too short (mirrors the C contract
/// that the buffer must hold at least two bytes).
pub fn pg_encoding_set_invalid(encoding: i32, dst: &mut [u8]) -> Option<()> {
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

/// Byte length of the leading UTF-8 character (`pg_utf_mblen` in wchar.c).
///
/// Returns `None` when `s` is empty.
pub fn pg_utf_mblen_private(s: &[u8]) -> Option<i32> {
    let first = *s.first()?;
    Some(pg_utf_mblen_byte(first))
}

/// Byte length of the leading MULE_INTERNAL character (`pg_mule_mblen`).
pub fn pg_mule_mblen(s: &[u8]) -> Option<i32> {
    s.first()?;
    Some(pg_mule_mblen_byte(s))
}

/// Whether the leading bytes of `s` form a legal UTF-8 sequence
/// (`pg_utf8_islegal`).  Returns `false` for an empty slice.
pub fn pg_utf8_islegal(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    pg_utf8_islegal_bytes(s, s.len() as i32)
}

fn table_index(encoding: i32) -> usize {
    if encoding >= 0 && encoding < _PG_LAST_ENCODING_ {
        encoding as usize
    } else {
        PG_SQL_ASCII as usize
    }
}

/// Byte length of the leading character of `mbstr` in `encoding`
/// (`pg_encoding_mblen`).  Returns `None` for an empty `mbstr`.
pub fn pg_encoding_mblen(encoding: i32, mbstr: &[u8]) -> Option<i32> {
    mbstr.first()?;
    let func = pg_wchar_table[table_index(encoding)].mblen;
    Some(func(mbstr))
}

/// Like [`pg_encoding_mblen`] but returns `INT_MAX` for empty/incomplete data
/// (`pg_encoding_mblen_or_incomplete`).
pub fn pg_encoding_mblen_or_incomplete(encoding: i32, mbstr: &[u8]) -> i32 {
    if mbstr.is_empty() || (encoding == PG_GB18030 && mbstr[0] & HIGHBIT != 0 && mbstr.len() < 2) {
        return INT_MAX;
    }
    pg_encoding_mblen(encoding, mbstr).unwrap_or(INT_MAX)
}

/// Byte length of the leading character, never reading past a NUL within that
/// character (`pg_encoding_mblen_bounded`).  Returns `None` for an empty slice.
pub fn pg_encoding_mblen_bounded(encoding: i32, mbstr: &[u8]) -> Option<i32> {
    let mblen = pg_encoding_mblen(encoding, mbstr)? as usize;
    let bounded = mbstr
        .iter()
        .take(mblen)
        .position(|byte| *byte == 0)
        .unwrap_or(mblen);
    Some(bounded as i32)
}

/// Display width of the leading character of `mbstr` (`pg_encoding_dsplen`).
/// Returns `None` for an empty slice.
pub fn pg_encoding_dsplen(encoding: i32, mbstr: &[u8]) -> Option<i32> {
    mbstr.first()?;
    let func = pg_wchar_table[table_index(encoding)].dsplen;
    Some(func(mbstr))
}

/// Verify a single multibyte character (`pg_encoding_verifymbchar`).
pub fn pg_encoding_verifymbchar(encoding: i32, mbstr: &[u8]) -> i32 {
    let func = pg_wchar_table[table_index(encoding)].mbverifychar;
    func(mbstr, mbstr.len() as i32)
}

/// Verify a multibyte string (`pg_encoding_verifymbstr`).
pub fn pg_encoding_verifymbstr(encoding: i32, mbstr: &[u8]) -> i32 {
    let func = pg_wchar_table[table_index(encoding)].mbverifystr;
    func(mbstr, mbstr.len() as i32)
}

/// Maximum bytes per character in `encoding` (`pg_encoding_max_length`).
pub fn pg_encoding_max_length(encoding: i32) -> i32 {
    pg_wchar_table[table_index(encoding)].maxmblen
}

// ---------------------------------------------------------------------------
// The per-encoding dispatch table (`pg_wchar_table` in wchar.c).
// ---------------------------------------------------------------------------

/// `mb2wchar_with_len` placeholder for the client-only encodings (SJIS, BIG5,
/// GBK, UHC, GB18030, JOHAB, SHIFT_JIS_2004).  In `wchar.c` these slots are the
/// NULL pointer (`0`): such encodings are never used as the *server* encoding,
/// so `pg_mb2wchar_with_len` (which dispatches on the database encoding) can
/// never reach them and a NULL deref is the C "this is unreachable" contract.
/// Our function pointers are non-nullable, so we stand in a routine that
/// upholds the same contract loudly rather than silently mis-converting.
fn pg_mb2wchar_unreachable(_from: &[u8], _to: &mut [pg_wchar], _len: i32) -> i32 {
    unreachable!(
        "mb2wchar_with_len is NULL in pg_wchar_table for client-only encodings; \
         such encodings are never the server encoding so this slot is never dispatched"
    )
}

/// `wchar2mb_with_len` placeholder for the client-only encodings.  See
/// [`pg_mb2wchar_unreachable`]: the C table leaves this slot NULL for the same
/// reason and it is never dispatched.
fn pg_wchar2mb_unreachable(_from: &[pg_wchar], _to: &mut [u8], _len: i32) -> i32 {
    unreachable!(
        "wchar2mb_with_len is NULL in pg_wchar_table for client-only encodings; \
         such encodings are never the server encoding so this slot is never dispatched"
    )
}

pub static pg_wchar_table: [pg_wchar_tbl; 42] = [
    // PG_SQL_ASCII
    pg_wchar_tbl {
        mb2wchar_with_len: pg_ascii2wchar_with_len,
        wchar2mb_with_len: pg_wchar2single_with_len,
        mblen: pg_ascii_mblen,
        dsplen: pg_ascii_dsplen,
        mbverifychar: pg_ascii_verifychar,
        mbverifystr: pg_ascii_verifystr,
        maxmblen: 1,
    },
    // PG_EUC_JP
    pg_wchar_tbl {
        mb2wchar_with_len: pg_eucjp2wchar_with_len,
        wchar2mb_with_len: pg_wchar2euc_with_len,
        mblen: pg_eucjp_mblen,
        dsplen: pg_eucjp_dsplen,
        mbverifychar: pg_eucjp_verifychar,
        mbverifystr: pg_eucjp_verifystr,
        maxmblen: 3,
    },
    // PG_EUC_CN
    pg_wchar_tbl {
        mb2wchar_with_len: pg_euccn2wchar_with_len,
        wchar2mb_with_len: pg_wchar2euc_with_len,
        mblen: pg_euccn_mblen,
        dsplen: pg_euccn_dsplen,
        mbverifychar: pg_euckr_verifychar,
        mbverifystr: pg_euckr_verifystr,
        maxmblen: 3,
    },
    // PG_EUC_KR
    pg_wchar_tbl {
        mb2wchar_with_len: pg_euckr2wchar_with_len,
        wchar2mb_with_len: pg_wchar2euc_with_len,
        mblen: pg_euckr_mblen,
        dsplen: pg_euckr_dsplen,
        mbverifychar: pg_euckr_verifychar,
        mbverifystr: pg_euckr_verifystr,
        maxmblen: 3,
    },
    // PG_EUC_TW
    pg_wchar_tbl {
        mb2wchar_with_len: pg_euctw2wchar_with_len,
        wchar2mb_with_len: pg_wchar2euc_with_len,
        mblen: pg_euctw_mblen,
        dsplen: pg_euctw_dsplen,
        mbverifychar: pg_euctw_verifychar,
        mbverifystr: pg_euctw_verifystr,
        maxmblen: 4,
    },
    // PG_EUC_JIS_2004
    pg_wchar_tbl {
        mb2wchar_with_len: pg_eucjp2wchar_with_len,
        wchar2mb_with_len: pg_wchar2euc_with_len,
        mblen: pg_eucjp_mblen,
        dsplen: pg_eucjp_dsplen,
        mbverifychar: pg_eucjp_verifychar,
        mbverifystr: pg_eucjp_verifystr,
        maxmblen: 3,
    },
    // PG_UTF8
    pg_wchar_tbl {
        mb2wchar_with_len: pg_utf2wchar_with_len,
        wchar2mb_with_len: pg_wchar2utf_with_len,
        mblen: pg_utf_mblen_dispatch,
        dsplen: pg_utf_dsplen,
        mbverifychar: pg_utf8_verifychar,
        mbverifystr: pg_utf8_verifystr,
        maxmblen: 4,
    },
    // PG_MULE_INTERNAL
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mule2wchar_with_len,
        wchar2mb_with_len: pg_wchar2mule_with_len,
        mblen: pg_mule_mblen_byte,
        dsplen: pg_mule_dsplen,
        mbverifychar: pg_mule_verifychar,
        mbverifystr: pg_mule_verifystr,
        maxmblen: 4,
    },
    // PG_LATIN1
    SINGLE_BYTE_TBL,
    // PG_LATIN2
    SINGLE_BYTE_TBL,
    // PG_LATIN3
    SINGLE_BYTE_TBL,
    // PG_LATIN4
    SINGLE_BYTE_TBL,
    // PG_LATIN5
    SINGLE_BYTE_TBL,
    // PG_LATIN6
    SINGLE_BYTE_TBL,
    // PG_LATIN7
    SINGLE_BYTE_TBL,
    // PG_LATIN8
    SINGLE_BYTE_TBL,
    // PG_LATIN9
    SINGLE_BYTE_TBL,
    // PG_LATIN10
    SINGLE_BYTE_TBL,
    // PG_WIN1256
    SINGLE_BYTE_TBL,
    // PG_WIN1258
    SINGLE_BYTE_TBL,
    // PG_WIN866
    SINGLE_BYTE_TBL,
    // PG_WIN874
    SINGLE_BYTE_TBL,
    // PG_KOI8R
    SINGLE_BYTE_TBL,
    // PG_WIN1251
    SINGLE_BYTE_TBL,
    // PG_WIN1252
    SINGLE_BYTE_TBL,
    // PG_ISO_8859_5
    SINGLE_BYTE_TBL,
    // PG_ISO_8859_6
    SINGLE_BYTE_TBL,
    // PG_ISO_8859_7
    SINGLE_BYTE_TBL,
    // PG_ISO_8859_8
    SINGLE_BYTE_TBL,
    // PG_WIN1250
    SINGLE_BYTE_TBL,
    // PG_WIN1253
    SINGLE_BYTE_TBL,
    // PG_WIN1254
    SINGLE_BYTE_TBL,
    // PG_WIN1255
    SINGLE_BYTE_TBL,
    // PG_WIN1257
    SINGLE_BYTE_TBL,
    // PG_KOI8U
    SINGLE_BYTE_TBL,
    // PG_SJIS
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_sjis_mblen,
        dsplen: pg_sjis_dsplen,
        mbverifychar: pg_sjis_verifychar,
        mbverifystr: pg_sjis_verifystr,
        maxmblen: 2,
    },
    // PG_BIG5
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_big5_mblen,
        dsplen: pg_big5_dsplen,
        mbverifychar: pg_big5_verifychar,
        mbverifystr: pg_big5_verifystr,
        maxmblen: 2,
    },
    // PG_GBK
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_gbk_mblen,
        dsplen: pg_gbk_dsplen,
        mbverifychar: pg_gbk_verifychar,
        mbverifystr: pg_gbk_verifystr,
        maxmblen: 2,
    },
    // PG_UHC
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_uhc_mblen,
        dsplen: pg_uhc_dsplen,
        mbverifychar: pg_uhc_verifychar,
        mbverifystr: pg_uhc_verifystr,
        maxmblen: 2,
    },
    // PG_GB18030
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_gb18030_mblen,
        dsplen: pg_gb18030_dsplen,
        mbverifychar: pg_gb18030_verifychar,
        mbverifystr: pg_gb18030_verifystr,
        maxmblen: 4,
    },
    // PG_JOHAB
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_johab_mblen,
        dsplen: pg_johab_dsplen,
        mbverifychar: pg_johab_verifychar,
        mbverifystr: pg_johab_verifystr,
        maxmblen: 3,
    },
    // PG_SHIFT_JIS_2004
    pg_wchar_tbl {
        mb2wchar_with_len: pg_mb2wchar_unreachable,
        wchar2mb_with_len: pg_wchar2mb_unreachable,
        mblen: pg_sjis_mblen,
        dsplen: pg_sjis_dsplen,
        mbverifychar: pg_sjis_verifychar,
        mbverifystr: pg_sjis_verifystr,
        maxmblen: 2,
    },
];

/// Shared `pg_wchar_tbl` value for every single-byte encoding (LATIN/WIN/KOI8/
/// ISO-8859), all of which share the same routines in wchar.c.
const SINGLE_BYTE_TBL: pg_wchar_tbl = pg_wchar_tbl {
    mb2wchar_with_len: pg_latin12wchar_with_len,
    wchar2mb_with_len: pg_wchar2single_with_len,
    mblen: pg_latin1_mblen,
    dsplen: pg_latin1_dsplen,
    mbverifychar: pg_latin1_verifychar,
    mbverifystr: pg_latin1_verifystr,
    maxmblen: 1,
};

/// `mblen` slot for UTF-8: the C table uses `pg_utf_mblen`, which only inspects
/// the leading byte.
fn pg_utf_mblen_dispatch(s: &[u8]) -> i32 {
    pg_utf_mblen_byte(s[0])
}

// ---------------------------------------------------------------------------
// Data tables (transcribed verbatim from wchar.c).
// ---------------------------------------------------------------------------

include!("tables.rs");

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
