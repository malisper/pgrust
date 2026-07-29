#![no_std]
#![allow(non_camel_case_types, non_upper_case_globals, non_snake_case)]

pub type pg_wchar = u32;
pub type pg_enc = i32;

pub const PG_SQL_ASCII: pg_enc = 0;
pub const PG_EUC_JP: pg_enc = 1;
pub const PG_EUC_CN: pg_enc = 2;
pub const PG_EUC_KR: pg_enc = 3;
pub const PG_EUC_TW: pg_enc = 4;
pub const PG_EUC_JIS_2004: pg_enc = 5;
pub const PG_UTF8: pg_enc = 6;
pub const PG_MULE_INTERNAL: pg_enc = 7;
pub const PG_LATIN1: pg_enc = 8;
pub const PG_LATIN2: pg_enc = 9;
pub const PG_LATIN3: pg_enc = 10;
pub const PG_LATIN4: pg_enc = 11;
pub const PG_LATIN5: pg_enc = 12;
pub const PG_LATIN6: pg_enc = 13;
pub const PG_LATIN7: pg_enc = 14;
pub const PG_LATIN8: pg_enc = 15;
pub const PG_LATIN9: pg_enc = 16;
pub const PG_LATIN10: pg_enc = 17;
pub const PG_WIN1256: pg_enc = 18;
pub const PG_WIN1258: pg_enc = 19;
pub const PG_WIN866: pg_enc = 20;
pub const PG_WIN874: pg_enc = 21;
pub const PG_KOI8R: pg_enc = 22;
pub const PG_WIN1251: pg_enc = 23;
pub const PG_WIN1252: pg_enc = 24;
pub const PG_ISO_8859_5: pg_enc = 25;
pub const PG_ISO_8859_6: pg_enc = 26;
pub const PG_ISO_8859_7: pg_enc = 27;
pub const PG_ISO_8859_8: pg_enc = 28;
pub const PG_WIN1250: pg_enc = 29;
pub const PG_WIN1253: pg_enc = 30;
pub const PG_WIN1254: pg_enc = 31;
pub const PG_WIN1255: pg_enc = 32;
pub const PG_WIN1257: pg_enc = 33;
pub const PG_KOI8U: pg_enc = 34;
pub const PG_SJIS: pg_enc = 35;
pub const PG_BIG5: pg_enc = 36;
pub const PG_GBK: pg_enc = 37;
pub const PG_UHC: pg_enc = 38;
pub const PG_GB18030: pg_enc = 39;
pub const PG_JOHAB: pg_enc = 40;
pub const PG_SHIFT_JIS_2004: pg_enc = 41;
pub const _PG_LAST_ENCODING_: pg_enc = 42;

pub const PG_ENCODING_BE_LAST: pg_enc = PG_KOI8U;
pub const MAX_MULTIBYTE_CHAR_LEN: i32 = 4;

pub const fn pg_valid_encoding(encoding: pg_enc) -> bool {
    encoding >= 0 && encoding < _PG_LAST_ENCODING_
}

pub const fn pg_valid_be_encoding(encoding: pg_enc) -> bool {
    encoding >= 0 && encoding <= PG_ENCODING_BE_LAST
}

pub const fn pg_valid_fe_encoding(encoding: pg_enc) -> bool {
    pg_valid_encoding(encoding)
}

pub const fn pg_encoding_is_client_only(encoding: pg_enc) -> bool {
    encoding > PG_ENCODING_BE_LAST && encoding < _PG_LAST_ENCODING_
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct mbinterval {
    pub first: u32,
    pub last: u32,
}

const HIGHBIT: u8 = 0x80;
const SS2: u8 = 0x8e;
const SS3: u8 = 0x8f;
const LCPRV1_A: u8 = 0x9a;
const LCPRV1_B: u8 = 0x9b;
const LCPRV2_A: u8 = 0x9c;
const LCPRV2_B: u8 = 0x9d;
const NONUTF8_INVALID_BYTE0: u8 = 0x8d;
const NONUTF8_INVALID_BYTE1: u8 = b' ';

#[inline(always)]
fn is_highbit_set(b: u8) -> bool {
    b & HIGHBIT != 0
}

#[inline(always)]
fn is_euc_range_valid(c: u8) -> bool {
    (0xa1..=0xfe).contains(&c)
}

pub type Mb2WcharConverter = fn(from: &[u8], to: &mut [pg_wchar]) -> i32;
pub type Wchar2MbConverter = fn(from: &[pg_wchar], to: &mut [u8]) -> i32;
pub type MblenConverter = fn(mbstr: &[u8]) -> i32;
pub type DsplenConverter = fn(mbstr: &[u8]) -> i32;
pub type MbCharVerifier = fn(mbstr: &[u8]) -> i32;
pub type MbStrVerifier = fn(mbstr: &[u8]) -> i32;

/// `pg_wchar_tbl` row. The C callbacks' `len` argument is the slice length
/// here; `mblen`/`dsplen` require the full character to be present (C reads
/// it unchecked), verifiers only require a non-empty slice.
#[derive(Clone, Copy)]
pub struct pg_wchar_tbl {
    pub mb2wchar_with_len: Option<Mb2WcharConverter>,
    pub wchar2mb_with_len: Option<Wchar2MbConverter>,
    pub mblen: MblenConverter,
    pub dsplen: DsplenConverter,
    pub mbverifychar: MbCharVerifier,
    pub mbverifystr: MbStrVerifier,
    pub maxmblen: i32,
}

fn pg_ascii2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        to[ti] = from[fi] as pg_wchar;
        fi += 1;
        ti += 1;
        len -= 1;
    }
    to[ti] = 0;
    ti as i32
}

fn pg_ascii_mblen(_s: &[u8]) -> i32 {
    1
}

fn pg_ascii_dsplen(s: &[u8]) -> i32 {
    let c = s[0];
    if c == 0 {
        0
    } else if c < 0x20 || c == 0x7f {
        -1
    } else {
        1
    }
}

fn pg_euc2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        if from[fi] == SS2 {
            if len < 2 {
                break;
            }
            fi += 1;
            to[ti] = ((SS2 as u32) << 8) | from[fi] as u32;
            fi += 1;
            len -= 2;
        } else if from[fi] == SS3 {
            if len < 3 {
                break;
            }
            fi += 1;
            to[ti] = ((SS3 as u32) << 16) | ((from[fi] as u32) << 8);
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 3;
        } else if is_highbit_set(from[fi]) {
            if len < 2 {
                break;
            }
            to[ti] = (from[fi] as u32) << 8;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 2;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
    }
    to[ti] = 0;
    ti as i32
}

#[inline]
fn pg_euc_mblen(s: &[u8]) -> i32 {
    if s[0] == SS2 {
        2
    } else if s[0] == SS3 {
        3
    } else if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

#[inline]
fn pg_euc_dsplen(s: &[u8]) -> i32 {
    if s[0] == SS2 || s[0] == SS3 || is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_eucjp2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    pg_euc2wchar_with_len(from, to)
}

fn pg_eucjp_mblen(s: &[u8]) -> i32 {
    pg_euc_mblen(s)
}

fn pg_eucjp_dsplen(s: &[u8]) -> i32 {
    if s[0] == SS2 {
        1
    } else if s[0] == SS3 {
        2
    } else if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_euckr2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    pg_euc2wchar_with_len(from, to)
}

fn pg_euckr_mblen(s: &[u8]) -> i32 {
    pg_euc_mblen(s)
}

fn pg_euckr_dsplen(s: &[u8]) -> i32 {
    pg_euc_dsplen(s)
}

fn pg_euccn2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        if from[fi] == SS2 || from[fi] == SS3 {
            let lead = from[fi] as u32;
            if len < 3 {
                break;
            }
            fi += 1;
            to[ti] = (lead << 16) | ((from[fi] as u32) << 8);
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 3;
        } else if is_highbit_set(from[fi]) {
            if len < 2 {
                break;
            }
            to[ti] = (from[fi] as u32) << 8;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 2;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
    }
    to[ti] = 0;
    ti as i32
}

fn pg_euccn_mblen(s: &[u8]) -> i32 {
    if s[0] == SS2 || s[0] == SS3 {
        3
    } else if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

fn pg_euccn_dsplen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_euctw2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        if from[fi] == SS2 {
            if len < 4 {
                break;
            }
            fi += 1;
            to[ti] = ((SS2 as u32) << 24) | ((from[fi] as u32) << 16);
            fi += 1;
            to[ti] |= (from[fi] as u32) << 8;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 4;
        } else if from[fi] == SS3 {
            if len < 3 {
                break;
            }
            fi += 1;
            to[ti] = ((SS3 as u32) << 16) | ((from[fi] as u32) << 8);
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 3;
        } else if is_highbit_set(from[fi]) {
            if len < 2 {
                break;
            }
            to[ti] = (from[fi] as u32) << 8;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 2;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
    }
    to[ti] = 0;
    ti as i32
}

fn pg_euctw_mblen(s: &[u8]) -> i32 {
    if s[0] == SS2 {
        4
    } else if s[0] == SS3 {
        3
    } else if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

fn pg_euctw_dsplen(s: &[u8]) -> i32 {
    if s[0] == SS2 {
        2
    } else if s[0] == SS3 {
        2
    } else if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_wchar2euc_with_len(from: &[pg_wchar], to: &mut [u8]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    let mut cnt = 0i32;
    while len > 0 && from[fi] != 0 {
        let w = from[fi];
        if (w >> 24) as u8 != 0 {
            to[ti] = (w >> 24) as u8;
            to[ti + 1] = (w >> 16) as u8;
            to[ti + 2] = (w >> 8) as u8;
            to[ti + 3] = w as u8;
            ti += 4;
            cnt += 4;
        } else if (w >> 16) as u8 != 0 {
            to[ti] = (w >> 16) as u8;
            to[ti + 1] = (w >> 8) as u8;
            to[ti + 2] = w as u8;
            ti += 3;
            cnt += 3;
        } else if (w >> 8) as u8 != 0 {
            to[ti] = (w >> 8) as u8;
            to[ti + 1] = w as u8;
            ti += 2;
            cnt += 2;
        } else {
            to[ti] = w as u8;
            ti += 1;
            cnt += 1;
        }
        fi += 1;
        len -= 1;
    }
    to[ti] = 0;
    cnt
}

fn pg_johab_mblen(s: &[u8]) -> i32 {
    pg_euc_mblen(s)
}

fn pg_johab_dsplen(s: &[u8]) -> i32 {
    pg_euc_dsplen(s)
}

fn pg_utf2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi];
        if b & 0x80 == 0 {
            to[ti] = b as pg_wchar;
            fi += 1;
            len -= 1;
        } else if b & 0xe0 == 0xc0 {
            if len < 2 {
                break;
            }
            let c1 = (from[fi] & 0x1f) as u32;
            let c2 = (from[fi + 1] & 0x3f) as u32;
            to[ti] = (c1 << 6) | c2;
            fi += 2;
            len -= 2;
        } else if b & 0xf0 == 0xe0 {
            if len < 3 {
                break;
            }
            let c1 = (from[fi] & 0x0f) as u32;
            let c2 = (from[fi + 1] & 0x3f) as u32;
            let c3 = (from[fi + 2] & 0x3f) as u32;
            to[ti] = (c1 << 12) | (c2 << 6) | c3;
            fi += 3;
            len -= 3;
        } else if b & 0xf8 == 0xf0 {
            if len < 4 {
                break;
            }
            let c1 = (from[fi] & 0x07) as u32;
            let c2 = (from[fi + 1] & 0x3f) as u32;
            let c3 = (from[fi + 2] & 0x3f) as u32;
            let c4 = (from[fi + 3] & 0x3f) as u32;
            to[ti] = (c1 << 18) | (c2 << 12) | (c3 << 6) | c4;
            fi += 4;
            len -= 4;
        } else {
            to[ti] = b as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
    }
    to[ti] = 0;
    ti as i32
}

fn pg_wchar2utf_with_len(from: &[pg_wchar], to: &mut [u8]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    let mut cnt = 0i32;
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

#[inline(always)]
fn pg_utf_mblen_byte(b: u8) -> i32 {
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

pub fn pg_utf_mblen(s: &[u8]) -> i32 {
    pg_utf_mblen_byte(s[0])
}

// pub (visibility only) for the proofs/utf8 Kani equivalence harnesses.
pub fn mbbisearch(ucs: pg_wchar, table: &[mbinterval]) -> bool {
    let mut min = 0i32;
    let mut max = table.len() as i32 - 1;
    if ucs < table[0].first || ucs > table[max as usize].last {
        return false;
    }
    while max >= min {
        let mid = (min + max) / 2;
        let iv = table[mid as usize];
        if ucs > iv.last {
            min = mid + 1;
        } else if ucs < iv.first {
            max = mid - 1;
        } else {
            return true;
        }
    }
    false
}

// pub (visibility only) for the proofs/utf8 Kani equivalence harnesses.
pub fn ucs_wcwidth(ucs: pg_wchar) -> i32 {
    if ucs == 0 {
        return 0;
    }
    if ucs < 0x20 || (0x7f..0xa0).contains(&ucs) || ucs > 0x10ffff {
        return -1;
    }
    if mbbisearch(ucs, &NONSPACING) {
        return 0;
    }
    if mbbisearch(ucs, &EAST_ASIAN_FW) {
        return 2;
    }
    1
}

fn pg_utf_dsplen(s: &[u8]) -> i32 {
    ucs_wcwidth(utf8_to_unicode(s))
}

fn pg_mule2wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        let b = from[fi];
        if is_lc1(b) {
            if len < 2 {
                break;
            }
            to[ti] = (from[fi] as u32) << 16;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 2;
        } else if is_lcprv1(b) {
            if len < 3 {
                break;
            }
            fi += 1;
            to[ti] = (from[fi] as u32) << 16;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 3;
        } else if is_lc2(b) {
            if len < 3 {
                break;
            }
            to[ti] = (from[fi] as u32) << 16;
            fi += 1;
            to[ti] |= (from[fi] as u32) << 8;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 3;
        } else if is_lcprv2(b) {
            if len < 4 {
                break;
            }
            fi += 1;
            to[ti] = (from[fi] as u32) << 16;
            fi += 1;
            to[ti] |= (from[fi] as u32) << 8;
            fi += 1;
            to[ti] |= from[fi] as u32;
            fi += 1;
            len -= 4;
        } else {
            to[ti] = from[fi] as pg_wchar;
            fi += 1;
            len -= 1;
        }
        ti += 1;
    }
    to[ti] = 0;
    ti as i32
}

#[inline(always)]
fn is_lc1(b: u8) -> bool {
    (0x81..=0x8d).contains(&b)
}

#[inline(always)]
fn is_lc2(b: u8) -> bool {
    (0x90..=0x99).contains(&b)
}

#[inline(always)]
fn is_lcprv1(b: u8) -> bool {
    b == LCPRV1_A || b == LCPRV1_B
}

#[inline(always)]
fn is_lcprv2(b: u8) -> bool {
    b == LCPRV2_A || b == LCPRV2_B
}

fn pg_wchar2mule_with_len(from: &[pg_wchar], to: &mut [u8]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    let mut cnt = 0i32;
    while len > 0 && from[fi] != 0 {
        let w = from[fi];
        let lb = (w >> 16) as u8;
        if is_lc1(lb) {
            to[ti] = lb;
            to[ti + 1] = w as u8;
            ti += 2;
            cnt += 2;
        } else if is_lc2(lb) {
            to[ti] = lb;
            to[ti + 1] = (w >> 8) as u8;
            to[ti + 2] = w as u8;
            ti += 3;
            cnt += 3;
        } else if (0xa0..=0xdf).contains(&lb) {
            to[ti] = LCPRV1_A;
            to[ti + 1] = lb;
            to[ti + 2] = w as u8;
            ti += 3;
            cnt += 3;
        } else if (0xe0..=0xef).contains(&lb) {
            to[ti] = LCPRV1_B;
            to[ti + 1] = lb;
            to[ti + 2] = w as u8;
            ti += 3;
            cnt += 3;
        } else if (0xf0..=0xf4).contains(&lb) {
            to[ti] = LCPRV2_A;
            to[ti + 1] = lb;
            to[ti + 2] = (w >> 8) as u8;
            to[ti + 3] = w as u8;
            ti += 4;
            cnt += 4;
        } else if (0xf5..=0xfe).contains(&lb) {
            to[ti] = LCPRV2_B;
            to[ti + 1] = lb;
            to[ti + 2] = (w >> 8) as u8;
            to[ti + 3] = w as u8;
            ti += 4;
            cnt += 4;
        } else {
            to[ti] = w as u8;
            ti += 1;
            cnt += 1;
        }
        fi += 1;
        len -= 1;
    }
    to[ti] = 0;
    cnt
}

pub fn pg_mule_mblen(s: &[u8]) -> i32 {
    let b = s[0];
    if is_lc1(b) {
        2
    } else if is_lcprv1(b) {
        3
    } else if is_lc2(b) {
        3
    } else if is_lcprv2(b) {
        4
    } else {
        1
    }
}

fn pg_mule_dsplen(s: &[u8]) -> i32 {
    let b = s[0];
    if is_lc1(b) {
        1
    } else if is_lcprv1(b) {
        1
    } else if is_lc2(b) {
        2
    } else if is_lcprv2(b) {
        2
    } else {
        1
    }
}

fn pg_latin12wchar_with_len(from: &[u8], to: &mut [pg_wchar]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        to[ti] = from[fi] as pg_wchar;
        fi += 1;
        ti += 1;
        len -= 1;
    }
    to[ti] = 0;
    ti as i32
}

fn pg_wchar2single_with_len(from: &[pg_wchar], to: &mut [u8]) -> i32 {
    let mut len = from.len();
    let mut fi = 0;
    let mut ti = 0;
    while len > 0 && from[fi] != 0 {
        to[ti] = from[fi] as u8;
        fi += 1;
        ti += 1;
        len -= 1;
    }
    to[ti] = 0;
    ti as i32
}

fn pg_latin1_mblen(_s: &[u8]) -> i32 {
    1
}

fn pg_latin1_dsplen(s: &[u8]) -> i32 {
    pg_ascii_dsplen(s)
}

fn pg_sjis_mblen(s: &[u8]) -> i32 {
    if (0xa1..=0xdf).contains(&s[0]) {
        1
    } else if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

fn pg_sjis_dsplen(s: &[u8]) -> i32 {
    if (0xa1..=0xdf).contains(&s[0]) {
        1
    } else if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_big5_mblen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

fn pg_big5_dsplen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_gbk_mblen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

fn pg_gbk_dsplen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn pg_uhc_mblen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        1
    }
}

fn pg_uhc_dsplen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

/// Like the C, reads `s[1]` when the high bit is set — callers must supply the
/// lookahead byte or use [`pg_encoding_mblen_or_incomplete`].
fn pg_gb18030_mblen(s: &[u8]) -> i32 {
    if !is_highbit_set(s[0]) {
        1
    } else if (0x30..=0x39).contains(&s[1]) {
        4
    } else {
        2
    }
}

fn pg_gb18030_dsplen(s: &[u8]) -> i32 {
    if is_highbit_set(s[0]) {
        2
    } else {
        pg_ascii_dsplen(s)
    }
}

fn nul_pos(s: &[u8]) -> i32 {
    match s.iter().position(|&b| b == 0) {
        Some(p) => p as i32,
        None => s.len() as i32,
    }
}

fn pg_ascii_verifychar(_s: &[u8]) -> i32 {
    1
}

fn pg_ascii_verifystr(s: &[u8]) -> i32 {
    nul_pos(s)
}

fn pg_eucjp_verifychar(s: &[u8]) -> i32 {
    let len = s.len();
    let c1 = s[0];
    if c1 == SS2 {
        if len < 2 {
            return -1;
        }
        if !(0xa1..=0xdf).contains(&s[1]) {
            return -1;
        }
        2
    } else if c1 == SS3 {
        if len < 3 {
            return -1;
        }
        if !is_euc_range_valid(s[1]) || !is_euc_range_valid(s[2]) {
            return -1;
        }
        3
    } else if is_highbit_set(c1) {
        if len < 2 {
            return -1;
        }
        if !is_euc_range_valid(c1) || !is_euc_range_valid(s[1]) {
            return -1;
        }
        2
    } else {
        1
    }
}

fn pg_eucjp_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_eucjp_verifychar)
}

fn pg_euckr_verifychar(s: &[u8]) -> i32 {
    let c1 = s[0];
    if is_highbit_set(c1) {
        if s.len() < 2 {
            return -1;
        }
        if !is_euc_range_valid(c1) || !is_euc_range_valid(s[1]) {
            return -1;
        }
        2
    } else {
        1
    }
}

fn pg_euckr_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_euckr_verifychar)
}

fn pg_euctw_verifychar(s: &[u8]) -> i32 {
    let len = s.len();
    let c1 = s[0];
    if c1 == SS2 {
        if len < 4 {
            return -1;
        }
        if !(0xa1..=0xa7).contains(&s[1]) {
            return -1;
        }
        if !is_euc_range_valid(s[2]) || !is_euc_range_valid(s[3]) {
            return -1;
        }
        4
    } else if c1 == SS3 {
        -1
    } else if is_highbit_set(c1) {
        if len < 2 {
            return -1;
        }
        if !is_euc_range_valid(s[1]) {
            return -1;
        }
        2
    } else {
        1
    }
}

fn pg_euctw_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_euctw_verifychar)
}

fn pg_johab_verifychar(s: &[u8]) -> i32 {
    let mbl = pg_johab_mblen(s);
    if (s.len() as i32) < mbl {
        return -1;
    }
    if !is_highbit_set(s[0]) {
        return mbl;
    }
    for i in 1..mbl as usize {
        if !is_euc_range_valid(s[i]) {
            return -1;
        }
    }
    mbl
}

fn pg_johab_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_johab_verifychar)
}

fn pg_mule_verifychar(s: &[u8]) -> i32 {
    let mbl = pg_mule_mblen(s);
    if (s.len() as i32) < mbl {
        return -1;
    }
    for i in 1..mbl as usize {
        if !is_highbit_set(s[i]) {
            return -1;
        }
    }
    mbl
}

fn pg_mule_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_mule_verifychar)
}

fn pg_latin1_verifychar(_s: &[u8]) -> i32 {
    1
}

fn pg_latin1_verifystr(s: &[u8]) -> i32 {
    nul_pos(s)
}

fn pg_sjis_verifychar(s: &[u8]) -> i32 {
    let mbl = pg_sjis_mblen(s);
    if (s.len() as i32) < mbl {
        return -1;
    }
    if mbl == 1 {
        return mbl;
    }
    let c1 = s[0];
    let c2 = s[1];
    let head = (0x81..=0x9f).contains(&c1) || (0xe0..=0xfc).contains(&c1);
    let tail = (0x40..=0x7e).contains(&c2) || (0x80..=0xfc).contains(&c2);
    if !head || !tail {
        return -1;
    }
    mbl
}

fn pg_sjis_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_sjis_verifychar)
}

fn nonutf8_invalid_pair(s: &[u8]) -> bool {
    s[0] == NONUTF8_INVALID_BYTE0 && s[1] == NONUTF8_INVALID_BYTE1
}

fn pg_big5_verifychar(s: &[u8]) -> i32 {
    let mbl = pg_big5_mblen(s);
    if (s.len() as i32) < mbl {
        return -1;
    }
    if mbl == 2 && nonutf8_invalid_pair(s) {
        return -1;
    }
    for i in 1..mbl as usize {
        if s[i] == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_big5_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_big5_verifychar)
}

fn pg_gbk_verifychar(s: &[u8]) -> i32 {
    let mbl = pg_gbk_mblen(s);
    if (s.len() as i32) < mbl {
        return -1;
    }
    if mbl == 2 && nonutf8_invalid_pair(s) {
        return -1;
    }
    for i in 1..mbl as usize {
        if s[i] == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_gbk_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_gbk_verifychar)
}

fn pg_uhc_verifychar(s: &[u8]) -> i32 {
    let mbl = pg_uhc_mblen(s);
    if (s.len() as i32) < mbl {
        return -1;
    }
    if mbl == 2 && nonutf8_invalid_pair(s) {
        return -1;
    }
    for i in 1..mbl as usize {
        if s[i] == 0 {
            return -1;
        }
    }
    mbl
}

fn pg_uhc_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_uhc_verifychar)
}

fn pg_gb18030_verifychar(s: &[u8]) -> i32 {
    let len = s.len();
    if !is_highbit_set(s[0]) {
        1
    } else if len >= 4 && (0x30..=0x39).contains(&s[1]) {
        if (0x81..=0xfe).contains(&s[0])
            && (0x81..=0xfe).contains(&s[2])
            && (0x30..=0x39).contains(&s[3])
        {
            4
        } else {
            -1
        }
    } else if len >= 2 && (0x81..=0xfe).contains(&s[0]) {
        if (0x40..=0x7e).contains(&s[1]) || (0x80..=0xfe).contains(&s[1]) {
            2
        } else {
            -1
        }
    } else {
        -1
    }
}

fn pg_gb18030_verifystr(s: &[u8]) -> i32 {
    verify_str(s, pg_gb18030_verifychar)
}

fn pg_utf8_verifychar(s: &[u8]) -> i32 {
    let b = s[0];
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
    if l as usize > s.len() {
        return -1;
    }
    if !pg_utf8_islegal(s, l) {
        return -1;
    }
    l
}

/// Shared body of every non-UTF8 `*_verifystr` in wchar.c, monomorphized per
/// character verifier.
#[inline]
fn verify_str(s: &[u8], verifychar: impl Fn(&[u8]) -> i32) -> i32 {
    let mut rest = s;
    while let Some((&b, tail)) = rest.split_first() {
        if !is_highbit_set(b) {
            if b == 0 {
                break;
            }
            rest = tail;
            continue;
        }
        let l = verifychar(rest);
        if l == -1 {
            break;
        }
        rest = &rest[l as usize..];
    }
    (s.len() - rest.len()) as i32
}

const STRIDE_LENGTH: usize = 32;

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn is_valid_ascii(chunk: &[u8; STRIDE_LENGTH]) -> bool {
    use core::arch::aarch64::*;
    // SAFETY: NEON is baseline on aarch64; both 16-byte loads are within `chunk`.
    unsafe {
        let zero = vdupq_n_u8(0);
        let v0 = vld1q_u8(chunk.as_ptr());
        let v1 = vld1q_u8(chunk.as_ptr().add(16));
        let mut cum = vorrq_u8(v0, vceqq_u8(v0, zero));
        cum = vorrq_u8(cum, vorrq_u8(v1, vceqq_u8(v1, zero)));
        vmaxvq_u8(cum) <= 0x7f
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn is_valid_ascii(chunk: &[u8; STRIDE_LENGTH]) -> bool {
    use core::arch::x86_64::*;
    // SAFETY: SSE2 is baseline on x86_64; both 16-byte loads are within `chunk`.
    unsafe {
        let zero = _mm_setzero_si128();
        let v0 = _mm_loadu_si128(chunk.as_ptr().cast());
        let v1 = _mm_loadu_si128(chunk.as_ptr().add(16).cast());
        let mut cum = _mm_or_si128(v0, _mm_cmpeq_epi8(v0, zero));
        cum = _mm_or_si128(cum, _mm_or_si128(v1, _mm_cmpeq_epi8(v1, zero)));
        _mm_movemask_epi8(cum) == 0
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline(always)]
fn is_valid_ascii(chunk: &[u8; STRIDE_LENGTH]) -> bool {
    // The 0x7f add flags zero bytes without carry only when no high bit is
    // set; high-bit inputs are caught by highbit_cum (C's USE_NO_SIMD path).
    let mut highbit_cum: u64 = 0;
    let mut zero_cum: u64 = 0x8080_8080_8080_8080;
    let mut i = 0;
    while i < STRIDE_LENGTH {
        let w = u64::from_ne_bytes(chunk[i..i + 8].try_into().unwrap());
        zero_cum &= w.wrapping_add(0x7f7f_7f7f_7f7f_7f7f);
        highbit_cum |= w;
        i += 8;
    }
    highbit_cum & 0x8080_8080_8080_8080 == 0 && zero_cum == 0x8080_8080_8080_8080
}

const ERR: u32 = 0;
const BGN: u32 = 11;
const CS1: u32 = 16;
const CS2: u32 = 1;
const CS3: u32 = 5;
const P3A: u32 = 6;
const P3B: u32 = 20;
const P4A: u32 = 25;
const P4B: u32 = 30;
const END: u32 = BGN;
const ASC: u32 = END << BGN;
const L2A: u32 = CS1 << BGN;
const L3A: u32 = P3A << BGN;
const L3B: u32 = CS2 << BGN;
const L3C: u32 = P3B << BGN;
const L4A: u32 = P4A << BGN;
const L4B: u32 = CS3 << BGN;
const L4C: u32 = P4B << BGN;
const CR1: u32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4B);
const CR2: u32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3B) | (CS2 << P4A);
const CR3: u32 = (END << CS1) | (CS1 << CS2) | (CS2 << CS3) | (CS1 << P3A) | (CS2 << P4A);
const ILL: u32 = ERR;

// Extracting bytes from u64 words keeps the byte feed scalar; a plain byte
// loop is auto-vectorized into ld1 + per-byte umov, which stalls the state
// chain (dense-multibyte ns 1.04x C, see docs/optimizations/wchar-parity.md).
#[inline]
fn utf8_advance(chunk: &[u8; STRIDE_LENGTH], state: &mut u32) {
    let mut st = *state;
    let mut i = 0;
    while i < STRIDE_LENGTH {
        let w = u64::from_le_bytes(chunk[i..i + 8].try_into().unwrap());
        let mut k = 0;
        while k < 64 {
            st = UTF8_TRANSITION[((w >> k) & 0xff) as usize] >> (st & 31);
            k += 8;
        }
        i += 8;
    }
    *state = st & 31;
}

fn pg_utf8_verifystr(s: &[u8]) -> i32 {
    let len = s.len();
    let mut state: u32 = BGN;
    let mut pos = 0usize;
    if len >= STRIDE_LENGTH {
        let (chunks, _) = s.as_chunks::<STRIDE_LENGTH>();
        for chunk in chunks {
            if state != END || !is_valid_ascii(chunk) {
                utf8_advance(chunk, &mut state);
            }
        }
        pos = chunks.len() * STRIDE_LENGTH;
        // The error state persists, so it only needs one check, here.
        if state == ERR {
            // Restart from the beginning so the slow path counts valid bytes.
            pos = 0;
        } else if state != END {
            // Fast path ended mid-sequence: back up to its lead byte.
            loop {
                pos -= 1;
                if pg_utf_mblen_byte(s[pos]) > 1 {
                    break;
                }
            }
        }
    }
    let mut rest = &s[pos..];
    while let Some((&b, tail)) = rest.split_first() {
        if !is_highbit_set(b) {
            if b == 0 {
                break;
            }
            rest = tail;
            continue;
        }
        let l = pg_utf8_verifychar(rest);
        if l == -1 {
            break;
        }
        rest = &rest[l as usize..];
    }
    (len - rest.len()) as i32
}

pub fn pg_utf8_islegal(source: &[u8], length: i32) -> bool {
    match length {
        4 => {
            if !(0x80..=0xbf).contains(&source[3]) {
                return false;
            }
            if !(0x80..=0xbf).contains(&source[2]) {
                return false;
            }
            if !utf8_second_byte_legal(source[0], source[1]) {
                return false;
            }
        }
        3 => {
            if !(0x80..=0xbf).contains(&source[2]) {
                return false;
            }
            if !utf8_second_byte_legal(source[0], source[1]) {
                return false;
            }
        }
        2 => {
            if !utf8_second_byte_legal(source[0], source[1]) {
                return false;
            }
        }
        1 => {}
        // C (wchar.c pg_utf8_islegal) switch default: lengths outside 1..=4
        // are illegal. Divergence found+fixed via proofs/utf8 out-of-contract
        // harness (Rust used to fall through to the first-byte checks).
        _ => return false,
    }
    let a = source[0];
    if (0x80..0xc2).contains(&a) {
        return false;
    }
    if a > 0xf4 {
        return false;
    }
    true
}

#[inline(always)]
fn utf8_second_byte_legal(b0: u8, a: u8) -> bool {
    match b0 {
        0xe0 => (0xa0..=0xbf).contains(&a),
        0xed => (0x80..=0x9f).contains(&a),
        0xf0 => (0x90..=0xbf).contains(&a),
        0xf4 => (0x80..=0x8f).contains(&a),
        _ => (0x80..=0xbf).contains(&a),
    }
}

/// Writes the two-byte "invalid character" marker; `dst` must hold two bytes
/// and `encoding` must be multibyte (C asserts).
pub fn pg_encoding_set_invalid(encoding: i32, dst: &mut [u8]) {
    debug_assert!(pg_encoding_max_length(encoding) > 1);
    dst[0] = if encoding == PG_UTF8 {
        0xc0
    } else {
        NONUTF8_INVALID_BYTE0
    };
    dst[1] = NONUTF8_INVALID_BYTE1;
}

#[inline(always)]
fn table_index(encoding: i32) -> usize {
    if pg_valid_encoding(encoding) {
        encoding as usize
    } else {
        PG_SQL_ASCII as usize
    }
}

pub fn pg_encoding_mblen(encoding: i32, mbstr: &[u8]) -> i32 {
    (pg_wchar_table[table_index(encoding)].mblen)(mbstr)
}

pub fn pg_encoding_mblen_or_incomplete(encoding: i32, mbstr: &[u8]) -> i32 {
    if mbstr.is_empty() || (encoding == PG_GB18030 && is_highbit_set(mbstr[0]) && mbstr.len() < 2)
    {
        return i32::MAX;
    }
    pg_encoding_mblen(encoding, mbstr)
}

pub fn pg_encoding_mblen_bounded(encoding: i32, mbstr: &[u8]) -> i32 {
    let mblen = pg_encoding_mblen(encoding, mbstr) as usize;
    match mbstr[..mblen.min(mbstr.len())].iter().position(|&b| b == 0) {
        Some(p) => p as i32,
        None => mblen as i32,
    }
}

pub fn pg_encoding_dsplen(encoding: i32, mbstr: &[u8]) -> i32 {
    (pg_wchar_table[table_index(encoding)].dsplen)(mbstr)
}

pub fn pg_encoding_verifymbchar(encoding: i32, mbstr: &[u8]) -> i32 {
    (pg_wchar_table[table_index(encoding)].mbverifychar)(mbstr)
}

pub fn pg_encoding_verifymbstr(encoding: i32, mbstr: &[u8]) -> i32 {
    (pg_wchar_table[table_index(encoding)].mbverifystr)(mbstr)
}

pub fn pg_encoding_max_length(encoding: i32) -> i32 {
    debug_assert!(pg_valid_encoding(encoding));
    pg_wchar_table[table_index(encoding)].maxmblen
}

pub const fn is_valid_unicode_codepoint(c: pg_wchar) -> bool {
    c > 0 && c <= 0x10ffff
}

pub const fn is_utf16_surrogate_first(c: pg_wchar) -> bool {
    c >= 0xd800 && c <= 0xdbff
}

pub const fn is_utf16_surrogate_second(c: pg_wchar) -> bool {
    c >= 0xdc00 && c <= 0xdfff
}

pub const fn surrogate_pair_to_codepoint(first: pg_wchar, second: pg_wchar) -> pg_wchar {
    ((first & 0x3ff) << 10) + 0x10000 + (second & 0x3ff)
}

/// `c` must hold a complete sequence (C reads the trailing bytes unchecked).
#[inline]
pub fn utf8_to_unicode(c: &[u8]) -> pg_wchar {
    let b0 = c[0];
    if b0 & 0x80 == 0 {
        b0 as pg_wchar
    } else if b0 & 0xe0 == 0xc0 {
        (((b0 & 0x1f) as u32) << 6) | (c[1] & 0x3f) as u32
    } else if b0 & 0xf0 == 0xe0 {
        (((b0 & 0x0f) as u32) << 12) | (((c[1] & 0x3f) as u32) << 6) | (c[2] & 0x3f) as u32
    } else if b0 & 0xf8 == 0xf0 {
        (((b0 & 0x07) as u32) << 18)
            | (((c[1] & 0x3f) as u32) << 12)
            | (((c[2] & 0x3f) as u32) << 6)
            | (c[3] & 0x3f) as u32
    } else {
        0xffffffff
    }
}

/// `utf8string` must hold `unicode_utf8len(c)` bytes.
#[inline]
pub fn unicode_to_utf8(c: pg_wchar, utf8string: &mut [u8]) {
    if c <= 0x7f {
        utf8string[0] = c as u8;
    } else if c <= 0x7ff {
        utf8string[0] = 0xc0 | ((c >> 6) & 0x1f) as u8;
        utf8string[1] = 0x80 | (c & 0x3f) as u8;
    } else if c <= 0xffff {
        utf8string[0] = 0xe0 | ((c >> 12) & 0x0f) as u8;
        utf8string[1] = 0x80 | ((c >> 6) & 0x3f) as u8;
        utf8string[2] = 0x80 | (c & 0x3f) as u8;
    } else {
        utf8string[0] = 0xf0 | ((c >> 18) & 0x07) as u8;
        utf8string[1] = 0x80 | ((c >> 12) & 0x3f) as u8;
        utf8string[2] = 0x80 | ((c >> 6) & 0x3f) as u8;
        utf8string[3] = 0x80 | (c & 0x3f) as u8;
    }
}

pub const fn unicode_utf8len(c: pg_wchar) -> i32 {
    if c <= 0x7f {
        1
    } else if c <= 0x7ff {
        2
    } else if c <= 0xffff {
        3
    } else {
        4
    }
}

const SINGLE_BYTE_TBL: pg_wchar_tbl = pg_wchar_tbl {
    mb2wchar_with_len: Some(pg_latin12wchar_with_len),
    wchar2mb_with_len: Some(pg_wchar2single_with_len),
    mblen: pg_latin1_mblen,
    dsplen: pg_latin1_dsplen,
    mbverifychar: pg_latin1_verifychar,
    mbverifystr: pg_latin1_verifystr,
    maxmblen: 1,
};

pub static pg_wchar_table: [pg_wchar_tbl; _PG_LAST_ENCODING_ as usize] = [
    // PG_SQL_ASCII
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_ascii2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2single_with_len),
        mblen: pg_ascii_mblen,
        dsplen: pg_ascii_dsplen,
        mbverifychar: pg_ascii_verifychar,
        mbverifystr: pg_ascii_verifystr,
        maxmblen: 1,
    },
    // PG_EUC_JP
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_eucjp2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: pg_eucjp_mblen,
        dsplen: pg_eucjp_dsplen,
        mbverifychar: pg_eucjp_verifychar,
        mbverifystr: pg_eucjp_verifystr,
        maxmblen: 3,
    },
    // PG_EUC_CN
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_euccn2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: pg_euccn_mblen,
        dsplen: pg_euccn_dsplen,
        mbverifychar: pg_euckr_verifychar,
        mbverifystr: pg_euckr_verifystr,
        maxmblen: 3,
    },
    // PG_EUC_KR
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_euckr2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: pg_euckr_mblen,
        dsplen: pg_euckr_dsplen,
        mbverifychar: pg_euckr_verifychar,
        mbverifystr: pg_euckr_verifystr,
        maxmblen: 3,
    },
    // PG_EUC_TW
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_euctw2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: pg_euctw_mblen,
        dsplen: pg_euctw_dsplen,
        mbverifychar: pg_euctw_verifychar,
        mbverifystr: pg_euctw_verifystr,
        maxmblen: 4,
    },
    // PG_EUC_JIS_2004
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_eucjp2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2euc_with_len),
        mblen: pg_eucjp_mblen,
        dsplen: pg_eucjp_dsplen,
        mbverifychar: pg_eucjp_verifychar,
        mbverifystr: pg_eucjp_verifystr,
        maxmblen: 3,
    },
    // PG_UTF8
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_utf2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2utf_with_len),
        mblen: pg_utf_mblen,
        dsplen: pg_utf_dsplen,
        mbverifychar: pg_utf8_verifychar,
        mbverifystr: pg_utf8_verifystr,
        maxmblen: 4,
    },
    // PG_MULE_INTERNAL
    pg_wchar_tbl {
        mb2wchar_with_len: Some(pg_mule2wchar_with_len),
        wchar2mb_with_len: Some(pg_wchar2mule_with_len),
        mblen: pg_mule_mblen,
        dsplen: pg_mule_dsplen,
        mbverifychar: pg_mule_verifychar,
        mbverifystr: pg_mule_verifystr,
        maxmblen: 4,
    },
    // PG_LATIN1 .. PG_KOI8U (27 single-byte encodings)
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    SINGLE_BYTE_TBL,
    // PG_SJIS
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_sjis_mblen,
        dsplen: pg_sjis_dsplen,
        mbverifychar: pg_sjis_verifychar,
        mbverifystr: pg_sjis_verifystr,
        maxmblen: 2,
    },
    // PG_BIG5
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_big5_mblen,
        dsplen: pg_big5_dsplen,
        mbverifychar: pg_big5_verifychar,
        mbverifystr: pg_big5_verifystr,
        maxmblen: 2,
    },
    // PG_GBK
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_gbk_mblen,
        dsplen: pg_gbk_dsplen,
        mbverifychar: pg_gbk_verifychar,
        mbverifystr: pg_gbk_verifystr,
        maxmblen: 2,
    },
    // PG_UHC
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_uhc_mblen,
        dsplen: pg_uhc_dsplen,
        mbverifychar: pg_uhc_verifychar,
        mbverifystr: pg_uhc_verifystr,
        maxmblen: 2,
    },
    // PG_GB18030
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_gb18030_mblen,
        dsplen: pg_gb18030_dsplen,
        mbverifychar: pg_gb18030_verifychar,
        mbverifystr: pg_gb18030_verifystr,
        maxmblen: 4,
    },
    // PG_JOHAB
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_johab_mblen,
        dsplen: pg_johab_dsplen,
        mbverifychar: pg_johab_verifychar,
        mbverifystr: pg_johab_verifystr,
        maxmblen: 3,
    },
    // PG_SHIFT_JIS_2004
    pg_wchar_tbl {
        mb2wchar_with_len: None,
        wchar2mb_with_len: None,
        mblen: pg_sjis_mblen,
        dsplen: pg_sjis_dsplen,
        mbverifychar: pg_sjis_verifychar,
        mbverifystr: pg_sjis_verifystr,
        maxmblen: 2,
    },
];

include!("tables.rs");
