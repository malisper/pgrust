//! unicode_case.c: Unicode Default Case Conversion over UTF-8 (simple
//! mappings; special/full mappings incl. Final_Sigma when `full`). Tables
//! build-time generated from the committed unicode_case_table.h (PG 18.3).

mod tables {
    include!(concat!(env!("OUT_DIR"), "/tables.rs"));
}
#[cfg(test)]
mod tests;

use tables::*;
use wchar::{unicode_to_utf8, unicode_utf8len, utf8_to_unicode};

pub const MAX_CASE_EXPANSION: usize = 3;
const PG_U_FINAL_SIGMA: i16 = 1;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CaseKind {
    Lower = 0,
    Title = 1,
    Upper = 2,
    Fold = 3,
}

fn casekind_map(kind: CaseKind) -> &'static [u32; 1704] {
    match kind {
        CaseKind::Lower => &CASE_MAP_LOWER,
        CaseKind::Title => &CASE_MAP_TITLE,
        CaseKind::Upper => &CASE_MAP_UPPER,
        CaseKind::Fold => &CASE_MAP_FOLD,
    }
}

fn case_index(cp: u32) -> u16 {
    if cp < 0x0588 {
        return CASE_MAP[cp as usize];
    }
    match CASE_INDEX_RANGES.binary_search_by(|&(start, end, _)| {
        if cp < start {
            core::cmp::Ordering::Greater
        } else if cp >= end {
            core::cmp::Ordering::Less
        } else {
            core::cmp::Ordering::Equal
        }
    }) {
        Ok(i) => {
            let (start, _, base) = CASE_INDEX_RANGES[i];
            CASE_MAP[(cp - start) as usize + base as usize]
        }
        Err(_) => 0,
    }
}

fn find_case_map(ucs: u32, map: &[u32; 1704]) -> u32 {
    if ucs < 0x80 {
        return map[ucs as usize + 1];
    }
    map[case_index(ucs) as usize]
}

pub fn unicode_lowercase_simple(code: u32) -> u32 {
    let cp = find_case_map(code, &CASE_MAP_LOWER);
    if cp != 0 { cp } else { code }
}

pub fn unicode_titlecase_simple(code: u32) -> u32 {
    let cp = find_case_map(code, &CASE_MAP_TITLE);
    if cp != 0 { cp } else { code }
}

pub fn unicode_uppercase_simple(code: u32) -> u32 {
    let cp = find_case_map(code, &CASE_MAP_UPPER);
    if cp != 0 { cp } else { code }
}

pub fn unicode_casefold_simple(code: u32) -> u32 {
    let cp = find_case_map(code, &CASE_MAP_FOLD);
    if cp != 0 { cp } else { code }
}

pub fn unicode_strlower(dst: &mut [u8], src: &[u8], full: bool) -> usize {
    convert_case(dst, src, CaseKind::Lower, full, None::<&mut fn() -> usize>)
}

pub fn unicode_strupper(dst: &mut [u8], src: &[u8], full: bool) -> usize {
    convert_case(dst, src, CaseKind::Upper, full, None::<&mut fn() -> usize>)
}

pub fn unicode_strfold(dst: &mut [u8], src: &[u8], full: bool) -> usize {
    convert_case(dst, src, CaseKind::Fold, full, None::<&mut fn() -> usize>)
}

// wbnext yields word-boundary offsets: 0 first, then each subsequent
// boundary, then src.len() (C's WordBoundaryNext contract).
pub fn unicode_strtitle<W: FnMut() -> usize>(
    dst: &mut [u8],
    src: &[u8],
    full: bool,
    wbnext: &mut W,
) -> usize {
    convert_case(dst, src, CaseKind::Title, full, Some(wbnext))
}

enum CaseMapResult {
    CaseSelf,
    Simple(u32),
    Special(&'static [u32; 3]),
}

// Result length may exceed dst.len(); only what fits is written, with a
// trailing NUL iff there is room (C convention; callers grow and retry).
fn convert_case<W: FnMut() -> usize>(
    dst: &mut [u8],
    src: &[u8],
    str_casekind: CaseKind,
    full: bool,
    mut wbnext: Option<&mut W>,
) -> usize {
    let dstsize = dst.len();
    let mut chr_casekind = str_casekind;
    let mut srcoff = 0usize;
    let mut result_len = 0usize;
    let mut boundary = 0usize;

    debug_assert!((str_casekind == CaseKind::Title) == wbnext.is_some());
    if let Some(wb) = wbnext.as_mut() {
        boundary = wb();
        debug_assert!(boundary == 0);
    }

    while srcoff < src.len() && src[srcoff] != 0 {
        let u1 = utf8_to_unicode(&src[srcoff..]);
        let u1len = unicode_utf8len(u1) as usize;

        if str_casekind == CaseKind::Title {
            if srcoff == boundary {
                chr_casekind = if full { CaseKind::Title } else { CaseKind::Upper };
                boundary = wbnext.as_mut().expect("title needs wbnext")();
            } else {
                chr_casekind = CaseKind::Lower;
            }
        }

        match casemap(u1, chr_casekind, full, src, srcoff) {
            CaseMapResult::CaseSelf => {
                if result_len + u1len <= dstsize {
                    dst[result_len..result_len + u1len]
                        .copy_from_slice(&src[srcoff..srcoff + u1len]);
                }
                result_len += u1len;
            }
            CaseMapResult::Simple(u2) => {
                let u2len = unicode_utf8len(u2) as usize;
                if result_len + u2len <= dstsize {
                    unicode_to_utf8(u2, &mut dst[result_len..]);
                }
                result_len += u2len;
            }
            CaseMapResult::Special(special) => {
                for &u2 in special.iter().take_while(|&&u| u != 0) {
                    let u2len = unicode_utf8len(u2) as usize;
                    if result_len + u2len <= dstsize {
                        unicode_to_utf8(u2, &mut dst[result_len..]);
                    }
                    result_len += u2len;
                }
            }
        }

        srcoff += u1len;
    }

    if result_len < dstsize {
        dst[result_len] = 0;
    }
    result_len
}

// Final_Sigma (Unicode Table 3-17): directly preceded by a Cased character
// and not directly followed by one, skipping Case_Ignorable in both scans.
fn check_final_sigma(s: &[u8], offset: usize) -> bool {
    if offset == 0 {
        return false;
    }

    // C falls through (rather than failing) if this scan runs off the start.
    for i in (0..offset).rev() {
        let b = s[i];
        if b & 0x80 == 0 || b & 0xC0 == 0xC0 {
            let curr = utf8_to_unicode(&s[i..]);
            if unicode_category::pg_u_prop_case_ignorable(curr) {
                continue;
            } else if unicode_category::pg_u_prop_cased(curr) {
                break;
            }
            return false;
        }
        debug_assert!(b & 0xC0 == 0x80);
    }

    let mut i = offset + 1;
    while i < s.len() && s[i] != 0 {
        let b = s[i];
        if b & 0x80 == 0 || b & 0xC0 == 0xC0 {
            let curr = utf8_to_unicode(&s[i..]);
            if unicode_category::pg_u_prop_case_ignorable(curr) {
                i += 1;
                continue;
            } else if unicode_category::pg_u_prop_cased(curr) {
                return false;
            }
            break;
        }
        debug_assert!(b & 0xC0 == 0x80);
        i += 1;
    }
    true
}

fn check_special_conditions(conditions: i16, s: &[u8], offset: usize) -> bool {
    if conditions == 0 {
        return true;
    }
    debug_assert!(conditions == PG_U_FINAL_SIGMA);
    check_final_sigma(s, offset)
}

fn casemap(u1: u32, casekind: CaseKind, full: bool, src: &[u8], srcoff: usize) -> CaseMapResult {
    if u1 < 0x80 {
        // Index 0 of every map is reserved; data starts at 1.
        return CaseMapResult::Simple(casekind_map(casekind)[u1 as usize + 1]);
    }

    let idx = case_index(u1) as usize;
    if idx == 0 {
        return CaseMapResult::CaseSelf;
    }

    if full && CASE_MAP_SPECIAL[idx] != 0 {
        let (conditions, map) = &SPECIAL_CASE[CASE_MAP_SPECIAL[idx] as usize];
        if check_special_conditions(*conditions, src, srcoff) {
            return CaseMapResult::Special(&map[casekind as usize]);
        }
    }

    CaseMapResult::Simple(casekind_map(casekind)[idx])
}
