#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
//! Idiomatic port of PostgreSQL's `src/common/unicode_case.c`.
//!
//! The case-conversion routines (`unicode_strlower`/`upper`/`fold`/`title`)
//! build a result buffer one mapped character at a time, mirroring C's
//! `unicode_strlower` et al. which `palloc` an output buffer in the current
//! memory context and grow it as the conversion expands characters. This port
//! builds the result into a context-charged [`PgVec<'mcx, u8>`] (via a
//! [`PgString<'mcx>`] wrapper) allocated in the caller-supplied [`Mcx`]; each
//! append grows it fallibly with `try_reserve`, so an allocator refusal returns
//! `Err(PgError)` rather than aborting (C's `palloc` would `ereport(ERROR)`).
//! The conversion algorithm is unchanged from the idiomatic base; only the
//! buffer type/lifetime differs.

#![forbid(unsafe_code)]

mod tables;

use common_unicode_category::{pg_u_prop_case_ignorable, pg_u_prop_cased};
use mcx::{Mcx, PgString, PgVec};
use tables::*;
use types_error::PgResult;

/// A Unicode code point. Alias of the fabled `PgWChar` (`pg_wchar` in C).
pub type pg_wchar = types_core::PgWChar;

pub type int16 = i16;
pub type uint8 = u8;
pub type uint16 = u16;
pub type CaseKind = u32;
pub type CaseMapResult = u32;

pub const CaseLower: CaseKind = 0;
pub const CaseTitle: CaseKind = 1;
pub const CaseUpper: CaseKind = 2;
pub const CaseFold: CaseKind = 3;
pub const NCaseKind: CaseKind = 4;

pub const CASEMAP_SELF: CaseMapResult = 0;
pub const CASEMAP_SIMPLE: CaseMapResult = 1;
pub const CASEMAP_SPECIAL: CaseMapResult = 2;

pub const MAX_CASE_EXPANSION: usize = 3;
pub const PG_U_FINAL_SIGMA: int16 = 1 << 0;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct pg_special_case {
    conditions: int16,
    map: [[pg_wchar; MAX_CASE_EXPANSION]; NCaseKind as usize],
}

impl pg_special_case {
    pub const fn conditions(&self) -> int16 {
        self.conditions
    }

    pub const fn map(&self, casekind: CaseKind) -> &[pg_wchar; MAX_CASE_EXPANSION] {
        &self.map[casekind as usize]
    }
}

pub fn unicode_lowercase_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &CASE_MAP_LOWER);
    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_titlecase_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &CASE_MAP_TITLE);
    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_uppercase_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &CASE_MAP_UPPER);
    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_casefold_simple(code: pg_wchar) -> pg_wchar {
    let cp = find_case_map(code, &CASE_MAP_FOLD);
    if cp != 0 {
        cp
    } else {
        code
    }
}

pub fn unicode_strlower<'mcx>(mcx: Mcx<'mcx>, src: &str, full: bool) -> PgResult<PgVec<'mcx, u8>> {
    convert_case(mcx, src, CaseLower, full, None::<fn() -> usize>)
}

pub fn unicode_strupper<'mcx>(mcx: Mcx<'mcx>, src: &str, full: bool) -> PgResult<PgVec<'mcx, u8>> {
    convert_case(mcx, src, CaseUpper, full, None::<fn() -> usize>)
}

pub fn unicode_strfold<'mcx>(mcx: Mcx<'mcx>, src: &str, full: bool) -> PgResult<PgVec<'mcx, u8>> {
    convert_case(mcx, src, CaseFold, full, None::<fn() -> usize>)
}

pub fn unicode_strtitle<'mcx>(
    mcx: Mcx<'mcx>,
    src: &str,
    full: bool,
    wbnext: impl FnMut() -> usize,
) -> PgResult<PgVec<'mcx, u8>> {
    convert_case(mcx, src, CaseTitle, full, Some(wbnext))
}

/// Drives a case conversion, building the result into a context-charged
/// `PgVec<'mcx, u8>` allocated in `mcx` (C: `palloc` in the current context).
/// Each append grows the buffer fallibly via `try_reserve`; an allocator
/// refusal returns `Err(PgError)`, the analog of C's `palloc` `ereport(ERROR)`.
fn convert_case<'mcx>(
    mcx: Mcx<'mcx>,
    src: &str,
    str_casekind: CaseKind,
    full: bool,
    wbnext: Option<impl FnMut() -> usize>,
) -> PgResult<PgVec<'mcx, u8>> {
    build_case(mcx, src, str_casekind, full, wbnext).map(PgString::into_bytes)
}

/// The core conversion loop. Builds into a `PgString<'mcx>` charged to `mcx`;
/// sizes are bounded by the in-memory input expanded by at most
/// `MAX_CASE_EXPANSION`, so no append can amplify.
fn build_case<'mcx>(
    mcx: Mcx<'mcx>,
    src: &str,
    str_casekind: CaseKind,
    full: bool,
    mut wbnext: Option<impl FnMut() -> usize>,
) -> PgResult<PgString<'mcx>> {
    let src = src.split_once('\0').map_or(src, |(prefix, _)| prefix);
    let mut result = PgString::new_in(mcx);
    // Scratch for encoding a single `char` into UTF-8 without an allocation.
    let mut ch_buf = [0u8; 4];
    let mut boundary = 0;

    if str_casekind == CaseTitle {
        boundary = wbnext.as_mut().expect("titlecase requires word boundaries")();
    }

    for (srcoff, ch) in src.char_indices() {
        let mut chr_casekind = str_casekind;
        if str_casekind == CaseTitle {
            if srcoff == boundary {
                chr_casekind = if full { CaseTitle } else { CaseUpper };
                boundary = wbnext.as_mut().expect("titlecase requires word boundaries")();
            } else {
                chr_casekind = CaseLower;
            }
        }

        match casemap(ch as pg_wchar, chr_casekind, full, src, srcoff) {
            CaseMap::Self_ => {
                let s = ch.encode_utf8(&mut ch_buf);
                result.try_push_str(s)?;
            }
            CaseMap::Simple(code) => {
                push_pg_wchar(&mut result, &mut ch_buf, code)?;
            }
            CaseMap::Special(codes) => {
                for code in codes.iter().copied().take_while(|code| *code != 0) {
                    push_pg_wchar(&mut result, &mut ch_buf, code)?;
                }
            }
        }
    }

    Ok(result)
}

enum CaseMap {
    Self_,
    Simple(pg_wchar),
    Special(&'static [pg_wchar; MAX_CASE_EXPANSION]),
}

fn casemap(u1: pg_wchar, casekind: CaseKind, full: bool, src: &str, srcoff: usize) -> CaseMap {
    if u1 < 0x80 {
        return CaseMap::Simple(casekind_map(casekind)[u1 as usize + 1]);
    }

    let idx = case_index(u1) as usize;
    if idx == 0 {
        return CaseMap::Self_;
    }

    let special_idx = CASE_MAP_SPECIAL[idx] as usize;
    if full
        && special_idx != 0
        && check_special_conditions(SPECIAL_CASE[special_idx].conditions(), src, srcoff)
    {
        return CaseMap::Special(SPECIAL_CASE[special_idx].map(casekind));
    }

    CaseMap::Simple(casekind_map(casekind)[idx])
}

fn check_special_conditions(conditions: int16, src: &str, offset: usize) -> bool {
    match conditions {
        0 => true,
        PG_U_FINAL_SIGMA => check_final_sigma(src, offset),
        _ => false,
    }
}

fn check_final_sigma(src: &str, offset: usize) -> bool {
    if offset == 0 {
        return false;
    }

    // Iterate backwards, looking for a Cased character. Case_Ignorable
    // characters are skipped. A non-ignorable, non-cased character
    // disqualifies the Final_Sigma condition. If the backward scan finds no
    // such character (e.g. all preceding characters are ignorable), the C
    // implementation falls through to the forward check rather than failing.
    for (_, ch) in src[..offset].char_indices().rev() {
        let code = ch as pg_wchar;
        if pg_u_prop_case_ignorable(code) {
            continue;
        } else if pg_u_prop_cased(code) {
            break;
        } else {
            return false;
        }
    }

    let after = offset + src[offset..].chars().next().map_or(0, char::len_utf8);
    for ch in src[after..].chars() {
        let code = ch as pg_wchar;
        if pg_u_prop_case_ignorable(code) {
            continue;
        }
        return !pg_u_prop_cased(code);
    }

    true
}

fn find_case_map(ucs: pg_wchar, map: &[pg_wchar; 1704]) -> pg_wchar {
    if ucs < 0x80 {
        return map[ucs as usize + 1];
    }
    map[case_index(ucs) as usize]
}

fn casekind_map(casekind: CaseKind) -> &'static [pg_wchar; 1704] {
    match casekind {
        CaseLower => &CASE_MAP_LOWER,
        CaseTitle => &CASE_MAP_TITLE,
        CaseUpper => &CASE_MAP_UPPER,
        CaseFold => &CASE_MAP_FOLD,
        _ => &CASE_MAP_LOWER,
    }
}

fn case_index(cp: pg_wchar) -> uint16 {
    if cp < 0x588 {
        return CASE_MAP[cp as usize];
    }
    if cp < 0xabc0 {
        if cp < 0x2185 {
            if (0x10a0..0x1100).contains(&cp) {
                return CASE_MAP[(cp - 0x10a0 + 1416) as usize];
            } else if cp >= 0x13a0 {
                if cp < 0x13fe {
                    return CASE_MAP[(cp - 0x13a0 + 1512) as usize];
                } else if cp >= 0x1c80 {
                    return CASE_MAP[(cp - 0x1c80 + 1606) as usize];
                }
            }
        } else if cp >= 0x24b6 {
            if cp < 0x2d2e {
                if cp < 0x24ea {
                    return CASE_MAP[(cp - 0x24b6 + 2891) as usize];
                } else if cp >= 0x2c00 {
                    return CASE_MAP[(cp - 0x2c00 + 2943) as usize];
                }
            } else if cp >= 0xa640 {
                if cp < 0xa7f7 {
                    return CASE_MAP[(cp - 0xa640 + 3245) as usize];
                } else if cp >= 0xab53 {
                    return CASE_MAP[(cp - 0xab53 + 3684) as usize];
                }
            }
        }
    } else if cp >= 0xfb00 {
        if cp < 0x10d86 {
            if cp < 0xff5b {
                if cp < 0xfb18 {
                    return CASE_MAP[(cp - 0xfb00 + 3793) as usize];
                } else if cp >= 0xff21 {
                    return CASE_MAP[(cp - 0xff21 + 3817) as usize];
                }
            } else if cp >= 0x10400 {
                if cp < 0x105bd {
                    return CASE_MAP[(cp - 0x10400 + 3875) as usize];
                } else if cp >= 0x10c80 {
                    return CASE_MAP[(cp - 0x10c80 + 4320) as usize];
                }
            }
        } else if cp >= 0x118a0 {
            if cp < 0x16e80 {
                if cp < 0x118e0 {
                    return CASE_MAP[(cp - 0x118a0 + 4582) as usize];
                } else if cp >= 0x16e40 {
                    return CASE_MAP[(cp - 0x16e40 + 4646) as usize];
                }
            } else if cp >= 0x1e900 && cp < 0x1e944 {
                return CASE_MAP[(cp - 0x1e900 + 4710) as usize];
            }
        }
    }
    0
}

/// Appends a [`pg_wchar`] code point to the context-charged result buffer.
///
/// Invalid code points (those not representing a scalar Unicode value) are
/// skipped, matching the original code's silent `char::from_u32` filter.
/// `ch_buf` is reusable scratch for encoding the char into UTF-8 without an
/// allocation. Returns `Err(PgError)` if the buffer's growth was refused by the
/// allocator.
fn push_pg_wchar(out: &mut PgString, ch_buf: &mut [u8; 4], code: pg_wchar) -> PgResult<()> {
    if let Some(ch) = char::from_u32(code) {
        let s = ch.encode_utf8(ch_buf);
        out.try_push_str(s)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    fn s<'a>(v: &'a PgResult<PgVec<'a, u8>>) -> &'a str {
        match v {
            Ok(bytes) => core::str::from_utf8(bytes).expect("conversion output is UTF-8"),
            Err(e) => panic!("conversion failed: {}", e),
        }
    }

    #[test]
    fn special_case_table_accessors_match_data() {
        // The first non-empty entry maps lowercase 0x00df ("ß") to the
        // uppercase expansion "SS" (0x0053, 0x0053) and the folded form "ss".
        let sc = &SPECIAL_CASE[1];
        assert_eq!(sc.conditions(), 0);
        assert_eq!(sc.map(CaseLower), &[0x0000df, 0x000000, 0x000000]);
        assert_eq!(sc.map(CaseUpper), &[0x000053, 0x000053, 0x000000]);
        assert_eq!(sc.map(CaseFold), &[0x000073, 0x000073, 0x000000]);
    }

    #[test]
    fn simple_case_maps_ascii_and_non_ascii() {
        assert_eq!(unicode_uppercase_simple('a' as pg_wchar), 'A' as pg_wchar);
        assert_eq!(unicode_lowercase_simple('A' as pg_wchar), 'a' as pg_wchar);
        assert_eq!(unicode_lowercase_simple('Σ' as pg_wchar), 'σ' as pg_wchar);
        assert_eq!(unicode_casefold_simple('ß' as pg_wchar), 'ß' as pg_wchar);
    }

    #[test]
    fn string_case_maps_ascii() {
        let ctx = MemoryContext::new("t");
        let m = ctx.mcx();
        assert_eq!(s(&unicode_strlower(m, "Hello", true)), "hello");
        assert_eq!(s(&unicode_strupper(m, "Hello", true)), "HELLO");
        assert_eq!(s(&unicode_strfold(m, "Hello", true)), "hello");
    }

    #[test]
    fn full_case_expands_special_mappings() {
        let ctx = MemoryContext::new("t");
        let m = ctx.mcx();
        assert_eq!(s(&unicode_strupper(m, "ß", true)), "SS");
        assert_eq!(s(&unicode_strupper(m, "ß", false)), "ß");
        assert_eq!(s(&unicode_strfold(m, "ﬃ", true)), "ffi");
    }

    #[test]
    fn final_sigma_condition_matches_word_context() {
        let ctx = MemoryContext::new("t");
        let m = ctx.mcx();
        assert_eq!(s(&unicode_strlower(m, "ΟΣ", true)), "ος");
        assert_eq!(s(&unicode_strlower(m, "ΟΣΑ", true)), "οσα");
        assert_eq!(s(&unicode_strlower(m, "Σ", true)), "σ");
    }

    #[test]
    fn final_sigma_preceded_only_by_ignorable_falls_through() {
        // U+0301 (COMBINING ACUTE ACCENT) is Case_Ignorable and not Cased.
        // With the sigma at offset > 0 preceded solely by an ignorable
        // character, the C implementation's backward scan finds no Cased
        // character but also no disqualifying non-ignorable character, so it
        // falls through to the forward check. Here the sigma is at the end of
        // the string (not followed by a Cased character), so Final_Sigma
        // applies and Σ lowercases to the final form ς (U+03C2) rather than
        // the medial σ (U+03C3).
        assert!(pg_u_prop_case_ignorable('\u{0301}' as pg_wchar));
        assert!(!pg_u_prop_cased('\u{0301}' as pg_wchar));
        let ctx = MemoryContext::new("t");
        assert_eq!(
            s(&unicode_strlower(ctx.mcx(), "\u{0301}\u{03A3}", true)),
            "\u{0301}\u{03C2}"
        );
    }

    #[test]
    fn titlecase_uses_word_boundaries() {
        let boundaries = [0, 5, 6, 11];
        let mut iter = boundaries.into_iter();
        let ctx = MemoryContext::new("t");
        assert_eq!(
            s(&unicode_strtitle(ctx.mcx(), "hello world", true, move || iter
                .next()
                .unwrap())),
            "Hello World"
        );
    }

    #[test]
    fn nul_terminates_like_c_string_source() {
        let ctx = MemoryContext::new("t");
        assert_eq!(s(&unicode_strupper(ctx.mcx(), "abc\0def", true)), "ABC");
    }
}
