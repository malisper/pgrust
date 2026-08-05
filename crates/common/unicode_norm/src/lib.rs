#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod qc_tables;
mod tables;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;
use wchar::pg_wchar;

use qc_tables::{NFC_QC_h, NFKC_QC_h, UnicodeNormProps_NFC_QC, UnicodeNormProps_NFKC_QC};
use tables::{UnicodeDecompMain, UnicodeDecomp_codepoints};

pub(crate) type uint8 = u8;
pub(crate) type uint16 = u16;
pub(crate) type uint32 = u32;

pub type UnicodeNormalizationForm = u32;

pub const UNICODE_NFC: UnicodeNormalizationForm = 0;
pub const UNICODE_NFD: UnicodeNormalizationForm = 1;
pub const UNICODE_NFKC: UnicodeNormalizationForm = 2;
pub const UNICODE_NFKD: UnicodeNormalizationForm = 3;

pub type UnicodeNormalizationQC = i32;

pub const UNICODE_NORM_QC_NO: UnicodeNormalizationQC = 0;
pub const UNICODE_NORM_QC_YES: UnicodeNormalizationQC = 1;
pub const UNICODE_NORM_QC_MAYBE: UnicodeNormalizationQC = -1;

#[derive(Copy, Clone)]
pub struct pg_unicode_decomposition {
    pub codepoint: uint32,
    pub comb_class: uint8,
    pub dec_size_flags: uint8,
    pub dec_index: uint16,
}

// C packs codepoint:21/quickcheck:4 bit fields only to save table space; the
// values are unchanged in plain fields.
#[derive(Copy, Clone)]
pub struct pg_unicode_normprops {
    pub codepoint: uint32,
    pub quickcheck: UnicodeNormalizationQC,
}

struct pg_unicode_norminfo {
    normprops: &'static [pg_unicode_normprops],
    hash_table: &'static [i16],
    hash_divisor: u32,
    hash_b_init: u32,
}

static UnicodeNormInfo_NFC_QC: pg_unicode_norminfo = pg_unicode_norminfo {
    normprops: &UnicodeNormProps_NFC_QC,
    hash_table: &NFC_QC_h,
    hash_divisor: 2505,
    hash_b_init: 0,
};

static UnicodeNormInfo_NFKC_QC: pg_unicode_norminfo = pg_unicode_norminfo {
    normprops: &UnicodeNormProps_NFKC_QC,
    hash_table: &NFKC_QC_h,
    hash_divisor: 10193,
    hash_b_init: 3,
};

impl pg_unicode_norminfo {
    // Generated *_QC_hash_func: the key is the codepoint in network byte order.
    fn hash(&self, key: pg_wchar) -> i32 {
        let mut a: u32 = 0;
        let mut b: u32 = self.hash_b_init;
        for &c in key.to_be_bytes().iter() {
            a = a.wrapping_mul(257).wrapping_add(c as u32);
            b = b.wrapping_mul(8191).wrapping_add(c as u32);
        }
        let ha = self.hash_table[(a % self.hash_divisor) as usize] as i32;
        let hb = self.hash_table[(b % self.hash_divisor) as usize] as i32;
        ha + hb
    }
}

// dec_size_flags is packed with c_int literals in the generated tables.rs;
// keep the type so the bit patterns match exactly.
const DECOMP_NO_COMPOSE: core::ffi::c_int = 0x80;
const DECOMP_INLINE: core::ffi::c_int = 0x40;
const DECOMP_COMPAT: core::ffi::c_int = 0x20;
const DECOMP_SIZE_MASK: uint8 = 0x1f;

const SBASE: pg_wchar = 0xac00;
const LBASE: pg_wchar = 0x1100;
const VBASE: pg_wchar = 0x1161;
const TBASE: pg_wchar = 0x11a7;
const LCOUNT: pg_wchar = 19;
const VCOUNT: pg_wchar = 21;
const TCOUNT: pg_wchar = 28;
const NCOUNT: pg_wchar = VCOUNT * TCOUNT;
const SCOUNT: pg_wchar = LCOUNT * NCOUNT;

impl pg_unicode_decomposition {
    fn decomposition_size(self) -> usize {
        (self.dec_size_flags & DECOMP_SIZE_MASK) as usize
    }

    fn is_inline(self) -> bool {
        self.dec_size_flags & DECOMP_INLINE as uint8 != 0
    }

    fn is_compat(self) -> bool {
        self.dec_size_flags & DECOMP_COMPAT as uint8 != 0
    }

    fn no_compose(self) -> bool {
        self.dec_size_flags & DECOMP_NO_COMPOSE as uint8 != 0
    }
}

fn is_compat_form(form: UnicodeNormalizationForm) -> bool {
    form == UNICODE_NFKC || form == UNICODE_NFKD
}

fn is_recompose_form(form: UnicodeNormalizationForm) -> bool {
    form == UNICODE_NFC || form == UNICODE_NFKC
}

// Backend C uses a generated perfect hash here; the C comment on
// get_code_entry says the frontend binary search over the same sorted table is
// interchangeable, so this uses the binary search.
fn get_code_entry(code: pg_wchar) -> Option<&'static pg_unicode_decomposition> {
    UnicodeDecompMain
        .binary_search_by_key(&code, |entry| entry.codepoint)
        .ok()
        .map(|index| &UnicodeDecompMain[index])
}

fn get_canonical_class(code: pg_wchar) -> uint8 {
    get_code_entry(code).map_or(0, |entry| entry.comb_class)
}

fn decomposition_codes(entry: pg_unicode_decomposition) -> DecompositionCodes<'static> {
    if entry.is_inline() {
        DecompositionCodes::Inline {
            code: entry.dec_index as pg_wchar,
            yielded: false,
        }
    } else {
        let start = entry.dec_index as usize;
        let end = start + entry.decomposition_size();
        DecompositionCodes::Slice(&UnicodeDecomp_codepoints[start..end])
    }
}

enum DecompositionCodes<'a> {
    Inline { code: pg_wchar, yielded: bool },
    Slice(&'a [pg_wchar]),
}

impl Iterator for DecompositionCodes<'_> {
    type Item = pg_wchar;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DecompositionCodes::Inline { code, yielded } => {
                if *yielded {
                    None
                } else {
                    *yielded = true;
                    Some(*code)
                }
            }
            DecompositionCodes::Slice(codes) => {
                let (first, rest) = codes.split_first()?;
                *codes = rest;
                Some(*first)
            }
        }
    }
}

fn get_decomposed_size(code: pg_wchar, compat: bool) -> usize {
    if (SBASE..SBASE + SCOUNT).contains(&code) {
        let sindex = code - SBASE;
        return if sindex % TCOUNT != 0 { 3 } else { 2 };
    }

    let Some(entry) = get_code_entry(code).copied() else {
        return 1;
    };

    if entry.decomposition_size() == 0 || (!compat && entry.is_compat()) {
        return 1;
    }

    decomposition_codes(entry)
        .map(|code| get_decomposed_size(code, compat))
        .sum()
}

fn decompose_code<'mcx>(code: pg_wchar, compat: bool, result: &mut PgVec<'mcx, pg_wchar>) {
    if (SBASE..SBASE + SCOUNT).contains(&code) {
        let sindex = code - SBASE;
        let l = LBASE + sindex / NCOUNT;
        let v = VBASE + (sindex % NCOUNT) / TCOUNT;
        let tindex = sindex % TCOUNT;

        result.push(l);
        result.push(v);
        if tindex != 0 {
            result.push(TBASE + tindex);
        }
        return;
    }

    let Some(entry) = get_code_entry(code).copied() else {
        result.push(code);
        return;
    };

    if entry.decomposition_size() == 0 || (!compat && entry.is_compat()) {
        result.push(code);
        return;
    }

    for code in decomposition_codes(entry) {
        decompose_code(code, compat, result);
    }
}

fn recompose_code(start: pg_wchar, code: pg_wchar) -> Option<pg_wchar> {
    if (LBASE..LBASE + LCOUNT).contains(&start) && (VBASE..VBASE + VCOUNT).contains(&code) {
        let lindex = start - LBASE;
        let vindex = code - VBASE;
        return Some(SBASE + (lindex * VCOUNT + vindex) * TCOUNT);
    }

    if (SBASE..SBASE + SCOUNT).contains(&start)
        && (start - SBASE) % TCOUNT == 0
        // TBASE itself is not a T syllable; the T range starts at U+11A8.
        && (TBASE + 1..TBASE + TCOUNT).contains(&code)
    {
        return Some(start + code - TBASE);
    }

    // Only decomposition_size() == 2 entries can match: the start character has
    // already been recomposed partially, so a perfect match on the size-two
    // sub-table suffices (per the C comment).
    UnicodeDecompMain
        .iter()
        .filter(|entry| entry.decomposition_size() == 2)
        .filter(|entry| !entry.no_compose() && !entry.is_compat())
        .find_map(|entry| {
            let index = entry.dec_index as usize;
            (UnicodeDecomp_codepoints[index] == start
                && UnicodeDecomp_codepoints[index + 1] == code)
                .then_some(entry.codepoint)
        })
}

fn canonical_order(chars: &mut [pg_wchar]) {
    let mut count = 1usize;
    while count < chars.len() {
        let prev_class = get_canonical_class(chars[count - 1]);
        let next_class = get_canonical_class(chars[count]);

        if prev_class != 0 && next_class != 0 && prev_class > next_class {
            chars.swap(count - 1, count);
            count = count.saturating_sub(2);
        }

        count += 1;
    }
}

fn recompose<'mcx>(mcx: Mcx<'mcx>, chars: &[pg_wchar]) -> PgResult<PgVec<'mcx, pg_wchar>> {
    if chars.is_empty() {
        return Ok(PgVec::new_in(mcx));
    }

    // The recomposed string is never longer than the decomposed one, so
    // chars.len() is an exact upper bound.
    let mut result = vec_with_capacity_in(mcx, chars.len())?;
    result.push(chars[0]);

    let mut last_class = -1i32;
    let mut starter_pos = 0usize;
    let mut starter_ch = chars[0];

    for &ch in &chars[1..] {
        let ch_class = get_canonical_class(ch) as i32;

        if last_class < ch_class {
            if let Some(composite) = recompose_code(starter_ch, ch) {
                result[starter_pos] = composite;
                starter_ch = composite;
                continue;
            }
        }

        if ch_class == 0 {
            starter_pos = result.len();
            starter_ch = ch;
            last_class = -1;
        } else {
            last_class = ch_class;
        }

        result.push(ch);
    }

    Ok(result)
}

fn normalize_into<'mcx>(
    mcx: Mcx<'mcx>,
    form: UnicodeNormalizationForm,
    input: &[pg_wchar],
) -> PgResult<PgVec<'mcx, pg_wchar>> {
    let compat = is_compat_form(form);

    // decomp_size is the exact decomposed length; the pushes below cannot
    // reallocate.
    let decomp_size = input
        .iter()
        .copied()
        .map(|code| get_decomposed_size(code, compat))
        .sum();
    let mut decomp_chars = vec_with_capacity_in(mcx, decomp_size)?;

    for &code in input {
        decompose_code(code, compat, &mut decomp_chars);
    }

    canonical_order(decomp_chars.as_mut_slice());

    if is_recompose_form(form) {
        recompose(mcx, &decomp_chars)
    } else {
        Ok(decomp_chars)
    }
}

pub fn unicode_normalize<'mcx>(
    mcx: Mcx<'mcx>,
    form: UnicodeNormalizationForm,
    input: &[pg_wchar],
) -> PgResult<PgVec<'mcx, pg_wchar>> {
    normalize_into(mcx, form, input)
}

pub fn unicode_normalize_z<'mcx>(
    mcx: Mcx<'mcx>,
    form: UnicodeNormalizationForm,
    input: &[pg_wchar],
) -> PgResult<PgVec<'mcx, pg_wchar>> {
    let end = input
        .iter()
        .position(|&code| code == 0)
        .unwrap_or(input.len());

    let mut buf = normalize_into(mcx, form, &input[..end])?;
    buf.push(0);
    Ok(buf)
}

fn qc_hash_lookup(
    ch: pg_wchar,
    norminfo: &pg_unicode_norminfo,
) -> Option<&'static pg_unicode_normprops> {
    let h = norminfo.hash(ch);

    if h < 0 || h >= norminfo.normprops.len() as i32 {
        return None;
    }

    // Perfect hash: only the specific codepoint it identifies can match.
    let entry = &norminfo.normprops[h as usize];
    if ch != entry.codepoint {
        return None;
    }

    Some(entry)
}

fn qc_is_allowed(form: UnicodeNormalizationForm, ch: pg_wchar) -> UnicodeNormalizationQC {
    let found = match form {
        UNICODE_NFC => qc_hash_lookup(ch, &UnicodeNormInfo_NFC_QC),
        UNICODE_NFKC => qc_hash_lookup(ch, &UnicodeNormInfo_NFKC_QC),
        _ => {
            debug_assert!(false, "qc_is_allowed called with a non-recomposing form");
            None
        }
    };

    match found {
        Some(entry) => entry.quickcheck,
        None => UNICODE_NORM_QC_YES,
    }
}

pub fn unicode_is_normalized_quickcheck(
    form: UnicodeNormalizationForm,
    input: &[pg_wchar],
) -> UnicodeNormalizationQC {
    let mut last_canonical_class: uint8 = 0;
    let mut result = UNICODE_NORM_QC_YES;

    // C omits the (huge) D-form quick-check tables because the slow path is
    // faster for those forms, so they always report MAYBE.
    if form == UNICODE_NFD || form == UNICODE_NFKD {
        return UNICODE_NORM_QC_MAYBE;
    }

    for &ch in input {
        if ch == 0 {
            break;
        }

        let canonical_class = get_canonical_class(ch);
        if last_canonical_class > canonical_class && canonical_class != 0 {
            return UNICODE_NORM_QC_NO;
        }

        let check = qc_is_allowed(form, ch);
        if check == UNICODE_NORM_QC_NO {
            return UNICODE_NORM_QC_NO;
        } else if check == UNICODE_NORM_QC_MAYBE {
            result = UNICODE_NORM_QC_MAYBE;
        }

        last_canonical_class = canonical_class;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    fn chars(s: &str) -> Vec<pg_wchar> {
        s.chars().map(|ch| ch as pg_wchar).collect()
    }

    fn normalize(form: UnicodeNormalizationForm, input: &[pg_wchar]) -> Vec<pg_wchar> {
        let cx = MemoryContext::new("test-unicode-norm");
        let out = unicode_normalize(cx.mcx(), form, input)
            .expect("normalization should not run out of memory");
        out.iter().copied().collect()
    }

    #[test]
    fn ascii_is_stable_in_all_forms() {
        let input = chars("PostgreSQL");
        for form in [UNICODE_NFC, UNICODE_NFD, UNICODE_NFKC, UNICODE_NFKD] {
            assert_eq!(normalize(form, &input), input);
        }
    }

    #[test]
    fn canonical_decomposition_and_composition_match_postgres_examples() {
        let composed = chars("\u{00e9}");
        let decomposed = vec!['e' as pg_wchar, 0x0301];

        assert_eq!(normalize(UNICODE_NFD, &composed), decomposed);
        assert_eq!(normalize(UNICODE_NFC, &decomposed), composed);
    }

    #[test]
    fn compatibility_forms_expand_ligatures() {
        let input = chars("\u{fb01}");
        let expected = chars("fi");

        assert_eq!(normalize(UNICODE_NFKD, &input), expected);
        assert_eq!(normalize(UNICODE_NFKC, &input), expected);
    }

    #[test]
    fn hangul_decomposes_and_recomposes_algorithmically() {
        let syllable = chars("\u{ac01}");
        let decomposed = vec![0x1100, 0x1161, 0x11a8];

        assert_eq!(normalize(UNICODE_NFD, &syllable), decomposed);
        assert_eq!(normalize(UNICODE_NFC, &decomposed), syllable);
    }

    #[test]
    fn hangul_tbase_is_not_a_t_syllable() {
        // U+11A7 (TBASE) must survive NFC unrecomposed (C 273fe94).
        let input = vec![0x1100, 0x1161, 0x11a7];
        let expected = vec![0xac00, 0x11a7];

        assert_eq!(normalize(UNICODE_NFC, &input), expected);
    }

    #[test]
    fn canonical_ordering_sorts_non_starters_by_combining_class() {
        let input = vec!['a' as pg_wchar, 0x0315, 0x0300];
        let expected = vec!['a' as pg_wchar, 0x0300, 0x0315];

        assert_eq!(normalize(UNICODE_NFD, &input), expected);
    }

    #[test]
    fn zero_terminated_api_preserves_terminator() {
        let cx = MemoryContext::new("test-unicode-norm-z");
        let input = [0x00e9, 0];
        let output = unicode_normalize_z(cx.mcx(), UNICODE_NFD, &input)
            .expect("normalization should not OOM");
        let output: Vec<pg_wchar> = output.iter().copied().collect();
        assert_eq!(output, [b'e' as pg_wchar, 0x0301, 0]);
    }

    #[test]
    fn quickcheck_d_forms_always_maybe() {
        let mut input = chars("abc");
        input.push(0);
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFD, &input),
            UNICODE_NORM_QC_MAYBE
        );
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFKD, &input),
            UNICODE_NORM_QC_MAYBE
        );
    }

    #[test]
    fn quickcheck_ascii_is_yes_for_c_forms() {
        let mut input = chars("PostgreSQL");
        input.push(0);
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFC, &input),
            UNICODE_NORM_QC_YES
        );
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFKC, &input),
            UNICODE_NORM_QC_YES
        );
    }

    #[test]
    fn quickcheck_combining_mark_is_maybe() {
        let input = vec!['e' as pg_wchar, 0x0301, 0];
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFC, &input),
            UNICODE_NORM_QC_MAYBE
        );
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFKC, &input),
            UNICODE_NORM_QC_MAYBE
        );
    }

    #[test]
    fn quickcheck_qc_no_codepoint_is_no() {
        let input = vec![0x0340, 0];
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFC, &input),
            UNICODE_NORM_QC_NO
        );
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFKC, &input),
            UNICODE_NORM_QC_NO
        );
    }

    #[test]
    fn quickcheck_nfkc_only_codepoint_is_no() {
        let input = vec![0x00a0, 0];
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFC, &input),
            UNICODE_NORM_QC_YES
        );
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFKC, &input),
            UNICODE_NORM_QC_NO
        );
    }

    #[test]
    fn quickcheck_misordered_combining_marks_is_no() {
        let input = vec!['a' as pg_wchar, 0x0315, 0x0300, 0];
        assert_eq!(
            unicode_is_normalized_quickcheck(UNICODE_NFC, &input),
            UNICODE_NORM_QC_NO
        );
    }

    #[test]
    fn qc_hash_lookup_finds_and_rejects() {
        let entry = qc_hash_lookup(0x0301, &UnicodeNormInfo_NFC_QC).expect("0x0301 is in NFC_QC");
        assert_eq!(entry.codepoint, 0x0301);
        assert_eq!(entry.quickcheck, UNICODE_NORM_QC_MAYBE);

        assert!(qc_hash_lookup('a' as pg_wchar, &UnicodeNormInfo_NFC_QC).is_none());
    }

    #[test]
    fn qc_hash_is_a_perfect_hash_for_every_entry() {
        for info in [&UnicodeNormInfo_NFC_QC, &UnicodeNormInfo_NFKC_QC] {
            for (index, entry) in info.normprops.iter().enumerate() {
                assert_eq!(
                    info.hash(entry.codepoint),
                    index as i32,
                    "codepoint {:#06x} did not hash to its index",
                    entry.codepoint
                );
            }
        }
    }
}
