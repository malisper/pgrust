//! Idiomatic port of PostgreSQL's `ScanKeywords` table (`src/common/keywords.c`)
//! and the keyword lookup (`src/common/kwlookup.c`).
//!
//! The keyword data itself is generated at build time by `build.rs`, which
//! reads `kwlist.h` and emits `keywords.rs` into `OUT_DIR`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

// `KeywordCategory` is the ported, idiomatic four-variant category enum from
// the `types-core` crate (`types_core::keywords`). It mirrors PostgreSQL's
// keyword category values (UNRESERVED_KEYWORD / COL_NAME_KEYWORD /
// TYPE_FUNC_NAME_KEYWORD / RESERVED_KEYWORD).
pub use types_core::keywords::KeywordCategory;

mod generated {
    // The generated table refers to the category enum via the original FFI
    // crate path `pgrust_pg_ffi::KeywordCategory`. In this workspace that
    // vocabulary lives in `types_core::keywords`, so alias it here to keep the
    // verbatim `build.rs` / generated code unchanged.
    use types_core::keywords as pgrust_pg_ffi;

    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

/// Number of keywords in the [`ScanKeywords`] table.
pub const SCANKEYWORDS_NUM_KEYWORDS: usize = generated::SCANKEYWORDS_NUM_KEYWORDS;
/// Length, in bytes, of the longest keyword in [`ScanKeywords`].
pub const SCANKEYWORDS_MAX_KW_LEN: usize = generated::SCANKEYWORDS_MAX_KW_LEN;

/// Idiomatic equivalent of PostgreSQL's `ScanKeywordList`
/// (`src/include/common/kwlookup.h`).
///
/// The faithful C layout stores raw pointers into a packed NUL-separated
/// keyword blob plus a perfect-hash function pointer. This idiomatic port
/// instead borrows the generated static data as safe slices, so a value can be
/// constructed in a plain `static`/`const` context with no `unsafe`:
///
/// * `kw_string` — the packed keyword bytes, each keyword NUL-terminated.
/// * `kw_offsets` — for each keyword, its starting byte offset into `kw_string`.
/// * `num_keywords` / `max_kw_len` — table size and longest keyword length.
///
/// The C version's `hash` function pointer is not stored as a field; instead
/// the equivalent minimal perfect hash is generated at build time (see
/// `build.rs`, a faithful port of PostgreSQL's `src/tools/PerfectHash.pm`) and
/// applied in [`scan_keywords_hash_bytes`]. The lookup is O(1) in the input
/// length, exactly as in `kwlookup.c`.
#[derive(Debug, Clone, Copy)]
pub struct ScanKeywordList {
    kw_string: &'static [u8],
    kw_offsets: &'static [u16],
    num_keywords: usize,
    max_kw_len: usize,
}

impl ScanKeywordList {
    /// Builds a keyword list over static keyword storage.
    ///
    /// `kw_string` holds `num_keywords` NUL-terminated keywords; `kw_offsets`
    /// holds the starting byte offset of each keyword within `kw_string`.
    pub const fn from_static_parts(
        kw_string: &'static [u8],
        kw_offsets: &'static [u16],
        num_keywords: usize,
        max_kw_len: usize,
    ) -> Self {
        Self {
            kw_string,
            kw_offsets,
            num_keywords,
            max_kw_len,
        }
    }

    /// Number of keywords in the table.
    pub const fn num_keywords(&self) -> usize {
        self.num_keywords
    }

    /// Length, in bytes, of the longest keyword in the table.
    pub const fn max_kw_len(&self) -> usize {
        self.max_kw_len
    }

    /// Returns the keyword at `index`, or `None` if out of range.
    pub fn keyword(&self, index: usize) -> Option<&str> {
        if index >= self.num_keywords {
            return None;
        }
        let start = *self.kw_offsets.get(index)? as usize;
        let rest = self.kw_string.get(start..)?;
        let len = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
        core::str::from_utf8(&rest[..len]).ok()
    }
}

/// The standard SQL scanner keyword table, generated from `kwlist.h`.
pub static ScanKeywords: ScanKeywordList = ScanKeywordList::from_static_parts(
    &generated::SCAN_KEYWORDS_KW_STRING,
    &generated::SCAN_KEYWORDS_KW_OFFSETS,
    generated::SCANKEYWORDS_NUM_KEYWORDS,
    generated::SCANKEYWORDS_MAX_KW_LEN,
);

/// Per-keyword grammatical category, indexed parallel to [`ScanKeywords`].
pub static ScanKeywordCategories: &[KeywordCategory; SCANKEYWORDS_NUM_KEYWORDS] =
    &generated::SCAN_KEYWORD_CATEGORIES;
/// Per-keyword "bare label" flag, indexed parallel to [`ScanKeywords`].
pub static ScanKeywordBareLabel: &[bool; SCANKEYWORDS_NUM_KEYWORDS] =
    &generated::SCAN_KEYWORD_BARE_LABEL;

/// Returns the `n`-th keyword in `keywords`, or `None` if out of range.
pub fn GetScanKeyword(n: usize, keywords: &ScanKeywordList) -> Option<&str> {
    keywords.keyword(n)
}

/// Looks up `str_` in `keywords`, returning its index or `-1` if not found.
///
/// Matching is ASCII case-insensitive, the input is truncated at the first NUL
/// byte (C-string semantics), and words longer than `max_kw_len` are rejected
/// without scanning.
pub fn ScanKeywordLookup(str_: &str, keywords: &ScanKeywordList) -> i32 {
    let bytes = c_string_prefix(str_.as_bytes());

    if bytes.len() > keywords.max_kw_len() {
        return -1;
    }

    let index = scan_keywords_hash_bytes(bytes);

    if index < 0 || index as usize >= keywords.num_keywords() {
        return -1;
    }

    let Some(keyword) = keywords.keyword(index as usize) else {
        return -1;
    };
    if ascii_keyword_eq(bytes, keyword.as_bytes()) {
        index
    } else {
        -1
    }
}

/// Returns the category of the keyword at `index`, or `None` if out of range.
pub fn keyword_category(index: usize) -> Option<KeywordCategory> {
    ScanKeywordCategories.get(index).copied()
}

/// Returns the bare-label flag of the keyword at `index`, or `None` if out of
/// range.
pub fn keyword_bare_label(index: usize) -> Option<bool> {
    ScanKeywordBareLabel.get(index).copied()
}

/// Returns the text of the keyword at `index`, or `None` if out of range.
pub fn keyword_text(index: usize) -> Option<&'static str> {
    generated::SCAN_KEYWORD_TEXT.get(index).copied()
}

fn c_string_prefix(bytes: &[u8]) -> &[u8] {
    match bytes.iter().position(|&byte| byte == 0) {
        Some(nul) => &bytes[..nul],
        None => bytes,
    }
}

/// Computes the minimal perfect hash of `bytes`, mirroring the function
/// generated by PostgreSQL's `PerfectHash.pm` (with `case_fold => 1`).
///
/// Two multiply-and-add hashes are folded over the case-folded key bytes and
/// used to index the build-time mapping table; their sum is the keyword index.
/// As in the C, the result may be out of range (negative or `>= nhash`) for a
/// non-keyword input, which the caller treats as "no match". This is O(1) in
/// the input length, replacing the former O(n) text scan.
fn scan_keywords_hash_bytes(bytes: &[u8]) -> i32 {
    let nhash = generated::SCAN_KEYWORD_HASH_NHASH;
    let mut a: u32 = generated::SCAN_KEYWORD_HASH_SEED1;
    let mut b: u32 = generated::SCAN_KEYWORD_HASH_SEED2;
    for &byte in bytes {
        let c = (byte | 0x20) as u32;
        a = a
            .wrapping_mul(generated::SCAN_KEYWORD_HASH_MULT1)
            .wrapping_add(c);
        b = b
            .wrapping_mul(generated::SCAN_KEYWORD_HASH_MULT2)
            .wrapping_add(c);
    }
    let ha = generated::SCAN_KEYWORD_HASH_TABLE[(a % nhash) as usize];
    let hb = generated::SCAN_KEYWORD_HASH_TABLE[(b % nhash) as usize];
    ha + hb
}

fn ascii_keyword_eq(input: &[u8], keyword: &[u8]) -> bool {
    input.len() == keyword.len()
        && input
            .iter()
            .zip(keyword)
            .all(|(&input, &keyword)| ascii_downcase(input) == keyword)
}

fn ascii_downcase(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + (b'a' - b'A')
    } else {
        byte
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_table_matches_postgres_keyword_count() {
        assert_eq!(SCANKEYWORDS_NUM_KEYWORDS, 494);
        assert_eq!(SCANKEYWORDS_MAX_KW_LEN, 17);
        assert_eq!(ScanKeywords.num_keywords(), 494);
        assert_eq!(ScanKeywords.max_kw_len(), 17);
    }

    #[test]
    fn get_scan_keyword_returns_ordered_keywords() {
        assert_eq!(GetScanKeyword(0, &ScanKeywords), Some("abort"));
        assert_eq!(GetScanKeyword(15, &ScanKeywords), Some("and"));
        assert_eq!(GetScanKeyword(493, &ScanKeywords), Some("zone"));
        assert_eq!(GetScanKeyword(494, &ScanKeywords), None);
    }

    #[test]
    fn lookup_is_ascii_case_insensitive() {
        let select = ScanKeywordLookup("select", &ScanKeywords);
        assert!(select >= 0);
        assert_eq!(ScanKeywordLookup("SELECT", &ScanKeywords), select);
        assert_eq!(ScanKeywordLookup("SeLeCt", &ScanKeywords), select);
    }

    #[test]
    fn lookup_rejects_unknown_and_overlong_words() {
        assert_eq!(
            ScanKeywordLookup("definitely_not_a_keyword", &ScanKeywords),
            -1
        );
        assert_eq!(
            ScanKeywordLookup("abcdefghijklmnopqrstuvwxyz", &ScanKeywords),
            -1
        );
    }

    #[test]
    fn lookup_uses_c_string_prefix() {
        assert_eq!(
            ScanKeywordLookup("select\0suffix", &ScanKeywords),
            ScanKeywordLookup("select", &ScanKeywords)
        );
    }

    #[test]
    fn categories_and_bare_label_flags_match_samples() {
        let all = ScanKeywordLookup("all", &ScanKeywords) as usize;
        let bigint = ScanKeywordLookup("bigint", &ScanKeywords) as usize;
        let between = ScanKeywordLookup("between", &ScanKeywords) as usize;

        assert_eq!(keyword_category(all), Some(KeywordCategory::Reserved));
        assert_eq!(keyword_category(bigint), Some(KeywordCategory::ColumnName));
        assert_eq!(keyword_category(between), Some(KeywordCategory::ColumnName));
        assert_eq!(keyword_bare_label(all), Some(true));
        assert_eq!(keyword_bare_label(between), Some(true));
    }
}
