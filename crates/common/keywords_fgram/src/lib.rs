#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::ffi::{c_int, c_void};

pub use pg_ffi_fgram::{KeywordCategory, ScanKeywordHashFunc, ScanKeywordList};

mod generated {
    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

pub const SCANKEYWORDS_NUM_KEYWORDS: usize = generated::SCANKEYWORDS_NUM_KEYWORDS;
pub const SCANKEYWORDS_MAX_KW_LEN: usize = generated::SCANKEYWORDS_MAX_KW_LEN;

pub static ScanKeywords: ScanKeywordList = unsafe {
    ScanKeywordList::from_static_parts(
        generated::SCAN_KEYWORDS_KW_STRING.as_ptr().cast(),
        generated::SCAN_KEYWORDS_KW_OFFSETS.as_ptr(),
        Some(ScanKeywords_hash_func),
        generated::SCANKEYWORDS_NUM_KEYWORDS as c_int,
        generated::SCANKEYWORDS_MAX_KW_LEN as c_int,
    )
};

pub static ScanKeywordCategories: &[KeywordCategory; SCANKEYWORDS_NUM_KEYWORDS] =
    &generated::SCAN_KEYWORD_CATEGORIES;
pub static ScanKeywordBareLabel: &[bool; SCANKEYWORDS_NUM_KEYWORDS] =
    &generated::SCAN_KEYWORD_BARE_LABEL;

pub fn GetScanKeyword(n: usize, keywords: &ScanKeywordList) -> Option<&str> {
    keywords.keyword(n)
}

pub fn ScanKeywordLookup(str_: &str, keywords: &ScanKeywordList) -> c_int {
    let bytes = c_string_prefix(str_.as_bytes());

    if bytes.len() > keywords.max_kw_len() {
        return -1;
    }

    let Some(hash) = keywords.hash() else {
        return -1;
    };
    let index = unsafe { hash(bytes.as_ptr().cast(), bytes.len()) };

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

pub fn keyword_category(index: usize) -> Option<KeywordCategory> {
    ScanKeywordCategories.get(index).copied()
}

pub fn keyword_bare_label(index: usize) -> Option<bool> {
    ScanKeywordBareLabel.get(index).copied()
}

pub fn keyword_text(index: usize) -> Option<&'static str> {
    generated::SCAN_KEYWORD_TEXT.get(index).copied()
}

fn c_string_prefix(bytes: &[u8]) -> &[u8] {
    match bytes.iter().position(|&byte| byte == 0) {
        Some(nul) => &bytes[..nul],
        None => bytes,
    }
}

unsafe extern "C" fn ScanKeywords_hash_func(key: *const c_void, keylen: usize) -> c_int {
    if key.is_null() {
        return -1;
    }

    let bytes = unsafe { std::slice::from_raw_parts(key.cast::<u8>(), keylen) };
    scan_keywords_hash_bytes(bytes)
}

fn scan_keywords_hash_bytes(bytes: &[u8]) -> c_int {
    generated::SCAN_KEYWORD_TEXT
        .iter()
        .position(|keyword| ascii_keyword_eq(bytes, keyword.as_bytes()))
        .map(|index| index as c_int)
        .unwrap_or(-1)
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
    use std::mem::{align_of, size_of};

    #[test]
    fn scan_keyword_list_has_c_layout() {
        assert_eq!(size_of::<ScanKeywordList>(), 32);
        assert_eq!(align_of::<ScanKeywordList>(), align_of::<usize>());
    }

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
