#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod generated {
    include!(concat!(env!("OUT_DIR"), "/keywords.rs"));
}

pub const SCANKEYWORDS_NUM_KEYWORDS: usize = generated::SCANKEYWORDS_NUM_KEYWORDS;
pub const SCANKEYWORDS_MAX_KW_LEN: usize = generated::SCANKEYWORDS_MAX_KW_LEN;

// src/include/common/keywords.h values; transmuted from u8 in generated data.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum KeywordCategory {
    Unreserved = 0,
    ColName = 1,
    TypeFuncName = 2,
    Reserved = 3,
}

pub struct ScanKeywordList {
    pub kw_string: &'static [u8],
    pub kw_offsets: &'static [u16],
    pub hash: fn(&[u8]) -> i32,
    pub num_keywords: i32,
    pub max_kw_len: i32,
}

pub static ScanKeywords: ScanKeywordList = ScanKeywordList {
    kw_string: &generated::KW_STRING,
    kw_offsets: &generated::KW_OFFSETS,
    hash: ScanKeywords_hash_func,
    num_keywords: generated::SCANKEYWORDS_NUM_KEYWORDS as i32,
    max_kw_len: generated::SCANKEYWORDS_MAX_KW_LEN as i32,
};

pub static ScanKeywordCategories: &[KeywordCategory; SCANKEYWORDS_NUM_KEYWORDS] =
    &generated::KEYWORD_CATEGORIES;
pub static ScanKeywordBareLabel: &[bool; SCANKEYWORDS_NUM_KEYWORDS] =
    &generated::KEYWORD_BARE_LABEL;

pub fn GetScanKeyword(n: usize, keywords: &ScanKeywordList) -> Option<&'static [u8]> {
    if n >= keywords.num_keywords as usize {
        return None;
    }
    let off = keywords.kw_offsets[n] as usize;
    let tail = &keywords.kw_string[off..];
    let len = tail.iter().position(|&b| b == 0).unwrap_or(tail.len());
    Some(&tail[..len])
}

pub fn keyword_text(n: usize) -> Option<&'static str> {
    generated::KEYWORD_TEXT.get(n).copied()
}

pub fn ScanKeywordLookup(word: &[u8], keywords: &ScanKeywordList) -> i32 {
    if word.len() > keywords.max_kw_len as usize {
        return -1;
    }
    let h = (keywords.hash)(word);
    if h < 0 || h >= keywords.num_keywords {
        return -1;
    }
    let off = keywords.kw_offsets[h as usize] as usize;
    // KW_STRING is NUL-padded by max_kw_len+1, so kw[i] for i <= word.len()
    // stays in bounds for every offset.
    let kw = &keywords.kw_string[off..];
    let mut i = 0;
    while i < word.len() {
        let mut ch = word[i];
        if ch.is_ascii_uppercase() {
            ch += b'a' - b'A';
        }
        if ch != kw[i] {
            return -1;
        }
        i += 1;
    }
    if kw[i] != 0 {
        return -1;
    }
    h
}

fn ScanKeywords_hash_func(key: &[u8]) -> i32 {
    let mut a: u32 = 0;
    let mut b: u32 = 0;
    for &k in key {
        let c = (k | 0x20) as u32;
        a = a.wrapping_mul(generated::KW_HASH_MULT_A).wrapping_add(c);
        b = b.wrapping_mul(generated::KW_HASH_MULT_B).wrapping_add(c);
    }
    generated::KW_HASH_H[(a % generated::KW_HASH_MOD) as usize] as i32
        + generated::KW_HASH_H[(b % generated::KW_HASH_MOD) as usize] as i32
}

#[cfg(test)]
mod tests;
