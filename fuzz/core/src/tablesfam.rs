//! Target: tablesfam_diff — the p1-lanef tables batch (common/keywords,
//! common/unicode_category) shipped Rust vs vendored PostgreSQL 18.3 C
//! (csrc/tablesfam/, verbatim @ 62d6c7d3df) in-process. The oracle compiles
//! against THE SHIPPED CRATE'S OWN kwlist_d.h (build.rs adds
//! crates/common/keywords to the include path) — not a private copy — so a
//! transcription drift between the crate's generated tables and the C side is
//! a divergence rather than an invisible agreement.
//!
//! Comparison planes: keyword lookup index + keyword text/category/bare-label
//! tables (index probes), unicode general category + all 18 property/classify
//! predicates (packed bitmask, both posix modes). No error plane exists —
//! every function is total. Any mismatch panics: a libFuzzer crash artifact
//! is a divergence reproducer.
//!
//! Domain carves (documented, ratified non-surfaces):
//!   1. C `ScanKeywordLookup` takes a NUL-terminated string: lookup input is
//!      truncated at the first NUL before BOTH sides (the scanner only ever
//!      passes NUL-free identifier text).
//!   2. GetScanKeyword out-of-range index is NOT a parity plane. Verbatim C
//!      (kwlookup.h:38-42) indexes `kw_offsets[n]` with no range check, so
//!      out-of-range is C UB; the shim's guard is HARNESS PLUMBING WE ADDED and
//!      is never used as an oracle. The shipped Rust `None` arm is pgrust
//!      hardening with no C counterpart (PARITY-SCOPE NOTE at the head of
//!      proofs/coverage/lanef/residual-rows-lanef.tsv; same shape as lane-C's
//!      relpath forkNames row). Parity claims cover in-range indexes only.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uint};

extern "C" {
    // tablesfam_-prefixed at compile time (build.rs symbol isolation): the
    // main oracle lib (p1-laneg enc_tables) exports glue with the SAME
    // pg_diff_* names over a DIFFERENT vendored kwlookup copy.
    #[link_name = "tablesfam_pg_diff_scan_keyword_lookup"]
    fn pg_diff_scan_keyword_lookup(s: *const c_char) -> c_int;
    #[link_name = "tablesfam_pg_diff_get_scan_keyword"]
    fn pg_diff_get_scan_keyword(n: c_int) -> *const c_char;
    fn pg_diff_keyword_category(n: c_int) -> c_int;
    fn pg_diff_keyword_bare_label(n: c_int) -> c_int;
    fn pg_diff_num_keywords() -> c_int;
    fn pg_diff_max_kw_len() -> c_int;
    fn pg_diff_unicode_category(code: c_uint) -> c_int;
    fn pg_diff_unicode_props(code: c_uint, posix: c_int) -> c_uint;
}

fn rust_unicode_props(code: u32, posix: bool) -> u32 {
    use unicode_category as u;
    let mut m = 0u32;
    m |= (u::pg_u_prop_alphabetic(code) as u32) << 0;
    m |= (u::pg_u_prop_lowercase(code) as u32) << 1;
    m |= (u::pg_u_prop_uppercase(code) as u32) << 2;
    m |= (u::pg_u_prop_cased(code) as u32) << 3;
    m |= (u::pg_u_prop_case_ignorable(code) as u32) << 4;
    m |= (u::pg_u_prop_white_space(code) as u32) << 5;
    m |= (u::pg_u_prop_hex_digit(code) as u32) << 6;
    m |= (u::pg_u_prop_join_control(code) as u32) << 7;
    m |= (u::pg_u_isdigit(code, posix) as u32) << 8;
    m |= (u::pg_u_isalpha(code) as u32) << 9;
    m |= (u::pg_u_isalnum(code, posix) as u32) << 10;
    m |= (u::pg_u_isupper(code) as u32) << 11;
    m |= (u::pg_u_islower(code) as u32) << 12;
    m |= (u::pg_u_isblank(code) as u32) << 13;
    m |= (u::pg_u_isgraph(code) as u32) << 14;
    m |= (u::pg_u_isprint(code) as u32) << 15;
    m |= (u::pg_u_ispunct(code, posix) as u32) << 16;
    m |= (u::pg_u_isspace(code) as u32) << 17;
    m
}

fn diff_keyword_lookup(payload: &[u8]) {
    // NUL-truncate (C-string domain, see header).
    let end = payload.iter().position(|&b| b == 0).unwrap_or(payload.len());
    let word = &payload[..end];
    let mut c_word = word.to_vec();
    c_word.push(0);

    let r = keywords::ScanKeywordLookup(word, &keywords::ScanKeywords);
    // SAFETY: NUL-terminated c_word.
    let c = unsafe { pg_diff_scan_keyword_lookup(c_word.as_ptr().cast()) };
    assert_eq!(r, c, "ScanKeywordLookup diverges for {:?}", word);

    // On a hit, probe every table surface at that index.
    if r >= 0 {
        diff_keyword_index(r as usize);
    }
}

fn diff_keyword_index(raw: usize) {
    // OUT-OF-RANGE ARM — NOT A PARITY CLAIM (see header carve 2). Verbatim C
    // `GetScanKeyword` (src/include/common/kwlookup.h:38-42) is guard-free:
    // `keywords->kw_string + keywords->kw_offsets[n]`, so an out-of-range n is
    // C UB, not a comparable behavior. The shipped Rust returns None there
    // (pgrust hardening with no C counterpart). We assert only the RUST side —
    // a self-consistency claim audited by every exec — and never compare it to
    // the oracle, whose range guard is harness plumbing we added.
    let oob = raw.max(keywords::SCANKEYWORDS_NUM_KEYWORDS);
    assert!(
        keywords::GetScanKeyword(oob, &keywords::ScanKeywords).is_none(),
        "GetScanKeyword({oob}) must be None (pgrust hardening arm)"
    );

    let n = raw % keywords::SCANKEYWORDS_NUM_KEYWORDS;
    let r_kw = keywords::GetScanKeyword(n, &keywords::ScanKeywords).expect("in range");
    // SAFETY: n < num_keywords; C returns a pointer into the static table.
    let c_kw = unsafe { CStr::from_ptr(pg_diff_get_scan_keyword(n as c_int)) }.to_bytes();
    assert_eq!(r_kw, c_kw, "GetScanKeyword({n}) diverges");
    assert_eq!(
        keywords::keyword_text(n).map(str::as_bytes),
        Some(c_kw),
        "keyword_text({n}) diverges"
    );

    let r_cat = keywords::ScanKeywordCategories[n] as u8 as c_int;
    // SAFETY: n in range.
    let c_cat = unsafe { pg_diff_keyword_category(n as c_int) };
    assert_eq!(r_cat, c_cat, "category({n}) diverges");

    let r_bare = keywords::ScanKeywordBareLabel[n] as c_int;
    // SAFETY: n in range.
    let c_bare = unsafe { pg_diff_keyword_bare_label(n as c_int) };
    assert_eq!(r_bare, c_bare, "bare_label({n}) diverges");
}

fn diff_unicode(payload: &[u8]) {
    let mut b = [0u8; 4];
    for (i, &x) in payload.iter().take(4).enumerate() {
        b[i] = x;
    }
    let code = u32::from_le_bytes(b);
    let posix = payload.get(4).map_or(false, |&x| x & 1 != 0);

    let r_cat = unicode_category::unicode_category(code) as c_int;
    // SAFETY: total function.
    let c_cat = unsafe { pg_diff_unicode_category(code) };
    assert_eq!(r_cat, c_cat, "unicode_category(U+{code:X}) diverges");

    let r_props = rust_unicode_props(code, posix);
    // SAFETY: total function.
    let c_props = unsafe { pg_diff_unicode_props(code, posix as c_int) };
    assert_eq!(
        r_props, c_props,
        "unicode props diverge at U+{code:X} posix={posix} (xor {:#x})",
        r_props ^ c_props
    );
}

/// Entry point: data[0] selects the member, the rest is its payload.
pub fn tablesfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let (op, payload) = match data.split_first() {
        Some((op, p)) => (*op, p),
        None => return,
    };
    match op % 3 {
        0 => diff_keyword_lookup(payload),
        1 => diff_keyword_index(payload.first().map_or(0, |&b| b as usize) * 4 + 3),
        _ => diff_unicode(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Static-table parity: both sides must agree on the list geometry.
    #[test]
    fn table_geometry() {
        let _serial = crate::c_oracle_serial();
        assert_eq!(
            keywords::SCANKEYWORDS_NUM_KEYWORDS as i32,
            // SAFETY: constant read.
            unsafe { pg_diff_num_keywords() }
        );
        assert_eq!(keywords::SCANKEYWORDS_MAX_KW_LEN as i32, unsafe {
            pg_diff_max_kw_len()
        });
    }

    // Exhaustive: every keyword index — text, category, bare-label.
    #[test]
    fn all_keyword_indexes() {
        let _serial = crate::c_oracle_serial();
        for n in 0..keywords::SCANKEYWORDS_NUM_KEYWORDS {
            diff_keyword_index(n);
        }
    }

    // Exhaustive: every keyword in original, upper, and mixed case, plus
    // near-misses (one char changed / truncated / extended).
    #[test]
    fn all_keywords_lookup_and_near_misses() {
        let _serial = crate::c_oracle_serial();
        for n in 0..keywords::SCANKEYWORDS_NUM_KEYWORDS {
            let kw = keywords::GetScanKeyword(n, &keywords::ScanKeywords).unwrap();
            let mut probes: Vec<Vec<u8>> = vec![
                kw.to_vec(),
                kw.to_ascii_uppercase(),
                kw.iter()
                    .enumerate()
                    .map(|(i, &c)| if i % 2 == 0 { c.to_ascii_uppercase() } else { c })
                    .collect(),
            ];
            let mut miss = kw.to_vec();
            miss[0] ^= 0x04;
            probes.push(miss);
            probes.push(kw[..kw.len() - 1].to_vec());
            let mut ext = kw.to_vec();
            ext.push(b'x');
            probes.push(ext);
            for p in probes {
                diff_keyword_lookup(&p);
            }
        }
    }

    // Exhaustive full-domain sweep of the meaningful codespace: every
    // codepoint 0..=0x110000 (one past the Unicode max, exercising the
    // unassigned arm), both posix modes, category + all 18 predicates.
    #[test]
    fn unicode_full_codespace_sweep() {
        let _serial = crate::c_oracle_serial();
        for code in 0..=0x110000u32 {
            let mut v = vec![2u8];
            v.extend_from_slice(&code.to_le_bytes());
            v.push(0);
            tablesfam_diff(&v);
            v[5] = 1;
            tablesfam_diff(&v);
        }
        // A band beyond the codespace (u32 tail behavior must also match).
        for code in [0x110001u32, 0xdead_beef, u32::MAX - 1, u32::MAX] {
            for posix in [0u8, 1] {
                let mut v = vec![2u8];
                v.extend_from_slice(&code.to_le_bytes());
                v.push(posix);
                tablesfam_diff(&v);
            }
        }
    }

    #[test]
    fn ops_smoke() {
        let _serial = crate::c_oracle_serial();
        for op in 0u8..3 {
            tablesfam_diff(&[op]);
            let mut v = vec![op];
            v.extend_from_slice(b"select");
            tablesfam_diff(&v);
        }
    }

    // CI regression rail: replay the banked corpus.
    #[test]
    fn tablesfam_corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/tablesfam_diff");
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                if e.path().is_file() {
                    tablesfam_diff(&std::fs::read(e.path()).unwrap());
                    n += 1;
                }
            }
        }
        assert!(n >= 20, "corpus bank missing or truncated ({n} units)");
    }
}
