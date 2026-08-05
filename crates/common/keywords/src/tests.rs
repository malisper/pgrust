use super::*;

#[test]
fn table_shape_matches_kwlist_d() {
    assert_eq!(SCANKEYWORDS_NUM_KEYWORDS, 494);
    assert_eq!(SCANKEYWORDS_MAX_KW_LEN, 17);
    assert_eq!(GetScanKeyword(0, &ScanKeywords), Some(&b"abort"[..]));
    assert_eq!(GetScanKeyword(493, &ScanKeywords), Some(&b"zone"[..]));
    assert_eq!(GetScanKeyword(494, &ScanKeywords), None);
}

#[test]
fn every_keyword_resolves_to_its_own_index() {
    for n in 0..SCANKEYWORDS_NUM_KEYWORDS {
        let kw = keyword_text(n).unwrap();
        assert_eq!(kw.as_bytes(), GetScanKeyword(n, &ScanKeywords).unwrap());
        assert_eq!(ScanKeywordLookup(kw.as_bytes(), &ScanKeywords), n as i32, "{kw}");
        let upper: Upper = upcase(kw.as_bytes());
        assert_eq!(ScanKeywordLookup(&upper.buf[..upper.len], &ScanKeywords), n as i32, "{kw} upper");
    }
}

struct Upper {
    buf: [u8; 32],
    len: usize,
}

fn upcase(w: &[u8]) -> Upper {
    let mut buf = [0u8; 32];
    for (i, &b) in w.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }
    Upper { buf, len: w.len() }
}

#[test]
fn near_misses_and_overlong_reject() {
    for w in [
        &b"selec"[..],
        b"selects",
        b"zzzz",
        b"select_",
        b"_select",
        b"abor",
        b"aborts",
        b"zonf",
        b"",
        b"authorizatioo",
    ] {
        assert_eq!(ScanKeywordLookup(w, &ScanKeywords), -1, "{w:?}");
    }
    assert_eq!(ScanKeywordLookup(b"abcdefghijklmnopqr", &ScanKeywords), -1);
    assert_eq!(
        ScanKeywordLookup(b"current_timestampx", &ScanKeywords),
        -1
    );
}

#[test]
fn case_folding_is_ascii_only() {
    let n = ScanKeywordLookup(b"select", &ScanKeywords);
    assert!(n >= 0);
    assert_eq!(ScanKeywordLookup(b"SELECT", &ScanKeywords), n);
    assert_eq!(ScanKeywordLookup(b"SeLeCt", &ScanKeywords), n);
    // High-bit bytes must not fold.
    assert_eq!(ScanKeywordLookup(b"s\xc9lect", &ScanKeywords), -1);
}

#[test]
fn categories_and_bare_label_match_kwlist_h() {
    let all = ScanKeywordLookup(b"all", &ScanKeywords) as usize;
    let bigint = ScanKeywordLookup(b"bigint", &ScanKeywords) as usize;
    let authorization = ScanKeywordLookup(b"authorization", &ScanKeywords) as usize;
    let abort = ScanKeywordLookup(b"abort", &ScanKeywords) as usize;
    let between = ScanKeywordLookup(b"between", &ScanKeywords) as usize;
    assert_eq!(ScanKeywordCategories[all], KeywordCategory::Reserved);
    assert_eq!(ScanKeywordCategories[bigint], KeywordCategory::ColName);
    assert_eq!(ScanKeywordCategories[authorization], KeywordCategory::TypeFuncName);
    assert_eq!(ScanKeywordCategories[abort], KeywordCategory::Unreserved);
    assert_eq!(ScanKeywordCategories[between], KeywordCategory::ColName);
    assert!(ScanKeywordBareLabel[all]);
    assert!(ScanKeywordBareLabel[between]);
    let day = ScanKeywordLookup(b"day", &ScanKeywords) as usize;
    assert!(!ScanKeywordBareLabel[day]);
}
