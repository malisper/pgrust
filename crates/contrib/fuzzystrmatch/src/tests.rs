//! Oracles: contrib/fuzzystrmatch expected/fuzzystrmatch.out and
//! expected/fuzzystrmatch_utf8.out (C 18.3).

use super::*;

fn sx(s: &str) -> String {
    let out = soundex(s.as_bytes());
    let n = soundex_text_len(&out);
    String::from_utf8(out[..n].to_vec()).unwrap()
}

#[test]
fn soundex_oracle() {
    assert_eq!(sx("hello world!"), "H464");
    assert_eq!(sx("Anne"), "A500");
    assert_eq!(sx("Ann"), "A500");
    assert_eq!(sx("Andrew"), "A536");
    assert_eq!(sx("Margaret"), "M626");
    assert_eq!(sx(""), "");
}

fn diff(a: &str, b: &str) -> i32 {
    let s1 = soundex(a.as_bytes());
    let s2 = soundex(b.as_bytes());
    (0..SOUNDEX_LEN).filter(|&i| s1[i] == s2[i]).count() as i32
}

#[test]
fn difference_oracle() {
    assert_eq!(diff("Anne", "Ann"), 4);
    assert_eq!(diff("Anne", "Andrew"), 2);
    assert_eq!(diff("Anne", "Margaret"), 0);
    assert_eq!(diff("", ""), 4);
}

#[test]
fn metaphone_oracle() {
    let mut out = Vec::new();
    metaphone(b"GUMBO", 4, &mut out);
    assert_eq!(out, b"KM");
}

#[test]
fn metaphone_shapes() {
    // Hand-traced through fuzzystrmatch.c _metaphone: TH->'0', GH->F after
    // non-BDH, initial-X->S, initial-WR->R, PH->F, SCH->K.
    for (word, len, want) in [
        ("Thompson", 10, "0MPSN"),
        ("knight", 10, "NFT"),
        ("Xavier", 10, "SFR"),
        ("Wright", 10, "RFT"),
        ("phone", 10, "FN"),
        ("cutoff", 2, "KT"),
    ] {
        let mut out = Vec::new();
        metaphone(word.as_bytes(), len, &mut out);
        assert_eq!(String::from_utf8(out).unwrap(), want, "metaphone({word})");
    }
}

#[test]
fn metaphone_limit_errors() {
    // C's exact error texts (fuzzystrmatch.c metaphone()).
    let e = metaphone_check_limits(256, 4).unwrap_err();
    assert!(e
        .to_string()
        .contains("argument exceeds the maximum length of 255 bytes"));
    let e = metaphone_check_limits(5, 256).unwrap_err();
    assert!(e
        .to_string()
        .contains("output exceeds the maximum length of 255 bytes"));
    let e = metaphone_check_limits(5, 0).unwrap_err();
    assert!(e.to_string().contains("output cannot be empty string"));
    assert!(metaphone_check_limits(255, 255).is_ok());
}

#[test]
fn dmetaphone_oracle() {
    let (p, a) = dmetaphone::double_metaphone(b"gumbo");
    assert_eq!((p.as_slice(), a.as_slice()), (&b"KMP"[..], &b"KMP"[..]));
    // Classic primary/alternate splits (C dmetaphone reference behavior).
    let (p, a) = dmetaphone::double_metaphone(b"Schmidt");
    assert_eq!((p.as_slice(), a.as_slice()), (&b"XMT"[..], &b"SMT"[..]));
    let (p, a) = dmetaphone::double_metaphone(b"Jose");
    assert_eq!((p.as_slice(), a.as_slice()), (&b"HS"[..], &b"HS"[..]));
    let (p, a) = dmetaphone::double_metaphone(b"");
    assert!(p.is_empty() && a.is_empty());
}

fn dm(s: &str) -> String {
    let ctx = mcx::MemoryContext::new("dm test");
    let mcx = ctx.mcx();
    let mut codes: mcx::PgVec<'_, [u8; 6]> = mcx::vec_with_capacity_in(mcx, 8).unwrap();
    if !daitch_mokotoff::daitch_mokotoff_coding(mcx, s.as_bytes(), &mut codes).unwrap() {
        return "NULL".into();
    }
    codes
        .iter()
        .map(|c| core::str::from_utf8(c).unwrap())
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
fn daitch_mokotoff_oracle() {
    assert_eq!(dm("Augsburg"), "054795");
    assert_eq!(dm("Breuer"), "791900");
    assert_eq!(dm("Freud"), "793000");
    assert_eq!(dm("Halberstadt"), "587943,587433");
    assert_eq!(dm("Mannheim"), "665600");
    assert_eq!(dm("Chernowitz"), "596740,496740");
    assert_eq!(dm("Cherkassy"), "595400,495400");
    assert_eq!(dm("Kleinman"), "586660");
    assert_eq!(dm("Nowy Targ"), "673950");
    assert_eq!(dm("Berlin"), "798600");
    assert_eq!(dm("Ceniow"), "567000,467000");
    assert_eq!(dm("Tsenyuv"), "467000");
    assert_eq!(dm("Holubica"), "587500,587400");
    assert_eq!(dm("Golubitsa"), "587400");
    assert_eq!(dm("Przemysl"), "794648,746480");
    assert_eq!(dm("Pshemeshil"), "746480");
    assert_eq!(
        dm("Rosochowaciec"),
        "945755,945754,945745,945744,944755,944754,944745,944744"
    );
    assert_eq!(dm("Rosokhovatsets"), "945744");
    assert_eq!(dm("'OBrien"), "079600");
    assert_eq!(dm("O'Brien"), "079600");
    assert_eq!(dm("CJC"), "550000,540000,545000,450000,400000,440000");
    assert_eq!(dm("BESST"), "743000");
    assert_eq!(dm("BOUEY"), "710000");
    assert_eq!(dm("HANNMANN"), "566600");
    assert_eq!(
        dm("MCCOYJR"),
        "651900,654900,654190,654490,645190,645490,641900,644900"
    );
    assert_eq!(
        dm("ACCURSO"),
        "059400,054000,054940,054400,045940,045400,049400,044000"
    );
    assert_eq!(
        dm("BIERSCHBACH"),
        "794575,794574,794750,794740,745750,745740,747500,747400"
    );
    assert_eq!(dm(""), "NULL");
}

#[test]
fn daitch_mokotoff_utf8_oracle() {
    assert_eq!(dm("Müller"), "689000");
    assert_eq!(dm("Schäfer"), "479000");
    assert_eq!(dm("Straßburg"), "294795");
    assert_eq!(dm("Éregon"), "095600");
    assert_eq!(dm("gąszczu"), "564000,540000");
    assert_eq!(dm("brzęczy"), "794640,794400,746400,744000");
    assert_eq!(dm("ţamas"), "364000,464000");
    assert_eq!(dm("țamas"), "364000,464000");
}
