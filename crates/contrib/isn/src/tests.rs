use super::*;

// Parse text as `accept` (strong mode) and unwrap the ean13 value.
fn parse(s: &str, accept: IsnType) -> Ean13 {
    string2ean(s.as_bytes(), None, accept, false)
        .expect("no hard error")
        .expect("parsed")
}

// Format an ean13 value, `short` = the legacy ISxN short output.
fn fmt(v: Ean13, short: bool) -> String {
    let mut buf = [0u8; MAXEAN13LEN + 1];
    ean2string(v, &mut buf, short).expect("formats");
    let end = buf.iter().position(|&c| c == 0).unwrap();
    String::from_utf8(buf[..end].to_vec()).unwrap()
}

// text --(accept)--> ean13 --(short output)--> string
fn roundtrip(s: &str, accept: IsnType, short: bool) -> String {
    fmt(parse(s, accept), short)
}

#[test]
fn ean13_valid_conversions() {
    assert_eq!(roundtrip("9780123456786", IsnType::Ean13, false), "978-0-12-345678-6");
    assert_eq!(roundtrip("9790123456785", IsnType::Ean13, false), "979-0-1234-5678-5");
    assert_eq!(roundtrip("9791234567896", IsnType::Ean13, false), "979-123456789-6");
    assert_eq!(roundtrip("9771234567898", IsnType::Ean13, false), "977-1234-567-89-8");
    assert_eq!(roundtrip("0123456789012", IsnType::Ean13, false), "012-345678901-2");
    assert_eq!(roundtrip("1234567890128", IsnType::Ean13, false), "123-456789012-8");
}

#[test]
fn isbn_short_and_13() {
    // ::ISBN uses isn_out (short); ::ISBN13 uses ean13_out (long).
    assert_eq!(roundtrip("9780123456786", IsnType::Isbn, true), "0-12-345678-9");
    assert_eq!(roundtrip("123456789X", IsnType::Isbn, true), "1-234-56789-X");
    assert_eq!(roundtrip("9791234567896", IsnType::Isbn, true), "979-123456789-6");
    assert_eq!(roundtrip("9780123456786", IsnType::Isbn, false), "978-0-12-345678-6");
    assert_eq!(roundtrip("123456789X", IsnType::Isbn, false), "978-1-234-56789-7");
    assert_eq!(roundtrip("9791234567896", IsnType::Isbn, false), "979-123456789-6");
}

#[test]
fn ismn_short_and_13() {
    assert_eq!(roundtrip("9790123456785", IsnType::Ismn, true), "M-1234-5678-5");
    assert_eq!(roundtrip("M123456785", IsnType::Ismn, true), "M-1234-5678-5");
    assert_eq!(roundtrip("M-1234-5678-5", IsnType::Ismn, true), "M-1234-5678-5");
    assert_eq!(roundtrip("9790123456785", IsnType::Ismn, false), "979-0-1234-5678-5");
    assert_eq!(roundtrip("M123456785", IsnType::Ismn, false), "979-0-1234-5678-5");
}

#[test]
fn issn_short_and_13() {
    assert_eq!(roundtrip("9771234567003", IsnType::Issn, true), "1234-5679");
    assert_eq!(roundtrip("12345679", IsnType::Issn, true), "1234-5679");
    assert_eq!(roundtrip("9771234567003", IsnType::Issn, false), "977-1234-567-00-3");
    assert_eq!(roundtrip("9771234567898", IsnType::Issn, false), "977-1234-567-89-8");
}

#[test]
fn upc_output() {
    assert_eq!(roundtrip("0123456789012", IsnType::Upc, true), "123456789012");
}

#[test]
fn ean13_to_subtype_casts() {
    // '...'::EAN13::ISBN goes through ean2isn on the stored value.
    let ean = parse("9780123456786", IsnType::Ean13);
    let as_isbn = ean2isn(ean, IsnType::Isbn).expect("cast ok");
    assert_eq!(fmt(as_isbn, true), "0-12-345678-9");

    let ean = parse("0123456789012", IsnType::Ean13);
    let as_upc = ean2isn(ean, IsnType::Upc).expect("cast ok");
    assert_eq!(fmt(as_upc, true), "123456789012");

    let ean = parse("9791234567896", IsnType::Ean13);
    let as_isbn = ean2isn(ean, IsnType::Isbn).expect("cast ok");
    assert_eq!(fmt(as_isbn, true), "979-123456789-6");
}

#[test]
fn invalid_check_digit_messages() {
    let cases = [
        ("1234567890", IsnType::Isbn, "should be X"),
        ("M123456780", IsnType::Ismn, "should be 5"),
        ("12345670", IsnType::Issn, "should be 9"),
        ("9780123456780", IsnType::Isbn, "should be 6"),
        ("9791234567890", IsnType::Isbn, "should be 6"),
        ("0123456789010", IsnType::Upc, "should be 2"),
        ("1234567890120", IsnType::Ean13, "should be 8"),
    ];
    for (s, accept, needle) in cases {
        let err = string2ean(s.as_bytes(), None, accept, false).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid check digit"), "{s}: {msg}");
        assert!(msg.ends_with(needle), "{s}: {msg}");
    }
}

#[test]
fn wrong_type_casts_error() {
    // '9790123456785'::ISBN -> cannot cast ISMN to ISBN
    let err = string2ean("9790123456785".as_bytes(), None, IsnType::Isbn, false).unwrap_err();
    assert_eq!(
        err.to_string(),
        "cannot cast ISMN to ISBN for number: \"9790123456785\""
    );
    let err = string2ean("9771234567898".as_bytes(), None, IsnType::Isbn, false).unwrap_err();
    assert_eq!(
        err.to_string(),
        "cannot cast ISSN to ISBN for number: \"9771234567898\""
    );
    let err = string2ean("0123456789012".as_bytes(), None, IsnType::Isbn, false).unwrap_err();
    assert_eq!(
        err.to_string(),
        "cannot cast UPC to ISBN for number: \"0123456789012\""
    );
}

#[test]
fn invalid_syntax_errors() {
    let err = string2ean("postgresql...".as_bytes(), None, IsnType::Ean13, false).unwrap_err();
    assert_eq!(
        err.to_string(),
        "invalid input syntax for EAN13 number: \"postgresql...\""
    );
}

#[test]
fn weak_mode_marks_invalid() {
    // Fails strong.
    assert!(string2ean("2222222222221".as_bytes(), None, IsnType::Ean13, false).is_err());
    // Accepted weak, flagged invalid (bit 0 set).
    let v = string2ean("2222222222221".as_bytes(), None, IsnType::Ean13, true)
        .unwrap()
        .unwrap();
    assert_eq!(v & 1, 1);
    assert_eq!(fmt(v, false), "222-222222222-2!");
    // make_valid clears the flag; is_valid then true.
    let cleared = v & !1u64;
    assert_eq!(fmt(cleared, false), "222-222222222-2");
}

#[test]
fn soft_error_saves_not_throws() {
    let mut ctx = SoftErrorContext::new(true);
    let r = string2ean(
        "postgresql...".as_bytes(),
        Some(&mut ctx),
        IsnType::Ean13,
        false,
    )
    .expect("soft path returns Ok");
    assert!(r.is_none());
    assert!(ctx.error_occurred());
}

#[test]
fn magic_question_mark_computes_check() {
    // '?' as the last char asks for the check digit; result is valid.
    let v = parse("012345678901?", IsnType::Ean13);
    assert_eq!(v & 1, 0);
    assert_eq!(fmt(v, false), "012-345678901-2");
}
