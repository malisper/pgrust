use super::*;

fn out(ch: i8) -> Vec<u8> {
    let mut buf = [0u8; 4];
    let n = charout(ch, &mut buf);
    buf[..n].to_vec()
}

#[test]
fn charin_forms() {
    assert_eq!(charin(b"x"), b'x' as i8);
    assert_eq!(charin(b""), 0);
    assert_eq!(charin(b"xyz"), b'x' as i8);
    assert_eq!(charin(b"\\101"), 0x41);
    assert_eq!(charin(b"\\377"), -1);
    // Leading digit > 3 wraps modulo 256 (C int arithmetic truncated to char).
    assert_eq!(charin(b"\\700"), ((0o700u32 as u8) as i8));
    // Not exactly 4 bytes: first byte taken literally.
    assert_eq!(charin(b"\\37"), b'\\' as i8);
    assert_eq!(charin(b"\\3777"), b'\\' as i8);
}

#[test]
fn charout_forms() {
    assert_eq!(out(0), b"");
    assert_eq!(out(b'A' as i8), b"A");
    assert_eq!(out(-1), b"\\377");
    assert_eq!(out(-128i8), b"\\200");
}

#[test]
fn charout_charin_roundtrip() {
    for v in i8::MIN..=i8::MAX {
        assert_eq!(charin(&out(v)), v, "roundtrip {v}");
    }
}

#[test]
fn comparisons_unsigned() {
    // 0x80 (-128 as i8) sorts above 0x7f under unsigned comparison.
    assert!(chargt(-128, 127));
    assert!(charlt(127, -128));
    assert!(charle(1, 1) && charge(1, 1));
    assert!(chareq(-5, -5) && charne(-5, 5));
    assert!(!charlt(-1, 1));
}

#[test]
fn int_casts_signed() {
    assert_eq!(chartoi4(-1), -1);
    assert_eq!(chartoi4(127), 127);
    assert_eq!(i4tochar(65).unwrap(), 65);
    assert_eq!(i4tochar(-128).unwrap(), -128);
    let e = i4tochar(128).unwrap_err();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(e.message(), "\"char\" out of range");
    assert!(i4tochar(-129).is_err());
}

#[test]
fn text_casts() {
    assert_eq!(text_char(b"q"), b'q' as i8);
    assert_eq!(text_char(b"\\377"), -1);
    assert_eq!(text_char(b""), 0);
}
