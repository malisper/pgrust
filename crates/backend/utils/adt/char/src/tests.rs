use super::*;
use mcx::MemoryContext;

#[test]
fn charin_plain_byte() {
    assert_eq!(charin("x"), b'x' as i8);
    // Zero-length input -> '\0'.
    assert_eq!(charin(""), 0);
    // Backwards-compat: multibyte keeps the first byte.
    assert_eq!(charin("abc"), b'a' as i8);
}

#[test]
fn charin_octal_escape() {
    // \377 == 0xFF == -1 as i8.
    assert_eq!(charin("\\377"), -1);
    // \101 == 0x41 == 'A'.
    assert_eq!(charin("\\101"), 0x41);
}

#[test]
fn charin_non_octal_4_bytes_takes_first() {
    // Not all octal digits -> not an escape -> first byte.
    assert_eq!(charin("\\9ab"), b'\\' as i8);
}

#[test]
fn charin_octal_high_leading_digit_wraps() {
    // C computes (o1<<6)+(o2<<3)+o3 in int then truncates to char on the
    // PG_RETURN_CHAR assignment: \700 = (7<<6) = 448 -> 448 & 0xff = 192.
    assert_eq!(charin("\\700") as u8, 192);
    // \777 = 511 -> 511 & 0xff = 255.
    assert_eq!(charin("\\777") as u8, 255);
    assert_eq!(text_char(b"\\700") as u8, 192);
}

#[test]
fn charout_plain() {
    {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        assert_eq!(charout(mcx, b'x' as i8).unwrap().as_str(), "x");
        // 0x00 -> empty string.
        assert_eq!(charout(mcx, 0).unwrap().as_str(), "");
    }
}

#[test]
fn charout_highbit_octal() {
    {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        // 0xFF -> "\377".
        assert_eq!(charout(mcx, -1).unwrap().as_str(), "\\377");
    }
}

#[test]
fn roundtrip_highbit() {
    {
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let c = charin("\\377");
        assert_eq!(charout(mcx, c).unwrap().as_str(), "\\377");
    }
}

#[test]
fn comparisons_are_unsigned() {
    // 0xFF (i8 -1) compares GREATER than 0x01 because comparisons are uint8.
    assert!(chargt(-1, 1));
    assert!(charlt(1, -1));
    assert!(charle(1, 1));
    assert!(charge(-1, -1));
    assert!(chareq(5, 5));
    assert!(charne(5, 6));
}

#[test]
fn chartoi4_sign_extends() {
    assert_eq!(chartoi4(-1), -1);
    assert_eq!(chartoi4(0x41), 65);
}

#[test]
fn i4tochar_range() {
    assert_eq!(i4tochar(65).unwrap(), 65);
    assert_eq!(i4tochar(-1).unwrap(), -1);
    assert!(i4tochar(200).is_err());
    assert!(i4tochar(-200).is_err());
}

#[test]
fn text_char_rules() {
    assert_eq!(text_char(b"\\377"), -1);
    assert_eq!(text_char(b"x"), b'x' as i8);
    assert_eq!(text_char(b""), 0);
    // 4 bytes but not all octal -> first byte.
    assert_eq!(text_char(b"\\9ab"), b'\\' as i8);
}
