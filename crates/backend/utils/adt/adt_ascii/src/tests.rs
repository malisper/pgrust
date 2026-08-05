use super::*;

fn conv(bytes: &[u8], enc: pg_enc) -> Result<Vec<u8>, String> {
    let mut out = vec![0u8; bytes.len()];
    pg_to_ascii(bytes, &mut out, enc).map_err(|e| format!("{e:?}"))?;
    Ok(out)
}

#[test]
fn latin1_accents_fold() {
    // 0xC0 'À' -> 'A'; 0xE9 'é' -> 'e'; 0xF7 '÷' -> '/'; 0xFF 'ÿ' -> 'y'
    assert_eq!(conv(&[0xC0, 0xE9, 0xF7, 0xFF], PG_LATIN1).unwrap(), b"Ae/y");
}

#[test]
fn ascii_passthrough_and_bogus_range() {
    assert_eq!(conv(b"abc XYZ", PG_LATIN1).unwrap(), b"abc XYZ");
    // bytes in 128..160 map to space for latin encodings
    assert_eq!(conv(&[0x80, 0x9F], PG_LATIN1).unwrap(), b"  ");
}

#[test]
fn win1250_range_starts_at_128() {
    // 0x8A is 'Š' in cp1250 -> 'S' (map index 10)
    assert_eq!(conv(&[0x8A], PG_WIN1250).unwrap(), b"S");
    assert_eq!(conv(&[0xF7], PG_WIN1250).unwrap(), b"/");
}

#[test]
fn latin2_and_latin9() {
    // latin2 0xA3 'Ł' -> 'L'; latin9 0xA4 '€' -> 'E'? (C map: index 4 = ' ')
    assert_eq!(conv(&[0xA3], PG_LATIN2).unwrap(), b"L");
    // latin9 0xBC is 'Œ'->'E' per C map index 28
    assert_eq!(conv(&[0xBC], PG_LATIN9).unwrap(), b"E");
}

#[test]
fn unsupported_encoding_errors() {
    assert!(conv(b"x", wchar::PG_UTF8).is_err());
}

#[test]
fn safe_strlcpy_replaces_nonascii() {
    let mut dest = [0u8; 8];
    ascii_safe_strlcpy(&mut dest, b"a\xC3\xA9b\tc");
    assert_eq!(&dest[..7], b"a??b\tc\0");
    let mut tiny = [0u8; 1];
    ascii_safe_strlcpy(&mut tiny, b"xyz");
    assert_eq!(tiny[0], 0);
}
