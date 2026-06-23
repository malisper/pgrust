use super::*;

// The `check_encoding_conversion_args` seam is owned by the unported
// `utils/mb/mbutils.c`; install a faithful 1:1 copy of that function for the
// duration of these tests so the conversion entry points are exercisable.
fn install_check_seam() {
    use utils_error::elog;
    use types_error::ERROR;
    use types_wchar::encoding::{pg_enc, pg_valid_encoding};
    fn check(
        src_encoding: pg_enc,
        dest_encoding: pg_enc,
        len: i32,
        expected_src_encoding: pg_enc,
        expected_dest_encoding: pg_enc,
    ) -> utils_error::PgResult<()> {
        if !pg_valid_encoding(src_encoding) {
            return elog(ERROR, format!("invalid source encoding ID: {src_encoding}"));
        }
        if src_encoding != expected_src_encoding && expected_src_encoding >= 0 {
            return elog(ERROR, "expected source encoding mismatch".to_string());
        }
        if !pg_valid_encoding(dest_encoding) {
            return elog(ERROR, format!("invalid destination encoding ID: {dest_encoding}"));
        }
        if dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0 {
            return elog(ERROR, "expected destination encoding mismatch".to_string());
        }
        if len < 0 {
            return elog(ERROR, "encoding conversion length must not be negative".to_string());
        }
        Ok(())
    }
    if !mbutils_seams::check_encoding_conversion_args::is_installed() {
        mbutils_seams::check_encoding_conversion_args::set(check);
    }
}

#[test]
fn latin2_mic_round_trip() {
    install_check_seam();
    // LATIN2 high-bit byte 0xC1 -> LC_ISO8859_2 0xC1.
    let mic = latin2_to_mic(PG_LATIN2, PG_MULE_INTERNAL, b"a\xc1b", false).unwrap();
    assert_eq!(&mic.bytes[..], b"a\x82\xc1b");
    let back = mic_to_latin2(PG_MULE_INTERNAL, PG_LATIN2, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"a\xc1b");
}

#[test]
fn win1250_to_mic_uses_table() {
    install_check_seam();
    // WIN1250 0x8A -> ISO-8859-2 0xA9 (WIN1250_2_ISO88592[0x0A]); emitted as
    // LC_ISO8859_2 0xA9.
    let r = win1250_to_mic(PG_WIN1250, PG_MULE_INTERNAL, b"\x8a", false).unwrap();
    assert_eq!(&r.bytes[..], b"\x82\xa9");
}

#[test]
fn mic_to_win1250_uses_table() {
    install_check_seam();
    // MULE LC_ISO8859_2 0xA9 -> ISO-8859-2 0xA9 -> WIN1250 0x8A
    // (ISO88592_2_WIN1250[0x29] == 0x8A).
    let r = mic_to_win1250(PG_MULE_INTERNAL, PG_WIN1250, b"\x82\xa9", false).unwrap();
    assert_eq!(&r.bytes[..], b"\x8a");
}

#[test]
fn latin2_to_win1250_local2local() {
    install_check_seam();
    // ISO-8859-2 0xA9 -> WIN1250 0x8A via ISO88592_2_WIN1250.
    let r = latin2_to_win1250(PG_LATIN2, PG_WIN1250, b"z\xa9", false).unwrap();
    assert_eq!(&r.bytes[..], b"z\x8a");
}

#[test]
fn win1250_to_latin2_local2local() {
    install_check_seam();
    // WIN1250 0x8A -> ISO-8859-2 0xA9 via WIN1250_2_ISO88592.
    let r = win1250_to_latin2(PG_WIN1250, PG_LATIN2, b"z\x8a", false).unwrap();
    assert_eq!(&r.bytes[..], b"z\xa9");
}

#[test]
fn no_error_stops_on_untranslatable() {
    install_check_seam();
    // ISO-8859-2 0x8A maps to WIN1250 0x00 (untranslatable). no_error stops.
    let r = latin2_to_win1250(PG_LATIN2, PG_WIN1250, b"k\x8a", true).unwrap();
    assert_eq!(&r.bytes[..], b"k");
    assert_eq!(r.converted, 1);
}
