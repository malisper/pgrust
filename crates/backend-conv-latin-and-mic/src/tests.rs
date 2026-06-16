use super::*;

// The `check_encoding_conversion_args` seam is owned by the unported
// `utils/mb/mbutils.c`; install a faithful 1:1 copy of that function for the
// duration of these tests so the conversion entry points are exercisable.
fn install_check_seam() {
    use backend_utils_error::elog;
    use types_error::ERROR;
    use types_wchar::encoding::{pg_enc, pg_valid_encoding};
    fn check(
        src_encoding: pg_enc,
        dest_encoding: pg_enc,
        len: i32,
        expected_src_encoding: pg_enc,
        expected_dest_encoding: pg_enc,
    ) -> backend_utils_error::PgResult<()> {
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
    if !backend_utils_mb_mbutils_seams::check_encoding_conversion_args::is_installed() {
        backend_utils_mb_mbutils_seams::check_encoding_conversion_args::set(check);
    }
}

#[test]
fn latin1_to_mic_prefixes_lead_charset() {
    install_check_seam();
    // High-bit byte 0xE9 becomes LC_ISO8859_1 0xE9; ASCII passes through.
    let r = latin1_to_mic(PG_LATIN1, PG_MULE_INTERNAL, b"a\xe9b", false).unwrap();
    assert_eq!(&r.bytes[..], b"a\x81\xe9b");
    assert_eq!(r.converted, 3);
}

#[test]
fn mic_to_latin1_strips_lead_charset() {
    install_check_seam();
    let r = mic_to_latin1(PG_MULE_INTERNAL, PG_LATIN1, b"a\x81\xe9b", false).unwrap();
    assert_eq!(&r.bytes[..], b"a\xe9b");
    assert_eq!(r.converted, 4);
}

#[test]
fn latin1_round_trip() {
    install_check_seam();
    let mic = latin1_to_mic(PG_LATIN1, PG_MULE_INTERNAL, b"x\xc0\xff y", false).unwrap();
    let back = mic_to_latin1(PG_MULE_INTERNAL, PG_LATIN1, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"x\xc0\xff y");
}

#[test]
fn latin3_round_trip() {
    install_check_seam();
    let mic = latin3_to_mic(PG_LATIN3, PG_MULE_INTERNAL, b"\xc1z", false).unwrap();
    assert_eq!(&mic.bytes[..], b"\x83\xc1z");
    let back = mic_to_latin3(PG_MULE_INTERNAL, PG_LATIN3, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"\xc1z");
}

#[test]
fn latin4_round_trip() {
    install_check_seam();
    let mic = latin4_to_mic(PG_LATIN4, PG_MULE_INTERNAL, b"\xb5", false).unwrap();
    assert_eq!(&mic.bytes[..], b"\x84\xb5");
    let back = mic_to_latin4(PG_MULE_INTERNAL, PG_LATIN4, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"\xb5");
}

#[test]
fn no_error_stops_on_bad_mic() {
    install_check_seam();
    // A mule lead byte not matching LC_ISO8859_1 is untranslatable; no_error
    // stops cleanly at the offending position.
    let r = mic_to_latin1(PG_MULE_INTERNAL, PG_LATIN1, b"a\x83\xe9", true).unwrap();
    assert_eq!(&r.bytes[..], b"a");
    assert_eq!(r.converted, 1);
}
