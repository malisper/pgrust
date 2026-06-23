use super::*;

// The `check_encoding_conversion_args` seam is owned by the unported
// `utils/mb/mbutils.c`; install a faithful 1:1 copy of that function for the
// duration of these tests so the conversion entry points are exercisable.
fn install_check_seam() {
    use ::utils_error::elog;
    use ::types_error::ERROR;
    use ::types_wchar::encoding::{pg_enc, pg_valid_encoding};
    fn check(
        src_encoding: pg_enc,
        dest_encoding: pg_enc,
        len: i32,
        expected_src_encoding: pg_enc,
        expected_dest_encoding: pg_enc,
    ) -> ::utils_error::PgResult<()> {
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
fn koi8r_mic_round_trip() {
    install_check_seam();
    // KOI8-R is the MULE Cyrillic charset: high-bit byte 0xE1 is prefixed with
    // LC_KOI8_R (0x8B).
    let mic = koi8r_to_mic(PG_KOI8R, PG_MULE_INTERNAL, b"a\xe1b", false).unwrap();
    assert_eq!(&mic.bytes[..], b"a\x8b\xe1b");
    let back = mic_to_koi8r(PG_MULE_INTERNAL, PG_KOI8R, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"a\xe1b");
}

#[test]
fn iso_to_mic_via_table() {
    install_check_seam();
    // ISO-8859-5 0xB0 -> KOI8-R 0xE1 (ISO2KOI), prefixed with LC_KOI8_R.
    let r = iso_to_mic(PG_ISO_8859_5, PG_MULE_INTERNAL, b"\xb0", false).unwrap();
    assert_eq!(&r.bytes[..], b"\x8b\xe1");
}

#[test]
fn mic_to_iso_via_table() {
    install_check_seam();
    // MULE LC_KOI8_R 0xE1 -> KOI8-R 0xE1 -> ISO-8859-5 0xB0 (KOI2ISO).
    let r = mic_to_iso(PG_MULE_INTERNAL, PG_ISO_8859_5, b"\x8b\xe1", false).unwrap();
    assert_eq!(&r.bytes[..], b"\xb0");
}

#[test]
fn win1251_mic_round_trip() {
    install_check_seam();
    // WIN1251 0xE0 -> KOI8-R via WIN12512KOI then prefixed; reverse via KOI2WIN1251.
    let mic = win1251_to_mic(PG_WIN1251, PG_MULE_INTERNAL, b"\xe0", false).unwrap();
    let back = mic_to_win1251(PG_MULE_INTERNAL, PG_WIN1251, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"\xe0");
}

#[test]
fn win866_mic_round_trip() {
    install_check_seam();
    let mic = win866_to_mic(PG_WIN866, PG_MULE_INTERNAL, b"\x80", false).unwrap();
    let back = mic_to_win866(PG_MULE_INTERNAL, PG_WIN866, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"\x80");
}

#[test]
fn koi8r_to_win1251_local2local() {
    install_check_seam();
    // KOI8-R 0xE1 -> WIN1251 0xC0 (KOI2WIN1251).
    let r = koi8r_to_win1251(PG_KOI8R, PG_WIN1251, b"\xe1", false).unwrap();
    assert_eq!(&r.bytes[..], b"\xc0");
}

#[test]
fn iso_koi_local2local_round_trip() {
    install_check_seam();
    // ISO-8859-5 0xB0 -> KOI8-R 0xE1 -> ISO-8859-5 0xB0.
    let k = iso_to_koi8r(PG_ISO_8859_5, PG_KOI8R, b"\xb0", false).unwrap();
    assert_eq!(&k.bytes[..], b"\xe1");
    let back = koi8r_to_iso(PG_KOI8R, PG_ISO_8859_5, &k.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"\xb0");
}

#[test]
fn win866_iso_local2local() {
    install_check_seam();
    // Exercise iso_to_win866 / win866_to_iso once each.
    let w = iso_to_win866(PG_ISO_8859_5, PG_WIN866, b"\xb0", false).unwrap();
    assert_eq!(&w.bytes[..], b"\x80");
    let back = win866_to_iso(PG_WIN866, PG_ISO_8859_5, &w.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"\xb0");
}

#[test]
fn win_pairs_smoke() {
    install_check_seam();
    // win866 <-> win1251 and win1251 <-> win866 / iso pairs reachable.
    let a = win866_to_win1251(PG_WIN866, PG_WIN1251, b"\x80", false).unwrap();
    let b = win1251_to_win866(PG_WIN1251, PG_WIN866, &a.bytes, false).unwrap();
    assert_eq!(&b.bytes[..], b"\x80");
    let c = win1251_to_koi8r(PG_WIN1251, PG_KOI8R, b"\xe0", false).unwrap();
    assert!(!c.bytes.is_empty());
    let d = koi8r_to_win866(PG_KOI8R, PG_WIN866, b"\xe1", false).unwrap();
    assert!(!d.bytes.is_empty());
    let e = win866_to_koi8r(PG_WIN866, PG_KOI8R, b"\x80", false).unwrap();
    assert!(!e.bytes.is_empty());
    let f = iso_to_win1251(PG_ISO_8859_5, PG_WIN1251, b"\xb0", false).unwrap();
    assert!(!f.bytes.is_empty());
    let g = win1251_to_iso(PG_WIN1251, PG_ISO_8859_5, &f.bytes, false).unwrap();
    assert_eq!(&g.bytes[..], b"\xb0");
}

#[test]
fn no_error_stops_on_untranslatable() {
    install_check_seam();
    // ISO-8859-5 0x80 maps to KOI8-R 0x00 (untranslatable). no_error stops.
    let r = iso_to_koi8r(PG_ISO_8859_5, PG_KOI8R, b"q\x80", true).unwrap();
    assert_eq!(&r.bytes[..], b"q");
    assert_eq!(r.converted, 1);
}
