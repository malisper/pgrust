use super::*;
use ::types_wchar::encoding::{PG_KOI8R, PG_KOI8U, PG_UTF8};

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
fn koi8r_round_trips_cyrillic_text() {
    install_check_seam();
    let utf8 = "Привет".as_bytes();
    let koi8r = utf8_to_koi8r(PG_UTF8, PG_KOI8R, utf8, false).unwrap();
    assert_eq!(&*koi8r.bytes, b"\xf0\xd2\xc9\xd7\xc5\xd4");
    assert_eq!(koi8r.converted, utf8.len() as i32);

    let back = koi8r_to_utf8(PG_KOI8R, PG_UTF8, &koi8r.bytes, false).unwrap();
    assert_eq!(&*back.bytes, utf8);
    assert_eq!(back.converted, koi8r.bytes.len() as i32);
}

#[test]
fn koi8u_round_trips_ukrainian_letters() {
    install_check_seam();
    let utf8 = "ҐЄІЇ".as_bytes();
    let koi8u = utf8_to_koi8u(PG_UTF8, PG_KOI8U, utf8, false).unwrap();
    assert_eq!(&*koi8u.bytes, b"\xbd\xb4\xb6\xb7");

    let back = koi8u_to_utf8(PG_KOI8U, PG_UTF8, &koi8u.bytes, false).unwrap();
    assert_eq!(&*back.bytes, utf8);
}

#[test]
fn ascii_passes_through() {
    install_check_seam();
    let result = utf8_to_koi8r(PG_UTF8, PG_KOI8R, b"ascii", false).unwrap();
    assert_eq!(&*result.bytes, b"ascii");
    assert_eq!(result.converted, 5);
}

#[test]
fn wrong_source_encoding_is_rejected() {
    install_check_seam();
    assert!(utf8_to_koi8r(PG_KOI8R, PG_KOI8R, b"x", false).is_err());
}
