use super::*;
use types_wchar::encoding::{PG_EUC_CN, PG_UTF8};

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
fn ascii_passes_through() {
    install_check_seam();
    let r = utf8_to_euc_cn(PG_UTF8, PG_EUC_CN, b"ascii", false).unwrap();
    assert_eq!(&*r.bytes, b"ascii");
    assert_eq!(r.converted, 5);
    let back = euc_cn_to_utf8(PG_EUC_CN, PG_UTF8, b"ascii", false).unwrap();
    assert_eq!(&*back.bytes, b"ascii");
}

#[test]
fn multibyte_round_trips() {
    install_check_seam();
    // A representative multibyte string, exercised UTF-8 -> local -> UTF-8.
    let utf8: &[u8] = &[228, 184, 173, 230, 150, 135];
    let local = utf8_to_euc_cn(PG_UTF8, PG_EUC_CN, utf8, false).unwrap();
    assert_eq!(local.converted, utf8.len() as i32);
    assert!(!local.bytes.is_empty());
    let back = euc_cn_to_utf8(PG_EUC_CN, PG_UTF8, &local.bytes, false).unwrap();
    assert_eq!(&*back.bytes, utf8);
    assert_eq!(back.converted, local.bytes.len() as i32);
}

#[test]
fn wrong_source_encoding_is_rejected() {
    install_check_seam();
    assert!(utf8_to_euc_cn(PG_EUC_CN, PG_EUC_CN, b"x", false).is_err());
}
