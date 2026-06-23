use super::*;
use error_fgram::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE;

#[test]
fn ascii_round_trips() {
    let to_sjis =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, b"abc", false).unwrap();
    assert_eq!(&*to_sjis.bytes, b"abc");
    assert_eq!(to_sjis.converted, 3);

    let to_euc =
        shift_jis_2004_to_euc_jis_2004(PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, b"abc", false).unwrap();
    assert_eq!(&*to_euc.bytes, b"abc");
    assert_eq!(to_euc.converted, 3);
}

#[test]
fn converts_euc_plane1_to_sjis() {
    let result =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, b"\xa4\xa2", false)
            .unwrap();
    assert_eq!(&*result.bytes, b"\x82\xa0");
    assert_eq!(result.converted, 2);
}

#[test]
fn converts_sjis_plane1_to_euc() {
    let result =
        shift_jis_2004_to_euc_jis_2004(PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, b"\x82\xa0", false)
            .unwrap();
    assert_eq!(&*result.bytes, b"\xa4\xa2");
    assert_eq!(result.converted, 2);
}

#[test]
fn converts_euc_kana_to_sjis() {
    let result =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, b"\x8e\xa6", false)
            .unwrap();
    assert_eq!(&*result.bytes, b"\xa6");
    assert_eq!(result.converted, 2);
}

#[test]
fn converts_sjis_kana_to_euc() {
    let result =
        shift_jis_2004_to_euc_jis_2004(PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, b"\xa6", false).unwrap();
    assert_eq!(&*result.bytes, b"\x8e\xa6");
    assert_eq!(result.converted, 1);
}

#[test]
fn converts_euc_plane2_to_sjis() {
    let result =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, b"\x8f\xa1\xa1", false)
            .unwrap();
    assert_eq!(&*result.bytes, b"\xf0\x40");
    assert_eq!(result.converted, 3);
}

#[test]
fn converts_sjis_plane2_to_euc() {
    let result =
        shift_jis_2004_to_euc_jis_2004(PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, b"\xf0\x40", false)
            .unwrap();
    assert_eq!(&*result.bytes, b"\x8f\xa1\xa1");
    assert_eq!(result.converted, 2);
}

#[test]
fn round_trips_plane1_and_plane2() {
    // EUC plane-1 (a4 a2), kana (8e a6) and plane-2 (8f a1 a1) all in one run.
    let euc = b"\xa4\xa2\x8e\xa6\x8f\xa1\xa1";

    let sjis =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, euc, false).unwrap();
    let back =
        shift_jis_2004_to_euc_jis_2004(PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, &sjis.bytes, false)
            .unwrap();
    assert_eq!(&*back.bytes, euc);
}

#[test]
fn no_error_stops_at_embedded_nul() {
    let result =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, b"a\0b", true).unwrap();
    assert_eq!(&*result.bytes, b"a");
    assert_eq!(result.converted, 1);
}

#[test]
fn reports_embedded_nul() {
    let error =
        shift_jis_2004_to_euc_jis_2004(PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, b"a\0b", false)
            .unwrap_err();

    assert_eq!(error.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
    assert_eq!(
        error.message(),
        "invalid byte sequence for encoding \"SHIFT_JIS_2004\": 0x00"
    );
}

#[test]
fn validates_conversion_arguments() {
    let error =
        euc_jis_2004_to_shift_jis_2004(PG_SHIFT_JIS_2004, PG_SHIFT_JIS_2004, b"a", false)
            .unwrap_err();

    assert_eq!(
        error.message(),
        "expected source encoding \"EUC_JIS_2004\", but got \"SHIFT_JIS_2004\""
    );
}

#[test]
fn empty_input_yields_empty_output() {
    let result =
        euc_jis_2004_to_shift_jis_2004(PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, b"", false).unwrap();
    assert!(result.bytes.is_empty());
    assert_eq!(result.converted, 0);
}
