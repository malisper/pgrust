use super::*;
use backend_utils_error::{ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_UNTRANSLATABLE_CHARACTER};
use types_wchar::encoding::PG_UTF8;

#[test]
fn converts_euc_cn_to_mic() {
    let result = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"ab\xd2\xbb", false).unwrap();
    assert_eq!(&result.bytes[..], b"ab\x91\xd2\xbb");
    assert_eq!(result.converted, 4);
}

#[test]
fn converts_mic_to_euc_cn() {
    let result = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, b"ab\x91\xd2\xbb", false).unwrap();
    assert_eq!(&result.bytes[..], b"ab\xd2\xbb");
    assert_eq!(result.converted, 5);
}

#[test]
fn round_trips_through_mic() {
    let mic = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"x\xd2\xbby", false).unwrap();
    assert_eq!(&mic.bytes[..], b"x\x91\xd2\xbby");
    assert_eq!(mic.converted, 4);

    let back = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"x\xd2\xbby");
    assert_eq!(back.converted, mic.bytes.len() as i32);
}

#[test]
fn no_error_stops_at_truncated_euc_cn() {
    let result = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"ab\xd2", true).unwrap();
    assert_eq!(&result.bytes[..], b"ab");
    assert_eq!(result.converted, 2);
}

#[test]
fn reports_invalid_euc_cn() {
    let error = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"\xd2a", false).unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
    assert_eq!(
        error.message(),
        "invalid byte sequence for encoding \"EUC_CN\": 0xd2 0x61"
    );
}

#[test]
fn no_error_stops_at_untranslatable_mic() {
    let result = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, b"ab\x92\xd2\xbb", true).unwrap();
    assert_eq!(&result.bytes[..], b"ab");
    assert_eq!(result.converted, 2);
}

#[test]
fn reports_untranslatable_mic() {
    let error = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, b"\x92\xd2\xbb", false).unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_UNTRANSLATABLE_CHARACTER);
    assert_eq!(
        error.message(),
        "character with byte sequence 0x92 0xd2 0xbb in encoding \"MULE_INTERNAL\" has no equivalent in encoding \"EUC_CN\""
    );
}

#[test]
fn no_error_stops_at_truncated_mic() {
    let result = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, b"ab\x91\xd2", true).unwrap();
    assert_eq!(&result.bytes[..], b"ab");
    assert_eq!(result.converted, 2);
}

#[test]
fn reports_invalid_mic_sequence() {
    let error = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, b"\x91\xd2a", false).unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
    assert_eq!(
        error.message(),
        "invalid byte sequence for encoding \"MULE_INTERNAL\": 0x91 0xd2 0x61"
    );
}

#[test]
fn reports_embedded_nul() {
    let error = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"a\0b", false).unwrap_err();
    assert_eq!(error.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
    assert_eq!(
        error.message(),
        "invalid byte sequence for encoding \"EUC_CN\": 0x00"
    );
}

#[test]
fn validates_conversion_arguments() {
    let error = euc_cn_to_mic(PG_UTF8, PG_MULE_INTERNAL, b"abc", false).unwrap_err();
    assert_eq!(
        error.message(),
        "expected source encoding \"EUC_CN\", but got \"UTF8\""
    );
}

#[test]
fn empty_input_yields_empty_output() {
    let result = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"", false).unwrap();
    assert!(result.bytes.is_empty());
    assert_eq!(result.converted, 0);
}

#[test]
fn ascii_passes_through_both_directions() {
    let mic = euc_cn_to_mic(PG_EUC_CN, PG_MULE_INTERNAL, b"hello", false).unwrap();
    assert_eq!(&mic.bytes[..], b"hello");
    let back = mic_to_euc_cn(PG_MULE_INTERNAL, PG_EUC_CN, &mic.bytes, false).unwrap();
    assert_eq!(&back.bytes[..], b"hello");
}
