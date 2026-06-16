use super::*;
use types_wchar::encoding::PG_UTF8;

#[test]
fn converts_euc_kr_to_mic() {
    let result = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"ab\xb0\xa1", false).unwrap();
    assert_eq!(result.bytes.as_slice(), b"ab\x93\xb0\xa1");
    assert_eq!(result.converted, 4);
}

#[test]
fn converts_mic_to_euc_kr() {
    let result = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, b"ab\x93\xb0\xa1", false).unwrap();
    assert_eq!(result.bytes.as_slice(), b"ab\xb0\xa1");
    assert_eq!(result.converted, 5);
}

#[test]
fn round_trips_through_mic() {
    let mic = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"x\xb0\xa1y", false).unwrap();
    assert_eq!(mic.bytes.as_slice(), b"x\x93\xb0\xa1y");
    assert_eq!(mic.converted, 4);

    let back = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, &mic.bytes, false).unwrap();
    assert_eq!(back.bytes.as_slice(), b"x\xb0\xa1y");
    assert_eq!(back.converted, mic.bytes.len() as i32);
}

#[test]
fn no_error_stops_at_invalid_euc_kr() {
    let result = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"ab\xb0", true).unwrap();
    assert_eq!(result.bytes.as_slice(), b"ab");
    assert_eq!(result.converted, 2);
}

#[test]
fn reports_invalid_euc_kr() {
    let error = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"\xb0a", false).unwrap_err();
    assert!(error.message().contains("EUC_KR"));
}

#[test]
fn no_error_stops_at_untranslatable_mic() {
    let result = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, b"ab\x91\xd2\xbb", true).unwrap();
    assert_eq!(result.bytes.as_slice(), b"ab");
    assert_eq!(result.converted, 2);
}

#[test]
fn reports_untranslatable_mic() {
    let error = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, b"\x91\xd2\xbb", false).unwrap_err();
    assert!(error.message().contains("MULE_INTERNAL"));
    assert!(error.message().contains("EUC_KR"));
}

#[test]
fn no_error_stops_at_invalid_mic() {
    let result = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, b"ab\x93\xb0", true).unwrap();
    assert_eq!(result.bytes.as_slice(), b"ab");
    assert_eq!(result.converted, 2);
}

#[test]
fn reports_invalid_mic() {
    let error = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, b"\x93\xb0", false).unwrap_err();
    assert!(error.message().contains("MULE_INTERNAL"));
}

#[test]
fn reports_embedded_nul() {
    let error = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"a\0b", false).unwrap_err();
    assert!(error.message().contains("EUC_KR"));
}

#[test]
fn validates_conversion_arguments() {
    let error = euc_kr_to_mic(PG_UTF8, PG_MULE_INTERNAL, b"abc", false).unwrap_err();
    assert!(error.message().contains("EUC_KR"));
}

#[test]
fn empty_input_yields_empty_output() {
    let result = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"", false).unwrap();
    assert!(result.bytes.is_empty());
    assert_eq!(result.converted, 0);
}

#[test]
fn ascii_passes_through_both_directions() {
    let mic = euc_kr_to_mic(PG_EUC_KR, PG_MULE_INTERNAL, b"hello", false).unwrap();
    assert_eq!(mic.bytes.as_slice(), b"hello");
    let back = mic_to_euc_kr(PG_MULE_INTERNAL, PG_EUC_KR, &mic.bytes, false).unwrap();
    assert_eq!(back.bytes.as_slice(), b"hello");
}
