use super::*;

#[test]
fn big5_round_trips_to_euc_tw() {
    let euc = big5_to_euc_tw(PG_BIG5, PG_EUC_TW, b"\xa4\x40", false).unwrap();
    assert_eq!(&*euc.bytes, b"\xc4\xa1");

    let big5 = euc_tw_to_big5(PG_EUC_TW, PG_BIG5, &euc.bytes, false).unwrap();
    assert_eq!(&*big5.bytes, b"\xa4\x40");
}

#[test]
fn euc_tw_round_trips_through_mic() {
    let mic = euc_tw_to_mic(PG_EUC_TW, PG_MULE_INTERNAL, b"\xc4\xa1", false).unwrap();
    assert_eq!(&*mic.bytes, b"\x95\xc4\xa1");

    let euc = mic_to_euc_tw(PG_MULE_INTERNAL, PG_EUC_TW, &mic.bytes, false).unwrap();
    assert_eq!(&*euc.bytes, b"\xc4\xa1");
}

#[test]
fn big5_round_trips_through_mic() {
    let mic = big5_to_mic(PG_BIG5, PG_MULE_INTERNAL, b"\xa4\x40", false).unwrap();
    assert_eq!(&*mic.bytes, b"\x95\xc4\xa1");

    let big5 = mic_to_big5(PG_MULE_INTERNAL, PG_BIG5, &mic.bytes, false).unwrap();
    assert_eq!(&*big5.bytes, b"\xa4\x40");
}

#[test]
fn validates_declared_encodings() {
    let err = euc_tw_to_big5(types_wchar::encoding::PG_UTF8, PG_BIG5, b"a", false).unwrap_err();
    assert_eq!(
        err.message(),
        "expected source encoding \"EUC_TW\", but got \"UTF8\""
    );
}

#[test]
fn ascii_passes_through_big5_to_euc_tw() {
    let result = big5_to_euc_tw(PG_BIG5, PG_EUC_TW, b"ascii", false).unwrap();
    assert_eq!(&*result.bytes, b"ascii");
    assert_eq!(result.converted, 5);
}

#[test]
fn empty_input_yields_empty_output() {
    let result = euc_tw_to_big5(PG_EUC_TW, PG_BIG5, b"", false).unwrap();
    assert!(result.bytes.is_empty());
    assert_eq!(result.converted, 0);
}
