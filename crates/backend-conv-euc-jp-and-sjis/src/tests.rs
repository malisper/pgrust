use super::*;
use types_wchar::encoding::PG_UTF8;

#[test]
fn hiragana_round_trips_between_euc_jp_and_sjis() {
    let sjis = euc_jp_to_sjis(PG_EUC_JP, PG_SJIS, b"\xa4\xa2", false).unwrap();
    assert_eq!(&sjis.bytes[..], b"\x82\xa0");

    let euc = sjis_to_euc_jp(PG_SJIS, PG_EUC_JP, &sjis.bytes, false).unwrap();
    assert_eq!(&euc.bytes[..], b"\xa4\xa2");
}

#[test]
fn kana_round_trips_through_mic() {
    let mic = euc_jp_to_mic(PG_EUC_JP, PG_MULE_INTERNAL, b"\x8e\xa6", false).unwrap();
    assert_eq!(&mic.bytes[..], b"\x89\xa6");

    let euc = mic_to_euc_jp(PG_MULE_INTERNAL, PG_EUC_JP, &mic.bytes, false).unwrap();
    assert_eq!(&euc.bytes[..], b"\x8e\xa6");

    let mic2 = sjis_to_mic(PG_SJIS, PG_MULE_INTERNAL, b"\xa6", false).unwrap();
    assert_eq!(&mic2.bytes[..], b"\x89\xa6");
    let sjis = mic_to_sjis(PG_MULE_INTERNAL, PG_SJIS, &mic2.bytes, false).unwrap();
    assert_eq!(&sjis.bytes[..], b"\xa6");
}

#[test]
fn ascii_passes_through_all_pairs() {
    let r = euc_jp_to_sjis(PG_EUC_JP, PG_SJIS, b"ascii", false).unwrap();
    assert_eq!(&r.bytes[..], b"ascii");
    assert_eq!(r.converted, 5);

    let r = euc_jp_to_mic(PG_EUC_JP, PG_MULE_INTERNAL, b"ascii", false).unwrap();
    assert_eq!(&r.bytes[..], b"ascii");
}

#[test]
fn validates_declared_encodings() {
    let err = euc_jp_to_sjis(PG_UTF8, PG_SJIS, b"a", false).unwrap_err();
    assert_eq!(
        err.message(),
        "expected source encoding \"EUC_JP\", but got \"UTF8\""
    );
}

#[test]
fn empty_input_yields_empty_output() {
    let r = euc_jp_to_sjis(PG_EUC_JP, PG_SJIS, b"", false).unwrap();
    assert!(r.bytes.is_empty());
    assert_eq!(r.converted, 0);
}

#[test]
fn no_error_stops_on_truncated_sjis() {
    // A lone SJIS lead byte with no trail byte: no_error stops mid-stream.
    let r = sjis_to_euc_jp(PG_SJIS, PG_EUC_JP, b"a\x82", true).unwrap();
    assert_eq!(&r.bytes[..], b"a");
    assert_eq!(r.converted, 1);
}

#[test]
fn reports_invalid_sjis() {
    // A truncated SJIS sequence with no_error = false reports the invalid
    // byte sequence.
    let err = sjis_to_euc_jp(PG_SJIS, PG_EUC_JP, b"\x82", false).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid byte sequence for encoding \"SJIS\": 0x82"
    );
}
