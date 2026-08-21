//! Golden pins over the frozen vocabulary: the tombstone payload codec
//! (round-trip + exact bit values), the delta rowid space, and the
//! refusal teeth (both born-RED directions: a delta-tagged rowid refuses
//! at encode, at decode, and at bitmap build).

use pgrc2_format::rowid::pack_rowid;

use crate::bitmap::DeletionIndexBuilder;
use crate::rowid::{
    check_part_no_for_pair, is_delta_rowid, pack_delta_rowid, unpack_delta_rowid,
    DELTA_ROWID_TAG, MAX_PAIR_PART_NO,
};
use crate::tombstone::{rowid_of_tombstone, tombstone_payload, TOMBSTONE_NATTS};
use crate::DeltaError;

#[test]
fn tombstone_schema_pin() {
    // The frozen spec §15 shape: ONE int8 attribute. A drift in either
    // this crate or the format crate fails here.
    assert_eq!(TOMBSTONE_NATTS, 1);
    assert_eq!(pgrc2_format::dml::TOMBSTONE_NATTS, 1);
}

#[test]
fn tombstone_payload_golden() {
    // Bit-preserving int8 carrier: exact values pinned, including the
    // i64-negative half of the space (part_no ≥ 2^31 encodes to a
    // negative payload — legal AS PAYLOAD; the pair-scan bound is a
    // separate, scan-side check).
    let cases: &[(u32, u32, u32, i64)] = &[
        (0, 0, 0, 0),
        (0, 0, 1, 1),
        (0, 1, 0, 1 << 13),
        (1, 0, 0, 1 << 32),
        (7, 3, 42, (7i64 << 32) | (3 << 13) | 42),
    ];
    for &(part, g, row, want) in cases {
        let rowid = pack_rowid(part, g, row);
        let payload = tombstone_payload(rowid).expect("sealed rowid encodes");
        assert_eq!(payload, want, "payload bits for ({part},{g},{row})");
        assert_eq!(rowid_of_tombstone(payload).expect("round-trip"), rowid);
    }
}

#[test]
fn delta_rowid_space_golden() {
    assert_eq!(DELTA_ROWID_TAG, 0x8000_0000_0000_0000);
    let r = pack_delta_rowid(3, 7);
    assert_eq!(r, DELTA_ROWID_TAG | (3 << 16) | 7);
    assert!(is_delta_rowid(r));
    assert_eq!(unpack_delta_rowid(r), Some((3, 7)));
    // Sealed rowids of admissible parts never carry the tag.
    let sealed = pack_rowid(MAX_PAIR_PART_NO, 5, 5);
    assert!(!is_delta_rowid(sealed));
    assert_eq!(unpack_delta_rowid(sealed), None);
    // Packing is order-preserving in (block, off) after the tag.
    assert!(pack_delta_rowid(1, 1) < pack_delta_rowid(1, 2));
    assert!(pack_delta_rowid(1, 65535) < pack_delta_rowid(2, 1));
}

#[test]
fn delta_tagged_tombstone_refuses_at_encode() {
    let r = pack_delta_rowid(0, 1);
    match tombstone_payload(r) {
        Err(DeltaError::DeltaTaggedTombstone { rowid }) => assert_eq!(rowid, r),
        other => panic!("expected DeltaTaggedTombstone, got {other:?}"),
    }
}

#[test]
fn delta_tagged_tombstone_refuses_at_decode() {
    let payload = (DELTA_ROWID_TAG | 42) as i64;
    match rowid_of_tombstone(payload) {
        Err(DeltaError::DeltaTaggedTombstone { .. }) => {}
        other => panic!("expected DeltaTaggedTombstone, got {other:?}"),
    }
}

#[test]
fn delta_tagged_tombstone_refuses_at_bitmap_build() {
    let mut b = DeletionIndexBuilder::new();
    match b.add(pack_delta_rowid(9, 9)) {
        Err(DeltaError::DeltaTaggedTombstone { .. }) => {}
        other => panic!("expected DeltaTaggedTombstone, got {other:?}"),
    }
}

// -- the silent-lossy-serialization teeth (coordinator rider, 2026-08-09:
// -- any length/offset cast guarded only by debug_assert is this class) --

#[test]
fn tooth_lossy_cast_out_of_granule_row_fails_loud() {
    // A window row ≥ the granule bound must CRASH, not wrap mod 65,536
    // into a false deletion match (the memo.rs >64KB exemplar's shape:
    // release-silent `as u16`). Both lookup faces carry the release-
    // effective guard.
    let mut b = DeletionIndexBuilder::new();
    b.add(pack_rowid(0, 0, 0)).expect("add");
    let index = b.finish();
    let part_owned = index.part(0).expect("part").clone();

    let r = std::panic::catch_unwind(|| {
        // 65,536 would wrap to row 0 (a real deletion) under a bare cast.
        part_owned.is_deleted(0, 65_536)
    });
    assert!(r.is_err(), "out-of-granule row must fail loud, not wrap silently");

    let part_owned = index.part(0).expect("part").clone();
    let r = std::panic::catch_unwind(move || {
        let mut positions = vec![65_536u32];
        part_owned.filter_window(0, 0, &mut positions);
        positions
    });
    assert!(r.is_err(), "filter_window must fail loud on an out-of-granule row");
}

#[test]
fn tooth_lossy_cast_malformed_delta_rowid_refuses() {
    // A tagged rowid with block bits beyond u32 (bits 62..48) is corrupt;
    // truncating `as u32` would alias it onto a VALID tid. Unpack must
    // answer None.
    let corrupt = DELTA_ROWID_TAG | (1u64 << 55) | (3 << 16) | 7;
    assert!(is_delta_rowid(corrupt));
    assert_eq!(
        unpack_delta_rowid(corrupt),
        None,
        "malformed delta rowid must refuse, not truncate to (3, 7)"
    );
    // The well-formed neighbor still unpacks.
    assert_eq!(unpack_delta_rowid(pack_delta_rowid(3, 7)), Some((3, 7)));
}

#[test]
fn pair_part_bound_refuses_beyond() {
    assert!(check_part_no_for_pair(MAX_PAIR_PART_NO).is_ok());
    match check_part_no_for_pair(MAX_PAIR_PART_NO + 1) {
        Err(DeltaError::PartNoBeyondPairBound { part_no }) => {
            assert_eq!(part_no, MAX_PAIR_PART_NO + 1)
        }
        other => panic!("expected PartNoBeyondPairBound, got {other:?}"),
    }
    // A bound-violating part_no PACKED into a rowid IS the delta tag
    // (part bit 31 = rowid bit 63 — the aliasing the bound forbids), so
    // the builder's tag refusal covers it; there is no untagged spelling
    // of an out-of-bound part. (CI-caught: a separate builder-side
    // part check is structurally unreachable.) The REAL part-number site
    // is the pair-scan builder over manifest part_nos —
    // `pgrc_pair_tests::pair_part_bound_tooth`.
    let mut b = DeletionIndexBuilder::new();
    let bad = pack_rowid(MAX_PAIR_PART_NO + 1, 0, 0);
    assert!(is_delta_rowid(bad), "part bit 31 must alias the delta tag");
    assert!(matches!(
        b.add(bad),
        Err(DeltaError::DeltaTaggedTombstone { .. })
    ));
}
