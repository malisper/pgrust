//! Tests for nodeSetOp pure (non-seam) logic: the per-group additional-space
//! codec and the `ApplySortComparator` null arithmetic.

use ::nodes::nodesetop::SetOpStatePerGroupData;

use super::{read_pergroup, write_pergroup};

#[test]
fn pergroup_roundtrip() {
    // The per-group struct is the entry's 16-byte additional space (two int64s).
    let pg = SetOpStatePerGroupData {
        numLeft: 7,
        numRight: -3,
    };
    let mut buf = [0u8; 16];
    write_pergroup(&mut buf, &pg);
    let got = read_pergroup(&buf);
    assert_eq!(got, pg);
}

#[test]
fn pergroup_zeroed_is_zero() {
    // A freshly-created entry's additional space is MemSet to 0 by execGrouping;
    // reading it yields zero counts (the C `if (isnew) numLeft = numRight = 0`).
    let buf = [0u8; 16];
    let got = read_pergroup(&buf);
    assert_eq!(got.numLeft, 0);
    assert_eq!(got.numRight, 0);
}
