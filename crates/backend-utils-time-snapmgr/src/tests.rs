//! Tests for the parts of snapmgr that don't require unported seam owners:
//! the wraparound-aware XID comparators, the text-parse helpers, and the
//! serialize/restore byte round-trip.

use super::*;

#[test]
fn xid_precedes_wraparound() {
    // Both normal: modulo-2^31 comparison.
    assert!(TransactionIdPrecedes(3, 4));
    assert!(!TransactionIdPrecedes(4, 3));
    // A huge xid "precedes" a small one across the wrap boundary.
    assert!(TransactionIdPrecedes(0xFFFF_FFF0, 5));
    // Permanent (non-normal) ids use plain unsigned comparison.
    assert!(TransactionIdPrecedes(InvalidTransactionId, FirstNormalTransactionId));
}

#[test]
fn xid_follows_or_equals() {
    assert!(TransactionIdFollowsOrEquals(4, 4));
    assert!(TransactionIdFollowsOrEquals(4, 3));
    assert!(!TransactionIdFollowsOrEquals(3, 4));
}

#[test]
fn scan_leading_handles_sscanf_semantics() {
    assert_eq!(scan_leading_i32("42\n"), Some(42));
    assert_eq!(scan_leading_i32("  -7rest"), Some(-7));
    assert_eq!(scan_leading_i32("nope"), None);
    assert_eq!(scan_leading_u32("123/"), Some(123));
    assert_eq!(scan_leading_u32(""), None);
}

#[test]
fn parse_line_advances_and_validates() {
    let buf = "vxid:3/9\npid:42\n";
    let mut cur = ImportCursor {
        rest: buf,
        filename: "t",
    };
    assert_eq!(parse_line(&mut cur, "vxid:").unwrap(), "3/9");
    assert_eq!(parse_line(&mut cur, "pid:").unwrap(), "42");
    // A wrong prefix is rejected.
    let mut cur2 = ImportCursor {
        rest: "xmin:7\n",
        filename: "t",
    };
    assert!(parse_line(&mut cur2, "xmax:").is_err());
}

#[test]
fn serialize_restore_round_trip() {
    let mut sd = new_snapshot_data(SnapshotType::SNAPSHOT_MVCC);
    sd.xmin = 100;
    sd.xmax = 200;
    sd.xip = vec![101, 102, 103];
    sd.xcnt = 3;
    sd.subxip = vec![150, 151];
    sd.subxcnt = 2;
    sd.suboverflowed = false;
    sd.takenDuringRecovery = false;
    sd.curcid = 7;
    let handle = new_handle(sd);

    let bytes = SerializeSnapshot(&handle);
    assert_eq!(bytes.len(), EstimateSnapshotSpace(&handle));

    let restored = RestoreSnapshot(&bytes);
    let r = restored.borrow();
    assert_eq!(r.xmin, 100);
    assert_eq!(r.xmax, 200);
    assert_eq!(r.xip, vec![101, 102, 103]);
    assert_eq!(r.subxip, vec![150, 151]);
    assert_eq!(r.curcid, 7);
    assert!(r.copied);
}

#[test]
fn serialize_drops_overflowed_subxids() {
    let mut sd = new_snapshot_data(SnapshotType::SNAPSHOT_MVCC);
    sd.xmin = 10;
    sd.xmax = 20;
    sd.subxip = vec![15, 16];
    sd.subxcnt = 2;
    sd.suboverflowed = true; // and not during recovery
    let handle = new_handle(sd);

    let bytes = SerializeSnapshot(&handle);
    let restored = RestoreSnapshot(&bytes);
    assert_eq!(restored.borrow().subxcnt, 0);
}
