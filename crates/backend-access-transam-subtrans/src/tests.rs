//! Unit tests for subtrans's own pure arithmetic (no SLRU shmem harness).

use super::*;

#[test]
fn page_and_entry_partition_xids() {
    // Entry 0 of page 0, then the page boundary.
    assert_eq!(TransactionIdToPage(0), 0);
    assert_eq!(TransactionIdToEntry(0), 0);
    assert_eq!(TransactionIdToEntry(SUBTRANS_XACTS_PER_PAGE - 1), SUBTRANS_XACTS_PER_PAGE - 1);
    assert_eq!(TransactionIdToPage(SUBTRANS_XACTS_PER_PAGE), 1);
    assert_eq!(TransactionIdToEntry(SUBTRANS_XACTS_PER_PAGE), 0);
}

#[test]
fn retreat_skips_special_xids() {
    // Retreating FirstNormalTransactionId wraps back to MaxTransactionId,
    // skipping the special (< FirstNormal) values.
    let mut xid = FirstNormalTransactionId;
    TransactionIdRetreat(&mut xid);
    assert_eq!(xid, MaxTransactionId);

    let mut xid2 = FirstNormalTransactionId + 1;
    TransactionIdRetreat(&mut xid2);
    assert_eq!(xid2, FirstNormalTransactionId);
}

#[test]
fn page_buffer_roundtrip() {
    let mut buf = vec![0u8; BLCKSZ];
    write_entry(&mut buf, 5, 0xDEAD_BEEF);
    assert_eq!(read_entry(&buf, 5), 0xDEAD_BEEF);
    // Adjacent entries untouched.
    assert_eq!(read_entry(&buf, 4), InvalidTransactionId);
    assert_eq!(read_entry(&buf, 6), InvalidTransactionId);
}

#[test]
fn page_precedes_is_antisymmetric_for_distant_pages() {
    // Two pages far apart in the same epoch: the lower one precedes.
    assert!(SubTransPagePrecedes(0, 100));
    assert!(!SubTransPagePrecedes(100, 0));
    // A page does not precede itself.
    assert!(!SubTransPagePrecedes(42, 42));
}
