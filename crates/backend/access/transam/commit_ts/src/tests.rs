use super::*;

#[test]
fn entry_size_and_per_page() {
    // SizeOfCommitTimestampEntry = sizeof(TimestampTz) + sizeof(RepOriginId) = 10.
    assert_eq!(SizeOfCommitTimestampEntry, 10);
    // COMMIT_TS_XACTS_PER_PAGE = BLCKSZ / 10.
    assert_eq!(COMMIT_TS_XACTS_PER_PAGE, (BLCKSZ / 10) as i64);
}

#[test]
fn page_entry_arithmetic() {
    let per_page = COMMIT_TS_XACTS_PER_PAGE as TransactionId;
    assert_eq!(TransactionIdToCTsPage(0), 0);
    assert_eq!(TransactionIdToCTsPage(per_page - 1), 0);
    assert_eq!(TransactionIdToCTsPage(per_page), 1);
    assert_eq!(TransactionIdToCTsEntry(0), 0);
    assert_eq!(TransactionIdToCTsEntry(per_page), 0);
    assert_eq!(TransactionIdToCTsEntry(per_page + 5), 5);
}

#[test]
fn entry_codec_roundtrip() {
    let mut page = vec![0u8; BLCKSZ];
    let entry = CommitTimestampEntry {
        time: 0x0102_0304_0506_0708,
        nodeid: 0xABCD,
    };
    write_entry(&mut page, 3, entry);
    assert_eq!(read_entry(&page, 3), entry);
    // Adjacent entries are untouched.
    assert_eq!(read_entry(&page, 2), CommitTimestampEntry::default());
    assert_eq!(read_entry(&page, 4), CommitTimestampEntry::default());
}

#[test]
fn page_precedes_self_is_false() {
    // A page never precedes itself.
    assert!(!CommitTsPagePrecedes(5, 5));
}

#[test]
fn shared_default_is_inactive_nobegin() {
    let s = CommitTimestampShared::default();
    assert!(!s.commitTsActive);
    assert_eq!(s.xidLastCommit, InvalidTransactionId);
    assert_eq!(s.dataLastCommit.time, DT_NOBEGIN);
    assert_eq!(s.dataLastCommit.nodeid, InvalidRepOriginId);
}

#[test]
fn rm_and_op_constants() {
    assert_eq!(RM_COMMIT_TS_ID, 18);
    assert_eq!(COMMIT_TS_ZEROPAGE, 0x00);
    assert_eq!(COMMIT_TS_TRUNCATE, 0x10);
}
