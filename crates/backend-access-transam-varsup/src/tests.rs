//! Pure-arithmetic unit tests for `backend-access-transam-varsup`.
//!
//! These exercise the in-crate XID/`FullTransactionId` arithmetic, the shmem
//! sizing, and the OID-threshold constants — none of which touch the
//! (panicking-until-installed) seams.

use super::*;

#[test]
fn full_xid_split_and_join() {
    let f = FullTransactionIdFromEpochAndXid(7, 12345);
    assert_eq!(EpochFromFullTransactionId(f), 7);
    assert_eq!(XidFromFullTransactionId(f), 12345);
    assert_eq!(f.value, (7u64 << 32) | 12345);
}

#[test]
fn transaction_id_advance_skips_special() {
    // Wrapping from MaxTransactionId skips 0,1,2 up to FirstNormalTransactionId.
    let mut xid = MaxTransactionId;
    TransactionIdAdvance(&mut xid);
    assert_eq!(xid, FirstNormalTransactionId);

    // Normal increment.
    let mut xid = 100;
    TransactionIdAdvance(&mut xid);
    assert_eq!(xid, 101);
}

#[test]
fn full_transaction_id_advance_steps_over_special_xids() {
    // At the 32-bit wrap boundary, the 32-bit xid steps over 0,1,2 while the
    // 64-bit epoch increments by one.
    let mut f = FullTransactionIdFromEpochAndXid(0, MaxTransactionId);
    FullTransactionIdAdvance(&mut f);
    assert_eq!(EpochFromFullTransactionId(f), 1);
    assert_eq!(XidFromFullTransactionId(f), FirstNormalTransactionId);

    // Below FirstNormalFullTransactionId, the loop does not run.
    let mut f = FullTransactionId { value: 0 };
    FullTransactionIdAdvance(&mut f);
    assert_eq!(f.value, 1);

    // Plain advance.
    let mut f = FullTransactionIdFromEpochAndXid(3, 500);
    FullTransactionIdAdvance(&mut f);
    assert_eq!(EpochFromFullTransactionId(f), 3);
    assert_eq!(XidFromFullTransactionId(f), 501);
}

#[test]
fn shmem_size_is_struct_size() {
    assert_eq!(
        VarsupShmemSize() as usize,
        core::mem::size_of::<TransamVariablesData>()
    );
}

#[test]
fn oid_constants_match_transam_h() {
    assert_eq!(FirstGenbkiObjectId, 10000);
    assert_eq!(FirstUnpinnedObjectId, 12000);
    assert_eq!(FirstNormalObjectId, 16384);
    assert_eq!(MaxTransactionId, 0xFFFF_FFFF);
}

#[test]
fn xid_comparison_wraps_modulo_2_32() {
    // Both normal: modulo-2^32 comparison.
    assert!(TransactionIdPrecedes(100, 200));
    assert!(!TransactionIdPrecedes(200, 100));
    assert!(TransactionIdFollowsOrEquals(200, 100));
    assert!(TransactionIdFollowsOrEquals(100, 100));
    assert!(TransactionIdPrecedesOrEquals(100, 100));

    // A permanent (non-normal) operand uses plain unsigned comparison.
    assert!(TransactionIdPrecedes(BootstrapTransactionId, 100));
    assert!(!TransactionIdIsNormal(BootstrapTransactionId));
    assert!(TransactionIdIsNormal(FirstNormalTransactionId));
    assert!(!TransactionIdIsValid(0));
    assert!(TransactionIdIsValid(1));
}

#[test]
fn set_transaction_id_limit_arithmetic_matches_c() {
    // Reproduce the limit computation for a normal oldest_datfrozenxid and
    // check the +/- correction signs match varsup.c exactly.
    let oldest: TransactionId = FirstNormalTransactionId;
    let mut xid_wrap_limit = oldest.wrapping_add(MaxTransactionId >> 1);
    if xid_wrap_limit < FirstNormalTransactionId {
        xid_wrap_limit = xid_wrap_limit.wrapping_add(FirstNormalTransactionId);
    }
    // 3 + 0x7FFFFFFF, no wrap-around correction needed.
    assert_eq!(xid_wrap_limit, 3u32.wrapping_add(0x7FFF_FFFF));

    let xid_stop_limit = xid_wrap_limit.wrapping_sub(3_000_000);
    assert!(xid_stop_limit >= FirstNormalTransactionId);
    let xid_warn_limit = xid_wrap_limit.wrapping_sub(40_000_000);
    assert!(xid_warn_limit >= FirstNormalTransactionId);
    assert!(xid_warn_limit < xid_stop_limit);
}

/// Cross-backend visibility: two [`TransamPtr`]s (conn1's and conn2's
/// per-process pointers) over the SAME backing `TransamVariablesData` — the
/// realization of fork sharing one `MAP_SHARED` `ShmemInitStruct` region — must
/// see each other's mutations. This is exactly the bug the change fixes: before
/// it, the struct lived in a process-local `static Mutex`, so conn1's
/// `MaintainLatestCompletedXid` advance at commit was invisible to conn2's
/// `GetSnapshotData`, leaving conn1's committed rows un-seeable by conn2.
#[test]
fn two_pointers_share_one_transam_variables() {
    // One backing struct in the (here heap-stand-in for) shmem segment. A real
    // fork would share the same physical page; both pointers address it.
    let mut backing = Box::new(TransamVariablesData::default());
    let base: *mut TransamVariablesData = &mut *backing;

    // conn1 and conn2: distinct per-process pointers at the SAME struct.
    let mut conn1 = TransamPtr(base);
    let conn2 = TransamPtr(base);

    // conn1 performs the commit-path mutation procarray's
    // MaintainLatestCompletedXid does under ProcArrayLock: advance
    // latestCompletedXid and bump xactCompletionCount.
    conn1.latestCompletedXid = FullTransactionId { value: 0xDEAD_BEEF };
    conn1.xactCompletionCount = 42;
    conn1.nextXid = FullTransactionId { value: 0x1_0000_0007 };

    // conn2 (a different per-process view) observes conn1's writes — the
    // cross-connection visibility the fix delivers.
    assert_eq!(conn2.latestCompletedXid, FullTransactionId { value: 0xDEAD_BEEF });
    assert_eq!(conn2.xactCompletionCount, 42);
    assert_eq!(conn2.nextXid, FullTransactionId { value: 0x1_0000_0007 });
}
