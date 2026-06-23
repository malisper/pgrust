//! Unit tests for the pure logic of `syncrep.c` (the parts that don't touch
//! the shmem queue / latch / lock substrate): the LSN math, the priority/quorum
//! comparators, the standby-priority name matching, and the wait-mode dispatch.

use super::*;

fn stby(priority: i32, idx: i32, write: XLogRecPtr, flush: XLogRecPtr, apply: XLogRecPtr) -> SyncRepStandbyData {
    SyncRepStandbyData {
        pid: 1000 + idx,
        walsnd_index: idx,
        is_me: false,
        sync_standby_priority: priority,
        write,
        flush,
        apply,
    }
}

#[test]
fn cmp_lsn_is_descending() {
    let mut v = alloc::vec![10u64, 30, 20];
    v.sort_by(cmp_lsn);
    assert_eq!(v, alloc::vec![30, 20, 10]);
}

#[test]
fn oldest_sync_rec_ptr_takes_minimum() {
    let s = [
        stby(1, 0, 100, 200, 50),
        stby(1, 1, 80, 250, 90),
        stby(1, 2, 120, 180, 40),
    ];
    let (mut w, mut f, mut a) = (0u64, 0u64, 0u64);
    SyncRepGetOldestSyncRecPtr(&mut w, &mut f, &mut a, &s);
    assert_eq!(w, 80);
    assert_eq!(f, 180);
    assert_eq!(a, 40);
}

#[test]
fn nth_latest_sync_rec_ptr_picks_nth_descending() {
    let s = [
        stby(1, 0, 100, 200, 50),
        stby(1, 1, 80, 250, 90),
        stby(1, 2, 120, 180, 40),
    ];
    // 2nd latest: write sorted desc [120,100,80] -> 100; flush [250,200,180] ->
    // 200; apply [90,50,40] -> 50.
    let (mut w, mut f, mut a) = (0u64, 0u64, 0u64);
    SyncRepGetNthLatestSyncRecPtr(&mut w, &mut f, &mut a, &s, 2);
    assert_eq!(w, 100);
    assert_eq!(f, 200);
    assert_eq!(a, 50);
}

#[test]
fn priority_comparator_orders_by_priority_then_index() {
    let lo = stby(1, 5, 0, 0, 0);
    let hi = stby(3, 2, 0, 0, 0);
    assert_eq!(standby_priority_comparator(&lo, &hi), core::cmp::Ordering::Less);

    let a = stby(2, 1, 0, 0, 0);
    let b = stby(2, 4, 0, 0, 0);
    // Equal priority -> tie-break by walsnd_index.
    assert_eq!(standby_priority_comparator(&a, &b), core::cmp::Ordering::Less);
    assert_eq!(standby_priority_comparator(&b, &a), core::cmp::Ordering::Greater);
}

#[test]
fn pg_strcasecmp_is_case_insensitive() {
    assert_eq!(pg_strcasecmp("Standby1", "standby1"), 0);
    assert_eq!(pg_strcasecmp("*", "*"), 0);
    assert!(pg_strcasecmp("a", "b") < 0);
    assert!(pg_strcasecmp("b", "a") > 0);
    assert!(pg_strcasecmp("abc", "ab") > 0);
}

#[test]
fn assign_synchronous_commit_maps_levels() {
    assign_synchronous_commit(SYNCHRONOUS_COMMIT_REMOTE_WRITE);
    assert_eq!(SYNC_REP_WAIT_MODE.with(Cell::get), SYNC_REP_WAIT_WRITE);
    assign_synchronous_commit(SYNCHRONOUS_COMMIT_REMOTE_FLUSH);
    assert_eq!(SYNC_REP_WAIT_MODE.with(Cell::get), SYNC_REP_WAIT_FLUSH);
    assign_synchronous_commit(SYNCHRONOUS_COMMIT_REMOTE_APPLY);
    assert_eq!(SYNC_REP_WAIT_MODE.with(Cell::get), SYNC_REP_WAIT_APPLY);
    assign_synchronous_commit(SYNCHRONOUS_COMMIT_LOCAL_FLUSH);
    assert_eq!(SYNC_REP_WAIT_MODE.with(Cell::get), SYNC_REP_NO_WAIT);
}
