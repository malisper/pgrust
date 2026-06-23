//! Tests for the four 2PC resource-manager callback tables: the
//! `NULL`-vs-callback slot pattern of each table against `twophase_rmgr.c`,
//! and that a non-`NULL` cell dispatches to the seam installed for it.

use super::*;

fn present(table: &[Option<TwoPhaseCallback>; NUM_TWOPHASE_RM]) -> [bool; NUM_TWOPHASE_RM] {
    [
        table[0].is_some(),
        table[1].is_some(),
        table[2].is_some(),
        table[3].is_some(),
        table[4].is_some(),
    ]
}

#[test]
fn rmgr_id_constants_match_c() {
    assert_eq!(TWOPHASE_RM_END_ID, 0);
    assert_eq!(TWOPHASE_RM_LOCK_ID, 1);
    assert_eq!(TWOPHASE_RM_PGSTAT_ID, 2);
    assert_eq!(TWOPHASE_RM_MULTIXACT_ID, 3);
    assert_eq!(TWOPHASE_RM_PREDICATELOCK_ID, 4);
    assert_eq!(TWOPHASE_RM_MAX_ID, TWOPHASE_RM_PREDICATELOCK_ID);
    assert_eq!(NUM_TWOPHASE_RM, 5);
}

#[test]
fn recover_table_null_pattern() {
    // { NULL, lock, NULL, multixact, predicatelock }
    assert_eq!(
        present(&twophase_recover_callbacks),
        [false, true, false, true, true]
    );
}

#[test]
fn postcommit_table_null_pattern() {
    // { NULL, lock, pgstat, multixact, NULL }
    assert_eq!(
        present(&twophase_postcommit_callbacks),
        [false, true, true, true, false]
    );
}

#[test]
fn postabort_table_null_pattern() {
    // { NULL, lock, pgstat, multixact, NULL }
    assert_eq!(
        present(&twophase_postabort_callbacks),
        [false, true, true, true, false]
    );
}

#[test]
fn standby_recover_table_null_pattern() {
    // { NULL, lock, NULL, NULL, NULL }
    assert_eq!(
        present(&twophase_standby_recover_callbacks),
        [false, true, false, false, false]
    );
}

#[test]
fn end_slot_is_always_null() {
    assert!(twophase_recover_callbacks[TWOPHASE_RM_END_ID as usize].is_none());
    assert!(twophase_postcommit_callbacks[TWOPHASE_RM_END_ID as usize].is_none());
    assert!(twophase_postabort_callbacks[TWOPHASE_RM_END_ID as usize].is_none());
    assert!(twophase_standby_recover_callbacks[TWOPHASE_RM_END_ID as usize].is_none());
}

#[test]
fn non_null_slot_dispatches_to_installed_seam() {
    use std::sync::atomic::{AtomicU32, Ordering};
    static HITS: AtomicU32 = AtomicU32::new(0);

    fn counting_cb(_xid: TransactionId, _info: u16, _recdata: &[u8]) -> PgResult<()> {
        HITS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    lock::lock_twophase_postcommit::set(counting_cb);

    let cb = twophase_postcommit_callbacks[TWOPHASE_RM_LOCK_ID as usize]
        .expect("lock postcommit slot present");
    cb(42, 7, &[1, 2, 3]).unwrap();
    assert_eq!(HITS.load(Ordering::SeqCst), 1);
}

#[test]
fn uninstalled_slot_panics_loudly() {
    let cb = twophase_recover_callbacks[TWOPHASE_RM_PREDICATELOCK_ID as usize]
        .expect("predicatelock recover slot present");
    let result = std::panic::catch_unwind(|| cb(1, 0, &[]));
    assert!(result.is_err());
}
