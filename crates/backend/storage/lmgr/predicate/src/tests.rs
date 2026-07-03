use super::*;

#[test]
fn invalid_sxact_arms_and_installs() {
    init_seams();
    predicate_seams::pre_commit_check_for_serialization_failure::call().unwrap();
    predicate_seams::register_predicate_locking_xid::call(100).unwrap();
    predicate_seams::at_prepare_predicate_locks::call().unwrap();
    predicate_seams::post_prepare_predicate_locks::call(100).unwrap();
    assert!(predicate_seams::predicate_lock_relation::is_installed());
    assert!(predicate_seams::predicate_lock_page::is_installed());
    assert!(predicate_seams::predicate_lock_tid::is_installed());
    assert!(predicate_seams::check_for_serializable_conflict_out_needed::is_installed());
    assert!(predicate_seams::check_for_serializable_conflict_out::is_installed());
    assert!(predicate_seams::check_for_serializable_conflict_in::is_installed());
    // CheckPointPredicate does unconditional SLRU work in C; stays loud.
    assert!(!predicate_seams::check_point_predicate::is_installed());
}

#[test]
fn set_sxact_bit_is_loud() {
    MY_SERIALIZABLE_XACT_SET.with(|c| c.set(true));
    let r = std::panic::catch_unwind(|| my_sxact_is_invalid());
    MY_SERIALIZABLE_XACT_SET.with(|c| c.set(false));
    assert!(r.is_err());
}
