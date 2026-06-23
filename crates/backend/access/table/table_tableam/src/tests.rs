use super::*;

#[test]
fn nextpower2_matches_c() {
    assert_eq!(pg_nextpower2_32(1), 1);
    assert_eq!(pg_nextpower2_32(2), 2);
    assert_eq!(pg_nextpower2_32(3), 4);
    assert_eq!(pg_nextpower2_32(2048), 2048);
    assert_eq!(pg_nextpower2_32(2049), 4096);
    assert_eq!(pg_nextpower2_32(0x4000_0001), 0x8000_0000);
}

#[test]
fn add_size_overflow_errors() {
    assert_eq!(add_size(1, 2).unwrap(), 3);
    assert!(add_size(usize::MAX, 1).is_err());
}

#[test]
fn guc_defaults_match_c() {
    assert_eq!(default_table_access_method(), "heap");
    assert!(synchronize_seqscans());
    set_synchronize_seqscans(false);
    assert!(!synchronize_seqscans());
    set_synchronize_seqscans(true);
}
