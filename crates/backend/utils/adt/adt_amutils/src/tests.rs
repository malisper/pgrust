use super::*;

#[test]
fn prop_names_case_insensitive() {
    assert!(matches!(lookup_prop_name(b"ASC"), Prop::Asc));
    assert!(matches!(lookup_prop_name(b"distance_orderable"), Prop::DistanceOrderable));
    assert!(matches!(lookup_prop_name(b"Can_Include"), Prop::CanInclude));
    assert!(matches!(lookup_prop_name(b"bogus"), Prop::Unknown));
    assert!(matches!(lookup_prop_name(b"asc2"), Prop::Unknown));
}

#[test]
fn am_flag_rows_match_c_handlers() {
    let bt = am_flags(BTREE_AM_OID).unwrap();
    assert!(bt.amcanorder && bt.amcanunique && bt.amsearcharray && bt.has_ambuildphasename);
    let hash = am_flags(HASH_AM_OID).unwrap();
    assert!(hash.amcanbackward && !hash.amcanorder && !hash.amcaninclude);
    let gin = am_flags(GIN_AM_OID).unwrap();
    assert!(!gin.has_amgettuple && gin.has_ambuildphasename && gin.amcanmulticol);
    let brin = am_flags(BRIN_AM_OID).unwrap();
    assert!(!brin.has_amgettuple && brin.amsearchnulls && !brin.amclusterable);
    assert!(am_flags(42).is_none());
}

#[test]
fn phasenames_match_c() {
    assert_eq!(bt_phasename(1), Some("initializing"));
    assert_eq!(bt_phasename(5), Some("loading tuples in tree"));
    assert_eq!(bt_phasename(6), None);
    assert_eq!(gin_phasename(3), Some("sorting tuples (workers)"));
    assert_eq!(gin_phasename(6), Some("merging tuples"));
    assert_eq!(gin_phasename(7), None);
}

#[test]
fn phasenum_truncates_like_pg_getarg_int32() {
    let phasenum = 0x1_0000_0001i64 as i32 as i64;
    assert_eq!(phasenum, 1);
}
