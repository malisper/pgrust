use super::*;
use ::types_storage::lock::LOCKTAG_ADVISORY;

fn fields(tag: &LOCKTAG) -> (u32, u32, u32, u16, u8, u8) {
    (
        tag.locktag_field1,
        tag.locktag_field2,
        tag.locktag_field3,
        tag.locktag_field4,
        tag.locktag_type,
        tag.locktag_lockmethodid,
    )
}

// C vectors: SET_LOCKTAG_INT64 splits key as (uint32)(key>>32) / (uint32)key,
// field4 = 1; SET_LOCKTAG_INT32 stores keys verbatim, field4 = 2 (lockfuncs.c
// lines 613-620); dbid 0 here (MyDatabaseId unset in unit tests).
#[test]
fn locktag_int64_split_matches_c() {
    for (key, hi, lo) in [
        (0i64, 0u32, 0u32),
        (1, 0, 1),
        (-1, 0xFFFF_FFFF, 0xFFFF_FFFF),
        (0x1234_5678_9ABC_DEF0, 0x1234_5678, 0x9ABC_DEF0),
        (i64::MIN, 0x8000_0000, 0),
        (i64::MAX, 0x7FFF_FFFF, 0xFFFF_FFFF),
        (-2_147_483_648, 0xFFFF_FFFF, 0x8000_0000),
        (4_294_967_296, 1, 0),
    ] {
        let tag = set_locktag_int64(key);
        assert_eq!(fields(&tag), (0, hi, lo, 1, LOCKTAG_ADVISORY, USER_LOCKMETHOD));
    }
}

#[test]
fn locktag_int32_pair_matches_c() {
    for (k1, k2, f2, f3) in [
        (0i32, 0i32, 0u32, 0u32),
        (1, 2, 1, 2),
        (-1, -2, 0xFFFF_FFFF, 0xFFFF_FFFE),
        (i32::MIN, i32::MAX, 0x8000_0000, 0x7FFF_FFFF),
    ] {
        let tag = set_locktag_int32(k1, k2);
        assert_eq!(fields(&tag), (0, f2, f3, 2, LOCKTAG_ADVISORY, USER_LOCKMETHOD));
    }
}

#[test]
fn locktag_type_names_match_c() {
    assert_eq!(LOCK_TAG_TYPE_NAMES[LOCKTAG_RELATION as usize], "relation");
    assert_eq!(LOCK_TAG_TYPE_NAMES[LOCKTAG_ADVISORY as usize], "advisory");
    assert_eq!(LOCK_TAG_TYPE_NAMES[LOCKTAG_LAST_TYPE as usize], "applytransaction");
    assert_eq!(PREDICATE_LOCK_TAG_TYPE_NAMES, ["relation", "page", "tuple"]);
}

#[test]
fn builtin_table_shape() {
    assert_eq!(LOCKFUNCS_BUILTINS.len(), 24);
    let srf: Vec<_> = LOCKFUNCS_BUILTINS.iter().filter(|b| b.retset).collect();
    assert_eq!(srf.len(), 1);
    assert_eq!(srf[0].foid, 1371);
    for b in LOCKFUNCS_BUILTINS {
        assert!(b.strict);
    }
}
