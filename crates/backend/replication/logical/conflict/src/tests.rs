//! Unit tests for the seam-free logic of `conflict.c`: the `ConflictType`
//! discriminant ordering, the `ConflictTypeNames[]` table, and the
//! `errcode_apply_conflict` SQLSTATE mapping.

use super::*;

/// `ConflictType` discriminants are the declaration order in the C enum and
/// are used in statistics collection, so they must match
/// `replication/conflict.h`.
#[test]
fn conflict_type_discriminants_match_c_enum_order() {
    assert_eq!(CT_INSERT_EXISTS as i32, 0);
    assert_eq!(CT_UPDATE_ORIGIN_DIFFERS as i32, 1);
    assert_eq!(CT_UPDATE_EXISTS as i32, 2);
    assert_eq!(CT_UPDATE_MISSING as i32, 3);
    assert_eq!(CT_DELETE_ORIGIN_DIFFERS as i32, 4);
    assert_eq!(CT_DELETE_MISSING as i32, 5);
    assert_eq!(CT_MULTIPLE_UNIQUE_CONFLICTS as i32, 6);
}

/// `CONFLICT_NUM_TYPES == CT_MULTIPLE_UNIQUE_CONFLICTS + 1`.
#[test]
fn conflict_num_types_is_count() {
    assert_eq!(CONFLICT_NUM_TYPES, 7);
    assert_eq!(CONFLICT_NUM_TYPES, CONFLICT_TYPE_NAMES.len());
}

/// The `ConflictTypeNames[]` designated-initializer table pairs each
/// discriminant with the exact C string.
#[test]
fn conflict_type_names_table_matches_c() {
    assert_eq!(CONFLICT_TYPE_NAMES[CT_INSERT_EXISTS as usize], "insert_exists");
    assert_eq!(
        CONFLICT_TYPE_NAMES[CT_UPDATE_ORIGIN_DIFFERS as usize],
        "update_origin_differs"
    );
    assert_eq!(CONFLICT_TYPE_NAMES[CT_UPDATE_EXISTS as usize], "update_exists");
    assert_eq!(CONFLICT_TYPE_NAMES[CT_UPDATE_MISSING as usize], "update_missing");
    assert_eq!(
        CONFLICT_TYPE_NAMES[CT_DELETE_ORIGIN_DIFFERS as usize],
        "delete_origin_differs"
    );
    assert_eq!(CONFLICT_TYPE_NAMES[CT_DELETE_MISSING as usize], "delete_missing");
    assert_eq!(
        CONFLICT_TYPE_NAMES[CT_MULTIPLE_UNIQUE_CONFLICTS as usize],
        "multiple_unique_conflicts"
    );
    // Every slot is filled (no "" gap left by the positional builder).
    for name in CONFLICT_TYPE_NAMES.iter() {
        assert!(!name.is_empty());
    }
}

/// `errcode_apply_conflict`: the unique-violation conflict types map to
/// `ERRCODE_UNIQUE_VIOLATION`; the rest map to
/// `ERRCODE_T_R_SERIALIZATION_FAILURE`.
#[test]
fn errcode_apply_conflict_maps_unique_violations() {
    assert_eq!(errcode_apply_conflict(CT_INSERT_EXISTS), ERRCODE_UNIQUE_VIOLATION);
    assert_eq!(errcode_apply_conflict(CT_UPDATE_EXISTS), ERRCODE_UNIQUE_VIOLATION);
    assert_eq!(
        errcode_apply_conflict(CT_MULTIPLE_UNIQUE_CONFLICTS),
        ERRCODE_UNIQUE_VIOLATION
    );
}

#[test]
fn errcode_apply_conflict_maps_serialization_failures() {
    assert_eq!(
        errcode_apply_conflict(CT_UPDATE_ORIGIN_DIFFERS),
        ERRCODE_T_R_SERIALIZATION_FAILURE
    );
    assert_eq!(
        errcode_apply_conflict(CT_UPDATE_MISSING),
        ERRCODE_T_R_SERIALIZATION_FAILURE
    );
    assert_eq!(
        errcode_apply_conflict(CT_DELETE_ORIGIN_DIFFERS),
        ERRCODE_T_R_SERIALIZATION_FAILURE
    );
    assert_eq!(
        errcode_apply_conflict(CT_DELETE_MISSING),
        ERRCODE_T_R_SERIALIZATION_FAILURE
    );
}

/// The two SQLSTATE buckets partition all seven conflict types (every type
/// maps to exactly one of the two codes, none panics or falls through).
#[test]
fn errcode_apply_conflict_total_over_all_types() {
    let all = [
        CT_INSERT_EXISTS,
        CT_UPDATE_ORIGIN_DIFFERS,
        CT_UPDATE_EXISTS,
        CT_UPDATE_MISSING,
        CT_DELETE_ORIGIN_DIFFERS,
        CT_DELETE_MISSING,
        CT_MULTIPLE_UNIQUE_CONFLICTS,
    ];
    for t in all {
        let code = errcode_apply_conflict(t);
        assert!(code == ERRCODE_UNIQUE_VIOLATION || code == ERRCODE_T_R_SERIALIZATION_FAILURE);
    }
}
