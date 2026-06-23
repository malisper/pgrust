//! Conflict vocabulary for logical replication (`replication/conflict.h`).

#![allow(non_camel_case_types)]

/// `ConflictType` (replication/conflict.h). The discriminant values are the
/// declaration order in the C enum — they index the statistics counters and
/// the conflict-type name table, so they must not be reordered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ConflictType {
    /// The row to be inserted violates unique constraint.
    CT_INSERT_EXISTS = 0,
    /// The row to be updated was modified by a different origin.
    CT_UPDATE_ORIGIN_DIFFERS = 1,
    /// The updated row value violates unique constraint.
    CT_UPDATE_EXISTS = 2,
    /// The row to be updated is missing.
    CT_UPDATE_MISSING = 3,
    /// The row to be deleted was modified by a different origin.
    CT_DELETE_ORIGIN_DIFFERS = 4,
    /// The row to be deleted is missing.
    CT_DELETE_MISSING = 5,
    /// The row to be inserted/updated violates multiple unique constraints.
    CT_MULTIPLE_UNIQUE_CONFLICTS = 6,
}

/// `CONFLICT_NUM_TYPES` (`CT_MULTIPLE_UNIQUE_CONFLICTS + 1`).
pub const CONFLICT_NUM_TYPES: usize = ConflictType::CT_MULTIPLE_UNIQUE_CONFLICTS as usize + 1;
