//! ABI definitions for query jumbling (`nodes/queryjumble.h`).
//!
//! `JumbleState` and `LocationLen` are working-state structs allocated in a
//! memory context and handed to the planner/parser via raw pointers, so they
//! are `#[repr(C)]` with exact layout matching PostgreSQL 18.3 (the
//! `total_jumble_len` assert field is omitted, matching a non-assert build).

use core::ffi::c_int;

use crate::types::Size;

/// `ComputeQueryIdType` (`nodes/queryjumble.h`): values for the
/// `compute_query_id` GUC.
pub type ComputeQueryIdType = c_int;
pub const COMPUTE_QUERY_ID_OFF: ComputeQueryIdType = 0;
pub const COMPUTE_QUERY_ID_ON: ComputeQueryIdType = 1;
pub const COMPUTE_QUERY_ID_AUTO: ComputeQueryIdType = 2;
pub const COMPUTE_QUERY_ID_REGRESS: ComputeQueryIdType = 3;

/// Query serialization buffer size (`JUMBLE_SIZE`).
pub const JUMBLE_SIZE: Size = 1024;

/// `LocationLen` (`nodes/queryjumble.h`): a constant location to be removed
/// during normalization.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LocationLen {
    /// start offset in query text
    pub location: c_int,
    /// length in bytes, or -1 to ignore
    pub length: c_int,
    /// does this location represent a squashed list?
    pub squashed: bool,
    /// is this location a PARAM_EXTERN parameter?
    pub extern_param: bool,
}

/// `JumbleState` (`nodes/queryjumble.h`): working state for computing a query
/// jumble and producing a normalized query string. Layout matches a build
/// without `USE_ASSERT_CHECKING` (no trailing `total_jumble_len`).
#[repr(C)]
#[derive(Debug)]
pub struct JumbleState {
    /// jumble of current query tree
    pub jumble: *mut u8,
    /// number of bytes used in `jumble[]`
    pub jumble_len: Size,
    /// array of locations of constants that should be removed
    pub clocations: *mut LocationLen,
    /// allocated length of `clocations` array
    pub clocations_buf_size: c_int,
    /// current number of valid entries in `clocations` array
    pub clocations_count: c_int,
    /// ID of the highest PARAM_EXTERN parameter seen
    pub highest_extern_param_id: c_int,
    /// whether squashable lists are present
    pub has_squashed_lists: bool,
    /// count of NULL nodes seen since last appending a value
    pub pending_nulls: core::ffi::c_uint,
}
