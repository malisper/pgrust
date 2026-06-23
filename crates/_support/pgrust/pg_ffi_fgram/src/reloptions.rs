//! On-disk / ABI layout structures for `access/reloptions.h`.
//!
//! These mirror the C structs that PostgreSQL constructs in static storage
//! (the built-in reloption tables) and passes across the relation-option
//! parsing interfaces. They are kept `#[repr(C)]` with compile-time size and
//! offset assertions so the safe `backend-access-common-reloptions` port can
//! interoperate with catalog data and access-method option callbacks.

use core::ffi::{c_char, c_int, c_void};

use crate::storage::ItemIdData;
use crate::storage::SizeOfPageHeaderData;
use crate::{bits32, List, Size, BLCKSZ, LOCKMODE};

// ---------------------------------------------------------------------------
// Built-in reloption limits and defaults
//
// These are the compile-time constants referenced by the built-in reloption
// tables in `reloptions.c` (drawn from `utils/rel.h`, `access/nbtree.h`,
// `access/hash.h`, `access/gist_private.h`, `access/spgist_private.h`,
// `access/heaptoast.h`, `utils/guc.h`, and `storage/bufmgr.h`).
// ---------------------------------------------------------------------------

/// `HEAP_MIN_FILLFACTOR` (`utils/rel.h`).
pub const HEAP_MIN_FILLFACTOR: c_int = 10;
/// `HEAP_DEFAULT_FILLFACTOR` (`utils/rel.h`).
pub const HEAP_DEFAULT_FILLFACTOR: c_int = 100;
/// `BTREE_MIN_FILLFACTOR` (`access/nbtree.h`).
pub const BTREE_MIN_FILLFACTOR: c_int = 10;
/// `BTREE_DEFAULT_FILLFACTOR` (`access/nbtree.h`).
pub const BTREE_DEFAULT_FILLFACTOR: c_int = 90;
/// `HASH_MIN_FILLFACTOR` (`access/hash.h`).
pub const HASH_MIN_FILLFACTOR: c_int = 10;
/// `HASH_DEFAULT_FILLFACTOR` (`access/hash.h`).
pub const HASH_DEFAULT_FILLFACTOR: c_int = 75;
/// `GIST_MIN_FILLFACTOR` (`access/gist_private.h`).
pub const GIST_MIN_FILLFACTOR: c_int = 10;
/// `GIST_DEFAULT_FILLFACTOR` (`access/gist_private.h`).
pub const GIST_DEFAULT_FILLFACTOR: c_int = 90;
/// `SPGIST_MIN_FILLFACTOR` (`access/spgist_private.h`).
pub const SPGIST_MIN_FILLFACTOR: c_int = 10;
/// `SPGIST_DEFAULT_FILLFACTOR` (`access/spgist_private.h`).
pub const SPGIST_DEFAULT_FILLFACTOR: c_int = 80;

/// `MAX_KILOBYTES` (`utils/guc.h`) -- on 64-bit platforms (sizeof(size_t) > 4)
/// this is `INT_MAX`.
pub const MAX_KILOBYTES: c_int = c_int::MAX;
/// `MAX_IO_CONCURRENCY` (`storage/bufmgr.h`).
pub const MAX_IO_CONCURRENCY: c_int = 1000;

/// `MAXIMUM_ALIGNOF` -- maximum alignment of any C scalar (8 on supported
/// 64-bit platforms).
const MAXIMUM_ALIGNOF: usize = 8;

const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

const fn maxalign_down(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}

/// `MaximumBytesPerTuple(tuplesPerPage)` (`access/heaptoast.h`).
const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> c_int {
    maxalign_down(
        (BLCKSZ
            - maxalign(
                SizeOfPageHeaderData + tuples_per_page * core::mem::size_of::<ItemIdData>(),
            ))
            / tuples_per_page,
    ) as c_int
}

/// `TOAST_TUPLES_PER_PAGE` (`access/heaptoast.h`).
pub const TOAST_TUPLES_PER_PAGE: usize = 4;
/// `TOAST_TUPLES_PER_PAGE_MAIN` (`access/heaptoast.h`).
pub const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
/// `TOAST_TUPLE_THRESHOLD` (`access/heaptoast.h`).
pub const TOAST_TUPLE_THRESHOLD: c_int = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE);
/// `TOAST_TUPLE_TARGET` (`access/heaptoast.h`).
pub const TOAST_TUPLE_TARGET: c_int = TOAST_TUPLE_THRESHOLD;
/// `TOAST_TUPLE_TARGET_MAIN` (`access/heaptoast.h`).
pub const TOAST_TUPLE_TARGET_MAIN: c_int = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE_MAIN);

// ---------------------------------------------------------------------------
// Built-in enum symbol values
// ---------------------------------------------------------------------------

/// `StdRdOptIndexCleanup` (`utils/rel.h`).
pub type StdRdOptIndexCleanup = c_int;
pub const STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO: StdRdOptIndexCleanup = 0;
pub const STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF: StdRdOptIndexCleanup = 1;
pub const STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON: StdRdOptIndexCleanup = 2;

/// `GistOptBufferingMode` (`access/gist_private.h`).
pub type GistOptBufferingMode = c_int;
pub const GIST_OPTION_BUFFERING_AUTO: GistOptBufferingMode = 0;
pub const GIST_OPTION_BUFFERING_ON: GistOptBufferingMode = 1;
pub const GIST_OPTION_BUFFERING_OFF: GistOptBufferingMode = 2;

/// `ViewOptCheckOption` (`utils/rel.h`).
pub type ViewOptCheckOption = c_int;
pub const VIEW_OPTION_CHECK_OPTION_NOT_SET: ViewOptCheckOption = 0;
pub const VIEW_OPTION_CHECK_OPTION_LOCAL: ViewOptCheckOption = 1;
pub const VIEW_OPTION_CHECK_OPTION_CASCADED: ViewOptCheckOption = 2;

/// `relopt_type` -- types supported by reloptions.
pub type relopt_type = c_int;
pub const RELOPT_TYPE_BOOL: relopt_type = 0;
pub const RELOPT_TYPE_INT: relopt_type = 1;
pub const RELOPT_TYPE_REAL: relopt_type = 2;
pub const RELOPT_TYPE_ENUM: relopt_type = 3;
pub const RELOPT_TYPE_STRING: relopt_type = 4;

/// `relopt_kind` -- kinds supported by reloptions.
pub type relopt_kind = c_int;
pub const RELOPT_KIND_LOCAL: relopt_kind = 0;
pub const RELOPT_KIND_HEAP: relopt_kind = 1 << 0;
pub const RELOPT_KIND_TOAST: relopt_kind = 1 << 1;
pub const RELOPT_KIND_BTREE: relopt_kind = 1 << 2;
pub const RELOPT_KIND_HASH: relopt_kind = 1 << 3;
pub const RELOPT_KIND_GIN: relopt_kind = 1 << 4;
pub const RELOPT_KIND_GIST: relopt_kind = 1 << 5;
pub const RELOPT_KIND_ATTRIBUTE: relopt_kind = 1 << 6;
pub const RELOPT_KIND_TABLESPACE: relopt_kind = 1 << 7;
pub const RELOPT_KIND_SPGIST: relopt_kind = 1 << 8;
pub const RELOPT_KIND_VIEW: relopt_kind = 1 << 9;
pub const RELOPT_KIND_BRIN: relopt_kind = 1 << 10;
pub const RELOPT_KIND_PARTITIONED: relopt_kind = 1 << 11;
/// If you add a new kind, make sure you update "last_default" too.
pub const RELOPT_KIND_LAST_DEFAULT: relopt_kind = RELOPT_KIND_PARTITIONED;
/// Some compilers treat enums as signed ints, so we can't use `1 << 31`.
pub const RELOPT_KIND_MAX: relopt_kind = 1 << 30;

/// Generic struct to hold shared data (`relopt_gen`).
#[repr(C)]
#[derive(Debug)]
pub struct relopt_gen {
    /// Must be first (used as list termination marker).
    pub name: *const c_char,
    pub desc: *const c_char,
    pub kinds: bits32,
    pub lockmode: LOCKMODE,
    pub namelen: c_int,
    pub type_: relopt_type,
}

/// Union payload for a parsed reloption value (`relopt_value.values`).
#[repr(C)]
#[derive(Clone, Copy)]
pub union relopt_value_union {
    pub bool_val: bool,
    pub int_val: c_int,
    pub real_val: f64,
    pub enum_val: c_int,
    /// Allocated separately.
    pub string_val: *mut c_char,
}

/// Holds a parsed value (`relopt_value`).
#[repr(C)]
pub struct relopt_value {
    pub gen: *mut relopt_gen,
    pub isset: bool,
    pub values: relopt_value_union,
}

/// `relopt_bool` -- reloption record for a boolean variable.
#[repr(C)]
#[derive(Debug)]
pub struct relopt_bool {
    pub gen: relopt_gen,
    pub default_val: bool,
}

/// `relopt_int` -- reloption record for an integer variable.
#[repr(C)]
#[derive(Debug)]
pub struct relopt_int {
    pub gen: relopt_gen,
    pub default_val: c_int,
    pub min: c_int,
    pub max: c_int,
}

/// `relopt_real` -- reloption record for a floating-point variable.
#[repr(C)]
#[derive(Debug)]
pub struct relopt_real {
    pub gen: relopt_gen,
    pub default_val: f64,
    pub min: f64,
    pub max: f64,
}

/// `relopt_enum_elt_def` -- one member of the array of acceptable values of an
/// enum reloption.
#[repr(C)]
#[derive(Debug)]
pub struct relopt_enum_elt_def {
    pub string_val: *const c_char,
    pub symbol_val: c_int,
}

/// `relopt_enum` -- reloption record for an enum variable.
#[repr(C)]
#[derive(Debug)]
pub struct relopt_enum {
    pub gen: relopt_gen,
    /// Null-terminated array of members.
    pub members: *mut relopt_enum_elt_def,
    pub default_val: c_int,
    pub detailmsg: *const c_char,
}

/// Validation routine for string reloptions.
pub type validate_string_relopt = Option<unsafe extern "C" fn(value: *const c_char)>;
/// Fill routine for string reloptions.
pub type fill_string_relopt =
    Option<unsafe extern "C" fn(value: *const c_char, ptr: *mut c_void) -> Size>;
/// Validation routine for the whole option set.
pub type relopts_validator = Option<
    unsafe extern "C" fn(parsed_options: *mut c_void, vals: *mut relopt_value, nvals: c_int),
>;

/// `relopt_string` -- reloption record for a string variable.
#[repr(C)]
pub struct relopt_string {
    pub gen: relopt_gen,
    pub default_len: c_int,
    pub default_isnull: bool,
    pub validate_cb: validate_string_relopt,
    pub fill_cb: fill_string_relopt,
    pub default_val: *mut c_char,
}

/// Table datatype for `build_reloptions()` (`relopt_parse_elt`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct relopt_parse_elt {
    /// Option's name.
    pub optname: *const c_char,
    /// Option's datatype.
    pub opttype: relopt_type,
    /// Offset of field in result struct.
    pub offset: c_int,
    /// Optional offset of an "is set" field in the result struct; only used
    /// when greater than zero.
    pub isset_offset: c_int,
}

/// Local reloption definition (`local_relopt`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct local_relopt {
    /// Option definition.
    pub option: *mut relopt_gen,
    /// Offset of parsed value in bytea structure.
    pub offset: c_int,
}

/// Structure to hold local reloption data for `build_local_reloptions()`
/// (`local_relopts`).
#[repr(C)]
#[derive(Debug)]
pub struct local_relopts {
    /// List of `local_relopt` definitions.
    pub options: *mut List,
    /// List of `relopts_validator` callbacks.
    pub validators: *mut List,
    /// Size of parsed bytea structure.
    pub relopt_struct_size: Size,
}

// ---------------------------------------------------------------------------
// Result option structs (on-disk `rd_options` / catalog bytea layouts)
//
// `default_reloptions`, `view_reloptions`, `attribute_reloptions`, and
// `tablespace_reloptions` build one of these structs and store it as a varlena
// `bytea`.  `fillRelOptions()` writes the parsed values at the byte offsets of
// these fields, so the field offsets must match the C layout exactly.
// ---------------------------------------------------------------------------

/// `AutoVacOpts` -- autovacuum-related reloptions (`utils/rel.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AutoVacOpts {
    pub enabled: bool,
    pub vacuum_threshold: c_int,
    pub vacuum_max_threshold: c_int,
    pub vacuum_ins_threshold: c_int,
    pub analyze_threshold: c_int,
    pub vacuum_cost_limit: c_int,
    pub freeze_min_age: c_int,
    pub freeze_max_age: c_int,
    pub freeze_table_age: c_int,
    pub multixact_freeze_min_age: c_int,
    pub multixact_freeze_max_age: c_int,
    pub multixact_freeze_table_age: c_int,
    pub log_min_duration: c_int,
    pub vacuum_cost_delay: f64,
    pub vacuum_scale_factor: f64,
    pub vacuum_ins_scale_factor: f64,
    pub analyze_scale_factor: f64,
}

/// `StdRdOptions` -- standard contents of `rd_options` for heaps (`utils/rel.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StdRdOptions {
    /// Varlena header (do not touch directly!).
    pub vl_len_: i32,
    pub fillfactor: c_int,
    pub toast_tuple_target: c_int,
    pub autovacuum: AutoVacOpts,
    pub user_catalog_table: bool,
    pub parallel_workers: c_int,
    pub vacuum_index_cleanup: StdRdOptIndexCleanup,
    pub vacuum_truncate: bool,
    pub vacuum_truncate_set: bool,
    pub vacuum_max_eager_freeze_failure_rate: f64,
}

/// `ViewOptions` -- contents of `rd_options` for views (`utils/rel.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ViewOptions {
    /// Varlena header (do not touch directly!).
    pub vl_len_: i32,
    pub security_barrier: bool,
    pub security_invoker: bool,
    pub check_option: ViewOptCheckOption,
}

/// `AttributeOpts` -- per-attribute reloptions (`utils/attoptcache.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AttributeOpts {
    /// Varlena header (do not touch directly!).
    pub vl_len_: i32,
    pub n_distinct: f64,
    pub n_distinct_inherited: f64,
}

/// `TableSpaceOpts` -- tablespace options (`commands/tablespace.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TableSpaceOpts {
    /// Varlena header (do not touch directly!).
    pub vl_len_: i32,
    pub random_page_cost: f64,
    pub seq_page_cost: f64,
    pub effective_io_concurrency: c_int,
    pub maintenance_io_concurrency: c_int,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{offset_of, size_of};

    #[test]
    fn relopt_gen_layout() {
        assert_eq!(size_of::<relopt_gen>(), 32);
        assert_eq!(offset_of!(relopt_gen, name), 0);
        assert_eq!(offset_of!(relopt_gen, desc), 8);
        assert_eq!(offset_of!(relopt_gen, kinds), 16);
        assert_eq!(offset_of!(relopt_gen, lockmode), 20);
        assert_eq!(offset_of!(relopt_gen, namelen), 24);
        assert_eq!(offset_of!(relopt_gen, type_), 28);
    }

    #[test]
    fn relopt_value_layout() {
        assert_eq!(size_of::<relopt_value>(), 24);
        assert_eq!(offset_of!(relopt_value, gen), 0);
        assert_eq!(offset_of!(relopt_value, isset), 8);
        assert_eq!(offset_of!(relopt_value, values), 16);
        assert_eq!(size_of::<relopt_value_union>(), 8);
    }

    #[test]
    fn relopt_typed_records_layout() {
        assert_eq!(size_of::<relopt_bool>(), 40);
        assert_eq!(offset_of!(relopt_bool, default_val), 32);

        assert_eq!(size_of::<relopt_int>(), 48);
        assert_eq!(offset_of!(relopt_int, default_val), 32);
        assert_eq!(offset_of!(relopt_int, min), 36);
        assert_eq!(offset_of!(relopt_int, max), 40);

        assert_eq!(size_of::<relopt_real>(), 56);
        assert_eq!(offset_of!(relopt_real, default_val), 32);
        assert_eq!(offset_of!(relopt_real, min), 40);
        assert_eq!(offset_of!(relopt_real, max), 48);
    }

    #[test]
    fn relopt_enum_layout() {
        assert_eq!(size_of::<relopt_enum_elt_def>(), 16);
        assert_eq!(offset_of!(relopt_enum_elt_def, symbol_val), 8);

        assert_eq!(size_of::<relopt_enum>(), 56);
        assert_eq!(offset_of!(relopt_enum, members), 32);
        assert_eq!(offset_of!(relopt_enum, default_val), 40);
        assert_eq!(offset_of!(relopt_enum, detailmsg), 48);
    }

    #[test]
    fn relopt_string_layout() {
        assert_eq!(size_of::<relopt_string>(), 64);
        assert_eq!(offset_of!(relopt_string, default_len), 32);
        assert_eq!(offset_of!(relopt_string, default_isnull), 36);
        assert_eq!(offset_of!(relopt_string, validate_cb), 40);
        assert_eq!(offset_of!(relopt_string, fill_cb), 48);
        assert_eq!(offset_of!(relopt_string, default_val), 56);
    }

    #[test]
    fn relopt_parse_elt_layout() {
        assert_eq!(size_of::<relopt_parse_elt>(), 24);
        assert_eq!(offset_of!(relopt_parse_elt, opttype), 8);
        assert_eq!(offset_of!(relopt_parse_elt, offset), 12);
        assert_eq!(offset_of!(relopt_parse_elt, isset_offset), 16);
    }

    #[test]
    fn local_relopt_layout() {
        assert_eq!(size_of::<local_relopt>(), 16);
        assert_eq!(offset_of!(local_relopt, offset), 8);

        assert_eq!(size_of::<local_relopts>(), 24);
        assert_eq!(offset_of!(local_relopts, validators), 8);
        assert_eq!(offset_of!(local_relopts, relopt_struct_size), 16);
    }

    #[test]
    fn auto_vac_opts_layout() {
        assert_eq!(size_of::<AutoVacOpts>(), 88);
        assert_eq!(offset_of!(AutoVacOpts, enabled), 0);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_threshold), 4);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_max_threshold), 8);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_ins_threshold), 12);
        assert_eq!(offset_of!(AutoVacOpts, analyze_threshold), 16);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_cost_limit), 20);
        assert_eq!(offset_of!(AutoVacOpts, freeze_min_age), 24);
        assert_eq!(offset_of!(AutoVacOpts, freeze_max_age), 28);
        assert_eq!(offset_of!(AutoVacOpts, freeze_table_age), 32);
        assert_eq!(offset_of!(AutoVacOpts, multixact_freeze_min_age), 36);
        assert_eq!(offset_of!(AutoVacOpts, multixact_freeze_max_age), 40);
        assert_eq!(offset_of!(AutoVacOpts, multixact_freeze_table_age), 44);
        assert_eq!(offset_of!(AutoVacOpts, log_min_duration), 48);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_cost_delay), 56);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_scale_factor), 64);
        assert_eq!(offset_of!(AutoVacOpts, vacuum_ins_scale_factor), 72);
        assert_eq!(offset_of!(AutoVacOpts, analyze_scale_factor), 80);
    }

    #[test]
    fn std_rd_options_layout() {
        assert_eq!(size_of::<StdRdOptions>(), 128);
        assert_eq!(offset_of!(StdRdOptions, vl_len_), 0);
        assert_eq!(offset_of!(StdRdOptions, fillfactor), 4);
        assert_eq!(offset_of!(StdRdOptions, toast_tuple_target), 8);
        assert_eq!(offset_of!(StdRdOptions, autovacuum), 16);
        assert_eq!(offset_of!(StdRdOptions, user_catalog_table), 104);
        assert_eq!(offset_of!(StdRdOptions, parallel_workers), 108);
        assert_eq!(offset_of!(StdRdOptions, vacuum_index_cleanup), 112);
        assert_eq!(offset_of!(StdRdOptions, vacuum_truncate), 116);
        assert_eq!(offset_of!(StdRdOptions, vacuum_truncate_set), 117);
        assert_eq!(
            offset_of!(StdRdOptions, vacuum_max_eager_freeze_failure_rate),
            120
        );
    }

    #[test]
    fn view_options_layout() {
        assert_eq!(size_of::<ViewOptions>(), 12);
        assert_eq!(offset_of!(ViewOptions, vl_len_), 0);
        assert_eq!(offset_of!(ViewOptions, security_barrier), 4);
        assert_eq!(offset_of!(ViewOptions, security_invoker), 5);
        assert_eq!(offset_of!(ViewOptions, check_option), 8);
    }

    #[test]
    fn attribute_opts_layout() {
        assert_eq!(size_of::<AttributeOpts>(), 24);
        assert_eq!(offset_of!(AttributeOpts, vl_len_), 0);
        assert_eq!(offset_of!(AttributeOpts, n_distinct), 8);
        assert_eq!(offset_of!(AttributeOpts, n_distinct_inherited), 16);
    }

    #[test]
    fn table_space_opts_layout() {
        assert_eq!(size_of::<TableSpaceOpts>(), 32);
        assert_eq!(offset_of!(TableSpaceOpts, vl_len_), 0);
        assert_eq!(offset_of!(TableSpaceOpts, random_page_cost), 8);
        assert_eq!(offset_of!(TableSpaceOpts, seq_page_cost), 16);
        assert_eq!(offset_of!(TableSpaceOpts, effective_io_concurrency), 24);
        assert_eq!(offset_of!(TableSpaceOpts, maintenance_io_concurrency), 28);
    }

    #[test]
    fn built_in_reloption_constants() {
        assert_eq!(TOAST_TUPLE_TARGET, 2032);
        assert_eq!(TOAST_TUPLE_TARGET_MAIN, 8160);
        assert_eq!(MAX_IO_CONCURRENCY, 1000);
        assert_eq!(MAX_KILOBYTES, i32::MAX);
    }
}
