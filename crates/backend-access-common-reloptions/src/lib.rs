//! Port of `src/backend/access/common/reloptions.c` — relation-options
//! (`pg_class.reloptions`) and tablespace-options (`pg_tablespace.spcoptions`)
//! parsing.
//!
//! ## Owned-tree representation vs. C's byte-buffer model
//!
//! In C the per-relkind result of `default_reloptions` / `view_reloptions` /
//! `attribute_reloptions` / `tablespace_reloptions` is a `palloc`'d varlena
//! `bytea` whose payload is one of the `#[repr(C)]` option structs
//! (`StdRdOptions`, `ViewOptions`, `AttributeOpts`, `TableSpaceOpts`).
//! `fillRelOptions` writes the parsed values at the `offsetof`-computed byte
//! offsets and `SET_VARSIZE` writes the varlena header.
//!
//! `types-reloptions` models those four option structs as owned typed Rust
//! structs, so this port builds them by typed field assignment keyed by
//! option name — the owned-tree equivalent of the byte-offset table; the
//! public entry points return a typed [`RelOptStruct`] (`None` mirrors the C
//! `(bytea *) NULL`). `build_local_reloptions` is different: its result is an
//! opaque AM-defined bytea (the AM chooses the layout via the byte offsets it
//! registered), so it keeps the faithful byte-buffer path (`Vec<u8>` +
//! `offset`-keyed writes + `SET_VARSIZE`).
//!
//! ## Seams
//!
//! `text[]` deconstruct/construct go through `backend-utils-adt-arrayfuncs`,
//! `defGetString`/`defGetBoolean` through `backend-commands-define`,
//! `parse_int`/`parse_real` through `backend-utils-misc-guc`, and the index
//! AM `amoptions` callback through `backend-access-index-amapi`. `parse_bool`
//! (`utils/adt/bool.c`) is pure logic and ported in-crate as
//! [`builtin_parse_bool`].
//!
//! The reloptions parser's per-backend file-scope statics (`relOpts` /
//! `last_assigned_kind` / `custom_options` / `need_initialization`) live in a
//! `thread_local!` (one backend = one thread; see AGENTS.md "Backend-global
//! state"). The option *definitions* are backend-lifetime metadata, so they
//! use owned `String`/`Vec` (mcx-design decision 5); query-lifetime parsing
//! working copies are allocated through the `Mcx` the caller threads in.

#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::rc::Rc;

use mcx::{Mcx, PgVec};
use types_datum::datum::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_reloptions::{
    AttributeOpts, AutoVacOpts, StdRdOptIndexCleanup, StdRdOptions, TableSpaceOpts,
    ViewOptCheckOption, ViewOptions, STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
    STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF, STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON,
    VIEW_OPTION_CHECK_OPTION_CASCADED, VIEW_OPTION_CHECK_OPTION_LOCAL,
    VIEW_OPTION_CHECK_OPTION_NOT_SET,
};
use types_storage::lock::{
    AccessExclusiveLock, NoLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE, RELKIND_VIEW,
};

use backend_commands_define_seams::DefElemArg;

/// `bits32` (`c.h`) — `uint32`.
pub type bits32 = u32;
/// `Size` (`c.h`) — `size_t`.
type Size = usize;

/// `VARHDRSZ`, the varlena length-header size in bytes.
const VARHDRSZ: usize = core::mem::size_of::<i32>();

// ---------------------------------------------------------------------------
// relopt_kind / relopt_type (access/reloptions.h) — parser-internal enums.
// ---------------------------------------------------------------------------

/// `relopt_type` (access/reloptions.h).
pub type relopt_type = i32;
pub const RELOPT_TYPE_BOOL: relopt_type = 0;
pub const RELOPT_TYPE_INT: relopt_type = 1;
pub const RELOPT_TYPE_REAL: relopt_type = 2;
pub const RELOPT_TYPE_ENUM: relopt_type = 3;
pub const RELOPT_TYPE_STRING: relopt_type = 4;

/// `relopt_kind` (access/reloptions.h).
pub type relopt_kind = i32;
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
pub const RELOPT_KIND_LAST_DEFAULT: relopt_kind = RELOPT_KIND_PARTITIONED;
/// Some compilers treat enums as signed ints, so C can't use `1 << 31`.
pub const RELOPT_KIND_MAX: relopt_kind = 1 << 30;

// ---------------------------------------------------------------------------
// Built-in reloption limits and defaults (drawn from utils/rel.h,
// access/nbtree.h, access/hash.h, access/gist_private.h,
// access/spgist_private.h, access/heaptoast.h, utils/guc.h,
// storage/bufmgr.h).
// ---------------------------------------------------------------------------

const HEAP_MIN_FILLFACTOR: i32 = 10;
const HEAP_DEFAULT_FILLFACTOR: i32 = 100;
const BTREE_MIN_FILLFACTOR: i32 = 10;
const BTREE_DEFAULT_FILLFACTOR: i32 = 90;
const HASH_MIN_FILLFACTOR: i32 = 10;
const HASH_DEFAULT_FILLFACTOR: i32 = 75;
const GIST_MIN_FILLFACTOR: i32 = 10;
const GIST_DEFAULT_FILLFACTOR: i32 = 90;
const SPGIST_MIN_FILLFACTOR: i32 = 10;
const SPGIST_DEFAULT_FILLFACTOR: i32 = 80;

/// `MAX_KILOBYTES` (`utils/guc.h`): on 64-bit (`sizeof(size_t) > 4`) this is
/// `INT_MAX`.
const MAX_KILOBYTES: i32 = i32::MAX;
/// `MAX_IO_CONCURRENCY` (`storage/bufmgr.h`).
const MAX_IO_CONCURRENCY: i32 = 1000;

/// `BLCKSZ` (`pg_config.h`).
const BLCKSZ: usize = 8192;
/// `MAXIMUM_ALIGNOF` (8 on supported 64-bit platforms).
const MAXIMUM_ALIGNOF: usize = 8;
/// `SizeOfPageHeaderData` — `offsetof(PageHeaderData, pd_linp)` = 24 bytes.
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
/// `sizeof(ItemIdData)` — 4 bytes.
const SIZE_OF_ITEM_ID_DATA: usize = 4;

const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}
const fn maxalign_down(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}
/// `MaximumBytesPerTuple(tuplesPerPage)` (`access/heaptoast.h`).
const fn maximum_bytes_per_tuple(tuples_per_page: usize) -> i32 {
    maxalign_down(
        (BLCKSZ - maxalign(SIZE_OF_PAGE_HEADER_DATA + tuples_per_page * SIZE_OF_ITEM_ID_DATA))
            / tuples_per_page,
    ) as i32
}
const TOAST_TUPLES_PER_PAGE: usize = 4;
const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
const TOAST_TUPLE_THRESHOLD: i32 = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE);
const TOAST_TUPLE_TARGET: i32 = TOAST_TUPLE_THRESHOLD;
const TOAST_TUPLE_TARGET_MAIN: i32 = maximum_bytes_per_tuple(TOAST_TUPLES_PER_PAGE_MAIN);

/// `GistOptBufferingMode` (`access/gist_private.h`).
const GIST_OPTION_BUFFERING_AUTO: i32 = 0;
const GIST_OPTION_BUFFERING_ON: i32 = 1;
const GIST_OPTION_BUFFERING_OFF: i32 = 2;

// ---------------------------------------------------------------------------
// Idiomatic reloption-definition model
//
// In C `relopt_gen` is a header up-cast to `relopt_bool`/`relopt_int`/etc.
// depending on `type`. Here the type-specific payload lives in [`RelOptData`]
// and the generic header fields stay alongside it in [`RelOptGen`].
// ---------------------------------------------------------------------------

/// Validation callback for a string reloption (`validate_string_relopt`).
pub type ValidateStringRelopt = fn(Option<&str>) -> PgResult<()>;
/// Fill callback for a string reloption (`fill_string_relopt`). Mirrors C's
/// `fill_string_relopt(const char *value, void *ptr)`: when `ptr` is `None` it
/// only computes the size; otherwise it writes into the buffer and returns the
/// number of bytes written.
pub type FillStringRelopt = fn(value: Option<&str>, ptr: Option<&mut [u8]>) -> Size;
/// Whole-option-set validator (`relopts_validator`).
pub type ReloptsValidator = fn(parsed_options: &mut [u8], vals: &[RelOptValue]) -> PgResult<()>;

/// One member of an enum reloption's accepted-value list
/// (`relopt_enum_elt_def`).
#[derive(Clone, Debug)]
pub struct RelOptEnumEltDef {
    pub string_val: &'static str,
    pub symbol_val: i32,
}

/// Type-specific payload of a reloption definition (the up-cast target of
/// `relopt_gen`).
#[derive(Clone)]
pub enum RelOptData {
    /// `relopt_bool`.
    Bool { default_val: bool },
    /// `relopt_int`.
    Int { default_val: i32, min: i32, max: i32 },
    /// `relopt_real`.
    Real { default_val: f64, min: f64, max: f64 },
    /// `relopt_enum`.
    Enum {
        members: Vec<RelOptEnumEltDef>,
        default_val: i32,
        detailmsg: Option<&'static str>,
    },
    /// `relopt_string`.
    Str {
        default_val: Option<String>,
        default_len: i32,
        default_isnull: bool,
        validate_cb: Option<ValidateStringRelopt>,
        fill_cb: Option<FillStringRelopt>,
    },
}

/// A reloption definition (`relopt_gen` plus its type-specific record).
#[derive(Clone)]
pub struct RelOptGen {
    pub name: String,
    pub desc: Option<String>,
    pub kinds: bits32,
    pub lockmode: LOCKMODE,
    pub namelen: i32,
    pub opttype: relopt_type,
    pub data: RelOptData,
}

/// Idiomatic, working-copy view of `relopt_value` produced during parsing.
#[derive(Clone)]
pub struct RelOptValue {
    pub gen: Rc<RelOptGen>,
    pub isset: bool,
    pub value: RelOptParsed,
}

/// Parsed payload of a [`RelOptValue`] (mirrors the `relopt_value.values`
/// union).
#[derive(Clone, Debug)]
pub enum RelOptParsed {
    /// Unparsed; the default will be used.
    None,
    Bool(bool),
    Int(i32),
    Real(f64),
    Enum(i32),
    String(String),
}

/// One entry in a `build_local_reloptions()` parse table (`relopt_parse_elt`).
#[derive(Clone, Debug)]
pub struct RelOptParseElt {
    pub optname: String,
    pub opttype: relopt_type,
    pub offset: i32,
    pub isset_offset: i32,
}

impl RelOptParseElt {
    /// Construct a parse-table entry without an `isset` offset (the 3-field
    /// `{name, type, offset}` initializer in C).
    pub fn new(optname: &str, opttype: relopt_type, offset: usize) -> Self {
        RelOptParseElt {
            optname: optname.to_string(),
            opttype,
            offset: offset as i32,
            isset_offset: 0,
        }
    }

    /// Construct a parse-table entry that also records an `isset` flag (the
    /// 4-field `{name, type, offset, isset_offset}` initializer in C).
    pub fn new_with_isset(
        optname: &str,
        opttype: relopt_type,
        offset: usize,
        isset_offset: usize,
    ) -> Self {
        RelOptParseElt {
            optname: optname.to_string(),
            opttype,
            offset: offset as i32,
            isset_offset: isset_offset as i32,
        }
    }
}

/// A single local reloption definition (`local_relopt`).
#[derive(Clone)]
pub struct LocalRelOpt {
    pub option: Rc<RelOptGen>,
    pub offset: i32,
}

/// Local reloption set for `build_local_reloptions()` (`local_relopts`).
#[derive(Default)]
pub struct LocalRelOpts {
    pub options: Vec<LocalRelOpt>,
    pub validators: Vec<ReloptsValidator>,
    pub relopt_struct_size: Size,
}

/// Owned-tree stand-in for the per-relkind result `bytea`.
#[derive(Clone, Debug, PartialEq)]
pub enum RelOptStruct {
    Std(StdRdOptions),
    View(ViewOptions),
    Attribute(AttributeOpts),
    TableSpace(TableSpaceOpts),
    /// Opaque AM-defined option bytea (from `index_reloptions`' `amoptions`
    /// callback).
    Bytea(Vec<u8>),
}

// ---------------------------------------------------------------------------
// parse_bool — ported in-crate (utils/adt/bool.c), pure logic.
// ---------------------------------------------------------------------------

/// Faithful in-crate port of `parse_bool` (`utils/adt/bool.c`) so the common
/// boolean case needs no installed seam.
pub fn builtin_parse_bool(value: &str) -> Option<bool> {
    let b = value.as_bytes();
    match b.len() {
        1 => match b[0] {
            b't' | b'T' | b'y' | b'Y' | b'1' => Some(true),
            b'f' | b'F' | b'n' | b'N' | b'0' => Some(false),
            _ => None,
        },
        2 => {
            if b.eq_ignore_ascii_case(b"on") {
                Some(true)
            } else if b.eq_ignore_ascii_case(b"no") {
                Some(false)
            } else {
                None
            }
        }
        3 => {
            if b.eq_ignore_ascii_case(b"yes") {
                Some(true)
            } else if b.eq_ignore_ascii_case(b"off") {
                Some(false)
            } else {
                None
            }
        }
        4 => {
            if b.eq_ignore_ascii_case(b"true") {
                Some(true)
            } else {
                None
            }
        }
        5 => {
            if b.eq_ignore_ascii_case(b"false") {
                Some(false)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// `pg_strcasecmp` (src/port/pgstrcasecmp.c) — ASCII-case-insensitive compare
/// with C semantics. Pure logic, ported in-crate for the enum match.
fn pg_strcasecmp(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().min(s2.len());
    for i in 0..n {
        let mut ch1 = s1[i];
        let mut ch2 = s2[i];
        if ch1 != ch2 {
            if ch1.is_ascii_uppercase() {
                ch1 += b'a' - b'A';
            }
            if ch2.is_ascii_uppercase() {
                ch2 += b'a' - b'A';
            }
            if ch1 != ch2 {
                return ch1 as i32 - ch2 as i32;
            }
        }
    }
    (s1.len() as i32) - (s2.len() as i32)
}

// ---------------------------------------------------------------------------
// Built-in reloption tables (boolRelOpts / intRelOpts / realRelOpts /
// enumRelOpts / stringRelOpts).
// ---------------------------------------------------------------------------

fn bool_rel_opts() -> Vec<RelOptGen> {
    vec![
        gen_bool("autosummarize", "Enables automatic summarization on this BRIN index", RELOPT_KIND_BRIN, AccessExclusiveLock, false),
        gen_bool("autovacuum_enabled", "Enables autovacuum in this relation", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, true),
        gen_bool("user_catalog_table", "Declare a table as an additional catalog table, e.g. for the purpose of logical replication", RELOPT_KIND_HEAP, AccessExclusiveLock, false),
        gen_bool("fastupdate", "Enables \"fast update\" feature for this GIN index", RELOPT_KIND_GIN, AccessExclusiveLock, true),
        gen_bool("security_barrier", "View acts as a row security barrier", RELOPT_KIND_VIEW, AccessExclusiveLock, false),
        gen_bool("security_invoker", "Privileges on underlying relations are checked as the invoking user, not the view owner", RELOPT_KIND_VIEW, AccessExclusiveLock, false),
        gen_bool("vacuum_truncate", "Enables vacuum to truncate empty pages at the end of this table", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, true),
        gen_bool("deduplicate_items", "Enables \"deduplicate items\" feature for this btree index", RELOPT_KIND_BTREE, ShareUpdateExclusiveLock, true),
    ]
}

fn int_rel_opts() -> Vec<RelOptGen> {
    vec![
        gen_int("fillfactor", "Packs table pages only to this percentage", RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, HEAP_DEFAULT_FILLFACTOR, HEAP_MIN_FILLFACTOR, 100),
        gen_int("fillfactor", "Packs btree index pages only to this percentage", RELOPT_KIND_BTREE, ShareUpdateExclusiveLock, BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR, 100),
        gen_int("fillfactor", "Packs hash index pages only to this percentage", RELOPT_KIND_HASH, ShareUpdateExclusiveLock, HASH_DEFAULT_FILLFACTOR, HASH_MIN_FILLFACTOR, 100),
        gen_int("fillfactor", "Packs gist index pages only to this percentage", RELOPT_KIND_GIST, ShareUpdateExclusiveLock, GIST_DEFAULT_FILLFACTOR, GIST_MIN_FILLFACTOR, 100),
        gen_int("fillfactor", "Packs spgist index pages only to this percentage", RELOPT_KIND_SPGIST, ShareUpdateExclusiveLock, SPGIST_DEFAULT_FILLFACTOR, SPGIST_MIN_FILLFACTOR, 100),
        gen_int("autovacuum_vacuum_threshold", "Minimum number of tuple updates or deletes prior to vacuum", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, i32::MAX),
        gen_int("autovacuum_vacuum_max_threshold", "Maximum number of tuple updates or deletes prior to vacuum", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -2, -1, i32::MAX),
        gen_int("autovacuum_vacuum_insert_threshold", "Minimum number of tuple inserts prior to vacuum, or -1 to disable insert vacuums", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -2, -1, i32::MAX),
        gen_int("autovacuum_analyze_threshold", "Minimum number of tuple inserts, updates or deletes prior to analyze", RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, -1, 0, i32::MAX),
        gen_int("autovacuum_vacuum_cost_limit", "Vacuum cost amount available before napping, for autovacuum", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 1, 10000),
        gen_int("autovacuum_freeze_min_age", "Minimum age at which VACUUM should freeze a table row, for autovacuum", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 1000000000),
        gen_int("autovacuum_multixact_freeze_min_age", "Minimum multixact age at which VACUUM should freeze a row multixact's, for autovacuum", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 1000000000),
        gen_int("autovacuum_freeze_max_age", "Age at which to autovacuum a table to prevent transaction ID wraparound", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 100000, 2000000000),
        gen_int("autovacuum_multixact_freeze_max_age", "Multixact age at which to autovacuum a table to prevent multixact wraparound", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 10000, 2000000000),
        gen_int("autovacuum_freeze_table_age", "Age at which VACUUM should perform a full table sweep to freeze row versions", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 2000000000),
        gen_int("autovacuum_multixact_freeze_table_age", "Age of multixact at which VACUUM should perform a full table sweep to freeze row versions", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 2000000000),
        gen_int("log_autovacuum_min_duration", "Sets the minimum execution time above which autovacuum actions will be logged", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, -1, i32::MAX),
        gen_int("toast_tuple_target", "Sets the target tuple length at which external columns will be toasted", RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, TOAST_TUPLE_TARGET, 128, TOAST_TUPLE_TARGET_MAIN),
        gen_int("pages_per_range", "Number of pages that each page range covers in a BRIN index", RELOPT_KIND_BRIN, AccessExclusiveLock, 128, 1, 131072),
        gen_int("gin_pending_list_limit", "Maximum size of the pending list for this GIN index, in kilobytes.", RELOPT_KIND_GIN, AccessExclusiveLock, -1, 64, MAX_KILOBYTES),
        gen_int("effective_io_concurrency", "Number of simultaneous requests that can be handled efficiently by the disk subsystem.", RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1, 0, MAX_IO_CONCURRENCY),
        gen_int("maintenance_io_concurrency", "Number of simultaneous requests that can be handled efficiently by the disk subsystem for maintenance work.", RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1, 0, MAX_IO_CONCURRENCY),
        gen_int("parallel_workers", "Number of parallel processes that can be used per executor node for this relation.", RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, -1, 0, 1024),
    ]
}

fn real_rel_opts() -> Vec<RelOptGen> {
    vec![
        gen_real("autovacuum_vacuum_cost_delay", "Vacuum cost delay in milliseconds, for autovacuum", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
        gen_real("autovacuum_vacuum_scale_factor", "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
        gen_real("autovacuum_vacuum_insert_scale_factor", "Number of tuple inserts prior to vacuum as a fraction of reltuples", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
        gen_real("autovacuum_analyze_scale_factor", "Number of tuple inserts, updates or deletes prior to analyze as a fraction of reltuples", RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
        gen_real("vacuum_max_eager_freeze_failure_rate", "Fraction of pages in a relation vacuum can scan and fail to freeze before disabling eager scanning.", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 1.0),
        gen_real("seq_page_cost", "Sets the planner's estimate of the cost of a sequentially fetched disk page.", RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1.0, 0.0, f64::MAX),
        gen_real("random_page_cost", "Sets the planner's estimate of the cost of a nonsequentially fetched disk page.", RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1.0, 0.0, f64::MAX),
        gen_real("n_distinct", "Sets the planner's estimate of the number of distinct values appearing in a column (excluding child relations).", RELOPT_KIND_ATTRIBUTE, ShareUpdateExclusiveLock, 0.0, -1.0, f64::MAX),
        gen_real("n_distinct_inherited", "Sets the planner's estimate of the number of distinct values appearing in a column (including child relations).", RELOPT_KIND_ATTRIBUTE, ShareUpdateExclusiveLock, 0.0, -1.0, f64::MAX),
        gen_real("vacuum_cleanup_index_scale_factor", "Deprecated B-Tree parameter.", RELOPT_KIND_BTREE, ShareUpdateExclusiveLock, -1.0, 0.0, 1e10),
    ]
}

fn std_rd_opt_index_cleanup_values() -> Vec<RelOptEnumEltDef> {
    vec![
        RelOptEnumEltDef { string_val: "auto", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO },
        RelOptEnumEltDef { string_val: "on", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON },
        RelOptEnumEltDef { string_val: "off", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF },
        RelOptEnumEltDef { string_val: "true", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON },
        RelOptEnumEltDef { string_val: "false", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF },
        RelOptEnumEltDef { string_val: "yes", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON },
        RelOptEnumEltDef { string_val: "no", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF },
        RelOptEnumEltDef { string_val: "1", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON },
        RelOptEnumEltDef { string_val: "0", symbol_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF },
    ]
}

fn gist_buffering_opt_values() -> Vec<RelOptEnumEltDef> {
    vec![
        RelOptEnumEltDef { string_val: "auto", symbol_val: GIST_OPTION_BUFFERING_AUTO },
        RelOptEnumEltDef { string_val: "on", symbol_val: GIST_OPTION_BUFFERING_ON },
        RelOptEnumEltDef { string_val: "off", symbol_val: GIST_OPTION_BUFFERING_OFF },
    ]
}

fn view_check_opt_values() -> Vec<RelOptEnumEltDef> {
    vec![
        RelOptEnumEltDef { string_val: "local", symbol_val: VIEW_OPTION_CHECK_OPTION_LOCAL },
        RelOptEnumEltDef { string_val: "cascaded", symbol_val: VIEW_OPTION_CHECK_OPTION_CASCADED },
    ]
}

fn enum_rel_opts() -> Vec<RelOptGen> {
    vec![
        gen_enum("vacuum_index_cleanup", "Controls index vacuuming and index cleanup", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, std_rd_opt_index_cleanup_values(), STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO, Some("Valid values are \"on\", \"off\", and \"auto\".")),
        gen_enum("buffering", "Enables buffering build for this GiST index", RELOPT_KIND_GIST, AccessExclusiveLock, gist_buffering_opt_values(), GIST_OPTION_BUFFERING_AUTO, Some("Valid values are \"on\", \"off\", and \"auto\".")),
        gen_enum("check_option", "View has WITH CHECK OPTION defined (local or cascaded).", RELOPT_KIND_VIEW, AccessExclusiveLock, view_check_opt_values(), VIEW_OPTION_CHECK_OPTION_NOT_SET, Some("Valid values are \"local\" and \"cascaded\".")),
    ]
}

fn string_rel_opts() -> Vec<RelOptGen> {
    Vec::new()
}

fn gen_header(name: &str, desc: &str, kinds: relopt_kind, lockmode: LOCKMODE, opttype: relopt_type) -> (String, Option<String>, bits32, LOCKMODE, i32, relopt_type) {
    (name.to_string(), Some(desc.to_string()), kinds as bits32, lockmode, name.len() as i32, opttype)
}

fn gen_bool(name: &str, desc: &str, kinds: relopt_kind, lockmode: LOCKMODE, default_val: bool) -> RelOptGen {
    let (name, desc, kinds, lockmode, namelen, opttype) = gen_header(name, desc, kinds, lockmode, RELOPT_TYPE_BOOL);
    RelOptGen { name, desc, kinds, lockmode, namelen, opttype, data: RelOptData::Bool { default_val } }
}

fn gen_int(name: &str, desc: &str, kinds: relopt_kind, lockmode: LOCKMODE, default_val: i32, min: i32, max: i32) -> RelOptGen {
    let (name, desc, kinds, lockmode, namelen, opttype) = gen_header(name, desc, kinds, lockmode, RELOPT_TYPE_INT);
    RelOptGen { name, desc, kinds, lockmode, namelen, opttype, data: RelOptData::Int { default_val, min, max } }
}

fn gen_real(name: &str, desc: &str, kinds: relopt_kind, lockmode: LOCKMODE, default_val: f64, min: f64, max: f64) -> RelOptGen {
    let (name, desc, kinds, lockmode, namelen, opttype) = gen_header(name, desc, kinds, lockmode, RELOPT_TYPE_REAL);
    RelOptGen { name, desc, kinds, lockmode, namelen, opttype, data: RelOptData::Real { default_val, min, max } }
}

fn gen_enum(name: &str, desc: &str, kinds: relopt_kind, lockmode: LOCKMODE, members: Vec<RelOptEnumEltDef>, default_val: i32, detailmsg: Option<&'static str>) -> RelOptGen {
    let (name, desc, kinds, lockmode, namelen, opttype) = gen_header(name, desc, kinds, lockmode, RELOPT_TYPE_ENUM);
    RelOptGen { name, desc, kinds, lockmode, namelen, opttype, data: RelOptData::Enum { members, default_val, detailmsg } }
}

/// Process-global parser state, mirroring the file-scope statics in
/// `reloptions.c` (`relOpts`, `last_assigned_kind`, `custom_options`,
/// `need_initialization`). One backend = one thread.
struct ReloptState {
    rel_opts: Vec<Rc<RelOptGen>>,
    last_assigned_kind: bits32,
    custom_options: Vec<Rc<RelOptGen>>,
    need_initialization: bool,
}

thread_local! {
    static RELOPT_STATE: RefCell<ReloptState> = const {
        RefCell::new(ReloptState {
            rel_opts: Vec::new(),
            last_assigned_kind: RELOPT_KIND_LAST_DEFAULT as bits32,
            custom_options: Vec::new(),
            need_initialization: true,
        })
    };
}

fn with_state<T>(f: impl FnOnce(&mut ReloptState) -> T) -> T {
    RELOPT_STATE.with(|s| f(&mut s.borrow_mut()))
}

// ---------------------------------------------------------------------------
// Initialization and custom-kind registration
// ---------------------------------------------------------------------------

/// `initialize_reloptions` -- initialization routine, must be called before
/// parsing. Rebuilds the `relOpts` parser table from the built-in and custom
/// reloption arrays.
fn initialize_reloptions(state: &mut ReloptState) {
    let mut rel_opts: Vec<Rc<RelOptGen>> = Vec::new();
    for opt in bool_rel_opts() {
        rel_opts.push(Rc::new(opt));
    }
    for opt in int_rel_opts() {
        rel_opts.push(Rc::new(opt));
    }
    for opt in real_rel_opts() {
        rel_opts.push(Rc::new(opt));
    }
    for opt in enum_rel_opts() {
        rel_opts.push(Rc::new(opt));
    }
    for opt in string_rel_opts() {
        rel_opts.push(Rc::new(opt));
    }
    for opt in &state.custom_options {
        rel_opts.push(Rc::clone(opt));
    }
    state.rel_opts = rel_opts;
    state.need_initialization = false;
}

/// `add_reloption_kind` -- create a new `relopt_kind` value, to be used in
/// custom reloptions by user-defined AMs.
pub fn add_reloption_kind() -> PgResult<relopt_kind> {
    with_state(|state| {
        // don't hand out the last bit so that the enum's behavior is portable
        if state.last_assigned_kind >= RELOPT_KIND_MAX as bits32 {
            return Err(PgError::error("user-defined relation parameter types limit exceeded")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        state.last_assigned_kind <<= 1;
        Ok(state.last_assigned_kind as relopt_kind)
    })
}

/// `add_reloption` -- add an already-created custom reloption to the list, and
/// recompute the main parser table.
fn add_reloption(newoption: Rc<RelOptGen>) {
    with_state(|state| {
        state.custom_options.push(newoption);
        state.need_initialization = true;
    });
}

/// `init_local_reloptions` -- initialize local reloptions that will be parsed
/// into a bytea structure of `relopt_struct_size`.
pub fn init_local_reloptions(relopts: &mut LocalRelOpts, relopt_struct_size: Size) {
    relopts.options = Vec::new();
    relopts.validators = Vec::new();
    relopts.relopt_struct_size = relopt_struct_size;
}

/// `register_reloptions_validator` -- register a custom validation callback to
/// run at the end of `build_local_reloptions()`.
pub fn register_reloptions_validator(relopts: &mut LocalRelOpts, validator: ReloptsValidator) {
    relopts.validators.push(validator);
}

/// `add_local_reloption` -- add an already-created custom reloption to the
/// local list.
fn add_local_reloption(relopts: &mut LocalRelOpts, newoption: Rc<RelOptGen>, offset: i32) {
    debug_assert!((offset as Size) < relopts.relopt_struct_size);
    relopts.options.push(LocalRelOpt { option: newoption, offset });
}

/// `allocate_reloption` -- allocate a new reloption and initialize the
/// type-agnostic fields.
fn allocate_reloption(kinds: bits32, type_: relopt_type, name: &str, desc: Option<&str>, lockmode: LOCKMODE, data: RelOptData) -> RelOptGen {
    RelOptGen {
        name: name.to_string(),
        desc: desc.map(|d| d.to_string()),
        kinds,
        namelen: name.len() as i32,
        opttype: type_,
        lockmode,
        data,
    }
}

// ---------------------------------------------------------------------------
// Typed reloption registration: bool / int / real / enum / string
// ---------------------------------------------------------------------------

fn init_bool_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: bool, lockmode: LOCKMODE) -> RelOptGen {
    allocate_reloption(kinds, RELOPT_TYPE_BOOL, name, desc, lockmode, RelOptData::Bool { default_val })
}

/// `add_bool_reloption` -- add a new boolean reloption.
pub fn add_bool_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: bool, lockmode: LOCKMODE) {
    let newoption = init_bool_reloption(kinds, name, desc, default_val, lockmode);
    add_reloption(Rc::new(newoption));
}

/// `add_local_bool_reloption` -- add a new boolean local reloption.
pub fn add_local_bool_reloption(relopts: &mut LocalRelOpts, name: &str, desc: Option<&str>, default_val: bool, offset: i32) {
    let newoption = init_bool_reloption(RELOPT_KIND_LOCAL as bits32, name, desc, default_val, 0);
    add_local_reloption(relopts, Rc::new(newoption), offset);
}

fn init_int_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: i32, min_val: i32, max_val: i32, lockmode: LOCKMODE) -> RelOptGen {
    allocate_reloption(kinds, RELOPT_TYPE_INT, name, desc, lockmode, RelOptData::Int { default_val, min: min_val, max: max_val })
}

/// `add_int_reloption` -- add a new integer reloption.
pub fn add_int_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: i32, min_val: i32, max_val: i32, lockmode: LOCKMODE) {
    let newoption = init_int_reloption(kinds, name, desc, default_val, min_val, max_val, lockmode);
    add_reloption(Rc::new(newoption));
}

/// `add_local_int_reloption` -- add a new local integer reloption.
pub fn add_local_int_reloption(relopts: &mut LocalRelOpts, name: &str, desc: Option<&str>, default_val: i32, min_val: i32, max_val: i32, offset: i32) {
    let newoption = init_int_reloption(RELOPT_KIND_LOCAL as bits32, name, desc, default_val, min_val, max_val, 0);
    add_local_reloption(relopts, Rc::new(newoption), offset);
}

fn init_real_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: f64, min_val: f64, max_val: f64, lockmode: LOCKMODE) -> RelOptGen {
    allocate_reloption(kinds, RELOPT_TYPE_REAL, name, desc, lockmode, RelOptData::Real { default_val, min: min_val, max: max_val })
}

/// `add_real_reloption` -- add a new float reloption.
pub fn add_real_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: f64, min_val: f64, max_val: f64, lockmode: LOCKMODE) {
    let newoption = init_real_reloption(kinds, name, desc, default_val, min_val, max_val, lockmode);
    add_reloption(Rc::new(newoption));
}

/// `add_local_real_reloption` -- add a new local float reloption.
pub fn add_local_real_reloption(relopts: &mut LocalRelOpts, name: &str, desc: Option<&str>, default_val: f64, min_val: f64, max_val: f64, offset: i32) {
    let newoption = init_real_reloption(RELOPT_KIND_LOCAL as bits32, name, desc, default_val, min_val, max_val, 0);
    add_local_reloption(relopts, Rc::new(newoption), offset);
}

fn init_enum_reloption(kinds: bits32, name: &str, desc: Option<&str>, members: Vec<RelOptEnumEltDef>, default_val: i32, detailmsg: Option<&'static str>, lockmode: LOCKMODE) -> RelOptGen {
    allocate_reloption(kinds, RELOPT_TYPE_ENUM, name, desc, lockmode, RelOptData::Enum { members, default_val, detailmsg })
}

/// `add_enum_reloption` -- add a new enum reloption.
pub fn add_enum_reloption(kinds: bits32, name: &str, desc: Option<&str>, members: Vec<RelOptEnumEltDef>, default_val: i32, detailmsg: Option<&'static str>, lockmode: LOCKMODE) {
    let newoption = init_enum_reloption(kinds, name, desc, members, default_val, detailmsg, lockmode);
    add_reloption(Rc::new(newoption));
}

/// `add_local_enum_reloption` -- add a new local enum reloption.
pub fn add_local_enum_reloption(relopts: &mut LocalRelOpts, name: &str, desc: Option<&str>, members: Vec<RelOptEnumEltDef>, default_val: i32, detailmsg: Option<&'static str>, offset: i32) {
    let newoption = init_enum_reloption(RELOPT_KIND_LOCAL as bits32, name, desc, members, default_val, detailmsg, 0);
    add_local_reloption(relopts, Rc::new(newoption), offset);
}

fn init_string_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: Option<&str>, validator: Option<ValidateStringRelopt>, filler: Option<FillStringRelopt>, lockmode: LOCKMODE) -> PgResult<RelOptGen> {
    // make sure the validator/default combination is sane
    if let Some(validator) = validator {
        validator(default_val)?;
    }
    let data = if let Some(default_val) = default_val {
        RelOptData::Str {
            default_len: default_val.len() as i32,
            default_isnull: false,
            default_val: Some(default_val.to_string()),
            validate_cb: validator,
            fill_cb: filler,
        }
    } else {
        RelOptData::Str {
            default_val: Some(String::new()),
            default_len: 0,
            default_isnull: true,
            validate_cb: validator,
            fill_cb: filler,
        }
    };
    Ok(allocate_reloption(kinds, RELOPT_TYPE_STRING, name, desc, lockmode, data))
}

/// `add_string_reloption` -- add a new string reloption.
pub fn add_string_reloption(kinds: bits32, name: &str, desc: Option<&str>, default_val: Option<&str>, validator: Option<ValidateStringRelopt>, lockmode: LOCKMODE) -> PgResult<()> {
    let newoption = init_string_reloption(kinds, name, desc, default_val, validator, None, lockmode)?;
    add_reloption(Rc::new(newoption));
    Ok(())
}

/// `add_local_string_reloption` -- add a new local string reloption.
pub fn add_local_string_reloption(relopts: &mut LocalRelOpts, name: &str, desc: Option<&str>, default_val: Option<&str>, validator: Option<ValidateStringRelopt>, filler: Option<FillStringRelopt>, offset: i32) -> PgResult<()> {
    let newoption = init_string_reloption(RELOPT_KIND_LOCAL as bits32, name, desc, default_val, validator, filler, 0)?;
    add_local_reloption(relopts, Rc::new(newoption), offset);
    Ok(())
}

// ---------------------------------------------------------------------------
// DefElem view for transformRelOptions / untransformRelOptions /
// AlterTableGetRelOptionsLockLevel
// ---------------------------------------------------------------------------

/// Idiomatic working view of a `DefElem` option node, holding the fields these
/// routines read. Value extraction (`defGetString`/`defGetBoolean`) is routed
/// through the `backend-commands-define` seam, which needs the value node; the
/// caller fills [`DefElem::arg`] from its real `def->arg`.
#[derive(Clone, Debug)]
pub struct DefElem {
    pub defnamespace: Option<String>,
    pub defname: String,
    /// The value node (`def->arg`), or `None` for `def->arg == NULL`.
    pub arg: Option<DefElemArg>,
}

impl DefElem {
    pub fn new(defnamespace: Option<&str>, defname: &str, arg: Option<DefElemArg>) -> Self {
        DefElem {
            defnamespace: defnamespace.map(|s| s.to_string()),
            defname: defname.to_string(),
            arg,
        }
    }

    /// C: `def->arg != NULL`.
    fn has_arg(&self) -> bool {
        self.arg.is_some()
    }
}

// ---------------------------------------------------------------------------
// Catalog transform / parsing
// ---------------------------------------------------------------------------

/// `transformRelOptions` -- transform a relation-options list (`DefElem`s) into
/// the `text[]` format kept in `pg_class.reloptions`, including only options in
/// the passed namespace. `old_options` is the verbatim existing `text[]`
/// varlena (or `None` for the C `(Datum) 0`); the result is the constructed
/// `text[]` `Datum` (or `None` for `(Datum) 0`).
pub fn transformRelOptions(
    mcx: Mcx<'_>,
    old_options: Option<&[u8]>,
    def_list: &[DefElem],
    namspace: Option<&str>,
    validnsps: Option<&[&str]>,
    accept_oids_off: bool,
    is_reset: bool,
) -> PgResult<Option<Datum>> {
    // Build the element strings, then emit the bare-word `text[]` Datum via the
    // construct seam. NOTE: this returns a bare in-`mcx` pointer word
    // (`types_datum::Datum`), which is NOT carried on the `types_tuple::Datum`
    // by-reference lane the catalog write path deforms; callers that store the
    // result into a catalog tuple (CREATE INDEX, attoptions) must instead use
    // [`transformRelOptionsBytes`], which returns the array varlena image so it
    // can ride a `Datum::ByRef`. This bare-word entry is kept for parity with the
    // C `Datum`-returning signature.
    let strings = transform_rel_options_strings(
        mcx,
        old_options,
        def_list,
        namspace,
        validnsps,
        accept_oids_off,
        is_reset,
    )?;
    match strings {
        None => Ok(None),
        Some(astate) => {
            let refs: Vec<&str> = astate.iter().map(|s| s.as_str()).collect();
            let datum =
                backend_utils_adt_arrayfuncs_seams::construct_text_array::call(mcx, &refs)?;
            Ok(Some(datum))
        }
    }
}

/// `transformRelOptions` returning the on-disk `text[]` array varlena image (the
/// bytes a `types_tuple::Datum::ByRef` / `RefPayload::Varlena` carries), rather
/// than a bare in-`mcx` pointer word. This is the catalog-write form: the
/// returned bytes are exactly what `pg_class.reloptions` / `pg_attribute.
/// attoptions` store, so a consumer lowers them onto the by-reference Datum lane
/// (`Datum::from_byref_bytes_in`) for `index_create` / the tuple-form path.
///
/// `None` mirrors the C `(Datum) 0` no-array case (no options → SQL NULL).
pub fn transformRelOptionsBytes<'mcx>(
    mcx: Mcx<'mcx>,
    old_options: Option<&[u8]>,
    def_list: &[DefElem],
    namspace: Option<&str>,
    validnsps: Option<&[&str]>,
    accept_oids_off: bool,
    is_reset: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let strings = transform_rel_options_strings(
        mcx,
        old_options,
        def_list,
        namspace,
        validnsps,
        accept_oids_off,
        is_reset,
    )?;
    match strings {
        None => Ok(None),
        Some(astate) => {
            let refs: Vec<&str> = astate.iter().map(|s| s.as_str()).collect();
            let bytes =
                backend_utils_adt_arrayfuncs_seams::construct_text_array_bytes::call(mcx, &refs)?;
            Ok(Some(bytes))
        }
    }
}

/// Shared element-string build for [`transformRelOptions`] /
/// [`transformRelOptionsBytes`]: produce the flattened `name=value` `text[]`
/// element list (`makeArrayResult` input). `None` is the C `(Datum) 0` no-array
/// case (empty result); `Some(strings)` is the non-empty element set.
fn transform_rel_options_strings(
    mcx: Mcx<'_>,
    old_options: Option<&[u8]>,
    def_list: &[DefElem],
    namspace: Option<&str>,
    validnsps: Option<&[&str]>,
    accept_oids_off: bool,
    is_reset: bool,
) -> PgResult<Option<Vec<String>>> {
    // no change if empty list. C: `return oldOptions;` — the input array
    // verbatim (or `(Datum) 0` when there were none). The port's input is the
    // raw `text[]` bytes, so re-hand them as the element strings: deconstruct
    // preserves the array content exactly (reloptions are a flat `text[]`).
    // `None` here mirrors the C `(Datum) 0` no-array case.
    if def_list.is_empty() {
        match old_options {
            None => return Ok(None),
            Some(old_options) => {
                let oldoptions =
                    backend_utils_adt_arrayfuncs_seams::deconstruct_text_array::call(mcx, old_options)?;
                if oldoptions.is_empty() {
                    return Ok(None);
                }
                let strings: Vec<String> =
                    oldoptions.iter().map(|s| s.as_str().to_string()).collect();
                return Ok(Some(strings));
            }
        }
    }

    // We build the new array element strings.
    let mut astate: Vec<String> = Vec::new();

    // Copy any oldOptions that aren't to be replaced.
    if let Some(old_options) = old_options {
        let oldoptions = backend_utils_adt_arrayfuncs_seams::deconstruct_text_array::call(mcx, old_options)?;
        for opt in oldoptions.iter() {
            let text_str = opt.as_bytes();
            let text_len = text_str.len();

            // Search for a match in defList.
            let mut matched = false;
            for def in def_list {
                // ignore if not in the same namespace
                match namspace {
                    None => {
                        if def.defnamespace.is_some() {
                            continue;
                        }
                    }
                    Some(ns) => match &def.defnamespace {
                        None => continue,
                        Some(defns) => {
                            if defns != ns {
                                continue;
                            }
                        }
                    },
                }
                let kw_len = def.defname.len();
                if text_len > kw_len && text_str[kw_len] == b'=' && text_str[..kw_len] == *def.defname.as_bytes() {
                    matched = true;
                    break;
                }
            }
            if !matched {
                // No match, so keep old option.
                astate.push(opt.as_str().to_string());
            }
        }
    }

    // If CREATE/SET, add new options to array; if RESET, just check that the
    // user didn't say RESET (option=val).
    for def in def_list {
        if is_reset {
            if def.has_arg() {
                return Err(PgError::error("RESET must not include values for parameters").with_sqlstate(ERRCODE_SYNTAX_ERROR));
            }
        } else {
            // Error out if the namespace is not valid. A NULL namespace is
            // always valid.
            if let Some(defns) = &def.defnamespace {
                let mut valid = false;
                if let Some(validnsps) = validnsps {
                    for ns in validnsps {
                        if defns == ns {
                            valid = true;
                            break;
                        }
                    }
                }
                if !valid {
                    return Err(PgError::error(format!("unrecognized parameter namespace \"{defns}\"")).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
                }
            }

            // ignore if not in the same namespace
            match namspace {
                None => {
                    if def.defnamespace.is_some() {
                        continue;
                    }
                }
                Some(ns) => match &def.defnamespace {
                    None => continue,
                    Some(defns) => {
                        if defns != ns {
                            continue;
                        }
                    }
                },
            }

            // Flatten the DefElem into a text string like "name=arg". If we
            // have just "name", assume "name=true" is meant.
            let name = &def.defname;
            let value: String = if def.has_arg() {
                let s = backend_commands_define_seams::def_get_string::call(mcx, def.defname.clone(), def.arg.clone())?;
                s.as_str().to_string()
            } else {
                "true".to_string()
            };

            // Insist that name not contain "=", else "a=b=c" is ambiguous.
            if name.contains('=') {
                return Err(PgError::error(format!("invalid option name \"{name}\": must not contain \"=\"")).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }

            // Filter out WITH (oids = false); error on oids = true.
            if accept_oids_off && def.defnamespace.is_none() && name == "oids" {
                if backend_commands_define_seams::def_get_boolean::call(def.defname.clone(), def.arg.clone())? {
                    return Err(PgError::error("tables declared WITH OIDS are not supported").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
                }
                // skip over option, reloptions machinery doesn't know it
                continue;
            }

            astate.push(format!("{name}={value}"));
        }
    }

    // makeArrayResult / (Datum) 0.
    if astate.is_empty() {
        return Ok(None);
    }
    Ok(Some(astate))
}

/// `untransformRelOptions` -- convert the text-array format of reloptions into
/// a list of `DefElem`. This is the inverse of [`transformRelOptions`].
///
/// Returns `(defname, arg)` pairs; `arg` is `None` when the element had no `=`.
pub fn untransformRelOptions(mcx: Mcx<'_>, options: Option<&[u8]>) -> PgResult<Vec<(String, Option<String>)>> {
    let mut result: Vec<(String, Option<String>)> = Vec::new();

    // Nothing to do if no options.
    let options = match options {
        None => return Ok(result),
        Some(o) => o,
    };

    let optiondatums = backend_utils_adt_arrayfuncs_seams::deconstruct_text_array::call(mcx, options)?;
    for s in optiondatums.iter() {
        let s = s.as_str();
        match s.find('=') {
            Some(pos) => {
                result.push((s[..pos].to_string(), Some(s[pos + 1..].to_string())));
            }
            None => {
                result.push((s.to_string(), None));
            }
        }
    }
    Ok(result)
}

/// Identifies a `pg_class` tuple for [`extractRelOptions`]: its relkind and the
/// `reloptions` attribute (verbatim `text[]` varlena bytes, or `None` when
/// NULL).
pub struct ExtractRelOptionsInput<'a> {
    pub relkind: u8,
    pub reloptions: Option<&'a [u8]>,
}

/// `extractRelOptions` -- extract and parse reloptions from a `pg_class` tuple.
///
/// The C reads `Anum_pg_class_reloptions` (`fastgetattr`) and `classForm`
/// (`GETSTRUCT`); the caller supplies both as [`ExtractRelOptionsInput`]. The
/// relkind dispatch is identical to C. `amoptions` is the index AM's
/// option-parser function OID, required for index relkinds.
pub fn extractRelOptions(
    mcx: Mcx<'_>,
    input: &ExtractRelOptionsInput<'_>,
    amoptions: Option<types_core::Oid>,
) -> PgResult<Option<RelOptStruct>> {
    let datum = match input.reloptions {
        None => return Ok(None), // C: isnull -> return NULL
        Some(d) => d,
    };
    let relkind = input.relkind;

    // Parse into appropriate format; don't error out here.
    let options = match relkind {
        x if x == RELKIND_RELATION || x == RELKIND_TOASTVALUE || x == RELKIND_MATVIEW => {
            heap_reloptions(mcx, relkind, Some(datum), false)?
        }
        x if x == RELKIND_PARTITIONED_TABLE => partitioned_table_reloptions(Some(datum), false)?,
        x if x == RELKIND_VIEW => view_reloptions(mcx, Some(datum), false)?,
        x if x == RELKIND_INDEX || x == RELKIND_PARTITIONED_INDEX => {
            let amoptions = amoptions.expect("amoptions OID required for index relkind");
            index_reloptions(mcx, amoptions, Some(datum), false)?
        }
        x if x == RELKIND_FOREIGN_TABLE => None,
        _ => {
            debug_assert!(false, "can't get here"); // Assert(false)
            None
        }
    };
    Ok(options)
}

/// `parseRelOptionsInternal` -- shared core of [`parseRelOptions`] and
/// [`parseLocalRelOptions`]: match each `name=value` text element against the
/// expected-option set and parse it.
fn parseRelOptionsInternal(mcx: Mcx<'_>, options: &[u8], validate: bool, reloptions: &mut [RelOptValue]) -> PgResult<()> {
    let optiondatums = backend_utils_adt_arrayfuncs_seams::deconstruct_text_array::call(mcx, options)?;
    let numoptions = reloptions.len();

    for opt in optiondatums.iter() {
        let opt = opt.as_str();
        let text_str = opt.as_bytes();
        let text_len = text_str.len();

        // Search for a match in reloptions.
        let mut j = 0;
        while j < numoptions {
            let kw_len = reloptions[j].gen.namelen as usize;
            if text_len > kw_len && text_str[kw_len] == b'=' && text_str[..kw_len] == *reloptions[j].gen.name.as_bytes() {
                parse_one_reloption(&mut reloptions[j], opt, validate)?;
                break;
            }
            j += 1;
        }

        if j >= numoptions && validate {
            let s = match opt.find('=') {
                Some(pos) => &opt[..pos],
                None => opt,
            };
            return Err(PgError::error(format!("unrecognized parameter \"{s}\"")).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
    }
    Ok(())
}

/// `parseRelOptions` -- interpret reloptions given in text-array format against
/// the global parser table for the given `kind`.
pub fn parseRelOptions(mcx: Mcx<'_>, options: Option<&[u8]>, validate: bool, kind: relopt_kind) -> PgResult<Vec<RelOptValue>> {
    // Snapshot the expected options for this kind, then do the (possibly
    // error-raising) parsing.
    let mut reloptions: Vec<RelOptValue> = with_state(|state| {
        if state.need_initialization {
            initialize_reloptions(state);
        }
        let mut out = Vec::new();
        for gen in &state.rel_opts {
            if gen.kinds & (kind as bits32) != 0 {
                out.push(RelOptValue {
                    gen: Rc::clone(gen),
                    isset: false,
                    value: RelOptParsed::None,
                });
            }
        }
        out
    });

    // Done if no options.
    if let Some(options) = options {
        parseRelOptionsInternal(mcx, options, validate, &mut reloptions)?;
    }
    Ok(reloptions)
}

/// `parseLocalRelOptions` -- parse local unregistered options against a
/// [`LocalRelOpts`] set.
fn parseLocalRelOptions(mcx: Mcx<'_>, relopts: &LocalRelOpts, options: Option<&[u8]>, validate: bool) -> PgResult<Vec<RelOptValue>> {
    let mut values: Vec<RelOptValue> = relopts
        .options
        .iter()
        .map(|opt| RelOptValue {
            gen: Rc::clone(&opt.option),
            isset: false,
            value: RelOptParsed::None,
        })
        .collect();

    if let Some(options) = options {
        parseRelOptionsInternal(mcx, options, validate, &mut values)?;
    }
    Ok(values)
}

/// `parse_one_reloption` -- subroutine for [`parseRelOptions`]: parse and
/// validate a single option's value.
pub fn parse_one_reloption(option: &mut RelOptValue, text_str: &str, validate: bool) -> PgResult<()> {
    if option.isset && validate {
        return Err(PgError::error(format!("parameter \"{}\" specified more than once", option.gen.name)).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // value starts after "name="
    let value = &text_str[option.gen.namelen as usize + 1..];

    let parsed;
    let gen = Rc::clone(&option.gen);
    match &gen.data {
        RelOptData::Bool { .. } => {
            match builtin_parse_bool(value) {
                Some(v) => {
                    option.value = RelOptParsed::Bool(v);
                    parsed = true;
                }
                None => {
                    parsed = false;
                }
            }
            if validate && !parsed {
                return Err(PgError::error(format!("invalid value for boolean option \"{}\": {value}", gen.name)).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
        }
        RelOptData::Int { min, max, .. } => {
            let int_val;
            match backend_utils_misc_guc_seams::parse_int::call(value.to_string()) {
                Some(v) => {
                    int_val = v;
                    option.value = RelOptParsed::Int(v);
                    parsed = true;
                }
                None => {
                    int_val = 0;
                    parsed = false;
                }
            }
            if validate && !parsed {
                return Err(PgError::error(format!("invalid value for integer option \"{}\": {value}", gen.name)).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            if validate && (int_val < *min || int_val > *max) {
                return Err(PgError::error(format!("value {value} out of bounds for option \"{}\"", gen.name))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!("Valid values are between \"{min}\" and \"{max}\".")));
            }
        }
        RelOptData::Real { min, max, .. } => {
            let real_val;
            match backend_utils_misc_guc_seams::parse_real::call(value.to_string()) {
                Some(v) => {
                    real_val = v;
                    option.value = RelOptParsed::Real(v);
                    parsed = true;
                }
                None => {
                    real_val = 0.0;
                    parsed = false;
                }
            }
            if validate && !parsed {
                return Err(PgError::error(format!("invalid value for floating point option \"{}\": {value}", gen.name)).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            if validate && (real_val < *min || real_val > *max) {
                return Err(PgError::error(format!("value {value} out of bounds for option \"{}\"", gen.name))
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!("Valid values are between \"{}\" and \"{}\".", format_pg_double(*min), format_pg_double(*max))));
            }
        }
        RelOptData::Enum { members, default_val, detailmsg } => {
            let mut found = false;
            for elt in members {
                if pg_strcasecmp(value.as_bytes(), elt.string_val.as_bytes()) == 0 {
                    option.value = RelOptParsed::Enum(elt.symbol_val);
                    found = true;
                    break;
                }
            }
            if validate && !found {
                let mut err = PgError::error(format!("invalid value for enum option \"{}\": {value}", gen.name)).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE);
                if let Some(msg) = detailmsg {
                    err = err.with_detail(*msg);
                }
                return Err(err);
            }
            // If value is not among the allowed string values but we are not
            // asked to validate, just use the default numeric value.
            if !found {
                option.value = RelOptParsed::Enum(*default_val);
            }
            parsed = found;
        }
        RelOptData::Str { validate_cb, .. } => {
            option.value = RelOptParsed::String(value.to_string());
            if validate {
                if let Some(cb) = validate_cb {
                    cb(Some(value))?;
                }
            }
            parsed = true;
        }
    }

    if parsed {
        option.isset = true;
    }
    Ok(())
}

/// PostgreSQL's `errdetail("...%f...")` uses C `printf("%f")`, which renders a
/// double with six fractional digits.
fn format_pg_double(v: f64) -> String {
    format!("{v:.6}")
}

// ---------------------------------------------------------------------------
// Result-struct assembly: scalar resolution helpers (the typed equivalent of
// what fillRelOptions writes at the field offset).
// ---------------------------------------------------------------------------

fn opt_bool(opt: &RelOptValue) -> bool {
    if opt.isset {
        matches!(opt.value, RelOptParsed::Bool(true))
    } else if let RelOptData::Bool { default_val } = &opt.gen.data {
        *default_val
    } else {
        false
    }
}

fn opt_int(opt: &RelOptValue) -> i32 {
    let dflt = if let RelOptData::Int { default_val, .. } = &opt.gen.data { *default_val } else { 0 };
    if opt.isset {
        if let RelOptParsed::Int(i) = opt.value { i } else { dflt }
    } else {
        dflt
    }
}

fn opt_real(opt: &RelOptValue) -> f64 {
    let dflt = if let RelOptData::Real { default_val, .. } = &opt.gen.data { *default_val } else { 0.0 };
    if opt.isset {
        if let RelOptParsed::Real(r) = opt.value { r } else { dflt }
    } else {
        dflt
    }
}

fn opt_enum(opt: &RelOptValue) -> i32 {
    let dflt = if let RelOptData::Enum { default_val, .. } = &opt.gen.data { *default_val } else { 0 };
    if opt.isset {
        if let RelOptParsed::Enum(e) = opt.value { e } else { dflt }
    } else {
        dflt
    }
}

// ---------------------------------------------------------------------------
// Result-struct assembly (opaque byte-buffer path, local reloptions only)
// ---------------------------------------------------------------------------

/// `GET_STRING_RELOPTION_LEN` — length of a string reloption (default or
/// user-defined), used for allocation.
fn get_string_reloption_len(option: &RelOptValue) -> usize {
    match &option.value {
        RelOptParsed::String(s) if option.isset => s.len(),
        _ => match &option.gen.data {
            RelOptData::Str { default_len, .. } => *default_len as usize,
            _ => 0,
        },
    }
}

/// `allocateReloptStruct` -- allocate a struct of the specified base size plus
/// any extra space needed for string variables (`palloc0`).
fn allocateReloptStruct(mcx: Mcx<'_>, base: Size, options: &[RelOptValue]) -> PgResult<Vec<u8>> {
    let mut size = base;
    for optval in options {
        if optval.gen.opttype == RELOPT_TYPE_STRING {
            if let RelOptData::Str { fill_cb, default_isnull, default_val, .. } = &optval.gen.data {
                if let Some(fill_cb) = fill_cb {
                    let val: Option<&str> = if optval.isset {
                        match &optval.value {
                            RelOptParsed::String(s) => Some(s.as_str()),
                            _ => None,
                        }
                    } else if *default_isnull {
                        None
                    } else {
                        default_val.as_deref()
                    };
                    size += fill_cb(val, None);
                } else {
                    size += get_string_reloption_len(optval) + 1;
                }
            }
        }
    }
    let mut buf: Vec<u8> = Vec::new();
    buf.try_reserve(size).map_err(|_| mcx.oom(size))?;
    buf.resize(size, 0u8);
    Ok(buf)
}

/// `fillRelOptions` -- copy parsed values into the result byte buffer per the
/// parse-element table, running fill callbacks for strings, then `SET_VARSIZE`.
/// Used by the local-reloption path only.
fn fillRelOptions(rdopts: &mut [u8], basesize: Size, options: &[RelOptValue], validate: bool, elems: &[RelOptParseElt]) -> PgResult<()> {
    let mut offset = basesize;
    for opt in options {
        let mut found = false;
        for elem in elems {
            if opt.gen.name == elem.optname {
                let itempos = elem.offset as usize;
                // If isset_offset is provided, store whether the reloption is set.
                if elem.isset_offset > 0 {
                    let setpos = elem.isset_offset as usize;
                    rdopts[setpos] = u8::from(opt.isset);
                }
                match &opt.gen.data {
                    RelOptData::Bool { .. } => {
                        rdopts[itempos] = u8::from(opt_bool(opt));
                    }
                    RelOptData::Int { .. } => {
                        rdopts[itempos..itempos + 4].copy_from_slice(&opt_int(opt).to_ne_bytes());
                    }
                    RelOptData::Real { .. } => {
                        rdopts[itempos..itempos + 8].copy_from_slice(&opt_real(opt).to_ne_bytes());
                    }
                    RelOptData::Enum { .. } => {
                        rdopts[itempos..itempos + 4].copy_from_slice(&opt_enum(opt).to_ne_bytes());
                    }
                    RelOptData::Str { fill_cb, default_isnull, default_val, .. } => {
                        let string_val: Option<&str> = if opt.isset {
                            match &opt.value {
                                RelOptParsed::String(s) => Some(s.as_str()),
                                _ => None,
                            }
                        } else if !*default_isnull {
                            default_val.as_deref()
                        } else {
                            None
                        };
                        if let Some(fill_cb) = fill_cb {
                            let size = {
                                let buf = &mut rdopts[offset..];
                                fill_cb(string_val, Some(buf))
                            };
                            if size != 0 {
                                rdopts[itempos..itempos + 4].copy_from_slice(&(offset as i32).to_ne_bytes());
                                offset += size;
                            } else {
                                rdopts[itempos..itempos + 4].copy_from_slice(&0i32.to_ne_bytes());
                            }
                        } else if let Some(s) = string_val {
                            let bytes = s.as_bytes();
                            rdopts[offset..offset + bytes.len()].copy_from_slice(bytes);
                            rdopts[offset + bytes.len()] = 0; // strcpy NUL
                            rdopts[itempos..itempos + 4].copy_from_slice(&(offset as i32).to_ne_bytes());
                            offset += bytes.len() + 1;
                        } else {
                            rdopts[itempos..itempos + 4].copy_from_slice(&0i32.to_ne_bytes());
                        }
                    }
                }
                found = true;
                break;
            }
        }
        if validate && !found {
            return Err(PgError::error(format!("reloption \"{}\" not found in parse table", opt.gen.name)));
        }
    }
    // SET_VARSIZE(rdopts, offset)
    set_varsize(rdopts, offset);
    Ok(())
}

/// `SET_VARSIZE` for a 4-byte (non-short, non-compressed) varlena header.
fn set_varsize(buf: &mut [u8], len: usize) {
    let header = (len as u32) << 2;
    buf[..VARHDRSZ].copy_from_slice(&header.to_ne_bytes());
}

// ---------------------------------------------------------------------------
// Public per-relation-kind entry points
// ---------------------------------------------------------------------------

/// `default_reloptions` -- option parser for anything that uses `StdRdOptions`.
pub fn default_reloptions(mcx: Mcx<'_>, reloptions: Option<&[u8]>, validate: bool, kind: relopt_kind) -> PgResult<Option<RelOptStruct>> {
    let options = parseRelOptions(mcx, reloptions, validate, kind)?;

    // if none set, we're done
    if options.is_empty() {
        return Ok(None);
    }

    let mut s = StdRdOptions::default();
    let mut av = AutoVacOpts::default();

    for opt in &options {
        match opt.gen.name.as_str() {
            "fillfactor" => s.fillfactor = opt_int(opt),
            "autovacuum_enabled" => av.enabled = opt_bool(opt),
            "autovacuum_vacuum_threshold" => av.vacuum_threshold = opt_int(opt),
            "autovacuum_vacuum_max_threshold" => av.vacuum_max_threshold = opt_int(opt),
            "autovacuum_vacuum_insert_threshold" => av.vacuum_ins_threshold = opt_int(opt),
            "autovacuum_analyze_threshold" => av.analyze_threshold = opt_int(opt),
            "autovacuum_vacuum_cost_limit" => av.vacuum_cost_limit = opt_int(opt),
            "autovacuum_freeze_min_age" => av.freeze_min_age = opt_int(opt),
            "autovacuum_freeze_max_age" => av.freeze_max_age = opt_int(opt),
            "autovacuum_freeze_table_age" => av.freeze_table_age = opt_int(opt),
            "autovacuum_multixact_freeze_min_age" => av.multixact_freeze_min_age = opt_int(opt),
            "autovacuum_multixact_freeze_max_age" => av.multixact_freeze_max_age = opt_int(opt),
            "autovacuum_multixact_freeze_table_age" => av.multixact_freeze_table_age = opt_int(opt),
            "log_autovacuum_min_duration" => av.log_min_duration = opt_int(opt),
            "toast_tuple_target" => s.toast_tuple_target = opt_int(opt),
            "autovacuum_vacuum_cost_delay" => av.vacuum_cost_delay = opt_real(opt),
            "autovacuum_vacuum_scale_factor" => av.vacuum_scale_factor = opt_real(opt),
            "autovacuum_vacuum_insert_scale_factor" => av.vacuum_ins_scale_factor = opt_real(opt),
            "autovacuum_analyze_scale_factor" => av.analyze_scale_factor = opt_real(opt),
            "user_catalog_table" => s.user_catalog_table = opt_bool(opt),
            "parallel_workers" => s.parallel_workers = opt_int(opt),
            "vacuum_index_cleanup" => s.vacuum_index_cleanup = opt_enum(opt) as StdRdOptIndexCleanup,
            "vacuum_truncate" => {
                // The C parse table records an isset offset for this option.
                s.vacuum_truncate_set = opt.isset;
                s.vacuum_truncate = opt_bool(opt);
            }
            "vacuum_max_eager_freeze_failure_rate" => s.vacuum_max_eager_freeze_failure_rate = opt_real(opt),
            // Any option the kind admits that is not in the StdRdOptions parse
            // table: C's fillRelOptions errors only under validate.
            other => {
                if validate {
                    return Err(PgError::error(format!("reloption \"{other}\" not found in parse table")));
                }
            }
        }
    }

    s.autovacuum = av;
    Ok(Some(RelOptStruct::Std(s)))
}

/// `build_reloptions` -- generic builder for callers (e.g. index AMs) that fill
/// an opaque caller-defined bytea described by a byte-offset `relopt_parse_elt`
/// table. Returns `None` if there were no options of the given kind (unless
/// `validate`).
pub fn build_reloptions(mcx: Mcx<'_>, reloptions: Option<&[u8]>, validate: bool, kind: relopt_kind, relopt_struct_size: Size, relopt_elems: &[RelOptParseElt]) -> PgResult<Option<Vec<u8>>> {
    let options = parseRelOptions(mcx, reloptions, validate, kind)?;
    debug_assert!(options.len() <= relopt_elems.len());

    if options.is_empty() {
        return Ok(None);
    }

    let mut rdopts = allocateReloptStruct(mcx, relopt_struct_size, &options)?;
    fillRelOptions(&mut rdopts, relopt_struct_size, &options, validate, relopt_elems)?;
    Ok(Some(rdopts))
}

/// `build_local_reloptions` -- parse local options and build a bytea struct,
/// running registered validators.
pub fn build_local_reloptions(mcx: Mcx<'_>, relopts: &LocalRelOpts, options: Option<&[u8]>, validate: bool) -> PgResult<Vec<u8>> {
    let elems: Vec<RelOptParseElt> = relopts
        .options
        .iter()
        .map(|opt| RelOptParseElt {
            optname: opt.option.name.clone(),
            opttype: opt.option.opttype,
            offset: opt.offset,
            isset_offset: 0,
        })
        .collect();

    let vals = parseLocalRelOptions(mcx, relopts, options, validate)?;
    let mut opts = allocateReloptStruct(mcx, relopts.relopt_struct_size, &vals)?;
    fillRelOptions(&mut opts, relopts.relopt_struct_size, &vals, validate, &elems)?;

    if validate {
        for validator in &relopts.validators {
            validator(&mut opts, &vals)?;
        }
    }
    Ok(opts)
}

/// `partitioned_table_reloptions` -- partitioned tables accept no reloptions;
/// only validate emptiness.
pub fn partitioned_table_reloptions(reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<RelOptStruct>> {
    if validate && reloptions.is_some() {
        return Err(PgError::error("cannot specify storage parameters for a partitioned table")
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_hint("Specify storage parameters for its leaf partitions instead."));
    }
    Ok(None)
}

/// `view_reloptions` -- option parser for views.
pub fn view_reloptions(mcx: Mcx<'_>, reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<RelOptStruct>> {
    let options = parseRelOptions(mcx, reloptions, validate, RELOPT_KIND_VIEW)?;
    if options.is_empty() {
        return Ok(None);
    }
    let mut v = ViewOptions::default();
    for opt in &options {
        match opt.gen.name.as_str() {
            "security_barrier" => v.security_barrier = opt_bool(opt),
            "security_invoker" => v.security_invoker = opt_bool(opt),
            "check_option" => v.check_option = opt_enum(opt) as ViewOptCheckOption,
            other => {
                if validate {
                    return Err(PgError::error(format!("reloption \"{other}\" not found in parse table")));
                }
            }
        }
    }
    Ok(Some(RelOptStruct::View(v)))
}

/// `heap_reloptions` -- parse options for heaps, materialized views and toast
/// tables by relkind.
pub fn heap_reloptions(mcx: Mcx<'_>, relkind: u8, reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<RelOptStruct>> {
    match relkind {
        x if x == RELKIND_TOASTVALUE => {
            let mut rdopts = default_reloptions(mcx, reloptions, validate, RELOPT_KIND_TOAST)?;
            if let Some(RelOptStruct::Std(s)) = rdopts.as_mut() {
                // adjust default-only parameters for TOAST relations
                s.fillfactor = 100;
                s.autovacuum.analyze_threshold = -1;
                s.autovacuum.analyze_scale_factor = -1.0;
            }
            Ok(rdopts)
        }
        x if x == RELKIND_RELATION || x == RELKIND_MATVIEW => default_reloptions(mcx, reloptions, validate, RELOPT_KIND_HEAP),
        // other relkinds are not supported
        _ => Ok(None),
    }
}

/// `index_reloptions` -- parse options for indexes by dispatching to the access
/// method's `amoptions` callback (assumed strict).
pub fn index_reloptions(mcx: Mcx<'_>, amoptions: types_core::Oid, reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<RelOptStruct>> {
    // Assume function is strict.
    let reloptions = match reloptions {
        None => return Ok(None),
        Some(r) => r,
    };
    let bytea = backend_access_index_amapi_seams::am_reloptions::call(mcx, amoptions, reloptions, validate)?;
    Ok(bytea.map(|v| RelOptStruct::Bytea(v.to_vec())))
}

/// `attribute_reloptions` -- option parser for attribute reloptions
/// (`pg_attribute.attoptions`). The C uses `build_reloptions` with the
/// `relopt_parse_elt` table `{n_distinct REAL, n_distinct_inherited REAL}`;
/// the owned model fills the typed [`AttributeOpts`] keyed by option name.
pub fn attribute_reloptions(mcx: Mcx<'_>, reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<AttributeOpts>> {
    let options = parseRelOptions(mcx, reloptions, validate, RELOPT_KIND_ATTRIBUTE)?;
    if options.is_empty() {
        return Ok(None);
    }
    let mut a = AttributeOpts::default();
    for opt in &options {
        match opt.gen.name.as_str() {
            "n_distinct" => a.n_distinct = opt_real(opt),
            "n_distinct_inherited" => a.n_distinct_inherited = opt_real(opt),
            other => {
                if validate {
                    return Err(PgError::error(format!("reloption \"{other}\" not found in parse table")));
                }
            }
        }
    }
    Ok(Some(a))
}

/// `tablespace_reloptions` -- option parser for tablespace reloptions
/// (`pg_tablespace.spcoptions`). C parse table:
/// `{random_page_cost REAL, seq_page_cost REAL, effective_io_concurrency INT,
/// maintenance_io_concurrency INT}`.
pub fn tablespace_reloptions(mcx: Mcx<'_>, reloptions: Option<&[u8]>, validate: bool) -> PgResult<Option<TableSpaceOpts>> {
    let options = parseRelOptions(mcx, reloptions, validate, RELOPT_KIND_TABLESPACE)?;
    if options.is_empty() {
        return Ok(None);
    }
    let mut t = TableSpaceOpts::default();
    for opt in &options {
        match opt.gen.name.as_str() {
            "random_page_cost" => t.random_page_cost = opt_real(opt),
            "seq_page_cost" => t.seq_page_cost = opt_real(opt),
            "effective_io_concurrency" => t.effective_io_concurrency = opt_int(opt),
            "maintenance_io_concurrency" => t.maintenance_io_concurrency = opt_int(opt),
            other => {
                if validate {
                    return Err(PgError::error(format!("reloption \"{other}\" not found in parse table")));
                }
            }
        }
    }
    Ok(Some(t))
}

/// `AlterTableGetRelOptionsLockLevel` -- determine the required `LOCKMODE` from
/// an option list.
pub fn AlterTableGetRelOptionsLockLevel(def_list: &[DefElem]) -> LOCKMODE {
    let mut lockmode = NoLock;

    if def_list.is_empty() {
        return AccessExclusiveLock;
    }

    with_state(|state| {
        if state.need_initialization {
            initialize_reloptions(state);
        }
        for def in def_list {
            for gen in &state.rel_opts {
                // strncmp(relOpts[i]->name, def->defname, namelen + 1) == 0
                // (an exact match including the terminating NUL).
                if gen.name == def.defname && lockmode < gen.lockmode {
                    lockmode = gen.lockmode;
                }
            }
        }
    });
    lockmode
}

// ---------------------------------------------------------------------------
// Seam installation
// ---------------------------------------------------------------------------

/// Seam target for `extractRelOptions(tuple, GetPgClassDescriptor(), amoptsfn)`
/// driven by `RelationParseRelOptions` (relcache.c). Parses the relation's
/// `pg_class.reloptions` into the relcache's parsed-options struct.
///
/// `extractRelOptions` returns the full per-relkind [`RelOptStruct`]; the
/// relcache `rd_options` carries only the `Std` arm (`StdRdOptions`), which is
/// what the table / toast / matview / partitioned-table relkinds produce. The
/// view (`RelOptStruct::View`) and AM-defined index (`RelOptStruct::Bytea`)
/// arms are not modeled by the trimmed `rd_options`, so they map to `None`
/// (the relcache leaves `rd_options` NULL for them) — matching the model's
/// existing reloptions trim. A scratch context holds the transient parse
/// allocations (C parses in the caller's context and copies into
/// `CacheMemoryContext`; the owned value is returned by value).
fn extract_rel_options_seam(
    relkind: u8,
    reloptions: Option<&[u8]>,
    amoptions: Option<types_core::Oid>,
) -> PgResult<Option<StdRdOptions>> {
    let scratch = mcx::MemoryContext::new("RelationParseRelOptions");
    let input = ExtractRelOptionsInput { relkind, reloptions };
    let parsed = extractRelOptions(scratch.mcx(), &input, amoptions)?;
    Ok(match parsed {
        Some(RelOptStruct::Std(s)) => Some(s),
        // View/Attribute/TableSpace/Bytea are not carried by the trimmed
        // rd_options (Option<StdRdOptions>); the relcache leaves rd_options NULL.
        _ => None,
    })
}

/// Seam target for `attribute_reloptions(reloptions, validate)`: the consumers
/// (`attoptcache.c`) only call with a non-null datum, where C always returns a
/// non-null bytea (the ATTRIBUTE kind always has registered options). A scratch
/// context holds the transient parse allocations.
fn attribute_reloptions_seam(reloptions: &[u8], validate: bool) -> PgResult<AttributeOpts> {
    let scratch = mcx::MemoryContext::new("attribute_reloptions");
    let opts = attribute_reloptions(scratch.mcx(), Some(reloptions), validate)?;
    // C returns the (always non-null) bytea for a non-null datum of this kind.
    Ok(opts.unwrap_or_default())
}

/// Seam target for `hashoptions(reloptions, validate)` (hashutil.c) — the hash
/// AM's `amoptions` callback. Mirrors the C:
/// `build_reloptions(reloptions, validate, RELOPT_KIND_HASH,
///  sizeof(HashOptions), tab, lengthof(tab))` where `tab` has the single entry
/// Seam target for `btoptions(reloptions, validate)` (nbtutils.c) — the B-tree
/// AM's `amoptions` callback. Mirrors the C:
/// `build_reloptions(reloptions, validate, RELOPT_KIND_BTREE,
///  sizeof(BTOptions), tab, lengthof(tab))` where `tab` is
/// `{{"fillfactor", RELOPT_TYPE_INT, offsetof(BTOptions, fillfactor)},
///   {"vacuum_cleanup_index_scale_factor", RELOPT_TYPE_REAL,
///    offsetof(BTOptions, vacuum_cleanup_index_scale_factor)},
///   {"deduplicate_items", RELOPT_TYPE_BOOL,
///    offsetof(BTOptions, deduplicate_items)}}`.
///
/// `BTOptions` is `{ int32 varlena_header_; int fillfactor;
/// float8 vacuum_cleanup_index_scale_factor; bool deduplicate_items; }`. With
/// the `float8` 8-byte alignment, `fillfactor` is at offset 4,
/// `vacuum_cleanup_index_scale_factor` at offset 8, `deduplicate_items` at
/// offset 16, and the struct size (padded to the 8-byte alignment) is 24.
fn build_reloptions_btree_seam(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let scratch = mcx::MemoryContext::new("btoptions");
    let tab = [
        RelOptParseElt::new("fillfactor", RELOPT_TYPE_INT, 4),
        RelOptParseElt::new("vacuum_cleanup_index_scale_factor", RELOPT_TYPE_REAL, 8),
        RelOptParseElt::new("deduplicate_items", RELOPT_TYPE_BOOL, 16),
    ];
    build_reloptions(scratch.mcx(), reloptions, validate, RELOPT_KIND_BTREE, 24, &tab)
}

/// `{"fillfactor", RELOPT_TYPE_INT, offsetof(HashOptions, fillfactor)}`.
///
/// `HashOptions` is `{ int32 varlena_header_; int fillfactor; }`, so its size is
/// 8 and `fillfactor` is at offset 4.
fn build_reloptions_hash_seam(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let scratch = mcx::MemoryContext::new("hashoptions");
    let tab = [RelOptParseElt::new("fillfactor", RELOPT_TYPE_INT, 4)];
    build_reloptions(scratch.mcx(), reloptions, validate, RELOPT_KIND_HASH, 8, &tab)
}

/// Seam target for `spgoptions(reloptions, validate)` (spgutils.c) — the
/// SP-GiST AM's `amoptions` callback. Mirrors the C:
/// `build_reloptions(reloptions, validate, RELOPT_KIND_SPGIST,
///  sizeof(SpGistOptions), tab, lengthof(tab))` where `tab` has the single
/// entry `{"fillfactor", RELOPT_TYPE_INT, offsetof(SpGistOptions, fillfactor)}`.
///
/// `SpGistOptions` is `{ int32 varlena_header_; int fillfactor; }`, so its size
/// is 8 and `fillfactor` is at offset 4 — same layout as `HashOptions`.
fn build_reloptions_spgist_seam(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let scratch = mcx::MemoryContext::new("spgoptions");
    let tab = [RelOptParseElt::new("fillfactor", RELOPT_TYPE_INT, 4)];
    build_reloptions(scratch.mcx(), reloptions, validate, RELOPT_KIND_SPGIST, 8, &tab)
}

/// Seam target for `gistoptions(reloptions, validate)` (gistutil.c) — the
/// GiST AM's `amoptions` callback. Mirrors the C:
/// `build_reloptions(reloptions, validate, RELOPT_KIND_GIST,
///  sizeof(GiSTOptions), tab, lengthof(tab))` where `tab` has the two entries
/// `{"fillfactor", RELOPT_TYPE_INT, offsetof(GiSTOptions, fillfactor)}` and
/// `{"buffering", RELOPT_TYPE_ENUM, offsetof(GiSTOptions, buffering_mode)}`.
///
/// `GiSTOptions` is `{ int32 varlena_header_; int fillfactor;
/// GistOptBufferingMode buffering_mode; }`, so its size is 12, `fillfactor` is
/// at offset 4 and `buffering_mode` (an `int`-sized enum) is at offset 8.
fn build_reloptions_gist_seam(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let scratch = mcx::MemoryContext::new("gistoptions");
    let tab = [
        RelOptParseElt::new("fillfactor", RELOPT_TYPE_INT, 4),
        RelOptParseElt::new("buffering", RELOPT_TYPE_ENUM, 8),
    ];
    build_reloptions(scratch.mcx(), reloptions, validate, RELOPT_KIND_GIST, 12, &tab)
}

/// Seam target for `brinoptions(reloptions, validate)` (brin.c) — the BRIN AM's
/// `amoptions` callback. Mirrors the C:
/// `build_reloptions(reloptions, validate, RELOPT_KIND_BRIN,
///  sizeof(BrinOptions), tab, lengthof(tab))` where `tab` has the entries
/// `{"pages_per_range", RELOPT_TYPE_INT, offsetof(BrinOptions, pagesPerRange)}`
/// and `{"autosummarize", RELOPT_TYPE_BOOL, offsetof(BrinOptions,
/// autosummarize)}`.
///
/// `BrinOptions` is `{ int32 vl_len_; BlockNumber pagesPerRange; bool
/// autosummarize; }`, so its size is 12, `pagesPerRange` is at offset 4 and
/// `autosummarize` at offset 8.
fn build_reloptions_brin_seam(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let scratch = mcx::MemoryContext::new("brinoptions");
    let tab = [
        RelOptParseElt::new("pages_per_range", RELOPT_TYPE_INT, 4),
        RelOptParseElt::new("autosummarize", RELOPT_TYPE_BOOL, 8),
    ];
    build_reloptions(scratch.mcx(), reloptions, validate, RELOPT_KIND_BRIN, 12, &tab)
}

/// Seam target for `tablespace_reloptions(reloptions, validate)` (see
/// [`attribute_reloptions_seam`]).
fn tablespace_reloptions_seam(reloptions: &[u8], validate: bool) -> PgResult<TableSpaceOpts> {
    let scratch = mcx::MemoryContext::new("tablespace_reloptions");
    let opts = tablespace_reloptions(scratch.mcx(), Some(reloptions), validate)?;
    Ok(opts.unwrap_or_default())
}

/// Seam target for `init_local_reloptions(relopts, relopt_struct_size)`.
///
/// Operates directly on the shared `types_reloptions::local_relopts` (the
/// cross-crate seam type). Lossless, exactly mirroring C: clear the
/// option/validator lists and record the struct size.
fn init_local_reloptions_seam(
    relopts: &mut types_reloptions::local_relopts,
    relopt_struct_size: usize,
) {
    relopts.options.clear();
    relopts.validators.clear();
    relopts.relopt_struct_size = relopt_struct_size;
}

/// Seam target for `add_local_int_reloption(relopts, name, desc, default, min,
/// max, offset)`.
///
/// Operates on the shared `types_reloptions::local_relopts`, mirroring the C:
/// `init_int_reloption(RELOPT_KIND_LOCAL, ...)` builds a `relopt_int` whose
/// `relopt_gen` tail carries `default_val`/`min`/`max`, then
/// `add_local_reloption` appends it at `offset`. The range/default are stored on
/// the option's [`types_reloptions::relopt_typed::Int`] payload, so nothing is
/// dropped at the seam boundary.
fn add_local_int_reloption_seam(
    relopts: &mut types_reloptions::local_relopts,
    name: &str,
    desc: Option<&str>,
    default_val: i32,
    min_val: i32,
    max_val: i32,
    offset: i32,
) {
    let newoption = types_reloptions::relopt_gen {
        name: Some(name.to_string()),
        desc: desc.map(|d| d.to_string()),
        kinds: RELOPT_KIND_LOCAL as types_reloptions::bits32,
        lockmode: 0,
        namelen: name.len() as i32,
        type_: types_reloptions::relopt_type::RELOPT_TYPE_INT,
        data: types_reloptions::relopt_typed::Int {
            default_val,
            min: min_val,
            max: max_val,
        },
    };
    debug_assert!((offset as usize) < relopts.relopt_struct_size);
    relopts.options.push(types_reloptions::local_relopt {
        option: Some(Box::new(newoption)),
        offset,
    });
}

/// Install every seam this crate owns.
pub fn init_seams() {
    backend_access_common_reloptions_seams::extract_rel_options::set(extract_rel_options_seam);
    backend_access_common_reloptions_seams::attribute_reloptions::set(attribute_reloptions_seam);
    backend_access_common_reloptions_seams::tablespace_reloptions::set(tablespace_reloptions_seam);
    backend_access_common_reloptions_seams::init_local_reloptions::set(init_local_reloptions_seam);
    backend_access_common_reloptions_seams::add_local_int_reloption::set(
        add_local_int_reloption_seam,
    );
    backend_access_common_reloptions_seams::build_reloptions_btree::set(build_reloptions_btree_seam);
    backend_access_common_reloptions_seams::build_reloptions_hash::set(build_reloptions_hash_seam);
    backend_access_common_reloptions_seams::build_reloptions_spgist::set(
        build_reloptions_spgist_seam,
    );
    backend_access_common_reloptions_seams::build_reloptions_gist::set(build_reloptions_gist_seam);
    backend_access_common_reloptions_seams::build_reloptions_brin::set(build_reloptions_brin_seam);
}
