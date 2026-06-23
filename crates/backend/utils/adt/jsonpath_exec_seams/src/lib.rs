//! Seam declarations for the `backend-utils-adt-jsonpath-exec` unit
//! (`utils/adt/jsonpath_exec.c`): the jsonpath_exec-specific externals whose
//! signatures carry this unit's local node/value types — the `JsonItemFromDatum`
//! `Datum`→`JsonbValue` coercions. The JSON_TABLE plan vocabulary
//! (`JsonTablePlan`/`JsonTableVariable`) is also defined here so consumers can
//! name it without depending on the `types-*` stack.
//!
//! The datetime substrate (`parse_datetime` / `compareDatetime` / the
//! `executeDateTimeMethod` cast switch) is NOT seamed — jsonpath_exec.c is a
//! leaf adt unit, so it calls the real ported `backend-utils-adt-formatting`
//! and `backend-utils-adt-datetime` casts/comparators in-crate, exactly as C
//! reaches them through `DirectFunctionCall*`. Only the shared
//! [`DateTimeValue`] carrier remains here.
//!
//! Genuine externals owned by other subsystems are reached through their own
//! owner-seams crates, not declared here: `RE_compile_and_execute`
//! (regexp-seams), `format_type_be` (format-type-seams), `pg_server_to_any` /
//! `GetDatabaseEncoding` (mbutils-seams), `JsonEncodeDateTime` (json-seams),
//! and `jspConvertRegexFlags` (the jsonpath type crate, a direct dep).
//!
//! Each remaining seam defaults to a loud panic until its owning unit installs
//! it.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::Oid;
use datum::Datum;

// The `compareDatetime` cross-type comparison, the `parse_datetime` text
// parser, and the `executeDateTimeMethod` cast switch are no longer seamed:
// they are implemented in-crate (jsonpath_exec.c is a leaf adt unit) against
// the real ported `backend-utils-adt-formatting` (`parse_datetime`) and
// `backend-utils-adt-datetime` (date/time/timestamp casts + comparators),
// exactly as C reaches them through `DirectFunctionCall*`. Only the shared
// [`DateTimeValue`] carrier remains here.

// The `DirectInputFunctionCallSafe(int4in/int8in/numeric_in, ...)` /
// `float8in_internal` / `parse_bool` soft-parse calls of the item methods are
// jsonpath_exec.c-internal: each calls its owning adt unit's real input
// function directly (a leaf adt dep, no seam, mirroring `numeric_*`), so they
// are not declared as seams here.

// `JsonItemFromDatum` (coerce a PASSING variable's SQL value into a
// `JsonbValue`) is no longer a seam: jsonpath_exec.c is a leaf adt unit, so the
// numeric/int/float coercions (`backend-utils-adt-numeric`), the text/varchar
// `VARDATA_ANY` arm, and the jsonb/json arms (`JsonbExtractScalar`/
// `JsonbInitBinary`/`jsonb_in`) are all reached in-crate. The bound value's
// by-reference varlena image is carried out-of-band on `JsonPathVariable` /
// `JsonTableVariable` (`value_bytes`), so no `Datum`-pointer detoast seam is
// needed.

// `check_stack_depth()` (utils/misc/stack_depth.c) and `CHECK_FOR_INTERRUPTS()`
// (miscadmin.h, serviced via tcop/postgres.c) are genuine cross-subsystem
// externals owned elsewhere; jsonpath_exec.c reaches them through their
// canonical owner-seams (`backend-utils-misc-stack-depth-seams` /
// `backend-tcop-postgres-seams`), not a private re-declaration here.

/// A datetime SQL/JSON value as carried by `jbvDatetime`: a `Datum`, its type
/// OID, the typmod, and the numeric timezone (seconds).
///
/// The datetime `Datum` is a by-value machine word (date = `int32`,
/// time/timestamp/timestamptz = `int64`); a `timetz`'s by-reference
/// `{ TimeADT time, int32 zone }` is carried losslessly as `value = time`,
/// `tz = zone`.
#[derive(Clone, Copy, Debug)]
pub struct DateTimeValue {
    pub value: Datum,
    pub typid: Oid,
    pub typmod: i32,
    pub tz: i32,
}

// ---------------------------------------------------------------------------
// JSON_TABLE executor/nodes boundary (jsonpath_exec.c:4082-4493).
// ---------------------------------------------------------------------------

/// A `JsonTablePathScan` plan node's relevant fields (C: `struct
/// JsonTablePathScan`).
#[derive(Clone, Debug)]
pub struct JsonTablePathScan {
    /// The compiled jsonpath (full on-disk `jsonpath` varlena bytes).
    pub path: Vec<u8>,
    /// `scan->errorOnError`.
    pub error_on_error: bool,
    /// `scan->colMin` (inclusive).
    pub col_min: i32,
    /// `scan->colMax` (inclusive).
    pub col_max: i32,
    /// `scan->child` plan, if any.
    pub child: core::option::Option<Box<JsonTablePlan>>,
}

/// A `JsonTableSiblingJoin` plan node (C: `struct JsonTableSiblingJoin`).
#[derive(Clone, Debug)]
pub struct JsonTableSiblingJoin {
    /// `join->lplan`.
    pub lplan: Box<JsonTablePlan>,
    /// `join->rplan`.
    pub rplan: Box<JsonTablePlan>,
}

/// A JSON_TABLE plan node (C: `JsonTablePlan`, a tagged union of the two kinds).
#[derive(Clone, Debug)]
pub enum JsonTablePlan {
    /// C: `JsonTablePathScan`.
    PathScan(JsonTablePathScan),
    /// C: `JsonTableSiblingJoin`.
    SiblingJoin(JsonTableSiblingJoin),
}

/// A bound jsonpath PASSING variable surfaced by the JSON_TABLE provider.
///
/// As with `JsonPathVariable`, the bound value is split: a pass-by-value type
/// rides in `value` as a bare machine word, and a pass-by-reference type
/// (`numeric`/`text`/`varchar`/`jsonb`/`json`) carries its full header-ful
/// varlena image in `value_bytes` (`None` for by-value types).
#[derive(Clone, Debug)]
pub struct JsonTableVariable {
    pub name: Vec<u8>,
    pub typid: Oid,
    pub typmod: i32,
    pub value: Datum,
    pub value_bytes: core::option::Option<Vec<u8>>,
    pub isnull: bool,
}

// The JSON_TABLE executor/`ExprState` boundary is NOT a seam: the executor
// (`backend-executor-nodeTableFuncscan`) depends on the jsonpath_exec crate
// directly (no cycle — jsonpath_exec is a leaf adt unit) and orchestrates the
// JSON_TABLE row-pattern builder. It builds the root [`JsonTablePlan`] from
// `tf->plan`, evaluates the PASSING / column `JsonExpr` expressions
// (`ExecEvalExpr`) itself, and calls jsonpath_exec's `JsonTableInitOpaque`
// (plan + PASSING vars + column count) and `JsonTableCurrentRow` (row-pattern
// bytes + ordinal) entry points. So `init_table_func` / `eval_column` callback
// seams are not needed; only the shared plan vocabulary below is.
