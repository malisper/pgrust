//! Seam declarations for the `backend-utils-adt-jsonpath-exec` unit
//! (`utils/adt/jsonpath_exec.c`): the jsonpath_exec-specific externals whose
//! signatures carry this unit's local node/value types or that are
//! jsonpath_exec-private soft-parse wrappers — `parse_datetime`,
//! `datetime_method_cast`, `compare_datetime`, the fmgr type-input soft-parse
//! helpers (`int4in`/`int8in`/`float8in_internal`/`parse_bool`/
//! `numeric_in_with_typmod`), the `JsonItemFromDatum` `Datum`→`JsonbValue`
//! coercions, the recursion/interrupt guards, and the JSON_TABLE
//! executor/`ExprState`/node boundary (`init_table_func`/`eval_column`).
//!
//! Genuine externals owned by other subsystems are reached through their own
//! owner-seams crates, not declared here: `RE_compile_and_execute`
//! (regexp-seams), `format_type_be` (format-type-seams), `pg_server_to_any` /
//! `GetDatabaseEncoding` (mbutils-seams), `JsonEncodeDateTime` (json-seams),
//! and `jspConvertRegexFlags` (the jsonpath type crate, a direct dep).
//!
//! Each defaults to a loud panic until its owning unit installs it. The three
//! seams whose signatures carry this module's local node-tree types
//! (`DateTimeValue` / `JsonTablePlan` / `JsonTableVariable`) — `parse_datetime`,
//! `datetime_method_cast`, `init_table_func` — are declared here because those
//! types are not in the layered `types-*` stack.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;

use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_jsonb::backend_utils_adt_jsonb_util::JsonbValue;

seam_core::seam!(
    /// C: `compareDatetime(Datum val1, Oid typid1, Datum val2, Oid typid2,
    /// bool useTz, bool *cast_error)` (jsonpath_exec.c) — cross-type comparison
    /// of two datetime SQL/JSON items. Returns `Ok(None)` when the items are
    /// uncomparable (`*cast_error = true`), else `Ok(Some(cmp))`.
    pub fn compare_datetime(
        val1: Datum,
        typid1: Oid,
        val2: Datum,
        typid2: Oid,
        use_tz: bool
    ) -> PgResult<core::option::Option<i32>>
);

seam_core::seam!(
    /// C: `DirectInputFunctionCallSafe(int4in, str, ...)` — parse `str` as
    /// `int4`, returning `Ok(None)` on a soft error.
    pub fn int4in(str: String) -> PgResult<core::option::Option<i32>>
);

seam_core::seam!(
    /// C: `DirectInputFunctionCallSafe(int8in, str, ...)` — parse `str` as
    /// `int8`, returning `Ok(None)` on a soft error.
    pub fn int8in(str: String) -> PgResult<core::option::Option<i64>>
);

seam_core::seam!(
    /// C: `float8in_internal(str, NULL, "double precision", str, escontext)` —
    /// parse `str` as `float8`, returning `Ok(None)` on a soft error.
    pub fn float8in_internal(str: String) -> PgResult<core::option::Option<f64>>
);

seam_core::seam!(
    /// C: `parse_bool(str, &bval)` (bool.c) — parse a textual boolean; returns
    /// `Ok(None)` when the text is not a recognizable boolean.
    pub fn parse_bool(str: String) -> PgResult<core::option::Option<bool>>
);

seam_core::seam!(
    /// C: `DirectInputFunctionCallSafe(numeric_in, numstr, InvalidOid, typmod,
    /// escontext, &datum)` — parse `numstr` as `numeric` with the typmod built
    /// from `(precision, scale)`. Returns the on-disk `numeric` varlena bytes,
    /// or `Ok(None)` on a soft error.
    pub fn numeric_in_with_typmod(
        numstr: String,
        precision: i32,
        scale: i32
    ) -> PgResult<core::option::Option<Vec<u8>>>
);

seam_core::seam!(
    /// C: `JsonItemFromDatum(Datum val, Oid typid, int32 typmod, JsonbValue *res)`
    /// — coerce a SQL `Datum` of a `numeric`/int/float/text/varchar/jsonb/json
    /// type into a `JsonbValue`. (The `BOOLOID`, datetime, and `default`-error
    /// arms are handled in-crate.)
    pub fn json_item_from_datum(val: Datum, typid: Oid, typmod: i32) -> PgResult<JsonbValue>
);

seam_core::seam!(
    /// C: `check_stack_depth()` (tcop/postgres.c) — guard against unbounded
    /// recursion. Returns the soft error the C function raises on overflow.
    pub fn check_stack_depth() -> PgResult<()>
);

seam_core::seam!(
    /// C: `CHECK_FOR_INTERRUPTS()` (miscadmin.h) — process any pending
    /// query-cancel / die interrupt; `Err` if one fires.
    pub fn check_for_interrupts() -> PgResult<()>
);

/// A datetime SQL/JSON value as carried by `jbvDatetime`: a `Datum`, its type
/// OID, the typmod, and the numeric timezone (seconds) for `timestamptz`.
#[derive(Clone, Copy, Debug)]
pub struct DateTimeValue {
    pub value: Datum,
    pub typid: Oid,
    pub typmod: i32,
    pub tz: i32,
}

seam_core::seam!(
    /// C: `parse_datetime(text *date_txt, text *fmt, Oid collid, bool strict,
    /// Oid *typid, int32 *typmod, int *tz, struct Node *escontext)` (json.c).
    ///
    /// On a soft error (`throw_error == false`), returns `Ok(None)`; on success
    /// returns the parsed value/typid/typmod/tz.
    pub fn parse_datetime(
        datetime: Vec<u8>,
        template: core::option::Option<Vec<u8>>,
        collid: Oid,
        throw_error: bool
    ) -> PgResult<core::option::Option<DateTimeValue>>
);

seam_core::seam!(
    /// The `.date()/.time()/.time_tz()/.timestamp()/.timestamp_tz()` cast switch
    /// of `executeDateTimeMethod`: convert the parsed datetime to the method's
    /// target type, applying the optional time-precision typmod and the `useTz`
    /// checks. Returns the converted value, or `Ok(None)` to signal a soft
    /// (suppressed) error.
    ///
    /// `target` is the method's `JsonPathItemType` discriminant; `datetime_cstr`
    /// is the source text used in "format is not recognized" messages.
    pub fn datetime_method_cast(
        target: i32,
        parsed: DateTimeValue,
        time_precision: i32,
        use_tz: bool,
        datetime_cstr: String,
        throw_error: bool
    ) -> PgResult<core::option::Option<DateTimeValue>>
);

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
#[derive(Clone, Debug)]
pub struct JsonTableVariable {
    pub name: Vec<u8>,
    pub typid: Oid,
    pub typmod: i32,
    pub value: Datum,
    pub isnull: bool,
}

seam_core::seam!(
    /// C: build the root [`JsonTablePlan`] from `tf->plan`, the PASSING argument
    /// variables (evaluating each via `ExecEvalExpr`), and the total number of
    /// columns (`list_length(tf->colvalexprs)`).
    pub fn init_table_func() -> PgResult<(JsonTablePlan, Vec<JsonTableVariable>, usize)>
);

seam_core::seam!(
    /// C: `JsonTableGetValue`'s expression-evaluation arm — evaluate column
    /// `colnum`'s `JsonExpr` (`ExecEvalExpr`) against the current row pattern
    /// `current_value` (the row-pattern jsonb varlena bytes), returning
    /// `(Datum, isnull)`. Returns `Ok(None)` when the column has no expression
    /// (an ORDINAL column).
    pub fn eval_column(
        colnum: i32,
        typid: Oid,
        typmod: i32,
        current_value: Vec<u8>
    ) -> PgResult<core::option::Option<(Datum, bool)>>
);
