#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]

//! Safe-Rust port of `src/backend/utils/adt/jsonpath_exec.c` (postgres-18.3):
//! the SQL/JSON path **execution engine**.
//!
//! Jsonpath is executed in the global context [`JsonPathExecContext`], passed to
//! almost every function involved in execution. The entry point is
//! [`executeJsonPath`], which initializes the context (initial [`JsonPathItem`],
//! root [`JsonbValue`], flags, `@`-stack for filters) and runs the root item.
//! The result is a [`JsonPathExecResult`] plus, on success, a sequence of
//! `JsonbValue` written into a [`JsonValueList`].
//!
//! Every top-level function from `jsonpath_exec.c` is ported here 1:1 against the
//! C — preserving branch order, the three-valued [`JsonPathExecResult`] /
//! [`JsonPathBool`] logic, message text and SQLSTATE. The recursion, the
//! predicate/comparison/arithmetic logic, the `JsonValueList` plumbing and the
//! JSON_TABLE plan machinery are all implemented here, on top of the in-repo
//! `backend_utils_adt_{numeric,jsonb,jsonb_util,jsonpath}` crates.
//!
//! Genuinely-external operations — the regex matcher (`regexp.c`),
//! `JsonEncodeDateTime` (`json.c`), the fmgr type-input functions,
//! `format_type_be`, and the JSON_TABLE executor/`ExprState` machinery — are
//! funneled through the per-owner seam crate
//! [`backend_utils_adt_jsonpath_exec_seams`]. The datetime substrate
//! (`parse_datetime` from `formatting.c`, the `executeDateTimeMethod` cast
//! switch, and `compareDatetime`) is implemented in-crate (see the [`datetime`]
//! module) against the real ported `backend-utils-adt-formatting` /
//! `backend-utils-adt-datetime` leaf units, exactly as C reaches them through
//! `DirectFunctionCall*`. There is zero `extern "C"`, no raw pointers, no
//! `c_void`.
//!
//! # Memory-context threading
//!
//! The numeric ops (`numeric_add`, `int64_to_numeric`, …) and `JsonbValueToJsonb`
//! are charged against an explicit [`Mcx`] in this repo, so the executor carries
//! `mcx` through the context and the public entrypoints take it as their first
//! argument (C charges against the ambient `CurrentMemoryContext`).

use mcx::Mcx;

use backend_utils_adt_jsonb::{JsonbExtractScalar, JsonbTypeName};
use backend_utils_adt_jsonb_util::{
    findJsonbValueFromContainer, getIthJsonbValueFromContainer, pushJsonbValue, JsonbIteratorInitAt,
    JsonbIteratorNext, JsonbToJsonbValue,
};
use backend_utils_adt_jsonpath::{
    jspGetArg, jspGetArraySubscript, jspGetBool, jspGetLeftArg, jspGetNext, jspGetNumeric,
    jspGetRightArg, jspGetString, jspHasNext, jspInit, jspInitByBuffer, JsonPathItem,
    JsonPathItemType, JSONPATH_HDRSZ, JSONPATH_LAX,
};
use backend_utils_error::{ereport, PgError, PgResult};
use types_error::ERROR;
use types_core::Oid;
use types_datum::Datum;
use types_error::{
    ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_SQL_JSON_SUBSCRIPT, ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM,
    ERRCODE_NON_NUMERIC_SQL_JSON_ITEM, ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED,
    ERRCODE_SQL_JSON_ARRAY_NOT_FOUND, ERRCODE_SQL_JSON_MEMBER_NOT_FOUND,
    ERRCODE_SQL_JSON_NUMBER_NOT_FOUND, ERRCODE_SQL_JSON_OBJECT_NOT_FOUND,
    ERRCODE_SQL_JSON_SCALAR_REQUIRED, ERRCODE_UNDEFINED_OBJECT,
};
use types_jsonb::backend_utils_adt_jsonb_util::{JsonbDatetime, JsonbValue, JsonbValueData};
use types_jsonb::jsonb::{
    is_a_jsonb_scalar, jbvType, json_container_is_array, json_container_is_object,
    json_container_is_scalar, json_container_size, JsonbIteratorToken, JB_FOBJECT,
};
use types_tuple::heaptuple::DEFAULT_COLLATION_OID;

pub(crate) use backend_utils_adt_jsonpath_exec_seams as seam;

use JsonPathItemType::*;

mod datetime;
mod fmgr_builtins;
mod json_table;
mod seams;
#[cfg(test)]
mod tests;

pub use fmgr_builtins::register_jsonpath_exec_builtins;
pub use seams::init_seams;

pub use json_table::{
    JsonTableCurrentRow, JsonTableDestroyOpaque, JsonTableExecContext, JsonTableFetchRow,
    JsonTableInitOpaque, JsonTableRowValue, JsonTableSetDocument,
};

// Re-export the seam vocabulary so wiring can install the genuine-external
// providers through one path, and downstream callers can name the JSON_TABLE
// plan vocabulary.
pub use seam::DateTimeValue;
pub use seam::{JsonTablePathScan, JsonTablePlan, JsonTableSiblingJoin, JsonTableVariable};

/// `VARHDRSZ`, the varlena length-header size in bytes.
const VARHDRSZ: usize = 4;

// ===========================================================================
// Result enums (jsonpath_exec.c:127-140)
// ===========================================================================

/// `JsonPathExecResult` (jsonpath_exec.c:135-140): the tri-state result of
/// evaluating a path item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum JsonPathExecResult {
    /// `jperOk`: result sequence is not empty.
    jperOk = 0,
    /// `jperNotFound`: result sequence is empty (not an error).
    jperNotFound = 1,
    /// `jperError`: error occurred during execution (silenced in lax mode).
    jperError = 2,
}

use JsonPathExecResult::*;

/// C: `jperIsError(jper)` (jsonpath_exec.c:142).
#[inline]
fn jperIsError(jper: JsonPathExecResult) -> bool {
    jper == jperError
}

/// `JsonPathBool` (jsonpath_exec.c:127-132): SQL/JSON three-valued boolean.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum JsonPathBool {
    /// `jpbFalse`.
    jpbFalse = 0,
    /// `jpbTrue`.
    jpbTrue = 1,
    /// `jpbUnknown`.
    jpbUnknown = 2,
}

use JsonPathBool::*;

/// `JsonWrapper` (miscnodes.h): the JSON_QUERY wrapper mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum JsonWrapper {
    JSW_UNSPEC,
    JSW_NONE,
    JSW_CONDITIONAL,
    JSW_UNCONDITIONAL,
}

const PG_UINT32_MAX: u32 = u32::MAX;
/// `10^10`, the `.keyvalue()` id multiplier (jsonpath_exec.c:2864).
const KEYVALUE_ID_MULT: i64 = 10000000000;

// ===========================================================================
// Execution context (jsonpath_exec.c:82-124)
// ===========================================================================

/// C: `struct JsonBaseObjectInfo` (jsonpath_exec.c:82-86). "base object" and
/// its "id" for `.keyvalue()` evaluation.
///
/// In the C struct `jbc` is a `JsonbContainer *` into the working data; here we
/// carry the container bytes plus an identity offset. `jbc` is `None` for a
/// non-binary base object.
#[derive(Clone, Debug, Default)]
struct JsonBaseObjectInfo {
    /// The base object's container bytes (C: `JsonbContainer *jbc`).
    jbc: Option<Vec<u8>>,
    /// Document-relative byte offset of the base object's container within its
    /// origin document. Used to compute the `.keyvalue()` id: C subtracts the
    /// two raw container pointers; the port subtracts the two document-relative
    /// offsets, which is identical when both share the same base document.
    id_addr: i64,
    /// C: `int id`.
    id: i32,
}

/// A jsonpath variable resolver callback (C: `JsonPathGetVarCallback`).
type JsonPathGetVarCallback = fn(
    mcx: Mcx<'_>,
    vars: &JsonPathVars,
    var_name: &[u8],
    base_object: &mut JsonbValue,
    base_object_id: &mut i32,
) -> PgResult<Option<JsonbValue>>;

/// A jsonpath variable counter callback (C: `JsonPathCountVarsCallback`).
type JsonPathCountVarsCallback = fn(vars: &JsonPathVars) -> PgResult<i32>;

/// The PASSING-arguments / `$var` value source (C: `void *vars`).
#[derive(Clone, Debug, Default)]
pub enum JsonPathVars {
    /// No variables (C: `vars == NULL`).
    #[default]
    None,
    /// C: `Jsonb *vars` (the full on-disk varlena, length header included).
    Jsonb(Vec<u8>),
    /// C: `List *vars` of `JsonPathVariable`.
    List(Vec<JsonPathVariable>),
}

/// C: `struct JsonPathVariable` (jsonpath.h) — a bound jsonpath variable.
#[derive(Clone, Debug)]
pub struct JsonPathVariable {
    /// Variable name (no leading `$`).
    pub name: Vec<u8>,
    /// SQL type OID of `value`.
    pub typid: Oid,
    /// Typmod of `value`.
    pub typmod: i32,
    /// The bound SQL value (as a `Datum`).
    pub value: Datum,
    /// Whether `value` is SQL NULL.
    pub isnull: bool,
}

/// Context of jsonpath execution (C: `struct JsonPathExecContext`,
/// jsonpath_exec.c:96-117).
struct JsonPathExecContext<'mcx, 'p> {
    /// Memory context charged for numeric/jsonb allocations (the repo carries
    /// this explicitly; C uses the ambient `CurrentMemoryContext`).
    mcx: Mcx<'mcx>,
    /// Variables to substitute into jsonpath (C: `void *vars`).
    vars: &'p JsonPathVars,
    /// Callback to extract a given variable (C: `getVar`).
    getVar: JsonPathGetVarCallback,
    /// For `$` evaluation (C: `JsonbValue *root`).
    root: JsonbValue,
    /// For `@` evaluation (C: `JsonbValue *current`).
    current: JsonbValue,
    /// "base object" for `.keyvalue()` (C: `JsonBaseObjectInfo baseObject`).
    baseObject: JsonBaseObjectInfo,
    /// "id" counter for `.keyvalue()` (C: `int lastGeneratedObjectId`).
    lastGeneratedObjectId: i32,
    /// For LAST array index evaluation (C: `int innermostArraySize`).
    innermostArraySize: i32,
    /// `true` for "lax" mode (C: `bool laxMode`).
    laxMode: bool,
    /// Ignore structural errors (C: `bool ignoreStructuralErrors`).
    ignoreStructuralErrors: bool,
    /// Suppress suppressible errors when `false` (C: `bool throwErrors`).
    throwErrors: bool,
    /// Use timezone for datetime casts/compares (C: `bool useTz`).
    useTz: bool,
}

/// Context for LIKE_REGEX execution (C: `struct JsonLikeRegexContext`).
#[derive(Default)]
struct JsonLikeRegexContext {
    /// Cached regex pattern bytes (C: `text *regex`).
    regex: Option<Vec<u8>>,
    /// Cached PostgreSQL regex cflags (C: `int cflags`).
    cflags: i32,
}

// strict/lax flags decomposed into the four [un]wrap/error flags
// (jsonpath_exec.c:236-240).
#[inline]
fn jspStrictAbsenceOfErrors(cxt: &JsonPathExecContext<'_, '_>) -> bool {
    !cxt.laxMode
}
#[inline]
fn jspAutoUnwrap(cxt: &JsonPathExecContext<'_, '_>) -> bool {
    cxt.laxMode
}
#[inline]
fn jspAutoWrap(cxt: &JsonPathExecContext<'_, '_>) -> bool {
    cxt.laxMode
}
#[inline]
fn jspIgnoreStructuralErrors(cxt: &JsonPathExecContext<'_, '_>) -> bool {
    cxt.ignoreStructuralErrors
}
#[inline]
fn jspThrowErrors(cxt: &JsonPathExecContext<'_, '_>) -> bool {
    cxt.throwErrors
}

/// C: `RETURN_ERROR(throw_error)` (jsonpath_exec.c:243-249) — if the context
/// throws errors, propagate `err` as `Err`; otherwise return `Ok(jperError)`.
#[inline]
fn return_error(
    cxt: &JsonPathExecContext<'_, '_>,
    err: PgError,
) -> PgResult<JsonPathExecResult> {
    if jspThrowErrors(cxt) {
        Err(err)
    } else {
        Ok(jperError)
    }
}

// ===========================================================================
// JsonValueList (jsonpath_exec.c:147-158)
// ===========================================================================

/// List of jsonb values with shortcut for single-value list (C:
/// `struct JsonValueList`).
#[derive(Clone, Debug, Default)]
pub struct JsonValueList {
    /// C: `JsonbValue *singleton`.
    singleton: Option<JsonbValue>,
    /// C: `List *list`.
    list: Vec<JsonbValue>,
}

/// Iterator over a [`JsonValueList`] (C: `struct JsonValueListIterator`).
struct JsonValueListIterator {
    /// All items, materialized in order (mirrors C's `singleton`-or-`list`).
    items: Vec<JsonbValue>,
    /// Next index to return.
    pos: usize,
}

// ===========================================================================
// SQL-callable entrypoints + internals (jsonpath_exec.c:382-650)
// ===========================================================================

/// Outcome of `jsonb_path_exists` (C returns NULL when the result is an error).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathExistsResult {
    /// SQL `true`.
    True,
    /// SQL `false`.
    False,
    /// SQL NULL (error suppressed).
    Null,
}

/// C: `jsonb_path_exists_internal` (jsonpath_exec.c:397).
fn jsonb_path_exists_internal(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<PathExistsResult> {
    let vars = vars_from_opt_jsonb(vars);

    let res = executeJsonPath(
        mcx,
        jp,
        &vars,
        getJsonPathVariableFromJsonb,
        countVariablesFromJsonb,
        jb,
        !silent,
        None,
        false,
    )?;

    if jperIsError(res) {
        return Ok(PathExistsResult::Null);
    }

    Ok(if res == jperOk {
        PathExistsResult::True
    } else {
        PathExistsResult::False
    })
}

/// C: `jsonb_path_exists` (jsonpath_exec.c:425).
pub fn jsonb_path_exists(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<PathExistsResult> {
    jsonb_path_exists_internal(mcx, jb, jp, vars, silent)
}

/// C: `jsonb_path_exists_tz` (jsonpath_exec.c:431). Identical body but `tz=true`.
pub fn jsonb_path_exists_tz(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<PathExistsResult> {
    let vars = vars_from_opt_jsonb(vars);
    let res = executeJsonPath(
        mcx,
        jp,
        &vars,
        getJsonPathVariableFromJsonb,
        countVariablesFromJsonb,
        jb,
        !silent,
        None,
        true,
    )?;
    if jperIsError(res) {
        return Ok(PathExistsResult::Null);
    }
    Ok(if res == jperOk {
        PathExistsResult::True
    } else {
        PathExistsResult::False
    })
}

/// C: `jsonb_path_exists_opr` (jsonpath_exec.c:442) — `jsonb @? jsonpath`.
pub fn jsonb_path_exists_opr(mcx: Mcx<'_>, jb: &[u8], jp: &[u8]) -> PgResult<PathExistsResult> {
    jsonb_path_exists_internal(mcx, jb, jp, None, true)
}

/// Result of `jsonb_path_match`: SQL boolean or NULL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathMatchResult {
    True,
    False,
    Null,
}

/// C: `jsonb_path_match_internal` (jsonpath_exec.c:454).
fn jsonb_path_match_internal(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
    tz: bool,
) -> PgResult<PathMatchResult> {
    let vars = vars_from_opt_jsonb(vars);
    let mut found = JsonValueList::default();

    let _ = executeJsonPath(
        mcx,
        jp,
        &vars,
        getJsonPathVariableFromJsonb,
        countVariablesFromJsonb,
        jb,
        !silent,
        Some(&mut found),
        tz,
    )?;

    if JsonValueListLength(&found) == 1 {
        let jbv = JsonValueListHead(&found);
        if jbv.typ == jbvType::jbvBool {
            if let JsonbValueData::Bool(b) = jbv.val {
                return Ok(if b {
                    PathMatchResult::True
                } else {
                    PathMatchResult::False
                });
            }
        }
        if jbv.typ == jbvType::jbvNull {
            return Ok(PathMatchResult::Null);
        }
    }

    if !silent {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED)
            .errmsg("single boolean result is expected")
            .into_error());
    }

    Ok(PathMatchResult::Null)
}

/// C: `jsonb_path_match` (jsonpath_exec.c:495).
pub fn jsonb_path_match(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<PathMatchResult> {
    jsonb_path_match_internal(mcx, jb, jp, vars, silent, false)
}

/// C: `jsonb_path_match_tz` (jsonpath_exec.c:501).
pub fn jsonb_path_match_tz(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<PathMatchResult> {
    jsonb_path_match_internal(mcx, jb, jp, vars, silent, true)
}

/// C: `jsonb_path_match_opr` (jsonpath_exec.c:512) — `jsonb @@ jsonpath`.
pub fn jsonb_path_match_opr(mcx: Mcx<'_>, jb: &[u8], jp: &[u8]) -> PgResult<PathMatchResult> {
    jsonb_path_match_internal(mcx, jb, jp, None, true, false)
}

/// C: `jsonb_path_query_internal` (jsonpath_exec.c:524). Each element is an
/// on-disk jsonb varlena.
fn jsonb_path_query_internal(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
    tz: bool,
) -> PgResult<Vec<Vec<u8>>> {
    let vars = vars_from_opt_jsonb(vars);
    let mut found = JsonValueList::default();

    let _ = executeJsonPath(
        mcx,
        jp,
        &vars,
        getJsonPathVariableFromJsonb,
        countVariablesFromJsonb,
        jb,
        !silent,
        Some(&mut found),
        tz,
    )?;

    let list = JsonValueListGetList(&found);
    let mut out = Vec::with_capacity(list.len());
    for v in &list {
        out.push(JsonbValueToJsonb(mcx, v)?);
    }
    Ok(out)
}

/// C: `jsonb_path_query` (jsonpath_exec.c:572).
pub fn jsonb_path_query(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<Vec<Vec<u8>>> {
    jsonb_path_query_internal(mcx, jb, jp, vars, silent, false)
}

/// C: `jsonb_path_query_tz` (jsonpath_exec.c:578).
pub fn jsonb_path_query_tz(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<Vec<Vec<u8>>> {
    jsonb_path_query_internal(mcx, jb, jp, vars, silent, true)
}

/// C: `jsonb_path_query_array_internal` (jsonpath_exec.c:589).
fn jsonb_path_query_array_internal(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
    tz: bool,
) -> PgResult<Vec<u8>> {
    let vars = vars_from_opt_jsonb(vars);
    let mut found = JsonValueList::default();

    let _ = executeJsonPath(
        mcx,
        jp,
        &vars,
        getJsonPathVariableFromJsonb,
        countVariablesFromJsonb,
        jb,
        !silent,
        Some(&mut found),
        tz,
    )?;

    JsonbValueToJsonb(mcx, &wrapItemsInArray(&found)?)
}

/// C: `jsonb_path_query_array` (jsonpath_exec.c:605).
pub fn jsonb_path_query_array(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<Vec<u8>> {
    jsonb_path_query_array_internal(mcx, jb, jp, vars, silent, false)
}

/// C: `jsonb_path_query_array_tz` (jsonpath_exec.c:611).
pub fn jsonb_path_query_array_tz(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<Vec<u8>> {
    jsonb_path_query_array_internal(mcx, jb, jp, vars, silent, true)
}

/// C: `jsonb_path_query_first_internal` (jsonpath_exec.c:622).
fn jsonb_path_query_first_internal(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
    tz: bool,
) -> PgResult<Option<Vec<u8>>> {
    let vars = vars_from_opt_jsonb(vars);
    let mut found = JsonValueList::default();

    let _ = executeJsonPath(
        mcx,
        jp,
        &vars,
        getJsonPathVariableFromJsonb,
        countVariablesFromJsonb,
        jb,
        !silent,
        Some(&mut found),
        tz,
    )?;

    if JsonValueListLength(&found) >= 1 {
        Ok(Some(JsonbValueToJsonb(mcx, JsonValueListHead(&found))?))
    } else {
        Ok(None)
    }
}

/// C: `jsonb_path_query_first` (jsonpath_exec.c:641).
pub fn jsonb_path_query_first(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<Option<Vec<u8>>> {
    jsonb_path_query_first_internal(mcx, jb, jp, vars, silent, false)
}

/// C: `jsonb_path_query_first_tz` (jsonpath_exec.c:647).
pub fn jsonb_path_query_first_tz(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    vars: Option<&[u8]>,
    silent: bool,
) -> PgResult<Option<Vec<u8>>> {
    jsonb_path_query_first_internal(mcx, jb, jp, vars, silent, true)
}

// ===========================================================================
// Core evaluator (jsonpath_exec.c:676-2011)
// ===========================================================================

/// C: `executeJsonPath` (jsonpath_exec.c:676). `json` is the full on-disk jsonb
/// varlena bytes.
fn executeJsonPath(
    mcx: Mcx<'_>,
    path: &[u8],
    vars: &JsonPathVars,
    getVar: JsonPathGetVarCallback,
    countVars: JsonPathCountVarsCallback,
    json: &[u8],
    throwErrors: bool,
    result: Option<&mut JsonValueList>,
    useTz: bool,
) -> PgResult<JsonPathExecResult> {
    let jsp = jspInit(path);

    let root_container = jsonb_root(json);
    let mut jbv = JsonbValue::null();
    if !JsonbExtractScalar(root_container, &mut jbv)? {
        JsonbInitBinary(&mut jbv, json);
    }

    let mut cxt = JsonPathExecContext {
        mcx,
        vars,
        getVar,
        laxMode: (jsonpath_header(path) & JSONPATH_LAX) != 0,
        ignoreStructuralErrors: false,
        root: jbv.clone(),
        current: jbv.clone(),
        baseObject: JsonBaseObjectInfo::default(),
        lastGeneratedObjectId: 0,
        innermostArraySize: -1,
        throwErrors,
        useTz,
    };
    cxt.ignoreStructuralErrors = cxt.laxMode;
    // 1 + number of base objects in vars (the jsonb variant validates here).
    cxt.lastGeneratedObjectId = 1 + countVars(vars)?;

    if jspStrictAbsenceOfErrors(&cxt) && result.is_none() {
        // In strict mode we must get a complete list of values to check that
        // there are no errors at all.
        let mut vals = JsonValueList::default();
        let res = executeItem(&mut cxt, &jsp, &jbv, Some(&mut vals))?;
        if jperIsError(res) {
            return Ok(res);
        }
        return Ok(if JsonValueListIsEmpty(&vals) {
            jperNotFound
        } else {
            jperOk
        });
    }

    let res = executeItem(&mut cxt, &jsp, &jbv, result)?;
    debug_assert!(!throwErrors || !jperIsError(res));
    Ok(res)
}

/// C: `executeItem` (jsonpath_exec.c:732).
fn executeItem(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    let unwrap = jspAutoUnwrap(cxt);
    executeItemOptUnwrapTarget(cxt, jsp, jb, found, unwrap)
}

/// C: `executeItemOptUnwrapTarget` (jsonpath_exec.c:744) — the big per-item
/// switch.
fn executeItemOptUnwrapTarget(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    mut found: Option<&mut JsonValueList>,
    unwrap: bool,
) -> PgResult<JsonPathExecResult> {
    let mut res = jperNotFound;

    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;
    backend_tcop_postgres_seams::check_for_interrupts::call()?;

    match jsp.typ {
        jpiNull | jpiBool | jpiNumeric | jpiString | jpiVariable => {
            let next = jspGetNext(jsp);
            let hasNext = next.is_some();

            if !hasNext && found.is_none() && jsp.typ != jpiVariable {
                // Skip evaluation, but not for variables. We must trigger an
                // error for the missing variable.
                return Ok(jperOk);
            }

            let baseObject = cxt.baseObject.clone();
            let mut v = JsonbValue::null();
            getJsonPathItem(cxt, jsp, &mut v)?;

            res = executeNextItem(cxt, jsp, next.as_ref(), &v, found, hasNext)?;
            cxt.baseObject = baseObject;
        }

        // all boolean item types:
        jpiAnd | jpiOr | jpiNot | jpiIsUnknown | jpiEqual | jpiNotEqual | jpiLess | jpiGreater
        | jpiLessOrEqual | jpiGreaterOrEqual | jpiExists | jpiStartsWith | jpiLikeRegex => {
            let st = executeBoolItem(cxt, jsp, jb, true)?;
            res = appendBoolResult(cxt, jsp, found, st)?;
        }

        jpiAdd => return executeBinaryArithmExpr(cxt, jsp, jb, ArithmOp::Add, found),
        jpiSub => return executeBinaryArithmExpr(cxt, jsp, jb, ArithmOp::Sub, found),
        jpiMul => return executeBinaryArithmExpr(cxt, jsp, jb, ArithmOp::Mul, found),
        jpiDiv => return executeBinaryArithmExpr(cxt, jsp, jb, ArithmOp::Div, found),
        jpiMod => return executeBinaryArithmExpr(cxt, jsp, jb, ArithmOp::Mod, found),

        jpiPlus => return executeUnaryArithmExpr(cxt, jsp, jb, None, found),
        jpiMinus => return executeUnaryArithmExpr(cxt, jsp, jb, Some(UnaryOp::Minus), found),

        jpiAnyArray => {
            if JsonbType(jb)? == jbvType::jbvArray {
                let next = jspGetNext(jsp);
                let unwrap2 = jspAutoUnwrap(cxt);
                res = executeItemUnwrapTargetArray(cxt, next.as_ref(), jb, found, unwrap2)?;
            } else if jspAutoWrap(cxt) {
                res = executeNextItem(cxt, jsp, None, jb, found, true)?;
            } else if !jspIgnoreStructuralErrors(cxt) {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND)
                        .errmsg("jsonpath wildcard array accessor can only be applied to an array")
                        .into_error(),
                );
            }
        }

        jpiAnyKey => {
            if JsonbType(jb)? == jbvType::jbvObject {
                let next = jspGetNext(jsp);
                let data = binary_data(jb, "invalid jsonb object type")?.to_vec();
                let data_off = binary_doc_offset(jb);
                let unwrap2 = jspAutoUnwrap(cxt);
                return executeAnyItem(
                    cxt,
                    next.as_ref(),
                    &data,
                    data_off,
                    found,
                    1,
                    1,
                    1,
                    false,
                    unwrap2,
                );
            } else if unwrap && JsonbType(jb)? == jbvType::jbvArray {
                return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
            } else if !jspIgnoreStructuralErrors(cxt) {
                debug_assert!(found.is_some());
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND)
                        .errmsg(
                            "jsonpath wildcard member accessor can only be applied to an object",
                        )
                        .into_error(),
                );
            }
        }

        jpiIndexArray => {
            if JsonbType(jb)? == jbvType::jbvArray || jspAutoWrap(cxt) {
                let innermostArraySize = cxt.innermostArraySize;
                let mut size = JsonbArraySize(jb)?;
                let singleton = size < 0;
                let next = jspGetNext(jsp);
                let hasNext = next.is_some();

                if singleton {
                    size = 1;
                }

                cxt.innermostArraySize = size; // for LAST evaluation

                let nelems = jsp.content.array.nelems;
                let mut i = 0;
                while i < nelems {
                    let (from, to_opt) = jspGetArraySubscript(jsp, i);

                    let mut index_from = 0i32;
                    res = getArrayIndex(cxt, &from, jb, &mut index_from)?;
                    if jperIsError(res) {
                        break;
                    }

                    let mut index_to;
                    if let Some(to) = to_opt {
                        index_to = 0i32;
                        res = getArrayIndex(cxt, &to, jb, &mut index_to)?;
                        if jperIsError(res) {
                            break;
                        }
                    } else {
                        index_to = index_from;
                    }

                    if !jspIgnoreStructuralErrors(cxt)
                        && (index_from < 0 || index_from > index_to || index_to >= size)
                    {
                        return return_error(
                            cxt,
                            ereport(ERROR)
                                .errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT)
                                .errmsg("jsonpath array subscript is out of bounds")
                                .into_error(),
                        );
                    }

                    if index_from < 0 {
                        index_from = 0;
                    }
                    if index_to >= size {
                        index_to = size - 1;
                    }

                    res = jperNotFound;

                    let mut index = index_from;
                    while index <= index_to {
                        let v;
                        let copy;
                        if singleton {
                            v = jb.clone();
                            copy = true;
                        } else {
                            let data = binary_data(jb, "invalid jsonb array value type")?.to_vec();
                            match getIthJsonbValueFromContainer(&data, index as u32)? {
                                Some(mut found_v) => {
                                    rebase_binary_offset(&mut found_v, binary_doc_offset(jb));
                                    v = found_v;
                                    copy = false;
                                }
                                None => {
                                    index += 1;
                                    continue;
                                }
                            }
                        }

                        if !hasNext && found.is_none() {
                            return Ok(jperOk);
                        }

                        res = executeNextItem(
                            cxt,
                            jsp,
                            next.as_ref(),
                            &v,
                            found.as_deref_mut(),
                            copy,
                        )?;

                        if jperIsError(res) {
                            break;
                        }
                        if res == jperOk && found.is_none() {
                            break;
                        }
                        index += 1;
                    }

                    if jperIsError(res) {
                        break;
                    }
                    if res == jperOk && found.is_none() {
                        break;
                    }
                    i += 1;
                }

                cxt.innermostArraySize = innermostArraySize;
            } else if !jspIgnoreStructuralErrors(cxt) {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND)
                        .errmsg("jsonpath array accessor can only be applied to an array")
                        .into_error(),
                );
            }
        }

        jpiAny => {
            let next = jspGetNext(jsp);
            let hasNext = next.is_some();

            // first try without any intermediate steps
            if jsp.content.anybounds.first == 0 {
                let savedIgnoreStructuralErrors = cxt.ignoreStructuralErrors;
                cxt.ignoreStructuralErrors = true;
                res = executeNextItem(cxt, jsp, next.as_ref(), jb, found.as_deref_mut(), true)?;
                cxt.ignoreStructuralErrors = savedIgnoreStructuralErrors;

                if res == jperOk && found.is_none() {
                    return Ok(res);
                }
            }

            if jb.typ == jbvType::jbvBinary {
                let data = binary_data(jb, "invalid jsonb binary type")?.to_vec();
                let data_off = binary_doc_offset(jb);
                res = executeAnyItem(
                    cxt,
                    if hasNext { next.as_ref() } else { None },
                    &data,
                    data_off,
                    found,
                    1,
                    jsp.content.anybounds.first,
                    jsp.content.anybounds.last,
                    true,
                    jspAutoUnwrap(cxt),
                )?;
            }
        }

        jpiKey => {
            if JsonbType(jb)? == jbvType::jbvObject {
                let key_bytes = jspGetString(jsp);
                let key = JsonbValue {
                    typ: jbvType::jbvString,
                    val: JsonbValueData::String(key_bytes.to_vec()),
                };
                let data = binary_data(jb, "invalid jsonb object type")?.to_vec();
                let v = findJsonbValueFromContainer(&data, JB_FOBJECT, &key)?;

                if let Some(mut v) = v {
                    rebase_binary_offset(&mut v, binary_doc_offset(jb));
                    res = executeNextItem(cxt, jsp, None, &v, found, false)?;
                } else if !jspIgnoreStructuralErrors(cxt) {
                    debug_assert!(found.is_some());

                    if !jspThrowErrors(cxt) {
                        return Ok(jperError);
                    }

                    let key_str = String::from_utf8_lossy(key_bytes).into_owned();
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND)
                        .errmsg(format!("JSON object does not contain key \"{key_str}\""))
                        .into_error());
                }
            } else if unwrap && JsonbType(jb)? == jbvType::jbvArray {
                return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
            } else if !jspIgnoreStructuralErrors(cxt) {
                debug_assert!(found.is_some());
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND)
                        .errmsg("jsonpath member accessor can only be applied to an object")
                        .into_error(),
                );
            }
        }

        jpiCurrent => {
            let current = cxt.current.clone();
            res = executeNextItem(cxt, jsp, None, &current, found, true)?;
        }

        jpiRoot => {
            let jb = cxt.root.clone();
            let baseObject = setBaseObject(cxt, &jb, 0);
            res = executeNextItem(cxt, jsp, None, &jb, found, true)?;
            cxt.baseObject = baseObject;
        }

        jpiFilter => {
            if unwrap && JsonbType(jb)? == jbvType::jbvArray {
                return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
            }

            let elem = jspGetArg(jsp);
            let st = executeNestedBoolItem(cxt, &elem, jb)?;
            if st != jpbTrue {
                res = jperNotFound;
            } else {
                res = executeNextItem(cxt, jsp, None, jb, found, true)?;
            }
        }

        jpiType => {
            let name = JsonbTypeName(jb)?;
            let jbv = JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(name.as_bytes().to_vec()),
            };
            res = executeNextItem(cxt, jsp, None, &jbv, found, false)?;
        }

        jpiSize => {
            let mut size = JsonbArraySize(jb)?;
            if size < 0 {
                if !jspAutoWrap(cxt) {
                    if !jspIgnoreStructuralErrors(cxt) {
                        return return_error(
                            cxt,
                            ereport(ERROR)
                                .errcode(ERRCODE_SQL_JSON_ARRAY_NOT_FOUND)
                                .errmsg(format!(
                                    "jsonpath item method .{}() can only be applied to an array",
                                    op_name(jsp.typ)?
                                ))
                                .into_error(),
                        );
                    }
                    return Ok(res);
                }
                size = 1;
            }

            let bytes = int64_to_numeric_bytes(cxt.mcx, size as i64)?;
            let jbv = JsonbValue {
                typ: jbvType::jbvNumeric,
                val: JsonbValueData::Numeric(bytes),
            };
            res = executeNextItem(cxt, jsp, None, &jbv, found, false)?;
        }

        jpiAbs => return executeNumericItemMethod(cxt, jsp, jb, unwrap, NumericMethod::Abs, found),
        jpiFloor => {
            return executeNumericItemMethod(cxt, jsp, jb, unwrap, NumericMethod::Floor, found)
        }
        jpiCeiling => {
            return executeNumericItemMethod(cxt, jsp, jb, unwrap, NumericMethod::Ceiling, found)
        }

        jpiDouble => return execute_double(cxt, jsp, jb, unwrap, found),

        jpiDatetime | jpiDate | jpiTime | jpiTimeTz | jpiTimestamp | jpiTimestampTz => {
            if unwrap && JsonbType(jb)? == jbvType::jbvArray {
                return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
            }
            return executeDateTimeMethod(cxt, jsp, jb, found);
        }

        jpiKeyValue => {
            if unwrap && JsonbType(jb)? == jbvType::jbvArray {
                return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
            }
            return executeKeyValueMethod(cxt, jsp, jb, found);
        }

        jpiLast => {
            let next = jspGetNext(jsp);
            let hasNext = next.is_some();

            if cxt.innermostArraySize < 0 {
                return Err(elog_error(
                    "evaluating jsonpath LAST outside of array subscript",
                ));
            }

            if !hasNext && found.is_none() {
                return Ok(jperOk);
            }

            let last = cxt.innermostArraySize - 1;
            let bytes = int64_to_numeric_bytes(cxt.mcx, last as i64)?;
            let lastjbv = JsonbValue {
                typ: jbvType::jbvNumeric,
                val: JsonbValueData::Numeric(bytes),
            };

            res = executeNextItem(cxt, jsp, next.as_ref(), &lastjbv, found, hasNext)?;
        }

        jpiBigint => return execute_bigint(cxt, jsp, jb, unwrap, found),
        jpiBoolean => return execute_boolean(cxt, jsp, jb, unwrap, found),
        jpiDecimal | jpiNumber => return execute_decimal_number(cxt, jsp, jb, unwrap, found),
        jpiInteger => return execute_integer(cxt, jsp, jb, unwrap, found),
        jpiStringFunc => return execute_string_func(cxt, jsp, jb, unwrap, found),

        _ => {
            return Err(elog_error(&format!(
                "unrecognized jsonpath item type: {}",
                jsp.typ as i32
            )));
        }
    }

    Ok(res)
}

/// C: `executeItemUnwrapTargetArray` (jsonpath_exec.c:1672).
fn executeItemUnwrapTargetArray(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: Option<&JsonPathItem<'_>>,
    jb: &JsonbValue,
    found: Option<&mut JsonValueList>,
    unwrapElements: bool,
) -> PgResult<JsonPathExecResult> {
    if jb.typ != jbvType::jbvBinary {
        debug_assert!(jb.typ != jbvType::jbvArray);
        return Err(elog_error(&format!(
            "invalid jsonb array value type: {}",
            jb.typ as i32
        )));
    }

    let data = binary_data(jb, "invalid jsonb array value type")?.to_vec();
    let data_off = binary_doc_offset(jb);
    executeAnyItem(cxt, jsp, &data, data_off, found, 1, 1, 1, false, unwrapElements)
}

/// C: `executeNextItem` (jsonpath_exec.c:1692).
fn executeNextItem(
    cxt: &mut JsonPathExecContext<'_, '_>,
    cur: &JsonPathItem<'_>,
    next: Option<&JsonPathItem<'_>>,
    v: &JsonbValue,
    found: Option<&mut JsonValueList>,
    copy: bool,
) -> PgResult<JsonPathExecResult> {
    let computed_next;
    let hasNext;
    let next_ref: Option<&JsonPathItem<'_>>;

    if let Some(n) = next {
        hasNext = jspHasNext(cur);
        next_ref = Some(n);
    } else {
        computed_next = jspGetNext(cur);
        hasNext = computed_next.is_some();
        next_ref = computed_next.as_ref();
    }

    if hasNext {
        return executeItem(cxt, next_ref.unwrap(), v, found);
    }

    if let Some(found) = found {
        JsonValueListAppend(found, if copy { copyJsonbValue(v) } else { v.clone() });
    }

    Ok(jperOk)
}

/// C: `executeItemOptUnwrapResult` (jsonpath_exec.c:1723).
fn executeItemOptUnwrapResult(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: &mut JsonValueList,
) -> PgResult<JsonPathExecResult> {
    if unwrap && jspAutoUnwrap(cxt) {
        let mut seq = JsonValueList::default();
        let res = executeItem(cxt, jsp, jb, Some(&mut seq))?;
        if jperIsError(res) {
            return Ok(res);
        }

        let mut it = JsonValueListInitIterator(&seq);
        while let Some(item) = JsonValueListNext(&mut it) {
            debug_assert!(item.typ != jbvType::jbvArray);

            if JsonbType(&item)? == jbvType::jbvArray {
                executeItemUnwrapTargetArray(cxt, None, &item, Some(found), false)?;
            } else {
                JsonValueListAppend(found, item);
            }
        }

        return Ok(jperOk);
    }

    executeItem(cxt, jsp, jb, Some(found))
}

/// C: `executeItemOptUnwrapResultNoThrow` (jsonpath_exec.c:1758).
fn executeItemOptUnwrapResultNoThrow(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: &mut JsonValueList,
) -> PgResult<JsonPathExecResult> {
    let throwErrors = cxt.throwErrors;
    cxt.throwErrors = false;
    let res = executeItemOptUnwrapResult(cxt, jsp, jb, unwrap, found);
    cxt.throwErrors = throwErrors;
    res
}

/// C: `executeBoolItem` (jsonpath_exec.c:1775).
fn executeBoolItem(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    canHaveNext: bool,
) -> PgResult<JsonPathBool> {
    // since this function recurses, it could be driven to stack overflow
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    if !canHaveNext && jspHasNext(jsp) {
        return Err(elog_error("boolean jsonpath item cannot have next item"));
    }

    match jsp.typ {
        jpiAnd => {
            let larg = jspGetLeftArg(jsp);
            let res = executeBoolItem(cxt, &larg, jb, false)?;
            if res == jpbFalse {
                return Ok(jpbFalse);
            }
            // SQL/JSON says that we should check second arg in case of jperError
            let rarg = jspGetRightArg(jsp);
            let res2 = executeBoolItem(cxt, &rarg, jb, false)?;
            Ok(if res2 == jpbTrue { res } else { res2 })
        }

        jpiOr => {
            let larg = jspGetLeftArg(jsp);
            let res = executeBoolItem(cxt, &larg, jb, false)?;
            if res == jpbTrue {
                return Ok(jpbTrue);
            }
            let rarg = jspGetRightArg(jsp);
            let res2 = executeBoolItem(cxt, &rarg, jb, false)?;
            Ok(if res2 == jpbFalse { res } else { res2 })
        }

        jpiNot => {
            let larg = jspGetArg(jsp);
            let res = executeBoolItem(cxt, &larg, jb, false)?;
            if res == jpbUnknown {
                return Ok(jpbUnknown);
            }
            Ok(if res == jpbTrue { jpbFalse } else { jpbTrue })
        }

        jpiIsUnknown => {
            let larg = jspGetArg(jsp);
            let res = executeBoolItem(cxt, &larg, jb, false)?;
            Ok(if res == jpbUnknown { jpbTrue } else { jpbFalse })
        }

        jpiEqual | jpiNotEqual | jpiLess | jpiGreater | jpiLessOrEqual | jpiGreaterOrEqual => {
            let larg = jspGetLeftArg(jsp);
            let rarg = jspGetRightArg(jsp);
            executePredicate(cxt, jsp, &larg, Some(&rarg), jb, true, PredicateKind::Comparison)
        }

        jpiStartsWith => {
            // 'whole STARTS WITH initial'
            let larg = jspGetLeftArg(jsp); // 'whole'
            let rarg = jspGetRightArg(jsp); // 'initial'
            executePredicate(cxt, jsp, &larg, Some(&rarg), jb, false, PredicateKind::StartsWith)
        }

        jpiLikeRegex => {
            // 'expr LIKE_REGEX pattern FLAGS flags'
            let mut lrcxt = JsonLikeRegexContext::default();
            let larg = jspInitByBuffer(jsp.buffer, jsp.base + jsp.content.like_regex.expr);
            executePredicate(cxt, jsp, &larg, None, jb, false, PredicateKind::LikeRegex(&mut lrcxt))
        }

        jpiExists => {
            let larg = jspGetArg(jsp);

            if jspStrictAbsenceOfErrors(cxt) {
                // In strict mode we must get a complete list of values to check
                // that there are no errors at all.
                let mut vals = JsonValueList::default();
                let res = executeItemOptUnwrapResultNoThrow(cxt, &larg, jb, false, &mut vals)?;
                if jperIsError(res) {
                    return Ok(jpbUnknown);
                }
                Ok(if JsonValueListIsEmpty(&vals) {
                    jpbFalse
                } else {
                    jpbTrue
                })
            } else {
                // found == NULL: stop at the first result.
                let res = executeItemOptUnwrapResultNoThrowExists(cxt, &larg, jb, false)?;
                if jperIsError(res) {
                    return Ok(jpbUnknown);
                }
                Ok(if res == jperOk { jpbTrue } else { jpbFalse })
            }
        }

        _ => Err(elog_error(&format!(
            "invalid boolean jsonpath item type: {}",
            jsp.typ as i32
        ))),
    }
}

/// `executeItemOptUnwrapResultNoThrow` with `found == NULL` semantics for the
/// non-strict EXISTS path: C passes `NULL` for `found`, so the executor stops at
/// the first match and returns `jperOk`/`jperNotFound`.
fn executeItemOptUnwrapResultNoThrowExists(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
) -> PgResult<JsonPathExecResult> {
    debug_assert!(!unwrap);
    let throwErrors = cxt.throwErrors;
    cxt.throwErrors = false;
    let res = executeItem(cxt, jsp, jb, None);
    cxt.throwErrors = throwErrors;
    res
}

/// C: `executeNestedBoolItem` (jsonpath_exec.c:1911).
fn executeNestedBoolItem(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
) -> PgResult<JsonPathBool> {
    let prev = cxt.current.clone();
    cxt.current = jb.clone();
    let res = executeBoolItem(cxt, jsp, jb, false);
    cxt.current = prev;
    res
}

/// C: `executeAnyItem` (jsonpath_exec.c:1932). `jbc` is the container bytes;
/// `jbc_doc_offset` is the byte position of `jbc` within its origin document's
/// root container (so iterated children keep document-relative `.keyvalue()`
/// identities — bookkeeping unique to the safe port).
fn executeAnyItem(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: Option<&JsonPathItem<'_>>,
    jbc: &[u8],
    jbc_doc_offset: i32,
    mut found: Option<&mut JsonValueList>,
    level: u32,
    first: u32,
    last: u32,
    ignoreStructuralErrors: bool,
    unwrapNext: bool,
) -> PgResult<JsonPathExecResult> {
    let mut res = jperNotFound;

    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    if level > last {
        return Ok(res);
    }

    let mut it = JsonbIteratorInitAt(jbc, jbc_doc_offset);
    let mut v = JsonbValue::null();

    // Recursively iterate over jsonb objects/arrays
    loop {
        let mut r = JsonbIteratorNext(&mut it, &mut v, true)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }

        if r == JsonbIteratorToken::WJB_KEY {
            r = JsonbIteratorNext(&mut it, &mut v, true)?;
            debug_assert_eq!(r, JsonbIteratorToken::WJB_VALUE);
        }

        if r == JsonbIteratorToken::WJB_VALUE || r == JsonbIteratorToken::WJB_ELEM {
            if level >= first
                || (first == PG_UINT32_MAX && last == PG_UINT32_MAX && v.typ != jbvType::jbvBinary)
            {
                // check expression
                if let Some(jsp) = jsp {
                    if ignoreStructuralErrors {
                        let savedIgnoreStructuralErrors = cxt.ignoreStructuralErrors;
                        cxt.ignoreStructuralErrors = true;
                        res = executeItemOptUnwrapTarget(
                            cxt,
                            jsp,
                            &v,
                            found.as_deref_mut(),
                            unwrapNext,
                        )?;
                        cxt.ignoreStructuralErrors = savedIgnoreStructuralErrors;
                    } else {
                        res = executeItemOptUnwrapTarget(
                            cxt,
                            jsp,
                            &v,
                            found.as_deref_mut(),
                            unwrapNext,
                        )?;
                    }

                    if jperIsError(res) {
                        break;
                    }
                    if res == jperOk && found.is_none() {
                        break;
                    }
                } else if let Some(found) = found.as_deref_mut() {
                    JsonValueListAppend(found, copyJsonbValue(&v));
                } else {
                    return Ok(jperOk);
                }
            }

            if level < last && v.typ == jbvType::jbvBinary {
                let data = binary_data(&v, "invalid jsonb binary type")?.to_vec();
                let data_off = binary_doc_offset(&v);
                res = executeAnyItem(
                    cxt,
                    jsp,
                    &data,
                    data_off,
                    found.as_deref_mut(),
                    level + 1,
                    first,
                    last,
                    ignoreStructuralErrors,
                    unwrapNext,
                )?;

                if jperIsError(res) {
                    break;
                }
                if res == jperOk && found.is_none() {
                    break;
                }
            }
        }
    }

    Ok(res)
}

/// The predicate callback to use in [`executePredicate`].
enum PredicateKind<'a> {
    /// C: `executeComparison`.
    Comparison,
    /// C: `executeStartsWith`.
    StartsWith,
    /// C: `executeLikeRegex` (with the cached regex context).
    LikeRegex(&'a mut JsonLikeRegexContext),
}

/// C: `executePredicate` (jsonpath_exec.c:2023).
fn executePredicate(
    cxt: &mut JsonPathExecContext<'_, '_>,
    pred: &JsonPathItem<'_>,
    larg: &JsonPathItem<'_>,
    rarg: Option<&JsonPathItem<'_>>,
    jb: &JsonbValue,
    unwrapRightArg: bool,
    mut exec: PredicateKind<'_>,
) -> PgResult<JsonPathBool> {
    let mut error = false;
    let mut found = false;

    // Left argument is always auto-unwrapped.
    let mut lseq = JsonValueList::default();
    let res = executeItemOptUnwrapResultNoThrow(cxt, larg, jb, true, &mut lseq)?;
    if jperIsError(res) {
        return Ok(jpbUnknown);
    }

    let mut rseq = JsonValueList::default();
    if let Some(rarg) = rarg {
        // Right argument is conditionally auto-unwrapped.
        let res = executeItemOptUnwrapResultNoThrow(cxt, rarg, jb, unwrapRightArg, &mut rseq)?;
        if jperIsError(res) {
            return Ok(jpbUnknown);
        }
    }

    let l_items = JsonValueListGetList(&lseq);
    let r_items = JsonValueListGetList(&rseq);

    for lval in &l_items {
        // Loop over right arg sequence or do single pass otherwise.
        if rarg.is_some() {
            for rval in &r_items {
                let res = exec_predicate_call(cxt, &mut exec, pred, lval, Some(rval))?;
                match res {
                    jpbUnknown => {
                        if jspStrictAbsenceOfErrors(cxt) {
                            return Ok(jpbUnknown);
                        }
                        error = true;
                    }
                    jpbTrue => {
                        if !jspStrictAbsenceOfErrors(cxt) {
                            return Ok(jpbTrue);
                        }
                        found = true;
                    }
                    jpbFalse => {}
                }
            }
        } else {
            let res = exec_predicate_call(cxt, &mut exec, pred, lval, None)?;
            match res {
                jpbUnknown => {
                    if jspStrictAbsenceOfErrors(cxt) {
                        return Ok(jpbUnknown);
                    }
                    error = true;
                }
                jpbTrue => {
                    if !jspStrictAbsenceOfErrors(cxt) {
                        return Ok(jpbTrue);
                    }
                    found = true;
                }
                jpbFalse => {}
            }
        }
    }

    if found {
        // possible only in strict mode
        return Ok(jpbTrue);
    }
    if error {
        // possible only in lax mode
        return Ok(jpbUnknown);
    }
    Ok(jpbFalse)
}

/// Dispatch a single predicate call (C: `exec(pred, lval, rval, param)`).
fn exec_predicate_call(
    cxt: &JsonPathExecContext<'_, '_>,
    exec: &mut PredicateKind<'_>,
    pred: &JsonPathItem<'_>,
    lval: &JsonbValue,
    rval: Option<&JsonbValue>,
) -> PgResult<JsonPathBool> {
    match exec {
        PredicateKind::Comparison => executeComparison(pred, lval, rval.unwrap(), cxt),
        PredicateKind::StartsWith => executeStartsWith(lval, rval.unwrap()),
        PredicateKind::LikeRegex(lrcxt) => executeLikeRegex(cxt.mcx, pred, lval, lrcxt),
    }
}

/// The binary arithmetic operation (C: `BinaryArithmFunc`).
#[derive(Clone, Copy)]
enum ArithmOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// The unary arithmetic operation passed to [`executeUnaryArithmExpr`].
#[derive(Clone, Copy)]
enum UnaryOp {
    Minus,
}

/// C: `executeBinaryArithmExpr` (jsonpath_exec.c:2103).
fn executeBinaryArithmExpr(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    func: ArithmOp,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    let elem = jspGetLeftArg(jsp);

    // XXX: By standard only operands of multiplicative expressions are
    // unwrapped. We extend it to other binary arithmetic expressions too.
    let mut lseq = JsonValueList::default();
    let jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &mut lseq)?;
    if jperIsError(jper) {
        return Ok(jper);
    }

    let elem = jspGetRightArg(jsp);
    let mut rseq = JsonValueList::default();
    let jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &mut rseq)?;
    if jperIsError(jper) {
        return Ok(jper);
    }

    let lval = if JsonValueListLength(&lseq) == 1 {
        getScalar(JsonValueListHead(&lseq), jbvType::jbvNumeric).cloned()
    } else {
        None
    };
    let lval = match lval {
        Some(v) => v,
        None => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED)
                    .errmsg(format!(
                        "left operand of jsonpath operator {} is not a single numeric value",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }
    };

    let rval = if JsonValueListLength(&rseq) == 1 {
        getScalar(JsonValueListHead(&rseq), jbvType::jbvNumeric).cloned()
    } else {
        None
    };
    let rval = match rval {
        Some(v) => v,
        None => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_SINGLETON_SQL_JSON_ITEM_REQUIRED)
                    .errmsg(format!(
                        "right operand of jsonpath operator {} is not a single numeric value",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }
    };

    let lnum = numeric_bytes_of(&lval).to_vec();
    let rnum = numeric_bytes_of(&rval).to_vec();

    let res = if jspThrowErrors(cxt) {
        numeric_binop(cxt.mcx, func, &lnum, &rnum)?
    } else {
        match numeric_binop(cxt.mcx, func, &lnum, &rnum) {
            Ok(v) => v,
            Err(_) => return Ok(jperError),
        }
    };

    let next = jspGetNext(jsp);
    if next.is_none() && found.is_none() {
        return Ok(jperOk);
    }

    let lval = JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(res),
    };

    executeNextItem(cxt, jsp, next.as_ref(), &lval, found, false)
}

/// C: `executeUnaryArithmExpr` (jsonpath_exec.c:2174).
fn executeUnaryArithmExpr(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    func: Option<UnaryOp>,
    mut found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    let elem = jspGetArg(jsp);
    let mut seq = JsonValueList::default();
    let jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &mut seq)?;
    if jperIsError(jper) {
        return Ok(jper);
    }

    let mut jper = jperNotFound;
    let next = jspGetNext(jsp);
    let hasNext = next.is_some();

    let items = JsonValueListGetList(&seq);
    for val in items {
        let mut val = val;
        if getScalar(&val, jbvType::jbvNumeric).is_some() {
            if found.is_none() && !hasNext {
                return Ok(jperOk);
            }
        } else {
            if found.is_none() && !hasNext {
                continue; // skip non-numerics processing
            }
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_SQL_JSON_NUMBER_NOT_FOUND)
                    .errmsg(format!(
                        "operand of unary jsonpath operator {} is not a numeric value",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }

        if let Some(UnaryOp::Minus) = func {
            let bytes = numeric_bytes_of(&val).to_vec();
            let var = numeric_uminus_bytes(cxt.mcx, &bytes)?;
            val.val = JsonbValueData::Numeric(var);
        }

        let jper2 = executeNextItem(cxt, jsp, next.as_ref(), &val, found.as_deref_mut(), false)?;

        if jperIsError(jper2) {
            return Ok(jper2);
        }
        if jper2 == jperOk {
            if found.is_none() {
                return Ok(jperOk);
            }
            jper = jperOk;
        }
    }

    Ok(jper)
}

/// C: `executeStartsWith` (jsonpath_exec.c:2241).
fn executeStartsWith(whole: &JsonbValue, initial: &JsonbValue) -> PgResult<JsonPathBool> {
    let whole = match getScalar(whole, jbvType::jbvString) {
        Some(v) => v,
        None => return Ok(jpbUnknown), // error
    };
    let initial = match getScalar(initial, jbvType::jbvString) {
        Some(v) => v,
        None => return Ok(jpbUnknown), // error
    };

    let ws = string_bytes(whole);
    let is = string_bytes(initial);

    if ws.len() >= is.len() && ws[..is.len()] == *is {
        Ok(jpbTrue)
    } else {
        Ok(jpbFalse)
    }
}

/// C: `executeLikeRegex` (jsonpath_exec.c:2265).
fn executeLikeRegex(
    mcx: Mcx<'_>,
    jsp: &JsonPathItem<'_>,
    str: &JsonbValue,
    cxt: &mut JsonLikeRegexContext,
) -> PgResult<JsonPathBool> {
    let str = match getScalar(str, jbvType::jbvString) {
        Some(v) => v,
        None => return Ok(jpbUnknown),
    };

    // Cache regex text and converted flags. `jspConvertRegexFlags` lives in the
    // jsonpath type crate (direct dep).
    if cxt.regex.is_none() {
        let pattern_bytes = like_regex_pattern(jsp).to_vec();
        cxt.regex = Some(pattern_bytes);
        cxt.cflags =
            backend_utils_adt_jsonpath::jspConvertRegexFlags(jsp.content.like_regex.flags)?;
    }

    // C: `RE_compile_and_execute(cxt->regex, str, len, cflags,
    // DEFAULT_COLLATION_OID, 0, NULL)` — a bare match test (nmatch == 0).
    let matched = backend_utils_adt_regexp_seams::RE_compile_and_execute::call(
        mcx,
        cxt.regex.as_deref().unwrap(),
        string_bytes(str),
        cxt.cflags,
        DEFAULT_COLLATION_OID,
        &mut [],
    )?;

    Ok(if matched { jpbTrue } else { jpbFalse })
}

/// The numeric item method (C: the `func` arg of `executeNumericItemMethod`).
#[derive(Clone, Copy)]
enum NumericMethod {
    Abs,
    Floor,
    Ceiling,
}

/// C: `executeNumericItemMethod` (jsonpath_exec.c:2296).
fn executeNumericItemMethod(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    func: NumericMethod,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let jb = match getScalar(jb, jbvType::jbvNumeric) {
        Some(v) => v,
        None => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                    .errmsg(format!(
                        "jsonpath item method .{}() can only be applied to a numeric value",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }
    };

    let result = numeric_unary_method(cxt.mcx, func, numeric_bytes_of(jb))?;

    let next = jspGetNext(jsp);
    if next.is_none() && found.is_none() {
        return Ok(jperOk);
    }

    let jbv = JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(result),
    };

    executeNextItem(cxt, jsp, next.as_ref(), &jbv, found, false)
}

/// C: `executeDateTimeMethod` (jsonpath_exec.c:2337).
fn executeDateTimeMethod(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    let mut res = jperNotFound;

    let jb = match getScalar(jb, jbvType::jbvString) {
        Some(v) => v,
        None => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                    .errmsg(format!(
                        "jsonpath item method .{}() can only be applied to a string",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }
    };

    let datetime = string_bytes(jb).to_vec();
    let collid = DEFAULT_COLLATION_OID;

    let mut parsed: DateTimeValue;
    let mut time_precision = -1i32;

    // .datetime(template) has an argument, the rest don't.
    if jsp.typ == jpiDatetime && jsp.content.arg != 0 {
        let elem = jspGetArg(jsp);
        if elem.typ != jpiString {
            return Err(elog_error(
                "invalid jsonpath item type for .datetime() argument",
            ));
        }
        let template = jspGetString(&elem).to_vec();

        match datetime::parse_datetime(
            cxt.mcx,
            &datetime,
            &template,
            collid,
            jspThrowErrors(cxt),
        )? {
            Some(v) => {
                parsed = v;
                res = jperOk;
            }
            None => {
                parsed = DateTimeValue {
                    value: Datum::null(),
                    typid: 0,
                    typmod: -1,
                    tz: 0,
                };
                res = jperError;
            }
        }
    } else {
        // Check for optional precision for methods other than
        // .datetime()/.date()
        if jsp.typ != jpiDatetime && jsp.typ != jpiDate && jsp.content.arg != 0 {
            let elem = jspGetArg(jsp);
            if elem.typ != jpiNumeric {
                return Err(elog_error(&format!(
                    "invalid jsonpath item type for {} argument",
                    op_name(jsp.typ)?
                )));
            }
            match numeric_int4_opt(cxt.mcx, jspGetNumeric(&elem)) {
                Ok(v) => time_precision = v,
                Err(_) => {
                    return return_error(
                        cxt,
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                            .errmsg(format!(
                                "time precision of jsonpath item method .{}() is out of range for type integer",
                                op_name(jsp.typ)?
                            ))
                            .into_error(),
                    );
                }
            }
        }

        // loop until datetime format fits (ISO formats).
        parsed = DateTimeValue {
            value: Datum::null(),
            typid: 0,
            typmod: -1,
            tz: 0,
        };
        for fmt in DATETIME_FORMATS.iter() {
            if let Some(v) =
                datetime::parse_datetime(cxt.mcx, &datetime, fmt.as_bytes(), collid, false)?
            {
                parsed = v;
                res = jperOk;
                break;
            }
        }

        if res == jperNotFound {
            let dt_cstr = String::from_utf8_lossy(&datetime).into_owned();
            if jsp.typ == jpiDatetime {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                        .errmsg(format!("datetime format is not recognized: \"{dt_cstr}\""))
                        .errhint(
                            "Use a datetime template argument to specify the input data format.",
                        )
                        .into_error(),
                );
            } else {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_ARGUMENT_FOR_SQL_JSON_DATETIME_FUNCTION)
                        .errmsg(format!(
                            "{} format is not recognized: \"{}\"",
                            op_name(jsp.typ)?,
                            dt_cstr
                        ))
                        .into_error(),
                );
            }
        }
    }

    // Convert the parsed value to the method's target type (the big cast switch),
    // applying time precision and useTz checks. This is seamed (fmgr/date-time).
    if jsp.typ != jpiDatetime && res != jperError {
        let dt_cstr = String::from_utf8_lossy(&datetime).into_owned();
        match datetime::datetime_method_cast(
            jsp.typ,
            parsed,
            time_precision,
            cxt.useTz,
            &dt_cstr,
            jspThrowErrors(cxt),
        )? {
            Some(v) => parsed = v,
            None => res = jperError,
        }
    }

    if jperIsError(res) {
        return Ok(res);
    }

    let next = jspGetNext(jsp);
    let hasNext = next.is_some();

    if !hasNext && found.is_none() {
        return Ok(res);
    }

    let jbv = JsonbValue {
        typ: jbvType::jbvDatetime,
        val: JsonbValueData::Datetime(JsonbDatetime {
            value: parsed.value.as_usize(),
            typid: parsed.typid,
            typmod: parsed.typmod,
            tz: parsed.tz,
        }),
    };

    executeNextItem(cxt, jsp, next.as_ref(), &jbv, found, hasNext)
}

/// C: `executeKeyValueMethod` (jsonpath_exec.c:2818).
fn executeKeyValueMethod(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    mut found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    let mut res = jperNotFound;

    if JsonbType(jb)? != jbvType::jbvObject || jb.typ != jbvType::jbvBinary {
        return return_error(
            cxt,
            ereport(ERROR)
                .errcode(ERRCODE_SQL_JSON_OBJECT_NOT_FOUND)
                .errmsg(format!(
                    "jsonpath item method .{}() can only be applied to an object",
                    op_name(jsp.typ)?
                ))
                .into_error(),
        );
    }

    let jbc = binary_data(jb, "invalid jsonb object type")?.to_vec();

    if json_container_size(container_header(&jbc)) == 0 {
        return Ok(jperNotFound); // no key-value pairs
    }

    let next = jspGetNext(jsp);
    let hasNext = next.is_some();

    let keystr = JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(b"key".to_vec()),
    };
    let valstr = JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(b"value".to_vec()),
    };
    let idstr = JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(b"id".to_vec()),
    };

    // construct object id from its base object and offset inside that
    //   C: id = jb->type != jbvBinary ? 0
    //          : (int64) ((char *) jbc - (char *) cxt->baseObject.jbc);
    //      id += (int64) cxt->baseObject.id * INT64CONST(10000000000);
    // `jb->type` is always jbvBinary here (checked above), so the offset branch
    // is taken.
    let mut id: i64 = jbc_identity(jb) - cxt.baseObject.id_addr;
    id += (cxt.baseObject.id as i64) * KEYVALUE_ID_MULT;

    let idval = JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(int64_to_numeric_bytes(cxt.mcx, id)?),
    };

    // The pairs produced here are re-serialized into a fresh object, so their
    // document offsets are immaterial: iterate with offset 0.
    let mut it = JsonbIteratorInitAt(&jbc, 0);
    let mut key = JsonbValue::null();

    loop {
        let tok = JsonbIteratorNext(&mut it, &mut key, true)?;
        if tok == JsonbIteratorToken::WJB_DONE {
            break;
        }
        if tok != JsonbIteratorToken::WJB_KEY {
            continue;
        }

        res = jperOk;

        if !hasNext && found.is_none() {
            break;
        }

        let mut val = JsonbValue::null();
        let tok = JsonbIteratorNext(&mut it, &mut val, true)?;
        debug_assert_eq!(tok, JsonbIteratorToken::WJB_VALUE);

        let mut ps = None;
        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;

        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_KEY, Some(&keystr))?;
        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_VALUE, Some(&key))?;

        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_KEY, Some(&valstr))?;
        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_VALUE, Some(&val))?;

        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_KEY, Some(&idstr))?;
        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_VALUE, Some(&idval))?;

        let keyval = pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_END_OBJECT, None)?
            .expect("WJB_END_OBJECT yields a container value");

        let jsonb = JsonbValueToJsonb(cxt.mcx, &keyval)?;

        let mut obj = JsonbValue::null();
        JsonbInitBinary(&mut obj, &jsonb);

        let lastid = cxt.lastGeneratedObjectId;
        cxt.lastGeneratedObjectId += 1;
        let baseObject = setBaseObject(cxt, &obj, lastid);

        res = executeNextItem(cxt, jsp, next.as_ref(), &obj, found.as_deref_mut(), true)?;

        cxt.baseObject = baseObject;

        if jperIsError(res) {
            return Ok(res);
        }
        if res == jperOk && found.is_none() {
            break;
        }
    }

    Ok(res)
}

/// C: `appendBoolResult` (jsonpath_exec.c:2928).
fn appendBoolResult(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    found: Option<&mut JsonValueList>,
    res: JsonPathBool,
) -> PgResult<JsonPathExecResult> {
    let next = jspGetNext(jsp);
    if next.is_none() && found.is_none() {
        return Ok(jperOk); // found singleton boolean value
    }

    let jbv = if res == jpbUnknown {
        JsonbValue::null()
    } else {
        JsonbValue {
            typ: jbvType::jbvBool,
            val: JsonbValueData::Bool(res == jpbTrue),
        }
    };

    executeNextItem(cxt, jsp, next.as_ref(), &jbv, found, true)
}

// ===========================================================================
// Item / variable access (jsonpath_exec.c:2956-3216)
// ===========================================================================

/// C: `getJsonPathItem` (jsonpath_exec.c:2956).
fn getJsonPathItem(
    cxt: &mut JsonPathExecContext<'_, '_>,
    item: &JsonPathItem<'_>,
    value: &mut JsonbValue,
) -> PgResult<()> {
    match item.typ {
        jpiNull => {
            value.typ = jbvType::jbvNull;
            value.val = JsonbValueData::Null;
        }
        jpiBool => {
            value.typ = jbvType::jbvBool;
            value.val = JsonbValueData::Bool(jspGetBool(item));
        }
        jpiNumeric => {
            value.typ = jbvType::jbvNumeric;
            value.val = JsonbValueData::Numeric(jspGetNumeric(item).to_vec());
        }
        jpiString => {
            value.typ = jbvType::jbvString;
            value.val = JsonbValueData::String(jspGetString(item).to_vec());
        }
        jpiVariable => {
            getJsonPathVariable(cxt, item, value)?;
            return Ok(());
        }
        _ => return Err(elog_error("unexpected jsonpath item type")),
    }
    Ok(())
}

/// C: `GetJsonPathVar` (jsonpath_exec.c:2989) — `JsonPathGetVarCallback` over a
/// `List` of `JsonPathVariable`.
fn GetJsonPathVar(
    mcx: Mcx<'_>,
    vars: &JsonPathVars,
    var_name: &[u8],
    base_object: &mut JsonbValue,
    base_object_id: &mut i32,
) -> PgResult<Option<JsonbValue>> {
    let list = match vars {
        JsonPathVars::List(l) => l,
        _ => {
            *base_object_id = -1;
            return Ok(None);
        }
    };

    let mut id = 1i32;
    let mut found: Option<&JsonPathVariable> = None;
    for curvar in list {
        if curvar.name.len() == var_name.len() && curvar.name == var_name {
            found = Some(curvar);
            break;
        }
        id += 1;
    }

    let var = match found {
        Some(v) => v,
        None => {
            *base_object_id = -1;
            return Ok(None);
        }
    };

    let result = if var.isnull {
        *base_object_id = 0;
        JsonbValue::null()
    } else {
        JsonItemFromDatum(mcx, var.value, var.typid, var.typmod)?
    };

    *base_object = result.clone();
    *base_object_id = id;
    Ok(Some(result))
}

/// C: `CountJsonPathVars` (jsonpath_exec.c:3034) — `return list_length(vars)`.
fn CountJsonPathVars(vars: &JsonPathVars) -> PgResult<i32> {
    Ok(match vars {
        JsonPathVars::List(l) => l.len() as i32,
        _ => 0,
    })
}

/// C: `JsonItemFromDatum` (jsonpath_exec.c:3047) — coerce a SQL `Datum` of a
/// known type into a `JsonbValue`.
///
/// The pure arms are in-crate: `BOOLOID`; the `DATE/TIME/TIMETZ/TIMESTAMP/
/// TIMESTAMPTZ` field-assignment arm; and the `default` arm's "could not convert
/// value of type %s to jsonpath" error (using the seamed `format_type_be`). The
/// numeric/int/float/text/varchar/jsonb/json arms (varlena interpretation +
/// fmgr coercions) stay behind the seam.
fn JsonItemFromDatum(mcx: Mcx<'_>, val: Datum, typid: Oid, typmod: i32) -> PgResult<JsonbValue> {
    use types_tuple::heaptuple::{
        BOOLOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, JSONBOID, JSONOID,
        NUMERICOID, TEXTOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, VARCHAROID,
    };

    match typid {
        BOOLOID => Ok(JsonbValue {
            typ: jbvType::jbvBool,
            // C: `res->val.boolean = DatumGetBool(val)`.
            val: JsonbValueData::Bool(val.as_usize() != 0),
        }),
        DATEOID | TIMEOID | TIMETZOID | TIMESTAMPOID | TIMESTAMPTZOID => Ok(JsonbValue {
            typ: jbvType::jbvDatetime,
            val: JsonbValueData::Datetime(JsonbDatetime {
                value: val.as_usize(),
                typid,
                typmod,
                tz: 0,
            }),
        }),
        NUMERICOID | INT2OID | INT4OID | INT8OID | FLOAT4OID | FLOAT8OID | TEXTOID | VARCHAROID
        | JSONBOID | JSONOID => seam::json_item_from_datum::call(val, typid, typmod),
        _ => Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "could not convert value of type {} to jsonpath",
                backend_utils_adt_format_type_seams::format_type_be::call(mcx, typid)?
                    .as_str()
            ))
            .into_error()),
    }
}

/// C: `getJsonPathVariable` (jsonpath_exec.c:3139).
fn getJsonPathVariable(
    cxt: &mut JsonPathExecContext<'_, '_>,
    variable: &JsonPathItem<'_>,
    value: &mut JsonbValue,
) -> PgResult<()> {
    debug_assert_eq!(variable.typ, jpiVariable);
    let var_name = jspGetString(variable);

    let mut baseObject = JsonbValue::null();
    let mut baseObjectId = 0i32;

    let v = if matches!(cxt.vars, JsonPathVars::None) {
        None
    } else {
        (cxt.getVar)(cxt.mcx, cxt.vars, var_name, &mut baseObject, &mut baseObjectId)?
    };

    let v = match v {
        Some(v) => v,
        None => {
            let name = String::from_utf8_lossy(var_name).into_owned();
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("could not find jsonpath variable \"{name}\""))
                .into_error());
        }
    };

    if baseObjectId > 0 {
        *value = v;
        setBaseObject(cxt, &baseObject, baseObjectId);
    }
    Ok(())
}

/// C: `getJsonPathVariableFromJsonb` (jsonpath_exec.c:3171).
fn getJsonPathVariableFromJsonb(
    _mcx: Mcx<'_>,
    vars: &JsonPathVars,
    var_name: &[u8],
    base_object: &mut JsonbValue,
    base_object_id: &mut i32,
) -> PgResult<Option<JsonbValue>> {
    let vars_bytes = match vars {
        JsonPathVars::Jsonb(b) => b,
        _ => {
            *base_object_id = -1;
            return Ok(None);
        }
    };

    let tmp = JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(var_name.to_vec()),
    };

    let result = findJsonbValueFromContainer(jsonb_root(vars_bytes), JB_FOBJECT, &tmp)?;

    match result {
        None => {
            *base_object_id = -1;
            Ok(None)
        }
        Some(result) => {
            *base_object_id = 1;
            JsonbInitBinary(base_object, vars_bytes);
            Ok(Some(result))
        }
    }
}

/// C: `countVariablesFromJsonb` (jsonpath_exec.c:3201).
fn countVariablesFromJsonb(vars: &JsonPathVars) -> PgResult<i32> {
    match vars {
        JsonPathVars::Jsonb(b) => {
            if !json_container_is_object(container_header(jsonb_root(b))) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("\"vars\" argument is not an object")
                    .errdetail(
                        "Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.",
                    )
                    .into_error());
            }
            // count of base objects
            Ok(1)
        }
        // C: `vars != NULL ? 1 : 0`. A NULL `vars` does not flow through this
        // jsonb-specific callback.
        _ => Ok(0),
    }
}

/// C: `JsonbArraySize` (jsonpath_exec.c:3223).
fn JsonbArraySize(jb: &JsonbValue) -> PgResult<i32> {
    debug_assert!(jb.typ != jbvType::jbvArray);

    if jb.typ == jbvType::jbvBinary {
        let jbc = binary_data(jb, "invalid jsonb binary type")?;
        let hdr = container_header(jbc);
        if json_container_is_array(hdr) && !json_container_is_scalar(hdr) {
            return Ok(json_container_size(hdr) as i32);
        }
    }
    Ok(-1)
}

// ===========================================================================
// Comparisons (jsonpath_exec.c:3239-3488)
// ===========================================================================

/// C: `executeComparison` (jsonpath_exec.c:3240) — `JsonPathPredicateCallback`.
fn executeComparison(
    cmp: &JsonPathItem<'_>,
    lv: &JsonbValue,
    rv: &JsonbValue,
    cxt: &JsonPathExecContext<'_, '_>,
) -> PgResult<JsonPathBool> {
    compareItems(cxt.mcx, cmp.typ, lv, rv, cxt.useTz)
}

/// C: `binaryCompareStrings` (jsonpath_exec.c:3251).
fn binaryCompareStrings(s1: &[u8], s2: &[u8]) -> i32 {
    let n = s1.len().min(s2.len());
    let cmp = memcmp(&s1[..n], &s2[..n]);
    if cmp != 0 {
        return cmp;
    }
    if s1.len() == s2.len() {
        return 0;
    }
    if s1.len() < s2.len() {
        -1
    } else {
        1
    }
}

/// C: `compareStrings` (jsonpath_exec.c:3272).
///
/// The ASCII/UTF-8 fast path is in-crate (a plain byte comparison; UTF-8 byte
/// order equals codepoint order). The other-encoding path routes both operands
/// through `pg_server_to_any` (mbutils.c, reached through its owner seam).
fn compareStrings(mcx: Mcx<'_>, mbstr1: &[u8], mbstr2: &[u8]) -> PgResult<i32> {
    const PG_SQL_ASCII: i32 = 0;
    const PG_UTF8: i32 = 6;

    let enc = backend_utils_mb_mbutils_seams::get_database_encoding::call();
    if enc == PG_SQL_ASCII || enc == PG_UTF8 {
        // UTF-8 per-byte comparison matches codepoint comparison; ASCII is a
        // special case of UTF-8.
        return Ok(binaryCompareStrings(mbstr1, mbstr2));
    }

    // We have to convert other encodings to UTF-8 first, then compare.
    let utf8str1 = backend_utils_mb_mbutils_seams::pg_server_to_any::call(mcx, mbstr1, PG_UTF8)?;
    let utf8str2 = backend_utils_mb_mbutils_seams::pg_server_to_any::call(mcx, mbstr2, PG_UTF8)?;
    let bytes1: &[u8] = utf8str1.as_deref().unwrap_or(mbstr1);
    let bytes2: &[u8] = utf8str2.as_deref().unwrap_or(mbstr2);

    let cmp = binaryCompareStrings(bytes1, bytes2);

    // If pg_server_to_any() did no real conversion we already compared the
    // original strings.
    if utf8str1.is_none() && utf8str2.is_none() {
        return Ok(cmp);
    }

    // When all Unicode codepoints are equal, return the binary comparison.
    if cmp == 0 {
        Ok(binaryCompareStrings(mbstr1, mbstr2))
    } else {
        Ok(cmp)
    }
}

/// C: `compareItems` (jsonpath_exec.c:3339).
fn compareItems(
    mcx: Mcx<'_>,
    op: JsonPathItemType,
    jb1: &JsonbValue,
    jb2: &JsonbValue,
    useTz: bool,
) -> PgResult<JsonPathBool> {
    let cmp: i32;

    if jb1.typ != jb2.typ {
        if jb1.typ == jbvType::jbvNull || jb2.typ == jbvType::jbvNull {
            // Equality and order comparison of nulls to non-nulls returns always
            // false, but inequality comparison returns true.
            return Ok(if op == jpiNotEqual { jpbTrue } else { jpbFalse });
        }
        // Non-null items of different types are not comparable.
        return Ok(jpbUnknown);
    }

    match jb1.typ {
        jbvType::jbvNull => {
            cmp = 0;
        }
        jbvType::jbvBool => {
            let b1 = bool_of(jb1);
            let b2 = bool_of(jb2);
            cmp = if b1 == b2 {
                0
            } else if b1 {
                1
            } else {
                -1
            };
        }
        jbvType::jbvNumeric => {
            cmp = compareNumeric(mcx, numeric_bytes_of(jb1), numeric_bytes_of(jb2));
        }
        jbvType::jbvString => {
            let s1 = string_bytes(jb1);
            let s2 = string_bytes(jb2);
            if op == jpiEqual {
                return Ok(if s1.len() != s2.len() || memcmp(s1, s2) != 0 {
                    jpbFalse
                } else {
                    jpbTrue
                });
            }
            cmp = compareStrings(mcx, s1, s2)?;
        }
        jbvType::jbvDatetime => {
            let d1 = datetime_of(jb1);
            let d2 = datetime_of(jb2);
            // C: `compareDatetime(jb1->val.datetime.value, ...typid..., useTz)`.
            // The datetime word is stored as a `usize`; `timetz` carries its
            // zone in the separate `tz` field (the lossless by-value split).
            let op1 = datetime::DtOperand {
                value: Datum::from_usize(d1.value),
                typid: d1.typid,
                tz: d1.tz,
            };
            let op2 = datetime::DtOperand {
                value: Datum::from_usize(d2.value),
                typid: d2.typid,
                tz: d2.tz,
            };
            match datetime::compare_datetime(op1, op2, useTz)? {
                Some(c) => cmp = c,
                None => return Ok(jpbUnknown),
            }
        }
        jbvType::jbvBinary | jbvType::jbvArray | jbvType::jbvObject => {
            // non-scalars are not comparable.
            return Ok(jpbUnknown);
        }
    }

    let res = match op {
        jpiEqual => cmp == 0,
        jpiNotEqual => cmp != 0,
        jpiLess => cmp < 0,
        jpiGreater => cmp > 0,
        jpiLessOrEqual => cmp <= 0,
        jpiGreaterOrEqual => cmp >= 0,
        _ => {
            return Err(elog_error(&format!(
                "unrecognized jsonpath operation: {}",
                op as i32
            )));
        }
    };

    Ok(if res { jpbTrue } else { jpbFalse })
}

/// C: `compareNumeric` (jsonpath_exec.c:3435).
fn compareNumeric(_mcx: Mcx<'_>, a: &[u8], b: &[u8]) -> i32 {
    use backend_utils_adt_numeric::ops_sql::numeric_cmp;
    use core::cmp::Ordering;
    match numeric_cmp(a, b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// C: `copyJsonbValue` (jsonpath_exec.c:3443).
fn copyJsonbValue(src: &JsonbValue) -> JsonbValue {
    src.clone()
}

/// C: `getArrayIndex` (jsonpath_exec.c:3457).
fn getArrayIndex(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    index: &mut i32,
) -> PgResult<JsonPathExecResult> {
    let mut found = JsonValueList::default();
    let res = executeItem(cxt, jsp, jb, Some(&mut found))?;
    if jperIsError(res) {
        return Ok(res);
    }

    let jbv = if JsonValueListLength(&found) == 1 {
        getScalar(JsonValueListHead(&found), jbvType::jbvNumeric).cloned()
    } else {
        None
    };
    let jbv = match jbv {
        Some(v) => v,
        None => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT)
                    .errmsg("jsonpath array subscript is not a single numeric value")
                    .into_error(),
            );
        }
    };

    // numeric_index = numeric_trunc(jbv, 0); *index = numeric_int4_opt_error(...)
    let truncated = numeric_trunc_bytes(cxt.mcx, numeric_bytes_of(&jbv), 0)?;
    match numeric_int4_opt(cxt.mcx, &truncated) {
        Ok(v) => *index = v,
        Err(_) => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_INVALID_SQL_JSON_SUBSCRIPT)
                    .errmsg("jsonpath array subscript is out of integer range")
                    .into_error(),
            );
        }
    }

    Ok(jperOk)
}

// ===========================================================================
// Base object + JsonValueList helpers (jsonpath_exec.c:3491-3593)
// ===========================================================================

/// C: `setBaseObject` (jsonpath_exec.c:3492).
fn setBaseObject(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jbv: &JsonbValue,
    id: i32,
) -> JsonBaseObjectInfo {
    let baseObject = cxt.baseObject.clone();

    if jbv.typ == jbvType::jbvBinary {
        let data = match &jbv.val {
            JsonbValueData::Binary { data, .. } => data.clone(),
            _ => Vec::new(),
        };
        cxt.baseObject.id_addr = jbc_identity(jbv);
        cxt.baseObject.jbc = Some(data);
    } else {
        cxt.baseObject.jbc = None;
        cxt.baseObject.id_addr = 0;
    }
    cxt.baseObject.id = id;

    baseObject
}

/// C: `JsonValueListClear` (jsonpath_exec.c:3504).
fn JsonValueListClear(jvl: &mut JsonValueList) {
    jvl.singleton = None;
    jvl.list = Vec::new();
}

/// C: `JsonValueListAppend` (jsonpath_exec.c:3511).
fn JsonValueListAppend(jvl: &mut JsonValueList, jbv: JsonbValue) {
    if let Some(singleton) = jvl.singleton.take() {
        jvl.list = vec![singleton, jbv];
    } else if jvl.list.is_empty() {
        jvl.singleton = Some(jbv);
    } else {
        jvl.list.push(jbv);
    }
}

/// C: `JsonValueListLength` (jsonpath_exec.c:3525).
fn JsonValueListLength(jvl: &JsonValueList) -> i32 {
    if jvl.singleton.is_some() {
        1
    } else {
        jvl.list.len() as i32
    }
}

/// C: `JsonValueListIsEmpty` (jsonpath_exec.c:3531).
fn JsonValueListIsEmpty(jvl: &JsonValueList) -> bool {
    jvl.singleton.is_none() && jvl.list.is_empty()
}

/// C: `JsonValueListHead` (jsonpath_exec.c:3537).
fn JsonValueListHead(jvl: &JsonValueList) -> &JsonbValue {
    if let Some(s) = &jvl.singleton {
        s
    } else {
        &jvl.list[0]
    }
}

/// C: `JsonValueListGetList` (jsonpath_exec.c:3543).
fn JsonValueListGetList(jvl: &JsonValueList) -> Vec<JsonbValue> {
    if let Some(s) = &jvl.singleton {
        vec![s.clone()]
    } else {
        jvl.list.clone()
    }
}

/// C: `JsonValueListInitIterator` (jsonpath_exec.c:3552).
fn JsonValueListInitIterator(jvl: &JsonValueList) -> JsonValueListIterator {
    JsonValueListIterator {
        items: JsonValueListGetList(jvl),
        pos: 0,
    }
}

/// C: `JsonValueListNext` (jsonpath_exec.c:3578).
fn JsonValueListNext(it: &mut JsonValueListIterator) -> Option<JsonbValue> {
    if it.pos < it.items.len() {
        let v = it.items[it.pos].clone();
        it.pos += 1;
        Some(v)
    } else {
        None
    }
}

/// C: `JsonbInitBinary` (jsonpath_exec.c:3599). `jb` is the full on-disk jsonb
/// varlena bytes.
fn JsonbInitBinary(jbv: &mut JsonbValue, jb: &[u8]) {
    // C never fails here (just stores binary.data/binary.len). The idiomatic
    // helper is fallible only on a structurally-impossible short slice; on the
    // execution path `jb` is always a valid serialized jsonb.
    let _ = JsonbToJsonbValue(jb, jbv);
}

/// C: `JsonbType` (jsonpath_exec.c:3612). Never returns `jbvBinary` as is.
fn JsonbType(jb: &JsonbValue) -> PgResult<jbvType> {
    let mut typ = jb.typ;

    if jb.typ == jbvType::jbvBinary {
        let jbc = binary_data(jb, "invalid jsonb binary type")?;
        let hdr = container_header(jbc);
        // Scalars should be always extracted during jsonpath execution.
        debug_assert!(!json_container_is_scalar(hdr));

        if json_container_is_object(hdr) {
            typ = jbvType::jbvObject;
        } else if json_container_is_array(hdr) {
            typ = jbvType::jbvArray;
        } else {
            return Err(elog_error(&format!(
                "invalid jsonb container type: 0x{hdr:08x}"
            )));
        }
    }

    Ok(typ)
}

/// C: `getScalar` (jsonpath_exec.c:3636).
fn getScalar(scalar: &JsonbValue, typ: jbvType) -> Option<&JsonbValue> {
    // Scalars should be always extracted during jsonpath execution.
    debug_assert!(
        scalar.typ != jbvType::jbvBinary
            || !matches!(&scalar.val, JsonbValueData::Binary { data, .. }
                if json_container_is_scalar(container_header(data)))
    );

    if scalar.typ == typ {
        Some(scalar)
    } else {
        None
    }
}

/// C: `wrapItemsInArray` (jsonpath_exec.c:3647).
fn wrapItemsInArray(items: &JsonValueList) -> PgResult<JsonbValue> {
    let mut ps = None;
    pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;

    let mut it = JsonValueListInitIterator(items);
    while let Some(jbv) = JsonValueListNext(&mut it) {
        pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_ELEM, Some(&jbv))?;
    }

    Ok(pushJsonbValue(&mut ps, JsonbIteratorToken::WJB_END_ARRAY, None)?
        .expect("WJB_END_ARRAY yields a container value"))
}

// ===========================================================================
// Public path-evaluation entrypoints (jsonpath_exec.c:3886-4080)
// ===========================================================================

/// Result of [`JsonPathExists`]: matched-or-not, with an optional suppressed
/// error flag (C's `bool *error`).
#[derive(Clone, Copy, Debug)]
pub struct JsonPathExistsResult {
    /// Whether the path matched (C return value).
    pub matched: bool,
    /// Whether an error was suppressed (C `*error`).
    pub error: bool,
}

/// C: `JsonPathExists` (jsonpath_exec.c:3886) — used by `JSON_EXISTS` and GIN.
pub fn JsonPathExists(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    suppress_errors: bool,
    vars: &JsonPathVars,
) -> PgResult<JsonPathExistsResult> {
    let res = executeJsonPath(
        mcx,
        jp,
        vars,
        GetJsonPathVar,
        CountJsonPathVars,
        jb,
        !suppress_errors,
        None,
        true,
    )?;

    debug_assert!(suppress_errors || !jperIsError(res));

    let error = suppress_errors && jperIsError(res);
    Ok(JsonPathExistsResult {
        matched: res == jperOk,
        error,
    })
}

/// Result of [`JsonPathQuery`].
#[derive(Clone, Debug)]
pub struct JsonPathQueryResult {
    /// The result jsonb (on-disk varlena bytes), or `None` for SQL NULL.
    pub value: Option<Vec<u8>>,
    /// Whether no match was found (C `*empty`).
    pub empty: bool,
    /// Whether an error was suppressed (C `*error`).
    pub error: bool,
}

/// C: `JsonPathQuery` (jsonpath_exec.c:3909) — used by `JSON_QUERY`.
pub fn JsonPathQuery(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    wrapper: JsonWrapper,
    suppress_errors: bool,
    vars: &JsonPathVars,
    column_name: Option<&str>,
) -> PgResult<JsonPathQueryResult> {
    let mut found = JsonValueList::default();
    let res = executeJsonPath(
        mcx,
        jp,
        vars,
        GetJsonPathVar,
        CountJsonPathVars,
        jb,
        !suppress_errors,
        Some(&mut found),
        true,
    )?;
    debug_assert!(suppress_errors || !jperIsError(res));
    if suppress_errors && jperIsError(res) {
        return Ok(JsonPathQueryResult {
            value: None,
            empty: false,
            error: true,
        });
    }

    let count = JsonValueListLength(&found);
    let singleton = if count > 0 {
        Some(JsonValueListHead(&found).clone())
    } else {
        None
    };

    // Branch order preserved verbatim from C; the first two arms intentionally
    // both yield `false`.
    #[allow(clippy::if_same_then_else)]
    let wrap = if singleton.is_none() {
        false
    } else if wrapper == JsonWrapper::JSW_NONE || wrapper == JsonWrapper::JSW_UNSPEC {
        false
    } else if wrapper == JsonWrapper::JSW_UNCONDITIONAL {
        true
    } else if wrapper == JsonWrapper::JSW_CONDITIONAL {
        count > 1
    } else {
        return Err(elog_error(&format!(
            "unrecognized json wrapper {}",
            wrapper as i32
        )));
    };

    if wrap {
        return Ok(JsonPathQueryResult {
            value: Some(JsonbValueToJsonb(mcx, &wrapItemsInArray(&found)?)?),
            empty: false,
            error: false,
        });
    }

    // No wrapping means only one item is expected.
    if count > 1 {
        if suppress_errors {
            return Ok(JsonPathQueryResult {
                value: None,
                empty: false,
                error: true,
            });
        }

        if let Some(column_name) = column_name {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM)
                .errmsg(format!(
                    "JSON path expression for column \"{column_name}\" must return single item when no wrapper is requested"
                ))
                .errhint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.")
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM)
                .errmsg("JSON path expression in JSON_QUERY must return single item when no wrapper is requested")
                .errhint("Use the WITH WRAPPER clause to wrap SQL/JSON items into an array.")
                .into_error());
        }
    }

    if let Some(singleton) = singleton {
        return Ok(JsonPathQueryResult {
            value: Some(JsonbValueToJsonb(mcx, &singleton)?),
            empty: false,
            error: false,
        });
    }

    Ok(JsonPathQueryResult {
        value: None,
        empty: true,
        error: false,
    })
}

/// Result of [`JsonPathValue`].
#[derive(Clone, Debug)]
pub struct JsonPathValueResult {
    /// The single scalar `JsonbValue`, or `None` for SQL NULL / empty.
    pub value: Option<JsonbValue>,
    /// Whether no match was found (C `*empty`).
    pub empty: bool,
    /// Whether an error was suppressed (C `*error`).
    pub error: bool,
}

/// C: `JsonPathValue` (jsonpath_exec.c:4003) — used by `JSON_VALUE`.
pub fn JsonPathValue(
    mcx: Mcx<'_>,
    jb: &[u8],
    jp: &[u8],
    suppress_errors: bool,
    vars: &JsonPathVars,
    column_name: Option<&str>,
) -> PgResult<JsonPathValueResult> {
    let mut found = JsonValueList::default();
    let jper = executeJsonPath(
        mcx,
        jp,
        vars,
        GetJsonPathVar,
        CountJsonPathVars,
        jb,
        !suppress_errors,
        Some(&mut found),
        true,
    )?;
    debug_assert!(suppress_errors || !jperIsError(jper));

    if suppress_errors && jperIsError(jper) {
        return Ok(JsonPathValueResult {
            value: None,
            empty: false,
            error: true,
        });
    }

    let count = JsonValueListLength(&found);
    let empty = count == 0;

    if empty {
        return Ok(JsonPathValueResult {
            value: None,
            empty: true,
            error: false,
        });
    }

    // JSON_VALUE expects to get only singletons.
    if count > 1 {
        if suppress_errors {
            return Ok(JsonPathValueResult {
                value: None,
                empty: false,
                error: true,
            });
        }
        return Err(more_than_one_scalar_error(column_name));
    }

    let mut res = JsonValueListHead(&found).clone();
    if res.typ == jbvType::jbvBinary {
        let is_scalar = match &res.val {
            JsonbValueData::Binary { data, .. } => json_container_is_scalar(container_header(data)),
            _ => false,
        };
        if is_scalar {
            let data = match &res.val {
                JsonbValueData::Binary { data, .. } => data.clone(),
                _ => unreachable!(),
            };
            JsonbExtractScalar(&data, &mut res)?;
        }
    }

    // JSON_VALUE expects to get only scalars.
    if !is_a_jsonb_scalar(res.typ) {
        if suppress_errors {
            return Ok(JsonPathValueResult {
                value: None,
                empty: false,
                error: true,
            });
        }
        return Err(scalar_required_error(column_name));
    }

    if res.typ == jbvType::jbvNull {
        return Ok(JsonPathValueResult {
            value: None,
            empty: false,
            error: false,
        });
    }

    Ok(JsonPathValueResult {
        value: Some(res),
        empty: false,
        error: false,
    })
}

fn more_than_one_scalar_error(column_name: Option<&str>) -> PgError {
    if let Some(column_name) = column_name {
        ereport(ERROR)
            .errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM)
            .errmsg(format!(
                "JSON path expression for column \"{column_name}\" must return single scalar item"
            ))
            .into_error()
    } else {
        ereport(ERROR)
            .errcode(ERRCODE_MORE_THAN_ONE_SQL_JSON_ITEM)
            .errmsg("JSON path expression in JSON_VALUE must return single scalar item")
            .into_error()
    }
}

fn scalar_required_error(column_name: Option<&str>) -> PgError {
    if let Some(column_name) = column_name {
        ereport(ERROR)
            .errcode(ERRCODE_SQL_JSON_SCALAR_REQUIRED)
            .errmsg(format!(
                "JSON path expression for column \"{column_name}\" must return single scalar item"
            ))
            .into_error()
    } else {
        ereport(ERROR)
            .errcode(ERRCODE_SQL_JSON_SCALAR_REQUIRED)
            .errmsg("JSON path expression in JSON_VALUE must return single scalar item")
            .into_error()
    }
}

// ===========================================================================
// Item-method helpers split out of the big switch for readability. Each is a
// faithful port of the corresponding `case` body in executeItemOptUnwrapTarget.
// ===========================================================================

/// C: the `jpiDouble` case (jsonpath_exec.c:1141).
fn execute_double(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let mut res = jperNotFound;
    let mut out = jb.clone();

    if jb.typ == jbvType::jbvNumeric {
        let tmp = numeric_out(cxt.mcx, numeric_bytes_of(jb))?;
        match soft_float8in_internal(&tmp)? {
            Some(val) => {
                if val.is_infinite() || val.is_nan() {
                    return return_error(
                        cxt,
                        ereport(ERROR)
                            .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                            .errmsg(format!(
                                "NaN or Infinity is not allowed for jsonpath item method .{}()",
                                op_name(jsp.typ)?
                            ))
                            .into_error(),
                    );
                }
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            tmp,
                            op_name(jsp.typ)?,
                            "double precision"
                        ))
                        .into_error(),
                );
            }
        }
    } else if jb.typ == jbvType::jbvString {
        // cast string as double
        let tmp = String::from_utf8_lossy(string_bytes(jb)).into_owned();
        match soft_float8in_internal(&tmp)? {
            Some(val) => {
                if val.is_infinite() || val.is_nan() {
                    return return_error(
                        cxt,
                        ereport(ERROR)
                            .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                            .errmsg(format!(
                                "NaN or Infinity is not allowed for jsonpath item method .{}()",
                                op_name(jsp.typ)?
                            ))
                            .into_error(),
                    );
                }
                out = JsonbValue {
                    typ: jbvType::jbvNumeric,
                    val: JsonbValueData::Numeric(float8_to_numeric_bytes(cxt.mcx, val)?),
                };
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            tmp,
                            op_name(jsp.typ)?,
                            "double precision"
                        ))
                        .into_error(),
                );
            }
        }
    }

    if res == jperNotFound {
        return return_error(
            cxt,
            ereport(ERROR)
                .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                .errmsg(format!(
                    "jsonpath item method .{}() can only be applied to a string or numeric value",
                    op_name(jsp.typ)?
                ))
                .into_error(),
        );
    }

    executeNextItem(cxt, jsp, None, &out, found, true)
}

/// C: the `jpiBigint` case (jsonpath_exec.c:1261).
fn execute_bigint(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let mut res = jperNotFound;
    let datum: i64;

    if jb.typ == jbvType::jbvNumeric {
        match numeric_int8_opt(cxt.mcx, numeric_bytes_of(jb)) {
            Ok(v) => {
                datum = v;
                res = jperOk;
            }
            Err(_) => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            numeric_out(cxt.mcx, numeric_bytes_of(jb))?,
                            op_name(jsp.typ)?,
                            "bigint"
                        ))
                        .into_error(),
                );
            }
        }
    } else if jb.typ == jbvType::jbvString {
        let tmp = String::from_utf8_lossy(string_bytes(jb)).into_owned();
        match soft_int8in(&tmp)? {
            Some(v) => {
                datum = v;
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            tmp,
                            op_name(jsp.typ)?,
                            "bigint"
                        ))
                        .into_error(),
                );
            }
        }
    } else {
        datum = 0;
    }

    if res == jperNotFound {
        return return_error(
            cxt,
            ereport(ERROR)
                .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                .errmsg(format!(
                    "jsonpath item method .{}() can only be applied to a string or numeric value",
                    op_name(jsp.typ)?
                ))
                .into_error(),
        );
    }

    let out = JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(int64_to_numeric_bytes(cxt.mcx, datum)?),
    };

    executeNextItem(cxt, jsp, None, &out, found, true)
}

/// C: the `jpiBoolean` case (jsonpath_exec.c:1324).
fn execute_boolean(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let mut res = jperNotFound;
    let mut bval = false;

    if jb.typ == jbvType::jbvBool {
        bval = bool_of(jb);
        res = jperOk;
    } else if jb.typ == jbvType::jbvNumeric {
        let tmp = numeric_out(cxt.mcx, numeric_bytes_of(jb))?;
        match soft_int4in(&tmp)? {
            Some(ival) => {
                bval = ival != 0;
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            tmp,
                            op_name(jsp.typ)?,
                            "boolean"
                        ))
                        .into_error(),
                );
            }
        }
    } else if jb.typ == jbvType::jbvString {
        let tmp = String::from_utf8_lossy(string_bytes(jb)).into_owned();
        match soft_parse_bool(&tmp) {
            Some(b) => {
                bval = b;
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            tmp,
                            op_name(jsp.typ)?,
                            "boolean"
                        ))
                        .into_error(),
                );
            }
        }
    }

    if res == jperNotFound {
        return return_error(
            cxt,
            ereport(ERROR)
                .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                .errmsg(format!(
                    "jsonpath item method .{}() can only be applied to a boolean, string, or numeric value",
                    op_name(jsp.typ)?
                ))
                .into_error(),
        );
    }

    let out = JsonbValue {
        typ: jbvType::jbvBool,
        val: JsonbValueData::Bool(bval),
    };

    executeNextItem(cxt, jsp, None, &out, found, true)
}

/// C: the `jpiDecimal`/`jpiNumber` case (jsonpath_exec.c:1396).
fn execute_decimal_number(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let mut res = jperNotFound;
    let mut num: Vec<u8>;
    let mut numstr: Option<String> = None;

    if jb.typ == jbvType::jbvNumeric {
        num = numeric_bytes_of(jb).to_vec();
        if numeric_is_nan_or_inf(&num) {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                    .errmsg(format!(
                        "NaN or Infinity is not allowed for jsonpath item method .{}()",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }
        if jsp.typ == jpiDecimal {
            numstr = Some(numeric_out(cxt.mcx, &num)?);
        }
        res = jperOk;
    } else if jb.typ == jbvType::jbvString {
        let s = String::from_utf8_lossy(string_bytes(jb)).into_owned();
        match soft_numeric_in_with_typmod(cxt.mcx, &s, -1, 0)? {
            Some(bytes) => {
                num = bytes;
                if numeric_is_nan_or_inf(&num) {
                    return return_error(
                        cxt,
                        ereport(ERROR)
                            .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                            .errmsg(format!(
                                "NaN or Infinity is not allowed for jsonpath item method .{}()",
                                op_name(jsp.typ)?
                            ))
                            .into_error(),
                    );
                }
                numstr = Some(s);
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            s,
                            op_name(jsp.typ)?,
                            "numeric"
                        ))
                        .into_error(),
                );
            }
        }
    } else {
        num = Vec::new();
    }

    if res == jperNotFound {
        return return_error(
            cxt,
            ereport(ERROR)
                .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                .errmsg(format!(
                    "jsonpath item method .{}() can only be applied to a string or numeric value",
                    op_name(jsp.typ)?
                ))
                .into_error(),
        );
    }

    // .decimal(precision[, scale]): convert args to a typmod and truncate.
    if jsp.typ == jpiDecimal && jsp.content.args.left != 0 {
        let elem = jspGetLeftArg(jsp);
        if elem.typ != jpiNumeric {
            return Err(elog_error(
                "invalid jsonpath item type for .decimal() precision",
            ));
        }
        let precision = match numeric_int4_opt(cxt.mcx, jspGetNumeric(&elem)) {
            Ok(v) => v,
            Err(_) => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "precision of jsonpath item method .{}() is out of range for type integer",
                            op_name(jsp.typ)?
                        ))
                        .into_error(),
                );
            }
        };

        let mut scale = 0i32;
        if jsp.content.args.right != 0 {
            let elem = jspGetRightArg(jsp);
            if elem.typ != jpiNumeric {
                return Err(elog_error(
                    "invalid jsonpath item type for .decimal() scale",
                ));
            }
            scale = match numeric_int4_opt(cxt.mcx, jspGetNumeric(&elem)) {
                Ok(v) => v,
                Err(_) => {
                    return return_error(
                        cxt,
                        ereport(ERROR)
                            .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                            .errmsg(format!(
                                "scale of jsonpath item method .{}() is out of range for type integer",
                                op_name(jsp.typ)?
                            ))
                            .into_error(),
                    );
                }
            };
        }

        // Convert numstr to Numeric with typmod (numeric_in with typmod).
        let numstr = numstr.expect("numstr is set when reaching the .decimal() typmod path");
        match soft_numeric_in_with_typmod(cxt.mcx, &numstr, precision, scale)? {
            Some(bytes) => num = bytes,
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            numstr,
                            op_name(jsp.typ)?,
                            "numeric"
                        ))
                        .into_error(),
                );
            }
        }
    }

    let out = JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(num),
    };

    executeNextItem(cxt, jsp, None, &out, found, true)
}

/// C: the `jpiInteger` case (jsonpath_exec.c:1542).
fn execute_integer(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let mut res = jperNotFound;
    let datum: i32;

    if jb.typ == jbvType::jbvNumeric {
        match numeric_int4_opt(cxt.mcx, numeric_bytes_of(jb)) {
            Ok(v) => {
                datum = v;
                res = jperOk;
            }
            Err(_) => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            numeric_out(cxt.mcx, numeric_bytes_of(jb))?,
                            op_name(jsp.typ)?,
                            "integer"
                        ))
                        .into_error(),
                );
            }
        }
    } else if jb.typ == jbvType::jbvString {
        let tmp = String::from_utf8_lossy(string_bytes(jb)).into_owned();
        match soft_int4in(&tmp)? {
            Some(v) => {
                datum = v;
                res = jperOk;
            }
            None => {
                return return_error(
                    cxt,
                    ereport(ERROR)
                        .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                        .errmsg(format!(
                            "argument \"{}\" of jsonpath item method .{}() is invalid for type {}",
                            tmp,
                            op_name(jsp.typ)?,
                            "integer"
                        ))
                        .into_error(),
                );
            }
        }
    } else {
        datum = 0;
    }

    if res == jperNotFound {
        return return_error(
            cxt,
            ereport(ERROR)
                .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                .errmsg(format!(
                    "jsonpath item method .{}() can only be applied to a string or numeric value",
                    op_name(jsp.typ)?
                ))
                .into_error(),
        );
    }

    let out = JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(int64_to_numeric_bytes(cxt.mcx, datum as i64)?),
    };

    executeNextItem(cxt, jsp, None, &out, found, true)
}

/// C: the `jpiStringFunc` case (jsonpath_exec.c:1604).
fn execute_string_func(
    cxt: &mut JsonPathExecContext<'_, '_>,
    jsp: &JsonPathItem<'_>,
    jb: &JsonbValue,
    unwrap: bool,
    found: Option<&mut JsonValueList>,
) -> PgResult<JsonPathExecResult> {
    if unwrap && JsonbType(jb)? == jbvType::jbvArray {
        return executeItemUnwrapTargetArray(cxt, Some(jsp), jb, found, false);
    }

    let tmp: Vec<u8> = match JsonbType(jb)? {
        jbvType::jbvString => string_bytes(jb).to_vec(),
        jbvType::jbvNumeric => numeric_out(cxt.mcx, numeric_bytes_of(jb))?.into_bytes(),
        jbvType::jbvBool => {
            if bool_of(jb) {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        jbvType::jbvDatetime => {
            let d = datetime_of(jb);
            // C: `JsonEncodeDateTime(buf, value, typid, &tz)` — `&tz` is a
            // non-NULL pointer to the jbvDatetime's tz field.
            backend_utils_adt_json_seams::json_encode_datetime::call(
                &types_tuple::Datum::from_usize(d.value),
                d.typid,
                Some(d.tz),
            )?
            .into_bytes()
        }
        jbvType::jbvNull | jbvType::jbvArray | jbvType::jbvObject | jbvType::jbvBinary => {
            return return_error(
                cxt,
                ereport(ERROR)
                    .errcode(ERRCODE_NON_NUMERIC_SQL_JSON_ITEM)
                    .errmsg(format!(
                        "jsonpath item method .{}() can only be applied to a boolean, string, numeric, or datetime value",
                        op_name(jsp.typ)?
                    ))
                    .into_error(),
            );
        }
    };

    let out = JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(tmp),
    };

    executeNextItem(cxt, jsp, None, &out, found, true)
}

// ===========================================================================
// Small adapters / numeric & jsonb bridging helpers
// ===========================================================================

/// `jspOperationName` wrapper returning an owned message-safe string.
pub(crate) fn op_name(typ: JsonPathItemType) -> PgResult<&'static str> {
    backend_utils_adt_jsonpath::jspOperationName(typ)
}

/// Build a `PgError` for an internal `elog(ERROR, ...)` (XX000) invariant.
pub(crate) fn elog_error(msg: &str) -> PgError {
    ereport(ERROR).errmsg_internal(msg.to_string()).into_error()
}

/// `memcmp` returning C-style sign (`-1`/`0`/`1`).
fn memcmp(a: &[u8], b: &[u8]) -> i32 {
    use core::cmp::Ordering;
    match a.cmp(b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

/// The on-disk numeric bytes of a `jbvNumeric` value.
fn numeric_bytes_of(v: &JsonbValue) -> &[u8] {
    match &v.val {
        JsonbValueData::Numeric(b) => b,
        _ => unreachable!("numeric_bytes_of on a non-numeric JsonbValue"),
    }
}

/// The string bytes of a `jbvString` value.
fn string_bytes(v: &JsonbValue) -> &[u8] {
    match &v.val {
        JsonbValueData::String(b) => b,
        _ => unreachable!("string_bytes on a non-string JsonbValue"),
    }
}

/// The boolean payload of a `jbvBool` value.
fn bool_of(v: &JsonbValue) -> bool {
    match &v.val {
        JsonbValueData::Bool(b) => *b,
        _ => unreachable!("bool_of on a non-bool JsonbValue"),
    }
}

/// The datetime payload of a `jbvDatetime` value.
fn datetime_of(v: &JsonbValue) -> JsonbDatetime {
    match &v.val {
        JsonbValueData::Datetime(d) => d.clone(),
        _ => unreachable!("datetime_of on a non-datetime JsonbValue"),
    }
}

/// The `jpiLikeRegex` pattern bytes within the node buffer.
fn like_regex_pattern<'a>(v: &JsonPathItem<'a>) -> &'a [u8] {
    let p = v.content.like_regex.pattern_pos as usize;
    let len = v.content.like_regex.patternlen as usize;
    &v.buffer[p..p + len]
}

/// The document-relative byte offset of a `jbvBinary` container, used as the
/// `.keyvalue()` id-offset basis.
fn jbc_identity(v: &JsonbValue) -> i64 {
    match &v.val {
        JsonbValueData::Binary { offset, .. } => *offset as i64,
        _ => 0,
    }
}

/// Read the container header word from container bytes.
fn container_header(jc: &[u8]) -> u32 {
    u32::from_ne_bytes([jc[0], jc[1], jc[2], jc[3]])
}

/// The root `JsonbContainer` bytes of a full on-disk jsonb varlena.
fn jsonb_root(jb: &[u8]) -> &[u8] {
    &jb[VARHDRSZ..]
}

/// The `path->header` version/flags word of a full on-disk `jsonpath` varlena
/// (the 4-byte word just past the varlena length header).
fn jsonpath_header(js: &[u8]) -> u32 {
    let off = JSONPATH_HDRSZ - 4;
    u32::from_ne_bytes([js[off], js[off + 1], js[off + 2], js[off + 3]])
}

/// The container bytes of a `jbvBinary` value, or an `elog(ERROR)` invariant.
fn binary_data<'a>(jb: &'a JsonbValue, msg: &str) -> PgResult<&'a [u8]> {
    match &jb.val {
        JsonbValueData::Binary { data, .. } => Ok(data),
        _ => Err(elog_error(&format!("{}: {}", msg, jb.typ as i32))),
    }
}

/// The document-relative byte offset of a `jbvBinary` value, or 0 for a
/// non-binary value.
fn binary_doc_offset(jb: &JsonbValue) -> i32 {
    match &jb.val {
        JsonbValueData::Binary { offset, .. } => *offset,
        _ => 0,
    }
}

/// Re-base a value returned from `findJsonbValueFromContainer` /
/// `getIthJsonbValueFromContainer` (which treat their `container` argument as a
/// document root) so its document-relative `offset` is measured from the same
/// root as `base`.
fn rebase_binary_offset(v: &mut JsonbValue, base_doc_offset: i32) {
    if let JsonbValueData::Binary { offset, .. } = &mut v.val {
        *offset += base_doc_offset;
    }
}

/// `JsonbValueToJsonb` -> on-disk jsonb varlena bytes.
fn JsonbValueToJsonb(mcx: Mcx<'_>, v: &JsonbValue) -> PgResult<Vec<u8>> {
    Ok(backend_utils_adt_jsonb_util::JsonbValueToJsonb(mcx, v)?.to_vec())
}

/// Build the `JsonPathVars` for the optional `vars` jsonb argument.
fn vars_from_opt_jsonb(vars: Option<&[u8]>) -> JsonPathVars {
    match vars {
        Some(b) => JsonPathVars::Jsonb(b.to_vec()),
        None => JsonPathVars::None,
    }
}

// --- numeric helpers (on-disk bytes) ---------------------------------------

/// C: `int64_to_numeric(val)` -> on-disk varlena bytes.
fn int64_to_numeric_bytes(mcx: Mcx<'_>, val: i64) -> PgResult<Vec<u8>> {
    Ok(backend_utils_adt_numeric::convert::int64_to_numeric(mcx, val)?.to_vec())
}

/// C: `DatumGetNumeric(DirectFunctionCall1(float8_numeric, val))` -> bytes.
fn float8_to_numeric_bytes(mcx: Mcx<'_>, val: f64) -> PgResult<Vec<u8>> {
    Ok(backend_utils_adt_numeric::convert::float8_to_numeric(mcx, val)?.to_vec())
}

/// The binary arithmetic op on on-disk numeric bytes. Returns `Err` on
/// overflow/division-by-zero, matching the C `*error == true` case.
fn numeric_binop(mcx: Mcx<'_>, op: ArithmOp, a: &[u8], b: &[u8]) -> PgResult<Vec<u8>> {
    use backend_utils_adt_numeric::ops_sql::{
        numeric_add, numeric_div, numeric_mod, numeric_mul, numeric_sub,
    };
    let res = match op {
        ArithmOp::Add => numeric_add(mcx, a, b)?,
        ArithmOp::Sub => numeric_sub(mcx, a, b)?,
        ArithmOp::Mul => numeric_mul(mcx, a, b)?,
        ArithmOp::Div => numeric_div(mcx, a, b)?,
        ArithmOp::Mod => numeric_mod(mcx, a, b)?,
    };
    Ok(res.to_vec())
}

/// C: `numeric_uminus(num)` -> bytes.
fn numeric_uminus_bytes(mcx: Mcx<'_>, a: &[u8]) -> PgResult<Vec<u8>> {
    Ok(backend_utils_adt_numeric::ops_sql::numeric_uminus(mcx, a)?.to_vec())
}

/// C: the `.abs()`/`.floor()`/`.ceiling()` numeric functions -> bytes.
fn numeric_unary_method(mcx: Mcx<'_>, func: NumericMethod, a: &[u8]) -> PgResult<Vec<u8>> {
    use backend_utils_adt_numeric::ops_sql::{numeric_abs, numeric_ceil, numeric_floor};
    let res = match func {
        NumericMethod::Abs => numeric_abs(mcx, a)?,
        NumericMethod::Floor => numeric_floor(mcx, a)?,
        NumericMethod::Ceiling => numeric_ceil(mcx, a)?,
    };
    Ok(res.to_vec())
}

/// C: `numeric_trunc(num, scale)` -> bytes.
fn numeric_trunc_bytes(mcx: Mcx<'_>, a: &[u8], scale: i32) -> PgResult<Vec<u8>> {
    Ok(backend_utils_adt_numeric::ops_sql::numeric_trunc(mcx, a, scale)?.to_vec())
}

/// C: `numeric_int4_opt_error(num, &have_error)` — `Err` on the error case.
fn numeric_int4_opt(mcx: Mcx<'_>, num: &[u8]) -> PgResult<i32> {
    use backend_utils_adt_numeric::convert::{numericvar_to_int32, set_var_from_num};
    let var = set_var_from_num(mcx, num)?;
    match numericvar_to_int32(&var)? {
        Some(v) => Ok(v),
        None => Err(int_out_of_range()),
    }
}

/// C: `numeric_int8_opt_error(num, &have_error)` — `Err` on the error case.
fn numeric_int8_opt(mcx: Mcx<'_>, num: &[u8]) -> PgResult<i64> {
    use backend_utils_adt_numeric::convert::set_var_from_num;
    use backend_utils_adt_numeric::kernel_transcendental::numericvar_to_int64;
    let var = set_var_from_num(mcx, num)?;
    match numericvar_to_int64(&var)? {
        Some(v) => Ok(v),
        None => Err(int_out_of_range()),
    }
}

/// The "X out of range" error the numeric-to-int conversions raise.
fn int_out_of_range() -> PgError {
    use types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE;
    PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// C: `numeric_is_nan(num) || numeric_is_inf(num)` on on-disk bytes.
fn numeric_is_nan_or_inf(num: &[u8]) -> bool {
    backend_utils_adt_numeric::on_disk::numeric_is_special(num)
}

/// C: `numeric_out(num)` -> canonical decimal text.
fn numeric_out(mcx: Mcx<'_>, num: &[u8]) -> PgResult<String> {
    backend_utils_adt_numeric::io::numeric_out(mcx, num)
}

// ---------------------------------------------------------------------------
// `DirectInputFunctionCallSafe(...)` soft-parse wrappers (jsonpath_exec.c).
//
// Each is the executor's call into the owning type's input function with an
// `ErrorSaveContext`: a soft (recoverable) parse failure becomes `None` (the C
// `!noerr || escontext.error_occurred` branch), a hard error propagates. The
// owning adt unit's real input function is called directly (a leaf adt dep, no
// seam, mirroring the `numeric_*` helpers above).
// ---------------------------------------------------------------------------

/// The error classes an `ErrorSaveContext` soft-suppresses for a type-input
/// function: an invalid textual representation (`22P02`) and an out-of-range
/// value (`22003`) — exactly the errors the int/float/numeric input functions
/// raise on a bad input string. Any other error is hard and propagates.
fn is_soft_input_error(e: &PgError) -> bool {
    use types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
    let s = e.sqlstate();
    s == ERRCODE_INVALID_TEXT_REPRESENTATION || s == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
}

/// Run a type-input function under `ErrorSaveContext` semantics: a soft
/// (invalid-input / out-of-range) failure becomes `None`; any other error
/// propagates.
fn soft_input<T>(r: PgResult<T>) -> PgResult<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if is_soft_input_error(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

/// C: `DirectInputFunctionCallSafe(int4in, str, ..., &escontext, ...)`.
fn soft_int4in(s: &str) -> PgResult<Option<i32>> {
    soft_input(backend_utils_adt_int::int4in(s, None))
}

/// C: `DirectInputFunctionCallSafe(int8in, str, ..., &escontext, ...)`.
fn soft_int8in(s: &str) -> PgResult<Option<i64>> {
    soft_input(backend_utils_adt_int8::int8in(s, None))
}

/// C: `float8in_internal(str, NULL, "double precision", str, escontext)`.
fn soft_float8in_internal(s: &str) -> PgResult<Option<f64>> {
    soft_input(backend_utils_adt_float::io::float8in_internal(
        s,
        None,
        "double precision",
        s,
        None,
    ))
}

/// C: `parse_bool(str, &bval)` (bool.c).
fn soft_parse_bool(s: &str) -> Option<bool> {
    probe_adt_scalar_bool::parse_bool(s)
}

/// C: `DirectInputFunctionCallSafe(numeric_in, numstr, InvalidOid, dtypmod,
/// &escontext, &datum)` with the typmod built from `(precision, scale)` via
/// `numerictypmodin` (precision `-1` selects the bare `-1` typmod). Returns the
/// on-disk `numeric` varlena bytes, or `None` on a soft error.
fn soft_numeric_in_with_typmod(
    mcx: Mcx<'_>,
    numstr: &str,
    precision: i32,
    scale: i32,
) -> PgResult<Option<Vec<u8>>> {
    let typmod = if precision == -1 {
        -1
    } else {
        backend_utils_adt_numeric::ops_sql::numerictypmodin(&[precision, scale])?
    };
    Ok(soft_input(backend_utils_adt_numeric::io::numeric_in(mcx, numstr, typmod))?.map(|b| b.to_vec()))
}

/// The 13 ISO datetime template strings (jsonpath_exec.c:2408-2423).
const DATETIME_FORMATS: [&str; 13] = [
    "yyyy-mm-dd",                     // date
    "HH24:MI:SS.USTZ",                // timetz
    "HH24:MI:SSTZ",                   //
    "HH24:MI:SS.US",                  // time without tz
    "HH24:MI:SS",                     //
    "yyyy-mm-dd HH24:MI:SS.USTZ",     // timestamptz
    "yyyy-mm-dd HH24:MI:SSTZ",        //
    "yyyy-mm-dd\"T\"HH24:MI:SS.USTZ", //
    "yyyy-mm-dd\"T\"HH24:MI:SSTZ",    //
    "yyyy-mm-dd HH24:MI:SS.US",       // timestamp without tz
    "yyyy-mm-dd HH24:MI:SS",          //
    "yyyy-mm-dd\"T\"HH24:MI:SS.US",   //
    "yyyy-mm-dd\"T\"HH24:MI:SS",      //
];

// ---------------------------------------------------------------------------
// Crate-visible wrappers used by the json_table submodule.
// ---------------------------------------------------------------------------

/// `crate`-visible alias of [`executeJsonPath`] for the JSON_TABLE submodule.
pub(crate) fn executeJsonPathPublic(
    mcx: Mcx<'_>,
    path: &[u8],
    vars: &JsonPathVars,
    getVar: JsonPathGetVarCallback,
    countVars: JsonPathCountVarsCallback,
    json: &[u8],
    throwErrors: bool,
    result: Option<&mut JsonValueList>,
    useTz: bool,
) -> PgResult<JsonPathExecResult> {
    executeJsonPath(mcx, path, vars, getVar, countVars, json, throwErrors, result, useTz)
}

/// `crate`-visible alias of [`jperIsError`].
pub(crate) fn jper_is_error(res: JsonPathExecResult) -> bool {
    jperIsError(res)
}

/// `crate`-visible alias of [`JsonValueListClear`].
pub(crate) fn JsonValueListClearPub(jvl: &mut JsonValueList) {
    JsonValueListClear(jvl)
}

/// `crate`-visible alias of [`JsonValueListInitIterator`].
pub(crate) fn JsonValueListInitIteratorPub(jvl: &JsonValueList) -> JsonValueListIterator {
    JsonValueListInitIterator(jvl)
}

/// `crate`-visible alias of [`JsonValueListNext`].
pub(crate) fn JsonValueListNextPub(it: &mut JsonValueListIterator) -> Option<JsonbValue> {
    JsonValueListNext(it)
}

/// `crate`-visible alias of [`JsonbValueToJsonb`].
pub(crate) fn JsonbValueToJsonbPub(mcx: Mcx<'_>, v: &JsonbValue) -> PgResult<Vec<u8>> {
    JsonbValueToJsonb(mcx, v)
}

/// `crate`-visible alias of [`GetJsonPathVar`] for the JSON_TABLE submodule.
pub(crate) fn GetJsonPathVarPub(
    mcx: Mcx<'_>,
    vars: &JsonPathVars,
    var_name: &[u8],
    base_object: &mut JsonbValue,
    base_object_id: &mut i32,
) -> PgResult<Option<JsonbValue>> {
    GetJsonPathVar(mcx, vars, var_name, base_object, base_object_id)
}

/// `crate`-visible alias of [`CountJsonPathVars`] for the JSON_TABLE submodule.
pub(crate) fn CountJsonPathVarsPub(vars: &JsonPathVars) -> PgResult<i32> {
    CountJsonPathVars(vars)
}
