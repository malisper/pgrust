# Audit: backend-utils-adt-jsonpath-exec (jsonpath_exec.c)

Self-audit of the port against `postgres-18.3/src/backend/utils/adt/jsonpath_exec.c`.
Each C function is listed with its Rust counterpart and whether it is **faithful**
(full logic in-crate), **owner-routed** (delegates a genuine cross-subsystem op
through that owner's own seams crate / a direct dep), or **seamed** (delegates a
jsonpath_exec-specific external — one carrying this unit's local node/value types
or a jsonpath_exec-private soft-parse wrapper — through
`backend-utils-adt-jsonpath-exec-seams`).

Owner-routed externals (NOT in this unit's own seams crate):
`RE_compile_and_execute` → `backend-utils-adt-regexp-seams`; `format_type_be` →
`backend-utils-adt-format-type-seams`; `pg_server_to_any` / `GetDatabaseEncoding`
→ `backend-utils-mb-mbutils-seams`; `JsonEncodeDateTime` →
`backend-utils-adt-json-seams`; `jspConvertRegexFlags` → the `backend-utils-adt-jsonpath`
type crate (direct dep). This unit's own seams crate keeps only
`parse_datetime`/`datetime_method_cast`/`compare_datetime` (carry the local
`DateTimeValue`), the fmgr soft-parse wrappers
(`int4in`/`int8in`/`float8in_internal`/`parse_bool`/`numeric_in_with_typmod`),
`json_item_from_datum`, `check_stack_depth`/`check_for_interrupts`, and the
JSON_TABLE boundary (`init_table_func`/`eval_column`).

Gate: `cargo check -p backend-utils-adt-jsonpath-exec`,
`-p backend-utils-adt-jsonpath-exec-seams`, `-p seams-init` all green;
`cargo test -p backend-utils-adt-jsonpath-exec` → 15 passed (incl. the golden
`.keyvalue()` id tests, ids 12/72 from `jsonb_jsonpath.out`).

## SQL-callable entrypoints (jsonpath_exec.c:382-650)

| C | Rust | Status |
|---|---|---|
| jsonb_path_exists_internal / jsonb_path_exists / _tz / _opr | jsonb_path_exists_internal / jsonb_path_exists / _tz / _opr | faithful (fmgr varlena unwrap done by caller; takes detoasted bytes + mcx) |
| jsonb_path_match_internal / jsonb_path_match / _tz / _opr | same names | faithful |
| jsonb_path_query_internal / jsonb_path_query / _tz | same names | faithful (SRF list materialized to Vec<Vec<u8>>) |
| jsonb_path_query_array_internal / _array / _array_tz | same names | faithful |
| jsonb_path_query_first_internal / _first / _first_tz | same names | faithful |

## Core evaluator (jsonpath_exec.c:676-2011)

| C | Rust | Status |
|---|---|---|
| executeJsonPath | executeJsonPath | faithful (strict-no-result complete-list branch preserved) |
| executeItem | executeItem | faithful |
| executeItemOptUnwrapTarget | executeItemOptUnwrapTarget | faithful (full per-item switch, branch order 1:1) |
| executeItemUnwrapTargetArray | executeItemUnwrapTargetArray | faithful |
| executeNextItem | executeNextItem | faithful (cur always non-NULL here; the C `!cur` path is the call from executeItemOptUnwrapResult which passes next=None, modeled by the `else` arm) |
| executeItemOptUnwrapResult | executeItemOptUnwrapResult | faithful |
| executeItemOptUnwrapResultNoThrow | executeItemOptUnwrapResultNoThrow | faithful |
| (non-strict EXISTS found==NULL path) | executeItemOptUnwrapResultNoThrowExists | faithful (C inlines `found==NULL`; split out, unwrap is always false here) |
| executeBoolItem | executeBoolItem | faithful (And/Or/Not/IsUnknown/comparison/StartsWith/LikeRegex/Exists) |
| executeNestedBoolItem | executeNestedBoolItem | faithful |
| executeAnyItem | executeAnyItem | faithful (recursive level walk; doc-offset bookkeeping for .keyvalue id) |
| executePredicate | executePredicate | faithful (strict found/error accumulation) |
| executeComparison | executeComparison | faithful |
| executeStartsWith | executeStartsWith | faithful |
| executeLikeRegex | executeLikeRegex | **owner-routed** RE_compile_and_execute (regexp-seams, nmatch=0) + jspConvertRegexFlags (jsonpath crate, direct dep); cache logic in-crate |
| executeBinaryArithmExpr | executeBinaryArithmExpr | faithful; numeric op via in-repo numeric crate (Err→jperError when !throwErrors) |
| executeUnaryArithmExpr | executeUnaryArithmExpr | faithful; numeric_uminus via numeric crate |
| executeNumericItemMethod (.abs/.floor/.ceiling) | executeNumericItemMethod | faithful; numeric_abs/floor/ceil via numeric crate |
| executeDateTimeMethod | executeDateTimeMethod | **seamed** parse_datetime (json.c) + datetime_method_cast (date/time fmgr); ISO format loop + precision/error logic in-crate |
| executeKeyValueMethod | executeKeyValueMethod | faithful (.keyvalue id = doc-offset − baseObject offset + base id*1e10; object re-serialized via pushJsonbValue/JsonbValueToJsonb) |
| appendBoolResult | appendBoolResult | faithful |
| the jpiDouble case | execute_double | **seamed** float8in_internal (NaN/Inf check + numeric/string arms in-crate) |
| the jpiBigint case | execute_bigint | **seamed** int8in for string; numeric_int8 via numeric crate |
| the jpiBoolean case | execute_boolean | **seamed** int4in (numeric) + parse_bool (string); bool arm in-crate |
| the jpiDecimal/jpiNumber case | execute_decimal_number | **seamed** numeric_in_with_typmod; NaN/Inf + precision/scale typmod logic in-crate |
| the jpiInteger case | execute_integer | **seamed** int4in for string; numeric_int4 via numeric crate |
| the jpiStringFunc case | execute_string_func | **owner-routed** JsonEncodeDateTime (json-seams, datetime arm); string/numeric/bool arms in-crate |

## Item / variable access (jsonpath_exec.c:2956-3216)

| C | Rust | Status |
|---|---|---|
| getJsonPathItem | getJsonPathItem | faithful |
| GetJsonPathVar | GetJsonPathVar | faithful (List<JsonPathVariable>) |
| CountJsonPathVars | CountJsonPathVars | faithful |
| JsonItemFromDatum | JsonItemFromDatum | BOOLOID + DATE/TIME*/TIMESTAMP* + default-error arm **faithful in-crate** (format_type_be **owner-routed** to format-type-seams); numeric/int/float/text/varchar/jsonb/json arms **seamed** json_item_from_datum |
| getJsonPathVariable | getJsonPathVariable | faithful |
| getJsonPathVariableFromJsonb | getJsonPathVariableFromJsonb | faithful (findJsonbValueFromContainer) |
| countVariablesFromJsonb | countVariablesFromJsonb | faithful (validates vars is an object, errcode 22023) |
| JsonbArraySize | JsonbArraySize | faithful |

## Comparisons (jsonpath_exec.c:3239-3488)

| C | Rust | Status |
|---|---|---|
| binaryCompareStrings | binaryCompareStrings | faithful |
| compareStrings | compareStrings | ASCII/UTF-8 fast path **faithful in-crate**; other-encoding path **owner-routed** GetDatabaseEncoding + pg_server_to_any (mbutils-seams) |
| compareItems | compareItems | faithful; datetime arm **seamed** compare_datetime (session_timezone) |
| compareNumeric | compareNumeric | faithful (numeric_cmp via numeric crate) |
| copyJsonbValue | copyJsonbValue | faithful (clone) |
| getArrayIndex | getArrayIndex | faithful (numeric_trunc + numeric_int4 via numeric crate) |

## Base object + JsonValueList (jsonpath_exec.c:3491-3593)

| C | Rust | Status |
|---|---|---|
| setBaseObject | setBaseObject | faithful (jbc=Some(bytes)/None + id_addr offset) |
| JsonValueListClear/Append/Length/IsEmpty/Head/GetList/InitIterator/Next | same names | faithful |
| JsonbInitBinary | JsonbInitBinary | faithful (JsonbToJsonbValue) |
| JsonbType | JsonbType | faithful |
| getScalar | getScalar | faithful |
| wrapItemsInArray | wrapItemsInArray | faithful (pushJsonbValue) |

## Public path entrypoints (jsonpath_exec.c:3886-4080)

| C | Rust | Status |
|---|---|---|
| JsonPathExists | JsonPathExists | faithful (returns matched + suppressed-error flag) |
| JsonPathQuery | JsonPathQuery | faithful (wrapper-mode branch order 1:1; column-name message variants) |
| JsonPathValue | JsonPathValue | faithful (singleton/scalar/null extraction) |

## JSON_TABLE plan machinery (jsonpath_exec.c:4082-4493, src/json_table.rs)

| C | Rust | Status |
|---|---|---|
| GetJsonTableExecContext (magic check) | check_magic | faithful |
| JsonTableInitOpaque | JsonTableInitOpaque | faithful; **seamed** init_table_func (plan tree + PASSING vars + ncols from executor) |
| JsonTableInitPlan | JsonTableInitPlan | faithful (colplanstates recorded as ChildStep path) |
| JsonTableSetDocument | JsonTableSetDocument | faithful |
| JsonTableResetRowPattern | JsonTableResetRowPattern | faithful (executeJsonPath, error→clear) |
| JsonTablePlanNextRow/ScanNextRow/JoinNextRow | same names | faithful |
| JsonTableResetNestedPlan | JsonTableResetNestedPlan | faithful |
| JsonTableFetchRow | JsonTableFetchRow | faithful |
| JsonTableGetValue | JsonTableGetValue | faithful; ORDINAL arm in-crate, expression arm **seamed** eval_column (ExecEvalExpr/JsonExpr) |
| JsonTableDestroyOpaque | JsonTableDestroyOpaque | faithful |

## Reconciliation notes (vs src-idiomatic)

- This repo's `JsonbValueData::Binary { len, data, offset }` carries the
  document-relative offset *inside* the variant (src-idiomatic kept it as a
  separate `addr` field). `jbc_identity`/`binary_doc_offset`/`rebase_binary_offset`
  read/adjust that field; the `.keyvalue()` id arithmetic is unchanged.
- Numeric ops and `JsonbValueToJsonb` are charged against an explicit `Mcx`
  here, so the executor carries `mcx` through `JsonPathExecContext` and the
  public entrypoints take it as the first argument (C uses the ambient
  `CurrentMemoryContext`).
- `numeric_int4`/`numeric_int8` map onto `set_var_from_num` +
  `numericvar_to_int32`/`numericvar_to_int64` (the in-repo numeric crate's
  byte→NumericVar→int path), with the `None` (out-of-range) result raised as the
  22003 error C's `*_opt_error` reports.

## Conformance

- No `extern "C"`, no `c_void`, no raw pointers.
- No `todo!()` / `unimplemented!()`.
- `.unwrap()` sites are guarded invariants mirroring the C's non-null guarantees
  (the `next` ref already proven `Some`; predicate rval present for
  Comparison/StartsWith; regex just cached). Error paths return `Err(PgError)` /
  `Ok(jperError)` per `RETURN_ERROR` / `throwErrors`.
- fallible mcx allocation: numeric/jsonb owner crates own the palloc paths and
  surface OOM as recoverable `PgError`; this crate only forwards.
