# Audit: backend-commands-functioncmds

**Re-audit 2026-06-13 (tightened "no-deferral" rule) — VERDICT: PASS (after fix).**
Auditor model: Claude Fable 5 (`claude-fable-5`).

This re-audit was triggered because the prior PASS leaned on the now-FORBIDDEN
"deferred / SEAMED-equivalent" rationalization for two functions whose bodies
contain genuine functioncmds-owned logic. Under the tightened rule the ONLY
acceptable "not implemented here" is a REAL seam `::call` into a genuinely
unported owner; a whole-body delegate that hides own control flow is MISSING,
not SEAMED.

## Fixes applied this round

Two functions had their *entire body* replaced by a single whole-body seam call
(`update_proconfig_value` and `interpret_func_support`). Each of those bodies
contains real own-logic (a loop, a kind dispatch, an add/delete decision; three
error-check predicates) that is NOT external. Both were re-implemented in-crate,
seaming only the genuinely-external callees at granular boundaries.

* **`update_proconfig_value` (functioncmds.c:659)** — was
  `seam::update_proconfig_value::call(a, set_items)` (whole body MISSING).
  Now implements the C body in `ddl_core.rs`: `foreach` over `set_items`,
  `lfirst_node(VariableSetStmt, ...)`, the `kind == VAR_RESET_ALL → a = NULL`
  branch, and the `valuestr ? GUCArrayAdd : GUCArrayDelete` decision — all
  in-crate. Only the three genuinely-external GUC/array helpers cross granular
  seams: `extract_set_variable_args` (`ExtractSetVariableArgs`), `guc_array_add`
  (`GUCArrayAdd`), `guc_array_delete` (`GUCArrayDelete`) — all owned by the
  unported `utils/misc/guc.c`. Required a real `VariableSetStmt`/
  `VariableSetKind` vocabulary (added to `types-parsenodes`, values verified
  against `parsenodes.h`) so the loop can match on the node kind. **MATCH.**
* **`interpret_func_support` (functioncmds.c:685)** — was
  `seam::interpret_func_support::call(defel)` (whole body MISSING). Now
  implements the C body in `ddl_core.rs`: `argList[0] = INTERNALOID`; the
  undefined-function check (42883, message via `func_signature_string`); the
  must-return-INTERNAL check (42P17, message via `NameListToString` + literal
  `"internal"`); the must-be-superuser check (42501) — all in-crate. Only the
  external lookups cross granular seams: `def_get_qualified_name`
  (`defGetQualifiedName`, commands/define.c), `lookup_func_name`
  (`LookupFuncName`, parser/parse_func.c), `get_func_rettype` (lsyscache.c),
  the pre-existing `superuser`, plus `func_signature_string` /
  `name_list_to_string` for the error renderers. **MATCH.**

The whole-body `update_proconfig_value` and `interpret_func_support` seam decls
were removed from `backend-commands-functioncmds-seams`; the six granular seams
above replace them.

## Function inventory (every definition in functioncmds.c)

| # | C function (functioncmds.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `compute_return_type` (:88) | `ddl_core.rs::compute_return_type` | MATCH | Branch order exact (LookupTypeName/typisdefined/existing-OID; not-found: language gate → undefined-object, typmod → syntax, else NOTICE + QualifiedNameGetCreationNamespace + ACL_CREATE + TypeShellMake). 42P13/42809/42704/42601 verified. |
| 2 | `interpret_function_parameter_list` (:183) | `ddl_core.rs::interpret_function_parameter_list` | MATCH | Full per-param mode normalization, type lookup, ACL_USAGE, setof rejection, VARIADIC validation, duplicate-name detection, default-expr transform + table-ref rejection, output construction. |
| 3 | `compute_common_attribute` (:515) | `ddl_core.rs::compute_common_attribute` | MATCH | Nine option names in C order, each with the procedure-error gate; duplicate → errorConflictingDefElem; `set` appends; unknown → Ok(false). |
| 4 | `interpret_func_volatility` (:617) | `ddl_core.rs::interpret_func_volatility` | MATCH | immutable/stable/volatile → `i`/`s`/`v`; else internal-error. |
| 5 | `interpret_func_parallel` (:635) | `ddl_core.rs::interpret_func_parallel` | MATCH | safe/unsafe/restricted → `s`/`u`/`r`; else 42601. |
| 6 | `update_proconfig_value` (:659) | `ddl_core.rs::update_proconfig_value` | **MATCH (fixed)** | Loop + VAR_RESET_ALL branch + add/delete decision now in-crate; only `ExtractSetVariableArgs`/`GUCArrayAdd`/`GUCArrayDelete` cross granular seams to unported guc.c. Previously whole-body-MISSING. |
| 7 | `interpret_func_support` (:685) | `ddl_core.rs::interpret_func_support` | **MATCH (fixed)** | argList + three error checks (42883/42P17/42501) now in-crate; only `defGetQualifiedName`/`LookupFuncName`/`get_func_rettype`/`superuser`/`func_signature_string`/`NameListToString` cross granular seams. Previously whole-body-MISSING. |
| 8 | `compute_function_attributes` (:729) | `ddl_core.rs::compute_function_attributes` | MATCH | Option loop + post-loop application order exact; only-overwrite-if-seen preserved. Calls the in-crate `update_proconfig_value` and `interpret_func_support`. |
| 9 | `interpret_AS_clause` (:866) | `ddl_core.rs::interpret_AS_clause` | MATCH | no-body/duplicate-body/inline-only guards; C-language probin+prosrc; inline SQL body (SEAMED `interpret_sql_body`); prosrc path. |
| 10 | `CreateFunction` (:1026) | `ddl_core.rs::CreateFunction` | MATCH | Namespace resolve + ACL_CREATE; default attrs; compute_function_attributes; language default/lookup; permissions; transform loop; param list; prorettype/returnsSet decision; AS clause; procost/prorows defaults; prokind; ProcedureCreate (SEAMED). |
| 11 | `RemoveFunctionById` (:1311) | `ddl_core.rs::RemoveFunctionById` → seam | SEAMED | Whole body is pg_proc/pg_aggregate catalog tuple deletes (table_open/SearchSysCache/CatalogTupleDelete/ReleaseSysCache/pgstat_drop_function); the `prokind == AGGREGATE` arm is itself catalog I/O on the same unported catalog owner — no functioncmds-independent decision. Thin delegate to `remove_function_tuple::call` (owner backend-catalog-pg-proc, unported). |
| 12 | `AlterFunction` (:1361) | `ddl_core.rs::AlterFunction` | MATCH | Lookup/owner/aggregate-reject preamble → `alter_function_begin` (SEAMED); in-crate action loop (compute_common_attribute + volatility/strict/security/leakproof/cost/rows/support/parallel/set) into AlterFunctionChanges; apply (SEAMED). Calls in-crate `interpret_func_support`. |
| 13 | `CreateCast` (:1539) | `cast_transform_do.rs::CreateCast` | MATCH | typename_type_id + get_typtype; pseudo-type rejects; owner-of-either; two ACL_USAGE; domain WARNINGs; castmethod func/inout/binary branches with all arg checks; CoercionContext → castcontext char; CastCreate (SEAMED). 42809/42P17/42501 verified. |
| 14 | `check_transform_function` (:1802) | `cast_transform_do.rs::check_transform_function` | MATCH | volatile-reject/normal-function/not-set/one-arg/arg0=INTERNALOID, all 42P17. |
| 15 | `CreateTransform` (:1832) | `cast_transform_do.rs::CreateTransform` | MATCH | type lookup + pseudo/domain rejects; ownercheck + ACL_USAGE; get_language_oid + language ACL_USAGE; fromsql/tosql via in-crate `check_transform_func`; catalog insert (SEAMED). |
| 16 | `get_transform_oid` (:2037) | `cast_transform_do.rs::get_transform_oid` | MATCH | GetSysCacheOid2(TRFTYPELANG, ...) direct; missing → 42704. |
| 17 | `IsThereFunctionInNamespace` (:2061) | `cast_transform_do.rs::IsThereFunctionInNamespace` | MATCH | SearchSysCacheExists3 → `function_exists_in_namespace` (SEAMED) → 42723. |
| 18 | `ExecuteDoStmt` (:2084) | `cast_transform_do.rs::ExecuteDoStmt` | MATCH | Build InlineCodeBlock; option loop; require AS; language default/lookup; permissions; laninline valid-or-0A000; OidFunctionCall1 → `execute_inline_handler` (SEAMED). |
| 19 | `ExecuteCallStmt` (:2206) | `call_stmt.rs::ExecuteCallStmt` → seam | SEAMED | Whole body is the executor/fmgr CALL invocation (acl/callcontext/arg eval/FunctionCallInvoke/result) over runtime params+dest. Thin delegate to `backend-executor-execMain-seams::execute_call_stmt::call` (owner unported). |
| 20 | `CallStmtResultDesc` (:2383) | `call_stmt.rs::CallStmtResultDesc` → seam | SEAMED | Whole body is `build_function_result_tupdesc_t` + outargs fixup over TupleDesc/Form_pg_attribute/exprType. Allocating seam takes `Mcx<'mcx>`, returns `PgResult<TupleDesc<'mcx>>`. Thin delegate to `backend-nodes-nodeFuncs-seams::call_stmt_result_desc::call` (owner unported). |

## Seam audit

- After the fix, `backend-commands-functioncmds-seams` declares its outward
  seams (ACL/aclchk, type/func/language lookups, defGet*, the granular GUC
  array ops, parser transform boundary, catalog inserts/deletes, fmgr DO,
  and the six new granular callees). The two whole-body deferral seams
  (`update_proconfig_value`, `interpret_func_support`) were **removed**.
- Every remaining seam call is a thin marshal + delegate to a genuinely
  unported owner; verified call sites exist for `remove_function_tuple`
  (`ddl_core.rs:1159`), `execute_call_stmt` (`call_stmt.rs:51`),
  `call_stmt_result_desc` (`call_stmt.rs:63`), and the six new granular seams
  (inside the repaired `update_proconfig_value` / `interpret_func_support`
  bodies). No owner crate (`backend-catalog-pg-proc`,
  `backend-executor-execMain`, guc.c, parse_func.c) exists, so each panics
  until its owner lands — the only acceptable missing piece.
- `init_seams()` is empty: functioncmds owns no INWARD seam (no other ported
  crate calls back into it across a cycle). Registered in
  `seams-init::init_all()`. Both recurrence_guard tests pass.
- Allocating seam `call_stmt_result_desc` correctly takes `Mcx<'mcx>` + returns
  `PgResult<TupleDesc<'mcx>>`. No `&'static mut`; `get_user_id`/`superuser`
  mirror the C `GetUserId()`/`superuser()` current-backend reads.

## Design conformance

- New `VariableSetStmt`/`VariableSetKind` are real owned types in
  `types-parsenodes` (trimmed to the consumed `kind`/`name`/`args` fields, args
  carried opaquely for the GUC owner), not stand-ins. Values verified against
  `parsenodes.h`.
- Allocating fns thread `Mcx` + return `PgResult`.
- No shared statics / ambient globals / locks-across-`?`.
- FATAL/PANIC → Err: functioncmds has no FATAL/PANIC; all ereport are
  ERROR/NOTICE/WARNING mapped to Err/`.finish()`.
- Zero `todo!()`/`unimplemented!()`/deferral-panic in own logic (grep clean; the
  lone `for now` comment is a verbatim copy of the C source comment at
  functioncmds.c:712, and `"is not yet defined"` is the verbatim NOTICE text).

## Verdict: PASS

After the fix, all 20 C functions are MATCH (14, including the 2 repaired) or
SEAMED-per-tightened-rules (4: `RemoveFunctionById`, `ExecuteCallStmt`,
`CallStmtResultDesc`, plus the genuinely-external sub-call seams inside the MATCH
functions). Every SEAMED body is wholly external catalog/executor/runtime work
owned by an unported neighbor with a verified `::call` site; no own-logic is
absent or approximated. The two previously-deferred functions now carry their
real C control flow in-crate. Gate: `cargo check --workspace` clean;
`cargo test --workspace` had zero logic failures (one unrelated doctest aborted
on a `StorageFull` disk-100%-full environment error, not a test failure);
touched-crate tests + seams-init recurrence_guard green.
