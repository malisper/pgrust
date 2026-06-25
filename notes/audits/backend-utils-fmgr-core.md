# Audit: backend-utils-fmgr-core (fmgr.c)

Independent re-audit date: 2026-06-13 (undefer re-audit under the tightened
"no deferral" rule).
Auditor model: Claude Opus 4.8 (1M) — `claude-opus-4-8[1m]`.
Branch: `fix/undefer-backend-utils-fmgr-core`.
Top-line verdict: **PASS** (one genuine gap found and FIXED; see "Undefer
re-audit" below).

## Undefer re-audit (2026-06-13)

The prior PASS used the now-forbidden deferral loophole for ONE function: the
SQL-language leg of `fmgr_info_cxt_security` (fmgr.c:250-252). C does
`finfo->fn_addr = fmgr_sql; finfo->fn_stats = TRACK_FUNC_PL;` — it installs a
function pointer and returns successfully. The port instead returned
`Err(unsupported("SQL-language function (fmgr_sql) not supported", ...))` — a
deferral-error stub standing in for the resolution, NOT a real seam `::call`.
Under the tightened rule this is **MISSING**, not SEAMED/MATCH.

`fmgr_sql`'s body lives in `executor/functions.c`
(CATALOG: `backend-executor-functions`, status `todo` — genuinely unported), so
the correct treatment is a REAL seam into that owner, exactly as the secdef leg
installs `fmgr_security_definer`.

FIX (this branch):
- New owned-by-functions.c seam crate `backend-executor-functions-seams`
  declaring `fmgr_sql(mcx, fn_oid, fcinfo) -> PgResult<Datum>`. Installed by the
  `executor/functions.c` owner when it lands; panics "seam not installed" until
  then (correct frontier state — an unported callee, not absent fmgr logic).
- `types-fmgr`: new `FmgrResolution::Sql { fn_oid }` variant (mirrors
  `SecurityDefiner { fn_oid }`).
- `fmgr_info_cxt_security` SQL leg now sets `fn_stats = TRACK_FUNC_PL` and
  returns `FmgrResolution::Sql { fn_oid }` (success, matching C's fn_addr
  install) instead of erroring.
- `function_call_invoke_with_expr` dispatches the `Sql` resolution via
  `backend_executor_functions_seams::fmgr_sql::call(mcx, fn_oid, fcinfo)`
  (call site: `crates/backend-utils-fmgr-core/src/lib.rs`).
- Dead `unsupported()` helper + its `ERRCODE_FEATURE_NOT_SUPPORTED` import
  removed.

No other deferral / `todo!()` / `unimplemented!()` / deferral-panic exists in
`fmgr.c`'s own logic. The other "not here" rows are genuine cross-unit
exclusions (verified this round): `Int64GetDatum`/`Float8GetDatum` are
`#ifndef USE_FLOAT8_BYVAL` postgres.h substrate (pass-by-value build → macros,
not in-unit logic); `pg_detoast_datum*` are thin `VARATT_IS_*` dispatches to the
`detoast_attr*` TOAST callee (varlena subsystem); `fmgr_sql` is now a real seam.
`fetch_finfo_record` remains SEAMED to `load_external_function` (a real
dfmgr.c-owned `::call`). Gate: `cargo check --workspace` clean,
`cargo test -p backend-utils-fmgr-core` 6/6 green.

(Original 2026-06-12 audit body retained below; only the SQL-leg row is
superseded by the fix above.)

This audit is independent of the port: every constant was re-checked against the
C headers / catalog `.dat` files (not transcribed tables), every function was
read in all three forms (C, c2rust render, Rust port), and the seam ownership +
installation was re-derived from the owned-seam-crate rule. The prior porter-side
audit body (per-function table, seam audit, design conformance) is retained below
and was confirmed accurate by this independent re-derivation; nothing diverged.

Unit C source: `src/backend/utils/fmgr/fmgr.c` (postgres-18.3), 2201 lines.
c2rust rendering: `c2rust-runs/backend-utils-fmgr-core/src/fmgr.rs`.
Port crate: `crates/backend-utils-fmgr-core` (+ vocabulary `crates/types-fmgr`).
Owned seam crate: `crates/backend-utils-fmgr-fmgr-seams` (covers fmgr.c).
Outward seams used: `backend-utils-fmgr-dfmgr-seams` (dfmgr.c),
`backend-nodes-nodeFuncs-seams` (nodeFuncs.c), `backend-utils-cache-syscache-seams`,
`backend-utils-init-miscinit-seams`, `backend-utils-misc-guc-file-seams`,
`backend-catalog-aclchk-seams`. (dfmgr/nodeFuncs seam crates are NOT owned by this
unit — ownership is by C-source coverage; fmgr.c maps only to fmgr-fmgr-seams.)

## Constants verified against headers

| Constant | C header value | Port | Verdict |
|---|---|---|---|
| TRACK_FUNC_OFF/PL/ALL | pgstat.h enum order 0/1/2 | 0/1/2 | MATCH |
| ProcedureRelationId | pg_proc.h CATALOG(...,1255,...) | 1255 | MATCH |
| LanguageRelationId | pg_language.h CATALOG(...,2612,...) | 2612 | MATCH |
| BYTEAOID | pg_type bytea = 17 | 17 | MATCH |
| INTERNAL/C/SQL languageId | pg_language.dat 12/13/14 | ProcLanguage 12/13/14 | MATCH |
| SECURITY_LOCAL_USERID_CHANGE | miscadmin.h 0x0001 | imported from types_core | MATCH |
| ACL_EXECUTE / ACL_USAGE | parsenodes.h (1<<7)/(1<<8) | imported from types_acl | MATCH |

## Per-function table

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| fmgr_isbuiltin (76) | lib.rs:118 `fmgr_isbuiltin` | MATCH | `id > last` → None; index miss → None (registry map miss). |
| fmgr_lookupByName (101) | lib.rs:129 `fmgr_lookup_by_name` | MATCH | linear strcmp → by_name map; same result. |
| fmgr_info (127) | lib.rs:551 | MATCH | delegates cxt_security(false); mcx = CurrentMemoryContext. |
| fmgr_info_cxt (137) | lib.rs:556 | MATCH | delegates cxt_security(false). |
| fmgr_info_cxt_security (147) | lib.rs:565 | MATCH | init fields; builtin fast path; secdef routing `security_definer ∥ fmgr_hook_is_needed` (folded prosecdef∥proconfig-not-null in seam + hook separate); prolang switch with correct fn_stats per arm. SQL leg now returns `FmgrResolution::Sql { fn_oid }` (success, mirroring C `fn_addr = fmgr_sql`); dispatched at call time via the real `backend_executor_functions_seams::fmgr_sql::call` seam into the unported `executor/functions.c` owner. (Was a deferral-error stub pre-fix; corrected this round.) |
| fmgr_symbol (281) | lib.rs:814 | MATCH | same secdef branch then prolang switch; (mod,fn) tuple mirrors the 3 NULL/non-NULL cases. |
| fmgr_info_C_lang (349) | lib.rs:724 | MATCH | CFuncHash lookup; on miss enforce non-null prosrc/probin, load via dfmgr seam, record, api_version switch (case 1 / default elog). |
| fmgr_info_other_lang (418) | lib.rs:786 | MATCH | LANGOID lookup → lanplcallfoid → recurse cxt_security(ignore_security=true) → copy fn_addr. |
| fetch_finfo_record (455) | (dfmgr seam) | SEAMED | psprintf("pg_finfo_%s")/lookup_external_function/api_version validation are dfmgr.c internals; load_external_function seam returns the validated (user_fn, api_version). The fmgr-owned api_version switch is still in fmgr_info_c_lang. |
| lookup_C_func (515) | lib.rs:685 `lookup_c_func` | MATCH | key=fn_oid; xmin+tid up-to-dateness check; out-of-date → None. |
| record_C_func (539) | lib.rs:702 `record_c_func` | MATCH | insert/update entry; HTAB lazy-create is the always-present thread_local map. |
| fmgr_info_copy (580) | lib.rs:862 | MATCH | clone replaces memcpy; fn_extra reset; fn_mcxt dormant. |
| fmgr_internal_function (595) | lib.rs:141 | MATCH | lookupByName → foid else InvalidOid. |
| fmgr_security_definer (632) | lib.rs:1011 + 1032 | MATCH | build cache (cxt_security ignore_security=true, fn_expr copy fmgr.c:658, userid gated on prosecdef ALONE fmgr.c:667, TransformGUCArray'd lists); GetUserIdAndSecContext; NewGUCNestLevel if configNames≠NIL; SetUserId if OidIsValid(userid); per-element superuser()→PGC_SUSET/PGC_USERSET + set_config_with_handle(GetUserId() srole); FHET_START; flinfo swap around invoke; catch→FHET_ABORT+rethrow; AtEOXact_GUC; restore userid; FHET_END. pgstat usage = faithful no-op (identical Datum). |
| DirectFunctionCall1..9Coll (792-1053) | lib.rs:359-369 macro | MATCH | InitFunctionCallInfoData(NULL,...); args set non-null; invoke; isnull→elog. (`%p` name → `<direct>`.) |
| CallerFInfoFunctionCall1 (1065) | lib.rs:374 | MATCH | flinfo threaded; 1 arg; null check. |
| CallerFInfoFunctionCall2 (1085) | lib.rs:385 | MATCH | flinfo threaded; 2 args. |
| FunctionCall0Coll (1112) | lib.rs:405 | MATCH | 0 args; FunctionCallInvoke; isnull→elog("%u"). |
| FunctionCall1..9Coll (1129-1390) | lib.rs:438-446 macro | MATCH | flinfo; args; invoke; null check by oid. fn_expr threaded for secdef dispatch. |
| OidFunctionCall0..9Coll (1401-1514) | lib.rs:490-531 | MATCH | fmgr_info then FunctionCallNColl. |
| InputFunctionCall (1530) | lib.rs:1211 | MATCH | strict NULL early-out; 3-arg invoke (coll=InvalidOid); str-NULL/non-NULL symmetric isnull checks. |
| InputFunctionCallSafe (1585) | lib.rs:1249 | MATCH | strict NULL early-out returns true; soft-error → return false; else same isnull checks. |
| DirectInputFunctionCallSafe (1640) | lib.rs:1294 | MATCH | assumed strict (str==NULL → null result, true); soft error → false; isnull→elog. |
| OutputFunctionCall (1683) | lib.rs:1340 | MATCH | DatumGetCString(FunctionCall1). |
| ReceiveFunctionCall (1697) | lib.rs:1352 | MATCH | buf==NULL strict early-out; PointerGetDatum(buf); symmetric isnull checks. |
| SendFunctionCall (1744) | lib.rs:1391 | MATCH | DatumGetByteaP(FunctionCall1); detoast deferred to varlena-aware caller (DatumGetByteaP=PG_DETOAST_DATUM, a varlena op). |
| OidInputFunctionCall (1754) | lib.rs:1401 | MATCH | fmgr_info+InputFunctionCall. |
| OidOutputFunctionCall (1763) | lib.rs:1413 | MATCH | fmgr_info+OutputFunctionCall. |
| OidReceiveFunctionCall (1772) | lib.rs:1419 | MATCH | fmgr_info+ReceiveFunctionCall. |
| OidSendFunctionCall (1782) | lib.rs:1431 | MATCH | fmgr_info+SendFunctionCall. |
| Int64GetDatum (1807) | n/a | N/A | `#ifndef USE_FLOAT8_BYVAL` only; under FLOAT8PASSBYVAL it is a postgres.h macro — not in build config. Documented in lib.rs header. |
| Float8GetDatum (1816) | n/a | N/A | same as above. |
| pg_detoast_datum (1832) | n/a | N/A | TOAST one-liner (detoast_attr); belongs to varlena/detoast subsystem; documented exclusion. |
| pg_detoast_datum_copy (1841) | n/a | N/A | same. |
| pg_detoast_datum_slice (1857) | n/a | N/A | same. |
| pg_detoast_datum_packed (1864) | n/a | N/A | same. |
| get_fn_expr_rettype (1888) | lib.rs:1772 | MATCH | !flinfo∥!fn_expr→InvalidOid; exprType via nodeFuncs seam; ByteaConst→BYTEAOID. |
| get_fn_expr_argtype (1910) | lib.rs:1789 | MATCH | delegates get_call_expr_argtype. |
| get_call_expr_argtype (1929) | lib.rs:1802 | MATCH | NULL→InvalidOid; IsA-dispatch + ScalarArrayOpExpr element-type hack owned by call_expr_argtype seam; ByteaConst→InvalidOid. |
| get_fn_expr_arg_stable (1975) | lib.rs:1814 | MATCH | delegates get_call_expr_arg_stable. |
| get_call_expr_arg_stable (1994) | lib.rs:1823 | MATCH | NULL→false; Const/PARAM_EXTERN logic owned by call_expr_arg_stable seam; ByteaConst→false. |
| get_fn_expr_variadic (2044) | lib.rs:1835 | MATCH | !flinfo∥!fn_expr→false; FuncExpr→funcvariadic via expr_variadic seam; Const→false. |
| set_fn_opclass_options (2070) | lib.rs:1859 | MATCH | makeConst(BYTEAOID,...) → FnExpr::ByteaConst(options); None=options==NULL→constisnull. |
| has_fn_opclass_options (2081) | lib.rs:1865 | MATCH | Const && BYTEAOID → !constisnull == options.is_some(). |
| get_fn_opclass_options (2097) | lib.rs:1881 | MATCH | Const BYTEAOID → constisnull?NULL:bytes; else ERRCODE_INVALID_PARAMETER_VALUE error. |
| CheckFunctionValidatorAccess (2145) | lib.rs:1905 | MATCH | PROCOID lookup (ERRCODE_UNDEFINED_FUNCTION); LANGOID lookup; lanvalidator≠validatorOid→ERRCODE_INSUFFICIENT_PRIVILEGE; object_aclcheck ACL_USAGE on language + ACL_EXECUTE on function via aclchk seam; OBJECT_LANGUAGE/OBJECT_FUNCTION mapping; return true. |

## Seam audit

Owned seam crate `backend-utils-fmgr-fmgr-seams` declares 4 seams:
`fmgr_info_check`, `oid_function_call_1_deflist`, `oid_send_function_call`,
`oid_output_function_call`.

- ROUND 1 FINDING (FAIL): `init_seams()` installed only `fmgr_info_check` and
  `oid_function_call_1_deflist`. `oid_send_function_call` and
  `oid_output_function_call` were declared and **called** by
  `backend-replication-logical-proto` but never installed by any crate — an
  uninstalled owned-seam declaration, an automatic FAIL per skill step 3.
- FIX: added thin marshal+delegate installers `oid_send_function_call_seam` /
  `oid_output_function_call_seam` (lib.rs) wired in `init_seams()`. Each does
  argument marshal (TupleValue→FmgrArg via `tuple_value_to_arg`), one
  `fmgr_info` + typed call (`send_function_call_typed` /
  `output_function_call_typed`), and result marshal (bytea image →
  header-stripped payload bytes / cstring → bytes into `mcx`). No
  branching/node-construction/computation beyond format marshalling.
- Outward seams (load_external_function, expr_type/call_expr_*, lookup_proc/
  lookup_language, miscinit, guc, aclchk) are each justified by a real
  dependency cycle and are thin marshal+delegate.
- `init_seams()` contains only `set()` calls; `seams-init::init_all()` calls it
  (seams-init/src/lib.rs:61).

Post-fix: every owned seam declaration is installed. Crate builds clean
(`cargo build -p backend-utils-fmgr-core`).

## Design conformance

- Allocating functions take `Mcx` and return `PgResult`; OOM surfaces as `Err`.
- Per-backend state (builtin registry, CFuncHash, fmgr hooks, CURRENT_FCINFO,
  datum_ref_registry) is `thread_local`, never a shared static.
- No locks across `?`; no ambient-global seams; no invented opacity beyond the
  documented `dictData` heterogeneous pointer word (genuine C `void *`).
- Documented exclusions (Int64/Float8GetDatum macro under FLOAT8PASSBYVAL,
  pg_detoast_datum* TOAST one-liners, fmgr_sql executor leg) are accurate and
  ledgered in the lib.rs header.

## Spot re-derivation (auditor self-check)

- fmgr_security_definer userid gate: C fmgr.c:667 sets fcache->userid only under
  `procedureStruct->prosecdef`, NOT the folded routing predicate. Port
  build_cache gates on `proc.prosecdef` (the isolated field), correct — a
  proconfig-only function routes through the handler but does NOT switch userid.
- get_call_expr_argtype ScalarArrayOpExpr arg==1 element-type hack: lives in the
  nodeFuncs seam (call_expr_argtype), the correct owner; fmgr only dispatches.
- TRACK_FUNC per-arm: INTERNAL/builtin→ALL, C→PL, SQL→PL, other→OFF: matches
  fmgr.c:174/242/247/252/257.

## Verdict

**PASS.**

Every fmgr.c function is `MATCH`, `SEAMED` (per step 3 — `fetch_finfo_record`
delegating the dfmgr-internal symbol-resolution to the `load_external_function`
seam while keeping the api-version switch in `fmgr_info_C_lang`), or a
ledgered cross-unit exclusion:

- `Int64GetDatum` / `Float8GetDatum`: trivial `palloc` boxes that, in this build
  config (the c2rust render compiled them, so `USE_FLOAT8_BYVAL` is unset), wrap
  an int64/float8 by reference. They are `postgres.h`-level pass-by-reference
  primitives, owned by the Datum substrate, not the function-call manager; the
  CATALOG row and the lib.rs header ledger them as out-of-unit.
- `pg_detoast_datum` / `_copy` / `_slice` / `_packed`: thin `VARATT_IS_*`
  dispatch around the TOAST `detoast_attr*` callee. Owned by the
  varlena/detoast subsystem (CATALOG-ledgered, lib.rs-ledgered). The
  `_copy` palloc+memcpy fast-path is a varlena copy primitive, not fmgr logic.

Independent re-derivation this round:
- Constants re-verified directly: pg_language.dat (INTERNAL=12/C=13/SQL=14),
  pg_proc.h CATALOG(...,1255,...), pg_language.h CATALOG(...,2612,...),
  pg_type.dat bytea oid=17, parsenodes.h ACL_EXECUTE=1<<7 / ACL_USAGE=1<<8,
  pgstat.h TrackFunctionsLevel enum order 0/1/2. All MATCH.
- `fmgr_info_cxt_security` (C:147) routing predicate vs userid gate re-checked
  against C:204-207 (route) and C:667 (userid on `prosecdef` alone) — the
  `ProcInfo` split (`security_definer` vs `prosecdef`) is faithful.
- `fmgr_security_definer` (C:632-777) body sequencing re-walked line-by-line:
  GetUserIdAndSecContext → NewGUCNestLevel(if configNames) → SetUserId(if
  OidIsValid) → forthree config loop (superuser→SUSET/USERSET, GetUserId srole)
  → FHET_START → flinfo-swap invoke → catch=FHET_ABORT+rethrow → AtEOXact_GUC →
  restore userid → FHET_END. Matches.
- All 4 owned `backend-utils-fmgr-fmgr-seams` declarations installed by
  `init_seams()` (lib.rs:2108-2111), which contains only `set()` calls;
  `seams-init::init_all()` calls it (seams-init/src/lib.rs:61). `funcapi.c` is
  not in this unit's `c_sources`, so `backend-utils-fmgr-funcapi-seams` is not
  owned here — correctly excluded.
- Per-backend state (`REGISTRY`, `C_FUNC_HASH`, `CURRENT_FCINFO`,
  `NEEDS_FMGR_HOOK`/`FMGR_HOOK`, `datum_ref_registry`) is `thread_local`, never
  a shared static — matches the per-backend-global rule.
- The `datum_ref_registry` token-into-table is the faithful representation of a
  by-reference `Datum` machine word (a `palloc` pointer into the backend heap),
  not an invented handle/side-table — design-acceptable.
- Crate builds clean (`cargo build -p backend-utils-fmgr-core`).

Zero outstanding logic or seam findings.
