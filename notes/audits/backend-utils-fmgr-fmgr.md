# Audit: backend-utils-fmgr-fmgr (fmgr.c)

Top-line verdict: **PASS**
Audit date: 2026-06-13
Auditor model: Claude Opus 4.8 (1M) — `claude-opus-4-8[1m]`
Branch: `port/backend-utils-fmgr-fmgr`

Independent function-by-function audit of the `backend-utils-fmgr-fmgr` unit
(C source `src/backend/utils/fmgr/fmgr.c`, CATALOG row `backend-utils-fmgr-core`,
crate `crates/backend-utils-fmgr-core` + vocabulary `crates/types-fmgr`). Every
function was enumerated from the C source and cross-checked against the c2rust
render (`c2rust-runs/backend-utils-fmgr-core/src/fmgr.rs`), then read in all
three forms. Constants were re-checked against headers/catalog `.dat`, not from
memory. The branch carries one commit beyond `port/backend-utils-fmgr-core`
(`fmgr-fmgr: install all fmgr.c-owned seams + register in seams-init`); this
audit re-derived the whole unit from scratch on this branch.

Unit C source: `src/backend/utils/fmgr/fmgr.c` (postgres-18.3), 2201 lines.
Owned seam crate (by C-source coverage): `crates/backend-utils-fmgr-fmgr-seams`
(fmgr.c maps only here; `funcapi.c`/`dfmgr.c` are separate units, so
`backend-utils-fmgr-funcapi-seams`/`-dfmgr-seams` are NOT owned by this unit).

## Function inventory

71 top-level functions are defined in fmgr.c (enumerated by brace-matched
signatures and corroborated against the c2rust render, which expanded header
macros/inlines such as `CStringGetDatum`/`GETSTRUCT`/`list_nth` — these are not
fmgr.c definitions and are excluded). `Int64GetDatum`/`Float8GetDatum` are
`#ifndef USE_FLOAT8_BYVAL` only; the c2rust render did NOT compile them, so under
this build config they are `postgres.h` macros and out-of-unit.

## Constants verified against headers

| Constant | C header value | Port | Verdict |
|---|---|---|---|
| TRACK_FUNC_OFF/PL/ALL | pgstat.h enum order 0/1/2 | 0/1/2 | MATCH |
| ProcedureRelationId | pg_proc.h CATALOG(...,1255,...) | 1255 | MATCH |
| LanguageRelationId | pg_language.h CATALOG(...,2612,...) | 2612 | MATCH |
| BYTEAOID | pg_type bytea = 17 | 17 | MATCH |
| INTERNAL/C/SQL languageId | pg_language.dat 12/13/14 | ProcLanguage 12/13/14 | MATCH |
| SECURITY_LOCAL_USERID_CHANGE | miscadmin.h 0x0001 | types_core | MATCH |
| ACL_EXECUTE / ACL_USAGE | parsenodes.h (1<<7)/(1<<8) | types_acl | MATCH |
| InvalidOidBuiltinMapping | (uint16)-1 | u16::MAX | MATCH (registry miss == NULL) |

## Per-function table

| C function (line) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|
| fmgr_isbuiltin (76) | 118 | MATCH | `id>last`→None; index miss→None (map miss). |
| fmgr_lookupByName (101) | 129 | MATCH | linear strcmp → by_name map; same result. |
| fmgr_info (127) | 551 | MATCH | delegates cxt_security(false); mcx = CurrentMemoryContext. |
| fmgr_info_cxt (137) | 556 | MATCH | delegates cxt_security(false). |
| fmgr_info_cxt_security (147) | 565 | MATCH | reset finfo fields; builtin fast path; secdef routing = `security_definer ∥ fmgr_hook_is_needed` (proconfig-not-null folded into seam-projected `security_definer`); prolang switch with per-arm fn_stats (INTERNAL/builtin→ALL, C→PL, SQL→PL, other→OFF). SQL leg → ERRCODE_FEATURE_NOT_SUPPORTED (fmgr_sql lives in executor/functions.c — unported callee, allowed). |
| fmgr_symbol (281) | 814 | MATCH | secdef branch then prolang switch; (mod,fn) tuple mirrors the 3 NULL/non-NULL cases (core/main-binary/extension). |
| fmgr_info_C_lang (349) | 724 | MATCH | CFuncHash lookup; on miss enforce non-null prosrc/probin, load via dfmgr seam, record_C_func, api_version switch (case 1 / default elog). |
| fmgr_info_other_lang (418) | 786 | MATCH | LANGOID lookup → lanplcallfoid → recurse cxt_security(ignore_security=true) → copy fn_addr. |
| fetch_finfo_record (455) | (dfmgr seam) | SEAMED | psprintf("pg_finfo_%s")/lookup_external_function/info-record validation are dfmgr.c internals; the `load_external_function` seam returns the validated `(user_fn, api_version)`. The fmgr-owned api_version switch stays in fmgr_info_C_lang. |
| lookup_C_func (515) | 685 | MATCH | key=fn_oid; xmin+tid up-to-dateness check; out-of-date→None. |
| record_C_func (539) | 702 | MATCH | insert/update entry; HTAB lazy-create == always-present thread_local map. |
| fmgr_info_copy (580) | 862 | MATCH | clone replaces memcpy; fn_extra reset; fn_mcxt dormant. |
| fmgr_internal_function (595) | 141 | MATCH | lookupByName → foid else InvalidOid. |
| fmgr_security_definer (632) | 1011 + 1032 | MATCH (1 ledgered note) | build cache (cxt_security ignore_security=true, fn_expr copy fmgr.c:658, userid gated on `prosecdef` ALONE fmgr.c:667, TransformGUCArray'd lists); GetUserIdAndSecContext; NewGUCNestLevel if configNames≠NIL; SetUserId if OidIsValid(userid); per-element superuser()→PGC_SUSET/PGC_USERSET + set_config_with_handle(GetUserId() srole); FHET_START; flinfo swap around invoke; catch→FHET_ABORT+rethrow; AtEOXact_GUC; restore userid; FHET_END. pgstat usage = faithful no-op (identical Datum). See note F1. |
| DirectFunctionCall1..9Coll (792-1053) | 359-369 macro | MATCH | InitFunctionCallInfoData(NULL,...); args set non-null; invoke; isnull→elog (`%p`→`<direct>`). |
| CallerFInfoFunctionCall1 (1065) | 374 | MATCH | flinfo threaded; 1 arg; null check. |
| CallerFInfoFunctionCall2 (1085) | 385 | MATCH | flinfo threaded; 2 args. |
| FunctionCall0Coll (1112) | 405 | MATCH | 0 args; invoke; isnull→elog("%u"). |
| FunctionCall1..9Coll (1129-1390) | 438-446 macro | MATCH | flinfo; args; invoke; null check by oid; fn_expr threaded for secdef dispatch (fmgr.c:658). |
| OidFunctionCall0..9Coll (1401-1514) | 490-531 | MATCH | fmgr_info then FunctionCallNColl. |
| InputFunctionCall (1530) | 1211 | MATCH | strict NULL early-out; 3-arg invoke (coll=InvalidOid); str-NULL/non-NULL symmetric isnull checks. |
| InputFunctionCallSafe (1585) | 1249 | MATCH | strict NULL early-out → true; soft error → false; else same isnull checks. |
| DirectInputFunctionCallSafe (1640) | 1294 | MATCH | assumed strict (str==NULL → null, true); soft error → false; isnull→elog. |
| OutputFunctionCall (1683) | 1340 | MATCH | DatumGetCString(FunctionCall1). |
| ReceiveFunctionCall (1697) | 1352 | MATCH | buf==NULL strict early-out; PointerGetDatum(buf); symmetric isnull checks. |
| SendFunctionCall (1744) | 1391 | MATCH | DatumGetByteaP(FunctionCall1); the DatumGetByteaP detoast is a varlena op applied by the varlena-aware caller. |
| OidInputFunctionCall (1754) | 1401 | MATCH | fmgr_info+InputFunctionCall. |
| OidOutputFunctionCall (1763) | 1413 | MATCH | fmgr_info+OutputFunctionCall. |
| OidReceiveFunctionCall (1772) | 1419 | MATCH | fmgr_info+ReceiveFunctionCall. |
| OidSendFunctionCall (1782) | 1431 | MATCH | fmgr_info+SendFunctionCall. |
| Int64GetDatum (1807) | n/a | N/A | `#ifndef USE_FLOAT8_BYVAL` only; postgres.h macro in build config (c2rust did not compile it). Ledgered in lib.rs header. |
| Float8GetDatum (1816) | n/a | N/A | same. |
| pg_detoast_datum (1832) | n/a | N/A | TOAST one-liner (detoast_attr); varlena/detoast subsystem; ledgered exclusion. |
| pg_detoast_datum_copy (1841) | n/a | N/A | same (palloc+memcpy fast-path is a varlena copy primitive). |
| pg_detoast_datum_slice (1857) | n/a | N/A | same. |
| pg_detoast_datum_packed (1864) | n/a | N/A | same. |
| get_fn_expr_rettype (1888) | 1772 | MATCH | !flinfo∥!fn_expr→InvalidOid; exprType via nodeFuncs seam; ByteaConst→BYTEAOID. |
| get_fn_expr_argtype (1910) | 1789 | MATCH | delegates get_call_expr_argtype. |
| get_call_expr_argtype (1929) | 1802 | MATCH | NULL→InvalidOid; ByteaConst (not a call expr)→InvalidOid; the IsA-dispatch over FuncExpr/OpExpr/DistinctExpr/ScalarArrayOpExpr/NullIfExpr/WindowFunc + argnum range check + exprType(list_nth) + the ScalarArrayOpExpr arg==1 element-type hack read the unported planner expression tree (`args` list) and call exprType/get_base_element_type — all owned by nodeFuncs. The seam (`call_expr_argtype`) is the correct owner; the fmgr-owned NULL/ByteaConst/flinfo guards stay here. See note F2. |
| get_fn_expr_arg_stable (1975) | 1814 | MATCH | delegates get_call_expr_arg_stable. |
| get_call_expr_arg_stable (1994) | 1823 | MATCH | NULL→false; ByteaConst→false; Const/Param(PARAM_EXTERN) logic over the unported expr tree owned by call_expr_arg_stable seam. |
| get_fn_expr_variadic (2044) | 1835 | MATCH | !flinfo∥!fn_expr→false; FuncExpr→funcvariadic via expr_variadic seam; Const→false. |
| set_fn_opclass_options (2070) | 1859 | MATCH | makeConst(BYTEAOID,...) → FnExpr::ByteaConst(options); None == options==NULL → constisnull. |
| has_fn_opclass_options (2081) | 1865 | MATCH | Const && consttype==BYTEAOID → !constisnull == options.is_some(). |
| get_fn_opclass_options (2097) | 1881 | MATCH | Const BYTEAOID → constisnull?NULL:bytes; else ERRCODE_INVALID_PARAMETER_VALUE "operator class options info is absent...". |
| CheckFunctionValidatorAccess (2145) | 1905 | MATCH | PROCOID lookup (ERRCODE_UNDEFINED_FUNCTION "function with OID %u does not exist"); LANGOID lookup (elog cache-lookup-failed); lanvalidator≠validatorOid→ERRCODE_INSUFFICIENT_PRIVILEGE; object_aclcheck ACL_USAGE on language + ACL_EXECUTE on function via aclchk seam; OBJECT_LANGUAGE/OBJECT_FUNCTION mapping; return true. |

## Seam audit

Owned seam crate `backend-utils-fmgr-fmgr-seams` declares 13 seams. Eleven are
fmgr.c's own logic and are installed by `init_seams()` (lib.rs:2217-2231),
which contains nothing but `set()` calls; `seams-init::init_all()` calls it
(seams-init/src/lib.rs:68):

`fmgr_info_check`, `oid_function_call_1_deflist`, `oid_send_function_call`,
`oid_output_function_call`, `function_call1_coll`, `function_call2_coll`,
`function_call3`, `output_function_call`, `send_function_call`,
`oid_input_function_call`, `oid_output_function_call_datum`.

Each installer is a thin re-resolve-by-OID + marshal + delegate adapter over the
crate's own FunctionCallNColl / I/O family (the caller's resolved `FmgrInfo`
cannot cross a seam, so the owned model re-resolves by OID at call time, as
elsewhere here). The marshalling present (TupleValue→FmgrArg, varlena-header
strip to wire payload, cstring→bytes into `mcx`) is fmgr's own I/O-call logic in
its OWN installed implementation, not branching on an outward seam path.

Two declarations in this crate are NOT fmgr.c logic and are correctly NOT
installed here (they panic until their real owners install them — the correct
frontier state, per the "Mirror PG and panic" rule):

- `render_slot_columns` — ri_triggers.c violator-column rendering.
- `call_bgworker_entrypoint` — bgworker library/function dispatch (loader).

Outward seams (load_external_function [dfmgr.c]; expr_type/call_expr_argtype/
call_expr_arg_stable/expr_variadic [nodeFuncs.c]; lookup_proc/lookup_language
[syscache.c]; get_user_id*/set_user_id*/superuser [miscinit.c];
new_guc_nest_level/set_config_with_handle/at_eoxact_guc [guc.c]; object_aclcheck/
aclcheck_error [aclchk.c]) are each justified by a real dependency cycle and are
thin marshal+delegate.

## Design conformance (§3b)

- Allocating functions/seams take `Mcx` and return `PgResult`; OOM surfaces as
  `Err` (copy_charged, bytes_into, the GUC-list copies use the fallible mcx API).
- Per-backend state (`REGISTRY`, `C_FUNC_HASH`, `CURRENT_FCINFO`,
  `NEEDS_FMGR_HOOK`/`FMGR_HOOK`, `datum_ref_registry`) is `thread_local`, never a
  shared static — matches the per-backend-global rule.
- `CURRENT_FCINFO` is a pushed owned snapshot (not a `thread_local` raw pointer
  aliasing the exclusive `&mut fcinfo`), popped by an RAII guard that is
  panic-safe — no aliasing, no leaked frame on unwind.
- No locks held across `?`; no ambient-global seams (CurrentMemoryContext arrives
  as the explicit `mcx` param; GetUserId/superuser cross as seams).
- No invented opacity: the `dictData` word (oid_function_call_1_deflist) is a
  genuine heterogeneous C `void *`; the `datum_ref_registry` token represents a
  by-reference `Datum` machine word (a palloc pointer into the backend heap), not
  an invented handle/registry side-table.
- The `ProcInfo` split (`security_definer` = folded routing predicate vs
  `prosecdef` = isolated userid gate) faithfully separates fmgr.c:204-207 (route)
  from fmgr.c:667 (userid switch).

## Ledgered notes (non-blocking)

- **F1 — fmgr_security_definer hook `arg` across calls.** C stashes the cache
  (incl. the `fmgr_hook` passthrough `arg`) in `fcinfo->flinfo->fn_extra`, so it
  persists across multiple FunctionCallInvoke on the same FmgrInfo. The owned
  port rebuilds the cache per dispatch (no `fn_extra` persistence yet), so the
  passthrough `arg` resets each call. Within a single call (START→invoke→
  END/ABORT) the `arg` is threaded consistently, matching C. The only observable
  difference is cross-call `arg` persistence — and `needs_fmgr_hook`/`fmgr_hook`
  are NULL by default (no plugin is ported), so `call_fmgr_hook` is a faithful
  passthrough no-op on every current path; the distinction has no observable
  effect today. Frontier-acceptable; ledgered, not a divergence in any reachable
  behavior.
- **F2 — get_call_expr_argtype/arg_stable dispatch.** The IsA chain reads the
  unported planner expression tree (`FuncExpr.args` etc.) and calls
  exprType/get_base_element_type, all owned by nodeFuncs. `ExternalFnExpr` is a
  tag-only opaque carrier (the expr tree is not ported), so the dispatch
  fundamentally cannot live in fmgr-core; delegating the whole accessor to the
  nodeFuncs seam (with fmgr's NULL/ByteaConst guards retained) is the correct
  ownership decision, not a stubbed body.

## Spot re-derivation (auditor self-check)

- fmgr_security_definer userid gate re-checked: C:667 sets `fcache->userid` only
  under `prosecdef` (not the folded predicate); build_cache gates on
  `proc.prosecdef` — a proconfig-only function routes through the handler but
  does NOT switch userid. Correct.
- TRACK_FUNC per-arm re-checked vs fmgr.c:174/242/247/252/257. Correct.
- fmgr_security_definer body sequencing re-walked line-by-line against
  C:632-777. Matches (incl. catch-arm flinfo restore being a no-op duplicate of
  the post-try restore, and AtEOXact_GUC only when configNames≠NIL).
- Input/Receive str-NULL vs strict symmetry re-checked vs C:1549-1561 /
  1717-1729: `str==NULL` requires isnull, non-NULL requires !isnull, both
  `elog(ERROR)` — matched in both the raw-Datum and typed Option-4 ports.
- c2rust render confirms the defined-function set (header macros expanded inline
  are not fmgr.c; Int64/Float8GetDatum not compiled → out-of-unit).

## Verdict

**PASS.** Every fmgr.c function is `MATCH`, `SEAMED` (fetch_finfo_record's
dfmgr-internal symbol resolution via the load_external_function seam, api-version
switch retained in fmgr_info_C_lang), or a ledgered cross-unit exclusion
(Int64/Float8GetDatum macros; pg_detoast_datum* TOAST one-liners; fmgr_sql
executor leg). Zero seam findings: every fmgr.c-owned seam declaration is
installed by `init_seams()`; the two non-fmgr.c declarations are correctly left
to their owners. Crate builds clean and all 6 unit tests pass
(`cargo test -p backend-utils-fmgr-core`).
