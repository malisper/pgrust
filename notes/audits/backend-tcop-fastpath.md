# Audit: backend-tcop-fastpath

C source: `src/backend/tcop/fastpath.c` (458 lines, PostgreSQL 18.3).
Port: `crates/backend-tcop-fastpath/src/lib.rs`.
Re-derived independently from the C + c2rust rendering
(`../pgrust/c2rust-runs/backend-tcop-fastpath/src/fastpath.rs`).

## Function inventory

| C function (loc) | Port | Verdict | Notes |
|---|---|---|---|
| `struct fp_info` (48) | `FpInfo` + `FpInfo::zeroed` | MATCH | funcid/flinfo/namespace/rettype/argtypes[FUNC_MAX_ARGS]/fname. `fname` is an owned `String` (NUL-trimmed name) instead of `char[NAMEDATALEN]`; the strlcpy truncation is preserved by `strlcpy_name`. `zeroed()` mirrors `MemSet(...,0)` (incl. all-zero FmgrInfo via `FmgrInfo::empty()`). |
| `SendFunctionResult` (66) | `send_function_result` | MATCH | isnull→`pq_sendint32(-1)`; format 0→`getTypeOutputInfo`+`OidOutputFunctionCall`+`pq_sendcountedtext`; format 1→`getTypeBinaryOutputInfo`+`OidSendFunctionCall`+`pq_sendint32(len)`+`pq_sendbytes`; else→`ERRCODE_INVALID_PARAMETER_VALUE` "unsupported format code: %d". The send seam returns exactly `VARSIZE-VARHDRSZ` payload bytes, so `outputbytes.len()` == C's `VARSIZE(outputbytes)-VARHDRSZ`. `pq_beginmessage`/`pq_endmessage` via direct dep. |
| `fetch_fp_info` (118) | `fetch_fp_info` | MATCH (SEAMED lookups) | zero+clear funcid; `SearchSysCache1(PROCOID)` miss→`ERRCODE_UNDEFINED_FUNCTION` "function with OID %u does not exist"; prokind!=PROKIND_FUNCTION(`'f'`)||proretset→`ERRCODE_FEATURE_NOT_SUPPORTED`; pronargs>FUNC_MAX_ARGS→`elog(ERROR)` (errmsg_internal); field copies (namespace/rettype/argtypes memcpy of pronargs/strlcpy fname); `fmgr_info`; funcid set last. |
| `HandleFunctionRequest` (188) | `handle_function_request` | MATCH (SEAMED externals) | IsAbortedTransactionBlockState→`ERRCODE_IN_FAILED_SQL_TRANSACTION`; PushActiveSnapshot(GetTransactionSnapshot()); fid=pq_getmsgint(4); fetch_fp_info; log_statement==LOGSTMT_ALL→LOG; object_aclcheck(Namespace,ACL_USAGE)+aclcheck_error(OBJECT_SCHEMA,get_namespace_name); InvokeNamespaceSearchHook(true); object_aclcheck(Procedure,ACL_EXECUTE)+aclcheck_error(OBJECT_FUNCTION,get_func_name); InvokeFunctionExecuteHook; parse args; pq_getmsgend; strict-null short-circuit; FunctionCallInvoke (callit) else isnull=true,retval=0; CHECK_FOR_INTERRUPTS; SendFunctionResult; PopActiveSnapshot; check_log_duration switch (1→"duration: %s ms", 2→"duration: %s ms  fastpath function call: \"%s\" (OID %u)"). collation=InvalidOid (matches C comment). |
| `parse_fcall_arguments` (328) | `parse_fcall_arguments` | MATCH (SEAMED type-in) | numAFormats=pq_getmsgint(2) (zero-extended, see below); alloc+read aformats; nargs=pq_getmsgint(2); fn_nargs!=nargs||nargs>FUNC_MAX_ARGS→`ERRCODE_PROTOCOL_VIOLATION`; size args; numAFormats>1&&!=nargs→PROTOCOL_VIOLATION; per-arg loop: argsize=pq_getmsgint(4) (signed), -1→isnull, <0→PROTOCOL_VIOLATION "invalid argument size %d", else reset+append abuf; aformat select (>1→[i], >0→[0], else 0); aformat 0→getTypeInputInfo+pg_client_to_server+OidInputFunctionCall(NULL when argsize==-1); aformat 1→getTypeBinaryInputInfo+OidReceiveFunctionCall, whole-buffer check (consumed!=len)→`ERRCODE_INVALID_BINARY_REPRESENTATION` "incorrect binary data format in function argument %d" (i+1); else→`ERRCODE_INVALID_PARAMETER_VALUE`; return (int16)pq_getmsgint(2). |

## Constants verified vs headers

- `PqMsg_FunctionCallResponse = 'V'` — `libpq/protocol.h:53`. ✓
- `PROKIND_FUNCTION = 'f'` — `catalog/pg_proc.h`. ✓
- `NAMEDATALEN = 64`, `FUNC_MAX_ARGS = 100` — `pg_config_manual.h`. ✓
- `NamespaceRelationId = 2615`, `ProcedureRelationId = 1255` (`types_core::catalog::{NAMESPACE,PROCEDURE}_RELATION_ID`). ✓
- `ACL_USAGE = 1<<8`, `ACL_EXECUTE = 1<<7` — `utils/acl.h`. ✓
- SQLSTATEs: 25P02, 42883, 0A000, 08P01, 22023, 22P03, 53200 — all confirmed in `types_error`. ✓

## Sign-of-integer audit (one finding, fixed)

C `pq_getmsgint(msg, 2)` returns `unsigned int` (the uint16 **zero-extended**),
so `numAFormats` / `nargs` (assigned to `int`) are always 0..65535, never
negative. The initial port read these with `as i16 as i32` (sign-extending,
turning 0xFFFF into -1, which would skip the format-array allocation and
the `nargs > FUNC_MAX_ARGS` guard a hostile client could otherwise hit).
**Fixed** to `as i32` (zero-extension), matching C. The `aformats[i]` reads
(C `int16` array) and the `rformat` return (C `(int16)` cast) keep `as i16`,
which is correct; `argsize = pq_getmsgint(4)` keeps `as i32` (C assigns the
uint32 to `int`, so -1 NULL sentinel is intended). Re-derived after the fix:
MATCH.

## Seam / wiring audit

- The unit's only C file is `fastpath.c`, whose sole external entry
  `HandleFunctionRequest` is called by `tcop/postgres.c` (PostgresMain). That
  is a one-directional dependency (postgres.c → fastpath), no cycle, so
  fastpath declares **no inward seam crate** and `init_seams()` is correctly
  empty. No `backend-tcop-fastpath-seams` crate exists — correct (an empty
  installer is a FAIL only when owned seam crates are outstanding; there are
  none). `init_seams()` is still wired into `seams-init::init_all()` and the
  `recurrence_guard` tests pass.
- Outward seams are all thin marshal+delegate (one call each, no branching /
  node construction in any seam path); each targets a genuine
  not-yet-ported owner and panics until it lands (mirror-PG-and-panic):
  - reused: `xact::is_aborted_transaction_block_state`,
    `snapmgr::{push_active_snapshot_transaction, pop_active_snapshot}`,
    `lsyscache::{get_type_output_info, get_type_input_info,
    get_type_binary_input_info, get_type_binary_output_info,
    get_namespace_name, get_func_name}`,
    `fmgr::fmgr_info`, `aclchk::{object_aclcheck, aclcheck_error}`,
    `objectaccess::invoke_namespace_search_hook`, `miscinit::get_user_id`,
    `mbutils::pg_client_to_server`, `postgres::check_for_interrupts`.
  - NEW (declared in the real owner's `-seams` crate; owners are `todo`, so
    not-yet-installed is expected):
    `syscache::search_pg_proc_fastpath` (→ new `types_namespace::FastpathProcRow`
    projection — opacity inherited: the real `pg_proc` columns, not a blob);
    `fmgr::{fastpath_input_function_call, fastpath_receive_function_call,
    fastpath_output_function_call, fastpath_send_function_call,
    fastpath_function_call_invoke}` (the generic raw-`Datum` PQfn dispatch;
    no generic `FunctionCallInvoke`/raw type-I/O seam existed — the existing
    fmgr seams are `TupleValue`-shaped for the record/rowtypes consumers.
    Dispatch is by OID because `FmgrInfo` cannot cross a seam, matching the
    repo-wide convention);
    `objectaccess::invoke_function_execute_hook`;
    `postgres::{log_statement_is_all, check_log_duration}`.

## Design conformance

- Allocating paths take `Mcx` and are fallible: the `aformats`/`args`/`abuf`
  growth all use `try_reserve` (→ `ERRCODE_OUT_OF_MEMORY`, C's palloc OOM);
  the mcx-backed `StringInfo` message buffers and the seam outputs
  (`PgVec`/`PgString`) carry the threaded `'mcx`. ✓
- No invented opacity: `FastpathProcRow` holds the real projected pg_proc
  columns; no `type X = usize`/`&[u8]` stand-ins. ✓
- No `static`/`Atomic`/`Mutex`/global state; no locks held across `?`. ✓
- No `todo!`/`unimplemented!`/`unreachable!`; the only `panic!` reachable is
  the seam-not-installed loud panic for an unported callee (allowed). ✓
- The single "for now" comment is verbatim C source (the collation=InvalidOid
  note), not an unledgered divergence. ✓

## Verdict: PASS

Every function MATCH (externals SEAMED per the rules); the one sign-extension
divergence was found and fixed before sign-off; zero seam findings; design
conformance clean. `cargo check --workspace` green, 4 crate unit tests +
2 recurrence_guard tests pass.
