# Audit: backend-pl-plpgsql-handler (pl_handler.c)

C source: `src/pl/plpgsql/src/pl_handler.c` (PostgreSQL 18.3). No c2rust run for
this unit (extension; not in the c2rust catalog) — audited against the C source
directly. Port: `crates/backend-pl-plpgsql-handler/src/{lib.rs,seam.rs}`. Top
(7th) layer of the PL/pgSQL subsystem.

## Function inventory + verdicts

| C function (loc) | Port location | Verdict | Notes |
|---|---|---|---|
| `plpgsql_extra_checks_check_hook` (62) | lib.rs `plpgsql_extra_checks_check_hook` | MATCH | "all"→XCHECK_ALL, "none"→XCHECK_NONE, else SplitIdentifierString + per-token `shadowed_variables`/`too_many_rows`/`strict_multi_assignment`; combined-keyword + unrecognized errors return the exact `GUC_check_errdetail` text (`Ok(Err(detail))`). `pg_strcasecmp` → `eq_ignore_ascii_case`. C's `guc_malloc(*extra)` → returned bitmask. SplitIdentifierString called directly (varlena.c, no cycle). |
| `plpgsql_extra_warnings_assign_hook` (129) | lib.rs `plpgsql_extra_warnings_assign_hook` | MATCH | stores bitmask into `plpgsql_extra_warnings` cell. |
| `plpgsql_extra_errors_assign_hook` (135) | lib.rs `plpgsql_extra_errors_assign_hook` | MATCH | stores bitmask into `plpgsql_extra_errors` cell. |
| `_PG_init` (147) | lib.rs `_pg_init` | MATCH (structure); SEAMED externals | once-guard (`PG_INIT_INITED`), then exactly: pg_bindtextdomain, 5× DefineCustom*Variable, MarkGUCPrefixReserved("plpgsql"), RegisterXactCallback, RegisterSubXactCallback, find_rendezvous_variable — identical order to C 156–209. The GUC-registration / xact-callback / rendezvous externals route to loud seams (custom-GUC substrate unported; `plpgsql_xact_cb`/`_subxact_cb` live in pl_exec.c, unported). |
| `plpgsql_call_handler` (223) | lib.rs `plpgsql_call_handler` + `plpgsql_call_handler_pg` | MATCH (structure); SEAMED at compile | nonatomic from CallContext, SPI_connect_ext, compile (loud bridge — two fcinfo models), procedure_resowner iff `nonatomic && requires_procedure_resowner`, trigger/event-trigger/scalar dispatch, `fcinfo->isnull` set, SPI_finish with `SPI_result_code_string` elog on non-OK. The `use_count++`/`save_cur_estate`/PG_FINALLY decrement+restore is a no-op in the owned model (func is an owned value, not a shared cache pointer); the resowner release is part of the loud resowner substrate. |
| `plpgsql_inline_handler` (315) | lib.rs `plpgsql_inline_handler` + `plpgsql_inline_handler_pg` | MATCH (structure); reachable | SPI_connect_ext (nonatomic iff `!atomic`), `plpgsql_compile_inline(source_text)`, run `plpgsql_exec_function` with `&[]` args + `atomic`, `plpgsql_free_function_memory`, SPI_finish. private simple_eval_estate/resowner are `None` (exec creates its econtext lazily; a control-flow-only block never reads them). The PG_CATCH cleanup branch (subxact_cb + FreeExecutorState + free) is the SPI/xact substrate; the happy path is real. Returns `(Datum)0` = `Ok(())`. |
| `plpgsql_validator` (442) | lib.rs `plpgsql_validator` + `plpgsql_validator_pg` | MATCH (structure); SEAMED externals | `CheckFunctionValidatorAccess` early-out (PG_RETURN_VOID), then the pg_proc read + pseudotype rejection + test-compile bottom out in the syscache/catalog/GUC substrate (`validate_function_body` loud). |

Module-level data: `variable_conflict_options[]` table, the 5 GUC globals
(`plpgsql_variable_conflict`/`print_strict_params`/`check_asserts`/
`extra_warnings`/`extra_errors`), `plpgsql_plugin_ptr` — modeled as per-backend
`thread_local` cells seeded with the identical C compile-time defaults
(PLPGSQL_RESOLVE_ERROR / false / true / XCHECK_NONE / XCHECK_NONE). MATCH.

## Seam audit

Owned inward seam crate: **none** — pl_handler.c is the top layer; nothing calls
into it across a cycle, so no `backend-pl-plpgsql-handler-seams` exists (correct).

Installed by this crate's `init_seams()`:
- `backend_commands_functioncmds_seams::execute_inline_handler` → the native
  `plpgsql_inline_handler(codeblock)` (the DO-block dispatch `ExecuteDoStmt`
  reaches). Installed exactly once in the tree (verified by grep); previously
  uninstalled. `init_seams()` contains only the one `set()` + the fmgr builtin
  registration. Wired into `seams-init::init_all()`.
- `register_handler_builtins()` registers the three handler `PGFunction`s in the
  fmgr built-in registry by **name** (`plpgsql_call_handler` etc., `foid=0`
  placeholder overwritten by the catalog OID). plpgsql is `CREATE EXTENSION`-
  installed, so its function OIDs are catalog-assigned, not fixed builtin OIDs;
  name-keyed registration is the C-language `fmgr_lookup_by_name` resolution
  path. This is the `PG_FUNCTION_INFO_V1` analogue (install-time table
  registration), not leaked logic.

Outward seams (`src/seam.rs`) — each names its precise C callee + the unported
external and panics (REAL-OR-LOUD), never faked:
- _PG_init substrate: `DefineCustom*Variable` ×5, `MarkGUCPrefixReserved`,
  `RegisterXactCallback`/`RegisterSubXactCallback` (callbacks in pl_exec.c),
  `find_rendezvous_variable` — all unported substrate. `pg_bindtextdomain` is a
  real no-op (single-locale build, no control-flow effect).
- call-handler fmgr demux: `called_nonatomic` (real: false unless CallContext;
  the CallContext.atomic bit is not carried by the tag-only ContextNode → loud),
  `called_as_trigger`/`called_as_event_trigger` (real tag compares),
  `take_trigger_data`/`take_event_trigger_data`/`create_procedure_resowner`/
  `compile_for_call` (loud: rich context / resowner / fcinfo-model bridge
  substrate).
- validator: `getarg_oid`/`flinfo_fn_oid` (real fcinfo reads),
  `check_function_validator_access`/`validate_function_body` (loud:
  syscache/catalog/GUC).
- error plumbing: `elog_spi_finish_failed` (real, uses SPI_result_code_string),
  `propagate` (real: `panic_any(PgError)` — the C `PG_RE_THROW`/ereport longjmp
  channel `invoke_pgfunction` catches).

No branching/node-construction/computation in any seam path beyond thin
marshal+delegate (the tag compares + the false-default of `called_nonatomic` are
direct fcinfo inspection, faithful to the C macros).

## Enabling exec changes (pl_exec.c, audited inline)

To make the DO control-flow path runnable, the following pl_exec.c functions were
ported into `backend-pl-plpgsql-exec` (previously panic stubs):
- `plpgsql_estate_setup` — MATCH for the scalar control-flow fields; the
  substrate handles (paramLI/makeParamList, simple_eval EState, cast hash,
  eval_econtext/plpgsql_create_econtext) are `None` and created lazily on first
  expr eval (which is itself loud). Documented divergence: C creates the
  econtext unconditionally; lazy creation is behavior-identical for any path
  that never evaluates an expression, and the expr-eval seams stay loud.
- `copy_plpgsql_datums` — MATCH: per-call clone of the datum array (C byte-copies
  VAR/PROMISE/REC, shares ROW/RECFIELD; the owned clone is value-equivalent).
- `assign_simple_var` — MATCH: the value store + promise-cancel is real; the
  non-atomic detoast leg and the free-old-value leg route loud (toast/expanded-
  object substrate). Exercised by FOUND + FOR-loop var stores.
- `plpgsql_exec_function` — MATCH for the entry/arg-store/FOUND/toplevel-block/
  RC-check/VOID-and-exact-type-match result legs; the SRF, tuple-coercion, and
  cast-to-declared-type result legs route loud. Returns a `FunctionResult`
  (value/isnull/rettype) — the owned analogue of writing `fcinfo->isnull` +
  returning the Datum.

## Design conformance

- No `type _ = Oid/uN/iN` opacity aliases; no `&[u8]` blob signatures.
- Per-backend C globals → `thread_local!` cells with const C-default values
  (not shared statics). `PG_INIT_INITED` mirrors C's `static bool inited`.
- `plpgsql_extra_checks_check_hook` takes `Mcx` + returns `PgResult` (the
  SplitIdentifierString scratch parse pallocs). No infallible alloc on a
  palloc path.
- `create_procedure_resowner() -> ResourceOwner` is a constructor seam
  (`ResourceOwnerCreate(NULL, name)`), not a foreign-global getter.
- No locks held across `?`. No `todo!`/`unimplemented!`. The "hack" comments
  quote the C source comments verbatim ("Special hack for function returning
  VOID").

## Verdict: PASS

Every function MATCH (structure) with externals SEAMED per step-3 rules (each a
real unported callee, loud, never faked). The single documented divergence
(lazy econtext creation in `plpgsql_estate_setup`) is behavior-identical on
every control-flow-only input and keeps the expr substrate loud. Seam wiring
clean: `execute_inline_handler` installed once, no leaked logic, wired into
`init_all()`. Gates green: `cargo build --bin postgres`, `seams-init` tests,
`no-todo-guard`.
