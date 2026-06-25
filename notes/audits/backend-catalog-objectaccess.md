# Audit: backend-catalog-objectaccess

- **Verdict: PASS**
- Date: 2026-06-13
- Model: claude-opus-4-8[1m]
- Branch: port/backend-catalog-objectaccess
- C source: `postgres-18.3/src/backend/catalog/objectaccess.c` (+ header
  `src/include/catalog/objectaccess.h` for the `Invoke*` macros)
- c2rust: `c2rust-runs/backend-catalog-probe-objectaccess/src/objectaccess.rs`
- Port: `crates/backend-catalog-objectaccess/src/lib.rs`
- Seam crate: `crates/backend-catalog-objectaccess-seams/src/lib.rs`

## 1. Function inventory

The `.c` defines 2 globals and 12 `Run*Hook` functions (6 object-ID + 6 `*Str`).
The header defines 12 `Invoke*Hook*` macros (the check-then-run wrappers), which
the port renders as functions. c2rust confirms exactly the 2 globals + 12 Run*
functions (macros are not in the c2rust output, being header macros). No statics
or inline helpers beyond these.

## 2. Per-function comparison

| C entity (objectaccess.c / .h) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `object_access_hook` (c:22) | `OBJECT_ACCESS_HOOK` thread_local (l:60) | MATCH | per-backend fn-ptr slot, default None; genuine external (sepgsql/security-label plugin, unported) |
| `object_access_hook_str` (c:23) | `OBJECT_ACCESS_HOOK_STR` thread_local (l:62) | MATCH | same |
| `RunObjectPostCreateHook` (c:31-46) | `run_object_post_create_hook` (l:124) | MATCH | OAT_POST_CREATE, classId, objectId, subId, &pc_arg{is_internal}; memset(0)+set == struct literal (no other fields) |
| `RunObjectDropHook` (c:53-68) | `run_object_drop_hook` (l:141) | MATCH | OAT_DROP, &drop_arg{dropflags} |
| `RunObjectTruncateHook` (c:75-84) | `run_object_truncate_hook` (l:158) | MATCH | OAT_TRUNCATE, RelationRelationId(1259), subId 0, arg NULL→`ObjectAccessArg::None` |
| `RunObjectPostAlterHook` (c:91-107) | `run_object_post_alter_hook` (l:169) | MATCH | OAT_POST_ALTER, &pa_arg{auxiliary_id,is_internal} |
| `RunNamespaceSearchHook` (c:114-131) | `run_namespace_search_hook` (l:191) | MATCH | OAT_NAMESPACE_SEARCH, NamespaceRelationId(2615), ns_arg{ereport_on_violation, result=true}; returns ns_arg.result (out-param via &mut) |
| `RunFunctionExecuteHook` (c:138-147) | `run_function_execute_hook` (l:207) | MATCH | OAT_FUNCTION_EXECUTE, ProcedureRelationId(1255), arg NULL→None |
| `RunObjectPostCreateHookStr` (c:157-172) | `run_object_post_create_hook_str` (l:223) | MATCH | str variant, uses object_access_hook_str |
| `RunObjectDropHookStr` (c:179-194) | `run_object_drop_hook_str` (l:240) | MATCH | |
| `RunObjectTruncateHookStr` (c:201-210) | `run_object_truncate_hook_str` (l:257) | MATCH | RelationRelationId, None arg |
| `RunObjectPostAlterHookStr` (c:217-233) | `run_object_post_alter_hook_str` (l:268) | MATCH | |
| `RunNamespaceSearchHookStr` (c:240-257) | `run_namespace_search_hook_str` (l:289) | MATCH | NamespaceRelationId, returns ns_arg.result |
| `RunFunctionExecuteHookStr` (c:264-273) | `run_function_execute_hook_str` (l:308) | MATCH | ProcedureRelationId, None arg |
| `InvokeObjectPostCreateHook[Arg]` (h:173-180) | `invoke_object_post_create_hook` (l:329) | MATCH | guards on object_access_hook present; macro w/o Arg passes is_internal=false |
| `InvokeObjectDropHook[Arg]` (h:182-189) | `invoke_object_drop_hook` (l:344) | MATCH | macro w/o Arg passes dropflags=0 |
| `InvokeObjectTruncateHook` (h:191-195) | `invoke_object_truncate_hook` (l:358) | MATCH | |
| `InvokeObjectPostAlterHook[Arg]` (h:197-206) | `invoke_object_post_alter_hook` (l:369) | MATCH | macro w/o Arg passes auxiliaryId=InvalidOid, is_internal=false |
| `InvokeNamespaceSearchHook` (h:208-211) | `invoke_namespace_search_hook` (l:385) | MATCH | returns true when no hook, else hook verdict |
| `InvokeFunctionExecuteHook` (h:213-217) | `invoke_function_execute_hook` (l:398) | MATCH | |
| `InvokeObjectPostCreateHook[Arg]Str` (h:220-227) | `invoke_object_post_create_hook_str` (l:407) | MATCH | guards on object_access_hook_str |
| `InvokeObjectDropHook[Arg]Str` (h:229-236) | `invoke_object_drop_hook_str` (l:421) | MATCH | |
| `InvokeObjectTruncateHookStr` (h:238-242) | `invoke_object_truncate_hook_str` (l:435) | MATCH | |
| `InvokeObjectPostAlterHook[Arg]Str` (h:244-253) | `invoke_object_post_alter_hook_str` (l:444) | MATCH | |
| `InvokeNamespaceSearchHookStr` (h:255-258) | `invoke_namespace_search_hook_str` (l:460) | MATCH | returns true when no hook |
| `InvokeFunctionExecuteHookStr` (h:260-264) | `invoke_function_execute_hook_str` (l:473) | MATCH | |

### Constants verified against C headers (not memory)
- `RELATION_RELATION_ID = 1259`, `NAMESPACE_RELATION_ID = 2615`,
  `PROCEDURE_RELATION_ID = 1255` (types-catalog/src/catalog.rs:7,55,78), matching
  the c2rust constants (objectaccess.rs:63-65).
- `OAT_POST_CREATE=0, OAT_DROP=1, OAT_POST_ALTER=2, OAT_NAMESPACE_SEARCH=3,
  OAT_FUNCTION_EXECUTE=4, OAT_TRUNCATE=5` (types-catalog/src/object_access.rs),
  matching the C enum order (objectaccess.h:48-56).

### Notes on faithful idiomatic restructuring
- The C `Assert(object_access_hook != NULL)` becomes `call_hook` `.expect(...)`
  — same "caller must have checked" contract; panics only on a programming error,
  not a runtime input.
- `void *arg` (opaque in C) is resolved to the typed `ObjectAccessArg<'a>` enum
  in types-catalog::object_access; this is inherited opacity resolved to real
  types (the C always points it at a concrete stack struct), not invented opacity.
  The `&mut` borrow preserves the namespace-search `result` out-parameter
  semantics exactly.
- A hook may `ereport(ERROR)`; every entrypoint returns `PgResult` — seam
  signatures mirror the C failure surface.

## 3. Seam audit

Owned seam crate (by C-source coverage, objectaccess.c):
`crates/backend-catalog-objectaccess-seams` — declares 6 inward seams:
`invoke_namespace_search_hook`, `object_access_hook_present`,
`invoke_object_post_create_hook`, `invoke_object_post_alter_hook`,
`invoke_object_post_alter_hook_arg`, `run_object_post_create_hook`.

All 6 are installed by `init_seams()` (lib.rs:482-495) with nothing but `set()`
calls; the two macro-default wrappers (`invoke_object_post_create_hook` → adds
`false`; `invoke_object_post_alter_hook` → adds `InvalidOid, false`) reproduce
the exact C macro default arguments — thin marshal, no invented branching.
`seams-init::init_all()` calls `backend_catalog_objectaccess::init_seams()`
(seams-init/src/lib.rs:22). No `set()` outside the owner. No uninstalled seam.
No function body was replaced by an outward seam call — all logic lives in this
crate (this is a leaf unit; the hook pointer is the only external).

## 3b. Design conformance

- Opacity: `void *arg` resolved to a real typed enum, no invented handles
  (types.md rules 6-7 satisfied). PASS.
- Allocation/Mcx: no allocating functions; no `Mcx` needed; failure surface
  carried via `PgResult`. PASS.
- Per-backend globals: hook pointers modeled as `thread_local` per-backend slots,
  not shared statics — conforms to the per-backend-globals rule. PASS.
- No ambient-global seams, no locks across `?`, no registry side tables, no
  unledgered divergence markers. PASS.

## 4. Verdict

**PASS** — all 26 entities MATCH, zero seam findings, design-conformant. Build
and the crate's 9 unit tests are green.
