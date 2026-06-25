# Audit: backend-commands-amcmds

Date: 2026-06-13. Model: Claude Opus 4.8 (1M). Verdict: PASS (independent re-audit).

C source: `src/backend/commands/amcmds.c` (PostgreSQL 18.3).
Port crate: `crates/backend-commands-amcmds`.
Cross-checked against `c2rust-runs/backend-commands-amcmds/src/amcmds.rs`.

## Function inventory (8 functions — all present)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `CreateAccessMethod` | amcmds.c:41 | lib.rs `CreateAccessMethod` | MATCH | table_open(AccessMethodRelationId, RowExclusiveLock) → superuser check (ERRCODE_INSUFFICIENT_PRIVILEGE + errhint) → GetSysCacheOid1(AMNAME) dup check (ERRCODE_DUPLICATE_OBJECT) → lookup_am_handler_func → insert tuple (GetNewOidWithIndex(AmOidIndexId)+namein+heap_form_tuple+CatalogTupleInsert) → recordDependencyOn(handler, DEPENDENCY_NORMAL) → recordDependencyOnCurrentExtension(false) → InvokeObjectPostCreateHook → table_close. Order, error codes, ProcedureRelationId reference class all match. |
| `get_am_type_oid` (static) | amcmds.c:128 | lib.rs `get_am_type_oid` | MATCH | SearchSysCache1(AMNAME); if valid and amtype!='\0' and amform.amtype!=amtype → ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE ("is not of type %s" using NameStr(amform->amname)); oid=amform.oid; if !OidIsValid(oid) && !missing_ok → ERRCODE_UNDEFINED_OBJECT. |
| `get_index_am_oid` | amcmds.c:162 | lib.rs `get_index_am_oid` | MATCH | get_am_type_oid(name, AMTYPE_INDEX, missing_ok). |
| `get_table_am_oid` | amcmds.c:172 | lib.rs `get_table_am_oid` | MATCH | get_am_type_oid(name, AMTYPE_TABLE, missing_ok). |
| `get_am_oid` | amcmds.c:182 | lib.rs `get_am_oid` | MATCH | get_am_type_oid(name, '\0'(NO_AMTYPE), missing_ok). |
| `get_am_name` | amcmds.c:192 | lib.rs `get_am_name` | MATCH | result=None; SearchSysCache1(AMOID) via the `search_am_name` syscache projection seam (GETSTRUCT + pstrdup(NameStr) into mcx, owns ReleaseSysCache); if valid → Some. Body kept in-crate (control flow not delegated). |
| `get_am_type_string` (static) | amcmds.c:212 | lib.rs `get_am_type_string` | MATCH | AMTYPE_INDEX→"INDEX", AMTYPE_TABLE→"TABLE", default→elog(ERROR,"invalid access method type '%c'") (errmsg_internal). Returns &'static str (C const char *). |
| `lookup_am_handler_func` (static) | amcmds.c:231 | lib.rs `lookup_am_handler_func` | MATCH | NIL/empty → ERRCODE_UNDEFINED_FUNCTION ("handler function is not specified"); funcargtypes=[INTERNALOID]; LookupFuncName(handler_name, 1, argtypes, false); amtype switch sets expectedType (INDEX_AM_HANDLEROID / TABLE_AM_HANDLEROID; default elog ERROR "unrecognized access method type"); if get_func_rettype != expectedType → ERRCODE_WRONG_OBJECT_TYPE ("function %s must return type %s", get_func_name, format_type_extended(t,-1,0)=format_type_be). |

## Constants verified against C headers / c2rust

- `INDEX_AM_HANDLEROID = 325`, `TABLE_AM_HANDLEROID = 269` (pg_type.dat / c2rust literals 325, 269). Added to types-core.
- `AMTYPE_INDEX = 'i' (105)`, `AMTYPE_TABLE = 't' (116)` (pg_am.h / c2rust).
- `T_CreateAmStmt = 180` (nodes/nodetags.h).
- `ACCESS_METHOD_RELATION_ID = 2601`, `PROCEDURE_RELATION_ID = 1255`, `INTERNALOID = 2281`, `DEPENDENCY_NORMAL = 'n'` — all from existing type crates.

## Seam audit

Owned inward seam crate: `backend-commands-amcmds-seams` declares `get_index_am_oid`.
Installed by `backend_commands_amcmds::init_seams()` (transient MemoryContext bridge; the seam returns Copy Oid, mcx only feeds the wrong-type error message). `init_seams()` is wired into `seams-init::init_all()` and contains only the one `set()`. PASS.

Outward seams (all thin marshal+delegate, justified by cycles into not-yet-direct-dep owners):
- `superuser` (miscinit-seams), `lookup_func_name` (parse-func-seams), `get_func_rettype` (lsyscache-seams), `format_type_be` (format-type-seams), `recordDependencyOn`/`recordDependencyOnCurrentExtension` (pg-depend-seams), `invoke_object_post_create_hook` (objectaccess-seams) — pre-existing canonical seams reused.
- `table_open`/`table_close` — direct dep on `backend-access-table-table` (ported; no cycle).
- NEW `get_am_oid_by_name`, `search_am_by_name` (syscache-seams) and `catalog_tuple_insert_pg_am` (indexing-seams): declarations only, mirror-and-panic until their owners (`backend-catalog-indexing` is `todo`; syscache install is the syscache unit's duty — consistent with the existing declared-but-not-installed `search_am_handler`/`search_am_name`). No amcmds-owned logic lives in any seam.

`search_am_by_name` returns the new `PgAmInfo` projection (oid, amtype, amname) in types-namespace — concrete row, no opacity. `get_am_oid_by_name` returns Copy Oid (no mcx). `search_am_by_name` + `catalog_tuple_insert_pg_am` take Mcx/RelationData and return PgResult (allocating/fallible). No invented opaque handles, no stand-in type aliases.

## Verdict: PASS

All 8 functions MATCH; zero seam findings; design-conformance greps clean (no todo/unwrap/panic, allocations only at errmsg return-Err sites, no statics, no stand-in aliases). `cargo check --workspace` and both recurrence_guard tests green.
