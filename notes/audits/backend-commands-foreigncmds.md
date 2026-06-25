# Audit: backend-commands-foreigncmds

- **Date:** 2026-06-12 (independent re-audit from scratch)
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Branch:** `port/backend-commands-foreigncmds`
- **C source:** `postgres-18.3/src/backend/commands/foreigncmds.c` (1628 lines)
- **c2rust:** `c2rust-runs/backend-commands-foreigncmds/src/foreigncmds.rs`
- **Port:** `crates/backend-commands-foreigncmds/src/lib.rs`, `crates/types-foreigncmds/src/lib.rs`

## Top-line verdict: **PASS** (one new FAIL found and fixed this round)

This run is an independent from-scratch re-audit. It **confirms the previously
reported finding is resolved** (the `ImportForeignSchema` loop body and
`import_error_callback` are genuinely in-crate, not in a seam) **but found a
new merge-blocking FAIL the earlier audit missed**: the unit's owned seam crate
`backend-commands-foreigncmds-seams` (two inward REASSIGN-OWNED seams,
`alter_foreign_server_owner_oid` / `alter_foreign_data_wrapper_owner_oid`,
called from `backend-catalog-pg-shdepend`) was **not installed** — the crate
did not depend on its own seams crate and `init_seams()` was an empty no-op.
Per §3 this is an automatic FAIL. The earlier audit wrongly concluded "owned
seam crates: none." **Fixed this round** (dependency added, `init_seams()`
wired with the `MemoryContext`-bridging idiom; build + tests green) and
re-audited clean — see the Seam audit section. Verdict after fix: **PASS**.

The confirmed-resolved IMPORT finding: `ImportForeignSchema`'s loop body and
`import_error_callback` relocated into the outward `import_foreign_schema_exec`
seam has been fixed.
The IMPORT loop orchestration now lives in this crate (`ImportForeignSchema` +
the per-command helper `import_one_command`): the `foreach` over the FDW
command list, the `nodeTag` type-check `elog` (ERRCODE_INTERNAL_ERROR), the
`IsImportableForeignTable` filter `continue`, the schema-name rewrite call, the
wrapper `PlannedStmt` construction (`ImportPlannedStmt` with `commandType =
CMD_UTILITY`, `canSetTag = false`, the embedded statement node, and the raw
stmt's location/length), and the inter-subcommand `CommandCounterIncrement`.
`import_error_callback` is also in-crate, implemented as the attach-on-
propagation idiom (`map_err`): it converts a syntax-error position to an
internal position/query and appends the `importing foreign table "%s"` context
line. The unported callees — `GetFdwRoutine` + the FDW `ImportForeignSchema`
callback (`fdw_import_foreign_schema`), `IsImportableForeignTable`
(`is_importable_foreign_table`), and the raw-parse-tree node projections /
schema-name rewrite — cross `backend-foreign-foreign-seams`; `pg_parse_query`
crosses `backend-tcop-postgres-seams`; `ProcessUtility` crosses the new
`backend-tcop-utility-fc-seams`. The `fdw_routine->ImportForeignSchema == NULL`
guard (ERRCODE_FDW_NO_SCHEMAS) is raised in-crate on the seam's `None` result.

The CRUD command drivers (FDW / SERVER / USER MAPPING / FOREIGN TABLE) and the
`transformGenericOptions` merge remain faithfully ported.

## Per-function table

| # | C function (foreigncmds.c) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `optionListToArray` (65) | folded into `transformGenericOptions` (lib.rs:156-167) | MATCH (folded) | Only foreigncmds-owned piece is the `"="`-in-name rejection (`ERRCODE_INVALID_PARAMETER_VALUE`, errloc 86); kept. text[] encode / `accumArrayResult` / `makeArrayResult` is genuine catalog encoding deferred to the catalog store seam. Acceptable fold. |
| 2 | `transformGenericOptions` (120) | `transformGenericOptions` (lib.rs:92) | MATCH | SET/ADD/DROP/UNSPEC merge identical; DROP/SET not-found → `ERRCODE_UNDEFINED_OBJECT`; ADD/UNSPEC dup → `ERRCODE_DUPLICATE_OBJECT`; validator call via `validate_options` seam with empty-array semantics. C's `default:` elog arm is impossible (exhaustive 4-variant enum) — correctly omitted. |
| 3 | `AlterForeignDataWrapperOwner_internal` (215) | lib.rs:201 | SEAMED | superuser / superuser_arg guards + hint strings + owner-change branch + post-alter hook ported; the tuple update + `aclnewowner` + `CatalogTupleUpdate` is real pg_foreign_data_wrapper DML via `fdw_set_owner` seam; `changeDependencyOnOwner` via shdepend seam. Thin delegate. |
| 4 | `AlterForeignDataWrapperOwner` (285) | lib.rs:255 | SEAMED | name lookup → `fdw_owner_row_by_name`; not-found error (errloc 300) + ObjectAddressSet ported. |
| 5 | `AlterForeignDataWrapperOwner_oid` (323) | lib.rs:276 | SEAMED | oid lookup → `fdw_owner_row_by_oid`; not-found (errloc 335) ported. |
| 6 | `AlterForeignServerOwner_internal` (348) | lib.rs:299 | SEAMED | full permission ladder (ownercheck / check_can_set_role / USAGE aclcheck on FDW with `OBJECT_FDW` error) ported; tuple/ACL update via `server_set_owner`; dep + hook ported. |
| 7 | `AlterForeignServerOwner` (425) | lib.rs:366 | SEAMED | name lookup seam; not-found (errloc 441) ported. |
| 8 | `AlterForeignServerOwner_oid` (460) | lib.rs:387 | SEAMED | oid lookup seam; not-found (errloc 472) ported. |
| 9 | `lookup_fdw_handler_func` (485) | lib.rs:426 + `func_name_arg` (411) | MATCH | `handler->arg == NULL` → InvalidOid; `LookupFuncName(arg,0,NULL,false)` via parse_func seam; rettype != `FDW_HANDLEROID` (3115) → `ERRCODE_WRONG_OBJECT_TYPE` (errloc 498). |
| 10 | `lookup_fdw_validator_func` (509) | lib.rs:452 | MATCH | `(text[]=1009, oid=26)`, arity 2, return ignored. |
| 11 | `parse_func_options` (528) | lib.rs:466 | MATCH | handler/validator dup → `errorConflictingDefElem` seam; unknown → internal elog (errloc 560). |
| 12 | `CreateForeignDataWrapper` (568) | lib.rs:508 | SEAMED | superuser guard, dup-name check, parse_func_options, transformGenericOptions, dep recording + extension dep + post-create hook ported; `insert_fdw` is real catalog DML seam. |
| 13 | `AlterForeignDataWrapper` (684) | lib.rs:597 | SEAMED | repl_handler/validator/options bookkeeping, both WARNINGs (errloc 740/755), validator-not-changed fallback, dep flush+rebuild, post-alter hook ported; `update_fdw` / `fdw_lookup_by_name` / `fdw_options` seams carry catalog DML. |
| 14 | `CreateForeignServer` (848) | lib.rs:731 | SEAMED | IF NOT EXISTS + checkMembershipInCurrentExtension + NOTICE/ERROR dup, USAGE aclcheck, transformGenericOptions, deps + extension + hook ported; `insert_server` seam DML. |
| 15 | `AlterForeignServer` (984) | lib.rs:843 | SEAMED | ownercheck-or-superuser, has_version/options branches, transformGenericOptions, post-alter hook ported; `server_lookup_by_name` / `server_options` / `update_server` seams. |
| 16 | `user_mapping_ddl_aclcheck` (1085) | lib.rs:919 | MATCH | ownercheck → (self ⇒ USAGE aclcheck) / (other ⇒ NOT_OWNER) ladder identical. |
| 17 | `CreateUserMapping` (1110) | lib.rs:954 | SEAMED | rolespec/PUBLIC split, aclcheck, uniqueness, IF NOT EXISTS NOTICE/ERROR with `MappingUserName`, transformGenericOptions, server dep + owner dep (only if `OidIsValid(useId)`) + hook ported; `insert_usermapping` seam DML. |
| 18 | `AlterUserMapping` (1236) | lib.rs:1055 | SEAMED | rolespec split, server lookup, `usermapping_oid` + not-found error, aclcheck, options branch, hook ported. C's re-fetch of the UM tuple + `cache lookup failed` elog folds into `usermapping_options` / `update_usermapping` seams (pg_user_mapping DML). |
| 19 | `RemoveUserMapping` (1334) | lib.rs:1126 | MATCH | PUBLIC/role split, IF EXISTS role NOTICE (internal, errloc 1354), server missing branches (errloc 1365/1370), UM missing branches (errloc 1383/1389), aclcheck on `srv->servername`, `performDeletion(DROP_CASCADE)` via dependency seam. Returns umId/InvalidOid correctly. |
| 20 | `CreateForeignTable` (1414) | lib.rs:1214 | SEAMED | leading `CommandCounterIncrement` (xact seam), USAGE aclcheck, transformGenericOptions ported; `insert_foreign_table` seam does the pg_foreign_table insert AND the `pg_class(relid) -> pg_foreign_server(serverid)` `recordDependencyOn` (dep edge on RelationRelationId, folded into the seam). |
| 21 | `ImportForeignSchema` (1494) | lib.rs:1294 + `import_one_command` | MATCH | Permission checks ported as before. The `foreach (lc, cmd_list)` body is now in-crate: the `ImportForeignSchema==NULL`/`ERRCODE_FDW_NO_SCHEMAS` guard (raised here on the `fdw_import_foreign_schema` seam's `None`), the `nodeTag != CreateForeignTableStmt` `elog` (errloc 1570), the `is_importable_foreign_table` filter `continue`, the `import_set_schemaname` rewrite, the `ImportPlannedStmt` build (CMD_UTILITY / canSetTag=false / utilityStmt / stmt_location / stmt_len), the `process_utility_import_subcommand` execute, and the per-subcommand `command_counter_increment`. `GetFdwRoutine`+callback / `pg_parse_query` / node projections / `ProcessUtility` cross their owners' seams (foreign-foreign / tcop-postgres / tcop-utility-fc). |
| 22 | `import_error_callback` (1610, static) | lib.rs `import_error_callback` | MATCH | In-crate as the attach-on-propagation transform (`map_err`): `geterrposition` → `err.cursor_position()`, `errposition(0)` → `with_cursor_position(0)`, `internalerrposition`/`internalerrquery` → `with_internal_position`/`with_internal_query`, `errcontext "importing foreign table \"%s\""` → `add_context_line`. Applied at every seam call inside the per-command loop, mirroring the `error_context_stack` push for the command's duration. |

## Seam audit

**Owned seam crate:** `backend-commands-foreigncmds-seams` (maps to
`foreigncmds.c` → owned by this unit per §3's C-source coverage rule). It
declares two inward seams — `alter_foreign_server_owner_oid` and
`alter_foreign_data_wrapper_owner_oid` — the `*_oid` owner-change entry points
reached from REASSIGN OWNED in `backend-catalog-pg-shdepend`
(`shdepReassignOwned_Owner`, a genuine dependency cycle). Both are real ported
functions (`AlterForeignServerOwner_oid` lib.rs:391, `AlterForeignDataWrapperOwner_oid`
lib.rs:280).

**FINDING (FAIL → fixed this round):** these were NOT installed — the crate did
not even depend on its own seams crate and `init_seams()` was an empty no-op.
Per §3 ("an empty installer with owned seam crates outstanding is an automatic
FAIL") this was an automatic FAIL.

**Fix applied:** added the `backend-commands-foreigncmds-seams` dependency and
wired `init_seams()` to install both seams. The REASSIGN-OWNED owner-oid seam
contract is `Mcx`-free (matching the neighbor `alter_type_owner_oid` /
`alter_schema_owner_oid` / `at_exec_change_owner` seams, all called from the
same shdepend dispatch where no `Mcx` is in scope); the ported functions need
an `Mcx` for syscache-row allocation, so each installer wrapper creates a local
`MemoryContext` and runs the ported function in it — the established bridging
idiom (cf. `backend-commands-matview::init_seams`).
`seams-init::init_all()` already calls
`backend_commands_foreigncmds::init_seams()`. Both seams now install; `set()`
appears at exactly one site (lib.rs:1561/1565). `cargo check -p
backend-commands-foreigncmds -p seams-init` and `cargo test -p
backend-commands-foreigncmds` (13 tests) pass. ✔ (re-audited clean)

**Outward seams used (into other owners — acceptable when thin marshal+delegate):**
- `backend-foreign-foreign-seams` (foreign.c, unported): catalog DML
  (`insert_*` / `update_*` / `*_set_owner` / `*_options` / `*_lookup_*` /
  `*_owner_row_*`), syscache projections, `get_foreign_*`, `usermapping_oid`,
  `mapping_user_name`, `validate_options`. Genuine pg_foreign_* catalog access /
  FDW cache lookups owned by foreign.c — thin. ✔ (panic seams, unimplemented
  until foreign.c lands — acceptable per "Mirror PG and panic.")
- `backend-utils-init-miscinit-seams` (superuser / superuser_arg / GetUserId). ✔
- `backend-utils-adt-acl-seams` (check_can_set_role / get_rolespec_oid). ✔
- `backend-catalog-aclchk-seams` (object_aclcheck / object_ownercheck /
  aclcheck_error / errorConflictingDefElem). ✔
- `backend-utils-cache-lsyscache-seams` (get_func_rettype). ✔
- `backend-catalog-namespace-seams` (LookupCreationNamespace). ✔
- `backend-catalog-objectaccess-seams` (post-create / post-alter hooks). ✔
- `backend-catalog-pg-depend-seams` (recordDependencyOn /
  recordDependencyOnCurrentExtension / checkMembershipInCurrentExtension /
  deleteDependencyRecordsForClass). ✔
- `backend-catalog-pg-shdepend-seams` (recordDependencyOnOwner /
  changeDependencyOnOwner). ✔
- `backend-catalog-dependency-seams` (performDeletion). ✔
- `backend-parser-parse-func-seams` (LookupFuncName),
  `backend-parser-parse-type-seams` (NameListToString). ✔
- `backend-access-transam-xact-seams` (CommandCounterIncrement). ✔

**Seam finding (resolved):**
- The fat `import_foreign_schema_exec` seam was removed and split into thin
  per-callee seams: `fdw_import_foreign_schema` (GetFdwRoutine + the FDW
  callback, returning the command list or `None` for an unsupported FDW),
  `is_importable_foreign_table` (the filter predicate), `import_classify_raw_stmt`
  (project one `RawStmt` into the tag/relname/location/len/node fields the loop
  branches on), and `import_set_schemaname` (the `pstrdup` rewrite) — all in
  `backend-foreign-foreign-seams` (foreign.c / fdwapi.c). `pg_parse_query` was
  added to `backend-tcop-postgres-seams`; `process_utility_import_subcommand`
  (ProcessUtility) to the new `backend-tcop-utility-fc-seams`. Each is a thin
  marshal+delegate; the loop control flow, the type-check `elog`, the filter
  `continue`, the `PlannedStmt` construction, the inter-subcommand
  `CommandCounterIncrement`, and the error-context callback all live in-crate.

## Design conformance (§3b)

- Types: `types-foreigncmds` defines the real parse-node statements, `DefElem`,
  and trimmed foreign descriptor / syscache row carriers; `RoleSpec` in
  types-nodes. No invented opacity / no `Oid`/`u64` stand-ins. ✔
- Constants verified against headers: ForeignDataWrapperRelationId=2328,
  ForeignServerRelationId=1417, ForeignTableRelationId=3118,
  UserMappingRelationId=1418 (CATALOG macros confirmed); ProcedureRelationId=1255,
  FDW_HANDLEROID=3115, TEXTARRAYOID=1009, OIDOID=26, ACL_ID_PUBLIC=0. ✔
- Allocating seams take `Mcx<'mcx>` and return `'mcx`-bound output
  (`fdw_owner_row_by_*`, `*_options`, `mapping_user_name`). ✔ No zero-arg value
  getters for per-backend globals; GetUserId is the sanctioned miscinit seam. ✔
- No shared statics, no ambient-global seams, no locks across `?`. ✔

## Conclusion

All 22 functions MATCH or are properly SEAMED. The previously reported IMPORT
finding is **confirmed resolved**: `ImportForeignSchema`'s loop orchestration
and `import_error_callback` live in this crate, calling the unported callees
(`GetFdwRoutine`+callback, `pg_parse_query`, `ProcessUtility`,
`IsImportableForeignTable`, the raw-parse-tree node projections) through thin
per-owner seams.

This independent re-audit found one new merge-blocking FAIL the earlier audit
missed — the owned seam crate `backend-commands-foreigncmds-seams` was
uninstalled (`init_seams()` empty) — and fixed it this round by wiring both
inward REASSIGN-OWNED seams via the `MemoryContext`-bridging installer idiom.
After the fix, every owned seam declaration is installed by exactly one
`set()`, the build and the crate's 13 tests pass, and there are no remaining
findings. Verdict: **PASS**.
