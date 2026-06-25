# Audit: backend-tcop-utility (classifier core)

C source: `src/backend/tcop/utility.c` (3770 lines, PostgreSQL 18.3).
Port: `crates/backend-tcop-utility/src/{consts,classify,returns,loglevel,commandtag}.rs`.
Out-seams: `crates/backend-tcop-utility-out-seams/src/lib.rs`.
Re-derived from the C, the src-idiomatic reference
(`../pgrust/src-idiomatic/crates/backend-tcop-utility`), reconciled to this
repo's split-crate owned-`Node` model.

## Scope of this port

The **parse-tree classifiers** of utility.c are ported in-crate 1:1. The
**dispatch** (`ProcessUtility` / `standard_ProcessUtility` / `ProcessUtilitySlow`
/ `ProcessUtilityForAlterTable` / `ExecDropStmt`) is NOT ported — keystone-blocked
(see below).

## Function inventory (ported)

| C function (loc) | Port | Verdict | Notes |
|---|---|---|---|
| `CommandIsReadOnly` (94) | `CommandIsReadOnly` | MATCH | CMD_SELECT (rowMarks!=NIL / hasModifyingCTE → false), CMD_UPDATE/INSERT/DELETE/MERGE → false, CMD_UTILITY → false, default → `elog(WARNING,"unrecognized commandType: %d")`+false. `rowMarks` is `Option<PgVec>`; `is_some_and(!is_empty())` mirrors `!= NIL`. |
| `ClassifyUtilityCommandAsReadOnly` (127, static) | `ClassifyUtilityCommandAsReadOnly` | MATCH | All node-tag arms reproduced verbatim: the big DDL/TRUNCATE → NOT_READ_ONLY group; ALTER SYSTEM/CALL/DO/CHECKPOINT → STRICTLY_READ_ONLY; backend-local group → OK_IN_RECOVERY|OK_IN_READ_ONLY_TXN; CLUSTER/REINDEX/VACUUM → OK_IN_READ_ONLY_TXN; COPY (is_from ? OK_IN_READ_ONLY_TXN : STRICTLY); EXPLAIN/SHOW → STRICTLY; LISTEN/NOTIFY → OK_IN_READ_ONLY_TXN; LOCK (mode>RowExclusiveLock ? OK_IN_READ_ONLY_TXN : STRICTLY); TransactionStmt sub-switch (BEGIN/START/COMMIT/ROLLBACK/SAVEPOINT/RELEASE/ROLLBACK_TO → STRICTLY; PREPARE/COMMIT_PREPARED/ROLLBACK_PREPARED → OK_IN_READ_ONLY_TXN); default → `elog(ERROR,"unrecognized node type: %d")`. |
| `PreventCommandIfReadOnly` (404) | `PreventCommandIfReadOnly` | MATCH | `xact_read_only` seam → ERRCODE_READ_ONLY_SQL_TRANSACTION "cannot execute %s in a read-only transaction". |
| `PreventCommandIfParallelMode` (422) | `PreventCommandIfParallelMode` | MATCH | `is_in_parallel_mode` seam → ERRCODE_INVALID_TRANSACTION_STATE "cannot execute %s during a parallel operation". |
| `PreventCommandDuringRecovery` (441) | `PreventCommandDuringRecovery` | MATCH | `recovery_in_progress` seam → ERRCODE_READ_ONLY_SQL_TRANSACTION "cannot execute %s during recovery". **Installed** as the `prevent_command_during_recovery` inward seam (consumed by async.c `pg_notify`, xid8funcs). |
| `CheckRestrictedOperation` (458, static) | `CheckRestrictedOperation` | MATCH | `in_security_restricted_operation` seam → ERRCODE_INSUFFICIENT_PRIVILEGE "cannot execute %s within security-restricted operation". |
| `UtilityReturnsTuples` (2028) | `UtilityReturnsTuples` | MATCH | CALL (funcexpr→FuncExpr.funcresulttype==RECORDOID, reached through `Node::Expr`+`as_funcexpr`), FETCH (ismove→false; portal tupDesc predicate seam), EXECUTE (has-result seam), EXPLAIN/SHOW → true, default → false. **Installed** as `utility_returns_tuples` (consumed by pquery). |
| `UtilityTupleDescriptor` (2084) | `UtilityTupleDescriptor` | MATCH | CALL/FETCH/EXECUTE/EXPLAIN/SHOW each route to a descriptor-source seam, allocating in `mcx`; NULL → `None` (repo `TupleDesc<'mcx> = Option<PgBox<…>>`). FETCH ismove → None. **Installed** as `utility_tuple_descriptor` (pquery passes the `mcx`). |
| `QueryReturnsTuples` (2137, `#ifdef NOT_USED`) | `QueryReturnsTuples` | MATCH | Ported for completeness; CMD_SELECT→true, modify forms→`!returningList.is_empty()`, CMD_UTILITY→UtilityReturnsTuples(utilityStmt), else false. |
| `UtilityContainsQuery` (2179) | `UtilityContainsQuery` | MATCH | DECLARE CURSOR / EXPLAIN / CREATE TABLE AS → contained query; `castNode(Query)` recursion through nested utility-Query wrappers via `as_query`. |
| `AlterObjectTypeCommandTag` (2215, static) | `AlterObjectTypeCommandTag` | MATCH | Full ObjectType → CMDTAG_ALTER_* table. |
| `CreateCommandTag` (2362) | `CreateCommandTag` | MATCH (carrier-limited arms noted) | Every raw-stmt / utility-stmt arm + the TransactionStmt / DropStmt / DefineStmt / CreateTableAsStmt / VariableSetStmt / DiscardStmt / GrantStmt / GrantRoleStmt / Rename / AlterObject* sub-switches reproduced. `Query` (analyzed) arm full incl. SELECT FOR-strength refinement via `RowMarkClause.strength`. |
| `GetCommandLogLevel` (3249) | `GetCommandLogLevel` | MATCH (carrier-limited arms noted) | Full tag switch: MOD for raw modify stmts + TRUNCATE; SELECT INTO → DDL; DDL group; PREPARE/EXECUTE look-through (EXECUTE via raw-parse-tree seam); EXPLAIN ANALYZE option scan via `def_get_boolean` seam + look-through; ALL group. |

## Carrier-model limitations (documented divergences, not omissions)

- `CreateCommandTag` / `GetCommandLogLevel` C arms `case T_RawStmt:` and
  `case T_PlannedStmt:`: `RawStmt`/`PlannedStmt` are NOT `types_nodes::Node`
  variants in this repo, so a `&Node` can never carry one. The arms are
  documented-out; the analyzed-`Query` path (`Node::Query`) covers the cooked
  case. (The dispatcher derives a PlannedStmt's tag from its `utilityStmt`, not
  through this `&Node` entrypoint.)
- `CreateCommandTag` `T_PlannedStmt` CMD_SELECT per-rowmark-strength refinement
  is moot (no PlannedStmt arm); the `Query` arm DOES refine via the real
  `RowMarkClause.strength`, matching C for the analyzed case.

## Constants verified vs headers

- `COMMAND_OK_IN_READ_ONLY_TXN/PARALLEL_MODE/RECOVERY` (0x1/0x2/0x4),
  `COMMAND_IS_STRICTLY_READ_ONLY` (0x7), `COMMAND_IS_NOT_READ_ONLY` (0) —
  `tcop/utility.h`. ✓
- `LOGSTMT_NONE/DDL/MOD/ALL` (0..3) — `tcop/tcopprot.h`. ✓
- `RowExclusiveLock = 3` — `storage/lockdefs.h`. ✓
- `RECORDOID = 2249`, `ROLE_PG_CHECKPOINT = 4544`. ✓
- Full `CMDTAG_*` enumerator table — 0-based `cmdtaglist.h` positions, verified
  against PG 18.3 (SELECT=179, REFRESH_MATERIALIZED_VIEW=169, etc.).
  `CommandTag(i32)` newtype values match `types_core::cmdtag` (SELECT=179 ✓).

## Dispatch: keystone-blocked (NOT ported)

`standard_ProcessUtility` must `make_parsestate(NULL)` (allocates a
`ParseState<'mcx>`), `copyObject(pstmt)` when `readOnlyTree`, and run the
`parse_utilcmd.c` transforms (`transformCreateStmt`/`transformIndexStmt`/
`transformStatsStmt`) inside `ProcessUtilitySlow` — every one needs an
`Mcx<'mcx>`. The already-installed-and-consumed inward `process_utility` seam
(`backend-tcop-pquery::portal_run_utility`, line 1084) carries the C parameter
set **minus an `mcx`** (`&PlannedStmt`, query_string, read_only_tree, context,
params, dest, `&mut QueryCompletion`), and `portal_run_utility` holds no `Mcx`
to supply. There is no ambient memory context in this workspace (by design).

Re-signing the inward `process_utility` seam to carry an `mcx` AND threading a
per-query `Mcx` into `portal_run_utility` is the prerequisite keystone. Until it
lands, the dispatch cannot be installed faithfully; the `process_utility` inward
seam stays on its panic-until-installed default (unchanged by this port). The
four classifier inward seams ARE installed.

## Seam wiring

- Inward seams installed in `init_seams()`: `create_command_tag`,
  `utility_returns_tuples`, `utility_tuple_descriptor`,
  `prevent_command_during_recovery` (wired into `seams-init::init_all`).
- Out-seams declared (owned by xact/xlog/miscinit/portal/prepare/explain/guc/
  define owners, installed when they land): the 4 state predicates +
  7 descriptor sources + `def_get_boolean` + `execute_stmt_raw_parse_tree`.

## Gate

`cargo check --workspace` clean; `no-todo-guard` green (no todo!/unimplemented!);
`seams-init` recurrence guards (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`) green.
