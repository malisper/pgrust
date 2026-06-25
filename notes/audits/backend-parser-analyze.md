# Audit: backend-parser-analyze (analyze.c) — SELECT-path milestone

C source: `src/backend/parser/analyze.c` (3745 LOC).
Crate: `crates/backend-parser-analyze` (lib.rs / select.rs / setop.rs / locking.rs).
Reference: `../pgrust/src-idiomatic/crates/backend-parser-analyze` (logic only;
its central-seams model is dead) and `../pgrust/c2rust-runs/backend-parser-analyze`.

This lane lands the SELECT path end to end (the Workstream-A milestone:
SQL text -> raw_parser -> transformStmt -> owned walkable `Query<'mcx>`) plus the
dispatch, drivers, locking family, and the requires-analysis predicates.
INSERT/UPDATE/DELETE/MERGE/RETURN/PLAssign/DeclareCursor/Explain/CreateTableAs/Call
are a decomposition follow-on (see the decomp-plan section).

## Per-function status

| C function | line | status | notes |
|---|---|---|---|
| `parse_analyze_fixedparams` | 105 | PORTED (`parse_analyze_fixedparams`) | make_parsestate(NULL)+p_sourcetext, transformTopLevelStmt, free_parsestate. JumbleQuery / post_parse_analyze_hook / pgstat_report_query_id are no-ops in the default config (query-id off, NULL hook, queryId==0). Params path: mirror-PG-and-panic (small1 `setup_parse_fixed_parameters` returns a carrier, not a ParseState mutation — owned-model param-hook follow-on; no milestone consumer passes params). |
| `parse_analyze_varparams` | 145 | DEFERRED | needs small1 var-param hook wiring (the ParamHookState carrier keystone small1 left mirror-and-panic); follow-on. |
| `parse_analyze_withcb` | 186 | DEFERRED | parser-setup callback over the owned ParseState; follow-on. |
| `parse_sub_analyze` | 222 | PORTED + INSTALLED | child ParseState off parent, p_parent_cte/p_locked_from_parent/p_resolve_unknowns, transformStmt, returns `Node::Query` (C `(Node *) query`). Installed into `backend-parser-analyze-seams::parse_sub_analyze` (consumed by parse_cte/parse_clause). |
| `transformTopLevelStmt` | 249 | PORTED | transformOptionalSelectInto + stmt_location/stmt_len transfer. |
| `transformOptionalSelectInto` | 273 | PORTED | drill to leftmost leaf; SELECT INTO -> CreateTableAsStmt rewrite (deep-copy of the borrowed SELECT with the leftmost intoClause cleared, mirroring the C in-place edit). |
| `transformStmt` | 312 | PORTED | full nodeTag dispatch; SELECT three-way (VALUES/SELECT/set-op); DML/special arms seam-and-panic (follow-on family); default arm wraps CMD_UTILITY. QSRC_ORIGINAL/canSetTag stamping. `Debug_raw_expression_coverage_test` is a DEBUG_NODE_TESTS_ENABLED build switch (off). |
| `stmt_requires_parse_analysis` | 447 | PORTED | exact NodeTag list. |
| `analyze_requires_snapshot` | 491 | PORTED | delegates to stmt_requires_parse_analysis (C does the same). |
| `query_requires_rewrite_plan` | 520 | PORTED | CMD_UTILITY -> DeclareCursor/Explain/CreateTableAs/Call exact list. |
| `transformDeleteStmt` | 553 | FOLLOW-ON | DML family. |
| `transformInsertStmt` | 625 | FOLLOW-ON | DML family. |
| `transformInsertRow` | 1052 | FOLLOW-ON | DML family. |
| `transformOnConflictClause` | 1162 | FOLLOW-ON | DML family (transformOnConflictArbiter is already in parse_clause). |
| `BuildOnConflictExcludedTargetlist` | 1269 | FOLLOW-ON | DML family. |
| `count_rowexpr_columns` | 1339 | FOLLOW-ON | DML family (PLAssign helper). |
| `transformSelectStmt` | 1381 | PORTED | WITH/INTO-guard/locking+windowdefs-into-pstate/FROM/targetlist/markOrigins/WHERE/HAVING/ORDER/GROUP/DISTINCT(ON)/LIMIT/window/resolveUnknowns/jointree/flags/locking/collations/parseCheckAggregates. 1:1 ordering. |
| `transformValuesClause` | 1524 | PORTED (boundary panic) | full column-organized transform/coerce/typmod/collation/row-rebuild/LATERAL probe; reaches `addRangeTableEntryForValues`, which is a seam-and-panic in the parse_relation owner (no List-of-columns carrier for RTE.values_lists yet) — VALUES analyze panics at that boundary until the parse_relation VALUES-RTE follow-on lands. |
| `transformSetOperationStmt` | 1743 | PORTED | leftmost-INTO error, clause extraction, locking reject, WITH, recursion, dummy targetlist (makeVar/makeTargetEntry/makeString + ParseNamespaceColumn), join-RTE namespace, SQL92 ORDER-BY enforcement, LIMIT, setOperations/rtable/jointree/flags/collations/parseCheckAggregates. |
| `makeSortGroupClauseForSetOp` | 2000 | PORTED | get_sort_group_operators; RECORD/RECORDARRAY hash override. |
| `transformSetOperationTree` | 2048 | PORTED | leaf vs internal; parse_sub_analyze leaf; contain_vars_of_level(1) guard; non-junk TLE extraction; subquery RTE + RangeTblRef; internal node colType/typmod/collation merge via select_common_type/coerce_to_common_type/select_common_typmod/select_common_collation; UNKNOWN Const/Param coercion; SetToDefault dummy TLE. See "trimmed-model boundaries". |
| `determineRecursiveColTypes` | 2379 | PORTED | leftmost-leaf walk; dummy tlist from non-recursive term; analyzeCTETargetList. |
| `transformReturnStmt` | 2433 | FOLLOW-ON | DML family (RETURN). |
| `transformUpdateStmt` | 2464 | FOLLOW-ON | DML family. |
| `transformUpdateTargetList` | 2530 | FOLLOW-ON | DML family. |
| `addNSItemForReturning` | 2605 | FOLLOW-ON | DML family. |
| `transformReturningClause` | 2645 | FOLLOW-ON | DML family. |
| `transformPLAssignStmt` | 2767 | FOLLOW-ON | special family. |
| `transformDeclareCursorStmt` | 3017 | FOLLOW-ON | special family. |
| `transformExplainStmt` | 3110 | FOLLOW-ON | special family. |
| `transformCreateTableAsStmt` | 3162 | FOLLOW-ON | special family. |
| `transformCallStmt` | 3237 | FOLLOW-ON | special family. |
| `LCS_asString` | 3362 | PORTED | exact strings. |
| `CheckSelectLocking` | 3387 | PORTED | all 7 feature rejections. |
| `transformLockingClause` | 3451 | PORTED | all-rels and named-rels paths; RTE name-visibility rules (alias/join_using_alias/subquery/values skip); per-rtekind dispatch + the full error set; ACL_SELECT_FOR_UPDATE; subquery recursion via an "all rels" clause. Index-based rtable iteration to satisfy the borrow checker (subquery taken/restored). |
| `applyLockingClause` | 3678 | PORTED | hasForUpdate, pre-existing rowmark Max(strength)/Max(waitPolicy)/pushedDown &=, new RowMarkClause append. |
| `test_raw_expression_coverage` | 3737 | N/A | DEBUG_NODE_TESTS_ENABLED only; not compiled. |

## Trimmed-model boundaries (documented, behaviour-preserving)

- **bestexpr/bestlocation in set-ops**: the ported `select_common_type` returns
  only the common-type Oid (no `bestexpr` out-param). The C uses `bestexpr` only
  for the error-cursor location of the resulting `SetToDefault`; with the
  repo-wide trimmed location model `bestlocation == -1`. The chosen common type
  and all coercions are identical to the C.
- **leaf UNKNOWN-Const in-place replacement**: the C mutates `ltle->expr`/
  `rtle->expr` of the leaf query's stored targetlist in place. The owned model
  returns per-level copies, so the replacement updates the extracted dummy tlist;
  the leaf query's stored Const is re-resolved by the planner from `colTypes`
  (the colTypes/typmods/collations carry the resolved types). Behaviour-preserving.
- **SELECT DISTINCT (all) marker**: the C `linitial(distinctClause) == NULL`
  marker (`list_make1(NIL)`) is not yet carried by the raw->owned converter
  (it requires every list cell); detected via `distinct_all_marker` (a one-element
  list whose element is an empty `Node::List`). DISTINCT ON is fully supported now.

## Seam ownership

`backend-parser-analyze` owns `backend-parser-analyze-seams`. `init_seams()`
installs `parse_sub_analyze`. The other declarations in that crate
(`pg_analyze_and_rewrite_fixedparams`, `analyze_and_rewrite_varparams`,
`run_post_parse_analyze_hook`) are tcop/postgres.c + rewriter functions grouped
into the analyze-seams crate by earlier consumers; their true owners
(postgres.c / rewriteHandler.c) are unported, so they remain panic-until-owner
(matching the existing COPY/PREPARE consumers). `make_parsestate` is owned and
installed by parse_node.c (`backend-parser-small1`).

## Decomp plan for the follow-on family (INSERT/UPDATE/DELETE/MERGE/special)

- **F1 RETURNING + Insert/Delete/Update**: `transformReturningClause`,
  `addNSItemForReturning`, `transformInsertStmt`+`transformInsertRow`,
  `transformOnConflictClause`+`BuildOnConflictExcludedTargetlist`,
  `transformDeleteStmt`, `transformUpdateStmt`+`transformUpdateTargetList`.
  Deps already landed: setTargetTable (parse_clause F2), transformOnConflictArbiter
  (parse_clause F3a), parse_target. New need: BuildOnConflictExcludedTargetlist
  reads relation tupdesc (relcache) — verify availability.
- **F2 MERGE**: `transformMergeStmt` (small1 has a mirror-and-panic stub) — the
  MERGE-action machinery; depends on F1's RETURNING.
- **F3 special**: `transformReturnStmt`/`count_rowexpr_columns`/
  `transformPLAssignStmt` (PL/pgSQL), `transformDeclareCursorStmt`,
  `transformExplainStmt`, `transformCreateTableAsStmt`, `transformCallStmt`.
  These need the explain/createas/call utility-stmt models + DECLARE CURSOR
  option checks.

## Test

`select_const_where_order_builds_owned_query`: `SELECT 1 AS x, 2 AS y` ->
raw_parser -> transformTopLevelStmt -> owned `Query` with CMD_SELECT,
QSRC_ORIGINAL, canSetTag, two walkable target entries (resno 1/2, names x/y),
a jointree, empty rtable/sort/group/distinct, no aggs/set-ops, wraps to a
`Node::Query` (tag `T_Query`). Constant targets are used because operator/sort-op
resolution and FROM-relation opens require a live catalog/syscache the bare
unit-test process lacks; the parse-analysis assembly is fully exercised.
`utility_statement_wraps_in_cmd_utility`: `BEGIN` -> CMD_UTILITY Query;
stmt_requires_parse_analysis / query_requires_rewrite_plan both false.
