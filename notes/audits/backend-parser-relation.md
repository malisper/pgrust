# Audit: backend-parser-relation (`parser/parse_relation.c`)

Verdict: **PASS** (with sanctioned mirror-PG-and-panic deferrals, each citing the C
line and the unported owner it is blocked on; no `todo!()`/`unimplemented!()`; no
own-logic stubs).

Gate (isolated `CARGO_TARGET_DIR`): `cargo check --workspace` green;
`cargo test -p no-todo-guard` green; `cargo test -p seams-init` green (both
recurrence guards); `cargo test --workspace` green except the pre-existing
allowed flake `backend-optimizer-path-small::range_pair_positive_combination`.

## Model

- `ParseState`/`RangeTblEntry`/`ParseNamespaceItem`/`RTEPermissionInfo` are the
  owned value types in `types-nodes` (mcx/PgVec/PgBox). The C `nsitem->p_rte`
  aliasing is modeled by `p_rtindex` (1-based into `p_rtable`) plus a boxed copy
  in `p_rte`; mutation of perminfo/rte goes by index back into the pstate's
  vectors. Scan/search helpers return indices (`Option<usize>` /
  `Option<(i32,usize)>`) instead of borrows to respect the borrow checker — the
  established sibling pattern (src-idiomatic `refnameNamespaceItemCoords`), not a
  divergence.
- `Var` gained `varnosyn`/`varattnosyn` (field-for-field vs primnodes.h; the
  keystone Expr expansion had left them trimmed). parse_relation is their first
  real producer (`scanNSItemForColumn`, `expandNSItemVars`). Additive: `Var`
  derives `Default`. One stale `Var { .. }` literal in
  `backend-optimizer-util-plancat` was updated to set the new fields (it had
  blocked `seams-init` independently).

## Inward seam (owned + installed)

`backend_parser_relation_seams::get_rte_permission_info(&[RTEPermissionInfo],
&RangeTblEntry) -> PgResult<usize>` (0-based index; C returns the pointer).
Installed in `init_seams()`, wired into `seams-init::init_all`.

## New outward seam decls added (owner installs later)

- `backend-optimizer-util-plancat-ext-seams::system_attribute_by_name(&str) ->
  PgResult<Option<i32>>` (consumed by `specialAttNum`; sibling of the existing
  `system_attribute_definition`).
- `backend-utils-cache-syscache-seams::search_attnum_attisdropped(relid, attnum)
  -> PgResult<Option<bool>>` (the `SearchSysCache2(ATTNUM,…)->attisdropped` read
  for `get_rte_attribute_is_dropped`).

## Function-by-function

Fully ported 1:1 (control flow, branch order, error text + SQLSTATE):
refnameNamespaceItem, scanNameSpaceForRefname/Relid/CTE/ENR, isFutureCTE,
searchRangeTableForRel, checkNameSpaceConflicts, check_lateral_ref_ok,
GetNSItemByRangeTablePosn, GetRTEByRangeTablePosn, GetCTEForRTE,
updateFuzzyAttrMatchState (+FuzzyAttrMatchState), scanNSItemForColumn,
scanRTEForColumn, colNameToVar, searchRangeTableForCol, markNullableIfNeeded,
markRTEForSelectPriv (incl. the whole-row JOIN larg/rarg recursion through
`Node::RangeTblRef`/`Node::JoinExpr`), markVarForSelectPriv, buildRelationAliases,
chooseScalarFunctionAlias (scalar/alias arms), buildNSItemFromTupleDesc,
buildNSItemFromLists, parserOpenTable, addRangeTableEntry,
addRangeTableEntryForRelation, addRangeTableEntryForSubquery,
addRangeTableEntryForCTE (incl. SEARCH/CYCLE extra columns),
addRangeTableEntryForENR, addRangeTableEntryForGroup, addRangeTableEntryForJoin,
isLockedRefname (empty-clause path), addNSItemToQuery, expandRTE (RELATION,
SUBQUERY, JOIN, TABLEFUNC/VALUES/CTE/NAMEDTUPLESTORE, RESULT/GROUP), expandRelation,
expandTupleDesc, expandNSItemVars, expandNSItemAttrs, get_rte_attribute_name,
get_rte_attribute_is_dropped (all arms except RTE_FUNCTION composite), get_tle_by_resno,
get_parse_rowmark, attnameAttNum, specialAttNum, attnumTypeId, attnumCollationId,
errorMissingRTE, errorMissingColumn, findNSItemForRTE, rte_visible_if_lateral,
rte_visible_if_qualified, isQueryUsingTempRelation(_walker), addRTEPermissionInfo,
getRTEPermissionInfo.

Mirror-PG-and-panic deferrals (blocked on a genuinely-absent dep; cited C line):
- addRangeTableEntryForFunction (1751) — funcapi per-function type resolution +
  tupdesc builders + parse_type (matches src-idiomatic's own deferral).
- expandRTE RTE_FUNCTION arm — get_expr_result_type + funcapi tupdesc expansion.
- get_rte_attribute_is_dropped RTE_FUNCTION composite arm — get_expr_result_tupdesc.
- chooseScalarFunctionAlias FuncExpr OUT-param branch — get_func_result_name.
- addRangeTableEntryForTableFunc (2065) — no `Node::TableFunc` central-enum arm.
- addRangeTableEntryForValues — no `Node::List` central-enum arm for a VALUES row.
- attnumAttName system-column branch (attid<=0) — system-attribute seam exposes
  type data only, not the attname table.
- isLockedRefname populated-locking-clause loop (2677) — `LockingClause` not a
  central `Node` enum arm (empty list path is correct).

Plus 4 internal-invariant panics mirroring C `Assert`/`elog`-unreachable paths.
Total panic sites: 11. No own-logic stubs.
