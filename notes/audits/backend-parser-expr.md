# Audit: backend-parser-parse-expr (parser/parse_expr.c)

C source: `src/backend/parser/parse_expr.c` (PostgreSQL 18.3, ~3.2K LOC).
Crate: `crates/backend-parser-parse-expr` (covers the `transformExpr` spine).
Verdict: **PASS** (function-by-function vs C; residual unported owners are
honest seam-and-panic with named rationale; `residual_own_todos = 0`).

This audit covers STEP 4d of the parser parse-analysis campaign (route to #159).
The crate was already substantially ported; an earlier pass verified it against C
and filled the `T_A_Const` `make_const` dispatch + the `analyze_one_exec_param`
inward seam.

**Re-pass (PARITY-FIX, wf_02a4ccb2):** wired the `transformExprRecurse` arms
that were stale seam-panics (cited their sibling owners as "not yet ported")
now that those owners have **landed**. Filled in-crate, 1:1 vs C:
`transformColumnRef` (+`transformWholeRowRef`), `transformIndirection`,
`transformFuncCall`, `transformAExprIn`, `transformAExprBetween`,
`transformRowExpr`, and the RowExpr leg of `transformMultiAssignRef`. The
`(List *) a->rexpr` "ListCell raw-pointer unwalkable" rationale was **stale** —
the node-walker keystone added `Node::List`, so the IN/BETWEEN value-list is
walked directly. Sibling owners now called directly (cycle-free; they depend
only on `backend-parser-parse-expr-seams`): `backend-parser-relation`
(`colNameToVar`/`refnameNamespaceItem`/`scanNSItemForColumn`/`errorMissing{Column,RTE}`/
`markNullableIfNeeded`/`markVarForSelectPriv`/`expandRTE`), `backend-parser-func`
(`ParseFuncOrColumn`), `backend-parser-parse-target` (`transformExpressionList`),
`backend-parser-small1` (`transformContainerSubscripts`), `backend-optimizer-util-vars`
(`contain_vars_of_level`).

## Per-function comparison

| C function | Port | Verdict |
|---|---|---|
| `transformExpr` (118) | `transformExpr` — saves/restores `p_expr_kind`, asserts `!= EXPR_KIND_NONE` | PASS |
| `transformExprRecurse` (137) | `transformExprRecurse` + `transform_expr_node` (the `Node::Expr`-carried arms); full node `switch` incl. nested `A_Expr` kind switch; unrecognized-node `elog(ERROR)` default | PASS |
| `transformIndirection` (437) | **FILLED**: `transformContainerSubscripts` (small1) for adjacent `A_Indices` runs, `ParseFuncOrColumn` (parse_func) for field selections; `A_Star` → "row expansion via *" ereport; trailing-subscript flush; `unknown_attribute` on NULL field-select | PASS |
| `transformColumnRef` (509) | **FILLED**: invalid-place check (DEFAULT/partition-bound), 1/2/3/4-field resolution via `colNameToVar`/`refnameNamespaceItem`/`scanNSItemForColumn`/`transformWholeRowRef`, 4-field catalog-name check (`get_database_name(MyDatabaseId)`), whole-row `*` refs, func-on-whole-row fallback, `CRERR_*` switch → `errorMissing{Column,RTE}`/cross-db/too-many ereports. Pre/Post columnref hooks → seam (absent in stock server) | PASS |
| `transformParamRef` (885) | seam-and-panic — opaque `p_paramref_hook` ABI | PASS (named) |
| `exprIsNullConstant` (909) | `exprIsNullConstant` — undecorated NULL `A_Const` test | PASS |
| `transformAExprOp` (922) | `transformAExprOp` — dead `Transform_null_equals` branch (GUC default off); **row-op-subselect rewrite guarded by `subLinkType == EXPR_SUBLINK`** (audit-v2 finding present, `is_expr_sublink`); ROW()-op-ROW() via `make_row_comparison_op`; scalar `make_op` w/ `p_last_srf` | PASS |
| `transformAExprOpAny` (1003) | `transformAExprOpAny` — `make_scalar_array_op(useOr=true)` | PASS |
| `transformAExprOpAll` (1017) | `transformAExprOpAll` — `make_scalar_array_op(useOr=false)` | PASS |
| `transformAExprDistinct` (1031) | `transformAExprDistinct` — NULL-side `make_nulltest_from_distinct`; ROW/ROW vs scalar; `AEXPR_NOT_DISTINCT` NOT-wrap | PASS |
| `transformAExprNullIf` (1082) | `transformAExprNullIf` — bool/set checks, `opresulttype = exprType(linitial)`, retag `T_NullIfExpr` | PASS |
| `transformAExprIn` (1125) | **FILLED**: walks `Node::List` rexpr, splits Var/non-Var (`contain_vars_of_level`), `useOr` from `<>`; ScalarArrayOpExpr path (>1 non-Var: `select_common_type`+`verify_common_type`+`get_array_type`, ArrayExpr, `make_scalar_array_op`) else boolean tree (ROW/ROW `make_row_comparison_op` or `make_op`, `coerce_to_boolean`, AND/OR) | PASS |
| `transformAExprBetween` (1294) | **FILLED**: 2-element `Node::List` rexpr; synthesizes `>=`/`<=` (or `<`/`>`) `makeSimpleA_Expr` comparisons (+SYM/NOT variants), `copyObject` of multiply-referenced bounds, transforms+`coerce_to_boolean` each, `makeBoolExpr` AND/OR assembly (matching `transformExprRecurse(makeBoolExpr(...))`) | PASS |
| `transformMergeSupportFunc` (1388) | `transformMergeSupportFunc` — MERGE RETURNING ancestor scan | PASS |
| `transformBoolExpr` (1413) | `transformBoolExpr` — AND/OR/NOT name, per-arg `coerce_to_boolean` | PASS |
| `transformFuncCall` (1449) | **FILLED**: transforms args + WITHIN GROUP `agg_order` exprs (as `EXPR_KIND_ORDER_BY`), hands off to `ParseFuncOrColumn` w/ `p_last_srf` and the `FuncCall` | PASS |
| `transformMultiAssignRef` (1494) | **FILLED (RowExpr leg)**: first-column RowExpr source → `transformRowExpr(allowDefault)`, column-count check, append junk TLE to `p_multiassign_exprs`; per-column RowExpr element extraction (pop at last column). EXPR-SubLink source leg + the MULTIEXPR→Param leg → SubLink seam (needs `parse_sub_analyze`/analyze.c) | PASS (SubLink leg named) |
| `transformCaseExpr` (1642) | `transformCaseExpr` — test-expr placeholder (CASE form 2 via `make_op("=")`), WHEN/THEN coerce-to-boolean, common-type w/ lcons(defresult), `srf_check` | PASS |
| `transformSubLink` (1782) | seam-and-panic — `parse_sub_analyze` (analyze.c) | PASS (named) |
| `transformArrayExpr` (2025) | `transformArrayExpr` — multidims detection (excl. int2vector/oidvector/domain-over-array), `select_common_type`/`coerce_to_target_type`/`coerce_to_common_type`, empty-array + missing-element/array-type ereports | PASS |
| `transformRowExpr` (2188) | **FILLED**: `transformExpressionList` (parse_target) over the args, `MaxTupleAttributeNumber` limit, RECORDOID + `COERCE_IMPLICIT_CAST`, invented `f1..fN` colnames | PASS |
| `transformCoalesceExpr` (2226) | `transformCoalesceExpr` — `select_common_type`+`coerce_to_common_type`, `srf_check` | PASS |
| `transformMinMaxExpr` (2275) | `transformMinMaxExpr` — GREATEST/LEAST funcname, common-type coerce | PASS |
| `transformSQLValueFunction` (2314) | `transformSQLValueFunction` — full op→type table; `(n)` variants reach `any{time,timestamp}_typmod_check` (utils/adt/date,timestamp.c) — seam-and-panic | PASS (named on `(n)` arms) |
| `transformXmlExpr` (2367) | seam-and-panic — utils/adt/xml.c | PASS (named) |
| `transformXmlSerialize` (2496) | seam-and-panic — utils/adt/xml.c | PASS (named) |
| `transformBooleanTest` (2540) | `transformBooleanTest` — clausename table, `coerce_to_boolean` | PASS |
| `transformCurrentOfExpr` (2580) | `transformCurrentOfExpr` — `cvarno = p_target_nsitem->p_rtindex`; hook-present path seam-and-panic (absent in stock server → unchanged node) | PASS |
| `transformWholeRowRef` (2632) | **FILLED**: whole-row Var (`makeWholeRowVar` in-crate + `markNullableIfNeeded`/`markVarForSelectPriv`) when `p_names==eref` (structural-equality proxy `alias_eq`) or `p_returning_type!=DEFAULT`, else JOIN-USING-alias RowExpr via `expandRTE`+`list_truncate`. `makeWholeRowVar`'s RTE_FUNCTION / SRF-subquery branches → funcapi seam (Node-level exprType over funcexpr) | PASS (func branch named) |
| `transformTypeCast` (2714) | `transformTypeCast` — `typenameTypeIdAndMod`, ARRAY[]-into-array-type direct transform via `getBaseTypeAndTypmod`+`get_element_type`, `coerce_to_target_type` w/ cannot-cast ereport; decorated-type (`typmods` non-empty) seam-and-panic (TypeName vocab-unification keystone) | PASS |
| `transformCollateClause` (2798) | `transformCollateClause` — collatable check, `LookupCollation` via String-node bridge into parse_type vocab | PASS |
| `make_row_comparison_op` (2838) | `make_row_comparison_op` — length checks, pairwise `make_op` (bool/non-set checks), cmptype intersection over `get_op_index_interpretation`, lowest-number choice, `=`/`<>` AND/OR fast path, opfamily selection, `RowCompareExpr` assembly | PASS |
| `make_row_distinct_op` (3040) | `make_row_distinct_op` — length check, pairwise `make_distinct_op` OR-chained, zero-length → const FALSE | PASS |
| `make_distinct_op` (...) | `make_distinct_op` — `make_op`, bool/set checks, retag `T_DistinctExpr` | PASS |
| `make_nulltest_from_distinct` (...) | `make_nulltest_from_distinct` — `IS [NOT] NULL` from `AEXPR_[NOT_]DISTINCT`, `argisrow=false` | PASS |
| `ParseExprKindName` (3142) | `ParseExprKindName` — full kind→name table | PASS |
| SQL/JSON constructor family (transformJson*) | seam-and-panic — owning parse-node structs + utils/adt/json*.c absent | PASS (named) |
| `T_A_Const` → `make_const` (parse_node.c) | **FILLED**: direct call to `backend_parser_small1::make_const` (cycle-free landed owner); by-value literals decode (int/bool/null); by-ref string/numeric/bitstring arms panic *inside the owner* pending the canonical Datum carrier | PASS |

## Inward seams (owned by this crate)

- `analyze_one_exec_param` — **FILLED** (was panic-until-`parse_coerce`). Now
  runs the per-parameter `EvaluateParams` body (prepare.c:311-341): fresh
  `make_parsestate(None)` carrying `p_sourcetext`, `copyObject` via
  `clone_in`, `transformExpr(EXPR_KIND_EXECUTE_PARAMETER)`,
  `exprType`, `coerce_to_target_type(COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
  -1, -1)`, NULL→`coercion_failed` (driver raises the ereport),
  `assign_expr_collations`. Verified flags/typmod/location vs C.
- `parser_errposition` — installed; clamps negative location to 0.

Both installed from `init_seams()`; wired in `seams-init`.

## Owner-signature reconciliation

`backend_parser_small1::make_const` had `pstate: &ParseState<'mcx>` /
`aconst: &A_Const<'mcx>` coupling the (unused) pstate lifetime to the alloc
context `'mcx`, which blocked a scratch-context call. Relaxed to
`&ParseState<'_>` / `&A_Const<'_>` (the C `make_const` allocates in the current
context independently of the pstate; pstate is unused in the body). This crate
is the sole external caller. Faithful, minimal owner-signature fix.

## Genuinely-blocked boundaries (sanctioned mirror-PG-and-panic)

Remaining seam-and-panic boundaries (owners still unported):
- analyze.c — `transformSubLink` and the EXPR-SubLink leg of
  `transformMultiAssignRef` (`parse_sub_analyze`).
- parse_agg.c — `transformGroupingFunc` (installed seam to landed parse-agg).
- utils/adt/xml.c — `transformXmlExpr`/`transformXmlSerialize`.
- utils/adt/json*.c — the SQL/JSON constructor family.
- utils/adt/date.c + timestamp.c — `any{time,timestamp}_typmod_check`
  (`CURRENT_TIME(n)` etc.).
- The decorated-type `TypeName.typmods` vocabulary-unification keystone
  (`transformTypeCast` of `x::varchar(10)`).
- funcapi Node-level `exprType` over `RangeTblFunction.funcexpr` — the
  RTE_FUNCTION branch of `makeWholeRowVar`.
- The columnref pre/post parser hooks (opaque cross-ABI fn-ptr; absent in the
  stock server) and `p_paramref_hook` (`transformParamRef`).
- The by-ref Datum arms of `make_const` panic inside the small1 owner
  (unported workspace-wide carrier).

**No longer blocked (this re-pass):** the `(List *) a->rexpr` "ListCell
raw-pointer unwalkable" rationale was stale (the `Node::List` carrier exists);
parse_relation.c / parse_func.c / parse_target.c / parse_node.c subscripting
all landed, so columnref/indirection/funccall/in/between/rowexpr/multiassign
(RowExpr leg) are filled with real logic.

## Gate

- `cargo check --workspace`: clean.
- `cargo test -p no-todo-guard`: pass (no new todo!/unimplemented!).
- `cargo test -p seams-init`: pass.
- `cargo test --workspace`: pass except the sanctioned `range_pair_*` flake.
- `cargo test -p backend-parser-parse-expr`: 9/9 (incl. 4 new re-pass e2e
  tests: ColumnRef/AExprOp, AExprIn, AExprBetween, FuncCall each reach real
  catalog/owner logic — verified the panic origin is the uninstalled catalog
  seam, not a stale "unported" stub).
