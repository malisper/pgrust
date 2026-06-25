# Audit — backend-parser-clause (parse_clause.c, F1 clause-core subset)

C source: `src/backend/parser/parse_clause.c` (PostgreSQL 18.3).
Scope: the expression-clause core only. FROM/JOIN (F2), window defs / on-conflict
(F3a), tablefunc / JSON_TABLE (F3b) are SEPARATE follow-on families and are NOT
in this crate (no stubs, not even seamed here).

Model: this repo splits the raw grammar tree (`types_nodes::nodes::Node<'mcx>`)
from the typed expression tree (`types_nodes::primnodes::Expr`). `TargetEntry.expr`
is a typed `Expr`; GROUP/ORDER/DISTINCT items arrive as raw `Node`. Lists are
owned `Vec`/`PgVec<NodePtr>`. Allocation threads `Mcx<'mcx>` (every allocating C
fn is fallible). `find*` return the targetlist *index* (the C returns a
`TargetEntry *` into a list we grow in place).

## Per-function audit (F1 subset)

| C function | line | status | notes |
|---|---|---|---|
| transformWhereClause | 1830 | PORTED | transformExpr + coerce_to_boolean (direct parse_coerce). NULL clause → None; NULL transform result → loud internal error (cannot happen for non-NULL input). |
| transformLimitClause | 1880 | PORTED | re-reads the raw `A_Const.isnull` before transform (C re-reads the original pointer); coerce_to_specific_type(INT8OID); checkExprIsVarFree; FETCH FIRST WITH TIES null check with exact errcode/text. |
| checkExprIsVarFree | 1924 | PORTED | contain_vars_of_level/locate_var_of_level (var.c, direct) over `Node::Expr(qual)`; exact errcode/msg, errposition via parser_errposition seam. |
| checkTargetlistEntrySQL92 | 1949 | PORTED | GROUP BY arm rejects aggs (parse_agg seam contain/locate_aggs_of_level, gated on p_hasAggs) and windowfuncs (rewriteManip seam contain/locate_windowfunc, gated on p_hasWindowFuncs); other exprKinds: no-op (C default break). |
| findTargetlistEntrySQL92 | 2050 | PORTED | bare single-field ColumnRef(String) path with GROUP BY colNameToVar(localonly) precedence; ambiguity via equal(prev.expr,cur.expr); A_Const integer ordinal path; non-integer & out-of-range errors; falls through to SQL99. |
| findTargetlistEntrySQL99 | 2171 | PORTED | transformExpr; match against strip_implicit_coercions(tle.expr) via equal; else transformTargetEntry seam (resjunk) appended; returns index. |
| flatten_grouping_sets | 2257 | PORTED | implicit-RowExpr recursion; GroupingSet flatten w/ toplevel-empty skip and SETS concat-vs-keep; `Flattened` enum captures the C 3 return shapes (NIL / single node / List). |
| transformGroupClauseExpr | 2366 | PORTED | dup elimination via seen_local (read here, mutated by caller, as in C); flat-list dedup; ORDER BY operator-info adoption (copy SortGroupClause, force nulls_first=false when nested); else addTargetToGroupList. |
| transformGroupClauseList | 2474 | PORTED | per-element transformGroupClauseExpr, seen_local accrual, integer-ref result list. Reached only from the grouping-set sublist arm. |
| transformGroupingSet | 2527 | PORTED | T_List sublist → transformGroupClauseList → SIMPLE set; nested GroupingSet recursion; scalar expr → SIMPLE set of one ref; CUBE>12 cap error. |
| transformGroupClause | 2631 | PORTED | top-level flatten; canonical GROUP BY () restore; EMPTY/SETS/CUBE/ROLLUP dispatch + SIMPLE top-level expr; returns (groupClause, groupingSets). |
| transformSortClause | 2731 | PORTED | per-SortBy find (SQL92/99) + addTargetToSortList. |
| transformDistinctClause | 2984 | PORTED | ORDER-BY prefix (reject resjunk) then remaining non-junk tlist; empty-result error; is_agg message variants. |
| transformDistinctOnClause | 3068 | PORTED | assign refs in DISTINCT ON order; ORDER BY prefix-match w/ skipped_sortitem guard + get_matching_location; remaining items via addTargetToGroupList. |
| get_matching_location | 3175 | PORTED | first sortgrouprefs member == ref → exprLocation of paired expr; else internal error. |
| addTargetToSortList | 3458 | PORTED | UNKNOWN→TEXT coerce_type; ASC/DESC/USING operator resolution (get_sort_group_operators / compatible_oper_opid + get_equality_op_for_ordering_op + op_hashjoinable); invalid-ordering-op error w/ hint; dup guard; NULLS default-by-direction. |
| addTargetToGroupList | 3536 | PORTED | UNKNOWN→TEXT coerce_type; dup guard; get_sort_group_operators(need_eq); default SortGroupClause. |
| assignSortGroupRef | 3593 | PORTED | reuse-or-(max+1); scan whole tlist. |
| targetIsInSortList | 3634 | PORTED | ref!=0 guard; sortop / commutator match (get_commutator seam); ignores nulls_first by design. |

## Seam / direct-call ledger

Direct merged-owner calls (cycle-free): `transformExpr` + `ParseExprKindName`
(parse_expr), `coerce_to_boolean`/`coerce_to_specific_type`/`coerce_type`
(parse_coerce), `compatible_oper_opid`/`get_sort_group_operators` (parse_oper),
`colNameToVar` (parse_relation), `contain_vars_of_level`/`locate_var_of_level`
(var.c), `make_grouping_set` + `expr_type`/`expr_location`/
`strip_implicit_coercions` (nodes-core).

Panic-until-owner-lands seams (mirror-PG-and-panic; owners not yet ported):
- `transform_target_entry` — `backend-parser-target-seams` (parse_target.c).
- `contain_aggs_of_level` / `locate_agg_of_level` —
  `backend-parser-parse-agg-seams` (parse_agg.c; in unported `backend-parser-medium2`).
- `contain_windowfuncs` / `locate_windowfunc` —
  `backend-rewrite-rewritemanip-seams` (rewriteManip.c; in unported `backend-rewrite-core`).
- `equal_expr` — `backend-nodes-equalfuncs-seams` (equalfuncs.c, owner not ported).

Installed seams used: `parser_errposition` (small1), `get_equality_op_for_ordering_op`
/ `op_hashjoinable` / `get_commutator` (lsyscache).

This crate owns NO inward seam (leaf consumer; callers in analyze.c are unported).
`init_seams()` is intentionally empty (functioncmds/dest precedent) and is not
wired into `init_all` (the wiring guard only requires crates with ≥1 `::set`).

## Behavioral notes / intentional divergences

1. `exprLocation((Node*)raw)`: the repo splits `exprLocation` so nodes-core only
   handles the typed `Expr` arms. The raw-grammar arms a clause item can be
   (ColumnRef/A_Const/A_Expr/FuncCall/TypeCast/A_ArrayExpr/CollateClause/SortBy/
   A_Indirection/RowExpr/GroupingSet/TypeName/List/value) are ported 1:1 here as
   `node_expr_location` + `leftmost_loc`, delegating to nodes-core for `Node::Expr`.
2. `setup/cancel_parser_errposition_callback(pstate, location)` around the
   operator lookups in addTargetToSortList/addTargetToGroupList: this repo models
   that callback as a documented no-op (small1; same as parse_oper/parse_type/
   coerce ports), so the `location` is not threaded into the lookup error
   position. Behaviour-preserving except for the error cursor on a missing-operator
   ereport. The operators themselves resolve identically.
3. The grouping-set parenthesized-sublist arm (`ROLLUP((a,b),c)`) is fully
   implemented (NOT a deferral): the implicit-RowExpr flattening builds a real
   `Node::List` cell and transformGroupingSet walks it via transformGroupClauseList
   — this repo's `Node::List(PgVec<NodePtr>)` is walkable, unlike the old
   src-idiomatic tag-only list that forced that deferral.
4. `copyObject(SortGroupClause)` → `*scl` (the struct is `Copy`/lifetime-free;
   a bitwise copy is the faithful deep copy of a flat scalar node).

## Gate

`cargo check --workspace` clean; `cargo test -p no-todo-guard` and
`-p seams-init` pass; 4 crate unit tests pass; `cargo test --workspace` green
except the sanctioned `range_pair_*` flake.
