# Audit: backend-parser-clause (F2 — FROM clause / JOIN)

Unit: `backend-parser-clause` (`src/from_clause.rs`) — C source
`src/backend/parser/parse_clause.c` (PostgreSQL 18.3), the FROM-clause / JOIN
subset (F2). F1 (clause core) was audited separately
(`audits/backend-parser-clause-f1.md`). Audited against the C, the c2rust
rendering, and the abandoned src-idiomatic
`backend-parser-parse-clause/from_item_join.rs` (logic reference only — its
central-types/seam model was not copied).

Owned model: raw `Node` in, typed `Expr` out, `Option = NULL`, `PgVec`/`Vec` =
`List *`. The C out-params `(*top_nsitem, *namespace)` are returned as the
namespace `Vec` whose LAST element is `*top_nsitem` (matching the C
`*namespace = lappend(my_namespace, nsitem); *top_nsitem = nsitem;`).

## Function inventory and verdicts

| C function | C loc | Port (from_clause.rs) | Verdict | Notes |
|---|---|---|---|---|
| transformFromClause | 112 | transformFromClause | MATCH | foreach item: transformFromClauseItem → checkNameSpaceConflicts(p_namespace, ns) → setNamespaceLateralState(ns, true, true) → append to p_joinlist + concat p_namespace; final setNamespaceLateralState(p_namespace, false, true). |
| setTargetTable | 178 | setTargetTable | MATCH | ENR-hides-table check (FEATURE_NOT_SUPPORTED); table_close old target; parserOpenTable(RowExclusiveLock); addRangeTableEntryForRelation; override perminfo->requiredPerms = requiredPerms (live entry in p_rteperminfos[perminfoindex-1] AND the nsitem snapshot); p_target_nsitem; if alsoSource addNSItemToQuery(true,true,true); returns p_rtindex. Open `Relation` is taken out of pstate during the RTE-builder borrow and moved back (it is `Drop`=table_close). |
| extractRemainingColumns | 252 | extractRemainingColumns | MATCH | prevcols = merged colnos (bms→Vec contains); per-name attnum++; non-dropped (`colname[0] != '\0'`) && !merged → push colno, colname, buildVarFromNSColumn, copy nscol; returns colcount. |
| transformJoinUsingClause | 306 | transformJoinUsingClause | MATCH | forboth(l,r): markVarForSelectPriv both; makeSimpleA_Expr(AEXPR_OP,"=",copy lvar, copy rvar,-1) over already-transformed Var leaves (wrapped Node::Expr(Var)); lappend AND args; 1 arg → that, else raw BoolExpr(AND_EXPR); transformExpr(EXPR_KIND_JOIN_USING); coerce_to_boolean("JOIN/USING"). |
| transformJoinOnClause | 365 | transformJoinOnClause | MATCH | setNamespaceLateralState(ns,false,true); save+swap p_namespace = ns (cloned); transformWhereClause(quals, EXPR_KIND_JOIN_ON, "JOIN/ON"); restore p_namespace. |
| transformTableEntry | 394 | transformTableEntry | MATCH | addRangeTableEntry(r, r->alias, r->inh, true). |
| transformRangeSubselect | 404 | transformRangeSubselect | MATCH | Assert p_expr_kind==NONE; set FROM_SUBSELECT; Assert !p_lateral_active; p_lateral_active=r->lateral; parse_sub_analyze(subquery, pstate, isLockedRefname(alias?), true) [SEAM, analyze unported]; restore lateral/expr_kind; non-SELECT elog; addRangeTableEntryForSubquery(query, alias, lateral, true). |
| transformRangeFunction | 462 | transformRangeFunction | MATCH | p_lateral_active=true; per (fexpr,coldeflist) pair: UNNEST(>1 arg, no decoration, no coldeflist) splits into per-arg SystemFuncName("unnest") calls (transformExpr FROM_FUNCTION, SRF-top-level check, FigureColname [SEAM]); else transformExpr + SRF check + FigureColname + multiple-coldeflist error; assign_list_collations(funcexprs); top-level coldeflist installs (multi-func / WITH ORDINALITY errors); is_lateral = lateral || contain_vars_of_level(funcexprs,0); addRangeTableEntryForFunction [PANICS: funcapi/parse_type owners unported — mirror-PG-and-panic]. |
| transformRangeTableSample | 685 | transformRangeTableSample | MATCH | funcargtypes=[INTERNALOID]; LookupFuncName(method,1,...,missing_ok=true) [SEAM, parse_func unported]; !OidIsValid → UNDEFINED_OBJECT "does not exist"; get_func_rettype != TSM_HANDLEROID → WRONG_OBJECT_TYPE [lsyscache, installed]; GetTsmRoutine(oid) [SEAM, tablesample registry unported]; arg-count check (errmsg_plural 1/n); forboth args/parameterTypes: transformExpr+coerce_to_specific_type(argtype,"TABLESAMPLE")+assign_expr_collations; REPEATABLE: !repeatable_across_queries err, transformExpr+coerce(FLOAT8,"REPEATABLE")+assign collations. |
| getNSItemForSpecialRelationTypes | 1010 | getNSItemForSpecialRelationTypes | MATCH | schemaname → None; scanNameSpaceForCTE → addRangeTableEntryForCTE(cte,levelsup,rv,true); else scanNameSpaceForENR → addRangeTableEntryForENR(rv,true); else None. |
| transformFromClauseItem | 1053 | transformFromClauseItem + transform_from_clause_item_join | MATCH | RangeVar (getNSItem || transformTableEntry, RangeTblRef); RangeSubselect; RangeFunction; RangeTableSample (recurse, RTE_RELATION + relkind r/m/p check else FEATURE_NOT_SUPPORTED, attach tablesample to live RTE); JoinExpr (see below). RangeTableFunc/JsonTable arm (C:996) has no reachable Node variant in this repo (deferred F3b) — documented, no panic needed. |
| (JoinExpr arm) | 1158 | transform_from_clause_item_join | MATCH | recurse larg then rarg; setNamespaceLateralState(l_ns,true,lateral_ok=INNER\|LEFT); push l_ns, recurse rarg, truncate; checkNameSpaceConflicts(l,r); my_namespace=l++r; natural-join builds usingClause from common colnames; join_using_alias->colnames=usingClause; USING loop (dup/ambiguous/undefined-column errors, l/r colnos, l/r usingvars, res_colnames) → transformJoinUsingClause; else ON → transformJoinOnClause; else CROSS; j->rtindex = len(p_rtable)+1; markRelsAsNulledBy per jointype; merged-join-var loop (buildMergedJoinVar + res_nscolumns L/R/New); extractRemainingColumns both sides; alias hides inputs (varnosyn/varattnosyn); addRangeTableEntryForJoin; p_joinexprs lazy pad + push; join_using_alias jnsitem (+conflict check); alias→empty ns else setNamespaceColumnVisibility(false); nsitem visibility; lappend nsitem. |
| buildVarFromNSColumn | 1639 | buildVarFromNSColumn | MATCH | Assert p_varno>0; makeVar(varno,attno,type,typmod,collid,0); set varreturningtype/varnosyn/varattnosyn by hand; markNullableIfNeeded. |
| buildMergedJoinVar | 1666 | buildMergedJoinVar | MATCH | select_common_type/typmod("JOIN/USING"); per side: vartype≠out → coerce_type(IMPLICIT,IMPLICIT_CAST); vartypmod≠out → makeRelabelType(InvalidOid coll, IMPLICIT_CAST); else the Var (l_is_var/r_is_var carry C's pointer identity); JOIN_INNER prefer-uncoerced, LEFT=left, RIGHT=right, FULL=CoalesceExpr; assign_expr_collations(res). Returns (Node, MergedWhich) — the L/R/New marker replaces C's `u_colvar==(Node*)l_colvar` pointer tests. |
| markRelsAsNulledBy | 1774 | markRelsAsNulledBy | MATCH | RangeTblRef→rtindex; JoinExpr→recurse larg/rarg, varno=rtindex; else elog; lazy-pad p_nullingrels with empty Bitmapset (C NULL cell), lfirst = bms_add_member(cell, jindex). |
| setNamespaceColumnVisibility | 1815 | setNamespaceColumnVisibility | MATCH | per nsitem p_cols_visible = cols_visible. |
| setNamespaceLateralState | 1832 | setNamespaceLateralState | MATCH | per nsitem p_lateral_only/p_lateral_ok. |

## Deferred / not in F2 (out of scope, no panic emitted by F2 code)

* `transformRangeTableFunc` (XMLTABLE, C:574) and `transformJsonTable` — F3b.
  The dispatcher arms are unreachable (no `Node::RangeTableFunc`/`Node::JsonTable`
  variant in the repo's central enum), so the grammar cannot produce them; the
  arm is documented in `transformFromClauseItem` with no stub.
* `transformWindowDefinitions` / on-conflict — F3a.

## Seam-and-panic dependencies (owner unported)

| Seam | Declared in | Owner | State |
|---|---|---|---|
| `parse_sub_analyze` | backend-parser-analyze-seams (added) | analyze.c | unported → loud panic |
| `lookup_func_name` | backend-parser-parse-func-seams | parse_func.c | unported → loud panic / InvalidOid contract |
| `get_tsm_routine_oid` | backend-executor-nodeSamplescan-seams (re-keyed OID-only; shared with ExecInitSampleScan) | access/tablesample/tablesample.c registry | unported → loud panic |
| `addRangeTableEntryForFunction` | backend-parser-relation (direct call) | funcapi/parse_type | panics in owner (mirror-PG-and-panic) |

## Installed seams used (owner complete)

* `FigureColname` (backend-parser-target-seams) — **added** and installed by
  parse_target's `init_seams()` (delegates to the owner's `FigureColname`).
* `get_func_rettype` (lsyscache-seams) — installed by lsyscache.
* `transformExpr`, `coerce_*`, `select_common_*`, `assign_*_collations`,
  `markVarForSelectPriv`, `markNullableIfNeeded`, `checkNameSpaceConflicts`,
  `addRangeTableEntry*`, `scanNameSpaceFor*`, `parserOpenTable`, `table_close`,
  `bms_add_member`, `makeVar`/`makeRelabelType`/`makeFuncCall`/`makeA_Expr` —
  direct cycle-free calls into merged owners.

## Modeling note (divergence removed, not introduced)

`RangeTblEntry.tablesample` is `Option<NodePtr>` but the central `Node` enum had
no carrier for the post-analysis `TableSampleClause`. Rather than panic at the
store site, a faithful `Node::TableSampleClause(TableSampleClause)` variant was
added to `types-nodes::nodes` (node_tag + clone_in arms), letting the TABLESAMPLE
arm store the clause exactly as the C does (`rte->tablesample = ...`).

Verdict: **PASS** for the F2 subset.
