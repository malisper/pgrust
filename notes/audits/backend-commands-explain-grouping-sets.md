# Audit: backend-commands-explain — show_grouping_sets / show_grouping_set_keys

Scope: the grouping-set EXPLAIN detail port in `crates/backend-commands-explain/src/walk.rs`
(C: `src/backend/commands/explain.c`). Independent re-derivation against C 18.3.

## Function table

| C function (explain.c) | Port (walk.rs) | Verdict | Notes |
|---|---|---|---|
| show_agg_keys (:2477) | T_Agg arm (~958) | MATCH | `numCols>0 || groupingSets`; `lcons(plan, ancestors)`; branch groupingSets→show_grouping_sets else show_sort_group_keys; child plan = outerPlanState. |
| show_grouping_sets (:2509) | show_grouping_sets (:1809) | MATCH | OpenGroup "Grouping Sets"; top node then `foreach chain` with `sortnode=(Sort*)aggnode->plan.lefttree`; CloseGroup. set_deparse_context_plan folded into per-key deparse_expr_for_plan seam (same model as show_sort_group_keys). |
| show_grouping_set_keys (:2542) | show_grouping_set_keys (:1864) | MATCH | keyname/keysetname by AGG_HASHED/AGG_MIXED; OpenGroup "Grouping Set" (NULL label, labeled=true); optional Sort Key + indent++; OpenGroup keysetname; per-gset key list via get_tle_by_resno + deparse(useprefix,true); empty+TEXT→PropertyText "()" else PropertyListNested; CloseGroup; indent--; CloseGroup. |

## Constants verified
- "Hash Key"/"Hash Keys" for AGG_HASHED|AGG_MIXED; "Group Key"/"Group Keys" otherwise — matches C.
- Group/label strings "Grouping Sets", "Grouping Set", keyset names — exact.
- useprefix = (rtable_size>1 || verbose) — matches C.
- showImplicit=true on deparse — matches C.

## Edge cases
- grpColIdx NULL (numCols==0, all-empty gset `()`): keycols Optional, only `.expect`-indexed inside the per-key loop, which never runs for an empty set. Matches C (raw pointer never dereferenced).
- empty grouping set: TEXT → "()" via ExplainPropertyText; non-TEXT → ExplainPropertyListNested(NIL). Matches C.

## Notes / consistency
- `get_tle_by_resno` miss: C `elog(ERROR, "no tlist entry for key %d")`; port `panic!` — mirrors the pre-existing adjacent `show_sort_group_keys` exactly (crate-local convention for this internal-error elog). Not introduced by this change.

## Seams
No new seams. Reuses `ruleutils_s::deparse_expr_for_plan` (existing, justified cycle) and `backend-commands-explain-format` helpers. No logic in seam paths.

## Runtime verification
ROLLUP / CUBE / GROUPING SETS / mixed-with-ORDER-BY and VERBOSE (prefixed) EXPLAIN
output is byte-identical to real PG 18.3; session survives.

## Verdict: PASS
