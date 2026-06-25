# Audit: backend-optimizer-util-vars — flatten_join_alias_vars (+ prepjointree get_relids_for_join)

Scope: the new `src/flatten.rs` in `backend-optimizer-util-vars` and the two new
functions in `backend-optimizer-prep-prepjointree` (`result_rtes.rs`). The rest
of util-vars (var.c read-only family, tlist.c, fix_indexqual) was audited
previously (CATALOG row `merged`); not re-audited here.

C sources: `optimizer/util/var.c` (789-1382), `optimizer/prep/prepjointree.c`
(4160-4197).

## Function table

| C function | C loc | Port | Verdict | Notes |
|---|---|---|---|---|
| `flatten_join_alias_vars` | var.c:789 | flatten.rs `flatten_join_alias_vars` | MATCH | root is always NULL at this seam (signature omits it). Context init (sublevels_up=0, possible_sublink/inserted_sublink = query->hasSubLinks) field-for-field. Top-Query handled by the mutator (increments sublevels_up). |
| `flatten_join_alias_vars_mutator` | var.c:812 | flatten.rs `flatten_join_alias_vars_mutator` | MATCH | Var branch: varlevelsup!=sublevels_up early-return; RTE_JOIN gate; whole-row (varattno==0) RowExpr expansion; single-attr list_nth expansion; IncrementVarSublevelsUp when sublevels_up!=0; location preservation on Var; recursion; sublink detection (possible_sublink && !inserted_sublink) on single-attr only; add_nullingrels_if_needed. PHV branch: recurse into phexpr (done explicitly — the repo walker does not descend into PHV phexpr), then alias_relid_set on phrels iff phlevelsup==sublevels_up. Query branch: QTW_IGNORE_JOINALIASES, sublevels_up/inserted_sublink save-restore, hasSubLinks |= inserted_sublink. Default: expression_tree_walker_mut recursion. The C `Assert(!IsA(SubPlan/...))` planner-aux nodes have no central Node arm, so they cannot appear — faithful. |
| `add_nullingrels_if_needed` | var.c:1176 | flatten.rs `add_nullingrels_if_needed` | MATCH (root=NULL specialization) | varnullingrels empty -> no-op; is_standard -> adjust in place; else (root always NULL here) -> `elog(ERROR, "unsupported join alias expression")` (the C final `else` arm). The `else if (root)` PlaceHolderVar fallback is unreachable when root is NULL and is correctly omitted. |
| `is_standard_join_alias_expression` | var.c:1228 | flatten.rs `is_standard_join_alias_expression`/`is_standard_expr` | MATCH | Var(levelsup==), PHV(phlevelsup==), FuncExpr(COERCE_IMPLICIT_CAST && args!=NIL, recurse arg[0]), RelabelType/CoerceViaIO/ArrayCoerceExpr (recurse arg), CoalesceExpr (all args), else false. |
| `adjust_standard_join_alias_expression` | var.c:1302 | flatten.rs `adjust_standard_join_alias_expression`/`adjust_standard_expr` | MATCH | Var/PHV at matching level -> bms_add_members into varnullingrels/phnullingrels; coercion nodes recurse arg; Coalesce recurse all; else C `Assert(false)` -> `debug_assert!(false)`. |
| `alias_relid_set` | var.c:1364 | flatten.rs `alias_relid_set` | MATCH | bms_next_member loop (rtindex=-1 init); RTE_JOIN -> bms_join(get_relids_for_join) via seam; else bms_add_member. Result is ExprRelids words. |
| `mark_nullable_by_grouping` | var.c:1106 | — | NOT IN SCOPE | Belongs to `flatten_group_exprs` (a different entry, root never NULL); not part of this seam. Documented as a remaining gap in lib.rs. |
| `flatten_group_exprs(_mutator)` | var.c:974/996 | — | NOT IN SCOPE | Same — separate group-expr entry, no consumer in repo. |
| `find_jointree_node_for_rel` | prepjointree.c:4160 | result_rtes.rs `find_jointree_node_for_rel` | MATCH | RangeTblRef(rtindex==relid), FromExpr(iterate fromlist), JoinExpr(rtindex==relid, then larg/rarg), else `elog(ERROR, "unrecognized node type")`. |
| `get_relids_for_join` | prepjointree.c:4191 | result_rtes.rs `get_relids_for_join` | MATCH | find_jointree_node_for_rel over `(Node*)query->jointree` (top FromExpr arm inlined since the top jointree is a typed FromExpr), `elog(ERROR, "could not find join node %d")` on miss, then get_relids_in_jointree(jtnode, true, false). Returns ExprRelids (Bitmapset->words conversion). |

## Constants / edge cases verified

- `RTE_JOIN` = the model `RTEKind::RTE_JOIN` (= 2).
- `COERCE_IMPLICIT_CAST` matched via the `CoercionForm` enum.
- whole-row `varattno == InvalidAttrNumber` (0); single-attr `varattno > 0` uses
  `joinaliasvars[varattno-1]`.
- Dropped column: C NULL `joinaliasvars` element; owned model uses a null `Const`
  placeholder (per parse_relation.c `get_rte_attribute_is_dropped`). The
  whole-row loop skips it (continue) and so omits both the field AND the
  colname, matching the C `forboth` `if (newvar == NULL) continue;` (the colname
  lappend follows the continue).
- RowExpr: row_typeid = var.vartype, row_format = COERCE_IMPLICIT_CAST, location
  = var.location, colnames built from rte->eref->colnames (String sval) — parallel
  index with joinaliasvars (asserted equal length, debug_assert here).
- next_member / union / add_member / is_empty are the audited rewrite-core
  ExprRelids algebra (1:1 with bitmapset.c).

## Seam audit

- New owned seam: `flatten_join_alias_vars` (declared in
  `backend-rewrite-rewritemanip-seams`, its real C home is var.c). Installed by
  `backend_optimizer_util_vars::init_seams()` (single `set()`, no other install
  site; util-vars init_seams is wired into `seams-init::init_all`). Per the
  rewrite-core docstring this seam was explicitly reserved for the var.c owner.
- New outward seam consumed: `get_relids_for_join` (declared in
  `backend-optimizer-prep-prepjointree-seams`, installed by
  `backend_optimizer_prep_prepjointree::init_seams`). Justified by a real cycle:
  prepjointree depends on util-vars, so util-vars cannot depend on prepjointree;
  reaching prepjointree.c's `get_relids_for_join` requires the seam. The seam
  path is thin: marshal (query, joinrelid) -> one call -> Bitmapset->ExprRelids.
  No branching/computation in the seam declaration itself; all logic lives in
  the prepjointree owner.
- Allocating functions take `Mcx` + return `PgResult`. No shared statics, no
  ambient globals, no locks across `?`, no registries, no divergence markers.

## Verdict: PASS

Every in-scope function MATCH (or correctly NOT-IN-SCOPE with a documented gap).
Zero seam findings. No design-conformance violations. `flatten_group_exprs` /
`mark_nullable_by_grouping` remain genuine gaps (separate entry, no consumer),
documented in lib.rs — not part of the `flatten_join_alias_vars` seam.
