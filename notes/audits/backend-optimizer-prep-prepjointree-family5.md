# Audit — backend-optimizer-prep-prepjointree FAMILY 5

Scope: the `remove_useless_result_rtes` family added in this change
(`src/result_rtes.rs`). FAMILY 4 (`reduce_outer_joins`, `src/lib.rs`) was audited
when #275 landed and is unchanged here. C source:
`../pgrust/postgres-18.3/src/backend/optimizer/prep/prepjointree.c`
(lines 3540–4452). c2rust rendering:
`../pgrust/c2rust-runs/backend-optimizer-prep-core/src/prepjointree.rs`.

## Function inventory & verdicts

| C function (prepjointree.c) | Port location (result_rtes.rs) | Verdict | Notes |
|---|---|---|---|
| `remove_useless_result_rtes` (3596) | `remove_useless_result_rtes` | MATCH | Recurse over top FromExpr (is_top guard preserves the FromExpr invariant); dropped-OJ `remove_nulling_relids` over parse + append_rel_list; rowMarks RTE_RESULT filter. |
| `remove_useless_results_recurse` (3669) | `remove_useless_results_recurse` + `_fromexpr` + `_joinexpr` | MATCH | RangeTblRef no-op; FromExpr drop+elide; JoinExpr per-jointype rules. Split by node kind for the owned-tree move semantics; behavior identical. |
| `get_result_relid` (3936) | `get_result_relid` | MATCH | RangeTblRef → rtindex iff RTE_RESULT, else 0. |
| `remove_result_refs` (3970) | `remove_result_refs_fromexpr` / `remove_result_refs_node` | MATCH | Two callers (newjtloc is a FromExpr value vs a jointree Node); both gate on lastPHId, compute `get_relids_in_jointree(newjtloc,true,false)`, assert non-empty, `substitute_phv_relids` + `fix_append_rel_relids`. |
| `find_dependent_phvs_walker` (4009) | `find_dependent_phvs_walker` | MATCH | PHV: phlevelsup==sublevels_up && phrels=={varno} → true; Query: bump sublevels, recurse; else expression_tree_walker. C asserts (SpecialJoinInfo/PlaceHolderInfo/MinMaxAggInfo) are debug-only, omitted. |
| `find_dependent_phvs` (4048) | `find_dependent_phvs` | MATCH | lastPHId==0 short-circuit; singleton {varno}; query_tree_walker(parse) ‖ append_rel_list walk (translated_vars arena Exprs). |
| `find_dependent_phvs_in_jointree` (4070) | `find_dependent_phvs_in_jointree_node` / `_fromexpr` | MATCH | walker over the jointree fragment, then per-LATERAL-RTE check over `get_relids_in_jointree(node,false,false)`. |
| `substitute_phv_relids_walker` (4117) | `substitute_phv_relids_walker` | MATCH | PHV in-place: phrels = (phrels ∪ subrelids) \ {varno}, asserts non-empty; Query bumps sublevels; else expression_tree_walker_mut. |
| `substitute_phv_relids` (4146) | `substitute_phv_relids_in_query` / `_in_node` | MATCH | query_or_expression_tree_walker split: Query→query_tree_mutator (level 0); bare expr (translated_var)→walker(node). |
| `fix_append_rel_relids` (4174) | `fix_append_rel_relids` | MATCH | parent_relid≠varno assert; child_relid==varno → bms_singleton_member(subrelids) (computed once); PHV fixup of translated_vars iff lastPHId≠0. |
| `get_relids_in_jointree` (4234) | `get_relids_in_jointree` / `_fromexpr` | MATCH | RangeTblRef singleton; FromExpr bms_join over fromlist; JoinExpr larg∪rarg + rtindex per include_inner/include_outer. |
| `get_nullingrels` (4361) | `get_nullingrels` | MATCH | rtlength + nullingrels[rtlength+1] (index 0 unused); recurse from jointree. |
| `get_nullingrels_recurse` (4385) | `get_nullingrels_recurse` / `_fromexpr` | MATCH | INNER pass upper to both; LEFT/SEMI/ANTI local=copy(upper)+rti to RHS only; FULL local to both; RIGHT local to LHS only. |

Not in FAMILY 5 (other prepjointree.c files / families): `reduce_outer_joins*`
(FAMILY 4, already audited), `pull_up_sublinks*`/`pull_up_subqueries*` (FAMILY
1/2, seam-and-panic), `get_relids_for_join`/`find_jointree_node_for_rel` (public
helpers used by deconstruct_jointree, not consumed by any landed unit — left for
a later family with their consumer).

## Model / divergence notes (all behavior-preserving)

- **fromlist in-place delete (verified faithful).** C uses
  `foreach_delete_current` with `list_length(f->fromlist) > 1` re-checked against
  the *shrinking* list and passes the *shrinking* `f` to
  `find_dependent_phvs_in_jointree`. The port mirrors this with an index walk
  that `remove(i)` + does-not-advance on a drop, checking `f.fromlist.len() > 1`
  and passing the current `f` each iteration. (An earlier draft that used the
  original length + deferred deletion was a divergence; fixed.)
- **quals are implicit-AND `Node::List`.** `list_concat(child, parent)` →
  `concat_quals` concatenates the element vectors child-first; NIL→empty.
- **subrelids bridge.** PHV phrels are lifetime-free `ExprRelids`; subrelids is
  the `'mcx` Bitmapset. `relids_to_expr_relids` converts the one direction used;
  `bms_singleton_member` reads the child relid as an integer.
- **translated_vars walk.** `AppendRelInfo.translated_vars` is `Vec<NodeId>` into
  `PlannerInfo.node_arena`; both the find and substitute passes resolve each
  arena Expr, run the walker, write back — the same pattern FAMILY 4 uses for
  `remove_nulling_relids` over the append_rel_list. No #274 keystone needed.
- **PHV phexpr descent.** The repo's `expression_tree_walker` does not recurse
  into `PlaceHolderVar.phexpr` (a pre-existing backend-nodes-core limitation,
  shared by the landed `pull_varnos_walker`). The PHV fall-through here matches
  that existing behavior; not a FAMILY-5 divergence.

## Seam-and-panic legs (sanctioned)

- **rowMarks RTE_RESULT filter.** `PlannerInfo.rowMarks` is `Vec<NodeId>` of
  opaque PlanRowMark handles with no arena store / `rti` accessor (PlanRowMarks
  come from `preprocess_rowmarks`, planmain.c, unported, which runs before this
  pass — so the list is empty on every reachable path). When non-empty the pass
  `panic!`s rather than silently skip required removals: PlanRowMark-carrier
  keystone first. (Loud panic on an unported neighbor's carrier; not absent
  logic.)

## Seam audit

- New inward seam `remove_useless_result_rtes` declared in
  `backend-optimizer-prep-prepjointree-seams`, installed by the crate's
  `init_seams()` (verified by the `seams-init` recurrence guard:
  `every_declared_seam_is_installed_by_its_owner` passes). Consumed by the
  (unported) `subquery_planner`.
- Outward seam call: `backend_rewrite_core::remove_nulling_relids(_in_query)` —
  real cyclic dependency (rewriteManip ⇄ prepjointree), thin delegate, no logic
  in the call path.
- `get_nullingrels`/`NullingrelInfo` re-exported from the crate root as the
  FAMILY-5 leaf API for the still-panicking FAMILY 2 (`pull_up_subqueries`).

## Verdict

**PASS** — every FAMILY-5 function MATCH; the single rowMarks leg is a sanctioned
seam-and-panic on an unported carrier (loud panic, no absent own-logic); zero
seam findings. Gate green (workspace check, no-todo-guard, seams-init,
workspace test — only the sanctioned `range_pair_*` flakes fail).
