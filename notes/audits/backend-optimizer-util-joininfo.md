# Audit — backend-optimizer-util-joininfo

Unit: `optimizer/util/restrictinfo.c`, `optimizer/util/joininfo.c`,
`optimizer/util/placeholder.c`, `optimizer/plan/orclauses.c` (the prompt listed
`plan/orclauses.c`; the file is at `util/orclauses.c` — confirmed via the source
tree; same TU).

Model: arena+handle over `types_pathnodes::PlannerInfo`. `RestrictInfo *` →
`RinfoId`, `RelOptInfo *` → `RelId`, `PlaceHolderInfo *` → `PhInfoId`,
`PlaceHolderVar *`/clause `Expr *` → `NodeId` interned in `node_arena`. `Relids`
set algebra via relnode-/pathnode-seams (`bms`/`bms_path`). Verdict: **PASS** —
every function ported 1:1; divergences below were caught and fixed; genuinely
unreachable model arms are seam-and-panic / documented, never stubbed.

## Keystone change (substrate)

To carry a `RestrictInfo` embedded in an `orclause` expression tree (C casts
`RestrictInfo *` to `Expr *` inside the OR/AND args built by
`make_sub_restrictinfos`), added:
- `types_nodes::primnodes::RinfoRef(pub u32)` handle + `Expr::RestrictInfo(RinfoRef)`
  variant (+ `T_RestrictInfo=318` tag arm in `expr_tag`), mirroring the existing
  `SlotId`/`EcxtId`/`ResultCellId` handle-in-`Expr` precedent. Verified `318`
  against `nodes/nodetags.h`.
- `RinfoId::as_expr_ref()` + `From<RinfoRef> for RinfoId` in types-pathnodes.
- `PlaceHolderInfo.ph_var: PlaceHolderVar` (full node; placeholder.c copies the
  whole PHV into rel targetlists and reads `phnullingrels`). Additive; the
  existing `ph_var_phexpr`/`ph_var_phrels` consumer mirrors kept and populated.
- `PlannerInfo::phinfo_mut` / `alloc_phinfo` accessors.

## restrictinfo.c

- **make_restrictinfo** — 1:1. OR → make_sub_restrictinfos; `Assert(!is_andclause)`
  → debug_assert; else make_plain_restrictinfo(orclause=None).
- **make_plain_restrictinfo** — 1:1. leakproof gate (`security_level>0` →
  `!contain_leaked_vars`, else false); binary-opclause left/right relids +
  can_join syntactic test (both sides nonempty + non-overlapping; `Assert
  !pseudoconstant`); else clause_relids via pull_varnos; required_relids defaults
  to clause_relids; num_base_rels = members(clause_relids − outer_join_rels);
  `++last_rinfo_serial`; all cacheable "not yet set" markers
  (eval_cost.startup=-1, norm/outer_selec=-1, NIL lists, Invalid OIDs,
  bucketsize/mcvfreq=-1, has_volatile=VOLATILITY_UNKNOWN). palloc0's per_tuple=0
  reproduced. `bms_free(baserels)` = drop (no-op).
- **make_sub_restrictinfos** (static) — split into the `RinfoId`-returning top
  entry (OR; arms get required_relids=NULL) and the `Expr`-returning recursion
  `make_sub_restrictinfos_expr` (OR→plain rinfo+orclause embedded as
  Expr::RestrictInfo; AND→make_andclause of recursed args, required_relids passed
  through, NO rinfo above; else→plain rinfo embedded as Expr::RestrictInfo). This
  reproduces C's `(Expr *) make_plain_restrictinfo(...)` cast and the AND/OR
  required_relids handling exactly.
- **commute_restrictinfo** — 1:1. castNode(OpExpr); flat-copy clause, set
  opno=comm_op/opfuncid=Invalid/args=list_make2(2nd,1st); flat-copy rinfo, set
  clause, swap left/right relids+ec+em, scansel_cache=NIL, hashjoinoperator
  conditional, swap bucketsize/mcvfreq, hasheqoperator=Invalid. `Assert(orclause
  == NULL)` → debug_assert (preserved from the clone).
- **restriction_is_or_clause** — `orclause != NULL` → `.is_some()`.
- **restriction_is_securely_promotable** — `security_level <=
  rel->baserestrict_min_security || leakproof`.
- **rinfo_is_constant_true** (static inline) — IsA(clause,Const) && !constisnull
  && DatumGetBool(constvalue) → match Expr::Const + `!constisnull &&
  constvalue.as_bool()`.
- **get_actual_clauses** — collect rinfo->clause (as NodeId) with the two
  Asserts (!pseudoconstant, !constant_true).
- **extract_actual_clauses** — pseudoconstant filter + drop constant-true.
- **extract_actual_join_clauses** — RINFO_IS_PUSHED_DOWN split. The macro
  (`restrictinfo.h`: `is_pushed_down || !bms_is_subset(required_relids,
  joinrelids)`) is ported as `rinfo_is_pushed_down`. Returns `(joinquals,
  otherquals)` tuple in place of the two out-params.
- **join_clause_is_movable_to** — all five gates, in C order: member(baserel.relid,
  clause_relids); !member(relid, outer_relids); !overlap(clause_relids,
  nulling_relids); !overlap(lateral_referencers, clause_relids); !is_clone.
  (Prompt flagged this as a known bug site — verified every relid subtraction /
  gate matches C; no missing test.)
- **join_clause_is_movable_into** — subset(clause_relids, current_and_outer);
  overlap(currentrelids, clause_relids); !overlap(currentrelids, outer_relids).
  The costsize consumer seam passes `(inner_parent, joinrel)` RelIds; the
  installer maps them to `currentrelids = inner_parent.relids`,
  `current_and_outer = joinrel.relids` — exactly the C call
  `join_clause_is_movable_into(rinfo, innerpath->parent->relids, joinrelids)`
  (costsize.c:5266). NO spurious union.

## joininfo.c

- **have_relevant_joinclause** — shorter-list selection; overlap(other_relids,
  required_relids) early-out; EquivalenceClass fallback gated on both
  has_eclass_joins (equivclass seam, installed).
- **add_join_clause_to_rels** — restriction_is_always_true early return;
  always-false → rebuild as makeBoolConst(false) restrictinfo preserving
  rinfo_serial + restoring last_rinfo_serial; per-relid loop via bms_next_member;
  find_base_rel_ignore_join NULL-skip; append to rel->joininfo.
- **remove_join_clause_from_rels** — per-relid loop; NULL-skip; `Assert
  list_member_ptr` → debug_assert contains; remove first matching handle
  (= list_delete_ptr by identity).

## placeholder.c

- **make_placeholder_expr** — `++root->glob->lastPHId`; phexpr/phrels set,
  phnullingrels=NULL, phlevelsup=0. (Relids↔ExprRelids word-vector conversions
  mirror var.c.)
- **find_placeholder_info** — placeholder_array fast lookup; `elog(ERROR, "too
  late ...")` when placeholdersFrozen → `Err(PgError::error(...))`;
  ph_var=copyObject(phv) with phnullingrels forced NULL; rels_used=pull_varnos;
  ph_lateral=difference, ph_eval_at=int_members (force to phrels copy if empty,
  with Assert); ph_width=get_typavgwidth(exprType,exprTypmod); append to
  placeholder_list; array enlarge (×2 doubling from 8, `repalloc0`/`palloc0` =
  `resize(.., None)`); recurse find_placeholders_in_expr.
- **find_placeholders_in_jointree** — Assert(!frozen); early-out lastPHId==0;
  else delegates the `root->parse->jointree` walk to the ext-seam
  `find_placeholders_in_jointree_walk` (parse tree is opaque `QueryId` in this
  model — mirror-and-panic until the parse-tree-aware owner lands). NOT stubbed.
- **find_placeholders_recurse** (static) — folded into the seamed jointree walk
  above (it only ever runs as a sub-step of find_placeholders_in_jointree).
- **find_placeholders_in_expr** (static) — pull_var_clause(PVC_RECURSE_AGGREGATES
  |WINDOWFUNCS|INCLUDE_PLACEHOLDERS); for each PlaceHolderVar call
  find_placeholder_info; plain Vars ignored. Ported in-crate.
- **fix_placeholder_input_needed_levels** — per-phinfo pull_var_clause +
  add_vars_to_targetlist(ph_eval_at).
- **rebuild_placeholder_attr_needed** — same with add_vars_to_attr_needed.
- **add_placeholders_to_base_rels** — singleton(ph_eval_at) &&
  nonempty_difference(ph_needed, eval_at); find_base_rel; Assert phnullingrels
  NULL; append copyObject(ph_var) to reltarget.exprs.
- **add_placeholders_to_joinrel** — computable (subset eval_at⊆relids) &&
  needed-above (nonempty_difference); add to tlist only if not computable in
  either input, charging cost_qual_eval_node + ph_width; always extend
  direct_lateral_relids with ph_lateral; final width=clamp_width_est(tuple_width).
  `int64 tuple_width` preserved.
- **contain_placeholder_references_to** (+ walker, static) — lastPHId==0
  early-out; recursive walker over `&Expr`: PHV phlevelsup==sublevels_up →
  member(relid, phrels) (no phnullingrels exam, no recurse into phexpr); else
  expression_tree_walker. The C `IsA(node,Query)` arm (sublevels_up++ +
  query_tree_walker) is unreachable in the arena `Expr` model (no Query variant);
  C itself notes the upper-level-PHV handling is "likely dead". Documented in the
  walker; not stubbed (no Query subtree can reach it).
- **get_placeholder_nulling_relids** — `while ((relid =
  bms_next_member(ph_eval_at, relid)) > 0)` — note the C `> 0` (skips relid 0 and
  the -1 terminator) reproduced exactly; skip group_rtindex; NULL slot ⇒ Assert
  member(outer_join_rels); else union nulling_relids; finally del_members
  (pathnode-seam) of ph_eval_at.

## Seams

Installed (this unit is the C-source owner of these declarations placed earlier
by consumers):
- small-seams: `restriction_is_or_clause`, `restriction_is_securely_promotable`,
  `join_clause_is_movable_to`, and **added** `clause_selectivity(RinfoId)`
  (installed in small's init_seams — see below).
- costsize-seams: `join_clause_is_movable_into`.
- equivclass-ext-seams: `make_restrictinfo`.
- joinpath-seams: `find_placeholder_info` (NodeId→PHV wrapper; returns PhInfoId;
  the frozen/OOM error path can't fire on the memoize cache-key path, surfaced as
  a panic to match the fixed consumer signature).
- geqo-all-seams: `have_relevant_joinclause`.

Owner-side seam extension: added `clause_selectivity(root, RinfoId, var_relid,
jointype, sjinfo) -> PgResult<f64>` to backend-optimizer-path-small-seams and
installed it from backend-optimizer-path-small::init_seams (clausesel.c is the
real owner; orclauses.c estimates its OR clauses through it).

Outward seams (panic until owner lands), homed in the new owner-less
`backend-optimizer-util-joininfo-ext-seams` (guard-exempt; mirror-and-panic):
`pull_varnos_expr`, `pull_var_clause_expr`, `contain_leaked_vars`, `expr_type`,
`expr_typmod`, `cost_qual_eval_node_expr`, `clamp_width_est`,
`find_base_rel_ignore_join`, `restriction_is_always_true`,
`restriction_is_always_false`, `add_vars_to_targetlist`,
`add_vars_to_attr_needed`, `find_placeholders_in_jointree_walk`.
Reused installed seams: relnode-seams `bms`/`find_base_rel`, pathnode-seams
`relids_del_members`, equivclass-seams `have_relevant_eclass_joinclause`,
lsyscache-seams `get_typavgwidth`, small-seams `contain_volatile_functions_expr`
+ `clause_selectivity`.

`init_dummy_sjinfo` (joinrels.c) for orclauses.c's `consider_new_or_clause` is
ported **in-crate** (pure struct initialiser) rather than via the costsize-seams
seam, because that seam keys on two `RelId`s but the C call passes arbitrary
relid sets (`bms_difference(...)` outer side). Body identical to joinrels.c.

## Divergences found & fixed during audit

1. `join_clause_is_movable_into` installer initially formed
   `union(current, join)` for `current_and_outer`; the C costsize caller passes
   `joinrelids` directly. Fixed to pass `joinrel.relids` unmodified.
2. `init_dummy_sjinfo` via the RelId-keyed seam couldn't express the
   `bms_difference` outer relids; replaced with a faithful in-crate builder.

## Tests

4 unit tests (`--test-threads=1`, in-test relids algebra): movable_to gates (all
5 rejection paths + accept), movable_into gates (subset / current-ref /
outer-overlap), OR-clause detection, extract_actual_join_clauses split.
