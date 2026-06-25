# Audit: backend-optimizer-path-indxpath

C source: `src/backend/optimizer/path/indxpath.c` (PostgreSQL 18.3), 50 function
definitions. Port over the planner NodeId/RinfoId/RelId/PathId arena model
(types-pathnodes): `RestrictInfo.clause` is a `NodeId` resolved via
`root.node()`; relids are `Option<Box<Bitmapset>>`; derived quals built as `Expr`
then `alloc_node`'d. Modules: drivers / bitmap / matchers / pathkeys /
predicates / unique / operand / cost / util.

Verdict: **PASS** (after fixing 2 functions / 3 defects found on first pass; all
re-audited from scratch and confirmed MATCH).

## Function table

| function | C line | port location | verdict | note |
|---|---|---|---|---|
| create_index_paths | 241 | drivers.rs:33 | MATCH | |
| consider_index_join_clauses | 438 | drivers.rs:190 | MATCH | |
| consider_index_join_outer_rels | 504 | drivers.rs:243 | MATCH | num_considered_relids snapshot + heuristic limit faithful |
| get_join_index_paths | 607 | drivers.rs:330 | MATCH | |
| eclass_already_used | 685 | drivers.rs:395 | MATCH | |
| get_index_paths | 717 | drivers.rs:421 | MATCH | |
| build_index_paths | 811 | drivers.rs:488 | MATCH | all 5 phases + fwd/back + parallel faithful |
| build_paths_for_OR | 1093 | bitmap.rs:36 | MATCH | |
| or_arg_index_match_cmp | 1201 | bitmap.rs:147 | MATCH | comparator signs exact |
| or_arg_index_match_cmp_group | 1239 | bitmap.rs:158 | MATCH | |
| group_similar_or_args | 1272 | bitmap.rs:169 | MATCH | group-boundary loop faithful |
| make_bitmap_paths_for_or_group | 1549 | bitmap.rs:389 | MATCH | |
| generate_bitmap_or_paths | 1630 | bitmap.rs:459 | MATCH | |
| choose_bitmap_and | 1786 | bitmap.rs:600 | MATCH | dedup + O(N^2) AND-leader + predicate_implied_by redundancy faithful |
| path_usage_comparator | 1992 | cost.rs:124 | MATCH | |
| bitmap_scan_cost_est | 2025 | cost.rs:154 | MATCH | |
| bitmap_and_cost_est | 2059 | cost.rs:210 | MATCH | |
| classify_index_clause_usage | 2088 | bitmap.rs:723 | MATCH | >100 -> unclassifiable |
| find_indexpath_quals | 2156 | bitmap.rs:768 | MATCH | |
| find_list_position | 2203 | bitmap.rs:808 | MATCH | equal() via seam |
| check_index_only | 2229 | predicates.rs:21 | MATCH | FirstLowInvalidHeapAttributeNumber offset faithful |
| get_loop_count | 2328 | cost.rs:76 | MATCH | clamp >=1.0 faithful |
| adjust_rowcount_for_semijoins | 2381 | cost.rs:50 | MATCH | |
| approximate_joinrel_size | 2425 | cost.rs:20 | MATCH | |
| match_restriction_clauses_to_index | 2467 | matchers.rs:72 | MATCH | |
| match_join_clauses_to_index | 2483 | matchers.rs:1073 | MATCH | **FIXED** (see below) |
| match_eclass_clauses_to_index | 2517 | matchers.rs:1100 | MATCH | callback to ec_member_matches_indexcol via equivclass owner |
| match_clauses_to_index | 2555 | matchers.rs:85 | MATCH | |
| match_clause_to_index | 2588 | matchers.rs:101 | MATCH | |
| match_clause_to_indexcol | 2712 | matchers.rs:157 | MATCH | |
| match_boolean_index_clause | 2818 | matchers.rs:229 | MATCH | |
| match_opclause_to_indexcol | 2905 | matchers.rs:310 | MATCH | |
| match_funcclause_to_indexcol | 3024 | matchers.rs:395 | MATCH | |
| get_index_clause_from_support | 3070 | matchers.rs:436 | MATCH | support fn dispatch SEAMED (expected) |
| match_saopclause_to_indexcol | 3136 | matchers.rs:492 | MATCH | |
| match_rowcompare_to_indexcol | 3204 | matchers.rs:589 | MATCH | |
| match_orclause_to_indexcol | 3298 | matchers.rs:900 | MATCH | |
| expand_indexqual_rowcompare | 3496 | matchers.rs:680 | MATCH | column off-by-one loop faithful |
| match_pathkeys_to_index | 3718 | pathkeys.rs:24 | MATCH | |
| match_clause_to_ordering_op | 3829 | pathkeys.rs:120 | MATCH | |
| check_index_predicates | 3943 | predicates.rs:83 | MATCH | **FIXED** (see below) |
| ec_member_matches_indexcol | 4091 | unique.rs:64 | MATCH | BTREE_AM_OID check faithful |
| relation_has_unique_index_for | 4149 | unique.rs:91 | MATCH | thin wrapper |
| relation_has_unique_index_ext | 4164 | unique.rs:109 | MATCH | O(N^2) match + extra_clauses faithful |
| indexcol_is_bool_constant_for_query | 4362 | predicates.rs:198 | MATCH | |
| match_index_to_operand | 4413 | operand.rs:23 | MATCH | nested RelabelType peel + PHV strip faithful |
| strip_phvs_in_index_operand | 4508 | operand.rs:92 | MATCH | |
| contain_strippable_phv_walker | 4526 | operand.rs:103 | MATCH | |
| strip_phvs_in_index_operand_mutator | 4551 | operand.rs:119 | MATCH | |
| is_pseudo_constant_for_index | 4596 | operand.rs:143 | MATCH | |

Counts: MATCH 50, DIVERGES/MISSING/PARTIAL 0.

## Defects found and fixed (first pass)

1. **match_join_clauses_to_index** (matchers.rs): (a) dropped the
   `join_clause_is_movable_to(rinfo, rel)` gate, so non-movable join clauses
   were collected; (b) used `if OR {...} else { match_clause_to_index }`, so OR
   join clauses were never dispatched to `match_clause_to_index`. Fixed to match
   C 2496-2507: movability `continue` gate, append-unique OR, then
   `match_clause_to_index` unconditionally.

2. **check_index_predicates** (predicates.rs), two defects on fields that ARE
   modeled in types-pathnodes:
   - omitted `bms_del_members(otherrels, rel->nulling_relids)` (C 4005). Fixed
     with `bms_difference_relids(&otherrels, &nulling_relids)`.
   - `is_target_rel` dropped the `bms_is_member(rel->relid,
     root->all_result_relids)` disjunct (C 4029). Fixed to OR it with the
     existing rowmark check.

All three re-audited from scratch post-fix: MATCH.

## Seams and wiring

Owned inward seam crate `backend-optimizer-path-indxpath-seams` (6 decls:
create_index_paths, check_index_predicates, relation_has_unique_index_for,
relation_has_unique_index_ext, indexcol_is_bool_constant_for_query,
match_index_to_operand). All 6 are installed by the crate's `init_seams()`
(lib.rs), which contains nothing but `set()` calls and is wired into
`seams-init::init_all()`. Both seams-init recurrence guards pass.

Outward seam calls (justified by genuinely-unported callees / dependency
boundaries; thin marshal+delegate, no logic in the seam path):
- predtest.c `predicate_implied_by` (choose_bitmap_and, check_index_predicates)
- equivclass.c `generate_join_implied_equalities` (check_index_predicates) and
  `generate_implied_equalities_for_column` (match_eclass_clauses_to_index) —
  equivclass is ported; both now call the equivclass owner directly (the latter
  with the ported `ec_member_matches_indexcol` as the callback closure), not a
  divergent self-seam.
- restrictinfo.c `make_simple_restrictinfo`, `join_clause_is_movable_to`,
  `find_childrel_parents`, `has_plan_rowmark` (relnode/preprocess owners unported)
- pathkeys.c has_useful/build_index/truncate_useless_pathkeys
- costsize.c `cost_bitmap_tree_node` / `enable_indexonlyscan` (now installed by
  the merged costsize owner) and `create_partial_bitmap_paths` (real owner
  allpaths.c, unported -> CONTRACT_RECONCILE_PENDING + DESIGN_DEBT)
- selfuncs.c `estimate_num_groups`; equalfuncs.c `equal`; nodeFuncs/var/clauses
  walkers; lsyscache; pathnode create_*_path/add_path; fmgr planner-support.

No body-replaced-by-seam (MISSING) cases. No allocating fn/seam missing
Mcx/PgResult. No invented opacity, shared statics for per-backend globals, or
registry side tables introduced by this port.

Gate (isolated CARGO_TARGET_DIR, CARGO_INCREMENTAL=0): `cargo check --workspace`
clean; `cargo test -p no-todo-guard` ok; `cargo test -p seams-init` both guards
ok; `cargo test --workspace` 0 failed (single-threaded; one pre-existing
multi-threaded flake in backend-optimizer-path-small, present on clean
origin/main, unrelated).
