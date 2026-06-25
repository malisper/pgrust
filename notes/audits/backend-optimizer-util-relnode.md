# Audit: backend-optimizer-util-relnode

- **Unit:** backend-optimizer-util-relnode
- **C source:** `src/backend/optimizer/util/relnode.c` (postgres-18.3, 2521 LOC)
- **Branch:** port/backend-optimizer-util-relnode
- **Date:** 2026-06-14
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Verdict:** **PASS** (1 divergence found and fixed)

Independent re-derivation from the C against the Rust port
(`crates/backend-optimizer-util-relnode/src/lib.rs`, 2091 LOC) over the
types-pathnodes arena+handle model (RelId/PathId/RinfoId/NodeId/EcId handles;
`rel()/rel_mut()/alloc_rel()`, `rinfo()`, `path()`, `node()/alloc_node()`,
`phinfo()`). The `bms_*` set algebra crosses the canonical `relids_*` seams
(relnode-seams + a few in pathnode-seams). Externals owned by other (mostly
unported) units cross their `-seams` crates; genuinely-absent ones are declared
in the new `backend-optimizer-util-relnode-ext-seams` (guard-exempt, no owner
dir).

## 1. Function inventory (all 30 definitions ported)

| C function (C loc) | Port | Verdict | Notes |
|---|---|---|---|
| setup_simple_rel_arrays (94) | :149 | MATCH | size = rtable_len+1; arrays NULL-init; append_rel_array filled from append_rel_list keyed by child_relid, "child relation already exists" panic. NB: C `setup_append_rel_array` does not exist as a separate fn (KEY-FUNCTIONS list named it; the file is authoritative — it is folded here). |
| expand_planner_arrays (163) | :199 | MATCH | repalloc0 = `resize(new,None)`; append_rel_array palloc-if-absent branch preserved. |
| build_simple_rel (192) | :223 | MATCH (reorder) | All field init mirrors C zero-out via `..Default::default()`. userid logic (BASEREL/OTHER+SUBQUERY → perminfo->checkAsUser else parent->userid) exact. attr range per rtekind exact (RESULT min=0/max=-1). **Reorder:** C calls `get_relation_info` inside the rtekind switch (before array insert); the arena port must allocate the rel to obtain a RelId before get_relation_info can mutate it by handle, so it runs immediately after array insert. Behaviour-identical: nothing reads simple_rel_array[relid] between the two points, and get_relation_info in C does not read the array. apply_child_basequals/mark_dummy_rel ordering preserved (after array insert, per C comment). |
| find_base_rel (414) | :385 | MATCH | unsigned compare; panic on miss. |
| find_base_rel_noerr (436) | :398 | MATCH | |
| find_base_rel_ignore_join (454) | :407 | MATCH | RTE_JOIN && jointype!=INNER → None, else panic. |
| build_join_rel_hash (486, static) | folded | EQUIVALENT | The hashtable is a pure lookup accelerator over the same join_rel_list with no observable effect; find_join_rel always runs the C linear-search branch (`bms_equal` scan), which yields the identical result. add_join_rel correspondingly only appends. No state is exposed to any other unit. |
| find_join_rel (527) | :433 | MATCH | linear `relids_equal` scan. |
| set_foreign_rel_properties (589, static) | :454 | MATCH | serverid match; 3 userid arms (equal / inner-Invalid+outer==GetUserId / outer-Invalid+inner==GetUserId); useridiscurrent OR; fdwroutine→has_fdwroutine flag (the trimmed model carries presence, not the pointer). GetUserId via miscinit seam. |
| add_join_rel (627, static) | :493 | MATCH | append; hash accelerator no-op (see folded note). |
| build_join_rel (665) | :511 | MATCH | find-or-build; direct_lateral_relids = union of inputs then `del_members(joinrel->relids)` after add_placeholders; min_join_parameterization; two build_joinrel_tlist calls with can_null = (FULL) / (!=INNER); restrictlist; joinlist; has_eclass_joins; partition_info; size estimates; consider_parallel triple-AND (inner&&outer parallel && parallel_safe(restrictlist) && parallel_safe(reltarget exprs)); join_rel_level append guarded by non-empty + cur_level asserts. |
| build_child_join_rel (882) | :664 | MATCH | adjust_child_relids; top_parent chain; reltarget via build_child_join_reltarget; joininfo via adjust_appendrel_attrs over restrictlist; lateral relids copied from parent; has_eclass_joins inherited; partition_info; consider_parallel inherited; build-once assert; eclass equivalences gated on has_eclass_joins||has_useful_pathkeys. |
| min_join_parameterization (1022) | :768 | MATCH | union(outer.lateral, inner.lateral) del_members joinrelids. |
| build_joinrel_tlist (1100, static) | :790 | MATCH | PHV branch (find_placeholder_info, ph_needed nonempty-diff test, copy+phnullingrels surgery when can_null: ojrelid membership + syn_righthand/FULL-syn_lefthand subset, pushed_down_joins loop, commute_above_r∩relids join, width += ph_width). Var branch (ROWID_VAR → rowidwidth; else baserel attr_needed[ndx] nonempty-diff skip + attr_widths[ndx]). varnullingrels surgery identical, never on ROWID_VAR. width = clamp_width_est. ExprRelids↔Relids bridge is a pure bit re-pack. |
| build_joinrel_restrictlist (1285, static) | :957 | MATCH | both_input_relids = union; two subbuild passes; concat generate_join_implied_equalities(joinrel.relids, outer.relids, inner_rel, sjinfo). |
| build_joinrel_joinlist (1322, static) | :992 | MATCH | two subbuild passes into joinrel.joininfo. |
| subbuild_joinrel_restrictlist (1340, static) | :1004 | MATCH | required_relids⊆joinrel.relids branch; clone-clause (has_clone||is_clone) both_input_relids subset + incompatible_relids overlap gates; non-clone assert; append-unique-ptr = `contains` dedup. |
| subbuild_joinrel_joinlist (1406, static) | :1053 | MATCH | RELOPT_JOINREL assert; subset→restriction(ignore) else append-unique. |
| fetch_upper_rel (1458) | :1083 | MATCH | per-kind list scan by relids_equal; else makeNode UPPER_REL with copied relids + empty target. |
| find_childrel_parents (1509) | :1112 | MATCH | do/while walk up append_rel_array[relid].parent_relid adding prelid until BASEREL. |
| get_baserel_parampathinfo (1545) | :1148 | MATCH | lateral⊆required assert; empty→None; overlap assert; find cache; movable joininfo loop; generate_join_implied_equalities(joinrelids, required_outer, baserel, NULL); assert-checking movability loop under cfg(debug); pserials = ∪ rinfo_serial; rows; PPI stored on ppilist. |
| get_joinrel_parampathinfo (1659) | :1238 | MATCH | join_and_req; outer/inner_and_req from param_info (else NULL); movable-into-join &&!outer&&!inner loop; EC loop dropping LHS/RHS-movable (RHS remembered via left_ec, left_ec==right_ec assert); dropped_ecs fixup with real_outer_and_req + generate_join_implied_equalities_for_ecs; `list_concat(pclauses, *restrict_clauses)` order preserved (pclauses front); cache check AFTER restrict_clauses build; rows; PPI with NIL clauses/serials. |
| get_appendrel_parampathinfo (1856) | :1398 | MATCH | asserts; empty→None; find cache; PPI rows=0, NIL clauses/serials. |
| find_param_path_info (1889) | :1435 | MATCH | ppilist scan by relids_equal(ppi_req_outer). |
| get_param_path_clause_serials (1910) | :1453 | MATCH | not-param→NULL; MergeAppend assert; Nest/Merge/Hash → ∪(outer serials, inner serials, joinrestrictinfo serials); Append → intersection across subpaths (first=copy, rest=int_members); else baserel ppi_serials. |
| build_joinrel_partition_info (1991, static) | :1534 | MATCH | enable_partitionwise_join gate; (a)part_scheme both set (b)consider_partitionwise_join both (c)schemes equal (d)have_partkey_equi_join; once-only asserts; part_scheme propagate + set_joinrel_partition_key_exprs; consider_partitionwise_join=true. |
| have_partkey_equi_join (2064, static) | :1609 | MATCH | restrictlist scan: outer-join pushed-down skip, can_join, equality (mergeopfamilies∥hashjoinoperator), OpExpr cast, operand→rel matching (both orientations), op_strict, remove_nulling_relids when strict & overlap outer_join_rels, match_expr_to_partition_keys both sides, ipk equality, collation reject, opfamily check (HASH: op_in_opfamily(hashjoinoperator); else mergeopfamilies membership), pk_known_equal counting → all-equal true. EC pass: btree_opfamily (HASH→get_opfamily_member(HTEqual)+first mergejoin opfamily; else partopfamily), partexprs cross-product exprs_known_equal with collation match, PG_USED_FOR_ASSERTS_ONLY partcoll2/exprcoll2 under cfg(debug); give-up break. |
| match_expr_to_partition_keys (2311, static) | :1825 | MATCH | RelabelType peel loop; partnatts scan non-nullable partexprs (always) then nullable (strict_op only) by `equal`. |
| set_joinrel_partition_key_exprs (2360, static) | :1878 | MATCH | per-key INNER (concat both) / SEMI,ANTI (outer copy) / LEFT (outer; nullable = inner_expr++outer_null++inner_null) / FULL (nullable = outer++inner++outer_null++inner_null + CoalesceExpr cross-product over (outer_expr++outer_null)×(inner_expr++inner_null), coalescetype/collid from larg, location -1). |
| build_child_join_reltarget (2504, static) | :1963 | MATCH | exprs = adjust_appendrel_attrs(parent exprs); cost.startup/per_tuple + width copied. |

## 2. Divergence found & fixed

**D1 (FIXED) — `IS_OUTER_JOIN` missing ANTI arms.** The inline `is_outer_join`
helper (consumed by `have_partkey_equi_join`'s pushed-down-clause skip) was
`(1<<LEFT)|(1<<FULL)|(1<<RIGHT)`, but the C macro (nodes.h:344) is
`LEFT|FULL|RIGHT|ANTI|RIGHT_ANTI`. Omitting JOIN_ANTI/JOIN_RIGHT_ANTI would have
let pushed-down (non-own) clauses of an anti-join be counted toward
partition-key equality, an incorrect partitionwise-join decision. Fixed to
include both anti arms (lib.rs:101-110). Re-checked: compiles, all gates green.

## 3. Seam contract notes (not divergences)

- relnode is the OWNER of and installs (init_seams, wired into seams-init):
  relnode-seams `find_base_rel/find_join_rel/build_join_rel/build_child_join_rel/
  min_join_parameterization`; pathnode-seams `get_baserel/appendrel/joinrel_parampathinfo/
  get_param_path_clause_serials`; equivclass-ext-seams `find_childrel_parents`.
- The pathnode-seams parampathinfo seams return bare values (no PgResult); the
  ereport channel (palloc / get_parameterized_*_size) is carried internally as
  PgResult and `.expect()`-ed at the seam boundary — i.e. an ereport(ERROR)
  becomes an abort, matching the C control flow. This is the pre-existing
  declared seam contract (owned elsewhere), not introduced here.
- `join_clause_is_movable_into_relids` is declared in the ext-seams (relids-typed)
  rather than reusing the costsize `RelId`-keyed seam because the C
  `join_clause_is_movable_into` takes raw `Relids` (e.g.
  `bms_union(baserel->relids, required_outer)`), which is not a built rel and
  cannot be expressed by a RelId. Faithful to the C signature.

## 4. Residual

No todo!()/unimplemented!()/own-logic stubs. All unported externals are
seam-and-panic via their owners' `-seams` crates (or the guard-exempt
relnode-ext-seams). residual_own_todos = 0.

## Verdict: PASS
