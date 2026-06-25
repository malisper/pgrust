# Audit: backend-optimizer-plan-init-subselect (initsplan.c)

C source: `src/backend/optimizer/plan/initsplan.c` (3895 LOC, PG 18.3).
c2rust: `../pgrust/c2rust-runs/` (initsplan unit).
src-idiomatic base: `../pgrust/src-idiomatic/crates/backend-optimizer-plan-initsplan`
(faithful control flow over the same arena model, but on the OLD central
`types`/`seams_ub_optcore` crate stack; STUBBED `lateral.rs` + `groupby.rs` and
omitted the `restriction_is_always_*` OR-clause arms).
Port: `crates/backend-optimizer-plan-init-subselect/src/{lib,baserels,targetlist,
groupby,lateral,jointree,outerjoin,quals,mergehash,fkeys}.rs`.

Scope: this unit's catalog `c_sources` is `initsplan.c,subselect.c`. ONLY
`initsplan.c` is ported here (subselect.c remains `todo`; catalog status =
`partial`).

## Reconciliation summary

Every external crossing was re-pointed off `seams_ub_optcore` onto this repo's
layered types (`types_core`/`types_nodes`/`types_pathnodes`/`types_error`) and
per-owner seam crates. `Relids` set-algebra → `backend-optimizer-util-relnode-seams`
(`bms`). Arena handles resolve on `root` (`root.rel/rel_mut/rinfo/rinfo_mut/
node/alloc_node`, `root.eq_classes`, `root.join_domains`). `deconstruct_jointree`
returns `Vec<JoinlistNode>` (`JoinlistNode::Rel(i32)` / `Sub(Vec<..>)`) — the
repo's joinlist carrier — instead of the C `List *` of RangeTblRef/sublists.

## Function inventory & verdicts

| # | C function | Port loc | Verdict |
|---|------------|----------|---------|
| 1 | add_base_rels_to_query | baserels.rs | MATCH |
| 2 | add_other_rels_to_query | baserels.rs | MATCH (expand_inherited_rtentry via inherit ext-seam) |
| 3 | build_base_rel_tlists | targetlist.rs | MATCH (reads processed_tlist + havingQual off run/root) |
| 4 | add_vars_to_targetlist | targetlist.rs | MATCH |
| 5 | add_vars_to_attr_needed | targetlist.rs | MATCH |
| 6 | remove_useless_groupby_columns (+helpers) | groupby.rs | MATCH (src-idiomatic was keystone-stubbed; ported from C) |
| 7 | find_lateral_references | lateral.rs | MATCH (src-idiomatic STUBBED; ported from C) |
| 8 | extract_lateral_references | lateral.rs | MATCH (stores Var/PHV into lateral_vars:Vec<NodeId> via alloc_node) |
| 9 | rebuild_lateral_attr_needed | lateral.rs | MATCH |
| 10 | create_lateral_join_info | lateral.rs | MATCH (incl. Warshall transitive closure + lateral_referencers inverse map) |
| 11 | deconstruct_jointree | jointree.rs | MATCH (returns Vec<JoinlistNode>, PgResult) |
| 12 | deconstruct_recurse | jointree.rs | MATCH (JoinTreeItem arena, pre-order reserve + post-order distribute) |
| 13 | deconstruct_distribute | jointree.rs | MATCH |
| 14 | process_security_barrier_quals | jointree.rs | MATCH |
| 15 | mark_rels_nulled_by_join | jointree.rs | MATCH |
| 16 | make_outerjoininfo | outerjoin.rs | MATCH |
| 17 | compute_semijoin_info | outerjoin.rs | MATCH (semi_rhs_exprs interned to arena) |
| 18 | deconstruct_distribute_oj_quals | jointree.rs | MATCH (OJ identity-3 commute qual variants) |
| 19 | distribute_quals_to_rels | jointree.rs | MATCH |
| 20 | distribute_qual_to_rels | quals.rs | MATCH |
| 21 | check_redundant_nullability_qual | quals.rs | MATCH |
| 22 | add_base_clause_to_rel | quals.rs | MATCH (always-true drop / always-false→FALSE-const, preserving rinfo_serial) |
| 23 | expr_is_nonnullable | quals.rs | MATCH (notnullattnums via bms) |
| 24 | restriction_is_always_true | quals.rs | MATCH (OR-clause recursion ported; src-idiomatic deferred) |
| 25 | restriction_is_always_false | quals.rs | MATCH (OR-clause all-branches rule ported; src-idiomatic omitted) |
| 26 | distribute_restrictinfo_to_rels | quals.rs | MATCH |
| 27 | process_implied_equality | quals.rs | MATCH |
| 28 | build_implied_join_equality | quals.rs | MATCH |
| 29 | get_join_domain_min_rels | quals.rs | MATCH |
| 30 | rebuild_joinclause_attr_needed | quals.rs | MATCH |
| 31 | match_foreign_keys_to_quals | fkeys.rs | KEYSTONE STOP — seam-and-panic (no-op when fkey_list empty) |
| 32 | check_mergejoinable | mergehash.rs | MATCH (get_mergejoin_opfamilies; PgResult for cache-lookup ereport) |
| 33 | check_hashjoinable | mergehash.rs | MATCH (op_hashjoinable) |
| 34 | check_memoizable | mergehash.rs | MATCH |

(add_vars_to_attr_needed/add_vars_to_targetlist appear once; helper `push_oj_clause_info`
and the implicit-AND / make_list / new_join_domain helpers are owned-tree
mechanics for the C List/JoinDomain construction.)

## Stub-fills verified against C (these were NOT blocked in this repo)

- **lateral.rs** — src-idiomatic panicked the whole family as KEYSTONE-BLOCKED
  (claimed `RelOptInfo.lateral_vars` was tag-only `Node`). In THIS repo
  `lateral_vars: Vec<NodeId>` carries full payload, so the keystone is resolved.
  All four functions ported from C lines 658-1083 incl. the two-pass PHV
  eval-site marking and the Warshall transitive-closure + inverse-mapping loops.
- **restriction_is_always_true/false** — src-idiomatic `_true` `continue`d on OR
  branches ("deferred") and `_false` omitted the OR arm entirely. Both arms
  ported faithfully using `Expr::RestrictInfo(RinfoRef)` → `RinfoId` recursion
  and the `restriction_is_or_clause` helper (path-small-seams). `_true`: true on
  ANY always-true branch. `_false`: true only if ALL branches are RestrictInfo
  AND always-false.
- **groupby.rs** — `remove_useless_groupby_columns` + helpers ported from C
  lines 412-657 (was keystone-stubbed in src-idiomatic).

## Contract change (resolved, faithful — #264 resolver model)

5 `backend-optimizer-plan-small-seams` decls were widened to carry
`run: &PlannerRun<'mcx>` (`remove_useless_groupby_columns`, `build_base_rel_tlists`,
`find_lateral_references`, `deconstruct_jointree`, `create_lateral_join_info`) and
`deconstruct_jointree` now returns `PgResult` (mirrors `make_outerjoininfo`'s C
ereport surface). The owner must resolve `parse->jointree` / `simple_rte_array`
through `PlannerRun` (the `QueryId`/`RangeTblEntryId` value resolver, #264) — the
original decls predated full resolver threading. Sole consumer `query_planner`
(`backend-optimizer-plan-small`, already holds `run` and returns `PgResult`) was
updated. No CONTRACT_RECONCILE_PENDING entry existed to retire.

## KEYSTONE STOP — match_foreign_keys_to_quals

`types_pathnodes::ForeignKeyOptInfo` is trimmed: it has the identity/key fields
(`con_relid/ref_relid/nkeys/conkey/confkey/conpfeqop/eclass/fk_eclass_member`)
but OMITS the five match-result fields the function writes back —
`rinfos[]` (`List **`), `nmatched_ec`, `nconst_ec`, `nmatched_ri`,
`nmatched_rcols`. Per policy this is a shared-types keystone, NOT widened here.
`match_foreign_keys_to_quals` is a faithful no-op when `root.fkey_list` is empty
(C loops an empty list and assigns the empty newlist back) and a loud panic with
the precise blocker otherwise. Decomposition to unblock: add the 5 fields to
`ForeignKeyOptInfo` (ripples its constructor in plancat/`get_relation_foreign_keys`
+ the `nmatched_*` readers in joinrels/costsize), then fill fkeys.rs (EC match via
`match_eclasses_to_foreign_key_col` + RelabelType-stripping loose-qual scan over
`con_rel->joininfo` + `get_commutator` reverse form) which is understood and ports
1:1.

## Seams installed (this crate's init_seams; wired in seams-init::init_all)

- backend-optimizer-plan-small-seams (8): add_base_rels_to_query,
  remove_useless_groupby_columns, build_base_rel_tlists, find_lateral_references,
  deconstruct_jointree, create_lateral_join_info, match_foreign_keys_to_quals,
  add_other_rels_to_query.
- backend-optimizer-path-equivclass-ext-seams (5): add_vars_to_targetlist,
  add_vars_to_attr_needed, distribute_restrictinfo_to_rels, process_implied_equality,
  build_implied_join_equality.
- backend-optimizer-util-joininfo-ext-seams (4): add_vars_to_targetlist,
  add_vars_to_attr_needed, restriction_is_always_true, restriction_is_always_false.

## Outward loud-panic seams (unported owners — faithful absent-subsystem boundary)

In `backend-optimizer-plan-init-subselect-ext-seams`: increment_var_sublevels_up /
preprocess_phv_expression (rewriteManip/subselect — reached only for upper-level
LATERAL PHVs), expand_inherited_rtentry (inherit.c), lookup_type_cache_hasheq,
phinfo_add_needed, plus arena-`Expr`-vs-owner-`&Node`/`Mcx` bridge seams for
clauses.c/var.c calls that would cycle. Loud-panic until installed; never silent.

## Gate

- `cargo check -p backend-optimizer-plan-init-subselect -p backend-optimizer-plan-small-seams -p seams-init` — PASS
- `cargo check --workspace` — PASS
- `cargo test -p no-todo-guard` — PASS (10/10)
- `cargo test -p seams-init` — PASS (1 passed, 1 ignored)
- No `todo!()`/`unimplemented!()`; the only `panic!`s are C `Assert(false)`
  defensive arms, the match_foreign_keys keystone, and loud-panic seams.
