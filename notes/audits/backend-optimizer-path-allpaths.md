# Audit: backend-optimizer-path-allpaths (allpaths.c, PG 18.3)

C source: `src/backend/optimizer/path/allpaths.c` (4433 LOC). Audited 1:1
function-by-function against the C. Value model: `types_pathnodes` arena
(`PlannerInfo`/`RelId`/`PathId`/`Relids`); cross-crate calls via `<owner>-seams`.

## Verdict

PASS for the portable surface. The path-generation spine is a faithful 1:1 port.
The subquery-pushdown vertical + CTE name resolution + a handful of unported-owner
dependencies are routed through seam-and-panic (documented below), because the
substrate they need (an owned `Query<'mcx>` value model; plancat/FDW/TSM/planner
owners) is not yet in the repo. This is the sanctioned mirror-pg-and-panic
pattern, not a silent stub — every deferral is a loud seam call.

## Functions ported in-crate (1:1)

| C function | Rust | Notes |
|---|---|---|
| make_one_rel (170) | lib.rs `make_one_rel` | total_table_pages loop, IS_DUMMY/IS_SIMPLE skips, final all_query_rels assert all 1:1. Returns final RelId (C returns `RelOptInfo *`). |
| set_base_rel_consider_startup (246) | lib.rs | SEMI/ANTI singleton-RHS → consider_param_startup. Singletons collected first to avoid borrow conflict (behaviour-identical). |
| set_base_rel_sizes (289) | lib.rs | RELOPT_BASEREL filter; set_rel_consider_parallel before set_rel_size. |
| set_base_rel_pathlists (332) | lib.rs | |
| set_rel_size (359) | lib.rs | RTE-kind dispatch 1:1; CE gate, inh, RELATION sub-dispatch (foreign/partitioned-ONLY-dummy/tablesample/plain), subquery, function/tablefunc/values size-est, CTE self-ref→worktable else cte, namedtuplestore, result. Final rows>0 assert. |
| set_rel_pathlist (472) | lib.rs | dispatch + finishing (set_rel_pathlist_hook is NULL in core, omitted; non-topmost baserel gather; set_cheapest). |
| set_plain_rel_size (571) | lib.rs | check_index_predicates then set_baserel_size_estimates. |
| set_rel_consider_parallel (588) | lib.rs | full rtekind switch; temp-table, tablesample, FDW, subquery-limit, function/values parallel-safety; baserestrictinfo + reltarget checks. The per-kind `is_parallel_safe` probes over RTE/Query subtrees are seamed (owner-absent). |
| set_plain_rel_pathlist (767) | lib.rs | tidscan-forced early return; seqscan+add_path; parallel partial; create_index_paths. |
| create_plain_partial_paths (805) | lib.rs | compute_parallel_worker + create_seqscan partial. |
| set_tablesample_rel_size (826) | lib.rs | check_index_predicates; SampleScanGetSampleSize seamed (TSM dispatch); set_baserel_size_estimates. |
| set_tablesample_rel_pathlist (866) | lib.rs | samplescan; material-wrap when (query_level>1 || membership!=SINGLETON) && !repeatable_across_scans (TSM seamed). |
| set_foreign_size (913) | lib.rs | set_foreign_size_estimates (seamed); FDW GetForeignRelSize (seamed); clamp_row_est; tuples=max(tuples,rows). |
| set_foreign_pathlist (937) | lib.rs | FDW GetForeignPaths (seamed). |
| set_append_rel_size (955) | append.rs | partitionwise-join flag (whole-row-Var-free attr_needed[InvalidAttrNumber-min_attr]); per-child CE/dummy; joininfo filter by nulling_relids then adjust_appendrel_attrs_restrictlist; reltarget exprs translated via adjust_appendrel_attrs (alloc_node back into arena); add_child_rel_equivalences when has_eclass_joins||has_useful_pathkeys; consider_partitionwise_join + parallel propagation; recurse set_rel_size; size + per-column width accumulation (Var match, get_typavgwidth fallback); finished width = rint(size/rows). |
| set_append_rel_pathlist (1250) | append.rs | per-child parallel-unsafe propagation; set_rel_pathlist; live-child collection; add_paths_to_append_rel. |
| add_paths_to_append_rel (1320) | append.rs | full: cheapest-total/startup/partial subpath collection; parallel-append mix (get_cheapest_parallel_safe_total_inner); all_child_pathkeys/outers dedup; unparam/startup/partial/pa appends; ordered appends; per-parameterization appends; single-child ordered partial paths. pg_leftmost_one_pos32 via leading_zeros. |
| generate_orderedappend_paths (1748) | append.rs | partitions_are_ordered (seamed→partbounds); build_partition_pathkeys fwd/desc; per-ordering match (asc/desc), reverse iteration for desc; cheapest startup/total/fractional per child; Append (singleton-flattened) vs MergeAppend. |
| get_cheapest_parameterized_child_path (2047) | append.rs | exact-param fast path; reparameterize loop with cost pruning. |
| accumulate_append_subpath (2135) | append.rs | AppendPath flatten (parallel-aware split via first_partial_path); MergeAppend flatten; else push. |
| get_singleton_append_subpath (2180) | append.rs | single-subpath Append/MergeAppend unwrap. |
| set_dummy_rel_pathlist (2215) | dummy.rs | rows=0, width=0, clear pathlists, childless AppendPath (have_root=false ≡ C root==NULL), set_cheapest. |
| set_function_pathlist (2795) | rte_simple.rs | WITH ORDINALITY → build_ordinality_pathkeys (Int8LessOperator 412, EC-membership gate via build_expression_pathkey). |
| set_values_pathlist (2862) | rte_simple.rs | |
| set_tablefunc_pathlist (2882) | rte_simple.rs | |
| set_namedtuplestore_pathlist (2985) | rte_simple.rs | size-est + scan path. |
| set_result_pathlist (3012) | rte_simple.rs | size-est + scan path. |
| generate_gather_paths (3098) | gather.rs | cheapest partial → Gather; per-ordered-partial → GatherMerge; compute_gather_rows; override_rows handling. |
| get_useful_pathkeys_for_relation (3167) | gather.rs | query_pathkeys truncated at first non-early-sortable EC (relation_can_be_sorted_early). |
| generate_useful_gather_paths (3235) | gather.rs | regular gather + per-useful-ordering sort/incremental-sort + GatherMerge; is_sorted skip; presorted/enable_incremental_sort gating. |
| make_rel_from_joinlist (3351) | joinsearch.rs | JoinlistNode enum (Rel(rtindex)/Sub) = owned `List` of RangeTblRef/List; recursion; single-node fast path; join_search_hook NULL; GEQO vs standard_join_search. |
| standard_join_search (3456) | joinsearch.rs | join_rel_level alloc [0..=levels]; per-level join_search_one_level then partitionwise+gather+set_cheapest; final single-rel check; clear join_rel_level. |
| compute_parallel_worker (4273) | parallel_workers.rs | reloption override; min-size early-zero (baserel only); log3 heap/index worker counts with overflow guard; min(max_workers). |
| generate_partitionwise_join_paths (4361) | partwise.rs | IS_JOIN_REL+IS_PARTITIONED_REL gates; recurse child-joins; set_cheapest; live collection; all-dummy→mark_dummy_rel; add_paths_to_append_rel. |
| create_partial_bitmap_paths (4237) | bitmap.rs | compute_bitmap_pages (costsize); compute_parallel_worker; create_bitmap_heap_path partial. |
| build_and_cost_join_rel | lib.rs | geqo merge_clump body: make_join_rel + partitionwise + (non-topmost) gather + set_cheapest. Installed for the geqo-all-seams seam (no Mcx param → local MemoryContext). |

## Owned seams INSTALLED (allpaths is the C owner)

* `backend_optimizer_path_costsize_seams::compute_parallel_worker` (consumed by costsize/scans.rs).
* `backend_optimizer_path_costsize_seams::create_partial_bitmap_paths` (consumed by indxpath/drivers.rs). (Static guard attributes it to costsize's dir → kept in CONTRACT_RECONCILE_PENDING; installed at runtime by allpaths.)
* `backend_geqo_all_seams::build_and_cost_join_rel` (consumed by geqo/eval.rs).

## Seam-and-panic (owner crate ABSENT / Query keystone) — `crate::seams` + `crate::subquery::seams`

KEYSTONE-BLOCKED — the subquery-pushdown vertical reads the owned `Query<'mcx>`
subtree (targetList/setOperations/windowClause/distinctClause/…), owned by the
unported planner-entry crate. `types_pathnodes` has only the opaque `QueryId`:
* set_subquery_pathlist (2528) + its pushdown cohort (subquery_is_pushdown_safe
  3627, recurse_pushdown_safe 3683, check_output_expressions, compare_tlist_
  datatypes, targetIsInAllPartitionLists, qual_is_pushdown_safe 3924,
  subquery_push_qual, recurse_push_qual, remove_unused_subquery_outputs 4125,
  find_window_run_conditions 2262, check_and_push_window_quals 2453) — routed via
  `subquery::seams::set_subquery_pathlist`.
* set_cte_pathlist (2906), set_worktable_pathlist (3039) — resolve a CTE by name
  out of `parse->cteList` (a Query subtree) — routed via
  `subquery::seams::set_{cte,worktable}_pathlist`.

OWNER-ABSENT dependency seams (`crate::seams`):
* relation_excluded_by_constraints (plancat.c — owner crate not ported).
* get_rel_persistence (lsyscache.c).
* set_foreign_size_estimates / set_values_size_estimates / set_subquery_size_estimates (costsize.c — not yet pub fns on the landed crate).
* FDW dispatch: fdw_get_foreign_rel_size / fdw_get_foreign_paths / fdw_is_foreign_scan_parallel_safe (fdwapi.h).
* TABLESAMPLE dispatch: tsm_get_sample_size / tsm_repeatable_across_scans / tsm_is_parallel_safe (tsmapi.h).
* parallel-safety probes over RTE/Query subtrees: subquery_limit_needed, rte_functions_parallel_safe, rte_values_lists_parallel_safe, rel_baserestrictinfo_parallel_safe, rel_reltarget_parallel_safe (clauses.c/planner.c).
* partitions_are_ordered (partbounds.c), get_cheapest_fractional_path (pathkeys.c, plain variant).

These are declared as `seam!`s on the allpaths crate; allpaths is a `todo`/now-
`audited` owner with an in-progress frontier, so the recurrence guard exempts its
own uninstalled seams (mirror-pg-and-panic) and no CONTRACT_RECONCILE_PENDING
entry is required for them (verified: adding them made the guard report them
stale, because the owner is not yet COMPLETE — they correctly loud-panic until
each real owner lands and installs them).

## Divergences found & fixed during audit

1. `path_req_outer` initially used `.path()` accessor; corrected to `.base()`
   (the PathNode→Path up-cast).
2. `Oid` is a bare `u32` (not a tuple struct): `Int8LessOperator` const fixed to
   `412`.
3. `build_partition_pathkeys` returns `(Vec<PathKey>, bool)` (no `&mut` out-param
   for the "partial" flag); call sites corrected.
4. `create_tidscan_paths` (path-small) lifts the `enable_tidscan` GUC to an
   explicit param and takes no Mcx; wrapper corrected to read the live GUC.
5. `compute_bitmap_pages` (costsize) returns `(pages, cost, tuples)`; allpaths
   uses only `pages`.
6. RTEKind constants beyond RTE_RELATION are not exported by types_pathnodes;
   mirrored the parsenodes.h discriminants (SUBQUERY=1 … RESULT=8) locally to
   match the `u32` the `rte_rtekind` seam returns.

## Behaviour-preserving model adaptations (not divergences)

* C returns `RelOptInfo *`; we return the `RelId` arena handle (the canonical
  optimizer value model).
* C reads file-scope GUC globals (enable_*, *_parallel_*_scan_size,
  geqo_threshold, max_parallel_workers_per_gather); we read the live
  `backend_utils_misc_guc_tables::vars` values at the same points.
* The joinlist `List` of `RangeTblRef`/`List` is the owned `JoinlistNode` enum.
* `build_and_cost_join_rel` builds a local `MemoryContext` for the GEQO
  merge_clump call since that seam carries no Mcx (the path work allocates into
  the PlannerInfo arena; Mcx is only the OOM channel).
