//! Seam declarations for the `backend-optimizer-path-costsize` unit
//! (`optimizer/path/costsize.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The `random_page_cost` / `seq_page_cost` GUC globals deliberately have no
//! getter seams: per the no-ambient-global-seams rule, consumers take the
//! values as explicit parameters.

extern crate alloc;

use types_error::PgResult;
use types_pathnodes::{PathId as IxPathId, PlannerInfo as IxPlannerInfo, RelId as IxRelId};

seam_core::seam!(
    /// `cost_bitmap_tree_node(path, &cost, &selec)` (costsize.c) — returned as a
    /// `(cost, selectivity)` tuple. The bitmap-tree path crosses as its `PathId`
    /// arena handle; the provider dispatches on the arena `PathNode` subtype.
    pub fn cost_bitmap_tree_node(root: &IxPlannerInfo, path: IxPathId) -> (f64, f64)
);

seam_core::seam!(
    /// `enable_indexonlyscan` (costsize.c GUC) — whether index-only scans are
    /// enabled. Read by `check_index_only`.
    pub fn enable_indexonlyscan() -> bool
);

seam_core::seam!(
    /// `create_partial_bitmap_paths(root, rel, bitmapqual)` (costsize.c) — build
    /// the partial (parallel) BitmapHeapPath(s) for the rel and `add_partial_path`
    /// them. The bitmapqual crosses as its `PathId` arena handle.
    pub fn create_partial_bitmap_paths<'mcx>(
        root: &mut IxPlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        rel: IxRelId,
        bitmapqual: IxPathId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `clamp_row_est(nrows)` (costsize.c): force a row-count estimate to a
    /// sane value — `rint()` it and clamp to at least one row. Pure math;
    /// cannot `ereport`.
    pub fn clamp_row_est(nrows: f64) -> f64
);

seam_core::seam!(
    /// `clamp_cardinality_to_long(x)` (costsize.c): cast a `Cardinality`
    /// (`double`) to a sane `long` (here `i64`). `NaN` -> `i64::MAX`; `x <= 0`
    /// -> 0; otherwise `x` if it is strictly below `i64::MAX` as a double, else
    /// `i64::MAX`. Pure math; cannot `ereport`.
    pub fn clamp_cardinality_to_long(x: f64) -> i64
);

/* ==========================================================================
 * Cross-unit deps with no ported owner in the fabled tree (selectivity /
 * catalog / AM / RTE-read / executor legs). costsize.c reads exactly the
 * value declared here; the surrounding cost arithmetic stays in-crate. Each
 * panics until its owner installs it ("mirror PG and panic").
 *
 * Argument/return shapes are adapted to the fabled value/arena model
 * (`NodeId`/`RinfoId`/`RelId`/`PathId` handles into the `PlannerInfo` arena,
 * `&PlannerInfo`, primitive scalars).
 * ======================================================================== */

use types_core::primitive::{Cost, Oid, Selectivity};
use types_pathnodes::{NodeId, PathId, PlannerInfo, RelId, RinfoId, SpecialJoinInfo};

/// `GetTablespacePageCosts` output (`utils/cache/spccache.c`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TablespacePageCosts {
    pub spc_random_page_cost: f64,
    pub spc_seq_page_cost: f64,
}

/// `amcostestimate` output (`access/<am>/...` cost callback via index AM).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AmCostEstimate {
    pub index_startup_cost: Cost,
    pub index_total_cost: Cost,
    pub index_selectivity: Selectivity,
    pub index_correlation: f64,
    pub index_pages: f64,
}

/// `hash_agg_set_limits` output (`executor/nodeAgg.c`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HashAggLimits {
    pub mem_limit: usize,
    pub ngroups_limit: u64,
    pub num_partitions: i32,
}

/// `ExecChooseHashTableSize` output (`executor/nodeHash.c`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HashTableSize {
    pub numbuckets: i32,
    pub numbatches: i32,
}

/* --- selectivity (selfuncs.c / clausesel.c) ----------------------------- */
seam_core::seam!(
    /// `clauselist_selectivity(root, clauses, varRelid, jointype, sjinfo)` over
    /// a list of clause-expr handles (the C `List *RestrictInfo*`).
    pub fn clauselist_selectivity<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clauses: &[NodeId],
        var_relid: i32,
        jointype: i32,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> Selectivity
);
seam_core::seam!(
    /// `clause_selectivity(root, clause, varRelid, jointype, sjinfo)`.
    pub fn clause_selectivity<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clause: NodeId,
        var_relid: i32,
        jointype: i32,
        sjinfo: Option<&SpecialJoinInfo>,
    ) -> Selectivity
);
// NOTE: `estimate_num_groups` is owned by selfuncs.c and is declared once in
// `backend-utils-adt-selfuncs-seams`; consumers (including this crate's costsize
// code) call it through that seam. It is intentionally NOT redeclared here to
// avoid a redundant, divergent contract.
seam_core::seam!(
    /// `mergejoinscansel(root, clause, opfamily, cmptype, nulls_first)` —
    /// returns `(leftstartsel, leftendsel, rightstartsel, rightendsel)`.
    ///
    /// The owner (selfuncs.c) calls `examine_variable`, which needs the
    /// planner-run RTE store and the `&mut PlannerInfo` node arena; the cost
    /// call sites (`initial_cost_mergejoin` / `final_cost_mergejoin`) already
    /// thread both. Returns `PgResult` because the stats path can `ereport`.
    pub fn mergejoinscansel<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        clause: NodeId,
        opfamily: Oid,
        cmptype: i32,
        nulls_first: bool,
    ) -> PgResult<(Selectivity, Selectivity, Selectivity, Selectivity)>
);
seam_core::seam!(
    /// `label_sort_with_costsize` cost half (createplan.c:5553): re-figure a
    /// `Sort` plan node's cost via `cost_sort` over a dummy stack `Path`. The
    /// caller passes the `Sort`'s `disabled_nodes` and the lefttree's
    /// `total_cost`/`plan_rows`/`plan_width`; comparison_cost is 0.0 and
    /// `sort_mem` is the `work_mem` GUC (both read inside the owner). Returns the
    /// `(startup_cost, total_cost)` to copy onto the Sort node.
    pub fn cost_sort_label(
        root: &mut PlannerInfo,
        input_disabled_nodes: i32,
        input_total_cost: Cost,
        input_rows: f64,
        input_width: i32,
        limit_tuples: f64,
    ) -> (Cost, Cost)
);
seam_core::seam!(
    /// `label_incrementalsort_with_costsize` cost half (createplan.c:5581):
    /// re-figure an `IncrementalSort` plan node's cost via `cost_incremental_sort`
    /// over a dummy stack `Path`. Returns `(startup_cost, total_cost)`.
    pub fn cost_incremental_sort_label<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        pathkeys: &[types_pathnodes::PathKey],
        n_presorted_cols: i32,
        input_disabled_nodes: i32,
        input_startup_cost: Cost,
        input_total_cost: Cost,
        input_rows: f64,
        input_width: i32,
        limit_tuples: f64,
    ) -> PgResult<(Cost, Cost)>
);
seam_core::seam!(
    /// `estimate_hash_bucket_stats(root, hashkey, nbuckets, &mcvfreq, &bucketsize)`
    /// — returns `(mcvfreq, bucketsize)`.
    ///
    /// The owner (selfuncs.c) calls `examine_variable`, which needs the
    /// planner-run RTE store and the `&mut PlannerInfo` node arena; the cost
    /// call site (`final_cost_hashjoin`) already threads both. Returns
    /// `PgResult` because the stats path can `ereport`.
    pub fn estimate_hash_bucket_stats<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        hashkey: NodeId,
        nbuckets: f64,
    ) -> PgResult<(Selectivity, Selectivity)>
);
seam_core::seam!(
    /// `estimate_multivariate_bucketsize(root, inner_rel, hashclauses, &out)` —
    /// returns `(bucketsize, remaining_clause_handles)`.
    pub fn estimate_multivariate_bucketsize(
        root: &PlannerInfo,
        inner_rel: RelId,
        hashclauses: &[RinfoId],
    ) -> (Selectivity, alloc::vec::Vec<RinfoId>)
);
seam_core::seam!(
    /// `clauses.c:expression_returns_set_rows(root, (Node *) expr)`.
    pub fn expression_returns_set_rows(root: &PlannerInfo, node: NodeId) -> f64
);

/* --- the heterogeneous-node clause-cost walker (clauses.c +
 *     pg_proc.procost catalog). `cost_qual_eval` recursion is routed whole
 *     through this single-node walker; the list wrapper is in-crate. ----- */
seam_core::seam!(
    /// `cost_qual_eval_walker((Node *) qual, &context)` over a single node.
    /// Returns the `(startup, per_tuple)` cost contributed by this node and its
    /// descendants. Crosses into `add_function_cost` (`pg_proc.procost`).
    pub fn cost_qual_eval_walker(root: &PlannerInfo, node: NodeId) -> (Cost, Cost)
);
seam_core::seam!(
    /// `add_function_cost(root, funcid, node, &cost)` — accumulate `pg_proc.procost`.
    /// Returns the `(startup, per_tuple)` to add.
    pub fn add_function_cost(root: &PlannerInfo, funcid: Oid, node: Option<NodeId>) -> (Cost, Cost)
);

/* --- catalog / type-width reads (lsyscache.c) --------------------------- */
seam_core::seam!(
    /// `get_typavgwidth(typid, typmod)`.
    pub fn get_typavgwidth(typid: Oid, typmod: i32) -> i32
);
seam_core::seam!(
    /// `get_attavgwidth(reloid, attnum)`.
    pub fn get_attavgwidth(reloid: Oid, attnum: i16) -> i32
);
seam_core::seam!(
    /// `get_relation_data_width(reloid, attr_widths)` (plancat.c). C passes a
    /// base-shifted pointer `rel->attr_widths - rel->min_attr` so the callee
    /// reads `attr_widths[attno]` by 1-based attno; the value model can't forge a
    /// negative-offset slice, so the caller's `rel->attr_widths` and its
    /// `min_attr` are passed and the callee indexes `attr_widths[attno -
    /// min_attr]`. `min_attr == 1` with an empty slice means "no cache" (C NULL).
    pub fn get_relation_data_width(reloid: Oid, attr_widths: &[i32], min_attr: i16) -> u32
);
seam_core::seam!(
    /// `exprType((Node *) expr)` (nodeFuncs.c).
    pub fn expr_type(root: &PlannerInfo, node: NodeId) -> Oid
);
seam_core::seam!(
    /// `exprTypmod((Node *) expr)` (nodeFuncs.c).
    pub fn expr_typmod(root: &PlannerInfo, node: NodeId) -> i32
);
seam_core::seam!(
    /// `find_placeholder_info(root, phv)->ph_width` + the PHV's contained-expr
    /// eval cost. Returns `(ph_width, cost_startup, cost_per_tuple)`. `root` is
    /// `&mut` because `find_placeholder_info` builds the `PlaceHolderInfo` on
    /// first sight (placeholder.c).
    pub fn find_placeholder_info_width(root: &mut PlannerInfo, node: NodeId) -> (i32, Cost, Cost)
);

/* --- index-AM / tablespace / parallel-worker (plancat.c, spccache.c,
 *     allpaths.c) ------------------------------------------------------- */
seam_core::seam!(
    /// `get_tablespace_page_costs(tablespace, &spc_random, &spc_seq)`.
    pub fn get_tablespace_page_costs(spcid: Oid) -> TablespacePageCosts
);
seam_core::seam!(
    /// `cpu_operator_cost` (costsize.c GUC global) — read by `genericcostestimate`
    /// / `btcostestimate` (selfuncs.c) across the dependency cycle.
    pub fn cpu_operator_cost() -> f64
);
seam_core::seam!(
    /// `cpu_index_tuple_cost` (costsize.c GUC global) — read by
    /// `genericcostestimate` (selfuncs.c) across the dependency cycle.
    pub fn cpu_index_tuple_cost() -> f64
);
seam_core::seam!(
    /// `index_pages_fetched(tuples_fetched, pages, index_pages, root)`
    /// (costsize.c) — the Mackert and Lohman cache-effect page-fetch estimate.
    /// `genericcostestimate` (selfuncs.c) calls this across the dependency
    /// cycle (selfuncs sits below costsize).
    pub fn index_pages_fetched(
        tuples_fetched: f64,
        pages: u32,
        index_pages: f64,
        root: &PlannerInfo,
    ) -> f64
);
seam_core::seam!(
    /// `OidFunctionCall...` the index AM's `amcostestimate` (index AM dispatch).
    ///
    /// `run` threads the planner-run RTE/Query store the AM cost routine
    /// (`btcostestimate`/`genericcostestimate`, selfuncs.c) needs to reach
    /// `examine_variable` / `clauselist_selectivity`. `root` is `&mut` because
    /// those examine the variable stats (re-interning stripped exprs into the
    /// planner node arena).
    pub fn amcostestimate<'mcx>(
        root: &mut PlannerInfo,
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        path: PathId,
        loop_count: f64,
    ) -> AmCostEstimate
);
seam_core::seam!(
    /// `compute_parallel_worker(rel, heap_pages, index_pages, max_workers)`.
    pub fn compute_parallel_worker(
        root: &PlannerInfo,
        rel: RelId,
        heap_pages: f64,
        index_pages: f64,
        max_workers: i32,
    ) -> i32
);

/* --- executor helpers (nodeHash.c / tidbitmap.c / tuplesort.c /
 *     nodeMemoize.c / execAmi.c) --------------------------------------- */
seam_core::seam!(
    /// `tbm_calculate_entries(maxbytes)` (tidbitmap.c).
    pub fn tbm_calculate_entries(maxbytes: usize) -> f64
);
seam_core::seam!(
    /// `tuplesort_merge_order(allowedMem)` (tuplesort.c).
    pub fn tuplesort_merge_order(allowed_mem: i64) -> f64
);
seam_core::seam!(
    /// `hash_agg_entry_size(numTrans, tupleWidth, transitionSpace)` (nodeAgg.c).
    pub fn hash_agg_entry_size(num_trans: i32, tuple_width: f64, transition_space: u64) -> f64
);
seam_core::seam!(
    /// `hash_agg_set_limits(hashentrysize, numGroups, used_bits, ...)` (nodeAgg.c).
    pub fn hash_agg_set_limits(
        hashentrysize: f64,
        num_groups: f64,
        used_bits: i32,
    ) -> HashAggLimits
);
seam_core::seam!(
    /// `ExecChooseHashTableSize(ntuples, tupwidth, useskew, try_combined_hash_mem,
    /// parallel_workers, ...)` (nodeHash.c).
    pub fn exec_choose_hash_table_size(
        ntuples: f64,
        tupwidth: i32,
        useskew: bool,
        try_combined_hash_mem: bool,
        parallel_workers: i32,
    ) -> HashTableSize
);
seam_core::seam!(
    /// `ExecSupportsMarkRestore(path)` (execAmi.c) over a `PathId`.
    pub fn exec_supports_mark_restore(root: &PlannerInfo, path: PathId) -> bool
);
seam_core::seam!(
    /// `ExecEstimateCacheEntryOverheadBytes(ntuples)` (nodeMemoize.c).
    pub fn exec_estimate_cache_entry_overhead_bytes(ntuples: f64) -> f64
);

/* --- predicate / movability walkers (indxpath.c / equivclass.c /
 *     restrictinfo.c / initsplan.c) ------------------------------------ */
seam_core::seam!(
    /// `is_redundant_with_indexclauses(rinfo, indexclauses)` (indxpath.c). The
    /// `index_path` is identified by its `PathId`; the rinfo by `RinfoId`.
    pub fn is_redundant_with_indexclauses(
        root: &PlannerInfo,
        rinfo: RinfoId,
        index_path: PathId,
    ) -> bool
);
seam_core::seam!(
    /// `join_clause_is_movable_into(rinfo, currentrelids, current_and_required)`
    /// (restrictinfo.c). Identifies the clause by `RinfoId`, the rels by `RelId`.
    pub fn join_clause_is_movable_into(
        root: &PlannerInfo,
        rinfo: RinfoId,
        current_rel: RelId,
        join_rel: RelId,
    ) -> bool
);
seam_core::seam!(
    /// `init_dummy_sjinfo(left_relids, right_relids)` (joinrels.c) — build a
    /// JOIN_INNER dummy `SpecialJoinInfo` for the two rels (by `RelId`).
    pub fn init_dummy_sjinfo(root: &PlannerInfo, outer_rel: RelId, inner_rel: RelId) -> SpecialJoinInfo
);
seam_core::seam!(
    /// `tsm_uses_random_access(tsmhandler)` (tablesample method probe): true if
    /// `GetTsmRoutine(tsmhandler)->NextSampleBlock != NULL`.
    pub fn tsm_uses_random_access(tsmhandler: Oid) -> bool
);
seam_core::seam!(
    /// `estimate_array_length(root, arrayexpr)` (selfuncs.c).
    pub fn estimate_array_length(root: &PlannerInfo, node: NodeId) -> f64
);
seam_core::seam!(
    /// `bms_is_member(0, pull_varnos(root, node))` (var.c) — true iff the
    /// expression references a Var with `varno 0` (cost_incremental_sort).
    pub fn pull_varnos_contains_zero(root: &PlannerInfo, node: NodeId) -> bool
);
seam_core::seam!(
    /// `get_sortgrouplist_exprs(sgClauses, targetList)` (tlist.c) — the C reads
    /// `root->parse->targetList`; the owner resolves the SortGroupClause handles
    /// against it and returns the matched expr handles.
    pub fn get_sortgrouplist_exprs(
        root: &PlannerInfo,
        sgclauses: &[NodeId],
    ) -> alloc::vec::Vec<NodeId>
);

/* --- RTE / Query reads (parsenodes.h structs are opaque handles in the
 *     fabled `PlannerInfo`: `simple_rte_array: Vec<RangeTblEntryId>` and
 *     `parse: QueryId` have no resolver). Each focused seam returns exactly the
 *     value costsize.c reads from the RTE/Query, keeping the surrounding
 *     arithmetic in-crate. -------------------------------------------- */
seam_core::seam!(
    /// `rte->tablesample->tsmhandler` for the baserel's RTE (cost_samplescan).
    /// `run` is threaded so the owner can `planner_rt_fetch` the RTE's owned
    /// `tablesample` clause node (the same RTE-projection contract as
    /// `rte_relid` / `rte_functions_exprcost`).
    pub fn rte_tablesample_tsmhandler<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
    ) -> Oid
);
seam_core::seam!(
    /// `cost_qual_eval_node((Node *) rte->functions, root)` for the baserel's
    /// RTE (cost_functionscan) — the eval cost of the function exprs. `run` is
    /// threaded so the owner can `planner_rt_fetch` the RTE's owned funcexprs
    /// (the same RTE-projection contract as `rte_relid`).
    pub fn rte_functions_exprcost<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
    ) -> (Cost, Cost)
);
seam_core::seam!(
    /// `cost_qual_eval_node((Node *) rte->tablefunc, root)` (cost_tablefuncscan).
    pub fn rte_tablefunc_exprcost<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
    ) -> (Cost, Cost)
);
seam_core::seam!(
    /// `set_function_size_estimates`: the largest `expression_returns_set_rows`
    /// over `rte->functions`. `run` is threaded so the owner can
    /// `planner_rt_fetch` the RTE's owned funcexprs.
    pub fn rte_function_max_set_rows<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
    ) -> f64
);
seam_core::seam!(
    /// `rte->self_reference` (set_cte_size_estimates). `rel` is the `RelOptInfo`
    /// handle; the owner resolves it to the RT index (`rel->relid`) and fetches
    /// the RTE through the planner run, so `run` is threaded (matching the
    /// `backend-optimizer-rte-seams` RTE-projection contract).
    pub fn rte_cte_self_reference<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
    ) -> bool
);
seam_core::seam!(
    /// `rte->enrtuples` (set_namedtuplestore_size_estimates).
    pub fn rte_enrtuples(root: &PlannerInfo, rel: RelId) -> f64
);
seam_core::seam!(
    /// `rte->relid` (set_rel_width): the underlying table OID, 0 for a phony rel.
    ///
    /// `rel` is the `RelOptInfo` handle; the owner resolves it to the RT index
    /// (`rel->relid`) and fetches the RTE through the planner run, so `run` is
    /// threaded (matching the `backend-optimizer-rte-seams` RTE-projection
    /// contract — `planner_rt_fetch(run, root, rti)->relid`).
    pub fn rte_relid<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &PlannerInfo,
        rel: RelId,
    ) -> Oid
);
/// The WindowClause-derived values `cost_windowagg` needs but cannot read from
/// the fabled arena (the `WindowClause` carries a lifetime and is not arena-
/// resolvable, and `get_windowclause_startup_tuples` also reads
/// `root->parse->targetList`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WindowClauseCostInfo {
    /// `list_length(winclause->partitionClause)`.
    pub num_part_cols: i32,
    /// `list_length(winclause->orderClause)`.
    pub num_order_cols: i32,
    /// `get_windowclause_startup_tuples(root, wc, input_tuples)`.
    pub startup_tuples: f64,
}
seam_core::seam!(
    /// Per-WindowFunc cost contribution (cost_windowagg inner loop): returns
    /// `(startup_contribution, per_input_row_cost)` for one WindowFunc =
    /// `add_function_cost(winfnoid)` + `cost_qual_eval_node(wfunc->args)` +
    /// `cost_qual_eval_node(wfunc->aggfilter)`. The args/aggfilter are inline
    /// `Expr` values on the WindowFunc (no `NodeId`), so the per-func cost is
    /// computed by the owner from the WindowFunc node handle. `&mut root`
    /// because the owner re-interns the WindowFunc's args/aggfilter into the
    /// arena to run `cost_qual_eval_node`. `run` threads the planner mcx so the
    /// owner can deep-copy the WindowFunc args (which may carry an `Aggref`, as in
    /// `SUM(SUM(x)) OVER ...`) via `clone_in` — a plain `.clone()` panics.
    pub fn windowfunc_cost<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        wfunc: NodeId,
    ) -> (Cost, Cost)
);
seam_core::seam!(
    /// The WindowClause column counts + startup-tuples estimate — needs the
    /// WindowClause fields + `root->parse->targetList`, neither reachable in the
    /// fabled arena (winclause carried as a `NodeId`; `parse` is opaque).
    ///
    /// `run` + `&mut root` thread `get_windowclause_startup_tuples`'s
    /// `estimate_num_groups` call (examines `pg_statistic` through the
    /// [`PlannerRun`] and re-interns stripped grouping expressions into the
    /// arena), so the seam is fallible.
    pub fn windowclause_cost_info<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        winclause: NodeId,
        input_tuples: f64,
    ) -> PgResult<WindowClauseCostInfo>
);

seam_core::seam!(
    /// `get_foreign_key_join_selectivity(root, outer_relids, inner_relids,
    /// sjinfo, &restrictlist)` (costsize.c:5650). The `root->fkey_list`
    /// `ForeignKeyOptInfo` structs are opaque `NodeId` handles in the fabled
    /// arena (no resolver), so the whole FK-matching pass — including the
    /// removal of FK-matched clauses from the restrictlist — is routed to the
    /// owner. Returns `(fkselec, remaining_clause_handles)`.
    pub fn get_foreign_key_join_selectivity<'mcx>(
        run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        outer_rel: RelId,
        inner_rel: RelId,
        sjinfo: &SpecialJoinInfo,
        restrictlist: &[RinfoId],
    ) -> (Selectivity, alloc::vec::Vec<RinfoId>)
);

seam_core::seam!(
    /// `equal((Node *) a, (Node *) b)` over two arena expr handles (nodeFuncs.c).
    /// Used by the FK-EC-member identity recovery in
    /// `get_foreign_key_join_selectivity`.
    pub fn equal_nodes(root: &PlannerInfo, a: NodeId, b: NodeId) -> bool
);

seam_core::seam!(
    /// `find_derived_clause_for_ec_member(root, ec, em)` (equivclass.c:2804).
    /// Owned by equivclass.c (which installs the body); consumed by
    /// `get_foreign_key_join_selectivity`'s `ec_has_const` double-count
    /// correction. Returns the derived `var = const` RestrictInfo for `em`, if
    /// one was generated.
    pub fn find_derived_clause_for_ec_member(
        root: &mut PlannerInfo,
        ec: types_pathnodes::EcId,
        em: types_pathnodes::EmId,
    ) -> Option<RinfoId>
);

/// Re-exported so installers can name the carrier types without importing the
/// optimizer crate.
pub use types_pathnodes::QualCost;
