// Seam installation for backend-optimizer-path-costsize.
//
// Installs every inward seam this unit owns:
//   * costsize-seams: clamp_row_est, clamp_cardinality_to_long.
//   * pathnode-seams: cpu_tuple_cost, cpu_operator_cost, enable_hashagg,
//     sizeof_minimal_tuple_header, cost_qual_eval, expression_returns_set_rows,
//     and the full cost-estimator family (cost_seqscan .. final_cost_hashjoin).
//   * joinpath-seams: initial_cost_{nestloop,mergejoin,hashjoin},
//     compute_semi_anti_join_factors.
//
// It wires the pathnode-seams `work_mem` alias to the canonical GUC getter
// (value owned by backend-utils-init-small `globals::work_mem`; delegation, not
// a value owned here). It does NOT install get_hash_memory_limit (owned by
// nodeHash; consumed via `ps::`), nor compare_path_costs (pathnode's). The cost
// GUC getters proper (cpu_tuple_cost, ...) are costsize.c globals backed by
// module statics here.

// NOTE: this file is `include!`d into lib.rs, so it shares lib.rs's imports
// (`cz`, `ps`, `PlannerInfo`, `PathId`, `RelId`, `QualCost`, ...). Only the
// joinpath-seams alias is added here.
use backend_optimizer_path_joinpath_seams as jp;
use types_pathnodes::NodeId;

/// `MAXALIGN(SizeofMinimalTupleHeader)` (htup_details.h). The minimal-tuple
/// header is `offsetof(MinimalTupleData, t_bits)`; on supported targets this is
/// `SizeofHeapTupleHeader - MINIMAL_TUPLE_OFFSET`, then MAXALIGN'd. C uses the
/// MAXALIGN'd value (23 - 6 = 17, MAXALIGN -> 24? no: t_len(4) precedes header).
/// PG's `SizeofMinimalTupleHeader` == `offsetof(MinimalTupleData, t_bits)` == 23
/// - MINIMAL_TUPLE_OFFSET(8)... we reproduce the canonical MAXALIGN value used
/// by the executor sizing: `MAXALIGN(offsetof(MinimalTupleData, t_bits))`.
const SIZEOF_MINIMAL_TUPLE_HEADER: usize = {
    // SizeofMinimalTupleHeader = offsetof(MinimalTupleData, t_bits).
    // MinimalTupleData = { uint32 t_len; char mt_padding[..]; uint16 t_infomask2;
    //   uint16 t_infomask; uint8 t_hoff; bits8 t_bits[]; } with t_bits at the
    // same offset as a HeapTupleHeader's t_bits minus MINIMAL_TUPLE_PADDING.
    // The value the cost model needs is MAXALIGN of that header; on LP64 it is
    // MAXALIGN(crate::SizeofHeapTupleHeader - 8 + 8) == MAXALIGN(23).
    let off = crate::SizeofHeapTupleHeader; // 23
    (off + 7) & !7
};

pub fn init_seams() {
    /* ---- costsize.c-owned scan/join `enable_*` cost GUC slots ---------- */
    crate::guc::install_enable_gucs();

    /* ---- costsize-seams (this unit's own clamp helpers) ---------------- */
    cz::clamp_row_est::set(crate::clamp_row_est);
    cz::clamp_cardinality_to_long::set(crate::clamp_cardinality_to_long);
    // costsize.c genuinely owns cost_bitmap_tree_node and the
    // enable_indexonlyscan GUC; indxpath.c reaches them through these seams.
    cz::cost_bitmap_tree_node::set(crate::scans::cost_bitmap_tree_node);
    cz::cost_sort_label::set(crate::cost_sort_label);
    cz::cost_incremental_sort_label::set(crate::cost_incremental_sort_label);
    cz::enable_indexonlyscan::set(|| crate::ENABLE_INDEXONLYSCAN);

    // `get_tablespace_page_costs(spcid)` (spccache.c). costsize.c owns the
    // `random_page_cost`/`seq_page_cost` GUC defaults the lookup falls back to
    // (its module globals); the lookup itself + `MyDatabaseTableSpace` live in
    // spccache/init. Install the cost seam here, delegating to spccache with
    // those defaults and the init-small `MyDatabaseTableSpace` global.
    cz::get_tablespace_page_costs::set(|spcid| {
        let defaults = backend_utils_cache_spccache::PageCostDefaults {
            random_page_cost: crate::RANDOM_PAGE_COST,
            seq_page_cost: crate::SEQ_PAGE_COST,
        };
        let my_db_ts = backend_utils_init_small_seams::my_database_table_space::call();
        let mut spc_random_page_cost = 0.0f64;
        let mut spc_seq_page_cost = 0.0f64;
        backend_utils_cache_spccache::get_tablespace_page_costs(
            spcid,
            my_db_ts,
            &defaults,
            Some(&mut spc_random_page_cost),
            Some(&mut spc_seq_page_cost),
        )
        .expect("get_tablespace_page_costs");
        cz::TablespacePageCosts { spc_random_page_cost, spc_seq_page_cost }
    });
    cz::index_pages_fetched::set(|tuples_fetched, pages, index_pages, root| {
        crate::index_pages_fetched(tuples_fetched, pages, index_pages, root)
    });
    cz::cpu_operator_cost::set(crate::cpu_operator_cost);
    cz::cpu_index_tuple_cost::set(crate::cpu_index_tuple_cost);

    /* ---- other costsize.c-owned GUC getters consumed elsewhere -------- */
    // `enable_partitionwise_join` (costsize.c GUC, default OFF) read by
    // relnode.c/joinrels.c through the relnode-ext consumer-side seam crate
    // (no owner directory; costsize installs it as the GUC owner).
    backend_optimizer_util_relnode_ext_seams::enable_partitionwise_join::set(
        || crate::ENABLE_PARTITIONWISE_JOIN,
    );
    // `cpu_operator_cost` (costsize.c GUC) — plancache reads it through the
    // planner-pc seam crate; planner.c (the rest of that crate's seams) is
    // unported, but costsize.c owns this GUC global, so install it here. The
    // seam signature is `() -> PgResult<f64>` (infallible read).
    backend_optimizer_plan_planner_pc_seams::cpu_operator_cost::set(
        || Ok(crate::CPU_OPERATOR_COST),
    );

    /* ---- pathnode-seams: cost GUC getters + sizing helpers owned here -- */
    ps::cpu_tuple_cost::set(|| crate::CPU_TUPLE_COST);
    ps::cpu_operator_cost::set(|| crate::CPU_OPERATOR_COST);
    ps::enable_hashagg::set(|| crate::ENABLE_HASHAGG);
    ps::sizeof_minimal_tuple_header::set(|| SIZEOF_MINIMAL_TUPLE_HEADER);
    // `work_mem` (utils/guc.c GUC, default 4 MB) read by the cost estimators
    // (`compute_bitmap_pages`, sort/hash sizing). The value is owned by the GUC
    // global in backend-utils-init-small (`globals::work_mem`), exposed through
    // its getter seam; this wires the pathnode-seams alias the cost code calls to
    // that canonical getter (delegation, not a duplicate value here).
    ps::work_mem::set(|| backend_utils_init_small_seams::work_mem::call());

    /* ---- costsize-seams: the cost_qual_eval per-node recursion --------- */
    // `cost_qual_eval` (in-crate) routes the whole walk through this single-node
    // walker (costsize.c:4796). This unit owns it.
    cz::cost_qual_eval_walker::set(crate::qualcost::cost_qual_eval_walker);

    /* ---- pathnode-seams: cost_qual_eval + expression_returns_set_rows -- */
    ps::cost_qual_eval::set(cost_qual_eval_seam);
    ps::expression_returns_set_rows::set(expression_returns_set_rows_seam);

    /* ---- pathnode-seams: the cost estimators ------------------------- */
    ps::cost_seqscan::set(crate::scans::cost_seqscan);
    ps::cost_samplescan::set(crate::scans::cost_samplescan);
    ps::cost_index::set(crate::scans::cost_index);
    ps::cost_bitmap_heap_scan::set(crate::scans::cost_bitmap_heap_scan);
    ps::cost_bitmap_and_node::set(crate::scans::cost_bitmap_and_node);
    ps::cost_bitmap_or_node::set(crate::scans::cost_bitmap_or_node);
    ps::cost_tidscan::set(cost_tidscan_seam);
    ps::cost_tidrangescan::set(cost_tidrangescan_seam);
    ps::cost_subqueryscan::set(crate::scans::cost_subqueryscan);
    ps::cost_functionscan::set(crate::scans::cost_functionscan);
    ps::cost_tablefuncscan::set(crate::scans::cost_tablefuncscan);
    ps::cost_valuesscan::set(crate::scans::cost_valuesscan);
    ps::cost_ctescan::set(crate::scans::cost_ctescan);
    ps::cost_namedtuplestorescan::set(crate::scans::cost_namedtuplestorescan);
    ps::cost_resultscan::set(crate::scans::cost_resultscan);
    ps::cost_append::set(crate::cost_append);
    ps::cost_merge_append::set(cost_merge_append_seam);
    ps::cost_material::set(crate::cost_material);
    ps::cost_gather::set(cost_gather_seam);
    ps::cost_gather_merge::set(crate::cost_gather_merge);
    ps::cost_sort::set(cost_sort_seam);
    ps::cost_incremental_sort::set(cost_incremental_sort_seam);
    ps::cost_group::set(crate::exprcost::cost_group);
    ps::cost_agg::set(cost_agg_seam);
    ps::cost_windowagg::set(cost_windowagg_seam);
    ps::cost_recursive_union::set(crate::cost_recursive_union);
    ps::final_cost_nestloop::set(crate::joins::final_cost_nestloop);
    ps::final_cost_mergejoin::set(crate::joins::final_cost_mergejoin);
    ps::final_cost_hashjoin::set(crate::joins::final_cost_hashjoin);

    /* ---- joinpath-seams: the preliminary join cost estimators -------- */
    jp::initial_cost_nestloop::set(crate::joins::initial_cost_nestloop);
    jp::initial_cost_mergejoin::set(crate::joins::initial_cost_mergejoin);
    jp::initial_cost_hashjoin::set(crate::joins::initial_cost_hashjoin);
    jp::compute_semi_anti_join_factors::set(crate::joins::compute_semi_anti_join_factors);
}

/* --------------------------------------------------------------------------
 * Thin adapters where the seam signature differs cosmetically from the crate
 * function (slice vs trailing args), so the installed `fn` pointer matches.
 * ------------------------------------------------------------------------ */

fn cost_qual_eval_seam(root: &PlannerInfo, quals: &[NodeId]) -> QualCost {
    crate::cost_qual_eval(root, quals)
}

fn expression_returns_set_rows_seam(root: &PlannerInfo, node: NodeId) -> f64 {
    cz::expression_returns_set_rows::call(root, node)
}

fn cost_tidscan_seam(root: &mut PlannerInfo, path: PathId, rel: RelId, tidquals: &[NodeId]) {
    crate::scans::cost_tidscan(root, path, rel, tidquals);
}
fn cost_tidrangescan_seam<'mcx>(
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path: PathId,
    rel: RelId,
    tidrangequals: &[NodeId],
) {
    crate::scans::cost_tidrangescan(run, root, path, rel, tidrangequals);
}

fn cost_merge_append_seam(
    root: &mut PlannerInfo,
    path: PathId,
    pathkeys: &[types_pathnodes::PathKey],
    n_streams: i32,
    input_disabled_nodes: i32,
    input_startup_cost: types_core::primitive::Cost,
    input_total_cost: types_core::primitive::Cost,
    tuples: f64,
) {
    crate::cost_merge_append(
        root,
        path,
        pathkeys,
        n_streams,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        tuples,
    );
}

fn cost_gather_seam(
    root: &mut PlannerInfo,
    path: PathId,
    rel: RelId,
    rows: Option<f64>,
) {
    crate::cost_gather(root, path, rel, rows);
}

fn cost_sort_seam(
    root: &mut PlannerInfo,
    path: PathId,
    pathkeys: &[types_pathnodes::PathKey],
    input_disabled_nodes: i32,
    input_cost: types_core::primitive::Cost,
    tuples: f64,
    width: i32,
    comparison_cost: types_core::primitive::Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    crate::cost_sort(
        root,
        path,
        pathkeys,
        input_disabled_nodes,
        input_cost,
        tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );
}

fn cost_incremental_sort_seam<'mcx>(
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path: PathId,
    pathkeys: &[types_pathnodes::PathKey],
    presorted_keys: i32,
    input_disabled_nodes: i32,
    input_startup_cost: types_core::primitive::Cost,
    input_total_cost: types_core::primitive::Cost,
    input_tuples: f64,
    width: i32,
    comparison_cost: types_core::primitive::Cost,
    sort_mem: i32,
    limit_tuples: f64,
) -> types_error::PgResult<()> {
    crate::cost_incremental_sort(
        run,
        root,
        path,
        pathkeys,
        presorted_keys,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    )
}

fn cost_agg_seam<'mcx>(
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path: PathId,
    aggstrategy: types_pathnodes::AggStrategy,
    aggcosts: Option<ps::AggClauseCostsLite>,
    num_group_cols: i32,
    num_groups: f64,
    quals: &[NodeId],
    input_disabled_nodes: i32,
    input_startup_cost: types_core::primitive::Cost,
    input_total_cost: types_core::primitive::Cost,
    input_tuples: f64,
    input_width: i32,
) {
    crate::exprcost::cost_agg(
        run,
        root,
        path,
        aggstrategy,
        aggcosts,
        num_group_cols,
        num_groups,
        quals,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        input_width,
    );
}

fn cost_windowagg_seam(
    root: &mut PlannerInfo,
    path: PathId,
    window_funcs: &[NodeId],
    winclause: NodeId,
    input_disabled_nodes: i32,
    input_startup_cost: types_core::primitive::Cost,
    input_total_cost: types_core::primitive::Cost,
    input_tuples: f64,
) {
    crate::exprcost::cost_windowagg(
        root,
        path,
        window_funcs,
        winclause,
        input_disabled_nodes,
        input_startup_cost,
        input_total_cost,
        input_tuples,
    );
}
