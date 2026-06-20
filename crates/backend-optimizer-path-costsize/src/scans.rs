//! The `cost_*scan` family + the bitmap-scan family of costsize.c.

use alloc::vec::Vec;

use types_core::primitive::{Cost, Selectivity};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    IndexPath, NodeId, ParamPathInfo, Path, PathId, PathNode, PlannerInfo, RelId, RinfoId,
};

use backend_optimizer_path_costsize_seams as cz;

use crate::{
    ceil, clamp_row_est, cost_qual_eval, cpu_operator_cost,
    cpu_tuple_cost, get_parallel_divisor,
    index_pages_fetched, max_parallel_workers_per_gather, rinfo_clause_nodes, sqrt, work_mem, Max,
    Min, ENABLE_BITMAPSCAN, ENABLE_INDEXSCAN, ENABLE_SEQSCAN, RTE_CTE, RTE_FUNCTION,
    RTE_NAMEDTUPLESTORE, RTE_RELATION, RTE_RESULT, RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};

/* ==========================================================================
 * cost_seqscan (costsize.c:294-360)
 * ========================================================================== */

/// `cost_seqscan` — fills a plain scan `Path` (by `PathId`).
pub fn cost_seqscan(root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let cpu_run_cost: Cost;
    let disk_run_cost: Cost;
    let cpu_per_tuple: Cost;

    let (relid, rtekind, reltablespace, pages, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.tuples,
            baserel.rows,
            baserel.baserestrictcost,
        )
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_RELATION);

    let param_info = path_param_info(root, path_id);

    // Mark the path with the correct row estimate.
    let mut rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let spc_seq_page_cost = cz::get_tablespace_page_costs::call(reltablespace).spc_seq_page_cost;
    disk_run_cost = spc_seq_page_cost * pages as f64;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    let mut cpu_run_cost_local = cpu_per_tuple * tuples;

    // tlist eval costs are paid per output row, not per tuple scanned.
    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_seqscan");
    startup_cost += pt_startup;
    cpu_run_cost_local += pt_per_tuple * rows;

    let parallel_workers = root.path(path_id).base().parallel_workers;
    if parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(root.path(path_id).base());
        cpu_run_cost_local /= parallel_divisor;
        rows = clamp_row_est(rows / parallel_divisor);
    }
    cpu_run_cost = cpu_run_cost_local;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = if ENABLE_SEQSCAN() { 0 } else { 1 };
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

/* ==========================================================================
 * cost_samplescan (costsize.c:369-433)
 * ========================================================================== */

/// `cost_samplescan` — fills a sample-scan `Path` (by `PathId`).
pub fn cost_samplescan<'mcx>(
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    rel: RelId,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, reltablespace, pages, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.tuples,
            baserel.rows,
            baserel.baserestrictcost,
        )
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_RELATION);

    // rte->tablesample->tsmhandler is unreachable in the fabled arena → seam.
    let tsmhandler = cz::rte_tablesample_tsmhandler::call(run, root, rel);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let spc = cz::get_tablespace_page_costs::call(reltablespace);
    let spc_page_cost = if cz::tsm_uses_random_access::call(tsmhandler) {
        spc.spc_random_page_cost
    } else {
        spc.spc_seq_page_cost
    };

    run_cost += spc_page_cost * pages as f64;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_samplescan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * cost_index (costsize.c:559-831)
 * ========================================================================== */

/// `extract_nonindex_conditions` (costsize.c:849).
fn extract_nonindex_conditions(
    root: &PlannerInfo,
    qual_clauses: &[RinfoId],
    index_path: PathId,
    out: &mut Vec<NodeId>,
) {
    for id in qual_clauses {
        let rinfo = root.rinfo(*id);
        if rinfo.pseudoconstant {
            continue;
        }
        if cz::is_redundant_with_indexclauses::call(root, *id, index_path) {
            continue;
        }
        out.push(rinfo.clause);
    }
}

/// `cost_index` (costsize.c:559) — fills an `IndexPath` (by `PathId`).
pub fn cost_index<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    path_id: PathId,
    loop_count: f64,
    partial_path: bool,
) {
    let (baserel_id, indexonly, index_total_pages, indrestrictinfo) = {
        let ip = expect_index_path(root, path_id);
        let index = ip
            .indexinfo
            .as_ref()
            .expect("cost_index: indexinfo must be set");
        (
            index.rel.expect("cost_index: index.rel must be set"),
            ip.path.pathtype == types_nodes::nodes::T_IndexOnlyScan,
            index.pages,
            index.indrestrictinfo.clone(),
        )
    };

    {
        let baserel = root.rel(baserel_id);
        debug_assert!(baserel.relid > 0);
        debug_assert!(baserel.rtekind == RTE_RELATION);
    }

    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let mut cpu_run_cost: Cost = 0.0;
    let csquared: f64;
    let min_IO_cost: Cost;
    let max_IO_cost: Cost;
    let cpu_per_tuple: Cost;
    let tuples_fetched: f64;
    let mut pages_fetched: f64;
    let mut rand_heap_pages: f64;
    let index_pages: f64;

    // Mark the path with the correct row estimate + identify qpquals.
    let mut qpquals: Vec<NodeId> = Vec::new();
    let param = path_param_info(root, path_id);
    let new_rows;
    if let Some(pi) = &param {
        new_rows = pi.ppi_rows;
        let ppi_clauses = pi.ppi_clauses.clone();
        extract_nonindex_conditions(root, &indrestrictinfo, path_id, &mut qpquals);
        extract_nonindex_conditions(root, &ppi_clauses, path_id, &mut qpquals);
    } else {
        new_rows = root.rel(baserel_id).rows;
        extract_nonindex_conditions(root, &indrestrictinfo, path_id, &mut qpquals);
    }
    set_index_rows(root, path_id, new_rows);

    set_index_disabled(root, path_id, if ENABLE_INDEXSCAN() { 0 } else { 1 });

    // amcostestimate (index-AM cost callback).
    let am = cz::amcostestimate::call(root, run, path_id, loop_count);
    let index_startup_cost = am.index_startup_cost;
    let index_total_cost = am.index_total_cost;
    let index_selectivity = am.index_selectivity;
    let index_correlation = am.index_correlation;
    index_pages = am.index_pages;

    if let PathNode::IndexPath(ip) = root.path_mut(path_id) {
        ip.indextotalcost = index_total_cost;
        ip.indexselectivity = index_selectivity;
    }

    startup_cost += index_startup_cost;
    run_cost += index_total_cost - index_startup_cost;

    let (baserel_tuples, baserel_pages, baserel_allvisfrac, reltablespace) = {
        let baserel = root.rel(baserel_id);
        (baserel.tuples, baserel.pages, baserel.allvisfrac, baserel.reltablespace)
    };

    tuples_fetched = clamp_row_est(index_selectivity * baserel_tuples);

    let spc = cz::get_tablespace_page_costs::call(reltablespace);
    let spc_random_page_cost = spc.spc_random_page_cost;
    let spc_seq_page_cost = spc.spc_seq_page_cost;

    if loop_count > 1.0 {
        pages_fetched = index_pages_fetched(
            tuples_fetched * loop_count,
            baserel_pages,
            index_total_pages as f64,
            root,
        );
        if indexonly {
            pages_fetched = ceil(pages_fetched * (1.0 - baserel_allvisfrac));
        }
        rand_heap_pages = pages_fetched;
        max_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;

        pages_fetched = ceil(index_selectivity * baserel_pages as f64);
        pages_fetched = index_pages_fetched(
            pages_fetched * loop_count,
            baserel_pages,
            index_total_pages as f64,
            root,
        );
        if indexonly {
            pages_fetched = ceil(pages_fetched * (1.0 - baserel_allvisfrac));
        }
        min_IO_cost = (pages_fetched * spc_random_page_cost) / loop_count;
    } else {
        pages_fetched =
            index_pages_fetched(tuples_fetched, baserel_pages, index_total_pages as f64, root);
        if indexonly {
            pages_fetched = ceil(pages_fetched * (1.0 - baserel_allvisfrac));
        }
        rand_heap_pages = pages_fetched;

        max_IO_cost = pages_fetched * spc_random_page_cost;

        pages_fetched = ceil(index_selectivity * baserel_pages as f64);
        if indexonly {
            pages_fetched = ceil(pages_fetched * (1.0 - baserel_allvisfrac));
        }
        if pages_fetched > 0.0 {
            let mut m = spc_random_page_cost;
            if pages_fetched > 1.0 {
                m += (pages_fetched - 1.0) * spc_seq_page_cost;
            }
            min_IO_cost = m;
        } else {
            min_IO_cost = 0.0;
        }
    }

    if partial_path {
        if indexonly {
            rand_heap_pages = -1.0;
        }
        let workers = cz::compute_parallel_worker::call(
            root,
            baserel_id,
            rand_heap_pages,
            index_pages,
            max_parallel_workers_per_gather(),
        );
        set_index_parallel_workers(root, path_id, workers);

        if workers <= 0 {
            return;
        }
        if let PathNode::IndexPath(ip) = root.path_mut(path_id) {
            ip.path.parallel_aware = true;
        }
    }

    csquared = index_correlation * index_correlation;
    run_cost += max_IO_cost + csquared * (min_IO_cost - max_IO_cost);

    let qpqual_cost = cost_qual_eval(root, &qpquals);
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    cpu_run_cost += cpu_per_tuple * tuples_fetched;

    let path_rows = root.path(path_id).base().rows;
    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_index");
    startup_cost += pt_startup;
    cpu_run_cost += pt_per_tuple * path_rows;

    let parallel_workers = root.path(path_id).base().parallel_workers;
    if parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(root.path(path_id).base());
        let new = clamp_row_est(path_rows / parallel_divisor);
        set_index_rows(root, path_id, new);
        cpu_run_cost /= parallel_divisor;
    }

    run_cost += cpu_run_cost;

    let p = root.path_mut(path_id).base_mut();
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * bitmap scan family (costsize.c:972-1247, 6513)
 * ========================================================================== */

/// `get_indexpath_pages` (costsize.c:972).
fn get_indexpath_pages(root: &PlannerInfo, bitmapqual: PathId) -> f64 {
    match root.path(bitmapqual) {
        PathNode::BitmapAndPath(apath) => {
            let mut result = 0.0;
            for sub in &apath.bitmapquals {
                result += get_indexpath_pages(root, *sub);
            }
            result
        }
        PathNode::BitmapOrPath(opath) => {
            let mut result = 0.0;
            for sub in &opath.bitmapquals {
                result += get_indexpath_pages(root, *sub);
            }
            result
        }
        PathNode::IndexPath(ipath) => ipath
            .indexinfo
            .as_ref()
            .expect("get_indexpath_pages: IndexPath.indexinfo must be set")
            .pages as f64,
        other => panic!(
            "backend-optimizer-path-costsize::get_indexpath_pages: unrecognized node type: {}",
            other.base().type_
        ),
    }
}

/// `cost_bitmap_tree_node` (costsize.c:1121) — returns `(cost, selec)`.
pub fn cost_bitmap_tree_node(root: &PlannerInfo, path: PathId) -> (Cost, Selectivity) {
    match root.path(path) {
        PathNode::IndexPath(ip) => {
            let cost = ip.indextotalcost + 0.1 * cpu_operator_cost() * ip.path.rows;
            (cost, ip.indexselectivity)
        }
        PathNode::BitmapAndPath(ap) => (ap.path.total_cost, ap.bitmapselectivity),
        PathNode::BitmapOrPath(op) => (op.path.total_cost, op.bitmapselectivity),
        other => panic!(
            "backend-optimizer-path-costsize::cost_bitmap_tree_node: unrecognized node type: {}",
            other.base().type_
        ),
    }
}

/// `cost_bitmap_and_node` (costsize.c:1164).
pub fn cost_bitmap_and_node(root: &mut PlannerInfo, path: PathId) {
    let bitmapquals = match root.path(path) {
        PathNode::BitmapAndPath(ap) => ap.bitmapquals.clone(),
        _ => panic!(
            "backend-optimizer-path-costsize::cost_bitmap_and_node: path is not a BitmapAndPath"
        ),
    };

    let mut total_cost: Cost = 0.0;
    let mut selec: Selectivity = 1.0;
    let cpu_op = cpu_operator_cost();
    for (i, sub) in bitmapquals.iter().enumerate() {
        let (sub_cost, subselec) = cost_bitmap_tree_node(root, *sub);
        selec *= subselec;
        total_cost += sub_cost;
        if i != 0 {
            total_cost += 100.0 * cpu_op;
        }
    }

    if let PathNode::BitmapAndPath(ap) = root.path_mut(path) {
        ap.bitmapselectivity = selec;
        ap.path.rows = 0.0;
        ap.path.disabled_nodes = 0;
        ap.path.startup_cost = total_cost;
        ap.path.total_cost = total_cost;
    }
}

/// `cost_bitmap_or_node` (costsize.c:1209).
pub fn cost_bitmap_or_node(root: &mut PlannerInfo, path: PathId) {
    let bitmapquals = match root.path(path) {
        PathNode::BitmapOrPath(op) => op.bitmapquals.clone(),
        _ => panic!(
            "backend-optimizer-path-costsize::cost_bitmap_or_node: path is not a BitmapOrPath"
        ),
    };

    let mut total_cost: Cost = 0.0;
    let mut selec: Selectivity = 0.0;
    let cpu_op = cpu_operator_cost();
    for (i, sub) in bitmapquals.iter().enumerate() {
        let (sub_cost, subselec) = cost_bitmap_tree_node(root, *sub);
        selec += subselec;
        total_cost += sub_cost;
        let is_index = matches!(root.path(*sub), PathNode::IndexPath(_));
        if i != 0 && !is_index {
            total_cost += 100.0 * cpu_op;
        }
    }

    if let PathNode::BitmapOrPath(op) = root.path_mut(path) {
        op.bitmapselectivity = Min(selec, 1.0);
        op.path.rows = 0.0;
        op.path.startup_cost = total_cost;
        op.path.total_cost = total_cost;
    }
}

/// `compute_bitmap_pages` (costsize.c:6513) — returns
/// `(pages_fetched, index_total_cost, tuples_fetched)`.
pub fn compute_bitmap_pages(
    root: &PlannerInfo,
    baserel: RelId,
    bitmapqual: PathId,
    loop_count: f64,
) -> (f64, Cost, f64) {
    let (rel_pages, rel_tuples) = {
        let r = root.rel(baserel);
        (r.pages, r.tuples)
    };
    let index_total_cost: Cost;
    let index_selectivity: Selectivity;
    let T: f64;
    let mut pages_fetched: f64;
    let mut tuples_fetched: f64;
    let heap_pages: f64;
    let maxentries: f64;

    let (c, s) = cost_bitmap_tree_node(root, bitmapqual);
    index_total_cost = c;
    index_selectivity = s;

    tuples_fetched = clamp_row_est(index_selectivity * rel_tuples);

    T = if rel_pages > 1 { rel_pages as f64 } else { 1.0 };

    pages_fetched = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);

    heap_pages = Min(pages_fetched, rel_pages as f64);
    maxentries = cz::tbm_calculate_entries::call(work_mem() as usize * 1024);

    if loop_count > 1.0 {
        pages_fetched = index_pages_fetched(
            tuples_fetched * loop_count,
            rel_pages,
            get_indexpath_pages(root, bitmapqual),
            root,
        );
        pages_fetched /= loop_count;
    }

    if pages_fetched >= T {
        pages_fetched = T;
    } else {
        pages_fetched = ceil(pages_fetched);
    }

    if maxentries < heap_pages {
        let lossy_pages = Max(0.0, heap_pages - maxentries / 2.0);
        let exact_pages = heap_pages - lossy_pages;

        if lossy_pages > 0.0 {
            tuples_fetched = clamp_row_est(
                index_selectivity * (exact_pages / heap_pages) * rel_tuples
                    + (lossy_pages / heap_pages) * rel_tuples,
            );
        }
    }

    (pages_fetched, index_total_cost, tuples_fetched)
}

/// `cost_bitmap_heap_scan` (costsize.c:1015) — fills a `Path` (by `PathId`).
pub fn cost_bitmap_heap_scan(
    root: &mut PlannerInfo,
    path_id: PathId,
    rel: RelId,
    bitmapqual: PathId,
    loop_count: f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    let cpu_per_tuple: Cost;
    let cost_per_page: Cost;
    let mut cpu_run_cost: Cost;
    let T: f64;

    let (relid, rtekind, reltablespace, pages, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.rows,
            baserel.baserestrictcost,
        )
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_RELATION);

    let param_info = path_param_info(root, path_id);
    let mut rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let (pages_fetched, index_total_cost, tuples_fetched) =
        compute_bitmap_pages(root, rel, bitmapqual, loop_count);

    startup_cost += index_total_cost;
    T = if pages > 1 { pages as f64 } else { 1.0 };

    let spc = cz::get_tablespace_page_costs::call(reltablespace);
    let spc_random_page_cost = spc.spc_random_page_cost;
    let spc_seq_page_cost = spc.spc_seq_page_cost;

    if pages_fetched >= 2.0 {
        cost_per_page = spc_random_page_cost
            - (spc_random_page_cost - spc_seq_page_cost) * sqrt(pages_fetched / T);
    } else {
        cost_per_page = spc_random_page_cost;
    }

    run_cost += pages_fetched * cost_per_page;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    cpu_run_cost = cpu_per_tuple * tuples_fetched;

    let parallel_workers = root.path(path_id).base().parallel_workers;
    if parallel_workers > 0 {
        let parallel_divisor = get_parallel_divisor(root.path(path_id).base());
        cpu_run_cost /= parallel_divisor;
        rows = clamp_row_est(rows / parallel_divisor);
    }

    run_cost += cpu_run_cost;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_bitmap_heap_scan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = if ENABLE_BITMAPSCAN() { 0 } else { 1 };
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * cost_tidscan / cost_tidrangescan (costsize.c:1262-1446)
 * ========================================================================== */

/// `cost_tidscan` (costsize.c:1262). `tidquals` are the implicitly-OR'ed TID
/// qual expression handles.
pub fn cost_tidscan(root: &mut PlannerInfo, path_id: PathId, rel: RelId, tidquals: &[NodeId]) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, reltablespace, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.rows,
            baserel.baserestrictcost,
        )
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_RELATION);
    debug_assert!(!tidquals.is_empty());

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    // Count expected tuples.
    let mut ntuples: f64 = 0.0;
    for &qual in tidquals {
        match node_kind(root, qual) {
            NodeKind::ScalarArrayOp { array_arg } => {
                ntuples += cz::estimate_array_length::call(root, array_arg);
            }
            NodeKind::CurrentOf => ntuples += 1.0,
            NodeKind::Other => ntuples += 1.0,
        }
    }

    let tid_qual_cost = cost_qual_eval(root, tidquals);
    let spc = cz::get_tablespace_page_costs::call(reltablespace);
    run_cost += spc.spc_random_page_cost * ntuples;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup + tid_qual_cost.per_tuple;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_tidscan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `cost_tidrangescan` (costsize.c:1362). `tidrangequals` are the implicitly-
/// AND'ed CTID range qual expression handles.
pub fn cost_tidrangescan<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    rel: RelId,
    tidrangequals: &[NodeId],
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, reltablespace, pages, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.reltablespace,
            baserel.pages,
            baserel.tuples,
            baserel.rows,
            baserel.baserestrictcost,
        )
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_RELATION);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    // selectivity via clauselist_selectivity over the bare qual nodes.
    let selectivity =
        cz::clauselist_selectivity::call(run, root, tidrangequals, relid as i32, super::JOIN_INNER as i32, None);
    let mut pages_d = (selectivity * pages as f64).ceil();
    if pages_d <= 0.0 {
        pages_d = 1.0;
    }

    let ntuples = selectivity * tuples;
    let nseqpages = pages_d - 1.0;

    let tid_qual_cost = cost_qual_eval(root, tidrangequals);
    let spc = cz::get_tablespace_page_costs::call(reltablespace);

    run_cost += spc.spc_random_page_cost + spc.spc_seq_page_cost * nseqpages;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup + tid_qual_cost.per_tuple;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple - tid_qual_cost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_tidrangescan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * cost_subqueryscan (costsize.c:1457)
 * ========================================================================== */

/// `cost_subqueryscan` — fills a `SubqueryScanPath` (by `PathId`).
pub fn cost_subqueryscan<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    path_id: PathId,
    rel: RelId,
    subpath: PathId,
    trivial_pathtarget: bool,
) {
    let (relid, rtekind, baserestrictinfo, baserestrictcost) = {
        let baserel = root.rel(rel);
        (
            baserel.relid,
            baserel.rtekind,
            baserel.baserestrictinfo.clone(),
            baserel.baserestrictcost,
        )
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_SUBQUERY);

    let param_info = path_param_info(root, path_id);

    // qpquals = ppi_clauses ++ baserestrictinfo (parameterized) else baserestrictinfo.
    let mut qpqual_nodes: Vec<NodeId> = Vec::new();
    if let Some(pi) = &param_info {
        qpqual_nodes.extend(rinfo_clause_nodes(root, &pi.ppi_clauses));
        qpqual_nodes.extend(rinfo_clause_nodes(root, &baserestrictinfo));
    } else {
        qpqual_nodes.extend(rinfo_clause_nodes(root, &baserestrictinfo));
    }

    let (sub_rows, sub_disabled, sub_startup, sub_total) = {
        let sp = root.path(subpath).base();
        (sp.rows, sp.disabled_nodes, sp.startup_cost, sp.total_cost)
    };

    let new_rows = clamp_row_est(
        sub_rows
            * cz::clauselist_selectivity::call(run, root, &qpqual_nodes, 0, super::JOIN_INNER as i32, None),
    );

    {
        let p = root.path_mut(path_id).base_mut();
        p.rows = new_rows;
        p.disabled_nodes = sub_disabled;
        p.startup_cost = sub_startup;
        p.total_cost = sub_total;
    }

    if qpqual_nodes.is_empty() && trivial_pathtarget {
        return;
    }

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    let startup_cost0 = qpqual_cost.startup;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    let run_cost0 = cpu_per_tuple * sub_rows;

    let path_rows = root.path(path_id).base().rows;
    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_subqueryscan");
    let startup_cost = startup_cost0 + pt_startup;
    let run_cost = run_cost0 + pt_per_tuple * path_rows;

    let p = root.path_mut(path_id).base_mut();
    p.startup_cost += startup_cost;
    p.total_cost += startup_cost + run_cost;
}

/* ==========================================================================
 * cost_functionscan / cost_tablefuncscan (costsize.c:1537-1647)
 * ========================================================================== */

/// `cost_functionscan` (costsize.c:1537).
pub fn cost_functionscan<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows, baserel.baserestrictcost)
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_FUNCTION);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    // cost_qual_eval_node over (Node *) rte->functions — the RTE's owned
    // funcexprs are reached by the seam owner through `planner_rt_fetch`.
    let (e_startup, e_per_tuple) = cz::rte_functions_exprcost::call(run, root, rel);
    startup_cost += e_startup + e_per_tuple;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_functionscan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `cost_tablefuncscan` (costsize.c:1599).
pub fn cost_tablefuncscan<'mcx>(run: &PlannerRun<'mcx>, root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows, baserel.baserestrictcost)
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_TABLEFUNC);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let (e_startup, e_per_tuple) = cz::rte_tablefunc_exprcost::call(run, root, rel);
    startup_cost += e_startup + e_per_tuple;

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_tablefuncscan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * cost_valuesscan (costsize.c:1657)
 * ========================================================================== */

/// `cost_valuesscan` (costsize.c:1657).
pub fn cost_valuesscan(root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows, baserel.baserestrictcost)
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_VALUES);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    // For now, estimate list evaluation cost at one operator eval per list.
    let mut cpu_per_tuple = cpu_operator_cost();

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_valuesscan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * cost_ctescan / cost_namedtuplestorescan / cost_resultscan
 * (costsize.c:1707-1816)
 * ========================================================================== */

/// `cost_ctescan` (costsize.c:1707).
pub fn cost_ctescan(root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows, baserel.baserestrictcost)
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_CTE);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let mut cpu_per_tuple = cpu_tuple_cost();
    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let (pt_startup, pt_per_tuple) = pathtarget_cost(root, path_id, "cost_ctescan");
    startup_cost += pt_startup;
    run_cost += pt_per_tuple * rows;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `cost_namedtuplestorescan` (costsize.c:1749).
pub fn cost_namedtuplestorescan(root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows, baserel.baserestrictcost)
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_NAMEDTUPLESTORE);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let mut cpu_per_tuple = cpu_tuple_cost();
    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    cpu_per_tuple += cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `cost_resultscan` (costsize.c:1787).
pub fn cost_resultscan(root: &mut PlannerInfo, path_id: PathId, rel: RelId) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let (relid, rtekind, tuples, base_rows, baserestrictcost) = {
        let baserel = root.rel(rel);
        (baserel.relid, baserel.rtekind, baserel.tuples, baserel.rows, baserel.baserestrictcost)
    };
    debug_assert!(relid > 0);
    debug_assert!(rtekind == RTE_RESULT);

    let param_info = path_param_info(root, path_id);
    let rows = match &param_info {
        Some(pi) => pi.ppi_rows,
        None => base_rows,
    };

    let qpqual_cost = get_restriction_qual_cost_ext(root, rel, baserestrictcost, param_info.as_ref());
    startup_cost += qpqual_cost.startup;
    let cpu_per_tuple = cpu_tuple_cost() + qpqual_cost.per_tuple;
    run_cost += cpu_per_tuple * tuples;

    let p = root.path_mut(path_id).base_mut();
    p.rows = rows;
    p.disabled_nodes = 0;
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * Local helpers over the arena.
 * ========================================================================== */

/// Snapshot a path's `ParamPathInfo` (cloned so the rel can be re-borrowed).
fn path_param_info(root: &PlannerInfo, path_id: PathId) -> Option<ParamPathInfo> {
    root.path(path_id).base().param_info.as_deref().cloned()
}

/// Read a path's pathtarget eval cost `(startup, per_tuple)`.
fn pathtarget_cost(root: &PlannerInfo, path_id: PathId, who: &str) -> (Cost, Cost) {
    let pt = root
        .path(path_id)
        .base()
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| panic!("backend-optimizer-path-costsize::{who}: path.pathtarget must be set"));
    (pt.cost.startup, pt.cost.per_tuple)
}

/// `get_restriction_qual_cost` resolving the baserel's cached cost (passed in)
/// plus, when parameterized, the pushed-down clause cost.
fn get_restriction_qual_cost_ext(
    root: &PlannerInfo,
    _rel: RelId,
    baserestrictcost: types_pathnodes::QualCost,
    param_info: Option<&ParamPathInfo>,
) -> types_pathnodes::QualCost {
    if let Some(pi) = param_info {
        let nodes = rinfo_clause_nodes(root, &pi.ppi_clauses);
        let mut qpqual_cost = cost_qual_eval(root, &nodes);
        qpqual_cost.startup += baserestrictcost.startup;
        qpqual_cost.per_tuple += baserestrictcost.per_tuple;
        qpqual_cost
    } else {
        baserestrictcost
    }
}

/// `IndexPath` accessor.
fn expect_index_path(root: &PlannerInfo, path_id: PathId) -> &IndexPath {
    match root.path(path_id) {
        PathNode::IndexPath(ip) => ip,
        _ => panic!("backend-optimizer-path-costsize::cost_index: path is not an IndexPath"),
    }
}

fn set_index_rows(root: &mut PlannerInfo, path_id: PathId, rows: f64) {
    if let PathNode::IndexPath(ip) = root.path_mut(path_id) {
        ip.path.rows = rows;
    }
}
fn set_index_disabled(root: &mut PlannerInfo, path_id: PathId, n: i32) {
    if let PathNode::IndexPath(ip) = root.path_mut(path_id) {
        ip.path.disabled_nodes = n;
    }
}
fn set_index_parallel_workers(root: &mut PlannerInfo, path_id: PathId, w: i32) {
    if let PathNode::IndexPath(ip) = root.path_mut(path_id) {
        ip.path.parallel_workers = w;
    }
}

/// Classify a TID qual node for `cost_tidscan` (matches the C `IsA` cascade).
enum NodeKind {
    ScalarArrayOp { array_arg: NodeId },
    CurrentOf,
    Other,
}
fn node_kind(root: &PlannerInfo, node: NodeId) -> NodeKind {
    use types_nodes::primnodes::Expr;
    match root.node(node) {
        // ScalarArrayOpExpr: each array element yields 1 tuple. C calls
        // `estimate_array_length(root, lsecond(saop->args))`; the SAOP arg is an
        // inline `Expr` value (no `NodeId`), so we pass the SAOP node handle and
        // the owner (`estimate_array_length`) reads `args[1]` from it.
        Expr::ScalarArrayOpExpr(_) => NodeKind::ScalarArrayOp { array_arg: node },
        Expr::CurrentOfExpr(_) => NodeKind::CurrentOf,
        _ => NodeKind::Other,
    }
}

#[allow(unused_imports)]
use types_pathnodes::IndexPath as _IndexPath;
#[allow(unused_imports)]
use Path as _Path;
