#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]

//! `backend/optimizer/path/costsize.c` — the planner's cost model (1:1 port over
//! the fabled value/arena `PlannerInfo` model).
//!
//! Each estimator computes a `startup_cost`/`total_cost` pair from numeric
//! fields of the owned planner tree (`RelOptInfo`, the `*Path` subtypes,
//! `JoinCostWorkspace`) and the cost GUC constants. That arithmetic is
//! node-independent and is ported faithfully here as the real crate body
//! operating on the owned arena, resolving `PathId`/`RinfoId`/`RelId`/`NodeId`
//! handles through `root`.
//!
//! Cross-unit dependencies (the selectivity subsystem, the catalog, the index
//! AM, the executor sizing helpers, and the RTE/Query reads that the fabled
//! arena does not resolve) are routed through outward seams declared in
//! [`backend_optimizer_path_costsize_seams`]; each panics until its owner
//! lands ("mirror PG and panic"). The cost arithmetic stays in-crate.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{BlockNumber, Cardinality, Cost};
use types_pathnodes::{
    GatherMergePath, GatherPath, ParamPathInfo, Path, PathId, PathKey, PathNode,
    PlannerInfo, QualCost, RelId, RelOptInfo, RinfoId, JOIN_INNER,
};

use backend_optimizer_path_costsize_seams as cz;
use backend_optimizer_util_pathnode_seams as ps;

pub mod exprcost;
pub mod joins;
pub mod scans;
pub mod sizeest;

/* --------------------------------------------------------------------------
 * GUC cost-constant defaults (cost.h).
 * ------------------------------------------------------------------------ */

pub const DEFAULT_SEQ_PAGE_COST: f64 = 1.0;
pub const DEFAULT_RANDOM_PAGE_COST: f64 = 4.0;
pub const DEFAULT_CPU_TUPLE_COST: f64 = 0.01;
pub const DEFAULT_CPU_INDEX_TUPLE_COST: f64 = 0.005;
pub const DEFAULT_CPU_OPERATOR_COST: f64 = 0.0025;
pub const DEFAULT_PARALLEL_TUPLE_COST: f64 = 0.1;
pub const DEFAULT_PARALLEL_SETUP_COST: f64 = 1000.0;
pub const DEFAULT_RECURSIVE_WORKTABLE_FACTOR: f64 = 10.0;
pub const DEFAULT_EFFECTIVE_CACHE_SIZE: i32 = 524288;

/// `#define DEFAULT_NUM_DISTINCT 200` (selfuncs.h).
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;

/* --------------------------------------------------------------------------
 * costsize.c GUC globals, backed by module statics at their DEFAULT_* / true
 * values. These are costsize.c's own GUCs (not seams; per the no-ambient-
 * global-seams rule for cross-crate consumers, but as the owner this crate
 * holds the canonical knobs).
 * ------------------------------------------------------------------------ */

pub static SEQ_PAGE_COST: f64 = DEFAULT_SEQ_PAGE_COST;
pub static RANDOM_PAGE_COST: f64 = DEFAULT_RANDOM_PAGE_COST;
pub static CPU_TUPLE_COST: f64 = DEFAULT_CPU_TUPLE_COST;
pub static CPU_INDEX_TUPLE_COST: f64 = DEFAULT_CPU_INDEX_TUPLE_COST;
pub static CPU_OPERATOR_COST: f64 = DEFAULT_CPU_OPERATOR_COST;
pub static PARALLEL_TUPLE_COST: f64 = DEFAULT_PARALLEL_TUPLE_COST;
pub static PARALLEL_SETUP_COST: f64 = DEFAULT_PARALLEL_SETUP_COST;
pub static RECURSIVE_WORKTABLE_FACTOR: f64 = DEFAULT_RECURSIVE_WORKTABLE_FACTOR;
pub static EFFECTIVE_CACHE_SIZE: i32 = DEFAULT_EFFECTIVE_CACHE_SIZE;
/// `disable_cost` (costsize.c).
pub static DISABLE_COST: Cost = 1.0e10;
pub static MAX_PARALLEL_WORKERS_PER_GATHER: i32 = 2;
pub static PARALLEL_LEADER_PARTICIPATION: bool = true;

// enable_* GUCs (default ON).
pub static ENABLE_SEQSCAN: bool = true;
pub static ENABLE_INDEXSCAN: bool = true;
pub static ENABLE_BITMAPSCAN: bool = true;
pub static ENABLE_TIDSCAN: bool = true;
pub static ENABLE_SORT: bool = true;
pub static ENABLE_INCREMENTAL_SORT: bool = true;
pub static ENABLE_HASHAGG: bool = true;
pub static ENABLE_NESTLOOP: bool = true;
pub static ENABLE_MATERIAL: bool = true;
pub static ENABLE_MERGEJOIN: bool = true;
pub static ENABLE_HASHJOIN: bool = true;
pub static ENABLE_GATHERMERGE: bool = true;

// Inline GUC accessors so the cost arithmetic reads like the C source.
#[inline]
pub(crate) fn seq_page_cost() -> f64 {
    SEQ_PAGE_COST
}
#[inline]
pub(crate) fn random_page_cost() -> f64 {
    RANDOM_PAGE_COST
}
#[inline]
pub(crate) fn cpu_tuple_cost() -> f64 {
    CPU_TUPLE_COST
}
#[inline]
pub(crate) fn cpu_operator_cost() -> f64 {
    CPU_OPERATOR_COST
}
#[inline]
pub(crate) fn parallel_tuple_cost() -> f64 {
    PARALLEL_TUPLE_COST
}
#[inline]
pub(crate) fn parallel_setup_cost() -> f64 {
    PARALLEL_SETUP_COST
}
#[inline]
pub(crate) fn recursive_worktable_factor() -> f64 {
    RECURSIVE_WORKTABLE_FACTOR
}
#[inline]
pub(crate) fn effective_cache_size() -> f64 {
    EFFECTIVE_CACHE_SIZE as f64
}
#[inline]
pub(crate) fn disable_cost() -> Cost {
    DISABLE_COST
}
#[inline]
pub(crate) fn max_parallel_workers_per_gather() -> i32 {
    MAX_PARALLEL_WORKERS_PER_GATHER
}
#[inline]
pub(crate) fn parallel_leader_participation() -> bool {
    PARALLEL_LEADER_PARTICIPATION
}
#[inline]
pub(crate) fn work_mem() -> i32 {
    ps::work_mem::call()
}

/* --------------------------------------------------------------------------
 * Module-internal macros/constants from costsize.c and supporting headers.
 * ------------------------------------------------------------------------ */

/// `#define LOG2(x)  (log(x) / 0.693147180559945)` — reproduced bit-for-bit.
#[inline]
pub(crate) fn LOG2(x: f64) -> f64 {
    libm_log(x) / 0.693147180559945
}

/// `APPEND_CPU_COST_MULTIPLIER`.
pub(crate) const APPEND_CPU_COST_MULTIPLIER: f64 = 0.5;
/// `MAXIMUM_ROWCOUNT`.
const MAXIMUM_ROWCOUNT: f64 = 1e100;
/// `MaxAllocSize` (memutils.h): `0x3fffffff`.
const MaxAllocSize: i64 = 0x3fff_ffff;
/// `BLCKSZ`.
pub(crate) const BLCKSZ: f64 = 8192.0;
/// `SizeofHeapTupleHeader` (htup_details.h) — `offsetof(.., t_bits)`.
pub(crate) const SizeofHeapTupleHeader: usize = 23;
/// `LONG_MAX` for the platform's 64-bit `long`.
const LONG_MAX: i64 = i64::MAX;

/* --------------------------------------------------------------------------
 * RTEKind values (parsenodes.h) — only RTE_RELATION is exported from
 * types-pathnodes; the others are local consts matching the C enum order.
 * ------------------------------------------------------------------------ */
pub(crate) const RTE_RELATION: u32 = 0;
pub(crate) const RTE_SUBQUERY: u32 = 1;
#[allow(dead_code)]
pub(crate) const RTE_JOIN: u32 = 2;
pub(crate) const RTE_FUNCTION: u32 = 3;
pub(crate) const RTE_TABLEFUNC: u32 = 4;
pub(crate) const RTE_VALUES: u32 = 5;
pub(crate) const RTE_CTE: u32 = 6;
pub(crate) const RTE_NAMEDTUPLESTORE: u32 = 7;
pub(crate) const RTE_RESULT: u32 = 8;

/* --------------------------------------------------------------------------
 * libm / C-macro shims.
 * ------------------------------------------------------------------------ */
#[inline]
pub(crate) fn libm_log(x: f64) -> f64 {
    x.ln()
}
#[inline]
pub(crate) fn ceil(x: f64) -> f64 {
    x.ceil()
}
#[inline]
pub(crate) fn sqrt(x: f64) -> f64 {
    x.sqrt()
}
#[inline]
pub(crate) fn Max(a: f64, b: f64) -> f64 {
    if a > b {
        a
    } else {
        b
    }
}
#[inline]
pub(crate) fn Min(a: f64, b: f64) -> f64 {
    if a < b {
        a
    } else {
        b
    }
}
/// C `rint()`: round to nearest, ties to even.
#[inline]
pub(crate) fn rint(x: f64) -> f64 {
    let r = x.round_ties_even();
    if r == 0.0 {
        0.0_f64.copysign(x)
    } else {
        r
    }
}
/// `MAXALIGN(LEN)` — `TYPEALIGN(MAXIMUM_ALIGNOF, LEN)`; ALIGNOF == 8.
#[inline]
pub(crate) fn MAXALIGN(len: i64) -> i64 {
    const MAXIMUM_ALIGNOF: i64 = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/* ==========================================================================
 * clamp helpers (costsize.c:212-284).
 * ========================================================================== */

/// `clamp_row_est` — force a row-count estimate to a sane value.
pub fn clamp_row_est(mut nrows: f64) -> f64 {
    if nrows > MAXIMUM_ROWCOUNT || nrows.is_nan() {
        nrows = MAXIMUM_ROWCOUNT;
    } else if nrows <= 1.0 {
        nrows = 1.0;
    } else {
        nrows = rint(nrows);
    }
    nrows
}

/// `clamp_width_est` — force a tuple-width estimate to a sane int32 value.
pub fn clamp_width_est(tuple_width: i64) -> i32 {
    if tuple_width > MaxAllocSize {
        return MaxAllocSize as i32;
    }
    debug_assert!(tuple_width >= 0);
    tuple_width as i32
}

/// `clamp_cardinality_to_long` — cast a Cardinality to a sane `long`.
pub fn clamp_cardinality_to_long(x: Cardinality) -> i64 {
    if x.is_nan() {
        return LONG_MAX;
    }
    if x <= 0.0 {
        return 0;
    }
    if x < LONG_MAX as f64 {
        x as i64
    } else {
        LONG_MAX
    }
}

/* ==========================================================================
 * byte / page / parallelism helpers (costsize.c:6450-6510, 6610-).
 * ========================================================================== */

/// `relation_byte_size`.
pub fn relation_byte_size(tuples: f64, width: i32) -> f64 {
    tuples * (MAXALIGN(width as i64) + MAXALIGN(SizeofHeapTupleHeader as i64)) as f64
}

/// `page_size`.
pub fn page_size(tuples: f64, width: i32) -> f64 {
    ceil(relation_byte_size(tuples, width) / BLCKSZ)
}

/// `get_parallel_divisor`.
pub fn get_parallel_divisor(path: &Path) -> f64 {
    let mut parallel_divisor: f64 = path.parallel_workers as f64;
    if parallel_leader_participation() {
        let leader_contribution = 1.0 - (0.3 * path.parallel_workers as f64);
        if leader_contribution > 0.0 {
            parallel_divisor += leader_contribution;
        }
    }
    parallel_divisor
}

/// `compute_gather_rows`.
pub fn compute_gather_rows(path: &Path) -> f64 {
    debug_assert!(path.parallel_workers > 0);
    clamp_row_est(path.rows * get_parallel_divisor(path))
}

/* ==========================================================================
 * Mackert-Lohman index_pages_fetched (costsize.c:907-961).
 * ========================================================================== */

/// `index_pages_fetched`.
pub fn index_pages_fetched(
    tuples_fetched: f64,
    pages: BlockNumber,
    index_pages: f64,
    root: &PlannerInfo,
) -> f64 {
    let pages_fetched: f64;
    let mut total_pages: f64;
    let T: f64;
    let mut b: f64;

    T = if pages > 1 { pages as f64 } else { 1.0 };

    total_pages = root.total_table_pages + index_pages;
    total_pages = Max(total_pages, 1.0);
    debug_assert!(T <= total_pages);

    b = effective_cache_size() * T / total_pages;

    if b <= 1.0 {
        b = 1.0;
    } else {
        b = ceil(b);
    }

    if T <= b {
        let pf = (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched);
        if pf >= T {
            pages_fetched = T;
        } else {
            pages_fetched = ceil(pf);
        }
    } else {
        let lim = (2.0 * T * b) / (2.0 * T - b);
        let pf = if tuples_fetched <= lim {
            (2.0 * T * tuples_fetched) / (2.0 * T + tuples_fetched)
        } else {
            b + (tuples_fetched - lim) * (T - b) / T
        };
        pages_fetched = ceil(pf);
    }
    pages_fetched
}

/* ==========================================================================
 * cost_gather / cost_gather_merge (costsize.c:445-539).
 * ========================================================================== */

/// `cost_gather` — fills a `GatherPath` (by `PathId`).
pub fn cost_gather(
    root: &mut PlannerInfo,
    path_id: PathId,
    rel: RelId,
    rows: Option<f64>,
) {
    // Mark the path with the correct row estimate.
    let new_rows = if let Some(r) = rows {
        r
    } else if let Some(pi) = path_base(root, path_id).param_info.as_deref() {
        pi.ppi_rows
    } else {
        root.rel(rel).rows
    };

    let subpath_id = match root.path(path_id) {
        PathNode::GatherPath(gp) => gp.subpath.expect("cost_gather: subpath must be set"),
        _ => panic!("backend-optimizer-path-costsize::cost_gather: path is not a GatherPath"),
    };
    let subpath = root.path(subpath_id).base();
    let startup_cost = subpath.startup_cost;
    let mut run_cost = subpath.total_cost - subpath.startup_cost;
    let sub_disabled = subpath.disabled_nodes;

    let startup_cost = startup_cost + parallel_setup_cost();
    run_cost += parallel_tuple_cost() * new_rows;

    if let PathNode::GatherPath(gp) = root.path_mut(path_id) {
        gp.path.rows = new_rows;
        gp.path.disabled_nodes = sub_disabled;
        gp.path.startup_cost = startup_cost;
        gp.path.total_cost = startup_cost + run_cost;
    }
}

/// `cost_gather` over an owned `GatherPath` value (helper).
#[allow(dead_code)]
pub(crate) fn cost_gather_owned(
    path: &mut GatherPath,
    root: &PlannerInfo,
    rel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
    rows: Option<f64>,
) {
    if let Some(r) = rows {
        path.path.rows = r;
    } else if let Some(pi) = param_info {
        path.path.rows = pi.ppi_rows;
    } else {
        path.path.rows = rel.rows;
    }
    let subpath_id = path.subpath.expect("cost_gather: subpath must be set");
    let subpath = root.path(subpath_id).base();
    let startup_cost = subpath.startup_cost;
    let mut run_cost = subpath.total_cost - subpath.startup_cost;
    let startup_cost = startup_cost + parallel_setup_cost();
    run_cost += parallel_tuple_cost() * path.path.rows;
    path.path.disabled_nodes = subpath.disabled_nodes;
    path.path.startup_cost = startup_cost;
    path.path.total_cost = startup_cost + run_cost;
}

/// `cost_gather_merge` — fills a `GatherMergePath` (by `PathId`).
pub fn cost_gather_merge(
    root: &mut PlannerInfo,
    path_id: PathId,
    rel: RelId,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    rows: Option<f64>,
) {
    let new_rows = if let Some(r) = rows {
        r
    } else if let Some(pi) = path_base(root, path_id).param_info.as_deref() {
        pi.ppi_rows
    } else {
        root.rel(rel).rows
    };

    let num_workers = match root.path(path_id) {
        PathNode::GatherMergePath(gmp) => gmp.num_workers,
        _ => panic!(
            "backend-optimizer-path-costsize::cost_gather_merge: path is not a GatherMergePath"
        ),
    };

    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    debug_assert!(num_workers > 0);
    let N = num_workers as f64 + 1.0;
    let logN = LOG2(N);

    let comparison_cost = 2.0 * cpu_operator_cost();

    startup_cost += comparison_cost * N * logN;
    run_cost += new_rows * comparison_cost * logN;
    run_cost += cpu_operator_cost() * new_rows;
    startup_cost += parallel_setup_cost();
    run_cost += parallel_tuple_cost() * new_rows * 1.05;

    let disabled = input_disabled_nodes + (if ENABLE_GATHERMERGE { 0 } else { 1 });

    if let PathNode::GatherMergePath(gmp) = root.path_mut(path_id) {
        gmp.path.rows = new_rows;
        gmp.path.disabled_nodes = disabled;
        gmp.path.startup_cost = startup_cost + input_startup_cost;
        gmp.path.total_cost = startup_cost + run_cost + input_total_cost;
    }
}

/// `cost_gather_merge` over an owned `GatherMergePath` value (helper).
#[allow(dead_code)]
pub(crate) fn cost_gather_merge_owned(
    path: &mut GatherMergePath,
    rel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    rows: Option<f64>,
) {
    if let Some(r) = rows {
        path.path.rows = r;
    } else if let Some(pi) = param_info {
        path.path.rows = pi.ppi_rows;
    } else {
        path.path.rows = rel.rows;
    }
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;
    debug_assert!(path.num_workers > 0);
    let N = path.num_workers as f64 + 1.0;
    let logN = LOG2(N);
    let comparison_cost = 2.0 * cpu_operator_cost();
    startup_cost += comparison_cost * N * logN;
    run_cost += path.path.rows * comparison_cost * logN;
    run_cost += cpu_operator_cost() * path.path.rows;
    startup_cost += parallel_setup_cost();
    run_cost += parallel_tuple_cost() * path.path.rows * 1.05;
    path.path.disabled_nodes = input_disabled_nodes + (if ENABLE_GATHERMERGE { 0 } else { 1 });
    path.path.startup_cost = startup_cost + input_startup_cost;
    path.path.total_cost = startup_cost + run_cost + input_total_cost;
}

/* ==========================================================================
 * cost_recursive_union (costsize.c:1787-1823).
 * ========================================================================== */

/// `cost_recursive_union` — fills the RecursiveUnion path (by `PathId`) from its
/// non-recursive (`nrterm`) and recursive (`rterm`) subpaths.
pub fn cost_recursive_union(
    root: &mut PlannerInfo,
    runion: PathId,
    nrterm: PathId,
    rterm: PathId,
) {
    let (nr_startup, nr_total, nr_rows, nr_disabled, nr_width) = {
        let p = root.path(nrterm).base();
        (
            p.startup_cost,
            p.total_cost,
            p.rows,
            p.disabled_nodes,
            p.pathtarget
                .as_ref()
                .expect("cost_recursive_union: nrterm.pathtarget must be set")
                .width,
        )
    };
    let (r_total, r_rows, r_disabled, r_width) = {
        let p = root.path(rterm).base();
        (
            p.total_cost,
            p.rows,
            p.disabled_nodes,
            p.pathtarget
                .as_ref()
                .expect("cost_recursive_union: rterm.pathtarget must be set")
                .width,
        )
    };

    let startup_cost = nr_startup;
    let mut total_cost = nr_total;
    let mut total_rows = nr_rows;

    total_cost += 10.0 * r_total;
    total_rows += 10.0 * r_rows;

    total_cost += cpu_tuple_cost() * total_rows;

    let runion_p = root.path_mut(runion).base_mut();
    runion_p.disabled_nodes = nr_disabled + r_disabled;
    runion_p.startup_cost = startup_cost;
    runion_p.total_cost = total_cost;
    runion_p.rows = total_rows;
    if let Some(pt) = runion_p.pathtarget.as_mut() {
        pt.width = nr_width.max(r_width);
    }
}

/* ==========================================================================
 * cost_tuplesort (costsize.c:1899-1989) + cost_sort + cost_incremental_sort.
 * ========================================================================== */

/// `cost_tuplesort` — returns `(startup_cost, run_cost)`.
pub(crate) fn cost_tuplesort(
    tuples: f64,
    width: i32,
    mut comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) -> (Cost, Cost) {
    let input_bytes = relation_byte_size(tuples, width);
    let output_bytes: f64;
    let output_tuples: f64;
    let sort_mem_bytes: i64 = sort_mem as i64 * 1024_i64;

    let startup_cost: Cost;
    let run_cost: Cost;

    let mut tuples = tuples;
    if tuples < 2.0 {
        tuples = 2.0;
    }

    comparison_cost += 2.0 * cpu_operator_cost();

    if limit_tuples > 0.0 && limit_tuples < tuples {
        output_tuples = limit_tuples;
        output_bytes = relation_byte_size(output_tuples, width);
    } else {
        output_tuples = tuples;
        output_bytes = input_bytes;
    }

    if output_bytes > sort_mem_bytes as f64 {
        let npages = ceil(input_bytes / BLCKSZ);
        let nruns = input_bytes / sort_mem_bytes as f64;
        let mergeorder = cz::tuplesort_merge_order::call(sort_mem_bytes);
        let log_runs: f64;
        let npageaccesses: f64;

        startup_cost = comparison_cost * tuples * LOG2(tuples);

        if nruns > mergeorder {
            log_runs = ceil(libm_log(nruns) / libm_log(mergeorder));
        } else {
            log_runs = 1.0;
        }
        npageaccesses = 2.0 * npages * log_runs;
        let startup_cost =
            startup_cost + npageaccesses * (seq_page_cost() * 0.75 + random_page_cost() * 0.25);

        run_cost = cpu_operator_cost() * tuples;
        return (startup_cost, run_cost);
    } else if tuples > 2.0 * output_tuples || input_bytes > sort_mem_bytes as f64 {
        startup_cost = comparison_cost * tuples * LOG2(2.0 * output_tuples);
    } else {
        startup_cost = comparison_cost * tuples * LOG2(tuples);
    }

    run_cost = cpu_operator_cost() * tuples;
    (startup_cost, run_cost)
}

/// `cost_sort` — fills a Sort path (by `PathId`).
pub fn cost_sort(
    root: &mut PlannerInfo,
    path_id: PathId,
    _pathkeys: &[PathKey],
    input_disabled_nodes: i32,
    input_cost: Cost,
    tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    let (mut startup_cost, run_cost) =
        cost_tuplesort(tuples, width, comparison_cost, sort_mem, limit_tuples);
    startup_cost += input_cost;

    let p = root.path_mut(path_id).base_mut();
    p.rows = tuples;
    p.disabled_nodes = input_disabled_nodes + (if ENABLE_SORT { 0 } else { 1 });
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/// `cost_sort` over an owned `Path` (helper used by mergejoin initial-cost).
pub(crate) fn cost_sort_owned(
    path: &mut Path,
    _pathkeys: &[PathKey],
    input_disabled_nodes: i32,
    input_cost: Cost,
    tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    let (mut startup_cost, run_cost) =
        cost_tuplesort(tuples, width, comparison_cost, sort_mem, limit_tuples);
    startup_cost += input_cost;
    path.rows = tuples;
    path.disabled_nodes = input_disabled_nodes + (if ENABLE_SORT { 0 } else { 1 });
    path.startup_cost = startup_cost;
    path.total_cost = startup_cost + run_cost;
}

/// `cost_incremental_sort` (costsize.c:2000) — fills a path (by `PathId`).
pub fn cost_incremental_sort(
    root: &mut PlannerInfo,
    path_id: PathId,
    pathkeys: &[PathKey],
    presorted_keys: i32,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    let (s, t) = cost_incremental_sort_compute(
        root,
        pathkeys,
        presorted_keys,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );
    let p = root.path_mut(path_id).base_mut();
    p.rows = input_tuples;
    p.disabled_nodes = input_disabled_nodes;
    p.startup_cost = s;
    p.total_cost = t;
}

/// `cost_incremental_sort` over an owned `Path` (helper for mergejoin).
pub(crate) fn cost_incremental_sort_owned(
    path: &mut Path,
    root: &PlannerInfo,
    pathkeys: &[PathKey],
    presorted_keys: i32,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    input_tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) {
    let (s, t) = cost_incremental_sort_compute(
        root,
        pathkeys,
        presorted_keys,
        input_startup_cost,
        input_total_cost,
        input_tuples,
        width,
        comparison_cost,
        sort_mem,
        limit_tuples,
    );
    path.rows = input_tuples;
    path.disabled_nodes = input_disabled_nodes;
    path.startup_cost = s;
    path.total_cost = t;
}

/// Shared arithmetic of `cost_incremental_sort`; returns `(startup, total)`.
fn cost_incremental_sort_compute(
    root: &PlannerInfo,
    pathkeys: &[PathKey],
    presorted_keys: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    mut input_tuples: f64,
    width: i32,
    comparison_cost: Cost,
    sort_mem: i32,
    limit_tuples: f64,
) -> (Cost, Cost) {
    let input_run_cost = input_total_cost - input_startup_cost;
    let mut input_groups: f64;

    debug_assert!(presorted_keys > 0 && (presorted_keys as usize) < pathkeys.len());

    if input_tuples < 2.0 {
        input_tuples = 2.0;
    }

    // Default estimate of number of groups, capped to one group per row.
    input_groups = Min(input_tuples, DEFAULT_NUM_DISTINCT);

    // Extract presorted keys as list of expressions; detect "varno 0".
    let mut presorted_exprs: Vec<types_pathnodes::NodeId> = Vec::new();
    let mut unknown_varno = false;
    for (i, key) in pathkeys.iter().enumerate() {
        let ec = key
            .pk_eclass
            .expect("cost_incremental_sort: pathkey pk_eclass must be set");
        let member_em = *root
            .ec(ec)
            .ec_members
            .first()
            .expect("cost_incremental_sort: ec_members must be non-empty");
        let em_expr = root.em(member_em).em_expr;

        if cz::pull_varnos_contains_zero::call(root, em_expr) {
            unknown_varno = true;
            break;
        }
        presorted_exprs.push(em_expr);

        if i + 1 >= presorted_keys as usize {
            break;
        }
    }

    if !unknown_varno {
        input_groups = cz::estimate_num_groups::call(root, &presorted_exprs, input_tuples);
    }

    let group_tuples = input_tuples / input_groups;
    let group_input_run_cost = input_run_cost / input_groups;

    let (group_startup_cost, group_run_cost) =
        cost_tuplesort(group_tuples, width, comparison_cost, sort_mem, limit_tuples);

    let startup_cost = group_startup_cost + input_startup_cost + group_input_run_cost;

    let mut run_cost = group_run_cost
        + (group_run_cost + group_startup_cost) * (input_groups - 1.0)
        + group_input_run_cost * (input_groups - 1.0);

    run_cost += (cpu_tuple_cost() + comparison_cost) * input_tuples;
    run_cost += 2.0 * cpu_tuple_cost() * input_groups;

    debug_assert!(ENABLE_INCREMENTAL_SORT);

    (startup_cost, startup_cost + run_cost)
}

/* ==========================================================================
 * cost_merge_append (costsize.c:2431-2475).
 * ========================================================================== */

/// `cost_merge_append` — fills a MergeAppend path (by `PathId`).
pub fn cost_merge_append(
    root: &mut PlannerInfo,
    path_id: PathId,
    _pathkeys: &[PathKey],
    n_streams: i32,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
) {
    let mut startup_cost: Cost = 0.0;
    let mut run_cost: Cost = 0.0;

    let N = if n_streams < 2 { 2.0 } else { n_streams as f64 };
    let logN = LOG2(N);

    let comparison_cost = 2.0 * cpu_operator_cost();

    startup_cost += comparison_cost * N * logN;
    run_cost += tuples * comparison_cost * logN;
    run_cost += cpu_tuple_cost() * APPEND_CPU_COST_MULTIPLIER * tuples;

    let p = root.path_mut(path_id).base_mut();
    p.disabled_nodes = input_disabled_nodes;
    p.startup_cost = startup_cost + input_startup_cost;
    p.total_cost = startup_cost + run_cost + input_total_cost;
}

/* ==========================================================================
 * cost_material (costsize.c:2482-2524).
 * ========================================================================== */

/// `cost_material` — fills a Material path (by `PathId`).
pub fn cost_material(
    root: &mut PlannerInfo,
    path_id: PathId,
    input_disabled_nodes: i32,
    input_startup_cost: Cost,
    input_total_cost: Cost,
    tuples: f64,
    width: i32,
) {
    let startup_cost: Cost = input_startup_cost;
    let mut run_cost: Cost = input_total_cost - input_startup_cost;
    let nbytes = relation_byte_size(tuples, width);
    let work_mem_bytes = work_mem() as f64 * 1024.0;

    run_cost += 2.0 * cpu_operator_cost() * tuples;

    if nbytes > work_mem_bytes {
        let npages = ceil(nbytes / BLCKSZ);
        run_cost += seq_page_cost() * npages;
    }

    let p = root.path_mut(path_id).base_mut();
    p.rows = tuples;
    p.disabled_nodes = input_disabled_nodes + (if ENABLE_MATERIAL { 0 } else { 1 });
    p.startup_cost = startup_cost;
    p.total_cost = startup_cost + run_cost;
}

/* ==========================================================================
 * append_nonpartial_cost + cost_append (costsize.c:2168-2418).
 * ========================================================================== */

/// `append_nonpartial_cost`.
fn append_nonpartial_cost(
    root: &PlannerInfo,
    subpaths: &[PathId],
    numpaths: i32,
    parallel_workers: i32,
) -> Cost {
    if numpaths == 0 {
        return 0.0;
    }

    let arrlen = (parallel_workers as usize).min(numpaths as usize);
    let mut costarr: Vec<Cost> = alloc::vec![0.0; arrlen];

    let mut path_index: usize = 0;
    for subpath_id in subpaths.iter() {
        if path_index == arrlen {
            break;
        }
        costarr[path_index] = root.path(*subpath_id).base().total_cost;
        path_index += 1;
    }

    let mut min_index = arrlen - 1;

    for subpath_id in subpaths.iter().skip(path_index) {
        if path_index == numpaths as usize {
            break;
        }
        path_index += 1;

        costarr[min_index] += root.path(*subpath_id).base().total_cost;

        min_index = 0;
        for i in 0..arrlen {
            if costarr[i] < costarr[min_index] {
                min_index = i;
            }
        }
    }

    let mut max_index = 0;
    for i in 0..arrlen {
        if costarr[i] > costarr[max_index] {
            max_index = i;
        }
    }

    costarr[max_index]
}

/// `cost_append` — fills an Append path (by `PathId`).
pub fn cost_append(root: &mut PlannerInfo, path_id: PathId) {
    // Snapshot the AppendPath fields we read; write back at the end.
    let (subpaths, parallel_aware, has_pathkeys, parallel_workers, first_partial_path) =
        match root.path(path_id) {
            PathNode::AppendPath(ap) => (
                ap.subpaths.clone(),
                ap.path.parallel_aware,
                !ap.path.pathkeys.is_empty(),
                ap.path.parallel_workers,
                ap.first_partial_path,
            ),
            _ => panic!("backend-optimizer-path-costsize::cost_append: path is not an AppendPath"),
        };

    let mut acc_disabled: i32 = 0;
    let mut acc_startup: Cost = 0.0;
    let mut acc_total: Cost = 0.0;
    let mut acc_rows: f64 = 0.0;

    if subpaths.is_empty() {
        let p = root.path_mut(path_id).base_mut();
        p.disabled_nodes = 0;
        p.startup_cost = 0.0;
        p.total_cost = 0.0;
        p.rows = 0.0;
        return;
    }

    if !parallel_aware {
        if !has_pathkeys {
            acc_startup = root.path(subpaths[0]).base().startup_cost;
            for subpath_id in &subpaths {
                let subpath = root.path(*subpath_id).base();
                acc_rows += subpath.rows;
                acc_disabled += subpath.disabled_nodes;
                acc_total += subpath.total_cost;
            }
        } else {
            // Ordered, non-parallel-aware Append: inject Sort into children that
            // aren't natively ordered. C uses cost_sort over a stack Path with
            // pathkeys_contained_in to test native order.
            let apath_pathkeys = match root.path(path_id) {
                PathNode::AppendPath(ap) => ap.path.pathkeys.clone(),
                _ => unreachable!(),
            };
            acc_startup = -1.0; // matches C's startup_cost = -1 sentinel
            for subpath_id in &subpaths {
                let (s_startup, s_total, s_rows, s_disabled, s_pathkeys, s_width) = {
                    let sp = root.path(*subpath_id).base();
                    (
                        sp.startup_cost,
                        sp.total_cost,
                        sp.rows,
                        sp.disabled_nodes,
                        sp.pathkeys.clone(),
                        sp.pathtarget
                            .as_ref()
                            .expect("cost_append: subpath pathtarget must be set")
                            .width,
                    )
                };

                let (sort_startup, sort_total, sort_disabled) =
                    if ps::pathkeys_contained_in::call(&apath_pathkeys, &s_pathkeys) {
                        // child is naturally ordered.
                        (s_startup, s_total, s_disabled)
                    } else {
                        // We'll insert a Sort node, so compute cost for sorting.
                        let mut sort_path = Path {
                            type_: root.path(*subpath_id).base().type_,
                            pathtype: root.path(*subpath_id).base().pathtype,
                            parent: root.path(*subpath_id).base().parent,
                            pathtarget: None,
                            param_info: None,
                            parallel_aware: false,
                            parallel_safe: false,
                            parallel_workers: 0,
                            rows: 0.0,
                            disabled_nodes: 0,
                            startup_cost: 0.0,
                            total_cost: 0.0,
                            pathkeys: Vec::new(),
                        };
                        cost_sort_owned(
                            &mut sort_path,
                            &apath_pathkeys,
                            s_disabled,
                            s_total,
                            s_rows,
                            s_width,
                            0.0,
                            work_mem(),
                            -1.0,
                        );
                        (sort_path.startup_cost, sort_path.total_cost, sort_path.disabled_nodes)
                    };

                if acc_startup < 0.0 {
                    acc_startup = sort_startup;
                }
                acc_rows += s_rows;
                acc_disabled += sort_disabled;
                acc_total += sort_total;
            }
            if acc_startup < 0.0 {
                acc_startup = 0.0;
            }
        }
    } else {
        // parallel-aware
        let mut i: i32 = 0;
        let (parallel_divisor, _) = {
            let ap_path = match root.path(path_id) {
                PathNode::AppendPath(ap) => &ap.path,
                _ => unreachable!(),
            };
            debug_assert!(ap_path.pathkeys.is_empty());
            (get_parallel_divisor(ap_path), ())
        };

        for subpath_id in &subpaths {
            let (s_startup, s_rows, s_total, s_disabled) = {
                let sp = root.path(*subpath_id).base();
                (sp.startup_cost, sp.rows, sp.total_cost, sp.disabled_nodes)
            };

            if i == 0 {
                acc_startup = s_startup;
            } else if i < parallel_workers {
                acc_startup = Min(acc_startup, s_startup);
            }

            if i < first_partial_path {
                acc_rows += s_rows / parallel_divisor;
            } else {
                let subpath_parallel_divisor = get_parallel_divisor(root.path(*subpath_id).base());
                acc_rows += s_rows * (subpath_parallel_divisor / parallel_divisor);
                acc_total += s_total;
            }

            acc_disabled += s_disabled;
            acc_rows = clamp_row_est(acc_rows);
            i += 1;
        }

        acc_total += append_nonpartial_cost(root, &subpaths, first_partial_path, parallel_workers);
    }

    // Append per-tuple overhead.
    acc_total += cpu_tuple_cost() * APPEND_CPU_COST_MULTIPLIER * acc_rows;

    let p = root.path_mut(path_id).base_mut();
    p.disabled_nodes = acc_disabled;
    p.startup_cost = acc_startup;
    p.total_cost = acc_total;
    p.rows = acc_rows;
}

/* ==========================================================================
 * Expression-cost evaluation (costsize.c:4756-5086).
 *
 * The recursive `cost_qual_eval_walker` (FuncExpr/OpExpr/Aggref/SubPlan/… +
 * pg_proc.procost) crosses the full node vocabulary + catalog, so it is routed
 * whole through the `cost_qual_eval_walker` seam (single node). The
 * list-accumulation wrapper is in-crate.
 * ========================================================================== */

/// `cost_qual_eval` (costsize.c:4756) over resolved RestrictInfo clause handles.
/// The `quals` are the clause-expr `NodeId`s; the walker recursion crosses the
/// seam, the accumulation stays in-crate.
pub fn cost_qual_eval(root: &PlannerInfo, quals: &[types_pathnodes::NodeId]) -> QualCost {
    let mut total = QualCost::default();
    for &q in quals {
        let (s, p) = cz::cost_qual_eval_walker::call(root, q);
        total.startup += s;
        total.per_tuple += p;
    }
    total
}

/// `cost_qual_eval_node` (costsize.c:4782) — single node.
pub fn cost_qual_eval_node(root: &PlannerInfo, qual: types_pathnodes::NodeId) -> QualCost {
    let (s, p) = cz::cost_qual_eval_walker::call(root, qual);
    QualCost {
        startup: s,
        per_tuple: p,
    }
}

/// Resolve a slice of `RinfoId` clause handles to their clause-expr `NodeId`s.
pub(crate) fn rinfo_clause_nodes(root: &PlannerInfo, ids: &[RinfoId]) -> Vec<types_pathnodes::NodeId> {
    ids.iter().map(|id| root.rinfo(*id).clause).collect()
}

/// `cost_qual_eval` over a list of `RinfoId` clause handles.
pub(crate) fn cost_qual_eval_rinfos(root: &PlannerInfo, ids: &[RinfoId]) -> QualCost {
    let nodes = rinfo_clause_nodes(root, ids);
    cost_qual_eval(root, &nodes)
}

/// `get_restriction_qual_cost` (costsize.c:5071). The scan estimators use the
/// snapshot-friendly `scans::get_restriction_qual_cost_ext`; this canonical
/// form is kept for fidelity to the C signature.
#[allow(dead_code)]
pub(crate) fn get_restriction_qual_cost(
    root: &PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) -> QualCost {
    if let Some(pi) = param_info {
        let nodes = rinfo_clause_nodes(root, &pi.ppi_clauses);
        let mut qpqual_cost = cost_qual_eval(root, &nodes);
        qpqual_cost.startup += baserel.baserestrictcost.startup;
        qpqual_cost.per_tuple += baserel.baserestrictcost.per_tuple;
        qpqual_cost
    } else {
        baserel.baserestrictcost
    }
}

/* ==========================================================================
 * Small arena helpers.
 * ========================================================================== */

/// Read a path's base `Path` immutably.
#[inline]
pub(crate) fn path_base(root: &PlannerInfo, id: PathId) -> &Path {
    root.path(id).base()
}

/// `bms_is_subset(a, b)` over the node-field `Relids`.
pub(crate) fn bms_is_subset(a: &types_pathnodes::Relids, b: &types_pathnodes::Relids) -> bool {
    use backend_optimizer_util_relnode_seams as bms;
    bms::relids_is_subset::call(a, b)
}

/* ==========================================================================
 * Seam installation.
 * ========================================================================== */

include!("init_seams.rs");
