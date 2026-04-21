use crate::backend::optimizer::{
    CPU_INDEX_TUPLE_COST, CPU_OPERATOR_COST, CPU_TUPLE_COST, RANDOM_PAGE_COST,
};

fn clamp_rows(rows: f64) -> f64 {
    rows.max(1.0)
}

pub(super) fn estimate_gist_scan_cost(
    index_pages: f64,
    index_rows: f64,
    total_rows: f64,
    ordered: bool,
    order_by_keys: usize,
) -> (f64, f64) {
    let pages = index_pages.max(1.0);
    let rows = clamp_rows(index_rows);
    let total = total_rows.max(rows);
    let tuple_fraction = (rows / total).clamp(1.0 / total.max(1.0), 1.0);
    let page_fraction = tuple_fraction.sqrt().clamp(1.0 / pages, 1.0);
    let visited_pages = (pages * page_fraction).clamp(1.0, pages);
    let tree_height = if pages <= 1.0 {
        1.0
    } else {
        pages.log(100.0).ceil().max(1.0) + 1.0
    };

    if ordered && order_by_keys > 0 {
        let startup_pages = (tree_height + visited_pages.sqrt()).clamp(1.0, pages);
        let startup_cost =
            startup_pages * RANDOM_PAGE_COST + order_by_keys as f64 * CPU_OPERATOR_COST;
        let knn_tuples = clamp_rows(rows.sqrt().max(tree_height));
        let total_cost = startup_cost
            + visited_pages * RANDOM_PAGE_COST
            + knn_tuples * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
        (startup_cost, total_cost)
    } else {
        let startup_cost = tree_height * CPU_OPERATOR_COST;
        let total_cost = tree_height * RANDOM_PAGE_COST
            + visited_pages * RANDOM_PAGE_COST
            + rows * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
        (startup_cost, total_cost)
    }
}
