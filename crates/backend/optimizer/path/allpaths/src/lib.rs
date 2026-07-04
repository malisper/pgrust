//! allpaths.c: only compute_parallel_worker (+ its GUCs) so far; path
//! generation rides the planner crate until phase 3 wires Gather paths.

use types_pathnodes::{RelOptInfo, RELOPT_BASEREL};

pub mod gucs;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    gucs::install();
}

pub fn compute_parallel_worker(
    rel: &RelOptInfo<'_>,
    heap_pages: f64,
    index_pages: f64,
    max_workers: i32,
) -> i32 {
    let mut parallel_workers = 0;

    if rel.rel_parallel_workers != -1 {
        parallel_workers = rel.rel_parallel_workers;
    } else {
        let min_table = gucs::min_parallel_table_scan_size();
        let min_index = gucs::min_parallel_index_scan_size();

        // Too-small rels get no workers — unless it's an inheritance child,
        // which may pay off combined with its siblings.
        if rel.reloptkind == RELOPT_BASEREL
            && ((heap_pages >= 0.0 && heap_pages < min_table as f64)
                || (index_pages >= 0.0 && index_pages < min_index as f64))
        {
            return 0;
        }

        if heap_pages >= 0.0 {
            // log3(size) rule; the GUC's max (INT_MAX/3) prevents overflow.
            let mut heap_parallel_threshold = min_table.max(1);
            let mut heap_parallel_workers = 1;
            while heap_pages >= (heap_parallel_threshold * 3) as f64 {
                heap_parallel_workers += 1;
                heap_parallel_threshold *= 3;
                if heap_parallel_threshold > i32::MAX / 3 {
                    break;
                }
            }
            parallel_workers = heap_parallel_workers;
        }

        if index_pages >= 0.0 {
            let mut index_parallel_threshold = min_index.max(1);
            let mut index_parallel_workers = 1;
            while index_pages >= (index_parallel_threshold * 3) as f64 {
                index_parallel_workers += 1;
                index_parallel_threshold *= 3;
                if index_parallel_threshold > i32::MAX / 3 {
                    break;
                }
            }
            if parallel_workers > 0 {
                parallel_workers = parallel_workers.min(index_parallel_workers);
            } else {
                parallel_workers = index_parallel_workers;
            }
        }
    }

    parallel_workers.min(max_workers)
}
