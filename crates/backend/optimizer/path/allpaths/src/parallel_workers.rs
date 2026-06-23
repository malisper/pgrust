//! `compute_parallel_worker` (allpaths.c:4273).

use ::pathnodes::{PlannerInfo, RelId, RELOPT_BASEREL};

use crate::{min_parallel_index_scan_size, min_parallel_table_scan_size};

/// `compute_parallel_worker` (allpaths.c:4273) — the number of parallel workers
/// to scan a relation, based on the log of the heap/index page counts (or the
/// `parallel_workers` reloption when set), capped at `max_workers`.
///
/// `heap_pages`/`index_pages` are `-1` to mean "don't expect to scan any".
pub fn compute_parallel_worker(
    root: &PlannerInfo,
    rel: RelId,
    heap_pages: f64,
    index_pages: f64,
    max_workers: i32,
) -> i32 {
    let _ = root;
    let r = root.rel(rel);
    let mut parallel_workers: i32 = 0;

    // If the user set the parallel_workers reloption, use that; else default.
    if r.rel_parallel_workers != -1 {
        parallel_workers = r.rel_parallel_workers;
    } else {
        // Too few pages to justify a parallel scan -> zero, unless it's an
        // inheritance child (we still generate a parallel path then).
        if r.reloptkind == RELOPT_BASEREL
            && ((heap_pages >= 0.0 && (heap_pages as i64) < min_parallel_table_scan_size() as i64)
                || (index_pages >= 0.0
                    && (index_pages as i64) < min_parallel_index_scan_size() as i64))
        {
            return 0;
        }

        if heap_pages >= 0.0 {
            // Workers based on the log (base 3) of the relation size.
            let mut heap_parallel_threshold: i32 = min_parallel_table_scan_size().max(1);
            let mut heap_parallel_workers: i32 = 1;
            while heap_pages >= (heap_parallel_threshold as f64) * 3.0 {
                heap_parallel_workers += 1;
                heap_parallel_threshold *= 3;
                if heap_parallel_threshold > i32::MAX / 3 {
                    break; // avoid overflow
                }
            }
            parallel_workers = heap_parallel_workers;
        }

        if index_pages >= 0.0 {
            let mut index_parallel_threshold: i32 = min_parallel_index_scan_size().max(1);
            let mut index_parallel_workers: i32 = 1;
            while index_pages >= (index_parallel_threshold as f64) * 3.0 {
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

    // In no case use more than the caller-supplied maximum.
    parallel_workers.min(max_workers)
}
