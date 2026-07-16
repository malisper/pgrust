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

// pgrcolumnar's parallel claim unit is one row group (pgrcolumnar::format::RG_ROWS;
// the shared phs_nallocated cursor advances one RG per claim).
pub const PGRCOLUMNAR_RG_ROWS: f64 = 65_536.0;

// Minimum claim units per worker: below this, worker startup + tqueue setup
// can't amortize and the shared cursor degenerates to single-claim workers.
pub const PGRCOLUMNAR_RGS_PER_WORKER: f64 = 4.0;

/// pgrcolumnar analog of `compute_parallel_worker`'s heap-pages arm.
///
/// C's intent is "relation size decides worker count, reloption overrides":
/// heap sizes with a log3 ladder over pages because heap workers share one
/// block-range cursor and the ladder is deliberately conservative for
/// row-store scans. pgrcolumnar's scan geometry is different — the parallel
/// claim unit is a whole row group (RG_ROWS = 65536 rows) and per-RG decode
/// is embarrassingly parallel — so the mapping is linear in claim units:
///
///   nrg     = ceil(tuples / RG_ROWS)
///   workers = clamp(nrg / PGRCOLUMNAR_RGS_PER_WORKER, 1, max_workers)
///
/// with C's small-table gate expressed in pgrcolumnar units: a plain baserel
/// under PGRCOLUMNAR_RGS_PER_WORKER row groups (< ~262k rows) plans serial
/// (inheritance children skip the gate, as C skips min_parallel_table_scan_
/// size for them, since siblings combine). A 10M-row bank (153 RGs) sizes
/// to 38 pre-clamp — i.e. machine-sized DOP on big banks, vs the page
/// ladder's 6 (heap-page heuristics over compressed bytes undercount the
/// available scan parallelism ~2.7x there; the ARMED16 audit leg measured
/// the healthy band gaining ~1.9x at DOP 16).
///
/// `rel_parallel_workers` (the parallel_workers reloption) overrides the
/// computation entirely, exactly as in C.
pub fn compute_pgrcolumnar_parallel_worker(
    rel: &RelOptInfo<'_>,
    tuples: f64,
    max_workers: i32,
) -> i32 {
    if rel.rel_parallel_workers != -1 {
        return rel.rel_parallel_workers.min(max_workers);
    }
    let nrg = (tuples / PGRCOLUMNAR_RG_ROWS).ceil().max(0.0);
    if rel.reloptkind == RELOPT_BASEREL && nrg < PGRCOLUMNAR_RGS_PER_WORKER {
        return 0;
    }
    let workers = (nrg / PGRCOLUMNAR_RGS_PER_WORKER).floor() as i32;
    workers.max(1).min(max_workers)
}
