//! `create_partial_bitmap_paths` (allpaths.c:4237).

use types_error::PgResult;
use types_pathnodes::{PathId, PlannerInfo, RelId};

use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;

use crate::{compute_parallel_worker, max_parallel_workers_per_gather};

/// `create_partial_bitmap_paths` (allpaths.c:4237) — build a partial bitmap heap
/// path for the relation (parallel bitmap scan), if a parallel scan is justified.
///
/// `compute_bitmap_pages` lives in the costsize crate; we call it through the
/// costsize-seams (it is the `cost_bitmap_*` family the indxpath driver also uses).
pub fn create_partial_bitmap_paths<'mcx>(
    root: &mut PlannerInfo,
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
    bitmapqual: PathId,
) -> PgResult<()> {
    // Compute heap pages for the bitmap heap scan (loop_count = 1.0). The
    // costsize port returns (pages, cost, tuples); allpaths uses only `pages`.
    let (pages_fetched, _cost, _tuples) =
        backend_optimizer_path_costsize::scans::compute_bitmap_pages(root, rel, bitmapqual, 1.0);

    let parallel_workers =
        compute_parallel_worker(root, rel, pages_fetched, -1.0, max_parallel_workers_per_gather());

    if parallel_workers <= 0 {
        return Ok(());
    }

    let lateral = bms::relids_copy::call(&root.rel(rel).lateral_relids);
    let path = pathnode::create_bitmap_heap_path::call(
        root,
        run,
        rel,
        bitmapqual,
        &lateral,
        1.0,
        parallel_workers,
    )?;
    pathnode::add_partial_path::call(root, rel, path)?;
    Ok(())
}
