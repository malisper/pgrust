//! `set_dummy_rel_pathlist` (allpaths.c:2215).

use types_error::PgResult;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, RelId};

use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;

/// `set_dummy_rel_pathlist` (allpaths.c:2215) — build a dummy path for a
/// relation excluded by constraints.
///
/// Represented as a childless `AppendPath` (see `IS_DUMMY_APPEND`/`IS_DUMMY_REL`).
/// The C `create_append_path(NULL, ...)` passes `root == NULL`; here that is the
/// `have_root = false` flag on the seam.
pub fn set_dummy_rel_pathlist<'mcx>(root: &mut PlannerInfo, run: &PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    // Dummy size estimates (leave attr_widths[] as zeroes).
    root.rel_mut(rel).rows = 0.0;
    if let Some(t) = root.rel_mut(rel).reltarget.as_mut() {
        t.width = 0;
    }

    // Discard any pre-existing paths; no further need for them.
    root.rel_mut(rel).pathlist.clear();
    root.rel_mut(rel).partial_pathlist.clear();

    // Set up the dummy path (in the rel's own context; root == NULL in C).
    let lateral = bms::relids_copy::call(&root.rel(rel).lateral_relids);
    let path = pathnode::create_append_path::call(
        root,
        run,
        /* have_root = */ false,
        rel,
        /* subpaths */ alloc::vec::Vec::new(),
        /* partial_subpaths */ alloc::vec::Vec::new(),
        /* pathkeys */ alloc::vec::Vec::new(),
        &lateral,
        /* parallel_workers */ 0,
        /* parallel_aware */ false,
        /* rows */ -1.0,
    )?;
    pathnode::add_path::call(root, rel, path)?;

    // Set the cheapest-path fields immediately (redundant but cheap, for safety
    // and consistency with mark_dummy_rel).
    pathnode::set_cheapest::call(root, rel)?;
    Ok(())
}

extern crate alloc;
