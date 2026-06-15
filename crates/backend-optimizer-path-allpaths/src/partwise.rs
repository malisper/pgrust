//! `generate_partitionwise_join_paths` (allpaths.c:4361).

use alloc::vec::Vec;

use mcx::Mcx;
use types_error::PgResult;
use types_pathnodes::{PlannerInfo, RelId, RELOPT_JOINREL};

use backend_optimizer_path_joinrels::{is_dummy_rel, mark_dummy_rel};
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_tcop_postgres_seams as tcop;

use crate::add_paths_to_append_rel;

/// `IS_JOIN_REL(rel)` (pathnodes.h) — a joinrel (reloptkind == RELOPT_JOINREL).
#[inline]
fn is_join_rel(rel: &types_pathnodes::RelOptInfo) -> bool {
    rel.reloptkind == RELOPT_JOINREL
}

/// `IS_PARTITIONED_REL(rel)` field-only conjuncts (pathnodes.h): has a scheme,
/// bound info, at least one partition, and the per-partition rel array. (The
/// macro's `&& !IS_DUMMY_REL` conjunct is applied at the callsite as in C.)
#[inline]
fn is_partitioned_rel(rel: &types_pathnodes::RelOptInfo) -> bool {
    rel.part_scheme.is_some()
        && rel.boundinfo.is_some()
        && rel.nparts > 0
        && !rel.part_rels.is_empty()
}

/// `generate_partitionwise_join_paths` (allpaths.c:4361) — create partitionwise
/// join paths for a partitioned join relation, recursing into child-joins.
pub fn generate_partitionwise_join_paths<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<()> {
    // Handle only join relations here.
    if !is_join_rel(root.rel(rel)) {
        return Ok(());
    }

    // Nothing to do if the relation is not partitioned.
    if !is_partitioned_rel(root.rel(rel)) {
        return Ok(());
    }

    // The relation should have consider_partitionwise_join set.
    debug_assert!(root.rel(rel).consider_partitionwise_join);

    // Guard against stack overflow due to overly deep partition hierarchy.
    tcop::check_stack_depth::call()?;

    let num_parts = root.rel(rel).nparts;
    let part_rels: Vec<Option<RelId>> = root.rel(rel).part_rels.clone();

    // Collect non-dummy child-joins.
    let mut live_children: Vec<RelId> = Vec::new();
    for cnt_parts in 0..num_parts as usize {
        let child_rel = match part_rels[cnt_parts] {
            // If it's been pruned entirely, it's certainly dummy.
            None => continue,
            Some(c) => c,
        };

        // Make partitionwise join paths for this partitioned child-join.
        generate_partitionwise_join_paths(mcx, root, child_rel)?;

        // If we failed to make any path for this child, we must give up.
        if root.rel(child_rel).pathlist.is_empty() {
            // Mark the parent joinrel as unpartitioned for later functions.
            root.rel_mut(rel).nparts = 0;
            return Ok(());
        }

        // Else, identify the cheapest path for it.
        pathnode::set_cheapest::call(root, child_rel)?;

        // Dummy children need not be scanned.
        if is_dummy_rel(root, child_rel) {
            continue;
        }

        live_children.push(child_rel);
    }

    // If all child-joins are dummy, the parent join is also dummy.
    if live_children.is_empty() {
        mark_dummy_rel(root, rel)?;
        return Ok(());
    }

    // Build additional paths for this rel from the child-join paths.
    add_paths_to_append_rel(mcx, root, rel, &live_children)?;
    Ok(())
}

extern crate alloc;
