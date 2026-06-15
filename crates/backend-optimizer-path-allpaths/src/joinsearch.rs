//! Join-search driver: `make_rel_from_joinlist` (allpaths.c:3351) and
//! `standard_join_search` (allpaths.c:3456).

use alloc::vec::Vec;

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_pathnodes::{PlannerInfo, RelId};

use backend_optimizer_path_joinrels::join_search_one_level;
use backend_optimizer_util_pathnode_seams as pathnode;
use backend_optimizer_util_relnode_seams as bms;

use crate::{enable_geqo, geqo_threshold, generate_partitionwise_join_paths,
            generate_useful_gather_paths};

/// A node of the planner's "joinlist" (see `deconstruct_jointree`): either a
/// leaf range-table reference (`RangeTblRef`, by 1-based rtindex) or a nested
/// sub-joinlist (`List`). This is the owned analogue of the C `List *joinlist`
/// whose elements are `RangeTblRef *` or `List *`.
#[derive(Clone, Debug)]
pub enum JoinlistNode {
    /// `RangeTblRef { rtindex }`.
    Rel(i32),
    /// A nested sub-joinlist (`List`).
    Sub(Vec<JoinlistNode>),
}

/// `make_rel_from_joinlist` (allpaths.c:3351) — build access paths using a
/// joinlist to guide the join-path search. Returns the final rel, or `None` for
/// an empty joinlist.
pub fn make_rel_from_joinlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    joinlist: &[JoinlistNode],
) -> PgResult<Option<RelId>> {
    // The number of child joinlist nodes is the DP depth.
    let levels_needed = joinlist.len() as i32;
    if levels_needed <= 0 {
        return Ok(None); // nothing to do?
    }

    // Build the list of rels for the child joinlist nodes (base rels and rels
    // constructed from sub-joinlists).
    let mut initial_rels: Vec<RelId> = Vec::new();
    for jlnode in joinlist {
        let thisrel = match jlnode {
            JoinlistNode::Rel(varno) => bms::find_base_rel::call(root, *varno),
            JoinlistNode::Sub(sub) => match make_rel_from_joinlist(mcx, root, sub)? {
                Some(r) => r,
                None => {
                    return Err(PgError::error("unrecognized joinlist node type: empty sublist"))
                }
            },
        };
        initial_rels.push(thisrel);
    }

    if levels_needed == 1 {
        // Single joinlist node, so we're done.
        return Ok(Some(initial_rels[0]));
    }

    // Consider the join orderings: a plugin (NULL in core), GEQO, or the regular
    // join search. initial_rels goes into PlannerInfo because
    // has_legal_joinclause() reads it.
    root.initial_rels = initial_rels.clone();

    // join_search_hook is NULL in core PostgreSQL.
    if enable_geqo() && levels_needed >= geqo_threshold() {
        let config = geqo_config_from_gucs();
        Ok(Some(backend_geqo_all::main::geqo(
            root,
            levels_needed,
            initial_rels,
            &config,
        )))
    } else {
        Ok(Some(standard_join_search(mcx, root, levels_needed, initial_rels)?))
    }
}

/// `GeqoConfig` from the live `Geqo_*` GUCs (C reads these as globals).
fn geqo_config_from_gucs() -> backend_geqo_all::main::GeqoConfig {
    use backend_utils_misc_guc_tables::vars;
    backend_geqo_all::main::GeqoConfig::from_gucs(
        vars::Geqo_effort.read(),
        vars::Geqo_pool_size.read(),
        vars::Geqo_generations.read(),
        vars::Geqo_selection_bias.read(),
        vars::Geqo_seed.read(),
    )
}

/// `standard_join_search` (allpaths.c:3456) — find join paths by successively
/// joining component relations (the dynamic-programming join search). Returns
/// the final-level rel (the join of all original relations).
pub fn standard_join_search<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    levels_needed: i32,
    initial_rels: Vec<RelId>,
) -> PgResult<RelId> {
    // Cannot be invoked recursively: join_rel_level must be unused.
    debug_assert!(root.join_rel_level.is_empty());

    // join_rel_level[j] is the list of j-item rels; allocate [0..=levels_needed].
    root.join_rel_level = Vec::new();
    for _ in 0..=(levels_needed as usize) {
        root.join_rel_level.push(Vec::new());
    }
    root.join_rel_level[1] = initial_rels;

    for lev in 2..=levels_needed {
        // Build all join rels at this level.
        join_search_one_level(mcx, root, lev)?;

        // Then, for each just-processed joinrel, run partitionwise-join +
        // (non-topmost) gather + set_cheapest. (Deferred until now because both
        // regular and partial paths can be added at multiple times within
        // join_search_one_level.)
        let level_rels: Vec<RelId> = root.join_rel_level[lev as usize].clone();
        for rel in level_rels {
            generate_partitionwise_join_paths(mcx, root, rel)?;

            // Except for the topmost scan/join rel, consider gathering partial
            // paths. (The topmost rel is postponed to grouping_planner.)
            if !bms::relids_equal::call(&root.rel(rel).relids, &root.all_query_rels) {
                generate_useful_gather_paths(root, rel, false)?;
            }

            pathnode::set_cheapest::call(root, rel)?;
        }
    }

    // We should have a single rel at the final level.
    if root.join_rel_level[levels_needed as usize].is_empty() {
        return Err(PgError::error(alloc::format!(
            "failed to build any {levels_needed}-way joins"
        )));
    }
    debug_assert_eq!(root.join_rel_level[levels_needed as usize].len(), 1);

    let rel = root.join_rel_level[levels_needed as usize][0];

    // Clear join_rel_level (C sets it back to NULL).
    root.join_rel_level = Vec::new();

    Ok(rel)
}

extern crate alloc;
