//! geqo_eval.c — the clump-merging heuristic (ported 1:1) whose joinrel
//! construction reuses make_join_rel + partitionwise/gather + set_cheapest
//! exactly as standard_join_search does.

use types_core::primitive::Cost;
use types_error::PgResult;
use types_pathnodes::RelId;

use super::Gene;
use crate::joinrels::{have_join_order_restriction, have_relevant_joinclause, make_join_rel};
use crate::run::PlannerRun;

const DBL_MAX: Cost = f64::MAX;

// A "clump" of already-joined relations within gimme_tree.
struct Clump {
    joinrel: RelId,
    size: i32,
}

// Cheapest total cost of the join tree for this tour, or DBL_MAX if no legal
// order exists. C's per-eval temp MemoryContext is the bounded divergence in
// the mod header; the logical save/restore is reproduced: gimme_tree appends
// to join_rel_list (truncated back) and join_rel_hash is nulled so a fresh
// local hash is built and any outer one is left untouched.
pub(super) fn geqo_eval<'mcx>(
    run: &mut PlannerRun<'mcx>,
    initial_rels: &[RelId],
    tour: &[Gene],
    num_gene: i32,
) -> PgResult<Cost> {
    let savelength = run.root.join_rel_list.len();
    let savehash = core::mem::take(&mut run.root.join_rel_hash);
    debug_assert!(run.root.join_rel_level.is_empty());

    let joinrel = gimme_tree(run, initial_rels, tour, num_gene)?;

    // Like C, GEQO ignores partial-retrieval and parameterized-path costs.
    let fitness = match joinrel {
        Some(rel) => {
            let best = run
                .root
                .rel(rel)
                .cheapest_total_path
                .expect("geqo_eval: joinrel has no cheapest_total_path");
            run.root.path(best).base().total_cost
        }
        None => DBL_MAX,
    };

    run.root.join_rel_list.truncate(savelength);
    run.root.join_rel_hash = savehash;
    Ok(fitness)
}

// Build a (possibly bushy) join rel following `tour` as a guideline,
// postponing illegal/undesirable joins. None if no single join rel forms.
pub(super) fn gimme_tree<'mcx>(
    run: &mut PlannerRun<'mcx>,
    initial_rels: &[RelId],
    tour: &[Gene],
    num_gene: i32,
) -> PgResult<Option<RelId>> {
    let mut clumps: Vec<Clump> = Vec::new();

    for rel_count in 0..num_gene as usize {
        let cur_rel_index = tour[rel_count] as usize;
        let cur_rel = initial_rels[cur_rel_index - 1];
        let cur_clump = Clump { joinrel: cur_rel, size: 1 };
        clumps = merge_clump(run, clumps, cur_clump, num_gene, false)?;
    }

    if clumps.len() > 1 {
        // Force-join the remaining clumps in some legal order.
        let mut fclumps: Vec<Clump> = Vec::new();
        for clump in clumps.into_iter() {
            fclumps = merge_clump(run, fclumps, clump, num_gene, true)?;
        }
        clumps = fclumps;
    }

    if clumps.len() != 1 {
        return Ok(None);
    }
    Ok(Some(clumps.into_iter().next().unwrap().joinrel))
}

// Merge new_clump into the clumps (repeating while successful); else insert
// keeping larger clumps first. `force` merges any legal join (even a
// cartesian), otherwise only "desirable" ones.
fn merge_clump<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut clumps: Vec<Clump>,
    new_clump: Clump,
    num_gene: i32,
    force: bool,
) -> PgResult<Vec<Clump>> {
    let mut idx = 0;
    while idx < clumps.len() {
        let old_joinrel = clumps[idx].joinrel;
        if force || desirable_join(run, old_joinrel, new_clump.joinrel)? {
            // make_join_rel returns None for an illegal order (keep searching).
            // Then run standard_join_search's per-joinrel finishing sequence,
            // which make_join_rel itself does not do.
            let joinrel = make_join_rel(run, old_joinrel, new_clump.joinrel)?;
            if let Some(joinrel) = joinrel {
                crate::allpaths::generate_partitionwise_join_paths(run, joinrel)?;
                if !crate::relnode::relids_equal(
                    &run.root.rel(joinrel).relids,
                    &run.root.all_query_rels,
                ) {
                    crate::allpaths::generate_useful_gather_paths(run, joinrel, false)?;
                }
                crate::pathnode::set_cheapest(run, joinrel)?;

                // Absorb new_clump into old, then recurse to merge the enlarged
                // clump; it is reinserted when no further merge works.
                let mut old_clump = clumps.remove(idx);
                old_clump.joinrel = joinrel;
                old_clump.size += new_clump.size;
                return merge_clump(run, clumps, old_clump, num_gene, force);
            }
        }
        idx += 1;
    }

    // No merge possible: insert in size order (size-1 clumps go at the end).
    if clumps.is_empty() || new_clump.size == 1 {
        clumps.push(new_clump);
        return Ok(clumps);
    }
    let mut pos = 0;
    while pos < clumps.len() {
        if new_clump.size > clumps[pos].size {
            break;
        }
        pos += 1;
    }
    clumps.insert(pos, new_clump);
    Ok(clumps)
}

// Join if there is an applicable join clause or a join-order restriction
// forcing the pair together; else postpone.
fn desirable_join<'mcx>(
    run: &mut PlannerRun<'mcx>,
    outer_rel: RelId,
    inner_rel: RelId,
) -> PgResult<bool> {
    if have_relevant_joinclause(run, outer_rel, inner_rel)
        || have_join_order_restriction(run, outer_rel, inner_rel)?
    {
        return Ok(true);
    }
    Ok(false)
}
