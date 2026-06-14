//! `geqo_eval.c` — routines to evaluate query trees.
//!
//! `geqo_eval` builds the join tree for a proposed tour and returns its cheapest
//! total cost (the individual's fitness), or `DBL_MAX` if no legal join order
//! can be extracted. `gimme_tree` is the clump-merging heuristic that turns a
//! tour into a valid (possibly bushy) join `RelOptInfo`.
//!
//! The clump-merging *algorithm* (`gimme_tree`/`merge_clump`/`desirable_join`)
//! is ported 1:1 in-crate. The planner externals it orchestrates
//! (`make_join_rel` + partitionwise/gather path generation + `set_cheapest`,
//! plus the two `desirable_join` predicates) and the private temp
//! `MemoryContext` cross the boundary through
//! [`backend_geqo_all_seams`].

use crate::{Gene, GeqoPrivateData};
use alloc::vec::Vec;
use backend_geqo_all_seams as geqo_seams;
use types_pathnodes::{PlannerInfo, RelId};
use types_core::primitive::Cost;

/// `DBL_MAX` (`<float.h>`) — fitness sentinel for an invalid join order.
const DBL_MAX: Cost = f64::MAX;

/// A "clump" of already-joined relations within `gimme_tree`
/// (`geqo_eval.c` file-local struct).
struct Clump {
    /// joinrel for the set of relations (arena handle)
    joinrel: RelId,
    /// number of input relations in clump
    size: i32,
}

/// `geqo_eval(root, tour, num_gene)` — returns the cost of the query tree for
/// this tour, or `DBL_MAX` if no legal join order exists.
///
/// Runs inside a private temp `MemoryContext` (seamed) so the many discarded
/// join rels are reclaimed between evaluations. `gimme_tree` appends entries to
/// `root.join_rel_list`; we save its length beforehand and truncate back to it
/// afterward (the C `list_truncate`), and temporarily null `join_rel_hash` so a
/// fresh local hash is built and the outer one is untouched.
pub fn geqo_eval(
    root: &mut PlannerInfo,
    private: &mut GeqoPrivateData,
    tour: &[Gene],
    num_gene: i32,
) -> Cost {
    /*
     * Create a private memory context that will hold all temp storage
     * allocated inside gimme_tree(), as a child of the planner's normal
     * context so it is freed even on ereport(ERROR). Switch into it; the seam
     * returns the saved old context.
     */
    let oldcxt = geqo_seams::geqo_eval_context_create::call();

    /*
     * gimme_tree will add entries to root->join_rel_list, which the
     * MemoryContextDelete below will recycle, so restore the list to its
     * former length on exit (entries are always appended at the end). Also
     * temporarily null join_rel_hash so a new local hash is built and the
     * outer hashtable is left untouched. join_rel_level[] shouldn't be in use.
     */
    let savelength = root.join_rel_list.len();
    let savehash = core::mem::take(&mut root.join_rel_hash);
    debug_assert!(root.join_rel_level.is_empty());

    // root->join_rel_hash = NULL;  (done by the take above)

    /* construct the best path for the given combination of relations */
    let joinrel = gimme_tree(root, private, tour, num_gene);

    /*
     * compute fitness, if we found a valid join
     *
     * XXX geqo does not currently support optimization for partial result
     * retrieval, nor do we take any cognizance of possible use of
     * parameterized paths --- how to fix?
     */
    let fitness = if let Some(joinrel) = joinrel {
        let best_path = root
            .rel(joinrel)
            .cheapest_total_path
            .expect("geqo_eval: joinrel has no cheapest_total_path");
        root.path(best_path).base().total_cost
    } else {
        DBL_MAX
    };

    /*
     * Restore join_rel_list to its former state, and put back original
     * hashtable if any.
     */
    root.join_rel_list.truncate(savelength);
    root.join_rel_hash = savehash;

    /* release all the memory acquired within gimme_tree */
    geqo_seams::geqo_eval_context_delete::call(oldcxt);

    fitness
}

/// `gimme_tree(root, tour, num_gene)` — form planner estimates for a join tree
/// constructed (heuristically) in the order given by `tour`. Returns a new join
/// `RelOptInfo` whose cheapest path is the best plan for this join order, or
/// `None` if the order is invalid and we can't fix it.
///
/// Maintains a list of "clumps" of successfully joined relations (larger clumps
/// at the front). Each tour relation is added to the first clump it can join
/// to (else becomes its own clump); enlarging a clump may let it merge with
/// others. After the tour is scanned, remaining clumps are force-joined in some
/// legal order; failure to reach a single clump means failure.
pub fn gimme_tree(
    root: &mut PlannerInfo,
    private: &mut GeqoPrivateData,
    tour: &[Gene],
    num_gene: i32,
) -> Option<RelId> {
    let mut clumps: Vec<Clump> = Vec::new();

    for rel_count in 0..num_gene as usize {
        /* Get the next input relation */
        let cur_rel_index = tour[rel_count] as usize;
        let cur_rel = private.initial_rels[cur_rel_index - 1];

        /* Make it into a single-rel clump */
        let cur_clump = Clump {
            joinrel: cur_rel,
            size: 1,
        };

        /* Merge it into the clumps list, using only desirable joins */
        clumps = merge_clump(root, private, clumps, cur_clump, num_gene, false);
    }

    if clumps.len() > 1 {
        /* Force-join the remaining clumps in some legal order */
        let mut fclumps: Vec<Clump> = Vec::new();
        for clump in clumps.into_iter() {
            fclumps = merge_clump(root, private, fclumps, clump, num_gene, true);
        }
        clumps = fclumps;
    }

    /* Did we succeed in forming a single join relation? */
    if clumps.len() != 1 {
        return None;
    }

    Some(clumps.into_iter().next().unwrap().joinrel)
}

/// `merge_clump(root, clumps, new_clump, num_gene, force)` — merge `new_clump`
/// into the existing clumps, repeating while successful; when no more merging
/// is possible, insert it preserving the larger-first ordering. With `force`,
/// merge anywhere a join is legal (even a cartesian join); otherwise only
/// "desirable" joins.
fn merge_clump(
    root: &mut PlannerInfo,
    private: &mut GeqoPrivateData,
    mut clumps: Vec<Clump>,
    new_clump: Clump,
    num_gene: i32,
    force: bool,
) -> Vec<Clump> {
    /* Look for a clump that new_clump can join to */
    let mut idx = 0;
    while idx < clumps.len() {
        let old_joinrel = clumps[idx].joinrel;

        if force || desirable_join(root, old_joinrel, new_clump.joinrel) {
            /*
             * Construct a RelOptInfo representing the join of these two input
             * relations (build + partitionwise/gather paths + set_cheapest),
             * expecting the joinrel not to exist yet so only the wanted paths
             * are built. Seamed; returns None if the join order is invalid.
             */
            let joinrel = geqo_seams::build_and_cost_join_rel::call(
                root,
                old_joinrel,
                new_clump.joinrel,
            );

            /* Keep searching if join order is not valid */
            if let Some(joinrel) = joinrel {
                /* Absorb new clump into old */
                let mut old_clump = clumps.remove(idx);
                old_clump.joinrel = joinrel;
                old_clump.size += new_clump.size;

                /*
                 * Recursively try to merge the enlarged old_clump with others.
                 * When no further merge is possible, we'll reinsert it.
                 */
                return merge_clump(root, private, clumps, old_clump, num_gene, force);
            }
        }

        idx += 1;
    }

    /*
     * No merging is possible, so add new_clump as an independent clump, in
     * proper order according to size. Fast path for the common size-1 case ---
     * it should always go at the end.
     */
    if clumps.is_empty() || new_clump.size == 1 {
        clumps.push(new_clump);
        return clumps;
    }

    /* Else search for the place to insert it */
    let mut pos = 0;
    while pos < clumps.len() {
        if new_clump.size > clumps[pos].size {
            break; /* new_clump belongs before old_clump */
        }
        pos += 1;
    }
    clumps.insert(pos, new_clump);

    clumps
}

/// `desirable_join(root, outer_rel, inner_rel)` — heuristic for `gimme_tree`:
/// join if there is an applicable join clause, or a join-order restriction
/// forcing these rels together; otherwise postpone.
fn desirable_join(root: &PlannerInfo, outer_rel: RelId, inner_rel: RelId) -> bool {
    if geqo_seams::have_relevant_joinclause::call(root, outer_rel, inner_rel)
        || geqo_seams::have_join_order_restriction::call(root, outer_rel, inner_rel)
    {
        return true;
    }

    /* Otherwise postpone the join till later. */
    false
}
