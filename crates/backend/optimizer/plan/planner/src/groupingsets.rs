//! Grouping-sets planning (planner.c).

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::Node;
use types_pathnodes::{GroupingSetData, NodeId, PathId, RelId, RollupData};

use crate::run::PlannerRun;

pub use types_pathnodes::run::GroupingSetsData;

/// C's `linitial(parse->groupingSets) != NIL` over an expanded set cell.
pub fn grouping_set_nonempty(node: Node<'_>) -> bool {
    node.as_int_list().is_some_and(|il| !il.is_nil())
}

fn int_list_members<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgVec<'mcx, i32> {
    let mut v = PgVec::new_in(mcx);
    match node.as_int_list() {
        Some(il) => v.extend(il.iter()),
            None => debug_assert!(node.as_list().is_some_and(|l| l.is_nil())),
    }
    v
}

/// preprocess_grouping_sets (planner.c); parse->groupingSets must already be
/// expanded into IntList cells of sortgrouprefs.
pub fn preprocess_grouping_sets<'mcx>(
    run: &mut PlannerRun<'mcx>,
) -> PgResult<GroupingSetsData<'mcx>> {
    let mcx = run.mcx;
    let parse = run.parse();

    let mut processed: PgVec<'mcx, NodeId> = PgVec::new_in(mcx);
    for gc_node in &parse.groupClause {
        processed.push(run.intern_expr(gc_node));
    }
    run.root.processed_groupClause = processed;

    let mut maxref: u32 = 0;
    let mut unhashable_refs: PgVec<'mcx, u32> = PgVec::new_in(mcx);
    let mut unsortable_refs: PgVec<'mcx, u32> = PgVec::new_in(mcx);
    for gc_node in &parse.groupClause {
        let gc = gc_node.as_sort_group_clause().expect("groupClause cell");
        maxref = maxref.max(gc.tleSortGroupRef);
        if !gc.hashable {
            unhashable_refs.push(gc.tleSortGroupRef);
        }
        if gc.sortop == 0 {
            unsortable_refs.push(gc.tleSortGroupRef);
        }
    }

    let mut tleref_to_colnum_map: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    tleref_to_colnum_map.resize(maxref as usize + 1, 0);

    let mut all_sets: PgVec<'mcx, PgVec<'mcx, i32>> = PgVec::new_in(mcx);
    for gset_node in &parse.groupingSets {
        all_sets.push(int_list_members(mcx, gset_node));
    }

    let overlap = |refs: &[u32], set: &[i32]| set.iter().any(|&r| refs.contains(&(r as u32)));

    let mut unsortable_sets: PgVec<'mcx, GroupingSetData<'mcx>> = PgVec::new_in(mcx);
    let sets = if !unsortable_refs.is_empty() {
        let mut sortable_sets: PgVec<'mcx, PgVec<'mcx, i32>> = PgVec::new_in(mcx);
        for gset in all_sets {
            if overlap(&unsortable_refs, &gset) {
                if overlap(&unhashable_refs, &gset) {
                    return Err(crate::grouping::could_not_implement("GROUP BY"));
                }
                let mut gs = GroupingSetData::new(mcx);
                for &r in gset.iter() {
                    gs.set.push(r as u32);
                }
                unsortable_sets.push(gs);
            } else {
                sortable_sets.push(gset);
            }
        }
        if !sortable_sets.is_empty() {
            extract_rollup_sets(mcx, sortable_sets)
        } else {
            PgVec::new_in(mcx)
        }
    } else {
        extract_rollup_sets(mcx, all_sets)
    };

    let mut gd = GroupingSetsData {
        rollups: PgVec::new_in(mcx),
        any_hashable: false,
        unsortable_refs,
        unhashable_refs,
        unsortable_sets,
        hash_sets_idx: PgVec::new_in(mcx),
        tleref_to_colnum_map,
        dNumHashGroups: 0.0,
    };

    let nchains = sets.len();
    for chain in sets {
        let mut rollup = RollupData::new(mcx);
        let sortclause =
            if nchains == 1 { run.parse().sortClause.clone_in(mcx)? } else { NodeList::nil() };
        let current_sets = reorder_grouping_sets(mcx, chain, &sortclause);
        let first_set = current_sets[0].set.iter().map(|&r| r as i32).collect::<Vec<i32>>();

        rollup.groupClause = if !first_set.is_empty() {
            crate::grouping::preprocess_groupclause(run, Some(&first_set))?
        } else {
            PgVec::new_in(mcx)
        };

        if !first_set.is_empty() && !overlap(&gd.unhashable_refs, &first_set) {
            rollup.hashable = true;
            gd.any_hashable = true;
        }

        rollup.gsets = remap_to_groupclause_idx(
            run,
            &rollup.groupClause,
            &current_sets,
            &mut gd.tleref_to_colnum_map,
        );
        rollup.gsets_data = current_sets;
        gd.rollups.push(rollup);
    }

    if !gd.unsortable_sets.is_empty() {
        // C annotates: no groupclause pinned yet; index against the original
        // groupclause for estimation.
        let group_clause =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.processed_groupClause);
        gd.hash_sets_idx = remap_to_groupclause_idx(
            run,
            &group_clause,
            &gd.unsortable_sets,
            &mut gd.tleref_to_colnum_map,
        );
        gd.any_hashable = true;
    }
    Ok(gd)
}

/// remap_to_groupclause_idx (planner.c).
pub fn remap_to_groupclause_idx<'mcx>(
    run: &PlannerRun<'mcx>,
    group_clause: &[NodeId],
    gsets: &[GroupingSetData<'mcx>],
    tleref_to_colnum_map: &mut [i32],
) -> PgVec<'mcx, PgVec<'mcx, i32>> {
    let mcx = run.mcx;
    for (i, &gc_id) in group_clause.iter().enumerate() {
        let gc = run.root.expr_node(gc_id).as_sort_group_clause().expect("group clause cell");
        tleref_to_colnum_map[gc.tleSortGroupRef as usize] = i as i32;
    }
    let mut result: PgVec<'mcx, PgVec<'mcx, i32>> = PgVec::new_in(mcx);
    for gs in gsets {
        let mut set: PgVec<'mcx, i32> = PgVec::new_in(mcx);
        for &r in gs.set.iter() {
            set.push(tleref_to_colnum_map[r as usize]);
        }
        result.push(set);
    }
    result
}

/// extract_rollup_sets (planner.c): minimum chain cover of the subset poset
/// via maximum bipartite matching; input smallest-first.
pub fn extract_rollup_sets<'mcx>(
    mcx: Mcx<'mcx>,
    grouping_sets: PgVec<'mcx, PgVec<'mcx, i32>>,
) -> PgVec<'mcx, PgVec<'mcx, PgVec<'mcx, i32>>> {
    let num_sets_raw = grouping_sets.len();
    let mut result: PgVec<'mcx, PgVec<'mcx, PgVec<'mcx, i32>>> = PgVec::new_in(mcx);

    let mut num_empty = 0usize;
    while num_empty < num_sets_raw && grouping_sets[num_empty].is_empty() {
        num_empty += 1;
    }
    if num_empty == num_sets_raw {
        result.push(grouping_sets);
        return result;
    }

    let mut orig_sets: Vec<PgVec<'mcx, PgVec<'mcx, i32>>> = Vec::with_capacity(num_sets_raw + 1);
    let mut set_masks: Vec<Vec<i32>> = Vec::with_capacity(num_sets_raw + 1);
    let mut adjacency: Vec<Vec<i16>> = Vec::with_capacity(num_sets_raw + 1);
    orig_sets.push(PgVec::new_in(mcx));
    set_masks.push(Vec::new());
    adjacency.push(Vec::new());

    let mut j_size = 0usize;
    let mut j = 0usize;
    let mut i = 1usize;
    for candidate in grouping_sets.into_iter().skip(num_empty) {
        let mut candidate_set: Vec<i32> = candidate.iter().copied().collect();
        candidate_set.sort_unstable();
        candidate_set.dedup();

        let mut dup_of = 0usize;
        if j_size == candidate.len() {
            for k in j..i {
                if set_masks[k] == candidate_set {
                    dup_of = k;
                    break;
                }
            }
        } else if j_size < candidate.len() {
            j_size = candidate.len();
            j = i;
        }

        if dup_of > 0 {
            orig_sets[dup_of].push(candidate);
        } else {
            // C fills the adjacency buffer with k descending but iterates
            // back-to-front: effective visit order is ascending k.
            let mut adj: Vec<i16> = Vec::new();
            for k in 1..j {
                if set_masks[k].iter().all(|x| candidate_set.binary_search(x).is_ok()) {
                    adj.push(k as i16);
                }
            }
            let mut os = PgVec::new_in(mcx);
            os.push(candidate);
            orig_sets.push(os);
            set_masks.push(candidate_set);
            adjacency.push(adj);
            i += 1;
        }
    }
    let num_sets = i - 1;

    let (pair_uv, pair_vu) = bipartite_match(num_sets, num_sets, &adjacency);

    let mut chains: Vec<usize> = vec![0; num_sets + 1];
    let mut num_chains = 0usize;
    for i in 1..=num_sets {
        let u = pair_vu[i] as usize;
        let v = pair_uv[i] as usize;
        if u > 0 && u < i {
            chains[i] = chains[u];
        } else if v > 0 && v < i {
            chains[i] = chains[v];
        } else {
            num_chains += 1;
            chains[i] = num_chains;
        }
    }

    let mut results: Vec<PgVec<'mcx, PgVec<'mcx, i32>>> = Vec::with_capacity(num_chains + 1);
    for _ in 0..=num_chains {
        results.push(PgVec::new_in(mcx));
    }
    for (i, os) in orig_sets.into_iter().enumerate().skip(1).take(num_sets) {
        let c = chains[i];
        debug_assert!(c > 0);
        for set in os {
            results[c].push(set);
        }
    }
    for _ in 0..num_empty {
        results[1].insert(0, PgVec::new_in(mcx));
    }
    for chain in results.into_iter().skip(1) {
        result.push(chain);
    }
    result
}

const HK_INFINITY: i16 = i16::MAX;

/// BipartiteMatch (lib/bipartite_match.c): Hopcroft-Karp; adjacency is
/// 1-based; returns (pair_uv, pair_vu).
fn bipartite_match(u_size: usize, v_size: usize, adjacency: &[Vec<i16>]) -> (Vec<i16>, Vec<i16>) {
    assert!(u_size < i16::MAX as usize && v_size < i16::MAX as usize);
    let mut pair_uv: Vec<i16> = vec![0; u_size + 1];
    let mut pair_vu: Vec<i16> = vec![0; v_size + 1];
    let mut distance: Vec<i16> = vec![0; u_size + 1];
    let mut queue: Vec<i16> = Vec::with_capacity(u_size + 2);

    loop {
        queue.clear();
        distance[0] = HK_INFINITY;
        for u in 1..=u_size {
            if pair_uv[u] == 0 {
                distance[u] = 0;
                queue.push(u as i16);
            } else {
                distance[u] = HK_INFINITY;
            }
        }
        let mut qtail = 0usize;
        while qtail < queue.len() {
            let u = queue[qtail] as usize;
            qtail += 1;
            if distance[u] < distance[0] {
                for &v in adjacency[u].iter() {
                    let u_next = pair_vu[v as usize] as usize;
                    if distance[u_next] == HK_INFINITY {
                        distance[u_next] = 1 + distance[u];
                        queue.push(u_next as i16);
                    }
                }
            }
        }
        if distance[0] == HK_INFINITY {
            break;
        }
        for u in 1..=u_size {
            if pair_uv[u] == 0 {
                hk_depth_search(adjacency, &mut pair_uv, &mut pair_vu, &mut distance, u);
            }
        }
    }
    (pair_uv, pair_vu)
}

fn hk_depth_search(
    adjacency: &[Vec<i16>],
    pair_uv: &mut [i16],
    pair_vu: &mut [i16],
    distance: &mut [i16],
    u: usize,
) -> bool {
    if u == 0 {
        return true;
    }
    if distance[u] == HK_INFINITY {
        return false;
    }
    let nextdist = distance[u] + 1;
    for idx in 0..adjacency[u].len() {
        let v = adjacency[u][idx] as usize;
        if distance[pair_vu[v] as usize] == nextdist
            && hk_depth_search(adjacency, pair_uv, pair_vu, distance, pair_vu[v] as usize)
        {
            pair_vu[v] = u as i16;
            pair_uv[u] = v as i16;
            return true;
        }
    }
    distance[u] = HK_INFINITY;
    false
}

/// reorder_grouping_sets (planner.c): smallest-first chain in, largest-first
/// prefix-ordered GroupingSetData out.
pub fn reorder_grouping_sets<'mcx>(
    mcx: Mcx<'mcx>,
    grouping_sets: PgVec<'mcx, PgVec<'mcx, i32>>,
    sortclause: &NodeList<'mcx>,
) -> PgVec<'mcx, GroupingSetData<'mcx>> {
    let mut previous: Vec<i32> = Vec::new();
    let mut result: PgVec<'mcx, GroupingSetData<'mcx>> = PgVec::new_in(mcx);
    let mut sortclause_live = !sortclause.is_nil();
    for candidate in grouping_sets {
        let mut new_elems: Vec<i32> =
            candidate.iter().copied().filter(|x| !previous.contains(x)).collect();

        while sortclause_live && sortclause.len() > previous.len() && !new_elems.is_empty() {
            let sc = sortclause
                .nth(previous.len())
                .as_sort_group_clause()
                .expect("sortClause cell");
            let r = sc.tleSortGroupRef as i32;
            if let Some(pos) = new_elems.iter().position(|&x| x == r) {
                previous.push(r);
                new_elems.remove(pos);
            } else {
                sortclause_live = false;
            }
        }
        previous.extend_from_slice(&new_elems);

        let mut gs = GroupingSetData::new(mcx);
        for &r in previous.iter() {
            gs.set.push(r as u32);
        }
        result.insert(0, gs);
    }
    result
}

// estimate_hashagg_tablesize (selfuncs.c).
fn estimate_hashagg_tablesize(
    run: &PlannerRun<'_>,
    path: PathId,
    agg_costs: &types_pathnodes::AggClauseCosts,
    d_num_groups: f64,
) -> f64 {
    let width = run
        .root
        .path(path)
        .base()
        .pathtarget_id
        .map_or(0, |pt| run.root.pathtarget(pt).width);
    let hashentrysize = ::nodeagg::hash_agg_entry_size(
        run.root.aggtransinfos.len(),
        width.max(0) as usize,
        agg_costs.transitionSpace as usize,
    );
    hashentrysize * d_num_groups
}

// DiscreteKnapsack (lib/knapsack.c): 0/1 knapsack, all item values 1;
// weight-0 items always included; equal-value comparisons replace (<=), as C.
fn discrete_knapsack(max_weight: i32, item_weights: &[i32]) -> Vec<bool> {
    assert!(max_weight >= 0);
    let num_items = item_weights.len();
    assert!(num_items > 0);
    let cap = max_weight as usize;
    let mut values: Vec<f64> = vec![0.0; cap + 1];
    let mut sets: Vec<Vec<bool>> = vec![vec![false; num_items]; cap + 1];
    for (i, &iw) in item_weights.iter().enumerate() {
        let iw = iw.max(0) as usize;
        if iw > cap {
            continue;
        }
        for j in (iw..=cap).rev() {
            let ow = j - iw;
            if values[j] <= values[ow] + 1.0 {
                if j != ow {
                    let (lo, hi) = sets.split_at_mut(j);
                    hi[0].copy_from_slice(&lo[ow]);
                }
                sets[j][i] = true;
                values[j] = values[ow] + 1.0;
            }
        }
    }
    sets.swap_remove(cap)
}

// One single-set hashed RollupData (the burst shape both arms share).
fn make_hashed_rollup<'mcx>(
    run: &mut PlannerRun<'mcx>,
    gs: &GroupingSetData<'mcx>,
) -> PgResult<RollupData<'mcx>> {
    let mcx = run.mcx;
    let gset: Vec<i32> = gs.set.iter().map(|&r| r as i32).collect();
    let mut rollup = RollupData::new(mcx);
    rollup.groupClause = crate::grouping::preprocess_groupclause(run, Some(&gset))?;
    let mut gsets_data: PgVec<'mcx, GroupingSetData<'mcx>> = PgVec::new_in(mcx);
    gsets_data.push(gs.clone());
    let mut map = {
        let gd = run.gset_data.as_ref().unwrap();
        crate::relnode::pgvec_clone_shallow(mcx, &gd.tleref_to_colnum_map)
    };
    rollup.gsets = remap_to_groupclause_idx(run, &rollup.groupClause, &gsets_data, &mut map);
    run.gset_data.as_mut().unwrap().tleref_to_colnum_map = map;
    rollup.gsets_data = gsets_data;
    rollup.numGroups = gs.numGroups;
    rollup.hashable = true;
    rollup.is_hashed = true;
    Ok(rollup)
}

/// consider_groupingsets_paths (planner.c).
#[allow(clippy::too_many_arguments)]
pub fn consider_groupingsets_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    grouped_rel: RelId,
    path: PathId,
    is_sorted: bool,
    can_hash: bool,
    agg_costs: &types_pathnodes::AggClauseCosts,
    having_qual: &[NodeId],
    d_num_groups: f64,
) -> PgResult<()> {
    let mcx = run.mcx;
    let hash_mem_limit = ::nodehash::get_hash_memory_limit() as f64;
    let quals = |run: &PlannerRun<'mcx>| {
        let mut v = PgVec::new_in(run.mcx);
        v.extend_from_slice(having_qual);
        v
    };

    if !is_sorted {
        assert!(can_hash);

        let mut unhashed_rollup: Option<RollupData<'mcx>> = None;
        let mut exclude_groups = 0.0;
        let mut l_start = 0usize;
        {
            let gd = run.gset_data.as_ref().expect("grouping sets preprocessed");
            // The input can be coincidentally sorted even when is_sorted is
            // false (the caller only set up no sort); vacuously true for a
            // rollup of only empty sets (group_pathkeys comes from rollup 0).
            if !gd.rollups.is_empty()
                && crate::pathkeys::pathkeys_contained_in(
                    &run.root.group_pathkeys,
                    &run.root.path(path).base().pathkeys,
                )
            {
                let r = gd.rollups[0].clone();
                exclude_groups = r.numGroups;
                unhashed_rollup = Some(r);
                l_start = 1;
            }
        }

        let hashsize =
            estimate_hashagg_tablesize(run, path, agg_costs, d_num_groups - exclude_groups);
        let gd = run.gset_data.as_ref().unwrap();
        if hashsize > hash_mem_limit && !gd.rollups.is_empty() {
            return Ok(());
        }

        let mut sets_data: Vec<GroupingSetData<'mcx>> =
            gd.unsortable_sets.iter().cloned().collect();
        for rollup in gd.rollups[l_start..].iter() {
            // An unhashable rollup here would need a differently-sorted
            // input we cannot get; the is_sorted case covers it.
            if !rollup.hashable {
                return Ok(());
            }
            sets_data.extend(rollup.gsets_data.iter().cloned());
        }
        let mut new_rollups: PgVec<'mcx, RollupData<'mcx>> = PgVec::new_in(mcx);
        let mut empty_sets_data: Vec<GroupingSetData<'mcx>> = Vec::new();
        for gs in sets_data {
            if gs.set.is_empty() {
                empty_sets_data.push(gs);
            } else {
                new_rollups.push(make_hashed_rollup(run, &gs)?);
            }
        }
        if new_rollups.is_empty() {
            return Ok(());
        }
        assert!(unhashed_rollup.is_none() || empty_sets_data.is_empty());
        let mut strat = types_pathnodes::AGG_HASHED;
        if let Some(r) = unhashed_rollup {
            new_rollups.push(r);
            strat = types_pathnodes::AGG_MIXED;
        } else if !empty_sets_data.is_empty() {
            let mut rollup = RollupData::new(mcx);
            rollup.numGroups = empty_sets_data.len() as f64;
            for gs in empty_sets_data {
                rollup.gsets.push(PgVec::new_in(mcx));
                rollup.gsets_data.push(gs);
            }
            new_rollups.push(rollup);
            strat = types_pathnodes::AGG_MIXED;
        }
        let q = quals(run);
        let gs_path = crate::pathnode::create_groupingsets_path(
            run,
            grouped_rel,
            path,
            q,
            strat,
            new_rollups,
            agg_costs,
        )?;
        crate::pathnode::add_path(run, grouped_rel, gs_path);
        return Ok(());
    }

    if run.gset_data.as_ref().expect("grouping sets preprocessed").rollups.is_empty() {
        return Ok(());
    }

    // Sorted input: try one mixed sort/hash path and one pure sorted path.
    let gd = run.gset_data.as_ref().unwrap();
    if can_hash && gd.any_hashable {
        let mut hash_sets: Vec<GroupingSetData<'mcx>> =
            gd.unsortable_sets.iter().cloned().collect();
        let availspace = hash_mem_limit
            - estimate_hashagg_tablesize(run, path, agg_costs, gd.dNumHashGroups);

        let gd = run.gset_data.as_ref().unwrap();
        let mut rollups: Vec<RollupData<'mcx>> = Vec::new();
        if availspace > 0.0 && gd.rollups.len() > 1 {
            // Knapsack: capacity is hash_mem, weights are per-rollup hashtable
            // sizes, all values equal (each hashed rollup saves one sort);
            // 5% error margin via the scale clamp.
            let num_rollups = gd.rollups.len();
            let scale = (availspace / (20.0 * num_rollups as f64)).max(1.0);
            let k_capacity = (availspace / scale).floor() as i32;

            // Rollup 0 matches the input sort order and stays sorted; index
            // i counts only hashable candidates (both loops must agree).
            let mut k_weights: Vec<i32> = Vec::with_capacity(num_rollups);
            let hashable_sizes: Vec<f64> = gd.rollups[1..]
                .iter()
                .filter(|r| r.hashable)
                .map(|r| r.numGroups)
                .collect();
            for &num_groups in &hashable_sizes {
                let sz = estimate_hashagg_tablesize(run, path, agg_costs, num_groups);
                k_weights.push((sz / scale).floor().min(k_capacity as f64 + 1.0) as i32);
            }

            if !k_weights.is_empty() {
                let hash_items = discrete_knapsack(k_capacity, &k_weights);
                if hash_items.iter().any(|&b| b) {
                    let gd = run.gset_data.as_ref().unwrap();
                    rollups.push(gd.rollups[0].clone());
                    let mut i = 0usize;
                    for rollup in gd.rollups[1..].iter() {
                        if rollup.hashable {
                            if hash_items[i] {
                                hash_sets.extend(rollup.gsets_data.iter().cloned());
                            } else {
                                rollups.push(rollup.clone());
                            }
                            i += 1;
                        } else {
                            rollups.push(rollup.clone());
                        }
                    }
                }
            }
        }

        if rollups.is_empty() && !hash_sets.is_empty() {
            let gd = run.gset_data.as_ref().unwrap();
            rollups = gd.rollups.iter().cloned().collect();
        }

        // C lcons's each hashed rollup while walking hash_sets forward: the
        // final order is reverse(hash_sets), then the sorted rollups.
        for gs in hash_sets.iter() {
            assert!(!gs.set.is_empty());
            rollups.insert(0, make_hashed_rollup(run, gs)?);
        }

        if !rollups.is_empty() {
            let mut rv: PgVec<'mcx, RollupData<'mcx>> = PgVec::new_in(mcx);
            for r in rollups {
                rv.push(r);
            }
            let q = quals(run);
            let gs_path = crate::pathnode::create_groupingsets_path(
                run,
                grouped_rel,
                path,
                q,
                types_pathnodes::AGG_MIXED,
                rv,
                agg_costs,
            )?;
            crate::pathnode::add_path(run, grouped_rel, gs_path);
        }
    }

    let gd = run.gset_data.as_ref().unwrap();
    if gd.unsortable_sets.is_empty() {
        let rollups = gd.rollups.clone();
        let q = quals(run);
        let gs_path = crate::pathnode::create_groupingsets_path(
            run,
            grouped_rel,
            path,
            q,
            types_pathnodes::AGG_SORTED,
            rollups,
            agg_costs,
        )?;
        crate::pathnode::add_path(run, grouped_rel, gs_path);
    }
    Ok(())
}
