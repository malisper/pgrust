use std::collections::{BTreeSet, VecDeque};

pub(super) type GroupingSetRefs = Vec<usize>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct GroupingSetData {
    pub(super) set: GroupingSetRefs,
}

pub(super) fn extract_rollup_sets(grouping_sets: &[GroupingSetRefs]) -> Vec<Vec<GroupingSetRefs>> {
    let num_sets_raw = grouping_sets.len();
    let mut num_empty = 0;
    while grouping_sets
        .get(num_empty)
        .is_some_and(|set| set.is_empty())
    {
        num_empty += 1;
    }

    if num_empty == grouping_sets.len() {
        return vec![grouping_sets.to_vec()];
    }

    let mut orig_sets = vec![Vec::<GroupingSetRefs>::new(); num_sets_raw + 1];
    let mut set_masks = vec![BTreeSet::<usize>::new(); num_sets_raw + 1];
    let mut adjacency = vec![Vec::<usize>::new(); num_sets_raw + 1];

    let mut j_size = 0;
    let mut j = 1;
    let mut i = 1;

    for candidate in &grouping_sets[num_empty..] {
        let candidate_set: BTreeSet<usize> = candidate.iter().copied().collect();
        let mut dup_of = 0;

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
            orig_sets[dup_of].push(candidate.clone());
            continue;
        }

        orig_sets[i] = vec![candidate.clone()];
        set_masks[i] = candidate_set;

        for k in (1..j).rev() {
            if set_masks[k].is_subset(&set_masks[i]) {
                adjacency[i].push(k);
            }
        }

        i += 1;
    }

    let num_sets = i - 1;
    let (pair_uv, pair_vu) = hopcroft_karp(&adjacency, num_sets);
    let mut chains = vec![0usize; num_sets + 1];
    let mut num_chains = 0;

    for i in 1..=num_sets {
        let u = pair_vu[i];
        let v = pair_uv[i];

        if u > 0 && u < i {
            chains[i] = chains[u];
        } else if v > 0 && v < i {
            chains[i] = chains[v];
        } else {
            num_chains += 1;
            chains[i] = num_chains;
        }
    }

    let mut results = vec![Vec::<GroupingSetRefs>::new(); num_chains + 1];
    for i in 1..=num_sets {
        let chain = chains[i];
        results[chain].extend(orig_sets[i].clone());
    }

    for _ in 0..num_empty {
        results[1].insert(0, Vec::new());
    }

    (1..=num_chains).map(|idx| results[idx].clone()).collect()
}

pub(super) fn reorder_grouping_sets(
    grouping_sets: &[GroupingSetRefs],
    sort_refs: &[usize],
) -> Vec<GroupingSetData> {
    let mut previous = Vec::new();
    let mut result = Vec::new();
    let mut use_sort_refs = true;

    for candidate in grouping_sets {
        let mut new_elems = list_difference(candidate, &previous);

        while use_sort_refs && sort_refs.len() > previous.len() && !new_elems.is_empty() {
            let sort_ref = sort_refs[previous.len()];
            if let Some(pos) = new_elems.iter().position(|ref_id| *ref_id == sort_ref) {
                previous.push(sort_ref);
                new_elems.remove(pos);
            } else {
                use_sort_refs = false;
                break;
            }
        }

        previous.extend(new_elems);
        result.insert(
            0,
            GroupingSetData {
                set: previous.clone(),
            },
        );
    }

    result
}

fn list_difference(candidate: &[usize], previous: &[usize]) -> Vec<usize> {
    candidate
        .iter()
        .copied()
        .filter(|ref_id| !previous.contains(ref_id))
        .collect()
}

fn hopcroft_karp(adjacency: &[Vec<usize>], size: usize) -> (Vec<usize>, Vec<usize>) {
    let mut pair_uv = vec![0usize; size + 1];
    let mut pair_vu = vec![0usize; size + 1];
    let mut distance = vec![0usize; size + 1];
    let infinity = usize::MAX;

    while hk_breadth_search(adjacency, size, &pair_uv, &pair_vu, &mut distance, infinity) {
        for u in 1..=size {
            if pair_uv[u] == 0 {
                hk_depth_search(
                    adjacency,
                    u,
                    &mut pair_uv,
                    &mut pair_vu,
                    &mut distance,
                    infinity,
                );
            }
        }
    }

    (pair_uv, pair_vu)
}

fn hk_breadth_search(
    adjacency: &[Vec<usize>],
    size: usize,
    pair_uv: &[usize],
    pair_vu: &[usize],
    distance: &mut [usize],
    infinity: usize,
) -> bool {
    let mut queue = VecDeque::new();
    distance[0] = infinity;

    for u in 1..=size {
        if pair_uv[u] == 0 {
            distance[u] = 0;
            queue.push_back(u);
        } else {
            distance[u] = infinity;
        }
    }

    while let Some(u) = queue.pop_front() {
        if distance[u] >= distance[0] {
            continue;
        }

        for &v in adjacency[u].iter().rev() {
            let next_u = pair_vu[v];
            if distance[next_u] == infinity {
                distance[next_u] = distance[u] + 1;
                if next_u != 0 {
                    queue.push_back(next_u);
                }
            }
        }
    }

    distance[0] != infinity
}

fn hk_depth_search(
    adjacency: &[Vec<usize>],
    u: usize,
    pair_uv: &mut [usize],
    pair_vu: &mut [usize],
    distance: &mut [usize],
    infinity: usize,
) -> bool {
    if u == 0 {
        return true;
    }
    if distance[u] == infinity {
        return false;
    }

    let nextdist = distance[u] + 1;
    for &v in adjacency[u].iter().rev() {
        let paired_u = pair_vu[v];
        if distance[paired_u] == nextdist
            && hk_depth_search(adjacency, paired_u, pair_uv, pair_vu, distance, infinity)
        {
            pair_vu[v] = u;
            pair_uv[u] = v;
            return true;
        }
    }

    distance[u] = infinity;
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_rollup_sets_keeps_all_empty_sets_together() {
        let grouping_sets = vec![vec![], vec![], vec![]];

        assert_eq!(extract_rollup_sets(&grouping_sets), vec![grouping_sets]);
    }

    #[test]
    fn extract_rollup_sets_uses_minimum_path_cover() {
        let grouping_sets = vec![vec![], vec![1], vec![2], vec![1, 2], vec![1, 3]];

        assert_eq!(
            extract_rollup_sets(&grouping_sets),
            vec![vec![vec![], vec![1], vec![1, 3]], vec![vec![2], vec![1, 2]]]
        );
    }

    #[test]
    fn extract_rollup_sets_preserves_duplicate_grouping_sets() {
        let grouping_sets = vec![vec![], vec![1], vec![1], vec![1, 2]];

        assert_eq!(
            extract_rollup_sets(&grouping_sets),
            vec![vec![vec![], vec![1], vec![1], vec![1, 2]]]
        );
    }

    #[test]
    fn reorder_grouping_sets_follows_sort_refs_when_they_match() {
        let grouping_sets = vec![vec![3], vec![1, 2, 3]];

        assert_eq!(
            reorder_grouping_sets(&grouping_sets, &[3, 2, 1]),
            vec![
                GroupingSetData { set: vec![3, 2, 1] },
                GroupingSetData { set: vec![3] },
            ]
        );
    }

    #[test]
    fn reorder_grouping_sets_stops_using_sort_refs_after_divergence() {
        let grouping_sets = vec![vec![3], vec![1, 2, 3]];

        assert_eq!(
            reorder_grouping_sets(&grouping_sets, &[2, 3, 1]),
            vec![
                GroupingSetData { set: vec![3, 1, 2] },
                GroupingSetData { set: vec![3] },
            ]
        );
    }
}
