//! Unit + golden tests for the `backend-lib-bipartite-match` port.
//!
//! The golden tests are cross-checked against PostgreSQL 18's observable
//! grouping-set reordering — the sole in-tree consumer of `BipartiteMatch` is
//! `extract_rollup_sets` (`optimizer/plan/planner.c`), which feeds it the
//! adjacency graph of "set k is a strict-cardinality subset of set i" and turns
//! the resulting maximum matching into the minimum number of rollup *chains*
//! (Dilworth: a minimum chain cover of the subset partial order). The chain
//! count and membership shape the GroupAggregate / MixedAggregate plans printed
//! in `src/test/regress/expected/groupingsets.out`.

extern crate std;

use super::*;

/// Install deterministic no-op mocks for the two query-progress seams so the
/// algorithm runs to completion under test. `check_for_interrupts` and
/// `check_stack_depth` both succeed (never an interrupt / never too deep),
/// matching a quiescent backend. Single-threaded; idempotent across tests.
fn install_seam_mocks() {
    if !backend_tcop_postgres_seams::check_for_interrupts::is_installed() {
        backend_tcop_postgres_seams::check_for_interrupts::set(|| Ok(()));
    }
    if !backend_tcop_postgres_seams::check_stack_depth::is_installed() {
        backend_tcop_postgres_seams::check_stack_depth::set(|| Ok(()));
    }
}

// ===========================================================================
// Unit tests.
// ===========================================================================

#[test]
fn finds_maximum_matching() {
    install_seam_mocks();
    let adjacency: &[&[i16]] = &[&[], &[2, 1, 2], &[1, 1], &[2, 2, 3]];
    let state = BipartiteMatch(3, 3, adjacency).unwrap();
    assert_eq!(state.matching, 3);
    assert_eq!(state.pair_uv[1] as usize, 2);
    assert_eq!(state.pair_uv[2] as usize, 1);
    assert_eq!(state.pair_uv[3] as usize, 3);
}

#[test]
fn rejects_invalid_sizes() {
    install_seam_mocks();
    let adjacency: &[&[i16]] = &[&[]];
    assert!(BipartiteMatch(i16::MAX as i32, 1, adjacency).is_err());
}

#[test]
fn rejects_invalid_adjacency_count() {
    install_seam_mocks();
    let adjacency: &[&[i16]] = &[&[], &[2, 1]];
    assert_eq!(
        BipartiteMatch(1, 1, adjacency).unwrap_err().message(),
        "adjacency entry count is invalid"
    );
}

#[test]
fn free_is_a_noop_drop() {
    install_seam_mocks();
    let adjacency: &[&[i16]] = &[&[], &[2, 1, 2], &[1, 1], &[2, 2, 3]];
    let state = BipartiteMatch(3, 3, adjacency).unwrap();
    BipartiteMatchFree(state); // consumes + drops; no leak, no panic.
}

// ===========================================================================
// Golden tests (PostgreSQL 18 groupingsets.out parity).
// ===========================================================================

/// Replay of `planner.c:3065-3098`: turn the matching into chains, returning
/// `chains[1..=num_sets]` (chains[i] is the 1-based chain id of set i) and the
/// total number of chains. Index 0 is unused, mirroring the C code.
fn assign_chains(state: &BipartiteMatchState, num_sets: usize) -> (std::vec::Vec<i32>, i32) {
    let mut chains = std::vec![0i32; num_sets + 1];
    let mut num_chains = 0i32;

    for i in 1..=num_sets {
        let u = state.pair_vu[i] as usize; // int u = state->pair_vu[i];
        let v = state.pair_uv[i] as usize; // int v = state->pair_uv[i];

        if u > 0 && u < i {
            chains[i] = chains[u];
        } else if v > 0 && v < i {
            chains[i] = chains[v];
        } else {
            num_chains += 1;
            chains[i] = num_chains;
        }
    }
    (chains, num_chains)
}

/// Group set indices by their assigned chain id, returning a sorted list of the
/// (sorted) member-index vectors so equality is order-independent across runs.
fn chains_of(
    chains: &[i32],
    num_chains: i32,
    num_sets: usize,
) -> std::vec::Vec<std::vec::Vec<usize>> {
    let mut out: std::vec::Vec<std::vec::Vec<usize>> =
        std::vec![std::vec::Vec::new(); num_chains as usize];
    for i in 1..=num_sets {
        out[(chains[i] - 1) as usize].push(i);
    }
    for c in out.iter_mut() {
        c.sort_unstable();
    }
    out.sort_unstable();
    out
}

// ROLLUP (a, b): {a}(1), {a,b}(2). {a} subset of {a,b} -> adjacency[2]=[1,1].
// Matching 1, ONE chain (groupingsets.out:36, "rollup (a,b)").
#[test]
fn rollup_two_columns_single_chain() {
    install_seam_mocks();
    let num_sets = 2;
    let adjacency: &[&[i16]] = &[&[], &[], &[1, 1]];
    let state = BipartiteMatch(num_sets as i32, num_sets as i32, adjacency).unwrap();
    assert_eq!(state.matching, 1);

    let (chains, num_chains) = assign_chains(&state, num_sets);
    assert_eq!(num_chains, 1, "rollup(a,b) collapses to a single chain");
    assert_eq!(chains_of(&chains, num_chains, num_sets), std::vec![std::vec![1, 2]]);
}

// ROLLUP (a, b, c): {a}(1), {a,b}(2), {a,b,c}(3). adjacency[2]=[1,1],
// adjacency[3]=[1,2]. Matching 2, ONE chain (full rollup).
#[test]
fn rollup_three_columns_single_chain() {
    install_seam_mocks();
    let num_sets = 3;
    let adjacency: &[&[i16]] = &[&[], &[], &[1, 1], &[1, 2]];
    let state = BipartiteMatch(num_sets as i32, num_sets as i32, adjacency).unwrap();
    assert_eq!(state.matching, 2);

    let (chains, num_chains) = assign_chains(&state, num_sets);
    assert_eq!(num_chains, 1);
    assert_eq!(
        chains_of(&chains, num_chains, num_sets),
        std::vec![std::vec![1, 2, 3]]
    );
}

// CUBE (a, b): {a}(1), {b}(2), {a,b}(3). adjacency[3]=[2,2,1]. Matching 1 ->
// TWO chains; the descending edge-scan tie-break makes V=1 ({a}) win, so {a,b}
// chains with {a} and {b} stands alone.
#[test]
fn cube_two_columns_two_chains() {
    install_seam_mocks();
    let num_sets = 3;
    let adjacency: &[&[i16]] = &[&[], &[], &[], &[2, 2, 1]];
    let state = BipartiteMatch(num_sets as i32, num_sets as i32, adjacency).unwrap();
    assert_eq!(state.matching, 1);

    let (chains, num_chains) = assign_chains(&state, num_sets);
    assert_eq!(num_chains, 2, "cube(a,b) needs two rollup chains");

    let grouped = chains_of(&chains, num_chains, num_sets);
    assert_eq!(grouped, std::vec![std::vec![1, 3], std::vec![2]]);
}

// CUBE (a, b, c): 7 sets. Minimum chain cover of the 3-element cube lattice is
// 3 chains (matching = 7 - 3 = 4).
#[test]
fn cube_three_columns_three_chains() {
    install_seam_mocks();
    let num_sets = 7;
    let adjacency: &[&[i16]] = &[
        &[],
        &[],
        &[],
        &[],
        &[2, 2, 1],
        &[2, 3, 1],
        &[2, 3, 2],
        &[3, 6, 5, 4],
    ];
    let state = BipartiteMatch(num_sets as i32, num_sets as i32, adjacency).unwrap();
    assert_eq!(state.matching, 4, "7 sets - 3 chains = 4 matched edges");

    let (_chains, num_chains) = assign_chains(&state, num_sets);
    assert_eq!(num_chains, 3, "cube(a,b,c) minimum chain cover is 3 chains");
}

// GROUPING SETS ((a,b),(a,c)) — two incomparable size-2 sets, no edges.
// Matching 0, two chains (groupingsets.out:354).
#[test]
fn disjoint_sets_each_own_chain() {
    install_seam_mocks();
    let num_sets = 2;
    let adjacency: &[&[i16]] = &[&[], &[], &[]];
    let state = BipartiteMatch(num_sets as i32, num_sets as i32, adjacency).unwrap();
    assert_eq!(state.matching, 0);

    let (chains, num_chains) = assign_chains(&state, num_sets);
    assert_eq!(num_chains, 2);
    assert_eq!(
        chains_of(&chains, num_chains, num_sets),
        std::vec![std::vec![1], std::vec![2]]
    );
}

// Empty graph: zero sets, matching 0, outer loop never runs.
#[test]
fn empty_graph_no_matching() {
    install_seam_mocks();
    let adjacency: &[&[i16]] = &[&[]];
    let state = BipartiteMatch(0, 0, adjacency).unwrap();
    assert_eq!(state.matching, 0);
}

// The elog(ERROR, "invalid set size for BipartiteMatch") path (C line 45):
// message text + the elog(ERROR) default SQLSTATE (ERRCODE_INTERNAL_ERROR /
// XX000) must match C exactly.
#[test]
fn invalid_set_size_matches_c_message() {
    install_seam_mocks();
    use types_error::ERRCODE_INTERNAL_ERROR;

    let adjacency: &[&[i16]] = &[&[], &[]];
    for (u, v) in [(-1, 1), (1, -1), (i16::MAX as i32, 1), (1, i16::MAX as i32)] {
        let err = BipartiteMatch(u, v, adjacency).unwrap_err();
        assert_eq!(err.message(), "invalid set size for BipartiteMatch");
        assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    }
}

// The adjacency-entry-outside-V-set check (safe-slice analogue of C
// dereferencing pair_vu[u_adj[i]] out of range): a row listing V=5 when v_size
// is 1 is rejected.
#[test]
fn rejects_adjacency_entry_outside_v_set() {
    install_seam_mocks();
    let adjacency: &[&[i16]] = &[&[], &[1, 5]];
    assert_eq!(
        BipartiteMatch(1, 1, adjacency).unwrap_err().message(),
        "adjacency entry is outside V set"
    );
}
