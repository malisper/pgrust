//! geqo_erx.c — edge recombination crossover [ERX], C's default operator.

use super::random::geqo_randint;
use super::{Gene, GeqoState};

// The (<=4) edges from a city plus counts.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Edge {
    edge_list: [Gene; 4],
    total_edges: i32,
    unused_edges: i32,
}

// num_gene + 1 rows so nodes 1..n index directly (0 unused).
pub(super) fn alloc_edge_table(num_gene: i32) -> Vec<Edge> {
    vec![Edge::default(); (num_gene + 1) as usize]
}

// Fill the table with both parents' (circular, bidirectional) edges; shared
// edges are marked negative by gimme_edge.
pub(super) fn gimme_edge_table(
    tour1: &[Gene],
    tour2: &[Gene],
    num_gene: i32,
    edge_table: &mut [Edge],
) {
    for i in 1..=num_gene as usize {
        edge_table[i].total_edges = 0;
        edge_table[i].unused_edges = 0;
    }
    for index1 in 0..num_gene as usize {
        let index2 = (index1 + 1) % num_gene as usize;
        gimme_edge(tour1[index1], tour1[index2], edge_table);
        gimme_edge(tour1[index2], tour1[index1], edge_table);
        gimme_edge(tour2[index1], tour2[index2], edge_table);
        gimme_edge(tour2[index2], tour2[index1], edge_table);
    }
}

// Register a directed edge; an already-present edge is marked shared (negated).
fn gimme_edge(gene1: Gene, gene2: Gene, edge_table: &mut [Edge]) -> i32 {
    let city1 = gene1 as usize;
    let city2 = gene2;
    let edges = edge_table[city1].total_edges;
    for i in 0..edges as usize {
        if edge_table[city1].edge_list[i].abs() == city2 {
            edge_table[city1].edge_list[i] = -city2;
            return 0;
        }
    }
    edge_table[city1].edge_list[edges as usize] = city2;
    edge_table[city1].total_edges += 1;
    edge_table[city1].unused_edges += 1;
    1
}

// Build a new tour from the edge table, favouring shared edges; returns the
// edge-failure count. Mutates edge_table in place (the driver refills it each
// generation via gimme_edge_table, as C does).
pub(super) fn gimme_tour(
    state: &mut GeqoState,
    edge_table: &mut [Edge],
    new_gene: &mut [Gene],
    num_gene: i32,
) -> i32 {
    let mut edge_failures = 0;

    new_gene[0] = geqo_randint(state, num_gene, 1) as Gene;

    for i in 1..num_gene as usize {
        let prev = new_gene[i - 1] as usize;
        remove_gene(new_gene[i - 1], edge_table[prev], edge_table);
        if edge_table[prev].unused_edges > 0 {
            new_gene[i] = gimme_gene(state, edge_table[prev], edge_table);
        } else {
            edge_failures += 1;
            new_gene[i] = edge_failure(state, new_gene, i - 1, edge_table, num_gene);
        }
        edge_table[new_gene[i - 1] as usize].unused_edges = -1;
    }
    edge_failures
}

// Delete gene from every edge list of a city that had an edge to it.
fn remove_gene(gene: Gene, edge: Edge, edge_table: &mut [Edge]) {
    for i in 0..edge.unused_edges as usize {
        let possess_edge = edge.edge_list[i].unsigned_abs() as usize;
        let genes_remaining = edge_table[possess_edge].unused_edges;
        for j in 0..genes_remaining as usize {
            if edge_table[possess_edge].edge_list[j].abs() == gene {
                edge_table[possess_edge].unused_edges -= 1;
                edge_table[possess_edge].edge_list[j] =
                    edge_table[possess_edge].edge_list[(genes_remaining - 1) as usize];
                break;
            }
        }
    }
}

// Choose the next city: a shared (negative) edge first, else a random one
// among the candidates with fewest unused edges.
fn gimme_gene(state: &mut GeqoState, edge: Edge, edge_table: &[Edge]) -> Gene {
    let mut minimum_edges = 5; // no city has >4 edges, so this is replaced
    let mut minimum_count = -1;

    for i in 0..edge.unused_edges as usize {
        let friend = edge.edge_list[i];
        // Shared edges are negative; prefer them.
        if friend < 0 {
            return friend.abs();
        }
        let unused = edge_table[friend as usize].unused_edges;
        if unused < minimum_edges {
            minimum_edges = unused;
            minimum_count = 1;
        } else if minimum_count == -1 {
            panic!("minimum_count not set");
        } else if unused == minimum_edges {
            minimum_count += 1;
        }
    }

    let rand_decision = geqo_randint(state, minimum_count - 1, 0);
    let mut minimum_count = minimum_count;
    for i in 0..edge.unused_edges as usize {
        let friend = edge.edge_list[i];
        if edge_table[friend as usize].unused_edges == minimum_edges {
            minimum_count -= 1;
            if minimum_count == rand_decision {
                return friend;
            }
        }
    }
    panic!("neither shared nor minimum number nor random edge found");
}

// Pick a replacement city when the tour hits a dead end.
fn edge_failure(
    state: &mut GeqoState,
    gene: &[Gene],
    index: usize,
    edge_table: &[Edge],
    num_gene: i32,
) -> Gene {
    let fail_gene = gene[index];
    let mut remaining_edges = 0;
    let mut four_count = 0;

    for i in 1..=num_gene as usize {
        if edge_table[i].unused_edges != -1 && i as Gene != fail_gene {
            remaining_edges += 1;
            if edge_table[i].total_edges == 4 {
                four_count += 1;
            }
        }
    }

    if four_count != 0 {
        let rand_decision = geqo_randint(state, four_count - 1, 0);
        let mut four_count = four_count;
        for i in 1..=num_gene as usize {
            if i as Gene != fail_gene
                && edge_table[i].unused_edges != -1
                && edge_table[i].total_edges == 4
            {
                four_count -= 1;
                if rand_decision == four_count {
                    return i as Gene;
                }
            }
        }
    } else if remaining_edges != 0 {
        let rand_decision = geqo_randint(state, remaining_edges - 1, 0);
        let mut remaining_edges = remaining_edges;
        for i in 1..=num_gene as usize {
            if i as Gene != fail_gene && edge_table[i].unused_edges != -1 {
                remaining_edges -= 1;
                if rand_decision == remaining_edges {
                    return i as Gene;
                }
            }
        }
    } else {
        // Only at the last tour point: take the first unused point.
        for i in 1..=num_gene as usize {
            if edge_table[i].unused_edges >= 0 {
                return i as Gene;
            }
        }
    }
    panic!("no edge found");
}
