//! `geqo_erx.c` — edge recombination crossover [ERX], the default operator.

use crate::random::geqo_randint;
use crate::{Gene, GeqoPrivateData};
use alloc::vec;
use alloc::vec::Vec;

/// `Edge` (`geqo_recombination.h`): one row of the ERX edge table.
#[derive(Clone, Copy, Debug, Default)]
pub struct Edge {
    /// list of edges (`edge_list[4]`)
    pub edge_list: [Gene; 4],
    pub total_edges: i32,
    pub unused_edges: i32,
}

/// `alloc_edge_table(root, num_gene)` — allocate the edge table. C palloc's one
/// extra location "so that nodes numbered 1..n can be indexed directly".
pub fn alloc_edge_table(num_gene: i32) -> Vec<Edge> {
    vec![Edge::default(); (num_gene + 1) as usize]
}

// free_edge_table is RAII (the Vec is dropped).

/// `gimme_edge_table(root, tour1, tour2, num_gene, edge_table)` — fill the edge
/// table with the explicit bidirectional edges of both tours (assuming circular
/// tours), marking shared edges negative. Returns the average number of
/// edges/city (2.0 homogeneous .. 4.0 diverse).
pub fn gimme_edge_table(
    tour1: &[Gene],
    tour2: &[Gene],
    num_gene: i32,
    edge_table: &mut [Edge],
) -> f32 {
    /* at first clear the edge table's old data */
    for i in 1..=num_gene as usize {
        edge_table[i].total_edges = 0;
        edge_table[i].unused_edges = 0;
    }

    /* fill edge table with new data */
    let mut edge_total = 0;

    for index1 in 0..num_gene as usize {
        /*
         * presume the tour is circular, i.e. 1->2, 2->3, 3->1 this operation
         * maps n back to 1
         */
        let index2 = (index1 + 1) % num_gene as usize;

        /*
         * edges are bidirectional, i.e. 1->2 is same as 2->1 call gimme_edge
         * twice per edge
         */
        edge_total += gimme_edge(tour1[index1], tour1[index2], edge_table);
        gimme_edge(tour1[index2], tour1[index1], edge_table);

        edge_total += gimme_edge(tour2[index1], tour2[index2], edge_table);
        gimme_edge(tour2[index2], tour2[index1], edge_table);
    }

    /* return average number of edges per index */
    (edge_total * 2) as f32 / num_gene as f32
}

/// `gimme_edge(root, gene1, gene2, edge_table)` — register edge `city1 -> city2`
/// in the edge table; returns 1 if newly added, 0 if it already existed (in
/// which case it is marked shared/negative).
fn gimme_edge(gene1: Gene, gene2: Gene, edge_table: &mut [Edge]) -> i32 {
    let city1 = gene1 as usize;
    let city2 = gene2;

    /* check whether edge city1->city2 already exists */
    let edges = edge_table[city1].total_edges;

    for i in 0..edges as usize {
        if edge_table[city1].edge_list[i].abs() == city2 {
            /* mark shared edges as negative */
            edge_table[city1].edge_list[i] = 0 - city2;
            return 0;
        }
    }

    /* add city1->city2; */
    edge_table[city1].edge_list[edges as usize] = city2;

    /* increment the number of edges from city1 */
    edge_table[city1].total_edges += 1;
    edge_table[city1].unused_edges += 1;

    1
}

/// `gimme_tour(root, edge_table, new_gene, num_gene)` — create a new tour using
/// edges from the edge table, preferring shared edges. Returns the number of
/// edge failures.
pub fn gimme_tour(
    private: &mut GeqoPrivateData,
    edge_table: &mut [Edge],
    new_gene: &mut [Gene],
    num_gene: i32,
) -> i32 {
    let mut edge_failures = 0;

    /* choose int between 1 and num_gene */
    new_gene[0] = geqo_randint(private, num_gene, 1) as Gene;

    for i in 1..num_gene as usize {
        /*
         * as each point is entered into the tour, remove it from the edge
         * table
         */
        let prev = new_gene[i - 1];
        let prev_edge = edge_table[prev as usize];
        remove_gene(prev, prev_edge, edge_table);

        /* find destination for the newly entered point */
        if edge_table[new_gene[i - 1] as usize].unused_edges > 0 {
            let edge = edge_table[new_gene[i - 1] as usize];
            new_gene[i] = gimme_gene(private, edge, edge_table);
        } else {
            /* cope with fault */
            edge_failures += 1;

            new_gene[i] = edge_failure(private, new_gene, i - 1, edge_table, num_gene);
        }

        /* mark this node as incorporated */
        edge_table[new_gene[i - 1] as usize].unused_edges = -1;
    }

    edge_failures
}

/// `remove_gene(root, gene, edge, edge_table)` — remove `gene` from the edge
/// table, using `edge` to identify the deletion locations.
fn remove_gene(gene: Gene, edge: Edge, edge_table: &mut [Edge]) {
    /*
     * do for every gene known to have an edge to input gene (i.e. in
     * edge_list for input edge)
     */
    for i in 0..edge.unused_edges as usize {
        let possess_edge = edge.edge_list[i].abs() as usize;
        let genes_remaining = edge_table[possess_edge].unused_edges;

        /* find the input gene in all edge_lists and delete it */
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

/// `gimme_gene(root, edge, edge_table)` — choose the next gene, giving priority
/// to shared (negative) edges, then to candidates with the fewest remaining
/// unused edges (random tie-break).
fn gimme_gene(private: &mut GeqoPrivateData, edge: Edge, edge_table: &mut [Edge]) -> Gene {
    let mut minimum_count: i32 = -1;

    /*
     * no point has edges to more than 4 other points thus, this contrived
     * minimum will be replaced
     */
    let mut minimum_edges = 5;

    /* consider candidate destination points in edge list */
    for i in 0..edge.unused_edges as usize {
        let friend = edge.edge_list[i];

        /*
         * give priority to shared edges that are negative; so return 'em
         * (negative values are caught here so we need not worry about
         * converting to absolute values)
         */
        if friend < 0 {
            return friend.abs() as Gene;
        }

        /*
         * give priority to candidates with fewest remaining unused edges; find
         * out what the minimum number of unused edges is (minimum_edges); if
         * there is more than one candidate with the minimum number of unused
         * edges keep count of this number (minimum_count);
         */
        if edge_table[friend as usize].unused_edges < minimum_edges {
            minimum_edges = edge_table[friend as usize].unused_edges;
            minimum_count = 1;
        } else if minimum_count == -1 {
            panic!("minimum_count not set");
        } else if edge_table[friend as usize].unused_edges == minimum_edges {
            minimum_count += 1;
        }
    }

    /* random decision of the possible candidates to use */
    let rand_decision = geqo_randint(private, minimum_count - 1, 0);

    let mut minimum_count = minimum_count;
    for i in 0..edge.unused_edges as usize {
        let friend = edge.edge_list[i];

        /* return the chosen candidate point */
        if edge_table[friend as usize].unused_edges == minimum_edges {
            minimum_count -= 1;

            if minimum_count == rand_decision {
                return friend;
            }
        }
    }

    /* ... should never be reached */
    panic!("neither shared nor minimum number nor random edge found");
}

/// `edge_failure(root, gene, index, edge_table, num_gene)` — handle edge
/// failure: pick a remaining gene (preferring genes with 4 total edges), then
/// any gene with remaining edges, then any unused point. The C `elog(LOG, ...)`
/// diagnostics on each "no edge found via …" fallthrough are debug-only logging
/// (a systemic i18n/logging deferral); the control flow they precede is
/// preserved exactly.
fn edge_failure(
    private: &mut GeqoPrivateData,
    gene: &[Gene],
    index: usize,
    edge_table: &mut [Edge],
    num_gene: i32,
) -> Gene {
    let fail_gene = gene[index];
    let mut remaining_edges = 0;
    let mut four_count = 0;

    /*
     * how many edges remain? how many gene with four total (initial) edges
     * remain?
     */
    for i in 1..=num_gene as usize {
        if edge_table[i].unused_edges != -1 && i as Gene != fail_gene {
            remaining_edges += 1;

            if edge_table[i].total_edges == 4 {
                four_count += 1;
            }
        }
    }

    /*
     * random decision of the gene with remaining edges and whose total_edges
     * == 4
     */
    if four_count != 0 {
        let rand_decision = geqo_randint(private, four_count - 1, 0);
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

        // elog(LOG, "no edge found via random decision and total_edges == 4")
    } else if remaining_edges != 0 {
        /* random decision of the gene with remaining edges */
        let rand_decision = geqo_randint(private, remaining_edges - 1, 0);
        let mut remaining_edges = remaining_edges;

        for i in 1..=num_gene as usize {
            if i as Gene != fail_gene && edge_table[i].unused_edges != -1 {
                remaining_edges -= 1;

                if rand_decision == remaining_edges {
                    return i as Gene;
                }
            }
        }

        // elog(LOG, "no edge found via random decision with remaining edges")
    } else {
        /*
         * edge table seems to be empty; this happens sometimes on the last
         * point due to the fact that the first point is removed from the table
         * even though only one of its edges has been determined
         */
        /* occurs only at the last point in the tour; simply look for the point
         * which is not yet used */
        for i in 1..=num_gene as usize {
            if edge_table[i].unused_edges >= 0 {
                return i as Gene;
            }
        }

        // elog(LOG, "no edge found via looking for the last unused point")
    }

    /* ... should never be reached */
    panic!("no edge found");
}
