//! `geqo_main.c` — solution to the query optimization problem by a Genetic
//! Algorithm (GA), plus the GEQO configuration globals.
//!
//! The configuration variables (`Geqo_effort`, `Geqo_pool_size`,
//! `Geqo_generations`, `Geqo_selection_bias`, `Geqo_seed`) are *defined* in
//! geqo_main.c (the GUC machinery in guc_tables.c references them via
//! `extern`), so this crate is their canonical home. They are modeled as a
//! [`GeqoConfig`] read at the start of [`geqo`]; [`GeqoConfig::from_gucs`] is the
//! single point the wiring layer feeds the live GUC values in, defaulting to the
//! `DEFAULT_GEQO_*` values from `geqo.h`.

use crate::erx::{alloc_edge_table, gimme_edge_table, gimme_tour};
use crate::eval::{geqo_eval, gimme_tree};
use crate::pool::{alloc_chromo, alloc_pool, random_init_pool, sort_pool, spread_chromo};
use crate::random::geqo_set_seed;
use crate::selection::geqo_selection;
use crate::{GeqoPrivateData, Operator};
use pathnodes::planner_run::PlannerRun;
use pathnodes::{PlannerInfo, RelId};

/* Configuration option bounds / defaults (geqo.h). */
pub const DEFAULT_GEQO_EFFORT: i32 = 5;
pub const MIN_GEQO_EFFORT: i32 = 1;
pub const MAX_GEQO_EFFORT: i32 = 10;
pub const DEFAULT_GEQO_SELECTION_BIAS: f64 = 2.0;
pub const MIN_GEQO_SELECTION_BIAS: f64 = 1.5;
pub const MAX_GEQO_SELECTION_BIAS: f64 = 2.0;

/// The GEQO configuration globals (`geqo_main.c`): `Geqo_effort`,
/// `Geqo_pool_size`, `Geqo_generations`, `Geqo_selection_bias`, `Geqo_seed`.
#[derive(Clone, Copy, Debug)]
pub struct GeqoConfig {
    /// `Geqo_effort` — 1 .. 10, knob for adjustment of defaults
    pub effort: i32,
    /// `Geqo_pool_size` — 2 .. inf, or 0 to use default
    pub pool_size: i32,
    /// `Geqo_generations` — 1 .. inf, or 0 to use default
    pub generations: i32,
    /// `Geqo_selection_bias`
    pub selection_bias: f64,
    /// `Geqo_seed` — 0 .. 1
    pub seed: f64,
}

impl Default for GeqoConfig {
    fn default() -> Self {
        Self {
            effort: DEFAULT_GEQO_EFFORT,
            pool_size: 0,
            generations: 0,
            selection_bias: DEFAULT_GEQO_SELECTION_BIAS,
            seed: 0.0,
        }
    }
}

impl GeqoConfig {
    /// Construct from the live GUC values (the wiring layer's hook).
    pub fn from_gucs(
        effort: i32,
        pool_size: i32,
        generations: i32,
        selection_bias: f64,
        seed: f64,
    ) -> Self {
        Self {
            effort,
            pool_size,
            generations,
            selection_bias,
            seed,
        }
    }
}

/// `geqo(root, number_of_rels, initial_rels)` — solution of the query
/// optimization problem (a constrained TSP) by a genetic algorithm. Returns the
/// best join `RelOptInfo` (the cheapest query tree found).
///
/// `initial_rels` are the base [`RelId`]s being joined (the C `List *`);
/// `config` supplies the GEQO GUC values (file-globals in C).
pub fn geqo(
    root: &mut PlannerInfo,
    run: &PlannerRun<'_>,
    number_of_rels: i32,
    initial_rels: alloc::vec::Vec<RelId>,
    config: &GeqoConfig,
) -> RelId {
    /* set up private information */
    let mut private = GeqoPrivateData {
        initial_rels,
        random_state: prng::PgPrng::default(),
    };

    /* initialize private number generator */
    geqo_set_seed(&mut private, config.seed);

    /* set GA parameters */
    let pool_size = gimme_pool_size(number_of_rels, config);
    let number_generations = gimme_number_generations(pool_size, config);

    /* allocate genetic pool memory */
    let mut pool = alloc_pool(pool_size, number_of_rels);

    /* random initialization of the pool */
    random_init_pool(root, run, &mut private, &mut pool);

    /*
     * sort the pool according to cheapest path as fitness. We have to do it
     * only once, since all kids replace the worst individuals in future
     * (-> geqo_pool.c:spread_chromo).
     */
    sort_pool(&mut pool);

    /* allocate chromosome momma and daddy memory */
    let mut momma = alloc_chromo(pool.string_length);
    let mut daddy = alloc_chromo(pool.string_length);

    /*
     * Operator-specific allocation. ERX (the default `#define`) allocates the
     * edge table; the other operators (compiled but not selected by default)
     * allocate a separate kid chromosome and, for CX/PX/OX1/OX2, a city table.
     */
    let mut edge_table = match crate::operator {
        Operator::Erx => Some(alloc_edge_table(pool.string_length)),
        _ => None,
    };
    let mut edge_failures = 0;

    /*
     * For non-ERX operators a distinct kid chromosome is bred each generation
     * (ERX reuses momma in place). Allocated up front to mirror the C
     * `alloc_chromo` calls; unused under ERX.
     */
    let mut kid_storage = alloc_chromo(pool.string_length);
    let mut city_table = match crate::operator {
        Operator::Cx | Operator::Px | Operator::Ox1 | Operator::Ox2 => {
            Some(crate::recombination::alloc_city_table(pool.string_length))
        }
        _ => None,
    };

    /* iterative optimization */
    for _generation in 0..number_generations {
        /* SELECTION: using linear bias function */
        geqo_selection(
            &mut private,
            &mut momma,
            &mut daddy,
            &pool,
            config.selection_bias,
        );

        let kid_worth;

        match crate::operator {
            Operator::Erx => {
                /* EDGE RECOMBINATION CROSSOVER */
                let et = edge_table.as_mut().unwrap();
                gimme_edge_table(&momma.string, &daddy.string, pool.string_length, et);

                /*
                 * C does `kid = momma;` — the kid IS momma's storage, bred in
                 * place. We compute the new tour into momma's string and then
                 * spread momma as the kid.
                 */
                edge_failures += gimme_tour(&mut private, et, &mut momma.string, pool.string_length);

                momma.worth = geqo_eval(root, run, &mut private, &momma.string, pool.string_length);
                kid_worth = momma.worth;
                spread_chromo(&momma, &mut pool);
            }
            Operator::Pmx => {
                crate::pmx::pmx(
                    &mut private,
                    &momma.string,
                    &daddy.string,
                    &mut kid_storage.string,
                    pool.string_length,
                );
                kid_storage.worth =
                    geqo_eval(root, run, &mut private, &kid_storage.string, pool.string_length);
                kid_worth = kid_storage.worth;
                spread_chromo(&kid_storage, &mut pool);
            }
            Operator::Cx => {
                let ct = city_table.as_mut().unwrap();
                let cycle_diffs = crate::cx::cx(
                    &mut private,
                    &momma.string,
                    &daddy.string,
                    &mut kid_storage.string,
                    pool.string_length,
                    ct,
                );
                /* mutate the child */
                if cycle_diffs == 0 {
                    crate::mutation::geqo_mutation(
                        &mut private,
                        &mut kid_storage.string,
                        pool.string_length,
                    );
                }
                kid_storage.worth =
                    geqo_eval(root, run, &mut private, &kid_storage.string, pool.string_length);
                kid_worth = kid_storage.worth;
                spread_chromo(&kid_storage, &mut pool);
            }
            Operator::Px => {
                let ct = city_table.as_mut().unwrap();
                crate::px::px(
                    &mut private,
                    &momma.string,
                    &daddy.string,
                    &mut kid_storage.string,
                    pool.string_length,
                    ct,
                );
                kid_storage.worth =
                    geqo_eval(root, run, &mut private, &kid_storage.string, pool.string_length);
                kid_worth = kid_storage.worth;
                spread_chromo(&kid_storage, &mut pool);
            }
            Operator::Ox1 => {
                let ct = city_table.as_mut().unwrap();
                crate::ox1::ox1(
                    &mut private,
                    &momma.string,
                    &daddy.string,
                    &mut kid_storage.string,
                    pool.string_length,
                    ct,
                );
                kid_storage.worth =
                    geqo_eval(root, run, &mut private, &kid_storage.string, pool.string_length);
                kid_worth = kid_storage.worth;
                spread_chromo(&kid_storage, &mut pool);
            }
            Operator::Ox2 => {
                let ct = city_table.as_mut().unwrap();
                crate::ox2::ox2(
                    &mut private,
                    &momma.string,
                    &daddy.string,
                    &mut kid_storage.string,
                    pool.string_length,
                    ct,
                );
                kid_storage.worth =
                    geqo_eval(root, run, &mut private, &kid_storage.string, pool.string_length);
                kid_worth = kid_storage.worth;
                spread_chromo(&kid_storage, &mut pool);
            }
        }

        let _ = kid_worth;
    }

    /* suppress variable-set-but-not-used (mirrors the C `(void) edge_failures`) */
    let _ = edge_failures;

    /*
     * got the cheapest query tree processed by geqo; first element of the
     * population indicates the best query tree
     */
    let best_tour = &pool.data[0].string;

    let best_rel = gimme_tree(root, run, &mut private, best_tour, pool.string_length);

    let best_rel = match best_rel {
        Some(rel) => rel,
        None => panic!("geqo failed to make a valid plan"),
    };

    /* ... free memory stuff is RAII (pool/chromos/tables dropped at scope end) */

    /* ... clear root pointer to our private storage */
    root.join_search_private = None;

    best_rel
}

/// `gimme_pool_size(nr_rel)` — either the configured pool size or a good
/// default (`2^(QS+1)`, clamped to a range based on the effort value).
fn gimme_pool_size(nr_rel: i32, config: &GeqoConfig) -> i32 {
    /* Legal pool size *must* be at least 2, so ignore attempt to select 1 */
    if config.pool_size >= 2 {
        return config.pool_size;
    }

    let size = 2.0_f64.powf(nr_rel as f64 + 1.0);

    let maxsize = 50 * config.effort; /* 50 to 500 individuals */
    if size > maxsize as f64 {
        return maxsize;
    }

    let minsize = 10 * config.effort; /* 10 to 100 individuals */
    if size < minsize as f64 {
        return minsize;
    }

    size.ceil() as i32
}

/// `gimme_number_generations(pool_size)` — either the configured number of
/// generations or the pool size as a good default.
fn gimme_number_generations(pool_size: i32, config: &GeqoConfig) -> i32 {
    if config.generations > 0 {
        return config.generations;
    }

    pool_size
}
