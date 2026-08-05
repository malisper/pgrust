//! Genetic query optimizer (src/backend/optimizer/geqo/); routed to from
//! make_rel_from_joinlist. gimme_tree reuses the exact make_join_rel/
//! set_cheapest/gather machinery standard_join_search uses.
//!
//! DIVERGENCE: C deletes a private temp MemoryContext per geqo_eval to reclaim
//! candidate joinrels; pgrust's run-global handle arenas have no mid-run
//! reclamation, so candidate joinrels accumulate until the planner boundary
//! (logical state stays C-correct via geqo_eval's list truncate + hash null).
//! Growth is bounded and reclaimed at run end; footprint math in the letter.
//! DETERMINISM: pg_prng seeded from geqo_seed (C's pg_prng_fseed) — same seed,
//! same plan; a seeded PRNG, not OS entropy, so the ratchet does not flag it.

use types_error::PgResult;
use types_pathnodes::RelId;

use crate::run::PlannerRun;

mod copy;
mod eval;
mod pool;
mod random;
mod recombination;
mod selection;

// ERX (edge recombination crossover) is the operator C compiles by default
// (`#define ERX` in geqo.h); it is the only operator wired into the driver.
mod erx;

// The alternative recombination operators are `#if defined(PMX|CX|PX|OX1|OX2)`
// in C — compiled out of the default build. The ifdef discipline is rendered as
// a cargo feature: these faithful ports compile only under
// `--features geqo_nondefault_operators`, exercised by feature-gated tests.
#[cfg(feature = "geqo_nondefault_operators")]
mod cx;
#[cfg(feature = "geqo_nondefault_operators")]
mod mutation;
#[cfg(feature = "geqo_nondefault_operators")]
mod ox1;
#[cfg(feature = "geqo_nondefault_operators")]
mod ox2;
#[cfg(feature = "geqo_nondefault_operators")]
mod pmx;
#[cfg(feature = "geqo_nondefault_operators")]
mod px;

#[cfg(test)]
mod tests;

// A gene is a 1-based base-relation index into initial_rels.
type Gene = i32;

#[derive(Clone, Debug)]
struct Chromosome {
    string: Vec<Gene>,
    worth: types_core::primitive::Cost,
}

#[derive(Clone, Debug)]
struct Pool {
    data: Vec<Chromosome>,
    size: i32,
    string_length: i32,
}

// C's join_search_private payload is { initial_rels, random_state };
// initial_rels is threaded as an argument (not aliased with run), so this
// carries only the PRNG.
struct GeqoState {
    rng: pg_prng::PgPrng,
}

// GUC storage owned by GEQO (geqo_main.c defines the 5 Geqo_* globals;
// allpaths.c defines enable_geqo/geqo_threshold). Boot values mirror the
// guc_tables boot_vals exactly (pg_settings/SHOW ALL byte-identity).
guc_tables::session_guc_cluster!(GeqoGucs, GEQO_GUCS:
    (enable_geqo_cell, bool, enable_geqo, set_enable_geqo, true),
    (geqo_threshold_cell, i32, geqo_threshold, set_geqo_threshold, 12),
    (geqo_effort_cell, i32, geqo_effort, set_geqo_effort, guc_tables::consts::DEFAULT_GEQO_EFFORT),
    (geqo_pool_size_cell, i32, geqo_pool_size, set_geqo_pool_size, 0),
    (geqo_generations_cell, i32, geqo_generations, set_geqo_generations, 0),
    (geqo_selection_bias_cell, f64, geqo_selection_bias, set_geqo_selection_bias, guc_tables::consts::DEFAULT_GEQO_SELECTION_BIAS),
    (geqo_seed_cell, f64, geqo_seed, set_geqo_seed, 0.0f64),
);

/// Wire the 7 GEQO GUC slot accessors to this crate's backing store (called
/// once from init_seams). The registry skips uninstalled slots, which is why
/// these GUCs were inert before this install existed.
pub fn install_gucs() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::enable_geqo
        .install(GucVarAccessors { get: enable_geqo, set: set_enable_geqo });
    guc_tables::vars::geqo_threshold
        .install(GucVarAccessors { get: geqo_threshold, set: set_geqo_threshold });
    guc_tables::vars::Geqo_effort
        .install(GucVarAccessors { get: geqo_effort, set: set_geqo_effort });
    guc_tables::vars::Geqo_pool_size
        .install(GucVarAccessors { get: geqo_pool_size, set: set_geqo_pool_size });
    guc_tables::vars::Geqo_generations
        .install(GucVarAccessors { get: geqo_generations, set: set_geqo_generations });
    guc_tables::vars::Geqo_selection_bias
        .install(GucVarAccessors { get: geqo_selection_bias, set: set_geqo_selection_bias });
    guc_tables::vars::Geqo_seed.install(GucVarAccessors { get: geqo_seed, set: set_geqo_seed });
}

/// GA solution of the join-order problem; returns the cheapest join RelId
/// found. `initial_rels` are the base rels being joined.
pub fn geqo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    number_of_rels: usize,
    initial_rels: &[RelId],
) -> PgResult<RelId> {
    let mut state = GeqoState { rng: pg_prng::PgPrng::default() };
    random::geqo_set_seed(&mut state, geqo_seed());

    let pool_size = gimme_pool_size(number_of_rels as i32);
    let number_generations = gimme_number_generations(pool_size);

    let mut pool = pool::alloc_pool(pool_size, number_of_rels as i32);
    pool::random_init_pool(run, &mut state, initial_rels, &mut pool)?;
    // Sort once; kids thereafter displace the worst via spread_chromo.
    pool::sort_pool(&mut pool);

    let mut momma = pool::alloc_chromo(pool.string_length);
    let mut daddy = pool::alloc_chromo(pool.string_length);
    let mut edge_table = erx::alloc_edge_table(pool.string_length);

    for _generation in 0..number_generations {
        selection::geqo_selection(&mut state, &mut momma, &mut daddy, &pool, geqo_selection_bias());

        // EDGE RECOMBINATION CROSSOVER. C sets `kid = momma` and breeds the new
        // tour into momma's string in place, so momma IS the kid here.
        erx::gimme_edge_table(&momma.string, &daddy.string, pool.string_length, &mut edge_table);
        erx::gimme_tour(&mut state, &mut edge_table, &mut momma.string, pool.string_length);

        momma.worth = eval::geqo_eval(run, initial_rels, &momma.string, pool.string_length)?;
        pool::spread_chromo(&momma, &mut pool);
    }

    // Best query tree = first pool element.
    match eval::gimme_tree(run, initial_rels, &pool.data[0].string, pool.string_length)? {
        Some(rel) => Ok(rel),
        None => Err(Box::new(types_error::PgError::error(
            "geqo failed to make a valid plan".to_string(),
        ))),
    }
}

// Configured pool size (>= 2), else default 2^(nr_rel+1) clamped to
// [10*effort, 50*effort].
fn gimme_pool_size(nr_rel: i32) -> i32 {
    let configured = geqo_pool_size();
    if configured >= 2 {
        return configured;
    }
    let effort = geqo_effort();
    let size = 2.0_f64.powf(nr_rel as f64 + 1.0);
    let maxsize = 50 * effort; // 50 to 500 individuals
    if size > maxsize as f64 {
        return maxsize;
    }
    let minsize = 10 * effort; // 10 to 100 individuals
    if size < minsize as f64 {
        return minsize;
    }
    size.ceil() as i32
}

// Configured generation count, else the pool size (pushes less-fit
// individuals out before the run ends).
fn gimme_number_generations(pool_size: i32) -> i32 {
    let configured = geqo_generations();
    if configured > 0 {
        return configured;
    }
    pool_size
}
