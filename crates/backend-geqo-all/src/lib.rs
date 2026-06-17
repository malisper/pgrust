#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::manual_swap)]
#![allow(clippy::too_many_arguments)]

//! Idiomatic safe-Rust port of `src/backend/optimizer/geqo/` (postgres-18.3):
//! the **genetic query optimizer** (GEQO).
//!
//! The whole `geqo/` subdirectory (15 `.c` files) is ported here as one crate
//! with one module per source file. GEQO solves the join-order problem as a
//! constrained Traveling Salesman Problem with a genetic algorithm: a *gene* is
//! a base-relation index, a *tour* (chromosome) is a permutation of all base
//! rels, a *pool* is a sorted population of tours, and each generation breeds a
//! child tour from two parents via a recombination operator, evaluates its
//! fitness (the cheapest total cost of the join tree built in that order), and
//! spreads it back into the pool displacing the worst member.
//!
//! # Recombination operator selection (`geqo.h`)
//!
//! PostgreSQL picks exactly one recombination mechanism with a `#define` in
//! `geqo.h`; the default is `ERX` (edge recombination crossover). The C build
//! `#if`-guards each operator's source file on its macro, so only one is
//! compiled. This port mirrors that with a `cfg`-style module gate driven by
//! the [`active operator`](operator) constant: the `ERX` module is built and
//! wired into [`main::geqo`]; the alternative operators (`PMX`, `CX`, `PX`,
//! `OX1`, `OX2`, plus `mutation`) are ported faithfully but compiled as
//! always-available library functions (Rust has no per-file conditional
//! compilation keyed on a source `#define`, and gating them out would discard
//! a 1:1 port). They are exercised by unit tests but not reached by the default
//! `geqo()` driver, exactly as in C.
//!
//! # Owned values, not raw pointers
//!
//! The C code `palloc`s `Gene *` strings, `Chromosome *`/`Pool *`/`Edge *`/
//! `City *` arrays and frees them explicitly. Here those become owned
//! [`alloc::vec::Vec`]s inside owned structs ([`Chromosome`], [`pool::Pool`],
//! [`erx::Edge`], [`recombination::City`]); RAII replaces the explicit
//! `free_*`/`pfree` calls. The PRNG state that C stashes in
//! `root->join_search_private` is carried in an owned [`GeqoPrivateData`] passed
//! by `&mut` through the GA routines (the idiomatic analogue of the
//! `void *join_search_private` pointer).
//!
//! # The planner boundary (seams)
//!
//! `geqo_eval`/`gimme_tree` reach back into the rest of the planner —
//! `make_join_rel`, `generate_partitionwise_join_paths`,
//! `generate_useful_gather_paths`, `set_cheapest`, `have_relevant_joinclause`,
//! `have_join_order_restriction` — and create a private temp `MemoryContext`.
//! Pulling those sibling-optimizer crates in would form a dependency cycle
//! (`geqo_eval` → `make_join_rel` → … → the join-search hook → `geqo`), so they
//! cross the boundary through the seams in
//! [`backend_geqo_all_seams`]. The clump-merging *algorithm*
//! of `gimme_tree`/`merge_clump`/`desirable_join` is ported 1:1 in-crate.

extern crate alloc;

#[cfg(test)]
extern crate std;

use types_pathnodes::RelId;
use types_core::primitive::Cost;

pub mod copy;
pub mod cx;
pub mod erx;
pub mod eval;
pub mod guc_state;
#[path = "geqo_main.rs"]
pub mod main;
pub mod misc;
pub mod mutation;
pub mod ox1;
pub mod ox2;
pub mod pmx;
pub mod pool;
pub mod px;
pub mod random;
pub mod recombination;
pub mod selection;

#[cfg(test)]
mod tests;

/* --------------------------------------------------------------------------
 * geqo_gene.h — genome representation
 * ------------------------------------------------------------------------ */

/// `Gene` (`geqo_gene.h`): "we presume that int instead of Relid is o.k. for
/// Gene; so don't change it!". A gene is a 1-based base-relation index into
/// `GeqoPrivateData::initial_rels`.
pub type Gene = i32;

/// `Chromosome` (`geqo_gene.h`): one individual = a tour (`string`) plus its
/// fitness (`worth`). The C `Gene *string` becomes an owned `Vec<Gene>`.
#[derive(Clone, Debug)]
pub struct Chromosome {
    pub string: alloc::vec::Vec<Gene>,
    pub worth: Cost,
}

/// `GeqoPrivateData` (`geqo.h`): the per-run private state C stashes in
/// `root->join_search_private`. `initial_rels` is the set of base
/// [`RelId`](types::pathnodes::RelId)s being joined (handles into the planner's
/// rel arena), and `random_state` is the run's PRNG (an idiomatic
/// [`pg_prng::PgPrng`], the analogue of `pg_prng_state`).
#[derive(Clone, Debug, Default)]
pub struct GeqoPrivateData {
    pub initial_rels: alloc::vec::Vec<RelId>,
    pub random_state: pg_prng::PgPrng,
}

/* --------------------------------------------------------------------------
 * Active recombination operator (geqo.h `#define ERX`)
 * ------------------------------------------------------------------------ */

/// The recombination operators GEQO can be built with (`geqo.h`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    /// edge recombination crossover (the PostgreSQL default `#define ERX`)
    Erx,
    /// partially matched crossover
    Pmx,
    /// cycle crossover
    Cx,
    /// position crossover
    Px,
    /// order crossover (Davis)
    Ox1,
    /// order crossover (Syswerda)
    Ox2,
}

/// The recombination mechanism selected at "compile time" — mirrors the
/// `#define ERX` in `geqo.h`. [`main::geqo`] dispatches on this.
pub const operator: Operator = Operator::Erx;

/// Install every seam this crate owns. GEQO owns no inward seam (no other crate
/// calls into it across a cycle — the join-search hook that reaches `geqo()` is
/// not modeled here). The planner externals GEQO *consumes* live in
/// [`backend_geqo_all_seams`] and are installed by their owners
/// (joinrels/joininfo/planner-memory), not here.
///
/// GEQO *does* own the five GEQO GUC variables (`Geqo_effort`,
/// `Geqo_pool_size`, `Geqo_generations`, `Geqo_selection_bias`, `Geqo_seed`),
/// defined as file-scope globals in `geqo_main.c`. Their `conf->variable`
/// accessors over this crate's backing store are installed here.
pub fn init_seams() {
    guc_state::install();
}
