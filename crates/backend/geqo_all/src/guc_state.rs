//! geqo_main.c-owned GUC variable storage and accessors.
//!
//! Mirrors the file-scope GUC globals defined in `geqo_main.c`:
//!
//! ```c
//! int      Geqo_effort;
//! int      Geqo_pool_size;
//! int      Geqo_generations;
//! double   Geqo_selection_bias;
//! double   Geqo_seed;
//! ```
//!
//! These are plain `int`/`double` globals (the `conf->variable` backing store
//! for the corresponding `guc_tables.c` entries `geqo_effort`,
//! `geqo_pool_size`, `geqo_generations`, `geqo_selection_bias`, `geqo_seed`).
//! They are *not* read from the ControlFile — the GUC machinery seeds them from
//! their boot values during `InitializeGUCOptions` and the planner reads them
//! through these accessors at join-search time. This crate is their canonical
//! home (geqo_main.c), so it installs the `conf->variable` accessors from
//! [`crate::init_seams`]. The boot-value defaults below mirror the
//! `boot_val`s in guc_tables.c.

extern crate std;

use core::cell::Cell;

std::thread_local! {
    /// `int Geqo_effort` — boot_val `DEFAULT_GEQO_EFFORT` (5).
    static GEQO_EFFORT: Cell<i32> = const { Cell::new(crate::main::DEFAULT_GEQO_EFFORT) };
    /// `int Geqo_pool_size` — boot_val 0 (use a suitable default).
    static GEQO_POOL_SIZE: Cell<i32> = const { Cell::new(0) };
    /// `int Geqo_generations` — boot_val 0 (use a suitable default).
    static GEQO_GENERATIONS: Cell<i32> = const { Cell::new(0) };
    /// `double Geqo_selection_bias` — boot_val `DEFAULT_GEQO_SELECTION_BIAS` (2.0).
    static GEQO_SELECTION_BIAS: Cell<f64> =
        const { Cell::new(crate::main::DEFAULT_GEQO_SELECTION_BIAS) };
    /// `double Geqo_seed` — boot_val 0.0.
    static GEQO_SEED: Cell<f64> = const { Cell::new(0.0) };
}

// --- Geqo_effort accessors (conf->variable) ---------------------------------

fn get_geqo_effort() -> i32 {
    GEQO_EFFORT.with(Cell::get)
}
fn set_geqo_effort(v: i32) {
    GEQO_EFFORT.with(|c| c.set(v));
}

// --- Geqo_pool_size accessors -----------------------------------------------

fn get_geqo_pool_size() -> i32 {
    GEQO_POOL_SIZE.with(Cell::get)
}
fn set_geqo_pool_size(v: i32) {
    GEQO_POOL_SIZE.with(|c| c.set(v));
}

// --- Geqo_generations accessors ---------------------------------------------

fn get_geqo_generations() -> i32 {
    GEQO_GENERATIONS.with(Cell::get)
}
fn set_geqo_generations(v: i32) {
    GEQO_GENERATIONS.with(|c| c.set(v));
}

// --- Geqo_selection_bias accessors ------------------------------------------

fn get_geqo_selection_bias() -> f64 {
    GEQO_SELECTION_BIAS.with(Cell::get)
}
fn set_geqo_selection_bias(v: f64) {
    GEQO_SELECTION_BIAS.with(|c| c.set(v));
}

// --- Geqo_seed accessors ----------------------------------------------------

fn get_geqo_seed() -> f64 {
    GEQO_SEED.with(Cell::get)
}
fn set_geqo_seed(v: f64) {
    GEQO_SEED.with(|c| c.set(v));
}

/// Install the `conf->variable` accessors for the five GEQO GUCs into the
/// guc_tables slots (geqo_main.c owns the storage). Called once from
/// [`crate::init_seams`] at single-threaded startup.
pub fn install() {
    use ::guc_tables::{vars, GucVarAccessors};

    vars::Geqo_effort.install(GucVarAccessors {
        get: get_geqo_effort,
        set: set_geqo_effort,
    });
    vars::Geqo_pool_size.install(GucVarAccessors {
        get: get_geqo_pool_size,
        set: set_geqo_pool_size,
    });
    vars::Geqo_generations.install(GucVarAccessors {
        get: get_geqo_generations,
        set: set_geqo_generations,
    });
    vars::Geqo_selection_bias.install(GucVarAccessors {
        get: get_geqo_selection_bias,
        set: set_geqo_selection_bias,
    });
    vars::Geqo_seed.install(GucVarAccessors {
        get: get_geqo_seed,
        set: set_geqo_seed,
    });
}
