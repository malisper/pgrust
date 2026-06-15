//! ABI types shared by the `optimizer/plan` and `optimizer/path` cost/plan
//! crates (`planner.c`, `createplan.c`, `costsize.c`, `joinpath.c`, …).
//!
//! These mirror the C definitions from `nodes/pathnodes.h`, `nodes/plannodes.h`,
//! `optimizer/cost.h`, and `optimizer/paths.h` that are not already exported by
//! other `pgrust-pg-ffi` modules.  They are the genuinely-missing pieces of the
//! planner ABI: the join cost-estimation scratch structs, the cheapest-path
//! selector and pathkey-comparison enums, and opaque pointer typedefs for the
//! handful of parse/plan node kinds (`Param`, `SubLink`, `WindowClause`,
//! `PlannedStmt`) that the plan/path public API references but that no ported
//! crate yet owns as a full `#[repr(C)]` layout.

use core::ffi::c_void;

use crate::pathnodes::{Cardinality, Cost, Relids};
use crate::types::Selectivity;

/// `Param *` (primnodes.h) — opaque to the plan/path skeletons.
pub type ParamPtr = *mut c_void;
/// `SubLink *` (primnodes.h) — opaque to the plan/path skeletons.
pub type SubLinkPtr = *mut c_void;
/// `WindowClause *` (parsenodes.h) — opaque to the plan/path skeletons.
pub type WindowClausePtr = *mut c_void;
/// `PlannedStmt *` (plannodes.h) — opaque to the plan/path skeletons.
pub type PlannedStmtPtr = *mut c_void;

/// `CostSelector` (pathnodes.h): whether a "cheapest path" search wants the
/// cheapest startup cost or the cheapest total cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum CostSelector {
    STARTUP_COST = 0,
    TOTAL_COST = 1,
}

/// `PathKeysComparison` (paths.h): result of comparing two pathkey lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum PathKeysComparison {
    /// pathkeys are identical
    PATHKEYS_EQUAL = 0,
    /// pathkey 1 is a superset of pathkey 2
    PATHKEYS_BETTER1 = 1,
    /// vice versa
    PATHKEYS_BETTER2 = 2,
    /// neither pathkey includes the other
    PATHKEYS_DIFFERENT = 3,
}

/// `SemiAntiJoinFactors` (pathnodes.h): selectivity factors for SEMI/ANTI joins,
/// filled by `compute_semi_anti_join_factors`.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct SemiAntiJoinFactors {
    pub outer_match_frac: Selectivity,
    pub match_count: Selectivity,
}

/// `JoinPathExtraData` (pathnodes.h): extra information passed to subroutines of
/// `add_paths_to_joinrel`.
#[repr(C)]
pub struct JoinPathExtraData {
    /// all RestrictInfo nodes for restriction clauses that apply to this join.
    pub restrictlist: *mut crate::list::List,
    /// available mergejoin clauses in this join.
    pub mergeclause_list: *mut crate::list::List,
    /// true if each outer tuple provably matches no more than one inner tuple.
    pub inner_unique: bool,
    /// extra info about special joins for selectivity estimation.
    pub sjinfo: *mut crate::pathnodes::SpecialJoinInfo,
    /// see [`SemiAntiJoinFactors`] (valid only for SEMI/ANTI/inner_unique joins).
    pub semifactors: SemiAntiJoinFactors,
    /// OK targets for parameterization of result paths.
    pub param_source_rels: Relids,
}

/// `JoinCostWorkspace` (pathnodes.h): preliminary cost estimates computed by the
/// `initial_cost_*` functions and consumed by the `final_cost_*` functions.
#[repr(C)]
pub struct JoinCostWorkspace {
    /* Preliminary cost estimates --- must not be larger than final ones! */
    pub disabled_nodes: i32,
    /// cost expended before fetching any tuples.
    pub startup_cost: Cost,
    /// total cost (assuming all tuples fetched).
    pub total_cost: Cost,

    /* Fields below here should be treated as private to costsize.c */
    /// non-startup cost components.
    pub run_cost: Cost,

    /* private for cost_nestloop code */
    /// also used by cost_mergejoin code.
    pub inner_run_cost: Cost,
    pub inner_rescan_run_cost: Cost,

    /* private for cost_mergejoin code */
    pub outer_rows: Cardinality,
    pub inner_rows: Cardinality,
    pub outer_skip_rows: Cardinality,
    pub inner_skip_rows: Cardinality,

    /* private for cost_hashjoin code */
    pub numbuckets: i32,
    pub numbatches: i32,
    pub inner_rows_total: Cardinality,
}
