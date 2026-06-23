//! Cost-estimation scratch structs shared by the `optimizer/path` cost and
//! enumeration crates (`costsize.c`, `joinpath.c`): the join cost workspace, the
//! cheapest-path selector, and the SEMI/ANTI selectivity factors. Mirrors
//! `nodes/pathnodes.h`.

use alloc::boxed::Box;
use alloc::vec::Vec;

use types_core::primitive::{Cardinality, Cost, Selectivity};

use crate::{Relids, RestrictInfo, SpecialJoinInfo};

/// `CostSelector` (pathnodes.h): whether a "cheapest path" search wants the
/// cheapest startup cost or the cheapest total cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostSelector {
    STARTUP_COST = 0,
    TOTAL_COST = 1,
}

/// `SemiAntiJoinFactors` (pathnodes.h): selectivity factors for SEMI/ANTI joins,
/// filled by `compute_semi_anti_join_factors`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SemiAntiJoinFactors {
    pub outer_match_frac: Selectivity,
    pub match_count: Selectivity,
}

/// `JoinPathExtraData` (pathnodes.h): extra information passed to subroutines of
/// `add_paths_to_joinrel`.
#[derive(Debug, Clone)]
pub struct JoinPathExtraData {
    /// all RestrictInfo nodes for restriction clauses that apply to this join.
    pub restrictlist: Vec<RestrictInfo>,
    /// available mergejoin clauses in this join.
    pub mergeclause_list: Vec<RestrictInfo>,
    /// true if each outer tuple provably matches no more than one inner tuple.
    pub inner_unique: bool,
    /// extra info about special joins for selectivity estimation.
    pub sjinfo: Option<Box<SpecialJoinInfo>>,
    /// see [`SemiAntiJoinFactors`] (valid only for SEMI/ANTI/inner_unique joins).
    pub semifactors: SemiAntiJoinFactors,
    /// OK targets for parameterization of result paths.
    pub param_source_rels: Relids,
}

/// `JoinCostWorkspace` (pathnodes.h): preliminary cost estimates computed by the
/// `initial_cost_*` functions and consumed by the `final_cost_*` functions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JoinCostWorkspace {
    pub disabled_nodes: i32,
    pub startup_cost: Cost,
    pub total_cost: Cost,
    pub run_cost: Cost,
    pub inner_run_cost: Cost,
    pub inner_rescan_run_cost: Cost,
    pub outer_rows: Cardinality,
    pub inner_rows: Cardinality,
    pub outer_skip_rows: Cardinality,
    pub inner_skip_rows: Cardinality,
    pub numbuckets: i32,
    pub numbatches: i32,
    pub inner_rows_total: Cardinality,
}
