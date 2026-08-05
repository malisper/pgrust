use ::types_core::primitive::{Cardinality, Cost, Selectivity};

use crate::SpecialJoinInfo;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostSelector {
    STARTUP_COST = 0,
    TOTAL_COST = 1,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SemiAntiJoinFactors {
    pub outer_match_frac: Selectivity,
    pub match_count: Selectivity,
}

// `sjinfo` is borrowed: joinpath builds one SpecialJoinInfo and threads it
// through every cost workspace; cloning by value was fabled #401's alloc storm.
#[derive(Debug, Clone, Copy)]
pub struct JoinPathExtraData<'a, 'mcx> {
    pub inner_unique: bool,
    pub sjinfo: Option<&'a SpecialJoinInfo<'mcx>>,
    pub semifactors: SemiAntiJoinFactors,
}

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
