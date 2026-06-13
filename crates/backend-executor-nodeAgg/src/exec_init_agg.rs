//! `ExecInitAgg` sub-module: building the `AggState` from the Agg plan node.
//!
//! Split out of [`crate::node_lifecycle`] because the C `ExecInitAgg`
//! (`nodeAgg.c` ~854 lines: catalog reads for every aggregate, per-trans /
//! per-agg setup, phase and grouping-set layout, hash-table and context
//! creation) is far larger than the rest of the lifecycle family combined.

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::nodeagg::{Agg, AggStateData};
use types_nodes::EStateData;

/// `ExecInitAgg(node, estate, eflags)` — build the `AggState` from the Agg
/// plan node: catalog reads for every aggregate, per-trans/per-agg setup,
/// phase and grouping-set layout, hash-table and context creation.
pub fn ExecInitAgg<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<PgBox<'mcx, AggStateData<'mcx>>> {
    todo!("decomp")
}
