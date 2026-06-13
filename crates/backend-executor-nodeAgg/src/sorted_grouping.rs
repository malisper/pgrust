//! Sorted-grouping family: the AGG_PLAIN / AGG_SORTED retrieve path, which
//! reads sorted (or single-group) input, advances transition state per group,
//! and returns one output tuple per group.

use types_error::PgResult;
use types_nodes::nodeagg::AggStateData;
use types_nodes::{EStateData, SlotId};

/// `agg_retrieve_direct(aggstate)` — the plain/sorted-grouping driver: read
/// input tuples, detect group boundaries with the per-phase equality
/// functions, advance transition state, and emit each group's projected
/// result. Handles grouping-set phases and rollup. Returns `None` at the end
/// of the scan.
pub fn agg_retrieve_direct<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    todo!("decomp")
}
