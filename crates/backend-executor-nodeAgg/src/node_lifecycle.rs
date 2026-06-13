//! Node-lifecycle family: init / end / rescan, the `ExecAgg` driver, and its
//! setup helpers (phase and grouping-set selection, input fetch, the column
//! analysis that decides which outer-plan columns are needed, and the
//! per-trans build that reads the catalog for each aggregate).

use mcx::{Mcx, PgBox};
use types_core::primitive::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::nodeagg::{
    Agg, Aggref, AggStateData, AggStatePerTransData,
};
use types_nodes::nodes::Node;
use types_nodes::{Bitmapset, EStateData, SlotId};

use crate::FindColsContext;

/// `select_current_set(aggstate, setno, is_hash)` — select the current
/// grouping set; affects `current_set` and `curaggcontext`.
pub fn select_current_set(aggstate: &mut AggStateData<'_>, setno: i32, is_hash: bool) {
    todo!("decomp")
}

/// `initialize_phase(aggstate, newphase)` — switch the Agg to a new phase,
/// resetting sort state as needed.
pub fn initialize_phase<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    newphase: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `fetch_input_tuple(aggstate)` — read the next input tuple (from the sort
/// for phases > 0, else from the outer plan); returns `None` at end of input.
pub fn fetch_input_tuple<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    todo!("decomp")
}

/// `find_cols(aggstate, &aggregated, &unaggregated)` — find the columns the
/// outer plan must supply, split into those referenced under an Aggref and
/// those referenced elsewhere.
pub fn find_cols<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<(
    Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    Option<PgBox<'mcx, Bitmapset<'mcx>>>,
)> {
    todo!("decomp")
}

/// `find_cols_walker(node, context)` — expression walker collecting referenced
/// `Var` colnos into the context, marking aggregated vs unaggregated.
pub fn find_cols_walker<'mcx>(
    node: Option<&Node<'mcx>>,
    context: &mut FindColsContext<'mcx>,
) -> PgResult<bool> {
    todo!("decomp")
}

/// `find_hash_columns(aggstate)` — set up the per-hash column descriptors
/// (input/hash slot column indices) for every grouping set.
pub fn find_hash_columns<'mcx>(
    aggstate: &mut AggStateData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `build_pertrans_for_aggref(...)` — set up one `AggStatePerTransData` from
/// the aggregate's catalog rows (transfn/serialfn/deserialfn lookups, input
/// type metadata, sort/distinct comparators, per-set sort objects).
#[allow(clippy::too_many_arguments)]
pub fn build_pertrans_for_aggref<'mcx>(
    pertrans: &mut AggStatePerTransData<'mcx>,
    aggstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    aggref: &Aggref<'mcx>,
    transfn_oid: Oid,
    aggtranstype: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    init_value: Datum,
    init_value_is_null: bool,
    input_types: &[Oid],
    num_arguments: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `GetAggInitVal(textInitVal, transtype)` — convert the `agginitval` text
/// Datum into the transition type's internal Datum via its input function.
pub fn GetAggInitVal(text_init_val: Datum, transtype: Oid) -> PgResult<Datum> {
    todo!("decomp")
}

/// `ExecAgg(pstate)` — the node's `ExecProcNode` callback: produce the next
/// aggregated output tuple, or `None` at end. Dispatches on strategy/phase to
/// the sorted-grouping and hash-grouping retrieve paths.
pub fn ExecAgg<'mcx>(
    pstate: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<SlotId>> {
    todo!("decomp")
}

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

/// `ExecEndAgg(node)` — shut down the Agg node: end sorts, close hash tapes,
/// release contexts.
pub fn ExecEndAgg<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecReScanAgg(node)` — rescan the Agg node, re-using the hash table where
/// the rescan parameters allow it.
pub fn ExecReScanAgg<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
