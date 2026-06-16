//! `AggState` runtime state and its per-aggregate satellites
//! (`executor/nodeAgg.h`, `executor/execnodes.h`).
//!
//! These structs were relocated out of `types-nodes::nodeagg` into their real
//! owner (`backend-executor-nodeAgg`, which sits ABOVE `utils/sort/logtape.c`)
//! so that [`AggStateData::hash_tapeset`] can hold a REAL owned
//! [`LogicalTapeSet`] value instead of an opaque handle into a side-table
//! registry. A `LogicalTape *` is a `usize` slot index into the set's `tapes`
//! vector (the faithful rendering of C's pointer into the set-owned tape array).
//!
//! The plan-node vocabulary (`AggStrategy`/`AggSplit`/`Aggref`/`Agg`), the
//! execGrouping hash-table types (`TupleHashTable`/`TupleHashIterator`), and the
//! `HyperLogLog` estimator stay in the foundational `types-nodes` crate; this
//! module names them downward through `types_nodes::`.

use mcx::{Mcx, MemoryContext, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{HeapTupleData, TupleDescData};

use types_nodes::bitmapset::Bitmapset;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::{EcxtId, ScanStateData, SlotId};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::nodeagg::{
    Agg, AggSplit, AggStrategy, Aggref, HyperLogLog, TupleHashIterator, TupleHashTable,
};
use types_nodes::nodesort::{Sort, Tuplesortstate};

use backend_utils_sort_storage_seams::LogicalTapeSet;

// ---------------------------------------------------------------------------
// nodeAgg.h per-* state structs
// ---------------------------------------------------------------------------

/// `AggStatePerTransData` (executor/nodeAgg.h) — per-aggregate transition
/// working state.
#[derive(Debug)]
pub struct AggStatePerTransData<'mcx> {
    /// `Aggref *aggref` — the (first) Aggref this state value is for.
    pub aggref: Option<PgBox<'mcx, Aggref<'mcx>>>,
    /// `bool aggshared`.
    pub aggshared: bool,
    /// `bool aggsortrequired`.
    pub aggsortrequired: bool,
    /// `int numInputs`.
    pub num_inputs: i32,
    /// `int numTransInputs`.
    pub num_trans_inputs: i32,
    /// `Oid transfn_oid` — state transition or combine function.
    pub transfn_oid: Oid,
    /// `Oid serialfn_oid`.
    pub serialfn_oid: Oid,
    /// `Oid deserialfn_oid`.
    pub deserialfn_oid: Oid,
    /// `Oid aggtranstype`.
    pub aggtranstype: Oid,
    /// `FmgrInfo transfn`.
    pub transfn: FmgrInfo,
    /// `FmgrInfo serialfn`.
    pub serialfn: FmgrInfo,
    /// `FmgrInfo deserialfn`.
    pub deserialfn: FmgrInfo,
    /// `Oid aggCollation`.
    pub agg_collation: Oid,
    /// `int numSortCols`.
    pub num_sort_cols: i32,
    /// `int numDistinctCols`.
    pub num_distinct_cols: i32,
    /// `AttrNumber *sortColIdx`.
    pub sort_col_idx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *sortOperators`.
    pub sort_operators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *sortCollations`.
    pub sort_collations: Option<PgVec<'mcx, Oid>>,
    /// `bool *sortNullsFirst`.
    pub sort_nulls_first: Option<PgVec<'mcx, bool>>,
    /// `FmgrInfo equalfnOne` — single-column DISTINCT comparator.
    pub equalfn_one: FmgrInfo,
    /// `ExprState *equalfnMulti` — multi-column DISTINCT comparator.
    pub equalfn_multi: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `Datum initValue`.
    pub init_value: Datum<'mcx>,
    /// `bool initValueIsNull`.
    pub init_value_is_null: bool,
    /// `int16 inputtypeLen`.
    pub inputtype_len: i16,
    /// `int16 transtypeLen`.
    pub transtype_len: i16,
    /// `bool inputtypeByVal`.
    pub inputtype_by_val: bool,
    /// `bool transtypeByVal`.
    pub transtype_by_val: bool,
    /// `TupleTableSlot *sortslot` — current input tuple.
    pub sortslot: Option<SlotId>,
    /// `TupleTableSlot *uniqslot` — multi-column DISTINCT.
    pub uniqslot: Option<SlotId>,
    /// `TupleDesc sortdesc` — descriptor of input tuples.
    pub sortdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `Datum lastdatum` — single-column DISTINCT last value.
    pub lastdatum: Datum<'mcx>,
    /// `bool lastisnull`.
    pub lastisnull: bool,
    /// `bool haslast`.
    pub haslast: bool,
    /// `Tuplesortstate **sortstates` — one per grouping set, if DISTINCT/ORDER BY.
    pub sortstates: Option<PgVec<'mcx, Option<PgBox<'mcx, Tuplesortstate<'mcx>>>>>,
    /// `FunctionCallInfo transfn_fcinfo` — pre-initialized transfn call info.
    pub transfn_fcinfo: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
    /// `FunctionCallInfo serialfn_fcinfo`.
    pub serialfn_fcinfo: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
    /// `FunctionCallInfo deserialfn_fcinfo`.
    pub deserialfn_fcinfo: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
}

impl Default for AggStatePerTransData<'_> {
    fn default() -> Self {
        // C `palloc0` zero-init of the per-trans entry: the canonical `Datum`
        // is not itself `Default`, so spell out the NULL values explicitly.
        AggStatePerTransData {
            aggref: None,
            aggshared: false,
            aggsortrequired: false,
            num_inputs: 0,
            num_trans_inputs: 0,
            transfn_oid: Default::default(),
            serialfn_oid: Default::default(),
            deserialfn_oid: Default::default(),
            aggtranstype: Default::default(),
            transfn: Default::default(),
            serialfn: Default::default(),
            deserialfn: Default::default(),
            agg_collation: Default::default(),
            num_sort_cols: 0,
            num_distinct_cols: 0,
            sort_col_idx: None,
            sort_operators: None,
            sort_collations: None,
            sort_nulls_first: None,
            equalfn_one: Default::default(),
            equalfn_multi: None,
            init_value: Datum::null(),
            init_value_is_null: false,
            inputtype_len: 0,
            transtype_len: 0,
            inputtype_by_val: false,
            transtype_by_val: false,
            sortslot: None,
            uniqslot: None,
            sortdesc: None,
            lastdatum: Datum::null(),
            lastisnull: false,
            haslast: false,
            sortstates: None,
            transfn_fcinfo: None,
            serialfn_fcinfo: None,
            deserialfn_fcinfo: None,
        }
    }
}

/// `AggStatePerAggData` (executor/nodeAgg.h) — per-aggregate finalfn info.
#[derive(Debug, Default)]
pub struct AggStatePerAggData<'mcx> {
    /// `Aggref *aggref`.
    pub aggref: Option<PgBox<'mcx, Aggref<'mcx>>>,
    /// `int transno` — index of the state value this agg uses.
    pub transno: i32,
    /// `Oid finalfn_oid` — final function (may be InvalidOid).
    pub finalfn_oid: Oid,
    /// `FmgrInfo finalfn`.
    pub finalfn: FmgrInfo,
    /// `int numFinalArgs`.
    pub num_final_args: i32,
    /// `List *aggdirectargs` — ExprStates for direct-argument expressions.
    pub aggdirectargs: Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>,
    /// `int16 resulttypeLen`.
    pub resulttype_len: i16,
    /// `bool resulttypeByVal`.
    pub resulttype_by_val: bool,
    /// `bool shareable`.
    pub shareable: bool,
}

/// `AggStatePerGroupData` (executor/nodeAgg.h) — per-agg-per-group working
/// state. `FIELDNO_AGGSTATEPERGROUPDATA_*`: transValue=0, transValueIsNull=1,
/// noTransValue=2.
#[derive(Clone, Debug)]
pub struct AggStatePerGroupData<'mcx> {
    /// `Datum transValue` — current transition value (field 0).
    pub trans_value: Datum<'mcx>,
    /// `bool transValueIsNull` (field 1).
    pub trans_value_is_null: bool,
    /// `bool noTransValue` — true if transValue not set yet (field 2).
    pub no_trans_value: bool,
}

impl Default for AggStatePerGroupData<'_> {
    fn default() -> Self {
        // C `palloc0` zero-init of a per-group entry.
        AggStatePerGroupData {
            trans_value: Datum::null(),
            trans_value_is_null: false,
            no_trans_value: false,
        }
    }
}

/// `AggStatePerPhaseData` (executor/nodeAgg.h) — per-grouping-set-phase state.
#[derive(Debug, Default)]
pub struct AggStatePerPhaseData<'mcx> {
    /// `AggStrategy aggstrategy` — strategy for this phase.
    pub aggstrategy: AggStrategy,
    /// `int numsets` — number of grouping sets (or 0).
    pub numsets: i32,
    /// `int *gset_lengths` — lengths of grouping sets.
    pub gset_lengths: Option<PgVec<'mcx, i32>>,
    /// `Bitmapset **grouped_cols` — column groupings for rollup.
    pub grouped_cols: Option<PgVec<'mcx, PgBox<'mcx, Bitmapset<'mcx>>>>,
    /// `ExprState **eqfunctions` — equality expr indexed by nr of cols.
    pub eqfunctions: Option<PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>>,
    /// `Agg *aggnode` — Agg node for phase data.
    pub aggnode: Option<PgBox<'mcx, Agg<'mcx>>>,
    /// `Sort *sortnode` — Sort node for input ordering for phase.
    pub sortnode: Option<PgBox<'mcx, Sort<'mcx>>>,
    /// `ExprState *evaltrans` — evaluation of transition functions.
    pub evaltrans: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *evaltrans_cache[2][2]` — cached compiled variants:
    /// [outerops|MinimalTuple][no-nullcheck|nullcheck].
    pub evaltrans_cache: [[Option<PgBox<'mcx, ExprState<'mcx>>>; 2]; 2],
}

/// `AggStatePerHashData` (executor/nodeAgg.h) — per-hashtable state.
#[derive(Debug, Default)]
pub struct AggStatePerHashData<'mcx> {
    /// `TupleHashTable hashtable` — the real owned table (`TupleHashTable` in
    /// C is `TupleHashTableData *`; carried by box in the owned model).
    pub hashtable: Option<alloc::boxed::Box<TupleHashTable<'mcx>>>,
    /// `TupleHashIterator hashiter`.
    pub hashiter: TupleHashIterator,
    /// `TupleTableSlot *hashslot` — slot for loading hash table.
    pub hashslot: Option<SlotId>,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash fns.
    pub hashfunctions: Option<PgVec<'mcx, FmgrInfo>>,
    /// `Oid *eqfuncoids` — per-grouping-field equality fns.
    pub eqfuncoids: Option<PgVec<'mcx, Oid>>,
    /// `int numCols`.
    pub num_cols: i32,
    /// `int numhashGrpCols`.
    pub numhash_grp_cols: i32,
    /// `int largestGrpColIdx`.
    pub largest_grp_col_idx: i32,
    /// `AttrNumber *hashGrpColIdxInput`.
    pub hash_grp_col_idx_input: Option<PgVec<'mcx, AttrNumber>>,
    /// `AttrNumber *hashGrpColIdxHash`.
    pub hash_grp_col_idx_hash: Option<PgVec<'mcx, AttrNumber>>,
    /// `Agg *aggnode` — original Agg node, for numGroups etc.
    pub aggnode: Option<PgBox<'mcx, Agg<'mcx>>>,
}

// ---------------------------------------------------------------------------
// SharedAggInfo (executor/execnodes.h) — DSM per-worker container
// ---------------------------------------------------------------------------

/// `AggregateInstrumentation` (executor/execnodes.h).
#[derive(Clone, Copy, Debug, Default)]
pub struct AggregateInstrumentation {
    /// `Size hash_mem_peak`.
    pub hash_mem_peak: usize,
    /// `uint64 hash_disk_used`.
    pub hash_disk_used: u64,
    /// `int hash_batches_used`.
    pub hash_batches_used: i32,
}

/// `SharedAggInfo` (executor/execnodes.h) — shared-memory per-worker container.
/// C uses a `FLEXIBLE_ARRAY_MEMBER` tail; the port carries it as a counted
/// slice whose `num_workers` mirrors C's leading int.
#[derive(Debug, Default)]
pub struct SharedAggInfo<'mcx> {
    /// `int num_workers`.
    pub num_workers: i32,
    /// `AggregateInstrumentation sinstrument[]`.
    pub sinstrument: Option<PgVec<'mcx, AggregateInstrumentation>>,
}

// ---------------------------------------------------------------------------
// AggState (executor/execnodes.h)
// ---------------------------------------------------------------------------

/// `AggState` (executor/execnodes.h) — runtime state for an Agg node.
/// `FIELDNO_AGGSTATE_*`: curaggcontext=14, curpertrans=16, current_set=20,
/// all_pergroups=54.
#[derive(Debug, Default)]
pub struct AggStateData<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `List *aggs` — all Aggref nodes in targetlist & quals.
    pub aggs: Option<PgVec<'mcx, PgBox<'mcx, Aggref<'mcx>>>>,
    /// The expression-tree-shaped [`types_nodes::primnodes::Aggref`] originals
    /// for each entry of [`Self::aggs`], parallel by index. In C there is one
    /// `Aggref` struct; the repo splits the expression-tree node
    /// (`primnodes::Aggref`) from the executor satellite (`nodeagg::Aggref`).
    /// `ExecInitExprRec` discovers the `primnodes::Aggref`s; `ExecInitAgg`
    /// converts each into the satellite for `aggs` and keeps the original here so
    /// the parse_agg helpers (`get_aggregate_argtypes`,
    /// `build_aggregate_*fn_expr`) — which take `&primnodes::Aggref` — can be
    /// called. Compile-time-only bookkeeping (not in the C struct). `None` is
    /// the pre-discovery NIL.
    pub aggs_prim: Option<PgVec<'mcx, types_nodes::primnodes::Aggref>>,
    /// `int numaggs`.
    pub numaggs: i32,
    /// `int numtrans`.
    pub numtrans: i32,
    /// `AggStrategy aggstrategy`.
    pub aggstrategy: AggStrategy,
    /// `AggSplit aggsplit`.
    pub aggsplit: AggSplit,
    /// `AggStatePerPhase phase` — index into `phases` of the current phase.
    pub phase: i32,
    /// `int numphases`.
    pub numphases: i32,
    /// `int current_phase`.
    pub current_phase: i32,
    /// `AggStatePerAgg peragg`.
    pub peragg: Option<PgVec<'mcx, AggStatePerAggData<'mcx>>>,
    /// `AggStatePerTrans pertrans`.
    pub pertrans: Option<PgVec<'mcx, AggStatePerTransData<'mcx>>>,
    /// `ExprContext *hashcontext`. In C this is an `ExprContext *` aliasing one
    /// context owned by the EState's pool (`CreateWorkExprContext(estate)`); in
    /// the owned model `ExprContext *` is an [`EcxtId`] index into
    /// `EStateData::es_exprcontexts`, mirroring `PlanStateData::ps_ExprContext`.
    pub hashcontext: Option<EcxtId>,
    /// `ExprContext **aggcontexts` — econtexts per grouping set. Each element is
    /// an `ExprContext *` aliasing an EState-pool context
    /// (`ExecAssignExprContext`); in the owned model that is an [`EcxtId`].
    pub aggcontexts: Option<PgVec<'mcx, EcxtId>>,
    /// `ExprContext *tmpcontext` — aliases `ss.ps.ps_ExprContext` (itself an
    /// [`EcxtId`] into the EState pool); carried as the same id.
    pub tmpcontext: Option<EcxtId>,
    /// `ExprContext *curaggcontext` — index into `aggcontexts` (field 14).
    pub curaggcontext: i32,
    /// `AggStatePerAgg curperagg` — index into `peragg`, or -1.
    pub curperagg: i32,
    /// `AggStatePerTrans curpertrans` — index into `pertrans`, or -1 (field 16).
    pub curpertrans: i32,
    /// `bool input_done`.
    pub input_done: bool,
    /// `bool agg_done`.
    pub agg_done: bool,
    /// `int projected_set`.
    pub projected_set: i32,
    /// `int current_set` (field 20).
    pub current_set: i32,
    /// `Bitmapset *grouped_cols`.
    pub grouped_cols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *all_grouped_cols`.
    pub all_grouped_cols: Option<PgVec<'mcx, i32>>,
    /// `Bitmapset *colnos_needed`.
    pub colnos_needed: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `int max_colno_needed`.
    pub max_colno_needed: i32,
    /// `bool all_cols_needed`.
    pub all_cols_needed: bool,
    /// `int maxsets`.
    pub maxsets: i32,
    /// `AggStatePerPhase phases` — array of all phases.
    pub phases: Option<PgVec<'mcx, AggStatePerPhaseData<'mcx>>>,
    /// `Tuplesortstate *sort_in`.
    pub sort_in: Option<PgBox<'mcx, Tuplesortstate<'mcx>>>,
    /// `Tuplesortstate *sort_out`.
    pub sort_out: Option<PgBox<'mcx, Tuplesortstate<'mcx>>>,
    /// `TupleTableSlot *sort_slot`.
    pub sort_slot: Option<SlotId>,
    /// `AggStatePerGroup *pergroups` — grouping-set-indexed per-group arrays.
    pub pergroups: Option<PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>>>,
    /// `HeapTuple grp_firstTuple` — copy of first tuple of current group.
    pub grp_first_tuple: Option<PgBox<'mcx, HeapTupleData<'mcx>>>,
    /// `bool table_filled`.
    pub table_filled: bool,
    /// `int num_hashes`.
    pub num_hashes: i32,
    /// `MemoryContext hash_metacxt`.
    pub hash_metacxt: Option<MemoryContext>,
    /// `MemoryContext hash_tablecxt`.
    pub hash_tablecxt: Option<MemoryContext>,
    /// `LogicalTapeSet *hash_tapeset` — the real owned tape set, held by value
    /// (no handle/registry). `None` is the C `NULL`.
    pub hash_tapeset: Option<PgBox<'mcx, LogicalTapeSet<'mcx>>>,
    /// `HashAggSpill *hash_spills` — per grouping set, first pass only.
    pub hash_spills: Option<PgVec<'mcx, HashAggSpill<'mcx>>>,
    /// `TupleTableSlot *hash_spill_rslot`.
    pub hash_spill_rslot: Option<SlotId>,
    /// `TupleTableSlot *hash_spill_wslot`.
    pub hash_spill_wslot: Option<SlotId>,
    /// `List *hash_batches` — batches remaining to be processed.
    pub hash_batches: Option<PgVec<'mcx, PgBox<'mcx, HashAggBatch>>>,
    /// `bool hash_ever_spilled`.
    pub hash_ever_spilled: bool,
    /// `bool hash_spill_mode`.
    pub hash_spill_mode: bool,
    /// `Size hash_mem_limit`.
    pub hash_mem_limit: usize,
    /// `uint64 hash_ngroups_limit`.
    pub hash_ngroups_limit: u64,
    /// `int hash_planned_partitions`.
    pub hash_planned_partitions: i32,
    /// `double hashentrysize`.
    pub hashentrysize: f64,
    /// `Size hash_mem_peak`.
    pub hash_mem_peak: usize,
    /// `uint64 hash_ngroups_current`.
    pub hash_ngroups_current: u64,
    /// `uint64 hash_disk_used`.
    pub hash_disk_used: u64,
    /// `int hash_batches_used`.
    pub hash_batches_used: i32,
    /// `AggStatePerHash perhash` — array of per-hashtable data.
    pub perhash: Option<PgVec<'mcx, AggStatePerHashData<'mcx>>>,
    /// `AggStatePerGroup *hash_pergroup`.
    pub hash_pergroup: Option<PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>>>,
    /// `AggStatePerGroup *all_pergroups` (field 54).
    pub all_pergroups: Option<PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData<'mcx>>>>>,
    /// `SharedAggInfo *shared_info` — one entry per worker.
    pub shared_info: Option<PgBox<'mcx, SharedAggInfo<'mcx>>>,
}

// ---------------------------------------------------------------------------
// nodeAgg.c-local structs (HashAggSpill / HashAggBatch)
// ---------------------------------------------------------------------------

/// `HashAggSpill` (nodeAgg.c) — set of in-progress spill files for one batch.
#[derive(Debug, Default)]
pub struct HashAggSpill<'mcx> {
    /// `int npartitions` — number of partitions.
    pub npartitions: i32,
    /// `LogicalTape **partitions` — spill partition tapes, each a `usize` slot
    /// index into the owning [`AggStateData::hash_tapeset`]'s `tapes` vector.
    pub partitions: Option<PgVec<'mcx, Option<usize>>>,
    /// `int64 *ntuples` — number of tuples in each partition.
    pub ntuples: Option<PgVec<'mcx, i64>>,
    /// `uint32 mask` — mask to find partition from hash value.
    pub mask: u32,
    /// `int shift` — after masking, shift down this many bits.
    pub shift: i32,
    /// `hyperLogLogState *hll_card` — cardinality estimator per partition.
    /// C `palloc0(sizeof(hyperLogLogState) * npartitions)`: an array of the
    /// estimator state held by value (one per partition). The operations live
    /// in `backend-lib-hyperloglog` and borrow `&mut hll_card[i]`.
    pub hll_card: Option<PgVec<'mcx, HyperLogLog<'mcx>>>,
}

/// `HashAggBatch` (nodeAgg.c) — one batch of spilled tuples to refill from.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashAggBatch {
    /// `int setno` — grouping set.
    pub setno: i32,
    /// `int used_bits` — number of bits of hash already used.
    pub used_bits: i32,
    /// `LogicalTape *input_tape` — input partition, a `usize` slot index into
    /// the owning tape set's `tapes` vector.
    pub input_tape: Option<usize>,
    /// `int64 input_tuples` — number of tuples in this batch.
    pub input_tuples: i64,
    /// `double input_card` — estimated group cardinality.
    pub input_card: f64,
}

impl<'mcx> AggStateData<'mcx> {
    /// Allocate an empty `AggState` (shape parity with C's `makeNode(AggState)`;
    /// fallible on OOM). The body phase fills it.
    pub fn new_in(_mcx: Mcx<'mcx>) -> PgResult<Self> {
        Ok(Self::default())
    }
}

// ---------------------------------------------------------------------------
// PlanStateNode::Agg carrier — let an `AggStateData<'mcx>` ride through the
// central `types_nodes::PlanStateNode` enum (and across the nodeAgg->execExpr
// `exec_build_agg_trans` seam edge) behind the tag-checked, owned
// `AggStateLive` trait object. `AggStateData` lives in THIS crate, above
// `types-nodes`, so the enum cannot name it directly; this is the faithful
// rendering of C's `castNode(AggState, planstate)` across the crate boundary.
// ---------------------------------------------------------------------------

impl<'mcx> types_nodes::aggstate_carrier::AggStateLive<'mcx> for AggStateData<'mcx> {
    fn agg_state_tag(&self) -> u64 {
        types_nodes::aggstate_carrier::AGG_STATE_TAG
    }

    fn live_type_name(&self) -> &'static str {
        types_nodes::aggstate_carrier::live_type_name_of::<Self>()
    }

    fn tag(&self) -> types_nodes::nodes::NodeTag {
        types_nodes::execstate_tags::T_AggState
    }

    fn ps(&self) -> &types_nodes::execnodes::PlanStateData<'mcx> {
        &self.ss.ps
    }

    fn ps_mut(&mut self) -> &mut types_nodes::execnodes::PlanStateData<'mcx> {
        &mut self.ss.ps
    }

    fn ss(&self) -> &types_nodes::execnodes::ScanStateData<'mcx> {
        &self.ss
    }
}

impl<'mcx> types_nodes::aggstate_carrier::AggStateTagged<'mcx> for AggStateData<'mcx> {
    const TAG: u64 = types_nodes::aggstate_carrier::AGG_STATE_TAG;
}
