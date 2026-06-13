//! Aggregation node vocabulary (`nodes/plannodes.h`, `nodes/primnodes.h`,
//! `nodes/nodes.h`, `executor/nodeAgg.h`, `executor/execnodes.h`), trimmed to
//! the fields the `nodeAgg.c` port consumes.
//!
//! C aliases used cross-subsystem appear as the established handle/id shapes:
//!
//! - `TupleTableSlot *` → [`SlotId`] into the EState slot pool (as in
//!   `execnodes`);
//! - `ExprContext *` → an owned [`ExprContext`] (or, where C aliases one of
//!   the EState-owned contexts, an index into the owning array);
//! - `Tuplesortstate *` (utils/sort/tuplesort.c) → the real
//!   [`crate::nodesort::Tuplesortstate`] (carried in an owning `PgBox`);
//! - `LogicalTapeSet *` / `LogicalTape *` (utils/sort/logtape.c) →
//!   [`LogicalTapeSetHandle`] / [`LogicalTapeHandle`];
//! - `TupleHashTable` / `TupleHashIterator` / `TupleHashEntry`
//!   (executor/execGrouping.c) → the real [`TupleHashTable`] /
//!   [`TupleHashIterator`] / [`TupleHashEntryData`] structs (execGrouping.c
//!   exposes the full `TupleHashTableData`/`TupleHashEntryData` definitions in
//!   execnodes.h, so these are real structs, not opaque handles).
//!
//! These collapse onto the owners' real types when those units land. `None`
//! collections are the C `NIL`/NULL array.

use mcx::{Mcx, MemoryContext, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_datum::Datum;
use types_error::PgResult;
use types_tuple::heaptuple::{HeapTupleData, MinimalTuple, TupleDescData};

use crate::bitmapset::Bitmapset;
use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, ExprContext, Opaque, ScanStateData, SlotId};
use crate::fmgr::FunctionCallInfoBaseData;
use crate::nodeindexscan::Plan;
use crate::nodesort::{Sort, Tuplesortstate};
use crate::primnodes::{Expr, TargetEntry};

// ---------------------------------------------------------------------------
// AggStrategy / AggSplit (nodes/nodes.h)
// ---------------------------------------------------------------------------

/// `AggStrategy` (nodes/nodes.h).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum AggStrategy {
    /// Simple agg across all input rows.
    #[default]
    AggPlain = 0,
    /// Grouped agg, input must be sorted.
    AggSorted = 1,
    /// Grouped agg, use internal hashtable.
    AggHashed = 2,
    /// Grouped agg, hash and sort both used.
    AggMixed = 3,
}

pub const AGG_PLAIN: AggStrategy = AggStrategy::AggPlain;
pub const AGG_SORTED: AggStrategy = AggStrategy::AggSorted;
pub const AGG_HASHED: AggStrategy = AggStrategy::AggHashed;
pub const AGG_MIXED: AggStrategy = AggStrategy::AggMixed;

/// `AggSplit` (nodes/nodes.h) — a bitmask of the `AGGSPLITOP_*` bits.
pub type AggSplit = i32;

/// `AGGSPLITOP_COMBINE` — substitute combinefn for transfn.
pub const AGGSPLITOP_COMBINE: i32 = 0x01;
/// `AGGSPLITOP_SKIPFINAL` — skip finalfn, return state as-is.
pub const AGGSPLITOP_SKIPFINAL: i32 = 0x02;
/// `AGGSPLITOP_SERIALIZE` — apply serialfn to output.
pub const AGGSPLITOP_SERIALIZE: i32 = 0x04;
/// `AGGSPLITOP_DESERIALIZE` — apply deserialfn to input.
pub const AGGSPLITOP_DESERIALIZE: i32 = 0x08;

/// `AGGSPLIT_SIMPLE` — basic, non-split aggregation.
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
/// `AGGSPLIT_INITIAL_SERIAL` — initial partial phase, with serialization.
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
/// `AGGSPLIT_FINAL_DESERIAL` — final partial phase, with deserialization.
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

/// `DO_AGGSPLIT_COMBINE(as)`.
#[inline]
pub fn do_aggsplit_combine(a: AggSplit) -> bool {
    (a & AGGSPLITOP_COMBINE) != 0
}
/// `DO_AGGSPLIT_SKIPFINAL(as)`.
#[inline]
pub fn do_aggsplit_skipfinal(a: AggSplit) -> bool {
    (a & AGGSPLITOP_SKIPFINAL) != 0
}
/// `DO_AGGSPLIT_SERIALIZE(as)`.
#[inline]
pub fn do_aggsplit_serialize(a: AggSplit) -> bool {
    (a & AGGSPLITOP_SERIALIZE) != 0
}
/// `DO_AGGSPLIT_DESERIALIZE(as)`.
#[inline]
pub fn do_aggsplit_deserialize(a: AggSplit) -> bool {
    (a & AGGSPLITOP_DESERIALIZE) != 0
}

// ---------------------------------------------------------------------------
// Aggref (nodes/primnodes.h), Agg / Sort plan nodes (nodes/plannodes.h)
// ---------------------------------------------------------------------------

/// Trimmed `SortGroupClause` (nodes/parsenodes.h) the agg path reads for its
/// ORDER BY / DISTINCT sort keys.
#[derive(Clone, Copy, Debug, Default)]
pub struct SortGroupClauseAgg {
    /// `Index tleSortGroupRef` — reference into the targetlist.
    pub tle_sort_group_ref: Index,
    /// `Oid eqop` — the equality operator (`pg_operator` OID).
    pub eqop: Oid,
    /// `Oid sortop` — the ordering operator (`pg_operator` OID), or 0.
    pub sortop: Oid,
    /// `bool nulls_first`.
    pub nulls_first: bool,
}

/// `Aggref` (nodes/primnodes.h) — an aggregate-function call in an expression
/// tree, trimmed to the fields the agg executor consumes.
#[derive(Debug, Default)]
pub struct Aggref<'mcx> {
    /// `Oid aggfnoid` — pg_proc OID of the aggregate.
    pub aggfnoid: Oid,
    /// `Oid aggtype` — type of the aggregate's result.
    pub aggtype: Oid,
    /// `Oid aggcollid` — OID of collation of result.
    pub aggcollid: Oid,
    /// `Oid inputcollid` — OID of collation that the function should use.
    pub inputcollid: Oid,
    /// `Oid aggtranstype` — type of aggregate's transition (state) data.
    pub aggtranstype: Oid,
    /// `List *aggargtypes` — type Oids of direct and aggregated args.
    pub aggargtypes: Option<PgVec<'mcx, Oid>>,
    /// `List *aggdirectargs` — direct arguments, if an ordered-set agg.
    pub aggdirectargs: Option<PgVec<'mcx, PgBox<'mcx, Expr>>>,
    /// `List *args` — aggregated arguments and sort expressions (TargetEntry).
    pub args: Option<PgVec<'mcx, PgBox<'mcx, TargetEntry<'mcx>>>>,
    /// `List *aggorder` — ORDER BY (list of SortGroupClause).
    pub aggorder: Option<PgVec<'mcx, SortGroupClauseAgg>>,
    /// `List *aggdistinct` — DISTINCT (list of SortGroupClause).
    pub aggdistinct: Option<PgVec<'mcx, SortGroupClauseAgg>>,
    /// `Expr *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<PgBox<'mcx, Expr>>,
    /// `bool aggstar` — true if argument list was really `*`.
    pub aggstar: bool,
    /// `bool aggvariadic` — true if variadic arguments combined into array.
    pub aggvariadic: bool,
    /// `char aggkind` — aggregate kind (see pg_aggregate.h).
    pub aggkind: i8,
    /// `bool aggpresorted` — input already sorted on order/distinct cols.
    pub aggpresorted: bool,
    /// `Index agglevelsup` — > 0 if agg belongs to outer query.
    pub agglevelsup: Index,
    /// `AggSplit aggsplit` — expected agg-splitting mode of parent Agg.
    pub aggsplit: AggSplit,
    /// `int aggno` — unique ID within the Agg node.
    pub aggno: i32,
    /// `int aggtransno` — unique ID of transition state in the Agg.
    pub aggtransno: i32,
    /// `ParseLoc location` — token location, or -1 if unknown.
    pub location: i32,
}

/// `Agg` plan node (nodes/plannodes.h).
#[derive(Debug, Default)]
pub struct Agg<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `AggStrategy aggstrategy` — basic strategy.
    pub aggstrategy: AggStrategy,
    /// `AggSplit aggsplit` — agg-splitting mode.
    pub aggsplit: AggSplit,
    /// `int numCols` — number of grouping columns.
    pub num_cols: i32,
    /// `AttrNumber *grpColIdx` — their indexes in the target list.
    pub grp_col_idx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *grpOperators` — equality operators to compare with.
    pub grp_operators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *grpCollations`.
    pub grp_collations: Option<PgVec<'mcx, Oid>>,
    /// `long numGroups` — estimated number of groups in input.
    pub num_groups: i64,
    /// `uint64 transitionSpace` — estimated size of the transition state.
    pub transition_space: u64,
    /// `Bitmapset *aggParams` — IDs of Params used in Aggref inputs.
    pub agg_params: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *groupingSets` — grouping sets to use (list of integer lists).
    pub grouping_sets: Option<PgVec<'mcx, PgVec<'mcx, i32>>>,
    /// `List *chain` — chained Agg/Sort nodes.
    pub chain: Option<PgVec<'mcx, PgBox<'mcx, Agg<'mcx>>>>,
}

// The `Sort` plan node (nodes/plannodes.h) is owned by `nodesort`; the agg
// `AggStatePerPhaseData.sortnode` field references that shared type
// ([`crate::nodesort::Sort`]).

// ---------------------------------------------------------------------------
// Sibling-subsystem handles (collapse onto real types when owners land)
// ---------------------------------------------------------------------------

// `Tuplesortstate *` (utils/sort/tuplesort.c) is owned by `nodesort`; the agg
// sort fields hold the real [`crate::nodesort::Tuplesortstate`] (carried in an
// owning `PgBox`, the owned-model shape for a `Tuplesortstate *`).

/// `LogicalTapeSet *` (utils/sort/logtape.c) — opaque spill tape-set handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LogicalTapeSetHandle(pub usize);

/// `LogicalTape *` (utils/sort/logtape.c) — opaque single-tape handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LogicalTapeHandle(pub usize);

/// `TupleHashEntryData` (executor/execnodes.h) — one hash-table entry.
///
/// The real C struct, faithfully mirrored (opacity-inherited: C exposes the
/// full definition in execnodes.h, so this is a real struct, not a handle).
/// The MAXALIGNed "additional" space C carves out immediately before
/// `firstTuple` (`TupleHashEntryGetAdditional`) is owned by the execGrouping
/// hash table's `tablecxt`; the seam surfaces it as a `&mut [u8]` view rather
/// than embedding it here, matching the C pointer-arithmetic layout.
#[derive(Debug, Default)]
pub struct TupleHashEntryData<'mcx> {
    /// `MinimalTuple firstTuple` — copy of first tuple in this group.
    pub firstTuple: MinimalTuple<'mcx>,
    /// `uint32 status` — simplehash slot status.
    pub status: u32,
    /// `uint32 hash` — cached hash value.
    pub hash: u32,
}

/// `TupleHashTableData` (executor/execnodes.h) — the all-in-memory tuple hash
/// table that `execGrouping.c` builds and probes for Agg/SetOp/Subplan/etc.
///
/// The real C struct, faithfully mirrored. `TupleHashTable` in C is
/// `TupleHashTableData *`; in the owned model the table is carried by value
/// (in a `Box`/owning field) and threaded through the execGrouping seams as
/// `&mut TupleHashTable`. The genuinely execGrouping-internal simplehash bucket
/// array (`tuplehash_hash *hashtab`) stays opaque — its concrete shape belongs
/// to the still-unported execGrouping owner — while every field C exposes by
/// type is mirrored concretely.
#[derive(Debug, Default)]
pub struct TupleHashTable<'mcx> {
    /// `tuplehash_hash *hashtab` — the underlying simplehash; execGrouping
    /// owner-internal, so opaque until that unit lands.
    pub hashtab: Opaque,
    /// `int numCols` — number of columns in the lookup key.
    pub numCols: i32,
    /// `AttrNumber *keyColIdx` — attr numbers of key columns.
    pub keyColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `ExprState *tab_hash_expr` — ExprState for hashing table datatype(s).
    pub tab_hash_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *tab_eq_func` — comparator for table datatype(s).
    pub tab_eq_func: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `Oid *tab_collations` — collations for hash and comparison.
    pub tab_collations: Option<PgVec<'mcx, Oid>>,
    /// `MemoryContext tablecxt` — memory context containing the table.
    pub tablecxt: Option<MemoryContext>,
    /// `MemoryContext tempcxt` — context for per-search function evaluations.
    pub tempcxt: Option<MemoryContext>,
    /// `Size additionalsize` — size of the per-entry additional data.
    pub additionalsize: usize,
    /// `TupleTableSlot *tableslot` — slot for referencing table entries (id
    /// into the EState slot pool).
    pub tableslot: Option<SlotId>,
    /// `TupleTableSlot *inputslot` — current input tuple's slot (transient).
    pub inputslot: Option<SlotId>,
    /// `ExprState *in_hash_expr` — ExprState for hashing input datatype(s)
    /// (transient).
    pub in_hash_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *cur_eq_func` — comparator for input vs. table (transient).
    pub cur_eq_func: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprContext *exprcontext` — expression context for the evaluations.
    pub exprcontext: Option<EcxtId>,
}

/// `TupleHashIterator` (executor/execnodes.h) — iteration cursor over a
/// `TupleHashTable`. C is `tuplehash_iterator`; trimmed to the opaque cursor
/// word the iterate seams round-trip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TupleHashIterator {
    /// The opaque `tuplehash_iterator` cursor word.
    pub cur: usize,
}

// ---------------------------------------------------------------------------
// nodeAgg.h per-* state structs
// ---------------------------------------------------------------------------

/// `AggStatePerTransData` (executor/nodeAgg.h) — per-aggregate transition
/// working state.
#[derive(Debug, Default)]
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
    pub init_value: Datum,
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
    pub lastdatum: Datum,
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
#[derive(Clone, Copy, Debug, Default)]
pub struct AggStatePerGroupData {
    /// `Datum transValue` — current transition value (field 0).
    pub trans_value: Datum,
    /// `bool transValueIsNull` (field 1).
    pub trans_value_is_null: bool,
    /// `bool noTransValue` — true if transValue not set yet (field 2).
    pub no_trans_value: bool,
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
    /// `ExprContext *hashcontext`.
    pub hashcontext: Option<PgBox<'mcx, ExprContext<'mcx>>>,
    /// `ExprContext **aggcontexts` — econtexts per grouping set.
    pub aggcontexts: Option<PgVec<'mcx, PgBox<'mcx, ExprContext<'mcx>>>>,
    /// `ExprContext *tmpcontext`.
    pub tmpcontext: Option<PgBox<'mcx, ExprContext<'mcx>>>,
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
    pub pergroups: Option<PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData>>>>,
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
    /// `LogicalTapeSet *hash_tapeset`.
    pub hash_tapeset: Option<LogicalTapeSetHandle>,
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
    pub hash_pergroup: Option<PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData>>>>,
    /// `AggStatePerGroup *all_pergroups` (field 54).
    pub all_pergroups: Option<PgVec<'mcx, Option<PgVec<'mcx, AggStatePerGroupData>>>>,
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
    /// `LogicalTape **partitions` — spill partition tapes.
    pub partitions: Option<PgVec<'mcx, Option<LogicalTapeHandle>>>,
    /// `int64 *ntuples` — number of tuples in each partition.
    pub ntuples: Option<PgVec<'mcx, i64>>,
    /// `uint32 mask` — mask to find partition from hash value.
    pub mask: u32,
    /// `int shift` — after masking, shift down this many bits.
    pub shift: i32,
    /// `hyperLogLogState *hll_card` — cardinality estimator per partition
    /// (`utils/hyperloglog`, not ported) — opaque handle word per partition.
    pub hll_card: Option<PgVec<'mcx, usize>>,
}

/// `HashAggBatch` (nodeAgg.c) — one batch of spilled tuples to refill from.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashAggBatch {
    /// `int setno` — grouping set.
    pub setno: i32,
    /// `int used_bits` — number of bits of hash already used.
    pub used_bits: i32,
    /// `LogicalTape *input_tape` — input partition.
    pub input_tape: Option<LogicalTapeHandle>,
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
