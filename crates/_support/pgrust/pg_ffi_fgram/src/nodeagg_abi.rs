//! `nodeAgg.c` / `nodeAgg.h` ABI vocabulary — faithful `#[repr(C)]` layouts for
//! the aggregate executor node (`backend-executor-nodeAgg`).
//!
//! Like the other ported executor nodes, `AggState` and its private per-* helper
//! structs (`AggStatePerTransData`, `AggStatePerAggData`, `AggStatePerGroupData`,
//! `AggStatePerPhaseData`, `AggStatePerHashData`) cross the crate boundary as
//! address-stable `#[repr(C)]` handles laid out exactly like PostgreSQL 18.3.
//! The node crate keeps its logic idiomatic but navigates these layouts for the
//! fields it reads (`node->ss.ps.<field>`, `pertrans-><field>`, ...).
//!
//! Every pointer-typed field that points at a sibling subsystem
//! (`Tuplesortstate *`, `TupleHashTable`, `ExprState *`, `FunctionCallInfo`, ...)
//! crosses the boundary as a raw pointer (`*mut c_void` or the existing typed
//! alias); only the address travels, and the node code reaches into those
//! subsystems through the crate's runtime seam.

use core::ffi::{c_int, c_void};

use crate::execnodes::ScanStateData;
use crate::fmgr::FmgrInfo;
use crate::heaptuple::{HeapTuple, TupleDesc};
use crate::{AttrNumber, Bitmapset, Datum, ExprContext, ExprState, List, Oid, TupleTableSlot};

// ===========================================================================
// AggStrategy / AggSplit (nodes.h)
// ===========================================================================

/// `AggStrategy` (nodes.h).
pub type AggStrategy = c_int;
/// `AGG_PLAIN` — simple agg across all input rows.
pub const AGG_PLAIN: AggStrategy = 0;
/// `AGG_SORTED` — grouped agg, input must be sorted.
pub const AGG_SORTED: AggStrategy = 1;
/// `AGG_HASHED` — grouped agg, use internal hashtable.
pub const AGG_HASHED: AggStrategy = 2;
/// `AGG_MIXED` — grouped agg, hash and sort both used.
pub const AGG_MIXED: AggStrategy = 3;

/// `AggSplit` (nodes.h).
pub type AggSplit = c_int;
/// `AGGSPLITOP_COMBINE` — substitute combinefn for transfn.
pub const AGGSPLITOP_COMBINE: AggSplit = 0x01;
/// `AGGSPLITOP_SKIPFINAL` — skip finalfn, return state as-is.
pub const AGGSPLITOP_SKIPFINAL: AggSplit = 0x02;
/// `AGGSPLITOP_SERIALIZE` — apply serialfn to output.
pub const AGGSPLITOP_SERIALIZE: AggSplit = 0x04;
/// `AGGSPLITOP_DESERIALIZE` — apply deserialfn to input.
pub const AGGSPLITOP_DESERIALIZE: AggSplit = 0x08;
/// `AGGSPLIT_SIMPLE` — basic, non-split aggregation.
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
/// `AGGSPLIT_INITIAL_SERIAL` — initial phase of partial aggregation.
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
/// `AGGSPLIT_FINAL_DESERIAL` — final phase of partial aggregation.
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

/// `DO_AGGSPLIT_COMBINE(as)` (nodes.h).
#[inline]
pub const fn DO_AGGSPLIT_COMBINE(aggsplit: AggSplit) -> bool {
    (aggsplit & AGGSPLITOP_COMBINE) != 0
}
/// `DO_AGGSPLIT_SKIPFINAL(as)` (nodes.h).
#[inline]
pub const fn DO_AGGSPLIT_SKIPFINAL(aggsplit: AggSplit) -> bool {
    (aggsplit & AGGSPLITOP_SKIPFINAL) != 0
}
/// `DO_AGGSPLIT_SERIALIZE(as)` (nodes.h).
#[inline]
pub const fn DO_AGGSPLIT_SERIALIZE(aggsplit: AggSplit) -> bool {
    (aggsplit & AGGSPLITOP_SERIALIZE) != 0
}
/// `DO_AGGSPLIT_DESERIALIZE(as)` (nodes.h).
#[inline]
pub const fn DO_AGGSPLIT_DESERIALIZE(aggsplit: AggSplit) -> bool {
    (aggsplit & AGGSPLITOP_DESERIALIZE) != 0
}

// ===========================================================================
// AGG_CONTEXT codes (fmgr.h) — returned by AggCheckCallContext.
// ===========================================================================

/// `AGG_CONTEXT_AGGREGATE` — regular aggregate.
pub const AGG_CONTEXT_AGGREGATE: c_int = 1;
/// `AGG_CONTEXT_WINDOW` — window function.
pub const AGG_CONTEXT_WINDOW: c_int = 2;

// ===========================================================================
// Opaque sibling-subsystem handles used by the per-* structs.
// ===========================================================================

/// `Tuplesortstate *` — opaque tuplesort handle (`utils/tuplesort.h`).
pub type Tuplesortstate = c_void;
/// `TupleHashTable` — opaque execGrouping hash-table handle
/// (`TupleHashTableData *`).
pub type TupleHashTable = *mut c_void;
/// `LogicalTapeSet *` — opaque logical-tape-set handle (`utils/logtape.h`).
pub type LogicalTapeSet = c_void;
/// `LogicalTape *` — opaque logical-tape handle (`utils/logtape.h`).
pub type LogicalTape = c_void;
/// `FunctionCallInfo` — pointer to a `FunctionCallInfoBaseData` (`fmgr.h`).
pub type FunctionCallInfo = *mut c_void;
/// `hyperLogLogState *` — opaque HLL cardinality estimator (`lib/hyperloglog.h`).
pub type HyperLogLogState = c_void;
/// `Aggref *` — opaque aggregate reference plan node (`primnodes.h`).
pub type Aggref = c_void;
/// `Agg *` — opaque aggregate plan node (`plannodes.h`).
pub type Agg = c_void;
/// `Sort *` — opaque sort plan node (`plannodes.h`).
pub type Sort = c_void;
/// `SharedAggInfo *` — opaque parallel shared-instrumentation block
/// (`execnodes.h`).
pub type SharedAggInfo = c_void;

/// `TupleHashIterator` — `tuplehash_iterator` cursor (`execGrouping`); its
/// concrete layout is `{ uint32 cur; uint32 end; bool done; }`. Crossing the
/// boundary it is a fixed-size, address-stable POD struct embedded by value in
/// `AggStatePerHashData`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct TupleHashIterator {
    /// `uint32 cur` — current iteration index.
    pub cur: u32,
    /// `uint32 end` — end of iteration range.
    pub end: u32,
    /// `bool done` — iteration finished.
    pub done: bool,
}

// ===========================================================================
// AggStatePerGroupData (nodeAgg.h)
// ===========================================================================

/// `AggStatePerGroupData` (nodeAgg.h) — per-aggregate-per-group working state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggStatePerGroupData {
    /// `Datum transValue` — current transition value.
    pub transValue: Datum,
    /// `bool transValueIsNull`.
    pub transValueIsNull: bool,
    /// `bool noTransValue` — true if `transValue` not set yet.
    pub noTransValue: bool,
}

/// `AggStatePerGroup` (nodeAgg.h) — pointer to an array of per-group structs.
pub type AggStatePerGroup = *mut AggStatePerGroupData;

// ===========================================================================
// AggStatePerTransData (nodeAgg.h)
// ===========================================================================

/// `AggStatePerTransData` (nodeAgg.h) — per-aggregate transition state info.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggStatePerTransData {
    /// `Aggref *aggref`.
    pub aggref: *mut Aggref,
    /// `bool aggshared`.
    pub aggshared: bool,
    /// `bool aggsortrequired`.
    pub aggsortrequired: bool,
    /// `int numInputs`.
    pub numInputs: c_int,
    /// `int numTransInputs`.
    pub numTransInputs: c_int,
    /// `Oid transfn_oid`.
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
    pub aggCollation: Oid,
    /// `int numSortCols`.
    pub numSortCols: c_int,
    /// `int numDistinctCols`.
    pub numDistinctCols: c_int,
    /// `AttrNumber *sortColIdx`.
    pub sortColIdx: *mut AttrNumber,
    /// `Oid *sortOperators`.
    pub sortOperators: *mut Oid,
    /// `Oid *sortCollations`.
    pub sortCollations: *mut Oid,
    /// `bool *sortNullsFirst`.
    pub sortNullsFirst: *mut bool,
    /// `FmgrInfo equalfnOne`.
    pub equalfnOne: FmgrInfo,
    /// `ExprState *equalfnMulti`.
    pub equalfnMulti: *mut ExprState,
    /// `Datum initValue`.
    pub initValue: Datum,
    /// `bool initValueIsNull`.
    pub initValueIsNull: bool,
    /// `int16 inputtypeLen`.
    pub inputtypeLen: i16,
    /// `int16 transtypeLen`.
    pub transtypeLen: i16,
    /// `bool inputtypeByVal`.
    pub inputtypeByVal: bool,
    /// `bool transtypeByVal`.
    pub transtypeByVal: bool,
    /// `TupleTableSlot *sortslot`.
    pub sortslot: *mut TupleTableSlot,
    /// `TupleTableSlot *uniqslot`.
    pub uniqslot: *mut TupleTableSlot,
    /// `TupleDesc sortdesc`.
    pub sortdesc: TupleDesc,
    /// `Datum lastdatum`.
    pub lastdatum: Datum,
    /// `bool lastisnull`.
    pub lastisnull: bool,
    /// `bool haslast`.
    pub haslast: bool,
    /// `Tuplesortstate **sortstates`.
    pub sortstates: *mut *mut Tuplesortstate,
    /// `FunctionCallInfo transfn_fcinfo`.
    pub transfn_fcinfo: FunctionCallInfo,
    /// `FunctionCallInfo serialfn_fcinfo`.
    pub serialfn_fcinfo: FunctionCallInfo,
    /// `FunctionCallInfo deserialfn_fcinfo`.
    pub deserialfn_fcinfo: FunctionCallInfo,
}

/// `AggStatePerTrans` (nodeAgg.h) — pointer to a per-trans struct.
pub type AggStatePerTrans = *mut AggStatePerTransData;

// ===========================================================================
// AggStatePerAggData (nodeAgg.h)
// ===========================================================================

/// `AggStatePerAggData` (nodeAgg.h) — per-aggregate (final-function) info.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggStatePerAggData {
    /// `Aggref *aggref`.
    pub aggref: *mut Aggref,
    /// `int transno`.
    pub transno: c_int,
    /// `Oid finalfn_oid`.
    pub finalfn_oid: Oid,
    /// `FmgrInfo finalfn`.
    pub finalfn: FmgrInfo,
    /// `int numFinalArgs`.
    pub numFinalArgs: c_int,
    /// `List *aggdirectargs`.
    pub aggdirectargs: *mut List,
    /// `int16 resulttypeLen`.
    pub resulttypeLen: i16,
    /// `bool resulttypeByVal`.
    pub resulttypeByVal: bool,
    /// `bool shareable`.
    pub shareable: bool,
}

/// `AggStatePerAgg` (nodeAgg.h) — pointer to a per-agg struct.
pub type AggStatePerAgg = *mut AggStatePerAggData;

// ===========================================================================
// AggStatePerPhaseData (nodeAgg.h)
// ===========================================================================

/// `AggStatePerPhaseData` (nodeAgg.h) — per-grouping-set-phase state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggStatePerPhaseData {
    /// `AggStrategy aggstrategy`.
    pub aggstrategy: AggStrategy,
    /// `int numsets`.
    pub numsets: c_int,
    /// `int *gset_lengths`.
    pub gset_lengths: *mut c_int,
    /// `Bitmapset **grouped_cols`.
    pub grouped_cols: *mut *mut Bitmapset,
    /// `ExprState **eqfunctions`.
    pub eqfunctions: *mut *mut ExprState,
    /// `Agg *aggnode`.
    pub aggnode: *mut Agg,
    /// `Sort *sortnode`.
    pub sortnode: *mut Sort,
    /// `ExprState *evaltrans`.
    pub evaltrans: *mut ExprState,
    /// `ExprState *evaltrans_cache[2][2]`.
    pub evaltrans_cache: [[*mut ExprState; 2]; 2],
}

/// `AggStatePerPhase` (nodeAgg.h) — pointer to a per-phase struct.
pub type AggStatePerPhase = *mut AggStatePerPhaseData;

// ===========================================================================
// AggStatePerHashData (nodeAgg.h)
// ===========================================================================

/// `AggStatePerHashData` (nodeAgg.h) — per-hashtable state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggStatePerHashData {
    /// `TupleHashTable hashtable`.
    pub hashtable: TupleHashTable,
    /// `TupleHashIterator hashiter`.
    pub hashiter: TupleHashIterator,
    /// `TupleTableSlot *hashslot`.
    pub hashslot: *mut TupleTableSlot,
    /// `FmgrInfo *hashfunctions`.
    pub hashfunctions: *mut FmgrInfo,
    /// `Oid *eqfuncoids`.
    pub eqfuncoids: *mut Oid,
    /// `int numCols`.
    pub numCols: c_int,
    /// `int numhashGrpCols`.
    pub numhashGrpCols: c_int,
    /// `int largestGrpColIdx`.
    pub largestGrpColIdx: c_int,
    /// `AttrNumber *hashGrpColIdxInput`.
    pub hashGrpColIdxInput: *mut AttrNumber,
    /// `AttrNumber *hashGrpColIdxHash`.
    pub hashGrpColIdxHash: *mut AttrNumber,
    /// `Agg *aggnode`.
    pub aggnode: *mut Agg,
}

/// `AggStatePerHash` (nodeAgg.h) — pointer to a per-hash struct.
pub type AggStatePerHash = *mut AggStatePerHashData;

// ===========================================================================
// AggState (execnodes.h)
// ===========================================================================

/// `AggState` (execnodes.h) — faithful `#[repr(C)]` ABI for the aggregate
/// executor node (`nodeAgg.c`). The leading [`ScanStateData`] head's first
/// member is a `NodeTag`, so a `*mut AggStateData` is also a valid `Node *` and
/// a valid `*mut AggState` opaque public handle.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggStateData {
    /// `ScanState ss` — its first field is a `NodeTag`.
    pub ss: ScanStateData,
    /// `List *aggs` — all Aggref nodes in targetlist & quals.
    pub aggs: *mut List,
    /// `int numaggs`.
    pub numaggs: c_int,
    /// `int numtrans`.
    pub numtrans: c_int,
    /// `AggStrategy aggstrategy`.
    pub aggstrategy: AggStrategy,
    /// `AggSplit aggsplit`.
    pub aggsplit: AggSplit,
    /// `AggStatePerPhase phase`.
    pub phase: AggStatePerPhase,
    /// `int numphases`.
    pub numphases: c_int,
    /// `int current_phase`.
    pub current_phase: c_int,
    /// `AggStatePerAgg peragg`.
    pub peragg: AggStatePerAgg,
    /// `AggStatePerTrans pertrans`.
    pub pertrans: AggStatePerTrans,
    /// `ExprContext *hashcontext`.
    pub hashcontext: *mut ExprContext,
    /// `ExprContext **aggcontexts`.
    pub aggcontexts: *mut *mut ExprContext,
    /// `ExprContext *tmpcontext`.
    pub tmpcontext: *mut ExprContext,
    /// `ExprContext *curaggcontext`.
    pub curaggcontext: *mut ExprContext,
    /// `AggStatePerAgg curperagg`.
    pub curperagg: AggStatePerAgg,
    /// `AggStatePerTrans curpertrans`.
    pub curpertrans: AggStatePerTrans,
    /// `bool input_done`.
    pub input_done: bool,
    /// `bool agg_done`.
    pub agg_done: bool,
    /// `int projected_set`.
    pub projected_set: c_int,
    /// `int current_set`.
    pub current_set: c_int,
    /// `Bitmapset *grouped_cols`.
    pub grouped_cols: *mut Bitmapset,
    /// `List *all_grouped_cols`.
    pub all_grouped_cols: *mut List,
    /// `Bitmapset *colnos_needed`.
    pub colnos_needed: *mut Bitmapset,
    /// `int max_colno_needed`.
    pub max_colno_needed: c_int,
    /// `bool all_cols_needed`.
    pub all_cols_needed: bool,
    /// `int maxsets`.
    pub maxsets: c_int,
    /// `AggStatePerPhase phases`.
    pub phases: AggStatePerPhase,
    /// `Tuplesortstate *sort_in`.
    pub sort_in: *mut Tuplesortstate,
    /// `Tuplesortstate *sort_out`.
    pub sort_out: *mut Tuplesortstate,
    /// `TupleTableSlot *sort_slot`.
    pub sort_slot: *mut TupleTableSlot,
    /// `AggStatePerGroup *pergroups`.
    pub pergroups: *mut AggStatePerGroup,
    /// `HeapTuple grp_firstTuple`.
    pub grp_firstTuple: HeapTuple,
    /// `bool table_filled`.
    pub table_filled: bool,
    /// `int num_hashes`.
    pub num_hashes: c_int,
    /// `MemoryContext hash_metacxt`.
    pub hash_metacxt: *mut c_void,
    /// `MemoryContext hash_tablecxt`.
    pub hash_tablecxt: *mut c_void,
    /// `struct LogicalTapeSet *hash_tapeset`.
    pub hash_tapeset: *mut LogicalTapeSet,
    /// `struct HashAggSpill *hash_spills`.
    pub hash_spills: *mut HashAggSpill,
    /// `TupleTableSlot *hash_spill_rslot`.
    pub hash_spill_rslot: *mut TupleTableSlot,
    /// `TupleTableSlot *hash_spill_wslot`.
    pub hash_spill_wslot: *mut TupleTableSlot,
    /// `List *hash_batches`.
    pub hash_batches: *mut List,
    /// `bool hash_ever_spilled`.
    pub hash_ever_spilled: bool,
    /// `bool hash_spill_mode`.
    pub hash_spill_mode: bool,
    /// `Size hash_mem_limit`.
    pub hash_mem_limit: usize,
    /// `uint64 hash_ngroups_limit`.
    pub hash_ngroups_limit: u64,
    /// `int hash_planned_partitions`.
    pub hash_planned_partitions: c_int,
    /// `double hashentrysize`.
    pub hashentrysize: f64,
    /// `Size hash_mem_peak`.
    pub hash_mem_peak: usize,
    /// `uint64 hash_ngroups_current`.
    pub hash_ngroups_current: u64,
    /// `uint64 hash_disk_used`.
    pub hash_disk_used: u64,
    /// `int hash_batches_used`.
    pub hash_batches_used: c_int,
    /// `AggStatePerHash perhash`.
    pub perhash: AggStatePerHash,
    /// `AggStatePerGroup *hash_pergroup`.
    pub hash_pergroup: *mut AggStatePerGroup,
    /// `AggStatePerGroup *all_pergroups`.
    pub all_pergroups: *mut AggStatePerGroup,
    /// `SharedAggInfo *shared_info`.
    pub shared_info: *mut SharedAggInfo,
}

// ===========================================================================
// Private nodeAgg.c spill structs (HashAggSpill / HashAggBatch).
// ===========================================================================

/// `HashAggSpill` (private in nodeAgg.c) — partitioned spill data for one
/// hashtable.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HashAggSpill {
    /// `int npartitions`.
    pub npartitions: c_int,
    /// `LogicalTape **partitions`.
    pub partitions: *mut *mut LogicalTape,
    /// `int64 *ntuples`.
    pub ntuples: *mut i64,
    /// `uint32 mask`.
    pub mask: u32,
    /// `int shift`.
    pub shift: c_int,
    /// `hyperLogLogState *hll_card`.
    pub hll_card: *mut HyperLogLogState,
}

/// `HashAggBatch` (private in nodeAgg.c) — work for one pass of hash agg.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HashAggBatch {
    /// `int setno` — grouping set.
    pub setno: c_int,
    /// `int used_bits` — number of bits of hash already used.
    pub used_bits: c_int,
    /// `LogicalTape *input_tape`.
    pub input_tape: *mut LogicalTape,
    /// `int64 input_tuples`.
    pub input_tuples: i64,
    /// `double input_card`.
    pub input_card: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, size_of};

    #[test]
    fn nodeagg_abi_layout_matches_postgres_on_64_bit() {
        // AggStatePerGroupData: Datum (8) + 2 bools, 8-aligned -> 16 bytes.
        assert_eq!(size_of::<AggStatePerGroupData>(), 16);
        assert_eq!(align_of::<AggStatePerGroupData>(), 8);

        // TupleHashIterator: two u32 + bool -> 12 bytes, 4-aligned.
        assert_eq!(size_of::<TupleHashIterator>(), 12);
        assert_eq!(align_of::<TupleHashIterator>(), 4);

        // All the larger structs are 8-aligned (pointer-bearing).
        assert_eq!(align_of::<AggStateData>(), 8);
        assert_eq!(align_of::<AggStatePerTransData>(), 8);
        assert_eq!(align_of::<AggStatePerAggData>(), 8);
        assert_eq!(align_of::<AggStatePerPhaseData>(), 8);
        assert_eq!(align_of::<AggStatePerHashData>(), 8);
        assert_eq!(align_of::<HashAggSpill>(), 8);
        assert_eq!(align_of::<HashAggBatch>(), 8);

        // A *mut AggStateData must be usable as a Node* (NodeTag first).
        assert_eq!(core::mem::offset_of!(AggStateData, ss), 0);
    }
}
