//! The designated node/executor knot crate (docs/types.md rule 4): plan-node,
//! plan-state, slot, and tuplestore-carrier vocabulary shared by the executor
//! node crates.
//!
//! The 49-module node/executor tangle in src-idiomatic's types crate is
//! irreducible, so its modules land here — but each is trimmed to only the
//! items the ports so far consume. Module names follow src-idiomatic.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod bitmapset;
pub mod copy_query;
pub mod execexpr;
pub mod execnodes;
pub mod execstate_tags;
pub mod executor;
pub mod fmgr;
pub mod funcapi;
pub mod instrument;
pub mod jointype;
pub mod list;
pub mod modifytable;
pub mod nodeagg;
pub mod nodeappend;
pub mod nodectescan;
pub mod nodeforeigncustom;
pub mod nodegroup;
pub mod nodehash;
pub mod nodehashjoin;
pub mod nodeindexonlyscan;
pub mod nodeindexscan;
pub mod nodelimit;
pub mod nodememoize;
pub mod nodemergeappend;
pub mod nodemergejoin;
pub mod nodeprojectset;
pub mod noderesult;
pub mod nodenestloop;
pub mod nodesetop;
pub mod nodes;
pub mod nodesamplescan;
pub mod nodeseqscan;
pub mod nodesort;
pub mod nodetablefuncscan;
pub mod nodetidrangescan;
pub mod nodeunique;
pub mod nodevaluesscan;
pub mod nodeworktablescan;
pub mod params;
pub mod parsenodes;
pub mod parsestmt;
pub mod partition;
pub mod pathnodes;
pub mod portalcmds;
pub mod planstate;
pub mod primnodes;
pub mod queryenvironment;
pub mod saophash;
pub mod tuptable;

pub use bitmapset::Bitmapset;
pub use execexpr::SubPlanState;
pub use execnodes::{
    CurrentOfTid, EPQState, EStateData, EcxtId, ExecProcNodeMtd, ExecRowMark, ExprContext,
    ExprContextCallbackFunction, ExprContext_CB, FetchedCursorParam, JunkFilter, Opaque,
    ParamExecData,
    PlanStateData, ResultRelInfo, RowMarkType, RriId, RunningCursorState, ScanDirection,
    ScanDirectionIsForward, ScanStateData, ScanTidOutcome, SlotId, SubqueryScanState,
    T_MaterialState,
};
pub use primnodes::CurrentOfExpr;
pub use modifytable::{
    EPQState as ModifyTableEPQState, MergeAction, MergeActionState, MergeMatchKind, ModifyTable,
    ModifyTableState, OnConflictSetState, OverridingKind, PartitionTupleRouting, ResultRelHash,
    TransitionCaptureState,
};
pub use instrument::Instrumentation;
pub use jointype::{
    Join, JoinStateData, JoinType, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
    JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI,
};
pub use nodemergeappend::{
    BinaryHeap, MergeAppend, MergeAppendStateData, T_MergeAppend,
    T_MergeAppendState,
};
pub use nodelimit::{
    Limit, LimitOption, LimitStateCond, LimitStateData, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES,
};
pub use nodehash::{
    AttStatsSlot, BucketAndBatch, Hash, HashChunkIdx, HashInstrumentation, HashJoinBuckets,
    HashJoinTupleData, HashJoinTupleLink, HashMemoryChunkData,
    HashMemoryChunkLink, HashSkewBucket, HashState, HashTupleIdx, ParallelHashGrowth,
    ParallelHashJoinBatch, ParallelHashJoinBatchAccessor, ParallelHashJoinState, SharedHashInfo,
    INVALID_SKEW_BUCKET_NO,
};
pub use nodeindexonlyscan::{
    IndexOnlyScan, IndexOnlyScanState, IndexRuntimeKeyInfo, IndexScanDesc, IndexScanDescData,
    IndexScanInstrumentation, ParallelIndexScanDesc, ParallelIndexScanDescData, Scan,
    SharedIndexScanInstrumentation, Buffer, InvalidBuffer,
};
pub use nodeappend::{
    Append, AppendChooseStrategy, AppendStateData, AsyncRequestData, ParallelAppendState,
    T_Append, T_AppendState,
};
pub use nodeagg::{
    Agg, AggSplit, AggStateData, AggStatePerAggData, AggStatePerGroupData,
    AggStatePerHashData, AggStatePerPhaseData, AggStatePerTransData, AggStrategy,
    Aggref, AggregateInstrumentation, HashAggBatch, HashAggSpill, SharedAggInfo,
    AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
pub use nodectescan::{CteScan, CteScanState, T_CteScan, T_CteScanState};
pub use nodemergejoin::{MergeJoin, MergeJoinClauseData, MergeJoinStateData};
pub use nodegroup::{Group, GroupStateData, T_Group, T_GroupState};
pub use noderesult::{Result as ResultPlan, ResultState, T_ResultState};
pub use nodesetop::{
    SetOp, SetOpCmd, SetOpStateData, SetOpStatePerGroupData, SetOpStatePerInput, SetOpStrategy,
    SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL,
    SETOP_HASHED, SETOP_SORTED, T_SetOp, T_SetOpState,
};
pub use nodesort::{
    SharedSortInfo, Sort, SortStateData, Tuplesortstate, TuplesortInstrumentation,
    TuplesortMethod, TuplesortSpaceType, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE,
    TUPLESORT_RANDOMACCESS,
};
pub use nodenestloop::{NestLoop, NestLoopParam, NestLoopStateData};
pub use nodeseqscan::{SeqScan, SeqScanState};
pub use nodeindexscan::{SubqueryScan, SubqueryScanStatus};
pub use pathnodes::PathNode;
pub use executor::{TupleSlotKind, TupleTableSlot};
pub use tuptable::{
    AttInMetadata, BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotBase,
    SlotData, TupOutputState, TupleTableSlotOps, VirtualTupleTableSlot,
};
pub use funcapi::Tuplestorestate;
pub use nodeforeigncustom::{
    AsyncRequest, FdwRoutine, ForeignScan, ForeignScanState, Material, MaterialState,
    ParallelContext, ParallelWorkerContext,
};
pub use nodememoize::{
    CacheEntry, CachedTuple, MemoStatus, Memoize, MemoizeCache, MemoizeInstrumentation,
    MemoizeScanState, SharedMemoizeInfo, T_Memoize, T_MemoizeState,
};
pub use nodetablefuncscan::{
    TableFuncRoutineKind, TableFuncScan, TableFuncScanState, T_TableFuncScanState,
};
pub use primnodes::{TableFunc, TableFuncType, TFT_JSON_TABLE, TFT_XMLTABLE, Expr, TargetEntry, Var};
pub use nodehashjoin::{
    HashJoin, HashJoinState, HashJoinTableData, T_HashJoin, T_HashJoinState,
};
pub use parsenodes::{RTEPermissionInfo, RangeTblEntry};
pub use partition::{
    PartitionBoundInfo, PartitionBoundInfoData, PartitionDesc, PartitionDescData, PartitionKey,
    PartitionKeyData, PartitionPruneContext, PartitionPruneState, PartitionPruningData,
    PartitionRangeDatumKind, PartitionStrategy, PartitionedRelPruningData,
};
pub use planstate::PlanStateNode;
pub use queryenvironment::{
    EphemeralNameRelationType, EphemeralNamedRelation, EphemeralNamedRelationData,
    EphemeralNamedRelationMetadata, EphemeralNamedRelationMetadataData, QueryEnvironment,
    ENR_NAMED_TUPLESTORE,
};
