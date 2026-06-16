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
pub mod ddlnodes;
pub mod execexpr;
pub mod execnodes;
pub mod execstate_tags;
/// `executor.h` / `tuptable.h` slot vocabulary (`TupleTableSlot`,
/// `TupleSlotKind`, `TTS_FLAG_*`, `EXEC_FLAG_*`) was relocated into the leaf
/// `types-slot` crate to break the `types-tableam` → `types-nodes` cycle. This
/// re-export preserves every existing `types_nodes::executor::…` and
/// `crate::executor::…` import path unchanged.
pub use types_slot as executor;
pub mod fmgr;
pub mod funcapi;
pub mod instrument;
pub mod jointype;
pub mod list;
pub mod modifytable;
pub mod nodeagg;
pub mod nodeappend;
pub mod nodebitmapindexscan;
pub mod nodectescan;
pub mod nodebitmapand;
pub mod nodebitmapheapscan;
pub mod nodebitmapor;
pub mod nodeforeigncustom;
pub mod nodefunctionscan;
pub mod nodegather;
pub mod nodegathermerge;
pub mod nodegroup;
pub mod nodehash;
pub mod nodehashjoin;
pub mod nodeincrementalsort;
pub mod nodeindexonlyscan;
pub mod nodeindexscan;
pub mod nodelimit;
pub mod nodelockrows;
pub mod nodememoize;
pub mod nodemergeappend;
pub mod nodemergejoin;
pub mod noderecursiveunion;
pub mod nodeprojectset;
pub mod nodenamedtuplestorescan;
pub mod noderesult;
pub mod nodenestloop;
pub mod nodesetop;
pub mod nodes;
/// Central generated leaf-node tree (`Node<'mcx>` + `node_tag`/`copy_node_in`/
/// `equal_node`), built by `build.rs` from `nodetags.h` + `nodes.list`.
pub mod node_tree;
pub mod nodesamplescan;
pub mod nodeseqscan;
pub mod nodesort;
pub mod nodetablefuncscan;
pub mod nodetidrangescan;
pub mod nodetidscan;
pub mod nodeunique;
pub mod nodevaluesscan;
pub mod nodewindowagg;
pub mod nodeworktablescan;
pub mod params;
pub mod parsenodes;
pub mod parsestmt;
/// Full parse-tree producer model + raw-grammar input nodes (the K1-parsetree
/// keystone): the statement-node vocabulary the parser/analyze/rewrite emit and
/// the raw-grammar nodes analyze.c/parse_clause/parse_expr consume.
pub mod rawnodes;
/// Raw-grammar *expression* nodes (the raw-expression node-model keystone): the
/// pre-analysis `Expr`-deriving nodes the grammar builds with raw `Node *`
/// children (BoolExpr/CaseExpr/SubLink/NullTest/…), distinct from the
/// post-analysis [`primnodes::Expr`] enum.
pub mod rawexprnodes;
pub mod partition;
pub mod pathnodes;
pub mod portalcmds;
pub mod planstate;
/// `AggState *` carrier — the owned, tag-checked erased trait object the central
/// [`planstate::PlanStateNode::Agg`] variant holds (its concrete `AggStateData`
/// lives in `backend-executor-nodeAgg`, above this crate). See the module docs.
pub mod aggstate_carrier;
pub mod primnodes;
pub mod querydesc;
pub mod queryenvironment;
pub mod saophash;
pub mod trigger;
pub mod tuptable;
/// Value nodes (`nodes/value.h`): the leaf literal nodes Integer/Float/Boolean/
/// String/BitString, `#[derive(PgNode)]`-enabled and re-homed onto `mcx`.
pub mod value;

pub use bitmapset::Bitmapset;
pub use execexpr::SubPlanState;
pub use execnodes::{
    CurrentOfTid, EPQState, EStateData, EcxtId, ExecProcNodeMtd, ExecRowMark, ExprContext,
    ExecPlanLink, ExprContextCallbackFunction, ExprContext_CB, FetchedCursorParam, JunkFilter,
    Opaque, ParamExecData,
    PlanStateData, ResultRelInfo, RowMarkType, RriId, RunningCursorState, ScanDirection,
    ScanDirectionIsForward, ScanStateData, ScanTidOutcome, SlotId, SubqueryScanState,
    T_MaterialState,
};
pub use primnodes::CurrentOfExpr;
pub use modifytable::{
    EPQState as ModifyTableEPQState, MergeAction, MergeActionState, MergeMatchKind, ModifyTable,
    ModifyTableState, OnConflictSetState, OverridingKind, PartitionDispatchData,
    PartitionDispatchId, PartitionTupleRouting, ResultRelHash, TransitionCaptureState,
};
pub use instrument::Instrumentation;
pub use jointype::{
    Join, JoinStateData, JoinType, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
    JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI,
};
pub use nodegather::{Gather, GatherStateData, T_Gather, T_GatherState};
pub use nodegathermerge::{
    GMReaderTupleBuffer, GatherMerge, GatherMergeStateData, MAX_TUPLE_STORE, T_GatherMerge,
    T_GatherMergeState,
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
    IndexScanInstrumentation, IndexScanState, ParallelIndexScanDesc, ParallelIndexScanDescData,
    ReorderTuple, SharedIndexScanInstrumentation, Buffer, InvalidBuffer,
};
pub use nodeappend::{
    Append, AppendChooseStrategy, AppendStateData, AsyncRequestData, ParallelAppendState,
    T_Append, T_AppendState,
};
pub use nodebitmapand::{BitmapAnd, BitmapAndState, T_BitmapAnd, T_BitmapAndState};
pub use nodebitmapor::{BitmapOr, BitmapOrState, T_BitmapOrState};
pub use nodeagg::{
    Agg, AggSplit, AggStrategy, Aggref, AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED,
};
pub use nodectescan::{CteScan, CteScanState, T_CteScan, T_CteScanState};
pub use nodemergejoin::{MergeJoin, MergeJoinClauseData, MergeJoinStateData};
pub use noderecursiveunion::{
    RecursiveUnion, RecursiveUnionStateData, T_RecursiveUnion, T_RecursiveUnionState,
};
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
pub use nodeindexscan::{Scan, SubqueryScan, SubqueryScanStatus};
pub use pathnodes::PathNode;
pub use types_slot::{TupleSlotKind, TupleTableSlot};
pub use tuptable::{
    AttInMetadata, BufferHeapTupleTableSlot, HeapTupleTableSlot, MinimalTupleTableSlot, SlotBase,
    SlotData, TupOutputState, TupleTableSlotOps, VirtualTupleTableSlot,
};
pub use funcapi::Tuplestorestate;
pub use nodeforeigncustom::{
    AsyncRequest, CustomExecMethods, CustomScan, CustomScanState, FdwRoutine, ForeignScan,
    ForeignScanState, Material, MaterialState, ParallelContext, ParallelWorkerContext,
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
