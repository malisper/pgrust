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
pub mod execexpr;
pub mod execnodes;
pub mod execstate_tags;
pub mod executor;
pub mod fmgr;
pub mod funcapi;
pub mod instrument;
pub mod jointype;
pub mod nodeforeigncustom;
pub mod nodehashjoin;
pub mod nodeindexscan;
pub mod nodelimit;
pub mod nodemergeappend;
pub mod nodemergejoin;
pub mod nodenestloop;
pub mod nodes;
pub mod nodesort;
pub mod nodetablefuncscan;
pub mod nodetidrangescan;
pub mod parsenodes;
pub mod pathnodes;
pub mod portalcmds;
pub mod planstate;
pub mod primnodes;
pub mod queryenvironment;

pub use bitmapset::Bitmapset;
pub use execexpr::SubPlanState;
pub use execnodes::{
    CurrentOfTid, EStateData, EcxtId, ExecProcNodeMtd, ExecRowMark, ExprContext, FetchedCursorParam,
    ExprContextCallbackFunction, ExprContext_CB, Opaque, ParamExecData, PlanStateData,
    ResultRelInfo, RowMarkType, RriId, RunningCursorState, ScanDirection,
    ScanDirectionIsForward, ScanStateData, ScanTidOutcome, SlotId, T_MaterialState,
};
pub use primnodes::CurrentOfExpr;
pub use instrument::Instrumentation;
pub use jointype::{
    Join, JoinStateData, JoinType, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_RIGHT,
    JOIN_RIGHT_ANTI, JOIN_RIGHT_SEMI, JOIN_SEMI,
};
pub use nodemergeappend::{
    BinaryHeap, MergeAppend, MergeAppendStateData, PartitionPruneState, T_MergeAppend,
    T_MergeAppendState,
};
pub use nodelimit::{
    Limit, LimitOption, LimitStateCond, LimitStateData, LIMIT_OPTION_COUNT, LIMIT_OPTION_WITH_TIES,
};
pub use nodemergejoin::{MergeJoin, MergeJoinClauseData, MergeJoinStateData};
pub use nodesort::{
    SharedSortInfo, Sort, SortStateData, Tuplesortstate, TuplesortInstrumentation,
    TuplesortMethod, TuplesortSpaceType, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE,
    TUPLESORT_RANDOMACCESS,
};
pub use nodenestloop::{NestLoop, NestLoopParam, NestLoopStateData};
pub use pathnodes::PathNode;
pub use executor::{TupleSlotKind, TupleTableSlot};
pub use funcapi::Tuplestorestate;
pub use nodeforeigncustom::{Material, MaterialState};
pub use nodetablefuncscan::{
    TableFuncRoutineKind, TableFuncScan, TableFuncScanState, T_TableFuncScanState,
};
pub use primnodes::{TableFunc, TableFuncType, TFT_JSON_TABLE, TFT_XMLTABLE, Expr, TargetEntry, Var};
pub use nodehashjoin::{
    HashJoin, HashJoinState, HashJoinTableData, T_HashJoin, T_HashJoinState,
};
pub use parsenodes::{RTEPermissionInfo, RangeTblEntry};
pub use planstate::PlanStateNode;
pub use queryenvironment::{
    EphemeralNameRelationType, EphemeralNamedRelation, EphemeralNamedRelationData,
    EphemeralNamedRelationMetadata, EphemeralNamedRelationMetadataData, QueryEnvironment,
    ENR_NAMED_TUPLESTORE,
};
