//! Executor node ABI vocabulary shared across the per-node executor crates.
//!
//! This module supplies the boundary-crossing types the executor node layer
//! needs: the `ExecProcNodeMtd`/`ExecEndNodeMtd` function-pointer typedefs, the
//! `NodeTag` values for every plan node and per-node state node, and opaque
//! `#[repr(C)]` handles for each `<Node>State` struct from `execnodes.h`.
//!
//! The per-node `State` structs are large and embed many sibling-subsystem
//! types (sort, hash table, bitmap, tuplestore, ...). Crossing the crate
//! boundary we therefore treat each `<Node>State` as an opaque, address-stable
//! `#[repr(C)]` handle (`_private: [u8; 0]`), exactly like the existing
//! [`crate::executor::ScanState`] placeholder. Each executor node crate keeps
//! its real, idiomatic state internal; only the address (`*mut <Node>State`)
//! travels through the public `ExecInit*/Exec*/ExecEnd*/ExecReScan*` signatures
//! and through the `PlanState.ExecProcNode` function pointer.

use core::ffi::c_void;

use crate::{NodeTag, PlanState, TupleTableSlot};

// `Node` (the `{ NodeTag type; }` node header) is already defined in
// `crate::fmgr` and re-exported; the executor crates use `crate::Node` for the
// `Node *` return type of the `MultiExec*` callbacks.

/// `Plan *` — opaque pointer to a plan-tree node. The concrete plan structs
/// (`SeqScan`, `Agg`, ...) live in `plannodes.h`; the executor node layer only
/// reads them through the expression/relcache seams, so here they cross the
/// boundary as an opaque address.
pub type Plan = c_void;

/// `ExecProcNodeMtd` — the per-node execution callback stored in
/// `PlanState.ExecProcNode` / `ExecProcNodeReal`. The cross-node recursion
/// `ExecProcNode(child)` dispatches through this pointer (installed at init),
/// never through a direct crate call.
pub type ExecProcNodeMtd =
    Option<unsafe extern "C-unwind" fn(pstate: *mut PlanState) -> *mut TupleTableSlot>;

/// `ParallelContext *` — DSM/parallel-query coordination context (parallel.h).
/// Opaque across the boundary; the DSM/parallel-shm seam owns its contents.
pub type ParallelContext = c_void;

/// `ParallelWorkerContext *` — per-worker parallel context (parallel.h).
pub type ParallelWorkerContext = c_void;

/// `AsyncRequest *` — async-append/foreign-scan request record (execnodes.h).
pub type AsyncRequest = c_void;

// ===========================================================================
// NodeTag values (verified against the generated `nodetags.h` order in
// PostgreSQL 18.3). Plan nodes and their matching `*State` nodes.
// ===========================================================================

// --- plan nodes (plannodes.h) ---
pub const T_Result: NodeTag = 331;
pub const T_ProjectSet: NodeTag = 332;
pub const T_ModifyTable: NodeTag = 333;
pub const T_Append: NodeTag = 334;
pub const T_MergeAppend: NodeTag = 335;
pub const T_RecursiveUnion: NodeTag = 336;
pub const T_BitmapAnd: NodeTag = 337;
pub const T_BitmapOr: NodeTag = 338;
pub const T_SeqScan: NodeTag = 339;
pub const T_SampleScan: NodeTag = 340;
pub const T_IndexScan: NodeTag = 341;
pub const T_IndexOnlyScan: NodeTag = 342;
pub const T_BitmapIndexScan: NodeTag = 343;
pub const T_BitmapHeapScan: NodeTag = 344;
pub const T_TidScan: NodeTag = 345;
pub const T_TidRangeScan: NodeTag = 346;
pub const T_SubqueryScan: NodeTag = 347;
pub const T_FunctionScan: NodeTag = 348;
pub const T_ValuesScan: NodeTag = 349;
pub const T_TableFuncScan: NodeTag = 350;
pub const T_CteScan: NodeTag = 351;
pub const T_NamedTuplestoreScan: NodeTag = 352;
pub const T_WorkTableScan: NodeTag = 353;
pub const T_ForeignScan: NodeTag = 354;
pub const T_CustomScan: NodeTag = 355;
pub const T_NestLoop: NodeTag = 356;
pub const T_MergeJoin: NodeTag = 358;
pub const T_HashJoin: NodeTag = 359;
pub const T_Material: NodeTag = 360;
pub const T_Memoize: NodeTag = 361;
pub const T_Sort: NodeTag = 362;
pub const T_IncrementalSort: NodeTag = 363;
pub const T_Group: NodeTag = 364;
pub const T_Agg: NodeTag = 365;
pub const T_WindowAgg: NodeTag = 366;
pub const T_Unique: NodeTag = 367;
pub const T_Gather: NodeTag = 368;
pub const T_GatherMerge: NodeTag = 369;
pub const T_Hash: NodeTag = 370;
pub const T_SetOp: NodeTag = 371;
pub const T_LockRows: NodeTag = 372;
pub const T_Limit: NodeTag = 373;
pub const T_SubPlan: NodeTag = 23;

// --- executor state nodes (execnodes.h) ---
pub const T_ResultState: NodeTag = 394;
pub const T_ProjectSetState: NodeTag = 395;
pub const T_ModifyTableState: NodeTag = 396;
pub const T_AppendState: NodeTag = 397;
pub const T_MergeAppendState: NodeTag = 398;
pub const T_RecursiveUnionState: NodeTag = 399;
pub const T_BitmapAndState: NodeTag = 400;
pub const T_BitmapOrState: NodeTag = 401;
pub const T_ScanState: NodeTag = 402;
pub const T_SeqScanState: NodeTag = 403;
pub const T_SampleScanState: NodeTag = 404;
pub const T_IndexScanState: NodeTag = 405;
pub const T_IndexOnlyScanState: NodeTag = 406;
pub const T_BitmapIndexScanState: NodeTag = 407;
pub const T_BitmapHeapScanState: NodeTag = 408;
pub const T_TidScanState: NodeTag = 409;
pub const T_TidRangeScanState: NodeTag = 410;
pub const T_SubqueryScanState: NodeTag = 411;
pub const T_FunctionScanState: NodeTag = 412;
pub const T_ValuesScanState: NodeTag = 413;
pub const T_TableFuncScanState: NodeTag = 414;
pub const T_CteScanState: NodeTag = 415;
pub const T_NamedTuplestoreScanState: NodeTag = 416;
pub const T_WorkTableScanState: NodeTag = 417;
pub const T_ForeignScanState: NodeTag = 418;
pub const T_CustomScanState: NodeTag = 419;
pub const T_JoinState: NodeTag = 420;
pub const T_NestLoopState: NodeTag = 421;
pub const T_MergeJoinState: NodeTag = 422;
pub const T_HashJoinState: NodeTag = 423;
pub const T_MaterialState: NodeTag = 424;
pub const T_MemoizeState: NodeTag = 425;
pub const T_SortState: NodeTag = 426;
pub const T_IncrementalSortState: NodeTag = 427;
pub const T_GroupState: NodeTag = 428;
pub const T_AggState: NodeTag = 429;
pub const T_WindowAggState: NodeTag = 430;
pub const T_UniqueState: NodeTag = 431;
pub const T_GatherState: NodeTag = 432;
pub const T_GatherMergeState: NodeTag = 433;
pub const T_HashState: NodeTag = 434;
pub const T_SetOpState: NodeTag = 435;
pub const T_LockRowsState: NodeTag = 436;
pub const T_LimitState: NodeTag = 437;
pub const T_SubPlanState: NodeTag = 392;
pub const T_EState: NodeTag = 389;
pub const T_ProjectionInfo: NodeTag = 384;

// --- other tags referenced by the executor node layer ---
/// `T_TIDBitmap` — tag of a `TIDBitmap` returned by `MultiExecBitmapIndexScan`
/// (checked by the bitmap-heap-scan node when consuming its child's output).
pub const T_TIDBitmap: NodeTag = 478;

/// `T_WindowObjectData` — tag of a `WindowObjectData` (the window-function API
/// object passed to window functions as `fcinfo->context`).
pub const T_WindowObjectData: NodeTag = 479;
/// `T_WindowFuncExprState` — tag of a `WindowFuncExprState` (the per-WindowFunc
/// expression-state node built by `ExecInitExpr`).
pub const T_WindowFuncExprState: NodeTag = 390;

/// Declares a `#[repr(C)]` opaque handle type for a `<Node>State` struct.
///
/// Mirrors the in-tree `ScanState` placeholder: an address-stable type whose
/// pointer carries the real (crate-internal) state across the ABI boundary.
macro_rules! opaque_state {
    ($($(#[$m:meta])* $name:ident),+ $(,)?) => {
        $(
            $(#[$m])*
            #[repr(C)]
            pub struct $name {
                _private: [u8; 0],
            }
        )+
    };
}

opaque_state! {
    /// `SeqScanState *`
    SeqScanState,
    /// `IndexScanState *`
    IndexScanState,
    /// `IndexOnlyScanState *`
    IndexOnlyScanState,
    /// `BitmapAndState *`
    BitmapAndState,
    /// `BitmapOrState *`
    BitmapOrState,
    /// `TidScanState *`
    TidScanState,
    /// `TidRangeScanState *`
    TidRangeScanState,
    /// `SubqueryScanState *`
    SubqueryScanState,
    /// `FunctionScanState *`
    FunctionScanState,
    /// `ValuesScanState *`
    ValuesScanState,
    /// `CteScanState *`
    CteScanState,
    // `NamedTuplestoreScanState`, `ForeignScanState`, `CustomScanState`, and
    // `MaterialState` now have faithful `#[repr(C)]` layouts in
    // `crate::nodeforeigncustom_abi`; they are no longer opaque handles.
    /// `WorkTableScanState *`
    WorkTableScanState,
    /// `TableFuncScanState *`
    TableFuncScanState,
    /// `NestLoopState *`
    NestLoopState,
    /// `MergeJoinState *`
    MergeJoinState,
    /// `HashJoinState *`
    HashJoinState,
    /// `MemoizeState *`
    MemoizeState,
    /// `SortState *`
    SortState,
    /// `IncrementalSortState *`
    IncrementalSortState,
    /// `UniqueState *`
    UniqueState,
    /// `GroupState *`
    GroupState,
    /// `AggState *`
    AggState,
    /// `ResultState *`
    ResultState,
    /// `ProjectSetState *`
    ProjectSetState,
    /// `AppendState *`
    AppendState,
    /// `MergeAppendState *`
    MergeAppendState,
    /// `RecursiveUnionState *`
    RecursiveUnionState,
    /// `LimitState *`
    LimitState,
    /// `LockRowsState *`
    LockRowsState,
    /// `ModifyTableState *`
    ModifyTableState,
    /// `SubPlanState *`
    SubPlanState,
    /// `GatherState *`
    GatherState,
    /// `GatherMergeState *`
    GatherMergeState,
    /// `JoinState *` — common base of the join state nodes.
    JoinState,
}

// `HashJoinTableData` / `HashJoinTable` now have a faithful `#[repr(C)]` layout
// in `nodehash_abi` (so the ported `nodeHash.c` can field-access the hash
// table); see `crate::nodehash_abi`.

// ===========================================================================
// `BitmapIndexScanState` — faithful `#[repr(C)]` ABI for `nodeBitmapIndexscan.c`.
//
// Unlike the opaque `[u8; 0]` handles above, the bitmap-index-scan node has
// been ported in-crate (`backend-executor-nodeBitmapIndexscan`), so its state
// node is a complete, address-stable `#[repr(C)]` struct laid out exactly like
// the C `BitmapIndexScanState` (execnodes.h). The embedded `PlanState` and
// `ScanState` heads are spelled out here field-for-field; the node code reaches
// the fields it needs through the accessor helpers below (mirroring
// `node->ss.ps.<field>` / `node->ss.<field>` in C).
//
// The embedded executor `PlanState`/`ScanState`/`Scan`/`BitmapIndexScan` layouts
// match PostgreSQL 18.3.  Pointer-typed fields cross the boundary as raw
// pointers (`*mut`), opaque to this crate; only the address travels.
// ===========================================================================

use core::ffi::c_int;

use crate::access::IndexScanDesc;
use crate::funcapi::ExprContext;
use crate::heaptuple::TupleDesc;
use crate::instrument::WorkerInstrumentation;
use crate::scankey::ScanKeyData;
use crate::{
    Bitmapset, ExprState, Index, Instrumentation, List, Oid, Relation, SharedJitInstrumentation,
};

/// Faithful `#[repr(C)]` `PlanState` head (execnodes.h) as embedded inside a
/// scan state. This is distinct from the JIT-oriented [`crate::PlanState`]
/// (which models only the offset-critical prefix): here every field is present
/// so the executor node layer can navigate the whole struct.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PlanStateData {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `Plan *plan`
    pub plan: *mut Plan,
    /// `EState *state`
    pub state: *mut crate::EState,
    /// `ExecProcNodeMtd ExecProcNode`
    pub ExecProcNode: ExecProcNodeMtd,
    /// `ExecProcNodeMtd ExecProcNodeReal`
    pub ExecProcNodeReal: ExecProcNodeMtd,
    /// `Instrumentation *instrument`
    pub instrument: *mut Instrumentation,
    /// `WorkerInstrumentation *worker_instrument`
    pub worker_instrument: *mut WorkerInstrumentation,
    /// `struct SharedJitInstrumentation *worker_jit_instrument`
    pub worker_jit_instrument: *mut SharedJitInstrumentation,
    /// `ExprState *qual`
    pub qual: *mut ExprState,
    /// `struct PlanState *lefttree`
    pub lefttree: *mut PlanState,
    /// `struct PlanState *righttree`
    pub righttree: *mut PlanState,
    /// `List *initPlan`
    pub initPlan: *mut List,
    /// `List *subPlan`
    pub subPlan: *mut List,
    /// `Bitmapset *chgParam`
    pub chgParam: *mut Bitmapset,
    /// `TupleDesc ps_ResultTupleDesc`
    pub ps_ResultTupleDesc: TupleDesc,
    /// `TupleTableSlot *ps_ResultTupleSlot`
    pub ps_ResultTupleSlot: *mut TupleTableSlot,
    /// `ExprContext *ps_ExprContext`
    pub ps_ExprContext: *mut ExprContext,
    /// `ProjectionInfo *ps_ProjInfo`
    pub ps_ProjInfo: *mut c_void,
    /// `bool async_capable`
    pub async_capable: bool,
    /// `TupleDesc scandesc`
    pub scandesc: TupleDesc,
    /// `const TupleTableSlotOps *scanops`
    pub scanops: *const c_void,
    /// `const TupleTableSlotOps *outerops`
    pub outerops: *const c_void,
    /// `const TupleTableSlotOps *innerops`
    pub innerops: *const c_void,
    /// `const TupleTableSlotOps *resultops`
    pub resultops: *const c_void,
    /// `bool scanopsfixed`
    pub scanopsfixed: bool,
    /// `bool outeropsfixed`
    pub outeropsfixed: bool,
    /// `bool inneropsfixed`
    pub inneropsfixed: bool,
    /// `bool resultopsfixed`
    pub resultopsfixed: bool,
    /// `bool scanopsset`
    pub scanopsset: bool,
    /// `bool outeropsset`
    pub outeropsset: bool,
    /// `bool inneropsset`
    pub inneropsset: bool,
    /// `bool resultopsset`
    pub resultopsset: bool,
}

/// Faithful `#[repr(C)]` `ScanState` head (execnodes.h). Distinct from the
/// opaque [`crate::executor::ScanState`] placeholder; this is the spelled-out
/// layout used by the ported scan-node state structs.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ScanStateData {
    /// `PlanState ps`
    pub ps: PlanStateData,
    /// `Relation ss_currentRelation`
    pub ss_currentRelation: Relation,
    /// `struct TableScanDescData *ss_currentScanDesc`
    pub ss_currentScanDesc: *mut c_void,
    /// `TupleTableSlot *ss_ScanTupleSlot`
    pub ss_ScanTupleSlot: *mut TupleTableSlot,
}

/// Faithful `#[repr(C)]` `EState` (execnodes.h) — the root of working storage
/// for one Executor invocation. This is the spelled-out counterpart of the
/// JIT-oriented opaque [`crate::EState`] (which models only the offset prefix
/// up to `es_jit_flags`); both have identical layout, so a `*mut EState` may be
/// reinterpreted as a `*mut EStateData` and vice versa.
///
/// Pointer-typed fields whose pointee the executor node layer never navigates
/// (snapshots, planned statement, junk filter, partition directory, query
/// environment, DSA area, JIT context) are kept as opaque `*mut c_void` exactly
/// as their owning subsystems define them; the field offsets are unaffected.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct EStateData {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `ScanDirection es_direction` — current scan direction.
    pub es_direction: c_int,
    /// `Snapshot es_snapshot` — time qual to use.
    pub es_snapshot: *mut c_void,
    /// `Snapshot es_crosscheck_snapshot` — crosscheck time qual for RI.
    pub es_crosscheck_snapshot: *mut c_void,
    /// `List *es_range_table` — `List` of `RangeTblEntry`.
    pub es_range_table: *mut List,
    /// `Index es_range_table_size` — size of the range table arrays.
    pub es_range_table_size: Index,
    /// `Relation *es_relations` — array of per-RTE `Relation` pointers.
    pub es_relations: *mut Relation,
    /// `struct ExecRowMark **es_rowmarks` — array of per-RTE `ExecRowMark`s.
    pub es_rowmarks: *mut *mut c_void,
    /// `List *es_rteperminfos` — `List` of `RTEPermissionInfo`.
    pub es_rteperminfos: *mut List,
    /// `PlannedStmt *es_plannedstmt` — link to top of the plan tree.
    pub es_plannedstmt: *mut c_void,
    /// `List *es_part_prune_infos` — `List` of `PartitionPruneInfo`.
    pub es_part_prune_infos: *mut List,
    /// `List *es_part_prune_states` — `List` of `PartitionPruneState`.
    pub es_part_prune_states: *mut List,
    /// `List *es_part_prune_results` — `List` of `Bitmapset`.
    pub es_part_prune_results: *mut List,
    /// `Bitmapset *es_unpruned_relids` — RT indexes that survive pruning.
    pub es_unpruned_relids: *mut Bitmapset,
    /// `const char *es_sourceText` — source text from the `QueryDesc`.
    pub es_sourceText: *const core::ffi::c_char,
    /// `JunkFilter *es_junkFilter` — top-level junk filter, if any.
    pub es_junkFilter: *mut c_void,
    /// `CommandId es_output_cid` — command id to mark inserted/deleted tuples.
    pub es_output_cid: crate::CommandId,
    /// `ResultRelInfo **es_result_relations` — array of per-RTE
    /// `ResultRelInfo` pointers, or NULL if not a target table.
    pub es_result_relations: *mut *mut c_void,
    /// `List *es_opened_result_relations` — non-NULL `es_result_relations`.
    pub es_opened_result_relations: *mut List,
    /// `PartitionDirectory es_partition_directory` — for `PartitionDesc` lookup.
    pub es_partition_directory: *mut c_void,
    /// `List *es_tuple_routing_result_relations`.
    pub es_tuple_routing_result_relations: *mut List,
    /// `List *es_trig_target_relations` — trigger-only `ResultRelInfo`s.
    pub es_trig_target_relations: *mut List,
    /// `ParamListInfo es_param_list_info` — values of external params.
    pub es_param_list_info: crate::params::ParamListInfo,
    /// `ParamExecData *es_param_exec_vals` — values of internal params.
    pub es_param_exec_vals: *mut c_void,
    /// `QueryEnvironment *es_queryEnv` — query environment.
    pub es_queryEnv: *mut c_void,
    /// `MemoryContext es_query_cxt` — per-query context in which `EState` lives.
    pub es_query_cxt: crate::MemoryContext,
    /// `List *es_tupleTable` — `List` of `TupleTableSlot`s.
    pub es_tupleTable: *mut List,
    /// `uint64 es_processed` — tuples processed during one `ExecutorRun`.
    pub es_processed: crate::uint64,
    /// `uint64 es_total_processed` — tuples aggregated across `ExecutorRun`s.
    pub es_total_processed: crate::uint64,
    /// `int es_top_eflags` — eflags passed to `ExecutorStart`.
    pub es_top_eflags: c_int,
    /// `int es_instrument` — OR of `InstrumentOption` flags.
    pub es_instrument: c_int,
    /// `bool es_finished` — true when `ExecutorFinish` is done.
    pub es_finished: bool,
    /// `List *es_exprcontexts` — `List` of `ExprContext`s within the `EState`.
    pub es_exprcontexts: *mut List,
    /// `List *es_subplanstates` — `List` of `PlanState` for SubPlans.
    pub es_subplanstates: *mut List,
    /// `List *es_auxmodifytables` — `List` of secondary `ModifyTableState`s.
    pub es_auxmodifytables: *mut List,
    /// `ExprContext *es_per_tuple_exprcontext` — per-output-tuple context.
    pub es_per_tuple_exprcontext: *mut ExprContext,
    /// `struct EPQState *es_epq_active` — the active EvalPlanQual state, if any.
    pub es_epq_active: *mut c_void,
    /// `bool es_use_parallel_mode` — can we use parallel workers?
    pub es_use_parallel_mode: bool,
    /// `int es_parallel_workers_to_launch` — number of workers to launch.
    pub es_parallel_workers_to_launch: c_int,
    /// `int es_parallel_workers_launched` — number of workers actually launched.
    pub es_parallel_workers_launched: c_int,
    /// `struct dsa_area *es_query_dsa` — per-query shared memory for parallelism.
    pub es_query_dsa: *mut c_void,
    /// `int es_jit_flags` — whether/how JIT should be performed.
    pub es_jit_flags: c_int,
    /// `struct JitContext *es_jit` — on-demand JIT context.
    pub es_jit: *mut c_void,
    /// `struct JitInstrumentation *es_jit_worker_instr` — combined worker instr.
    pub es_jit_worker_instr: *mut c_void,
    /// `List *es_insert_pending_result_relations`.
    pub es_insert_pending_result_relations: *mut List,
    /// `List *es_insert_pending_modifytables`.
    pub es_insert_pending_modifytables: *mut List,
}

/// `SeqScanState` (execnodes.h) — faithful `#[repr(C)]` ABI for the
/// sequential-scan executor node (`nodeSeqscan.c`).
///
/// ```c
/// typedef struct SeqScanState
/// {
///     ScanState   ss;         /* its first field is NodeTag */
///     Size        pscan_len;  /* size of parallel heap scan descriptor */
/// } SeqScanState;
/// ```
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut SeqScanStateData` is also a valid `Node *` and a valid
/// `*mut SeqScanState` (the opaque public handle). The node crate keeps its
/// logic idiomatic but navigates this address-stable layout for the fields it
/// reads (`node->ss.ps.<field>`, `node->ss.<field>`, `node->pscan_len`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SeqScanStateData {
    /// `ScanState ss` — the common scan-node base.
    pub ss: ScanStateData,
    /// `Size pscan_len` — size of the parallel heap scan descriptor.
    pub pscan_len: usize,
}

/// `IndexScanInstrumentation` (access/genam.h). Plain POD: no pointers, so it can
/// be copied into shared memory during parallel scans.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexScanInstrumentation {
    /// `uint64 nsearches` — index search count.
    pub nsearches: u64,
}

/// `SharedIndexScanInstrumentation` (access/genam.h): every worker's
/// instrumentation, stored in shared memory. `winstrument` is a
/// flexible-array-member, modeled as a zero-length array.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedIndexScanInstrumentation {
    /// `int num_workers`
    pub num_workers: c_int,
    /// `IndexScanInstrumentation winstrument[FLEXIBLE_ARRAY_MEMBER]`
    pub winstrument: [IndexScanInstrumentation; 0],
}

/// `BitmapIndexScan` plan node (plannodes.h). Embeds the abstract `Scan` base.
/// The bitmap-index-scan node reads `indexid`, `isshared`, `indexqual`, and the
/// embedded `scan.scanrelid`.
///
/// The leading `scan` field carries the abstract `Scan` (which itself embeds
/// `Plan`); since this crate never lays out a `BitmapIndexScan` by value (it is
/// always reached through `*mut Plan`), we expose just the `Scan` head plus the
/// bitmap-index-scan-specific fields. The `Plan` prefix inside `scan` is owned by
/// `plannodes`; the node only navigates the typed trailing fields, reached via
/// the accessor helpers, which dereference the node through the plannode seam.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BitmapIndexScan {
    /// `Scan scan` head — represented by its `scanrelid` (the only base-scan
    /// field this node reads), preceded by the opaque embedded `Plan` whose
    /// layout `plannodes` owns. Carried as the `Scan` C struct's first reachable
    /// field below.
    pub scan: Scan,
    /// `Oid indexid` — OID of the index to scan.
    pub indexid: Oid,
    /// `bool isshared` — create a shared bitmap if set.
    pub isshared: bool,
    /// `List *indexqual` — list of index quals (`OpExpr`s).
    pub indexqual: *mut List,
    /// `List *indexqualorig` — the same in original form.
    pub indexqualorig: *mut List,
}

/// `Scan` plan-node head (plannodes.h): the abstract base of all scan plans.
/// Embeds `Plan plan` (owned by plannodes; opaque pointer-sized fields preserved
/// via the leading `Plan` value) followed by `Index scanrelid`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Scan {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `Index scanrelid` — index into the range table.
    pub scanrelid: Index,
}

/// `Plan` plan-node base (plannodes.h). Only the fields the executor node layer
/// reads are spelled out: `type`, costs, row/width estimates, `plan_node_id`,
/// and the target/qual lists. Trailing fields preserved for layout fidelity.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PlanNode {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `int disabled_nodes`
    pub disabled_nodes: c_int,
    /// `Cost startup_cost`
    pub startup_cost: f64,
    /// `Cost total_cost`
    pub total_cost: f64,
    /// `Cardinality plan_rows`
    pub plan_rows: f64,
    /// `int plan_width`
    pub plan_width: c_int,
    /// `bool parallel_aware`
    pub parallel_aware: bool,
    /// `bool parallel_safe`
    pub parallel_safe: bool,
    /// `bool async_capable`
    pub async_capable: bool,
    /// `int plan_node_id`
    pub plan_node_id: c_int,
    /// `List *targetlist`
    pub targetlist: *mut List,
    /// `List *qual`
    pub qual: *mut List,
    /// `struct Plan *lefttree`
    pub lefttree: *mut Plan,
    /// `struct Plan *righttree`
    pub righttree: *mut Plan,
    /// `List *initPlan`
    pub initPlan: *mut List,
    /// `Bitmapset *extParam`
    pub extParam: *mut Bitmapset,
    /// `Bitmapset *allParam`
    pub allParam: *mut Bitmapset,
}

/// `CteScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct CteScan {
///     Scan        scan;
///     int         ctePlanId;  /* ID of init SubPlan for CTE */
///     int         cteParam;   /* ID of Param representing CTE output */
/// } CteScan;
/// ```
///
/// The leading `Scan` embeds `Plan`; the node layer reads the trailing
/// `ctePlanId`/`cteParam` and the embedded `scan.plan.qual`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CteScan {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan,
    /// `int ctePlanId` — ID (1-based) of the init `SubPlan` for the CTE,
    /// indexing `EState.es_subplanstates`.
    pub ctePlanId: c_int,
    /// `int cteParam` — ID of the `Param` representing the CTE output, indexing
    /// `EState.es_param_exec_vals`.
    pub cteParam: c_int,
}

/// `CteScanState` (execnodes.h):
///
/// ```c
/// typedef struct CteScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     int         eflags;         /* capability flags to pass to tuplestore */
///     int         readptr;        /* index of my tuplestore read pointer */
///     PlanState  *cteplanstate;   /* PlanState for the CTE query itself */
///     struct CteScanState *leader; /* Link to the "leader" CteScanState */
///     Tuplestorestate *cte_table; /* rows already read from the CTE query */
///     bool        eof_cte;        /* reached end of CTE query? */
/// } CteScanState;
/// ```
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut CteScanStateData` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CteScanStateData {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `int eflags` — capability flags to pass to the tuplestore.
    pub eflags: c_int,
    /// `int readptr` — index of my tuplestore read pointer.
    pub readptr: c_int,
    /// `PlanState *cteplanstate` — `PlanState` for the CTE query itself.
    pub cteplanstate: *mut PlanStateData,
    /// `struct CteScanState *leader` — link to the "leader" `CteScanState`
    /// (possibly this same node), which holds the shared tuplestore.
    pub leader: *mut CteScanStateData,
    /// `Tuplestorestate *cte_table` — rows already read from the CTE query.
    /// Only valid in the leader.
    pub cte_table: *mut c_void,
    /// `bool eof_cte` — reached end of CTE query? Only valid in the leader.
    pub eof_cte: bool,
}

/// `WorkTableScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct WorkTableScan {
///     Scan        scan;
///     int         wtParam;    /* ID of Param representing work table */
/// } WorkTableScan;
/// ```
///
/// The leading `Scan` embeds `Plan`; the node layer reads the trailing `wtParam`
/// (used to index `EState.es_param_exec_vals` to locate the ancestor
/// `RecursiveUnion`'s state) and the embedded `scan.plan.qual`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WorkTableScan {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan,
    /// `int wtParam` — ID of the `Param` representing the work table, indexing
    /// `EState.es_param_exec_vals`.
    pub wtParam: c_int,
}

/// `WorkTableScanState` (execnodes.h):
///
/// ```c
/// typedef struct WorkTableScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     RecursiveUnionState *rustate;
/// } WorkTableScanState;
/// ```
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut WorkTableScanStateData` is also a valid `Node *` / `PlanState *`. The
/// `rustate` back-link points at the ancestor `RecursiveUnion`'s executor state
/// (found lazily on the first `ExecWorkTableScan` call via the work-table Param).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WorkTableScanStateData {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `RecursiveUnionState *rustate` — the ancestor `RecursiveUnion`'s executor
    /// state, owning the work-table tuplestore. `NULL` until the first
    /// `ExecWorkTableScan` call resolves it.
    pub rustate: *mut RecursiveUnionStateData,
}

/// `RecursiveUnion` plan node (plannodes.h):
///
/// ```c
/// typedef struct RecursiveUnion
/// {
///     Plan        plan;
///     int         wtParam;        /* ID of Param representing work table */
///     /* Remaining fields are zero/null in UNION ALL case */
///     int         numCols;        /* number of columns to check for dup-ness */
///     AttrNumber *dupColIdx;      /* their indexes in the target list */
///     Oid        *dupOperators;   /* equality operators to compare with */
///     Oid        *dupCollations;
///     long        numGroups;      /* estimated number of groups in input */
/// } RecursiveUnion;
/// ```
///
/// The leading [`PlanNode`] is the abstract plan base; the node layer reads the
/// trailing `wtParam`/`numCols`/`dupColIdx`/`dupOperators`/`dupCollations`/
/// `numGroups`. `long` is a 64-bit C `long` on the LP64 platforms this crate
/// targets, so it is modeled as `i64`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RecursiveUnion {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `int wtParam` — ID of the `Param` representing the work table.
    pub wtParam: c_int,
    /// `int numCols` — number of columns to check for duplicate-ness (0 in the
    /// UNION ALL case).
    pub numCols: c_int,
    /// `AttrNumber *dupColIdx` — indexes of the grouping columns in the target
    /// list (`array_size(numCols)`).
    pub dupColIdx: *mut crate::AttrNumber,
    /// `Oid *dupOperators` — equality operators to compare with
    /// (`array_size(numCols)`).
    pub dupOperators: *mut crate::Oid,
    /// `Oid *dupCollations` — collations for the equality comparisons
    /// (`array_size(numCols)`).
    pub dupCollations: *mut crate::Oid,
    /// `long numGroups` — estimated number of groups in the input.
    pub numGroups: i64,
}

/// `RecursiveUnionState` (execnodes.h):
///
/// ```c
/// typedef struct RecursiveUnionState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     bool        recursing;
///     bool        intermediate_empty;
///     Tuplestorestate *working_table;
///     Tuplestorestate *intermediate_table;
///     /* Remaining fields are unused in UNION ALL case */
///     Oid        *eqfuncoids;     /* per-grouping-field equality fns */
///     FmgrInfo   *hashfunctions;  /* per-grouping-field hash fns */
///     MemoryContext tempContext;  /* short-term context for comparisons */
///     TupleHashTable hashtable;   /* hash table for tuples already seen */
///     MemoryContext tableContext; /* memory context containing hash table */
/// } RecursiveUnionState;
/// ```
///
/// The leading [`PlanStateData`] head's first member is a `NodeTag`, so a
/// `*mut RecursiveUnionStateData` is also a valid `Node *` / `PlanState *`. The
/// boundary handle is the opaque [`crate::RecursiveUnionState`]; this is its
/// concrete layout, navigated in-crate.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct RecursiveUnionStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData,
    /// `bool recursing` — are we in the recursive (phase-2) loop yet?
    pub recursing: bool,
    /// `bool intermediate_empty` — nothing stashed in the intermediate table?
    pub intermediate_empty: bool,
    /// `Tuplestorestate *working_table` — the current working table (WT).
    pub working_table: *mut c_void,
    /// `Tuplestorestate *intermediate_table` — accumulates this iteration's rows.
    pub intermediate_table: *mut c_void,
    /// `Oid *eqfuncoids` — per-grouping-field equality functions (UNION only).
    pub eqfuncoids: *mut crate::Oid,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash functions (UNION only).
    pub hashfunctions: *mut crate::FmgrInfo,
    /// `MemoryContext tempContext` — short-term context for comparisons.
    pub tempContext: crate::MemoryContext,
    /// `TupleHashTable hashtable` — hash table for tuples already seen.
    pub hashtable: crate::nodeagg_abi::TupleHashTable,
    /// `MemoryContext tableContext` — memory context containing the hash table.
    pub tableContext: crate::MemoryContext,
}

// ===========================================================================
// `SetOp` — faithful `#[repr(C)]` ABI for `nodeSetOp.c` (INTERSECT / EXCEPT).
//
// The set-op node has been ported in-crate (`backend-executor-nodeSetOp`), so
// its plan node (`SetOp`), per-input working state (`SetOpStatePerInput`), the
// per-group counts struct (`SetOpStatePerGroupData`) and the state node
// (`SetOpState`) are laid out here field-for-field exactly as PostgreSQL 18.3
// produces them (`plannodes.h` / `execnodes.h`), with size/align asserts below.
// ===========================================================================

/// `SetOpCmd` (nodes.h) — what a `SetOp` node does. Canonical definition (with
/// the `SETOPCMD_*` constants) lives in [`crate::pathnodes`]; re-exported here so
/// the executor and planner share one type. `c_uint` and `u32` are ABI-identical.
pub use crate::pathnodes::{
    SetOpCmd, SETOPCMD_EXCEPT, SETOPCMD_EXCEPT_ALL, SETOPCMD_INTERSECT, SETOPCMD_INTERSECT_ALL,
};

/// `SetOpStrategy` (nodes.h) — how a `SetOp` node does it. Canonical definition
/// (with `SETOP_SORTED`/`SETOP_HASHED`) lives in [`crate::pathnodes`].
pub use crate::pathnodes::{SetOpStrategy, SETOP_HASHED, SETOP_SORTED};

/// `SetOp` plan node (plannodes.h):
///
/// ```c
/// typedef struct SetOp {
///     Plan        plan;
///     SetOpCmd    cmd;            /* what to do */
///     SetOpStrategy strategy;     /* how to do it */
///     int         numCols;        /* number of columns to compare */
///     AttrNumber *cmpColIdx;      /* their indexes in the target list */
///     Oid        *cmpOperators;   /* comparison operators (eq or sort ops) */
///     Oid        *cmpCollations;
///     bool       *cmpNullsFirst;  /* nulls-first flags if sorting */
///     long        numGroups;      /* estimated number of groups in left input */
/// } SetOp;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SetOp {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: PlanNode,
    /// `SetOpCmd cmd` — what to do (see [`SetOpCmd`]).
    pub cmd: SetOpCmd,
    /// `SetOpStrategy strategy` — how to do it (see [`SetOpStrategy`]).
    pub strategy: SetOpStrategy,
    /// `int numCols` — number of columns to compare.
    pub numCols: c_int,
    /// `AttrNumber *cmpColIdx` — their indexes in the target list.
    pub cmpColIdx: *mut crate::AttrNumber,
    /// `Oid *cmpOperators` — comparison operators (equality or sort operators).
    pub cmpOperators: *mut crate::Oid,
    /// `Oid *cmpCollations` — collations for the comparisons.
    pub cmpCollations: *mut crate::Oid,
    /// `bool *cmpNullsFirst` — nulls-first flags if sorting, else uninteresting.
    pub cmpNullsFirst: *mut bool,
    /// `long numGroups` — estimated number of groups in the left input.
    pub numGroups: core::ffi::c_long,
}

/// `SetOpStatePerGroupData` (nodeSetOp.c) — per-group working state: how many
/// duplicates of each group arrived from each side.
///
/// ```c
/// typedef struct SetOpStatePerGroupData {
///     int64       numLeft;        /* number of left-input dups in group */
///     int64       numRight;       /* number of right-input dups in group */
/// } SetOpStatePerGroupData;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SetOpStatePerGroupData {
    /// `int64 numLeft` — number of left-input dups in group.
    pub numLeft: crate::int64,
    /// `int64 numRight` — number of right-input dups in group.
    pub numRight: crate::int64,
}

/// `SetOpStatePerInput` (execnodes.h) — per-input working state used in
/// `SETOP_SORTED` mode.
///
/// ```c
/// typedef struct SetOpStatePerInput {
///     TupleTableSlot *firstTupleSlot; /* first tuple of current group */
///     int64       numTuples;          /* number of tuples in current group */
///     TupleTableSlot *nextTupleSlot;  /* next input tuple, if already read */
///     bool        needGroup;          /* do we need to load a new group? */
/// } SetOpStatePerInput;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SetOpStatePerInput {
    /// `TupleTableSlot *firstTupleSlot` — first tuple of current group.
    pub firstTupleSlot: *mut TupleTableSlot,
    /// `int64 numTuples` — number of tuples in current group.
    pub numTuples: crate::int64,
    /// `TupleTableSlot *nextTupleSlot` — next input tuple, if already read.
    pub nextTupleSlot: *mut TupleTableSlot,
    /// `bool needGroup` — do we need to load a new group?
    pub needGroup: bool,
}

/// `SetOpState` (execnodes.h):
///
/// ```c
/// typedef struct SetOpState {
///     PlanState   ps;                 /* its first field is NodeTag */
///     bool        setop_done;         /* indicates completion of output scan */
///     int64       numOutput;          /* number of dups left to output */
///     int         numCols;            /* number of grouping columns */
///     /* SETOP_SORTED mode: */
///     SortSupport sortKeys;           /* per-grouping-field sort data */
///     SetOpStatePerInput leftInput;   /* current outer-relation input state */
///     SetOpStatePerInput rightInput;  /* current inner-relation input state */
///     bool        need_init;          /* have we read the first tuples yet? */
///     /* SETOP_HASHED mode: */
///     Oid        *eqfuncoids;         /* per-grouping-field equality fns */
///     FmgrInfo   *hashfunctions;      /* per-grouping-field hash fns */
///     TupleHashTable hashtable;       /* hash table with one entry per group */
///     MemoryContext tableContext;     /* memory context containing hash table */
///     bool        table_filled;       /* hash table filled yet? */
///     TupleHashIterator hashiter;     /* for iterating through hash table */
/// } SetOpState;
/// ```
///
/// The leading [`PlanStateData`] head's first member is a `NodeTag`, so a
/// `*mut SetOpStateData` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SetOpStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData,
    /// `bool setop_done` — indicates completion of output scan.
    pub setop_done: bool,
    /// `int64 numOutput` — number of dups left to output.
    pub numOutput: crate::int64,
    /// `int numCols` — number of grouping columns.
    pub numCols: c_int,
    /// `SortSupport sortKeys` — per-grouping-field sort data (`SETOP_SORTED`).
    pub sortKeys: *mut crate::SortSupportData,
    /// `SetOpStatePerInput leftInput` — current outer-relation input state.
    pub leftInput: SetOpStatePerInput,
    /// `SetOpStatePerInput rightInput` — current inner-relation input state.
    pub rightInput: SetOpStatePerInput,
    /// `bool need_init` — have we read the first tuples yet?
    pub need_init: bool,
    /// `Oid *eqfuncoids` — per-grouping-field equality fns (`SETOP_HASHED`).
    pub eqfuncoids: *mut crate::Oid,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash fns (`SETOP_HASHED`).
    pub hashfunctions: *mut crate::FmgrInfo,
    /// `TupleHashTable hashtable` — hash table with one entry per group.
    pub hashtable: crate::nodeagg_abi::TupleHashTable,
    /// `MemoryContext tableContext` — memory context containing the hash table.
    pub tableContext: crate::MemoryContext,
    /// `bool table_filled` — hash table filled yet?
    pub table_filled: bool,
    /// `TupleHashIterator hashiter` — for iterating through the hash table.
    pub hashiter: crate::nodeagg_abi::TupleHashIterator,
}

// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut SetOpStateData` can be navigated as the C `SetOpState *`, and a
// `*mut SetOp` as the C `SetOp *`.
const _: () = {
    // SetOpState: PlanState at offset 0 (so `&self` is a valid Node*/PlanState*).
    assert!(core::mem::offset_of!(SetOpStateData, ps) == 0);
    assert!(core::mem::offset_of!(PlanStateData, type_) == 0);
    // numCols follows setop_done (bool) + numOutput (int64, 8-aligned) after ps.
    assert!(
        core::mem::offset_of!(SetOpStateData, setop_done) == core::mem::size_of::<PlanStateData>()
    );
    // SetOp plan: Plan base at offset 0; cmd/strategy are the first trailing ints.
    assert!(core::mem::offset_of!(SetOp, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
    assert!(core::mem::offset_of!(SetOp, cmd) == core::mem::size_of::<PlanNode>());
    // SetOpStatePerGroupData is two int64s, 16 bytes.
    assert!(core::mem::size_of::<SetOpStatePerGroupData>() == 16);
    assert!(core::mem::offset_of!(SetOpStatePerGroupData, numRight) == 8);
};

/// `ResultState` (execnodes.h) — faithful `#[repr(C)]` ABI struct for the
/// `Result` executor node (`nodeResult.c`).
///
/// ```c
/// typedef struct ResultState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     ExprState  *resconstantqual;
///     bool        rs_done;        /* are we done? */
///     bool        rs_checkqual;   /* do we need to check the qual? */
/// } ResultState;
/// ```
///
/// The leading [`PlanStateData`] head's first member is a `NodeTag`, so a
/// `*mut ResultStateData` is also a valid `Node *` / `PlanState *`. The node
/// crate keeps its logic idiomatic but navigates this address-stable layout for
/// the fields it reads (`node->ps.<field>`, `node->resconstantqual`,
/// `node->rs_done`, `node->rs_checkqual`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ResultStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData,
    /// `ExprState *resconstantqual` — the compiled one-time (constant) qual.
    pub resconstantqual: *mut ExprState,
    /// `bool rs_done` — are we done?
    pub rs_done: bool,
    /// `bool rs_checkqual` — do we need to check the constant qual?
    pub rs_checkqual: bool,
}

impl ResultStateData {
    /// `&node->ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ps
    }

    /// `&mut node->ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ps
    }
}

// Layout asserts: `ResultState` = { PlanState ps; ExprState *resconstantqual;
// bool rs_done; bool rs_checkqual; }. `ps` must lead so the address is a valid
// `Node *`/`*mut ResultState`; `resconstantqual` follows the PlanState head and
// the two bools follow the pointer.
const _: () = {
    assert!(core::mem::offset_of!(ResultStateData, ps) == 0);
    assert!(
        core::mem::offset_of!(ResultStateData, resconstantqual)
            == core::mem::size_of::<PlanStateData>()
    );
    assert!(core::mem::align_of::<ResultStateData>() == 8);
};

/// `ParamExecData` (params.h):
///
/// ```c
/// typedef struct ParamExecData {
///     void   *execPlan;   /* should be "SubPlanState *" */
///     Datum   value;
///     bool    isnull;
/// } ParamExecData;
/// ```
///
/// Faithful `#[repr(C)]` view used to navigate an entry of
/// `EState.es_param_exec_vals`. The opaque pointer typedef
/// [`crate::ParamExecData`] (`c_void`) remains the boundary type; this is the
/// laid-out layout for in-crate field access.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ParamExecDataLayout {
    /// `void *execPlan` — should be `NULL` if `value` is valid.
    pub execPlan: *mut c_void,
    /// `Datum value`.
    pub value: crate::Datum,
    /// `bool isnull`.
    pub isnull: bool,
}

/// `BitmapIndexScanState` (execnodes.h) — faithful `#[repr(C)]` ABI struct for
/// the bitmap-index-scan executor node. The leading `ss` field's first member is
/// a `NodeTag`, so a `*mut BitmapIndexScanState` is also a valid `Node *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BitmapIndexScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `TIDBitmap *biss_result`
    pub biss_result: *mut c_void,
    /// `struct ScanKeyData *biss_ScanKeys`
    pub biss_ScanKeys: *mut ScanKeyData,
    /// `int biss_NumScanKeys`
    pub biss_NumScanKeys: c_int,
    /// `IndexRuntimeKeyInfo *biss_RuntimeKeys`
    pub biss_RuntimeKeys: *mut c_void,
    /// `int biss_NumRuntimeKeys`
    pub biss_NumRuntimeKeys: c_int,
    /// `IndexArrayKeyInfo *biss_ArrayKeys`
    pub biss_ArrayKeys: *mut c_void,
    /// `int biss_NumArrayKeys`
    pub biss_NumArrayKeys: c_int,
    /// `bool biss_RuntimeKeysReady`
    pub biss_RuntimeKeysReady: bool,
    /// `ExprContext *biss_RuntimeContext`
    pub biss_RuntimeContext: *mut ExprContext,
    /// `Relation biss_RelationDesc`
    pub biss_RelationDesc: Relation,
    /// `struct IndexScanDescData *biss_ScanDesc`
    pub biss_ScanDesc: IndexScanDesc,
    /// `IndexScanInstrumentation biss_Instrument`
    pub biss_Instrument: IndexScanInstrumentation,
    /// `SharedIndexScanInstrumentation *biss_SharedInfo`
    pub biss_SharedInfo: *mut SharedIndexScanInstrumentation,
}

impl BitmapIndexScanState {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

// Layout asserts: the embedded heads must keep their C offsets so a
// `*mut BitmapIndexScanState` can be navigated as the C struct. `NodeTag` is the
// very first field of `ss.ps`, so the node-tag offset is zero.
const _: () = {
    assert!(core::mem::offset_of!(BitmapIndexScanState, ss) == 0);
    assert!(core::mem::offset_of!(ScanStateData, ps) == 0);
    assert!(core::mem::offset_of!(PlanStateData, type_) == 0);
    assert!(core::mem::offset_of!(Scan, plan) == 0);
    assert!(core::mem::offset_of!(BitmapIndexScan, scan) == 0);
    // POD instrumentation copied into shared memory must be pointer-free and
    // 8-byte sized (single uint64).
    assert!(core::mem::size_of::<IndexScanInstrumentation>() == 8);
    assert!(core::mem::align_of::<IndexScanInstrumentation>() == 8);
    // `SeqScanState` = { ScanState ss; Size pscan_len; }.  `ss` must lead so the
    // address is a valid `Node *`/`*mut SeqScanState`; `pscan_len` follows the
    // ScanState head.
    assert!(core::mem::offset_of!(SeqScanStateData, ss) == 0);
    assert!(
        core::mem::offset_of!(SeqScanStateData, pscan_len) == core::mem::size_of::<ScanStateData>()
    );
    assert!(core::mem::align_of::<SeqScanStateData>() == 8);

    // `CteScan` = { Scan scan; int ctePlanId; int cteParam; }.
    assert!(core::mem::offset_of!(CteScan, scan) == 0);
    assert!(core::mem::offset_of!(CteScan, ctePlanId) == core::mem::size_of::<Scan>());
    assert!(
        core::mem::offset_of!(CteScan, cteParam)
            == core::mem::size_of::<Scan>() + core::mem::size_of::<c_int>()
    );

    // `CteScanState` = { ScanState ss; int eflags; int readptr; PlanState
    // *cteplanstate; CteScanState *leader; Tuplestorestate *cte_table; bool
    // eof_cte; }.  `ss` must lead so the address is a valid `Node *`.
    assert!(core::mem::offset_of!(CteScanStateData, ss) == 0);
    // eflags + readptr (two ints, 8 bytes) directly follow the ScanState head;
    // the trailing pointers are 8-byte aligned so no padding is inserted.
    assert!(
        core::mem::offset_of!(CteScanStateData, eflags) == core::mem::size_of::<ScanStateData>()
    );
    assert!(
        core::mem::offset_of!(CteScanStateData, readptr)
            == core::mem::size_of::<ScanStateData>() + core::mem::size_of::<c_int>()
    );
    assert!(
        core::mem::offset_of!(CteScanStateData, cteplanstate)
            == core::mem::size_of::<ScanStateData>() + 2 * core::mem::size_of::<c_int>()
    );
    assert!(core::mem::align_of::<CteScanStateData>() == 8);

    // `ParamExecData` = { void *execPlan; Datum value; bool isnull; }.
    assert!(core::mem::offset_of!(ParamExecDataLayout, execPlan) == 0);
    assert!(core::mem::offset_of!(ParamExecDataLayout, value) == 8);
    assert!(core::mem::offset_of!(ParamExecDataLayout, isnull) == 16);
    assert!(core::mem::align_of::<ParamExecDataLayout>() == 8);

    // `RecursiveUnion` = { Plan plan; int wtParam; int numCols; AttrNumber
    // *dupColIdx; Oid *dupOperators; Oid *dupCollations; long numGroups; }.
    // (Offsets verified against PostgreSQL 18.3 on LP64: sizeof == 144.)
    assert!(core::mem::offset_of!(RecursiveUnion, plan) == 0);
    assert!(core::mem::offset_of!(RecursiveUnion, wtParam) == 104);
    assert!(core::mem::offset_of!(RecursiveUnion, numCols) == 108);
    assert!(core::mem::offset_of!(RecursiveUnion, dupColIdx) == 112);
    assert!(core::mem::offset_of!(RecursiveUnion, dupOperators) == 120);
    assert!(core::mem::offset_of!(RecursiveUnion, dupCollations) == 128);
    assert!(core::mem::offset_of!(RecursiveUnion, numGroups) == 136);
    assert!(core::mem::size_of::<RecursiveUnion>() == 144);
    assert!(core::mem::align_of::<RecursiveUnion>() == 8);

    // `RecursiveUnionState` = { PlanState ps; bool recursing; bool
    // intermediate_empty; Tuplestorestate *working_table; Tuplestorestate
    // *intermediate_table; Oid *eqfuncoids; FmgrInfo *hashfunctions;
    // MemoryContext tempContext; TupleHashTable hashtable; MemoryContext
    // tableContext; }.  `ps` must lead so the address is a valid `Node *`.
    // (Offsets verified against PostgreSQL 18.3 on LP64: sizeof == 264.)
    assert!(core::mem::offset_of!(RecursiveUnionStateData, ps) == 0);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, recursing) == 200);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, intermediate_empty) == 201);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, working_table) == 208);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, intermediate_table) == 216);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, eqfuncoids) == 224);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, hashfunctions) == 232);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, tempContext) == 240);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, hashtable) == 248);
    assert!(core::mem::offset_of!(RecursiveUnionStateData, tableContext) == 256);
    assert!(core::mem::size_of::<RecursiveUnionStateData>() == 264);
    assert!(core::mem::align_of::<RecursiveUnionStateData>() == 8);
};

// ===========================================================================
// `BitmapHeapScanState` — faithful `#[repr(C)]` ABI for `nodeBitmapHeapscan.c`.
//
// Like `BitmapIndexScanState`, the bitmap-heap-scan node is ported in-crate
// (`backend-executor-nodeBitmapHeapscan`), so its state node is a complete,
// address-stable `#[repr(C)]` struct laid out exactly like the C
// `BitmapHeapScanState` (execnodes.h).  The supporting parallel-state,
// instrumentation, and the `BitmapHeapScan` plan node are spelled out too.
// ===========================================================================

use crate::storage::{proclist_head, slock_t};

/// `dsa_pointer` (utils/dsa.h) — an offset into a `dsa_area`.
pub type dsa_pointer = u64;

/// `InvalidDsaPointer` (utils/dsa.h).
pub const InvalidDsaPointer: dsa_pointer = 0;

/// `ConditionVariable` (storage/condition_variable.h):
/// `{ slock_t mutex; proclist_head wakeup; }`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ConditionVariable {
    /// `slock_t mutex` — protects the wait list.
    pub mutex: slock_t,
    /// `proclist_head wakeup` — list of backends waiting on this CV.
    pub wakeup: proclist_head,
}

/// `BitmapHeapScanInstrumentation` (execnodes.h). Plain POD copied into shared
/// memory during parallel scans.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BitmapHeapScanInstrumentation {
    /// `uint64 exact_pages` — total number of exact pages retrieved.
    pub exact_pages: u64,
    /// `uint64 lossy_pages` — total number of lossy pages retrieved.
    pub lossy_pages: u64,
}

/// `SharedBitmapState` (execnodes.h) — the state of the parallel bitmap scan.
pub type SharedBitmapState = c_int;
/// `BM_INITIAL` — leader has not yet built the TID bitmap.
pub const BM_INITIAL: SharedBitmapState = 0;
/// `BM_INPROGRESS` — leader is building (or has built) the TID bitmap.
pub const BM_INPROGRESS: SharedBitmapState = 1;
/// `BM_FINISHED` — the leader is done building the TID bitmap.
pub const BM_FINISHED: SharedBitmapState = 2;

/// `ParallelBitmapHeapState` (execnodes.h) — the shared state for a parallel
/// bitmap heap scan, allocated in the DSM segment.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ParallelBitmapHeapState {
    /// `dsa_pointer tbmiterator` — iterator for scanning the TID bitmap.
    pub tbmiterator: dsa_pointer,
    /// `slock_t mutex` — mutual exclusion for state machine and iterator.
    pub mutex: slock_t,
    /// `SharedBitmapState state` — current state of the TID bitmap.
    pub state: SharedBitmapState,
    /// `ConditionVariable cv` — used for waiting/wakeup on state changes.
    pub cv: ConditionVariable,
}

/// `SharedBitmapHeapInstrumentation` (execnodes.h) — shared instrumentation for
/// a parallel bitmap heap scan. `sinstrument` is a flexible-array-member.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SharedBitmapHeapInstrumentation {
    /// `int num_workers`
    pub num_workers: c_int,
    /// `BitmapHeapScanInstrumentation sinstrument[FLEXIBLE_ARRAY_MEMBER]`
    pub sinstrument: [BitmapHeapScanInstrumentation; 0],
}

/// `BitmapHeapScan` plan node (plannodes.h). Embeds the abstract `Scan` base
/// (which itself embeds `Plan`) and adds `bitmapqualorig`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BitmapHeapScan {
    /// `Scan scan` head — embeds `Plan` then `Index scanrelid`.
    pub scan: Scan,
    /// `List *bitmapqualorig` — original index quals, for rechecking on lossy
    /// pages.
    pub bitmapqualorig: *mut List,
}

/// `BitmapHeapScanState` (execnodes.h) — faithful `#[repr(C)]` ABI struct for
/// the bitmap-heap-scan executor node. The leading `ss` field's first member is
/// a `NodeTag`, so a `*mut BitmapHeapScanState` is also a valid `Node *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BitmapHeapScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `ExprState *bitmapqualorig`
    pub bitmapqualorig: *mut ExprState,
    /// `TIDBitmap *tbm`
    pub tbm: *mut core::ffi::c_void,
    /// `BitmapHeapScanInstrumentation stats`
    pub stats: BitmapHeapScanInstrumentation,
    /// `bool initialized`
    pub initialized: bool,
    /// `ParallelBitmapHeapState *pstate`
    pub pstate: *mut ParallelBitmapHeapState,
    /// `SharedBitmapHeapInstrumentation *sinstrument`
    pub sinstrument: *mut SharedBitmapHeapInstrumentation,
    /// `bool recheck`
    pub recheck: bool,
}

impl BitmapHeapScanState {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

// Layout asserts: the embedded heads keep their C offsets so a
// `*mut BitmapHeapScanState` can be navigated as the C struct.
const _: () = {
    assert!(core::mem::offset_of!(BitmapHeapScanState, ss) == 0);
    assert!(core::mem::offset_of!(BitmapHeapScan, scan) == 0);
    // POD instrumentation copied into shared memory must be pointer-free and
    // 16-byte sized (two uint64).
    assert!(core::mem::size_of::<BitmapHeapScanInstrumentation>() == 16);
    assert!(core::mem::align_of::<BitmapHeapScanInstrumentation>() == 8);
    // ConditionVariable is { slock_t (i32); proclist_head (8 bytes) } = 12 bytes.
    assert!(core::mem::size_of::<ConditionVariable>() == 12);
};

// ===========================================================================
// `SampleScanState` / `TsmRoutine` — faithful `#[repr(C)]` ABI for
// `nodeSamplescan.c`.
//
// The sample-scan node is ported in-crate (`backend-executor-nodeSamplescan`),
// so its state node is a complete, address-stable `#[repr(C)]` struct laid out
// exactly like the C `SampleScanState` (execnodes.h). `TsmRoutine` (the
// tablesample-method descriptor returned by `GetTsmRoutine`, tsmapi.h) and the
// `SampleScan` plan node / `TableSampleClause` it reads are spelled out too.
// ===========================================================================

use crate::executor::Expr;
use crate::types::{int64, uint32, BlockNumber, OffsetNumber};
use crate::Datum;

/// `PlannerInfo` (nodes/pathnodes.h) — canonical full planner struct lives in
/// [`crate::pathnodes`]; re-exported here so the executor boundary uses the same
/// type (referenced by the `SampleScanGetSampleSize` planner callback pointer).
pub use crate::pathnodes::PlannerInfo;
/// `RelOptInfo` (nodes/pathnodes.h) — canonical full planner struct lives in
/// [`crate::pathnodes`]; re-exported here.
pub use crate::pathnodes::RelOptInfo;

/// `SampleScanGetSampleSize_function` (access/tsmapi.h).
pub type SampleScanGetSampleSizeFunction = Option<
    unsafe extern "C" fn(
        root: *mut PlannerInfo,
        baserel: *mut RelOptInfo,
        paramexprs: *mut List,
        pages: *mut BlockNumber,
        tuples: *mut f64,
    ),
>;
/// `InitSampleScan_function` (access/tsmapi.h). `can be NULL`.
pub type InitSampleScanFunction =
    Option<unsafe extern "C" fn(node: *mut SampleScanState, eflags: c_int)>;
/// `BeginSampleScan_function` (access/tsmapi.h).
pub type BeginSampleScanFunction = Option<
    unsafe extern "C" fn(node: *mut SampleScanState, params: *mut Datum, nparams: c_int, seed: u32),
>;
/// `NextSampleBlock_function` (access/tsmapi.h). `can be NULL`.
pub type NextSampleBlockFunction =
    Option<unsafe extern "C" fn(node: *mut SampleScanState, nblocks: BlockNumber) -> BlockNumber>;
/// `NextSampleTuple_function` (access/tsmapi.h).
pub type NextSampleTupleFunction = Option<
    unsafe extern "C" fn(
        node: *mut SampleScanState,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
    ) -> OffsetNumber,
>;
/// `EndSampleScan_function` (access/tsmapi.h). `can be NULL`.
pub type EndSampleScanFunction = Option<unsafe extern "C" fn(node: *mut SampleScanState)>;

/// `TsmRoutine` (access/tsmapi.h): the struct returned by a tablesample
/// method's handler function. Crosses the boundary by address; this crate only
/// reads/calls its callback pointers.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TsmRoutine {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `List *parameterTypes`
    pub parameterTypes: *mut List,
    /// `bool repeatable_across_queries`
    pub repeatable_across_queries: bool,
    /// `bool repeatable_across_scans`
    pub repeatable_across_scans: bool,
    /// `SampleScanGetSampleSize_function SampleScanGetSampleSize`
    pub SampleScanGetSampleSize: SampleScanGetSampleSizeFunction,
    /// `InitSampleScan_function InitSampleScan` (can be NULL)
    pub InitSampleScan: InitSampleScanFunction,
    /// `BeginSampleScan_function BeginSampleScan`
    pub BeginSampleScan: BeginSampleScanFunction,
    /// `NextSampleBlock_function NextSampleBlock` (can be NULL)
    pub NextSampleBlock: NextSampleBlockFunction,
    /// `NextSampleTuple_function NextSampleTuple`
    pub NextSampleTuple: NextSampleTupleFunction,
    /// `EndSampleScan_function EndSampleScan` (can be NULL)
    pub EndSampleScan: EndSampleScanFunction,
}

/// `TableSampleClause` (nodes/parsenodes.h): the parsed `TABLESAMPLE` clause the
/// `SampleScan` plan node points at. Reached through `*mut Plan`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TableSampleClause {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `Oid tsmhandler` — OID of the tablesample handler function.
    pub tsmhandler: Oid,
    /// `List *args` — tablesample argument expression(s).
    pub args: *mut List,
    /// `Expr *repeatable` — REPEATABLE expression, or NULL if none.
    pub repeatable: *mut Expr,
}

/// `SampleScan` plan node (plannodes.h): `Scan scan` head plus the
/// `TableSampleClause *tablesample` pointer.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SampleScan {
    /// `Scan scan` head — its first field is the embedded `Plan` (whose first
    /// field is `NodeTag`).
    pub scan: Scan,
    /// `struct TableSampleClause *tablesample`.
    pub tablesample: *mut TableSampleClause,
}

/// `SampleScanState` (execnodes.h) — faithful `#[repr(C)]` ABI struct for the
/// sample-scan executor node. The leading `ss` field's first member is a
/// `NodeTag`, so a `*mut SampleScanState` is also a valid `Node *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SampleScanState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `List *args` — expr states for TABLESAMPLE params.
    pub args: *mut List,
    /// `ExprState *repeatable` — expr state for REPEATABLE expr.
    pub repeatable: *mut ExprState,
    /// `struct TsmRoutine *tsmroutine` — descriptor for tablesample method.
    pub tsmroutine: *mut TsmRoutine,
    /// `void *tsm_state` — tablesample method can keep state here.
    pub tsm_state: *mut c_void,
    /// `bool use_bulkread` — use bulkread buffer access strategy?
    pub use_bulkread: bool,
    /// `bool use_pagemode` — use page-at-a-time visibility checking?
    pub use_pagemode: bool,
    /// `bool begun` — false means need to call BeginSampleScan.
    pub begun: bool,
    /// `uint32 seed` — random seed.
    pub seed: uint32,
    /// `int64 donetuples` — number of tuples already returned.
    pub donetuples: int64,
    /// `bool haveblock` — has a block for sampling been determined.
    pub haveblock: bool,
    /// `bool done` — exhausted all tuples?
    pub done: bool,
}

impl SampleScanState {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

// Layout asserts: the embedded heads keep their C offsets so a
// `*mut SampleScanState` can be navigated as the C struct. `NodeTag` is the
// very first field of `ss.ps`, so the node-tag offset is zero.
const _: () = {
    assert!(core::mem::offset_of!(SampleScanState, ss) == 0);
    assert!(core::mem::offset_of!(SampleScan, scan) == 0);
    assert!(core::mem::offset_of!(TableSampleClause, type_) == 0);
    assert!(core::mem::offset_of!(TsmRoutine, type_) == 0);
};

// ===========================================================================
// `WindowAggState` and friends — faithful `#[repr(C)]` ABI for
// `nodeWindowAgg.c`.
//
// The window-aggregate node is ported in-crate
// (`backend-executor-nodeWindowAgg`), so its state node, the per-function /
// per-aggregate working structs, the window-function API object, and the
// `WindowAgg` plan node are all complete, address-stable `#[repr(C)]` structs
// laid out exactly like the C originals (execnodes.h / plannodes.h).  Pointer-
// typed fields cross the boundary as raw pointers (`*mut`), opaque to this
// crate; only the address travels.
// ===========================================================================

use core::ffi::c_uint;

use crate::FmgrInfo;

/// `WindowFunc *` — opaque pointer to a `WindowFunc` plan-tree node
/// (primnodes.h). Read through the expression/catalog seams; only its address
/// crosses the boundary here.
pub type WindowFunc = c_void;

/// `WindowAggStatus` (execnodes.h) — run status of a `WindowAggState`. C enum
/// of `unsigned int` width.
pub type WindowAggStatus = c_uint;
/// No more processing to do.
pub const WINDOWAGG_DONE: WindowAggStatus = 0;
/// Normal processing of window funcs.
pub const WINDOWAGG_RUN: WindowAggStatus = 1;
/// Don't eval window funcs.
pub const WINDOWAGG_PASSTHROUGH: WindowAggStatus = 2;
/// Pass-through plus don't store new tuples during spool.
pub const WINDOWAGG_PASSTHROUGH_STRICT: WindowAggStatus = 3;

/// `WindowFuncExprState` (execnodes.h) — the per-`WindowFunc` expression-state
/// node built by `ExecInitExpr` (`type` == [`T_WindowFuncExprState`]).
///
/// The canonical layout (full embedded `ExprState xprstate`, C-faithful) lives in
/// `execexpr`; re-exported here so both modules share one definition (the former
/// `NodeTag`-only stub had the wrong size). Field access changes `.type_` →
/// `.xprstate.type_`.
pub use crate::execexpr::WindowFuncExprState;

/// `WindowObjectData` (nodeWindowAgg.c) — the window-function API object passed
/// to window functions as `fcinfo->context` (`type` == [`T_WindowObjectData`]).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WindowObjectData {
    /// `NodeTag type`
    pub type_: NodeTag,
    /// `WindowAggState *winstate` — parent `WindowAggState`.
    pub winstate: *mut WindowAggState,
    /// `List *argstates` — `ExprState` trees for fn's arguments.
    pub argstates: *mut List,
    /// `void *localmem` — `WinGetPartitionLocalMemory`'s chunk.
    pub localmem: *mut c_void,
    /// `int markptr` — tuplestore mark pointer for this fn.
    pub markptr: c_int,
    /// `int readptr` — tuplestore read pointer for this fn.
    pub readptr: c_int,
    /// `int64 markpos` — row that `markptr` is positioned on.
    pub markpos: int64,
    /// `int64 seekpos` — row that `readptr` is positioned on.
    pub seekpos: int64,
}

/// `WindowObject` typedef — `WindowObjectData *`.
pub type WindowObject = *mut WindowObjectData;

/// `WindowStatePerFuncData` (nodeWindowAgg.c) — per-window-function working
/// state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WindowStatePerFuncData {
    /// `WindowFuncExprState *wfuncstate`
    pub wfuncstate: *mut WindowFuncExprState,
    /// `WindowFunc *wfunc`
    pub wfunc: *mut WindowFunc,
    /// `int numArguments` — number of arguments.
    pub numArguments: c_int,
    /// `FmgrInfo flinfo` — fmgr lookup data for window function.
    pub flinfo: FmgrInfo,
    /// `Oid winCollation` — collation derived for window function.
    pub winCollation: Oid,
    /// `int16 resulttypeLen`
    pub resulttypeLen: i16,
    /// `bool resulttypeByVal`
    pub resulttypeByVal: bool,
    /// `bool plain_agg` — is it just a plain aggregate function?
    pub plain_agg: bool,
    /// `int aggno` — index of its `WindowStatePerAggData` if so.
    pub aggno: c_int,
    /// `WindowObject winobj` — object used in window function API.
    pub winobj: WindowObject,
}

/// `WindowStatePerFunc` typedef — `WindowStatePerFuncData *`.
pub type WindowStatePerFunc = *mut WindowStatePerFuncData;

/// `WindowStatePerAggData` (nodeWindowAgg.c) — per-plain-aggregate working
/// state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WindowStatePerAggData {
    /// `Oid transfn_oid`
    pub transfn_oid: Oid,
    /// `Oid invtransfn_oid` — may be `InvalidOid`.
    pub invtransfn_oid: Oid,
    /// `Oid finalfn_oid` — may be `InvalidOid`.
    pub finalfn_oid: Oid,
    /// `FmgrInfo transfn`
    pub transfn: FmgrInfo,
    /// `FmgrInfo invtransfn`
    pub invtransfn: FmgrInfo,
    /// `FmgrInfo finalfn`
    pub finalfn: FmgrInfo,
    /// `int numFinalArgs` — number of arguments to pass to finalfn.
    pub numFinalArgs: c_int,
    /// `Datum initValue`
    pub initValue: Datum,
    /// `bool initValueIsNull`
    pub initValueIsNull: bool,
    /// `Datum resultValue` — cached value for current frame boundaries.
    pub resultValue: Datum,
    /// `bool resultValueIsNull`
    pub resultValueIsNull: bool,
    /// `int16 inputtypeLen`
    pub inputtypeLen: i16,
    /// `int16 resulttypeLen`
    pub resulttypeLen: i16,
    /// `int16 transtypeLen`
    pub transtypeLen: i16,
    /// `bool inputtypeByVal`
    pub inputtypeByVal: bool,
    /// `bool resulttypeByVal`
    pub resulttypeByVal: bool,
    /// `bool transtypeByVal`
    pub transtypeByVal: bool,
    /// `int wfuncno` — index of associated `WindowStatePerFuncData`.
    pub wfuncno: c_int,
    /// `MemoryContext aggcontext` — may be private, or `winstate->aggcontext`.
    pub aggcontext: crate::MemoryContext,
    /// `Datum transValue` — current transition value.
    pub transValue: Datum,
    /// `bool transValueIsNull`
    pub transValueIsNull: bool,
    /// `int64 transValueCount` — number of currently-aggregated rows.
    pub transValueCount: int64,
    /// `bool restart` — need to restart this agg in this cycle?
    pub restart: bool,
}

/// `WindowStatePerAgg` typedef — `WindowStatePerAggData *`.
pub type WindowStatePerAgg = *mut WindowStatePerAggData;

/// `WindowAggState` (execnodes.h) — the run-time state for the window-aggregate
/// executor node (`nodeWindowAgg.c`).
///
/// The leading [`ScanStateData`] head's first member is a `NodeTag`, so a
/// `*mut WindowAggState` is also a valid `Node *` / `PlanState *`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WindowAggState {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData,
    /// `List *funcs` — all `WindowFunc` nodes in targetlist.
    pub funcs: *mut List,
    /// `int numfuncs` — total number of window functions.
    pub numfuncs: c_int,
    /// `int numaggs` — number that are plain aggregates.
    pub numaggs: c_int,
    /// `WindowStatePerFunc perfunc` — per-window-function information.
    pub perfunc: WindowStatePerFunc,
    /// `WindowStatePerAgg peragg` — per-plain-aggregate information.
    pub peragg: WindowStatePerAgg,
    /// `ExprState *partEqfunction` — equality funcs for partition columns.
    pub partEqfunction: *mut ExprState,
    /// `ExprState *ordEqfunction` — equality funcs for ordering columns.
    pub ordEqfunction: *mut ExprState,
    /// `Tuplestorestate *buffer` — stores rows of current partition.
    pub buffer: *mut crate::Tuplestorestate,
    /// `int current_ptr` — read pointer # for current row.
    pub current_ptr: c_int,
    /// `int framehead_ptr` — read pointer # for frame head, if used.
    pub framehead_ptr: c_int,
    /// `int frametail_ptr` — read pointer # for frame tail, if used.
    pub frametail_ptr: c_int,
    /// `int grouptail_ptr` — read pointer # for group tail, if used.
    pub grouptail_ptr: c_int,
    /// `int64 spooled_rows` — total # of rows in buffer.
    pub spooled_rows: int64,
    /// `int64 currentpos` — position of current row in partition.
    pub currentpos: int64,
    /// `int64 frameheadpos` — current frame head position.
    pub frameheadpos: int64,
    /// `int64 frametailpos` — current frame tail position (frame end+1).
    pub frametailpos: int64,
    /// `struct WindowObjectData *agg_winobj` — winobj for aggregate fetches.
    pub agg_winobj: *mut WindowObjectData,
    /// `int64 aggregatedbase` — start row for current aggregates.
    pub aggregatedbase: int64,
    /// `int64 aggregatedupto` — rows before this one are aggregated.
    pub aggregatedupto: int64,
    /// `WindowAggStatus status` — run status of `WindowAggState`.
    pub status: WindowAggStatus,
    /// `int frameOptions` — frame_clause options, see WindowDef.
    pub frameOptions: c_int,
    /// `ExprState *startOffset` — expression for starting bound offset.
    pub startOffset: *mut ExprState,
    /// `ExprState *endOffset` — expression for ending bound offset.
    pub endOffset: *mut ExprState,
    /// `Datum startOffsetValue` — result of `startOffset` evaluation.
    pub startOffsetValue: Datum,
    /// `Datum endOffsetValue` — result of `endOffset` evaluation.
    pub endOffsetValue: Datum,
    /// `FmgrInfo startInRangeFunc` — in_range function for `startOffset`.
    pub startInRangeFunc: FmgrInfo,
    /// `FmgrInfo endInRangeFunc` — in_range function for `endOffset`.
    pub endInRangeFunc: FmgrInfo,
    /// `Oid inRangeColl` — collation for in_range tests.
    pub inRangeColl: Oid,
    /// `bool inRangeAsc` — use ASC sort order for in_range tests?
    pub inRangeAsc: bool,
    /// `bool inRangeNullsFirst` — nulls sort first for in_range tests?
    pub inRangeNullsFirst: bool,
    /// `bool use_pass_through`
    pub use_pass_through: bool,
    /// `bool top_window` — true if this is the top-most WindowAgg.
    pub top_window: bool,
    /// `ExprState *runcondition` — condition which must remain true.
    pub runcondition: *mut ExprState,
    /// `int64 currentgroup` — peer group # of current row in partition.
    pub currentgroup: int64,
    /// `int64 frameheadgroup` — peer group # of frame head row.
    pub frameheadgroup: int64,
    /// `int64 frametailgroup` — peer group # of frame tail row.
    pub frametailgroup: int64,
    /// `int64 groupheadpos` — current row's peer group head position.
    pub groupheadpos: int64,
    /// `int64 grouptailpos` — current row's peer group tail position (group
    /// end+1).
    pub grouptailpos: int64,
    /// `MemoryContext partcontext` — context for partition-lifespan data.
    pub partcontext: crate::MemoryContext,
    /// `MemoryContext aggcontext` — shared context for aggregate working data.
    pub aggcontext: crate::MemoryContext,
    /// `MemoryContext curaggcontext` — current aggregate's working data.
    pub curaggcontext: crate::MemoryContext,
    /// `ExprContext *tmpcontext` — short-term evaluation context.
    pub tmpcontext: *mut ExprContext,
    /// `bool all_first` — true if the scan is starting.
    pub all_first: bool,
    /// `bool partition_spooled`
    pub partition_spooled: bool,
    /// `bool next_partition` — true if `begin_partition` needs to be called.
    pub next_partition: bool,
    /// `bool more_partitions`
    pub more_partitions: bool,
    /// `bool framehead_valid`
    pub framehead_valid: bool,
    /// `bool frametail_valid`
    pub frametail_valid: bool,
    /// `bool grouptail_valid`
    pub grouptail_valid: bool,
    /// `TupleTableSlot *first_part_slot` — first tuple of current/next
    /// partition.
    pub first_part_slot: *mut TupleTableSlot,
    /// `TupleTableSlot *framehead_slot` — first tuple of current frame.
    pub framehead_slot: *mut TupleTableSlot,
    /// `TupleTableSlot *frametail_slot` — first tuple after current frame.
    pub frametail_slot: *mut TupleTableSlot,
    /// `TupleTableSlot *agg_row_slot`
    pub agg_row_slot: *mut TupleTableSlot,
    /// `TupleTableSlot *temp_slot_1`
    pub temp_slot_1: *mut TupleTableSlot,
    /// `TupleTableSlot *temp_slot_2`
    pub temp_slot_2: *mut TupleTableSlot,
}

impl WindowAggState {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ss.ps
    }
}

/// `WindowAgg` plan node (plannodes.h).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct WindowAgg {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `char *winname` — name of `WindowClause` implemented by this node.
    pub winname: *mut core::ffi::c_char,
    /// `Index winref` — ID referenced by window functions.
    pub winref: Index,
    /// `int partNumCols` — number of columns in partition clause.
    pub partNumCols: c_int,
    /// `AttrNumber *partColIdx` — their indexes in the target list.
    pub partColIdx: *mut crate::AttrNumber,
    /// `Oid *partOperators` — equality operators for partition columns.
    pub partOperators: *mut Oid,
    /// `Oid *partCollations` — collations for partition columns.
    pub partCollations: *mut Oid,
    /// `int ordNumCols` — number of columns in ordering clause.
    pub ordNumCols: c_int,
    /// `AttrNumber *ordColIdx` — their indexes in the target list.
    pub ordColIdx: *mut crate::AttrNumber,
    /// `Oid *ordOperators` — equality operators for ordering columns.
    pub ordOperators: *mut Oid,
    /// `Oid *ordCollations` — collations for ordering columns.
    pub ordCollations: *mut Oid,
    /// `int frameOptions` — frame_clause options, see WindowDef.
    pub frameOptions: c_int,
    /// `Node *startOffset` — expression for starting bound, if any.
    pub startOffset: *mut crate::Node,
    /// `Node *endOffset` — expression for ending bound, if any.
    pub endOffset: *mut crate::Node,
    /// `List *runCondition` — qual to help short-circuit execution.
    pub runCondition: *mut List,
    /// `List *runConditionOrig` — runCondition for display in EXPLAIN.
    pub runConditionOrig: *mut List,
    /// `Oid startInRangeFunc` — in_range function for `startOffset`.
    pub startInRangeFunc: Oid,
    /// `Oid endInRangeFunc` — in_range function for `endOffset`.
    pub endInRangeFunc: Oid,
    /// `Oid inRangeColl` — collation for in_range tests.
    pub inRangeColl: Oid,
    /// `bool inRangeAsc` — use ASC sort order for in_range tests?
    pub inRangeAsc: bool,
    /// `bool inRangeNullsFirst` — nulls sort first for in_range tests?
    pub inRangeNullsFirst: bool,
    /// `bool topWindow` — false for all apart from the WindowAgg closest to the
    /// root of the plan.
    pub topWindow: bool,
}

// Layout asserts: the embedded heads keep their C offsets so a
// `*mut WindowAggState` can be navigated as the C struct. `NodeTag` is the very
// first field of `ss.ps`, so the node-tag offset is zero.
const _: () = {
    assert!(core::mem::offset_of!(WindowAggState, ss) == 0);
    assert!(core::mem::offset_of!(ScanStateData, ps) == 0);
    assert!(core::mem::offset_of!(WindowObjectData, type_) == 0);
    assert!(core::mem::offset_of!(WindowFuncExprState, xprstate.type_) == 0);
    assert!(core::mem::offset_of!(WindowAgg, plan) == 0);
    assert!(core::mem::align_of::<WindowAggState>() == 8);
};

// ===========================================================================
// Append node ABI (plannodes.h `Append` + execnodes.h `AppendState`,
// `AsyncRequest`, and the parallel-shared `ParallelAppendState`). Used by the
// `backend-executor-nodeAppend` crate.
// ===========================================================================

/// `Append` plan node (plannodes.h):
///
/// ```c
/// typedef struct Append {
///     Plan        plan;
///     Bitmapset  *apprelids;
///     List       *appendplans;
///     int         nasyncplans;
///     int         first_partial_plan;
///     int         part_prune_index;
/// } Append;
/// ```
///
/// The leading `plan` embeds the abstract [`PlanNode`]; a `*mut AppendPlan` is a
/// valid `Plan *` (its first field is `NodeTag`). The node layer reads
/// `appendplans`, `nasyncplans`, `first_partial_plan`, and `part_prune_index`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AppendPlan {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: PlanNode,
    /// `Bitmapset *apprelids` — RTIs of appendrel(s) formed by this node.
    pub apprelids: *mut Bitmapset,
    /// `List *appendplans` — the list of child `Plan`s.
    pub appendplans: *mut List,
    /// `int nasyncplans` — number of asynchronous plans.
    pub nasyncplans: c_int,
    /// `int first_partial_plan` — index in `appendplans`; all preceding plans
    /// are non-partial, all from here onwards are partial.
    pub first_partial_plan: c_int,
    /// `int part_prune_index` — index into `PlannedStmt.partPruneInfos`; `-1`
    /// when no run-time pruning is used.
    pub part_prune_index: c_int,
}

/// `AsyncRequest` (execnodes.h) — async-append/foreign-scan request record:
///
/// ```c
/// typedef struct AsyncRequest {
///     struct PlanState *requestor;
///     struct PlanState *requestee;
///     int     request_index;
///     bool    callback_pending;
///     bool    request_complete;
///     TupleTableSlot *result;
/// } AsyncRequest;
/// ```
///
/// The opaque [`crate::AsyncRequest`] (`c_void`) remains the public boundary
/// type; this is the faithful laid-out struct for in-crate field access.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AsyncRequestData {
    /// `struct PlanState *requestor` — node that wants a tuple.
    pub requestor: *mut PlanStateData,
    /// `struct PlanState *requestee` — node from which a tuple is wanted.
    pub requestee: *mut PlanStateData,
    /// `int request_index` — scratch space for the requestor.
    pub request_index: c_int,
    /// `bool callback_pending` — a callback is needed.
    pub callback_pending: bool,
    /// `bool request_complete` — request complete, `result` valid.
    pub request_complete: bool,
    /// `TupleTableSlot *result` — result (`NULL` or an empty slot if no more
    /// tuples).
    pub result: *mut TupleTableSlot,
}

/// `ParallelAppendState` (nodeAppend.c, file-private). Shared-memory coordination
/// state for parallel-aware Append:
///
/// ```c
/// struct ParallelAppendState {
///     LWLock      pa_lock;
///     int         pa_next_plan;
///     bool        pa_finished[FLEXIBLE_ARRAY_MEMBER];
/// };
/// ```
///
/// The trailing `pa_finished` flexible array is modeled as a zero-length array.
/// The `pa_lock` `LWLock` is carried as the opaque [`crate::LWLock`] head so the
/// struct's size/alignment match the C layout; the node accesses it through the
/// DSM/LWLock seam.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ParallelAppendState {
    /// `LWLock pa_lock` — mutual exclusion to choose the next subplan.
    pub pa_lock: crate::LWLock,
    /// `int pa_next_plan` — next plan to choose by any worker.
    pub pa_next_plan: c_int,
    /// `bool pa_finished[FLEXIBLE_ARRAY_MEMBER]` — per-subplan "finished" flags.
    pub pa_finished: [bool; 0],
}

/// `AppendState` (execnodes.h):
///
/// ```c
/// struct AppendState {
///     PlanState   ps;
///     PlanState **appendplans;
///     int         as_nplans;
///     int         as_whichplan;
///     bool        as_begun;
///     Bitmapset  *as_asyncplans;
///     int         as_nasyncplans;
///     AsyncRequest **as_asyncrequests;
///     TupleTableSlot **as_asyncresults;
///     int         as_nasyncresults;
///     bool        as_syncdone;
///     int         as_nasyncremain;
///     Bitmapset  *as_needrequest;
///     struct WaitEventSet *as_eventset;
///     int         as_first_partial_plan;
///     ParallelAppendState *as_pstate;
///     Size        pstate_len;
///     struct PartitionPruneState *as_prune_state;
///     bool        as_valid_subplans_identified;
///     Bitmapset  *as_valid_subplans;
///     Bitmapset  *as_valid_asyncplans;
///     bool        (*choose_next_subplan) (AppendState *);
/// };
/// ```
///
/// The leading [`PlanStateData`] head's first field is a `NodeTag`, so a
/// `*mut AppendStateData` is a valid `Node *` / `PlanState *`. The
/// `choose_next_subplan` C function pointer is carried as a pointer-sized
/// `*mut c_void` (layout fidelity); the node selects the locally/leader/worker
/// variant via its in-crate strategy, never by dereferencing this slot.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AppendStateData {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData,
    /// `PlanState **appendplans` — array of child `PlanState`s.
    pub appendplans: *mut *mut PlanStateData,
    /// `int as_nplans`.
    pub as_nplans: c_int,
    /// `int as_whichplan` — index of the currently-active sync subplan.
    pub as_whichplan: c_int,
    /// `bool as_begun` — false means the node still needs initialization.
    pub as_begun: bool,
    /// `Bitmapset *as_asyncplans` — indexes of asynchronous plans.
    pub as_asyncplans: *mut Bitmapset,
    /// `int as_nasyncplans` — number of asynchronous plans.
    pub as_nasyncplans: c_int,
    /// `AsyncRequest **as_asyncrequests` — array of `AsyncRequest`s.
    pub as_asyncrequests: *mut *mut AsyncRequestData,
    /// `TupleTableSlot **as_asyncresults` — unreturned results of async plans.
    pub as_asyncresults: *mut *mut TupleTableSlot,
    /// `int as_nasyncresults` — number of valid entries in `as_asyncresults`.
    pub as_nasyncresults: c_int,
    /// `bool as_syncdone` — true if all sync plans done in async mode.
    pub as_syncdone: bool,
    /// `int as_nasyncremain` — number of remaining asynchronous plans.
    pub as_nasyncremain: c_int,
    /// `Bitmapset *as_needrequest` — async plans needing a new request.
    pub as_needrequest: *mut Bitmapset,
    /// `struct WaitEventSet *as_eventset` — wait-event set for async fds.
    pub as_eventset: *mut c_void,
    /// `int as_first_partial_plan` — index of `appendplans` containing the
    /// first partial plan.
    pub as_first_partial_plan: c_int,
    /// `ParallelAppendState *as_pstate` — parallel coordination info.
    pub as_pstate: *mut ParallelAppendState,
    /// `Size pstate_len` — size of the parallel coordination info.
    pub pstate_len: crate::Size,
    /// `struct PartitionPruneState *as_prune_state`.
    pub as_prune_state: *mut c_void,
    /// `bool as_valid_subplans_identified` — is `as_valid_subplans` valid?
    pub as_valid_subplans_identified: bool,
    /// `Bitmapset *as_valid_subplans`.
    pub as_valid_subplans: *mut Bitmapset,
    /// `Bitmapset *as_valid_asyncplans` — valid asynchronous plan indexes.
    pub as_valid_asyncplans: *mut Bitmapset,
    /// `bool (*choose_next_subplan)(AppendState *)` — carried for layout
    /// fidelity; the node dispatches via its in-crate strategy.
    pub choose_next_subplan: *mut c_void,
}

// Layout asserts: the embedded `PlanState` head keeps its C offset so a
// `*mut AppendStateData` can be navigated as the C `AppendState *`.
const _: () = {
    assert!(core::mem::offset_of!(AppendStateData, ps) == 0);
    assert!(core::mem::offset_of!(PlanStateData, type_) == 0);
    assert!(core::mem::offset_of!(AppendPlan, plan) == 0);
    assert!(core::mem::offset_of!(PlanNode, type_) == 0);
    assert!(core::mem::offset_of!(AsyncRequestData, requestor) == 0);
    assert!(core::mem::offset_of!(ParallelAppendState, pa_lock) == 0);
    assert!(core::mem::align_of::<AppendStateData>() == 8);
};

// ===========================================================================
// `Limit` / `LimitState` — faithful `#[repr(C)]` ABI for `nodeLimit.c`.
//
// The LIMIT/OFFSET node has been ported in-crate
// (`backend-executor-nodeLimit`), so its plan node and state node are
// complete, address-stable `#[repr(C)]` structs laid out exactly like the
// PostgreSQL 18.3 `Limit` (plannodes.h) and `LimitState` (execnodes.h). The
// node crate keeps its logic idiomatic but navigates this layout for the
// fields it reads (`node->ps.<field>`, `node->offset`, `node->lstate`, ...).
// ===========================================================================

/// `LimitOption` (nodes.h) — the limit-specification type.
///
/// ```c
/// typedef enum LimitOption
/// {
///     LIMIT_OPTION_COUNT,         /* FETCH FIRST... ONLY */
///     LIMIT_OPTION_WITH_TIES,     /* FETCH FIRST... WITH TIES */
/// } LimitOption;
/// ```
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitOption {
    /// `LIMIT_OPTION_COUNT` — FETCH FIRST... ONLY.
    LIMIT_OPTION_COUNT = 0,
    /// `LIMIT_OPTION_WITH_TIES` — FETCH FIRST... WITH TIES.
    LIMIT_OPTION_WITH_TIES = 1,
}
pub use LimitOption::*;

/// `LimitStateCond` (execnodes.h) — the LIMIT node's state-machine status.
///
/// ```c
/// typedef enum
/// {
///     LIMIT_INITIAL,          /* initial state for LIMIT node */
///     LIMIT_RESCAN,           /* rescan after recomputing parameters */
///     LIMIT_EMPTY,            /* there are no returnable rows */
///     LIMIT_INWINDOW,         /* have returned a row in the window */
///     LIMIT_WINDOWEND_TIES,   /* have returned a tied row */
///     LIMIT_SUBPLANEOF,       /* at EOF of subplan (within window) */
///     LIMIT_WINDOWEND,        /* stepped off end of window */
///     LIMIT_WINDOWSTART,      /* stepped off beginning of window */
/// } LimitStateCond;
/// ```
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LimitStateCond {
    /// `LIMIT_INITIAL` — initial state for LIMIT node.
    LIMIT_INITIAL = 0,
    /// `LIMIT_RESCAN` — rescan after recomputing parameters.
    LIMIT_RESCAN = 1,
    /// `LIMIT_EMPTY` — there are no returnable rows.
    LIMIT_EMPTY = 2,
    /// `LIMIT_INWINDOW` — have returned a row in the window.
    LIMIT_INWINDOW = 3,
    /// `LIMIT_WINDOWEND_TIES` — have returned a tied row.
    LIMIT_WINDOWEND_TIES = 4,
    /// `LIMIT_SUBPLANEOF` — at EOF of subplan (within window).
    LIMIT_SUBPLANEOF = 5,
    /// `LIMIT_WINDOWEND` — stepped off end of window.
    LIMIT_WINDOWEND = 6,
    /// `LIMIT_WINDOWSTART` — stepped off beginning of window.
    LIMIT_WINDOWSTART = 7,
}
pub use LimitStateCond::*;

/// `Limit` plan node (plannodes.h):
///
/// ```c
/// typedef struct Limit
/// {
///     Plan        plan;
///     Node       *limitOffset;     /* OFFSET parameter, or NULL if none */
///     Node       *limitCount;      /* COUNT parameter, or NULL if none */
///     LimitOption limitOption;     /* limit type */
///     int         uniqNumCols;     /* number of columns to check for similarity */
///     AttrNumber *uniqColIdx;      /* their indexes in the target list */
///     Oid        *uniqOperators;   /* equality operators to compare with */
///     Oid        *uniqCollations;  /* collations for equality comparisons */
/// } Limit;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Limit {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: PlanNode,
    /// `Node *limitOffset` — OFFSET parameter, or NULL if none.
    pub limitOffset: *mut crate::Node,
    /// `Node *limitCount` — COUNT parameter, or NULL if none.
    pub limitCount: *mut crate::Node,
    /// `LimitOption limitOption` — limit type.
    pub limitOption: LimitOption,
    /// `int uniqNumCols` — number of columns to check for similarity.
    pub uniqNumCols: c_int,
    /// `AttrNumber *uniqColIdx` — their indexes in the target list.
    pub uniqColIdx: *mut crate::AttrNumber,
    /// `Oid *uniqOperators` — equality operators to compare with.
    pub uniqOperators: *mut Oid,
    /// `Oid *uniqCollations` — collations for equality comparisons.
    pub uniqCollations: *mut Oid,
}

/// `LimitState` (execnodes.h):
///
/// ```c
/// typedef struct LimitState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     ExprState  *limitOffset;    /* OFFSET parameter, or NULL if none */
///     ExprState  *limitCount;     /* COUNT parameter, or NULL if none */
///     LimitOption limitOption;    /* limit specification type */
///     int64       offset;         /* current OFFSET value */
///     int64       count;          /* current COUNT, if any */
///     bool        noCount;        /* if true, ignore count */
///     LimitStateCond lstate;      /* state machine status, as above */
///     int64       position;       /* 1-based index of last tuple returned */
///     TupleTableSlot *subSlot;    /* tuple last obtained from subplan */
///     ExprState  *eqfunction;     /* tuple equality qual in case of WITH TIES */
///     TupleTableSlot *last_slot;  /* slot for evaluation of ties */
/// } LimitState;
/// ```
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LimitStateData {
    /// `PlanState ps` — its first field is `NodeTag`, so `&self` is a valid
    /// `Node *` / `PlanState *`.
    pub ps: PlanStateData,
    /// `ExprState *limitOffset` — OFFSET parameter, or NULL if none.
    pub limitOffset: *mut ExprState,
    /// `ExprState *limitCount` — COUNT parameter, or NULL if none.
    pub limitCount: *mut ExprState,
    /// `LimitOption limitOption` — limit specification type.
    pub limitOption: LimitOption,
    /// `int64 offset` — current OFFSET value.
    pub offset: crate::int64,
    /// `int64 count` — current COUNT, if any.
    pub count: crate::int64,
    /// `bool noCount` — if true, ignore count.
    pub noCount: bool,
    /// `LimitStateCond lstate` — state machine status.
    pub lstate: LimitStateCond,
    /// `int64 position` — 1-based index of last tuple returned.
    pub position: crate::int64,
    /// `TupleTableSlot *subSlot` — tuple last obtained from subplan.
    pub subSlot: *mut TupleTableSlot,
    /// `ExprState *eqfunction` — tuple equality qual in case of WITH TIES option.
    pub eqfunction: *mut ExprState,
    /// `TupleTableSlot *last_slot` — slot for evaluation of ties.
    pub last_slot: *mut TupleTableSlot,
}

impl LimitStateData {
    /// `&node->ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData {
        &self.ps
    }

    /// `&mut node->ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData {
        &mut self.ps
    }
}

// Layout asserts: the embedded `PlanState` head keeps its C offset so a
// `*mut LimitStateData` can be navigated as the C `LimitState *`, and the
// `Limit` plan node head keeps the `Plan` at offset 0.
const _: () = {
    assert!(core::mem::offset_of!(LimitStateData, ps) == 0);
    assert!(
        core::mem::offset_of!(LimitStateData, limitOffset) == core::mem::size_of::<PlanStateData>()
    );
    assert!(core::mem::offset_of!(Limit, plan) == 0);
    assert!(core::mem::align_of::<LimitStateData>() == 8);
    assert!(core::mem::align_of::<Limit>() == 8);
    // LimitOption/LimitStateCond are 4-byte C enums.
    assert!(core::mem::size_of::<LimitOption>() == 4);
    assert!(core::mem::size_of::<LimitStateCond>() == 4);
};

// Layout asserts for the spelled-out `EStateData`: it must be ABI-identical to
// the JIT-oriented opaque [`crate::EState`] (the field offsets matched by the
// `EState` accessor helpers in `jit.rs`), so a pointer can be reinterpreted
// between the two views.
const _: () = {
    use core::mem::offset_of;
    assert!(offset_of!(EStateData, es_direction) == 4);
    assert!(offset_of!(EStateData, es_snapshot) == 8);
    assert!(offset_of!(EStateData, es_range_table) == 24);
    assert!(offset_of!(EStateData, es_range_table_size) == 32);
    assert!(offset_of!(EStateData, es_relations) == 40);
    assert!(offset_of!(EStateData, es_unpruned_relids) == 96);
    assert!(offset_of!(EStateData, es_output_cid) == 120);
    assert!(offset_of!(EStateData, es_param_exec_vals) == 176);
    assert!(offset_of!(EStateData, es_query_cxt) == 192);
    assert!(offset_of!(EStateData, es_exprcontexts) == 240);
    assert!(offset_of!(EStateData, es_subplanstates) == 248);
    assert!(offset_of!(EStateData, es_per_tuple_exprcontext) == 264);
    assert!(offset_of!(EStateData, es_jit_flags) == 304);
    assert!(offset_of!(EStateData, es_insert_pending_modifytables) == 336);
    assert!(core::mem::size_of::<EStateData>() == 344);
    assert!(core::mem::align_of::<EStateData>() == 8);
};
