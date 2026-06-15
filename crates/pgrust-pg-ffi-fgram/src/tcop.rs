//! ABI vocabulary for the `tcop` command-processing spine.
//!
//! These `#[repr(C)]` structs / enums / constants cross the boundary between
//! the rewritten `backend-tcop-*` crates and the rest of the backend.  They
//! mirror the C definitions in
//!   * `src/include/tcop/cmdtag.h`   (`QueryCompletion`, `COMPLETION_TAG_BUFSIZE`,
//!                                    the `CommandTag` enumerators)
//!   * `src/include/utils/portal.h`  (`PortalData`/`Portal`, `PortalStrategy`,
//!                                    `PortalStatus`)
//!   * `src/include/tcop/dest.h`     (`CommandDest`, `DestReceiver` — already
//!                                    modeled in `executor.rs`; re-used here, not
//!                                    re-defined, to avoid the ambiguous-glob trap)
//!
//! `CommandTag` is already declared (`= c_int`) in `cache_remainder.rs` and
//! `CommandDest` / `DestReceiver` / all `Dest*` constants in `executor.rs`; this
//! module deliberately re-uses those rather than re-defining them.  Layout-
//! critical fields keep their exact C order/width; sub-objects the backend has
//! not yet modeled are held as pointer-width opaque handles (`*mut c_void`),
//! which is ABI-identical to the C pointers they stand in for.

use core::ffi::{c_char, c_int, c_void};

use crate::list::List;
use crate::memory::MemoryContext;
use crate::{Snapshot, TupleDesc};

// `CommandTag` is declared (`= c_int`) in `cache_remainder.rs`; re-export it
// here so the whole tcop ABI is nameable from this one module.
pub use crate::CommandTag;

// `CommandDest` / `DestReceiver` / the `Dest*` constants already live in
// `executor.rs` (modeled from `tcop/dest.h`); re-export them here so the
// `backend-tcop-*` crates can name the whole tcop ABI from one module without
// re-defining them (avoiding the ambiguous-glob trap).
pub use crate::executor::{
    CommandDest, DestCopyOut, DestDebug, DestExplainSerialize, DestIntoRel, DestNone, DestReceiver,
    DestRemote, DestRemoteExecute, DestRemoteSimple, DestSPI, DestSQLFunction, DestTransientRel,
    DestTupleQueue, DestTuplestore,
};

/* ===========================================================================
 * cmdtag.h — QueryCompletion + buffer size + key CommandTag enumerators.
 * ======================================================================== */

/// `COMPLETION_TAG_BUFSIZE` — buffer size required for command completion tags
/// (`src/include/tcop/cmdtag.h`).
pub const COMPLETION_TAG_BUFSIZE: usize = 64;

/// `struct QueryCompletion` (`src/include/tcop/cmdtag.h`) — command-completion
/// data for an executed query.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryCompletion {
    /// `CommandTag commandTag`.
    pub commandTag: CommandTag,
    /// `uint64 nprocessed`.
    pub nprocessed: u64,
}

// The `CommandTag` enumerators are an `enum` (C `int`-sized) defined by the
// alphabetically-sorted `cmdtaglist.h`; their integer values equal their
// 0-based position in that list.  Only the handful referenced by the tcop spine
// are surfaced here (verified against
// `src/include/tcop/cmdtaglist.h` in PostgreSQL 18.3).
/// `CMDTAG_UNKNOWN` — the `"???"` tag (list position 0).
pub const CMDTAG_UNKNOWN: CommandTag = 0;
/// `CMDTAG_DELETE` (list position 103).
pub const CMDTAG_DELETE: CommandTag = 103;
/// `CMDTAG_INSERT` (list position 158).
pub const CMDTAG_INSERT: CommandTag = 158;
/// `CMDTAG_MERGE` (list position 163).
pub const CMDTAG_MERGE: CommandTag = 163;
/// `CMDTAG_SELECT` (list position 179).
pub const CMDTAG_SELECT: CommandTag = 179;
/// `CMDTAG_UPDATE` (list position 191).
pub const CMDTAG_UPDATE: CommandTag = 191;

/* ===========================================================================
 * portal.h — PortalStrategy / PortalStatus / PortalData.
 * ======================================================================== */

/// `PortalStrategy` (`src/include/utils/portal.h`).
pub type PortalStrategy = c_int;
/// `PORTAL_ONE_SELECT`.
pub const PORTAL_ONE_SELECT: PortalStrategy = 0;
/// `PORTAL_ONE_RETURNING`.
pub const PORTAL_ONE_RETURNING: PortalStrategy = 1;
/// `PORTAL_ONE_MOD_WITH`.
pub const PORTAL_ONE_MOD_WITH: PortalStrategy = 2;
/// `PORTAL_UTIL_SELECT`.
pub const PORTAL_UTIL_SELECT: PortalStrategy = 3;
/// `PORTAL_MULTI_QUERY`.
pub const PORTAL_MULTI_QUERY: PortalStrategy = 4;

/// `PortalStatus` (`src/include/utils/portal.h`).
pub type PortalStatus = c_int;
/// `PORTAL_NEW` — freshly created.
pub const PORTAL_NEW: PortalStatus = 0;
/// `PORTAL_DEFINED` — `PortalDefineQuery` done.
pub const PORTAL_DEFINED: PortalStatus = 1;
/// `PORTAL_READY` — `PortalStart` complete, can run it.
pub const PORTAL_READY: PortalStatus = 2;
/// `PORTAL_ACTIVE` — portal is running (can't delete it).
pub const PORTAL_ACTIVE: PortalStatus = 3;
/// `PORTAL_DONE` — portal is finished (don't re-run it).
pub const PORTAL_DONE: PortalStatus = 4;
/// `PORTAL_FAILED` — portal got error (can't re-run it).
pub const PORTAL_FAILED: PortalStatus = 5;

/// `SubTransactionId` (`c.h`).
pub type SubTransactionId = u32;
/// `TimestampTz` (`datatype/timestamp.h`).
pub type TimestampTz = i64;

/// `ParamListInfo` — opaque handle (`nodes/params.h`).
pub type ParamListInfo = *mut c_void;
/// `QueryEnvironment *` — opaque handle (`utils/queryenvironment.h`).
pub type QueryEnvironmentPtr = *mut c_void;
/// `ResourceOwner` — opaque handle (`utils/resowner.h`).
pub type ResourceOwner = *mut c_void;
/// `CachedPlan *` — opaque handle (`utils/plancache.h`).
pub type CachedPlanPtr = *mut c_void;
/// `Tuplestorestate *` — opaque handle (`utils/tuplestore.h`).
pub type TuplestorestatePtr = *mut c_void;
/// `QueryDesc *` — pointer to the [`QueryDesc`] modeled below (constructed by
/// `CreateQueryDesc` / the executor, passed through the tcop seams).
pub type QueryDescPtr = *mut QueryDesc;

/// `struct PortalData` (`src/include/utils/portal.h`) — execution state of a
/// running or runnable query.  `Portal` is `*mut PortalData`.
///
/// Layout matches PostgreSQL 18.3 exactly so a raw `Portal` produced by
/// portalmem can be reinterpreted here.  Sub-objects not yet modeled are held
/// as opaque pointers (ABI-identical to the C pointers).
#[repr(C)]
pub struct PortalData {
    // Bookkeeping data
    pub name: *const c_char,
    pub prepStmtName: *const c_char,
    pub portalContext: MemoryContext,
    pub resowner: ResourceOwner,
    pub cleanup: Option<unsafe extern "C" fn(*mut PortalData)>,

    // Subtransaction bookkeeping
    pub createSubid: SubTransactionId,
    pub activeSubid: SubTransactionId,
    pub createLevel: c_int,

    // The query or queries the portal will execute
    pub sourceText: *const c_char,
    pub commandTag: CommandTag,
    pub qc: QueryCompletion,
    pub stmts: *mut List,
    pub cplan: CachedPlanPtr,

    pub portalParams: ParamListInfo,
    pub queryEnv: QueryEnvironmentPtr,

    // Features/options
    pub strategy: PortalStrategy,
    pub cursorOptions: c_int,

    // Status data
    pub status: PortalStatus,
    pub portalPinned: bool,
    pub autoHeld: bool,

    // Executor invocation info
    pub queryDesc: QueryDescPtr,

    // Result tuple descriptor + per-column format codes
    pub tupDesc: TupleDesc,
    pub formats: *mut i16,

    // Outermost ActiveSnapshot for execution
    pub portalSnapshot: Snapshot,

    // Held-cursor tuple store
    pub holdStore: TuplestorestatePtr,
    pub holdContext: MemoryContext,
    pub holdSnapshot: Snapshot,

    // Cursor position
    pub atStart: bool,
    pub atEnd: bool,
    pub portalPos: u64,

    // Presentation data
    pub creation_time: TimestampTz,
    pub visible: bool,
}

/// `Portal` — `typedef struct PortalData *Portal`.
pub type Portal = *mut PortalData;

/* ===========================================================================
 * nodetags.h — node tags the pquery spine inspects via `IsA(...)`.
 *
 * Values verified against `build-rust/src/include/nodes/nodetags.h` (PG 18.3).
 * ======================================================================== */

use crate::types::NodeTag;

/// `T_Query` = 67.
pub const T_Query: NodeTag = 67;
/// `T_RoleSpec` = 75 (`nodes/nodetags.h`).
pub const T_RoleSpec: NodeTag = 75;
/// `T_VariableSetStmt` = 158.
pub const T_VariableSetStmt: NodeTag = 158;
/// `T_VariableShowStmt` = 159.
pub const T_VariableShowStmt: NodeTag = 159;
/// `T_FetchStmt` = 203.
pub const T_FetchStmt: NodeTag = 203;
/// `T_NotifyStmt` = 222.
pub const T_NotifyStmt: NodeTag = 222;
/// `T_ListenStmt` = 223.
pub const T_ListenStmt: NodeTag = 223;
/// `T_UnlistenStmt` = 224.
pub const T_UnlistenStmt: NodeTag = 224;
/// `T_TransactionStmt` = 225.
pub const T_TransactionStmt: NodeTag = 225;
/// `T_CheckPointStmt` = 244.
pub const T_CheckPointStmt: NodeTag = 244;
/// `T_LockStmt` = 246.
pub const T_LockStmt: NodeTag = 246;
/// `T_ConstraintsSetStmt` = 247.
pub const T_ConstraintsSetStmt: NodeTag = 247;
/// `T_ExecuteStmt` = 253.
pub const T_ExecuteStmt: NodeTag = 253;
/// `T_PlannedStmt` = 330.
pub const T_PlannedStmt: NodeTag = 330;

/* ===========================================================================
 * nodes.h — CmdType (re-export) + ScanDirection / FetchDirection scalars.
 * ======================================================================== */

// `CmdType` and the `CMD_*` constants are declared in `pathnodes.rs`; re-export
// them so the tcop spine names them from this one module.
pub use crate::pathnodes::{
    CmdType, CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_NOTHING, CMD_SELECT, CMD_UNKNOWN, CMD_UPDATE,
    CMD_UTILITY,
};

/// `ScanDirection` (`access/sdir.h`) — `enum ScanDirection`, `int`-sized.
pub type ScanDirection = c_int;
/// `BackwardScanDirection = -1`.
pub const BackwardScanDirection: ScanDirection = -1;
/// `NoMovementScanDirection = 0`.
pub const NoMovementScanDirection: ScanDirection = 0;
/// `ForwardScanDirection = 1`.
pub const ForwardScanDirection: ScanDirection = 1;

/// `FetchDirection` (`nodes/parsenodes.h`) — `enum FetchDirection`, `int`-sized.
pub type FetchDirection = c_int;
/// `FETCH_FORWARD = 0`.
pub const FETCH_FORWARD: FetchDirection = 0;
/// `FETCH_BACKWARD = 1`.
pub const FETCH_BACKWARD: FetchDirection = 1;
/// `FETCH_ABSOLUTE = 2`.
pub const FETCH_ABSOLUTE: FetchDirection = 2;
/// `FETCH_RELATIVE = 3`.
pub const FETCH_RELATIVE: FetchDirection = 3;
/// `FETCH_ALL` = `LONG_MAX` (`nodes/parsenodes.h`).
pub const FETCH_ALL: core::ffi::c_long = core::ffi::c_long::MAX;

/* ===========================================================================
 * executor.h — EXEC_FLAG_* (executor eflags the pquery spine passes/OR-s).
 * ======================================================================== */

/// `EXEC_FLAG_REWIND` = 0x0004 — need efficient rescan.
pub const EXEC_FLAG_REWIND: c_int = 0x0004;
/// `EXEC_FLAG_BACKWARD` = 0x0008 — need backward scan.
pub const EXEC_FLAG_BACKWARD: c_int = 0x0008;

/* ===========================================================================
 * parsenodes.h — CURSOR_OPT_* cursor option bits.
 * ======================================================================== */

/// `CURSOR_OPT_SCROLL` = 0x0002 — SCROLL explicitly given.
pub const CURSOR_OPT_SCROLL: c_int = 0x0002;
/// `CURSOR_OPT_NO_SCROLL` = 0x0004 — NO SCROLL explicitly given.
pub const CURSOR_OPT_NO_SCROLL: c_int = 0x0004;

/* ===========================================================================
 * utility.h — ProcessUtilityContext values passed to ProcessUtility.
 * ======================================================================== */

/// `ProcessUtilityContext` (`tcop/utility.h`).
pub type ProcessUtilityContext = c_int;
/// `PROCESS_UTILITY_TOPLEVEL` = 0 — toplevel interactive command.
pub const PROCESS_UTILITY_TOPLEVEL: ProcessUtilityContext = 0;
/// `PROCESS_UTILITY_QUERY` = 1 — a complete query, but not toplevel.
pub const PROCESS_UTILITY_QUERY: ProcessUtilityContext = 1;
/// `PROCESS_UTILITY_QUERY_NONATOMIC` = 2.
pub const PROCESS_UTILITY_QUERY_NONATOMIC: ProcessUtilityContext = 2;
/// `PROCESS_UTILITY_SUBCOMMAND` = 3.
pub const PROCESS_UTILITY_SUBCOMMAND: ProcessUtilityContext = 3;

/* ===========================================================================
 * execdesc.h — QueryDesc.
 * ======================================================================== */

/// `struct QueryDesc` (`src/include/executor/execdesc.h`) — the executor's
/// per-query descriptor built by `CreateQueryDesc`.  Layout matches PG 18.3.
#[repr(C)]
pub struct QueryDesc {
    /// `CmdType operation` — `CMD_SELECT`, `CMD_UPDATE`, etc.
    pub operation: CmdType,
    /// `PlannedStmt *plannedstmt` — planner's output (could be utility, too).
    pub plannedstmt: *mut PlannedStmt,
    /// `const char *sourceText` — source text of the query.
    pub sourceText: *const c_char,
    /// `Snapshot snapshot` — snapshot to use for query.
    pub snapshot: Snapshot,
    /// `Snapshot crosscheck_snapshot` — crosscheck for RI update/delete.
    pub crosscheck_snapshot: Snapshot,
    /// `DestReceiver *dest` — the destination for tuple output.
    pub dest: *mut DestReceiver,
    /// `ParamListInfo params` — param values being passed in.
    pub params: ParamListInfo,
    /// `QueryEnvironment *queryEnv` — query environment passed in.
    pub queryEnv: QueryEnvironmentPtr,
    /// `int instrument_options` — OR of `InstrumentOption` flags.
    pub instrument_options: c_int,

    /// `TupleDesc tupDesc` — descriptor for result tuples (set by ExecutorStart).
    pub tupDesc: TupleDesc,
    /// `EState *estate` — executor's query-wide state (set by ExecutorStart).
    pub estate: *mut crate::execnodes::EStateData,
    /// `PlanState *planstate` — tree of per-plan-node state (set by ExecutorStart).
    pub planstate: *mut c_void,

    /// `bool already_executed` — true if previously executed (set by ExecutePlan).
    pub already_executed: bool,

    /// `struct Instrumentation *totaltime` — total time spent in ExecutorRun;
    /// always NULL from core, plugins may set it.
    pub totaltime: *mut c_void,
}

/* ===========================================================================
 * plannodes.h — PlannedStmt.  Layout matches PG 18.3 exactly so a raw
 * `PlannedStmt *` from the planner can be reinterpreted here.
 * ======================================================================== */

/// `ParseLoc` (`nodes/nodes.h`) — `typedef int`.
pub type ParseLoc = c_int;

/// `struct PlannedStmt` (`src/include/nodes/plannodes.h`).
#[repr(C)]
pub struct PlannedStmt {
    /// `NodeTag type` — always `T_PlannedStmt`.
    pub type_: NodeTag,
    /// `CmdType commandType` — select|insert|update|delete|merge|utility.
    pub commandType: CmdType,
    /// `int64 queryId` — query identifier (copied from Query).
    pub queryId: i64,
    /// `int64 planId` — plan identifier (can be set by plugins).
    pub planId: i64,
    /// `bool hasReturning` — is it insert|update|delete|merge RETURNING?
    pub hasReturning: bool,
    /// `bool hasModifyingCTE` — has insert|update|delete|merge in WITH?
    pub hasModifyingCTE: bool,
    /// `bool canSetTag` — do I set the command result tag?
    pub canSetTag: bool,
    /// `bool transientPlan` — redo plan when TransactionXmin changes?
    pub transientPlan: bool,
    /// `bool dependsOnRole` — is plan specific to current role?
    pub dependsOnRole: bool,
    /// `bool parallelModeNeeded` — parallel mode required to execute?
    pub parallelModeNeeded: bool,
    /// `int jitFlags` — which forms of JIT should be performed.
    pub jitFlags: c_int,
    /// `struct Plan *planTree` — tree of Plan nodes.
    pub planTree: *mut crate::nodeindexscan::Plan,
    /// `List *partPruneInfos` — list of PartitionPruneInfo in the plan.
    pub partPruneInfos: *mut List,
    /// `List *rtable` — list of RangeTblEntry nodes.
    pub rtable: *mut List,
    /// `Bitmapset *unprunableRelids`.
    pub unprunableRelids: *mut c_void,
    /// `List *permInfos` — list of RTEPermissionInfo nodes.
    pub permInfos: *mut List,
    /// `List *resultRelations` — integer list of RT indexes, or NIL.
    pub resultRelations: *mut List,
    /// `List *appendRelations` — list of AppendRelInfo nodes.
    pub appendRelations: *mut List,
    /// `List *subplans` — plan trees for SubPlan expressions.
    pub subplans: *mut List,
    /// `Bitmapset *rewindPlanIDs`.
    pub rewindPlanIDs: *mut c_void,
    /// `List *rowMarks` — a list of PlanRowMark's.
    pub rowMarks: *mut List,
    /// `List *relationOids` — OIDs of relations the plan depends on.
    pub relationOids: *mut List,
    /// `List *invalItems` — other dependencies, as PlanInvalItems.
    pub invalItems: *mut List,
    /// `List *paramExecTypes` — type OIDs for PARAM_EXEC Params.
    pub paramExecTypes: *mut List,
    /// `Node *utilityStmt` — non-null if this is utility stmt.
    pub utilityStmt: *mut c_void,
    /// `ParseLoc stmt_location` — start location, or -1 if unknown.
    pub stmt_location: ParseLoc,
    /// `ParseLoc stmt_len` — length in bytes; 0 means "rest of string".
    pub stmt_len: ParseLoc,
}

/* ===========================================================================
 * parsenodes.h — Query.  Modeled fully (layout matches PG 18.3) so the pquery
 * spine can read commandType/canSetTag/utilityStmt/hasModifyingCTE/targetList/
 * returningList directly, as `ChoosePortalStrategy`/`FetchStatementTargetList`
 * do in C.
 * ======================================================================== */

/// `QuerySource` (`nodes/parsenodes.h`).
pub type QuerySource = c_int;
/// `OverridingKind` (`nodes/primnodes.h`).
pub type OverridingKind = c_int;
/// `LimitOption` (`nodes/nodes.h`).
pub type LimitOption = c_int;

/// `struct Query` (`src/include/nodes/parsenodes.h`).
#[repr(C)]
pub struct Query {
    /// `NodeTag type` — always `T_Query`.
    pub type_: NodeTag,
    /// `CmdType commandType`.
    pub commandType: CmdType,
    /// `QuerySource querySource`.
    pub querySource: QuerySource,
    /// `int64 queryId`.
    pub queryId: i64,
    /// `bool canSetTag` — do I set the command result tag?
    pub canSetTag: bool,
    /// `Node *utilityStmt` — non-null if commandType == CMD_UTILITY.
    pub utilityStmt: *mut c_void,
    /// `int resultRelation`.
    pub resultRelation: c_int,
    /// `bool hasAggs`.
    pub hasAggs: bool,
    /// `bool hasWindowFuncs`.
    pub hasWindowFuncs: bool,
    /// `bool hasTargetSRFs`.
    pub hasTargetSRFs: bool,
    /// `bool hasSubLinks`.
    pub hasSubLinks: bool,
    /// `bool hasDistinctOn`.
    pub hasDistinctOn: bool,
    /// `bool hasRecursive`.
    pub hasRecursive: bool,
    /// `bool hasModifyingCTE` — has INSERT/UPDATE/DELETE/MERGE in WITH.
    pub hasModifyingCTE: bool,
    /// `bool hasForUpdate`.
    pub hasForUpdate: bool,
    /// `bool hasRowSecurity`.
    pub hasRowSecurity: bool,
    /// `bool hasGroupRTE`.
    pub hasGroupRTE: bool,
    /// `bool isReturn`.
    pub isReturn: bool,
    /// `List *cteList`.
    pub cteList: *mut List,
    /// `List *rtable`.
    pub rtable: *mut List,
    /// `List *rteperminfos`.
    pub rteperminfos: *mut List,
    /// `FromExpr *jointree`.
    pub jointree: *mut c_void,
    /// `List *mergeActionList`.
    pub mergeActionList: *mut List,
    /// `int mergeTargetRelation`.
    pub mergeTargetRelation: c_int,
    /// `Node *mergeJoinCondition`.
    pub mergeJoinCondition: *mut c_void,
    /// `List *targetList` — target list (of TargetEntry).
    pub targetList: *mut List,
    /// `OverridingKind override`.
    pub r#override: OverridingKind,
    /// `OnConflictExpr *onConflict`.
    pub onConflict: *mut c_void,
    /// `char *returningOldAlias`.
    pub returningOldAlias: *mut c_char,
    /// `char *returningNewAlias`.
    pub returningNewAlias: *mut c_char,
    /// `List *returningList` — return-values list (of TargetEntry).
    pub returningList: *mut List,
    /// `List *groupClause`.
    pub groupClause: *mut List,
    /// `bool groupDistinct`.
    pub groupDistinct: bool,
    /// `List *groupingSets`.
    pub groupingSets: *mut List,
    /// `Node *havingQual`.
    pub havingQual: *mut c_void,
    /// `List *windowClause`.
    pub windowClause: *mut List,
    /// `List *distinctClause`.
    pub distinctClause: *mut List,
    /// `List *sortClause`.
    pub sortClause: *mut List,
    /// `Node *limitOffset`.
    pub limitOffset: *mut c_void,
    /// `Node *limitCount`.
    pub limitCount: *mut c_void,
    /// `LimitOption limitOption`.
    pub limitOption: LimitOption,
    /// `List *rowMarks`.
    pub rowMarks: *mut List,
    /// `Node *setOperations`.
    pub setOperations: *mut c_void,
    /// `List *constraintDeps`.
    pub constraintDeps: *mut List,
    /// `List *withCheckOptions`.
    pub withCheckOptions: *mut List,
    /// `ParseLoc stmt_location`.
    pub stmt_location: ParseLoc,
    /// `ParseLoc stmt_len`.
    pub stmt_len: ParseLoc,
}

// Layout asserts locking the fields the tcop spine reads to the C ABI (PG 18.3,
// LP64).  Offsets follow `#[repr(C)]` from the field order/widths above.
const _: () = {
    use core::mem::offset_of;
    // QueryDesc (execdesc.h)
    assert!(offset_of!(QueryDesc, operation) == 0);
    assert!(offset_of!(QueryDesc, plannedstmt) == 8);
    assert!(offset_of!(QueryDesc, sourceText) == 16);
    assert!(offset_of!(QueryDesc, snapshot) == 24);
    assert!(offset_of!(QueryDesc, crosscheck_snapshot) == 32);
    assert!(offset_of!(QueryDesc, dest) == 40);
    assert!(offset_of!(QueryDesc, params) == 48);
    assert!(offset_of!(QueryDesc, queryEnv) == 56);
    assert!(offset_of!(QueryDesc, instrument_options) == 64);
    assert!(offset_of!(QueryDesc, tupDesc) == 72);
    assert!(offset_of!(QueryDesc, estate) == 80);
    assert!(offset_of!(QueryDesc, planstate) == 88);
    assert!(offset_of!(QueryDesc, already_executed) == 96);
    assert!(offset_of!(QueryDesc, totaltime) == 104);
    // PlannedStmt (plannodes.h)
    assert!(offset_of!(PlannedStmt, commandType) == 4);
    assert!(offset_of!(PlannedStmt, queryId) == 8);
    assert!(offset_of!(PlannedStmt, hasReturning) == 24);
    assert!(offset_of!(PlannedStmt, hasModifyingCTE) == 25);
    assert!(offset_of!(PlannedStmt, canSetTag) == 26);
    assert!(offset_of!(PlannedStmt, planTree) == 40);
    assert!(offset_of!(PlannedStmt, utilityStmt) == 144);
    // Query (parsenodes.h)
    assert!(offset_of!(Query, commandType) == 4);
    assert!(offset_of!(Query, querySource) == 8);
    assert!(offset_of!(Query, queryId) == 16);
    assert!(offset_of!(Query, canSetTag) == 24);
    assert!(offset_of!(Query, utilityStmt) == 32);
    assert!(offset_of!(Query, hasModifyingCTE) == 50);
    assert!(offset_of!(Query, targetList) == 112);
    assert!(offset_of!(Query, returningList) == 152);
};
