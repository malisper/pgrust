//! Window-aggregation node vocabulary (`nodes/plannodes.h`,
//! `nodes/primnodes.h`, `executor/execnodes.h`, `executor/nodeWindowAgg.h`,
//! `windowapi.h`), trimmed to what the `nodeWindowAgg.c` port consumes.
//!
//! C aliases used cross-subsystem appear as the established handle/id shapes,
//! exactly as in [`crate::nodeagg`]:
//!
//! - `TupleTableSlot *` → [`SlotId`] into the EState slot pool;
//! - `ExprContext *` → an [`EcxtId`] index into the EState-owned pool (the C
//!   `tmpcontext` aliases a second `ExecAssignExprContext`-built context, so it
//!   is carried by id);
//! - `Tuplestorestate *` (utils/sort/tuplestore.c) → the real
//!   [`crate::funcapi::Tuplestorestate`] carried in an owning `PgBox`;
//! - `MemoryContext` → the real [`MemoryContext`] handle;
//! - `FmgrInfo` → the real [`FmgrInfo`] struct.
//!
//! The window-function-private `WindowObjectData` / `WindowStatePerFuncData` /
//! `WindowStatePerAggData` structs live here (alongside `WindowAggState`),
//! mirroring nodeAgg's decision to home `AggStatePerAgg`/`PerTrans`/`PerGroup`
//! in [`crate::nodeagg`].

use mcx::{Mcx, MemoryContext, PgBox, PgString, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::PgResult;
use types_tuple::heaptuple::Datum;

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, ScanStateData, SlotId};
use crate::funcapi::Tuplestorestate;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;
use crate::primnodes::{Expr, WindowFunc};

// ---------------------------------------------------------------------------
// Node tags (nodes/nodetags.h)
// ---------------------------------------------------------------------------

/// `T_WindowAgg` plan node.
pub const T_WindowAgg: NodeTag = NodeTag(366);
/// `T_WindowAggState` executor state node.
pub const T_WindowAggState: NodeTag = NodeTag(430);
/// `T_WindowFuncExprState` executor state node.
pub const T_WindowFuncExprState: NodeTag = NodeTag(390);

// ---------------------------------------------------------------------------
// FRAMEOPTION_* and WINDOW_SEEK_* constants (nodes/parsenodes.h, windowapi.h)
// ---------------------------------------------------------------------------

/// `FRAMEOPTION_NONDEFAULT` — any specs?
pub const FRAMEOPTION_NONDEFAULT: i32 = 0x00001;
/// `FRAMEOPTION_RANGE` — RANGE behavior.
pub const FRAMEOPTION_RANGE: i32 = 0x00002;
/// `FRAMEOPTION_ROWS` — ROWS behavior.
pub const FRAMEOPTION_ROWS: i32 = 0x00004;
/// `FRAMEOPTION_GROUPS` — GROUPS behavior.
pub const FRAMEOPTION_GROUPS: i32 = 0x00008;
/// `FRAMEOPTION_BETWEEN` — BETWEEN given?
pub const FRAMEOPTION_BETWEEN: i32 = 0x00010;
/// `FRAMEOPTION_START_UNBOUNDED_PRECEDING`.
pub const FRAMEOPTION_START_UNBOUNDED_PRECEDING: i32 = 0x00020;
/// `FRAMEOPTION_END_UNBOUNDED_PRECEDING`.
pub const FRAMEOPTION_END_UNBOUNDED_PRECEDING: i32 = 0x00040;
/// `FRAMEOPTION_START_UNBOUNDED_FOLLOWING`.
pub const FRAMEOPTION_START_UNBOUNDED_FOLLOWING: i32 = 0x00080;
/// `FRAMEOPTION_END_UNBOUNDED_FOLLOWING`.
pub const FRAMEOPTION_END_UNBOUNDED_FOLLOWING: i32 = 0x00100;
/// `FRAMEOPTION_START_CURRENT_ROW`.
pub const FRAMEOPTION_START_CURRENT_ROW: i32 = 0x00200;
/// `FRAMEOPTION_END_CURRENT_ROW`.
pub const FRAMEOPTION_END_CURRENT_ROW: i32 = 0x00400;
/// `FRAMEOPTION_START_OFFSET_PRECEDING`.
pub const FRAMEOPTION_START_OFFSET_PRECEDING: i32 = 0x00800;
/// `FRAMEOPTION_END_OFFSET_PRECEDING`.
pub const FRAMEOPTION_END_OFFSET_PRECEDING: i32 = 0x01000;
/// `FRAMEOPTION_START_OFFSET_FOLLOWING`.
pub const FRAMEOPTION_START_OFFSET_FOLLOWING: i32 = 0x02000;
/// `FRAMEOPTION_END_OFFSET_FOLLOWING`.
pub const FRAMEOPTION_END_OFFSET_FOLLOWING: i32 = 0x04000;
/// `FRAMEOPTION_EXCLUDE_CURRENT_ROW`.
pub const FRAMEOPTION_EXCLUDE_CURRENT_ROW: i32 = 0x08000;
/// `FRAMEOPTION_EXCLUDE_GROUP`.
pub const FRAMEOPTION_EXCLUDE_GROUP: i32 = 0x10000;
/// `FRAMEOPTION_EXCLUDE_TIES`.
pub const FRAMEOPTION_EXCLUDE_TIES: i32 = 0x20000;

/// `FRAMEOPTION_START_OFFSET` — START PRECEDING or FOLLOWING by an offset.
pub const FRAMEOPTION_START_OFFSET: i32 =
    FRAMEOPTION_START_OFFSET_PRECEDING | FRAMEOPTION_START_OFFSET_FOLLOWING;
/// `FRAMEOPTION_END_OFFSET` — END PRECEDING or FOLLOWING by an offset.
pub const FRAMEOPTION_END_OFFSET: i32 =
    FRAMEOPTION_END_OFFSET_PRECEDING | FRAMEOPTION_END_OFFSET_FOLLOWING;
/// `FRAMEOPTION_EXCLUSION` — any EXCLUDE option.
pub const FRAMEOPTION_EXCLUSION: i32 =
    FRAMEOPTION_EXCLUDE_CURRENT_ROW | FRAMEOPTION_EXCLUDE_GROUP | FRAMEOPTION_EXCLUDE_TIES;

/// `FRAMEOPTION_DEFAULTS` — the default frame: RANGE BETWEEN UNBOUNDED
/// PRECEDING AND CURRENT ROW.
pub const FRAMEOPTION_DEFAULTS: i32 =
    FRAMEOPTION_RANGE | FRAMEOPTION_START_UNBOUNDED_PRECEDING | FRAMEOPTION_END_CURRENT_ROW;

/// `WINDOW_SEEK_CURRENT` (windowapi.h).
pub const WINDOW_SEEK_CURRENT: i32 = 0;
/// `WINDOW_SEEK_HEAD` (windowapi.h).
pub const WINDOW_SEEK_HEAD: i32 = 1;
/// `WINDOW_SEEK_TAIL` (windowapi.h).
pub const WINDOW_SEEK_TAIL: i32 = 2;

// ---------------------------------------------------------------------------
// WindowAggStatus (executor/nodeWindowAgg.h)
// ---------------------------------------------------------------------------

/// `WindowAggStatus` (executor/nodeWindowAgg.h) — run status of the node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum WindowAggStatus {
    /// `WINDOWAGG_DONE` — finished.
    Done,
    /// `WINDOWAGG_RUN` — normal processing of the WindowAgg.
    #[default]
    Run,
    /// `WINDOWAGG_PASSTHROUGH` — pass through tuples for upper WindowAgg.
    PassThrough,
    /// `WINDOWAGG_PASSTHROUGH_STRICT` — pass through but throw the tuples away.
    PassThroughStrict,
}

pub use WindowAggStatus::Done as WINDOWAGG_DONE;
pub use WindowAggStatus::PassThrough as WINDOWAGG_PASSTHROUGH;
pub use WindowAggStatus::PassThroughStrict as WINDOWAGG_PASSTHROUGH_STRICT;
pub use WindowAggStatus::Run as WINDOWAGG_RUN;

// ---------------------------------------------------------------------------
// WindowAgg plan node (nodes/plannodes.h)
// ---------------------------------------------------------------------------

/// `WindowAgg` (nodes/plannodes.h) — the window-aggregation plan node.
#[derive(Debug, Default)]
pub struct WindowAgg<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `char *winname` — name of `WindowClause` implemented by this node.
    pub winname: Option<PgString<'mcx>>,
    /// `Index winref` — ID referenced by window functions.
    pub winref: Index,
    /// `int partNumCols` — number of columns in partition clause.
    pub partNumCols: i32,
    /// `AttrNumber *partColIdx` — their indexes in the target list.
    pub partColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *partOperators` — equality operators for partition columns.
    pub partOperators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *partCollations` — collations for partition columns.
    pub partCollations: Option<PgVec<'mcx, Oid>>,
    /// `int ordNumCols` — number of columns in ordering clause.
    pub ordNumCols: i32,
    /// `AttrNumber *ordColIdx` — their indexes in the target list.
    pub ordColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *ordOperators` — equality operators for ordering columns.
    pub ordOperators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *ordCollations` — collations for ordering columns.
    pub ordCollations: Option<PgVec<'mcx, Oid>>,
    /// `int frameOptions` — frame_clause options, see WindowDef.
    pub frameOptions: i32,
    /// `Node *startOffset` — expression for starting bound, if any.
    pub startOffset: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `Node *endOffset` — expression for ending bound, if any.
    pub endOffset: Option<PgBox<'mcx, Expr<'mcx>>>,
    /// `List *runCondition` — qual to help short-circuit execution.
    pub runCondition: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `List *runConditionOrig` — runCondition for display in EXPLAIN.
    pub runConditionOrig: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `Oid startInRangeFunc` — in_range function for startOffset.
    pub startInRangeFunc: Oid,
    /// `Oid endInRangeFunc` — in_range function for endOffset.
    pub endInRangeFunc: Oid,
    /// `Oid inRangeColl` — collation for in_range tests.
    pub inRangeColl: Oid,
    /// `bool inRangeAsc` — use ASC sort order for in_range tests?
    pub inRangeAsc: bool,
    /// `bool inRangeNullsFirst` — nulls sort first for in_range tests?
    pub inRangeNullsFirst: bool,
    /// `bool topWindow` — false for all apart from the WindowAgg that's closest
    /// to the root of the plan.
    pub topWindow: bool,
}

impl WindowAgg<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<WindowAgg<'b>> {
        Ok(WindowAgg {
            plan: self.plan.clone_in(mcx)?,
            winname: match &self.winname {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            winref: self.winref,
            partNumCols: self.partNumCols,
            partColIdx: clone_opt_vec(mcx, &self.partColIdx)?,
            partOperators: clone_opt_vec(mcx, &self.partOperators)?,
            partCollations: clone_opt_vec(mcx, &self.partCollations)?,
            ordNumCols: self.ordNumCols,
            ordColIdx: clone_opt_vec(mcx, &self.ordColIdx)?,
            ordOperators: clone_opt_vec(mcx, &self.ordOperators)?,
            ordCollations: clone_opt_vec(mcx, &self.ordCollations)?,
            frameOptions: self.frameOptions,
            startOffset: clone_opt_box_expr(mcx, &self.startOffset)?,
            endOffset: clone_opt_box_expr(mcx, &self.endOffset)?,
            runCondition: clone_opt_expr_vec(mcx, &self.runCondition)?,
            runConditionOrig: clone_opt_expr_vec(mcx, &self.runConditionOrig)?,
            startInRangeFunc: self.startInRangeFunc,
            endInRangeFunc: self.endInRangeFunc,
            inRangeColl: self.inRangeColl,
            inRangeAsc: self.inRangeAsc,
            inRangeNullsFirst: self.inRangeNullsFirst,
            topWindow: self.topWindow,
        })
    }
}

fn clone_opt_vec<'b, T: Copy>(
    mcx: Mcx<'b>,
    src: &Option<PgVec<'_, T>>,
) -> PgResult<Option<PgVec<'b, T>>> {
    match src {
        None => Ok(None),
        Some(v) => {
            let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
            for x in v.iter() {
                out.push(*x);
            }
            Ok(Some(out))
        }
    }
}

fn clone_opt_expr_vec<'b>(
    mcx: Mcx<'b>,
    src: &Option<PgVec<'_, Expr<'_>>>,
) -> PgResult<Option<PgVec<'b, Expr<'b>>>> {
    match src {
        None => Ok(None),
        Some(v) => {
            let mut out = mcx::vec_with_capacity_in(mcx, v.len())?;
            for x in v.iter() {
                // Deep-copy via `clone_in`, not the derived `Expr::clone`
                // (which panics on a `SubPlan` arm).
                out.push(x.clone_in(mcx)?);
            }
            Ok(Some(out))
        }
    }
}

fn clone_opt_box_expr<'b>(
    mcx: Mcx<'b>,
    src: &Option<PgBox<'_, Expr<'_>>>,
) -> PgResult<Option<PgBox<'b, Expr<'b>>>> {
    match src {
        None => Ok(None),
        Some(b) => Ok(Some(mcx::alloc_in(mcx, (**b).clone_in(mcx)?)?)),
    }
}

// ---------------------------------------------------------------------------
// WindowFuncExprState (executor/execnodes.h)
// ---------------------------------------------------------------------------

/// `WindowFuncExprState` (executor/execnodes.h) — runtime state of one window
/// function expression.
#[derive(Debug, Default)]
pub struct WindowFuncExprState<'mcx> {
    /// `WindowFunc *wfunc` — the expression plan node.
    pub wfunc: Option<PgBox<'mcx, WindowFunc<'mcx>>>,
    /// `List *args` — ExprStates for argument expressions.
    pub args: Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>,
    /// `ExprState *aggfilter` — FILTER expression, if any.
    pub aggfilter: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `int wfuncno` — ordinal number of the window function in the WindowAgg
    /// node's result-array indexing.
    pub wfuncno: i32,
}

// ---------------------------------------------------------------------------
// WindowObjectData (executor/nodeWindowAgg.c, exposed via windowapi.h)
// ---------------------------------------------------------------------------

/// `WindowObjectData` (nodeWindowAgg.c) — the object passed to window functions
/// as `fcinfo->context`.
///
/// The C `winstate` back-pointer is NOT carried; nodeWindowAgg threads
/// `&mut WindowAggState` explicitly (as nodeMaterial threads `estate`).
#[derive(Debug, Default)]
pub struct WindowObjectData<'mcx> {
    /// `List *argstates` — ExprState trees for fn's arguments.
    pub argstates: Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>,
    /// `void *localmem` — WinGetPartitionLocalMemory's chunk, allocated in the
    /// partition context. `None` is the C `NULL` (no chunk yet / released).
    pub localmem: Option<PgVec<'mcx, u8>>,
    /// `int markptr` — tuplestore mark pointer for this fn (-1 if none).
    pub markptr: i32,
    /// `int readptr` — tuplestore read pointer for this fn (-1 if none).
    pub readptr: i32,
    /// `int64 markpos` — row that markptr is positioned on.
    pub markpos: i64,
    /// `int64 seekpos` — row that readptr is positioned on.
    pub seekpos: i64,
}

// ---------------------------------------------------------------------------
// WindowStatePerFuncData (nodeWindowAgg.c)
// ---------------------------------------------------------------------------

/// `WindowStatePerFuncData` (nodeWindowAgg.c) — per-window-function working
/// state.
#[derive(Debug, Default)]
pub struct WindowStatePerFuncData<'mcx> {
    /// `WindowFuncExprState *wfuncstate` — the expr/state node this is for.
    pub wfuncstate: Option<PgBox<'mcx, WindowFuncExprState<'mcx>>>,
    /// `WindowFunc *wfunc` — the WindowFunc plan node.
    pub wfunc: Option<PgBox<'mcx, WindowFunc<'mcx>>>,
    /// `int numArguments` — number of arguments.
    pub numArguments: i32,
    /// `FmgrInfo flinfo` — fmgr lookup data for window function.
    pub flinfo: FmgrInfo,
    /// `Oid winCollation` — collation derived for window function.
    pub winCollation: Oid,
    /// `int16 resulttypeLen`.
    pub resulttypeLen: i16,
    /// `bool resulttypeByVal`.
    pub resulttypeByVal: bool,
    /// `bool plain_agg` — is it just a plain aggregate function?
    pub plain_agg: bool,
    /// `int aggno` — if so, index of its WindowStatePerAggData.
    pub aggno: i32,
    /// `WindowObject winobj` — object used in window function API (only for real
    /// window functions; `None` for plain aggregates).
    pub winobj: Option<PgBox<'mcx, WindowObjectData<'mcx>>>,
}

// ---------------------------------------------------------------------------
// WindowStatePerAggData (nodeWindowAgg.c)
// ---------------------------------------------------------------------------

/// `WindowStatePerAggData` (nodeWindowAgg.c) — per-plain-aggregate state.
#[derive(Debug, Default)]
pub struct WindowStatePerAggData<'mcx> {
    /// `Oid transfn_oid`.
    pub transfn_oid: Oid,
    /// `Oid invtransfn_oid` — may be InvalidOid.
    pub invtransfn_oid: Oid,
    /// `Oid finalfn_oid` — may be InvalidOid.
    pub finalfn_oid: Oid,
    /// `FmgrInfo transfn`.
    pub transfn: FmgrInfo,
    /// `FmgrInfo invtransfn`.
    pub invtransfn: FmgrInfo,
    /// `FmgrInfo finalfn`.
    pub finalfn: FmgrInfo,
    /// `int numFinalArgs` — number of arguments to pass to finalfn.
    pub numFinalArgs: i32,
    /// `Datum initValue`.
    pub initValue: Datum<'mcx>,
    /// `bool initValueIsNull`.
    pub initValueIsNull: bool,
    /// `Datum resultValue` — cached value for current frame boundaries.
    pub resultValue: Datum<'mcx>,
    /// `bool resultValueIsNull`.
    pub resultValueIsNull: bool,
    /// `int16 inputtypeLen`.
    pub inputtypeLen: i16,
    /// `int16 resulttypeLen`.
    pub resulttypeLen: i16,
    /// `int16 transtypeLen`.
    pub transtypeLen: i16,
    /// `bool inputtypeByVal`.
    pub inputtypeByVal: bool,
    /// `bool resulttypeByVal`.
    pub resulttypeByVal: bool,
    /// `bool transtypeByVal`.
    pub transtypeByVal: bool,
    /// `int wfuncno` — index of associated WindowStatePerFuncData.
    pub wfuncno: i32,
    /// `MemoryContext aggcontext` — may be private, or winstate->aggcontext.
    pub aggcontext: Option<MemoryContext>,
    /// `Datum transValue` — current transition value.
    pub transValue: Datum<'mcx>,
    /// `bool transValueIsNull`.
    pub transValueIsNull: bool,
    /// `int64 transValueCount` — number of currently-aggregated rows.
    pub transValueCount: i64,
    /// `bool restart` — need to restart this agg in this cycle?
    pub restart: bool,
}

// ---------------------------------------------------------------------------
// WindowAggState (executor/execnodes.h)
// ---------------------------------------------------------------------------

/// `WindowAggState` (executor/execnodes.h) — runtime state of a WindowAgg node.
#[derive(Debug, Default)]
pub struct WindowAggState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,

    /// `List *funcs` — all WindowFunc nodes in targetlist (as runtime states).
    pub funcs: Option<PgVec<'mcx, PgBox<'mcx, WindowFuncExprState<'mcx>>>>,
    /// `int numfuncs` — total number of window functions.
    pub numfuncs: i32,
    /// `int numaggs` — number that are plain aggregates.
    pub numaggs: i32,

    /// `WindowStatePerFunc perfunc` — per-window-function information.
    pub perfunc: Option<PgVec<'mcx, WindowStatePerFuncData<'mcx>>>,
    /// `WindowStatePerAgg peragg` — per-plain-aggregate information.
    pub peragg: Option<PgVec<'mcx, WindowStatePerAggData<'mcx>>>,
    /// `ExprState *partEqfunction` — equality funcs for partition columns.
    pub partEqfunction: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *ordEqfunction` — equality funcs for ordering columns.
    pub ordEqfunction: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `Tuplestorestate *buffer` — stores rows of current partition.
    pub buffer: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `int current_ptr` — read pointer # for current row.
    pub current_ptr: i32,
    /// `int framehead_ptr` — read pointer # for frame head, if used.
    pub framehead_ptr: i32,
    /// `int frametail_ptr` — read pointer # for frame tail, if used.
    pub frametail_ptr: i32,
    /// `int grouptail_ptr` — read pointer # for group tail, if used.
    pub grouptail_ptr: i32,
    /// `int64 spooled_rows` — total # of rows in buffer.
    pub spooled_rows: i64,
    /// `int64 currentpos` — position of current row in partition.
    pub currentpos: i64,
    /// `int64 frameheadpos` — current frame head position.
    pub frameheadpos: i64,
    /// `int64 frametailpos` — current frame tail position (frame end+1).
    pub frametailpos: i64,
    /// `struct WindowObjectData *agg_winobj` — winobj for aggregate fetches.
    pub agg_winobj: Option<PgBox<'mcx, WindowObjectData<'mcx>>>,
    /// `int64 aggregatedbase` — start row for current aggregates.
    pub aggregatedbase: i64,
    /// `int64 aggregatedupto` — rows before this one are aggregated.
    pub aggregatedupto: i64,
    /// `WindowAggStatus status` — run status of WindowAggState.
    pub status: WindowAggStatus,

    /// `int frameOptions` — frame_clause options, see WindowDef.
    pub frameOptions: i32,
    /// `ExprState *startOffset` — expression for starting bound offset.
    pub startOffset: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *endOffset` — expression for ending bound offset.
    pub endOffset: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `Datum startOffsetValue` — result of startOffset evaluation.
    pub startOffsetValue: Datum<'mcx>,
    /// `Datum endOffsetValue` — result of endOffset evaluation.
    pub endOffsetValue: Datum<'mcx>,

    /// `FmgrInfo startInRangeFunc` — in_range function for startOffset.
    pub startInRangeFunc: FmgrInfo,
    /// `FmgrInfo endInRangeFunc` — in_range function for endOffset.
    pub endInRangeFunc: FmgrInfo,
    /// `Oid inRangeColl` — collation for in_range tests.
    pub inRangeColl: Oid,
    /// `bool inRangeAsc` — use ASC sort order for in_range tests?
    pub inRangeAsc: bool,
    /// `bool inRangeNullsFirst` — nulls sort first for in_range tests?
    pub inRangeNullsFirst: bool,

    /// `bool use_pass_through` — when false, stop execution when runcondition
    /// is no longer true; else just stop evaluating window funcs.
    pub use_pass_through: bool,
    /// `bool top_window` — true if this is the top-most WindowAgg or the only
    /// WindowAgg in this query level.
    pub top_window: bool,
    /// `ExprState *runcondition` — condition which must remain true; `None` if
    /// none.
    pub runcondition: Option<PgBox<'mcx, ExprState<'mcx>>>,

    /// `int64 currentgroup` — peer group # of current row in partition.
    pub currentgroup: i64,
    /// `int64 frameheadgroup` — peer group # of frame head row.
    pub frameheadgroup: i64,
    /// `int64 frametailgroup` — peer group # of frame tail row.
    pub frametailgroup: i64,
    /// `int64 groupheadpos` — current row's peer group head position.
    pub groupheadpos: i64,
    /// `int64 grouptailpos` — peer group tail position (group end+1).
    pub grouptailpos: i64,

    /// `MemoryContext partcontext` — context for partition-lifespan data.
    pub partcontext: Option<MemoryContext>,
    /// `MemoryContext aggcontext` — shared context for aggregate working data.
    pub aggcontext: Option<MemoryContext>,
    /// `MemoryContext curaggcontext` — current aggregate's working data.
    pub curaggcontext: Option<MemoryContext>,
    /// `ExprContext *tmpcontext` — short-term evaluation context (id into the
    /// EState pool; the C aliases a second ExecAssignExprContext context).
    pub tmpcontext: Option<EcxtId>,

    /// `bool all_first` — true if the scan is starting.
    pub all_first: bool,
    /// `bool partition_spooled` — true if all tuples in current partition have
    /// been spooled into tuplestore.
    pub partition_spooled: bool,
    /// `bool next_partition` — true if begin_partition needs to be called.
    pub next_partition: bool,
    /// `bool more_partitions` — true if there's more partitions after this one.
    pub more_partitions: bool,
    /// `bool framehead_valid` — true if frameheadpos is up to date.
    pub framehead_valid: bool,
    /// `bool frametail_valid` — true if frametailpos is up to date.
    pub frametail_valid: bool,
    /// `bool grouptail_valid` — true if grouptailpos is up to date.
    pub grouptail_valid: bool,

    /// `TupleTableSlot *first_part_slot` — first tuple of current or next
    /// partition.
    pub first_part_slot: Option<SlotId>,
    /// `TupleTableSlot *framehead_slot` — first tuple of current frame.
    pub framehead_slot: Option<SlotId>,
    /// `TupleTableSlot *frametail_slot` — first tuple after current frame.
    pub frametail_slot: Option<SlotId>,

    /// `TupleTableSlot *agg_row_slot`.
    pub agg_row_slot: Option<SlotId>,
    /// `TupleTableSlot *temp_slot_1`.
    pub temp_slot_1: Option<SlotId>,
    /// `TupleTableSlot *temp_slot_2`.
    pub temp_slot_2: Option<SlotId>,
}
