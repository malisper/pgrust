//! SetOp node vocabulary (`nodes/plannodes.h`, `nodes/execnodes.h`,
//! `nodes/nodes.h`, `executor/nodeSetOp.c`), trimmed.
//!
//! INTERSECT/EXCEPT plan node and its executor state. The `SetOpCmd` and
//! `SetOpStrategy` enums live in `nodes.h`; the `SetOp` plan node embeds the
//! abstract `Plan` base; `SetOpState` embeds the `PlanState` head.

use mcx::{slice_in, Mcx, MemoryContext};
use types_error::PgResult;
use types_core::primitive::{AttrNumber, Oid};
use types_core::fmgr::FmgrInfo;
use types_sortsupport::SortSupportData;
use types_execgrouping::{TupleHashIterator, TupleHashTable};

use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;

/// `SetOpCmd` (`nodes/nodes.h`) — what a SetOp node should do.
///
/// ```c
/// typedef enum SetOpCmd {
///     SETOPCMD_INTERSECT,
///     SETOPCMD_INTERSECT_ALL,
///     SETOPCMD_EXCEPT,
///     SETOPCMD_EXCEPT_ALL,
/// } SetOpCmd;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum SetOpCmd {
    Intersect = 0,
    IntersectAll = 1,
    Except = 2,
    ExceptAll = 3,
}

/// `SetOpStrategy` (`nodes/nodes.h`) — how a SetOp node should do it.
///
/// ```c
/// typedef enum SetOpStrategy {
///     SETOP_SORTED,   /* input must be sorted */
///     SETOP_HASHED,   /* use internal hashtable */
/// } SetOpStrategy;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum SetOpStrategy {
    Sorted = 0,
    Hashed = 1,
}

/// `SetOp` plan node (`nodes/plannodes.h`):
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
#[derive(Debug)]
pub struct SetOp<'mcx> {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: Plan<'mcx>,
    /// `SetOpCmd cmd` — what to do.
    pub cmd: SetOpCmd,
    /// `SetOpStrategy strategy` — how to do it.
    pub strategy: SetOpStrategy,
    /// `int numCols` — number of columns to compare.
    pub numCols: i32,
    /// `AttrNumber *cmpColIdx` — their indexes in the target list.
    pub cmpColIdx: mcx::PgVec<'mcx, AttrNumber>,
    /// `Oid *cmpOperators` — comparison operators (equality or sort operators).
    pub cmpOperators: mcx::PgVec<'mcx, Oid>,
    /// `Oid *cmpCollations` — collations for the comparisons.
    pub cmpCollations: mcx::PgVec<'mcx, Oid>,
    /// `bool *cmpNullsFirst` — nulls-first flags if sorting, else uninteresting.
    pub cmpNullsFirst: mcx::PgVec<'mcx, bool>,
    /// `long numGroups` — estimated number of groups in the left input.
    pub numGroups: i64,
}

impl Default for SetOpCmd {
    fn default() -> Self {
        SetOpCmd::Intersect
    }
}

impl Default for SetOpStrategy {
    fn default() -> Self {
        SetOpStrategy::Sorted
    }
}

impl SetOp<'_> {
    /// Deep copy of the SetOp node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SetOp<'b>> {
        Ok(SetOp {
            plan: self.plan.clone_in(mcx)?,
            cmd: self.cmd,
            strategy: self.strategy,
            numCols: self.numCols,
            cmpColIdx: slice_in(mcx, &self.cmpColIdx)?,
            cmpOperators: slice_in(mcx, &self.cmpOperators)?,
            cmpCollations: slice_in(mcx, &self.cmpCollations)?,
            cmpNullsFirst: slice_in(mcx, &self.cmpNullsFirst)?,
            numGroups: self.numGroups,
        })
    }
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SetOpStatePerGroupData {
    /// `int64 numLeft` — number of left-input dups in group.
    pub numLeft: i64,
    /// `int64 numRight` — number of right-input dups in group.
    pub numRight: i64,
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
#[derive(Clone, Copy, Debug, Default)]
pub struct SetOpStatePerInput {
    /// `TupleTableSlot *firstTupleSlot` — first tuple of current group (slot id).
    pub firstTupleSlot: Option<SlotId>,
    /// `int64 numTuples` — number of tuples in current group.
    pub numTuples: i64,
    /// `TupleTableSlot *nextTupleSlot` — next input tuple, if already read
    /// (slot id; C's pointer alias of the child's returned slot).
    pub nextTupleSlot: Option<SlotId>,
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
///     SortSupport sortKeys;           /* per-grouping-field sort data */
///     SetOpStatePerInput leftInput;   /* current outer-relation input state */
///     SetOpStatePerInput rightInput;  /* current inner-relation input state */
///     bool        need_init;          /* have we read the first tuples yet? */
///     Oid        *eqfuncoids;         /* per-grouping-field equality fns */
///     FmgrInfo   *hashfunctions;      /* per-grouping-field hash fns */
///     TupleHashTable hashtable;       /* hash table with one entry per group */
///     MemoryContext tableContext;     /* memory context containing hash table */
///     bool        table_filled;       /* hash table filled yet? */
///     TupleHashIterator hashiter;     /* for iterating through hash table */
/// } SetOpState;
/// ```
///
/// The C `SortSupport sortKeys` / `Oid *eqfuncoids` / `FmgrInfo *hashfunctions`
/// `palloc`'d arrays become counted [`mcx::PgVec`]s; `MemoryContext
/// tableContext` is an owned child [`mcx::MemoryContext`] (`None` = the C
/// `NULL`), reset/deleted via the execGrouping seams.
#[derive(Debug)]
pub struct SetOpStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `bool setop_done` — indicates completion of output scan.
    pub setop_done: bool,
    /// `int64 numOutput` — number of dups left to output.
    pub numOutput: i64,
    /// `int numCols` — number of grouping columns.
    pub numCols: i32,
    /// `SortSupport sortKeys` — per-grouping-field sort data (`SETOP_SORTED`).
    pub sortKeys: mcx::PgVec<'mcx, SortSupportData<'mcx>>,
    /// `SetOpStatePerInput leftInput` — current outer-relation input state.
    pub leftInput: SetOpStatePerInput,
    /// `SetOpStatePerInput rightInput` — current inner-relation input state.
    pub rightInput: SetOpStatePerInput,
    /// `bool need_init` — have we read the first tuples yet?
    pub need_init: bool,
    /// `Oid *eqfuncoids` — per-grouping-field equality fns (`SETOP_HASHED`).
    pub eqfuncoids: mcx::PgVec<'mcx, Oid>,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash fns (`SETOP_HASHED`).
    pub hashfunctions: mcx::PgVec<'mcx, FmgrInfo>,
    /// `TupleHashTable hashtable` — hash table with one entry per group.
    pub hashtable: TupleHashTable,
    /// `MemoryContext tableContext` — memory context containing the hash table
    /// (`None` = the C `NULL`). An owned child context (drop =
    /// `MemoryContextDelete`); reset = [`MemoryContext::reset`].
    pub tableContext: Option<MemoryContext>,
    /// `bool table_filled` — hash table filled yet?
    pub table_filled: bool,
    /// `TupleHashIterator hashiter` — for iterating through the hash table.
    pub hashiter: TupleHashIterator,
}
