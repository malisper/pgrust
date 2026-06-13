//! SetOp plan-node / executor-state vocabulary (`nodes/plannodes.h`,
//! `nodes/nodes.h`, `executor/execnodes.h`, `executor/nodeSetOp.c`), plus the
//! public `TupleHashTable` family the SetOp/Agg/Subplan grouping nodes embed.
//!
//! A `SetOp` node implements INTERSECT / EXCEPT (UNION/UNION ALL are cheaper and
//! use other nodes). In `SETOP_SORTED` mode the per-group counts come from a
//! merge over the two sorted inputs (`SetOpStatePerInput` cursors); in
//! `SETOP_HASHED` mode the `SetOpStatePerGroupData` lives in each tuple-hash
//! entry's MAXALIGN'd "additional space".
//!
//! # `TupleHashTable` family placement
//!
//! `executor/execnodes.h` exposes `TupleHashTableData` / `TupleHashEntryData` /
//! `TupleHashIterator` as public types (the SetOp/Agg state structs embed them
//! by value/pointer). They are owned by the `execGrouping.c` subsystem, which is
//! not yet ported; only the underlying `tuplehash` simplehash bucket array is
//! execGrouping-internal (modelled here as [`crate::execnodes::Opaque`]). They
//! live in this module (the lowest types crate that stays acyclic) so SetOp can
//! embed them and the execGrouping seam can name them; when a sibling grouping
//! node (Agg) lands it re-uses these same definitions.

extern crate alloc;

use alloc::boxed::Box;

use mcx::{alloc_in, vec_with_capacity_in, MemoryContext, Mcx, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_sortsupport::SortSupportData;
use types_tuple::heaptuple::MinimalTuple;

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, Opaque, PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

// ===========================================================================
// NodeTags (nodes/nodetags.h, PostgreSQL 18.3 generated order).
// ===========================================================================

/// `T_SetOp` — the SetOp plan node tag.
pub const T_SetOp: NodeTag = NodeTag(371);
/// `T_SetOpState` — the SetOp executor-state node tag.
pub const T_SetOpState: NodeTag = NodeTag(435);

// ===========================================================================
// SetOpCmd / SetOpStrategy (nodes/nodes.h).
// ===========================================================================

/// `SetOpCmd` (nodes.h) — what a `SetOp` node does. Stored as the C enum's
/// underlying integer.
pub type SetOpCmd = i32;

/// `SETOPCMD_INTERSECT`.
pub const SETOPCMD_INTERSECT: SetOpCmd = 0;
/// `SETOPCMD_INTERSECT_ALL`.
pub const SETOPCMD_INTERSECT_ALL: SetOpCmd = 1;
/// `SETOPCMD_EXCEPT`.
pub const SETOPCMD_EXCEPT: SetOpCmd = 2;
/// `SETOPCMD_EXCEPT_ALL`.
pub const SETOPCMD_EXCEPT_ALL: SetOpCmd = 3;

/// `SetOpStrategy` (nodes.h) — how a `SetOp` node does it.
pub type SetOpStrategy = i32;

/// `SETOP_SORTED` — input must be sorted.
pub const SETOP_SORTED: SetOpStrategy = 0;
/// `SETOP_HASHED` — use internal hash table.
pub const SETOP_HASHED: SetOpStrategy = 1;

// ===========================================================================
// TupleHashTable family (executor/execnodes.h) — public, owned by execGrouping.
// ===========================================================================

/// `TupleHashEntryData` (execnodes.h):
///
/// ```c
/// typedef struct TupleHashEntryData {
///     MinimalTuple firstTuple;   /* copy of first tuple in this group */
///     uint32       status;       /* hash status */
///     uint32       hash;         /* hash value (cached) */
/// } TupleHashEntryData;
/// ```
///
/// The MAXALIGN'd per-group "additional space" the C lays out after the entry
/// (`TupleHashEntryGetAdditional`) is owned by execGrouping; SetOp stores its
/// `SetOpStatePerGroupData` there.
#[derive(Debug, Default)]
pub struct TupleHashEntryData<'mcx> {
    /// `MinimalTuple firstTuple` — copy of first tuple in this group.
    pub firstTuple: MinimalTuple<'mcx>,
    /// `uint32 status` — hash status.
    pub status: u32,
    /// `uint32 hash` — cached hash value.
    pub hash: u32,
}

/// `TupleHashEntry` (execnodes.h) — `TupleHashEntryData *`.
pub type TupleHashEntry<'mcx> = Option<PgBox<'mcx, TupleHashEntryData<'mcx>>>;

/// `TupleHashTableData` (execnodes.h):
///
/// ```c
/// typedef struct TupleHashTableData {
///     tuplehash_hash *hashtab;
///     int          numCols;
///     AttrNumber  *keyColIdx;
///     ExprState   *tab_hash_expr;
///     ExprState   *tab_eq_func;
///     Oid         *tab_collations;
///     MemoryContext tablecxt;
///     MemoryContext tempcxt;
///     Size         additionalsize;
///     TupleTableSlot *tableslot;
///     /* transient per-search: */
///     TupleTableSlot *inputslot;
///     ExprState   *in_hash_expr;
///     ExprState   *cur_eq_func;
///     ExprContext *exprcontext;
/// } TupleHashTableData;
/// ```
///
/// `TupleHashTable` in C is `TupleHashTableData *`; the owned model carries it
/// by value (`Box`). The `tuplehash` simplehash bucket array (`hashtab`) is
/// execGrouping-internal and stays [`Opaque`] until that unit lands.
#[derive(Debug)]
pub struct TupleHashTable<'mcx> {
    /// `tuplehash_hash *hashtab` — the underlying simplehash; execGrouping
    /// owner-internal, so opaque until that unit lands.
    pub hashtab: Opaque,
    /// `int numCols` — number of columns in the lookup key.
    pub numCols: i32,
    /// `AttrNumber *keyColIdx` — attr numbers of key columns.
    pub keyColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `ExprState *tab_hash_expr` — ExprState for hashing table datatype(s).
    pub tab_hash_expr: Option<PgBox<'mcx, ExprState>>,
    /// `ExprState *tab_eq_func` — comparator for table datatype(s).
    pub tab_eq_func: Option<PgBox<'mcx, ExprState>>,
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
    pub in_hash_expr: Option<PgBox<'mcx, ExprState>>,
    /// `ExprState *cur_eq_func` — comparator for input vs. table (transient).
    pub cur_eq_func: Option<PgBox<'mcx, ExprState>>,
    /// `ExprContext *exprcontext` — expression context for the evaluations.
    pub exprcontext: Option<EcxtId>,
}

/// `TupleHashIterator` (execnodes.h) — iteration cursor over a
/// `TupleHashTable`. C is `tuplehash_iterator`; trimmed to the opaque cursor
/// word the iterate seams round-trip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TupleHashIterator {
    /// The opaque `tuplehash_iterator` cursor word.
    pub cur: usize,
}

// ===========================================================================
// SetOp plan node (nodes/plannodes.h).
// ===========================================================================

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
///
/// The four `pg_node_attr(array_size(numCols))` arrays are `numCols` long; the
/// owned model carries them as `PgVec`s.
#[derive(Debug)]
pub struct SetOp<'mcx> {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: Plan<'mcx>,
    /// `SetOpCmd cmd` — what to do (see [`SetOpCmd`]).
    pub cmd: SetOpCmd,
    /// `SetOpStrategy strategy` — how to do it (see [`SetOpStrategy`]).
    pub strategy: SetOpStrategy,
    /// `int numCols` — number of columns to compare.
    pub numCols: i32,
    /// `AttrNumber *cmpColIdx` — their indexes in the target list.
    pub cmpColIdx: PgVec<'mcx, AttrNumber>,
    /// `Oid *cmpOperators` — comparison operators (equality or sort operators).
    pub cmpOperators: PgVec<'mcx, Oid>,
    /// `Oid *cmpCollations` — collations for the comparisons.
    pub cmpCollations: PgVec<'mcx, Oid>,
    /// `bool *cmpNullsFirst` — nulls-first flags if sorting, else uninteresting.
    pub cmpNullsFirst: PgVec<'mcx, bool>,
    /// `long numGroups` — estimated number of groups in the left input.
    pub numGroups: i64,
}

impl SetOp<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SetOp<'b>> {
        Ok(SetOp {
            plan: self.plan.clone_in(mcx)?,
            cmd: self.cmd,
            strategy: self.strategy,
            numCols: self.numCols,
            cmpColIdx: copy_vec(mcx, &self.cmpColIdx)?,
            cmpOperators: copy_vec(mcx, &self.cmpOperators)?,
            cmpCollations: copy_vec(mcx, &self.cmpCollations)?,
            cmpNullsFirst: copy_vec(mcx, &self.cmpNullsFirst)?,
            numGroups: self.numGroups,
        })
    }
}

fn copy_vec<'b, T: Copy>(mcx: Mcx<'b>, src: &PgVec<'_, T>) -> PgResult<PgVec<'b, T>> {
    let mut out = vec_with_capacity_in(mcx, src.len())?;
    for &v in src.iter() {
        out.push(v);
    }
    Ok(out)
}

// ===========================================================================
// SetOp executor state (executor/execnodes.h, executor/nodeSetOp.c).
// ===========================================================================

/// `SetOpStatePerGroupData` (nodeSetOp.c) — per-group working state: how many
/// duplicates of each group arrived from each side.
///
/// ```c
/// typedef struct SetOpStatePerGroupData {
///     int64       numLeft;        /* number of left-input dups in group */
///     int64       numRight;       /* number of right-input dups in group */
/// } SetOpStatePerGroupData;
/// ```
///
/// In `SETOP_SORTED` mode this is just a local in `setop_retrieve_sorted`; in
/// `SETOP_HASHED` mode the tuple hash table stores one of these in each entry's
/// MAXALIGN'd additional space (`additionalsize = sizeof(SetOpStatePerGroupData)`).
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
    /// `TupleTableSlot *firstTupleSlot` — first tuple of current group (arena id).
    pub firstTupleSlot: Option<SlotId>,
    /// `int64 numTuples` — number of tuples in current group.
    pub numTuples: i64,
    /// `TupleTableSlot *nextTupleSlot` — next input tuple, if already read
    /// (arena id; C's pointer alias of the child's returned slot).
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
    pub sortKeys: PgVec<'mcx, SortSupportData<'mcx>>,
    /// `SetOpStatePerInput leftInput` — current outer-relation input state.
    pub leftInput: SetOpStatePerInput,
    /// `SetOpStatePerInput rightInput` — current inner-relation input state.
    pub rightInput: SetOpStatePerInput,
    /// `bool need_init` — have we read the first tuples yet?
    pub need_init: bool,
    /// `Oid *eqfuncoids` — per-grouping-field equality fns (`SETOP_HASHED`).
    pub eqfuncoids: PgVec<'mcx, Oid>,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash fns (`SETOP_HASHED`).
    pub hashfunctions: PgVec<'mcx, FmgrInfo>,
    /// `TupleHashTable hashtable` — hash table with one entry per group.
    pub hashtable: Option<Box<TupleHashTable<'mcx>>>,
    /// `MemoryContext tableContext` — memory context containing the hash table.
    /// `mcx::MemoryContext` owns its allocation domain and resets on drop, so
    /// `MemoryContextDelete`/`Reset` are native (drop / `reset`).
    pub tableContext: Option<MemoryContext>,
    /// `bool table_filled` — hash table filled yet?
    pub table_filled: bool,
    /// `TupleHashIterator hashiter` — for iterating through the hash table.
    pub hashiter: TupleHashIterator,
}

impl<'mcx> SetOpStateData<'mcx> {
    /// `makeNode(SetOpState)` — a zeroed `SetOpState` whose `PgVec`s are anchored
    /// to `mcx` (the per-query context the state tree lives in). Every field
    /// starts at its zero/empty value; `ExecInitSetOp` fills the rest.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        SetOpStateData {
            ps: PlanStateData::default(),
            setop_done: false,
            numOutput: 0,
            numCols: 0,
            sortKeys: PgVec::new_in(mcx),
            leftInput: SetOpStatePerInput::default(),
            rightInput: SetOpStatePerInput::default(),
            need_init: false,
            eqfuncoids: PgVec::new_in(mcx),
            hashfunctions: PgVec::new_in(mcx),
            hashtable: None,
            tableContext: None,
            table_filled: false,
            hashiter: TupleHashIterator::default(),
        }
    }

    /// Allocate a fresh zeroed `SetOpState` in `mcx` (C: `makeNode(SetOpState)`).
    pub fn alloc_in(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, SetOpStateData<'mcx>>> {
        alloc_in(mcx, SetOpStateData::new_in(mcx))
    }
}
