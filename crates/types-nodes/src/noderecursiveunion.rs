//! RecursiveUnion plan-node / executor-state vocabulary
//! (`nodes/plannodes.h`, `nodes/nodes.h`, `executor/execnodes.h`,
//! `executor/nodeRecursiveunion.c`).
//!
//! A `RecursiveUnion` node implements `WITH RECURSIVE`: it evaluates the
//! non-recursive (outer) term once, then repeatedly evaluates the recursive
//! (inner) term against a *working table* until the term produces no new rows.
//! Each iteration's output is stashed in an *intermediate table* which then
//! becomes the next working table. For `UNION` (without `ALL`) a tuple hash
//! table of already-seen tuples filters duplicates. The recursive term reads
//! the working table through a `WorkTableScan` node that finds this
//! `RecursiveUnionState` via the `wtParam` `Param` slot.

extern crate alloc;

use alloc::boxed::Box;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, MemoryContext, PgBox, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;

use crate::execnodes::PlanStateData;
use crate::funcapi::Tuplestorestate;
use crate::nodeagg::TupleHashTable;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

// ===========================================================================
// NodeTags (nodes/nodetags.h, PostgreSQL 18.3 generated order).

/// `T_RecursiveUnion` (nodetags.h = 336) — the plan node.
pub const T_RecursiveUnion: NodeTag = NodeTag(336);

/// `T_RecursiveUnionState` (nodetags.h = 399) — the executor state node.
pub const T_RecursiveUnionState: NodeTag = NodeTag(399);

/// `RecursiveUnion` plan node (plannodes.h):
///
/// ```c
/// typedef struct RecursiveUnion {
///     Plan        plan;
///     int         wtParam;        /* ID of Param representing work table */
///     int         numCols;        /* number of columns to check for
///                                  * duplicate-ness */
///     AttrNumber *dupColIdx;      /* their indexes in the target list */
///     Oid        *dupOperators;   /* equality operators to compare with */
///     Oid        *dupCollations;
///     long        numGroups;      /* estimated number of groups in input */
/// } RecursiveUnion;
/// ```
///
/// The three `pg_node_attr(array_size(numCols))` arrays are `numCols` long; the
/// owned model carries them as `PgVec`s.
#[derive(Debug)]
pub struct RecursiveUnion<'mcx> {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: Plan<'mcx>,
    /// `int wtParam` — ID of the `Param` representing the work table.
    pub wtParam: i32,
    /// `int numCols` — number of columns to check for duplicate-ness (0 in the
    /// `UNION ALL` case).
    pub numCols: i32,
    /// `AttrNumber *dupColIdx` — indexes of the grouping columns in the target
    /// list.
    pub dupColIdx: PgVec<'mcx, AttrNumber>,
    /// `Oid *dupOperators` — equality operators to compare with.
    pub dupOperators: PgVec<'mcx, Oid>,
    /// `Oid *dupCollations` — collations for the equality comparisons.
    pub dupCollations: PgVec<'mcx, Oid>,
    /// `long numGroups` — estimated number of groups in the input.
    pub numGroups: i64,
}

impl RecursiveUnion<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RecursiveUnion<'b>> {
        Ok(RecursiveUnion {
            plan: self.plan.clone_in(mcx)?,
            wtParam: self.wtParam,
            numCols: self.numCols,
            dupColIdx: copy_vec(mcx, &self.dupColIdx)?,
            dupOperators: copy_vec(mcx, &self.dupOperators)?,
            dupCollations: copy_vec(mcx, &self.dupCollations)?,
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
// RecursiveUnion executor state (executor/execnodes.h,
// executor/nodeRecursiveunion.c).
// ===========================================================================

/// `RecursiveUnionState` (execnodes.h):
///
/// ```c
/// typedef struct RecursiveUnionState {
///     PlanState   ps;             /* its first field is NodeTag */
///     bool        recursing;
///     bool        intermediate_empty;
///     Tuplestorestate *working_table;
///     Tuplestorestate *intermediate_table;
///     Oid        *eqfuncoids;     /* per-grouping-field equality fns */
///     FmgrInfo   *hashfunctions;  /* per-grouping-field hash fns */
///     MemoryContext tempContext;  /* short-term context for comparisons */
///     TupleHashTable hashtable;   /* hash table for tuples already seen */
///     MemoryContext tableContext; /* memory context containing hash table */
/// } RecursiveUnionState;
/// ```
#[derive(Debug)]
pub struct RecursiveUnionStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `bool recursing` — are we in the recursive (phase-2) loop yet?
    pub recursing: bool,
    /// `bool intermediate_empty` — nothing stashed in the intermediate table?
    pub intermediate_empty: bool,
    /// `Tuplestorestate *working_table` — the current working table (WT).
    pub working_table: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `Tuplestorestate *intermediate_table` — accumulates this iteration's rows.
    pub intermediate_table: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `Oid *eqfuncoids` — per-grouping-field equality functions (UNION only).
    pub eqfuncoids: PgVec<'mcx, Oid>,
    /// `FmgrInfo *hashfunctions` — per-grouping-field hash functions (UNION only).
    pub hashfunctions: PgVec<'mcx, FmgrInfo>,
    /// `MemoryContext tempContext` — short-term context for comparisons.
    /// `mcx::MemoryContext` owns its allocation domain and resets on drop, so
    /// `MemoryContextDelete`/`Reset` are native (drop / `reset`).
    pub tempContext: Option<MemoryContext>,
    /// `TupleHashTable hashtable` — hash table for tuples already seen.
    pub hashtable: Option<Box<TupleHashTable<'mcx>>>,
    /// `MemoryContext tableContext` — memory context containing the hash table.
    pub tableContext: Option<MemoryContext>,
}

impl<'mcx> RecursiveUnionStateData<'mcx> {
    /// `makeNode(RecursiveUnionState)` — a zeroed state whose `PgVec`s are
    /// anchored to `mcx` (the per-query context the state tree lives in). Every
    /// field starts at its zero/empty value; `ExecInitRecursiveUnion` fills the
    /// rest.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        RecursiveUnionStateData {
            ps: PlanStateData::default(),
            recursing: false,
            intermediate_empty: true,
            working_table: None,
            intermediate_table: None,
            eqfuncoids: PgVec::new_in(mcx),
            hashfunctions: PgVec::new_in(mcx),
            tempContext: None,
            hashtable: None,
            tableContext: None,
        }
    }

    /// Allocate a fresh zeroed `RecursiveUnionState` in `mcx`
    /// (C: `makeNode(RecursiveUnionState)`).
    pub fn alloc_in(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, RecursiveUnionStateData<'mcx>>> {
        alloc_in(mcx, RecursiveUnionStateData::new_in(mcx))
    }
}
