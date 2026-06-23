//! Group plan-node / executor-state vocabulary (`nodes/plannodes.h`,
//! `nodes/nodes.h`, `executor/execnodes.h`, `executor/nodeGroup.c`).
//!
//! A `Group` node implements `GROUP BY` without aggregates: its outer plan
//! delivers tuples sorted by the grouping columns (so tuples of the same group
//! are consecutive), and the node compares adjacent tuples to find group
//! boundaries, returning one projected tuple per group that passes the `HAVING`
//! qual. `GroupState` is the executor state.

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::primitive::{AttrNumber, Oid};
use ::types_error::PgResult;

use crate::execexpr::ExprState;
use crate::execnodes::ScanStateData;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

// ===========================================================================
// NodeTags (nodes/nodetags.h, PostgreSQL 18.3 generated order).
// ===========================================================================

/// `T_Group` — the Group plan-node tag. Verified against the PostgreSQL 18.3
/// c2rust rendering of nodeGroup.c (`T_Group = 364`).
pub const T_Group: NodeTag = NodeTag(364);
/// `T_GroupState` — the Group executor-state node tag. Verified against the
/// PostgreSQL 18.3 c2rust rendering of nodeGroup.c (`T_GroupState = 428`).
pub const T_GroupState: NodeTag = NodeTag(428);

// ===========================================================================
// Group plan node (nodes/plannodes.h).
// ===========================================================================

/// `Group` plan node (plannodes.h):
///
/// ```c
/// typedef struct Group
/// {
///     Plan        plan;
///     int         numCols;        /* number of grouping columns */
///     AttrNumber *grpColIdx;      /* their indexes in the target list */
///     Oid        *grpOperators;   /* equality operators to compare with */
///     Oid        *grpCollations;
/// } Group;
/// ```
///
/// The three `pg_node_attr(array_size(numCols))` arrays are `numCols` long; the
/// owned model carries them as `PgVec`s.
#[derive(Debug)]
pub struct Group<'mcx> {
    /// `Plan plan` — the abstract plan-node base (its first field is `NodeTag`).
    pub plan: Plan<'mcx>,
    /// `int numCols` — number of grouping columns.
    pub numCols: i32,
    /// `AttrNumber *grpColIdx` — their indexes in the target list.
    pub grpColIdx: PgVec<'mcx, AttrNumber>,
    /// `Oid *grpOperators` — equality operators to compare with.
    pub grpOperators: PgVec<'mcx, Oid>,
    /// `Oid *grpCollations` — collations for the equality comparisons.
    pub grpCollations: PgVec<'mcx, Oid>,
}

impl Group<'_> {
    /// `nodeTag(node)` — always `T_Group`.
    pub fn tag(&self) -> NodeTag {
        T_Group
    }

    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Group<'b>> {
        Ok(Group {
            plan: self.plan.clone_in(mcx)?,
            numCols: self.numCols,
            grpColIdx: copy_vec(mcx, &self.grpColIdx)?,
            grpOperators: copy_vec(mcx, &self.grpOperators)?,
            grpCollations: copy_vec(mcx, &self.grpCollations)?,
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
// Group executor state (executor/execnodes.h).
// ===========================================================================

/// `GroupState` (execnodes.h):
///
/// ```c
/// typedef struct GroupState
/// {
///     ScanState   ss;             /* its first field is NodeTag */
///     ExprState  *eqfunction;     /* equality function */
///     bool        grp_done;       /* indicates completion of Group scan */
/// } GroupState;
/// ```
#[derive(Debug, Default)]
pub struct GroupStateData<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `ExprState *eqfunction` — equality function (`None` = the C `NULL`).
    pub eqfunction: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `bool grp_done` — indicates completion of the Group scan.
    pub grp_done: bool,
}
