//! Unique node vocabulary (nodes/nodes.h, nodes/plannodes.h,
//! executor/execnodes.h).
//!
//! The `Unique` plan node and the `UniqueState` executor state consumed by
//! `nodeUnique.c`.

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;

use crate::execexpr::ExprState;
use crate::execnodes::PlanStateData;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

/// `T_Unique` (nodes/nodetags.h) — value verified against PostgreSQL 18.3.
pub const T_Unique: NodeTag = NodeTag(367);

/// `T_UniqueState` (nodes/nodetags.h) — value verified against PostgreSQL 18.3.
pub const T_UniqueState: NodeTag = NodeTag(431);

/// `Unique` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct Unique
/// {
///     Plan        plan;
///     int         numCols;        /* number of columns to check for uniqueness */
///     AttrNumber *uniqColIdx;     /* their indexes in the target list */
///     Oid        *uniqOperators;  /* equality operators to compare with */
///     Oid        *uniqCollations; /* collations for equality comparisons */
/// } Unique;
/// ```
#[derive(Debug, Default)]
pub struct Unique<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `int numCols` — number of columns to check for uniqueness.
    pub numCols: i32,
    /// `AttrNumber *uniqColIdx` — their indexes in the target list (`None` is
    /// the C `NULL`, used when `numCols == 0`).
    pub uniqColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *uniqOperators` — equality operators to compare with.
    pub uniqOperators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *uniqCollations` — collations for equality comparisons.
    pub uniqCollations: Option<PgVec<'mcx, Oid>>,
}

impl Unique<'_> {
    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Unique<'b>> {
        Ok(Unique {
            plan: self.plan.clone_in(mcx)?,
            numCols: self.numCols,
            uniqColIdx: clone_vec(&self.uniqColIdx, mcx)?,
            uniqOperators: clone_vec(&self.uniqOperators, mcx)?,
            uniqCollations: clone_vec(&self.uniqCollations, mcx)?,
        })
    }
}

/// Deep-copy a `Option<PgVec<T>>` of `Copy` scalars into `mcx`.
fn clone_vec<'b, T: Copy>(
    src: &Option<PgVec<'_, T>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, T>>> {
    match src {
        Some(v) => {
            let mut out = vec_with_capacity_in(mcx, v.len())?;
            for x in v.iter() {
                out.push(*x);
            }
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

/// `UniqueState` (executor/execnodes.h):
///
/// ```c
/// typedef struct UniqueState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     ExprState  *eqfunction;     /* tuple equality qual */
/// } UniqueState;
/// ```
#[derive(Debug, Default)]
pub struct UniqueStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `ExprState *eqfunction` — tuple equality qual.
    pub eqfunction: Option<PgBox<'mcx, ExprState<'mcx>>>,
}
