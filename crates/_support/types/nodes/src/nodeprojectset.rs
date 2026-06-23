//! ProjectSet plan-node / executor-state vocabulary (`nodes/plannodes.h`,
//! `executor/execnodes.h`, `executor/nodeProjectSet.c`).
//!
//! `ProjectSet` nodes are inserted by the planner to evaluate set-returning
//! functions in the targetlist; all SRFs are guaranteed to be directly at the
//! top level of the targetlist. `ProjectSetState` is the executor state.

use ::mcx::{MemoryContext, PgBox, PgVec};

use crate::execexpr::{ExprDoneCond, ExprState, SetExprState};
use crate::execnodes::PlanStateData;
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;

/// `T_ProjectSet` (nodes/nodetags.h) — the ProjectSet plan-node tag. Verified
/// against PostgreSQL 18.3's generated `nodetags.h` (value 332).
pub const T_ProjectSet: NodeTag = NodeTag(332);

/// `T_ProjectSetState` (nodes/nodetags.h) — the ProjectSet executor-state node
/// tag. Verified against PostgreSQL 18.3's generated `nodetags.h` (value 395).
pub const T_ProjectSetState: NodeTag = NodeTag(395);

/// `ProjectSet` plan node (plannodes.h):
///
/// ```c
/// typedef struct ProjectSet
/// {
///     Plan        plan;
/// } ProjectSet;
/// ```
#[derive(Debug, Default)]
pub struct ProjectSet<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
}

impl ProjectSet<'_> {
    /// `nodeTag(node)` — always [`T_ProjectSet`].
    #[inline]
    pub fn tag(&self) -> NodeTag {
        T_ProjectSet
    }

    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(
        &self,
        mcx: ::mcx::Mcx<'b>,
    ) -> types_error::PgResult<ProjectSet<'b>> {
        Ok(ProjectSet {
            plan: self.plan.clone_in(mcx)?,
        })
    }
}

/// One entry of `ProjectSetState.elems[]` (the C `Node **elems`).
///
/// In C each `elems[argno]` is a `Node *` distinguished at runtime by
/// `IsA(elem, SetExprState)`: a [`SetExprState`] produced by
/// `ExecInitFunctionResultSet` (the set-returning case) or a plain
/// [`ExprState`] produced by `ExecInitExpr`. The owned model makes that
/// tag-test the enum discriminant.
#[derive(Debug)]
pub enum ProjectSetElem<'mcx> {
    /// `IsA(elem, SetExprState)` — a compiled set-returning function/operator
    /// state (`ExecInitFunctionResultSet`).
    Srf(PgBox<'mcx, SetExprState<'mcx>>),
    /// A plain (non-set-returning) compiled expression (`ExecInitExpr`).
    Plain(PgBox<'mcx, ExprState<'mcx>>),
}

/// `ProjectSetState` (execnodes.h):
///
/// ```c
/// typedef struct ProjectSetState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     Node      **elems;          /* array of expression states */
///     ExprDoneCond *elemdone;     /* array of per-SRF is-done states */
///     int         nelems;         /* length of elemdone[] array */
///     bool        pending_srf_tuples; /* still evaluating srfs in tlist? */
///     MemoryContext argcontext;   /* context for SRF arguments */
/// } ProjectSetState;
/// ```
///
/// The outer child plan state (`outerPlanState(node)` == `ps.lefttree`) lives in
/// the embedded [`PlanStateData`] head, exactly as the C macro resolves it.
#[derive(Debug, Default)]
pub struct ProjectSetState<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `Node **elems` — array of compiled expression states (one per tlist
    /// entry).
    pub elems: Option<PgVec<'mcx, ProjectSetElem<'mcx>>>,
    /// `ExprDoneCond *elemdone` — array of per-SRF is-done states.
    pub elemdone: Option<PgVec<'mcx, ExprDoneCond>>,
    /// `int nelems` — length of the `elemdone[]` array.
    pub nelems: i32,
    /// `bool pending_srf_tuples` — still evaluating SRFs in tlist?
    pub pending_srf_tuples: bool,
    /// `MemoryContext argcontext` — context for SRF arguments (`None` = the C
    /// `NULL`, before `ExecInitProjectSet` creates it).
    pub argcontext: Option<MemoryContext>,
}

impl<'mcx> ProjectSetState<'mcx> {
    /// `&node->ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ps
    }

    /// `&mut node->ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ps
    }
}
