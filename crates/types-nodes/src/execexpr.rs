//! Expression-evaluation vocabulary (executor/execExpr.h), trimmed.

use mcx::PgBox;

use crate::planstate::PlanStateNode;

/// `ProjectionInfo` (execnodes.h) — node for caching needed info for
/// projection. Trimmed: ports so far only set/test a `ProjectionInfo *` for
/// NULL-ness (`ps_ProjInfo`); the expression machinery stays with its owning
/// unit when it lands.
#[derive(Clone, Debug, Default)]
pub struct ProjectionInfo;

/// `SubPlanState` (execnodes.h) — executor state for a subplan, trimmed to
/// the fields the `ExecReScan` walk consumes (`sstate->planstate`); the
/// expression/hash fields arrive with the nodeSubplan owner. The C `parent`
/// back-pointer is not carried: callers thread the parent state explicitly.
#[derive(Debug, Default)]
pub struct SubPlanState<'mcx> {
    /// `PlanState *planstate` — the subselect plan's state tree.
    pub planstate: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
}
