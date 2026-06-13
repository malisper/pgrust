//! Result node vocabulary (nodes/plannodes.h / executor/execnodes.h).
//!
//! `Result` is the plan node for queries needing constant-target evaluation
//! (e.g. `SELECT 1*2`, `INSERT ... VALUES`) and for hoisting a constant
//! ("one-time") qualification above a controlled subplan. `ResultState` is the
//! executor state.

use mcx::{Mcx, PgBox};
use types_error::PgResult;

use crate::execexpr::ExprState;
use crate::execnodes::PlanStateData;
use crate::nodes::{NodeTag, T_Result};
use crate::primnodes::Expr;

/// `T_ResultState` (nodes/nodetags.h) — the executor-state node tag for a
/// `ResultState`. Verified against PostgreSQL 18.3's generated `nodetags.h`
/// (value 394).
pub const T_ResultState: NodeTag = NodeTag(394);

/// `Result` plan node (plannodes.h):
///
/// ```c
/// typedef struct Result
/// {
///     Plan        plan;
///     Node       *resconstantqual;
/// } Result;
/// ```
///
/// `resconstantqual` is a `Node *` that the planner always fills with a
/// `List *` of qual clauses (consumed by `ExecInitQual` as a list); the owned
/// model holds the list directly (`None` = the C `NULL`).
#[derive(Debug, Default)]
pub struct Result<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: crate::nodeindexscan::Plan<'mcx>,
    /// `Node *resconstantqual` — the constant ("one-time") qualification, an
    /// implicitly-ANDed list of clauses (`None` = the C `NULL`).
    pub resconstantqual: Option<mcx::PgVec<'mcx, Expr>>,
}

impl Result<'_> {
    /// `nodeTag(node)` — always `T_Result`.
    pub fn tag(&self) -> NodeTag {
        T_Result
    }

    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Result<'b>> {
        let resconstantqual = match &self.resconstantqual {
            Some(list) => {
                let mut out = mcx::vec_with_capacity_in(mcx, list.len())?;
                for e in list.iter() {
                    out.push(e.clone());
                }
                Some(out)
            }
            None => None,
        };
        Ok(Result {
            plan: self.plan.clone_in(mcx)?,
            resconstantqual,
        })
    }
}

/// `ResultState` (execnodes.h):
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
#[derive(Debug, Default)]
pub struct ResultState<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `ExprState *resconstantqual` — the compiled constant qual (`None` = the
    /// C `NULL`, treated as always-true).
    pub resconstantqual: Option<PgBox<'mcx, ExprState>>,
    /// `bool rs_done` — are we done?
    pub rs_done: bool,
    /// `bool rs_checkqual` — do we need to check the qual?
    pub rs_checkqual: bool,
}

impl<'mcx> ResultState<'mcx> {
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
