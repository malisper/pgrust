//! `Result` plan-node and `ResultState` executor-state vocabulary
//! (nodes/plannodes.h / executor/execnodes.h), trimmed to what the
//! `nodeResult.c` port consumes.

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::execexpr::ExprState;
use crate::execnodes::PlanStateData;
use crate::nodes::NodeTag;
use crate::primnodes::Expr;

/// `T_ResultState` (nodes/nodetags.h) — the executor-state node tag for a
/// `Result` node. Value verified against PostgreSQL 18.3's generated
/// enumeration.
pub const T_ResultState: NodeTag = NodeTag(394);

/// `Result` plan node (plannodes.h):
///
/// ```c
/// typedef struct Result {
///     Plan        plan;
///     Node       *resconstantqual;
/// } Result;
/// ```
///
/// `resconstantqual` is a one-time qualification test (a qual `List` cast to a
/// `Node *` by the planner), or NULL; `ExecInitResult` compiles it with
/// `ExecInitQual((List *) node->resconstantqual, ...)`, so the owned model
/// holds it as the same implicitly-ANDed qual-clause list the executor
/// compiles (`None` = the C `NULL`).
#[derive(Debug, Default)]
pub struct Result<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: crate::nodeindexscan::Plan<'mcx>,
    /// `Node *resconstantqual` — one-time qualification test, or NULL.
    pub resconstantqual: Option<PgVec<'mcx, Expr>>,
}

impl Result<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Result<'b>> {
        let resconstantqual = match &self.resconstantqual {
            Some(list) => {
                let mut out: PgVec<'b, Expr> = mcx::vec_with_capacity_in(mcx, list.len())?;
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

/// `ResultState` (execnodes.h) — the `Result` executor node's run-time state:
///
/// ```c
/// typedef struct ResultState {
///     PlanState   ps;             /* its first field is NodeTag */
///     ExprState  *resconstantqual;
///     bool        rs_done;        /* are we done? */
///     bool        rs_checkqual;   /* do we need to check the qual? */
/// } ResultState;
/// ```
#[derive(Debug, Default)]
pub struct ResultStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `ExprState *resconstantqual` — the compiled one-time (constant) qual.
    pub resconstantqual: Option<PgBox<'mcx, ExprState>>,
    /// `bool rs_done` — are we done?
    pub rs_done: bool,
    /// `bool rs_checkqual` — do we need to check the constant qual?
    pub rs_checkqual: bool,
}

impl<'mcx> ResultStateData<'mcx> {
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
