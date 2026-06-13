//! Nested-loop join node vocabulary (`nodes/plannodes.h` `NestLoop`/
//! `NestLoopParam`, `executor/execnodes.h` `NestLoopState`).
//!
//! The embedded `JoinState`/`PlanState` head reuses
//! [`crate::jointype::JoinStateData`], the leading `Join` plan base reuses
//! [`crate::jointype::Join`], and the result/null slots follow the owned model
//! ([`SlotId`] for `TupleTableSlot *`).

use alloc::vec::Vec;

use mcx::Mcx;
use types_error::PgResult;

use crate::execnodes::SlotId;
use crate::jointype::{Join, JoinStateData};
use crate::nodes::NodeTag;
use crate::primnodes::Var;

/// `T_NestLoop` (nodes/nodetags.h) — the plan-node tag for a NestLoop.
pub const T_NestLoop: NodeTag = NodeTag(356);
/// `T_NestLoopState` (nodes/nodetags.h) — the executor-state node tag.
pub const T_NestLoopState: NodeTag = NodeTag(421);

/// `NestLoopParam` (nodes/plannodes.h):
///
/// ```c
/// typedef struct NestLoopParam
/// {
///     NodeTag     type;
///     int         paramno;        /* number of the PARAM_EXEC Param to set */
///     Var        *paramval;       /* outer-relation Var to assign to Param */
/// } NestLoopParam;
/// ```
#[derive(Clone, Debug, Default)]
pub struct NestLoopParam {
    /// `int paramno` — number of the PARAM_EXEC Param to set.
    pub paramno: i32,
    /// `Var *paramval` — outer-relation Var to assign to Param.
    pub paramval: Var,
}

/// `NestLoop` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct NestLoop
/// {
///     Join        join;
///     List       *nestParams;     /* list of NestLoopParam nodes */
/// } NestLoop;
/// ```
#[derive(Debug, Default)]
pub struct NestLoop<'mcx> {
    /// `Join join` — its first field (`plan`) starts with the `NodeTag`.
    pub join: Join<'mcx>,
    /// `List *nestParams` — list of `NestLoopParam` nodes. An empty vec is the
    /// C `NIL`.
    pub nestParams: Vec<NestLoopParam>,
}

impl<'mcx> NestLoop<'mcx> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying the
    /// embedded join/plan subtree allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<NestLoop<'b>> {
        Ok(NestLoop {
            join: self.join.clone_in(mcx)?,
            nestParams: self.nestParams.clone(),
        })
    }
}

/// `NestLoopState` (executor/execnodes.h):
///
/// ```c
/// typedef struct NestLoopState
/// {
///     JoinState   js;             /* its first field is NodeTag */
///     bool        nl_NeedNewOuter;
///     bool        nl_MatchedOuter;
///     TupleTableSlot *nl_NullInnerTupleSlot;
/// } NestLoopState;
/// ```
#[derive(Debug, Default)]
pub struct NestLoopStateData<'mcx> {
    /// `JoinState js` — its first field is `NodeTag`.
    pub js: JoinStateData<'mcx>,
    /// `bool nl_NeedNewOuter`.
    pub nl_NeedNewOuter: bool,
    /// `bool nl_MatchedOuter`.
    pub nl_MatchedOuter: bool,
    /// `TupleTableSlot *nl_NullInnerTupleSlot` — id into `es_tupleTable`.
    pub nl_NullInnerTupleSlot: Option<SlotId>,
}
