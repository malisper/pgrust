//! BitmapOr node vocabulary (`nodes/plannodes.h` `BitmapOr`,
//! `executor/execnodes.h` `BitmapOrState`).
//!
//! `BitmapOr` is the plan node for unioning the result bitmaps of several
//! bitmap-yielding sub-plans (`BitmapIndexScan`/`BitmapAnd`/`BitmapOr`).
//! `BitmapOrState` is the executor state. Like `BitmapAnd`, the C `List *` of
//! child `Plan *` is the owned `Vec<Node>`; the C `PlanState **` child array is
//! the owned `PgVec<Option<PgBox<PlanStateNode>>>` (each slot is the C
//! `palloc0`-zeroed pointer, set as each child is inited).
//!
//! These types live here (not in the `backend-executor-nodeBitmapOr` crate)
//! because the central `PlanStateNode` dispatch enum — which lives in
//! `types-nodes` — must name `BitmapOrState` as a variant; the struct's fields
//! are all nameable from this layer, so it relocates cleanly (no executor-only
//! type above `types-nodes` appears in them). The executor logic
//! (`ExecInitBitmapOr` / `MultiExecBitmapOr` / ...) stays in its node crate and
//! imports the struct from here.

use alloc::vec::Vec;

use mcx::{PgBox, PgVec};

use crate::execnodes::PlanStateData;
use crate::nodeindexscan::Plan;
use crate::planstate::PlanStateNode;

pub use crate::execstate_tags::T_BitmapOrState;

/// `T_BitmapOr` plan-node tag (nodetags.h).
pub const T_BitmapOr: crate::nodes::NodeTag = crate::nodes::NodeTag(338);

/// `BitmapOr` plan node (plannodes.h):
///
/// ```c
/// typedef struct BitmapOr {
///     Plan        plan;
///     bool        isshared;
///     List       *bitmapplans;
/// } BitmapOr;
/// ```
#[derive(Debug)]
pub struct BitmapOr<'mcx> {
    /// `Plan plan` base.
    pub plan: Plan<'mcx>,
    /// `bool isshared` — whether the result bitmap is built in shared memory
    /// (a parallel bitmap heap scan below this OR).
    pub isshared: bool,
    /// `List *bitmapplans` — the input bitmap-producing subplans (each a
    /// `BitmapIndexScan` / `BitmapAnd` / `BitmapOr` `Plan`). `Node` is the
    /// unified plan-node enum that `ExecInitNode` recurses over.
    pub bitmapplans: Vec<crate::nodes::Node<'mcx>>,
}

impl BitmapOr<'_> {
    /// `nodeTag(node)` — always `T_BitmapOr`.
    pub fn tag(&self) -> crate::nodes::NodeTag {
        T_BitmapOr
    }

    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: mcx::Mcx<'b>) -> types_error::PgResult<BitmapOr<'b>> {
        let mut bitmapplans = mcx::vec_with_capacity_in(mcx, self.bitmapplans.len())?;
        for child in self.bitmapplans.iter() {
            bitmapplans.push(child.clone_in(mcx)?);
        }
        Ok(BitmapOr {
            plan: self.plan.clone_in(mcx)?,
            isshared: self.isshared,
            bitmapplans: bitmapplans.into_iter().collect(),
        })
    }
}

/// `BitmapOrState` executor node (execnodes.h):
///
/// ```c
/// typedef struct BitmapOrState {
///     PlanState   ps;             /* its first field is NodeTag */
///     PlanState **bitmapplans;    /* array of PlanStates for my inputs */
///     int         nplans;         /* number of input plans */
/// } BitmapOrState;
/// ```
#[derive(Debug)]
pub struct BitmapOrState<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `PlanState **bitmapplans` — array of plan states for the inputs. The C
    /// `palloc0`'d array holds possibly-NULL pointers, so each slot is an
    /// `Option<PgBox<PlanStateNode>>` (`ExecInitNode` may return `None`).
    pub bitmapplans: PgVec<'mcx, Option<PgBox<'mcx, PlanStateNode<'mcx>>>>,
    /// `int nplans` — number of input plans.
    pub nplans: i32,
    /// `((BitmapOr *) ps.plan)->isshared` — snapshotted from the plan node at
    /// init time. The C re-reads it through `ps.plan` in `MultiExecBitmapOr`;
    /// the owned model carries the read-only value here because the `BitmapOr`
    /// plan node is not a `types-nodes` `Node` variant the head can alias.
    pub isshared: bool,
}
