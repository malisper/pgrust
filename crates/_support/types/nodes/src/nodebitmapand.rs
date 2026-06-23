//! BitmapAnd node vocabulary (`nodes/plannodes.h` `BitmapAnd`,
//! `executor/execnodes.h` `BitmapAndState`).
//!
//! `BitmapAnd` is the plan node for intersecting the result bitmaps of several
//! bitmap-yielding sub-plans (`BitmapIndexScan`/`BitmapAnd`/`BitmapOr`).
//! `BitmapAndState` is the executor state. Like `Append`, the C `List *` of
//! child `Plan *` is the owned `Vec<Node>`; the C `PlanState **` child array is
//! the owned `PgVec<Option<PgBox<PlanStateNode>>>` (each slot is the C
//! `palloc0`-zeroed pointer, set as each child is inited).

use alloc::vec::Vec;

use ::mcx::{Mcx, PgBox, PgVec};
use ::types_error::PgResult;

use crate::execnodes::PlanStateData;
use crate::nodeindexscan::Plan;
use crate::planstate::PlanStateNode;

pub use crate::nodes::T_BitmapAnd;

/// `T_BitmapAndState` (nodes/nodetags.h) — the executor-state node tag for a
/// `BitmapAndState`. Verified against PostgreSQL 18.3's generated `nodetags.h`
/// (value 400).
pub const T_BitmapAndState: crate::nodes::NodeTag = crate::nodes::NodeTag(400);

/// `BitmapAnd` plan node (plannodes.h):
///
/// ```c
/// typedef struct BitmapAnd
/// {
///     Plan        plan;
///     List       *bitmapplans;
/// } BitmapAnd;
/// ```
#[derive(Debug, Default)]
pub struct BitmapAnd<'mcx> {
    /// `Plan plan` — its first field starts with the `NodeTag`.
    pub plan: Plan<'mcx>,
    /// `List *bitmapplans` — list of bitmap-yielding sub-plans.
    pub bitmapplans: Vec<crate::nodes::Node<'mcx>>,
}

impl BitmapAnd<'_> {
    /// `nodeTag(node)` — always `T_BitmapAnd`.
    pub fn tag(&self) -> crate::nodes::NodeTag {
        T_BitmapAnd
    }

    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<BitmapAnd<'b>> {
        let mut bitmapplans = ::mcx::vec_with_capacity_in(mcx, self.bitmapplans.len())?;
        for child in self.bitmapplans.iter() {
            bitmapplans.push(child.clone_in(mcx)?);
        }
        Ok(BitmapAnd {
            plan: self.plan.clone_in(mcx)?,
            bitmapplans: bitmapplans.into_iter().collect(),
        })
    }
}

/// `BitmapAndState` (execnodes.h):
///
/// ```c
/// typedef struct BitmapAndState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     PlanState **bitmapplans;    /* array of PlanStates for my inputs */
///     int         nplans;         /* number of input plans */
/// } BitmapAndState;
/// ```
#[derive(Debug)]
pub struct BitmapAndState<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `PlanState **bitmapplans` — array of child `PlanState`s (the C
    /// `palloc0`-zeroed pointer array, each slot set as the child is inited).
    pub bitmapplans: PgVec<'mcx, Option<PgBox<'mcx, PlanStateNode<'mcx>>>>,
    /// `int nplans` — number of input plans.
    pub nplans: i32,
}

impl<'mcx> BitmapAndState<'mcx> {
    /// Build an empty `BitmapAndState` (the C `makeNode(BitmapAndState)` zeroed
    /// struct) with the child array allocated in `mcx`.
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        BitmapAndState {
            ps: PlanStateData::default(),
            bitmapplans: PgVec::new_in(mcx),
            nplans: 0,
        }
    }

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
