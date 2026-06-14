//! `BitmapOr` plan node (`plannodes.h`) and `BitmapOrState` executor node
//! (`execnodes.h`), owned by this unit.
//!
//! Like `nodeBitmapHeapscan`, these node/state types are not (yet) threaded
//! into the central `types-nodes` `Node` / `PlanStateNode` enums — the executor
//! dispatch reaches `BitmapOr` only through the seam-and-panic arm in
//! `execProcnode` until that unit wires this crate's `ExecInitBitmapOr` /
//! `MultiExecBitmapOr`. The state is built and consumed here.

use mcx::{PgBox, PgVec};
use types_nodes::execnodes::PlanStateData;
use types_nodes::nodes::Node;
use types_nodes::nodeindexscan::Plan;
use types_nodes::planstate::PlanStateNode;

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
    pub bitmapplans: Vec<Node<'mcx>>,
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
