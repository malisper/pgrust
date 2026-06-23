//! Seam declarations for the `BitmapAnd` executor node (`nodeBitmapAnd.c`).
//!
//! The executor dispatch crates (`execProcnode.c` / `execAmi.c`) call back into
//! the per-node `BitmapAnd` routines — a real dependency cycle that cannot be
//! resolved by a direct dependency (the node crate depends on the dispatch
//! crates' seams). So the four interface routines are declared here and
//! installed by `backend-executor-nodeBitmapAnd::init_seams()`; until then a
//! call panics loudly.

#![allow(non_snake_case)]

use ::mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::nodes::nodebitmapand::BitmapAnd;
use ::nodes::nodes::Node;
use ::nodes::{EStateData, PlanStateNode};
use ::tidbitmap::TIDBitmap;

seam_core::seam!(
    /// `ExecInitBitmapAnd(node, estate, eflags)` (nodeBitmapAnd.c): begin all of
    /// the subscans of the `BitmapAnd` node, returning the initialized
    /// `BitmapAndState` as a `PlanStateNode`. Allocates in the per-query context,
    /// so fallible on OOM / child-init error. `node` is the plan node as a
    /// `Node` (for the `ps.plan` back-link); `bitmap_and` is the same node
    /// narrowed to the concrete `BitmapAnd` (the C `castNode`).
    pub fn exec_init_bitmap_and<'mcx>(
        mcx: Mcx<'mcx>,
        node: &'mcx Node<'mcx>,
        bitmap_and: &'mcx BitmapAnd<'mcx>,
        estate: &mut EStateData<'mcx>,
        eflags: i32,
    ) -> PgResult<PgBox<'mcx, PlanStateNode<'mcx>>>
);

seam_core::seam!(
    /// `MultiExecBitmapAnd(node)` (nodeBitmapAnd.c): retrieve the AND of all the
    /// child subplan result bitmaps. Returns the built `TIDBitmap`; allocates
    /// during execution, so fallible.
    pub fn multi_exec_bitmap_and<'mcx>(
        node: &mut PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<PgBox<'mcx, TIDBitmap>>
);

seam_core::seam!(
    /// `ExecEndBitmapAnd(node)` (nodeBitmapAnd.c): shut down the subscans of the
    /// `BitmapAnd` node.
    pub fn exec_end_bitmap_and<'mcx>(
        node: &mut PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ExecReScanBitmapAnd(node)` (nodeBitmapAnd.c): rescan the `BitmapAnd`
    /// node — propagate changed-param signaling to each child.
    pub fn exec_rescan_bitmap_and<'mcx>(
        node: &mut PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);
