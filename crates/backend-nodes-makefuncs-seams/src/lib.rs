//! Seam declarations for the `backend-nodes-makefuncs` unit
//! (`nodes/makefuncs.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Allocating constructors take the target context
//! handle (C: they palloc the node in `CurrentMemoryContext`).

use mcx::{Mcx, PgBox, PgVec};
use types_error::PgResult;
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `makeBoolExpr(AND_EXPR, args, location)` (makefuncs.c): build a
    /// `BoolExpr` node combining `args` with `AND_EXPR` at the source
    /// `location` (-1 for "unknown"). The node and its arg list are allocated
    /// in `mcx`. `Err` carries OOM.
    pub fn make_and_boolexpr<'mcx>(
        mcx: Mcx<'mcx>,
        args: PgVec<'mcx, Node<'mcx>>,
        location: i32,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);
