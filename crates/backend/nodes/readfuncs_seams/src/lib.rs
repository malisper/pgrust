//! Seam declarations for the `backend-nodes-readfuncs` unit
//! (`nodes/readfuncs.c`): the per-tag field readers that reconstruct a node
//! from its `nodeToString` rendering.
//!
//! `readfuncs.c` is the other half of the node de-serializer: `read.c` owns the
//! tokenizer (`pg_strtok`) and the polymorphic driver (`nodeRead`), while
//! `readfuncs.c` owns `parseNodeString()` — the giant tag dispatch that, having
//! seen a `{`, reads the node-type keyword and the per-field `READ_*` macros to
//! rebuild one concrete node. The two recurse into each other through the
//! shared `pg_strtok` cursor exactly as in C, so a direct dependency would be
//! cyclic; this seam breaks the cycle on the `read.c` -> `readfuncs.c` edge.
//!
//! `read.c`'s `nodeRead` calls `parseNodeString()` for the `LEFT_BRACE` case.
//! `parseNodeString` itself reads further tokens from the shared cursor (owned
//! by `read.c`) and recurses back through `nodeRead`, so it takes no explicit
//! cursor argument — exactly the C `parseNodeString(void)` surface. The
//! reconstructed node is allocated in `mcx`.
//!
//! The owning unit (`backend-nodes-readfuncs`) installs this from its
//! `init_seams()` when it lands; until then a call panics loudly
//! (`mirror-pg-and-panic`).

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use nodes::nodeindexscan::PlannedStmt;
use nodes::nodes::Node;

seam_core::seam!(
    /// `(PlannedStmt *) stringToNode(pstmtspace)` (execParallel.c
    /// `ExecParallelGetQueryDesc`) — reconstruct a `PlannedStmt` from its
    /// `nodeToString` text. `PlannedStmt` is not a `Node` enum variant in the
    /// trimmed model, so it cannot route through the `parse_node_string`
    /// (`Node`-returning) dispatch; this dedicated reader drives the shared
    /// `pg_strtok` cursor itself (`read.c`'s `with_strtok`), reads the opening
    /// `{ PLANNEDSTMT`, and reverses `ExecSerializePlan`'s `_outPlannedStmt`
    /// field-for-field. Allocated in `mcx`. `Err` carries OOM / a parse error.
    pub fn string_to_planned_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        text: &str,
    ) -> PgResult<PgBox<'mcx, PlannedStmt<'mcx>>>
);

seam_core::seam!(
    /// `parseNodeString()` (`nodes/readfuncs.c`): with the shared `pg_strtok`
    /// cursor positioned just past a node-opening `{`, read the node-type
    /// keyword and that node's fields back into a freshly allocated `Node`
    /// (`mcx`). Recurses into `read.c`'s `nodeRead`/`pg_strtok` for sub-fields
    /// via the shared cursor. `Err` carries OOM / a parse error
    /// (C: `elog(ERROR, "badly formatted node string ...")`).
    pub fn parse_node_string<'mcx>(
        mcx: Mcx<'mcx>,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);
