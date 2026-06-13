//! Hash executor-node lifecycle and the `MultiExec` build entry points.

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::execnodes::{EStateData, PlanStateData};
use types_nodes::nodehash::{Hash, HashState};
use types_nodes::nodes::Node;

/// `ExecHash(PlanState *pstate)` (nodeHash.c:91) — the per-node executor
/// callback slot for a Hash node. Hash never returns single tuples this way;
/// it `elog(ERROR)`s, so the body returns `Err`.
pub fn ExecHash(_pstate: &mut PlanStateData<'_>) -> PgResult<()> {
    todo!("decomp")
}

/// `MultiExecHash(HashState *node)` (nodeHash.c:105) — build the hash table by
/// pulling every tuple from the outer (inner-relation) child. Dispatches to the
/// serial or parallel build. Returns the node's result (the C returns `Node *`,
/// always NULL here — the table is the side effect).
pub fn MultiExecHash<'mcx>(
    _mcx: Mcx<'mcx>,
    _node: &mut HashState<'mcx>,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    todo!("decomp")
}

/// `MultiExecPrivateHash(HashState *node)` (nodeHash.c:138) — the serial build:
/// fetch outer tuples and insert them into the private hash table.
pub fn MultiExecPrivateHash<'mcx>(_mcx: Mcx<'mcx>, _node: &mut HashState<'mcx>) -> PgResult<()> {
    todo!("decomp")
}

/// `MultiExecParallelHash(HashState *node)` (nodeHash.c:219) — the parallel
/// build: coordinate with peers through the build barrier and load tuples into
/// the shared hash table.
pub fn MultiExecParallelHash<'mcx>(_mcx: Mcx<'mcx>, _node: &mut HashState<'mcx>) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecInitHash(Hash *node, EState *estate, int eflags)` (nodeHash.c:370) —
/// initialize the Hash plan node into a `HashState`. Allocates node state in
/// `mcx`.
pub fn ExecInitHash<'mcx>(
    _mcx: Mcx<'mcx>,
    _node: &Hash<'mcx>,
    _estate: &mut EStateData<'mcx>,
    _eflags: i32,
) -> PgResult<PgBox<'mcx, HashState<'mcx>>> {
    todo!("decomp")
}

/// `ExecEndHash(HashState *node)` (nodeHash.c:427) — shut down the Hash node
/// (frees the expr context and recurses into the outer plan).
pub fn ExecEndHash<'mcx>(_node: &mut HashState<'mcx>) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecReScanHash(HashState *node)` (nodeHash.c:2381) — rescan support; if the
/// child's chgParam is unchanged the subtree is rescanned.
pub fn ExecReScanHash<'mcx>(_node: &mut HashState<'mcx>) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecShutdownHash(HashState *node)` (nodeHash.c:2831) — copy shared-memory
/// instrumentation to local storage before DSM shutdown.
pub fn ExecShutdownHash<'mcx>(_mcx: Mcx<'mcx>, _node: &mut HashState<'mcx>) -> PgResult<()> {
    todo!("decomp")
}
