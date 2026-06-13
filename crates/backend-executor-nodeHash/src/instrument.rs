//! Hash-build instrumentation and the parallel-DSM node hooks. These are the
//! implementations the parallel executor reaches through
//! `backend-executor-nodeHash-pq-seams` (installed by [`crate::init_seams`]).

use mcx::Mcx;
use types_error::PgResult;
use types_execparallel::{ParallelContextHandle, ParallelWorkerContextHandle};
use types_nodes::nodehash::{HashInstrumentation, HashJoinTableData, HashState};

/// `ExecHashEstimate(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2761) — reserve DSM space for the shared instrumentation area.
pub fn ExecHashEstimate<'mcx>(
    _node: &mut HashState<'mcx>,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashInitializeDSM(HashState *node, ParallelContext *pcxt)`
/// (nodeHash.c:2780) — set up the shared `SharedHashInfo` instrumentation area.
pub fn ExecHashInitializeDSM<'mcx>(
    _node: &mut HashState<'mcx>,
    _pcxt: ParallelContextHandle,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashInitializeWorker(HashState *node, ParallelWorkerContext *pwcxt)`
/// (nodeHash.c:2805) — attach a worker to the shared instrumentation area.
pub fn ExecHashInitializeWorker<'mcx>(
    _node: &mut HashState<'mcx>,
    _pwcxt: ParallelWorkerContextHandle,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashRetrieveInstrumentation(HashState *node)` (nodeHash.c:2846) — the
/// leader copies the shared-memory stats into local storage before DSM
/// shutdown. Allocates the local copy in `mcx`.
pub fn ExecHashRetrieveInstrumentation<'mcx>(
    _mcx: Mcx<'mcx>,
    _node: &mut HashState<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecHashAccumInstrumentation(HashInstrumentation *instrument,
/// HashJoinTable hashtable)` (nodeHash.c:2877) — fold the live hashtable's
/// dimensions into the running instrumentation maxima. Pure field updates.
pub fn ExecHashAccumInstrumentation<'mcx>(
    _instrument: &mut HashInstrumentation,
    _hashtable: &HashJoinTableData<'mcx>,
) {
    todo!("decomp")
}
