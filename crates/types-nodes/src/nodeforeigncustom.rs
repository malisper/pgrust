//! Material node vocabulary (nodes/plannodes.h / executor/execnodes.h).
//!
//! src-idiomatic hosts `Material` / `MaterialState` in this module; the name
//! is preserved.

use mcx::{vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Index, Oid};
use types_error::PgResult;

use crate::execnodes::{Opaque, PlanStateData, RriId, ScanStateData, SlotId};
use crate::funcapi::Tuplestorestate;
use crate::nodeindexscan::Scan;
use crate::nodes::CmdType;
use crate::primnodes::Expr;

/// `Material` plan node (plannodes.h):
///
/// ```c
/// typedef struct Material { Plan plan; } Material;
/// ```
#[derive(Debug, Default)]
pub struct Material<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: crate::nodeindexscan::Plan<'mcx>,
}

impl Material<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Material<'b>> {
        Ok(Material {
            plan: self.plan.clone_in(mcx)?,
        })
    }
}

/// `MaterialState` (execnodes.h):
///
/// ```c
/// typedef struct MaterialState {
///     ScanState   ss;                 /* its first field is NodeTag */
///     int         eflags;             /* capability flags to pass to tuplestore */
///     bool        eof_underlying;     /* reached end of underlying plan? */
///     Tuplestorestate *tuplestorestate;
/// } MaterialState;
/// ```
#[derive(Debug, Default)]
pub struct MaterialState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `int eflags` — capability flags to pass to the tuplestore.
    pub eflags: i32,
    /// `bool eof_underlying` — reached end of underlying plan?
    pub eof_underlying: bool,
    /// `Tuplestorestate *tuplestorestate` — the materialized rows. The box is
    /// context-allocated (C: `tuplestore_begin_heap` pallocs the state in the
    /// caller's current context).
    pub tuplestorestate: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
}

impl<'mcx> MaterialState<'mcx> {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}

// ===========================================================================
// Foreign-scan vocabulary (nodes/plannodes.h, executor/execnodes.h,
// foreign/fdwapi.h). The `FdwRoutine` provider callbacks are installed by an
// FDW extension; the node only ever reads which callbacks are present and
// invokes them across the FDW-provider boundary (a seam). The presence of
// each callback the node consumes is modeled here as a `bool`, matching the C
// `fdwroutine->X != NULL` checks; the invocation crosses
// `backend-foreign-fdwapi-seams`.
// ===========================================================================

/// `FdwRoutine` (foreign/fdwapi.h) — the foreign-data-wrapper callback table,
/// trimmed to the *presence flags* `nodeForeignscan.c` tests before invoking a
/// callback. The mandatory scan callbacks (BeginForeignScan / IterateForeignScan
/// / ReScanForeignScan / EndForeignScan) and the modify callbacks the node uses
/// are always invoked unconditionally in C, so they are not carried as presence
/// flags; the optional callbacks the node guards with `if (fdwroutine->X)` /
/// `Assert(fdwroutine->X != NULL)` are.
///
/// The function pointers themselves are extension-owned and live behind the
/// FDW-provider seam; this table only records which optional callbacks the
/// provider supplied (C: a non-NULL slot).
#[derive(Clone, Copy, Debug, Default)]
pub struct FdwRoutine {
    /// `RecheckForeignScan` — optional EvalPlanQual recheck callback.
    pub has_recheck_foreign_scan: bool,
    /// `EstimateDSMForeignScan` — optional parallel DSM-size estimator.
    pub has_estimate_dsm_foreign_scan: bool,
    /// `InitializeDSMForeignScan` — optional parallel DSM initializer.
    pub has_initialize_dsm_foreign_scan: bool,
    /// `ReInitializeDSMForeignScan` — optional parallel DSM re-initializer.
    pub has_reinitialize_dsm_foreign_scan: bool,
    /// `InitializeWorkerForeignScan` — optional parallel worker initializer.
    pub has_initialize_worker_foreign_scan: bool,
    /// `ShutdownForeignScan` — optional async/resource shutdown callback.
    pub has_shutdown_foreign_scan: bool,
    /// `ForeignAsyncRequest` — async-execution request callback (mandatory for
    /// async-capable paths; the node `Assert`s it is present).
    pub has_foreign_async_request: bool,
    /// `ForeignAsyncConfigureWait` — async wait-configuration callback.
    pub has_foreign_async_configure_wait: bool,
    /// `ForeignAsyncNotify` — async event-notification callback.
    pub has_foreign_async_notify: bool,
}

/// `ForeignScan` plan node (nodes/plannodes.h), trimmed to the fields
/// `nodeForeignscan.c` consumes.
#[derive(Debug)]
pub struct ForeignScan<'mcx> {
    /// `Scan scan` — the abstract scan-node base (carries the `Plan` head and
    /// `scanrelid`).
    pub scan: Scan<'mcx>,
    /// `CmdType operation` — SELECT/INSERT/UPDATE/DELETE.
    pub operation: CmdType,
    /// `Index resultRelation` — direct-modification target's RT index.
    pub resultRelation: Index,
    /// `Oid fs_server` — OID of the foreign server.
    pub fs_server: Oid,
    /// `List *fdw_scan_tlist` — optional tlist describing the scan tuple
    /// (`None` = the C `NIL`).
    pub fdw_scan_tlist: Option<PgVec<'mcx, crate::primnodes::TargetEntry<'mcx>>>,
    /// `List *fdw_recheck_quals` — original quals not in `scan.plan.qual`
    /// (`None` = the C `NIL`).
    pub fdw_recheck_quals: Option<PgVec<'mcx, Expr>>,
    /// `bool fsSystemCol` — true if any "system column" is needed.
    pub fsSystemCol: bool,
}

impl Default for ForeignScan<'_> {
    /// C `makeNode(ForeignScan)` zero-init: `operation` is `CMD_UNKNOWN` (the
    /// C `CmdType` zero value); every other field is its zero/`NIL`.
    fn default() -> Self {
        ForeignScan {
            scan: Scan::default(),
            operation: CmdType::CMD_UNKNOWN,
            resultRelation: 0,
            fs_server: Oid::default(),
            fdw_scan_tlist: None,
            fdw_recheck_quals: None,
            fsSystemCol: false,
        }
    }
}

impl ForeignScan<'_> {
    /// `outerPlan(node)` shortcut and the `node->scan.scanrelid` reads are
    /// direct field access; this is the deep-copy used by `copyObject`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ForeignScan<'b>> {
        let fdw_scan_tlist = match &self.fdw_scan_tlist {
            Some(tl) => {
                let mut out = vec_with_capacity_in(mcx, tl.len())?;
                for tle in tl.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let fdw_recheck_quals = match &self.fdw_recheck_quals {
            Some(q) => {
                let mut out = vec_with_capacity_in(mcx, q.len())?;
                for e in q.iter() {
                    out.push(e.clone());
                }
                Some(out)
            }
            None => None,
        };
        Ok(ForeignScan {
            scan: self.scan.clone_in(mcx)?,
            operation: self.operation,
            resultRelation: self.resultRelation,
            fs_server: self.fs_server,
            fdw_scan_tlist,
            fdw_recheck_quals,
            fsSystemCol: self.fsSystemCol,
        })
    }
}

/// `ForeignScanState` (executor/execnodes.h), trimmed:
///
/// ```c
/// typedef struct ForeignScanState {
///     ScanState   ss;
///     ExprState  *fdw_recheck_quals;
///     Size        pscan_len;
///     ResultRelInfo *resultRelInfo;
///     struct FdwRoutine *fdwroutine;
///     void       *fdw_state;
/// } ForeignScanState;
/// ```
#[derive(Debug, Default)]
pub struct ForeignScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `ExprState *fdw_recheck_quals` — compiled recheck quals (`None` = the C
    /// `NULL`). The compiled `ExprState` is execExpr-owned; carried opaquely.
    pub fdw_recheck_quals: Option<PgBox<'mcx, crate::execexpr::ExprState>>,
    /// `Size pscan_len` — size of parallel coordination information.
    pub pscan_len: usize,
    /// `ResultRelInfo *resultRelInfo` — result rel info, if UPDATE or DELETE
    /// (id into the EState pool; `None` = the C `NULL`).
    pub resultRelInfo: Option<RriId>,
    /// `struct FdwRoutine *fdwroutine` — the FDW callback table the handler
    /// installed (`None` = the C `NULL`, before the GetFdwRoutine* lookup).
    pub fdwroutine: Option<FdwRoutine>,
    /// `void *fdw_state` — FDW-private per-scan state (genuinely opaque
    /// extension memory; `None` is the C `NULL`).
    pub fdw_state: Opaque,
}

impl<'mcx> ForeignScanState<'mcx> {
    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}

/// `ParallelContext` (access/parallel.h) — parallel-coordination context for
/// the parallel-scan entry points, trimmed. The `shm_toc`/`shm_toc_estimator`
/// it carries are storage-owned (DSM); they are reached through the FDW /
/// shm_toc seams, so the owned model carries the live object opaquely.
#[derive(Debug, Default)]
pub struct ParallelContext {
    /// `shm_toc_estimator estimator` + `shm_toc *toc` — the live DSM
    /// coordination objects (storage-owned, opaque here).
    pub toc: Opaque,
}

/// `ParallelWorkerContext` (access/parallel.h), trimmed: the worker's view of
/// the parallel-coordination DSM.
#[derive(Debug, Default)]
pub struct ParallelWorkerContext {
    /// `shm_toc *toc` — the worker's DSM table-of-contents (opaque here).
    pub toc: Opaque,
}

/// `AsyncRequest` (executor/execnodes.h), trimmed to the fields the async
/// foreign-scan entry points consume. `requestee` is a `PlanState *` into the
/// executor tree (resolved to a `ForeignScanState` in C); in the owned model
/// the requestee node and its `fdwroutine` are reached through the FDW seam,
/// so it is carried opaquely.
#[derive(Debug, Default)]
pub struct AsyncRequest {
    /// `struct PlanState *requestee` — node from which a tuple is wanted
    /// (the `ForeignScanState` whose `fdwroutine` async callbacks are run).
    pub requestee: Opaque,
    /// `int request_index` — scratch space for the requestor.
    pub request_index: i32,
    /// `bool callback_pending` — callback is needed.
    pub callback_pending: bool,
    /// `bool request_complete` — request complete, result valid.
    pub request_complete: bool,
    /// `TupleTableSlot *result` — result (`None` / an empty slot = no more
    /// tuples).
    pub result: Option<SlotId>,
}
