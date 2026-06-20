//! Material node vocabulary (nodes/plannodes.h / executor/execnodes.h).
//!
//! src-idiomatic hosts `Material` / `MaterialState` in this module; the name
//! is preserved.

use alloc::string::String;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Index, Oid};
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::execnodes::{Opaque, PlanStateData, RriId, ScanStateData};
use crate::funcapi::Tuplestorestate;
use crate::nodeindexscan::Scan;
use crate::nodes::CmdType;
use crate::primnodes::{Expr, TargetEntry};
use crate::TupleSlotKind;

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
    /// `Oid checkAsUser` — user to perform the scan as; `0` (`InvalidOid`)
    /// means to check as the current user.
    pub checkAsUser: Oid,
    /// `Oid fs_server` — OID of the foreign server.
    pub fs_server: Oid,
    /// `List *fdw_exprs` — expressions that the FDW may evaluate
    /// (`None` = the C `NIL`).
    pub fdw_exprs: Option<PgVec<'mcx, Expr>>,
    /// `List *fdw_private` — private data for the FDW (an arbitrary node list;
    /// `None` = the C `NIL`).
    pub fdw_private: Option<PgVec<'mcx, crate::nodes::NodePtr<'mcx>>>,
    /// `List *fdw_scan_tlist` — optional tlist describing the scan tuple
    /// (`None` = the C `NIL`).
    pub fdw_scan_tlist: Option<PgVec<'mcx, crate::primnodes::TargetEntry<'mcx>>>,
    /// `List *fdw_recheck_quals` — original quals not in `scan.plan.qual`
    /// (`None` = the C `NIL`).
    pub fdw_recheck_quals: Option<PgVec<'mcx, Expr>>,
    /// `Bitmapset *fs_relids` — base+OJ RTIs generated by this scan
    /// (`None` = the C `NULL`).
    pub fs_relids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *fs_base_relids` — base RTIs generated by this scan
    /// (`None` = the C `NULL`).
    pub fs_base_relids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
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
            checkAsUser: Oid::default(),
            fs_server: Oid::default(),
            fdw_exprs: None,
            fdw_private: None,
            fdw_scan_tlist: None,
            fdw_recheck_quals: None,
            fs_relids: None,
            fs_base_relids: None,
            fsSystemCol: false,
        }
    }
}

impl ForeignScan<'_> {
    /// `outerPlan(node)` shortcut and the `node->scan.scanrelid` reads are
    /// direct field access; this is the deep-copy used by `copyObject`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ForeignScan<'b>> {
        let fdw_exprs = match &self.fdw_exprs {
            Some(q) => {
                let mut out = vec_with_capacity_in(mcx, q.len())?;
                for e in q.iter() {
                    // Deep-copy via `clone_in`, not the derived `Expr::clone`
                    // (which panics on a `SubPlan` arm).
                    out.push(e.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let fdw_private = match &self.fdw_private {
            Some(list) => {
                let mut out = vec_with_capacity_in(mcx, list.len())?;
                for n in list.iter() {
                    out.push(alloc_in(mcx, n.clone_in(mcx)?)?);
                }
                Some(out)
            }
            None => None,
        };
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
                    // Deep-copy via `clone_in`, not the derived `Expr::clone`
                    // (which panics on a `SubPlan` arm).
                    out.push(e.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(ForeignScan {
            scan: self.scan.clone_in(mcx)?,
            operation: self.operation,
            resultRelation: self.resultRelation,
            checkAsUser: self.checkAsUser,
            fs_server: self.fs_server,
            fdw_exprs,
            fdw_private,
            fdw_scan_tlist,
            fdw_recheck_quals,
            fs_relids: match &self.fs_relids {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            fs_base_relids: match &self.fs_base_relids {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
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
    pub fdw_recheck_quals: Option<PgBox<'mcx, crate::execexpr::ExprState<'mcx>>>,
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
/// the parallel-scan entry points, trimmed. The `shm_toc *toc` it carries is
/// storage-owned (DSM), reached through the FDW / shm_toc seams, so it is kept
/// opaque. The `shm_toc_estimator estimator` is a backend-local sizing
/// accumulator (not in the segment), so it is carried as a real field that the
/// `shm_toc_estimate_{chunk,keys}` seams operate on directly.
#[derive(Debug, Default)]
pub struct ParallelContext {
    /// `shm_toc_estimator estimator` — backend-local DSM-size sizing
    /// accumulator (`space_for_chunks` / `number_of_keys`).
    pub estimator: types_storage::storage::shm_toc_estimator,
    /// `shm_toc *toc` — the live DSM table-of-contents (storage-owned, opaque
    /// here).
    pub toc: Opaque,
}

/// `ParallelWorkerContext` (access/parallel.h), trimmed: the worker's view of
/// the parallel-coordination DSM.
#[derive(Debug, Default)]
pub struct ParallelWorkerContext {
    /// `shm_toc *toc` — the worker's DSM table-of-contents (opaque here).
    pub toc: Opaque,
}

// `AsyncRequest` (executor/execnodes.h) is modeled by
// [`crate::nodeappend::AsyncRequestData`]: the owned tree never reconstructs the
// `requestor`/`requestee` raw back-pointers, so the requestee `ForeignScanState`
// is passed by reference to the async dispatch (see the execAsync seam) rather
// than carried opaquely on the request record.

// ===========================================================================
// Custom-scan vocabulary (nodes/plannodes.h, executor/execnodes.h,
// nodes/extensible.h). The `CustomExecMethods` provider callbacks are
// installed by a custom-scan-provider extension; `nodeCustom.c` only ever
// reads which optional callbacks are present and invokes them across the
// provider boundary (a seam). Each optional callback the node guards with
// `if (methods->X)` / `Assert(methods->X != NULL)` is modeled here as a
// `bool`; the invocations cross `backend-nodes-extensible-seams`.
// ===========================================================================

/// `CustomExecMethods` (nodes/extensible.h) — the custom-scan provider's
/// executor callback table, trimmed to what `nodeCustom.c` reads directly: the
/// provider name (used in the mark/restore error message) and the *presence
/// flags* of the optional callbacks the node guards before invoking. The
/// mandatory callbacks (BeginCustomScan / ExecCustomScan / EndCustomScan /
/// ReScanCustomScan) are invoked unconditionally in C, so they are not carried
/// as presence flags; the invocations all cross the provider seam.
///
/// The function pointers themselves are extension-owned and live behind the
/// provider seam; this table records only the provider's name and which
/// optional callbacks it supplied (C: a non-NULL slot).
#[derive(Clone, Debug, Default)]
pub struct CustomExecMethods {
    /// `const char *CustomName` — the provider's name (the mark/restore error
    /// message interpolates it).
    pub CustomName: Option<String>,
    /// `MarkPosCustomScan` — optional mark/restore: mark position.
    pub has_mark_pos_custom_scan: bool,
    /// `RestrPosCustomScan` — optional mark/restore: restore position.
    pub has_restr_pos_custom_scan: bool,
    /// `EstimateDSMCustomScan` — optional parallel DSM-size estimator.
    pub has_estimate_dsm_custom_scan: bool,
    /// `InitializeDSMCustomScan` — optional parallel DSM initializer.
    pub has_initialize_dsm_custom_scan: bool,
    /// `ReInitializeDSMCustomScan` — optional parallel DSM re-initializer.
    pub has_reinitialize_dsm_custom_scan: bool,
    /// `InitializeWorkerCustomScan` — optional parallel worker initializer.
    pub has_initialize_worker_custom_scan: bool,
    /// `ShutdownCustomScan` — optional async/resource shutdown callback.
    pub has_shutdown_custom_scan: bool,
}

/// `CustomScan` plan node (nodes/plannodes.h), trimmed to the fields
/// `nodeCustom.c` consumes (`flags`, `custom_scan_tlist`, and the embedded
/// `Scan`); the remaining custom-code-private lists (`custom_plans`,
/// `custom_exprs`, `custom_private`, `custom_relids`) are carried for the
/// `copyObject` shape.
#[derive(Debug)]
pub struct CustomScan<'mcx> {
    /// `Scan scan` — the abstract scan-node base (carries the `Plan` head and
    /// `scanrelid`).
    pub scan: Scan<'mcx>,
    /// `uint32 flags` — mask of `CUSTOMPATH_*` flags (nodes/extensible.h).
    pub flags: u32,
    /// `List *custom_plans` — list of child `Plan` nodes, if any (`None` = the
    /// C `NIL`).
    pub custom_plans: Option<PgVec<'mcx, crate::nodes::Node<'mcx>>>,
    /// `List *custom_exprs` — expressions custom code may evaluate (`None` =
    /// the C `NIL`).
    pub custom_exprs: Option<PgVec<'mcx, Expr>>,
    /// `List *custom_private` — private data for custom code (`None` = the C
    /// `NIL`). Carried opaquely.
    pub custom_private: Opaque,
    /// `List *custom_scan_tlist` — optional tlist describing the scan tuple
    /// (`None` = the C `NIL`).
    pub custom_scan_tlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `Bitmapset *custom_relids` — RTIs generated by this scan (`None` = the
    /// C `NULL`).
    pub custom_relids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `const struct CustomScanMethods *methods` — the provider's
    /// planner-side method table (a pointer to a static table in C). Carried
    /// opaquely; `nodeCustom.c` only invokes `CreateCustomScanState` through it,
    /// which crosses the provider seam.
    pub methods: Opaque,
}

impl Default for CustomScan<'_> {
    /// C `makeNode(CustomScan)` zero-init.
    fn default() -> Self {
        CustomScan {
            scan: Scan::default(),
            flags: 0,
            custom_plans: None,
            custom_exprs: None,
            custom_private: Opaque::default(),
            custom_scan_tlist: None,
            custom_relids: None,
            methods: Opaque::default(),
        }
    }
}

impl CustomScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). The `methods` table is a
    /// pointer to a static table — not copied, just referenced — so it carries
    /// across opaquely.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CustomScan<'b>> {
        let custom_plans = match &self.custom_plans {
            Some(ps) => {
                let mut out = vec_with_capacity_in(mcx, ps.len())?;
                for p in ps.iter() {
                    out.push(p.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let custom_exprs = match &self.custom_exprs {
            Some(es) => {
                let mut out = vec_with_capacity_in(mcx, es.len())?;
                for e in es.iter() {
                    // Deep-copy via `clone_in`, not the derived `Expr::clone`
                    // (which panics on a `SubPlan` arm).
                    out.push(e.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let custom_scan_tlist = match &self.custom_scan_tlist {
            Some(tl) => {
                let mut out = vec_with_capacity_in(mcx, tl.len())?;
                for tle in tl.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let custom_relids = match &self.custom_relids {
            Some(b) => Some(mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        };
        Ok(CustomScan {
            scan: self.scan.clone_in(mcx)?,
            flags: self.flags,
            custom_plans,
            custom_exprs,
            custom_private: Opaque::default(),
            custom_scan_tlist,
            custom_relids,
            methods: Opaque::default(),
        })
    }
}

/// `CustomScanState` (executor/execnodes.h), trimmed:
///
/// ```c
/// typedef struct CustomScanState {
///     ScanState   ss;
///     uint32      flags;
///     List       *custom_ps;
///     Size        pscan_len;
///     const struct CustomExecMethods *methods;
///     const struct TupleTableSlotOps *slotOps;
/// } CustomScanState;
/// ```
#[derive(Debug, Default)]
pub struct CustomScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `uint32 flags` — mask of `CUSTOMPATH_*` flags (copied from the plan).
    pub flags: u32,
    /// `List *custom_ps` — list of child `PlanState` nodes, if any (`None` =
    /// the C `NIL`).
    pub custom_ps: Option<PgVec<'mcx, PgBox<'mcx, crate::planstate::PlanStateNode<'mcx>>>>,
    /// `Size pscan_len` — size of parallel coordination information.
    pub pscan_len: usize,
    /// `const struct CustomExecMethods *methods` — the provider's executor
    /// method table (`None` = before the provider's `CreateCustomScanState`
    /// has set it). The node reads its presence flags and `CustomName`; the
    /// callback invocations cross the provider seam.
    pub methods: Option<CustomExecMethods>,
    /// `const struct TupleTableSlotOps *slotOps` — the provider's chosen scan
    /// slot class (`None` = the C `NULL`, meaning "use `&TTSOpsVirtual`").
    pub slotOps: Option<TupleSlotKind>,
}

impl<'mcx> CustomScanState<'mcx> {
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
