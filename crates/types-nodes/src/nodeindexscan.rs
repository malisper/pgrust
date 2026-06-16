//! Plan-node base vocabulary (nodes/plannodes.h), trimmed.
//!
//! src-idiomatic hosts the canonical `Plan` base in this module; the name is
//! preserved. Trimmed to the fields ports consume (`outerPlan(node)` =
//! `plan.lefttree`); cost/targetlist/qual fields arrive with the units that
//! read them.

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::primnodes::TargetEntry;

/// `CUSTOMPATH_SUPPORT_BACKWARD_SCAN` (nodes/extensible.h) — custom path/scan
/// flag: supports backward scanning.
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0001;
/// `CUSTOMPATH_SUPPORT_MARK_RESTORE` (nodes/extensible.h) — custom path/scan
/// flag: supports mark/restore.
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;

/// `Plan` (nodes/plannodes.h) — the abstract base every plan-tree node embeds
/// first. The child links are context-allocated (the plan tree lives in a
/// memory context); copying a plan tree allocates, so it goes through the
/// fallible [`Plan::clone_in`] rather than a derived `Clone`.
#[derive(Debug, Default)]
pub struct Plan<'mcx> {
    /// `int disabled_nodes` — count of disabled nodes at and below this plan
    /// node (the planner's `enable_*`-GUC disable accumulator; created by
    /// costsize and propagated up the plan tree in createplan).
    pub disabled_nodes: i32,
    /// `Cost startup_cost` — cost expended before fetching any tuples. `Cost`
    /// is `double` in C.
    pub startup_cost: f64,
    /// `Cost total_cost` — total cost (assuming all tuples fetched).
    pub total_cost: f64,
    /// `List *targetlist` — target list to be computed at this node
    /// (`None` = the C `NIL`).
    pub targetlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *qual` — implicitly-ANDed qual conditions (`None` = the C `NIL`).
    pub qual: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `Cardinality plan_rows` — estimated number of rows this node emits.
    pub plan_rows: f64,
    /// `bool parallel_aware` — engage parallel-aware logic?
    pub parallel_aware: bool,
    /// `bool parallel_safe` — OK to use as part of a parallel plan? Set by
    /// `copy_generic_path_info` / `copy_plan_costsize` (createplan.c) from the
    /// `Path`'s parallel-safety; read by setrefs.c and the parallel planner.
    pub parallel_safe: bool,
    /// `bool async_capable` — engage asynchronous-capable logic?
    pub async_capable: bool,
    /// `int plan_node_id` — unique across the entire final plan tree; used as
    /// the DSM TOC key for a node's parallel state.
    pub plan_node_id: i32,
    /// `int plan_width` — average row width in bytes. Consumed alongside
    /// `plan_rows` when sizing the hash table.
    pub plan_width: i32,
    /// `List *initPlan` — Init `SubPlan` nodes (un-correlated expr subselects).
    /// `None` is the C `NIL`. Each element is a `SubPlan` expression node
    /// (`nodes/primnodes.h`); the planner attaches a node's init-plans here, and
    /// `ExecInitNode` walks this list building each one's `SubPlanState` via
    /// `ExecInitSubPlan`.
    pub initPlan: Option<PgVec<'mcx, crate::primnodes::SubPlan<'mcx>>>,
    /// `struct Plan *lefttree` — input plan tree (`outerPlan(node)`).
    pub lefttree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `struct Plan *righttree` — `innerPlan(node)`.
    pub righttree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `Bitmapset *extParam` — indices of all external `PARAM_EXEC` params
    /// affecting this plan node or its children.
    pub extParam: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *allParam` — all PARAM_EXEC params the node depends on.
    pub allParam: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl Plan<'_> {
    /// Deep copy of the plan subtree into `mcx` (C: `copyObject` shape).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Plan<'b>> {
        let targetlist = match &self.targetlist {
            Some(tlist) => {
                let mut out = vec_with_capacity_in(mcx, tlist.len())?;
                for tle in tlist.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let qual = match &self.qual {
            Some(q) => {
                let mut out = vec_with_capacity_in(mcx, q.len())?;
                for e in q.iter() {
                    out.push(e.clone());
                }
                Some(out)
            }
            None => None,
        };
        let initPlan = match &self.initPlan {
            Some(list) => {
                let mut out = vec_with_capacity_in(mcx, list.len())?;
                for sp in list.iter() {
                    out.push(sp.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(Plan {
            disabled_nodes: self.disabled_nodes,
            startup_cost: self.startup_cost,
            total_cost: self.total_cost,
            async_capable: self.async_capable,
            plan_node_id: self.plan_node_id,
            targetlist,
            qual,
            plan_rows: self.plan_rows,
            parallel_aware: self.parallel_aware,
            parallel_safe: self.parallel_safe,
            plan_width: self.plan_width,
            initPlan,
            lefttree: match &self.lefttree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            righttree: match &self.righttree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            extParam: match &self.extParam {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            allParam: match &self.allParam {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
}

/// `Scan` (nodes/plannodes.h) — the abstract base every scan plan node embeds:
///
/// ```c
/// typedef struct Scan { Plan plan; Index scanrelid; } Scan;
/// ```
#[derive(Debug, Default)]
pub struct Scan<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `Index scanrelid` — relid is index into the range table.
    pub scanrelid: types_core::primitive::Index,
}

impl Scan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Scan<'b>> {
        Ok(Scan {
            plan: self.plan.clone_in(mcx)?,
            scanrelid: self.scanrelid,
        })
    }
}

/// `IndexScan` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct IndexScan
/// {
///     Scan          scan;
///     Oid           indexid;
///     List         *indexqual;
///     List         *indexqualorig;
///     List         *indexorderby;
///     List         *indexorderbyorig;
///     List         *indexorderbyops;
///     ScanDirection indexorderdir;
/// } IndexScan;
/// ```
#[derive(Debug)]
pub struct IndexScan<'mcx> {
    /// `Scan scan` — the abstract scan base (embeds `Plan plan` first).
    pub scan: Scan<'mcx>,
    /// `Oid indexid` — OID of index to scan.
    pub indexid: types_core::Oid,
    /// `List *indexqual` — list of index quals (usually OpExprs).
    pub indexqual: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `List *indexqualorig` — the same in original form.
    pub indexqualorig: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `List *indexorderby` — list of index ORDER BY exprs.
    pub indexorderby: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `List *indexorderbyorig` — the same in original form.
    pub indexorderbyorig: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `List *indexorderbyops` — OIDs of sort ops for ORDER BY exprs.
    pub indexorderbyops: Option<PgVec<'mcx, types_core::Oid>>,
    /// `ScanDirection indexorderdir` — forward or backward or don't care.
    pub indexorderdir: types_scan::sdir::ScanDirection,
}

impl IndexScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IndexScan<'b>> {
        let clone_exprs = |src: &Option<PgVec<'_, crate::primnodes::Expr>>|
            -> PgResult<Option<PgVec<'b, crate::primnodes::Expr>>> {
            match src {
                Some(list) => {
                    let mut out = vec_with_capacity_in(mcx, list.len())?;
                    for e in list.iter() {
                        out.push(e.clone());
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        };
        let indexorderbyops = match &self.indexorderbyops {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(*x);
                }
                Some(out)
            }
            None => None,
        };
        Ok(IndexScan {
            scan: self.scan.clone_in(mcx)?,
            indexid: self.indexid,
            indexqual: clone_exprs(&self.indexqual)?,
            indexqualorig: clone_exprs(&self.indexqualorig)?,
            indexorderby: clone_exprs(&self.indexorderby)?,
            indexorderbyorig: clone_exprs(&self.indexorderbyorig)?,
            indexorderbyops,
            indexorderdir: self.indexorderdir,
        })
    }
}

/// `SubqueryScanStatus` (nodes/plannodes.h) — caches the trivial-subqueryscan
/// property of the node; `SUBQUERY_SCAN_UNKNOWN` means not yet determined (only
/// used during planning).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SubqueryScanStatus {
    /// `SUBQUERY_SCAN_UNKNOWN`.
    #[default]
    Unknown = 0,
    /// `SUBQUERY_SCAN_TRIVIAL`.
    Trivial = 1,
    /// `SUBQUERY_SCAN_NONTRIVIAL`.
    Nontrivial = 2,
}

/// `SubqueryScan` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct SubqueryScan {
///     Scan        scan;
///     Plan       *subplan;
///     SubqueryScanStatus scanstatus;
/// } SubqueryScan;
/// ```
#[derive(Debug, Default)]
pub struct SubqueryScan<'mcx> {
    /// `Scan scan` — the abstract scan base (embeds `Plan plan` first).
    pub scan: Scan<'mcx>,
    /// `Plan *subplan` — the child plan producing the subquery's rows. Stored
    /// on the plan node (not in the generic `lefttree`), so plan-tree walkers do
    /// not recurse into it.
    pub subplan: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `SubqueryScanStatus scanstatus`.
    pub scanstatus: SubqueryScanStatus,
}

impl SubqueryScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SubqueryScan<'b>> {
        Ok(SubqueryScan {
            scan: self.scan.clone_in(mcx)?,
            subplan: match &self.subplan {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            scanstatus: self.scanstatus,
        })
    }
}

/// `TidScan` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct TidScan { Scan scan; List *tidquals; } TidScan;
/// ```
#[derive(Debug, Default)]
pub struct TidScan<'mcx> {
    /// `Scan scan` — the abstract scan base.
    pub scan: Scan<'mcx>,
    /// `List *tidquals` — qual(s) involving CTID = something, or CTID = ANY
    /// (...), or CURRENT OF cursor. `None` = the C `NIL`.
    pub tidquals: Option<PgVec<'mcx, crate::primnodes::Expr>>,
}

impl TidScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TidScan<'b>> {
        let tidquals = match &self.tidquals {
            Some(q) => {
                let mut out = vec_with_capacity_in(mcx, q.len())?;
                for e in q.iter() {
                    out.push(e.clone());
                }
                Some(out)
            }
            None => None,
        };
        Ok(TidScan {
            scan: self.scan.clone_in(mcx)?,
            tidquals,
        })
    }
}

/// `PlannedStmt` (nodes/plannodes.h), trimmed to the fields ports consume.
#[derive(Debug, Default)]
pub struct PlannedStmt<'mcx> {
    /// `CmdType commandType` — select|insert|update|delete|merge|utility.
    pub commandType: crate::nodes::CmdType,
    /// `Node *utilityStmt` — non-null if this is a `CMD_UTILITY` PlannedStmt;
    /// the utility parse node to dispatch.
    pub utilityStmt: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `List *resultRelations` — integer list of RT indexes of the query's
    /// target relations (`None` = the C `NIL`).
    pub resultRelations: Option<PgVec<'mcx, i32>>,
    /// `List *relationOids` — OIDs of relations the plan depends on, used by
    /// COPY-(query)-TO's RLS double-check (`None` = the C `NIL`).
    pub relationOids: Option<PgVec<'mcx, types_core::Oid>>,
    /// `struct Plan *planTree` — tree of `Plan` nodes (`None` = the C `NULL`).
    pub planTree: Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>,
    /// `List *rowMarks` — a list of `PlanRowMark` nodes (`None` = the C `NIL`).
    /// portalcmds only tests `rowMarks == NIL`; the elements arrive with the
    /// planner port.
    pub rowMarks: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `bool canSetTag` — do we set the command result tag/es_processed?
    /// `PortalGetPrimaryStmt` (portalmem.c) walks the portal's stmt list for
    /// the first stmt with this set.
    pub canSetTag: bool,
    /// `bool hasReturning` — is it insert|update|delete|merge RETURNING?
    /// (execMain `ExecutorStart` reads this to decide RETURNING projection;
    /// additive, defaults to the C `false`.)
    pub hasReturning: bool,
    /// `bool hasModifyingCTE` — has insert|update|delete|merge in WITH?
    /// (`ExecCheckXactReadOnly` forces parallel-unsafe when set.)
    pub hasModifyingCTE: bool,
    /// `bool parallelModeNeeded` — parallel mode required to execute?
    /// (`ExecutorStart` reads this with the parallel-mode GUC to decide
    /// `es_use_parallel_mode`.)
    pub parallelModeNeeded: bool,
    /// `int jitFlags` — which forms of JIT should be performed
    /// (`ExecutorStart` copies it into `es_jit_flags`).
    pub jitFlags: i32,
    /// `List *permInfos` — list of `RTEPermissionInfo` nodes for the query's
    /// RTEs (`ExecCheckPermissions` / `ExecCheckXactReadOnly` walk it). `None`
    /// = the C `NIL`. The trimmed `RTEPermissionInfo` (parsenodes.rs) carries
    /// only the fields its current consumers read; the permission-bit fields
    /// (`requiredPerms`/`selectedCols`) land with the full
    /// `ExecCheckPermissions` consumer (docs/types.md rule 3).
    pub permInfos: Option<PgVec<'mcx, crate::parsenodes::RTEPermissionInfo<'mcx>>>,
    /// `List *paramExecTypes` — type OIDs for `PARAM_EXEC` Params
    /// (`InitPlan` sizes `es_param_exec_vals` from this). `None` = the C `NIL`.
    pub paramExecTypes: Option<PgVec<'mcx, types_core::Oid>>,
    /// `List *rtable` — list of `RangeTblEntry` nodes (`None` = the C `NIL`).
    /// `InitPlan`/`ExecInitRangeTable` install this into `es_range_table`.
    pub rtable: Option<PgVec<'mcx, crate::parsenodes::RangeTblEntry<'mcx>>>,
    /// `Bitmapset *unprunableRelids` — RT indexes of relations not subject to
    /// runtime pruning (or needed to perform it). `InitPlan` passes this into
    /// `ExecInitRangeTable` as `es_unpruned_relids`. `None` = the C `NULL`.
    pub unprunableRelids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *subplans` — plan trees for `SubPlan` expressions; note that some
    /// elements can be `NULL` (hence the inner `Option`). `InitPlan` walks this
    /// to build `es_subplanstates`. `None` = the C `NIL`.
    pub subplans: Option<PgVec<'mcx, Option<PgBox<'mcx, crate::nodes::Node<'mcx>>>>>,
    /// `int stmt_location` — the start location, or -1 if unknown, of the
    /// statement's source text within the overall query string (set by the
    /// rewriter from the originating `Query`). `ProcessUtility` threads it to
    /// `DoCopy` / `PrepareQuery` so they can record the precise statement text.
    pub stmt_location: i32,
    /// `int stmt_len` — the length in bytes of the statement's source text, or 0
    /// if unknown/unset.
    pub stmt_len: i32,
}

impl PlannedStmt<'_> {
    /// `copyObject(plannedstmt)` shape — deep copy into `mcx`. Fallible:
    /// copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PlannedStmt<'b>> {
        let resultRelations = match &self.resultRelations {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(*x);
                }
                Some(out)
            }
            None => None,
        };
        let rowMarks = match &self.rowMarks {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(x.clone());
                }
                Some(out)
            }
            None => None,
        };
        let relationOids = match &self.relationOids {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(*x);
                }
                Some(out)
            }
            None => None,
        };
        let permInfos = match &self.permInfos {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(x.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let paramExecTypes = match &self.paramExecTypes {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(*x);
                }
                Some(out)
            }
            None => None,
        };
        let rtable = match &self.rtable {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(x.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        let subplans = match &self.subplans {
            Some(v) => {
                let mut out = vec_with_capacity_in(mcx, v.len())?;
                for x in v.iter() {
                    out.push(match x {
                        Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                        None => None,
                    });
                }
                Some(out)
            }
            None => None,
        };
        Ok(PlannedStmt {
            commandType: self.commandType,
            utilityStmt: match &self.utilityStmt {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            resultRelations,
            relationOids,
            planTree: match &self.planTree {
                Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                None => None,
            },
            rowMarks,
            canSetTag: self.canSetTag,
            hasReturning: self.hasReturning,
            hasModifyingCTE: self.hasModifyingCTE,
            parallelModeNeeded: self.parallelModeNeeded,
            jitFlags: self.jitFlags,
            permInfos,
            paramExecTypes,
            rtable,
            unprunableRelids: match &self.unprunableRelids {
                Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            subplans,
            stmt_location: self.stmt_location,
            stmt_len: self.stmt_len,
        })
    }
}
