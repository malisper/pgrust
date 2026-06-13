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
    /// `List *targetlist` — target list to be computed at this node
    /// (`None` = the C `NIL`).
    pub targetlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *qual` — implicitly-ANDed qual conditions (`None` = the C `NIL`).
    pub qual: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `bool parallel_aware` — engage parallel-aware logic?
    pub parallel_aware: bool,
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
        Ok(Plan {
            targetlist,
            qual,
            parallel_aware: self.parallel_aware,
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
    /// `List *resultRelations` — integer list of RT indexes of the query's
    /// target relations (`None` = the C `NIL`).
    pub resultRelations: Option<PgVec<'mcx, i32>>,
}
