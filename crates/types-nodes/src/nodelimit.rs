//! Limit node vocabulary (nodes/nodes.h, nodes/plannodes.h, executor/execnodes.h).
//!
//! The `Limit` plan node, the `LimitState` executor state, and the
//! `LimitOption` / `LimitStateCond` enums consumed by `nodeLimit.c`.

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::int64;
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;

use crate::execexpr::ExprState;
use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;
use crate::primnodes::Expr;

/// `T_LimitState` (nodes/nodetags.h) — value verified against PostgreSQL 18.3.
pub const T_LimitState: NodeTag = NodeTag(437);

/// `LimitOption` (nodes/nodes.h) — the limit-specification type.
///
/// ```c
/// typedef enum LimitOption
/// {
///     LIMIT_OPTION_COUNT,         /* FETCH FIRST... ONLY */
///     LIMIT_OPTION_WITH_TIES,     /* FETCH FIRST... WITH TIES */
/// } LimitOption;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum LimitOption {
    /// `LIMIT_OPTION_COUNT` — FETCH FIRST... ONLY.
    LIMIT_OPTION_COUNT = 0,
    /// `LIMIT_OPTION_WITH_TIES` — FETCH FIRST... WITH TIES.
    LIMIT_OPTION_WITH_TIES = 1,
}
pub use LimitOption::*;

impl Default for LimitOption {
    fn default() -> Self {
        LimitOption::LIMIT_OPTION_COUNT
    }
}

/// `LimitStateCond` (executor/execnodes.h) — the LIMIT node's state-machine
/// status.
///
/// ```c
/// typedef enum
/// {
///     LIMIT_INITIAL,          /* initial state for LIMIT node */
///     LIMIT_RESCAN,           /* rescan after recomputing parameters */
///     LIMIT_EMPTY,            /* there are no returnable rows */
///     LIMIT_INWINDOW,         /* have returned a row in the window */
///     LIMIT_WINDOWEND_TIES,   /* have returned a tied row */
///     LIMIT_SUBPLANEOF,       /* at EOF of subplan (within window) */
///     LIMIT_WINDOWEND,        /* stepped off end of window */
///     LIMIT_WINDOWSTART,      /* stepped off beginning of window */
/// } LimitStateCond;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum LimitStateCond {
    /// `LIMIT_INITIAL` — initial state for LIMIT node.
    LIMIT_INITIAL = 0,
    /// `LIMIT_RESCAN` — rescan after recomputing parameters.
    LIMIT_RESCAN = 1,
    /// `LIMIT_EMPTY` — there are no returnable rows.
    LIMIT_EMPTY = 2,
    /// `LIMIT_INWINDOW` — have returned a row in the window.
    LIMIT_INWINDOW = 3,
    /// `LIMIT_WINDOWEND_TIES` — have returned a tied row.
    LIMIT_WINDOWEND_TIES = 4,
    /// `LIMIT_SUBPLANEOF` — at EOF of subplan (within window).
    LIMIT_SUBPLANEOF = 5,
    /// `LIMIT_WINDOWEND` — stepped off end of window.
    LIMIT_WINDOWEND = 6,
    /// `LIMIT_WINDOWSTART` — stepped off beginning of window.
    LIMIT_WINDOWSTART = 7,
}
pub use LimitStateCond::*;

impl Default for LimitStateCond {
    fn default() -> Self {
        LimitStateCond::LIMIT_INITIAL
    }
}

/// `Limit` plan node (nodes/plannodes.h):
///
/// ```c
/// typedef struct Limit
/// {
///     Plan        plan;
///     Node       *limitOffset;     /* OFFSET parameter, or NULL if none */
///     Node       *limitCount;      /* COUNT parameter, or NULL if none */
///     LimitOption limitOption;     /* limit type */
///     int         uniqNumCols;     /* number of columns to check for similarity */
///     AttrNumber *uniqColIdx;      /* their indexes in the target list */
///     Oid        *uniqOperators;   /* equality operators to compare with */
///     Oid        *uniqCollations;  /* collations for equality comparisons */
/// } Limit;
/// ```
#[derive(Debug, Default)]
pub struct Limit<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `Node *limitOffset` — OFFSET parameter, or `None` if none. C types it
    /// `Node *`, but the planner only ever stores an expression here (the
    /// OFFSET expression), and `ExecInitLimit` casts it `(Expr *)`; the owned
    /// model stores the `Expr` directly.
    pub limitOffset: Option<PgBox<'mcx, Expr>>,
    /// `Node *limitCount` — COUNT parameter, or `None` if none (see
    /// `limitOffset`).
    pub limitCount: Option<PgBox<'mcx, Expr>>,
    /// `LimitOption limitOption` — limit type.
    pub limitOption: LimitOption,
    /// `int uniqNumCols` — number of columns to check for similarity.
    pub uniqNumCols: i32,
    /// `AttrNumber *uniqColIdx` — their indexes in the target list (`None` is
    /// the C `NULL`, used when `uniqNumCols == 0`).
    pub uniqColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *uniqOperators` — equality operators to compare with.
    pub uniqOperators: Option<PgVec<'mcx, Oid>>,
    /// `Oid *uniqCollations` — collations for equality comparisons.
    pub uniqCollations: Option<PgVec<'mcx, Oid>>,
}

impl Limit<'_> {
    /// Deep copy of the node (and its plan subtree) into `mcx`
    /// (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Limit<'b>> {
        let limitOffset = match &self.limitOffset {
            Some(n) => Some(alloc_in(mcx, (**n).clone())?),
            None => None,
        };
        let limitCount = match &self.limitCount {
            Some(n) => Some(alloc_in(mcx, (**n).clone())?),
            None => None,
        };
        let uniqColIdx = clone_vec(&self.uniqColIdx, mcx)?;
        let uniqOperators = clone_vec(&self.uniqOperators, mcx)?;
        let uniqCollations = clone_vec(&self.uniqCollations, mcx)?;
        Ok(Limit {
            plan: self.plan.clone_in(mcx)?,
            limitOffset,
            limitCount,
            limitOption: self.limitOption,
            uniqNumCols: self.uniqNumCols,
            uniqColIdx,
            uniqOperators,
            uniqCollations,
        })
    }
}

/// Deep-copy a `Option<PgVec<T>>` of `Copy` scalars into `mcx`.
fn clone_vec<'b, T: Copy>(
    src: &Option<PgVec<'_, T>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgVec<'b, T>>> {
    match src {
        Some(v) => {
            let mut out = vec_with_capacity_in(mcx, v.len())?;
            for x in v.iter() {
                out.push(*x);
            }
            Ok(Some(out))
        }
        None => Ok(None),
    }
}

/// `LimitState` (executor/execnodes.h):
///
/// ```c
/// typedef struct LimitState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     ExprState  *limitOffset;    /* OFFSET parameter, or NULL if none */
///     ExprState  *limitCount;     /* COUNT parameter, or NULL if none */
///     LimitOption limitOption;    /* limit specification type */
///     int64       offset;         /* current OFFSET value */
///     int64       count;          /* current COUNT, if any */
///     bool        noCount;        /* if true, ignore count */
///     LimitStateCond lstate;      /* state machine status, as above */
///     int64       position;       /* 1-based index of last tuple returned */
///     TupleTableSlot *subSlot;    /* tuple last obtained from subplan */
///     ExprState  *eqfunction;     /* tuple equality qual in case of WITH TIES */
///     TupleTableSlot *last_slot;  /* slot for evaluation of ties */
/// } LimitState;
/// ```
#[derive(Debug, Default)]
pub struct LimitStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `ExprState *limitOffset` — OFFSET parameter, or `None` if none.
    pub limitOffset: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `ExprState *limitCount` — COUNT parameter, or `None` if none.
    pub limitCount: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `LimitOption limitOption` — limit specification type.
    pub limitOption: LimitOption,
    /// `int64 offset` — current OFFSET value.
    pub offset: int64,
    /// `int64 count` — current COUNT, if any.
    pub count: int64,
    /// `bool noCount` — if true, ignore count.
    pub noCount: bool,
    /// `LimitStateCond lstate` — state machine status.
    pub lstate: LimitStateCond,
    /// `int64 position` — 1-based index of last tuple returned.
    pub position: int64,
    /// `TupleTableSlot *subSlot` — tuple last obtained from subplan (id into
    /// `es_tupleTable`; C's pointer alias of the child's returned slot).
    pub subSlot: Option<SlotId>,
    /// `ExprState *eqfunction` — tuple equality qual in case of WITH TIES.
    pub eqfunction: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `TupleTableSlot *last_slot` — slot for evaluation of ties (a persistent
    /// node-owned arena slot; WITH TIES copies the boundary row's datums in).
    pub last_slot: Option<SlotId>,
}

impl<'mcx> LimitStateData<'mcx> {
    /// `&node->ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &PlanStateData<'mcx> {
        &self.ps
    }

    /// `&mut node->ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut PlanStateData<'mcx> {
        &mut self.ps
    }
}
