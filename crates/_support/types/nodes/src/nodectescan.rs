//! CteScan plan-node / executor-state vocabulary (`nodes/plannodes.h`,
//! `executor/execnodes.h`, `executor/nodeCtescan.c`).
//!
//! A `CteScan` node scans the output of a `WITH` (CTE) query, materialized in a
//! shared `Tuplestorestate`. Several `CteScan` nodes can read from the same CTE:
//! the first one to initialize becomes the "leader" and owns the shared store;
//! the others allocate their own read pointers into it. The `leader` link is an
//! aliased self-/cross-reference into the executor-owned node graph (the C
//! `struct CteScanState *leader`), resolved through the owning crate's seams;
//! the leader-only fields (`cte_table`, `eof_cte`) are valid only in the leader,
//! exactly as the C documents.

use mcx::Mcx;
use types_error::PgResult;

use crate::execnodes::ScanStateData;
use crate::nodeindexscan::Scan;
use crate::nodes::NodeTag;

// ===========================================================================
// NodeTags (nodes/nodetags.h, PostgreSQL 18.3 generated order).
// ===========================================================================

/// `T_CteScan` — the CteScan plan node tag. Verified against PostgreSQL 18.3.
pub const T_CteScan: NodeTag = NodeTag(351);
/// `T_CteScanState` — the CteScan executor-state node tag. Verified against
/// PostgreSQL 18.3.
pub const T_CteScanState: NodeTag = NodeTag(415);

// ===========================================================================
// CteScan plan node (nodes/plannodes.h).
// ===========================================================================

/// `CteScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct CteScan {
///     Scan        scan;
///     int         ctePlanId;  /* ID of init SubPlan for CTE */
///     int         cteParam;   /* ID of Param representing CTE output */
/// } CteScan;
/// ```
#[derive(Debug, Default)]
pub struct CteScan<'mcx> {
    /// `Scan scan` — the abstract scan-plan base (embeds `Plan plan`).
    pub scan: Scan<'mcx>,
    /// `int ctePlanId` — ID (1-based) of the init `SubPlan` for the CTE,
    /// indexing `EState.es_subplanstates`.
    pub ctePlanId: i32,
    /// `int cteParam` — ID of the `Param` representing the CTE output, indexing
    /// `EState.es_param_exec_vals`.
    pub cteParam: i32,
}

// ===========================================================================
// CteScanState executor state (executor/execnodes.h).
// ===========================================================================

/// `CteScanState` (execnodes.h):
///
/// ```c
/// typedef struct CteScanState {
///     ScanState   ss;              /* its first field is NodeTag */
///     int         eflags;          /* capability flags to pass to tuplestore */
///     int         readptr;         /* index of my tuplestore read pointer */
///     PlanState  *cteplanstate;    /* PlanState for the CTE query itself */
///     struct CteScanState *leader; /* Link to the "leader" CteScanState */
///     Tuplestorestate *cte_table;  /* rows already read from the CTE query */
///     bool        eof_cte;         /* reached end of CTE query? */
/// } CteScanState;
/// ```
///
/// The `leader` link is the aliased self-/cross-reference into the
/// executor-owned node graph: it may point at this same node or at another live
/// `CteScanState`. A live mutable alias into another owned node cannot be held
/// in safe Rust, so the leader-resolved operations (reading/setting the leader's
/// `eof_cte`, selecting/reading/writing the shared `cte_table`, the subplan
/// dispatch through the leader's `cteplanstate`, and the leader-identity test)
/// are reached through the owning crate's seams, which the executor's node-graph
/// driver installs. The leader-only fields below are valid only in the leader,
/// exactly as the C documents.
#[derive(Debug)]
pub struct CteScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `int eflags` — capability flags to pass to the tuplestore.
    pub eflags: i32,
    /// `int readptr` — index of my tuplestore read pointer.
    pub readptr: i32,
    /// `PlanState *cteplanstate` — the `PlanState` for the CTE query itself.
    ///
    /// In C this is a borrowed pointer `= list_nth(es_subplanstates,
    /// ctePlanId - 1)`. The owned model cannot hold a live alias into the
    /// `es_subplanstates`-owned plan-state (multiple followers share it and the
    /// end-of-plan teardown loop owns it), so — like
    /// [`SubPlanState`](crate::execnodes) reaching its child by `plan_id` index —
    /// this records the subplan's 1-based `ctePlanId` identity; the CTE's
    /// plan-state is reached at access time via
    /// `es_subplanstates[cte_plan_id - 1]`. `None` until linked at init.
    pub cte_plan_id: Option<i32>,
    /// `int cteParam` — index of this CTE's shared
    /// [`CteSharedState`](crate::execnodes::CteSharedState) in
    /// `EState.es_cte_shared` (the C `CteScan.cteParam`, which also keys
    /// `es_param_exec_vals`). The owned-model replacement for the aliasing
    /// `leader` back-pointer: leader and followers reach the shared
    /// `cte_table` / `eof_cte` by this index. `None` until resolved at init.
    pub cte_param: Option<i32>,
    /// Whether this node is the leader of its CTE (the C `node->leader == node`).
    /// The leader created the shared store and is responsible for freeing it in
    /// `ExecEndCteScan`. Set by `cte_resolve_leader`.
    pub is_leader: bool,
}

impl<'mcx> CteScanState<'mcx> {
    /// `makeNode(CteScanState)`-shaped construction: a palloc0 state, allocated
    /// in `mcx`.
    pub fn new_in(_mcx: Mcx<'mcx>) -> Self {
        CteScanState {
            ss: ScanStateData::default(),
            eflags: 0,
            readptr: 0,
            cte_plan_id: None,
            cte_param: None,
            is_leader: false,
        }
    }

    /// `&node->ss.ps` — the embedded `PlanState` head.
    #[inline]
    pub fn ps(&self) -> &crate::execnodes::PlanStateData<'mcx> {
        &self.ss.ps
    }

    /// `&mut node->ss.ps`.
    #[inline]
    pub fn ps_mut(&mut self) -> &mut crate::execnodes::PlanStateData<'mcx> {
        &mut self.ss.ps
    }
}

impl CteScan<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying
    /// allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CteScan<'b>> {
        Ok(CteScan {
            scan: self.scan.clone_in(mcx)?,
            ctePlanId: self.ctePlanId,
            cteParam: self.cteParam,
        })
    }
}
