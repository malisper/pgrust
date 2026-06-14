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

use mcx::{Mcx, PgBox};
use types_error::PgResult;

use crate::execnodes::ScanStateData;
use crate::funcapi::Tuplestorestate;
use crate::nodeindexscan::Scan;
use crate::nodes::NodeTag;
use crate::planstate::PlanStateNode;

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
    /// `PlanState *cteplanstate` — `PlanState` for the CTE query itself, found
    /// in `EState.es_subplanstates`. `None` until linked at init.
    pub cteplanstate: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `Tuplestorestate *cte_table` — rows already read from the CTE query.
    /// Only valid in the leader (`None` in a follower / before init).
    pub cte_table: Option<PgBox<'mcx, Tuplestorestate<'mcx>>>,
    /// `bool eof_cte` — reached end of CTE query? Only valid in the leader.
    pub eof_cte: bool,
}

impl<'mcx> CteScanState<'mcx> {
    /// `makeNode(CteScanState)`-shaped construction: a palloc0 state, allocated
    /// in `mcx`.
    pub fn new_in(_mcx: Mcx<'mcx>) -> Self {
        CteScanState {
            ss: ScanStateData::default(),
            eflags: 0,
            readptr: 0,
            cteplanstate: None,
            cte_table: None,
            eof_cte: false,
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
