//! Row-locking executor-node vocabulary (`nodes/plannodes.h` `LockRows` /
//! `PlanRowMark`, `nodes/execnodes.h` `LockRowsState` / `ExecRowMark` /
//! `ExecAuxRowMark` / `EPQState`).
//!
//! These mirror the PostgreSQL 18.3 node shapes so the `nodeLockRows.c` port
//! can navigate the fields it reads/writes (`erm->markType`, `erm->curCtid`,
//! `aerm->ctidAttNo`, ...). Every genuinely-external call (the table AM, the
//! FDW, EvalPlanQual, relcache, junk-attribute fetch) goes through the node
//! crate's per-owner seams; the tableam outcome types (`TM_Result` /
//! `TM_FailureData`) are not referenced here (they live in `types-tableam`,
//! which depends on this crate — naming them here would cycle), only in the
//! node-crate logic and its seams.

use mcx::{Mcx, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::PgResult;
use types_tuple::heaptuple::ItemPointerData;

use crate::execnodes::{PlanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::NodeTag;
pub use crate::nodes::T_LockRows;

/// `T_LockRowsState` (nodes/nodetags.h) — the executor-state node tag.
pub const T_LockRowsState: NodeTag = NodeTag(436);
/// `T_PlanRowMark` (nodes/nodetags.h) — the planner rowmark node tag.
pub const T_PlanRowMark: NodeTag = NodeTag(374);

/// `RowMarkType` (`nodes/plannodes.h`) — how a rowmark is enforced at runtime.
/// The first four (`<= ROW_MARK_KEYSHARE`) take a real tuple lock; the rest are
/// handled by the EvalPlanQual machinery.
pub type RowMarkType = i32;
/// `ROW_MARK_EXCLUSIVE` — obtain exclusive tuple lock.
pub const ROW_MARK_EXCLUSIVE: RowMarkType = 0;
/// `ROW_MARK_NOKEYEXCLUSIVE` — obtain no-key exclusive tuple lock.
pub const ROW_MARK_NOKEYEXCLUSIVE: RowMarkType = 1;
/// `ROW_MARK_SHARE` — obtain shared tuple lock.
pub const ROW_MARK_SHARE: RowMarkType = 2;
/// `ROW_MARK_KEYSHARE` — obtain keyshare tuple lock.
pub const ROW_MARK_KEYSHARE: RowMarkType = 3;
/// `ROW_MARK_REFERENCE` — just fetch the TID, don't lock it.
pub const ROW_MARK_REFERENCE: RowMarkType = 4;
/// `ROW_MARK_COPY` — physically copy the row value.
pub const ROW_MARK_COPY: RowMarkType = 5;

/// `RowMarkRequiresRowShareLock(marktype)` (`nodes/plannodes.h`) — true if the
/// rowmark takes a real RowShareLock (i.e. `marktype <= ROW_MARK_KEYSHARE`).
#[inline]
pub fn RowMarkRequiresRowShareLock(marktype: RowMarkType) -> bool {
    marktype <= ROW_MARK_KEYSHARE
}

/// `LockClauseStrength` (`nodes/lockoptions.h`) — `FOR UPDATE`/`FOR SHARE`
/// strength, or `LCS_NONE`.
pub type LockClauseStrength = i32;

/// `LockWaitPolicy` (`nodes/lockoptions.h`) — `NOWAIT`/`SKIP LOCKED` behavior.
pub type LockWaitPolicy = i32;
/// `LockWaitBlock` — wait for the lock to become available (default).
pub const LockWaitBlock: LockWaitPolicy = 0;
/// `LockWaitSkip` — skip rows that can't be locked (`SKIP LOCKED`).
pub const LockWaitSkip: LockWaitPolicy = 1;
/// `LockWaitError` — raise an error if a row cannot be locked (`NOWAIT`).
pub const LockWaitError: LockWaitPolicy = 2;

/// `LockRows` (`nodes/plannodes.h`) — the `FOR UPDATE/SHARE` row-locking plan
/// node.
///
/// ```c
/// typedef struct LockRows
/// {
///     Plan        plan;
///     List       *rowMarks;   /* a list of PlanRowMark's */
///     int         epqParam;   /* ID of Param for EvalPlanQual re-eval */
/// } LockRows;
/// ```
#[derive(Debug, Default)]
pub struct LockRows<'mcx> {
    /// `Plan plan` — the common plan-node base.
    pub plan: Plan<'mcx>,
    /// `List *rowMarks` — a list of `PlanRowMark`s.
    pub rowMarks: Option<PgVec<'mcx, PlanRowMark>>,
    /// `int epqParam` — ID of Param for EvalPlanQual re-eval.
    pub epqParam: i32,
}

impl LockRows<'_> {
    /// Deep copy into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<LockRows<'b>> {
        let rowMarks = match &self.rowMarks {
            Some(rms) => {
                let mut out = mcx::vec_with_capacity_in(mcx, rms.len())?;
                for rm in rms.iter() {
                    out.push(*rm);
                }
                Some(out)
            }
            None => None,
        };
        Ok(LockRows {
            plan: self.plan.clone_in(mcx)?,
            rowMarks,
            epqParam: self.epqParam,
        })
    }
}

/// `PlanRowMark` (`nodes/plannodes.h`) — a planner rowmark describing one
/// markable relation.
///
/// ```c
/// typedef struct PlanRowMark
/// {
///     NodeTag     type;
///     Index       rti;
///     Index       prti;
///     Index       rowmarkId;
///     RowMarkType markType;
///     int         allMarkTypes;
///     LockClauseStrength strength;
///     LockWaitPolicy waitPolicy;
///     bool        isParent;
/// } PlanRowMark;
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlanRowMark {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `Index rti` — range table index of markable relation.
    pub rti: Index,
    /// `Index prti` — range table index of parent relation.
    pub prti: Index,
    /// `Index rowmarkId` — unique identifier for resjunk columns.
    pub rowmarkId: Index,
    /// `RowMarkType markType`.
    pub markType: RowMarkType,
    /// `int allMarkTypes` — OR of (1<<markType) for all children.
    pub allMarkTypes: i32,
    /// `LockClauseStrength strength` — LockingClause's strength, or LCS_NONE.
    pub strength: LockClauseStrength,
    /// `LockWaitPolicy waitPolicy` — NOWAIT and SKIP LOCKED options.
    pub waitPolicy: LockWaitPolicy,
    /// `bool isParent` — true if this is a "dummy" parent entry.
    pub isParent: bool,
}

impl Default for PlanRowMark {
    fn default() -> Self {
        PlanRowMark {
            type_: T_PlanRowMark,
            rti: 0,
            prti: 0,
            rowmarkId: 0,
            markType: ROW_MARK_REFERENCE,
            allMarkTypes: 0,
            strength: 0,
            waitPolicy: LockWaitBlock,
            isParent: false,
        }
    }
}

/// `ExecRowMark` (`nodes/execnodes.h`) — execution-time state for a rowmark on
/// one range-table entry. Built by `InitPlan` and stored in `es_rowmarks`.
///
/// ```c
/// typedef struct ExecRowMark
/// {
///     Relation    relation;
///     Oid         relid;
///     Index       rti;
///     Index       prti;
///     Index       rowmarkId;
///     RowMarkType markType;
///     LockClauseStrength strength;
///     LockWaitPolicy waitPolicy;
///     bool        ermActive;
///     ItemPointerData curCtid;
///     void       *ermExtra;
/// } ExecRowMark;
/// ```
#[derive(Debug)]
pub struct ExecRowMark<'mcx> {
    /// `Relation relation` — opened and suitably locked relation. `None` (C
    /// NULL) for a non-relation rowmark. Stored as the Rc-backed
    /// [`types_rel::Relation`] alias (the same handle `ExecGetRangeTableRelation`
    /// returns and `ScanState::ss_currentRelation` holds); `es_relations` owns
    /// the open, this aliases it.
    pub relation: Option<types_rel::Relation<'mcx>>,
    /// `Oid relid` — its OID (or InvalidOid, if subquery).
    pub relid: Oid,
    /// `Index rti` — its range table index.
    pub rti: Index,
    /// `Index prti` — parent range table index, if child.
    pub prti: Index,
    /// `Index rowmarkId` — unique identifier for resjunk columns.
    pub rowmarkId: Index,
    /// `RowMarkType markType`.
    pub markType: RowMarkType,
    /// `LockClauseStrength strength` — LockingClause's strength, or LCS_NONE.
    pub strength: LockClauseStrength,
    /// `LockWaitPolicy waitPolicy` — NOWAIT and SKIP LOCKED.
    pub waitPolicy: LockWaitPolicy,
    /// `bool ermActive` — is this mark relevant for the current tuple?
    pub ermActive: bool,
    /// `ItemPointerData curCtid` — ctid of currently locked tuple, if any.
    pub curCtid: ItemPointerData,
    /// `void *ermExtra` — available for use by the relation source node. It is a
    /// bare `void *` "available for use by the plan node that sources the
    /// relation" with no PostgreSQL-defined type, so it stays opaque.
    pub ermExtra: Option<PgBox<'mcx, ErmExtra>>,
}

impl<'mcx> ExecRowMark<'mcx> {
    /// Same-context clone (the recheck EState shares the parent's per-query
    /// `'mcx`). The `relation` handle is an alias (Rc-backed); `ermExtra`
    /// re-allocates the (empty) opaque carrier. Used by `EvalPlanQualStart` to
    /// share the parent's rowmarks into the recheck EState (C aliases the same
    /// `ExecRowMark *`).
    pub fn clone_in(&self, mcx: mcx::Mcx<'mcx>) -> PgResult<ExecRowMark<'mcx>> {
        Ok(ExecRowMark {
            relation: self.relation.as_ref().map(|r| r.alias()),
            relid: self.relid,
            rti: self.rti,
            prti: self.prti,
            rowmarkId: self.rowmarkId,
            markType: self.markType,
            strength: self.strength,
            waitPolicy: self.waitPolicy,
            ermActive: self.ermActive,
            curCtid: self.curCtid,
            ermExtra: match self.ermExtra.as_ref() {
                Some(_) => Some(mcx::alloc_in(mcx, ErmExtra {})?),
                None => None,
            },
        })
    }
}

/// `ExecRowMark.ermExtra` is a bare `void *` with no PostgreSQL-defined type
/// (relation-source private user-data), so it stays opaque.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErmExtra {}

/// `ExecAuxRowMark` (`nodes/execnodes.h`) — pairs an `ExecRowMark` with the
/// resjunk column numbers the executor uses to fetch the row's ctid/tableoid.
///
/// ```c
/// typedef struct ExecAuxRowMark
/// {
///     ExecRowMark *rowmark;
///     AttrNumber  ctidAttNo;
///     AttrNumber  toidAttNo;
///     AttrNumber  wholeAttNo;
/// } ExecAuxRowMark;
/// ```
#[derive(Debug)]
pub struct ExecAuxRowMarkData<'mcx> {
    /// `ExecRowMark *rowmark` — related entry in `es_rowmarks`. C aliases the
    /// `es_rowmarks`-owned entry; the owned model holds it directly (the
    /// rowmark build hands ownership to the aux mark).
    pub rowmark: Option<PgBox<'mcx, ExecRowMark<'mcx>>>,
    /// `AttrNumber ctidAttNo` — resno of ctid junk attribute, if any.
    pub ctidAttNo: AttrNumber,
    /// `AttrNumber toidAttNo` — resno of tableoid junk attribute, if any.
    pub toidAttNo: AttrNumber,
    /// `AttrNumber wholeAttNo` — resno of whole-row junk attribute, if any.
    pub wholeAttNo: AttrNumber,
}

impl<'mcx> ExecAuxRowMarkData<'mcx> {
    /// Same-context clone (the recheck EState shares the parent's per-query
    /// `'mcx`). `EvalPlanQualStart` clones the non-locking aux rowmarks onto the
    /// recheck estate's `es_epq_active` marker so `EvalPlanQualFetchRowMark`
    /// (which runs threaded with the recheck estate) can reach the `ExecRowMark`
    /// + the resjunk column numbers. C aliases the single `ExecAuxRowMark *`.
    pub fn clone_in(&self, mcx: mcx::Mcx<'mcx>) -> PgResult<ExecAuxRowMarkData<'mcx>> {
        Ok(ExecAuxRowMarkData {
            rowmark: match self.rowmark.as_deref() {
                Some(e) => Some(mcx::alloc_in(mcx, e.clone_in(mcx)?)?),
                None => None,
            },
            ctidAttNo: self.ctidAttNo,
            toidAttNo: self.toidAttNo,
            wholeAttNo: self.wholeAttNo,
        })
    }
}

/// `EPQState` (`nodes/execnodes.h`) — EvalPlanQual recheck state. The canonical
/// owned `EPQState` lives in [`crate::execnodes`] (the same struct
/// `ModifyTableState::mt_epqstate` holds and the execMain EPQ machinery
/// populates: `relsubs_slot`/`relsubs_rowmark`/`relsubs_done`/`relsubs_blocked`/
/// `resultRelations`). `LockRowsState::lr_epqstate` carries that canonical type
/// so the execMain-owned EPQ seams (`EvalPlanQualInit`/`Slot`/`Begin`/`Next`/
/// `End`) operate on the real fields.
pub use crate::execnodes::EPQState;

/// `LockRowsState` (`nodes/execnodes.h`) — the `LockRows` executor node's
/// run-time state.
///
/// ```c
/// typedef struct LockRowsState
/// {
///     PlanState   ps;             /* its first field is NodeTag */
///     List       *lr_arowMarks;   /* List of ExecAuxRowMarks */
///     EPQState    lr_epqstate;    /* for evaluating EvalPlanQual rechecks */
/// } LockRowsState;
/// ```
#[derive(Debug)]
pub struct LockRowsStateData<'mcx> {
    /// `PlanState ps` — the common plan-node base.
    pub ps: PlanStateData<'mcx>,
    /// `List *lr_arowMarks` — list of locking `ExecAuxRowMark`s.
    pub lr_arowMarks: PgVec<'mcx, ExecAuxRowMarkData<'mcx>>,
    /// `EPQState lr_epqstate` — for evaluating EvalPlanQual rechecks.
    pub lr_epqstate: EPQState<'mcx>,
    /// The node's working "outer" slot — the `slot` local in C `ExecLockRows`,
    /// the tuple just pulled from the subplan. C re-reads it through `slot`
    /// after the lock loop (for `EvalPlanQualSetSlot`/return); the owned model
    /// stashes its [`SlotId`] here so the EPQ seams (which only carry the node
    /// handle) can reach it. `None` is the C `slot == NULL`.
    pub lr_curOuterSlot: Option<SlotId>,
}

impl<'mcx> LockRowsStateData<'mcx> {
    /// `makeNode(LockRowsState)` — palloc0 the node. (The C `NodeTag` head stamp
    /// is conveyed by the [`crate::planstate::PlanStateNode::LockRows`] variant
    /// the central dispatch matches on, not a tag field on the trimmed
    /// [`PlanStateData`].)
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        LockRowsStateData {
            ps: PlanStateData::default(),
            lr_arowMarks: PgVec::new_in(mcx),
            lr_epqstate: EPQState::default(),
            lr_curOuterSlot: None,
        }
    }
}
