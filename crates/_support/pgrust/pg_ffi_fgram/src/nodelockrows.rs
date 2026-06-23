//! Row-locking executor-node ABI vocabulary for `nodeLockRows.c`.
//!
//! These structs cross the boundary between the `LockRows` executor node, the
//! planner's `LockRows`/`PlanRowMark` plan nodes, the per-query `ExecRowMark`s,
//! and the table access method's tuple-lock interface (`table_tuple_lock`). They
//! mirror the PostgreSQL 18.3 `#[repr(C)]` layout exactly so the node-state crate
//! can navigate the fields it reads/writes (`erm->markType`, `erm->curCtid`,
//! `aerm->ctidAttNo`, the `TM_Result`/`TM_FailureData` lock outcome, ...) while
//! every genuinely-external call (the table AM, the FDW, EvalPlanQual, relcache)
//! goes through the node crate's runtime seam. Compile-time size/align/offset
//! assertions pin the layout where it crosses the ABI.

use core::ffi::{c_int, c_void};

use crate::nodeindexscan::Plan;
use crate::{
    AttrNumber, CommandId, EPQState, ItemPointerData, List, Oid, PlanStateData, Relation,
    TransactionId,
};

/// `Index` (`c.h`) — a 1-based range-table index.
pub type Index = c_int;

/// `RowMarkType` (`nodes/plannodes.h`) — how a rowmark should be enforced at
/// runtime. The first four (`<= ROW_MARK_KEYSHARE`) take a real tuple lock; the
/// rest are handled by the EvalPlanQual machinery.
pub type RowMarkType = c_int;
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
pub type LockClauseStrength = c_int;

/// `LockWaitPolicy` (`nodes/lockoptions.h`) — `NOWAIT`/`SKIP LOCKED` behavior.
pub type LockWaitPolicy = c_int;
/// `LockWaitBlock` — wait for the lock to become available (default).
pub const LockWaitBlock: LockWaitPolicy = 0;
/// `LockWaitSkip` — skip rows that can't be locked (`SKIP LOCKED`).
pub const LockWaitSkip: LockWaitPolicy = 1;
/// `LockWaitError` — raise an error if a row cannot be locked (`NOWAIT`).
pub const LockWaitError: LockWaitPolicy = 2;

/// `LockTupleMode` (`nodes/lockoptions.h`) — the tuple-lock strength passed to
/// `table_tuple_lock`.
pub type LockTupleMode = c_int;
/// `LockTupleKeyShare` — `SELECT FOR KEY SHARE`.
pub const LockTupleKeyShare: LockTupleMode = 0;
/// `LockTupleShare` — `SELECT FOR SHARE`.
pub const LockTupleShare: LockTupleMode = 1;
/// `LockTupleNoKeyExclusive` — `SELECT FOR NO KEY UPDATE`.
pub const LockTupleNoKeyExclusive: LockTupleMode = 2;
/// `LockTupleExclusive` — `SELECT FOR UPDATE`.
pub const LockTupleExclusive: LockTupleMode = 3;

/// `TM_Result` (`access/tableam.h`) — outcome of an update/delete/lock_tuple.
pub type TM_Result = c_int;
/// `TM_Ok` — the action succeeded.
pub const TM_Ok: TM_Result = 0;
/// `TM_Invisible` — the affected tuple wasn't visible to the relevant snapshot.
pub const TM_Invisible: TM_Result = 1;
/// `TM_SelfModified` — the tuple was already modified by the calling backend.
pub const TM_SelfModified: TM_Result = 2;
/// `TM_Updated` — the tuple was updated by another transaction.
pub const TM_Updated: TM_Result = 3;
/// `TM_Deleted` — the tuple was deleted by another transaction.
pub const TM_Deleted: TM_Result = 4;
/// `TM_BeingModified` — the tuple is currently being modified by another session.
pub const TM_BeingModified: TM_Result = 5;
/// `TM_WouldBlock` — the lock couldn't be acquired, action skipped.
pub const TM_WouldBlock: TM_Result = 6;

/// `TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS` (`access/tableam.h`).
pub const TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS: c_int = 1 << 0;
/// `TUPLE_LOCK_FLAG_FIND_LAST_VERSION` (`access/tableam.h`).
pub const TUPLE_LOCK_FLAG_FIND_LAST_VERSION: c_int = 1 << 1;

/// `TM_FailureData` (`access/tableam.h`) — details about a failed
/// update/delete/lock; the node reads `traversed` after `table_tuple_lock`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TM_FailureData {
    /// `ItemPointerData ctid` — TID of the conflicting tuple.
    pub ctid: ItemPointerData,
    /// `TransactionId xmax`.
    pub xmax: TransactionId,
    /// `CommandId cmax`.
    pub cmax: CommandId,
    /// `bool traversed` — true if `table_tuple_lock` followed an update chain.
    pub traversed: bool,
}

/// `RTEKind` (`nodes/parsenodes.h`) — the kind of a range-table entry.
pub type RTEKind = c_int;
/// `RTE_RELATION` — an ordinary relation reference.
pub const RTE_RELATION: RTEKind = 0;

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
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockRows {
    /// `Plan plan` — the common plan-node base.
    pub plan: Plan,
    /// `List *rowMarks` — a list of `PlanRowMark`s.
    pub rowMarks: *mut List,
    /// `int epqParam` — ID of Param for EvalPlanQual re-eval.
    pub epqParam: c_int,
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
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PlanRowMark {
    /// `NodeTag type`.
    pub type_: crate::NodeTag,
    /// `Index rti` — range table index of markable relation.
    pub rti: Index,
    /// `Index prti` — range table index of parent relation.
    pub prti: Index,
    /// `Index rowmarkId` — unique identifier for resjunk columns.
    pub rowmarkId: Index,
    /// `RowMarkType markType`.
    pub markType: RowMarkType,
    /// `int allMarkTypes` — OR of (1<<markType) for all children.
    pub allMarkTypes: c_int,
    /// `LockClauseStrength strength` — LockingClause's strength, or LCS_NONE.
    pub strength: LockClauseStrength,
    /// `LockWaitPolicy waitPolicy` — NOWAIT and SKIP LOCKED options.
    pub waitPolicy: LockWaitPolicy,
    /// `bool isParent` — true if this is a "dummy" parent entry.
    pub isParent: bool,
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
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExecRowMark {
    /// `Relation relation` — opened and suitably locked relation.
    pub relation: Relation,
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
    /// `void *ermExtra` — available for use by the relation source node.
    pub ermExtra: *mut c_void,
}

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
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExecAuxRowMarkData {
    /// `ExecRowMark *rowmark` — related entry in `es_rowmarks`.
    pub rowmark: *mut ExecRowMark,
    /// `AttrNumber ctidAttNo` — resno of ctid junk attribute, if any.
    pub ctidAttNo: AttrNumber,
    /// `AttrNumber toidAttNo` — resno of tableoid junk attribute, if any.
    pub toidAttNo: AttrNumber,
    /// `AttrNumber wholeAttNo` — resno of whole-row junk attribute, if any.
    pub wholeAttNo: AttrNumber,
}

/// `RangeTblEntry` (`nodes/parsenodes.h`) — prefix view modeled up through
/// `rtekind`.  PG 18 lays the node out as `NodeTag type; Alias *alias; Alias
/// *eref; RTEKind rtekind; ...` — `alias`/`eref` are deliberately placed first
/// "to make dump more legible".  The two `Alias *` fields MUST be present here
/// so `rtekind` lands at its real offset (24 on LP64); without them every
/// consumer that casts a live `RangeTblEntry*` to this view and reads `.rtekind`
/// (the optimizer's RTE-kind dispatch in initsplan/planmain/setrefs/allpaths/
/// createplan/ruleutils, etc.) reads the low half of the `alias` pointer
/// instead.  Consumers only ever pointer-cast and read `type_`/`rtekind`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RangeTblEntry {
    /// `NodeTag type`.
    pub type_: crate::NodeTag,
    /// `Alias *alias` — user-written alias clause, if any.
    pub alias: *mut core::ffi::c_void,
    /// `Alias *eref` — expanded reference names.
    pub eref: *mut core::ffi::c_void,
    /// `RTEKind rtekind` — relation/subquery/function/... kind.
    pub rtekind: RTEKind,
}

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
///
/// The leading [`PlanStateData`]'s first member is a `NodeTag`, so a
/// `*mut LockRowsStateData` is also a valid `Node *`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockRowsStateData {
    /// `PlanState ps` — the common plan-node base.
    pub ps: PlanStateData,
    /// `List *lr_arowMarks` — list of locking `ExecAuxRowMark`s.
    pub lr_arowMarks: *mut List,
    /// `EPQState lr_epqstate` — for evaluating EvalPlanQual rechecks.
    pub lr_epqstate: EPQState,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn tm_failuredata_layout() {
        // ItemPointerData (6 bytes, 2-aligned) then TransactionId(4)+CommandId(4)
        // are 4-aligned: ctid at 0, then 2 bytes pad to 8, xmax at 8, cmax at 12,
        // traversed at 16, struct rounds to 20.
        assert_eq!(offset_of!(TM_FailureData, ctid), 0);
        assert_eq!(offset_of!(TM_FailureData, xmax), 8);
        assert_eq!(offset_of!(TM_FailureData, cmax), 12);
        assert_eq!(offset_of!(TM_FailureData, traversed), 16);
        assert_eq!(align_of::<TM_FailureData>(), 4);
        assert_eq!(size_of::<TM_FailureData>(), 20);
    }

    #[test]
    fn lockrows_plan_layout() {
        // LockRows { Plan plan; List *rowMarks; int epqParam; }
        assert_eq!(offset_of!(LockRows, plan), 0);
        assert_eq!(offset_of!(LockRows, rowMarks), size_of::<Plan>());
        assert_eq!(
            offset_of!(LockRows, epqParam),
            size_of::<Plan>() + size_of::<*mut List>()
        );
    }

    #[test]
    fn planrowmark_layout() {
        // NodeTag(4) + 3 Index(4) + RowMarkType(4) + int(4) + 2 enum(4) then bool.
        assert_eq!(offset_of!(PlanRowMark, type_), 0);
        assert_eq!(offset_of!(PlanRowMark, rti), 4);
        assert_eq!(offset_of!(PlanRowMark, prti), 8);
        assert_eq!(offset_of!(PlanRowMark, rowmarkId), 12);
        assert_eq!(offset_of!(PlanRowMark, markType), 16);
        assert_eq!(offset_of!(PlanRowMark, allMarkTypes), 20);
        assert_eq!(offset_of!(PlanRowMark, strength), 24);
        assert_eq!(offset_of!(PlanRowMark, waitPolicy), 28);
        assert_eq!(offset_of!(PlanRowMark, isParent), 32);
    }

    #[test]
    fn execrowmark_layout() {
        // Relation(8) Oid(4) 3*Index(4)=12 RowMarkType(4) 2*enum(4)=8 bool(1)
        // then curCtid (ItemPointerData, 2-aligned) and ermExtra (8-aligned ptr).
        assert_eq!(offset_of!(ExecRowMark, relation), 0);
        assert_eq!(offset_of!(ExecRowMark, relid), 8);
        assert_eq!(offset_of!(ExecRowMark, rti), 12);
        assert_eq!(offset_of!(ExecRowMark, prti), 16);
        assert_eq!(offset_of!(ExecRowMark, rowmarkId), 20);
        assert_eq!(offset_of!(ExecRowMark, markType), 24);
        assert_eq!(offset_of!(ExecRowMark, strength), 28);
        assert_eq!(offset_of!(ExecRowMark, waitPolicy), 32);
        assert_eq!(offset_of!(ExecRowMark, ermActive), 36);
        // curCtid is ItemPointerData (6 bytes, 2-aligned) -> offset 38.
        assert_eq!(offset_of!(ExecRowMark, curCtid), 38);
        // ermExtra is an 8-aligned pointer: 38+6=44 rounds up to 48.
        assert_eq!(offset_of!(ExecRowMark, ermExtra), 48);
        assert_eq!(size_of::<ExecRowMark>(), 56);
    }

    #[test]
    fn execauxrowmark_layout() {
        assert_eq!(offset_of!(ExecAuxRowMarkData, rowmark), 0);
        assert_eq!(offset_of!(ExecAuxRowMarkData, ctidAttNo), 8);
        assert_eq!(offset_of!(ExecAuxRowMarkData, toidAttNo), 10);
        assert_eq!(offset_of!(ExecAuxRowMarkData, wholeAttNo), 12);
        assert_eq!(size_of::<ExecAuxRowMarkData>(), 16);
    }

    #[test]
    fn lockrowsstate_layout() {
        // LockRowsState { PlanState ps; List *lr_arowMarks; EPQState lr_epqstate; }
        assert_eq!(offset_of!(LockRowsStateData, ps), 0);
        assert_eq!(
            offset_of!(LockRowsStateData, lr_arowMarks),
            size_of::<PlanStateData>()
        );
        assert_eq!(
            offset_of!(LockRowsStateData, lr_epqstate),
            size_of::<PlanStateData>() + size_of::<*mut List>()
        );
        // a *mut LockRowsStateData is a valid Node* (NodeTag at offset 0)
        assert_eq!(offset_of!(PlanStateData, type_), 0);
    }

    #[test]
    fn rangetblentry_rtekind() {
        // PG 18 lays RangeTblEntry out as `NodeTag type; Alias *alias;
        // Alias *eref; RTEKind rtekind; ...` (alias/eref placed first to make
        // dumps legible).  On LP64 the leading NodeTag(4) pads to 8 for the two
        // 8-byte `Alias *` pointers (offsets 8 and 16), so `rtekind` lands at 24.
        assert_eq!(offset_of!(RangeTblEntry, type_), 0);
        assert_eq!(offset_of!(RangeTblEntry, alias), 8);
        assert_eq!(offset_of!(RangeTblEntry, eref), 16);
        assert_eq!(offset_of!(RangeTblEntry, rtekind), 24);
    }
}
