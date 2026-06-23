//! `ModifyTableState`/`ResultRelInfo` executor-node ABI vocabulary, shared with
//! the `backend-executor-nodeModifyTable` crate.
//!
//! These are the `#[repr(C)]` node-state structs that `nodeModifyTable.c` owns
//! and navigates directly: the per-target `ResultRelInfo[]`, the
//! `ModifyTableState` node, the ON CONFLICT / MERGE action sub-states, and the
//! two file-local context structs (`ModifyTableContext`, `UpdateContext`) that
//! carry per-row state through the INSERT/UPDATE/DELETE/MERGE helpers. The
//! `Plan`/`EState`/`ExprContext`/`TupleTableSlot`/`EPQState` types are reused
//! from their existing modules so the whole codebase shares one layout.

use core::ffi::{c_double, c_int, c_void};

use crate::execnodes::PlanStateData;
use crate::nodeindexscan::EPQState;
use crate::nodemodifytable_abi::CmdType;
use crate::{
    AttrNumber, Bitmapset, CommandId, Index, ItemPointerData, List, Oid, Relation, TransactionId,
    TupleTableSlot,
};

/// `NUM_MERGE_MATCH_KINDS` (`nodes/primnodes.h`) — three buckets:
/// `MERGE_WHEN_MATCHED`, `MERGE_WHEN_NOT_MATCHED_BY_SOURCE`,
/// `MERGE_WHEN_NOT_MATCHED_BY_TARGET`.
pub const NUM_MERGE_MATCH_KINDS: usize = 3;

/// `MergeMatchKind` (`nodes/primnodes.h`).
pub const MERGE_WHEN_MATCHED: usize = 0;
pub const MERGE_WHEN_NOT_MATCHED_BY_SOURCE: usize = 1;
pub const MERGE_WHEN_NOT_MATCHED_BY_TARGET: usize = 2;

/// `TM_Result` (`access/tableam.h`) — outcome of a tuple_update/delete/lock.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TM_Result {
    TM_Ok = 0,
    TM_Invisible = 1,
    TM_SelfModified = 2,
    TM_Updated = 3,
    TM_Deleted = 4,
    TM_BeingModified = 5,
    TM_WouldBlock = 6,
}

/// `LockTupleMode` (`access/tableam.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockTupleMode {
    LockTupleKeyShare = 0,
    LockTupleShare = 1,
    LockTupleNoKeyExclusive = 2,
    LockTupleExclusive = 3,
}

/// `TU_UpdateIndexes` (`access/tableam.h`) — which index updates a tuple update
/// requires.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TU_UpdateIndexes {
    TU_None = 0,
    TU_All = 1,
    TU_Summarizing = 2,
}

/// `TM_FailureData` (`access/tableam.h`) — info about a concurrent
/// modification, populated by the table-AM on a non-`TM_Ok` result.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TM_FailureData {
    pub ctid: ItemPointerData,
    pub xmax: TransactionId,
    pub cmax: CommandId,
    pub traversed: bool,
}

impl Default for TM_FailureData {
    fn default() -> Self {
        // SAFETY: a `{0}` C initializer; all-zero is a valid bit pattern.
        unsafe { core::mem::zeroed() }
    }
}

/// `OnConflictSetState` (`nodes/execnodes.h`) — state for ON CONFLICT DO UPDATE.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct OnConflictSetState {
    pub type_: crate::NodeTag,
    pub oc_Existing: *mut TupleTableSlot,
    pub oc_ProjSlot: *mut TupleTableSlot,
    pub oc_ProjInfo: *mut c_void,
    pub oc_WhereClause: *mut c_void,
}

/// `MergeActionState` (`nodes/execnodes.h`) — runtime state for one MERGE WHEN
/// action.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MergeActionState {
    pub type_: crate::NodeTag,
    /// `MergeAction *mas_action`
    pub mas_action: *mut c_void,
    /// `ProjectionInfo *mas_proj`
    pub mas_proj: *mut c_void,
    /// `ExprState *mas_whenqual`
    pub mas_whenqual: *mut c_void,
}

/// `ResultRelInfo` (`nodes/execnodes.h`) — per-target-relation executor state.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ResultRelInfo {
    pub type_: crate::NodeTag,
    pub ri_RangeTableIndex: Index,
    pub ri_RelationDesc: Relation,
    pub ri_NumIndices: c_int,
    /// `RelationPtr ri_IndexRelationDescs`
    pub ri_IndexRelationDescs: *mut Relation,
    /// `IndexInfo **ri_IndexRelationInfo`
    pub ri_IndexRelationInfo: *mut *mut c_void,
    pub ri_RowIdAttNo: AttrNumber,
    pub ri_extraUpdatedCols: *mut Bitmapset,
    pub ri_extraUpdatedCols_valid: bool,
    /// `ProjectionInfo *ri_projectNew`
    pub ri_projectNew: *mut c_void,
    pub ri_newTupleSlot: *mut TupleTableSlot,
    pub ri_oldTupleSlot: *mut TupleTableSlot,
    pub ri_projectNewInfoValid: bool,
    pub ri_needLockTagTuple: bool,
    /// `TriggerDesc *ri_TrigDesc`
    pub ri_TrigDesc: *mut c_void,
    /// `FmgrInfo *ri_TrigFunctions`
    pub ri_TrigFunctions: *mut c_void,
    /// `ExprState **ri_TrigWhenExprs`
    pub ri_TrigWhenExprs: *mut *mut c_void,
    /// `Instrumentation *ri_TrigInstrument`
    pub ri_TrigInstrument: *mut c_void,
    pub ri_ReturningSlot: *mut TupleTableSlot,
    pub ri_TrigOldSlot: *mut TupleTableSlot,
    pub ri_TrigNewSlot: *mut TupleTableSlot,
    pub ri_AllNullSlot: *mut TupleTableSlot,
    /// `struct FdwRoutine *ri_FdwRoutine`
    pub ri_FdwRoutine: *mut c_void,
    pub ri_FdwState: *mut c_void,
    pub ri_usesFdwDirectModify: bool,
    pub ri_NumSlots: c_int,
    pub ri_NumSlotsInitialized: c_int,
    pub ri_BatchSize: c_int,
    pub ri_Slots: *mut *mut TupleTableSlot,
    pub ri_PlanSlots: *mut *mut TupleTableSlot,
    pub ri_WithCheckOptions: *mut List,
    pub ri_WithCheckOptionExprs: *mut List,
    /// `ExprState **ri_CheckConstraintExprs`
    pub ri_CheckConstraintExprs: *mut *mut c_void,
    /// `ExprState **ri_GenVirtualNotNullConstraintExprs`
    pub ri_GenVirtualNotNullConstraintExprs: *mut *mut c_void,
    /// `ExprState **ri_GeneratedExprsI`
    pub ri_GeneratedExprsI: *mut *mut c_void,
    /// `ExprState **ri_GeneratedExprsU`
    pub ri_GeneratedExprsU: *mut *mut c_void,
    pub ri_NumGeneratedNeededI: c_int,
    pub ri_NumGeneratedNeededU: c_int,
    pub ri_returningList: *mut List,
    /// `ProjectionInfo *ri_projectReturning`
    pub ri_projectReturning: *mut c_void,
    pub ri_onConflictArbiterIndexes: *mut List,
    pub ri_onConflict: *mut OnConflictSetState,
    /// `List *ri_MergeActions[NUM_MERGE_MATCH_KINDS]`
    pub ri_MergeActions: [*mut List; NUM_MERGE_MATCH_KINDS],
    /// `ExprState *ri_MergeJoinCondition`
    pub ri_MergeJoinCondition: *mut c_void,
    /// `ExprState *ri_PartitionCheckExpr`
    pub ri_PartitionCheckExpr: *mut c_void,
    /// `TupleConversionMap *ri_ChildToRootMap`
    pub ri_ChildToRootMap: *mut c_void,
    pub ri_ChildToRootMapValid: bool,
    /// `TupleConversionMap *ri_RootToChildMap`
    pub ri_RootToChildMap: *mut c_void,
    pub ri_RootToChildMapValid: bool,
    pub ri_RootResultRelInfo: *mut ResultRelInfo,
    pub ri_PartitionTupleSlot: *mut TupleTableSlot,
    /// `struct CopyMultiInsertBuffer *ri_CopyMultiInsertBuffer`
    pub ri_CopyMultiInsertBuffer: *mut c_void,
    pub ri_ancestorResultRels: *mut List,
}

/// `ModifyTableState` (`nodes/execnodes.h`) — the executor node state struct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyTableStateData {
    /// `PlanState ps` (its first field is the `NodeTag`)
    pub ps: PlanStateData,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub mt_done: bool,
    pub mt_nrels: c_int,
    pub resultRelInfo: *mut ResultRelInfo,
    pub rootResultRelInfo: *mut ResultRelInfo,
    pub mt_epqstate: EPQState,
    pub fireBSTriggers: bool,
    pub mt_resultOidAttno: c_int,
    pub mt_lastResultOid: Oid,
    pub mt_lastResultIndex: c_int,
    /// `HTAB *mt_resultOidHash`
    pub mt_resultOidHash: *mut c_void,
    pub mt_root_tuple_slot: *mut TupleTableSlot,
    /// `struct PartitionTupleRouting *mt_partition_tuple_routing`
    pub mt_partition_tuple_routing: *mut c_void,
    /// `struct TransitionCaptureState *mt_transition_capture`
    pub mt_transition_capture: *mut c_void,
    /// `struct TransitionCaptureState *mt_oc_transition_capture`
    pub mt_oc_transition_capture: *mut c_void,
    pub mt_merge_subcommands: c_int,
    pub mt_merge_action: *mut MergeActionState,
    pub mt_merge_pending_not_matched: *mut TupleTableSlot,
    pub mt_merge_inserted: c_double,
    pub mt_merge_updated: c_double,
    pub mt_merge_deleted: c_double,
    pub mt_updateColnosLists: *mut List,
    pub mt_mergeActionLists: *mut List,
    pub mt_mergeJoinConditions: *mut List,
}

/// `ModifyTableContext` (file-local to `nodeModifyTable.c`) — per-operation
/// execution state plus the output variables set by the *Act helpers.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ModifyTableContext {
    pub mtstate: *mut ModifyTableStateData,
    pub epqstate: *mut EPQState,
    pub estate: *mut crate::EState,
    pub planSlot: *mut TupleTableSlot,
    pub tmfd: TM_FailureData,
    pub cpDeletedSlot: *mut TupleTableSlot,
    pub cpUpdateReturningSlot: *mut TupleTableSlot,
}

/// `UpdateContext` (file-local to `nodeModifyTable.c`) — UPDATE-specific output
/// data.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct UpdateContext {
    pub crossPartUpdate: bool,
    pub updateIndexes: TU_UpdateIndexes,
    pub lockmode: LockTupleMode,
}

/// `MTTargetRelLookup` (file-local) — hash entry mapping a target rel's OID to
/// its index in `resultRelInfo[]`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MTTargetRelLookup {
    pub relationOid: Oid,
    pub relationIndex: c_int,
}

#[cfg(test)]
mod abi_tests {
    use super::*;
    use core::mem::offset_of;

    #[test]
    fn resultrelinfo_offsets() {
        assert_eq!(offset_of!(ResultRelInfo, type_), 0);
        assert_eq!(offset_of!(ResultRelInfo, ri_RangeTableIndex), 4);
        // ri_RelationDesc is pointer-aligned right after the 4-byte Index.
        assert_eq!(offset_of!(ResultRelInfo, ri_RelationDesc), 8);
    }

    #[test]
    fn modifytablestate_starts_with_planstate() {
        assert_eq!(offset_of!(ModifyTableStateData, ps), 0);
    }

    #[test]
    fn tm_result_discriminants() {
        assert_eq!(TM_Result::TM_Ok as i32, 0);
        assert_eq!(TM_Result::TM_Updated as i32, 3);
        assert_eq!(TM_Result::TM_Deleted as i32, 4);
    }

    #[test]
    fn lock_tuple_mode_discriminants() {
        assert_eq!(LockTupleMode::LockTupleExclusive as i32, 3);
    }
}
