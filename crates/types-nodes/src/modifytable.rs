//! ModifyTable plan-node, plan-state, and per-statement context vocabulary
//! (nodes/plannodes.h `ModifyTable`, nodes/execnodes.h `ModifyTableState` /
//! `MergeActionState` / `OnConflictSetState`, plus the local
//! `ModifyTableContext` / `UpdateContext` helper structs from
//! `executor/nodeModifyTable.c`).
//!
//! Owned-tree conventions match `execnodes.rs`:
//! - `List *` → `Option<PgVec<'mcx, T>>` (`None` = the C `NIL`);
//! - a single owned pointee → `Option<PgBox<'mcx, T>>`;
//! - `TupleTableSlot *` → [`SlotId`] into the `EState`'s slot pool;
//! - `ResultRelInfo *` → [`RriId`] into the `EState`'s result-rel pool.
//!
//! `TransitionCaptureState` and `PartitionTupleRouting` are defined here
//! trimmed to the fields nodeModifyTable consumes; their logic is owned by
//! trigger.c and execPartition.c respectively and reached through those units'
//! seam crates. `OnConflictAction` (nodes/nodes.h) and the canonical owned
//! `EPQState` (execMain.c's EvalPlanQual machinery) are re-exported from their
//! canonical homes (`crate::nodes` / `crate::execnodes`) rather than redefined.

use mcx::{PgBox, PgVec};
use types_core::primitive::{Index, Oid};

use crate::bitmapset::Bitmapset;
use crate::execexpr::{ExprState, ProjectionInfo};
use crate::execnodes::{PlanStateData, RriId, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::{CmdType, Node, NodeTag};
use crate::primnodes::TargetEntry;

// `OnConflictAction` is canonically defined in `crate::nodes` (nodes/nodes.h);
// re-export it here so the modifytable port can reach it under the
// `types_nodes::modifytable::OnConflictAction` path it expects.
pub use crate::nodes::OnConflictAction;
pub use crate::nodes::OnConflictAction::{ONCONFLICT_NONE, ONCONFLICT_NOTHING, ONCONFLICT_UPDATE};

// The canonical owned `EPQState` lives in `crate::execnodes` and is held by
// `EStateData::es_epq_active` during a recheck. nodeModifyTable embeds one in
// `ModifyTableState::mt_epqstate` (faithful to the C `EPQState mt_epqstate`
// by-value member) and threads it through the execMain EvalPlanQual seams.
pub use crate::execnodes::EPQState;

/// `MergeMatchKind` (nodes/primnodes.h) — which class of MERGE action this is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum MergeMatchKind {
    MERGE_WHEN_MATCHED = 0,
    MERGE_WHEN_NOT_MATCHED_BY_SOURCE = 1,
    MERGE_WHEN_NOT_MATCHED_BY_TARGET = 2,
}
pub use MergeMatchKind::{
    MERGE_WHEN_MATCHED, MERGE_WHEN_NOT_MATCHED_BY_SOURCE, MERGE_WHEN_NOT_MATCHED_BY_TARGET,
};

/// Number of `MergeMatchKind` values, mirroring C's `NUM_MERGE_MATCH_KINDS`.
pub const NUM_MERGE_MATCH_KINDS: usize = 3;

/// `OverridingKind` (nodes/parsenodes.h) — OVERRIDING clause on an INSERT.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum OverridingKind {
    OVERRIDING_NOT_SET = 0,
    OVERRIDING_USER_VALUE = 1,
    OVERRIDING_SYSTEM_VALUE = 2,
}

/// `MergeAction` (nodes/primnodes.h) — a single WHEN clause of a MERGE.
#[derive(Debug)]
pub struct MergeAction<'mcx> {
    pub matchKind: MergeMatchKind,
    /// `CmdType commandType` — INSERT/UPDATE/DELETE/DO NOTHING.
    pub commandType: CmdType,
    /// `OverridingKind override` — OVERRIDING clause.
    pub overriding: OverridingKind,
    /// `Node *qual` — transformed WHEN conditions (`None` = `NULL`).
    pub qual: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `List *targetList` — the target list (of `TargetEntry`).
    pub targetList: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *updateColnos` — target attribute numbers for an UPDATE.
    pub updateColnos: Option<PgVec<'mcx, i32>>,
}

/// `ModifyTable` plan node (nodes/plannodes.h).
#[derive(Debug)]
pub struct ModifyTable<'mcx> {
    /// `Plan plan` — the abstract plan-node base.
    pub plan: Plan<'mcx>,
    /// `CmdType operation` — INSERT, UPDATE, DELETE, or MERGE.
    pub operation: CmdType,
    /// `bool canSetTag` — do we set the command tag / `es_processed`?
    pub canSetTag: bool,
    /// `Index nominalRelation` — parent RT index for EXPLAIN.
    pub nominalRelation: Index,
    /// `Index rootRelation` — root RT index, if partitioned/inherited.
    pub rootRelation: Index,
    /// `bool partColsUpdated` — some partition key in hierarchy updated?
    pub partColsUpdated: bool,
    /// `List *resultRelations` — integer list of RT indexes.
    pub resultRelations: Option<PgVec<'mcx, Index>>,
    /// `List *updateColnosLists` — per-target-table update_colnos lists.
    pub updateColnosLists: Option<PgVec<'mcx, PgVec<'mcx, i32>>>,
    /// `List *withCheckOptionLists` — per-target-table WCO lists.
    pub withCheckOptionLists: Option<PgVec<'mcx, PgVec<'mcx, Node<'mcx>>>>,
    /// `char *returningOldAlias` — alias for OLD in RETURNING lists.
    pub returningOldAlias: Option<PgBox<'mcx, [u8]>>,
    /// `char *returningNewAlias` — alias for NEW in RETURNING lists.
    pub returningNewAlias: Option<PgBox<'mcx, [u8]>>,
    /// `List *returningLists` — per-target-table RETURNING tlists.
    pub returningLists: Option<PgVec<'mcx, PgVec<'mcx, TargetEntry<'mcx>>>>,
    /// `List *fdwPrivLists` — per-target-table FDW private data lists.
    pub fdwPrivLists: Option<PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>>>,
    /// `Bitmapset *fdwDirectModifyPlans` — indices of FDW DM plans.
    pub fdwDirectModifyPlans: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `List *rowMarks` — PlanRowMarks (non-locking only).
    pub rowMarks: Option<PgVec<'mcx, PgBox<'mcx, Node<'mcx>>>>,
    /// `int epqParam` — ID of Param for EvalPlanQual re-eval.
    pub epqParam: i32,
    /// `OnConflictAction onConflictAction`.
    pub onConflictAction: OnConflictAction,
    /// `List *arbiterIndexes` — ON CONFLICT arbiter index OIDs.
    pub arbiterIndexes: Option<PgVec<'mcx, Oid>>,
    /// `List *onConflictSet` — INSERT ON CONFLICT DO UPDATE targetlist.
    pub onConflictSet: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *onConflictCols` — target column numbers for `onConflictSet`.
    pub onConflictCols: Option<PgVec<'mcx, i32>>,
    /// `Node *onConflictWhere` — WHERE for ON CONFLICT UPDATE.
    pub onConflictWhere: Option<PgBox<'mcx, Node<'mcx>>>,
    /// `Index exclRelRTI` — RTI of the EXCLUDED pseudo relation.
    pub exclRelRTI: Index,
    /// `List *exclRelTlist` — tlist of the EXCLUDED pseudo relation.
    pub exclRelTlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *mergeActionLists` — per-target-table MERGE actions.
    pub mergeActionLists: Option<PgVec<'mcx, PgVec<'mcx, MergeAction<'mcx>>>>,
    /// `List *mergeJoinConditions` — per-target-table MERGE join conditions.
    pub mergeJoinConditions: Option<PgVec<'mcx, Option<PgBox<'mcx, Node<'mcx>>>>>,
}

/// `MergeActionState` (nodes/execnodes.h) — exec state for one MERGE action.
#[derive(Debug)]
pub struct MergeActionState<'mcx> {
    pub type_: NodeTag,
    /// `MergeAction *mas_action` — associated MergeAction node.
    pub mas_action: Option<PgBox<'mcx, MergeAction<'mcx>>>,
    /// `ProjectionInfo *mas_proj` — projection of the action's targetlist.
    pub mas_proj: Option<PgBox<'mcx, ProjectionInfo<'mcx>>>,
    /// `ExprState *mas_whenqual` — WHEN [NOT] MATCHED AND conditions.
    pub mas_whenqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

/// `OnConflictSetState` (nodes/execnodes.h) — exec state for ON CONFLICT DO
/// UPDATE.
#[derive(Debug)]
pub struct OnConflictSetState<'mcx> {
    pub type_: NodeTag,
    /// `TupleTableSlot *oc_Existing` — slot for the existing target tuple.
    pub oc_Existing: Option<SlotId>,
    /// `TupleTableSlot *oc_ProjSlot` — SET projection target.
    pub oc_ProjSlot: Option<SlotId>,
    /// `ProjectionInfo *oc_ProjInfo` — for ON CONFLICT DO UPDATE SET.
    pub oc_ProjInfo: Option<ProjectionInfo<'mcx>>,
    /// `ExprState *oc_WhereClause` — state for the WHERE clause.
    pub oc_WhereClause: Option<ExprState<'mcx>>,
}

/// `TransitionCaptureState` (commands/trigger.h), trimmed to the fields
/// nodeModifyTable consumes. Owned by trigger.c; reached through the trigger
/// seam crate.
#[derive(Debug, Default)]
pub struct TransitionCaptureState {
    pub tcs_delete_old_table: bool,
    pub tcs_update_old_table: bool,
    pub tcs_update_new_table: bool,
    pub tcs_insert_new_table: bool,
    /// `TupleTableSlot *tcs_original_insert_tuple`.
    pub tcs_original_insert_tuple: Option<SlotId>,
}

/// `struct PartitionTupleRouting` (executor/execPartition.h). nodeModifyTable
/// holds only a forward-declared pointer and never reads its fields — the real
/// layout is owned by execPartition.c and reached through its seam crate. Kept
/// opaque here (a real owned struct with no consumed fields) per the
/// inherited-opacity rule.
#[derive(Debug, Default)]
pub struct PartitionTupleRouting {
    _private: (),
}

/// `ModifyTableState` (nodes/execnodes.h) — exec state for a ModifyTable node.
#[derive(Debug)]
pub struct ModifyTableState<'mcx> {
    /// `PlanState ps` — its first field is the NodeTag.
    pub ps: PlanStateData<'mcx>,
    /// `(ModifyTable *) ps.plan` — the typed alias of this node's `ModifyTable`
    /// plan node. The trimmed `PlanStateData.plan` (a `&Node`) cannot carry the
    /// `ModifyTable` variant (it is not in the `Node` enum), so the typed plan
    /// node is aliased here directly (inherited opacity: a real `&'mcx`
    /// reference into the shared, read-only plan tree). `None` only before init
    /// wires it. Consumers that the C reaches via `(ModifyTable *)
    /// mtstate->ps.plan` (the partition-routing init legs, ExecInsert's ON
    /// CONFLICT view) read it here.
    pub plan_node: Option<&'mcx ModifyTable<'mcx>>,
    /// `CmdType operation` — INSERT, UPDATE, DELETE, or MERGE.
    pub operation: CmdType,
    /// `((ModifyTable *) ps.plan)->onConflictAction` — cached at node init
    /// from the plan node, so the per-tuple paths need not re-downcast the
    /// plan tree.
    pub onConflictAction: OnConflictAction,
    /// `bool canSetTag`.
    pub canSetTag: bool,
    /// `bool mt_done`.
    pub mt_done: bool,
    /// `ResultRelInfo *resultRelInfo` (`mt_nrels` entries) — ids into the
    /// EState result-rel pool.
    pub resultRelInfo: PgVec<'mcx, RriId>,
    /// `ResultRelInfo *rootResultRelInfo`.
    pub rootResultRelInfo: Option<RriId>,
    /// `EPQState mt_epqstate`.
    pub mt_epqstate: EPQState<'mcx>,
    /// `bool fireBSTriggers`.
    pub fireBSTriggers: bool,
    /// `int mt_resultOidAttno` — resno of the "tableoid" junk attr.
    pub mt_resultOidAttno: i32,
    /// `Oid mt_lastResultOid` — last-seen value of tableoid.
    pub mt_lastResultOid: Oid,
    /// `int mt_lastResultIndex` — corresponding index in `resultRelInfo[]`.
    pub mt_lastResultIndex: i32,
    /// `HTAB *mt_resultOidHash` — optional OID→index lookup table.
    pub mt_resultOidHash: Option<PgBox<'mcx, ResultRelHash>>,
    /// `TupleTableSlot *mt_root_tuple_slot`.
    pub mt_root_tuple_slot: Option<SlotId>,
    /// `struct PartitionTupleRouting *mt_partition_tuple_routing`.
    pub mt_partition_tuple_routing: Option<PgBox<'mcx, PartitionTupleRouting>>,
    /// `struct TransitionCaptureState *mt_transition_capture`.
    pub mt_transition_capture: Option<PgBox<'mcx, TransitionCaptureState>>,
    /// `struct TransitionCaptureState *mt_oc_transition_capture`.
    pub mt_oc_transition_capture: Option<PgBox<'mcx, TransitionCaptureState>>,
    /// `int mt_merge_subcommands` — bitmask of present subcommands.
    pub mt_merge_subcommands: i32,
    /// `MergeActionState *mt_merge_action` — current MERGE action.
    pub mt_merge_action: Option<PgBox<'mcx, MergeActionState<'mcx>>>,
    /// `TupleTableSlot *mt_merge_pending_not_matched`.
    pub mt_merge_pending_not_matched: Option<SlotId>,
    /// `double mt_merge_inserted`.
    pub mt_merge_inserted: f64,
    /// `double mt_merge_updated`.
    pub mt_merge_updated: f64,
    /// `double mt_merge_deleted`.
    pub mt_merge_deleted: f64,
    /// `List *mt_updateColnosLists`.
    pub mt_updateColnosLists: Option<PgVec<'mcx, PgVec<'mcx, i32>>>,
    /// `List *mt_mergeActionLists` — the per-kept-target-table MERGE action
    /// lists. The C aliases the planner-owned `node->mergeActionLists` sublists
    /// (`lappend` of the shared list cells); the owned model stores `&'mcx`
    /// borrows of the plan node's per-target `MergeAction` lists (the plan tree
    /// outlives the state tree), subset to the unpruned result relations.
    pub mt_mergeActionLists: Option<PgVec<'mcx, &'mcx PgVec<'mcx, MergeAction<'mcx>>>>,
    /// `List *mt_mergeJoinConditions` — per-kept-target-table MERGE join
    /// conditions, borrowed from the plan node (`None` element = the C `NULL`).
    pub mt_mergeJoinConditions:
        Option<PgVec<'mcx, Option<&'mcx PgBox<'mcx, Node<'mcx>>>>>,
}

/// `HTAB *mt_resultOidHash` payload (the OID→resultRelInfo-index map for
/// inherited UPDATE/DELETE target resolution). The hashtab owner is dynahash;
/// modeled here as the consumed key/value mapping the modifytable port fills.
#[derive(Debug, Default)]
pub struct ResultRelHash {
    pub entries: alloc::collections::BTreeMap<Oid, i32>,
}

// `ModifyTableContext` and `UpdateContext` are file-local helper structs of
// executor/nodeModifyTable.c that reference tableam vocabulary
// (`TM_FailureData`, `TU_UpdateIndexes`, `LockTupleMode`) which lives in
// `types-tableam` (a crate that sits *above* `types-nodes`). They are defined
// in the consuming unit crate (`backend-executor-nodeModifyTable`) instead, to
// avoid a `types-nodes` → `types-tableam` cycle.
