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
//! `TransitionCaptureState` is defined here trimmed to the fields
//! nodeModifyTable consumes; its logic is owned by trigger.c and reached
//! through that unit's seam crate. `PartitionTupleRouting` (and its subsidiary
//! `PartitionDispatchData`) is the full, canonical carrier homed here so the
//! execPartition owner, its seam declarations, and the nodeModifyTable
//! consumers all share one type; the routing logic itself is owned by
//! execPartition.c and reached through its seam crate. `OnConflictAction` (nodes/nodes.h) and the canonical owned
//! `EPQState` (execMain.c's EvalPlanQual machinery) are re-exported from their
//! canonical homes (`crate::nodes` / `crate::execnodes`) rather than redefined.

use mcx::{alloc_in, slice_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{Index, Oid};
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::execexpr::{ExprState, ProjectionInfo};
use crate::execnodes::{Opaque, PlanStateData, RriId, SlotId};
use crate::nodeindexscan::Plan;
use crate::nodes::{CmdType, Node, NodeTag};
use crate::partition::{PartitionDescData, PartitionKeyData};
use crate::primnodes::{Expr, TargetEntry};
use rel::Relation;
use types_slot::TupleTableSlot;
use types_tuple::attmap::AttrMap;

// `OnConflictAction` is canonically defined in `crate::nodes` (nodes/nodes.h);
// re-export it here so the modifytable port can reach it under the
// `nodes::modifytable::OnConflictAction` path it expects.
pub use crate::nodes::OnConflictAction;
pub use crate::nodes::OnConflictAction::{ONCONFLICT_NONE, ONCONFLICT_NOTHING, ONCONFLICT_UPDATE};

// `T_ModifyTable` / `T_MergeAction` (nodes/nodetags.h) — the node tags of the
// two copyObject targets this module owns. `T_MergeAction` is defined
// canonically in `crate::nodes`; re-export both here where the `Node` arm and
// `nodeTag` reader consult them.
pub use crate::nodes::T_MergeAction;
/// `T_ModifyTable = 333` (nodes/nodetags.h, PostgreSQL 18.3 generated order).
pub const T_ModifyTable: NodeTag = NodeTag(333);

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

impl Default for MergeMatchKind {
    /// A zero-initialized `makeNode(MergeAction)` leaves `matchKind == 0`
    /// (`MERGE_WHEN_MATCHED`).
    fn default() -> Self {
        MergeMatchKind::MERGE_WHEN_MATCHED
    }
}

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

impl Default for OverridingKind {
    /// A zero-initialized `makeNode` leaves `override == 0`
    /// (`OVERRIDING_NOT_SET`).
    fn default() -> Self {
        OverridingKind::OVERRIDING_NOT_SET
    }
}

/// `MergeAction` (nodes/primnodes.h) — a single WHEN clause of a MERGE.
#[derive(Debug)]
pub struct MergeAction<'mcx> {
    pub matchKind: MergeMatchKind,
    /// `CmdType commandType` — INSERT/UPDATE/DELETE/DO NOTHING.
    pub commandType: CmdType,
    /// `OverridingKind override` — OVERRIDING clause.
    pub overriding: OverridingKind,
    /// `Node *qual` — transformed WHEN conditions (`None` = `NULL`). C stores a
    /// `Node *`, but the executor always casts it `(List *) action->qual` and
    /// feeds it to `ExecInitQual`, i.e. it is an implicit-AND `List` of `Expr`.
    /// Modeled as that Expr-list so the modify-qual builders can consume it.
    pub qual: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `List *targetList` — the target list (of `TargetEntry`).
    pub targetList: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *updateColnos` — target attribute numbers for an UPDATE.
    pub updateColnos: Option<PgVec<'mcx, i32>>,
}

impl MergeAction<'_> {
    /// `_copyMergeAction` (copyfuncs.funcs.c) — deep copy of the MERGE WHEN
    /// clause into `mcx` (C: `copyObject` shape). Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<MergeAction<'b>> {
        // `COPY_NODE_FIELD(qual)` — the implicit-AND `Expr` list (lifetime-free).
        let qual = match &self.qual {
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
        // `COPY_NODE_FIELD(targetList)`.
        let targetList = match &self.targetList {
            Some(tlist) => {
                let mut out = vec_with_capacity_in(mcx, tlist.len())?;
                for tle in tlist.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(updateColnos)` — an integer `List`.
        let updateColnos = match &self.updateColnos {
            Some(cols) => Some(slice_in(mcx, cols)?),
            None => None,
        };
        Ok(MergeAction {
            matchKind: self.matchKind,
            commandType: self.commandType,
            overriding: self.overriding,
            qual,
            targetList,
            updateColnos,
        })
    }
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
    /// `List *withCheckOptionLists` — per-target-table WCO lists. Each entry is
    /// a `List` of `WithCheckOption` nodes (one per RLS/CHECK constraint), each
    /// carrying its own `qual` (cast `(List *) wco->qual` and fed to
    /// `ExecInitQual`). The `WithCheckOption` node is not yet modeled in the
    /// trimmed `Node` enum, so this stays a plan-`Node` list.
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
    /// `Node *onConflictWhere` — WHERE for ON CONFLICT UPDATE. Cast
    /// `(List *) node->onConflictWhere` and fed to `ExecInitQual`, so modeled
    /// as the implicit-AND `List` of `Expr`.
    pub onConflictWhere: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `Index exclRelRTI` — RTI of the EXCLUDED pseudo relation.
    pub exclRelRTI: Index,
    /// `List *exclRelTlist` — tlist of the EXCLUDED pseudo relation.
    pub exclRelTlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `List *mergeActionLists` — per-target-table MERGE actions.
    pub mergeActionLists: Option<PgVec<'mcx, PgVec<'mcx, MergeAction<'mcx>>>>,
    /// `List *mergeJoinConditions` — per-target-table MERGE join conditions.
    /// Each entry's `joinCondition` is cast `(List *) joinCondition` and fed to
    /// `ExecInitQual`, so modeled as the implicit-AND `List` of `Expr`.
    pub mergeJoinConditions: Option<PgVec<'mcx, Option<PgVec<'mcx, Expr<'mcx>>>>>,
}

impl ModifyTable<'_> {
    /// `_copyModifyTable` (copyfuncs.funcs.c) — deep copy of the ModifyTable
    /// plan node (and its subtrees) into `mcx` (C: `copyObject` shape).
    /// Fallible: copying allocates.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ModifyTable<'b>> {
        // `COPY_NODE_FIELD(resultRelations)` — integer `List` of RT indexes.
        let resultRelations = match &self.resultRelations {
            Some(v) => Some(slice_in(mcx, v)?),
            None => None,
        };
        // `COPY_NODE_FIELD(updateColnosLists)` — a `List` of integer `List`s.
        let updateColnosLists = match &self.updateColnosLists {
            Some(lists) => {
                let mut out = vec_with_capacity_in(mcx, lists.len())?;
                for sub in lists.iter() {
                    out.push(slice_in(mcx, sub)?);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(withCheckOptionLists)` — a `List` of `Node` `List`s.
        let withCheckOptionLists = match &self.withCheckOptionLists {
            Some(lists) => {
                let mut out = vec_with_capacity_in(mcx, lists.len())?;
                for sub in lists.iter() {
                    let mut inner = vec_with_capacity_in(mcx, sub.len())?;
                    for n in sub.iter() {
                        inner.push(n.clone_in(mcx)?);
                    }
                    out.push(inner);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_STRING_FIELD(returningOldAlias)` / `returningNewAlias`.
        let returningOldAlias = match &self.returningOldAlias {
            Some(s) => Some(slice_in(mcx, s)?.into_boxed_slice()),
            None => None,
        };
        let returningNewAlias = match &self.returningNewAlias {
            Some(s) => Some(slice_in(mcx, s)?.into_boxed_slice()),
            None => None,
        };
        // `COPY_NODE_FIELD(returningLists)` — a `List` of `TargetEntry` `List`s.
        let returningLists = match &self.returningLists {
            Some(lists) => {
                let mut out = vec_with_capacity_in(mcx, lists.len())?;
                for sub in lists.iter() {
                    let mut inner = vec_with_capacity_in(mcx, sub.len())?;
                    for tle in sub.iter() {
                        inner.push(tle.clone_in(mcx)?);
                    }
                    out.push(inner);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(fdwPrivLists)` — a `List` of per-target FDW private
        // `Node` lists; an element is `None` for a target with no FDW data.
        let fdwPrivLists = match &self.fdwPrivLists {
            Some(lists) => {
                let mut out = vec_with_capacity_in(mcx, lists.len())?;
                for entry in lists.iter() {
                    out.push(match entry {
                        Some(n) => Some(alloc_in(mcx, n.clone_in(mcx)?)?),
                        None => None,
                    });
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_BITMAPSET_FIELD(fdwDirectModifyPlans)`.
        let fdwDirectModifyPlans = match &self.fdwDirectModifyPlans {
            Some(b) => Some(alloc_in(mcx, b.clone_in(mcx)?)?),
            None => None,
        };
        // `COPY_NODE_FIELD(rowMarks)` — a `List` of `PlanRowMark` nodes.
        let rowMarks = match &self.rowMarks {
            Some(marks) => {
                let mut out = vec_with_capacity_in(mcx, marks.len())?;
                for m in marks.iter() {
                    out.push(alloc_in(mcx, m.clone_in(mcx)?)?);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(arbiterIndexes)` — an OID `List`.
        let arbiterIndexes = match &self.arbiterIndexes {
            Some(v) => Some(slice_in(mcx, v)?),
            None => None,
        };
        // `COPY_NODE_FIELD(onConflictSet)` — a `TargetEntry` `List`.
        let onConflictSet = match &self.onConflictSet {
            Some(tlist) => {
                let mut out = vec_with_capacity_in(mcx, tlist.len())?;
                for tle in tlist.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(onConflictCols)` — an integer `List`.
        let onConflictCols = match &self.onConflictCols {
            Some(v) => Some(slice_in(mcx, v)?),
            None => None,
        };
        // `COPY_NODE_FIELD(onConflictWhere)` — the implicit-AND `Expr` list.
        let onConflictWhere = match &self.onConflictWhere {
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
        // `COPY_NODE_FIELD(exclRelTlist)` — a `TargetEntry` `List`.
        let exclRelTlist = match &self.exclRelTlist {
            Some(tlist) => {
                let mut out = vec_with_capacity_in(mcx, tlist.len())?;
                for tle in tlist.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(mergeActionLists)` — a `List` of `MergeAction` lists.
        let mergeActionLists = match &self.mergeActionLists {
            Some(lists) => {
                let mut out = vec_with_capacity_in(mcx, lists.len())?;
                for sub in lists.iter() {
                    let mut inner = vec_with_capacity_in(mcx, sub.len())?;
                    for action in sub.iter() {
                        inner.push(action.clone_in(mcx)?);
                    }
                    out.push(inner);
                }
                Some(out)
            }
            None => None,
        };
        // `COPY_NODE_FIELD(mergeJoinConditions)` — a `List` of per-target join
        // condition `Expr` lists; an element is `None` for the C `NULL`.
        let mergeJoinConditions = match &self.mergeJoinConditions {
            Some(lists) => {
                let mut out = vec_with_capacity_in(mcx, lists.len())?;
                for entry in lists.iter() {
                    out.push(match entry {
                        Some(cond) => {
                            let mut inner = vec_with_capacity_in(mcx, cond.len())?;
                            for e in cond.iter() {
                                // Deep-copy via `clone_in`, not the derived
                                // `Expr::clone` (panics on a `SubPlan` arm).
                                inner.push(e.clone_in(mcx)?);
                            }
                            Some(inner)
                        }
                        None => None,
                    });
                }
                Some(out)
            }
            None => None,
        };
        Ok(ModifyTable {
            // `COPY_SCALAR_FIELD(plan.*)` + `COPY_NODE_FIELD(plan.*)` — the
            // embedded `Plan` base is deep-copied by `Plan::clone_in`.
            plan: self.plan.clone_in(mcx)?,
            operation: self.operation,
            canSetTag: self.canSetTag,
            nominalRelation: self.nominalRelation,
            rootRelation: self.rootRelation,
            partColsUpdated: self.partColsUpdated,
            resultRelations,
            updateColnosLists,
            withCheckOptionLists,
            returningOldAlias,
            returningNewAlias,
            returningLists,
            fdwPrivLists,
            fdwDirectModifyPlans,
            rowMarks,
            epqParam: self.epqParam,
            onConflictAction: self.onConflictAction,
            arbiterIndexes,
            onConflictSet,
            onConflictCols,
            onConflictWhere,
            exclRelRTI: self.exclRelRTI,
            exclRelTlist,
            mergeActionLists,
            mergeJoinConditions,
        })
    }
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
    /// `struct AfterTriggersTableData *tcs_insert_private` — in the owned model,
    /// the index of the per-(relation, INSERT) `AfterTriggersTableData` within
    /// the current after-trigger query level's `tables` list (owned by trigger.c
    /// via the `afterTriggers` thread-local); `None` if no INSERT table is needed.
    /// Transition tables are non-deferrable and fire within the same query level,
    /// so this index stays valid until `AfterTriggerEndQuery`.
    pub tcs_insert_private: Option<usize>,
    /// `struct AfterTriggersTableData *tcs_update_private` — index of the
    /// per-(relation, UPDATE) table-data, or `None`.
    pub tcs_update_private: Option<usize>,
    /// `struct AfterTriggersTableData *tcs_delete_private` — index of the
    /// per-(relation, DELETE) table-data, or `None`.
    pub tcs_delete_private: Option<usize>,
}

/// `PartitionDispatchData` (executor/execPartition.c, private): per-partitioned-
/// table info needed to route a tuple to any of its partitions. Always
/// encapsulated in a [`PartitionTupleRouting`].
///
/// This struct's logic is owned by execPartition.c and reached through that
/// unit's seam crate; the layout is homed here (not in the owner crate) so the
/// owner, the seam declarations, and the nodeModifyTable consumers all share one
/// canonical carrier type (see [`PartitionTupleRouting`]).
///
/// The C struct ends with a `int indexes[FLEXIBLE_ARRAY_MEMBER]` tail
/// (`partdesc->nparts` entries); here that is the owned `indexes` `PgVec`.
#[derive(Debug)]
pub struct PartitionDispatchData<'mcx> {
    /// `Relation reldesc` — relation descriptor of the table.
    pub reldesc: Option<Relation<'mcx>>,
    /// `PartitionKey key` — partition key information of the table.
    pub key: Option<PgBox<'mcx, PartitionKeyData<'mcx>>>,
    /// `List *keystate` — `ExprState`s for the partition-key expressions
    /// (`NIL` until first `FormPartitionKeyDatum`).
    pub keystate: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    /// `PartitionDesc partdesc` — partition descriptor of the table.
    pub partdesc: Option<PgBox<'mcx, PartitionDescData<'mcx>>>,
    /// `TupleTableSlot *tupslot` — standalone slot for this table's tupdesc, or
    /// `None` if no tuple conversion from the parent is required.
    pub tupslot: Option<crate::tuptable::SlotData<'mcx>>,
    /// `AttrMap *tupmap` — parent→this-table rowtype map, or `None` if no
    /// conversion is required.
    pub tupmap: Option<PgBox<'mcx, AttrMap<'mcx>>>,
    /// `int indexes[FLEXIBLE_ARRAY_MEMBER]` — per-partition index into the
    /// `PartitionTupleRouting` `partitions` (leaf) or `partition_dispatch_info`
    /// (sub-partitioned) array; -1 if nothing allocated yet.
    pub indexes: PgVec<'mcx, i32>,
}

/// `PartitionDispatch` — owned alias (the C `PartitionDispatchData *`); in the
/// owned model a dispatch is addressed by its index into the routing's
/// `partition_dispatch_info` pool.
pub type PartitionDispatchId = usize;

/// `struct PartitionTupleRouting` (executor/execPartition.c, opaque in
/// execPartition.h): everything required to route a tuple inserted into a
/// partitioned table to one of its leaf partitions. Allocated in the per-query
/// context (`memcxt`).
///
/// The routing logic is owned by execPartition.c and reached through that
/// unit's seam crate; the layout is homed here (not in the owner crate) so the
/// owner's routing fns, the seam declarations, and the nodeModifyTable
/// consumers (`ModifyTableState.mt_partition_tuple_routing`) all share one
/// canonical carrier type. Mirrors the C struct field-for-field.
#[derive(Debug)]
pub struct PartitionTupleRouting<'mcx> {
    /// `Relation partition_root` — the partitioned table targeted by the
    /// command.
    pub partition_root: Option<Relation<'mcx>>,
    /// `PartitionDispatch *partition_dispatch_info` — one per partitioned table
    /// touched by routing; element 0 is always the target table.
    pub partition_dispatch_info: PgVec<'mcx, PgBox<'mcx, PartitionDispatchData<'mcx>>>,
    /// `ResultRelInfo **nonleaf_partitions` — fake `ResultRelInfo`s (ids into
    /// the EState pool) for nonleaf partitions, used to check the partition
    /// constraint; a `None` element is the C `NULL` (root level).
    pub nonleaf_partitions: PgVec<'mcx, Option<RriId>>,
    /// `int num_dispatch` — items stored in `partition_dispatch_info`.
    pub num_dispatch: i32,
    /// `int max_dispatch` — allocated size of `partition_dispatch_info`
    /// (tracked for 1:1 mirror of the C grow logic).
    pub max_dispatch: i32,
    /// `ResultRelInfo **partitions` — one per leaf partition touched by
    /// routing (ids into the EState pool); some borrowed from the owning
    /// `ModifyTableState`, the rest built here.
    pub partitions: PgVec<'mcx, RriId>,
    /// `bool *is_borrowed_rel` — parallel to `partitions`: whether the entry is
    /// borrowed from the owning `ModifyTableState` (do not close on cleanup).
    pub is_borrowed_rel: PgVec<'mcx, bool>,
    /// `int num_partitions` — items stored in `partitions`.
    pub num_partitions: i32,
    /// `int max_partitions` — allocated size of `partitions`.
    pub max_partitions: i32,
    /// `MemoryContext memcxt` — context used to allocate subsidiary structs.
    pub memcxt: Opaque,
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
    pub mt_partition_tuple_routing: Option<PgBox<'mcx, PartitionTupleRouting<'mcx>>>,
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
        Option<PgVec<'mcx, Option<&'mcx PgVec<'mcx, Expr<'mcx>>>>>,
    /// Back-link to the enclosing `PlanStateNode::ModifyTable` enum, stamped by
    /// `stamp_modifytable_expr_parents` once the node is address-stable. In C
    /// every `ExecBuildProjectionInfo`/`ExecInitQual` in `ExecInitModifyTable`/
    /// `ExecInitMerge`/`ExecInitPartitionInfo` passes `&mtstate->ps` as the
    /// expression `parent`. The result-relation `ExprState`s built up-front are
    /// stamped by `stamp_modifytable_expr_parents`, but the per-leaf-partition
    /// projections/quals built lazily by `ExecInitPartitionInfo` (RETURNING
    /// holding `merge_action()`, `mas_proj`/`mas_whenqual`,
    /// `ri_MergeJoinCondition`) are created *after* that stamp pass; this link
    /// lets the lazy partition init stamp them with the same `ModifyTableState`
    /// identity. `None` until stamped.
    pub mt_self_link: Option<crate::planstate::PlanStateLink>,
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
