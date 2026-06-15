//! Executor node vocabulary (executor/execnodes.h plus the `sdir.h` scan
//! direction), trimmed.
//!
//! In the owned-tree model each `<Node>StateData` layout carries its fields as
//! owned children (`Option<PgBox<'mcx, T>>` for a single nullable pointee,
//! `PgVec<'mcx, T>` for a counted array), allocated in the per-query memory
//! context whose `'mcx` the tree carries. C pointers that alias objects owned
//! by the `EState` become `Copy` ids into EState-owned pools:
//!
//! - `TupleTableSlot *` is a [`SlotId`] into [`EStateData::es_tupleTable`]
//!   (C's slot pointers point into the `es_tupleTable`-owned objects);
//! - `ExprContext *` is an [`EcxtId`] into [`EStateData::es_exprcontexts`]
//!   (C aliases one context from both the node's `ps_ExprContext` and the
//!   `es_exprcontexts` shutdown list — the pool keeps the EState able to shut
//!   every context down at `FreeExecutorState` while nodes hold the id);
//! - `ResultRelInfo *` is an [`RriId`] into
//!   [`EStateData::es_result_rel_pool`] (C aliases caller-owned nodes from
//!   `es_result_relations`, `es_opened_result_relations`, and
//!   `ri_RootResultRelInfo` back-links at once).
//!
//! The C `PlanState.state` back-pointer to the `EState` is not carried: the
//! owned model threads `&mut EStateData` explicitly through the executor entry
//! points instead.

use mcx::{Mcx, MemoryContext, PgBox, PgString, PgVec};
use types_core::primitive::{AttrNumber, Index, Oid};
use types_core::fmgr::INDEX_MAX_KEYS;
use types_core::xact::CommandId;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{TupleDesc, TupleDescData};
use types_tuple::tupconvert::TupleConversionMap;

use crate::bitmapset::Bitmapset;
use crate::execexpr::{ExprState, ProjectionInfo, SubPlanState};
use types_slot::{TupleSlotKind, TupleTableSlot};
use crate::tuptable::{SlotData, VirtualTupleTableSlot};
use crate::instrument::Instrumentation;
use crate::nodeindexscan::PlannedStmt;
use crate::parsenodes::{RTEPermissionInfo, RangeTblEntry};
use crate::planstate::PlanStateNode;
use crate::nodes::NodeTag;

/// `T_MaterialState` (nodes/nodetags.h) — the executor-state node tag for a
/// Material node.
pub const T_MaterialState: NodeTag = NodeTag(424);

pub use types_scan::sdir::{
    BackwardScanDirection, ForwardScanDirection, NoMovementScanDirection, ScanDirection,
    ScanDirectionIsBackward, ScanDirectionIsForward, ScanDirectionIsNoMovement,
};

/// `TupleTableSlot *` in the owned model: a `Copy` index into the owning
/// [`EStateData::es_tupleTable`] slot pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SlotId(pub u32);

/// `ExprContext *` in the owned model: a `Copy` index into the owning
/// [`EStateData::es_exprcontexts`] pool. Ids are stable for the EState's
/// lifetime (freeing a context tombstones its pool entry; entries are never
/// shifted).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EcxtId(pub u32);

/// `ResultRelInfo *` in the owned model: a `Copy` index into the owning
/// [`EStateData::es_result_rel_pool`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RriId(pub u32);

/// `JunkFilter` (nodes/execnodes.h) — junk-attribute filter state built by
/// `ExecInitJunkFilter`/`ExecInitJunkFilterConversion` (execJunk.c).
///
/// `jf_resultSlot` (C: `TupleTableSlot *`) is an id into the owning EState slot
/// pool ([`EStateData::es_tupleTable`]); the rest mirror the C struct
/// field-for-field. `jf_cleanMap` (C: `AttrNumber *`) is the per-clean-attribute
/// map onto the source tuple's 1-based attribute numbers (0 = NULL output).
#[derive(Debug)]
pub struct JunkFilter<'mcx> {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `List *jf_targetList` — the original target list (including junk
    /// attributes).
    pub jf_targetList: PgVec<'mcx, crate::primnodes::TargetEntry<'mcx>>,
    /// `TupleDesc jf_cleanTupType` — the "clean" tuple's descriptor.
    pub jf_cleanTupType: TupleDesc<'mcx>,
    /// `AttrNumber *jf_cleanMap` — clean->original attribute-number map.
    pub jf_cleanMap: PgVec<'mcx, AttrNumber>,
    /// `TupleTableSlot *jf_resultSlot` — the slot holding the cleaned tuple
    /// (id into [`EStateData::es_tupleTable`]).
    pub jf_resultSlot: SlotId,
}

/// `EPQState` (`nodes/execnodes.h`) — state for executing an EvalPlanQual
/// recheck on a candidate tuple, owned by the EvalPlanQual machinery
/// (execMain.c) and held by [`EStateData::es_epq_active`] for the duration of
/// a recheck. Scan nodes read its per-relation substitute arrays (indexed by
/// `scanrelid - 1`) directly off the owned struct.
///
/// Mirrors the C struct field-by-field. The fields the EvalPlanQual port
/// (execMain.c) will need but no current consumer reads — `parentestate`,
/// `tuple_table`, `plan`, `arowMarks`, `origslot`, `recheckestate`,
/// `recheckplanstate` — are trimmed here and land with that port (docs/types.md
/// rule 3); they are documented inline below so the layout stays a faithful
/// mirror.
///
/// `relsubs_slot[i]` is `Some(slot_id)` when the caller has provided an EPQ
/// test slot for that target relation (C: a non-NULL `relsubs_slot[i]` entry);
/// the array itself is `None` for the C `NULL` (no EPQ in progress / not yet
/// built). `relsubs_rowmark[i]` is `true` when a non-locking rowmark can fetch
/// a replacement tuple on demand. `relsubs_done[i]` records that the EPQ tuple
/// for that relation has already been returned (or that there is none);
/// `relsubs_blocked[i]` records that there is no EPQ tuple this test.
#[derive(Debug, Default)]
pub struct EPQState<'mcx> {
    // C: `EState *parentestate` — main query's EState. Trimmed (rule 3).
    /// `int epqParam` — ID of the Param that forces a scan node to re-eval.
    pub epqParam: i32,
    /// `List *resultRelations` — integer list of RT indexes, or NIL (`None`).
    pub resultRelations: Option<PgVec<'mcx, i32>>,
    // C: `List *tuple_table` — tuple table for `relsubs_slot`. Trimmed (rule 3).
    /// `TupleTableSlot **relsubs_slot` — per-relation EPQ test slots
    /// (`Some(slot_id)` = a non-NULL C entry). `None` = the C `NULL` array.
    pub relsubs_slot: Option<PgVec<'mcx, Option<SlotId>>>,
    // C: `Plan *plan` — plan tree to be executed. Trimmed (rule 3).
    // C: `List *arowMarks` — ExecAuxRowMarks (non-locking only). Trimmed.
    // C: `TupleTableSlot *origslot` — original output tuple. Trimmed (rule 3).
    // C: `EState *recheckestate` — EState for EPQ execution. Trimmed (rule 3).
    /// `ExecAuxRowMark **relsubs_rowmark` — per-relation non-locking rowmarks
    /// (`true` = a non-NULL C entry the EPQ machinery can fetch through).
    /// `None` = the C `NULL` array.
    pub relsubs_rowmark: Option<PgVec<'mcx, bool>>,
    /// `bool *relsubs_done` — per-relation "EPQ tuple already returned / none".
    /// `None` = the C `NULL` array.
    pub relsubs_done: Option<PgVec<'mcx, bool>>,
    /// `bool *relsubs_blocked` — per-relation "no EPQ tuple this test".
    /// `None` = the C `NULL` array.
    pub relsubs_blocked: Option<PgVec<'mcx, bool>>,
    // C: `PlanState *recheckplanstate` — EPQ-specific exec nodes. Trimmed.
}

/// An opaque handle to a genuinely AM/extension-opaque object the executor
/// only stores and hands back (`JitContext`, `PartitionDirectory` — types C
/// itself leaves opaque). The owning unit downcasts with a loud panic on
/// mismatch; the executor never inspects the payload. `None` is the C `NULL`.
#[derive(Default)]
pub struct Opaque(pub Option<alloc::boxed::Box<dyn core::any::Any>>);

impl core::fmt::Debug for Opaque {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some(_) => f.write_str("Opaque(<set>)"),
            None => f.write_str("Opaque(<null>)"),
        }
    }
}

/// `ExprContextCallbackFunction` (execnodes.h): `void (*)(Datum arg)`.
///
/// In C the callbacks run in ereport-capable code, inside the ExprContext's
/// `ecxt_per_tuple_memory`; the Rust shape carries both halves of that
/// surface — the per-tuple context handle is passed in, and failure is
/// `Err(PgError)`.
pub type ExprContextCallbackFunction = for<'mcx> fn(Mcx<'mcx>, Datum<'mcx>) -> PgResult<()>;

/// `ExprContext_CB` (execnodes.h) — one registered shutdown callback. The
/// chain nodes are allocated in the context's per-query memory
/// (`RegisterExprContextCallback`'s `MemoryContextAlloc`), so they carry the
/// allocator lifetime.
#[derive(Debug)]
pub struct ExprContext_CB<'mcx> {
    pub next: Option<PgBox<'mcx, ExprContext_CB<'mcx>>>,
    pub function: ExprContextCallbackFunction,
    pub arg: Datum<'mcx>,
}

/// `ExprContext` (execnodes.h) — per-node expression-evaluation context,
/// trimmed:
///
/// - `ecxt_per_tuple_memory` is a real owned child context of the per-query
///   context (`MemoryContextReset` is [`MemoryContext::reset`]);
/// - the `ecxt_param_exec_vals` / `ecxt_param_list_info` aliases of the
///   EState's params and the `ecxt_estate` back-pointer are not carried: the
///   owned model threads the `EState` explicitly, so readers take the params
///   from [`EStateData::es_param_exec_vals`] / `es_param_list_info` directly;
/// - the `NodeTag type` field is not carried (the owned struct is its tag).
#[derive(Debug)]
pub struct ExprContext<'mcx> {
    /// `TupleTableSlot *ecxt_scantuple` — current input tuple (slot id).
    pub ecxt_scantuple: Option<SlotId>,
    /// `TupleTableSlot *ecxt_innertuple` — inner tuple of current join.
    pub ecxt_innertuple: Option<SlotId>,
    /// `TupleTableSlot *ecxt_outertuple` — outer tuple of current join.
    pub ecxt_outertuple: Option<SlotId>,
    /// `TupleTableSlot *ecxt_oldtuple` — the OLD row for RETURNING (and the
    /// MERGE/ON CONFLICT old-tuple slot). `None` is the C `NULL`.
    pub ecxt_oldtuple: Option<SlotId>,
    /// `TupleTableSlot *ecxt_newtuple` — the NEW row for RETURNING. `None` is
    /// the C `NULL`.
    pub ecxt_newtuple: Option<SlotId>,
    /// `MemoryContext ecxt_per_query_memory` — the owning EState's per-query
    /// context (or the creating caller's context for a standalone context).
    pub ecxt_per_query_memory: Mcx<'mcx>,
    /// `MemoryContext ecxt_per_tuple_memory` — short-term working memory,
    /// reset per tuple. A real owned child context of
    /// `ecxt_per_query_memory`.
    pub ecxt_per_tuple_memory: MemoryContext,
    /// `Datum *ecxt_aggvalues` — precomputed aggregate values.
    pub ecxt_aggvalues: PgVec<'mcx, Datum<'mcx>>,
    /// `bool *ecxt_aggnulls` — their is-null flags.
    pub ecxt_aggnulls: PgVec<'mcx, bool>,
    /// `Datum caseValue_datum` / `bool caseValue_isNull` — CASE expr value.
    pub caseValue_datum: Datum<'mcx>,
    pub caseValue_isNull: bool,
    /// `Datum domainValue_datum` / `bool domainValue_isNull` — domain check.
    pub domainValue_datum: Datum<'mcx>,
    pub domainValue_isNull: bool,
    /// `ExprContext_CB *ecxt_callbacks` — registered shutdown callbacks.
    pub ecxt_callbacks: Option<PgBox<'mcx, ExprContext_CB<'mcx>>>,
}

/// `ParamExecData` (params.h):
///
/// ```c
/// typedef struct ParamExecData {
///     void   *execPlan;   /* should be "SubPlanState *" */
///     Datum   value;
///     bool    isnull;
/// } ParamExecData;
/// ```
///
/// `execPlan` is the C `void *` (documented "should be `SubPlanState *`") link to
/// a not-yet-evaluated subplan: when non-`NULL` it points at the `SubPlanState`
/// that must run to lazily produce this PARAM_EXEC's value (the InitPlan /
/// correlated-SubPlan lazy-evaluation mechanism). In the owned model the
/// `SubPlanState` is not reachable by a stored raw pointer or a `&mut` alias —
/// the InitPlan `SubPlanState`s are owned by the parent plan-state's `initPlan`
/// list and the subselect plan-state trees live in `EState.es_subplanstates`
/// (one per `SubPlan`, addressed by the subplan's 1-based `plan_id`). So the
/// `void *execPlan` alias is rendered here as the subplan's stable identity:
/// [`ExecPlanLink`], a 1-based `plan_id` index into `es_subplanstates` (the same
/// index the C `sstate->planstate = list_nth(es_subplanstates, plan_id - 1)`
/// uses). `None` is the C `NULL` ("value is valid, no evaluation pending"); the
/// executor resolves the identity back to its `SubPlanState` when it must run the
/// subplan (`ExecSetParamPlan`). This is an index, not a side-table registry.
#[derive(Clone, Debug)]
pub struct ParamExecData<'mcx> {
    /// `void *execPlan` — `Some(link)` while this param awaits lazy evaluation by
    /// the linked subplan; `None` (the C `NULL`) once the value is valid.
    pub execPlan: Option<ExecPlanLink>,
    pub value: Datum<'mcx>,
    pub isnull: bool,
}

/// The owned-model rendering of `ParamExecData.execPlan`'s `void *` ("should be
/// `SubPlanState *`"): the stable identity of the subplan that must run to
/// produce a not-yet-evaluated PARAM_EXEC value. It is the subplan's 1-based
/// `plan_id`, i.e. the index identity into [`EStateData::es_subplanstates`] (and
/// the parent plan-state's `initPlan` list) — the same identity the C uses to
/// reach `sstate->planstate` via `list_nth(es_subplanstates, plan_id - 1)`. The
/// executor resolves this back to the `SubPlanState` to lazily evaluate the
/// initplan. Not a registry: it carries no side state, only the C pointer's
/// referent identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExecPlanLink {
    /// `SubPlan.plan_id` — 1-based; `plan_id - 1` indexes `es_subplanstates`.
    pub plan_id: i32,
}

impl Default for ParamExecData<'_> {
    fn default() -> Self {
        // C `palloc0` zero-init: NULL execPlan, NULL value, isnull cleared.
        ParamExecData {
            execPlan: None,
            value: Datum::null(),
            isnull: false,
        }
    }
}

/// `IndexInfo` (execnodes.h) — full field set mirroring the C struct.
///
/// `ii_IndexAttrNumbers` is the C `AttrNumber ii_IndexAttrNumbers[INDEX_MAX_KEYS]`,
/// fixed-size here. The C struct's `NodeTag type` is the variant discriminant
/// and is not carried as a field.
///
/// In the owned-tree model the C pointer-array members become owned `PgVec`s
/// (or `None` for the C `NULL`); the expression/predicate `List *`s become
/// `PgVec<Expr>` / `PgVec<ExprState>`; the single `ExprState *ii_PredicateState`
/// becomes `Option<PgBox<ExprState>>`. The `void *ii_AmCache` (AM-private cache)
/// becomes the [`Opaque`] downcast handle, and the `MemoryContext ii_Context`
/// (the context holding this node) becomes an [`Mcx`] borrow (`None` until set).
///
/// Holding `ExprState` (which is not `Clone`/`Copy`) and `Opaque` (a
/// `Box<dyn Any>`) means `IndexInfo` is neither `Clone` nor `Copy`, unlike the
/// earlier trimmed shape; consumers borrow it (`&IndexInfo`).
#[derive(Debug, Default)]
pub struct IndexInfo<'mcx> {
    /// `int ii_NumIndexAttrs` — total number of columns in the index.
    pub ii_NumIndexAttrs: i32,
    /// `int ii_NumIndexKeyAttrs` — number of key columns in the index.
    pub ii_NumIndexKeyAttrs: i32,
    /// `AttrNumber ii_IndexAttrNumbers[INDEX_MAX_KEYS]` — heap-attribute
    /// numbers of the index's columns (0 for an expression column).
    pub ii_IndexAttrNumbers: [AttrNumber; INDEX_MAX_KEYS as usize],
    /// `List *ii_Expressions` — expr trees for expression entries, or `None`
    /// (the C `NIL`) if none.
    pub ii_Expressions: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `List *ii_ExpressionsState` — exec state for expressions, or `None`
    /// (the C `NIL`) if none.
    pub ii_ExpressionsState: Option<PgVec<'mcx, ExprState<'mcx>>>,
    /// `List *ii_Predicate` — partial-index predicate, or `None` (the C `NIL`)
    /// if none.
    pub ii_Predicate: Option<PgVec<'mcx, crate::primnodes::Expr>>,
    /// `ExprState *ii_PredicateState` — exec state for the predicate, or `None`
    /// (the C `NULL`) if none.
    pub ii_PredicateState: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `Oid *ii_ExclusionOps` — per-column exclusion operators, or `None` (the
    /// C `NULL`) if none.
    pub ii_ExclusionOps: Option<PgVec<'mcx, Oid>>,
    /// `Oid *ii_ExclusionProcs` — underlying function OIDs for
    /// `ii_ExclusionOps`.
    pub ii_ExclusionProcs: Option<PgVec<'mcx, Oid>>,
    /// `uint16 *ii_ExclusionStrats` — opclass strategy numbers for
    /// `ii_ExclusionOps`.
    pub ii_ExclusionStrats: Option<PgVec<'mcx, u16>>,
    /// `Oid *ii_UniqueOps` — like `ii_ExclusionOps`, but for unique indexes.
    pub ii_UniqueOps: Option<PgVec<'mcx, Oid>>,
    /// `Oid *ii_UniqueProcs` — underlying function OIDs for `ii_UniqueOps`.
    pub ii_UniqueProcs: Option<PgVec<'mcx, Oid>>,
    /// `uint16 *ii_UniqueStrats` — opclass strategy numbers for `ii_UniqueOps`.
    pub ii_UniqueStrats: Option<PgVec<'mcx, u16>>,
    /// `bool ii_Unique` — is it a unique index?
    pub ii_Unique: bool,
    /// `bool ii_NullsNotDistinct` — does a unique index treat NULLs as not
    /// distinct?
    pub ii_NullsNotDistinct: bool,
    /// `bool ii_ReadyForInserts` — is the index ready for inserts?
    pub ii_ReadyForInserts: bool,
    /// `bool ii_CheckedUnchanged` — HOT/summarizing-unchanged checked for the
    /// current tuple?
    pub ii_CheckedUnchanged: bool,
    /// `bool ii_IndexUnchanged` — is the current tuple unchanged wrt this
    /// index?
    pub ii_IndexUnchanged: bool,
    /// `bool ii_Concurrent` — built with CONCURRENTLY?
    pub ii_Concurrent: bool,
    /// `bool ii_BrokenHotChain` — was a broken HOT chain seen during build?
    pub ii_BrokenHotChain: bool,
    /// `bool ii_Summarizing` — is it a summarizing index?
    pub ii_Summarizing: bool,
    /// `bool ii_WithoutOverlaps` — is it a WITHOUT OVERLAPS index?
    pub ii_WithoutOverlaps: bool,
    /// `int ii_ParallelWorkers` — number of parallel workers for the build.
    pub ii_ParallelWorkers: i32,
    /// `Oid ii_Am` — the index access method's OID.
    pub ii_Am: Oid,
    /// `void *ii_AmCache` — private cache area for the index AM. `Opaque`
    /// default is the C `NULL`.
    pub ii_AmCache: Opaque,
    /// `MemoryContext ii_Context` — the context holding this `IndexInfo`.
    /// `None` until set (the C struct stores the owning context here).
    pub ii_Context: Option<Mcx<'mcx>>,
}

/// `TriggerDesc` (utils/reltrigger.h) — the full per-relation trigger set
/// (array of [`Trigger`] + the per-event/transition flags), re-exported from
/// the leaf [`types_trigger`] crate so consumers keep the `execnodes::TriggerDesc`
/// path. The relation builds it via `RelationBuildTriggers` (commands/trigger.c);
/// the executor reads the row-level booleans before firing.
pub use types_trigger::{Trigger, TriggerDesc};

/// `ResultRelInfo` (execnodes.h), trimmed to the fields ports consume. Lives
/// in the EState's [`EStateData::es_result_rel_pool`], addressed by [`RriId`].
#[derive(Debug, Default)]
pub struct ResultRelInfo<'mcx> {
    /// `TriggerDesc *ri_TrigDesc` — triggers to be fired, if any. `None` is the
    /// C `NULL` (relation has no triggers).
    pub ri_TrigDesc: Option<PgBox<'mcx, TriggerDesc<'mcx>>>,
    /// `Index ri_RangeTableIndex` — the rangetable index, or 0 for a
    /// trigger-only target relation not in the range table.
    pub ri_RangeTableIndex: Index,
    /// `Relation ri_RelationDesc` — the open target relation. In C this
    /// aliases the relation `es_relations` (or the trigger-target list) owns;
    /// here it is a [`types_rel::Relation::alias`] of that handle (shared
    /// data, no release authority).
    pub ri_RelationDesc: Option<types_rel::Relation<'mcx>>,
    /// `int ri_NumIndices` — number of indices existing on result relation.
    pub ri_NumIndices: i32,
    /// `RelationPtr ri_IndexRelationDescs` — the open index relations
    /// (aliases of the executor-owned opens; a `None` element is the C NULL
    /// slot of a closed/unopened index). `None` is the C NULL array.
    pub ri_IndexRelationDescs: Option<PgVec<'mcx, Option<types_rel::Relation<'mcx>>>>,
    /// `IndexInfo **ri_IndexRelationInfo` — per-index info, parallel to
    /// `ri_IndexRelationDescs`. `None` is the C NULL array.
    pub ri_IndexRelationInfo: Option<PgVec<'mcx, IndexInfo<'mcx>>>,
    /// `List *ri_onConflictArbiterIndexes` — index OIDs that arbitrate
    /// ON CONFLICT / apply-conflict detection. `None` is the C NIL.
    pub ri_onConflictArbiterIndexes: Option<PgVec<'mcx, Oid>>,
    /// `TupleTableSlot *ri_TrigOldSlot` — for trigger OLD tuples.
    pub ri_TrigOldSlot: Option<SlotId>,
    /// `TupleTableSlot *ri_TrigNewSlot` — for trigger NEW tuples.
    pub ri_TrigNewSlot: Option<SlotId>,
    /// `TupleTableSlot *ri_ReturningSlot` — for RETURNING processing.
    pub ri_ReturningSlot: Option<SlotId>,
    /// `TupleTableSlot *ri_AllNullSlot` — all-NULL slot for RETURNING.
    pub ri_AllNullSlot: Option<SlotId>,
    /// `TupleTableSlot *ri_PartitionTupleSlot` — non-NULL if the relation is a
    /// partition whose rowtype differs from the root partitioned table's; used
    /// to convert tuples for the partition's own layout. `None` is the C NULL.
    pub ri_PartitionTupleSlot: Option<SlotId>,
    /// `Bitmapset *ri_extraUpdatedCols` — generated columns updated.
    pub ri_extraUpdatedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `bool ri_extraUpdatedCols_valid`.
    pub ri_extraUpdatedCols_valid: bool,
    /// `struct ResultRelInfo *ri_RootResultRelInfo` — the root target
    /// relation, when this is a child (partition routing / inheritance).
    pub ri_RootResultRelInfo: Option<RriId>,
    /// `TupleConversionMap *ri_ChildToRootMap` (+ its computed flag).
    pub ri_ChildToRootMap: Option<PgBox<'mcx, TupleConversionMap<'mcx>>>,
    pub ri_ChildToRootMapValid: bool,
    /// `TupleConversionMap *ri_RootToChildMap` (+ its computed flag).
    pub ri_RootToChildMap: Option<PgBox<'mcx, TupleConversionMap<'mcx>>>,
    pub ri_RootToChildMapValid: bool,
    /// `ExprState **ri_GeneratedExprsI` — per-column stored-generated-column
    /// expression states for INSERT/MERGE (1-based attno - 1 indexed, parallel
    /// to the relation's columns). A `None` element is a column with no stored
    /// generation expression. `None` for the whole field is the C `NULL` (not
    /// yet initialized).
    pub ri_GeneratedExprsI: Option<PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>>,
    /// `ExprState **ri_GeneratedExprsU` — same, for UPDATE.
    pub ri_GeneratedExprsU: Option<PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>>,
    /// `int ri_NumGeneratedNeededI` — number of stored generated columns to
    /// compute for INSERT/MERGE.
    pub ri_NumGeneratedNeededI: i32,
    /// `int ri_NumGeneratedNeededU` — same, for UPDATE.
    pub ri_NumGeneratedNeededU: i32,
    /// `ProjectionInfo *ri_projectReturning` — the compiled RETURNING
    /// projection (built by `ExecBuildProjectionInfo`). `None` is the C `NULL`.
    pub ri_projectReturning: Option<PgBox<'mcx, ProjectionInfo<'mcx>>>,
    /// `ri_TrigDesc->trig_update_before_row` — BEFORE ROW UPDATE triggers
    /// exist.
    pub ri_trig_update_before_row: bool,
    /// `ri_TrigDesc->trig_update_instead_row` — INSTEAD OF ROW UPDATE triggers
    /// exist (a view).
    pub ri_trig_update_instead_row: bool,
    /// `ri_TrigDesc->trig_update_after_row` — AFTER ROW UPDATE triggers exist.
    pub ri_trig_update_after_row: bool,
    /// `bool` proxy for `ri_TrigDesc != NULL` — the relation has any triggers.
    pub ri_has_trigdesc: bool,
    /// `FdwRoutine *ri_FdwRoutine != NULL` — the relation is a foreign table
    /// handled by an FDW (the routine vtable lands with the fdwapi type).
    pub ri_has_fdw_routine: bool,
    /// `ProjectionInfo *ri_projectReturning != NULL` — a RETURNING projection
    /// has been built for this relation.
    pub ri_has_project_returning: bool,
    /// `List *ri_WithCheckOptions != NIL` — WITH CHECK OPTION constraints
    /// apply (RLS / updatable views).
    pub ri_has_with_check_options: bool,
    /// `bool ri_needLockTagTuple` — UPDATE/DELETE needs a tuple-level heavy
    /// lock (in-place update tuple lock) on this relation.
    pub ri_needLockTagTuple: bool,
    /// `bool ri_projectNewInfoValid` — `ri_projectNew` / `ri_newTupleSlot` /
    /// `ri_oldTupleSlot` have been initialized.
    pub ri_projectNewInfoValid: bool,
    /// `TupleTableSlot *ri_oldTupleSlot` — old-tuple slot for UPDATE
    /// projection (id into `es_tupleTable`).
    pub ri_oldTupleSlot: Option<SlotId>,
    /// `TupleTableSlot *ri_newTupleSlot` — new-tuple slot (UPDATE/INSERT
    /// projection output).
    pub ri_newTupleSlot: Option<SlotId>,
    /// `AttrNumber ri_RowIdAttNo` — attribute number of the row-identity junk
    /// column ("ctid" for heap, "wholerow" for foreign/other relkinds) found in
    /// the subplan targetlist for UPDATE/DELETE/MERGE. `0` (`InvalidAttrNumber`)
    /// when not applicable.
    pub ri_RowIdAttNo: AttrNumber,
    /// `List *ri_returningList` — the RETURNING target list for this relation
    /// (stored alongside `ri_projectReturning`). `None` is the C `NIL`.
    pub ri_returningList: Option<PgVec<'mcx, crate::primnodes::TargetEntry<'mcx>>>,
    /// `List *ri_WithCheckOptions` — the WITH CHECK OPTION constraints (RLS /
    /// updatable views) for this relation. `None` is the C `NIL`. (The
    /// presence flag `ri_has_with_check_options` mirrors `!= NIL`.)
    pub ri_WithCheckOptions: Option<PgVec<'mcx, crate::nodes::Node<'mcx>>>,
    /// `List *ri_WithCheckOptionExprs` — the compiled `ExprState`s for the
    /// WITH CHECK OPTION constraints, parallel to `ri_WithCheckOptions`.
    pub ri_WithCheckOptionExprs: Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>,
    /// `struct OnConflictSetState *ri_onConflict` — exec state for ON CONFLICT
    /// DO UPDATE. `None` is the C `NULL`.
    pub ri_onConflict: Option<PgBox<'mcx, crate::modifytable::OnConflictSetState<'mcx>>>,
    /// `List *ri_MergeActions[NUM_MERGE_MATCH_KINDS]` — per-`MergeMatchKind`
    /// lists of `MergeActionState`s (built by `ExecInitMerge` /
    /// `ExecInitPartitionInfo`). Each element `None` is the C `NIL` for that
    /// match kind.
    pub ri_MergeActions:
        [Option<PgVec<'mcx, PgBox<'mcx, crate::modifytable::MergeActionState<'mcx>>>>;
            crate::modifytable::NUM_MERGE_MATCH_KINDS],
    /// `ExprState *ri_MergeJoinCondition` — compiled MERGE join-condition qual
    /// for this relation. `None` is the C `NULL`.
    pub ri_MergeJoinCondition: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

/// `ExecProcNodeMtd` — the per-node execution callback stored in
/// `PlanState.ExecProcNode`. The cross-node recursion `ExecProcNode(child)`
/// dispatches through this pointer (installed at node init). Returns the
/// `SlotId` of the produced tuple's slot, or `None` for the C `NULL` return.
/// The callback is tied to the state tree's allocator lifetime: any memory it
/// needs (C: `palloc` while executing) comes from
/// [`EStateData::es_query_cxt`].
pub type ExecProcNodeMtd<'mcx> = Option<
    fn(
        pstate: &mut PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<SlotId>>,
>;

/// `PlanState` head (execnodes.h), trimmed to the fields ports consume.
#[derive(Debug, Default)]
pub struct PlanStateData<'mcx> {
    /// `EState *state` — the executor state for this query (the *single* `EState`
    /// that owns the whole `PlanState` tree). Carried as a lifetime-free raw
    /// back-pointer ([`EStateLink`]); `None` is the un-linked default (the C
    /// zero-init before `ExecInitNode` sets `planstate->state = estate`). See
    /// [`EStateLink`] for the liveness invariant. `Option` is `Default` (→
    /// `None`), so `#[derive(Default)]` keeps working even though `EStateLink`
    /// (a `NonNull`) is not `Default`.
    pub state: Option<EStateLink>,
    /// `Plan *plan` — associated plan node. C aliases the shared, read-only
    /// plan tree (`planstate->plan = (Plan *) node`); the borrow does the
    /// same — node init never copies the plan.
    pub plan: Option<&'mcx crate::nodes::Node<'mcx>>,
    /// `ExecProcNodeMtd ExecProcNode` — function to return next tuple.
    pub ExecProcNode: ExecProcNodeMtd<'mcx>,
    /// `ExecProcNodeMtd ExecProcNodeReal` — actual function, if above is a
    /// wrapper. `ExecSetExecProcNode` records the per-node "real" next-tuple
    /// routine here and installs the `ExecProcNodeFirst` first-call wrapper into
    /// `ExecProcNode`; the wrapper dispatches through this slot (and, once past
    /// the first call, copies it back into `ExecProcNode` or the instrumentation
    /// wrapper).
    pub ExecProcNodeReal: ExecProcNodeMtd<'mcx>,
    /// `Instrumentation *instrument` — optional runtime stats for this node.
    pub instrument: Option<PgBox<'mcx, Instrumentation>>,
    /// `bool async_capable` — true if node is async-capable. Set by the planner
    /// / async-aware parent (`ExecInitAppend`); the default `false` is the C
    /// zero-initialized value for nodes that are not async-capable.
    pub async_capable: bool,
    /// `ExprState *qual` — boolean qual condition (compiled `plan.qual`).
    /// `None` = the C `NULL` (always-true).
    pub qual: Option<PgBox<'mcx, crate::execexpr::ExprState<'mcx>>>,
    /// `struct PlanState *lefttree` — input plan tree (`outerPlanState`).
    pub lefttree: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `struct PlanState *righttree` — `innerPlanState`.
    pub righttree: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `List *initPlan` — `SubPlanState` nodes for my init-plans (un-correlated
    /// expression subselects). `None` is the C `NIL`.
    pub initPlan: Option<PgVec<'mcx, SubPlanState<'mcx>>>,
    /// `List *subPlan` — `SubPlanState` nodes in my expressions. `None` is the
    /// C `NIL`.
    pub subPlan: Option<PgVec<'mcx, SubPlanState<'mcx>>>,
    /// `Bitmapset *chgParam` — set of IDs of changed Params.
    pub chgParam: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `ExprContext *ps_ExprContext` — node's expression-evaluation context
    /// (id into `es_exprcontexts`).
    pub ps_ExprContext: Option<EcxtId>,
    /// `TupleDesc ps_ResultTupleDesc` — node's return type.
    pub ps_ResultTupleDesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `TupleTableSlot *ps_ResultTupleSlot` — slot for my result tuples (id
    /// into `es_tupleTable`).
    pub ps_ResultTupleSlot: Option<SlotId>,
    /// `ProjectionInfo *ps_ProjInfo` — info for doing tuple projection.
    pub ps_ProjInfo: Option<PgBox<'mcx, ProjectionInfo<'mcx>>>,
    /// `bool scanopsset` / `const TupleTableSlotOps *scanops` /
    /// `bool scanopsfixed` — information about the type of the scan slot.
    pub scanopsset: bool,
    pub scanops: Option<TupleSlotKind>,
    pub scanopsfixed: bool,
    /// `bool resultopsset` / `const TupleTableSlotOps *resultops` /
    /// `bool resultopsfixed` — information about the type of the result slot.
    pub resultopsset: bool,
    pub resultops: Option<TupleSlotKind>,
    pub resultopsfixed: bool,
}

/// `ScanState` head (execnodes.h), trimmed.
#[derive(Debug, Default)]
pub struct ScanStateData<'mcx> {
    /// `PlanState ps` — its first field is `NodeTag`.
    pub ps: PlanStateData<'mcx>,
    /// `Relation ss_currentRelation` — the relation this scan node is scanning,
    /// or `None` (C `NULL`, e.g. a ForeignScan/CustomScan with no
    /// currentRelation). Aliases the executor-owned open (no release authority).
    pub ss_currentRelation: Option<types_rel::Relation<'mcx>>,
    /// `struct TableScanDescData *ss_currentScanDesc` — the table scan
    /// descriptor (`NULL` for index-only scans, which carry no heap scan). The
    /// table-AM scan-descriptor type lives above this crate's layer, so the
    /// owned handle rides opaquely; consumers that need it resolve it through
    /// the table-AM owner.
    pub ss_currentScanDesc: Option<Opaque>,
    /// `TupleTableSlot *ss_ScanTupleSlot` — id into `es_tupleTable`.
    pub ss_ScanTupleSlot: Option<SlotId>,
}

/// `SubqueryScanState` (execnodes.h):
///
/// ```c
/// typedef struct SubqueryScanState {
///     ScanState   ss;             /* its first field is NodeTag */
///     PlanState  *subplan;
/// } SubqueryScanState;
/// ```
#[derive(Debug, Default)]
pub struct SubqueryScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `PlanState *subplan` — the sub-query's `PlanState`. The SubqueryScan
    /// node's single child link, carried as the owned whole-node
    /// [`crate::planstate::PlanStateNode`] so the executor can recurse into it
    /// (`ExecProcNode`/`ExecEndNode`/`ExecReScan`) through the central dispatch.
    pub subplan: Option<PgBox<'mcx, crate::planstate::PlanStateNode<'mcx>>>,
}

// `ModifyTableState` (execnodes.h) is the full owned struct defined in
// `crate::modifytable` (landed with the `nodeModifyTable.c` port) and
// re-exported at `types_nodes::ModifyTableState`. execPartition's tuple-routing
// port consumes its `ps` / `resultRelInfo` / `rootResultRelInfo` fields, which
// remain present in the full definition.

/// The resolved outcome of `fetch_cursor_param_value`'s live-state core:
/// reading `econtext->ecxt_param_list_info->params[paramId - 1]` (calling the
/// dynamic `paramFetch` hook when present) and, for an OID-valid non-NULL param,
/// classifying its `ptype`. `None` (the C falls through to "no value found") is
/// the `Option` wrapper in the seam return.
#[derive(Debug)]
pub enum FetchedCursorParam<'mcx> {
    /// `prm->ptype == REFCURSOROID` — the decoded `refcursor` text value
    /// (`TextDatumGetCString`, palloc'd in the caller's `mcx`).
    RefCursor(PgString<'mcx>),
    /// `prm->ptype` is some other valid OID (caller raises datatype_mismatch).
    WrongType(Oid),
}

/// The per-scan-type TID extraction outcome (`execCurrentOf` plain-scan
/// strategy, after `search_plan_tree` found the scan node and the
/// `TupIsNull`/`pending_rescan` "inactive" test passed). C digs the TID out of
/// the scan's current physical tuple — `xs_heaptid` for an `IndexOnlyScanState`,
/// else the scan tuple's `SelfItemPointerAttributeNumber` via `slot_getsysattr`
/// (with the `USE_ASSERT_CHECKING` tableoid cross-check).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanTidOutcome {
    /// A valid physical TID was extracted (C `*current_tid = ...; return true`).
    Tid(types_tuple::heaptuple::ItemPointerData),
    /// The scan provided no physical tuple / null self-ctid — the C raises the
    /// "not a simply updatable scan" error (the caller turns this into that
    /// `ereport`, matching the C).
    NotUpdatable,
}

/// `execCurrentOf`'s result: the C `bool` return plus the `*current_tid`
/// out-parameter. `Found` is the C `return true` (a row of the target table is
/// currently scanned); `NotOnThisTable` is `return false` (the cursor is valid
/// for the table but is not currently scanning a row of *this* table — a legal
/// inheritance case). The `ereport(ERROR)` paths surface as `Err`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CurrentOfTid {
    /// A row was identified; carries the row's TID.
    Found(types_tuple::heaptuple::ItemPointerData),
    /// The cursor is not currently scanning a row of this table.
    NotOnThisTable,
}

/// A borrowed view of a *running* cursor's live executor state, lent by the
/// portal/executor owner (`GetPortalByName` + its `QueryDesc`) to a consumer
/// (`execCurrentOf`) for the duration of a callback. C reaches the portal's
/// `strategy`/`atStart`/`atEnd` scalar fields and, through `queryDesc`, the live
/// `EState` (rowmarks, range table) and `PlanState` tree. Lending a borrow (not
/// returning `&'static mut`) keeps the foreign owner in control of the state's
/// lifetime, per the seam rules.
#[derive(Debug)]
pub struct RunningCursorState<'a, 'mcx> {
    /// `portal->strategy` — the C `PortalStrategy` code (`PORTAL_ONE_SELECT`
    /// etc.). Modeled as the raw `u32` the portal owner stores.
    pub strategy: u32,
    /// `queryDesc != NULL && queryDesc->estate != NULL` — false for a held
    /// cursor or a non-SELECT with no live query.
    pub has_live_query: bool,
    /// `portal->atStart` — cursor is before the first row.
    pub at_start: bool,
    /// `portal->atEnd` — cursor is after the last row.
    pub at_end: bool,
    /// `queryDesc->estate` — the live executor state (rowmarks, range table,
    /// slot pool). `None` when `has_live_query` is false.
    pub estate: Option<&'a EStateData<'mcx>>,
    /// `queryDesc->planstate` — the root of the live plan-state tree. `None`
    /// when `has_live_query` is false.
    pub planstate: Option<&'a PlanStateNode<'mcx>>,
}

/// `RowMarkType` (nodes/plannodes.h) — the kind of row-marking a
/// FOR UPDATE/SHARE (or referential) rowmark requires.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum RowMarkType {
    /// `ROW_MARK_EXCLUSIVE` — obtain exclusive tuple lock.
    Exclusive = 0,
    /// `ROW_MARK_NOKEYEXCLUSIVE` — obtain no-key exclusive tuple lock.
    NoKeyExclusive = 1,
    /// `ROW_MARK_SHARE` — obtain shared tuple lock.
    Share = 2,
    /// `ROW_MARK_KEYSHARE` — obtain keyshare tuple lock.
    KeyShare = 3,
    /// `ROW_MARK_REFERENCE` — reference the row (no lock).
    Reference = 4,
    /// `ROW_MARK_COPY` — physically copy the row value.
    Copy = 5,
}

impl RowMarkType {
    /// `RowMarkRequiresRowShareLock(marktype)` (nodes/plannodes.h):
    /// `((marktype) <= ROW_MARK_KEYSHARE)`.
    #[inline]
    pub fn requires_row_share_lock(self) -> bool {
        (self as u32) <= (RowMarkType::KeyShare as u32)
    }
}

/// `ExecRowMark` (execnodes.h) — runtime state for a FOR [KEY] UPDATE/SHARE (or
/// referential) row mark, trimmed to the fields consumed so far. The unconsumed
/// C fields (`relation`, `rti`, `prti`, `rowmarkId`, `strength`, `waitPolicy`,
/// `ermActive`, `ermExtra`) land with their first consumer (docs/types.md rule
/// 3).
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecRowMark {
    /// `Oid relid` — its OID (or `InvalidOid`, if subquery).
    pub relid: Oid,
    /// `RowMarkType markType` — see `RowMarkType`.
    pub markType: RowMarkType,
    /// `ItemPointerData curCtid` — ctid of currently locked tuple, if any.
    pub curCtid: types_tuple::heaptuple::ItemPointerData,
}

impl Default for RowMarkType {
    fn default() -> Self {
        RowMarkType::Exclusive
    }
}

/// `EState` (execnodes.h) — working storage for one Executor invocation,
/// trimmed to the fields ports consume (unconsumed C fields — `es_snapshot`,
/// `es_crosscheck_snapshot`, `es_junkFilter`, `es_queryEnv` — are trimmed
/// outright and land with their first consumer, per docs/types.md rule 3).
#[derive(Debug)]
pub struct EStateData<'mcx> {
    /// `ScanDirection es_direction` — current scan direction.
    pub es_direction: ScanDirection,
    /// `Snapshot es_snapshot` — time qual to use. The C `Snapshot` is a shared
    /// pointer, modeled as the shared `Rc<SnapshotData>` the active-snapshot
    /// stack/owner alias; `None` is the C `NULL`. Lands with its first consumer
    /// (the index/heap scan ports), per docs/types.md rule 3.
    pub es_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    /// `Snapshot es_crosscheck_snapshot` — crosscheck time qual for RI (used by
    /// `table_tuple_update`/`table_tuple_delete`). Shared pointer, modeled like
    /// `es_snapshot` as an `Rc<SnapshotData>` alias; `None` is the C
    /// `InvalidSnapshot`.
    pub es_crosscheck_snapshot: Option<alloc::rc::Rc<types_snapshot::SnapshotData>>,
    /// `struct EPQState *es_epq_active` — if not `None`, the EvalPlanQual
    /// recheck state this EState belongs to (C: a pointer to the active
    /// `EPQState`). The owned model holds the real `EPQState`; scan nodes read
    /// its `relsubs_*` arrays directly. `None` is the C `NULL`.
    pub es_epq_active: Option<PgBox<'mcx, EPQState<'mcx>>>,
    /// `List *es_range_table` — the query's range table.
    pub es_range_table: PgVec<'mcx, RangeTblEntry<'mcx>>,
    /// `Index es_range_table_size` — size of the range table.
    pub es_range_table_size: usize,
    /// `ExecRowMark **es_rowmarks` — per-RTE `ExecRowMark`s (indexed by RT
    /// index − 1), with `None` entries for RTEs that have no rowmark. Empty =
    /// the C `NULL` (no FOR UPDATE/SHARE in the query).
    pub es_rowmarks: PgVec<'mcx, Option<ExecRowMark>>,
    /// `Relation *es_relations` — array of per-RTE open relations, `None`
    /// until opened. Parallel to `es_range_table`. These handles own the
    /// opens: EState teardown (or abort-path drop) releases them.
    pub es_relations: PgVec<'mcx, Option<types_rel::Relation<'mcx>>>,
    /// `List *es_rteperminfos` — the query's RTEPermissionInfos.
    pub es_rteperminfos: PgVec<'mcx, RTEPermissionInfo<'mcx>>,
    /// `PlannedStmt *es_plannedstmt` — link to the top of the plan tree.
    pub es_plannedstmt: Option<PgBox<'mcx, PlannedStmt<'mcx>>>,
    /// `List *es_part_prune_infos` — `PlannedStmt.partPruneInfos`.
    pub es_part_prune_infos: PgVec<'mcx, Opaque>,
    /// `List *es_part_prune_states` — the `PartitionPruneState`s built by
    /// `ExecDoInitialPruning`, parallel to `es_part_prune_infos`. Each is a
    /// `PgBox` because the consuming plan node (e.g. `MergeAppendState`) takes
    /// ownership of its entry (C aliases the same object the list holds; the
    /// owned model moves it out of the pool with `.take()`, leaving a `None`
    /// tombstone so the parallel indexing with `es_part_prune_infos` /
    /// `es_part_prune_results` stays stable).
    pub es_part_prune_states:
        PgVec<'mcx, Option<PgBox<'mcx, crate::partition::PartitionPruneState<'mcx>>>>,
    /// `List *es_part_prune_results` — per-pruneinfo bitmapset of subplans that
    /// survived initial pruning (a `None` element is the C `NULL`), parallel to
    /// `es_part_prune_infos`.
    pub es_part_prune_results: PgVec<'mcx, Option<PgBox<'mcx, Bitmapset<'mcx>>>>,
    /// `CommandId es_output_cid` — the inserted/updated tuples' cmin/cmax.
    pub es_output_cid: CommandId,
    /// `ResultRelInfo **es_result_relations` — per-RTE result-rel info (ids
    /// into the pool), allocated only if needed. Empty = the C `NULL`.
    pub es_result_relations: PgVec<'mcx, Option<RriId>>,
    /// `List *es_opened_result_relations` — result relations already opened.
    pub es_opened_result_relations: PgVec<'mcx, RriId>,
    /// `List *es_tuple_routing_result_relations` — for tuple routing.
    pub es_tuple_routing_result_relations: PgVec<'mcx, RriId>,
    /// `List *es_trig_target_relations` — trigger-only target relations.
    pub es_trig_target_relations: PgVec<'mcx, RriId>,
    /// `List *es_insert_pending_result_relations` — pending multi-inserts.
    pub es_insert_pending_result_relations: PgVec<'mcx, RriId>,
    /// `List *es_insert_pending_modifytables` — their ModifyTableStates.
    pub es_insert_pending_modifytables: PgVec<'mcx, Opaque>,
    /// `ParamListInfo es_param_list_info` — values of external params. The
    /// `ParamListInfo` lives in the (unported) params unit, so this carries the
    /// opaque handle (`NULL` == no external params). The PREPARE/EXECUTE driver
    /// sets it on the throwaway EState before evaluating EXECUTE parameters.
    pub es_param_list_info: crate::parsestmt::ParamListInfoHandle,
    /// `ParamExecData *es_param_exec_vals` — values of internal params.
    /// Empty = the C `NULL`.
    pub es_param_exec_vals: PgVec<'mcx, ParamExecData<'mcx>>,
    /// `MemoryContext es_query_cxt` — the per-query context the executor
    /// allocates in (C: the context `CreateExecutorState` made the `EState`
    /// in, current while nodes init and run).
    pub es_query_cxt: Mcx<'mcx>,
    /// `List *es_tupleTable` — the executor slot pool. Slots are addressed by
    /// [`SlotId`] (the owned-model `TupleTableSlot *`). Each entry is a
    /// payload-bearing [`SlotData`] (the four `Virtual/Heap/Minimal/BufferHeap`
    /// superstructures with `tts_values`/`tts_isnull`), so the pool carries the
    /// real per-attribute slot contents, not just the shared header bits.
    /// The header projection ([`slot`](EStateData::slot) /
    /// [`slot_mut`](EStateData::slot_mut)) still resolves through
    /// [`SlotData::base`], so the existing header consumers are unchanged; the
    /// payload-aware path uses [`slot_data`](EStateData::slot_data) /
    /// [`slot_data_mut`](EStateData::slot_data_mut).
    pub es_tupleTable: PgVec<'mcx, SlotData<'mcx>>,
    /// `uint64 es_processed` — # of tuples processed by current command.
    pub es_processed: u64,
    /// `uint64 es_total_processed` — total across all firings.
    pub es_total_processed: u64,
    /// `int es_top_eflags` — eflags passed to ExecutorStart.
    pub es_top_eflags: i32,
    /// `int es_instrument` — instrumentation options (OR of flags).
    pub es_instrument: i32,
    /// `bool es_finished` — ExecutorFinish has run.
    pub es_finished: bool,
    /// `List *es_exprcontexts` — the ExprContext pool ([`EcxtId`] addressed;
    /// a freed context tombstones to `None`). Shutdown order at
    /// `FreeExecutorState` is reverse creation order (highest index first),
    /// matching the C `lcons` + front-to-back walk.
    pub es_exprcontexts: PgVec<'mcx, Option<ExprContext<'mcx>>>,
    /// `List *es_subplanstates` — exec state of each init plan.
    pub es_subplanstates: PgVec<'mcx, PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `List *es_auxmodifytables` — not-canSetTag ModifyTableStates.
    pub es_auxmodifytables: PgVec<'mcx, Opaque>,
    /// `ExprContext *es_per_tuple_exprcontext` — for per-output-tuple work.
    pub es_per_tuple_exprcontext: Option<EcxtId>,
    /// `const char *es_sourceText` — source query text.
    pub es_sourceText: Option<PgString<'mcx>>,
    /// `bool es_use_parallel_mode` — can we use parallel workers?
    pub es_use_parallel_mode: bool,
    /// `int es_parallel_workers_to_launch` / `_launched`.
    pub es_parallel_workers_to_launch: i32,
    pub es_parallel_workers_launched: i32,
    /// `int es_jit_flags` / `struct JitContext *es_jit` (jit-owned).
    pub es_jit_flags: i32,
    pub es_jit: Opaque,
    /// `Bitmapset *es_unpruned_relids` — RT indexes that will be scanned.
    pub es_unpruned_relids: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `PartitionDirectory es_partition_directory` (partdesc-owned).
    pub es_partition_directory: Opaque,
    /// Owned-model pool holding every `ResultRelInfo` belonging to this
    /// EState (C: caller-owned nodes aliased from the lists above), addressed
    /// by [`RriId`].
    pub es_result_rel_pool: PgVec<'mcx, ResultRelInfo<'mcx>>,
    /// `struct dsa_area *es_query_dsa` — the per-query DSA area for parallel
    /// execution, a live [`DsaAreaHandle`] into the DSA subsystem; `None` is
    /// the C `NULL` (no parallel workers). Consumed first by
    /// `nodeBitmapHeapscan`'s parallel-scan DSM setup.
    pub es_query_dsa: Option<types_execparallel::DsaAreaHandle>,
    /// `JunkFilter *es_junkFilter` — top-level junk filter, if any
    /// (execMain.c sets it in `InitPlan` for the top plan's junk attributes;
    /// `ExecFilterJunk` projects through it). `None` = the C `NULL`. Carries
    /// the real owned [`JunkFilter`].
    pub es_junkFilter: Option<PgBox<'mcx, JunkFilter<'mcx>>>,
    /// `QueryEnvironment *es_queryEnv` — context for `ENR` (ephemeral named
    /// relations, e.g. trigger transition tables). The `QueryEnvironment` lives
    /// above this layer; the executor only stores and threads it, so it rides
    /// opaquely. `None` = the C `NULL`.
    pub es_queryEnv: Opaque,
}

impl<'mcx> EStateData<'mcx> {
    /// `CreateExecutorState()`-shaped construction: initialize every carried
    /// field exactly as execUtils.c's `CreateExecutorState` does, with the
    /// EState's allocations living in (and accounted to) `mcx` (C: the fresh
    /// "ExecutorState" per-query context).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        EStateData {
            // estate->es_direction = ForwardScanDirection;
            es_direction: ForwardScanDirection,
            // es_snapshot = InvalidSnapshot; es_crosscheck_snapshot = InvalidSnapshot;
            // es_epq_active = NULL;
            es_snapshot: None,
            es_crosscheck_snapshot: None,
            es_epq_active: None,
            // es_range_table = NIL; es_range_table_size = 0;
            es_range_table: PgVec::new_in(mcx),
            es_range_table_size: 0,
            // es_rowmarks = NULL;
            es_rowmarks: PgVec::new_in(mcx),
            // es_relations = NULL;
            es_relations: PgVec::new_in(mcx),
            // es_rteperminfos = NIL; es_plannedstmt = NULL;
            es_rteperminfos: PgVec::new_in(mcx),
            es_plannedstmt: None,
            // es_part_prune_infos = NIL;
            es_part_prune_infos: PgVec::new_in(mcx),
            // es_part_prune_states = NIL; es_part_prune_results = NIL;
            es_part_prune_states: PgVec::new_in(mcx),
            es_part_prune_results: PgVec::new_in(mcx),
            // es_output_cid = (CommandId) 0;
            es_output_cid: 0,
            // es_result_relations = NULL; the relation lists = NIL;
            es_result_relations: PgVec::new_in(mcx),
            es_opened_result_relations: PgVec::new_in(mcx),
            es_tuple_routing_result_relations: PgVec::new_in(mcx),
            es_trig_target_relations: PgVec::new_in(mcx),
            // es_insert_pending_* = NIL;
            es_insert_pending_result_relations: PgVec::new_in(mcx),
            es_insert_pending_modifytables: PgVec::new_in(mcx),
            // es_param_list_info = NULL;
            es_param_list_info: crate::parsestmt::ParamListInfoHandle::NULL,
            // es_param_exec_vals = NULL;
            es_param_exec_vals: PgVec::new_in(mcx),
            // es_query_cxt = qcontext;
            es_query_cxt: mcx,
            // es_tupleTable = NIL;
            es_tupleTable: PgVec::new_in(mcx),
            // es_processed = 0; es_total_processed = 0;
            es_processed: 0,
            es_total_processed: 0,
            // es_top_eflags = 0; es_instrument = 0; es_finished = false;
            es_top_eflags: 0,
            es_instrument: 0,
            es_finished: false,
            // es_exprcontexts = NIL; es_subplanstates = NIL;
            es_exprcontexts: PgVec::new_in(mcx),
            es_subplanstates: PgVec::new_in(mcx),
            // es_auxmodifytables = NIL;
            es_auxmodifytables: PgVec::new_in(mcx),
            // es_per_tuple_exprcontext = NULL;
            es_per_tuple_exprcontext: None,
            // es_sourceText = NULL;
            es_sourceText: None,
            // es_use_parallel_mode = false; worker counters = 0;
            es_use_parallel_mode: false,
            es_parallel_workers_to_launch: 0,
            es_parallel_workers_launched: 0,
            // es_jit_flags = 0; es_jit = NULL;
            es_jit_flags: 0,
            es_jit: Opaque(None),
            // (set later by ExecInitRangeTable / partition pruning setup)
            es_unpruned_relids: None,
            es_partition_directory: Opaque(None),
            es_result_rel_pool: PgVec::new_in(mcx),
            // es_query_dsa = NULL;
            es_query_dsa: None,
            // es_junkFilter = NULL; es_queryEnv = NULL;
            es_junkFilter: None,
            es_queryEnv: Opaque(None),
        }
    }

    /// `ExecAllocTableSlot` — append a slot to the per-query pool
    /// (`es_tupleTable`) and return its id (C: the pointer). Fallible: the
    /// pool grows by `palloc` (OOM is `ereport(ERROR)` in C).
    ///
    /// The caller hands in the slot's shared header bits ([`TupleTableSlot`]);
    /// the pool now stores the payload-bearing [`SlotData`], so the header is
    /// wrapped in a [`SlotData::Virtual`] with empty per-attribute
    /// `tts_values`/`tts_isnull` arrays and a `None` descriptor — exactly the
    /// state a freshly-allocated virtual slot is in before any tuple is stored
    /// or descriptor assigned. (`ExecSetSlotDescriptor`/the store callbacks fill
    /// those payload fields.)
    pub fn make_slot(&mut self, slot: TupleTableSlot<'mcx>) -> PgResult<SlotId> {
        let mcx = *self.es_tupleTable.allocator();
        self.es_tupleTable
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<SlotData<'_>>()))?;
        let id = SlotId(self.es_tupleTable.len() as u32);
        // The incoming slot is the unified base (header + payload); wrap it in
        // the Virtual superstructure (its `data` materialization buffer empty).
        self.es_tupleTable.push(SlotData::Virtual(VirtualTupleTableSlot {
            base: slot,
            data: PgVec::new_in(mcx),
        }));
        Ok(id)
    }

    /// `ExecAllocTableSlot` over an already-built payload-bearing [`SlotData`]:
    /// append the live slot (the proper `Virtual/Heap/Minimal/BufferHeap`
    /// superstructure, with its descriptor + value arrays) to the per-query pool
    /// and return its id. This is the kind-aware path the slot-creation seams use
    /// (via `MakeTupleTableSlot`); [`make_slot`](Self::make_slot) is the
    /// header-only convenience that wraps a bare header in an empty virtual slot.
    pub fn push_slot_data(&mut self, slot: SlotData<'mcx>) -> PgResult<SlotId> {
        let mcx = *self.es_tupleTable.allocator();
        self.es_tupleTable
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<SlotData<'_>>()))?;
        let id = SlotId(self.es_tupleTable.len() as u32);
        self.es_tupleTable.push(slot);
        Ok(id)
    }

    /// Resolve a slot id to the live slot's shared header (C: dereference the
    /// pointer and read its base bits). The header projection bridges through
    /// [`SlotData::base`]; the payload-aware view is [`slot_data`](Self::slot_data).
    pub fn slot(&self, id: SlotId) -> &TupleTableSlot<'mcx> {
        self.es_tupleTable[id.0 as usize].base()
    }

    /// Resolve a slot id to its shared header mutably (C: dereference the
    /// pointer). Mutates only the header bits (`tts_flags`/`tts_tid`/…); the
    /// payload-aware mutable view is [`slot_data_mut`](Self::slot_data_mut).
    pub fn slot_mut(&mut self, id: SlotId) -> &mut TupleTableSlot<'mcx> {
        self.es_tupleTable[id.0 as usize].base_mut()
    }

    /// Resolve a slot id to the live payload-bearing [`SlotData`] (the owned
    /// `TupleTableSlot *` with `tts_values`/`tts_isnull`). This is the view the
    /// store/clear/copy callbacks and `dest_receive_slot`/`ExecFilterJunk` flow
    /// through.
    pub fn slot_data(&self, id: SlotId) -> &SlotData<'mcx> {
        &self.es_tupleTable[id.0 as usize]
    }

    /// Resolve a slot id to the live payload-bearing [`SlotData`] mutably.
    pub fn slot_data_mut(&mut self, id: SlotId) -> &mut SlotData<'mcx> {
        &mut self.es_tupleTable[id.0 as usize]
    }

    /// Two DISTINCT slots' shared headers mutably at once (e.g. copy one slot's
    /// tuple into another). Panics if `a == b` — the slots play distinct roles
    /// by construction in the C executor too.
    pub fn slot_pair_mut(
        &mut self,
        a: SlotId,
        b: SlotId,
    ) -> (&mut TupleTableSlot<'mcx>, &mut TupleTableSlot<'mcx>) {
        assert_ne!(a, b, "slot_pair_mut: the two slots must be distinct");
        let (ai, bi) = (a.0 as usize, b.0 as usize);
        if ai < bi {
            let (lo, hi) = self.es_tupleTable.split_at_mut(bi);
            (lo[ai].base_mut(), hi[0].base_mut())
        } else {
            let (lo, hi) = self.es_tupleTable.split_at_mut(ai);
            (hi[0].base_mut(), lo[bi].base_mut())
        }
    }

    /// Two DISTINCT slots' payload-bearing [`SlotData`] mutably at once (the
    /// `ExecCopySlot`-shaped payload flow). Panics if `a == b`.
    pub fn slot_data_pair_mut(
        &mut self,
        a: SlotId,
        b: SlotId,
    ) -> (&mut SlotData<'mcx>, &mut SlotData<'mcx>) {
        assert_ne!(a, b, "slot_data_pair_mut: the two slots must be distinct");
        let (ai, bi) = (a.0 as usize, b.0 as usize);
        if ai < bi {
            let (lo, hi) = self.es_tupleTable.split_at_mut(bi);
            (&mut lo[ai], &mut hi[0])
        } else {
            let (lo, hi) = self.es_tupleTable.split_at_mut(ai);
            (&mut hi[0], &mut lo[bi])
        }
    }

    /// Register an `ExprContext` in the pool, returning its id. Fallible:
    /// the pool grows by `palloc`.
    pub fn add_expr_context(&mut self, econtext: ExprContext<'mcx>) -> PgResult<EcxtId> {
        let mcx = self.es_query_cxt;
        self.es_exprcontexts
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<Option<ExprContext<'_>>>()))?;
        let id = EcxtId(self.es_exprcontexts.len() as u32);
        self.es_exprcontexts.push(Some(econtext));
        Ok(id)
    }

    /// Resolve an ExprContext id (C: dereference the pointer). Panics on a
    /// freed (tombstoned) context — the C analogue is a use-after-free.
    pub fn ecxt(&self, id: EcxtId) -> &ExprContext<'mcx> {
        self.es_exprcontexts[id.0 as usize]
            .as_ref()
            .expect("ExprContext used after FreeExprContext")
    }

    /// Resolve an ExprContext id mutably.
    pub fn ecxt_mut(&mut self, id: EcxtId) -> &mut ExprContext<'mcx> {
        self.es_exprcontexts[id.0 as usize]
            .as_mut()
            .expect("ExprContext used after FreeExprContext")
    }

    /// Add a `ResultRelInfo` to the pool, returning its id. Fallible: the
    /// pool grows by `palloc`.
    pub fn add_result_rel(&mut self, rri: ResultRelInfo<'mcx>) -> PgResult<RriId> {
        let mcx = self.es_query_cxt;
        self.es_result_rel_pool
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<ResultRelInfo<'_>>()))?;
        let id = RriId(self.es_result_rel_pool.len() as u32);
        self.es_result_rel_pool.push(rri);
        Ok(id)
    }

    /// Resolve a ResultRelInfo id (C: dereference the pointer).
    pub fn result_rel(&self, id: RriId) -> &ResultRelInfo<'mcx> {
        &self.es_result_rel_pool[id.0 as usize]
    }

    /// Resolve a ResultRelInfo id mutably.
    pub fn result_rel_mut(&mut self, id: RriId) -> &mut ResultRelInfo<'mcx> {
        &mut self.es_result_rel_pool[id.0 as usize]
    }
}

/// `EState *` back-link (the executor-state uplink stored in `PlanState.state`).
///
/// A `PlanState` points back at the *single* `EState` that owns its whole node
/// tree. Modelled as a **lifetime-free raw back-pointer** to the owned `EState`,
/// exactly the [`backend-utils-mctx`](mcx) child→parent uplink idiom (and the
/// `#225` `RelationRef` / `RelAlias` mechanism): no lifetime to infect
/// `PlanState`/`EState`, `Copy`, re-derive `&` per access, validity underwritten
/// by the invariant that the owned `EState` OUTLIVES — and, because it OWNS the
/// tree, never moves while linked — the `PlanState` tree it owns.
///
/// The wrapped pointer erases the EState's `'mcx` to `'static` (the link carries
/// only a raw address; it is never dereferenced as a stored tagged reference —
/// `get`/`get_mut` re-derive a fresh `&` at a caller-chosen lifetime per
/// access). This mirrors the `mcx` parent uplink and the `RelAlias` raw
/// back-pointer.
#[derive(Clone, Copy, Debug)]
pub struct EStateLink(core::ptr::NonNull<EStateData<'static>>);

impl EStateLink {
    /// Wrap the stable address of the owned `EState` as a back-link. The caller
    /// must guarantee the `EState` outlives every `PlanState` carrying the link
    /// (it does: the `EState` owns the whole `PlanState` tree); see the type
    /// docs.
    #[inline]
    pub fn new(p: core::ptr::NonNull<EStateData<'static>>) -> Self {
        EStateLink(p)
    }

    /// Wrap an owned `EState` borrow as a back-link (the usual construction:
    /// `EStateLink::from_ref(&estate)` while filling a `PlanState`). The `'mcx`
    /// is erased into the raw address; see the type docs.
    #[inline]
    pub fn from_ref<'mcx>(estate: &EStateData<'mcx>) -> Self {
        EStateLink(core::ptr::NonNull::from(estate).cast())
    }

    /// Momentary shared read of the owned `EState` through the back-link — the
    /// single audited deref of the raw uplink (mirrors the `mcx` parent and
    /// `RelAlias::get`). Re-derives the `&` per access at the caller-chosen
    /// lifetime; never stores a stale reference.
    #[allow(unsafe_code)]
    #[inline]
    pub fn get<'a>(&self) -> &'a EStateData<'a> {
        // Re-derive a fresh, untagged `NonNull` from the raw address so this
        // deref's provenance is current (a once-captured `&`-tag would be
        // revoked by an intervening `&mut` to the owned `EState`); never deref
        // the stored `self.0` directly. Mirrors `RelAlias::get` exactly.
        // SAFETY: `self.0` is non-null (newtype invariant), so `new_unchecked`
        // is valid.
        let fresh =
            unsafe { core::ptr::NonNull::new_unchecked(self.0.as_ptr() as *mut EStateData<'a>) };
        debug_assert_eq!(
            fresh.as_ptr() as *mut (),
            self.0.as_ptr() as *mut (),
            "owned EState moved under EStateLink"
        );
        // SAFETY: the uplink is set only to the single owned `EState` (created in
        // the executor start and held by the executor guard / `QueryDesc`) that,
        // by construction, OWNS — and therefore outlives + never moves while
        // linked — the `PlanState` tree carrying this link. The cross-struct
        // reference points from the shorter-lived `PlanState` to the
        // longer-lived owning `EState`, exactly the verified `backend-utils-mctx`
        // parent-outlives-child invariant and the `#225` `RelationRef`
        // mechanism. `fresh` is re-derived this call from the raw address (not a
        // stored stale-tag pointer), so its provenance is current and the deref
        // is momentary.
        unsafe { fresh.as_ref() }
    }

    /// Momentary mutable read of the owned `EState` through the back-link.
    /// Same audited-deref obligations as [`get`](Self::get); the caller must hold
    /// no other live borrow of the `EState` for the duration (it is the sole
    /// owned executor state, so the executor threads `&mut EState` explicitly and
    /// uses this only where it does not already hold one).
    #[allow(unsafe_code)]
    #[inline]
    pub fn get_mut<'a>(&mut self) -> &'a mut EStateData<'a> {
        // Re-derive a fresh, untagged `NonNull` from the raw address per access
        // (mirrors `get` / `RelAlias::get`); never deref the stored `self.0`.
        // SAFETY: `self.0` is non-null (newtype invariant), so `new_unchecked`
        // is valid.
        let mut fresh =
            unsafe { core::ptr::NonNull::new_unchecked(self.0.as_ptr() as *mut EStateData<'a>) };
        debug_assert_eq!(
            fresh.as_ptr() as *mut (),
            self.0.as_ptr() as *mut (),
            "owned EState moved under EStateLink"
        );
        // SAFETY: see `get`. The uplink targets the single owned `EState` that
        // outlives + never moves while linked; `fresh` is re-derived this call
        // from the raw address (not a stored stale-tag pointer).
        unsafe { fresh.as_mut() }
    }

    /// Raw escape hatch (the bare `EState *` the C executor holds), for the rare
    /// spot where tying the borrow to `&self` is too restrictive. The caller
    /// takes on the liveness obligation [`get`](Self::get) discharges.
    #[inline]
    pub fn as_ptr(&self) -> *mut EStateData<'static> {
        self.0.as_ptr()
    }
}
