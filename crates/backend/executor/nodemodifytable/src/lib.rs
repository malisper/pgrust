// nodeModifyTable.c, single-relation INSERT/UPDATE/DELETE arms. The subplan
// stays with the ExecProcNode dispatcher (execmain owns the node enum;
// nodesort precedent) — exec_modify_table takes fetch and EvalPlanQual
// closures. AFTER ROW triggers queue via the trigger crate (RI lane);
// BEFORE/INSTEAD/statement triggers, MERGE, ON CONFLICT and FDW batching are
// loud named panics; RETURNING supplies OLD/NEW rows per C ExecProcessReturning
// (all-NULL substitutes when the row doesn't exist).
#![allow(non_snake_case)]

use std::rc::Rc;

use datum::Datum;
use execexpr::{exec_build_projection_info, EvalSlots, ExprState};
use executils::{EStateData, ExecSlotId};
use mcx::PgBox;
use tableam_vocab::{
    LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result, TU_UpdateIndexes,
    TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
};
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_CHECK_VIOLATION,
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_NOT_NULL_VIOLATION,
    ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION, ERRCODE_T_R_SERIALIZATION_FAILURE,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::WCOKind;
use types_nodes::plannodes::ModifyTable;
use types_nodes::{Node, NodeTag};
use types_rel::{Relation, RELKIND_RELATION};
use types_slot::{SlotData, TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};
use types_snapshot::{SnapshotData, SNAPSHOT_ANY};
use types_tuple::itemptr::ItemPointerSetInvalid;
use types_tuple::{ItemPointerData, TupleDescData};

// ExecBuildUpdateProjection's step stream, resolved once per statement onto a
// flat per-target-column source map (rule 4: known-set dispatch, no ExprState).
#[derive(Clone, Copy)]
enum NewColSrc {
    Outer(u16),
    Old(u16),
    NullDropped,
}

// The per-result-relation half of C's ResultRelInfo: one per (unpruned)
// entry of node->resultRelations, plus a separate root entry for
// inherited/partitioned targets (node->rootRelation). Everything lazily
// built per relation in C (projections, trigger caches, constraint exprs)
// lives here so the tableoid dispatch can switch relations mid-scan.
pub struct ResultRelExec<'mcx> {
    pub rti: u32,
    // RelationGetRelid, snapshotted at init for the tableoid junk-attr
    // lookup without re-borrowing estate.es_relations.
    rd_id: Oid,
    relkind: u8,
    ri_newTupleSlot: Option<ExecSlotId>,
    ri_oldTupleSlot: Option<ExecSlotId>,
    // ri_ReturningSlot: the DELETE ... RETURNING old-tuple slot.
    ri_ReturningSlot: Option<ExecSlotId>,
    // C ri_AllNullSlot: all-NULL OLD/NEW source when that row doesn't exist.
    ri_AllNullSlot: Option<ExecSlotId>,
    ri_projectNewInfoValid: bool,
    ri_RowIdAttNo: i16,
    update_cols: mcx::PgVec<'mcx, NewColSrc>,
    // The per-rel updateColnos list (planner adjusted attnums per child);
    // resolved into update_cols by exec_init_update_projection.
    update_colnos: Option<&'mcx types_nodes::IntList<'mcx>>,
    indexes: Option<execindexing::ResultRelIndexState<'mcx>>,
    project_returning: Option<PgBox<'mcx, ExprState<'mcx>>>,
    // ri_CheckConstraintExprs (built on first ExecRelCheck, per C); each
    // compiled qual rides with its constraint name for the 23514 report.
    check_exprs: Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>,
    // ri_PartitionCheckExpr (built on first ExecPartitionCheck, per C).
    partition_check: Option<PgBox<'mcx, ExprState<'mcx>>>,
    // ri_WithCheckOptions + ri_WithCheckOptionExprs, flattened.
    wco_exprs: mcx::PgVec<'mcx, WcoExpr<'mcx>>,
    // C ri_TrigDesc; Rc clone of the relcache entry's desc (CopyTriggerDesc).
    trigdesc: Option<Rc<types_trigger::TriggerDesc<'static>>>,
    // C ri_TrigFunctions + ExecGetTriggerOldSlot.
    trig_fmgr: ::trigger::TriggerFmgrCache,
    trig_old_slot: Option<ExecSlotId>,
    // C ri_TrigWhenExprs.
    trig_when: ::trigger::TriggerWhenCache<'mcx>,
    // ExecGetAllUpdatedCols, resolved once (C caches in ri_all_updated_cols).
    all_updated_cols: Option<types_nodes::Bitmapset<'mcx>>,
    // C ri_ChildToRootMap + ri_ChildToRootMapValid (ExecGetChildToRootMap):
    // outer None = unresolved, inner None = no conversion needed.
    child_to_root: Option<Option<mcx::PgVec<'mcx, i16>>>,
    // ri_GeneratedExprsI/U collapsed to one set: the UPDATE updatedCols skip
    // is perf-only (values are immutable functions of non-generated columns).
    generated_exprs: Option<mcx::PgVec<'mcx, GeneratedExpr<'mcx>>>,
    // ri_GenVirtualNotNullConstraintExprs.
    virtual_nn_exprs: Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>,
    // ri_MergeActions + per-rel merge slots (ExecInitMerge).
    merge: Option<MergeState<'mcx>>,
}

pub struct ModifyTableState<'mcx> {
    pub plan: &'mcx ModifyTable<'mcx>,
    pub operation: CmdType,
    pub canSetTag: bool,
    pub mt_done: bool,
    fireBSTriggers: bool,
    // The unpruned result relations (C mtstate->resultRelInfo[0..mt_nrels]).
    // Arena-backed (es_query_cxt) like every sibling collection: the whole
    // planstate bundle is FORGOTTEN at standard_executor_end, so a std-heap
    // buffer here leaked once per write statement (832 B — the OLTP read-write bank's
    // eviction driver, notes/oltp-memory-footprint-2026-07-12.md).
    rels: mcx::PgVec<'mcx, ResultRelExec<'mcx>>,
    // C mtstate->rootResultRelInfo when node->rootRelation > 0 (inherited or
    // partitioned target); None means the root is rels[0].
    root: Option<ResultRelExec<'mcx>>,
    // mt_lastResultIndex + mt_lastResultOid: one-element dispatch cache.
    cur: usize,
    // MERGE ... WHEN NOT MATCHED INSERT over an inherited/partitioned target
    // inserts via the root (C passes rootResultRelInfo to ExecInsert);
    // rel()/rel_mut() honor this so the whole insert path targets the root.
    insert_target_root: bool,
    last_result_oid: Oid,
    // mt_resultOidAttno: the "tableoid" junk attr when total_nrels > 1.
    result_oid_attno: i16,
    // C's per-tuple econtext for index expression/predicate eval, reset per
    // outer row; node-owned because estate can't lend its per-tuple mcx while
    // relation/slot field borrows are live. Option: dropped in
    // exec_end_modify_table (the node struct is forgotten, never dropped).
    index_eval_cx: Option<mcx::MemoryContext>,
    snapshot_any: Option<Rc<SnapshotData<'mcx>>>,
    // The shared RETURNING result slot (C ps_ResultTupleSlot): all result
    // rels project into one slot over the node targetlist's descriptor.
    returning_slot: Option<ExecSlotId>,
    // C ps_ExprContext: SubPlans in RETURNING / ON CONFLICT DO UPDATE /
    // MERGE actions evaluate against it; reset per row.
    node_ecxt: Option<executils::EcxtId>,
    // C mt_root_tuple_slot: root-format staging slot for a cross-partition
    // UPDATE's re-routed tuple.
    cross_part_root_slot: Option<ExecSlotId>,
    /// This node's C EPQState.relsubs_* (execmain swaps them live per run).
    pub epq_subs: Option<executils::EpqSubs<'mcx>>,
    // C EvalPlanQualInit's arowMarks: (rti, junk-attno fetch spec) per
    // non-locking source-rel PlanRowMark.
    epq_arowmarks: mcx::PgVec<'mcx, (u32, executils::EpqRowMarkFetch)>,
    // C EvalPlanQualSetSlot: the outer plan row currently being processed.
    epq_origslot: Option<ExecSlotId>,
    on_conflict: Option<OnConflictState<'mcx>>,
    // ON CONFLICT DO UPDATE's locked pre-update row, carried to the INSERT
    // arm's RETURNING (C processes it inside ExecUpdate instead).
    oc_old_slot: Option<ExecSlotId>,
    // mt_transition_capture + mt_oc_transition_capture.
    transition_capture: Option<::trigger::TransitionCaptureState>,
    oc_transition_capture: Option<::trigger::TransitionCaptureState>,
    // Partitioned-target INSERT routing (execPartition.c); per-leaf insert
    // state is indexed by the router's leaf index. The leaf_* collections are
    // arena-backed for the same forget-at-end reason as `rels` above (their
    // std forms leaked their buffers per routed INSERT statement).
    router: Option<execpartition::PartitionTupleRouting<'mcx>>,
    leaf_indexes: mcx::PgVec<'mcx, Option<execindexing::ResultRelIndexState<'mcx>>>,
    leaf_checks: mcx::PgVec<'mcx, Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>>,
    leaf_virtual_nn: mcx::PgVec<'mcx, Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>>,
    // ri_GeneratedExprsI per leaf: partitions may override generation exprs.
    leaf_generated: mcx::PgVec<'mcx, Option<mcx::PgVec<'mcx, GeneratedExpr<'mcx>>>>,
    // ri_PartitionTupleSlot per remapped leaf (estate slot, leaf layout) and
    // the leaf's ri_PartitionCheckExpr.
    leaf_slots: mcx::PgVec<'mcx, Option<ExecSlotId>>,
    leaf_partition_check: mcx::PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
    // C ExecInitPartitionInfo's ri_onConflictArbiterIndexes: the root arbiter
    // index OIDs mapped to this leaf's own index children.
    leaf_arbiters: mcx::PgVec<'mcx, Option<mcx::PgVec<'mcx, Oid>>>,
    // C ExecInitPartitionInfo's per-leaf oc_Existing (table_slot_create on the
    // leaf rel); the root's existing slot is Virtual when the target is
    // partitioned and cannot feed the heap AM lock/fetch callbacks.
    leaf_existing: mcx::PgVec<'mcx, Option<ExecSlotId>>,
    // C ri_ChildToRootMap per routed leaf (ExecGetChildToRootMap); outer
    // Option = resolved yet, inner None = no conversion needed.
    leaf_child_to_root: mcx::PgVec<'mcx, Option<Option<mcx::PgVec<'mcx, i16>>>>,
    // C ExecInitPartitionInfo's per-leaf ri_WithCheckOptions: the first WCO
    // list translated to the leaf's attnos via map_variable_attnos.
    leaf_wco: mcx::PgVec<'mcx, Option<mcx::PgVec<'mcx, WcoExpr<'mcx>>>>,
    // C ExecInitPartitionInfo's per-leaf OnConflictSetState (map != NULL leg,
    // execPartition.c:781-864): only built for attno-remapped leaves; other
    // leaves reuse the root's DO UPDATE state as C does.
    leaf_on_conflict: mcx::PgVec<'mcx, Option<LeafOnConflict<'mcx>>>,
    // C ExecInitPartitionInfo's per-leaf ri_projectReturning: the first
    // returningList translated to the leaf's attnos; only built (and used)
    // for attno-remapped leaves — a non-remapped leaf's projection is
    // identical to the root's.
    leaf_returning: mcx::PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
    // C ExecInitPartitionInfo's CheckValidResultRel: once per routed leaf.
    leaf_ri_checked: mcx::PgVec<'mcx, bool>,
    // Routed-leaf trigger state (C: per-partition ResultRelInfo trigger
    // fields); outer Option = resolved yet. Elements hold non-arena state
    // (Rc trigdesc, std fmgr/when caches): exec_end_modify_table clears them.
    leaf_trigdesc: mcx::PgVec<'mcx, Option<Option<Rc<types_trigger::TriggerDesc<'static>>>>>,
    leaf_trig_fmgr: mcx::PgVec<'mcx, ::trigger::TriggerFmgrCache>,
    leaf_trig_when: mcx::PgVec<'mcx, ::trigger::TriggerWhenCache<'mcx>>,
    // C ExecInsert's *insert_destrel out-param (routed leaf of the last
    // insert; None = unrouted), for the cross-partition FK update event.
    last_insert_leaf: Option<usize>,
    // The remapped leaf's work slot when the last insert converted into a
    // leaf layout (ri_PartitionTupleSlot); RETURNING evaluates the leaf
    // projection over it (C ExecInsert projects on the routed leaf).
    last_insert_remapped: Option<ExecSlotId>,
    // Set with oc_old_slot when ON CONFLICT DO UPDATE modified a remapped
    // leaf: its existing/proj slots are leaf-format, so RETURNING runs the
    // leaf-translated projection.
    oc_returning_leaf: Option<usize>,
    // C mt_merge_action's commandType: the WHEN action being executed
    // (routed-INSERT WCO kind selection, nodeModifyTable.c:1079-1081).
    merge_active_cmd: Option<CmdType>,
    // mt_merge_pending_not_matched: a concurrent update flipped a MATCHED row
    // after a NOT MATCHED BY SOURCE action already produced the RETURNING
    // row; the NOT MATCHED [BY TARGET] half runs on the next call.
    mt_merge_pending_not_matched: Option<ExecSlotId>,
    // outerPlanState(mtstate)->instrument: the EPQ MATCHED -> NOT MATCHED
    // list switch counts the source row twice (InstrUpdateTupleCount).
    outer_instr_idx: Option<u32>,
    // mt_merge_inserted/updated/deleted (EXPLAIN ANALYZE's Tuples: line;
    // skipped is derived by explain as source-total minus these).
    pub mt_merge_inserted: f64,
    pub mt_merge_updated: f64,
    pub mt_merge_deleted: f64,
}

impl<'mcx> ModifyTableState<'mcx> {
    // The dispatch-current result relation (C: resultRelInfo cursor preloaded
    // from mt_lastResultIndex).
    // inline(always): trivial accessor on the per-row insert path — the
    // plain hint lost to the two-monomorphization caller context after the
    // wave-2 seam split (se2-cost-fix round 2; +13 instr/row outlined).
    #[inline(always)]
    fn rel(&self) -> &ResultRelExec<'mcx> {
        if self.insert_target_root {
            return self.root_rel();
        }
        &self.rels[self.cur]
    }

    #[inline]
    fn rel_mut(&mut self) -> &mut ResultRelExec<'mcx> {
        if self.insert_target_root {
            return self.root_rel_mut();
        }
        &mut self.rels[self.cur]
    }

    // getTargetResultRelInfo: the root result rel (statement triggers,
    // transition tuple format, INSERT tuple routing).
    #[inline]
    fn root_rel(&self) -> &ResultRelExec<'mcx> {
        self.root.as_ref().unwrap_or(&self.rels[0])
    }

    #[inline]
    fn root_rel_mut(&mut self) -> &mut ResultRelExec<'mcx> {
        self.root.as_mut().unwrap_or(&mut self.rels[0])
    }

    // ExecLookupResultRelByOid with update_cache=true; linear search (C uses
    // a hash above 64 rels — a perf shortcut only, same result).
    fn lookup_result_rel_by_oid(&mut self, resultoid: Oid) -> PgResult<()> {
        for ndx in 0..self.rels.len() {
            if self.rels[ndx].rd_id == resultoid {
                self.last_result_oid = resultoid;
                self.cur = ndx;
                return Ok(());
            }
        }
        Err(Box::new(PgError::error(format!(
            "incorrect result relation OID {resultoid}"
        ))))
    }
}

// ExecInitMerge's per-statement state: ri_MergeActions split by match kind
// plus ri_MergeJoinCondition (non-NULL only with BY SOURCE actions; rechecked
// above the join to split MATCHED from NOT MATCHED BY SOURCE).
struct MergeState<'mcx> {
    matched_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>>,
    not_matched_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>>,
    not_matched_by_source_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>>,
    join_condition: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

// MergeActionState: INSERT carries a full-tuple projection; UPDATE the
// two-step SET projection of ExecBuildUpdateProjection (setvals + overlay at
// set_attnos), the ON CONFLICT DO UPDATE shape.
struct MergeActionExec<'mcx> {
    command_type: CmdType,
    when_qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    proj: Option<PgBox<'mcx, ExprState<'mcx>>>,
    setvals_slot: Option<ExecSlotId>,
    set_attnos: mcx::PgVec<'mcx, u16>,
}

pub struct GeneratedExpr<'mcx> {
    attnum: usize,
    state: PgBox<'mcx, ExprState<'mcx>>,
}

pub struct VirtualNnExpr<'mcx> {
    attnum: usize,
    state: PgBox<'mcx, ExprState<'mcx>>,
}

// ri_onConflict (OnConflictSetState) + ri_onConflictArbiterIndexes. The DO
// UPDATE projection runs in two steps: set_proj evaluates the SET exprs
// (scan = existing tuple, inner = excluded) into setvals_slot, then the merge
// into proj_slot overlays them onto the existing tuple at set_attnos — the
// flat-map shape of C's ExecBuildUpdateProjection.
struct OnConflictState<'mcx> {
    arbiters: mcx::PgVec<'mcx, types_core::Oid>,
    existing_slot: ExecSlotId,
    setvals_slot: Option<ExecSlotId>,
    proj_slot: Option<ExecSlotId>,
    set_proj: Option<PgBox<'mcx, ExprState<'mcx>>>,
    set_attnos: mcx::PgVec<'mcx, u16>,
    where_clause: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

// The remapped-leaf half of C's per-partition OnConflictSetState
// (ExecInitPartitionInfo, execPartition.c:781-864): SET/WHERE recompiled with
// EXCLUDED (INNER_VAR) and target (firstVarno) Vars mapped to the leaf's
// attnos, the SET target colnos adjusted through the same map, and the
// projection slots rebuilt over the leaf descriptor.
struct LeafOnConflict<'mcx> {
    setvals_slot: ExecSlotId,
    proj_slot: ExecSlotId,
    set_proj: PgBox<'mcx, ExprState<'mcx>>,
    set_attnos: mcx::PgVec<'mcx, u16>,
    where_clause: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

pub struct CheckExpr<'mcx> {
    name: mcx::PgString<'mcx>,
    state: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

struct WcoExpr<'mcx> {
    kind: WCOKind,
    relname: &'mcx str,
    polname: Option<&'mcx str>,
    state: PgBox<'mcx, ExprState<'mcx>>,
}

/// `ExecInitModifyTable` (nodeModifyTable.c); the caller inits the subplan
/// and, when RETURNING is present, passes the result descriptor built from
/// the node's targetlist (C's ExecInitResultTupleSlotTL).
pub fn exec_init_modify_table<'mcx>(
    node: &'mcx ModifyTable<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    returning_desc: Option<Rc<TupleDescData<'mcx>>>,
) -> PgResult<ModifyTableState<'mcx>> {
    assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    if !matches!(
        node.operation,
        CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
    ) {
        panic!(
            "ExecInitModifyTable (nodeModifyTable.c): {:?} arm not ported",
            node.operation
        );
    }
    if !node.fdwPrivLists.is_nil() {
        panic!("ExecInitModifyTable (nodeModifyTable.c): FDW lists not ported");
    }
    // C's arowmarks loop: resolve each non-parent PlanRowMark's junk attnos
    // against the subplan targetlist (ExecFindRowMark + ExecBuildAuxRowMark);
    // the EPQ recheck re-fetches these source rows instead of rescanning.
    let mut epq_arowmarks: mcx::PgVec<'mcx, (u32, executils::EpqRowMarkFetch)> =
        mcx::PgVec::new_in(estate.es_query_cxt);
    if !node.rowMarks.is_nil() {
        let outer_tlist = &node
            .plan
            .lefttree
            .expect("ModifyTable has a subplan")
            .as_plan()
            .expect("subplan is a Plan")
            .targetlist;
        for rc_node in &node.rowMarks {
            let rc = rc_node.as_plan_row_mark().expect("rowMarks cell is a PlanRowMark");
            if rc.isParent {
                continue;
            }
            let rte = estate.exec_rt_fetch(rc.rti);
            if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
                && !estate.es_unpruned_relids.is_member(rc.rti as i32)
            {
                continue;
            }
            let erm = estate.es_rowmarks[(rc.rti - 1) as usize]
                .expect("InitPlan built the ExecRowMark for every PlanRowMark rti");
            use types_nodes::plannodes::RowMarkType;
            // Inherited source marks (erm.rti != erm.prti) flow through the
            // same wholerow/ctid fetch arms: the junk column is emitted per
            // child rti and looked up by rowmarkId below, matching C's
            // EvalPlanQualFetchRowMark which never branches on prti. Verified
            // byte-identical to C on merge.sql's inheritance MERGE (release
            // regress 230/230); a prior debug_assert here mis-fired on that
            // legitimate case.
            let fetch = if erm.markType == RowMarkType::ROW_MARK_COPY {
                let name = format!("wholerow{}", erm.rowmarkId);
                let n = exec_find_junk_attribute_in_tlist(outer_tlist, &name);
                assert!(n != 0, "could not find junk {name} column");
                executils::EpqRowMarkFetch::Copy { whole_attno: n }
            } else {
                assert!(
                    erm.markType == RowMarkType::ROW_MARK_REFERENCE,
                    "ExecInitModifyTable: locking rowmark under ModifyTable"
                );
                let name = format!("ctid{}", erm.rowmarkId);
                let n = exec_find_junk_attribute_in_tlist(outer_tlist, &name);
                assert!(n != 0, "could not find junk {name} column");
                executils::EpqRowMarkFetch::Reference { ctid_attno: n }
            };
            epq_arowmarks.push((rc.rti, fetch));
        }
    }
    let total_nrels = node.resultRelations.len();

    // The unpruned-filter loop (C 4670-4726): keep each unpruned rti with its
    // original list position (the per-rel lists index by it); if every result
    // relation was pruned, keep the first so MERGE NOT MATCHED actions and
    // statement triggers still have a relation.
    let mut kept: Vec<(u32, usize)> = Vec::with_capacity(total_nrels);
    for (i, rti) in node.resultRelations.iter().enumerate() {
        let rti = rti as u32;
        if estate.es_unpruned_relids.is_member(rti as i32) {
            kept.push((rti, i));
        } else if i == total_nrels - 1 && kept.is_empty() {
            kept.push((node.resultRelations.nth(0) as u32, 0));
        }
    }
    let nrels = kept.len();
    assert!(nrels > 0);

    // Resolve the target (root) relation: with an inherited/partitioned
    // target its RT index is node.rootRelation and it gets its own entry;
    // otherwise the sole result relation is the root.
    let mut root = None;
    if node.rootRelation > 0 {
        debug_assert!(estate.es_unpruned_relids.is_member(node.rootRelation as i32));
        root = Some(init_result_rel(node, estate, node.rootRelation as u32, None, None)?);
    } else {
        assert_eq!(total_nrels, 1);
    }

    let mut rels: mcx::PgVec<'mcx, ResultRelExec<'mcx>> =
        mcx::PgVec::with_capacity_in(nrels, estate.es_query_cxt);
    for &(rti, i) in &kept {
        rels.push(init_result_rel(
            node,
            estate,
            rti,
            Some(i),
            (node.rootRelation > 0).then_some(node.rootRelation as u32),
        )?);
    }

    // ExecSetupTransitionCaptureState (skipped in explain-only mode); the
    // capture target is the root relation.
    let mut transition_capture = None;
    let mut oc_transition_capture = None;
    {
        let target = root.as_ref().unwrap_or(&rels[0]);
        if estate.es_top_eflags & types_slot::EXEC_FLAG_EXPLAIN_ONLY == 0 {
            if let Some(td) = &target.trigdesc {
                transition_capture =
                    ::trigger::MakeTransitionCaptureState(td, target.rd_id, node.operation)?;
                if node.operation == CmdType::CMD_INSERT
                    && node.onConflictAction
                        == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32
                {
                    oc_transition_capture =
                        ::trigger::MakeTransitionCaptureState(td, target.rd_id, CmdType::CMD_UPDATE)?;
                }
            }
        }
    }

    // ExecInitMerge root-INSERT half: NOT MATCHED INSERTs over an inherited/
    // partitioned target insert via the root, projecting into a root-format
    // slot (C tgtslot: rootRelInfo->ri_newTupleSlot / mt_root_tuple_slot).
    if node.operation == CmdType::CMD_MERGE {
        if let Some(root_exec) = root.as_mut() {
            let has_insert = rels.iter().any(|r| {
                r.merge.as_ref().is_some_and(|m| {
                    m.not_matched_actions.iter().any(|a| a.command_type == CmdType::CMD_INSERT)
                })
            });
            if has_insert {
                // RETURNING over root INSERTs: exec_merge_not_matched builds
                // the root projection lazily (exec_init_root_returning, the
                // C 3844-3947 rootRelInfo leg in root coordinates).
                let mcx = estate.es_query_cxt;
                // C 3867-3910: the root's WITH CHECK OPTIONs, mapped from the
                // first result rel's list to root attnos.
                if !node.withCheckOptionLists.is_nil() {
                    let first_rti = rels[0].rti;
                    let (root_reltype, attmap) = {
                        let EStateData { es_relations, .. } = &*estate;
                        let first = es_relations[(first_rti - 1) as usize]
                            .as_ref()
                            .expect("result relation opened");
                        let root_rel = es_relations[(root_exec.rti - 1) as usize]
                            .as_ref()
                            .expect("root relation opened");
                        (
                            root_rel.rd_rel.reltype,
                            tupdesc::build_attrmap_by_name_if_req(
                                mcx,
                                &root_rel.rd_att,
                                &first.rd_att,
                                false,
                            )?,
                        )
                    };
                    let wlist = node
                        .withCheckOptionLists
                        .nth(0)
                        .as_list()
                        .expect("withCheckOptionLists cell is a List");
                    let params = estate.param_bind();
                    for wco_node in wlist {
                        let wco_node = match &attmap {
                            None => wco_node,
                            Some(map) => {
                                rewrite_manip::map_variable_attnos(
                                    mcx,
                                    wco_node,
                                    first_rti as i32,
                                    0,
                                    map,
                                    root_reltype,
                                )?
                                .0
                            }
                        };
                        let wco = wco_node.as_with_check_option().expect("WCO cell");
                        let qual = wco
                            .qual
                            .expect("planned WCO has a qual")
                            .as_list()
                            .expect("WCO qual is an implicit-AND List after preprocessing");
                        let state = executils::with_subplan_compile_env(estate, |env| {
                            execexpr::exec_init_qual_subplans(mcx, qual, params, env)
                        })?
                        .expect("planner dropped constant-true WCO quals");
                        root_exec.wco_exprs.push(WcoExpr {
                            kind: wco.kind,
                            relname: wco.relname.expect("WCO relname"),
                            polname: wco.polname,
                            state,
                        });
                    }
                }
                let (kind, desc) = {
                    let rel = estate.es_relations[(root_exec.rti - 1) as usize]
                        .as_ref()
                        .expect("root relation opened");
                    (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
                };
                let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
                let id = ExecSlotId(estate.es_tupleTable.len() as u32);
                estate.es_tupleTable.push(slot);
                root_exec.ri_newTupleSlot = Some(id);
            }
        }
    }

    // Child-format tuples are converted to the root format for transition
    // tables per C's ri_ChildToRootMap; the maps resolve lazily at the AR
    // trigger sites (ensure_child_to_root / ensure_leaf_child_to_root).

    // mt_resultOidAttno: the inherited/partitioned-target dispatch column.
    let subplan_tlist = &node
        .plan
        .lefttree
        .expect("ModifyTable has a subplan")
        .as_plan()
        .expect("plan node")
        .targetlist;
    let result_oid_attno = exec_find_junk_attribute_in_tlist(subplan_tlist, "tableoid");
    assert!(result_oid_attno > 0 || total_nrels == 1);

    // The shared RETURNING result slot, virtual over the caller-built
    // descriptor (C ExecInitResultTupleSlotTL over the node targetlist).
    let mut returning_slot = None;
    let mut node_ecxt = None;
    if !node.returningLists.is_nil() {
        let desc = returning_desc.expect("caller passes the RETURNING result descriptor");
        returning_slot =
            Some(estate.exec_init_extra_tuple_slot(Some(desc), TupleSlotKind::Virtual));
    }
    // C creates ps_ExprContext lazily for RETURNING / ON CONFLICT UPDATE /
    // MERGE (the consumers of SubPlan-bearing node expressions).
    if !node.returningLists.is_nil()
        || node.onConflictAction == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32
        || !node.mergeActionLists.is_nil()
        || !node.withCheckOptionLists.is_nil()
    {
        node_ecxt = Some(estate.create_expr_context());
    }
    let rti = rels[0].rti;

    // ExecInitModifyTable's ON CONFLICT block. Slots live in the shared tuple
    // table; the SET projection's input descriptor is the result relation's.
    let mut on_conflict = None;
    if node.onConflictAction != 0 {
        let mcx = estate.es_query_cxt;
        let mut arbiters: mcx::PgVec<'mcx, types_core::Oid> = mcx::PgVec::new_in(mcx);
        for oid in node.arbiterIndexes.iter() {
            arbiters.push(oid);
        }
        let (kind, desc) = {
            let rel = estate.es_relations[(rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        let existing_slot = {
            let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc.clone()));
            let id = ExecSlotId(estate.es_tupleTable.len() as u32);
            estate.es_tupleTable.push(slot);
            id
        };

        let mut setvals_slot = None;
        let mut proj_slot = None;
        let mut set_proj = None;
        let mut set_attnos: mcx::PgVec<'mcx, u16> = mcx::PgVec::new_in(mcx);
        let mut where_clause = None;
        if node.onConflictAction == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32 {
            let params = estate.param_bind();
            let proj = {
                let desc = estate.es_relations[(rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened")
                    .rd_att
                    .clone();
                executils::with_subplan_compile_env(estate, |env| {
                    execexpr::exec_build_projection_info_subplans(
                        mcx,
                        &node.onConflictSet,
                        Some(&desc),
                        params,
                        env,
                    )
                })?
            };
            let set_desc = execscan::exec_type_from_tl(mcx, &node.onConflictSet)?;
            setvals_slot = Some({
                let slot = exectuples::make_tuple_table_slot(
                    mcx,
                    TupleSlotKind::Virtual,
                    Some(set_desc),
                );
                let id = ExecSlotId(estate.es_tupleTable.len() as u32);
                estate.es_tupleTable.push(slot);
                id
            });
            proj_slot = Some({
                let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
                let id = ExecSlotId(estate.es_tupleTable.len() as u32);
                estate.es_tupleTable.push(slot);
                id
            });
            set_proj = Some(proj);
            for attno in node.onConflictCols.iter() {
                set_attnos.push(attno as u16);
            }
            // ExecBuildUpdateProjection (execExpr.c:580-601): junk entries
            // (MULTIEXPR SubPlans) follow the SET columns and are never
            // assigned; onConflictCols covers only the non-junk columns.
            let mut non_junk = 0usize;
            let mut seen_junk = false;
            for tle_node in &node.onConflictSet {
                let tle = tle_node.as_target_entry().expect("TargetEntry");
                if tle.resjunk {
                    seen_junk = true;
                } else {
                    assert!(!seen_junk, "onConflictSet tlist: junk before a SET column");
                    non_junk += 1;
                }
            }
            assert_eq!(set_attnos.len(), non_junk);
            if let Some(where_node) = node.onConflictWhere {
                let qual = where_node
                    .as_list()
                    .expect("onConflictWhere is an implicit-AND List after preprocessing");
                let params = estate.param_bind();
                where_clause = executils::with_subplan_compile_env(estate, |env| {
                    execexpr::exec_init_qual_subplans(mcx, qual, params, env)
                })?;
            }
        }
        on_conflict = Some(OnConflictState {
            arbiters,
            existing_slot,
            setvals_slot,
            proj_slot,
            set_proj,
            set_attnos,
            where_clause,
        });
    }

    let _ = rti;

    let qcx = estate.es_query_cxt;
    Ok(ModifyTableState {
        plan: node,
        operation: node.operation,
        canSetTag: node.canSetTag,
        mt_done: false,
        fireBSTriggers: true,
        epq_subs: None,
        epq_arowmarks,
        epq_origslot: None,
        rels,
        root,
        cur: 0,
        insert_target_root: false,
        cross_part_root_slot: None,
        last_result_oid: 0,
        result_oid_attno,
        index_eval_cx: Some(mcx::MemoryContext::new_bump("IndexEvalPerTuple")),
        snapshot_any: Some(Rc::new(SnapshotData::sentinel(estate.es_query_cxt, SNAPSHOT_ANY))),
        returning_slot,
        node_ecxt,
        on_conflict,
        oc_old_slot: None,
        transition_capture,
        oc_transition_capture,
        router: None,
        leaf_indexes: mcx::PgVec::new_in(qcx),
        leaf_checks: mcx::PgVec::new_in(qcx),
        leaf_virtual_nn: mcx::PgVec::new_in(qcx),
        leaf_generated: mcx::PgVec::new_in(qcx),
        leaf_slots: mcx::PgVec::new_in(qcx),
        leaf_partition_check: mcx::PgVec::new_in(qcx),
        leaf_arbiters: mcx::PgVec::new_in(qcx),
        leaf_existing: mcx::PgVec::new_in(qcx),
        leaf_child_to_root: mcx::PgVec::new_in(qcx),
        leaf_wco: mcx::PgVec::new_in(qcx),
        leaf_on_conflict: mcx::PgVec::new_in(qcx),
        leaf_returning: mcx::PgVec::new_in(qcx),
        leaf_ri_checked: mcx::PgVec::new_in(qcx),
        leaf_trigdesc: mcx::PgVec::new_in(qcx),
        leaf_trig_fmgr: mcx::PgVec::new_in(qcx),
        leaf_trig_when: mcx::PgVec::new_in(qcx),
        last_insert_leaf: None,
        last_insert_remapped: None,
        oc_returning_leaf: None,
        merge_active_cmd: None,
        mt_merge_pending_not_matched: None,
        outer_instr_idx: None,
        mt_merge_inserted: 0.0,
        mt_merge_updated: 0.0,
        mt_merge_deleted: 0.0,
    })
}

// One entry of C's ExecInitModifyTable per-result-relation work: open +
// CheckValidResultRel (skipped for the root entry, as in C where the root is
// initialized outside the loop), trigger desc, row-identity junk attr,
// RETURNING projection, WITH CHECK OPTIONs, updateColnos, and MERGE actions —
// each indexed by the relation's position in the plan's per-rel lists
// (list_index; None for the separate root entry).
fn init_result_rel<'mcx>(
    node: &'mcx ModifyTable<'mcx>,
    estate: &mut EStateData<'mcx>,
    rti: u32,
    list_index: Option<usize>,
    // The separate root's rti when node.rootRelation > 0: MERGE INSERT action
    // projections build against the ROOT descriptor (C nodeModifyTable.c:3754).
    root_rti: Option<u32>,
) -> PgResult<ResultRelExec<'mcx>> {
    estate.exec_init_result_relation(rti)?;
    let mcx = estate.es_query_cxt;
    let (trigdesc, relkind, rd_id) = {
        let rel = estate.es_relations[(rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let td = if rel.rd_hastriggers {
            relcache::RelationGetTriggerDesc(rel.rd_id)?
        } else {
            None
        };
        if list_index.is_some() {
            check_valid_result_rel(mcx, rel, node, td.as_deref())?;
        }
        (td, rel.rd_rel.relkind, rel.rd_id)
    };

    // The UPDATE/DELETE/MERGE row identity (C 4864-4924): heap-ish relkinds
    // carry a junk ctid, views a junk wholerow. A partitioned table appears
    // in the array only when every leaf was pruned (nrels==1) and produces no
    // rows, so a missing ctid is allowed there. The root entry gets none.
    let mut rowid_attno: i16 = 0;
    if list_index.is_some()
        && matches!(
            node.operation,
            CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
        )
    {
        let subplan = node
            .plan
            .lefttree
            .expect("ModifyTable has a subplan")
            .as_plan()
            .expect("plan node");
        if relkind == types_rel::RELKIND_VIEW {
            rowid_attno = exec_find_junk_attribute_in_tlist(&subplan.targetlist, "wholerow");
            assert!(rowid_attno > 0, "could not find junk wholerow column");
        } else {
            rowid_attno = exec_find_junk_attribute_in_tlist(&subplan.targetlist, "ctid");
            assert!(
                rowid_attno > 0 || relkind == types_rel::RELKIND_PARTITIONED_TABLE,
                "could not find junk ctid column"
            );
        }
    }

    // The per-rel RETURNING projection: scan vars read the returned tuple
    // (this relation's descriptor), OUTER_VARs the plan tuple.
    let mut project_returning = None;
    if let Some(i) = list_index {
        if !node.returningLists.is_nil() {
            let rlist = node
                .returningLists
                .nth(i)
                .as_list()
                .expect("returningLists cell is a List");
            let params = estate.param_bind();
            let desc = estate.es_relations[(rti - 1) as usize]
                .as_ref()
                .expect("result relation opened")
                .rd_att
                .clone();
            // SubPlans in RETURNING compile against the estate's init hook and
            // run through the node's suspension driver (exec_process_returning).
            let is_merge = node.operation == CmdType::CMD_MERGE;
            project_returning = Some(executils::with_subplan_compile_env(estate, |env| {
                if is_merge {
                    // MERGE_ACTION() may appear here (C gates EEOP_MERGE_
                    // SUPPORT_FUNC on the parent being a CMD_MERGE node).
                    execexpr::exec_build_merge_projection_info_subplans(
                        mcx,
                        rlist,
                        Some(&desc),
                        params,
                        env,
                    )
                } else {
                    execexpr::exec_build_projection_info_subplans(
                        mcx,
                        rlist,
                        Some(&desc),
                        params,
                        env,
                    )
                }
            })?);
        }
    }

    let mut wco_exprs: mcx::PgVec<'mcx, WcoExpr<'mcx>> = mcx::PgVec::new_in(mcx);
    if let Some(i) = list_index {
        if !node.withCheckOptionLists.is_nil() {
            debug_assert_eq!(node.withCheckOptionLists.len(), node.resultRelations.len());
            let params = estate.param_bind();
            let wlist = node
                .withCheckOptionLists
                .nth(i)
                .as_list()
                .expect("withCheckOptionLists cell is a List");
            for wco_node in wlist {
                let wco = wco_node.as_with_check_option().expect("WCO cell");
                let qual = wco
                    .qual
                    .expect("planned WCO has a qual")
                    .as_list()
                    .expect("WCO qual is an implicit-AND List after preprocessing");
                let state = executils::with_subplan_compile_env(estate, |env| {
                    execexpr::exec_init_qual_subplans(mcx, qual, params, env)
                })?
                .expect("planner dropped constant-true WCO quals");
                wco_exprs.push(WcoExpr {
                    kind: wco.kind,
                    relname: wco.relname.expect("WCO relname"),
                    polname: wco.polname,
                    state,
                });
            }
        }
    }

    // The per-rel updateColnos list (attnums already translated to this
    // child by the planner); resolved lazily by exec_init_update_projection.
    let mut update_colnos = None;
    if let Some(i) = list_index {
        if node.operation == CmdType::CMD_UPDATE {
            update_colnos = Some(
                node.updateColnosLists
                    .nth(i)
                    .as_int_list()
                    .expect("updateColnosLists cell is an IntList"),
            );
        }
    }

    // ExecInitMerge + ExecInitMergeTupleSlots for this relation.
    let mut merge = None;
    let mut merge_old_slot = None;
    let mut merge_new_slot = None;
    let mut merge_proj_valid = false;
    if node.operation == CmdType::CMD_MERGE {
        if let Some(i) = list_index {
            let jc = node
                .mergeJoinConditions
                .nth(i)
                .as_list()
                .expect("mergeJoinConditions cell is a List");
            let join_condition = if jc.is_nil() {
                None
            } else {
                let params = estate.param_bind();
                executils::with_subplan_compile_env(estate, |env| {
                    execexpr::exec_init_qual_subplans(mcx, jc, params, env)
                })?
            };
            let (kind, desc) = {
                let rel = estate.es_relations[(rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
                (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
            };
            let mut mk_slot = |estate: &mut EStateData<'mcx>| {
                let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc.clone()));
                let id = ExecSlotId(estate.es_tupleTable.len() as u32);
                estate.es_tupleTable.push(slot);
                id
            };
            merge_old_slot = Some(mk_slot(estate));
            merge_new_slot = Some(mk_slot(estate));
            merge_proj_valid = true;

            let mut matched_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>> =
                mcx::PgVec::new_in(mcx);
            let mut not_matched_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>> =
                mcx::PgVec::new_in(mcx);
            let mut not_matched_by_source_actions: mcx::PgVec<'mcx, MergeActionExec<'mcx>> =
                mcx::PgVec::new_in(mcx);
            let mal = node
                .mergeActionLists
                .nth(i)
                .as_list()
                .expect("mergeActionLists cell is a List");
            let params = estate.param_bind();
            for action_node in mal {
                let action = action_node.as_merge_action().expect("MergeAction cell");
                let when_qual = match action.qual {
                    None => None,
                    Some(q) => {
                        let ql = q.as_list().expect("preprocessed WHEN qual is a List");
                        executils::with_subplan_compile_env(estate, |env| {
                            execexpr::exec_init_qual_subplans(mcx, ql, params, env)
                        })?
                    }
                };
                let mut exec_action = MergeActionExec {
                    command_type: action.commandType,
                    when_qual,
                    proj: None,
                    setvals_slot: None,
                    set_attnos: mcx::PgVec::new_in(mcx),
                };
                match action.commandType {
                    CmdType::CMD_INSERT => {
                        // INSERT actions always use the root relation (its
                        // descriptor shapes the projection and the plan-output
                        // check; the insert itself routes through the root).
                        let insert_rti = root_rti.unwrap_or(rti);
                        let desc = {
                            let rel = estate.es_relations[(insert_rti - 1) as usize]
                                .as_ref()
                                .expect("result relation opened");
                            exec_check_plan_output(rel, &action.targetList)?;
                            rel.rd_att.clone()
                        };
                        exec_action.proj =
                            Some(executils::with_subplan_compile_env(estate, |env| {
                                execexpr::exec_build_projection_info_subplans(
                                    mcx,
                                    &action.targetList,
                                    Some(&desc),
                                    params,
                                    env,
                                )
                            })?);
                    }
                    CmdType::CMD_UPDATE => {
                        // Junk entries (MULTIEXPR SubPlans) are evaluated for
                        // their Param side effects but never assigned; the
                        // parser puts them after the SET columns
                        // (ExecBuildUpdateProjection's evalTargetList shape).
                        let mut non_junk = 0usize;
                        let mut seen_junk = false;
                        for tle_node in &action.targetList {
                            let tle = tle_node.as_target_entry().expect("TargetEntry");
                            if tle.resjunk {
                                seen_junk = true;
                            } else {
                                assert!(
                                    !seen_junk,
                                    "MERGE UPDATE action tlist: junk before a SET column"
                                );
                                non_junk += 1;
                            }
                        }
                        let proj = {
                            let desc = estate.es_relations[(rti - 1) as usize]
                                .as_ref()
                                .expect("result relation opened")
                                .rd_att
                                .clone();
                            executils::with_subplan_compile_env(estate, |env| {
                                execexpr::exec_build_projection_info_subplans(
                                    mcx,
                                    &action.targetList,
                                    Some(&desc),
                                    params,
                                    env,
                                )
                            })?
                        };
                        let set_desc = execscan::exec_type_from_tl(mcx, &action.targetList)?;
                        let slot = exectuples::make_tuple_table_slot(
                            mcx,
                            TupleSlotKind::Virtual,
                            Some(set_desc),
                        );
                        let id = ExecSlotId(estate.es_tupleTable.len() as u32);
                        estate.es_tupleTable.push(slot);
                        exec_action.setvals_slot = Some(id);
                        exec_action.proj = Some(proj);
                        for attno in action.updateColnos.iter() {
                            exec_action.set_attnos.push(attno as u16);
                        }
                        assert_eq!(exec_action.set_attnos.len(), non_junk);
                    }
                    CmdType::CMD_DELETE | CmdType::CMD_NOTHING => {}
                    other => panic!("unknown action in MERGE WHEN clause: {other:?}"),
                }
                use types_nodes::MergeMatchKind::*;
                match action.matchKind {
                    MERGE_WHEN_MATCHED => matched_actions.push(exec_action),
                    MERGE_WHEN_NOT_MATCHED_BY_TARGET => not_matched_actions.push(exec_action),
                    MERGE_WHEN_NOT_MATCHED_BY_SOURCE => {
                        not_matched_by_source_actions.push(exec_action)
                    }
                }
            }
            merge = Some(MergeState {
                matched_actions,
                not_matched_actions,
                not_matched_by_source_actions,
                join_condition,
            });
        }
    }

    Ok(ResultRelExec {
        rti,
        rd_id,
        relkind,
        ri_newTupleSlot: merge_new_slot,
        ri_oldTupleSlot: merge_old_slot,
        ri_ReturningSlot: None,
        ri_AllNullSlot: None,
        ri_projectNewInfoValid: merge_proj_valid,
        ri_RowIdAttNo: rowid_attno,
        update_cols: mcx::PgVec::new_in(mcx),
        update_colnos,
        indexes: None,
        project_returning,
        check_exprs: None,
        partition_check: None,
        wco_exprs,
        trigdesc,
        trig_fmgr: ::trigger::TriggerFmgrCache::default(),
        trig_old_slot: None,
        trig_when: ::trigger::TriggerWhenCache::default(),
        all_updated_cols: None,
        child_to_root: None,
        generated_exprs: None,
        virtual_nn_exprs: None,
        merge,
    })
}

// ExecFindJunkAttributeInTlist (execJunk.c); hosted here while the execjunk
// crate is claimed by the ORDER-BY-junk lane.
fn exec_find_junk_attribute_in_tlist(tlist: &types_nodes::NodeList<'_>, attr_name: &str) -> i16 {
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.resjunk && tle.resname == Some(attr_name) {
            return tle.resno;
        }
    }
    0
}

// CheckValidResultRel (execMain.c), plain-table + view + matview arms.
fn check_valid_result_rel<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    node: &'mcx ModifyTable<'mcx>,
    trigdesc: Option<&types_trigger::TriggerDesc<'static>>,
) -> PgResult<()> {
    let operation = node.operation;
    if rel.rd_rel.relkind == types_rel::RELKIND_VIEW {
        if operation == CmdType::CMD_MERGE {
            // view_has_instead_trigger (rewriteHandler.c), MERGE arm: every
            // non-NOTHING action needs its INSTEAD OF row trigger.
            let mal = node
                .mergeActionLists
                .nth(0)
                .as_list()
                .expect("mergeActionLists cell is a List");
            for action_node in mal {
                let action = action_node.as_merge_action().expect("MergeAction cell");
                let ok = match action.commandType {
                    CmdType::CMD_INSERT => {
                        trigdesc.is_some_and(|td| td.trig_insert_instead_row)
                    }
                    CmdType::CMD_UPDATE => {
                        trigdesc.is_some_and(|td| td.trig_update_instead_row)
                    }
                    CmdType::CMD_DELETE => {
                        trigdesc.is_some_and(|td| td.trig_delete_instead_row)
                    }
                    CmdType::CMD_NOTHING => true,
                    other => panic!("unrecognized commandType: {other:?}"),
                };
                if !ok {
                    return Err(error_view_not_updatable_merge(rel, action.commandType));
                }
            }
            return Ok(());
        }
        let has_instead = match operation {
            CmdType::CMD_INSERT => trigdesc.is_some_and(|td| td.trig_insert_instead_row),
            CmdType::CMD_UPDATE => trigdesc.is_some_and(|td| td.trig_update_instead_row),
            CmdType::CMD_DELETE => trigdesc.is_some_and(|td| td.trig_delete_instead_row),
            other => panic!("CheckValidResultRel (execMain.c): {other:?} on a view not ported"),
        };
        if !has_instead {
            return Err(error_view_not_updatable(rel, operation));
        }
        return Ok(());
    }
    if rel.rd_rel.relkind == types_rel::RELKIND_MATVIEW {
        if !matview_seams::matview_maintenance_is_enabled::call() {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot change materialized view \"{}\"",
                    String::from_utf8_lossy(rel.rd_rel.relname.name_str())
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        return Ok(());
    }
    if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
        // C asks the FDW's routine for the operation's callback; no in-tree
        // FDW models any of them, so the per-operation error is invariant.
        let verb = match operation {
            CmdType::CMD_INSERT => "insert into",
            CmdType::CMD_UPDATE => "update",
            CmdType::CMD_DELETE => "delete from",
            _ => panic!("CheckValidResultRel (execMain.c): {operation:?} on a foreign table"),
        };
        return Err(Box::new(
            PgError::error(format!(
                "cannot {verb} foreign table \"{}\"",
                String::from_utf8_lossy(rel.rd_rel.relname.name_str())
            ))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if rel.rd_rel.relkind != RELKIND_RELATION
        && rel.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE
    {
        panic!(
            "CheckValidResultRel (execMain.c): relkind '{}' result relation not ported",
            rel.rd_rel.relkind as char
        );
    }
    if operation == CmdType::CMD_MERGE {
        let mal = node
            .mergeActionLists
            .nth(0)
            .as_list()
            .expect("mergeActionLists cell is a List");
        for action_node in mal {
            let action = action_node.as_merge_action().expect("MergeAction cell");
            execreplication_seams::check_cmd_replica_identity::call(mcx, rel, action.commandType)?;
        }
    } else {
        execreplication_seams::check_cmd_replica_identity::call(mcx, rel, operation)?;
    }
    if node.onConflictAction == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32 {
        execreplication_seams::check_cmd_replica_identity::call(mcx, rel, CmdType::CMD_UPDATE)?;
    }
    Ok(())
}

// error_view_not_updatable (rewriteHandler.c), executor-check leg (no
// errdetail, per C's CheckValidResultRel call).
#[track_caller]
#[cold]
#[inline(never)]
fn error_view_not_updatable(rel: &Relation<'_>, operation: CmdType) -> Box<PgError> {
    let name = rel.name();
    let (msg, hint) = match operation {
        CmdType::CMD_INSERT => (
            format!("cannot insert into view \"{name}\""),
            "To enable inserting into the view, provide an INSTEAD OF INSERT trigger or \
             an unconditional ON INSERT DO INSTEAD rule.",
        ),
        CmdType::CMD_UPDATE => (
            format!("cannot update view \"{name}\""),
            "To enable updating the view, provide an INSTEAD OF UPDATE trigger or an \
             unconditional ON UPDATE DO INSTEAD rule.",
        ),
        _ => (
            format!("cannot delete from view \"{name}\""),
            "To enable deleting from the view, provide an INSTEAD OF DELETE trigger or \
             an unconditional ON DELETE DO INSTEAD rule.",
        ),
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(hint.to_string()),
    )
}

// error_view_not_updatable (rewriteHandler.c), CMD_MERGE arm: MERGE hints
// omit rules (MERGE doesn't support them).
#[track_caller]
#[cold]
#[inline(never)]
fn error_view_not_updatable_merge(rel: &Relation<'_>, action: CmdType) -> Box<PgError> {
    let name = rel.name();
    let (msg, hint) = match action {
        CmdType::CMD_INSERT => (
            format!("cannot insert into view \"{name}\""),
            "To enable inserting into the view using MERGE, provide an INSTEAD OF INSERT \
             trigger.",
        ),
        CmdType::CMD_UPDATE => (
            format!("cannot update view \"{name}\""),
            "To enable updating the view using MERGE, provide an INSTEAD OF UPDATE trigger.",
        ),
        _ => (
            format!("cannot delete from view \"{name}\""),
            "To enable deleting from the view using MERGE, provide an INSTEAD OF DELETE \
             trigger.",
        ),
    };
    Box::new(
        PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint(hint.to_string()),
    )
}

/// `ExecModifyTable` (nodeModifyTable.c), INSERT/UPDATE/DELETE loop.
/// `epq_eval` is execMain's `EvalPlanQual` over the caller-owned EPQState
/// (input = the locked latest row version in the EvalPlanQualSlot).
pub fn exec_modify_table<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_instr_idx: Option<u32>,
    mut fetch_outer: impl FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    mut epq_eval: impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    if !mt_begin(mt, estate, outer_instr_idx)? {
        return Ok(None);
    }
    mt_step(mt, estate, &mut fetch_outer, &mut epq_eval)
}

/// The per-call head of `exec_modify_table` (wave-2 WS-N seam `mt_begin`,
/// integration contract §3.7 — a pure code move of the pre-loop statements):
/// the node-done check, the EXPLAIN-ANALYZE outer-instrument carry, and the
/// once-per-statement BEFORE STATEMENT trigger firing. `Ok(false)` = the node
/// already ran to completion (the caller returns end-of-set without touching
/// anything else). Idempotent across pulls: `fireBSTriggers` flips off after
/// the first call, `mt_done` short-circuits post-completion calls.
///
/// inline attributes on the mt_* seams (se2-cost-fix): the seam
/// decomposition must not cost the knob-OFF Volcano arm its pre-seam
/// codegen — the m4 fleet pair measured the outlined seams at +123
/// instr/INSERT-statement and +55-60 instr/row (batch) against the
/// knob-OFF==baseline letter. `#[inline(always)]` on the per-row seams
/// restores the loop-body inlining `exec_modify_table` had when this code
/// was its literal loop body; `#[inline]` on the per-statement seams keeps
/// their call overhead out of the per-statement floor (±4).
#[inline(always)]
pub fn mt_begin<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_instr_idx: Option<u32>,
) -> PgResult<bool> {
    if mt.mt_done {
        return Ok(false);
    }
    mt.outer_instr_idx = outer_instr_idx;
    if mt.fireBSTriggers {
        fire_bs_triggers(mt, estate)?;
        mt.fireBSTriggers = false;
    }
    Ok(true)
}

/// The loop-top per-row reset of `exec_modify_table` (wave-2 WS-N seam
/// `mt_row_prologue`, contract §3.7): the per-tuple expr-context reset plus
/// the node's index-expression eval-context reset. Contract LAW (§3.7): this
/// runs BEFORE the child pull — the per-tuple context may hold by-ref datums
/// the PREVIOUS row's processing produced, and the reset must not run after
/// the next child row is fetched (the fetched slot's datums could live
/// there). In the lane hosting the placement is structural: `MtChildSource::
/// next_row` (lanev2/dml.rs) calls this before pulling, never `accept`.
#[inline(always)] // per-row seam — see mt_begin's se2-cost-fix note
pub fn mt_row_prologue<'mcx>(mt: &mut ModifyTableState<'mcx>, estate: &mut EStateData<'mcx>) {
    estate.reset_per_tuple_expr_context();
    mt.index_eval_cx.as_mut().expect("index_eval_cx live until ExecEndNode").reset();
}

/// Whether a deferred MERGE ... WHEN NOT MATCHED [BY TARGET] action from the
/// previous source row is queued (wave-2 WS-N seam `mt_pending`, contract
/// §3.7). Only `exec_merge_matched_scan`'s concurrent-flip leg sets it; a
/// plain INSERT/UPDATE/DELETE node never reports pending.
#[inline(always)] // per-row seam — see mt_begin's se2-cost-fix note
pub fn mt_pending(mt: &ModifyTableState<'_>) -> bool {
    mt.mt_merge_pending_not_matched.is_some()
}

/// Run the deferred MERGE NOT MATCHED action (wave-2 WS-N seam `mt_resume`,
/// contract §3.7 — the loop-top pending arm of `exec_modify_table`, C
/// nodeModifyTable.c 4200-4218, as a pure code move): it targets the node's
/// toplevel result relation and runs BEFORE the next source row is fetched.
/// The caller runs `mt_row_prologue` first (the C loop-top order).
pub fn mt_resume<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    let pending = mt
        .mt_merge_pending_not_matched
        .take()
        .expect("mt_resume called without a pending MERGE action");
    mt.cur = 0;
    mt.last_result_oid = 0;
    exec_merge_not_matched(mt, estate, pending, epq_eval)
}

/// One `exec_modify_table` call's worth of the ModifyTable loop, composed
/// from the wave-2 WS-N seams (contract §3.7): per row, `mt_row_prologue` →
/// the `mt_pending`/`mt_resume` deferred-MERGE arm → child pull →
/// `mt_accept_row`; on child exhaustion, `mt_source_exhausted`. BOTH engines
/// drive this exact function — `exec_modify_table` (the Volcano arm) above,
/// and the lane host's `MtChildSource` delegation (lanev2/dml.rs) — so the
/// statement stream is identical by construction.
#[inline(always)] // loop composition — see mt_begin's se2-cost-fix note
pub fn mt_step<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut impl FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    loop {
        mt_row_prologue(mt, estate);

        if mt_pending(mt) {
            if let Some(rslot) = mt_resume(mt, estate, epq_eval)? {
                return Ok(Some(rslot));
            }
            continue;
        }

        let Some(plan_slot) = fetch_outer(estate)? else {
            mt_source_exhausted(mt, estate)?;
            return Ok(None);
        };

        if let Some(rslot) = mt_accept_row(mt, estate, plan_slot, epq_eval)? {
            return Ok(Some(rslot));
        }
    }
}

/// Process one fetched source row (wave-2 WS-N seam `mt_accept_row`,
/// contract §3.7 — the loop body of `exec_modify_table` from the
/// EvalPlanQualSetSlot mirror through the operation dispatch, as a pure code
/// move). `Some` = a RETURNING row to hand to the caller; `None` = the row
/// was consumed without producing output (the caller pulls the next one).
#[inline(always)] // per-row seam (the former loop body) — see mt_begin's note
pub fn mt_accept_row<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    // The 4-space-deep block below is the loop body verbatim (pure code
    // move): a former `continue` (row consumed, pull the next) is now
    // `return Ok(None)`, the `return Ok(Some(..))`s are unchanged.
    {
        // C EvalPlanQualSetSlot: the EPQ rowmark fetch reads this row's junk
        // ctid/wholerow columns to re-return the source rel's tuple.
        mt.epq_origslot = Some(plan_slot);
        if let Some(subs) = mt.epq_subs.as_mut() {
            subs.origslot = Some(plan_slot);
        }

        // Multi-result-relation dispatch: the junk tableoid column names the
        // relation this row came from (C 4263-4311). A NULL tableoid is a
        // MERGE NOT MATCHED source row (handled against the toplevel result
        // relation, rels[0]) and an error otherwise.
        if mt.result_oid_attno > 0 {
            let mut isnull = false;
            let datum = {
                let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
                exectuples::slot_getattr(slot, mt.result_oid_attno as i32, &mut isnull)
            };
            if isnull {
                if mt.operation == CmdType::CMD_MERGE {
                    // Both cache halves must move together: clobbering cur
                    // while last_result_oid stands lets the next row of the
                    // pre-insert relation skip the lookup and run against
                    // rels[0] (C passes resultRelInfo as an argument and
                    // leaves the mt_lastResultOid cache untouched).
                    mt.cur = 0;
                    mt.last_result_oid = 0;
                    if let Some(rslot) =
                        exec_merge(mt, estate, plan_slot, None, None, &mut *epq_eval)?
                    {
                        return Ok(Some(rslot));
                    }
                    // Former loop `continue`: the row is consumed.
                    return Ok(None);
                }
                return Err(Box::new(PgError::error("tableoid is NULL".to_string())));
            }
            let resultoid: Oid = datum.as_oid();
            if resultoid != mt.last_result_oid {
                mt.lookup_result_rel_by_oid(resultoid)?;
            }
        }

        match mt.operation {
            CmdType::CMD_INSERT => {
                if !mt.rel().ri_projectNewInfoValid {
                    exec_init_insert_projection(mt, estate)?;
                }
                let slot = exec_get_insert_new_tuple(mt, estate, plan_slot)?;
                let result = exec_insert(mt, estate, slot, &mut *epq_eval)?;
                if let Some(rslot) = result {
                    if mt.rel().project_returning.is_some() {
                        let old = mt.oc_old_slot.take();
                        let cmd = if old.is_some() {
                            CmdType::CMD_UPDATE
                        } else {
                            CmdType::CMD_INSERT
                        };
                        let out = exec_process_returning(
                            mt, estate, cmd, old, Some(rslot), plan_slot,
                        )?;
                        if let Some(oid) = old {
                            // C ExecOnConflictUpdate (nodeModifyTable.c):
                            // the RETURNING slot may hold by-reference OLD
                            // Datums (e.g. o.ctid) that alias the existing
                            // slot's own storage; materialize before
                            // clearing existing or those Datums dangle.
                            let has_old = mt
                                .rel()
                                .project_returning
                                .as_deref()
                                .is_some_and(|st| st.has_old());
                            if has_old {
                                let mcx = estate.es_query_cxt;
                                exectuples::exec_materialize_slot(
                                    &mut estate.es_tupleTable[out.0 as usize],
                                    mcx,
                                )?;
                            }
                            clear_slot(estate, oid);
                        }
                        return Ok(Some(out));
                    }
                }
            }
            CmdType::CMD_UPDATE if mt.rel().relkind == types_rel::RELKIND_VIEW => {
                let old_tup = fetch_wholerow_tuple(mt, estate, plan_slot)?;
                if !mt.rel().ri_projectNewInfoValid {
                    exec_init_update_projection(mt, estate)?;
                }
                let old_slot = mt.rel().ri_oldTupleSlot.expect("ExecInitUpdateProjection ran");
                {
                    let mcx = estate.es_query_cxt;
                    exectuples::exec_force_store_heap_tuple(
                        old_tup,
                        &mut estate.es_tupleTable[old_slot.0 as usize],
                        mcx,
                    )?;
                }
                let slot = exec_get_update_new_tuple(mt, estate, plan_slot)?;
                let modified = ir_row_triggers(
                    mt,
                    estate,
                    types_trigger::TRIGGER_TYPE_UPDATE,
                    types_trigger::TRIGGER_EVENT_UPDATE,
                    Some(old_slot),
                    Some(slot),
                )?;
                if modified {
                    // Parent-view CHECK OPTIONs still apply after INSTEAD OF
                    // triggers (C ExecUpdateEpilogue's WCO_VIEW_CHECK leg).
                    if !mt.rel().wco_exprs.is_empty() {
                        let mcx = estate.es_query_cxt;
                        let ecxt = mt.node_ecxt;
                        let r = &mut mt.rels[mt.cur];
                        let rti = r.rti;
                        exec_view_check_options(
                            mcx,
                            estate,
                            ecxt,
                            &mut r.wco_exprs,
                            slot,
                            WcoRel::Rti { rti, root_rti: None },
                        )?;
                    }
                    if mt.canSetTag {
                        estate.es_processed += 1;
                    }
                    if mt.rel().project_returning.is_some() {
                        return Ok(Some(exec_process_returning(
                            mt,
                            estate,
                            CmdType::CMD_UPDATE,
                            Some(old_slot),
                            Some(slot),
                            plan_slot,
                        )?));
                    }
                }
            }
            CmdType::CMD_UPDATE => {
                let mut tupleid = fetch_row_id(mt, estate, plan_slot);
                if !mt.rel().ri_projectNewInfoValid {
                    exec_init_update_projection(mt, estate)?;
                }
                fetch_old_row_version(mt, estate, &tupleid)?;
                let slot = exec_get_update_new_tuple(mt, estate, plan_slot)?;
                match exec_update(mt, estate, &mut tupleid, slot, &mut *epq_eval)? {
                    UpdateResult::NotModified => {}
                    UpdateResult::Modified => {
                        if mt.rel().project_returning.is_some() {
                            return Ok(Some(exec_process_returning(
                                mt,
                                estate,
                                CmdType::CMD_UPDATE,
                                mt.rel().ri_oldTupleSlot,
                                Some(slot),
                                plan_slot,
                            )?));
                        }
                    }
                    // Cross-partition move: RETURNING reports the INSERT
                    // half's row (C cpUpdateReturningSlot).
                    UpdateResult::CrossPart(inserted) => {
                        if let Some(islot) = inserted {
                            if mt.rel().project_returning.is_some() {
                                let old = mt.rel().ri_oldTupleSlot;
                                return Ok(Some(exec_cross_part_returning(
                                    mt, estate, old, islot, plan_slot,
                                )?));
                            }
                        }
                    }
                }
            }
            CmdType::CMD_DELETE if mt.rel().relkind == types_rel::RELKIND_VIEW => {
                let old_tup = fetch_wholerow_tuple(mt, estate, plan_slot)?;
                let old_slot = ensure_trig_old_slot(mt, estate);
                {
                    let mcx = estate.es_query_cxt;
                    exectuples::exec_force_store_heap_tuple(
                        old_tup,
                        &mut estate.es_tupleTable[old_slot.0 as usize],
                        mcx,
                    )?;
                }
                let deleted = ir_row_triggers(
                    mt,
                    estate,
                    types_trigger::TRIGGER_TYPE_DELETE,
                    types_trigger::TRIGGER_EVENT_DELETE,
                    Some(old_slot),
                    None,
                )?;
                if deleted {
                    if mt.canSetTag {
                        estate.es_processed += 1;
                    }
                    if mt.rel().project_returning.is_some() {
                        return Ok(Some(exec_process_returning(
                            mt,
                            estate,
                            CmdType::CMD_DELETE,
                            Some(old_slot),
                            None,
                            plan_slot,
                        )?));
                    }
                }
            }
            CmdType::CMD_DELETE => {
                let mut tupleid = fetch_row_id(mt, estate, plan_slot);
                let modified =
                    exec_delete(mt, estate, &mut tupleid, &mut *epq_eval, false, None, None)?;
                if modified && mt.rel().project_returning.is_some() {
                    let old_slot = exec_delete_fetch_old(mt, estate, &tupleid)?;
                    return Ok(Some(exec_process_returning(
                        mt,
                        estate,
                        CmdType::CMD_DELETE,
                        Some(old_slot),
                        None,
                        plan_slot,
                    )?));
                }
            }
            CmdType::CMD_MERGE => {
                // A view target carries the old row as a wholerow junk attr
                // (NULL = NOT MATCHED); tables carry a ctid (C 4237-4291).
                let (tupleid, oldtup) = if mt.rel().relkind == types_rel::RELKIND_VIEW {
                    (None, fetch_wholerow_tuple_opt(mt, estate, plan_slot)?)
                } else {
                    (fetch_merge_row_id(mt, estate, plan_slot), None)
                };
                if tupleid.is_none() && oldtup.is_none() {
                    // NOT MATCHED rows run against the node's toplevel result
                    // relation, not any specific child's (C 4283-4287).
                    mt.cur = 0;
                    mt.last_result_oid = 0;
                }
                if let Some(rslot) =
                    exec_merge(mt, estate, plan_slot, tupleid, oldtup, &mut *epq_eval)?
                {
                    return Ok(Some(rslot));
                }
            }
            other => panic!("ExecModifyTable (nodeModifyTable.c): {other:?} arm not ported"),
        }
    }
    // The row was consumed without producing a RETURNING row (the former
    // loop-bottom fall-through).
    Ok(None)
}

/// The child-exhausted epilogue of `exec_modify_table` (wave-2 WS-N seam
/// `mt_source_exhausted`, contract §3.7 — the post-loop statements as a pure
/// code move): the pgrcolumnar statement-end flush, AFTER STATEMENT
/// triggers, and the `mt_done` latch. Runs exactly once per statement — the
/// latch makes every later `mt_begin` report done.
// inline(always): the plain hint did not take (round-2 dist-prof callgrind
// still shows both codegen copies outlined, +55/stmt) — se2-cost-fix round 3.
#[inline(always)]
pub fn mt_source_exhausted<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(estate.es_insert_pending_result_relations.is_empty());
    // pgrcolumnar statement-end flush: single-tuple inserts buffer in the AM's
    // per-statement ingest writer (pgrcolumnar::tuple_insert; RG-sized seals
    // instead of one row group per row), published here — before AS triggers
    // so statement triggers observe the rows.
    for r in mt.rels.iter() {
        if r.relkind != RELKIND_RELATION {
            continue;
        }
        if let Some(rel) = estate.es_relations[(r.rti - 1) as usize].as_ref() {
            if tableam_vocab::is_pgrcolumnar_am_oid(rel.rd_rel.relam) {
                tableam::table_finish_bulk_insert(rel, 0)?;
            }
        }
    }
    fire_as_triggers(mt, estate)?;
    mt.mt_done = true;
    Ok(())
}

/// Lane-admission shape probe (lanev2/dml.rs, behind `PGRUST_LANE_V2_DML`;
/// wave-2 WS-N inc-1 authored it as `mt_lane_insert_refusal`, wave-3 WS-T
/// inc-3a renamed + WIDENED it per docs/design/lane-dml-epq.md §6, wave-5
/// WS-W widened the ON CONFLICT arm per the wave-5 contract §8.3):
/// `None` = a shape the DML lane hosts — a single-result-relation
/// plain-table mutation with no triggers, no partition routing / inherited
/// root, and at most trivial (no OLD/NEW alias) RETURNING, where the
/// operation is INSERT always, UPDATE/DELETE only when the caller passes
/// `admit_ud` (the nested `PGRUST_LANE_V2_DML_UD` stretch knob, read by
/// the lane AFTER the host knob — never here), and INSERT .. ON CONFLICT
/// (DO NOTHING and DO UPDATE, the ladder-named OC arms — the four oc_*
/// seams above compose the whole ceremony inside `exec_insert`, which
/// both engines share) only when the caller passes `admit_oc` (the nested
/// `PGRUST_LANE_V2_DML_OC` knob, same read discipline).
/// `Some(detail)` = the `DmlShape` refusal with its mechanism-attribution
/// detail string (integration contract §1: attribution rides the detail
/// string, never a second class). MERGE stays refused EVEN under both
/// nested knobs (blocked on the C-side trace pin); partition routing and
/// triggers have no scheduled increment; the structural gates below are
/// operation-agnostic, so a UD- or OC-admitted shape passes exactly the
/// inc-1 INSERT gates (in particular `target-not-plain-table` keeps the
/// VIEW/ir-trigger arms of `mt_accept_row` out of the admitted set, and
/// `partition-routing` keeps the leaf-arbiter/leaf-on-conflict legs of the
/// oc_* seams out of the OC-admitted set).
///
/// Lives here (not in the lane) because the verdict reads private node
/// state; it is a read-only probe — calling it changes nothing, so a refusal
/// falls through to the unchanged Volcano arm byte-safely. The admitted set
/// widens in later increments (docs/design/lane-dml-epq.md ladder); every
/// widening deletes (or knob-gates) a `Some` arm here and re-justifies its
/// allowlist row together.
pub fn mt_lane_shape_refusal(
    mt: &ModifyTableState<'_>,
    admit_ud: bool,
    admit_oc: bool,
) -> Option<&'static str> {
    match mt.operation {
        CmdType::CMD_INSERT => {}
        CmdType::CMD_UPDATE if admit_ud => {}
        CmdType::CMD_DELETE if admit_ud => {}
        CmdType::CMD_UPDATE => return Some("update"),
        CmdType::CMD_DELETE => return Some("delete"),
        CmdType::CMD_MERGE => return Some("merge"),
        _ => return Some("unknown-operation"),
    }
    if mt.plan.onConflictAction
        != types_nodes::primnodes::OnConflictAction::ONCONFLICT_NONE as u32
        && !admit_oc
    {
        return Some("on-conflict");
    }
    // rootRelation > 0 = partitioned or inherited target: INSERT routes
    // through the root (`root` is Some); >1 result rels never happens for
    // INSERT but is refused defensively with the same detail.
    if mt.root.is_some() || mt.rels.len() != 1 {
        return Some("partition-routing");
    }
    let rel = &mt.rels[0];
    // Views (INSTEAD OF triggers / auto-updatable), foreign tables,
    // matviews, partitioned roots reached without `root`: not plain heaps.
    if rel.relkind != RELKIND_RELATION {
        return Some("target-not-plain-table");
    }
    // ANY triggers on the target: refused since inc-1. (The wave-7 WS-AA
    // trigger-INSERT chain carve-out that briefly widened this arm — the
    // `admit_row_triggers` parameter fed by the `PGRUST_LANESTITCH_ROWCHAIN`
    // knob, default OFF at every tip — was DELETED at RB-R1/SE18 with the
    // stitched chain: the refusal is unconditional again, byte-identical to
    // every default-config tip since inc-1.)
    if rel.trigdesc.is_some() {
        return Some("triggers");
    }
    // RETURNING is admitted (contract §6-WS-N(1)); the OLD/NEW-alias form
    // (RETURNING OLD.*, NEW.*) is the non-trivial carve-out this increment.
    if let Some(st) = rel.project_returning.as_deref() {
        if st.has_old() || st.has_new() {
            return Some("returning-old-new");
        }
    }
    None
}

// ExecGetAllUpdatedCols (execUtils.c): perminfo updatedCols unioned with the
// ExecInitGenerated(CMD_UPDATE) extraUpdatedCols leg — generated columns whose
// expressions depend on an updated column (all of them when a BEFORE ROW
// UPDATE trigger could change more columns).
fn ensure_all_updated_cols<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &EStateData<'mcx>,
    for_root: bool,
) -> PgResult<()> {
    let (this_rti, is_child) = if for_root {
        (mt.root_rel().rti, false)
    } else {
        (mt.rel().rti, mt.root.is_some())
    };
    {
        let r = if for_root { mt.root_rel() } else { mt.rel() };
        if r.all_updated_cols.is_some() {
            return Ok(());
        }
    }
    let mcx = estate.es_query_cxt;
    // GetResultRTEPermissionInfo (execUtils.c): a child result relation reads
    // the root parent's RTE — the only one carrying a perminfo — and maps the
    // column numbers through the root-to-child attrmap (ExecGetUpdatedCols).
    let perminfo_rti = if is_child { mt.root_rel().rti } else { this_rti };
    let rte = estate.es_range_table[(perminfo_rti - 1) as usize];
    let mut cols = types_nodes::Bitmapset::empty();
    if rte.perminfoindex > 0 {
        let pis = estate.es_rteperminfos.expect("result RTE carries a perminfo");
        let pi = pis
            .nth(rte.perminfoindex as usize - 1)
            .as_rte_permission_info()
            .expect("permInfos cell");
        cols = pi.updatedCols.clone_in(mcx)?;
        if is_child {
            let root_rti = mt.root_rel().rti;
            let attr_map = {
                let root_rel = estate.es_relations[(root_rti - 1) as usize]
                    .as_ref()
                    .expect("root relation opened");
                let child = estate.es_relations[(this_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
                tupdesc::build_attrmap_by_name_if_req(
                    mcx,
                    &root_rel.rd_att,
                    &child.rd_att,
                    !child.rd_rel.relispartition,
                )?
            };
            if let Some(map) = attr_map {
                cols = execute_attr_map_cols(mcx, &map, &cols)?;
            }
        }
    }
    {
        const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
        let rel = estate.es_relations[(this_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let has_generated = rel
            .rd_att
            .constr
            .as_deref()
            .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual);
        if has_generated {
            let trigdesc = if for_root { &mt.root_rel().trigdesc } else { &mt.rel().trigdesc };
            let skip_by_deps =
                !trigdesc.as_ref().is_some_and(|td| td.trig_update_before_row);
            let constr = rel.rd_att.constr.as_deref().expect("checked above");
            for i in 0..rel.rd_att.natts as usize {
                if rel.rd_att.attr(i).attgenerated == 0 {
                    continue;
                }
                if skip_by_deps {
                    let adbin = constr
                        .defval
                        .iter()
                        .find(|d| d.adnum == (i + 1) as i16)
                        .and_then(|d| d.adbin.as_ref())
                        .unwrap_or_else(|| {
                            panic!(
                                "no generation expression found for column number {} of table \"{}\"",
                                i + 1,
                                String::from_utf8_lossy(rel.rd_rel.relname.name_str())
                            )
                        });
                    let expr = readfuncs::stringToNode(mcx, adbin.as_str())?;
                    let mut attrs_used = types_nodes::Bitmapset::empty();
                    vars::var::pull_varattnos(mcx, expr, 1, &mut attrs_used)?;
                    if !cols.overlap(&attrs_used) {
                        continue;
                    }
                }
                cols.add_member(mcx, (i + 1) as i32 - FLIHAN)?;
            }
        }
    }
    let r = if for_root { mt.root_rel_mut() } else { mt.rel_mut() };
    r.all_updated_cols = Some(cols);
    Ok(())
}

// ExecGetAllUpdatedCols for a ROUTED leaf (execUtils.c ExecGetUpdatedCols'
// ri_RootResultRelInfo arm): the target's updated columns renumbered through
// the root->leaf attrmap. C recomputes per call; so does this. Same
// simplification as on_conflict_update_lock_mode: leaf-local generated-column
// extras aren't recomputed — the root's, mapped, stand in (partitions share
// the parent's generation expressions).
fn leaf_all_updated_cols<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &EStateData<'mcx>,
    idx: usize,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    let mcx = estate.es_query_cxt;
    ensure_all_updated_cols(mt, estate, false)?;
    let rti = mt.rel().rti;
    let root_rel = estate.es_relations[(rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let mut cols = mt
        .rel()
        .all_updated_cols
        .as_ref()
        .expect("resolved above")
        .clone_in(mcx)?;
    let leaf_rel = mt.router.as_ref().expect("routed").leaf_rel(idx);
    if let Some(map) = tupdesc::build_attrmap_by_name_if_req(
        mcx,
        &root_rel.rd_att,
        &leaf_rel.rd_att,
        !leaf_rel.rd_rel.relispartition,
    )? {
        cols = execute_attr_map_cols(mcx, &map, &cols)?;
    }
    Ok(cols)
}

// bms_union(ExecGetInsertedCols, ExecGetUpdatedCols) through the result
// RTE's perminfo (execUtils.c GetResultRTEPermissionInfo): pass the root's
// rti for a routed child so the numbering matches the description relation.
fn rte_modified_cols<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    es_range_table: &[&'mcx types_nodes::RangeTblEntry<'mcx>],
    es_rteperminfos: Option<&types_nodes::NodeList<'mcx>>,
    rti: u32,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    let rte = es_range_table[(rti - 1) as usize];
    if rte.perminfoindex > 0 {
        if let Some(pis) = es_rteperminfos {
            let pi = pis
                .nth(rte.perminfoindex as usize - 1)
                .as_rte_permission_info()
                .expect("permInfos cell");
            return pi.insertedCols.union(&pi.updatedCols, mcx);
        }
    }
    Ok(types_nodes::Bitmapset::empty())
}

// execute_attr_map_cols (attmap.c): translate a perminfo column bitmapset
// (attnos offset by FirstLowInvalidHeapAttributeNumber) from the map's input
// (root) numbering to its output (child) numbering; attr_map[out-1] = in.
fn execute_attr_map_cols<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    attr_map: &[i16],
    in_cols: &types_nodes::Bitmapset<'mcx>,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let mut out_cols = types_nodes::Bitmapset::empty();
    for (out_idx, &in_attno) in attr_map.iter().enumerate() {
        if in_attno == 0 {
            continue;
        }
        if in_cols.is_member(in_attno as i32 - FLIHAN) {
            out_cols.add_member(mcx, (out_idx + 1) as i32 - FLIHAN)?;
        }
    }
    Ok(out_cols)
}

// fireBSTriggers/fireASTriggers (nodeModifyTable.c); INSERT ... ON CONFLICT
// DO UPDATE fires both INSERT and UPDATE statement triggers (AS: UPDATE
// first); MERGE fires per present subcommand.
#[inline(always)] // se2-cost-fix round 3: the round-2 plain hint did not take (+40/stmt outlined)
fn fire_bs_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use types_trigger::*;
    if mt.root_rel().trigdesc.is_none() {
        return Ok(());
    }
    let (ins, upd, del) = stmt_trigger_ops(mt, true);
    if ins {
        exec_bs_triggers(mt, estate, TRIGGER_TYPE_INSERT, TRIGGER_EVENT_INSERT)?;
    }
    if upd {
        exec_bs_triggers(mt, estate, TRIGGER_TYPE_UPDATE, TRIGGER_EVENT_UPDATE)?;
    }
    if del {
        exec_bs_triggers(mt, estate, TRIGGER_TYPE_DELETE, TRIGGER_EVENT_DELETE)?;
    }
    Ok(())
}

#[inline(always)] // se2-cost-fix round 3: rides mt_source_exhausted's always-inline chain
fn fire_as_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(td) = mt.root_rel().trigdesc.clone() else {
        return Ok(());
    };
    let (ins, upd, del) = stmt_trigger_ops(mt, false);
    if upd && td.triggers.iter().any(|t| t.tgnattr > 0) {
        ensure_all_updated_cols(mt, estate, true)?;
    }
    let mcx = estate.es_query_cxt;
    let result_rti = mt.root_rel().rti;
    let ModifyTableState { rels, root, transition_capture, oc_transition_capture, .. } = mt;
    let target = root.as_mut().unwrap_or(&mut rels[0]);
    let (trig_when, all_updated_cols) = (&mut target.trig_when, &target.all_updated_cols);
    let rel = estate.es_relations[(result_rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let tc = transition_capture.as_ref();
    if del {
        let mut when =
            ::trigger::TriggerWhenEval { mcx, cache: trig_when, modified_cols: None };
        ::trigger::ExecASDeleteTriggers(rel, &td, tc, Some(&mut when))?;
    }
    if upd {
        let mut when = ::trigger::TriggerWhenEval {
            mcx,
            cache: trig_when,
            modified_cols: all_updated_cols.as_ref(),
        };
        let oc = oc_transition_capture.as_ref();
        ::trigger::ExecASUpdateTriggers(rel, &td, if oc.is_some() { oc } else { tc }, Some(&mut when))?;
    }
    if ins {
        let mut when =
            ::trigger::TriggerWhenEval { mcx, cache: trig_when, modified_cols: None };
        ::trigger::ExecASInsertTriggers(rel, &td, tc, Some(&mut when))?;
    }
    Ok(())
}

// (insert, update, delete) statement-trigger ops for this node. BS order is
// op-major (INSERT then conflict-UPDATE); AS inverts (C fireASTriggers), which
// the caller's DELETE/UPDATE/INSERT sequencing preserves for MERGE too.
fn stmt_trigger_ops(mt: &ModifyTableState<'_>, _before: bool) -> (bool, bool, bool) {
    match mt.operation {
        CmdType::CMD_INSERT => (
            true,
            mt.plan.onConflictAction == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32,
            false,
        ),
        CmdType::CMD_UPDATE => (false, true, false),
        CmdType::CMD_DELETE => (false, false, true),
        CmdType::CMD_MERGE => {
            let mut ops = (false, false, false);
            if let Some(m) = &mt.rels[0].merge {
                for a in m
                    .matched_actions
                    .iter()
                    .chain(m.not_matched_actions.iter())
                    .chain(m.not_matched_by_source_actions.iter())
                {
                    match a.command_type {
                        CmdType::CMD_INSERT => ops.0 = true,
                        CmdType::CMD_UPDATE => ops.1 = true,
                        CmdType::CMD_DELETE => ops.2 = true,
                        _ => {}
                    }
                }
            }
            ops
        }
        _ => (false, false, false),
    }
}

// ExecBS{Insert,Update,Delete}Triggers (trigger.c).
fn exec_bs_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tgtype_event: i16,
    event_op: u32,
) -> PgResult<()> {
    use types_trigger::{
        TRIGGER_EVENT_BEFORE, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_LEVEL_MASK,
        TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_TIMING_MASK,
    };
    let trigdesc = mt.root_rel().trigdesc.as_ref().expect("caller checked trigdesc").clone();
    let has_before = match event_op {
        types_trigger::TRIGGER_EVENT_INSERT => trigdesc.trig_insert_before_statement,
        types_trigger::TRIGGER_EVENT_UPDATE => trigdesc.trig_update_before_statement,
        _ => trigdesc.trig_delete_before_statement,
    };
    if !has_before {
        return Ok(());
    }
    let relid = mt.root_rel().rd_id;
    if ::trigger::before_stmt_triggers_fired(relid, event_op) {
        return Ok(());
    }
    if event_op == types_trigger::TRIGGER_EVENT_UPDATE
        && trigdesc.triggers.iter().any(|t| t.tgnattr > 0)
    {
        ensure_all_updated_cols(mt, estate, true)?;
    }
    let mcx = estate.es_query_cxt;
    let tg_event = event_op | TRIGGER_EVENT_BEFORE;
    for (i, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | tgtype_event)
            != TRIGGER_TYPE_STATEMENT | TRIGGER_TYPE_BEFORE | tgtype_event
        {
            continue;
        }
        if !::trigger::TriggerEnabled(trigger) {
            continue;
        }
        if trigger.tgnattr > 0 || trigger.tgqual.is_some() {
            let target = mt.root_rel_mut();
            let modified_cols = target.all_updated_cols.take();
            let rel = estate.es_relations[(target.rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let mut when = ::trigger::TriggerWhenEval {
                mcx,
                cache: &mut target.trig_when,
                modified_cols: modified_cols.as_ref(),
            };
            let pass = when.check(i, trigger, rel, tg_event, None, None)?;
            target.all_updated_cols = modified_cols;
            if !pass {
                continue;
            }
        }
        let ret = {
            let root_rti = mt.root_rel().rti;
            let finfo = mt.root_rel_mut().trig_fmgr.get(i, trigger.tgfoid)?;
            let rel = estate.es_relations[(root_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let mut tdata =
                types_trigger_call::TriggerData::new(tg_event, rel, None, None, trigger);
            ::trigger::ExecCallTriggerFunc(mcx, &mut tdata, finfo)?
        };
        if ret.is_some() {
            return Err(Box::new(
                PgError::error("BEFORE STATEMENT trigger cannot return a value".to_string())
                    .with_sqlstate(types_error::ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
            ));
        }
    }
    Ok(())
}

// The ctid-junk fetch of ExecModifyTable's row-identity block; the datum is a
// pointer into the plan slot's tuple, copied out as C copies to tuple_ctid.
fn fetch_row_id<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> ItemPointerData {
    debug_assert!(mt.rel().ri_RowIdAttNo > 0);
    let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getattr(slot, mt.rel().ri_RowIdAttNo as i32, &mut isnull);
    assert!(!isnull, "ctid is NULL");
    // SAFETY: a tid datum is a pointer to an ItemPointerData inside the
    // deformed plan tuple, live for this row.
    unsafe { *(datum.as_usize() as *const ItemPointerData) }
}

// The MERGE row-identity fetch: a NULL ctid is a NOT MATCHED [BY TARGET]
// source row from the outer join.
fn fetch_merge_row_id<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> Option<ItemPointerData> {
    debug_assert!(mt.rel().ri_RowIdAttNo > 0);
    let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getattr(slot, mt.rel().ri_RowIdAttNo as i32, &mut isnull);
    if isnull {
        return None;
    }
    // SAFETY: a tid datum is a pointer to an ItemPointerData inside the
    // deformed plan tuple, live for this row.
    Some(unsafe { *(datum.as_usize() as *const ItemPointerData) })
}

// ExecMerge (nodeModifyTable.c).
fn exec_merge<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    tupleid: Option<ItemPointerData>,
    oldtup: Option<types_tuple::HeapTupleData<'mcx>>,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    let mut rslot = None;
    let mut matched = tupleid.is_some() || oldtup.is_some();
    if matched {
        let mut tid = tupleid.unwrap_or_default();
        rslot = exec_merge_matched(
            mt, estate, plan_slot, &mut tid, oldtup, &mut matched, epq_eval,
        )?;
    }
    if !matched {
        // A WHEN NOT MATCHED BY SOURCE action may have already produced the
        // RETURNING row for this source row (concurrent-update flip); the
        // NOT MATCHED [BY TARGET] action is deferred to the next call
        // (C 3013-3024, mt_merge_pending_not_matched).
        if rslot.is_none() {
            rslot = exec_merge_not_matched(mt, estate, plan_slot, epq_eval)?;
        } else {
            mt.mt_merge_pending_not_matched = Some(plan_slot);
        }
    }
    Ok(rslot)
}

enum MergeMatchedOutcome {
    // Action performed (or none qualified); RETURNING slot if projected.
    Done(Option<ExecSlotId>),
    // Concurrent update kept the row matched: restart the action scan.
    Restart,
    // Concurrent update/delete unmatched the row: caller runs NOT MATCHED.
    NotMatched,
}

// ExecMergeMatched (nodeModifyTable.c), lmerge_matched loop. The BY SOURCE
// list is empty on this lane, so an unmatched row goes straight back to the
// caller; the join condition is NULL (always true).
fn exec_merge_matched<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    tupleid: &mut ItemPointerData,
    oldtup: Option<types_tuple::HeapTupleData<'mcx>>,
    matched: &mut bool,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(*matched);
    {
        let m = mt.rel().merge.as_ref().expect("merge state");
        if m.matched_actions.is_empty() && m.not_matched_by_source_actions.is_empty() {
            return Ok(None);
        }
    }
    match oldtup {
        // View target: the wholerow junk attr is the old row (C 3040-3045).
        Some(old_tup) => {
            let old_id = mt.rel().ri_oldTupleSlot.expect("ExecInitMergeTupleSlots ran");
            let mcx = estate.es_query_cxt;
            exectuples::exec_force_store_heap_tuple(
                old_tup,
                &mut estate.es_tupleTable[old_id.0 as usize],
                mcx,
            )?;
        }
        None => fetch_old_row_version(mt, estate, tupleid)?,
    }

    // The retained join condition picks the action list: satisfied = MATCHED,
    // else NOT MATCHED BY SOURCE (C 3128-3139). A NULL condition (no BY
    // SOURCE actions) always passes. The choice is sticky across concurrent
    // -update restarts: only the recheck leg may switch it (C actionStates).
    let mut use_by_source = !merge_join_qual_passes(mt, estate, plan_slot)?;

    loop {
        match exec_merge_matched_scan(
            mt, estate, plan_slot, tupleid, &mut use_by_source, matched, epq_eval,
        )? {
            MergeMatchedOutcome::Done(rslot) => return Ok(rslot),
            MergeMatchedOutcome::Restart => continue,
            MergeMatchedOutcome::NotMatched => {
                *matched = false;
                return Ok(None);
            }
        }
    }
}

// The ri_MergeJoinCondition ExecQual (scan = old target tuple, inner = plan
// row); None (no BY SOURCE actions) evaluates true.
fn merge_join_qual_passes<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<bool> {
    let old_id = mt.rel().ri_oldTupleSlot.expect("ExecInitMergeTupleSlots ran");
    pre_eval_param_deps(
        mt.rel().merge.as_ref().expect("merge state").join_condition.as_deref(),
        estate,
    )?;
    let jc_subplans = mt
        .rel()
        .merge
        .as_ref()
        .expect("merge state")
        .join_condition
        .as_deref()
        .is_some_and(|q| q.has_subplan());
    let node_ecxt = mt.node_ecxt;
    let ModifyTableState { rels, cur, .. } = &mut *mt;
    let merge = rels[*cur].merge.as_mut().expect("merge state");
    match merge.join_condition.as_deref_mut() {
        None => Ok(true),
        Some(jc) => {
            if jc_subplans {
                let ec = node_ecxt.expect("node ecxt created with MERGE");
                estate.reset_expr_context(ec);
                {
                    let e = estate.ecxt_mut(ec);
                    e.ecxt_scantuple = Some(old_id);
                    e.ecxt_innertuple = Some(plan_slot);
                    e.ecxt_outertuple = None;
                }
                executils::exec_qual_with_subplans(Some(jc), estate, ec)
            } else {
                let EStateData { es_tupleTable, .. } = &mut *estate;
                let (o, p) = (old_id.0 as usize, plan_slot.0 as usize);
                assert!(o != p && o < es_tupleTable.len() && p < es_tupleTable.len());
                let base = es_tupleTable.as_mut_ptr();
                // SAFETY: distinct in-bounds indices of one live slice.
                let (old_slot, plan) = unsafe { (&mut *base.add(o), &mut *base.add(p)) };
                let mut slots =
                    EvalSlots { scan: Some(old_slot), inner: Some(plan), outer: None };
                execexpr::exec_qual(Some(jc), &mut slots)
            }
        }
    }
}

// =============================================================================
// MERGE action-dispatch seams — wave-5 WS-W §8.2.
//
// PURE CODE MOVES: the WHEN-qual action selection blocks of
// `exec_merge_matched_scan` / `exec_merge_not_matched` and the NOT MATCHED
// INSERT-action projection, relocated verbatim behind named seams so the
// MERGE dispatch has the same reviewable joints as the OC ceremony above.
// The action walk itself (list order, first-pass-wins, the per-action
// TM_Result arms) is UNTOUCHED. MERGE stays REFUSAL-ONLY in the lane even
// knob-ON (contract §8.2: the inc-3 C-side trace pin is outstanding);
// these seams serve the Volcano arm today and give the eventual MERGE
// increment its admission joints.
// =============================================================================

/// MERGE seam — WHEN [MATCHED | NOT MATCHED BY SOURCE] AND qual over one
/// action of the caller-selected list (`by_source` = the sticky
/// actionStates choice): scan = old target tuple, inner = plan row.
/// Returns the action's command type and whether its qual passed.
#[inline]
fn merge_when_qual_matched<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ai: usize,
    by_source: bool,
    plan_slot: ExecSlotId,
    old_id: ExecSlotId,
) -> PgResult<(CmdType, bool)> {
    let node_ecxt = mt.node_ecxt;
    {
        let merge = mt.rel().merge.as_ref().expect("merge state");
        let action = if by_source {
            &merge.not_matched_by_source_actions[ai]
        } else {
            &merge.matched_actions[ai]
        };
        pre_eval_param_deps(action.when_qual.as_deref(), estate)?;
    }
    let merge = mt.rel_mut().merge.as_mut().expect("merge state");
    let action = if by_source {
        &mut merge.not_matched_by_source_actions[ai]
    } else {
        &mut merge.matched_actions[ai]
    };
    if action.when_qual.as_deref().is_some_and(|q| q.has_subplan()) {
        let ec = node_ecxt.expect("node ecxt created with MERGE");
        estate.reset_expr_context(ec);
        {
            let e = estate.ecxt_mut(ec);
            e.ecxt_scantuple = Some(old_id);
            e.ecxt_innertuple = Some(plan_slot);
            e.ecxt_outertuple = None;
        }
        Ok((
            action.command_type,
            executils::exec_qual_with_subplans(
                action.when_qual.as_deref_mut(),
                estate,
                ec,
            )?,
        ))
    } else {
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (o, p) = (old_id.0 as usize, plan_slot.0 as usize);
        assert!(o != p && o < es_tupleTable.len() && p < es_tupleTable.len());
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (old_slot, plan) = unsafe { (&mut *base.add(o), &mut *base.add(p)) };
        let mut slots =
            EvalSlots { scan: Some(old_slot), inner: Some(plan), outer: None };
        Ok((
            action.command_type,
            execexpr::exec_qual(action.when_qual.as_deref_mut(), &mut slots)?,
        ))
    }
}

/// MERGE seam — WHEN NOT MATCHED [BY TARGET] AND qual over one action of
/// the not_matched list: no old tuple (scan = None), inner = plan row.
/// Returns the action's command type and whether its qual passed.
#[inline]
fn merge_when_qual_not_matched<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ai: usize,
    plan_slot: ExecSlotId,
) -> PgResult<(CmdType, bool)> {
    let node_ecxt = mt.node_ecxt;
    {
        let merge = mt.rel().merge.as_ref().expect("merge state");
        pre_eval_param_deps(merge.not_matched_actions[ai].when_qual.as_deref(), estate)?;
    }
    let merge = mt.rel_mut().merge.as_mut().expect("merge state");
    let action = &mut merge.not_matched_actions[ai];
    if action.when_qual.as_deref().is_some_and(|q| q.has_subplan()) {
        let ec = node_ecxt.expect("node ecxt created with MERGE");
        estate.reset_expr_context(ec);
        {
            let e = estate.ecxt_mut(ec);
            e.ecxt_scantuple = None;
            e.ecxt_innertuple = Some(plan_slot);
            e.ecxt_outertuple = None;
        }
        Ok((
            action.command_type,
            executils::exec_qual_with_subplans(
                action.when_qual.as_deref_mut(),
                estate,
                ec,
            )?,
        ))
    } else {
        let plan = &mut estate.es_tupleTable[plan_slot.0 as usize];
        let mut slots = EvalSlots { scan: None, inner: Some(plan), outer: None };
        Ok((
            action.command_type,
            execexpr::exec_qual(action.when_qual.as_deref_mut(), &mut slots)?,
        ))
    }
}

/// MERGE seam — the NOT MATCHED INSERT action's merge-action projection
/// (the matched-side counterpart is the pre-existing
/// `merge_project_update`): project the plan row through the action's
/// projection into `new_id` (the root-format new slot when the target is
/// inherited/partitioned).
#[inline]
fn merge_project_not_matched<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ai: usize,
    plan_slot: ExecSlotId,
    new_id: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let node_ecxt = mt.node_ecxt;
    {
        let merge = mt.rel().merge.as_ref().expect("merge state");
        pre_eval_param_deps(merge.not_matched_actions[ai].proj.as_deref(), estate)?;
    }
    let merge = mt.rel_mut().merge.as_mut().expect("merge state");
    let action = &mut merge.not_matched_actions[ai];
    if action.proj.as_deref().is_some_and(|p| p.has_subplan()) {
        let ec = node_ecxt.expect("node ecxt created with MERGE");
        estate.reset_expr_context(ec);
        {
            let e = estate.ecxt_mut(ec);
            e.ecxt_scantuple = None;
            e.ecxt_innertuple = Some(plan_slot);
            e.ecxt_outertuple = None;
        }
        let proj =
            action.proj.as_deref_mut().expect("INSERT action projection");
        executils::exec_project_with_subplans(proj, estate, ec, new_id)?;
    } else {
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (p, n) = (plan_slot.0 as usize, new_id.0 as usize);
        assert!(p != n && p < es_tupleTable.len() && n < es_tupleTable.len());
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (plan, new_slot) =
            unsafe { (&mut *base.add(p), &mut *base.add(n)) };
        let mut slots =
            EvalSlots { scan: None, inner: Some(plan), outer: None };
        let proj = action.proj.as_deref_mut().expect("INSERT action projection");
        execexpr::exec_project(proj, &mut slots, new_slot, mcx)?;
    }
    Ok(())
}

// One pass over the MATCHED (or NOT MATCHED BY SOURCE) action list — the
// lmerge_matched body. `use_by_source` is the caller-held actionStates
// choice; the concurrent-update recheck leg may flip it to BY SOURCE.
fn exec_merge_matched_scan<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    tupleid: &mut ItemPointerData,
    use_by_source: &mut bool,
    matched: &mut bool,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<MergeMatchedOutcome> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let old_id = mt.rel().ri_oldTupleSlot.expect("ExecInitMergeTupleSlots ran");
    let new_id = mt.rel().ri_newTupleSlot.expect("ExecInitMergeTupleSlots ran");
    let by_source = *use_by_source;

    let n_actions = {
        let m = mt.rel().merge.as_ref().expect("merge state");
        if by_source {
            m.not_matched_by_source_actions.len()
        } else {
            m.matched_actions.len()
        }
    };
    for ai in 0..n_actions {
        // WHEN [MATCHED] AND qual: scan = old target tuple, inner = plan row.
        let (command_type, pass) =
            merge_when_qual_matched(mt, estate, ai, by_source, plan_slot, old_id)?;
        if !pass {
            continue;
        }

        // The existing target row must pass the USING checks of UPDATE/DELETE
        // RLS policies, checked only after the WHEN qual (C 3159-3178).
        if command_type != CmdType::CMD_NOTHING && !mt.rel().wco_exprs.is_empty() {
            let kind = if command_type == CmdType::CMD_UPDATE {
                WCOKind::WCO_RLS_MERGE_UPDATE_CHECK
            } else {
                WCOKind::WCO_RLS_MERGE_DELETE_CHECK
            };
            let ecxt = mt.node_ecxt;
            let ModifyTableState { rels, cur, .. } = &mut *mt;
            let r = &mut rels[*cur];
            exec_with_check_options(estate, ecxt, &mut r.wco_exprs, kind, old_id)?;
        }

        mt.merge_active_cmd = Some(command_type);
        let mut tmfd = TM_FailureData::default();
        let result = match command_type {
            CmdType::CMD_UPDATE if mt
                .rel()
                .trigdesc
                .as_ref()
                .is_some_and(|td| td.trig_update_instead_row) =>
            {
                // INSTEAD OF ROW UPDATE triggers on a view target
                // (C 3202-3213); the epilogue's WCO_VIEW_CHECK still applies.
                merge_project_update(mt, estate, ai, by_source, plan_slot)?;
                if !ir_row_triggers(
                    mt,
                    estate,
                    types_trigger::TRIGGER_TYPE_UPDATE,
                    types_trigger::TRIGGER_EVENT_UPDATE,
                    Some(old_id),
                    Some(new_id),
                )? {
                    return Ok(MergeMatchedOutcome::Done(None));
                }
                if !mt.rel().wco_exprs.is_empty() {
                    let ecxt = mt.node_ecxt;
                    let r = &mut mt.rels[mt.cur];
                    let rti = r.rti;
                    exec_view_check_options(
                        mcx,
                        estate,
                        ecxt,
                        &mut r.wco_exprs,
                        new_id,
                        WcoRel::Rti { rti, root_rti: None },
                    )?;
                }
                TM_Result::TM_Ok
            }
            CmdType::CMD_DELETE if mt
                .rel()
                .trigdesc
                .as_ref()
                .is_some_and(|td| td.trig_delete_instead_row) =>
            {
                // INSTEAD OF ROW DELETE triggers on a view target (C 3255-3266).
                if !ir_row_triggers(
                    mt,
                    estate,
                    types_trigger::TRIGGER_TYPE_DELETE,
                    types_trigger::TRIGGER_EVENT_DELETE,
                    Some(old_id),
                    None,
                )? {
                    return Ok(MergeMatchedOutcome::Done(None));
                }
                TM_Result::TM_Ok
            }
            CmdType::CMD_UPDATE => {
                merge_project_update(mt, estate, ai, by_source, plan_slot)?;
                // ExecUpdatePrologue: BEFORE ROW UPDATE triggers; a NULL
                // return is C's "do nothing" (goto out, no count). A
                // concurrent update seen by the trigger fetch breaks to the
                // TM_Updated recheck below (C's prologue-false + break).
                let mut trig_tm = None;
                if mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_update_before_row) {
                    match merge_tuple_for_trigger(mt, estate, tupleid)? {
                        MergeTrigFetch::Fetched(trig_old) => {
                            if !br_row_triggers(
                                mt,
                                estate,
                                types_trigger::TRIGGER_TYPE_UPDATE,
                                types_trigger::TRIGGER_EVENT_UPDATE,
                                Some(trig_old),
                                Some(new_id),
                                None,
                            )? {
                                return Ok(MergeMatchedOutcome::Done(None));
                            }
                        }
                        MergeTrigFetch::SelfModified(fd) => {
                            return Err(merge_self_modified(&fd, output_cid));
                        }
                        MergeTrigFetch::Deleted => {
                            return Ok(MergeMatchedOutcome::NotMatched);
                        }
                        MergeTrigFetch::Updated(fd) => {
                            tmfd = fd;
                            trig_tm = Some(TM_Result::TM_Updated);
                        }
                    }
                }
                if let Some(r) = trig_tm {
                    r
                } else {
                    match merge_update_act(mt, estate, tupleid, new_id, &mut tmfd, &mut *epq_eval)? {
                        MergeUpdActRes::Tm(r) => r,
                        // C ExecMergeMatched crossPartUpdate leg: the INSERT half
                        // counted the row; RETURNING reports the inserted row
                        // (cpUpdateReturningSlot).
                        MergeUpdActRes::CrossPart(inserted) => {
                            mt.mt_merge_updated += 1.0;
                            let mut rslot = None;
                            if let Some(islot) = inserted {
                                if mt.rel().project_returning.is_some() {
                                    rslot = Some(exec_cross_part_returning(
                                        mt,
                                        estate,
                                        Some(old_id),
                                        islot,
                                        plan_slot,
                                    )?);
                                }
                            }
                            return Ok(MergeMatchedOutcome::Done(rslot));
                        }
                    }
                }
            }
            CmdType::CMD_DELETE => {
                // ExecDeletePrologue: BEFORE ROW DELETE triggers.
                let mut trig_tm = None;
                if mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_delete_before_row) {
                    match merge_tuple_for_trigger(mt, estate, tupleid)? {
                        MergeTrigFetch::Fetched(trig_old) => {
                            if !br_row_triggers(
                                mt,
                                estate,
                                types_trigger::TRIGGER_TYPE_DELETE,
                                types_trigger::TRIGGER_EVENT_DELETE,
                                Some(trig_old),
                                None,
                                None,
                            )? {
                                return Ok(MergeMatchedOutcome::Done(None));
                            }
                        }
                        MergeTrigFetch::SelfModified(fd) => {
                            return Err(merge_self_modified(&fd, output_cid));
                        }
                        MergeTrigFetch::Deleted => {
                            return Ok(MergeMatchedOutcome::NotMatched);
                        }
                        MergeTrigFetch::Updated(fd) => {
                            tmfd = fd;
                            trig_tm = Some(TM_Result::TM_Updated);
                        }
                    }
                }
                if let Some(r) = trig_tm {
                    r
                } else {
                    merge_delete_act(mt, estate, tupleid, &mut tmfd)?
                }
            }
            CmdType::CMD_NOTHING => TM_Result::TM_Ok,
            other => panic!("unknown action in MERGE WHEN clause: {other:?}"),
        };

        match result {
            TM_Result::TM_Ok => {
                match command_type {
                    CmdType::CMD_UPDATE => mt.mt_merge_updated += 1.0,
                    CmdType::CMD_DELETE => mt.mt_merge_deleted += 1.0,
                    _ => {}
                }
                if mt.canSetTag && command_type != CmdType::CMD_NOTHING {
                    estate.es_processed += 1;
                }
            }
            TM_Result::TM_SelfModified => {
                return Err(merge_self_modified(&tmfd, output_cid));
            }
            TM_Result::TM_Deleted => {
                if xact::IsolationUsesXactSnapshot() {
                    return Err(serialization_conflict("delete"));
                }
                return Ok(MergeMatchedOutcome::NotMatched);
            }
            TM_Result::TM_Updated => {
                if xact::IsolationUsesXactSnapshot() {
                    return Err(serialization_conflict("update"));
                }
                // Concurrent update: lock the latest version. A MATCHED
                // action then rechecks the join via EvalPlanQual; a NOT
                // MATCHED BY SOURCE action relocks in place and restarts
                // (cannot switch back to MATCHED) — C 3358-3375.
                let was_matched = !by_source;
                let inputslot =
                    if was_matched { eval_plan_qual_slot(mt, estate) } else { old_id };
                let lock_result = {
                    let EStateData { es_relations, es_tupleTable, es_snapshot, .. } =
                        &mut *estate;
                    let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
                    let rel = es_relations[(mt.rel().rti - 1) as usize]
                        .as_ref()
                        .expect("result relation opened");
                    tableam::table_tuple_lock(
                        mcx,
                        rel,
                        tupleid,
                        snapshot,
                        &mut es_tupleTable[inputslot.0 as usize],
                        output_cid,
                        LockTupleMode::LockTupleExclusive,
                        LockWaitPolicy::LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut tmfd,
                    )?
                };
                match lock_result {
                    TM_Result::TM_Ok => {
                        *tupleid = estate.slot(inputslot).base().tts_tid;
                        // The current MERGE implementation can't follow a
                        // row moved to another partition (C 3395-3402).
                        if types_tuple::ItemPointerIndicatesMovedPartitions(tupleid) {
                            return Err(Box::new(
                                PgError::error(
                                    "tuple to be merged was already moved to another \
                                     partition due to concurrent update",
                                )
                                .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            ));
                        }
                        if was_matched {
                            let rti = mt.rel().rti;
                            let Some(epqslot) =
                                epq_eval(&mut mt.epq_subs, estate, inputslot, rti)?
                            else {
                                // Inner join no longer matches and there are
                                // no NOT MATCHED actions reachable through it.
                                return Ok(MergeMatchedOutcome::Done(None));
                            };
                            // A NULL ctid junk attr from the recheck subplan
                            // means the join quals no longer pass: switch to
                            // the NOT MATCHED BY SOURCE case (C 3427-3436).
                            let mut isnull = false;
                            let _ = exectuples::slot_getattr(
                                &mut estate.es_tupleTable[epqslot.0 as usize],
                                mt.rel().ri_RowIdAttNo as i32,
                                &mut isnull,
                            );
                            if isnull {
                                *matched = false;
                            }
                            fetch_old_row_version(mt, estate, tupleid)?;
                            if *matched {
                                *matched = merge_join_qual_passes(mt, estate, plan_slot)?;
                            }
                            if !*matched {
                                *use_by_source = true;
                                // The one previously matched outer row now
                                // counts as two not-matched ones
                                // (InstrUpdateTupleCount leg, C 3452-3465).
                                if let Some(idx) = mt.outer_instr_idx {
                                    let m = mt.rel().merge.as_ref().expect("merge state");
                                    if !m.not_matched_by_source_actions.is_empty()
                                        && !m.not_matched_actions.is_empty()
                                    {
                                        ::instrument::instr_update_tuple_count(
                                            &mut estate.es_instrumentation[idx as usize],
                                            1.0,
                                        );
                                    }
                                }
                            }
                        }
                        return Ok(MergeMatchedOutcome::Restart);
                    }
                    TM_Result::TM_Deleted => return Ok(MergeMatchedOutcome::NotMatched),
                    TM_Result::TM_SelfModified => {
                        return Err(merge_self_modified(&tmfd, output_cid));
                    }
                    other => panic!(
                        "ExecMergeMatched (nodeModifyTable.c): unexpected \
                         table_tuple_lock status: {other:?}"
                    ),
                }
            }
            other => panic!(
                "ExecMergeMatched (nodeModifyTable.c): unexpected tuple operation \
                 result: {other:?}"
            ),
        }

        // One WHEN clause activated; stop scanning (required behaviour).
        let mut rslot = None;
        if mt.rel().project_returning.is_some() {
            rslot = match command_type {
                CmdType::CMD_UPDATE => Some(exec_process_returning(
                    mt,
                    estate,
                    CmdType::CMD_UPDATE,
                    Some(old_id),
                    Some(new_id),
                    plan_slot,
                )?),
                CmdType::CMD_DELETE => Some(exec_process_returning(
                    mt,
                    estate,
                    CmdType::CMD_DELETE,
                    Some(old_id),
                    None,
                    plan_slot,
                )?),
                _ => None,
            };
        }
        return Ok(MergeMatchedOutcome::Done(rslot));
    }
    Ok(MergeMatchedOutcome::Done(None))
}

// The UPDATE action's ExecProject: evaluate the SET exprs (scan = old tuple,
// inner = plan row) into the action's setvals slot, then overlay them onto
// the old tuple at set_attnos into ri_newTupleSlot.
fn merge_project_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    action_idx: usize,
    by_source: bool,
    plan_slot: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let old_id = mt.rel().ri_oldTupleSlot.expect("merge slots");
    let new_id = mt.rel().ri_newTupleSlot.expect("merge slots");
    let node_ecxt = mt.node_ecxt;
    let merge = mt.rel_mut().merge.as_mut().expect("merge state");
    let action = if by_source {
        &mut merge.not_matched_by_source_actions[action_idx]
    } else {
        &mut merge.matched_actions[action_idx]
    };
    let setvals_id = action.setvals_slot.expect("UPDATE action state");

    pre_eval_param_deps(action.proj.as_deref(), estate)?;
    if action.proj.as_deref().is_some_and(|p| p.has_subplan()) {
        let ec = node_ecxt.expect("node ecxt created with MERGE");
        estate.reset_expr_context(ec);
        {
            let e = estate.ecxt_mut(ec);
            e.ecxt_scantuple = Some(old_id);
            e.ecxt_innertuple = Some(plan_slot);
            e.ecxt_outertuple = None;
        }
        let proj = action.proj.as_deref_mut().expect("UPDATE action projection");
        executils::exec_project_with_subplans(proj, estate, ec, setvals_id)?;
    } else {
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (o, p, v) = (old_id.0 as usize, plan_slot.0 as usize, setvals_id.0 as usize);
        assert!(o != p && o != v && p != v);
        assert!(o < es_tupleTable.len() && p < es_tupleTable.len() && v < es_tupleTable.len());
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (old_slot, plan, setvals) =
            unsafe { (&mut *base.add(o), &mut *base.add(p), &mut *base.add(v)) };
        let mut slots = EvalSlots { scan: Some(old_slot), inner: Some(plan), outer: None };
        let proj = action.proj.as_deref_mut().expect("UPDATE action projection");
        execexpr::exec_project(proj, &mut slots, setvals, mcx)?;
    }

    {
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (o, v, n) = (old_id.0 as usize, setvals_id.0 as usize, new_id.0 as usize);
        assert!(o != v && o != n && v != n);
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (old_slot, setvals, new_slot) =
            unsafe { (&mut *base.add(o), &mut *base.add(v), &mut *base.add(n)) };
        exectuples::slot_getallattrs(old_slot);
        exectuples::slot_getallattrs(setvals);
        exectuples::exec_clear_tuple(new_slot, mcx);
        {
            let (ob, vb) = (old_slot.base(), setvals.base());
            let nb = new_slot.base_mut();
            let natts = ob.tts_nvalid as usize;
            nb.tts_values[..natts].copy_from_slice(&ob.tts_values[..natts]);
            nb.tts_isnull[..natts].copy_from_slice(&ob.tts_isnull[..natts]);
            for (i, &attno) in action.set_attnos.iter().enumerate() {
                nb.tts_values[attno as usize - 1] = vb.tts_values[i];
                nb.tts_isnull[attno as usize - 1] = vb.tts_isnull[i];
            }
        }
        exectuples::exec_store_virtual_tuple(new_slot);
    }
    Ok(())
}

// ExecUpdateAct's MERGE outcome: a cross-partition move carries the INSERT
// half's result slot (C updateCxt->crossPartUpdate + cpUpdateReturningSlot).
enum MergeUpdActRes {
    Tm(TM_Result),
    CrossPart(Option<ExecSlotId>),
}

// ExecUpdateAct + ExecUpdateEpilogue for a MERGE UPDATE action; unlike
// exec_update the TM_Result flows back so lmerge_matched drives the retry.
fn merge_update_act<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    slot_id: ExecSlotId,
    tmfd: &mut TM_FailureData,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<MergeUpdActRes> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let mut lockmode = LockTupleMode::LockTupleExclusive;
    let mut update_indexes = TU_UpdateIndexes::TU_None;

    let mut cross_part = false;
    {
        let EStateData { es_relations, es_tupleTable, es_range_table, es_rteperminfos, .. } =
            &mut *estate;
        let rel = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &mut es_tupleTable[slot_id.0 as usize];

        slot.base_mut().tts_tableOid = rel.rd_id;
        if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
            exec_compute_stored_generated(mcx, &mut mt.rel_mut().generated_exprs, rel, slot)?;
        }
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;

        // C shares ExecUpdateAct with MERGE; same direct-leaf partition
        // constraint enforcement as exec_update.
        if rel.rd_rel.relispartition
            && !execpartition::exec_partition_check(mcx, &mut mt.rel_mut().partition_check, rel, slot)?
        {
            if mt.root.is_none() {
                let mod_cols = rte_modified_cols(
                    mcx,
                    &es_range_table[..],
                    *es_rteperminfos,
                    mt.rels[mt.cur].rti,
                )?;
                return Err(execpartition::partition_constraint_violation(
                    mcx,
                    rel,
                    slot,
                    Some(&mod_cols),
                    None,
                ));
            }
            // ExecCrossPartitionUpdate: DELETE here + re-routed INSERT,
            // performed outside this borrow scope.
            cross_part = true;
        }
    }
    // The WITH CHECK quals of UPDATE RLS policies apply to the NEW row here,
    // exactly as in ExecUpdateAct (C 2210-2213), only when the partition
    // constraint passed. Policy quals carry SubPlans/initplans (executils
    // subplan driver).
    if !cross_part && !mt.rel().wco_exprs.is_empty() {
        let ecxt = mt.node_ecxt;
        let r = mt.rel_mut();
        exec_with_check_options(estate, ecxt, &mut r.wco_exprs, WCOKind::WCO_RLS_UPDATE_CHECK, slot_id)?;
    }
    let result = if cross_part {
        TM_Result::TM_Ok
    } else {
        let EStateData { es_relations, es_tupleTable, es_snapshot, es_crosscheck_snapshot, es_range_table, es_rteperminfos, .. } =
            &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let crosscheck: &tableam_vocab::Snapshot<'mcx> = &*es_crosscheck_snapshot;
        let rel = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &mut es_tupleTable[slot_id.0 as usize];

        if rel.rd_rel.relhasindex && mt.rel_mut().indexes.is_none() {
            mt.rel_mut().indexes = Some(execindexing::ExecOpenIndices(mcx, rel, false)?);
        }

        {
            // ExecConstraints (execMain.c): partition children report through
            // the root rel + root perminfo (ri_RootResultRelInfo leg).
            let (perm_rti, err_root_rel) = match &mt.root {
                Some(rr) => (rr.rti, es_relations[(rr.rti - 1) as usize].as_ref()),
                None => (mt.rels[mt.cur].rti, None),
            };
            let mod_cols =
                rte_modified_cols(mcx, &es_range_table[..], *es_rteperminfos, perm_rti)?;
            let r = &mut mt.rels[mt.cur];
            exec_constraints(
                mcx,
                &mut r.check_exprs,
                &mut r.virtual_nn_exprs,
                rel,
                slot,
                err_root_rel,
                Some(&mod_cols),
            )?;
        }

        tableam::table_tuple_update(
            mcx,
            rel,
            tupleid,
            slot,
            output_cid,
            snapshot,
            crosscheck,
            true,
            tmfd,
            &mut lockmode,
            &mut update_indexes,
        )?
    };
    if cross_part {
        return match exec_cross_partition_update(mt, estate, tupleid, slot_id, epq_eval)? {
            CrossPartResult::Done(inserted) => Ok(MergeUpdActRes::CrossPart(inserted)),
            CrossPartResult::MergeConcurrent(tm, fd) => {
                *tmfd = fd;
                Ok(MergeUpdActRes::Tm(tm))
            }
            CrossPartResult::Retry(_) => {
                unreachable!("MERGE cross-partition concurrency surfaces as MergeConcurrent")
            }
        };
    }
    if result != TM_Result::TM_Ok {
        return Ok(MergeUpdActRes::Tm(result));
    }

    let EStateData { es_relations, es_tupleTable, .. } = estate;
    let rel = es_relations[(mt.rel().rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let slot = &mut es_tupleTable[slot_id.0 as usize];
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    let ModifyTableState { rels, cur, index_eval_cx, .. } = &mut *mt;
    if let Some(indexes) = rels[*cur].indexes.as_mut() {
        if indexes.num_indices() > 0 && update_indexes != TU_UpdateIndexes::TU_None {
            recheck_indexes = execindexing::ExecInsertIndexTuples(
                mcx,
                index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                indexes,
                rel,
                slot,
                false,
                None,
                &[],
                update_indexes == TU_UpdateIndexes::TU_Summarizing,
            )?;
        }
    }
    let ar_new_tid = slot.base().tts_tid;
    if let Some(td) = mt.rel().trigdesc.clone() {
        // Unconditional (C ExecARUpdateTriggers → ExecGetAllUpdatedCols):
        // every queued UPDATE event carries ats_modifiedcols, not just
        // UPDATE-OF filters.
        ensure_all_updated_cols(mt, estate, false)?;
        let result_rti = mt.rel().rti;
        ensure_child_to_root(mt, estate)?;
        let root_rti = mt.root.as_ref().map(|rr| rr.rti);
        let ModifyTableState {
            rels, cur, transition_capture, oc_transition_capture, operation, ..
        } = mt;
        let r = &mut rels[*cur];
        // ON CONFLICT DO UPDATE (operation == INSERT) captures into the
        // UPDATE tables via mt_oc_transition_capture (C ExecOnConflictUpdate).
        let tc = if *operation == CmdType::CMD_INSERT {
            oc_transition_capture.as_ref()
        } else {
            transition_capture.as_ref()
        };
        let modified_cols = r.all_updated_cols.as_ref();
        let mut when = ::trigger::TriggerWhenEval {
            mcx,
            cache: &mut r.trig_when,
            modified_cols,
        };
        let rel = estate.es_relations[(result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let root_rel = root_rti.map(|rti| {
            estate.es_relations[(rti - 1) as usize]
                .as_ref()
                .expect("root relation opened")
        });
        let conv = child_to_root_spec(&r.child_to_root, rel, root_rel);
        ::trigger::ExecARUpdateTriggers(
            mcx, rel, Some(&td), None, None, Some(*tupleid), Some(ar_new_tid),
            &recheck_indexes, tc, Some(&mut when), false, conv.as_ref(), conv.as_ref(),
            modified_cols,
        )?;
    }

    // Parent-view CHECK OPTIONs are checked after updating (the qual must see
    // the actual row, post defaults/triggers) — C's shared WCO_VIEW_CHECK leg.
    if !mt.rel().wco_exprs.is_empty() {
        let ecxt = mt.node_ecxt;
        let root_rti = mt.root.as_ref().map(|rr| rr.rti);
        let r = &mut mt.rels[mt.cur];
        let rti = r.rti;
        exec_view_check_options(
            mcx,
            estate,
            ecxt,
            &mut r.wco_exprs,
            slot_id,
            WcoRel::Rti { rti, root_rti },
        )?;
    }
    Ok(MergeUpdActRes::Tm(TM_Result::TM_Ok))
}

// ExecDeleteAct + ExecDeleteEpilogue for a MERGE DELETE action.
fn merge_delete_act<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
    tmfd: &mut TM_FailureData,
) -> PgResult<TM_Result> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let result = {
        let EStateData { es_relations, es_snapshot, es_crosscheck_snapshot, .. } = &*estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = es_snapshot;
        let crosscheck: &tableam_vocab::Snapshot<'mcx> = es_crosscheck_snapshot;
        let rel = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        tableam::table_tuple_delete(
            mcx, rel, tupleid, output_cid, snapshot, crosscheck, true, tmfd, false,
        )?
    };
    if result != TM_Result::TM_Ok {
        return Ok(result);
    }
    let delete_capture =
        mt.transition_capture.as_ref().is_some_and(|tc| tc.tcs_delete_old_table);
    if mt.rel().trigdesc.is_some() || delete_capture {
        let td = mt.rel().trigdesc.clone();
        let result_rti = mt.rel().rti;
        ensure_child_to_root(mt, estate)?;
        let root_rti = mt.root.as_ref().map(|rr| rr.rti);
        let ModifyTableState { rels, cur, transition_capture, .. } = mt;
        let r = &mut rels[*cur];
        let EStateData { es_relations, es_query_cxt, .. } = &*estate;
        let rel = es_relations[(result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let root_rel = root_rti.map(|rti| {
            es_relations[(rti - 1) as usize].as_ref().expect("root relation opened")
        });
        let conv = child_to_root_spec(&r.child_to_root, rel, root_rel);
        let mut when = ::trigger::TriggerWhenEval {
            mcx: *es_query_cxt,
            cache: &mut r.trig_when,
            modified_cols: None,
        };
        ::trigger::ExecARDeleteTriggers(
            *es_query_cxt, rel, td.as_deref(), *tupleid, transition_capture.as_ref(),
            Some(&mut when), false, conv.as_ref(),
        )?;
    }
    Ok(TM_Result::TM_Ok)
}

// ExecMergeNotMatched (nodeModifyTable.c): first qualifying NOT MATCHED [BY
// TARGET] action; INSERT projects from the source row alone (no scan tuple).
fn exec_merge_not_matched<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    // INSERT actions project into and insert via the root relation when the
    // target is inherited/partitioned (C rootRelInfo).
    let new_id = if mt.root.is_some() {
        mt.root_rel().ri_newTupleSlot.expect("ExecInitMerge built the root new slot")
    } else {
        mt.rel().ri_newTupleSlot.expect("ExecInitMergeTupleSlots ran")
    };
    let n_actions = mt.rel().merge.as_ref().expect("merge state").not_matched_actions.len();
    for ai in 0..n_actions {
        // WHEN NOT MATCHED AND qual: no old tuple, inner = plan row.
        let (command_type, pass) = merge_when_qual_not_matched(mt, estate, ai, plan_slot)?;
        if !pass {
            continue;
        }
        match command_type {
            CmdType::CMD_INSERT => {
                merge_project_not_matched(mt, estate, ai, plan_slot, new_id)?;
                mt.merge_active_cmd = Some(CmdType::CMD_INSERT);
                mt.insert_target_root = mt.root.is_some();
                let inserted = exec_insert(mt, estate, new_id, epq_eval);
                mt.mt_merge_inserted += 1.0;
                let inserted = match inserted {
                    Ok(v) => v,
                    Err(e) => {
                        mt.insert_target_root = false;
                        return Err(e);
                    }
                };
                if let Some(islot) = inserted {
                    // Root INSERTs project RETURNING over the root layout
                    // (C rootRelInfo ri_projectReturning, built lazily here
                    // from returningLists[0] as in exec_init_root_returning).
                    let init = if mt.insert_target_root {
                        exec_init_root_returning(mt, estate)
                    } else {
                        Ok(())
                    };
                    if let Err(e) = init {
                        mt.insert_target_root = false;
                        return Err(e);
                    }
                    if mt.rel().project_returning.is_some() {
                        let out = exec_process_returning(
                            mt,
                            estate,
                            CmdType::CMD_INSERT,
                            None,
                            Some(islot),
                            plan_slot,
                        );
                        mt.insert_target_root = false;
                        return Ok(Some(out?));
                    }
                }
                mt.insert_target_root = false;
            }
            CmdType::CMD_NOTHING => {}
            other => panic!("unknown action in MERGE WHEN NOT MATCHED clause: {other:?}"),
        }
        return Ok(None);
    }
    Ok(None)
}

#[track_caller]
#[cold]
#[inline(never)]
fn merge_self_modified(
    tmfd: &TM_FailureData,
    output_cid: types_core::CommandId,
) -> Box<PgError> {
    if tmfd.cmax != output_cid {
        return Box::new(
            PgError::error(
                "tuple to be updated or deleted was already modified by an operation \
                 triggered by the current command",
            )
            .with_sqlstate(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
            .with_hint(
                "Consider using an AFTER trigger instead of a BEFORE trigger to \
                 propagate changes to other rows.",
            ),
        );
    }
    if xact::TransactionIdIsCurrentTransactionId(tmfd.xmax) {
        return Box::new(
            PgError::error("MERGE command cannot affect row a second time")
                .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
                .with_hint(
                    "Ensure that not more than one source row matches any one \
                     target row.",
                ),
        );
    }
    Box::new(PgError::error("attempted to update or delete invisible tuple".to_string()))
}

/// `ExecEndModifyTable` node-local half; the caller ends the subplan.
pub fn exec_end_modify_table(mt: &mut ModifyTableState<'_>) {
    for r in mt.rels.iter_mut().chain(mt.root.iter_mut()) {
        if let Some(indexes) = r.indexes.take() {
            execindexing::ExecCloseIndices(indexes).expect("ExecCloseIndices");
        }
        r.project_returning = None;
        r.check_exprs = None;
        r.partition_check = None;
        r.wco_exprs.clear();
        r.trigdesc = None;
        r.trig_fmgr = ::trigger::TriggerFmgrCache::default();
        // trig_when's compiled-WHEN cache and scratch slots are std-backed;
        // the struct is forgotten, so drop them here like trig_fmgr's.
        r.trig_when = ::trigger::TriggerWhenCache::default();
        r.child_to_root = None;
        r.generated_exprs = None;
        r.virtual_nn_exprs = None;
        r.merge = None;
    }
    mt.snapshot_any = None;
    mt.on_conflict = None;
    // ExecCleanupTupleRouting: close routed leaves (Relation Drop = NoLock
    // close, lock kept to commit as C) and their per-leaf insert state.
    for idx in mt.leaf_indexes.iter_mut() {
        if let Some(indexes) = idx.take() {
            execindexing::ExecCloseIndices(indexes).expect("ExecCloseIndices");
        }
    }
    mt.leaf_indexes.clear();
    mt.leaf_checks.clear();
    mt.leaf_virtual_nn.clear();
    mt.leaf_generated.clear();
    mt.leaf_slots.clear();
    mt.leaf_partition_check.clear();
    mt.leaf_arbiters.clear();
    mt.leaf_existing.clear();
    mt.leaf_child_to_root.clear();
    mt.leaf_wco.clear();
    mt.leaf_on_conflict.clear();
    mt.leaf_returning.clear();
    mt.leaf_ri_checked.clear();
    // These hold non-arena state (Rc trigdesc clones, std-backed fmgr/WHEN
    // caches); the planstate is forgotten, so their element drops must run
    // here (the PgVec buffers themselves are arena-backed and forget-safe).
    mt.leaf_trigdesc.clear();
    mt.leaf_trig_fmgr.clear();
    mt.leaf_trig_when.clear();
    mt.router = None;
    mt.index_eval_cx = None;
}

// ExecInitInsertProjection (nodeModifyTable.c). INSERT subplans carry no junk
// columns on this lane (loud below), so need_projection is always false and
// ri_newTupleSlot only exists for slot-type coercion.
#[inline(always)] // se2-cost-fix round 3: the round-2 plain hint did not take (+139/stmt outlined)
fn exec_init_insert_projection<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let subplan = mt
        .plan
        .plan
        .lefttree
        .expect("ModifyTable has a subplan")
        .as_plan()
        .expect("plan node");
    for tle_node in &subplan.targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.resjunk {
            panic!(
                "ExecInitInsertProjection (nodeModifyTable.c): junk-column \
                 projection (ExecBuildProjectionInfo) not ported"
            );
        }
    }

    let mcx = estate.es_query_cxt;
    let (kind, desc) = {
        let rel = estate.es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        exec_check_plan_output(rel, &subplan.targetlist)?;
        (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
    };
    let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
    let id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(slot);
    mt.rel_mut().ri_newTupleSlot = Some(id);
    mt.rel_mut().ri_projectNewInfoValid = true;
    Ok(())
}

// ExecCheckPlanOutput (execMain.c), non-junk arm.
fn exec_check_plan_output<'mcx>(
    rel: &Relation<'mcx>,
    tlist: &types_nodes::NodeList<'mcx>,
) -> PgResult<()> {
    let desc = &rel.rd_att;
    let mut attno = 0usize;
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        debug_assert!(!tle.resjunk);
        if attno >= desc.natts as usize {
            return Err(plan_output_mismatch("Query has too many columns."));
        }
        let att = desc.attr(attno);
        attno += 1;
        // Special cases here match the planner's expand_insert_targetlist.
        if att.attisdropped {
            if tle.expr.node_tag() != NodeTag::T_Const
                || !tle.expr.as_const().unwrap().constisnull
            {
                return Err(plan_output_mismatch(format!(
                    "Query provides a value for a dropped column at ordinal position {attno}."
                )));
            }
        } else if att.attgenerated != 0 {
            // The planner inserted a null of the column's base type; a null
            // is type-independent, so only insist on *some* NULL constant.
            if tle.expr.node_tag() != NodeTag::T_Const
                || !tle.expr.as_const().unwrap().constisnull
            {
                return Err(plan_output_mismatch(format!(
                    "Query provides a value for a generated column at ordinal position {attno}."
                )));
            }
        } else {
            let exprtype = expr_type(tle.expr);
            if exprtype != att.atttypid {
                let want = format_type::format_type_be(att.atttypid)
                    .unwrap_or_else(|_| "???".into());
                let got =
                    format_type::format_type_be(exprtype).unwrap_or_else(|_| "???".into());
                return Err(plan_output_mismatch(format!(
                    "Table has type {want} at ordinal position {attno}, but query expects {got}."
                )));
            }
        }
    }
    if attno != desc.natts as usize {
        return Err(plan_output_mismatch("Query has too few columns."));
    }
    Ok(())
}

// exprType over the shapes an INSERT subplan tlist can carry today.
fn expr_type(node: Node<'_>) -> u32 {
    match node.node_tag() {
        NodeTag::T_Const => node.as_const().unwrap().consttype,
        NodeTag::T_Param => node.as_param().unwrap().paramtype,
        NodeTag::T_Var => node.as_var().unwrap().vartype,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resulttype,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funcresulttype,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opresulttype,
        NodeTag::T_NextValueExpr => {
            node.as_variant::<types_nodes::primnodes::NextValueExpr>().unwrap().typeId
        }
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resulttype,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resulttype,
        NodeTag::T_ArrayCoerceExpr => node.as_array_coerce_expr().unwrap().resulttype,
        NodeTag::T_ConvertRowtypeExpr => node.as_convert_rowtype_expr().unwrap().resulttype,
        NodeTag::T_SubscriptingRef => node.as_subscripting_ref().unwrap().refrestype,
        NodeTag::T_ArrayExpr => node.as_array_expr().unwrap().array_typeid,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggtype,
        NodeTag::T_ScalarArrayOpExpr => 16,
        NodeTag::T_RowExpr => node.as_row_expr().unwrap().row_typeid,
        NodeTag::T_FieldSelect => node.as_field_select().unwrap().resulttype,
        NodeTag::T_FieldStore => node.as_field_store().unwrap().resulttype,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casetype,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescetype,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxtype,
        NodeTag::T_RowCompareExpr => 16,
        // Everything else rides execexpr's exprType (nodeFuncs.c) — the
        // authoritative copy (SQLValueFunction, SubLink, Json*, ...).
        _ => execexpr::expr_type(node),
    }
}

// ExecGetInsertNewTuple (nodeModifyTable.c), no-projection arm.
//
// inline(always): the per-row new-tuple fetch (and its exec_copy_slot call
// site) was inline in exec_modify_table at base; after the wave-2 seam
// split it went outlined in BOTH monomorphizations (+51 instr/row named by
// the se2-cost dist-prof attribution) — se2-cost-fix round 2.
#[inline(always)]
fn exec_get_insert_new_tuple<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    let new_slot = mt.rel().ri_newTupleSlot.expect("ExecInitInsertProjection ran");
    let mcx = estate.es_query_cxt;
    let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
    if table[new_slot.0 as usize].kind() == table[plan_slot.0 as usize].kind() {
        return Ok(plan_slot);
    }
    assert_ne!(new_slot, plan_slot);
    let base = table.as_mut_ptr();
    // SAFETY: distinct in-bounds indices of one live slice.
    let (dst, src) = unsafe {
        (
            &mut *base.add(new_slot.0 as usize),
            &mut *base.add(plan_slot.0 as usize),
        )
    };
    exectuples::exec_copy_slot(dst, src, mcx, mcx)?;
    Ok(new_slot)
}

// ExecInitUpdateProjection + ExecBuildUpdateProjection (execExpr.c): resolve
// the merge of subplan output columns (via updateColnos) and old-tuple
// columns into a flat per-column source map, with ExecCheckPlanOutput-grade
// sanity checks; two table-format slots (old/new) join the tuple table.
fn exec_init_update_projection<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let subplan = mt
        .plan
        .plan
        .lefttree
        .expect("ModifyTable has a subplan")
        .as_plan()
        .expect("plan node");
    let update_colnos = mt.rel().update_colnos.expect("UPDATE result rel carries updateColnos");

    let mcx = estate.es_query_cxt;
    let (kind, desc) = {
        let rel = estate.es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
    };
    let natts = desc.natts as usize;

    let mut n_assignable = 0usize;
    let mut saw_junk = false;
    for tle_node in &subplan.targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.resjunk {
            saw_junk = true;
        } else {
            assert!(!saw_junk, "subplan target list is out of order");
            n_assignable += 1;
        }
    }
    assert_eq!(
        n_assignable,
        update_colnos.len(),
        "targetColnos does not match subplan target list"
    );

    let mut cols: mcx::PgVec<'mcx, NewColSrc> = mcx::PgVec::new_in(mcx);
    cols.try_reserve_exact(natts).map_err(|_| mcx.oom(natts))?;
    for attno in 1..=natts {
        cols.push(if desc.attr(attno - 1).attisdropped {
            NewColSrc::NullDropped
        } else {
            NewColSrc::Old(attno as u16)
        });
    }
    for (outer_idx, (tle_node, target_attnum)) in subplan
        .targetlist
        .iter()
        .zip(update_colnos.iter())
        .enumerate()
    {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        debug_assert!(!tle.resjunk);
        let target_attnum = target_attnum as usize;
        if target_attnum < 1 || target_attnum > natts {
            return Err(plan_output_mismatch("Query has too many columns."));
        }
        let att = desc.attr(target_attnum - 1);
        if att.attisdropped {
            return Err(plan_output_mismatch(
                "Query provides a value for a dropped column.",
            ));
        }
        if expr_type(tle.expr) != att.atttypid {
            return Err(plan_output_mismatch(
                "Table has a column of one type at a position where the \
                 query expects another type.",
            ));
        }
        cols[target_attnum - 1] = NewColSrc::Outer(outer_idx as u16);
    }
    mt.rel_mut().update_cols = cols;

    let old_slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc.clone()));
    let old_id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(old_slot);
    let new_slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
    let new_id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(new_slot);
    mt.rel_mut().ri_oldTupleSlot = Some(old_id);
    mt.rel_mut().ri_newTupleSlot = Some(new_id);
    mt.rel_mut().ri_projectNewInfoValid = true;
    Ok(())
}

// ExecGetUpdateNewTuple (nodeModifyTable.c): run the resolved column map over
// the plan (outer) and old (scan) tuples into ri_newTupleSlot. Per row: two
// deforms + one datum copy loop, no allocations.
fn exec_get_update_new_tuple<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    let new_id = mt.rel().ri_newTupleSlot.expect("ExecInitUpdateProjection ran");
    let old_id = mt.rel().ri_oldTupleSlot.expect("ExecInitUpdateProjection ran");
    let mcx = estate.es_query_cxt;
    let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
    let (n, o, p) = (new_id.0 as usize, old_id.0 as usize, plan_slot.0 as usize);
    assert!(n < table.len() && o < table.len() && p < table.len());
    assert!(n != o && n != p && o != p);
    let base = table.as_mut_ptr();
    // SAFETY: distinct in-bounds indices of one live slice.
    let (new_slot, old_slot, outer) = unsafe {
        (&mut *base.add(n), &mut *base.add(o), &mut *base.add(p))
    };

    exectuples::slot_getallattrs(outer);
    exectuples::slot_getallattrs(old_slot);
    exectuples::exec_clear_tuple(new_slot, mcx);
    {
        let (ob, sb) = (outer.base(), old_slot.base());
        let nb = new_slot.base_mut();
        for (i, src) in mt.rel().update_cols.iter().enumerate() {
            let (v, isnull) = match *src {
                NewColSrc::Outer(j) => (ob.tts_values[j as usize], ob.tts_isnull[j as usize]),
                NewColSrc::Old(a) => {
                    (sb.tts_values[a as usize - 1], sb.tts_isnull[a as usize - 1])
                }
                NewColSrc::NullDropped => (Datum::null(), true),
            };
            nb.tts_values[i] = v;
            nb.tts_isnull[i] = isnull;
        }
    }
    exectuples::exec_store_virtual_tuple(new_slot);
    Ok(new_id)
}

// ExecModifyTable's UPDATE row-identity block + the EPQ redo's "fetch the
// most recent version of old tuple" step: latest version at tupleid into
// ri_oldTupleSlot under SnapshotAny.
fn fetch_old_row_version<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
) -> PgResult<()> {
    let old_slot = mt.rel().ri_oldTupleSlot.expect("ExecInitUpdateProjection ran");
    let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = estate;
    let rel = es_relations[(mt.rel().rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let found = tableam::table_tuple_fetch_row_version(
        *es_query_cxt,
        rel,
        tupleid,
        &mt.snapshot_any,
        &mut es_tupleTable[old_slot.0 as usize],
    )?;
    assert!(found, "failed to fetch tuple being updated");
    Ok(())
}

// EvalPlanQualSlot (execMain.c): the per-result-rel EPQ test slot,
// created on first use into the shared tuple table.
// C EvalPlanQualStart's resultRelations loop: every result relation starts
// blocked (and done), not just the dispatch-current one — otherwise an EPQ
// recheck on one inheritance child rescans its siblings and requalifies
// against unrelated rows (the writep4a/writep4b class).
fn ensure_mt_epq_subs<'mcx>(mt: &mut ModifyTableState<'mcx>, estate: &EStateData<'mcx>) {
    if mt.epq_subs.is_some() {
        return;
    }
    let mcx = estate.es_query_cxt;
    let ModifyTableState { rels, epq_subs, epq_arowmarks, epq_origslot, .. } = mt;
    let subs = executils::ensure_epq_subs(epq_subs, mcx, estate.epq_rtsize(), rels[0].rti);
    for r in rels.iter() {
        subs.relsubs_blocked[(r.rti - 1) as usize] = true;
        subs.relsubs_done[(r.rti - 1) as usize] = true;
    }
    // C EvalPlanQualStart's relsubs_rowmark loop + EvalPlanQualSetSlot.
    for &(rti, fetch) in epq_arowmarks.iter() {
        subs.relsubs_rowmark[(rti - 1) as usize] = Some(fetch);
    }
    subs.origslot = *epq_origslot;
}

fn eval_plan_qual_slot<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ExecSlotId {
    ensure_mt_epq_subs(mt, estate);
    let rti = mt.rel().rti;
    let idx = (rti - 1) as usize;
    if let Some(id) = mt.epq_subs.as_ref().expect("just ensured").relsubs_slot[idx] {
        return id;
    }
    let mcx = estate.es_query_cxt;
    let (kind, desc) = {
        let rel = estate.es_relations[idx].as_ref().expect("result relation opened");
        (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
    };
    let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
    let id = ExecSlotId(estate.es_tupleTable.len() as u32);
    estate.es_tupleTable.push(slot);
    mt.epq_subs.as_mut().expect("just ensured").relsubs_slot[idx] = Some(id);
    id
}

// ExecUpdate + ExecUpdatePrologue/Act/Epilogue (nodeModifyTable.c), plain-heap
// arm: no triggers/FDW/partitions. Concurrent TM_Updated runs the EPQ
// recheck (redo_act loop); the ri_needLockTagTuple relock is omitted —
// inplace-update catalogs never reach this executor path.
// ExecUpdate's outcome: a cross-partition move carries the INSERT half's
// result slot (C updateCxt->crossPartUpdate + cpUpdateReturningSlot).
enum UpdateResult {
    NotModified,
    Modified,
    CrossPart(Option<ExecSlotId>),
}

fn exec_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    slot_id: ExecSlotId,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<UpdateResult> {
    let output_cid = estate.es_output_cid;
    let mut slot_id = slot_id;
    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleExclusive;

    if mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_update_before_row) {
        let (old_slot, epq) = match get_tuple_for_trigger(mt, estate, tupleid, epq_eval)? {
            TrigFetch::Skip => return Ok(UpdateResult::NotModified),
            TrigFetch::Proceed { old_slot, epq } => (old_slot, epq),
        };
        if let Some(eslot) = epq {
            // ExecBRUpdateTriggers: rebuild the new tuple from the
            // EPQ-rechecked row (ExecGetUpdateNewTuple over the re-fetched
            // latest old version).
            debug_assert!(mt.rel().ri_projectNewInfoValid);
            fetch_old_row_version(mt, estate, tupleid)?;
            slot_id = exec_get_update_new_tuple(mt, estate, eslot)?;
        }
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_UPDATE,
            types_trigger::TRIGGER_EVENT_UPDATE,
            Some(old_slot),
            Some(slot_id),
            None,
        )? {
            return Ok(UpdateResult::NotModified);
        }
    }
    let mut update_indexes = TU_UpdateIndexes::TU_None;

    // redo_act:
    loop {
        let mcx = estate.es_query_cxt;
        let mut cross_part = false;
        {
            let EStateData { es_relations, es_tupleTable, es_range_table, es_rteperminfos, .. } =
                &mut *estate;
            let rel = es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let slot = &mut es_tupleTable[slot_id.0 as usize];

            slot.base_mut().tts_tableOid = rel.rd_id;
            if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
                exec_compute_stored_generated(mcx, &mut mt.rel_mut().generated_exprs, rel, slot)?;
            }
            exectuples::exec_materialize_slot(slot, mcx)?;
            slot.base_mut().tts_tableOid = rel.rd_id;

            // ExecUpdateAct (nodeModifyTable.c): the new tuple must satisfy
            // this partition's constraint. On a single-result-relation plan
            // the target IS the root (C resultRelInfo == rootResultRelInfo),
            // so a failure is ExecCrossPartitionUpdate's direct-leaf error
            // leg, after its ON CONFLICT DO UPDATE refusal.
            if rel.rd_rel.relispartition
                && !execpartition::exec_partition_check(mcx, &mut mt.rel_mut().partition_check, rel, slot)?
            {
                if mt.plan.onConflictAction
                    == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32
                {
                    return Err(invalid_on_update_specification());
                }
                if mt.root.is_none() {
                    let mod_cols = rte_modified_cols(
                        mcx,
                        &es_range_table[..],
                        *es_rteperminfos,
                        mt.rels[mt.cur].rti,
                    )?;
                    return Err(execpartition::partition_constraint_violation(
                        mcx,
                        rel,
                        slot,
                        Some(&mod_cols),
                        None,
                    ));
                }
                // ExecCrossPartitionUpdate: DELETE here + re-routed INSERT,
                // performed outside this borrow scope.
                cross_part = true;
            }
        }
        // C ExecUpdateAct: UPDATE RLS WITH CHECK quals over the NEW row, only
        // when the partition constraint passed. Policy quals carry
        // SubPlans/initplans (executils subplan driver).
        if !cross_part && !mt.rel().wco_exprs.is_empty() {
            let ecxt = mt.node_ecxt;
            let r = mt.rel_mut();
            exec_with_check_options(estate, ecxt, &mut r.wco_exprs, WCOKind::WCO_RLS_UPDATE_CHECK, slot_id)?;
        }
        let result = if cross_part {
            TM_Result::TM_Ok
        } else {
            let EStateData {
                es_relations,
                es_tupleTable,
                es_snapshot,
                es_range_table,
                es_rteperminfos,
            es_crosscheck_snapshot,
                ..
            } = &mut *estate;
            let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
            let crosscheck: &tableam_vocab::Snapshot<'mcx> = &*es_crosscheck_snapshot;
            let rel = es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let slot = &mut es_tupleTable[slot_id.0 as usize];

            if rel.rd_rel.relhasindex && mt.rel_mut().indexes.is_none() {
                mt.rel_mut().indexes = Some(execindexing::ExecOpenIndices(mcx, rel, false)?);
            }

            {
                // ExecConstraints (execMain.c): partition children report
                // through the root rel + root perminfo.
                let (perm_rti, err_root_rel) = match &mt.root {
                    Some(rr) => (rr.rti, es_relations[(rr.rti - 1) as usize].as_ref()),
                    None => (mt.rels[mt.cur].rti, None),
                };
                let mod_cols =
                    rte_modified_cols(mcx, &es_range_table[..], *es_rteperminfos, perm_rti)?;
                let r = &mut mt.rels[mt.cur];
                exec_constraints(
                    mcx,
                    &mut r.check_exprs,
                    &mut r.virtual_nn_exprs,
                    rel,
                    slot,
                    err_root_rel,
                    Some(&mod_cols),
                )?;
            }

            tableam::table_tuple_update(
                mcx,
                rel,
                tupleid,
                slot,
                output_cid,
                snapshot,
                crosscheck,
                true,
                &mut tmfd,
                &mut lockmode,
                &mut update_indexes,
            )?
        };

        if cross_part {
            match exec_cross_partition_update(mt, estate, tupleid, slot_id, epq_eval)? {
                CrossPartResult::Done(ins) => return Ok(UpdateResult::CrossPart(ins)),
                // C: goto lreplace with the EPQ-reprojected tuple.
                CrossPartResult::Retry(retry) => {
                    slot_id = retry;
                    continue;
                }
                CrossPartResult::MergeConcurrent(..) => {
                    unreachable!("MergeConcurrent only surfaces under CMD_MERGE")
                }
            }
        }

        match result {
            TM_Result::TM_Ok => break,
            TM_Result::TM_SelfModified => {
                if tmfd.cmax != output_cid {
                    return Err(self_modified_violation("updated"));
                }
                return Ok(UpdateResult::NotModified);
            }
            TM_Result::TM_Updated => {
                if xact::IsolationUsesXactSnapshot() {
                    return Err(serialization_conflict("update"));
                }
                let inputslot = eval_plan_qual_slot(mt, estate);
                let lock_result = {
                    let EStateData { es_relations, es_tupleTable, es_snapshot, .. } =
                        &mut *estate;
                    let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
                    let rel = es_relations[(mt.rel().rti - 1) as usize]
                        .as_ref()
                        .expect("result relation opened");
                    tableam::table_tuple_lock(
                        mcx,
                        rel,
                        tupleid,
                        snapshot,
                        &mut es_tupleTable[inputslot.0 as usize],
                        output_cid,
                        lockmode,
                        LockWaitPolicy::LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut tmfd,
                    )?
                };
                match lock_result {
                    TM_Result::TM_Ok => {
                        debug_assert!(tmfd.traversed);
                        // The locked latest version's tid (C: table_tuple_lock
                        // writes through tupleid); read before EvalPlanQual
                        // clears the test slot.
                        *tupleid = estate.slot(inputslot).base().tts_tid;
                        let rti = mt.rel().rti;
                        let Some(epqslot) = epq_eval(&mut mt.epq_subs, estate, inputslot, rti)?
                        else {
                            return Ok(UpdateResult::NotModified);
                        };
                        debug_assert!(mt.rel().ri_projectNewInfoValid);
                        fetch_old_row_version(mt, estate, tupleid)?;
                        slot_id = exec_get_update_new_tuple(mt, estate, epqslot)?;
                        continue;
                    }
                    TM_Result::TM_Deleted => return Ok(UpdateResult::NotModified),
                    TM_Result::TM_SelfModified => {
                        if tmfd.cmax != output_cid {
                            return Err(self_modified_violation("updated"));
                        }
                        return Ok(UpdateResult::NotModified);
                    }
                    other => panic!(
                        "ExecUpdate (nodeModifyTable.c): unexpected \
                         table_tuple_lock status: {other:?}"
                    ),
                }
            }
            TM_Result::TM_Deleted => {
                if xact::IsolationUsesXactSnapshot() {
                    return Err(serialization_conflict("delete"));
                }
                return Ok(UpdateResult::NotModified);
            }
            other => panic!("ExecUpdate (nodeModifyTable.c): unexpected {other:?}"),
        }
    }

    let mcx = estate.es_query_cxt;
    let EStateData { es_relations, es_tupleTable, .. } = estate;
    let rel = es_relations[(mt.rel().rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let slot = &mut es_tupleTable[slot_id.0 as usize];
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    let ModifyTableState { rels, cur, index_eval_cx, .. } = &mut *mt;
    if let Some(indexes) = rels[*cur].indexes.as_mut() {
        if indexes.num_indices() > 0 && update_indexes != TU_UpdateIndexes::TU_None {
            recheck_indexes = execindexing::ExecInsertIndexTuples(
                mcx,
                index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                indexes,
                rel,
                slot,
                false,
                None,
                &[],
                update_indexes == TU_UpdateIndexes::TU_Summarizing,
            )?;
        }
    }

    let ar_new_tid = slot.base().tts_tid;
    // ExecARUpdateTriggers (trigger.c) fires on trig_update_after_row OR an
    // active transition capture: a statement trigger with a REFERENCING
    // clause on the root captures child rows even when the child has no row
    // triggers (trigdesc NULL).
    let td = mt.rel().trigdesc.clone();
    let tc_active = if mt.operation == CmdType::CMD_INSERT {
        mt.oc_transition_capture.is_some()
    } else {
        mt.transition_capture.is_some()
    };
    if td.is_some() || tc_active {
        // Unconditional (C ExecARUpdateTriggers → ExecGetAllUpdatedCols):
        // every queued UPDATE event carries ats_modifiedcols, not just
        // UPDATE-OF filters.
        ensure_all_updated_cols(mt, estate, false)?;
        let result_rti = mt.rel().rti;
        ensure_child_to_root(mt, estate)?;
        let root_rti = mt.root.as_ref().map(|rr| rr.rti);
        let ModifyTableState {
            rels, cur, transition_capture, oc_transition_capture, operation, ..
        } = mt;
        let r = &mut rels[*cur];
        // ON CONFLICT DO UPDATE (operation == INSERT) captures into the
        // UPDATE tables via mt_oc_transition_capture (C ExecOnConflictUpdate).
        let tc = if *operation == CmdType::CMD_INSERT {
            oc_transition_capture.as_ref()
        } else {
            transition_capture.as_ref()
        };
        let modified_cols = r.all_updated_cols.as_ref();
        let mut when = ::trigger::TriggerWhenEval {
            mcx,
            cache: &mut r.trig_when,
            modified_cols,
        };
        let rel = estate.es_relations[(result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let root_rel = root_rti.map(|rti| {
            estate.es_relations[(rti - 1) as usize]
                .as_ref()
                .expect("root relation opened")
        });
        let conv = child_to_root_spec(&r.child_to_root, rel, root_rel);
        ::trigger::ExecARUpdateTriggers(
            mcx, rel, td.as_deref(), None, None, Some(*tupleid), Some(ar_new_tid),
            &recheck_indexes, tc, Some(&mut when), false, conv.as_ref(), conv.as_ref(),
            modified_cols,
        )?;
    }

    // Parent-view CHECK OPTIONs are checked after updating (the qual must see
    // the actual row, post defaults/triggers).
    if !mt.rel().wco_exprs.is_empty() {
        let mcx = estate.es_query_cxt;
        let ecxt = mt.node_ecxt;
        let root_rti = mt.root.as_ref().map(|rr| rr.rti);
        let r = &mut mt.rels[mt.cur];
        let rti = r.rti;
        exec_view_check_options(
            mcx,
            estate,
            ecxt,
            &mut r.wco_exprs,
            slot_id,
            WcoRel::Rti { rti, root_rti },
        )?;
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(UpdateResult::Modified)
}

// ExecCrossPartitionUpdate outcomes: Done = C returned true (inserted slot
// carried); Retry = C returned false with *retry_slot set — the caller redoes
// the UPDATE from lreplace with the EPQ-reprojected tuple.
enum CrossPartResult {
    Done(Option<ExecSlotId>),
    Retry(ExecSlotId),
    // MERGE leg: the DELETE half saw a concurrent update/delete;
    // ExecMergeMatched redispatches on the TM_Result (C ExecUpdateAct
    // "if operation == CMD_MERGE return result").
    MergeConcurrent(TM_Result, TM_FailureData),
}

// ExecCrossPartitionUpdate (nodeModifyTable.c): move an updated tuple to
// another partition — DELETE from the source partition (RETURNING skipped,
// canSetTag=false), convert the new tuple to the root layout, and INSERT
// through the root, which re-routes to the destination leaf. The INSERT half
// produces the RETURNING row and the command count.
fn exec_cross_partition_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    slot_id: ExecSlotId,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<CrossPartResult> {
    let mcx = estate.es_query_cxt;
    let old_tid = *tupleid;

    // Row movement, part 1: delete, marking the tuple moved (changingPart).
    // The delete epilogue files the row into the UPDATE OLD transition table;
    // the insert epilogue files the new row as UPDATE NEW.
    let is_merge = mt.operation == CmdType::CMD_MERGE;
    let mut merge_state = (TM_Result::TM_Ok, TM_FailureData::default());
    let mut epqslot: Option<ExecSlotId> = None;
    let deleted = exec_delete(
        mt,
        estate,
        tupleid,
        epq_eval,
        true,
        Some(&mut epqslot),
        if is_merge { Some(&mut merge_state) } else { None },
    )?;
    if !deleted {
        // MERGE leaves a concurrently updated/deleted row to ExecMergeMatched
        // (it must recheck, and might run a different action entirely) —
        // C "return *tmresult == TM_Ok".
        if is_merge {
            if merge_state.0 != TM_Result::TM_Ok {
                return Ok(CrossPartResult::MergeConcurrent(merge_state.0, merge_state.1));
            }
            return Ok(CrossPartResult::Done(None));
        }
        match epqslot {
            // C TupIsNull(epqslot): vanished/blocked tuple skips the INSERT —
            // never turning one row into two.
            None => return Ok(CrossPartResult::Done(None)),
            Some(eslot) => {
                // Fetch the most recent version of the old tuple and project
                // the new tuple to retry the UPDATE with.
                debug_assert!(mt.rel().ri_projectNewInfoValid);
                fetch_old_row_version(mt, estate, tupleid)?;
                let retry = exec_get_update_new_tuple(mt, estate, eslot)?;
                return Ok(CrossPartResult::Retry(retry));
            }
        }
    }

    // Part 2: convert to the root layout (ExecGetChildToRootMap +
    // mt_root_tuple_slot) and insert via the root.
    let mut work_slot = slot_id;
    let map = {
        let EStateData { es_relations, .. } = &*estate;
        let src = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let root = es_relations[(mt.root_rel().rti - 1) as usize]
            .as_ref()
            .expect("root relation opened");
        tupdesc::build_attrmap_by_name_if_req(mcx, &src.rd_att, &root.rd_att, false)?
    };
    if let Some(map) = map {
        if mt.cross_part_root_slot.is_none() {
            let (kind, desc) = {
                let EStateData { es_relations, .. } = &*estate;
                let root = es_relations[(mt.root_rel().rti - 1) as usize]
                    .as_ref()
                    .expect("root relation opened");
                (tableam::table_slot_callbacks(root), root.rd_att.clone())
            };
            mt.cross_part_root_slot = Some(estate.exec_init_extra_tuple_slot(Some(desc), kind));
        }
        let rsid = mt.cross_part_root_slot.expect("just built");
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (i, o) = (slot_id.0 as usize, rsid.0 as usize);
        assert!(i != o && i < es_tupleTable.len() && o < es_tupleTable.len());
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (in_slot, out) = unsafe { (&mut *base.add(i), &mut *base.add(o)) };
        exectuples::execute_attr_map_slot(&map, in_slot, out, mcx);
        work_slot = rsid;
    }

    mt.insert_target_root = mt.root.is_some();
    let inserted = exec_insert(mt, estate, work_slot, epq_eval);
    mt.insert_target_root = false;
    let inserted = inserted?;

    // ExecUpdateAct's post-move FK leg: if the source partition carries AR
    // UPDATE triggers (RI enforcement on the referenced root), queue the
    // root-table UPDATE event; leaf AR triggers alone cannot see the move.
    if inserted.is_some()
        && mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_update_after_row)
    {
        exec_cross_partition_update_foreign_key(mt, estate, old_tid, work_slot)?;
    }

    Ok(CrossPartResult::Done(inserted))
}

// ExecInitRoutingInfo's RETURNING leg (execPartition.c:623-680), expressed in
// root coordinates: the routed-INSERT slots here stay root-format, so one
// projection over the root descriptor — returningLists[0] with the first
// result rel's varattnos map-converted to the root's (map_variable_attnos +
// build_attrmap_by_name, C's exact mechanism) — serves every destination
// leaf. Values match C's per-leaf projections because both maps pair columns
// by name. The same construction is C's rootRelInfo leg for MERGE root
// INSERTs (nodeModifyTable.c:3911-3947).
fn exec_init_root_returning<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let node = mt.plan;
    if node.returningLists.is_nil() {
        return Ok(());
    }
    // Root == rels[0]: the per-rel projection already targets the root layout.
    if mt.root.is_none() || mt.root.as_ref().is_some_and(|r| r.project_returning.is_some()) {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let first_rti = mt.rels[0].rti;
    let root_rti = mt.root.as_ref().expect("checked").rti;
    let rlist = node
        .returningLists
        .nth(0)
        .as_list()
        .expect("returningLists cell is a List");
    let (root_desc, root_reltype, attmap) = {
        let EStateData { es_relations, .. } = &*estate;
        let first = es_relations[(first_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let root_rel = es_relations[(root_rti - 1) as usize]
            .as_ref()
            .expect("root relation opened");
        let attmap = tupdesc::build_attrmap_by_name_if_req(
            mcx,
            &root_rel.rd_att,
            &first.rd_att,
            false,
        )?;
        (root_rel.rd_att.clone(), root_rel.rd_rel.reltype, attmap)
    };
    let mut mapped = types_nodes::list::NodeList::nil();
    for tle_node in rlist {
        let n = match &attmap {
            None => tle_node,
            Some(map) => {
                rewrite_manip::map_variable_attnos(mcx, tle_node, first_rti as i32, 0, map, root_reltype)?.0
            }
        };
        mapped.lappend(mcx, n)?;
    }
    let params = estate.param_bind();
    let is_merge = node.operation == CmdType::CMD_MERGE;
    let proj = executils::with_subplan_compile_env(estate, |env| {
        if is_merge {
            execexpr::exec_build_merge_projection_info_subplans(
                mcx,
                &mapped,
                Some(&root_desc),
                params,
                env,
            )
        } else {
            execexpr::exec_build_projection_info_subplans(
                mcx,
                &mapped,
                Some(&root_desc),
                params,
                env,
            )
        }
    })?;
    mt.root.as_mut().expect("checked").project_returning = Some(proj);
    Ok(())
}

// The cross-partition UPDATE RETURNING evaluation (C ExecInsert:1298-1348 +
// ExecDelete's saveOld leg 1823-1890): C projects on the routed destination
// rel with the source partition's deleted tuple converted through the root
// as OLD; our slots stay root-format, so the root projection runs over the
// root-format inserted slot, with the old tuple converted child-to-root
// (ExecGetChildToRootMap) when the projection references OLD.
fn exec_cross_part_returning<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    old_leaf_slot: Option<ExecSlotId>,
    islot: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    let mcx = estate.es_query_cxt;
    exec_init_root_returning(mt, estate)?;
    let has_old = mt
        .root_rel()
        .project_returning
        .as_deref()
        .is_some_and(|st| st.has_old());
    let mut old_root: Option<ExecSlotId> = None;
    if has_old {
        if let Some(src_id) = old_leaf_slot {
            let map = {
                let EStateData { es_relations, .. } = &*estate;
                let src = es_relations[(mt.rel().rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened");
                let root = es_relations[(mt.root_rel().rti - 1) as usize]
                    .as_ref()
                    .expect("root relation opened");
                tupdesc::build_attrmap_by_name_if_req(mcx, &src.rd_att, &root.rd_att, false)?
            };
            match map {
                None => old_root = Some(src_id),
                Some(map) => {
                    if mt.root_rel().ri_ReturningSlot.is_none() {
                        let (kind, desc) = {
                            let EStateData { es_relations, .. } = &*estate;
                            let root = es_relations[(mt.root_rel().rti - 1) as usize]
                                .as_ref()
                                .expect("root relation opened");
                            (tableam::table_slot_callbacks(root), root.rd_att.clone())
                        };
                        mt.root_rel_mut().ri_ReturningSlot =
                            Some(estate.exec_init_extra_tuple_slot(Some(desc), kind));
                    }
                    let out_id = mt.root_rel().ri_ReturningSlot.expect("just built");
                    let EStateData { es_tupleTable, .. } = &mut *estate;
                    let (i, o) = (src_id.0 as usize, out_id.0 as usize);
                    assert!(i != o && i < es_tupleTable.len() && o < es_tupleTable.len());
                    let base = es_tupleTable.as_mut_ptr();
                    // SAFETY: distinct in-bounds indices of one live slice.
                    let (in_slot, out) = unsafe { (&mut *base.add(i), &mut *base.add(o)) };
                    exectuples::execute_attr_map_slot(&map, in_slot, out, mcx);
                    // C copies the source tableoid and tid through (1883-1886):
                    // OLD.tableoid/ctid report the source partition's row.
                    out.base_mut().tts_tableOid = in_slot.base().tts_tableOid;
                    out.base_mut().tts_tid = in_slot.base().tts_tid;
                    old_root = Some(out_id);
                }
            }
        }
    }
    mt.insert_target_root = mt.root.is_some();
    let out = exec_process_returning(mt, estate, CmdType::CMD_UPDATE, old_root, Some(islot), plan_slot);
    mt.insert_target_root = false;
    out
}

// ExecCrossPartitionUpdateForeignKey (nodeModifyTable.c): queue an UPDATE
// event on the root partitioned table so foreign keys pointing into it are
// enforced across the row movement; an FK pointing at a non-root ancestor of
// the source partition cannot be enforced this way (C's error).
fn exec_cross_partition_update_foreign_key<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    old_tid: ItemPointerData,
    inserted_slot: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let src_rti = mt.rel().rti;
    let src_oid = mt.rel().rd_id;
    let root_rti = mt.root_rel().rti;
    let root_oid = mt.root_rel().rd_id;

    // ExecGetAncestorResultRels: the source partition's ancestors up to the
    // query's target root; the root's own triggers are processed below.
    for anc in pg_inherits::get_partition_ancestors(mcx, src_oid)?.iter() {
        if *anc == root_oid {
            break;
        }
        let Some(td) = relcache::RelationGetTriggerDesc(*anc)? else {
            continue;
        };
        if td.trig_update_after_row
            && td.triggers.iter().any(|t| {
                !t.tgisclone
                    && ::trigger::ri_trigger_kind(t.tgfoid) == types_trigger::RI_TRIGGER_PK
            })
        {
            let anc_name = lsyscache::relation::get_rel_name(mcx, *anc)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            let root_name = lsyscache::relation::get_rel_name(mcx, root_oid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::error(
                    "cannot move tuple across partitions when a non-root ancestor \
                     of the source partition is directly referenced in a foreign key"
                        .to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail(format!(
                    "A foreign key points to ancestor \"{anc_name}\" but not the \
                     root ancestor \"{root_name}\"."
                ))
                .with_hint(format!(
                    "Consider defining the foreign key on table \"{root_name}\"."
                )),
            ));
        }
    }

    let dst_idx = mt
        .last_insert_leaf
        .expect("cross-partition insert routed to a leaf");
    let Some(root_td) = mt.root_rel().trigdesc.clone() else {
        return Ok(());
    };
    let new_tid = estate.es_tupleTable[inserted_slot.0 as usize].base().tts_tid;
    let ModifyTableState { root, rels, router, .. } = &mut *mt;
    let root_r = root.as_mut().unwrap_or(&mut rels[0]);
    let EStateData { es_relations, .. } = &*estate;
    let src_rel = es_relations[(src_rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let root_rel = es_relations[(root_rti - 1) as usize]
        .as_ref()
        .expect("root relation opened");
    let dst_rel = router.as_ref().expect("routed insert has a router").leaf_rel(dst_idx);

    // The queued event's tuples must be in the root's format: C converts via
    // ExecGetChildToRootMap before the RI checks (AfterTriggerSaveEvent,
    // trigger.c:6384-6410); the specs carry the leaf->root maps.
    let src_map =
        tupdesc::build_attrmap_by_name_if_req(mcx, &src_rel.rd_att, &root_rel.rd_att, false)?;
    let dst_map =
        tupdesc::build_attrmap_by_name_if_req(mcx, &dst_rel.rd_att, &root_rel.rd_att, false)?;
    let src_conv = src_map.as_deref().map(|map| ::trigger::ChildToRoot {
        map,
        child_desc: src_rel.rd_att.as_ref(),
        root_desc: root_rel.rd_att.as_ref(),
    });
    let dst_conv = dst_map.as_deref().map(|map| ::trigger::ChildToRoot {
        map,
        child_desc: dst_rel.rd_att.as_ref(),
        root_desc: root_rel.rd_att.as_ref(),
    });

    let mut when = ::trigger::TriggerWhenEval {
        mcx,
        cache: &mut root_r.trig_when,
        modified_cols: None,
    };
    ::trigger::ExecARUpdateTriggers(
        mcx,
        root_rel,
        Some(&root_td),
        Some(src_rel),
        Some(dst_rel),
        Some(old_tid),
        Some(new_tid),
        &[],
        None,
        Some(&mut when),
        true,
        src_conv.as_ref(),
        dst_conv.as_ref(),
        // CP-update root: tg_updatedcols follows this path's WHEN (None) — a
        // pre-existing partition-move limitation, not exercised by lo/tcn.
        None,
    )
}

// ExecDelete + ExecDeletePrologue/Act/Epilogue (nodeModifyTable.c), plain-heap
// arm; concurrent TM_Updated runs the EPQ recheck (ldelete loop).
fn exec_delete<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
    // C changingPart: this delete is half of a cross-partition UPDATE — the
    // storage layer marks the tuple moved and the row counts once, via the
    // INSERT half (C passes canSetTag=false here).
    changing_part: bool,
    // C epqreturnslot: on a concurrently-updated row the delete is skipped
    // and the EPQ-rechecked slot handed back for the cross-partition caller
    // to retry the UPDATE with.
    mut epqreturnslot: Option<&mut Option<ExecSlotId>>,
    // C tmresult out-param, passed only from a MERGE cross-partition update:
    // a concurrent update/delete returns false without EPQ retry here, and
    // ExecMergeMatched redispatches on the (TM_Result, TM_FailureData).
    mut merge_out: Option<&mut (TM_Result, TM_FailureData)>,
) -> PgResult<bool> {
    let output_cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();

    if mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_delete_before_row) {
        let old_slot = if let Some(out) = merge_out.as_deref_mut() {
            // ExecBRDeleteTriggers(is_merge_delete=true): skip the EPQ
            // recheck, hand the concurrency status back to lmerge_matched.
            match merge_tuple_for_trigger(mt, estate, tupleid)? {
                MergeTrigFetch::Fetched(slot) => slot,
                MergeTrigFetch::SelfModified(fd) => {
                    *out = (TM_Result::TM_SelfModified, fd);
                    return Ok(false);
                }
                MergeTrigFetch::Deleted => {
                    *out = (TM_Result::TM_Deleted, TM_FailureData::default());
                    return Ok(false);
                }
                MergeTrigFetch::Updated(fd) => {
                    *out = (TM_Result::TM_Updated, fd);
                    return Ok(false);
                }
            }
        } else {
            let (old_slot, epq) = match get_tuple_for_trigger(mt, estate, tupleid, epq_eval)? {
                TrigFetch::Skip => return Ok(false),
                TrigFetch::Proceed { old_slot, epq } => (old_slot, epq),
            };
            // ExecBRDeleteTriggers: when the caller asked for the updated
            // row, skip the trigger and the delete and pass the EPQ slot back.
            if let (Some(eslot), Some(out)) = (epq, epqreturnslot.as_deref_mut()) {
                *out = Some(eslot);
                return Ok(false);
            }
            old_slot
        };
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_DELETE,
            types_trigger::TRIGGER_EVENT_DELETE,
            Some(old_slot),
            None,
            None,
        )? {
            return Ok(false);
        }
    }

    // ldelete:
    loop {
        let mcx = estate.es_query_cxt;
        let result = {
            let EStateData { es_relations, es_snapshot, es_crosscheck_snapshot, .. } = &*estate;
            let snapshot: &tableam_vocab::Snapshot<'mcx> = es_snapshot;
            let crosscheck: &tableam_vocab::Snapshot<'mcx> = es_crosscheck_snapshot;
            let rel = es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            tableam::table_tuple_delete(
                mcx,
                rel,
                tupleid,
                output_cid,
                snapshot,
                crosscheck,
                true,
                &mut tmfd,
                changing_part,
            )?
        };
        if let Some(out) = merge_out.as_deref_mut() {
            *out = (result, tmfd);
        }

        match result {
            TM_Result::TM_Ok => break,
            TM_Result::TM_SelfModified => {
                if tmfd.cmax != output_cid {
                    return Err(self_modified_violation("deleted"));
                }
                return Ok(false);
            }
            TM_Result::TM_Updated => {
                if xact::IsolationUsesXactSnapshot() {
                    return Err(serialization_conflict("update"));
                }
                if merge_out.is_some() {
                    // C ExecDelete locks + rechecks here too, then hands the
                    // row back untouched via epqreturnslot; lmerge_matched's
                    // own lock + EvalPlanQual is the one that acts, so skip
                    // straight to it.
                    return Ok(false);
                }
                let inputslot = eval_plan_qual_slot(mt, estate);
                let lock_result = {
                    let EStateData { es_relations, es_tupleTable, es_snapshot, .. } =
                        &mut *estate;
                    let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
                    let rel = es_relations[(mt.rel().rti - 1) as usize]
                        .as_ref()
                        .expect("result relation opened");
                    tableam::table_tuple_lock(
                        mcx,
                        rel,
                        tupleid,
                        snapshot,
                        &mut es_tupleTable[inputslot.0 as usize],
                        output_cid,
                        LockTupleMode::LockTupleExclusive,
                        LockWaitPolicy::LockWaitBlock,
                        TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
                        &mut tmfd,
                    )?
                };
                match lock_result {
                    TM_Result::TM_Ok => {
                        debug_assert!(tmfd.traversed);
                        *tupleid = estate.slot(inputslot).base().tts_tid;
                        let rti = mt.rel().rti;
                        let Some(epqslot) = epq_eval(&mut mt.epq_subs, estate, inputslot, rti)?
                        else {
                            return Ok(false);
                        };
                        // C: skip the delete and pass back the updated row
                        // when requested; otherwise redo the delete (ldelete).
                        if let Some(out) = epqreturnslot.as_deref_mut() {
                            *out = Some(epqslot);
                            return Ok(false);
                        }
                        continue;
                    }
                    TM_Result::TM_SelfModified => {
                        if tmfd.cmax != output_cid {
                            return Err(self_modified_violation("deleted"));
                        }
                        return Ok(false);
                    }
                    TM_Result::TM_Deleted => return Ok(false),
                    other => panic!(
                        "ExecDelete (nodeModifyTable.c): unexpected \
                         table_tuple_lock status: {other:?}"
                    ),
                }
            }
            TM_Result::TM_Deleted => {
                if xact::IsolationUsesXactSnapshot() {
                    return Err(serialization_conflict("delete"));
                }
                return Ok(false);
            }
            other => panic!("ExecDelete (nodeModifyTable.c): unexpected {other:?}"),
        }
    }

    // ExecDeleteEpilogue: a row moved out by a cross-partition UPDATE is
    // captured into the UPDATE OLD transition table (old-only
    // ExecARUpdateTriggers), and the AR DELETE triggers then run without the
    // capture state so they don't re-file it as a DELETE.
    let moved_capture = changing_part
        && mt.operation == CmdType::CMD_UPDATE
        && mt.transition_capture.as_ref().is_some_and(|tc| tc.tcs_update_old_table);
    let delete_capture =
        mt.transition_capture.as_ref().is_some_and(|tc| tc.tcs_delete_old_table);
    if mt.rel().trigdesc.is_some() || moved_capture || delete_capture {
        let td = mt.rel().trigdesc.clone();
        let result_rti = mt.rel().rti;
        ensure_child_to_root(mt, estate)?;
        let root_rti = mt.root.as_ref().map(|rr| rr.rti);
        let ModifyTableState { rels, cur, transition_capture, .. } = mt;
        let r = &mut rels[*cur];
        let EStateData { es_relations, es_query_cxt, .. } = &*estate;
        let rel = es_relations[(result_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let root_rel = root_rti.map(|rti| {
            es_relations[(rti - 1) as usize].as_ref().expect("root relation opened")
        });
        let conv = child_to_root_spec(&r.child_to_root, rel, root_rel);
        let mut when = ::trigger::TriggerWhenEval {
            mcx: *es_query_cxt,
            cache: &mut r.trig_when,
            modified_cols: None,
        };
        if moved_capture {
            // New-only transition capture on a partition-move DELETE — no
            // real UPDATE trigger fires here, so tg_updatedcols is moot.
            ::trigger::ExecARUpdateTriggers(
                *es_query_cxt, rel, td.as_deref(), None, None, Some(*tupleid), None,
                &[], transition_capture.as_ref(), Some(&mut when), false, conv.as_ref(), None,
                None,
            )?;
        }
        let ar_tcs = if moved_capture { None } else { transition_capture.as_ref() };
        ::trigger::ExecARDeleteTriggers(
            *es_query_cxt, rel, td.as_deref(), *tupleid, ar_tcs, Some(&mut when),
            changing_part, conv.as_ref(),
        )?;
    }

    if mt.canSetTag && !changing_part {
        estate.es_processed += 1;
    }
    Ok(true)
}

#[track_caller]
#[cold]
#[inline(never)]
fn serialization_conflict(kind: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not serialize access due to concurrent {kind}"))
            .with_sqlstate(ERRCODE_T_R_SERIALIZATION_FAILURE),
    )
}

// ExecBRInsertTriggers (trigger.c): tgisclone replacement tuple failed the
// partition constraint re-verify.
#[track_caller]
#[cold]
#[inline(never)]
fn moved_row_before_trigger<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    trigger: &types_trigger::Trigger<'static>,
    rel: &Relation<'mcx>,
) -> Box<PgError> {
    let nspname = lsyscache::misc::get_namespace_name(mcx, rel.rd_rel.relnamespace)
        .ok()
        .flatten()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();
    Box::new(
        PgError::error(
            "moving row to another partition during a BEFORE FOR EACH ROW trigger is not \
             supported"
                .to_string(),
        )
        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail(format!(
            "Before executing trigger \"{}\", the row was to be in partition \"{}.{}\".",
            trigger.tgname.as_str(),
            nspname,
            rel.name()
        )),
    )
}

// ExecCrossPartitionUpdate (nodeModifyTable.c): ON CONFLICT DO UPDATE may
// not move a row to another partition.
#[track_caller]
#[cold]
#[inline(never)]
fn invalid_on_update_specification() -> Box<PgError> {
    Box::new(
        PgError::error("invalid ON UPDATE specification".to_string())
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(
                "The result tuple would appear in a different partition than the original tuple."
                    .to_string(),
            ),
    )
}

// ExecDelete's RETURNING arm: re-fetch the deleted tuple under SnapshotAny
// into a lazily-built table-format slot (C ExecGetReturningSlot).
fn exec_delete_fetch_old<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
) -> PgResult<ExecSlotId> {
    if mt.rel().ri_ReturningSlot.is_none() {
        let mcx = estate.es_query_cxt;
        let (kind, desc) = {
            let rel = estate.es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
        let id = ExecSlotId(estate.es_tupleTable.len() as u32);
        estate.es_tupleTable.push(slot);
        mt.rel_mut().ri_ReturningSlot = Some(id);
    }
    let slot_id = mt.rel().ri_ReturningSlot.expect("just initialized");
    let found = {
        let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = estate;
        let rel = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        tableam::table_tuple_fetch_row_version(
            *es_query_cxt,
            rel,
            tupleid,
            &mt.snapshot_any,
            &mut es_tupleTable[slot_id.0 as usize],
        )?
    };
    assert!(found, "failed to fetch deleted tuple for DELETE RETURNING");
    Ok(slot_id)
}


// ExecBR{Insert,Update,Delete}Triggers + GetTupleForTrigger (trigger.c),
// plain-heap BEFORE ROW lane. LOUD: WHEN clauses, UPDATE OF columns,
// replacement tuples returned by a trigger, and the concurrent-update EPQ
// recheck (single-backend port: loud beats silently wrong).
// C should_free_trig discipline (trigger.c): a Copied fetch owns the image;
// the returned holder must stay live for as long as the raw parts are read.
fn slot_raw_tuple<'mcx>(
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
) -> PgResult<(
    (*const u8, u32, ItemPointerData, types_core::Oid),
    Option<heaptuple::HeapTuple<'mcx>>,
)> {
    let mcx = estate.es_query_cxt;
    let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
    let fetched = exectuples::exec_fetch_slot_heap_tuple(slot, true, mcx, mcx)?;
    Ok(match fetched {
        exectuples::FetchedHeapTuple::Slot(t) => {
            ((t.header_ptr(), t.t_len, t.t_self, t.t_tableOid), None)
        }
        exectuples::FetchedHeapTuple::Copied(t) => {
            ((t.header_ptr(), t.t_len, t.t_self, t.t_tableOid), Some(t))
        }
    })
}

fn br_row_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tgtype_event: i16,
    event_op: u32,
    old_slot: Option<ExecSlotId>,
    new_slot: Option<ExecSlotId>,
    leaf: Option<usize>,
) -> PgResult<bool> {
    row_triggers_common(mt, estate, tgtype_event, event_op, old_slot, new_slot, false, leaf)
}

// ExecIR{Insert,Update,Delete}Triggers (trigger.c): same protocol as BEFORE
// ROW with INSTEAD timing; the view row is never stored.
fn ir_row_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tgtype_event: i16,
    event_op: u32,
    old_slot: Option<ExecSlotId>,
    new_slot: Option<ExecSlotId>,
) -> PgResult<bool> {
    row_triggers_common(mt, estate, tgtype_event, event_op, old_slot, new_slot, true, None)
}

#[allow(clippy::too_many_arguments)]
fn row_triggers_common<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tgtype_event: i16,
    event_op: u32,
    old_slot: Option<ExecSlotId>,
    new_slot: Option<ExecSlotId>,
    instead: bool,
    leaf: Option<usize>,
) -> PgResult<bool> {
    use types_trigger::{
        TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_DELETE, TRIGGER_EVENT_INSTEAD, TRIGGER_EVENT_ROW,
        TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_ROW,
        TRIGGER_TYPE_TIMING_MASK,
    };
    let mcx = estate.es_query_cxt;
    let (raw_old, _old_owned) = match old_slot {
        Some(id) => {
            let (raw, owned) = slot_raw_tuple(estate, id)?;
            (Some(raw), owned)
        }
        None => (None, None),
    };
    let (mut raw_new, mut _new_owned) = match new_slot {
        Some(id) => {
            let (raw, owned) = slot_raw_tuple(estate, id)?;
            (Some(raw), owned)
        }
        None => (None, None),
    };
    let trigdesc = match leaf {
        None => mt.rel().trigdesc.as_ref().expect("BR caller checked trigdesc").clone(),
        Some(ix) => mt.leaf_trigdesc[ix]
            .clone()
            .flatten()
            .expect("leaf BR caller checked trigdesc"),
    };
    let (type_timing, event_timing) = if instead {
        (TRIGGER_TYPE_INSTEAD, TRIGGER_EVENT_INSTEAD)
    } else {
        (TRIGGER_TYPE_BEFORE, TRIGGER_EVENT_BEFORE)
    };
    let tg_event = event_op | TRIGGER_EVENT_ROW | event_timing;
    let is_delete = event_op == TRIGGER_EVENT_DELETE;
    // C ExecBR/IR UpdateTriggers hand ExecGetAllUpdatedCols to every row
    // trigger via tg_updatedcols, not just WHEN-column filters. A routed
    // leaf (the upsert BR-UPDATE leg) gets the root's columns renumbered
    // through the root->leaf attrmap (execUtils.c ExecGetUpdatedCols).
    let leaf_updated_cols = if event_op == types_trigger::TRIGGER_EVENT_UPDATE {
        match leaf {
            Some(ix) => Some(leaf_all_updated_cols(mt, estate, ix)?),
            None => {
                ensure_all_updated_cols(mt, estate, false)?;
                None
            }
        }
    } else {
        None
    };
    for (i, trigger) in trigdesc.triggers.iter().enumerate() {
        if trigger.tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | tgtype_event)
            != TRIGGER_TYPE_ROW | type_timing | tgtype_event
        {
            continue;
        }
        if !::trigger::TriggerEnabled(trigger) {
            continue;
        }
        // SAFETY (both): materialized query-context images; the slots are not
        // written while these handles live within this iteration.
        let mut old_t = raw_old.map(|(img, len, tid, oid)| unsafe {
            types_tuple::HeapTupleData::from_raw_parts(img, len, tid, oid)
        });
        let mut new_t = raw_new.map(|(img, len, tid, oid)| unsafe {
            types_tuple::HeapTupleData::from_raw_parts(img, len, tid, oid)
        });
        if trigger.tgnattr > 0 || trigger.tgqual.is_some() {
            let ModifyTableState { rels, cur, router, leaf_trig_when, .. } = &mut *mt;
            let r = &mut rels[*cur];
            let (rel, cache) = match leaf {
                None => (
                    estate.es_relations[(r.rti - 1) as usize]
                        .as_ref()
                        .expect("result relation opened"),
                    &mut r.trig_when,
                ),
                Some(ix) => (
                    router.as_ref().expect("routed insert has a router").leaf_rel(ix),
                    &mut leaf_trig_when[ix],
                ),
            };
            let mut when = ::trigger::TriggerWhenEval {
                mcx,
                cache,
                // Routed leaf: the leaf-numbered copy; else the target's own.
                modified_cols: leaf_updated_cols.as_ref().or(r.all_updated_cols.as_ref()),
            };
            if !when.check_tuples(i, trigger, rel, tg_event, old_t.as_ref(), new_t.as_ref())? {
                continue;
            }
        }
        // C: INSERT/DELETE put the affected row in tg_trigtuple; UPDATE
        // carries old in tg_trigtuple and new in tg_newtuple.
        let old_nn = old_t.as_mut().map(core::ptr::NonNull::from);
        let new_nn = new_t.as_mut().map(core::ptr::NonNull::from);
        let (trig_nn, newtup_nn) =
            if old_nn.is_some() { (old_nn, new_nn) } else { (new_nn, None) };
        let expected = if newtup_nn.is_some() { newtup_nn } else { trig_nn };
        // Stable across the call: nothing reassigns all_updated_cols after
        // ensure_all_updated_cols above, and leaf_updated_cols is a local
        // that outlives the loop.
        let updatedcols_ptr = if event_op == types_trigger::TRIGGER_EVENT_UPDATE {
            match &leaf_updated_cols {
                Some(b) => b as *const _ as usize,
                None => mt
                    .rel()
                    .all_updated_cols
                    .as_ref()
                    .map_or(0usize, |b| b as *const _ as usize),
            }
        } else {
            0
        };
        let (ret, rel_tupdesc) = {
            let ModifyTableState { rels, cur, leaf_trig_fmgr, router, .. } = &mut *mt;
            let cur_rti = rels[*cur].rti;
            let finfo = match leaf {
                None => rels[*cur].trig_fmgr.get(i, trigger.tgfoid)?,
                Some(ix) => leaf_trig_fmgr[ix].get(i, trigger.tgfoid)?,
            };
            let rel = match leaf {
                None => estate.es_relations[(cur_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                Some(ix) => {
                    router.as_ref().expect("routed insert has a router").leaf_rel(ix)
                }
            };
            let tupdesc = rel.rd_att.clone();
            let mut tdata = types_trigger_call::TriggerData::from_raw(
                tg_event, rel, trig_nn, newtup_nn, trigger,
            );
            tdata.tg_updatedcols = updatedcols_ptr;
            (::trigger::ExecCallTriggerFunc(mcx, &mut tdata, finfo)?, tupdesc)
        };
        match ret {
            None => return Ok(false),
            Some(p) if Some(p) == expected => {}
            Some(_) if is_delete => {}
            Some(p) => {
                // ExecBR{Insert,Update}Triggers replacement-tuple arm:
                // ExecForceStoreHeapTuple into the new slot, subsequent
                // triggers and the DML proper see the replaced row.
                let slot_id = new_slot.expect("insert/update BR has a new slot");
                // SAFETY: p is the trigger's returned tuple, live in the
                // per-call context; copied into the slot before reuse.
                let returned = unsafe { p.as_ref() };
                // check_modified_virtual_generated (trigger.c:6735), applied
                // before the store (trigger.c:2513, 3108): a trigger-set
                // non-null value in a virtual generated column reverts to
                // null so it is never stored.
                let nulled = check_modified_virtual_generated(mcx, &rel_tupdesc, returned)?;
                let returned = nulled.as_ref().map_or(returned, |t| t.as_tuple());
                let img = unsafe {
                    core::slice::from_raw_parts(returned.header_ptr(), returned.t_len as usize)
                };
                let mut buf = mcx::vec_with_capacity_in(mcx, img.len())?;
                mcx::vec_append_bytes(&mut buf, img)?;
                let ptr = buf.as_ptr();
                core::mem::forget(buf);
                // SAFETY: fresh query-context copy of the returned image.
                let copy = unsafe {
                    types_tuple::HeapTupleData::from_raw_parts(
                        ptr,
                        returned.t_len,
                        returned.t_self,
                        returned.t_tableOid,
                    )
                };
                exectuples::exec_force_store_heap_tuple(
                    copy,
                    &mut estate.es_tupleTable[slot_id.0 as usize],
                    mcx,
                )?;
                // ExecBRInsertTriggers (trigger.c): a cloned trigger's
                // replacement tuple may no longer satisfy the partition
                // constraint of the partition the row was routed to.
                if trigger.tgisclone && event_op == types_trigger::TRIGGER_EVENT_INSERT {
                    let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
                    let ModifyTableState { rels, cur, router, leaf_partition_check, .. } =
                        &mut *mt;
                    let r = &mut rels[*cur];
                    let (rel, pcheck) = match leaf {
                        Some(ix) => (
                            router.as_ref().expect("routed insert has a router").leaf_rel(ix),
                            &mut leaf_partition_check[ix],
                        ),
                        None => (
                            es_relations[(r.rti - 1) as usize]
                                .as_ref()
                                .expect("result relation opened"),
                            &mut r.partition_check,
                        ),
                    };
                    let slot = &mut es_tupleTable[slot_id.0 as usize];
                    if !execpartition::exec_partition_check(mcx, pcheck, rel, slot)? {
                        return Err(moved_row_before_trigger(mcx, trigger, rel));
                    }
                }
                let (raw, owned) = slot_raw_tuple(estate, slot_id)?;
                raw_new = Some(raw);
                _new_owned = owned;
            }
        }
    }
    Ok(true)
}

// GetTupleForTrigger (trigger.c): lock + fetch the target row into the
// trigger old slot. Ok(None) = row gone, skip the operation.
fn ensure_trig_old_slot<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> ExecSlotId {
    if mt.rel().trig_old_slot.is_none() {
        let mcx = estate.es_query_cxt;
        let (kind, desc) = {
            let rel = estate.es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        let slot = exectuples::make_tuple_table_slot(mcx, kind, Some(desc));
        let id = ExecSlotId(estate.es_tupleTable.len() as u32);
        estate.es_tupleTable.push(slot);
        mt.rel_mut().trig_old_slot = Some(id);
    }
    mt.rel().trig_old_slot.expect("just initialized")
}

// The wholerow-junk row identity of views (nodeModifyTable.c:4409-4470):
// rebuild the OLD view row; t_self invalid, t_tableOid invalid (historical
// view-trigger behavior).
fn fetch_wholerow_tuple<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<types_tuple::HeapTupleData<'mcx>> {
    Ok(fetch_wholerow_tuple_opt(mt, estate, plan_slot)?.expect("wholerow is NULL"))
}

// MERGE leg: a NULL wholerow means the outer join produced a NOT MATCHED row.
fn fetch_wholerow_tuple_opt<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<Option<types_tuple::HeapTupleData<'mcx>>> {
    debug_assert!(mt.rel().ri_RowIdAttNo > 0);
    let slot = &mut estate.es_tupleTable[plan_slot.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getattr(slot, mt.rel().ri_RowIdAttNo as i32, &mut isnull);
    if isnull {
        return Ok(None);
    }
    let hdr = datum.as_usize() as *const u8;
    // SAFETY: a composite datum is an in-memory HeapTupleHeader image
    // (RowExpr output, never toasted); live in the plan slot for this row.
    let t_len = unsafe {
        (*(hdr as *const types_tuple::htup::HeapTupleHeaderData)).datum_length()
    };
    let mut tid = ItemPointerData::default();
    ItemPointerSetInvalid(&mut tid);
    // SAFETY: image bounds established above.
    Ok(Some(unsafe {
        types_tuple::HeapTupleData::from_raw_parts(hdr, t_len, tid, types_core::InvalidOid)
    }))
}

// GetTupleForTrigger outcomes MERGE must tell apart: C's ExecMergeMatched
// maps a BEFORE ROW prologue's TM_SelfModified to the 21000/27000-exact
// errors, TM_Deleted to the NOT MATCHED flip instead of a silent skip, and
// TM_Updated (do_epq_recheck=false, trigger.c 3431-3436) to its own
// EvalPlanQual recheck, which may run a different action entirely.
enum MergeTrigFetch {
    Fetched(ExecSlotId),
    SelfModified(TM_FailureData),
    Deleted,
    Updated(TM_FailureData),
}

fn merge_tuple_for_trigger<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &ItemPointerData,
) -> PgResult<MergeTrigFetch> {
    let slot_id = ensure_trig_old_slot(mt, estate);
    let output_cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();
    let lock_result = {
        let mcx = estate.es_query_cxt;
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let flags = if xact::IsolationUsesXactSnapshot() {
            0
        } else {
            TUPLE_LOCK_FLAG_FIND_LAST_VERSION
        };
        tableam::table_tuple_lock(
            mcx,
            rel,
            tupleid,
            snapshot,
            &mut es_tupleTable[slot_id.0 as usize],
            output_cid,
            LockTupleMode::LockTupleExclusive,
            LockWaitPolicy::LockWaitBlock,
            flags,
            &mut tmfd,
        )?
    };
    match lock_result {
        TM_Result::TM_SelfModified => Ok(MergeTrigFetch::SelfModified(tmfd)),
        TM_Result::TM_Ok => {
            if tmfd.traversed {
                return Ok(MergeTrigFetch::Updated(tmfd));
            }
            Ok(MergeTrigFetch::Fetched(slot_id))
        }
        TM_Result::TM_Updated => {
            if xact::IsolationUsesXactSnapshot() {
                return Err(serialization_conflict("update"));
            }
            panic!("GetTupleForTrigger (trigger.c): unexpected table_tuple_lock status")
        }
        TM_Result::TM_Deleted => {
            if xact::IsolationUsesXactSnapshot() {
                return Err(serialization_conflict("delete"));
            }
            Ok(MergeTrigFetch::Deleted)
        }
        other => panic!("GetTupleForTrigger (trigger.c): unrecognized status {other:?}"),
    }
}

// GetTupleForTrigger (trigger.c) outcomes for the plain BR paths: Skip = C
// returned false; Proceed carries the locked old-row slot and, when a
// concurrent update was traversed and the EPQ recheck passed, the EPQ slot.
enum TrigFetch {
    Skip,
    Proceed { old_slot: ExecSlotId, epq: Option<ExecSlotId> },
}

fn get_tuple_for_trigger<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tupleid: &mut ItemPointerData,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<TrigFetch> {
    let slot_id = ensure_trig_old_slot(mt, estate);
    let output_cid = estate.es_output_cid;
    let mut tmfd = TM_FailureData::default();
    let lock_result = {
        let mcx = estate.es_query_cxt;
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let flags = if xact::IsolationUsesXactSnapshot() {
            0
        } else {
            TUPLE_LOCK_FLAG_FIND_LAST_VERSION
        };
        tableam::table_tuple_lock(
            mcx,
            rel,
            tupleid,
            snapshot,
            &mut es_tupleTable[slot_id.0 as usize],
            output_cid,
            LockTupleMode::LockTupleExclusive,
            LockWaitPolicy::LockWaitBlock,
            flags,
            &mut tmfd,
        )?
    };
    match lock_result {
        TM_Result::TM_SelfModified => {
            if tmfd.cmax != output_cid {
                return Err(self_modified_violation("updated"));
            }
            Ok(TrigFetch::Skip)
        }
        TM_Result::TM_Ok => {
            if tmfd.traversed {
                // C: table_tuple_lock wrote the locked version's tid back
                // through tid; recheck it via EPQ (do_epq_recheck arm; MERGE
                // stays on merge_tuple_for_trigger).
                *tupleid = estate.slot(slot_id).base().tts_tid;
                eval_plan_qual_slot(mt, estate);
                let rti = mt.rel().rti;
                let Some(epqslot) = epq_eval(&mut mt.epq_subs, estate, slot_id, rti)? else {
                    return Ok(TrigFetch::Skip);
                };
                return Ok(TrigFetch::Proceed { old_slot: slot_id, epq: Some(epqslot) });
            }
            Ok(TrigFetch::Proceed { old_slot: slot_id, epq: None })
        }
        TM_Result::TM_Updated => {
            if xact::IsolationUsesXactSnapshot() {
                return Err(serialization_conflict("update"));
            }
            panic!("GetTupleForTrigger (trigger.c): unexpected table_tuple_lock status")
        }
        TM_Result::TM_Deleted => {
            if xact::IsolationUsesXactSnapshot() {
                return Err(serialization_conflict("delete"));
            }
            Ok(TrigFetch::Skip)
        }
        other => panic!("GetTupleForTrigger (trigger.c): unrecognized status {other:?}"),
    }
}

// ExecEvalParamExec's pending-initplan arm, hoisted out of the interpreter
// (execscan precedent).
fn pre_eval_param_deps(
    state: Option<&ExprState<'_>>,
    estate: &mut EStateData<'_>,
) -> PgResult<()> {
    if let Some(st) = state {
        let deps = st.param_exec_deps();
        if !deps.is_empty() {
            executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }
    Ok(())
}

// RLS WCO quals can reference initplan params (policy with an uncorrelated
// sublink); resolve them while `estate` is still whole — the eval sites sit
// inside EStateData destructures (execscan note).
fn pre_eval_wco_param_deps(
    wcos: &mcx::PgVec<'_, WcoExpr<'_>>,
    kind: WCOKind,
    estate: &mut EStateData<'_>,
) -> PgResult<()> {
    for w in wcos.iter() {
        if w.kind == kind {
            pre_eval_param_deps(Some(&*w.state), estate)?;
        }
    }
    Ok(())
}

// ExecProcessReturning (nodeModifyTable.c): scan slot = the returned tuple,
// outer slot = the plan tuple, projected into the node's virtual result slot
// (C's econtext scantuple/outertuple + ExecProject).
fn exec_process_returning<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    cmd: CmdType,
    old_id: Option<ExecSlotId>,
    new_id: Option<ExecSlotId>,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    let result_id = mt.returning_slot.expect("RETURNING slot initialized");
    let (has_old, has_new) = {
        let ModifyTableState { rels, root, cur, insert_target_root, .. } = &*mt;
        let st = if *insert_target_root {
            root.as_ref().unwrap_or(&rels[0])
        } else {
            &rels[*cur]
        }
        .project_returning
        .as_deref()
        .expect("RETURNING projection built");
        (st.has_old(), st.has_new())
    };
    // C ExecInsert projects RETURNING on the routed leaf's ResultRelInfo over
    // the leaf-format tuple. A remapped leaf needs the leaf-translated
    // projection and the leaf work slot so system columns read the real
    // inserted tuple; for everything else the root-format projection over
    // the root slot (tid + tableoid carried over by exec_insert) is
    // byte-identical and keeps the OLD/NEW legs, so the insert leaf path
    // only engages for projections without OLD/NEW refs. insert_target_root
    // marks the cross-partition-UPDATE insert half's RETURNING call.
    //
    // ON CONFLICT DO UPDATE on a remapped leaf (oc_returning_leaf) differs:
    // the existing (OLD) and projected (NEW/tuple) slots are BOTH
    // leaf-format, so the leaf projection runs with the passed old/new
    // sources intact (C ExecUpdate on the leaf ResultRelInfo).
    let oc_leaf = mt.oc_returning_leaf.take();
    let ins_leaf = mt.last_insert_remapped.take();
    let leaf = match oc_leaf {
        Some(idx) => Some((idx, new_id.expect("DO UPDATE returned the leaf proj slot"))),
        None => match ins_leaf {
            Some(ws)
                if (cmd == CmdType::CMD_INSERT || mt.insert_target_root)
                    && !has_old
                    && !has_new =>
            {
                Some((mt.last_insert_leaf.expect("remapped insert routed to a leaf"), ws))
            }
            _ => None,
        },
    };
    if let Some((idx, _)) = leaf {
        resolve_leaf_returning(mt, estate, idx)?;
    }
    // C runs pending initplans lazily inside ExecProject (ExecEvalParamExec);
    // RETURNING-list $n params resolve here instead (execscan note).
    {
        let ModifyTableState { rels, cur, leaf_returning, .. } = &*mt;
        let deps = match leaf {
            Some((idx, _)) => leaf_returning[idx].as_deref(),
            None => rels[*cur].project_returning.as_deref(),
        }
        .expect("RETURNING projection built")
        .param_exec_deps();
        if !deps.is_empty() {
            executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }
    // The insert leaf path carries no OLD/NEW steps (gated above): keep both
    // sources empty so the slot-aliasing checks below see only
    // scan/plan/result (the root-format new slot may alias the plan slot).
    // The conflict leaf path keeps them: both are leaf-format.
    let keep_old_new = leaf.is_none() || oc_leaf.is_some();
    let old_src = match old_id {
        Some(id) if keep_old_new => Some(id),
        None if has_old => Some(exec_get_all_null_slot(mt, estate)?),
        _ => None,
    };
    let new_src = match new_id {
        Some(id) if keep_old_new => Some(id),
        None if has_new => Some(exec_get_all_null_slot(mt, estate)?),
        _ => None,
    };
    let tuple_slot = match leaf {
        Some((_, ws)) => ws,
        None => match cmd {
            CmdType::CMD_INSERT | CmdType::CMD_UPDATE => new_id.expect("returned new tuple"),
            CmdType::CMD_DELETE => old_id.expect("returned old tuple"),
            other => panic!(
                "ExecProcessReturning (nodeModifyTable.c): unrecognized commandType: {other:?}"
            ),
        },
    };
    // The node econtext for SubPlan evaluation inside RETURNING (C
    // ps_ExprContext): scan = returned tuple, outer = plan tuple; reset per
    // projected row (C ResetExprContext in the ExecModifyTable loop).
    let ec = mt.node_ecxt.expect("RETURNING ecxt created at init");
    estate.reset_expr_context(ec);
    let mcx = estate.es_query_cxt;
    let (t, p, r) = (tuple_slot.0 as usize, plan_slot.0 as usize, result_id.0 as usize);
    let (o, n) = (old_src.map(|x| x.0 as usize), new_src.map(|x| x.0 as usize));
    {
        let e = estate.ecxt_mut(ec);
        e.ecxt_scantuple = Some(tuple_slot);
        e.ecxt_innertuple = None;
        e.ecxt_outertuple = if p != t { Some(plan_slot) } else { None };
    }
    exectuples::exec_clear_tuple(&mut estate.es_tupleTable[r], mcx);
    let mut resume: Option<execexpr::Resume> = None;
    loop {
        let suspended = {
            let ModifyTableState { rels, root, cur, insert_target_root, leaf_returning, .. } =
                &mut *mt;
            // insert_target_root: cross-partition UPDATE / MERGE root-INSERT
            // RETURNING runs the root's projection (C evaluates the routed
            // destination's ri_projectReturning; our slots are root-format)
            // — except a remapped routed leaf, which projects its own
            // leaf-translated list over the leaf-format tuple.
            let state = match leaf {
                Some((idx, _)) => leaf_returning[idx].as_deref_mut(),
                None if *insert_target_root => root
                    .as_mut()
                    .unwrap_or(&mut rels[0])
                    .project_returning
                    .as_deref_mut(),
                None => rels[*cur].project_returning.as_deref_mut(),
            }
            .expect("RETURNING projection built");
            state.set_old_new_null(old_id.is_none(), new_id.is_none());
            // C mtstate->mt_merge_action: under MERGE, `cmd` is the fired
            // action's command type; MERGE_SUPPORT_FUNC steps read it.
            state.set_merge_action(Some(cmd));
            state.arm_result_mcx(mcx);
            let table: &mut [SlotData<'mcx>] = &mut estate.es_tupleTable;
            let tlen = table.len();
            assert!(t < tlen && p < tlen && r < tlen);
            assert!(r != t && r != p);
            for i in [o, n].into_iter().flatten() {
                assert!(i < tlen && i != r && (i == t || i != p));
            }
            if let (Some(o), Some(n)) = (o, n) {
                assert!(o == t || n == t || o != n);
            }
            let base = table.as_mut_ptr();
            // SAFETY: bounds-checked, result distinct from both inputs; when
            // the plan slot IS the tuple slot (INSERT without slot coercion)
            // only one &mut is derived and OUTER_VAR references panic loudly
            // in the interpreter.
            let scan = unsafe { &mut *base.add(t) };
            // SAFETY: as above; p != t makes the borrows disjoint.
            let outer = if p != t { Some(unsafe { &mut *base.add(p) }) } else { None };
            // SAFETY: as above; r is distinct from t and p.
            let result = unsafe { &mut *base.add(r) };
            let old = match o {
                None => execexpr::RetSlot::None,
                Some(i) if i == t => execexpr::RetSlot::Scan,
                // SAFETY: bounds/distinctness asserted above (not in {t,p,r,n}).
                Some(i) => execexpr::RetSlot::Slot(unsafe { &mut *base.add(i) }),
            };
            let new = match n {
                None => execexpr::RetSlot::None,
                Some(i) if i == t => execexpr::RetSlot::Scan,
                // SAFETY: as above (not in {t, p, r, o}).
                Some(i) => execexpr::RetSlot::Slot(unsafe { &mut *base.add(i) }),
            };
            let mut ret = execexpr::RetSlots { old, new };
            let mut slots = EvalSlots { scan: Some(scan), inner: None, outer };
            execexpr::exec_project_returning_outcome(
                state,
                &mut slots,
                &mut ret,
                result,
                resume.take(),
            )?
        };
        match suspended {
            None => {
                exectuples::exec_store_virtual_tuple(&mut estate.es_tupleTable[r]);
                return Ok(result_id);
            }
            Some(sus) => {
                let d = executils::run_subplan_eval(sus.sstate, estate, ec)?;
                resume = Some(sus.resume_with(d));
            }
        }
    }
}

// ExecGetAllNullSlot (execUtils.c): lazily-built all-NULL virtual slot in the
// dispatch-current result relation's row format.
fn exec_get_all_null_slot<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    if let Some(id) = mt.rel().ri_AllNullSlot {
        return Ok(id);
    }
    let mcx = estate.es_query_cxt;
    let desc = {
        let rel = estate.es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        rel.rd_att.clone()
    };
    let id = estate.exec_init_extra_tuple_slot(Some(desc), TupleSlotKind::Virtual);
    exectuples::exec_store_all_null_tuple(&mut estate.es_tupleTable[id.0 as usize], mcx);
    mt.rel_mut().ri_AllNullSlot = Some(id);
    Ok(id)
}

#[track_caller]
#[cold]
#[inline(never)]
fn self_modified_violation(verb: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "tuple to be {verb} was already modified by an operation triggered \
             by the current command"
        ))
        .with_sqlstate(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION)
        .with_hint(
            "Consider using an AFTER trigger instead of a BEFORE trigger to \
             propagate changes to other rows.",
        ),
    )
}

enum OnConflictOutcome {
    // The conflict was consumed; project RETURNING from the slot if any.
    Done(Option<ExecSlotId>),
    // Concurrent update/delete of the conflict tuple: redo from vlock.
    Retry,
}

// ExecInsert (nodeModifyTable.c), plain-heap + speculative (ON CONFLICT)
// arms. Returns the slot RETURNING should project from, or None when the row
// was consumed without producing one (DO NOTHING, or a DO UPDATE whose WHERE
// filtered). Row triggers are undetectable (no TrigDesc yet).
fn resolve_leaf_trigdesc<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    idx: usize,
) -> PgResult<Option<Rc<types_trigger::TriggerDesc<'static>>>> {
    while mt.leaf_trigdesc.len() <= idx {
        mt.leaf_trigdesc.push(None);
        mt.leaf_trig_fmgr.push(::trigger::TriggerFmgrCache::default());
        mt.leaf_trig_when.push(::trigger::TriggerWhenCache::default());
    }
    if mt.leaf_trigdesc[idx].is_none() {
        let rel = mt.router.as_ref().expect("routed insert has a router").leaf_rel(idx);
        let td = if rel.rd_hastriggers {
            relcache::RelationGetTriggerDesc(rel.rd_id)?
        } else {
            None
        };
        mt.leaf_trigdesc[idx] = Some(td);
    }
    Ok(mt.leaf_trigdesc[idx].clone().expect("just resolved"))
}

// ExecGetChildToRootMap (execUtils.c:1300): resolve and cache the current
// result relation's child->root attmap; the root itself (and layout-matched
// children) resolve to None.
fn ensure_child_to_root<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    if mt.rel().child_to_root.is_some() {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let map = match (&mt.root, mt.insert_target_root) {
        (Some(root), false) => {
            let child = estate.es_relations[(mt.rels[mt.cur].rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let root_rel = estate.es_relations[(root.rti - 1) as usize]
                .as_ref()
                .expect("root relation opened");
            tupdesc::build_attrmap_by_name_if_req(mcx, &child.rd_att, &root_rel.rd_att, false)?
        }
        _ => None,
    };
    mt.rel_mut().child_to_root = Some(map);
    Ok(())
}

// The routed-leaf leg of ExecGetChildToRootMap: leaves route from mt.rel()
// (the root target, or the CP update's re-route root).
fn ensure_leaf_child_to_root<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &EStateData<'mcx>,
    idx: usize,
) -> PgResult<()> {
    if mt.leaf_child_to_root[idx].is_some() {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let map = {
        let root = estate.es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let leaf = mt.router.as_ref().expect("routed insert has a router").leaf_rel(idx);
        tupdesc::build_attrmap_by_name_if_req(mcx, &leaf.rd_att, &root.rd_att, false)?
    };
    mt.leaf_child_to_root[idx] = Some(map);
    Ok(())
}

// ExecInitPartitionInfo's WCO leg (execPartition.c:556-615): the first WCO
// list's Vars translated to the routed leaf's attnos (map_variable_attnos
// over build_attrmap_by_name(leaf, first)) and compiled against the leaf
// layout — the check runs on the leaf-format tuple, so the failing-row
// DETAIL prints in the leaf's column order as C's does.
fn resolve_leaf_wco<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idx: usize,
) -> PgResult<()> {
    if mt.leaf_wco[idx].is_some() {
        return Ok(());
    }
    let node = mt.plan;
    let mcx = estate.es_query_cxt;
    let mut wcos: mcx::PgVec<'mcx, WcoExpr<'mcx>> = mcx::PgVec::new_in(mcx);
    if !node.withCheckOptionLists.is_nil() {
        let first_rti = mt.rels[0].rti;
        let (leaf_reltype, attmap) = {
            let first = estate.es_relations[(first_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let leaf = mt.router.as_ref().expect("routed insert has a router").leaf_rel(idx);
            let attmap = tupdesc::build_attrmap_by_name_if_req(
                mcx,
                &leaf.rd_att,
                &first.rd_att,
                false,
            )?;
            (leaf.rd_rel.reltype, attmap)
        };
        let params = estate.param_bind();
        let wlist = node
            .withCheckOptionLists
            .nth(0)
            .as_list()
            .expect("withCheckOptionLists cell is a List");
        for wco_node in wlist {
            let wco = wco_node.as_with_check_option().expect("WCO cell");
            let qual = wco
                .qual
                .expect("planned WCO has a qual")
                .as_list()
                .expect("WCO qual is an implicit-AND List after preprocessing");
            let mut mapped = types_nodes::list::NodeList::nil();
            for q in qual {
                let n = match &attmap {
                    None => q,
                    Some(map) => {
                        rewrite_manip::map_variable_attnos(
                            mcx, q, first_rti as i32, 0, map, leaf_reltype,
                        )?
                        .0
                    }
                };
                mapped.lappend(mcx, n)?;
            }
            let state = executils::with_subplan_compile_env(estate, |env| {
                execexpr::exec_init_qual_subplans(mcx, &mapped, params, env)
            })?
            .expect("planner dropped constant-true WCO quals");
            wcos.push(WcoExpr {
                kind: wco.kind,
                relname: wco.relname.expect("WCO relname"),
                polname: wco.polname,
                state,
            });
        }
    }
    mt.leaf_wco[idx] = Some(wcos);
    Ok(())
}

// ExecInitPartitionInfo's RETURNING leg (execPartition.c): an attno-remapped
// routed leaf projects RETURNING from its own layout — the first
// returningList with Vars translated to the leaf's attnos
// (map_partition_varattnos).
fn resolve_leaf_returning<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idx: usize,
) -> PgResult<()> {
    if mt.leaf_returning[idx].is_some() {
        return Ok(());
    }
    let node = mt.plan;
    let mcx = estate.es_query_cxt;
    let first_rti = mt.rels[0].rti;
    let rlist = node
        .returningLists
        .nth(0)
        .as_list()
        .expect("returningLists cell is a List");
    let (leaf_desc, leaf_reltype, attmap) = {
        let first = estate.es_relations[(first_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let leaf = mt.router.as_ref().expect("routed insert has a router").leaf_rel(idx);
        let attmap =
            tupdesc::build_attrmap_by_name_if_req(mcx, &leaf.rd_att, &first.rd_att, false)?;
        (leaf.rd_att.clone(), leaf.rd_rel.reltype, attmap)
    };
    let mut mapped = types_nodes::list::NodeList::nil();
    for tle_node in rlist {
        let n = match &attmap {
            None => tle_node,
            Some(map) => {
                rewrite_manip::map_variable_attnos(
                    mcx,
                    tle_node,
                    first_rti as i32,
                    0,
                    map,
                    leaf_reltype,
                )?
                .0
            }
        };
        mapped.lappend(mcx, n)?;
    }
    let params = estate.param_bind();
    let is_merge = node.operation == CmdType::CMD_MERGE;
    let proj = executils::with_subplan_compile_env(estate, |env| {
        if is_merge {
            execexpr::exec_build_merge_projection_info_subplans(
                mcx,
                &mapped,
                Some(&leaf_desc),
                params,
                env,
            )
        } else {
            execexpr::exec_build_projection_info_subplans(mcx, &mapped, Some(&leaf_desc), params, env)
        }
    })?;
    mt.leaf_returning[idx] = Some(proj);
    Ok(())
}

// Assemble the trigger crate's conversion spec from a resolved cache slot.
#[inline]
fn child_to_root_spec<'a, 'mcx>(
    cache: &'a Option<Option<mcx::PgVec<'mcx, i16>>>,
    child: &'a Relation<'mcx>,
    root: Option<&'a Relation<'mcx>>,
) -> Option<::trigger::ChildToRoot<'a, 'mcx>> {
    match (cache.as_ref().expect("child_to_root resolved"), root) {
        (Some(map), Some(root_rel)) => Some(::trigger::ChildToRoot {
            map,
            child_desc: child.rd_att.as_ref(),
            root_desc: root_rel.rd_att.as_ref(),
        }),
        _ => None,
    }
}

// The ExecARInsertTriggers call of ExecInsert: routed inserts fire the leaf's
// (cloned) triggers with the leaf relation (C: resultRelInfo is the leaf);
// transition capture always uses the root's state.
fn ar_insert_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    recheck_indexes: &[Oid],
    leaf: Option<usize>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let td = match leaf {
        None => mt.rel().trigdesc.clone(),
        Some(ix) => resolve_leaf_trigdesc(mt, ix)?,
    };
    if td.is_none() && mt.transition_capture.is_none() {
        return Ok(());
    }
    let new_tid = estate.es_tupleTable[slot_id.0 as usize].base().tts_tid;
    let result_rti = mt.rel().rti;
    match leaf {
        Some(ix) => ensure_leaf_child_to_root(mt, estate, ix)?,
        None => ensure_child_to_root(mt, estate)?,
    }
    let root_rti = mt.root.as_ref().map(|r| r.rti);
    let ModifyTableState {
        rels, cur, leaf_trig_when, leaf_child_to_root, transition_capture, router, operation, ..
    } = mt;
    let (rel, cache, conv) = match leaf {
        None => {
            let r = &mut rels[*cur];
            let rel = estate.es_relations[(result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let root_rel = root_rti.map(|rti| {
                estate.es_relations[(rti - 1) as usize]
                    .as_ref()
                    .expect("root relation opened")
            });
            let conv = child_to_root_spec(&r.child_to_root, rel, root_rel);
            (rel, &mut r.trig_when, conv)
        }
        Some(ix) => {
            let rel = router.as_ref().expect("routed insert has a router").leaf_rel(ix);
            let root_rel = estate.es_relations[(result_rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            let conv = child_to_root_spec(&leaf_child_to_root[ix], rel, Some(root_rel));
            (rel, &mut leaf_trig_when[ix], conv)
        }
    };
    let mut when = ::trigger::TriggerWhenEval { mcx, cache, modified_cols: None };
    // The INSERT half of a cross-partition UPDATE files the row into the
    // UPDATE NEW transition table (new-only ExecARUpdateTriggers); AR INSERT
    // triggers then run without the capture state (C's ar_insert_trig_tcs).
    let mut ar_tcs = transition_capture.as_ref();
    if *operation == CmdType::CMD_UPDATE
        && transition_capture.as_ref().is_some_and(|tc| tc.tcs_update_new_table)
    {
        // New-only capture (CP-update INSERT half): no row event is queued
        // (old/new one-sided), so tg_updatedcols is moot.
        ::trigger::ExecARUpdateTriggers(
            mcx, rel, td.as_deref(), None, None, None, Some(new_tid), &[],
            transition_capture.as_ref(), Some(&mut when), false, None, conv.as_ref(),
            None,
        )?;
        ar_tcs = None;
    }
    ::trigger::ExecARInsertTriggers(
        mcx,
        rel,
        td.as_deref(),
        new_tid,
        recheck_indexes,
        ar_tcs,
        Some(&mut when),
        conv.as_ref(),
    )
}

fn exec_insert<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let onconflict = mt.plan.onConflictAction;
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);

    if mt.rel().relkind == types_rel::RELKIND_VIEW {
        if !ir_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_INSERT,
            types_trigger::TRIGGER_EVENT_INSERT,
            None,
            Some(slot_id),
        )? {
            return Ok(None);
        }
        // Parent-view CHECK OPTIONs still apply after INSTEAD OF triggers
        // (C ExecInsert's shared WCO_VIEW_CHECK leg).
        if !mt.rel().wco_exprs.is_empty() {
            let ecxt = mt.node_ecxt;
            let r = &mut mt.rels[mt.cur];
            let rti = r.rti;
            exec_view_check_options(
                mcx,
                estate,
                ecxt,
                &mut r.wco_exprs,
                slot_id,
                WcoRel::Rti { rti, root_rti: None },
            )?;
        }
        if mt.canSetTag {
            estate.es_processed += 1;
        }
        return Ok(Some(slot_id));
    }

    let partitioned_target = mt.rel().relkind == types_rel::RELKIND_PARTITIONED_TABLE;
    if !partitioned_target
        && mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_insert_before_row)
    {
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_INSERT,
            types_trigger::TRIGGER_EVENT_INSERT,
            None,
            Some(slot_id),
            None,
        )? {
            return Ok(None);
        }
    }

    // ExecFindPartition first checks the routing root's own partition
    // constraint when the root is itself a partition (execPartition.c): no
    // point routing a tuple that doesn't belong in the root table itself —
    // e.g. a cross-partition UPDATE on a sub-partitioned parent whose new
    // row leaves the parent's own bounds errors here, not "no partition".
    {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState { rels, root, cur, insert_target_root, .. } = &mut *mt;
        let r = if *insert_target_root {
            root.as_mut().unwrap_or(&mut rels[0])
        } else {
            &mut rels[*cur]
        };
        let target = es_relations[(r.rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        if target.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE
            && target.rd_rel.relispartition
        {
            let slot = &mut es_tupleTable[slot_id.0 as usize];
            if !execpartition::exec_partition_check(mcx, &mut r.partition_check, target, slot)? {
                return Err(execpartition::partition_constraint_violation(mcx, target, slot, None, None));
            }
        }
    }

    // ExecPrepareTupleRouting: partitioned targets route to a leaf; slots are
    // shared unconverted (attno-remapped children are loud in the router).
    let leaf_idx = {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let target = es_relations[(mt.rel().rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        if target.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            let slot = &mut es_tupleTable[slot_id.0 as usize];
            let router = match mt.router.as_mut() {
                Some(r) => r,
                None => {
                    mt.router =
                        Some(execpartition::PartitionTupleRouting::new(mcx, target)?);
                    mt.router.as_mut().unwrap()
                }
            };
            let idx = router.find_partition(
                slot,
                mt.index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
            )?;
            while mt.leaf_indexes.len() <= idx {
                mt.leaf_indexes.push(None);
                mt.leaf_checks.push(None);
                mt.leaf_virtual_nn.push(None);
                mt.leaf_generated.push(None);
                mt.leaf_slots.push(None);
                mt.leaf_partition_check.push(None);
                mt.leaf_arbiters.push(None);
                mt.leaf_existing.push(None);
                mt.leaf_child_to_root.push(None);
                mt.leaf_wco.push(None);
                mt.leaf_returning.push(None);
                mt.leaf_on_conflict.push(None);
                mt.leaf_ri_checked.push(false);
            }
            // C ExecInitPartitionInfo's CheckValidResultRel(leaf, CMD_INSERT,
            // onConflictAction): ONCONFLICT_UPDATE requires the leaf to also
            // support UPDATE; the plain-table CMD_INSERT leg is a no-op, and
            // a foreign leaf always errors (no in-tree ExecForeignInsert).
            if !mt.leaf_ri_checked[idx] {
                mt.leaf_ri_checked[idx] = true;
                let lrel = mt.router.as_ref().expect("router built above").leaf_rel(idx);
                if lrel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
                    return Err(Box::new(
                        PgError::error(format!(
                            "cannot insert into foreign table \"{}\"",
                            String::from_utf8_lossy(lrel.rd_rel.relname.name_str())
                        ))
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                    ));
                }
                if mt.plan.onConflictAction
                    == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32
                {
                    execreplication_seams::check_cmd_replica_identity::call(
                        mcx,
                        mt.router.as_ref().expect("router built above").leaf_rel(idx),
                        CmdType::CMD_UPDATE,
                    )?;
                }
            }
            Some(idx)
        } else {
            None
        }
    };
    // C ExecInsert's *inserted_tuple/*insert_destrel out-params: the caller
    // (cross-partition update) needs the destination leaf for the root FK
    // update event.
    mt.last_insert_leaf = leaf_idx;

    // ExecPrepareTupleRouting: an attno-remapped leaf takes the tuple
    // converted into its own layout in a dedicated estate slot BEFORE any
    // leaf trigger sees it (C ri_RootToPartitionMap + ri_PartitionTupleSlot).
    let mut work_slot = slot_id;
    if let Some(idx) = leaf_idx {
        if mt.router.as_ref().unwrap().leaf_attrmap(idx).is_some() {
            if mt.leaf_slots[idx].is_none() {
                let (kind, desc) = {
                    let leaf = mt.router.as_ref().unwrap().leaf_rel(idx);
                    (tableam::table_slot_callbacks(leaf), leaf.rd_att.clone())
                };
                mt.leaf_slots[idx] =
                    Some(estate.exec_init_extra_tuple_slot(Some(desc), kind));
            }
            let lsid = mt.leaf_slots[idx].expect("just built");
            let map = mt.router.as_ref().unwrap().leaf_attrmap(idx).expect("checked");
            let EStateData { es_tupleTable, .. } = &mut *estate;
            let (s, e) = (slot_id.0 as usize, lsid.0 as usize);
            assert!(s != e && s < es_tupleTable.len() && e < es_tupleTable.len());
            let base = es_tupleTable.as_mut_ptr();
            // SAFETY: distinct in-bounds indices of one live slice.
            let (in_slot, out) = unsafe { (&mut *base.add(s), &mut *base.add(e)) };
            exectuples::execute_attr_map_slot(map, in_slot, out, mcx);
            work_slot = lsid;
        }
    }
    mt.last_insert_remapped = (work_slot != slot_id).then_some(work_slot);

    // An attno-remapped leaf gets its own DO UPDATE SET/WHERE state with Vars
    // mapped to the leaf's attnos (C ExecInitPartitionInfo's map != NULL leg,
    // execPartition.c:781-864); DO NOTHING needs no extra state.
    if onconflict == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32
        && work_slot != slot_id
    {
        resolve_leaf_on_conflict(mt, estate, leaf_idx.expect("remapped implies routed"))?;
    }

    // C fires BR INSERT on the routed leaf's ResultRelInfo (cloned triggers).
    let mut leaf_has_br_insert = false;
    if let Some(idx) = leaf_idx {
        let td = resolve_leaf_trigdesc(mt, idx)?;
        leaf_has_br_insert = td.as_ref().is_some_and(|t| t.trig_insert_before_row);
        if leaf_has_br_insert
            && !br_row_triggers(
                mt,
                estate,
                types_trigger::TRIGGER_TYPE_INSERT,
                types_trigger::TRIGGER_EVENT_INSERT,
                None,
                Some(work_slot),
                Some(idx),
            )?
        {
            return Ok(None);
        }
    }

    // Routed leaves check the first WCO list translated to their own attnos
    // (C ExecInitPartitionInfo builds ri_WithCheckOptions per leaf).
    if let Some(idx) = leaf_idx {
        if !mt.plan.withCheckOptionLists.is_nil() {
            resolve_leaf_wco(mt, estate, idx)?;
        }
    }

    {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState {
            rels,
            root,
            cur,
            insert_target_root,
            router,
            leaf_indexes,
            leaf_generated,
            ..
        } = &mut *mt;
        // A MERGE root INSERT over an inherited target runs entirely against
        // the root (C uses rootRelInfo; constraint exclusion can make rels[0]
        // an attno-remapped child).
        let r = if *insert_target_root && leaf_idx.is_none() {
            root.as_mut().unwrap_or(&mut rels[0])
        } else {
            &mut rels[*cur]
        };
        let (rel, indexes, gen_exprs) = match leaf_idx {
            Some(idx) => (
                router.as_ref().unwrap().leaf_rel(idx),
                &mut leaf_indexes[idx],
                &mut leaf_generated[idx],
            ),
            None => (
                es_relations[(r.rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                &mut r.indexes,
                &mut r.generated_exprs,
            ),
        };
        let remapped = work_slot != slot_id;
        let slot = &mut es_tupleTable[work_slot.0 as usize];

        slot.base_mut().tts_tableOid = rel.rd_id;
        if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
            if remapped {
                // Leaf-relative compile should agree with the leaf-layout
                // slot; unverified, loud.
                panic!(
                    "ExecInsert: stored generated columns on an attno-remapped \
                     partition not ported"
                );
            }
            exec_compute_stored_generated(mcx, gen_exprs, rel, slot)?;
        }
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;

        if rel.rd_rel.relhasindex && indexes.is_none() {
            *indexes = Some(execindexing::ExecOpenIndices(mcx, rel, onconflict != 0)?);
        }
    }

    // A routed leaf checks its own translated WCO list over the leaf-format
    // tuple, post BR triggers (C ExecInsert -> ri_WithCheckOptions built by
    // ExecInitPartitionInfo). Runs with `estate` whole: policy quals carry
    // SubPlans/initplans (executils subplan driver).
    {
        let ecxt = mt.node_ecxt;
        // Cross-partition-moving UPDATE (or MERGE UPDATE action) checks the
        // target's UPDATE policies through the routed insert (C
        // nodeModifyTable.c:1067-1090).
        let wco_kind = if mt.operation == CmdType::CMD_UPDATE
            || (mt.operation == CmdType::CMD_MERGE
                && mt.merge_active_cmd == Some(CmdType::CMD_UPDATE))
        {
            WCOKind::WCO_RLS_UPDATE_CHECK
        } else {
            WCOKind::WCO_RLS_INSERT_CHECK
        };
        let ModifyTableState { rels, root, cur, insert_target_root, leaf_wco, .. } = &mut *mt;
        let wcos = match leaf_idx {
            Some(idx) => leaf_wco[idx].as_mut(),
            None => {
                let r = if *insert_target_root {
                    root.as_mut().unwrap_or(&mut rels[0])
                } else {
                    &mut rels[*cur]
                };
                Some(&mut r.wco_exprs)
            }
        };
        if let Some(wcos) = wcos {
            if !wcos.is_empty() {
                exec_with_check_options(estate, ecxt, wcos, wco_kind, work_slot)?;
            }
        }
    }

    {
        // Copy-out RTE/perminfo handles for the cold partition-constraint
        // error path (the destructure below pins *estate). The perminfo RTE
        // is the routing target's: during a cross-partition UPDATE's
        // re-routed INSERT, rels[cur] is the SOURCE partition, whose RTE
        // carries no perminfo (execUtils.c GetResultRTEPermissionInfo via
        // ri_RootResultRelInfo).
        let target_rte = estate.es_range_table[(mt.rel().rti - 1) as usize];
        let perminfos = estate.es_rteperminfos;
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState {
            rels,
            root,
            cur,
            insert_target_root,
            router,
            leaf_checks,
            leaf_virtual_nn,
            leaf_partition_check,
            ..
        } = &mut *mt;
        let root_rti = root.as_ref().map_or(rels[0].rti, |rr| rr.rti);
        // On insert_target_root a routed leaf's errors report through the
        // root too (its on-the-fly ResultRelInfo has ri_RootResultRelInfo
        // set, execPartition.c ExecFindPartition) — no leaf_idx guard.
        let r = if *insert_target_root {
            root.as_mut().unwrap_or(&mut rels[0])
        } else {
            &mut rels[*cur]
        };
        let (rel, check_exprs, virtual_nn_exprs, pcheck) = match leaf_idx {
            Some(idx) => (
                router.as_ref().unwrap().leaf_rel(idx),
                &mut leaf_checks[idx],
                &mut leaf_virtual_nn[idx],
                &mut leaf_partition_check[idx],
            ),
            None => (
                es_relations[(r.rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                &mut r.check_exprs,
                &mut r.virtual_nn_exprs,
                &mut r.partition_check,
            ),
        };
        let remapped = work_slot != slot_id;
        let slot = &mut es_tupleTable[work_slot.0 as usize];

        // ri_RootResultRelInfo: a routed leaf's constraint errors report
        // the failing row in the root's rowtype (execMain.c). Routing always
        // starts at the routing root — for a cross-partition UPDATE's
        // re-routed INSERT that is mt.root, not the update's child result
        // rel (C ExecCrossPartitionUpdate inserts via rootResultRelInfo,
        // nodeModifyTable.c:2070).
        let err_root_rel = match leaf_idx {
            Some(_) => es_relations[(root_rti - 1) as usize].as_ref(),
            None => None,
        };
        // ExecGetInsertedCols/UpdatedCols via the target RTE's perminfo, in
        // the description rel's (root's) numbering (execUtils.c
        // GetResultRTEPermissionInfo).
        let mod_cols = {
            let rte = target_rte;
            let mut cols = types_nodes::Bitmapset::empty();
            if rte.perminfoindex > 0 {
                if let Some(pis) = perminfos {
                    let pi = pis
                        .nth(rte.perminfoindex as usize - 1)
                        .as_rte_permission_info()
                        .expect("permInfos cell");
                    cols = pi.insertedCols.union(&pi.updatedCols, mcx)?;
                }
            }
            cols
        };
        exec_constraints(
            mcx,
            check_exprs,
            virtual_nn_exprs,
            rel,
            slot,
            err_root_rel,
            Some(&mod_cols),
        )?;

        // ExecInsert (nodeModifyTable.c): direct INSERTs into a partition
        // check the partition constraint; routed tuples re-check only when a
        // leaf BR trigger could have changed the row.
        if rel.rd_rel.relispartition && (leaf_idx.is_none() || leaf_has_br_insert) {
            if !execpartition::exec_partition_check(mcx, pcheck, rel, slot)? {
                return Err(execpartition::partition_constraint_violation(
                    mcx,
                    rel,
                    slot,
                    Some(&mod_cols),
                    err_root_rel,
                ));
            }
        }

        // RETURNING reads the root-format slot; carry the leaf's tableoid.
        if remapped {
            es_tupleTable[slot_id.0 as usize].base_mut().tts_tableOid = rel.rd_id;
        }
    }

    let num_indices = match leaf_idx {
        Some(idx) => mt.leaf_indexes[idx].as_ref().map_or(0, |x| x.num_indices()),
        None => mt.rel_mut().indexes.as_ref().map_or(0, |x| x.num_indices()),
    };
    if onconflict != 0 && num_indices > 0 {
        // The speculative-insert (ON CONFLICT) ceremony, composed from the
        // four named OC seams (wave-5 WS-W §8.1 — pure code moves of the
        // former inline blocks; the statement stream is exec_insert's own,
        // unchanged): arbiter selection → the vlock retry loop of
        // [arbiter pre-check → committed-conflict dispatch |
        // speculative token insert/confirm/abort].
        oc_resolve_arbiters(mt, mcx, leaf_idx)?;
        let existing_id = resolve_existing_slot(mt, estate, leaf_idx);
        // vlock:
        loop {
            let mut conflict_tid = ItemPointerData::default();
            ItemPointerSetInvalid(&mut conflict_tid);

            let pre_ok = oc_check_arbiter_indexes(
                mt, estate, work_slot, existing_id, leaf_idx, &mut conflict_tid,
            )?;

            if !pre_ok {
                // Committed conflict tuple found.
                match oc_conflict_dispatch(
                    mt, estate, conflict_tid, work_slot, leaf_idx, epq_eval,
                )? {
                    OnConflictOutcome::Done(rslot) => return Ok(rslot),
                    OnConflictOutcome::Retry => continue,
                }
            }

            if oc_speculative_insert(mt, estate, work_slot, leaf_idx, &mut recheck_indexes)? {
                // Speculative conflict: another inserter won the race; redo
                // from vlock (the former inline `if spec_conflict { continue }`).
                continue;
            }
            break;
        }
    } else {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState {
            rels, root, cur, insert_target_root, router, leaf_indexes, index_eval_cx, ..
        } = &mut *mt;
        // Inheritance-root MERGE INSERT: the tuple lands in the root itself.
        let r = if *insert_target_root && leaf_idx.is_none() {
            root.as_mut().unwrap_or(&mut rels[*cur])
        } else {
            &mut rels[*cur]
        };
        let (rel, indexes) = match leaf_idx {
            Some(idx) => (
                router.as_ref().unwrap().leaf_rel(idx),
                &mut leaf_indexes[idx],
            ),
            None => (
                es_relations[(r.rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                &mut r.indexes,
            ),
        };
        let slot = &mut es_tupleTable[work_slot.0 as usize];

        tableam::table_tuple_insert(mcx, rel, slot, output_cid, 0, None)?;

        if let Some(indexes) = indexes.as_mut() {
            if indexes.num_indices() > 0 {
                recheck_indexes = execindexing::ExecInsertIndexTuples(
                    mcx,
                    index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                    indexes,
                    rel,
                    slot,
                    false,
                    None,
                    &[],
                    false,
                )?;
            }
        }
    }

    // C returns the leaf-format slot itself; our RETURNING reads the
    // root-format one, so carry the inserted tuple's tid over for new.ctid
    // (tableoid was copied before the insert).
    if work_slot != slot_id {
        let tid = estate.es_tupleTable[work_slot.0 as usize].base().tts_tid;
        estate.es_tupleTable[slot_id.0 as usize].base_mut().tts_tid = tid;
    }

    let ar_leaf = leaf_idx;
    ar_insert_triggers(mt, estate, work_slot, &recheck_indexes, ar_leaf)?;

    // Parent-view CHECK OPTIONs are checked after inserting (the qual must see
    // the actual row, post defaults/triggers); a routed leaf checks its
    // translated list over the leaf-format tuple (C's leaf resultRelInfo).
    match leaf_idx {
        Some(idx) if mt.leaf_wco[idx].as_ref().is_some_and(|w| !w.is_empty()) => {
            let mcx = estate.es_query_cxt;
            let target_rti = mt.rel().rti;
            let ecxt = mt.node_ecxt;
            let ModifyTableState { router, leaf_wco, .. } = &mut *mt;
            let rel = router.as_ref().expect("routed insert has a router").leaf_rel(idx);
            exec_view_check_options(
                mcx,
                estate,
                ecxt,
                leaf_wco[idx].as_mut().expect("checked"),
                work_slot,
                WcoRel::Leaf { rel, root_rti: target_rti },
            )?;
        }
        None if !mt.rel().wco_exprs.is_empty() => {
            let mcx = estate.es_query_cxt;
            let ecxt = mt.node_ecxt;
            let r = &mut mt.rels[mt.cur];
            let rti = r.rti;
            exec_view_check_options(
                mcx,
                estate,
                ecxt,
                &mut r.wco_exprs,
                slot_id,
                WcoRel::Rti { rti, root_rti: None },
            )?;
        }
        _ => {}
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(Some(slot_id))
}

// =============================================================================
// ON CONFLICT (speculative insertion) seams — wave-5 WS-W §8.1.
//
// PURE CODE MOVES out of `exec_insert`'s former inline vlock loop; every
// statement below is the loop's own, relocated behind a named seam so the
// ceremony has reviewable joints (the wave-2 mt_* seam discipline). The
// composition in `exec_insert` replays the original control flow exactly:
// `Done` = the former `return`, `Retry`/spec-conflict = the former
// `continue`, pre_ok fall-through = the former loop `break`. No seam is
// lane-aware: BOTH engines (the Volcano arm and the knob-gated DML lane
// host, lanev2/dml.rs) reach these through the SAME `mt_accept_row` →
// `exec_insert` chain, so the statement stream is identical by
// construction.
// =============================================================================

/// OC seam 1/4 — arbiter index selection (C ExecInitPartitionInfo's
/// ri_onConflictArbiterIndexes leg): a routed leaf arbitrates through its
/// own index children of the root arbiter indexes, resolved once per leaf
/// and cached in `leaf_arbiters`. The unrouted (root) case reads
/// `on_conflict.arbiters` directly in the pre-check seam and needs no
/// resolution here.
#[inline]
fn oc_resolve_arbiters<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    mcx: mcx::Mcx<'mcx>,
    leaf_idx: Option<usize>,
) -> PgResult<()> {
    // ExecInitPartitionInfo: a routed leaf arbitrates through its own
    // index children of the root arbiter indexes.
    if let Some(idx) = leaf_idx {
        if mt.leaf_arbiters[idx].is_none() {
            let mapped = resolve_leaf_arbiters(mt, mcx, idx)?;
            mt.leaf_arbiters[idx] = Some(mapped);
        }
    }
    Ok(())
}

/// OC seam 2/4 — the arbiter pre-check (C ExecCheckIndexConstraints call in
/// ExecInsert's ON CONFLICT arm): probe the arbiter indexes for a committed
/// conflicting tuple BEFORE inserting. `Ok(true)` = no conflict, proceed to
/// the speculative insertion; `Ok(false)` = committed conflict found,
/// `conflict_tid` names it (the caller dispatches DO NOTHING / DO UPDATE).
#[inline]
fn oc_check_arbiter_indexes<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    work_slot: ExecSlotId,
    existing_id: ExecSlotId,
    leaf_idx: Option<usize>,
    conflict_tid: &mut ItemPointerData,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let mut invalid_tid = ItemPointerData::default();
    ItemPointerSetInvalid(&mut invalid_tid);

    let ModifyTableState {
        rels, cur, on_conflict, index_eval_cx, router, leaf_indexes, leaf_arbiters, ..
    } = &mut *mt;
    let oc = on_conflict.as_ref().expect("on_conflict state");
    let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
    let (rel, indexes, arbiters): (_, _, &[Oid]) = match leaf_idx {
        Some(idx) => (
            router.as_ref().unwrap().leaf_rel(idx),
            leaf_indexes[idx].as_mut().expect("indexes opened"),
            leaf_arbiters[idx].as_deref().expect("just resolved"),
        ),
        None => (
            es_relations[(rels[*cur].rti - 1) as usize]
                .as_ref()
                .expect("result relation opened"),
            rels[*cur].indexes.as_mut().expect("indexes opened"),
            &oc.arbiters,
        ),
    };
    let (s, e) = (work_slot.0 as usize, existing_id.0 as usize);
    assert!(s != e && s < es_tupleTable.len() && e < es_tupleTable.len());
    let base = es_tupleTable.as_mut_ptr();
    // SAFETY: distinct in-bounds indices of one live slice.
    let (slot, existing) = unsafe { (&mut *base.add(s), &mut *base.add(e)) };
    execindexing::ExecCheckIndexConstraints(
        mcx,
        index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
        indexes,
        rel,
        slot,
        existing,
        &invalid_tid,
        arbiters,
        conflict_tid,
    )
}

/// OC seam 3/4 — DO NOTHING vs DO UPDATE dispatch over a committed conflict
/// tuple (the former `!pre_ok` arm): DO UPDATE delegates to
/// `exec_on_conflict_update` (whose `Retry` = redo from vlock); DO NOTHING
/// runs the C ExecCheckTIDVisible serialization-visibility check and
/// consumes the row (`Done(None)` ≡ the former `return Ok(None)`).
#[inline]
fn oc_conflict_dispatch<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    conflict_tid: ItemPointerData,
    work_slot: ExecSlotId,
    leaf_idx: Option<usize>,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<OnConflictOutcome> {
    if mt.plan.onConflictAction == types_nodes::OnConflictAction::ONCONFLICT_UPDATE as u32 {
        return exec_on_conflict_update(mt, estate, conflict_tid, work_slot, leaf_idx, epq_eval);
    }
    exec_check_tid_visible(mt, estate, &conflict_tid, leaf_idx)?;
    Ok(OnConflictOutcome::Done(None))
}

/// OC seam 4/4 — the speculative token insert/confirm/abort ceremony (C
/// table_tuple_insert_speculative .. table_tuple_complete_speculative under
/// the SpeculativeInsertionLock): insert the tuple speculatively, insert
/// index tuples with the arbiters in deferred-check mode, then confirm
/// (`!spec_conflict`) or kill (`spec_conflict`) the speculative tuple and
/// wake waiters. `Ok(true)` = a concurrent inserter won the race (the
/// caller redoes from vlock); `Ok(false)` = the insertion stands.
#[inline]
fn oc_speculative_insert<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    work_slot: ExecSlotId,
    leaf_idx: Option<usize>,
    recheck_indexes: &mut mcx::PgVec<'mcx, Oid>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let xid = xact::GetCurrentTransactionId()?;
    let spec_token = lmgr::SpeculativeInsertionLockAcquire(xid)?;
    let mut spec_conflict = false;
    {
        let ModifyTableState {
            rels, cur, on_conflict, index_eval_cx, router, leaf_indexes, leaf_arbiters, ..
        } = &mut *mt;
        let oc = on_conflict.as_ref().expect("on_conflict state");
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let (rel, indexes, arbiters): (_, _, &[Oid]) = match leaf_idx {
            Some(idx) => (
                router.as_ref().unwrap().leaf_rel(idx),
                leaf_indexes[idx].as_mut().expect("indexes opened"),
                leaf_arbiters[idx].as_deref().expect("just resolved"),
            ),
            None => (
                es_relations[(rels[*cur].rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
                rels[*cur].indexes.as_mut().expect("indexes opened"),
                &oc.arbiters,
            ),
        };
        let slot = &mut es_tupleTable[work_slot.0 as usize];
        tableam::table_tuple_insert_speculative(
            mcx, rel, slot, output_cid, 0, None, spec_token,
        )?;
        *recheck_indexes = execindexing::ExecInsertIndexTuples(
            mcx,
            index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
            indexes,
            rel,
            slot,
            true,
            Some(&mut spec_conflict),
            arbiters,
            false,
        )?;
        tableam::table_tuple_complete_speculative(
            mcx,
            rel,
            slot,
            spec_token,
            !spec_conflict,
        )?;
    }
    // Wake up anyone waiting for our verdict.
    lmgr::SpeculativeInsertionLockRelease(xid)?;
    Ok(spec_conflict)
}

// ExecUpdateLockMode (execMain.c): the conflicting row takes the weaker
// NoKeyExclusive lock when the DO UPDATE SET columns don't overlap any key
// column (ExecGetAllUpdatedCols vs INDEX_ATTR_BITMAP_KEY). Routed leaves map
// the root's updated columns through the root->leaf attrmap; leaf-local
// generated-column extras aren't recomputed (the root's, mapped, stand in).
fn on_conflict_update_lock_mode<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &EStateData<'mcx>,
    leaf: Option<usize>,
) -> PgResult<LockTupleMode> {
    const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
    let mcx = estate.es_query_cxt;
    ensure_all_updated_cols(mt, estate, false)?;
    let rti = mt.rel().rti;
    let rel = estate.es_relations[(rti - 1) as usize]
        .as_ref()
        .expect("result relation opened");
    let mut cols = mt
        .rel()
        .all_updated_cols
        .as_ref()
        .expect("resolved above")
        .clone_in(mcx)?;
    let rel_id = match leaf {
        None => rel.rd_id,
        Some(idx) => {
            let leaf_rel = mt.router.as_ref().expect("routed").leaf_rel(idx);
            if let Some(map) = tupdesc::build_attrmap_by_name_if_req(
                mcx,
                &rel.rd_att,
                &leaf_rel.rd_att,
                !leaf_rel.rd_rel.relispartition,
            )? {
                cols = execute_attr_map_cols(mcx, &map, &cols)?;
            }
            leaf_rel.rd_id
        }
    };
    let bitmaps = ::relcache_seams::relation_get_index_attr_bitmap::call(rel_id)?;
    let key_updated = bitmaps
        .key
        .iter()
        .any(|&attno| cols.is_member(attno as i32 - FLIHAN));
    Ok(if key_updated {
        LockTupleMode::LockTupleExclusive
    } else {
        LockTupleMode::LockTupleNoKeyExclusive
    })
}

// ExecOnConflictUpdate (nodeModifyTable.c): lock the conflict tuple, verify
// visibility, apply the DO UPDATE WHERE qual and SET projection, then run the
// plain UPDATE path against the locked tuple.
fn exec_on_conflict_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    conflict_tid: ItemPointerData,
    excluded_id: ExecSlotId,
    leaf: Option<usize>,
    epq_eval: &mut impl FnMut(
        &mut Option<executils::EpqSubs<'mcx>>,
        &mut EStateData<'mcx>,
        ExecSlotId,
        u32,
    ) -> PgResult<Option<ExecSlotId>>,
) -> PgResult<OnConflictOutcome> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    // A routed leaf's WCOs are the first list translated to the leaf's
    // attnos (C ExecInitPartitionInfo).
    if let Some(idx) = leaf {
        if !mt.plan.withCheckOptionLists.is_nil() {
            resolve_leaf_wco(mt, estate, idx)?;
        }
    }
    let existing_id = resolve_existing_slot(mt, estate, leaf);
    // An attno-remapped leaf carries its own SET/WHERE state and slots
    // (C reads them off the leaf's ri_onConflict either way).
    let remapped = leaf.filter(|&idx| mt.leaf_on_conflict[idx].is_some());
    let (setvals_id, proj_id) = match remapped {
        Some(idx) => {
            let l = mt.leaf_on_conflict[idx].as_ref().expect("resolved");
            (l.setvals_slot, l.proj_slot)
        }
        None => {
            let oc = mt.on_conflict.as_ref().expect("on_conflict state");
            (
                oc.setvals_slot.expect("DO UPDATE state"),
                oc.proj_slot.expect("DO UPDATE state"),
            )
        }
    };

    let mut tmfd = TM_FailureData::default();
    let lockmode = on_conflict_update_lock_mode(mt, estate, leaf)?;
    let lock_result = {
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = match leaf {
            Some(idx) => mt.router.as_ref().expect("routed").leaf_rel(idx),
            None => es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened"),
        };
        tableam::table_tuple_lock(
            mcx,
            rel,
            &conflict_tid,
            snapshot,
            &mut es_tupleTable[existing_id.0 as usize],
            output_cid,
            lockmode,
            LockWaitPolicy::LockWaitBlock,
            0,
            &mut tmfd,
        )?
    };

    match lock_result {
        TM_Result::TM_Ok => {}
        TM_Result::TM_Invisible => {
            // A row inserted by our own transaction later in the same
            // command, e.g. duplicate constrained values proposed at once.
            // C reads xmin off the lock slot; refetch under SnapshotAny.
            let found = {
                let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = &mut *estate;
                let rel = match leaf {
                    Some(idx) => mt.router.as_ref().expect("routed").leaf_rel(idx),
                    None => es_relations[(mt.rel().rti - 1) as usize]
                        .as_ref()
                        .expect("result relation opened"),
                };
                tableam::table_tuple_fetch_row_version(
                    *es_query_cxt,
                    rel,
                    &conflict_tid,
                    &mt.snapshot_any,
                    &mut es_tupleTable[existing_id.0 as usize],
                )?
            };
            assert!(found, "failed to fetch invisible conflicting tuple");
            let xmin = slot_xmin(estate, existing_id)?;
            if xact::TransactionIdIsCurrentTransactionId(xmin) {
                return Err(cardinality_violation());
            }
            panic!("attempted to lock invisible tuple");
        }
        TM_Result::TM_SelfModified => {
            panic!("unexpected self-updated tuple");
        }
        TM_Result::TM_Updated => {
            if xact::IsolationUsesXactSnapshot() {
                return Err(serialization_conflict("update"));
            }
            clear_slot(estate, existing_id);
            return Ok(OnConflictOutcome::Retry);
        }
        TM_Result::TM_Deleted => {
            if xact::IsolationUsesXactSnapshot() {
                return Err(serialization_conflict("delete"));
            }
            clear_slot(estate, existing_id);
            return Ok(OnConflictOutcome::Retry);
        }
        other => panic!(
            "ExecOnConflictUpdate (nodeModifyTable.c): unexpected \
             table_tuple_lock status: {other:?}"
        ),
    }

    exec_check_tuple_visible(mt, estate, existing_id, leaf)?;

    // EXCLUDED reads through INNER_VAR (setrefs), the existing tuple through
    // scan Vars; evaluate the WHERE qual then the SET projection that way.
    let use_subplans = {
        let (where_clause, set_proj) = match remapped {
            Some(idx) => {
                let l = mt.leaf_on_conflict[idx].as_ref().expect("resolved");
                (l.where_clause.as_deref(), Some(&*l.set_proj))
            }
            None => {
                let oc = mt.on_conflict.as_ref().expect("on_conflict state");
                (oc.where_clause.as_deref(), oc.set_proj.as_deref())
            }
        };
        pre_eval_param_deps(where_clause, estate)?;
        pre_eval_param_deps(set_proj, estate)?;
        // Conflict-check WCO policy quals carry SubPlans too (C evaluates
        // them through the same econtext); they force the driver arm.
        let wcos = match leaf {
            Some(idx) => mt.leaf_wco[idx].as_ref(),
            None => Some(&mt.rels[mt.cur].wco_exprs),
        };
        let wco_subplan = wcos.is_some_and(|ws| {
            ws.iter()
                .any(|w| w.kind == WCOKind::WCO_RLS_CONFLICT_CHECK && w.state.has_subplan())
        });
        where_clause.is_some_and(|q| q.has_subplan())
            || set_proj.is_some_and(|p| p.has_subplan())
            || wco_subplan
    };
    if use_subplans {
        let ec = mt.node_ecxt.expect("node ecxt created with ON CONFLICT UPDATE");
        estate.reset_expr_context(ec);
        {
            let e = estate.ecxt_mut(ec);
            e.ecxt_scantuple = Some(existing_id);
            e.ecxt_innertuple = Some(excluded_id);
            e.ecxt_outertuple = None;
        }
        let pass = {
            let where_clause = match remapped {
                Some(idx) => mt.leaf_on_conflict[idx]
                    .as_mut()
                    .expect("resolved")
                    .where_clause
                    .as_deref_mut(),
                None => mt
                    .on_conflict
                    .as_mut()
                    .expect("on_conflict state")
                    .where_clause
                    .as_deref_mut(),
            };
            executils::exec_qual_with_subplans(where_clause, estate, ec)?
        };
        if !pass {
            clear_slot(estate, existing_id);
            return Ok(OnConflictOutcome::Done(None));
        }
        {
            let ModifyTableState { rels, cur, leaf_wco, .. } = &mut *mt;
            let wcos = match leaf {
                Some(idx) => leaf_wco[idx].as_mut(),
                None => Some(&mut rels[*cur].wco_exprs),
            };
            if let Some(wcos) = wcos {
                if !wcos.is_empty() {
                    exec_with_check_options(
                        estate,
                        Some(ec),
                        wcos,
                        WCOKind::WCO_RLS_CONFLICT_CHECK,
                        existing_id,
                    )?;
                    // Restore the qual/projection bindings the WCO eval reset.
                    let e = estate.ecxt_mut(ec);
                    e.ecxt_scantuple = Some(existing_id);
                    e.ecxt_innertuple = Some(excluded_id);
                    e.ecxt_outertuple = None;
                }
            }
        }
        {
            let set_proj = match remapped {
                Some(idx) => &mut *mt.leaf_on_conflict[idx].as_mut().expect("resolved").set_proj,
                None => mt
                    .on_conflict
                    .as_mut()
                    .expect("on_conflict state")
                    .set_proj
                    .as_deref_mut()
                    .expect("DO UPDATE projection"),
            };
            executils::exec_project_with_subplans(set_proj, estate, ec, setvals_id)?;
        }
    } else {
        let ModifyTableState { rels, cur, on_conflict, leaf_wco, leaf_on_conflict, .. } =
            &mut *mt;
        let r = &mut rels[*cur];
        let oc = on_conflict.as_mut().expect("on_conflict state");
        let mut oc_leaf = match remapped {
            Some(idx) => leaf_on_conflict[idx].as_mut(),
            None => None,
        };
        {
            let wcos = match leaf {
                Some(idx) => leaf_wco[idx].as_ref(),
                None => Some(&r.wco_exprs),
            };
            if let Some(wcos) = wcos {
                pre_eval_wco_param_deps(wcos, WCOKind::WCO_RLS_CONFLICT_CHECK, estate)?;
            }
        }
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (e, x, v) = (
            existing_id.0 as usize,
            excluded_id.0 as usize,
            setvals_id.0 as usize,
        );
        assert!(e != x && e != v && x != v);
        assert!(e < es_tupleTable.len() && x < es_tupleTable.len() && v < es_tupleTable.len());
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (existing, excluded, setvals) =
            unsafe { (&mut *base.add(e), &mut *base.add(x), &mut *base.add(v)) };

        let mut slots = EvalSlots {
            scan: Some(existing),
            inner: Some(excluded),
            outer: None,
        };
        let where_clause = match &mut oc_leaf {
            Some(l) => l.where_clause.as_deref_mut(),
            None => oc.where_clause.as_deref_mut(),
        };
        if !execexpr::exec_qual(where_clause, &mut slots)? {
            exectuples::exec_clear_tuple(slots.scan.take().expect("scan slot"), mcx);
            return Ok(OnConflictOutcome::Done(None));
        }

        // C evaluates the leaf's translated WCOs (ExecInitPartitionInfo)
        // over the existing tuple.
        let wcos = match leaf {
            Some(idx) => leaf_wco[idx].as_mut(),
            None => Some(&mut r.wco_exprs),
        };
        if let Some(wcos) = wcos {
            if !wcos.is_empty() {
                let scan = slots.scan.take().expect("scan slot");
                exec_with_check_options_basic(wcos, WCOKind::WCO_RLS_CONFLICT_CHECK, scan)?;
                slots.scan = Some(scan);
            }
        }

        let set_proj = match &mut oc_leaf {
            Some(l) => &mut *l.set_proj,
            None => oc.set_proj.as_deref_mut().expect("DO UPDATE projection"),
        };
        execexpr::exec_project(set_proj, &mut slots, setvals, mcx)?;
    }

    // Merge SET values over the existing tuple into the projected new tuple.
    {
        let set_attnos: &[u16] = match remapped {
            Some(idx) => &mt.leaf_on_conflict[idx].as_ref().expect("resolved").set_attnos,
            None => &mt.on_conflict.as_ref().expect("on_conflict state").set_attnos,
        };
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let (e, v, p) = (
            existing_id.0 as usize,
            setvals_id.0 as usize,
            proj_id.0 as usize,
        );
        assert!(e != v && e != p && v != p);
        let base = es_tupleTable.as_mut_ptr();
        // SAFETY: distinct in-bounds indices of one live slice.
        let (existing, setvals, proj) =
            unsafe { (&mut *base.add(e), &mut *base.add(v), &mut *base.add(p)) };

        exectuples::slot_getallattrs(existing);
        exectuples::slot_getallattrs(setvals);
        exectuples::exec_clear_tuple(proj, mcx);
        {
            let (eb, vb) = (existing.base(), setvals.base());
            let pb = proj.base_mut();
            let natts = eb.tts_nvalid as usize;
            pb.tts_values[..natts].copy_from_slice(&eb.tts_values[..natts]);
            pb.tts_isnull[..natts].copy_from_slice(&eb.tts_isnull[..natts]);
            for (i, &attno) in set_attnos.iter().enumerate() {
                pb.tts_values[attno as usize - 1] = vb.tts_values[i];
                pb.tts_isnull[attno as usize - 1] = vb.tts_isnull[i];
            }
        }
        exectuples::exec_store_virtual_tuple(proj);
    }

    let mut tupleid = conflict_tid;
    // ON CONFLICT DO UPDATE refuses cross-partition moves inside exec_update
    // (invalid_on_update_specification), so CrossPart is unreachable here.
    let modified = match leaf {
        Some(idx) => exec_leaf_conflict_update(mt, estate, idx, &mut tupleid, existing_id, proj_id)?,
        None => matches!(
            exec_update(mt, estate, &mut tupleid, proj_id, epq_eval)?,
            UpdateResult::Modified
        ),
    };
    if modified && mt.rel().project_returning.is_some() {
        // C clears `existing` only after ExecUpdate's RETURNING projection.
        mt.oc_old_slot = Some(existing_id);
        // A remapped leaf's existing/proj slots are leaf-format: RETURNING
        // must run the leaf-translated projection (C ExecUpdate on the leaf
        // ResultRelInfo projects its own ri_projectReturning).
        mt.oc_returning_leaf = remapped;
    } else {
        clear_slot(estate, existing_id);
    }
    Ok(OnConflictOutcome::Done(if modified { Some(proj_id) } else { None }))
}

// ExecInitPartitionInfo's per-leaf oc_Existing (execPartition.c):
// table_slot_create on the routed leaf; the shared root slot is Virtual when
// the target is partitioned, which the heap AM lock/fetch callbacks reject.
fn resolve_existing_slot<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    leaf: Option<usize>,
) -> ExecSlotId {
    let Some(idx) = leaf else {
        return mt.on_conflict.as_ref().expect("on_conflict state").existing_slot;
    };
    if mt.leaf_existing[idx].is_none() {
        let (kind, desc) = {
            let rel = mt.router.as_ref().expect("routed").leaf_rel(idx);
            (tableam::table_slot_callbacks(rel), rel.rd_att.clone())
        };
        mt.leaf_existing[idx] = Some(estate.exec_init_extra_tuple_slot(Some(desc), kind));
    }
    mt.leaf_existing[idx].expect("just built")
}

// ExecInitPartitionInfo's arbiter mapping (nodeModifyTable.c side of
// execPartition.c): each root arbiter index resolves to the routed leaf's own
// index child (pg_inherits ancestry over index partition trees).
fn resolve_leaf_arbiters<'mcx>(
    mt: &ModifyTableState<'mcx>,
    mcx: mcx::Mcx<'mcx>,
    idx: usize,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let oc = mt.on_conflict.as_ref().expect("on_conflict state");
    let mut out: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    let indexes = mt.leaf_indexes[idx].as_ref().expect("indexes opened");
    for leaf_ix in indexes.descs.iter() {
        let leaf_oid = leaf_ix.rd_id;
        if oc.arbiters.contains(&leaf_oid) {
            out.push(leaf_oid);
            continue;
        }
        let ancestors = pg_inherits::get_partition_ancestors(mcx, leaf_oid)?;
        if ancestors.iter().any(|a| oc.arbiters.contains(a)) {
            out.push(leaf_oid);
        }
    }
    if !oc.arbiters.is_empty() && out.is_empty() {
        // C: ExecInitPartitionInfo asserts every arbiter maps.
        panic!("could not find arbiter index on partition");
    }
    Ok(out)
}

// ExecInitPartitionInfo's DO UPDATE leg for an attno-remapped leaf
// (execPartition.c:781-864): onConflictSet/Where get their Vars mapped to the
// leaf's attnos twice — INNER_VAR for the EXCLUDED pseudo-rel, firstVarno for
// the target — the SET colnos go through adjust_partition_colnos, and the
// projection slots are rebuilt over the leaf descriptor.
fn resolve_leaf_on_conflict<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idx: usize,
) -> PgResult<()> {
    if mt.leaf_on_conflict[idx].is_some() {
        return Ok(());
    }
    let node = mt.plan;
    let mcx = estate.es_query_cxt;
    let first_rti = mt.rels[0].rti;
    let (leaf_reltype, leaf_kind, leaf_desc, attmap) = {
        let first = estate.es_relations[(first_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let leaf = mt.router.as_ref().expect("routed insert has a router").leaf_rel(idx);
        let attmap = tupdesc::build_attrmap_by_name(mcx, &leaf.rd_att, &first.rd_att)?;
        (
            leaf.rd_rel.reltype,
            tableam::table_slot_callbacks(leaf),
            leaf.rd_att.clone(),
            attmap,
        )
    };
    let map_to_leaf = |n: Node<'mcx>| -> PgResult<Node<'mcx>> {
        let n = rewrite_manip::map_variable_attnos(
            mcx,
            n,
            types_nodes::primnodes::INNER_VAR,
            0,
            &attmap,
            leaf_reltype,
        )?
        .0;
        Ok(rewrite_manip::map_variable_attnos(
            mcx,
            n,
            first_rti as i32,
            0,
            &attmap,
            leaf_reltype,
        )?
        .0)
    };

    let mut onconflset = types_nodes::list::NodeList::nil();
    for tle in &node.onConflictSet {
        onconflset.lappend(mcx, map_to_leaf(tle)?)?;
    }
    let params = estate.param_bind();
    let set_proj = executils::with_subplan_compile_env(estate, |env| {
        execexpr::exec_build_projection_info_subplans(
            mcx,
            &onconflset,
            Some(&leaf_desc),
            params,
            env,
        )
    })?;
    let set_desc = execscan::exec_type_from_tl(mcx, &onconflset)?;
    let setvals_slot =
        estate.exec_init_extra_tuple_slot(Some(set_desc), TupleSlotKind::Virtual);
    let proj_slot = estate.exec_init_extra_tuple_slot(Some(leaf_desc), leaf_kind);

    // adjust_partition_colnos (execPartition.c): root SET colnos through the
    // child-to-root attrMap.
    let mut set_attnos: mcx::PgVec<'mcx, u16> = mcx::PgVec::new_in(mcx);
    for attno in node.onConflictCols.iter() {
        let leaf_attno = attmap[attno as usize - 1];
        assert!(leaf_attno > 0, "invalid ON CONFLICT SET column number");
        set_attnos.push(leaf_attno as u16);
    }
    // ExecBuildUpdateProjection (execExpr.c:580-601): junk entries
    // (MULTIEXPR SubPlans) follow the SET columns and are never
    // assigned; onConflictCols covers only the non-junk columns.
    let mut non_junk = 0usize;
    let mut seen_junk = false;
    for tle_node in &node.onConflictSet {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.resjunk {
            seen_junk = true;
        } else {
            assert!(!seen_junk, "onConflictSet tlist: junk before a SET column");
            non_junk += 1;
        }
    }
    assert_eq!(set_attnos.len(), non_junk);

    let mut where_clause = None;
    if let Some(where_node) = node.onConflictWhere {
        let qual = where_node
            .as_list()
            .expect("onConflictWhere is an implicit-AND List after preprocessing");
        let mut mapped = types_nodes::list::NodeList::nil();
        for q in qual {
            mapped.lappend(mcx, map_to_leaf(q)?)?;
        }
        where_clause = executils::with_subplan_compile_env(estate, |env| {
            execexpr::exec_init_qual_subplans(mcx, &mapped, params, env)
        })?;
    }

    mt.leaf_on_conflict[idx] = Some(LeafOnConflict {
        setvals_slot,
        proj_slot,
        set_proj,
        set_attnos,
        where_clause,
    });
    Ok(())
}

// The routed-leaf half of ExecOnConflictUpdate's ExecUpdate call: the locked
// existing tuple, the projected new tuple and all machinery live on the leaf
// (C runs ExecUpdate with the leaf's ResultRelInfo). Concurrency legs are
// unreachable — the caller holds the tuple lock.
fn exec_leaf_conflict_update<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idx: usize,
    tupleid: &mut ItemPointerData,
    existing_id: ExecSlotId,
    proj_id: ExecSlotId,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;

    // BR UPDATE row triggers on the leaf (cloned triggers).
    let td = resolve_leaf_trigdesc(mt, idx)?;
    if td.as_ref().is_some_and(|t| t.trig_update_before_row) {
        if !br_row_triggers(
            mt,
            estate,
            types_trigger::TRIGGER_TYPE_UPDATE,
            types_trigger::TRIGGER_EVENT_UPDATE,
            Some(existing_id),
            Some(proj_id),
            Some(idx),
        )? {
            return Ok(false);
        }
    }

    let mut tmfd = TM_FailureData::default();
    let mut lockmode = LockTupleMode::LockTupleExclusive;
    let mut update_indexes = TU_UpdateIndexes::TU_None;
    let result = {
        let ModifyTableState {
            router, leaf_checks, leaf_virtual_nn, leaf_generated, leaf_partition_check,
            rels, root, cur, ..
        } = &mut *mt;
        let root_rti = root.as_ref().map_or(rels[0].rti, |rr| rr.rti);
        let rel = router.as_ref().expect("routed").leaf_rel(idx);
        let EStateData {
            es_tupleTable,
            es_snapshot,
            es_relations,
            es_range_table,
            es_rteperminfos,
            es_crosscheck_snapshot,
            ..
        } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let crosscheck: &tableam_vocab::Snapshot<'mcx> = &*es_crosscheck_snapshot;
        let slot = &mut es_tupleTable[proj_id.0 as usize];

        slot.base_mut().tts_tableOid = rel.rd_id;
        if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
            exec_compute_stored_generated(mcx, &mut leaf_generated[idx], rel, slot)?;
        }
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;

        // ExecUpdateAct: the updated row may not leave this partition under
        // ON CONFLICT DO UPDATE.
        if rel.rd_rel.relispartition
            && !execpartition::exec_partition_check(mcx, &mut leaf_partition_check[idx], rel, slot)?
        {
            return Err(invalid_on_update_specification());
        }

        // ri_RootResultRelInfo leg: the routed leaf reports through the
        // target's (root's) rel + perminfo (execMain.c ExecConstraints).
        let (perm_rti, err_root_rel) = match root {
            Some(rr) => (rr.rti, es_relations[(rr.rti - 1) as usize].as_ref()),
            None => (rels[*cur].rti, es_relations[(rels[*cur].rti - 1) as usize].as_ref()),
        };
        let mod_cols = rte_modified_cols(mcx, &es_range_table[..], *es_rteperminfos, perm_rti)?;
        exec_constraints(
            mcx,
            &mut leaf_checks[idx],
            &mut leaf_virtual_nn[idx],
            rel,
            slot,
            err_root_rel,
            Some(&mod_cols),
        )?;

        tableam::table_tuple_update(
            mcx,
            rel,
            tupleid,
            slot,
            output_cid,
            snapshot,
            crosscheck,
            true,
            &mut tmfd,
            &mut lockmode,
            &mut update_indexes,
        )?
    };
    if result != TM_Result::TM_Ok {
        // The caller holds the conflict tuple lock; nothing else can move it.
        panic!("ExecOnConflictUpdate leaf update: unexpected {result:?} on a locked tuple");
    }

    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    {
        let ModifyTableState { router, leaf_indexes, index_eval_cx, .. } = &mut *mt;
        let rel = router.as_ref().expect("routed").leaf_rel(idx);
        let EStateData { es_tupleTable, .. } = &mut *estate;
        let slot = &mut es_tupleTable[proj_id.0 as usize];
        if let Some(indexes) = leaf_indexes[idx].as_mut() {
            if indexes.num_indices() > 0 && update_indexes != TU_UpdateIndexes::TU_None {
                recheck_indexes = execindexing::ExecInsertIndexTuples(
                    mcx,
                    index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                    indexes,
                    rel,
                    slot,
                    false,
                    None,
                    &[],
                    update_indexes == TU_UpdateIndexes::TU_Summarizing,
                )?;
            }
        }
    }

    // Transition capture runs even when the leaf has no cloned row triggers
    // (C ExecARUpdateTriggers fires on mt_oc_transition_capture alone).
    let has_capture = mt
        .oc_transition_capture
        .as_ref()
        .is_some_and(|tc| tc.tcs_update_old_table || tc.tcs_update_new_table);
    if td.is_some() || has_capture {
        // C ExecARUpdateTriggers hands ExecGetAllUpdatedCols to every queued
        // UPDATE event; the leaf's copy is the root's mapped through the
        // root->leaf attrmap, feeding both the UPDATE OF/WHEN gate and the
        // event's tg_updatedcols.
        let leaf_cols = leaf_all_updated_cols(mt, estate, idx)?;
        let ar_new_tid = estate.es_tupleTable[proj_id.0 as usize].base().tts_tid;
        // Captured rows convert to the root layout through the leaf's
        // child->root map (C ri_ChildToRootMap in AfterTriggerSaveEvent).
        ensure_leaf_child_to_root(mt, estate, idx)?;
        let root_rti = mt.rel().rti;
        let ModifyTableState {
            router, leaf_trig_when, leaf_child_to_root, oc_transition_capture, ..
        } = &mut *mt;
        let rel = router.as_ref().expect("routed").leaf_rel(idx);
        let root_rel = estate.es_relations[(root_rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let conv = child_to_root_spec(&leaf_child_to_root[idx], rel, Some(root_rel));
        let mut when = ::trigger::TriggerWhenEval {
            mcx,
            cache: &mut leaf_trig_when[idx],
            modified_cols: Some(&leaf_cols),
        };
        ::trigger::ExecARUpdateTriggers(
            mcx,
            rel,
            td.as_deref(),
            None,
            None,
            Some(*tupleid),
            Some(ar_new_tid),
            &recheck_indexes,
            oc_transition_capture.as_ref(),
            Some(&mut when),
            false,
            conv.as_ref(),
            conv.as_ref(),
            Some(&leaf_cols),
        )?;
    }

    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(true)
}

// ExecCheckTIDVisible (nodeModifyTable.c): under xact-snapshot isolation the
// DO NOTHING skip must not be based on a tuple invisible to our snapshot.
fn exec_check_tid_visible<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tid: &ItemPointerData,
    leaf: Option<usize>,
) -> PgResult<()> {
    if !xact::IsolationUsesXactSnapshot() {
        return Ok(());
    }
    let existing_id = resolve_existing_slot(mt, estate, leaf);
    let found = {
        let EStateData { es_relations, es_tupleTable, es_query_cxt, .. } = &mut *estate;
        let rel = match leaf {
            Some(idx) => mt.router.as_ref().expect("routed").leaf_rel(idx),
            None => es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened"),
        };
        tableam::table_tuple_fetch_row_version(
            *es_query_cxt,
            rel,
            tid,
            &mt.snapshot_any,
            &mut es_tupleTable[existing_id.0 as usize],
        )?
    };
    assert!(found, "failed to fetch conflicting tuple for ON CONFLICT");
    exec_check_tuple_visible(mt, estate, existing_id, leaf)?;
    clear_slot(estate, existing_id);
    Ok(())
}

// ExecCheckTupleVisible (nodeModifyTable.c).
fn exec_check_tuple_visible<'mcx>(
    mt: &ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    leaf: Option<usize>,
) -> PgResult<()> {
    if !xact::IsolationUsesXactSnapshot() {
        return Ok(());
    }
    let visible = {
        let EStateData { es_relations, es_tupleTable, es_snapshot, .. } = &mut *estate;
        let snapshot: &tableam_vocab::Snapshot<'mcx> = &*es_snapshot;
        let rel = match leaf {
            Some(idx) => mt.router.as_ref().expect("routed").leaf_rel(idx),
            None => es_relations[(mt.rel().rti - 1) as usize]
                .as_ref()
                .expect("result relation opened"),
        };
        tableam::table_tuple_satisfies_snapshot(
            rel,
            &mut es_tupleTable[slot_id.0 as usize],
            snapshot,
        )?
    };
    if !visible {
        let xmin = slot_xmin(estate, slot_id)?;
        // A conflict against our own transaction's tuple isn't a
        // serialization failure (duplicate keys proposed in one command).
        if !xact::TransactionIdIsCurrentTransactionId(xmin) {
            return Err(serialization_conflict("update"));
        }
    }
    Ok(())
}

fn slot_xmin(estate: &EStateData<'_>, slot_id: ExecSlotId) -> PgResult<types_core::TransactionId> {
    let slot = &estate.es_tupleTable[slot_id.0 as usize];
    let mut isnull = false;
    let datum = exectuples::slot_getsysattr(
        slot,
        types_tuple::htup::MinTransactionIdAttributeNumber,
        &mut isnull,
    )?;
    debug_assert!(!isnull);
    Ok(datum.as_usize() as types_core::TransactionId)
}

fn clear_slot<'mcx>(estate: &mut EStateData<'mcx>, slot_id: ExecSlotId) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(&mut estate.es_tupleTable[slot_id.0 as usize], mcx);
}

#[track_caller]
#[cold]
#[inline(never)]
fn cardinality_violation() -> Box<PgError> {
    Box::new(
        PgError::error("ON CONFLICT DO UPDATE command cannot affect row a second time")
            .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION)
            .with_hint(
                "Ensure that no rows proposed for insertion within the same command \
                 have duplicate constrained values.",
            ),
    )
}

// ExecComputeStoredGenerated + ExecInitGenerated (nodeModifyTable.c). The
// slot must be virtual: retained by-ref values point at subplan/projection
// memory that survives the clear+restore (C datumCopies instead).
pub fn exec_compute_stored_generated<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    generated_exprs: &mut Option<mcx::PgVec<'mcx, GeneratedExpr<'mcx>>>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    let constr = rel.rd_att.constr.as_deref().expect("caller checked");
    if generated_exprs.is_none() {
        let mut compiled: mcx::PgVec<'mcx, GeneratedExpr<'mcx>> = mcx::PgVec::new_in(mcx);
        for i in 0..rel.rd_att.natts as usize {
            if rel.rd_att.attr(i).attgenerated == 0 {
                continue;
            }
            let adbin = constr
                .defval
                .iter()
                .find(|d| d.adnum == (i + 1) as i16)
                .and_then(|d| d.adbin.as_ref())
                .unwrap_or_else(|| {
                    panic!(
                        "no generation expression found for column number {} of table \"{}\"",
                        i + 1,
                        String::from_utf8_lossy(rel.rd_rel.relname.name_str())
                    )
                });
            // cookDefault coerced the stored tree to the column type, so
            // build_column_default's re-coercion is a no-op; skipped.
            let node = readfuncs::stringToNode(mcx, adbin.as_str())?;
            let mut state = execexpr::exec_init_expr(mcx, Some(node), execexpr::ParamBind::NONE)?
                .expect("generation expr");
            state.arm_result_mcx(mcx);
            compiled.push(GeneratedExpr { attnum: i, state });
        }
        *generated_exprs = Some(compiled);
    }

    exectuples::slot_getallattrs(slot);
    let exprs = generated_exprs.as_mut().expect("just built");
    let mut results: mcx::PgVec<'mcx, (usize, Datum, bool)> = mcx::PgVec::new_in(mcx);
    results
        .try_reserve_exact(exprs.len())
        .map_err(|_| Box::new(mcx.oom(exprs.len() * 24)))?;
    for ge in exprs.iter_mut() {
        let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
        let r = execexpr::exec_eval_expr(&mut ge.state, &mut slots)?;
        results.push((ge.attnum, r.value, r.isnull));
    }
    // C copies every by-ref datum (old and computed) before the clear frees
    // the backing image; the copies live in the query context, not C's
    // per-tuple context — WATCH bulk-insert memory growth.
    let natts = rel.rd_att.natts as usize;
    let mut values: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut nulls: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    {
        let base = slot.base_mut();
        values.extend(base.tts_values.iter().copied());
        nulls.extend(base.tts_isnull.iter().copied());
    }
    for &(attnum, value, isnull) in results.iter() {
        values[attnum] = value;
        nulls[attnum] = isnull;
    }
    for i in 0..natts {
        let att = rel.rd_att.attr(i);
        if !nulls[i] && !att.attbyval {
            values[i] = copy_by_ref_datum(mcx, values[i], att.attlen)?;
        }
    }
    exectuples::exec_clear_tuple(slot, mcx);
    let base = slot.base_mut();
    for i in 0..natts {
        base.tts_values[i] = values[i];
        base.tts_isnull[i] = nulls[i];
    }
    exectuples::exec_store_virtual_tuple(slot);
    Ok(())
}

fn copy_by_ref_datum<'mcx>(mcx: mcx::Mcx<'mcx>, d: Datum, attlen: i16) -> PgResult<Datum> {
    let p = d.as_usize() as *const u8;
    let size = match attlen {
        // SAFETY: non-null by-ref datum points at a live varlena image.
        -1 => unsafe { types_tuple::varatt::varsize_any(p) },
        // SAFETY: cstring datum is NUL-terminated.
        -2 => unsafe {
            let mut n = 0usize;
            while *p.add(n) != 0 {
                n += 1;
            }
            n + 1
        },
        l => l as usize,
    };
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, size)?;
    // SAFETY: size bytes are readable per the attlen contract above.
    unsafe {
        core::ptr::copy_nonoverlapping(p, buf.as_mut_ptr(), size);
        buf.set_len(size);
    }
    Ok(Datum::from_usize(buf.leak().as_ptr() as usize))
}

// ExecWithCheckOptions (execMain.c): NULL or false qual = violation for
// every kind (ExecQual semantics). Only for WCOs proven subplan-free (the
// on-conflict driver-arm gate); initplan params must be pre-evaluated by the
// caller. Subplan-bearing WCOs go through `exec_with_check_options`.
fn exec_with_check_options_basic<'mcx>(
    wcos: &mut mcx::PgVec<'mcx, WcoExpr<'mcx>>,
    kind: WCOKind,
    slot: &mut SlotData<'mcx>,
) -> PgResult<()> {
    for w in wcos.iter_mut() {
        if w.kind != kind {
            continue;
        }
        let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
        if !execexpr::exec_qual(Some(&mut *w.state), &mut slots)? {
            return Err(wco_violation(w));
        }
    }
    Ok(())
}

// ExecWithCheckOptions with the executils subplan driver: policy quals carry
// SubPlans/initplans (exec_view_check_options shape).
fn exec_with_check_options<'mcx>(
    estate: &mut EStateData<'mcx>,
    ecxt: Option<executils::EcxtId>,
    wcos: &mut mcx::PgVec<'mcx, WcoExpr<'mcx>>,
    kind: WCOKind,
    slot_id: ExecSlotId,
) -> PgResult<()> {
    for w in wcos.iter_mut() {
        if w.kind != kind {
            continue;
        }
        pre_eval_param_deps(Some(&*w.state), estate)?;
        let ok = if w.state.has_subplan() {
            let ec = ecxt.expect("node ecxt created with WCO");
            estate.reset_expr_context(ec);
            {
                let e = estate.ecxt_mut(ec);
                e.ecxt_scantuple = Some(slot_id);
                e.ecxt_innertuple = None;
                e.ecxt_outertuple = None;
            }
            executils::exec_qual_with_subplans(Some(&mut *w.state), estate, ec)?
        } else {
            let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
            let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
            execexpr::exec_qual(Some(&mut *w.state), &mut slots)?
        };
        if !ok {
            return Err(wco_violation(w));
        }
    }
    Ok(())
}

// ExecWithCheckOptions WCO_VIEW_CHECK (execMain.c): a child result rel's
// failing row is reported in the ROOT's rowtype (ri_RootResultRelInfo
// reverse-attmap leg, as ExecConstraints).
enum WcoRel<'a, 'mcx> {
    Rti { rti: u32, root_rti: Option<u32> },
    Leaf { rel: &'a Relation<'mcx>, root_rti: u32 },
}

// ExecWithCheckOptions (execMain.c), WCO_VIEW_CHECK arm.
fn exec_view_check_options<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    estate: &mut EStateData<'mcx>,
    ecxt: Option<executils::EcxtId>,
    wcos: &mut mcx::PgVec<'mcx, WcoExpr<'mcx>>,
    slot_id: ExecSlotId,
    rel: WcoRel<'_, 'mcx>,
) -> PgResult<()> {
    let mut failing = None;
    for (i, w) in wcos.iter_mut().enumerate() {
        if w.kind != WCOKind::WCO_VIEW_CHECK {
            continue;
        }
        pre_eval_param_deps(Some(&*w.state), estate)?;
        let ok = if w.state.has_subplan() {
            let ec = ecxt.expect("node ecxt created with WCO");
            estate.reset_expr_context(ec);
            {
                let e = estate.ecxt_mut(ec);
                e.ecxt_scantuple = Some(slot_id);
                e.ecxt_innertuple = None;
                e.ecxt_outertuple = None;
            }
            executils::exec_qual_with_subplans(Some(&mut *w.state), estate, ec)?
        } else {
            let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
            let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
            execexpr::exec_qual(Some(&mut *w.state), &mut slots)?
        };
        if !ok {
            failing = Some(i);
            break;
        }
    }
    let Some(i) = failing else { return Ok(()) };
    let EStateData { es_relations, es_tupleTable, es_range_table, es_rteperminfos, .. } =
        &mut *estate;
    let (vrel, root_rel, perm_rti) = match rel {
        WcoRel::Rti { rti, root_rti } => (
            es_relations[(rti - 1) as usize].as_ref().expect("result relation opened"),
            root_rti.map(|r| {
                es_relations[(r - 1) as usize].as_ref().expect("root relation opened")
            }),
            root_rti.unwrap_or(rti),
        ),
        WcoRel::Leaf { rel, root_rti } => (
            rel,
            Some(
                es_relations[(root_rti - 1) as usize]
                    .as_ref()
                    .expect("result relation opened"),
            ),
            root_rti,
        ),
    };
    // ExecWithCheckOptions (execMain.c): the failing-row DETAIL is filtered by
    // column privileges plus the target RTE perminfo's modified columns.
    let mod_cols = rte_modified_cols(mcx, &es_range_table[..], *es_rteperminfos, perm_rti)?;
    let slot = &mut es_tupleTable[slot_id.0 as usize];
    Err(view_wco_violation(mcx, wcos[i].relname, vrel, slot, root_rel, Some(&mod_cols)))
}

#[track_caller]
#[cold]
#[inline(never)]
fn view_wco_violation<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    relname: &str,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    root_rel: Option<&Relation<'mcx>>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> Box<PgError> {
    let mut e = PgError::error(format!(
        "new row violates check option for view \"{relname}\""
    ))
    .with_sqlstate(types_error::ERRCODE_WITH_CHECK_OPTION_VIOLATION);
    if let Ok(Some(desc)) = root_slot_value_description(mcx, rel, slot, root_rel, modified_cols) {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    Box::new(e)
}

#[track_caller]
#[cold]
#[inline(never)]
fn wco_violation(w: &WcoExpr<'_>) -> Box<PgError> {
    let relname = w.relname;
    let msg = match w.kind {
        WCOKind::WCO_RLS_INSERT_CHECK | WCOKind::WCO_RLS_UPDATE_CHECK => match w.polname {
            Some(p) => format!(
                "new row violates row-level security policy \"{p}\" for table \"{relname}\""
            ),
            None => {
                format!("new row violates row-level security policy for table \"{relname}\"")
            }
        },
        WCOKind::WCO_RLS_CONFLICT_CHECK => match w.polname {
            Some(p) => format!(
                "new row violates row-level security policy \"{p}\" (USING expression) \
                 for table \"{relname}\""
            ),
            None => format!(
                "new row violates row-level security policy (USING expression) for \
                 table \"{relname}\""
            ),
        },
        WCOKind::WCO_RLS_MERGE_UPDATE_CHECK | WCOKind::WCO_RLS_MERGE_DELETE_CHECK => {
            match w.polname {
                Some(p) => format!(
                    "target row violates row-level security policy \"{p}\" (USING \
                     expression) for table \"{relname}\""
                ),
                None => format!(
                    "target row violates row-level security policy (USING expression) \
                     for table \"{relname}\""
                ),
            }
        }
        WCOKind::WCO_VIEW_CHECK => unreachable!("routed via exec_view_check_options"),
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE))
}

// ExecConstraints (execMain.c): NOT NULL + CHECK arms live.
pub fn exec_constraints<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    check_exprs: &mut Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>,
    virtual_nn_exprs: &mut Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    root_rel: Option<&Relation<'mcx>>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<()> {
    if let Some(constr) = rel.rd_att.constr.as_deref() {
        if constr.has_not_null {
            exec_not_null_constraints(mcx, rel, slot, root_rel, modified_cols)?;
            if constr.has_generated_virtual {
                if let Some(i) = exec_rel_gen_virtual_notnull(mcx, virtual_nn_exprs, rel, slot)? {
                    return Err(not_null_violation(mcx, rel, slot, i, root_rel, modified_cols));
                }
            }
        }
        if constr.num_check > 0 {
            if let Some(failed) = exec_rel_check(mcx, check_exprs, rel, slot)? {
                return Err(check_violation(mcx, rel, slot, failed, root_rel, modified_cols));
            }
        }
    }
    Ok(())
}

// ExecRelCheck (execMain.c): compile once into check_exprs, evaluate with the
// slot as the scan tuple; ExecCheck semantics (NULL result passes). Returns
// the failing constraint's index.
fn exec_rel_check<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    check_exprs: &mut Option<mcx::PgVec<'mcx, CheckExpr<'mcx>>>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Option<usize>> {
    let constr = rel.rd_att.constr.as_deref().expect("caller checked");
    assert!(
        constr.check.len() == constr.num_check as usize,
        "{} pg_constraint record(s) missing for relation \"{}\"",
        constr.num_check as usize - constr.check.len(),
        String::from_utf8_lossy(rel.rd_rel.relname.name_str()),
    );
    if check_exprs.is_none() {
        let mut compiled: mcx::PgVec<'mcx, CheckExpr<'mcx>> = mcx::PgVec::new_in(mcx);
        compiled.try_reserve_exact(constr.check.len()).map_err(|_| {
            Box::new(mcx.oom(constr.check.len() * core::mem::size_of::<CheckExpr<'_>>()))
        })?;
        for c in constr.check.iter() {
            let name = c.ccname.as_ref().expect("ccname").clone_in(mcx)?;
            if !c.ccenforced {
                compiled.push(CheckExpr { name, state: None });
                continue;
            }
            let ccbin = c.ccbin.as_ref().expect("ccbin");
            let mut node = readfuncs::stringToNode(mcx, ccbin.as_str())?;
            if constr.has_generated_virtual {
                // execMain.c:1818 expand_generated_columns_in_expr.
                node = expand_generated_columns_in_expr(mcx, node, rel, 1)?.unwrap_or(node);
            }
            // C ExecPrepareExpr: expression_planner before init — the stored
            // ccbin is parse output (CollateExpr survives until const-fold).
            node = clauses::eval_const_expressions(mcx, node)?;
            nodes_core::fix_opfuncids(node)?;
            let mut state = execexpr::exec_init_expr(mcx, Some(node), execexpr::ParamBind::NONE)?
                .expect("check constraint expr");
            // Whole-row/composite steps return by-ref datums (C evaluates in
            // the per-tuple context).
            state.arm_result_mcx(mcx);
            compiled.push(CheckExpr { name, state: Some(state) });
        }
        *check_exprs = Some(compiled);
    }
    for (i, ce) in check_exprs.as_mut().expect("just built").iter_mut().enumerate() {
        let Some(state) = ce.state.as_deref_mut() else { continue };
        let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
        let r = execexpr::exec_eval_expr(state, &mut slots)?;
        if !r.isnull && !r.value.as_bool() {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

// ExecConstraints (execMain.c), NOT NULL arm (ReportNotNullViolationError).
fn exec_not_null_constraints<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    root_rel: Option<&Relation<'mcx>>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<()> {
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attgenerated == VIRTUAL_GEN {
            continue;
        }
        if att.attnotnull && exectuples::slot_attisnull(slot, i as i32 + 1) {
            return Err(not_null_violation(mcx, rel, slot, i, root_rel, modified_cols));
        }
    }
    Ok(())
}

const VIRTUAL_GEN: i8 = types_core::catalog::ATTRIBUTE_GENERATED_VIRTUAL as i8;

// check_modified_virtual_generated (trigger.c:6735): a trigger-returned tuple
// must not carry a non-null value in a virtual generated column; offending
// columns revert to null. None means the tuple was already clean.
fn check_modified_virtual_generated<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    tupdesc: &TupleDescData<'mcx>,
    tuple: &types_tuple::HeapTupleData<'_>,
) -> PgResult<Option<heaptuple::HeapTuple<'mcx>>> {
    if !tupdesc.constr.as_deref().is_some_and(|c| c.has_generated_virtual) {
        return Ok(None);
    }
    let mut cols: mcx::PgVec<'_, i32> = mcx::PgVec::new_in(mcx);
    for i in 0..tupdesc.natts as usize {
        if tupdesc.attr(i).attgenerated == VIRTUAL_GEN
            && !types_tuple::heap_attisnull(tuple, i as i32 + 1, Some(tupdesc))
        {
            cols.push(i as i32 + 1);
        }
    }
    if cols.is_empty() {
        return Ok(None);
    }
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, cols.len())?;
    for _ in 0..cols.len() {
        values.push(Datum::null());
        isnull.push(true);
    }
    heaptuple::heap_modify_tuple_by_cols(mcx, tuple, tupdesc, &cols, &values, &isnull).map(Some)
}

// build_generation_expression (rewriteHandler.c:4520), adbin-direct copy: the
// rewrite_handler home is unreachable (planner -> execmain -> this crate
// cycle) and cookDefault stored a coerced tree, so re-coercion is a no-op.
fn build_generation_expression<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attrno: usize,
) -> PgResult<types_nodes::Node<'mcx>> {
    let att = rel.rd_att.attr(attrno - 1);
    let constr = rel.rd_att.constr.as_deref().expect("caller checked");
    let adbin = constr
        .defval
        .iter()
        .find(|d| d.adnum == attrno as i16)
        .and_then(|d| d.adbin.as_ref())
        .unwrap_or_else(|| {
            panic!(
                "no generation expression found for column number {} of table \"{}\"",
                attrno,
                String::from_utf8_lossy(rel.rd_rel.relname.name_str())
            )
        });
    let expr = readfuncs::stringToNode(mcx, adbin.as_str())?;
    if att.attcollation != 0 && att.attcollation != nodes_core::node_funcs::expr_collation(expr) {
        return types_nodes::Node::mk(
            mcx,
            types_nodes::primnodes::CollateExpr {
                arg: expr,
                collOid: att.attcollation,
                location: -1,
            },
        );
    }
    Ok(expr)
}

// expand_generated_columns_in_expr (rewriteHandler.c:4493): Vars naming a
// virtual generated column of rel at varno become the generation expression.
fn expand_generated_columns_in_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: types_nodes::Node<'mcx>,
    rel: &Relation<'mcx>,
    varno: i32,
) -> PgResult<Option<types_nodes::Node<'mcx>>> {
    if let Some(v) = node.as_var() {
        if v.varlevelsup != 0 || v.varno != varno {
            return Ok(None);
        }
        if v.varattno == 0 {
            // ReplaceVarsFromTargetList whole-row arm (rewriteManip.c:1801):
            // a named-rowtype whole-row Var becomes a RowExpr over per-field
            // Vars (dropped columns as NULL int4 consts, expandRTE shape),
            // each field then replaced so virtual columns expand.
            let mut args = types_nodes::list::NodeList::nil();
            for i in 0..rel.rd_att.natts as usize {
                let att = rel.rd_att.attr(i);
                let field = if att.attisdropped {
                    types_nodes::Node::mk_const(
                        mcx,
                        types_core::catalog::INT4OID,
                        -1,
                        0,
                        4,
                        datum::Datum::null(),
                        true,
                        true,
                    )?
                } else if att.attgenerated == VIRTUAL_GEN {
                    debug_assert!(varno == 1, "generation expression Vars are varno 1");
                    build_generation_expression(mcx, rel, i + 1)?
                } else {
                    types_nodes::Node::mk_var(
                        mcx,
                        varno,
                        (i + 1) as i16,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        0,
                    )?
                };
                args.lappend(mcx, field)?;
            }
            return Ok(Some(types_nodes::Node::mk(
                mcx,
                types_nodes::RowExpr {
                    args,
                    row_typeid: v.vartype,
                    row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                    colnames: types_nodes::list::NodeList::nil(),
                    location: v.location,
                },
            )?));
        }
        if rel.rd_att.attr(v.varattno as usize - 1).attgenerated != VIRTUAL_GEN {
            return Ok(None);
        }
        let e = build_generation_expression(mcx, rel, v.varattno as usize)?;
        debug_assert!(varno == 1, "generation expression Vars are varno 1");
        return Ok(Some(e));
    }
    clauses::walker::expression_tree_mutator(mcx, node, &mut |n| {
        expand_generated_columns_in_expr(mcx, n, rel, varno)
    })
}

// ExecRelGenVirtualNotNull (execMain.c:2098): NullTest(IS NOT NULL) over the
// generation expression per virtual not-null column; compiled once.
pub fn exec_rel_gen_virtual_notnull<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    virtual_nn_exprs: &mut Option<mcx::PgVec<'mcx, VirtualNnExpr<'mcx>>>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
) -> PgResult<Option<usize>> {
    if virtual_nn_exprs.is_none() {
        let mut compiled: mcx::PgVec<'mcx, VirtualNnExpr<'mcx>> = mcx::PgVec::new_in(mcx);
        for i in 0..rel.rd_att.natts as usize {
            let att = rel.rd_att.attr(i);
            if !(att.attnotnull && att.attgenerated == VIRTUAL_GEN) {
                continue;
            }
            let arg = build_generation_expression(mcx, rel, i + 1)?;
            let nulltest = types_nodes::Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(arg),
                    nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                    argisrow: false,
                    location: -1,
                },
            )?;
            let mut state =
                execexpr::exec_init_expr(mcx, Some(nulltest), execexpr::ParamBind::NONE)?
                    .expect("virtual not-null expr");
            state.arm_result_mcx(mcx);
            compiled.push(VirtualNnExpr { attnum: i, state });
        }
        *virtual_nn_exprs = Some(compiled);
    }
    exectuples::slot_getallattrs(slot);
    for e in virtual_nn_exprs.as_mut().expect("just built").iter_mut() {
        let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
        let r = execexpr::exec_eval_expr(&mut e.state, &mut slots)?;
        if !r.isnull && !r.value.as_bool() {
            return Ok(Some(e.attnum));
        }
    }
    Ok(None)
}


#[cold]
#[inline(never)]
fn schema_name_of(mcx: mcx::Mcx<'_>, rel: &Relation<'_>) -> String {
    lsyscache::misc::get_namespace_name(mcx, rel.rd_rel.relnamespace)
        .ok()
        .flatten()
        .map(|s| s.as_str().to_owned())
        .unwrap_or_default()
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_null_violation<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    attidx: usize,
    root_rel: Option<&Relation<'mcx>>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> Box<PgError> {
    let att = rel.rd_att.attr(attidx);
    let col = String::from_utf8_lossy(att.attname.name_str()).into_owned();
    let table = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
    let mut e = PgError::error(format!(
        "null value in column \"{col}\" of relation \"{table}\" violates \
         not-null constraint"
    ))
    .with_sqlstate(ERRCODE_NOT_NULL_VIOLATION)
    .with_schema_name(schema_name_of(mcx, rel))
    .with_table_name(table);
    if let Ok(Some(desc)) = root_slot_value_description(mcx, rel, slot, root_rel, modified_cols) {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    e.column_name = Some(col);
    Box::new(e)
}

// ExecConstraints' ri_RootResultRelInfo leg: a routed leaf's failing row is
// reported in the root's rowtype (execMain.c reverse attrmap); modified_cols
// is numbered in the description relation's (root's) attnos.
fn root_slot_value_description<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    root_rel: Option<&Relation<'mcx>>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> PgResult<Option<String>> {
    match root_rel {
        Some(root) if root.rd_id != rel.rd_id => {
            let map = tupdesc::build_attrmap_by_name_if_req(mcx, &rel.rd_att, &root.rd_att, false)?;
            execpartition::slot_value_description(mcx, root, slot, modified_cols, map.as_deref())
        }
        _ => execpartition::slot_value_description(mcx, rel, slot, modified_cols, None),
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn check_violation<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    slot: &mut SlotData<'mcx>,
    failed: usize,
    root_rel: Option<&Relation<'mcx>>,
    modified_cols: Option<&types_nodes::Bitmapset<'mcx>>,
) -> Box<PgError> {
    let constr = rel.rd_att.constr.as_deref().expect("has checks");
    let ccname = constr.check[failed]
        .ccname
        .as_ref()
        .map(|s| s.as_str().to_owned())
        .unwrap_or_default();
    let table = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
    let mut e = PgError::error(format!(
        "new row for relation \"{table}\" violates check constraint \"{ccname}\""
    ))
    .with_sqlstate(ERRCODE_CHECK_VIOLATION)
    .with_schema_name(schema_name_of(mcx, rel))
    .with_table_name(table)
    .with_constraint_name(ccname);
    if let Ok(Some(desc)) = root_slot_value_description(mcx, rel, slot, root_rel, modified_cols) {
        e = e.with_detail(format!("Failing row contains {desc}."));
    }
    Box::new(e)
}

#[track_caller]
#[cold]
#[inline(never)]
fn plan_output_mismatch(detail: impl Into<String>) -> Box<PgError> {
    Box::new(
        PgError::error("table row type and query-specified row type do not match")
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_detail(detail.into()),
    )
}

mcx::forget_safe_nodrop!(NewColSrc);

// Exempt: indexes/snapshot_any/project_returning/on_conflict/check_exprs/
// trigdesc/trig_fmgr/trig_when/generated_exprs/router/leaf_indexes/
// leaf_partition_check/leaf_on_conflict/leaf_returning/leaf_trigdesc/
// leaf_trig_fmgr/leaf_trig_when/index_eval_cx/merge
// (and each CheckExpr's/GeneratedExpr's state) are
// released in exec_end_modify_table; CmdType is no-drop, const-proven below.
// rels and the leaf_* collections are arena PgVecs (their buffers die with
// es_query_cxt); the ones still exempt have element types without ForgetSafe
// impls (ExprState boxes, Rc trigdesc, std trigger caches) — all cleared in
// exec_end_modify_table before the bundle is forgotten.
const _: () = assert!(!core::mem::needs_drop::<CmdType>());
mcx::forget_safe_struct!(
    CheckExpr<'_> { name; state },
    GeneratedExpr<'_> { attnum; state },
    VirtualNnExpr<'_> { attnum; state },
    WcoExpr<'_> { kind, relname, polname; state },
    ResultRelExec<'_> { rti, rd_id, relkind, ri_newTupleSlot, ri_oldTupleSlot,
        ri_ReturningSlot, ri_AllNullSlot, ri_projectNewInfoValid, ri_RowIdAttNo,
        update_cols, update_colnos;
        indexes, project_returning, check_exprs, partition_check, trigdesc,
        trig_fmgr, trig_old_slot, trig_when, all_updated_cols, child_to_root,
        generated_exprs, virtual_nn_exprs, wco_exprs, merge },
    ModifyTableState<'_> { plan, canSetTag, mt_done, fireBSTriggers, cur,
        insert_target_root, last_result_oid, result_oid_attno, returning_slot,
        node_ecxt, oc_old_slot, cross_part_root_slot, last_insert_leaf,
        last_insert_remapped, oc_returning_leaf,
        mt_merge_inserted, mt_merge_updated, mt_merge_deleted, merge_active_cmd,
        mt_merge_pending_not_matched, outer_instr_idx, epq_origslot,
        rels, root, leaf_checks, leaf_virtual_nn, leaf_generated, leaf_slots,
        leaf_arbiters, leaf_existing, leaf_child_to_root, leaf_wco,
        leaf_ri_checked;
        operation, snapshot_any, on_conflict, epq_subs, epq_arowmarks,
        router, leaf_indexes, leaf_partition_check,
        leaf_on_conflict,
        leaf_returning, leaf_trigdesc, leaf_trig_fmgr, leaf_trig_when,
        transition_capture, oc_transition_capture,
        index_eval_cx },
);

// =============================================================================
// ===== WAVE-9 APPEND REGION (WS-AG fusion D1a) — do not edit above ==========
//
// The rowmode-endgame §2.2 accept-seam decomposition (wave-9 contract §1
// rung 2): the chain-admitted trigger-INSERT shape's `mt_accept_row` →
// `exec_insert` composition, re-exposed as five separately callable seams
// so the stitched row chain (lanev2/dml.rs) can drive the §2.2 targets —
// `br_row_triggers` / `table_tuple_insert`+`ExecInsertIndexTuples` /
// `ar_insert_triggers` / `exec_process_returning` — as individual protocol
// calls with the statement-shape checks hoisted to admission (the
// work-removal channel: the per-row drive stops re-deciding view/partition/
// leaf/ON-CONFLICT arms that are structurally dead for the admitted shape).
//
// SHAPE PRECONDITION (every seam): `mt_rowchain_shape_mask` returned Some —
// CMD_INSERT, ONCONFLICT_NONE, single plain-heap result relation (rels[0],
// cur == 0, root None, RELKIND_RELATION), no tableoid dispatch column
// (result_oid_attno == 0), RETURNING absent or trivial. The chain host
// re-probes per drive and falls to the DmlInsertOp portable host on ANY
// mismatch (fail closed, never a wrong specialization); the seams
// debug-assert the load-bearing facts.
//
// BYTE-IDENTITY ARGUMENT, per target (each statement below is
// `mt_accept_row`'s / `exec_insert`'s own, in its original order, with
// branches DEAD-for-the-shape removed — dead-branch elision changes no
// executed statement and no error site):
// * `mt_ins_stage`   ≡ mt_accept_row's EvalPlanQualSetSlot mirror (the
//   epq_origslot/epq_subs stores) + the CMD_INSERT arm's projection-init +
//   exec_get_insert_new_tuple statements. The multi-rel tableoid dispatch
//   block is dead (result_oid_attno == 0, debug-pinned).
// * `mt_ins_br_triggers` ≡ exec_insert's BR block: the IDENTICAL
//   `br_row_triggers(.., TRIGGER_TYPE_INSERT, TRIGGER_EVENT_INSERT, None,
//   Some(slot), None)` call (leaf = None: the shape routes nothing).
//   false = suppression ≡ exec_insert's `return Ok(None)`.
// * `mt_ins_write`   ≡ exec_insert's statements from the (dead) routing
//   blocks through the physical write: last_insert_leaf/remapped resets,
//   the generated-columns + materialize + ExecOpenIndices block, the RLS
//   WCO_RLS_INSERT_CHECK block, the constraints + relispartition-check
//   block, then `table_tuple_insert` + `ExecInsertIndexTuples` (the
//   onconflict == 0 arm verbatim; the speculative vlock loop is dead by
//   ONCONFLICT_NONE). Returns the recheck_indexes the AR epilogue consumes.
// * `mt_ins_epilogue` ≡ exec_insert's tail: `ar_insert_triggers` (leaf =
//   None), the post-insert WCO_VIEW_CHECK arm, and the canSetTag
//   es_processed bump.
// * `mt_ins_returning` ≡ mt_accept_row's CMD_INSERT RETURNING block for
//   ONCONFLICT_NONE: oc_old_slot is None by shape (debug-pinned), so cmd =
//   CMD_INSERT and the OC materialize/clear leg is dead; the call is the
//   IDENTICAL `exec_process_returning(.., CMD_INSERT, None, Some(rslot),
//   plan_slot)`.
// Error identity: every erroring statement above IS the node's own helper
// (the two-regime error law's effectful half) — its PgError unwind is
// byte-identical by construction, at the same position in the per-row
// statement stream.
// =============================================================================

/// Chain-shape mask bit: the target has BEFORE ROW INSERT triggers (the
/// chain program carries the `mt_ins_br_triggers` protocol step).
pub const MT_ROWCHAIN_BR: u8 = 1 << 0;
/// Chain-shape mask bit: the target projects (trivial) RETURNING (the
/// chain program carries the `mt_ins_returning` protocol step).
pub const MT_ROWCHAIN_RET: u8 = 1 << 1;
/// The closed set of chain shape variants (compile-once per mask).
pub const MT_ROWCHAIN_MASKS: usize = 4;

/// The per-statement chain-shape mask (wave-9 WS-AG rung 2): Some(mask) =
/// this ADMITTED statement is the decomposed trigger-INSERT chain shape and
/// the `mt_ins_*` seams' specializations hold; None = drive the DmlInsertOp
/// portable host instead (fail closed). Reads private node state, so it
/// lives here like `mt_lane_shape_refusal`; read-only — refusal falls
/// through byte-safely. The structural facts are already guaranteed by the
/// admission verdict (`mt_lane_shape_refusal` with the rowchain arm); they
/// are re-checked here defensively because the seams SPECIALIZE on them.
pub fn mt_rowchain_shape_mask(mt: &ModifyTableState<'_>) -> Option<u8> {
    if mt.operation != CmdType::CMD_INSERT
        || mt.plan.onConflictAction
            != types_nodes::primnodes::OnConflictAction::ONCONFLICT_NONE as u32
        || mt.root.is_some()
        || mt.rels.len() != 1
        || mt.cur != 0
        || mt.result_oid_attno != 0
    {
        return None;
    }
    let r = &mt.rels[0];
    if r.relkind != RELKIND_RELATION {
        return None;
    }
    // The chain family is the trigger-bearing shape (mt_rowchain_shape's
    // own gate — trigger-less INSERTs stay on their existing hosts).
    let td = r.trigdesc.as_ref()?;
    let mut mask = 0u8;
    if td.trig_insert_before_row {
        mask |= MT_ROWCHAIN_BR;
    }
    if let Some(st) = r.project_returning.as_deref() {
        if st.has_old() || st.has_new() {
            // Admission refuses returning-old-new; defensive re-check.
            return None;
        }
        mask |= MT_ROWCHAIN_RET;
    }
    Some(mask)
}

/// Chain seam 1/5 — stage one source row for the insert (mt_accept_row's
/// pre-dispatch bookkeeping + the CMD_INSERT arm's projection staging).
/// Returns the slot the write path consumes.
#[inline]
pub fn mt_ins_stage<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    // C EvalPlanQualSetSlot mirror — mt_accept_row's head, verbatim.
    mt.epq_origslot = Some(plan_slot);
    if let Some(subs) = mt.epq_subs.as_mut() {
        subs.origslot = Some(plan_slot);
    }
    // The multi-rel tableoid dispatch is dead for the chain shape.
    debug_assert_eq!(mt.result_oid_attno, 0, "chain shape has no tableoid dispatch");
    debug_assert_eq!(mt.cur, 0, "chain shape is single-result-relation");
    // The CMD_INSERT arm's head, verbatim.
    if !mt.rel().ri_projectNewInfoValid {
        exec_init_insert_projection(mt, estate)?;
    }
    exec_get_insert_new_tuple(mt, estate, plan_slot)
}

/// Chain seam 2/5 — the §2.2 `br_row_triggers` target: exec_insert's BR
/// block for the unrouted plain-heap shape. `true` = proceed; `false` = the
/// trigger suppressed the row (≡ exec_insert's `return Ok(None)`; the chain
/// skips back to the loop top — es_processed and the RETURNING stream see
/// nothing, exactly the Volcano suppression).
#[inline]
pub fn mt_ins_br_triggers<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
) -> PgResult<bool> {
    debug_assert!(
        mt.rel().trigdesc.as_ref().is_some_and(|td| td.trig_insert_before_row),
        "BR seam driven without BR-row triggers (mask drift)"
    );
    br_row_triggers(
        mt,
        estate,
        types_trigger::TRIGGER_TYPE_INSERT,
        types_trigger::TRIGGER_EVENT_INSERT,
        None,
        Some(slot_id),
        None,
    )
}

/// Chain seam 3/5 — the §2.2 write target (`table_tuple_insert` +
/// `ExecInsertIndexTuples`) plus exec_insert's pre-write statements for the
/// unrouted ONCONFLICT_NONE shape, in exec_insert's own order. Returns the
/// recheck-index list `mt_ins_epilogue` consumes.
#[inline]
pub fn mt_ins_write<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let mcx = estate.es_query_cxt;
    let output_cid = estate.es_output_cid;
    let mut recheck_indexes: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);

    // exec_insert's routing out-params, reset exactly as the unrouted path
    // leaves them (exec_process_returning `take`s last_insert_remapped).
    mt.last_insert_leaf = None;
    mt.last_insert_remapped = None;

    // Stored generated columns + materialize + index open (exec_insert's
    // middle block; the routed/remapped arms are dead for the shape).
    {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState { rels, cur, .. } = &mut *mt;
        let r = &mut rels[*cur];
        let rel = es_relations[(r.rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &mut es_tupleTable[slot_id.0 as usize];

        slot.base_mut().tts_tableOid = rel.rd_id;
        if rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored) {
            exec_compute_stored_generated(mcx, &mut r.generated_exprs, rel, slot)?;
        }
        exectuples::exec_materialize_slot(slot, mcx)?;
        slot.base_mut().tts_tableOid = rel.rd_id;

        if rel.rd_rel.relhasindex && r.indexes.is_none() {
            // onconflict == 0 by shape: speculative = false.
            r.indexes = Some(execindexing::ExecOpenIndices(mcx, rel, false)?);
        }
    }

    // RLS WITH CHECK OPTIONS (exec_insert's WCO block; operation is
    // CMD_INSERT by shape, so the kind is WCO_RLS_INSERT_CHECK).
    {
        let ecxt = mt.node_ecxt;
        let ModifyTableState { rels, cur, .. } = &mut *mt;
        let wcos = &mut rels[*cur].wco_exprs;
        if !wcos.is_empty() {
            exec_with_check_options(estate, ecxt, wcos, WCOKind::WCO_RLS_INSERT_CHECK, slot_id)?;
        }
    }

    // Constraints + the direct-partition check (exec_insert's constraints
    // block; err_root_rel is None — the unrouted arm).
    {
        let target_rte = estate.es_range_table[(mt.rel().rti - 1) as usize];
        let perminfos = estate.es_rteperminfos;
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState { rels, cur, .. } = &mut *mt;
        let r = &mut rels[*cur];
        let rel = es_relations[(r.rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &mut es_tupleTable[slot_id.0 as usize];
        let mod_cols = {
            let rte = target_rte;
            let mut cols = types_nodes::Bitmapset::empty();
            if rte.perminfoindex > 0 {
                if let Some(pis) = perminfos {
                    let pi = pis
                        .nth(rte.perminfoindex as usize - 1)
                        .as_rte_permission_info()
                        .expect("permInfos cell");
                    cols = pi.insertedCols.union(&pi.updatedCols, mcx)?;
                }
            }
            cols
        };
        exec_constraints(
            mcx,
            &mut r.check_exprs,
            &mut r.virtual_nn_exprs,
            rel,
            slot,
            None,
            Some(&mod_cols),
        )?;
        // Direct INSERT into a partition leaf checks the partition
        // constraint (leaf_idx.is_none() arm of exec_insert's condition).
        if rel.rd_rel.relispartition {
            if !execpartition::exec_partition_check(mcx, &mut r.partition_check, rel, slot)? {
                return Err(execpartition::partition_constraint_violation(
                    mcx,
                    rel,
                    slot,
                    Some(&mod_cols),
                    None,
                ));
            }
        }
    }

    // The physical write: exec_insert's onconflict == 0 arm, verbatim.
    {
        let EStateData { es_relations, es_tupleTable, .. } = &mut *estate;
        let ModifyTableState { rels, cur, index_eval_cx, .. } = &mut *mt;
        let r = &mut rels[*cur];
        let rel = es_relations[(r.rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let slot = &mut es_tupleTable[slot_id.0 as usize];

        tableam::table_tuple_insert(mcx, rel, slot, output_cid, 0, None)?;

        if let Some(indexes) = r.indexes.as_mut() {
            if indexes.num_indices() > 0 {
                recheck_indexes = execindexing::ExecInsertIndexTuples(
                    mcx,
                    index_eval_cx.as_ref().expect("index_eval_cx live until ExecEndNode").mcx(),
                    indexes,
                    rel,
                    slot,
                    false,
                    None,
                    &[],
                    false,
                )?;
            }
        }
    }
    Ok(recheck_indexes)
}

/// Chain seam 4/5 — the §2.2 `ar_insert_triggers` target plus exec_insert's
/// tail (post-insert view CHECK OPTIONs, the canSetTag es_processed bump).
#[inline]
pub fn mt_ins_epilogue<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    recheck_indexes: &[Oid],
) -> PgResult<()> {
    ar_insert_triggers(mt, estate, slot_id, recheck_indexes, None)?;
    if !mt.rel().wco_exprs.is_empty() {
        let mcx = estate.es_query_cxt;
        let ecxt = mt.node_ecxt;
        let r = &mut mt.rels[mt.cur];
        let rti = r.rti;
        exec_view_check_options(
            mcx,
            estate,
            ecxt,
            &mut r.wco_exprs,
            slot_id,
            WcoRel::Rti { rti, root_rti: None },
        )?;
    }
    if mt.canSetTag {
        estate.es_processed += 1;
    }
    Ok(())
}

/// Chain seam 5/5 — the §2.2 `exec_process_returning` target:
/// mt_accept_row's CMD_INSERT RETURNING block for ONCONFLICT_NONE.
#[inline]
pub fn mt_ins_returning<'mcx>(
    mt: &mut ModifyTableState<'mcx>,
    estate: &mut EStateData<'mcx>,
    result_slot: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<ExecSlotId> {
    // ONCONFLICT_NONE never stages an OC old slot, so the `oc_old_slot
    // .take()` in mt_accept_row reads None (cmd = CMD_INSERT, the OC
    // materialize/clear leg dead) — debug-pinned.
    debug_assert!(mt.oc_old_slot.is_none(), "OC old slot in an ONCONFLICT_NONE chain");
    debug_assert!(mt.rel().project_returning.is_some(), "RETURNING seam without a projection");
    exec_process_returning(mt, estate, CmdType::CMD_INSERT, None, Some(result_slot), plan_slot)
}
// --- end WS-AG (wave-9) -------------------------------------------------------
